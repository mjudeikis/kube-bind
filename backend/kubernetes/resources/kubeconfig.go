/*
Copyright 2022 The Kube Bind Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resources

import (
	"context"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubebindv1alpha2 "github.com/kube-bind/kube-bind/sdk/apis/kubebind/v1alpha2"
)

func GenerateKubeconfig(ctx context.Context,
	client client.Client,
	clusterConfig *rest.Config,
	externalAddressGenerator ExternalAddressGeneratorFunc,
	externalCA []byte,
	externalTLSServerName string,
	saSecretName, ns, kubeconfigSecretName string,
) (*corev1.Secret, error) {
	logger := klog.FromContext(ctx)

	externalAddress, err := externalAddressGenerator(ctx, clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to generate external address: %w", err)
	}

	if externalCA == nil {
		if len(clusterConfig.CAData) != 0 {
			externalCA = clusterConfig.CAData
		} else if len(clusterConfig.CAFile) != 0 {
			ca, err := os.ReadFile(clusterConfig.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file at %s: %w", clusterConfig.CAFile, err)
			}
			externalCA = ca
		}
	}

	var saSecret corev1.Secret
	logger.V(2).Info("Waiting for service account secret to be updated with a token", "name", saSecretName)
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (done bool, err error) {
		err = client.Get(ctx, types.NamespacedName{Namespace: ns, Name: saSecretName}, &saSecret)
		if err != nil && !errors.IsNotFound(err) {
			return false, err
		} else if errors.IsNotFound(err) {
			return false, nil
		}
		return saSecret.Data["token"] != nil && saSecret.Data["ca.crt"] != nil, nil
	}); err != nil {
		return nil, err
	}

	cfg := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"default": {
				Server:                   externalAddress,
				TLSServerName:            externalTLSServerName,
				CertificateAuthorityData: externalCA,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"default": {
				Cluster:   "default",
				Namespace: ns,
				AuthInfo:  "default",
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"default": {
				Token: string(saSecret.Data["token"]),
			},
		},
		CurrentContext: "default",
	}

	kubeconfig, err := clientcmd.Write(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode kubeconfig: %w", err)
	}

	kubeconfigSecret := &corev1.Secret{
		ObjectMeta: v1.ObjectMeta{
			Name:      kubeconfigSecretName,
			Namespace: ns,
		},
		Data: map[string][]byte{
			"kubeconfig": kubeconfig,
		},
	}

	logger.V(1).Info("Creating kubeconfig secret", "name", kubeconfigSecretName)
	if err := client.Create(ctx, kubeconfigSecret); err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	} else if err == nil {
		return kubeconfigSecret, nil
	}

	var existing corev1.Secret
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		err := client.Get(ctx, types.NamespacedName{Namespace: ns, Name: kubeconfigSecret.Name}, &existing)
		if err != nil {
			return err
		}
		existing.Data = kubeconfigSecret.Data
		logger.V(1).Info("Updating kubeconfig secret", "name", kubeconfigSecretName)
		err = client.Update(ctx, &existing)
		return err
	}); err != nil {
		return nil, err
	}
	return &existing, nil
}

// ExternalAddressGeneratorFunc is a function that generates the external address for a cluster based on the clusterConfig
// and is dependent on the provider.
type ExternalAddressGeneratorFunc func(ctx context.Context, clusterConfig *rest.Config) (string, error)

// FixedExternalAddressGenerator returns an address generator uses the given address when available,
// otherwise falls back to the host of the provided rest config.
func NewFixedExternalAddressGenerator(address string) ExternalAddressGeneratorFunc {
	return func(_ context.Context, clusterConfig *rest.Config) (string, error) {
		if address == "" {
			return clusterConfig.Host, nil
		}

		return address, nil
	}
}

type ClusterIdentityGeneratorFunc func(ctx context.Context, client client.Client, cluster *kubebindv1alpha2.Cluster) (*kubebindv1alpha2.ClusterIdentity, error)

const kubeSystemNamespace = "kube-system"

func NewFixedClusterIdentityGenerator() ClusterIdentityGeneratorFunc {
	return func(ctx context.Context, client client.Client, _ *kubebindv1alpha2.Cluster) (*kubebindv1alpha2.ClusterIdentity, error) {
		// Cluster identity is based on the UID of the kube-system namespace.
		ns := &corev1.Namespace{}
		if err := client.Get(ctx, types.NamespacedName{Name: kubeSystemNamespace}, ns); err != nil {
			return nil, fmt.Errorf("failed to get namespace %q: %w", kubeSystemNamespace, err)
		}

		// Name is stored in the Kind: Cluster object inside the cluster.
		cluster := &kubebindv1alpha2.Cluster{}
		if err := client.Get(ctx, types.NamespacedName{Name: kubebindv1alpha2.DefaultClusterName}, cluster); err != nil {
			return nil, fmt.Errorf("failed to get cluster %q: %w", kubebindv1alpha2.DefaultClusterName, err)
		}

		prettyName := cluster.Spec.PrettyName
		if cluster.Spec.PrettyName == "" {
			prettyName = string(ns.UID)
		}

		return &kubebindv1alpha2.ClusterIdentity{
			UID:  string(ns.UID),
			Name: prettyName,
		}, nil
	}
}
