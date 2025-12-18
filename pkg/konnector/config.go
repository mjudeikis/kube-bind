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

package konnector

import (
	"context"
	"time"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	kubernetesclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kube-bind/kube-bind/pkg/konnector/options"
	bindclient "github.com/kube-bind/kube-bind/sdk/client/clientset/versioned"
	bindinformers "github.com/kube-bind/kube-bind/sdk/client/informers/externalversions"
)

type Config struct {
	ClientConfig        *rest.Config
	BindClient          *bindclient.Clientset
	KubeClient          *kubernetesclient.Clientset
	ApiextensionsClient *apiextensionsclient.Clientset

	KubeInformers          kubeinformers.SharedInformerFactory
	BindInformers          bindinformers.SharedInformerFactory
	ApiextensionsInformers apiextensionsinformers.SharedInformerFactory

	ServerAddr string

	// ClusterIdentity is uniuqe identity of the cluster. Value is taked from LeaseNamepace UID.
	ClusterIdentity string
	// ClusterName is a pretty name of the cluster. If not set, ClusterIdentity is used.
	ClusterName string
}

func NewConfig(options *options.CompletedOptions) (*Config, error) {
	config := &Config{
		ServerAddr: options.ServerAddr,
	}

	// create clients
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = options.KubeConfigPath
	var err error
	config.ClientConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, err
	}
	config.ClientConfig = rest.CopyConfig(config.ClientConfig)
	config.ClientConfig = rest.AddUserAgent(config.ClientConfig, "konnector")

	if config.BindClient, err = bindclient.NewForConfig(config.ClientConfig); err != nil {
		return nil, err
	}
	if config.KubeClient, err = kubernetesclient.NewForConfig(config.ClientConfig); err != nil {
		return nil, err
	}
	if config.ApiextensionsClient, err = apiextensionsclient.NewForConfig(config.ClientConfig); err != nil {
		return nil, err
	}

	// construct informer factories
	config.KubeInformers = kubeinformers.NewSharedInformerFactory(config.KubeClient, time.Minute*30)
	config.BindInformers = bindinformers.NewSharedInformerFactory(config.BindClient, time.Minute*30)
	config.ApiextensionsInformers = apiextensionsinformers.NewSharedInformerFactory(config.ApiextensionsClient, time.Minute*30)

	if err := setupClusterIdentity(config, options); err != nil {
		return nil, err
	}

	return config, nil
}

// setupClusterIdentity sets up ClusterIdentity and ClusterName in the config.
func setupClusterIdentity(config *Config, options *options.CompletedOptions) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	ns, err := config.KubeClient.CoreV1().Namespaces().Get(ctx, options.LeaseLockNamespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	config.ClusterIdentity = string(ns.UID)

	if options.ClusterName == "" {
		config.ClusterName = config.ClusterIdentity
	} else {
		config.ClusterName = options.ClusterName
	}

	return nil

}
