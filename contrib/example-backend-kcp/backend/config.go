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

package backend

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"time"

	apiextensionsclient "github.com/kcp-dev/client-go/apiextensions/client"
	apiextensionsinformers "github.com/kcp-dev/client-go/apiextensions/informers"
	kubeinformers "github.com/kcp-dev/client-go/informers"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	apisv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/apis/v1alpha1"
	kcpclusterclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
	"github.com/kcp-dev/logicalcluster/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/kube-bind/kube-bind/contrib/example-backend-kcp/options"
	bindclient "github.com/kube-bind/kube-bind/sdk/kcp/clientset/versioned/cluster"
	bindinformers "github.com/kube-bind/kube-bind/sdk/kcp/informers/externalversions"
)

type Config struct {
	Options *options.CompletedOptions

	ClientConfig        *rest.Config
	BindClient          *bindclient.ClusterClientset
	KubeClient          *kcpkubernetesclientset.ClusterClientset
	ApiextensionsClient *apiextensionsclient.ClusterClientset

	KubeInformers          kubeinformers.SharedInformerFactory
	BindInformers          bindinformers.SharedInformerFactory
	ApiextensionsInformers apiextensionsinformers.SharedInformerFactory
}

// NewConfig will create clients and informers for the backend
// Important: This will create clients, pointing to virtual workspace of the APIExport,
// not the actual workspace of the APIExport.
func NewConfig(options *options.CompletedOptions) (*Config, error) {
	config := &Config{
		Options: options,
	}

	// create clients
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = options.KubeConfig
	var err error
	config.ClientConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, err
	}

	cluster := logicalcluster.Name(options.KubeBindWorkspacePath)

	bootstrapConfig := rest.CopyConfig(config.ClientConfig)
	bootstrapConfig = rest.AddUserAgent(bootstrapConfig, "kube-bind-kcp-bootstrap")
	h, err := url.Parse(bootstrapConfig.Host)
	if err != nil {
		return nil, err
	}
	h.Path = ""
	bootstrapConfig.Host = h.String()
	// Get rest configs for all virtual workspaces.
	// TODO(mjudeikis): We will use only first one for now, hence NOT supporting sharding.
	restConfigs, err := restConfigForAPIExport(context.Background(), bootstrapConfig, options.KubeBindAPIExportName, cluster.Path())
	if err != nil {
		return nil, err
	}
	restConfig := restConfigs[0]

	restConfig = rest.CopyConfig(restConfig)
	restConfig = rest.AddUserAgent(restConfig, "kube-bind-kcp-backend")

	h, err = url.Parse(restConfig.Host)
	if err != nil {
		return nil, err
	}
	h.Path = ""
	restConfig.Host = h.String()

	// In dev-mode, we use provided kubeconfig to access service account secrets and generate in-memory kubeconfig.
	// In production this should be done via service account inside workspace, which has access to secrets.
	if options.DevMode {
		fmt.Println("Running in development mode. Using kubeconfig to get dev secrets and generate in-memory kubeconfig", options.KubeConfig)
		fmt.Println("This will override restConfig object credentials to use ServiceAccount token and CA.")
		rest := rest.CopyConfig(config.ClientConfig)
		h, err = url.Parse(rest.Host)
		if err != nil {
			return nil, err
		}
		h.Path = ""
		rest.Host = h.String()

		clientset, err := kcpkubernetesclientset.NewForConfig(rest)
		if err != nil {
			return nil, err
		}
		// TODO(mjudeikis): This is hardcoded for now, but should be configurable.
		secretName := "kube-bind-controller-secret"
		saNamespace := "default"
		secret, err := clientset.CoreV1().Cluster(cluster.Path()).Secrets(saNamespace).Get(context.TODO(), secretName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		token := secret.Data["token"]
		ca := base64.StdEncoding.EncodeToString(secret.Data["ca.crt"])

		restConfig.BearerToken = string(token)
		restConfig.TLSClientConfig.CAData = []byte(ca)
	}

	if config.BindClient, err = bindclient.NewForConfig(config.ClientConfig); err != nil {
		return nil, err
	}
	if config.KubeClient, err = kcpkubernetesclientset.NewForConfig(config.ClientConfig); err != nil {
		return nil, err
	}
	if config.ApiextensionsClient, err = apiextensionsclient.NewForConfig(config.ClientConfig); err != nil {
		return nil, err
	}

	// construct informer factories
	config.KubeInformers = kubeinformers.NewSharedInformerFactory(config.KubeClient, time.Minute*30)
	config.BindInformers = bindinformers.NewSharedInformerFactory(config.BindClient, time.Minute*30)
	config.ApiextensionsInformers = apiextensionsinformers.NewSharedInformerFactory(config.ApiextensionsClient, time.Minute*30)

	return config, nil
}

// restConfigForAPIExport returns a *rest.Config properly configured to communicate with the endpoint for the
// APIExport's virtual workspace.
func restConfigForAPIExport(ctx context.Context, rootRestConfig *rest.Config, apiExportName string, cluster logicalcluster.Path) ([]*rest.Config, error) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("getting apiexport")

	bootstrapClient, err := kcpclusterclientset.NewForConfig(rootRestConfig)
	if err != nil {
		return nil, err
	}

	var apiExport *apisv1alpha1.APIExport
	if apiExportName != "" {
		if apiExport, err = bootstrapClient.ApisV1alpha1().APIExports().Cluster(cluster).Get(ctx, apiExportName, metav1.GetOptions{}); err != nil {
			return nil, fmt.Errorf("error getting APIExport [%q] in cluster [%s] %w", apiExportName, cluster, err)
		}
	} else {
		logger := klog.FromContext(ctx)
		logger.V(2).Info("api-export-name is empty - listing")
		exports := &apisv1alpha1.APIExportList{}
		if exports, err = bootstrapClient.ApisV1alpha1().APIExports().List(ctx, metav1.ListOptions{}); err != nil {
			return nil, fmt.Errorf("error listing APIExports: %w", err)
		}
		if len(exports.Items) == 0 {
			return nil, fmt.Errorf("no APIExport found")
		}
		if len(exports.Items) > 1 {
			return nil, fmt.Errorf("more than one APIExport found")
		}
		apiExport = &exports.Items[0]
	}

	if len(apiExport.Status.VirtualWorkspaces) < 1 {
		return nil, fmt.Errorf("APIExport %q status.virtualWorkspaces is empty", apiExportName)
	}

	var results []*rest.Config
	// TODO(mjudeikis): For sharding support we would need to interact with the APIExportEndpointSlice API
	// rather than APIExport. We would then have an URL per shard. For now we just get list of all and move on.
	// TODO: WE should use something else as base for kubeconfig, not the rootRestConfig. Maybe dedicated service account?
	for _, ws := range apiExport.Status.VirtualWorkspaces {
		logger.Info("virtual workspace", "url", ws.URL)
		cfg := rest.CopyConfig(rootRestConfig)
		cfg.Host = ws.URL
		results = append(results, cfg)
	}

	return results, nil
}
