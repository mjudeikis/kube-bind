/*
Copyright 2025 The Kube Bind Authors.

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/kube-bind/kube-bind/kcp/bootstrap"
	"github.com/kube-bind/kube-bind/kcp/bootstrap/options"
	"github.com/kube-bind/kube-bind/test/e2e/framework"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"gopkg.in/headzoo/surf.v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

const (
	kcpTestTimeout     = 30 * time.Minute
	kcpPollingInterval = 10 * time.Second
)

// TestKCPHappyCase tests the complete KCP integration workflow
// This test follows the steps outlined in kcp/README.md
func TestKCPHappyCase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping KCP e2e test in short mode")
	}

	_, cancel := context.WithTimeout(context.Background(), kcpTestTimeout)
	defer cancel()

	testDir := t.TempDir()
	t.Logf("Using test directory: %s", testDir)

	// Step 1: Bootstrap KCP
	t.Log("Step 2: Bootstrapping KCP...")
	err := bootstrapKCP(t, testDir)
	require.NoError(t, err, "Failed to bootstrap KCP")

	// Step 3: Setup backend workspace and start backend server
	t.Log("Step 3: Setting up backend workspace...")
	backendConfig := setupBackendWorkspace(t, testDir)

	t.Log("Step 3a: Starting backend server...")
	startBackendServer(t, backendConfig)

	// Step 4: Setup provider workspace
	t.Log("Step 4: Setting up provider workspace...")
	providerConfig := setupProviderWorkspace(t, testDir)

	// Step 5: Bind APIExport to provider workspace
	t.Log("Step 5: Binding APIExport to provider workspace...")
	bindAPIExportToProvider(t, providerConfig)

	// Step 6: Create example APIResourceSchema and APIExport
	t.Log("Step 6: Creating APIResourceSchema and APIExport...")
	createExampleResources(t, providerConfig)

	// Step 7: Get LogicalCluster URL
	t.Log("Step 7: Getting LogicalCluster URL...")
	clusterURL := getLogicalClusterURL(t, providerConfig)

	// Step 8: Setup consumer workspace
	t.Log("Step 8: Setting up consumer workspace...")
	consumerConfig := setupConsumerWorkspace(t, testDir)

	// THIS IS WHERE ACTUAL TEST STARTS:

	// Step 9: Perform binding process (browser simulation)
	t.Log("Step 9: Performing binding process with browser simulation...")
	performBindingWithBrowser(t, clusterURL, consumerConfig)
	t.Log("Binding process completed")

	// Step 11: Start konnector
	t.Log("Step 11: Starting konnector...")
	spew.Dump("Starting konnector with kubeconfig:", consumerConfig)
	framework.StartKonnector(t, createConfigFromFile(t, consumerConfig), "--kubeconfig="+consumerConfig)

	// Step 12: Test resource creation and synchronization
	t.Log("Step 12: Testing resource creation and synchronization...")
	testKCPResourceSync(t, consumerConfig, providerConfig)

	t.Log("KCP happy case test completed successfully!")
}

func bootstrapKCP(t *testing.T, testDir string) error {
	ctx := context.Background()
	// Copy admin kubeconfig for backend
	rootConfig := os.Getenv("KUBECONFIG")
	if rootConfig == "" {
		// Default KCP kubeconfig location
		rootConfig = ".kcp/admin.kubeconfig"
	}

	adminConfig := filepath.Join(testDir, "admin.kubeconfig")
	backendConfig := filepath.Join(testDir, "backend.kubeconfig")

	t.Logf("Using root kubeconfig: %s", rootConfig)
	copyFile(t, rootConfig, adminConfig)
	copyFile(t, adminConfig, backendConfig)

	// Create a new flagset to avoid conflicts with test flags
	fs := pflag.NewFlagSet("kcp-bootstrap", pflag.ContinueOnError)
	options := options.NewOptions()
	options.AddFlags(fs)

	// Set the kubeconfig directly without parsing command line flags
	options.KCPKubeConfig = adminConfig

	t.Logf("Bootstrapping KCP API with kubeconfig: %s", adminConfig)

	// create init server
	completed, err := options.Complete()
	if err != nil {
		return err
	}
	config, err := bootstrap.NewConfig(completed)
	if err != nil {
		return err
	}

	server, err := bootstrap.NewServer(ctx, config)
	if err != nil {
		return err
	}

	return server.Start(ctx)
}

func setupBackendWorkspace(t *testing.T, testDir string) string {
	backendConfig := filepath.Join(testDir, "backend.kubeconfig")

	// Switch to backend workspace
	env := append(os.Environ(), "KUBECONFIG="+backendConfig)
	cmd := exec.Command("kubectl", "ws", "use", ":root:kube-bind")
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to switch to backend workspace: %s", output)

	return backendConfig
}

func startBackendServer(t *testing.T, backendConfig string) {
	env := append(os.Environ(), "KUBECONFIG="+backendConfig)

	// Get the API export endpoint URL
	cmd := exec.Command("kubectl", "get", "apiexportendpointslice", "kube-bind.io",
		"-o", "jsonpath={.status.endpoints[0].url}")
	cmd.Env = env
	endpoint, err := cmd.Output()
	require.NoError(t, err, "Failed to get API export endpoint")

	serverURL := strings.TrimSpace(string(endpoint))
	require.NotEmpty(t, serverURL, "API export endpoint URL is empty")
	t.Logf("Using server URL: %s", serverURL)

	// Start the backend server as specified in the README
	backendCmd := exec.Command("../../../bin/backend",
		"--multicluster-runtime-provider", "kcp",
		"--server-url="+serverURL,
		"--oidc-issuer-client-secret=ZXhhbXBsZS1hcHAtc2VjcmV0",
		"--oidc-issuer-client-id=kube-bind",
		"--oidc-issuer-url=http://127.0.0.1:5556/dex",
		"--oidc-callback-url=http://127.0.0.1:8080/callback",
		"--pretty-name=BigCorp.com",
		"--namespace-prefix=kube-bind-",
		"--cookie-signing-key=bGMHz7SR9XcI9JdDB68VmjQErrjbrAR9JdVqjAOKHzE=",
		"--cookie-encryption-key=wadqi4u+w0bqnSrVFtM38Pz2ykYVIeeadhzT34XlC1Y=",
		"--schema-source=apiresourceschemas")

	backendCmd.Env = env

	// Start the backend server in background
	err = backendCmd.Start()
	require.NoError(t, err, "Failed to start backend server")

	// Clean up the backend process when test ends
	t.Cleanup(func() {
		if backendCmd.Process != nil {
			t.Logf("Stopping backend server (PID: %d)", backendCmd.Process.Pid)
			backendCmd.Process.Kill()
		}
	})

	// Wait for backend to be ready - check if it's listening on port 8080
	err = wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
		// Try to connect to the backend HTTP endpoint
		testCmd := exec.Command("curl", "-f", "-s", "http://127.0.0.1:8080/healthz")
		return testCmd.Run() == nil, nil
	})
	require.NoError(t, err, "Backend server failed to start or become ready")

	t.Log("Backend server started and ready")
}

func setupProviderWorkspace(t *testing.T, testDir string) string {
	providerConfig := filepath.Join(testDir, "provider.kubeconfig")
	copyFile(t, filepath.Join(testDir, "admin.kubeconfig"), providerConfig)

	env := append(os.Environ(), "KUBECONFIG="+providerConfig)

	// Switch to root and create provider workspace
	cmd := exec.Command("kubectl", "ws", "use", ":root")
	cmd.Env = env
	require.NoError(t, cmd.Run(), "Failed to switch to root workspace")

	cmd = exec.Command("kubectl", "ws", "create", "provider", "--enter", "--ignore-existing")
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create provider workspace: %s", output)

	return providerConfig
}

func bindAPIExportToProvider(t *testing.T, providerConfig string) {
	env := append(os.Environ(), "KUBECONFIG="+providerConfig)

	args := []string{
		"kcp", "bind", "apiexport", "root:kube-bind:kube-bind.io",
		"--accept-permission-claim", "clusterrolebindings.rbac.authorization.k8s.io",
		"--accept-permission-claim", "clusterroles.rbac.authorization.k8s.io",
		"--accept-permission-claim", "customresourcedefinitions.apiextensions.k8s.io",
		"--accept-permission-claim", "serviceaccounts.core",
		"--accept-permission-claim", "configmaps.core",
		"--accept-permission-claim", "secrets.core",
		"--accept-permission-claim", "namespaces.core",
		"--accept-permission-claim", "roles.rbac.authorization.k8s.io",
		"--accept-permission-claim", "rolebindings.rbac.authorization.k8s.io",
		"--accept-permission-claim", "apiresourceschemas.apis.kcp.io",
		"--ignore-existing",
	}

	cmd := exec.Command("kubectl", args...)
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to bind APIExport: %s", output)
}

func createExampleResources(t *testing.T, providerConfig string) {
	env := append(os.Environ(), "KUBECONFIG="+providerConfig)

	// Create APIExport
	cmd := exec.Command("kubectl", "apply", "-f", "../../deploy/examples/apiexport.yaml")
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create APIExport: %s", output)

	// Create APIResourceSchema
	cmd = exec.Command("kubectl", "apply", "-f", "../../deploy/examples/apiresourceschema.yaml")
	cmd.Env = env
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create APIResourceSchema: %s", output)

	// Recursive bind
	cmd = exec.Command("kubectl", "kcp", "bind", "apiexport", "root:provider:cowboys-stable", "--ignore-existing")
	cmd.Env = env
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Failed to perform recursive bind: %s", output)
}

func getLogicalClusterURL(t *testing.T, providerConfig string) string {
	env := append(os.Environ(), "KUBECONFIG="+providerConfig)

	var clusterURL string
	err := wait.PollImmediate(kcpPollingInterval, 5*time.Minute, func() (bool, error) {
		cmd := exec.Command("kubectl", "get", "logicalcluster", "-o", "jsonpath='{.items[0].status.URL}'")
		cmd.Env = env
		output, err := cmd.Output()
		if err != nil {
			t.Logf("Waiting for LogicalCluster URL: %v", err)
			return false, nil
		}

		clusterURL = strings.TrimSpace(string(output))
		clusterURL = strings.Trim(clusterURL, "'")
		return clusterURL != "", nil
	})

	require.NoError(t, err, "Failed to get LogicalCluster URL")
	t.Logf("LogicalCluster URL: %s", clusterURL)
	return clusterURL
}

func setupConsumerWorkspace(t *testing.T, testDir string) string {
	consumerConfig := filepath.Join(testDir, "consumer.kubeconfig")
	copyFile(t, filepath.Join(testDir, "admin.kubeconfig"), consumerConfig)

	env := append(os.Environ(), "KUBECONFIG="+consumerConfig)

	// Switch to root and create consumer workspace
	cmd := exec.Command("kubectl", "ws", "use", ":root")
	cmd.Env = env
	require.NoError(t, cmd.Run(), "Failed to switch to root workspace")

	cmd = exec.Command("kubectl", "ws", "create", "consumer", "--ignore-existing")
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create consumer workspace: %s", output)

	cmd = exec.Command("kubectl", "ws", "use", "consumer")
	cmd.Env = env
	require.NoError(t, cmd.Run(), "Failed to switch to consumer workspace")

	return consumerConfig
}

func performBindingWithBrowser(t *testing.T, clusterURL, consumerConfig string) {
	// Extract cluster ID from URL for binding
	parts := strings.Split(clusterURL, "/")
	var clusterID string
	for i, part := range parts {
		if part == "clusters" && i+1 < len(parts) {
			clusterID = parts[i+1]
			break
		}
	}
	require.NotEmpty(t, clusterID, "Failed to extract cluster ID from URL: %s", clusterURL)

	bindURL := fmt.Sprintf("http://127.0.0.1:8080/clusters/%s/exports", clusterID)

	// Test binding dry run first (similar to happy-case test)
	t.Run("Service is bound dry run", func(t *testing.T) {
		authURLDryRunCh := make(chan string, 1)
		go simulateKCPBrowser(t, authURLDryRunCh, "cowboys")

		iostreams, _, bufOut, _ := genericclioptions.NewTestIOStreams()
		framework.Bind(t, iostreams, authURLDryRunCh, nil, bindURL, "--kubeconfig", consumerConfig, "--skip-konnector", "--dry-run")
		_, err := yaml.YAMLToJSON(bufOut.Bytes())
		require.NoError(t, err, "Generated output is not valid YAML")
	})
	// Perform actual binding (similar to happy-case test)
	t.Run("Service is bound", func(t *testing.T) {
		authURLCh := make(chan string, 1)
		go simulateKCPBrowser(t, authURLCh, "cowboys")

		iostreams, _, _, _ := genericclioptions.NewTestIOStreams()
		invocations := make(chan framework.SubCommandInvocation, 1)
		framework.Bind(t, iostreams, authURLCh, invocations, bindURL, "--kubeconfig", consumerConfig, "--skip-konnector")
		inv := <-invocations
		requireEqualSlicePattern(t, []string{"apiservice", "--remote-kubeconfig-namespace", "*", "--remote-kubeconfig-name", "*", "-f", "-", "--kubeconfig=" + consumerConfig, "--skip-konnector=true", "--no-banner"}, inv.Args)
		framework.BindAPIService(t, inv.Stdin, "", inv.Args...)

		// Wait for CRD to be created on consumer side
		t.Logf("Waiting for cowboy CRD to be created on consumer side")
		crdClient := framework.ApiextensionsClient(t, createConfigFromFile(t, consumerConfig)).ApiextensionsV1().CustomResourceDefinitions()
		require.Eventually(t, func() bool {
			_, err := crdClient.Get(context.Background(), "cowboys.wildwest.dev", metav1.GetOptions{})
			return err == nil
		}, 5*time.Minute, 5*time.Second, "waiting for cowboys CRD to be created on consumer side")
	})
}

// verifyAPIServiceBindingsCRD waits for APIServiceBindings CRD to be available
func verifyAPIServiceBindingsCRD(t *testing.T, kubeconfig string) {
	crdClient := framework.ApiextensionsClient(t, createConfigFromFile(t, kubeconfig)).ApiextensionsV1().CustomResourceDefinitions()

	require.Eventually(t, func() bool {
		_, err := crdClient.Get(context.Background(), "apiservicebindings.kube-bind.io", metav1.GetOptions{})
		if err != nil {
			t.Logf("APIServiceBindings CRD not ready yet: %v", err)
			return false
		}
		return true
	}, 2*time.Minute, 5*time.Second, "APIServiceBindings CRD should be available")

	t.Log("APIServiceBindings CRD is available")
}

func testKCPResourceSync(t *testing.T, consumerConfig, providerConfig string) {
	ctx := context.Background()
	serviceGVR := schema.GroupVersionResource{Group: "wildwest.dev", Version: "v1alpha1", Resource: "cowboys"}

	// Create clients
	consumerClient := createDynamicClient(t, consumerConfig).Resource(serviceGVR)
	providerClient := createDynamicClient(t, providerConfig).Resource(serviceGVR)

	cowboyInstance := `
apiVersion: wildwest.dev/v1alpha1
kind: Cowboy
metadata:
  name: test-cowboy
spec:
  intent: "draw"
`

	// Run structured tests similar to happy-case test
	for _, tc := range []struct {
		name string
		step func(t *testing.T)
	}{
		{
			name: "instance created downstream syncs upstream",
			step: func(t *testing.T) {
				t.Logf("Creating cowboy instance on consumer side")

				require.Eventually(t, func() bool {
					_, err := consumerClient.Namespace("default").Create(ctx, toUnstructured(t, cowboyInstance), metav1.CreateOptions{})
					return err == nil
				}, 2*time.Minute, 5*time.Second, "waiting for cowboy instance to be created on consumer side")

				t.Logf("Waiting for cowboy instance to be synced to provider side")
				var instances *unstructured.UnstructuredList
				require.Eventually(t, func() bool {
					var err error
					instances, err = providerClient.List(ctx, metav1.ListOptions{})
					return err == nil && len(instances.Items) >= 1
				}, 5*time.Minute, 5*time.Second, "waiting for cowboy instance to be synced to provider side")

				t.Logf("Found %d cowboy instances on provider side", len(instances.Items))
			},
		},
		{
			name: "instance spec updated downstream syncs upstream",
			step: func(t *testing.T) {
				t.Logf("Updating cowboy spec on consumer side")

				require.Eventually(t, func() bool {
					obj, err := consumerClient.Namespace("default").Get(ctx, "test-cowboy", metav1.GetOptions{})
					if err != nil {
						return false
					}

					unstructured.SetNestedField(obj.Object, "holster", "spec", "intent") //nolint:errcheck
					_, err = consumerClient.Namespace("default").Update(ctx, obj, metav1.UpdateOptions{})
					return err == nil
				}, 2*time.Minute, 5*time.Second, "waiting for cowboy spec to be updated on consumer side")

				t.Logf("Waiting for cowboy spec update to sync to provider side")
				require.Eventually(t, func() bool {
					instances, err := providerClient.List(ctx, metav1.ListOptions{})
					if err != nil || len(instances.Items) == 0 {
						return false
					}

					intent, found, err := unstructured.NestedString(instances.Items[0].Object, "spec", "intent")
					return err == nil && found && intent == "holster"
				}, 5*time.Minute, 5*time.Second, "waiting for cowboy spec update to sync to provider side")
			},
		},
		{
			name: "instance deleted downstream is deleted upstream",
			step: func(t *testing.T) {
				t.Logf("Deleting cowboy instance on consumer side")

				err := consumerClient.Namespace("default").Delete(ctx, "test-cowboy", metav1.DeleteOptions{})
				require.NoError(t, err, "Failed to delete cowboy on consumer side")

				t.Logf("Waiting for cowboy instance to be deleted on provider side")
				require.Eventually(t, func() bool {
					instances, err := providerClient.List(ctx, metav1.ListOptions{})
					return err == nil && len(instances.Items) == 0
				}, 5*time.Minute, 5*time.Second, "waiting for cowboy instance to be deleted on provider side")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.step(t)
		})
	}

	t.Log("KCP resource synchronization tests passed!")
}

// simulateKCPBrowser simulates browser interaction for KCP binding
func simulateKCPBrowser(t *testing.T, authURLCh chan string, resource string) {
	browser := surf.NewBrowser()
	authURL := <-authURLCh

	t.Logf("Browsing to auth URL: %s", authURL)
	err := browser.Open(authURL)
	require.NoError(t, err, "Failed to open auth URL")

	t.Logf("Waiting for browser to be at /resources")
	framework.BrowserEventuallyAtPath(t, browser, "/resources")

	t.Logf("Clicking %s resource", resource)
	err = browser.Click("a." + resource)
	require.NoError(t, err, "Failed to click resource link")

	t.Logf("Waiting for browser to be forwarded to client")
	framework.BrowserEventuallyAtPath(t, browser, "/callback")
}

// createConfigFromFile creates a rest.Config from a kubeconfig file path
func createConfigFromFile(t *testing.T, kubeconfigPath string) *rest.Config {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	require.NoError(t, err, "Failed to build config from kubeconfig file")
	return config
}

// toUnstructured converts YAML manifest to unstructured object (from happy-case test)
func toUnstructured(t *testing.T, manifest string) *unstructured.Unstructured {
	t.Helper()

	obj := map[string]any{}
	err := yaml.Unmarshal([]byte(manifest), &obj)
	require.NoError(t, err, "Failed to unmarshal YAML manifest")

	return &unstructured.Unstructured{Object: obj}
}

// requireEqualSlicePattern matches slice patterns like the happy-case test
func requireEqualSlicePattern(t *testing.T, pattern []string, slice []string) {
	t.Helper()

	require.Equal(t, len(pattern), len(slice), "slice length doesn't match pattern length\n     got: %s\nexpected: %s", strings.Join(slice, " "), strings.Join(pattern, " "))

	for i, s := range slice {
		if pattern[i] == "*" {
			continue
		}
		require.Equal(t, pattern[i], s, "slice doesn't match pattern at index %d\n     got: %s\nexpected: %s", i, strings.Join(slice, " "), strings.Join(pattern, " "))
	}
}

func copyFile(t *testing.T, src, dst string) {
	data, err := os.ReadFile(src)
	require.NoError(t, err, "Failed to read source file: %s", src)

	err = os.WriteFile(dst, data, 0644)
	require.NoError(t, err, "Failed to write destination file: %s", dst)
}

// Helper function to create a dynamic client from kubeconfig
func createDynamicClient(t *testing.T, kubeconfigPath string) dynamic.Interface {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	require.NoError(t, err, "Failed to build config from kubeconfig")

	client, err := dynamic.NewForConfig(config)
	require.NoError(t, err, "Failed to create dynamic client")

	return client
}
