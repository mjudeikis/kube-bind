/*
Copyright 2024 The Kube Bind Authors.

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

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"

	kubebindv1alpha1 "github.com/kube-bind/kube-bind/sdk/apis/kubebind/v1alpha1"
)

// ClusterBindingLister helps list ClusterBindings.
// All objects returned here must be treated as read-only.
type ClusterBindingLister interface {
	// List lists all ClusterBindings in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*kubebindv1alpha1.ClusterBinding, err error)
	// ClusterBindings returns an object that can list and get ClusterBindings.
	ClusterBindings(namespace string) ClusterBindingNamespaceLister
	ClusterBindingListerExpansion
}

// clusterBindingLister implements the ClusterBindingLister interface.
type clusterBindingLister struct {
	listers.ResourceIndexer[*kubebindv1alpha1.ClusterBinding]
}

// NewClusterBindingLister returns a new ClusterBindingLister.
func NewClusterBindingLister(indexer cache.Indexer) ClusterBindingLister {
	return &clusterBindingLister{listers.New[*kubebindv1alpha1.ClusterBinding](indexer, kubebindv1alpha1.Resource("clusterbinding"))}
}

// ClusterBindings returns an object that can list and get ClusterBindings.
func (s *clusterBindingLister) ClusterBindings(namespace string) ClusterBindingNamespaceLister {
	return clusterBindingNamespaceLister{listers.NewNamespaced[*kubebindv1alpha1.ClusterBinding](s.ResourceIndexer, namespace)}
}

// ClusterBindingNamespaceLister helps list and get ClusterBindings.
// All objects returned here must be treated as read-only.
type ClusterBindingNamespaceLister interface {
	// List lists all ClusterBindings in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*kubebindv1alpha1.ClusterBinding, err error)
	// Get retrieves the ClusterBinding from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*kubebindv1alpha1.ClusterBinding, error)
	ClusterBindingNamespaceListerExpansion
}

// clusterBindingNamespaceLister implements the ClusterBindingNamespaceLister
// interface.
type clusterBindingNamespaceLister struct {
	listers.ResourceIndexer[*kubebindv1alpha1.ClusterBinding]
}
