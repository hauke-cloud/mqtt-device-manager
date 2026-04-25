/*
Copyright 2026 hauke.cloud.

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

package crds

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// Install ensures all required CRDs are installed in the cluster
// It creates a direct client that doesn't rely on the manager's cache
func Install(ctx context.Context, log *zap.Logger) error {
	// Create a direct client that doesn't use a cache
	cfg, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubernetes config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add apiextensions to scheme: %w", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	log.Info("Installing/updating CRDs...")

	crdManifests := GetAll()
	decoder := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)

	for _, crdYAML := range crdManifests {
		// Decode YAML to unstructured object
		obj := &unstructured.Unstructured{}
		_, _, err := decoder.Decode(crdYAML, nil, obj)
		if err != nil {
			return fmt.Errorf("failed to decode CRD YAML: %w", err)
		}

		// Convert to CRD type
		crd := &apiextensionsv1.CustomResourceDefinition{}
		err = c.Scheme().Convert(obj, crd, nil)
		if err != nil {
			return fmt.Errorf("failed to convert to CRD: %w", err)
		}

		// Try to create or update the CRD
		existing := &apiextensionsv1.CustomResourceDefinition{}
		err = c.Get(ctx, client.ObjectKey{Name: crd.Name}, existing)
		if err != nil {
			if errors.IsNotFound(err) {
				// Create new CRD
				log.Info("Creating CRD", zap.String("name", crd.Name))
				if err := c.Create(ctx, crd); err != nil {
					return fmt.Errorf("failed to create CRD %s: %w", crd.Name, err)
				}
			} else {
				return fmt.Errorf("failed to get CRD %s: %w", crd.Name, err)
			}
		} else {
			// Update existing CRD
			log.Info("Updating CRD", zap.String("name", crd.Name))
			crd.ResourceVersion = existing.ResourceVersion
			if err := c.Update(ctx, crd); err != nil {
				return fmt.Errorf("failed to update CRD %s: %w", crd.Name, err)
			}
		}
	}

	log.Info("CRD installation completed", zap.Int("count", len(crdManifests)))
	return nil
}
