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

package controller

import (
	"context"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	iotv1alpha1 "github.com/hauke-cloud/kubernetes-iot-api/api/v1alpha1"
	"github.com/hauke-cloud/mqtt-device-manager/internal/mqtt"
)

// MQTTBridgeReconciler reconciles an MQTTBridge object
type MQTTBridgeReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Log         *zap.Logger
	MQTTManager *mqtt.Manager
}

// +kubebuilder:rbac:groups=iot.hauke.cloud,resources=mqttbridges,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile ensures mqtt-device-manager is connected to MQTT bridges
func (r *MQTTBridgeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.With(
		zap.String("namespace", req.Namespace),
		zap.String("name", req.Name))

	log.Debug("Reconciling MQTTBridge")

	// Fetch the MQTTBridge instance
	bridge := &iotv1alpha1.MQTTBridge{}
	err := r.Get(ctx, req.NamespacedName, bridge)
	if err != nil {
		if errors.IsNotFound(err) {
			// Bridge was deleted - disconnect
			log.Info("MQTTBridge deleted, disconnecting")
			r.MQTTManager.Disconnect(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		log.Error("Failed to get MQTTBridge", zap.Error(err))
		return ctrl.Result{}, err
	}

	// Connect to the bridge
	if err := r.MQTTManager.Connect(ctx, bridge); err != nil {
		log.Error("Failed to connect to MQTT bridge", zap.Error(err))
		return ctrl.Result{}, err
	}

	log.Info("Successfully connected to MQTT bridge",
		zap.String("host", bridge.Spec.Host),
		zap.String("deviceType", bridge.Spec.DeviceType))

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MQTTBridgeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&iotv1alpha1.MQTTBridge{}).
		Named("mqttbridge").
		Complete(r)
}
