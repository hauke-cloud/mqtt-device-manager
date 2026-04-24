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
"fmt"

"go.uber.org/zap"
"k8s.io/apimachinery/pkg/api/errors"
"k8s.io/apimachinery/pkg/runtime"
ctrl "sigs.k8s.io/controller-runtime"
"sigs.k8s.io/controller-runtime/pkg/client"

iotv1alpha1 "github.com/hauke-cloud/kubernetes-iot-api/api/v1alpha1"
"github.com/hauke-cloud/mqtt-device-manager/internal/mqtt"
)

const (
deviceFriendlyNameAnnotation = "iot.hauke.cloud/last-synced-friendly-name"
)

// DeviceReconciler reconciles a Device object
type DeviceReconciler struct {
client.Client
Scheme      *runtime.Scheme
Log         *zap.Logger
MQTTManager *mqtt.Manager
}

// +kubebuilder:rbac:groups=iot.hauke.cloud,resources=devices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=iot.hauke.cloud,resources=devices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=iot.hauke.cloud,resources=devices/finalizers,verbs=update
// +kubebuilder:rbac:groups=iot.hauke.cloud,resources=mqttbridges,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles Device CR changes and syncs friendly name to Tasmota
func (r *DeviceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
log := r.Log.With(
zap.String("namespace", req.Namespace),
zap.String("name", req.Name))

log.Debug("Reconciling Device")

// Fetch the Device instance
device := &iotv1alpha1.Device{}
err := r.Get(ctx, req.NamespacedName, device)
if err != nil {
if errors.IsNotFound(err) {
// Object not found, could have been deleted
log.Debug("Device not found, likely deleted")
return ctrl.Result{}, nil
}
log.Error("Failed to get Device", zap.Error(err))
return ctrl.Result{}, err
}

// Skip if device is disabled
if device.Spec.Disabled {
log.Debug("Device is disabled, skipping")
return ctrl.Result{}, nil
}

// Check if friendly name has changed and sync to Tasmota
if err := r.syncFriendlyNameToTasmota(ctx, device, log); err != nil {
log.Error("Failed to sync friendly name to Tasmota", zap.Error(err))
// Don't return error - we'll retry on next reconciliation
}

return ctrl.Result{}, nil
}

// syncFriendlyNameToTasmota syncs the friendly name from Device CR to Tasmota
func (r *DeviceReconciler) syncFriendlyNameToTasmota(ctx context.Context, device *iotv1alpha1.Device, log *zap.Logger) error {
// Check if friendly name is set
if device.Spec.FriendlyName == "" {
log.Debug("No friendly name set, skipping sync")
return nil
}

// Get the bridge reference
bridgeNamespace := device.Spec.BridgeRef.Namespace
if bridgeNamespace == "" {
bridgeNamespace = device.Namespace
}
bridgeName := device.Spec.BridgeRef.Name

// Fetch the bridge to check device type
bridge := &iotv1alpha1.MQTTBridge{}
if err := r.Get(ctx, client.ObjectKey{
Namespace: bridgeNamespace,
Name:      bridgeName,
}, bridge); err != nil {
return fmt.Errorf("failed to get bridge: %w", err)
}

// Only sync for Tasmota bridges
if bridge.Spec.DeviceType != "tasmota" {
log.Debug("Bridge is not Tasmota type, skipping friendly name sync",
zap.String("deviceType", bridge.Spec.DeviceType))
return nil
}

// Check if friendly name has changed since last sync
lastSyncedName := device.Annotations[deviceFriendlyNameAnnotation]
if lastSyncedName == device.Spec.FriendlyName {
log.Debug("Friendly name unchanged, skipping sync",
zap.String("friendlyName", device.Spec.FriendlyName))
return nil
}

// Sync friendly name to Tasmota via ZbName command
// Format: <IEEE Address>,<new name>
payload := fmt.Sprintf("%s,%s", device.Spec.IEEEAddr, device.Spec.FriendlyName)

log.Info("Syncing friendly name to Tasmota",
zap.String("ieeeAddr", device.Spec.IEEEAddr),
zap.String("friendlyName", device.Spec.FriendlyName),
zap.String("bridge", bridgeName))

if err := r.MQTTManager.PublishTasmotaCommand(
bridgeNamespace,
bridgeName,
"ZbName",
payload,
); err != nil {
return fmt.Errorf("failed to publish ZbName command: %w", err)
}

// Update annotation to track last synced name
if device.Annotations == nil {
device.Annotations = make(map[string]string)
}
device.Annotations[deviceFriendlyNameAnnotation] = device.Spec.FriendlyName

if err := r.Update(ctx, device); err != nil {
return fmt.Errorf("failed to update device annotations: %w", err)
}

log.Info("Successfully synced friendly name to Tasmota",
zap.String("ieeeAddr", device.Spec.IEEEAddr),
zap.String("friendlyName", device.Spec.FriendlyName))

return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DeviceReconciler) SetupWithManager(mgr ctrl.Manager) error {
return ctrl.NewControllerManagedBy(mgr).
For(&iotv1alpha1.Device{}).
Named("device").
Complete(r)
}
