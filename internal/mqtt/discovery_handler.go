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

package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	iotv1alpha1 "github.com/hauke-cloud/kubernetes-iot-api/api/v1alpha1"
)

// DiscoveryHandler processes Tasmota device discovery
type DiscoveryHandler struct {
	client      client.Client
	log         *zap.Logger
	mqttManager *Manager
	// pendingDevices tracks devices waiting for ZbStatus3 response
	// key: "namespace/bridge/shortAddr"
	pendingDevices map[string]*ZbStatus1DeviceEntry
	mu             sync.RWMutex
}

// NewDiscoveryHandler creates a new discovery handler
func NewDiscoveryHandler(c client.Client, log *zap.Logger, mqttManager *Manager) *DiscoveryHandler {
	return &DiscoveryHandler{
		client:         c,
		log:            log,
		mqttManager:    mqttManager,
		pendingDevices: make(map[string]*ZbStatus1DeviceEntry),
	}
}

// HandleMessage processes discovery messages (ZbStatus1 and ZbStatus3)
func (h *DiscoveryHandler) HandleMessage(ctx context.Context, namespace, bridgeName, topic string, payload []byte) error {
	h.log.Debug("Processing discovery message",
		zap.String("topic", topic),
		zap.String("bridge", bridgeName),
		zap.String("payload", string(payload)))

	var msg StatusMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		h.log.Error("Failed to parse discovery message",
			zap.String("topic", topic),
			zap.String("payload", string(payload)),
			zap.Error(err))
		return err
	}

	// Handle ZbStatus1 response (initial discovery with short addresses)
	if len(msg.ZbStatus1) > 0 {
		return h.handleZbStatus1(ctx, namespace, bridgeName, msg.ZbStatus1)
	}

	// Handle ZbStatus3 response (detailed info with full IEEE address)
	if len(msg.ZbStatus3) > 0 {
		return h.handleZbStatus3(ctx, namespace, bridgeName, msg.ZbStatus3)
	}

	// Log when message doesn't contain expected discovery data
	h.log.Debug("Discovery message does not contain ZbStatus1 or ZbStatus3",
		zap.String("topic", topic),
		zap.String("bridge", bridgeName),
		zap.String("payload", string(payload)))

	return nil
}

// handleZbStatus1 processes initial discovery response with short addresses
func (h *DiscoveryHandler) handleZbStatus1(_ context.Context, namespace, bridgeName string, devices []ZbStatus1DeviceEntry) error {
	h.log.Info("Processing ZbStatus1 discovery",
		zap.String("bridge", bridgeName),
		zap.Int("deviceCount", len(devices)))

	// For each device, request detailed info with ZbStatus3
	for _, device := range devices {
		shortAddr := device.Device

		// Store pending device
		key := fmt.Sprintf("%s/%s/%s", namespace, bridgeName, shortAddr)
		h.mu.Lock()
		h.pendingDevices[key] = &device
		h.mu.Unlock()

		// Request ZbStatus3 for this device to get full IEEE address
		if err := h.mqttManager.PublishTasmotaCommand(
			namespace,
			bridgeName,
			"ZbStatus3",
			shortAddr,
		); err != nil {
			h.log.Error("Failed to request ZbStatus3",
				zap.String("device", shortAddr),
				zap.Error(err))
			continue
		}

		h.log.Debug("Requested ZbStatus3 for device",
			zap.String("device", shortAddr),
			zap.String("bridge", bridgeName))
	}

	return nil
}

// handleZbStatus3 processes detailed device info with full IEEE address
func (h *DiscoveryHandler) handleZbStatus3(ctx context.Context, namespace, bridgeName string, devices []ZbStatus3DeviceEntry) error {
	h.log.Info("Processing ZbStatus3 response",
		zap.String("bridge", bridgeName),
		zap.Int("deviceCount", len(devices)))

	for _, device := range devices {
		if err := h.createOrUpdateDevice(ctx, namespace, bridgeName, &device); err != nil {
			h.log.Error("Failed to create/update device",
				zap.String("shortAddr", device.Device),
				zap.String("ieeeAddr", device.IEEEAddr),
				zap.Error(err))
			continue
		}

		// Remove from pending
		key := fmt.Sprintf("%s/%s/%s", namespace, bridgeName, device.Device)
		h.mu.Lock()
		delete(h.pendingDevices, key)
		h.mu.Unlock()
	}

	return nil
}

// createOrUpdateDevice creates or updates a Device CR with full device information
func (h *DiscoveryHandler) createOrUpdateDevice(ctx context.Context, namespace, bridgeName string, device *ZbStatus3DeviceEntry) error {
	// Use full 64-bit IEEE address as the stable identifier
	ieeeAddr := device.IEEEAddr
	if ieeeAddr == "" {
		return fmt.Errorf("device %s has no IEEE address", device.Device)
	}

	friendlyName := strings.TrimSpace(device.Name)

	// Generate Device CR name from full IEEE address
	deviceName := sanitizeDeviceName(bridgeName, ieeeAddr)

	h.log.Debug("Creating/updating device from ZbStatus3",
		zap.String("ieeeAddr", ieeeAddr),
		zap.String("shortAddr", device.Device),
		zap.String("friendlyName", friendlyName),
		zap.String("deviceName", deviceName))

	// Check if device already exists (by IEEE address label)
	deviceList := &iotv1alpha1.DeviceList{}
	if err := h.client.List(ctx, deviceList, client.InNamespace(namespace),
		client.MatchingLabels{
			"iot.hauke.cloud/ieee-addr": sanitizeLabel(ieeeAddr),
		}); err != nil {
		return fmt.Errorf("failed to list devices: %w", err)
	}

	var existingDevice *iotv1alpha1.Device
	if len(deviceList.Items) > 0 {
		existingDevice = &deviceList.Items[0]
	}

	if existingDevice != nil {
		// Device exists, update it
		return h.updateExistingDevice(ctx, existingDevice, device)
	}

	// Device doesn't exist, create it
	return h.createNewDevice(ctx, namespace, bridgeName, device, deviceName, ieeeAddr, friendlyName)
}

// updateExistingDevice updates an existing Device CR
func (h *DiscoveryHandler) updateExistingDevice(ctx context.Context, existing *iotv1alpha1.Device, device *ZbStatus3DeviceEntry) error {
	h.log.Debug("Updating existing device",
		zap.String("device", existing.Name),
		zap.String("ieeeAddr", device.IEEEAddr))

	updated := false

	// Update spec fields if needed
	if device.Name != "" && existing.Spec.FriendlyName != device.Name {
		existing.Spec.FriendlyName = device.Name
		updated = true
	}

	// Update status fields
	statusUpdated := false

	// Set short address from device key (e.g., "0x4F2E")
	if device.Device != "" && existing.Status.ShortAddr != device.Device {
		existing.Status.ShortAddr = device.Device
		statusUpdated = true
	}

	if device.ModelId != "" && existing.Status.ModelID != device.ModelId {
		existing.Status.ModelID = device.ModelId
		statusUpdated = true
	}

	if device.Manufacturer != "" && existing.Status.Manufacturer != device.Manufacturer {
		existing.Status.Manufacturer = device.Manufacturer
		statusUpdated = true
	}

	if device.Reachable != nil {
		reachable := *device.Reachable
		if existing.Status.Reachable == nil || *existing.Status.Reachable != reachable {
			existing.Status.Reachable = device.Reachable
			existing.Status.Available = reachable
			statusUpdated = true
		}
	}

	if device.LastSeen != nil {
		lastSeenSec := int32(*device.LastSeen)
		if existing.Status.LastSeenSeconds == nil || *existing.Status.LastSeenSeconds != lastSeenSec {
			existing.Status.LastSeenSeconds = &lastSeenSec
			statusUpdated = true
		}
	}

	if device.LastSeenEpoch != nil {
		if existing.Status.LastSeenEpoch == nil || *existing.Status.LastSeenEpoch != *device.LastSeenEpoch {
			existing.Status.LastSeenEpoch = device.LastSeenEpoch
			// Update LastSeen timestamp
			lastSeen := metav1.NewTime(time.Unix(*device.LastSeenEpoch, 0))
			existing.Status.LastSeen = &lastSeen
			statusUpdated = true
		}
	}

	if device.LinkQuality != nil {
		linkQuality := int32(*device.LinkQuality)
		if existing.Status.LinkQuality == nil || *existing.Status.LinkQuality != linkQuality {
			existing.Status.LinkQuality = &linkQuality
			statusUpdated = true
		}
	}

	if updated {
		if err := h.client.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update device spec: %w", err)
		}
		h.log.Info("Updated device spec",
			zap.String("device", existing.Name))
	}

	if statusUpdated {
		if err := h.client.Status().Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update device status: %w", err)
		}
		h.log.Info("Updated device status",
			zap.String("device", existing.Name))
	}

	return nil
}

// createNewDevice creates a new Device CR
func (h *DiscoveryHandler) createNewDevice(ctx context.Context, namespace, bridgeName string, device *ZbStatus3DeviceEntry, deviceName, ieeeAddr, friendlyName string) error {
	newDevice := &iotv1alpha1.Device{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deviceName,
			Namespace: namespace,
			Labels: map[string]string{
				"iot.hauke.cloud/bridge":      bridgeName,
				"iot.hauke.cloud/device-type": "tasmota-zigbee",
				"iot.hauke.cloud/ieee-addr":   sanitizeLabel(ieeeAddr),
			},
		},
		Spec: iotv1alpha1.DeviceSpec{
			BridgeRef: iotv1alpha1.BridgeReference{
				Name:      bridgeName,
				Namespace: namespace,
			},
			IEEEAddr:     ieeeAddr,
			FriendlyName: friendlyName,
		},
		Status: iotv1alpha1.DeviceStatus{
			ModelID:      device.ModelId,
			Manufacturer: device.Manufacturer,
		},
	}

	// Set optional status fields
	if device.Reachable != nil {
		newDevice.Status.Reachable = device.Reachable
		newDevice.Status.Available = *device.Reachable
	}

	if device.LastSeen != nil {
		lastSeenSec := int32(*device.LastSeen)
		newDevice.Status.LastSeenSeconds = &lastSeenSec
	}

	if device.LastSeenEpoch != nil {
		newDevice.Status.LastSeenEpoch = device.LastSeenEpoch
		lastSeen := metav1.NewTime(time.Unix(*device.LastSeenEpoch, 0))
		newDevice.Status.LastSeen = &lastSeen
	}

	if device.LinkQuality != nil {
		linkQuality := int32(*device.LinkQuality)
		newDevice.Status.LinkQuality = &linkQuality
	}

	if err := h.client.Create(ctx, newDevice); err != nil {
		return fmt.Errorf("failed to create device: %w", err)
	}

	h.log.Info("Created new device from discovery",
		zap.String("device", deviceName),
		zap.String("ieeeAddr", ieeeAddr),
		zap.String("shortAddr", device.Device),
		zap.String("friendlyName", friendlyName),
		zap.String("model", device.ModelId),
		zap.String("manufacturer", device.Manufacturer))

	return nil
}
