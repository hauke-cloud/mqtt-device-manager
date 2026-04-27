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

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	iotv1alpha1 "github.com/hauke-cloud/kubernetes-iot-api/api/v1alpha1"
)

// StateHandler processes Tasmota state messages
type StateHandler struct {
	client client.Client
	log    *zap.Logger
}

// NewStateHandler creates a new state handler
func NewStateHandler(c client.Client, log *zap.Logger) *StateHandler {
	return &StateHandler{
		client: c,
		log:    log,
	}
}

// StateMessage represents a Tasmota STATE message
type StateMessage struct {
	Time   string      `json:"Time"`
	Uptime string      `json:"Uptime"`
	Wifi   *WifiStatus `json:"Wifi,omitempty"`
}

// WifiStatus represents WiFi status in Tasmota STATE message
type WifiStatus struct {
	RSSI int `json:"RSSI"`
}

// HandleMessage processes a state message
func (h *StateHandler) HandleMessage(ctx context.Context, bridgeNamespace, bridgeName, topic string, payload []byte) error {
	var msg StateMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		h.log.Error("Failed to parse state message",
			zap.String("topic", topic),
			zap.Error(err))
		return err
	}

	h.log.Debug("Processing state message",
		zap.String("bridge", bridgeName),
		zap.String("uptime", msg.Uptime))

	// Update MQTTBridge status
	return h.updateBridgeStatus(ctx, bridgeNamespace, bridgeName, &msg)
}

// updateBridgeStatus updates the MQTTBridge CR status
func (h *StateHandler) updateBridgeStatus(ctx context.Context, bridgeNamespace, bridgeName string, state *StateMessage) error {
	// Fetch the MQTTBridge CR
	bridge := &iotv1alpha1.MQTTBridge{}
	bridgeKey := types.NamespacedName{
		Name:      bridgeName,
		Namespace: bridgeNamespace,
	}

	if err := h.client.Get(ctx, bridgeKey, bridge); err != nil {
		return fmt.Errorf("failed to get bridge: %w", err)
	}

	// Update status
	now := metav1.Now()
	bridge.Status.ConnectionState = "Connected"
	bridge.Status.LastConnectedTime = &now

	// Add informational message
	if state.Wifi != nil {
		bridge.Status.Message = fmt.Sprintf("Connected (WiFi RSSI: %d)", state.Wifi.RSSI)
	} else {
		bridge.Status.Message = fmt.Sprintf("Connected (Uptime: %s)", state.Uptime)
	}

	// Update the status
	if err := h.client.Status().Update(ctx, bridge); err != nil {
		h.log.Error("Failed to update bridge status",
			zap.String("bridge", bridgeName),
			zap.Error(err))
		return err
	}

	h.log.Debug("Updated bridge status",
		zap.String("bridge", bridgeName),
		zap.String("uptime", state.Uptime))

	return nil
}
