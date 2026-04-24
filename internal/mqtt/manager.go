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
"crypto/tls"
"fmt"
"sync"
"time"

mqtt "github.com/eclipse/paho.mqtt.golang"
"go.uber.org/zap"
corev1 "k8s.io/api/core/v1"
"k8s.io/apimachinery/pkg/types"
"sigs.k8s.io/controller-runtime/pkg/client"

iotv1alpha1 "github.com/hauke-cloud/kubernetes-iot-api/api/v1alpha1"
)

// Manager handles MQTT connections to bridges for device management
type Manager struct {
client  client.Client
log     *zap.Logger
bridges map[string]*BridgeConnection
mu      sync.RWMutex
}

// BridgeConnection represents a single MQTT bridge connection
type BridgeConnection struct {
bridge     *iotv1alpha1.MQTTBridge
mqttClient mqtt.Client
connected  bool
lastSeen   time.Time
mu         sync.RWMutex
}

// NewManager creates a new MQTT manager
func NewManager(c client.Client, log *zap.Logger) *Manager {
return &Manager{
client:  c,
log:     log,
bridges: make(map[string]*BridgeConnection),
}
}

// Connect establishes connection to an MQTT bridge
func (m *Manager) Connect(ctx context.Context, bridge *iotv1alpha1.MQTTBridge) error {
m.mu.Lock()
defer m.mu.Unlock()

key := fmt.Sprintf("%s/%s", bridge.Namespace, bridge.Name)

m.log.Info("Connecting to MQTT bridge",
zap.String("bridge", key),
zap.String("host", bridge.Spec.Host),
zap.Int32("port", bridge.Spec.Port))

// Disconnect existing connection if any
if existing, ok := m.bridges[key]; ok {
if existing.mqttClient != nil && existing.mqttClient.IsConnected() {
m.log.Info("Disconnecting existing connection", zap.String("bridge", key))
existing.mqttClient.Disconnect(250)
}
}

// Get credentials from secret if specified
username := ""
password := ""
if bridge.Spec.CredentialsSecretRef != nil {
secret := &corev1.Secret{}
if err := m.client.Get(ctx, types.NamespacedName{
Name:      bridge.Spec.CredentialsSecretRef.Name,
Namespace: bridge.Namespace,
}, secret); err != nil {
return fmt.Errorf("failed to get credentials secret: %w", err)
}

if u, ok := secret.Data["username"]; ok {
username = string(u)
}
if p, ok := secret.Data["password"]; ok {
password = string(p)
}
}

// Create MQTT client options
opts := mqtt.NewClientOptions()
broker := fmt.Sprintf("tcp://%s:%d", bridge.Spec.Host, bridge.Spec.Port)
opts.AddBroker(broker)
opts.SetClientID(fmt.Sprintf("mqtt-device-manager-%s", bridge.Name))
opts.SetUsername(username)
opts.SetPassword(password)
opts.SetAutoReconnect(true)
opts.SetConnectRetry(true)
opts.SetMaxReconnectInterval(1 * time.Minute)

// TLS configuration
if bridge.Spec.TLS != nil && bridge.Spec.TLS.Enabled {
tlsConfig := &tls.Config{
InsecureSkipVerify: bridge.Spec.TLS.InsecureSkipVerify,
}
opts.SetTLSConfig(tlsConfig)

// Update broker URL for TLS
broker = fmt.Sprintf("ssl://%s:%d", bridge.Spec.Host, bridge.Spec.Port)
opts.Servers = nil
opts.AddBroker(broker)
}

// Connection handlers
opts.SetOnConnectHandler(func(c mqtt.Client) {
m.log.Info("Connected to MQTT broker", zap.String("bridge", key))
})

opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
m.log.Warn("Lost connection to MQTT broker",
zap.String("bridge", key),
zap.Error(err))
})

// Create and connect client
mqttClient := mqtt.NewClient(opts)
if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
}

// Store connection
m.bridges[key] = &BridgeConnection{
bridge:     bridge,
mqttClient: mqttClient,
connected:  true,
lastSeen:   time.Now(),
}

m.log.Info("Successfully connected to MQTT bridge",
zap.String("bridge", key),
zap.String("deviceType", bridge.Spec.DeviceType))

return nil
}

// Disconnect closes connection to an MQTT bridge
func (m *Manager) Disconnect(namespace, name string) {
m.mu.Lock()
defer m.mu.Unlock()

key := fmt.Sprintf("%s/%s", namespace, name)

if conn, ok := m.bridges[key]; ok {
if conn.mqttClient != nil && conn.mqttClient.IsConnected() {
m.log.Info("Disconnecting from MQTT bridge", zap.String("bridge", key))
conn.mqttClient.Disconnect(250)
}
delete(m.bridges, key)
}
}

// PublishTasmotaCommand publishes a command to a Tasmota bridge
// Topic format: cmnd/<bridgeName>/<command>
func (m *Manager) PublishTasmotaCommand(namespace, bridgeName, command, payload string) error {
m.mu.RLock()
key := fmt.Sprintf("%s/%s", namespace, bridgeName)
conn, ok := m.bridges[key]
m.mu.RUnlock()

if !ok || conn.mqttClient == nil {
return fmt.Errorf("bridge not connected: %s", key)
}

if conn.bridge.Spec.DeviceType != "tasmota" {
return fmt.Errorf("bridge is not a Tasmota device type: %s", conn.bridge.Spec.DeviceType)
}

tasmotaBridgeName := conn.bridge.Spec.BridgeName
if tasmotaBridgeName == "" {
return fmt.Errorf("bridgeName not configured for Tasmota bridge")
}

topic := fmt.Sprintf("cmnd/%s/%s", tasmotaBridgeName, command)

token := conn.mqttClient.Publish(topic, 0, false, []byte(payload))
if token.Wait() && token.Error() != nil {
return fmt.Errorf("failed to publish command: %w", token.Error())
}

m.log.Info("Published Tasmota command",
zap.String("command", command),
zap.String("topic", topic),
zap.String("payload", payload),
zap.String("bridge", bridgeName))

return nil
}
