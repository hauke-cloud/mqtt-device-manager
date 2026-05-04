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

const (
	deviceTypeTasmota = "tasmota"
)

// Manager handles MQTT connections to bridges for device management
type Manager struct {
	client           client.Client
	log              *zap.Logger
	bridges          map[string]*BridgeConnection
	mu               sync.RWMutex
	stateHandler     *StateHandler
	discoveryHandler *DiscoveryHandler
}

// BridgeConnection represents a single MQTT bridge connection
type BridgeConnection struct {
	bridge          *iotv1alpha1.MQTTBridge
	mqttClient      mqtt.Client
	connected       bool
	lastSeen        time.Time
	mu              sync.RWMutex
	discoveryTicker *time.Ticker
	stopDiscovery   chan struct{}
}

// NewManager creates a new MQTT manager
func NewManager(c client.Client, log *zap.Logger) *Manager {
	m := &Manager{
		client:  c,
		log:     log,
		bridges: make(map[string]*BridgeConnection),
	}
	m.stateHandler = NewStateHandler(c, log.With(zap.String("handler", "state")))
	m.discoveryHandler = NewDiscoveryHandler(c, log.With(zap.String("handler", "discovery")), m)
	return m
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
	opts.SetKeepAlive(30 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetCleanSession(false) // Maintain subscriptions across reconnects

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

	bridgeConn := &BridgeConnection{
		bridge:        bridge,
		stopDiscovery: make(chan struct{}),
	}

	// Connection handlers
	// Note: We use context.Background() here because the handlers are called on reconnect
	// and the original ctx might be canceled by then
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		m.onConnect(context.Background(), bridgeConn)
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		m.onConnectionLost(bridgeConn, err)
	})

	// Create and connect client
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	// Store connection
	bridgeConn.mqttClient = mqttClient
	bridgeConn.connected = true
	bridgeConn.lastSeen = time.Now()
	m.bridges[key] = bridgeConn

	m.log.Info("Successfully connected to MQTT bridge",
		zap.String("bridge", key),
		zap.String("deviceType", bridge.Spec.DeviceType))

	return nil
}

// onConnect is called when connection to MQTT broker is established
func (m *Manager) onConnect(ctx context.Context, conn *BridgeConnection) {
	conn.mu.Lock()
	conn.connected = true
	conn.lastSeen = time.Now()
	conn.mu.Unlock()

	m.log.Info("MQTT connection established",
		zap.String("bridge", conn.bridge.Name),
		zap.String("component", "mqtt"))

	// Subscribe to topics
	m.subscribeToTopics(ctx, conn)

	// Start periodic device discovery for Tasmota bridges
	if conn.bridge.Spec.DeviceType == deviceTypeTasmota {
		m.startPeriodicDiscovery(conn)
	}
}

// onConnectionLost is called when connection to MQTT broker is lost
func (m *Manager) onConnectionLost(conn *BridgeConnection, err error) {
	conn.mu.Lock()
	conn.connected = false
	conn.mu.Unlock()

	// Stop periodic discovery
	m.stopPeriodicDiscovery(conn)

	m.log.Error("MQTT connection lost",
		zap.String("bridge", conn.bridge.Name),
		zap.Error(err))
}

// subscribeToTopics subscribes to all relevant topics for device management
func (m *Manager) subscribeToTopics(ctx context.Context, conn *BridgeConnection) {
	// Check if Topics are configured
	if len(conn.bridge.Spec.Topics) > 0 {
		m.log.Info("Checking topics for subscriptions",
			zap.String("bridge", conn.bridge.Name),
			zap.Int("topicCount", len(conn.bridge.Spec.Topics)))

		for _, topicSub := range conn.bridge.Spec.Topics {
			// Subscribe to state, status, and result type topics for device management
			if topicSub.Type == "state" || topicSub.Type == "status" || topicSub.Type == "result" {
				m.subscribeToTopic(ctx, conn, &topicSub)
			} else {
				m.log.Debug("Skipping non-device-management topic",
					zap.String("topic", topicSub.Topic),
					zap.String("type", topicSub.Type))
			}
		}
		return
	}

	// Fallback: If no topics configured and device type is Tasmota, subscribe to default topics
	if conn.bridge.Spec.DeviceType == deviceTypeTasmota {
		bridgeName := conn.bridge.Spec.BridgeName
		devicePattern := "+"
		if bridgeName != "" {
			devicePattern = bridgeName
		}

		// Subscribe to STATE topic for bridge status
		stateTopic := fmt.Sprintf("stat/%s/STATE", devicePattern)
		m.subscribeToTopic(ctx, conn, &iotv1alpha1.TopicSubscription{
			Topic: stateTopic,
			Type:  "state",
		})
		m.log.Info("Subscribed to default Tasmota STATE topic",
			zap.String("topic", stateTopic))

		// Subscribe to RESULT topic for command responses (ZbStatus1, ZbStatus3)
		resultTopic := fmt.Sprintf("stat/%s/RESULT", devicePattern)
		m.subscribeToTopic(ctx, conn, &iotv1alpha1.TopicSubscription{
			Topic: resultTopic,
			Type:  "result",
		})
		m.log.Info("Subscribed to default Tasmota RESULT topic for discovery",
			zap.String("topic", resultTopic))
	}
}

// subscribeToTopic subscribes to a single MQTT topic
func (m *Manager) subscribeToTopic(ctx context.Context, conn *BridgeConnection, topicSub *iotv1alpha1.TopicSubscription) {
	// Check if client is still connected
	if conn.mqttClient == nil || !conn.mqttClient.IsConnected() {
		m.log.Debug("MQTT client not connected, skipping subscription",
			zap.String("topic", topicSub.Topic))
		return
	}

	qos := byte(0)
	if topicSub.QoS != nil {
		qos = byte(*topicSub.QoS)
	}

	handler := func(mqttClient mqtt.Client, msg mqtt.Message) {
		m.log.Info("MQTT message handler called",
			zap.String("topic", msg.Topic()),
			zap.String("type", topicSub.Type),
			zap.Int("payloadSize", len(msg.Payload())))
		// Use context.Background() for message handlers as they run asynchronously
		m.handleMessage(context.Background(), conn, topicSub, msg)
	}

	if token := conn.mqttClient.Subscribe(topicSub.Topic, qos, handler); token.Wait() && token.Error() != nil {
		m.log.Error("Failed to subscribe to topic",
			zap.String("topic", topicSub.Topic),
			zap.String("type", topicSub.Type),
			zap.Error(token.Error()))
	} else {
		m.log.Info("Subscribed to topic",
			zap.String("topic", topicSub.Topic),
			zap.String("type", topicSub.Type),
			zap.Int("qos", int(qos)))
	}
}

// handleMessage processes incoming MQTT state messages
func (m *Manager) handleMessage(ctx context.Context, conn *BridgeConnection, topicSub *iotv1alpha1.TopicSubscription, msg mqtt.Message) {
	m.log.Info("Received MQTT message",
		zap.String("topic", msg.Topic()),
		zap.String("type", topicSub.Type),
		zap.String("bridge", conn.bridge.Name),
		zap.Int("payloadSize", len(msg.Payload())))

	// Handle messages based on topic type
	switch conn.bridge.Spec.DeviceType {
	case deviceTypeTasmota:
		// Route status/result messages to discovery handler (ZbStatus1, ZbStatus3)
		if topicSub.Type == "status" || topicSub.Type == "result" {
			if err := m.discoveryHandler.HandleMessage(ctx, conn.bridge.Namespace, conn.bridge.Name, msg.Topic(), msg.Payload()); err != nil {
				m.log.Error("Failed to handle discovery message",
					zap.String("topic", msg.Topic()),
					zap.Error(err))
			}
		}

		// Also route state messages to state handler for bridge status updates
		if topicSub.Type == "state" {
			if err := m.stateHandler.HandleMessage(ctx, conn.bridge.Namespace, conn.bridge.Name, msg.Topic(), msg.Payload()); err != nil {
				m.log.Error("Failed to handle state message",
					zap.String("topic", msg.Topic()),
					zap.Error(err))
			}
		}
	default:
		m.log.Debug("Device type not supported for state handling",
			zap.String("deviceType", conn.bridge.Spec.DeviceType))
	}
}

// Disconnect closes connection to an MQTT bridge
func (m *Manager) Disconnect(namespace, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%s", namespace, name)

	if conn, ok := m.bridges[key]; ok {
		// Stop periodic discovery
		m.stopPeriodicDiscovery(conn)

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

// TriggerDeviceDiscovery triggers device discovery for a Tasmota bridge
// Sends ZbStatus1 command to get list of devices
func (m *Manager) TriggerDeviceDiscovery(namespace, bridgeName string) error {
	m.log.Info("Triggering device discovery",
		zap.String("namespace", namespace),
		zap.String("bridge", bridgeName))

	// Send ZbStatus1 command to discover devices
	if err := m.PublishTasmotaCommand(namespace, bridgeName, "ZbStatus1", ""); err != nil {
		return fmt.Errorf("failed to trigger discovery: %w", err)
	}

	return nil
}

// startPeriodicDiscovery starts a periodic device discovery for a bridge
func (m *Manager) startPeriodicDiscovery(conn *BridgeConnection) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// Stop any existing discovery ticker
	if conn.discoveryTicker != nil {
		conn.discoveryTicker.Stop()
	}

	// Create a new ticker for 30 seconds
	conn.discoveryTicker = time.NewTicker(30 * time.Second)

	m.log.Info("Starting periodic device discovery",
		zap.String("bridge", conn.bridge.Name),
		zap.String("interval", "30s"))

	// Run discovery in a goroutine
	go func() {
		// Trigger initial discovery immediately
		if err := m.TriggerDeviceDiscovery(conn.bridge.Namespace, conn.bridge.Name); err != nil {
			m.log.Error("Failed to trigger initial device discovery",
				zap.String("bridge", conn.bridge.Name),
				zap.Error(err))
		}

		// Periodic discovery
		for {
			select {
			case <-conn.discoveryTicker.C:
				if err := m.TriggerDeviceDiscovery(conn.bridge.Namespace, conn.bridge.Name); err != nil {
					m.log.Error("Failed to trigger periodic device discovery",
						zap.String("bridge", conn.bridge.Name),
						zap.Error(err))
				}
			case <-conn.stopDiscovery:
				m.log.Info("Stopping periodic device discovery",
					zap.String("bridge", conn.bridge.Name))
				return
			}
		}
	}()
}

// stopPeriodicDiscovery stops the periodic device discovery for a bridge
func (m *Manager) stopPeriodicDiscovery(conn *BridgeConnection) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.discoveryTicker != nil {
		conn.discoveryTicker.Stop()
		conn.discoveryTicker = nil
	}

	// Signal the goroutine to stop
	select {
	case conn.stopDiscovery <- struct{}{}:
	default:
		// Channel already closed or no goroutine waiting
	}
}
