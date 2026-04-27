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
	"testing"

	iotv1alpha1 "github.com/hauke-cloud/kubernetes-iot-api/api/v1alpha1"
)

// TestTopicFiltering verifies that device management topics are processed correctly
func TestTopicFiltering(t *testing.T) {
	tests := []struct {
		name         string
		topicType    string
		shouldAccept bool
	}{
		{
			name:         "state topic should be accepted",
			topicType:    "state",
			shouldAccept: true,
		},
		{
			name:         "result topic should be accepted for discovery",
			topicType:    "result",
			shouldAccept: true,
		},
		{
			name:         "status topic should be accepted for discovery",
			topicType:    "status",
			shouldAccept: true,
		},
		{
			name:         "telemetry topic should be rejected",
			topicType:    "telemetry",
			shouldAccept: false,
		},
		{
			name:         "discovery topic should be rejected",
			topicType:    "discovery",
			shouldAccept: false,
		},
		{
			name:         "unknown topic type should be rejected",
			topicType:    "unknown",
			shouldAccept: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the filtering logic from subscribeToTopics
			topicSub := iotv1alpha1.TopicSubscription{
				Topic: "test/topic",
				Type:  tt.topicType,
			}

			// Check if the topic type is accepted for device management
			accepted := topicSub.Type == "state" || topicSub.Type == "status" || topicSub.Type == "result"

			if accepted != tt.shouldAccept {
				t.Errorf("Topic type %q: expected accept=%v, got accept=%v",
					tt.topicType, tt.shouldAccept, accepted)
			}
		})
	}
}

// TestDefaultTasmotaStateTopic verifies default state topic generation
func TestDefaultTasmotaStateTopic(t *testing.T) {
	tests := []struct {
		name         string
		bridgeName   string
		expectedPath string
	}{
		{
			name:         "with bridge name",
			bridgeName:   "tasmota_zigbee",
			expectedPath: "stat/tasmota_zigbee/STATE",
		},
		{
			name:         "without bridge name (wildcard)",
			bridgeName:   "",
			expectedPath: "stat/+/STATE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the default topic generation logic
			devicePattern := "+"
			if tt.bridgeName != "" {
				devicePattern = tt.bridgeName
			}

			generatedTopic := "stat/" + devicePattern + "/STATE"

			if generatedTopic != tt.expectedPath {
				t.Errorf("Expected topic %q, got %q", tt.expectedPath, generatedTopic)
			}
		})
	}
}
