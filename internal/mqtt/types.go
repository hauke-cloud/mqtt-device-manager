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
	"regexp"
	"strings"
)

// StatusMessage represents a Tasmota status/result message
type StatusMessage struct {
	// ZbStatus1 contains device list from initial discovery
	ZbStatus1 []ZbStatus1DeviceEntry `json:"ZbStatus1,omitempty"`

	// ZbStatus3 contains detailed device information
	ZbStatus3 []ZbStatus3DeviceEntry `json:"ZbStatus3,omitempty"`

	// ZbSend contains command response
	ZbSend *ZbSendResult `json:"ZbSend,omitempty"`

	// ZbName contains naming response
	ZbName *ZbNameResult `json:"ZbName,omitempty"`
}

// ZbStatus1DeviceEntry represents a device entry in ZbStatus1 response
type ZbStatus1DeviceEntry struct {
	Device string `json:"Device"` // Short address (e.g., "0x4F2E")
	Name   string `json:"Name"`   // Friendly name
}

// ZbStatus3DeviceEntry represents a device entry in ZbStatus3 response
type ZbStatus3DeviceEntry struct {
	Device        string   `json:"Device"`                  // Short address (e.g., "0x4F2E")
	Name          string   `json:"Name"`                    // Friendly name
	IEEEAddr      string   `json:"IEEEAddr"`                // Full 64-bit IEEE address (e.g., "0xF4B3B1FFFE4EA459")
	ModelId       string   `json:"ModelId,omitempty"`       // Device model
	Manufacturer  string   `json:"Manufacturer,omitempty"`  // Manufacturer
	Endpoints     []int    `json:"Endpoints,omitempty"`     // Available endpoints
	Config        []string `json:"Config,omitempty"`        // Device configuration
	Power         *int     `json:"Power,omitempty"`         // Power state
	Reachable     *bool    `json:"Reachable,omitempty"`     // Device reachability
	LastSeen      *int     `json:"LastSeen,omitempty"`      // Seconds since last seen
	LastSeenEpoch *int64   `json:"LastSeenEpoch,omitempty"` // Last seen Unix timestamp
	LinkQuality   *int     `json:"LinkQuality,omitempty"`   // Link quality 0-255
}

// ZbSendResult represents the result of a ZbSend command
type ZbSendResult struct {
	Device  string                 `json:"Device"`
	Name    string                 `json:"Name,omitempty"`
	Command string                 `json:"Command,omitempty"`
	Status  string                 `json:"Status,omitempty"`
	Data    map[string]interface{} `json:"Data,omitempty"`
}

// ZbNameResult represents the result of a ZbName command
type ZbNameResult struct {
	Device string `json:"Device"`
	Name   string `json:"Name"`
}

// sanitizeDeviceName converts an IEEE address to a valid Kubernetes resource name
func sanitizeDeviceName(ieeeAddr string) string {
	// Remove "0x" prefix if present
	name := strings.TrimPrefix(strings.ToLower(ieeeAddr), "0x")
	// Replace any non-alphanumeric characters
	name = regexp.MustCompile(`[^a-z0-9-]`).ReplaceAllString(name, "-")
	// Ensure it starts with alphanumeric
	name = "device-" + name
	// Truncate if too long (max 253 characters for k8s names)
	if len(name) > 253 {
		name = name[:253]
	}
	return name
}

// sanitizeLabel converts a value to a valid Kubernetes label value
func sanitizeLabel(value string) string {
	// Remove "0x" prefix if present
	label := strings.TrimPrefix(strings.ToLower(value), "0x")
	// Labels can only contain alphanumeric, '-', '_', and '.'
	label = regexp.MustCompile(`[^a-z0-9._-]`).ReplaceAllString(label, "-")
	// Labels must be <= 63 characters
	if len(label) > 63 {
		label = label[:63]
	}
	// Labels must start and end with alphanumeric
	label = strings.Trim(label, "-_.")
	return label
}
