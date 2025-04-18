// MIT License
//
// Copyright (c) 2025 DaggerTech
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package config provides configuration management for the Polestar hub system.
// It handles loading and validation of configuration parameters with sensible defaults
// optimized for high-throughput message processing (>10,000 msg/sec).
package config

import (
	"encoding/json"
	"os"
	"strings"
)

// Config defines the operational parameters for the Polestar hub.
// Default values are optimized for high-throughput scenarios:
// - Queue Size: 1,000,000 messages for handling >10,000 msg/sec
// - Worker Count: 100 concurrent workers for parallel processing
// - Timeouts: Optimized for ~0.06ms average latency
type Config struct {
	IP                 string `json:"ip"`                   // IP address to bind the server to (e.g., "0.0.0.0" for all interfaces)
	Port               uint16 `json:"port"`                 // Port number to listen on (e.g., 24353)
	QueueSize          int    `json:"queue_size"`           // Size of message buffer (default: 1,000,000)
	QueueFullBehaviour string `json:"queue_full_behaviour"` // Behaviour when queue is full (default: "drop")
	WorkerCount        int    `json:"worker_count"`         // Number of concurrent workers (default: 100)
	DialTimeout        int    `json:"dial_timeout"`         // TCP connection timeout in ms (default: 1000)
	WriteTimeout       int    `json:"write_timeout"`        // Message write timeout in ms (default: 2000)
	ReadTimeout        int    `json:"read_timeout"`         // Message read timeout in ms (default: 30000)
	MaxRetries         int    `json:"max_retries"`          // Maximum message delivery attempts (default: 3)
	ClientQueueSize    int    `json:"client_queue_size"`    // Size of each client's message queue (default: 1000)
	ClientWorkerCount  int    `json:"client_worker_count"`  // Workers per client for message processing (default: 100)
	ClientDialTimeout  int    `json:"client_dial_timeout"`  // Client TCP connection timeout in ms (default: 1000)
	ClientWriteTimeout int    `json:"client_write_timeout"` // Client message write timeout in ms (default: 2000)
	ClientMaxRetries   int    `json:"client_max_retries"`   // Maximum client delivery attempts (default: 3)
}

// Load reads and parses the configuration file, applying performance-optimized
// defaults if values are not specified. The configuration file must be in the
// current working directory and must be named "config.json".
//
// Default values are tuned for high-throughput scenarios:
// - QueueSize: 1,000,000 messages to handle >10,000 msg/sec throughput
// - WorkerCount: 100 workers for optimal parallel processing
// - DialTimeout: 1000ms for connection establishment
// - WriteTimeout: 2000ms for message transmission
// - ReadTimeout: 30000ms for long-polling operations
//
// Returns:
//   - *Config: A fully initialized configuration with defaults applied
//   - error: nil if successful, or an error if the file is invalid or missing
func Load() (*Config, error) {
	b, err := os.ReadFile("config.json")
	if err != nil {
		return nil, err
	}
	var config Config
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000000
	}
	if config.WorkerCount <= 0 {
		config.WorkerCount = 100
	}
	if config.QueueFullBehaviour == "" {
		config.QueueFullBehaviour = "wait"
	}
	config.QueueFullBehaviour = strings.ToLower(config.QueueFullBehaviour)
	if config.DialTimeout <= 0 {
		config.DialTimeout = 1000
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 2000
	}
	if config.ReadTimeout <= 0 {
		config.ReadTimeout = 30000
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.ClientQueueSize <= 0 {
		config.ClientQueueSize = 1000
	}
	if config.ClientWorkerCount <= 0 {
		config.ClientWorkerCount = 100
	}
	if config.ClientDialTimeout <= 0 {
		config.ClientDialTimeout = 1000
	}
	if config.ClientWriteTimeout <= 0 {
		config.ClientWriteTimeout = 2000
	}
	if config.ClientMaxRetries <= 0 {
		config.ClientMaxRetries = 3
	}
	return &config, nil
}

// MustLoad is like Load but panics if the configuration cannot be loaded.
// This should only be used during program initialization where a missing
// or invalid configuration file is a fatal error.
//
// Example:
//
//	cfg := config.MustLoad()
//	server := NewServer(cfg)
//
// Panics if the configuration file cannot be read or contains invalid JSON.
func MustLoad() *Config {
	config, err := Load()
	if err != nil {
		panic(err)
	}
	return config
}
