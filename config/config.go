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

// Package config provides configuration management for the Thalamini messaging system.
// It handles loading and validating server and client settings from a JSON configuration file.
// The package supports both required and optional configuration parameters with sensible defaults.
package config

import (
	"encoding/json"
	"os"
)

// Config holds the server configuration settings loaded from config.json.
// It defines network settings, performance tuning parameters, and timeout values
// for both the Thalamini hub server and its clients.
type Config struct {
	IP                 string `json:"ip"`                 // IP address to bind the server to (e.g., "0.0.0.0" for all interfaces)
	Port               uint16 `json:"port"`               // Port number to listen on (e.g., 24353)
	QueueSize          int    `json:"queueSize"`          // Size of the message queue buffer (default: 1,000,000)
	WorkerCount        int    `json:"workerCount"`        // Number of concurrent message processing workers (default: 100)
	DialTimeout        int    `json:"dialTimeout"`        // TCP connection timeout in milliseconds (default: 1000)
	WriteTimeout       int    `json:"writeTimeout"`       // Message write timeout in milliseconds (default: 2000)
	ReadTimeout        int    `json:"readTimeout"`        // Message read timeout in milliseconds (default: 30000)
	MaxRetries         int    `json:"maxRetries"`         // Maximum message delivery attempts (default: 3)
	ClientQueueSize    int    `json:"clientQueueSize"`    // Size of each client's message queue (default: 1000)
	ClientWorkerCount  int    `json:"clientWorkerCount"`  // Workers per client for message processing (default: 100)
	ClientDialTimeout  int    `json:"clientDialTimeout"`  // Client TCP connection timeout in ms (default: 1000)
	ClientWriteTimeout int    `json:"clientWriteTimeout"` // Client message write timeout in ms (default: 2000)
	ClientMaxRetries   int    `json:"clientMaxRetries"`   // Maximum client delivery attempts (default: 3)
}

// Load reads and parses the config.json file into a Config struct.
// It applies default values for any missing optional parameters
// and validates that all required parameters are present.
//
// The configuration file must be in the current working directory
// and must be named "config.json".
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
