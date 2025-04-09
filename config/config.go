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

// Package main provides configuration management for the Thalamini hub server.
package config

import (
	"encoding/json"
	"os"
)

// Config holds the server configuration settings loaded from config.json.
// It defines the network address and port for the Thalamini hub server.
// The configuration is loaded during server startup and used to initialize the listener.
type Config struct {
	IP   string `json:"ip"`   // IP address to bind the server to
	Port uint16 `json:"port"` // Port number to listen on
}

// Load reads and parses the config.json file into a Config struct.
// Returns an error if the file cannot be read or contains invalid JSON.
// The config file must be in the current working directory.
// Example:
//
//	config, err := LoadConfig()
func Load() (*Config, error) {
	b, err := os.ReadFile("config.json")
	if err != nil {
		return nil, err
	}
	var config Config
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// MustLoadConfig is like LoadConfig but panics if the configuration
// cannot be loaded. This should only be used during program initialization
// where a missing or invalid config is fatal.
// Example:
//
//	config := MustLoadConfig()
func MustLoad() *Config {
	config, err := Load()
	if err != nil {
		panic(err)
	}
	return config
}
