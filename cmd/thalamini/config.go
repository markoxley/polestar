// Package main provides configuration management for the Thalamini hub server.
package main

import (
	"encoding/json"
	"os"
)

// Config holds the server configuration settings loaded from config.json.
// It defines the network address and port for the Thalamini hub server.
type Config struct {
	IP   string `json:"ip"`   // IP address to bind the server to
	Port uint16 `json:"port"` // Port number to listen on
}

// LoadConfig reads and parses the config.json file into a Config struct.
// Returns an error if the file cannot be read or contains invalid JSON.
// The config file must be in the current working directory.
func LoadConfig() (*Config, error) {
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
func MustLoadConfig() *Config {
	config, err := LoadConfig()
	if err != nil {
		panic(err)
	}
	return config
}
