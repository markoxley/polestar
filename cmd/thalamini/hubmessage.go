// Package main provides message handling for the Thalamini hub.
package main

// HubMessage represents a message in the Thalamini hub system.
// It contains both the message data and metadata about its origin.
type HubMessage struct {
	IP   string // Source IP address of the message
	Data []byte // Raw message data
}
