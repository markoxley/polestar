// Package main provides message handling for the Thalamini hub.
package hub

// HubMessage represents a message in the Thalamini hub system.
// It contains both the message data and metadata about its origin.
// This struct is used internally by the hub to manage message routing.
// For example, a HubMessage can be created and sent through the hub like this:
//
//	msg := HubMessage{IP: "192.168.1.100", Data: []byte("Hello, world!")}
//	hub.SendMessage(msg)
type HubMessage struct {
	IP   string // Source IP address of the message
	Data []byte // Raw message data
}
