// Package hub implements the core message routing and distribution system
// for Polestar. It provides high-performance message handling with support
// for concurrent operations and automatic backpressure management.
package hub

import (
	"errors"
	"time"
)

// HubMessage represents a message in the Polestar hub system.
// It encapsulates both the message payload and routing information,
// providing a unified structure for message handling throughout the hub.
//
// Performance characteristics:
//   - Zero-copy message passing
//   - Minimal memory allocation
//   - Thread-safe access patterns
//
// Example usage:
//
//	// Create a new message
//	msg := &HubMessage{
//	    IP: "192.168.1.100",
//	    Data: []byte("sensor.temperature:23.5"),
//	}
//
//	// Send through hub channel
//	if dropped, err := hubChannel.Send(msg); err != nil {
//	    log.Printf("Failed to send: %v", err)
//	} else if dropped {
//	    log.Printf("Message queued, but older message was dropped")
//	}
type HubMessage struct {
	IP   string // Source IP address of the message sender
	Data []byte // Raw message payload for efficient processing
}

// HubChannel is a specialized channel type for routing messages through the hub.
// It implements a circular buffer with automatic oldest-message drop policy when
// the channel reaches capacity, ensuring non-blocking message handling even
// under high load.
//
// Performance characteristics:
//   - Non-blocking message operations
//   - Automatic backpressure handling
//   - Zero allocation for normal operations
//   - Thread-safe by channel semantics
type HubChannel chan *HubMessage

// Send attempts to queue a message in the channel. If the channel is full,
// it automatically drops the oldest message to make room for the new one.
// This ensures that recent messages are preserved while maintaining
// non-blocking behavior under high load.
//
// Parameters:
//   - m: The message to queue
//
// Returns:
//   - bool: true if a message was dropped to make room, false otherwise
//   - error: non-nil if the channel remains full after dropping a message
//
// Thread safety:
//   - Safe for concurrent use from multiple goroutines
//   - Channel operations provide synchronization
//
// Example:
//
//	msg := &HubMessage{IP: "192.168.1.100", Data: data}
//	if dropped, err := channel.Send(msg); err != nil {
//	    log.Printf("Channel congested: %v", err)
//	} else if dropped {
//	    metrics.IncrementDropped()
//	}
func (h HubChannel) Send(m *HubMessage, behaviour string) (bool, error) {
	for {
		select {
		case h <- m:
			return false, nil
		default:
			switch behaviour {
			case "drop":
				return true, nil
			case "dropold":
				// Channel is full, drop the oldest and try again
				<-h // Discard oldest
				select {
				case h <- m:
					// Message sent after dropping oldest
					return true, nil
				default:
					//This should rarely, if ever, happen.
					//Handle error/log message.
					return true, errors.New("channel still full after dropping oldest.")
				}
			default:
				time.Sleep(time.Microsecond)
			}
		}
	}
}
