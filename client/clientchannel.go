// Package client provides thread-safe client management for the Polestar system.
package client

import "errors"

// ClientChannel is a specialized channel type for handling client message queues.
// It implements a circular buffer with automatic oldest-message drop policy when
// the channel reaches capacity. This ensures that new messages can always be
// queued without blocking, making it suitable for high-throughput scenarios.
//
// Performance characteristics:
//   - Non-blocking message queue operations
//   - Automatic oldest-message removal on full
//   - Zero allocation for normal operations
//   - Thread-safe by channel semantics
type ClientChannel chan []byte

// Send attempts to queue a message in the channel. If the channel is full,
// it automatically drops the oldest message to make room for the new one.
// This ensures that the most recent messages are always preserved while
// maintaining non-blocking behavior.
//
// Parameters:
//   - data: The message bytes to queue
//
// Returns:
//   - bool: true if a message was dropped to make room, false otherwise
//   - error: non-nil if the channel remains full after dropping a message
//
// Thread safety:
//   - Safe for concurrent use from multiple goroutines
//   - Channel operations provide synchronization
func (c ClientChannel) Send(data []byte) (bool, error) {
	select {
	case c <- data:
		return false, nil
	default:
		// Channel is full, drop the oldest and try again
		<-c // Discard oldest
		select {
		case c <- data:
			// Message sent after dropping oldest
			return true, nil
		default:
			//This should rarely, if ever, happen.
			//Handle error/log message.
			return true, errors.New("channel still full after dropping oldest.")
		}
	}
}
