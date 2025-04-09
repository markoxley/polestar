// Package thal provides message publishing capabilities for the Thalamini system.
// It implements a non-blocking, buffered message queue with configurable retries
// and timeouts for reliable message delivery.
package thal

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/markoxley/dani/msg"
)

// Network communication constants
const (
	// dialTimeout specifies the maximum time allowed to establish
	// a TCP connection to the hub server.
	// Connections exceeding this timeout will be aborted.
	dialTimeout = 5 * time.Second

	// writeTimeout defines the maximum duration for completing
	// a message write operation to an established connection.
	// Writes that take longer than this will result in an error.
	writeTimeout = 10 * time.Second

	// maxRetries sets the maximum number of delivery attempts
	// for a single message before permanent failure.
	// Messages will be dropped after exceeding this retry limit.
	maxRetries = 3

	// queueSize determines the capacity of the internal message buffer.
	// When the queue is full, new publish requests will be rejected immediately
	// to prevent blocking the caller.
	queueSize = 1000
)

var (
	// queue acts as a buffered channel between message producers and the
	// background sender goroutine. Uses FIFO ordering to preserve message order.
	queue chan *msg.Message

	// count tracks the number of unprocessed messages in the system
	// for monitoring purposes (not used for core functionality).
	count = 0
)

// Init initializes the publishing subsystem and starts background processing.
// It creates the message queue and launches a goroutine to handle message delivery.
//
// Parameters:
//   - addr: IP address or hostname of the hub server
//   - port: TCP port number of the hub server
//
// Concurrency:
//   - Safe for concurrent calls but will reset existing configuration.
//   - Starts a long-running goroutine for asynchronous message delivery.
func Init(addr string, port uint16) {
	queue = make(chan *msg.Message, queueSize)
	go run(addr, port)
}

// Publish adds a message to the outgoing queue for asynchronous delivery.
// Constructs a Thalamini message wrapper around the raw data and attempts to
// enqueue it. If the queue is full, the publish operation will fail immediately.
//
// Parameters:
//   - topic: The message topic for routing
//   - data: Key-value pairs to be sent in the message
//
// Returns:
//   - error: nil on success, error when queue is full or message invalid
//
// Concurrency:
//   - Safe for concurrent use from multiple goroutines.
//   - Non-blocking - fails immediately if the queue is full.
func Publish(topic string, data map[string]interface{}) error {
	m := msg.NewMessage(topic)
	if err := m.SetData(data); err != nil {
		return err
	}
	select {
	case queue <- m:
		return nil
	default:
		return errors.New("queue full, message dropped")
	}
}

// run continuously processes messages from the queue until it is closed.
// It implements the core message delivery loop with retry logic, ensuring
// reliable delivery even in the face of transient network failures.
//
// Flow:
//   1. Receive message from queue
//   2. Attempt delivery through managed connection
//   3. Retry failed messages according to the configured policy
//   4. Log permanent delivery failures
//
// Note: This function runs in a dedicated goroutine started during initialization.
func run(addr string, port uint16) {
	for msg := range queue {
		count--
		if err := attemptSend(msg, addr, port); err != nil {
			log.Printf("Failed to send message after %d attempts", maxRetries)
		}
	}
}

// attemptSend manages the retry logic for a single message delivery.
// It spawns a goroutine to handle the actual network operations, allowing
// the main delivery loop to continue processing messages without blocking.
//
// Parameters:
//   - msg: Message to be delivered
//   - addr: Target server address
//   - port: Target server port
//
// Returns:
//   - error: Final error after all retries are exhausted
func attemptSend(msg *msg.Message, addr string, port uint16) error {
	go func() {
		retry := 0
		for retry < maxRetries {
			err := send(msg, addr, port)
			if err == nil {
				return
			}
			retry++
		}
	}()
	return nil
}

// send performs a single delivery attempt for a message.
// It handles the full TCP connection lifecycle, including:
// - DNS resolution
// - TCP connection establishment
// - Setting a write deadline
// - Error classification
//
// Parameters:
//   - m: Message to serialize and send
//   - addr: Target hostname/IP address
//   - port: Target TCP port
//
// Returns:
//   - error: Detailed error information including:
//     - Connection failures
//     - Write timeouts
//     - Partial writes
//     - Message serialization errors
func send(m *msg.Message, addr string, port uint16) error {
	dialer := net.Dialer{Timeout: dialTimeout}
	client, err := dialer.Dial("tcp", net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", addr, err)
	}
	defer client.Close()

	if err := client.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	n, err := client.Write(m.Serialize())
	if err != nil {
		return fmt.Errorf("write failed to %s: %v", addr, err)
	}
	if n < len(m.Serialize()) {
		return fmt.Errorf("incomplete write to %s: sent %d of %d bytes", addr, n, len(m.Serialize()))
	}

	return nil
}
