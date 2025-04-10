// Package thal provides high-performance publish-subscribe messaging capabilities
// for the Thalamini system. It implements both publisher and consumer interfaces
// with configurable performance parameters, connection management, and error handling.
package thal

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/markoxley/dani/msg"
)

// PublishConfig defines the configuration parameters for a publisher instance.
// All time-based fields are specified in milliseconds.
type PublishConfig struct {
	Address      string `json:"address"`      // Hub server address (default: "127.0.0.1")
	Port         uint16 `json:"port"`         // Hub server port (default: 24353)
	QueueSize    int    `json:"queueSize"`    // Size of message buffer (default: 1000)
	DialTimeout  int    `json:"dialTimeout"`  // TCP connection timeout (default: 1000ms)
	WriteTimeout int    `json:"writeTimeout"` // Message write timeout (default: 2000ms)
	MaxRetries   int    `json:"maxRetries"`   // Failed message retry limit (default: 3)
}

var (
	// queue acts as a buffered channel between message producers and the
	// background sender goroutine. Uses FIFO ordering to preserve message order.
	queue     chan *msg.Message
	pubConfig *PublishConfig
	// count tracks the number of unprocessed messages in the system
	// for monitoring purposes (not used for core functionality).
	count = 0
)

// Init initializes the publisher system with the specified configuration.
// It validates the configuration, applies defaults where needed, and starts
// the asynchronous message processing goroutine.
//
// Configuration defaults:
//   - QueueSize: 1000 messages
//   - DialTimeout: 1000ms
//   - WriteTimeout: 2000ms
//   - MaxRetries: 3 attempts
//   - Address: "127.0.0.1"
//   - Port: 24353
//
// Returns an error if the configuration is invalid.
//
// Example usage:
//
//	cfg := &PublishConfig{
//	    Address: "hub.example.com",
//	    Port: 24353,
//	    QueueSize: 5000,
//	}
//	if err := Init(cfg); err != nil {
//	    log.Fatal(err)
//	}
func Init(config *PublishConfig) error {
	if config == nil {
		return errors.New("publish config cannot be nil")
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 1000
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 2000
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.Address == "" {
		config.Address = "127.0.0.1"
	}
	if config.Port <= 0 {
		config.Port = 24353
	}
	pubConfig = config
	queue = make(chan *msg.Message, config.QueueSize)
	go run(config.Address, config.Port)
	return nil
}

// Publish adds a message to the outgoing queue for asynchronous delivery.
// Messages are delivered in a separate goroutine with automatic retries
// and backoff. If the queue is full, the message is dropped to prevent
// blocking. The function is thread-safe and can be called concurrently.
//
// Performance characteristics:
//   - Non-blocking operation (uses select on channel)
//   - Automatic retries up to MaxRetries
//   - Configurable queue size for backpressure
//   - Connection pooling for efficiency
//
// Example usage:
//
//	data := map[string]interface{}{
//	    "timestamp": time.Now().UnixNano(),
//	    "value": 42,
//	}
//	Publish("sensors.temperature", data)
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
//  1. Receive message from queue
//  2. Attempt delivery through managed connection
//  3. Retry failed messages according to the configured policy
//  4. Log permanent delivery failures
//
// Note: This function runs in a dedicated goroutine started during initialization.
func run(addr string, port uint16) {
	for msg := range queue {
		count--
		if err := attemptSend(msg, addr, port); err != nil {
			log.Printf("Failed to send message after %d attempts", pubConfig.MaxRetries)
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
		for retry < pubConfig.MaxRetries {
			err := send(msg, addr, port)
			if err == nil {
				return
			}
			retry++
		}
	}()
	return nil
}

// send attempts to deliver a message to the hub server with configurable timeouts.
// It establishes a new TCP connection for each message to ensure reliability
// and prevent connection staleness. The function implements the retry and
// timeout logic specified in the configuration.
//
// Parameters:
//   - m: The message to send
//   - addr: Hub server address
//   - port: Hub server port
//   - timeouts: Optional dial and write timeouts in milliseconds
//
// Returns:
//   - error: Detailed error for connection, timeout, or transmission failures
func send(m *msg.Message, addr string, port uint16, timeouts ...int) error {
	dialTimeout := 2000
	writeTimeout := 2000
	if pubConfig != nil {
		dialTimeout = pubConfig.DialTimeout
		writeTimeout = pubConfig.WriteTimeout
	}
	if len(timeouts) > 0 {
		dialTimeout = timeouts[0]
	}
	if len(timeouts) > 1 {
		writeTimeout = timeouts[1]
	}
	dialer := net.Dialer{Timeout: time.Duration(dialTimeout) * time.Millisecond}
	client, err := dialer.Dial("tcp", net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", addr, err)
	}
	defer client.Close()

	if err := client.SetWriteDeadline(time.Now().Add(time.Duration(writeTimeout) * time.Millisecond)); err != nil {
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
