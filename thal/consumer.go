// Package thal provides a robust pub/sub messaging system with queuing and retry capabilities.
// It implements both publisher and consumer interfaces for distributed messaging.
package thal

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/markoxley/dani/msg"
)

// Consumer defines the interface that must be implemented by message consumers.
// Implementations must handle message consumption and provide hub connection details.
// The consumer is responsible for managing its own error handling and recovery.
type Consumer interface {
	// Consume processes a received message. This method should handle any errors
	// internally as it does not return an error value. Implementations should be
	// thread-safe as messages may be processed concurrently.
	Consume(*msg.Message)

	// GetHub returns the address and port of the hub this consumer connects to.
	// This is used during registration and should return consistent values.
	GetHub() (string, uint16)
}

// Listen starts a consumer service that listens for messages on the specified topics.
// It registers the consumer with the hub and starts a TCP listener for incoming messages.
// The service runs in the background and will continue until the program exits.
//
// Parameters:
//   - name: Unique identifier for this consumer instance
//   - addr: IP address or hostname to listen on
//   - port: TCP port to listen on
//   - c: Implementation of the Consumer interface
//   - topics: List of topics to subscribe to
//
// Returns an error if registration fails or if the TCP listener cannot be started.
func Listen(name string, addr string, port uint16, c Consumer, topics ...string) error {
	err := register(c, name, port, topics...)
	if err != nil {
		return err
	}
	go ping(c, name)
	go func() {
		l, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", addr, port))
		if err != nil {
			log.Printf("Failed to listen on %s:%d: %v", addr, port, err)
			return
		}
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			handler(c, conn)
		}
	}()
	return nil
}

// handler processes incoming connections by reading the message data,
// deserializing it, and passing it to the consumer's Consume method.
// It automatically closes the connection when done. Uses a 1KB buffer
// for reading messages, which should be sufficient for most use cases.
func handler(c Consumer, conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}
	m := &msg.Message{}
	err = m.Deserialize(buf[:n])
	if err != nil {
		log.Printf("Failed to deserialize message: %v", err)
		return
	}
	c.Consume(m)
}

// register sends a registration message to the hub to subscribe to the specified topics.
// It establishes a temporary connection to the hub for registration and closes it
// immediately after the registration message is sent. The registration message includes
// the consumer's name, port, and list of topics it wants to subscribe to.
//
// Parameters:
//   - c: The consumer instance to register
//   - name: Unique identifier for this consumer
//   - port: The port this consumer is listening on
//   - topics: List of topics to subscribe to
//
// Returns an error if the connection to the hub fails or if the registration
// message cannot be sent.
func register(c Consumer, name string, port uint16, topics ...string) error {
	reg := msg.NewRegistrationMessage(port, name, topics...)
	ip, pt := c.GetHub()
	return send(reg, ip, pt)
}

// ping sends a ping message to the hub every 15 seconds to maintain
// client liveness tracking. If a client fails to ping for 2 minutes,
// the hub will consider it inactive and remove it during cleanup.
//
// Parameters:
//   - c: The consumer instance to ping from
//   - name: The name of this consumer for identification
//
// The ping routine runs indefinitely until the consumer is stopped.
// Each ping establishes a temporary connection to send the message.
func ping(c Consumer, name string) error {
	t := time.NewTicker(time.Second * 15) // Ping more frequently
	addr, port := c.GetHub()
	pingCh := make(chan struct{}, 10) // Buffer for ping requests
	var currentConn net.Conn
	var connMutex sync.Mutex

	// Separate goroutine for sending pings
	go func() {
		for range pingCh {
			connMutex.Lock()
			// Try to reuse existing connection
			if currentConn == nil {
				dialer := net.Dialer{Timeout: 1 * time.Second}
				conn, err := dialer.Dial("tcp4", fmt.Sprintf("%s:%d", addr, port))
				if err != nil {
					log.Printf("connection failed to %s: %v\n", addr, err)
					connMutex.Unlock()
					time.Sleep(time.Millisecond * 100) // Add delay before retry
					continue
				}
				currentConn = conn
			}

			m := msg.NewPingMessage(name)

			// Set write deadline
			if err := currentConn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
				log.Printf("failed to set write deadline: %v\n", err)
				currentConn.Close()
				currentConn = nil
				connMutex.Unlock()
				continue
			}

			n, err := currentConn.Write(m.Serialize())
			if err != nil {
				log.Printf("write failed to %s: %v\n", addr, err)
				currentConn.Close()
				currentConn = nil
				connMutex.Unlock()
				continue
			}
			if n < len(m.Serialize()) {
				log.Printf("incomplete write to %s: sent %d of %d bytes", addr, n, len(m.Serialize()))
				currentConn.Close()
				currentConn = nil
				connMutex.Unlock()
				continue
			}
			connMutex.Unlock()
		}
	}()

	for {
		select {
		case <-t.C:
			select {
			case pingCh <- struct{}{}: // Non-blocking ping request
			default:
				log.Printf("Warning: ping channel full for %s", name)
			}
		}
	}
}
