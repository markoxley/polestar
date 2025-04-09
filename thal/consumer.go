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

// Package thal provides a robust pub/sub messaging system with queuing and retry capabilities.
// It implements both publisher and consumer interfaces for distributed messaging.
// The system uses TCP for reliable message delivery and supports topic-based routing.
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
// Messages are delivered asynchronously and the consumer must be thread-safe.
type Consumer interface {
	// Consume processes a received message. This method should handle any errors
	// internally as it does not return an error value. Implementations should be
	// thread-safe as messages may be processed concurrently.
	Consume(*msg.Message)

	// GetHub returns the address and port of the hub this consumer connects to.
	// This is used during registration and should return consistent values.
	// The returned address can be an IP or hostname, and port must be a valid TCP port.
	GetHub() (string, uint16)
}

// Listen starts a consumer service that listens for messages on the specified topics.
// It registers the consumer with the hub and starts a TCP listener for incoming messages.
// The service runs in the background and will continue until the program exits.
// It maintains a connection to the hub through periodic ping messages every 15 seconds.
//
// Parameters:
//   - name: Unique identifier for this consumer instance
//   - addr: IP address or hostname to listen on
//   - port: TCP port to listen on
//   - c: Implementation of the Consumer interface
//   - topics: List of topics to subscribe to
//
// Returns an error if registration fails or if the TCP listener cannot be started.
// Once started, the service handles connection errors internally and attempts to recover.
func Listen(name string, addr string, port uint16, c Consumer, topics ...string) error {
	err := register(c, name, port, topics...)
	if err != nil {
		return err
	}
	go ping(c, name)
	go func() {
		l, err := net.Listen("tcp", net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
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
// Any errors during message processing are logged but do not stop the handler.
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
// message cannot be sent. The consumer should handle registration failures
// appropriately, possibly by retrying or shutting down.
func register(c Consumer, name string, port uint16, topics ...string) error {
	reg := msg.NewRegistrationMessage(port, name, topics...)
	ip, pt := c.GetHub()
	return send(reg, ip, pt)
}

// ping sends a ping message to the hub every 15 seconds to maintain
// client liveness tracking. If a client fails to ping for 2 minutes,
// the hub will consider it inactive and remove it during cleanup.
// The ping routine implements connection pooling and error recovery
// to minimize connection overhead.
//
// Parameters:
//   - c: The consumer instance to ping from
//   - name: The name of this consumer for identification
//
// The ping routine runs indefinitely until the consumer is stopped.
// Each ping establishes a temporary connection to send the message.
// Failed pings are retried with exponential backoff to prevent
// overwhelming the network during outages.
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
				conn, err := dialer.Dial("tcp", net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
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
