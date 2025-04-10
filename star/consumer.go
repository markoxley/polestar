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

// Package star provides high-performance publish-subscribe messaging capabilities
// for the Polestar system. It implements both publisher and consumer interfaces
// with configurable performance parameters, connection management, and error handling.
package star

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/markoxley/polestar/msg"
)

// ConsumerConfig defines the configuration parameters for a consumer instance.
// All time-based fields are specified in milliseconds.
type ConsumerConfig struct {
	Name         string   `json:"name"`         // Unique identifier for this consumer
	HubAddress   string   `json:"hubAddress"`   // Hub server address (default: "127.0.0.1")
	HubPort      uint16   `json:"hubPort"`      // Hub server port (default: 24353)
	Address      string   `json:"address"`      // Local binding address
	Port         uint16   `json:"port"`         // Local binding port
	QueueSize    int      `json:"queueSize"`    // Message buffer size (default: 1000)
	DialTimeout  int      `json:"dialTimeout"`  // TCP connection timeout (default: 1000ms)
	WriteTimeout int      `json:"writeTimeout"` // Message write timeout (default: 2000ms)
	MaxRetries   int      `json:"maxRetries"`   // Failed operation retry limit (default: 3)
	Topics       []string `json:"topics"`       // Topics to subscribe to
}

// Consumer defines the interface that must be implemented by message consumers.
// Implementations must handle message consumption in a thread-safe manner as
// messages may be processed concurrently depending on the configuration.
//
// Example implementation:
//
//	type MyConsumer struct {
//	    // ... consumer state ...
//	}
//
//	func (c *MyConsumer) Consume(msg *msg.Message) {
//	    data, err := msg.Data()
//	    if err != nil {
//	        log.Printf("Failed to parse message: %v", err)
//	        return
//	    }
//	    // ... process message data ...
//	}
type Consumer interface {
	// Consume processes a single message received from the hub.
	// This method must be thread-safe as it may be called concurrently
	// from multiple goroutines. Any errors should be handled
	// internally as it does not return an error value.
	Consume(*msg.Message)
}

// Listen starts a consumer service that listens for messages on the specified topics.
// It handles the complete lifecycle of a consumer including:
//   - Registration with the hub
//   - Topic subscription
//   - Message reception and queuing
//   - Connection management
//   - Health monitoring (ping)
//
// Configuration defaults:
//   - QueueSize: 1000 messages
//   - DialTimeout: 1000ms
//   - WriteTimeout: 2000ms
//   - MaxRetries: 3 attempts
//   - HubAddress: "127.0.0.1"
//   - HubPort: 24353
//
// Example usage:
//
//	cfg := &ConsumerConfig{
//	    Name: "temperature_monitor",
//	    Address: "0.0.0.0",
//	    Port: 8080,
//	    Topics: []string{"sensors.temperature"},
//	}
//	consumer := &MyConsumer{}
//	if err := Listen(consumer, cfg); err != nil {
//	    log.Fatal(err)
//	}
//
// Returns an error if registration fails or if the TCP listener cannot be started.
func Listen(c Consumer, config *ConsumerConfig) error {
	if config == nil {
		return errors.New("consumer config cannot be nil")
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
	if config.HubAddress == "" {
		config.HubAddress = "127.0.0.1"
	}
	if config.HubPort <= 0 {
		config.HubPort = 24353
	}
	if config.Port <= 0 {
		return errors.New("port must be a valid TCP port number")
	}
	if config.Name == "" {
		return errors.New("name must be a non-empty string")
	}
	err := register(c, config)
	if err != nil {
		return err
	}
	q := startQueue(c, config)
	go ping(c, config)
	go func() {
		l, err := net.Listen("tcp", net.JoinHostPort(config.Address, fmt.Sprintf("%d", config.Port)))
		if err != nil {
			log.Printf("Failed to listen on %s:%d: %v", config.Address, config.Port, err)
			return
		}
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go handler(c, conn, q)
		}
	}()
	return nil
}

// startQueue initializes the message processing queue for a consumer.
// It creates a buffered channel for incoming messages and starts a
// goroutine to process them. The queue size is configurable through
// the ConsumerConfig to handle different throughput requirements.
//
// Parameters:
//   - c: The consumer implementation that will process messages
//   - config: Configuration including queue size
//
// Returns:
//   - chan *msg.Message: Channel for queuing incoming messages
func startQueue(c Consumer, config *ConsumerConfig) chan *msg.Message {
	queueIn := make(chan *msg.Message, config.QueueSize)
	go func() {
		for msg := range queueIn {
			c.Consume(msg)
		}
	}()
	return queueIn
}

// handler processes incoming connections by reading the message data,
// deserializing it, and passing it to the consumer's Consume method.
// It automatically closes the connection when done. Uses a 1KB buffer
// for reading messages, which should be sufficient for most use cases.
// Any errors during message processing are logged but do not stop the handler.
func handler(c Consumer, conn net.Conn, queueIn chan *msg.Message) {
	defer conn.Close()
	buf := make([]byte, 1024)
	data := make([]byte, 0)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		data = append(data, buf[:n]...)
		if n < 1024 {
			break
		}
	}
	msgs := msg.Split(data)
	for _, m := range msgs {
		select {
		case queueIn <- m:
			//
		default:
			log.Printf("Queue full, discarding message")
		}
	}
}

// register sends a registration message to the hub to subscribe to topics.
// The registration includes the consumer's name, listening port, and
// list of topics. The hub will start forwarding matching messages
// after successful registration.
//
// Parameters:
//   - c: The consumer implementation
//   - config: Configuration including registration details
//
// Returns:
//   - error: nil if registration succeeds, error otherwise
func register(c Consumer, config *ConsumerConfig) error {
	reg := msg.NewRegistrationMessage(config.Port, config.Name, config.Topics...)
	return send(reg, config.HubAddress, config.HubPort)
}

// ping maintains the consumer's active status with the hub by sending
// periodic ping messages. It implements automatic reconnection with
// exponential backoff on failures. The ping interval is 15 seconds
// to ensure reliable connection monitoring.
//
// Parameters:
//   - c: The consumer implementation
//   - config: Configuration including connection details
//
// Returns:
//   - error: Any fatal errors that prevent ping operation
func ping(c Consumer, config *ConsumerConfig) error {
	t := time.NewTicker(time.Second * 15) // Ping more frequently
	addr, port := config.HubAddress, config.HubPort
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

			m := msg.NewPingMessage(config.Name)

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
				log.Printf("Warning: ping channel full for %s", config.Name)
			}
		}
	}
}
