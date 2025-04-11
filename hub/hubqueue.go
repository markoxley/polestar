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

// Package hub implements a concurrent message processing hub for the Polestar system.
// It provides a high-performance, thread-safe message routing system with support
// for client registration, topic-based message routing, and automatic retries.
// The package uses a worker pool pattern for efficient message processing and
// implements backpressure handling through bounded queues.
//
// Key features:
//   - Worker pool for concurrent message processing
//   - Topic-based message routing with pattern matching
//   - Automatic client cleanup for inactive connections
//   - Bounded message queues with backpressure handling
//   - Configurable timeouts and retry mechanisms
package hub

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/markoxley/polestar/client"
	"github.com/markoxley/polestar/config"
	"github.com/markoxley/polestar/msg"
	"github.com/markoxley/polestar/topic"
)

// Constants defining the hub configuration and timeouts
const ()

// HubQueue manages concurrent message processing with a buffered channel and worker pool.
// It handles client registration, message routing, and maintains thread-safe client state.
// The hub uses a producer-consumer pattern where messages are queued for processing
// by a pool of worker goroutines.
//
// Thread Safety:
//   - All public methods are thread-safe and can be called concurrently
//   - Internal state is protected by sync.Mutex where needed
//   - Channel operations provide synchronization for message passing
//
// Error Handling:
//   - Network errors are logged and may trigger retries
//   - Queue overflow errors are returned to callers
//   - Client errors do not affect other clients
type HubQueue struct {
	messageQueue HubChannel      // Buffered channel for pending messages
	waitGroup    sync.WaitGroup  // For graceful shutdown coordination
	workerCount  int             // Number of message processing workers
	clients      *client.Clients // Thread-safe client registry
	topics       *topic.Topic    // Topic subscription manager
	config       *config.Config  // System configuration
}

// New creates and returns a new Hub instance.
// It initializes all internal components including the message queue,
// client registry, and topic manager. The hub is ready to process
// messages but Run() must be called to start the worker pool.
//
// Parameters:
//   - config: System configuration including worker count and queue sizes
//
// Returns:
//   - *HubQueue: A fully initialized hub instance
func New(config *config.Config) *HubQueue {
	c := client.New(config)
	t := topic.New()
	h := &HubQueue{
		messageQueue: make(HubChannel, config.QueueSize),
		workerCount:  config.WorkerCount,
		clients:      c,
		topics:       t,
		config:       config,
	}
	return h
}

// Run starts the worker pool with the configured number of workers.
// Each worker processes messages from the message queue independently
// and continues running until Stop is called. This method is non-blocking
// and returns immediately after starting all workers.
//
// Workers handle:
//   - Message deserialization and validation
//   - Client registration and pings
//   - Topic-based message routing
//   - Error recovery and retries
func (h *HubQueue) Run() {
	for i := 0; i < h.workerCount; i++ {
		h.waitGroup.Add(1)
		go func() {
			defer h.waitGroup.Done()
			h.workerRun(h.messageQueue)
		}()
	}
}

// Stop gracefully shuts down the hub by closing the message queue
// and waiting for all workers to complete their processing.
// This method blocks until all workers have finished their current
// tasks and cleaned up their resources.
//
// The shutdown sequence:
//  1. Message queue is closed (no new messages accepted)
//  2. Workers finish processing queued messages
//  3. All goroutines are cleaned up
//  4. Method returns when shutdown is complete
func (h *HubQueue) Stop() {
	close(h.messageQueue)
	h.waitGroup.Wait()
}

// Store queues a message for processing by the worker pool.
// It implements backpressure by returning an error when the queue is full
// rather than blocking indefinitely. This prevents memory exhaustion
// and ensures system stability under high load.
//
// Parameters:
//   - message: The message to be processed
//
// Returns:
//   - error: nil if queued successfully, or an error if the queue is full
//
// Example:
//
//	err := hub.Store(hubMessage)
//	if err != nil {
//	    log.Println("Queue full:", err)
//	}
func (h *HubQueue) Store(message HubMessage) error {
	_, err := h.messageQueue.Send(&message)
	return err
}

// workerRun processes messages from the provided channel until it's closed.
// It handles message deserialization, routing, and error recovery.
// This method is intended to be run as a goroutine and implements the
// core message processing logic.
//
// Message Types Handled:
//   - Ping messages for client liveness
//   - Registration messages for new clients
//   - Data messages for topic-based routing
//
// Parameters:
//   - ch: Channel to receive messages from
//
// Error Handling:
//   - Deserialization errors are logged
//   - Unknown message types are rejected
//   - Client errors are isolated
func (h *HubQueue) workerRun(ch HubChannel) {
	for hm := range ch {

		m := &msg.Message{}
		err := m.Deserialize(hm.Data)
		if err != nil {
			log.Println("Data", hm.Data)
			log.Println("String", string(hm.Data))
			log.Printf("Failed to deserialize message: %v", err)
			continue
		}
		switch m.MsgType {
		case msg.PingMsgByte:
			err = h.receivePing(m)
		case msg.RegMsgByte:
			err = h.registerClient(hm.IP, m)
		case msg.DataMsgByte:
			h.processData(m.Topic, hm.Data)
			time.Sleep(time.Millisecond)
		default:
			err = fmt.Errorf("unknown message type: %d", m.MsgType)
		}
		if err != nil {
			log.Printf("Failed to process message: %v", err)
		}
	}
}

// receivePing processes a ping message from a client and updates
// their last seen timestamp. This is used to track client liveness
// and determine which clients should be removed during garbage collection.
//
// Parameters:
//   - m: The ping message containing the client's name
//
// Returns:
//   - error: If the ping message is invalid or the client is not registered
func (h *HubQueue) receivePing(m *msg.Message) error {
	data := m.Raw()
	if len(data) == 0 {
		return errors.New("invalid ping message")
	}
	name := string(data)
	return h.clients.Ping(name)
}

// registerClient processes a registration message and adds the client to the registry.
// It parses the registration data and sets up topic subscriptions.
//
// Parameters:
//   - ip: Client's IP address
//   - m: Registration message containing client details
//
// Returns:
//   - error: nil if registration succeeds, or an error if the data is invalid
func (h *HubQueue) registerClient(ip string, m *msg.Message) error {
	data := m.Raw()
	if len(data) < 2 {
		return errors.New("invalid registration message")
	}
	port := uint16(data[0]) + (uint16(data[1]) << 8)
	data = data[2:]

	if len(data) == 0 {
		return errors.New("invalid registration message")
	}
	topics := strings.Split(string(data), ",")
	name := topics[0]
	if name == "" {
		return errors.New("missing source name")
	}
	topics = topics[1:]
	h.clients.Add(client.NewClient(ip, port, name, h.config))

	if len(topics) == 0 {
		return nil
	}
	h.topics.Add(name, topics...)
	log.Printf("Registered client %s with topics %v\n", name, topics)
	return nil
}

// processData handles data message routing to subscribed clients.
// It implements concurrent message delivery with automatic retries
// and error handling.
//
// Parameters:
//   - topic: The message topic for routing
//   - data: Raw message data to be delivered
func (h *HubQueue) processData(topic string, data []byte) {
	l := h.topics.GetClients(topic)
	for _, client := range l {
		c, ok := h.clients.Get(client)
		if !ok {
			return
		}
		c.Send(data)
	}
}

// sendData attempts to deliver a message to a specific client.
// It handles connection management, timeouts, and delivery confirmation.
//
// Parameters:
//   - c: Target client
//   - data: Message data to send
//
// Returns:
//   - error: nil if delivery succeeds, or an error describing the failure
func (h *HubQueue) sendData(c *client.Client, data []byte) error {
	dialer := net.Dialer{Timeout: time.Duration(h.config.DialTimeout) * time.Millisecond}
	// Use JoinHostPort to properly handle IPv6 addresses
	addr := net.JoinHostPort(c.IP, fmt.Sprintf("%d", c.Port))
	client, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", addr, err)
	}
	defer client.Close()

	// Set write deadline
	if err := client.SetWriteDeadline(time.Now().Add(time.Duration(h.config.WriteTimeout) * time.Millisecond)); err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	n, err := client.Write(data)
	if err != nil {
		return fmt.Errorf("write failed to %s: %v", addr, err)
	}
	if n < len(data) {
		return fmt.Errorf("incomplete write to %s: sent %d of %d bytes", addr, n, len(data))
	}

	return nil
}
