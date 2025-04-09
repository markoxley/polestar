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

// Package hub implements a concurrent message processing hub for the Thalamini system.
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

	"github.com/markoxley/dani/client"
	"github.com/markoxley/dani/msg"
	"github.com/markoxley/dani/topic"
)

// Constants defining the hub configuration and timeouts
const (
	// queueSize is the buffer size for the message queue channel.
	// Messages beyond this limit will be dropped to prevent memory exhaustion.
	// This provides backpressure to publishers when the system is overloaded.
	queueSize = 1000000

	// defaultWorkerCount is the default number of concurrent workers processing messages.
	// This provides a balance between parallelism and system resource usage.
	// Can be overridden using NewWithWorkers for specific use cases.
	defaultWorkerCount = 100

	// dialTimeout is the maximum time allowed for establishing a TCP connection.
	// Connections that take longer will be aborted. This prevents resource
	// exhaustion from slow or unresponsive clients.
	dialTimeout = 1 * time.Second

	// writeTimeout is the maximum time allowed for writing data to a client.
	// Writes that exceed this timeout will fail and may trigger retries.
	// This ensures that slow clients cannot block the entire system.
	writeTimeout = 2 * time.Second

	// maxRetries is the maximum number of attempts to deliver a message.
	// After this many failures, the message will be dropped and logged.
	// This prevents infinite retry loops for persistently failing clients.
	maxRetries = 3

	// garbageTimer defines how often to run cleanup of inactive clients.
	// Clients that haven't sent a ping within this duration are removed.
	// This prevents resource leaks from disconnected clients.
	garbageTimer = time.Minute * 2
)

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
	messageQueue chan HubMessage // Channel for queuing messages to be processed
	waitGroup    sync.WaitGroup  // WaitGroup for synchronizing worker goroutines
	workerCount  int            // Number of workers to spawn
	clients      *client.Clients // Thread-safe client registry
	topics       *topic.Topic   // Topic subscription management
}

// New creates and returns a new Hub instance with the default worker count.
// It initializes all internal components including the message queue,
// client registry, and topic manager. The hub is ready to process
// messages but Run() must be called to start the worker pool.
//
// Returns:
//   - *HubQueue: A fully initialized hub ready for use
//
// Example:
//   hub := New()
//   hub.Run()
//   defer hub.Stop()
func New() *HubQueue {
	return NewWithWorkers(defaultWorkerCount)
}

// NewWithWorkers creates a new Hub with a specified number of workers.
// It allows customization of the concurrency level while maintaining
// all other default settings. This is useful for tuning performance
// based on the expected message load and system resources.
//
// Parameters:
//   - workers: Number of concurrent message processing workers
//
// Returns:
//   - *HubQueue: A fully initialized hub with the specified worker count
//
// If workers <= 0, the default worker count will be used.
//
// Example:
//   hub := NewWithWorkers(50) // Create hub with 50 workers
//   hub.Run()
//   defer hub.Stop()
func NewWithWorkers(workers int) *HubQueue {
	if workers <= 0 {
		workers = defaultWorkerCount
	}
	c := client.New()
	t := topic.New()
	h := &HubQueue{
		messageQueue: make(chan HubMessage, queueSize),
		workerCount:  workers,
		clients:      c,
		topics:       t,
	}
	go h.garbageCollection()
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
//   1. Message queue is closed (no new messages accepted)
//   2. Workers finish processing queued messages
//   3. All goroutines are cleaned up
//   4. Method returns when shutdown is complete
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
//   err := hub.Store(hubMessage)
//   if err != nil {
//       log.Println("Queue full:", err)
//   }
func (h *HubQueue) Store(message HubMessage) error {
	select {
	case h.messageQueue <- message:
		return nil
	default:
		return errors.New("queue full")
	}
}

// garbageCollection runs a periodic cleanup of inactive clients.
// It runs every 2 minutes and removes any clients that haven't
// sent a ping message within the last 2 minutes. This ensures
// that the hub maintains an accurate list of active clients and
// prevents resource leaks from disconnected clients.
//
// The cleanup process is thread-safe and runs concurrently with
// normal message processing. Removed clients must re-register
// to resume receiving messages.
//
// It iterates through the clients and checks the last time they sent a ping.
// If a client hasn't sent a ping within the garbageTimer duration, it is removed.
func (h *HubQueue) garbageCollection() {
	ticker := time.NewTicker(garbageTimer)
	defer ticker.Stop()
	for range ticker.C {
		h.clients.Range(func(name string, c *client.Client) bool {
			if time.Since(c.LastPing()) > garbageTimer {
				log.Printf("Removing inactive client: %s\n", name)
				h.clients.Delete(name)
			}
			return true
		})
	}
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
func (h *HubQueue) workerRun(ch chan HubMessage) {
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
	h.clients.Add(client.NewClient(ip, port, name))

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
	dialer := net.Dialer{Timeout: dialTimeout}
	// Use JoinHostPort to properly handle IPv6 addresses
	addr := net.JoinHostPort(c.IP, fmt.Sprintf("%d", c.Port))
	client, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", addr, err)
	}
	defer client.Close()

	// Set write deadline
	if err := client.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
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
