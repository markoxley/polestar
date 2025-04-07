// Package hub implements a concurrent message processing hub for the Thalamini system.
// It provides a high-performance, thread-safe message routing system with support
// for client registration, topic-based message routing, and automatic retries.
// The package uses a worker pool pattern for efficient message processing and
// implements backpressure handling through bounded queues.
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
	queueSize = 2000

	// defaultWorkerCount is the default number of concurrent workers processing messages.
	// This provides a balance between parallelism and system resource usage.
	defaultWorkerCount = 20

	// dialTimeout is the maximum time allowed for establishing a TCP connection.
	// Connections that take longer will be aborted.
	dialTimeout = 5 * time.Second

	// writeTimeout is the maximum time allowed for writing data to a client.
	// Writes that exceed this timeout will fail and may trigger retries.
	writeTimeout = 10 * time.Second

	// maxRetries is the maximum number of attempts to deliver a message.
	// After this many failures, the message will be dropped.
	maxRetries = 3
)

// HubQueue manages concurrent message processing with a buffered channel and worker pool.
// It handles client registration, message routing, and maintains thread-safe client state.
// The hub uses a producer-consumer pattern where messages are queued for processing
// by a pool of worker goroutines.
type HubQueue struct {
	messageQueue chan HubMessage // Channel for queuing messages to be processed
	waitGroup    sync.WaitGroup  // WaitGroup for synchronizing worker goroutines
	workerCount  int            // Number of workers to spawn
	clients      *client.Clients // Thread-safe client registry
	topics       *topic.Topic    // Topic subscription management
}

// New creates and returns a new Hub instance with the default worker count.
// It initializes all internal components including the message queue,
// client registry, and topic manager.
//
// Returns:
//   - *HubQueue: A fully initialized hub ready for use
func New() *HubQueue {
	return NewWithWorkers(defaultWorkerCount)
}

// NewWithWorkers creates a new Hub with a specified number of workers.
// It allows customization of the concurrency level while maintaining
// all other default settings.
//
// Parameters:
//   - workers: Number of concurrent message processing workers
//
// Returns:
//   - *HubQueue: A fully initialized hub with the specified worker count
//
// If workers <= 0, the default worker count will be used.
func NewWithWorkers(workers int) *HubQueue {
	if workers <= 0 {
		workers = defaultWorkerCount
	}
	c := client.New()
	t := topic.New()
	return &HubQueue{
		messageQueue: make(chan HubMessage, queueSize),
		workerCount:  workers,
		clients:      c,
		topics:       t,
	}
}

// Run starts the worker pool with the configured number of workers.
// Each worker processes messages from the message queue independently
// and continues running until Stop is called. This method is non-blocking
// and returns immediately after starting all workers.
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
func (h *HubQueue) Stop() {
	close(h.messageQueue)
	h.waitGroup.Wait()
}

// Store queues a message for processing by the worker pool.
// It implements backpressure by returning an error when the queue is full
// rather than blocking indefinitely.
//
// Parameters:
//   - message: The message to be processed
//
// Returns:
//   - error: nil if queued successfully, or an error if the queue is full
func (h *HubQueue) Store(message HubMessage) error {
	select {
	case h.messageQueue <- message:
		// Successfully queued
	default:
		return errors.New("queue full, message dropped")
	}
	return nil
}

// workerRun processes messages from the provided channel until it's closed.
// It handles message deserialization, routing, and error recovery.
// This method is intended to be run as a goroutine and implements the
// core message processing logic.
//
// Parameters:
//   - ch: Channel to receive messages from
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
		case msg.RegMsgByte:
			err = h.registerClient(hm.IP, m)
		case msg.DataMsgByte:
			h.processData(m.Topic, hm.Data)
		default:
			err = fmt.Errorf("unknown message type: %d", m.MsgType)
		}
		if err != nil {
			log.Printf("Failed to process message: %v", err)
		}
	}
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
	h.clients.Add(client.Client{
		IP:   ip,
		Port: port,
		Name: name,
	})

	if len(topics) == 0 {
		return nil
	}
	h.topics.Add(name, topics...)
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
		go func(name string, d []byte) {
			c, ok := h.clients.Get(name)
			if !ok {
				return
			}
			retry := 0
			for retry < maxRetries {
				err := h.sendData(c, d)
				if err == nil {
					return
				}
				log.Printf("Failed to send message to %s: %v", name, err)
				retry++
			}
			log.Printf("Failed to send message to %s after %d attempts", c.Name, maxRetries)
		}(client, data)
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
