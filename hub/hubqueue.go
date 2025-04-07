// Package main implements a concurrent message processing hub for the Thalamini system.
// It provides a high-performance, thread-safe message routing system with support
// for client registration and message forwarding.
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
	// queueSize is the buffer size for the message queue channel
	queueSize = 2000
	// defaultWorkerCount is the default number of concurrent workers processing messages
	defaultWorkerCount = 20
	// dialTimeout is the maximum time allowed for establishing a TCP connection
	dialTimeout = 5 * time.Second
	// writeTimeout is the maximum time allowed for writing data to a client
	writeTimeout = 10 * time.Second
	maxRetries   = 3
)

// HubQueue manages concurrent message processing with a buffered channel and worker pool.
// It handles client registration, message routing, and maintains thread-safe client state.
// The hub uses a producer-consumer pattern where messages are queued for processing
// by a pool of worker goroutines.
type HubQueue struct {
	messageQueue chan HubMessage // Channel for queuing messages to be processed
	waitGroup    sync.WaitGroup  // WaitGroup for synchronizing worker goroutines
	workerCount  int             // Number of workers to spawn
	clients      *client.Clients // Thread-safe client registry
	topics       *topic.Topic    // Topic subscription management
}

// New creates and returns a new Hub instance with the default worker count.
// It initializes the message queue and client registry with default settings.
// Example:
//
//	hub := NewHub()
func New() *HubQueue {
	return NewWithWorkers(defaultWorkerCount)
}

// NewWithWorkers creates a new Hub with a specified number of workers.
// If the provided worker count is less than or equal to 0, it uses the default worker count.
// This method initializes the message queue and client registry with the specified worker count.
// Example:
//
//	hub := NewWithWorkers(10) // Create hub with 10 worker goroutines
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
// Each worker processes messages from the message queue independently.
// Workers continue running until Stop is called.
// This method blocks until all workers are started.
// Example:
//
//	hub.Run()
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
// This method blocks until all workers have finished.
// Example:
//
//	hub.Stop()
func (h *HubQueue) Stop() {
	close(h.messageQueue)
	h.waitGroup.Wait()
}

// Store queues a message for processing by the worker pool.
// Returns an error if the queue is full or the message cannot be processed.
// This method is thread-safe.
// Example:
//
//	err := hub.Store(message)
func (h *HubQueue) Store(message HubMessage) error {
	select {
	case h.messageQueue <- message:
		// Successfully queued
	default:
		return errors.New("queue full, message dropped")
	}
	return nil
}

// workerRun processes messages from the provided channel until the channel is closed.
// It handles both registration and data messages, routing them appropriately.
// This method is intended to be run as a goroutine and should not be called directly.
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
// It extracts the client's port from the message data and creates a new client entry.
// Returns an error if the registration data is invalid or incomplete.
// This method is thread-safe.
// Example:
//
//	err := hub.registerClient("192.168.1.1", message)
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

// processData handles data message routing to one or more destination clients.
// It attempts to send the message to all specified destinations and collects any errors.
// This method spawns a goroutine for each destination to enable concurrent message delivery.
// Example:
//
//	hub.processData("weather", data)
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

// sendData sends a message to a specific client with retry logic.
// It handles connection establishment, write timeouts, and ensures complete message delivery.
// Returns an error if the client is not found, connection fails, or write is incomplete.
// This method is thread-safe.
// Example:
//
//	err := hub.sendData(client, data)
func (h *HubQueue) sendData(c *client.Client, data []byte) error {
	dialer := net.Dialer{Timeout: dialTimeout}
	client, err := dialer.Dial("tcp4", fmt.Sprintf("%s:%d", c.IP, c.Port))
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", c.Name, err)
	}
	defer client.Close()

	// Set write deadline
	if err := client.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	n, err := client.Write(data)
	if err != nil {
		return fmt.Errorf("write failed to %s: %v", c.Name, err)
	}
	if n < len(data) {
		return fmt.Errorf("incomplete write to %s: sent %d of %d bytes", c.Name, n, len(data))
	}

	return nil
}
