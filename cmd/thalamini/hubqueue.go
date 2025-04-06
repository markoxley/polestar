// Package main implements a concurrent message processing hub for the Thalamini system.
// It provides a high-performance, thread-safe message routing system with support
// for client registration and message forwarding.
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/markoxley/dani/msg"
)

// Constants defining the hub configuration and timeouts
const (
	// QUEUE_SIZE is the buffer size for the message queue channel
	QUEUE_SIZE = 2000
	// DEFAULT_WORKER_COUNT is the default number of concurrent workers processing messages
	DEFAULT_WORKER_COUNT = 20
	// dialTimeout is the maximum time allowed for establishing a TCP connection
	dialTimeout = 5 * time.Second
	// writeTimeout is the maximum time allowed for writing data to a client
	writeTimeout = 10 * time.Second
)

// HubQueue manages concurrent message processing with a buffered channel and worker pool.
// It handles client registration, message routing, and maintains thread-safe client state.
type HubQueue struct {
	messageQueue chan HubMessage // Channel for queuing messages to be processed
	waitGroup    sync.WaitGroup  // WaitGroup for synchronizing worker goroutines
	workerCount  int            // Number of workers to spawn
	clients      *Clients       // Thread-safe client registry
}

// NewHub creates and returns a new Hub instance with the default worker count.
// It initializes the message queue and client registry.
func NewHub() *HubQueue {
	return NewWithWorkers(DEFAULT_WORKER_COUNT)
}

// NewWithWorkers creates a new Hub with a specified number of workers.
// If workers <= 0, it uses the default worker count.
func NewWithWorkers(workers int) *HubQueue {
	if workers <= 0 {
		workers = DEFAULT_WORKER_COUNT
	}
	return &HubQueue{
		messageQueue: make(chan HubMessage, QUEUE_SIZE),
		workerCount:  workers,
		clients:      &Clients{clients: make(map[string]Client)},
	}
}

// Run starts the worker pool with workerCount workers.
// Each worker processes messages from the message queue independently.
// Workers continue running until Stop is called.
func (h *HubQueue) Run() {
	for i := 0; i < h.workerCount; i++ {
		h.waitGroup.Add(1)
		go func() {
			defer h.waitGroup.Done()
			workerRun(h.clients, h.messageQueue)
		}()
	}
}

// Stop gracefully shuts down the hub by closing the message queue
// and waiting for all workers to complete their processing.
func (h *HubQueue) Stop() {
	close(h.messageQueue)
	h.waitGroup.Wait()
}

// Store queues a message for processing by the worker pool.
// Returns an error if the queue is full or the message cannot be processed.
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
func workerRun(clients *Clients, ch chan HubMessage) {
	for hm := range ch {
		m := &msg.Message{}
		err := m.Deserialize(hm.Data)
		if err != nil {
			log.Printf("Failed to deserialize message: %v", err)
			continue
		}
		switch m.MsgType {
		case msg.RegMsgByte:
			err = registerClient(clients, hm.IP, m)
		case msg.DataMsgByte:
			err = processData(clients, m.Destination, hm.Data)
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
func registerClient(clients *Clients, ip string, m *msg.Message) error {
	data, err := m.Data()
	if err != nil {
		return err
	}
	port, ok := data["port"]
	if !ok {
		return errors.New("port not found in registration message")
	}
	portInt, ok := port.(int64)
	if !ok {
		return errors.New("port not found in registration message")
	}
	clients.Add(Client{
		IP:   ip,
		Port: uint16(portInt),
		Name: m.Source,
	})
	return nil
}

// processData handles data message routing to one or more destination clients.
// It attempts to send the message to all specified destinations and collects any errors.
// Returns a combined error if any destinations failed to receive the message.
func processData(clients *Clients, destination []string, data []byte) error {
	var errs []error
	for _, dest := range destination {
		if err := sendData(clients, dest, data); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// sendData delivers a message to a specific client over TCP.
// It handles connection establishment, write timeouts, and ensures complete message delivery.
// Returns an error if the client is not found, connection fails, or write is incomplete.
func sendData(clients *Clients, dest string, data []byte) error {
	c, exists := clients.Get(dest)
	if !exists {
		return fmt.Errorf("Client %s not found", dest)
	}

	dialer := net.Dialer{Timeout: dialTimeout}
	client, err := dialer.Dial("tcp4", fmt.Sprintf("%s:%d", c.IP, c.Port))
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", dest, err)
	}
	defer client.Close()

	// Set write deadline
	if err := client.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	n, err := client.Write(data)
	if err != nil {
		return fmt.Errorf("write failed to %s: %v", dest, err)
	}
	if n < len(data) {
		return fmt.Errorf("incomplete write to %s: sent %d of %d bytes", dest, n, len(data))
	}

	return nil
}
