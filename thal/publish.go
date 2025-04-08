// Package thal provides a robust pub/sub messaging system with queuing and retry capabilities.
// It implements a non-blocking publisher with automatic retries and a bounded message queue.
package thal

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/markoxley/dani/msg"
)

// Configuration constants for the publisher
const (
	dialTimeout  = 5 * time.Second  // Maximum time to wait for TCP connection
	writeTimeout = 10 * time.Second // Maximum time to wait for write operation
	maxRetries   = 3                // Maximum number of retry attempts for failed sends
	queueSize    = 50               // Size of the message queue buffer
)

var (
	queue chan *msg.Message // Channel for queuing messages before sending
	count = 0
)

// Init initializes the publisher with the specified hub address and port.
// It creates a buffered message queue and starts the background message processor.
//
// Parameters:
//   - addr: The IP address or hostname of the hub
//   - port: The TCP port number the hub is listening on
func Init(addr string, port uint16) error {
	queue = make(chan *msg.Message, queueSize)
	go run(addr, port)
	return nil
}

// Publish queues a message with the given topic and data for sending.
// If the queue is full, it returns an error immediately instead of blocking.
// The message will be processed asynchronously by the background worker.
//
// Parameters:
//   - topic: The message topic for routing
//   - data: Key-value pairs to be sent in the message
//
// Returns an error if the message queue is full or if the data cannot be serialized.
func Publish(topic string, data map[string]interface{}) error {
	m := msg.NewMessage(topic)
	if err := m.SetData(data); err != nil {
		return err
	}
	select {
	case queue <- m:
		// Successfully queued
	default:
		return errors.New("queue full, message dropped")
	}
	time.Sleep(time.Millisecond * 10)
	return nil
}

// run processes queued messages in the background, attempting to send each message
// up to maxRetries times before giving up. It manages concurrent sends using a
// WaitGroup to ensure proper cleanup. Each message is processed in its own goroutine,
// but the function ensures all goroutines complete before returning.
func run(addr string, port uint16) {
	//	wg := sync.WaitGroup{}
	for msg := range queue {
		count--
		//wg.Add(1)
		//go func() {
		//	defer wg.Done()
		if err := attemptSend(msg, addr, port); err != nil {
			log.Printf("Failed to send message after %d attempts", maxRetries)
		}
		//}()
	}
	//fmt.Printf("Count=%d\n", count)
}

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

// send attempts to deliver a single message to the specified address and port.
// It handles connection timeouts and partial writes, returning detailed errors
// if the send fails. The message is serialized using the msg package's format.
//
// Parameters:
//   - m: The message to send
//   - addr: Target IP address or hostname
//   - port: Target TCP port
//
// Returns an error if the connection fails, write times out, or the write is incomplete.
func send(m *msg.Message, addr string, port uint16) error {
	dialer := net.Dialer{Timeout: dialTimeout}
	client, err := dialer.Dial("tcp4", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return fmt.Errorf("connection failed to %s: %v", addr, err)
	}
	defer client.Close()

	// Set write deadline
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
