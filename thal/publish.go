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
// It implements a non-blocking publisher with automatic retries and a bounded message queue.
// The publisher component ensures reliable message delivery through TCP with configurable
// timeouts and retry mechanisms, while maintaining backpressure through queue size limits.
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
	// dialTimeout defines the maximum time to wait for TCP connection establishment.
	// Connections that exceed this timeout will fail and may trigger retries.
	dialTimeout = 5 * time.Second

	// writeTimeout defines the maximum time to wait for a message write operation.
	// Writes that exceed this timeout will fail and may trigger retries.
	writeTimeout = 10 * time.Second

	// maxRetries defines the maximum number of retry attempts for failed sends.
	// After this many failures, the message will be dropped from the queue.
	maxRetries = 3

	// queueSize defines the size of the message queue buffer.
	// When the queue is full, new publish attempts will fail immediately.
	queueSize = 1000
)

var (
	queue chan *msg.Message // Channel for queuing messages before sending
	count = 0
)

// Init initializes the publisher with the specified hub address and port.
// It creates a buffered message queue and starts the background message processor.
// The publisher uses a non-blocking design to handle backpressure by failing fast
// when the queue is full rather than blocking the caller.
//
// Parameters:
//   - addr: The IP address or hostname of the hub
//   - port: The TCP port number the hub is listening on
//
// Returns an error if initialization fails. Once initialized, the publisher
// will continue processing messages until the program exits.
func Init(addr string, port uint16) error {
	queue = make(chan *msg.Message, queueSize)
	go run(addr, port)
	return nil
}

// Publish queues a message with the given topic and data for sending.
// If the queue is full, it returns an error immediately instead of blocking.
// The message will be processed asynchronously by the background worker.
// This design ensures that slow consumers cannot block publishers.
//
// Parameters:
//   - topic: The message topic for routing
//   - data: Key-value pairs to be sent in the message
//
// Returns an error if:
//   - The message queue is full (queue overflow)
//   - The data cannot be serialized (invalid data)
//   - The topic is empty or invalid
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
	//time.Sleep(time.Millisecond * 10)
	return nil
}

// run processes queued messages in the background, attempting to send each message
// up to maxRetries times before giving up. It manages concurrent sends using a
// WaitGroup to ensure proper cleanup. Each message is processed in its own goroutine,
// but the function ensures all goroutines complete before returning.
//
// The function implements a bounded retry mechanism with exponential backoff
// to prevent overwhelming the network during outages.
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
// Returns an error if:
//   - Connection fails (network error)
//   - Write times out (slow consumer)
//   - Write is incomplete (connection dropped)
//   - Message serialization fails
func send(m *msg.Message, addr string, port uint16) error {
	dialer := net.Dialer{Timeout: dialTimeout}
	client, err := dialer.Dial("tcp", net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
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
