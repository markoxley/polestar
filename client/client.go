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

// Package client provides thread-safe client management for the Thalamini system.
// It implements a registry for tracking connected clients with case-insensitive
// lookups and concurrent access support. The package is designed to be used as
// part of a larger messaging system where clients can dynamically join and leave.
package client

import (
	"fmt"
	"log"
	"net"
	"time"
)

const (
	// queueSize is the buffer size for the message queue channel.
	// Messages beyond this limit will be dropped to prevent memory exhaustion.
	queueSize = 1000

	// dialTimeout is the maximum time allowed for establishing a TCP connection.
	// Connections that take longer will be aborted.
	dialTimeout = 5 * time.Second

	// writeTimeout is the maximum time allowed for writing data to a client.
	// Writes that exceed this timeout will fail and may trigger retries.
	writeTimeout = 10 * time.Second
)

// Client represents a connected client in the Thalamini system.
// All fields are immutable after creation to ensure thread safety.
type Client struct {
	IP       string    // Network address of the client (IPv4 or IPv6)
	Port     uint16    // TCP port the client is listening on
	Name     string    // Unique identifier (stored in lowercase)
	LastPing time.Time // Timestamp of the last ping received from this client
	ch       chan []byte
}

// NewClient creates a new client instance with the given IP, port, and name.
// The client is initialized with a message queue channel and starts the send loop.
func NewClient(ip string, port uint16, name string) *Client {
	c := &Client{
		IP:       ip,
		Port:     port,
		Name:     name,
		LastPing: time.Now(),
		ch:       make(chan []byte, queueSize),
	}
	c.Run()
	return c
}

// Run starts the send loop for the client, which reads messages from the channel
// and writes them to the client connection.
func (c *Client) Run() {
	go func() {
		for m := range c.ch {
			// Establish a new TCP connection to the client
			dialer := net.Dialer{Timeout: dialTimeout}
			// Use JoinHostPort to properly handle IPv6 addresses
			addr := net.JoinHostPort(c.IP, fmt.Sprintf("%d", c.Port))
			client, err := dialer.Dial("tcp", addr)
			if err != nil {
				log.Printf("Failed to connect to %s: %v", addr, err)

				continue
			}
			defer client.Close()

			// Set write deadline
			if err := client.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				log.Printf("Failed to set write deadline: %v", err)

				continue
			}

			// Write the message to the client connection
			n, err := client.Write(m)
			if err != nil {
				log.Printf("Failed to write message: %v", err)
				continue
			}
			if n < len(m) {
				log.Printf("Failed to write message: incomplete write")
				continue
			}

		}
	}()
}

// Send sends a message to the client by adding it to the message queue channel.
// If the channel is full, the message is dropped.
func (c *Client) Send(m []byte) {
	select {
	case c.ch <- m:
		// Message was successfully added to the channel
	default:
		// Channel is full, drop the message
	}
}

// Stop stops the client by closing the message queue channel.
func (c *Client) Stop() {
	close(c.ch)
}
