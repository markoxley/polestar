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
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
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

func (c *Client) Run() {
	go func() {
		for m := range c.ch {
			//time.Sleep(time.Millisecond * 10)
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

func (c *Client) Send(m []byte) {
	select {
	case c.ch <- m:
		// ok
	default:
		// drop
	}
}

func (c *Client) Stop() {
	close(c.ch)
}

// Clients provides thread-safe management of connected clients.
// It uses a mutex to protect concurrent access to the client registry
// and ensures case-insensitive operations by normalizing client names
// to lowercase.
type Clients struct {
	clients map[string]*Client // Map of lowercase name to Client
	mutex   sync.Mutex         // Protects concurrent access
}

// New creates and returns a new Clients registry.
// The registry is initialized with an empty client map
// and is ready for concurrent use.
//
// Returns:
//   - *Clients: A new, empty client registry
func New() *Clients {
	return &Clients{
		clients: make(map[string]*Client),
	}
}

// Add registers multiple new clients in the system.
// This is a convenience method that calls AddSingle for each client.
// The operation is atomic for each individual client but not for the
// entire batch.
//
// Parameters:
//   - clients: Variable number of Client structs to register
func (c *Clients) Add(clients ...*Client) {
	for _, client := range clients {
		c.AddSingle(client)
	}
}

// AddSingle registers a single new client in the system.
// The client's name is converted to lowercase before storage
// to ensure case-insensitive lookups. If a client with the
// same name already exists, it will be overwritten.
//
// Parameters:
//   - client: The Client struct to register
func (c *Clients) AddSingle(client *Client) {
	client.Name = strings.ToLower(client.Name)
	client.LastPing = time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clients[client.Name] = client
}

// Remove deregisters a client from the system.
// The operation is case-insensitive and is a no-op if the
// client doesn't exist.
//
// Parameters:
//   - name: The name of the client to remove (case-insensitive)
func (c *Clients) Remove(name string) {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	client, exists := c.clients[name]
	if !exists {
		return
	}
	client.Stop()
	delete(c.clients, name)
}

// Get retrieves a client by name.
// The lookup is case-insensitive. The returned Client pointer
// should not be modified as it is shared across goroutines.
//
// Parameters:
//   - name: The name of the client to retrieve (case-insensitive)
//
// Returns:
//   - *Client: Pointer to the client if found, nil otherwise
//   - bool: True if the client exists, false otherwise
func (c *Clients) Get(name string) (*Client, bool) {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	client, exists := c.clients[name]
	return client, exists
}

// Exists checks if a client with the given name exists.
// The lookup is case-insensitive.
//
// Parameters:
//   - name: The name to check (case-insensitive)
//
// Returns:
//   - bool: True if the client exists, false otherwise
func (c *Clients) Exists(name string) bool {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, exists := c.clients[name]
	return exists
}

// List returns a slice containing all registered clients.
// The returned slice is a new copy to ensure thread safety,
// but the Client pointers within the slice point to the
// same underlying data as the registry.
//
// Returns:
//   - []*Client: Slice of pointers to all registered clients
func (c *Clients) List() []*Client {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var clients []*Client
	for _, client := range c.clients {
		clients = append(clients, client)
	}
	return clients
}

// Ping updates the last seen timestamp for a client.
// This is called when a ping message is received from the client
// and is used to track client liveness.
//
// Parameters:
//   - name: The name of the client to update
//
// Returns:
//   - error: If the client is not found in the registry
func (c *Clients) Ping(name string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	client, exists := c.clients[strings.ToLower(name)]
	if !exists {
		return errors.New("client not found")
	}
	client.LastPing = time.Now()
	return nil
}

// getExpired returns a list of client names that haven't sent
// a ping in the last 2 minutes. These clients are considered
// inactive and will be removed during cleanup.
//
// Returns:
//   - []string: Names of expired clients
func (c *Clients) getExpired(d time.Duration) []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	epoch := time.Now().Add(-d)
	names := make([]string, 0, len(c.clients))
	for _, client := range c.clients {
		if epoch.After(client.LastPing) {
			names = append(names, client.Name)
		}
	}
	return names
}

// Cleanup removes all clients that haven't sent a ping
// in the last 2 minutes. This prevents resource leaks from
// clients that have disconnected without proper cleanup.
func (c *Clients) Cleanup(d time.Duration) {
	names := c.getExpired(d)
	if len(names) == 0 {
		return
	}
	log.Printf("Garbage collection cleaning %d clients\n", len(names))
	for _, name := range names {
		c.Remove(name)
	}
}
