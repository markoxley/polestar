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

// Package client provides thread-safe client management for the Polestar system.
// It implements a registry for tracking connected clients with case-insensitive
// lookups and concurrent access support. The package is designed to be used as
// part of a larger messaging system where clients can dynamically join and leave.
package client

import (
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/markoxley/polestar/config"
)

// garbageTimer defines how often to run cleanup of inactive clients.
// Clients that haven't sent a ping within this duration are removed.
// This prevents resource leaks from disconnected clients.
const (
	garbageTimer = time.Second * 30
	garbageEpoch = time.Minute * 2
)

// Clients provides thread-safe management of connected clients.
// It uses a mutex to protect concurrent access to the client registry
// and ensures case-insensitive operations by normalizing client names
// to lowercase.
type Clients struct {
	// Map of lowercase name to Client
	clients map[string]*Client
	// Protects concurrent access
	mutex sync.Mutex
}

// New creates and returns a new Clients registry.
// The registry is initialized with an empty client map
// and is ready for concurrent use.
//
// Returns:
//   - *Clients: A new, empty client registry
func New(config *config.Config) *Clients {
	c := &Clients{
		clients: make(map[string]*Client),
	}
	c.beginGarbageCollector()
	return c
}

// Add registers multiple new clients in the system.
// This is a convenience method that calls AddSingle for each client.
// The operation is atomic for each individual client but not for the
// entire batch.
//
// Parameters:
//   - clients: Variable number of Client structs to register
func (c *Clients) Add(clients ...*Client) {
	// Iterate over each client and add it to the registry
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
	// Normalize client name to lowercase
	client.Name = strings.ToLower(client.Name)
	// Set last ping timestamp to current time
	client.LastPing = time.Now()
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Add client to registry
	c.clients[client.Name] = client
}

// Remove deregisters a client from the system.
// The operation is case-insensitive and is a no-op if the
// client doesn't exist.
//
// Parameters:
//   - name: The name of the client to remove (case-insensitive)
func (c *Clients) Remove(name string) {
	// Normalize name to lowercase
	name = strings.ToLower(name)
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check if client exists
	client, exists := c.clients[name]
	if !exists {
		// Client doesn't exist, return early
		return
	}
	// Stop client
	client.Stop()
	// Remove client from registry
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
	// Normalize name to lowercase
	name = strings.ToLower(name)
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check if client exists
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
	// Normalize name to lowercase
	name = strings.ToLower(name)
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check if client exists
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
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Create new slice to store clients
	var clients []*Client
	// Iterate over clients in registry
	for _, client := range c.clients {
		// Append client to slice
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
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check if client exists
	client, exists := c.clients[strings.ToLower(name)]
	if !exists {
		// Client doesn't exist, return error
		return errors.New("client not found")
	}
	// Update last ping timestamp
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
	// Lock mutex to protect concurrent access
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Calculate epoch time
	epoch := time.Now().Add(-d)
	// Create new slice to store expired client names
	names := make([]string, 0, len(c.clients))
	// Iterate over clients in registry
	for _, client := range c.clients {
		// Check if client is expired
		if epoch.After(client.LastPing) {
			// Append client name to slice
			names = append(names, client.Name)
		}
	}
	return names
}

// beginGarbageCollector starts a background goroutine that periodically removes
// clients considered inactive (those that haven't sent a ping within the
// garbageEpoch duration). This prevents resource leaks from clients that have
// disconnected without proper cleanup. The cleanup runs every garbageTimer interval.
func (c *Clients) beginGarbageCollector() {
	go func() {
		ticker := time.NewTicker(garbageTimer)
		defer ticker.Stop()
		for range ticker.C {
			names := c.getExpired(garbageEpoch)
			if len(names) == 0 {
				// No expired clients, return early
				return
			}
			// Log cleanup message
			log.Printf("Garbage collection cleaning %d clients\n", len(names))
			// Iterate over expired client names
			for _, name := range names {
				// Remove client from registry
				c.Remove(name)
			}
		}
	}()
}
