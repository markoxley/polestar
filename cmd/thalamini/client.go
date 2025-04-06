// Package main provides client management functionality for the Thalamini system.
package main

import (
	"strings"
	"sync"
)

// Client represents a connected client in the Thalamini system.
// It maintains the client's network address and identification details.
type Client struct {
	IP   string  // IP address of the client
	Port uint16  // Port number the client is listening on
	Name string  // Unique identifier for the client
}

// Clients provides thread-safe management of connected clients.
// It supports concurrent access to client information through mutex locking.
type Clients struct {
	clients map[string]Client // Map of client name to Client object
	mutex   sync.Mutex       // Mutex for thread-safe access
}

// Add registers a new client in the system.
// Client names are stored in lowercase for case-insensitive lookups.
// This operation is thread-safe.
func (c *Clients) Add(client Client) {
	client.Name = strings.ToLower(client.Name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clients[client.Name] = client
}

// Remove deregisters a client from the system.
// The client name is converted to lowercase before removal.
// This operation is thread-safe.
func (c *Clients) Remove(name string) {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.clients, name)
}

// Get retrieves a client by name.
// Returns the client object and a boolean indicating if the client was found.
// This operation is thread-safe.
func (c *Clients) Get(name string) (Client, bool) {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	client, exists := c.clients[name]
	return client, exists
}

// Exists checks if a client with the given name exists.
// The check is case-insensitive.
// This operation is thread-safe.
func (c *Clients) Exists(name string) bool {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, exists := c.clients[name]
	return exists
}

// List returns a slice containing all registered clients.
// This operation is thread-safe.
func (c *Clients) List() []Client {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var clients []Client
	for _, client := range c.clients {
		clients = append(clients, client)
	}
	return clients
}
