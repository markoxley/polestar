// Package main provides client management functionality for the Thalamini system.
package client

import (
	"strings"
	"sync"
)

// Client represents a connected client in the Thalamini system.
// It maintains the client's network address and identification details.
// All client names are stored in lowercase for case-insensitive lookups.
type Client struct {
	IP   string // IP address of the client
	Port uint16 // Port number the client is listening on
	Name string // Unique identifier for the client
}

// Clients provides thread-safe management of connected clients.
// It supports concurrent access to client information through mutex locking.
// All client operations are case-insensitive (names are stored in lowercase).
type Clients struct {
	clients map[string]*Client // Map of client name to Client object
	mutex   sync.Mutex         // Mutex for thread-safe access
}

func New() *Clients {
	return &Clients{
		clients: make(map[string]*Client),
	}
}

// Add registers a new client in the system.
// This method is thread-safe and converts the client name to lowercase.
// The client will be added to the internal map with its lowercase name.
// Example:
//
//	clients.Add(Client{IP: "192.168.1.1", Port: 8080, Name: "Client1"})
func (c *Clients) Add(clients ...Client) {
	for _, client := range clients {
		c.AddSingle(client)
	}
}

func (c *Clients) AddSingle(client Client) {
	client.Name = strings.ToLower(client.Name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clients[client.Name] = &client
}

// Remove deregisters a client from the system.
// This method is thread-safe and removes the client by its lowercase name.
// The client name is automatically converted to lowercase before removal.
// Example:
//
//	clients.Remove("Client1")
func (c *Clients) Remove(name string) {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.clients, name)
}

// Get retrieves a client by name.
// This method is thread-safe and performs a case-insensitive lookup.
// Returns the client object and a boolean indicating if the client was found.
// Example:
//
//	client, exists := clients.Get("Client1")
func (c *Clients) Get(name string) (*Client, bool) {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	client, exists := c.clients[name]
	return client, exists
}

// Exists checks if a client with the given name exists.
// This method is thread-safe and performs a case-insensitive lookup.
// Returns true if the client exists, false otherwise.
// Example:
//
//	exists := clients.Exists("Client1")
func (c *Clients) Exists(name string) bool {
	name = strings.ToLower(name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, exists := c.clients[name]
	return exists
}

// List returns a slice containing all registered clients.
// This method is thread-safe and provides a snapshot of all current clients.
// The returned slice is a copy of the internal data and can be safely modified.
// Example:
//
//	allClients := clients.List()
func (c *Clients) List() []*Client {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var clients []*Client
	for _, client := range c.clients {
		clients = append(clients, client)
	}
	return clients
}
