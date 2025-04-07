// Package client provides thread-safe client management for the Thalamini system.
// It implements a registry for tracking connected clients with case-insensitive
// lookups and concurrent access support. The package is designed to be used as
// part of a larger messaging system where clients can dynamically join and leave.
package client

import (
	"strings"
	"sync"
)

// Client represents a connected client in the Thalamini system.
// All fields are immutable after creation to ensure thread safety.
type Client struct {
	IP   string // Network address of the client (IPv4 or IPv6)
	Port uint16 // TCP port the client is listening on
	Name string // Unique identifier (stored in lowercase)
}

// Clients provides thread-safe management of connected clients.
// It uses a mutex to protect concurrent access to the client registry
// and ensures case-insensitive operations by normalizing client names
// to lowercase.
type Clients struct {
	clients map[string]*Client // Map of lowercase name to Client
	mutex   sync.Mutex        // Protects concurrent access
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
func (c *Clients) Add(clients ...Client) {
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
func (c *Clients) AddSingle(client Client) {
	client.Name = strings.ToLower(client.Name)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clients[client.Name] = &client
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
