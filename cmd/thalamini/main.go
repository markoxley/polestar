// Package main implements the Thalamini message hub server.
// It provides a TCP server that handles client connections and routes messages
// between registered clients. The server supports dynamic client registration,
// topic-based message routing, and handles high-throughput message processing.
package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/markoxley/dani/config"
	"github.com/markoxley/dani/hub"
)

const (
	// bufferSize defines the size of the read buffer for incoming connections.
	// This value provides a balance between memory usage and read efficiency.
	bufferSize = 1024

	// maxMessageSize defines the maximum allowed size for a single message (10MB).
	// Messages exceeding this size will be rejected to prevent memory exhaustion.
	maxMessageSize = 10 * 1024 * 1024

	// readTimeout defines how long to wait for data from a client before closing
	// the connection. This prevents idle connections from consuming resources.
	readTimeout = 30 * time.Second
)

// main initializes and runs the Thalamini hub server.
// It sets up a TCP listener and handles incoming connections in separate goroutines.
// The server runs indefinitely until interrupted.
//
// Configuration is loaded from a JSON file with the following structure:
//
//	{
//	  "ip": "0.0.0.0",    // IP address to bind to
//	  "port": 8080        // Port to listen on
//	}
//
// If configuration loading fails, the program will panic.
func main() {
	config := config.MustLoad()
	hb := hub.New()
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.IP, config.Port))
	if err != nil {
		panic(err)
	}
	defer l.Close()

	hb.Run()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue // Continue accepting other connections
		}
		go handleConnection(conn, hb)
	}
}

// handleConnection processes a single client connection.
// It reads incoming data in chunks, assembles complete messages,
// and forwards them to the hub for processing. The method implements
// various safety measures:
//   - Message size limits to prevent memory exhaustion
//   - Read timeouts to handle stale connections
//   - Panic recovery to prevent connection handler crashes
//   - Automatic connection cleanup
//
// Parameters:
//   - c: The TCP connection to handle
//   - hb: The hub instance for message processing
//
// The connection is automatically closed when the function returns.
func handleConnection(c net.Conn, hb *hub.HubQueue) {

	defer func() {
		c.Close()
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in connection handler: %v", r)
		}
	}()

	buffer := make([]byte, 0, bufferSize)
	totalBytes := 0

	for {
		if err := c.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			log.Printf("Failed to set read deadline: %v", err)
			return
		}

		chunk := make([]byte, bufferSize)
		n, err := c.Read(chunk)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from connection: %v", err)
			}
			return
		}

		totalBytes += n
		if totalBytes > maxMessageSize {
			log.Printf("Message exceeds maximum size of %d bytes", maxMessageSize)
			return
		}

		buffer = append(buffer, chunk[:n]...)

		if n < bufferSize {
			ip := c.RemoteAddr().String()
			ip = ip[:strings.Index(ip, ":")]
			// Message complete, process it
			err = hb.Store(hub.HubMessage{
				IP:   ip,
				Data: buffer,
			})
			if err != nil {
				log.Printf("Failed to store message: %v", err)
			}
			return
		}
	}
}
