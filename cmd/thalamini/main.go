// Package main implements the Thalamini message hub server.
// It provides a TCP server that handles client connections and routes messages
// between registered clients.
package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/markoxley/dani/config"
	"github.com/markoxley/dani/hub"
)

const (
	// bufferSize defines the size of the read buffer for incoming connections
	bufferSize = 1024
	// maxMessageSize defines the maximum allowed size for a single message (10MB)
	maxMessageSize = 10 * 1024 * 1024
	// readTimeout defines how long to wait for data from a client
	readTimeout = 30 * time.Second
)

// main initializes and runs the Thalamini hub server.
// It sets up a TCP listener and handles incoming connections in separate goroutines.
// The server runs indefinitely until interrupted.
// Example configuration:
//
//	{
//	  "ip": "0.0.0.0",
//	  "port": 8080
//	}
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
// and forwards them to the hub for processing.
// The connection is automatically closed when the function returns.
// This method includes error handling and connection cleanup.
// Example:
//
//	go handleConnection(conn, hub)
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
			// Message complete, process it
			err = hb.Store(hub.HubMessage{
				IP:   c.RemoteAddr().String(),
				Data: buffer,
			})
			if err != nil {
				log.Printf("Failed to store message: %v", err)
			}
			return
		}
	}
}
