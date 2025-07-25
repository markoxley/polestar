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

// Package main implements the Polestar message hub server.
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

	"github.com/markoxley/polestar/config"
	"github.com/markoxley/polestar/hub"
)

const (
	// bufferSize defines the size of the read buffer for incoming connections.
	// This value provides a balance between memory usage and read efficiency.
	bufferSize = 2048

	// maxMessageSize defines the maximum allowed size for a single message (10MB).
	// Messages exceeding this size will be rejected to prevent memory exhaustion.
	maxMessageSize = 10 * 2048 * 2048
)

// main initializes and runs the Polestar hub server.
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
	hb := hub.New(config)
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.IP, config.Port))
	if err != nil {
		panic(err)
	}
	defer l.Close()

	hb.Run()
	showStartup(config.IP, config.Port)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v", err)
			continue // Continue accepting other connections
		}
		go handleConnection(conn, hb, config)
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
//   - config: The configuration for the server
//
// The connection is automatically closed when the function returns.
func handleConnection(c net.Conn, hb *hub.HubQueue, config *config.Config) {

	defer func() {
		c.Close()
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in connection handler: %v", r)
		}
	}()

	buffer := make([]byte, 0, bufferSize)
	totalBytes := 0

	for {
		if err := c.SetReadDeadline(time.Now().Add(time.Duration(config.ReadTimeout) * time.Millisecond)); err != nil {
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
			// Create a HubMessage and store it in the hub.
			fmt.Printf("Message from %s: %s\n", ip, string(buffer))
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

// showStartup displays a formatted banner to the console
// indicating the server has started and the address it's listening on.
func showStartup(ip string, port uint16) {
	w := 44
	address := fmt.Sprintf("http://%s:%d", ip, port)
	prefix := strings.Repeat(" ", (w-len(address))/2)
	suffix := strings.Repeat(" ", w-(len(address)+len(prefix)))
	address = prefix + address + suffix
	fmt.Println("┌────────────────────────────────────────────┐")
	fmt.Println("│                Polestar Hub                │")
	fmt.Println("│                                            │")
	fmt.Printf("│%s│\n", address)
	fmt.Println("└────────────────────────────────────────────┘")

}
