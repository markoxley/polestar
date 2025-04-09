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

// Package main provides a sample consumer application demonstrating
// the usage of the Thal messaging system. It implements a simple message
// consumer that subscribes to the "test" topic and processes messages
// until receiving a quit signal.
//
// The sample demonstrates:
//   - Consumer implementation and registration
//   - Message handling with channels
//   - Graceful shutdown on quit message
//   - Error handling for malformed messages
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/markoxley/dani/msg"
	"github.com/markoxley/dani/thal"
)

// consumerSample implements the thal.Consumer interface and provides
// a channel-based message processing system. It demonstrates a pattern
// for handling messages asynchronously while maintaining clean shutdown.
type consumerSample struct {
	quit bool                // Flag to signal shutdown
	ch   chan *msg.Message  // Channel for passing messages to processor
}

// Consume implements the thal.Consumer interface. It receives messages
// from the Thal system and forwards them to the processing channel.
// This method is called concurrently by the Thal system, so it must
// be thread-safe.
func (c *consumerSample) Consume(m *msg.Message) {
	c.ch <- m
}

// Run processes messages from the channel until receiving a quit message.
// It demonstrates proper message handling including:
//   - Data extraction and error handling
//   - Type assertion safety
//   - Graceful shutdown on quit message
//   - Continuous message processing
func (c *consumerSample) Run() {
	for m := range c.ch {
		d, err := m.Data()
		if err != nil {
			log.Printf("error: %s", err)
		}
		fmt.Println(d)
		data, ok := d["data"]
		if !ok {
			continue
		}
		if q, ok := data.(string); ok && q == "quit" {
			os.Exit(0)
		}
	}
}

// GetHub implements the thal.Consumer interface by returning the
// address and port of the Thalamini hub. This consumer connects
// to a local hub on the default port.
func (c *consumerSample) GetHub() (string, uint16) {
	return "127.0.0.1", 24353
}

// main initializes and runs the sample consumer. It:
//   1. Creates a new consumer with a buffered message channel
//   2. Registers with the hub on the "test" topic
//   3. Starts message processing in a separate goroutine
//   4. Waits for shutdown signal
//
// The consumer listens on localhost:8080 and connects to a
// Thalamini hub at 127.0.0.1:24353.
func main() {
	c := &consumerSample{
		ch: make(chan *msg.Message, 1000),
	}

	c.quit = false
	if err := thal.Listen("sample", "127.0.0.1", 8080, c, "test"); err != nil {
		log.Panic(err)
	}
	fmt.Println("listening on 127.0.0.1:8080")
	go c.Run()
	for !c.quit {
		time.Sleep(100 * time.Millisecond)
	}
}
