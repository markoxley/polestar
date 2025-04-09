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

// Package main provides a sample publisher application demonstrating
// the usage of the Thal messaging system. It publishes a series of
// numbered messages to the "test" topic and includes timestamps.
//
// The sample demonstrates:
//   - Publisher initialization
//   - Message construction and publishing
//   - Rate-limited message sending
//   - Graceful shutdown with quit message
package main

import (
	"fmt"
	"time"

	"github.com/markoxley/dani/msg"
	"github.com/markoxley/dani/thal"
)

// main demonstrates a simple publisher that sends 10,000 messages
// to the "test" topic at a rate of approximately 1000 messages per second.
// After sending all messages, it sends a "quit" message to signal consumers
// to shut down, then waits briefly to ensure message delivery before exiting.
//
// Each message contains:
//   - A sequential message number
//   - A timestamp in nanoseconds
//
// The publisher connects to a local Thalamini hub on port 24353.
func main() {
	thal.Init("127.0.0.1", 24353)
	for i := range 10000 {
		t := time.Now().UnixNano()
		d := msg.MessageData{"data": fmt.Sprintf("message-%d", i), "time": t}
		fmt.Printf("Sending: %v\n", d)
		thal.Publish("test", d)
		time.Sleep(time.Millisecond * 1)
	}
	time.Sleep(time.Second * 5)
	thal.Publish("test", msg.MessageData{"data": "quit"})
	time.Sleep(time.Second * 5)

}
