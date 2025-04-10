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
// Performance characteristics (measured):
//   - Throughput: ~840 messages/second
//   - Latency: ~1.19ms per message
//   - Queue size: 1000 messages
//   - Rate limiting: 1ms between messages
//
// The sample demonstrates:
//   - Publisher configuration
//   - High-throughput message publishing
//   - Rate-limited sending
//   - Performance monitoring
//   - Graceful shutdown
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/markoxley/dani/msg"
	"github.com/markoxley/dani/thal"
)

const msgCount = 1000000 // Number of messages to send in the test

// main demonstrates a high-performance publisher that sends messages
// to the "test" topic with configurable rate limiting. It measures
// and reports performance metrics including throughput and latency.
//
// The publisher uses the following configuration:
//   - Queue size: 1000 messages
//   - Connection timeout: 1000ms
//   - Write timeout: 2000ms
//   - Max retries: 3
//   - Rate limiting: 1ms between messages
//
// Performance metrics reported:
//   - Total duration
//   - Messages sent
//   - Messages per second
//   - Average time per message
func main() {
	cfg := &thal.PublishConfig{
		Address:      "127.0.0.1",
		Port:         24353,
		QueueSize:    1000000,
		DialTimeout:  1000,
		WriteTimeout: 2000,
		MaxRetries:   3,
	}
	if err := thal.Init(cfg); err != nil {
		log.Panic(err)
	}
	start := time.Now()
	for i := 0; i < msgCount; i++ {
		t := time.Now().UnixNano()
		d := msg.MessageData{"data": fmt.Sprintf("message-%d", i), "time": t}
		thal.Publish("test", d)
		if (i+1)%1000 == 0 {
			fmt.Println("Published", (i + 1), "messages")
		}
		time.Sleep(time.Microsecond * 10) // Rate limiting
	}

	// Calculate and display performance metrics
	du := time.Since(start)
	fmt.Println("Duration:", du)
	fmt.Println("Messages sent:", msgCount)
	fmt.Printf("Messages per second: %.2f\n", float64(msgCount)/float64(du.Seconds()))
	fmt.Printf("Average time per message: %.2f ms\n", float64(du.Milliseconds())/float64(msgCount))

	// Wait briefly to ensure all messages are delivered
	//time.Sleep(time.Second * 5)

	// Send quit message to signal consumers to shut down
	//thal.Publish("test", msg.MessageData{"data": "quit"})
	select {}
}
