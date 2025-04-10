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

package topic

import (
	"slices"
	"sync"
)

// Topic manages client subscriptions to topics in the Polestar system.
// It provides thread-safe operations for adding and removing clients from topics,
// and retrieving clients subscribed to a specific topic.
// Each topic maintains a list of clients interested in receiving messages on that topic.
type Topic struct {
	topics map[string][]string // Map of topic names to lists of subscribed clients
	mutex  sync.Mutex          // Mutex for thread-safe access to topics
}

func New() *Topic {
	return &Topic{
		topics: make(map[string][]string),
	}
}

// Add subscribes a client to one or more topics.
// This method is thread-safe and ensures that each client is only added once per topic.
// If the topic doesn't exist, it will be created automatically.
// Example:
//
//	topic.Add(client, "weather", "news")
func (t *Topic) Add(c string, topic ...string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for _, tp := range topic {
		l, exists := t.topics[tp]
		if !exists {
			l = make([]string, 0, 1)
		}
		if !slices.Contains(l, c) {
			l = append(l, c)
			t.topics[tp] = l
		}
	}
}

// Remove unsubscribes a client from all topics.
// This method is thread-safe and removes the client from all topic subscriptions.
// The client is identified by its name.
// Example:
//
//	topic.Remove("client1")
func (t *Topic) Remove(c string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for i, l := range t.topics {
		for i, nm := range l {
			if nm == c {
				l = append(l[:i], l[i+1:]...)
				break
			}
		}
		t.topics[i] = l
	}
}

// GetClients retrieves all clients subscribed to a specific topic.
// This method is thread-safe and returns a slice of clients.
// If the topic doesn't exist, an empty slice is returned.
// Example:
//
//	clients := topic.GetClients("weather")
func (t *Topic) GetClients(topic string) []string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.topics[topic]
}
