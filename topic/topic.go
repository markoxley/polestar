package topic

import (
	"slices"
	"sync"
)

// Topic manages client subscriptions to topics in the Thalamini system.
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
