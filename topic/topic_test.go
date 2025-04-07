package topic

import (
	"testing"
)

func TestTopicManagement(t *testing.T) {
	topics := New()

	// Test topic subscription
	t.Run("Subscribe to Topic", func(t *testing.T) {
		topics.Add("client1", "weather", "news")
		clients := topics.GetClients("weather")
		if len(clients) != 1 {
			t.Error("Client was not subscribed to topic")
		}
		if clients[0] != "client1" {
			t.Error("Wrong client was subscribed to topic")
		}
	})

	// Test multiple topic subscriptions
	t.Run("Multiple Topic Subscriptions", func(t *testing.T) {
		topics.Add("client2", "weather", "sports", "tech")
		
		// Check weather topic
		weatherClients := topics.GetClients("weather")
		if len(weatherClients) != 2 {
			t.Errorf("Expected 2 clients in weather topic, got %d", len(weatherClients))
		}

		// Check sports topic
		sportsClients := topics.GetClients("sports")
		if len(sportsClients) != 1 {
			t.Errorf("Expected 1 client in sports topic, got %d", len(sportsClients))
		}
	})

	// Test duplicate subscription prevention
	t.Run("Prevent Duplicate Subscriptions", func(t *testing.T) {
		topics.Add("client1", "weather")
		clients := topics.GetClients("weather")
		if len(clients) != 2 {
			t.Error("Duplicate subscription was added")
		}
	})

	// Test client removal
	t.Run("Remove Client", func(t *testing.T) {
		topics.Remove("client1")
		
		// Check weather topic
		weatherClients := topics.GetClients("weather")
		if len(weatherClients) != 1 {
			t.Errorf("Expected 1 client after removal, got %d", len(weatherClients))
		}

		// Check if client was removed from all topics
		newsClients := topics.GetClients("news")
		if len(newsClients) != 0 {
			t.Error("Client was not removed from all topics")
		}
	})
}

func TestTopicConcurrency(t *testing.T) {
	topics := New()
	done := make(chan bool)

	// Test concurrent topic subscriptions
	t.Run("Concurrent Subscriptions", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			go func(id int) {
				clientName := "client1"
				topics.Add(clientName, "concurrent-topic")
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}

		// Should only have one subscription despite concurrent adds
		clients := topics.GetClients("concurrent-topic")
		if len(clients) != 1 {
			t.Errorf("Expected 1 client after concurrent adds, got %d", len(clients))
		}
	})

	// Test concurrent topic operations
	t.Run("Concurrent Add and Remove", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			go func(id int) {
				topics.Add("concurrent-client", "mixed-topic")
				done <- true
			}(i)
			go func(id int) {
				topics.Remove("concurrent-client")
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}
