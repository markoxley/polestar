package client

import (
	"testing"
)

func TestClientManagement(t *testing.T) {
	clients := New()

	// Test client addition
	t.Run("Add Client", func(t *testing.T) {
		testClient := Client{
			IP:   "192.168.1.1",
			Port: 8080,
			Name: "TestClient1",
		}
		clients.Add(testClient)

		// Verify client was added
		c, exists := clients.Get("testclient1") // should be lowercase
		if !exists {
			t.Error("Client was not added successfully")
		}
		if c.IP != testClient.IP || c.Port != testClient.Port {
			t.Error("Client data does not match")
		}
	})

	// Test case insensitive client retrieval
	t.Run("Case Insensitive Get", func(t *testing.T) {
		_, exists := clients.Get("TESTCLIENT1")
		if !exists {
			t.Error("Client not found with uppercase name")
		}
	})

	// Test multiple client addition
	t.Run("Add Multiple Clients", func(t *testing.T) {
		client2 := Client{IP: "192.168.1.2", Port: 8081, Name: "TestClient2"}
		client3 := Client{IP: "192.168.1.3", Port: 8082, Name: "TestClient3"}
		clients.Add(client2, client3)

		list := clients.List()
		if len(list) != 3 {
			t.Errorf("Expected 3 clients, got %d", len(list))
		}
	})

	// Test client removal
	t.Run("Remove Client", func(t *testing.T) {
		clients.Remove("TestClient2")
		if clients.Exists("TestClient2") {
			t.Error("Client was not removed successfully")
		}
	})

	// Test client listing
	t.Run("List Clients", func(t *testing.T) {
		list := clients.List()
		if len(list) != 2 {
			t.Errorf("Expected 2 clients after removal, got %d", len(list))
		}
	})
}

func TestClientConcurrency(t *testing.T) {
	clients := New()
	done := make(chan bool)

	// Test concurrent client additions
	t.Run("Concurrent Add", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			go func(id int) {
				client := Client{
					IP:   "192.168.1.1",
					Port: uint16(8080 + id),
					Name: "TestClient",
				}
				clients.Add(client)
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}

		// Only one client should exist due to same name
		list := clients.List()
		if len(list) != 1 {
			t.Errorf("Expected 1 client after concurrent adds with same name, got %d", len(list))
		}
	})
}
