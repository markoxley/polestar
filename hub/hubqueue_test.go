package hub

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/markoxley/dani/msg"
)

// mockClient simulates a client for testing
type mockClient struct {
	listener net.Listener
	received [][]byte
	wg       sync.WaitGroup
}

func newMockClient(t *testing.T) *mockClient {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create mock client: %v", err)
	}

	mc := &mockClient{
		listener: listener,
		received: make([][]byte, 0),
	}

	// Start accepting connections
	mc.wg.Add(1)
	go func() {
		defer mc.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener closed
			}

			// Handle connection
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				n, err := c.Read(buf)
				if err != nil {
					return
				}
				mc.received = append(mc.received, buf[:n])
			}(conn)
		}
	}()

	return mc
}

func (mc *mockClient) close() {
	mc.listener.Close()
	mc.wg.Wait()
}

func (mc *mockClient) addr() string {
	return mc.listener.Addr().String()
}

func TestHubQueue(t *testing.T) {
	hub := NewWithWorkers(2) // Create hub with 2 workers
	hub.Run()
	defer hub.Stop()

	// Test client registration
	t.Run("Register Client", func(t *testing.T) {
		mock := newMockClient(t)
		defer mock.close()

		q := New()
		q.Run()
		defer q.Stop()

		// Create registration message
		regMsg := msg.Message{
			MsgType: msg.RegMsgByte,
		}

		err := q.Store(HubMessage{
			IP:   "127.0.0.1",
			Data: regMsg.Serialize(),
		})
		if err != nil {
			t.Fatalf("Failed to register client: %v", err)
		}

		// Wait a bit for registration to complete
		time.Sleep(100 * time.Millisecond)

		// Send test message
		msg := HubMessage{
			IP:   "127.0.0.1",
			Data: []byte("test message"),
		}
		err = q.Store(msg)
		if err != nil {
			t.Fatalf("Failed to store message: %v", err)
		}

		// Wait for message with timeout
		timeout := time.After(2 * time.Second)
		for len(mock.received) == 0 {
			select {
			case <-timeout:
				t.Fatal("Timeout waiting for message")
				return
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}

		if string(mock.received[0]) != "test message" {
			t.Errorf("Expected 'test message', got '%s'", string(mock.received[0]))
		}
	})

	// Test message queueing
	t.Run("Message Queue", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			msg := HubMessage{
				IP:   "127.0.0.1",
				Data: []byte(fmt.Sprintf("message-%d", i)),
			}
			err := hub.Store(msg)
			if err != nil {
				t.Errorf("Failed to store message %d: %v", i, err)
			}
		}
	})

	// Test worker pool
	t.Run("Worker Pool", func(t *testing.T) {
		hub := NewWithWorkers(3)
		if hub.workerCount != 3 {
			t.Errorf("Expected 3 workers, got %d", hub.workerCount)
		}
	})
}

func TestHubQueueConcurrency(t *testing.T) {
	hub := NewWithWorkers(4)
	hub.Run()
	defer hub.Stop()

	// Test concurrent message storage
	t.Run("Concurrent Message Storage", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				msg := HubMessage{
					IP:   "127.0.0.1",
					Data: []byte(fmt.Sprintf("concurrent-message-%d", id)),
				}
				err := hub.Store(msg)
				if err != nil {
					t.Errorf("Failed to store concurrent message %d: %v", id, err)
				}
			}(i)
		}
		wg.Wait()
	})
}
