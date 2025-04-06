package main

import (
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	hub := NewHub()
	if hub == nil {
		t.Error("Expected non-nil Hub instance")
	}
	if hub.messageQueue == nil {
		t.Error("Expected non-nil message queue")
	}
	if cap(hub.messageQueue) != QUEUE_SIZE {
		t.Errorf("Expected message queue capacity to be %d, got %d", QUEUE_SIZE, cap(hub.messageQueue))
	}
	if hub.workerCount != DEFAULT_WORKER_COUNT {
		t.Errorf("Expected default worker count to be %d, got %d", DEFAULT_WORKER_COUNT, hub.workerCount)
	}
}

func TestNewWithWorkers(t *testing.T) {
	testCases := []struct {
		name          string
		workerCount   int
		expectedCount int
	}{
		{"positive count", 5, 5},
		{"zero count", 0, DEFAULT_WORKER_COUNT},
		{"negative count", -1, DEFAULT_WORKER_COUNT},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hub := NewWithWorkers(tc.workerCount)
			if hub.workerCount != tc.expectedCount {
				t.Errorf("Expected worker count to be %d, got %d", tc.expectedCount, hub.workerCount)
			}
		})
	}
}

func TestHub_Store(t *testing.T) {
	hub := NewHub()
	message := []byte("test message")

	// Start a goroutine to read from the queue
	done := make(chan bool)
	go func() {
		msg := <-hub.messageQueue
		if string(msg.Data) != string(message) {
			t.Errorf("Expected message %s, got %s", string(message), string(msg.Data))
		}
		done <- true
	}()

	// Store the message
	hub.Store(HubMessage{Data: message})

	// Wait for the message to be received
	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Error("Timeout waiting for message to be processed")
	}
}

func TestHub_RunAndStop(t *testing.T) {
	hub := NewHub()
	messages := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}

	// Start the hub
	hub.Run()

	// Send test messages
	for _, msg := range messages {
		hub.Store(HubMessage{Data: msg})
	}

	// Stop the hub
	start := time.Now()
	hub.Stop()
	duration := time.Since(start)

	// Given each message takes 100ms to process and we have DEFAULT_WORKER_COUNT workers,
	// we can calculate the expected duration. Add some buffer for system overhead.
	expectedMaxDuration := time.Duration(len(messages)*100/DEFAULT_WORKER_COUNT+150) * time.Millisecond
	if duration > expectedMaxDuration {
		t.Errorf("Expected processing to complete within %v, took %v", expectedMaxDuration, duration)
	}

	// Verify the message queue is closed
	select {
	case _, ok := <-hub.messageQueue:
		if ok {
			t.Error("Expected message queue to be closed")
		}
	default:
		t.Error("Expected message queue to be closed")
	}
}

func TestConcurrentAccess(t *testing.T) {
	hub := NewHub()
	messageCount := 100
	var wg sync.WaitGroup

	hub.Run()

	// Send messages concurrently
	for i := 0; i < messageCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hub.Store(HubMessage{Data: []byte("message")})
		}(i)
	}

	// Wait for all messages to be sent
	wg.Wait()

	// Stop the hub and ensure no deadlocks
	doneChan := make(chan bool)
	go func() {
		hub.Stop()
		doneChan <- true
	}()

	select {
	case <-doneChan:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for hub to stop - possible deadlock")
	}
}
