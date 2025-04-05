// Package main implements a concurrent message processing hub
package main

import (
	"sync"
	"time"
)

// Constants defining the hub configuration
const (
	// QUEUE_SIZE is the buffer size for the message queue channel
	QUEUE_SIZE = 2000
	// DEFAULT_WORKER_COUNT is the default number of concurrent workers processing messages
	DEFAULT_WORKER_COUNT = 20
)

// HubQueue manages concurrent message processing with a buffered channel and worker pool
type HubQueue struct {
	messageQueue chan []byte    // Channel for queuing messages to be processed
	waitGroup    sync.WaitGroup // WaitGroup for synchronizing worker goroutines
	workerCount  int            // Number of workers to spawn
}

// New creates and returns a new Hub instance with initialized message queue
func New() *HubQueue {
	return NewWithWorkers(DEFAULT_WORKER_COUNT)
}

// NewWithWorkers creates a new Hub with a specified number of workers
func NewWithWorkers(workers int) *HubQueue {
	if workers <= 0 {
		workers = DEFAULT_WORKER_COUNT
	}
	return &HubQueue{
		messageQueue: make(chan []byte, QUEUE_SIZE),
		workerCount:  workers,
	}
}

// Run starts the worker pool with workerCount workers
// Each worker processes messages from the message queue independently
func (h *HubQueue) Run() {
	for i := 0; i < h.workerCount; i++ {
		h.waitGroup.Add(1)
		go func() {
			defer h.waitGroup.Done()
			workerRun(h.messageQueue)
		}()
	}
}

// Stop gracefully shuts down the hub by closing the message queue
// and waiting for all workers to complete their processing
func (h *HubQueue) Stop() {
	close(h.messageQueue)
	h.waitGroup.Wait()
}

// Store queues a message for processing by the worker pool
// The message is sent to the message queue channel
func (h *HubQueue) Store(message []byte) {
	h.messageQueue <- message
}

// workerRun processes messages from the provided channel
// It continues until the channel is closed
func workerRun(ch chan []byte) {
	for _ = range ch {
		time.Sleep(100 * time.Millisecond)
	}
}
