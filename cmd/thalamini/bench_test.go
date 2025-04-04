package main

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkHub_Store(b *testing.B) {
	hub := New()
	message := []byte("benchmark message")

	hub.Run()
	defer hub.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Store(message)
	}
}

func BenchmarkHub_ParallelStore(b *testing.B) {
	hub := New()
	message := []byte("benchmark message")

	hub.Run()
	defer hub.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hub.Store(message)
		}
	})
}

func BenchmarkHub_StoreWithDifferentSizes(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("message_size_%d", size), func(b *testing.B) {
			hub := New()
			message := make([]byte, size)

			for i := range message {
				message[i] = byte(i % 256)
			}

			hub.Run()
			defer hub.Stop()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hub.Store(message)
			}
		})
	}
}

func BenchmarkHub_ConcurrentWorkers(b *testing.B) {
	workerCounts := []int{1, 5, 10, 20, 50}
	messageCount := 1000

	for _, count := range workerCounts {
		b.Run(fmt.Sprintf("workers_%d", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				hub := NewWithWorkers(count)
				message := []byte("test message")
				var wg sync.WaitGroup

				b.StartTimer()
				hub.Run()

				for j := 0; j < messageCount; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						hub.Store(message)
					}()
				}

				wg.Wait()
				hub.Stop()
			}
		})
	}
}
