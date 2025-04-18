# Polestar Advanced Usage Guide

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Performance Tuning](#performance-tuning)
3. [Advanced Usage Patterns](#advanced-usage-patterns)
4. [Troubleshooting](#troubleshooting)
5. [Best Practices](#best-practices)

## System Architecture

### Core Components

#### 1. Hub
The central message router that handles:
- Message routing and distribution
- Client connection management
- Topic-based subscription matching
- Health monitoring

```go
// Initialize the hub with performance-optimized configuration
cfg := &config.Config{
    QueueSize:   1000000,  // Handle >10k msg/sec
    WorkerCount: 100,      // Parallel processing
    DialTimeout: 1000,     // Quick connection establishment
}
hub := hub.New(cfg)
hub.Run()
```

#### 2. Publisher
Handles message publishing with:
- Asynchronous message queuing
- Connection pooling
- Automatic retry logic
- Backpressure handling

```go
// Configure publisher for high throughput
cfg := &star.PublishConfig{
    QueueSize:    100000,  // Large queue for bursts
    DialTimeout:  500,     // Fast connection timeout
    WriteTimeout: 1000,    // Quick write timeout
    MaxRetries:   2,       // Minimal retries for fresh data
}
```

#### 3. Consumer
Manages message consumption with:
- Topic-based subscription
- Automatic reconnection
- Health monitoring
- Message filtering

```go
// Configure consumer for reliable processing
cfg := &star.ConsumerConfig{
    Name:      "metrics_processor",
    QueueSize: 10000,
    Topics:    []string{"metrics.*"},
}
```

## Performance Tuning

### Queue Sizing
Optimize queue sizes based on your use case:
- High throughput: Large queues (1,000,000+)
- Low latency: Smaller queues (1,000-10,000)
- Memory constrained: Minimal queues (100-1,000)

```go
// High throughput configuration
cfg := &config.Config{
    QueueSize:       1000000,
    ClientQueueSize: 100000,
}

// Low latency configuration
cfg := &config.Config{
    QueueSize:       10000,
    ClientQueueSize: 1000,
    DialTimeout:     500,
    WriteTimeout:    1000,
}
```

### Worker Pool Tuning
Adjust worker counts based on:
- CPU core count
- I/O patterns
- Message complexity

```go
// CPU-bound processing
cfg := &config.Config{
    WorkerCount:       runtime.NumCPU(),
    ClientWorkerCount: runtime.NumCPU() / 2,
}

// I/O-bound processing
cfg := &config.Config{
    WorkerCount:       runtime.NumCPU() * 4,
    ClientWorkerCount: runtime.NumCPU() * 2,
}
```

### Network Optimization
Fine-tune network parameters:
```go
cfg := &config.Config{
    DialTimeout:  500,    // Fast connection establishment
    WriteTimeout: 1000,   // Quick writes
    ReadTimeout:  5000,   // Balanced reads
    MaxRetries:   2,      // Minimal retry overhead
}
```

## Advanced Usage Patterns

### Topic Patterns
Implement efficient topic hierarchies:
```go
// Weather data routing
publisher.Publish("weather.temperature.london", data)
publisher.Publish("weather.humidity.london", data)

// Consumer subscribing to all London weather
consumer.Subscribe("weather.*.london")

// Consumer subscribing to all temperature readings
consumer.Subscribe("weather.temperature.*")
```

### Message Batching
Optimize throughput with batching:
```go
batch := make([]msg.Message, 0, 100)
for i := 0; i < 100; i++ {
    batch = append(batch, createMessage())
    if len(batch) == cap(batch) {
        publishBatch(batch)
        batch = batch[:0]
    }
}
```

### Error Handling
Implement robust error handling:
```go
type MyConsumer struct {
    errorCount int64
    mu         sync.Mutex
}

func (c *MyConsumer) Consume(m *msg.Message) {
    data, err := m.Data()
    if err != nil {
        c.handleError(err)
        return
    }
    
    if err := c.processMessage(data); err != nil {
        c.handleError(err)
        // Implement circuit breaker if needed
        if c.shouldPause() {
            time.Sleep(time.Second)
        }
    }
}
```

## Troubleshooting

### Common Issues

1. Queue Overflow
```go
// Monitor queue capacity
metrics := hub.GetMetrics()
if metrics.QueueCapacity > 0.8 {
    log.Printf("Queue near capacity: %.2f%%", metrics.QueueCapacity*100)
}
```

2. Connection Issues
```go
// Implement connection monitoring
type ConnectionMonitor struct {
    failures int64
    lastSuccess time.Time
}

func (m *ConnectionMonitor) OnFailure() {
    atomic.AddInt64(&m.failures, 1)
    if m.shouldReconnect() {
        m.reconnectWithBackoff()
    }
}
```

3. Message Loss
```go
// Implement message tracking
type MessageTracker struct {
    sent     map[string]time.Time
    received map[string]time.Time
    mu       sync.RWMutex
}

func (t *MessageTracker) TrackMessage(id string) {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.sent[id] = time.Now()
}
```

## Best Practices

### 1. Message Design
- Keep messages small and focused
- Use efficient serialization
- Include metadata for tracking

### 2. Topic Design
- Use hierarchical structures
- Keep topics granular
- Document topic patterns

### 3. Error Handling
- Implement circuit breakers
- Use exponential backoff
- Log errors comprehensively

### 4. Monitoring
- Track queue sizes
- Monitor message latency
- Watch error rates

### 5. Testing
- Test with production-like loads
- Verify message ordering
- Check error handling

### 6. Security
- Use TLS for sensitive data
- Implement authentication
- Validate message sources

## Performance Benchmarks

Typical performance metrics:
- Throughput: >10,000 messages/second
- Latency: ~0.06ms per message
- Queue Capacity: 1,000,000 messages
- Worker Pool: 100 concurrent workers

To achieve these metrics:
1. Use appropriate queue sizes
2. Tune worker pools
3. Optimize network settings
4. Monitor system resources
5. Profile under load

## Additional Resources

- [API Documentation](./API.md)
- [Configuration Guide](./CONFIG.md)
- [Performance Tuning Guide](./PERFORMANCE.md)
- [Security Guide](./SECURITY.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)
