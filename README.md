# Thalamini Hub

A high-performance message routing system written in Go that provides reliable message delivery through a publish-subscribe (pub/sub) pattern with topic-based routing.

## Features

- Non-blocking message queue with backpressure handling
- Topic-based message routing with pattern matching
- Automatic client cleanup for inactive connections
- Thread-safe operations
- TCP-based communication
- Built-in connection pooling
- Automatic garbage collection of inactive clients

## Installation

```bash
go get github.com/markoxley/hub
```

## Quick Start

### Starting the Hub Server

```go
package main

import (
    "github.com/markoxley/hub"
)

func main() {
    // Create a new hub with default worker count
    h := hub.New()
    
    // Start processing messages
    h.Run()
    defer h.Stop()
    
    // Your application logic here...
}
```

### Publishing Messages

```go
package main

import (
    "github.com/markoxley/thal"
    "time"
)

func main() {
    // Initialize the publisher
    thal.Init("localhost", 8080)
    
    // Prepare message data
    data := map[string]interface{}{
        "message": "Hello, World!",
        "timestamp": time.Now(),
        "priority": 1,
    }
    
    // Publish a message to a specific topic
    err := thal.Publish("notifications", data)
    if err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}
```

### Consuming Messages

```go
package main

import (
    "github.com/markoxley/thal"
    "github.com/markoxley/dani/msg"
)

// MyConsumer implements the thal.Consumer interface
type MyConsumer struct {
    hubAddr string
    hubPort uint16
}

// Consume processes received messages
func (c *MyConsumer) Consume(m *msg.Message) {
    // Handle the message
    log.Printf("Received message: %v", m.GetData())
}

// GetHub returns the hub connection details
func (c *MyConsumer) GetHub() (string, uint16) {
    return c.hubAddr, c.hubPort
}

func main() {
    // Create a new consumer
    consumer := &MyConsumer{
        hubAddr: "localhost",
        hubPort: 8080,
    }
    
    // Start listening for messages on specific topics
    err := thal.Listen("myclient", "localhost", 9090, consumer, "topic1", "topic2")
    if err != nil {
        log.Fatalf("Failed to start consumer: %v", err)
    }
    
    // Keep the main thread running
    select {}
}
```

## Configuration

### Hub Configuration

The hub is configured using a `config.json` file. Here's an example configuration:

```json
{
    "ip": "127.0.0.1",
    "port": 24353
}
```

| Parameter | Description |
|-----------|-------------|
| ip | IP address the hub listens on |
| port | TCP port the hub listens on |

### Publisher and Consumer Configuration

#### Publisher Configuration
Publishers only require the hub connection details:
- Hub IP address
- Hub port

Example:
```go
thal.Init("127.0.0.1", 24353)
```

#### Consumer Configuration
Consumers require:
- Hub IP address and port (provided through the `GetHub` method)
- Local port to listen on
- Local IP address (optional, only needed if restricting access)

Example:
```go
// MyConsumer implements the thal.Consumer interface
type MyConsumer struct {
    hubAddr string
    hubPort uint16
}

// GetHub returns the hub connection details
func (c *MyConsumer) GetHub() (string, uint16) {
    return "127.0.0.1", 24353  // Hub connection details
}

// Start the consumer
err := thal.Listen("myclient", "", 9090, consumer, "topic1", "topic2")  // Local listening config
```

The hub connection details (IP and port) are provided by implementing the `GetHub` method in your Consumer struct, while the local listening address and port are specified in the `Listen` call.

## Architecture

The system consists of three main components:

1. **Hub**: Central message router that manages connections and message delivery
2. **Publisher**: Client that sends messages to the hub
3. **Consumer**: Client that receives messages from the hub based on topic subscriptions

### Message Flow

1. Publishers send messages to the hub
2. Hub queues messages in a bounded buffer
3. Worker pool processes messages concurrently
4. Messages are routed to subscribed consumers
5. Failed deliveries are retried automatically

## Thread Safety

All public methods are thread-safe and can be called concurrently. The system uses various synchronization primitives to ensure safe concurrent access:

- Mutex protection for client registry
- Channel-based message passing
- Atomic operations for counters
- Connection pooling for network I/O

## Error Handling

The system implements comprehensive error handling:

- Network errors trigger automatic retries
- Queue overflow errors are returned to callers
- Client errors are isolated and don't affect other clients
- Timeouts prevent resource exhaustion
- Dead clients are automatically removed

## Best Practices

1. **Configure Queue Size**: Set appropriate queue size based on your message volume and memory constraints
2. **Monitor Queue Length**: Watch for queue full errors as they indicate backpressure
3. **Handle Errors**: Always check error returns from Publish calls
4. **Clean Shutdown**: Call Stop() on consumers and hub for graceful shutdown
5. **Topic Design**: Use hierarchical topics for better message organization

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
