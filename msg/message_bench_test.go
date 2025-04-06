package msg

import (
	"testing"
)

var (
	benchMsg = &Message{
		Source:      "BenchService",
		Destination: []string{"Dest1", "Dest2"},
		MsgType:     DataMsgByte,
	}

	benchData = MessageData{
		"string": "test value",
		"int":    int64(42),
		"float":  float64(3.14159),
		"bool":   true,
	}

	benchBytes []byte // Used to store serialized message
)

// BenchmarkSetData measures the performance of setting data in a message
func BenchmarkSetData(b *testing.B) {
	msg := &Message{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.SetData(benchData)
	}
}

// BenchmarkData measures the performance of retrieving data from a message
func BenchmarkData(b *testing.B) {
	msg := &Message{}
	msg.SetData(benchData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.Data()
	}
}

// BenchmarkSerialize measures the performance of message serialization
func BenchmarkSerialize(b *testing.B) {
	msg := &Message{
		Source:      "BenchService",
		Destination: []string{"Dest1", "Dest2"},
		MsgType:     DataMsgByte,
	}
	msg.SetData(benchData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchBytes = msg.Serialize()
	}
}

// BenchmarkDeserialize measures the performance of message deserialization
func BenchmarkDeserialize(b *testing.B) {
	msg1 := &Message{
		Source:      "BenchService",
		Destination: []string{"Dest1", "Dest2"},
		MsgType:     DataMsgByte,
	}
	msg1.SetData(benchData)
	bytes := msg1.Serialize()

	msg2 := &Message{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg2.Deserialize(bytes)
	}
}

// BenchmarkNewMessage measures the performance of creating new messages
func BenchmarkNewMessage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewMessage("BenchService")
	}
}

// BenchmarkNewRegistrationMessage measures the performance of creating new registration messages
func BenchmarkNewRegistrationMessage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewRegistrationMessage("BenchService", 1234)
	}
}

// BenchmarkFullMessageCycle measures the performance of a complete message cycle:
// create -> set data -> serialize -> deserialize -> get data
func BenchmarkFullMessageCycle(b *testing.B) {
	var (
		bytes []byte
		data  MessageData
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg1 := NewMessage("BenchService")
		msg1.Destination = []string{"Dest1", "Dest2"}
		msg1.SetData(benchData)
		bytes = msg1.Serialize()

		msg2 := &Message{}
		msg2.Deserialize(bytes)
		data, _ = msg2.Data()
	}
	// Prevent compiler optimization
	benchBytes = bytes
	_ = data
}

// BenchmarkLargeMessage measures the performance with a large message
func BenchmarkLargeMessage(b *testing.B) {
	largeData := make(MessageData)
	for i := 0; i < 100; i++ {
		largeData[string(rune(i))] = string(make([]byte, 1000))
	}

	msg := NewMessage("BenchService")
	msg.Destination = []string{"Dest1", "Dest2"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.SetData(largeData)
		benchBytes = msg.Serialize()
		msg.Deserialize(benchBytes)
		_, _ = msg.Data()
	}
}
