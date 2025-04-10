// MIT License
//
// Copyright (c) 2025 DaggerTech
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package msg implements a binary message protocol for the Thalamini system.
// It provides efficient serialization and deserialization of messages with support
// for multiple data types, message framing, and data integrity verification through
// checksums. The protocol is optimized for low overhead in high-throughput scenarios.
package msg

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Protocol-specific byte constants used for message framing and type identification
const (
	// Message frame delimiters
	startByte1 = byte(0xff) // First byte of start marker
	startByte2 = byte(0xfe) // Second byte of start marker
	endByte1   = byte(0x05) // First byte of end marker
	endByte2   = byte(0xfd) // Second byte of end marker

	// Message type identifiers
	RegMsgByte  = byte(0xfe) // Registration message type
	DataMsgByte = byte(0xff) // Data message type
	PingMsgByte = byte(0xfd) // Ping message type for client liveness tracking
	// Message part delimiters
	endPartByte     = byte(0x02) // End of message part marker
	endDataPartByte = byte(0x03) // End of data section marker
	splitDataByte   = byte(0x04) // Data field separator

	// Data type identifiers
	dtString = byte(0x11) // String data type
	dtInt    = byte(0x12) // Integer data type (64-bit)
	dtFloat  = byte(0x13) // Float data type (64-bit)
	dtBool   = byte(0x14) // Boolean data type

	// Boolean value encodings
	vlTrue  = byte(0x15) // True value marker
	vlFalse = byte(0x16) // False value marker
)

// Pre-allocated byte slices for common protocol elements
var (
	startBytes = []byte{startByte1, startByte2} // Start of message marker
	endBytes   = []byte{endByte1, endByte2}     // End of message marker
)

// MessageData represents a key-value store for message payload data.
// It supports the following data types:
//   - string
//   - int64
//   - float64
//   - bool
//
// Any other data types will result in an error during serialization.
type MessageData map[string]interface{}

// Message represents a communication packet in the hub system.
// It implements a binary protocol with the following format:
//
//	[start marker (2B)] [length (2B)] [topic] [delimiter] [type (1B)]
//	[data] [delimiter] [checksum (1B)] [end marker (2B)]
type Message struct {
	MsgType byte   // Type of message (RegMsgByte or DataMsgByte)
	Topic   string // Routing topic for the message
	data    []byte // Raw message payload
}

// NewMessage creates a new data message with the specified topic.
// The message is initialized with DataMsgByte type and empty payload.
//
// Parameters:
//   - topic: The routing topic for this message
//
// Returns:
//   - *Message: A new message instance ready for data
func NewMessage(topic string) *Message {
	return &Message{
		Topic:   topic,
		MsgType: DataMsgByte,
	}
}

// NewRegistrationMessage creates a registration message for a service.
// The message includes the service's listening port and subscribed topics.
// The data is encoded as: [port (2B)] [name,topic1,topic2,...]
//
// Parameters:
//   - port: TCP port the service is listening on
//   - name: Unique identifier for the service
//   - topics: List of topics to subscribe to
//
// Returns:
//   - *Message: A registration message ready for transmission
func NewRegistrationMessage(port uint16, name string, topics ...string) *Message {
	data := append([]string{name}, topics...)
	return &Message{
		Topic:   "subscription",
		MsgType: RegMsgByte,
		data:    append([]byte{byte(port), byte(port >> 8)}, []byte(strings.Join(data, ","))...),
	}
}

// NewPingMessage creates a new ping message for client liveness tracking.
// The ping message contains the client's name and is sent periodically
// to inform the hub that the client is still active.
//
// Parameters:
//   - name: The name of the client sending the ping
//
// Returns:
//   - *Message: A new ping message ready to be sent to the hub
func NewPingMessage(name string) *Message {
	return &Message{
		Topic:   "ping",
		MsgType: PingMsgByte,
		data:    []byte(name),
	}
}

// SetData encodes the provided data map into the message's binary format.
// Each key-value pair is encoded as:
//
//	[key] [separator] [type] [value] [delimiter]
//
// Parameters:
//   - data: Map of key-value pairs to encode
//
// Returns:
//   - error: nil if successful, or error if data contains unsupported types
func (m *Message) SetData(data MessageData) error {
	buffer := bytes.NewBuffer(nil)
	for k, v := range data {
		buffer.Write([]byte(k))
		buffer.WriteByte(splitDataByte)

		switch val := v.(type) {
		case string:
			buffer.WriteByte(dtString)
			buffer.Write([]byte(val))
		case int64:
			buffer.WriteByte(dtInt)
			buffer.Write([]byte(fmt.Sprintf("%d", val)))
		case float64:
			buffer.WriteByte(dtFloat)
			buffer.Write([]byte(fmt.Sprintf("%f", val)))
		case bool:
			buffer.WriteByte(dtBool)
			if val {
				buffer.WriteByte(vlTrue)
			} else {
				buffer.WriteByte(vlFalse)
			}
		default:
			return errors.New("unsupported data type")
		}
		buffer.WriteByte(endDataPartByte)
	}
	m.data = buffer.Bytes()
	return nil
}

// Data decodes the message's binary data into a MessageData structure.
// It processes each key-value pair according to its type marker and
// converts the values back to their original Go types.
//
// Returns:
//   - MessageData: Decoded key-value pairs
//   - error: nil if successful, or error if the data format is invalid
func (m *Message) Data() (MessageData, error) {
	md := make(MessageData)
	buffer := make([]byte, 0, len(m.data))
	for _, b := range m.data {
		if b == endDataPartByte {
			s := strings.Split(string(buffer), string(splitDataByte))
			if len(s) != 2 {
				return nil, errors.New("invalid data format")
			}
			if len(s[1]) == 0 {
				return nil, errors.New("invalid data format")
			}
			tp := s[1][0]
			val := s[1][1:]
			switch tp {
			case dtString:
				md[string(s[0])] = string(val)
			case dtInt:
				v, err := strconv.ParseInt(string(val), 10, 64)
				if err != nil {
					return nil, err
				}
				md[string(s[0])] = v
			case dtFloat:
				v, err := strconv.ParseFloat(string(val), 64)
				if err != nil {
					return nil, err
				}
				md[string(s[0])] = v
			case dtBool:
				v := val[0] == vlTrue
				md[string(s[0])] = v
			default:
				return nil, errors.New("unsupported data type")
			}
			buffer = buffer[:0]
			continue
		}
		buffer = append(buffer, b)
	}
	return md, nil
}

// Raw returns the message's raw binary payload data.
// This method is primarily used internally by the hub for
// message transmission and should not be modified externally.
//
// Returns:
//   - []byte: Raw binary payload data
func (m *Message) Raw() []byte {
	return m.data
}

// Serialize encodes the entire message into a binary format.
// The format includes framing, length, topic, type, payload, and checksum.
// Message format:
//
//	[start (2B)] [length (2B)] [topic] [delimiter] [type (1B)]
//	[data] [delimiter] [checksum (1B)] [end (2B)]
//
// Returns:
//   - []byte: Complete serialized message
func (m *Message) Serialize() []byte {
	// Calculate size of message parts
	topicLen := len(m.Topic)
	dataLen := len(m.data)

	// Total size = start(2) + length(2) + source + endPart(1) + destinations + endPart(1) + type(1) + data + endPart(1) + checksum(1) + end(2)
	totalLen := 2 + 2 + topicLen + 1 + 1 + dataLen + 1 + 1 + 2

	// Create buffer with capacity
	buffer := make([]byte, 0, totalLen)

	// Start bytes
	buffer = append(buffer, startBytes...)

	// Length (2 bytes, little endian)
	buffer = append(buffer, byte(totalLen))
	buffer = append(buffer, byte(totalLen>>8))

	// Topic
	buffer = append(buffer, []byte(m.Topic)...)
	buffer = append(buffer, endPartByte)

	// Type and data
	buffer = append(buffer, m.MsgType)
	buffer = append(buffer, m.data...)
	buffer = append(buffer, endPartByte)

	// Checksum
	buffer = append(buffer, m.generateChecksum(buffer))

	// End bytes
	buffer = append(buffer, endBytes...)

	return buffer
}

// Deserialize decodes a binary message into this Message instance.
// It verifies the message format, framing, and checksum.
//
// Parameters:
//   - data: Raw binary message to decode
//
// Returns:
//   - error: nil if successful, or error describing the validation failure
func (m *Message) Deserialize(data []byte) error {
	l := len(data)
	if l < 10 {
		return errors.New("invalid message length")
	}
	if data[0] != startBytes[0] || data[1] != startBytes[1] {
		return errors.New("invalid message start bytes")
	}
	if data[l-2] != endBytes[0] || data[l-1] != endBytes[1] {
		return errors.New("invalid message end bytes")
	}

	length := int(data[2]) | int(data[3])<<8
	if length != l {
		return errors.New("invalid message length")
	}

	if data[l-3] != m.generateChecksum(data[:l-3]) {
		return errors.New("invalid message checksum")
	}

	// Parse message parts
	parts := make([][]byte, 0, 3)
	start := 4                     // Skip start bytes and length
	for i := start; i < l-3; i++ { // -3 for checksum and end bytes
		if data[i] == endPartByte {
			parts = append(parts, data[start:i])
			start = i + 1
		}
	}

	if len(parts) != 2 {
		return errors.New("invalid message structure")
	}

	m.Topic = string(parts[0])
	if len(parts[1]) < 1 {
		return errors.New("invalid message structure")
	}
	m.MsgType = parts[1][0]
	m.data = parts[1][1:]

	return nil
}

// generateChecksum calculates a simple XOR checksum of the message data.
// The checksum covers all bytes except the checksum itself and end marker.
//
// Parameters:
//   - data: Byte slice to calculate checksum for
//
// Returns:
//   - byte: Calculated checksum value
func (m *Message) generateChecksum(data []byte) byte {
	var checksum byte
	for i := 0; i < len(data)-3; i++ { // -3 to exclude checksum and end bytes
		checksum ^= data[i]
	}
	return checksum
}

func Split(data []byte) []*Message {
	res := make([]*Message, 0)
	for len(data) > 0 {
		if len(data) < 10 {
			break
		}
		m := &Message{}
		l := int(data[2]) | int(data[3])<<8
		if l > len(data) {
			break
		}
		err := m.Deserialize(data[:l])
		if err != nil {
			break
		}
		res = append(res, m)
		data = data[l:]
	}
	return res
}
