// Package msg provides message handling functionality for inter-service communication
// within the hub system. It implements a custom binary message protocol for efficient
// data transfer between components.
package msg

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Protocol-specific byte constants used for message framing and data type identification
var (
	// Message frame delimiters
	startBytes = []byte{0xFE, 0xFF} // Start of message marker
	endBytes   = []byte{0x05, 0xFD} // End of message marker

	// Message type identifiers
	RegMsgByte  = byte(0xfe) // Registration message type
	DataMsgByte = byte(0xff) // Data message type

	// Message part delimiters
	endPartByte     = byte(0x02) // End of message part marker
	endDataPartByte = byte(0x03) // End of data section marker
	splitDataByte   = byte(0x04) // Data field separator

	// Data type identifiers
	dtString = byte(0x11) // String data type
	dtInt    = byte(0x12) // Integer data type
	dtFloat  = byte(0x13) // Float data type
	dtBool   = byte(0x14) // Boolean data type

	vlTrue  = byte(0x15)
	vlFalse = byte(0x16)
)

// MessageData represents a key-value store for message payload data
// It supports various data types including strings, integers, floats, and booleans.
type MessageData map[string]interface{}

// Message represents a communication packet in the hub system
// It contains metadata about the message type and topic, as well as the actual payload data.
type Message struct {
	MsgType byte // Type of the message (registration or data)
	//Source      string   // Source identifier of the message
	//Destination []string // List of destination identifiers
	Topic string
	data  []byte // Raw message payload data
}

// NewMessage creates a new data message with the specified topic
// This method initializes a new Message with the data message type.
// Example:
//   msg := NewMessage("weather")
func NewMessage(topic string) *Message {
	return &Message{
		Topic:   topic,
		MsgType: DataMsgByte,
	}
}

// NewRegistrationMessage creates a new registration message for a service
// with the specified source and port number
// This method initializes a new Message with the registration message type
// and includes the service's port and topics in the message data.
// Example:
//   msg := NewRegistrationMessage(8080, "weather-service", "weather", "forecast")
func NewRegistrationMessage(port uint16, name string, topics ...string) *Message {
	data := append([]string{name}, topics...)
	return &Message{
		Topic:   "subscription",
		MsgType: RegMsgByte,
		data:    append([]byte{byte(port), byte(port >> 8)}, []byte(strings.Join(data, ","))...),
	}
}

// SetData sets the message's payload data using a MessageData structure
// It converts the data into a binary format for transmission.
// Returns an error if the data contains unsupported types.
// Example:
//   data := MessageData{
//     "temperature": 25.5,
//     "humidity":   60,
//     "location":   "London",
//   }
//   err := msg.SetData(data)
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

// Data deserializes the message's internal data format into a MessageData structure
// Returns an error if the data format is invalid or contains unsupported types
// Example:
//   data, err := msg.Data()
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

// Raw returns the raw binary data of the message
// This method is used internally by the hub for message transmission.
// Example:
//   rawData := msg.Raw()
func (m *Message) Raw() []byte {
	return m.data
}

// Serialize converts the Message into a byte slice according to the protocol
// The format is: [start bytes (2)] [length (2)] [source] [end part] [destinations] [end part] [type (1)] [data] [end part] [checksum (1)] [end bytes (2)]
// This method is used internally by the hub for message transmission.
// Example:
//   serialized := msg.Serialize()
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

// Deserialize parses a byte slice into the Message structure
// Returns an error if the message format is invalid or checksum verification fails
// This method is used internally by the hub for message reception.
// Example:
//   err := msg.Deserialize(serialized)
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

// generateChecksum calculates a checksum for the message data
// The checksum is calculated by XORing all bytes in the message except for the checksum byte and end bytes
func (m *Message) generateChecksum(data []byte) byte {
	var checksum byte
	for i := 0; i < len(data)-3; i++ { // -3 to exclude checksum and end bytes
		checksum ^= data[i]
	}
	return checksum
}
