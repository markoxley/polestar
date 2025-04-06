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
type MessageData map[string]interface{}

// Message represents a communication packet in the hub system
type Message struct {
	MsgType     byte     // Type of the message (registration or data)
	Source      string   // Source identifier of the message
	Destination []string // List of destination identifiers
	data        []byte   // Raw message payload data
}

// NewMessage creates a new data message with the specified source
func NewMessage(source string) *Message {
	return &Message{
		Source:  source,
		MsgType: DataMsgByte,
	}
}

// NewRegistrationMessage creates a new registration message for a service
// with the specified source and port number
func NewRegistrationMessage(source string, port uint16) *Message {
	return &Message{
		Source:      source,
		Destination: []string{"Hub"},
		MsgType:     RegMsgByte,
		data:        []byte{byte(port), byte(port >> 8)},
	}
}

// SetData sets the message data from a MessageData map
// Returns an error if any of the data types are not supported
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

// Serialize converts the Message into a byte slice according to the protocol
// The format is: [start bytes (2)] [length (2)] [source] [end part] [destinations] [end part] [type (1)] [data] [end part] [checksum (1)] [end bytes (2)]
func (m *Message) Serialize() []byte {
	// Calculate size of message parts
	sourceLen := len(m.Source)
	destLen := 0
	for _, d := range m.Destination {
		if destLen > 0 {
			destLen++ // For comma
		}
		destLen += len(d)
	}
	dataLen := len(m.data)

	// Total size = start(2) + length(2) + source + endPart(1) + destinations + endPart(1) + type(1) + data + endPart(1) + checksum(1) + end(2)
	totalLen := 2 + 2 + sourceLen + 1 + destLen + 1 + 1 + dataLen + 1 + 1 + 2

	// Create buffer with capacity
	buffer := make([]byte, 0, totalLen)

	// Start bytes
	buffer = append(buffer, startBytes...)

	// Length (2 bytes, little endian)
	buffer = append(buffer, byte(totalLen))
	buffer = append(buffer, byte(totalLen>>8))

	// Source
	buffer = append(buffer, []byte(m.Source)...)
	buffer = append(buffer, endPartByte)

	// Destinations
	buffer = append(buffer, []byte(strings.Join(m.Destination, ","))...)
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
func (m *Message) Deserialize(data []byte) error {
	l := len(data)
	if l < 11 {
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

	if len(parts) != 3 {
		return errors.New("invalid message structure")
	}

	m.Source = string(parts[0])
	m.Destination = strings.Split(string(parts[1]), ",")
	if len(parts[2]) < 1 {
		return errors.New("invalid message structure")
	}
	m.MsgType = parts[2][0]
	m.data = parts[2][1:]

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
