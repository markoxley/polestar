package msg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
)

var (
	startBytes      = []byte{0xFE, 0xFF}
	endBytes        = []byte{0x05, 0xFD}
	regMsgByte      = byte(0x00)
	dataMsgByte     = byte(0xff)
	endPartByte     = byte(0x02)
	endDataPartByte = byte(0x03)
	splitDataByte   = byte(0x04)

	dtString = byte(0x01)
	dtInt    = byte(0x02)
	dtFloat  = byte(0x03)
	dtBool   = byte(0x04)
)

type MessageData map[string]interface{}

type Message struct {
	MsgType     byte
	Source      string
	Destination []string
	data        []byte
}

func NewMessage(source string) *Message {
	return &Message{
		Source:  source,
		MsgType: dataMsgByte,
	}
}

func NewRegistrationMessage(source string, port uint16) *Message {
	return &Message{
		Source:      source,
		Destination: []string{"Hub"},
		MsgType:     regMsgByte,
		data:        []byte{byte(port), byte(port >> 8)},
	}
}

func (m *Message) SetData(data MessageData) error {
	m.data = []byte{}
	for k, v := range data {
		m.data = append(m.data, []byte(k)...)
		m.data = append(m.data, splitDataByte)
		switch v.(type) {
		case string:
			m.data = append(m.data, dtString)
			m.data = append(m.data, []byte(v.(string))...)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			val := reflect.ValueOf(v).Int() // Use reflection for simplicity, or handle each type explicitly
			m.data = append(m.data, dtInt)
			buf := new(bytes.Buffer)
			err := binary.Write(buf, binary.BigEndian, val) // Choose byte order
			if err != nil {
				return err // Handle error
			}
			m.data = append(m.data, buf.Bytes()...)
		case float32, float64:
			val := reflect.ValueOf(v).Float()
			m.data = append(m.data, dtFloat)
			bits := math.Float64bits(val)
			buf := new(bytes.Buffer)
			err := binary.Write(buf, binary.BigEndian, bits)
			if err != nil {
				return err // Handle error
			}
			m.data = append(m.data, buf.Bytes()...)
		case bool:
			m.data = append(m.data, dtBool)
			if v.(bool) {
				m.data = append(m.data, 1)
			} else {
				m.data = append(m.data, 0)
			}
		default:
			return errors.New("unsupported data type")
		}
		m.data = append(m.data, endDataPartByte)
	}
	return nil
}

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
			s[1] = s[1][1:]
			switch tp {
			case dtString:
				md[string(s[0])] = s[1]
			case dtInt:
				if len(s[1]) < 8 { // Assuming int64
					return nil, errors.New("invalid integer data length")
				}
				buf := bytes.NewReader([]byte(s[1]))
				var val int64
				err := binary.Read(buf, binary.BigEndian, &val)
				if err != nil {
					return nil, err
				}
				md[string(s[0])] = val
			case dtFloat:
				v, err := strconv.ParseFloat(string(s[1]), 64)
				if err != nil {
					return nil, err
				}
				md[string(s[0])] = v
			case dtBool:
				v, err := strconv.ParseBool(string(s[1]))
				if err != nil {
					return nil, err
				}
				md[string(s[0])] = v
			default:
				return nil, errors.New("unsupported data type")
			}
			buffer = buffer[:0]
		}
		buffer = append(buffer, b)
	}
	return md, nil
}

func (m *Message) Serialize() []byte {
	d := strings.Join(m.Destination, ",")
	l := len(m.Source) + len(d) + len(m.data) + 11

	msg := make([]byte, 0, l)
	msg = append(msg, startBytes...)
	msg = append(msg, byte(l), byte(l>>8))
	msg = append(msg, []byte(m.Source)...)
	msg = append(msg, endPartByte)
	msg = append(msg, []byte(d)...)
	msg = append(msg, endPartByte)
	msg = append(msg, m.MsgType)
	msg = append(msg, m.data...)
	msg = append(msg, endPartByte)
	msg = append(msg, 0)
	msg = append(msg, endBytes...)
	msg[len(msg)-3] = m.generateChecksum(msg)
	return msg
}

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
	if data[l-3] != m.generateChecksum(data) {
		return errors.New("invalid message checksum")
	}
	length := int(data[2]) + int(data[3])<<8
	if length != l {
		return errors.New("invalid message length")
	}
	st := 0
	buffer := make([]byte, 0, l)
	for idx := 4; idx < l-3; idx++ {
		if data[idx] == endPartByte {
			switch st {
			case 0:
				m.Source = string(buffer)
			case 1:
				m.Destination = strings.Split(string(buffer), ",")
			case 2:
				if len(buffer) < 1 {
					return errors.New("invalid message structure")
				}
				m.MsgType = buffer[0]
				m.data = buffer[1:]
			default:
				return errors.New("invalid message structure")
			}
			buffer = buffer[:0]
			st++
		}
		buffer = append(buffer, data[idx])
	}
	return nil
}

func (m *Message) generateChecksum(data []byte) byte {
	var res byte
	for i, b := range data {
		if i == len(data)-3 {
			continue
		}
		res ^= b
	}
	return res
}
