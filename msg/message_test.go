package msg

import (
	"bytes"
	"reflect"
	"testing"
)

func TestNewMessage(t *testing.T) {
	source := "TestService"
	msg := NewMessage(source)

	if msg.Source != source {
		t.Errorf("Expected source %q, got %q", source, msg.Source)
	}
	if msg.MsgType != DataMsgByte {
		t.Errorf("Expected message type %d, got %d", DataMsgByte, msg.MsgType)
	}
	if len(msg.Destination) != 0 {
		t.Errorf("Expected empty destination, got %v", msg.Destination)
	}
}

func TestNewRegistrationMessage(t *testing.T) {
	source := "TestService"
	port := uint16(8080)
	msg := NewRegistrationMessage(source, port)

	if msg.Source != source {
		t.Errorf("Expected source %q, got %q", source, msg.Source)
	}
	if msg.MsgType != RegMsgByte {
		t.Errorf("Expected message type %d, got %d", RegMsgByte, msg.MsgType)
	}
	if !reflect.DeepEqual(msg.Destination, []string{"Hub"}) {
		t.Errorf("Expected destination [Hub], got %v", msg.Destination)
	}
	expectedData := []byte{byte(port), byte(port >> 8)}
	if !reflect.DeepEqual(msg.data, expectedData) {
		t.Errorf("Expected data %v, got %v", expectedData, msg.data)
	}
}

func TestSetData(t *testing.T) {
	tests := []struct {
		name    string
		data    MessageData
		wantErr bool
	}{
		{
			name: "basic types",
			data: MessageData{
				"string": "test",
				"int":    int64(42),
				"float":  float64(3.14),
				"bool":   true,
			},
			wantErr: false,
		},
		{
			name: "unsupported type",
			data: MessageData{
				"complex": complex(1, 2),
			},
			wantErr: true,
		},
		{
			name:    "empty data",
			data:    MessageData{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMessage("test")
			err := msg.SetData(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Verify we can read back the data
			got, err := msg.Data()
			if err != nil {
				t.Errorf("Data() unexpected error = %v", err)
				return
			}

			if len(got) != len(tt.data) {
				t.Errorf("Data() returned %d items, want %d", len(got), len(tt.data))
				return
			}

			// Compare values accounting for type conversions
			for k, want := range tt.data {
				gotVal, exists := got[k]
				if !exists {
					t.Errorf("Data() missing key %q", k)
					continue
				}

				switch want := want.(type) {
				case float64:
					gotFloat, ok := gotVal.(float64)
					if !ok {
						t.Errorf("Data()[%q] = %T, want float64", k, gotVal)
						continue
					}
					if gotFloat != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotFloat, want)
					}
				case int64:
					gotInt, ok := gotVal.(int64)
					if !ok {
						t.Errorf("Data()[%q] = %T, want int64", k, gotVal)
						continue
					}
					if gotInt != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotInt, want)
					}
				case string:
					gotStr, ok := gotVal.(string)
					if !ok {
						t.Errorf("Data()[%q] = %T, want string", k, gotVal)
						continue
					}
					if gotStr != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotStr, want)
					}
				case bool:
					gotBool, ok := gotVal.(bool)
					if !ok {
						t.Errorf("Data()[%q] = %T, want bool", k, gotVal)
						continue
					}
					if gotBool != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotBool, want)
					}
				}
			}
		})
	}
}

func TestSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		destination []string
		msgType     byte
		data        MessageData
	}{
		{
			name:        "simple message",
			source:      "TestService",
			destination: []string{"Dest1", "Dest2"},
			msgType:     DataMsgByte,
			data: MessageData{
				"key1": "value1",
				"key2": int64(42),
			},
		},
		{
			name:        "empty data",
			source:      "TestService",
			destination: []string{"Dest1"},
			msgType:     DataMsgByte,
			data:        MessageData{},
		},
		{
			name:        "multiple data types",
			source:      "TestService",
			destination: []string{"Dest"},
			msgType:     DataMsgByte,
			data: MessageData{
				"string": "test",
				"int":    int64(42),
				"float":  float64(3.14),
				"bool":   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and set up original message
			msg1 := &Message{
				Source:      tt.source,
				Destination: tt.destination,
				MsgType:     tt.msgType,
			}
			err := msg1.SetData(tt.data)
			if err != nil {
				t.Fatalf("SetData() error = %v", err)
			}

			// Serialize
			serialized := msg1.Serialize()

			// Verify start and end bytes
			if !bytes.Equal(serialized[0:2], startBytes) {
				t.Errorf("Invalid start bytes: got %v, want %v", serialized[0:2], startBytes)
			}
			if !bytes.Equal(serialized[len(serialized)-2:], endBytes) {
				t.Errorf("Invalid end bytes: got %v, want %v", serialized[len(serialized)-2:], endBytes)
			}

			// Verify length field
			length := int(serialized[2]) + int(serialized[3])<<8
			if length != len(serialized) {
				t.Errorf("Invalid length field: got %d, want %d", length, len(serialized))
			}

			// Deserialize into new message
			msg2 := &Message{}
			err = msg2.Deserialize(serialized)
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}

			// Compare fields
			if msg2.Source != tt.source {
				t.Errorf("Source = %q, want %q", msg2.Source, tt.source)
			}
			if !reflect.DeepEqual(msg2.Destination, tt.destination) {
				t.Errorf("Destination = %v, want %v", msg2.Destination, tt.destination)
			}
			if msg2.MsgType != tt.msgType {
				t.Errorf("MsgType = %d, want %d", msg2.MsgType, tt.msgType)
			}

			// Compare data
			data2, err := msg2.Data()
			if err != nil {
				t.Fatalf("Data() error = %v", err)
			}

			if len(data2) != len(tt.data) {
				t.Errorf("Data() returned %d items, want %d", len(data2), len(tt.data))
				return
			}

			// Compare values accounting for type conversions
			for k, want := range tt.data {
				gotVal, exists := data2[k]
				if !exists {
					t.Errorf("Data() missing key %q", k)
					continue
				}

				switch want := want.(type) {
				case float64:
					gotFloat, ok := gotVal.(float64)
					if !ok {
						t.Errorf("Data()[%q] = %T, want float64", k, gotVal)
						continue
					}
					if gotFloat != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotFloat, want)
					}
				case int64:
					gotInt, ok := gotVal.(int64)
					if !ok {
						t.Errorf("Data()[%q] = %T, want int64", k, gotVal)
						continue
					}
					if gotInt != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotInt, want)
					}
				case string:
					gotStr, ok := gotVal.(string)
					if !ok {
						t.Errorf("Data()[%q] = %T, want string", k, gotVal)
						continue
					}
					if gotStr != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotStr, want)
					}
				case bool:
					gotBool, ok := gotVal.(bool)
					if !ok {
						t.Errorf("Data()[%q] = %T, want bool", k, gotVal)
						continue
					}
					if gotBool != want {
						t.Errorf("Data()[%q] = %v, want %v", k, gotBool, want)
					}
				}
			}
		})
	}
}

func TestDeserializeErrors(t *testing.T) {
	validMsg := &Message{
		Source:      "test",
		Destination: []string{"dest"},
		MsgType:     DataMsgByte,
	}
	validBytes := validMsg.Serialize()

	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "too short",
			data:    []byte{0x00, 0x01},
			wantErr: "invalid message length",
		},
		{
			name:    "invalid start bytes",
			data:    append([]byte{0x00, 0x00}, validBytes[2:]...),
			wantErr: "invalid message start bytes",
		},
		{
			name:    "invalid end bytes",
			data:    append(validBytes[:len(validBytes)-2], []byte{0x00, 0x00}...),
			wantErr: "invalid message end bytes",
		},
		{
			name:    "invalid length field",
			data:    append(append(append(startBytes, []byte{0xFF, 0xFF}...), validBytes[4:len(validBytes)-3]...), endBytes...),
			wantErr: "invalid message length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{}
			err := msg.Deserialize(tt.data)
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
