package client

import "errors"

type ClientChannel chan []byte

func (c ClientChannel) Send(data []byte) (bool, error) {
	select {
	case c <- data:
		return false, nil
	default:
		// Channel is full, drop the oldest and try again
		<-c // Discard oldest
		select {
		case c <- data:
			// Message sent after dropping oldest
			return true, nil
		default:
			//This should rarely, if ever, happen.
			//Handle error/log message.
			return true, errors.New("channel still full after dropping oldest.")
		}
	}
}
