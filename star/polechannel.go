package star

import (
	"errors"

	"github.com/markoxley/polestar/msg"
)

type PoleChannel chan *msg.Message

// Send attempts to send a message to the channel.
// If the channel is full,
// it drops the oldest message and tries again.
// Returns a boolean indicating
// whether a message was dropped (true) and an error if the operation failed.
// The error is non-nil only if the channel remains full after attempting to
// drop the oldest message.
func (c PoleChannel) Send(m *msg.Message) (bool, error) {
	select {
	case c <- m:
		return false, nil
	default:
		// Channel is full, drop the oldest and try again
		<-c // Discard oldest
		select {
		case c <- m:
			// Message sent after dropping oldest
			return true, nil
		default:
			//This should rarely, if ever, happen.
			//Handle error/log message.
			return true, errors.New("channel still full after dropping oldest.")
		}
	}
}
