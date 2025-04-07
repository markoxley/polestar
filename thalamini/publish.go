package thalamini

import "github.com/markoxley/dani/msg"

func Publish(topic string, data map[string]interface{}) error {

	m := msg.NewMessage(topic)
	if err := m.SetData(data); err != nil {
		return err
	}
	// Need to send to hub
	return nil
}
