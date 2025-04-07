package thalamini

import (
	"fmt"
	"log"
	"net"

	"github.com/markoxley/dani/msg"
)

type Consumer interface {
	Consume(*msg.Message)
	GetHub() (string, uint16)
}

func Consume(name string, addr string, port uint16, c Consumer, topics ...string) {
	go func() {
		reg := msg.NewRegistrationMessage(port, name, topics...)
		ip, pt := c.GetHub()
		l, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", addr, port))
		if err != nil {
			log.Printf("Failed to listen on %s:%d: %v", addr, port, err)
			return
		}
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			handler(c, conn)
		}

	}()

}

func handler(c Consumer, conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}
	m := &msg.Message{}
	err = m.Deserialize(buf[:n])
	if err != nil {
		log.Printf("Failed to deserialize message: %v", err)
		return
	}
	c.Consume(m)
}
