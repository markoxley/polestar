package main

import (
	"fmt"
	"log"
	"time"

	"github.com/markoxley/dani/msg"
	"github.com/markoxley/dani/thal"
)

type consumerSample struct {
	quit bool
	ch   chan *msg.Message
}

func (c *consumerSample) Consume(m *msg.Message) {
	c.ch <- m
}

func (c *consumerSample) Run() {
	for m := range c.ch {
		d, err := m.Data()
		if err != nil {
			log.Printf("error: %s", err)
		}
		fmt.Println(d)
		_, ok := d["quit"]
		if !ok {
			continue
		}
		c.quit = true
	}
}

func (c *consumerSample) GetHub() (string, uint16) {
	return "127.0.0.1", 24353
}

func main() {
	c := &consumerSample{
		ch: make(chan *msg.Message, 100),
	}

	c.quit = false
	if err := thal.Listen("sample", "127.0.0.1", 80, c, "test"); err != nil {
		log.Panic(err)
	}
	fmt.Println("listening on 127.0.0.1:80")
	go c.Run()
	for !c.quit {
		time.Sleep(100 * time.Millisecond)
	}
}
