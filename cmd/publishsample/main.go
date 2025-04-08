package main

import (
	"fmt"
	"time"

	"github.com/markoxley/dani/msg"
	"github.com/markoxley/dani/thal"
)

func main() {
	thal.Init("127.0.0.1", 24353)
	for i := range 10000 {
		t := time.Now().UnixNano()
		d := msg.MessageData{"data": fmt.Sprintf("message-%d", i), "time": t}
		fmt.Printf("Sending: %v\n", d)
		thal.Publish("test", d)
		time.Sleep(time.Millisecond * 10)
	}
	thal.Publish("test", msg.MessageData{"data": "quit"})
	time.Sleep(time.Second * 5)

}
