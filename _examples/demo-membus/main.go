package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/goware/pubsub/membus"
)

func main() {
	bus, err := membus.NewMemBus[membus.Message]() // TODO: options, like buffer size..?
	if err != nil {
		log.Fatal(err)
	}

	sub1 := bus.Subscribe(context.Background(), "peter")
	sub2 := bus.Subscribe(context.Background(), "julia")
	sub3 := bus.Subscribe(context.Background(), "julia") // sub3 is also listening on "julia" channel

	go func() {
		n := 0

		for {
			err := bus.Publish(context.Background(), "peter", membus.Message{Body: fmt.Sprintf("hello peter %d", n)})
			if err != nil {
				log.Fatal(err)
			}

			err = bus.Publish(context.Background(), "julia", membus.Message{Body: fmt.Sprintf("hello julia %d", n)})
			if err != nil {
				log.Fatal(err)
			}

			n += 1
			if n == 5 {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}()

loop:
	for {
		select {

		case <-sub1.Done():
			break loop

		case <-sub2.Done():
			break loop

		case msg := <-sub1.ReadMessage():
			fmt.Println("sub1 message:", msg, "channelid", sub1.ChannelID())

		case msg := <-sub2.ReadMessage():
			fmt.Println("sub2 message:", msg, "channelid", sub2.ChannelID())

		case msg := <-sub3.ReadMessage():
			fmt.Println("sub3 message:", msg, "channelid", sub3.ChannelID())

		case <-time.After(2 * time.Second):
			break loop

		}
	}

	sub1.Unsubscribe()
	sub2.Unsubscribe()
}
