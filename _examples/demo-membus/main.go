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

	sub1, err := bus.Subscribe(context.Background(), "peter")
	if err != nil {
		log.Fatal(err)
	}
	defer sub1.Unsubscribe()

	sub2, err := bus.Subscribe(context.Background(), "julia")
	if err != nil {
		log.Fatal(err)
	}
	defer sub2.Unsubscribe()

	go func() {
		err := bus.Publish("peter", membus.Message{Body: "hello peter 1"})
		if err != nil {
			log.Fatal(err)
		}

		err := bus.Publish("peter", membus.Message{Body: "hello peter 2"})
		if err != nil {
			log.Fatal(err)
		}

		err := bus.Publish("julia", membus.Message{Body: "hello julia 1"})
		if err != nil {
			log.Fatal(err)
		}
	}()

	for {
		select {

		case <-sub1.Done():
			break

		case <-sub2.Done():
			break

		case msg <- sub1.ReadMessage():
			fmt.Println("sub1 message:", msg, "channelid", sub1.ChannelID())

		case msg <- sub2.ReadMessage():
			fmt.Println("sub2 message:", msg, "channelid", sub2.ChannelID())

		case <-time.After(2 * time.Second):
			break

		}
	}

	sub1.Unsubscribe()
	sub2.Unsubscribe()
}
