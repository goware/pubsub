package main

import (
	"context"
	"fmt"
	"log"
	"time"

	gpubsub "cloud.google.com/go/pubsub"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
	"github.com/goware/pubsub/googlebus"
	"github.com/goware/pubsub/membus"
)

type Message = gpubsub.Message

func main() {
	logg := logger.NewLogger(logger.LogLevel_DEBUG)

	mbus, err := membus.New[*gpubsub.Message](logg)
	if err != nil {
		log.Fatalf("new membus: %v", err)
	}
	_ = mbus

	gbus, err := googlebus.New(logg, "horizon-games-data", []string{"topic1"})
	if err != nil {
		log.Fatalf("new googlebus: %v", err)
	}
	_ = gbus

	bus := gbus

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			log.Fatalf("run bus: %v", err)
		}
	}()
	defer bus.Stop()

	time.Sleep(1 * time.Second) // wait for run to start

	sub1, err := bus.Subscribe(context.Background(), "topic1", "sub1")
	if err != nil {
		log.Fatalf("subscribe 1: %v", err)
	}

	sub2, _ := bus.Subscribe(context.Background(), "topic1", "sub2")
	if err != nil {
		log.Fatalf("subscribe 2: %v", err)
	}

	go func() {
		n := 0

		for {
			bus.Publish(context.Background(), "topic1", &Message{Data: []byte(fmt.Sprintf("hello peter %d", n))})

			n += 1
			if n == 10 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	batchSub2 := pubsub.BatchMessageReader(logg, sub2, 4, 2*time.Second)

loop:
	for {
		select {

		case <-sub1.Done():
			break loop

		case <-sub2.Done():
			break loop

		case msg := <-sub1.ReadMessage():
			fmt.Println("sub1 message:", string(msg.Data), "channelid", sub1.ChannelID())
			msg.Ack()

		case msgs := <-batchSub2:
			fmt.Println("got batch of messages:", len(msgs))
			for _, msg := range msgs {
				fmt.Println("sub2 message:", string(msg.Data), "channelid", sub2.ChannelID())
				msg.Ack()
			}

		case <-time.After(10 * time.Second):
			break loop

		}
	}

	sub1.Unsubscribe()
	sub2.Unsubscribe()

	// simulate run of application, to prevent instant halt
	time.Sleep(time.Second * 3)
}
