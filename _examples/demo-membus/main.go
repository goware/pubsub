package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/goware/pubsub/membus"
)

type Message struct {
	Body string
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	bus, err := membus.New[Message](log)
	if err != nil {
		slog.Error("new membus", "error", err)
		return
	}

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			slog.Error("run bus", "error", err)
			return
		}
	}()
	defer bus.Stop()

	time.Sleep(1 * time.Second) // wait for run to start

	sub1, _ := bus.Subscribe(context.Background(), "peter")
	sub2, _ := bus.Subscribe(context.Background(), "julia")
	sub3, _ := bus.Subscribe(context.Background(), "julia") // sub3 is also listening on "julia" channel

	go func() {
		n := 0

		for {
			bus.Publish(context.Background(), "peter", Message{Body: fmt.Sprintf("hello peter %d", n)})
			bus.Publish(context.Background(), "julia", Message{Body: fmt.Sprintf("hello julia %d", n)})

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

		case <-sub3.Done():
			break loop

		case msg := <-sub1.ReadMessage():
			fmt.Println("sub1 message:", msg, "channelid", sub1.ChannelID())

		case msg := <-sub2.ReadMessage():
			fmt.Println("sub2 message:", msg, "channelid", sub2.ChannelID())

		case msg := <-sub3.ReadMessage():
			fmt.Println("sub3 message:", msg, "channelid", sub3.ChannelID())

		case <-time.After(4 * time.Second):
			break loop

		}
	}

	sub1.Unsubscribe()
	sub2.Unsubscribe()
	sub3.Unsubscribe()
}
