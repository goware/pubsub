package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goware/logger"
	"github.com/goware/pubsub/redisbus"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	bus, err := redisbus.New[Message](logger.NewLogger(logger.LogLevel_DEBUG), redisClient, MessageEncoder[Message]{})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}()
	defer bus.Stop()

	time.Sleep(1000 * time.Millisecond)

	peterSub, _ := bus.Subscribe(context.Background(), "peter")
	juliaSub, _ := bus.Subscribe(context.Background(), "julia")

	go func() {
		n := 0

		peterSub.SendMessage(context.Background(), ConnectMessage{User: "peter"})
		juliaSub.SendMessage(context.Background(), ConnectMessage{User: "julia"})

		for {
			peterSub.SendMessage(context.Background(), ChatMessage{User: "peter", Text: fmt.Sprintf("hello from peter %d", n)})
			juliaSub.SendMessage(context.Background(), ChatMessage{User: "julia", Text: fmt.Sprintf("hello from julia %d", n)})

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

		case <-peterSub.Done():
			break loop

		case <-juliaSub.Done():
			break loop

		case msg := <-peterSub.ReadMessage():
			switch m := msg.(type) {
			case ConnectMessage:
				fmt.Println("connect!", m.User)
			case ChatMessage:
				fmt.Println("chat text:", m.Text)
			default:
				// this should not happen, unless the type changes between server versions
				fmt.Println("unknown type, dropping the message")
			}

		case msg := <-juliaSub.ReadMessage():
			switch m := msg.(type) {
			case ConnectMessage:
				fmt.Println("connect!", m.User)
			case ChatMessage:
				fmt.Println("chat text:", m.Text)
			default:
				fmt.Println("unknown type, dropping the message")
			}

		case <-time.After(2 * time.Second):
			break loop
		}
	}
}
