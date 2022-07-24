package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/goware/pubsub/redisbus"
)

func main() {
	redisPool, err := NewRedisPool("localhost")
	if err != nil {
		log.Fatal(err)
	}

	bus, err := redisbus.New[Event](redisPool, MessageEncoder[Event]{}) // TODO: options, like buffer size..?
	if err != nil {
		log.Fatal(err)
	}

	go bus.Run(context.Background()) // TODO .. review, etc.

	time.Sleep(1500 * time.Millisecond)

	sub1 := bus.Subscribe(context.Background(), "peter")
	sub2 := bus.Subscribe(context.Background(), "julia")
	sub3 := bus.Subscribe(context.Background(), "julia") // sub3 is also listening on "julia" channel

	go func() {
		n := 0

		err := bus.Publish(context.Background(), "peter", ConnectMessage{User: "peter"})
		if err != nil {
			log.Fatal(err)
		}
		err = bus.Publish(context.Background(), "julia", ConnectMessage{User: "julia"})
		if err != nil {
			log.Fatal(err)
		}

		for {
			err = bus.Publish(context.Background(), "peter", ChatMessage{User: "peter", Text: fmt.Sprintf("hello from peter %d", n)})
			if err != nil {
				log.Fatal(err)
			}

			err = bus.Publish(context.Background(), "julia", ChatMessage{User: "julia", Text: fmt.Sprintf("hello from julia %d", n)})
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

		case <-sub3.Done():
			break loop

		case msg := <-sub1.ReadMessage():
			switch m := msg.(type) {
			case ConnectMessage:
				fmt.Println("connect!", m.User)
			case ChatMessage:
				fmt.Println("chat text:", m.Text)
			default:
				// this should not happen, unless the type changes between server versions
				fmt.Println("unknown type, dropping the message")
			}

		case msg := <-sub2.ReadMessage():
			switch m := msg.(type) {
			case ConnectMessage:
				fmt.Println("connect!", m.User)
			case ChatMessage:
				fmt.Println("chat text:", m.Text)
			default:
				fmt.Println("unknown type, dropping the message")
			}

		case msg := <-sub3.ReadMessage():
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

	sub1.Unsubscribe()
	sub2.Unsubscribe()
}

func NewRedisPool(host string) (*redis.Pool, error) {
	dialFn := func() (redis.Conn, error) {
		addr := fmt.Sprintf("%s:%d", host, 6379)

		conn, err := redis.Dial("tcp", addr, redis.DialDatabase(0))
		if err != nil {
			return nil, fmt.Errorf("could not dial redis host: %w", err)
		}

		return conn, nil
	}

	pool := &redis.Pool{
		Dial: dialFn,
		TestOnBorrow: func(conn redis.Conn, t time.Time) error {
			_, err := conn.Do("PING")
			return fmt.Errorf("PING failed: %w", err)
		},
	}

	conn := pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	return pool, nil
}
