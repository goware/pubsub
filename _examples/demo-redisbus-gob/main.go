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

	bus, err := redisbus.New[Event](redisPool, GobMessageEncoder[Event]{}) // TODO: options, like buffer size..?
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

		for {
			err := bus.Publish(context.Background(), "peter", Message{Body: fmt.Sprintf("hello peter %d", n)})
			if err != nil {
				log.Fatal(err)
			}

			err = bus.Publish(context.Background(), "julia", Message{Body: fmt.Sprintf("hello julia %d", n)})
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
