package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/goware/pubsub/logger"
	"github.com/goware/pubsub/redisbus"
)

type Message struct {
	Body string `json:"body"`
}

func main() {
	redisPool, err := NewRedisPool("localhost")
	if err != nil {
		log.Fatal(err)
	}

	bus, err := redisbus.New[Message](logger.NewLogger(logger.LogLevel_DEBUG), redisPool)
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

	time.Sleep(1 * time.Second)

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
				return
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

	// Subscribers will be automatically unsubscribed at bus.Stop call at defer
	// sub1.Unsubscribe()
	// sub2.Unsubscribe()
	// sub3.Unsubscribe()
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
