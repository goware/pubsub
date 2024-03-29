package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/goware/logger"
	"github.com/goware/pubsub/redisbus"
	"github.com/redis/go-redis/v9"
)

var (
	flags       = flag.NewFlagSet("demo-redisbus-chat", flag.ExitOnError)
	fServerName = flags.String("server", "", "Server name")
)

func main() {
	flags.Parse(os.Args[1:])

	if fServerName == nil || *fServerName == "" {
		flags.Usage()
		os.Exit(1)
		return
	}
	serverName := *fServerName

	// Setup redis
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Setup pubsub
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

	connectedServers := map[string]bool{}

	// Channel "connect" is where we listen for new connections, and we also announce
	// ourselves when we boot up.
	connectSub, _ := bus.Subscribe(context.Background(), "connect")
	bus.Publish(context.Background(), "connect", ConnectMessage{From: serverName})

	roomSub, _ := bus.Subscribe(context.Background(), "#funroom")

	var wg sync.WaitGroup

	// Handling incoming messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {

			case <-connectSub.Done():
				return

			case msg := <-connectSub.ReadMessage():
				connectMsg, ok := msg.(ConnectMessage)
				if !ok {
					continue
				}
				if connectMsg.From == serverName {
					// skip message from ourselves
					continue
				}
				if connectedServers[connectMsg.From] {
					// already connected
					continue
				}

				fmt.Printf("[%s] connect %s\n", serverName, connectMsg.From)
				connectedServers[connectMsg.From] = true
				bus.Publish(context.Background(), "connect", ConnectMessage{From: serverName})

			case msg := <-roomSub.ReadMessage():
				chatMsg, ok := msg.(ChatMessage)
				if !ok {
					continue
				}
				if chatMsg.From == serverName {
					// skip message from ourselves
					continue
				}
				fmt.Printf("[%s] chat incoming message -- %s\n", serverName, chatMsg.Text)
			}
		}
	}()

	// Faux chat... sending some pretend messages after second
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for {
			select {
			case <-connectSub.Done():
				return
			case <-time.After(1 * time.Second):
				n += 1
				text := fmt.Sprintf("hi from %s, %d", serverName, n)
				bus.Publish(context.Background(), "#funroom", ChatMessage{From: serverName, Text: text})
			}
		}
	}()

	wg.Wait()
}
