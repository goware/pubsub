package pubsub

import (
	"context"
	"fmt"
	"time"

	"github.com/goware/logger"
)

// PubSub represents a messaging system with producers and consumers.
type PubSub[M any] interface {
	// Run the pubsub subsystem
	Run(ctx context.Context) error

	// Stop the pubsub subsystem
	Stop()

	// IsRunning returns running state ot the pubsub subsystem
	IsRunning() bool

	// Publish sends a message to the channel
	Publish(ctx context.Context, channelID string, message M) error

	// Subscribe listens for messages on the channel
	Subscribe(ctx context.Context, channelID string, optSubcriptionID ...string) (Subscription[M], error)

	// Returns the number of active subscribers on the channel
	NumSubscribers(channelID string) (int, error)
}

type Subscription[M any] interface {
	ChannelID() string
	SendMessage(ctx context.Context, message M) error
	ReadMessage() <-chan M
	Done() <-chan struct{}
	Err() error
	Unsubscribe()
}

func BatchMessageReader[M any](log logger.Logger, sub Subscription[M], maxMessages int, maxWait time.Duration) <-chan []M {
	ch := make(chan []M)

	// maxWait minimum is 1 second
	if maxWait < time.Second {
		maxWait = time.Second
	}

	// batch message reader
	go func() {
		defer close(ch)

		var msgs []M

		for {
			select {

			case <-sub.Done():
				if sub.Err() != nil {
					log.Error(fmt.Sprintf("pubsub: batch message reader error: %v", sub.Err()))
				}
				return

			case msg, ok := <-sub.ReadMessage():
				if !ok {
					return
				}

				msgs = append(msgs, msg)

				if len(msgs) == maxMessages {
					ch <- msgs
					msgs = msgs[:0]
				}

			case <-time.After(maxWait):
				if len(msgs) > 0 {
					ch <- msgs
					msgs = msgs[:0]
				}
			}
		}
	}()

	return ch
}
