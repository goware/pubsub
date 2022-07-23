package pubsub

import "context"

// PubSub represents a messaging system with producers and consumers.
type PubSub[M any] interface {
	// Run the pubsub subsystem
	Run(ctx context.Context) error

	// Stop the pubsub subsystem
	Stop(ctx context.Context) error

	// Publish sends a message to the channel
	Publish(ctx context.Context, channelID string, message M) error

	// Subscribe listens for messages on the channel
	Subscribe(ctx context.Context, channelID string) (Subscription[M], error)

	// Returns the number of active subscribers on the channel
	NumSubscribers(channelID string) (int, error)
}

type Subscription[M any] interface {
	ChannelID() string
	ReadMessage() <-chan M
	Done() <-chan struct{}
	Unsubscribe()
}

var _ Subscription[Message] = &subscriber[Message]{}

type subscriber[M any] struct {
	channelID   string
	ch          <-chan M
	sendCh      chan<- M
	done        chan struct{}
	unsubscribe func()
}

func (s *subscriber[M]) ChannelID() string {
	return s.channelID
}

func (s *subscriber[M]) ReadMessage() <-chan M {
	return s.ch
}

func (s *subscriber[M]) Done() <-chan struct{} {
	return s.done
}

func (s *subscriber[M]) Unsubscribe() {
	s.unsubscribe()
}
