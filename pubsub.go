package pubsub

import "context"

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
	Subscribe(ctx context.Context, channelID string) (Subscription[M], error)

	// Returns the number of active subscribers on the channel
	NumSubscribers(channelID string) (int, error)
}

type Subscription[M any] interface {
	ChannelID() string
	SendMessage(ctx context.Context, message M) error
	ReadMessage() <-chan M
	Done() <-chan struct{}
	Unsubscribe()
}
