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
	Subscribe(ctx context.Context, channelID string) Subscription[M]

	// Returns the number of active subscribers on the channel
	NumSubscribers(channelID string) (int, error)
}

type Subscription[M any] interface {
	ChannelID() string
	ReadMessage() <-chan M
	Done() <-chan struct{}
	Unsubscribe()
}

// TODO: lets think about the Pipe interface ... for reading and writing..
