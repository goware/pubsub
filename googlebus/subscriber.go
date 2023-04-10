package googlebus

import (
	"context"
	"sync"

	"github.com/goware/pubsub"
)

var _ pubsub.Subscription[*Message] = &subscriber{}

type subscriber struct {
	pubsub          pubsub.PubSub[*Message]
	channelID       string
	ch              chan *Message
	done            chan struct{}
	err             error
	unsubscribe     func()
	unsubscribeOnce sync.Once
}

// ChannelID is the topic ID for the subscriber.
func (s *subscriber) ChannelID() string {
	return s.channelID
}

func (s *subscriber) SendMessage(ctx context.Context, message *Message) error {
	return s.pubsub.Publish(ctx, s.channelID, message)
}

func (s *subscriber) ReadMessage() <-chan *Message {
	return s.ch
}

func (s *subscriber) Done() <-chan struct{} {
	return s.done
}

func (s *subscriber) Err() error {
	return s.err
}

func (s *subscriber) Unsubscribe() {
	s.unsubscribeOnce.Do(s.unsubscribe)
}
