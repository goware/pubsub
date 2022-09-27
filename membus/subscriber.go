package membus

import (
	"context"
	"sync"

	"github.com/goware/pubsub"
)

var _ pubsub.Subscription[any] = &subscriber[any]{}

type subscriber[M any] struct {
	pubsub          pubsub.PubSub[M]
	channelID       string
	ch              <-chan M
	sendCh          chan<- M
	done            chan struct{}
	unsubscribe     func()
	unsubscribeOnce sync.Once
}

func (s *subscriber[M]) ChannelID() string {
	return s.channelID
}

func (s *subscriber[M]) SendMessage(ctx context.Context, message M) error {
	return s.pubsub.Publish(ctx, s.channelID, message)
}

func (s *subscriber[M]) ReadMessage() <-chan M {
	return s.ch
}

func (s *subscriber[M]) Done() <-chan struct{} {
	return s.done
}

func (s *subscriber[M]) Unsubscribe() {
	s.unsubscribeOnce.Do(s.unsubscribe)
}
