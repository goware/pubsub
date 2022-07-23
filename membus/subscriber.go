package membus

import "github.com/goware/pubsub"

var _ pubsub.Subscription[any] = &subscriber[any]{}

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
