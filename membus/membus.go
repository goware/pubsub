package membus

import (
	"context"

	"github.com/goware/pubsub"
)

type MemBus[M any] struct {
	bus map[string][]*subscriber[M]
}

var _ pubsub.PubSub[any] = &MemBus[any]{}

func NewMemBus[M any]() (*MemBus[M], error) {
	return nil, nil
}

func (m *MemBus[M]) Run(ctx context.Context) error {
	return nil
}

func (m *MemBus[M]) Stop(ctx context.Context) error {
	return nil
}

func (m *MemBus[M]) Publish(ctx context.Context, channelID string, message M) error {
	// for all subscribers of the channel... send the message...

	// hm........ so Publish should be from a different goroutine
	// .. and the caller of <-sub.ReadMessage() should also be a diff goroutine

	// .. maybe check webrpc stream API how we do this.. cuz, maybe we can use for { .. etc.. }
	// and might be a bit simpler API ...? and then we spin up a new goroutine in the Subscribe method

	return nil
}

func (m *MemBus[M]) Subscribe(ctx context.Context, channelID string) (pubsub.Subscription[M], error) {
	// I think we should spin up a separate go-routine here..
	// or not...? kinda depends on the api to consume it..
	sub := &subscriber[Message]{}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.Done():
				return
			default:
			}

		}
	}()

	return nil, nil
}

func (m *MemBus[M]) NumSubscribers(channelID string) (int, error) {
	subscribers, ok := m.bus[channelID]
	if !ok {
		return 0, nil
	}
	return len(subscribers), nil
}
