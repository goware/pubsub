package membus

import (
	"context"
	"sync"

	"github.com/goware/pubsub"
)

type MemBus[M any] struct {
	subscribers map[string][]*subscriber[M]

	mu sync.Mutex
}

var _ pubsub.PubSub[any] = &MemBus[any]{}

func NewMemBus[M any]() (*MemBus[M], error) {
	bus := &MemBus[M]{
		subscribers: make(map[string][]*subscriber[M], 0),
	}
	return bus, nil
}

func (m *MemBus[M]) Run(ctx context.Context) error {
	// allow pub messages or subscribers or not..?
	// this is more interesting with redis..? but should be okay
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

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.subscribers[channelID]
	if !ok {
		// no subscribers on this channel, which is okay
		return nil
	}

	for _, sub := range m.subscribers[channelID] {
		select {
		case <-sub.done:
		case sub.sendCh <- message:
		}
	}

	return nil
}

func (m *MemBus[M]) Subscribe(ctx context.Context, channelID string) pubsub.Subscription[M] {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.subscribers[channelID]
	if !ok {
		m.subscribers[channelID] = []*subscriber[M]{}
	}

	ch := make(chan M)
	subscriber := &subscriber[M]{
		ch:     ch,
		sendCh: pubsub.MakeUnboundedBuffered[M](ch, 100),
		done:   make(chan struct{}),
	}

	subscriber.unsubscribe = func() {
		close(subscriber.done)
		m.mu.Lock()
		defer m.mu.Unlock()
		close(subscriber.sendCh)

		// flush subscriber.ch so that the makeUnboundedBuffered goroutine exits
		for ok := true; ok; _, ok = <-subscriber.ch {
		}

		for i, sub := range m.subscribers[channelID] {
			if sub == subscriber {
				m.subscribers[channelID] = append(m.subscribers[channelID][:i], m.subscribers[channelID][i+1:]...)
				if len(m.subscribers[channelID]) == 0 {
					delete(m.subscribers, channelID)
				}
				return
			}
		}
	}

	m.subscribers[channelID] = append(m.subscribers[channelID], subscriber)

	return subscriber
}

/*func (m *MemBus[M]) Subscribe(ctx context.Context, channelID string) (pubsub.Subscription[M], error) {
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
}*/

func (m *MemBus[M]) NumSubscribers(channelID string) (int, error) {
	subscribers, ok := m.subscribers[channelID]
	if !ok {
		return 0, nil
	}
	return len(subscribers), nil
}
