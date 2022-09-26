package membus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type MemBus[M any] struct {
	log         logger.Logger
	subscribers map[string][]*subscriber[M]

	ctx     context.Context
	ctxStop context.CancelFunc
	running int32
	mu      sync.Mutex
}

var _ pubsub.PubSub[any] = &MemBus[any]{}

func New[M any](log logger.Logger) (*MemBus[M], error) {
	bus := &MemBus[M]{
		log:         log,
		subscribers: make(map[string][]*subscriber[M], 0),
	}
	return bus, nil
}

func (m *MemBus[M]) Run(ctx context.Context) error {
	if m.IsRunning() {
		return fmt.Errorf("membus: already running")
	}

	m.ctx, m.ctxStop = context.WithCancel(ctx)

	atomic.StoreInt32(&m.running, 1)
	defer atomic.StoreInt32(&m.running, 0)

	m.log.Info("membus: run")

	// block and wait until stopped
	<-m.ctx.Done()
	return nil
}

func (m *MemBus[M]) Stop() {
	if !m.IsRunning() {
		return
	}

	for _, subscribers := range m.subscribers {
		for _, sub := range subscribers {
			sub.Unsubscribe()
		}
	}

	m.log.Info("membus: stop")
	m.ctxStop()
}

func (m *MemBus[M]) IsRunning() bool {
	return atomic.LoadInt32(&m.running) == 1
}

func (m *MemBus[M]) Publish(ctx context.Context, channelID string, message M) error {
	if !m.IsRunning() {
		return fmt.Errorf("membus: pubsub is not running")
	}

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

func (m *MemBus[M]) Subscribe(ctx context.Context, channelID string) (pubsub.Subscription[M], error) {
	if !m.IsRunning() {
		return nil, fmt.Errorf("membus: pubsub is not running")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.subscribers[channelID]
	if !ok {
		m.subscribers[channelID] = []*subscriber[M]{}
	}

	ch := make(chan M)
	subscriber := &subscriber[M]{
		pubsub:    m,
		channelID: channelID,
		ch:        ch,
		sendCh:    pubsub.MakeUnboundedBufferedChan(ch, m.log, 100),
		done:      make(chan struct{}),
	}

	subscriber.unsubscribe = func() {
		close(subscriber.done)
		m.mu.Lock()
		defer m.mu.Unlock()
		close(subscriber.sendCh)

		// flush subscriber.ch so that the MakeUnboundedBuffered goroutine exits
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

	return subscriber, nil
}

func (m *MemBus[M]) NumSubscribers(channelID string) (int, error) {
	if !m.IsRunning() {
		return 0, fmt.Errorf("membus: pubsub is not running")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	subscribers, ok := m.subscribers[channelID]
	if !ok {
		return 0, nil
	}
	return len(subscribers), nil
}
