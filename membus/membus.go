package membus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/goware/channel"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type MemBus[M any] struct {
	log logger.Logger

	channels   map[string]map[*subscriber[M]]bool
	channelsMu sync.Mutex

	ctx     context.Context
	ctxStop context.CancelFunc

	running int32
}

var _ pubsub.PubSub[any] = &MemBus[any]{}

func New[M any](log logger.Logger) (*MemBus[M], error) {
	bus := &MemBus[M]{
		log:      log,
		channels: map[string]map[*subscriber[M]]bool{},
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

	m.channelsMu.Lock()
	for channelID, subscribers := range m.channels {
		for sub := range subscribers {
			m.cleanUpSubscription(channelID, sub)
		}
	}
	m.channelsMu.Unlock()

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

	m.channelsMu.Lock()
	defer m.channelsMu.Unlock()

	_, ok := m.channels[channelID]
	if !ok {
		// no subscribers on this channel, which is okay
		return nil
	}

	for sub, _ := range m.channels[channelID] {
		sub.ch.Send(message)
	}

	return nil
}

func (m *MemBus[M]) Subscribe(ctx context.Context, channelID string, optSubcriptionID ...string) (pubsub.Subscription[M], error) {
	if !m.IsRunning() {
		return nil, fmt.Errorf("membus: pubsub is not running")
	}

	sub := &subscriber[M]{
		pubsub:    m,
		channelID: channelID,
		ch:        channel.NewUnboundedChan[M](m.log, 100, -1),
		done:      make(chan struct{}),
	}

	sub.unsubscribe = func() {
		select {
		case <-sub.done:
		default:
			close(sub.done)
		}
		sub.ch.Close()
		sub.ch.Flush()

		m.channelsMu.Lock()
		m.cleanUpSubscription(channelID, sub)
		m.channelsMu.Unlock()
	}

	// add channel and subscription
	m.channelsMu.Lock()
	_, ok := m.channels[channelID]
	if !ok {
		m.channels[channelID] = map[*subscriber[M]]bool{}
	}
	m.channels[channelID][sub] = true
	m.channelsMu.Unlock()

	return sub, nil
}

func (m *MemBus[M]) NumSubscribers(channelID string) (int, error) {
	if !m.IsRunning() {
		return 0, fmt.Errorf("membus: pubsub is not running")
	}

	m.channelsMu.Lock()
	defer m.channelsMu.Unlock()

	channels, ok := m.channels[channelID]
	if !ok {
		return 0, nil
	}

	if len(channels) > 0 {
		return 1, nil
	}

	return 0, nil
}

func (m *MemBus[M]) cleanUpSubscription(channelID string, sub *subscriber[M]) {
	if len(m.channels[channelID]) < 1 {
		return
	}
	if !m.channels[channelID][sub] {
		return
	}

	select {
	case <-sub.done:
	default:
		close(sub.done)
	}
	sub.ch.Close()
	sub.ch.Flush()

	// remove sub from list of subscribers
	delete(m.channels[channelID], sub)

	if len(m.channels[channelID]) > 0 {
		return // channel has more subscriptions, exit here
	}

	// channel has no more subscribers
	m.log.Debugf("membus: removing channel %q", channelID)

	// delete channel
	delete(m.channels, channelID)
}
