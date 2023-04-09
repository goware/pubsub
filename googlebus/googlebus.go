package googlebus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	gpubsub "cloud.google.com/go/pubsub"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type GoogleBus struct {
	log logger.Logger

	// Google Pubsub client, project and topic ids
	client    *gpubsub.Client
	projectID string
	topicIDs  []string
	options   Options

	// topics maps topicID to topic
	topics map[string]*gpubsub.Topic

	// subsByTopic maps topicID to a map of subID to sub
	subs map[string]map[string]*gpubsub.Subscription

	// subscribers list of pubsub.Subscription
	subscribers   map[string]*subscriber
	subscribersMu sync.Mutex

	ctx     context.Context
	ctxStop context.CancelFunc

	running int32
}

type Options struct {
	SubscriptionConfig gpubsub.SubscriptionConfig
	ReceiveSettings    gpubsub.ReceiveSettings
}

var DefaultOptions = Options{
	SubscriptionConfig: gpubsub.SubscriptionConfig{
		EnableExactlyOnceDelivery: true,
		ExpirationPolicy:          nil, // will use defualt of 31 days
	},
	ReceiveSettings: gpubsub.ReceiveSettings{},
}

type Message = gpubsub.Message

var _ pubsub.PubSub[*Message] = &GoogleBus{}

func New(log logger.Logger, projectID string, topicIDs []string, opts ...Options) (*GoogleBus, error) {
	var options Options
	if len(opts) > 0 {
		options = opts[0]
	} else {
		options = DefaultOptions
	}

	client, err := gpubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}

	bus := &GoogleBus{
		log:         log,
		client:      client,
		options:     options,
		projectID:   projectID,
		topicIDs:    topicIDs,
		topics:      map[string]*gpubsub.Topic{},
		subs:        map[string]map[string]*gpubsub.Subscription{},
		subscribers: map[string]*subscriber{},
	}
	return bus, nil
}

func (m *GoogleBus) Run(ctx context.Context) error {
	if m.IsRunning() {
		return fmt.Errorf("googlebus: already running")
	}

	m.ctx, m.ctxStop = context.WithCancel(ctx)

	atomic.StoreInt32(&m.running, 1)
	defer atomic.StoreInt32(&m.running, 0)

	m.log.Info("googlebus: run")

	// Setup/open topics on Google PubSub
	for _, topicID := range m.topicIDs {
		_, err := m.SetupTopic(topicID)
		if err != nil {
			return fmt.Errorf("googlebus: failed to setup topic: %w", err)
		}
	}

	// block and wait until stopped
	<-m.ctx.Done()
	return nil
}

func (m *GoogleBus) Stop() {
	if !m.IsRunning() {
		return
	}

	// unsubscribe all subscribers
	m.subscribersMu.Lock()
	for _, sub := range m.subscribers {
		sub.unsubscribe()
	}
	m.subscribersMu.Unlock()

	m.log.Info("googlebus: stop")
	m.ctxStop()
}

func (m *GoogleBus) IsRunning() bool {
	return atomic.LoadInt32(&m.running) == 1
}

func (m *GoogleBus) Publish(ctx context.Context, topicID string, message *Message) error {
	if !m.IsRunning() {
		return fmt.Errorf("googlebus: pubsub is not running")
	}

	t, ok := m.topics[topicID]
	if !ok {
		return fmt.Errorf("googlebus: topic not found: %s", topicID)
	}

	_, err := t.Publish(ctx, message).Get(ctx)
	return err
}

func (m *GoogleBus) Subscribe(ctx context.Context, topicID string, optSubcriptionID ...string) (pubsub.Subscription[*Message], error) {
	if len(optSubcriptionID) == 0 {
		return nil, fmt.Errorf("googlebus: subscriptionID is required")
	}
	subscriptionID := optSubcriptionID[0]

	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	s, err := m.SetupSubscription(topicID, subscriptionID)
	if err != nil {
		return nil, err
	}

	s.ReceiveSettings = m.options.ReceiveSettings

	sub := &subscriber{
		pubsub:    m,
		channelID: topicID,
		ch:        make(chan *gpubsub.Message),
		done:      make(chan struct{}),
	}

	sctx, cancel := context.WithCancel(ctx)

	sub.unsubscribe = func() {
		select {
		case <-sub.done:
		default:
			close(sub.done)
		}

		// stop the Receive
		cancel()

		// delete the subscriber
		m.subscribersMu.Lock()
		delete(m.subscribers, subscriptionID)
		m.subscribersMu.Unlock()
	}

	go func() {
		defer close(sub.ch)
		defer close(sub.done)

		// Receive messages from the Google PubSub subscription
		err := s.Receive(sctx, func(ctx context.Context, msg *gpubsub.Message) {
			sub.ch <- msg
		})

		// In case of error, report it on the subscription
		if err != nil {
			sub.err = err
		}
	}()

	m.subscribers[subscriptionID] = sub

	return sub, nil
}

func (m *GoogleBus) NumSubscribers(topicID string) (int, error) {
	s, ok := m.subs[topicID]
	if !ok {
		return 0, nil
	}
	return len(s), nil
}

func (m *GoogleBus) SetupTopic(topicID string) (*gpubsub.Topic, error) {
	t := m.topics[topicID]
	if t != nil {
		return t, nil
	}

	t = m.client.Topic(topicID)
	exists, err := t.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if exists {
		m.topics[topicID] = t
		return t, nil
	}

	// Create a new topic if one doesn't exist
	t, err = m.client.CreateTopic(context.Background(), topicID)
	if err != nil {
		return nil, err
	}

	m.topics[topicID] = t
	return t, nil
}

func (m *GoogleBus) SetupSubscription(topicID, subscriptionID string) (*gpubsub.Subscription, error) {
	s := m.subs[topicID][subscriptionID]
	if s != nil {
		return s, nil
	}

	s = m.client.Subscription(subscriptionID)
	exists, err := s.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if exists {
		if m.subs[topicID] == nil {
			m.subs[topicID] = map[string]*gpubsub.Subscription{}
		}
		m.subs[topicID][subscriptionID] = s
		return s, nil
	}

	// Create a new subscription if one doesn't exist
	subConfig := m.options.SubscriptionConfig
	subConfig.Topic = m.topics[topicID]

	s, err = m.client.CreateSubscription(context.Background(), subscriptionID, subConfig)
	if err != nil {
		return nil, err
	}

	if m.subs[topicID] == nil {
		m.subs[topicID] = map[string]*gpubsub.Subscription{}
	}
	m.subs[topicID][subscriptionID] = s
	return s, nil
}
