package redisbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/goware/pubsub"
	"github.com/goware/pubsub/logger"
)

type RedisBus[M any] struct {
	log         logger.Logger
	conn        *redis.Pool
	psc         *redis.PubSubConn
	namespace   string
	encoder     MessageEncoder[M]
	subscribers map[string][]*subscriber[M]

	ctx     context.Context
	ctxStop context.CancelFunc
	running int32
	mu      sync.Mutex
}

var _ pubsub.PubSub[any] = &RedisBus[any]{}

var (
	healthCheckInterval                   = time.Duration(time.Second * 15)
	reconnectOnRecoverableFailureInterval = time.Second * 10
)

func New[M any](log logger.Logger, conn *redis.Pool, optEncoder ...MessageEncoder[M]) (*RedisBus[M], error) {
	var encoder MessageEncoder[M]
	if len(optEncoder) > 0 {
		encoder = optEncoder[0]
	} else {
		encoder = JSONMessageEncoder[M]{}
	}

	redisDBIndex, err := getRedisDBIndex(conn)
	if err != nil {
		return nil, err
	}

	// Redis pubsub messaging does not split messaged based on the dbIndex
	// connected, so we have to specify the index to all channels ourselves.
	namespace := fmt.Sprintf("%d:", redisDBIndex)

	// Construct the bus object
	bus := &RedisBus[M]{
		log:         log,
		conn:        conn,
		namespace:   namespace,
		encoder:     encoder,
		subscribers: make(map[string][]*subscriber[M], 0),
	}
	return bus, nil
}

func (r *RedisBus[M]) Run(ctx context.Context) error {
	if r.conn == nil {
		return errors.New("redisbus: missing redis connection")
	}
	if r.IsRunning() {
		return fmt.Errorf("redisbus: already running")
	}

	r.ctx, r.ctxStop = context.WithCancel(ctx)

	atomic.StoreInt32(&r.running, 1)
	defer atomic.StoreInt32(&r.running, 0)

	r.log.Info("redisbus: run")

	// attempt to reconnect a number of consecutive times, before
	// stopping attempts and returning an error
	const maxRetries = 5
	retry := 0
	lastRetry := time.Now().Unix()

	for {
		select {
		case <-r.ctx.Done():
			// in case of shutdown, stop reconnect attempts
			return nil
		default:
		}

		err := r.connectAndListen(r.ctx)
		if err != nil {
			if time.Now().Unix()-int64(time.Duration(time.Minute*3).Seconds()) > lastRetry {
				retry = 0
			}
			if retry >= maxRetries {
				return err
			}
			lastRetry = time.Now().Unix()
			retry += 1
		}

		// wait before trying to reconnect to pubsub service
		delay := time.Second * time.Duration(float64(retry)*3)
		r.log.Warnf("redisbus: lost connection, pausing for %v, then retrying to connect (attempt #%d)...", delay, retry)
		time.Sleep(delay)
	}
}

func (r *RedisBus[M]) Stop() {
	if !r.IsRunning() {
		return
	}

	if r.psc != nil {
		r.psc.Unsubscribe()
	}

	for _, subscribers := range r.subscribers {
		for _, sub := range subscribers {
			sub.Unsubscribe()
		}
	}

	r.log.Info("redisbus: stop")
	r.ctxStop()
}

func (r *RedisBus[M]) IsRunning() bool {
	return atomic.LoadInt32(&r.running) == 1
}

func (r *RedisBus[M]) Publish(ctx context.Context, channelID string, message M) error {
	if !r.IsRunning() {
		return fmt.Errorf("redisbus: pubsub is not running")
	}

	data, err := r.encoder.EncodeMessage(message)
	if err != nil {
		return fmt.Errorf("redisbus: encoding error: %w", err)
	}

	conn := r.conn.Get()
	defer conn.Close()

	_, err = conn.Do("PUBLISH", r.namespace+channelID, data)
	if err != nil {
		return fmt.Errorf("redisbus: failed to publish to channel %q: %w", channelID, err)
	}
	return nil
}

func (r *RedisBus[M]) Subscribe(ctx context.Context, channelID string) (pubsub.Subscription[M], error) {
	if !r.IsRunning() {
		return nil, fmt.Errorf("redisbus: pubsub is not running")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Redis
	// TODO: at this time, we use a single redis pubsubconn for all subscriptions.
	// In the future we could make multiple psc connections in a pool to use for
	// a bunch of subscriptions.
	err := r.psc.Subscribe(r.namespace + channelID)
	if err != nil {
		return nil, fmt.Errorf("redisbus: failed to subscribe to channel %q: %w", channelID, err)
	}

	// Pubsub subsystem
	_, ok := r.subscribers[channelID]
	if !ok {
		r.subscribers[channelID] = []*subscriber[M]{}
	}

	ch := make(chan M)
	subscriber := &subscriber[M]{
		channelID: channelID,
		ch:        ch,
		sendCh:    pubsub.MakeUnboundedBuffered(ch, r.log, 100),
		done:      make(chan struct{}),
	}

	subscriber.unsubscribe = func() {
		close(subscriber.done)
		r.mu.Lock()
		defer r.mu.Unlock()
		close(subscriber.sendCh)

		// flush subscriber.ch so that the MakeUnboundedBuffered goroutine exits
		for ok := true; ok; _, ok = <-subscriber.ch {
		}

		for i, sub := range r.subscribers[channelID] {
			if sub == subscriber {
				r.subscribers[channelID] = append(r.subscribers[channelID][:i], r.subscribers[channelID][i+1:]...)
				if len(r.subscribers[channelID]) == 0 {
					delete(r.subscribers, channelID)
				}
				return
			}
		}
	}

	r.subscribers[channelID] = append(r.subscribers[channelID], subscriber)

	return subscriber, nil
}

func (r *RedisBus[M]) NumSubscribers(channelID string) (int, error) {
	if !r.IsRunning() {
		return 0, fmt.Errorf("redisbus: pubsub is not running")
	}

	conn := r.conn.Get()
	defer conn.Close()

	vs, err := redis.Values(conn.Do("PUBSUB", "NUMSUB", r.namespace+channelID))
	if err != nil {
		return 0, fmt.Errorf("redisbus: failed to retrive subscriber count: %w", err)
	}
	if len(vs) < 2 {
		return 0, nil
	}
	return redis.Int(vs[1], nil)
}

func (r *RedisBus[M]) connectAndListen(ctx context.Context) error {
	// TODO: at this time, we have a single PubSubConn for all of our subscribers,
	// but instead we should have some kind of pool, like 20% of our connections (MaxConn etc.)
	// could be used for just pubsub..
	psc := redis.PubSubConn{Conn: r.conn.Get()}
	defer func() {
		psc.Close()
		r.psc = nil
	}()

	// internal service channel (this is currently only used for pinging)
	if err := psc.Subscribe("ping"); err != nil {
		return fmt.Errorf("redisbus: failed to subscribe to ping channel: %w", err)
	}

	r.mu.Lock()
	r.psc = &psc
	// re-subscribing to all channels
	for channelID := range r.subscribers {
		if err := psc.Subscribe(r.namespace + channelID); err != nil {
			r.log.Warnf("redisbus: failed to re-subscribe to channel %q due to %v", channelID, err)
			r.mu.Unlock()
			return err
		}
	}
	r.mu.Unlock()

	errCh := make(chan error, 1)
	defer close(errCh)

	// reading messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				// context signaled to stop listening
				return
			default:
			}

			switch redisMsg := psc.Receive().(type) {

			case error:
				errCh <- redisMsg
				return

			case redis.Message:
				var msg M
				err := r.encoder.DecodeMessage(redisMsg.Data, &msg)
				if err != nil {
					r.log.Errorf("redisbus: error decoding message: %v", err)
					continue
				}

				if len(redisMsg.Channel) < len(r.namespace) {
					r.log.Errorf("redisbus: unexpected channel name from message received by subscriber")
					continue
				}
				channelID := redisMsg.Channel[len(r.namespace):]

				err = r.broadcast(ctx, channelID, msg)
				if err != nil {
					r.log.Errorf("redisbus: subscriber broadcast error to channel %q: %v", channelID, err)
					continue
				}

			case redis.Subscription:
				if redisMsg.Count > 0 {
					r.log.Debugf("redisbus: received action %s %s %d", redisMsg.Kind, redisMsg.Channel, redisMsg.Count)
				}

			case redis.Pong:
				// ok! skip
				r.log.Debugf("redisbus: pubsub pong")

			default:
				// skip
			}
		}
	}()

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

loop:
	for {
		select {

		case <-ctx.Done():
			break loop

		case err := <-errCh:
			return fmt.Errorf("redisbus: received error from pubsub: %w", err)

		case <-ticker.C:
			if err := psc.Ping(""); err != nil {
				r.log.Error("redisbus: ping health check error: %w", err)
				break loop
			}
		}
	}

	// Unsubscribe from all channels, it will resubscribe on connect, and
	// on Stop(), we will force-unsubscribe all, etc.
	if err := psc.Unsubscribe(); err != nil {
		return fmt.Errorf("redisbus: unsubscribe from health check error: %w", err)
	}

	return nil
}

func (r *RedisBus[M]) broadcast(ctx context.Context, channelID string, message M) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.subscribers[channelID]
	if !ok {
		// no subscribers on this channel, which is okay
		// TODO: with redis though, we should have subscribers if we're receiving this message?
		// it means the subscribers are out-of-sync, from our internal chan state and redis bus
		return nil
	}

	for _, sub := range r.subscribers[channelID] {
		select {
		case <-sub.done:
		case sub.sendCh <- message:
		}
	}

	return nil
}

type MessageEncoder[M any] interface {
	EncodeMessage(message M) ([]byte, error)
	DecodeMessage(data []byte, message *M) error
}

type JSONMessageEncoder[M any] struct{}

func (e JSONMessageEncoder[M]) EncodeMessage(message M) ([]byte, error) {
	return json.Marshal(message)
}

func (e JSONMessageEncoder[M]) DecodeMessage(data []byte, message *M) error {
	return json.Unmarshal(data, message)
}
