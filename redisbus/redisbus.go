package redisbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis/v8"
	"github.com/goware/channel"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type RedisBus[M any] struct {
	log       logger.Logger
	client    *redis.Client
	namespace string
	encoder   MessageEncoder[M]

	psc         *redis.PubSub
	pscCtx      context.Context
	pscCtxClose context.CancelFunc
	pscMu       sync.Mutex

	channels   map[string]map[*subscriber[M]]bool
	channelsMu sync.Mutex

	ctx     context.Context
	ctxStop context.CancelFunc

	running int32
}

var _ pubsub.PubSub[any] = &RedisBus[any]{}

var (
	healthCheckInterval = time.Duration(time.Second * 15)
	// reconnectOnRecoverableFailureInterval = time.Second * 10
)

func New[M any](log logger.Logger, client *redis.Client, optEncoder ...MessageEncoder[M]) (*RedisBus[M], error) {
	var encoder MessageEncoder[M]
	if len(optEncoder) > 0 {
		encoder = optEncoder[0]
	} else {
		encoder = JSONMessageEncoder[M]{}
	}

	// TODO/NOTE: google cloud's managed Redis does not support the "CLIENT"
	// command, as a result, we're unable to use the method below.
	// If we really want this, our options are to wait for GCP to support it
	// or, we can pass the namespace as a config parameter with some Options type.
	//
	// Redis pubsub messaging does not split messaged based on the dbIndex
	// connected, so we have to specify the index to all channels ourselves.
	// redisDBIndex, err := getRedisDBIndex(conn)
	// if err != nil {
	// 	return nil, err
	// }
	// namespace := fmt.Sprintf("%d:", redisDBIndex)
	namespace := ""

	// Construct the bus object
	bus := &RedisBus[M]{
		log:       log,
		client:    client,
		namespace: namespace,
		encoder:   encoder,
		channels:  map[string]map[*subscriber[M]]bool{},
	}
	return bus, nil
}

func (r *RedisBus[M]) Run(ctx context.Context) error {
	if r.IsRunning() {
		return fmt.Errorf("redisbus: already running")
	}
	if r.client == nil {
		return errors.New("redisbus: missing redis connection pool")
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
			r.log.Debug("redisbus: service was stopped")
			return nil
		default:
		}

		// wait before trying to reconnect to pubsub service
		delay := time.Second * time.Duration(float64(retry)*3)
		if delay > 0 {
			r.log.Warnf("redisbus: lost connection, pausing for %v, then retrying to connect (attempt #%d)...", delay, retry)
			time.Sleep(delay)
		}

		err := r.connectAndConsume(r.ctx)
		if err == nil {
			r.log.Debugf("redisbus: service was stopped")
			return nil
		}

		if time.Now().Unix()-int64(time.Duration(time.Minute*3).Seconds()) > lastRetry {
			retry = 0
		}
		if retry > maxRetries {
			r.log.Warnf("redisbus: unable to connect after %d retries, giving up: %v", retry, err)
			return fmt.Errorf("redisbus: unable to connect after %d retries", retry)
		}
		lastRetry = time.Now().Unix()
		retry += 1
		r.log.Debugf("redisbus: unable to connect (retry %d/%d): %v", retry, maxRetries, err)
	}
}

func (r *RedisBus[M]) Stop() {
	if !r.IsRunning() {
		return
	}

	// attempt to unsubscribe all subscribers
	r.channelsMu.Lock()
	for channelID, subscribers := range r.channels {
		for sub := range subscribers {
			r.cleanUpSubscription(channelID, sub)
		}
	}
	r.channelsMu.Unlock()

	// send stop
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

	err = r.client.Publish(ctx, r.namespace+channelID, data).Err()
	if err != nil {
		return fmt.Errorf("redisbus: failed to publish to channel %q: %w", channelID, err)
	}
	return nil
}

func (r *RedisBus[M]) Subscribe(ctx context.Context, channelID string) (pubsub.Subscription[M], error) {
	if !r.IsRunning() {
		return nil, fmt.Errorf("redisbus: pubsub is not running")
	}

	// Pubsub subsystem
	sub := &subscriber[M]{
		pubsub:    r,
		channelID: channelID,
		ch:        channel.NewUnboundedChan[M](r.log, 200, 2000),
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

		r.channelsMu.Lock()
		defer r.channelsMu.Unlock()
		r.cleanUpSubscription(channelID, sub)
	}

	// add channel and subscription
	r.channelsMu.Lock()
	_, ok := r.channels[channelID]
	if !ok {
		r.channels[channelID] = map[*subscriber[M]]bool{}
	}
	r.channels[channelID][sub] = true
	r.channelsMu.Unlock()

	// Redis
	// TODO: at this time, we use a single redis pubsubconn for all subscriptions.
	// In the future we could make multiple psc connections in a pool to use for
	// a bunch of subscriptions.

	// psc, err := r.getPubSubConn()
	// if err != nil {
	// 	return nil, fmt.Errorf("redisbus: unable to subscribe to channel %q: %w", channelID, err)
	// }

	r.pscMu.Lock()
	defer r.pscMu.Unlock()
	// if err := psc.Subscribe(r.namespace + channelID); err != nil {
	// 	return nil, fmt.Errorf("redisbus: failed to subscribe to channel %q: %w", channelID, err)
	// }
	if err := r.psc.Subscribe(r.pscCtx, r.namespace+channelID); err != nil {
		return nil, fmt.Errorf("redisbus: failed to subscribe to channel %q: %w", channelID, err)
	}

	return sub, nil
}

func (r *RedisBus[M]) NumSubscribers(channelID string) (int, error) {
	if !r.IsRunning() {
		return 0, fmt.Errorf("redisbus: pubsub is not running")
	}

	vs, err := r.client.PubSubNumSub(r.ctx, r.namespace+channelID).Result()
	// vs, err := redis.Values(conn.Do("PUBSUB", "NUMSUB", r.namespace+channelID))
	if err != nil {
		return 0, fmt.Errorf("redisbus: failed to retrive subscriber count: %w", err)
	}
	spew.Dump(vs)
	if len(vs) < 2 {
		return 0, nil
	}
	return 0, nil
	// return redis.Int(vs[1], nil)
}

func (r *RedisBus[M]) connectAndConsume(parentCtx context.Context) error {
	var err error

	r.pscCtx, r.pscCtxClose = context.WithCancel(parentCtx)

	psc := r.client.Subscribe(r.pscCtx, "ping")
	ret, err := psc.Receive(r.pscCtx)
	if err != nil {
		return err
	}
	spew.Dump(ret)

	r.psc = psc

	defer func() {
		r.pscMu.Lock()
		psc.PUnsubscribe(r.pscCtx)
		r.pscCtxClose()
		psc.Close()
		r.psc = nil
		r.pscMu.Unlock()
	}()

	// TODO: at this time, we have a single PubSubConn for all of our subscribers,
	// but instead we should have some kind of pool, like 20% of our connections (MaxConn etc.)
	// could be used for just pubsub..
	// r.pscMu.Lock()
	// if r.psc.Conn != nil {
	// 	r.pscMu.Unlock()
	// 	return fmt.Errorf("redisbus: connection already opened")
	// }

	// r.pscCtx, r.pscCtxClose = context.WithCancel(parentCtx)

	// redisConn, err := r.pool.GetContext(r.pscCtx)
	// if err != nil {
	// 	r.pscMu.Unlock()
	// 	return fmt.Errorf("redisbus: failed to get connection: %w", err)
	// }

	// r.psc, err = newPubSubConn(redisConn)
	// if err != nil {
	// 	r.psc.Conn = nil
	// 	r.pscMu.Unlock()
	// 	return err
	// }

	// r.pscMu.Unlock()

	// defer func() {
	// 	r.pscMu.Lock()
	// 	if err := r.psc.Unsubscribe(); err != nil {
	// 		r.log.Warnf("redisbus: unable to unsubscribe from all channels: %v", err)
	// 	}
	// 	r.pscCtxClose()
	// 	if err := r.psc.Conn.Flush(); err != nil {
	// 		r.log.Warnf("redisbus: unable to flush pubsub connection: %v", err)
	// 	}
	// 	if err := r.psc.Close(); err != nil {
	// 		r.log.Warnf("redisbus: unable to close pubsub connection gracefully: %v", err)
	// 	}
	// 	r.psc.Conn = nil
	// 	r.pscMu.Unlock()
	// }()

	// re-subscribing to all channels
	r.channelsMu.Lock()
	for channelID := range r.channels {
		if len(r.channels[channelID]) == 0 {
			continue
		}
		if err := psc.Subscribe(r.pscCtx, r.namespace+channelID); err != nil {
			r.log.Warnf("redisbus: failed to re-subscribe to channel %q due to %v", channelID, err)
			r.channelsMu.Unlock()
			return err
		}
		r.log.Debugf("redisbus: re-subscribed to channel %q", channelID)
	}
	r.channelsMu.Unlock()

	return r.consumeMessages(r.pscCtx)
}

func (r *RedisBus[M]) consumeMessages(ctx context.Context) error {
	errCh := make(chan error, 1)

	// reading messages
	go func() {
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				// context signaled to stop listening
				return
			default:
			}

			msg, err := r.psc.Receive(ctx)
			if err != nil {
				// TODO: timeout review..?
				// if os.IsTimeout(redisMsg) {
				// 	continue // ok
				// }
				errCh <- err
				return
			}

			switch redisMsg := msg.(type) {

			case *redis.Message:
				var msg M
				err := r.encoder.DecodeMessage([]byte(redisMsg.Payload), &msg)
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

			case *redis.Subscription:
				if redisMsg.Count > 0 {
					r.log.Debugf("redisbus: received action %s on channel %q (%d)", redisMsg.Kind, redisMsg.Channel, redisMsg.Count)
				}

			case *redis.Pong:
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
			if err != nil {
				return err
			}
			break loop

		case <-ticker.C:
			r.pscMu.Lock()
			err := r.psc.Ping(r.pscCtx, "")
			r.pscMu.Unlock()

			if err != nil {
				r.log.Errorf("redisbus: ping health check error: %v", err)
				break loop
			}
		}
	}

	return nil
}

func (r *RedisBus[M]) broadcast(ctx context.Context, channelID string, message M) error {
	r.channelsMu.Lock()
	defer r.channelsMu.Unlock()

	subscribers, ok := r.channels[channelID]
	if !ok {
		// no subscribers on this channel, which is okay

		// TODO: with redis though, we should have subscribers if we're receiving
		// this message?  it means the subscribers are out-of-sync, from our
		// internal chan state and redis bus
		return nil
	}

	for sub, ok := range subscribers {
		if !ok {
			continue
		}
		sub.ch.Send(message)
	}

	return nil
}

func (r *RedisBus[M]) cleanUpSubscription(channelID string, sub *subscriber[M]) {
	if len(r.channels[channelID]) < 1 {
		return
	}
	if !r.channels[channelID][sub] {
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
	delete(r.channels[channelID], sub)

	if len(r.channels[channelID]) > 0 {
		return // channel has more subscriptions, exit here
	}

	// channel has no more subscribers
	r.log.Debugf("redisbus: removing channel %q", channelID)

	// delete channel
	delete(r.channels, channelID)

	// unsubscribe from channel
	// psc, _ := r.getPubSubConn()
	// if psc.Conn == nil {
	// 	return
	// }
	if r.psc == nil {
		return
	}

	r.pscMu.Lock()
	if err := r.psc.Unsubscribe(r.pscCtx, r.namespace+channelID); err != nil {
		// just log, not a fatal error
		r.log.Warnf("redisbus: failed to unsubscribe from channel %q: %v", channelID, err)
	}
	r.pscMu.Unlock()
}

// func (r *RedisBus[M]) getPubSubConn() (redis.PubSubConn, error) {
// 	r.pscMu.Lock()
// 	defer r.pscMu.Unlock()

// 	psc := r.psc
// 	if psc.Conn == nil {
// 		return psc, errors.New("no active pubsubconn")
// 	}

// 	return psc, nil
// }

// func newPubSubConn(conn redis.Conn) (redis.PubSubConn, error) {
// 	psc := redis.PubSubConn{Conn: conn}

// 	// internal service channel (this is currently only used for pinging)
// 	if err := psc.Subscribe("ping"); err != nil {
// 		return psc, fmt.Errorf("redisbus: failed to subscribe to ping channel: %w", err)
// 	}

// 	return psc, nil
// }
