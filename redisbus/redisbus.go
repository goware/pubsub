package redisbus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goware/channel"
	"github.com/goware/pubsub"
	"github.com/redis/go-redis/v9"
)

type RedisBus[M any] struct {
	log       *slog.Logger
	client    *redis.Client
	namespace string
	encoder   MessageEncoder[M]

	psc   *pubSubConn
	pscMu sync.Mutex

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

func New[M any](log *slog.Logger, client *redis.Client, optEncoder ...MessageEncoder[M]) (*RedisBus[M], error) {
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
			r.log.Warn("redisbus: lost connection, retrying to connect",
				slog.Duration("delay", delay),
				slog.Int("attempt", retry))
			time.Sleep(delay)
		}

		err := r.connectAndConsume(r.ctx)
		if err == nil {
			r.log.Debug("redisbus: service was stopped")
			return nil
		}

		if time.Now().Unix()-int64(time.Duration(time.Minute*3).Seconds()) > lastRetry {
			retry = 0
		}
		if retry > maxRetries {
			r.log.Warn("redisbus: unable to connect, giving up",
				slog.Int("retries", retry),
				slog.String("error", err.Error()))
			return fmt.Errorf("redisbus: unable to connect after %d retries", retry)
		}
		lastRetry = time.Now().Unix()
		retry += 1
		r.log.Debug("redisbus: unable to connect",
			slog.Int("retry", retry),
			slog.Int("max_retries", maxRetries),
			slog.String("error", err.Error()))
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

func (r *RedisBus[M]) Subscribe(ctx context.Context, channelID string, optSubcriptionID ...string) (pubsub.Subscription[M], error) {
	if !r.IsRunning() {
		return nil, fmt.Errorf("redisbus: pubsub is not running")
	}

	// Pubsub subsystem
	sub := &subscriber[M]{
		pubsub:    r,
		channelID: channelID,
		ch: channel.NewUnboundedChan[M](1000, -1, channel.Options{
			Logger: r.log,
		}),
		done: make(chan struct{}),
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
		r.cleanUpSubscription(channelID, sub)
		r.channelsMu.Unlock()
	}

	// add channel and subscription
	r.channelsMu.Lock()
	_, ok := r.channels[channelID]
	if !ok {
		r.channels[channelID] = map[*subscriber[M]]bool{}
	}
	r.channels[channelID][sub] = true
	r.channelsMu.Unlock()

	r.pscMu.Lock()
	defer r.pscMu.Unlock()

	if r.psc == nil {
		return nil, errors.New("redisbus: consumer is not initialized")
	}

	if err := r.psc.Subscribe(channelID); err != nil {
		return nil, fmt.Errorf("redisbus: failed to subscribe to channel %q: %w", channelID, err)
	}

	return sub, nil
}

func (r *RedisBus[M]) NumSubscribers(channelID string) (int, error) {
	if !r.IsRunning() {
		return 0, fmt.Errorf("redisbus: pubsub is not running")
	}

	redisChannel := r.namespace + channelID

	res, err := r.client.PubSubNumSub(r.ctx, redisChannel).Result()
	if err != nil {
		return 0, fmt.Errorf("redisbus: failed to retrive subscriber count: %w", err)
	}
	return int(res[redisChannel]), nil
}

func (r *RedisBus[M]) connectAndConsume(ctx context.Context) error {
	var err error

	psc, err := r.newPubSubConn(ctx, r.client, r.namespace)
	if err != nil {
		return err
	}

	defer func() {
		if err := psc.Close(); err != nil {
			r.log.Debug("redisbus: unable to close pubsub connection gracefully",
				slog.String("error", err.Error()))
		}
		r.pscMu.Lock()
		r.psc = nil
		r.pscMu.Unlock()
	}()

	r.pscMu.Lock()
	r.psc = psc
	r.pscMu.Unlock()

	// re-subscribing to all channels
	r.channelsMu.Lock()
	for channelID := range r.channels {
		if len(r.channels[channelID]) == 0 {
			continue
		}
		if err := psc.Subscribe(channelID); err != nil {
			r.log.Warn("redisbus: failed to re-subscribe to channel",
				slog.String("channel", channelID),
				slog.String("error", err.Error()))
			r.channelsMu.Unlock()
			return err
		}
		r.log.Debug("redisbus: re-subscribed to channel",
			slog.String("channel", channelID))
	}
	r.channelsMu.Unlock()

	return r.consumeMessages(psc)
}

func (r *RedisBus[M]) consumeMessages(psc *pubSubConn) error {
	errCh := make(chan error, 1)

	// reading messages
	go func() {
		defer close(errCh)

		for {
			select {
			case <-psc.Done():
				// context signaled to stop listening
				return
			default:
			}

			msg, err := psc.Receive()
			if err != nil {
				errCh <- err
				return
			}

			switch redisMsg := msg.(type) {

			case *redis.Message:
				var msg M
				err := r.encoder.DecodeMessage([]byte(redisMsg.Payload), &msg)
				if err != nil {
					r.log.Error("redisbus: error decoding message",
						slog.String("error", err.Error()))
					continue
				}

				if len(redisMsg.Channel) < len(r.namespace) {
					r.log.Error("redisbus: unexpected channel name from message received by subscriber")
					continue
				}
				channelID := redisMsg.Channel[len(r.namespace):]

				err = r.broadcast(channelID, msg)
				if err != nil {
					r.log.Error("redisbus: subscriber broadcast error",
						slog.String("channel", channelID),
						slog.String("error", err.Error()))
					continue
				}

			case *redis.Subscription:
				if redisMsg.Count > 0 {
					r.log.Debug("redisbus: received subscription action",
						slog.String("action", redisMsg.Kind),
						slog.String("channel", redisMsg.Channel),
						slog.Int("count", redisMsg.Count))
				}

			case *redis.Pong:
				// ok! skip
				r.log.Debug("redisbus: pubsub pong")

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

		case <-psc.Done():
			break loop

		case err := <-errCh:
			if err != nil {
				return err
			}
			break loop

		case <-ticker.C:
			if err := psc.Ping(); err != nil {
				r.log.Error("redisbus: ping health check error",
					slog.String("error", err.Error()))
				break loop
			}
		}
	}

	return nil
}

func (r *RedisBus[M]) broadcast(channelID string, message M) error {
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
	r.log.Debug("redisbus: removing channel",
		slog.String("channel", channelID))

	// delete channel
	delete(r.channels, channelID)

	r.pscMu.Lock()
	defer r.pscMu.Unlock()

	if r.psc == nil {
		return
	}

	if err := r.psc.Unsubscribe(channelID); err != nil {
		r.log.Warn("redisbus: failed to unsubscribe from channel",
			slog.String("channel", channelID),
			slog.String("error", err.Error()))
	}
}
