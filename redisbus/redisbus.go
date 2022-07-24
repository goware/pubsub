package redisbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/goware/pubsub"
)

// TODO:
// - [ ] if client disconnects, i assume subscribe's are reset too..? lets reconnect as needed..
// - [ ] sounds like a client can either only be a subscriber or a publisher, never both.

// TODO: lets add a logger i guess?

var (
	healthCheckInterval                   = time.Duration(time.Second * 1) // time.Duration(time.Second * 15)
	reconnectOnRecoverableFailureInterval = time.Second * 10
)

type RedisBus[M any] struct {
	conn        *redis.Pool
	psc         *redis.PubSubConn
	namespace   string
	encoder     MessageEncoder[M]
	subscribers map[string][]*subscriber[M]

	mu sync.Mutex
}

var _ pubsub.PubSub[any] = &RedisBus[any]{}

func New[M any](conn *redis.Pool, optEncoder ...MessageEncoder[M]) (*RedisBus[M], error) {
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
		conn:        conn,
		namespace:   namespace,
		encoder:     encoder,
		subscribers: make(map[string][]*subscriber[M], 0),
	}
	return bus, nil
}

func (r *RedisBus[M]) Run(ctx context.Context) error {
	if r.conn == nil {
		return errors.New("missing redis connection")
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				// in case of shutdown, stop reconnect attempt
				return
			default:
			}

			err := r.connectAndListen(ctx)
			if err == nil {
				// r.log.Info().Msg("terminated gracefully")
				return
			}

			// r.log.Warn().Err(err).Msg("pubsub subscription failed")

			// wait before trying to reconnect to pubsub service
			// TODO: make it quick, but progressively slow down
			time.Sleep(2 * time.Second)
		}
	}()
	return nil
}

func (r *RedisBus[M]) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.psc != nil {
		r.psc.Unsubscribe()
	}

	for _, subscribers := range r.subscribers {
		for _, sub := range subscribers {
			sub.Unsubscribe()
		}
	}

	return nil
}

func (r *RedisBus[M]) Publish(ctx context.Context, channelID string, message M) error {
	data, err := r.encoder.EncodeMessage(message)
	if err != nil {
		return fmt.Errorf("encoding error: %w", err)
	}

	conn := r.conn.Get()
	defer conn.Close()

	_, err = conn.Do("PUBLISH", r.namespace+channelID, data)
	if err != nil {
		// r.log.Warn().Err(err).Msgf("failed to publish to channel %q", channel)
		fmt.Println("publish failed to chan", channelID)
		return err
	}

	return nil
}

func (r *RedisBus[M]) Subscribe(ctx context.Context, channelID string) pubsub.Subscription[M] {
	// TODO.. make a new pubsub conn...........
	// then....... subscribe......
	// i think, we can multiplex this though... dont need new conn per subscribe..

	r.mu.Lock()
	defer r.mu.Unlock()

	// Redis..
	err := r.psc.Subscribe(r.namespace + channelID)
	if err != nil {
		// TODO: perhaps Subscribe(..) method should also return an error
		fmt.Println("subscribe error...", err)
		_ = err
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
		sendCh:    pubsub.MakeUnboundedBuffered(ch, 100),
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

	return subscriber
}

func (r *RedisBus[M]) NumSubscribers(channelID string) (int, error) {
	conn := r.conn.Get()
	defer conn.Close()

	vs, err := redis.Values(conn.Do("PUBSUB", "NUMSUB", r.namespace+channelID))
	if err != nil {
		return 0, fmt.Errorf("failed to retrive subscriber count: %w", err)
	}
	if len(vs) < 2 {
		return 0, nil
	}
	return redis.Int(vs[1], nil)
}

func (r *RedisBus[M]) connectAndListen(ctx context.Context) error {
	// TODO: ad this time, we have a single PubSubConn for all of our subscribers,
	// but instead we should have some kind of pool, like 20% of our connections (MaxConn etc.)
	// could be used for just pubsub..
	psc := redis.PubSubConn{Conn: r.conn.Get()}

	r.mu.Lock()
	r.psc = &psc
	// re-subscribing to all channels
	for channelID := range r.subscribers {
		if err := psc.Subscribe(r.namespace + channelID); err != nil {
			// r.log.Warn().Err(err).Msgf("failed to re-subscribe to channel %q", channel)
			fmt.Println("failed to re-sub to channel", channelID)
			r.mu.Unlock()
			return err
		}
	}
	r.mu.Unlock()

	// internal service channel (this is currently only used for pinging)
	if err := psc.Subscribe("ping"); err != nil {
		// TODO: announce presence to other matchmaker instances
		return err
	}

	errCh := make(chan error, 1)

	go func() {

		// reading messages
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
					fmt.Println("decoding error..", err)
				}

				if len(redisMsg.Channel) < len(r.namespace) {
					fmt.Println("weird..../ error ..")
					continue
				}
				channelID := redisMsg.Channel[len(r.namespace):]

				err = r.broadcast(ctx, channelID, msg)
				if err != nil {
					fmt.Println("subscriber broadcast error", err)
					continue
				}

			case redis.Subscription:
				if redisMsg.Count > 0 {
					// TODO: we can leave this as a Debug message..
					// fmt.Println("sup?, got action", redisMsg.Kind, redisMsg.Channel, redisMsg.Count)
					// 	r.log.Trace().Msgf("got %q action on %q (%v subscribers)", msg.Kind, msg.Channel, msg.Count)
				}

			case redis.Pong:
				// ok!
				// fmt.Println("redis, Pong..?")
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
			// r.log.Warn().Err(err).Msgf("received failure while reading from pubsub service: %v", err)
			fmt.Println("loop err", err)
			return err

		case <-ticker.C:
			if err := psc.Ping(""); err != nil {
				// r.log.Warn().Err(err).Msg("failed to check pubsub service health")
				fmt.Println("ping err?", err)
				break loop
			}
		}
	}

	// Unsubscribe from all channels, it will resubscribe on connect, and
	// on Stop(), we will force-unsubscribe all, etc.
	if err := psc.Unsubscribe(); err != nil {
		fmt.Println("psc unsub err", err)
		// r.log.Warn().Err(err).Msgf("received failure while unsubscribing from pubsub service: %v", err)
		return err
	}

	close(errCh)

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
