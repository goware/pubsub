package redisbus

import (
	"context"
	"encoding/json"
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

var (
	healthCheckInterval                   = time.Duration(time.Second * 30)
	reconnectOnRecoverableFailureInterval = time.Second * 10
)

type RedisBus[M any] struct {
	conn *redis.Pool
	psc  *redis.PubSubConn

	mu sync.Mutex
}

var _ pubsub.PubSub[any] = &RedisBus[any]{}

func New[M any](conn *redis.Pool) (*RedisBus[M], error) {
	bus := &RedisBus[M]{
		conn: conn,
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
	// TODO, see ethmonitor
	return nil
}

func (r *RedisBus[M]) Publish(ctx context.Context, channelID string, message M) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	conn := r.conn.Get()
	defer conn.Close()

	// TODO: lets build the channel name with the db prefix, etc..

	_, err = conn.Do("PUBLISH", channelID, data)
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

	err := r.psc.Subscribe(channelID)
	if err != nil {
		fmt.Println("subscribe error...", err)
		_ = err
	}

	return nil
}

func (r *RedisBus[M]) NumSubscribers(channelID string) (int, error) {
	conn := r.conn.Get()
	defer conn.Close()

	vs, err := redis.Values(conn.Do("PUBSUB", "NUMSUB", channelID))
	if err != nil {
		return 0, fmt.Errorf("failed to retrive subscriber count: %w", err)
	}
	if len(vs) < 2 {
		return 0, nil
	}
	return redis.Int(vs[1], nil)
}

func (r *RedisBus[M]) connectAndListen(ctx context.Context) error {
	psc := redis.PubSubConn{Conn: r.conn.Get()}

	r.mu.Lock()
	r.psc = &psc
	// re-subscribing to all channels
	// for channel := range r.bus {
	// 	if err := psc.Subscribe(channel); err != nil {
	// 		// r.log.Warn().Err(err).Msgf("failed to re-subscribe to channel %q", channel)
	// 		fmt.Println("failed to re-sub to channel", channel)
	// 		r.mu.Unlock()
	// 		return err
	// 	}
	// }
	r.mu.Unlock()

	// internal service channel (this is currently only used for pinging)
	if err := psc.Subscribe("xyz..todo"); err != nil {
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
				err := json.Unmarshal(redisMsg.Data, &msg)
				if err != nil {
					fmt.Println("unmarshal error..", err)
				}

				// TODO: this is more of the broadcast..
				// to like, broadcast to the subscribers..

				err = r.Publish(ctx, redisMsg.Channel, msg)
				if err != nil {
					fmt.Println("publish error", err)
				}
				// event, err := decodeEventMessage(msg.Data)
				// if err != nil {
				// 	r.log.Warn().Err(err).Msg("failed to decode event")
				// 	continue
				// }
				// if err := r.broadcast(msg.Channel, *event); err != nil {
				// 	r.log.Warn().Err(err).Msgf("failed to broadcast message to channel: %q", msg.Channel)
				// }
			case redis.Subscription:
				if redisMsg.Count > 0 {
					fmt.Println("sup?, got action", redisMsg.Kind, redisMsg.Channel, redisMsg.Count)
				}
				// if msg.Count > 0 {
				// 	r.log.Trace().Msgf("got %q action on %q (%v subscribers)", msg.Kind, msg.Channel, msg.Count)
				// }
			case redis.Pong:
				// ok!
			}
		}
	}()

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			if err := psc.Ping(""); err != nil {
				// r.log.Warn().Err(err).Msg("failed to check pubsub service health")
				fmt.Println("ping err?", err)
				break loop
			}
		case <-ctx.Done():
			break loop
		case err := <-errCh:
			// r.log.Warn().Err(err).Msgf("received failure while reading from pubsub service: %v", err)
			fmt.Println("loop err", err)
			return err
		}
	}

	if err := psc.Unsubscribe(); err != nil {
		fmt.Println("psc unsub err", err)
		// r.log.Warn().Err(err).Msgf("received failure while unsubscribing from pubsub service: %v", err)
		return err
	}

	close(errCh)

	return nil
}
