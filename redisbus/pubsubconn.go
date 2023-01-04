package redisbus

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

var errInvalidPubSubConn = errors.New("invalid pubsub connection")

type pubSubConn struct {
	conn     *redis.PubSub
	ctx      context.Context
	ctxClose func()

	namespace string
}

func (r *RedisBus[M]) newPubSubConn(ctx context.Context, client *redis.Client, namespace string) (*pubSubConn, error) {
	connCtx, connCtxClose := context.WithCancel(ctx)

	conn := client.Subscribe(connCtx, "ping")
	if _, err := conn.Receive(connCtx); err != nil {
		return nil, err
	}

	return &pubSubConn{
		conn:     conn,
		ctx:      connCtx,
		ctxClose: connCtxClose,

		namespace: namespace,
	}, nil
}

func (p *pubSubConn) Subscribe(channel string) error {
	if p == nil {
		return errInvalidPubSubConn
	}

	return p.conn.Subscribe(p.ctx, p.namespace+channel)
}

func (p *pubSubConn) Unsubscribe(channel string) error {
	if p == nil {
		return errInvalidPubSubConn
	}

	return p.conn.Unsubscribe(p.ctx, p.namespace+channel)
}

func (p *pubSubConn) Ping() error {
	if p == nil {
		return errInvalidPubSubConn
	}

	return p.conn.Ping(p.ctx, "")
}

func (p *pubSubConn) Receive() (interface{}, error) {
	if p == nil {
		return nil, errInvalidPubSubConn
	}

	return p.conn.Receive(p.ctx)
}

func (p *pubSubConn) Done() <-chan struct{} {
	return p.ctx.Done()
}

func (p *pubSubConn) Close() error {
	if p == nil {
		return errInvalidPubSubConn
	}

	p.conn.PUnsubscribe(p.ctx)
	p.ctxClose()
	return p.conn.Close()
}
