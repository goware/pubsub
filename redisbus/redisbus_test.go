package redisbus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type message struct {
	Body string
}

// func newPool(addr string) *redis.Pool {
// 	return &redis.Pool{
// 		Dial: func() (redis.Conn, error) {
// 			return redis.Dial("tcp", addr)
// 		},
// 	}
// }

func newRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})
}

func TestRedisbusStartStop(t *testing.T) {
	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- bus.Run(context.Background())
	}()

	time.Sleep(time.Second * 1)
	bus.Stop()

	err = <-runErr
	assert.NoError(t, err)
}

func TestRedisbusStartStopRestart(t *testing.T) {
	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	{
		runErr := make(chan error, 1)
		go func() {
			runErr <- bus.Run(context.Background())
		}()

		time.Sleep(time.Second * 1)
		bus.Stop()

		err = <-runErr
		assert.NoError(t, err)
	}

	{
		runErr := make(chan error, 1)
		go func() {
			runErr <- bus.Run(context.Background())
		}()

		time.Sleep(time.Second * 1)
		bus.Stop()

		err = <-runErr
		assert.NoError(t, err)
	}
}

func TestRedisbusSendAndUnsubscribe(t *testing.T) {
	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			require.NoError(t, err)
		}
	}()
	defer bus.Stop()

	time.Sleep(1 * time.Second) // wait for run to start

	sub1, _ := bus.Subscribe(context.Background(), "peter")
	sub2, _ := bus.Subscribe(context.Background(), "julia")
	sub3, _ := bus.Subscribe(context.Background(), "julia")

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub1.Unsubscribe()
			sub2.Unsubscribe()
			sub3.Unsubscribe()
		}()
	}
	wg.Wait()
}

func TestRedisbusSendAndReceiveConcurrently(t *testing.T) {
	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			require.NoError(t, err)
		}
	}()
	defer bus.Stop()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 20
	subscribers := make([]pubsub.Subscription[message], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			received := <-subscribers[i].ReadMessage()
			assert.Equal(t, fmt.Sprintf("hello: %d", i), received.Body)
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := subscribers[i].SendMessage(context.Background(), message{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func TestRedisbusOverSendAndReceiveConcurrently(t *testing.T) {
	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			require.NoError(t, err)
		}
	}()
	defer bus.Stop()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 20
	subscribers := make([]pubsub.Subscription[message], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			received := <-subscribers[i].ReadMessage()
			assert.Equal(t, fmt.Sprintf("hello: %d", i), received.Body)
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := subscribers[i].SendMessage(context.Background(), message{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)

			err = subscribers[i].SendMessage(context.Background(), message{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)

			err = subscribers[i].SendMessage(context.Background(), message{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func TestRedisbusSendAndReceiveConcurrentlyThenStop(t *testing.T) {
	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			require.NoError(t, err)
		}
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 100
	subscribers := make([]pubsub.Subscription[message], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = subscribers[i].SendMessage(context.Background(), message{Body: fmt.Sprintf("hello: %d", i)})
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, ok := <-subscribers[i].ReadMessage()
			if ok {
				bus.Stop()
			}
		}(i)
	}
	wg.Wait()
}

func TestRedisbusSendLargeAmount(t *testing.T) {
	// this is a long running test, you may restart redis service while the test
	// is executing

	pool := newRedisClient("127.0.0.1:6379")

	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	assert.NoError(t, err)

	go func() {
		err := bus.Run(context.Background())
		if err != nil {
			require.NoError(t, err)
		}
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 100
	subscribers := make([]pubsub.Subscription[message], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		require.NoError(t, err)
	}

	require.Len(t, subscribers, subscribersNum)

	nTickets := 2000
	tickets := make(chan struct{}, nTickets)

	for i := 0; i < nTickets; i++ {
		tickets <- struct{}{}
	}

	var wg sync.WaitGroup

	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				message, ok := <-subscribers[i].ReadMessage()
				if !ok {
					break
				}
				t.Logf("message: %v", message)
			}
		}(i)
	}

	testUntil := time.Now().Add(time.Second * 60)
	for {
		if time.Now().After(testUntil) {
			t.Logf("stopping test")
			bus.Stop()
			break
		}
		for i := 0; i < subscribersNum; i++ {
			go func(i int, ticket struct{}) {
				defer func() {
					tickets <- ticket
				}()
				_ = subscribers[i].SendMessage(context.Background(), message{Body: fmt.Sprintf("hello: %d", i)})
			}(i, <-tickets)
		}
	}

	wg.Wait()
}
