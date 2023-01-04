package redisbus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type messageEnvelope struct {
	Body string
}

func newRedisTestClient() *redis.Client {
	const addr = "127.0.0.1:6379"
	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

func TestRedisbusStartStop(t *testing.T) {
	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

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
	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

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

		assert.NoError(t, <-runErr)
	}
}

func TestRedisbusSendAndUnsubscribe(t *testing.T) {
	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- bus.Run(context.Background())
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	sub1, err := bus.Subscribe(context.Background(), "peter")
	require.NoError(t, err)

	sub2, err := bus.Subscribe(context.Background(), "julia")
	require.NoError(t, err)

	sub3, err := bus.Subscribe(context.Background(), "julia")
	require.NoError(t, err)

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

	bus.Stop()

	require.NoError(t, <-runErr)
}

func TestRedisbusSendAndReceiveConcurrently(t *testing.T) {
	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- bus.Run(context.Background())
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 20
	subscribers := make([]pubsub.Subscription[messageEnvelope], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		require.NoError(t, err)
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
			err := subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	bus.Stop()

	require.NoError(t, <-runErr)
}

func TestRedisbusOverSendAndReceiveConcurrently(t *testing.T) {
	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- bus.Run(context.Background())
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 20
	subscribers := make([]pubsub.Subscription[messageEnvelope], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		require.NoError(t, err)
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
			err := subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)

			err = subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)

			err = subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: fmt.Sprintf("hello: %d", i)})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	bus.Stop()

	require.NoError(t, <-runErr)
}

func TestRedisbusSendAndReceiveConcurrentlyThenStop(t *testing.T) {
	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- bus.Run(context.Background())
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 100
	subscribers := make([]pubsub.Subscription[messageEnvelope], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%d", i))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: fmt.Sprintf("hello: %d", i)})
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

	require.NoError(t, <-runErr)
}

func TestRedisbusSendLargeAmount(t *testing.T) {
	// this is a long running test, you may restart redis service while the test
	// is executing

	pool := newRedisTestClient()

	bus, err := New[messageEnvelope](logger.NewLogger(logger.LogLevel_DEBUG), pool)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- bus.Run(context.Background())
	}()

	time.Sleep(1 * time.Second) // wait for run to start

	const subscribersNum = 50
	subscribers := make([]pubsub.Subscription[messageEnvelope], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%03d", i))
		require.NoError(t, err)
	}

	require.Len(t, subscribers, subscribersNum)

	nTickets := subscribersNum * 100 // 100x more messages than subscribers
	tickets := make(chan struct{}, nTickets)

	for i := 0; i < nTickets; i++ {
		tickets <- struct{}{}
	}

	var messages = map[string]bool{}
	var messagesSent, messagesReceived int
	var stoppedSending bool
	var messagesMu sync.Mutex

	var wg sync.WaitGroup

	// receive messages
	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {

				messagesMu.Lock()
				completed := stoppedSending && messagesReceived >= messagesSent
				t.Logf("received: %d, sent: %d", messagesReceived, messagesSent)
				messagesMu.Unlock()

				if completed {
					bus.Stop()
					return
				}

				message, ok := <-subscribers[i].ReadMessage()
				if !ok {
					return
				}

				t.Logf("received: %#v", message.Body)

				messagesMu.Lock()
				value, exists := messages[message.Body]

				assert.True(t, exists) // message was sent
				assert.False(t, value) // message was received once

				messages[message.Body] = true // set as received
				messagesReceived++
				messagesMu.Unlock()
			}
		}(i)
	}

	// send messages
	testUntil := time.Now().Add(time.Second * 60)
	for {
		if time.Now().After(testUntil) {
			t.Logf("stop sending")
			messagesMu.Lock()
			stoppedSending = true
			messagesMu.Unlock()
			break
		}

		for i := 0; i < subscribersNum; i++ {
			go func(i int, ticket struct{}) {
				defer func() {
					tickets <- ticket
				}()

				var message string

				messagesMu.Lock()
				for {
					message = fmt.Sprintf("message: %d", rand.Int63())
					if _, ok := messages[message]; !ok { // make sure message is unique
						break
					}
				}
				messages[message] = false // record message in map
				messagesSent++
				messagesMu.Unlock()

				err := subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: message})
				assert.NoError(t, err)

			}(i, <-tickets)
		}
	}

	require.True(t, stoppedSending)

	wg.Wait()

	require.NoError(t, <-runErr)

	assert.Equal(t, messagesSent, messagesReceived)

	// make sure there are no lost messages
	for _, received := range messages {
		assert.True(t, received)
	}
}
