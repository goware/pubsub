package redisbus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/goware/logger"
	"github.com/goware/pubsub"
	"github.com/redis/go-redis/v9"
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

	{
		count, err := bus.NumSubscribers("peter")
		require.NoError(t, err)
		require.Equal(t, 0, count)
	}

	{
		count, err := bus.NumSubscribers("julia")
		require.NoError(t, err)
		require.Equal(t, 0, count)
	}

	sub1, err := bus.Subscribe(context.Background(), "peter")
	require.NoError(t, err)

	sub2, err := bus.Subscribe(context.Background(), "julia")
	require.NoError(t, err)

	sub3, err := bus.Subscribe(context.Background(), "julia")
	require.NoError(t, err)

	// wait for subscriptions to be registered
	time.Sleep(1 * time.Second)

	{
		count, err := bus.NumSubscribers("peter")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	}

	{
		count, err := bus.NumSubscribers("julia")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		received, ok := <-sub1.ReadMessage()
		assert.Equal(t, "sub1", received.Body)
		assert.True(t, ok)

		_, ok = <-sub1.ReadMessage()
		assert.False(t, ok)

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		received, ok := <-sub2.ReadMessage()
		assert.Equal(t, "sub2", received.Body)
		assert.True(t, ok)

		received, ok = <-sub2.ReadMessage()
		assert.Equal(t, "sub3", received.Body)
		assert.True(t, ok)

		_, ok = <-sub2.ReadMessage()
		assert.False(t, ok)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		received, ok := <-sub3.ReadMessage()
		assert.Equal(t, "sub2", received.Body)
		assert.True(t, ok)

		received, ok = <-sub3.ReadMessage()
		assert.Equal(t, "sub3", received.Body)
		assert.True(t, ok)

		_, ok = <-sub3.ReadMessage()
		assert.False(t, ok)
	}()

	sub1.SendMessage(context.Background(), messageEnvelope{Body: "sub1"})
	sub2.SendMessage(context.Background(), messageEnvelope{Body: "sub2"})
	sub3.SendMessage(context.Background(), messageEnvelope{Body: "sub3"})

	time.Sleep(time.Second)

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

	const subscribersNum = 20
	const messagesPerSubscriber = 10000

	subscribers := make([]pubsub.Subscription[messageEnvelope], subscribersNum)

	for i := 0; i < subscribersNum; i++ {
		subscribers[i], err = bus.Subscribe(context.Background(), fmt.Sprintf("channel-%03d", i))
		require.NoError(t, err)
	}

	require.Len(t, subscribers, subscribersNum)

	nTickets := subscribersNum * 10 // 10x more tickets than subscribers
	tickets := make(chan struct{}, nTickets)

	for i := 0; i < nTickets; i++ {
		tickets <- struct{}{}
	}

	var messages = map[string]bool{}
	var messagesSent, messagesReceived int
	var messagesMu sync.Mutex

	messagesTotal := subscribersNum * messagesPerSubscriber

	var wg sync.WaitGroup

	// receive messages
	for i := 0; i < subscribersNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {

				messagesMu.Lock()
				completed := messagesReceived >= messagesTotal
				t.Logf("received: %d, sent: %d", messagesReceived, messagesSent)
				messagesMu.Unlock()

				if completed {
					bus.Stop()
					return
				}

				message, ok := <-subscribers[i].ReadMessage()
				if !ok {
					t.Logf("exit subscription")
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
	for j := 0; j < messagesPerSubscriber; j++ {
		for i := 0; i < subscribersNum; i++ {
			wg.Add(1)
			go func(i int, ticket struct{}) {
				defer func() {
					tickets <- ticket
					wg.Done()
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
				messagesMu.Unlock()

				err := subscribers[i].SendMessage(context.Background(), messageEnvelope{Body: message})
				require.NoError(t, err)
				if err != nil {

					t.Logf("SendMessage: %v", err)

					messagesMu.Lock()
					delete(messages, message)
					messagesMu.Unlock()

					return
				}

				messagesMu.Lock()
				messagesSent++
				messagesMu.Unlock()

			}(i, <-tickets)
		}
	}

	wg.Wait()

	require.NoError(t, <-runErr)

	assert.LessOrEqual(t, messagesReceived, messagesTotal)
	assert.LessOrEqual(t, messagesSent, messagesTotal)

	assert.Equal(t, messagesSent, messagesReceived)

	// make sure there are no lost messages
	for _, received := range messages {
		assert.True(t, received)
	}
}
