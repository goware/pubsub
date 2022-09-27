package membus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/goware/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type message struct {
	Body string
}

func TestMembusUnsubscribe(t *testing.T) {
	bus, err := New[message](logger.NewLogger(logger.LogLevel_DEBUG))
	if err != nil {
		assert.NoError(t, err)
	}

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
