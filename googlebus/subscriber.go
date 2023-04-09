package googlebus

import (
	"context"
	"sync"
	"time"

	"github.com/goware/pubsub"
)

var _ pubsub.Subscription[*Message] = &subscriber{}

type subscriber struct {
	pubsub          pubsub.PubSub[*Message]
	channelID       string
	ch              chan *Message
	done            chan struct{}
	err             error
	unsubscribe     func()
	unsubscribeOnce sync.Once
}

// ChannelID is the topic ID for the subscriber.
func (s *subscriber) ChannelID() string {
	return s.channelID
}

func (s *subscriber) SendMessage(ctx context.Context, message *Message) error {
	return s.pubsub.Publish(ctx, s.channelID, message)
}

func (s *subscriber) ReadMessage() <-chan *Message {
	return s.ch
}

func (s *subscriber) Done() <-chan struct{} {
	return s.done
}

func (s *subscriber) Err() error {
	return s.err
}

func (s *subscriber) Unsubscribe() {
	s.unsubscribeOnce.Do(s.unsubscribe)
}

func BatchMessageReader(sub pubsub.Subscription[*Message], maxMessages int, maxWait time.Duration) <-chan []*Message {
	ch := make(chan []*Message)

	// maxWait minimum is 1 second
	if maxWait < time.Second {
		maxWait = time.Second
	}

	// batch message reader
	go func() {
		defer close(ch)

		var msgs []*Message

		for {
			select {

			case <-sub.Done():
				return

			case msg, ok := <-sub.ReadMessage():
				if !ok {
					return
				}

				msgs = append(msgs, msg)

				if len(msgs) == maxMessages {
					ch <- msgs
					msgs = msgs[:0]
				}

			case <-time.After(maxWait):
				if len(msgs) > 0 {
					ch <- msgs
					msgs = msgs[:0]
				}
			}
		}
	}()

	return ch
}
