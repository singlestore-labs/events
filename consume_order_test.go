package events

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/simultaneous"
	"github.com/stretchr/testify/require"
)

func TestOrderedHandlerWaitsForEarlierSequence(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	topic := eventmodels.BindTopic[int]("ordered")
	delivered := make(chan int, 2)
	boundHandler := topic.Handler(func(_ context.Context, event eventmodels.Event[int]) error {
		delivered <- event.Payload
		return nil
	})
	boundHandler.SetLibrary(libraryInterface[eventmodels.BinaryEventID, *NoDBTx, *NoDB]{lib})

	handlers := topicHandlers{handlers: make(map[string]*registeredHandler)}
	handlers.addHandler(
		"ordered",
		eventmodels.OnFailureDiscard,
		&lib.LibraryNoDB,
		boundHandler,
		[]HandlerOpt{WithOrderedDelivery()},
	)
	handler := handlers.handlers["ordered"]
	activeLimiter := simultaneous.New[eventLimiterType](2)
	message := func(value string) *kafka.Message {
		return &kafka.Message{
			Topic:     "ordered",
			Partition: 0,
			Value:     []byte(value),
			Headers: []kafka.Header{{
				Key:   "content-type",
				Value: []byte("application/json"),
			}},
		}
	}

	laterDone := make(chan struct{})
	go func() {
		defer close(laterDone)
		lib.callOrderedHandler(context.Background(), activeLimiter, handler, message("1"), 1, []bool{false})
	}()
	select {
	case value := <-delivered:
		t.Fatalf("later sequence was delivered first: %d", value)
	case <-time.After(20 * time.Millisecond):
	}

	lib.callOrderedHandler(context.Background(), activeLimiter, handler, message("0"), 0, []bool{false})
	require.Equal(t, 0, <-delivered)
	require.Equal(t, 1, <-delivered)
	<-laterDone
}
