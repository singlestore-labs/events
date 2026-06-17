package events

import (
	"context"
	"testing"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
)

func TestTopicListingRetryWaitsForBackoffBeforeTryingAgain(t *testing.T) {
	controller := &testTopicListingBackoffController{
		done: make(chan struct{}),
		next: make(chan struct{}, 1),
	}
	controller.next <- struct{}{}

	origBackoffPolicy := topicListingBackoffPolicy
	topicListingBackoffPolicy = testTopicListingBackoffPolicy{controller: controller}
	defer func() {
		topicListingBackoffPolicy = origBackoffPolicy
	}()

	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.Configure(nil, nil, false, nil, nil, []string{"${NODE_IP}:30992"})

	attempts := make(chan int, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var attempt int
		lib.listAvailableTopicsWith(context.Background(), func(context.Context, string) ([]kafka.Partition, bool) {
			attempt++
			attempts <- attempt
			if attempt == 1 {
				return nil, false
			}
			return []kafka.Partition{{Topic: "existing-topic"}}, true
		})
	}()

	require.Equal(t, 1, requireTopicListingAttempt(t, attempts))
	select {
	case attempt := <-attempts:
		t.Fatalf("unexpected listing attempt before backoff allowed it: %d", attempt)
	default:
	}

	controller.next <- struct{}{}
	assert.Equal(t, 2, requireTopicListingAttempt(t, attempts))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for topic listing to finish")
	}
}

type testTopicListingBackoffPolicy struct {
	controller backoff.Controller
}

func (p testTopicListingBackoffPolicy) Start(context.Context) backoff.Controller {
	return p.controller
}

type testTopicListingBackoffController struct {
	done chan struct{}
	next chan struct{}
}

func (c *testTopicListingBackoffController) Done() <-chan struct{} {
	return c.done
}

func (c *testTopicListingBackoffController) Next() <-chan struct{} {
	return c.next
}

func requireTopicListingAttempt(t *testing.T, attempts <-chan int) int {
	t.Helper()
	select {
	case attempt := <-attempts:
		return attempt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for topic listing attempt")
		return 0
	}
}
