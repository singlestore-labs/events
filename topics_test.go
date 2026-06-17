package events

import (
	"context"
	"testing"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"github.com/singlestore-labs/events/eventmodels"
)

func TestTopicListingRetryWaitsBeforeTryingAgain(t *testing.T) {
	origBackoffPolicy := topicListingBackoffPolicy
	topicListingBackoffPolicy = backoff.Exponential(
		backoff.WithMinInterval(50*time.Millisecond),
		backoff.WithMaxInterval(50*time.Millisecond),
		backoff.WithJitterFactor(0),
		backoff.WithMaxRetries(0),
	)
	defer func() {
		topicListingBackoffPolicy = origBackoffPolicy
	}()

	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.Configure(nil, nil, false, nil, nil, []string{"${NODE_IP}:30992"})

	var attempts []time.Time
	lib.listAvailableTopicsWith(context.Background(), func(context.Context, string) ([]kafka.Partition, bool) {
		attempts = append(attempts, time.Now())
		if len(attempts) == 1 {
			return nil, false
		}
		return []kafka.Partition{{Topic: "existing-topic"}}, true
	})

	assert.Len(t, attempts, 2)
	assert.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 45*time.Millisecond)
}
