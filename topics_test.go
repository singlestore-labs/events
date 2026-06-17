package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/singlestore-labs/events/eventmodels"
)

func TestTopicListingRetryWaitsAndCanRetryAfterContextCancel(t *testing.T) {
	var lock sync.Mutex
	var logs []string
	tracer := func(context.Context) eventmodels.Tracer {
		return func(format string, a ...any) {
			lock.Lock()
			defer lock.Unlock()
			logs = append(logs, fmt.Sprintf(format, a...))
		}
	}

	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.Configure(nil, tracer, false, nil, nil, []string{"${NODE_IP}:30992"})

	for attempt := 1; attempt <= 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := lib.waitForTopicsListing(ctx)
		cancel()
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		lock.Lock()
		starts := countLogsContaining(logs, "starting over on listing topics")
		lock.Unlock()
		assert.Equal(t, attempt, starts)
	}

	select {
	case <-lib.topicsHaveBeenListed:
		t.Fatal("topics should not be marked listed after failed listing attempts")
	default:
	}
}

func countLogsContaining(logs []string, needle string) int {
	var count int
	for _, line := range logs {
		if strings.Contains(line, needle) {
			count++
		}
	}
	return count
}
