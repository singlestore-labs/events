package events

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/backoff/v2"

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
	logs := make(chan string, 20)
	tracer := func(context.Context) eventmodels.Tracer {
		return func(format string, a ...any) {
			logs <- fmt.Sprintf(format, a...)
		}
	}
	lib.Configure(nil, tracer, false, nil, nil, []string{"${NODE_IP}:30992"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		lib.listAvailableTopics(context.Background())
	}()

	requireTopicListingLog(t, logs, "starting over on listing topics")
	requireTopicListingLog(t, logs, "waiting before making another attempt to list topics")
	select {
	case log := <-logs:
		if strings.Contains(log, "starting over on listing topics") {
			t.Fatalf("unexpected listing attempt before backoff allowed it: %s", log)
		}
	default:
	}

	close(controller.done)
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

func requireTopicListingLog(t *testing.T, logs <-chan string, contains string) string {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case log := <-logs:
			if strings.Contains(log, contains) {
				return log
			}
		case <-deadline:
			t.Fatalf("timed out waiting for log containing %q", contains)
			return ""
		}
	}
}
