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
	topicListingBackoffPolicy = newTestTopicListingBackoffPolicy(controller)
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
	lib.Configure(nil, tracer, false, nil, nil, []string{"$$$invalid hostname$$$:1"})

	errCh := make(chan error, 1)
	go func() {
		errCh <- lib.listAvailableTopics(context.Background())
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
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected topic listing to fail when backoff stops before success")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for topic listing to finish")
	}
}

func TestTopicListingContinuesAfterCallerCancel(t *testing.T) {
	firstController := &testTopicListingBackoffController{
		done: make(chan struct{}),
		next: make(chan struct{}, 1),
	}
	firstController.next <- struct{}{}

	origBackoffPolicy := topicListingBackoffPolicy
	topicListingBackoffPolicy = newTestTopicListingBackoffPolicy(firstController)
	defer func() {
		topicListingBackoffPolicy = origBackoffPolicy
	}()

	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	logs := make(chan string, 40)
	tracer := func(context.Context) eventmodels.Tracer {
		return func(format string, a ...any) {
			logs <- fmt.Sprintf(format, a...)
		}
	}
	lib.Configure(nil, tracer, false, nil, nil, []string{"$$$invalid hostname$$$:1"})

	ctx, cancel := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- lib.waitForTopicsListing(ctx)
	}()
	requireTopicListingLog(t, logs, "waiting before making another attempt to list topics")
	cancel()
	requireTopicListingError(t, firstErr)

	select {
	case <-lib.topicsHaveBeenListed:
		t.Fatal("topics should not be marked listed when listing is still running")
	default:
	}

	firstController.next <- struct{}{}
	requireTopicListingLog(t, logs, "starting over on listing topics")
	close(firstController.done)
}

type testTopicListingBackoffPolicy struct {
	controllers chan backoff.Controller
}

func newTestTopicListingBackoffPolicy(controllers ...backoff.Controller) testTopicListingBackoffPolicy {
	ch := make(chan backoff.Controller, len(controllers))
	for _, controller := range controllers {
		ch <- controller
	}
	return testTopicListingBackoffPolicy{
		controllers: ch,
	}
}

func (p testTopicListingBackoffPolicy) Start(context.Context) backoff.Controller {
	return <-p.controllers
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

func requireTopicListingError(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected topic listing error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for topic listing error")
	}
}
