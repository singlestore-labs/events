package events

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/internal/pwork"
)

func TestDeadLetterTopicsComeFromReaders(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.readers[consumerGroupName("billing")] = &group{topics: map[string]*topicHandlers{"orders": {}}}
	lib.readers[consumerGroupName("shipping")] = &group{topics: map[string]*topicHandlers{"orders": {}}}
	lib.readers[consumerGroupName("eu.billing")] = &group{topics: map[string]*topicHandlers{"orders": {}}}
	lib.existingTopics = map[string]struct{}{
		"orders":                        {},
		"orders.eu":                     {},
		"orders.billing.dead-letter":    {},
		"orders.shipping.dead-letter":   {},
		"orders.eu.billing.dead-letter": {},
		"other.billing.dead-letter":     {},
	}

	requireEqualTopics(t, lib.deadLetterTopics("orders"), []string{
		"orders.billing.dead-letter",
		"orders.eu.billing.dead-letter",
		"orders.shipping.dead-letter",
	})
	requireEqualTopics(t, lib.deadLetterTopics("orders.eu"), nil)
	requireEqualTopics(t, lib.deadLetterTopics("other"), nil)
}

func TestDeadLetterStaysOpenWhenTopicHasConfig(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetTopicConfig(kafka.TopicConfig{Topic: "orders"})
	lib.SetTopicConfig(kafka.TopicConfig{Topic: "missing"})
	lib.readers[consumerGroupName("billing")] = &group{topics: map[string]*topicHandlers{
		"orders":  {},
		"missing": {},
	}}
	lib.readers[consumerGroupName("shipping")] = &group{topics: map[string]*topicHandlers{
		"orders": {},
	}}
	lib.markListedTopicsDone(map[string]struct{}{
		"orders":                        {},
		"orders.billing.dead-letter":    {},
		"orders.shipping.dead-letter":   {},
		"orders.eu":                     {},
		"orders.eu.billing.dead-letter": {},
		"plain":                         {},
		"missing.billing.dead-letter":   {},
	})

	requireTopicOpen(t, lib, "orders")
	requireTopicOpen(t, lib, "orders.billing.dead-letter")
	requireTopicOpen(t, lib, "orders.shipping.dead-letter")
	requireTopicOpen(t, lib, "missing.billing.dead-letter")
	requireTopicDone(t, lib, "orders.eu")
	requireTopicDone(t, lib, "orders.eu.billing.dead-letter")
	requireTopicDone(t, lib, "plain")
}

func requireTopicOpen(t *testing.T, lib *Library[eventmodels.BinaryEventID, *NoDBTx, *NoDB], topic string) {
	t.Helper()
	if lib.topicsWork.GetState(topic) == pwork.ItemDone {
		t.Fatalf("topic %s was marked done", topic)
	}
}

func requireTopicDone(t *testing.T, lib *Library[eventmodels.BinaryEventID, *NoDBTx, *NoDB], topic string) {
	t.Helper()
	if lib.topicsWork.GetState(topic) != pwork.ItemDone {
		t.Fatalf("topic %s was not marked done", topic)
	}
}

func requireEqualTopics(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("topics = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("topics = %v, want %v", got, want)
		}
	}
}

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
	lib.Configure(nil, tracer, false, nil, nil, nil)

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
	lib.Configure(nil, tracer, false, nil, nil, nil)

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

func (p testTopicListingBackoffPolicy) Start(ctx context.Context) backoff.Controller {
	return newTestTopicListingBackoffControllerWithContext(ctx, <-p.controllers)
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

type testTopicListingBackoffControllerWithContext struct {
	controller backoff.Controller
	done       chan struct{}
}

func newTestTopicListingBackoffControllerWithContext(ctx context.Context, controller backoff.Controller) testTopicListingBackoffControllerWithContext {
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
		case <-controller.Done():
		}
	}()
	return testTopicListingBackoffControllerWithContext{
		controller: controller,
		done:       done,
	}
}

func (c testTopicListingBackoffControllerWithContext) Done() <-chan struct{} {
	return c.done
}

func (c testTopicListingBackoffControllerWithContext) Next() <-chan struct{} {
	return c.controller.Next()
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
