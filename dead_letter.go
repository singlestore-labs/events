package events

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
)

// deadLetterWriteBound is the maximum time one Kafka write of a dead-letter
// copy may run. Processed-event trim Margin must exceed this bound plus clock
// skew. The default margin is 6 hours.
const deadLetterWriteBound = 2 * time.Minute

const (
	deadLetterGroupPostfix = "-dead-letter"
	deadLetterTopicPostfix = ".dead-letter"
)

// DeadLetterTopic returns the topic name to use for dead letters. The dead letter
// topics include the consumer group name because otherwise messages could be
// cross-delivered between consumer groups. It consumes and returns un-prefixed topics.
func DeadLetterTopic(unprefixedTopic string, consumerGroup ConsumerGroupName) string {
	return unprefixedTopic + "." + consumerGroup.String() + deadLetterTopicPostfix
}

// startDeadLetterConsumers checks the handlers to see if any of them have onFailure set to
// use dead letter handling and if so creates the dead letter topics and starts dead letter
// consumers.
func (lib *Library[ID, TX, DB]) startDeadLetterConsumers(startupCtx context.Context, baseCtx context.Context, consumerGroup consumerGroupName, originalGroup *group, limiter *limit, allStarted *sync.WaitGroup, groupDone *sync.WaitGroup) {
	preCreate := make([]string, 0, len(originalGroup.topics))
	for topic, topicHandler := range originalGroup.topics {
		var doCreate bool
		for _, handler := range topicHandler.handlers {
			if handler.isDeadLetter {
				continue
			}
			switch handler.onFailure {
			case eventmodels.OnFailureRetryLater, eventmodels.OnFailureSave:
				doCreate = true
			}
		}
		if !doCreate {
			continue
		}
		dlTopic := DeadLetterTopic(topic, consumerGroup)
		preCreate = append(preCreate, dlTopic)
		// pre-configure the dead-letter topic to match the original topic
		if config, ok := lib.getTopicConfig(topic); ok {
			config.Topic = dlTopic
			lib.SetTopicConfig(config)
		}
	}
	if len(preCreate) == 0 {
		return
	}
	// This shouldn't error because the precreate for the non-dead letter versions
	// succeeded before this was called
	err := lib.precreateTopicsForConsuming(startupCtx, consumerGroup, preCreate)
	if err != nil {
		if e := startupCtx.Err(); e == nil {
			lib.logf(startupCtx, "[events] UNEXPECTED ERROR creating topics for dead letter consumption, not consuming dead letter topics: %+v", err)
		}
		return
	}
	var startConsumer bool
	dlGroup := &group{
		topics:  make(map[string]*topicHandlers),
		maxIdle: originalGroup.maxIdle,
	}
	for topic, topicHandler := range originalGroup.topics {
		// iterate in the same order as the original handlers were registered
		var setConfig bool
		for _, handlerName := range topicHandler.handlerNames {
			handler := topicHandler.handlers[handlerName]
			if handler.isDeadLetter {
				continue
			}
			if handler.onFailure != eventmodels.OnFailureRetryLater {
				continue
			}
			startConsumer = true
			dlTopic := DeadLetterTopic(topic, consumerGroup)
			dlTopicHandler, ok := dlGroup.topics[dlTopic]
			if !ok {
				dlTopicHandler = &topicHandlers{
					handlers: make(map[string]*registeredHandler),
				}
				dlGroup.topics[dlTopic] = dlTopicHandler
			}
			dlTopicHandler.addHandler(handlerName, eventmodels.OnFailureBlock, &lib.LibraryNoDB, handler.handler, []HandlerOpt{WithRetrying(true), IsDeadLetterHandler(true), WithQueueDepthLimit(maximumDeadLetterOutstanding)})
			setConfig = true
		}
		if setConfig {
			topicConfig, ok := lib.getTopicConfig(topic)
			if ok {
				lib.SetTopicConfig(topicConfig)
			} else if lib.mustRegisterTopics {
				panic(errors.Alertf("unexpected missing topic config for topic (%s)", topic))
			}
			topicConfig.Topic = DeadLetterTopic(topic, consumerGroup)
		}
	}
	if startConsumer {
		if debugConsumeStartup {
			lib.logf(startupCtx, "[events] Debug: consume startwait +1 for %s", consumerGroup+deadLetterGroupPostfix)
		}
		allStarted.Add(1)
		if debugShutdown {
			lib.logf(startupCtx, "[events] Debug shutdown: allDone/groupDone +1 for %s", consumerGroup+deadLetterGroupPostfix)
		}
		groupDone.Add(1)
		go lib.startConsumingGroup(startupCtx, baseCtx, consumerGroup+deadLetterGroupPostfix, dlGroup, limiter, false, allStarted, groupDone, true, nil, nil, nil)
	}
}

// writeDeadLetterMessages is the Kafka write used by produceToDeadLetter. Tests replace it.
var writeDeadLetterMessages = func(w *kafka.Writer, ctx context.Context, msgs ...kafka.Message) error {
	if w == nil {
		return errors.Errorf("kafka writer is not started")
	}
	return w.WriteMessages(ctx, msgs...)
}

func (lib *Library[ID, TX, DB]) produceToDeadLetter(ctx context.Context, consumerGroup consumerGroupName, handlerName string, msg kafka.Message) error {
	originalTopic := msg.Topic
	baseTopic := lib.removePrefix(originalTopic)
	source, id, hasIdentity := stableEventIdentity(msg)
	if !hasIdentity {
		lib.logf(ctx, "[events] dead-letter copy of %s has no stable ce_id and ce_source; exactly-once trim protection does not apply", originalTopic)
	}
	msg.Topic = lib.addPrefix(DeadLetterTopic(baseTopic, consumerGroup))

	// touch advances lastSeenAt before the write so a trim cannot drop the row
	// while the copy is in flight, and again after so a row committed during the
	// write is included. A missing row is left for the other call.
	touch := func() error {
		if !hasIdentity {
			return nil
		}
		readers := lib.readers[consumerGroup]
		if readers == nil {
			return nil
		}
		topicHandler := readers.topics[baseTopic]
		if topicHandler == nil {
			return nil
		}
		names := make([]string, 0, len(topicHandler.handlerNames))
		for _, name := range topicHandler.handlerNames {
			handler := topicHandler.handlers[name]
			if handler.exactlyOnce && !handler.isDeadLetter {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			return nil
		}
		toucher, ok := any(lib.db).(eventmodels.CanTouchEventProcessed)
		if !ok {
			return errors.Errorf("database cannot touch eventsProcessed for topic %s", baseTopic)
		}
		for _, name := range names {
			if err := toucher.TouchEventProcessed(ctx, baseTopic, source, id, name); err != nil {
				return err
			}
		}
		return nil
	}

	b := backoffPolicy.Start(ctx)
	var failures int
	// stop reports the failure and reports whether the retry budget is exhausted.
	stop := func(err error, format string) bool {
		failures++
		_ = lib.RecordErrorNoWait(ctx, "produceEvents", errors.Errorf(format, baseTopic, failures, err))
		return ctx.Err() != nil || !backoff.Continue(b)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := touch(); err != nil {
			if stop(err, "cannot touch eventsProcessed before dead-letter copy of %s (%d failures): %w") {
				return err
			}
			continue
		}
		writeCtx, cancel := context.WithTimeout(ctx, deadLetterWriteBound)
		err := writeDeadLetterMessages(lib.writer, writeCtx, msg)
		cancel()
		if err != nil {
			if stop(err, "cannot produce dead letter message for %s (%d failures) to Kafka: %w") {
				return err
			}
			continue
		}
		if err := touch(); err != nil {
			if stop(err, "cannot touch eventsProcessed after dead-letter copy of %s (%d failures): %w") {
				return err
			}
			continue
		}
		lib.logf(ctx, "[events] produced dead letter message (%s/%s) to Kafka", msg.Topic, string(msg.Key))
		DeadLetterProduceCounts.WithLabelValues(handlerName, originalTopic).Inc()
		return nil
	}
}

func stableEventIdentity(msg kafka.Message) (source, id string, ok bool) {
	var contentType string
	for _, header := range msg.Headers {
		switch header.Key {
		case "ce_source":
			if source == "" {
				source = string(header.Value)
			}
		case "ce_id":
			if id == "" {
				id = string(header.Value)
			}
		case "content-type":
			contentType = string(header.Value)
		}
	}
	if source != "" && id != "" {
		return source, id, true
	}
	if strings.Contains(contentType, "cloudevents+json") && len(msg.Value) > 0 {
		var body struct {
			Source string `json:"source"`
			ID     string `json:"id"`
		}
		if err := json.Unmarshal(msg.Value, &body); err == nil {
			if source == "" {
				source = body.Source
			}
			if id == "" {
				id = body.ID
			}
		}
	}
	if source == "" || id == "" {
		return "", "", false
	}
	return source, id, true
}
