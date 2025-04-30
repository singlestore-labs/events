package events

import (
	"context"
	"sync"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
)

const (
	deadLetterGroupPostfix = "-dead-letter"
	deadLetterTopicPostfix = ".dead-letter"
)

// DeadLetterTopic returns the topic name to use for dead letters. The dead letter
// topics include the consumer group name because otherwise messages could be
// cross-delivered between consumer groups.
func DeadLetterTopic(topic string, consumerGroup ConsumerGroupName) string {
	return topic + "." + consumerGroup.String() + deadLetterTopicPostfix
}

// startDeadLetterConsumers checks the handlers to see if any of them have onFailure set to
// use dead letter handling and if so creates the dead letter topics and starts dead letter
// consumers.
func (lib *Library[ID, TX, DB]) startDeadLetterConsumers(ctx context.Context, consumerGroup consumerGroupName, originalGroup *group, limiter *limit, allStarted *sync.WaitGroup, allDone *sync.WaitGroup) {
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
	err := lib.precreateTopicsForConsuming(ctx, consumerGroup, preCreate)
	if err != nil {
		lib.tracer.Logf("[events] UNEXPECTED ERROR creating topics for dead letter consumption: %+v", err)
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
		allStarted.Add(1)
		allDone.Add(1)
		if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: consume startwait +1 for %s", consumerGroup+deadLetterGroupPostfix)
		}
		go lib.startConsumingGroup(ctx, consumerGroup+deadLetterGroupPostfix, dlGroup, limiter, false, allStarted, allDone, true)
	}
}

func (lib *Library[ID, TX, DB]) produceToDeadLetter(ctx context.Context, consumerGroup consumerGroupName, handlerName string, msg kafka.Message) {
	originalTopic := msg.Topic
	msg.Topic = DeadLetterTopic(msg.Topic, consumerGroup)
	b := backoffPolicy.Start(ctx)
	var failures int
	for {
		err := lib.writer.WriteMessages(ctx, msg)
		if err == nil {
			if failures > 0 {
				lib.tracer.Logf("[events] finally produced dead letter message (%s/%s) to Kafka after %d failure(s)", msg.Topic, string(msg.Key), failures)
			} else {
				lib.tracer.Logf("[events] produced dead letter message (%s/%s) to Kafka", msg.Topic, string(msg.Key))
			}
			DeadLetterProduceCounts.WithLabelValues(handlerName, originalTopic).Inc()
			return
		}
		failures++
		_ = lib.RecordErrorNoWait("produceEvents", errors.Errorf("cannot produce dead letter message (%s/%s, %d failures) to Kafka: %w", msg.Topic, string(msg.Key), failures, err))
		if !backoff.Continue(b) {
			return
		}
	}
}
