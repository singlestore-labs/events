package events

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
)

// This file handles the creation of topics. Topic creation is done on-the-fly as
// messages are sent or consumers are started. The topic configuration can be
// overridden before the topic is created. It is expected that the same topic can
// be requested to be created from multiple go routines at once. Only one go routine
// will actually create the topic. All other will wait for the one that is doing the
// work to complete.

const (
	// state=topicCreating means that the topic is not yet fully created but
	// the creation is in progress. The default state is topicCreating.
	topicCreating = 0
	// state=topicCreated means that the topic has been created
	topicCreated = 1
	// state=topicCreateFailed means that the last attempt to create the topic failed
	// and we are not yet retrying
	topicCreateFailed = 2
)

// creatingTopic is a per-topic structure that is optimized for the
// fastpath of the topic already created: state==1.
//
// The state of creatingTopic
//
//	created			state=topicCreated
//	not yet tried		not present in map, created not closed
//	trying			state=topicCreating, created not closed
//	just created		state=topicCreating, created closed
//	errored, too soon	state=topicCreateFailed, error set to not-nil, errorTime < retry time, created closed
//	errored, time to try	state=topicCreateFailed, error set to not-nil, errorTime > retry time, created closed
//	errored, retrying	state=topicCreating, created not closed
//
// lock is held when changing state from topicCreateFailed to topicCreating so that only one thread makes that change
type creatingTopic struct {
	lock      sync.Mutex
	state     atomic.Int32  // 0=processing 1=done 2=error
	created   chan struct{} // closed when when request is finished
	error     error
	errorTime time.Time
}

const (
	topicCreateSleepTime     = time.Second
	defaultNumPartitions     = 2
	defaultReplicationFactor = 3
	topicCreationDeadline    = time.Second * 30
)

// SetTopicConfig can be used to override the configuration parameters
// for new topics. If no override has been set, then the default configuration
// for new topics is simply: 2 partitions. High volume topics should use 10
// or even 20 partitions.
//
// Topics will be auto-created when a message is sent. Topics will be auto-created
// on startup for all topics that are consumed.
func (lib *LibraryNoDB) SetTopicConfig(topicConfig kafka.TopicConfig) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	if topicConfig.Topic == "" {
		panic(errors.Alertf("attempt to register event library topic configuration with an empty topic name"))
	}
	lib.topicConfig[topicConfig.Topic] = topicConfig
}

func (lib *LibraryNoDB) getTopicConfig(topic string) (kafka.TopicConfig, bool) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	c, ok := lib.topicConfig[topic]
	return c, ok
}

func (lib *LibraryNoDB) createTopicsForOutgoingEvents(ctx context.Context, events []eventmodels.ProducingEvent) error {
	if len(events) == 0 {
		return nil
	}
	topicSet := make(map[string]struct{})
	for _, event := range events {
		topicSet[event.GetTopic()] = struct{}{}
	}
	return lib.CreateTopics(ctx, fmt.Sprintf("produce %d (eg %s %s)", len(events), events[0].GetTopic(), events[0].GetKey()), generic.Keys(topicSet))
}

// precreateTopicsForConsuming makes sure that the topics to be consumed exist.
// It blocks until they do.
func (lib *LibraryNoDB) precreateTopicsForConsuming(ctx context.Context, consumerGroup consumerGroupName, topics []string) {
	var priorError bool
	b := backoffPolicy.Start(ctx)
	for {
		err := lib.CreateTopics(ctx, "consume with "+consumerGroup.String(), topics)
		if err == nil {
			if priorError {
				lib.tracer.Logf("[events] prior error creating topics %v, preventing starting of consumer group %s, has cleared up", topics, consumerGroup)
			}
			return
		}
		priorError = true
		_ = errors.Alertf("could not create topics needed to consume group (%s): %w", consumerGroup, err)
		if !backoff.Continue(b) {
			return
		}
	}
}

// CreateTopics orechestrates the creation of topics that have not already been successfully
// created. The set of created topics is in lib.topicsSeen. It is expected that createTopics
// will be called simultaneously from multiple threads. Its behavior is optimized to do
// minimal work and to return almost instantly if there are no topics that need creating.
func (lib *LibraryNoDB) CreateTopics(ctx context.Context, why string, topics []string) error {
	if lib.ready.Load() == isNotConfigured {
		err := errors.Alertf("attempt to create topics before library configuration (%s)", why)
		lib.tracer.Logf("[events] %s: %+v", why, err)
		panic(err)
	}
	for _, topic := range topics {
		if topic == "" {
			err := errors.Errorf("cannot create an empty topic (%s) in event library", why)
			lib.tracer.Logf("[events] %s: %+v", why, err)
			return err
		}
	}
	var ctr kafka.CreateTopicsRequest
	var outstanding map[string]*creatingTopic
	var topicRequest []string
	var overrideFinalError error
	for _, topic := range topics {
		seen, ok := lib.topicsSeen.Load(topic)
		if !ok {
			seen, ok = lib.topicsSeen.LoadOrStore(topic,
				&creatingTopic{
					created: make(chan struct{}),
					// state: topicCreating
				})
		}
		state := seen.state.Load()
		if state == topicCreated {
			continue
		}
		doCreate := !ok
		if state == topicCreateFailed {
			var err error
			doCreate, err = lib.prepareTopicCreateRetry(seen)
			if err != nil {
				err := errors.Errorf("event library topic (%s) creation failed (%s): %w", topic, why, err)
				lib.tracer.Logf("[events] %s: %+v", why, err)
				return err
			}
			if doCreate {
				lib.tracer.Logf("[events] %s: will re-attempt creation of topic %s, previous attempt failed", why, topic)
			} else {
				lib.tracer.Logf("[events] %s: will NOT re-attempt creation of topic %s yet, previous attempt failed", why, topic)
			}
		}
		// doCreate is true if either no other thread was already working on the topic (ok == false) or
		// if it has previously failed and enough time has passed that it's time to try creating it again.
		// In either case, state will already be topicCreating.
		if doCreate {
			tc, ok := lib.getTopicConfig(topic)
			if lib.mustRegisterTopics && !ok {
				lib.tracer.Logf("[events] %s: requested topic, %s, not pre-registered", why, topic)
				overrideFinalError = errors.Errorf("event library attempt to create topic (%s) that was not pre-registered (%s)", topic, why)
				continue
			}
			tc.Topic = topic
			if tc.NumPartitions == 0 {
				tc.NumPartitions = defaultNumPartitions
			}
			if tc.ReplicationFactor == 0 {
				tc.ReplicationFactor = defaultReplicationFactor
			}
			if tc.ReplicationFactor > len(lib.brokers) {
				tc.ReplicationFactor = len(lib.brokers)
			}
			mir := getIntConfigValue(tc, "min.insync.replicas")
			if mir <= 0 || mir >= int64(tc.ReplicationFactor) {
				mir = int64(tc.ReplicationFactor) - 1
				if mir == 0 {
					mir = 1
				}
				tc.ConfigEntries = setIntConfigValue(tc, "min.insync.replicas", mir)
			}

			mir = getIntConfigValue(tc, "min.insync.replicas")
			ctr.Topics = append(ctr.Topics, tc)
			lib.tracer.Logf("[events] %s: attempting creation of topic %s with replicas %d and min.insync %d", why, topic, tc.ReplicationFactor, mir)
			topicRequest = append(topicRequest, topic)
		}
		if outstanding == nil {
			outstanding = make(map[string]*creatingTopic)
		}
		lib.tracer.Logf("[events] %s: will wait for creation attempt of topic %s to complete", why, topic)
		outstanding[topic] = seen
	}
	if len(outstanding) == 0 {
		return overrideFinalError
	}
	// If ctr.Topics is empty, then other threads are making the topic creation request.
	if len(ctr.Topics) != 0 {
		setErrors := func(err error) {
			// setErrors is used when there is an error affecting all
			// of the topics we're requesting to be created.
			err = errors.WithStack(err)
			lib.tracer.Logf("[events] %s: create topics %v request failed: %+v", why, topicRequest, err)
			now := time.Now()
			for _, topicConfig := range ctr.Topics {
				seen := outstanding[topicConfig.Topic]
				func() {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					// must set error before setting state to topicCreateFailed
					seen.error = err
					seen.errorTime = now
					seen.state.Store(topicCreateFailed)
					close(seen.created)
				}()
			}
		}
		go func() {
			// This go-routine actually tries to create the topics. It makes one attempt.
			// If the attempt succeeds, it marks topics as created. A TopicAlreadyExists
			// error counts as success.
			client, err := lib.getController(ctx)
			if err != nil {
				setErrors(err)
				return
			}
			lib.tracer.Logf("[events] %s: making topic creation request for %v", why, topics)
			resp, err := client.CreateTopics(ctx, &ctr)
			if err != nil {
				setErrors(errors.Errorf("event library create topics (%s): %w", why, err))
				return
			}
			if resp.Throttle != 0 {
				lib.tracer.Logf("[events] %s: topic creation request was throttled for %s", why, resp.Throttle)
			}
			now := time.Now()
			for topic, err := range resp.Errors {
				if err == nil {
					continue
				}
				if errors.Is(err, kafka.TopicAlreadyExists) {
					lib.tracer.Logf("[events] %s: topic %s already exists", why, topic)
					continue
				}
				seen, ok := outstanding[topic]
				if !ok {
					_ = errors.Alertf("event library recevied create topic response for topic (%s) not in request (%s): %w", topic, why, err)
					continue
				}
				alert := errors.Alertf("event library error creating topic (%s) (%s): %w", topic, why, err)
				lib.tracer.Logf("[events] %+v", alert)
				func() {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					// must set error before setting state to topicCreateFailed
					seen.error = errors.WithStack(err)
					seen.errorTime = now
					seen.state.Store(topicCreateFailed)
					close(seen.created)
				}()
			}
			var succeeded int
			for _, topicConfig := range ctr.Topics {
				seen, ok := outstanding[topicConfig.Topic]
				if !ok {
					err := errors.Alertf("event library internal error creating topic (%s) (%s)", topicConfig.Topic, why)
					lib.tracer.Logf("[events] %+v", err)
					continue
				}
				if func() bool {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					if seen.state.Load() == topicCreating {
						seen.state.Store(topicCreated)
						close(seen.created)
						return true
					}
					return false
				}() {
					lib.tracer.Logf("[events] %s: topic %s should now exist", why, topicConfig.Topic)
					succeeded++
				}
			}
			lib.tracer.Logf("[events] %s: topic creation attempt for %v is complete (%d of %d succeeded)", why, topics, succeeded, len(ctr.Topics))
		}()
	}
	start := time.Now()
	if len(outstanding) == 0 {
		return overrideFinalError
	}
	for topic, seen := range outstanding {
		waiting := time.Since(start)
		if waiting >= topicCreationDeadline {
			return errors.Errorf("event library could not create kafka topic (%s) (%s): %w", topic, why, ErrTopicCreationTimeout)
		}
		timer := time.NewTimer(topicCreationDeadline - waiting)
		var err error
		select {
		case <-seen.created:
			timer.Stop()
			state := seen.state.Load()
			if state == topicCreateFailed {
				err := func() error {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					if seen.state.Load() == topicCreateFailed {
						return seen.error
					}
					return nil
				}()
				if err != nil {
					err = errors.Alertf("event library could not create kafka topic (%s) (%s): %w", topic, why, err)
					lib.tracer.Logf("[events] %+v", err)
					return err
				}
			}
		case <-timer.C:
			err = errors.Alertf("event library could not create kafka topic (%s) before timeout (%s): %w", topic, why, ErrTopicCreationTimeout)
		case <-ctx.Done():
			err = errors.Errorf("event library could not create kafka topic (%s) (%s): %w", topic, why, ErrTopicCreationTimeout)
		}
		if err != nil {
			lib.tracer.Logf("[events] %+v", err)
			return err
		}
	}
	return overrideFinalError
}

var ErrTopicCreationTimeout errors.String = "event library topic creation deadline exceeded"

// prepareTopicCreateRetry returns true when the last attempt to create the topic
// resulted in an error and enough time has gone by that it's okay to
// try again.
func (lib *LibraryNoDB) prepareTopicCreateRetry(seen *creatingTopic) (bool, error) {
	seen.lock.Lock()
	defer seen.lock.Unlock()
	if seen.state.Load() != topicCreateFailed {
		return false, nil
	}
	if time.Since(seen.errorTime) < topicCreateSleepTime {
		return false, seen.error
	}
	seen.created = make(chan struct{})
	seen.state.Store(topicCreating)
	seen.errorTime = time.Time{}
	seen.error = nil
	return true, nil
}

func getIntConfigValue(tc kafka.TopicConfig, configName string) int64 {
	i := generic.FirstMatchIndex(tc.ConfigEntries, func(e kafka.ConfigEntry) bool { return e.ConfigName == configName })
	if i >= 0 {
		v, _ := strconv.ParseInt(tc.ConfigEntries[i].ConfigValue, 10, 64)
		return v
	}
	return 0
}

func setIntConfigValue(tc kafka.TopicConfig, configName string, value int64) []kafka.ConfigEntry {
	return generic.ReplaceOrAppend(tc.ConfigEntries, kafka.ConfigEntry{
		ConfigName:  configName,
		ConfigValue: strconv.FormatInt(value, 10),
	}, func(e kafka.ConfigEntry) bool { return e.ConfigName == configName })
}
