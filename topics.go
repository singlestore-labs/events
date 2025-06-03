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

// UnregisteredTopicError is the base error when attempting to create a
// topic that isn't pre-preregistered when pre-registration is required.
const UnregisteredTopicError errors.String = "topic is not pre-registered"

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

// ValidateTopics will be fast whenever it can be fast. Sometimes it will
// wait for topics to be listed. ValidateTopics topics can only be used after Configure.
func (lib *Library[ID, TX, DB]) ValidateTopics(ctx context.Context, topics []string) error {
	err := lib.start("validate topics")
	if err != nil {
		return err
	}
	if !lib.mustRegisterTopics {
		return nil
	}
Topics:
	for _, topic := range topics {
		if _, ok := lib.getTopicConfig(topic); ok {
			continue
		}
		if topic == heartbeatTopic.Topic() {
			continue
		}
		if err := lib.waitForTopicsListing(ctx); err != nil {
			return err
		}
		seen, ok := lib.topicsSeen.Load(topic)
		if !ok {
			return errors.Errorf("topic (%s) is invalid", topic)
		}
		select {
		case <-seen.created:
			continue Topics
		default:
		}
		return errors.Errorf("topic (%s) is invalid, or at least not created yet", topic)
	}
	// not reachable
	return nil
}

// precreateTopicsForConsuming makes sure that the topics to be consumed exist.
// It blocks until they do.
func (lib *LibraryNoDB) precreateTopicsForConsuming(ctx context.Context, consumerGroup consumerGroupName, topics []string) error {
	if debugConsumeStartup {
		lib.tracer.Logf("[events] Debug: pre-creating topics: %v", topics)
	}
	var priorError bool
	b := backoffPolicy.Start(ctx)
	for {
		err := lib.CreateTopics(ctx, "consume with "+consumerGroup.String(), topics)
		if err == nil {
			if priorError {
				lib.tracer.Logf("[events] prior error creating topics %v, preventing starting of consumer group %s, has cleared up", topics, consumerGroup)
			}
			return nil
		}
		priorError = true
		_ = lib.RecordErrorNoWait("preCreateTopicsForConsume", err)
		if errors.Is(err, UnregisteredTopicError) {
			return err
		}
		if !backoff.Continue(b) {
			return err
		}
	}
}

func (lib *LibraryNoDB) listAvailableTopics() {
	defer close(lib.topicsHaveBeenListed)
	dialer := lib.dialer()
	for {
		lib.tracer.Logf("[events] starting over on listing topics")
		for _, broker := range lib.brokers {
			lib.tracer.Logf("[events] connecting to %s to list topics", broker)
			conn, err := dialer.Dial("tcp", broker)
			if err != nil {
				lib.tracer.Logf("[events] could not connect to broker %s, was going to list topics: %v", broker, err)
				continue
			}
			defer func() {
				_ = conn.Close()
			}()
			partitions, err := conn.ReadPartitions()
			if err != nil {
				lib.tracer.Logf("[events] could not list partitions on broker %s: %v", broker, err)
				continue
			}
			lib.tracer.Logf("[events] listing existing topics...")
			seen := make(map[string]bool)
			for _, p := range partitions {
				if seen[p.Topic] {
					continue
				}
				seen[p.Topic] = true
				lib.tracer.Logf("[events] topic %s found in partition", p.Topic)
				ct := creatingTopic{
					created: make(chan struct{}),
				}
				ct.state.Store(1)
				close(ct.created)
				_, _ = lib.topicsSeen.LoadOrStore(p.Topic, &ct)
			}
			lib.tracer.Logf("[events] done listing existing topics")
			return
		}
		lib.tracer.Logf("[events] waiting before making another attempt to list topics")
	}
}

func (lib *LibraryNoDB) waitForTopicsListing(ctx context.Context) error {
	lib.topicListingStarted.Do(func() {
		lib.listAvailableTopics()
	})
	select {
	case <-lib.topicsHaveBeenListed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CreateTopics orechestrates the creation of topics that have not already been successfully
// created. The set of created topics is in lib.topicsSeen. It is expected that createTopics
// will be called simultaneously from multiple threads. Its behavior is optimized to do
// minimal work and to return almost instantly if there are no topics that need creating.
func (lib *LibraryNoDB) CreateTopics(ctx context.Context, why string, topics []string) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, topicCreationDeadline)
	defer cancelTimeout()
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
	if err := lib.waitForTopicsListing(ctx); err != nil {
		return err
	}
	var firstWorkFound bool // no point in logging much until we know there is work to do
	var outstanding map[string]*creatingTopic
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
		if !firstWorkFound {
			lib.tracer.Logf("done waiting for topic listing to complete")
			firstWorkFound = true
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
			if lib.mustRegisterTopics && !ok && topic != heartbeatTopic.Topic() {
				lib.tracer.Logf("[events] %s: requested topic, %s, not pre-registered", why, topic)
				overrideFinalError = UnregisteredTopicError.Errorf("event library attempt to create topic (%s) that was not pre-registered (%s)", topic, why)
				now := time.Now()
				func() {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					// must set error before setting state to topicCreateFailed
					seen.error = overrideFinalError
					seen.errorTime = now
					seen.state.Store(topicCreateFailed)
					close(seen.created)
				}()
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
			var ctr kafka.CreateTopicsRequest
			ctr.Topics = append(ctr.Topics, tc)
			lib.tracer.Logf("[events] %s: attempting creation of topic %s with replicas %d and min.insync %d", why, topic, tc.ReplicationFactor, mir)
			go func() {
				// This go-routine actually tries to create the topic. It makes one attempt.
				// If the attempt succeeds, it marks topics as created. A TopicAlreadyExists
				// error counts as success.
				client, err := lib.getController(ctx)
				if err == nil {
					lib.tracer.Logf("[events] %s: making topic creation request for %v", why, topic)
					var resp *kafka.CreateTopicsResponse
					resp, err = client.CreateTopics(ctx, &ctr)
					if err == nil {
						err = resp.Errors[topic]
						switch {
						case err == nil:
							lib.tracer.Logf("[events] %s: topic %s no error when creating", why, topic)
						case errors.Is(err, kafka.TopicAlreadyExists):
							lib.tracer.Logf("[events] %s: topic %s already exists", why, topic)
							err = nil
						default:
							// uh, oh. Handled later
						}
						for tpc, topicErr := range resp.Errors {
							if tpc != topic {
								lib.tracer.Logf("[event] recevied create topic response for topic (%s) not in request (%s %s): %s", tpc, why, topic, topicErr)
							}
						}
					}
					if resp.Throttle != 0 {
						lib.tracer.Logf("[events] %s: topic creation request was throttled for %s", why, resp.Throttle)
					}
				}
				now := time.Now()
				// This function exists so that the lock is held for a minimal time
				created := func() bool {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					if err != nil {
						// must set error before setting state to topicCreateFailed
						seen.error = errors.WithStack(err)
						seen.errorTime = now
						seen.state.Store(topicCreateFailed)
						close(seen.created)
					} else if seen.state.Load() == topicCreating {
						seen.state.Store(topicCreated)
						close(seen.created)
						return true
					}
					return false
				}()
				if created {
					lib.tracer.Logf("[events] %s: topic %s should now exist", why, topic)
				}
				if err != nil {
					alert := errors.Alertf("event library error creating topic (%s) (%s): %w", topic, why, err)
					lib.tracer.Logf("[events] %+v", alert)
				}
			}()
		}
		if outstanding == nil {
			outstanding = make(map[string]*creatingTopic)
		}
		lib.tracer.Logf("[events] %s: will wait for creation attempt of topic %s to complete", why, topic)
		outstanding[topic] = seen
	}
	if len(outstanding) == 0 || overrideFinalError != nil {
		return overrideFinalError
	}
	lib.tracer.Logf("[events] waiting for topic creation to complete")
	for topic, seen := range outstanding {
		select {
		case <-seen.created:
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
		case <-ctx.Done():
			return errors.Errorf("event library could not create kafka topic (%s) (%s): %w", topic, why, ErrTopicCreationTimeout)
		}
	}
	return nil
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
