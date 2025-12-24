package events

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/internal/pwork"
	"github.com/singlestore-labs/generic"
)

type topicsWhy struct {
	why           string
	errorCategory string
}

// This file handles the creation of topics. Topic creation is done on-the-fly as
// messages are sent or consumers are started. The topic configuration can be
// overridden before the topic is created. It is expected that the same topic can
// be requested to be created from multiple go routines at once. Only one go routine
// will actually create the topic. All other will wait for the one that is doing the
// work to complete.

const (
	topicCreateSleepTime        = time.Second
	topicCreationDeadline       = time.Second * 30
	defaultNumPartitions        = 2
	defaultReplicationFactor    = 3
	debugLogTopicsMissingPrefix = false
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

func (lib *LibraryNoDB) getTopicConfig(unprefixedTopic string) (kafka.TopicConfig, bool) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	c, ok := lib.topicConfig[unprefixedTopic]
	return c, ok
}

// ValidateTopics will be fast whenever it can be fast. Sometimes it will
// wait for topics to be listed. ValidateTopics topics can only be used after Configure.
func (lib *Library[ID, TX, DB]) ValidateTopics(ctx context.Context, unprefixedTopics []string) error {
	err := lib.start("validate topics")
	if err != nil {
		return err
	}
	if !lib.mustRegisterTopics {
		return nil
	}
	for _, unprefixedTopic := range unprefixedTopics {
		if _, ok := lib.getTopicConfig(unprefixedTopic); ok {
			continue
		}
		if unprefixedTopic == heartbeatTopic.Topic() {
			continue
		}
		if err := lib.waitForTopicsListing(ctx); err != nil {
			return err
		}
		switch lib.topicsWork.GetState(unprefixedTopic) {
		case pwork.ItemDone:
			continue
		case pwork.ItemDoesNotExist:
			return errors.Errorf("topic (%s) is invalid", unprefixedTopic)
		default:
			return errors.Errorf("topic (%s) is invalid, or at least not created yet", unprefixedTopic)
		}
	}
	return nil
}

func (lib *LibraryNoDB) precreateTopicsForConsuming(ctx context.Context, consumerGroup consumerGroupName, unprefixedTopics []string) error {
	return lib.topicsWork.WorkUntilDone(ctx, unprefixedTopics, topicsWhy{
		why:           "consume with " + consumerGroup.String(),
		errorCategory: "preCreateTopicsForConsume",
	})
}

func (lib *LibraryNoDB) configureTopicsPrework() {
	lib.topicsWork.MaxSimultaneous = 20
	lib.topicsWork.BackoffPolicy = backoffPolicy
	lib.topicsWork.WorkDeadline = topicCreationDeadline
	lib.topicsWork.ItemRetryDelay = 5 * time.Second
	lib.topicsWork.ErrorReporter = func(_ context.Context, err error, why topicsWhy) {
		_ = lib.RecordErrorNoWait(why.errorCategory, err)
	}
	lib.topicsWork.IsFatalError = func(err error) bool {
		return errors.Is(err, UnregisteredTopicError)
	}
	lib.topicsWork.ClearedUp = func(_ context.Context, _ error, why topicsWhy, unprefixedTopics []string) {
		lib.tracer.Logf("[events] prior error creating topics %v, preventing %s, has cleared up", unprefixedTopics, why.why)
	}
	lib.topicsWork.FirstWorkMessage = func(_ context.Context, _ topicsWhy, unprefixedTopic string) {
		lib.tracer.Logf("done waiting for topic listing to complete (%s needs to be created)", unprefixedTopic)
	}
	lib.topicsWork.NotRetryingError = func(ctx context.Context, unprefixedTopic string, why topicsWhy, err error) error {
		err = errors.Errorf("event library topic (%s) creation failed (%s): %w", unprefixedTopic, why.why, err)
		lib.tracer.Logf("[events] %s: %+v", why.why, err)
		return err
	}
	lib.topicsWork.RetryingOrNot = func(ctx context.Context, doCreate bool, unprefixedTopic string, why topicsWhy) {
		if doCreate {
			lib.tracer.Logf("[events] %s: will re-attempt creation of topic %s, previous attempt failed", why.why, unprefixedTopic)
		} else {
			lib.tracer.Logf("[events] %s: will NOT re-attempt creation of topic %s yet, previous attempt failed", why.why, unprefixedTopic)
		}
	}
	lib.topicsWork.ItemPreWork = func(ctx context.Context, unprefixedTopic string, why topicsWhy) error {
		_, ok := lib.getTopicConfig(unprefixedTopic)
		if lib.mustRegisterTopics && !ok && unprefixedTopic != heartbeatTopic.Topic() {
			lib.tracer.Logf("[events] %s: requested topic, %s, not pre-registered", why.why, unprefixedTopic)
			return UnregisteredTopicError.Errorf("event library attempt to create topic (%s) that was not pre-registered (%s)", unprefixedTopic, why.why)
		}
		return nil
	}
	lib.topicsWork.ItemWork = func(ctx context.Context, unprefixedTopic string, why topicsWhy) error {
		tc, _ := lib.getTopicConfig(unprefixedTopic)
		prefixedTopic := lib.addPrefix(unprefixedTopic)
		tc.Topic = prefixedTopic
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
		tsti := generic.FirstMatchIndex(tc.ConfigEntries, func(e kafka.ConfigEntry) bool { return e.ConfigName == "message.timestamp.type" })
		if tsti < 0 {
			tc.ConfigEntries = append(tc.ConfigEntries, kafka.ConfigEntry{
				ConfigName:  "message.timestamp.type",
				ConfigValue: "LogAppendTime",
			})
		}

		mir = getIntConfigValue(tc, "min.insync.replicas")
		var ctr kafka.CreateTopicsRequest
		ctr.Topics = append(ctr.Topics, tc)
		lib.tracer.Logf("[events] %s: attempting creation of topic %s with replicas %d and min.insync %d", why, prefixedTopic, tc.ReplicationFactor, mir)
		client, err := lib.getController(ctx)
		if err == nil {
			lib.tracer.Logf("[events] %s: making topic creation request for %v", why, prefixedTopic)
			var resp *kafka.CreateTopicsResponse
			resp, err = client.CreateTopics(ctx, &ctr)
			if err == nil {
				err = resp.Errors[prefixedTopic]
				switch {
				case err == nil:
					lib.tracer.Logf("[events] %s: topic %s no error when creating", why, prefixedTopic)
				case errors.Is(err, kafka.TopicAlreadyExists):
					lib.tracer.Logf("[events] %s: topic %s already exists", why, prefixedTopic)
					err = nil
				default:
					// uh, oh. Handled later
				}
				for tpc, topicErr := range resp.Errors {
					if tpc != prefixedTopic {
						lib.tracer.Logf("[event] received create topic response for topic (%s) not in request (%s %s): %s", tpc, why, prefixedTopic, topicErr)
					}
				}
			}
			if resp.Throttle != 0 {
				lib.tracer.Logf("[events] %s: topic creation request was throttled for %s", why, resp.Throttle)
			}
		}
		return err
	}
	lib.topicsWork.ItemDone = func(_ context.Context, unprefixedTopic string, why topicsWhy) {
		lib.tracer.Logf("[events] %s: topic %s should now exist", why.why, unprefixedTopic)
	}
	lib.topicsWork.ItemFailed = func(_ context.Context, unprefixedTopic string, why topicsWhy, err error, primary bool) error {
		err = errors.Errorf("event library error creating topic (%s) (%s): %w", unprefixedTopic, why.why, err)
		if primary {
			err = errors.Alert(err)
		}
		lib.tracer.Logf("[events] %+v", err)
		return err
	}
	lib.topicsWork.ItemTimeoutError = func(_ context.Context, unprefixedTopic string, why topicsWhy, _ error) error {
		return errors.Errorf("event library could not create kafka topic (%s) (%s): %w", unprefixedTopic, why.why, ErrTopicCreationTimeout)
	}
	lib.topicsWork.ItemPending = func(_ context.Context, unprefixedTopic string, why topicsWhy) {
		lib.tracer.Logf("[events] %s: will wait for creation attempt of topic %s to complete", why.why, unprefixedTopic)
	}
	lib.topicsWork.PreWork = func(ctx context.Context, why topicsWhy, unprefixedTopics []string) error {
		if lib.ready.Load() == isNotConfigured {
			err := errors.Alertf("attempt to create topics before library configuration (%s)", why.why)
			lib.tracer.Logf("[events] %s: %+v", why, err)
			panic(err)
		}
		for _, unprefixedTopic := range unprefixedTopics {
			if unprefixedTopic == "" {
				err := errors.Errorf("cannot create an empty topic (%s) in event library", why.why)
				lib.tracer.Logf("[events] %s: %+v", why, err)
				return err
			}
		}
		if err := lib.waitForTopicsListing(ctx); err != nil {
			return err
		}
		return nil
	}
}

func (lib *LibraryNoDB) listAvailableTopics() {
	defer close(lib.topicsHaveBeenListed)
	dialer := lib.dialer()
	for {
		lib.tracer.Logf("[events] starting over on listing topics")
		for _, i := range rand.Perm(len(lib.brokers)) {
			broker := lib.brokers[i]
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
				unprefixedTopic := lib.removePrefix(p.Topic)
				if lib.prefix != "" && unprefixedTopic == p.Topic {
					if debugLogTopicsMissingPrefix {
						lib.tracer.Logf("[events] topic %s found in partition, IGNORING (not prefixed)", p.Topic)
					}
					continue
				}
				lib.tracer.Logf("[events] topic %s found in partition", unprefixedTopic)
				lib.topicsWork.SetDone(unprefixedTopic)
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
func (lib *LibraryNoDB) CreateTopics(ctx context.Context, why string, unprefixedTopics []string) error {
	return lib.topicsWork.Work(ctx, unprefixedTopics, topicsWhy{
		why:           why,
		errorCategory: "createTopics",
	})
}

var ErrTopicCreationTimeout errors.String = "event library topic creation deadline exceeded"

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
