package events

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/lestrrat-go/backoff/v2"
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

var topicListingBackoffPolicy = backoff.Exponential(
	backoff.WithMinInterval(time.Second),
	backoff.WithMaxInterval(time.Second*30),
	backoff.WithJitterFactor(0.05),
	backoff.WithMaxRetries(0),
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
//
// For existing topics, SetTopicConfig can be used to update the ConfigEntries of the topic.
// The same entries are applied to every existing dead-letter topic for that topic.
// NOTE: removing configuration will left the configuration unchanged.
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
	err := lib.start(ctx, "validate topics")
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
	lib.topicsWork.ThreadContext = lib.threadContext
	lib.topicsWork.WorkDeadline = topicCreationDeadline
	lib.topicsWork.ItemRetryDelay = 5 * time.Second
	lib.topicsWork.ErrorReporter = func(ctx context.Context, err error, why topicsWhy) {
		_ = lib.RecordErrorNoWait(ctx, why.errorCategory, err)
	}
	lib.topicsWork.IsFatalError = func(err error) bool {
		return errors.Is(err, UnregisteredTopicError)
	}
	lib.topicsWork.ClearedUp = func(ctx context.Context, _ error, why topicsWhy, unprefixedTopics []string) {
		lib.logf(ctx, "[events] prior error creating topics %v, preventing %s, has cleared up", unprefixedTopics, why.why)
	}
	lib.topicsWork.FirstWorkMessage = func(ctx context.Context, _ topicsWhy, unprefixedTopic string) {
		lib.logf(ctx, "done waiting for topic listing to complete (%s needs to be created)", unprefixedTopic)
	}
	lib.topicsWork.NotRetryingError = func(ctx context.Context, unprefixedTopic string, why topicsWhy, err error) error {
		err = errors.Errorf("event library topic (%s) creation failed (%s): %w", unprefixedTopic, why.why, err)
		lib.logf(ctx, "[events] %s: %+v", why.why, err)
		return err
	}
	lib.topicsWork.RetryingOrNot = func(ctx context.Context, doCreate bool, unprefixedTopic string, why topicsWhy) {
		if doCreate {
			lib.logf(ctx, "[events] %s: will re-attempt creation of topic %s, previous attempt failed", why.why, unprefixedTopic)
		} else {
			lib.logf(ctx, "[events] %s: will NOT re-attempt creation of topic %s yet, previous attempt failed", why.why, unprefixedTopic)
		}
	}
	lib.topicsWork.ItemPreWork = func(ctx context.Context, unprefixedTopic string, why topicsWhy) error {
		_, ok := lib.getTopicConfig(unprefixedTopic)
		if lib.mustRegisterTopics && !ok && unprefixedTopic != heartbeatTopic.Topic() {
			lib.logf(ctx, "[events] %s: requested topic, %s, not pre-registered", why.why, unprefixedTopic)
			return UnregisteredTopicError.Errorf("event library attempt to create topic (%s) that was not pre-registered (%s)", unprefixedTopic, why.why)
		}
		return nil
	}
	lib.topicsWork.ItemWork = func(ctx context.Context, unprefixedTopic string, why topicsWhy) error {
		client, err := lib.getController(ctx)
		if err != nil {
			return err
		}
		tc, _ := lib.getTopicConfig(unprefixedTopic)
		if _, exists := lib.existingTopics[unprefixedTopic]; exists {
			prefixedTopic := lib.addPrefix(unprefixedTopic)
			lib.logf(ctx, "[events] %s: topic %s already exists, checking config to update", why.why, prefixedTopic)
			if err := alterExistingTopicConfig(ctx, client, prefixedTopic, tc.ConfigEntries); err != nil {
				return err
			}
			return lib.updateDeadLetterTopicConfigs(ctx, client, unprefixedTopic, tc.ConfigEntries)
		}
		// create topic
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
		lib.logf(ctx, "[events] %s: attempting creation of topic %s with replicas %d and min.insync %d", why.why, prefixedTopic, tc.ReplicationFactor, mir)
		if err == nil {
			lib.logf(ctx, "[events] %s: making topic creation request for %v", why.why, prefixedTopic)
			var resp *kafka.CreateTopicsResponse
			resp, err = client.CreateTopics(ctx, &ctr)
			if err == nil {
				err = resp.Errors[prefixedTopic]
				switch {
				case err == nil:
					lib.logf(ctx, "[events] %s: topic %s no error when creating", why.why, prefixedTopic)
				case errors.Is(err, kafka.TopicAlreadyExists):
					lib.logf(ctx, "[events] %s: topic %s already exists, updating config", why.why, prefixedTopic)
				default:
					// uh, oh. Handled later
				}
				for tpc, topicErr := range resp.Errors {
					if tpc != prefixedTopic {
						lib.logf(ctx, "[event] received create topic response for topic (%s) not in request (%s %s): %s", tpc, why.why, prefixedTopic, topicErr)
					}
				}
			}
			if resp.Throttle != 0 {
				lib.logf(ctx, "[events] %s: topic creation request was throttled for %s", why.why, resp.Throttle)
			}
		}
		return nil
	}
	lib.topicsWork.ItemDone = func(ctx context.Context, unprefixedTopic string, why topicsWhy) {
		lib.logf(ctx, "[events] %s: topic %s should now exist", why.why, unprefixedTopic)
	}
	lib.topicsWork.ItemFailed = func(ctx context.Context, unprefixedTopic string, why topicsWhy, err error, primary bool) error {
		err = errors.Errorf("event library error creating topic (%s) (%s): %w", unprefixedTopic, why.why, err)
		if primary {
			err = errors.Alert(err)
		}
		lib.logf(ctx, "[events] %+v", err)
		return err
	}
	lib.topicsWork.ItemTimeoutError = func(_ context.Context, unprefixedTopic string, why topicsWhy, _ error) error {
		return errors.Errorf("event library could not create kafka topic (%s) (%s): %w", unprefixedTopic, why.why, ErrTopicCreationTimeout)
	}
	lib.topicsWork.ItemPending = func(ctx context.Context, unprefixedTopic string, why topicsWhy) {
		lib.logf(ctx, "[events] %s: will wait for creation attempt of topic %s to complete", why.why, unprefixedTopic)
	}
	lib.topicsWork.PreWork = func(ctx context.Context, why topicsWhy, unprefixedTopics []string) error {
		if lib.ready.Load() == isNotConfigured {
			err := errors.Alertf("attempt to create topics before library configuration (%s)", why.why)
			lib.logf(ctx, "[events] %s: %+v", why.why, err)
			panic(err)
		}
		for _, unprefixedTopic := range unprefixedTopics {
			if unprefixedTopic == "" {
				err := errors.Errorf("cannot create an empty topic (%s) in event library", why.why)
				lib.logf(ctx, "[events] %s: %+v", why.why, err)
				return err
			}
		}
		if err := lib.waitForTopicsListing(ctx); err != nil {
			return err
		}
		return nil
	}
	lib.topicsWork.SpanMapItem = func(_ context.Context, topic string, why topicsWhy) map[string]string {
		return map[string]string{
			"action": "thread",
			"thread": "create or update topic " + topic + " for " + why.why,
		}
	}
}

func (lib *LibraryNoDB) listAvailableTopics(ctx context.Context) error {
	dialer := lib.dialer()
	b := topicListingBackoffPolicy.Start(ctx)
	for backoff.Continue(b) {
		lib.logf(ctx, "[events] starting over on listing topics")
		for _, i := range rand.Perm(len(lib.brokers)) {
			broker := lib.brokers[i]
			lib.logf(ctx, "[events] connecting to %s to list topics", broker)
			conn, err := dialer.DialContext(ctx, "tcp", broker)
			if err != nil {
				lib.logf(ctx, "[events] could not connect to broker %s, was going to list topics: %v", broker, err)
				continue
			}
			partitions, err := conn.ReadPartitions()
			_ = conn.Close()
			if err != nil {
				lib.logf(ctx, "[events] could not list partitions on broker %s: %v", broker, err)
				continue
			}
			lib.logf(ctx, "[events] listing existing topics...")
			seen := make(map[string]bool)
			found := make(map[string]struct{})
			for _, p := range partitions {
				if seen[p.Topic] {
					continue
				}
				seen[p.Topic] = true
				unprefixedTopic := lib.removePrefix(p.Topic)
				if lib.prefix != "" && unprefixedTopic == p.Topic {
					if debugLogTopicsMissingPrefix {
						lib.logf(ctx, "[events] topic %s found in partition, IGNORING (not prefixed)", p.Topic)
					}
					continue
				}
				lib.logf(ctx, "[events] topic %s found in partition", unprefixedTopic)
				found[unprefixedTopic] = struct{}{}
			}
			lib.markListedTopicsDone(found)
			// Published before topicsHaveBeenListed is closed. Waiters synchronize on that close.
			lib.existingTopics = found
			lib.logf(ctx, "[events] done listing existing topics")
			return nil
		}
		lib.logf(ctx, "[events] waiting before making another attempt to list topics")
	}
	if err := ctx.Err(); err != nil {
		return errors.Errorf("event library could not list kafka topics from any broker: %w", err)
	}
	return errors.Errorf("event library could not list kafka topics from any broker")
}

func (lib *LibraryNoDB) waitForTopicsListing(ctx context.Context) error {
	lib.topicListingStarted.Do(func() {
		// The listing thread is library-owned. Individual callers may stop waiting
		// via ctx, but must not cancel the one shared listing attempt.
		threadCtx, threadDone := lib.threadContext(lib.shutdownCtx, map[string]string{
			"action": "thread",
			"thread": "list available topics",
		})
		go func() {
			defer threadDone()
			lib.topicsListingErr = lib.listAvailableTopics(threadCtx)
			close(lib.topicsHaveBeenListed)
		}()
	})
	select {
	case <-lib.topicsHaveBeenListed:
		return lib.topicsListingErr
	case <-ctx.Done():
		select {
		case <-lib.topicsHaveBeenListed:
			return lib.topicsListingErr
		default:
			return ctx.Err()
		}
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

func (lib *LibraryNoDB) updateDeadLetterTopicConfigs(ctx context.Context, client *kafka.Client, unprefixedTopic string, entries []kafka.ConfigEntry) error {
	for _, deadLetter := range lib.deadLetterTopics(unprefixedTopic) {
		prefixed := lib.addPrefix(deadLetter)
		lib.logf(ctx, "[events] dead letter topic %s already exists, checking config to update", prefixed)
		if err := alterExistingTopicConfig(ctx, client, prefixed, entries); err != nil {
			return err
		}
	}
	return nil
}

// deadLetterTopics returns existing dead-letter topics for unprefixedTopic
// built from registered consumer groups. Only topics already in the cluster
// are returned.
func (lib *LibraryNoDB) deadLetterTopics(unprefixedTopic string) []string {
	names := make([]string, 0)
	for _, name := range lib.registeredDeadLetterTopics(unprefixedTopic) {
		if _, exists := lib.existingTopics[name]; exists {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (lib *LibraryNoDB) registeredDeadLetterTopics(unprefixedTopic string) []string {
	names := make([]string, 0)
	for consumerGroup, group := range lib.readers {
		if _, ok := group.topics[unprefixedTopic]; !ok {
			continue
		}
		names = append(names, DeadLetterTopic(unprefixedTopic, consumerGroup))
	}
	return names
}

// markListedTopicsDone marks existing topics that have no configuration to
// apply. A dead-letter topic built from a registered consumer group stays
// open when its original topic stays open.
func (lib *LibraryNoDB) markListedTopicsDone(found map[string]struct{}) {
	configured := lib.configuredTopicNames()
	keepOpen := make(map[string]struct{})
	for topic := range configured {
		for _, deadLetter := range lib.registeredDeadLetterTopics(topic) {
			keepOpen[deadLetter] = struct{}{}
		}
	}
	for topic := range found {
		if _, ok := configured[topic]; ok {
			continue
		}
		if _, ok := keepOpen[topic]; ok {
			continue
		}
		lib.topicsWork.SetDone(topic)
	}
}

func (lib *LibraryNoDB) configuredTopicNames() map[string]struct{} {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	names := make(map[string]struct{}, len(lib.topicConfig))
	for name := range lib.topicConfig {
		names[name] = struct{}{}
	}
	return names
}

func alterExistingTopicConfig(ctx context.Context, client *kafka.Client, topic string, entries []kafka.ConfigEntry) error {
	if len(entries) == 0 {
		return nil
	}
	configNames := make([]string, len(entries))
	for i, entry := range entries {
		configNames[i] = entry.ConfigName
	}
	described, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Resources: []kafka.DescribeConfigRequestResource{{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
			ConfigNames:  configNames,
		}},
	})
	if err != nil {
		return errors.Errorf("describe topic config for %s: %w", topic, err)
	}
	if described == nil || len(described.Resources) != 1 {
		return errors.Errorf("describe topic config for %s: unexpected response", topic)
	}
	resource := described.Resources[0]
	if resource.Error != nil {
		return errors.Errorf("describe topic config for %s: %w", topic, resource.Error)
	}

	current := make(map[string]string, len(resource.ConfigEntries))
	for _, entry := range resource.ConfigEntries {
		current[entry.ConfigName] = entry.ConfigValue
	}
	changed := make([]kafka.IncrementalAlterConfigsRequestConfig, 0, len(entries))
	for _, entry := range entries {
		if value, ok := current[entry.ConfigName]; ok && value == entry.ConfigValue {
			continue
		}
		changed = append(changed, kafka.IncrementalAlterConfigsRequestConfig{
			Name:            entry.ConfigName,
			Value:           entry.ConfigValue,
			ConfigOperation: kafka.ConfigOperationSet,
		})
	}
	if len(changed) == 0 {
		return nil
	}
	altered, err := client.IncrementalAlterConfigs(ctx, &kafka.IncrementalAlterConfigsRequest{
		Resources: []kafka.IncrementalAlterConfigsRequestResource{{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
			Configs:      changed,
		}},
	})
	if err != nil {
		return errors.Errorf("alter topic config for %s: %w", topic, err)
	}
	if altered == nil || len(altered.Resources) != 1 {
		return errors.Errorf("alter topic config for %s: unexpected response", topic)
	}
	if altered.Resources[0].Error != nil {
		return errors.Errorf("alter topic config for %s: %w", topic, altered.Resources[0].Error)
	}
	return nil
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
