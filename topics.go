package events

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
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
// SetTopicConfig should accept the unprefixed topic name.
func (lib *LibraryNoDB) SetTopicConfig(topicConfig kafka.TopicConfig) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	if topicConfig.Topic == "" {
		panic(errors.Alertf("attempt to register event library topic configuration with an empty topic name"))
	}
	lib.topicConfig[topicConfig.Topic] = topicConfig
}

// SetTopicConfigWithDeadLetter is SetTopicConfig plus the same config for every
// dead-letter topic of topicConfig.Topic. topicConfig.Topic is unprefixed.
func (lib *LibraryNoDB) SetTopicConfigWithDeadLetter(topicConfig kafka.TopicConfig) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	if topicConfig.Topic == "" {
		panic(errors.Alertf("attempt to register event library topic configuration with an empty topic name"))
	}
	lib.topicConfig[topicConfig.Topic] = topicConfig

	deadLetters := make(map[string]struct{})
	for groupName, group := range lib.readers {
		topicHandler, ok := group.topics[topicConfig.Topic]
		if !ok {
			continue
		}
		for _, handler := range topicHandler.handlers {
			if handler.isDeadLetter {
				continue
			}
			switch handler.onFailure {
			case eventmodels.OnFailureRetryLater, eventmodels.OnFailureSave:
				deadLetters[DeadLetterTopic(topicConfig.Topic, groupName)] = struct{}{}
			}
		}
	}
	for deadLetterTopic := range deadLetters {
		deadLetterConfig := topicConfig
		deadLetterConfig.Topic = deadLetterTopic
		deadLetterConfig.ConfigEntries = append([]kafka.ConfigEntry(nil), topicConfig.ConfigEntries...)
		lib.topicConfig[deadLetterTopic] = deadLetterConfig
	}
}

func (lib *LibraryNoDB) getTopicConfig(unprefixedTopic string) (kafka.TopicConfig, bool) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	c, ok := lib.topicConfig[unprefixedTopic]
	return c, ok
}

// UpdateTopicConfig updates every topic registered in topicConfig.
// It creates those topics, then applies the registered config to them.
func (lib *LibraryNoDB) UpdateTopicConfig(ctx context.Context) (err error) {
	lib.lock.Lock()
	topics := make([]string, 0, len(lib.topicConfig))
	for topic := range lib.topicConfig {
		topics = append(topics, topic)
	}
	lib.lock.Unlock()

	lib.libraryDone.Add(1)
	go func() {
		defer lib.libraryDone.Done()
		lib.syncConfigProcess <- struct{}{}
		defer func() {
			<-lib.syncConfigProcess
		}()
		err = lib.createTopics(ctx, topics)
		if err != nil {
			return
		}
		err = lib.syncTopicConfigFromKafka(ctx, topics)
	}()
	return err
}

func (lib *LibraryNoDB) getOrDefaultConfig(unprefixedTopic string) kafka.TopicConfig {
	tc, _ := lib.getTopicConfig(unprefixedTopic)
	tc.Topic = lib.addPrefix(unprefixedTopic)
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
	return tc
}

// createTopics creates each topic in unprefixedTopics.
func (lib *LibraryNoDB) createTopics(ctx context.Context, unprefixedTopics []string) error {
	if len(unprefixedTopics) == 0 {
		return nil
	}
	client, err := lib.getController(ctx)
	if err != nil {
		return errors.Errorf("event library could not get kafka controller: %w", err)
	}

	topicConfigs := make([]kafka.TopicConfig, 0, len(unprefixedTopics))
	seen := make(map[string]struct{}, len(unprefixedTopics))
	for _, topic := range unprefixedTopics {
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		topicConfigs = append(topicConfigs, lib.getOrDefaultConfig(topic))
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: topicConfigs,
	})
	if err != nil {
		return errors.Errorf("could not create topics %v: %w", unprefixedTopics, err)
	}
	respErrors := make([]error, 0)
	for t, e := range resp.Errors {
		if e == nil || errors.Is(e, kafka.TopicAlreadyExists) {
			continue
		}
		lib.logf(ctx, "[events] received error when creating topic %s", t)
		respErrors = append(respErrors, errors.Errorf("failed on create topic-%s: %w", t, e))
	}
	return errors.Join(respErrors...)
}

// describeTopicConfigs reads topic configuration from Kafka and does not change it.
func (lib *LibraryNoDB) describeTopicConfigs(ctx context.Context, unprefixedTopics []string) (*kafka.DescribeConfigsResponse, error) {
	client, err := lib.getController(ctx)
	if err != nil {
		return nil, errors.Errorf("event library could not get kafka controller: %w", err)
	}

	reqResources := make([]kafka.DescribeConfigRequestResource, 0, len(unprefixedTopics))
	for _, t := range unprefixedTopics {
		reqResources = append(reqResources, kafka.DescribeConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: lib.addPrefix(t),
		})
	}

	rsp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Resources: reqResources,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not get topics config from kafka")
	}
	return rsp, nil
}

// syncTopicConfigFromKafka describes topics and applies the cached config where it differs.
func (lib *LibraryNoDB) syncTopicConfigFromKafka(ctx context.Context, topics []string) error {
	rsp, err := lib.describeTopicConfigs(ctx, topics)
	if err != nil {
		return err
	}
	client, err := lib.getController(ctx)
	if err != nil {
		return errors.Errorf("event library could not get kafka controller: %w", err)
	}

	updateResp, err := client.IncrementalAlterConfigs(ctx, &kafka.IncrementalAlterConfigsRequest{
		Resources: lib.handleDescribeTopicResponse(rsp),
	})
	if err != nil {
		return errors.Wrapf(err, "failed to update topics config")
	}
	topicUpdateErrors := make([]error, 0)
	for _, resp := range updateResp.Resources {
		topicUpdateErrors = append(topicUpdateErrors, errors.Wrapf(resp.Error, "failed update topic-%s", resp.ResourceName))
	}
	return errors.Join(topicUpdateErrors...)
}

// handleDescribeTopicResponse compare with lib cache topic config that
// 1. generate incremental update request if lib cache exists and different with the describe response.
// 2. save config to lib cache when there is no one.
func (lib *LibraryNoDB) handleDescribeTopicResponse(rsp *kafka.DescribeConfigsResponse) []kafka.IncrementalAlterConfigsRequestResource {
	incReqResources := make([]kafka.IncrementalAlterConfigsRequestResource, 0)

	for _, kTopicConfig := range rsp.Resources {
		unprefixedTopicName := lib.removePrefix(kTopicConfig.ResourceName)
		existTopicConfig, ok := lib.topicConfig[unprefixedTopicName]
		if !ok { // if never set config
			existTopicConfig = kafka.TopicConfig{
				Topic:         unprefixedTopicName,
				ConfigEntries: make([]kafka.ConfigEntry, len(kTopicConfig.ConfigEntries)),
			}
		} // else { // compare and update if different

		currentReqResource := kafka.IncrementalAlterConfigsRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: kTopicConfig.ResourceName,
			Configs:      make([]kafka.IncrementalAlterConfigsRequestConfig, 0),
		}

		existConfigEntries := make(map[string]string, len(existTopicConfig.ConfigEntries))
		for _, entry := range existTopicConfig.ConfigEntries {
			existConfigEntries[entry.ConfigName] = entry.ConfigValue
		}

		for _, entry := range kTopicConfig.ConfigEntries {
			// if not exist, take from kafka, if exist override kafka
			if existValue, ok := existConfigEntries[entry.ConfigName]; ok {
				if entry.ConfigValue != existValue {
					currentReqResource.Configs = append(currentReqResource.Configs,
						kafka.IncrementalAlterConfigsRequestConfig{
							Name:            entry.ConfigName,
							Value:           existValue,
							ConfigOperation: kafka.ConfigOperationSet,
						})
				}
				// skip if same value
			} else {
				// else, not set, not change, save value to cache
				existTopicConfig.ConfigEntries = append(existTopicConfig.ConfigEntries, kafka.ConfigEntry{
					ConfigName:  entry.ConfigName,
					ConfigValue: entry.ConfigValue,
				})
			}
		}
		if len(currentReqResource.Configs) > 0 {
			incReqResources = append(incReqResources, currentReqResource)
		}
		lib.topicConfig[unprefixedTopicName] = existTopicConfig
	}
	return incReqResources
}

// DefaultTrimBatchSize is how many eventsProcessed rows one delete statement removes
// when TrimDB is called with batchSize <= 0.
var DefaultTrimBatchSize = 1000

// DefaultTrimBatchInterval is the pause after a full trim batch when TrimDB is called
// with interval < 0.
var DefaultTrimBatchInterval = 100 * time.Millisecond

// PauseTrimBatch waits between full trim batches. interval == 0 returns immediately.
// interval < 0 uses DefaultTrimBatchInterval.
func PauseTrimBatch(ctx context.Context, batchInterval time.Duration) error {
	if batchInterval < 0 {
		batchInterval = DefaultTrimBatchInterval
	}
	if batchInterval == 0 {
		return nil
	}
	timer := time.NewTimer(batchInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// TrimDB deletes exactly-once delivery records that Kafka can no longer redeliver.
// The cutoff is the original topic's retention.ms + segment.ms, plus the longest
// dead-letter retention.ms + segment.ms, plus margin. Those durations come from
// the configs Kafka reports, including for dead-letter topics created from the original.
// batchSize is the number of rows deleted per statement. batchSize <= 0 uses DefaultTrimBatchSize.
// interval is the pause after a full batch. interval == 0 does not pause. interval < 0 uses DefaultTrimBatchInterval.
func (lib *Library[ID, TX, DB]) TrimDB(ctx context.Context, margin time.Duration, batchSize int, batchInterval time.Duration) error {
	if margin < 0 {
		return errors.Errorf("trim margin must not be negative")
	}
	if batchSize <= 0 {
		batchSize = DefaultTrimBatchSize
	}
	if batchSize <= 0 {
		return errors.Errorf("trim batch size must be positive")
	}
	if batchInterval < 0 {
		batchInterval = DefaultTrimBatchInterval
	}
	if !lib.HasDB() {
		return errors.Errorf("event library trim requires a database")
	}
	exactlyOnceTopics := func() map[string][]string {
		lib.lock.Lock()
		defer lib.lock.Unlock()
		topics := make(map[string][]string)
		for groupName, group := range lib.readers {
			for topic, topicHandler := range group.topics {
				var exactlyOnce bool
				var deadLetter bool
				for _, handler := range topicHandler.handlers {
					if handler.isDeadLetter {
						continue
					}
					if _, ok := handler.handler.(eventmodels.HandlerTxInterface[ID, TX]); !ok {
						continue
					}
					exactlyOnce = true
					switch handler.onFailure {
					case eventmodels.OnFailureRetryLater, eventmodels.OnFailureSave:
						deadLetter = true
					}
				}
				if !exactlyOnce {
					continue
				}
				if _, ok := topics[topic]; !ok {
					topics[topic] = nil
				}
				if deadLetter {
					topics[topic] = append(topics[topic], DeadLetterTopic(topic, groupName))
				}
			}
		}
		return topics
	}()
	if len(exactlyOnceTopics) == 0 {
		return nil
	}
	topicNames := func() []string {
		names := make([]string, 0, len(exactlyOnceTopics))
		for original, deadLetters := range exactlyOnceTopics {
			names = append(names, original)
			names = append(names, deadLetters...)
		}
		return names
	}()

	err := lib.createTopics(ctx, topicNames)
	if err != nil {
		return err
	}

	rsp, err := lib.describeTopicConfigs(ctx, topicNames)
	if err != nil {
		return err
	}
	described := make(map[string]kafka.DescribeConfigResponseResource, len(rsp.Resources))
	for _, resource := range rsp.Resources {
		described[lib.removePrefix(resource.ResourceName)] = resource
	}

	now := time.Now()
	trimErrs := make([]error, 0)
	for topic, deadLetters := range exactlyOnceTopics {
		original, ok := described[topic]
		if !ok {
			trimErrs = append(trimErrs, errors.Errorf("could not describe topic %s", topic))
			continue
		}
		if original.Error != nil {
			trimErrs = append(trimErrs, errors.Wrapf(original.Error, "could not describe topic %s", topic))
			continue
		}
		originalKeep, err := topicRetentionAndSegment(original.ConfigEntries)
		if err != nil {
			trimErrs = append(trimErrs, errors.Wrapf(err, "could not trim topic %s", topic))
			continue
		}
		if originalKeep < 0 {
			lib.logf(ctx, "[events] skip trim of topic %s, retention.ms or segment.ms is unlimited", topic)
			continue
		}
		var longestDeadLetter time.Duration
		skip := false
		for _, deadLetter := range deadLetters {
			resource, ok := described[deadLetter]
			if !ok {
				trimErrs = append(trimErrs, errors.Errorf("could not describe dead letter topic %s", deadLetter))
				skip = true
				break
			}
			if resource.Error != nil {
				trimErrs = append(trimErrs, errors.Wrapf(resource.Error, "could not describe dead letter topic %s", deadLetter))
				skip = true
				break
			}
			deadLetterKeep, err := topicRetentionAndSegment(resource.ConfigEntries)
			if err != nil {
				trimErrs = append(trimErrs, errors.Wrapf(err, "could not trim topic %s", topic))
				skip = true
				break
			}
			if deadLetterKeep < 0 {
				lib.logf(ctx, "[events] skip trim of topic %s, dead letter topic %s retention.ms or segment.ms is unlimited", topic, deadLetter)
				skip = true
				break
			}
			if deadLetterKeep > longestDeadLetter {
				longestDeadLetter = deadLetterKeep
			}
		}
		if skip {
			continue
		}
		cutoff := now.Add(-(originalKeep + longestDeadLetter + margin))
		deleted, err := lib.db.TrimEventsProcessed(ctx, topic, cutoff, batchSize, batchInterval)
		if err != nil {
			trimErrs = append(trimErrs, err)
			continue
		}
		lib.logf(ctx, "[events] trimmed %d exactly-once records for topic %s older than %s", deleted, topic, cutoff.Format(time.RFC3339))
	}
	return errors.Join(trimErrs...)
}

// topicRetentionAndSegment returns retention.ms + segment.ms. A negative config means no time limit.
// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
// https://kafka.apache.org/documentation/#topicconfigs_segment.ms
func topicRetentionAndSegment(entries []kafka.DescribeConfigResponseConfigEntry) (time.Duration, error) {
	topicConfigDuration := func(name string) (time.Duration, error) {
		for _, entry := range entries {
			if entry.ConfigName != name {
				continue
			}
			ms, err := strconv.ParseInt(entry.ConfigValue, 10, 64)
			if err != nil {
				return 0, errors.Errorf("topic config %s value %q: %w", name, entry.ConfigValue, err)
			}
			return time.Duration(ms) * time.Millisecond, nil
		}
		return 0, errors.Errorf("topic config %s is missing", name)
	}
	retention, err := topicConfigDuration("retention.ms")
	if err != nil || retention < 0 {
		return retention, err
	}
	segment, err := topicConfigDuration("segment.ms")
	if err != nil || segment < 0 {
		return segment, err
	}
	return retention + segment, nil
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
		tc := lib.getOrDefaultConfig(unprefixedTopic)
		mir := getIntConfigValue(tc, "min.insync.replicas")

		var ctr kafka.CreateTopicsRequest
		ctr.Topics = append(ctr.Topics, tc)
		lib.logf(ctx, "[events] %s: attempting creation of topic %s with replicas %d and min.insync %d", why.why, tc.Topic, tc.ReplicationFactor, mir)
		client, err := lib.getController(ctx)
		if err == nil {
			lib.logf(ctx, "[events] %s: making topic creation request for %v", why.why, tc.Topic)
			var resp *kafka.CreateTopicsResponse
			resp, err = client.CreateTopics(ctx, &ctr)
			if err == nil {
				err = resp.Errors[tc.Topic]
				switch {
				case err == nil:
					lib.logf(ctx, "[events] %s: topic %s no error when creating", why.why, tc.Topic)
				case errors.Is(err, kafka.TopicAlreadyExists):
					lib.logf(ctx, "[events] %s: topic %s already exists", why.why, tc.Topic)
					err = nil
				default:
					// uh, oh. Handled later
				}
				for tpc, topicErr := range resp.Errors {
					if tpc != tc.Topic {
						lib.logf(ctx, "[event] received create topic response for topic (%s) not in request (%s %s): %s", tpc, why.why, tc.Topic, topicErr)
					}
				}
			}
			if resp.Throttle != 0 {
				lib.logf(ctx, "[events] %s: topic creation request was throttled for %s", why.why, resp.Throttle)
			}
		}
		return err
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
			"thread": "create topic " + topic + " for " + why.why,
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
				lib.topicsWork.SetDone(unprefixedTopic)
			}
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
