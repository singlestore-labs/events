package events

import (
	"context"
	"math/rand"
	"slices"
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
// The topic name in the topic config must be unprefixed.
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

// setKafkaTopicConfig records the configs Kafka reported, keyed by unprefixed topic.
// Resource names are prefixed. Resources with errors are skipped.
func (lib *LibraryNoDB) setKafkaTopicConfig(resources []kafka.DescribeConfigResponseResource) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	for _, r := range resources {
		if r.Error != nil {
			continue
		}
		lib.kafkaTopicConfig[lib.removePrefix(r.ResourceName)] = r.ConfigEntries
	}
}

// deleteKafkaTopicConfig forgets the configs Kafka reported for resources whose state is unknown.
// Resource names are prefixed.
func (lib *LibraryNoDB) deleteKafkaTopicConfig(resources []kafka.IncrementalAlterConfigsRequestResource) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	for _, r := range resources {
		delete(lib.kafkaTopicConfig, lib.removePrefix(r.ResourceName))
	}
}

// getKafkaTopicConfig returns the configs Kafka last reported. The result must not be modified.
func (lib *LibraryNoDB) getKafkaTopicConfig(unprefixedTopic string) ([]kafka.DescribeConfigResponseConfigEntry, bool) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	c, ok := lib.kafkaTopicConfig[unprefixedTopic]
	return c, ok
}

// UpdateTopicConfigs updates every topic registered in topicConfig.
// If a topic doesn't already exist, it will be created.
// Configs that were not set with SetTopicConfig are left as they are in Kafka.
func (lib *LibraryNoDB) UpdateTopicConfigs(ctx context.Context) (err error) {
	topics := func() []string {
		lib.lock.Lock()
		defer lib.lock.Unlock()
		defer func() {
			if lib.ready.Load() == isShutdown {
				err = errors.Errorf("event library has been shut down, cannot update topic configs: %w", lib.shutdownCtx.Err())
			} else {
				lib.libraryDone.Add(1)
			}
		}()

		topics := make([]string, 0, len(lib.topicConfig))
		for topic := range lib.topicConfig {
			topics = append(topics, topic)
		}
		return topics
	}()
	if err != nil {
		return err
	}

	defer lib.libraryDone.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lib.shutdownCtx.Done():
		return errors.Errorf("event library has been shut down, cannot update topic configs: %w", lib.shutdownCtx.Err())
	case lib.syncConfigProcess <- struct{}{}:
	}

	defer func() {
		<-lib.syncConfigProcess
	}()
	err = lib.createTopics(ctx, topics)
	if err != nil {
		return
	}
	return lib.syncTopicConfigFromKafka(ctx, topics)
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

func (lib *LibraryNoDB) setupDeadLetterTopicConfigHelper(topic, deadLetterTopic string) {
	if _, ok := lib.getTopicConfig(deadLetterTopic); !ok {
		if config, ok := lib.getTopicConfig(topic); ok {
			config.Topic = deadLetterTopic
			config.ConfigEntries = slices.Clone(config.ConfigEntries)
			lib.SetTopicConfig(config)
		}
	}
}

// createTopics creates each topic in unprefixedTopics. Topics must be unique.
func (lib *LibraryNoDB) createTopics(ctx context.Context, unprefixedTopics []string) error {
	if len(unprefixedTopics) == 0 {
		return nil
	}
	client, err := lib.getController(ctx)
	if err != nil {
		return errors.Errorf("event library could not get kafka controller: %w", err)
	}

	topicConfigs := make([]kafka.TopicConfig, 0, len(unprefixedTopics))
	for _, topic := range unprefixedTopics {
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

// describeTopicConfigs reads topic configuration from Kafka.
// The response will have prefixedTopics. Errors for individual topics are in each resource's Error.
func (lib *LibraryNoDB) describeTopicConfigs(ctx context.Context, unprefixedTopics []string) (*kafka.DescribeConfigsResponse, error) {
	client, err := lib.getController(ctx)
	if err != nil {
		return nil, errors.Errorf("event library could not get kafka controller: %w", err)
	}

	reqResources := make([]kafka.DescribeConfigRequestResource, 0, len(unprefixedTopics))
	for _, topic := range unprefixedTopics {
		reqResources = append(reqResources, kafka.DescribeConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: lib.addPrefix(topic),
		})
	}

	resp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Resources: reqResources,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not get topics config from kafka")
	}
	return resp, nil
}

// syncTopicConfigFromKafka describes topics and applies the configs set with SetTopicConfig where Kafka differs.
func (lib *LibraryNoDB) syncTopicConfigFromKafka(ctx context.Context, unprefixedTopics []string) error {
	if len(unprefixedTopics) == 0 {
		return nil
	}
	resp, err := lib.describeTopicConfigs(ctx, unprefixedTopics)
	if err != nil {
		return err
	}
	incRequestResources, describeErr := lib.generateKafkaUpdateRequests(resp)
	if len(incRequestResources) == 0 {
		return describeErr
	}
	// Kafka's configs for these topics may have changed, even if the request failed.
	// They are described again when next needed.
	lib.deleteKafkaTopicConfig(incRequestResources)

	client, err := lib.getController(ctx)
	if err != nil {
		return errors.Join(describeErr, errors.Errorf("event library could not get kafka controller: %w", err))
	}
	updateResp, err := client.IncrementalAlterConfigs(ctx, &kafka.IncrementalAlterConfigsRequest{
		Resources: incRequestResources,
	})
	if err != nil {
		return errors.Join(describeErr, errors.Wrapf(err, "failed to update topics config"))
	}
	topicUpdateErrors := []error{describeErr}
	for _, r := range updateResp.Resources {
		if r.Error != nil {
			topicUpdateErrors = append(topicUpdateErrors, errors.Wrapf(r.Error, "failed update topic-%s", r.ResourceName))
		}
	}
	return errors.Join(topicUpdateErrors...)
}

// generateKafkaUpdateRequests records the configs Kafka reported and returns the changes needed
// to make Kafka match the configs set with SetTopicConfig. Configs only Kafka has are left alone.
func (lib *LibraryNoDB) generateKafkaUpdateRequests(resp *kafka.DescribeConfigsResponse) ([]kafka.IncrementalAlterConfigsRequestResource, error) {
	if resp == nil {
		return nil, errors.Errorf("empty describe topic response")
	}
	lib.setKafkaTopicConfig(resp.Resources)

	incReqResources := make([]kafka.IncrementalAlterConfigsRequestResource, 0, len(resp.Resources))
	respErrors := make([]error, 0)
	for _, kTopicConfig := range resp.Resources {
		if kTopicConfig.Error != nil {
			respErrors = append(respErrors, errors.Wrapf(kTopicConfig.Error, "could not describe topic %s", kTopicConfig.ResourceName))
			continue
		}
		kafkaValues := make(map[string]string, len(kTopicConfig.ConfigEntries))
		for _, entry := range kTopicConfig.ConfigEntries {
			kafkaValues[entry.ConfigName] = entry.ConfigValue
		}

		wantConfig, _ := lib.getTopicConfig(lib.removePrefix(kTopicConfig.ResourceName))
		var incConfigs []kafka.IncrementalAlterConfigsRequestConfig
		// Only configs we want are set. Configs that Kafka has but we don't
		// set are intentionally left alone; we never delete extra configs.
		for _, want := range wantConfig.ConfigEntries {
			if have, ok := kafkaValues[want.ConfigName]; ok && have == want.ConfigValue {
				continue
			}
			incConfigs = append(incConfigs, kafka.IncrementalAlterConfigsRequestConfig{
				Name:            want.ConfigName,
				Value:           want.ConfigValue,
				ConfigOperation: kafka.ConfigOperationSet,
			})
		}
		if len(incConfigs) > 0 {
			incReqResources = append(incReqResources, kafka.IncrementalAlterConfigsRequestResource{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: kTopicConfig.ResourceName,
				Configs:      incConfigs,
			})
		}
	}
	return incReqResources, errors.Join(respErrors...)
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
