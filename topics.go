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

// UpdateTopicConfig is used to update the topic config for an existing topic.
// it pulls the config from kafka then compares it to the desired config and applies the changes.
func (lib *LibraryNoDB) UpdateTopicConfig(ctx context.Context) (err error) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.libraryDone.Add(1)
	go func() {
		defer lib.libraryDone.Done()
		lib.syncConfigProcess <- struct{}{}
		defer func() {
			<-lib.syncConfigProcess
		}()
		updateTopicList := make([]string, len(lib.topicConfig))
		for t := range lib.topicConfig {
			updateTopicList = append(updateTopicList, t)
		}
		err = lib.createTopics(ctx, updateTopicList)
		if err != nil {
			return
		}
		err = lib.syncTopicConfigFromKafka(ctx, updateTopicList)
	}()
	return err
}

func (lib *LibraryNoDB) getOrDefaultConfig(unprefixedTopic string) kafka.TopicConfig {
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
	return tc
}

func (lib *LibraryNoDB) createTopics(ctx context.Context, topics []string) error {
	client, err := lib.getController(ctx)
	if err != nil {
		return errors.Errorf("event library could not get kafka controller: %w", err)
	}

	topicConfigs := make([]kafka.TopicConfig, len(topics))
	for _, t := range topics {
		topicConfigs = append(topicConfigs, lib.getOrDefaultConfig(t))
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: topicConfigs,
	})
	if err != nil {
		return errors.Errorf("could not create topics %v: %w", topics, err)
	}
	respErrors := make([]error, 0)
	for t, e := range resp.Errors {
		if e != nil && errors.Is(e, kafka.TopicAlreadyExists) {
			lib.logf(ctx, "[events] received error when creating topic %s", t)
			respErrors = append(respErrors, errors.Errorf("failed on create topic-%s: %w", t, e))
		}
	}
	return errors.Join(respErrors...)
}

// syncTopicConfigFromKafka is used to sync the topic config from kafka to the library.
func (lib *LibraryNoDB) syncTopicConfigFromKafka(ctx context.Context, unprefixedTopics []string) error {
	client, err := lib.getController(ctx)
	if err != nil {
		return errors.Errorf("event library could not get kafka controller: %w", err)
	}

	reqResources := make([]kafka.DescribeConfigRequestResource, len(unprefixedTopics))
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
		return errors.Wrapf(err, "could not get topics config from kafka")
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

func (lib *LibraryNoDB) TrimDB(ctx context.Context, margin time.Duration) error {
	exactlyOneTopics := func() []string {
		lib.lock.Lock()
		defer lib.lock.Unlock()
		seen := make(map[string]struct{})
		topics := make([]string, 0)
		for _, group := range lib.readers {
			for topic, topicHandler := range group.topics {
				if _, ok := seen[topic]; ok {
					continue
				}
				for _, handler := range topicHandler.handlers {
					if _, ok := handler.handler.(eventmodels.HandlerTxInterface[ID, TX]); ok {
						seen[topic] = struct{}{}
						topics = append(topics, topic)
						break
					}
				}
			}
		}
		return topics
	}()

	err := lib.createTopics(ctx, exactlyOneTopics)
	if err != nil {
		return err
	}

	// pull the kafka config (only pull don't update, could abstract with the descibe func in the update )

	// trim based on the kafka config (retention time, segment time , margin)
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
