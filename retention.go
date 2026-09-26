package events

import (
	"context"
	"strconv"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
)

const (
	configRetentionMS   = "retention.ms"
	configSegmentMS     = "segment.ms"
	configTimestampType = "message.timestamp.type"
	configCleanupPolicy = "cleanup.policy"
	logAppendTime       = "LogAppendTime"
)

// topicRetentionConfig is a per-topic override. A zero duration leaves that
// setting unchanged and does not fall through to the library default.
type topicRetentionConfig struct {
	retention time.Duration
	segment   time.Duration
}

// topicAdmin is the Kafka admin API used to read and update topic configuration.
// Production uses *kafka.Client. Tests may substitute an implementation.
type topicAdmin interface {
	DescribeConfigs(ctx context.Context, req *kafka.DescribeConfigsRequest) (*kafka.DescribeConfigsResponse, error)
	IncrementalAlterConfigs(ctx context.Context, req *kafka.IncrementalAlterConfigsRequest) (*kafka.IncrementalAlterConfigsResponse, error)
}

// SetDefaultTopicRetention sets retention.ms and segment.ms applied to topics
// created later. A duration <= 0 leaves that broker setting unchanged.
// Explicit retention.ms or segment.ms entries passed to SetTopicConfig win.
// Per-topic overrides from SetTopicRetention also win.
//
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SetDefaultTopicRetention(retention, segment time.Duration) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.defaultRetention = retention
	lib.defaultSegment = segment
	lib.warnIfSegmentExceedsRetention(context.Background(), "default", retention, segment)
}

// SetTopicRetention sets retention.ms and segment.ms for one unprefixed topic.
// The override is stored separately from SetTopicConfig, so a later
// SetTopicConfig call does not erase it. A duration <= 0 leaves that broker
// setting unchanged and suppresses the library default for that setting.
// Explicit retention.ms or segment.ms entries in SetTopicConfig still win.
//
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SetTopicRetention(unprefixedTopic string, retention, segment time.Duration) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	if unprefixedTopic == "" {
		panic(errors.Alertf("attempt to set event library topic retention with an empty topic name"))
	}
	if lib.topicRetention == nil {
		lib.topicRetention = make(map[string]topicRetentionConfig)
	}
	lib.topicRetention[unprefixedTopic] = topicRetentionConfig{
		retention: retention,
		segment:   segment,
	}
	lib.warnIfSegmentExceedsRetention(context.Background(), unprefixedTopic, retention, segment)
}

// ApplyTopicRetention updates retention.ms and segment.ms on topics that already
// exist. Only durations configured through SetDefaultTopicRetention,
// SetTopicRetention, or an explicit SetTopicConfig entry are sent.
// Unspecified values are left as they are. Dead-letter topics are not changed
// unless their unprefixed names are passed in.
//
// ApplyTopicRetention can only be used after Configure.
func (lib *Library[ID, TX, DB]) ApplyTopicRetention(ctx context.Context, unprefixedTopics ...string) error {
	if err := lib.start(ctx, "apply topic retention"); err != nil {
		return err
	}
	var resources []kafka.IncrementalAlterConfigsRequestResource
	var describe []kafka.DescribeConfigRequestResource
	for _, unprefixed := range unprefixedTopics {
		configs := lib.retentionAlterConfigs(unprefixed)
		if len(configs) == 0 {
			lib.logf(ctx, "[events] topic %s has no retention or segment override to apply", unprefixed)
			continue
		}
		prefixed := lib.addPrefix(unprefixed)
		describe = append(describe, kafka.DescribeConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: prefixed,
			ConfigNames:  []string{configRetentionMS, configSegmentMS},
		})
		resources = append(resources, kafka.IncrementalAlterConfigsRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: prefixed,
			Configs:      configs,
		})
	}
	if len(resources) == 0 {
		return nil
	}
	client, err := lib.adminClient(ctx)
	if err != nil {
		return err
	}
	described, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{Resources: describe})
	if err != nil {
		return errors.Errorf("describe topic retention: %w", err)
	}
	if described == nil {
		return errors.Errorf("describe topic retention: empty response")
	}
	for _, resource := range described.Resources {
		if resource.Error != nil {
			return errors.Errorf("describe topic retention for %s: %w", resource.ResourceName, resource.Error)
		}
		prevRetention, prevSegment := "", ""
		for _, entry := range resource.ConfigEntries {
			switch entry.ConfigName {
			case configRetentionMS:
				prevRetention = entry.ConfigValue
			case configSegmentMS:
				prevSegment = entry.ConfigValue
			}
		}
		lib.logf(ctx, "[events] topic %s retention before apply: retention.ms=%s segment.ms=%s", resource.ResourceName, prevRetention, prevSegment)
	}
	altered, err := client.IncrementalAlterConfigs(ctx, &kafka.IncrementalAlterConfigsRequest{
		Resources: resources,
	})
	if err != nil {
		return errors.Errorf("alter topic retention: %w", err)
	}
	if altered == nil {
		return errors.Errorf("alter topic retention: empty response")
	}
	for _, resource := range altered.Resources {
		if resource.Error != nil {
			return errors.Errorf("alter topic retention for %s: %w", resource.ResourceName, resource.Error)
		}
	}
	return nil
}

func (lib *LibraryNoDB) adminClient(ctx context.Context) (topicAdmin, error) {
	if lib.topicAdmin != nil {
		return lib.topicAdmin, nil
	}
	return lib.getController(ctx)
}

// effectiveRetention returns the setter values for a topic. Explicit
// SetTopicConfig entries are not applied here; callers that need them check
// the topic config themselves. A zero result means do not set that key.
func (lib *LibraryNoDB) effectiveRetention(unprefixedTopic string) (retention, segment time.Duration) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	if ov, ok := lib.topicRetention[unprefixedTopic]; ok {
		return positiveDuration(ov.retention), positiveDuration(ov.segment)
	}
	return positiveDuration(lib.defaultRetention), positiveDuration(lib.defaultSegment)
}

func positiveDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d
}

// withRetentionConfig appends retention.ms and segment.ms when the topic
// config does not already set them. SetTopicConfig entries win.
func (lib *LibraryNoDB) withRetentionConfig(ctx context.Context, unprefixedTopic string, entries []kafka.ConfigEntry) []kafka.ConfigEntry {
	retention, segment := lib.effectiveRetention(unprefixedTopic)
	if !hasConfigEntry(entries, configRetentionMS) && retention > 0 {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  configRetentionMS,
			ConfigValue: strconv.FormatInt(retention.Milliseconds(), 10),
		})
	}
	if !hasConfigEntry(entries, configSegmentMS) && segment > 0 {
		entries = append(entries, kafka.ConfigEntry{
			ConfigName:  configSegmentMS,
			ConfigValue: strconv.FormatInt(segment.Milliseconds(), 10),
		})
	}
	lib.warnRetentionEntries(ctx, unprefixedTopic, entries)
	return entries
}

func (lib *LibraryNoDB) retentionAlterConfigs(unprefixedTopic string) []kafka.IncrementalAlterConfigsRequestConfig {
	var configs []kafka.IncrementalAlterConfigsRequestConfig
	retention, segment := "", ""
	haveRetention, haveSegment := false, false
	if tc, ok := lib.getTopicConfig(unprefixedTopic); ok {
		if value, ok := configEntryValue(tc.ConfigEntries, configRetentionMS); ok {
			retention, haveRetention = value, true
		}
		if value, ok := configEntryValue(tc.ConfigEntries, configSegmentMS); ok {
			segment, haveSegment = value, true
		}
	}
	effectiveRetention, effectiveSegment := lib.effectiveRetention(unprefixedTopic)
	if !haveRetention && effectiveRetention > 0 {
		retention = strconv.FormatInt(effectiveRetention.Milliseconds(), 10)
		haveRetention = true
	}
	if !haveSegment && effectiveSegment > 0 {
		segment = strconv.FormatInt(effectiveSegment.Milliseconds(), 10)
		haveSegment = true
	}
	if haveRetention {
		configs = append(configs, kafka.IncrementalAlterConfigsRequestConfig{
			Name:            configRetentionMS,
			Value:           retention,
			ConfigOperation: kafka.ConfigOperationSet,
		})
	}
	if haveSegment {
		configs = append(configs, kafka.IncrementalAlterConfigsRequestConfig{
			Name:            configSegmentMS,
			Value:           segment,
			ConfigOperation: kafka.ConfigOperationSet,
		})
	}
	return configs
}

// prepareDeadLetterTopicConfig copies the base topic's create-time configuration
// and retention override onto the unprefixed dead-letter topic name. Defaults
// still apply later in ItemWork when neither was set.
func (lib *LibraryNoDB) prepareDeadLetterTopicConfig(baseTopic, deadLetterTopic string) {
	if config, ok := lib.getTopicConfig(baseTopic); ok {
		config.Topic = deadLetterTopic
		config.ConfigEntries = append([]kafka.ConfigEntry(nil), config.ConfigEntries...)
		lib.SetTopicConfig(config)
	}
	lib.copyRetentionOverride(baseTopic, deadLetterTopic)
}

func (lib *LibraryNoDB) copyRetentionOverride(from, to string) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	ov, ok := lib.topicRetention[from]
	if !ok {
		return
	}
	if lib.topicRetention == nil {
		lib.topicRetention = make(map[string]topicRetentionConfig)
	}
	lib.topicRetention[to] = ov
}

func (lib *LibraryNoDB) warnRetentionEntries(ctx context.Context, unprefixedTopic string, entries []kafka.ConfigEntry) {
	retention, haveRetention := configEntryMillis(entries, configRetentionMS)
	segment, haveSegment := configEntryMillis(entries, configSegmentMS)
	if !haveRetention || !haveSegment || retention <= 0 || segment <= 0 {
		return
	}
	if segment > retention {
		lib.logf(ctx, "[events] topic %s segment.ms (%d) is greater than retention.ms (%d); a segment can outlive the retention window", unprefixedTopic, segment, retention)
	}
}

func (lib *LibraryNoDB) warnIfSegmentExceedsRetention(ctx context.Context, topic string, retention, segment time.Duration) {
	if retention <= 0 || segment <= 0 || segment <= retention {
		return
	}
	lib.logf(ctx, "[events] topic %s segment (%s) is greater than retention (%s); a segment can outlive the retention window", topic, segment, retention)
}

func hasConfigEntry(entries []kafka.ConfigEntry, name string) bool {
	_, ok := configEntryValue(entries, name)
	return ok
}

func configEntryValue(entries []kafka.ConfigEntry, name string) (string, bool) {
	for _, entry := range entries {
		if entry.ConfigName == name {
			return entry.ConfigValue, true
		}
	}
	return "", false
}

func configEntryMillis(entries []kafka.ConfigEntry, name string) (int64, bool) {
	value, ok := configEntryValue(entries, name)
	if !ok || value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}
