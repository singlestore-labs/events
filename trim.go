package events

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/events/eventmodels"
)

// TrimExactlyOnceDeliveryRecords deletes exactly-once delivery records corresponding to messages that
// should no longer exist in Kafka and thus won't be re-presented.
//
// Records are kept in the duration of
// - the original topic's retention.ms + segment.ms,
// - the longest dead-letter retention.ms + segment.ms,
// - the broker's log.retention.check.interval.ms,
// - margin.
// Those durations come from the configs Kafka reports, including for dead-letter topics created from the original.
// The cutoff is measured against the clock of the server running this code,
//
// Topic configs come from the last UpdateTopicConfigs call or an earlier trim; topics without
// them are described from Kafka. Configs changed directly in Kafka are not seen until
// UpdateTopicConfigs runs again.
//
// margin is extra time to keep records. It could help to cover some edge cases
// - clock differences between the Kafka brokers and the database.
// - database client and server time difference
//
// batchSize is the number of rows deleted per statement.
// batchInterval is the pause after a full batch. batchInterval == 0 does not pause.
//
// NOTE: Trim does not support topic with message.timestamp.type = "CreateTime", it skip trim those topic.
func (lib *Library[ID, TX, DB]) TrimExactlyOnceDeliveryRecords(ctx context.Context, margin time.Duration, batchSize int, batchInterval time.Duration) error {
	if margin < 0 {
		return errors.Errorf("trim margin %s must not be negative", margin)
	}
	if batchSize <= 0 {
		return errors.Errorf("trim batch size %d must be positive", batchSize)
	}
	if batchInterval < 0 {
		return errors.Errorf("trim batch interval %s must not be negative", batchInterval)
	}
	if !lib.HasDB() {
		return errors.Errorf("event library trim requires a database")
	}
	exactlyOnceUnprefixedTopics := func() map[string][]string {
		lib.lock.Lock()
		defer lib.lock.Unlock()
		unprefixedTopics := make(map[string][]string)
		for groupName, group := range lib.readers {
			for topic, topicHandler := range group.topics {
				var exactlyOnce bool
				var needDeadLetterTopic bool
				for _, handler := range topicHandler.handlers {
					if !handler.exactlyOnce {
						continue
					}
					exactlyOnce = true
					switch handler.onFailure {
					case eventmodels.OnFailureRetryLater, eventmodels.OnFailureSave:
						needDeadLetterTopic = true
					}
				}
				if !exactlyOnce {
					continue
				}
				if _, ok := unprefixedTopics[topic]; !ok {
					unprefixedTopics[topic] = nil
				}
				if needDeadLetterTopic {
					unprefixedTopics[topic] = append(unprefixedTopics[topic], DeadLetterTopic(topic, groupName))
				}
			}
		}
		return unprefixedTopics
	}()
	if len(exactlyOnceUnprefixedTopics) == 0 {
		return nil
	}

	// Use the original topic's config only when the dead-letter topic has none.
	for original, deadLetters := range exactlyOnceUnprefixedTopics {
		for _, deadLetter := range deadLetters {
			lib.setupDeadLetterTopicConfigHelper(original, deadLetter)
		}
	}

	unprefixedTopicNames := func() []string {
		names := make([]string, 0, len(exactlyOnceUnprefixedTopics))
		for original, deadLetters := range exactlyOnceUnprefixedTopics {
			names = append(names, original)
			names = append(names, deadLetters...)
		}
		return names
	}()

	err := lib.createTopics(ctx, unprefixedTopicNames)
	if err != nil {
		return err
	}

	unprefixedTopicConfigs := make(map[string][]kafka.DescribeConfigResponseConfigEntry, len(unprefixedTopicNames))
	missingUnprefixedTopics := make([]string, 0, len(unprefixedTopicNames))
	for _, unprefixedTopic := range unprefixedTopicNames {
		if entries, ok := lib.getKafkaTopicConfig(unprefixedTopic); ok {
			unprefixedTopicConfigs[unprefixedTopic] = entries
		} else {
			missingUnprefixedTopics = append(missingUnprefixedTopics, unprefixedTopic)
		}
	}
	unprefixedDescribeErrs := make(map[string]error)
	if len(missingUnprefixedTopics) > 0 {
		resp, err := lib.describeTopicConfigs(ctx, missingUnprefixedTopics)
		if err != nil {
			return err
		}
		lib.setKafkaTopicConfig(resp.Resources)
		for _, resource := range resp.Resources {
			unprefixedTopic := lib.removePrefix(resource.ResourceName)
			if resource.Error != nil {
				unprefixedDescribeErrs[unprefixedTopic] = resource.Error
				continue
			}
			unprefixedTopicConfigs[unprefixedTopic] = resource.ConfigEntries
		}
	}
	getUnprefixedTopicEntries := func(unprefixedTopic string) ([]kafka.DescribeConfigResponseConfigEntry, error) {
		if entries, ok := unprefixedTopicConfigs[unprefixedTopic]; ok {
			return entries, nil
		}
		if err, ok := unprefixedDescribeErrs[unprefixedTopic]; ok {
			return nil, errors.Wrapf(err, "could not describe topic %s", unprefixedTopic)
		}
		return nil, errors.Errorf("not found topic %s", unprefixedTopic)
	}
	checkIntervalMs, err := lib.retentionCheckIntervalMs(ctx)
	if err != nil {
		return err
	}

	trimErrs := make([]error, 0)
	for unprefixedTopic, unprefixedDeadLetters := range exactlyOnceUnprefixedTopics {
		originalEntries, err := getUnprefixedTopicEntries(unprefixedTopic)
		if err != nil {
			trimErrs = append(trimErrs, err)
			continue
		}

		if !isLogAppendTime(originalEntries) {
			lib.logf(ctx, "[events] skip trim of topic %s, message.timestamp.type is not LogAppendTime", unprefixedTopic)
			continue
		}

		originalKeepMs, err := topicRetentionAndSegmentMs(originalEntries)
		if err != nil {
			trimErrs = append(trimErrs, errors.Wrapf(err, "could not trim topic %s", unprefixedTopic))
			continue
		}
		if originalKeepMs < 0 {
			lib.logf(ctx, "[events] skip trim of topic %s, retention.ms or segment.ms is unlimited or cleanup.policy is not delete", unprefixedTopic)
			continue
		}
		var longestDeadLetterMs int64
		skip := false
		for _, unprefixedDeadLetter := range unprefixedDeadLetters {
			deadLetterEntries, err := getUnprefixedTopicEntries(unprefixedDeadLetter)
			if err != nil {
				trimErrs = append(trimErrs, err)
				skip = true
				break
			}
			if !isLogAppendTime(deadLetterEntries) {
				lib.logf(ctx, "[events] skip trim of topic %s, dead letter topic %s message.timestamp.type is not LogAppendTime", unprefixedTopic, unprefixedDeadLetter)
				skip = true
				break
			}
			deadLetterKeepMs, err := topicRetentionAndSegmentMs(deadLetterEntries)
			if err != nil {
				trimErrs = append(trimErrs, errors.Wrapf(err, "could not trim topic %s", unprefixedTopic))
				skip = true
				break
			}
			if deadLetterKeepMs < 0 {
				lib.logf(ctx, "[events] skip trim of topic %s, dead letter topic %s retention.ms or segment.ms is unlimited or cleanup.policy is not delete", unprefixedTopic, unprefixedDeadLetter)
				skip = true
				break
			}
			if deadLetterKeepMs > longestDeadLetterMs {
				longestDeadLetterMs = deadLetterKeepMs
			}
		}
		if skip {
			continue
		}

		if originalKeepMs > math.MaxInt64-longestDeadLetterMs-checkIntervalMs {
			lib.logf(ctx, "[events] skip trim of topic %s, retention overflows", unprefixedTopic)
			continue
		}
		keepMs := originalKeepMs + longestDeadLetterMs + checkIntervalMs
		if keepMs > int64((math.MaxInt64-margin)/time.Millisecond) {
			lib.logf(ctx, "[events] skip trim of topic %s, retention plus margin overflows", unprefixedTopic)
			continue
		}
		keep := time.Duration(keepMs)*time.Millisecond + margin
		trimOlderThan := time.Now().Add(-keep)
		deleted, err := lib.db.TrimEventsProcessed(ctx, unprefixedTopic, trimOlderThan, batchSize, batchInterval)
		if err != nil {
			trimErrs = append(trimErrs, err)
			continue
		}
		lib.logf(ctx, "[events] trimmed %d exactly-once records for topic %q older than %s", deleted, unprefixedTopic, keep)
	}
	return errors.Join(trimErrs...)
}

// retentionCheckIntervalMs returns the controller's log.retention.check.interval.ms. Kafka only
// deletes expired segments when that check runs, so a message can stay readable for up to this long
// after its segment has passed retention.ms. Kafka reports the effective value, which is the
// default (300000) when the broker never set it.
// https://kafka.apache.org/documentation/#brokerconfigs_log.retention.check.interval.ms
//
// multiple brokers could have different intervals but it's very rare.
// And an solve by include the largest interval in margin time.
func (lib *LibraryNoDB) retentionCheckIntervalMs(ctx context.Context) (int64, error) {
	const configName = "log.retention.check.interval.ms"
	client, err := lib.getController(ctx)
	if err != nil {
		return 0, errors.Errorf("event library could not get kafka controller: %w", err)
	}
	// A broker only describes its own broker configs, so ask the controller about itself.
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return 0, errors.Errorf("could not get kafka metadata: %w", err)
	}
	resp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Resources: []kafka.DescribeConfigRequestResource{{
			ResourceType: kafka.ResourceTypeBroker,
			ResourceName: strconv.Itoa(meta.Controller.ID),
			ConfigNames:  []string{configName},
		}},
	})
	if err != nil {
		return 0, errors.Errorf("could not describe broker config %s: %w", configName, err)
	}

	for _, resource := range resp.Resources {
		if resource.Error != nil {
			return 0, errors.Errorf("could not describe broker config %s: %w", configName, resource.Error)
		}
		for _, entry := range resource.ConfigEntries {
			if entry.ConfigName != configName {
				continue
			}
			ms, err := strconv.ParseInt(entry.ConfigValue, 10, 64)
			if err != nil {
				return 0, errors.Errorf("broker config %s value %q: %w", configName, entry.ConfigValue, err)
			}
			if ms < 0 {
				return 0, errors.Errorf("broker config %s value %d must not be negative", configName, ms)
			}
			return ms, nil
		}
	}
	return 0, errors.Errorf("broker config %s is missing", configName)
}

// isLogAppendTime reports whether Kafka sets the message timestamps. With CreateTime, producers set
// them and a future timestamp can keep a segment past retention.
func isLogAppendTime(entries []kafka.DescribeConfigResponseConfigEntry) bool {
	return slices.ContainsFunc(entries, func(e kafka.DescribeConfigResponseConfigEntry) bool {
		return e.ConfigName == "message.timestamp.type" && e.ConfigValue == "LogAppendTime"
	})
}

// topicRetentionAndSegmentMs returns retention.ms + segment.ms. A negative result means no time limit.
// That includes any cleanup.policy other than delete alone: without delete, compacted messages are kept
// regardless of age; with compact, the log cleaner can merge segments, and time-based deletion of a merged
// segment waits for its newest message, so older messages can outlive retention.ms + segment.ms.
// https://kafka.apache.org/documentation/#topicconfigs_cleanup.policy
// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
// https://kafka.apache.org/documentation/#topicconfigs_segment.ms
func topicRetentionAndSegmentMs(entries []kafka.DescribeConfigResponseConfigEntry) (int64, error) {
	policyIndex := slices.IndexFunc(entries, func(e kafka.DescribeConfigResponseConfigEntry) bool { return e.ConfigName == "cleanup.policy" })
	if policyIndex < 0 {
		return 0, errors.Errorf("topic config cleanup.policy is missing")
	}
	policies := strings.Split(entries[policyIndex].ConfigValue, ",")
	hasPolicy := func(name string) bool {
		return slices.ContainsFunc(policies, func(p string) bool { return strings.TrimSpace(p) == name })
	}
	// should not trim if compact or no delete.
	if !hasPolicy("delete") || hasPolicy("compact") {
		return -1, nil
	}
	configMs := func(name string) (int64, error) {
		for _, entry := range entries {
			if entry.ConfigName != name {
				continue
			}
			ms, err := strconv.ParseInt(entry.ConfigValue, 10, 64)
			if err != nil {
				return 0, errors.Errorf("topic config %s value %q: %w", name, entry.ConfigValue, err)
			}
			return ms, nil
		}
		return 0, errors.Errorf("topic config %s is missing", name)
	}
	retention, err := configMs("retention.ms")
	if err != nil || retention < 0 {
		return retention, err
	}
	segment, err := configMs("segment.ms")
	if err != nil || segment < 0 {
		return segment, err
	}
	if retention > math.MaxInt64-segment {
		return -1, nil
	}
	return retention + segment, nil
}
