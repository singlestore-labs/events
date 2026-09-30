package events

import (
	"context"
	stderrors "errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
)

const (
	processedTrimBatchSize     = 1000
	processedTrimDescribeBatch = 20

	topicConfigRetentionMS      = "retention.ms"
	topicConfigSegmentMS        = "segment.ms"
	topicConfigTimestampType    = "message.timestamp.type"
	topicConfigCleanupPolicy    = "cleanup.policy"
	topicTimestampLogAppendTime = "LogAppendTime"

	trimSkipUnreadableConfig = "unreadable_config"
	trimSkipInvalidConfig    = "invalid_config"
	trimSkipUnlimited        = "retention_unlimited"
	trimSkipTimestampType    = "timestamp_type"
	trimSkipCleanupPolicy    = "cleanup_policy"
	trimSkipOfflinePartition = "offline_partition"
)

// processedTrimDeleteAll is later than any processedAt. A cutoff of this value
// removes every eventsProcessed row for a topic that is no longer in Kafka.
var processedTrimDeleteAll = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// ProcessedTrimReport describes one TrimProcessedEvents pass. Deleted contains
// the number of rows removed for each base topic. Skipped contains topics that
// could not be trimmed safely and the reason they were left unchanged.
type ProcessedTrimReport struct {
	Deleted map[string]int
	Skipped map[string]string
}

// TrimProcessedEvents deletes eventsProcessed rows based on their processedAt
// time and the effective Kafka retention window.
//
// Each Kafka topic keeps its own retention.ms and segment.ms together. A row
// is deleted only when:
//
//	processedAt < now - factor*(original + deadLetter) - margin
//
// original is retention.ms + segment.ms of the base topic.
// deadLetter is the longest retention.ms + segment.ms among that topic's dead-letter
// topics that are still in Kafka, or zero when none exist.
func (lib *Library[ID, TX, DB]) TrimProcessedEvents(ctx context.Context, margin time.Duration, factor float64) (ProcessedTrimReport, error) {
	report := ProcessedTrimReport{
		Deleted: make(map[string]int),
		Skipped: make(map[string]string),
	}
	if margin < 0 {
		return report, errors.Errorf("processed event trim margin must not be negative")
	}
	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return report, errors.Errorf("processed event trim factor must be positive")
	}
	if err := lib.start(ctx, "trim processed events"); err != nil {
		return report, err
	}
	dbTopics, err := lib.db.ProcessedTopics(ctx)
	if err != nil {
		return report, errors.Errorf("list eventsProcessed topics: %w", err)
	}
	if len(dbTopics) == 0 {
		return report, nil
	}
	kafkaPartitions, err := lib.listProcessedTrimPartitions(ctx)
	if err != nil {
		return report, err
	}

	// kafkaFamilies is keyed by the unprefixed eventsProcessed topic. Each value is
	// the prefixed Kafka base topic plus any matching dead-letter topics that
	// currently exist in Kafka.
	kafkaFamilies := make(map[string][]string, len(dbTopics))
	kafkaTopics := make([]string, 0, len(dbTopics))
	seenKafkaTopics := make(map[string]bool)
	for _, dbTopic := range dbTopics {
		kafkaBaseTopic := lib.addPrefix(dbTopic)
		kafkaTopicFamily := processedTrimKafkaTopicFamily(kafkaBaseTopic, kafkaPartitions)
		kafkaFamilies[dbTopic] = kafkaTopicFamily
		for _, kafkaTopic := range kafkaTopicFamily {
			if !seenKafkaTopics[kafkaTopic] {
				seenKafkaTopics[kafkaTopic] = true
				kafkaTopics = append(kafkaTopics, kafkaTopic)
			}
		}
	}
	kafkaConfigs, err := lib.describeProcessedTrimConfigs(ctx, kafkaTopics)
	if err != nil {
		return report, err
	}

	now := time.Now()
	for _, dbTopic := range dbTopics {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		kafkaBaseTopic := lib.addPrefix(dbTopic)
		cutoff, reason := processedTrimCutoff(now, margin, factor, kafkaBaseTopic, kafkaFamilies[dbTopic], kafkaPartitions, kafkaConfigs)
		if reason != "" {
			report.Skipped[dbTopic] = reason
			continue
		}
		deleted, err := lib.trimProcessedBatches(ctx, dbTopic, cutoff)
		report.Deleted[dbTopic] = deleted
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

type processedTrimTopicConfig struct {
	values map[string]string
	err    error
}

func (lib *LibraryNoDB) listProcessedTrimPartitions(ctx context.Context) ([]kafka.Partition, error) {
	if lib.processedTrimKafkaPartitions != nil {
		return lib.processedTrimKafkaPartitions(ctx)
	}
	var lastErr error
	for _, broker := range lib.brokers {
		conn, err := lib.dialer().DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}
		partitions, err := conn.ReadPartitions()
		_ = conn.Close()
		if err == nil {
			return partitions, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.Errorf("no brokers configured")
	}
	return nil, errors.Errorf("list Kafka topics for processed-event trim: %w", lastErr)
}

func (lib *LibraryNoDB) describeProcessedTrimConfigs(ctx context.Context, kafkaTopics []string) (map[string]processedTrimTopicConfig, error) {
	if lib.processedTrimKafkaConfigs != nil {
		return lib.processedTrimKafkaConfigs(ctx, kafkaTopics)
	}
	out := make(map[string]processedTrimTopicConfig, len(kafkaTopics))
	if len(kafkaTopics) == 0 {
		return out, nil
	}
	client, err := lib.getController(ctx)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(kafkaTopics); start += processedTrimDescribeBatch {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + processedTrimDescribeBatch
		if end > len(kafkaTopics) {
			end = len(kafkaTopics)
		}
		resources := make([]kafka.DescribeConfigRequestResource, end-start)
		for i, kafkaTopic := range kafkaTopics[start:end] {
			resources[i] = kafka.DescribeConfigRequestResource{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: kafkaTopic,
				ConfigNames: []string{
					topicConfigRetentionMS,
					topicConfigSegmentMS,
					topicConfigTimestampType,
					topicConfigCleanupPolicy,
				},
			}
		}
		response, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{Resources: resources})
		if err != nil {
			return nil, errors.Errorf("describe Kafka topics for processed-event trim: %w", err)
		}
		if response == nil {
			return nil, errors.Errorf("describe Kafka topics for processed-event trim: empty response")
		}
		for _, resource := range response.Resources {
			config := processedTrimTopicConfig{
				values: make(map[string]string),
				err:    resource.Error,
			}
			for _, entry := range resource.ConfigEntries {
				config.values[entry.ConfigName] = entry.ConfigValue
			}
			out[resource.ResourceName] = config
		}
	}
	return out, nil
}

func processedTrimKafkaTopicFamily(kafkaBaseTopic string, kafkaPartitions []kafka.Partition) []string {
	var kafkaTopics []string
	seenKafkaTopics := make(map[string]bool)
	deadLetterPrefix := kafkaBaseTopic + "."
	for _, partition := range kafkaPartitions {
		kafkaTopic := partition.Topic
		isDeadLetter := strings.HasPrefix(kafkaTopic, deadLetterPrefix) &&
			strings.HasSuffix(kafkaTopic, deadLetterTopicPostfix) &&
			len(kafkaTopic) > len(deadLetterPrefix)+len(deadLetterTopicPostfix)
		if (kafkaTopic == kafkaBaseTopic || isDeadLetter) && !seenKafkaTopics[kafkaTopic] {
			seenKafkaTopics[kafkaTopic] = true
			kafkaTopics = append(kafkaTopics, kafkaTopic)
		}
	}
	return kafkaTopics
}

func processedTrimCutoff(
	now time.Time,
	margin time.Duration,
	factor float64,
	kafkaBaseTopic string,
	kafkaTopicFamily []string,
	kafkaPartitions []kafka.Partition,
	kafkaConfigs map[string]processedTrimTopicConfig,
) (time.Time, string) {
	if !processedTrimKafkaFamilyContains(kafkaTopicFamily, kafkaBaseTopic) {
		// The database topic has no matching Kafka base topic, so every processed
		// row can go. A dead-letter topic that is still present does not keep these rows.
		return processedTrimDeleteAll, ""
	}
	var originalWindow time.Duration
	var deadLetterWindow time.Duration
	for _, kafkaTopic := range kafkaTopicFamily {
		if processedTrimKafkaTopicOffline(kafkaTopic, kafkaPartitions) {
			return time.Time{}, trimSkipOfflinePartition
		}
		config, ok := kafkaConfigs[kafkaTopic]
		if processedTrimKafkaTopicAbsent(config, ok) {
			if kafkaTopic == kafkaBaseTopic {
				return processedTrimDeleteAll, ""
			}
			continue
		}
		if config.err != nil {
			return time.Time{}, trimSkipUnreadableConfig
		}
		retention, segment, reason := parseProcessedTrimConfig(config.values)
		if reason != "" {
			return time.Time{}, reason
		}
		window := retention + segment
		if kafkaTopic == kafkaBaseTopic {
			originalWindow = window
			continue
		}
		if window > deadLetterWindow {
			deadLetterWindow = window
		}
	}
	total := originalWindow + deadLetterWindow
	if deadLetterWindow > 0 && total < originalWindow {
		return time.Time{}, trimSkipInvalidConfig
	}
	scaled := float64(total) * factor
	if scaled > float64(math.MaxInt64) {
		return time.Time{}, trimSkipInvalidConfig
	}
	return now.Add(-time.Duration(scaled) - margin), ""
}

func processedTrimKafkaFamilyContains(kafkaTopicFamily []string, kafkaBaseTopic string) bool {
	for _, kafkaTopic := range kafkaTopicFamily {
		if kafkaTopic == kafkaBaseTopic {
			return true
		}
	}
	return false
}

func processedTrimKafkaTopicAbsent(config processedTrimTopicConfig, ok bool) bool {
	if !ok {
		return true
	}
	return stderrors.Is(config.err, kafka.UnknownTopicOrPartition)
}

func processedTrimKafkaTopicOffline(kafkaTopic string, kafkaPartitions []kafka.Partition) bool {
	for _, partition := range kafkaPartitions {
		if partition.Topic == kafkaTopic && partition.Leader.ID < 0 {
			return true
		}
	}
	return false
}

func parseProcessedTrimConfig(values map[string]string) (time.Duration, time.Duration, string) {
	retentionRaw := strings.TrimSpace(values[topicConfigRetentionMS])
	segmentRaw := strings.TrimSpace(values[topicConfigSegmentMS])
	timestampType := strings.TrimSpace(values[topicConfigTimestampType])
	cleanupPolicy := strings.TrimSpace(values[topicConfigCleanupPolicy])
	if retentionRaw == "" || segmentRaw == "" || timestampType == "" || cleanupPolicy == "" {
		return 0, 0, trimSkipInvalidConfig
	}
	retentionMS, err := strconv.ParseInt(retentionRaw, 10, 64)
	maxDurationMillis := int64(math.MaxInt64) / int64(time.Millisecond)
	if err != nil || retentionMS < -1 || retentionMS > maxDurationMillis {
		return 0, 0, trimSkipInvalidConfig
	}
	if retentionMS == -1 {
		return 0, 0, trimSkipUnlimited
	}
	segmentMS, err := strconv.ParseInt(segmentRaw, 10, 64)
	if err != nil || segmentMS <= 0 || segmentMS > maxDurationMillis {
		return 0, 0, trimSkipInvalidConfig
	}
	if timestampType != topicTimestampLogAppendTime {
		return 0, 0, trimSkipTimestampType
	}
	if !processedTrimDeletes(cleanupPolicy) {
		return 0, 0, trimSkipCleanupPolicy
	}
	return time.Duration(retentionMS) * time.Millisecond, time.Duration(segmentMS) * time.Millisecond, ""
}

func processedTrimDeletes(policy string) bool {
	for _, value := range strings.Split(policy, ",") {
		if strings.TrimSpace(value) == "delete" {
			return true
		}
	}
	return false
}

func (lib *Library[ID, TX, DB]) trimProcessedBatches(ctx context.Context, dbTopic string, olderThan time.Time) (int, error) {
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := lib.db.TrimProcessedEvents(ctx, dbTopic, olderThan, processedTrimBatchSize)
		total += n
		if err != nil {
			return total, errors.Errorf("trim eventsProcessed topic %s: %w", dbTopic, err)
		}
		if n < processedTrimBatchSize {
			return total, nil
		}
	}
}
