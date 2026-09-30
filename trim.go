package events

import (
	"context"
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

	processedTrimConfigRetentionMS   = "retention.ms"
	processedTrimConfigSegmentMS     = "segment.ms"
	processedTrimConfigTimestampType = "message.timestamp.type"
	processedTrimConfigCleanupPolicy = "cleanup.policy"
	processedTrimLogAppendTime       = "LogAppendTime"

	trimSkipMissingTopic     = "missing_topic"
	trimSkipUnreadableConfig = "unreadable_config"
	trimSkipInvalidConfig    = "invalid_config"
	trimSkipUnlimited        = "retention_unlimited"
	trimSkipTimestampType    = "timestamp_type"
	trimSkipCleanupPolicy    = "cleanup_policy"
	trimSkipOfflinePartition = "offline_partition"
)

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
// For each base topic, the retention window is the largest retention.ms plus
// the largest segment.ms across the base topic and its dead-letter topics. A
// row is deleted only when:
//
//	processedAt < now - retention - segment - margin
//
// margin must be non-negative and should cover the Kafka retention check
// interval, clock skew, and the maximum expected delay before a duplicate or
// dead-letter copy is written. Topics with unsafe or unreadable settings are
// reported as skipped.
func (lib *Library[ID, TX, DB]) TrimProcessedEvents(ctx context.Context, margin time.Duration) (ProcessedTrimReport, error) {
	report := ProcessedTrimReport{
		Deleted: make(map[string]int),
		Skipped: make(map[string]string),
	}
	if margin < 0 {
		return report, errors.Errorf("processed event trim margin must not be negative")
	}
	if err := lib.start(ctx, "trim processed events"); err != nil {
		return report, err
	}
	topics, err := lib.db.ProcessedTopics(ctx)
	if err != nil {
		return report, errors.Errorf("list eventsProcessed topics: %w", err)
	}
	if len(topics) == 0 {
		return report, nil
	}
	partitions, err := lib.listProcessedTrimPartitions(ctx)
	if err != nil {
		return report, err
	}

	families := make(map[string][]string, len(topics))
	allNames := make([]string, 0, len(topics))
	seen := make(map[string]bool)
	for _, topic := range topics {
		names := processedTrimTopicFamily(lib.addPrefix(topic), partitions)
		families[topic] = names
		for _, name := range names {
			if !seen[name] {
				seen[name] = true
				allNames = append(allNames, name)
			}
		}
	}
	configs, err := lib.describeProcessedTrimConfigs(ctx, allNames)
	if err != nil {
		return report, err
	}

	now := time.Now()
	for _, topic := range topics {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		cutoff, reason := processedTrimCutoff(now, margin, families[topic], partitions, configs)
		if reason != "" {
			report.Skipped[topic] = reason
			continue
		}
		deleted, err := lib.trimProcessedBatches(ctx, topic, cutoff)
		report.Deleted[topic] = deleted
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
	if lib.processedTrimPartitions != nil {
		return lib.processedTrimPartitions(ctx)
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

func (lib *LibraryNoDB) describeProcessedTrimConfigs(ctx context.Context, names []string) (map[string]processedTrimTopicConfig, error) {
	if lib.processedTrimConfigs != nil {
		return lib.processedTrimConfigs(ctx, names)
	}
	out := make(map[string]processedTrimTopicConfig, len(names))
	if len(names) == 0 {
		return out, nil
	}
	client, err := lib.getController(ctx)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(names); start += processedTrimDescribeBatch {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + processedTrimDescribeBatch
		if end > len(names) {
			end = len(names)
		}
		resources := make([]kafka.DescribeConfigRequestResource, end-start)
		for i, name := range names[start:end] {
			resources[i] = kafka.DescribeConfigRequestResource{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: name,
				ConfigNames: []string{
					processedTrimConfigRetentionMS,
					processedTrimConfigSegmentMS,
					processedTrimConfigTimestampType,
					processedTrimConfigCleanupPolicy,
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

func processedTrimTopicFamily(base string, partitions []kafka.Partition) []string {
	var names []string
	seen := make(map[string]bool)
	deadLetterPrefix := base + "."
	for _, partition := range partitions {
		name := partition.Topic
		isDeadLetter := strings.HasPrefix(name, deadLetterPrefix) &&
			strings.HasSuffix(name, deadLetterTopicPostfix) &&
			len(name) > len(deadLetterPrefix)+len(deadLetterTopicPostfix)
		if (name == base || isDeadLetter) && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names
}

func processedTrimCutoff(
	now time.Time,
	margin time.Duration,
	names []string,
	partitions []kafka.Partition,
	configs map[string]processedTrimTopicConfig,
) (time.Time, string) {
	if len(names) == 0 {
		return time.Time{}, trimSkipMissingTopic
	}
	var maxRetention time.Duration
	var maxSegment time.Duration
	for _, name := range names {
		if processedTrimTopicOffline(name, partitions) {
			return time.Time{}, trimSkipOfflinePartition
		}
		config, ok := configs[name]
		if !ok || config.err != nil {
			return time.Time{}, trimSkipUnreadableConfig
		}
		retention, segment, reason := parseProcessedTrimConfig(config.values)
		if reason != "" {
			return time.Time{}, reason
		}
		if retention > maxRetention {
			maxRetention = retention
		}
		if segment > maxSegment {
			maxSegment = segment
		}
	}
	return now.Add(-maxRetention - maxSegment - margin), ""
}

func processedTrimTopicOffline(name string, partitions []kafka.Partition) bool {
	for _, partition := range partitions {
		if partition.Topic == name && partition.Leader.ID < 0 {
			return true
		}
	}
	return false
}

func parseProcessedTrimConfig(values map[string]string) (time.Duration, time.Duration, string) {
	retentionRaw := strings.TrimSpace(values[processedTrimConfigRetentionMS])
	segmentRaw := strings.TrimSpace(values[processedTrimConfigSegmentMS])
	timestampType := strings.TrimSpace(values[processedTrimConfigTimestampType])
	cleanupPolicy := strings.TrimSpace(values[processedTrimConfigCleanupPolicy])
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
	if timestampType != processedTrimLogAppendTime {
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

func (lib *Library[ID, TX, DB]) trimProcessedBatches(ctx context.Context, topic string, olderThan time.Time) (int, error) {
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := lib.db.TrimProcessedEvents(ctx, topic, olderThan, processedTrimBatchSize)
		total += n
		if err != nil {
			return total, errors.Errorf("trim eventsProcessed topic %s: %w", topic, err)
		}
		if n < processedTrimBatchSize {
			return total, nil
		}
	}
}
