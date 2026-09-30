package events

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
)

func TestProcessedTrimCutoffAddsDeadLetterWindow(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	partitions := []kafka.Partition{
		{Topic: "app.orders", Leader: kafka.Broker{ID: 1}},
		{Topic: "app.orders.workers.dead-letter", Leader: kafka.Broker{ID: 1}},
		{Topic: "app.orders.other.dead-letter", Leader: kafka.Broker{ID: 1}},
	}
	configs := map[string]processedTrimTopicConfig{
		"app.orders": {
			values: trimConfigValues(48*time.Hour, time.Hour),
		},
		"app.orders.workers.dead-letter": {
			values: trimConfigValues(24*time.Hour, 10*time.Hour),
		},
		"app.orders.other.dead-letter": {
			values: trimConfigValues(time.Hour, time.Hour),
		},
	}

	// original 49h + longest dead letter 34h + margin 6h
	cutoff, reason := processedTrimCutoff(
		now,
		6*time.Hour,
		1,
		"app.orders",
		[]string{"app.orders", "app.orders.workers.dead-letter", "app.orders.other.dead-letter"},
		partitions,
		configs,
	)

	require.Empty(t, reason)
	require.Equal(t, now.Add(-89*time.Hour), cutoff)

	cutoff, reason = processedTrimCutoff(
		now,
		6*time.Hour,
		2,
		"app.orders",
		[]string{"app.orders", "app.orders.workers.dead-letter", "app.orders.other.dead-letter"},
		partitions,
		configs,
	)

	require.Empty(t, reason)
	require.Equal(t, now.Add(-172*time.Hour), cutoff)
}

func TestProcessedTrimCutoffDeletesRowsWhenTopicFamilyIsGone(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

	cutoff, reason := processedTrimCutoff(now, 6*time.Hour, 2, "app.orders", nil, nil, nil)

	require.Empty(t, reason)
	require.Equal(t, processedTrimDeleteAll, cutoff)
}

func TestProcessedTrimCutoffDeletesRowsWhenBaseTopicIsGone(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	partitions := []kafka.Partition{
		{Topic: "app.orders.workers.dead-letter", Leader: kafka.Broker{ID: 1}},
	}
	configs := map[string]processedTrimTopicConfig{
		"app.orders.workers.dead-letter": {
			values: trimConfigValues(24*time.Hour, 10*time.Hour),
		},
	}

	cutoff, reason := processedTrimCutoff(
		now,
		6*time.Hour,
		1,
		"app.orders",
		[]string{"app.orders.workers.dead-letter"},
		partitions,
		configs,
	)

	require.Empty(t, reason)
	require.Equal(t, processedTrimDeleteAll, cutoff)

	cutoff, reason = processedTrimCutoff(
		now,
		6*time.Hour,
		1,
		"app.orders",
		[]string{"app.orders", "app.orders.workers.dead-letter"},
		partitions,
		map[string]processedTrimTopicConfig{
			"app.orders": {err: kafka.UnknownTopicOrPartition},
			"app.orders.workers.dead-letter": {
				values: trimConfigValues(24*time.Hour, 10*time.Hour),
			},
		},
	)

	require.Empty(t, reason)
	require.Equal(t, processedTrimDeleteAll, cutoff)
}

func TestProcessedTrimCutoffIgnoresDeadLetterMissingFromKafka(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	partitions := []kafka.Partition{
		{Topic: "app.orders", Leader: kafka.Broker{ID: 1}},
		{Topic: "app.orders.workers.dead-letter", Leader: kafka.Broker{ID: 1}},
	}

	// original 49h + margin 6h; the unknown dead-letter topic adds nothing
	cutoff, reason := processedTrimCutoff(
		now,
		6*time.Hour,
		1,
		"app.orders",
		[]string{"app.orders", "app.orders.workers.dead-letter"},
		partitions,
		map[string]processedTrimTopicConfig{
			"app.orders": {
				values: trimConfigValues(48*time.Hour, time.Hour),
			},
			"app.orders.workers.dead-letter": {err: kafka.UnknownTopicOrPartition},
		},
	)

	require.Empty(t, reason)
	require.Equal(t, now.Add(-55*time.Hour), cutoff)
}

func TestProcessedTrimCutoffSkipsUnsafeTopics(t *testing.T) {
	now := time.Now()
	partition := kafka.Partition{Topic: "orders", Leader: kafka.Broker{ID: 1}}

	tests := []struct {
		name   string
		values map[string]string
		reason string
	}{
		{
			name:   "unlimited retention",
			values: trimConfigValuesMS("-1", "1000", topicTimestampLogAppendTime, "delete"),
			reason: trimSkipUnlimited,
		},
		{
			name:   "create time",
			values: trimConfigValuesMS("1000", "1000", "CreateTime", "delete"),
			reason: trimSkipTimestampType,
		},
		{
			name:   "compact only",
			values: trimConfigValuesMS("1000", "1000", topicTimestampLogAppendTime, "compact"),
			reason: trimSkipCleanupPolicy,
		},
		{
			name:   "invalid retention",
			values: trimConfigValuesMS("invalid", "1000", topicTimestampLogAppendTime, "delete"),
			reason: trimSkipInvalidConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, reason := processedTrimCutoff(
				now,
				time.Hour,
				1,
				"orders",
				[]string{"orders"},
				[]kafka.Partition{partition},
				map[string]processedTrimTopicConfig{
					"orders": {values: tt.values},
				},
			)
			require.Equal(t, tt.reason, reason)
		})
	}

	partition.Leader.ID = -1
	_, reason := processedTrimCutoff(
		now,
		time.Hour,
		1,
		"orders",
		[]string{"orders"},
		[]kafka.Partition{partition},
		map[string]processedTrimTopicConfig{
			"orders": {values: trimConfigValues(time.Hour, time.Hour)},
		},
	)
	require.Equal(t, trimSkipOfflinePartition, reason)
}

func TestTrimProcessedEventsDeletesRowsWhenTopicIsGone(t *testing.T) {
	db := &processedTrimTestDB{
		NoDB:      &NoDB{},
		dbTopics:  []string{"orders"},
		remaining: 10,
	}
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *processedTrimTestDB]()
	lib.Configure(db, nil, false, nil, nil, []string{"unused"})
	lib.prefix = "app."
	lib.processedTrimKafkaPartitions = func(context.Context) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: "app.other", Leader: kafka.Broker{ID: 1}}}, nil
	}
	lib.processedTrimKafkaConfigs = func(_ context.Context, kafkaTopics []string) (map[string]processedTrimTopicConfig, error) {
		require.Empty(t, kafkaTopics)
		return map[string]processedTrimTopicConfig{}, nil
	}

	report, err := lib.TrimProcessedEvents(context.Background(), time.Hour, 1)

	require.NoError(t, err)
	require.Equal(t, map[string]int{"orders": 10}, report.Deleted)
	require.Empty(t, report.Skipped)
	require.Equal(t, []time.Time{processedTrimDeleteAll}, db.cutoffs)
}

func TestTrimProcessedEventsUsesProcessedAtInBatches(t *testing.T) {
	db := &processedTrimTestDB{
		NoDB:      &NoDB{},
		dbTopics:  []string{"orders"},
		remaining: 1250,
	}
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *processedTrimTestDB]()
	lib.Configure(db, nil, false, nil, nil, []string{"unused"})
	lib.prefix = "app."
	lib.processedTrimKafkaPartitions = func(context.Context) ([]kafka.Partition, error) {
		return []kafka.Partition{
			{Topic: "app.orders", Leader: kafka.Broker{ID: 1}},
			{Topic: "app.orders.group.dead-letter", Leader: kafka.Broker{ID: 1}},
		}, nil
	}
	lib.processedTrimKafkaConfigs = func(_ context.Context, kafkaTopics []string) (map[string]processedTrimTopicConfig, error) {
		require.ElementsMatch(t, []string{"app.orders", "app.orders.group.dead-letter"}, kafkaTopics)
		return map[string]processedTrimTopicConfig{
			"app.orders": {
				values: trimConfigValues(48*time.Hour, time.Hour),
			},
			"app.orders.group.dead-letter": {
				values: trimConfigValues(24*time.Hour, 10*time.Hour),
			},
		}, nil
	}

	before := time.Now().Add(-89 * time.Hour)
	report, err := lib.TrimProcessedEvents(context.Background(), 6*time.Hour, 1)
	after := time.Now().Add(-89 * time.Hour)

	require.NoError(t, err)
	require.Equal(t, map[string]int{"orders": 1250}, report.Deleted)
	require.Empty(t, report.Skipped)
	require.Len(t, db.cutoffs, 2)
	require.Len(t, db.batchSizes, 2)
	require.Equal(t, []int{processedTrimBatchSize, processedTrimBatchSize}, db.batchSizes)
	require.False(t, db.cutoffs[0].Before(before))
	require.False(t, db.cutoffs[0].After(after))
}

type processedTrimTestDB struct {
	*NoDB
	dbTopics   []string
	remaining  int
	cutoffs    []time.Time
	batchSizes []int
}

func (db *processedTrimTestDB) ProcessedTopics(context.Context) ([]string, error) {
	return db.dbTopics, nil
}

func (db *processedTrimTestDB) TrimProcessedEvents(_ context.Context, _ string, olderThan time.Time, batchSize int) (int, error) {
	db.cutoffs = append(db.cutoffs, olderThan)
	db.batchSizes = append(db.batchSizes, batchSize)
	n := batchSize
	if db.remaining < n {
		n = db.remaining
	}
	db.remaining -= n
	return n, nil
}

func trimConfigValues(retention, segment time.Duration) map[string]string {
	return trimConfigValuesMS(
		timeDurationMilliseconds(retention),
		timeDurationMilliseconds(segment),
		topicTimestampLogAppendTime,
		"delete",
	)
}

func trimConfigValuesMS(retention, segment, timestampType, cleanupPolicy string) map[string]string {
	return map[string]string{
		topicConfigRetentionMS:   retention,
		topicConfigSegmentMS:     segment,
		topicConfigTimestampType: timestampType,
		topicConfigCleanupPolicy: cleanupPolicy,
	}
}

func timeDurationMilliseconds(value time.Duration) string {
	return strconv.FormatInt(value.Milliseconds(), 10)
}
