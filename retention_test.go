package events

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
)

func TestDefaultRetentionAppliedToNewTopics(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(48*time.Hour, 12*time.Hour)

	entries := lib.withRetentionConfig(context.Background(), "orders", nil)
	require.Equal(t, "172800000", configValue(t, entries, configRetentionMS))
	require.Equal(t, "43200000", configValue(t, entries, configSegmentMS))
}

func TestDefaultRetentionAppliedToDeadLetterTopics(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(48*time.Hour, 12*time.Hour)
	group := NewConsumerGroup("state-server")
	dead := DeadLetterTopic("orders", group)
	lib.prepareDeadLetterTopicConfig("orders", dead)

	entries := lib.withRetentionConfig(context.Background(), dead, nil)
	require.Equal(t, "172800000", configValue(t, entries, configRetentionMS))
	require.Equal(t, "43200000", configValue(t, entries, configSegmentMS))
}

func TestTopicRetentionOverridesDefaults(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(48*time.Hour, 12*time.Hour)
	lib.SetTopicRetention("orders", 6*time.Hour, time.Hour)

	entries := lib.withRetentionConfig(context.Background(), "orders", nil)
	require.Equal(t, "21600000", configValue(t, entries, configRetentionMS))
	require.Equal(t, "3600000", configValue(t, entries, configSegmentMS))

	group := NewConsumerGroup("state-server")
	dead := DeadLetterTopic("orders", group)
	lib.prepareDeadLetterTopicConfig("orders", dead)
	deadEntries := lib.withRetentionConfig(context.Background(), dead, nil)
	require.Equal(t, "21600000", configValue(t, deadEntries, configRetentionMS))
	require.Equal(t, "3600000", configValue(t, deadEntries, configSegmentMS))
}

func TestSetTopicConfigWinsOverRetentionSetters(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(48*time.Hour, 12*time.Hour)
	lib.SetTopicRetention("orders", 6*time.Hour, time.Hour)
	lib.SetTopicConfig(kafka.TopicConfig{
		Topic: "orders",
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: configRetentionMS, ConfigValue: "1000"},
		},
	})

	entries := lib.withRetentionConfig(context.Background(), "orders", []kafka.ConfigEntry{
		{ConfigName: configRetentionMS, ConfigValue: "1000"},
	})
	require.Equal(t, "1000", configValue(t, entries, configRetentionMS))
	require.Equal(t, "3600000", configValue(t, entries, configSegmentMS))
}

func TestSetTopicConfigDoesNotEraseRetentionOverride(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetTopicRetention("orders", 6*time.Hour, time.Hour)
	lib.SetTopicConfig(kafka.TopicConfig{Topic: "orders", NumPartitions: 4})

	entries := lib.withRetentionConfig(context.Background(), "orders", nil)
	require.Equal(t, "21600000", configValue(t, entries, configRetentionMS))
	require.Equal(t, "3600000", configValue(t, entries, configSegmentMS))
}

func TestNonPositiveRetentionLeavesBrokerDefault(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(48*time.Hour, 12*time.Hour)
	lib.SetTopicRetention("orders", 0, time.Hour)

	entries := lib.withRetentionConfig(context.Background(), "orders", nil)
	_, hasRetention := configEntryValue(entries, configRetentionMS)
	require.False(t, hasRetention)
	require.Equal(t, "3600000", configValue(t, entries, configSegmentMS))
}

func TestSegmentGreaterThanRetentionWarns(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	var logs []string
	lib.tracerProvider = func(context.Context) eventmodels.Tracer {
		return func(format string, args ...any) {
			logs = append(logs, format)
		}
	}
	lib.SetDefaultTopicRetention(time.Hour, 2*time.Hour)
	require.NotEmpty(t, logs)
	joined := strings.Join(logs, "\n")
	require.Contains(t, joined, "segment")
	require.Contains(t, joined, "retention")
}

func TestApplyTopicRetentionChangesExistingTopics(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetPrefix("test.")
	lib.SetDefaultTopicRetention(72*time.Hour, 24*time.Hour)
	admin := &fakeTopicAdmin{
		current: map[string]map[string]string{
			"test.orders": {
				configRetentionMS: "1000",
				configSegmentMS:   "2000",
			},
		},
	}
	lib.topicAdmin = admin
	lib.Configure(nil, func(context.Context) eventmodels.Tracer {
		return func(string, ...any) {}
	}, false, nil, nil, []string{"localhost:9092"})

	err := lib.ApplyTopicRetention(context.Background(), "orders")
	require.NoError(t, err)
	require.Len(t, admin.described, 1)
	require.Equal(t, "test.orders", admin.described[0])
	require.Len(t, admin.altered, 1)
	require.Equal(t, "test.orders", admin.altered[0].ResourceName)
	require.Equal(t, []kafka.IncrementalAlterConfigsRequestConfig{
		{Name: configRetentionMS, Value: "259200000", ConfigOperation: kafka.ConfigOperationSet},
		{Name: configSegmentMS, Value: "86400000", ConfigOperation: kafka.ConfigOperationSet},
	}, admin.altered[0].Configs)
}

func TestApplyTopicRetentionLeavesUnspecifiedDuration(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetTopicRetention("orders", 9*time.Hour, 0)
	admin := &fakeTopicAdmin{
		current: map[string]map[string]string{
			"orders": {configSegmentMS: "555"},
		},
	}
	lib.topicAdmin = admin
	lib.Configure(nil, func(context.Context) eventmodels.Tracer {
		return func(string, ...any) {}
	}, false, nil, nil, []string{"localhost:9092"})

	err := lib.ApplyTopicRetention(context.Background(), "orders")
	require.NoError(t, err)
	require.Equal(t, []kafka.IncrementalAlterConfigsRequestConfig{
		{Name: configRetentionMS, Value: "32400000", ConfigOperation: kafka.ConfigOperationSet},
	}, admin.altered[0].Configs)
}

func TestApplyTopicRetentionSkipsUnnamedDeadLetterTopics(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(time.Hour, time.Hour)
	group := NewConsumerGroup("state-server")
	dead := DeadLetterTopic("orders", group)
	admin := &fakeTopicAdmin{current: map[string]map[string]string{}}
	lib.topicAdmin = admin
	lib.Configure(nil, func(context.Context) eventmodels.Tracer {
		return func(string, ...any) {}
	}, false, nil, nil, []string{"localhost:9092"})

	err := lib.ApplyTopicRetention(context.Background(), "orders")
	require.NoError(t, err)
	for _, name := range admin.described {
		require.NotEqual(t, dead, name)
	}

	err = lib.ApplyTopicRetention(context.Background(), dead)
	require.NoError(t, err)
	require.Contains(t, admin.described, dead)
}

func TestApplyTopicRetentionReportsResourceErrors(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(time.Hour, 0)
	admin := &fakeTopicAdmin{
		current:       map[string]map[string]string{"orders": {configRetentionMS: "1"}},
		resourceError: errors.New("broker rejected"),
	}
	lib.topicAdmin = admin
	lib.Configure(nil, func(context.Context) eventmodels.Tracer {
		return func(string, ...any) {}
	}, false, nil, nil, []string{"localhost:9092"})

	err := lib.ApplyTopicRetention(context.Background(), "orders")
	require.Error(t, err)
	require.ErrorContains(t, err, "broker rejected")
}

func TestApplyTopicRetentionExplicitConfigWins(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	lib.SetDefaultTopicRetention(48*time.Hour, 12*time.Hour)
	lib.SetTopicConfig(kafka.TopicConfig{
		Topic: "orders",
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: configRetentionMS, ConfigValue: "1000"},
		},
	})
	admin := &fakeTopicAdmin{current: map[string]map[string]string{"orders": {}}}
	lib.topicAdmin = admin
	lib.Configure(nil, func(context.Context) eventmodels.Tracer {
		return func(string, ...any) {}
	}, false, nil, nil, []string{"localhost:9092"})

	err := lib.ApplyTopicRetention(context.Background(), "orders")
	require.NoError(t, err)
	require.Equal(t, "1000", admin.altered[0].Configs[0].Value)
	require.Equal(t, configRetentionMS, admin.altered[0].Configs[0].Name)
	require.Equal(t, configSegmentMS, admin.altered[0].Configs[1].Name)
}

type fakeTopicAdmin struct {
	current       map[string]map[string]string
	described     []string
	altered       []kafka.IncrementalAlterConfigsRequestResource
	resourceError error
	requestError  error
}

func (f *fakeTopicAdmin) DescribeConfigs(_ context.Context, req *kafka.DescribeConfigsRequest) (*kafka.DescribeConfigsResponse, error) {
	if f.requestError != nil {
		return nil, f.requestError
	}
	resp := &kafka.DescribeConfigsResponse{}
	for _, resource := range req.Resources {
		f.described = append(f.described, resource.ResourceName)
		out := kafka.DescribeConfigResponseResource{
			ResourceName: resource.ResourceName,
			Error:        f.resourceError,
		}
		for name, value := range f.current[resource.ResourceName] {
			out.ConfigEntries = append(out.ConfigEntries, kafka.DescribeConfigResponseConfigEntry{
				ConfigName:  name,
				ConfigValue: value,
			})
		}
		resp.Resources = append(resp.Resources, out)
	}
	return resp, nil
}

func (f *fakeTopicAdmin) IncrementalAlterConfigs(_ context.Context, req *kafka.IncrementalAlterConfigsRequest) (*kafka.IncrementalAlterConfigsResponse, error) {
	if f.requestError != nil {
		return nil, f.requestError
	}
	resp := &kafka.IncrementalAlterConfigsResponse{}
	for _, resource := range req.Resources {
		f.altered = append(f.altered, resource)
		resp.Resources = append(resp.Resources, kafka.IncrementalAlterConfigsResponseResource{
			ResourceName: resource.ResourceName,
		})
	}
	return resp, nil
}

func configValue(t *testing.T, entries []kafka.ConfigEntry, name string) string {
	t.Helper()
	value, ok := configEntryValue(entries, name)
	require.Truef(t, ok, "missing config %s", name)
	return value
}
