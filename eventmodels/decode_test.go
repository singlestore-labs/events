package eventmodels

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type decodeTestLibrary struct{}

func (decodeTestLibrary) RemovePrefix(topic string) string { return topic }
func (decodeTestLibrary) TracerProvider(context.Context) Tracer {
	return func(string, ...any) {}
}
func (decodeTestLibrary) TracerConfig() TracerConfig { return TracerConfig{} }

func TestDecodeIncludesKafkaPosition(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}
	message := kafka.Message{
		Topic:     "topic",
		Partition: 3,
		Offset:    42,
		Value:     []byte(`{"value":"test"}`),
		Headers: []kafka.Header{{
			Key:   "content-type",
			Value: []byte("application/json"),
		}},
	}

	event, err := decode[payload](&message, "group", decodeTestLibrary{}, "handler")
	require.NoError(t, err)
	require.Equal(t, 3, event.Partition)
	require.Equal(t, int64(42), event.Offset)
	require.Equal(t, "test", event.Payload.Value)
}
