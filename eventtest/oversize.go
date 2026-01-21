package eventtest

import (
	"context"
	"os"
	"time"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"

	"github.com/chrismcguire/gobberish"
	"github.com/google/uuid"
	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func OversizeSendTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
	prefix Prefix,
) {
	baseT := t
	t = ntest.ExtraDetailLogger(t, string(prefix)+"OST")

	lib := events.New[ID, TX, DB]()
	lib.SetEnhanceDB(true)
	lib.SetSizeCapLowerLimit(1000)
	lib.SetPrefix(string(prefix))
	lib.SetTracerConfig(GetTracerConfig(t))

	tracerCtx := TracerContext(ctx, t)
	defer func() {
		t.Log("waiting for lib shutdown")
		lib.Shutdown(tracerCtx)
		t.Log("done waiting for lib shutdown")
	}()
	defer CatchPanic(t)

	lib.Configure(conn, TracerProvider(baseT, string(prefix)+"OST"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	if !IsNilDB(conn) {
		conn.AugmentWithProducer(lib)
	}

	topic1 := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t) + "Topic1B")
	lib.SetTopicConfig(kafka.TopicConfig{
		Topic: topic1.Topic(),
		ConfigEntries: []kafka.ConfigEntry{
			{
				ConfigName:  "max.message.bytes",
				ConfigValue: "15000",
			},
		},
	})
	topic2 := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t) + "Topic2B")
	lib.SetTopicConfig(kafka.TopicConfig{
		Topic: topic2.Topic(),
		ConfigEntries: []kafka.ConfigEntry{
			{
				ConfigName:  "max.message.bytes",
				ConfigValue: "3000",
			},
		},
	})
	topic3 := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t) + "Topic3B")
	lib.SetTopicConfig(kafka.TopicConfig{
		Topic: topic3.Topic(),
		ConfigEntries: []kafka.ConfigEntry{
			{
				ConfigName:  "max.message.bytes",
				ConfigValue: "1000",
			},
		},
	})

	id := uuid.New().String()
	t.Log("base id is", id)

	now := time.Now().Round(time.Second).UTC()

	mediumText := uncompressable(2000) // too big for 1000 default
	largeText := uncompressable(4000)  // too big for 2000 topic

	t.Log("Attempt to send test messages...")

	t.Log("Attempt to send medium to Topic3 (should fail)")
	require.ErrorIs(t, lib.Produce(tracerCtx, eventmodels.ProduceImmediate, topic3.Event("key-3m-"+id, MyEvent{
		S: mediumText,
	}).
		ID("3m-"+id).
		Subject(Name(t)+"-subject").
		Time(now)), events.ErrTooBig)

	t.Log("Attempt to send medium to Topic2 (should succeed)")
	require.NoError(t, lib.Produce(tracerCtx, eventmodels.ProduceImmediate, topic2.Event("key-2m-"+id, MyEvent{
		S: mediumText,
	}).
		ID("2m-"+id).
		Subject(Name(t)+"-subject").
		Time(now)))

	t.Log("Attempt to send large to Topic2 (should fail)")
	require.ErrorIs(t, lib.Produce(tracerCtx, eventmodels.ProduceImmediate, topic2.Event("key-2l-"+id, MyEvent{
		S: largeText,
	}).
		ID("2l-"+id).
		Subject(Name(t)+"-subject").
		Time(now)), events.ErrTooBig)

	t.Log("Attempt to send large to Topic1 (should succeed)")
	require.NoError(t, lib.Produce(tracerCtx, eventmodels.ProduceImmediate, topic1.Event("key-1l-"+id, MyEvent{
		S: largeText,
	}).
		ID("1l-"+id).
		Subject(Name(t)+"-subject").
		Time(now)))
}

func uncompressable(length int) string {
	s := gobberish.GenerateString(length)
	ratio := float64(length) / float64(len([]byte(s)))
	cut := int(ratio * float64(len(s)))
	if cut < len(s) {
		return s[:cut]
	}
	return s
}
