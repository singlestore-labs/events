package eventtest

import (
	"context"
	"os"
	"time"

	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
)

// ErrorWhenMisusedTest verifies that publishing events fails when:
//
//   - No Producer is embedded in the connection
//   - An invalid topic is used
func ErrorWhenMisusedTest[
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
	if IsNilDB(conn) {
		t.Skipf("%s requires a database", t.Name())
	}
	type myEvent map[string]string

	lib := events.New[ID, TX, DB]()
	lib.SkipNotifierSupport()
	lib.SetEnhanceDB(true)
	lib.SetPrefix(string(prefix))
	lib.Configure(conn, t, true, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)

	goodTopic := eventmodels.BindTopic[myEvent](Name(t) + "-good")
	badTopic := eventmodels.BindTopic[myEvent](Name(t) + "-bad")

	lib.SetTopicConfig(kafka.TopicConfig{Topic: goodTopic.Topic()})

	startTime := time.Now()
	require.NoError(t, conn.Transact(ctx, func(tx TX) error {
		return nil
	}))
	duration := time.Since(startTime)
	t.Log("time for an empty transaction: %s", duration)
	require.Lessf(t, duration, time.Second, "empty tx should be fast, not %s", duration)

	t.Log("The bad topics should be rejected before the catch-up-producer is started")
	startTime = time.Now()
	require.Error(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(badTopic.Event("irrelevant", myEvent{"foo": "bar"}).
			ID("doesn't matter"),
		)
		t.Logf("added events to transaction")
		return nil
	}), "transact/send with bad topic")
	duration = time.Since(startTime)
	t.Log("time for an transaction with event w/o backup producer: %s", duration)

	produceDone, err := lib.CatchUpProduce(ctx, time.Second*10, 20)
	require.NoError(t, err)
	t.Log("catch up producer started...")

	t.Log("The bad topics should be rejected after the catch-up-producer is started")
	startTime = time.Now()
	require.Error(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(badTopic.Event("irrelevant", myEvent{"foo": "bar"}).
			ID("doesn't matter"),
		)
		t.Logf("added events to transaction")
		return nil
	}), "transact/send with bad topic")
	duration = time.Since(startTime)
	t.Log("time for an transaction with event with backup producer: %s", duration)
	require.Lessf(t, duration, time.Second, "tx should be fast, not %s", duration)

	t.Log("immediate produce will check the topic and should error")
	require.Error(t, lib.Produce(ctx, eventmodels.ProduceImmediate, badTopic.Event("irrelevant",
		myEvent{"foo": "bar"}).ID("also doesn't matter")),
		"immediate with bad topic")

	lib2 := events.New[ID, TX, DB]()
	lib2.SkipNotifierSupport()
	lib2.SetPrefix(string(prefix))
	lib2.ConsumeIdempotent(events.NewConsumerGroup("consumerGroup"), eventmodels.OnFailureBlock, "handler", goodTopic.Handler(func(_ context.Context, event eventmodels.Event[myEvent]) error {
		return nil
	}))
	_, _, err = lib2.StartConsuming(ctx)
	require.Errorf(t, err, "start consuming w/o configured events")

	cancel()
	t.Log("waiting for catch up produce to complete")
	<-produceDone
}
