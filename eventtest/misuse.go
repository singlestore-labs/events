package eventtest

import (
	"context"
	"os"
	"time"

	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"singlestore.com/helios/events"
	"singlestore.com/helios/events/eventmodels"
	"singlestore.com/helios/test/di"
	"singlestore.com/helios/trace"
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
	brokers di.Brokers,
	tracer trace.Iface,
	cancel di.Cancel,
) {
	type myEvent map[string]string

	lib := events.New[ID, TX, DB]()
	lib.SetEnhanceDB(true)
	lib.Configure(conn, tracer, true, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	produceDone, err := lib.CatchUpProduce(ctx, time.Second*10, 20)
	require.NoError(t, err)

	goodTopic := eventmodels.BindTopic[myEvent](Name(t) + "-good")
	badTopic := eventmodels.BindTopic[myEvent](Name(t) + "-bad")

	lib.SetTopicConfig(kafka.TopicConfig{Topic: goodTopic.Topic()})

	t.Log("The bad topics are not a blocker for the transaction because they're asyncronously")
	startTime := time.Now()
	require.NoError(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(badTopic.Event("irrelevant", myEvent{"foo": "bar"}).
			ID("doesn't matter"),
		)
		t.Logf("added events to transaction")
		return nil
	}), "transact/send with bad topic")
	duration := time.Since(startTime)
	require.Lessf(t, duration, time.Second, "tx should be fast, not %s", duration)

	t.Log("immediate produce will check the topic and should error")
	require.Error(t, lib.Produce(ctx, eventmodels.ProduceImmediate, badTopic.Event("irrelevant",
		myEvent{"foo": "bar"}).ID("also doesn't matter")),
		"immediate with bad topic")

	lib2 := events.New[ID, TX, DB]()
	lib2.ConsumeIdempotent(events.NewConsumerGroup("consumerGroup"), eventmodels.OnFailureBlock, "handler", goodTopic.Handler(func(_ context.Context, event eventmodels.Event[myEvent]) error {
		return nil
	}))
	_, _, err = lib2.StartConsuming(ctx)
	require.Errorf(t, err, "start consuming w/o configured events")

	cancel()
	t.Log("waiting for catch up produce to complete")
	<-produceDone
}
