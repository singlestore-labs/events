package eventtest

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/wait"
)

const (
	smallBatchSleep = time.Millisecond * 600
	eventsToSend    = 200
	maxWait         = 30 * time.Second
)

type batchDeliveryInfo struct {
	name           string
	lock           sync.Mutex
	maxBatchSize   int
	totalDelivered int
	done           chan struct{}
	doneCalled     atomic.Int32
}

func createHandler[ID eventmodels.AbstractID[ID], TX eventmodels.EnhancedTX, DB AugmentAbstractDB[ID, TX]](
	t ntest.T,
	name string,
) (*batchDeliveryInfo, func(context.Context, []eventmodels.Event[map[string]string]) error) {
	info := &batchDeliveryInfo{
		name: name,
		done: make(chan struct{}),
	}

	handler := func(ctx context.Context, events []eventmodels.Event[map[string]string]) error {
		t.Logf("got batch of %d for %s", len(events), name)
		switch len(events) {
		case 0:
			return errors.Errorf("zero size batch")
		case 1:
			time.Sleep(smallBatchSleep)
		}

		info.lock.Lock()
		defer info.lock.Unlock()
		if len(events) > info.maxBatchSize {
			info.maxBatchSize = len(events)
		}
		info.totalDelivered += len(events)
		if info.totalDelivered >= eventsToSend && info.doneCalled.Add(1) == 1 {
			t.Logf("completed deliveries for %s: %d", name, info.totalDelivered)
			close(info.done)
		}
		return nil
	}

	return info, handler
}

func runTest[ID eventmodels.AbstractID[ID], TX eventmodels.EnhancedTX, DB AugmentAbstractDB[ID, TX]](
	ctx context.Context,
	t ntest.T,
	lib *events.Library[ID, TX, DB],
	events []eventmodels.ProducingEvent,
	cancel Cancel,
	info *batchDeliveryInfo,
) {
	defer cancel()

	produceDone, err := lib.CatchUpProduce(ctx, time.Second*5, 64)
	defer func() {
		cancel()
		t.Log("wait for produce done")
		<-produceDone
	}()
	require.NoError(t, err, "catch up")

	consumeDone := lib.StartConsumingOrPanic(ctx)
	defer func() {
		cancel()
		t.Log("waiting for consume done")
		<-consumeDone
	}()

	t.Log("consumer/producer started")

	t.Log("producing may take a few tries for brand new topics")
	require.NoError(t, wait.For(func() (bool, error) {
		err := lib.Produce(ctx, eventmodels.ProduceImmediate, events...)
		if err != nil {
			t.Logf("got error trying to produce: %v", err)
			return false, err
		}
		return true, nil
	}, wait.ExitOnError(false), wait.WithLimit(time.Second*60)))

	t.Log("all events produced")
	t.Log("waiting for events to be delivered")

	select {
	case <-ctx.Done():
		t.Log("context cancelled")
		require.FailNow(t, "timeout", "aborted waiting for completion")
	case <-info.done:
		t.Log("delivery completed")
	case <-time.After(maxWait):
		t.Log("timeout")
		require.FailNow(t, "timeout", "aborted waiting for completion")
	}

	t.Log("VIA HANDLER", info.name)
	t.Log(" max batch", info.maxBatchSize)
	t.Log(" delivered", info.totalDelivered)
	assert.Greaterf(t, info.maxBatchSize, 1, "max batch size %s", info.name)
	assert.GreaterOrEqual(t, info.totalDelivered, eventsToSend, "total delivered %s", info.name)
}

func batchTestCommon[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	logPrefix string,
) (
	*events.Library[ID, TX, DB],
	eventmodels.BoundTopicTx[map[string]string, ID, TX, DB],
	events.ConsumerGroupName,
	[]eventmodels.ProducingEvent,
) {
	baseT := t
	t = ntest.ExtraDetailLogger(t, logPrefix)

	consumerGroup := events.NewConsumerGroup(Name(t) + "Topic")
	lib := events.New[ID, TX, DB]()
	conn.AugmentWithProducer(lib)
	topic := eventmodels.BindTopicTx[map[string]string, ID, TX, DB](Name(t) + "Topic")
	lib.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})

	lib.Configure(conn, ntest.ExtraDetailLogger(baseT, logPrefix+"-L"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)

	// Generate test events
	toSend := make([]eventmodels.ProducingEvent, eventsToSend)
	for i := 0; i < eventsToSend; i++ {
		id := uuid.New().String()
		toSend[i] = topic.Event(id, map[string]string{
			"X":  "Y",
			"I":  strconv.Itoa(i),
			"ID": id,
		}).ID(id)
	}

	return lib, topic, consumerGroup, toSend
}

func BatchDeliveryIdempotentTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
) {
	lib, topic, consumerGroup, events := batchTestCommon(ctx, t, conn, brokers, "BDI")

	info, handler := createHandler[ID, TX, DB](t, "idempotent")
	lib.ConsumeIdempotent(consumerGroup, eventmodels.OnFailureDiscard, Name(t)+"-CI", topic.BatchHandler(handler))

	runTest(ctx, t, lib, events, cancel, info)
}

func BatchDeliveryBroadcastTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
) {
	lib, topic, _, events := batchTestCommon(ctx, t, conn, brokers, "BDB")

	info, handler := createHandler[ID, TX, DB](t, "broadcast")
	lib.ConsumeBroadcast(Name(t)+"CB", topic.BatchHandler(handler))

	runTest(ctx, t, lib, events, cancel, info)
}

func BatchDeliveryExactlyOnceTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
) {
	lib, topic, consumerGroup, events := batchTestCommon(ctx, t, conn, brokers, "BDEO")

	info, handler := createHandler[ID, TX, DB](t, "exactlyOnce")
	lib.ConsumeExactlyOnce(consumerGroup, eventmodels.OnFailureDiscard, Name(t)+"-CEO", topic.BatchHandlerTx(
		func(ctx context.Context, tx TX, events []eventmodels.Event[map[string]string]) error {
			return handler(ctx, events)
		}))

	runTest(ctx, t, lib, events, cancel, info)
}
