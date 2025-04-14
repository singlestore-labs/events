package eventtest

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
)

const (
	smallBatchSleep = time.Millisecond * 600
	eventsToSend    = 200
	maxWait         = 30 * time.Second
)

func BatchDeliveryTest[
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
	type myType map[string]string

	type deliveryInfoBlock struct {
		name           string
		lock           sync.Mutex
		maxBatchSize   int
		totalDelivered int
	}

	defer cancel()

	baseT := t
	t = ntest.ExtraDetailLogger(t, "TBD")

	consumerGroup := events.NewConsumerGroup(Name(t))
	lib := events.New[ID, TX, DB]()
	conn.AugmentWithProducer(lib)
	topic := eventmodels.BindTopicTx[myType, ID, TX, DB](Name(t))

	infoBlocks := make(map[string]*deliveryInfoBlock)

	var wg sync.WaitGroup

	produceComplete := make(chan struct{})

	mkHandler := func(name string) func(context.Context, []eventmodels.Event[myType]) error {
		info := &deliveryInfoBlock{
			name: name,
		}
		infoBlocks[name] = info
		wg.Add(1)
		return func(ctx context.Context, events []eventmodels.Event[myType]) error {
			select {
			case <-produceComplete:
			case <-ctx.Done():
			}
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
			if info.totalDelivered < eventsToSend {
				info.totalDelivered += len(events)
				if info.totalDelivered >= eventsToSend {
					t.Logf("completed deliveries for %s: %d", name, info.totalDelivered)
					wg.Done()
				}
			}
			return nil
		}
	}

	lib.ConsumeIdempotent(consumerGroup, eventmodels.OnFailureDiscard, Name(t)+"-CI", topic.BatchHandler(mkHandler("idempotent")))

	lib.ConsumeBroadcast(Name(t), topic.BatchHandler(mkHandler("broadcast")))

	eoh := mkHandler("exactlyOnce")
	lib.ConsumeExactlyOnce(consumerGroup, eventmodels.OnFailureDiscard, Name(t)+"-CEO", topic.BatchHandlerTx(
		func(ctx context.Context, tx TX, events []eventmodels.Event[myType]) error {
			return eoh(ctx, events)
		}))

	t.Log("handlers registered")

	lib.Configure(conn, ntest.ExtraDetailLogger(baseT, "TBD-L"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)

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

	toSend := make([]eventmodels.ProducingEvent, eventsToSend)
	for i := 0; i < eventsToSend; i++ {
		id := uuid.New().String()

		toSend[i] = topic.Event(id, myType{
			"X":  "Y",
			"I":  strconv.Itoa(i),
			"ID": id,
		}).ID(id)
		t.Logf("generate event %d %s", i, id)
	}
	err = lib.Produce(ctx, eventmodels.ProduceImmediate, toSend...)
	require.NoError(t, err)

	t.Log("all events produced")
	close(produceComplete)

	wgDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	t.Log("waiting for events to be delivered")

	select {
	case <-ctx.Done():
		t.Log("context cancelled")
		require.FailNow(t, "timeout", "aborted waiting for waitgroup")
	case <-wgDone:
		t.Log("waitgroup is done")
	case <-time.After(maxWait):
		t.Log("timeout")
		require.FailNow(t, "timeout", "aborted waiting for waitgroup")
	}

	for name, info := range infoBlocks {
		t.Log("VIA HANDLER", name)
		t.Log(" max batch", info.maxBatchSize)
		t.Log(" delivered", info.totalDelivered)
		assert.Greaterf(t, info.maxBatchSize, 1, "max batch size %s", name)
		assert.GreaterOrEqual(t, info.totalDelivered, eventsToSend, "total delivered %s", name)
	}
}
