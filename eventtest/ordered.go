package eventtest

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/wait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
)

const delayFromE1 = time.Second * 10

func OrderedBlockTestOneCG[
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
	OrderedTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureBlock, "OB1", true)
}

func OrderedBlockTestTwoCG[
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
	OrderedTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureBlock, "OB2", false)
}

func OrderedRetryTestOncCG[
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
	OrderedTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureRetryLater, "ORL1", true)
}

func OrderedRetryTestTwoCG[
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
	OrderedTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureRetryLater, "ORL2", false)
}

// OrderedTest sends events in different topics that have to be processed
// out of order compared to how they're sent
func OrderedTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
	onFailure eventmodels.OnFailure,
	prefix string,
	oneConsumerGroup bool,
) {
	type myType map[string]string

	baseT := t
	t = ntest.ExtraDetailLogger(t, prefix)

	consumerGroup1 := events.NewConsumerGroup(Name(t) + "CG1")
	var consumerGroup2 events.ConsumerGroupName
	if oneConsumerGroup {
		t.Log("beginning test with one consumer group")
		consumerGroup2 = consumerGroup1
	} else {
		t.Log("beginning test with two consumer groups")
		consumerGroup2 = events.NewConsumerGroup(Name(t) + "CG2")
	}

	topic1 := eventmodels.BindTopicTx[myType, ID, TX, DB](Name(t) + "Topic1")
	topic2 := eventmodels.BindTopicTx[myType, ID, TX, DB](Name(t) + "Topic2")

	lib := events.New[ID, TX, DB]()
	conn.AugmentWithProducer(lib)
	lib.SetTopicConfig(kafka.TopicConfig{Topic: topic1.Topic()})
	lib.SetTopicConfig(kafka.TopicConfig{Topic: topic2.Topic()})

	key1 := uuid.New().String()
	t.Logf("E1: %s", key1)
	key2 := uuid.New().String()
	t.Logf("E2: %s", key2)

	var key1Success atomic.Int32
	var key1SuccessTime atomic.Int64
	key2Success := make(chan struct{})
	var key2Failure atomic.Int32

	lib.ConsumeExactlyOnce(consumerGroup1, onFailure, Name(t)+"H1", topic1.HandlerTx(func(ctx context.Context, _ TX, e eventmodels.Event[myType]) error {
		t.Logf("H1: received key:%s id:%s payloadid:%s", e.Key, e.ID, e.Payload["ID"])
		if e.Key != key1 {
			t.Logf("H1: ignoring %s", e.Key)
			return nil
		}
		if key2Failure.Load() == 0 {
			t.Logf("H1: E2 failure still outstanding, rejecting E1: %s", e.Key)
			return errors.Errorf("out of order")
		}
		t.Logf("H1: E2 already failed, processing E1: %s", e.Key)
		key1SuccessTime.CompareAndSwap(0, time.Now().UnixNano())
		key1Success.Add(1)
		return nil
	}))
	lib.ConsumeExactlyOnce(consumerGroup2, onFailure, Name(t)+"H2", topic2.HandlerTx(func(ctx context.Context, _ TX, e eventmodels.Event[myType]) error {
		t.Logf("H2: received key:%s id:%s payloadid:%s", e.Key, e.ID, e.Payload["ID"])
		if e.Key != key2 {
			t.Logf("H2: ignoring %s", e.Key)
			return nil
		}
		if key1Success.Load() == 0 {
			t.Logf("H2: E1 success still outstanding, rejecting E2: %s", e.Key)
			key2Failure.Add(1)
			return errors.Errorf("out of order")
		}
		when := time.Unix(0, key1SuccessTime.Load())
		if since := time.Since(when); since < delayFromE1 {
			t.Logf("H2: E1 success too recent (%s), rejecting E2: %s", since, e.Key)
			key2Failure.Add(1)
			return errors.Errorf("too soon")
		}
		t.Logf("H2: E1 already processed, processing E2: %s", e.Key)
		close(key2Success)
		return nil
	}))

	lib.Configure(conn, ntest.ExtraDetailLogger(baseT, prefix+"-L"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	consumeDone := lib.StartConsumingOrPanic(ctx)

	t.Log("producing may take a few tries for brand new topics")
	require.NoError(t, wait.For(func() (bool, error) {
		err := lib.Produce(ctx, eventmodels.ProduceImmediate, topic2.Event(key2, myType{"ID": key2}))
		if err != nil {
			t.Logf("got error trying to produce: %v", err)
			return false, err
		}
		return true, nil
	}, wait.ExitOnError(false), wait.WithLimit(time.Second*60)))
	t.Logf("sent E2: %s", key2)
	require.NoError(t, wait.For(func() (bool, error) {
		err := lib.Produce(ctx, eventmodels.ProduceImmediate, topic1.Event(key1, myType{"ID": key1}))
		if err != nil {
			t.Logf("got error trying to produce: %v", err)
			return false, err
		}
		return true, nil
	}, wait.ExitOnError(false), wait.WithLimit(time.Second*60)))
	t.Logf("sent E1: %s", key1)

	t.Log("both events produced, waiting for delivery")

	select {
	case <-key2Success:
		t.Log("great!")
	case <-time.After(time.Minute * 2):
		t.Log("timeout failure")
		assert.True(t, false)
	}
	cancel()
	t.Log("waiting for consumer to finish")
	<-consumeDone
	t.Log("all done")
}
