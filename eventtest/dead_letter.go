package eventtest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/ntest"
	"github.com/stretchr/testify/require"

	"singlestore.com/helios/events"
	"singlestore.com/helios/events/eventmodels"
	"singlestore.com/helios/test/di"
	"singlestore.com/helios/testutil"
)

func DeadLetterDiscardTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t di.T,
	conn DB,
	brokers di.Brokers,
	cancel di.Cancel,
) {
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureDiscard, "DLD")
}

func DeadLetterBlockTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t di.T,
	conn DB,
	brokers di.Brokers,
	cancel di.Cancel,
) {
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureBlock, "DLB")
}

func DeadLetterSaveTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t di.T,
	conn DB,
	brokers di.Brokers,
	cancel di.Cancel,
) {
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureSave, "DLS")
}

func DeadLetterRetryLaterTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t di.T,
	conn DB,
	brokers di.Brokers,
	cancel di.Cancel,
) {
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureRetryLater, "DLRL")
}

func DeadLetterTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t di.T,
	conn DB,
	brokers di.Brokers,
	cancel di.Cancel,
	onFailure eventmodels.OnFailure,
	prefix string,
) {
	baseT := t
	t = ntest.ExtraDetailLogger(t, prefix)
	type deliveryInfoBlock struct {
		name      string
		topic     string
		baseTopic string
	}
	var deliveryInfo deliveryInfoBlock

	consumerGroup := events.NewConsumerGroup(Name(t))

	var lock sync.Mutex
	firstSignal := make(chan struct{})
	signal := &firstSignal

	type myType map[string]string

	lib := events.New[ID, TX, DB]()
	conn.AugmentWithProducer(lib)
	topic := eventmodels.BindTopic[myType](Name(t))
	deadLetterTopic := eventmodels.BindTopic[myType](events.DeadLetterTopic(Name(t), consumerGroup))
	id1 := uuid.New().String()
	configuredTimeout := 500 * time.Millisecond
	mkHandler := func(name string) func(context.Context, eventmodels.Event[myType]) error {
		return func(ctx context.Context, e eventmodels.Event[myType]) error {
			t.Logf("%s consumer called with %s", name, e.ID)
			if e.ID != id1 {
				// cleans out messages from prior tests
				return nil
			}
			lock.Lock()
			defer lock.Unlock()
			deliveryInfo.name = name
			deliveryInfo.topic = e.Topic
			deliveryInfo.baseTopic = e.BaseTopic
			if signal != nil {
				close(*signal)
				signal = nil
			}
			return fmt.Errorf("always errors")
		}
	}
	getDelivery := func() deliveryInfoBlock {
		lock.Lock()
		defer lock.Unlock()
		return deliveryInfo
	}

	lib.ConsumeIdempotent(consumerGroup, onFailure, Name(t), topic.Handler(mkHandler("first")), events.WithTimeout(configuredTimeout))

	firstCtx, cancelFirst := context.WithCancel(ctx)
	lib.Configure(conn, testutil.NewTestingLogger(ntest.ExtraDetailLogger(baseT, prefix+"1")), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	produceDone, err := lib.CatchUpProduce(ctx, time.Second*5, 64)
	defer func() {
		t.Log("wait for produce done")
		<-produceDone
	}()
	require.NoError(t, err, "catch up")
	firstDone := lib.StartConsumingOrPanic(firstCtx)

	body := myType{
		Name(t): "value",
	}
	t.Logf("message 1 %s", id1)
	require.NoErrorf(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(topic.Event(id1, body).ID(id1))
		t.Logf("added events to transaction")
		return nil
	}), "transact/send")

	WaitFor(ctx, t, "first delivery", firstSignal, DeliveryTimeout)
	t.Log("sleeping...")
	time.Sleep(configuredTimeout + LongerOnCI(time.Second*2, time.Minute))
	t.Log("delivery should have failed by now")
	cancelFirst()
	WaitFor(ctx, t, "first library shutdown", firstDone, DeliveryTimeout)

	lib2 := events.New[ID, TX, DB]()
	if onFailure == eventmodels.OnFailureSave {
		lib2.ConsumeIdempotent(consumerGroup, eventmodels.OnFailureBlock, "deadLetterDirect", deadLetterTopic.Handler(mkHandler("deadLetterDirect")), events.WithTimeout(configuredTimeout))
	} else {
		lib2.ConsumeIdempotent(consumerGroup, onFailure, Name(t), topic.Handler(mkHandler("second")), events.WithTimeout(configuredTimeout))
	}
	lib2.Configure(conn, testutil.NewTestingLogger(ntest.ExtraDetailLogger(baseT, prefix+"2")), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	// this assignment is thread-safe because lib1 is completely shut down and lib2 hasn't started yet
	secondSignal := make(chan struct{})
	signal = &secondSignal
	secondDone := lib2.StartConsumingOrPanic(ctx)
	defer func() {
		t.Log("wait for 2nd library shutdown")
		<-secondDone
	}()

	switch onFailure {
	case eventmodels.OnFailureBlock:
		// We should see re-delivery attempts
		WaitFor(ctx, t, "second delivery", secondSignal, DeliveryTimeout)
		require.Equal(t, "second", getDelivery().name, "handler name")
	case eventmodels.OnFailureDiscard:
		// We should not see re-deliver attempts. We're testing for a negative and
		// don't know how long to wait. If this test is flaky, then it's actually failing
		// but we're not waiting long enough either here or above when "sleeping..."
		t.Log("waiting a while to see if we get a re-delivery. We should not.")
		select {
		case <-secondSignal:
			require.FailNow(t, "timeout", "re-delivery attempted which we don't want")
		case <-time.After(time.Second * 3):
		}
		t.Log("good, we didn't get a re-delivery")
	case eventmodels.OnFailureSave:
		// This should be via the dead letter topic - reading directly
		WaitFor(ctx, t, "second delivery", secondSignal, DeliveryTimeout)
		require.Equal(t, getDelivery().topic, getDelivery().baseTopic, "topics")
		require.Contains(t, getDelivery().topic, "dead-letter", "topic")
		require.Equal(t, "deadLetterDirect", getDelivery().name, "deadletter")
	case eventmodels.OnFailureRetryLater:
		// This should be via the dead letter topic
		WaitFor(ctx, t, "second delivery", secondSignal, DeliveryTimeout)
		require.Equal(t, "second", getDelivery().name, "second")
		require.Contains(t, getDelivery().topic, "dead-letter", "topic")
		require.NotEqual(t, getDelivery().topic, getDelivery().baseTopic, "topics")
	}
	cancel()
}
