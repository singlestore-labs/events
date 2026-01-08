package eventtest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/stretchr/testify/require"
)

func DeadLetterDiscardTest[
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
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureDiscard, "DLD", prefix)
}

func DeadLetterBlockTest[
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
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureBlock, "DLB", prefix)
}

func DeadLetterSaveTest[
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
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureSave, "DLS", prefix)
}

func DeadLetterRetryLaterTest[
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
	DeadLetterTest(ctx, t, conn, brokers, cancel,
		eventmodels.OnFailureRetryLater, "DLRL", prefix)
}

func DeadLetterTest[
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
	testPrefix string,
	libraryPrefix Prefix,
) {
	defer cancel()
	baseT := t
	t = ntest.ExtraDetailLogger(t, string(libraryPrefix)+testPrefix)
	type deliveryInfoBlock struct {
		name      string
		topic     string
		baseTopic string
	}
	var deliveryInfo deliveryInfoBlock

	consumerGroup := events.NewConsumerGroup(Name(t) + "CG")

	var lock sync.Mutex
	firstSignal := make(chan struct{})
	signal := &firstSignal

	type myType map[string]string

	t.Log("setting up 1st events library instance")
	lib := events.New[ID, TX, DB]()
	lib.SkipNotifierSupport()
	lib.SetPrefix(string(libraryPrefix))
	if !IsNilDB(conn) {
		conn.AugmentWithProducer(lib)
	}
	topic := eventmodels.BindTopic[myType](Name(t) + "Topic")
	lib.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})
	deadLetterTopic := eventmodels.BindTopic[myType](events.DeadLetterTopic(Name(t)+"Topic", consumerGroup))
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
			return fmt.Errorf("always errors (message id matches)")
		}
	}
	getDelivery := func() deliveryInfoBlock {
		lock.Lock()
		defer lock.Unlock()
		return deliveryInfo
	}

	lib.ConsumeIdempotent(consumerGroup, onFailure, Name(t)+"CIH", topic.Handler(mkHandler("first")), events.WithTimeout(configuredTimeout))

	firstCtx, cancelFirst := context.WithCancel(ctx)
	lib.Configure(conn, ntest.ExtraDetailLogger(baseT, string(libraryPrefix)+testPrefix+"1"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	firstDone := lib.StartConsumingOrPanic(firstCtx)

	body := myType{
		Name(t): "value",
	}
	t.Logf("producing message 1 %s", id1)
	t.Log("deliver ie expected to be attempted, but it will fail")
	require.NoError(t, lib.Produce(ctx, eventmodels.ProduceImmediate, topic.Event(id1, body).ID(id1)))

	WaitFor(ctx, t, "first delivery", firstSignal, DeliveryTimeout)
	t.Log("sleeping... (delivery should happen during this sleep)")
	time.Sleep(configuredTimeout + LongerOnCI(time.Second*2, time.Minute, time.Second*20))
	t.Log("delivery should have failed by now")
	cancelFirst()
	WaitFor(ctx, t, "first events library shutdown", firstDone, DeliveryTimeout)

	t.Log("starting up second events library")
	lib2 := events.New[ID, TX, DB]()
	lib2.SkipNotifierSupport()
	lib2.SetPrefix(string(libraryPrefix))
	if onFailure == eventmodels.OnFailureSave {
		t.Log("second events library is consuming directly from the dead letter topic")
		lib2.ConsumeIdempotent(consumerGroup, eventmodels.OnFailureBlock, Name(t)+"DLd", deadLetterTopic.Handler(mkHandler("deadLetterDirect")), events.WithTimeout(configuredTimeout))
	} else {
		lib2.ConsumeIdempotent(consumerGroup, onFailure, Name(t)+"CIH", topic.Handler(mkHandler("second")), events.WithTimeout(configuredTimeout))
	}
	lib2.Configure(conn, ntest.ExtraDetailLogger(baseT, string(libraryPrefix)+testPrefix+"2"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	// this assignment is thread-safe because lib1 is completely shut down and lib2 hasn't started yet
	secondSignal := make(chan struct{})
	signal = &secondSignal
	secondCtx, cancelSecond := context.WithCancel(ctx)
	secondDone := lib2.StartConsumingOrPanic(secondCtx)
	defer func() {
		t.Log("cancel second")
		cancelSecond()
		t.Log("wait for 2nd library shutdown")
		<-secondDone
	}()

	switch onFailure {
	case eventmodels.OnFailureBlock:
		t.Log("We should see re-delivery attempts")
		WaitFor(ctx, t, "delivery in second events library instance", secondSignal, DeliveryTimeout)
		require.Equal(t, "second", getDelivery().name, "handler name")
	case eventmodels.OnFailureDiscard:
		t.Log("We should not see re-delivery attempts. We'll just wait a while and hope we don't see any")
		// We're testing for a negative and
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
		t.Log("We should see re-delivery via the dead letter topic, not the original topic - consuming the dead letter topic")
		WaitFor(ctx, t, "second delivery", secondSignal, DeliveryTimeout)
		require.Equal(t, getDelivery().topic, getDelivery().baseTopic, "topics")
		require.Contains(t, getDelivery().topic, "dead-letter", "topic")
		require.Equal(t, "deadLetterDirect", getDelivery().name, "deadletter")
	case eventmodels.OnFailureRetryLater:
		t.Log("We should see re-delivery via the dead letter topic, not the original topic - consuming the original topic")
		WaitFor(ctx, t, "second delivery", secondSignal, DeliveryTimeout)
		require.Equal(t, "second", getDelivery().name, "second")
		require.Contains(t, getDelivery().topic, "dead-letter", "topic")
		require.NotEqual(t, getDelivery().topic, getDelivery().baseTopic, "topics")
	}
}
