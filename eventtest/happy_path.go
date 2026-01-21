package eventtest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/wait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BroadcastDeliveryTest verifies:
// - That broadcast events are delivered to all instances of events.Library
// - That returning an error from a handler temporarily will not stop eventual message delivery
// - That multiple handlers in the same library receive broadcast events
func BroadcastDeliveryTest[
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
	t = ntest.ExtraDetailLogger(t, string(prefix)+"TBDT")

	t.Log("Create two library instances")
	lib1 := events.New[ID, TX, DB]()
	lib1.SetEnhanceDB(true)
	lib1.SetPrefix(string(prefix))
	lib2 := events.New[ID, TX, DB]()
	lib2.SetEnhanceDB(true)
	lib2.SetPrefix(string(prefix))

	lib1.Configure(conn, ntest.ExtraDetailLogger(baseT, string(prefix)+"TBDT-1"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	lib2.Configure(conn, ntest.ExtraDetailLogger(baseT, string(prefix)+"TBDT-2"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	if !IsNilDB(conn) {
		conn.AugmentWithProducer(lib1)
	}

	topic := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t) + "Topic")
	lib1.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})
	lib2.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})

	var lock sync.Mutex
	handlerCalled := make(map[string]bool)
	handlerRetried := make(map[string]bool)

	id := uuid.New().String()
	t.Log("id is", id)

	now := time.Now().Round(time.Second).UTC()

	// Create broadcast handlers
	mkBroadcastHandler := func(name string) func(context.Context, eventmodels.Event[MyEvent]) error {
		return func(_ context.Context, event eventmodels.Event[MyEvent]) error {
			if event.ID != id {
				t.Logf("received callback for %s but id is wrong (%s vs %s)", name, event.ID, id)
				return nil
			}

			t.Logf("received callback for %s, already failed: %v", name, handlerCalled[name])
			assert.Equal(t, now.Format(time.RFC3339), event.Timestamp.UTC().Format(time.RFC3339), "timestamp for %s", name)

			lock.Lock()
			defer lock.Unlock()

			if handlerCalled[name] {
				handlerRetried[name] = true
				return nil
			}

			handlerCalled[name] = true
			return errors.Errorf("failing %s first attempt", name)
		}
	}

	t.Log("Register broadcast handlers")
	lib1.ConsumeBroadcast("B1", topic.Handler(mkBroadcastHandler("B1")))
	lib1.ConsumeBroadcast("B2", topic.Handler(mkBroadcastHandler("B2"))) // Multiple handlers in lib1
	lib2.ConsumeBroadcast("B3", topic.Handler(mkBroadcastHandler("B3")))

	t.Log("Start consumers and producers")

	started1, done1, err := lib1.StartConsuming(ctx)
	require.NoError(t, err, "start consuming lib1")
	started2, done2, err := lib2.StartConsuming(ctx)
	require.NoError(t, err, "start consuming lib2")

	WaitFor(ctx, t, "consumer1", started1, StartupTimeout)
	WaitFor(ctx, t, "consumer2", started2, StartupTimeout)

	t.Logf("Verify broadcast consumer groups are different: %s vs %s", lib1.GetBroadcastConsumerGroupName(), lib2.GetBroadcastConsumerGroupName())
	require.NotEqual(t, lib1.GetBroadcastConsumerGroupName(), lib2.GetBroadcastConsumerGroupName(), "broadcast consumer group names")

	if IsNilDB(conn) {
		assert.True(t, strings.HasPrefix(lib1.GetBroadcastConsumerGroupName(), "broadcastLFConsumer-"), "consumer prefix") // defaultLockFreeBroadcastBase
	} else {
		assert.True(t, strings.HasPrefix(lib1.GetBroadcastConsumerGroupName(), "broadcastConsumer-"), "consumer prefix") // defaultBroadcastConsumerBaseName
	}

	// Clean up when done
	defer func() {
		t.Log("cancel")
		cancel()
		t.Log("wait done1")
		<-done1
		t.Log("wait done2")
		<-done2
		t.Log("done waits")
	}()

	t.Log("Send test message")
	require.NoError(t, lib1.Produce(ctx, eventmodels.ProduceImmediate, topic.Event("key-"+id, MyEvent{
		S: "broadcast-test",
	}).
		ID(id).
		Subject(Name(t)+"-subject").
		Time(now)))

	t.Logf("transaction complete, message sent")

	t.Log("Wait for handlers to be called")
	require.NoErrorf(t, wait.For(func() (bool, error) {
		lock.Lock()
		defer lock.Unlock()

		// Check that all handlers were called
		if !handlerCalled["B1"] || !handlerCalled["B2"] || !handlerCalled["B3"] {
			missing := []string{}
			if !handlerCalled["B1"] {
				missing = append(missing, "B1")
			}
			if !handlerCalled["B2"] {
				missing = append(missing, "B2")
			}
			if !handlerCalled["B3"] {
				missing = append(missing, "B3")
			}
			t.Logf("Still waiting for handlers to be called: %v", missing)
			return false, nil
		}

		// Check that all handlers were retried successfully
		if !handlerRetried["B1"] || !handlerRetried["B2"] || !handlerRetried["B3"] {
			missing := []string{}
			if !handlerRetried["B1"] {
				missing = append(missing, "B1")
			}
			if !handlerRetried["B2"] {
				missing = append(missing, "B2")
			}
			if !handlerRetried["B3"] {
				missing = append(missing, "B3")
			}
			t.Logf("Still waiting for retry success: %v", missing)
			return false, nil
		}

		return true, nil
	}, wait.WithLogger(t.Logf), wait.WithLimit(DeliveryTimeout), wait.WithMinInterval(time.Millisecond*500),
		wait.ExitOnError(true), wait.WithMaxInterval(time.Second*5), wait.WithBackoff(1.04),
		wait.WithReports(20)), "broadcast message delivery")

	t.Log("Final verification")
	assert.Zero(t, lib1.ProduceSyncCount.Load())
	assert.Zero(t, lib2.ProduceSyncCount.Load())
}

// IdempotentDeliveryTest verifies:
// - That idempotent events with the same consumer group are delivered exactly once
// - That idempotent events with different consumer groups are delivered to each group
// - That returning an error from a handler temporarily will not stop eventual message delivery
func IdempotentDeliveryTest[
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
	t = ntest.ExtraDetailLogger(t, string(prefix)+"TIDT")

	lib1 := events.New[ID, TX, DB]()
	lib1.SetEnhanceDB(true)
	lib1.SkipNotifierSupport()
	lib1.SetPrefix(string(prefix))
	lib2 := events.New[ID, TX, DB]()
	lib2.SetEnhanceDB(true)
	lib2.SkipNotifierSupport()
	lib2.SetPrefix(string(prefix))

	lib1.Configure(conn, ntest.ExtraDetailLogger(baseT, string(prefix)+"TIDT-1"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	lib2.Configure(conn, ntest.ExtraDetailLogger(baseT, string(prefix)+"TIDT-2"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	if !IsNilDB(conn) {
		conn.AugmentWithProducer(lib1)
	}

	topic := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t) + "Topic")
	lib1.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})
	lib2.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})

	var lock sync.Mutex
	handlerCalled := make(map[string]bool)
	handlerRetried := make(map[string]bool)
	callCounts := make(map[string]int)

	id := uuid.New().String()
	t.Log("id is", id)

	now := time.Now().Round(time.Second).UTC()

	mkIdempotentHandler := func(name string) func(context.Context, eventmodels.Event[MyEvent]) error {
		return func(_ context.Context, event eventmodels.Event[MyEvent]) error {
			if event.ID != id {
				t.Logf("received callback for %s but id is wrong (%s vs %s)", name, event.ID, id)
				return nil
			}

			t.Logf("received callback for %s, already failed: %v", name, handlerCalled[name])
			assert.Equal(t, now.Format(time.RFC3339), event.Timestamp.UTC().Format(time.RFC3339), "timestamp for %s", name)

			lock.Lock()
			defer lock.Unlock()

			callCounts[name]++

			if handlerCalled[name] {
				handlerRetried[name] = true
				return nil
			}

			handlerCalled[name] = true
			return errors.Errorf("failing %s first attempt", name)
		}
	}

	t.Log("Register same consumer group across libraries")
	lib1.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentA"), eventmodels.OnFailureBlock, "Ia1", topic.Handler(mkIdempotentHandler("Ia1")))
	lib2.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentA"), eventmodels.OnFailureBlock, "Ia2", topic.Handler(mkIdempotentHandler("Ia2")))

	t.Log("Register in different consumer groups")
	lib1.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentB"), eventmodels.OnFailureBlock, "Ib1", topic.Handler(mkIdempotentHandler("Ib1")))
	lib2.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentC"), eventmodels.OnFailureBlock, "Ic1", topic.Handler(mkIdempotentHandler("Ic1")))

	t.Log("Start consumers and producers")

	started1, done1, err := lib1.StartConsuming(ctx)
	require.NoError(t, err, "start consuming lib1")
	started2, done2, err := lib2.StartConsuming(ctx)
	require.NoError(t, err, "start consuming lib2")

	WaitFor(ctx, t, "consumer1", started1, StartupTimeout)
	WaitFor(ctx, t, "consumer2", started2, StartupTimeout)

	// Clean up when done
	defer func() {
		t.Log("cancel")
		cancel()
		t.Log("wait done1")
		<-done1
		t.Log("wait done2")
		<-done2
		t.Log("done waits")
	}()

	t.Log("Send test message")
	require.NoError(t, lib1.Produce(ctx, eventmodels.ProduceImmediate, topic.Event("key-"+id, MyEvent{
		S: "idempotent-test",
	}).
		ID(id).
		Subject(Name(t)+"-subject").
		Time(now)))

	t.Logf("transaction complete, message sent")

	t.Log("Wait for message delivery")
	require.NoErrorf(t, wait.For(func() (bool, error) {
		lock.Lock()
		defer lock.Unlock()

		// For same consumer group, only one should be called
		if !handlerCalled["Ia1"] && !handlerCalled["Ia2"] {
			t.Logf("Still waiting for one of the Ia handlers to be called")
			return false, nil
		}

		// The handler that was called should also be retried
		if handlerCalled["Ia1"] && !handlerRetried["Ia1"] {
			t.Logf("Still waiting for Ia1 to be retried")
			return false, nil
		}
		if handlerCalled["Ia2"] && !handlerRetried["Ia2"] {
			t.Logf("Still waiting for Ia2 to be retried")
			return false, nil
		}

		// For different consumer groups, both should be called
		if !handlerCalled["Ib1"] || !handlerCalled["Ic1"] {
			t.Logf("Still waiting for Ib1 and/or Ic1 to be called: Ib1=%v, Ic1=%v",
				handlerCalled["Ib1"], handlerCalled["Ic1"])
			return false, nil
		}

		// Both should be retried
		if !handlerRetried["Ib1"] || !handlerRetried["Ic1"] {
			t.Logf("Still waiting for Ib1 and/or Ic1 to be retried: Ib1=%v, Ic1=%v",
				handlerRetried["Ib1"], handlerRetried["Ic1"])
			return false, nil
		}

		return true, nil
	}, wait.WithLogger(t.Logf), wait.WithLimit(DeliveryTimeout), wait.WithMinInterval(time.Millisecond*500),
		wait.ExitOnError(true), wait.WithMaxInterval(time.Second*5), wait.WithBackoff(1.04),
		wait.WithReports(20)), "idempotent message delivery")

	t.Log("Verify one call for same consumer group")
	lock.Lock()
	sameGroupCalls := callCounts["Ia1"] + callCounts["Ia2"]
	differentGroupCalls := callCounts["Ib1"] > 0 && callCounts["Ic1"] > 0
	lock.Unlock()

	assert.Equal(t, 2, sameGroupCalls, "Should have exactly 2 calls (1 initial + 1 retry) across Ia1 and Ia2")
	assert.True(t, differentGroupCalls, "Both Ib1 and Ic1 should have been called")

	assert.Zero(t, lib1.ProduceSyncCount.Load())
	assert.Zero(t, lib2.ProduceSyncCount.Load())
}

// ExactlyOnceDeliveryTest verifies:
// - That exactly-once events are delivered to exactly one consumer in a consumer group
// - That returning an error from a handler temporarily will not stop eventual message delivery
func ExactlyOnceDeliveryTest[
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

	baseT := t
	t = ntest.ExtraDetailLogger(t, string(prefix)+"TEOD")

	lib1 := events.New[ID, TX, DB]()
	lib1.SetEnhanceDB(true)
	lib1.SkipNotifierSupport()
	lib1.SetPrefix(string(prefix))
	lib2 := events.New[ID, TX, DB]()
	lib2.SetEnhanceDB(true)
	lib2.SkipNotifierSupport()
	lib2.SetPrefix(string(prefix))

	lib1.Configure(conn, ntest.ExtraDetailLogger(baseT, string(prefix)+"TEOD-1"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	lib2.Configure(conn, ntest.ExtraDetailLogger(baseT, string(prefix)+"TEOD-2"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	conn.AugmentWithProducer(lib1)

	topic := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t) + "Topic")
	lib1.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})
	lib2.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})

	var lock sync.Mutex
	handlerCalled := make(map[string]bool)
	handlerRetried := make(map[string]bool)
	callCounts := make(map[string]int)

	id := uuid.New().String()
	t.Log("id is", id)

	now := time.Now().Round(time.Second).UTC()

	mkExactlyOnceHandler := func(name string) func(context.Context, TX, eventmodels.Event[MyEvent]) error {
		return func(ctx context.Context, _ TX, event eventmodels.Event[MyEvent]) error {
			if event.ID != id {
				t.Logf("received callback for %s but id is wrong (%s vs %s)", name, event.ID, id)
				return nil
			}

			t.Logf("received callback for %s, already failed: %v", name, handlerCalled[name])
			assert.Equal(t, now.Format(time.RFC3339), event.Timestamp.UTC().Format(time.RFC3339), "timestamp for %s", name)

			lock.Lock()
			defer lock.Unlock()

			callCounts[name]++

			if handlerCalled[name] {
				handlerRetried[name] = true
				return nil
			}

			handlerCalled[name] = true
			return errors.Errorf("failing %s first attempt", name)
		}
	}

	// Same consumer group across libraries
	lib1.ConsumeExactlyOnce(events.NewConsumerGroup(Name(t)+"-exactlyonce"), eventmodels.OnFailureBlock, "EO1", topic.HandlerTx(mkExactlyOnceHandler("EO1")))
	lib2.ConsumeExactlyOnce(events.NewConsumerGroup(Name(t)+"-exactlyonce"), eventmodels.OnFailureBlock, "EO2", topic.HandlerTx(mkExactlyOnceHandler("EO2")))

	// Start consumers and producers
	produceDone, err := lib1.CatchUpProduce(ctx, time.Second*5, 64)
	require.NoError(t, err, "start catch up")

	started1, done1, err := lib1.StartConsuming(ctx)
	require.NoError(t, err, "start consuming lib1")
	started2, done2, err := lib2.StartConsuming(ctx)
	require.NoError(t, err, "start consuming lib2")

	WaitFor(ctx, t, "consumer1", started1, StartupTimeout)
	WaitFor(ctx, t, "consumer2", started2, StartupTimeout)

	// Clean up when done
	defer func() {
		t.Log("cancel")
		cancel()
		t.Log("wait done1")
		<-done1
		t.Log("wait done2")
		<-done2
		t.Log("wait produce")
		<-produceDone
		t.Log("done waits")
	}()

	// Send test message
	require.NoErrorf(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(topic.Event("key-"+id, MyEvent{
			S: "exactly-once-test",
		}).
			ID(id).
			Subject(Name(t) + "-subject").
			Time(now))
		t.Logf("added event to transaction")
		return nil
	}), "transact/send")
	t.Logf("transaction complete, message sent")

	// Wait for message delivery with clear verification
	require.NoErrorf(t, wait.For(func() (bool, error) {
		lock.Lock()
		defer lock.Unlock()

		// Only one of the exactly-once handlers should be called
		if !handlerCalled["EO1"] && !handlerCalled["EO2"] {
			t.Logf("Still waiting for one of the EO handlers to be called")
			return false, nil
		}

		// The handler that was called should also be retried
		if handlerCalled["EO1"] && !handlerRetried["EO1"] {
			t.Logf("Still waiting for EO1 to be retried")
			return false, nil
		}
		if handlerCalled["EO2"] && !handlerRetried["EO2"] {
			t.Logf("Still waiting for EO2 to be retried")
			return false, nil
		}

		return true, nil
	}, wait.WithLogger(t.Logf), wait.WithLimit(DeliveryTimeout), wait.WithMinInterval(time.Millisecond*500),
		wait.ExitOnError(true), wait.WithMaxInterval(time.Second*5), wait.WithBackoff(1.04),
		wait.WithReports(20)), "exactly-once message delivery")

	// Verify only one consumer received the message
	lock.Lock()
	totalHandlerCalls := 0
	if handlerCalled["EO1"] {
		totalHandlerCalls++
	}
	if handlerCalled["EO2"] {
		totalHandlerCalls++
	}
	lock.Unlock()

	assert.Equal(t, 1, totalHandlerCalls, "Only one of the exactly-once handlers should have been called")

	assert.Zero(t, lib1.ProduceSyncCount.Load())
	assert.Zero(t, lib2.ProduceSyncCount.Load())
}

// CloudEventEncodingTest verifies:
//
// That non-CloudEvent messages present with CloudEvent fields.
//
// That both primary encodings of CloudEvent messages result in the same Event.
//
// That the messages generated by produce are valid CloudEvent messages.
func CloudEventEncodingTest[
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
	t = ntest.ExtraDetailLogger(t, string(prefix)+"TCEE")
	lib := events.New[ID, TX, DB]()
	lib.SkipNotifierSupport()
	lib.SetPrefix(string(prefix))
	var lock sync.Mutex

	type myEvent map[string]string
	received := make(map[string][]eventmodels.Event[myEvent])

	topic := eventmodels.BindTopic[myEvent](Name(t) + "Topic")
	lib.SetTopicConfig(kafka.TopicConfig{Topic: topic.Topic()})

	lib.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentA"), eventmodels.OnFailureBlock, Name(t), topic.Handler(
		func(ctx context.Context, e eventmodels.Event[myEvent]) error {
			lock.Lock()
			defer lock.Unlock()
			t.Logf("recevied event %s", e.ID)
			received[e.ID] = append(received[e.ID], e)
			return nil
		}))

	lib.Configure(conn, t, false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	done := lib.StartConsumingOrPanic(ctx)

	defer func() {
		t.Log("cancel")
		cancel()
		t.Log("wait done")
		<-done
		t.Log("done waiting")
	}()

	body := myEvent{
		"f1": "v1",
		"f2": "v2",
	}
	encBody, err := json.Marshal(body)

	require.NoError(t, err, "body enc")

	now := time.Now()

	id1 := uuid.New().String()
	t.Logf("message 1 %s is sent the normal way", id1)
	require.NoError(t, lib.Produce(ctx, eventmodels.ProduceImmediate, topic.Event(id1, body).
		ID(id1).
		Time(now)))

	t.Logf("msg1 (%s) sent", id1)

	id2 := uuid.New().String()
	t.Logf("message 2 %s is manually created and uses cloudevents+json with a base64 body", id2)
	msg2Headers := []kafka.Header{
		{
			Key:   "content-type",
			Value: []byte("application/cloudevents+json; charset=UTF-8"),
		},
	}
	encMsg2, err := json.Marshal(map[string]string{
		"specversion":     "1.0",
		"type":            Name(t) + "Topic",
		"source":          Name(t) + "Topic",
		"id":              id2,
		"datacontenttype": "application/json",
		"time":            now.UTC().Format(time.RFC3339),
		"data_base64":     base64.StdEncoding.EncodeToString(encBody),
	})
	require.NoError(t, err, "msg2 enc")
	t.Logf("msg2 encoding: %s", string(encMsg2))

	id3 := uuid.New().String()
	t.Logf("message 2 %s is manually created and uses cloudevents+json with a json body", id3)
	msg3Headers := []kafka.Header{
		{
			Key:   "content-type",
			Value: []byte("application/cloudevents+json; charset=UTF-8"),
		},
	}
	encMsg3, err := json.Marshal(map[string]string{
		"specversion":     "1.0",
		"type":            topic.Topic(),
		"source":          topic.Topic(),
		"id":              id3,
		"datacontenttype": "application/json",
		"time":            now.UTC().Format(time.RFC3339),
		"data":            string(encBody),
	})
	require.NoError(t, err, "msg3 enc")
	t.Logf("msg3 encoding: %s", string(encMsg3))

	id4 := uuid.New().String()
	t.Logf("message 2 %s is manually created and uses cloudevents headers and a json body", id4)
	msg4Headers := []kafka.Header{
		{
			Key:   "content-type",
			Value: []byte("application/json; charset=UTF-8"),
		},
		{Key: "ce_specversion", Value: []byte("1.0")},
		{Key: "ce_type", Value: []byte(topic.Topic())},
		{Key: "ce_source", Value: []byte(topic.Topic())},
		{Key: "ce_id", Value: []byte(id4)},
		{Key: "ce_time", Value: []byte(now.UTC().Format(time.RFC3339))},
	}

	messages := []kafka.Message{
		{
			Topic:   string(prefix) + topic.Topic(),
			Key:     []byte(id2),
			Headers: msg2Headers,
			Value:   encMsg2,
		},
		{
			Topic:   string(prefix) + topic.Topic(),
			Key:     []byte(id3),
			Headers: msg3Headers,
			Value:   encMsg3,
		},
		{
			Topic:   string(prefix) + topic.Topic(),
			Key:     []byte(id4),
			Headers: msg4Headers,
			Value:   encBody,
		},
	}
	require.NoErrorf(t, wait.For(func() (b bool, err error) {
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
		})
		defer func() {
			e := writer.Close()
			if e != nil && err == nil {
				err = e
			}
		}()
		err = writer.WriteMessages(ctx, messages...)
		if err == nil {
			t.Logf("msg2 (%s) & msg3 (%s) & msg4 (%s) sent", id2, id3, id4)
			return true, nil
		}
		t.Logf("Write messages received error: %s", err)
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			return false, nil
		}
		return false, err
	}, wait.WithLogger(t.Logf), wait.WithLimit(DeliveryTimeout), wait.ExitOnError(true), wait.WithReports(50)), "sent messages")

	ids := []string{id1, id2, id3, id4}

	require.NoErrorf(t, wait.For(func() (bool, error) {
		select {
		case <-done:
			require.FailNow(t, "predone", "consumer finished too soon")
		default:
		}
		lock.Lock()
		defer lock.Unlock()
		for i, id := range ids {
			if len(received[id]) == 0 {
				t.Logf("still waiting for id%d %s", i+1, id)
				return false, nil
			}
		}
		t.Logf("all found")
		return true, nil
	}, wait.WithLogger(t.Logf), wait.WithLimit(DeliveryTimeout), wait.WithMinInterval(time.Second), wait.ExitOnError(true), wait.WithMaxInterval(time.Second*30), wait.WithBackoff(1.05), wait.WithReports(50)), "everything delivered")

	for i, id := range ids {
		for _, meta := range received[id] {
			assert.Equalf(t, Name(t)+"Topic", meta.Topic, "topic msg%d %s", i+1, id)
			assert.Equalf(t, id, meta.Key, "key msg%d %s", i+1, id)
			assert.Equalf(t, id, meta.ID, "id msg%d %s", i+1, id)
			assert.Equalf(t, body, meta.Payload, "payload msg%d %s", i+1, id)
			assert.Equalf(t, now.UTC().Format(time.RFC3339), meta.Timestamp.UTC().Format(time.RFC3339), "timstamp msg%d %s", i+1, id)
			assert.Contains(t, meta.ContentType, "application/json", meta.ContentType, "contentType msg%d %s", i+1, id)
			assert.Equalf(t, id, meta.Subject, "subject msg%d %s", i+1, id)
			assert.Equalf(t, Name(t)+"Topic", meta.Type, "type msg%d %s", i+1, id)
			assert.Equalf(t, "1.0", meta.SpecVersion, "specVersion msg%d %s", i+1, id)
			assert.Equalf(t, Name(t)+"-idempotentA", meta.ConsumerGroup, "consumerGroup msg%d %s", i+1, id)
			assert.Equalf(t, Name(t), meta.HandlerName, "handlerName msg%d %s", i+1, id)
		}
	}
}
