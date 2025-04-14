package eventtest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
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

// EventDeliveryTest verifies:
//
// That if there are multiple instances of
// events.Library, all instances will deliver copies of a broadcast event.
//
// That if there are multiple instances of events.Library, that non-broadcast
// events will be delivered only once.
//
// That returning error from a handler temporarily will not stop eventual message delivery.
//
// That two consumers of the same topic with different consumer
// group names will both have the message deliverd to them.
func EventDeliveryTest[
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
	baseT := t
	t = ntest.ExtraDetailLogger(t, "TED-T")
	lib1 := events.New[ID, TX, DB]()
	lib1.SetEnhanceDB(true)
	lib2 := events.New[ID, TX, DB]()
	lib2.SetEnhanceDB(true)
	lib1.Configure(conn, ntest.ExtraDetailLogger(baseT, "TED-1"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	lib2.Configure(conn, ntest.ExtraDetailLogger(baseT, "TED-2"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	conn.AugmentWithProducer(lib1)

	topic := eventmodels.BindTopicTx[MyEvent, ID, TX, DB](Name(t))

	var lock sync.Mutex
	failed := make(map[string]bool)
	successCount := make(map[string]int)

	id := uuid.New().String()
	t.Log("id is", id)
	id2 := uuid.New().String()
	t.Log("id2 is", id2)

	now := time.Now().Round(time.Second).Add(-2 * time.Minute).Round(time.Microsecond).UTC() // validating that timestamps are propagated

	mkCallback := func(name string) func(context.Context, eventmodels.Event[MyEvent]) error {
		return func(_ context.Context, event eventmodels.Event[MyEvent]) error {
			if event.ID != id {
				t.Logf("received callback for %s/%s but id is wrong (%s vs %s)", name, event.Topic, event.ID, id)
				return nil
			}
			t.Logf("received callback for %s/%s, already failed: %v (will deliberately fail once)", name, event.Topic, failed[name])
			assert.Equalf(t, now.Format(time.RFC3339Nano), event.Timestamp.UTC().Format(time.RFC3339Nano), "callback for %s/%s", name, event.Topic)
			lock.Lock()
			defer lock.Unlock()
			if failed[name] {
				successCount[name]++
				return nil
			}
			failed[name] = true
			return errors.Errorf("failing (on purpose) %s", name)
		}
	}

	lib1.ConsumeExactlyOnce(events.NewConsumerGroup(Name(t)+"-exactlyonce"), eventmodels.OnFailureBlock, "EO1", topic.HandlerTx(
		func(ctx context.Context, _ TX, e eventmodels.Event[MyEvent]) error {
			return mkCallback("EO1")(ctx, e)
		}))
	lib2.ConsumeExactlyOnce(events.NewConsumerGroup(Name(t)+"-exactlyonce"), eventmodels.OnFailureBlock, "EO2", topic.HandlerTx(
		func(ctx context.Context, _ TX, e eventmodels.Event[MyEvent]) error {
			return mkCallback("EO2")(ctx, e)
		}))
	lib1.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentA"), eventmodels.OnFailureBlock, "Ia1", topic.Handler(mkCallback("Ia1")))
	lib2.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentA"), eventmodels.OnFailureBlock, "Ia1", topic.Handler(mkCallback("Ia1")))
	lib1.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentB"), eventmodels.OnFailureBlock, "Ib1", topic.Handler(mkCallback("Ib1")))
	lib2.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentB"), eventmodels.OnFailureBlock, "Ib2", topic.Handler(mkCallback("Ib2")))
	lib1.ConsumeBroadcast("B1", topic.Handler(mkCallback("B1")))
	lib1.ConsumeBroadcast("B2", topic.Handler(mkCallback("B2")))
	lib2.ConsumeBroadcast("B3", topic.Handler(mkCallback("B3")))
	lib2.ConsumeBroadcast("B4", topic.Handler(mkCallback("B4")))

	receivedSequence := make(map[string]int)
	seqNo := 1
	lib2.ConsumeBroadcast("B5", topic.Handler(func(_ context.Context, event eventmodels.Event[MyEvent]) error {
		t.Logf("recevied callback for B5: %s", event.ID)
		lock.Lock()
		defer lock.Unlock()
		receivedSequence[event.ID] = seqNo
		seqNo++
		return nil
	}))

	produceDone, err := lib1.CatchUpProduce(ctx, time.Second*5, 64)
	require.NoError(t, err, "start catch up")

	started1, done1, err := lib1.StartConsuming(ctx)
	require.NoError(t, err, "start consuming")
	started2, done2, err := lib2.StartConsuming(ctx)
	require.NoError(t, err, "start consuming")

	WaitFor(ctx, t, "consumer1", started1, StartupTimeout)
	WaitFor(ctx, t, "consumer2", started2, StartupTimeout)

	require.NotEqual(t, lib1.GetBroadcastConsumerGroupName(), lib2.GetBroadcastConsumerGroupName(), "broadcast consumer group names")

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

	require.NoErrorf(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(topic.Event("key1-"+id, MyEvent{
			S: "seven",
		}).
			ID(id).
			Subject(Name(t) + "subject").
			Time(now),
		)
		tx.Produce(topic.Event("key2"+id2, MyEvent{
			S: "eight",
		}).
			ID(id2).
			Subject(Name(t) + "subject2").
			Time(now),
		)
		t.Logf("added events to transaction")
		return nil
	}), "transact/send")
	t.Logf("transaction complete, message sent")

	expectations := map[string]struct {
		broadcast bool
		overage   bool
	}{
		"EO": {broadcast: false, overage: false},
		"Ia": {broadcast: false, overage: true},
		"B":  {broadcast: true, overage: true},
	}

	require.NoErrorf(t, wait.For(func() (bool, error) {
		lock.Lock()
		defer lock.Unlock()
		for prefix, want := range expectations {
			var sum int
			var failCount int
			for _, postfix := range []string{"1", "2"} {
				if failed[prefix+postfix] {
					failCount++
				} else if want.broadcast {
					t.Logf("Still waiting for failure call for %s%s", prefix, postfix)
					return false, nil
				}
				if successCount[prefix+postfix] > 1 && !want.overage {
					return false, errors.Errorf("too many deliveries for %s%s: %d", prefix, postfix, successCount[prefix+postfix])
				}
				sum += successCount[prefix+postfix]
			}
			if failCount == 0 {
				t.Logf("Still waiting for failure call for %s", prefix)
				return false, nil
			}
			target := 1
			if want.broadcast {
				target = 2
			}
			if sum < target {
				t.Logf("Still waiting for calls for %s %dof%d", prefix, sum, target)
				return false, nil
			}
		}
		for _, id := range []string{id, id2} {
			if receivedSequence[id] == 0 {
				t.Logf("Still waiting for B3 to get %s", id)
				return false, nil
			}
		}
		return true, nil
	}, wait.WithLogger(t.Logf), wait.WithLimit(DeliveryTimeout), wait.WithMinInterval(time.Millisecond*500), wait.ExitOnError(true), wait.WithMaxInterval(time.Second*20), wait.WithBackoff(1.04), wait.WithReports(50)), "everything delivered")
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
) {
	t = ntest.ExtraDetailLogger(t, "TCEE")
	lib := events.New[ID, TX, DB]()
	var lock sync.Mutex

	type myEvent map[string]string
	received := make(map[string][]eventmodels.Event[myEvent])

	topic := eventmodels.BindTopic[myEvent](Name(t))

	lib.ConsumeIdempotent(events.NewConsumerGroup(Name(t)+"-idempotentA"), eventmodels.OnFailureBlock, Name(t), topic.Handler(
		func(ctx context.Context, e eventmodels.Event[myEvent]) error {
			lock.Lock()
			defer lock.Unlock()
			t.Logf("recevied event %s", e.ID)
			received[e.ID] = append(received[e.ID], e)
			return nil
		}))

	lib.Configure(conn, t, false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
	produceDone, err := lib.CatchUpProduce(ctx, time.Second*5, 64)
	require.NoError(t, err, "start catch up")
	done := lib.StartConsumingOrPanic(ctx)

	defer func() {
		t.Log("cancel")
		cancel()
		t.Log("wait done")
		<-done
		t.Log("wait produce")
		<-produceDone
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
	t.Logf("message 1 %s is sent the normal way with a transaction", id1)
	require.NoErrorf(t, conn.Transact(ctx, func(tx TX) error {
		tx.Produce(topic.Event(id1, body).
			ID(id1).
			Time(now),
		)
		t.Logf("added events to transaction")
		return nil
	}), "transact/send")
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
		"type":            Name(t),
		"source":          Name(t),
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
			Topic:   topic.Topic(),
			Key:     []byte(id2),
			Headers: msg2Headers,
			Value:   encMsg2,
		},
		{
			Topic:   topic.Topic(),
			Key:     []byte(id3),
			Headers: msg3Headers,
			Value:   encMsg3,
		},
		{
			Topic:   topic.Topic(),
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
			assert.Equalf(t, Name(t), meta.Topic, "topic msg%d %s", i+1, id)
			assert.Equalf(t, id, meta.Key, "key msg%d %s", i+1, id)
			assert.Equalf(t, id, meta.ID, "id msg%d %s", i+1, id)
			assert.Equalf(t, body, meta.Payload, "payload msg%d %s", i+1, id)
			assert.Equalf(t, now.UTC().Format(time.RFC3339), meta.Timestamp.UTC().Format(time.RFC3339), "timstamp msg%d %s", i+1, id)
			assert.Contains(t, meta.ContentType, "application/json", meta.ContentType, "contentType msg%d %s", i+1, id)
			assert.Equalf(t, id, meta.Subject, "subject msg%d %s", i+1, id)
			assert.Equalf(t, Name(t), meta.Type, "type msg%d %s", i+1, id)
			assert.Equalf(t, "1.0", meta.SpecVersion, "specVersion msg%d %s", i+1, id)
			assert.Equalf(t, Name(t)+"-idempotentA", meta.ConsumerGroup, "consumerGroup msg%d %s", i+1, id)
			assert.Equalf(t, Name(t), meta.HandlerName, "handlerName msg%d %s", i+1, id)
		}
	}
}
