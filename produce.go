package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"

	"singlestore.com/helios/codegate"
	"singlestore.com/helios/events/eventmodels"
	"singlestore.com/helios/trace"
	"singlestore.com/helios/util/generic"
)

const debugProduce = false

var NoBlockCodegate = codegate.New("KafkaDownDoesNotBlock")

// Produce sends events directly to Kafka. It is not transactional. Use tx.Produce to produce
// from within a transaction.
func (lib *Library[ID, TX, DB]) Produce(ctx context.Context, method eventmodels.ProduceMethod, events ...eventmodels.ProducingEvent) error {
	if len(events) == 0 {
		return nil
	}
	err := lib.start("produce events (%d)", len(events))
	if err != nil {
		return lib.RecordError("produceNotReady", err)
	}

	err = lib.createTopicsForOutgoingEvents(ctx, events)
	if err != nil {
		return lib.RecordErrorNoWait("createTopics", errors.Errorf("unable to create topic(s) (%s) to produce (%d) events: %w", events[0].GetTopic(), len(events), err))
	}

	messages := make([]kafka.Message, len(events))
	for i, event := range events {
		topic := event.GetTopic()
		messages[i].Topic = topic
		ProduceTopicCounts.WithLabelValues(topic, string(method)).Inc()
		messages[i].Key = []byte(event.GetKey())
		if tracer := trace.FromContextOrNil(ctx); tracer != nil {
			tracer.Logf("[events] produce %s / %s", messages[i].Topic, string(messages[i].Key))
		} else if debugProduce {
			lib.tracer.Logf("[events] produce %s / %s", messages[i].Topic, string(messages[i].Key))
		}
		ts := event.GetTimestamp()
		if !ts.IsZero() {
			messages[i].Time = ts
		}
		var err error
		contentType := "application/json"
		if sme, ok := event.(eventmodels.SelfMarshalingEvent); ok {
			messages[i].Value, contentType, err = sme.Marshal()
		} else {
			messages[i].Value, err = json.Marshal(event)
		}
		if err != nil {
			return lib.RecordErrorNoWait("marshalEvents", errors.Errorf("cannot marshal event (%T %s) to produce in topic (%s): %w", event, string(messages[i].Key), messages[i].Topic, err))
		}
		messages[i].Headers = make([]kafka.Header, 0, len(event.GetHeaders())) // most of the time there is only one value per key
		var seenContentType bool
		for key, values := range event.GetHeaders() {
			if key == "content-type" {
				seenContentType = true
			}
			for _, value := range values {
				messages[i].Headers = append(messages[i].Headers, protocol.Header{
					Key:   key,
					Value: []byte(value),
				})
			}
		}
		if !seenContentType {
			messages[i].Headers = append(messages[i].Headers, protocol.Header{
				Key:   "content-type",
				Value: []byte(contentType),
			})
		}
	}
	err = lib.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return lib.RecordErrorNoWait("produceEvents", errors.Errorf("cannot produce messages (%d, example topic %s) to Kafka: %w", len(messages), messages[0].Topic, err))
	}
	return nil
}

// ProduceFromTable is used to send events that have been written during a
// transaction. If a CatchUpProducer is running, the events will be forwarded
// to that thread. If not, they'll be sent to Kafka synchronously. Sending to
// Kafka synchronously is slow.
func (lib *Library[ID, TX, DB]) ProduceFromTable(ctx context.Context, eventIDs []ID) error {
	if len(eventIDs) == 0 {
		return nil
	}
	if lib.producerRunning.Load() != 0 {
		select {
		case lib.produceFromTable <- eventIDs:
			ProduceFromTxSplit.WithLabelValues("async").Inc()
			return nil
		default:
			if NoBlockCodegate.Enabled() {
				// the channel is full, we're going to let CatchUpProduce
				// handle it
				ProduceFromTxSplit.WithLabelValues("catch-up").Inc()
				return nil
			}
		}
	}
	if lib.lazyProduce {
		return nil
	}
	if lib.db == nil {
		return errors.Errorf("cannot produce from table with nil db")
	}
	ProduceFromTxSplit.WithLabelValues("sync").Inc()
	_ = lib.ProduceSyncCount.Add(uint64(len(eventIDs)))
	_, err := lib.db.ProduceSpecificTxEvents(ctx, eventIDs)
	return err
}

// CatchUpProduce starts a background thread that looks for events that were written to the
// database during a transaction but were not sent to Kafka
//
// The returned channel is closed when CatchUpProduce shuts down (due to context cancel)
func (lib *Library[ID, TX, DB]) CatchUpProduce(ctx context.Context, sleepTime time.Duration, batchSize int) (chan struct{}, error) {
	done := make(chan struct{})
	err := lib.start("produce Kafka messages")
	if err != nil {
		close(done)
		return done, err
	}
	if lib.db == nil {
		close(done)
		return done, errors.Alertf("attempt to produce dropped events in library that does not embed a database")
	}
	lib.tracer.Logf("[events] Catch-up background producer started")
	_ = lib.producerRunning.Add(1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		send := func(ids []ID, tcount int) {
			count, err := lib.db.ProduceSpecificTxEvents(ctx, ids)
			if err == nil || errors.Is(err, sql.ErrNoRows) {
				lib.tracer.Logf("[events] background producer sent %d out of %d events for %d transactions", count, len(ids), tcount)
			} else {
				_ = lib.RecordError("produceTxEvents", err)
			}
		}
		ids := make([]ID, 0, batchSize*2)
		for {
			// We build a batch until either it is full or until there are no more
			// ids available to add to it.
			var tcount int
			synchronous := false
			select {
			case <-ctx.Done():
				return
			case moreIDs := <-lib.produceFromTable:
				tcount = 1
				ids = append(ids, moreIDs...)
			BuildBatch:
				for len(ids) < batchSize {
					select {
					case <-ctx.Done():
						return
					case moreIDs := <-lib.produceFromTable:
						tcount += 1
						ids = append(ids, moreIDs...)
					default:
						// Send synchronously because we've processed the
						// backlog and don't need to race.
						synchronous = true
						break BuildBatch
					}
				}
			}
			if synchronous {
				send(ids, tcount)
			} else {
				// We send in a go-routine because there is latency in sending and we've got
				// a backlog to work through
				go send(generic.CopySlice(ids), tcount)
			}
			ids = ids[:0]
		}
	}()
	go func() {
		defer wg.Done()
		if sleepTime == 0 {
			return
		}
		timer := time.NewTimer(sleepTime)
		for {
			_, err := lib.db.ProduceDroppedTxEvents(ctx, batchSize)
			if err != nil {
				_ = lib.RecordErrorNoWait("produceTxEvents", errors.Errorf("cannot produce dropped tx events: %w", err))
			}
			timer.Reset(sleepTime)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				// back to top of loop
			}
		}
	}()
	go func() {
		wg.Wait()
		close(done)
	}()
	return done, nil
}
