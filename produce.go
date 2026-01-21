package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
)

var debugProduce = os.Getenv("EVENTS_DEBUG_PRODUCE") == "true"

// Produce sends events directly to Kafka. It is not transactional. Use tx.Produce to produce
// from within a transaction.
func (lib *Library[ID, TX, DB]) Produce(ctx context.Context, method eventmodels.ProduceMethod, events ...eventmodels.ProducingEvent) (err error) {
	ctx, spanDone := lib.tracerConfig.BeginSpan(ctx, map[string]string{
		"action": "produce",
	})
	defer spanDone()
	if len(events) == 0 {
		return nil
	}
	if debugProduce {
		defer func() {
			for _, event := range events {
				if err != nil {
					lib.logf(ctx, "[events] failed produce %s / %s: %s", event.GetTopic(), event.GetKey(), err)
				} else {
					lib.logf(ctx, "[events] produced %s / %s", event.GetTopic(), event.GetKey())
				}
			}
		}()
	}
	err = lib.start(ctx, "produce events (%d)", len(events))
	if err != nil {
		return lib.RecordError(ctx, "produceNotReady", err)
	}

	messages := make([]kafka.Message, len(events))
	for i, event := range events {
		topic := event.GetTopic()
		messages[i].Topic = lib.addPrefix(topic)
		ProduceTopicCounts.WithLabelValues(topic, messages[i].Topic).Inc()
		messages[i].Key = []byte(event.GetKey())
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
			return lib.RecordErrorNoWait(ctx, "marshalEvents", errors.Errorf("cannot marshal event (%T %s) to produce in topic (%s): %w", event, string(messages[i].Key), messages[i].Topic, err))
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

	err = lib.prepareToProduce(ctx, messages)
	if err != nil {
		return lib.RecordErrorNoWait(ctx, "createTopics", errors.Errorf("unable (%s) to produce (%d) events: %w", events[0].GetTopic(), len(events), err))
	}

	err = lib.writer.WriteMessages(ctx, messages...)
	if err != nil {
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			lib.logf(ctx, "[events] got an unknown topic or partition error when writing %d message(s) that have known good topics. Retrying with transactional fallback...", len(messages))
			return lib.transactionalFallbackWrite(ctx, messages)
		}
		return lib.RecordErrorNoWait(ctx, "produceEvents", errors.Errorf("cannot produce messages (%d, example topic %s) to Kafka: %w", len(messages), messages[0].Topic, err))
	}
	return nil
}

// transactionalFallbackWrite attempts transactional writes across brokers when the main code gets an unknown topic/partition error
// It's possible that WriteMessages (above) may have had a partial success. So that we don't end up
// with lots of duplicates, when do our redo attempts, we'll do so inside a Kafka transaction so that
// there is at most one duplicate of each message.
func (lib *Library[ID, TX, DB]) transactionalFallbackWrite(ctx context.Context, messages []kafka.Message) error {
	if len(lib.brokers) == 0 || lib.brokers[0] == "" {
		return errors.Errorf("no brokers available for transactional fallback")
	}

	// Group messages by topic for logging
	topicSet := make(map[string]struct{})
	for _, msg := range messages {
		topicSet[msg.Topic] = struct{}{}
	}
	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	lib.logf(ctx, "[events] attempting transactional fallback for %d messages across topics %v", len(messages), topics)

	// Try each broker in sequence
	for _, i := range rand.Perm(len(lib.brokers)) {
		broker := lib.brokers[i]
		lib.logf(ctx, "[events] trying transactional write to broker %d/%d: %s", i+1, len(lib.brokers), broker)

		err := lib.tryTransactionalWriteWithBroker(ctx, broker, messages)
		if err == nil {
			lib.logf(ctx, "[events] transactional fallback completed successfully with broker %s", broker)
			return nil
		}

		lib.logf(ctx, "[events] transactional write failed with broker %s: %v", broker, err)
	}

	lib.logf(ctx, "[events] transactional fallback failed on all %d brokers", len(lib.brokers))
	return errors.Errorf("transactional fallback failed on all %d brokers", len(lib.brokers))
}

// tryTransactionalWriteWithBroker attempts a transactional write with a specific broker
func (lib *Library[ID, TX, DB]) tryTransactionalWriteWithBroker(ctx context.Context, broker string, messages []kafka.Message) error {
	// Create unique transaction ID for this attempt
	txID := fmt.Sprintf("fallback-tx-%d-%d", os.Getpid(), time.Now().UnixNano())

	lib.logf(ctx, "[events] creating transactional writer for broker %s with transaction ID %s", broker, txID)

	// Create a transactional writer for this specific broker
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Transport:    lib.transport(),
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: transactionalBatchTimeout,
		WriteTimeout: transactionalWriteTimeout,
		ReadTimeout:  transactionalReadTimeout,
	}

	// Create context with timeout for the entire operation
	ctx, cancel := context.WithTimeout(ctx, transactionalOperationTimeout)
	defer cancel()

	lib.logf(ctx, "[events] writing %d messages transactionally to broker %s", len(messages), broker)

	// Write messages - the writer handles the transaction lifecycle
	err := writer.WriteMessages(ctx, messages...)
	if err != nil {
		// Close writer to clean up any state
		_ = writer.Close()
		return errors.Errorf("failed to write messages to broker %s: %w", broker, err)
	}

	// Close writer
	err = writer.Close()
	if err != nil {
		return errors.Errorf("failed to close writer for broker %s: %w", broker, err)
	}

	lib.logf(ctx, "[events] successfully wrote %d fallback transactional messages to broker %s", len(messages), broker)

	return nil
}

// ProduceFromTable is used to send events that have been written during a
// transaction. If a CatchUpProducer is running, the events will be forwarded
// to that thread. If not, they'll be sent to Kafka synchronously. Sending to
// Kafka synchronously is slow.
//
// ProduceFromTable can only be used after Configure.
func (lib *Library[ID, TX, DB]) ProduceFromTable(ctx context.Context, eventsByTopic map[string][]ID) error {
	ctx, spanDone := lib.tracerConfig.BeginSpan(ctx, map[string]string{
		"action": "produce from table",
	})
	defer spanDone()
	err := lib.start(ctx, "produce events from table")
	if err != nil {
		return lib.RecordError(ctx, "produceNotReady", err)
	}
	if len(eventsByTopic) == 0 {
		return nil
	}
	var eventCount int
	for _, events := range eventsByTopic {
		eventCount += len(events)
	}
	if eventCount == 0 {
		return nil
	}
	eventIDs := make([]ID, 0, eventCount)
	for _, events := range eventsByTopic {
		eventIDs = append(eventIDs, events...)
	}
	err = lib.ValidateTopics(ctx, generic.Keys(eventsByTopic))
	if err != nil {
		return err
	}
	if lib.producerRunning.Load() != 0 {
		select {
		case lib.produceFromTable <- eventIDs:
			ProduceFromTxSplit.WithLabelValues("async").Inc()
			return nil
		default:
			// the channel is full, we're going to let CatchUpProduce
			// handle it
			if debugProduce {
				lib.logf(ctx, "[events] debug: produce from table channel is full")
			}
			ProduceFromTxSplit.WithLabelValues("catch-up").Inc()
			return nil
		}
	}
	if lib.lazyProduce {
		return nil
	}
	if !lib.HasDB() {
		return errors.Errorf("cannot produce from table with nil db")
	}
	if debugProduce {
		lib.logf(ctx, "[events] debug: produceFromTable producing synchronously because no producer is running")
	}
	ProduceFromTxSplit.WithLabelValues("sync").Inc()
	_ = lib.ProduceSyncCount.Add(uint64(len(eventIDs)))
	_, err = lib.db.ProduceSpecificTxEvents(ctx, eventIDs)
	return err
}

// CatchUpProduce starts a background thread that looks for events that were written to the
// database during a transaction but were not sent to Kafka
//
// The returned channel is closed when CatchUpProduce shuts down (due to context cancel)
//
// CatchUpProduce can only be used after Configure.
func (lib *Library[ID, TX, DB]) CatchUpProduce(ctx context.Context, sleepTime time.Duration, batchSize int) (chan struct{}, error) {
	setupCtx, spanDone := lib.tracerConfig.BeginSpan(ctx, map[string]string{
		"action": "catch up produce",
	})
	defer spanDone()
	done := make(chan struct{})
	err := lib.start(setupCtx, "catch up produce")
	if err != nil {
		close(done)
		return done, err
	}
	func() {
		lib.lock.Lock()
		defer lib.lock.Unlock()
		lib.produceCtx = ctx
	}()
	if !lib.HasDB() {
		close(done)
		return done, errors.Alertf("attempt to produce dropped events in library that does not embed a database")
	}
	lib.logf(setupCtx, "[events] Catch-up background producer started")
	_ = lib.producerRunning.Add(1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		ctx, spanDone := lib.tracerConfig.BeginSpan(ctx, map[string]string{
			"action": "thread",
			"thread": "collect batches to produce",
		})
		defer spanDone()
		defer wg.Done()
		send := func(ids []ID, tcount int) {
			count, err := lib.db.ProduceSpecificTxEvents(ctx, ids)
			if err == nil || errors.Is(err, sql.ErrNoRows) {
				lib.logf(ctx, "[events] background producer sent %d out of %d events for %d transactions", count, len(ids), tcount)
			} else {
				_ = lib.RecordError(ctx, "produceTxEvents", err)
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
		ctx, spanDone := lib.tracerConfig.BeginSpan(ctx, map[string]string{
			"action": "thread",
			"thread": "produce dropped tx events",
		})
		defer spanDone()
		if sleepTime == 0 {
			return
		}
		timer := time.NewTimer(sleepTime)
		for {
			_, err := lib.db.ProduceDroppedTxEvents(ctx, batchSize)
			if err != nil {
				_ = lib.RecordErrorNoWait(ctx, "produceTxEvents", errors.Errorf("cannot produce dropped tx events: %w", err))
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
	lib.libraryDone.Add(1)
	go func() {
		wg.Wait()
		lib.libraryDone.Done()
		close(done)
	}()
	return done, nil
}
