// Package eventpg implements the abstract types for PostgreSQL
package eventpg

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
	"github.com/memsql/errors"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventdb"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
)

var debugging = os.Getenv("EVENTS_DEBUG_PRODUCE") == "true"

type Connection[TX eventdb.BasicTX, DB eventdb.BasicDB[TX]] struct {
	eventdb.BasicDB[TX]
	producer eventmodels.Producer[eventmodels.StringEventID, TX]
}

func New[TX eventdb.BasicTX, DB eventdb.BasicDB[TX]](db DB) *Connection[TX, DB] {
	return &Connection[TX, DB]{
		BasicDB: db,
	}
}

var _ eventmodels.AbstractDB[eventmodels.StringEventID, eventdb.BasicTX] = &Connection[eventdb.BasicTX, eventdb.BasicDB[eventdb.BasicTX]]{}

var _ eventmodels.CanAugment[eventmodels.StringEventID, eventdb.BasicTX] = &Connection[eventdb.BasicTX, eventdb.BasicDB[eventdb.BasicTX]]{}

var _ eventdb.BasicDB[eventdb.BasicTX] = &Connection[eventdb.BasicTX, eventdb.BasicDB[eventdb.BasicTX]]{}

func (w *Connection[TX, DB]) AugmentWithProducer(producer eventmodels.Producer[eventmodels.StringEventID, TX]) {
	w.producer = producer
	if augmenter, ok := any(w.BasicDB).(eventmodels.CanAugment[eventmodels.StringEventID, TX]); ok {
		augmenter.AugmentWithProducer(producer)
	}
}

// The methods that Connection supports are all done with stubs so that the underlying implementations can be
// reused by other implementations of AbstractDB.

func (w Connection[TX, DB]) tracer() eventmodels.Tracer {
	if w.producer != nil {
		return w.producer.Tracer()
	}
	return nil
}

func (c *Connection[TX, DB]) Transact(ctx context.Context, f func(TX) error) error {
	return eventdb.Transact[eventmodels.StringEventID, TX, *Connection[TX, DB]](ctx, c, c.tracer(), f, SaveEventsInsideTx[TX], c.producer)
}

func (c *Connection[TX, DB]) LockOrError(ctx context.Context, key uint32, timeout time.Duration) (unlock func() error, err error) {
	return LockOrError[TX, *Connection[TX, DB]](ctx, c, key, timeout)
}

func (c *Connection[TX, DB]) ProduceSpecificTxEvents(ctx context.Context, ids []eventmodels.StringEventID) (int, error) {
	return ProduceSpecificTxEvents(ctx, c, ids, c.producer)
}

func (c *Connection[TX, DB]) ProduceDroppedTxEvents(ctx context.Context, batchSize int) (int, error) {
	return ProduceDroppedTxEvents[TX, *Connection[TX, DB]](ctx, c, batchSize, c.tracer(), c.producer)
}

func (c Connection[TX, DB]) MarkEventProcessed(ctx context.Context, tx TX, topic string, source string, id string, handlerName string) error {
	return MarkEventProcessed[TX](ctx, tx, topic, source, id, handlerName)
}

func (c Connection[TX, DB]) SaveEventsInsideTx(ctx context.Context, tracer eventmodels.Tracer, tx TX, events ...eventmodels.ProducingEvent) (map[string][]eventmodels.StringEventID, error) {
	return SaveEventsInsideTx[TX](ctx, tracer, tx, c.producer, events...)
}

func LockOrError[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, key uint32, timeout time.Duration) (unlock func() error, err error) {
	ereturn := make(chan error)
	done := make(chan struct{})
	go func() {
		start := time.Now()
		_ = db.Transact(ctx, func(tx TX) (err error) {
			var returned bool
			defer func() {
				if !returned {
					ereturn <- err
				}
			}()
			for {
				var locked bool
				err = tx.QueryRowContext(ctx, `
					SELECT pg_try_advisory_xact_lock($1)`, int64(key)).Scan(&locked)
				if err != nil {
					return err
				}
				if locked {
					ereturn <- nil
					returned = true
					break
				}
				since := time.Since(start)
				if since >= timeout {
					return eventmodels.TimeoutErr.Errorf("could not acquire lock after %s", time.Since(start))
				}
				select {
				case <-time.After(timeout / 100):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case <-ctx.Done():
			case <-done:
			}
			return nil
		})
	}()
	df := func() error {
		close(done)
		return nil
	}
	select {
	case <-ctx.Done():
		return df, errors.WithStack(ctx.Err())
	case err := <-ereturn:
		return df, err
	}
}

func ProduceSpecificTxEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, ids []eventmodels.StringEventID, producer eventmodels.Producer[eventmodels.StringEventID, TX]) (int, error) {
	startTime := time.Now()
	count, err := produceEvents[TX, DB](ctx, db, eventmodels.ProduceInTransaction, 0, ids, producer)
	events.ObserveProduceLatency(err, startTime)
	if errors.Is(err, sql.ErrNoRows) {
		return count, nil
	}
	return count, err
}

func ProduceDroppedTxEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, batchSize int, tracer eventmodels.Tracer, producer eventmodels.Producer[eventmodels.StringEventID, TX]) (int, error) {
	if producer == nil {
		return 0, errors.Errorf("attempt to produce dropped events with a connection not augmented with a producer")
	}
	var totalCount int
	for {
		count, err := produceEvents[TX, DB](ctx, db, eventmodels.ProduceCatchUp, batchSize, nil, producer)
		totalCount += count
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if debugging {
				producer.Tracer().Logf("[events] Catch-up background producer found no pending events")
			}
			return totalCount, nil
		case err != nil:
			_ = producer.RecordError("produceStoredEvents", err)
			return totalCount, err
		default:
			producer.Tracer().Logf("[events] Catch-up background producer sent %d events", count)
			if count != batchSize {
				return totalCount, nil
			}
			// go around again and look for more right away
		}
	}
}

// produceEvents looks for stored outgoing events (filtering with the provided
// where clause) and sends them to the message queue. If there are no events that
// match the where clause then sql.ErrNoRows will be returned. Events that are
// sent are deleted.
//
// If ids are provided (optional) then only those ids will be considered an the limit
// parameter is ignored. If no ids are provided, then limit must be greater than
// zero and at most that limit events will be sent.
func produceEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, method eventmodels.ProduceMethod, limit int, ids []eventmodels.StringEventID, producer eventmodels.Producer[eventmodels.StringEventID, TX]) (int, error) {
	if producer == nil {
		return 0, errors.Errorf("attempt to produce events with a connection not augmented with a producer")
	}
	select {
	case <-ctx.Done():
		// They'll have to be sent some other way since this context is closed
		// The CatchUpProducer from some process that isn't shut down will likely pick up these events
		return 0, nil // on purpose
	default:
	}
	sb := sq.
		Select("id").
		From("eventsOutgoing").
		Suffix("FOR UPDATE SKIP LOCKED")

	if len(ids) == 0 {
		// It's possible that this may lock more rows than are returned. If
		// that happens, the extra rows will be unlocked when the transaction
		// completes and some other producer can send them. It's better to
		// over-lock than to under-limit because filling the limit is a signal
		// to the CatchUpProducer that ProduceEvents should be called again
		// right away.
		sb = sb.Limit(uint64(limit)).
			OrderBy("ts ASC", "sequenceNumber ASC")
	} else {
		sb = sb.Where(sq.Eq{"id": ids})
	}
	q, args, err := sb.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return 0, errors.Errorf("[Events] cannot build query to produce events: %w", err)
	}
	q = `WITH selection AS (` + q + `),
	     deleted AS (
		DELETE FROM eventsOutgoing o
		WHERE id IN (SELECT id FROM selection)
		RETURNING o.id, o.topic, o.ts, o.sequenceNumber, o.key, o.data, o.headers)
	     SELECT * FROM deleted ORDER BY ts, sequenceNumber`
	var count int
	err = db.Transact(ctx, func(tx TX) (finalErr error) {
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return errors.Errorf("cannot delete produced events: %w\nquery was %s", err, q)
		}
		defer func() {
			err := rows.Close()
			if finalErr == nil && err != nil {
				finalErr = errors.Errorf("cannot close delete events produce cursor: %w", err)
			}
		}()
		var toProduce []*eventdb.ProducingEventBlob[eventmodels.StringEventID]
		for rows.Next() {
			var blob eventdb.ProducingEventBlob[eventmodels.StringEventID]
			err := rows.Scan(&blob.ID, &blob.KafkaTopic, &blob.TS, &blob.SequenceNumber, &blob.K, &blob.Data, &blob.EncodedHeader)
			if err != nil {
				return errors.Errorf("cannot scan list of events to produce: %w", err)
			}
			err = json.Unmarshal(blob.EncodedHeader, &blob.HeaderMap)
			if err != nil {
				return errors.Errorf("produce cannot unmarshal event header map: %w", err)
			}
			if _, ok := blob.HeaderMap["id"]; !ok {
				blob.HeaderMap["id"] = []string{blob.ID.String()}
			}
			toProduce = append(toProduce, &blob)
		}
		if len(toProduce) == 0 {
			return sql.ErrNoRows
		}
		count = len(toProduce)
		err = producer.Produce(ctx, method, generic.TransformSlice(toProduce,
			func(blob *eventdb.ProducingEventBlob[eventmodels.StringEventID]) eventmodels.ProducingEvent {
				return eventmodels.ProducingEvent(blob)
			})...)
		if err != nil {
			return err
		}
		return nil
	})
	return count, err
}

func MarkEventProcessed[TX eventmodels.AbstractTX](ctx context.Context, tx TX, topic string, source string, id string, handlerName string) error {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO eventsProcessed (topic, source, id, handlerName, processedAt)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (topic, source, id, handlerName) DO NOTHING`,
		topic, source, id, handlerName)
	if err != nil {
		return errors.Errorf("consume could not mark event as delivered: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Errorf("consume could not mark event as delivered: %w", err)
	}
	if rowsAffected == 0 {
		return eventmodels.ErrAlreadyProcessed.Errorf("event (%s %s %s) is already delivered to handler (%s)", topic, source, id, handlerName)
	}
	return nil
}

// SaveEventsInsideTx is meant to be used inside a transaction to persist
// events as part of that transaction.
func SaveEventsInsideTx[TX eventmodels.AbstractTX](ctx context.Context, tracer eventmodels.Tracer, tx TX, producer eventmodels.CanValidateTopics, events ...eventmodels.ProducingEvent) (map[string][]eventmodels.StringEventID, error) {
	if len(events) == 0 {
		return nil, nil
	}
	err := eventdb.ValidateEventTopics(ctx, producer, events...)
	if err != nil {
		return nil, err
	}
	ids := eventdb.PreAllocateIDMap[eventmodels.StringEventID](events...)
	ib := sq.Insert("eventsOutgoing").
		Columns("id", "sequenceNumber", "topic", "ts", "key", "data", "headers").
		PlaceholderFormat(sq.Dollar)
	var emptyID eventmodels.StringEventID
	for i, event := range events {
		id := emptyID.New()
		topic := event.GetTopic()
		ids[topic] = append(ids[topic], id)
		headers := event.GetHeaders()
		enc, err := json.Marshal(event)
		if err != nil {
			return nil, errors.Errorf("could not encode event to produce in topic (%s): %w", topic, err)
		}
		headersEnc, err := json.Marshal(headers)
		if err != nil {
			return nil, errors.Errorf("could not encode event headers to produce in topic (%s): %w", topic, err)
		}
		ib = ib.Values(id, i+1, topic, event.GetTimestamp().UTC(), event.GetKey(), enc, headersEnc)
	}
	if tracer != nil {
		tracer.Logf("[events] saving %d events as part of a transaction, example topic: '%s'", len(events), events[0].GetTopic())
	}
	sql, args, err := ib.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		tracer.Logf("[SaveEventsInsideTx] could not insert event: %s: %s", sql, err)
		return nil, errors.WithStack(err)
	}
	return ids, nil
}

func Migrations(database *libschema.Database) {
	database.Migrations("events",
		lspostgres.Script("create-eventsOutgoing-table", `
			CREATE TABLE IF NOT EXISTS eventsOutgoing (
				id		uuid				NOT NULL,
				topic		varchar(255)			NOT NULL,
				ts		timestamp with time zone	NOT NULL,
				sequenceNumber	int				NOT NULL,
				key		text				NOT NULL,
				headers		json				NOT NULL,
				data		json				NOT NULL,
				PRIMARY KEY (id)
			);

			COMMENT ON TABLE eventsOutgoing IS 'ephemeral data, only existing until events are sent to Kafka';

			CREATE INDEX eventsOutgoing_ts_idx ON eventsOutgoing (ts, sequenceNumber);
			`),

		lspostgres.Script("create-eventsProcessed-table", `
			CREATE TABLE IF NOT EXISTS eventsProcessed (
				topic		varchar(255)			NOT NULL,
				handlerName	varchar(255)			NOT NULL,
				source		varchar(255)			NOT NULL,
				id		varchar(255)			NOT NULL,
				processedAt	timestamp with time zone,
				PRIMARY KEY	(topic, source, id, handlerName)
			);

			COMMENT ON TABLE eventsProcessed IS 'persistent data to track exactly-once consumer deliveries';
			`),
	)
}
