// Package events2 implements the abstract types using SingleStore
package events2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/memsql/errors"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lssinglestore"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventdb"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
)

const (
	produceFromTableBuffer = 512
	minimumLockAttemptTime = 100 * time.Millisecond // DB BEGIN + SELECT FOR UPDATE
	foreverLockAttemptTime = time.Minute * 2
)

var debugging = os.Getenv("EVENTS_DEBUG_PRODUCE") == "true"

type Connection[TX eventdb.BasicTX, DB eventdb.BasicDB[TX]] struct {
	eventdb.BasicDB[TX]
	lockPrefix string
	producer   eventmodels.Producer[eventmodels.BinaryEventID, TX]
}

func New[TX eventdb.BasicTX, DB eventdb.BasicDB[TX]](db DB, lockPrefix string) *Connection[TX, DB] {
	return &Connection[TX, DB]{
		BasicDB:    db,
		lockPrefix: lockPrefix,
	}
}

func Migrations(database *libschema.Database, singlestore *lssinglestore.SingleStore) {
	database.Migrations("events",
		lssinglestore.Script("create-locks-table", `
			CREATE TABLE IF NOT EXISTS eventLocks (
				lockName	VARCHAR(255)			NOT NULL,
				fluf		int,
				PRIMARY KEY (lockName),
				SHARD KEY (lockName)
			)
			COMMENT 'used for SELECT FOR UPDATE, no data ever written'`),

		lssinglestore.Script("create-eventsOutgoing-table", `
			CREATE TABLE IF NOT EXISTS eventsOutgoing (
				id		BINARY(16)			NOT NULL,
				topic		varchar(255)			NOT NULL,
				ts		datetime(6)			NOT NULL,
				sequenceNumber	int				NOT NULL,
				`+"`key`"+`	text				NOT NULL,
				headers		json				NOT NULL,
				data		json				NOT NULL,
				PRIMARY KEY (id),
				SORT KEY (ts, sequenceNumber, id),
				SHARD KEY (id)
			)
			COMMENT 'ephemeral data, only existing until events are sent to Kafka'`),

		lssinglestore.Script("create-eventsOutgoing-ts-index", `
			CREATE INDEX eventsOutgoing_ts_idx ON eventsOutgoing (ts, sequenceNumber)`,
			libschema.SkipIf(func() (bool, error) {
				return singlestore.TableHasIndex("eventsOutgoing", "eventsOutgoing_ts_idx")
			}),
		),

		lssinglestore.Script("create-eventsProcessed-table", `
			CREATE TABLE IF NOT EXISTS eventsProcessed (
				topic		varchar(255)			NOT NULL,
				handlerName	varchar(255)			NOT NULL,
				source		varchar(255)			NOT NULL,
				id		varchar(255)			NOT NULL,
				processedAt	datetime(6),
				PRIMARY KEY	(topic, source, id, handlerName),
				SHARD KEY	(topic, source, id, handlerName)
			)
			COMMENT 'persistent data to track exactly-once consumer deliveries'`),
	)
}

var _ eventmodels.AbstractDB[eventmodels.BinaryEventID, eventdb.BasicTX] = &Connection[eventdb.BasicTX, eventdb.BasicDB[eventdb.BasicTX]]{}

var _ eventmodels.CanAugment[eventmodels.BinaryEventID, eventdb.BasicTX] = &Connection[eventdb.BasicTX, eventdb.BasicDB[eventdb.BasicTX]]{}

func (c *Connection[TX, DB]) AugmentWithProducer(producer eventmodels.Producer[eventmodels.BinaryEventID, TX]) {
	c.producer = producer
	if augmenter, ok := any(c.BasicDB).(eventmodels.CanAugment[eventmodels.BinaryEventID, TX]); ok {
		augmenter.AugmentWithProducer(producer)
	}
}

// The methods that Connection supports are all done with stubs so that the underlying implementations can be
// reused by other implementations of AbstractDB.

func (c Connection[TX, DB]) tracer() eventmodels.Tracer {
	if c.producer != nil {
		return c.producer.Tracer()
	}
	return nil
}

func (c *Connection[TX, DB]) Transact(ctx context.Context, f func(TX) error) error {
	return eventdb.Transact[eventmodels.BinaryEventID, TX, *Connection[TX, DB]](ctx, c, c.tracer(), f, SaveEventsInsideTx[TX], c.producer)
}

func (c *Connection[TX, DB]) LockOrError(ctx context.Context, key uint32, timeout time.Duration) (unlock func() error, err error) {
	return LockOrError[TX, *Connection[TX, DB]](ctx, c, key, timeout, c.lockPrefix)
}

func (c *Connection[TX, DB]) ProduceSpecificTxEvents(ctx context.Context, ids []eventmodels.BinaryEventID) (int, error) {
	return ProduceSpecificTxEvents(ctx, c, ids, c.producer)
}

func (c *Connection[TX, DB]) ProduceDroppedTxEvents(ctx context.Context, batchSize int) (int, error) {
	return ProduceDroppedTxEvents[TX, *Connection[TX, DB]](ctx, c, batchSize, c.tracer(), c.producer)
}

func (c Connection[TX, DB]) MarkEventProcessed(ctx context.Context, tx TX, topic string, source string, id string, handlerName string) error {
	return MarkEventProcessed[TX](ctx, tx, topic, source, id, handlerName)
}

func (c Connection[TX, DB]) SaveEventsInsideTx(ctx context.Context, tracer eventmodels.Tracer, tx TX, events ...eventmodels.ProducingEvent) ([]eventmodels.BinaryEventID, error) {
	return SaveEventsInsideTx[TX](ctx, tracer, tx, events...)
}

func LockOrError[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](
	ctx context.Context, db DB, key uint32, timeout time.Duration, lockPrefix string,
) (unlock func() error, err error) {
	done := make(chan struct{})
	lockString := fmt.Sprintf("%s-%d", lockPrefix, key)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
			close(done)
			//nolint:unparam
			unlock = func() error { return nil }
		} else {
			//nolint:unparam
			unlock = func() error {
				close(done)
				cancel()
				return nil
			}
		}
	}()
	if timeout == 0 {
		timeout = foreverLockAttemptTime
	}
	if timeout < minimumLockAttemptTime {
		timeout = minimumLockAttemptTime
	}
	ereturn := make(chan error)
	_, err = db.ExecContext(ctx, `
		INSERT IGNORE eventLocks (lockName, fluf)
		VALUES	(?, 0)`, lockString)
	if err != nil {
		return
	}
	go func() {
		_ = db.Transact(ctx, func(tx TX) error {
			var ignored string
			err = tx.QueryRowContext(ctx, `
				SELECT	lockName
				FROM	eventLocks
				WHERE	lockName = ?
				FOR UPDATE`, lockString).Scan(&ignored)

			if err != nil {
				ereturn <- err
				return err
			}
			ereturn <- nil
			<-done
			return sql.ErrNoRows // any error will trigger a rollback
		})
	}()
	timer := time.NewTimer(timeout)
	select {
	case err = <-ereturn:
		timer.Stop()
		if err != nil {
			err = errors.Wrapf(err, "obtain lock on %s", lockString)
			return
		}
		return
	case <-timer.C:
		cancel()
		err = eventmodels.TimeoutErr.Errorf("could not obtain lock on %s in %s", lockString, timeout)
		return
	}
}

func ProduceSpecificTxEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, ids []eventmodels.BinaryEventID, producer eventmodels.Producer[eventmodels.BinaryEventID, TX]) (int, error) {
	startTime := time.Now()
	count, err := produceEvents[TX, DB](ctx, db, eventmodels.ProduceInTransaction, 0, ids, producer)
	events.ObserveProduceLatency(err, startTime)
	if errors.Is(err, sql.ErrNoRows) {
		return count, nil
	}
	return count, err
}

func ProduceDroppedTxEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, batchSize int, tracer eventmodels.Tracer, producer eventmodels.Producer[eventmodels.BinaryEventID, TX]) (int, error) {
	if producer == nil {
		return 0, errors.Errorf("attempt to produce dropped events with a connection not augmented with a producer")
	}
	var totalCount int
	for {
		count, err := produceEvents(ctx, db, eventmodels.ProduceCatchUp, batchSize, nil, producer)
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
			tracer.Logf("[events] Catch-up background producer sent %d events", count)
			if count == 0 {
				return totalCount, nil
			}
			// Go around again and look for more right away. We'll stop when we get zero records.
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
func produceEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, method eventmodels.ProduceMethod, limit int, ids []eventmodels.BinaryEventID, producer eventmodels.Producer[eventmodels.BinaryEventID, TX]) (int, error) {
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
		Select("id", "topic", "ts", "sequenceNumber", "`key`", "data", "headers").
		From("eventsOutgoing").
		Suffix("FOR UPDATE").
		PlaceholderFormat(sq.Question)

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
	q, args, err := sb.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return 0, errors.Errorf("[Events] cannot build query to produce events: %w", err)
	}
	var count int
	err = db.Transact(ctx, func(tx TX) (finalErr error) {
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() {
			err := rows.Close()
			if finalErr == nil && err != nil {
				finalErr = errors.Errorf("cannot close delete events produce cursor: %w", err)
			}
		}()
		var toProduce []*eventdb.ProducingEventBlob[eventmodels.BinaryEventID]
		for rows.Next() {
			var blob eventdb.ProducingEventBlob[eventmodels.BinaryEventID]
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

		for _, event := range toProduce {
			results, err := tx.ExecContext(ctx, `
				DELETE FROM eventsOutgoing
				WHERE	id = ?`, event.ID)
			if err != nil {
				return errors.WithStack(err)
			}
			rowCount, err := results.RowsAffected()
			if err != nil {
				return errors.WithStack(err)
			}
			if rowCount != 1 {
				return errors.Errorf("attempt to delete event (%s) from outgoing table didn't work (%d)", event.ID, rowCount)
			}
		}

		count += len(toProduce)
		err = producer.Produce(ctx, method, generic.TransformSlice(toProduce,
			func(blob *eventdb.ProducingEventBlob[eventmodels.BinaryEventID]) eventmodels.ProducingEvent {
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
		INSERT IGNORE INTO eventsProcessed (topic, source, id, handlerName, processedAt)
		VALUES (?, ?, ?, ?, now())`,
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
func SaveEventsInsideTx[TX eventmodels.AbstractTX](ctx context.Context, tracer eventmodels.Tracer, tx TX, events ...eventmodels.ProducingEvent) ([]eventmodels.BinaryEventID, error) {
	if len(events) == 0 {
		return nil, nil
	}
	ids := make([]eventmodels.BinaryEventID, len(events))
	ib := sq.Insert("eventsOutgoing").
		Columns("id", "sequenceNumber", "topic", "ts", "`key`", "data", "headers").
		PlaceholderFormat(sq.Question)
	for i, event := range events {
		ids[i] = eventmodels.BinaryEventID{}.New()
		headers := event.GetHeaders()
		enc, err := json.Marshal(event)
		if err != nil {
			return nil, errors.Errorf("could not encode event to produce in topic (%s): %w", event.GetTopic(), err)
		}
		headersEnc, err := json.Marshal(headers)
		if err != nil {
			return nil, errors.Errorf("could not encode event headers to produce in topic (%s): %w", event.GetTopic(), err)
		}
		// ib = ib.Values(ids[i], i+1, event.GetTopic(), event.GetTimestamp().UTC(), event.GetKey(), enc, headersEnc)
		ib = ib.Values(ids[i], i+1, event.GetTopic(), event.GetTimestamp().UTC().Format("2006-01-02 15:04:05.000000"), event.GetKey(), enc, headersEnc)
	}
	if tracer != nil {
		tracer.Logf("[events] saving %d events as part of a transaction, example topic: '%s'", len(events), events[0].GetTopic())
	}
	sql, args, err := ib.PlaceholderFormat(sq.Question).ToSql()
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
