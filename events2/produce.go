// Package events2 implements the abstract types using SingleStore
package events2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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

	defaultCatchUpBuckets = 8
	// sendAfterGrace is how long the post-commit send owns a row. Catch-up
	// selects a row only after this has passed.
	//
	// fastPathSendTimeout bounds one post-commit attempt. The library can hold
	// one batch behind an in-flight send, so a row's locks last at most two
	// attempts. Two attempts fit inside sendAfterGrace.
	sendAfterGrace      = 2 * time.Minute
	fastPathSendTimeout = 30 * time.Second
)

var debugging = os.Getenv("EVENTS_DEBUG_PRODUCE") == "true"

type Connection[TX eventdb.BasicTX, DB eventdb.BasicDB[TX]] struct {
	eventdb.BasicDB[TX]
	lockPrefix     string
	producer       eventmodels.Producer[eventmodels.BinaryEventID, TX]
	catchUpBuckets *atomic.Int32
	catchUpStride  *catchUpStrideCache
}

// catchUpStrideCache keeps one prime stride and one start offset per bucket
// count. A pod walks buckets in the same order until the count changes.
// Pods with different offsets and strides cross instead of all locking
// bucket 0 on step 0.
type catchUpStrideCache struct {
	mu     sync.Mutex
	count  int
	stride int
	offset int
}

func (c *catchUpStrideCache) strideFor(count int) (stride, offset int) {
	if count <= 1 {
		return 1, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.count != count || c.stride == 0 {
		c.count = count
		c.stride = catchUpStride(count)
		c.offset = catchUpOffset(count)
	}
	return c.stride, c.offset
}

func New[TX eventdb.BasicTX, DB eventdb.BasicDB[TX]](db DB, lockPrefix string) *Connection[TX, DB] {
	buckets := &atomic.Int32{}
	buckets.Store(defaultCatchUpBuckets)
	return &Connection[TX, DB]{
		BasicDB:        db,
		lockPrefix:     lockPrefix,
		catchUpBuckets: buckets,
		catchUpStride:  &catchUpStrideCache{},
	}
}

// SetCatchUpBuckets sets how many id ranges the background catch-up splits
// eventsOutgoing into. The minimum is 1, which turns bucketing off and scans
// the whole id space in one pass. The default is 8. A change takes effect on
// the next catch-up pass.
func (c *Connection[TX, DB]) SetCatchUpBuckets(n int) {
	if c == nil || c.catchUpBuckets == nil {
		panic("events2: SetCatchUpBuckets on uninitialized connection")
	}
	if n < 1 {
		panic("events2: catch-up bucket count must be at least 1")
	}
	c.catchUpBuckets.Store(int32(n))
}

func (c Connection[TX, DB]) catchUpBucketCount() int {
	if c.catchUpBuckets == nil {
		return defaultCatchUpBuckets
	}
	n := int(c.catchUpBuckets.Load())
	if n < 1 {
		return 1
	}
	return n
}

func (c Connection[TX, DB]) catchUpStrideFor(count int) (int, int) {
	if count <= 1 {
		return 1, 0
	}
	if c.catchUpStride == nil {
		return catchUpStride(count), catchUpOffset(count)
	}
	return c.catchUpStride.strideFor(count)
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
				sendAfter	datetime(6)			NOT NULL DEFAULT NOW(6),
				PRIMARY KEY (id),
				SORT KEY (ts, sequenceNumber, id),
				SHARD KEY (id)
			)
			COMMENT 'ephemeral data, only existing until events are sent to Kafka'`),

		lssinglestore.Script("add-eventsOutgoing-sendAfter", `
			ALTER TABLE eventsOutgoing
			ADD COLUMN sendAfter DATETIME(6) NOT NULL DEFAULT NOW(6)`,
			libschema.SkipIf(func() (bool, error) {
				return singlestore.DoesColumnExist("eventsOutgoing", "sendAfter")
			}),
		),

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

// backupTracer only needs to create a tracer if we don't have a producer.
func (c Connection[TX, DB]) backupTracer() eventmodels.Tracer {
	if c.producer == nil {
		return log.Printf
	}
	return nil
}

func (c *Connection[TX, DB]) Transact(ctx context.Context, f func(TX) error) error {
	return eventdb.Transact[eventmodels.BinaryEventID, TX, *Connection[TX, DB]](ctx, c, c.backupTracer(), f, SaveEventsInsideTx[TX], c.producer)
}

func (c *Connection[TX, DB]) LockOrError(ctx context.Context, key uint32, timeout time.Duration) (unlock func() error, err error) {
	return LockOrError[TX, *Connection[TX, DB]](ctx, c, key, timeout, c.lockPrefix)
}

func (c *Connection[TX, DB]) ProduceSpecificTxEvents(ctx context.Context, ids []eventmodels.BinaryEventID) (int, error) {
	return ProduceSpecificTxEvents(ctx, c, ids, c.producer)
}

func (c *Connection[TX, DB]) ProduceDroppedTxEvents(ctx context.Context, batchSize int) (int, error) {
	return ProduceDroppedTxEvents[TX, *Connection[TX, DB]](ctx, c, batchSize, c.producer)
}

func (c Connection[TX, DB]) MarkEventProcessed(ctx context.Context, tx TX, topic string, source string, id string, handlerName string) error {
	return MarkEventProcessed[TX](ctx, tx, topic, source, id, handlerName)
}

func (c Connection[TX, DB]) SaveEventsInsideTx(ctx context.Context, tx TX, events ...eventmodels.ProducingEvent) (map[string][]eventmodels.BinaryEventID, error) {
	return SaveEventsInsideTx[TX](ctx, c.backupTracer(), tx, c.producer, events...)
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
	// One attempt. A batch can wait behind one in-flight send before this starts,
	// so a row is locked for at most two of these, which stays inside
	// sendAfterGrace. A shorter caller deadline still wins.
	ctx, cancel := context.WithTimeout(ctx, fastPathSendTimeout)
	defer cancel()
	startTime := time.Now()
	count, err := produceEvents[TX, DB](ctx, db, eventmodels.ProduceInTransaction, ids, producer)
	events.ObserveProduceLatency(err, startTime)
	if errors.Is(err, sql.ErrNoRows) {
		return count, nil
	}
	return count, err
}

func ProduceDroppedTxEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, batchSize int, producer eventmodels.Producer[eventmodels.BinaryEventID, TX]) (int, error) {
	if producer == nil {
		return 0, errors.Errorf("attempt to produce dropped events with a connection not augmented with a producer")
	}
	if batchSize < 1 {
		return 0, errors.Errorf("catch-up batch size must be at least 1")
	}
	bucketCount, stride, offset := catchUpPlanFor(db)
	var totalCount int
	for step := 0; step < bucketCount; step++ {
		if ctx.Err() != nil {
			return totalCount, nil
		}
		bucket := bucketForStep(step, stride, offset, bucketCount)
		for {
			ids, err := selectCatchUpIDs(ctx, db, batchSize, bucket, bucketCount)
			if err != nil {
				if ctx.Err() != nil {
					return totalCount, nil
				}
				_ = producer.RecordError(ctx, "produceStoredEvents", err)
				return totalCount, err
			}
			if len(ids) == 0 {
				break
			}
			count, err := produceEvents(ctx, db, eventmodels.ProduceCatchUp, ids, producer)
			totalCount += count
			if ctx.Err() != nil {
				return totalCount, nil
			}
			switch {
			case errors.Is(err, sql.ErrNoRows):
				// The pre-selected rows were taken by another producer.
			case err != nil:
				_ = producer.RecordError(ctx, "produceStoredEvents", err)
				return totalCount, err
			default:
				producer.TracerProvider(ctx)("[events] Catch-up background producer sent %d events", count)
			}
			if len(ids) < batchSize {
				break
			}
		}
	}
	if totalCount == 0 && debugging {
		producer.TracerProvider(ctx)("[events] Catch-up background producer found no pending events")
	}
	return totalCount, nil
}

// produceEvents locks the given outgoing rows, deletes them, and sends them to
// the message queue. Locks are taken by primary key. If none of the ids are
// still pending, sql.ErrNoRows is returned.
//
// While catch-up is running, a post-commit send leaves rows whose sendAfter has
// passed. Those belong to catch-up. With no catch-up running, every requested
// id is sent so nothing is stranded.
func produceEvents[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, method eventmodels.ProduceMethod, ids []eventmodels.BinaryEventID, producer eventmodels.Producer[eventmodels.BinaryEventID, TX]) (int, error) {
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
	if len(ids) == 0 {
		return 0, sql.ErrNoRows
	}

	sb := sq.
		Select("id", "topic", "ts", "sequenceNumber", "`key`", "data", "headers").
		From("eventsOutgoing").
		Where(sq.Eq{"id": ids}).
		Suffix("FOR UPDATE").
		PlaceholderFormat(sq.Question)
	if method == eventmodels.ProduceInTransaction && producer.CatchUpProduceRunning() {
		sb = sb.Where("sendAfter > NOW(6)")
	}
	q, args, err := sb.ToSql()
	if err != nil {
		return 0, errors.Errorf("[Events] cannot build query to produce events: %w", err)
	}
	order := make(map[string]int, len(ids))
	for i, id := range ids {
		order[id.String()] = i
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
		if err := rows.Err(); err != nil {
			return errors.Errorf("cannot read list of events to produce: %w", err)
		}
		if len(toProduce) == 0 {
			return sql.ErrNoRows
		}
		sort.SliceStable(toProduce, func(i, j int) bool {
			return order[toProduce[i].ID.String()] < order[toProduce[j].ID.String()]
		})

		deleted, delArgs, err := sq.Delete("eventsOutgoing").
			Where(sq.Eq{"id": eventIDs(toProduce)}).
			PlaceholderFormat(sq.Question).
			ToSql()
		if err != nil {
			return errors.Errorf("cannot build delete of produced events: %w", err)
		}
		results, err := tx.ExecContext(ctx, deleted, delArgs...)
		if err != nil {
			return errors.WithStack(err)
		}
		rowCount, err := results.RowsAffected()
		if err != nil {
			return errors.WithStack(err)
		}
		if rowCount != int64(len(toProduce)) {
			return errors.Errorf("attempt to delete %d events from outgoing table deleted %d", len(toProduce), rowCount)
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

func sendAfterExpr(producerIsSet bool) sq.Sqlizer {
	if !producerIsSet {
		return sq.Expr("NOW(6)")
	}
	return sq.Expr(fmt.Sprintf("NOW(6) + INTERVAL %d MICROSECOND", sendAfterGrace.Microseconds()))
}

func eventIDs(events []*eventdb.ProducingEventBlob[eventmodels.BinaryEventID]) []eventmodels.BinaryEventID {
	ids := make([]eventmodels.BinaryEventID, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	return ids
}

// selectCatchUpIDs reads ids of rows catch-up is allowed to send, without
// locking them. The caller then locks those ids by primary key. A bucket count
// of 1 covers the whole id space.
func selectCatchUpIDs[TX eventmodels.AbstractTX, DB eventmodels.CanTransact[TX]](ctx context.Context, db DB, limit, bucket, bucketCount int) (ids []eventmodels.BinaryEventID, err error) {
	sb := sq.Select("id").
		From("eventsOutgoing").
		Where("sendAfter <= NOW(6)").
		OrderBy("ts ASC", "sequenceNumber ASC").
		Limit(uint64(limit)).
		PlaceholderFormat(sq.Question)
	sb = applyBucketBounds(sb, bucket, bucketCount)
	q, args, err := sb.ToSql()
	if err != nil {
		return nil, errors.Errorf("[Events] cannot build catch-up id query: %w", err)
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errors.Errorf("cannot list events for catch-up: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if err == nil && closeErr != nil {
			err = errors.Errorf("cannot close catch-up id cursor: %w", closeErr)
		}
	}()
	for rows.Next() {
		var id eventmodels.BinaryEventID
		if err = rows.Scan(&id); err != nil {
			return nil, errors.Errorf("cannot scan catch-up event id: %w", err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.Errorf("cannot read catch-up event ids: %w", err)
	}
	return ids, nil
}

func applyBucketBounds(sb sq.SelectBuilder, bucket, count int) sq.SelectBuilder {
	if count <= 1 {
		return sb
	}
	low, high, hasHigh := bucketBounds(bucket, count)
	sb = sb.Where(sq.GtOrEq{"id": low})
	if hasHigh {
		sb = sb.Where(sq.Lt{"id": high})
	}
	return sb
}

// bucketBounds splits the 16-byte id space into count contiguous ranges.
// Ranges tile every possible id, so shrinking the count leaves nothing behind.
// The last bucket has no upper bound.
func bucketBounds(bucket, count int) (low, high []byte, hasHigh bool) {
	span := new(big.Int).Lsh(big.NewInt(1), 128)
	n := big.NewInt(int64(count))
	low = idBound(new(big.Int).Div(new(big.Int).Mul(big.NewInt(int64(bucket)), span), n))
	if bucket >= count-1 {
		return low, nil, false
	}
	high = idBound(new(big.Int).Div(new(big.Int).Mul(big.NewInt(int64(bucket+1)), span), n))
	return low, high, true
}

func idBound(i *big.Int) []byte {
	raw := i.Bytes()
	if len(raw) > 16 {
		raw = raw[len(raw)-16:]
	}
	out := make([]byte, 16)
	copy(out[16-len(raw):], raw)
	return out
}

// bucketForStep walks one catch-up pass. offset is a per-pod start, so step 0
// is not bucket 0 on every pod. stride then visits each bucket once.
func bucketForStep(step, stride, offset, count int) int {
	if count <= 1 {
		return 0
	}
	return int((int64(offset) + int64(step)*int64(stride)) % int64(count))
}

type catchUpPlanner interface {
	catchUpBucketCount() int
	catchUpStrideFor(int) (stride, offset int)
}

func catchUpPlanFor(db any) (count, stride, offset int) {
	if plan, ok := db.(catchUpPlanner); ok {
		count = plan.catchUpBucketCount()
		if count < 1 {
			count = 1
		}
		stride, offset = plan.catchUpStrideFor(count)
		return count, stride, offset
	}
	return defaultCatchUpBuckets, catchUpStride(defaultCatchUpBuckets), catchUpOffset(defaultCatchUpBuckets)
}

// catchUpOffset is the bucket a pod locks first. It stays in [0, count).
// Without it, (step * stride) % count is 0 for every stride when step is 0.
func catchUpOffset(count int) int {
	if count <= 1 {
		return 0
	}
	return rand.Intn(count)
}

// catchUpStride picks a prime below 1000 that does not divide count. Stepping
// by that prime visits every bucket once per pass. The start bucket is
// catchUpOffset; this prime only changes the order after that.
func catchUpStride(count int) int {
	if count <= 1 {
		return 1
	}
	candidates := primesBelow1000()
	usable := make([]int, 0, len(candidates))
	for _, prime := range candidates {
		if count%prime != 0 {
			usable = append(usable, prime)
		}
	}
	if len(usable) == 0 {
		return 1
	}
	return usable[rand.Intn(len(usable))]
}

var primesBelow1000 = sync.OnceValue(func() []int {
	const max = 1000
	composite := make([]bool, max)
	primes := make([]int, 0, 168)
	for i := 2; i < max; i++ {
		if composite[i] {
			continue
		}
		primes = append(primes, i)
		for j := i * i; j < max; j += i {
			composite[j] = true
		}
	}
	return primes
})

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
// events as part of that transaction. backupTracer is unused if producer is provided.
// topics are unvalidated if a producer is not provided.
func SaveEventsInsideTx[TX eventmodels.AbstractTX](ctx context.Context, backupTracer eventmodels.Tracer, tx TX, optProducer eventmodels.Producer[eventmodels.BinaryEventID, TX], events ...eventmodels.ProducingEvent) (map[string][]eventmodels.BinaryEventID, error) {
	if len(events) == 0 {
		return nil, nil
	}
	err := eventdb.ValidateEventTopics(ctx, optProducer, events...)
	if err != nil {
		return nil, err
	}
	ids := eventdb.PreAllocateIDMap[eventmodels.BinaryEventID](events...)
	ib := sq.Insert("eventsOutgoing").
		Columns("id", "sequenceNumber", "topic", "ts", "`key`", "data", "headers", "sendAfter").
		PlaceholderFormat(sq.Question)
	// A producer will try to send the row itself, so catch-up waits out
	// sendAfterGrace. With no producer, only catch-up can send it.
	sendAfter := sendAfterExpr(optProducer != nil)

	for i, event := range events {
		topic := event.GetTopic()
		id := eventmodels.BinaryEventID{}.New()
		ids[topic] = append(ids[topic], id)
		headers := event.GetHeaders()
		enc, err := json.Marshal(event)
		if err != nil {
			return nil, errors.Errorf("could not encode event to produce in topic (%s): %w", event.GetTopic(), err)
		}
		headersEnc, err := json.Marshal(headers)
		if err != nil {
			return nil, errors.Errorf("could not encode event headers to produce in topic (%s): %w", event.GetTopic(), err)
		}
		ib = ib.Values(id, i+1, event.GetTopic(), event.GetTimestamp().UTC().Format("2006-01-02 15:04:05.000000"), event.GetKey(), enc, headersEnc, sendAfter)
	}
	tracer := backupTracer
	if optProducer != nil {
		tracer = optProducer.TracerProvider(ctx)
	}
	tracer("[events] saving %d events as part of a transaction, example topic: '%s'", len(events), events[0].GetTopic())
	sql, args, err := ib.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		tracer("[SaveEventsInsideTx] could not insert event: %s: %s", sql, err)
		return nil, errors.WithStack(err)
	}
	return ids, nil
}
