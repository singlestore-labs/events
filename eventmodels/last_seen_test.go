package eventmodels

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type payload struct {
	N int `json:"n"`
}

type hwInfo struct {
	dead bool
}

func (hwInfo) Name() string          { return "handler-b" }
func (hwInfo) BaseTopic() string     { return "orders" }
func (hwInfo) ConsumerGroup() string { return "cg" }
func (h hwInfo) IsDeadLetter() bool  { return h.dead }

type hwDB struct {
	mu        sync.Mutex
	now       time.Time
	rows      map[string]time.Time
	touch     []time.Time
	failTouch bool
}

func newHWDB(now time.Time) *hwDB {
	return &hwDB{now: now, rows: map[string]time.Time{}}
}

type hwTx struct {
	db      *hwDB
	pending map[string]time.Time
}

func hwKey(topic, source, id, handler string) string {
	return topic + "\x00" + source + "\x00" + id + "\x00" + handler
}

func (db *hwDB) Transact(_ context.Context, fn func(hwTx) error) error {
	tx := hwTx{db: db, pending: map[string]time.Time{}}
	if err := fn(tx); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for key, seen := range tx.pending {
		db.rows[key] = seen
	}
	return nil
}

func (db *hwDB) MarkEventProcessed(_ context.Context, tx hwTx, topic, source, id, handler string) error {
	key := hwKey(topic, source, id, handler)
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.rows[key]; ok {
		return ErrAlreadyProcessed.Errorf("already")
	}
	if _, ok := tx.pending[key]; ok {
		return ErrAlreadyProcessed.Errorf("already")
	}
	tx.pending[key] = db.now
	return nil
}

func (db *hwDB) TouchEventProcessed(_ context.Context, topic, source, id, handler string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.touch = append(db.touch, db.now)
	if db.failTouch {
		return sql.ErrConnDone
	}
	key := hwKey(topic, source, id, handler)
	current, ok := db.rows[key]
	if !ok || !db.now.After(current) {
		return nil
	}
	db.rows[key] = db.now
	return nil
}

func (db *hwDB) seen(topic, source, id, handler string) (time.Time, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	seen, ok := db.rows[hwKey(topic, source, id, handler)]
	return seen, ok
}

func (hwTx) ExecContext(context.Context, string, ...any) (sql.Result, error) { return nil, nil }
func (hwTx) QueryContext(context.Context, string, ...any) (*sql.Rows, error) { return nil, nil }
func (hwTx) QueryRowContext(context.Context, string, ...any) *sql.Row        { return &sql.Row{} }

func (db *hwDB) LockOrError(context.Context, uint32, time.Duration) (func() error, error) {
	return func() error { return nil }, nil
}
func (db *hwDB) ProduceSpecificTxEvents(context.Context, []StringEventID) (int, error) {
	return 0, nil
}
func (db *hwDB) ProduceDroppedTxEvents(context.Context, int) (int, error) { return 0, nil }
func (db *hwDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (db *hwDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (db *hwDB) QueryRowContext(context.Context, string, ...any) *sql.Row { return &sql.Row{} }

type hwLib struct{ db *hwDB }

func (l hwLib) DB() AbstractDB[StringEventID, hwTx] { return l.db }
func (hwLib) RemovePrefix(topic string) string      { return topic }
func (hwLib) TracerProvider(context.Context) Tracer {
	return func(string, ...any) {}
}
func (hwLib) TracerConfig() TracerConfig {
	return TracerConfig{Handle: func(ctx context.Context, _ bool, _ string, _ *kafka.Message) (context.Context, func()) {
		return ctx, func() {}
	}}
}

func testMessage(id string, at time.Time) *kafka.Message {
	return &kafka.Message{
		Topic: "orders",
		Key:   []byte("k"),
		Time:  at,
		Value: []byte(`{"n":1}`),
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "ce_source", Value: []byte("src")},
			{Key: "ce_id", Value: []byte(id)},
		},
	}
}

func TestFirstDeliveryStoresDatabaseTime(t *testing.T) {
	dbNow := time.Now().UTC()
	db := newHWDB(dbNow)
	errs := handleTx(context.Background(), hwInfo{}, []*kafka.Message{testMessage("id-1", dbNow.Add(time.Hour))}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		return nil
	})
	require.Empty(t, firstErr(errs))
	seen, ok := db.seen("orders", "src", "id-1", "handler-b")
	require.True(t, ok)
	require.True(t, seen.Equal(dbNow))
}

func TestInsertIsNotEarlierThanDatabaseTime(t *testing.T) {
	dbNow := time.Now().UTC()
	appendTime := dbNow.Add(-time.Hour)
	db := newHWDB(dbNow)
	errs := handleTx(context.Background(), hwInfo{}, []*kafka.Message{testMessage("id-1", appendTime)}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		return nil
	})
	require.Empty(t, firstErr(errs))
	seen, ok := db.seen("orders", "src", "id-1", "handler-b")
	require.True(t, ok)
	require.True(t, seen.Equal(dbNow))
}

func TestTouchMissingRowSkips(t *testing.T) {
	db := newHWDB(time.Now().UTC())
	err := db.TouchEventProcessed(context.Background(), "orders", "src", "missing", "handler-b")
	require.NoError(t, err)
	_, ok := db.seen("orders", "src", "missing", "handler-b")
	require.False(t, ok)
}

func TestNewerDuplicateAdvancesLastSeenAt(t *testing.T) {
	older := time.Now().Add(-time.Hour).UTC()
	dbNow := time.Now().UTC()
	db := newHWDB(dbNow)
	db.rows[hwKey("orders", "src", "id-1", "handler-b")] = older
	errs := handleTx(context.Background(), hwInfo{}, []*kafka.Message{testMessage("id-1", older)}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		t.Fatal("duplicate must not call the handler")
		return nil
	})
	require.Empty(t, firstErr(errs))
	seen, _ := db.seen("orders", "src", "id-1", "handler-b")
	require.True(t, seen.Equal(dbNow))
}

func TestOlderDuplicateDoesNotLowerLastSeenAt(t *testing.T) {
	newer := time.Now().UTC()
	older := newer.Add(-time.Hour)
	db := newHWDB(older)
	db.rows[hwKey("orders", "src", "id-1", "handler-b")] = newer
	errs := handleTx(context.Background(), hwInfo{}, []*kafka.Message{testMessage("id-1", older)}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		return nil
	})
	require.Empty(t, firstErr(errs))
	seen, _ := db.seen("orders", "src", "id-1", "handler-b")
	require.True(t, seen.Equal(newer))
}

func TestDuplicateTouchSurvivesBatchRollback(t *testing.T) {
	older := time.Now().Add(-time.Hour).UTC()
	dbNow := time.Now().UTC()
	db := newHWDB(dbNow)
	db.rows[hwKey("orders", "src", "old", "handler-b")] = older
	errs := handleTx(context.Background(), hwInfo{}, []*kafka.Message{
		testMessage("old", older),
		testMessage("new", older),
	}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		return sql.ErrTxDone
	})
	require.Error(t, errs[1])
	require.NoError(t, errs[0])
	_, newExists := db.seen("orders", "src", "new", "handler-b")
	require.False(t, newExists)
	seen, _ := db.seen("orders", "src", "old", "handler-b")
	require.True(t, seen.Equal(dbNow))
}

func TestDeadLetterConsumptionAdvancesToDatabaseTime(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour).UTC()
	dbNow := time.Now().UTC()
	db := newHWDB(dbNow)
	db.rows[hwKey("orders", "src", "id-1", "handler-b")] = older
	errs := handleTx(context.Background(), hwInfo{dead: true}, []*kafka.Message{testMessage("id-1", older)}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		return nil
	})
	require.Empty(t, firstErr(errs))
	seen, _ := db.seen("orders", "src", "id-1", "handler-b")
	require.True(t, seen.Equal(dbNow))
}

func TestRowCommittedAfterDeadLetterUsesDatabaseTime(t *testing.T) {
	dbNow := time.Now().UTC()
	db := newHWDB(dbNow)
	errs := handleTx(context.Background(), hwInfo{}, []*kafka.Message{testMessage("id-1", dbNow.Add(-time.Hour))}, hwLib{db}, func(context.Context, hwTx, []Event[payload]) error {
		return nil
	})
	require.Empty(t, firstErr(errs))
	seen, _ := db.seen("orders", "src", "id-1", "handler-b")
	require.True(t, seen.Equal(dbNow))
}

func firstErr(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
