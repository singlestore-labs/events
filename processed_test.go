package events

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
)

type touchDB struct {
	mu         sync.Mutex
	rows       map[string]time.Time
	touchCalls int
	touchErr   func(call int) error
}

func newTouchDB() *touchDB {
	return &touchDB{rows: map[string]time.Time{}}
}

type touchTx struct{}

func (touchTx) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (touchTx) QueryContext(context.Context, string, ...any) (*sql.Rows, error) { return nil, nil }
func (touchTx) QueryRowContext(context.Context, string, ...any) *sql.Row        { return &sql.Row{} }

func touchKey(topic, source, id, handler string) string {
	return topic + "\x00" + source + "\x00" + id + "\x00" + handler
}

func (db *touchDB) insert(topic, source, id, handler string, at time.Time) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rows[touchKey(topic, source, id, handler)] = at
}

func (db *touchDB) seen(topic, source, id, handler string) (time.Time, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	at, ok := db.rows[touchKey(topic, source, id, handler)]
	return at, ok
}

func (db *touchDB) TouchEventProcessed(_ context.Context, topic, source, id, handler string, timestamp time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.touchCalls++
	if db.touchErr != nil {
		if err := db.touchErr(db.touchCalls); err != nil {
			return err
		}
	}
	key := touchKey(topic, source, id, handler)
	current, ok := db.rows[key]
	if !ok || !timestamp.After(current) {
		return nil
	}
	db.rows[key] = timestamp
	return nil
}

func (db *touchDB) Transact(context.Context, func(touchTx) error) error { return nil }
func (db *touchDB) MarkEventProcessed(context.Context, touchTx, string, string, string, string) error {
	return nil
}
func (db *touchDB) LockOrError(context.Context, uint32, time.Duration) (func() error, error) {
	return func() error { return nil }, nil
}
func (db *touchDB) ProduceSpecificTxEvents(context.Context, []eventmodels.BinaryEventID) (int, error) {
	return 0, nil
}
func (db *touchDB) ProduceDroppedTxEvents(context.Context, int) (int, error) { return 0, nil }
func (db *touchDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (db *touchDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (db *touchDB) QueryRowContext(context.Context, string, ...any) *sql.Row { return &sql.Row{} }

func testDeadLetterLib(t *testing.T, db *touchDB) (*Library[eventmodels.BinaryEventID, touchTx, *touchDB], consumerGroupName) {
	t.Helper()
	lib := New[eventmodels.BinaryEventID, touchTx, *touchDB]()
	lib.SkipNotifierSupport()
	lib.Configure(db, func(context.Context) eventmodels.Tracer {
		return func(string, ...any) {}
	}, false, nil, nil, []string{"127.0.0.1:9092"})
	group := NewConsumerGroup("state-server")
	topic := eventmodels.BindTopicTx[struct{}, eventmodels.BinaryEventID, touchTx, *touchDB]("orders")
	lib.ConsumeExactlyOnce(group, eventmodels.OnFailureSave, "B", topic.HandlerTx(func(context.Context, touchTx, eventmodels.Event[struct{}]) error {
		return nil
	}), WithRetrying(false))
	lib.readers[group.name()].topics["orders"].handlers["B"].consumerGroup = group.name()
	return lib, group.name()
}

func TestDeadLetterPreTouchAdvancesExistingRow(t *testing.T) {
	db := newTouchDB()
	lib, group := testDeadLetterLib(t, db)
	start := time.Now()
	old := start.Add(-time.Hour)
	lib.deadLetterHook = func(phase string) {
		if phase == "before-pre-touch" {
			db.insert("orders", "src", "id-1", "B", old)
		}
	}
	err := lib.writeDeadLetterCopy(context.Background(), "orders", group, "src", "id-1", true, func(context.Context) error {
		return nil
	})
	require.NoError(t, err)
	seen, ok := db.seen("orders", "src", "id-1", "B")
	require.True(t, ok)
	require.False(t, seen.Before(start))
}

func TestDeadLetterPostTouchAdvancesRowCommittedDuringWrite(t *testing.T) {
	db := newTouchDB()
	lib, group := testDeadLetterLib(t, db)
	start := time.Now()
	old := start.Add(-time.Hour)
	lib.deadLetterHook = func(phase string) {
		if phase == "after-pre-touch" {
			db.insert("orders", "src", "id-1", "B", old)
		}
	}
	err := lib.writeDeadLetterCopy(context.Background(), "orders", group, "src", "id-1", true, func(context.Context) error {
		return nil
	})
	require.NoError(t, err)
	seen, ok := db.seen("orders", "src", "id-1", "B")
	require.True(t, ok)
	require.False(t, seen.Before(start))
}

func TestPreTouchFailureDoesNotWriteDeadLetter(t *testing.T) {
	db := newTouchDB()
	lib, group := testDeadLetterLib(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.touchErr = func(call int) error {
		if call == 1 {
			cancel()
			return sql.ErrConnDone
		}
		return nil
	}
	writes := 0
	err := lib.writeDeadLetterCopy(ctx, "orders", group, "src", "id-1", true, func(context.Context) error {
		writes++
		return nil
	})
	require.Error(t, err)
	require.Equal(t, 0, writes)
}

func TestPostTouchFailurePreventsAcknowledgement(t *testing.T) {
	db := newTouchDB()
	lib, group := testDeadLetterLib(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.touchErr = func(call int) error {
		if call >= 2 {
			cancel()
			return sql.ErrConnDone
		}
		return nil
	}
	writes := 0
	err := lib.writeDeadLetterCopy(ctx, "orders", group, "src", "id-1", true, func(context.Context) error {
		writes++
		return nil
	})
	require.Error(t, err)
	require.Equal(t, 1, writes)
}

func TestUnstableIdentitySkipsDeadLetterTouch(t *testing.T) {
	db := newTouchDB()
	lib, group := testDeadLetterLib(t, db)
	err := lib.writeDeadLetterCopy(context.Background(), "orders", group, "", "", false, func(context.Context) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, db.touchCalls)
}

func TestStableIdentityFromCloudEventBody(t *testing.T) {
	source, id, ok := stableEventIdentity(kafka.Message{
		Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/cloudevents+json")}},
		Value:   []byte(`{"source":"svc","id":"abc"}`),
	})
	require.True(t, ok)
	require.Equal(t, "svc", source)
	require.Equal(t, "abc", id)
}
