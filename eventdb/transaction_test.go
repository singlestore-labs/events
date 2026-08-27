package eventdb

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
)

type testProducingEvent struct{}

func (testProducingEvent) GetKey() string                  { return "key" }
func (testProducingEvent) GetTimestamp() time.Time         { return time.Time{} }
func (testProducingEvent) GetTopic() string                { return "topic" }
func (testProducingEvent) GetHeaders() map[string][]string { return nil }

type testTX struct {
	committed  bool
	rolledBack bool
}

func (*testTX) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (*testTX) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (*testTX) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }
func (tx *testTX) Commit() error {
	tx.committed = true
	return nil
}
func (tx *testTX) Rollback() error {
	tx.rolledBack = true
	return nil
}
func (*testTX) GetPendingEvents() []eventmodels.ProducingEvent {
	return []eventmodels.ProducingEvent{testProducingEvent{}}
}

type testDB struct {
	tx *testTX
}

func (db *testDB) BeginTx(context.Context, *sql.TxOptions) (*testTX, error) { return db.tx, nil }
func (*testDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (*testDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (*testDB) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }
func (db *testDB) Transact(_ context.Context, f func(*testTX) error) error {
	return f(db.tx)
}
func (*testDB) ProduceSpecificTxEvents(context.Context, []eventmodels.StringEventID) (int, error) {
	return 0, nil
}
func (*testDB) ProduceDroppedTxEvents(context.Context, int) (int, error) { return 0, nil }
func (*testDB) LockOrError(context.Context, uint32, time.Duration) (func() error, error) {
	return func() error { return nil }, nil
}
func (*testDB) MarkEventProcessed(context.Context, *testTX, string, string, string, string) error {
	return nil
}

type failingPostCommitProducer struct {
	produceFromTableCalled bool
	recordErrorCalled      bool
}

func (*failingPostCommitProducer) DB() eventmodels.AbstractDB[eventmodels.StringEventID, *testTX] {
	return nil
}
func (*failingPostCommitProducer) Produce(context.Context, eventmodels.ProduceMethod, ...eventmodels.ProducingEvent) error {
	return nil
}
func (p *failingPostCommitProducer) ProduceFromTable(context.Context, map[string][]eventmodels.StringEventID) error {
	p.produceFromTableCalled = true
	return assert.AnError
}
func (p *failingPostCommitProducer) RecordError(context.Context, string, error) error { return nil }
func (p *failingPostCommitProducer) RecordErrorNoWait(context.Context, string, error) error {
	p.recordErrorCalled = true
	return nil
}
func (*failingPostCommitProducer) IsConfigured() bool                             { return true }
func (*failingPostCommitProducer) ValidateTopics(context.Context, []string) error { return nil }
func (*failingPostCommitProducer) TracerProvider(context.Context) eventmodels.Tracer {
	return func(string, ...any) {}
}

func TestTransactDoesNotFailCommittedTransactionWhenPostCommitProduceFails(t *testing.T) {
	tx := &testTX{}
	db := &testDB{tx: tx}
	producer := &failingPostCommitProducer{}
	save := func(
		context.Context,
		eventmodels.Tracer,
		*testTX,
		eventmodels.Producer[eventmodels.StringEventID, *testTX],
		...eventmodels.ProducingEvent,
	) (map[string][]eventmodels.StringEventID, error) {
		return map[string][]eventmodels.StringEventID{
			"topic": []eventmodels.StringEventID{{}},
		}, nil
	}

	err := Transact(context.Background(), db, nil, func(*testTX) error {
		return nil
	}, save, producer)

	require.NoError(t, err)
	require.True(t, tx.committed)
	require.False(t, tx.rolledBack)
	require.True(t, producer.produceFromTableCalled)
	require.True(t, producer.recordErrorCalled)
}
