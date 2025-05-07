// Package eventmodels has the interfaces needed to use the events package
package eventmodels

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/memsql/errors"
)

// The DB layers are complex. The challenge is that there are low-level things
// that need to be available to mid-layer things.
//
// Direct DB insert etc is low-level.
// Wrapping transactions is mid-level.
//
// So when implmenting a full solution, it's a bit upside-down since the type
// of transactions is mid-level and needed by low-level.
//
// Reading and writing events to the database is defined as low-level.
// A different kind of low-level code defines a transaction wrapper.
// The transaction wrapper is low-level because it uses BeginTx.
//
// eventdb.WrappedTX provides an example implementation of a transaction that
// implements the recommended EnhancedTX interface for sending events inside
// transactions.
//
// eventdb.WrappedDB pairs with eventdb.WrappedTX to provide the a DB interface
// that supports Transact.
//
// It is suggested, but not required, that the general TX used by other parts of
// the system (ie: not the event framework) support EnhancedTX so that it is easy
// to add events to transactions.

type AbstractID[ID AbstractIDMethods] interface {
	AbstractIDMethods
	New() ID
}

type AbstractIDMethods interface {
	Value() (driver.Value, error)
	String() string
	// Scan(src any) error - hidden pointer method
}

// Producer is implemented by events.Library. It's an interface so that
// import cycles can be avoided.
type Producer[ID AbstractID[ID], TX AbstractTX] interface {
	CanValidateTopics
	DB() AbstractDB[ID, TX]
	Produce(context.Context, ProduceMethod, ...ProducingEvent) error
	// ProduceFromTable should be called by transaction wappers. It will
	// send ids to a central thread that will in turn call ProduceSpecificTxEvents.
	ProduceFromTable(ctx context.Context, eventIDs map[string][]ID) error
	RecordError(string, error) error       // pauses to avoid spamming
	RecordErrorNoWait(string, error) error // does not pause
	IsConfigured() bool
	Tracer() Tracer
	ValidateTopics(context.Context, []string) error
}

type CanValidateTopics interface {
	ValidateTopics(context.Context, []string) error
}

type Tracer interface {
	Logf(string, ...any)
}

type AbstractTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// EnhancedTX is used by event tests. It is the suggested interface for how
// transactions should enable events to be sent. It is implemented by
// eventdb.WrappedTX
type EnhancedTX interface {
	AbstractTX
	Produce(events ...ProducingEvent)
}

// If DB implements CanAugment then AugmentWithProducer will be invoked by
// the event framework.
type CanAugment[ID AbstractID[ID], TX AbstractTX] interface {
	AugmentWithProducer(Producer[ID, TX])
}

// AbstractDB is what events.Library consume. It is implemented by a combination of
// the db packages.
type AbstractDB[ID AbstractID[ID], TX AbstractTX] interface {
	CanTransact[TX]
	// ProduceSpecificTxEvents should not be called by transaction wrappers because
	// sending a batch to Kafka usually has a one second latecny.
	ProduceSpecificTxEvents(context.Context, []ID) (int, error)
	ProduceDroppedTxEvents(ctx context.Context, batchSize int) (int, error)
	LockOrError(ctx context.Context, key uint32, timeout time.Duration) (unlock func() error, err error)

	// MarkEventProcessed is always called inside a transaction. It must return
	// ErrAlreadyProcessed if the event has already been processed. The DB should not
	// be used by MarkEventProcessed, it is a DB method for simplicity of wrapping.
	MarkEventProcessed(ctx context.Context, tx TX, topic string, source string, id string, handlerName string) error
}

const ErrAlreadyProcessed errors.String = "event already processed"

const TimeoutErr errors.String = "did not obtain lock before deadline"

// This can be returned to prevent retry
const NotImplementedErr errors.String = "not implemented"

// CanTransact is a is a DB interface that implements that transaction part of AbstractDB
type CanTransact[TX AbstractTX] interface {
	AbstractTX
	Transact(context.Context, func(TX) error) error
}

type ProduceMethod string

const (
	ProduceInTransaction ProduceMethod = "tx"
	ProduceCatchUp       ProduceMethod = "catchUp"
	ProduceImmediate     ProduceMethod = "immediate"
)
