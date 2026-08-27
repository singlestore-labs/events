package eventdb

import (
	"context"
	"database/sql"

	"github.com/memsql/errors"

	"github.com/singlestore-labs/events/eventmodels"
)

// BasicTX is what's needed from a transaction in WrapTransaction
type BasicTX interface {
	JustSQLTX
	GetPendingEvents() []eventmodels.ProducingEvent
}

type JustSQLTX interface {
	eventmodels.AbstractTX
	Commit() error
	Rollback() error
}

// BasicDB exists to allow the type of transactions to be overridden.
type BasicDB[TX JustSQLTX] interface {
	eventmodels.AbstractTX
	BeginTx(context.Context, *sql.TxOptions) (TX, error)
}

// ComboDB has both the low-level BeginTx and the high-level methods required of AbstractDB
type ComboDB[ID eventmodels.AbstractID[ID], TX BasicTX] interface {
	BasicDB[TX]
	eventmodels.AbstractDB[ID, TX]
}

// SaveEventsFunc is used to preserve events from a transaction.
// It needs to work if the producer is nil. Tracer can be ignored
// if producer is not nil.
type SaveEventsFunc[ID eventmodels.AbstractID[ID], TX BasicTX] func(context.Context, eventmodels.Tracer, TX, eventmodels.Producer[ID, TX], ...eventmodels.ProducingEvent) (map[string][]ID, error)

// Transact implements a Transact method as needed by AbstractDB (in ComboDB).
// It does not call itself recursively and insteads depends upon BeginTx
// from BasicDB (in ComboDB).
// backupTracer is only used if optProducer is nil.
func Transact[ID eventmodels.AbstractID[ID], TX BasicTX, DB ComboDB[ID, TX]](
	ctx context.Context,
	db DB,
	backupTracer eventmodels.Tracer,
	f func(TX) error,
	saveEvents SaveEventsFunc[ID, TX],
	optProducer eventmodels.Producer[ID, TX],
) error {
	ids, err := WrapTransaction[ID, TX, DB](ctx, backupTracer, db, f, saveEvents, optProducer)
	if err != nil {
		return err
	}
	if len(ids) != 0 && optProducer != nil {
		err = optProducer.ProduceFromTable(ctx, ids)
	}
	return err
}

// WrapTransaction is a building block that can be shared between database-sprecific
// implementations. It handles the Begin/Rollback/Commit sequence and saving
// events.
// backupTracer is only used if optProducer is nil
func WrapTransaction[ID eventmodels.AbstractID[ID], TX BasicTX, DB BasicDB[TX]](
	ctx context.Context,
	backupTracer eventmodels.Tracer,
	db DB,
	f func(TX) error,
	saveEvents SaveEventsFunc[ID, TX],
	optProducer eventmodels.Producer[ID, TX],
) (map[string][]ID, error) {
	var ids map[string][]ID
	err := func() (err error) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = e
				} else {
					err = errors.Errorf("panic in transaction: %s", r)
				}
			}
			if err != nil {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
				if err != nil {
					err = errors.WithStack(err)
				}
			}
		}()
		err = f(tx)
		if err != nil {
			return err
		}
		if pending := tx.GetPendingEvents(); len(pending) != 0 {
			err := ValidateEventTopics(ctx, optProducer, pending...)
			if err != nil {
				return err
			}
			ids, err = saveEvents(ctx, backupTracer, tx, optProducer, pending...)
			if err != nil {
				return err
			}
		}
		return nil
	}()
	return ids, err
}

// ValidateEventTopics returns an error if the topics are not valid. It only
// checks if a Producer is provided.
func ValidateEventTopics[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX](
	ctx context.Context,
	optProducer eventmodels.Producer[ID, TX],
	events ...eventmodels.ProducingEvent,
) error {
	if optProducer == nil {
		return nil
	}
	topics := make([]string, 0, len(events))
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		topic := event.GetTopic()
		if _, ok := seen[topic]; !ok {
			seen[topic] = struct{}{}
			topics = append(topics, topic)
		}
	}
	return optProducer.ValidateTopics(ctx, topics)
}
