package eventdb

import (
	"context"
	"database/sql"

	"github.com/memsql/errors"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
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

type SaveEventsFunc[ID eventmodels.AbstractID[ID], TX BasicTX] func(context.Context, eventmodels.Tracer, TX, ...eventmodels.ProducingEvent) (map[string][]ID, error)

// Transact implements a Transact method as needed by AbstractDB (in ComboDB).
// It does not call itself recursively and insteads depends upon BeginTx
// from BasicDB (in ComboDB).
func Transact[ID eventmodels.AbstractID[ID], TX BasicTX, DB ComboDB[ID, TX]](
	ctx context.Context, db DB, tracer eventmodels.Tracer,
	f func(TX) error,
	saveEvents SaveEventsFunc[ID, TX],
	producer eventmodels.Producer[ID, TX],
) error {
	ids, err := WrapTransaction[ID, TX, DB](ctx, db, tracer, f, saveEvents, producer)
	if err != nil {
		return err
	}
	if len(ids) != 0 {
		if producer != nil {
			err = producer.ProduceFromTable(ctx, ids)
		} else {
			_, err = db.ProduceSpecificTxEvents(ctx, generic.CombineSlices(generic.Values(ids)...))
		}
	}
	return err
}

// WrapTransaction is a building block that can be shared between database-sprecific
// implementations. It handles the Begin/Rollback/Commit sequence and saving
// events.
func WrapTransaction[ID eventmodels.AbstractID[ID], TX BasicTX, DB BasicDB[TX]](
	ctx context.Context, db DB, tracer eventmodels.Tracer,
	f func(TX) error,
	saveEvents SaveEventsFunc[ID, TX],
	producer eventmodels.Producer[ID, TX],
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
			if producer != nil {
				topics := make([]string, 0, len(pending))
				seen := make(map[string]struct{}, len(pending))
				for _, event := range pending {
					topic := event.GetTopic()
					if _, ok := seen[topic]; !ok {
						seen[topic] = struct{}{}
						topics = append(topics, topic)
					}
				}
				err := producer.ValidateTopics(ctx, topics)
				if err != nil {
					return err
				}
			}
			var err error
			ids, err = saveEvents(ctx, tracer, tx, pending...)
			if err != nil {
				return err
			}
		}
		return nil
	}()
	return ids, err
}
