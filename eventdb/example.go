package eventdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/memsql/errors"
	"go.uber.org/atomic"

	"singlestore.com/helios/events/eventmodels"
)

var uniqueID atomic.Int64

type ExampleBasicDB struct {
	*sql.DB
}

type ExampleBasicTX struct {
	*sql.Tx
	pendingEvents []eventmodels.ProducingEvent
	id            int64
}

func (tx *ExampleBasicTX) GetPendingEvents() []eventmodels.ProducingEvent {
	return tx.pendingEvents
}

func (tx *ExampleBasicTX) Produce(events ...eventmodels.ProducingEvent) {
	tx.pendingEvents = append(tx.pendingEvents, events...)
}

func (db ExampleBasicDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*ExampleBasicTX, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &ExampleBasicTX{
		Tx: tx,
		id: uniqueID.Add(1),
	}, nil
}

var _ eventmodels.EnhancedTX = &ExampleBasicTX{}

var _ BasicDB[*ExampleBasicTX] = &ExampleBasicDB{}

func GetTxID(tx any) string {
	if ebt, ok := tx.(*ExampleBasicTX); ok {
		return fmt.Sprintf("id:%d", ebt.id)
	}
	return ""
}
