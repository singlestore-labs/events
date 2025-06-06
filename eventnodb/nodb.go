/*
Package eventnodb provides some convenience wrappers for
using events without a database.
*/
package eventnodb

import (
	"context"
	"database/sql"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go/sasl"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
)

// We need to provide:
//	ID eventmodels.AbstractID[ID],
//	TX eventmodels.AbstractTX,
//	DB eventmodels.AbstractDB[ID, TX]]

// ------- begin section that is duplicated in consumer_group_test.go ----------

type NoDBTx struct {
	*sql.Tx
}

func (tx *NoDBTx) Produce(events ...eventmodels.ProducingEvent) {
	// No-op for no-database implementation - events are discarded
}

type NoDB struct {
	*sql.DB
}

func (*NoDB) LockOrError(_ context.Context, _ uint32, _ time.Duration) (func() error, error) {
	return nil, errors.WithStack(eventmodels.NotImplementedErr)
}

func (*NoDB) MarkEventProcessed(_ context.Context, _ *NoDBTx, _ string, _ string, _ string, _ string) error {
	return errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) ProduceSpecificTxEvents(_ context.Context, _ []eventmodels.BinaryEventID) (int, error) {
	return 0, errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) ProduceDroppedTxEvents(_ context.Context, _ int) (int, error) {
	return 0, errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) Transact(_ context.Context, _ func(*NoDBTx) error) error {
	return errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) AugmentWithProducer(_ eventmodels.Producer[eventmodels.BinaryEventID, *NoDBTx]) {}

// ------- end section that is duplicated in consumer_group_test.go ----------

type Library struct {
	*events.Library[eventmodels.BinaryEventID, *NoDBTx, *NoDB]
}

func New() *Library {
	return &Library{
		Library: events.New[eventmodels.BinaryEventID, *NoDBTx, *NoDB](),
	}
}

func (lib *Library) ConfigureNoDB(tracer eventmodels.Tracer, mustRegisterTopics bool, saslMechanism sasl.Mechanism, tlsConfig *events.TLSConfig, brokers []string) {
	//nolint:staticcheck // QF1008: could remove embedded field "Library" from selector
	lib.Library.Configure(nil, tracer, mustRegisterTopics, saslMechanism, tlsConfig, brokers)
}
