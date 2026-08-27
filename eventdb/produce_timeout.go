package eventdb

import (
	"context"
	"time"

	"github.com/memsql/errors"

	"github.com/singlestore-labs/events/eventmodels"
)

// ProduceInTransactionTimeout caps how long Kafka produce may run while a
// database transaction is open. produceEvents deletes rows from eventsOutgoing
// then produces to Kafka before commit. If Kafka is unreachable (for example
// SASL auth failure), topic listing/creation retries until the parent context
// is cancelled. CatchUpProduce uses a process-lifetime context, so those
// retries previously held an open transaction for hours. That pins
// PostgreSQL restart_lsn / WAL for replication slots.
//
// On timeout the transaction rolls back, so the uncommitted delete is undone
// and CatchUpProduce can retry later.
const ProduceInTransactionTimeout = time.Minute

// ErrProduceInTransactionTimeout is returned when Kafka produce does not
// finish before ProduceInTransactionTimeout while a DB transaction is open.
const ErrProduceInTransactionTimeout errors.String = "kafka produce timed out while a database transaction was open"

// ProduceInTransactionIdleTimeout is for database-side idle-transaction limits
// that back up ProduceInTransactionTimeout. It is deliberately longer so that
// the Go deadline wins the race in normal operation: cancelling the context
// rolls the transaction back cleanly, whereas PostgreSQL's
// idle_in_transaction_session_timeout terminates the whole session.
const ProduceInTransactionIdleTimeout = 2 * ProduceInTransactionTimeout

// BoundProduceContext returns a child context that expires after
// ProduceInTransactionTimeout, or sooner if ctx already has an earlier deadline.
func BoundProduceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return boundProduceContext(ctx, ProduceInTransactionTimeout)
}

func boundProduceContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// ProduceFromOutgoingTable sends events that were selected/deleted inside an
// open database transaction. The Kafka call is bounded so the transaction
// cannot stay open indefinitely when brokers are down.
func ProduceFromOutgoingTable[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX](
	ctx context.Context,
	producer eventmodels.Producer[ID, TX],
	method eventmodels.ProduceMethod,
	events ...eventmodels.ProducingEvent,
) error {
	ctx, cancel := BoundProduceContext(ctx)
	defer cancel()
	err := producer.Produce(ctx, method, events...)
	return wrapProduceInTransactionError(ctx, err)
}

func wrapProduceInTransactionError(boundCtx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if boundCtx.Err() == context.DeadlineExceeded && errors.Is(err, context.DeadlineExceeded) {
		return ErrProduceInTransactionTimeout.Errorf("%w", err)
	}
	return err
}
