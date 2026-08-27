package eventdb

import (
	"context"
	"time"

	"github.com/memsql/errors"

	"github.com/singlestore-labs/events/eventmodels"
)

// ProduceTransactionTimeout caps how long Kafka produce may run while the
// eventsOutgoing transaction is open. produceEvents locks and deletes rows in
// that transaction and then produces to Kafka before committing. If Kafka is
// unreachable (for example a SASL auth failure), topic listing and creation
// retry until the context is cancelled, and CatchUpProduce supplies a
// process-lifetime context, so the transaction could stay open for hours. An
// open transaction that long holds back restart_lsn on PostgreSQL replication
// slots and retains WAL.
//
// On timeout the transaction rolls back, undoing the uncommitted deletes, and
// the events are produced by a later catch-up pass.
const ProduceTransactionTimeout = 5 * time.Minute

// ErrProduceTransactionTimeout is returned when Kafka produce does not finish
// before ProduceTransactionTimeout while the eventsOutgoing transaction is open.
const ErrProduceTransactionTimeout errors.String = "kafka produce timed out while the outgoing events transaction was open"

// BoundProduceContext returns a child context that expires after
// ProduceTransactionTimeout, or sooner if ctx already has an earlier deadline.
func BoundProduceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return boundProduceContext(ctx, ProduceTransactionTimeout)
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

// ProduceFromOutgoingTable produces events that were locked and deleted inside
// the eventsOutgoing transaction. The Kafka call is bounded so that the
// transaction cannot stay open indefinitely when brokers are unreachable.
func ProduceFromOutgoingTable[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX](
	ctx context.Context,
	producer eventmodels.Producer[ID, TX],
	method eventmodels.ProduceMethod,
	events ...eventmodels.ProducingEvent,
) error {
	ctx, cancel := BoundProduceContext(ctx)
	defer cancel()
	err := producer.Produce(ctx, method, events...)
	return wrapProduceTimeout(ctx, err)
}

func wrapProduceTimeout(boundCtx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if boundCtx.Err() == context.DeadlineExceeded && errors.Is(err, context.DeadlineExceeded) {
		return ErrProduceTransactionTimeout.Errorf("%w", err)
	}
	return err
}
