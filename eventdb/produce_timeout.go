package eventdb

import (
	"context"
	"time"

	"github.com/memsql/errors"

	"github.com/singlestore-labs/events/eventmodels"
)

// BackgroundProduceTransactionTimeout caps how long background Kafka produce
// may run while its outgoing-table transaction is open. If Kafka is
// unreachable (for example SASL auth failure), topic listing/creation retries
// until the context is cancelled. CatchUpProduce uses a process-lifetime
// context, so those retries previously held a transaction for hours.
//
// On timeout the transaction rolls back, so the uncommitted delete is undone
// and CatchUpProduce can retry later.
const BackgroundProduceTransactionTimeout = 5 * time.Minute

// ErrBackgroundProduceTransactionTimeout is returned when background Kafka
// produce does not finish before BackgroundProduceTransactionTimeout.
const ErrBackgroundProduceTransactionTimeout errors.String = "background kafka produce transaction timed out"

// BoundProduceContext returns a child context that expires after
// BackgroundProduceTransactionTimeout, or sooner if ctx already has an earlier
// deadline.
func BoundProduceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return boundProduceContext(ctx, BackgroundProduceTransactionTimeout)
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

// ProduceFromOutgoingTable sends events selected/deleted by the transactional
// background producer. The Kafka call is bounded so that transaction cannot
// stay open indefinitely when brokers are down.
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
		return ErrBackgroundProduceTransactionTimeout.Errorf("%w", err)
	}
	return err
}
