package internal

import (
	"context"
	"time"
)

// DefaultTrimBatchSize is how many eventsProcessed rows one delete statement removes
// when TrimDB is called with batchSize <= 0.
var DefaultTrimBatchSize = 1000

// DefaultTrimBatchInterval is the pause after a full trim batch when TrimDB is called
// with interval < 0.
var DefaultTrimBatchInterval = 100 * time.Millisecond

// PauseTrimBatch waits between full trim batches. interval == 0 returns immediately.
// interval < 0 uses DefaultTrimBatchInterval.
func PauseTrimBatch(ctx context.Context, batchInterval time.Duration) error {
	if batchInterval < 0 {
		batchInterval = DefaultTrimBatchInterval
	}
	if batchInterval == 0 {
		return nil
	}
	timer := time.NewTimer(batchInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
