package internal

import (
	"context"
	"time"

	"github.com/memsql/errors"
)

// PauseTrimBatch waits between full trim batches. interval == 0 returns immediately.
func PauseTrimBatch(ctx context.Context, batchInterval time.Duration) error {
	if batchInterval < 0 {
		return errors.Errorf("trim batch interval %s must not be negative", batchInterval)
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
