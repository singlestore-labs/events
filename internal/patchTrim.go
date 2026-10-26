package internal

import (
	"context"
	"time"

	"github.com/memsql/errors"
)

// Sleep waits for duration or for context cancel, whichever comes first
func Sleep(ctx context.Context, duration time.Duration) error {
	if duration < 0 {
		return errors.Errorf("trim batch interval %s must not be negative", duration)
	}
	if duration == 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
