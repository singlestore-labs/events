package events

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundProduceContextTimesOut(t *testing.T) {
	ctx, cancel := boundProduceContext(context.Background(), 20*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("produce bound did not expire")
	}
}

func TestBoundProduceContextKeepsShorterParentDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancelParent()
	start := time.Now()
	ctx, cancel := boundProduceContext(parent, time.Hour)
	defer cancel()
	<-ctx.Done()
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

func TestWrapProduceTimeout(t *testing.T) {
	ctx, cancel := boundProduceContext(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	err := wrapProduceTimeout(ctx, ctx.Err())
	require.ErrorIs(t, err, ErrProduceTransactionTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWrapProduceTimeoutLeavesOtherErrorsAlone(t *testing.T) {
	err := wrapProduceTimeout(context.Background(), assert.AnError)
	require.Equal(t, assert.AnError, err)
}
