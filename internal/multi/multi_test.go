package multi

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestContext_New_Add_Cancel_Toggle(t *testing.T) {
	m := New()

	// Initially closed (cancelled) and Err() should be context.Canceled
	require.True(t, isClosed(m.Done()))
	require.ErrorIs(t, m.Err(), context.Canceled)

	// Add a live context -> becomes un-cancelled
	ctx, cancel := context.WithCancel(context.Background())
	m.Add(ctx)
	// Done should be open now and Err() nil
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())

	// Cancel the only live context -> eventually closed and Err() canceled
	cancel()
	require.Eventually(t, func() bool { return isClosed(m.Done()) }, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return m.Err() == context.Canceled }, time.Second, 10*time.Millisecond)

	// Add a new live context after cancellation -> re-opens Done and Err() nil again
	ctx2, cancel2 := context.WithCancel(context.Background())
	m.Add(ctx2)
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())
	cancel2() // cleanup
}

func TestContext_MultipleContexts_CancelOneKeepsOpen(t *testing.T) {
	m := New()
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	m.Add(ctx1)
	m.Add(ctx2)

	// Not closed while at least one is alive
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())

	// Cancel only one -> still open
	cancel1()
	// Allow background cleanup, but Done must remain open
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())

	// Cancel the second -> eventually closed
	cancel2()
	require.Eventually(t, func() bool { return isClosed(m.Done()) }, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return m.Err() == context.Canceled }, time.Second, 10*time.Millisecond)
}

func TestContext_DeadlineLatest(t *testing.T) {
	m := New()
	// Two contexts with deadlines; latest should be returned
	now := time.Now()
	later := now.Add(200 * time.Millisecond)
	ctx1, cancel1 := context.WithDeadline(context.Background(), now.Add(100*time.Millisecond))
	ctx2, cancel2 := context.WithDeadline(context.Background(), later)
	defer cancel1()
	defer cancel2()
	m.Add(ctx1)
	m.Add(ctx2)

	deadline, ok := m.Deadline()
	require.True(t, ok)
	require.Equal(t, later, deadline)
}

func TestContext_Deadline_NoDeadlinePresentReturnsFalse(t *testing.T) {
	m := New()
	ctx := context.Background() // no deadline
	m.Add(ctx)
	_, ok := m.Deadline()
	require.False(t, ok)
}

func TestContext_ValueFromFirstLiveContext(t *testing.T) {
	m := New()
	type keyType string
	key := keyType("key")
	ctx := context.WithValue(context.Background(), key, "value")
	m.Add(ctx)
	// Eventually, the cancelled placeholder is cleaned and first live context provides value
	require.Eventually(t, func() bool { v := m.Value(key); return v == "value" }, time.Second, 10*time.Millisecond)
}

func TestContext_ZeroValueSafety(t *testing.T) {
	// Use zero-value Context and ensure Add initializes it properly
	var m Context
	// Before any Add, Done should behave like closed channel
	require.True(t, isClosed(m.Done()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Add(ctx)
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())
}

// This test primarily exercises the cleaning path triggered by Add() when
// the number of underlying contexts reaches multiples of cleanEvery.
// Validation is lightweight: we ensure the context remains open while
// at least one live context exists.
func TestContext_Cleaning_Exercises(t *testing.T) {
	m := New()

	// First live context kept un-cancelled so Done stays open
	front, frontCancel := context.WithCancel(context.Background())
	m.Add(front)

	// Add a bunch of contexts and cancel some of them deeper in the slice.
	// This should hit the clean() path multiple times via Add.
	cancels := make([]context.CancelFunc, 0, 80)
	for i := 0; i < 80; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		m.Add(ctx)
		cancels = append(cancels, cancel)
		// cancel every 10th to create cancelled contexts not at the front
		if i%10 == 5 {
			cancel()
		}
	}

	// With the front still live, Done should remain open and Err() nil
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())

	// Cleanup: cancel everything and ensure eventual closure
	frontCancel()
	for _, c := range cancels {
		c()
	}
	require.Eventually(t, func() bool { return isClosed(m.Done()) }, time.Second, 10*time.Millisecond)
}

// Exercise the maxKept limit by attempting to add more than maxKept contexts.
// We don't verify exact counts; we just ensure behavior stays sane and the
// context eventually closes after cancelling all added contexts.
func TestContext_MaxKept_Exercises(t *testing.T) {
	m := New()

	// Add a front live context
	front, frontCancel := context.WithCancel(context.Background())
	m.Add(front)

	// Attempt to add significantly more than maxKept contexts.
	// Add() will stop accepting once internal length exceeds maxKept.
	cancels := make([]context.CancelFunc, 0, 300)
	for i := 0; i < 300; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		m.Add(ctx)
		cancels = append(cancels, cancel)
	}

	// While at least one is live, Done should be open
	require.False(t, isClosed(m.Done()))
	require.NoError(t, m.Err())

	// Cancel all contexts; Done should eventually close
	frontCancel()
	for _, c := range cancels {
		c()
	}
	require.Eventually(t, func() bool { return isClosed(m.Done()) }, 2*time.Second, 10*time.Millisecond)
}
