package events

import (
	"context"
	"testing"
	"time"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/stretchr/testify/assert"
)

func TestConstants(t *testing.T) {
	t.Log("make sure nobody changes contants in ways that would break behavior")
	assert.Greater(t, broadcastReaderIdleTimeout, time.Second)
	assert.Less(t, broadcastHeartbeatRandom, 0.8)
	assert.Less(t, maxConsumerGroupNameLength, 56)
}

func TestThreadContextAdoptsLateLifecycleContext(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	ctx1, done1 := lib.threadContext(map[string]string{"thread": "test 1"})
	defer done1()
	ctx2, done2 := lib.threadContext(map[string]string{"thread": "test 2"})
	defer done2()

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	lib.lock.Lock()
	lib.consumeCtx = consumeCtx
	lib.notifyContextUpdateLocked()
	lib.lock.Unlock()

	cancelConsume()
	assert.Eventually(t, func() bool {
		return ctx1.Err() != nil && ctx2.Err() != nil
	}, time.Second, time.Millisecond*10)
}

func TestThreadContextWithoutLifecycleLastsUntilShutdown(t *testing.T) {
	lib := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
	ctx, done := lib.threadContext(map[string]string{"thread": "test"})
	defer done()

	assert.Never(t, func() bool {
		return ctx.Err() != nil
	}, time.Millisecond*50, time.Millisecond*10)

	lib.Shutdown(context.Background())
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}
