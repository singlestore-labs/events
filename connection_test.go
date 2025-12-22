package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConstants(t *testing.T) {
	t.Log("make sure nobody changes contants in ways that would break behavior")
	assert.Greater(t, broadcastReaderIdleTimeout, time.Second)
	assert.Less(t, broadcastHeartbeatRandom, 0.8)
	assert.Less(t, maxConsumerGroupNameLength, 56)
}
