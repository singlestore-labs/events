package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocatorNewIDAllocator(t *testing.T) {
	t.Parallel()

	t.Log("Testing newIDAllocator creates allocator with correct initial state")
	allocator := newIDAllocator(5, 3, 1.5, 100)
	assert.Equal(t, uint32(5), allocator.initialBatchSize)
	assert.Equal(t, uint32(3), allocator.increment)
	assert.Equal(t, 1.5, allocator.multiplier)
	assert.Equal(t, uint32(100), allocator.limit)
	assert.Equal(t, uint32(0), allocator.max)
	assert.Empty(t, allocator.untried)
	assert.Equal(t, 50, cap(allocator.untried)) // initialBatchSize * 10
}

func TestAllocatorInitialBatchGrowth(t *testing.T) {
	t.Parallel()

	t.Log("Testing initial batch growth adds correct number of IDs")
	allocator := newIDAllocator(3, 2, 1.5, 100)
	allocator.grow()

	assert.Equal(t, uint32(3), allocator.max)
	assert.Len(t, allocator.untried, 3) // Should add exactly 3 IDs

	t.Log("Verifying initial batch contains expected IDs")
	expectedIDs := map[uint32]bool{0: true, 1: true, 2: true}
	for _, id := range allocator.untried {
		assert.True(t, expectedIDs[id], "Unexpected ID %d in initial batch", id)
		delete(expectedIDs, id)
	}
	assert.Empty(t, expectedIDs, "Missing expected IDs")
}

func TestAllocatorIncrementGrowth(t *testing.T) {
	t.Parallel()

	t.Log("Testing growth behavior adds IDs as expected")
	allocator := newIDAllocator(2, 3, 1.2, 100)
	allocator.grow() // Initial batch
	initialCount := len(allocator.untried)

	allocator.grow() // Should grow by increment since multiplier result is small

	t.Log("Verifying growth added more IDs")
	assert.Greater(t, len(allocator.untried), initialCount, "Should have more IDs after growth")
	assert.Greater(t, allocator.max, uint32(initialCount), "Max should increase")
}

func TestAllocatorMultiplierGrowth(t *testing.T) {
	t.Parallel()

	t.Log("Testing multiplier-based growth when multiplier result > increment")
	allocator := newIDAllocator(2, 1, 2.0, 100)
	allocator.grow() // Initial batch
	allocator.grow() // First regular growth

	beforeCount := len(allocator.untried)
	beforeMax := allocator.max

	allocator.grow() // Should grow by multiplier since it's larger than increment

	t.Log("Verifying multiplier-based growth adds more IDs than increment would")
	assert.Greater(t, len(allocator.untried), beforeCount+1, "Should add more than increment(1)")
	assert.Greater(t, allocator.max, beforeMax+1, "Max should increase by more than increment")
}

func TestAllocatorLimitRespected(t *testing.T) {
	t.Parallel()

	t.Log("Testing growth respects limit boundary")
	allocator := newIDAllocator(5, 10, 2.0, 8)
	allocator.grow() // Initial batch
	allocator.grow() // Should grow but be clamped by limit

	t.Log("Verifying all IDs are within limit")
	for _, id := range allocator.untried {
		assert.LessOrEqual(t, id, uint32(8), "All IDs should be within limit")
	}

	t.Log("Verifying max respects limit")
	assert.LessOrEqual(t, allocator.max, uint32(9), "Max should be at most limit+1")

	beforeCount := len(allocator.untried)
	allocator.grow() // Should not add anything since we're at limit

	t.Log("Testing no further growth when at limit")
	assert.Equal(t, beforeCount, len(allocator.untried), "Should not add more IDs when at limit")
}

func TestAllocatorGetReturnsValidIDs(t *testing.T) {
	t.Parallel()

	t.Log("Testing get() returns valid IDs and removes them from pool")
	allocator := newIDAllocator(3, 2, 1.5, 10)
	id1, err1 := allocator.get()
	require.NoError(t, err1)
	assert.LessOrEqual(t, id1, uint32(10)) // Should be within limit

	id2, err2 := allocator.get()
	require.NoError(t, err2)
	assert.LessOrEqual(t, id2, uint32(10))
	assert.NotEqual(t, id1, id2, "Should not return duplicate IDs")
}

func TestAllocatorGetExhaustsPool(t *testing.T) {
	t.Parallel()

	t.Log("Testing get() until pool exhaustion")
	allocator := newIDAllocator(2, 1, 1.1, 3)
	seenIDs := make(map[uint32]bool)
	var ids []uint32

	// Keep getting IDs until we can't get any more
	for {
		id, err := allocator.get()
		if err != nil {
			t.Log("Pool exhausted as expected")
			assert.Contains(t, err.Error(), "ran out of possible lock ids")
			break
		}

		assert.False(t, seenIDs[id], "Got duplicate ID %d", id)
		assert.LessOrEqual(t, id, uint32(3), "ID should be within limit")
		seenIDs[id] = true
		ids = append(ids, id)

		// Safety check to avoid infinite loop
		if len(ids) > 10 {
			t.Fatal("Got too many IDs, something is wrong")
		}
	}

	t.Log("Verifying we got at least some IDs before exhaustion")
	assert.Greater(t, len(ids), 0, "Should get at least one ID before exhaustion")
}

func TestAllocatorRandomnessOfGet(t *testing.T) {
	t.Parallel()

	t.Log("Testing randomness of ID selection")
	allocator := newIDAllocator(10, 5, 1.5, 50)
	results := make(map[uint32]int)
	trials := 100

	// Get and return IDs many times to test randomness
	for i := 0; i < trials; i++ {
		id, err := allocator.get()
		require.NoError(t, err)
		results[id]++

		// Return the ID to the pool for next iteration
		allocator.untried = append(allocator.untried, id)
	}

	t.Log("Verifying some variation in ID selection")
	assert.Greater(t, len(results), 1, "Should select from multiple different IDs")
}

func TestAllocatorEdgeCaseZeroInitialBatch(t *testing.T) {
	t.Parallel()

	t.Log("Testing edge case with zero initial batch size")
	allocator := newIDAllocator(0, 2, 1.5, 10)
	id, err := allocator.get()

	require.NoError(t, err)
	assert.LessOrEqual(t, id, uint32(10), "Should get a valid ID within limit")

	t.Log("Verifying allocator can continue to work after zero initial batch")
	id2, err2 := allocator.get()
	require.NoError(t, err2)
	assert.NotEqual(t, id, id2, "Should get different IDs")
}

func TestAllocatorEdgeCaseLimit0(t *testing.T) {
	t.Parallel()

	t.Log("Testing edge case with limit 0")
	allocator := newIDAllocator(5, 2, 1.5, 0)
	id, err := allocator.get()

	if err == nil {
		t.Log("Got ID 0 successfully")
		assert.Equal(t, uint32(0), id, "Only ID 0 should be available")

		t.Log("Should error on second get() call since only ID 0 exists")
		_, err = allocator.get()
		assert.Error(t, err)
	} else {
		t.Log("Immediately ran out of IDs with limit 0")
		assert.Contains(t, err.Error(), "ran out of possible lock ids")
	}
}

func TestAllocatorConcurrentGrowthBehavior(t *testing.T) {
	t.Parallel()

	t.Log("Testing multiple growth cycles maintain correct behavior")
	allocator := newIDAllocator(2, 3, 1.8, 100)
	var allIDs []uint32
	seenIDs := make(map[uint32]bool)

	// Get a bunch of IDs to trigger multiple growths
	for len(allIDs) < 20 {
		id, err := allocator.get()
		require.NoError(t, err)

		assert.False(t, seenIDs[id], "Found duplicate ID %d", id)
		assert.LessOrEqual(t, id, uint32(100), "ID should be within limit")

		seenIDs[id] = true
		allIDs = append(allIDs, id)
	}

	t.Log("Verifying all IDs are unique and within bounds")
	assert.Len(t, seenIDs, len(allIDs), "All IDs should be unique")

	for id := range seenIDs {
		assert.LessOrEqual(t, id, uint32(100), "All IDs should be within limit")
	}
}
