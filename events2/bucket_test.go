package events2

import (
	"bytes"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventdb"
)

func TestSetCatchUpBuckets(t *testing.T) {
	conn := New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](eventdb.ExampleBasicDB{}, "prefix")
	assert.Equal(t, defaultCatchUpBuckets, conn.catchUpBucketCount())

	conn.SetCatchUpBuckets(1)
	count, stride, offset := catchUpPlanFor(conn)
	assert.Equal(t, 1, count)
	assert.Equal(t, 1, stride)
	assert.Equal(t, 0, offset)

	conn.SetCatchUpBuckets(8)
	assert.Equal(t, 8, conn.catchUpBucketCount())
	assert.Panics(t, func() { conn.SetCatchUpBuckets(0) })
}

func TestBucketBoundsTileTheIDSpace(t *testing.T) {
	for _, count := range []int{2, 3, 8, 16} {
		var previousHigh []byte
		for bucket := 0; bucket < count; bucket++ {
			low, high, hasHigh := bucketBounds(bucket, count)
			require.Len(t, low, 16, "count %d bucket %d", count, bucket)
			if bucket == 0 {
				assert.Equal(t, make([]byte, 16), low, "count %d starts at zero", count)
			} else {
				assert.Equal(t, previousHigh, low, "count %d bucket %d continues the previous range", count, bucket)
			}
			if bucket == count-1 {
				assert.False(t, hasHigh, "count %d last bucket", count)
				continue
			}
			require.True(t, hasHigh)
			require.Len(t, high, 16)
			assert.Negative(t, bytes.Compare(low, high), "count %d bucket %d", count, bucket)
			previousHigh = high
		}
	}

	_, high, hasHigh := bucketBounds(0, 8)
	require.True(t, hasHigh)
	assert.Equal(t, byte(0x20), high[0])
	assert.Equal(t, make([]byte, 15), high[1:])

	low, _, hasHigh := bucketBounds(7, 8)
	require.False(t, hasHigh)
	assert.Equal(t, byte(0xe0), low[0])
	assert.Equal(t, make([]byte, 15), low[1:])
}

func TestApplyBucketBounds(t *testing.T) {
	base := sq.Select("id").From("eventsOutgoing").Where("sendAfter <= NOW(6)").PlaceholderFormat(sq.Question)

	q, args, err := applyBucketBounds(base, 0, 1).ToSql()
	require.NoError(t, err)
	assert.NotContains(t, q, "id >")
	assert.NotContains(t, q, "id <")
	assert.Empty(t, args)

	q, args, err = applyBucketBounds(base, 7, 8).ToSql()
	require.NoError(t, err)
	assert.Contains(t, q, "id >= ?")
	assert.NotContains(t, q, "id < ?")
	require.Len(t, args, 1)
	assert.Equal(t, byte(0xe0), args[0].([]byte)[0])
}

func TestCatchUpStrideVisitsEveryBucket(t *testing.T) {
	for _, count := range []int{1, 2, 8, 9, 16, 997} {
		for trial := 0; trial < 20; trial++ {
			stride := catchUpStride(count)
			if count > 1 && count%stride == 0 {
				t.Fatalf("count %d stride %d divides the count", count, stride)
			}
			offsets := []int{0}
			if count > 1 {
				offsets = []int{0, 1, count / 2, count - 1}
			}
			for _, offset := range offsets {
				seen := make(map[int]struct{}, count)
				for step := 0; step < count; step++ {
					seen[bucketForStep(step, stride, offset, count)] = struct{}{}
				}
				assert.Len(t, seen, count, "count %d stride %d offset %d trial %d", count, stride, offset, trial)
				start := 0
				if count > 1 {
					start = offset % count
				}
				assert.Equal(t, start, bucketForStep(0, stride, offset, count), "count %d stride %d offset %d", count, stride, offset)
			}
		}
	}
}

func TestCatchUpStrideStaysPutUntilTheCountChanges(t *testing.T) {
	cache := &catchUpStrideCache{}
	first, firstOffset := cache.strideFor(8)
	again, againOffset := cache.strideFor(8)
	assert.Equal(t, first, again)
	assert.Equal(t, firstOffset, againOffset)
	assert.GreaterOrEqual(t, firstOffset, 0)
	assert.Less(t, firstOffset, 8)
	assert.NotZero(t, 8%first)

	seen := map[int]struct{}{}
	for step := 0; step < 8; step++ {
		seen[bucketForStep(step, first, firstOffset, 8)] = struct{}{}
	}
	assert.Len(t, seen, 8)
	assert.Equal(t, firstOffset, bucketForStep(0, first, firstOffset, 8))

	other, otherOffset := cache.strideFor(9)
	againOther, againOtherOffset := cache.strideFor(9)
	assert.Equal(t, other, againOther)
	assert.Equal(t, otherOffset, againOtherOffset)
	assert.GreaterOrEqual(t, otherOffset, 0)
	assert.Less(t, otherOffset, 9)
	assert.NotZero(t, 9%other)

	restored, restoredOffset := cache.strideFor(8)
	againRestored, againRestoredOffset := cache.strideFor(8)
	assert.Equal(t, restored, againRestored)
	assert.Equal(t, restoredOffset, againRestoredOffset)
	assert.GreaterOrEqual(t, restoredOffset, 0)
	assert.Less(t, restoredOffset, 8)
	assert.NotZero(t, 8%restored)
}

func TestSendAfterExpr(t *testing.T) {
	sql, args, err := sendAfterExpr(false).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "NOW(6)", sql)
	assert.Empty(t, args)

	sql, args, err = sendAfterExpr(true).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "NOW(6) + INTERVAL 120000000 MICROSECOND", sql)
	assert.Empty(t, args)
	assert.Greater(t, sendAfterGrace, 2*fastPathSendTimeout)
}
