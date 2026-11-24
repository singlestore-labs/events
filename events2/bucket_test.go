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
	assert.Equal(t, defaultCatchUpBuckets, conn.catchUpBuckets)
	assert.GreaterOrEqual(t, conn.catchUpStride, 100)
	assert.GreaterOrEqual(t, conn.catchUpOffset, 0)
	assert.Less(t, conn.catchUpOffset, conn.catchUpBuckets)
	stride, offset := conn.catchUpStride, conn.catchUpOffset

	conn.SetCatchUpBuckets(0)
	assert.Equal(t, 1, conn.catchUpBuckets)
	assert.Equal(t, stride, conn.catchUpStride)
	assert.Equal(t, offset, conn.catchUpOffset)

	conn.SetCatchUpBuckets(8)
	assert.Equal(t, 8, conn.catchUpBuckets)
	assert.Equal(t, stride, conn.catchUpStride)
	assert.Equal(t, offset, conn.catchUpOffset)
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
	strides := []int{primesAtLeast100[0], primesAtLeast100[len(primesAtLeast100)/2], primesAtLeast100[len(primesAtLeast100)-1]}
	for _, count := range []int{1, 2, 8, 9, 16, 99} {
		for _, stride := range strides {
			assert.GreaterOrEqual(t, stride, 100)
			offset := 0
			if count > 1 {
				offset = count / 2
			}
			seen := make(map[int]struct{}, count)
			for step := 0; step < count; step++ {
				seen[bucketForStep(step, stride, offset, count)] = struct{}{}
			}
			assert.Len(t, seen, count, "count %d stride %d", count, stride)
			if count > 1 {
				assert.Equal(t, offset, bucketForStep(0, stride, offset, count))
			}
		}
	}
}

func TestCatchUpWalkCoversEveryBucket(t *testing.T) {
	conn := New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](eventdb.ExampleBasicDB{}, "prefix")
	conn.SetCatchUpBuckets(8)
	seen := map[int]struct{}{}
	for step := 0; step < conn.catchUpBuckets; step++ {
		seen[bucketForStep(step, conn.catchUpStride, conn.catchUpOffset, conn.catchUpBuckets)] = struct{}{}
	}
	assert.Len(t, seen, conn.catchUpBuckets)
	assert.Equal(t, conn.catchUpOffset, bucketForStep(0, conn.catchUpStride, conn.catchUpOffset, conn.catchUpBuckets))
}

func TestSendAfterExpr(t *testing.T) {
	sql, args, err := sendAfterExpr(false).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "NOW(6)", sql)
	assert.Empty(t, args)

	sql, args, err = sendAfterExpr(true).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "NOW(6) + INTERVAL 10000000 MICROSECOND", sql)
	assert.Empty(t, args)
	assert.Equal(t, fastPathSendTimeout, sendAfterGrace)
}
