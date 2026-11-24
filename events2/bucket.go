package events2

import (
	"math/big"

	sq "github.com/Masterminds/squirrel"
)

const defaultCatchUpBuckets = 8

// SetCatchUpBuckets sets how many id ranges a catch-up pass splits
// eventsOutgoing into. Values below 1 are stored as 1, which turns bucketing
// off. The default is 8.
//
// The stride and starting bucket are chosen in New and never change. The
// stride is a prime of at least 100, so any count below that is visited
// exactly once. Call this before catch-up starts. It is not goroutine-safe,
// and there is no reason to change the count once catch-up is running.
func (c *Connection[TX, DB]) SetCatchUpBuckets(n int) {
	if n < 1 {
		n = 1
	}
	c.catchUpBuckets = n
}

func applyBucketBounds(sb sq.SelectBuilder, bucket, count int) sq.SelectBuilder {
	if count <= 1 {
		return sb
	}
	low, high, hasHigh := bucketBounds(bucket, count)
	sb = sb.Where(sq.GtOrEq{"id": low})
	if hasHigh {
		sb = sb.Where(sq.Lt{"id": high})
	}
	return sb
}

// bucketBounds splits the 16-byte id space into count contiguous ranges.
// Ranges tile every possible id, so shrinking the count leaves nothing behind.
// The last bucket has no upper bound.
func bucketBounds(bucket, count int) (low, high []byte, hasHigh bool) {
	span := new(big.Int).Lsh(big.NewInt(1), 128)
	n := big.NewInt(int64(count))
	low = idBound(new(big.Int).Div(new(big.Int).Mul(big.NewInt(int64(bucket)), span), n))
	if bucket >= count-1 {
		return low, nil, false
	}
	high = idBound(new(big.Int).Div(new(big.Int).Mul(big.NewInt(int64(bucket+1)), span), n))
	return low, high, true
}

func idBound(i *big.Int) []byte {
	raw := i.Bytes()
	if len(raw) > 16 {
		raw = raw[len(raw)-16:]
	}
	out := make([]byte, 16)
	copy(out[16-len(raw):], raw)
	return out
}

func bucketForStep(step, stride, offset, count int) int {
	if count <= 1 {
		return 0
	}
	return int((int64(offset) + int64(step)*int64(stride)) % int64(count))
}

// primesAtLeast100 is every prime from 100 up to 1000. A stride chosen from
// this list stays coprime to any smaller bucket count, so it never has to
// change.
var primesAtLeast100 = []int{
	101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157,
	163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227,
	229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283,
	293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367,
	373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439,
	443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509,
	521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599,
	601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661,
	673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751,
	757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829,
	839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919,
	929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997,
}
