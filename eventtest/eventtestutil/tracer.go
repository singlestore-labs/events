package eventtestutil

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/memsql/ntest"
	"github.com/stretchr/testify/assert"

	"github.com/singlestore-labs/events/eventmodels"
)

type Tracer struct {
	t ntest.T
}

func (tr Tracer) Logf(ctx context.Context, fmt string, a ...any) {
	tr.t.Helper()
	_, ok := ctx.Value(contextValue("span")).(string)
	assert.True(tr.t, ok, "log has a span")
	tr.t.Logf(fmt, a...)
}

type contextValue string

const verbose = true // XXX

func GetTracerConfig(t ntest.T) eventmodels.TracerConfig {
	var counter atomic.Int32
	return eventmodels.TracerConfig{
		BeginSpan: func(ctx context.Context, kv map[string]string) (context.Context, func()) {
			spanID := t.Name() + strconv.Itoa(int(counter.Add(1)))
			if verbose {
				t.Logf("starting span %s with %v", spanID, kv)
			}
			var closed int
			t.Cleanup(func() {
				assert.Equalf(t, 1, closed, "count of times span %s has been closed", closed)
			})
			return context.WithValue(ctx, contextValue("span"), spanID), func() {
				if verbose {
					t.Logf("ending span %s", spanID)
				}
				closed++
			}
		},
	}
}

func ExtraDetailLogger(t ntest.T, prefix string) eventmodels.Tracer {
	return Tracer{
		t: ntest.ExtraDetailLogger(t, prefix),
	}
}
