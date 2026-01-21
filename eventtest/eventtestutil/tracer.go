package eventtestutil

import (
	"context"
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/memsql/ntest"
	"github.com/stretchr/testify/assert"

	"github.com/singlestore-labs/events/eventmodels"
)

type contextValue string

var verbose = os.Getenv("EVENTS_TEST_VERBOSE_SPANS") == "true"

type spanContext struct {
	spanID           string
	closed           atomic.Int32
	closedErrorGiven atomic.Int32
	stack            string
	kv               map[string]string
}

func GetTracerConfig(t ntest.T) eventmodels.TracerConfig {
	var counter atomic.Int32
	return eventmodels.TracerConfig{
		BeginSpan: func(ctx context.Context, kv map[string]string) (context.Context, func()) {
			spanID := t.Name() + strconv.Itoa(int(counter.Add(1)))
			if verbose {
				t.Logf("starting span %s with %v", spanID, kv)
			}
			spanData := &spanContext{
				spanID: spanID,
				stack:  string(debug.Stack()),
				kv:     kv,
			}
			t.Cleanup(func() {
				closed := spanData.closed.Load()
				assert.Equalf(t, int32(1), closed, "count of times span %s %v has been closed, span started at %s", spanID, kv, spanData.stack)
			})
			return context.WithValue(ctx, contextValue("span"), spanData), func() {
				if verbose {
					t.Logf("ending span %s with %v", spanID, kv)
				}
				spanData.closed.Add(1)
			}
		},
	}
}

func TracerProvider(t ntest.T, prefix string) eventmodels.TracerProvider {
	t.Helper()
	edl := t
	if prefix != "" {
		edl = ntest.ExtraDetailLogger(t, prefix)
	}
	return func(ctx context.Context) eventmodels.Tracer {
		t.Helper()
		spanData, ok := ctx.Value(contextValue("span")).(*spanContext)
		if assert.True(t, ok, "log has a span") {
			if spanData.closedErrorGiven.Load() == 0 {
				if !assert.Zerof(t, spanData.closed.Load(), "span %s %v should not be closed but is. Span started at %s", spanData.spanID, spanData.kv, spanData.stack) {
					spanData.closedErrorGiven.Add(1)
				}
			}
		}
		return edl.Logf
	}
}

// TracerContext returns a context with a span inside. It is not part of the default context because
// we don't want to pass a context that already has a span to the library when starting consumers
// because then we wouldn't be able to test if logging inside consumers has a span.
func TracerContext(ctx context.Context, t ntest.T) context.Context {
	ctx, spanDone := GetTracerConfig(t).BeginSpan(ctx, map[string]string{
		"action": "test",
		"test":   t.Name(),
	})
	t.Cleanup(spanDone)
	return ctx
}
