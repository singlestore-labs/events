/*
package tracer is a minimal implementation of the tracer interface. It builds
on top of something that implements Logf.

It ignores key/values, begin, end.
*/
package tracer

import "context"

type Base interface {
	Logf(string, ...any)
}

type tracer struct {
	base base
}

type ctxkey struct{}

var key = ctxkey{}

func New(base Base) {
	return &tracer{
		keys: make(map[string]string),
	}
}

func (t *tracer) FromContext(ctx context.Context) Tracer {
	logger := ctx.Value(key)
	if logger == nil {
		return t
	}
	return logger.(*tracer)
}

func (t *tracer) Context(ctx context.Context) context.Context {
	return ctx.WithValue(ctx, key, t)
}

func (t *tracer) Begin() Tracer                         { return t }
func (t *tracer) Done()                                 {}
func (t *tracer) WithKeyValue(key, value string) Tracer { return t }
func (t *tracer) Logf(a string, v ...any)               { t.base.Logf(a, v...) }
