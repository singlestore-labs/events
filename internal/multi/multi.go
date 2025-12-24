package multi

import (
	"context"
	"sync"
	"time"
)

const (
	cleanEvery = 50  // remove dead contexts every Nth additional context added
	maxKept    = 250 // if we reach this many, we assume they're not being cancelled, ever
)

// Context is meant for background operations that should run
// only as long as there is a valid context available. It manages a
// collection of contexts and if any one of them is not cancelled
// then Context is not cancelled. Even after cancellation, a
// new context can be added and then Context becomes un-cancelled
// which is not normal behavior for a context.
//
// All operations are thread-safe.
//
// Context limits the number of contexts that are added to it.
type Context struct {
	underlying []context.Context // always with at least one element
	lock       sync.Mutex
	done       chan struct{}
}

var _ context.Context = &Context{}

var cancelledContext = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

var closedChannel = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// New creates a Context. It is in a done/cancelled state
// until contexts are added with Add.
func New() *Context {
	return &Context{
		underlying: []context.Context{cancelledContext},
		done:       closedChannel,
	}
}

// Add supplies an additional context to Context.
func (m *Context) Add(ctx context.Context) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.done == nil {
		// handling the situation of an un-initialized Context
		m.done = closedChannel
	}
	if len(m.underlying) > maxKept {
		return
	}
	m.underlying = append(m.underlying, ctx)
	select {
	case <-m.done:
		m.startDoneWatch()
	default:
	}
	if len(m.underlying)%cleanEvery == 0 {
		m.clean()
	}
}

// startDoneWatch removes done contexts from underlying. It does this
// in a background thread.
// Context must be locked to call startDoneWatch
func (m *Context) startDoneWatch() {
	m.done = make(chan struct{})
	// go-routine exits when all contexts are cancelled
	go func() {
		for {
			// non-blocking, clear out cancelled contexts from the
			// start of the array, returning a live context to wait upon
			done, ctx := func() (bool, context.Context) {
				m.lock.Lock()
				defer m.lock.Unlock()
				for len(m.underlying) > 0 {
					select {
					case <-m.underlying[0].Done():
						if len(m.underlying) == 1 {
							close(m.done)
							return true, m.underlying[0]
						}
						m.underlying = m.underlying[1:]
					default:
						return false, m.underlying[0]
					}
				}
				close(m.done)
				return true, nil
			}()
			if done {
				return
			}
			<-ctx.Done()
		}
	}()
}

// Context must be locked to call clean
func (m *Context) clean() {
	for i, ctx := range m.underlying {
		if ctx.Err() != nil {
			if len(m.underlying) == 1 {
				return
			}
			n := make([]context.Context, i, len(m.underlying)-1)
			copy(n, m.underlying[:i])
			for _, ctx := range m.underlying[i+1:] {
				if ctx.Err() == nil {
					n = append(n, ctx)
				}
			}
			m.underlying = n
			return
		}
	}
}

// Deadline isn't used in events. It's implemented because it's part of
// Context. If there are multiple contexts in underlying, the one with
// the latest deadline is returned.
func (m *Context) Deadline() (deadline time.Time, ok bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var latest time.Time
	if len(m.underlying) == 0 {
		// only if Context not initialized
		return time.Now(), true
	}
	for _, ctx := range m.underlying {
		if ctx.Err() != nil {
			continue
		}
		t, ok := ctx.Deadline()
		if !ok {
			return t, ok
		}
		if latest.IsZero() || latest.Before(t) {
			latest = t
		}
	}
	return latest, true
}

func (m *Context) Done() <-chan struct{} {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.done == nil {
		return closedChannel
	}
	return m.done
}

func (m *Context) Err() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// The following loop should be fast because if the
	// first context is cancelled, the background thread in
	// startDoneWatch() should quickly discard it (and any more)
	// until it find one that is not cancelled. The only time
	// Err() would need to walk a long list is if the call to Err()
	// coincided with the first context being cancelled before
	// startDoneWatch could clean up.
	for _, ctx := range m.underlying {
		if err := ctx.Err(); err == nil {
			return nil
		}
	}
	return m.underlying[0].Err()
}

func (m *Context) Value(key any) any {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.underlying[0].Value(key)
}
