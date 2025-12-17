/*
package pwork orchestrates doing common work in parallel
launched from multiple threads.

For a given instance, there is a map of items -> state.
The work for a single item is done in a thread.

The work can be invoked from multiple threads but an item
will be attempted in only one thread at a time.

Requesting threads will ask for work for multiple items at
once. The request returns when those items are complete,
even if they're completed by other threads.
*/

package pwork

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/muir/gwrap"
)

// Work provides the parameters for the jobs to be done.
// Except where marked, all parameters are optional.
type Work[Item comparable, Why any] struct {
	MaxSimultaneous int                                    // required
	BackoffPolicy   backoff.Policy                         // required
	ItemWork        func(context.Context, Item, Why) error // called to do the work in a background thread. Required.

	// everything below is optional

	WorkDeadline     time.Duration                                       // how long to work on all items
	ItemRetryDelay   time.Duration                                       // how long to wait before retrying a specific item
	ErrorReporter    func(context.Context, error, Why)                   // called to report errors
	IsFatalError     func(error) bool                                    // called in WorkUntilDone to bail out early.
	ClearedUp        func(context.Context, error, Why, []Item)           // called when prior errors are resolved
	FirstWorkMessage func(context.Context, Why, Item)                    // called when there is work to be done
	NotRetryingError func(context.Context, Item, Why, error) error       // called when an item will not be retried, perhaps too soon
	RetryingOrNot    func(context.Context, bool, Item, Why)              // called before ItemWork, only if previously failed
	ItemPreWork      func(context.Context, Item, Why) error              // called before doing the work
	ItemDone         func(context.Context, Item, Why)                    // called on success
	ItemFailed       func(context.Context, Item, Why, error, bool) error // called on failure
	ItemTimeoutError func(context.Context, Item, Why, error) error       // called on timeout
	ItemPending      func(context.Context, Item, Why)                    // called if will be or is being processed
	PreWork          func(context.Context, Why, []Item) error            // called before starting per-item work

	progressStore     gwrap.SyncMap[Item, *itemProgress]
	simultaneousLimit chan struct{}
	initOnce          sync.Once
}

type ItemState int32

const (
	// state=itemProcessing means that the work for the item has been
	// started but it is not yet complete.
	ItemProcessing ItemState = 0
	ItemDone       ItemState = 1
	// itemFailed indicates that the work for the item failed but it's
	// not yet being retried
	ItemFailed       ItemState = 2
	ItemDoesNotExist ItemState = 3
)

// itemProgress is a per-item structure that is optimized for the
// fastpath of the work already done created: state==ItemDone.
//
// The state of creatingTopic
//
//	work done		state=ItemDone
//	not yet tried		not present in map, progressing not closed
//	trying			state=ItemProcessing, progressing not closed
//	just finished		state=ItemProcessing, progressing closed
//	errored, too soon	state=ItemFailed, error set to not-nil, errorTime < retry time, created closed
//	errored, time to try	state=ItemFailed, error set to not-nil, errorTime > retry time, created closed
//	errored, retrying	state=ItemProcessing, created not closed
//
// lock is held when changing state from ItemFailed to ItemProcessing so that only one thread makes that change
// and the channel can be adjusted at the same time.
type itemProgress struct {
	lock       sync.Mutex
	state      atomic.Int32  // 0=processing 1=done 2=error
	processing chan struct{} // closed when when request is finished
	error      error
	errorTime  time.Time
}

func (w *Work[Item, Why]) GetState(item Item) ItemState {
	seen, ok := w.progressStore.Load(item)
	if !ok {
		return ItemDoesNotExist
	}
	return ItemState(seen.state.Load())
}

func (w *Work[Item, Why]) SetDone(item Item) {
	n := &itemProgress{
		processing: make(chan struct{}),
	}
	close(n.processing)
	n.state.Store(int32(ItemDone))
	seen, ok := w.progressStore.LoadOrStore(item, n)
	if ok {
		seen.state.Store(int32(ItemDone))
	}
}

func (w *Work[Item, Why]) WorkUntilDone(ctx context.Context, items []Item, why Why) error {
	var priorError error
	b := w.BackoffPolicy.Start(ctx)
	for {
		err := w.Work(ctx, items, why)
		if err == nil {
			if priorError != nil && w.ClearedUp != nil {
				w.ClearedUp(ctx, priorError, why, items)
			}
			return nil
		}
		priorError = err
		if w.ErrorReporter != nil {
			w.ErrorReporter(ctx, err, why)
		}
		if w.IsFatalError != nil && w.IsFatalError(err) {
			return err
		}
		if !backoff.Continue(b) {
			return err
		}
	}
}

func (w *Work[Item, Why]) Work(ctx context.Context, items []Item, why Why) error {
	if w.MaxSimultaneous <= 0 {
		return errors.Errorf("must set MaxSimultaneous")
	}
	w.initOnce.Do(func() {
		w.simultaneousLimit = make(chan struct{}, w.MaxSimultaneous)
	})
	if w.WorkDeadline != 0 {
		var cancelTimeout func()
		ctx, cancelTimeout = context.WithTimeout(ctx, w.WorkDeadline)
		defer cancelTimeout()
	}
	if w.PreWork != nil {
		err := w.PreWork(ctx, why, items)
		if err != nil {
			return err
		}
	}
	var firstWorkFound bool // no point in logging much until we know there is work to do
	var outstanding map[Item]*itemProgress
	var overrideFinalError error
	for _, item := range items {
		seen, ok := w.progressStore.Load(item)
		if !ok {
			seen, ok = w.progressStore.LoadOrStore(item,
				&itemProgress{
					processing: make(chan struct{}),
				})
		}
		state := ItemState(seen.state.Load())
		if state == ItemDone {
			continue
		}
		if !firstWorkFound {
			if w.FirstWorkMessage != nil {
				w.FirstWorkMessage(ctx, why, item)
			}
			firstWorkFound = true
		}
		doWork := !ok
		if state == ItemFailed {
			var err error
			doWork, err = w.shouldRetry(seen)
			if err != nil {
				if err != nil {
					if w.NotRetryingError != nil {
						err = w.NotRetryingError(ctx, item, why, err)
					} else if w.ItemFailed != nil {
						err = w.ItemFailed(ctx, item, why, err, false)
					}
				}
				return err
			}
			if w.RetryingOrNot != nil {
				w.RetryingOrNot(ctx, doWork, item, why)
			}
		}

		// doWork is true if either no other thread was already working on the item (ok == false) or
		// if it has previously failed and enough time has passed that it's time to try doing the work it again.
		// In either case, state will already be ItemProcessing
		if doWork {
			if w.ItemPreWork != nil {
				err := w.ItemPreWork(ctx, item, why)
				if err != nil && overrideFinalError == nil {
					overrideFinalError = err
					now := time.Now()
					func() {
						seen.lock.Lock()
						defer seen.lock.Unlock()
						// must set error before setting state to failed
						seen.error = overrideFinalError
						seen.errorTime = now
						seen.state.Store(int32(ItemFailed))
						close(seen.processing)
					}()
					continue
				}
			}
			select {
			case w.simultaneousLimit <- struct{}{}:
				// great
			case <-ctx.Done():
				err := ctx.Err()
				if w.ItemTimeoutError != nil {
					err = w.ItemTimeoutError(ctx, item, why, err)
				}
				return err
			}
			go func() {
				defer func() {
					<-w.simultaneousLimit // release slot
				}()
				err := w.ItemWork(ctx, item, why)
				now := time.Now()
				// This function exists so that the lock is held for a minimal time
				done := func() bool {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					if err != nil {
						// must set error before setting state to failed
						seen.error = errors.WithStack(err)
						seen.errorTime = now
						seen.state.Store(int32(ItemFailed))
						close(seen.processing)
					} else if ItemState(seen.state.Load()) == ItemProcessing {
						seen.state.Store(int32(ItemDone))
						close(seen.processing)
						return true
					}
					return false
				}()
				if done && w.ItemDone != nil {
					w.ItemDone(ctx, item, why)
				}
				if err != nil && w.ItemFailed != nil {
					// We call this after saving the error in seen.error because
					// we also call w.ItemFailed later on seen.error and we don't
					// want to double-wrap the error.
					_ = w.ItemFailed(ctx, item, why, err, true)
				}
			}()
		}
		if outstanding == nil {
			outstanding = make(map[Item]*itemProgress)
		}
		if w.ItemPending != nil {
			w.ItemPending(ctx, item, why)
		}
		outstanding[item] = seen
	}
	if len(outstanding) == 0 || overrideFinalError != nil {
		return overrideFinalError
	}
	for item, seen := range outstanding {
		select {
		case <-seen.processing:
			state := ItemState(seen.state.Load())
			if state == ItemFailed {
				err := func() error {
					seen.lock.Lock()
					defer seen.lock.Unlock()
					if ItemState(seen.state.Load()) == ItemFailed {
						return seen.error
					}
					return nil
				}()
				if err != nil {
					if w.ItemFailed != nil {
						err = w.ItemFailed(ctx, item, why, err, false)
					}
					return err
				}
			}
		case <-ctx.Done():
			err := ctx.Err()
			if w.ItemTimeoutError != nil {
				err = w.ItemTimeoutError(ctx, item, why, err)
			}
			return err
		}
	}
	return nil
}

// shouldRetry returns true when the last attempt to do the work
// resulted in an error and enough time has gone by that it's okay to
// try again.
func (w *Work[Item, Key]) shouldRetry(seen *itemProgress) (bool, error) {
	seen.lock.Lock()
	defer seen.lock.Unlock()
	if ItemState(seen.state.Load()) != ItemFailed {
		return false, nil
	}
	if time.Since(seen.errorTime) < w.ItemRetryDelay {
		return false, seen.error
	}
	seen.processing = make(chan struct{})
	seen.state.Store(int32(ItemProcessing))
	seen.errorTime = time.Time{}
	seen.error = nil
	return true, nil
}
