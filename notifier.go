package events

import (
	"context"
	"os"
	"sync"

	"github.com/memsql/errors"
	"github.com/memsql/keyeddistributor"
	"github.com/sharnoff/eventdistributor"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/simultaneous"
)

// CanConsumeBroadcast is implemented by [Library]
type CanConsumeBroadcast interface {
	ConsumeBroadcast(handlerName string, handler eventmodels.HandlerInterface, opts ...HandlerOpt)
	TracerProvider(context.Context) eventmodels.Tracer
	InstanceID() int32
}

var _ CanConsumeBroadcast = &Library[eventmodels.StringEventID, eventmodels.AbstractTX, eventmodels.AbstractDB[eventmodels.StringEventID, eventmodels.AbstractTX]]{}

var debugNotify = os.Getenv("EVENTS_DEBUG_NOTIFY") == "true"

type NotifierOpt func(*notifierConfig)

type notifierConfig struct {
	handlerOpts []HandlerOpt
	asyncLimit  int
}

func WrapHandlerOpt(handlerOpts ...HandlerOpt) NotifierOpt {
	return func(c *notifierConfig) {
		c.handlerOpts = append(c.handlerOpts, handlerOpts...)
	}
}

// Async delivers events asynchronously with a limit on the number of
// outstanding events. A limit of -1 indicates no limit. A limit of zero
// indicates synchronous delivery. A positive number allows that many
// events to be outstanding at once but no more.
func Async(limit int) func(*notifierConfig) {
	return func(c *notifierConfig) {
		c.asyncLimit = limit
	}
}

type notifierLimiterType struct{}

var (
	registrationLock sync.Mutex
	registrationTodo []func(lib CanConsumeBroadcast)
)

func processRegistrationTodo(lib CanConsumeBroadcast) {
	registrationLock.Lock()
	defer registrationLock.Unlock()
	for _, f := range registrationTodo {
		f(lib)
	}
}

func processOpts(lib CanConsumeBroadcast, handlerName string, opts []NotifierOpt) (notifierConfig, *simultaneous.Limit[notifierLimiterType]) {
	config := notifierConfig{}
	for _, opt := range opts {
		opt(&config)
	}
	tmpLimit := config.asyncLimit // used to create a limiter even if we're not limiting
	if tmpLimit < 1 {
		tmpLimit = 1
	}
	limiter := simultaneous.New[notifierLimiterType](tmpLimit).SetForeverMessaging(
		limiterStuckMessageAfter,
		func(ctx context.Context) {
			_ = throttle.Alertf("distributor (%s) is stuck, waiting (%s) for a runner", handlerName, limiterStuckMessageAfter)
			lib.TracerProvider(ctx)("[events] distributor %s is stuck due to reaching the simultaneous limit", handlerName)
		},
		func(ctx context.Context) {
			lib.TracerProvider(ctx)("[events] distributor %s is no longer stuck", handlerName)
		},
	)
	return config, limiter
}

// RegisterFiltered pre-registers a filtered broadcast event distributor that will
// will receive events from any [Library] that is [Configure]()ed after RegisterFiltered
// is called. See [keyeddistributor] for behavior details.
//
// keyeddistributor: https://pkg.go.dev/github.com/memsql/keyeddistributor
func RegisterFiltered[E any, K comparable](handlerName string, topic eventmodels.BoundTopic[E], key func(eventmodels.Event[E]) K, opts ...NotifierOpt) Filtered[K, E] {
	f := make(Filtered[K, E])
	registrationLock.Lock()
	defer registrationLock.Unlock()
	registrationTodo = append(registrationTodo, func(lib CanConsumeBroadcast) {
		config, limiter := processOpts(lib, handlerName, opts)
		distributor := keyeddistributor.New(key)
		lib.ConsumeBroadcast(handlerName, topic.Handler(func(ctx context.Context, event eventmodels.Event[E]) error {
			if debugNotify {
				lib.TracerProvider(ctx)("[events] DEBUG: filtered distributor to %s, starting to handle %s / %s / %s",
					handlerName, event.Topic, event.Key, event.ID)
				defer lib.TracerProvider(ctx)("[events] DEBUG: filtered distributor to %s, done handling %s / %s / %s",
					handlerName, event.Topic, event.Key, event.ID)
			}
			switch config.asyncLimit {
			case -1:
				_ = distributor.Submit(event)
				return nil
			case 0:
			default:
				limited := limiter.Forever(ctx)
				defer limited.Done()
			}
			select {
			case <-distributor.Submit(event):
				return nil
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			}
		}),
			config.handlerOpts...)
		f[lib.InstanceID()] = distributor
	})
	return f
}

type Filtered[K comparable, E any] map[int32]*keyeddistributor.Distributor[eventmodels.Event[E], K]

func (f Filtered[K, E]) Subscribe(lib CanConsumeBroadcast, key K) *keyeddistributor.Reader[eventmodels.Event[E]] {
	d, ok := f[lib.InstanceID()]
	if !ok {
		// This panic shouldn't happen in practice because RegisterUnfiltered should be called during init
		// and the event library will have been registred with the distributor unless it doesn't support
		// broadcast because it doesn't have a db.
		panic("attempt to subscribe to event distributor not defined or supported by event library for topic")
	}
	return d.Subscribe(key)
}

// RegisterUnfiltered pre-registers an unfiltered broadcast event distributor that will
// will receive events from any [Library] that is [Configure]()ed after RegisterUnfiltered
// is called. See [eventdistributor] for behavior details.
//
// eventdistributor: https://pkg.go.dev/github.com/sharnoff/eventdistributor
func RegisterUnfiltered[E any](handlerName string, topic eventmodels.BoundTopic[E], opts ...NotifierOpt) Unfiltered[E] {
	u := make(Unfiltered[E])
	registrationLock.Lock()
	defer registrationLock.Unlock()
	registrationTodo = append(registrationTodo, func(lib CanConsumeBroadcast) {
		config, limiter := processOpts(lib, handlerName, opts)
		distributor := eventdistributor.New[eventmodels.Event[E]]()
		lib.ConsumeBroadcast(handlerName, topic.Handler(func(ctx context.Context, event eventmodels.Event[E]) error {
			if debugNotify {
				lib.TracerProvider(ctx)("[events] DEBUG: unfiltered distributor to %s, starting to handle %s / %s / %s",
					handlerName, event.Topic, event.Key, event.ID)
				defer lib.TracerProvider(ctx)("[events] DEBUG: unfiltered distributor to %s, done handling %s / %s / %s",
					handlerName, event.Topic, event.Key, event.ID)
			}
			switch config.asyncLimit {
			case -1:
				_ = distributor.Submit(event)
				return nil
			case 0:
			default:
				limited := limiter.Forever(ctx)
				defer limited.Done()
			}
			select {
			case <-distributor.Submit(event):
				return nil
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			}
		}),
			config.handlerOpts...)
		u[lib.InstanceID()] = distributor
	})
	return u
}

type Unfiltered[E any] map[int32]*eventdistributor.Distributor[eventmodels.Event[E]]

func (u Unfiltered[E]) Subscribe(lib CanConsumeBroadcast) eventdistributor.Reader[eventmodels.Event[E]] {
	d, ok := u[lib.InstanceID()]
	if !ok {
		// This panic shouldn't happen in practice because RegisterUnfiltered should be called during init
		// and the event library will have been registred with the distributor unless it doesn't support
		// broadcast because it doesn't have a db.
		panic("attempt to subscribe to event distributor not defined or supported by event library for topic")
	}
	return d.Subscribe()
}
