package eventmodels

import (
	"context"
	"os"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
)

var debugDelivery = os.Getenv("EVENTS_DEBUG_DELIVERY") == "true"

type HandlerInfo interface {
	Name() string
	BaseTopic() string
	ConsumerGroup() string
	IsDeadLetter() bool
}

type LibraryInterface interface {
	RemovePrefix(string) string
	TracerProvider(context.Context) Tracer
	TracerConfig() TracerConfig
}

type LibraryInterfaceTx[ID AbstractID[ID], TX AbstractTX] interface {
	LibraryInterface
	DB() AbstractDB[ID, TX]
}

// handler exists to implement HandlerInterface
//
// The callback will not be simultaneously called for by the same Library for the
// same topic, for the same consumer group.
type handler[E any] struct {
	sharedHandler
	lib      LibraryInterface
	callback func(context.Context, []Event[E]) error
}

type sharedHandler struct {
	topic string
}

func (h *sharedHandler) GetTopic() string { return h.topic }

var _ HandlerInterface = &handler[any]{}

type SharedHandlerInterface interface {
	GetTopic() string
	// Handle synchronously invokes the handler function for a message.
	Handle(ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message) []error
	Batch() bool
}

type HandlerInterface interface {
	SharedHandlerInterface
	SetLibrary(LibraryInterface)
	private() // marker function preventing other implmentations
}

func (h *handler[E]) Handle(ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message) []error {
	return handle[E](ctx, handlerInfo, messages, h.lib, h.callback)
}

func handle[E any](ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message, lib LibraryInterface, callback func(context.Context, []Event[E]) error) (errs []error) {
	errs = make([]error, len(messages))
	metas := make([]Event[E], 0, len(messages))
	callbackCtx := make([]context.Context, len(messages))
	for i, message := range messages {
		handlerCtx, done := lib.TracerConfig().Handle(ctx, handlerInfo.IsDeadLetter(), handlerInfo.Name(), message)
		defer done()
		callbackCtx[i] = handlerCtx
		meta, err := decode[E](message, handlerInfo.ConsumerGroup(), lib, handlerInfo.Name())
		if err != nil {
			lib.TracerProvider(handlerCtx)("[events] could not decode (%s) event (%s / %s) for handler (%s / %s): %+v", message.Topic, meta.ID, string(message.Key), handlerInfo.ConsumerGroup(), handlerInfo.Name(), err)
			errs[i] = err
			continue
		}
		meta.BaseTopic = handlerInfo.BaseTopic()
		meta.idx = i
		metas = append(metas, meta)
		lib.TracerProvider(handlerCtx)("[events] delivered (%s) event (%s / %s) to handler (%s / %s)", message.Topic, meta.ID, string(message.Key), handlerInfo.ConsumerGroup(), handlerInfo.Name())
	}
	if len(metas) == 0 {
		return
	}
	callbackErr := callback(callbackCtx[metas[0].idx], metas)
	for _, meta := range metas {
		if callbackErr != nil {
			errs[meta.idx] = errors.Errorf("consume error handling (%s) event (%s / %s) in with handler (%s / %s): %w", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name(), callbackErr)
		} else if debugDelivery {
			lib.TracerProvider(callbackCtx[meta.idx])("[events] success delivering (%s) event (%s / %s) to handler (%s / %s)", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name())
		}
	}
	return
}

func (h *handler[E]) private()                        {} //nolint:unused
func (h *handler[E]) SetLibrary(lib LibraryInterface) { h.lib = lib }
func (h *handler[E]) Batch() bool                     { return false }

type BatchHandlerInterface interface {
	HandlerInterface
	privateBatch()
}

type batchHandler[E any] struct {
	sharedHandler
	lib      LibraryInterface
	callback func(context.Context, []Event[E]) error
}

func (h *batchHandler[E]) Batch() bool                     { return true }
func (h *batchHandler[E]) SetLibrary(lib LibraryInterface) { h.lib = lib }
func (h *batchHandler[E]) privateBatch()                   {} //nolint:unused
func (h *batchHandler[E]) private()                        {} //nolint:unused

var _ BatchHandlerInterface = &batchHandler[any]{}

func (h *batchHandler[E]) Handle(ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message) []error {
	return handle[E](ctx, handlerInfo, messages, h.lib, h.callback)
}

// handlerTx exists to implement the HandlerTxInterface. It is similar to handler but includes a transaction
// in the callback.
type handlerTx[E any, ID AbstractID[ID], TX AbstractTX] struct {
	sharedHandler
	lib      LibraryInterfaceTx[ID, TX]
	callback func(context.Context, TX, []Event[E]) error
}

var _ HandlerTxInterface[StringEventID, AbstractTX] = &handlerTx[any, StringEventID, AbstractTX]{}

type HandlerTxInterface[ID AbstractID[ID], TX AbstractTX] interface {
	SharedHandlerInterface
	SetLibrary(LibraryInterfaceTx[ID, TX])
	privateTx()
}

func (h *handlerTx[E, ID, TX]) privateTx()                                {} //nolint:unused
func (h *handlerTx[E, ID, TX]) Batch() bool                               { return false }
func (h *handlerTx[E, ID, TX]) SetLibrary(lib LibraryInterfaceTx[ID, TX]) { h.lib = lib }

// Handle provides at-most once semantics for transactionally handling a messsage. At-least-once
// semantics must be provided the message producer so that together we achieve exactly-once
// behavior.
func (h *handlerTx[E, ID, TX]) Handle(ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message) []error {
	return handleTx[E, ID, TX](ctx, handlerInfo, messages, h.lib, h.callback)
}

func handleTx[E any, ID AbstractID[ID], TX AbstractTX](ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message, lib LibraryInterfaceTx[ID, TX], callback func(context.Context, TX, []Event[E]) error) (errs []error) {
	errs = make([]error, len(messages))
	metas := make([]Event[E], 0, len(messages))
	callbackCtx := make([]context.Context, len(messages))
	for i, message := range messages {
		handlerCtx, done := lib.TracerConfig().Handle(ctx, handlerInfo.IsDeadLetter(), handlerInfo.Name(), message)
		defer done()
		callbackCtx[i] = handlerCtx
		meta, err := decode[E](message, handlerInfo.ConsumerGroup(), lib, handlerInfo.Name())
		if err != nil {
			lib.TracerProvider(handlerCtx)("[events] could not decode (%s) tx event (%s / %s) for handler (%s / %s): %+v", message.Topic, meta.ID, string(message.Key), handlerInfo.ConsumerGroup(), handlerInfo.Name(), err)
			errs[i] = err
			continue
		}
		meta.BaseTopic = handlerInfo.BaseTopic()
		meta.idx = i
		metas = append(metas, meta)
	}
	alreadyDone := make([]bool, len(metas))

	err := lib.DB().Transact(ctx, func(tx TX) error {
		todo := make([]Event[E], 0, len(metas))
		for i, meta := range metas {
			err := lib.DB().MarkEventProcessed(callbackCtx[meta.idx], tx, handlerInfo.BaseTopic(), meta.Source, meta.ID, handlerInfo.Name())
			if err != nil {
				if errors.Is(err, ErrAlreadyProcessed) {
					alreadyDone[i] = true
					lib.TracerProvider(callbackCtx[meta.idx])("[events] tx (%s) event (%s / %s) for handler (%s / %s) already delivered", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name())
					continue
				}
				err = errors.WithStack(err)
				lib.TracerProvider(callbackCtx[meta.idx])("[events] db failure delivering tx (%s) event (%s / %s) to handler (%s / %s): %+v", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name(), err)
				return err
			}
			lib.TracerProvider(callbackCtx[meta.idx])("[events] delivering tx (%s) event (%s / %s) to handler (%s / %s)", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name())
			todo = append(todo, meta)
		}
		if len(todo) == 0 {
			return nil
		}
		return callback(ctx, tx, todo)
	})
	for i, meta := range metas {
		if alreadyDone[i] {
			continue
		}
		if err != nil {
			errs[meta.idx] = errors.Errorf("consume error handling (%s) tx event (%s / %s) in with handler (%s / %s): %w", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name(), err)
			lib.TracerProvider(callbackCtx[meta.idx])("[events] tx failure delivering tx (%s) event (%s / %s) to handler (%s / %s): %+v", meta.Topic, meta.ID, meta.Key, handlerInfo.ConsumerGroup(), handlerInfo.Name(), err)
		}
	}
	return
}

type batchHandlerTx[E any, ID AbstractID[ID], TX AbstractTX] struct {
	sharedHandler
	lib      LibraryInterfaceTx[ID, TX]
	callback func(ctx context.Context, conn TX, ms []Event[E]) error
}

type BatchHandlerTxInterface[ID AbstractID[ID], TX AbstractTX] interface {
	HandlerTxInterface[ID, TX]
	privateBatch()
}

func (h *batchHandlerTx[E, ID, TX]) privateBatch()                             {} //nolint:unused
func (h *batchHandlerTx[E, ID, TX]) private()                                  {} //nolint:unused
func (h *batchHandlerTx[E, ID, TX]) privateTx()                                {} //nolint:unused
func (h *batchHandlerTx[E, ID, TX]) Batch() bool                               { return true }
func (h *batchHandlerTx[E, ID, TX]) SetLibrary(lib LibraryInterfaceTx[ID, TX]) { h.lib = lib }

var _ BatchHandlerTxInterface[StringEventID, AbstractTX] = &batchHandlerTx[any, StringEventID, AbstractTX]{}

func (h *batchHandlerTx[E, ID, TX]) Handle(ctx context.Context, handlerInfo HandlerInfo, messages []*kafka.Message) []error {
	return handleTx[E, ID, TX](ctx, handlerInfo, messages, h.lib, h.callback)
}
