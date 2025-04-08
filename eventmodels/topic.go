package eventmodels

import (
	"context"
)

type BoundTopic[E any] string

type BoundTopicTx[E any, ID AbstractID[ID], TX AbstractTX, DB AbstractDB[ID, TX]] struct {
	BoundTopic[E]
}

// BindTopic does not validate topics. cpevents.BindTopic does, use it instead.
func BindTopic[E any](name string) BoundTopic[E] {
	return (BoundTopic[E])(name)
}

func BindTopicTx[E any, ID AbstractID[ID], TX AbstractTX, DB AbstractDB[ID, TX]](name string) BoundTopicTx[E, ID, TX, DB] {
	return BoundTopicTx[E, ID, TX, DB]{
		BoundTopic: BindTopic[E](name),
	}
}

func (t BoundTopic[E]) Topic() string { return string(t) }
func (t BoundTopic[E]) Event(key string, model E) SimpleEvent {
	return newEvent(string(t), key).addPayload(model)
}

func (t BoundTopic[E]) Handler(callback func(context.Context, Event[E]) error) HandlerInterface {
	return &handler[E]{
		sharedHandler: sharedHandler{
			topic: string(t),
		},
		callback: func(ctx context.Context, events []Event[E]) error {
			for _, event := range events {
				err := callback(ctx, event)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func (t BoundTopic[E]) BatchHandler(callback func(context.Context, []Event[E]) error) BatchHandlerInterface {
	return &batchHandler[E]{
		sharedHandler: sharedHandler{
			topic: string(t),
		},
		callback: callback,
	}
}

func (t BoundTopicTx[E, ID, TX, DB]) HandlerTx(callback func(context.Context, TX, Event[E]) error) HandlerTxInterface[ID, TX] {
	return &handlerTx[E, ID, TX]{
		sharedHandler: sharedHandler{
			topic: string(t.BoundTopic),
		},
		callback: func(ctx context.Context, tx TX, events []Event[E]) error {
			for _, event := range events {
				err := callback(ctx, tx, event)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func (t BoundTopicTx[E, ID, TX, DB]) BatchHandlerTx(callback func(context.Context, TX, []Event[E]) error) BatchHandlerTxInterface[ID, TX] {
	return &batchHandlerTx[E, ID, TX]{
		sharedHandler: sharedHandler{
			topic: string(t.BoundTopic),
		},
		callback: callback,
	}
}
