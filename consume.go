package events

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/muir/gwrap"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
	"github.com/singlestore-labs/once"
	"github.com/singlestore-labs/simultaneous"
)

var (
	debugAck            = os.Getenv("EVENTS_DEBUG_ACK") == "true"
	debugConsume        = os.Getenv("EVENTS_DEBUG_CONSUME") == "true"
	debugConsumeStartup = os.Getenv("EVENTS_DEBUG_START_CONSUME") == "true"
	debugBatching       = os.Getenv("EVENTS_DEBUG_BATCHING") == "true"
)

type eventLimiterType struct{}

type limit = simultaneous.Limit[eventLimiterType]

var backoffPolicy = backoff.Exponential(
	backoff.WithMinInterval(time.Second),
	backoff.WithMaxInterval(time.Second*30),
	backoff.WithJitterFactor(0.05),
)

var deadLetterBackoffPolicy = backoff.Exponential(
	backoff.WithMinInterval(time.Second),
	backoff.WithMaxInterval(time.Minute*30),
	backoff.WithJitterFactor(0.05),
)

// StartConsumingOrPanic is a wapper around StartConsuming that returns only after the consumers
// have started. If StartConsuming returns error, it panics.
func (lib *Library[ID, TX, DB]) StartConsumingOrPanic(ctx context.Context) (stopped chan struct{}) {
	started, stopped, err := lib.StartConsuming(ctx)
	if err != nil {
		panic(errors.Alert(err))
	}
	lib.tracer.Logf("[events] waiting for event consuming to start")
	<-started
	return stopped
}

var throttle = errors.Throttle{Scope: "events.Consume", Threshold: 300}

// StartConsuming should be called only after all Consume* requests have have been made and
// Configure has been called.
//
// It returns two channels: one is closed when the consumers have started
// The other is closed when all of the consumers have stopped.
// Consumers will finish after the passed context is canceled.
//
// StartConsuming synchronously creates the broadcast consumer group if there are
// any broadcast consumers. All the other work it does, like creating groups that don't
// exist and establishing the reader connections happens asynchronously.
func (lib *Library[ID, TX, DB]) StartConsuming(ctx context.Context) (started chan struct{}, stopped chan struct{}, err error) {
	err = lib.start("consume Kafka messages")
	if err != nil {
		return nil, nil, err
	}
	for _, group := range lib.readers {
		for topic := range group.topics {
			if err := lib.validateTopic(topic); err != nil {
				return nil, nil, err
			}
		}
	}
	for topic := range lib.broadcast.topics {
		if err := lib.validateTopic(topic); err != nil {
			return nil, nil, err
		}
	}

	if lib.hasTxConsumers && !lib.HasDB() {
		return nil, nil, errors.Alertf("attempt to consume exactly-once in an event library w/o a database connection")
	}
	limiter := simultaneous.New[eventLimiterType](maximumParallelConsumption).SetForeverMessaging(
		limiterStuckMessageAfter,
		func() {
			_ = throttle.Alertf("All event handlers are stuck, waiting (%s) for a runner", limiterStuckMessageAfter)
			lib.tracer.Logf("[events] All event handlers are stuck due to reaching the simultaneous limit")
		},
		func() {
			lib.tracer.Logf("[events] Event handlers are no longer stuck")
		},
	)
	// allDone tracks closing down the many threads that are involved in consuming topics
	var allDone sync.WaitGroup
	// allStarted tracks getting the consumers ready
	var allStarted sync.WaitGroup
	if debugConsumeStartup {
		lib.tracer.Logf("[events] Debug: consume startwait +%d for readers", len(lib.readers))
	}
	allStarted.Add(len(lib.readers))
	allDone.Add(len(lib.readers))
	if debugConsumeStartup {
		lib.tracer.Logf("[events] Debug: allDone +%d (readers)", len(lib.readers))
	}
	if len(lib.broadcast.topics) > 0 {
		allDone.Add(1)
		if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: consume startwait +1 for broadcast")
			lib.tracer.Logf("[events] Debug: allDone +1 broadcast")
		}
		allStarted.Add(1) // for the group startup
		allStarted.Add(1) // for receiving the first broadcast message
		err := lib.consumeBroadcast(ctx, &allStarted, &allDone)
		if err != nil {
			return nil, nil, err
		}
	}
	for consumerGroup, group := range lib.readers {
		go lib.startConsumingGroup(ctx, consumerGroup, group, limiter, false, &allStarted, &allDone, false, nil, nil, nil)
	}
	doneChan := make(chan struct{})
	startChan := make(chan struct{})
	go func() {
		allDone.Wait()
		if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: allDone for consumers is done")
		}
		close(doneChan)
	}()
	go func() {
		allStarted.Wait()
		lib.tracer.Logf("[events] consumers started")
		close(startChan)
	}()
	return startChan, doneChan, nil
}

// startConsumingGroup reads messages and calls handlers for a single consumer group
//
// consume() exits on idleness because sometimes readers hang. startConsumingGroup calls consume() over and over.
//
// Readers get re-created
func (lib *Library[ID, TX, DB]) startConsumingGroup(ctx context.Context, consumerGroup consumerGroupName, group *group, limiter *limit, isBroadcast bool, allStarted *sync.WaitGroup, allDone *sync.WaitGroup, isDeadLetter bool, reader *kafka.Reader, readerConfig *kafka.ReaderConfig, unlock func() error) {
	defer func() {
		if unlock != nil {
			_ = unlock()
		}
	}()
	if debugConsumeStartup {
		lib.tracer.Logf("[events] Debug: consume startwait 0 waiting for %s %s", consumerGroup, group.Describe())
	}
	// startedSideEffects should be called only once the consumer is started
	startedSideEffects := once.New(func() {
		if isBroadcast {
			allDone.Add(1)
			if debugConsumeStartup {
				lib.tracer.Logf("[events] Debug: allDone +1 for broadcast heartbeat %s %s", consumerGroup, group.Describe())
			}
			go lib.sendBroadcastHeartbeat(ctx, allDone)
		} else if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: consume startwait -1 ... started() called for %s %s", consumerGroup, group.Describe())
		}
		allStarted.Done()
	})
	defer func() {
		startedSideEffects.Do()
		if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: allDone -1 for consumer %s %s", consumerGroup, group.Describe())
		}
		allDone.Done()
	}()
	// precreateTopicsForConsuming keeps trying until it succeeds or the context is cancelled.
	err := lib.precreateTopicsForConsuming(ctx, consumerGroup, generic.Keys(group.topics))
	if err != nil {
		return
	}
	for _, topicHandler := range group.topics {
		for name, handler := range topicHandler.handlers {
			handler.consumerGroup = consumerGroup
			topicHandler.handlers[name] = handler
		}
	}
	if !isDeadLetter {
		lib.startDeadLetterConsumers(ctx, consumerGroup, group, limiter, allStarted, allDone)
	}
	cgWithPrefix := lib.addPrefix(string(consumerGroup))
	if isBroadcast {
		cgWithPrefix = "broadcast"
	}
	for topic, topicHandlers := range group.topics {
		prefixedTopic := lib.addPrefix(topic)
		ConsumeCounts.WithLabelValues(prefixedTopic, cgWithPrefix).Add(0)
		for handlerName, handler := range topicHandlers.handlers {
			HandlerSuccessCounts.WithLabelValues(handlerName, prefixedTopic).Add(0)
			if handler.isDeadLetter {
				DeadLetterConsumeCounts.WithLabelValues(handlerName, lib.addPrefix(handler.baseTopic)).Add(0)
			} else {
				DeadLetterProduceCounts.WithLabelValues(handlerName, prefixedTopic).Inc()
			}
			HandlerPanicCounts.WithLabelValues(handlerName, prefixedTopic).Add(0)
			HandlerErrorCounts.WithLabelValues(handlerName, prefixedTopic).Add(0)
			if handler.requestedBatchSize > 0 {
				HandlerBatchQueued.WithLabelValues(handlerName).Set(0)
				HandlerBatchConcurrency.WithLabelValues(handlerName).Set(0)
			}
		}
	}
	var priorSuccess time.Time
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if reader == nil {
			if isBroadcast {
				consumerGroup, reader, readerConfig, err = lib.refreshBroadcastReader(ctx, consumerGroup, &unlock)
				if err != nil {
					if ctx.Err() == nil {
						err = errors.Alertf("cannot refresh broadcast reader: %w", err)
					}
					lib.tracer.Logf("[events] FATAL ERROR: %+v", err)
					return
				}
				if !priorSuccess.IsZero() {
					err := reader.SetOffsetAt(ctx, priorSuccess)
					if err != nil && ctx.Err() != nil {
						return
					}
					_ = lib.RecordError("reader set offset", errors.Errorf("could not set reader offset for (%s): %w", consumerGroup, err))
					reader = nil
					continue
				}
			} else {
				reader, _, readerConfig, err = lib.getReader(ctx, consumerGroup, lib.addPrefixes(generic.Keys(group.topics)), isBroadcast, false)
				if err != nil {
					// the only possible error is timeout with the context cancelled
					return
				}
			}
		}
		// set zero counters for metrics
		startedSideEffects.Do()
		if isBroadcast && priorSuccess.IsZero() {
			allStarted.Done()
		}
		if !lib.consume(ctx, consumerGroup, group, limiter, isBroadcast, allDone, allStarted, &priorSuccess, reader, readerConfig) {
			return
		}
		reader = nil
	}
}

// consume uses one reader and to fetch, process, and acknowledge messages.
//
// If message fetching times out, consume exits so that a new reader can be created since
// it seems that sometimes readers hang.
//
// Careful management of waitGroups and contexts means that consume() doesn't exit until all message
// handlers are done and if those message handlers completed processing of messages, consume waits for
// the processCommits go routine to finish too.
//
// Each message is processed in a separate go routine so that multiple messages can be consumed
// quickly. The number of active go routines is limited by a limiter that is shared across all
// consumers.
//
// When a handler is done with a message, it writes it to a channel that is read by the processCommits
// go routine. Messages are explicitly committed in order by partition.
func (lib *Library[ID, TX, DB]) consume(ctx context.Context, consumerGroup consumerGroupName, group *group, activeLimiter *limit, isBroadcast bool, allDone *sync.WaitGroup, allStarted *sync.WaitGroup, priorSuccess *time.Time, reader *kafka.Reader, readerConfig *kafka.ReaderConfig) bool {
	*priorSuccess = time.Now()
	cgWithPrefix := lib.addPrefix(string(consumerGroup))
	queueLimit := simultaneous.New[eventLimiterType](group.maxQueueLimit()).SetForeverMessaging(
		limiterStuckMessageAfter,
		func() {
			lib.tracer.Logf("[events] Queue depth for consumer group %s %s reached %s ago and processing is stuck",
				cgWithPrefix, group.Describe(), limiterStuckMessageAfter)
		},
		func() {
			lib.tracer.Logf("[events] Queue depth for consumer group %s %s is no longer stuck",
				cgWithPrefix, group.Describe())
		},
	)
	commitsSoftCtx, commitsCancel := context.WithCancel(ctx)
	var outstandingWork sync.WaitGroup
	if debugConsumeStartup {
		lib.tracer.Logf("[events] Debug: consume %s readerconfig topics %v", cgWithPrefix, readerConfig.GroupTopics)
	}
	defer func() {
		// outstandingWork.Wait() must precede commitsCancel. CommitsCancel stops
		// the processCommits task, but only once it has completed all pending work.
		outstandingWork.Wait()
		if debugConsume {
			lib.tracer.Logf("[events] Debug: outstanding work for %s %s is done", cgWithPrefix, group.Describe())
		}
		commitsCancel()
		err := reader.Close()
		if err != nil {
			_ = lib.RecordError("reader close error", errors.Errorf("could not close reader for consumerGroup (%s): %w", cgWithPrefix, err))
		}
	}()

	lib.tracer.Logf("[events] consumer started for consumerGroup %s for %s", cgWithPrefix, group.Describe())
	sequenceNumbers := make(map[int]int)
	// done is used to commit offsets for messages that have been processed
	done := make(chan *messageAndSequenceNumber, commitQueueDepth)
	// we pass allDone rather than outstandingWork because the commits
	// we want to signal the commits process to finish when there are no handler
	// threads and to do that, we cannot wait on the commits process itself
	// before signalling the commits process.
	if debugConsumeStartup {
		lib.tracer.Logf("[events] Debug: allDone +1 for process commits for %s %s", cgWithPrefix, group.Describe())
	}
	allDone.Add(1)
	go lib.processCommits(commitsSoftCtx, ctx, consumerGroup, cgWithPrefix, reader, done, allDone)
	for {
		select {
		case <-ctx.Done():
			lib.tracer.Logf("[events] done reading from consumer group %s for topics %v", cgWithPrefix, group.Describe())
			return false
		default:
		}
		shortCtx, shortCancel := context.WithTimeout(ctx, group.maxIdle)
		if debugConsume {
			lib.tracer.Logf("[events] Debug: consume begin fetch %s %s with timeout %s", cgWithPrefix, group.Describe(), group.maxIdle)
		}
		msg, err := reader.FetchMessage(shortCtx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				select {
				case <-ctx.Done():
					lib.tracer.Logf("[events] done listening for consumer group %s for topics %v", cgWithPrefix, group.Describe())
					shortCancel()
					return false
				default:
				}
				select {
				case <-shortCtx.Done():
					lib.tracer.Logf("[events] idle timeout for consumer group %s for topics %v", cgWithPrefix, group.Describe())
					shortCancel() // to satisfy lint
					return true
				default:
				}
			}
			_ = lib.RecordError("kafka fetch error", errors.Errorf("fetch from consumer group (%s) failed: %w", cgWithPrefix, err))
			shortCancel()
			return true
		}
		if debugConsume {
			lib.tracer.Logf("[events] Debug: received one %s message in consumer group %s: %s", msg.Topic, cgWithPrefix, string(msg.Key))
		}
		shortCancel()
		if isBroadcast {
			func() {
				lib.lastBroadcastLock.Lock()
				defer lib.lastBroadcastLock.Unlock()
				if msg.Time.After(lib.lastBroadcast) && msg.Time.Before(time.Now()) {
					lib.lastBroadcast = msg.Time
				}
			}()
		}
		sequenceNumber := sequenceNumbers[msg.Partition]
		if debugAck {
			lib.tracer.Logf("[events] Debug: ack sequence number assigned for %s %s %s is %d: %d", msg.Topic, string(msg.Key), cgWithPrefix, msg.Partition, sequenceNumber)
		}
		sequenceNumbers[msg.Partition]++
		ConsumeCounts.WithLabelValues(msg.Topic, cgWithPrefix).Inc()
		outstandingWork.Add(1)
		ConsumersWaitingForQueueConcurrencyDemand.WithLabelValues(cgWithPrefix).Add(1)
		ConsumersWaitingForQueueConcurrencyLimit.WithLabelValues(cgWithPrefix).Add(1)
		queuedLimit := queueLimit.Forever()
		ConsumersWaitingForQueueConcurrencyLimit.WithLabelValues(cgWithPrefix).Add(-1)
		TransmissionLatency.WithLabelValues(msg.Topic, cgWithPrefix).Observe(float64(time.Since(msg.Time)) / float64(time.Second))
		go lib.deliverOneMessage(ctx, msg, consumerGroup, cgWithPrefix, group, &outstandingWork, queuedLimit, sequenceNumber, done, activeLimiter)
	}
}

// deliverOnMessage is responsible for the deliver of one message. Batch delivery only happens
// when batch concurrency limits cause messages to back up. Batch delivery is optional. When
// requested by a handler, there is both a batch size limit and a concurrency limit. If there is
// no backup then there will be a batch of size one. As the concurrency limit is exceeded, messages
// are added to the handler's queue. When there becomes room within the concurrency limit, additional
// batches can get formed and delivered.
func (lib *Library[ID, TX, DB]) deliverOneMessage(
	ctx context.Context,
	msg kafka.Message,
	consumerGroup consumerGroupName,
	cgWithPrefix string,
	group *group,
	outstandingWork *sync.WaitGroup,
	queuedLimit simultaneous.Limited[eventLimiterType],
	sequenceNumber int,
	done chan *messageAndSequenceNumber,
	activeLimiter *limit,
) {
	defer outstandingWork.Done()
	defer queuedLimit.Done()
	defer ConsumersWaitingForQueueConcurrencyDemand.WithLabelValues(cgWithPrefix).Add(-1)
	masn := messageAndSequenceNumber{
		sequenceNumber: sequenceNumber,
		Message:        &msg,
	}
	handlers, ok := group.topics[lib.RemovePrefix(msg.Topic)]
	if ok {
		waiters := make(chan handlerSuccess, len(handlers.handlerNames))
		go func() {
			if debugConsume {
				lib.tracer.Logf("[events] Debug: consume will deliver message %s/%s in %s to %v", msg.Topic, string(msg.Key), cgWithPrefix, handlers.handlerNames)
			}
			for _, handlerName := range handlers.handlerNames {
				handler := handlers.handlers[handlerName]
				if handler.requestedBatchSize <= 0 {
					if debugBatching {
						lib.tracer.Logf("[events] Debug: delivering to handler %s without batching %s/%s in %s to %v", handlerName, msg.Topic, string(msg.Key), cgWithPrefix, handlers.handlerNames)
					}
					successes := []bool{false}
					lib.callHandler(ctx, activeLimiter, handler, []*kafka.Message{&msg}, successes)
					waiters <- handlerSuccess{
						handler: handler,
						success: successes[0],
					}
					continue
				}
				mad := messageAndDone{
					Message: &msg,
					waiter:  waiters,
				}
				queued := func() int {
					handler.batchLock.Lock()
					defer handler.batchLock.Unlock()
					handler.waitingBatch = append(handler.waitingBatch, mad)
					if handler.batchesRunning >= handler.batchParallelism {
						if debugBatching {
							lib.tracer.Logf("[events] Debug: queuing for handler %s batch %s/%s in %s to %v", handlerName, msg.Topic, string(msg.Key), cgWithPrefix, handlers.handlerNames)
						}
						return len(handler.waitingBatch)
					}
					handler.batchesRunning += 1
					if debugBatching {
						lib.tracer.Logf("[events] Debug: starting additional batch processes for %s, %d/%d %s/%s in %s to %v", handlerName, handler.batchesRunning, handler.batchParallelism, msg.Topic, string(msg.Key), cgWithPrefix, handlers.handlerNames)
					}
					HandlerBatchConcurrency.WithLabelValues(handler.name).Set(float64(handler.batchesRunning))
					go lib.formAndDeliverBatches(ctx, handler, activeLimiter)
					return len(handler.waitingBatch)
				}()
				HandlerBatchQueued.WithLabelValues(handler.name).Set(float64(queued))
			}
		}()
		// Consume success/failure for each handler. Do this synchronously. They may arrive
		// out-of-order.
		for range handlers.handlerNames {
			select {
			case hs := <-waiters:
				handler, success := hs.handler, hs.success
				if success {
					if debugConsume {
						lib.tracer.Logf("[events] Debug: success for %s / %s / %s", handler.name, msg.Topic, string(msg.Key))
					}
					continue
				}
				switch handler.onFailure {
				case eventmodels.OnFailureDiscard:
					if debugConsume {
						lib.tracer.Logf("[events] Debug: DISCARD for %s / %s / %s", handler.name, msg.Topic, string(msg.Key))
					}
				case eventmodels.OnFailureBlock:
					if debugConsume {
						lib.tracer.Logf("[events] Debug: BLOCK for %s / %s / %s", handler.name, msg.Topic, string(msg.Key))
					}
					return
				case eventmodels.OnFailureRetryLater, eventmodels.OnFailureSave:
					if debugConsume {
						lib.tracer.Logf("[events] Debug: DEAD LETTER for %s / %s / %s", handler.name, msg.Topic, string(msg.Key))
					}
					lib.produceToDeadLetter(ctx, handler.consumerGroup, handler.name, msg)
				default:
					lib.tracer.Logf("[events] unexpected onfailure value %d for handler %s", handler.onFailure, handler.name)
				}
			case <-ctx.Done():
				return
			}
		}
	} else if debugConsume {
		lib.tracer.Logf("[events] Debug: consume no handler for %s/%s in %s", msg.Topic, string(msg.Key), cgWithPrefix)
	}
	done <- &masn
	if debugAck {
		lib.tracer.Logf("[events] Debug: queued for ack %s/%s in %s", msg.Topic, string(msg.Key), cgWithPrefix)
	}
}

// formAndDeliverBatches runs as a go-routine, repeatedly grabbing a batch-worth
// of messages from the handler's queue and delivering them. When it runs out of work,
// it decreases the batches-running count and exits.
func (lib *Library[ID, TX, DB]) formAndDeliverBatches(
	ctx context.Context,
	handler *registeredHandler,
	activeLimiter *limit,
) {
	defer func() {
		handler.batchLock.Lock()
		defer handler.batchLock.Unlock()
		select {
		case <-ctx.Done():
		default:
			if len(handler.waitingBatch) > 0 {
				// this is a rare case, avoiding a race condition that could
				// cause message delivery delay
				if debugBatching {
					lib.tracer.Logf("[events] Debug: messages are waiting for %s, handing off to new handler", handler.name)
				}
				go lib.formAndDeliverBatches(ctx, handler, activeLimiter)
				return
			}
		}
		handler.batchesRunning -= 1
		if debugBatching {
			lib.tracer.Logf("[events] Debug: batch handler for %s complete, now %d/%d", handler.name, handler.batchesRunning, handler.batchParallelism)
		}
		HandlerBatchConcurrency.WithLabelValues(handler.name).Set(float64(handler.batchesRunning))
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		batch, remaining := func() ([]messageAndDone, int) {
			handler.batchLock.Lock()
			defer handler.batchLock.Unlock()
			batch := handler.waitingBatch
			if len(batch) > handler.requestedBatchSize {
				batch = batch[:handler.requestedBatchSize]
			}
			handler.waitingBatch = handler.waitingBatch[len(batch):]
			return batch, len(handler.waitingBatch)
		}()
		if debugBatching {
			lib.tracer.Logf("[events] Debug: formed batch of %d items for %s", len(batch), handler.name)
		}
		HandlerBatchQueued.WithLabelValues(handler.name).Set(float64(remaining))
		if len(batch) == 0 {
			return
		}
		func() {
			successes := make([]bool, len(batch))
			defer func() {
				for i, mad := range batch {
					mad.waiter <- handlerSuccess{
						handler: handler,
						success: successes[i],
					}
				}
			}()
			msgs := generic.TransformSlice(batch, func(mad messageAndDone) *kafka.Message {
				return mad.Message
			})
			lib.callHandler(ctx, activeLimiter, handler, msgs, successes)
		}()
	}
}

// callHandler invokes the handler. The returned bools indicates if there was a fatal delivery failure on
// a per-message basis.
func (lib *Library[ID, TX, DB]) callHandler(ctx context.Context, activeLimiter *limit, handler *registeredHandler, msgs []*kafka.Message, successes []bool) {
	start := time.Now()
	defer func() {
		for _, msg := range msgs {
			HandlerLatency.WithLabelValues(handler.name, msg.Topic).Observe(float64(time.Since(start)) / float64(time.Second))
		}
	}()
	hipi := noteHandlerStart(msgs[0].Topic, handler.name)
	defer noteHandlerEnd(hipi)
	HandlerWaitingForQueueConcurrencyDemand.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(len(msgs)))
	HandlerWaitingForQueueConcurrencyLimit.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(len(msgs)))
	queueLimit := handler.limit.Forever()
	HandlerWaitingForQueueConcurrencyLimit.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(-len(msgs)))
	defer HandlerWaitingForQueueConcurrencyDemand.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(-len(msgs)))
	defer queueLimit.Done()
	originalCtx := ctx
	if handler.timeout != 0 && handler.onFailure != eventmodels.OnFailureBlock {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, handler.timeout)
		defer cancel()
	}
	// The lifetime of the backoff controller, b, and the cancel of the context that
	// it uses need to align perfectly so that it functions correctly and does not
	// hold onto resources beyond its scope. The context for this, backoffCtx,
	// is not appropriate for any other use.
	var b backoff.Controller
	backoffCtx := ctx
	var backoffCancel func()
	backoffCtx, backoffCancel = context.WithCancel(backoffCtx)
	defer backoffCancel()
	if handler.isDeadLetter {
		b = deadLetterBackoffPolicy.Start(backoffCtx)
	} else {
		b = backoffPolicy.Start(backoffCtx)
	}

	outstanding := make([]int, len(msgs))
	for i := range msgs {
		outstanding[i] = i
	}

	for len(outstanding) > 0 {
		if debugConsume {
			for _, msg := range msgs {
				lib.tracer.Logf("[events] Debug: consume invoking %s for message in %s/%s/%s", handler.name, msg.Topic, handler.consumerGroup, string(msg.Key))
			}
		}
		var paniced bool
		errs := func() (errs []error) {
			errs = make([]error, len(outstanding))
			defer func() {
				if r := recover(); r != nil {
					var err error
					if e, ok := r.(error); ok {
						err = e
					} else {
						err = errors.Errorf("%s", fmt.Sprint(r))
					}
					err = errors.Errorf("panic in handler (%s) for topic (%s) for message (%s) in consumer group (%s): %w", handler.name, msgs[0].Topic, string(msgs[0].Key), handler.consumerGroup, err)
					paniced = true
					for i := range errs {
						errs[i] = err
					}
				}
			}()
			HandlerWaitingForActiveConcurrencyDemand.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(len(outstanding)))
			HandlerWaitingForActiveConcurrencyLimit.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(len(outstanding)))
			limitActive := activeLimiter.Forever()
			HandlerWaitingForActiveConcurrencyLimit.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(-len(outstanding)))
			defer HandlerWaitingForActiveConcurrencyDemand.WithLabelValues(handler.name, msgs[0].Topic).Add(float64(-len(outstanding)))
			defer limitActive.Done()

			pending := make([]*kafka.Message, len(outstanding))
			for i, idx := range outstanding {
				pending[i] = msgs[idx]
			}
			if handler.isDeadLetter {
				DeadLetterConsumeCounts.WithLabelValues(handler.name, lib.addPrefix(handler.baseTopic)).Inc()
			}
			return handler.handler.Handle(originalCtx, handler, pending)
		}()
		if paniced {
			HandlerPanicCounts.WithLabelValues(handler.name, msgs[0].Topic).Inc()
		}
		var backoffDone bool
		var backoffContinue bool
		stillOutstanding := make([]int, 0, len(outstanding))
		for i, err := range errs {
			idx := outstanding[i]
			msg := msgs[idx]
			if err == nil {
				HandlerSuccessCounts.WithLabelValues(handler.name, msg.Topic).Inc()
				successes[idx] = true
				continue
			}
			handling := eventmodels.GetErrorHandling(err)
			_ = lib.RecordErrorNoWait("consumer handler failure", errors.Errorf("handler (%s) for topic (%s) for message (%s) in consumer group (%s) failed: %w",
				handler.name, msg.Topic, string(msg.Key), handler.consumerGroup, err))
			HandlerErrorCounts.WithLabelValues(handler.name, msg.Topic).Inc()
			if !handler.retry {
				lib.tracer.Logf("[events] dropping message %s for handler %s in topic %s for consumer group %s due to error", string(msg.Key), handler.name, msg.Topic, handler.consumerGroup)
				successes[idx] = false
				continue
			}
			switch handling {
			case eventmodels.DoNotRetry:
				lib.tracer.Logf("[events] dropping message %s for handler %s in topic %s for consumer group %s due to error encoding", string(msg.Key), handler.name, msg.Topic, handler.consumerGroup)
				successes[idx] = false
				continue
			case eventmodels.IgnoreError:
				lib.tracer.Logf("[events] marking as consumed message %s for handler %s in topic %s for consumer group %s despite error", string(msg.Key), handler.name, msg.Topic, handler.consumerGroup)
				successes[idx] = false
				continue
			}
			if !backoffDone {
				if debugConsume {
					lib.tracer.Logf("[events] Debug: about to call backoff %s for %s %s", handler.name, msg.Topic, string(msg.Key))
				}
				backoffDone = true
				backoffContinue = backoff.Continue(b)
			}
			if !backoffContinue {
				lib.tracer.Logf("[events] dropping message %s for handler %s in topic %s for consumer group %s due to success timeout", string(msg.Key), handler.name, msg.Topic, handler.consumerGroup)
				successes[idx] = false
				continue
			}
			if errors.Is(err, eventmodels.ErrDecode) {
				lib.tracer.Logf("[events] could not decode message %s for handler %s in topic %s, will not retry. Message body is '%s'", string(msg.Key), handler.name, msg.Topic, handler.consumerGroup, string(msg.Value))
				successes[idx] = false
				continue
			}
			if debugConsume {
				lib.tracer.Logf("[events] Debug: will retry %s for %s %s", handler.name, msg.Topic, string(msg.Key))
			}
			stillOutstanding = append(stillOutstanding, outstanding[i])
		}
		outstanding = stillOutstanding
	}
}

type messageAndSequenceNumber struct {
	*kafka.Message
	sequenceNumber int // by partition
	gwrap.PQItemEmbed[int]
}

type queueAndSequence struct {
	sequenceNumber int
	queue          *gwrap.PriorityQueue[int, *messageAndSequenceNumber]
	ready          bool
}

type messageTimestamp struct {
	prefixedTopic string
	ts            time.Time
}

// processCommits tells Kafka that it can advance the consumer offsets. It figures
// out when it is safe to advance the offsets by tracking the last contiguous message
// processed on a per-partiton basis.
//
// # The sequence numbers exist only in-memory and start at zero for each partition
//
// hardCtx is used to signal forced shutdown
//
// softCtx is used to request a shutdown, eventually, and will be ignored while there is work that can be done immediately.
// softCtx is used during the switchover from one reader to another due to idleness.
func (lib *Library[ID, TX, DB]) processCommits(softCtx context.Context, hardCtx context.Context, consumerGroup consumerGroupName, cgWithPrefix string, reader *kafka.Reader, done chan *messageAndSequenceNumber, allDone *sync.WaitGroup) {
	defer func() {
		if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: allDone -1 for process commits for %s", consumerGroup)
		}
		allDone.Done()
	}()
	queues := make(map[int]*queueAndSequence)
	readyPartitions := make(map[int]struct{})
	newMessage := func(msg *messageAndSequenceNumber) bool {
		queue, ok := queues[msg.Partition]
		if !ok {
			queue = &queueAndSequence{
				queue: gwrap.NewPriorityQueue[int, *messageAndSequenceNumber](),
			}
			queues[msg.Partition] = queue
		}
		queue.queue.Enqueue(msg, msg.sequenceNumber)
		if msg.sequenceNumber == queue.sequenceNumber {
			readyPartitions[msg.Partition] = struct{}{}
			if debugAck {
				lib.tracer.Logf("[events] Debug: ack message %s %s %s is ready to ack and is the next message %d in its partition %d", string(msg.Key), msg.Topic, consumerGroup, msg.sequenceNumber, msg.Partition)
			}
			queue.ready = true
			return true
		}
		if debugAck {
			lib.tracer.Logf("[events] Debug: message %s %s %s is ready to ack and is NOT the next message in its partition %d: %d vs %d and %v", string(msg.Key), msg.Topic, consumerGroup, msg.Partition, msg.sequenceNumber, queue.sequenceNumber, queue.ready)
		}
		return queue.ready
	}

	for {
		// No messages in the queues that are ready to be committed
		select {
		case <-hardCtx.Done():
			lib.tracer.Logf("[events] consume done processing commits")
			return
		default:
			select {
			case msg := <-done:
				if !newMessage(msg) {
					if debugAck {
						lib.tracer.Logf("[events] Debug: ack there are NO messages ready to ack for %s", consumerGroup)
					}
					continue
				}
				if debugAck {
					lib.tracer.Logf("[events] Debug: ack there are messages ready to ack for %s", consumerGroup)
				}
			default:
				select {
				case <-hardCtx.Done():
					lib.tracer.Logf("[events] consumer group %s done processing commits", consumerGroup)
					return
				case <-softCtx.Done():
					lib.tracer.Logf("[events] consumer group %s done processing commits", consumerGroup)
					return
				case msg := <-done:
					if !newMessage(msg) {
						if debugAck {
							lib.tracer.Logf("[events] Debug: ack (inner) there are NO messages ready to ack for %s", consumerGroup)
						}
						continue
					}
					if debugAck {
						lib.tracer.Logf("[events] Debug: ack (inner) there are messages ready to ack for %s", consumerGroup)
					}
				}
			}
		}

		// There are now messages that can be committed, read any more that are
		// in the queue. Ignore softCtx here since we have work to do.
		if debugAck {
			lib.tracer.Logf("[events] Debug: ack seeing if there are more messages queued to ack for %s", consumerGroup)
		}
	MoreQueued:
		for {
			select {
			case <-hardCtx.Done():
				lib.tracer.Logf("[events] consumer group %s done processing commits", consumerGroup)
				return
			case msg := <-done:
				_ = newMessage(msg)
			default:
				break MoreQueued
			}
		}
		if debugAck {
			lib.tracer.Logf("[events] Debug: ack moving on to ack for %s", consumerGroup)
		}

		// There are no more in the queue and we have at least one that can
		// be committed so let's commit it.
		messages := make([]kafka.Message, 0, len(readyPartitions))
		sendTimestamps := make([]messageTimestamp, 0, len(readyPartitions)*8)
		for partition := range readyPartitions {
			queue := queues[partition]
			var lastMessage *kafka.Message
			for queue.queue.Len() > 0 {
				msg := queue.queue.Dequeue()
				if msg.sequenceNumber == queue.sequenceNumber {
					queue.sequenceNumber++
					if debugAck {
						lib.tracer.Logf("[events] Debug: ack msg %s %s is next %d for its partition %d in group %s", string(msg.Key), msg.Topic, msg.sequenceNumber, msg.Partition, consumerGroup)
					}
					lastMessage = msg.Message
					sendTimestamps = append(sendTimestamps, messageTimestamp{
						prefixedTopic: msg.Topic,
						ts:            msg.Time,
					})
				} else {
					// not in-order, put it back
					if debugAck {
						lib.tracer.Logf("[events] Debug: ack msg %s %s is NOT next %d for its partition %d in group %s", string(msg.Key), msg.Topic, msg.sequenceNumber, msg.Partition, consumerGroup)
					}
					queue.queue.Enqueue(msg, msg.sequenceNumber)
					break
				}
			}
			if debugAck {
				lib.tracer.Logf("[events] Debug: ack consume commit message %s in %s", string(lastMessage.Key), lastMessage.Topic)
			}
			messages = append(messages, *lastMessage)
			queue.ready = false
		}
		if debugAck {
			lib.tracer.Logf("[events] Debug: ack committing %d messages for %s", len(messages), consumerGroup)
		}
		if stopProcessing := func() bool {
			// The lifetime of the backoff controller, b, and the cancel of the context that
			// it uses need to align perfectly so that it functions correctly and does not
			// hold onto resources beyond its scope. The context for this, backoffCtx,
			// is not appropriate for any other use. This function exists just so that the
			// defer will invoke at the right moment.
			backoffCtx, backoffCancel := context.WithCancel(hardCtx)
			defer backoffCancel() // required for cleanup
			b := backoffPolicy.Start(backoffCtx)
			for {
				// either hardCtx or backoffCtx could be used here
				err := reader.CommitMessages(hardCtx, messages...)
				if err != nil {
					if !backoff.Continue(b) {
						_ = lib.RecordErrorNoWait("kafka commit error", errors.Errorf("consumer commit of (%d) messages failed: %w", len(messages), err))
						lib.tracer.Logf("[events] consume done processing commits, dropping some")
						return true
					}
					if errors.Is(err, kafka.NotCoordinatorForGroup) {
						// NotCoordinatorForGroup is an expected error, just log a warning
						lib.tracer.Logf("[events] warning: commit failed due to not coordinator for group; will retry (%d messages): %v", len(messages), err)
					} else {
						_ = lib.RecordErrorNoWait("kafka commit error", errors.Errorf("consumer commit of (%d) messages failed: %w", len(messages), err))
					}
					continue
				}
				return false
			}
		}(); stopProcessing {
			return
		}
		clear(readyPartitions)
		for _, ts := range sendTimestamps {
			AckLatency.WithLabelValues(ts.prefixedTopic, cgWithPrefix).Observe(float64(time.Since(ts.ts)) / float64(time.Second))
		}
	}
}
