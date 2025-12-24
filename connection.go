package events

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/errors"
	"github.com/muir/gwrap"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/singlestore-labs/generic"
	"github.com/singlestore-labs/simultaneous"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/internal"
	"github.com/singlestore-labs/events/internal/multi"
	"github.com/singlestore-labs/events/internal/pwork"
)

var _ eventmodels.Producer[eventmodels.StringEventID, eventmodels.AbstractTX] = &Library[eventmodels.StringEventID, eventmodels.AbstractTX, eventmodels.AbstractDB[eventmodels.StringEventID, eventmodels.AbstractTX]]{}

// These could all become configurable if needed
const (
	maxBytes                            = 10e6 // 10MB
	readCommitInterval                  = time.Second
	commitQueueDepth                    = 1000
	limiterStuckMessageAfter            = time.Minute
	broadcastParallelConsumption        = 3
	errorSleep                          = time.Second
	baseBroadcastHeartbeat              = time.Second * 10
	broadcastHeartbeatRandom            = 0.25
	broadcastNotCoordinatorErrorRetries = 50
	broadcastStartupMaxWait             = 15 * time.Minute // only checked on group allocation failure
	nonBroadcastReaderIdleTimeout       = 5 * time.Minute
	broadcastReaderIdleTimeout          = baseBroadcastHeartbeat * 3 // This cannot be < 1s: Kafka takes a while to deliver events after reader startup or generation change
	maxConsumerGroupNameLength          = 55                         // actual limit is 249 characters, but we need to subtract this from the topic name length limit so we use less
	dialTimeout                         = time.Minute * 2
	deleteTimeout                       = time.Minute * 4
	describeTimeout                     = time.Minute * 4
	produceFromTableBuffer              = 512
	readerStartupSleep                  = time.Millisecond * 100
	readerStartupReport                 = time.Minute
	defaultBroadcastConsumerBaseName    = "broadcastConsumer"
	defaultLockFreeBroadcastBase        = "broadcastLFConsumer"
	transactionalWriteTimeout           = 30 * time.Second
	transactionalReadTimeout            = 30 * time.Second
	transactionalBatchTimeout           = 100 * time.Millisecond
	transactionalOperationTimeout       = 45 * time.Second

	// maximumParallelConsumption limits the number of handlers that can run at once.
	// The larger this number, the more memory and CPU devoted to event processing. The
	// number is arbitrary. The number is arbitrary but must be at least 1.
	maximumParallelConsumption = 20

	// maximumHandlerOutstanding limits the number of outstanding messages that can be handed
	// to a single handler. This limit counts both messages that the handler is currently
	// processing, and messages that have failed delivery (handler returned error) and are queued to retry.
	// The larger the number, the more memory that can be used by this handler. This can be a larger
	// number than maximumParallelConsumption since this counts messages that are waiting to retry
	// and ones that are running. The bigger the number, the less that OnFailureBlock will stop work
	// in the consumer group that includes the handler. The number is arbitrary but must be at
	// least 1.
	maximumHandlerOutstanding = 30

	// maximumDeadLetterOutstanding is maximumHandlerOutstanding but specifically for handlers working
	// on dead letter topics. The number is arbitrary but must be at least 1.
	maximumDeadLetterOutstanding = 10

	defaultBatchSize        = 30
	defaultBatchConcurrency = 3
)

var (
	maxTopicNameLength      = 249 - maxConsumerGroupNameLength - len(deadLetterTopicPostfix) - 1 // 249 is actual limit
	LegalTopicNames         = regexp.MustCompile(fmt.Sprintf(`^[-._a-zA-Z0-9]{1,%d}$`, maxTopicNameLength))
	legalConsumerGroupNames = regexp.MustCompile(fmt.Sprintf(`^[-._a-zA-Z0-9]{1,%d}$`, maxConsumerGroupNameLength))
)

var instanceCount atomic.Int32

type Library[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX, DB eventmodels.AbstractDB[ID, TX]] struct {
	db               eventmodels.AbstractDB[ID, TX]
	produceFromTable chan []ID
	LibraryNoDB
}

type LibraryNoDB struct {
	// --- Size cap subsystem fields (producer-only, decoupled from topic creation) ---
	// Global broker caps
	sizeCapBrokerLock       sync.Mutex // protects changes to sizeCapBrokerState
	sizeCapBrokerState      atomic.Int32
	sizeCapBrokerLoadCtx    *multi.Context
	sizeCapBrokerReady      chan struct{}
	sizeCapDefaultAssumed   int64                                    // anything smaller than this can be sent before knowing actual limits
	sizeCapBrokerMessageMax atomic.Int64                             // message.max.bytes (0 unknown)
	sizeCapSocketRequestMax atomic.Int64                             // socket.request.max.bytes (rarely limiting; 0 unknown)
	sizeCapWork             pwork.Work[string, string]               // un-prefixed in APIs
	sizeCapTopicLimits      gwrap.SyncMap[string, sizeCapTopicLimit] // un-prefixed topic

	// Per-topic limits map (separate from creatingTopic). Keys are topic names.

	hasDB                     atomic.Bool
	tracer                    eventmodels.Tracer
	brokers                   []string
	writer                    *kafka.Writer
	readers                   map[consumerGroupName]*group
	broadcast                 *group
	startTime                 time.Time
	ready                     atomic.Int32
	topicConfig               map[string]kafka.TopicConfig  // un-prefixed
	topicsWork                pwork.Work[string, topicsWhy] // un-prefixed in APIs
	topicListingStarted       sync.Once
	topicsHaveBeenListed      chan struct{}
	mustRegisterTopics        bool
	hasTxConsumers            bool
	clientID                  string
	mechanism                 sasl.Mechanism
	lastBroadcastLock         sync.Mutex
	lastBroadcast             time.Time
	broadcastHeartbeat        time.Duration
	heartbeatRandomness       float64
	tlsConfig                 *tls.Config
	producerRunning           atomic.Int32
	broadcastConsumerBaseName string
	broadcastConsumerMaxLock  uint32
	broadcastConsumerSkipLock bool
	doEnhance                 bool
	ProduceSyncCount          atomic.Uint64 // used and exported for testing only
	instanceID                int32
	lazyProduce               bool
	skipNotifier              bool
	prefix                    string // prefixes all topics and consumer groups

	// lock must be held when....
	//
	// "ready" is isNotConfigured or isConfigured:
	// - writing to most of the fields of Library (calls to Configure)
	// - reading/writing "readers" (calls to Consume*)
	// - reading/writing topicConfig (can happen at any time)
	// - writing "ready"
	//
	// "ready" is isRunning:
	// - reading/writing topicConfig (can happen at any time)
	//
	// lock is not needed when "ready" is isRunning:
	// - reading most fields of Library
	// - reading "readers" (used while consuming)
	lock sync.Mutex // taken when adjusting consumers

	broadcastConsumerGroupName gwrap.AtomicValue[consumerGroupName]
}

type ConsumerGroupName interface {
	String() string
	name() consumerGroupName
}

const (
	// values for lib.ready
	isNotConfigured = 0
	isConfigured    = 1
	isRunning       = 2
)

type group struct {
	topics  map[string]*topicHandlers // unprefixed topic -> handlers
	maxIdle time.Duration             // reset reader when idle for this long
}

type topicHandlers struct {
	handlerNames []string                      // so that iteration is deterministic
	handlers     map[string]*registeredHandler // handlerName -> handler
}

type registeredHandler struct {
	retry              bool
	timeout            time.Duration
	handler            canHandle
	name               string
	onFailure          eventmodels.OnFailure
	isDeadLetter       bool
	baseTopic          string
	consumerGroup      consumerGroupName
	queueLimit         int
	limit              *limit
	requestedBatchSize int
	batchParallelism   int
	waitingBatch       []messageAndDone
	batchLock          sync.Mutex
	batchesRunning     int
}

type messageAndDone struct {
	*kafka.Message
	waiter chan handlerSuccess
}

type handlerSuccess struct {
	handler *registeredHandler
	success bool
}

type HandlerOpt func(*registeredHandler, *LibraryNoDB)

type canHandle interface {
	GetTopic() string // unprefixed
	Handle(ctx context.Context, handlerInfo eventmodels.HandlerInfo, message []*kafka.Message) []error
	Batch() bool
}

// New creates an event Library. It can be used from init() since
// it requires no arguments. The library is not ready to use.
//
// After New(), Configure() must be called.
//
// After New(), ConsumeExactlyOnce(), ConsumeIdempotent(), and ConsumeBroadcast()
// may be called to register event consumers.
//
// After the call to Configure() and all Consume*() calls, consumers can be
// started with StartConsuming(); background production of orphaned messages
// may be started with CatchUpProduce(); messages may be produced with
// Produce().
//
// Configure() and Consume*() may not be used once consumption or production
// has started.
func New[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX, DB eventmodels.AbstractDB[ID, TX]]() *Library[ID, TX, DB] {
	lib := Library[ID, TX, DB]{
		produceFromTable: make(chan []ID, produceFromTableBuffer),
		LibraryNoDB: LibraryNoDB{
			startTime: time.Now(),
			readers:   make(map[consumerGroupName]*group),
			broadcast: &group{
				topics:  make(map[string]*topicHandlers),
				maxIdle: broadcastReaderIdleTimeout,
			},
			topicConfig:              make(map[string]kafka.TopicConfig),
			clientID:                 uuid.New().String(),
			broadcastHeartbeat:       baseBroadcastHeartbeat,
			heartbeatRandomness:      broadcastHeartbeatRandom,
			broadcastConsumerMaxLock: 200,
			doEnhance:                true,
			instanceID:               instanceCount.Add(1),
			topicsHaveBeenListed:     make(chan struct{}),
			sizeCapBrokerReady:       make(chan struct{}),
			sizeCapDefaultAssumed:    1_000_000,
			sizeCapBrokerLoadCtx:     multi.New(),
		},
	}
	lib.configureTopicsPrework()
	lib.configureSizeCapPrework()
	return &lib
}

// For testing
func (lib *Library[ID, TX, DB]) SetEnhanceDB(enhance bool) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.doEnhance = enhance
}

// SetBroadcastConsumerBaseName sets the base consumer group name used for
// creating broadcast consumers.  A dash (-) and number will be appended.
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SetBroadcastConsumerBaseName(name string) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.broadcastConsumerBaseName = name
}

// SetBroadcastConsumerMaxLocks sets a limit on the maximum number used to form
// consumer group IDs for creating broadcast consumers. Defaults to 200. Keep this
// reasonably small so that Kafka doesn't end up thrashing if broadcast consumers
// cannot be allocated.
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SetBroadcastConsumerMaxLocks(max uint32) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.broadcastConsumerMaxLock = max
}

// SetPrefix sets a string prefix used for all topics and all consumer groups. Keep this
// short since max topic length is :
//
//	249 - 55 (maxConsumerGroupLength) - len("-dead-letter") - len(prefix) - 1
//
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SetPrefix(prefix string) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.prefix = prefix
}

// DoNotLockBroadcastConsumerNumbers must be used before starting the library. If called,
// no database locks will be taken to reserve broadcast consumer numbers. There is a
// tradeoff: this saves a database connection that would otherwise sit around holding
// a lock, but it makes the allocation and refreshing of broadcast consumers much more
// expensive in terms of interactions with Kafka.
//
// It is safe to use this in combination with other broadcast consumers that do take
// locks: they'll use different namespace prefixes unless you mess that up by
// using SetBroadcastConsumerBaseName() with the same name for both lock-free and locked
// instances.
//
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) DoNotLockBroadcastConsumerNumbers() {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.broadcastConsumerSkipLock = true
}

// SetLazyTxProduce controls what to do if a transaction produces an event but
// the [Library] doesn't have a running producer. Defaults to false. If true, the
// event will be left in the database for some other [Library] to pick up sometime
// in the future.
//
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SetLazyTxProduce(lazy bool) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.lazyProduce = lazy
}

// SkipNotifierSupport turns off support for runtime subscription to broadcast topics
// via RegisterFiltered and RegisterUnfiltered. This is mostly used in testing the events library.
//
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) SkipNotifierSupport() {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.skipNotifier = true
}

// SetSizeCapLowerLimit overrides the default lower limit on sizes: any message under
// this size can be sent before actual limits are known.
func (lib *Library[ID, TX, DB]) SetSizeCapLowerLimit(sizeCapDefaultAssumed int64) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.sizeCapDefaultAssumed = sizeCapDefaultAssumed
}

// Configure sets up the Library so that it has the configuration it needs to run.
// The database connection is optional. Without it, certain features will always error:
//
//	CatchUpProduce requires a database
//	StartConsuming requires a database if ConsumeExactlyOnce has been called
//
// The conn parameter may be nil, in which case CatchUpProduce() and ProduceFromTable() will error.
//
// It is required that Configure be called exactly once before any consumers are started or
// messages are produced.
// Calling this after consumers have started or messages are being produced will panic.
func (lib *Library[ID, TX, DB]) Configure(conn DB, tracer eventmodels.Tracer, mustRegisterTopics bool, saslMechanism sasl.Mechanism, tlsConfig *TLSConfig, brokers []string) {
	if !lib.skipNotifier {
		processRegistrationTodo(lib) // before taking lock to avoid deadlock
	}
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.ready.Store(isConfigured)
	if !internal.IsNil(conn) {
		lib.db = conn
		lib.hasDB.Store(true)
		if augmenter, ok := any(conn).(eventmodels.CanAugment[ID, TX]); ok && lib.doEnhance {
			augmenter.AugmentWithProducer(lib)
		}
	}
	lib.brokers = brokers
	if saslMechanism != nil {
		lib.mechanism = saslMechanism
		if tracer != nil {
			tracer.Logf("[events] configured with SASL %T", saslMechanism)
		}
	}
	if tlsConfig != nil {
		lib.tlsConfig = tlsConfig.Config
		if tracer != nil {
			tracer.Logf("[events] configured with TLS override")
		}
	}
	lib.tracer = tracer
	lib.mustRegisterTopics = mustRegisterTopics
}

func (lib *Library[ID, TX, DB]) start(str string, args ...any) error {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	switch lib.ready.Load() {
	case isNotConfigured:
		return errors.Alertf("attempt to %s when library has not been configured", fmt.Sprintf(str, args...))
	case isRunning:
		return nil
	}
	if len(lib.brokers) == 0 || lib.brokers[0] == "" {
		return errors.Errorf("no brokers configured")
	}
	lib.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: lib.brokers,
		Dialer:  lib.dialer(),
	})
	lib.ready.Store(isRunning)
	return nil
}

func (lib *Library[ID, TX, DB]) ConfigureBroadcastHeartbeat(dur time.Duration) {
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event library that is already processing")
	lib.broadcastHeartbeat = dur
}

// BroadcastConsumerLastLatency returns the time since the sending of the last
// broadcast event that was received. It also returns the configured heartbeat interval.
// As long as the latency is less than twice the configured interval, the broadcast
// consumer can be considred to be working.
func (lib *Library[ID, TX, DB]) BroadcastConsumerLastLatency() (lastGap time.Duration, configuredHeartbeat time.Duration) {
	lib.lastBroadcastLock.Lock()
	defer lib.lastBroadcastLock.Unlock()
	return time.Since(lib.lastBroadcast), lib.broadcastHeartbeat
}

// ConsumeExactlyOnce delivers messages transactionally (with an open transaction) and
// they are delivered exactly once. If the handler returns error, the transaction
// rolls back and the message can be re-delivered. Messages will only be delivered to one
// instance of the consuming server(s).
//
// A consumerGroupName can and should be reused, but only if all consuming servers register
// all handler instances for that consumerGroupName (messages will only be delivered successfully
// once per consumer group)
func (lib *Library[ID, TX, DB]) ConsumeExactlyOnce(consumerGroup ConsumerGroupName, onFailure eventmodels.OnFailure, handlerName string, handler eventmodels.HandlerTxInterface[ID, TX], opts ...HandlerOpt) {
	handler.SetLibrary(libraryInterface[ID, TX, DB]{lib})
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event consumer in library that is already processing")
	lib.getTopicHandler(consumerGroup, handler.GetTopic()).addHandler(handlerName, onFailure, &lib.LibraryNoDB, handler, opts)
	lib.hasTxConsumers = true
}

// ConsumeIdempotent should be used to consume events at least once where the handler
// takes care of any issues that arise from consuming the event more than once. The
// event will be presented to handlers until a handler has consumed it without error.
//
// Events handled with ConsumeIdempotent will generally be consumed on only one instance
// of the server that has registered to consume such events. If there is no registered
// consumer, events will be held until there is a consumer. Duplicate events are possible.
//
// A consumerGroupName can and should be reused, but only if all consuming servers register
// all handler instances for that consumerGroupName. If multiple handlers use the same consumerGroupName
// and one of them returns error, then the message can be re-delivered to the handlers that did not
// return error.
func (lib *Library[ID, TX, DB]) ConsumeIdempotent(consumerGroup ConsumerGroupName, onFailure eventmodels.OnFailure, handlerName string, handler eventmodels.HandlerInterface, opts ...HandlerOpt) {
	handler.SetLibrary(libraryInterface[ID, TX, DB]{lib})
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event consumer in library that is already processing")
	lib.getTopicHandler(consumerGroup, handler.GetTopic()).addHandler(handlerName, onFailure, &lib.LibraryNoDB, handler, opts)
}

// ConsumeBroadcast should be used to consume events where every instance of the consumer
// will receive every event. Only events timestamped after the start time of the consuming
// server will be delivered. Duplicate events are possible.
//
// When broadcast handlers return error, the message will be dropped.
// By default broadcast handlers are not retried and the handler timeout is 30 seconds.
func (lib *Library[ID, TX, DB]) ConsumeBroadcast(handlerName string, handler eventmodels.HandlerInterface, opts ...HandlerOpt) {
	handler.SetLibrary(libraryInterface[ID, TX, DB]{lib})
	lib.lock.Lock()
	defer lib.lock.Unlock()
	lib.mustNotBeRunning("attempt configure event consumer in library that is already processing")
	handlers, ok := lib.broadcast.topics[handler.GetTopic()]
	if !ok {
		handlers = &topicHandlers{
			handlers: make(map[string]*registeredHandler),
		}
		lib.broadcast.topics[handler.GetTopic()] = handlers
	}
	opts = append([]HandlerOpt{
		WithRetrying(true),
		WithTimeout(time.Second * 30),
	}, opts...)
	handlers.addHandler(handlerName, eventmodels.OnFailureDiscard, &lib.LibraryNoDB, handler, opts)
}

// WithTimeout limits the duration of retries of delivery attempts on a per-message basis.
// When the limit is exceeded, delivery is considered to have failed.
//
// When delivery has failed, failure handling becomes key:
//
// OnFailureRetryLater & OnFailureSave: the event will be sent to
// the dead letter topic.
//
// OnFailureDiscard: the event will be ack'ed without processing.
//
// OnFailureBlock: no messages will get
// acknowledged in the consumer group (since they must be ack'ed
// in-order) and eventually processing of the consumer group will stall.
func WithTimeout(d time.Duration) HandlerOpt {
	return func(r *registeredHandler, _ *LibraryNoDB) {
		r.timeout = d
	}
}

// WithRetrying controls message delivery.
// Normally, multiple attempts are made to deliver messages.
//
// WithRetrying(false) means only one attempt will be made.
//
// In connection with OnFailureRetryLater, OnFailureSave, the
// message will be immediately sent to corresponding dead letter
// topic. In connection with OnFailureDiscard, it will be dropped
// after just one delivery attempt.
//
// In combination with OnFailureBlock, no messages will get
// acknowledged in the consumer group (since they must be ack'ed
// in-order) and eventually processing of the consumer group will stall.
func WithRetrying(retry bool) HandlerOpt {
	return func(r *registeredHandler, _ *LibraryNoDB) {
		r.retry = retry
	}
}

// WithBaseTopic is applied automatically when processing dead letter topics
func WithBaseTopic(topic string) HandlerOpt {
	return func(r *registeredHandler, _ *LibraryNoDB) {
		r.baseTopic = topic
	}
}

// WithQueueDepthLimit places a maximum number of outstanding messages for this
// handler to be working on. This counts both active handlers and ones that are
// waiting for backoff. If WithBatchDelivery is also true, then the limit also
// acts as a maximum size for the batch.
func WithQueueDepthLimit(n int) HandlerOpt {
	return func(r *registeredHandler, lib *LibraryNoDB) {
		r.queueLimit = n
		r.limit = simultaneous.New[eventLimiterType](n).SetForeverMessaging(
			limiterStuckMessageAfter,
			func() {
				lib.tracer.Logf("[events] Handler %s in consumer group %s for topic %s has been waiting for more than %s for a chance to run and is stuck",
					r.name, r.consumerGroup, r.handler.GetTopic(), limiterStuckMessageAfter)
			},
			func() {
				lib.tracer.Logf("[events] Handler %s in consumer group %s for topic %s is no longer stuck",
					r.name, r.consumerGroup, r.handler.GetTopic())
			},
		)
	}
}

// WithBatchSize specifies the maximum number of messages to deliver at once. This
// should be used only with batch consumers as there is no advantage of batching with
// a non-batch consumer. If used with a non-batch consumer, the default WithConcurrency
// is 1.
func WithBatch(size int) HandlerOpt {
	return func(r *registeredHandler, lib *LibraryNoDB) {
		r.requestedBatchSize = size
	}
}

// WithConcurrency specifies the maximum number of instances of the handler
// that can be running at the same time. Setting this to 1 means that
// delivery of messages is single-threaded. If used with a non-batch consumer,
// the default WithBatch is 1.
func WithConcurrency(parallelism int) HandlerOpt {
	return func(r *registeredHandler, lib *LibraryNoDB) {
		r.batchParallelism = parallelism
	}
}

// IsDeadLetterHandler can be used when registering a handler for dead letter topics.
// Normally this is not needed as dead letter handlers are created automatically if
// the consumer uses OnFailureRetryLater. Using IsDeadLetterHandler only makes sense
// for creating custom handlers to deal with OnFailureSave events. Use DeadLetterTopic
// to form the dead letter topic name.
//
// Dead letter handlers record different metrics and use different simultaneous
// runner limits. OnFailureBlock is appropriate with dead letter handler because
// where would you save a dead letter from a dead letter consumer?
func IsDeadLetterHandler(isDeadLetter bool) HandlerOpt {
	return func(r *registeredHandler, _ *LibraryNoDB) {
		r.isDeadLetter = isDeadLetter
	}
}

func (topicHandler *topicHandlers) addHandler(handlerName string, onFailure eventmodels.OnFailure, lib *LibraryNoDB, handler canHandle, opts []HandlerOpt) {
	if _, ok := topicHandler.handlers[handlerName]; ok {
		panic(errors.Alertf("attempt to register duplicate handlers (topic:%s name:%s) in event library", handler.GetTopic(), handlerName))
	}
	r := registeredHandler{
		handler:            handler,
		retry:              true,
		timeout:            time.Minute * 5,
		name:               handlerName,
		onFailure:          onFailure,
		baseTopic:          handler.GetTopic(),
		requestedBatchSize: 0, // any non-zero size causes single-threaded delivery
		// consumerGroup is set later
	}
	if handler.Batch() {
		r.batchParallelism = defaultBatchConcurrency
		r.requestedBatchSize = defaultBatchSize
	}
	WithQueueDepthLimit(maximumHandlerOutstanding)(&r, lib)
	for _, opt := range opts {
		opt(&r, lib)
	}
	if r.batchParallelism != 0 && r.requestedBatchSize == 0 {
		r.requestedBatchSize = 1
	}
	if r.requestedBatchSize != 0 && r.batchParallelism == 0 {
		r.batchParallelism = 1
	}
	topicHandler.handlerNames = append(topicHandler.handlerNames, handlerName)
	topicHandler.handlers[handlerName] = &r
}

func (lib *Library[ID, TX, DB]) getTopicHandler(consumerGroupName ConsumerGroupName, topic string) *topicHandlers {
	groupReader, ok := lib.readers[consumerGroupName.name()]
	if !ok {
		groupReader = &group{
			topics:  make(map[string]*topicHandlers),
			maxIdle: nonBroadcastReaderIdleTimeout,
		}
		lib.readers[consumerGroupName.name()] = groupReader
	}
	topicHandler, ok := groupReader.topics[topic]
	if !ok {
		topicHandler = &topicHandlers{
			handlers: make(map[string]*registeredHandler),
		}
		groupReader.topics[topic] = topicHandler
	}
	return topicHandler
}

func (h *registeredHandler) Name() string          { return h.name }
func (h *registeredHandler) BaseTopic() string     { return h.baseTopic }
func (h *registeredHandler) ConsumerGroup() string { return h.consumerGroup.String() }

func (lib *LibraryNoDB) RecordErrorNoWait(category string, err error) error {
	ErrorCounts.WithLabelValues(category).Inc()
	if lib.tracer != nil {
		lib.tracer.Logf("[events] %s error: %+v", category, err)
	} else {
		log.Printf("[events] %s error: %+v", category, err)
	}
	return errors.Alertf("events error %s: %w", category, err)
}

func (lib *LibraryNoDB) RecordError(category string, err error) error {
	err = lib.RecordErrorNoWait(category, err)
	time.Sleep(errorSleep)
	return err
}

// IsConfigured reports if the library exists and has been configured
func (lib *Library[ID, TX, DB]) IsConfigured() bool { return lib.ready.Load() >= isConfigured }

func (lib *Library[ID, TX, DB]) DB() eventmodels.AbstractDB[ID, TX] { return lib.db }
func (lib *Library[ID, TX, DB]) Tracer() eventmodels.Tracer         { return lib.tracer }

// getController returns a client talking to the controlling broker. The
// controller is needed for certain requests, like creating a topic
func (lib *LibraryNoDB) getController(ctx context.Context) (_ *kafka.Client, err error) {
	var c *kafka.Client
	err = lib.findABroker(ctx, func(conn *kafka.Conn) error {
		controller, err := conn.Controller()
		if err != nil {
			return errors.Errorf("event library get controller from kafka connection: %w", err)
		}
		ips, err := net.LookupIP(controller.Host)
		if err != nil {
			return errors.Errorf("event library lookup IP of controller (%s): %w", controller.Host, err)
		}
		if len(ips) == 0 {
			return errors.Errorf("event library lookup IP of controller (%s) got no addresses", controller.Host)
		}
		c = &kafka.Client{
			Addr: &net.TCPAddr{
				IP:   ips[0],
				Port: controller.Port,
			},
			Transport: lib.transport(),
		}
		return nil
	})
	return c, err
}

func (lib *LibraryNoDB) findABroker(ctx context.Context, f func(*kafka.Conn) error) (err error) {
	dialer := lib.dialer()
	var tried int
	for _, i := range rand.Perm(len(lib.brokers)) {
		tried++
		broker := lib.brokers[i]
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			lib.tracer.Logf("[events] could not connect to broker %d (of %d) %s: %v", i+1, len(lib.brokers), broker, err)
			if tried == len(lib.brokers) {
				// last broker, give up
				return errors.Errorf("event library dial kafka broker (%s): %w", broker, err)
			}
			continue
		}
		defer func() {
			e := conn.Close()
			if err == nil && e != nil {
				err = errors.Errorf("event library close dialer (%s): %w", broker, e)
			}
		}()
		return f(conn)
	}
	return errors.Errorf("unexpected condition")
}

func (lib *LibraryNoDB) getBrokers(ctx context.Context) ([]kafka.Broker, error) {
	var brokers []kafka.Broker
	err := lib.findABroker(ctx, func(conn *kafka.Conn) error {
		var err error
		brokers, err = conn.Brokers()
		return err
	})
	return brokers, err
}

func (lib *LibraryNoDB) getABrokerID(ctx context.Context) (string, error) {
	brokers, err := lib.getBrokers(ctx)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if len(brokers) == 0 {
		return "", errors.Errorf("get brokers request returned no brokers")
	}
	broker := brokers[rand.Intn(len(brokers))]
	return strconv.Itoa(broker.ID), nil
}

// getConsumerGroupCoordinator returns a client talking to the control group's
// coordinator.
func (lib *Library[ID, TX, DB]) getConsumerGroupCoordinator(ctx context.Context, consumerGroup consumerGroupName) (*kafka.Client, error) {
	controller, err := lib.getController(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := controller.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Addr:    controller.Addr,
		Key:     lib.addPrefix(consumerGroup.String()),
		KeyType: kafka.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		return nil, errors.Errorf("find coordinator for consumer group (%s): %w", consumerGroup, err)
	}
	if resp.Error != nil {
		return nil, errors.Errorf("find coordinator for consumer group (%s): %w", consumerGroup, resp.Error)
	}
	if resp.Coordinator.Host == "" {
		// Err, what's going on here? Can we really assume that the consumer group doesn't exist?
		return nil, errors.Errorf("empty coordinator when looking up consumer group (%s): %w", consumerGroup, kafka.GroupCoordinatorNotAvailable)
	}
	ips, err := net.LookupIP(resp.Coordinator.Host)
	if err != nil {
		return nil, errors.Errorf("lookup IP of consumer group coordinator (%s): %w", resp.Coordinator.Host, err)
	}
	if len(ips) == 0 {
		return nil, errors.Errorf("lookup IP of consumer group coordinator (%s) got no addresses", resp.Coordinator.Host)
	}
	return &kafka.Client{
		Addr: &net.TCPAddr{
			IP:   ips[0],
			Port: resp.Coordinator.Port,
		},
		Transport: lib.transport(),
	}, nil
}

func (g *group) Describe() string {
	return strings.Join(
		generic.TransformSlice(generic.Keys(g.topics), func(k string) string {
			return k + ": " + strings.Join(generic.Keys(g.topics[k].handlers), ", ")
		}), "; ")
}

func (g *group) maxQueueLimit() int {
	var max int
	for _, topicHandlers := range g.topics {
		for _, handler := range topicHandlers.handlers {
			if handler.queueLimit > max {
				max = handler.queueLimit
			}
		}
	}
	return max
}

func (lib *Library[ID, TX, DB]) mustNotBeRunning(message string) {
	if lib.ready.Load() == isRunning {
		panic(errors.Alertf("%s", message))
	}
}

func (lib *Library[ID, TX, DB]) InstanceID() int32 {
	return lib.instanceID
}

func (lib *Library[ID, TX, DB]) HasDB() bool {
	return lib.hasDB.Load()
}

type consumerGroupName string

func NewConsumerGroup(name string) ConsumerGroupName {
	if !legalConsumerGroupNames.MatchString(name) {
		panic(errors.Alertf("invalid consumer group name '%s'", name))
	}
	return consumerGroupName(name)
}

func (n consumerGroupName) String() string          { return string(n) }
func (n consumerGroupName) name() consumerGroupName { return n }

func (lib *LibraryNoDB) validateTopic(unprefixedTopic string) error {
	if len(unprefixedTopic)+len(lib.prefix) > maxTopicNameLength {
		return errors.Errorf("topic name (%s%s) is too long", lib.prefix, unprefixedTopic)
	}
	if !LegalTopicNames.MatchString(unprefixedTopic) {
		return errors.Errorf("topic name (%s%s) is invalid", lib.prefix, unprefixedTopic)
	}
	return nil
}

// removePrefix removes a prefix from a topic or consumer group that was added
// because the library had SetPrefix called previously.
func (lib *LibraryNoDB) removePrefix(topicOrConsumerGroup string) string {
	if lib.prefix == "" {
		return topicOrConsumerGroup
	}
	return strings.TrimPrefix(topicOrConsumerGroup, lib.prefix)
}

func (lib *LibraryNoDB) addPrefix(topicOrConsumerGroup string) string {
	return lib.prefix + topicOrConsumerGroup
}

func (lib *LibraryNoDB) addPrefixes(topicsOrConsumerGroups []string) []string {
	p := make([]string, len(topicsOrConsumerGroups))
	for i, unprefixed := range topicsOrConsumerGroups {
		p[i] = lib.prefix + unprefixed
	}
	return p
}

// libraryInterface exists to implement eventmodels.LibraryInterface and make RemovePrefix public
type libraryInterface[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX, DB eventmodels.AbstractDB[ID, TX]] struct {
	*Library[ID, TX, DB]
}

func (lib libraryInterface[ID, TX, DB]) RemovePrefix(topicOrConsumerGroup string) string {
	return lib.removePrefix(topicOrConsumerGroup)
}
