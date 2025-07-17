# events - golang events wrapper makes using Kafka type safe and easy

[![GoDoc](https://godoc.org/github.com/singlestore-labs/events?status.svg)](https://pkg.go.dev/github.com/singlestore-labs/events)
![unit tests](https://github.com/singlestore-labs/events/actions/workflows/go.yml/badge.svg)
[![report card](https://goreportcard.com/badge/github.com/singlestore-labs/events)](https://goreportcard.com/report/github.com/singlestore-labs/events)
[![codecov](https://codecov.io/gh/singlestore-labs/events/branch/main/graph/badge.svg)](https://codecov.io/gh/singlestore-labs/events)

Install:

	go get github.com/singlestore-labs/events

---

The events library tries to make using Kafka super easy. It provides:

- Single line message produce 
- Single line message consume
- Full type safety without casts
- A broadcast abstraction that can be used for cache invalidation
- Bulk consumers
- A limit on the number of outstanding consumption go-routines
- Prometheus metrics
- Optional required pre-registration of topics
- Dynamic opt-in/out consumption
- When used with a transactional database:

  - exactly once semantics for message consumption
  - at least once semantics for messages created during a transaction
  - support for PostgreSQL
  - support for SingleStore

## Basics

This library provides a message queue abstraction. It is built on top of Kafka. Events are
categorized into "topics".

To use events, you must initialize the event library.

In code, you must register the topic, binding it to a particular Go model.

To publish to a topic, you can do so in one line either inside a transaction (with
transactional guarantees) or outside a transaction (without guarantees).

To consume a topic, you can do so with one line. You need to pick which flavor of consumer
to use (idempotent, exactly-once, or broadcast). 

When used with a database, two tables are needed: one to track events created during a transaction
and another to track the consumption of exactly-once events.

## Initialize the library

In `init()` or in `main()`, create an instance:

```go
db, _  := sql.Open(...)

// PostgreSQL:
eventLib := events.New[eventmodels.StringEventID, *eventdb.ExampleBasicTX, *eventpg.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]]() 
conn := eventpg.New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](eventdb.ExampleBasicDB{
	DB: db,
})

// SingleStore:
eventLib := events.New[eventmodels.BinaryEventID, *eventdb.ExampleBasicTX, *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]]()
conn := events2.New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](eventdb.ExampleBasicDB{
	DB: db,
}, "lockPrefix")

// No database
eventLib := events.New[eventmodels.BinaryEventID, *eventnodb.NoDBTx, *eventnodb.NoDB]()
var conn *eventnodb.NoDB
```

That instance needs to be configured. It can be configured before or after
consumers are registered.

```go
eventLib.Configure(conn, tracer, true, saslMechanism, tlsConfig, brokers)
```

After `Configure` and after consumers are registered, consumers can be started:

```go
consumerStarted, consumerFinished, err := eventLib.StartConsuming(ctx)
```

You know that consumers are ready and processing when the `consumerStarted` channel is
closed.

After `Configure` you can start a background produce thread. This handles sending events
that were created transactionally but not actually sent at the time of the transaction.
There should be at least one server doing this. The more servers doing this, the longer
the delay can be. In the control plane, this is handled by the state service.

```go
producerDone, err := eventLib.CatchUpProduce(ctx, time.Second*5, 64)
```

Create at least one consumer group per service:

```
StateServerConsumer := events.NewConsumerGroup("state-server")
```

### Event ids

The event id (Kafka "key") can be used by Kafka to discard obsolete versions of the same
message. We are not currently using this feature, but we could in the future. The key should
should either be random or the topic+key should represent the current state of an object.
This implies, for example, that if you have an object that represents a user's membership within
a group, the key MUST have both the userid and the groupid.

## Consume events

There are three patterns for event consumption.

Every consumer must have a consumer group, a handler name, and a handler.

The handler is created by combining the registered topic with code.

Handlers can be called in parallel if there are multiple outstanding messages. Kafka does not
guarantee message ordering so expect messages to come in any order.

Behavior for failure must be chosen for each handler.

For the non-broadcast consumers, the system tries to deliver each message once per consumer group.
If a consumer group used by multiple server instances (same service) that is still true: only
one instance should get the message.

There are no guarantees made about event ordering. In practice, generally events sent earlier
will be consumed earlier, but out-of-order deliver must be tolerated by all consumers.

### Consumer groups

Consumer groups are a Kafka concept. Multiple topics can be in the same consumer group. There is a
cost for each consumer group and Kafka limits the number of them. Consumer group names should not be
shared between different services. For this reason, include the service name in the consumer group name.

Do not share consumer groups between different services.

Except for possible starvation due to the use of `OnFailureBlock` (see below), there is little
need to have more than one consumer group per service. Each consumer group will be read 
one-message-at-a-time in a go-routine. The processing of the events happens in other go-routines.
At high volume, it could be useful to have multiple consumer groups in a service.

When a consumer group is created, or a topic added to a consumer group, it will read messages from
the beginning of time for each (new) topic. The binding of topics and handlers to consumer
groups should be kept very stable.

Consumer group names must be registered with `NewConsumerGroup()`

### Handler names

Multiple handlers can be invoked for each topic in each consumer group. To make error reporting
sane, handlers must be named. The name will show up in error messages and in prometheus parameterizations.

### Failure behavior

There are four possible behaviors for what happens if a handler returns error. They are:

- `eventmodels.OnFailureDiscard`:
  If the handler returns error beyond the retry point, which could
  be just one failure, the message will be discarded from the point-of-view of
  that handler.
- `eventmodels.OnFailureBlock`:
  If the handler returns error, the handler will be called again. And again.
  This behavior can cause re-delivery of old messages in the same consumer group since
  no messages would get committed.
  It can cause starvation of handlers and consumer groups
  due to per-handler and per-consumer-group limits on the number of simultaneous outstanding
  messages waiting to be handled.
- `eventmodels.OnFailureSave`:
  If the handler returns error beyond the retry point, which could
  be just one failure, the message will be copied into a per-topic, per-consumer-group
  dead letter queue (topic). The message will be left there.
  Alerts are required to know that there is a problem and the queue needs to be looked at.
- `eventmodels.OnFailureRetryLater`:
  If the handler returns error beyond the retry point, which could
  be just one failure, the message will be copied into a per-topic, per-consumer-group
  dead letter queue (topic). Messages in the dead letter queue will be automatically re-processed.
  The re-processing is subject to limits on the number of simultaneous outstanding messages
  and thus if the handler fails again during re-processing, it may prevent further failures
  from being re-processed.

In all cases, prometheus metrics will be incremented to count the failures on a per-handler
basis.

### Idempotent consumer

The idempotent consumer tries to deliver events once. It can deliver the same message multiple
times. The consumer must handle that itself.


```go
eventLib.ConsumeIdempotent(StateServerConsumer, eventmodels.OnFailureDiscard, "send-cluster-created-email", MyTopic.Handler(
	func(ctx context.Context, e eventmodels.Event[PayloadForMyTopic]) error {
		// do something
		return nil
	}))
```

### Exactly-once consumer

The exactly-once consumer includes an transaction in its callback. If that tranaction commits,
the message is considered to have been delivered. If it does not commit, the message is not considered
delivered and the handler may be called again in the future.


```go
eventLib.ConsumeExactlyOnce(StateServerConsumer, eventmodels.OnFailureDiscard, "send-cluster-created-email", MyTopic.HandlerTx(
	func(ctx context.Context, tx *sql.Tx, e eventmodels.Event[PayloadForMyTopic]) error {
		// do something
		return nil
	}))
```

### Broadcast consumer

Broadcast consumers are special. They're meant for things like cache invalidation.

The other consumer start from the beginning of time for each topic and try to deliver each
message just once.

The broadcast consumer ignores messages that were created before the consumer was started. It
delivers messages to all handlers in all services.

```go
eventLib.ConsumeBroadcast("invalidate-cluster-cache", MyTopic.Handler(
	func(ctx context.Context, e eventmodels.Event[PayloadForMyTopic]) error {
		// do something
		return nil
	}))
```

## Batching

Each of the consumers can be used with a handler that processes one message at a time or with
a handler that processes a slice of messages at once.

To process with batches, use `MyTopic.BatchHandler` with `ConsumeIdempotent` or `ConsumeBroadcast`,
or `MyTopic.BatchHandlerTx` with `ConsumeExactlyOnce`.

When processing in batches, there is both a limit on the size of the batch (defaults to 30 and
can be overridden with `WithBatch`) and a limit on the number of batches that can be processed
in parallel (defaults to 3 and can be overridden with `WithConcurrency`).

Batches of more than one message are only formed when the limit on batch concurrency has been
reached so that no additional handler can be invoked. When that happens messages are queued.
When a concurrent handler becomes available, a batch will be formed out of the queued messages.

This behavior is designed to increase efficiency when handling more than one message at time
is more efficient than handling one message at a time. This is usually true for the 
`ConsumeExactlyOnce` handlers because fewer transactions will be created.

## Dynamic opt-in/opt-out consumption of broadcast consumers

To add/remove consumers at runtime, use `RegisterFiltered` and `RegisterUnfiltered`. 
`RegisterFiltered` uses a function to extract a key from an event so that you can then
subscribe to specific keys.

## Without a database

The event library does not require a database connection. Without a database connection some
features are not available:

- produce from within a transaction
- catch up production
- exactly once consumption

## Database configuration

The required tables can be created with [libschema](https://github.com/muir/libschema) or
they can be created through other means. See the `Migrations` function in
[events2](https://pkg.go.dev/github.com/singlestore-labs/events/events2#Migrations) for SingleStore migrations or 
[eventpg](https://pkg.go.dev/github.com/singlestore-labs/events/eventpg#Migrations) for PostgreSQL migrations.
See examples of using libschema in the 
[events2/shared_test](https://github.com/singlestore-labs/events/blob/main/events2/shared_test.go)
and
[eventpg/shared_test](https://github.com/singlestore-labs/events/blob/main/eventpg/shared_test.go).

## Kafka configuration

For production use, configure:

At least 3 availability zones (replicas) and each topic needs:

- `min.insync.replicas`: at least 2 and less than the number of availability zones
- `replication.factor`: the number of availabilty zones 

Because broadcast topics sometimes use the `offsetsForTimes` request, message timestamps must be
set to the the non-default value of `LogAppendTime`. This is the default for topics created by
the events library. See [discusstion with ChatGPT](https://chatgpt.com/share/68432d60-ac28-8005-bea1-cf3c175204ba)
about why this is important.

- `message.timestamp.type`: `LogAppendTime`

Since the message timestamps will use the log append time, if message consumers want to know when
the message was created, a second timestamp needs to be within the message itself. Perhaps as
a header.
