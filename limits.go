package events

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/generic"
)

type sizeCapTopicLimit struct {
	state       atomic.Int32  // 0=init 1=loading 2=ready 3=failed
	ready       chan struct{} // closed when state becomes ready or failed
	overrideMax int           // max.message.bytes from topic config (0 means unknown/not set)
	effective   int           // computed limit (writer/broker/topic)
	err         error         // set if failed
}

const (
	// Assumed minimum per-topic capacity (Kafka default broker/topic limit is usually >= 1,000,001).
	sizeCapDefaultAssumed = 1_000_000
	sizeCapWriterDefault  = 1_048_576 // kafka-go default BatchBytes when unset

	ErrTooBig errors.String = "message size exceeds topic/broker limits"

	sizeCapFetchNotStarted = 0
	sizeCapFetchStarted    = 1
	sizeCapFetchFinished   = 2
	sizeCapFetchFailed     = 3
)

var highLimitBackoffPolicy = backoff.Exponential(
	backoff.WithMinInterval(time.Second*4),
	backoff.WithMaxInterval(time.Second*1800),
	backoff.WithJitterFactor(0.05),
)

// prepareToProduce makes sure that all topics are created and if
// there are any messages that are >1MB, it makes sure that the message size
// caps are known. If any message exceeds the size cap, it return error.
// prepareToProduce ensures topics exist and enforces size caps for large kafka.Messages.
// Returning error early is needed because otherwise large messages would clog the
// system by failing repeatadly.
func (lib *Library[ID, TX, DB]) prepareToProduce(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	var maxPayload int
	sizes := make([]int, len(messages))
	topicSet := make(map[string]struct{}, len(messages))
	for i, m := range messages {
		size := sizeOfMessage(m)
		sizes[i] = size
		if size > maxPayload {
			maxPayload = size
		}
		topicSet[m.Topic] = struct{}{}
	}
	topics := generic.Keys(topicSet)

	// Start broker caps & precreated scan but do not wait yet.
	lib.sizeCapStartBrokerCaps(ctx)
	lib.sizeCapStartPrecreatedScan(ctx)

	err := lib.CreateTopics(ctx, fmt.Sprintf("produce %d (eg %s %s)", len(messages), messages[0].Topic, string(messages[0].Key)), topics)
	if err != nil {
		return err
	}

	// Decide if we can defer full cap resolution.
	if maxPayload <= sizeCapDefaultAssumed {
		// Fire off background fetch; do not block. Reuse provided context.
		go lib.sizeCapFetchTopicConfigLimits(ctx, topics) // background
		return nil
	}

	// Large message path: ensure limits (blocking fetch).
	lib.sizeCapWaitForBroker(ctx)
	lib.sizeCapFetchTopicConfigLimits(ctx, topics) // blocking

	for i, m := range messages {
		cap := lib.sizeCapTopic(ctx, m.Topic)
		if cap > 0 && sizes[i] > cap {
			return ErrTooBig.Errorf("message size is too big for topic %s: %d > %d", m.Topic, sizes[i], cap)
		}
	}

	return nil
}

// sizeCapStartBrokerCaps triggers asynchronous broker cap loading if not started.
func (lib *LibraryNoDB) sizeCapStartBrokerCaps(ctx context.Context) {
	if !lib.sizeCapBrokerState.CompareAndSwap(sizeCapFetchNotStarted, sizeCapFetchStarted) {
		return
	}
	go lib.sizeCapLoadBrokerCaps(ctx)
}

// sizeCapLoadBrokerCaps performs DescribeConfigs for broker-level size limits
func (lib *LibraryNoDB) sizeCapLoadBrokerCaps(ctx context.Context) {
	defer close(lib.sizeCapBrokerReady)
	b := highLimitBackoffPolicy.Start(ctx)
	var resp *kafka.DescribeConfigsResponse
	for {
		err := func() error {
			brokerID, err := lib.getABrokerID(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			client, err := lib.getController(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			// Perform DescribeConfigs for broker and parse size-related entries.
			resp, err = client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
				Resources: []kafka.DescribeConfigRequestResource{
					{
						ResourceType: kafka.ResourceTypeBroker,
						ResourceName: brokerID,
						ConfigNames:  []string{"message.max.bytes", "socket.request.max.bytes"},
					},
				},
			})
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		}()
		if err != nil {
			lib.sizeCapBrokerErr.Store(err)
			lib.sizeCapBrokerState.Store(sizeCapFetchFailed)
			_ = lib.RecordErrorNoWait("size cap fetch", errors.Errorf("could not fetch client size cap: %w", err))
			if backoff.Continue(b) {
				continue
			}
			return
		}
		break
	}
	for _, r := range resp.Resources {
		for _, e := range r.ConfigEntries {
			if e.ConfigName == "message.max.bytes" {
				if v, convErr := strconv.ParseInt(e.ConfigValue, 10, 64); convErr == nil {
					lib.sizeCapBrokerMessageMax.Store(int32(v))
				}
			}
			if e.ConfigName == "socket.request.max.bytes" {
				if v, convErr := strconv.ParseInt(e.ConfigValue, 10, 64); convErr == nil {
					lib.sizeCapSocketRequestMax.Store(int32(v))
				}
			}
		}
	}
	lib.sizeCapBrokerState.Store(sizeCapFetchFinished)
}

// sizeCapWaitForBroker waits for broker caps goroutine completion or ctx cancel.
func (lib *LibraryNoDB) sizeCapWaitForBroker(ctx context.Context) {
	switch lib.sizeCapBrokerState.Load() {
	case sizeCapFetchFinished:
		return
	case sizeCapFetchNotStarted:
		lib.sizeCapStartBrokerCaps(ctx)
	}
	select {
	case <-lib.sizeCapBrokerReady:
	case <-ctx.Done():
	}
}

// sizeCapBrokerEffective returns effective broker cap (0 if unknown).
func (lib *LibraryNoDB) sizeCapBrokerEffective() int {
	if lib.sizeCapBrokerState.Load() != 2 {
		return 0
	}
	bm := int(lib.sizeCapBrokerMessageMax.Load())
	if bm <= 0 {
		return 0
	}
	sr := int(lib.sizeCapSocketRequestMax.Load())
	if sr > 0 && sr < bm {
		return sr
	}
	return bm
}

// sizeCapStartPrecreatedScan initializes background scan of precreated topics.
func (lib *LibraryNoDB) sizeCapStartPrecreatedScan(ctx context.Context) {
	if lib.sizeCapPrecreatedState.Load() != 0 {
		return
	}
	if !lib.sizeCapPrecreatedState.CompareAndSwap(0, 1) {
		return
	}
	lib.sizeCapPrecreatedReady = make(chan struct{})
	go lib.sizeCapRunPrecreatedScan(ctx)
}

func (lib *LibraryNoDB) sizeCapRunPrecreatedScan(ctx context.Context) {
	defer func() {
		lib.sizeCapPrecreatedState.Store(sizeCapFetchFinished)
		close(lib.sizeCapPrecreatedReady)
	}()
	// Enumerate existing topicsSeen; create empty sizeCapTopicLimit entries (state stays init).
	lib.topicsSeen.Range(func(topic string, _ *creatingTopic) bool {
		_ = lib.sizeCapGetOrInit(topic)
		return true
	})
}

// sizeCapGetOrInit returns or creates a per-topic sizeCapTopicLimit.
func (lib *LibraryNoDB) sizeCapGetOrInit(topic string) *sizeCapTopicLimit {
	if v, ok := lib.sizeCapTopicLimits.Load(topic); ok {
		return v
	}
	tl := &sizeCapTopicLimit{ready: make(chan struct{})}
	actual, _ := lib.sizeCapTopicLimits.LoadOrStore(topic, tl)
	return actual
}

// sizeCapFetchTopicConfigLimits loads overrides for provided topics.
func (lib *LibraryNoDB) sizeCapFetchTopicConfigLimits(ctx context.Context, topics []string) {
	// Identify which need loading.
	loadList := make([]string, 0, len(topics))
	for _, topic := range topics {
		tl := lib.sizeCapGetOrInit(topic)
		if tl.state.CompareAndSwap(sizeCapFetchNotStarted, sizeCapFetchStarted) {
			loadList = append(loadList, topic)
		}
	}
	if len(loadList) == 0 {
		return
	}
	b := highLimitBackoffPolicy.Start(ctx)
	for {
		if len(loadList) == 0 {
			return
		}
		err := func() error {
			client, err := lib.getController(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			req := &kafka.DescribeConfigsRequest{
				Resources: make([]kafka.DescribeConfigRequestResource, len(loadList)),
			}
			for i, topic := range loadList {
				req.Resources[i].ResourceType = kafka.ResourceTypeTopic
				req.Resources[i].ResourceName = topic
				req.Resources[i].ConfigNames = []string{"max.message.bytes"}
			}
			resp, err := client.DescribeConfigs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			// Now assign overrides aligned by index
			// Map results back to topics
			newLoadList := make([]string, 0, len(loadList))
			for i, topic := range loadList {
				if i >= len(resp.Resources) {
					continue
				}
				result := resp.Resources[i]
				tl := lib.sizeCapGetOrInit(topic)
				if result.Error != nil {
					_ = lib.RecordErrorNoWait("fetchSpecificTopicConfiguration", errors.WithStack(result.Error))
					tl.state.Store(sizeCapFetchFailed)
					newLoadList = append(newLoadList, topic)
					continue
				}
				var v int64
				for _, entry := range result.ConfigEntries {
					if entry.ConfigName == "max.message.bytes" {
						if parsed, err := strconv.ParseInt(entry.ConfigValue, 10, 64); err == nil {
							v = parsed
						}
					}
				}
				tl.overrideMax = int(v)
				lib.sizeCapComputeEffective(tl)
				tl.state.Store(sizeCapFetchFinished)
				close(tl.ready)
			}
			loadList = newLoadList
			return nil
		}()
		if err != nil {
			_ = lib.RecordErrorNoWait("fetchTopicConfiguration", err)
		}
		if err != nil || len(loadList) != 0 {
			if !backoff.Continue(b) {
				// context cancelled
				return
			}
			continue
		}
		break
	}
}

func (lib *LibraryNoDB) sizeCapTopic(ctx context.Context, topic string) int {
	lib.sizeCapFetchTopicConfigLimits(ctx, []string{topic})
	tl := lib.sizeCapGetOrInit(topic)
	return tl.effective
}

// sizeCapComputeEffective derives final topic specific limit from override, broker, writer.
func (lib *LibraryNoDB) sizeCapComputeEffective(tl *sizeCapTopicLimit) {
	if tl.effective != 0 {
		return
	}
	broker := lib.sizeCapBrokerEffective()
	writer := lib.sizeCapWriterBatchBytes
	if writer <= 0 {
		writer = sizeCapWriterDefault
	}
	var base int
	if tl.overrideMax > 0 && broker > 0 {
		if tl.overrideMax < broker {
			base = tl.overrideMax
		} else {
			base = broker
		}
	} else if tl.overrideMax > 0 {
		base = tl.overrideMax
	} else if broker > 0 {
		base = broker
	} else {
		base = writer // fallback when nothing known
	}
	if writer > 0 && writer < base {
		base = writer
	}
	if base <= 0 {
		base = writer
	}
	tl.effective = base
}

// sizeCapEnsureEffective waits for a topic limit to finish loading or computes fallback.
func (lib *LibraryNoDB) sizeCapEnsureEffective(ctx context.Context, topic string) {
	tl := lib.sizeCapGetOrInit(topic)
	st := tl.state.Load()
	switch st {
	case sizeCapFetchFinished:
		lib.sizeCapComputeEffective(tl)
		return
	case sizeCapFetchNotStarted:
		if tl.state.CompareAndSwap(sizeCapFetchNotStarted, sizeCapFetchStarted) {
			lib.sizeCapFetchTopicConfigLimits(ctx, []string{topic})
		}
	}
	select {
	case <-tl.ready:
	case <-ctx.Done():
	}
}

func sizeOfMessage(m kafka.Message) int {
	s := 64 + len(m.Topic) + len(m.Key) + len(m.Value)
	for _, h := range m.Headers {
		s += 8 + len(h.Key) + len(h.Value)
	}
	return s
}
