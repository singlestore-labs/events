package events

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/generic"
)

type sizeCapTopicLimit struct {
	overrideMax int64 // max.message.bytes from topic config (0 means unknown/not set)
	effective   int64 // computed limit (writer/broker/topic)
}

const (
	ErrTooBig errors.String = "message size exceeds topic/broker limits"

	sizeCapFetchNotStarted = 0
	sizeCapFetchStarted    = 1
	sizeCapFetchFinished   = 2
	sizeCapFetchFailed     = 3

	debugLimits = false
)

var highLimitBackoffPolicy = backoff.Exponential(
	backoff.WithMinInterval(time.Second*4),
	backoff.WithMaxInterval(time.Second*1800),
	backoff.WithJitterFactor(0.05),
)

// prepareToProduce makes sure that all topics are created and if
// there are any messages that are >1MB, it makes sure that the message size
// caps are known. If any message exceeds the size cap, it returns an error.
// prepareToProduce ensures topics exist and enforces size caps for large kafka.Messages.
// Returning error early is needed because otherwise large messages would clog the
// system by failing repeatdly.
func (lib *Library[ID, TX, DB]) prepareToProduce(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	var maxPayload int64
	sizes := make([]int64, len(messages))
	unprefixedTopicSet := make(map[string]struct{}, len(messages))
	for i, m := range messages {
		size := sizeOfMessage(m)
		sizes[i] = size
		if size > maxPayload {
			maxPayload = size
		}
		unprefixedTopicSet[lib.removePrefix(m.Topic)] = struct{}{}
	}
	unprefixedTopics := generic.Keys(unprefixedTopicSet)

	// Start broker caps & precreated scan but do not wait yet.
	lib.sizeCapStartBrokerCaps(ctx)

	err := lib.topicsWork.WorkUntilDone(ctx, unprefixedTopics, topicsWhy{
		why:           fmt.Sprintf("produce %d (eg %s %s)", len(messages), messages[0].Topic, string(messages[0].Key)),
		errorCategory: "createTopicsForProduce",
	})
	if err != nil {
		return err
	}

	// Decide if we can defer full cap resolution.
	if maxPayload <= lib.sizeCapDefaultAssumed {
		// Fire off background fetch; do not block. Reuse provided context.
		go func() {
			_ = lib.sizeCapWork.WorkUntilDone(ctx, unprefixedTopics, "prefetching size caps")
		}()
		if debugLimits {
			lib.tracer.Logf("limits: no large messages in batch %d (%s %s)", len(messages), messages[0].Topic, string(messages[0].Key))
		}
		return nil
	}

	// Large message path: ensure limits (blocking fetch)
	lib.sizeCapWaitForBroker(ctx)
	err = lib.sizeCapWork.WorkUntilDone(ctx, unprefixedTopics, "fetching size caps before sending large messages")
	if err != nil {
		return err
	}

	for i, m := range messages {
		// we know the load will succeed because lib.sizeCapWork.WorkUntilDone returned nil
		size, _ := lib.sizeCapTopicLimits.Load(lib.removePrefix(m.Topic))
		if size.effective > 0 && sizes[i] > size.effective {
			return ErrTooBig.Errorf("message size is too big for topic %s: %d > %d", m.Topic, sizes[i], size.effective)
		}
	}

	if debugLimits {
		lib.tracer.Logf("limits: no violating messages in batch %d (%s %s)", len(messages), messages[0].Topic, string(messages[0].Key))
	}
	return nil
}

// sizeCapStartBrokerCaps triggers asynchronous broker cap loading if not started.
//
// If the context provided is cancelled, which is entirely possible, then the loading
// of size caps could fail. If it fails, then it can get restarted by another call
// to sizeCapStartBrokerCaps. There exists a tiny race condition where the broker
// capacity loading is failing but the status is not yet failed. In that case,
// the capacity loading could fail to be started.
func (lib *LibraryNoDB) sizeCapStartBrokerCaps(ctx context.Context) {
	if lib.sizeCapBrokerState.Load() == sizeCapFetchFinished {
		return
	}
	lib.sizeCapBrokerLoadCtx.Add(ctx)
	// Even though sizeCapBrokerState is atomic, there is a bit of critical
	// code around changing it that needs to be protected so that if
	// sizeCapLoadBrokerCaps is about to fail, it can restart itself
	// if there is a brand new context to use
	lib.sizeCapBrokerLock.Lock()
	defer lib.sizeCapBrokerLock.Unlock()
	switch lib.sizeCapBrokerState.Load() {
	case sizeCapFetchNotStarted, sizeCapFetchFailed:
		lib.sizeCapBrokerState.Store(sizeCapFetchStarted)
		go lib.sizeCapLoadBrokerCaps()
	}
}

// sizeCapLoadBrokerCaps performs DescribeConfigs for broker-level size limits
func (lib *LibraryNoDB) sizeCapLoadBrokerCaps() {
	ctx := lib.sizeCapBrokerLoadCtx
	if debugLimits {
		lib.tracer.Logf("limits: sizeCapLoadBrokerCaps started")
		defer func() {
			lib.tracer.Logf("limits: sizeCapLoadBrokerCaps completed")
		}()
	}
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
			_ = lib.RecordErrorNoWait("size cap fetch", errors.Errorf("could not fetch client size cap: %w", err))
			if backoff.Continue(b) {
				continue
			}
			lib.sizeCapBrokerLock.Lock()
			defer lib.sizeCapBrokerLock.Unlock()
			if ctx.Err() == nil {
				go lib.sizeCapLoadBrokerCaps()
			} else {
				lib.sizeCapBrokerState.Store(sizeCapFetchFailed)
			}
			return
		}
		break
	}
	for _, r := range resp.Resources {
		for _, e := range r.ConfigEntries {
			if e.ConfigName == "message.max.bytes" {
				if v, err := strconv.ParseInt(e.ConfigValue, 10, 64); err == nil {
					lib.sizeCapBrokerMessageMax.Store(v)
				} else {
					lib.tracer.Logf("invalid value in broker config for message.max.bytes: %s: %v", e.ConfigValue, err)
				}
			}
			if e.ConfigName == "socket.request.max.bytes" {
				if v, err := strconv.ParseInt(e.ConfigValue, 10, 64); err == nil {
					lib.sizeCapSocketRequestMax.Store(v)
				} else {
					lib.tracer.Logf("invalid value in broker config for socket.request.max.bytes: %s: %v", e.ConfigValue, err)
				}
			}
		}
	}
	lib.tracer.Logf("[events] broker size cap limits fetched: message: %d request: %d", lib.sizeCapBrokerMessageMax.Load(), lib.sizeCapSocketRequestMax.Load())
	lib.sizeCapBrokerLock.Lock()
	defer lib.sizeCapBrokerLock.Unlock()
	if lib.sizeCapBrokerState.Load() != sizeCapFetchFinished {
		lib.sizeCapBrokerState.Store(sizeCapFetchFinished)
		close(lib.sizeCapBrokerReady)
	}
}

// sizeCapWaitForBroker waits for broker caps goroutine completion or ctx cancel.
func (lib *LibraryNoDB) sizeCapWaitForBroker(ctx context.Context) {
	if lib.sizeCapBrokerState.Load() == sizeCapFetchFinished {
		return
	}
	if debugLimits {
		lib.tracer.Logf("limits: sizeCapWaitForBroker start wait")
		defer func() {
			lib.tracer.Logf("limits: sizeCapWaitForBroker wait complete")
		}()
	}
	lib.sizeCapStartBrokerCaps(ctx)
	select {
	case <-lib.sizeCapBrokerReady:
	case <-ctx.Done():
	}
}

// sizeCapBrokerEffective returns effective broker cap (0 if unknown).
func (lib *LibraryNoDB) sizeCapBrokerEffective() int64 {
	if lib.sizeCapBrokerState.Load() != sizeCapFetchFinished {
		if debugLimits {
			lib.tracer.Logf("limits: sizeCapBrokerEffective - fetch unfinished, returning 0")
		}
		return 0
	}
	bm := lib.sizeCapBrokerMessageMax.Load()
	if bm <= 0 {
		if debugLimits {
			lib.tracer.Logf("limits: sizeCapBrokerEffective - max is <= 0, returning 0")
		}
		return 0
	}
	sr := lib.sizeCapSocketRequestMax.Load()
	if sr > 0 && sr < bm {
		if debugLimits {
			lib.tracer.Logf("limits: sizeCapBrokerEffective - broker max: %d, request max: %d -> %d", bm, sr, sr)
		}
		return sr
	}
	if debugLimits {
		lib.tracer.Logf("limits: sizeCapBrokerEffective - broker max: %d, request max: %d -> %d", bm, sr, bm)
	}
	return bm
}

func (lib *LibraryNoDB) configureSizeCapPrework() {
	lib.sizeCapWork.MaxSimultaneous = 20
	lib.sizeCapWork.BackoffPolicy = highLimitBackoffPolicy
	lib.sizeCapWork.WorkDeadline = topicCreationDeadline
	lib.sizeCapWork.ItemRetryDelay = 5 * time.Second
	lib.sizeCapWork.ErrorReporter = func(_ context.Context, err error, _ string) {
		_ = lib.RecordErrorNoWait("sizeCapPerTopic", err)
	}
	lib.sizeCapWork.NotRetryingError = func(ctx context.Context, unprefixedTopic string, why string, err error) error {
		err = errors.Errorf("event library topic (%s) size cap fetch failed (%s): %w", unprefixedTopic, why, err)
		lib.tracer.Logf("[events] %s: %+v", why, err)
		return err
	}
	lib.sizeCapWork.ItemsWork = func(ctx context.Context, loadList []string, why string) []error {
		client, err := lib.getController(ctx)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		req := &kafka.DescribeConfigsRequest{
			Resources: make([]kafka.DescribeConfigRequestResource, len(loadList)),
		}
		for i, unprefixedTopic := range loadList {
			req.Resources[i].ResourceType = kafka.ResourceTypeTopic
			req.Resources[i].ResourceName = lib.addPrefix(unprefixedTopic)
			req.Resources[i].ConfigNames = []string{"max.message.bytes"}
		}
		resp, err := client.DescribeConfigs(ctx, req)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		// Now assign overrides aligned by index
		// Map results back to topics
		errs := make([]error, len(loadList))
		for i, unprefixedTopic := range loadList {
			if i >= len(resp.Resources) {
				continue
			}
			result := resp.Resources[i]
			if result.Error != nil {
				_ = lib.RecordErrorNoWait("fetchSpecificTopicConfiguration", errors.Errorf("fetch topic (%s) config for size caps: %w", unprefixedTopic, result.Error))
				errs[i] = errors.WithStack(result.Error)
				continue
			}
			var overrideMax int64
			for _, entry := range result.ConfigEntries {
				if entry.ConfigName == "max.message.bytes" {
					parsed, err := strconv.ParseInt(entry.ConfigValue, 10, 64)
					if err != nil {
						errs[i] = errors.Errorf("could not parse config value (%s): %w", entry.ConfigValue, err)
					} else {
						overrideMax = parsed
					}
				}
			}
			if errs[i] == nil {
				effective := lib.sizeCapComputeEffective(overrideMax, unprefixedTopic)
				lib.sizeCapTopicLimits.Store(unprefixedTopic, sizeCapTopicLimit{
					overrideMax: overrideMax,
					effective:   effective,
				})
				lib.tracer.Logf("[events] topic %s limit %d", unprefixedTopic, effective)
			}
		}
		return errs
	}
	lib.sizeCapWork.ItemDone = func(_ context.Context, unprefixedTopic string, why string) {
		size, _ := lib.sizeCapTopicLimits.Load(unprefixedTopic)
		lib.tracer.Logf("[events] %s: topic %s size cap fetched: %d", why, unprefixedTopic, size.effective)
	}
	lib.sizeCapWork.ItemFailed = func(_ context.Context, unprefixedTopic string, why string, err error, primary bool) error {
		err = errors.Errorf("event library fetching topic size limit (%s) (%s): %w", unprefixedTopic, why, err)
		if primary {
			err = errors.Alert(err)
		}
		lib.tracer.Logf("[events] %+v", err)
		return err
	}
	lib.sizeCapWork.ItemPending = func(_ context.Context, unprefixedTopic string, why string) {
		lib.tracer.Logf("[events] %s: will wait for size cap fetch of topic %s to complete", why, unprefixedTopic)
	}
}

// sizeCapComputeEffective derives final topic specific limit from override, broker, writer.
func (lib *LibraryNoDB) sizeCapComputeEffective(overrideMax int64, unprefixedTopic string) int64 {
	broker := lib.sizeCapBrokerEffective()
	var effective int64
	if overrideMax > 0 && broker > 0 {
		if overrideMax < broker {
			effective = overrideMax
		} else {
			effective = broker
		}
	} else if overrideMax > 0 {
		effective = overrideMax
	} else if broker > 0 {
		effective = broker
	}
	if debugLimits {
		lib.tracer.Logf("limits: topic %s: broker %d override max %d -> %d",
			unprefixedTopic, broker, overrideMax, effective)
	}
	return effective
}

func sizeOfMessage(m kafka.Message) int64 {
	s := 64 + int64(len(m.Topic)) + int64(len(m.Key)) + int64(len(m.Value))
	for _, h := range m.Headers {
		s += 8 + int64(len(h.Key)) + int64(len(h.Value))
	}
	return s
}
