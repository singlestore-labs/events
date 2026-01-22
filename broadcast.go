package events

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/generic"
	"github.com/singlestore-labs/simultaneous"
)

type HeartbeatEvent struct{}

var heartbeatTopic = eventmodels.BindTopic[HeartbeatEvent]("event-library.heartbeat.broadcast.v1")

const (
	startingMaxLockID      = 10
	maxLockIDMinIncrement  = 3
	maxLockIDMinMultiplier = 1.02
)

// consumeBroadcast handles consuming messages and calling handlers for
// handlers that were registered with ConsumeBroadcast. For broadcast consumption,
// each server needs its own consumer group. We use advisory locks to probe for
// the first broadcast consumer group that is not currently in use.
//
// The only way to "catch up" a consumer group to current positions using the
// segmentio library is to remove the consumer group and then when it's re-created
// through use, set StartOffiset = LastOffset in the reader configuration.
//
// Unfortunately, removing and creating topics can be kinda slow so this can delay the startup
// of servers that consume broadcasts if they wait for StartConsuming() to complete.
func (lib *Library[ID, TX, DB]) consumeBroadcast(startupCtx context.Context, baseCtx context.Context, allStarted *sync.WaitGroup, allDone *sync.WaitGroup) error {
	err := lib.precreateTopicsForConsuming(startupCtx, "broadcast", broadcastTopics(generic.Keys(lib.broadcast.topics)))
	if err != nil {
		return err
	}
	limiter := simultaneous.New[eventLimiterType](maximumParallelConsumption)
	broadcastConsumerGroup, reader, readerConfig, unlock, err := lib.getBroadcastConsumerGroup(startupCtx, broadcastStartupMaxWait, allStarted)
	if err != nil {
		return err
	}
	go lib.startConsumingGroup(startupCtx, baseCtx, broadcastConsumerGroup, lib.broadcast, limiter, true, allStarted, allDone, false, reader, readerConfig, unlock)
	return nil
}

func (lib *Library[ID, TX, DB]) getBroadcastConsumerGroup(ctx context.Context, maxWait time.Duration, optStartupWG *sync.WaitGroup) (_ consumerGroupName, reader *kafka.Reader, readerConfig *kafka.ReaderConfig, unlock func() error, err error) {
	idAllocator := newIDAllocator(startingMaxLockID, maxLockIDMinIncrement, maxLockIDMinMultiplier, lib.broadcastConsumerMaxLock)
	startTime := time.Now()
	defer func() {
		if unlock != nil && err != nil {
			_ = unlock()
			unlock = nil
		}
	}()
	broadcastConsumerBaseName := lib.broadcastConsumerBaseName
	var broadcastConsumerGroup consumerGroupName
NewGroup:
	for {
		if err := ctx.Err(); err != nil {
			return "", nil, nil, unlock, err
		}
		potentialLock, err := idAllocator.get()
		if err != nil {
			return "", nil, nil, unlock, err
		}
		if unlock != nil {
			_ = unlock()
			unlock = nil
		}
		if lib.HasDB() && !lib.broadcastConsumerSkipLock {
			var err error
			// We do not unlock on success -- the lock is held until the context is cancelled
			unlock, err = lib.db.LockOrError(ctx, potentialLock, 0)
			switch {
			case err == nil:
				// great, we've got the lock
			case errors.Is(err, eventmodels.TimeoutErr):
				continue
			case errors.Is(err, eventmodels.NotImplementedErr):
				// no lock, and we should disable locking
				lib.logf(ctx, "[events] discovered at run time that our database doesn't support locking")
				// This is safe to change without a lock because the only place this is touched
				// post-startup is in this function and there will be only one instance of this function
				// running.
				lib.broadcastConsumerSkipLock = true
			default:
				_ = lib.RecordError(ctx, "lock-attempt", err)
				continue
			}
		}
		if lib.broadcastConsumerBaseName == "" {
			if !lib.HasDB() || lib.broadcastConsumerSkipLock {
				broadcastConsumerBaseName = defaultLockFreeBroadcastBase
			} else {
				broadcastConsumerBaseName = defaultBroadcastConsumerBaseName
			}
		}
		broadcastConsumerGroup = consumerGroupName(broadcastConsumerBaseName + "-" + strconv.Itoa(int(potentialLock)))
		for {
			reader, readerConfig, err = lib.getBroadcastReader(ctx, broadcastConsumerGroup, true)
			if err != nil {
				if errors.Is(err, errGroupUnavailable) {
					lib.logf(ctx, "[events] potential broadcast group %s was not available: %s", broadcastConsumerGroup, err)
					continue NewGroup
				}
				lib.logf(ctx, "[events] failed allocate broadcast consumer group %s: %+v", broadcastConsumerGroup, err)
				if waiting := time.Since(startTime); waiting > maxWait {
					lib.logf(ctx, "[events] giving up after %s on allocating a consumer group", waiting)
					return "", nil, nil, unlock, err
				}
				continue
			}
			if optStartupWG != nil {
				if debugConsumeStartup {
					lib.logf(ctx, "[events] Debug: start wait for stability for %s", broadcastConsumerGroup)
				}
				optStartupWG.Add(1)
				go lib.waitForStability(ctx, optStartupWG, broadcastConsumerGroup)
			}
			break
		}
		break
	}
	lib.logf(ctx, "[events] using consumer group %s for receiving broadcasts", broadcastConsumerGroup)
	lib.broadcastConsumerGroupName.Store(broadcastConsumerGroup)
	return broadcastConsumerGroup, reader, readerConfig, unlock, nil
}

// errGroupUnavailable is only returned for groups that are definitely unavailable
// and we must try a different group to proceed.
// It's important that errGroupUnavailable is returned exactly for the times
// when the problem is that the group is already in use. If it's returned for other
// errors, then we'll give up on perfectly valid groups. If it's not returned for
// an error when the group is unavailable, we'll keep trying the same unavailable
// group over and over.
const errGroupUnavailable errors.String = "consumer group is unavailable"

// refreshBroadcastReader attempts to re-allocate the same group that was already being used. If that's not successful,
// it will allocate a fresh group.
func (lib *Library[ID, TX, DB]) refreshBroadcastReader(ctx context.Context, broadcastConsumerGroup consumerGroupName, unlock *func() error) (consumerGroupName, *kafka.Reader, *kafka.ReaderConfig, error) {
	reader, readerConfig, err := lib.getBroadcastReader(ctx, broadcastConsumerGroup, false)
	if err != nil {
		if *unlock != nil {
			_ = (*unlock)()
			*unlock = nil
		}
		// We're not going to give up on getting a new consumer group.
		consumerGroupName, reader, readerConfig, newUnlock, err := lib.getBroadcastConsumerGroup(ctx, time.Hour*24*365*10, nil)
		if err != nil {
			return "", nil, nil, err
		}
		*unlock = newUnlock
		return consumerGroupName, reader, readerConfig, nil
	}
	return broadcastConsumerGroup, reader, readerConfig, nil
}

// getBroadcastReader tries to ensure exclusive access to the consumer group
func (lib *Library[ID, TX, DB]) getBroadcastReader(ctx context.Context, broadcastConsumerGroup consumerGroupName, resetPosition bool) (reader *kafka.Reader, readerConfig *kafka.ReaderConfig, err error) {
	lib.logf(ctx, "[events] getting consumer group coordinator for %s", broadcastConsumerGroup)

	// We delete the consumer group to reset it's position to now.
	err = lib.deleteBroadcastConsumerGroup(ctx, broadcastConsumerGroup)
	if err != nil {
		return nil, nil, err
	}

	// Make sure to close the reader if there is an error
	defer func() {
		if err != nil && reader != nil {
			_ = reader.Close()
			reader = nil
		}
	}()

	// We get a reader. Once we check that there is exactly one reader, we know we have exclusive use
	// of the group because any other thread will see more than one reader
	reader, _, readerConfig, err = lib.getReader(ctx, broadcastConsumerGroup, lib.addPrefixes(broadcastTopics(generic.Keys(lib.broadcast.topics))), true, resetPosition)
	if err != nil {
		// context was cancelled
		return nil, nil, errors.WithStack(err)
	}
	resp, err := lib.describeGroup(ctx, broadcastConsumerGroup)
	if err != nil {
		return nil, nil, err
	}
	if len(resp.Members) == 1 {
		lib.logf(ctx, "[events] confirmed exactly one member of consumer group %s, using it", broadcastConsumerGroup)
		return reader, readerConfig, nil
	}
	return nil, nil, errGroupUnavailable.Errorf("not exactly one (%d) member of group (%s)", len(resp.Members), broadcastConsumerGroup)
}

func (lib *Library[ID, TX, DB]) describeGroup(ctx context.Context, broadcastConsumerGroup consumerGroupName) (*kafka.DescribeGroupsResponseGroup, error) {
	prefixedBroadcastConsumerGroup := lib.addPrefix(string(broadcastConsumerGroup))
	b := backoffPolicy.Start(ctx)
FreshClient:
	for {
		// We need a new client. Either the group was deleted and thus the broker may have changed,
		// or there was no prior group and thus no prior client.
		ctxWithTimeout, cancelCtx := context.WithTimeout(ctx, dialTimeout)
		client, err := lib.getConsumerGroupCoordinator(ctxWithTimeout, broadcastConsumerGroup)
		cancelCtx()
		switch {
		case err == nil:
			// perfect
		case isTransientCoordinatorError(err):
			if !backoff.Continue(b) {
				return nil, errors.Errorf("could not start broadcast consumer, failed to get coordinator for group (%s): %w", broadcastConsumerGroup, err)
			}
			_ = lib.RecordErrorNoWait(ctx, "timeout-consume-broadcast", errors.Errorf("failed to get coordinator for group (%s): %w", broadcastConsumerGroup, err))
			continue FreshClient
		default:
			return nil, err
		}

		// We check the number of readers with describe
		ctxWithTimeout, cancelCtx = context.WithTimeout(ctx, describeTimeout)
		describeResponse, err := client.DescribeGroups(ctxWithTimeout, &kafka.DescribeGroupsRequest{
			Addr:     client.Addr,
			GroupIDs: []string{prefixedBroadcastConsumerGroup},
		})
		cancelCtx()
		if err != nil {
			if errors.Is(err, kafka.RequestTimedOut) {
				if !backoff.Continue(b) {
					return nil, errors.Errorf("could not describe consumer group (%s): %w", broadcastConsumerGroup, err)
				}
				_ = lib.RecordErrorNoWait(ctx, "timeout-consume-broadcast", errors.Errorf("describe consumer group (%s) failed: %w", broadcastConsumerGroup, err))
				continue FreshClient
			}
			return nil, errors.Errorf("could not describe broadcast group: %w", err)
		}
		for _, resp := range describeResponse.Groups {
			if resp.GroupID != prefixedBroadcastConsumerGroup {
				return nil, errors.Errorf("got a description for a group (%s) that wasn't what was asked for (%s)", resp.GroupID, broadcastConsumerGroup)
			}
			switch {
			case resp.Error == nil:
				// great!
			case isTransientCoordinatorError(resp.Error):
				if !backoff.Continue(b) {
					return nil, errors.Errorf("could not start broadcast consumer, describe consumer group (%s): %w", broadcastConsumerGroup, resp.Error)
				}
				_ = lib.RecordErrorNoWait(ctx, "timeout-consume-broadcast", errors.Errorf("describe consumer group (%s) failed: %w", broadcastConsumerGroup, resp.Error))
				// Try again to get a description
				continue FreshClient
			default:
				return nil, errors.WithStack(resp.Error)
			}
			return &resp, nil
		}
		if !backoff.Continue(b) {
			return nil, errors.Errorf("could not describe consumer group response (%s) did not include group", broadcastConsumerGroup)
		}
		_ = lib.RecordErrorNoWait(ctx, "timeout-consume-broadcast", errors.Errorf("describe consumer group (%s) did not include group", broadcastConsumerGroup))
		continue
	}
}

// waitForStability will always return very quickly when running single-broker kafka
// clusters like is done for testing this repo. When using real kafka clusters, it can
// take a few seconds for groups to stabalize
func (lib *Library[ID, TX, DB]) waitForStability(ctx context.Context, startupWG *sync.WaitGroup, broadcastConsumerGroup consumerGroupName) {
	defer startupWG.Done()
	if debugConsumeStartup {
		defer lib.logf(ctx, "[events] Debug: done wait for stability for %s", broadcastConsumerGroup)
	}
	for {
		resp, err := lib.describeGroup(ctx, broadcastConsumerGroup)
		if err != nil {
			if ctx.Err() != nil {
				// context cancelled, let's bail
				return
			}
			// this is an awkward situation, describe groups failed with a non-transient error
			_ = lib.RecordError(ctx, "timeout-consume-broadcast", errors.Errorf("describe groups on %s failed with a non-transient error. Startup is incomplete: %+v", string(broadcastConsumerGroup), err))
			continue
		}
		state := strings.TrimSpace(resp.GroupState)
		if strings.EqualFold("Stable", state) {
			// yay!
			lib.logf(ctx, "[events] broadcast consumer group %s is %s", string(broadcastConsumerGroup), state)
			return
		}
		lib.logf(ctx, "[events] broadcast consumer group %s is %s, waiting for it to be stable", string(broadcastConsumerGroup), state)
		timer := time.NewTimer(groupStabilityWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		continue
	}
}

// deleteBroadcastConsumerGroup returns on error or when the group doesn't exist. If the group
// already doesn't exist, that's fine.
func (lib *Library[ID, TX, DB]) deleteBroadcastConsumerGroup(ctx context.Context, broadcastConsumerGroup consumerGroupName) error {
	lib.logf(ctx, "[events] getting consumer group coordinator for %s", broadcastConsumerGroup)
	var triesThisOne int
	b := backoffPolicy.Start(ctx)
	for {
		ctxWithTimeout, cancelCtx := context.WithTimeout(ctx, dialTimeout)
		client, err := lib.getConsumerGroupCoordinator(ctxWithTimeout, broadcastConsumerGroup)
		cancelCtx()
		switch {
		case err == nil:
			// We delete the group to reset its offsets to zero
			// We'll keep trying until the delete doesn't time out
			ctxWithTimeout, cancelCtx = context.WithTimeout(ctx, deleteTimeout)
			lib.logf(ctx, "[events] deleting consumer group %s", broadcastConsumerGroup)
			resp, err := client.DeleteGroups(ctxWithTimeout, &kafka.DeleteGroupsRequest{
				Addr:     client.Addr,
				GroupIDs: []string{lib.addPrefix(string(broadcastConsumerGroup))},
			})
			cancelCtx()
			switch {
			case err == nil:
				// great
			case isTransientCoordinatorError(err):
				if !backoff.Continue(b) {
					return errors.Errorf("could not start broadcast consumer, delete consumer group (%s): %w", broadcastConsumerGroup, err)
				}
				_ = lib.RecordErrorNoWait(ctx, "timeout-consume-broadcast", errors.Errorf("delete consumer group (%s) failed: %w", broadcastConsumerGroup, err))
				continue
			default:
				return errors.Errorf("could not delete broadcast to reset before consume: %w", err)
			}
			err = resp.Errors[string(broadcastConsumerGroup)]
			switch {
			case err == nil, errors.Is(err, kafka.GroupIdNotFound), errors.Is(err, kafka.GroupCoordinatorNotAvailable):
				// Group was deleted or doesn't exist
				return nil
			case errors.Is(err, kafka.NonEmptyGroup):
				// Sometimes we get a NonEmptyGroup error when a consumer group was
				// recently used.
				lib.logf(ctx, "[events] tried consumer group (%s), but could not use it: %s", broadcastConsumerGroup, err)
				return errGroupUnavailable.Errorf("group (%s) is not empty: %w", broadcastConsumerGroup, err)
			case isTransientCoordinatorError(err):
				if triesThisOne < broadcastNotCoordinatorErrorRetries {
					lib.logf(ctx, "[events] got lock on consumer group (%s), but could not use it, retrying: %s", broadcastConsumerGroup, err)
					time.Sleep(time.Millisecond * 500)
					triesThisOne++
					continue
				}
				lib.logf(ctx, "[events] tried consumer group (%s), but could not use it, not retrying: %s", broadcastConsumerGroup, err)
				return errors.Errorf("not the coordinator for group (%s): %w", broadcastConsumerGroup, err)
			default:
				return errors.Errorf("could not delete broadcast consumer group to reset before consume: %w", err)
			}
		case errors.Is(err, kafka.GroupIdNotFound), errors.Is(err, kafka.GroupCoordinatorNotAvailable):
			// skip the delete, but we'll have to create the client later
			return nil
		case errors.Is(err, kafka.RequestTimedOut):
			if !backoff.Continue(b) {
				return errors.Errorf("could not start broadcast consumer, failed to get coordinator for group (%s): %w", broadcastConsumerGroup, err)
			}
			_ = lib.RecordErrorNoWait(ctx, "timeout-consume-broadcast", errors.Errorf("failed to get coordinator for group (%s): %w", broadcastConsumerGroup, err))
			continue
		default:
			return err
		}
	}
}

// isTransientCoordinatorError is used only in the context of broadcast groups where the group may or
// may not exist
func isTransientCoordinatorError(err error) bool {
	switch {
	case errors.Is(err, kafka.GroupIdNotFound),
		errors.Is(err, kafka.GroupCoordinatorNotAvailable),
		errors.Is(err, kafka.NotCoordinatorForGroup),
		errors.Is(err, kafka.RequestTimedOut), // transiant because we use short timeouts on most requests
		errors.Is(err, kafka.GroupLoadInProgress),
		errors.Is(err, kafka.RebalanceInProgress):
		return true
	default:
		return false
	}
}

// GetBroadcastConsumerGroupName this will be "" until a broadcast consumer group is figured out
func (lib *Library[ID, TX, DB]) GetBroadcastConsumerGroupName() string {
	return string(lib.broadcastConsumerGroupName.Load())
}

// sendBroadcastHeartbeat makes sure that there is always recent traffic for the
// broadcast consumers to consume. If there is traffic from another source, then
// it does not need to do anything at all. There are two reasons to make sure that
// there is always recent broadcast traffic:
//
//   - It allows the broadcast consumer to have a much shorter wait time before
//     deciding that the consumer is hung. This is important because the broadcast
//     consumers are often used for time-sensative things like cache invalidation.
//   - It allows the broadcast consumer to know if it is healthy. Caches could do
//     time-based expiration if the broadcast consumer is not healthy.
func (lib *Library[ID, TX, DB]) sendBroadcastHeartbeat(ctx context.Context, groupDone *sync.WaitGroup) {
	ctx, spanDone := lib.tracerConfig.BeginSpan(ctx, map[string]string{
		"action": "thread",
		"thread": "broadcast heartbeat",
	})
	defer spanDone()
	timer := time.NewTimer(time.Hour * 10000)
	defer func() {
		_ = timer.Stop()
		if debugConsumeStartup || debugShutdown {
			lib.logf(ctx, "[events] Debug shutdown: groupDone -1 for broadcast heartbeat")
		}
		groupDone.Done()
	}()
	b := backoffPolicy.Start(ctx)
	var lastSend time.Time
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// gap is the minimum of how long since we've either received or sent a broadcast.
		// We use it to avoid sending a heartbeat if we've either received or sent
		// a heartbeat recently.
		gap, _ := lib.BroadcastConsumerLastLatency()
		sinceLastSend := time.Since(lastSend) // on the first iteration, this is huge
		if sinceLastSend < gap {
			gap = sinceLastSend
		}
		// wantHB is a somewhat random interval after which we send a heartbeat.
		// It's somewhat random because if it wasn't then different servers could all
		// send at the same time if they all received something at the same time.
		wantHB := lib.pickHeartbeat()
		if gap < wantHB {
			// too soon to send, sleep for a bit
			timer.Reset(wantHB)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			// if nothing has been received in the meantime, the gap will now be larger and
			// for the same wantHB would fall through and send a heartbeat. The wantHB might
			// be longer or shorter. If longer, we'll wait some more. If shorter, we'll send
			// right away.
			continue
		}
		lib.logf(ctx, "[events] sending broadcast heartbeat to %s (%s, %s)", heartbeatTopic.Topic(), gap, wantHB)
		err := lib.Produce(ctx, eventmodels.ProduceImmediate, heartbeatTopic.Event(uuid.New().String(), HeartbeatEvent{}))
		if err == nil {
			b = backoffPolicy.Start(ctx)
			lastSend = time.Now()
		} else if !backoff.Continue(b) {
			// We use backoff so that if sending fails we don't keep retrying in a tight loop
			// Continue will only be false if the context has expired
			return
		}
	}
}

// We randomize the heartbeat somewhat so that different servers don't end up aligned
// with each other and send out heartbeats at the same time
func (lib *Library[ID, TX, DB]) pickHeartbeat() time.Duration {
	const big = 1000000000
	factor := 1.0 - float64(rand.Int63n(int64(big*lib.heartbeatRandomness)))/big
	randomized := time.Duration(float64(lib.broadcastHeartbeat) * factor)
	return randomized
}

type lockIDAllocator struct {
	initialBatchSize uint32
	increment        uint32
	multiplier       float64
	limit            uint32
	untried          []uint32
	max              uint32
}

func newIDAllocator(initialBatchSize uint32, increment uint32, multiplier float64, limit uint32) *lockIDAllocator {
	if initialBatchSize == 0 && increment == 0 {
		panic("invalid id allocator configuration")
	}
	return &lockIDAllocator{
		untried:          make([]uint32, 0, int(initialBatchSize)*10),
		max:              0,
		initialBatchSize: initialBatchSize,
		increment:        increment,
		multiplier:       multiplier,
		limit:            limit,
	}
}

func (a *lockIDAllocator) get() (uint32, error) {
	a.grow()
	if len(a.untried) == 0 {
		return 0, errors.Errorf("could not get broadcast consumer lock -- ran out of possible lock ids (%d)", a.limit)
	}
	i := rand.Intn(len(a.untried))
	n := a.untried[i]
	a.untried[i] = a.untried[len(a.untried)-1]
	a.untried = a.untried[:len(a.untried)-1]
	return n, nil
}

func (a *lockIDAllocator) grow() {
	var n uint32
	r := uint32(float64(a.max) * a.multiplier)
	switch {
	case a.max == 0 && a.initialBatchSize > 0:
		n = a.initialBatchSize
	case a.max >= a.limit:
		return
	case r > a.increment:
		n = a.max + r
	default:
		n = a.max + a.increment
	}
	if n > a.limit {
		n = a.limit
	}
	for i := a.max; i < n; i++ {
		a.untried = append(a.untried, i)
	}
	a.max = n
}

func broadcastTopics(unprefixedTopics []string) []string {
	if !generic.SliceContainsElement(unprefixedTopics, heartbeatTopic.Topic()) {
		// we don't actually need a handler, just need to subscribe to the heartbeat topic
		unprefixedTopics = append(generic.CopySlice(unprefixedTopics), heartbeatTopic.Topic())
	}
	return unprefixedTopics
}

// getReader only returns once the reader is actually connected working. This is determined
// by calling Stats()
// The only error that getReader returns is from context cancellation
func (lib *Library[ID, TX, DB]) getReader(ctx context.Context, consumerGroup consumerGroupName, prefixedTopics []string, isBroadcast bool, resetPosition bool) (*kafka.Reader, *kafka.ReaderStats, *kafka.ReaderConfig, error) {
	readerConfig := kafka.ReaderConfig{
		Brokers:                lib.brokers,
		GroupID:                lib.addPrefix(consumerGroup.String()),
		GroupTopics:            prefixedTopics,
		MaxBytes:               maxBytes,
		Dialer:                 lib.dialer(),
		StartOffset:            kafka.FirstOffset,
		WatchPartitionChanges:  true,
		MaxAttempts:            6,                        // connection attempts, default is 3
		ReadLagInterval:        10 * time.Second,         // default is 0
		MaxWait:                10 * time.Second,         // default is 10s
		ReadBatchTimeout:       10 * time.Second,         // default is 10s
		PartitionWatchInterval: 5 * time.Second,          // default is 5s
		CommitInterval:         0,                        //  default is 0, synchronous
		HeartbeatInterval:      3 * time.Second,          // default is 3s
		ReadBackoffMin:         100 * time.Millisecond,   // default is 100ms
		ReadBackoffMax:         1 * time.Second,          // default is 1s
		RetentionTime:          21 * 86400 * time.Second, // how long to remember the consumer group; 21 days, default is 7 days
	}
	if isBroadcast {
		readerConfig.RetentionTime = broadcastReaderIdleTimeout * 2 // forget this consumer group quickly when inactive
		if resetPosition {
			if debugConsumeStartup {
				lib.logf(ctx, "[events] Debug: consume %s setting start offset = last offset", consumerGroup)
			}
			readerConfig.StartOffset = kafka.LastOffset
		} else if debugConsumeStartup {
			lib.logf(ctx, "[events] Debug: consume %s setting start offset = current offset", consumerGroup)
		}
	}
	reader := kafka.NewReader(readerConfig)
	startTime := time.Now()
	lastReport := startTime
	timer := time.NewTimer(readerStartupSleep)
	for {
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, nil, errors.WithStack(ctx.Err())
		case <-timer.C:
		}
		stats := reader.Stats()
		if stats.Partition != "" {
			lib.logf(ctx, "[events] consumer group %s reader, for topics %v, started after %s", consumerGroup, readerConfig.GroupTopics, time.Since(startTime))
			return reader, &stats, &readerConfig, nil
		}
		if time.Since(lastReport) > readerStartupReport {
			lastReport = time.Now()
			lib.logf(ctx, "[events] consumer group %s reader, for topics %v, not yet started, waiting %s",
				consumerGroup, readerConfig.GroupTopics, time.Since(startTime))
		}
		timer.Reset(readerStartupSleep)
	}
}
