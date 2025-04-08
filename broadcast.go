package events

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/backoff/v2"
	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"singlestore.com/helios/events/eventmodels"
	"singlestore.com/helios/util/simultaneous"
)

type HeartbeatEvent struct{}

var heartbeatTopic = eventmodels.BindTopic[HeartbeatEvent]("event-library.heartbeat.broadcast.v1")

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
func (lib *Library[ID, TX, DB]) consumeBroadcast(ctx context.Context, allStarted *sync.WaitGroup, allDone *sync.WaitGroup) error {
	var broadcastConsumerGroup consumerGroupName
NextPotential:
	for potentialLock := uint32(0); ; potentialLock++ {
		if potentialLock > lib.broadcastConsumerMaxLock {
			return errors.Errorf("could not get broadcast consumer lock -- ran out of possible lock ids (%d)", lib.broadcastConsumerMaxLock)
		}
		b := backoffPolicy.Start(ctx)
		var triesThisOne int
		var unlock func() error
	RetryThisOne:
		for {
			triesThisOne++

			if unlock != nil {
				_ = unlock()
			}
			// We do not unlock on success
			var err error
			unlock, err = lib.db.LockOrError(ctx, potentialLock, 0)
			if err != nil {
				if errors.Is(err, eventmodels.TimeoutErr) {
					continue NextPotential
				}
				if errors.Is(err, eventmodels.NotImplementedErr) {
					return err
				}
				_ = lib.RecordError("lock-attempt", err)
				continue NextPotential
			}
			broadcastConsumerGroup = consumerGroupName(lib.broadcastConsumerBaseName + "-" + strconv.Itoa(int(potentialLock)))

			lib.tracer.Logf("[events] getting consumer group coordinator for %s", broadcastConsumerGroup)

			ctxWithTimeout, cancelCtx := context.WithTimeout(ctx, dialTimeout)
			client, err := lib.getConsumerGroupCoordinator(ctxWithTimeout, broadcastConsumerGroup)
			cancelCtx()
			if err != nil {
				if errors.Is(err, kafka.GroupIdNotFound) || errors.Is(err, kafka.GroupCoordinatorNotAvailable) {
					break NextPotential
				}
				if errors.Is(err, kafka.RequestTimedOut) {
					if !backoff.Continue(b) {
						return errors.Errorf("could not start broadcast consumer, failed to get coordinator for group (%s): %w", broadcastConsumerGroup, err)
					}
					_ = lib.RecordErrorNoWait("timeout-consume-broadcast", errors.Errorf("failed to get coordinator for group (%s): %w", broadcastConsumerGroup, err))
					continue
				}
				return err
			}

			// We'll keep trying until the delete succeeds
			ctxWithTimeout, cancelCtx = context.WithTimeout(ctx, deleteTimeout)
			lib.tracer.Logf("[events] deleting consumer group %s", broadcastConsumerGroup)
			resp, err := client.DeleteGroups(ctxWithTimeout, &kafka.DeleteGroupsRequest{
				Addr:     client.Addr,
				GroupIDs: []string{broadcastConsumerGroup.String()},
			})
			cancelCtx()
			if err != nil {
				if errors.Is(err, kafka.RequestTimedOut) {
					if !backoff.Continue(b) {
						return errors.Errorf("could not start broadcast consumer, delete consumer group (%s): %w", broadcastConsumerGroup, err)
					}
					_ = lib.RecordErrorNoWait("timeout-consume-broadcast", errors.Errorf("delete consumer group (%s) failed: %w", broadcastConsumerGroup, err))
					continue
				}
				return errors.Errorf("could not delete broadcast to reset before consume: %w", err)
			}
			err = resp.Errors[broadcastConsumerGroup.String()]
			if err != nil {
				if errors.Is(err, kafka.GroupIdNotFound) {
					break NextPotential
				}
				// Sometimes we get a NonEmptyGroup error when a consumer group was
				// recently used.
				if errors.Is(err, kafka.NonEmptyGroup) {
					lib.tracer.Logf("[events] got lock on consumer group (%s), but could not use it: %s", broadcastConsumerGroup, err)
					continue NextPotential
				}
				if errors.Is(err, kafka.NotCoordinatorForGroup) {
					if triesThisOne < 50 {
						lib.tracer.Logf("[events] got lock on consumer group (%s), but could not use it, retrying: %s", broadcastConsumerGroup, err)
						time.Sleep(time.Millisecond * 500)
						continue RetryThisOne
					}
					lib.tracer.Logf("[events] got lock on consumer group (%s), but could not use it, not retrying: %s", broadcastConsumerGroup, err)
					continue NextPotential
				}
				return errors.Errorf("could not delete broadcast consumer group to reset before consume: %w", err)
			}
			break NextPotential
		}
	}
	lib.tracer.Logf("[events] using consumer group %s for receiving broadcasts", broadcastConsumerGroup)
	lib.broadcastConsumerGroupName.Store(broadcastConsumerGroup)
	limiter := simultaneous.New[eventLimiterType](maximumParallelConsumption)
	go lib.startConsumingGroup(ctx, broadcastConsumerGroup, lib.broadcast, limiter, true, allStarted, allDone, false)
	return nil
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
func (lib *Library[ID, TX, DB]) sendBroadcastHeartbeat(ctx context.Context, allDone *sync.WaitGroup) {
	timer := time.NewTimer(time.Hour * 10000)
	defer func() {
		_ = timer.Stop()
		if debugConsumeStartup {
			lib.tracer.Logf("[events] Debug: allDone -1 for broadcast heartbeat")
		}
		allDone.Done()
	}()
	b := backoffPolicy.Start(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		gap, _ := lib.BroadcastConsumerLastLatency()
		if wantHB := lib.pickHeartbeat(); gap < wantHB {
			timer.Reset(wantHB)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			continue // if nothing has been received in the meantime, the gap will now be larger
		}
		lib.tracer.Logf("[events] sending broadcast heartbeat to %s", heartbeatTopic.Topic())
		err := lib.Produce(ctx, eventmodels.ProduceImmediate, heartbeatTopic.Event(uuid.New().String(), HeartbeatEvent{}))
		if err == nil {
			b = backoffPolicy.Start(ctx)
		} else if !backoff.Continue(b) {
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
