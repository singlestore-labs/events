package eventtest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/memsql/ntest"
	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/wait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type myNotifierEvent map[string]string

var (
	// We protect these variables because the test can run more than once but must have the same values
	// We don't do this in init because it causes side effects (broadcast consumers) that may or may not be running
	unfilteredBindLock      sync.Mutex
	unfilteredNotifierTopic eventmodels.BoundTopic[myNotifierEvent]
	unfiltered3             events.Unfiltered[myNotifierEvent]
)

func EventUnfilteredNotifierTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
) {
	func() {
		unfilteredBindLock.Lock()
		defer unfilteredBindLock.Unlock()
		if unfilteredNotifierTopic == "" {
			unfilteredNotifierTopic = eventmodels.BindTopic[myNotifierEvent]("TestEventUnfilteredNotifier")
			unfiltered3 = events.RegisterUnfiltered("TestEventUnfilteredNotifier-registration", unfilteredNotifierTopic, events.Async(10))
		}
	}()

	origT := t
	t = ntest.ExtraDetailLogger(origT, "UNT")
	lib := events.New[ID, TX, DB]()
	if !IsNilDB(conn) {
		lib.SetEnhanceDB(true)
		conn.AugmentWithProducer(lib)
	}
	lib.Configure(conn, ntest.ExtraDetailLogger(origT, "UNT-L"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)

	consumeDone := lib.StartConsumingOrPanic(ctx)

	allChan := unfiltered3.Subscribe(lib)

	id := uuid.New().String()
	t.Log("producing may take a few tries for brand new topics")
	require.NoError(t, wait.For(func() (bool, error) {
		err := lib.Produce(ctx, eventmodels.ProduceImmediate, unfilteredNotifierTopic.Event("key-"+id,
			myNotifierEvent{"foo": "bar"}).ID(id))
		if err != nil {
			t.Logf("got error trying to produce: %v", err)
			return false, err
		}
		return true, nil
	}, wait.ExitOnError(false), wait.WithLimit(time.Second*60)))

	timer := time.NewTimer(time.Minute * 3)
Wait:
	for {
		select {
		case <-allChan.WaitChan():
			event := allChan.Consume()
			if event.ID == id {
				t.Log("received the event we were looking for")
				timer.Stop()
				break Wait
			}
			t.Logf("received an event with a different id, ignoring %s", event.ID)
		case <-timer.C:
			t.Log("timed out, giving up")
		}
	}
	t.Log("waiting for shutdown")
	allChan.Unsubscribe()
	cancel()
	<-consumeDone
}

var notifierTopic = eventmodels.BindTopic[myNotifierEvent]("TestEventComprehensiveNotifier")

var (
	// We protect these variables because the test can run more than once but must have the same values
	// We don't do this in init because it causes side effects (broadcast consumers) that may or may not be running
	comprehensiveBindLock sync.Mutex
	filtered              events.Filtered[string, myNotifierEvent]
	unfiltered1           events.Unfiltered[myNotifierEvent]
	unfiltered2           events.Unfiltered[myNotifierEvent]
)

func EventComprehensiveNotifierTest[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
](
	ctx context.Context,
	t ntest.T,
	conn DB,
	brokers Brokers,
	cancel Cancel,
) {
	if IsNilDB(conn) {
		// In theory this test could run w/o a database, but it would likely fail due to timing issues
		t.Skipf("%s requires a database", t.Name())
	}
	// The order of filtered & unfiltered matters here because broadcast events
	// are delivered synchronously and the inner unfiltered receivers wait for the
	// filtered receiver to have triggered before they consume. If unfiltered is
	// first, the test deadlocks.

	func() {
		comprehensiveBindLock.Lock()
		defer comprehensiveBindLock.Unlock()
		if filtered == nil {
			filtered = events.RegisterFiltered("TestEventComprehensiveNotifier-filtered", notifierTopic, func(e eventmodels.Event[myNotifierEvent]) string {
				return e.Payload["quarter"]
			})
			unfiltered1 = events.RegisterUnfiltered("TestEventComprehensiveNotifier-unfiltered1", notifierTopic, events.Async(10))
			unfiltered2 = events.RegisterUnfiltered("TestEventComprehensiveNotifier-unfiltered2", notifierTopic, events.Async(-1))
		}
	}()

	const threadCount = 10
	const requiredIterations = 3
	const stopAfter = (threadCount*requiredIterations*2 + 1) * 5
	const stepsPerIteration = 4 // must be 2 or more
	sendSleep := time.Millisecond * 2

	origT := t
	t = ntest.ExtraDetailLogger(origT, "TEN-O")
	lib := events.New[ID, TX, DB]()
	lib.SetEnhanceDB(true)
	conn.AugmentWithProducer(lib)
	lib.Configure(conn, ntest.ExtraDetailLogger(origT, "TEN-L"), false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)

	// slowCtx is for the library
	slowCtx, slowCtxCancel := context.WithCancel(ctx)

	produceDone, err := lib.CatchUpProduce(slowCtx, time.Second*5, 64)
	require.NoError(t, err)

	consumeDone := lib.StartConsumingOrPanic(slowCtx)

	// ctx will be used for the threads
	ctx, cancelCtx := context.WithCancel(ctx)

	type rdata struct {
		event eventmodels.Event[myNotifierEvent]
		where string
	}

	received := make([]rdata, 0, stopAfter*4)
	var lock sync.Mutex

	var allConditions sync.WaitGroup
	allConditions.Add(requiredIterations)

	noteEvent := func(t ntest.T, event eventmodels.Event[myNotifierEvent], where string) {
		t.Logf("noting event from %s: %s", where, event.ID)
		lock.Lock()
		defer lock.Unlock()
		received = append(received, rdata{
			event: event,
			where: where,
		})
		if len(received) == stopAfter {
			t.Log("AllConditions: enough events received")
			allConditions.Done()
		}
	}

	go func() {
		allConditions.Wait()
		t.Log("AllConditions: done! Cancelling context")
		cancelCtx()
	}()

	var wg sync.WaitGroup

	var recvCount atomic.Int32
	allChan := unfiltered1.Subscribe(lib)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer allChan.Unsubscribe()
		//nolint:govet // shadows t on purpose
		t := ntest.ExtraDetailLogger(origT, "TEN-ALL")
		for {
			select {
			case <-allChan.WaitChan():
				noteEvent(t, allChan.Consume(), "all")
				recvCount.Add(1)
			case <-ctx.Done():
				t.Log("done (ctx cancelled)")
				return
			}
		}
	}()

	var sentCount atomic.Int32

	sendPermission := make(chan string, stopAfter/stepsPerIteration+1)
	sendPermission <- "initial"

	wg.Add(1)
	go func() {
		defer wg.Done()
		//nolint:govet // shadows t on purpose
		t := ntest.ExtraDetailLogger(origT, "TEN-SEND")
		t.Log("starting to send")
		var perm string
		for i := 0; i < stopAfter; i++ {
			if i%stepsPerIteration == 0 {
				t.Logf("waiting to send %d/%d", i, stopAfter)
				select {
				case perm = <-sendPermission:
					t.Logf("got permission to send from %s", perm)
				case <-ctx.Done():
					t.Log("done (ctx cancelled)")
					return
				}
			}
			id := fmt.Sprintf("%03d-%s", i, uuid.New().String())
			t.Logf("sending event %s (%d) with permission from %s", id, i%4, perm)
			sendTime := time.Now()
			var txTime time.Duration // we won't count this
			require.NoErrorf(t, conn.Transact(slowCtx, func(tx TX) error {
				start := time.Now()
				tx.Produce(notifierTopic.Event(id, myNotifierEvent{
					"id":      id,
					"quarter": fmt.Sprintf("%d", i%stepsPerIteration),
				}).ID(id))
				txTime = time.Since(start)
				return nil
			}), "transact/send")
			duration := time.Since(sendTime) - txTime
			if !assert.Lessf(t, duration, time.Second, "tx should be fast, not %s", duration) {
				cancel()
				return
			}
			sentCount.Add(1)
			time.Sleep(sendSleep)
			sendSleep = time.Duration(float64(sendSleep) * 1.02)
			select {
			case <-ctx.Done():
				t.Log("done (ctx cancelled)")
				return
			default:
			}
		}
		t.Log("send complete")
	}()

	for i := 1; i <= threadCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			//nolint:govet // shadows t on purpose
			t := ntest.ExtraDetailLogger(origT, fmt.Sprintf("TEN-%dT", i))
			for j := 1; j < requiredIterations+2; j++ {
				t.Logf("starting iteration %d", j)
				filteredReader := filtered.Subscribe(lib, "1")
				gotOne := make(chan struct{})
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					defer close(gotOne)
					defer filteredReader.Unsubscribe()
					//nolint:govet // shadows t on purpose
					t := ntest.ExtraDetailLogger(origT, fmt.Sprintf("TEN-flt-%dT-%d", i, j))
					t.Log("waiting")
					if j <= requiredIterations {
						select {
						case sendPermission <- fmt.Sprintf("flt-%dT-%d", i, j):
						default:
							t.Log("could not give sendPermission")
						}
					}
					select {
					case <-filteredReader.WaitChan():
						e := filteredReader.Consume()
						assert.Equal(t, "1", e.Payload["quarter"], "quarter payload")
						noteEvent(t, e, fmt.Sprintf("once-%dT-%d", i, j))
						if i == 1 && j == 2 {
							t.Log("AllConditions: once 1/2 received")
							allConditions.Done()
						}
					case <-ctx.Done():
						t.Log("ctx cancelled")
					}
					t.Log("done")
				}(j)

				unfilteredReader := unfiltered2.Subscribe(lib)
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					defer unfilteredReader.Unsubscribe()
					//nolint:govet // shadows t on purpose
					t := ntest.ExtraDetailLogger(origT, fmt.Sprintf("TEN-unfl-%dT-%d", i, j))
					t.Log("sequence-two-S0")
					select {
					case <-gotOne:
					case <-ctx.Done():
						t.Log("done (ctx cancelled)")
						return
					}
					if j <= requiredIterations {
						select {
						case sendPermission <- fmt.Sprintf("unfl-%dT-%d", i, j):
						default:
							t.Log("could not give sendPermission")
						}
					}
					for s := 1; s <= stepsPerIteration; s++ {
						t.Logf("sequence-two-S%d", s)
						select {
						case <-unfilteredReader.WaitChan():
							e := unfilteredReader.Consume()
							noteEvent(t, e, fmt.Sprintf("two-%dT-%d-A", i, j))
						case <-ctx.Done():
							t.Log("done (ctx cancelled)")
							return
						}
					}
					t.Log("sequence-two-SF")
					if i == 2 && j == 3 {
						t.Log("AllConditions: D received in 2/3")
						allConditions.Done()
					}
					t.Log("done")
				}(j)

				t.Logf("waiting to start next iteration %d", j)
				select {
				case <-gotOne:
				case <-ctx.Done():
					t.Log("done (ctx cancelled)")
					return
				}
			}
		}(i)
	}
	assert.Zero(t, lib.ProduceSyncCount.Load())
	t.Log("wait for context cancel")
	<-ctx.Done()
	t.Log("wait for thread shutdown")
	wg.Wait()
	t.Log("stopping event library")
	slowCtxCancel()
	t.Log("wait for produce done")
	<-produceDone
	t.Log("wait for consume done")
	<-consumeDone
	t.Log("everything is stopped")
}
