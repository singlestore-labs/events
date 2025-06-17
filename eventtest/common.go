// Package eventtest has abstract tests to validate database implementations
package eventtest

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/memsql/ntest"
	"github.com/muir/nject/v2"
	"github.com/singlestore-labs/once"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/internal"
)

// --------- begin section that is duplicated in consumer_group_test.go -----------

type T = ntest.T

type Brokers []string

func KafkaBrokers(t T) Brokers {
	brokers := strings.Split(os.Getenv("EVENTS_KAFKA_BROKERS"), " ")
	if len(brokers) == 0 {
		t.Skip("EVENTS_KAFKA_BROKERS must be set to run this test")
	}
	return Brokers(brokers)
}

var CommonInjectors = nject.Sequence("common",
	nject.Provide("context", context.Background),
	nject.Required(nject.Provide("Report-results", func(inner func(), t T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("RESULT: %s FAILED w/panic", t.Name())
				panic(r)
			}
			if t.Failed() {
				t.Logf("RESULT: %s FAILED", t.Name())
			} else {
				t.Logf("RESULT: %s PASSED", t.Name())
			}
		}()
		inner()
	})),
	nject.Provide("cancel", AutoCancel),
	nject.Provide("brokers", KafkaBrokers),
)

type Cancel func()

func AutoCancel(ctx context.Context, t T) (context.Context, Cancel) {
	ctx, cancel := context.WithCancel(ctx)
	onlyOnce := once.New(cancel)
	t.Cleanup(onlyOnce.Do)
	return ctx, onlyOnce.Do
}

// --------- end section that is duplicated in consumer_group_test.go -----------

type AugmentAbstractDB[ID eventmodels.AbstractID[ID], TX eventmodels.AbstractTX] interface {
	eventmodels.AbstractDB[ID, TX]
	eventmodels.CanAugment[ID, TX]
}

func Name(t ntest.T) string {
	x := strings.Split(t.Name(), "/")
	return x[len(x)-1]
}

type MyEvent struct {
	S string
}

var (
	DeliveryTimeout = LongerOnCI(20*time.Second, 10*time.Minute, 2*time.Minute)
	StartupTimeout  = LongerOnCI(65*time.Second, 5*time.Minute, 65*time.Second)
)

func IsNilDB[DB any](db DB) bool {
	return internal.IsNil(db)
}

func LongerOnCI(local, gitlab, github time.Duration) time.Duration {
	switch {
	case os.Getenv("GITLAB_CI") != "":
		return gitlab
	case os.Getenv("GITHUB_ACTIONS") != "":
		return github
	default:
		return local
	}
}

func WaitFor(ctx context.Context, t ntest.T, what string, start chan struct{}, maxWait time.Duration) {
	t.Logf("waiting for %s", what)
	select {
	case <-ctx.Done():
		t.Logf("aborted waiting for %s -- context cancelled", what)
		return
	case <-time.After(maxWait):
		require.FailNow(t, "timeout", "aborted waiting for %s -- timeout", what)
	case <-start:
		t.Logf("Done waiting for %s", what)
		return
	}
}

type NewLibFunc[ID eventmodels.AbstractID[ID], TX eventmodels.EnhancedTX, DB AugmentAbstractDB[ID, TX]] func() *events.Library[ID, TX, DB]

func GenerateSharedTestMatrix[
	ID eventmodels.AbstractID[ID],
	TX eventmodels.EnhancedTX,
	DB AugmentAbstractDB[ID, TX],
]() map[string]nject.Provider {
	return map[string]nject.Provider{
		"BatchDeliveryBroadcast":   nject.Provide("BDB", BatchDeliveryBroadcastTest[ID, TX, DB]),
		"BatchDeliveryExactlyOnce": nject.Provide("BDEO", BatchDeliveryExactlyOnceTest[ID, TX, DB]),
		"BatchDeliveryIdempotent":  nject.Provide("BDI", BatchDeliveryIdempotentTest[ID, TX, DB]),
		"BroadcastDelivery":        nject.Provide("BCD", BroadcastDeliveryTest[ID, TX, DB]),
		"CloudEventEncoding":       nject.Provide("CEET", CloudEventEncodingTest[ID, TX, DB]),
		"ComprehensiveNotifier":    nject.Provide("N", EventComprehensiveNotifierTest[ID, TX, DB]),
		"DeadLetterBlock":          nject.Provide("DLB", DeadLetterBlockTest[ID, TX, DB]),
		"DeadLetterDiscard":        nject.Provide("DLD", DeadLetterDiscardTest[ID, TX, DB]),
		"DeadLetterRetryLater":     nject.Provide("DLRL", DeadLetterRetryLaterTest[ID, TX, DB]),
		"DeadLetterSave":           nject.Provide("DLS", DeadLetterSaveTest[ID, TX, DB]),
		"ErrorWhenMisused":         nject.Provide("EWM", ErrorWhenMisusedTest[ID, TX, DB]),
		"ExactlyOnceDelivery":      nject.Provide("EOD", ExactlyOnceDeliveryTest[ID, TX, DB]),
		"IdempotentDelivery":       nject.Provide("ID", IdempotentDeliveryTest[ID, TX, DB]),
		"OrderedBlock1CG":          nject.Provide("OB1", OrderedBlockTestOneCG[ID, TX, DB]),
		"OrderedBlock2CG":          nject.Provide("OB2", OrderedBlockTestTwoCG[ID, TX, DB]),
		"OrderedRetryLater1CG":     nject.Provide("ORL1", OrderedRetryTestOncCG[ID, TX, DB]),
		"OrderedRetryLater2CG":     nject.Provide("ORL2", OrderedRetryTestTwoCG[ID, TX, DB]),
		"UnfilteredNotifier":       nject.Provide("UN", EventUnfilteredNotifierTest[ID, TX, DB]),
	}
}
