// Package eventtest has abstract tests to validate database implementations
package eventtest

import (
	"context"
	"os"
	"regexp"
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

type Prefix string

type Brokers []string

func KafkaBrokers(t T) Brokers {
	brokers := os.Getenv("EVENTS_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("EVENTS_KAFKA_BROKERS must be set to run this test")
	}
	return Brokers(strings.Split(brokers, " "))
}

var CommonInjectors = nject.Sequence("common",
	nject.Required(func(t T) {
		t.Logf("starting %s", t.Name())
	}),
	nject.Provide("context", context.Background),
	nject.Required(nject.Provide("Report-results", func(inner func(), t T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("RESULT: %s FAILED w/panic", t.Name())
				panic(r)
			}
			switch {
			case t.Failed():
				t.Logf("RESULT: %s FAILED", t.Name())
			case t.Skipped():
				t.Logf("RESULT: %s SKIPPED", t.Name())
			default:
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

var squashRE = regexp.MustCompile(`[^A-Z]+`)

func Name(t ntest.T) string {
	n := t.Name()
	x := strings.LastIndexByte(n, '/')
	if x == -1 {
		return n
	}
	after := n[x+1:]
	before := n[:x]
	return squashRE.ReplaceAllString(before, "") + after
}

type MyEvent struct {
	S string
}

var (
	DeliveryTimeout = LongerOnCI(60*time.Second, 10*time.Minute, 4*time.Minute)
	StartupTimeout  = LongerOnCI(85*time.Second, 7*time.Minute, 125*time.Second)
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
		"OversizeSendTest":         nject.Provide("OST", OversizeSendTest[ID, TX, DB]),
		"UnfilteredNotifier":       nject.Provide("UN", EventUnfilteredNotifierTest[ID, TX, DB]),
	}
}
