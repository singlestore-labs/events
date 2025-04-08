package eventtest

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/memsql/ntest"
	"github.com/muir/nject"
	"github.com/stretchr/testify/require"

	"singlestore.com/helios/events"
	"singlestore.com/helios/events/eventmodels"
)

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
	DeliveryTimeout = LongerOnCI(20*time.Second, 10*time.Minute)
	StartupTimeout  = LongerOnCI(time.Minute, 5*time.Minute)
)

func LongerOnCI(local, ci time.Duration) time.Duration {
	if os.Getenv("CI_PIPELINE_SOURCE") != "" {
		return ci
	}
	return local
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
		"CloudEventEncoding":   nject.Provide("CEET", CloudEventEncodingTest[ID, TX, DB]),
		"EventDelivery":        nject.Provide("ED", EventDeliveryTest[ID, TX, DB]),
		"DeadLetterSave":       nject.Provide("DLS", DeadLetterSaveTest[ID, TX, DB]),
		"DeadLetterBlock":      nject.Provide("DLB", DeadLetterBlockTest[ID, TX, DB]),
		"DeadLetterRetryLater": nject.Provide("DLRL", DeadLetterRetryLaterTest[ID, TX, DB]),
		"DeadLetterDiscard":    nject.Provide("DLD", DeadLetterDiscardTest[ID, TX, DB]),
		"ErrorWhenMisused":     nject.Provide("EWM", ErrorWhenMisusedTest[ID, TX, DB]),
		"BatchDelivery":        nject.Provide("BD", BatchDeliveryTest[ID, TX, DB]),
		"Notifier":             nject.Provide("N", EventNotifierTest[ID, TX, DB]),
	}
}
