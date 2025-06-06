package eventnodb_test

import (
	"os"
	"testing"

	"github.com/memsql/ntest"
	"github.com/muir/nject/v2"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/eventnodb"
	"github.com/singlestore-labs/events/eventtest"
)

var chain = nject.Sequence("nodb-injectors",
	eventtest.CommonInjectors,
	nject.Provide("nodb", func() *eventnodb.NoDB {
		return nil // Pass nil connection to trigger no-database behavior
	}),
)

func TestSharedEventNoDB(t *testing.T) {
	if os.Getenv("EVENTS_KAFKA_BROKERS") == "" {
		t.Skipf("%s requires kafka brokers", t.Name())
	}
	ntest.RunParallelMatrix(t,
		chain,
		eventtest.GenerateSharedTestMatrix[eventmodels.BinaryEventID, *eventnodb.NoDBTx, *eventnodb.NoDB](),
	)
}
