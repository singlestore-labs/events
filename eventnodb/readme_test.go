package eventnodb_test

import (
	"os"
	"strings"
	"testing"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/eventnodb"
)

func TestReadmeCompiles(t *testing.T) {
	t.Log("This validates the example in the readme compiles, update this test if you change the readme")
	brokers := strings.Split(os.Getenv("EVENTS_KAFKA_BROKERS"), " ")
	if len(brokers) == 0 || brokers[0] == "" {
		t.Skip("EVENTS_KAFKA_BROKERS must be set to run this test")
	}

	eventLib := events.New[eventmodels.BinaryEventID, *eventnodb.NoDBTx, *eventnodb.NoDB]()
	eventLib.Configure(nil, t, false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
}
