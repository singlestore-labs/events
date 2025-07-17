package eventnodb_test

import (
	"os"
	"testing"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/eventnodb"
	"github.com/singlestore-labs/events/eventtest"
)

func TestReadmeCompiles(t *testing.T) {
	t.Log("This validates the example in the readme compiles, update this test if you change the readme and vice versa")
	brokers := []string(eventtest.KafkaBrokers(t))

	eventLib := events.New[eventmodels.BinaryEventID, *eventnodb.NoDBTx, *eventnodb.NoDB]()
	eventLib.Configure(nil, t, false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
}
