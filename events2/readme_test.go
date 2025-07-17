package events2_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventdb"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/events2"
	"github.com/singlestore-labs/events/eventtest"
	"github.com/stretchr/testify/require"
)

func TestReadmeCompiles(t *testing.T) {
	t.Log("This validates the example in the readme compiles, update this test if you change the readme and vice versa")
	dsn := os.Getenv("EVENTS_S2TEST_DSN")
	if dsn == "" {
		t.Skip("must set EVENTS_S2TEST_DSN to run this test, it compiles, so that's probably enough")
	}
	brokers := []string(eventtest.KafkaBrokers(t))

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	eventLib := events.New[eventmodels.BinaryEventID, *eventdb.ExampleBasicTX, *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]]()
	conn := events2.New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](eventdb.ExampleBasicDB{
		DB: db,
	}, "lockPrefix")

	eventLib.Configure(conn, t, false, events.SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
}
