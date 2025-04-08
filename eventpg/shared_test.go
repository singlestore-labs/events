package eventpg_test

import (
	"database/sql"
	"testing"

	"github.com/memsql/ntest"
	"github.com/muir/nject"

	"singlestore.com/helios/data"
	"singlestore.com/helios/events"
	"singlestore.com/helios/events/eventdb"
	"singlestore.com/helios/events/eventmodels"
	"singlestore.com/helios/events/eventpg"
	"singlestore.com/helios/events/eventtest"
	"singlestore.com/helios/test/di"
)

func realSql(conn *data.Connection) *sql.DB {
	return conn.DB.DB
}

var chain = nject.Sequence("tests",
	di.IntegrationSequenceBeforeUser,
	di.IntegrationSequenceUserAndMore,
	nject.Required(realSql),
	pgconn,
)

type DBType = eventpg.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]

func pgconn(db *sql.DB) *DBType {
	wrapped := eventdb.ExampleBasicDB{
		DB: db,
	}
	eventpgConnection := eventpg.New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](wrapped)
	return eventpgConnection
}

type LibraryType = events.Library[eventmodels.StringEventID, *eventdb.ExampleBasicTX, *eventpg.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]]

func TestSharedEventPG(t *testing.T) {
	ntest.RunParallelMatrix(t,
		chain,
		eventtest.GenerateSharedTestMatrix[eventmodels.StringEventID, *eventdb.ExampleBasicTX, *eventpg.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]](),
	)
}
