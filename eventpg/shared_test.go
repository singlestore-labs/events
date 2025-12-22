package eventpg_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/memsql/ntest"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/nject/v2"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventdb"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/eventpg"
	"github.com/singlestore-labs/events/eventtest"
)

/*

Set up to run tests with PostgreSQL. You also need Kafka, see eventnodb tests.

export PGPASSWORD=postgres
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD="$PGPASSWORD" --restart=always -d postgres:latest

psql -h localhost -p 5432 --user postgres <<END
CREATE DATABASE evtesting;
CREATE USER evtestuser WITH NOCREATEDB PASSWORD 'evtestpass';
GRANT ALL PRIVILEGES ON DATABASE evtesting TO evtestuser;
END

EVENTS_POSTGRES_TEST_DSN="postgresql://evtestuser:evtestpass@localhost:5432/evtesting?sslmode=disable"

*/

func sqlDB(ctx context.Context, t ntest.T) *sql.DB {
	dsn := os.Getenv("EVENTS_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $EVENTS_POSTGRES_TEST_DSN to run PostgreSQL tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "open database")
	t.Cleanup(func() {
		_ = db.Close()
	})

	schema := libschema.New(ctx, libschema.Options{})

	database, err := lspostgres.New(
		libschema.LogFromLogur(testinglogur.Get(t)),
		"eventstest",
		schema,
		db)
	require.NoError(t, err)

	eventpg.Migrations(database)

	err = schema.Migrate(ctx)
	require.NoError(t, err)
	return db
}

var chain = nject.Sequence("pgtest-injectors",
	eventtest.Prefix("PG_"),
	eventtest.CommonInjectors,
	sqlDB,
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
	ntest.RunParallelMatrix(ntest.BufferedLogger(t),
		chain,
		eventtest.GenerateSharedTestMatrix[eventmodels.StringEventID, *eventdb.ExampleBasicTX, *eventpg.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]](),
	)
}
