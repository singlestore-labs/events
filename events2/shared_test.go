package events2_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lssinglestore"
	"github.com/muir/nject"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"singlestore.com/helios/events"
	"singlestore.com/helios/events/eventdb"
	"singlestore.com/helios/events/eventmodels"
	"singlestore.com/helios/events/events2"
	"singlestore.com/helios/events/eventtest"
	"singlestore.com/helios/test/di"
)

/*

 Set up to run tests...

 EVENTS_S2PASSWORD="chooseSomething"

 docker run \
    -d --name singlestoredb-dev \
    -e ROOT_PASSWORD="$EVENTS_S2PASSWORD" \
    -p 13306:3306 -p 18080:8080 -p 19000:9000 \
    ghcr.io/singlestore-labs/singlestoredb-dev:latest

 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "CREATE DATABASE eventstest PARTITIONS 2"
 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "set global snapshots_to_keep = 1"
 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "set global minimal_disk_space = 10"
 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "set global log_file_size_partitions = 1048576"
 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "set global log_file_size_ref_dbs = 1048576"

 # check the compatibility level....
 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "select @@data_conversion_compatibility_level"
 # if it's 8.0 then:
 mysql -u root --password="$EVENTS_S2PASSWORD" -h 127.0.0.1 -P 13306 -e "SET GLOBAL data_conversion_compatibility_level = '7.0'"


 EVENTS_S2TEST_DSN="root:${EVENTS_S2PASSWORD}@tcp(127.0.0.1:13306)/eventstest?tls=false&parseTime=true&loc=UTC" go test events/s2event

*/

func env(n string, dflt string) string {
	if s := os.Getenv(n); s != "" {
		return s
	}
	return dflt
}

func realSql(t ntest.T, ctx context.Context) *sql.DB {
	dsn := env("EVENTS_S2TEST_DSN", "")
	if dsn == "" {
		t.Skip("must set EVENTS_S2TEST_DSN to run this test")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)

	schema := libschema.New(ctx, libschema.Options{})

	database, singlestore, err := lssinglestore.New(
		libschema.LogFromLogur(testinglogur.Get(t)),
		"eventstest",
		schema,
		db)
	require.NoError(t, err)

	events2.Migrations(database, singlestore)

	err = schema.Migrate(ctx)
	require.NoError(t, err)

	return db
}

var chain = nject.Sequence("tests",
	di.IntegrationSequenceBeforeUser,
	di.IntegrationSequenceUserAndMore,
	nject.Required(realSql),
	s2conn,
)

type DBType = events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]

func s2conn(db *sql.DB) *DBType {
	wrapped := eventdb.ExampleBasicDB{
		DB: db,
	}
	return events2.New[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB](wrapped, "lockPrefix")
}

type LibraryType = events.Library[eventmodels.BinaryEventID, *eventdb.ExampleBasicTX, *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]]

func TestSharedEventS2(t *testing.T) {
	ntest.RunParallelMatrix(t,
		chain,
		eventtest.GenerateSharedTestMatrix[eventmodels.BinaryEventID, *eventdb.ExampleBasicTX, *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]](),
	)
}

func TestLockingLockOrDie(t *testing.T) {
	t.Parallel()
	ntest.RunTest(t, chain, func(
		ctx context.Context,
		db *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB],
	) {
		const threads = 10

		ctx, cancel := context.WithCancel(ctx)

		var got atomic.Uint32
		var failed atomic.Uint32
		var wg sync.WaitGroup
		allDone := make(chan struct{})

		for i := 0; i < threads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				done, err := db.LockOrError(ctx, 328, 0)
				defer func() {
					_ = done()
				}()
				if errors.Is(err, eventmodels.TimeoutErr) {
					t.Logf("failed to get lock in %d: timeout", i)
					failed.Add(1)
					return
				}
				t.Logf("got lock in %d", i)
				got.Add(1)
				<-allDone
			}()
		}
		time.Sleep(5 * time.Second)
		close(allDone)
		cancel()
		wg.Wait()
		require.Equal(t, 1, int(got.Load()), "got")
		require.Equal(t, threads-1, int(failed.Load()), "failed")
	})
}
