package events2_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lssinglestore"
	"github.com/muir/nject/v2"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
	"github.com/singlestore-labs/events/eventdb"
	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/events2"
	"github.com/singlestore-labs/events/eventtest"
)

/*

 Set up to run tests...

 You also need Kafka. See the eventnodb tests

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

func sqlDB(t ntest.T, ctx context.Context) *sql.DB {
	dsn := os.Getenv("EVENTS_S2TEST_DSN")
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

var chain = nject.Sequence("s2test-injectors",
	eventtest.CommonInjectors,
	nject.Required(nject.Provide("sql.DB", sqlDB)),
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
	ntest.RunParallelMatrix(ntest.BufferedLogger(t),
		chain,
		eventtest.GenerateSharedTestMatrix[eventmodels.BinaryEventID, *eventdb.ExampleBasicTX, *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB]](),
	)
}

func TestLockingLockOrDie(t *testing.T) {
	t.Parallel()
	ntest.RunTest(ntest.BufferedLogger(t), chain, func(
		t ntest.T,
		ctx context.Context,
		db *events2.Connection[*eventdb.ExampleBasicTX, eventdb.ExampleBasicDB],
	) {
		baseT := t
		t = ntest.ExtraDetailLogger(t, "TLOD")
		const threads = 10

		ctx1, cancel1 := context.WithCancel(ctx)

		var got atomic.Uint32
		var failed atomic.Uint32
		var wg1 sync.WaitGroup
		allDone := make(chan struct{})

		n, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
		require.NoError(t, err)
		lockID := uint32(n.Int64())
		t.Logf("locking id %d", lockID)

		t.Log("locks are held, only one is expected to succceed")
		f := func(ctx context.Context, i int, wg *sync.WaitGroup) {
			t := ntest.ExtraDetailLogger(baseT, fmt.Sprintf("TLOD-%d", i))
			defer wg.Done()
			timeStart := time.Now()
			done, err := db.LockOrError(ctx, lockID, time.Second*5)
			defer func() {
				_ = done()
			}()
			if errors.Is(err, eventmodels.TimeoutErr) {
				t.Logf("failed to get lock in %s, timeout", time.Since(timeStart))
				failed.Add(1)
				return
			}
			if !assert.NoError(t, err, "we shouldn't have some different error") {
				return
			}
			t.Logf("got lock in %s", time.Since(timeStart))
			got.Add(1)
			<-allDone
		}

		for i := 0; i < threads; i++ {
			wg1.Add(1)
			go f(ctx1, i, &wg1)
		}
		time.Sleep(10 * time.Second)
		close(allDone)
		cancel1()
		wg1.Wait()
		require.Equal(t, 1, int(got.Load()), "got")
		require.Equal(t, threads-1, int(failed.Load()), "failed")

		got.Store(0)
		failed.Store(0)

		t.Log("locks are released, all are expected to succceed")
		var wg2 sync.WaitGroup
		ctx2, cancel2 := context.WithCancel(ctx)
		for i := 0; i < threads; i++ {
			wg2.Add(1)
			go f(ctx2, i, &wg2)
		}
		time.Sleep(2 * time.Second)
		cancel2()
		wg2.Wait()

		require.Equal(t, threads, int(got.Load()), "got")
		require.Equal(t, 0, int(failed.Load()), "failed")
	})
}
