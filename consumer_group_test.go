package events

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/memsql/errors"
	"github.com/memsql/ntest"
	"github.com/muir/nject/v2"
	"github.com/segmentio/kafka-go"
	"github.com/singlestore-labs/once"
	"github.com/singlestore-labs/wait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
)

// --------- begin section that is duplicated in eventtest/common.go -----------

type T = ntest.T

type Brokers []string

func KafkaBrokers(t T) Brokers {
	brokers := os.Getenv("EVENTS_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("EVENTS_KAFKA_BROKERS must be set to run this test")
	}
	return Brokers(strings.Split(brokers, " "))
}

var CommonInjectors = nject.Sequence("common",
	nject.Provide("context", context.Background),
	nject.Required(nject.Provide("Report-results", func(inner func(), t T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("RESULT: %s FAILED w/panic", t.Name())
				panic(r)
			}
			if t.Failed() {
				t.Logf("RESULT: %s FAILED", t.Name())
			} else {
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

// --------- end section that is duplicated in eventtest/common.go -----------

// ------- begin section that is duplicated in eventnodb/nodb.go ----------

type NoDBTx struct {
	*sql.Tx
}

func (tx *NoDBTx) Produce(events ...eventmodels.ProducingEvent) {
	// No-op for no-database implementation - events are discarded
}

type NoDB struct {
	*sql.DB
}

func (*NoDB) LockOrError(_ context.Context, _ uint32, _ time.Duration) (func() error, error) {
	return nil, errors.WithStack(eventmodels.NotImplementedErr)
}

func (*NoDB) MarkEventProcessed(_ context.Context, _ *NoDBTx, _ string, _ string, _ string, _ string) error {
	return errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) ProduceSpecificTxEvents(_ context.Context, _ []eventmodels.BinaryEventID) (int, error) {
	return 0, errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) ProduceDroppedTxEvents(_ context.Context, _ int) (int, error) {
	return 0, errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) Transact(_ context.Context, _ func(*NoDBTx) error) error {
	return errors.Alert(eventmodels.NotImplementedErr)
}

func (*NoDB) AugmentWithProducer(_ eventmodels.Producer[eventmodels.BinaryEventID, *NoDBTx]) {}

// ------- end section that is duplicated in eventnodb/nodb.go ----------

func TestBroadcastGroupRefresh(t *testing.T) {
	t.Parallel()
	if os.Getenv("EVENTS_KAFKA_BROKERS") == "" {
		t.Skipf("%s requires kafka brokers", t.Name())
	}
	t.Log("starting broadcast refresh test")
	ntest.RunTest(ntest.BufferedLogger(t),
		CommonInjectors,
		nject.Provide("nodb", func() *NoDB {
			return nil // Pass nil connection to trigger no-database behavior
		}),
		func(
			t ntest.T,
			ctx context.Context,
			brokers Brokers,
		) {
			baseT := t
			lib1 := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
			lib1.Configure(nil, ntest.ExtraDetailLogger(baseT, "BGR-1"), false, SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)
			lib2 := New[eventmodels.BinaryEventID, *NoDBTx, *NoDB]()
			lib2.Configure(nil, ntest.ExtraDetailLogger(baseT, "BGR-2"), false, SASLConfigFromString(os.Getenv("KAFKA_SASL")), nil, brokers)

			for try := 1; try <= 10; try++ {
				t.Log("attempt #%d to reuse the consumer group", try)
				g1, r1, _, u1, err := lib1.getBroadcastConsumerGroup(ctx, time.Duration(0))
				require.NoError(t, err)
				t.Logf("got group %s", g1)
				t.Log("closing the reader, unlocking the group")
				_ = r1.Close()
				assert.True(t, strings.HasPrefix(string(g1), defaultLockFreeBroadcastBase), "prefix")

				// Sometimes even though r1 is closed, the group isn't available. We should be able
				g2, r2, _, err := lib1.refreshBroadcastReader(ctx, g1, &u1)
				require.NoError(t, err)
				_ = r2.Close()
				t.Logf("refreshed to group %s", g2)
				t.Log("closing the reader, unlocking the group")
				if g1 != g2 {
					t.Logf("oh no! we refreshed to a different group: %s vs %s", g1, g2)
					continue
				}
				t.Log("refreshed to the same group")

				var rX *kafka.Reader
				require.NoError(t, wait.For(func() (bool, error) {
					var err error
					rX, _, err = lib2.getBroadcastReader(ctx, g1, false)
					if err != nil {
						if errors.Is(err, errGroupUnavailable) {
							t.Logf("group %s not available (yet): %s", g1, err)
							return false, nil
						}
						return false, err
					}
					return true, nil
				}, wait.ExitOnError(true), wait.WithMinInterval(time.Second), wait.WithLimit(time.Minute*3)))
				defer func() {
					_ = rX.Close()
				}()

				t.Log("now that %s is locked to another server, getting it in the first should fail", g1)
				g4, r4, _, err := lib1.refreshBroadcastReader(ctx, g1, &u1)
				require.NoError(t, err)
				_ = r4.Close()
				t.Log("refreshed to %s", g4)
				require.NotEqual(t, g1, g4)
				return
			}
			assert.True(t, false, "oh, we never refreshed to the same group")
		})
}
