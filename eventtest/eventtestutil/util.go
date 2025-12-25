package eventtestutil

import (
	"context"
	"os"
	"strings"

	"github.com/memsql/ntest"
	"github.com/muir/nject/v2"
	"github.com/singlestore-labs/once"
)

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




