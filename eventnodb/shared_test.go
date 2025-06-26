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

/*

Set up to run tests...

docker run -d \
  --name kafka \
  -p 9092:9092 \
  -p 9093:9093 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=false \
  -e KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_CFG_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e GROUP_ID=1 \
  bitnami/kafka:3.6.2

EVENTS_KAFKA_BROKERS=localhost:9092

*/

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
