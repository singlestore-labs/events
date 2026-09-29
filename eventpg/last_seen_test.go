package eventpg_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/memsql/errors"
	"github.com/muir/libschema"
	"github.com/muir/libschema/lspostgres"
	"github.com/muir/testinglogur"
	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events/eventmodels"
	"github.com/singlestore-labs/events/eventpg"
)

func TestPostgresLastSeenAt(t *testing.T) {
	dsn := os.Getenv("EVENTS_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("Set $EVENTS_POSTGRES_TEST_DSN to run PostgreSQL lastSeenAt tests")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	schema := libschema.New(ctx, libschema.Options{})
	database, err := lspostgres.New(libschema.LogFromLogur(testinglogur.Get(t)), "eventstest", schema, db)
	require.NoError(t, err)
	eventpg.Migrations(database)
	require.NoError(t, schema.Migrate(ctx))

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	err = eventpg.TouchEventProcessed(ctx, tx, "orders", "src", "missing-id", "handler")
	require.NoError(t, err)

	before := time.Now().UTC()
	err = eventpg.MarkEventProcessed(ctx, tx, "orders", "src", "past-id", "handler")
	require.NoError(t, err)
	var seen time.Time
	err = tx.QueryRowContext(ctx, `SELECT lastSeenAt FROM eventsProcessed WHERE id = $1`, "past-id").Scan(&seen)
	require.NoError(t, err)
	require.False(t, seen.Before(before.Add(-time.Second)))

	err = eventpg.MarkEventProcessed(ctx, tx, "orders", "src", "past-id", "handler")
	require.Error(t, err)
	require.True(t, errors.Is(err, eventmodels.ErrAlreadyProcessed))

	_, err = tx.ExecContext(ctx, `
		UPDATE eventsProcessed
		SET lastSeenAt = $1
		WHERE id = $2`, before.Add(-time.Hour), "past-id")
	require.NoError(t, err)
	err = eventpg.TouchEventProcessed(ctx, tx, "orders", "src", "past-id", "handler")
	require.NoError(t, err)
	err = tx.QueryRowContext(ctx, `SELECT lastSeenAt FROM eventsProcessed WHERE id = $1`, "past-id").Scan(&seen)
	require.NoError(t, err)
	require.False(t, seen.Before(before.Add(-time.Second)))
}
