//go:build integration

package events_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/singlestore-labs/events"
)

func TestSASLConfig(t *testing.T) {
	for _, good := range []string{
		"none",
		"plain:username:password",
		"sha256:username:password",
		"sha512:username:password",
	} {
		var sc events.SASLConfig
		err := sc.UnmarshalText([]byte(good))
		require.NoErrorf(t, err, "unmarshal '%s'", good)
	}

	for _, bad := range []string{
		"None",
		"plane:username:password",
		"sha256:username:password:extra",
		"sha512:username",
	} {
		var sc events.SASLConfig
		err := sc.UnmarshalText([]byte(bad))
		require.Errorf(t, err, "unmarshal '%s'", bad)
	}
}
