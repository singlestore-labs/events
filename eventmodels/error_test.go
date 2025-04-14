package eventmodels_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/singlestore-labs/events/eventmodels"
)

func TestErrorWrapping(t *testing.T) {
	t.Parallel()
	assert.Nil(t, eventmodels.SetErrorHandling(nil, eventmodels.DoNotRetry))
	assert.Equal(t, eventmodels.RetryUntilTimeout,
		eventmodels.GetErrorHandling(
			fmt.Errorf("whatever")))
	assert.Equal(t, eventmodels.IgnoreError, eventmodels.GetErrorHandling(
		eventmodels.SetErrorHandling(
			fmt.Errorf("whatever"),
			eventmodels.IgnoreError)))
}
