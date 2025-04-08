package eventmodels

import "errors"

// Error provides a way for handlers to explicitly control retry and timeout by the event
// consuming framework
type Error struct {
	inner    error
	handling ErrorHandling
}

type ErrorHandling int

const (
	RetryUntilTimeout ErrorHandling = iota
	IgnoreError
	DoNotRetry
)

func SetErrorHandling(err error, handling ErrorHandling) error {
	if err == nil {
		return nil
	}
	return Error{
		inner:    err,
		handling: handling,
	}
}

func (e Error) Error() string { return e.inner.Error() }
func (e Error) Unwrap() error { return e.inner }

func GetErrorHandling(err error) ErrorHandling {
	var e Error
	if errors.As(err, &e) {
		return e.handling
	}
	return RetryUntilTimeout
}
