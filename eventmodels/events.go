package eventmodels

import (
	"time"
)

// SelfMarshalingEvent is a variant of ProducingEvent that has a custom
// marshal function.
type SelfMarshalingEvent interface {
	Marshal() (encoded []byte, contentType string, err error)
}

// ProducingEvent is used when generating an event. The body of the event
// is created by JSON marshaling.
type ProducingEvent interface {
	GetKey() string
	GetTimestamp() time.Time // used for the cloudevents time header
	GetTopic() string        // Topic is also used to generate the cloudevents type header. Format: noun.verb
	GetHeaders() map[string][]string
}
