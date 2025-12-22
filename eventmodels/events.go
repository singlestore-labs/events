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
	GetTopic() string        // Topic is also used to generate the cloudevents type header. The topic is un-prefixed. Suggested format: noun.verb
	GetHeaders() map[string][]string
}

// Event abstracts away the underlying message system (Kafka)
//
// It is a superset of CloudEvent (https://github.com/cloudevents/spec) and Kafka data.
//
// ID is either a CloudEvent header (if present) or a checksum of the event data. ID plus
// Source should be unique (or if not unique, then the message is a duplicate).
//
// For Kafka events that follow the CloudEvents spec, Headers is the CloudEvents headers
// so the "id" header will be the CloudEvents id and any other CloudEvents. For
// non-CloudEvents-compliant messages, Headers will reflect the Kafka Headers.
type Event[E any] struct {
	Topic         string // may be a dead-letter topic. Un-prefixed.
	Key           string
	Data          []byte
	Payload       E
	Headers       map[string][]string
	Timestamp     time.Time
	ID            string // CloudEvents field, defaults to a checksum if not present
	ContentType   string // Optional field, required for CloudEvents messages
	Subject       string // CloudEvents field, defaults to Kafka Key
	Type          string // CloudEvents field, defaults to Kafka Topic
	Source        string // CloudEvents field, empty for non-CloudEvents messages
	SpecVersion   string // CloudEvents field, empty for non-CloudEvents messages
	DataSchema    string // optional CloudEvents field, empty if not set
	ConsumerGroup string
	HandlerName   string
	BaseTopic     string // always the non-dead-letter topic, different from Topic when processing dead letters. Un-prefixed.
	idx           int
}
