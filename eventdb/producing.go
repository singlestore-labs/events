package eventdb

import (
	"time"

	"singlestore.com/helios/events/eventmodels"
)

// ProducingEventBlob is used to bridge from pending events stored in the
// database to calls to Produce() to actually send the event
type ProducingEventBlob[ID eventmodels.AbstractID[ID]] struct {
	K              string
	TS             time.Time
	KafkaTopic     string
	Data           []byte
	ID             ID
	EncodedHeader  []byte
	HeaderMap      map[string][]string
	SequenceNumber int
}

func (e ProducingEventBlob[ID]) GetKey() string                  { return e.K }
func (e ProducingEventBlob[ID]) GetTimestamp() time.Time         { return e.TS }
func (e ProducingEventBlob[ID]) GetTopic() string                { return e.KafkaTopic }
func (e ProducingEventBlob[ID]) GetHeaders() map[string][]string { return e.HeaderMap }

func (e ProducingEventBlob[ID]) MarshalJSON() ([]byte, error) {
	return e.Data, nil
}
