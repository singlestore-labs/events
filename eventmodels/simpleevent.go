package eventmodels

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/google/uuid"
)

var defaultSource = filepath.Base(os.Args[0])

type SimpleEvent struct {
	key     string
	ts      time.Time
	topic   string
	headers map[string][]string
	model   any
	source  string
}

var _ ProducingEvent = SimpleEvent{}

var _ json.Marshaler = SimpleEvent{}

func newEvent(topic string, key string) SimpleEvent {
	return SimpleEvent{
		source:  defaultSource,
		topic:   topic,
		key:     key,
		headers: make(map[string][]string),
		ts:      time.Now(),
	}
}

func (e SimpleEvent) addPayload(m any) SimpleEvent { e.model = m; return e }

func (e SimpleEvent) SpecVersion(v string) SimpleEvent {
	e.headers["ce_specversion"] = []string{v}
	return e
}
func (e SimpleEvent) Type(t string) SimpleEvent { e.headers["ce_type"] = []string{t}; return e }
func (e SimpleEvent) Source(source string) SimpleEvent {
	e.headers["ce_source"] = []string{source}
	return e
}
func (e SimpleEvent) ID(id string) SimpleEvent { e.headers["ce_id"] = []string{id}; return e }
func (e SimpleEvent) Subject(sub string) SimpleEvent {
	e.headers["ce_subject"] = []string{sub}
	return e
}
func (e SimpleEvent) Time(t time.Time) SimpleEvent { e.ts = t; return e }
func (e SimpleEvent) DataSchema(s string) SimpleEvent {
	e.headers["ce_dataschema"] = []string{s}
	return e
}

func (e SimpleEvent) GetTimestamp() time.Time { return e.ts }
func (e SimpleEvent) GetKey() string          { return e.key }
func (e SimpleEvent) GetTopic() string        { return e.topic }

// Header returns a map[string][]string object that contains the data that will become the
// event headers.
//
// All headers, except "content-type" should be prefixed with "ce_" to indicate that
// they're CloudEvent headers. The "ce_" will be stripped on the receiving side.
func (e SimpleEvent) Header() map[string][]string { return e.headers }

// GetHeaders fills in default values for the headers and returns a http.Header. It is
// required to fulfill the ProducingEvent interface contract.
func (e SimpleEvent) GetHeaders() map[string][]string {
	if _, ok := e.headers["ce_specversion"]; !ok {
		e.headers["ce_specversion"] = []string{"1.0"}
	}
	if _, ok := e.headers["ce_type"]; !ok {
		e.headers["ce_type"] = []string{e.topic}
	}
	if _, ok := e.headers["ce_source"]; !ok {
		e.headers["ce_source"] = []string{defaultSource}
	}
	if _, ok := e.headers["ce_id"]; !ok {
		e.headers["ce_id"] = []string{uuid.New().String()}
	}
	if _, ok := e.headers["ce_subject"]; !ok {
		e.headers["ce_subject"] = []string{e.key}
	}
	if _, ok := e.headers["ce_time"]; !ok {
		e.headers["ce_time"] = []string{e.ts.Format(time.RFC3339)}
	}
	if _, ok := e.headers["ce_dataschema"]; !ok && e.model != nil {
		e.headers["ce_dataschema"] = []string{reflect.TypeOf(e.model).String()}
	}
	return e.headers
}

func (e SimpleEvent) MarshalJSON() ([]byte, error) {
	if e.model == nil {
		return []byte("null"), nil
	}
	return json.Marshal(e.model)
}
