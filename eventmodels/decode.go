package eventmodels

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
)

var ErrDecode errors.String = "could not unmarshal message"

// decode transforms a Kafka Message into a Event struct.
func decode[E any](message *kafka.Message, consumerGroup string, lib LibraryInterface, handlerName string) (Event[E], error) {
	unprefixedTopic := lib.RemovePrefix(message.Topic)
	meta := Event[E]{
		Topic:         unprefixedTopic,
		Type:          unprefixedTopic,
		Key:           string(message.Key),
		Subject:       string(message.Key),
		Headers:       make(http.Header),
		Timestamp:     message.Time,
		ConsumerGroup: consumerGroup,
		HandlerName:   handlerName,
	}
	var foundContent bool
	headers := meta.Headers
	for _, h := range message.Headers {
		value := string(h.Value)
		switch h.Key {
		case "ce_time":
			headers["time"] = append(headers["time"], value)
			t, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return meta, errors.Errorf("consume invalid ce_time for message in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
			}
			meta.Timestamp = t
		case "ce_source":
			headers["source"] = append(headers["source"], value)
			meta.Source = string(h.Value)
		case "ce_id":
			headers["id"] = append(headers["id"], value)
			meta.ID = value
		case "ce_type":
			headers["type"] = append(headers["type"], value)
			meta.Type = value
		case "ce_subject":
			headers["subject"] = append(headers["subject"], value)
			meta.Subject = value
		case "ce_specversion":
			headers["specversion"] = append(headers["specversion"], value)
			meta.SpecVersion = value
		case "ce_dataschema":
			headers["dataschema"] = append(headers["dataschema"], value)
			meta.DataSchema = value
		case "content-type":
			headers["content-type"] = append(headers["content-type"], value)
			meta.Data = message.Value
			meta.ContentType = string(h.Value)
			switch contentTypeTrim(string(h.Value)) {
			case "application/json":
				err := json.Unmarshal(message.Value, &meta.Payload)
				if err != nil {
					return meta, ErrDecode.Errorf("consume could not JSON decode message in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
				}
			case "application/xml":
				err := xml.Unmarshal(message.Value, &meta.Payload)
				if err != nil {
					return meta, ErrDecode.Errorf("consume could not XML decode message in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
				}
			case "application/cloudevents+json",
				"application/cloudevents+json; charset=UTF-8":
				var ce cloudEvent
				err := json.Unmarshal(message.Value, &ce)
				if err != nil {
					return meta, ErrDecode.Errorf("consume could not JSON decode cloudevent message in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
				}
				if !ce.Time.IsZero() {
					meta.Timestamp = ce.Time
				}
				data := []byte(ce.Data)
				if len(ce.Data) == 0 && len(ce.DataBase64) != 0 {
					var err error
					data, err = base64.StdEncoding.DecodeString(ce.DataBase64)
					if err != nil {
						return meta, ErrDecode.Errorf("consume could not base64-decode data in cloudevent message in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
					}
				}
				if ce.Type != "" {
					meta.Type = ce.Type
				}
				if ce.Subject != "" {
					meta.Subject = ce.Subject
				}
				if ce.Source != "" {
					meta.Source = ce.Source
				}
				meta.SpecVersion = ce.SpecVersion
				meta.ID = ce.ID
				meta.Data = data
				meta.ContentType = ce.ContentType
				meta.DataSchema = ce.DataSchema
				switch contentTypeTrim(ce.ContentType) {
				case "application/json":
					err := json.Unmarshal(data, &meta.Payload)
					if err != nil {
						return meta, ErrDecode.Errorf("consume could not JSON decode cloudevent data in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
					}
				case "application/xml":
					err := xml.Unmarshal(data, &meta.Payload)
					if err != nil {
						return meta, ErrDecode.Errorf("consume could not XML decode cloudevent data in topic (%s) with key (%s): %w", message.Topic, string(message.Key), err)
					}
				default:
					return meta, ErrDecode.Errorf("consume unsupported content type (%s -- %s) for cloudevent message in topic (%s) with key (%s)", ce.ContentType, contentTypeTrim(ce.ContentType), message.Topic, string(message.Key))
				}
			default:
				return meta, ErrDecode.Errorf("consume unsupported content type (%s - %s) for message in topic (%s) with key (%s)", string(h.Value), contentTypeTrim(string(h.Value)), message.Topic, string(message.Key))
			}
			foundContent = true
		default:
			key := strings.TrimPrefix(h.Key, "ce_")
			headers[key] = append(headers[key], value)
		}
	}
	if !foundContent {
		return meta, ErrDecode.Errorf("consume message in topic (%s) with key (%s) is missing cloudevent headers", message.Topic, string(message.Key))
	}
	if meta.ID == "" {
		checksumData := make([]byte, len(meta.Data), len(meta.Data)+len(meta.ContentType)+len(meta.Source)+25)
		copy(checksumData, meta.Data)
		checksumData = append(checksumData, '\x00')
		checksumData = append(checksumData, []byte(meta.ContentType)...)
		checksumData = append(checksumData, '\x00')
		checksumData = append(checksumData, []byte(meta.Source)...)
		checksumData = append(checksumData, '\x00')
		checksumData = append(checksumData, []byte(meta.Timestamp.Format(time.RFC3339Nano))...)
		meta.ID = fmt.Sprintf("%x", md5.Sum(checksumData))
	}

	return meta, nil
}

func contentTypeTrim(ct string) string {
	splits := strings.SplitN(ct, "; ", 2)
	if len(splits) == 2 && len(splits[0]) > 0 {
		return splits[0]
	}
	return ct
}

// cloudEvent is used to decode application/cloudevents+json models.
// See https://github.com/cloudevents/spec. This struct is only relevant for
// consuming events. When producing events, most of these fields are filled
// automatically.
type cloudEvent struct {
	SpecVersion string    `json:"specversion"`
	Type        string    `json:"type"`
	Source      string    `json:"source"`
	ID          string    `json:"id"`
	Subject     string    `json:"subject"`
	Time        time.Time `json:"time"`
	ContentType string    `json:"datacontenttype"`
	Data        string    `json:"data"`
	DataBase64  string    `json:"data_base64"`
	DataSchema  string    `json:"dataschema"`
}
