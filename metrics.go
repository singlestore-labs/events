package events

import (
	"database/sql"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"singlestore.com/helios/trace"
)

const prefix = "events_"

var timeBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

func mustRegister[C prometheus.Collector](collector C) C {
	prometheus.MustRegister(collector)
	return collector
}

var DeadLetterProduceCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "dead_letter_produce",
		Help: "Number of messages (by handler name and topic) produced to the dead letter queue",
	},
	[]string{"handler_name", "topic"},
))

var DeadLetterConsumeCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "dead_letter_consume",
		Help: "Number of messages (by handler name and topic) consumed from the dead letter queue",
	},
	[]string{"handler_name", "topic"},
))

var ErrorCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "errors",
		Help: "Number of errors (by category) from the eventing system",
	},
	[]string{"category"},
))

var HandlerErrorCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "handler_errors",
		Help: "Number of errors (by handler name and topic) from the eventing system",
	},
	[]string{"handler_name", "topic"},
))

var ConsumersWaitingForQueueConcurrencyDemand = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "consumer_group_queue_concurrency_demand",
		Help: "Per-consumer group concurrency limit, total waiting OR processing",
	},
	[]string{"consumer_group"},
))

var ConsumersWaitingForQueueConcurrencyLimit = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "consumer_group_queue_concurrency_wait",
		Help: "Per-consumer group concurrency limit, total waiting",
	},
	[]string{"consumer_group"},
))

var HandlerWaitingForQueueConcurrencyDemand = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "handler_queue_concurrency_demand",
		Help: "Per-handler concurrency limit, total waiting OR processing",
	},
	[]string{"handler_name", "topic"},
))

var HandlerPanicCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "handler_panic",
		Help: "Number handler panics (by handler name and topic)",
	},
	[]string{"handler_name", "topic"},
))

var HandlerCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "handler_success",
		Help: "Number handler success (by handler name and topic)",
	},
	[]string{"handler_name", "topic"},
))

var HandlerLatency = mustRegister(prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: prefix + "handler_latencies",
		Help: "Per-handler processing times (seconds)",
	},
	[]string{"handler_name", "topic"},
))

var HandlerWaitingForQueueConcurrencyLimit = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "handler_queue_concurrency_wait",
		Help: "Per-handler concurrency limit, total waiting",
	},
	[]string{"handler_name", "topic"},
))

var LongestHandlerLatency = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "unfinished_handler_time",
		Help: "Handlers that have been processing for a while (seconds)",
	},
	[]string{"handler_name", "topic"},
))

var HandlerWaitingForActiveConcurrencyDemand = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "active_queue_concurrency_demand",
		Help: "Global concurrency limit, total waiting OR processing",
	},
	[]string{"handler_name", "topic"},
))

var HandlerWaitingForActiveConcurrencyLimit = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "active_queue_concurrency_wait",
		Help: "Global concurrency limit, total waiting",
	},
	[]string{"handler_name", "topic"},
))

var ConsumeCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "messages_consumed",
		Help: "Number of messaged consumed",
	},
	[]string{"topic", "consumer_group"},
))

var ProduceTopicCounts = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "messages_produced",
		Help: "Number of messaged produced",
	},
	[]string{"topic", "produce_method"},
))

var ProduceFromTxSplit = mustRegister(prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: prefix + "batches_in_tx",
		Help: "Number in-transaction messages",
	},
	[]string{"method"},
))

var TransmissionLatency = mustRegister(prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: prefix + "transmission_latency",
		Help: "Latency from message creation to message receipt (seconds)",
	},
	[]string{"topic", "consumer_group"},
))

var AckLatency = mustRegister(prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: prefix + "acknowledgement_latency",
		Help: "Latency from message creation to message receive acknowledgement",
	},
	[]string{"topic", "consumer_group"},
))

var HandlerBatchConcurrency = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "batch_handler_concurrency",
		Help: "How many batches are being processed in parallel",
	},
	[]string{"handler_name"},
))

var HandlerBatchQueued = mustRegister(prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: prefix + "batch_handler_queued",
		Help: "How many messages are queued for batch handling",
	},
	[]string{"handler_name"},
))

var produceEventsLatency = trace.MustRegisterMetrics(prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    prefix + "post_tx_produce_seconds",
		Buckets: timeBuckets,
		Help:    "Durations in seconds, sending events post-transaction (seconds)",
	},
	[]string{"tx_produce_error"},
))

func ObserveProduceLatency(err error, startTime time.Time) {
	label := "nil"
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			label = "noRows"
		} else {
			label = "error"
		}
	}
	produceEventsLatency.WithLabelValues(label).Observe(float64(time.Since(startTime) / time.Second))
}
