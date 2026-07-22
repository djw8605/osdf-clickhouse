// Package metrics defines the Prometheus instrumentation for the ingester.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics bundles all collectors so they can be passed explicitly (easier to
// test than package-level globals).
type Metrics struct {
	MessagesConsumed *prometheus.CounterVec // by exchange
	ParseErrors      prometheus.Counter
	RowsInserted     prometheus.Counter
	BatchesCommitted prometheus.Counter
	InsertFailures   prometheus.Counter
	DeadLetters      prometheus.Counter
	InsertLatency    prometheus.Histogram
	CurrentBatchSize prometheus.Gauge
	AMQPReconnects   prometheus.Counter
	CHReconnects     prometheus.Counter
	Ready            prometheus.Gauge // 1 when ready to serve, 0 otherwise
}

// New registers and returns the metric set on the given registry.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		MessagesConsumed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "ingester_messages_consumed_total",
			Help: "Messages received from RabbitMQ, labeled by source exchange.",
		}, []string{"exchange"}),
		ParseErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_parse_errors_total",
			Help: "Messages that failed JSON parsing.",
		}),
		RowsInserted: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_rows_inserted_total",
			Help: "Rows successfully committed to ClickHouse.",
		}),
		BatchesCommitted: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_batches_committed_total",
			Help: "Batches successfully committed to ClickHouse.",
		}),
		InsertFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_insert_failures_total",
			Help: "Batch insert attempts that failed and were retried.",
		}),
		DeadLetters: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_dead_letters_total",
			Help: "Messages routed to the dead-letter exchange after exhausting retries.",
		}),
		InsertLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "ingester_insert_latency_seconds",
			Help:    "Wall-clock latency of a successful batch commit to ClickHouse.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms .. ~20s
		}),
		CurrentBatchSize: f.NewGauge(prometheus.GaugeOpts{
			Name: "ingester_current_batch_size",
			Help: "Number of rows buffered in the batch not yet flushed.",
		}),
		AMQPReconnects: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_amqp_reconnects_total",
			Help: "Number of RabbitMQ reconnect attempts.",
		}),
		CHReconnects: f.NewCounter(prometheus.CounterOpts{
			Name: "ingester_clickhouse_reconnects_total",
			Help: "Number of ClickHouse reconnect attempts.",
		}),
		Ready: f.NewGauge(prometheus.GaugeOpts{
			Name: "ingester_ready",
			Help: "1 when the ingester is connected and ready, 0 otherwise.",
		}),
	}
}
