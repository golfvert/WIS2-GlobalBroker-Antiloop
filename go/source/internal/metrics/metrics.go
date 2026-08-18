// Package metrics exposes exactly the 9 metrics found in flows.json's
// prometheus-metric-config nodes, same names and types, so existing
// Grafana dashboards and alert rules keep working unmodified.
//
// The original flow updates them through a generic {op, labels, val}
// object sent to a shared prometheus-exporter node ("set"/"inc" ops).
// In Go there's no need for that indirection — callers just call the
// typed methods below directly.
//
// Since each process handles exactly one centre_id (not a multi-tenant
// process), centre_id is a ConstLabel baked in at startup rather than a
// free dimension on a *Vec — simpler, and each process only ever
// reports its own centre anyway.
//
// Every wmo_wis2_gb_* metric is labeled centre_id|report_by, per the
// wis2-metric-hierarchy gb.csv spec
// (https://github.com/wmo-im/wis2-metric-hierarchy/blob/main/metrics/gb.csv)
// — report_by is this broker's own identity (GB_ID, e.g.
// "fr-meteofrance-global-broker"). Both are ConstLabels for the same
// reason: one process per centre_id, one GB_ID per process, neither
// varies over the process's lifetime.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	LastMessageTimestamp *prometheus.GaugeVec
	MessagesPublished    prometheus.Counter
	MessagesInvalidFormat prometheus.Counter
	MessagesReceived     prometheus.Counter
	Connected            prometheus.Gauge
	MessagesNoMetadata   prometheus.Counter
	MessagesInvalidTopic prometheus.Counter
	AllConnected         prometheus.Gauge // monitor_wis2_gb_all_connected_flag
	ConnectedBackup      prometheus.Gauge

	// gbID is stashed so SetLastMessageTimestamp (the one metric still
	// exposed as a *Vec rather than a plain Gauge with ConstLabels) can
	// fill in the report_by label value without callers having to pass
	// GB_ID through on every call — see New's doc comment.
	gbID string

	registry *prometheus.Registry
}

func New(centreID, gbID string) *Metrics {
	labels := prometheus.Labels{"centre_id": centreID, "report_by": gbID}
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		gbID:     gbID,

		// GaugeVec here (not a plain Gauge with ConstLabels) purely so the
		// centre_id/report_by labels are visible in the metric name's
		// label set the same way the original does — either works, this
		// just matches the spec's `"labels": "centre_id|report_by"` shape
		// 1:1 (see package doc comment).
		LastMessageTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wmo_wis2_gb_last_message_timestamp_seconds",
			Help: "Timestamp of last message received (in seconds)",
		}, []string{"centre_id", "report_by"}),

		MessagesPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "wmo_wis2_gb_messages_published_total",
			Help:        "Number of message published",
			ConstLabels: labels,
		}),
		MessagesInvalidFormat: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "wmo_wis2_gb_messages_invalid_format_total",
			Help:        "Number of invalid messages",
			ConstLabels: labels,
		}),
		MessagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "wmo_wis2_gb_messages_received_total",
			Help:        "Number of messages received from broker",
			ConstLabels: labels,
		}),
		Connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "wmo_wis2_gb_connected_flag",
			Help:        "Connection status from broker to centre",
			ConstLabels: labels,
		}),
		MessagesNoMetadata: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "wmo_wis2_gb_messages_no_metadata_total",
			Help:        "Number of messages received without corresponding metadata from centre",
			ConstLabels: labels,
		}),
		MessagesInvalidTopic: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "wmo_wis2_gb_messages_invalid_topic_total",
			Help:        "Number of messages on invalid topic",
			ConstLabels: labels,
		}),
		AllConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "monitor_wis2_gb_all_connected_flag",
			Help:        "Connection status to local broker",
			ConstLabels: labels,
		}),
		ConnectedBackup: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "wmo_wis2_gb_connected_backup_flag",
			Help:        "Connection status from broker to centre backup",
			ConstLabels: labels,
		}),
	}

	reg.MustRegister(
		m.LastMessageTimestamp,
		m.MessagesPublished,
		m.MessagesInvalidFormat,
		m.MessagesReceived,
		m.Connected,
		m.MessagesNoMetadata,
		m.MessagesInvalidTopic,
		m.AllConnected,
		m.ConnectedBackup,
	)

	return m
}

// Handler returns the /metrics HTTP handler for this process's registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SetLastMessageTimestamp mirrors the "Last message" change node:
// floor(now_ms / 1000), labeled by centre_id and report_by (GB_ID —
// see New's doc comment for why the wis2-metric-hierarchy spec
// requires report_by).
func (m *Metrics) SetLastMessageTimestamp(centreID string, unixSeconds float64) {
	m.LastMessageTimestamp.WithLabelValues(centreID, m.gbID).Set(unixSeconds)
}
