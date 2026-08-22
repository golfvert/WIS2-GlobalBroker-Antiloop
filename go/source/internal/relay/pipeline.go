// Package relay wires the per-message pipeline together.
//
// Incoming messages go through a role-gated buffer (internal/gate)
// BEFORE Check/Validate/Metadata/Dedup, not after — this matters
// because dedup's SET NX is a one-way door. Running dedup on a
// secondary for a message that then gets discarded (because publish is
// skipped) would permanently burn that message's dedup slot; if this
// instance is later promoted to primary, it could never publish that
// message, since the dedup check would already see it as seen. That
// would be silent, permanent loss, concentrated exactly at the
// highest-risk moment: a failover. See internal/gate's doc comment for
// the full trace of the original flow's q-gate mechanism this
// replicates.
//
// The three *_CHECK_OPTION env vars each gate TWO things, traced
// directly from the flow's switch nodes (config.CheckOption doc
// comment has the full trace):
//
//  1. Enabled() — `in ["discard","verify"]` — if false, the check does
//     not run at all. No metric increment, message passes through.
//  2. On failure, only if Enabled(): "discard" drops the message right
//     there; "verify" still counts the failure metric but lets the
//     message continue down the pipeline anyway.
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"antiloop/internal/allowlist"
	"antiloop/internal/config"
	"antiloop/internal/dedup"
	"antiloop/internal/gate"
	"antiloop/internal/metrics"
	"antiloop/internal/monitor"
	"antiloop/internal/mqttbroker"
	"antiloop/internal/topics"
	"antiloop/internal/wnm"
)

// publishCountLogInterval controls how often the "pubcount" debug
// category logs a running total — see Pipeline.publishCount below. A
// lightweight alternative to the "publisher" category, which logs
// every single message's full topic+payload and is too verbose to
// leave on during a throughput test (e.g. against wnmtest), where the
// logging itself would skew the measurement.
const publishCountLogInterval = 100

// stageStat accumulates a running total (nanoseconds) and count for one
// pipeline stage — backs the "timing" debug category (see Pipeline.timing
// below). Cumulative since process start, not a sliding window — cheap
// (two atomic adds per sample) and enough to see which stage dominates.
type stageStat struct {
	totalNs atomic.Int64
	count   atomic.Int64
}

func (s *stageStat) record(d time.Duration) {
	s.totalNs.Add(int64(d))
	s.count.Add(1)
}

func (s *stageStat) avg() time.Duration {
	n := s.count.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(s.totalNs.Load() / n)
}

// pipelineTiming holds one stageStat per instrumented stage of
// process() — cumulative time spent in topic check, schema validation,
// metadata check, Redis dedup, and publish, so the "timing" debug
// category (like "pubcount") can show which stage actually accounts
// for a given per-message average, rather than guessing at where time
// is going during a throughput investigation.
type pipelineTiming struct {
	topicCheck    stageStat
	schemaCheck   stageStat
	metadataCheck stageStat
	dedup         stageStat
	publish       stageStat
}

// Msg is what sits in the gate's backlog — the raw inputs to process(),
// captured at receive time so replaying the backlog on promotion
// reprocesses the original message, not something mutated in place.
type Msg struct {
	Topic   string
	Payload []byte
}

type Pipeline struct {
	CentreID  string
	Metrics   *metrics.Metrics
	Validator *wnm.Validator
	Dedup     *dedup.Dedup
	Publisher *mqttbroker.Publisher

	// GDC-backed per-centre dataset registration check — parses
	// GDC_URL's metadata_id,topic CSV, filtered to this centre. See
	// allowlist.GDCRegistry doc comment.
	Metadata *allowlist.GDCRegistry

	// TOPIC_URL's flat md5-hash set — used inside topics.Check() itself
	// (passed through, not applied here) for the topic-hash membership
	// test. See topics.Check() doc comment for why this lives inside
	// Check() rather than as a separate pipeline step.
	TopicHashes *allowlist.Set

	MsgCheckOption      config.CheckOption
	TopicCheckOption    config.CheckOption
	MetadataCheckOption config.CheckOption

	// Strips payload.properties.content before publish when set to
	// "discard" — a different, boolean shape than the three checks
	// above (see config.CheckOption's DeleteContentOption field doc).
	DeleteContentOption config.CheckOption

	// PubQoS is the QoS every Publish() call uses — MQTT_PUB_QOS,
	// default 0. See config.Config's MQTTPubQoS doc comment for why 0
	// is the default (dedup already handles the exactly-once concern
	// QoS 2 would otherwise buy).
	PubQoS byte

	// Monitor reports topic/schema/metadata check failures as WME
	// events on monitor/a/wis2/<centre_id> — see internal/monitor's
	// package doc comment for the full trace of what this replicates.
	// nil means reporting is off entirely (no panics — Reporter.report
	// nil-checks its receiver).
	Monitor *monitor.Reporter

	// Debug, if set, is consulted with a category name ("checks",
	// "publisher", "pubcount", or "timing") before logging that
	// category's detail — wired from cmd/antiloop/main.go's -d keyword
	// set (dbg.has). nil means no debug logging at all, same as every
	// category returning false. A func predicate rather than importing
	// the flag/keyword-set type directly so this package doesn't need
	// to know anything about how -d is parsed.
	Debug func(category string) bool

	// TopicMatch, if set, further narrows "publisher" debug logging
	// (see debugfTopic) to only topics of interest — wired from
	// cmd/antiloop/main.go's -t flag(s) (topicFilterFlag.match). nil
	// means "match everything", same as -t never having been given, so
	// this is purely opt-in on top of Debug. Deliberately not consulted
	// by the plain debugf/"checks" path — -t only ever narrows
	// subscriber/publisher, per its own doc comment.
	TopicMatch func(topic string) bool

	// timing backs the "timing" debug category — see pipelineTiming's
	// doc comment. processedCount drives its every-100 log cadence,
	// same pattern as publishCount/"pubcount" but counting every
	// process() call, not just successful publishes, since a stage can
	// be slow even on messages that get discarded partway through.
	timing         pipelineTiming
	processedCount atomic.Uint64

	// publishCount is a running tally of successful publishes, used
	// only by the "pubcount" debug category to print a lightweight
	// periodic running total (every publishCountLogInterval messages)
	// instead of the "publisher" category's full per-message
	// topic+payload log line. atomic.Uint64 since process() runs
	// concurrently across however many goroutines are draining the
	// gate/handling live messages.
	publishCount atomic.Uint64

	// ctx is stored (against the usual advice) because the gate defers
	// processing of buffered messages to an arbitrary later point — the
	// original per-call ctx from mqtt's message handler doesn't apply
	// once a message has sat in the backlog for seconds or minutes.
	// This is the app-lifetime ctx, set once at construction, not a
	// per-request context — the case that advice is actually about.
	ctx  context.Context
	gate *gate.Gate[Msg]

	// gateMu guards isPrimary/pubConnected — the gate is Open only when
	// BOTH are true, Queueing otherwise. See SetPrimary/SetPubConnected.
	gateMu       sync.Mutex
	isPrimary    bool
	pubConnected bool

	// There is deliberately no fixed worker pool / job-queue field here.
	// dispatch (below) spawns a goroutine directly per message instead
	// of scheduling onto a fixed-size pool. A fixed pool size is an
	// unscalable knob in this context — it would need manual re-tuning
	// as traffic grows, differently per centre, across however many
	// antiloop processes see real load — and it would silently cap how
	// large a dedup batch (internal/dedup's NewBatched) can ever get,
	// since a batch can't contain more concurrent callers than there
	// are workers running at once.
	//
	// Dedup batching already decouples Redis connection count from
	// message concurrency: a batch of N messages costs one pipelined
	// round trip via a small, fixed DEDUP_BATCH_CONCURRENCY, not N
	// connections. That removes the original motivation for a worker
	// pool — bounding concurrent Redis connections — so `go
	// p.process(m)` per message is a strictly simpler design that also
	// can never block the caller (unlike a channel send, which blocks
	// once its buffer fills, however generously sized), leaving no
	// queue-depth to size either. This is safe because nothing
	// process() does depends on message ordering: dedup's SET NX is
	// per-message-id; topic/schema/metadata checks are stateless per
	// message; the Redis client, the WNM validator (RWMutex), the
	// allowlist/GDC registries (RWMutex), and paho's Client.Publish are
	// all documented-safe for concurrent use. It also matches the
	// original flow's own concurrency model — no fixed worker count,
	// just as many concurrent async operations as there happen to be
	// messages in flight — which is why it scales the same way
	// regardless of fleet size, with no worker-count value to guess.
	//
	// Realistic WIS2 GTS burst sizes (thousands per second at most, not
	// millions) are well within what Go's scheduler handles as
	// transient goroutines-per-message without special tuning. If a
	// genuine flood ever needs a hard ceiling as a safety valve — not
	// for normal-case throughput tuning — a global semaphore would be
	// the place to add it; deliberately not added speculatively here.
}

// New wires up the gate (defaulting to Queueing — fail-closed, same
// posture as election.Client: don't process/publish anything until
// explicitly told this instance is primary AND at least one pub broker
// is reachable). maxQueueBacklog should be generous — the original
// flow's observed q-gate maxQueueLength values ran up to 50000; size
// for your actual per-centre message rate times the longest plausible
// time-to-promotion (election tick + fresh-window margin, seconds, but
// leave real headroom for a slow/stuck election).
//
// dedupTTL must be the exact same duration passed to the dedup layer's
// constructor (dedup.New/NewBatched) — it's used as the gate's maxAge,
// not an independently-tunable value. See internal/gate's package doc
// comment: a message that sits queued here longer than the dedup TTL
// would, if drained late, publish as an undetected duplicate (the
// instance that actually published it while this one was secondary
// set a dedup key that's since expired) as well as being stale. Callers
// must pass the caller's actual dedup TTL, not a separately-chosen
// number.
//
// No workers/queueDepth parameters — see the struct's doc comment
// above for why there's no fixed worker pool. dispatch (below) spawns
// process() directly, per message, unbounded.
func New(ctx context.Context, maxQueueBacklog int, dedupTTL time.Duration) *Pipeline {
	p := &Pipeline{ctx: ctx}
	p.gate = gate.New(maxQueueBacklog, dedupTTL, p.dispatch)
	return p
}

// dispatch is the gate's forward callback. Called both for live
// messages (state==Open) and while draining the backlog on promotion
// (gate.Open()'s loop), in both cases from whatever goroutine invoked
// Handle()/Open() (the paho subscriber goroutine, or the election
// callback goroutine on promotion) — so this must never block. A
// goroutine spawn can't block (unlike a channel send, which can once
// the channel fills) — see the struct's doc comment above for the full
// rationale for this design.
func (p *Pipeline) dispatch(m Msg) {
	go p.process(m)
}

// SetPrimary is the election client's onChange callback.
//
// The gate opens only when BOTH this instance is primary AND at least
// one pub broker is connected — see SetPubConnected, wired from
// mqttbroker.Publisher's onState callback in cmd/antiloop/main.go. This
// matches the original flow's "Monitor connection" group, where its
// Open/Queue control messages (driven by whether any of the
// MQTT_PUB_BROKER* targets is reachable) feed into the exact same
// q-gate chain as the election's own Open/Queue/Flush/Reset messages —
// both land on the same q-gates sitting immediately after the
// subscriber's mqtt-in node, i.e. before Check/Validate/Dedup, not just
// before the final Publish call. Gating only inside
// mqttbroker.Publisher, at the very end of the pipeline, would reproduce
// exactly the bug class internal/gate exists to prevent for the
// primary/secondary case: dedup's SET NX would already have permanently
// consumed the message's dedup slot before Publish ever got a chance to
// (not) deliver it. mqttbroker.Publisher's own internal queue (see its
// doc comment) is kept as a secondary safety net for the narrow race
// where a broker disconnects between this gate draining a message and
// the resulting Publish() call actually running — not the primary
// mechanism.
func (p *Pipeline) SetPrimary(isPrimary bool) {
	p.gateMu.Lock()
	p.isPrimary = isPrimary
	open := p.isPrimary && p.pubConnected
	p.gateMu.Unlock()
	p.applyGateState(open)
}

// SetPubConnected should be wired to mqttbroker.Publisher's connection
// state (AnyConnected()) — true whenever at least one MQTT_PUB_BROKER*
// target is reachable. See SetPrimary's doc comment for why this and
// isPrimary jointly control the gate rather than isPrimary alone.
func (p *Pipeline) SetPubConnected(connected bool) {
	p.gateMu.Lock()
	p.pubConnected = connected
	open := p.isPrimary && p.pubConnected
	p.gateMu.Unlock()
	p.applyGateState(open)
}

// applyGateState drains the backlog (in order) and switches to live
// pass-through when open; switches back to buffering otherwise.
// Deliberately does NOT periodically flush while queueing — see
// internal/gate's doc comment for why.
func (p *Pipeline) applyGateState(open bool) {
	if open {
		p.gate.Open()
	} else {
		p.gate.Queue()
	}
}

// BacklogLen exposes the current buffered count — wire to a gauge so a
// growing secondary-side backlog (broker up but stuck in secondary
// role longer than expected) is visible before it hits maxQueueBacklog
// and starts dropping.
func (p *Pipeline) BacklogLen() int { return p.gate.Len() }

// HandleMessage is the mqtt.MessageHandler entry point — only ever
// enqueues into the gate now; process() is where the real pipeline
// logic lives, run either immediately (already primary) or later when
// the gate drains on promotion.
func (p *Pipeline) HandleMessage(topic string, payload []byte) {
	p.gate.Handle(Msg{Topic: topic, Payload: payload})
}

func (p *Pipeline) process(m Msg) {
	topic, payload := m.Topic, m.Payload

	p.Metrics.MessagesReceived.Inc()
	p.Metrics.SetLastMessageTimestamp(p.CentreID, float64(time.Now().Unix()))

	if p.Debug != nil && p.Debug("timing") {
		if n := p.processedCount.Add(1); n%publishCountLogInterval == 0 {
			p.logTimingSummary(n)
		}
	}

	// 1. Topic routing/validation — verbatim port of "Check" plus the
	// TOPIC_URL hash-membership test (see topics.Check() doc comment),
	// only if TOPIC_CHECK_OPTION enables it. Disabled means Check()
	// never runs, so its "topic_<md5>" key and DataTopic are never
	// computed either — matches the flow (the "Check" function node
	// itself sits behind this gate, not just its failure handling).
	var chk topics.Result
	if p.TopicCheckOption.Enabled() {
		t0 := time.Now()
		chk = topics.Check(topic, p.TopicHashes)
		p.timing.topicCheck.record(time.Since(t0))
		p.debugf("checks", "topic check: topic=%q allow=%v data_topic=%q", topic, chk.Allow, chk.DataTopic)
		if !chk.Allow {
			p.Metrics.MessagesInvalidTopic.Inc()
			// Fires on both discard and verify — reporting a failure
			// doesn't depend on whether the message is also dropped;
			// the monitoring event is about something being wrong,
			// independent of the discard/verify policy choice.
			p.Monitor.ReportTopicFailure(topic, payload)
			if p.TopicCheckOption == config.CheckDiscard {
				p.debugf("checks", "topic check: DISCARDING topic=%q", topic)
				return
			}
			// verify: counted above, falls through and keeps processing.
		}
	} else {
		chk = topics.Result{Allow: true}
	}

	// 2. Schema validation (WNM, or WME for "monitor" topics) — only if
	// MSG_CHECK_OPTION enables it.
	if p.MsgCheckOption.Enabled() {
			// 2a. WNM links-rel pre-check — ported from the original flow's
			// "cache or GB ?" / "Rel ?" switches. A WNM message (not exempted
			// by NeedsRelCheck) that fails this never reaches ajv schema
			// validation, Discard ?, or Publish at all in the original flow
			// — it's a hard dead-end that only emits the monitoring event,
			// regardless of MSG_CHECK_OPTION being "discard" or "verify".
			// That's different from every other check in this function:
			// there's no verify-mode passthrough for this one specific
			// rule, and it does not increment MessagesInvalidFormat either
			// (the original flow's Rel? failure path never touches that
			// Prometheus counter). Deliberately checked before, and
			// independent of, Validate() below.
		if monitor.NeedsRelCheck(topic, p.CentreID) && !monitor.HasValidRel(payload) {
			p.debugf("checks", "schema check: DISCARDING topic=%q (WNM links require exactly one canonical/update/deletion rel)", topic)
			p.Monitor.ReportSchemaFailure(topic, payload, monitor.ErrBadRel)
			return
		}

		t0 := time.Now()
		valid, err := p.Validator.Validate(topic, payload)
		p.timing.schemaCheck.record(time.Since(t0))
		failed := err != nil || !valid
		if err != nil {
			log.Printf("[%s] schema validation error on topic %q: %v", p.CentreID, topic, err)
		}
		p.debugf("checks", "schema check: topic=%q valid=%v err=%v", topic, valid, err)
		if failed {
			p.Metrics.MessagesInvalidFormat.Inc()
			p.Monitor.ReportSchemaFailure(topic, payload, monitor.ClassifySchemaError(topic, p.CentreID, payload))
			if p.MsgCheckOption == config.CheckDiscard {
				p.debugf("checks", "schema check: DISCARDING topic=%q", topic)
				return
			}
		}
	}

	// 3. Metadata/channel-registration check — only if METADATA_CHECK_
	// OPTION enables it, and only when this message's RAW topic has
	// segment[4]=="data" (topics.DataTopicOf). Deliberately checked
	// independent of chk/TOPIC_CHECK_OPTION — see DataTopicOf's doc
	// comment for why coupling this to chk.DataTopic was a real bug,
	// since fixed. Checks against the GDC registry using the raw
	// topic path (dataTopic, e.g. "data/core/weather/.../synop"), not
	// the hashed chk.Topic key — that one's for the TOPIC_URL check
	// above, a different file with a different shape. See
	// allowlist.GDCRegistry doc comment for the full reasoning, and
	// for exactly which Redis key metadataID vs. the empty-string
	// fallback resolves to.
	if p.MetadataCheckOption.Enabled() && p.Metadata != nil {
		if dataTopic, ok := topics.DataTopicOf(topic); ok {
			t0 := time.Now()
			metadataID := extractMetadataID(payload)
			registered := p.Metadata.Has(metadataID, dataTopic)
			p.timing.metadataCheck.record(time.Since(t0))
			p.debugf("checks", "metadata check: data_topic=%q metadata_id=%q registered=%v", dataTopic, metadataID, registered)
			if !registered {
				p.Metrics.MessagesNoMetadata.Inc()
				p.Monitor.ReportMetadataFailure(topic, payload)
				if p.MetadataCheckOption == config.CheckDiscard {
					p.debugf("checks", "metadata check: DISCARDING data_topic=%q", dataTopic)
					return
				}
			}
		}
	}

	// 4. Dedup — global (no centre_id scoping): the same id can arrive
	// via another centre's relay or another Global Broker entirely —
	// see dedup package doc comment. Only reached once actually
	// processing (i.e. this instance is primary and either live or
	// draining its backlog) — never burned speculatively by a
	// secondary, which is the whole point of gating ahead of this.
	msgID := extractMessageID(payload)
	if msgID != "" && p.Dedup != nil {
		t0 := time.Now()
		seen, err := p.Dedup.Seen(p.ctx, msgID)
		p.timing.dedup.record(time.Since(t0))
		if err != nil {
			log.Printf("[%s] dedup check error: %v", p.CentreID, err)
		}
		p.debugf("checks", "dedup check: id=%q seen=%v err=%v", msgID, seen, err)
		if seen {
			p.debugf("checks", "dedup check: DISCARDING id=%q (already seen)", msgID)
			return
		}
	}

	// 5. Optional content stripping before publish.
	if p.DeleteContentOption == config.CheckDiscard {
		payload = stripPropertiesContent(payload)
	}

	// 6. Publish. No IsPrimary check here anymore — reaching this point
	// at all already implies primary (gate.Open() is what got us here,
	// either live or draining the backlog).
	t0 := time.Now()
	delivered, err := p.Publisher.Publish(topic, p.PubQoS, false, payload)
	p.timing.publish.record(time.Since(t0))
	switch {
	case errors.Is(err, mqttbroker.ErrQueued):
		p.debugfTopic("publisher", topic, "publish topic=%q: no pub broker connected, queued for later delivery", topic)
	case err != nil:
		log.Printf("[%s] publish error (delivered to %d brokers): %v", p.CentreID, delivered, err)
	}
	// %q, not %s: WNM payloads are sometimes pretty-printed JSON with
	// real embedded newlines, and journald splits a service's stdout on
	// every '\n' it sees — %q escapes them to literal \n text so one
	// log call stays one journal line (see main.go's matching "recv"
	// debug line for the fuller explanation).
	p.debugfTopic("publisher", topic, "publish topic=%q delivered=%d payload=%q", topic, delivered, truncate(payload, debugPayloadLogLimit))
	if delivered > 0 {
		p.Metrics.MessagesPublished.Inc()
		// "pubcount": a running total every publishCountLogInterval
		// messages, gated separately from "publisher" so it stays cheap
		// (and usable) at message rates where full per-message logging
		// would itself become the bottleneck being measured.
		if p.Debug != nil && p.Debug("pubcount") {
			if n := p.publishCount.Add(1); n%publishCountLogInterval == 0 {
				log.Printf("[%s] published %d messages", p.CentreID, n)
			}
		}
	}
}

// logTimingSummary prints the cumulative (since process start, not a
// sliding window — see stageStat's doc comment) average time spent per
// message in each instrumented stage. Called every publishCountLogInterval
// processed messages when the "timing" debug category is enabled — see
// pipelineTiming's doc comment for why this exists.
func (p *Pipeline) logTimingSummary(n uint64) {
	log.Printf("[%s] timing (cumulative avg, n=%d): topic=%s schema=%s metadata=%s dedup=%s publish=%s",
		p.CentreID, n,
		p.timing.topicCheck.avg(),
		p.timing.schemaCheck.avg(),
		p.timing.metadataCheck.avg(),
		p.timing.dedup.avg(),
		p.timing.publish.avg(),
	)
}

// debugf logs, prefixed with centre_id, only if the given category is
// currently enabled (via Debug) — a no-op otherwise, including when
// Debug itself is nil (no -d categories requested at all).
func (p *Pipeline) debugf(category, format string, args ...interface{}) {
	if p.Debug == nil || !p.Debug(category) {
		return
	}
	log.Printf("[%s] "+format, append([]interface{}{p.CentreID}, args...)...)
}

// debugfTopic is debugf plus one extra gate: TopicMatch, if set, must
// also match topic — this is what lets -t narrow "publisher" logging
// down to a subset of topics instead of every message. Used only at
// the publisher log sites; every other category (checks, dedup,
// timing, pubcount) goes through plain debugf and ignores -t entirely,
// per TopicMatch's own doc comment on the Pipeline struct.
func (p *Pipeline) debugfTopic(category, topic, format string, args ...interface{}) {
	if p.Debug == nil || !p.Debug(category) {
		return
	}
	if p.TopicMatch != nil && !p.TopicMatch(topic) {
		return
	}
	log.Printf("[%s] "+format, append([]interface{}{p.CentreID}, args...)...)
}

// debugPayloadLogLimit caps how much of a message payload gets written
// into a single debug log line — large enough to show a full real-world
// WNM/WME message in the common case, small enough that one oversized
// or malformed message can't flood the log/journal.
const debugPayloadLogLimit = 8192 // 8KB

// truncate caps a debug-logged payload so one huge message can't flood
// the log — duplicated from cmd/antiloop/main.go's helper of the same
// name/purpose; small enough that a shared package for just this
// isn't worth it.
func truncate(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + fmt.Sprintf("...(%d more bytes)", len(b)-max)
}

// extractMessageID pulls the top-level "id" field without a full
// schema-typed unmarshal. NOT specifically a CloudEvents field: WME
// (monitoring events) are CloudEvents-shaped, but WNM (the actual data
// notifications) are GeoJSON-based per the WIS2 Notification Message
// spec, not CloudEvents — both carry a top-level "id" regardless, which
// is all dedup needs. See dedup package doc comment.
func extractMessageID(payload []byte) string {
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return ""
	}
	return envelope.ID
}

// extractMetadataID pulls payload.properties.metadata_id, mirroring
// the original flow's "Metadata_id ?" switch exactly:
// $type(payload.properties.metadata_id) = "string" and
// $length(payload.properties.metadata_id) > 0. A missing field, a
// non-string value, or a malformed payload all collapse to "" here,
// same as that jsonata expression evaluating false — which is exactly
// what GDCRegistry.Has()'s own "" -> fall back to centre_id" branch is
// built to receive.
func extractMetadataID(payload []byte) string {
	var envelope struct {
		Properties struct {
			MetadataID string `json:"metadata_id"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return ""
	}
	return envelope.Properties.MetadataID
}

// stripPropertiesContent ports the "Delete" change node:
// delete payload.properties.content. Round-trips through a generic
// map rather than a typed struct since the pipeline doesn't otherwise
// need a full WNM/WME type — on any error, returns the original
// payload unchanged rather than dropping content-stripping silently
// AND corrupting the message.
func stripPropertiesContent(payload []byte) []byte {
	var doc map[string]interface{}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return payload
	}
	props, ok := doc["properties"].(map[string]interface{})
	if !ok {
		return payload
	}
	if _, exists := props["content"]; !exists {
		return payload
	}
	delete(props, "content")
	out, err := json.Marshal(doc)
	if err != nil {
		return payload
	}
	return out
}
