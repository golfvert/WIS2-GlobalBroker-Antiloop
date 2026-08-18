// Package monitor builds and publishes WIS2 monitoring-event (WME)
// messages reporting that a specific relayed message failed a check —
// NOT a periodic self-status heartbeat. A periodic self-status
// heartbeat has no real counterpart in the original flow: its
// "Log"/"Show" inject nodes wire into nothing but plain Node-RED
// `debug` sidebar nodes — developer debug-print statements, never
// published anywhere, no MQTT or HTTP downstream at all. A real
// self-status heartbeat would be a genuine future feature, not
// something this flow ever actually implemented.
//
// What DOES get published to a monitor/... topic is a different
// mechanism: a "Cloud" change node (reached via a "Monitor ?" switch,
// itself fed by a "UUID" function sitting at the convergence point of
// three independent check-failure branches) builds a WME event
// reporting that the current message failed the topic, schema, or
// metadata check.
//
//   - Gated by "Monitor ?": only fires when the ORIGINAL topic does NOT
//     itself contain "monitor" — otherwise a failure on a monitor/...
//     topic would generate a monitoring event about a monitoring
//     event, forever. Reporter.Report enforces this.
//   - Fires on BOTH "discard" and "verify" — unlike discard/verify's
//     effect on whether the message itself continues downstream,
//     reporting the failure doesn't depend on it. Callers
//     (internal/relay) call Report as soon as a check fails, before
//     deciding whether to discard.
//   - Published via the same fan-out used for regular relayed
//     messages — confirmed by tracing "Cloud"'s second output through
//     "link out 34" into "link in 23", the same junction the real WNM
//     publish path uses to reach BROKER_2..BROKER_5 — i.e. ALL
//     configured MQTT_PUB_BROKER* targets, not just the first. Wire
//     Reporter.Publish to mqttbroker.Publisher.Publish directly.
//   - QoS 0, retain false on the original mqtt-out node — already
//     matches this codebase's qos-0-everywhere default; Reporter.QoS
//     should be set from config.Config.MQTTPubQoS.
//
// Two simplifications versus the original flow, flagged rather than
// silently ported:
//
//   - The topic-check-failure branch has a second, rarer path (behind
//     a Redis-backed "Result" switch whose exact trigger condition
//     wasn't fully pinned down) that sets errors to a "Metadata topic
//     should not be deeper than origin/a/wis2/centre-id/metadata..."
//     grace-period string. Not reproduced — ReportTopicFailure always
//     reports with a nil errors field.
//   - A WNM payload missing its top-level "id" field fires TWO separate
//     events in the original flow: one type
//     int.wmo.wis.wme.event.wnm.validation.format (via a dedicated
//     "id ?" switch, with a hardcoded ajv-shaped error object) and one
//     type int.wmo.wis.wme.wnm.validation.schema (via a static "No id
//     in WNM" string). This port collapses that into a single
//     validation.schema event with errors == ErrNoID — one event, not
//     two duplicates for the same root cause.
package monitor

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	dataSchemaURL = "https://schemas.wmo.int/wme/1.0.0/schemas/wis2-event-message-encoding-bundled.json"
	conformsToURL = "http://wis.wmo.int/spec/wme/1/conf/monitoring-event-message-core"

	// TypeTopic/TypeMetadata/TypeSchema are the CloudEvents "type"
	// values, ported verbatim from the flow's three "Exception"/"Add
	// title" change nodes.
	TypeTopic    = "int.wmo.wis.wme.wnm.validation.topic"
	TypeMetadata = "int.wmo.wis.wme.wnm.validation.metadata"
	TypeSchema   = "int.wmo.wis.wme.wnm.validation.schema"

	titleTopic    = "WIS2 Notification Message published on a invalid topic"
	titleMetadata = "Missing Metadata record for data announced in WIS2 Notification Message"
	titleSchema   = "WIS2 Notification Message not compliant with the defined schema"
)

// ErrNotJSON, ErrNoID, and ErrBadRel are the three static error values
// the original flow's "Error" change nodes use for specific,
// cheaply-detectable schema-failure sub-causes (see ClassifySchemaError
// and the package doc comment). Ported verbatim, typo included
// ("pertiod").
var (
	ErrNotJSON = []string{"Notification Message is not JSON"}
	ErrNoID    = []string{"No id in WNM"}
	ErrBadRel  = []string{"WNM links require one and only one of rel=canonical, rel=update, rel=deletion. Please update before the end of the grace pertiod (2027-06-30)"}
)

type cloudEvent struct {
	SpecVersion     string    `json:"specversion"`
	Type            string    `json:"type"`
	Source          string    `json:"source"`  // this Global Broker's own id (GB_ID)
	Subject         string    `json:"subject"` // centre_id
	ID              string    `json:"id"`      // fresh uuid per event
	Time            string    `json:"time"`    // millisecond ISO8601
	DataContentType string    `json:"datacontenttype"`
	DataSchema      string    `json:"dataschema"`
	Data            eventData `json:"data"`
}

type eventData struct {
	ConformsTo []string     `json:"conformsTo"`
	Channel    string       `json:"channel"` // the original message's topic (mqtttopic in the flow)
	Content    eventContent `json:"content"`
	Severity   string       `json:"severity"` // always "ERROR" in the source flow
}

type eventContent struct {
	WNM    interface{} `json:"wnm"` // the original message's payload (mqttpayload), parsed
	Title  string      `json:"title"`
	Errors interface{} `json:"errors,omitempty"`
}

// Reporter builds and publishes WME validation-failure events for one
// centre_id — construct one, share it across every check in the
// pipeline (internal/relay.Pipeline.Monitor).
type Reporter struct {
	GlobalBrokerID string // GB_ID
	CentreID       string

	// Publish sends the built event's (topic, payload) onward — wire
	// this to the app's existing fan-out publisher
	// (mqttbroker.Publisher.Publish), which already publishes to every
	// configured MQTT_PUB_BROKER* target. Not called at all if Reporter
	// is nil or Publish is nil (reporting is fully optional; a nil
	// Reporter is the zero-config "don't report" state).
	Publish func(topic string, qos byte, retained bool, payload []byte) (delivered int, err error)

	// QoS applied to the published event — set from config.Config's
	// MQTTPubQoS (default 0, same as everything else).
	QoS byte
}

// ReportTopicFailure/ReportMetadataFailure/ReportSchemaFailure build
// and publish one WME event each, for the matching check having
// failed. originalTopic/originalPayload must be the message exactly
// AS RECEIVED — a snapshot taken before any check mutates topic or
// payload (mqtttopic/mqttpayload in the flow's own "Save msg" node).
//
// All three no-op (do not publish) if originalTopic itself contains
// "monitor" — see package doc comment's "Monitor ?" gate.
func (r *Reporter) ReportTopicFailure(originalTopic string, originalPayload []byte) {
	r.report(originalTopic, originalPayload, TypeTopic, titleTopic, nil)
}

func (r *Reporter) ReportMetadataFailure(originalTopic string, originalPayload []byte) {
	r.report(originalTopic, originalPayload, TypeMetadata, titleMetadata, nil)
}

// ReportSchemaFailure's errors param should be the result of
// ClassifySchemaError (one of ErrNotJSON/ErrNoID/ErrBadRel, or nil for
// a generic/undetermined schema mismatch — matching the original
// flow's real ajv-validate failure path, which also leaves errors
// unset).
func (r *Reporter) ReportSchemaFailure(originalTopic string, originalPayload []byte, errors interface{}) {
	r.report(originalTopic, originalPayload, TypeSchema, titleSchema, errors)
}

func (r *Reporter) report(originalTopic string, originalPayload []byte, eventType, title string, errors interface{}) {
	if r == nil || r.Publish == nil || strings.Contains(originalTopic, "monitor") {
		return
	}

	var wnm interface{}
	if err := json.Unmarshal(originalPayload, &wnm); err != nil {
		// Original payload wasn't valid JSON — embed as a raw string
		// rather than dropping it; still useful context in the event,
		// and this is itself often exactly why the check failed.
		wnm = string(originalPayload)
	}

	ev := cloudEvent{
		SpecVersion:     "1.0",
		Type:            eventType,
		Source:          r.GlobalBrokerID,
		Subject:         r.CentreID,
		ID:              uuid.NewString(),
		Time:            time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		DataContentType: "application/json",
		DataSchema:      dataSchemaURL,
		Data: eventData{
			ConformsTo: []string{conformsToURL},
			Channel:    originalTopic,
			Content: eventContent{
				WNM:    wnm,
				Title:  title,
				Errors: errors,
			},
			Severity: "ERROR",
		},
	}

	body, err := json.Marshal(ev)
	if err != nil {
		return
	}
	topic := "monitor/a/wis2/" + r.CentreID
	r.Publish(topic, r.QoS, false, body)
}

// ClassifySchemaError best-effort-matches a schema validation failure
// to one of the three static error strings the original flow's "Error"
// nodes use for specific, cheaply-detectable sub-causes — invalid JSON,
// missing "id", or a WNM "links" array without exactly one
// canonical/update/deletion entry. Returns nil (generic/undetermined
// mismatch) if none apply, same as the original flow's real
// ajv-driven failure path.
//
// This is independent of, and does not affect, the actual pass/fail
// decision — that's still entirely wnm.Validator.Validate()'s call.
// This only chooses what to put in the reported event's errors field
// when Validate() has already said no. (The links-rel sub-case is
// additionally enforced as its own hard pre-check, independent of
// Validate() — see NeedsRelCheck/HasValidRel and internal/relay's
// process(), which is the actual gate; this function only picks error
// text for whatever already failed.)
func ClassifySchemaError(topic, centreID string, payload []byte) interface{} {
	var doc map[string]interface{}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return ErrNotJSON
	}

	if id, ok := doc["id"].(string); !ok || id == "" {
		return ErrNoID
	}

	if NeedsRelCheck(topic, centreID) && !HasValidRel(payload) {
		return ErrBadRel
	}
	return nil
}

// NeedsRelCheck reports whether topic/centreID are subject to the WNM
// links-rel requirement at all. Ported verbatim from flows.json's
// "cache or GB ?" switch: exempt if centreID contains "-global-broker"
// or topic contains "cache/a/wis2". Also exempt for monitor/... topics
// (WME/CloudEvents messages there don't carry a "links" array at all —
// not itself in flows.json's condition, but Reporter.report no-ops on
// monitor topics anyway, and the rel requirement is WNM-specific by
// construction).
func NeedsRelCheck(topic, centreID string) bool {
	if strings.Contains(topic, "monitor") {
		return false
	}
	if strings.Contains(centreID, "-global-broker") {
		return false
	}
	if strings.Contains(topic, "cache/a/wis2") {
		return false
	}
	return true
}

// HasValidRel reports whether payload's top-level "links" array has
// exactly one entry with rel canonical/update/deletion — ported
// verbatim from flows.json's "Rel ?" switch
// ($count(links[rel="canonical"]) + $count(links[rel="update"]) +
// $count(links[rel="deletion"]) = 1). A payload that isn't valid JSON,
// or has no "links" array at all, fails this (count 0 != 1) rather
// than being treated as N/A.
func HasValidRel(payload []byte) bool {
	var doc map[string]interface{}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return false
	}
	links, _ := doc["links"].([]interface{})
	count := 0
	for _, l := range links {
		lm, ok := l.(map[string]interface{})
		if !ok {
			continue
		}
		switch lm["rel"] {
		case "canonical", "update", "deletion":
			count++
		}
	}
	return count == 1
}
