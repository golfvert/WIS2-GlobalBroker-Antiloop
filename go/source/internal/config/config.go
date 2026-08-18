// Package config loads the relay's environment into a typed struct.
//
// Field names and env var names are matched directly against the
// original Node-RED flow (flows.json) rather than assumed by naming
// convention wherever the two could plausibly differ — several env
// var names in this file look superficially predictable but are not
// what a symmetry-based guess would produce (see the doc comments on
// Host, MQTTBackup, and the MQTTPubTargets construction loop below for
// specific examples). When adding a new field, prefer grepping
// flows.json for the actual $env(...) reference over inferring a name
// from a sibling variable.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// MQTTTarget bundles a broker URL with its own credentials, keepalive,
// and TLS verification setting.
//
// Only the broker URL is ever per-target-suffixed among the 5 possible
// publish targets (MQTT_PUB_BROKER, MQTT_PUB_BROKER_2..5) — username,
// password, and keepalive are shared across all of them via a single
// unsuffixed variable each. See the MQTTPubTargets construction loop
// in Load() for how this is wired.
type MQTTTarget struct {
	URL       string
	Username  string
	Password  string
	Keepalive time.Duration
	// VerifyCert controls TLS server-certificate verification for this
	// target. Only meaningful for mqtts/wss (TLS) URLs; ignored for
	// plain mqtt/ws. Subscriber targets default to false (skip
	// verification), matching the original flow's own default —
	// publish targets default to true (verify), since nothing in the
	// flow indicates that side should behave differently.
	VerifyCert bool
}

// CheckOption mirrors MSG_CHECK_OPTION / TOPIC_CHECK_OPTION /
// METADATA_CHECK_OPTION and the differently-shaped DELETE_CONTENT_OPTION.
// Each check in the original flow is gated by two switch nodes:
//
//  1. "<X> check ?": the option must be exactly "discard" or "verify"
//     for the check to run at all. Any other value — including unset,
//     or the literal string "no" — skips the check entirely: the
//     message passes through untouched and no metric is incremented.
//  2. "Discard ?": only reached if the check above ran and failed.
//     "discard" drops the message; "verify" (the only other value
//     that can reach this point) lets it continue downstream anyway —
//     the failure is still counted, it just never blocks delivery.
//
// So exactly two values ever produce distinct behavior: "discard" and
// "verify". Everything else, including "no" or an unset variable, has
// identical effect (check disabled) — there is no third behavior
// hiding behind a third value.
type CheckOption string

const (
	CheckDiscard CheckOption = "discard"
	CheckVerify  CheckOption = "verify"

	// CheckNo is a real, deliberately-set value seen in production env
	// files (e.g. a centre disabling topic/metadata checks explicitly
	// rather than leaving the variable unset). Behaves identically to
	// an empty value — Enabled() is false either way — kept as its own
	// named constant purely so Load()'s startup validation recognizes
	// it as an expected value rather than flagging it as a typo.
	CheckNo CheckOption = "no"
)

// Enabled reports whether this check should run at all. False means:
// skip the check, pass the message through unmodified, leave the
// metric untouched.
func (c CheckOption) Enabled() bool {
	return c == CheckDiscard || c == CheckVerify
}

type Config struct {
	CentreID string

	// Host identifies which physical host this process is running on
	// — one of the deployment's several machines, not the centre_id.
	// A given centre_id's two competing instances (see
	// internal/election) normally run on two different physical hosts
	// for redundancy, so Host is what answers "which machine" once a
	// role-change log line says "primary" — without needing to
	// separately track which host was SSH'd into. Sourced from the
	// WHOAMI environment variable (matching the original flow), with
	// HOST accepted as a fallback for compatibility, and finally the
	// OS hostname if neither is set. Explicitly setting this only
	// matters where the OS hostname isn't meaningful (containers,
	// local development) or a specific label is preferred over the
	// real hostname.
	Host string

	MQTTSub       MQTTTarget
	MQTTSubTopics []string // MQTT_SUB_TOPIC is comma-separated, e.g. "origin/a/wis2/se-smhi/#,monitor/a/wis2/se-smhi/#"

	// MQTTSubQoS is the QoS antiloop subscribes at (MQTT_SUB_QOS, all
	// topics — no per-topic override). MQTTPubQoS is the QoS every
	// Publish() call fans out at (MQTT_PUB_QOS, same value to every
	// configured MQTT_PUB_BROKER* target — no per-target override).
	//
	// Both default to 0, a deliberate departure from the original
	// flow's QoS 2 subscription: QoS 2 buys exactly-once delivery at
	// the cost of a 4-part handshake per message, and antiloop already
	// has its own dedup layer downstream (internal/dedup), making QoS
	// 2's guarantee largely redundant here. Set MQTT_SUB_QOS /
	// MQTT_PUB_QOS explicitly, per centre or globally, to opt back
	// into 1 or 2 where a specific broker relationship needs it.
	MQTTSubQoS byte
	MQTTPubQoS byte

	// MQTTBackup configures an optional backup broker the subscriber
	// fails over to when the primary connection has been down for a
	// sustained period — see internal/mqttbroker's backupFailover for
	// the full state machine. Only active when MQTT_SUB_BROKER_BACKUP
	// is set; a centre with no backup broker leaves this entirely
	// unconfigured.
	MQTTBackup MQTTTarget

	// Up to 5 publish targets: MQTT_PUB_BROKER..MQTT_PUB_BROKER_5. All
	// non-empty entries are included, in order, starting from the
	// unsuffixed variable. Username, password, and keepalive are
	// shared across every target rather than configured per-target —
	// see the construction loop in Load().
	MQTTPubTargets []MQTTTarget

	// RedisURL is a JSON array of {host,port} objects describing every
	// Redis node — see internal/redisconn for how this is consumed.
	RedisURL string

	// RedisCluster selects which go-redis client internal/redisconn.New
	// builds against RedisURL. true (the default) builds a
	// cluster-aware client against every entry; false builds a plain
	// client and requires RedisURL to contain exactly one {host,port}
	// entry — internal/redisconn.New fails startup outright if it
	// contains more or fewer than one, rather than silently using the
	// first entry and discarding the rest.
	RedisCluster bool

	// MetricsAddr defaults to ":0" — bind to any free port the OS
	// hands out, rather than a fixed, manually-assigned one. This
	// avoids needing a hand-maintained per-centre port registry to
	// prevent collisions when multiple centres share a host. Set this
	// explicitly only when a known, stable port is specifically
	// required — otherwise leave it at the default and let
	// internal/traefik.Register (see cmd/antiloop/main.go) tell
	// Traefik where the OS actually bound it.
	MetricsAddr string

	// TraefikDynamicDir is where cmd/antiloop/main.go writes this
	// centre's Traefik dynamic-configuration file at startup,
	// registering its randomly-assigned metrics port — see
	// internal/traefik's package doc comment for the full mechanism.
	// Must already exist and already be watched by Traefik's own
	// file-provider configuration; antiloop only ever writes one file
	// into it and never configures Traefik itself.
	TraefikDynamicDir string

	// TraefikEntryPoint is the entryPoint name the generated router
	// attaches to, and TraefikBackendHost is the host antiloop tells
	// Traefik to reach it on:
	//
	//   - The entrypoint name must match one actually defined in that
	//     host's own Traefik static configuration. A router with no
	//     entryPoints set is silently unreachable — it builds without
	//     error but never matches any request — so this is always
	//     emitted unconditionally in the generated config, never
	//     omitted even when TraefikTLS (below) is false.
	//   - The backend host matters because "127.0.0.1" only resolves
	//     correctly when Traefik shares antiloop's network namespace
	//     (bare-metal or host-networked Traefik). When Traefik runs in
	//     a bridge-networked Docker container instead, "127.0.0.1" is
	//     the container's own loopback, not the host's, and antiloop
	//     becomes unreachable from it. Set this to whatever address
	//     resolves to the host from inside that container (e.g.
	//     host.docker.internal) in that case; leave it at the
	//     127.0.0.1 default for a bare-metal/host-networked Traefik.
	TraefikEntryPoint  string
	TraefikBackendHost string

	// TraefikTLS controls whether the generated router includes a TLS
	// termination block. Set to false when TLS termination happens
	// centrally at a separate edge proxy that fronts multiple hosts
	// (so each host's own Traefik only needs to serve plain HTTP
	// internally); set to true for a deployment with no edge proxy in
	// front, where each host's Traefik must terminate TLS itself.
	// TraefikEntryPoint should be set consistently with this choice
	// (a plain HTTP entrypoint when false, a TLS-terminating one when
	// true).
	TraefikTLS bool

	// Election is embedded in-process (internal/election) rather than
	// run as a separate binary — these two values are its tick cadence
	// and staleness cutoff.
	ElectionInterval    time.Duration
	ElectionFreshWindow time.Duration

	// DedupBatchSize/DedupBatchWait/DedupBatchConcurrency configure
	// internal/dedup.NewBatched, which pipelines multiple deduplication
	// checks into a single Redis round trip via go-redis's Pipeliner
	// rather than issuing one SET NX call per message. This keeps
	// per-process Redis connection/command overhead roughly constant
	// as message concurrency grows, instead of scaling linearly with
	// however many messages are in flight.
	//
	// DedupBatchConcurrency caps how many pipelined Exec round trips
	// can be in flight at once. This must be greater than 1 — a single
	// in-flight batch at a time means the next batch can't start
	// assembling until the current round trip completes, which turns
	// batching into a net loss (every batch pays the full
	// DedupBatchWait as added latency on top of an effectively
	// serialized round trip) rather than a throughput improvement.
	DedupBatchSize        int
	DedupBatchWait        time.Duration
	DedupBatchConcurrency int

	// DedupBatching switches between the batched pipeline above and
	// dedup.New's simpler unbatched path (one direct SET NX call per
	// message, no channel handoff, no collector goroutine). Useful as
	// an A/B toggle when tuning against a specific Redis deployment:
	// batching's own overhead (goroutine per flush, semaphore
	// acquisition, an extra channel round trip per check) only pays
	// for itself when it's amortizing a Redis round trip expensive
	// enough to matter — against a very fast local Redis, the
	// unbatched path can outperform it.
	DedupBatching bool

	SchemaWNMURL  string
	SchemaWMEURL  string
	SchemaRefresh time.Duration

	GDCURL           string // text file, not a query API — e.g. https://wmo-im.github.io/wis2-guide/gdc-all-channels-latest.txt
	TopicURL         string // same, a flat text file of hashes — e.g. .../earth-system-discipline-and-below.md5.txt
	AllowlistRefresh time.Duration

	// GBID is this broker's own identity, used as the CloudEvents
	// "source" field in the monitor heartbeat and as the report_by
	// label on every exported Prometheus metric.
	GBID string

	MsgCheckOption      CheckOption
	TopicCheckOption    CheckOption
	MetadataCheckOption CheckOption

	// DeleteContentOption has a different shape from the three checks
	// above — it's a plain on/off switch, not a discard/verify
	// tri-state: "discard" strips payload.properties.content before
	// publish; any other value publishes content unchanged. It doesn't
	// gate whether the message itself is dropped, only whether this
	// one field is stripped from it.
	DeleteContentOption CheckOption

	LogLevel string
}

func Load() (*Config, error) {
	c := &Config{
		CentreID: os.Getenv("CENTRE_ID"),
		// WHOAMI is the source of truth (matches the original flow's
		// Init node); HOST is accepted as a fallback so a not-yet-
		// updated env file keeps working; os.Hostname() is the final
		// fallback below if neither is set.
		Host: firstNonEmpty(os.Getenv("WHOAMI"), os.Getenv("HOST")),

		MQTTSub: MQTTTarget{
			URL:        os.Getenv("MQTT_SUB_BROKER"),
			Username:   os.Getenv("MQTT_SUB_USERNAME"),
			Password:   os.Getenv("MQTT_SUB_PASSWORD"),
			Keepalive:  getenvSecondsDuration("MQTT_SUB_KEEPALIVE", 60*time.Second),
			VerifyCert: getenvBool("MQTT_SUB_VERIFYCERT", false),
		},
		// The backup subscriber has no keepalive override in the
		// original flow (its keepalive is fixed at 60s in the flow's
		// own broker config, not env-driven), so none is read here —
		// mqttbroker.go's own 60s default already matches it.
		MQTTBackup: MQTTTarget{
			URL:        os.Getenv("MQTT_SUB_BROKER_BACKUP"),
			Username:   os.Getenv("MQTT_SUB_USERNAME_BACKUP"),
			Password:   os.Getenv("MQTT_SUB_PASSWORD_BACKUP"),
			VerifyCert: getenvBool("MQTT_SUB_VERIFYCERT_BACKUP", false),
		},

		RedisURL:     os.Getenv("REDIS_URL"),
		RedisCluster: getenvBool("REDIS_CLUSTER", true),

		MetricsAddr:        getenvDefault("METRICS_ADDR", ":0"),
		TraefikDynamicDir:  getenvDefault("TRAEFIK_DYNAMIC_DIR", "/etc/traefik/dynamic"),
		TraefikEntryPoint:  getenvDefault("TRAEFIK_ENTRYPOINT", "websecure"),
		TraefikBackendHost: getenvDefault("TRAEFIK_BACKEND_HOST", "127.0.0.1"),
		TraefikTLS:         getenvBool("TRAEFIK_TLS", false),

		ElectionInterval:    getenvDuration("ELECTION_INTERVAL", 2*time.Second),
		ElectionFreshWindow: getenvDuration("ELECTION_FRESH_WINDOW", 8*time.Second),

		DedupBatchSize:        getenvInt("DEDUP_BATCH_SIZE", 200),
		DedupBatchWait:        getenvDuration("DEDUP_BATCH_WAIT", 5*time.Millisecond),
		DedupBatchConcurrency: getenvInt("DEDUP_BATCH_CONCURRENCY", 16),
		DedupBatching:         getenvBool("DEDUP_BATCHING", true),

		SchemaWNMURL:  os.Getenv("SCHEMA_WNM_URL"),
		SchemaWMEURL:  os.Getenv("SCHEMA_WME_URL"),
		SchemaRefresh: getenvDuration("SCHEMA_REFRESH", time.Hour),

		GDCURL:           os.Getenv("GDC_URL"),
		TopicURL:         os.Getenv("TOPIC_URL"),
		AllowlistRefresh: getenvDuration("ALLOWLIST_REFRESH", 15*time.Minute),

		GBID: os.Getenv("GB_ID"),

		// Default is "" (empty), not "discard" — matches the original
		// flow exactly: an unset env var isn't in ["discard","verify"],
		// so the check is skipped, same as any other unrecognized
		// value. Defaulting to "discard" here would silently change
		// production behavior (checks that don't currently run would
		// start running).
		MsgCheckOption:      CheckOption(os.Getenv("MSG_CHECK_OPTION")),
		TopicCheckOption:    CheckOption(os.Getenv("TOPIC_CHECK_OPTION")),
		MetadataCheckOption: CheckOption(os.Getenv("METADATA_CHECK_OPTION")),
		DeleteContentOption: CheckOption(os.Getenv("DELETE_CONTENT_OPTION")),

		LogLevel: getenvDefault("LOG_LEVEL", "info"),
	}

	if c.Host == "" {
		if h, err := os.Hostname(); err == nil {
			c.Host = h
		} else {
			c.Host = "unknown"
		}
	}

	c.MQTTSubQoS = getenvQoS("MQTT_SUB_QOS", 0)
	c.MQTTPubQoS = getenvQoS("MQTT_PUB_QOS", 0)

	if v := strings.TrimSpace(os.Getenv("MQTT_SUB_TOPIC")); v != "" {
		for _, t := range strings.Split(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				c.MQTTSubTopics = append(c.MQTTSubTopics, t)
			}
		}
	}

	// Only the broker URL is per-target-suffixed among the 5 possible
	// publish targets — username, password, and keepalive are shared
	// across all of them via a single unsuffixed variable each.
	username := os.Getenv("MQTT_PUB_USERNAME")
	password := os.Getenv("MQTT_PUB_PASSWORD")
	keepalive := getenvSecondsDuration("MQTT_PUB_KEEPALIVE", 60*time.Second)

	suffixes := []string{"", "_2", "_3", "_4", "_5"}
	for _, sfx := range suffixes {
		url := strings.TrimSpace(os.Getenv("MQTT_PUB_BROKER" + sfx))
		if url == "" {
			continue
		}
		c.MQTTPubTargets = append(c.MQTTPubTargets, MQTTTarget{
			URL:       url,
			Username:  username,
			Password:  password,
			Keepalive: keepalive,
			// Publish targets verify TLS certificates by default,
			// unlike subscriber targets — no evidence in the original
			// flow suggests publish-side verification should be
			// skipped, so this keeps Go's ordinary secure default.
			VerifyCert: true,
		})
	}

	var missing []string
	for name, v := range map[string]string{
		"CENTRE_ID":       c.CentreID,
		"MQTT_SUB_BROKER": c.MQTTSub.URL,
		"REDIS_URL":       c.RedisURL,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(c.MQTTSubTopics) == 0 {
		missing = append(missing, "MQTT_SUB_TOPIC (at least one)")
	}
	if len(c.MQTTPubTargets) == 0 {
		missing = append(missing, "MQTT_PUB_BROKER (at least one)")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}

	// Sanity-check REDIS_URL parses as the expected JSON array shape
	// so a malformed value fails fast at startup instead of on the
	// first Redis call.
	var probe []map[string]interface{}
	if err := json.Unmarshal([]byte(c.RedisURL), &probe); err != nil {
		return nil, fmt.Errorf("REDIS_URL is not a JSON array of {host,port}: %w", err)
	}

	// "discard", "verify", and "no" are the only values with defined
	// behavior (see CheckOption's doc comment); empty/unset is also
	// expected, equivalent to "no". Only warn on something that's none
	// of those — a typo or a genuinely unsupported option value is
	// worth surfacing, but a deliberately-disabled check shouldn't be.
	for _, opt := range []struct {
		name string
		val  CheckOption
	}{
		{"MSG_CHECK_OPTION", c.MsgCheckOption},
		{"TOPIC_CHECK_OPTION", c.TopicCheckOption},
		{"METADATA_CHECK_OPTION", c.MetadataCheckOption},
		{"DELETE_CONTENT_OPTION", c.DeleteContentOption},
	} {
		if opt.val != "" && opt.val != CheckDiscard && opt.val != CheckVerify && opt.val != CheckNo {
			fmt.Fprintf(os.Stderr, "warning: %s=%q is not \"discard\", \"verify\", or \"no\" — treating as disabled, same as unset; confirm this is intended\n", opt.name, opt.val)
		}
	}

	// relay.Pipeline no longer uses a fixed worker pool, so these two
	// variables have no effect if still present in an env file — warn
	// rather than silently ignore them, so a leftover value is an
	// obvious no-op instead of a mystery.
	for _, name := range []string{"PIPELINE_WORKERS", "PIPELINE_QUEUE_DEPTH"} {
		if os.Getenv(name) != "" {
			fmt.Fprintf(os.Stderr, "warning: %s is set but no longer has any effect — relay.Pipeline no longer uses a fixed worker pool; safe to delete this line from your env file\n", name)
		}
	}

	return c, nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// firstNonEmpty returns the first non-empty string among vals, or ""
// if all are empty. Used for the WHOAMI/HOST fallback chain — see
// Host's construction in Load().
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// getenvSecondsDuration parses an env var given in plain seconds (e.g.
// "300", not a Go duration string like "300s") — this is how
// MQTT_SUB_KEEPALIVE and MQTT_PUB_KEEPALIVE are formatted in real env
// files. Empty/unset or unparseable both fall back to def silently
// (unlike getenvQoS's warn-on-bad-value posture) — a keepalive
// misconfiguration isn't a correctness issue worth failing loudly over.
func getenvSecondsDuration(key string, def time.Duration) time.Duration {
	secStr := os.Getenv(key)
	if secStr == "" {
		return def
	}
	secs, err := strconv.Atoi(secStr)
	if err != nil {
		return def
	}
	return time.Duration(secs) * time.Second
}

// getenvQoS parses an MQTT QoS level (0, 1, or 2). Empty/unset returns
// def without warning (the normal case). An unparseable or
// out-of-range value falls back to def too, but warns — startup isn't
// failed over a bad value, but a typo isn't silently accepted either.
func getenvQoS(key string, def byte) byte {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > 2 {
		fmt.Fprintf(os.Stderr, "warning: %s=%q is not 0, 1, or 2 — using default %d\n", key, v, def)
		return def
	}
	return byte(n)
}

// getenvInt parses a plain positive integer env var, falling back to
// def (and warning) on anything unparseable or <= 0.
func getenvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		fmt.Fprintf(os.Stderr, "warning: %s=%q is not a positive integer — using default %d\n", key, v, def)
		return def
	}
	return n
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// getenvBool parses a boolean env var (accepts anything
// strconv.ParseBool does: "true"/"false", "1"/"0", "t"/"f", etc.),
// falling back to def (with a warning, not a fatal error) on anything
// unparseable.
func getenvBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: %s=%q is not a valid boolean — using default %v\n", key, v, def)
		return def
	}
	return b
}
