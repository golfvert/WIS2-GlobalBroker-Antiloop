// antiloop is the per-centre relay process: same binary, same .env,
// deployed identically to 2 of the 3 hosts for a given centre_id. Role
// (primary/secondary) is decided at runtime by the embedded election
// algorithm (internal/election) running against the same Redis Cluster
// connection used for dedup — not by config, and not by a separate
// binary/process anymore. See internal/election's doc comment for why
// this used to be a subprocess and isn't anymore.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"antiloop/internal/allowlist"
	"antiloop/internal/config"
	"antiloop/internal/dedup"
	"antiloop/internal/election"
	"antiloop/internal/envfile"
	"antiloop/internal/metrics"
	"antiloop/internal/monitor"
	"antiloop/internal/mqttbroker"
	"antiloop/internal/redisconn"
	"antiloop/internal/relay"
	"antiloop/internal/traefik"
	"antiloop/internal/wnm"
)

// Version identifies this build — set at compile time via
// -ldflags "-X main.Version=...", never at runtime. Left as "dev" for
// plain `go build`/`go run` with no ldflags (local testing).
//
// Multiple binary versions can coexist on a host
// (bin/antiloop-<version>, see the Ansible repo's README), and each
// wis2node can pin its own version independently — so "which version
// is actually running" isn't obvious from a binary's filename alone
// once a process is up. Version is the runtime source of truth
// instead: logged at startup and served on GET /version (see
// metricsMux) so it's checkable without SSHing in.
var Version = "dev"

func main() {
	var dbg debugFlag
	flag.Var(&dbg, "d", "debug categories to log, comma-separated and/or repeatable (-d subscriber -d checks): "+
		"subscriber (message topic+payload as received from the broker), "+
		"publisher (topic+payload+delivery count as sent to the Global Broker), "+
		"pubcount (lightweight running total of successful publishes, logged every 100 — see relay.publishCountLogInterval — use instead of "+
		"publisher when the per-message log line itself would skew a throughput test), "+
		"timing (cumulative average time spent per message in each pipeline stage — topic/schema/metadata check, dedup, publish — "+
		"logged every 100 processed messages; use this to find which stage actually dominates a slow run instead of guessing), "+
		"checks (result of each topic/schema/metadata/dedup check, including why a message was discarded), "+
		"dedup (periodic avg-batch-size stats from the dedup Redis-pipeline batching path, logged every 50 flushes — "+
		"useful when tuning DEDUP_BATCH_SIZE/DEDUP_BATCH_WAIT, just noise otherwise), "+
		"paho (paho.mqtt.golang's own internal per-packet trace — very chatty), "+
		"all (every category above)")
	flag.Var(&dbg.topicStatic, "t", "MQTT topic filter to further narrow the subscriber/publisher debug categories to, "+
		"comma-separated and/or repeatable (-t a/b/c -t x/y/z): standard MQTT wildcard semantics, "+
		"'+' matches exactly one level, '#' (only valid as the last level) matches that level and everything after it. "+
		"Has no effect on any other -d category. Not given: subscriber/publisher log every topic, same as before. "+
		"Also live-adjustable via <centre_id>.debug 'topic:<filter>' lines, same mechanism as -d's categories.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-d category[,category...]] [-t topic-filter] <centre_id>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Reads ./common.env then ./<centre_id>.env from the current directory\nand loads them into the process environment itself — no need to\n`source` them first.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		flag.Usage()
		os.Exit(2)
	}
	centreID := args[0]

	// Surfaces paho's internal connection errors (bad URL, TLS
	// failures, DNS failures, auth rejections) instead of the total
	// silence we had before — see mqttbroker.EnableLogging's doc
	// comment for why that silence was actually dangerous combined
	// with SetConnectRetry(true), not just unhelpful. Gated on "paho"
	// specifically, not on -d being set at all: paho's DEBUG logger is
	// per-packet chatter (ping, puback, ...) unrelated to the other
	// categories, and was the thing that buried a single received
	// message under a wall of noise before this split.
	mqttbroker.EnableLogging(dbg.has("paho"))
	dbg.pahoLogging.Store(dbg.has("paho"))

	// Off unless asked for — see dedup.EnableLogging's doc comment for
	// why this used to log unconditionally (a leftover from the local
	// Colima tuning session that never got gated) and was spamming the
	// journal on real deployments as a result.
	dedup.EnableLogging(dbg.has("dedup"))
	dbg.dedupLogging.Store(dbg.has("dedup"))

	// The two Store calls above just cache the STATIC (-d) baseline —
	// dbg.dynamic is still nil at this point (watchFile/reloadFile
	// hasn't run yet), so dbg.has(...) here reads static only, which is
	// exactly the value reloadFile's first run needs to diff against.
	// See pahoLogging/dedupLogging's doc comment on the debugFlag
	// struct for why this caching exists at all.

	// Replaces the old `set -a; source ./common.env; source ./<id>.env;
	// set +a` workflow. That had a real footgun: bash's own quote-
	// stripping applied during `source`, so REDIS_URL's JSON value
	// needed exactly the right outer single-quoting to survive and got
	// silently mangled otherwise (the "invalid character 'h'" incident).
	// envfile.Load does its own minimal, predictable quote handling
	// instead of relying on a shell at all — see its doc comment.
	// common.env first (shared defaults), then the per-centre file
	// second so it can override — same precedence the old
	// EnvironmentFile= ordering had.
	//
	// Lives in internal/envfile (not private to this file) so
	// cmd/wis2nodes can load the same real common.env this process
	// does, using the exact same KEY=VALUE parsing rules, instead of
	// requiring REDIS_URL/REDIS_CLUSTER to be re-typed as shell env
	// vars separately.
	if err := envfile.Load("common.env"); err != nil {
		log.Fatalf("loading common.env: %v", err)
	}
	envFile := centreID + ".env"
	if err := envfile.Load(envFile); err != nil {
		log.Fatalf("loading %s: %v", envFile, err)
	}
	// The command-line argument is authoritative for which centre this
	// process is — override whatever CENTRE_ID the .env file itself
	// declares. In the normal case they already agree (se-smhi.env has
	// CENTRE_ID=se-smhi); this just makes a copy-paste mistake in the
	// file (wrong CENTRE_ID line left over from copying another
	// centre's file) harmless instead of a silent misconfiguration.
	os.Setenv("CENTRE_ID", centreID)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// centre_ids ending in "-gts-to-wis2" hit topics.Check()'s branch 4
	// unconditionally, which returns Allow=true and never populates
	// DataTopic — so TOPIC_CHECK_OPTION and METADATA_CHECK_OPTION are
	// no-ops for these centres no matter what they're set to (nothing
	// is ever discarded, no metadata match is ever attempted). This is
	// intentional: gts-to-wis2 feeds are legacy GTS bulletins bridged
	// onto WIS2 and are never GDC-registered by design, so gating them
	// on registration would break the entire feed. Warn at startup so
	// setting discard here isn't silently a no-op.
	if strings.HasSuffix(cfg.CentreID, "-gts-to-wis2") && (cfg.TopicCheckOption.Enabled() || cfg.MetadataCheckOption.Enabled()) {
		log.Printf("[%s] NOTE: centre_id ends in \"-gts-to-wis2\" — TOPIC_CHECK_OPTION and METADATA_CHECK_OPTION are configured but have no effect here (topics.Check() bypasses both checks unconditionally for this centre class, by design)", cfg.CentreID)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// <centre_id>.debug in the current directory, one debug category
	// per line, re-read every 10s — see debugFlag's doc comment for
	// the full semantics (additive on top of -d, live, no restart).
	debugFile := centreID + ".debug"
	go dbg.watchFile(ctx, cfg.CentreID, debugFile, 10*time.Second)

	// --- Redis cluster (shared by dedup AND election — one pooled
	// connection for both, no per-tick reconnect) ---
	//
	// MinIdleConns=cfg.DedupBatchConcurrency plus the explicit WarmUp
	// call below ensure connections to every cluster node are actually
	// established at startup — see redisconn.New and redisconn.WarmUp's
	// doc comments. A plain Ping alone verifies connectivity but only
	// actually establishes one connection, to one node, leaving every
	// other connection to be created lazily during the first real burst
	// of traffic — indistinguishable, in the "dedup" timing category,
	// from genuine per-message Redis latency.
	//
	// Sized against cfg.DedupBatchConcurrency rather than a general
	// worker-pool figure: internal/dedup.NewBatched is the one place
	// with a fixed, small cap on concurrent Redis round trips (pipelined
	// Execs), so it's the right number to warm up connections for.
	rdb, err := redisconn.New(cfg.RedisURL, cfg.DedupBatchConcurrency, cfg.RedisCluster)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	// Ping and WarmUp run against a bounded startup-only context, not
	// the app-lifetime ctx (which has no deadline of its own). If even
	// one cluster node is slow to resolve or unreachable — a
	// hostname-based cluster-announce depends on the OS resolver, so a
	// stale/missing /etc/hosts entry or a resolver hiccup is enough —
	// an unbounded wait here would hang antiloop at startup with no
	// error and no indication of which node is the problem. The bounded
	// timeout turns that into a fast, explicit dial/lookup error
	// instead.
	redisStartupCtx, cancelRedisStartup := context.WithTimeout(ctx, 10*time.Second)
	if err := rdb.Ping(redisStartupCtx).Err(); err != nil {
		cancelRedisStartup()
		log.Fatalf("redis ping failed: %v", err)
	}
	redisconn.WarmUp(redisStartupCtx, rdb, cfg.DedupBatchConcurrency)
	cancelRedisStartup()

	// --- Metrics ---
	// primaryFlag backs GET /primary (see metricsMux) — declared here,
	// before the election client exists, because the HTTP server needs
	// to start listening well before election.NewClient is constructed
	// further down. Written from the election onChange callback below,
	// read from the /primary handler; atomic.Bool is the connective
	// tissue between the two rather than threading elector itself
	// through metricsMux.
	//
	// net.Listen (not metricsSrv.ListenAndServe directly) so the
	// actual bound port is known before serving starts — needed because
	// cfg.MetricsAddr defaults to ":0" (see config.Config's MetricsAddr
	// doc comment and internal/traefik's package doc comment). The OS
	// assigns whatever free port is available, this process learns it
	// from the listener, and internal/traefik.Register writes it to a
	// Traefik dynamic-config file so Traefik can find it without any
	// manual per-centre port assignment.
	metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		log.Fatalf("[%s] metrics listen failed: %v", cfg.CentreID, err)
	}
	metricsPort := metricsLn.Addr().(*net.TCPAddr).Port

	var primaryFlag atomic.Bool
	m := metrics.New(cfg.CentreID, cfg.GBID)

	// pipeline is declared here — well before relay.New actually
	// constructs it further down — for the same reason publisher is
	// forward-declared below: metricsMux's /inject handler needs to
	// call into it, but the metrics server (deliberately) starts
	// listening before pipeline exists, so the closure passed to
	// metricsMux below captures this variable BY REFERENCE (standard
	// Go closure semantics — it reads pipeline's value at request
	// time, not at closure-creation time) rather than being handed a
	// value that would still be nil forever. getPipeline nil-checks
	// its own read for the narrow real window where that matters: a
	// request landing in the brief startup gap before relay.New runs.
	var pipeline *relay.Pipeline
	getPipeline := func() *relay.Pipeline { return pipeline }

	metricsSrv := &http.Server{Handler: metricsMux(m, primaryFlag.Load, getPipeline)}
	go func() {
		log.Printf("[%s] metrics listening on %s (port %d)", cfg.CentreID, metricsLn.Addr(), metricsPort)
		if err := metricsSrv.Serve(metricsLn); err != nil && err != http.ErrServerClosed {
			log.Printf("[%s] metrics server error: %v", cfg.CentreID, err)
		}
	}()

	// Register with Traefik (path-based: /<centre_id>/metrics,
	// /<centre_id>/health, /<centre_id>/primary — see
	// internal/traefik's package doc comment) now that the real port is
	// known. Not fatal on failure — a broker/schema/whatever hiccup
	// shouldn't be treated as more important than this being unable to
	// come up at all, but it IS worth a loud log line since it silently
	// means Traefik has no route to this centre's metrics until fixed.
	if err := traefik.Register(cfg.TraefikDynamicDir, cfg.CentreID, metricsPort, cfg.TraefikEntryPoint, cfg.TraefikBackendHost, cfg.TraefikTLS); err != nil {
		log.Printf("[%s] traefik registration failed (metrics won't be reachable via Traefik until this is fixed): %v", cfg.CentreID, err)
	}
	defer func() {
		if err := traefik.Deregister(cfg.TraefikDynamicDir, cfg.CentreID); err != nil {
			log.Printf("[%s] traefik deregistration failed: %v", cfg.CentreID, err)
		}
	}()

	// --- Schema validation ---
	// rdb is passed through so a fetch failure (GitHub down, DNS blip,
	// whatever) falls back to the last-known-good copy cached in Redis
	// rather than just failing outright — see internal/fetchcache's
	// doc comment.
	validator := wnm.New(cfg.SchemaWNMURL, cfg.SchemaWMEURL, rdb)
	if err := validator.Refresh(ctx); err != nil {
		// Non-fatal at startup by design: better to come up and reject
		// messages with a clear "validator not initialized" log line
		// than to crash-loop because a schema host had a blip.
		log.Printf("[%s] initial schema fetch failed (will retry on next refresh): %v", cfg.CentreID, err)
	}
	go refreshLoop(ctx, cfg.SchemaRefresh, func() {
		if err := validator.Refresh(ctx); err != nil {
			log.Printf("[%s] schema refresh failed: %v", cfg.CentreID, err)
		}
	})

	// --- GDC per-centre dataset registry (CSV: metadata_id,topic —
	// fetched and decoded for real, see allowlist.GDCRegistry doc
	// comment) and TOPIC_URL's flat md5 hash set (also fetched and
	// confirmed: ~2500 raw hashes, one per line, no CSV) ---
	metadataRegistry := allowlist.NewGDCRegistry(ctx, cfg.GDCURL, cfg.CentreID, rdb)
	go metadataRegistry.RunRefreshLoop(ctx, cfg.AllowlistRefresh)
	// RunIndexSyncLoop is independent of RunRefreshLoop's own HTTP-fetch
	// gate — every process warms its own fast-path cache from the
	// shared per-centre index this way, not just whichever process
	// happens to win the fetch this cycle. See allowlist.GDCRegistry's
	// package doc comment ("Go-only addition").
	go metadataRegistry.RunIndexSyncLoop(ctx)

	// TOPIC_URL's refresh cadence is intentionally NOT cfg.AllowlistRefresh
	// — it's driven by allowlist.TopicRefreshCheckInterval (1h), matching
	// the flow's own per-process "TTL" inject node exactly. Each hourly
	// tick is a cheap TTL check against this process's own sentinel key;
	// the real fetch+write only happens roughly once every 12-36h per
	// process, fully decentralized (no fleet-wide coordination) — see
	// allowlist.go's package doc comment and Set.Refresh.
	topicHashSet := allowlist.New(ctx, cfg.TopicURL, rdb)
	go topicHashSet.RunRefreshLoop(ctx, allowlist.TopicRefreshCheckInterval, "topic-hierarchy")
	go topicHashSet.RunIndexSyncLoop(ctx)

	// --- MQTT: publisher fan-out ---
	// `var` + separate assignment (not `:=`) so the onState closure can
	// legally reference `publisher` — its scope wouldn't include its own
	// initializer if declared with `:=` on one line. `pipeline` is
	// pre-declared the same way, for the same reason: the closure below
	// needs to call pipeline.SetPubConnected, but pipeline isn't
	// constructed until further down — see SetPrimary's doc comment in
	// relay/pipeline.go for why this wiring exists at all: the original
	// flow's pub-broker-connectivity Open/Queue signal feeds the SAME
	// early q-gate as the primary/secondary election result, not just
	// the final publish step.
	// mqttUUID is the one crypto.randomUUID()-equivalent value the
	// original flow generates exactly once per process (its "UUID"
	// function node, flows.json) and fans out to every single "Handle
	// connection" change node — subscriber, subscriber-backup, and every
	// MQTT_PUB_BROKER[_2..5] target alike — each of which appends its
	// own prefix (GB_ID / MQTT_CLIENTID_BACKUP / CENTRE_ID) plus this
	// UUID's own last segment to build its own broker.clientid. See
	// mqttbroker.BuildClientID's doc comment for the exact reproduction.
	// Deliberately a fresh, dedicated UUID — not elector.UUID() (built
	// further down, and used for a wholly separate purpose: Redis
	// election/liveness identity) — reproducing this specific flow
	// mechanism doesn't require reusing that one, only generating one
	// the same way (uuid.NewString()) and sharing it the same way (one
	// value, every broker connection).
	mqttUUID := uuid.NewString()

	// pipeline itself was already declared above (see the getPipeline
	// comment near metricsMux's construction) — not redeclared here,
	// just assigned below once relay.New actually runs.
	var publisher *mqttbroker.Publisher
	publisher = mqttbroker.NewPublisher(cfg.MQTTPubTargets, cfg.CentreID, mqttUUID, func(cs mqttbroker.ConnState) {
		m.AllConnected.Set(boolToFloat(publisher.AllConnected()))
		if pipeline != nil {
			pipeline.SetPubConnected(publisher.AnyConnected())
		}
	})

	// Global dedup keyspace shared across every centre_id on every host
	// — deliberately not scoped to this centre, see dedup package doc
	// comment (the same message id can legitimately arrive via another
	// centre's relay or another Global Broker entirely). TTL is 2h,
	// matching the original flow's "Save msg" change node
	// (SET <id> true NX EX 7200).
	//
	// NewBatched, not New — coalesces concurrent Seen() calls into
	// pipelined Redis round trips instead of needing one live
	// connection/goroutine per in-flight message. See dedup's batching
	// doc comment and config.Config's DedupBatchSize/DedupBatchWait doc
	// comment for the full rationale: per-call Redis latency is a
	// genuine network floor (see cmd/redislat), and pipelining spreads
	// that cost across a batch instead of paying it once per message on
	// a single connection.
	//
	// DedupBatching (DEDUP_BATCHING, default true) is an A/B toggle —
	// see config.Config's doc comment. Whether batching is worth its own
	// overhead (goroutine spawn per flush, semaphore, extra channel hop)
	// depends on real batch sizes achieved against the target Redis
	// deployment; set DEDUP_BATCHING=false to compare against the
	// unbatched path directly.
	// dedupTTL is shared with relay.New below as the gate's maxAge, not
	// just dedup's own key TTL — see relay.New's doc comment for why
	// those two must never be independently tuned.
	const dedupTTL = 2 * time.Hour

	var dd *dedup.Dedup
	if cfg.DedupBatching {
		dd = dedup.NewBatched(rdb, dedupTTL, cfg.DedupBatchSize, cfg.DedupBatchWait, cfg.DedupBatchConcurrency)
	} else {
		dd = dedup.New(rdb, dedupTTL)
	}

	// relay.New wires up the role-gated backlog buffer — see relay and
	// gate package doc comments for why this exists (dedup must never
	// run on a message a secondary is just going to discard). 50000
	// matches the largest maxQueueLength observed among the original
	// flow's q-gates; size for your real per-centre throughput if that's
	// ever not generous enough.
	//
	// No worker-count/queue-depth arguments: every message gets its own
	// goroutine, unbounded, rather than being scheduled onto a
	// fixed-size worker pool — this scales with actual message
	// concurrency instead of a manually-tuned pool size.
	pipeline = relay.New(ctx, 50000, dedupTTL)
	pipeline.CentreID = cfg.CentreID
	pipeline.Metrics = m
	pipeline.Validator = validator
	pipeline.Dedup = dd
	pipeline.Publisher = publisher
	pipeline.Metadata = metadataRegistry
	pipeline.TopicHashes = topicHashSet
	pipeline.MsgCheckOption = cfg.MsgCheckOption
	pipeline.TopicCheckOption = cfg.TopicCheckOption
	pipeline.MetadataCheckOption = cfg.MetadataCheckOption
	pipeline.DeleteContentOption = cfg.DeleteContentOption
	pipeline.PubQoS = cfg.MQTTPubQoS
	pipeline.Monitor = &monitor.Reporter{
		GlobalBrokerID: cfg.GBID,
		CentreID:       cfg.CentreID,
		Publish:        publisher.Publish,
		QoS:            cfg.MQTTPubQoS,
	}
	pipeline.Debug = dbg.has // "checks" and "publisher" categories — see pipeline.debugf
	pipeline.TopicMatch = dbg.topicMatch // narrows "publisher" logging only — see pipeline.debugfTopic

	// Establish the initial pub-connectivity state explicitly — the
	// publisher's onState closure (above) only fires on a future
	// connect/disconnect, and pipeline didn't exist yet when NewPublisher
	// ran, so any connect that happened in between was silently missed.
	pipeline.SetPubConnected(publisher.AnyConnected())

	// --- Election (embedded, same rdb as dedup) ---
	// SetPrimary is what actually opens/queues the pipeline's gate —
	// this is the fix for the message-loss-on-promotion gap: the
	// pipeline only processes (and only then dedups/publishes) once
	// this fires with isPrimary=true, draining anything buffered while
	// secondary in order first.
	elector := election.NewClient(rdb, cfg.CentreID, cfg.Host, cfg.ElectionInterval, cfg.ElectionFreshWindow)
	go elector.Run(ctx, func(isPrimary bool) {
		role := "secondary"
		if isPrimary {
			role = "primary"
		}
		// Now fires on the very first tick too (see election.Client's
		// tick() doc comment for the bug this fixes), not just on
		// later transitions — so this line is guaranteed to appear
		// once at startup regardless of which role this instance
		// starts in, and again every time the role actually changes.
		// host= is what answers "which of this centre's 2 hosts is
		// this" from the log alone.
		log.Printf("[%s] role changed -> %s (host=%s, uuid=%s, backlog=%d)", cfg.CentreID, role, cfg.Host, elector.UUID(), pipeline.BacklogLen())
		pipeline.SetPrimary(isPrimary)
		primaryFlag.Store(isPrimary)
	})

	// Fleet-wide role audit trail — records which host holds which role
	// for every centre_id in the "wis2gb:instances" hash, queryable from
	// Redis directly without checking individual host logs.
	//
	// This writes unconditionally on every tick, not just on a role
	// transition, mirroring the original flow's own "Elect" behavior:
	// its periodic inject re-runs election and re-HSETs
	// "<centre>_primary"/"<centre>_secondary" every cycle regardless of
	// whether the role actually changed. That's what makes it
	// self-healing — whoever currently holds a role keeps re-asserting
	// it as the field's last writer, so any stale/wrong value (e.g. from
	// the brief startup race where an instance can transiently pass
	// through the wrong role before settling — see
	// internal/election.Client's fresh/stale doc comment) only survives
	// one cycle before being overwritten by the real current holder. A
	// transition-only write wouldn't self-heal: once a host settles into
	// a stable role, it would never write to Redis again, leaving a
	// stale field from an earlier transient role uncorrected
	// indefinitely. Runs on its own timer here, independent of the
	// onChange callback, so it keeps re-asserting regardless of how long
	// the role stays stable.
	go func() {
		write := func() {
			role := "secondary"
			if elector.IsPrimary() {
				role = "primary"
			}
			if err := rdb.HSet(ctx, "wis2gb:instances",
				cfg.CentreID+"_"+role, cfg.Host,
				cfg.CentreID+"_"+role+"_time", time.Now().UnixMilli(),
			).Err(); err != nil {
				log.Printf("[%s] role audit HSET failed (non-fatal): %v", cfg.CentreID, err)
			}

			// Housekeeping: if a "secondary" entry hasn't been updated
			// in a while, the current primary removes it. Only the
			// primary ever does this, so a stale entry only gets one
			// cleaner rather than every remaining instance racing HDELs
			// against each other. "Stale" means the "_secondary_time"
			// field is older than
			// ELECTION_FRESH_WINDOW — the same threshold that already
			// decides freshness everywhere else in this codebase (the
			// core election algorithm's own fresh/stale cutoff, and
			// what ./wis2nodes colors a role as stale past). Since the
			// current secondary re-asserts its own "_secondary"/
			// "_secondary_time" every ELECTION_INTERVAL (same "write"
			// closure, running on that other instance), a genuinely
			// live secondary's entry is always well inside that window
			// — this only ever fires once nothing has updated it in a
			// while, meaning whoever wrote it has actually stopped.
			if role != "primary" {
				return
			}
			secTimeField := cfg.CentreID + "_secondary_time"
			secTimeStr, err := rdb.HGet(ctx, "wis2gb:instances", secTimeField).Result()
			if err != nil {
				if err != redis.Nil {
					log.Printf("[%s] stale-secondary check failed (non-fatal): %v", cfg.CentreID, err)
				}
				return // redis.Nil: no secondary has ever been recorded — nothing to clean up
			}
			secMs, err := strconv.ParseInt(secTimeStr, 10, 64)
			if err != nil {
				return // unparseable — leave it alone rather than guess
			}
			age := time.Since(time.UnixMilli(secMs))
			if age <= cfg.ElectionFreshWindow {
				return // still being actively re-asserted by whoever holds it
			}
			if err := rdb.HDel(ctx, "wis2gb:instances",
				cfg.CentreID+"_secondary", secTimeField,
			).Err(); err != nil {
				log.Printf("[%s] stale-secondary cleanup HDEL failed (non-fatal): %v", cfg.CentreID, err)
			} else {
				log.Printf("[%s] removed stale secondary audit entry (age=%s)", cfg.CentreID, age.Round(time.Second))
			}
		}
		write()
		ticker := time.NewTicker(cfg.ElectionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				write()
			}
		}
	}()

	// Fleet-wide per-process liveness key — ported from the original
	// flow's "Expire" change node + SET redis-command in its "Refresh
	// allowed topics" group: SET wis2gb:uuid_<uuid> true EX <jittered
	// 12-36h>, on the same cadence as that group's topic-hash refresh
	// (AllowlistRefresh). What reads this key isn't anything else in
	// this codebase — presumably an external fleet-inventory/monitoring
	// tool.
	go func() {
		setLiveness := func() {
			// Matches "[$random()+0.5)*86400] floor'd" exactly: a value
			// uniformly distributed over [43200, 129600) seconds, i.e.
			// 12h to 36h.
			ttl := time.Duration(43200+rand.Intn(86400)) * time.Second
			key := "wis2gb:uuid_" + elector.UUID()
			if err := rdb.Set(ctx, key, true, ttl).Err(); err != nil {
				log.Printf("[%s] uuid liveness key refresh failed (non-fatal): %v", cfg.CentreID, err)
			}
		}
		setLiveness()
		ticker := time.NewTicker(cfg.AllowlistRefresh)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				setLiveness()
			}
		}
	}()

	// Periodic heartbeat — role + backlog, once a minute, unconditionally.
	//
	// This replaces an earlier version that only logged when
	// pipeline.BacklogLen() > 0 ("gate backlog: N messages buffered").
	// That was meant to flag a problem — a stuck-secondary backlog
	// growing toward maxAge eviction, see internal/gate's package doc
	// comment — but the gate is fail-closed while secondary (see
	// CLAUDE.md's "Failover message buffering" divergence entry), so
	// backlog is >0 for the entire time an instance isn't primary. The
	// old line therefore fired every single minute for any secondary
	// instance regardless of whether anything was actually wrong,
	// which is both noisy (especially once centralized — see the
	// waloop Vector/Loki pipeline) and useless as a signal: "backlog
	// > 0" no longer distinguishes "normal secondary buffering" from
	// "about to silently lose messages to maxAge eviction."
	//
	// Logging unconditionally instead turns this into a proper
	// liveness/role heartbeat — one line per centre per host per
	// minute either way, but now the content itself is the useful
	// part (is this instance primary or secondary right now), with
	// backlog still attached for troubleshooting rather than being the
	// sole reason the line exists. Not a Prometheus metric: none of
	// the 9 confirmed flow metrics cover this, and adding an
	// unrequested one would surprise existing dashboards.
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				role := "secondary"
				if elector.IsPrimary() {
					role = "primary"
				}
				log.Printf("[%s] alive: role=%s backlog=%d", cfg.CentreID, role, pipeline.BacklogLen())
			}
		}
	}()

	// --- MQTT: subscriber (primary + backup source) ---
	sub := mqttbroker.NewSubscriber(
		cfg.MQTTSub,
		cfg.MQTTBackup,
		cfg.MQTTSubTopics,
		cfg.MQTTSubQoS, // MQTT_SUB_QOS, default 0 — see config.Config doc comment
		cfg.GBID, cfg.MQTTClientIDBackup, mqttUUID,
		func(_ mqtt.Client, msg mqtt.Message) {
			if dbg.has("subscriber") && dbg.topicMatch(msg.Topic()) {
				// -d subscriber: raw, as received, before any check/dedup/
				// gate logic touches it — this is "what did the broker
				// actually send", not "what did antiloop do with it".
				// Payload is capped so one oversized message can't flood
				// the terminal/journal. %q, not %s: WNM payloads are
				// sometimes pretty-printed JSON with real embedded
				// newlines, and journald splits a service's stdout on
				// every '\n' it sees — one Printf with embedded newlines
				// becomes several separate journal entries otherwise,
				// each stamped with the same timestamp/pid, which reads
				// as if the message were torn apart. %q escapes them to
				// literal \n text so one log call is one journal line.
				// dbg.topicMatch is a no-op (always true) unless -t was
				// given or a "topic:" line is active in <centre_id>.debug
				// — see its doc comment.
				log.Printf("[%s] recv topic=%q payload=%q", cfg.CentreID, msg.Topic(), truncate(msg.Payload(), debugPayloadLogLimit))
			}
			pipeline.HandleMessage(msg.Topic(), msg.Payload())
		},
		func(cs mqttbroker.ConnState) {
			if cs.Name == "backup" {
				m.ConnectedBackup.Set(boolToFloat(cs.Connected))
			} else {
				m.Connected.Set(boolToFloat(cs.Connected))
			}
		},
	)
	defer sub.Close()
	defer publisher.Close()

	// A periodic self-status "heartbeat" log/goroutine is not
	// implemented here — the original flow's equivalent nodes are pure
	// debug-sidebar output with no real MQTT/HTTP downstream, so there's
	// nothing to port. pipeline.Monitor (wired above) is this codebase's
	// real monitoring-event mechanism; see internal/monitor's package
	// doc comment.

	// dedup-batch=size/wait/concurrency is logged here as the closest
	// available figure for "how much concurrency is this process
	// configured for" — there's no fixed worker-pool size to report
	// (see relay.New's construction above), since every message gets
	// its own goroutine.
	log.Printf("[%s] antiloop started (version=%s, host=%s, uuid=%s, sub-topics=%v, pub-targets=%d, dedup-batch=%d/%s/%d)",
		cfg.CentreID, Version, cfg.Host, elector.UUID(), cfg.MQTTSubTopics, len(cfg.MQTTPubTargets),
		cfg.DedupBatchSize, cfg.DedupBatchWait, cfg.DedupBatchConcurrency)
	<-ctx.Done()
	log.Printf("[%s] shutting down", cfg.CentreID)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
}

// metricsMux's isPrimary reflects the latest election result — GET
// /primary ports the original flow's "Health check" group verbatim:
// 200 with no body if primary, 404 with no body if secondary, for a
// load balancer or orchestrator to route only to the primary instance.
// This is distinct from role display: role is reported via log lines
// (see the onChange callback above), not a JSON status endpoint — an
// HTTP signal an external router can act on isn't the same thing as a
// human-readable role display.
func metricsMux(m *metrics.Metrics, isPrimary func() bool, getPipeline func() *relay.Pipeline) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	// "/health", not "/healthz" — the "z" suffix is a
	// Kubernetes/Google-ism (/healthz, /readyz, /livez) that doesn't
	// mean anything in this deployment; plain /health matches
	// non-k8s convention and Traefik's own health-check documentation.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/primary", func(w http.ResponseWriter, r *http.Request) {
		if isPrimary() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	// Which binary this process actually is — see Version's doc comment.
	// Plain text, not JSON: this is for a human running curl, not
	// another system parsing it (there's nowhere else in this HTTP
	// surface that returns JSON either).
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(Version + "\n"))
	})
	mux.HandleFunc("/inject", injectHandler(getPipeline))
	return mux
}

// injectHandler replays a stored message through the exact same
// Check/Validate/Dedup/Publish path a real MQTT-received message
// takes — reproducing Node-RED's manual "inject button" capability,
// which lets an operator manually replay a message captured
// elsewhere. Reachable as POST /<centre_id>/inject via both Traefik
// tiers with no routing changes needed on either: both already use
// PathPrefix(`/<centre_id>`) + stripPrefix (see internal/traefik's
// package doc comment and templates/traefik-centre.yml.j2 in the
// Ansible repo), so this unprefixed /inject route is automatically
// reachable the same way /<centre_id>/health, /primary, and /metrics
// already are — no Traefik dynamic-config changes required.
//
// Unlike every other route on this mux, this one has a real side
// effect (it can cause a live publish to real brokers), so — unlike
// /health, /primary, /version, which don't care — it's restricted to
// POST and validates its body strictly rather than best-effort.
//
// getPipeline, not a bare *relay.Pipeline: this handler is wired in
// before relay.New actually runs (see the getPipeline closure built
// alongside metricsMux's call site in main(), and its own comment for
// why) — the indirection exists purely to survive that startup
// ordering, not for any per-request reason. Guards against the narrow
// real window (a request arriving before relay.New has run yet) with
// 503, same spirit as any other "not ready" case.
//
// The message is always injected with ForceDebug=true (see Msg's doc
// comment) — a manual replay is something an operator deliberately
// triggered and almost always wants full visibility into, and forcing
// it per-message rather than toggling -d fleet-wide means no other
// concurrently-processing message's logging is affected either way.
//
// payload is decoded as json.RawMessage, not re-marshaled through an
// interface{} round-trip — this preserves the submitted WNM's exact
// bytes (key order, spacing) rather than however Go's own json.Marshal
// would have chosen to re-serialize it, which matters since the
// injected message should reproduce exactly what was captured
// elsewhere, not a re-encoded approximation of it.
func injectHandler(getPipeline func() *relay.Pipeline) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed, use POST", http.StatusMethodNotAllowed)
			return
		}
		p := getPipeline()
		if p == nil {
			http.Error(w, "not ready yet: pipeline not initialized", http.StatusServiceUnavailable)
			return
		}
		var body struct {
			Topic   string          `json:"topic"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON body: %v", err), http.StatusBadRequest)
			return
		}
		if body.Topic == "" {
			http.Error(w, `"topic" is required`, http.StatusBadRequest)
			return
		}
		if len(body.Payload) == 0 {
			http.Error(w, `"payload" is required`, http.StatusBadRequest)
			return
		}
		p.HandleMessageWithDebug(body.Topic, []byte(body.Payload), true)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("queued\n"))
	}
}

func refreshLoop(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// debugFlag is -d's value: a set of category keywords, buildable from
// either repeated flags (-d subscriber -d checks) or a single
// comma-separated one (-d subscriber,checks) or both mixed. It
// implements flag.Value (String/Set), which is what lets flag.Var
// accept it directly instead of a plain string. "all" is a shorthand
// category meaning every other category is also considered enabled —
// see has().
//
// Categories are split out individually (rather than one single debug
// boolean) so that, for example, per-message logging and paho's very
// chatty per-packet DEBUG trace can be enabled independently — a
// single undifferentiated debug flag buries real message log lines
// under a wall of paho ping/puback traffic.
//
// static is what -d set at startup (flag.Parse time) — read-only
// after that, so safe to read from any goroutine without locking.
// dynamic is the live, re-readable half: watchFile below re-reads
// <centre_id>.debug every 10s and swaps this in wholesale, letting
// debug categories be toggled at runtime (e.g. turn on "checks" for a
// few minutes to diagnose something) without restarting the process —
// which for a live relay means dropping out of its current role and
// re-electing, not something you want to do just to see more logs.
// atomic.Pointer since it's written by the watcher goroutine and read
// by every message-handling goroutine concurrently.
//
// Effective state = static UNION dynamic: the file is purely additive
// on top of whatever -d already turned on. A category listed in the
// file is enabled; removed from the file (or the file deleted
// entirely), it goes back to disabled UNLESS it's also in static —
// -d categories can't be silently switched off by an empty/missing
// file, only ones the file itself turned on can be turned back off by
// it. That's the literal "enable/disable based on the file's content"
// behavior, scoped to what the file controls.
//
// The same file also carries -t's topic filters, one per line
// prefixed "topic:" (e.g. "topic:+/a/wis2/se-smhi/+/data#") so they
// can be narrowed/widened live too, without a restart — topicStatic/
// topicDynamic mirror static/dynamic exactly, just for filter strings
// instead of category keywords; see topicMatch. Distinct field pair
// (not folded into the same map) since a topic filter isn't a
// keyword with an on/off state, it's a string to be matched against.
type debugFlag struct {
	static  map[string]bool
	dynamic atomic.Pointer[map[string]bool]

	topicStatic  topicFilterFlag
	topicDynamic atomic.Pointer[topicFilterFlag]

	// pahoLogging/dedupLogging cache the last-APPLIED effective state
	// for the "paho"/"dedup" categories specifically. Every other
	// category is read fresh on every debugf() call via has(), so
	// there's nothing to cache. These two are different: they gate a
	// one-time package-level logger swap (mqttbroker.EnableLogging /
	// dedup.EnableLogging), not a per-call check, so something has to
	// actually re-invoke that swap when the live file flips the
	// category, not just update the `dynamic` map and assume the
	// logger picks it up on its own — it doesn't.
	//
	// Fixed 2026-08-23: before this, EnableLogging/dedup.EnableLogging
	// were only ever called once, synchronously, at startup, using the
	// STATIC (-d) value alone — reloadFile updated `dynamic` but never
	// re-ran either EnableLogging call. So enabling "paho" (or
	// "dedup") via the live <centre_id>.debug file did precisely
	// nothing: mqtt.DEBUG could only ever be turned on by a real -d
	// paho flag at process exec time. Since run-antiloop.sh's
	// systemd ExecStart has no flag-passthrough mechanism at all
	// (exec "$bin" "$centre_id", nothing else), that made "paho"
	// tracing practically unreachable on any systemd-managed centre —
	// the exact gap that blocked live packet-level diagnosis of the
	// ca-eccc-msc/bz-nms duplicate-WME investigation. See reloadFile.
	pahoLogging  atomic.Bool
	dedupLogging atomic.Bool
}

func (d *debugFlag) String() string {
	if d == nil || len(d.static) == 0 {
		return ""
	}
	return strings.Join(sortedKeys(d.static), ",")
}

func (d *debugFlag) Set(value string) error {
	if d.static == nil {
		d.static = make(map[string]bool)
	}
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			d.static[part] = true
		}
	}
	return nil
}

// has reports whether category is enabled right now, either directly
// or via the "all" shorthand, in either the static (-d) or dynamic
// (debug file) set. Matches relay.Pipeline's Debug field's signature
// (func(string) bool) so it can be passed as pipeline.Debug = dbg.has
// directly.
func (d *debugFlag) has(category string) bool {
	if d == nil {
		return false
	}
	if d.static["all"] || d.static[category] {
		return true
	}
	if dyn := d.dynamic.Load(); dyn != nil {
		m := *dyn
		if m["all"] || m[category] {
			return true
		}
	}
	return false
}

// topicMatch reports whether topic matches -t's effective filter set
// right now — topicStatic (set at startup) UNION whatever topicDynamic
// currently holds (the debug file's "topic:" lines, live-reloadable
// the same way categories are). Matches everything (returns true) when
// that combined set is empty, same "no filters = no narrowing"
// default topicFilterFlag.match itself already implements — this just
// combines the static and dynamic halves before delegating to it.
func (d *debugFlag) topicMatch(topic string) bool {
	if d == nil {
		return true
	}
	combined := make(topicFilterFlag, 0, len(d.topicStatic))
	combined = append(combined, d.topicStatic...)
	if dyn := d.topicDynamic.Load(); dyn != nil {
		combined = append(combined, (*dyn)...)
	}
	return combined.match(topic)
}

// watchFile reads path once immediately, then every interval, until
// ctx is canceled — see the debugFlag doc comment for the semantics.
// centreID is only used to prefix log lines consistently with the
// rest of the process's output.
func (d *debugFlag) watchFile(ctx context.Context, centreID, path string, interval time.Duration) {
	d.reloadFile(centreID, path)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.reloadFile(centreID, path)
		}
	}
}

// reloadFile reads path (one category keyword per line; blank lines
// and "#"-prefixed lines ignored, same conventions as loadEnvFile) and
// atomically replaces the dynamic set. A line prefixed "topic:" is a
// topic filter instead of a category keyword — e.g.
// "topic:+/a/wis2/se-smhi/+/data#" — and goes into topicDynamic
// instead, following the exact same replace-wholesale-on-every-read
// semantics. A missing or unreadable file is not an error — it just
// means no dynamic categories or topic filters are active, the
// normal/expected state when nobody's asked for runtime debug control.
// Only logs when the effective dynamic set actually changes, not on
// every 10s tick, so this doesn't itself become log noise.
func (d *debugFlag) reloadFile(centreID, path string) {
	next := map[string]bool{}
	nextTopics := topicFilterFlag{}
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[%s] debug file %s: %v", centreID, path, err)
		}
	} else {
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if filter, ok := strings.CutPrefix(line, "topic:"); ok {
				if filter = strings.TrimSpace(filter); filter != "" {
					nextTopics = append(nextTopics, filter)
				}
				continue
			}
			next[line] = true
		}
		if err := sc.Err(); err != nil {
			log.Printf("[%s] debug file %s: read error: %v", centreID, path, err)
		}
	}

	var prev map[string]bool
	if p := d.dynamic.Load(); p != nil {
		prev = *p
	}
	if !debugSetsEqual(prev, next) {
		if len(next) == 0 {
			log.Printf("[%s] debug file %s: no dynamic categories active (missing, empty, or unreadable)", centreID, path)
		} else {
			log.Printf("[%s] debug file %s -> dynamic categories: %s", centreID, path, strings.Join(sortedKeys(next), ","))
		}
	}
	d.dynamic.Store(&next)

	// Re-derive the EFFECTIVE (static UNION dynamic) state for "paho"
	// and "dedup" specifically, and re-invoke the matching
	// EnableLogging call whenever that effective state actually
	// changed — see pahoLogging/dedupLogging's doc comment on the
	// debugFlag struct for why these two categories need this and
	// nothing else does. Compared against the last-APPLIED value
	// (pahoLogging/dedupLogging), not the previous `dynamic` map,
	// since static can independently make this a no-op forever (a
	// category already on via -d has nothing left for the file to
	// toggle).
	newPaho := d.static["all"] || d.static["paho"] || next["all"] || next["paho"]
	if d.pahoLogging.Load() != newPaho {
		mqttbroker.EnableLogging(newPaho)
		d.pahoLogging.Store(newPaho)
		log.Printf("[%s] debug file %s: paho packet-level logging (mqtt.DEBUG) now %v", centreID, path, newPaho)
	}
	newDedup := d.static["all"] || d.static["dedup"] || next["all"] || next["dedup"]
	if d.dedupLogging.Load() != newDedup {
		dedup.EnableLogging(newDedup)
		d.dedupLogging.Store(newDedup)
		log.Printf("[%s] debug file %s: dedup logging now %v", centreID, path, newDedup)
	}

	var prevTopics topicFilterFlag
	if p := d.topicDynamic.Load(); p != nil {
		prevTopics = *p
	}
	if !topicFiltersEqual(prevTopics, nextTopics) {
		if len(nextTopics) == 0 {
			log.Printf("[%s] debug file %s: no dynamic topic filters active", centreID, path)
		} else {
			log.Printf("[%s] debug file %s -> dynamic topic filters: %s", centreID, path, strings.Join(nextTopics, ","))
		}
	}
	d.topicDynamic.Store(&nextTopics)
}

func debugSetsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// topicFiltersEqual compares two topicFilterFlag slices by content and
// order — order-sensitive is fine here since both sides come from the
// same source (a re-read of the same file) rather than independently
// built sets, so a real content change is what actually reorders them.
func topicFiltersEqual(a, b topicFilterFlag) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// topicFilterFlag is -t's value: zero or more MQTT topic filters,
// buildable the same way -d is (repeated flags and/or comma-separated,
// both mixed). Purely additive narrowing on top of the "subscriber"/
// "publisher" debug categories — see match. Used both as debugFlag's
// topicStatic field (the -t flag itself, fixed at startup) and as the
// type of topicDynamic's pointee (the live, file-reloadable half) —
// see debugFlag's doc comment for how those two combine.
type topicFilterFlag []string

func (t *topicFilterFlag) String() string {
	return strings.Join(*t, ",")
}

func (t *topicFilterFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*t = append(*t, part)
		}
	}
	return nil
}

// match reports whether topic matches at least one of the configured
// filters (multiple -t values are OR'd together, same as subscribing
// to several MQTT topic filters at once). No filters given at all
// means "match everything" — -t is opt-in narrowing, never a
// requirement, so subscriber/publisher logging behaves exactly as
// before when it's not used.
func (t topicFilterFlag) match(topic string) bool {
	if len(t) == 0 {
		return true
	}
	for _, filter := range t {
		if mqttTopicMatch(filter, topic) {
			return true
		}
	}
	return false
}

// mqttTopicMatch reports whether topic matches the single MQTT filter,
// per the standard wildcard rules: '+' matches exactly one topic
// level; '#' matches its own level and every level after it, and is
// only valid as the filter's last level (a '#' anywhere else is
// treated as a literal level, same as an MQTT broker would reject it
// as malformed rather than silently special-case it — this is a
// debug-only convenience, not a broker, so it just won't match).
func mqttTopicMatch(filter, topic string) bool {
	filterLevels := strings.Split(filter, "/")
	topicLevels := strings.Split(topic, "/")
	for i, fl := range filterLevels {
		if fl == "#" && i == len(filterLevels)-1 {
			return true
		}
		if i >= len(topicLevels) {
			return false
		}
		if fl != "+" && fl != topicLevels[i] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

// debugPayloadLogLimit caps how much of a message payload gets written
// into a single debug log line — large enough to show a full real-world
// WNM/WME message in the common case, small enough that one oversized
// or malformed message can't flood the log/journal.
const debugPayloadLogLimit = 8192 // 8KB

// truncate caps a debug-logged payload so one huge message can't flood
// the log; appends a marker showing how much was cut so it's obvious
// the line was shortened, not that the message was actually that short.
func truncate(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + fmt.Sprintf("...(%d more bytes)", len(b)-max)
}
