// GDCRegistry parses GDC_URL, which is CSV, not a flat allowlist: each
// line is `<metadata_id>,<topic>`, e.g.
//
//	urn:wmo:md:ae-ncm:observations.surface.surface-synop,data/core/weather/surface-based-observations/synop
//
// This is what the original flow's "Prepare"/"Query"/"Save" nodes
// parse: "Prepare" does `$split(metadata,":")[3]` to pull the
// centre_id out of the URN (urn:wmo:md:<centre_id>:<dataset>,
// 0-indexed so [3] is the 4th segment), pairing it with the topic. The
// id embeds a centre, which is what makes this a *per-centre*
// registration check, not a global one.
//
// The metadata check this backs answers: "is this centre registered in
// the WIS2 Global Discovery Catalogue to publish on this topic?" — a
// different question from topics.Check()'s topic-hash membership test
// (is the topic *pattern* well-formed at all, checked against
// TOPIC_URL). Two different files, two different questions, both
// gated by their own *_CHECK_OPTION.
//
// Redis-backed, matching the original flow exactly (fixed 2026-08-20 —
// an earlier version of this file was in-memory-only; see git history /
// CLAUDE.md's divergence list for why that was a real bug, not just a
// style difference): the flow's "Prepare"/"Save" nodes write, for
// EVERY line in the GDC file (every centre, not just this process's
// own), two Redis keys — "metadata_id:<full_urn>:<topic>" and
// "metadata_id:<centre_id>:<topic>" — each `SET ... EX 1209600` (14
// days). Its "Query"/"Get"/"Valid ?" chain checks membership with a
// plain Redis GET (null vs non-null) against whichever of those two
// keys applies to a given message (see Has's doc comment). Refresh() and
// Has() below reproduce exactly that: same key format, same TTL, same
// shared fleet-wide refresh gate (metadata_id_refresh, keyed off a
// flat 600000ms/10min cooldown, exactly like the flow's own "Soon ?"
// switch) so a Go process refreshing the registry and a Node-RED
// process refreshing it cooperate on the same rate limit instead of
// each hammering GDC_URL on its own schedule.
//
// The in-memory map (this process's own centre_id's entries only) is
// kept IN ADDITION to the Redis-backed check, purely as a fast-path
// cache so the hot per-message path doesn't pay a Redis round trip for
// every single check — Has() only ever falls through to Redis on a
// local miss. It is never the sole source of truth: even if this
// process's own last GDC_URL fetch failed, is stale, or the shared
// refresh gate skipped it entirely this tick because some OTHER
// process (Go or Node-RED) refreshed recently, Has() still gets the
// correct, current answer from Redis. That's the actual property being
// fixed here — a single process's own fetch failing used to silently
// disable the metadata check for every message it handled; now it
// degrades to "one extra Redis round trip per check", same as
// Node-RED always paid anyway.
//
// The fast-path cache covers BOTH shapes Has() can be asked about, not
// just the "no explicit metadata_id" case: it's keyed by composite
// "<key>:<dataTopic>" strings, one entry for the full URN and one for
// the bare centre_id, for every GDC line belonging to g.centreID — the
// exact same two variants the flow itself (and writeRegistry below)
// write to Redis. A WNM that declares its OWN centre's metadata_id
// explicitly (the common case — see extractMetadataID in
// relay/pipeline.go) hits this cache exactly as often as one that
// omits it and falls back to centre_id. A metadata_id naming some
// OTHER centre entirely (WIS2 permits this, but it's the unusual case)
// is never in this process's own cache — see the per-centre index
// mechanism below for how that's still made cheap, not just correct.
//
// Go-only addition, flagged (see CLAUDE.md's divergence list): at real
// fleet size, the shared metadataRefreshGateKey cooldown means only
// ONE process fleet-wide actually performs the HTTP fetch (and
// populates ITS OWN cache from it) every ~10 minutes — every other
// process's cache would otherwise stay cold indefinitely, falling
// through to Redis on nearly every check rather than occasionally.
// Refresh() below also writes a small, per-centre Redis SET
// (wis2gb:gdc_index:{<centre_id>}, written for EVERY centre found in
// the file, not just g.centreID) that ANY process can rebuild its own
// cache from with a single SMEMBERS call — see syncFromIndex/
// RunIndexSyncLoop, and allowlist.go's package doc comment for why
// this is a SET index rather than every process periodically SCANning
// the shared Redis cluster. Node-RED has no equivalent — it has no
// in-memory cache at all, so it never needed one.
package allowlist

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"antiloop/internal/fetchcache"
)

// metadataIDTTL is the flow's own EX 1209600 (14 days) on every
// "metadata_id:*" key it writes (flows.json's "Prepare" node,
// $split... 1209600 literal) — reused verbatim so a Go-written key and
// a Node-RED-written key expire on the same schedule.
const metadataIDTTL = 14 * 24 * time.Hour

// metadataRefreshGateKey/metadataRefreshCooldown mirror the flow's
// shared "metadata_id_refresh" key and its "Soon ?" switch
// ($toMillis($now()) - $number(payload) >= 600000) exactly — same key
// name (so a Go and a Node-RED process genuinely share one fleet-wide
// rate limit, not two separate ones that happen to look similar), same
// 10-minute cooldown. Checked/armed via allowlist.go's shared
// refreshedRecently/armRefreshGate — Set (TOPIC_URL) uses the same two
// functions against its own, Go-only gate key; see that file's doc
// comment for why TOPIC_URL needs this too despite having no
// equivalent named key in the original flow to match.
const (
	metadataRefreshGateKey  = "metadata_id_refresh"
	metadataRefreshCooldown = 10 * time.Minute
)

type GDCRegistry struct {
	url      string
	centreID string

	httpClient *http.Client
	rdb        redis.Cmdable
	// ctx is the app-lifetime context (set once at construction, like
	// relay.Pipeline's own ctx field — see that struct's doc comment
	// for why this is fine here: Has() runs on the per-message hot
	// path, long after any single request's context would still be
	// meaningful, so there is no single caller ctx to thread through
	// the HashSet-style interface Has() is called through). Each Has()
	// call derives its own short-timeout child from this so a slow or
	// hung Redis doesn't stall a message-processing goroutine forever.
	ctx context.Context

	mu sync.RWMutex
	// topics holds composite "<key>:<dataTopic>" entries — key is
	// either the full URN or the bare centre_id, dataTopic is e.g.
	// "data/core/.../synop" — for every GDC line belonging to
	// g.centreID. Fast-path cache only, see package doc comment; never
	// the sole source of truth.
	topics map[string]struct{}
}

// rdb backs both the Redis fallback for when url can't be reached (see
// internal/fetchcache's doc comment) and the shared registry itself
// (see package doc comment) — nil disables both, degrading to the
// in-memory-only behavior this package had before the Redis-backed
// rewrite.
func NewGDCRegistry(ctx context.Context, url, centreID string, rdb redis.Cmdable) *GDCRegistry {
	return &GDCRegistry{
		url:        url,
		centreID:   centreID,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		rdb:        rdb,
		ctx:        ctx,
		topics:     map[string]struct{}{},
	}
}

func (g *GDCRegistry) Refresh(ctx context.Context) error {
	if g.url == "" || g.centreID == "" {
		return nil
	}

	if g.rdb != nil {
		recently, err := refreshedRecently(ctx, g.rdb, metadataRefreshGateKey, metadataRefreshCooldown)
		if err != nil {
			log.Printf("GDC registry: refresh-gate check failed (proceeding with fetch anyway): %v", err)
		} else if recently {
			// Some other process in the fleet (Go or Node-RED) refreshed
			// the shared registry within the last 10 minutes — mirrors
			// the flow's own "Soon ?" gate on this exact key. Skip the
			// HTTP fetch and Redis rewrite this tick; Has() below still
			// answers correctly regardless, from whatever the shared
			// registry already has.
			return nil
		}
	}

	body, usedFallback, err := fetchcache.Fetch(ctx, g.httpClient, g.rdb, g.url, "wis2gb:cache:gdc")
	if err != nil {
		return err
	}
	if usedFallback {
		log.Printf("GDC registry: %s unreachable, using last-known-good copy cached in Redis", g.url)
	}

	next := make(map[string]struct{})          // this process's own fast-path cache — composite "<key>:<topic>", see struct field doc comment
	var registryWrites []redisEntry            // every centre — the individual metadata_id:<key>:<topic> keys, see writeRegistry
	indexByCentre := make(map[string][]string) // centre_id -> composite "<key>:<topic>" entries, every centre — feeds each centre's fast-path index SET, see writeIndex
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		metadataID, topic, ok := strings.Cut(line, ",")
		if !ok {
			continue // malformed line — skip rather than fail the whole refresh
		}
		// urn:wmo:md:<centre_id>:<dataset> — 0-indexed split, [3] is
		// the 4th colon-separated segment. Matches the "Prepare" node's
		// $split(metadata,":")[3] exactly.
		urnParts := strings.Split(metadataID, ":")
		if len(urnParts) < 4 {
			continue
		}
		centreID := urnParts[3]

		// Both keys, every centre — matches the flow's "Prepare" node
		// writing BOTH "metadata_id:<urn>:<topic>" and
		// "metadata_id:<centreid>:<topic>" for every line, not just
		// this process's own centre_id. This is the shared, fleet-wide
		// registry; filtering it to one centre here would defeat the
		// point of writing it to Redis at all.
		registryWrites = append(registryWrites,
			redisEntry{key: metadataID, topic: topic},
			redisEntry{key: centreID, topic: topic},
		)

		// Same two variants, composite-keyed, filed under centreID's
		// own index regardless of whether centreID happens to be
		// g.centreID — this is what lets EVERY process (not just this
		// one) warm its own cache for its own centre via syncFromIndex,
		// see package doc comment.
		indexByCentre[centreID] = append(indexByCentre[centreID],
			metadataID+":"+topic,
			centreID+":"+topic,
		)

		if centreID == g.centreID {
			// This process's own immediate fast-path cache — covers
			// both the explicit-metadata_id and centre_id-fallback
			// shapes Has() can be asked about (see struct field doc
			// comment), populated right away rather than waiting for
			// this process's own next syncFromIndex tick.
			next[metadataID+":"+topic] = struct{}{}
			next[centreID+":"+topic] = struct{}{}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	g.topics = next
	g.mu.Unlock()

	// Only write back / re-arm the shared gate on a genuine fresh 200
	// from GDC_URL — matches the flow, whose "Refresh" (gate bump) and
	// "Prepare"/"Save" (registry write) nodes only ever fire off its
	// "200 ?" switch's true branch. Re-deriving from a stale Redis
	// fallback body would just rewrite Redis with what Redis already
	// has, and would incorrectly re-arm the gate as if this were a
	// fresh fetch, delaying some OTHER process's legitimate attempt to
	// actually reach the real URL.
	if !usedFallback && g.rdb != nil {
		if err := writeRegistry(ctx, g.rdb, "metadata_id:", metadataIDTTL, registryWrites); err != nil {
			log.Printf("GDC registry: writing shared registry to redis failed (this process's own in-memory cache is still up to date, %d entries): %v", len(next), err)
		} else if err := armRefreshGate(ctx, g.rdb, metadataRefreshGateKey); err != nil {
			log.Printf("GDC registry: updating refresh gate failed (non-fatal — next refresh may re-fetch sooner than necessary): %v", err)
		}

		// Fast-path index for every centre found in the file, not just
		// g.centreID — see package doc comment's "Go-only addition"
		// section. Best-effort per centre: one centre's write failing
		// doesn't block the others, and doesn't affect correctness
		// either way (Has() always falls back to a direct per-key
		// Redis EXISTS on any cache/index miss).
		for centreID, entries := range indexByCentre {
			key := fmt.Sprintf("wis2gb:gdc_index:{%s}", centreID)
			if err := writeIndex(ctx, g.rdb, key, entries, metadataIDTTL); err != nil {
				log.Printf("GDC registry: writing fast-path index for centre_id=%s failed (non-fatal — that centre's processes fall back to per-key redis checks): %v", centreID, err)
			}
		}
	}
	return nil
}

// syncFromIndex refreshes g's in-memory fast-path cache from
// wis2gb:gdc_index:{g.centreID} — see package doc comment's "Go-only
// addition" section. Runs on its own indexSyncInterval timer
// (RunIndexSyncLoop), entirely decoupled from Refresh()'s own
// HTTP-fetch/gate cadence, so a process that rarely or never wins that
// gate still gets a warm cache from whichever process did. An empty
// read (nobody has published this centre's index yet) deliberately
// leaves the existing cache alone rather than clearing it.
func (g *GDCRegistry) syncFromIndex(ctx context.Context) {
	if g.rdb == nil || g.centreID == "" {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, redisCheckTimeout)
	defer cancel()
	key := fmt.Sprintf("wis2gb:gdc_index:{%s}", g.centreID)
	members, err := g.rdb.SMembers(rctx, key).Result()
	if err != nil {
		log.Printf("GDC registry: fast-path index sync failed (serving previous cache, %d entries): %v", g.Len(), err)
		return
	}
	if len(members) == 0 {
		return
	}
	next := make(map[string]struct{}, len(members))
	for _, m := range members {
		next[m] = struct{}{}
	}
	g.mu.Lock()
	g.topics = next
	g.mu.Unlock()
}

// RunIndexSyncLoop refreshes this process's fast-path cache from the
// shared per-centre index SET immediately, then every
// indexSyncInterval until ctx is canceled. Safe/expected to run on
// every process regardless of whether it ever wins Refresh()'s own
// HTTP-fetch gate — see syncFromIndex and the package doc comment.
func (g *GDCRegistry) RunIndexSyncLoop(ctx context.Context) {
	g.syncFromIndex(ctx)
	ticker := time.NewTicker(indexSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.syncFromIndex(ctx)
		}
	}
}

// Has answers "is this centre (or an explicitly-declared metadata_id)
// registered in the WIS2 GDC to publish on this data topic?" — mirrors
// the flow's "Metadata_id ?" / "Query" / "Get" / "Valid ?" chain
// exactly (flows.json's metadata-check group):
//
//   - metadataID is payload.properties.metadata_id (see
//     extractMetadataID in relay/pipeline.go), if present and
//     non-empty: the key checked is "metadata_id:<metadataID>:<dataTopic>".
//   - Otherwise (metadataID == ""): falls back to
//     "metadata_id:<g.centreID>:<dataTopic>" — the flow's own
//     $flowContext("centreid") fallback.
//
// dataTopic is topics.Result.DataTopic / topics.DataTopicOf's raw
// "data/core/.../synop" path, NOT the hashed key from the TOPIC_URL
// check — a different file, a different question, see package doc
// comment.
//
// Checks the in-memory fast-path cache first — covers BOTH the
// explicit-metadataID and centre_id-fallback shapes equally, see
// struct field doc comment — and trusts a hit immediately, no Redis
// round trip. A miss — including every case where metadataID names
// some OTHER centre than g.centreID entirely, which this process's own
// cache/index never covers, by design (see package doc comment) —
// falls through to a Redis EXISTS on the exact key Node-RED itself
// would GET (EXISTS instead of GET: the flow only ever checks
// null/non-null, never the stored value, so EXISTS is a strictly
// cheaper equivalent). A Redis error is logged and treated as "not
// registered", same fail-safe posture as the rest of this package.
func (g *GDCRegistry) Has(metadataID, dataTopic string) bool {
	key := metadataID
	if key == "" {
		key = g.centreID
	}
	entryKey := key + ":" + dataTopic

	g.mu.RLock()
	_, ok := g.topics[entryKey]
	g.mu.RUnlock()
	if ok {
		return true
	}

	if g.rdb == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(g.ctx, redisCheckTimeout)
	defer cancel()
	n, err := g.rdb.Exists(ctx, "metadata_id:"+entryKey).Result()
	if err != nil {
		log.Printf("GDC registry: redis check failed for key=%q data_topic=%q: %v", key, dataTopic, err)
		return false
	}
	return n > 0
}

func (g *GDCRegistry) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.topics)
}

func (g *GDCRegistry) RunRefreshLoop(ctx context.Context, interval time.Duration) {
	if err := g.Refresh(ctx); err != nil {
		log.Printf("GDC registry: initial refresh failed: %v", err)
	} else {
		log.Printf("GDC registry: %d topics registered for centre_id=%s (own-centre fast-path cache; Redis-backed check covers every centre regardless)", g.Len(), g.centreID)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := g.Refresh(ctx); err != nil {
				log.Printf("GDC registry: refresh failed (serving stale set, %d topics): %v", g.Len(), err)
			}
		}
	}
}
