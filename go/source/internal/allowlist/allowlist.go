// Set fetches TOPIC_URL — a plain text file, one raw MD5 hash per line
// (fetched and confirmed: ~2500 lines, nothing but hex hashes, no CSV,
// no labels). Used by topics.Check() for its topic-hash membership
// test — see that package's doc comment.
//
// GDC_URL is a different shape entirely — CSV (metadata_id,topic
// pairs), not a flat list — so it gets its own type, GDCRegistry, in
// gdc.go rather than reusing this one. gdc.go's package doc comment has
// the fuller trace of why both types are Redis-backed, not just
// in-memory: the same reasoning applies here.
//
// Redis-backed, matching the original flow: its "Save" node
// (flows.json, "Refresh allowed topics" group) writes, for every hash
// line, "topic_<hash>" `SET ... EX 172800` (48h); its own membership
// check ("Result" switch's string-topic branch -> redis GET -> "Valid
// ?") is a plain GET against that same key. Refresh()/Has() below
// reproduce the key format and TTL exactly.
//
// Refresh TRIGGER for TOPIC_URL — reproduced exactly, not simplified
// (fixed 2026-08-20; see CLAUDE.md's divergence list for the earlier,
// wrong design this replaced). The flow's own mechanism is a
// per-process one, NOT a shared fleet-wide gate: each Node-RED instance
// owns its own dedicated Redis key with a randomly-jittered ~12-36h
// TTL and, once an hour, checks that key's OWN remaining TTL,
// re-fetching TOPIC_URL (and resetting the TTL) only once it's within
// 5400s (90min) of expiring — "TTL" inject -> redis TTL command ->
// "Soon ?" switch (rule: <=5400) -> "URL"/"WTH" chain. Fully
// decentralized: every instance decides for itself, on its own clock;
// there is no single fleet-wide "winner". topicRefreshDue/
// armTopicRefreshSentinel below reproduce that check-then-maybe-refresh
// shape exactly; s.topicSentinelKey/RunRefreshLoop's hourly ticker (see
// main.go) reproduce the per-process key and the hourly check cadence.
//
// One deliberate divergence remains, flagged in CLAUDE.md: this
// process's sentinel key is its OWN dedicated key
// ("wis2gb:topic_ttl_<uuid>"), not a reuse of main.go's
// "wis2gb:uuid_<uuid>" fleet-liveness key, even though the flow
// conceptually uses the very same per-process uuid+TTL key for both
// concerns (its "Expire" change node fires right after every TOPIC_URL
// fetch, whether from this hourly path or the one-time startup link).
// Re-coupling to the liveness key would (a) tie an external
// liveness/inventory consumer's freshness signal to a rare
// (~once-per-12-36h) event instead of its own independent cadence, and
// (b) require restructuring main.go's construction order (elector, which
// owns that uuid, is built after this Set today). A dedicated key
// reproduces the CHECK MECHANISM the user asked for exactly — hourly,
// per-process, TTL-gated, no shared coordination — without those side
// effects. Also worth noting: the exported flow's TTL-check step reads
// key "uuid_<uuid>" (no "wis2gb:" prefix) while its Expire step WRITES
// "wis2gb:uuid_<uuid>" (prefixed) — those are different Redis keys, so
// the live flow's own TTL check may in practice always see "key doesn't
// exist" and refresh every hour regardless of the jittered ~24h design
// intent. Go's dedicated sentinel key is read and written under the
// SAME name here, so it does not carry that same apparent bug forward —
// Go implements the flow's evident INTENT (jittered ~12-36h, refreshed
// proactively before expiry), not its possible implementation slip.
//
// The part that actually matters for correctness — Redis holding the
// shared, interoperable "topic_<hash>" registry, and Has() checking it
// — is reproduced exactly regardless of exactly how often any given
// process refreshes it.
//
// Go-only addition, flagged (see CLAUDE.md's divergence list): the
// shared "metadata_id_refresh"/per-process-timer gates mean, at real
// fleet size, only ONE process at a time ever actually performs the
// HTTP fetch and populates its OWN in-memory fast-path cache from it —
// every other process's Has() would fall through to a Redis round trip
// on nearly every check, indefinitely, not just occasionally. Rather
// than fix that by having every process periodically SCAN the shared
// Redis cluster (expensive: SCAN's cost is proportional to the WHOLE
// keyspace scanned, not just matches — and this cluster's dedup
// keyspace, at message-rate scale with a 2h TTL, dwarfs the small
// metadata_id:*/topic_* registries), whichever process DOES perform
// the real fetch also publishes a small, cheaply-fully-readable index:
// one Redis SET per centre_id for GDC (wis2gb:gdc_index:{<centre_id>},
// written for EVERY centre found in the file, not just this process's
// own — see gdc.go's Refresh), and one global SET for TOPIC_URL
// (wis2gb:{topic_index}). Every process — including ones that never
// win the fetch gate — reads its own relevant index with a single
// SMEMBERS call on its own RunIndexSyncLoop timer (indexSyncInterval,
// capped at 10 minutes) and rebuilds its local cache from that,
// entirely decoupled from the HTTP-fetch gate. This is O(this
// process's own registered entry count), not O(cluster keyspace).
//
// These index SETs have no Node-RED equivalent at all — Node-RED has
// no in-memory fast-path cache of any kind, so it never needed one.
// They are purely an additional, Go-only optimization layered on top
// of the exact-parity metadata_id:*/topic_* keys (which remain the
// only thing either side's actual correctness depends on — see Has()
// on both types, which always falls through to a direct per-key Redis
// EXISTS on an index/cache miss, never trusts the index as the sole
// answer).
package allowlist

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"antiloop/internal/fetchcache"
)

// redisCheckTimeout bounds each on-demand Redis fallback call in
// Has() (both Set's and GDCRegistry's) — long enough to absorb a slow
// cluster hop, short enough that a genuinely hung Redis can't stall a
// message-processing goroutine (see relay.Pipeline's per-message
// goroutine-per-call design) indefinitely.
const redisCheckTimeout = 500 * time.Millisecond

// redisWriteBatchSize caps how many SETs/SADDs go into one Redis
// pipeline Exec during a registry or index refresh — GDC_URL/TOPIC_URL
// can run to several thousand lines (2 keys per line for GDC) fleet-
// wide, and one unbounded pipeline would turn a single Exec into a
// multi-second blocking call. Same chunking rationale as dedup's own
// batching (see that package's doc comment), just applied once per
// refresh instead of continuously.
const redisWriteBatchSize = 500

// indexSyncInterval is how often every process (writer or not) rebuilds
// its in-memory fast-path cache from the shared index SET(s) — see
// package doc comment's "Go-only addition" section. Deliberately
// independent of, and shorter than, cfg.AllowlistRefresh (15m default):
// the index reflects whatever the fleet's last real HTTP-fetch cycle
// published, so reading it more often than that doesn't return
// materially fresher data, but 10 minutes is the agreed upper bound on
// how stale any one process's local cache is allowed to sit relative
// to what the fleet already knows.
const indexSyncInterval = 10 * time.Minute

// redisEntry is one row to write as a Redis key during a registry
// refresh — shared shape between GDCRegistry (key = urn or centre_id,
// topic = the raw data-topic path) and Set (key = md5 hash, topic
// unused, left "").
type redisEntry struct {
	key   string
	topic string // "" for Set's flat hash list; GDCRegistry always sets this
}

// refreshedRecently and armRefreshGate implement one shared,
// fleet-wide "don't re-fetch more often than `cooldown`" gate. Used
// only by GDCRegistry, against the flow's own "metadata_id_refresh" key
// (see gdc.go, exact port of the flow's "Soon ?" switch for GDC_URL) —
// appropriate there because GDC_URL is genuinely one shared, fleet-wide
// file with one clean shared-gate mechanism in the original flow. Set
// (TOPIC_URL) does NOT use this: TOPIC_URL's own refresh trigger in the
// flow is a per-process TTL-gated mechanism, not a shared cooldown gate
// — see topicRefreshDue/armTopicRefreshSentinel and Set.Refresh's own
// doc comment below.
func refreshedRecently(ctx context.Context, rdb redis.Cmdable, key string, cooldown time.Duration) (bool, error) {
	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lastMs, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return false, err
	}
	return time.Since(time.UnixMilli(lastMs)) < cooldown, nil
}

// armRefreshGate stamps key with the current time (millis, plain
// decimal — same shape the flow's own jsonata $toMillis($now()) writes
// for metadata_id_refresh, so a Go-armed gate and a Node-RED-armed one
// are mutually readable) with no expiry: refreshedRecently only ever
// compares age, never presence, so there's nothing to let lapse.
func armRefreshGate(ctx context.Context, rdb redis.Cmdable, key string) error {
	return rdb.Set(ctx, key, time.Now().UnixMilli(), 0).Err()
}

// writeRegistry pipelines SET <prefix><key>[:<topic>] true EX <ttl>
// for every entry, batched by redisWriteBatchSize. Shared by
// GDCRegistry.Refresh (prefix "metadata_id:", topic included) and
// Set.Refresh (prefix "topic_", topic left empty so the key is just
// prefix+key).
func writeRegistry(ctx context.Context, rdb redis.Cmdable, prefix string, ttl time.Duration, entries []redisEntry) error {
	for i := 0; i < len(entries); i += redisWriteBatchSize {
		end := i + redisWriteBatchSize
		if end > len(entries) {
			end = len(entries)
		}
		pipe := rdb.Pipeline()
		for _, e := range entries[i:end] {
			key := prefix + e.key
			if e.topic != "" {
				key += ":" + e.topic
			}
			pipe.Set(ctx, key, "true", ttl)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// writeIndex atomically replaces a fast-path index SET's contents:
// build under a temp key (chunked SADD, same redisWriteBatchSize
// rationale as writeRegistry), give it ttl, then RENAME over the real
// key in the same pipeline — so a concurrent reader's SMEMBERS never
// observes a half-written or momentarily-empty set. key MUST already
// be hash-tagged (e.g. "wis2gb:gdc_index:{<centre_id>}" or
// "wis2gb:{topic_index}") so the temp key (key+":tmp") lands on the
// SAME cluster slot — RENAME requires both keys in one slot, and
// without a shared {tag} a plain suffix would hash differently under
// REDIS_CLUSTER=true.
//
// Called only by whichever process wins a given registry's HTTP-fetch
// gate (see gdc.go/allowlist.go's package doc comments) — every other
// process only ever reads these via syncFromIndex, never writes them.
func writeIndex(ctx context.Context, rdb redis.Cmdable, key string, members []string, ttl time.Duration) error {
	tmpKey := key + ":tmp"
	pipe := rdb.Pipeline()
	pipe.Del(ctx, tmpKey)
	for i := 0; i < len(members); i += redisWriteBatchSize {
		end := i + redisWriteBatchSize
		if end > len(members) {
			end = len(members)
		}
		args := make([]interface{}, end-i)
		for j, m := range members[i:end] {
			args[j] = m
		}
		pipe.SAdd(ctx, tmpKey, args...)
	}
	pipe.Expire(ctx, tmpKey, ttl)
	pipe.Rename(ctx, tmpKey, key)
	_, err := pipe.Exec(ctx)
	return err
}

// topicHashTTL is the flow's own EX 172800 (48h) on every "topic_*"
// key its "Save" node writes — reused verbatim, see package doc
// comment.
const topicHashTTL = 48 * time.Hour

// topicIndexKey is the single, global fast-path index SET for
// TOPIC_URL — see package doc comment's "Go-only addition" section.
// Hash-tagged ({topic_index}) so writeIndex's RENAME (real key <-
// temp key) always lands both keys on the same Cluster slot.
const topicIndexKey = "wis2gb:{topic_index}"

// TopicRefreshCheckInterval is how often each process checks whether
// ITS OWN topic-hash refresh sentinel is due — reproducing the flow's
// "TTL" inject node's "repeat": "3600" (1 hour) exactly. Exported so
// main.go can drive Set.RunRefreshLoop's ticker at this cadence instead
// of the generic cfg.AllowlistRefresh interval GDCRegistry still uses
// (GDC_URL's shared-gate model is unaffected by this change — see
// gdc.go).
const TopicRefreshCheckInterval = time.Hour

// topicRefreshTTLThreshold reproduces the flow's "Soon ?" switch rule
// (property "payload" — the TTL command's result — rule "lte 5400"):
// a process only re-fetches TOPIC_URL once its own sentinel key has
// 5400 seconds (90min) or less left before expiring.
const topicRefreshTTLThreshold = 5400 * time.Second

// topicRefreshSentinelTTLMin/topicRefreshSentinelJitter reproduce the
// flow's own "Expire" change node formula —
// floor((random()+0.5)*86400) — a value uniformly distributed over
// [43200, 129600) seconds, i.e. 12h-36h. Same formula main.go's
// unrelated "Fleet-wide per-process liveness key" setLiveness()
// independently reproduces for its own, different key — both are
// correct, independent ports of the identical flow formula, not shared
// code (see package doc comment for why these two keys are kept
// separate).
const (
	topicRefreshSentinelTTLMin    = 43200 * time.Second
	topicRefreshSentinelTTLJitter = 86400 // seconds; rand.Intn range
)

type Set struct {
	url        string
	httpClient *http.Client
	rdb        redis.Cmdable
	// ctx — see GDCRegistry's identical field for why this is stored
	// rather than threaded through Has()'s call site (topics.Check()'s
	// HashSet interface has no ctx parameter, deliberately, to avoid
	// rippling a context.Context through every Check() caller for a
	// dependency that's really only needed on the rare local-cache-miss
	// path).
	ctx context.Context

	// topicSentinelKey — this process's own dedicated Redis key backing
	// the per-instance TTL-gated refresh check (topicRefreshDue /
	// armTopicRefreshSentinel below) — see package doc comment for why
	// this is a key of its own rather than a reuse of main.go's
	// "wis2gb:uuid_<uuid>" liveness key. Generated once per process at
	// construction time and stable for the process's lifetime.
	topicSentinelKey string

	mu      sync.RWMutex
	entries map[string]struct{}
}

// rdb backs both the Redis fallback for when url can't be reached (see
// internal/fetchcache's doc comment) and the shared "topic_<hash>"
// registry itself (see package doc comment) — nil disables both,
// degrading to the in-memory-only behavior this package had before the
// Redis-backed rewrite.
func New(ctx context.Context, url string, rdb redis.Cmdable) *Set {
	return &Set{
		url:              url,
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		rdb:              rdb,
		ctx:              ctx,
		entries:          map[string]struct{}{},
		topicSentinelKey: "wis2gb:topic_ttl_" + uuid.NewString(),
	}
}

// topicRefreshDue reproduces the flow's "Soon ?" switch (property
// "payload" — the TTL command's result — rule "lte 5400") against THIS
// process's own sentinel key. A missing key (Redis TTL command returns
// -2) or, in principle, a no-expiry key (-1 — shouldn't happen here,
// since armTopicRefreshSentinel always sets one) both count as "due":
// there's no remaining time to wait on, so refresh now — this is also
// what makes the very first Refresh() call of a process's lifetime
// (before its sentinel key has ever been set) behave like the flow's
// own one-time startup link into "URL"/"WTH", which bypasses the TTL
// check entirely.
func topicRefreshDue(ctx context.Context, rdb redis.Cmdable, key string) (bool, error) {
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if ttl < 0 {
		return true, nil
	}
	return ttl <= topicRefreshTTLThreshold, nil
}

// armTopicRefreshSentinel resets this process's own sentinel key to a
// fresh, randomly-jittered TTL (12h-36h) — see the const block above
// for the formula this reproduces.
func armTopicRefreshSentinel(ctx context.Context, rdb redis.Cmdable, key string) error {
	ttl := topicRefreshSentinelTTLMin + time.Duration(rand.Intn(topicRefreshSentinelTTLJitter))*time.Second
	return rdb.Set(ctx, key, true, ttl).Err()
}

// Refresh re-fetches the text file, swaps in a new in-memory set
// atomically, and (on a genuine fresh fetch, not a stale Redis
// fallback body — see package doc comment) writes every hash to Redis
// as "topic_<hash>" EX 172800, matching the flow's own "Save" node. A
// failed refresh (and no usable Redis fallback either) keeps serving
// the previous (stale) in-memory set rather than clearing it — a fetch
// hiccup shouldn't suddenly reject every message as "not in
// allowlist", and Has() below falls through to Redis regardless.
//
// Gated by topicRefreshDue against this process's OWN sentinel key —
// see package doc comment and topicRefreshDue's own doc comment. This
// is a per-process, fully decentralized check, not a shared/fleet-wide
// gate: every process decides for itself, on its own clock, whether
// it's time to re-fetch. Call this on an hourly ticker (see main.go,
// TopicRefreshCheckInterval) to match the flow's own "TTL" inject
// cadence — the TTL check itself is cheap (one Redis TTL command), so
// most hourly ticks are expected to be a no-op skip, with the real
// fetch+write happening roughly once every 12-36h per process.
func (s *Set) Refresh(ctx context.Context) error {
	if s.url == "" {
		return nil // not configured for this deployment; Has() just always returns false
	}

	if s.rdb != nil {
		due, err := topicRefreshDue(ctx, s.rdb, s.topicSentinelKey)
		if err != nil {
			log.Printf("allowlist: topic-hash refresh-due check failed (proceeding with fetch anyway): %v", err)
		} else if !due {
			return nil // this process's own sentinel isn't close enough to expiring yet — see doc comment above
		}
	}

	body, usedFallback, err := fetchcache.Fetch(ctx, s.httpClient, s.rdb, s.url, "wis2gb:cache:topic-hierarchy")
	if err != nil {
		return err
	}
	if usedFallback {
		log.Printf("allowlist: %s unreachable, using last-known-good copy cached in Redis", s.url)
	}

	next := make(map[string]struct{})
	var registryWrites []redisEntry
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		next[line] = struct{}{}
		registryWrites = append(registryWrites, redisEntry{key: line})
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		return err
	}

	s.mu.Lock()
	s.entries = next
	s.mu.Unlock()

	// Only write back / re-arm this process's own sentinel on a genuine
	// fresh fetch, not a stale Redis fallback body — same reasoning as
	// GDCRegistry.Refresh: re-deriving from a fallback would just
	// rewrite Redis with what it already has, and would incorrectly
	// reset this process's own TTL clock as if it had actually reached
	// the real URL.
	if !usedFallback && s.rdb != nil {
		if err := writeRegistry(ctx, s.rdb, "topic_", topicHashTTL, registryWrites); err != nil {
			log.Printf("allowlist: writing shared topic-hash registry to redis failed (this process's own in-memory cache is still up to date, %d entries): %v", len(next), err)
		} else if err := armTopicRefreshSentinel(ctx, s.rdb, s.topicSentinelKey); err != nil {
			log.Printf("allowlist: resetting topic-hash refresh sentinel failed (non-fatal — next hourly check may re-fetch sooner than necessary): %v", err)
		}
		// Fast-path index for every OTHER process — see package doc
		// comment's "Go-only addition" section. Best-effort: a failure
		// here doesn't affect correctness, only how many processes get
		// a warm local cache before their own next syncFromIndex tick.
		hashes := make([]string, 0, len(next))
		for h := range next {
			hashes = append(hashes, h)
		}
		if err := writeIndex(ctx, s.rdb, topicIndexKey, hashes, topicHashTTL); err != nil {
			log.Printf("allowlist: writing topic-hash fast-path index failed (non-fatal — other processes fall back to per-hash redis checks): %v", err)
		}
	}
	return nil
}

// syncFromIndex refreshes the in-memory cache from the shared
// topicIndexKey SET — see package doc comment's "Go-only addition"
// section. Runs on its own indexSyncInterval timer (RunIndexSyncLoop),
// entirely decoupled from Refresh()'s own HTTP-fetch cadence, so a
// process that rarely (or never) is the one to actually re-fetch
// TOPIC_URL itself still gets a warm cache from whichever process did.
// An empty read (nobody has published yet, e.g. a brand new fleet)
// deliberately leaves the existing cache alone rather than clearing it.
func (s *Set) syncFromIndex(ctx context.Context) {
	if s.rdb == nil {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, redisCheckTimeout)
	defer cancel()
	members, err := s.rdb.SMembers(rctx, topicIndexKey).Result()
	if err != nil {
		log.Printf("allowlist: topic-hash index sync failed (serving previous cache, %d entries): %v", s.Len(), err)
		return
	}
	if len(members) == 0 {
		return
	}
	next := make(map[string]struct{}, len(members))
	for _, m := range members {
		next[m] = struct{}{}
	}
	s.mu.Lock()
	s.entries = next
	s.mu.Unlock()
}

// RunIndexSyncLoop refreshes this process's in-memory cache from the
// shared index SET immediately, then every indexSyncInterval until ctx
// is canceled. Safe/expected to run on every process regardless of
// whether it ever wins Refresh()'s own HTTP-fetch cadence — see
// syncFromIndex and the package doc comment.
func (s *Set) RunIndexSyncLoop(ctx context.Context) {
	s.syncFromIndex(ctx)
	ticker := time.NewTicker(indexSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.syncFromIndex(ctx)
		}
	}
}

// Has is nil-safe on purpose: topics.Check() takes this through a
// HashSet interface, and an interface holding a typed nil *Set is NOT
// itself == nil (classic Go gotcha) — so a nil check on the interface
// value in Check() wouldn't catch a nil *Set passed in by a caller
// that forgot to construct one. Guarding here instead means "not
// configured" fails safe (Has -> false, same as an empty set) no
// matter which side the nil comes from.
//
// Checks the in-memory cache first (fast path, no Redis round trip on
// a hit). A miss falls through to a Redis EXISTS on "topic_<key>" —
// the same key the flow's own GET would check — so a hash this
// process's own TOPIC_URL fetch hasn't seen yet (stale local cache, or
// this process just started) still gets the correct answer as long as
// the shared registry has it. See package doc comment for the full
// resilience rationale, same as GDCRegistry.Has.
func (s *Set) Has(key string) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	_, ok := s.entries[key]
	s.mu.RUnlock()
	if ok {
		return true
	}

	if s.rdb == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, redisCheckTimeout)
	defer cancel()
	n, err := s.rdb.Exists(ctx, "topic_"+key).Result()
	if err != nil {
		log.Printf("allowlist: redis check failed for topic hash %q: %v", key, err)
		return false
	}
	return n > 0
}

func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// RunRefreshLoop refreshes once immediately, then on `interval` until
// ctx is canceled. Failures are logged, not fatal — see Refresh's
// stale-on-error behavior.
func (s *Set) RunRefreshLoop(ctx context.Context, interval time.Duration, name string) {
	if err := s.Refresh(ctx); err != nil {
		log.Printf("allowlist %q: initial refresh failed: %v", name, err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Refresh(ctx); err != nil {
				log.Printf("allowlist %q: refresh failed (serving stale set, %d entries): %v", name, s.Len(), err)
			}
		}
	}
}
