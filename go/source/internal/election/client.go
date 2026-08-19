// Package election ports the original flow's "Elect" function node's
// lowest-uuid algorithm directly in-process, sharing the app's
// existing Redis connection — no external binary, no subprocess.
//
// Running this in-process rather than as a separate binary avoids
// opening a brand new Redis Cluster connection (including cluster
// topology discovery) on every single election tick — a standalone
// binary invoked per tick would pay that cost repeatedly instead of
// reusing the long-lived pooled connection the main process already
// holds for dedup. At 2s intervals across a large centre fleet with 2
// instances each, that adds up to a lot of needless cluster-topology
// round trips, plus process-spawn overhead and a separate deployable
// artifact to version. Nothing about this algorithm needs process
// isolation (a hung Redis call is already bounded by a context timeout
// either way) or its own versioning/deployment lifecycle. If ad-hoc
// CLI inspection of the current role is ever needed, the tick method
// below is a small wrapper away from a standalone tool — kept as its
// own package specifically so that stays cheap to add without
// duplicating the logic.
package election

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Algorithm (verbatim port of "Elect" + its neighboring HSET "Set" /
// HGETALL "Getall" / HDEL "Del" redis-command nodes). The key is
// "wis2gb:election:<centre_id>" rather than the original flow's bare
// <centre_id> — every key this codebase writes uses the wis2gb:
// prefix, for a consistent, easily-greppable namespace in Redis:
//
//  1. HSET wis2gb:election:<centre_id> <uuid> <now_ms>|<host>  (announce liveness)
//  2. HGETALL wis2gb:election:<centre_id>                       -> flat [uuid,val,...]
//  3. walk pairs: track the lexicographically smallest uuid whose
//     timestamp is fresher than the fresh window (8000ms in the
//     original); collect stale uuids separately
//  4. HDEL wis2gb:election:<centre_id> <stale_uuid...>   (opportunistic cleanup)
//  5. isPrimary = (smallest fresh uuid == my uuid)
//
// No Redis TTL/EXPIRE — staleness is computed client-side on every
// tick, stale entries pruned lazily by whichever instance runs the
// check next. That's load-bearing production behavior from the
// source flow, not an implementation detail to "simplify" with a
// TTL-key scheme.
//
// The announced value is "<ms>|<host>", not a bare millisecond
// timestamp — the election algorithm itself still only ever looks at
// the millisecond part (host is informational, never affects who
// wins), but attaching the host lets anything outside this package
// (see cmd/wis2nodes) answer "which host is the current primary", not
// just "how many instances are alive" — a uuid alone isn't an address.
// Parsing splits on the first "|" via strings.Cut, which degrades
// safely for an old-format bare-integer value (Cut just returns the
// whole string as the before-part, found=false) — this fleet
// deliberately runs multiple antiloop versions side by side (see the
// Ansible repo's binary-versioning scheme), so a node not yet
// redeployed past this change must keep working exactly as before, not
// error out just because its own entries lack a host.

// keyExpiry is the whole-key TTL applied on every successful announce
// — a garbage-collection safety net for fully-decommissioned centres,
// not a change to how staleness/primary is actually decided.
// Comfortably above ELECTION_FRESH_WINDOW (8s default) and
// ELECTION_INTERVAL (2s default) by a wide margin on purpose: this
// must never be the thing that expires a field a live instance still
// cares about — that's still ELECTION_FRESH_WINDOW's job, computed
// client-side. This only ever fires on a key nothing has touched in a
// full hour.
const keyExpiry = time.Hour

type Client struct {
	rdb redis.Cmdable
	// redisKey is the HSET/HGETALL/HDEL key this liveness hash lives
	// under — "wis2gb:election:<centre_id>".
	redisKey string
	centreID string
	uuid     string
	// host is this instance's own Host value (see config.Config's Host
	// field) — embedded in every announce so external tooling (see
	// cmd/wis2nodes) can resolve uuid -> host without a second data
	// source. Purely informational from this package's own point of
	// view; never read back or compared against anything here.
	host     string
	interval time.Duration
	fresh    time.Duration

	isPrimary atomic.Bool

	// announced tracks whether onChange has fired at least once yet.
	// Only ever touched inside tick(), which Run() calls serially from
	// a single goroutine (never concurrently with itself), so this is
	// a plain bool, not another atomic — unlike isPrimary, which
	// IsPrimary() reads from other goroutines.
	announced bool
}

// rdb is the app's existing Redis client (shared with dedup) — not a
// fresh connection per call. host is embedded in every announce — see
// the Client.host field's doc comment.
func NewClient(rdb redis.Cmdable, centreID, host string, interval, freshWindow time.Duration) *Client {
	return &Client{
		rdb:      rdb,
		redisKey: "wis2gb:election:" + centreID,
		centreID: centreID,
		uuid:     uuid.NewString(),
		host:     host,
		interval: interval,
		fresh:    freshWindow,
	}
}

// UUID is this instance's identity for the lifetime of the process —
// exposed so callers can log/report it (e.g. in the monitor heartbeat).
func (c *Client) UUID() string { return c.uuid }

// IsPrimary returns the last-known role. Defaults to false (secondary)
// until the first successful election tick — deliberately fails closed:
// an instance that can't reach Redis should not assume it's primary and
// start publishing.
func (c *Client) IsPrimary() bool { return c.isPrimary.Load() }

// Run polls at the configured interval until ctx is canceled, calling
// onChange whenever the role flips.
func (c *Client) Run(ctx context.Context, onChange func(isPrimary bool)) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Tick once immediately, matching the flow's "2s" inject having
	// once:true (fires right away, then every interval).
	c.tick(ctx, onChange)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx, onChange)
		}
	}
}

func (c *Client) tick(ctx context.Context, onChange func(bool)) {
	cctx, cancel := context.WithTimeout(ctx, c.interval)
	defer cancel()

	isPrimary, err := c.evaluate(cctx)
	if err != nil {
		log.Printf("[%s] election tick failed: %v", c.centreID, err)
		// Fail closed: leave role as-is (don't flip to primary on error).
		// If Redis is down long enough that BOTH instances lose contact,
		// both stay/become secondary — no publish happens, which is the
		// safe failure mode for a relay vs. risking a split-brain publish.
		// Deliberately does NOT set c.announced — a failed first tick
		// shouldn't count as having announced anything; the eventual
		// first successful tick still fires onChange, startup role
		// still gets logged once Redis is reachable.
		return
	}

	changed := isPrimary != c.isPrimary.Swap(isPrimary)
	// onChange fires unconditionally on the first tick, not just on a
	// genuine role transition thereafter. isPrimary's zero value is
	// false, so an instance that starts as secondary (the common case —
	// it lost the election to an already-fresher instance) would
	// otherwise never trigger onChange on its first tick at all
	// (false != Swap(false) is false) — its startup role would go
	// unlogged unless and until it later flipped to primary.
	if (changed || !c.announced) && onChange != nil {
		onChange(isPrimary)
	}
	c.announced = true
}

func (c *Client) evaluate(ctx context.Context) (bool, error) {
	nowMs := time.Now().UnixMilli()

	// 1. Announce liveness. Value is "<ms>|<host>" — see Client.host's
	// doc comment for why, and the package doc comment above for the
	// backward-compat parsing this pairs with below.
	//
	// Also refreshes a whole-key EXPIRE on every successful announce.
	// This is a separate mechanism from the per-field staleness scheme
	// described above, not a replacement for it — that algorithm
	// (fresh/stale by comparing each field's own timestamp against
	// ELECTION_FRESH_WINDOW, HDEL'd opportunistically by whoever ticks
	// next) is untouched and still what decides who's primary. The gap
	// this EXPIRE closes is different: per-field pruning only runs as a
	// side effect of some live instance still ticking — if every
	// instance for a centre stops (decommissioned, or just gone),
	// nothing is left alive to ever prune that last stale field, so the
	// key would sit in Redis forever. keyExpiry means the whole key
	// quietly disappears keyExpiry after the last instance's last
	// successful tick instead — while at least one instance is alive
	// and ticking (every ELECTION_INTERVAL, 2s default), this EXPIRE
	// keeps getting pushed back out, so it never fires on a genuinely
	// live centre. Pipelined with the HSET above so this doesn't cost a
	// second Redis round trip per tick.
	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, c.redisKey, c.uuid, fmt.Sprintf("%d|%s", nowMs, c.host))
	pipe.Expire(ctx, c.redisKey, keyExpiry)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("hset+expire: %w", err)
	}

	// 2. Read all members.
	all, err := c.rdb.HGetAll(ctx, c.redisKey).Result()
	if err != nil {
		return false, fmt.Errorf("hgetall: %w", err)
	}

	var fresh, stale []string
	cutoff := time.Now().Add(-c.fresh).UnixMilli()
	for id, raw := range all {
		// strings.Cut on a value with no "|" (an old-format bare
		// timestamp, from an instance not yet redeployed past
		// 2026-08-11) returns the whole string as tsStr with
		// found=false — parses exactly the same as before this change.
		tsStr, _, _ := strings.Cut(raw, "|")
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			stale = append(stale, id) // unparseable entry — treat like stale, prune it
			continue
		}
		if ts >= cutoff {
			fresh = append(fresh, id)
		} else {
			stale = append(stale, id)
		}
	}

	// 3. Opportunistic cleanup.
	if len(stale) > 0 {
		if err := c.rdb.HDel(ctx, c.redisKey, stale...).Err(); err != nil {
			// Non-fatal: another instance may win the race to delete the
			// same fields, or the connection blips. Correctness of this
			// election doesn't depend on the delete succeeding.
			log.Printf("[%s] election hdel cleanup failed (non-fatal): %v", c.centreID, err)
		}
	}

	if len(fresh) == 0 {
		// Shouldn't happen since we just HSET our own entry, but guard
		// against clock skew / races rather than panic.
		return false, fmt.Errorf("no fresh members found, including self")
	}

	sort.Strings(fresh)
	return fresh[0] == c.uuid, nil
}
