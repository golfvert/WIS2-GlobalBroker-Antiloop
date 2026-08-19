// Package dedup suppresses re-publishing a message this Global Broker
// deployment has already seen.
//
// The dedup keyspace is deliberately GLOBAL, not scoped per centre_id.
// The same message id can legitimately arrive more than once from more
// than one source that has nothing to do with this centre's own
// primary/secondary pair — per the WIS2 architecture, Global Brokers
// federate with each other, so the same notification can reach this
// GB's infrastructure via one centre's own relay AND via a different
// centre's relay that re-received it from another GB, or via another
// GB this one peers with. Scoping the dedup key by centre_id would only
// catch the primary/backup-broker duplicate case for a single centre
// and miss cross-centre / cross-GB duplicates entirely — defeating the
// point of anti-loop protection.
//
// The "id" field this package keys on is not specifically a
// CloudEvents field. WME (monitoring events) are CloudEvents-shaped,
// per wis2-event-message-bundled.json. WNM (the actual data
// notifications) are GeoJSON-based per the WIS2 Notification Message
// spec and are NOT CloudEvents — but both shapes carry a top-level
// "id", which is all this package needs, so the extraction in
// relay/pipeline.go works for both without caring which spec produced
// the message.
//
// This is a standard SET-NX-with-TTL implementation, not a verbatim
// port of any specific flow node — the original flow's "Anti-loop"
// comment node groups its logic differently, but the SET-NX-with-TTL
// semantics reproduced here are functionally equivalent.
//
// Separately, the flow has an "Expire" change node that does
// SET uuid_<this-instance's-uuid> true EX <jittered 12h-36h>. That's a
// different concern — a fleet-wide per-process liveness key, not
// message dedup — implemented in cmd/antiloop/main.go (as
// "wis2gb:uuid_<uuid>"), not in this package.
//
// Batching: cmd/redislat measures the raw network round trip to the
// shared Redis cluster — PING with no cluster routing at all, isolated
// from every line of antiloop's own code — at roughly 16-17ms average,
// p50 ~13ms, with real spikes past 100ms. That is the floor; no
// client-side change removes it. A naive fixed worker pool can work
// around it by keeping many SET NX calls in flight concurrently (one
// call per worker), but that doesn't scale well to a large fleet: many
// processes each running enough workers to hide 15ms of latency would
// open a very large number of simultaneous connections/requests against
// a cluster that already shows real queuing latency under load from a
// single prober. Node-RED, which antiloop's throughput is measured
// against, does the same synchronous wait-for-the-reply dedup check yet
// only ever holds one connection to the cluster — the only way to
// reconcile "one connection, synchronous per-message wait, high
// throughput" is pipelining: a single-threaded event-loop client can
// have many commands written to the socket before their replies come
// back, so N concurrent dedup checks cost roughly one round trip, not N.
//
// NewBatched reproduces that explicitly, since go-redis's normal
// Cmdable methods do NOT do this on their own: each SetNX call from a
// separate goroutine checks out its own pool connection and blocks that
// connection for a full synchronous round trip. Concurrent Seen() calls
// arriving within a short window (batchWait) are coalesced into a
// single redis.Pipeliner Exec — one network round trip services the
// whole batch, so per-process throughput no longer requires one live
// connection/goroutine per in-flight message. A large fleet, each
// running one batching loop, puts a small, roughly constant number of
// connections on the cluster regardless of message rate, instead of
// scaling linearly with however many workers each process needs to hide
// round-trip latency.
package dedup

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redis.Cmdable, not *redis.Client — REDIS_URL is a 6-node cluster
// (see internal/redisconn), so this needs to work with
// *redis.ClusterClient too. Cmdable is the interface both satisfy,
// and (needed for batching, below) both implement Pipeline() Pipeliner
// as part of that same interface.
// One Dedup instance backed by the one shared Redis cluster is enough
// for the whole fleet — every centre's process on every host can (and
// should) point at the same instance/cluster, since the whole point is
// a keyspace shared across all of them.
type Dedup struct {
	rdb redis.Cmdable
	ttl time.Duration

	// Batching state — nil/zero unless constructed via NewBatched. See
	// the package doc comment's BATCHING section for why this exists.
	reqCh     chan seenReq
	batchSize int
	batchWait time.Duration

	// flushSem bounds how many pipeline Execs can be in flight at once
	// — see NewBatched's doc comment and startFlush's doc comment for
	// why this exists (flush must not block the collector loop, or
	// batching only ever serializes what would otherwise be parallel
	// connections).
	flushSem chan struct{}

	// flushCount/flushedItems back a periodic average-batch-size log.
	// Real batch size under load is a variable a simple
	// wait-plus-round-trip cost model doesn't account for: a large
	// pipelined Exec still takes proportionally longer server-side even
	// though it's one network round trip, since Redis processes each
	// command in the pipeline sequentially. Tracking actual average
	// batch size makes that cost visible and explainable instead of
	// looking like unexplained overhead.
	flushCount   atomic.Int64
	flushedItems atomic.Int64
}

// loggingEnabled gates the periodic batch-stats log in flush() below.
// Off by default, since it fires every 50 flushes indefinitely and
// would otherwise flood the journal on a real deployment — same
// pattern as mqttbroker.EnableLogging, wired to the "dedup" -d category
// in cmd/antiloop.
var loggingEnabled atomic.Bool

// EnableLogging turns the periodic average-batch-size log in flush()
// on or off — call once at startup, e.g.
// dedup.EnableLogging(dbg.has("dedup")). Off by default: silent unless
// explicitly asked for.
func EnableLogging(enabled bool) {
	loggingEnabled.Store(enabled)
}

// seenReq/seenResult carry one Seen() call across to the batch loop
// goroutine and its answer back — result is buffered (cap 1) so the
// batch loop's send never blocks on a caller that's already given up
// via ctx.
type seenReq struct {
	id     string
	result chan seenResult
}

type seenResult struct {
	seen bool
	err  error
}

func New(rdb redis.Cmdable, ttl time.Duration) *Dedup {
	if ttl == 0 {
		ttl = 5 * time.Minute // generous vs. any plausible cross-GB re-delivery delay
	}
	return &Dedup{rdb: rdb, ttl: ttl}
}

// NewBatched is New plus pipelined batching of concurrent Seen() calls
// — see the package doc comment's BATCHING section for the full
// rationale (measured ~15-17ms Redis round-trip floor; matching
// Node-RED's one-connection throughput requires amortizing that round
// trip across many messages, not opening more connections).
//
// batchSize caps how many Seen() calls one Redis pipeline Exec
// services; batchWait is the longest a call will sit waiting for
// company before the batch (however small) is flushed anyway — this
// is the real latency cost of batching, so keep it well under the
// measured round-trip time it's meant to amortize (a few ms is plenty;
// waiting longer than the round trip you're saving is self-defeating).
// batchConcurrency caps how many pipeline Execs can be in flight at
// once — this must be greater than 1 (see batchLoop's doc comment on
// startFlush for why): calling flush() inline from the collector loop
// would block the loop for the whole round trip before it could resume
// collecting, meaning no batch could ever grow past whatever number of
// concurrent callers happened to be in flight, and every batch would
// pay the full batchWait as dead time stacked on top of a fully
// serialized round trip — worse than not batching at all. Running
// flush() as its own goroutine, bounded by batchConcurrency, lets
// multiple batches' round trips overlap while still coalescing whatever
// concurrent callers there are into fewer round trips.
// batchSize<1 becomes 1, batchWait<=0 becomes 1ms, batchConcurrency<1
// becomes 1 — all degenerate to "flush almost immediately, one at a
// time", not a hang.
func NewBatched(rdb redis.Cmdable, ttl time.Duration, batchSize int, batchWait time.Duration, batchConcurrency int) *Dedup {
	d := New(rdb, ttl)
	if batchSize < 1 {
		batchSize = 1
	}
	if batchWait <= 0 {
		batchWait = time.Millisecond
	}
	if batchConcurrency < 1 {
		batchConcurrency = 1
	}
	d.batchSize = batchSize
	d.batchWait = batchWait
	d.flushSem = make(chan struct{}, batchConcurrency)
	// Buffered generously (4x batchSize) so a burst of callers can hand
	// off their request and move on to waiting on their own result
	// channel without contending on reqCh itself becoming the new
	// bottleneck.
	d.reqCh = make(chan seenReq, batchSize*4)
	go d.batchLoop()
	return d
}

// Seen returns true if this message id was already processed recently
// — by this instance, its primary/secondary twin, another centre's
// relay process, or another Global Broker's feed into this same Redis
// cluster — and atomically marks it seen if not. SET NX EX is a single
// round trip (or, for a batched Dedup, one command inside a shared
// pipelined round trip — see NewBatched), so there's no TOCTOU window
// between the check and the mark.
//
// Deliberately global: no centre_id in the key. See package doc comment.
func (d *Dedup) Seen(ctx context.Context, messageID string) (bool, error) {
	if d.reqCh == nil {
		return d.seenDirect(ctx, messageID)
	}

	result := make(chan seenResult, 1)
	select {
	case d.reqCh <- seenReq{id: messageID, result: result}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case res := <-result:
		return res.seen, res.err
	case <-ctx.Done():
		// The batch loop may still complete this request and write to
		// result after we've given up on it — result is buffered (cap
		// 1) specifically so that send never blocks/leaks a goroutine.
		return false, ctx.Err()
	}
}

// seenDirect is the original unbatched path — one SET NX EX per call,
// its own round trip. Used directly by New()-constructed Dedups, and
// as NewBatched's actual Redis call inside each flushed batch (see
// flush) is done via the pipeliner instead, not this method.
func (d *Dedup) seenDirect(ctx context.Context, messageID string) (bool, error) {
	key := fmt.Sprintf("wis2gb:dedup:%s", messageID)
	ok, err := d.rdb.SetNX(ctx, key, 1, d.ttl).Result()
	if err != nil {
		return false, err
	}
	// SetNX returns true if the key was newly set (i.e. NOT seen before).
	return !ok, nil
}

// batchLoop is NewBatched's single collector goroutine — one per Dedup
// instance, one per process (see New()'s "one Dedup instance" doc
// comment: the whole fleet shares this pattern, one batching loop per
// antiloop process, not per worker). Coalesces whatever Seen() calls
// land within batchWait of each other (or the first batchSize of them,
// whichever comes first) into a single flush.
//
// startFlush launches flush() in its own goroutine rather than calling
// it inline from this loop. An inline call would block this loop for
// the whole pipeline Exec round trip before it could resume collecting
// — which, combined with a bounded number of concurrent callers never
// reaching batchSize, would mean every batch pays the full batchWait as
// dead time on top of a round trip that no longer overlaps with the
// next one, ending up strictly worse than each caller just doing its
// own unbatched round trip in parallel. See startFlush/flushSem below.
func (d *Dedup) batchLoop() {
	batch := make([]seenReq, 0, d.batchSize)
	var timerC <-chan time.Time

	for {
		select {
		case req := <-d.reqCh:
			batch = append(batch, req)
			if timerC == nil {
				// Starts the clock on THIS batch's max wait as soon as
				// its first member arrives — not a fixed ticker, so an
				// idle period between bursts costs nothing.
				timerC = time.After(d.batchWait)
			}
			if len(batch) >= d.batchSize {
				d.startFlush(batch)
				batch = make([]seenReq, 0, d.batchSize)
				timerC = nil
			}
		case <-timerC:
			if len(batch) > 0 {
				d.startFlush(batch)
				batch = make([]seenReq, 0, d.batchSize)
			}
			timerC = nil
		}
	}
}

// startFlush hands batch off to run concurrently, bounded by flushSem
// (batchConcurrency) so a sudden storm of tiny batches can't spawn
// unbounded goroutines/Redis round trips. Acquiring the semaphore slot
// happens in the spawned goroutine, not here, so a momentarily-full
// flushSem never blocks batchLoop itself from continuing to collect
// the next batch — that's the whole point of this being separate from
// the old inline d.flush(batch) call.
func (d *Dedup) startFlush(batch []seenReq) {
	go func() {
		d.flushSem <- struct{}{}
		defer func() { <-d.flushSem }()
		d.flush(batch)
	}()
}

// flush issues one pipelined round trip for the whole batch and
// delivers each request's own result back on its own channel. Uses
// context.Background() for the actual Redis call rather than any one
// caller's ctx — a shared pipeline call can't honor N different
// contexts, and a caller whose ctx is already cancelled has already
// stopped listening on result (see Seen's ctx.Done branch) so a wasted
// call costs nothing but a little Redis load, never an incorrect
// result delivered to anyone.
func (d *Dedup) flush(batch []seenReq) {
	ctx := context.Background()
	pipe := d.rdb.Pipeline()
	cmds := make([]*redis.BoolCmd, len(batch))
	for i, req := range batch {
		key := fmt.Sprintf("wis2gb:dedup:%s", req.id)
		cmds[i] = pipe.SetNX(ctx, key, 1, d.ttl)
	}
	// Exec's own returned error is an aggregate ("did anything in this
	// pipeline fail") and deliberately ignored here — each cmd's own
	// Result()/Err() below is authoritative for that specific command,
	// which is what each individual caller actually needs to know.
	_, _ = pipe.Exec(ctx)
	for i, req := range batch {
		ok, err := cmds[i].Result()
		if err != nil {
			req.result <- seenResult{false, err}
			continue
		}
		req.result <- seenResult{seen: !ok}
	}

	// Periodic average-batch-size log — see flushCount/flushedItems'
	// doc comment on the struct. Every 50 flushes so it's cheap and
	// doesn't flood the log, but frequent enough to be useful during a
	// single wnmtest run (6000 messages / DEDUP_BATCH_SIZE=200 max is
	// at least 30 flushes even in the best case, so 50 lands inside a
	// typical run rather than only firing once at the very end).
	// Gated by loggingEnabled (see its doc comment) — the counters
	// themselves are still tracked unconditionally (cheap, just two
	// atomic adds), only the log line is skipped when disabled.
	d.flushedItems.Add(int64(len(batch)))
	if n := d.flushCount.Add(1); n%50 == 0 && loggingEnabled.Load() {
		avg := float64(d.flushedItems.Load()) / float64(n)
		log.Printf("dedup: batch stats (cumulative avg, n=%d flushes): avg_batch_size=%.1f last_batch_size=%d", n, avg, len(batch))
	}
}
