// Package gate ports the node-red-contrib-queue-gate behavior that the
// original flow wires directly to the election result: a "Primary ?"
// switch's Open branch fans out to every q-gate; becoming secondary
// fans a Reset, then a Queue+Flush pair, out to the same gates.
//
// Why this matters: dedup's Redis SET NX EX consumes a message id's
// dedup slot the instant it runs, permanently — there's no "undo".
// Running dedup unconditionally for both primary and secondary, and
// only gating the final Publish call on IsPrimary(), would mean a
// message arriving while an instance is secondary gets its dedup slot
// consumed and is then silently discarded (never published by that
// instance) — and if this instance is later promoted to primary, it
// can never publish that message, because the dedup check would now
// see it as "already seen" even though nobody actually published it.
// That's permanent, silent message loss, concentrated exactly in the
// highest-risk window: a failover.
//
// The fix implemented here: buffer raw messages in a Gate while
// secondary, and only run them through Check/Validate/Dedup/Publish
// once actually primary. Promotion drains the whole backlog, in order,
// before resuming live pass-through — so dedup is only ever consumed by
// whichever instance actually processes a message for real.
//
// The original flow's secondary-side handoff is a one-shot ~500ms
// sequence (Reset, then after a fixed 250ms delay Queue+Flush, then
// after another fixed 250ms delay Queue again) run exactly once per
// primary->secondary transition — not a recurring drain while staying
// secondary. That sequence exists because the original flow chains two
// separate q-gate node instances, and node-red-contrib-queue-gate's own
// async internals need a buffer window to hand off between them; it's
// not a semantic requirement to flush content while secondary, just
// Node-RED-node-specific plumbing. A single mutex-protected Go Gate
// (this package) reaches the identical end state atomically, with no
// async handoff to bridge and therefore no delays needed — this
// implementation only ever drains on an actual primary transition,
// which is a complete, correct port of the underlying behavior.
//
// maxAge closes a gap that maxLen alone leaves open: a secondary that
// never gets promoted just accumulates backlog up to maxLen and holds
// it indefinitely — there's no age-based eviction, only a count-based
// one. If that instance is eventually promoted after sitting secondary
// for longer than the dedup TTL, draining the backlog publishes
// messages that are both stale AND, worse, no longer caught as
// duplicates by dedup: whichever instance actually published them
// while this one was secondary set a dedup key that has since expired,
// so the drained copy sails through dedup as if new. maxAge must match
// the dedup TTL exactly for this reason — it's not an independent
// tuning knob, it's the same number the dedup layer already commits to.
// Eviction happens from the head of the queue (append-only, so the
// oldest entries are always contiguous at the front) both opportunistically
// on every Handle() — so stale entries don't sit in the bounded queue
// occupying room that keepNewest:false would otherwise deny to a fresh
// arrival — and again on Open(), to cover a queue that received no
// traffic (and therefore no opportunistic eviction) for a while before
// promotion finally happened.
package gate

import (
	"sync"
	"time"
)

type State int

const (
	Queueing State = iota
	Open
)

type entry[T any] struct {
	msg T
	at  time.Time
}

type Gate[T any] struct {
	mu      sync.Mutex
	state   State
	queue   []entry[T]
	maxLen  int
	maxAge  time.Duration
	forward func(T)
}

// New creates a gate defaulting to Queueing (matches every relevant
// q-gate's defaultState:"queueing" in flows.json — an instance that
// hasn't heard from the election binary yet buffers rather than
// assumes it's live, same fail-closed posture as election.Client).
// forward is called for each message that passes through, either
// immediately (state==Open) or later when Open()/Flush() drains the
// backlog. maxLen matches the largest observed maxQueueLength (50000)
// — keepNewest:false in the source, i.e. once full, drop the newest
// arrival and keep the queued backlog, not the other way around.
// maxAge, if non-zero, evicts (without ever forwarding) any queued
// entry older than maxAge — see the package doc comment for why this
// must match the dedup TTL. Pass 0 to disable age-based eviction
// entirely (e.g. for a gate buffering already-deduped work, where a
// stale entry is a delivery-delay concern rather than a duplicate-risk
// one).
func New[T any](maxLen int, maxAge time.Duration, forward func(T)) *Gate[T] {
	return &Gate[T]{maxLen: maxLen, maxAge: maxAge, forward: forward}
}

// Handle is the ingestion entry point — call it for every incoming
// message instead of processing directly.
//
// forward is called after g.mu is released, not while still holding
// it — forward can be a channel send that legitimately blocks under
// backpressure (see relay.Pipeline's dispatch), so calling it under the
// lock would block every other caller of Open()/Queue()/Len() (role
// changes, the backlog gauge) for as long as that send takes. This
// matches the pattern Open() already uses for its own drain loop below.
func (g *Gate[T]) Handle(msg T) {
	g.mu.Lock()
	if g.state == Open {
		g.mu.Unlock()
		g.forward(msg)
		return
	}
	g.evictExpiredLocked()
	if len(g.queue) >= g.maxLen {
		g.mu.Unlock()
		return // keepNewest:false — drop the new arrival, keep the backlog
	}
	g.queue = append(g.queue, entry[T]{msg: msg, at: time.Now()})
	g.mu.Unlock()
}

// evictExpiredLocked drops entries older than maxAge from the head of
// the queue. The queue is strict FIFO (append-only), so expired
// entries are always contiguous at the front — no need to scan past
// the first non-expired one. Must be called with g.mu held. No-op when
// maxAge is 0 (disabled).
func (g *Gate[T]) evictExpiredLocked() {
	if g.maxAge <= 0 || len(g.queue) == 0 {
		return
	}
	cutoff := time.Now().Add(-g.maxAge)
	i := 0
	for i < len(g.queue) && g.queue[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		g.queue = g.queue[i:]
	}
}

// Open drains the current backlog in order, then switches to
// pass-through mode for everything after. Call this on promotion to
// primary. Entries older than maxAge are dropped, not forwarded — see
// the package doc comment for why this must never publish a message
// dedup can no longer recognize as a duplicate.
func (g *Gate[T]) Open() {
	g.mu.Lock()
	g.evictExpiredLocked()
	backlog := make([]T, len(g.queue))
	for i, e := range g.queue {
		backlog[i] = e.msg
	}
	g.queue = nil
	g.state = Open
	g.mu.Unlock()

	for _, msg := range backlog {
		g.forward(msg)
	}
}

// Queue switches back to buffering mode (nothing is drained). Call
// this on becoming/remaining secondary — mirrors the flow's "Reset".
func (g *Gate[T]) Queue() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = Queueing
}

// Len reports the current backlog size — useful for a gauge metric so
// a growing secondary-side backlog is observable before it matters.
// Evicts expired entries first, so the reported size never counts
// backlog that Open() would just discard anyway.
func (g *Gate[T]) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.evictExpiredLocked()
	return len(g.queue)
}
