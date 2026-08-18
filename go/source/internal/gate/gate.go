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
package gate

import "sync"

type State int

const (
	Queueing State = iota
	Open
)

type Gate[T any] struct {
	mu       sync.Mutex
	state    State
	queue    []T
	maxLen   int
	forward  func(T)
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
func New[T any](maxLen int, forward func(T)) *Gate[T] {
	return &Gate[T]{maxLen: maxLen, forward: forward}
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
	if len(g.queue) >= g.maxLen {
		g.mu.Unlock()
		return // keepNewest:false — drop the new arrival, keep the backlog
	}
	g.queue = append(g.queue, msg)
	g.mu.Unlock()
}

// Open drains the current backlog in order, then switches to
// pass-through mode for everything after. Call this on promotion to
// primary.
func (g *Gate[T]) Open() {
	g.mu.Lock()
	backlog := g.queue
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
func (g *Gate[T]) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.queue)
}
