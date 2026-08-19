// Set fetches TOPIC_URL — a plain text file, one raw MD5 hash per line
// (fetched and confirmed: ~2500 lines, nothing but hex hashes, no CSV,
// no labels). Refreshed on a timer (matches the flow's "Refresh"
// inject, 900s), held as an in-memory set per process for O(1)
// lookup — no need to round-trip to Redis or an HTTP API on the hot
// path. Used by topics.Check() for its topic-hash membership test —
// see that package's doc comment.
//
// GDC_URL is a different shape entirely — CSV (metadata_id,topic
// pairs), not a flat list — so it gets its own type, GDCRegistry, in
// gdc.go rather than reusing this one.
package allowlist

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"antiloop/internal/fetchcache"
)

type Set struct {
	url        string
	httpClient *http.Client
	rdb        redis.Cmdable

	mu      sync.RWMutex
	entries map[string]struct{}
}

// rdb backs a Redis fallback for when url can't be reached — see
// internal/fetchcache's doc comment. May be nil to disable the
// fallback.
func New(url string, rdb redis.Cmdable) *Set {
	return &Set{
		url:        url,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		rdb:        rdb,
		entries:    map[string]struct{}{},
	}
}

// Refresh re-fetches the text file and swaps in a new set atomically.
// A failed refresh (and no usable Redis fallback either) keeps serving
// the previous (stale) set rather than clearing it — a fetch hiccup
// shouldn't suddenly reject every message as "not in allowlist".
func (s *Set) Refresh(ctx context.Context) error {
	if s.url == "" {
		return nil // not configured for this deployment; Has() just always returns false
	}
	body, usedFallback, err := fetchcache.Fetch(ctx, s.httpClient, s.rdb, s.url, "wis2gb:cache:topic-hierarchy")
	if err != nil {
		return err
	}
	if usedFallback {
		log.Printf("allowlist: %s unreachable, using last-known-good copy cached in Redis", s.url)
	}

	next := make(map[string]struct{})
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		next[line] = struct{}{}
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		return err
	}

	s.mu.Lock()
	s.entries = next
	s.mu.Unlock()
	return nil
}

// Has is nil-safe on purpose: topics.Check() takes this through a
// HashSet interface, and an interface holding a typed nil *Set is NOT
// itself == nil (classic Go gotcha) — so a nil check on the interface
// value in Check() wouldn't catch a nil *Set passed in by a caller
// that forgot to construct one. Guarding here instead means "not
// configured" fails safe (Has -> false, same as an empty set) no
// matter which side the nil comes from.
func (s *Set) Has(key string) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[key]
	return ok
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
