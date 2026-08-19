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
// Deviation from the original flow, flagged: the flow's Query/Save
// nodes read/write this through Redis (a shared cache keyed something
// like "metadata_id:<centreid>:<topic>", built by whichever process
// last ran the refresh, queried by all). This implementation fetches
// and parses GDC_URL directly in each process instead, filtering to
// only this CENTRE_ID's entries at parse time. Functionally
// equivalent — every process ends up with the correct per-centre
// answer — but trades the shared Redis cache for redundant per-process
// fetches of a few-hundred-line file every refresh interval, which is
// cheap enough not to matter at typical fleet sizes. If a large fleet
// hitting GDC_URL independently ever becomes a problem (e.g. rate
// limits on the hosting side), the shared Redis-cache version is the
// fallback design.
package allowlist

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"antiloop/internal/fetchcache"
)

type GDCRegistry struct {
	url      string
	centreID string

	httpClient *http.Client
	rdb        redis.Cmdable

	mu     sync.RWMutex
	topics map[string]struct{} // this centre's registered "data/core/.../synop" suffixes
}

// rdb backs a Redis fallback for when url can't be reached — see
// internal/fetchcache's doc comment. May be nil to disable the
// fallback.
func NewGDCRegistry(url, centreID string, rdb redis.Cmdable) *GDCRegistry {
	return &GDCRegistry{
		url:        url,
		centreID:   centreID,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		rdb:        rdb,
		topics:     map[string]struct{}{},
	}
}

func (g *GDCRegistry) Refresh(ctx context.Context) error {
	if g.url == "" || g.centreID == "" {
		return nil
	}
	body, usedFallback, err := fetchcache.Fetch(ctx, g.httpClient, g.rdb, g.url, "wis2gb:cache:gdc")
	if err != nil {
		return err
	}
	if usedFallback {
		log.Printf("GDC registry: %s unreachable, using last-known-good copy cached in Redis", g.url)
	}

	next := make(map[string]struct{})
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
		if urnParts[3] != g.centreID {
			continue // not our centre — this file covers every centre globally
		}
		next[topic] = struct{}{}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	g.topics = next
	g.mu.Unlock()
	return nil
}

// Has checks a raw data-topic path, e.g.
// "data/core/weather/surface-based-observations/synop" — this is
// topics.Result.DataTopic, NOT the hashed Topic field (that one's for
// the TOPIC_URL check instead).
func (g *GDCRegistry) Has(dataTopic string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.topics[dataTopic]
	return ok
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
		log.Printf("GDC registry: %d topics registered for centre_id=%s", g.Len(), g.centreID)
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
