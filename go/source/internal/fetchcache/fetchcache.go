// Package fetchcache fetches a static resource over HTTP, falling back
// to the last successfully-fetched copy cached in Redis if the HTTP
// request itself fails.
//
// This is scoped narrowly to resilience against a fetch source being
// temporarily unreachable (GitHub, GitHub Pages, or wherever a given
// schema/allowlist URL is hosted), not to reducing how often the
// source URLs get hit — every process still fetches independently on
// its own timer, same as before; Redis is only consulted when a fetch
// itself fails, and only written to opportunistically on success, so
// there's something to fall back to next time. See the doc comments on
// wnm.Validator, allowlist.Set, and allowlist.GDCRegistry for how each
// caller uses this.
package fetchcache

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/redis/go-redis/v9"
)

// Fetch retrieves url over HTTP. On success, the body is also
// best-effort written to Redis under redisKey (so it's available as a
// fallback on a future failure) and returned with usedFallback=false.
// On failure, if rdb is non-nil, it tries reading redisKey back from
// Redis instead — if that succeeds, returns the cached bytes with
// usedFallback=true and a nil error (this is still a usable result,
// just a stale one; callers should log, not treat it as failure to
// refresh). If both the HTTP fetch and the Redis fallback fail (or
// rdb is nil), returns the original HTTP error.
//
// rdb may be nil — callers without a Redis connection handy simply
// don't get the fallback, same as this package not existing.
func Fetch(ctx context.Context, client *http.Client, rdb redis.Cmdable, url, redisKey string) (body []byte, usedFallback bool, err error) {
	body, httpErr := fetchHTTP(ctx, client, url)
	if httpErr == nil {
		if rdb != nil {
			// Best-effort — a failed cache write doesn't fail this
			// fetch, it just means this round's success won't be
			// available as a future fallback.
			_ = rdb.Set(ctx, redisKey, body, 0).Err()
		}
		return body, false, nil
	}

	if rdb == nil {
		return nil, false, httpErr
	}
	cached, redisErr := rdb.Get(ctx, redisKey).Bytes()
	if redisErr != nil {
		return nil, false, fmt.Errorf("%w (redis fallback for %q also unavailable: %v)", httpErr, redisKey, redisErr)
	}
	return cached, true, nil
}

func fetchHTTP(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	if url == "" {
		return nil, fmt.Errorf("URL not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
