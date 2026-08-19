// Package wnm validates messages against WIS2 Notification Message
// (WNM) or WIS2 Metadata Event (WME) JSON Schemas, matching the
// "Compile WNM" / "Compile WME" / "Schema" function nodes: schema is
// fetched over HTTP, compiled once (ajv in the original, gojsonschema
// here), recompiled on a refresh timer (the "TTL" inject, 3600s), and
// picked by message type — "monitor"-prefixed topics use WME, everything
// else uses WNM.
//
// NEEDS CONFIRMATION: the original flow's two "Get" http request nodes
// have empty URLs in the exported flows.json — they're set dynamically
// elsewhere (not visible in the export). Point WNM_SCHEMA_URL /
// WME_SCHEMA_URL at the actual schema documents (likely from
// https://github.com/wmo-im/wis2-notification-message or the WIS2
// schemas served off schemas.wmo.int) before relying on this.
package wnm

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xeipuuv/gojsonschema"

	"antiloop/internal/fetchcache"
)

type Validator struct {
	mu         sync.RWMutex
	wnmSchema  *gojsonschema.Schema
	wmeSchema  *gojsonschema.Schema
	wnmURL     string
	wmeURL     string
	httpClient *http.Client
	rdb        redis.Cmdable
}

// rdb backs a Redis fallback for when wnmURL/wmeURL can't be reached —
// see internal/fetchcache's doc comment. May be nil to disable the
// fallback (fetch failures then behave exactly as before).
func New(wnmURL, wmeURL string, rdb redis.Cmdable) *Validator {
	return &Validator{
		wnmURL:     wnmURL,
		wmeURL:     wmeURL,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		rdb:        rdb,
	}
}

// Refresh re-fetches and recompiles both schemas. Call once at startup
// and then on a ticker (config.SchemaRefresh, default 1h — matches the
// "TTL" inject node).
func (v *Validator) Refresh(ctx context.Context) error {
	wnm, err := v.fetchAndCompile(ctx, v.wnmURL, "wis2gb:cache:schema:wnm")
	if err != nil {
		return fmt.Errorf("compile WNM schema: %w", err)
	}
	wme, err := v.fetchAndCompile(ctx, v.wmeURL, "wis2gb:cache:schema:wme")
	if err != nil {
		return fmt.Errorf("compile WME schema: %w", err)
	}

	v.mu.Lock()
	v.wnmSchema, v.wmeSchema = wnm, wme
	v.mu.Unlock()
	return nil
}

func (v *Validator) fetchAndCompile(ctx context.Context, url, redisKey string) (*gojsonschema.Schema, error) {
	if url == "" {
		return nil, fmt.Errorf("schema URL not configured")
	}
	body, usedFallback, err := fetchcache.Fetch(ctx, v.httpClient, v.rdb, url, redisKey)
	if err != nil {
		return nil, err
	}
	if usedFallback {
		log.Printf("wnm: %s unreachable, using last-known-good schema cached in Redis (%s)", url, redisKey)
	}

	// The bundled WME schema embeds a nested sub-schema —
	// allOf[1].properties.data.properties.links.items — that carries
	// its own "$id"/"$schema" (it's the WCMP link.yaml schema, inlined).
	// Per the JSON Schema spec, a nested "$id" starts a new base URI
	// scope, so any "$ref": "#/definitions/..." underneath it resolves
	// against that sub-schema's root instead of the document's actual
	// top-level "definitions" — which don't exist there, and
	// gojsonschema fails compiling with "Object has no key
	// 'definitions'". The original Node-RED flow strips $id/$schema
	// from that nested sub-schema before compiling for the same reason.
	// Ported here as a generic recursive strip (every nested $id/$schema,
	// not just the one at this specific path) so it keeps working if the
	// schema's shape shifts on a future refresh.
	var doc interface{}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}
	stripNestedIDs(doc, true)
	cleaned, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	loader := gojsonschema.NewBytesLoader(cleaned)
	return gojsonschema.NewSchema(loader)
}

// stripNestedIDs deletes "$id" and "$schema" from every object in the
// tree except the root (see the doc comment in fetchAndCompile), and
// separately drops any "pattern" field Go's regexp engine can't
// compile.
//
// The WME schema's "time.resolution" field validates ISO 8601
// durations with pattern "^(-?)P(?=\\d|T\\d)(?:(\\d+)Y)?...$" — that
// "(?=...)" is a lookahead assertion. Go's regexp package is RE2-based,
// and RE2 deliberately doesn't support lookahead/lookbehind/
// backreferences (they can be exponential-time under RE2's
// guarantees), so gojsonschema fails the entire schema compile with
// "pattern must be a valid regex" over this one field — the same class
// of problem as the $id case above: a single unsupported node breaking
// compilation of everything else around it. Rather than hand-patch
// that one known pattern (and risk missing another PCRE-only one
// elsewhere in either schema, or a future schema revision), this tries
// to compile every "pattern" found and just drops the ones that fail —
// that field goes unvalidated by regex rather than the whole schema
// refusing to load. root is only true for the initial call.
func stripNestedIDs(v interface{}, root bool) {
	switch t := v.(type) {
	case map[string]interface{}:
		if !root {
			delete(t, "$id")
			delete(t, "$schema")
		}
		if pat, ok := t["pattern"].(string); ok {
			if _, err := regexp.Compile(pat); err != nil {
				delete(t, "pattern")
			}
		}
		for _, child := range t {
			stripNestedIDs(child, false)
		}
	case []interface{}:
		for _, child := range t {
			stripNestedIDs(child, false)
		}
	}
}

// Validate picks WME vs WNM the same way the "Schema" function node
// does: topic prefix "monitor" -> WME, everything else -> WNM. Returns
// (valid, error) — error is only for infra failures (schema not loaded
// yet, payload not valid JSON at all), not validation failures.
func (v *Validator) Validate(topic string, payload []byte) (bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	schema := v.wnmSchema
	if strings.HasPrefix(topic, "monitor") {
		schema = v.wmeSchema
	}
	if schema == nil {
		return false, fmt.Errorf("validator not initialized yet")
	}

	var doc interface{}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return false, fmt.Errorf("payload not valid JSON: %w", err)
	}

	result, err := schema.Validate(gojsonschema.NewGoLoader(doc))
	if err != nil {
		return false, err
	}
	return result.Valid(), nil
}
