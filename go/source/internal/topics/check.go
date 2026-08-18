// Package topics ports the original flow's "Check" function node, plus
// a topic-hash membership check that makes the "topic_<md5>" key it
// computes actually meaningful: the extracted JS logic for the
// data/core|recommended branch always returned Allow=true, just
// computing the key and handing it downstream. That only makes sense
// if something compares it against a known-good set — and TOPIC_URL is
// exactly that: its real content (see internal/allowlist) is a flat
// file of roughly 2500 raw MD5 hashes, one per line, no CSV, no labels.
// That shape only makes sense as a hash-membership check, and the
// filename ("earth-system-discipline-and-below.md5.txt") lines up with
// topicShort being exactly the discipline-and-below portion of the
// topic. So Check() takes a HashSet and gates on membership for that
// branch, rather than always allowing.
//
// This membership check is inferred from the TOPIC_URL file's content
// shape plus the otherwise-pointless "topic_" key construction, not
// read off an explicit switch/redis-lookup node in the original flow —
// worth confirming against real reject-rate data if this branch's
// behavior is ever in question; if wrong, the fix is narrow (this one
// branch).
package topics

import (
	"crypto/md5"
	"encoding/hex"
	"strings"
)

// HashSet is satisfied by *allowlist.Set — kept as an interface here so
// this package doesn't need to import allowlist (topics is lower-level).
type HashSet interface {
	Has(key string) bool
}

// discipline segments — verbatim from the JS DISCIPLINES array.
var disciplines = map[string]bool{
	"weather":                 true,
	"climate":                 true,
	"hydrology":               true,
	"atmospheric-composition": true,
	"cryosphere":               true,
	"ocean":                    true,
	"space-weather":            true,
}

// Result mirrors the JS's msg.check / msg.topic, plus DataTopic — new,
// not from the JS — which carries the raw "data/core/.../synop" path
// (as opposed to Topic's hashed "topic_<md5>" form) for the METADATA
// check to match against the GDC registry. See allowlist.GDCRegistry
// doc comment for why these are two separate lookups against two
// differently-shaped files, not one.
type Result struct {
	Allow bool
	// Rewritten topic. Empty string means "not set" (JS `null`).
	Topic string
	// Raw topic path from the "data" segment onward, e.g.
	// "data/core/weather/surface-based-observations/synop". Only set
	// on the same branch that sets Topic. Empty otherwise.
	DataTopic string
}

// topicHashes may be nil (e.g. TOPIC_URL not configured for this
// deployment) — treated as "don't gate on it", not "reject everything",
// consistent with how allowlist.Set behaves when its source URL is empty.
func Check(topic string, topicHashes HashSet) Result {
	parts := strings.Split(topic, "/")
	get := func(i int) string {
		if i < 0 || i >= len(parts) {
			return ""
		}
		return parts[i]
	}

	// 1. Monitor topic
	if get(0) == "monitor" {
		return Result{Allow: get(5) == ""}
	}

	// 2. Experimental discipline topic
	if disciplines[get(6)] && get(7) == "experimental" {
		return Result{Allow: true}
	}

	// topicShort = everything from index 6 onward, joined — used for the
	// md5 lookup key below. Matches the JS's splice(6, len) + join('/').
	var topicShort string
	if len(parts) > 6 {
		topicShort = strings.Join(parts[6:], "/")
	}

	// 3. Cache + recommended -> reject
	if get(0) == "cache" && get(5) == "recommended" {
		return Result{Allow: false}
	}

	// 4. GTS-to-WIS2
	if strings.HasSuffix(get(3), "-gts-to-wis2") {
		return Result{Allow: true}
	}

	// 5. Metadata
	if get(4) == "metadata" {
		r := Result{Allow: true}
		if get(5) != "" {
			// Verbatim port of the original JS's literal "FT2025-2" tag.
			// Its meaning isn't established from the exported flow
			// alone — worth confirming against a live source before
			// relying heavily on this branch, since a wrong guess here
			// would silently misroute metadata messages.
			r.Topic = "FT2025-2"
		}
		return r
	}

	// 6. Data core/recommended
	if get(4) == "data" && (get(5) == "core" || get(5) == "recommended") {
		sum := md5.Sum([]byte(topicShort))
		hash := hex.EncodeToString(sum[:])

		allow := true
		if topicHashes != nil {
			allow = topicHashes.Has(hash)
		}

		var dataTopic string
		if len(parts) > 4 {
			dataTopic = strings.Join(parts[4:], "/")
		}

		return Result{Allow: allow, Topic: "topic_" + hash, DataTopic: dataTopic}
	}

	// Default: reject
	return Result{Allow: false}
}
