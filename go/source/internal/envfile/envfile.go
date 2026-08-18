// Package envfile reads simple KEY=VALUE files (common.env,
// <centre_id>.env, and any other file in that shape) into the current
// process's environment, so config.Load()'s (and any other package's)
// os.Getenv calls see them.
//
// This is a shared, standalone implementation (rather than living
// privately inside cmd/antiloop) so other tools — e.g. cmd/wis2nodes —
// can load the exact same REDIS_URL/REDIS_CLUSTER values straight out
// of the real common.env, using identical parsing rules, instead of
// requiring those values to be re-typed as shell env vars or CLI flags
// on every run, or risking two independent copies of the quoting logic
// silently drifting apart.
package envfile

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Load reads a simple KEY=VALUE file and sets each key into the
// current process's environment via os.Setenv — this is what replaces
// `source`-ing the file in bash before running a binary.
//
// Format: one KEY=VALUE per line. Blank lines and lines whose first
// non-whitespace character is "#" are ignored. Whitespace around both
// key and value is trimmed.
//
// If, after trimming, the value is wrapped in a single matching pair
// of ' or " (value[0] and value[len-1] are the same quote character),
// that whole value is taken as-is between the quotes (stripped, no
// escape-sequence interpretation) and nothing inside it — including a
// "#" — is ever treated as a comment. This is what makes an existing
// quoted value (e.g. REDIS_URL='[{"host":"x","port":6379}]', quoted
// because it used to need to survive bash's `source`) keep working
// unchanged, AND makes a quoted value containing "#" safe.
//
// An UNQUOTED value is taken literally up to, but not including, the
// first " #" or "\t#" — i.e. a "#" preceded by whitespace starts a
// trailing comment (matches common.env.example's
// "ELECTION_INTERVAL=2s   # matches the flow's..." style lines). A "#"
// with NO preceding whitespace is just part of the value, not a
// comment — required because MQTT_SUB_TOPIC's wildcard segments end
// literally in "#" with nothing separating it from the rest of the
// topic (e.g. "origin/a/wis2/se-smhi/#"), and that must survive
// intact. Besides that one rule, an unquoted value is otherwise
// completely literal — no shell involved, so no shell-quoting class of
// bug is possible here (this is what actually fixes the REDIS_URL
// "invalid character 'h'" bug: that JSON value now works whether or
// not it's wrapped in quotes, instead of silently breaking if bash's
// quoting wasn't exactly right).
func Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNum := 0
	for sc.Scan() {
		lineNum++
		trimmed := strings.TrimSpace(sc.Text())
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key, rawValue, ok := strings.Cut(trimmed, "=")
		if !ok {
			return fmt.Errorf("%s:%d: no '=' found: %q", path, lineNum, trimmed)
		}
		key = strings.TrimSpace(key)
		value := strings.TrimSpace(rawValue)

		if n := len(value); n >= 2 {
			first, last := value[0], value[n-1]
			if (first == '\'' || first == '"') && first == last {
				value = value[1 : n-1]
			} else if i := indexInlineComment(value); i >= 0 {
				value = strings.TrimSpace(value[:i])
			}
		}

		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("%s:%d: setenv %s: %w", path, lineNum, key, err)
		}
	}
	return sc.Err()
}

// indexInlineComment returns the index of the first "#" preceded by a
// space or tab (the start of a trailing comment), or -1 if there is
// none. A "#" with no preceding whitespace — e.g. an MQTT topic
// wildcard like ".../se-smhi/#" — is not a comment and is ignored.
func indexInlineComment(s string) int {
	for i := 1; i < len(s); i++ {
		if s[i] == '#' && (s[i-1] == ' ' || s[i-1] == '\t') {
			return i
		}
	}
	return -1
}
