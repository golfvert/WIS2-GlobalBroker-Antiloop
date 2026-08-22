# Go port changelog

Two different kinds of entries live here, on two different axes — a
flow release can land with "no Go changes needed," and a Go release
can happen with no corresponding flow release, so they're kept as
separate sections rather than forced onto one timeline:

- **Go releases** — what actually changed in `go/source/` itself, by
  release tag (`antiloop-<version>`, published as a GitHub Release by
  `.github/workflows/go-build.yml`). Building a release does **not**
  promote it to the fleet — that's a separate, deliberate step
  (`shared/antiloop.version` in the `Global_Broker_Go`/
  `Global_Broker_FR` deploy repos; see
  `go/deploy/fetch_antiloop_binary.sh`).
- **Flow-release reviews** — review of each Node-RED reference flow
  release (`image:tag` bump) against `go/source/`, run by the
  automated port workflow (`.github/workflows/go-port.yml`). Tagged
  `wis2gb:<image:tag>`.

Neither section covers the Node-RED reference flow's own behavior in
detail — see `CLAUDE.md` (in the antiloop working area) for where and
why the two implementations deliberately diverge.

## Go releases (`antiloop-<version>`)

### 2026.8.6 — 2026-08-22

- Added a `POST /inject` HTTP endpoint (reachable as
  `/<centre_id>/inject` through Traefik, no routing changes needed —
  same unprefixed-route pattern as `/health`/`/primary`/`/version`)
  that replays a `{"topic": ..., "payload": ...}` JSON body through the
  real Check/Validate/Dedup/Publish pipeline, reproducing Node-RED's
  manual inject-button capability. The replayed message is logged in
  full regardless of the process's active `-d` debug categories, via a
  new per-message `Msg.ForceDebug` field threaded through `debugf`/
  `debugfTopic` — an operator gets complete visibility into one
  deliberately-replayed message without turning any debug category on
  fleet-wide for every other concurrently-processing message.

### 2026.8.5 — 2026-08-22

- `internal/gate.Gate` — the role-gated backlog buffer that defers
  Check/Validate/Dedup/Publish while an instance is secondary — now
  evicts queued entries by age, not just by count. Previously a
  permanently-secondary instance could buffer messages indefinitely,
  bounded only by a 50000-entry cap; draining that backlog on a late
  promotion could publish messages that were both stale and, since
  dedup's own key TTL is 2 hours, undetected duplicates (the dedup key
  set by whoever actually published them while this instance was
  secondary had already expired by drain time). `gate.New` now takes a
  `maxAge`, wired to the exact same TTL constant used to construct the
  dedup layer, so the two can never drift apart.

### 2026.8.4 — 2026-08-21

- Added `TLS12Force` (`MQTT_SUB_TLS12FORCE[_BACKUP]`,
  `MQTT_PUB_TLS12FORCE[_2..5]`) — a per-target workaround for brokers
  with legacy TLS stacks that mishandle a modern TLS-1.3-capable
  ClientHello (driving case: cn-cma). Caps the handshake at TLS 1.2
  *and* re-adds the non-forward-secret RSA cipher suites Go's
  `crypto/tls` disables by default — both parts are required; neither
  alone was sufficient in practice.
- Added `SSLKEYLOGFILE` support, wired explicitly via
  `tls.Config.KeyLogWriter` since Go's `crypto/tls` doesn't
  auto-honor this env var the way curl/OpenSSL/Node/browsers do —
  needed for capturing TLS session secrets while diagnosing the above.

### 2026.8.3 — 2026-08-20

- Rewrote GDC_URL/TOPIC_URL allowlist checking to fall through to a
  direct per-key Redis `EXISTS` whenever a process's own in-memory
  cache doesn't have an answer, fixing a real bug where a single
  process's own metadata/topic-hash fetch failing silently disabled
  *that process's* checks instead of falling back to whatever the
  shared fleet already had cached in Redis.
- Added fast-path index SETs (`wis2gb:gdc_index:{<centre_id>}`,
  `wis2gb:{topic_index}`), rebuilt locally by every process on its own
  timer, so most processes' in-memory caches get populated without
  needing their own rare/gated fetch to succeed — avoids a Redis round
  trip on nearly every message check.
- Ported TOPIC_URL's per-instance refresh cadence exactly: an hourly
  Redis TTL check per process, refreshing only when within 5400s of a
  jittered 12–36h expiry, rather than a shared fleet-wide gate.

### 2026.8.2 — 2026-08-19

- Debug log lines for received/published payloads now use `%q`
  instead of `%s`. WNM payloads are sometimes pretty-printed JSON with
  real embedded newlines, and journald splits a service's stdout on
  every `\n` it sees — under `%s` a single message could be torn
  across several separate journal entries; `%q` escapes newlines to
  literal `\n` text so each log call stays one journal line.

### 2026.8.1 — 2026-08-19

- Added `-t` (repeatable/comma-separated MQTT topic filters, real
  `+`/`#` wildcard semantics), narrowing the `subscriber`/`publisher`
  `-d` debug categories to matching topics instead of logging every
  message. Live-adjustable via `<centre_id>.debug` `topic:<filter>`
  lines, same mechanism as `-d`'s categories.

### 1.0.0-rc6 and earlier

- Initial Go port of the Node-RED reference flow: election-based
  primary/secondary failover, MQTT subscribe/publish fan-out, Redis
  dedup, WNM schema validation, topic/metadata allowlist checks,
  Prometheus metrics. Predates this changelog and this repo's git
  history for `go/source/` — see `CLAUDE.md` for the current state of
  each of these mechanisms.

## Flow-release reviews (`wis2gb:<image:tag>`)

### wis2gb:2026.8.1

Reviewed diff since previous release `wis2gb:2026.7.17` (previous release
commit `a8b51a1`, current release commit `df043a2`).

**No Go source changes made this release.** Everything the diff touches was
already reflected in `go/source/` before this review:

- **Redis key namespace prefixing** (`flows.json`): the dedup, election,
  instances, and per-uuid liveness keys all gained a `wis2gb:` prefix
  (`wis2gb:dedup:<id>`, `wis2gb:election:<centre_id>`, `wis2gb:instances`,
  `wis2gb:uuid_<uuid>`). This was originally introduced in commit `559811e`
  ("Rename all Redis keys with wis2gb:xxx"), which landed *before* the Go
  reimplementation was added in `715372b`. `go/source/internal/dedup`,
  `go/source/internal/election`, and `cmd/antiloop/main.go` were written
  against the already-renamed keyspace, so no change was needed.
- **`wis2-notification-message.json`** (new file in this diff): this is a
  scheduled-automation artifact (`.github/workflows/Remove_example_schema.yml`,
  a daily cron job), not something introduced by hand in this release, and
  not something `flows.json` reads from disk — verified no reference to it
  anywhere in `flows.json`. The Go WNM validator (`go/source/internal/wnm`)
  fetches its schema from `SCHEMA_WNM_URL`, which already points at a
  separate mirror (`golfvert/wis2-topic-hierarchy`), independent of this
  file. No Go-side equivalent applies.
- **`package.json`**: `node-red` bumped 5.0.1 → 5.0.4, and the
  now-unused `node-red-contrib-full-msg-json-schema-validation` dependency
  was dropped (schema validation in `flows.json` already runs through
  `Ajv`-in-a-function-node, not that contrib package). Both are Node-RED
  packaging changes with no Go/npm equivalent.
- **`settings.js`**: no changes in this release.

No `go/deploy/` changes needed — nothing in this diff implies a new or
changed env var/config shape.
