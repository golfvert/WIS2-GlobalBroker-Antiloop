# Changelog

Notable changes to the Go implementation of the WIS2 Global Broker
(`go/source/`), by release tag (`antiloop-<version>`, published as a
GitHub Release by `.github/workflows/go-build.yml`). Building a
release does **not** promote it to the fleet — that's a separate,
deliberate step (`shared/antiloop.version` in the `Global_Broker_Go`/
`Global_Broker_FR` deploy repos; see `go/deploy/fetch_antiloop_binary.sh`).

This changelog covers the Go port only, not the Node-RED reference
flow (`flows.json`, versioned separately via this repo's own
`image:tag`). See `CLAUDE.md` for where and why the two implementations
deliberately diverge.

## 2026.8.6 — 2026-08-22

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

## 2026.8.5 — 2026-08-22

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

## 2026.8.4 — 2026-08-21

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

## 2026.8.3 — 2026-08-20

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

## 2026.8.2 — 2026-08-19

- Debug log lines for received/published payloads now use `%q`
  instead of `%s`. WNM payloads are sometimes pretty-printed JSON with
  real embedded newlines, and journald splits a service's stdout on
  every `\n` it sees — under `%s` a single message could be torn
  across several separate journal entries; `%q` escapes newlines to
  literal `\n` text so each log call stays one journal line.

## 2026.8.1 — 2026-08-19

- Added `-t` (repeatable/comma-separated MQTT topic filters, real
  `+`/`#` wildcard semantics), narrowing the `subscriber`/`publisher`
  `-d` debug categories to matching topics instead of logging every
  message. Live-adjustable via `<centre_id>.debug` `topic:<filter>`
  lines, same mechanism as `-d`'s categories.

## 1.0.0-rc6 and earlier

- Initial Go port of the Node-RED reference flow: election-based
  primary/secondary failover, MQTT subscribe/publish fan-out, Redis
  dedup, WNM schema validation, topic/metadata allowlist checks,
  Prometheus metrics. Predates this changelog and this repo's git
  history for `go/source/` — see `CLAUDE.md` for the current state of
  each of these mechanisms.
