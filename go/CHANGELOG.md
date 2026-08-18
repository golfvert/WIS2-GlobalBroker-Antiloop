# Go port changelog

Tracks review of each reference-implementation release (`image:tag` bumps)
against `go/source/`, per the automated port workflow
(`.github/workflows/go-port.yml`).

## wis2gb:2026.8.1

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
