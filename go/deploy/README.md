# deploy/ — antiloop Ansible deployment

Deploys antiloop to the waloop1-3 fleet, one wis2node (centre_id) at a
time, run under systemd. See `inventory.ini` for the hosts.

**First-time setup:** this directory ships only `*.example` templates
for anything that carries real credentials or real network addresses
(`inventory.ini.example`, `shared/common.env.example`,
`wis2node/_TEMPLATE.env.example`). Copy each one to its real name
(dropping `.example`, and for the last one, renaming to your actual
`<centre_id>.env`) and fill in your own values — those real files are
gitignored and never committed. See `.gitignore` in this directory.

## Day-to-day commands

    ./add_wis2node.sh <name> [user|system]   # deploy or redeploy
    ./del_wis2node.sh <name> [user|system]   # stop and remove, no redeploy

`<name>` is the centre_id without `.env` (e.g. `se-smhi`). The
`[user|system]` scope defaults to `user` (no sudo — real VM/bare-metal
targets). Use `system` for the current LXC test fleet, where
`systemd --user` lingering is broken at the container-host level (see
`deploy_antiloop.yml`'s doc comment) — that mode needs sudo (the
deploy account has NOPASSWD sudo configured on the LXC hosts) and runs
as root.

Re-running `add_wis2node.sh` for an already-deployed name **redeploys
it**: it stops the service wherever it's currently running, then picks
2 fresh hosts at random (not the same 2 every time — see
`deploy_antiloop.yml`'s "REDEPLOY behaviour" comment). It is not
idempotent-to-the-same-hosts on purpose.

Both scripts also run `sync-collector.yml` at the end, which keeps the
**collector** host (edge Traefik + Prometheus, see
`docs/architecture.md`) in sync with whatever's actually deployed —
see `inventory.ini.example`'s `[collector]` group.

## Local layout

- `shared/common.env` — the one env file shared by every centre_id,
  identical across the fleet except for one templated line
  (`HOST=...`, filled in per host). Deployed to
  `{{ antiloop_dir }}/wis2node/common.env` on all 3 hosts (`antiloop_dir`
  is defined once in `group_vars/all.yml`, derived from `ansible_user`
  in `inventory.ini`).
- `wis2node/<name>.env` — one file per centre_id (e.g. `se-smhi.env`).
  Deployed to `{{ antiloop_dir }}/wis2node/<name>.env`, only on the 2
  hosts selected for that centre.
- `shared/antiloop.version` — one line, `ANTILOOP_VERSION=<version>`,
  the single "current version" for the whole fleet (see Binary
  versioning below) — not a per-centre setting.
- `bin/antiloop-<version>` — compiled binaries. Multiple versions can
  coexist; see below.
- `templates/antiloop@.service.j2` — the systemd unit template (covers
  both `user` and `system` scope).
- `docs/architecture.md` — how the collector host's two-tier Traefik +
  Prometheus setup actually works end to end.

common.env, `<name>.env`, and `<name>.version` all land in the SAME
remote directory (`wis2node/`) on purpose — antiloop's own
`loadEnvFile` reads `common.env` then `<centre_id>.env` relative to its
working directory, and the systemd unit's `EnvironmentFile=` for the
version pin needs to sit there too.

## Binary versioning

Multiple `antiloop-<version>` binaries can sit in `bin/` at once, but
there's only ONE "current" version at any given time —
`shared/antiloop.version` — not a per-centre choice. Every
(re)deployment copies whatever that file currently says into that
centre's own `wis2node/<name>.version` on the target, read at process
start by a small wrapper (`bin/run-antiloop.sh`, see
`templates/antiloop@.service.j2`'s doc comment for why a wrapper is
needed instead of `EnvironmentFile=` substitution directly in
`ExecStart=`). Bumping `shared/antiloop.version` only affects
wis2nodes you actually redeploy afterward — anything already running
keeps whatever version it was deployed with, untouched, until you
redeploy it.

Build a new version from the sibling `source/` directory:

    cd ../source
    echo v1.4.0 > antiloop.version
    ./build.sh

This cross-compiles for linux/amd64 and drops the binary straight into
this repo's `bin/antiloop-<version>`. It does NOT make it current —
that's a deliberate separate step (build, test, then promote):

    echo 'ANTILOOP_VERSION=v1.4.0' > shared/antiloop.version

From that point on, any node you (re)deploy picks up v1.4.0:

    ./add_wis2node.sh se-smhi

To know what's actually running (not just what's been promoted), check
the process directly rather than reading files — the version is logged
at startup and served on `GET /<centre_id>/version` through Traefik
(or plain `GET /version` hitting the process itself).

The very first binary in this repo predates the versioning feature
(compiled before `Version`/`/version` existed in the Go source) and is
named `antiloop-initial` — rebuild with `build.sh` once you've pulled
the latest antiloop source to get real version reporting.

## Removal

    ./del_wis2node.sh se-smhi system

Finds wherever `antiloop@se-smhi` is currently running (checks all 3
hosts, not just 2), stops and disables it there, and removes its
`.env`/`.version` files. No restart, and it leaves `common.env`, the
binaries, and the shared unit template alone — none of those are
specific to one wis2node.
