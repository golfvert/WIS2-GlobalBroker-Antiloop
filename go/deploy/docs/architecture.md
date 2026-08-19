# Routing & metrics architecture

Two independent Traefik tiers, plus Prometheus, work together so that
(a) every centre_id's metrics/health are reachable at a stable public
path regardless of which physical host currently runs it, and (b)
failover between the 2 hosts running a given centre is automatic, with
no coordination logic anywhere outside Traefik itself.

```
Prometheus  --scrape-->  collector edge Traefik  --route-->  waloop host's local Traefik  -->  antiloop process
 (collector)              (collector, TLS)                    (waloop, plain HTTP)              (:metrics/:health/:primary)
```

## Tier 1 — local Traefik on each waloop host

Every waloop host runs its own Traefik instance. antiloop doesn't get
a fixed, hand-assigned metrics port: at startup, each antiloop process
binds a metrics listener on an OS-assigned port and self-registers a
small dynamic-config file with its host's local Traefik (see
`source/internal/traefik`'s package doc comment, and `Register`/
`Deregister`). That file is removed automatically on graceful
shutdown, and overwritten if antiloop restarts.

The router this produces is path-based: `PathPrefix(`/<centre_id>`)`,
with a `stripPrefix` middleware so antiloop's own HTTP mux (which
registers plain, unprefixed `/metrics`, `/health`, `/primary` routes —
see `cmd/antiloop/main.go`) doesn't need to know its own centre_id
prefix at all. TLS is NOT terminated here — this tier runs plain HTTP
on the `web` entrypoint (`TRAEFIK_ENTRYPOINT=web`,
`TRAEFIK_TLS=false` in `shared/common.env`), because TLS termination
happens once, centrally, at tier 2.

This whole tier is entirely self-managed by the antiloop binaries
themselves — no Ansible playbook writes or touches these per-centre
files. A given waloop host only has a router for a centre_id if it's
actually one of the 2 (of 3) hosts currently running that centre; the
third host simply has no router for it, so a request there 404s at
that host's own Traefik routing layer.

## Tier 2 — the collector host's edge Traefik

A single **collector** host runs one edge Traefik instance that fronts
all 3 waloop hosts. For every currently active centre_id, it has:

- one router: `PathPrefix(`/<centre_id>`)`, TLS terminated here
  (`tls: {}`)
- one service: a `loadBalancer` listing **all 3** waloop hosts as
  candidate backends (not just the 2 actually running that centre —
  see below), with a `healthCheck` hitting `/<centre_id>/primary`
  every 5s on each candidate.

Because only the actual current primary for a centre answers that
health check as healthy (the secondary answers differently, and the
third host has no router at all for that centre, i.e. it 404s),
Traefik only ever sends live traffic to whichever of the 3 hosts is
genuinely primary right now. Failover on an election change is
therefore automatic and entirely Traefik-native: nothing needs to
tell the collector's Traefik that a role changed, it just stops
seeing the old primary as healthy and starts seeing the new one.

This tier's config IS Ansible-managed: `sync-collector.yml` scans all
3 waloop hosts for which `<centre_id>.env` files are actually deployed
right now (the real source of truth — see that playbook's own doc
comment), and renders `templates/traefik-centres.yml.j2` into
`{{ traefik_dir }}/dynamic/wis2gb-centres.yml` on the collector host.
It's triggered automatically at the end of both `add_wis2node.sh` and
`del_wis2node.sh`, and can also be run standalone any time you want
the collector's config to match reality.

**One file for every active centre, not one file per centre** — see
"Scalability" below for why.

## Prometheus

Prometheus also runs on the collector host, and scrapes through the
collector's OWN edge Traefik rather than reaching any waloop host
directly. `sync-collector.yml` also renders
`templates/wis2gb_targets.json.j2` into a Prometheus file_sd targets
file (`{{ prometheus_dir }}/wis2gb_targets.json`), one entry per active
centre_id: `{"targets": ["traefik:443"], "labels": {"__metrics_path__":
"/<centre_id>/metrics", ...}}`.

Since the scrape target is always the collector's own Traefik
container (over the shared Docker network, by container name), a
scrape for any centre_id automatically rides the exact same
health-checked failover routing described above — Prometheus needs
zero host-selection logic of its own, and keeps scraping correctly
through a role change without any reconfiguration.

## Scalability

An earlier version of `sync-collector.yml` rendered ONE Traefik
dynamic-config file PER active centre, via an Ansible `loop:` over
`active_centres` calling `ansible.builtin.template` once per item.
Each loop iteration is a separate module invocation — effectively a
separate SSH round trip to the collector host. That's fine at 3
centres, but scales linearly and would get genuinely slow with a
couple hundred (the target file-render step alone could take minutes,
on every single `add_wis2node.sh`/`del_wis2node.sh` run).

The fix: `templates/traefik-centres.yml.j2` loops over
`active_centres` with Jinja's own `{% for %}` INSIDE a single template,
producing one file containing every active centre's router+service —
exactly the pattern `templates/wis2gb_targets.json.j2` already used for
the Prometheus targets file. Rendering that template is always exactly
one Ansible task, regardless of whether 3 or 300 centres are active —
so the cost of a resync no longer grows with fleet size. The
now-unnecessary "delete stale per-centre files" cleanup loop went away
with it, since there's only one file, fully rewritten from scratch
every run (still idempotent, still no separate state file — same
design principle as before, just implemented without an Ansible-level
loop).

If you're migrating a collector host that still has old per-centre
`<centre_id>.yml` files from before this change, remove them by hand
once — `sync-collector.yml` no longer knows about or manages them,
only `wis2gb-centres.yml`.
