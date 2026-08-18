# shared/

Files here are copied to `{{ antiloop_dir }}/wis2node/` (see
`../group_vars/all.yml`) on every waloop host, identically except for
`common.env`'s templated `HOST=` line. Nothing here is per-centre —
see `../wis2node/` for that.

- `common.env.example` — the committed template. Copy it to
  `common.env` (gitignored — see `../.gitignore`) and fill in your
  real `MQTT_PUB_PASSWORD`, `REDIS_URL`, and Traefik settings. Treat
  the real file as a live credential file, never committed.
- `antiloop.version` — the fleet's current promoted binary version;
  not a secret, tracked normally.

See `../deploy_antiloop.yml` for how this gets deployed, and
`../bin/` / `../wis2node/` for the other two pieces (binary,
per-centre env files).
