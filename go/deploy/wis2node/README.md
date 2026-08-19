# wis2node/

One `<centre_id>.env` per WIS2 centre, each deployed only to the 2 (of
3) waloop hosts selected for that centre — see `../deploy_antiloop.yml`.

- `_TEMPLATE.env.example` — the committed template, documents the
  shape of a per-centre file. Copy it to `<centre_id>.env`
  (gitignored — see `../.gitignore`) and fill in that centre's real
  broker URL/credentials to add a centre.

Real `<centre_id>.env` files are never committed: each one carries
that centre's actual MQTT credentials.
