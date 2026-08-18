#!/usr/bin/env bash
# Deploys and starts one wis2node across the waloop fleet. Re-running
# this for a name that's already deployed REDEPLOYS it: stops it
# wherever it's currently running, then picks 2 fresh hosts at random
# (see deploy_antiloop.yml's "REDEPLOY behaviour" doc comment) — it
# does not just leave it where it was.
#
# Usage: ./add_wis2node.sh <name> [user|system]
#   <name>          the node's name WITHOUT ".env", e.g.:
#                      ./add_wis2node.sh se-smhi
#                      ./add_wis2node.sh br-inmet-global-broker
#   [user|system]   systemd scope, defaults to "user" (no sudo, real
#                    VM/bare-metal targets). Use "system" for the LXC
#                    test fleet, where systemd --user lingering is
#                    broken at the container-host level (see
#                    deploy_antiloop.yml's doc comment) — that mode
#                    needs sudo and runs as root.
#
# Requires wis2node/<name>.env to already exist in this repo, mirroring
# se-smhi.env's shape — CENTRE_ID inside must match <name>. The binary
# version isn't chosen per node: shared/antiloop.version is the one
# global "current version" file, and whatever it says gets deployed to
# this node right now (see README.md's binary-versioning section) —
# bump that file to move new deployments onto a different build.
#
# Wraps:
#   ansible-playbook -i inventory.ini deploy_antiloop.yml \
#     -e wis2node_name=<name> -e service_scope=<scope> -e redeploy_seed=<fresh random value>
#   ansible-playbook -i inventory.ini sync-collector.yml
#     (regenerates the collector's Traefik dynamic config +
#     Prometheus targets so this node shows up there too — see
#     sync-collector.yml's own doc comment)
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <name> [user|system]  (name without .env, e.g. se-smhi)" >&2
  exit 1
fi

name="$1"
scope="${2:-user}"

if [ "$scope" != "user" ] && [ "$scope" != "system" ]; then
  echo "error: scope must be 'user' or 'system', got: $scope" >&2
  exit 1
fi

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
env_file="$dir/wis2node/${name}.env"
version_file="$dir/shared/antiloop.version"

if [ ! -f "$env_file" ]; then
  echo "error: $env_file not found — add it first (see wis2node/se-smhi.env for the shape)" >&2
  exit 1
fi
if [ ! -f "$version_file" ]; then
  echo "error: $version_file not found — this is a repo-level file, not per-node; add it once:" >&2
  echo "  echo 'ANTILOOP_VERSION=initial' > $version_file" >&2
  exit 1
fi

# Fresh every invocation, on purpose — this is what makes host
# reselection genuinely random across separate runs while still being
# the SAME value seen by all 3 hosts within this one run (they each
# compute the pick independently from it, no cross-host coordination
# needed — see deploy_antiloop.yml). $RANDOM alone isn't enough
# entropy on its own across quick repeated runs, hence pairing it with
# the current time in nanoseconds.
redeploy_seed="$(date +%s%N)-$RANDOM"

# NOPASSWD sudo is configured for the deploy account on the LXC test
# fleet, so system scope needs no password prompt — become just works
# silently.
ansible-playbook -i "$dir/inventory.ini" "$dir/deploy_antiloop.yml" \
  -e "wis2node_name=${name}" -e "service_scope=${scope}" -e "redeploy_seed=${redeploy_seed}"

# Regenerates the collector's Traefik dynamic config and Prometheus
# file_sd targets from whatever's actually deployed on waloop now,
# which includes this new node — keeps the collector from ever showing
# instances that aren't really deployed. Only runs if the deploy above
# succeeded (set -e above).
echo "--- syncing collector (Traefik dynamic config + Prometheus targets) ---"
exec ansible-playbook -i "$dir/inventory.ini" "$dir/sync-collector.yml"
