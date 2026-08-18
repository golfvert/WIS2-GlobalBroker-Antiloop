#!/usr/bin/env bash
# Stops and removes one wis2node from wherever it's currently running
# across the waloop fleet. No restart — this is a pure teardown, not
# a redeploy (see add_wis2node.sh for that).
#
# Usage: ./del_wis2node.sh <name>
#   <name>   the node's name WITHOUT ".env"
#
# No scope argument, unlike add_wis2node.sh — deletion checks both
# systemd scopes (user and system) on every host automatically, so it
# doesn't matter which one it was deployed under.
#
# Wraps:
#   ansible-playbook -i inventory.ini remove_wis2node.yml -e wis2node_name=<name>
#   ansible-playbook -i inventory.ini sync-collector.yml
#     (regenerates the collector's Traefik dynamic config +
#     Prometheus targets so this node disappears from there too — see
#     sync-collector.yml's own doc comment)
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <name>" >&2
  exit 1
fi

name="$1"
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ansible-playbook -i "$dir/inventory.ini" "$dir/remove_wis2node.yml" \
  -e "wis2node_name=${name}"

# Same sync as add_wis2node.sh — this centre is now gone from all 3
# waloop hosts, so it must also disappear from the collector's Traefik
# dynamic config and Prometheus targets, not just linger there.
echo "--- syncing collector (Traefik dynamic config + Prometheus targets) ---"
exec ansible-playbook -i "$dir/inventory.ini" "$dir/sync-collector.yml"
