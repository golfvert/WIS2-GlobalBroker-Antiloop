#!/bin/sh
# Resolves and execs the right antiloop-<version> binary for a given
# centre_id, reading which version from wis2node/<centre_id>.version.
#
# systemd's ExecStart= does NOT expand variables sourced from
# EnvironmentFile= (that file is only read at process-spawn time,
# after ExecStart's own command line was already parsed and any
# ${VAR} substitution resolved — confirmed live: ExecStart tried to
# exec the literal path ".../antiloop-${ANTILOOP_VERSION}",
# status=203/EXEC, "No such file or directory"). A plain shell exec
# here is the reliable way to do this instead — see
# templates/antiloop@.service.j2's ExecStart, which just calls this.
#
# This file is deployed via ansible.builtin.template (not a plain
# copy), purely so the two paths below can pick up antiloop_dir from
# group_vars/all.yml — {{ }} is filled in by Ansible at deploy time,
# everything else here is ordinary POSIX shell evaluated at run time.
set -eu

centre_id="$1"
version_file="{{ antiloop_dir }}/wis2node/${centre_id}.version"

if [ ! -f "$version_file" ]; then
  echo "run-antiloop.sh: $version_file not found" >&2
  exit 1
fi

# Same KEY=VALUE shape systemd's own EnvironmentFile= expects (see
# deploy_antiloop.yml's "Binary versioning" doc comment) — also just
# valid POSIX shell, so sourcing it directly sets ANTILOOP_VERSION.
. "$version_file"

if [ -z "${ANTILOOP_VERSION:-}" ]; then
  echo "run-antiloop.sh: ANTILOOP_VERSION not set in $version_file" >&2
  exit 1
fi

bin="{{ antiloop_dir }}/bin/antiloop-${ANTILOOP_VERSION}"
if [ ! -x "$bin" ]; then
  echo "run-antiloop.sh: $bin not found or not executable" >&2
  exit 1
fi

exec "$bin" "$centre_id"
