#!/usr/bin/env bash
# Cross-compiles antiloop AND wis2nodes. The waloop fleet build of
# antiloop (linux/amd64 only) drops straight into the sibling deploy/
# dir's bin/ (../deploy/bin, i.e. this repo's own deploy/ — see
# deploy/README.md's "binary versioning" section for why multiple
# versions coexist there instead of one file getting overwritten each
# build. wis2nodes is never deployed to the fleet at all (it's an
# operator-run diagnostic against the fleet's Redis, run from your own
# machine — see cmd/wis2nodes's own package doc comment), so every
# wis2nodes build lands in ./dist only, all 4 platforms, none of them
# touching deploy/.
#
# The tag comes from antiloop.version (next to this script) — edit
# THAT file to change what gets built, then run this with no
# arguments. This is a separate, deliberate file rather than a CLI
# flag or env var so the tag you're building is always visible/diffable
# in the repo, not just whatever happened to be typed at the shell.
# Reused as wis2nodes's own dist/ filename tag too, purely for a
# consistent naming scheme across dist/ — wis2nodes has no
# main.Version to actually embed (see the build() calls below), so the
# tag there is a build-batch label, not a version baked into the
# binary the way it is for antiloop.
#
# CROSS-COMPILE APPROACH: Go cross-compiles natively — just set
# GOOS/GOARCH per build, no separate toolchain or emulation needed.
# CGO_ENABLED=0 below keeps every build fully static (antiloop has no
# cgo dependency, go-redis included, so this costs nothing and avoids
# needing a C cross-compiler for the non-native targets).
#
# ONLY antiloop's linux/amd64 is deployable to the fleet as-is:
# deploy_antiloop.yml copies the whole deploy/bin/ dir to every waloop
# host, and deploy/templates/run-antiloop.sh execs the exact path
# bin/antiloop-<tag> — no arch suffix, no per-host arch detection. So
# that one build keeps that exact name and lands in deploy_dir/bin/,
# same as before. Every other build (antiloop's other 3 targets, and
# all 4 of wis2nodes's) is arch-suffixed and lands in ./dist instead —
# Ansible never looks there, so none of them can accidentally get
# shipped to a host with the wrong architecture, and wis2nodes builds
# specifically can never end up on the fleet at all.
#
# Usage: ./build.sh
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
version_file="$dir/antiloop.version"

if [ ! -f "$version_file" ]; then
  echo "error: $version_file not found — create it with the tag to build, e.g.:" >&2
  echo "  echo v1.4.0 > $version_file" >&2
  exit 1
fi

tag="$(tr -d '[:space:]' < "$version_file")"
if [ -z "$tag" ]; then
  echo "error: $version_file is empty — put a tag in it, e.g.: echo v1.4.0 > $version_file" >&2
  exit 1
fi

deploy_dir="$dir/../deploy"
fleet_out="$deploy_dir/bin/antiloop-${tag}"
dist_dir="$dir/dist"

if [ ! -d "$deploy_dir" ]; then
  echo "error: $deploy_dir not found — this script expects to sit in source/ next to a sibling deploy/ dir" >&2
  exit 1
fi

mkdir -p "$deploy_dir/bin"

mkdir -p "$dist_dir"

build() {
  local goos="$1" goarch="$2" out="$3" desc="$4" pkg="$5" ldflags="${6:-}"
  echo "building ${desc} (GOOS=${goos} GOARCH=${goarch})..."
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build ${ldflags:+-ldflags "$ldflags"} -o "$out" "$pkg"
  echo "  -> $out"
}

# Fleet target — unchanged name/location, this is the one
# deploy_antiloop.yml and run-antiloop.sh actually know about. Also
# copied into dist/ alongside every other arch, for consistency —
# dist/ ends up holding one binary per target, fleet build included,
# even though the fleet build's real deployable copy is the one in
# deploy/bin/.
build linux   amd64 "$fleet_out"                                "antiloop-${tag} (waloop fleet)"              ./cmd/antiloop "-X main.Version=${tag}"
cp "$fleet_out" "$dist_dir/antiloop-${tag}-linux-amd64"

# Extra antiloop targets — local dist/ only, never touched by Ansible.
build linux   arm64 "$dist_dir/antiloop-${tag}-linux-arm64"     "antiloop-${tag} (future ARM Linux hosts)"    ./cmd/antiloop "-X main.Version=${tag}"
build darwin  arm64 "$dist_dir/antiloop-${tag}-darwin-arm64"    "antiloop-${tag} (Mac, Apple Silicon)"        ./cmd/antiloop "-X main.Version=${tag}"
build darwin  amd64 "$dist_dir/antiloop-${tag}-darwin-amd64"    "antiloop-${tag} (Mac, Intel)"                ./cmd/antiloop "-X main.Version=${tag}"

# wis2nodes — operator-run diagnostic tool against the fleet's Redis,
# run from your own machine, never deployed to a waloop host (see
# cmd/wis2nodes's own package doc comment). All 4 platforms, dist/
# only, no deploy/bin/ copy — nothing here should ever land on the
# fleet. No -ldflags: cmd/wis2nodes/main.go has no main.Version to
# target, unlike antiloop, so there's nothing to embed.
build linux   amd64 "$dist_dir/wis2nodes-${tag}-linux-amd64"    "wis2nodes-${tag} (Linux, x86_64)"             ./cmd/wis2nodes
build linux   arm64 "$dist_dir/wis2nodes-${tag}-linux-arm64"    "wis2nodes-${tag} (Linux, ARM64)"              ./cmd/wis2nodes
build darwin  arm64 "$dist_dir/wis2nodes-${tag}-darwin-arm64"   "wis2nodes-${tag} (Mac, Apple Silicon)"        ./cmd/wis2nodes
build darwin  amd64 "$dist_dir/wis2nodes-${tag}-darwin-amd64"   "wis2nodes-${tag} (Mac, Intel)"                ./cmd/wis2nodes

echo
echo "done."
echo
echo "this does NOT make the fleet build current — that's a deliberate separate step"
echo "(build, test, THEN promote), not automatic on every build:"
echo "  echo 'ANTILOOP_VERSION=${tag}' > $deploy_dir/shared/antiloop.version"
echo
echo "once promoted, only wis2nodes you actually redeploy pick it up — everything"
echo "already running keeps its own version until you redeploy it, e.g.:"
echo "  cd $deploy_dir && ./add_wis2node.sh <name>"
