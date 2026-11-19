#!/usr/bin/env bash
# Build one ZeroFS commit into the owned target volume inside a short-lived
# native linux/arm64 container on the Mac Studio's Docker Desktop.
# usage: build-one.sh <label> <git-rev> [patch-file]
set -euo pipefail
ID=zsd-a24001
HOST=/Volumes/bigssd/projects/ZeroFS-targets/$ID
WORKTREE=/Users/zackjackson/.t3/worktrees/ZeroFS/perf-sustained-journal--fbd3e751398982de
label=${1:?label}; rev=${2:?rev}; patch=${3-}
locked=--locked
[[ -n $patch || -n ${NOLOCK-} ]] && locked=
# Stream the exact tree into the source volume, replacing any previous copy.
git -C "$WORKTREE" archive --format=tar --prefix="$label/" "$rev" |
  docker run --rm -i --name "$ID-untar-$label" --network none \
    -v "$ID-src:/src" rust:1.100.0-bookworm \
    sh -c "rm -rf /src/$label && tar -C /src -xf -"
patch_mount=()
if [[ -n $patch ]]; then
  patch_mount=(-v "$patch:/patch.diff:ro")
fi
FEATURES=${FEATURES:-webui}
docker run --rm --name "$ID-build-$label" --network bridge \
  --cpus 6 --memory 10g \
  -v "$ID-src:/src" -v "$ID-target:/target" -v "$ID-cargo:/cargo" \
  -v "$HOST/bin:/out" ${patch_mount[@]+"${patch_mount[@]}"} \
  -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target \
  -e CARGO_PROFILE_CI_STRIP=false -e CARGO_PROFILE_CI_DEBUG=line-tables-only \
  zsd-a24001-rust:local sh -ec "
    cd /src/$label
    if [ -f /patch.diff ]; then patch -p1 -s < /patch.diff; fi
    mkdir -p webui/dist
    echo '<!doctype html><title>stub</title>' > webui/dist/index.html
    # Fresh mtimes so cargo cannot mistake this tree for a previous build.
    find . -type f -exec touch {} +
    cd zerofs
    cargo build $locked -j6 --profile ci --features ${FEATURES:-webui} --bin zerofs
    cp /target/ci/zerofs /out/$label
    sha256sum /out/$label
  "
