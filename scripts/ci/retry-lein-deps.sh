#!/usr/bin/env bash
# Resolve external Jepsen dependencies before exercising the durability assertions.
# Retrying only dependency setup must never retry or mask a failed test.
set -euo pipefail

if (( $# == 0 )); then
  echo 'usage: retry-lein-deps.sh <lein command...>' >&2
  exit 2
fi

for attempt in 1 2 3; do
  if "$@" deps; then
    exit 0
  fi
  if (( attempt == 3 )); then
    echo 'Leiningen dependency resolution failed after 3 attempts' >&2
    exit 1
  fi
  sleep "$((attempt * 5))"
done
