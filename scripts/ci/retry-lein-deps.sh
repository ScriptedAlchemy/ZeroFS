#!/usr/bin/env bash
# Resolve external Jepsen dependencies before exercising the durability assertions.
# Retrying only dependency setup must never retry or mask a failed test.
set -euo pipefail

operation=deps
limit=180
if [[ ${1:-} == --bootstrap ]]; then
  operation=version
  limit=120
  shift
fi

if (( $# == 0 )); then
  echo 'usage: retry-lein-deps.sh [--bootstrap] <lein command...>' >&2
  exit 2
fi

limit=${LEIN_SETUP_TIMEOUT_SECONDS:-$limit}
if [[ ! $limit =~ ^[1-9][0-9]*$ ]]; then
  echo 'LEIN_SETUP_TIMEOUT_SECONDS must be a positive integer' >&2
  exit 2
fi

for attempt in 1 2 3; do
  # A stalled network request must not consume the entire HA job deadline.
  # Only version (standalone-JAR bootstrap) and deps are retried here; callers
  # continue to execute test/run exactly once outside this setup helper.
  if timeout --kill-after=10s "${limit}s" "$@" "$operation"; then
    exit 0
  else
    status=$?
  fi
  echo "Leiningen $operation attempt $attempt failed (exit $status)" >&2
  if (( attempt == 3 )); then
    echo "Leiningen $operation failed after 3 attempts" >&2
    exit 1
  fi
  sleep "$((attempt * 5))"
done
