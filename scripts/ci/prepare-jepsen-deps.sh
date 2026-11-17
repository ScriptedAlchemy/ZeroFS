#!/usr/bin/env bash
# Run from jepsen/ha so every matrix leg gets the same project dependencies.
set -euo pipefail

staging=$(mktemp -d)
trap 'rm -rf -- "$staging"' EXIT
export LEIN_HOME="$staging/.lein"
mkdir -p "$LEIN_HOME"
printf '{:user {:local-repo "%s/.m2/repository"}}\n' "$staging" > "$LEIN_HOME/profiles.clj"

curl --fail --location --show-error --retry 3 --retry-all-errors \
  --connect-timeout 15 --max-time 60 --retry-max-time 180 \
  https://raw.githubusercontent.com/technomancy/leiningen/2.12.0/bin/lein \
  -o "$staging/lein"
chmod +x "$staging/lein"
retry="$GITHUB_WORKSPACE/scripts/ci/retry-lein-deps.sh"
bash "$retry" --bootstrap "$staging/lein"
bash "$retry" "$staging/lein"
"$staging/lein" -o deps
tar -cf "$RUNNER_TEMP/jepsen-ha-deps.tar" -C "$staging" lein .lein/self-installs .m2/repository
