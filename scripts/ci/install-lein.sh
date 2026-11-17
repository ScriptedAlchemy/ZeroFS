#!/usr/bin/env bash
# Download the Leiningen launcher with bounded transport, then bootstrap its
# dependencies. Pass --root when the job runs the launcher as root.
set -euo pipefail

curl_command=(curl)
bootstrap=(lein)
if [[ "${1:-}" == "--root" ]]; then
  curl_command=(sudo curl)
  bootstrap=(sudo env HOME=/root LEIN_ROOT=1 JAVA_HOME="$JAVA_HOME" /usr/local/bin/lein)
fi

"${curl_command[@]}" --fail --location --show-error --retry 3 --retry-all-errors \
  --connect-timeout 15 --max-time 60 --retry-max-time 180 \
  https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
  -o /usr/local/bin/lein
sudo chmod +x /usr/local/bin/lein
bash "$GITHUB_WORKSPACE/scripts/ci/retry-lein-deps.sh" --bootstrap "${bootstrap[@]}"
