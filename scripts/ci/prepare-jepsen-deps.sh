#!/usr/bin/env bash
# Run from jepsen/ha so every matrix leg gets the same project dependencies.
set -euo pipefail

staging=$(mktemp -d)
trap 'rm -rf -- "$staging"' EXIT
export LEIN_HOME="$staging/.lein"
mkdir -p "$LEIN_HOME"
bundle="$RUNNER_TEMP/jepsen-ha-deps.tar"
if [[ -f "$bundle" ]]; then
  restored="$staging/restored"
  mkdir -p "$restored/.lein"
  printf '{:user {:local-repo "%s/.m2/repository"}}\n' "$restored" > "$restored/.lein/profiles.clj"
  # The launcher can bootstrap even with -o if its standalone jar is absent.
  if tar -xf "$bundle" -C "$restored" lein .lein/self-installs .m2/repository &&
     [[ -s "$restored/.lein/self-installs/leiningen-2.12.0-standalone.jar" ]] &&
     LEIN_HOME="$restored/.lein" "$restored/lein" -o deps; then
    exit 0
  fi
  echo "Discarding incomplete cached Jepsen bundle and resolving from a clean repository" >&2
  rm -- "$bundle"
fi

# Show which registry responded even if the resolver never logs its first transfer.
probe() {
  curl --fail --silent --show-error --location --connect-timeout 5 --max-time 15 \
    --retry 2 --retry-all-errors --retry-max-time 45 -o /dev/null \
    --write-out '%{url_effective} HTTP %{http_code} in %{time_total}s\n' "$1"
}
pom=org/clojure/clojure/1.11.1/clojure-1.11.1.pom
central=https://repo.maven.apache.org/maven2
if ! probe "$central/$pom"; then
  central=https://repo1.maven.org/maven2
  probe "$central/$pom"
fi
probe https://repo.clojars.org/jepsen/jepsen/0.3.7/jepsen-0.3.7.pom
cat > "$LEIN_HOME/profiles.clj" <<EOF
{:user {:local-repo "$staging/.m2/repository"
        :mirrors {"central" {:name "central" :url "$central/" :snapshots false}}}}
EOF

curl --fail --location --show-error --retry 3 --retry-all-errors \
  --connect-timeout 15 --max-time 60 --retry-max-time 180 \
  https://raw.githubusercontent.com/technomancy/leiningen/2.12.0/bin/lein \
  -o "$staging/lein"
chmod +x "$staging/lein"
retry="$GITHUB_WORKSPACE/scripts/ci/retry-lein-deps.sh"
bash "$retry" --bootstrap "$staging/lein"
bash "$retry" "$staging/lein"
"$staging/lein" -o deps
tar -cf "$bundle" -C "$staging" lein .lein/self-installs .m2/repository
