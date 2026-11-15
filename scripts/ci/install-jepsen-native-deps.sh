#!/usr/bin/env bash
# Retry package transport only; never repeat the Jepsen durability assertions.
set -euo pipefail

apt_options=(
  -o Acquire::Retries=3
  -o Acquire::http::Timeout=30
  -o Acquire::https::Timeout=30
)

# By default apt-get update may report success after a failed index download.
# Do not install against stale indexes after the bounded transport retries fail.
sudo env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" \
  -o APT::Update::Error-Mode=any update

# The guest only renders files: avoid gnuplot's Qt/desktop dependency tree,
# while retaining the fonts needed by Jepsen's performance graphs.
sudo env DEBIAN_FRONTEND=noninteractive apt-get "${apt_options[@]}" \
  install -y --no-install-recommends \
  curl gnuplot-nox fonts-liberation openjdk-17-jdk-headless
