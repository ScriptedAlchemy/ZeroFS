#!/usr/bin/env bash
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/../qemu-vm.sh"

test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
mkdir -p "$test_dir/host/new fixtures" "$test_dir/guest"
fixture="$test_dir/host/new fixtures/ci-minio-image.tar"
printf 'pinned fixture\n' > "$fixture"
printf 'pending guest state\n' > "$test_dir/guest/journal"

# Exercise handoff without requiring KVM or changing the host SSH identity.
require_ready_vm() { :; }
ssh_guest() { printf '%s\n' "$1" >> "$test_dir/commands"; }
rsync_to_guest() {
    [[ "$1" == "$fixture" && "$2" == "$fixture" ]]
    cp -- "$1" "$test_dir/guest/ci-minio-image.tar"
}

push_guest_file "$fixture"
cmp "$fixture" "$test_dir/guest/ci-minio-image.tar"
[[ "$(cat "$test_dir/guest/journal")" == 'pending guest state' ]]
[[ "$(wc -l < "$test_dir/commands")" -eq 1 ]]

for invalid in "$test_dir/missing.tar" relative.tar /; do
    if (push_guest_file "$invalid") 2>/dev/null; then
        echo "unexpected success for invalid fixture: $invalid" >&2
        exit 1
    fi
done
[[ "$(wc -l < "$test_dir/commands")" -eq 1 ]]
echo 'QEMU fixture handoff checks passed'
