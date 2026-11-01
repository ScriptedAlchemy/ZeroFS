#!/usr/bin/env bash
# xfstests compares GNU diagnostics and copies ls under a different basename.
# Ubuntu's uutils defaults do not have those semantics. Keep the selection
# local to xfstests, whose harness puts its own directory first in PATH.
set -euo pipefail

suite=$(realpath "${1:?usage: xfstests-gnu-tools.sh XFSTESTS_DIR [BIN_DIR]}")
bin_dir=$(realpath "${2:-/usr/bin}")
[[ -f "$suite/common/preamble" ]] || {
    echo "Not an xfstests checkout: $suite" >&2
    exit 1
}

for tool in ln ls mkdir mknod od touch; do
    candidate="$bin_dir/gnu$tool"
    if [[ ! -x "$candidate" ]]; then
        candidate="$bin_dir/$tool"
    fi
    version=$(LC_ALL=C "$candidate" --version) || exit 1
    if [[ "$version" != *"GNU coreutils"* ]]; then
        echo "Required GNU coreutils executable is missing: $tool" >&2
        exit 1
    fi
    if [[ -e "$suite/$tool" || -L "$suite/$tool" ]]; then
        [[ "$suite/$tool" -ef "$candidate" ]] || {
            echo "Refusing to replace existing xfstests path: $suite/$tool" >&2
            exit 1
        }
    else
        ln -s -- "$candidate" "$suite/$tool"
    fi
done
