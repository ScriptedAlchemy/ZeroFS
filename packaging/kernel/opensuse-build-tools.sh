#!/usr/bin/env bash

# The 20261002 kernel was built before Tumbleweed advanced gcc16 and binutils.
# Retain exact, authenticated build tools instead of weakening the module ABI
# checks. These RPMs come from the official 20260924 history snapshot; their
# primary-metadata checksums were independently verified before pinning SHA-256.
install_opensuse_build_tools() {
    local auto_conf=$1
    local work_root=$2
    local directory
    local index
    local path
    local verification
    local -a paths=()
    local -a filenames=(
        binutils-2.45-4.3.x86_64.rpm
        cpp16-16.2.0+git9497-3.1.x86_64.rpm
        gcc16-16.2.0+git9497-3.1.x86_64.rpm
    )
    local -a digests=(
        ba87b86e4494dd39c249e13ccc53d6b2bf4ffc7d508590624b08c641511ac063
        40370cce3b36a76284a8bfa6dff63993da3660fade98f71690650f05279e55a2
        a935e32195519da0c603e907ffa1e62f36ed6ab25295cc0b0486d8b48251ce2f
    )

    # Other kernel/compiler identities keep their existing selection rules.
    [[ $(config_value "$auto_conf" CONFIG_CC_VERSION_TEXT) == \
       'gcc (SUSE Linux) 16.2.0' &&
       $(config_value "$auto_conf" CONFIG_GCC_VERSION) == 160200 &&
       $(config_value "$auto_conf" CONFIG_AS_VERSION) == 24500 &&
       $(config_value "$auto_conf" CONFIG_LD_VERSION) == 24500 ]] || return 0

    require_command curl
    require_command rpmkeys
    require_command sha256sum
    directory=$(mktemp -d "$work_root/opensuse-build-tools.XXXXXX") ||
        die "cannot create the openSUSE build-tools directory"
    for ((index = 0; index < ${#filenames[@]}; index++)); do
        path="$directory/${filenames[$index]}"
        curl --fail --show-error --location \
            "https://download.opensuse.org/history/20260924/tumbleweed/repo/oss/x86_64/${filenames[$index]}" \
            --output "$path" || die "cannot acquire exact openSUSE build tools"
        printf '%s  %s\n' "${digests[$index]}" "$path" |
            sha256sum --check --strict - ||
            die "openSUSE build-tools SHA-256 mismatch"
        # Trust only the official image's existing distribution keys. No key
        # advertised by a repository or RPM is imported here.
        verification=$(rpmkeys --checksig --verbose "$path") ||
            die "openSUSE build-tools signature verification failed"
        grep -Eq 'Signature, key ID [0-9a-f]+: OK[[:space:]]*$' \
            <<<"$verification" || die "openSUSE build tools are not signed"
        paths+=("$path")
    done
    zypper --non-interactive install --oldpackage "${paths[@]}" ||
        die "cannot install the exact openSUSE build tools"
    echo "openSUSE build tools: authenticated snapshot 20260924 (GCC 16.2.0, binutils 2.45)"
    # select_target_cc and self_contained/build.sh still enforce the compiler
    # banner, numeric compiler/assembler versions, and kernel configuration.
}
