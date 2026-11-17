# Vendored crates

`foyer-storage/` is foyer-storage 0.22.3 exactly as published on crates.io
(upstream commit 94d8fbcab9039b4b391600d5efdbadf615c48a4c, recorded in
`.cargo_vcs_info.json`), wired in through `[patch.crates-io]` in
`../Cargo.toml`. Changes made here are listed below, each with the reason.
To compare with the published crate, diff against the copy cargo unpacks
under `~/.cargo/registry/src/*/foyer-storage-0.22.3`.

Drop the patch once a foyer release carries the same changes.
