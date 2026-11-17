# Vendored crates

`foyer-storage/` is foyer-storage 0.22.3 exactly as published on crates.io
(upstream commit 94d8fbcab9039b4b391600d5efdbadf615c48a4c, recorded in
`.cargo_vcs_info.json`), wired in through `[patch.crates-io]` in
`../Cargo.toml`. Changes made here are listed below, each with the reason.
To compare with the published crate, diff against the copy cargo unpacks
under `~/.cargo/registry/src/*/foyer-storage-0.22.3`.

Drop the patch once a foyer release carries the same changes.

## Changes to foyer-storage

- `src/engine/block/buffer.rs`: `Buffer::push` and `Buffer::push_slice` zero
  each entry's page padding, and `BlobIndex::seal` zeroes the index tail past
  the last record. foyer allocates its I/O buffers uninitialized and reuses
  them across flushes, and writes the padding and the whole 4 KiB index to
  the cache disk, so without this those bytes carried freed heap memory of the
  process or bytes of earlier entries. `test_written_bytes_are_defined`
  covers it; run it from a foyer checkout with
  `cargo test -p foyer-storage --features test_utils --lib engine::block`,
  because this directory is not a workspace member.
