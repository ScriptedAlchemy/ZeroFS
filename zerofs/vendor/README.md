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
- `src/engine/block/reclaimer.rs`, `recover.rs`, `engine.rs`:
  For the Linux directory device used by ZeroFS, `BlockCleaner::clean` punches the whole block (`fallocate` with
  `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`) instead of zeroing only its
  first page, and recovery cleans each reusable directory-backed block. Other partition types keep their original cleanup behavior. foyer
  writes a reused block again from offset zero, so before this the earlier
  generation's entries, padding and indices stayed past the new blob chain,
  where no index describes which bytes are padding. With recovery disabled
  every block came back without even its first page zeroed. Where punching
  fails, for example on a filesystem without hole support or off Linux, the
  cleaner falls back to the upstream first-page write. A reader holding an
  address in a punched block reads zeros, which fails the entry magic check
  and is a miss, as when the block is written again.
