# PR86 current audit and regression evidence

Reviewed remote head `81d56a6cab5a8b5bc2013daf32497c452e61ecb0`, including its
merge of integration head `daad582926801f5a107a75141d4ab7325d343fc7`.
The fresh validation below covers production source
`3c9564a7bb8b39311f13b3918e49da22f20c39f5`, which includes corrections
`4db69eb` and `3c9564a`. Subsequent changes to this receipt do not change the
validated Rust source.

## Corrections

1. **Verify empty generations conditionally.** A zero-length physical HEAD
   now uses the committed ETag and version, just like the nonempty range
   reads. Replacing an empty object can no longer pass verification against
   the old catalog generation.
2. **Count a member absent at cold start in every automatic evacuation
   fence.** The scheduler, status report, upload-start transaction, and
   publication transaction include the startup exclusion. An absent member
   that is also quarantined counts once. Zero-byte objects and pending
   uploads still count as ownership; a fully retired historical quarantine
   does not. This does not persist a synthetic health quarantine.
3. **Keep legacy authority deletion owned through native completion.** The
   authority is local storage too. Its cleanup task retains the logical key
   lease and catalog settlement after the startup wait times out, as local
   data-member cleanup already does.
4. **Preserve target refusals through writeback reconciliation.** The
   durable segment target is checked before a pending local object can
   return `AlreadyExists`. Multipart completion does not reinterpret a
   permanent target refusal as success because remote bytes happen to match.
5. **Keep a pinned segment's admission refusal from terminating recovery.**
   A target mismatch while reserving a migration is an admission refusal.
   Other movable segments can still evacuate; the refused segment remains
   on its exact target.
6. **Make pending generated-create retries idempotent at their original
   acknowledgement tier.** A retry can reuse the original accepted sequence
   and ETag only for a conditional Create with matching verified length and
   SHA-256, after checking the exact target. It waits for that original
   sequence's configured SSD or remote barrier. The retry adds no journal
   sequence and releases its unused admission reservations. Authoritative
   remote reads continue to bypass the overlay when resolving remote
   publication conflicts.

The durable target registration intentionally survives admission failure,
cancellation, replay, and physical retirement. Replay does not carry the
original extension, so the immutable key must retain its member contract.
This PR still provides internal placement scaffolding: filesystem writers do
not yet emit whole-file targets, and whole-file placement is not enabled.

## Fresh validation

**All 25 selected current-source regression entrypoints passed.** The
focused executable compiled successfully in 189.6 seconds and ran the
selectors in 22.4 seconds. Formatting and `git diff --check` passed. An
independent review of the pending-create correction found no remaining
correctness blocker.

The selected executable copies the exact current production modules and
extracts the checked-in regression functions. It exposes existing test
constructors and pause hooks explicitly, omits the CLI entrypoint, and
preserves the required CLI error helper and constants. It does not replace
the pool, writeback, catalog, local object store, or filesystem algorithms.
Every source hash in the preparation manifest matched the validated commit
after the code was committed. `validation.json` records individual results,
selected source hashes, test-body hashes, and harness transformations.

Coverage includes:

- Empty and nonempty generation replacement; authenticated sealed-body
  corruption; verification of the same committed generation.
- Legacy and member garbage cleanup after a timeout; owned native upload
  and staging cleanup; legacy segment migration.
- Target persistence through capacity refusal, reopen, and cold writeback
  replay; quarantine and in-flight multipart conflicts; single-put and
  multipart wrong-target refusal; changed-digest refusal; same-target retry;
  preservation of the original remote acknowledgement barrier.
- Absent-plus-quarantined failure accounting; zero-byte ownership; retired
  historical quarantine; a second failure at reservation and publication;
  pinned-segment admission refusal while other recovery continues.
- Filesystem cold start with one member unavailable, reading the survivor,
  and resolving affected file paths.

The initial successful build against `4db69eb` passed 22 of 24 selectors.
Both failures were same-target retries of journal-pending generated creates:
the existing `AlreadyExists` path tried authoritative remote readback before
the accepted object existed remotely. Those unchanged assertions pass after
`3c9564a`; the additional remote-barrier regression also passes.

<!-- baseline-controls:start -->
A separate executable restoring the historical generation, degraded-fence,
legacy-cleanup, and migration-admission defects was prepared for four further
negative controls. Its compilation reached the 180-second cap without a
compiler diagnostic and was stopped. Those controls did not execute and are
not counted as successful regression proofs. The two failing-before and
passing-after pending-create assertions above are the completed behavioral
regression proof from this run.
<!-- baseline-controls:end -->

Representative commands for the normal repository test runner are:

```sh
cargo test --locked --no-default-features -p zerofs --lib member_verification_rejects_replaced_empty_and_nonempty_generations
cargo test --locked --no-default-features -p zerofs --lib degraded_member_counts_once_in_automatic_evacuation_fences
cargo test --locked --no-default-features -p zerofs --lib legacy_native_garbage_reconciles_after_startup_wait_times_out
cargo test --locked --no-default-features -p zerofs --lib writeback_target_retry_does_not_reconcile_pending_bytes_on_another_member
cargo test --locked --no-default-features -p zerofs --lib writeback_multipart_target_retry_does_not_reconcile_pending_bytes_on_another_member
cargo test --locked --no-default-features -p zerofs --lib writeback_segment_retry_waits_for_original_remote_ack
cargo test --locked --no-default-features -p zerofs --lib bound_segment_refusal_does_not_stop_other_automatic_evacuation
```

## Scope and limits

These fresh checks used the isolated cloud Linux x86_64 executor and native
Rust 1.100.0, with a single compiler process and no emulation. They are
focused source validation, not a complete Cargo test or Clippy pass, a Mac
Studio Docker qualification, a mounted-client or physical-disk acceptance
run, or a NAS deployment. No production storage was accessed.

The first focused compilation encountered incompatible cached SlateDB/Foyer
artifacts. Rebuilding those dependencies consistently resolved the environment
failure before the passing builds above. No result from that failed compile
is counted as a passing check.

Current-head hosted CI and combined integration checks remain separate
verification gates. Earlier qualification and audit receipts document their
own historical source revisions and do not qualify this changed source.
