# Follow-up: anchor authority, member, and cache I/O

This work follows the startup preflight in PR 76. It is not implemented by that
preflight. Its purpose is to keep object and cache operations inside the
directory that was opened when readiness succeeded, even if another process
later replaces a path component.

## Required changes

- Move authority and member object access to descriptor-relative, no-follow
  traversal. Preserve object-key encoding, conditional-write locks, create-only
  publication, atomic replacement, and directory durability.
- Keep multipart staging creation, completion, abort, cancellation, and drop
  cleanup relative to the captured directory descriptors. A failed readiness
  recheck must not run a path-based destructor against a replacement tree.
- Give cache directory and partition creation, resizing, reopening, and pruning
  the same anchored contract. A replacement symlink must not redirect an open,
  `set_len`, or partition deletion.
- Move the scoped object listing walk to anchored handles. Preserve its lazy
  execution, prefix bounds, symlink refusals, and streaming behavior while
  preventing concurrent parent replacement from redirecting enumeration.
- Preserve startup identity and mounted-filesystem checks. Anchoring an inode
  does not by itself prove continued mount identity or device durability.

## Acceptance evidence

For each affected operation, pause after acquiring the original directory,
rename a parent aside, and replace it with a symlink to a separate temporary
tree. The operation must either fail before publication or finish exclusively
inside the retained original tree. Verify the external tree's file contents,
sizes, and directory entries are unchanged. Cover authority and member reads,
writes, copies, renames, deletes, and all listing variants; multipart completion,
abort, dropped uploads, and cancelled parts; and cache directory creation,
partition open, resize, and prune deletion.

Run conditional-write and crash-durability regressions as well as these path
tests. Measure cold metadata enumeration on a populated HDD before claiming an
improvement to HDD startup or listing latency. Keep the remaining runtime
loss-of-mount and durability-receipt contracts explicit.
