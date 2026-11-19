# Mounted raw-member fixture

The Studio runs used the parent-supplied v3 working-snapshot binaries with
integration reference `056d4158`. Each log records both executable SHA-256
digests. The fixture uses the production ZeroFS server and FUSE client with
strict consistency and write-through. Both raw members are whole, positively
identified loop devices backed by owned tmpfs inodes.

- [Attempt 1](studio-v3-attempt-1.log) initialized the 32 MiB and 96 MiB loops.
  Startup refused the 2 GiB cgroup because legacy admission required
  43,050,336,256 bytes. This is the bounded Studio result.
- [Attempts 2](studio-v3-attempt-2.log) and [3](studio-v3-attempt-3.log) ran
  under the default Docker VM envelope. Attempt 2 confirmed that the actual
  kernel mount type was `fuse`. Attempt 3 verified the initial mounted hash,
  then showed that the pool identity marker also funds a tombstone.
- [Attempt 4](studio-v3-attempt-4.log) used the same default envelope. The
  first restart verified the initial 8 MiB hash and the catalog assigned one
  segment to each member. The smaller member reached 3,768,320 allocatable
  bytes with its 16 MiB reserve and 16,384-byte retirement reservation.
  Two further mounted fsyncs succeeded and the resulting 28 MiB file matched
  its expected SHA-256. The run was stopped at operator direction before its
  post-spill catalog and final cold-restart checks. Docker removed the owned
  container. The runner uses the original 2 GiB limit again.

There is no completed acceptance receipt.
