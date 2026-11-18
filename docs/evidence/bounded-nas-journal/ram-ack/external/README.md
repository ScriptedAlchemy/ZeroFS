# External 9P client comparison

Ubuntu host `ubuntu-main` ran three alternating materialized and volatile
trials of the same engine binary. Source commit
`7821e763c69437c6785b5c67f677fa166a1c167d` produced SHA-256
`93488a4e5a160de9fd0e170568a8dae05beddfc7ab64b615894e8a482d75bebb`.
Both modes used the small NAS journal profile. Volatile mode additionally
used the 256 MiB overlay and 2048-operation cap. [Runner](../run-external.sh)

The service ran on host CPUs 0–1 in a private cgroup with 8 GiB and zero
swap. The generator and six 9P writers ran on CPUs 2–7 in a separate private
cgroup with 2 GiB and zero swap. All eight CPUs mapped to distinct physical
cores. The containers shared a private Unix socket and coordination directory.
The service used two member directories on one local ext4 filesystem.

Each trial pre-generated six distinct 36 MiB payloads before timing. Six
independent 9P2000.L connections negotiated 1 MiB messages and sent one WRITE
at a time per connection. The write interval ends at the last WRITE response.
After all writes were acknowledged, a separate connection per file verified
its content hash. The original connections then issued concurrent fsyncs.
The first-WRITE-through-final-fsync interval includes the explicit hash phase.
The fsync phase starts when the first fsync is sent. Drain is the service's
first observed clean sample after the client completes and closes the files.

| Trial | Acknowledged WRITE MB/s | ACK to last hash check (s) | Fsync phase (s) | Through final fsync MB/s | Through observed drain MB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Materialized 1 | 301.66 | 0.793 | 0.087 | 138.91 | 121.28 |
| Volatile 1 | 1365.07 | 1.167 | 0.207 | 147.00 | 120.11 |
| Materialized 2 | 347.35 | 0.631 | 0.123 | 161.12 | 129.99 |
| Volatile 2 | 1463.50 | 1.243 | 0.334 | 130.79 | 118.83 |
| Materialized 3 | 337.63 | 0.696 | 0.514 | 120.41 | 106.72 |
| Volatile 3 | 1531.79 | 1.250 | 0.069 | 154.40 | 138.80 |

Median WRITE acknowledgement throughput increased from 337.63 to
1463.50 MB/s. Median throughput through the last fsync was 138.91 and
147.00 MB/s, respectively. Median observed drain throughput was 121.28 and
120.11 MB/s.

Each trial verified the full content of all files again through an independent
read-only engine after a post-drain SIGKILL. The verifier used a new cache
with the journal parked and writeback omitted. Both members gained committed
segments, and the check found zero journal payloads after drain. The service
and client cgroups peaked at up to 1,250,803,712 and 283,279,360 bytes,
respectively. Both reported zero swap usage and zero memory-event deltas.
The owned containers and scratch directory were removed and checked absent
after each trial.

- [Materialized trial 1](external-materialized-1/run/receipt.json)
- [Volatile trial 1](external-volatile-1/run/receipt.json)
- [Materialized trial 2](external-materialized-2/run/receipt.json)
- [Volatile trial 2](external-volatile-2/run/receipt.json)
- [Materialized trial 3](external-materialized-3/run/receipt.json)
- [Volatile trial 3](external-volatile-3/run/receipt.json)
