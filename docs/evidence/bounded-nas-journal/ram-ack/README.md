# Bounded RAM acknowledgement on the two-member NAS journal

Performance source `7821e763c69437c6785b5c67f677fa166a1c167d` includes upstream through `b0cdb6172183cd724e1cec7d186aaeba1fa97c5e`. The optimized Linux CI executable SHA-256 is `93488a4e5a160de9fd0e170568a8dae05beddfc7ab64b615894e8a482d75bebb`. Both comparison modes used this executable, the same 128 MiB object writeback RAM, 512 MiB journal, reserve, and round-robin local destinations. The opt-in filesystem overlay had a 256 MiB byte cap and 2,048-operation cap. Admission charges an additional 512 MiB for its two ownership stages, bringing the engine requirement to 6,341,787,648 B.

## Foreground and durable rates

The short burst used six independent 9P2000.L file descriptors, six distinct pre-generated 36 MiB inputs, 1 MiB negotiated messages and one outstanding write per client. Timing spans first send through last actual Rwrite. The separate-client run restricted the engine and helper to two physical CPU cores, 8 GiB and zero swap; clients ran on six other cores. The client hashed each file through a second connection before fsync. Three matched trials yielded the following medians.

| 216 MiB, external clients | Materialized WRITE | Volatile WRITE |
| --- | ---: | ---: |
| Through last Rwrite | 337.63 MB/s | **1,463.50 MB/s** |
| Through last fsync, including cross-client verification | 138.91 MB/s | 147.00 MB/s |
| Through observed destination settlement | 121.28 MB/s | 120.11 MB/s |

The volatile burst delivered 11.71 Gb/s of logical payload over local Unix sockets in approximately 155 ms at the median. The measured NAS network ceiling was previously 9.41 Gb/s toward the NAS. Network, FUSE, SMB signing, NFS stability and hardware costs enter the physical package measurement. [All external trials and independent destination hashes](external/README.md)

With clients inside the same two-CPU quota and immediate fsync following a writer barrier, median volatile and materialized WRITE rates were 1,273 and 325 MB/s. Through last fsync they were 226 and 234 MB/s. One 96 MiB file reached 957 and 356 MB/s. On a 960 MiB six-file transfer, volatile and materialized WRITE measured 357 and 261 MB/s; the volatile dirty gauge reached 268,429,312 B against a 268,435,456 B cap. Message size affected this result: a single matched 216 MiB pair at 256 KiB negotiated messages reached 277 and 304 MB/s. All reported trials reached journal-free independent verification after SIGKILL. [Compressed JSON for all 23 trials](comparisons.json.gz) · [Reproduction](run-native.sh)

## Destination rate and backpressure

For 4.5 GiB of distinct input, matched runs measured **107.61 MB/s volatile** and **140.45 MB/s materialized** in the middle 25–75% of the destination byte counter. Each member received over 2.4 GB in both modes and all file hashes matched in a new read-only process. Each destination was a separate directory on the same ext4 filesystem. These rates measure the synthetic end-to-end engine path and its own encoding/metadata work. The private inputs were counted in the aggregate cgroup. [Sustained counters, member allocations, failed initial verifier and cleanup](sustained/README.md)

With both destinations below reserve, all six fsynced files survived process SIGKILL and cache removal. They later survived a second independent verification after journal removal. At the full-journal boundary, the same pending fsync resumed 284 ms after the first member regained capacity, with the other member still held. Peak observed SSD reservation was 503,489,563 B. Pressure-run aggregate memory peaked at 2,403,086,336 B. These fixtures held both clients and tmpfs ballast in the 8 GiB, two-CPU, zero-swap cgroup. [Compressed replay, pressure and allocation receipts](recovery.json.gz)

The authenticated private NVMe/TCP target fsyncs ordinary block writes as well as FUA writes. On this executable the FUA command took 12.26 ms, an ordinary WRITE 15.45 ms and the separate FLUSH 0.243 ms. Both crash cases recovered their distinct full backing-file hashes, reauthenticated to the original namespace, verified guard sectors and passed journal-free reads after drain. [Authenticated initiator receipts](../../../../scripts/nas-journal-nvme/ram-ack/final/README.md)

## What changed

The first candidate admitted the overlay but still prepared each write behind the preceding write's canonical inode lock. Its 216 MiB pair at 1 MiB negotiated messages reached 320 MB/s volatile and 325 MB/s materialized. The first build attempt of the admission change failed on a private configuration field; the tested candidate used the normalized settings. Its executable hash was `f87f22822760dea612c03ea4b6b54ca276d1e748d83bfcc420419d7d879fa0fb`. [Original authenticated crash cases](../../../../scripts/nas-journal-nvme/ram-ack/README.md)

Consecutive writes at EOF can now prepare against the previous published inode state while an outstanding batch still holds the same canonical lock. A weak reference expires as soon as the last batch releases the lock. The materializer applies each batch in order. A noncontiguous write, expired lock or 256-write chain limit goes back through canonical acquisition. Quota reservation, ordered extent publication, client read visibility and fsync barriers remain in the path. This optimization covers the single-inode file-write path used by the NAS companions. Striping and native NBD batch preparation retain their existing ordering.

## Physical trial scope

Direct-to-HDD remains selectable. Journal writes on the consumer P3 count toward its endurance; track actual NVMe host writes and temperature alongside dataset usage and HDD drain on the physical trial. The older direct package measured 66.7/68.8 MB/s signed SMB/NFS including flush, and the P3 measured 198.9 MB/s in one QD1 fsynced direct-file test. The physical hosts, protocols, filesystem and app release differ from this Linux comparison. A single journal device is the durability location for acknowledged flushes until HDD drain. [Existing journal geometry and physical allocation guidance](../merged/README.md) · [Restart-only settings and admin contract](../../../write-ack-settings.md)
