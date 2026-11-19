# Raw-device storage increment

The target gives ZeroFS two whole HDDs for payload and one whole P3 for the journal, catalog, authority, key envelopes, scratch and clean cache. The original Storage data disk stays in its existing TrueNAS pool. The boot disk and Storage disk are outside the ZeroFS device set.

This draft implements an engine storage library and a qualification executable in `zerofs/raw-device`. The executable admits only bounded Linux loop devices attached to private, independently identified tmpfs inodes. Its media I/O is positioned directly against the exclusively opened block node. The filesystem on the test controller holds the synthetic media image. There is no filesystem layer on the positive test devices. The three-protocol package currently uses the older directory backend.

## Device and acknowledgement contract

The fixture verifies the kernel's backing device and inode against a separately opened descriptor, the whole block-node identity, capacity and mount/holder state. It retains the backing inode lock and the block claim during I/O. Reopen also requires matching pool UUID, logical member UUID, region role and geometry. Physical commissioning needs a trusted host inventory binding the allowed WWN or serial to this claim. The probe refuses every hardware disk class before opening the device.

Explicit initialization scans the entire claimed range for zero bytes. Initialization of a P3 checks the whole device before writing its first role. Every P3 open validates all six region bindings. An interrupted multi-region initialization requires offline inspection if any role is incomplete. Ordinary startup only opens existing labels.

Each log record has a checksummed header binding sequence, key, content digest and provisioning nonce. PUT can expose staged bytes to its process. FLUSH synchronizes the payload and writes and synchronizes two checkpoint copies before acknowledging. Reopen reconstructs the committed index and verifies content on GET. Loss of a single checkpoint copy after acknowledgement retains the new object. These guarantees depend on the device honoring its flush contract.

The raw landing journal admits an immutable object for a named destination member and acknowledges after its own FLUSH. The drain verifies the payload, commits it to that member and verifies the destination before durably retiring the journal record. Admission reserves one retirement block for each outstanding item. Duplicate staging and drain retries use the member identity and content hash. A failed destination write leaves the pending journal object available after reopen.

## Integration matrix

| Role or boundary | Implemented and exercised in this draft | Required before physical application acceptance |
| --- | --- | --- |
| Two HDD payload devices | Separate whole-device logs, durable PUT, GET with SHA-256, member-specific drain, exact-capacity refusal, and independent reopen with the P3 absent | Connect the pool's logical segment locator, atomic conditional create, bounded range reads, reserve, evacuation, allocator health and idempotent **physical** reclaim. Exercise placement and concurrent drain on separate physical drives. |
| P3 journal | Dedicated extent, persisted destination plus payload, replay enumeration, verified drain, retained pending data on missing/full destination and bounded completion space | Bridge the production writeback sequence, file visibility and fsync/FUA fences. Reclaim retired space safely and preserve on-disk admission across interruption. A single P3 is the only durable copy until HDD drain unless replication is separately implemented. |
| P3 catalog | Isolated checksummed raw region, independent capacity and verified sentinel across reopen | Back the real redb catalog transactions, including logical length, crash-safe growth, recovery and per-segment locator updates. |
| P3 authority | Distinct raw region and verified sentinel | Connect the SlateDB authority object store and generation fencing. Integrate the recovery owner's replica contract and demonstrate safe degraded startup. |
| P3 key envelopes | Distinct raw region and verified synthetic sentinel | Route the real encrypted key envelope, rotation and recovery set through the authority/survival transaction. Prove unlock and restore after a single device loss. |
| P3 scratch | Dedicated bounded region and cross-region isolation | Replace directory-based temporary operations, bound reservations, and prove abandoned allocations can be reclaimed after restart. |
| P3 clean cache | Dedicated bounded region and cross-region isolation at journal exhaustion | Adapt the cache device interface and prove misses, eviction, reset and bounded RAM/disk use. |
| Loss and identity | Live raw object read returns EIO after media loss. Cold open refuses lost geometry, absent nodes, wrong pool/member/role and altered P3 geometry. A surviving member serves its committed hashes. | Resolve WWN/serial from a held physical descriptor. Fence boot, legacy Storage, mounts across namespaces, swap, partitions and device holders at host admission. Qualify the filesystem's file-level errors, reader protection, redundancy and restart behavior. |
| Serving and performance | Linux virtual block I/O and process-level crash cuts | Connect the raw profile to `zerofs run`, package admission and SMB/NFS/private NVMe/TCP. Measure authenticated durable traffic, raw synchronous P3 rate, SSD endurance, per-HDD drain, RSS and backpressure. |

The append-only allocator currently consumes space for overwrites and deletes. An empty pending queue can coexist with an exhausted region. Production use also needs a bounded recovery index and streaming access for large objects. A process kill against tmpfs-based virtual media exercises engine ordering. It supplies no hardware power-cut or raw P3 throughput measurement.

## Reuse decisions

The placement layer already keys assignments by logical member ID. Its current filesystem free-space probe cannot report raw allocatable extents. The catalog already uses redb, whose `StorageBackend` supports bounded reads, writes, logical resizing and sync. A region adapter must implement zero-on-growth and persist the logical length before it can back the catalog. The existing key-management code stores a wrapped data key through the object-store API. The clean cache uses the Foyer device interface. These are integration points for retaining the existing transaction, encryption and eviction logic. The package's SPDK target serves a private client block namespace; its userspace target and AIO bdev do not supply a host-device object catalog.

## Reproduction

Run `scripts/test-raw-device.sh` on Studio Docker Desktop. It builds the locked standalone crate, runs the unit tests, then starts a separate network-isolated container with loop control and access to block major 7. Owned mappings use Linux autoclear and their backing inode is checked at teardown. The only ext4 volume is a disposable negative control for signature and mounted-device refusal. [Measured results and cleanup](evidence/raw-device-engine/README.md)

The upstream SPDK Blobstore at the package's SPDK revision is a separate raw-block allocator with blob IDs, asynchronous I/O channels and metadata synchronization. Its metadata operations use the SPDK thread that loaded the store. Reusing it for payload reclamation remains an option. That integration would need a Rust-to-SPDK service boundary or FFI, exclusive claim transfer, a durable object-key-to-blob index, data-barrier verification and measured memory/CPU admission. This draft uses the positioned-I/O log for the bounded device proof. [Blobstore design](https://github.com/spdk/spdk/blob/0bbb7fe4/doc/blob.md) · [Blobstore API](https://github.com/spdk/spdk/blob/0bbb7fe4/include/spdk/blob.h)
