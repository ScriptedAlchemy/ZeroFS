# Raw-device storage increment

The target gives ZeroFS two whole HDDs for payload and one whole P3 for the journal, catalog, authority, key envelopes, scratch and clean cache. The original Storage data disk stays in its existing TrueNAS pool. The boot disk and Storage disk are outside the ZeroFS device set.

The engine pool uses the object log and region adapters in `zerofs/raw-device`. The small Linux fixture uses bounded loops on private tmpfs inodes. The larger fixture uses independently claimed, fully allocated ext4 backing files. ZeroFS issues positioned I/O on the loop block devices in both cases. The candidate also implements a separate physical claim for three fixed designations. The package previously qualified on the small raw fixture used an adapter and a 1.5 GiB companion allowance. [Physical claim and larger geometry](raw-device-physical.md)

## Device and acknowledgement contract

The fixture verifies the kernel's backing device and inode against a separately opened descriptor, the whole block-node identity, capacity and mount/holder state. It retains the backing inode lock and the block claim during I/O. Reopen also requires matching pool UUID, logical member UUID, region role and geometry. The physical claim resolves the designated by-id alias through the initial Linux host namespace, checks kernel and udev identity and topology, and compares the held descriptor before and after inventory inspection. Its fixture and hardware entry points have distinct arguments.

Explicit initialization scans the entire claimed range for zero bytes. Initialization of a P3 checks the whole device before writing its first role. Every P3 open validates all six region bindings. An interrupted multi-region initialization requires offline inspection if any role is incomplete. Ordinary startup only opens existing labels.

Each log record has a checksummed header binding sequence, key, content digest and provisioning nonce. PUT can expose staged bytes to its process. FLUSH synchronizes the payload and writes and synchronizes two checkpoint copies before acknowledging. Reopen reconstructs the committed index and verifies content on GET. Loss of a single checkpoint copy after acknowledgement retains the new object. These guarantees depend on the device honoring its flush contract.

The standalone v1 landing probe admits an immutable object for a named destination member and acknowledges after its own FLUSH. The drain verifies the payload, commits it to that member and verifies the destination before durably retiring the journal record. Admission reserves one retirement block for each outstanding item. Duplicate staging and drain retries use the member identity and content hash. A failed destination write leaves the pending journal object available after reopen.

## Engine integration

| Role | Current engine implementation | Remaining work for physical use |
| --- | --- | --- |
| HDD payload | Pool locators bind the pool and member UUIDs. Raw segment PUT enforces conditional creation and updates, checks exact physical cost plus future deletion debt, and flushes before returning. A range read verifies the entire object in bounded chunks. Mounted FUSE evidence covers one file spanning two members and continued allocation when the smaller member reaches reserve. | Reuse space from deleted or overwritten records, qualify evacuation at hardware scale, and measure independent physical HDD drains. |
| P3 journal | The v2 journal region backs the production writeback redb transactions. The same durable commit publishes the payload chunks, mutation records and local progress. Remote retirement releases allocator pages. An explicit resize ceiling fences the physical reserve. | Qualify full-capacity operation, long-lived readers, target-device power loss and endurance. Until drain, the P3 holds the only durable copy of an acknowledged write. |
| P3 catalog | A separate redb region holds placement, reservations and locators. Ordinary startup requires an existing catalog identity. | Prove restoration or replica recovery after P3 loss. |
| P3 metadata and keys | The authority log stores the SlateDB objects. The key-envelope path routes to its own log on the same P3. Both retain an exclusive device claim. | Reclaim log space, exercise rotation and restore a usable namespace after P3 loss. |
| P3 scratch and clean cache | Both regions have fixed boundaries. The candidate leaves them unused and runs the clean cache in RAM. The fixture uses private temporary directories for sockets, control files and the mount. | Persistent hot-read retention and scratch allocation/reclamation on raw media. |
| Loss and identity | Mounted reads returned EIO on the affected file after live media loss. The unaffected file remained readable. Offline migration moved a live segment and verified its destination hash. Cold startup requires both HDDs by default. Operators can opt into startup with one confirmed absent HDD. | Hardware qualification of the physical claim and its host exclusions. |
| Clients and admission | The original small synthetic profile admits one exact geometry in a dedicated 8–16 GiB Linux cgroup with zero swap, the package WebUI feature and a provisional 1 GiB companion allowance. | Combined acceptance on the larger candidate with the actual package, transfer rates, and hardware FLUSH/FUA on the raw path. |

The original small synthetic fixture uses a 96 MiB journal, 32 MiB catalog, 88 MiB metadata, 8 MiB keys, 16 MiB scratch and 16 MiB clean-cache layout for this fixture. A v1 region and a v2 transactional region have different labels. Opening an incompatible region fails. All-device blank verification applies to initialization of either format. A quick wipe does not establish that the device passes that check.

The fixture manifest selects 16 MiB and 128 operations of volatile filesystem writes, 16 MiB of writeback RAM, and a 16 MiB dirty journal budget. A segment rotates after a complete write batch: a maximum negotiated 9P write can add almost 10 MiB to an open segment holding almost 4 MiB. The writeback RAM budget covers that complete segment and its framing so it can reach the journal and satisfy fsync. The admission calculation charges two volatile ownership stages, open and retained segments, clean-cache floors, raw index and replay memory, and companion headroom. It resolves to 7,876,902,912 bytes (7,512 MiB). The 1 GiB companion allowance is provisional. [Exact manifest](../zerofs/src/cli/memory_raw_fixture.toml) · [Raw HDD mounted evidence](evidence/raw-mounted-members/README.md)

The historical mounted receipts used the earlier 8 MiB writeback RAM budget and smaller fixture writes. They do not cover a maximum-size negotiated 9P write; the current budget corrects that gap without changing the historical measurements.

## Reproduction

Run `scripts/test-raw-device.sh` on Studio Docker Desktop. It builds the locked standalone crate, runs the unit tests, then starts a separate network-isolated container with loop control and access to block major 7. Owned mappings use Linux autoclear and their backing inode is checked at teardown. The only ext4 volume is a disposable negative control for signature and mounted-device refusal. [Measured results and cleanup](evidence/raw-device-engine/README.md)

Run `scripts/test-raw-device-mounted.sh --degraded-start BIN_DIR` to exercise
cold startup with one absent member. Supply the Linux `zerofs` and
`zerofs-raw-device` executables in `BIN_DIR`. The fixture uses a 44 GiB
cgroup limit and checks the default refusal, byte-exact survivor reads, the
affected paths and their I/O errors, and all three files after the missing
member returns.

The upstream SPDK Blobstore at the package's SPDK revision is a separate raw-block allocator with blob IDs, asynchronous I/O channels and metadata synchronization. Its metadata operations use the SPDK thread that loaded the store. Reusing it for payload reclamation remains an option. That integration would need a Rust-to-SPDK service boundary or FFI, exclusive claim transfer, a durable object-key-to-blob index, data-barrier verification and measured memory/CPU admission. This draft uses the positioned-I/O log for the bounded device proof. [Blobstore design](https://github.com/spdk/spdk/blob/0bbb7fe4/doc/blob.md) · [Blobstore API](https://github.com/spdk/spdk/blob/0bbb7fe4/include/spdk/blob.h)

