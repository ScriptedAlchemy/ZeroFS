# Raw-device candidate for the N5

The engine selects `raw-device-p3-nas-fit-v1` through `[runtime].profile`. Its physical manifest and its fully allocated loop manifest use the same journal, allocator, memory, and durability policy. The loop fixture has smaller payload devices and its own identity checks. The physical TOML in the source tree is a parser test with disposable UUIDs and a test password. [Physical manifest](../zerofs/src/cli/memory_raw_physical_test.toml) · [Loop manifest](../zerofs/src/cli/memory_raw_nas_fit_fixture.toml)

| Designation | Exact device bytes | Serial | Additional identity |
| --- | ---: | --- | --- |
| `nvme` | 1,000,204,886,016 | 2318E6D18CE7 | Crucial CT1000P3SSD8, NVMe namespace 1 |
| `hdd6` | 6,001,175,126,016 | WF203KQ8 | ATA by-id and WWN 5000c500ac8aed0a |
| `hdd18` | 18,000,207,937,536 | 3WH7H3BJ | ATA by-id and WWN 5000cca284d17ed1 |

The trusted local claim process requires initial host PID namespace visibility, readable original host sysfs, mount and swap information, trusted udev records, by-id links, libblkid and write access for its exclusive block open. It rejects a partition, a device with partitions, holders, slaves, active mounts or swap, conflicting identities, a protected alias, or a recognized foreign signature. It checks device number, node identity, disk sequence, sector size and capacity across lookup and exclusive open. An uncooperative privileged process with ordinary raw write access can still damage the media. The legacy Storage disk ZL213G03 and the boot device are protected. Physical commissioning belongs to the app owner. The parent's 19 November preflight reported three unused whole devices and an SSH account without block read permission.

## Isolated engine and claim service

The package interface takes one absolute pathname. The trusted local process runs
`zerofs-raw-device physical serve-claims SOCKET 20001 20001`. Supply a canonical,
dedicated root-owned directory whose ancestors exclude group and other writes.
The broker owns its socket as root:20001 with mode 0660. Bind the directory
read-only into the isolated engine so restarting the broker makes its replacement
listener visible there. The broker uses only a local Unix seqpacket listener. Its
trusted namespace must support the host inventory checks above, reading the peer's
namespace metadata in procfs, and Linux `SO_PEERPIDFD`.

Run the engine as 20001:20001 with no supplementary groups, its own PID and mount
namespaces, the host user namespace, and private procfs. Give it its finite cgroup,
group-readable config, and its own writable runtime and socket directories. Set
`ZEROFS_RAW_CLAIM_SOCKET` to the socket path **inside that namespace** for
`pool init`, stopped `pool verify-raw`, and `run`. The engine needs no host-root
bind mount, host PID access, device node, udev database, or host sysfs inventory.
`memory-status` reads the configuration and cgroup without opening a device.

Each request names one of the three fixed designations and the configured pool and
member UUIDs. The broker authenticates the connecting process and checks its namespaces
and the original labels on the exclusively held whole disk. It transfers that open
file description with `SCM_RIGHTS`. The engine authenticates the broker and checks
the received descriptor's block type, access, device number, disk sequence and exact
geometry before opening its role. Missing transport, a partial grant or a failed
binding check aborts open. Once a grant completes, the engine owns its duplicate
until the last reference closes. A broker outage prevents new claims and preserves
existing exclusive opens. Device provisioning is available only as a separate trusted
local command. Logical initialization still requires the explicit `pool init` command.

## Provisioning and reopen interfaces

These are engine interface specifications. Physical initialization writes media and belongs to the commissioning owner.

| Command | Contract |
| --- | --- |
| `zerofs-raw-device physical check-claim DESIGNATION` | Temporarily acquires the exclusive claim, issues the identity and signature checks and reports the held byte length. Performs no media writes. Exit releases the descriptor. |
| `zerofs-raw-device physical init-blank DESIGNATION POOL_UUID MEMBER_UUID payload` | Explicitly provisions the respective HDD after a full zero-content scan. |
| `zerofs-raw-device physical p3-v2-init-blank nvme POOL_UUID MEMBER_UUID p3 LAYOUT_BYTES` | Requires six exact comma-separated byte lengths. Checks the entire P3 for zero before its first label write. Each role also checks its own range. |
| `zerofs pool init -c CONFIG` | Initializes the namespace on explicitly provisioned devices. Existing journal, catalog, authority, key, scratch or cache contents on the P3 block initialization. An interrupted partial setup requires inspection. |
| `zerofs pool verify-raw -c CONFIG` | Stopped-engine check. Acquires the claims and checks the original catalog, authority bucket, key envelope and journal identity. Emits schema version 1 JSON, including verification booleans, member status, local/remote sequence and the typed `memory.included_companion_bytes`. The `journal_allocation` object reports redb length, allocated pages, stored key/value bytes, metadata, fragmentation and pending payload charges. Opening a transactional database can run normal recovery and retirement of an already-drained prefix. |
| `zerofs run -c CONFIG` | Independently claims and checks the devices. An empty original journal, missing label, mismatched pool/member/role or missing required member prevents ordinary startup. |

Whole-media zero verification on successful provisioning reads at least 6,001,175,126,016 B and 18,000,207,937,536 B on the two HDDs, plus two passes over the 1,000,204,886,016 B P3. QUICK wipes do not establish zero content throughout those ranges. A missing label does not authorize initialization. The earlier extended WD SMART check was waived for the disposable trial.

## Geometry and finite admission

| Resource | Candidate budget |
| --- | ---: |
| P3 journal extent | 60 GiB |
| Logical dirty SSD budget | 10 GiB |
| Physical free reserve within journal region | 4 GiB |
| P3 placement catalog / namespace authority / key envelopes | 128 MiB / 3 GiB / 16 MiB |
| P3 scratch / remaining clean-cache extent | 32 MiB / 932,374,601,728 B on the physical P3 |
| Filesystem volatile memory / operations | 16 MiB / 128 |
| Object writeback RAM / journal redb cache | 16 MiB / 64 MiB |
| Aggregate cgroup / included app companions | 8 GiB, zero swap / 1,610,612,736 B |

The total memory admission charge is 8,480,882,688 bytes (8,088 MiB), including companions. The 16 MiB writeback RAM budget admits a complete seal containing a maximum negotiated 9P write after a partially filled segment. Earlier qualification receipts used an 8 MiB RAM budget and smaller writes.

Scratch and the large clean-cache extent are currently unused. The engine runs the clean read caches in RAM. It caps outstanding journal operations and budgets raw indexes by the bound geometry. The two 4 KiB length checkpoints and 4 GiB free reserve leave 60,129,533,952 B under the redb resize ceiling. Startup limits the logical dirty budget to one quarter of that ceiling for allocator growth, copy-on-write and pinned pages. On the 8 GiB held fixture at `143fb0c8`, redb reported 2,332,338 allocated 4 KiB pages (9,553,256,448 B) and a file length of 10,737,586,176 B. The measured length includes space for reuse. Freshly provisioned regions checkpoint the highest extent ever exposed. On growth the engine can skip redundant zero writes beyond that extent. Regrowth clears any previously exposed bytes. Older, ambiguous or incomplete checkpoint histories fall back to zeroing the full growth range. Pacing begins at 95% of the logical dirty budget and resumes below the configured 85% watermark. The HDDs require a physical reserve of at least 1% of their respective byte capacities. Round-robin placement distributes sealed segments among eligible devices.

Ordinary WRITE can complete in volatile memory. Explicit FLUSH waits for local durability, which can leave the only recoverable copy on the P3 until HDD drain. Sustained throughput after the dirty window fills depends on destination progress. The 512 MiB journal proposal has been superseded. Physical link speed, actual HDD/SSD throughput and power-loss persistence require separate measurements.

Payload and authority stores reuse deleted and superseded allocations after FLUSH and any outstanding readers. Live-key density can reach the memory budget before payload space is exhausted. [Allocator and durability contract](raw-device-engine.md#format-v2-allocation-and-commit) Catalog capacity, degraded cold startup and recovery after P3 loss remain release gates. Physical qualification still needs actual-device exclusivity, sustained drain, power-loss and restart evidence. [Emulated claim tests](evidence/raw-physical-claim-vm/README.md) · [Large loop fixture](evidence/large-raw/README.md)

## Per-file drain observation

After fsync and remote drain, query the running engine once for each test file.

```sh
zerofs pool file-placement --endpoint unix:/run/zerofs-app/engine/rpc.sock /copy.bin
```

The reply identifies the resolved inode and file size. Each member receives an extent count and logical byte count clipped to EOF. The engine reads that inode's indexed extents and size from one metadata snapshot, then looks up their segment owners in one catalog read transaction. The report retains one counter per distinct segment, up to 65,536 segments, and returns one counter pair per member. It reads placement metadata without reading file payloads.

Use a quiescent file for acceptance. Sparse or as-yet unindexed data contributes to `unindexed_bytes`. Indexed bytes without a committed member identity contribute to `unattributed`. For a fully drained, dense test file, both values should be zero and the member bytes should sum to the verified file size. A concurrent rename can change which path names the reported inode after lookup. Member counts describe the recorded placement. Cold readback verifies the payload, and host device counters measure physical traffic including metadata and padding.
