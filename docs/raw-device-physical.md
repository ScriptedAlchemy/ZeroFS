# Raw-device candidate for the N5

The engine selects `raw-device-p3-nas-fit-v1` through `[runtime].profile`. Its physical manifest and its fully allocated loop manifest use the same journal, allocator, memory, and durability policy. The loop fixture has smaller payload devices and its own identity checks. The physical TOML in the source tree is a parser test with disposable UUIDs and a test password. [Physical manifest](../zerofs/src/cli/memory_raw_physical_test.toml) · [Loop manifest](../zerofs/src/cli/memory_raw_nas_fit_fixture.toml)

| Designation | Exact device bytes | Serial | Additional identity |
| --- | ---: | --- | --- |
| `nvme` | 1,000,204,886,016 | 2318E6D18CE7 | Crucial CT1000P3SSD8, NVMe namespace 1 |
| `hdd6` | 6,001,175,126,016 | WF203KQ8 | ATA by-id and WWN 5000c500ac8aed0a |
| `hdd18` | 18,000,207,937,536 | 3WH7H3BJ | ATA by-id and WWN 5000cca284d17ed1 |

The engine requires host PID namespace visibility, readable original host sysfs, mount and swap information, trusted udev records, by-id links, libblkid and write access for its exclusive block open. It rejects a partition, a device with partitions, holders, slaves, active mounts or swap, conflicting identities, a protected alias, or a recognized foreign signature. It checks device number, node identity, disk sequence, sector size and capacity across lookup and exclusive open. An uncooperative privileged process with ordinary raw write access can still damage the media. The legacy Storage disk ZL213G03 and the boot device are protected. App commissioning requires its own authorization and a compatible host-claim arrangement. The parent's 19 November preflight reported three unused whole devices and an SSH account without block read permission.

## Provisioning and reopen interfaces

These are engine interface specifications. Physical initialization writes media and belongs to the commissioning owner.

| Command | Contract |
| --- | --- |
| `zerofs-raw-device physical check-claim DESIGNATION` | Temporarily acquires the exclusive claim, issues the identity and signature checks and reports the held byte length. Performs no media writes. Exit releases the descriptor. |
| `zerofs-raw-device physical init-blank DESIGNATION POOL_UUID MEMBER_UUID payload` | Explicitly provisions the respective HDD after a full zero-content scan. |
| `zerofs-raw-device physical p3-v2-init-blank nvme POOL_UUID MEMBER_UUID p3 LAYOUT_BYTES` | Requires six exact comma-separated byte lengths. Checks the entire P3 for zero before its first label write. Each role also checks its own range. |
| `zerofs pool init -c CONFIG` | Initializes the namespace on explicitly provisioned devices. Existing journal, catalog, authority, key, scratch or cache contents on the P3 block initialization. An interrupted partial setup requires inspection. |
| `zerofs pool verify-raw -c CONFIG` | Stopped-engine check. Acquires the claims and checks the original catalog, authority bucket, key envelope and journal identity. Emits schema version 1 JSON, including verification booleans, member status, local/remote sequence and the typed `memory.included_companion_bytes`. |
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
| Object writeback RAM / journal redb cache | 8 MiB / 64 MiB |
| Aggregate cgroup / included app companions | 8 GiB, zero swap / 1,610,612,736 B |

Scratch and the large clean-cache extent are currently unused. The engine runs the clean read caches in RAM. It caps outstanding journal operations and raw-index key counts independently of device capacity. The larger geometry keeps a dedicated free reserve under the redb resize ceiling and accounts for transaction growth. Pacing begins at 95% of the logical dirty budget and resumes below the configured 85% watermark. The HDDs require a physical reserve of at least 1% of their respective byte capacities. Round-robin placement distributes sealed segments among eligible devices.

Ordinary WRITE can complete in volatile memory. Explicit FLUSH waits for local durability, which can leave the only recoverable copy on the P3 until HDD drain. Sustained throughput after the dirty window fills depends on destination progress. The 512 MiB journal proposal has been superseded. Physical link speed, actual HDD/SSD throughput and power-loss persistence require separate measurements.

Payload and authority logs append. Overwrites and deletion do not reclaim their old physical records. A 65,536-live-key index limit can stop allocation well before reaching a multi-terabyte device's nominal capacity. Catalog capacity, degraded cold startup and recovery after P3 loss remain release gates. A tested working set or an exclusive claim does not establish full-disk usability.
