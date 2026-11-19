# NAS write acknowledgement settings

On the Storage page, **Active** describes the running engine. An administrator can save a desired filesystem write policy for the next engine start. Use the supervisor’s orderly restart after saving. A process restart following a crash also reads the last durably saved document.

| Mode | Ordinary write response | Client durability barrier |
| --- | --- | --- |
| Materialized | The filesystem has applied the write. The bounded object writeback tier can still have journal and destination work pending. | With the bounded journal enabled, fsync waits for the local journal. |
| Volatile memory | The filesystem has admitted the bytes to bounded RAM and made them visible to other readers. Unflushed bytes can be lost on process or power failure. | fsync waits for the covered writes to materialize and reach the local journal. The packaged NVMe/TCP target also fsyncs its writes, including FUA, and forwards explicit FLUSH. |

HDD drain continues asynchronously. A full journal or unavailable destination can delay durability through backpressure. For the bounded journal profile the editor accepts 16–256 whole MiB and 1–2048 operations. Startup memory admission charges two ownership stages. Throughput qualification uses 256 MiB and 2048 operations.

## What the acknowledgement modes can accelerate

`Materialized` is the default filesystem policy. It means that the filesystem
transaction has been applied; it does not by itself mean that every ordinary
write has reached the NVMe journal. The Storage editor selects this policy or
`Volatile memory`. It does not currently expose a third filesystem policy that
acknowledges each write only after local SSD durability.

The filesystem RAM queue and the object writeback queue are separate. A volatile
filesystem write must pass admission, reserve both byte and operation capacity,
and prepare its ordered update before it can respond. Contiguous appends can
share a pending inode update, while conflicting overwrites may wait for earlier
materialization. When the RAM queue fills, new writes wait for materialization
to release capacity. They do not bypass it through a separate direct-to-NVMe
acknowledgement path. Sustained throughput therefore follows the drain rate once
the bounded burst capacity is exhausted.

With writeback enabled, the filesystem resolves client durability requests to
the local journal. Without it, they wait for the remote backend. The bounded NAS
profile uses object writeback memory acknowledgement, but that does not weaken
fsync, NFS stable writes or COMMIT, or block FLUSH/FUA. A fast transfer followed
by a long final flush means that durability work remains after the ordinary
write responses; measure both phases when comparing policies.

### Finite journal capacity

The `small-nas-journal-v1` profile fixes object writeback at 128 MiB of dirty
RAM and a 512 MiB dirty-journal budget, independently of the NVMe drive's
physical capacity. These are separate from the filesystem RAM limit in the
Storage editor. The profile's validation in [config.rs](../zerofs/src/config.rs)
requires that geometry; changing the filesystem RAM setting does not enlarge
the journal.

The local journal can absorb a burst while the HDD destination drains. Once
its capacity or free-space admission limit is reached, new work is paced by
destination progress. A larger sustained copy therefore measures both the
software path and the finite buffer's drain rate. Logical file size alone does
not predict the boundary because compression and journal overhead affect the
stored bytes. A larger journal needs a configuration with the corresponding
memory admission and capacity validation; the bounded profile does not silently
use all free space on the P3.

### Packaged NVMe/TCP target

The target is maintained in the separate TrueNAS app repository. The fixture's
[pinned AIO patch](https://github.com/ScriptedAlchemy/truenas-storage-union/blob/08d4f95067b0445e2193dd26937fc3801819b660/deploy/truenas-zerofs/nvme-tcp/spdk-zerofs-aio.patch)
calls `fsync` synchronously from the AIO completion loop for **every** successful
block write. This includes ordinary writes, because the patch enforces FUA by
syncing all writes. Enabling filesystem RAM acknowledgement cannot remove that
extra target barrier. Increasing queue depth also does not remove the serial
fsync calls from the completion loop.

The [pinned launcher](https://github.com/ScriptedAlchemy/truenas-storage-union/blob/08d4f95067b0445e2193dd26937fc3801819b660/deploy/truenas-zerofs/nvme-tcp/target.py)
starts one SPDK reactor without interrupt mode. SPDK's
[TCP target](https://spdk.io/doc/nvmf_interrupt_mode.html) and
[AIO backend at the pinned revision](https://github.com/spdk/spdk/blob/0bbb7fe4dccfb31e8d446da8822360704949d1e9/module/bdev/aio/bdev_aio.c)
support interrupt-driven completion. Adding `--interrupt-mode` to the app
launcher is a separate packaging change to qualify for lower idle CPU use; it
does not change write durability or eliminate synchronous flush latency.
Removing the per-write sync requires an explicit cache policy and a verified
FUA path before ordinary completions can safely acknowledge volatile data.

For a protocol comparison, retain the engine and app revisions, active
acknowledgement settings, effective CPU quota, target polling mode, I/O size,
queue depth, transfer time, final flush time, CPU time and throttling counters.
Compare the same workload with one variable changed. An idle polling reactor
can consume CPU budget shared with encryption, compression and filesystem work;
a larger quota can reduce that contention while leaving the per-write sync
bottleneck intact.

## Package contract

Select `[runtime] profile = "small-nas-journal-v1"` with its required local pool, bounded writeback geometry and dedicated memory limit. Pass `ZEROFS_WRITE_ACK_SETTINGS_FILE=/state/private/write-ack-settings.json` to both `zerofs memory-status -c ...` and `zerofs run -c ...`. Both load the same override before memory admission. A missing document selects the base TOML. Invalid or unreadable configured documents fail startup. Configure `ZEROFS_WRITE_ACK_ADMIN_TOKEN_FILE` separately to enable saves on a writable engine with WebUI.

Provision each parent as an absolute, engine-owned private directory outside data exports and storage roots. Mount the settings directory writable so the engine can atomically replace and sync a file there. The token and document must be distinct private regular files, owned by the engine user, with one hard link each. The token file contains 32 random bytes encoded as 64 hexadecimal characters with an optional final newline. The server loads the token at startup. A restart activates token rotation. Keep browser access on a loopback/SSH tunnel or trusted TLS origin.

The persistence format and PUT body are identical. A materialized policy uses the following document.

```json
{"version":1,"filesystem":{"write_ack_mode":"materialized"}}
```

A volatile policy carries the RAM budget in decimal GB. This example selects 256 MiB.

```json
{"version":1,"filesystem":{"write_ack_mode":"volatile_memory","volatile_memory_gb":0.268435456,"volatile_max_operations":2048}}
```

`GET /api/settings/write-ack` returns `active`, `desired`, `pending_restart`, `activation` (the literal `"restart"`) and `editable`. The first two values have the same shape as the `filesystem` object above. `editable` means the server has an administrator credential configured. The existing data WebUI can read the reported policy.

`PUT /api/settings/write-ack` requires `Authorization: Bearer <token>` and `Content-Type: application/json`. The server checks browser origin, rejects extra fields, resolves the proposed settings against the base config and applies finite memory admission before saving. Success returns the same status representation with the running values preserved. The editor sends the credential for that save and clears its input. The package owns token provisioning, access to the management origin, persistence mounts and restart.
