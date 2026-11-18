# NAS write acknowledgement settings

On the Storage page, **Active** describes the running engine. An administrator can save a desired filesystem write policy for the next engine start. Use the supervisor’s orderly restart after saving. A process restart following a crash also reads the last durably saved document.

| Mode | Ordinary write response | Client durability barrier |
| --- | --- | --- |
| Materialized | The filesystem has applied the write. The bounded object writeback tier can still have journal and destination work pending. | With the bounded journal enabled, fsync waits for the local journal. |
| Volatile memory | The filesystem has admitted the bytes to bounded RAM and made them visible to other readers. Unflushed bytes can be lost on process or power failure. | fsync waits for the covered writes to materialize and reach the local journal. The packaged NVMe/TCP target also fsyncs its writes, including FUA, and forwards explicit FLUSH. |

HDD drain continues asynchronously. A full journal or unavailable destination can delay durability through backpressure. For the bounded journal profile the editor accepts 16–256 whole MiB and 1–2048 operations. Startup memory admission charges two ownership stages. Throughput qualification uses 256 MiB and 2048 operations.

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
