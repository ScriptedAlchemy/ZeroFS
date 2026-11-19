# Isolated raw claims on Linux

The engine CLI ran as 20001:20001, with supplementary groups cleared, in a private PID, mount, network and cgroup namespace in a disposable x86-64 KVM guest. The guest used Linux 7.0.0-28-generic, two virtual CPUs, 10 GiB of guest RAM, an initramfs root, and three emulated ATA/NVMe disks. It had no network device or host disk passthrough. The broker ran as root in the guest's initial namespaces. Its socket directory was bound read-only into the engine, which had its own procfs, a cgroup view, two character devices (null and urandom), a root-owned mode-0640 config and a private mode-0700 state directory. The engine had zero effective capabilities, host UID/GID mappings and no-new-privileges. The parent of the state directory and binaries was root-owned. The engine could not see the guest host-root canary through either its own root or PID 1.

| Component | Tested revision or SHA-256 |
| --- | --- |
| Engine source | `4050a4180d3b66260ec42674e82ff9e4983c9781` |
| Broker source, unchanged at engine pin | `b531787837ae033568d3b205a62bc7d714a57f41` |
| Harness, including identical engine and broker sources | `b2271f010a1ca0b27cb2e705d53bfd0c5da74195` |
| Linux x86-64 engine, CI profile, WebUI enabled | `9f9f083616b30f9c978f06f06b4e6ddf7a6e2c2e1ab47efef196b182bf8410ca` |
| Linux x86-64 broker, release profile | `b635606f8042dac4b9083be09bc1b6e933076dd688a9ff254486de2f6c95e917` |
| Builder image | `sha256:9bcde2ca73816af4827f8f9e64de4ad38566cced6c8a3550d122bad9ace39845` |

The successful run invoked `scripts/raw-device/broker-vm.py` with `--release 7.0.0-28-generic`, the extracted unsigned Ubuntu kernel, both native executables and `--manifest zerofs/src/cli/memory_raw_physical_test.toml`. The runner verified that all three newly created qcow2 images mapped entirely to unallocated zeros, injected synthetic empty-label vectors at the real role offsets and booted using disposable snapshots. The stock emulator's fixed NVMe Identify model was changed in a temporary copy. Every qcow2 base digest matched after shutdown. The guest did not scan 25 TB or exercise physical provisioning. [Runner](../../../scripts/raw-device/broker-vm.py) · [Manifest](../../../zerofs/src/cli/memory_raw_physical_test.toml) · [CLI contract](../../raw-device-physical.md)

## Outcomes

- Device-free `memory-status` succeeded without the socket environment variable. Ordinary run before logical initialization refused the missing original journal. Explicit `pool init` then succeeded. Stopped `pool verify-raw` returned affirmative authority, catalog, key envelope and journal fields, journal allocation data, and `memory.included_companion_bytes = 1610612736`.
- The broker denied a process with the socket group but the wrong UID. It also denied the authorized UID with only a private mount namespace or only a private PID namespace. The engine refused a missing transport, premature disconnect, missing descriptor, character device, wrong disk sequence, unprivileged replacement listener, mismatched member UUID and a competing claim while running.
- After SIGKILL of the broker, the running engine uploaded 12,582,912 incompressible bytes over its private 9P socket and completed explicit flush. A stopped reopen while the broker remained dead failed with connection refused. Broker restart replaced socket inode 89 with 94. Stopped verification and engine restart both succeeded. The downloaded file matched the source byte-for-byte and by SHA-256 (`68c31a27a353e77c311d7892fbfe1a718dca21d52175b65103543d33b25f2315`).
- The isolated engine, client, and driver shared an 8,589,934,592 B cgroup with zero swap. Its peak was 216,526,848 B. All recorded memory event counters and peak swap usage were zero. The host-side broker, QEMU and full app companions were outside that figure. This guest measures a 12 MiB functional workload. The larger FUSE benchmark and app package have separate source pins and receipts.
- The raw crate passed 64 tests, followed by the one normally ignored libblkid signature regression in a separate invocation. Linux Rust formatting checks passed for the engine workspace and raw crate. The CI-profile engine built offline. The full engine suite was not rerun in this increment.

[Exact guest output, emulator and kernel hashes](guest.json) · [Cleanup](cleanup.json)

## Failures retained

Attempts 1 and 2 stopped at guest scaffolding (untrusted initramfs directory permissions and the BusyBox unshare applet). Attempts 3–5 stopped at the wrong-UID control. Tracing first exposed the private state directory refusal and then a connection reset on writing a request to a peer that had already rejected it. The final control reads the broker's credential refusal directly without sending an unauthorized request.

Attempt 6 reached the running service and uncovered a real write failure. One 12 MiB write generated a sealed mutation requiring 12,655,878 B against an 8,388,608 B object-RAM budget and the upload reported EIO on sync. The raw profiles now stage at most 2 MiB of plaintext extent edits per batch within the same file transaction. This allows the 4 MiB segment rotation target to take effect inside a larger write. Admission and durability settings remain as measured in the manifest. Attempt 7 passed that same upload and flush, then a client tried to use an old socket pathname before the restarted server accepted connections. The final harness waits for a live pool-status RPC before download.

The broker trusts host root and the dedicated engine identity. It checks effective UID/GID and namespace identity. Clearing supplementary groups and dropping capabilities are package launch responsibilities. This run does not establish the minimum capability set for a containerized host broker, N5 device permissions or power-failure behavior. Payload and authority log reuse, P3-loss recovery, degraded cold start and physical commissioning remain open. [Large held/draining benchmark and failed final gate](../large-raw/README.md)
