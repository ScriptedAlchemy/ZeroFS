# Storage write acknowledgement editor qualification

The Linux engine and the embedded UI were built from source at `560eaae7d54e21f5b3c3171ea17e4b086e9c51b9`. All 547 Git blobs under `zerofs`, `webui` and `scripts/fixtures/nas-journal` matched the Linux build tree. The SHA-256 of the GitHub codeload archive at `/ScriptedAlchemy/ZeroFS/tar.gz/560eaae7d54e21f5b3c3171ea17e4b086e9c51b9` was `7d99b4ddf57c4248cfd16f25bef4301543484d82a3b219925eafc3e43e2065d2`.

| Build input or output | SHA-256 |
| --- | --- |
| Linux executable, optimized CI profile with webui | `ad0db0a57404413584474001c2f6826fc13cd2d4fe8885f58521732764bf1287` |
| Frontend dist archive, 126 files | `06fce2dd60a72fa0e0ec2193c7347a58b8602805a90757ff7a1cb94380356c91` |
| Source-built browser client WASM | `f75d3f0e430c55c86e01379d93b813cac43ccaca1acf9ce7b344cf708976109f` |
| Linux test container image | `5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b` |

The frontend passed 31 Node tests, 76 Vitest tests and the normal production build with a fresh Linux WASM client. Linux Rust 1.92.0 passed `cargo fmt --all -- --check`. Both Rust builds used `--locked --profile ci --no-default-features --features webui`. The focused test binary passed 155 tests across memory admission, filesystem mutation and append ordering, 9P writes, and write-ack settings and routes. [Test output](linux-tests.txt)

The process fixture used the checked-in journal config template with isolated file members, private state, a synthetic token, private 9P and RPC sockets, and loopback WebUI. The engine and HTTP test client shared two CPUs, 8 GiB, and zero swap. The following steps completed in the same isolated container.

1. Initialized the local pool, read the baseline with `memory-status`, and started `zerofs run`. GET reported materialized as active and desired. The served index, main bundle and Storage chunk matched the production dist bytes.
2. Submitted the volatile policy without a credential. The engine returned HTTP 401, and the desired settings file remained absent.
3. Saved the same policy with a bearer credential. HTTP 200 reported materialized active, volatile desired and a pending restart. Reading the same config and override using a fresh `memory-status` process charged another 536,870,912 bytes.
4. Terminated the engine with SIGTERM, waited for exit zero and started a new engine. GET reported volatile memory active and desired, 256 MiB, 2,048 operations and no pending restart. The second engine also exited zero on SIGTERM. The cgroup peaked at 113,274,880 bytes during this control test.

[HTTP results, served hashes, CLI admission and cgroup](runtime.json) · [Supported settings and activation](../../../../write-ack-settings.md) · [Burst, sustained drain and crash measurements on their measured engine revision](../README.md)

At 2026-11-18T23:57:52Z the private build and fixture root `ubuntu-main:/var/tmp/zerofs-ram-ack.6idAFVNq` was verified absent after removal. The owned test and smoke containers were removed. The existing buildbuddy container remained running. Sorted kernel module names had matching before/after SHA-256 `6c5c8a66d01225bf5fc11a4ec291ba3f9e40744f1c6d362a69414c21e5fd2ff1`. Shared builder records were preserved. The catalog owner received the tested source and the exclusive slot for the supervised package check.
