# Authenticated NVMe/TCP crash fixture

The fixture runs a native ZeroFS candidate and the pinned TrueNAS SPDK target
inside one loopback-only Linux container. Both member paths are dedicated
256 MiB container tmpfs mounts. The metadata authority, placement catalog,
writeback journal, and read cache are separate paths under the container's
writable layer. Keep the journal, catalog and metadata through the engine
restart. Give each restart a new read cache directory and a new strict FUSE
mount. Each case has a separate pool and namespace.

Build only the `spdk-build` Dockerfile stage from app
`08d4f95067b0445e2193dd26937fc3801819b660` in an owned context. This pins SPDK
`0bbb7fe4dccfb31e8d446da8822360704949d1e9` and the app's inherited-FD AIO patch.
Start an owned container with `--network none --cap-add SYS_ADMIN`,
`--security-opt apparmor=unconfined`, `--device /dev/fuse`, and these mounts:

```
--tmpfs /fixture/member-a:rw,nodev,nosuid,noexec,size=256m
--tmpfs /fixture/member-b:rw,nodev,nosuid,noexec,size=256m
```

Copy the pinned app's `nvme-tcp` directory contents to `/fixture/app` and the
confirmed native candidate to `/fixture/zerofs`. Copy this directory's Python
scripts and patch to `/fixture`. Run the app's target unit suite, then apply
the fixture patch to the probe source and rebuild only that executable within
the container.

```sh
cd /fixture/app
python3 -B -m unittest -v test_target
cd /source/app/zerofs_nvme_probe
patch -p0 </fixture/initiator-cutoffs.patch
make -j2
```

Supply the materialized `small-nas-journal-v1` template as
`/fixture/nas-journal.toml.in`. It must use local Unix 9P, 128 MiB dirty RAM,
512 MiB SSD journal budget, 0.01 GB reserve on both members, and
`ignore_fsync = false`. The fixture fills both member tmpfs mounts to at most
1 MiB free after creating the zero namespace and starting the target. Run
`python3 -B /fixture/run.py` and retain its JSON lines. Inspect per-role logs
on failure, then remove the owned container, fixture scratch and image tag.
Do not prune builder caches.

The FUSE adapter retains the app launcher’s descriptor setup, file identity
and fsync checks, DH-HMAC-CHAP credentials, and SPDK startup RPCs. It supplies
a private FUSE mount identity check in place of the launcher's kernel 9P
mount check. The initiator verifies a FUA write to LBA 3 in one case and a
normal write to LBA 8 followed by a distinct FLUSH in the other. The target's
patched AIO path also fsyncs the normal write before its completion. The
FLUSH scenario checks recovery after a successful FLUSH under that contract.
The probe holds the connection open at each cutoff while the supervisor
SIGKILLs the engine. Before starting the recovered target, the fixture
verifies SHA-256 for the whole 16 MiB backing file. It then reauthenticates and
verifies the changed LBA, the other case's LBA, and their guards over NVMe/TCP.

The crash boundary is the ZeroFS engine process. Host storage and the
container remain running throughout the restart.

## Ubuntu qualification receipt

[timed.jsonl](timed.jsonl) records the exit-0 run against candidate
`bf1a9fb5319fbf3f01f94b8ea881d940767b3588`. Both members had exactly
1,048,576 bytes free and reported `below_free_space_reserve` at the
cutoffs and during first recovery. Both scenarios verified the whole backing
SHA-256 with the original writeback journal and an empty read cache, then
verified the sector contents over an authenticated connection to the same
namespace UUID. After releasing the ballast, both scenarios reached the
remote frontier with zero blobs, took a second SIGKILL, and verified the
whole backing SHA-256 through a read-only engine with writeback omitted and
another fresh read cache. Offline catalog status confirmed placed segments.

| Case | Initiator submit to completion callback |
| --- | ---: |
| FUA WRITE (LBA 3) | 9.695 ms |
| Normal WRITE (LBA 8) | 8.778 ms |
| Explicit FLUSH after that WRITE | 0.335 ms |

Each duration is one observed command on the synthetic fixture, measured
inside the initiator using SPDK ticks. The ACK-observed and SIGKILL-issued
timestamps use the supervisor's separate monotonic clock.

[first-cutoff-recovery.jsonl](first-cutoff-recovery.jsonl) preserves the
initial post-ACK observation failure. Its first half contains the original
cutoffs and empty observations from scanning the wrong path. Its second half
contains recovery using the same retained journals and namespace UUIDs after
reinstating the ballast. Both recovered and drained before that data was
removed. The completed runner uses the bucket-scoped blob path. An earlier
pre-ACK startup at 256 MiB failed to allocate the SPDK bdev I/O pool. The
completed run uses the app qualification's 512 MiB SPDK reserve. A timed
attempt also confirmed that a read-only verifier must omit writeback settings
and the bounded profile before starting.

## Provenance and cleanup

| Input or executable | ID or SHA-256 |
| --- | --- |
| TrueNAS app commit | `08d4f95067b0445e2193dd26937fc3801819b660` |
| SPDK commit | `0bbb7fe4dccfb31e8d446da8822360704949d1e9` |
| DPDK submodule | `37a1a9b926b4678141931f2f22e7a877b5b565c2` |
| ISA-L submodule | `7c3479e0a9dac17f448603ec1ad64c7c625f530c` |
| App SPDK AIO patch | `d42c3f910b03b7b86711e4d9b3dd674b3a4917cd829dee558522be7e7dca3b95` |
| App target launcher | `493ebda0a6614b85b00ebe240480b7afcabe2949fea832f7260e5b5e7aaebb1a` |
| App probe source | `89fe44fdbf6a152816a122662cc7b009548dcaf9bfbd29b43f6bdca6967f` |
| Debian base image | `sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2` |
| Built stage image ID | `sha256:7ed852f9291488d881228767c325781ffc5cc3c4c059aecb3b49c26a0bae79cd` |
| Candidate executable | `a4318f24cae7b6f807f68711efb9ac5003fe85625d5e3265bbb68899cb201b11` |
| Target executable | `e6e3c23ea94571f77ebda295adc3b05c327c0fd2be657a3c971e5a0982070b2c` |
| Timed initiator executable | `bf262a5940d08f8f074337a9c99e6088ff210a678acf4ee8dcadce466d5a15bb` |
| Supplied engine config template | `05f96e08ca01df1f81f8f219b93cfd516581b930f52052057139a9d83484ed86` |
| Fixture initiator patch | `5b0c8ed31e1bdfe2c6e8f8ef1af74290340cd051a336151c236914a4db5722c1` |
| FUSE adapter | `2861e0b5598c5105628f7481283df189abe790624317c9cc9f96290d42f800d3` |
| Timed runner | `47fc3f53c5f43c4129fa759460dc63d379d41ce8eea3479badf051898577b9d3` |

On `ubuntu-main`, the build context came from `git archive 08d4f95` for
the Dockerfile and the `nvme-tcp` directory. Build command from that context's
parent was:

```sh
docker buildx build --builder default --platform linux/amd64 \
  --target spdk-build --tag zerofs-nas-journal-spdk:1xhscyy3 \
  --load deploy/truenas-zerofs
```

The stage used glibc 2.41. The target launcher suite passed all five tests.
The container used the default Docker seccomp policy, no published ports,
and no bind mounts. The owned container, image tag, and
`/var/tmp/zerofs-nas-journal.1XHScyY3/nvme` were removed and their absence
verified. The parent scratch directory was still present. The shared builder
cache was preserved.
