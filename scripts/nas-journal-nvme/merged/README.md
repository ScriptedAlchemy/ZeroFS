# Merged engine NVMe/TCP durability repeat

The repeat uses native ZeroFS commit `f1fc225a0509a15e33bfa146335d442407e57cc5`
and the [existing FUA and FLUSH fixture](../README.md). The runner, FUSE
adapter, and timed initiator patch are byte-identical to commit
`e17c00d9bc15549835bd6e07e82673bd77e1fec3`. The original receipts remain in
the parent directory.

## Exact build and runtime options

On `ubuntu-main`, the owned working directory was
`/var/tmp/zerofs-nas-journal-merged.c19mWe8L/nvme`. The context came from
`git archive 08d4f95067b0445e2193dd26937fc3801819b660` for the Dockerfile and
`nvme-tcp` directory in the read-only TrueNAS app checkout.

```sh
docker buildx build --builder default --platform linux/amd64 \
  --target spdk-build --tag zerofs-nas-journal-spdk:merged-c19mwe8l \
  --load deploy/truenas-zerofs
```

The following options were also used for the separate preflight container
`zerofs-nas-journal-nvme-preflight-c19mwe8l`. The app tests and probe rebuild
ran there. The timed container received the resulting executable before use.

```sh
docker run -d --name zerofs-nas-journal-nvme-run-c19mwe8l \
  --network none --cap-add SYS_ADMIN --security-opt apparmor=unconfined \
  --device /dev/fuse --memory 8g --memory-swap 8g --cpuset-cpus 0,1 \
  --cgroupns private \
  --tmpfs /fixture/member-a:rw,nodev,nosuid,noexec,size=256m \
  --tmpfs /fixture/member-b:rw,nodev,nosuid,noexec,size=256m \
  zerofs-nas-journal-spdk:merged-c19mwe8l sleep infinity

docker exec zerofs-nas-journal-nvme-run-c19mwe8l \
  python3 -B /fixture/measure.py > timed.jsonl 2> timed.stderr
```

The default Docker seccomp policy remained in effect. The measurement program
checked the private cgroup and limits before invoking
`python3 -B /fixture/run.py`. Both native ZeroFS processes, SPDK, the real
initiator, and the two member tmpfs mounts were in that cgroup. SPDK used CPU
0 and the initiator used CPU 1. The host reported distinct physical core IDs
for CPUs 0 and 1. [Measurement wrapper](measure.py)

## Provenance

| Input or resource | Exact ID or SHA-256 |
| --- | --- |
| Pinned SPDK | `0bbb7fe4dccfb31e8d446da8822360704949d1e9` |
| DPDK submodule | `37a1a9b926b4678141931f2f22e7a877b5b565c2` |
| ISA-L submodule | `7c3479e0a9dac17f448603ec1ad64c7c625f530c` |
| Stage image | `sha256:1aa996e0936d246eb111ad5cf2b5f1c40f4d7e7c567bb624a752d611191977c8` |
| Preflight container | `e3a61b3ca3150cbcb8feb2f8dc8318eab314d8f26fa476643b7aff9edccdc1cb` |
| Workload container | `c40b433309d60368bc4423003de6e28903642f62199315c185165727d655d21f` |
| Target executable | `e6e3c23ea94571f77ebda295adc3b05c327c0fd2be657a3c971e5a0982070b2c` |
| Timed initiator executable | `bf262a5940d08f8f074337a9c99e6088ff210a678acf4ee8dcadce466d5a15bb` |
| Engine template | `05f96e08ca01df1f81f8f219b93cfd516581b930f52052057139a9d83484ed86` |

The engine executable came from `../target/ci/zerofs` and the template
from `../src/scripts/fixtures/nas-journal/engine.toml.in`. The template hash
matches the first run. The stage used glibc 2.41. All five app target tests
passed.

The native engine executable SHA-256 was
`bd87fd6e387afb775d5d7805e45b79b2d8c14854461c5ae88283fe4d1d3f83ee`.
[Native mount preflight](preflight.jsonl) · [Inspected container options](runtime-options.json)

## Results

Both authenticated cases exited successfully. Each command cutoff preceded
engine SIGKILL. With 1 MiB free on each member, cold-cache recovery read the
expected SHA-256 of the whole 16 MiB backing image and the initiator verified
the payload and guards after reconnecting to that case's original namespace
UUID. Both cases then drained to zero journal blobs, took a second SIGKILL,
and verified the full backing image independently through a read-only engine
with writeback omitted. Missing-key, wrong-key, and wrong-host connections
were refused in both cases. [Timed receipt](timed.jsonl)

| Command | Initiator submit to completion callback |
| --- | ---: |
| FUA WRITE | 14.439 ms |
| Normal WRITE in the FLUSH case | 13.490 ms |
| Explicit FLUSH | 0.323 ms |

These are individual command observations. The pinned target fsyncs the
normal write before acknowledging it.

The fresh runtime cgroup reported `memory.max` of 8,589,934,592 bytes,
`memory.peak` of 1,099,005,952 bytes (1,048.09 MiB), and zero swap limit,
usage, and peak. Every memory-event delta and all swap-event counters were
zero. CPUs 0 and 1 were effective throughout and CPU throttling was zero.
[Cgroup receipt](cgroup.json)

Both owned containers, the image tag, and the NVMe subdirectory were removed
after verification. The parent scratch directory and shared BuildKit caches
were retained.
