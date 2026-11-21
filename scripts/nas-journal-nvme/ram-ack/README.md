# Bounded volatile acknowledgement NVMe/TCP qualification

The [existing authenticated crash fixture](../README.md) ran on `ubuntu-main`. The workload template enables `filesystem.write_ack_mode = "volatile_memory"` with 256 MiB and 2048 operations. The runner patch checks those settings in both the initial and recovery engines. The final read-only verifier uses materialized mode because the engine requires a writable journal for volatile mode. The recovery and authentication checks follow the original fixture. [Engine template](nas-journal.toml.in) · [Runner patch](runner.patch)

The private Docker builder built the pinned TrueNAS app stage from a `git archive` of the Dockerfile and `nvme-tcp` directory at `08d4f95067b0445e2193dd26937fc3801819b660`.

```sh
docker buildx create --name zerofs-ram-ack-nvme-pjwuwlh2 --driver docker-container
docker buildx build --builder zerofs-ram-ack-nvme-pjwuwlh2 --platform linux/amd64 \
  --target spdk-build --tag zerofs-nas-journal-spdk:ram-ack-pjwuwlh2 \
  --load --progress plain deploy/truenas-zerofs
```

Separate preflight and timed containers used the following Docker options. SPDK used CPU 0 and the initiator used CPU 1. The host reported cores 0 and 1 on physical package 0. [Inspected options](runtime-options.json)

```sh
--network none --cap-add SYS_ADMIN --security-opt apparmor=unconfined \
--device /dev/fuse --memory 8g --memory-swap 8g --cpuset-cpus 0,1 \
--cgroupns private \
--tmpfs /fixture/member-a:rw,nodev,nosuid,noexec,size=256m \
--tmpfs /fixture/member-b:rw,nodev,nosuid,noexec,size=256m
```

Preflight ran the pinned app's `python3 -B -m unittest -v test_target` before applying the timed initiator patch and rebuilding the probe with `make -j2`. All five tests passed. The timed container received the resulting probe. The [original cgroup wrapper](../merged/measure.py) ran `python3 -B /fixture/run.py`. [App tests](app-tests.txt) · [SPDK build inputs and executable digests](spdk-provenance.txt)

## Native engine and results

The native `ci` build came from ZeroFS commit
`f1e3762fbbbd2b58ada1a9875838c6b14c3b1443`. The executable's SHA-256 was
`f87f22822760dea612c03ea4b6b54ca276d1e748d83bfcc420419d7d879fa0fb`
on the host and in both containers. The mount preflight and both measured
cases exited successfully. [Preflight receipt](preflight.jsonl) · [Timed receipt](timed.jsonl)

| NVMe command | Initiator submit to completion callback |
| --- | ---: |
| FUA WRITE to LBA 3 | 7.642 ms |
| Normal WRITE to LBA 8 | 11.907 ms |
| Explicit FLUSH after the normal WRITE | 0.333 ms |

Each duration measures one command on the synthetic fixture. The inherited
SPDK AIO patch fsyncs the normal WRITE before acknowledging it.

Both command cutoffs preceded the engine SIGKILL. Each member was held at
1,048,576 bytes free and reported `below_free_space_reserve`. Both recovered
the full 16 MiB backing SHA-256 from the retained writeback journal with a
fresh read cache. The initiator authenticated against the original namespace
UUID and validated the changed LBA, the other case's LBA, and neighboring
guards. Missing-key, wrong-key, and wrong-host connections were refused in
each case. After removing the ballast, both cases reached the remote frontier
with zero blobs, took a second SIGKILL, and verified the full backing hash
using a read-only engine without writeback and with another fresh read cache.

The timed cgroup reported a memory limit of 8,589,934,592 bytes and a peak of
1,109,590,016 bytes (1,058.19 MiB). Swap limit, use, peak, and all swap and
memory event counters were zero. Effective CPUs were 0 and 1, with zero
throttling. [Cgroup receipt](cgroup.json)

The owned preflight and workload containers, image tag, private builder,
builder container, builder volume, and scratch directory were removed and
their absence verified. The default BuildKit cache was untouched. [Cleanup receipt](cleanup.txt)
