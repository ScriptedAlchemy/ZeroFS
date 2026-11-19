# Authenticated NVMe/TCP acceptance on the merged engine

The [bounded volatile acknowledgement fixture](../README.md) passed on
ZeroFS source commit `7821e763c69437c6785b5c67f677fa166a1c167d`, native
`ci` binary SHA-256
`93488a4e5a160de9fd0e170568a8dae05beddfc7ab64b615894e8a482d75bebb`.
The same template, runner patch and cgroup wrapper were applied. The
`spdk-build` stage was rebuilt in fresh private builder
`zerofs-ram-ack-nvme-dzwdfvz5` on `ubuntu-main` using the same TrueNAS app
pin `08d4f95067b0445e2193dd26937fc3801819b660`. The five pinned launcher
unit tests and the native mount preflight passed.
[App tests](app-tests.txt) · [Preflight receipt](preflight.jsonl) · [SPDK provenance](spdk-provenance.txt) · [Runtime options](runtime-options.json)

| Command | Initiator submit to completion callback |
| --- | ---: |
| FUA WRITE to LBA 3 | 12.260 ms |
| Normal WRITE to LBA 8 | 15.452 ms |
| Explicit FLUSH after normal WRITE | 0.243 ms |

Each row measures one command from the synthetic workload. The pinned target
fsyncs the normal write before acknowledging it.

Both cases refused missing-key, wrong-key and wrong-host clients. Both
verified the expected 16 MiB backing SHA-256 after an ACK-triggered engine
SIGKILL with 1 MiB free per member, reauthenticated to their original
namespace and checked the payload and guards. Both reached the remote
frontier with zero journal blobs and verified the full backing SHA-256
through a journal-free read-only engine after another SIGKILL.
[Timed receipt](timed.jsonl)

The service used CPUs 0–1, an 8 GiB cgroup and zero swap. Peak memory
was 1,107,415,040 bytes and all swap and memory events were zero. The
owned containers, image tag, scratch directory, builder and its volume
were removed and verified absent.
[Cgroup receipt](cgroup.json) · [Cleanup receipt](cleanup.txt)
