# ZeroFS Proxmox LXC deployment

This bundle builds one exact ZeroFS commit, verifies its SHA-256, and deploys it
to a private unprivileged Proxmox LXC. It deliberately defines two incompatible
roles so production state cannot be reused by a disposable performance test.

| Role | Ownership and access | Acknowledgement | Lifecycle |
|---|---|---|---|
| `prod` | ZeroFS serves native NFS on the private CT address; 9P/FUSE + SMB3 is an optional fallback | no volatile NBD; writeback waits for SSD | stable CT; drain-safe in-place deploy and rollback only |
| `dev` | isolated NBD-only endpoint; never stages, quiesces, or reconciles VM100 NFS | explicit 16 GB `volatile_memory` burst tier | replaceable and cleanable after a full drain |

Both roles use one RFC1918 interface on `vmbr1`. Neither creates a public
listener. Production NFS, 9P, WebUI, Prometheus, and NBD bind only the exact
container address at ports 2049, 5564, 8080, 9567, and 10809. RPC uses only a
Unix socket; production 9P and NBD also expose container-owned Unix sockets.
Dev NBD uses only the dev container address at port 10809. Optional production
SMB uses only loopback and the production container interface at port 445,
requires SMB3 encryption/signing and an authenticated user, and allows the
Proxmox subnet and Tailnet ranges.

## Persistence and collision guards

Replaceable rootfs contents never own ZeroFS data. Proxmox bind mounts a
role-specific host directory:

```text
/var/lib/zerofs-lxc/prod-<CTID> -> /srv/zerofs-persist
/var/lib/zerofs-lxc/dev-<CTID>  -> /srv/zerofs-persist
```

It contains clean cache, writeback journal, immutable release directories
(binary, configuration and keys), deployment receipts and (for local dev
storage) the backend itself. The `current` symlink switches the whole release,
so rollback cannot combine an old binary with a new config.
The lifecycle hook refuses startup unless the state marker matches role, CTID
and storage namespace. A host registry under `/etc/zerofs-lxc/namespaces/`
refuses to attach one remote storage URL to another role, CTID or state root.
File backends incorporate their role-specific host state root in the namespace
identity.

Production `replace` and `cleanup` are rejected in both coordinator and host
scripts. An existing production CT is never destroyed by deployment.

## Production access and ownership requirements

Native NFS is the default (`--prod-access nfs`) and does not require FUSE,
Samba, a Samba password, or a container-side filesystem mount. The NFS listener
is not authenticated; the exact RFC1918 bind plus Proxmox/Tailscale network
policy is the security boundary. Never port-forward 2049 or 8080.

The production template configures NFS and 9P `shared_identity` as UID 501 and
GID 20 and runs the WebUI with the same identity. Every NFS and 9P client,
including root, therefore acts as that shared owner; creation requests cannot
select another owner and later NFS chown/chgrp requests are ignored. This lets
the Mac and VM100 use the same direct NFS namespace even when their local
numeric users differ. It deliberately removes per-client identity separation,
so the private-network restriction is mandatory. Deployment requires numeric
identity parity across all three writable frontends. Before quiescing an
existing managed NFS mount, it recursively scans the mounted namespace without
crossing filesystems and excludes `.nbd`. The fail-closed receipt must say
`ZEROFS_SHARED_NAMESPACE_V1 verified=1 objects=<nonzero> wrong_owner=0 ...`.
Deployment never changes ownership automatically; a rejected receipt prints
the manual `chown 501:20` remediation and the receipt required for retry.

For an existing tree, inventory first without mutation:

```bash
./proxmox/deploy.sh ownership-inventory --role prod --ctid 198 \
  --container-ip 10.10.10.55 --dry-run
```

The live inventory omits `--dry-run`. If repair is required, review that
inventory, then explicitly confirm the fixed target twice:

```bash
ZEROFS_CONFIRM_OWNERSHIP_REPAIR=501:20 \
  ./proxmox/deploy.sh ownership-repair --role prod --ctid 198 \
  --container-ip 10.10.10.55 --confirm-ownership-repair 501:20
```

Repair is resumable and idempotent: it visits only wrong-owner objects, uses
`-xdev`, prunes `.nbd`, and changes symlink ownership without following the
target. A converged scan is atomically retained at
`/var/lib/zerofs-deploy/ownership-receipts/shared-501-20.receipt`; normal deploy
still requires a fresh successful recursive receipt before quiescing.

The optional `--prod-access both` (or legacy `smb`) mode additionally makes the
production LXC own a 9P/FUSE mount and export it through Samba. That requires:

- Proxmox `features: fuse=1`, added by the host script;
- `/dev/fuse` access in the unprivileged LXC (provided by that feature);
- `fuse3`, Samba, and `user_allow_other` inside the LXC;
- the ZeroFS server, `zerofs-lxc-mount.service`, and `smbd.service` in that
  order;
- enough mapped UID/GID permissions for the host bind mount. The script creates
  writable state/cache as host UID/GID 100000, the default mapping for root in
  an unprivileged CT;
- a dedicated Samba user. The mount unit creates `/srv/zerofs-share/data` for
  that user after FUSE is ready.

The production mount disables FUSE writeback and relaxed consistency. Combined
with `[writeback].ack_mode = "ssd"`, ordinary writes cross the local persistent
journal rather than a volatile acknowledgement tier. Samba can serve both Mac
and VM100, but the same filesystem must not also be mounted read-write by an NBD
client.

The WebUI is intentionally unauthenticated and has writable filesystem/admin
capabilities. It is compiled only for production and binds only
`http://<container-ip>:8080`; expose it solely through the private Proxmox
network or an approved Tailnet subnet route. Do not forward NFS, 9P, WebUI,
SMB, NBD, or Prometheus from a public interface.

## Safe lifecycle

Production in-place deployment holds a root-owned global Proxmox deployment
lock and a root-owned global VM100 transition lock. It recovers any crash-left host/VM transaction,
validates the ownership receipt, then records VM100's direct-NFS unit,
enablement, activity, and live mount record in a root-owned transaction. It
quiesces that one mount before changing the container. Unmount the Mac NFS
client before deployment; the coordinator handles VM100. The host also
quiesces an optional active Samba/FUSE share and refuses any remaining
established NFS session. It then requires four stable metrics samples with:

- accepted, local and remote sequences equal;
- dirty RAM and SSD bytes equal to zero;
- no terminal writeback error.

The default remains a complete remote drain. A production upgrade whose sole
purpose is to replace a slow remote transport may instead use
`--local-durable-upgrade --confirm-local-durable-upgrade CTID` together with
`ZEROFS_CONFIRM_LOCAL_DURABLE_UPGRADE=CTID`. This exception still requires four
stable samples with accepted equal to local, zero dirty RAM, and no terminal
error. It also requires the existing and staged releases to use exactly
`/srv/zerofs-persist/state/writeback`; the new server must recover the retained
SSD journal before serving clients. Remote lag and dirty SSD may remain only for
this explicitly confirmed upgrade, and rollback returns to the prior release
against the same journal.

When VM100 has no direct mount yet, or has a recognized legacy bindfs topology,
the host first activates a private NFS-and-metrics-only configuration. VM100
mounts that namespace and produces the real recursive ownership receipt before
the coordinator unmounts it again and promotes the full 9P/NBD/WebUI and
optional SMB access profile. Optional SMB assets are staged during maintenance
but are not started until promotion.

Only after the drain does deployment switch the persistent release symlink and restart
the server plus the access services selected by `--prod-access`. The old server
is stopped immediately after the second no-NFS-session proof, closing the
reconnect window while the release changes. Failure switches the symlink back
and restarts the previously active services. The coordinator then restores
VM100's exact prior unit contents, enablement, and mounted source. Staging
fails before quiescing, while failures during quiesce, host deployment, or
reconcile invoke rollback. The host-owned state root and its release,
transaction, receipt, and `current` names are not writable by container root;
only the `state`, `cache`, and dev backend directories are CT-owned. The exact
prior config is included in the durable rollback transaction, including when a
maintenance bootstrap reuses an existing release identifier. Host activation remains uncommitted until the VM
mount is proven; reconcile failure compensates the host release and resources
before restoring the VM mount. Durable phase state makes a retry recover a
crash-left transaction. Before either participant deletes recovery state, VM100
persists the authoritative commit decision. A retry finishes a decided host/VM
commit and rolls back only a transaction that never reached that decision.
Incomplete compensation leaves the CT stopped and preserves its transaction
and resource snapshot for another recovery attempt. Success removes only its owned transaction and staging
directory. No production rootfs destruction is available.

Dev replacement requires the same four stable writeback samples plus zero
volatile NBD bytes and operations. The Proxmox host repeats the gate before
shutdown. A dev action never touches VM100's production NFS unit and never
connects or mounts an NBD device on VM100. Destructive dev
replacement requires the exact CTID confirmation and takes a `vzdump` rollback
backup. Dev cleanup destroys only the rootfs and preserves its role-specific
state directory and namespace registry.

Destroying a CT cannot preserve RAM; these drain gates make accepted RAM data
durable before a dev CT is replaced. A missing metric fails closed.

## Requirements

- Python 3.11+, Rust and Cargo on the build/control machine.
- Production WebUI build tools on VM100: Node.js 22/npm, `wasm-pack`, GNU Make,
  `curl`, `bsdtar` (`libarchive-tools`), `unsquashfs`
  (`squashfs-tools`), `cpio`, `xz`, and the Rust `wasm32-unknown-unknown`
  target. Production deploy runs `make webui` before
  `cargo build --features webui`, prepends `~/.local/bin` and `~/.cargo/bin` to
  `PATH`, enforces the Vite Node minimum (20.19+, 22.12+, or newer), and fails
  if `webui/dist/index.html` is absent.
- An SSH alias for Proxmox (default `gthost-tor-pve-root`). VM commands use
  the backward-compatible `ssh` transport by default, which also requires the
  VM100 alias `ubuntu-main`.
- `pct`, `vzdump`, and a downloaded Debian LXC template on Proxmox.
- Proxmox `local` storage configured for `snippets` so it can hold
  `local:snippets/zerofs-lxc-hook.sh`.
- NFS client support (`nfs-common`) and systemd on VM100. The bundle never
  connects, mounts, or configures an NBD device on VM100.

### VM100 command transport

`deploy.py` defaults to `--vm-transport ssh`, preserving the existing VM100
SSH path. When the coordinator itself is running inside VM100, explicitly use
`--vm-transport local --vm-vmid 100` to avoid SSHing back into the same guest.
Before any deployment mutation, local mode reads the guest hostname and DMI
SMBIOS UUID, reads VM 100's `name` and `smbios1` UUID through `qm config` on
the configured Proxmox host, and requires both identities to match exactly.
Missing or mismatched identity fails closed; it never silently falls back to
local execution. Local dry-runs perform the same identity reads plus a
nonblocking probe of the VM-global lock when its lock file already exists; the
probe does not create that file. Planned VM commands and staging are labelled
as lock-owned without printing fabricated identity or lease success. Omit the
local option to use the SSH fallback.

Local mode starts the same root-owned, transaction-long `flock` holder directly
with `sudo`. Every VM command, staged file, recovery action, rollback, and
cleanup request continues through that one process. Its protocol preserves
stdout, stderr, and the exit status separately. QEMU guest-agent commands such
as `qm agent 100 ping` and `qm guest exec 100 -- hostname` remain useful for
external health or recovery checks, but they are not a deployment transport:
independent guest-agent executions cannot own the coordinator lock across the
whole transaction. The lock holder runs in a separate process group so a
terminal interrupt reaches the coordinator first; rollback finishes while the
holder still owns the lock, and the original interrupt remains the reported
failure. Each command runs in its own process group without inheriting the lock
descriptor. A STARTED receipt binds cancellation to that command; interrupt or
coordinator loss terminates and reaps the command group with bounded
TERM-to-KILL escalation while the holder retains the lock for rollback. Clean
holder exit then releases the lock immediately.

## Templates and resource sizing

Copy the appropriate config and secret template outside the repository:

```bash
cp proxmox/templates/zerofs-prod.toml.example /secure/zerofs-prod.toml
cp proxmox/templates/zerofs-dev.toml.example /secure/zerofs-dev.toml
cp proxmox/templates/zerofs.env.example /secure/zerofs-prod.env
chmod 600 /secure/zerofs-prod.env
```

Production defaults to a 1 TB clean disk cache, 32.0 decimal GB of clean read
RAM, a distinct 4.0 decimal GB writeback staging tier, and a 64 GB SSD journal.
`[runtime] memory_limit_gb = 96.0` declares the dedicated ZeroFS envelope; it
is not another cache. Validation reserves room for the eventual 16.0 GB
unified volatile tier plus the memory guard's 32 GiB unmodeled-residency and 8
GiB process/companion allowances. It converts Proxmox `--memory-mb` from MiB
to bytes and rejects a runtime envelope larger than the CT limit. The default
CT remains 98304 MiB (96 GiB), which is larger than the 96.0 decimal-GB ZeroFS
envelope and leaves the difference for the container. Dev keeps its smaller
clean cache, separate 4 GB staging tier, and 16 GB volatile NBD tier.

Every SFTP session field is capped at four in both roles. The dev template uses
a role-local `file://` backend by default; enabling SFTP requires a distinct URL
namespace and session values no greater than four.

## Dry-run production creation or update

Production has no guest NBD lifecycle. It creates the CT when absent and later
updates it in place. Native NFS is the default and needs no Samba secret:

```bash
./proxmox/deploy.sh deploy \
  --role prod \
  --ctid 130 \
  --container-ip 10.10.10.30 \
  --bridge vmbr1 \
  --gateway 10.10.10.1 \
  --template local:vztmpl/debian-13-standard_13.1-2_amd64.tar.zst \
  --config /secure/zerofs-prod.toml \
  --env-file /secure/zerofs-prod.env \
  --identity-file /secure/storage-key \
  --known-hosts /secure/known_hosts \
  --dry-run
```

Remove `--dry-run` only after checking every resolved host, CTID, address,
template, namespace and state path.

For the optional encrypted SMB fallback, add `--prod-access both`,
`--samba-user zerofs-share`, and
`--samba-password-file /secure/samba-password`. The password file contains one
line and is never logged or retained in the CT rootfs.

## macOS native NFS and WebUI

After the production listener proof succeeds and the Mac can route the private
Proxmox subnet through Tailscale:

```bash
ZEROFS_CT_IP=10.10.10.55  # Set this to the actual production CT address.
sudo mkdir -p /Volumes/ZeroFS
sudo mount_nfs \
  -o async,nolocks,rsize=1048576,wsize=1048576,tcp,port=2049,mountport=2049,hard \
  "${ZEROFS_CT_IP}:/" /Volumes/ZeroFS
```

The private WebUI is at `http://${ZEROFS_CT_IP}:8080` and must never be made
public.

VM100 has exactly one persistent ZeroFS mount: the direct read-write NFSv3/TCP
file namespace at `/mnt/zerofs-files`. The NFS client uses hard mounts, 1 MiB
read/write requests, a one-second attribute cache, and `_netdev`.
Only a production deploy renders this unit's `What=` source from its validated
private container address. Dev NBD-only deploys do not stage or invoke the VM
NFS transition helper. Reconcile changes the unit only when its bytes differ,
enables or starts it only when needed, and requires the exact source, NFSv3
`nfs` type, and `rw` option. A direct reconcile against an already-correct
unit and live mount performs no systemd mutation.

During reconciliation, recognized legacy NBD/raw-namespace topology is retired
automatically in strict order: sync the expected `/dev/nbd0` mount; disable both
known mount-unit spellings; unmount it; disable the known client; disconnect
only a device proven owned by that client or mount; then remove only known
artifacts. Raw namespace units, mounts, permissions service, and known
normalizer artifact are also retired. Unknown mount sources, units, and
unowned connected devices fail closed. The helper never reconnects NBD or
recreates a second mount.

The deploy command above installs and renders the one persistent direct NFS
mount. Do not copy the tracked unit verbatim: its `What=` line is only an
example placeholder. After deployment, verify the actual source, filesystem
type, and options:

```bash
findmnt -rn -M /mnt/zerofs-files -o SOURCE,FSTYPE,OPTIONS
# Required: <actual-CT-IP>:/ nfs and an rw option.
```

The result is one persistent VM100 ZeroFS mount: the read-write shared file tree
at `/mnt/zerofs-files` over the production NFSv3 export. No bindfs layer,
second raw mount, ownership normalizer, `.nbd` guard, or persistent VM NBD
client participates in this path.

The deploy path never installs, enables, starts, mounts, or silently restores
retired NBD artifacts and never deletes remote NBD export data.
It accepts `--source-client-unit`, `--source-mount-unit`, and
`--source-mountpoint` only as an explicit, one-way legacy-quiescing set during a
migration; it does not reconnect the retired device.

ZeroFS NFS reports writes as stable while they are buffered, and tested macOS
and Linux clients do not issue a durability-producing COMMIT on `fsync`.
Production `ack_mode = "ssd"` protects the local writeback boundary, but NFS
`fsync` does **not** prove that the remote SFTP/object-store sequence caught up.
Use the Grafana remote-lag panels and the drain gate before maintenance; use 9P
instead when per-call stable-storage semantics are required.

## Dry-run dev creation, update or replacement

Use `--skip-existing-drain` only for a genuinely new dev address with no old
server. The host-side collision and live-CT checks remain enabled.

```bash
./proxmox/deploy.sh deploy \
  --role dev \
  --ctid 120 \
  --container-ip 10.10.10.20 \
  --config /secure/zerofs-dev.toml \
  --skip-existing-drain \
  --dry-run
```

For a disposable rootfs replacement:

```bash
ZEROFS_CONFIRM_REPLACE=120 ./proxmox/deploy.sh replace \
  --role dev \
  --ctid 120 \
  --container-ip 10.10.10.20 \
  --config /secure/zerofs-dev.toml \
  --dry-run
```

When migrating an older VM-local pilot into the dev CT, name its units and
metrics explicitly. The old server stops only after its drain:

```text
--source-client-unit zerofs-nbd-client.service
--source-mount-unit mnt-storagebox-nbd-pilot.mount
--source-mountpoint /mnt/storagebox-nbd-pilot
--source-server-unit zerofs-nbd-pilot.service
--existing-metrics-url http://127.0.0.1:19567/metrics
```

Migration failure remains fail-closed rather than starting two writable servers
against one backend.

## Status and dev cleanup

```bash
./proxmox/deploy.sh status \
  --role prod --ctid 130 --container-ip 10.10.10.30

./proxmox/deploy.sh cleanup \
  --role dev --ctid 120 --container-ip 10.10.10.20 --dry-run
```

Dev cleanup prints the preserved state path and leaves VM100's production NFS
mount untouched.

For both roles, an existing CT's requested cores, memory, swap, on-boot flag,
startup order, private `net0`, and persistent `mp0` binding are validated on
every deploy. Only mismatched fields are reconciled; unrequested network and
mount options are preserved. The original `pct config` is captured before the
first change and restored if a later deployment step fails. A correct existing
CT is not restarted merely to restate the same resource values.

## Prometheus and Grafana provisioning

`monitoring/` contains a credential-free Prometheus scrape fragment, a
loopback-only Prometheus service override, a provisioned Grafana Prometheus
datasource, and the `ZeroFS Production Overview` dashboard. It does not replace
or delete the monitoring CT's existing InfluxDB datasource.

The installer defaults to the existing monitoring CT 123 at `10.10.10.53`, but
both values are explicit options. It validates that the CT owns that private
address and can reach ZeroFS metrics before changing files. If Prometheus is
absent, it masks the service first, installs the Debian-family package without
allowing an interim public listener, then configures it on
`127.0.0.1:9090`. `promtool` must accept the complete config before either
service restarts. Prometheus 2.43 or newer is required for
`scrape_config_files` only when integrating into a pre-existing unmanaged
Prometheus config; a newly provisioned instance uses a compatible complete
repo-owned config and is not subject to that import requirement.

Review the non-mutating plan first:

```bash
python3 proxmox/monitoring/install.py \
  --monitoring-ctid 123 \
  --monitoring-ip 10.10.10.53 \
  --zerofs-ip 10.10.10.30 \
  --dry-run
```

After reviewing the resolved CTID and addresses, replace `--dry-run` with the
explicit `--apply` guard. Existing destination files are copied to a unique
`/var/lib/zerofs-monitoring-backups/<UTC timestamp>` directory inside the CT.
Any config, service, listener, or health-check failure restores those exact
files and prior service state. A newly installed Prometheus package remains
installed but is stopped and disabled after rollback, avoiding an unsafe
package purge.

The dashboard shows service health; accepted, local, and remote sequences and
lags; dirty RAM/SSD use and capacities; filesystem/local/remote payload rates;
retry, terminal-error, and pending-age state; and segment GC/allocator
footprints. ZeroFS currently exports no explicit clean-cache hit/miss counters,
so the dashboard says so rather than inventing a proxy metric.

## Verification

```bash
python3 -m unittest discover -s proxmox/tests -p 'test_*.py'
python3 -m py_compile proxmox/deploy.py proxmox/nfs_mount.py proxmox/vm_nfs_transition.py
bash -n proxmox/*.sh proxmox/guest/*.sh proxmox/hooks/*.sh proxmox/monitoring/*.sh
shellcheck proxmox/*.sh proxmox/guest/*.sh proxmox/hooks/*.sh proxmox/monitoring/*.sh
```
