# ZeroFS Proxmox LXC deployment

This bundle builds one exact ZeroFS commit, verifies its SHA-256, and deploys it
to a private unprivileged Proxmox LXC. It deliberately defines two incompatible
roles so production state cannot be reused by a disposable performance test.

| Role | Ownership and access | Acknowledgement | Lifecycle |
|---|---|---|---|
| `prod` | ZeroFS serves native NFS on the private CT address; 9P/FUSE + SMB3 is an optional fallback | no volatile NBD; writeback waits for SSD | stable CT; drain-safe in-place deploy and rollback only |
| `dev` | VM100 connects directly to the LXC NBD listener with eight native connections | explicit 16 GB `volatile_memory` burst tier | replaceable and cleanable after a full drain |

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

Production in-place deployment first quiesces an optional active Samba/FUSE
share and refuses any established NFS session. Unmount every Mac/VM NFS client
before deployment. It then requires four stable metrics samples with:

- accepted, local and remote sequences equal;
- dirty RAM and SSD bytes equal to zero;
- no terminal writeback error.

Only after that drain does it switch the persistent release symlink and restart
the server plus the access services selected by `--prod-access`. The old server
is stopped immediately after the second no-NFS-session proof, closing the
reconnect window while the release changes. Failure
switches the symlink back and restarts the previously active services. No
production rootfs destruction is available.

Dev replacement first syncs/unmounts VM100, disconnects its NBD client, and
requires the same four stable writeback samples plus zero volatile NBD bytes and
operations. The Proxmox host repeats the gate before shutdown. Destructive dev
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
- SSH aliases for Proxmox and VM100 (defaults `gthost-tor-pve-root` and
  `ubuntu-main`).
- `pct`, `vzdump`, and a downloaded Debian LXC template on Proxmox.
- Proxmox `local` storage configured for `snippets` so it can hold
  `local:snippets/zerofs-lxc-hook.sh`.
- For dev: `nbd-client`, XFS and systemd on VM100, plus an existing named NBD
  export. The bundle never runs `mkfs`, creates an export, or changes its size.

## Templates and resource sizing

Copy the appropriate config and secret template outside the repository:

```bash
cp proxmox/templates/zerofs-prod.toml.example /secure/zerofs-prod.toml
cp proxmox/templates/zerofs-dev.toml.example /secure/zerofs-dev.toml
cp proxmox/templates/zerofs.env.example /secure/zerofs-prod.env
chmod 600 /secure/zerofs-prod.env
```

Production defaults to a 1 TB clean disk cache, 64 GB clean read RAM, 4 GB
writeback RAM staging and a 64 GB SSD journal. Dev defaults to a smaller clean
cache, 4 GB staging and a 16 GB volatile NBD tier. The LXC limit defaults to 96
GiB because a 64 GB read cache, 4 GB staging tier and process overhead do not fit
safely in a 64 GiB cgroup. Config validation rejects an undersized limit.

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
sudo mkdir -p /Volumes/ZeroFS
sudo mount_nfs \
  -o async,nolocks,rsize=1048576,wsize=1048576,tcp,port=2049,mountport=2049,hard \
  10.10.10.30:/ /Volumes/ZeroFS
```

Use the actual production CT address in place of `10.10.10.30`. The private
WebUI is at `http://10.10.10.30:8080` and must never be made public.

VM100 can keep the production NBD/XFS volume mounted at `/mnt/zerofs-lxc`
while separately mounting the normal ZeroFS file namespace at
`/mnt/zerofs-files`. The raw NFSv3 mount is kept at
`/mnt/zerofs-files-raw`; `bindfs` presents it at `/mnt/zerofs-files` with
VM100's `zack` user mirrored as the owner. Files created from VM100 are stored
as macOS UID/GID `501:20`, so both machines can read, update, rename, and remove
the same files even though their local user IDs differ. Chown, chgrp, and chmod
requests through the mapped view are ignored so a client cannot accidentally
break that shared identity contract.

Because the normal namespace also exposes the live NBD lanes below `.nbd`,
`mnt-zerofs\x2dfiles\x2draw-.nbd.mount` overlays the raw control directory
read-only before the mapped view starts. The one-second NFS attribute cache
keeps Mac and iPhone uploads visible without using 9P on VM100.

`zerofs-shared-namespace-permissions.service` then verifies both mount
boundaries and normalizes only the ordinary namespace to UID/GID `501:20`
with owner/group write access. It fails closed unless the raw namespace is the
expected read-write NFS export and `.nbd` is a separate read-only mount.

Install the mount without changing the existing NBD units:

```bash
sudo apt-get install -y bindfs
sudo install -d -m 0755 /mnt/zerofs-files-raw /mnt/zerofs-files
sudo install -m 0644 \
  'proxmox/systemd/mnt-zerofs\x2dfiles\x2draw.mount' \
  '/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw.mount'
sudo install -m 0644 \
  'proxmox/systemd/mnt-zerofs\x2dfiles.mount' \
  '/etc/systemd/system/mnt-zerofs\x2dfiles.mount'
sudo install -m 0644 \
  'proxmox/systemd/mnt-zerofs\x2dfiles\x2draw-.nbd.mount' \
  '/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw-.nbd.mount'
sudo install -d -m 0755 /usr/local/libexec
sudo install -m 0755 \
  proxmox/guest/normalize-shared-namespace.py \
  /usr/local/libexec/zerofs-normalize-shared-namespace
sudo install -m 0644 \
  proxmox/systemd/zerofs-shared-namespace-permissions.service \
  /etc/systemd/system/zerofs-shared-namespace-permissions.service
sudo systemctl daemon-reload
sudo systemctl enable --now 'mnt-zerofs\x2dfiles\x2draw.mount'
sudo systemctl enable --now 'mnt-zerofs\x2dfiles\x2draw-.nbd.mount'
sudo systemctl start zerofs-shared-namespace-permissions.service
sudo systemctl enable --now 'mnt-zerofs\x2dfiles.mount'
```

The result is two independent VM100 mountpoints: read-write XFS over NBD at
`/mnt/zerofs-lxc`, and the read-write shared file tree at
`/mnt/zerofs-files`, with only `/mnt/zerofs-files/.nbd` protected read-only.
The raw NFS mount is an implementation detail; normal VM100 file operations
must use the mapped `/mnt/zerofs-files` path.

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

Cleanup leaves the VM disconnected and prints the preserved state path.

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
shellcheck proxmox/*.sh proxmox/hooks/*.sh proxmox/guest/*.sh \
  proxmox/monitoring/*.sh
```
