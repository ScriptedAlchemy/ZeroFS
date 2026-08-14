# ZeroFS Proxmox LXC deployment

This bundle builds one exact ZeroFS commit, verifies its SHA-256, and deploys it
to a private unprivileged Proxmox LXC. It deliberately defines two incompatible
roles so production state cannot be reused by a disposable performance test.

| Role | Ownership and access | Acknowledgement | Lifecycle |
|---|---|---|---|
| `prod` | LXC owns a local 9P/FUSE mount and exports its `data` directory over private SMB3 | no volatile NBD; writeback waits for SSD | stable CT; drain-safe in-place deploy and rollback only |
| `dev` | VM100 connects directly to the LXC NBD listener with eight native connections | explicit 16 GB `volatile_memory` burst tier | replaceable and cleanable after a full drain |

Both roles use one RFC1918 interface on `vmbr1`. Neither creates a public
listener. Prometheus uses the container address at port 9567; RPC and production
9P use Unix sockets. Dev NBD uses only the dev container address at port 10809.
Production SMB uses only loopback and the production container interface at
port 445, requires SMB3 encryption/signing and an authenticated user, and allows
the Proxmox subnet and Tailnet ranges.

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

## Production ownership requirements

The production LXC owns the filesystem mount. That requires:

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

Mac access should reach private port 445 through a Tailnet subnet route or a
private Tailscale address. Do not forward SMB, NBD or Prometheus from a public
interface.

## Safe lifecycle

Production in-place deployment first stops Samba, syncs and stops the FUSE
mount, then requires four stable metrics samples with:

- accepted, local and remote sequences equal;
- dirty RAM and SSD bytes equal to zero;
- no terminal writeback error.

Only after that drain does it switch the persistent release symlink and restart
the server, mount and Samba. Failure switches the symlink back and restarts the
previous services. No production rootfs destruction is available.

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
updates it in place:

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
  --samba-user zerofs-share \
  --samba-password-file /secure/samba-password \
  --dry-run
```

Remove `--dry-run` only after checking every resolved host, CTID, address,
template, namespace and state path. The password file contains one line and is
never logged or retained in the CT rootfs.

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

## Verification

```bash
python3 -m unittest discover -s proxmox/tests -p 'test_*.py'
shellcheck proxmox/*.sh proxmox/hooks/*.sh proxmox/guest/*.sh
```
