# Physical claim software qualification

Two bounded QEMU/KVM boots on `ubuntu-main` completed on 2026-11-19. Both used Linux `7.0.0-28-generic`, udev `255.4-1ubuntu8.18`, and libblkid `2.39.3-9ubuntu6.6`. QEMU presented three synthetic sparse qcow2 files through its emulated PCI ATA and NVMe controllers. Each guest had one vCPU, 512 MiB RAM, a RAM-only root, and no network or host device passthrough. The test used `physical check-claim ROLE` on unprovisioned images. It made no request to initialize, read the full device, or write a label. Base image hashes were unchanged after both boots.

| Guest | Control | Result |
| --- | --- | --- |
| Stock QEMU | Emulated Seagate `hdd6` | Held claim, 6,001,175,126,016 bytes |
| Stock QEMU | Emulated WD `hdd18` | Held claim, 18,000,207,937,536 bytes |
| Stock QEMU | Independent exclusive open already held | Refused with `EBUSY`, then `hdd6` reacquired after release |
| Stock QEMU | Designated HDD alias redirected to the other HDD | Refused on kernel serial mismatch |
| Stock QEMU | Correct NVMe serial and capacity, stock QEMU model exposed via the designated alias | Refused on kernel model mismatch |
| Emulated P3 controller model | Unmodified kernel and udev produced the expected NVMe alias from the emulated Identify Controller data | Held claim, 1,000,204,886,016 bytes |

Stock QEMU 8.2.2 exposes an NVMe serial option. Its Identify Controller model is a fixed string. The P3 guest ran a disposable copy of the packaged x86-64 ELF with the sole NUL-terminated `QEMU NVMe Ctrl` constant at byte offset 11,927,524 replaced by `CT1000P3SSD8` with NUL padding. The runner verified the copied ELF outside that constant and the input ELF after boot. It used the packaged SeaBIOS and normal Ubuntu guest drivers. The positive guest used only aliases created by udev. The symlink changes in the stock guest served the two identity refusal controls.

## Build and regression

The initial guest run built from `2991e1e954a1c9c8c8ebb3966012bdbadeef80e8` and exited before the first HDD claim with `invalid udev WWN`. Its probe SHA-256 was `023c0c19ee3ae6a1ee2ba405b28a2f2a138fa55f92875a85e6f83e5f0bbbe6dd`. The physical identity check enumerated an unrelated NVMe device whose real udev record used `ID_WWN=nvme.1b36-…`. The final probe used the raw-device source from `a9ca875d0e4eb4d19b0b1a725d213664b38dd1fb`. That change retained the distinct NVMe representation while keeping exact ATA WWN matching. The remote source for the changed file matched the committed file at SHA-256 `11b8ffdb8be45bd46f4a45dac9df4163456550ff5749035ce153745302604a51`.

The probe used the normal development profile, Rust 1.100.0, and the crate lockfile in the existing x86-64 Rust image. The physical-claim unit filter passed seven tests. The explicitly run libblkid memfd control passed and verified rejection of an MBR spliced into a valid ZeroFS label. The captured two-guest receipt records binary and kernel hashes, exact guest observations, and the QEMU model substitution. [receipt.json](receipt.json)

To run on a matching x86-64 Ubuntu system with unprivileged KVM, supply a readable guest kernel with matching installed drivers under `/lib/modules` and a matching Linux probe binary. The recorded kernel came from extracting the Ubuntu `linux-image-unsigned-7.0.0-28-generic` package into the owned driver directory. The runner makes no package installation or host module load.

```sh
cargo build --locked --manifest-path zerofs/raw-device/Cargo.toml --bin zerofs-raw-device
python3 -B scripts/raw-device/physical-vm.py \
  --release 7.0.0-28-generic \
  --kernel /path/to/extracted/boot/vmlinuz-7.0.0-28-generic \
  --probe /path/to/debug/zerofs-raw-device
```

## Cleanup and scope

The verification at 2026-11-19T18:18:22Z found no remaining guest scratch or build container. The final guest directory was `/var/tmp/zerofs-physical-vm-guest-hd2q5249` and the first successful stock guest directory was `/var/tmp/zerofs-physical-vm-0f69f124-6s74hyty`. The owned driver directory `/var/tmp/zerofs-physical-vm-driver-10nCqusy` held 136 MiB before removal and was verified absent. The build container `zerofs-physical-vm-probe-10nCqusy` and Studio formatting container `zerofs-physical-vm-rustfmt-identity` were verified absent. Existing Docker images and system QEMU were left intact. No NAS connection, physical storage claim, service change, or physical commissioning occurred.
