//! Explicit raw P3 geometry. Each region has an independent object log,
//! durable checkpoint and capacity, identified by pool, device and role.
use crate::{Error, Identity, MIN_DEVICE_SIZE, RawStore, Result, Role, region::Region};
use std::fs::File;

pub const ROLES: [Role; 6] = [
    Role::Journal,
    Role::Catalog,
    Role::Metadata,
    Role::KeyEnvelope,
    Role::Scratch,
    Role::Cache,
];

/// The leading bytes [`P3Layout::provision_v2`] requires to be zero: the
/// larger of the transactional and object-region headers, as for an HDD.
pub const FRESH_BYTES: u64 = crate::store::HEADER_BYTES;

fn header_bytes(role: Role) -> u64 {
    match role {
        Role::Journal | Role::Catalog => crate::journal_region::HEADER_BYTES,
        _ => crate::store::HEADER_BYTES,
    }
}

/// Byte capacities in the order of ROLES. The complete device must be
/// accounted for, including the labels and append overhead in each region.
pub struct P3Layout {
    lengths: [u64; 6],
    device_len: u64,
}

impl P3Layout {
    /// Whole-device size used by the bounded file-backed NAS-fit fixture.
    pub const NAS_FIT_FIXTURE_BYTES: u64 = 64 << 30;

    /// Check the NAS-fit region contract. The final role owns exactly the
    /// remaining bytes and must still meet the minimum and alignment bounds.
    pub fn validate_nas_fit(device_len: u64, lengths: [u64; 6]) -> Result<()> {
        if Self::nas_fit(device_len)?.lengths != lengths {
            return Err(Error::InvalidGeometry("incorrect NAS-fit P3 layout".into()));
        }
        Ok(())
    }

    pub(crate) fn nas_fit(device_len: u64) -> Result<Self> {
        const MIB: u64 = 1 << 20;
        let mut lengths = [61_440 * MIB, 128 * MIB, 3072 * MIB, 16 * MIB, 32 * MIB, 0];
        lengths[5] = device_len
            .checked_sub(lengths.iter().sum())
            .ok_or_else(|| {
                Error::InvalidGeometry("device is too small for NAS-fit P3 layout".into())
            })?;
        Self::new(device_len, lengths)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn verify_labels(
        &self,
        file: &File,
        pool: [u8; 16],
        member: [u8; 16],
    ) -> Result<()> {
        for (offset, len, identity) in self.bindings(pool, member) {
            if matches!(identity.role, Role::Journal | Role::Catalog) {
                crate::journal_region::TransactionRegion::open(
                    file.try_clone()?,
                    self.device_len,
                    offset,
                    len,
                    identity,
                )?;
            } else {
                RawStore::binding(
                    &Region::new(file.try_clone()?, self.device_len, offset, len)?,
                    identity,
                )?;
            }
        }
        Ok(())
    }

    pub fn new(device_len: u64, lengths: [u64; 6]) -> Result<Self> {
        crate::format::check_device_size(device_len).map_err(Error::InvalidGeometry)?;
        let total = lengths.iter().try_fold(0u64, |sum, &n| sum.checked_add(n));
        if total != Some(device_len)
            || lengths
                .iter()
                .any(|&n| n < MIN_DEVICE_SIZE || !n.is_multiple_of(crate::BLOCK_SIZE))
        {
            return Err(Error::InvalidGeometry(
                "P3 regions must be aligned, nonempty and cover exactly the claimed device".into(),
            ));
        }
        Ok(Self {
            lengths,
            device_len,
        })
    }

    /// A partially initialized P3 fails ordinary open. A repeated init must
    /// refuse before changing any region, even if a later region is foreign.
    pub fn provision(&self, file: File, pool: [u8; 16], device: [u8; 16]) -> Result<()> {
        let whole = Region::whole(file.try_clone()?, self.device_len)?;
        crate::store::require_blank(&whole)?;
        for (region, id) in self.regions(&file, pool, device)? {
            RawStore::provision_region(region, id)?;
        }
        Ok(())
    }

    pub fn open(&self, file: File, pool: [u8; 16], device: [u8; 16]) -> Result<P3> {
        Region::whole(file.try_clone()?, self.device_len)?;
        let stores = self
            .regions(&file, pool, device)?
            .into_iter()
            .map(|(r, id)| RawStore::open_region(r, id))
            .collect::<Result<Vec<_>>>()?;
        Ok(P3 { stores })
    }

    /// Explicit v2 layout for a reusable transactional journal and catalog.
    ///
    /// Reads and writes only the region headers, so the time does not depend
    /// on the device size. The first [`FRESH_BYTES`] must be zero. Every P3
    /// initialization, v1 or v2, writes there first, so a provisioned or
    /// partially initialized device is refused before any write. Old bytes
    /// elsewhere are allowed. The device is discarded where it supports
    /// that, and every region header is zeroed. No region claims a blank
    /// suffix, so transactional growth zeroes what it exposes and object
    /// replay stops at the durable tail. The journal checkpoint at offset 0
    /// is written last, so an interrupted run can be repeated.
    pub fn provision_v2(&self, file: File, pool: [u8; 16], device: [u8; 16]) -> Result<()> {
        let whole = Region::whole(file.try_clone()?, self.device_len)?;
        crate::store::require_zero(&whole, FRESH_BYTES)?;
        whole.discard()?;
        let bindings: Vec<_> = self.bindings(pool, device).collect();
        let zeros = [0; FRESH_BYTES as usize];
        for &(offset, _, id) in &bindings {
            whole.write_all_at(&zeros[..header_bytes(id.role) as usize], offset)?;
        }
        whole.sync_data()?;
        // Each region rereads its own header and refuses a nonzero byte.
        for (offset, len, id) in bindings.into_iter().rev() {
            if matches!(id.role, Role::Journal | Role::Catalog) {
                crate::journal_region::TransactionRegion::provision_unproven(
                    file.try_clone()?,
                    self.device_len,
                    offset,
                    len,
                    id,
                )?;
            } else {
                RawStore::provision_region(
                    Region::new(file.try_clone()?, self.device_len, offset, len)?,
                    id,
                )?;
            }
        }
        Ok(())
    }

    pub fn open_v2(&self, file: File, pool: [u8; 16], device: [u8; 16]) -> Result<P3v2> {
        Region::whole(file.try_clone()?, self.device_len)?;
        let mut transactional = Vec::with_capacity(2);
        let mut objects = Vec::with_capacity(4);
        for (offset, len, id) in self.bindings(pool, device) {
            if matches!(id.role, Role::Journal | Role::Catalog) {
                transactional.push(crate::journal_region::TransactionRegion::open(
                    file.try_clone()?,
                    self.device_len,
                    offset,
                    len,
                    id,
                )?);
            } else {
                objects.push(RawStore::open_region(
                    Region::new(file.try_clone()?, self.device_len, offset, len)?,
                    id,
                )?);
            }
        }
        let [journal, catalog] = transactional
            .try_into()
            .map_err(|_| Error::InvalidArgument("incomplete transactional layout"))?;
        let [metadata, keys, scratch, cache] = objects
            .try_into()
            .map_err(|_| Error::InvalidArgument("incomplete object layout"))?;
        Ok(P3v2 {
            journal,
            catalog,
            metadata,
            keys,
            scratch,
            cache,
        })
    }

    fn regions(
        &self,
        file: &File,
        pool_uuid: [u8; 16],
        member_uuid: [u8; 16],
    ) -> Result<Vec<(Region, Identity)>> {
        self.bindings(pool_uuid, member_uuid)
            .map(|(offset, len, identity)| {
                let region = Region::new(file.try_clone()?, self.device_len, offset, len)?;
                Ok((region, identity))
            })
            .collect()
    }

    fn bindings(
        &self,
        pool_uuid: [u8; 16],
        member_uuid: [u8; 16],
    ) -> impl Iterator<Item = (u64, u64, Identity)> {
        let mut offset = 0;
        self.lengths.into_iter().zip(ROLES).map(move |(len, role)| {
            let start = offset;
            offset += len;
            (
                start,
                len,
                Identity {
                    pool_uuid,
                    member_uuid,
                    role,
                },
            )
        })
    }
}

pub struct P3 {
    stores: Vec<RawStore>,
}

impl P3 {
    /// Check every role binding before handing out an individual store.
    pub fn into_role(self, role: Role) -> Result<RawStore> {
        self.stores
            .into_iter()
            .find(|s| s.identity().role == role)
            .ok_or(Error::InvalidArgument("role is not on the P3"))
    }
}

pub struct P3v2 {
    pub journal: crate::journal_region::TransactionRegion,
    pub catalog: crate::journal_region::TransactionRegion,
    pub metadata: RawStore,
    pub keys: RawStore,
    pub scratch: RawStore,
    pub cache: RawStore,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BLOCK_SIZE;
    use crate::journal_region::TransactionRegion;
    use std::fs::OpenOptions;
    use std::os::unix::fs::{FileExt, MetadataExt};
    use std::sync::atomic::{AtomicU64, Ordering};

    const MIB: u64 = 1 << 20;
    const POOL: [u8; 16] = [0x51; 16];
    const MEMBER: [u8; 16] = [0x52; 16];
    /// Bytes an earlier use of the device left behind.
    const OLD: u8 = 0xa5;

    struct Media {
        path: std::path::PathBuf,
        file: File,
    }

    impl Media {
        /// A sparse device of `len` zero bytes.
        fn new(len: u64) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "zerofs-p3-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            file.set_len(len).unwrap();
            Self { path, file }
        }

        /// A device holding old bytes everywhere except the leading bytes a
        /// quick wipe clears.
        fn used(len: u64) -> Self {
            let media = Self::new(len);
            let old = vec![OLD; MIB as usize];
            for offset in (0..len).step_by(old.len()) {
                media.file.write_all_at(&old, offset).unwrap();
            }
            media
                .file
                .write_all_at(&[0; FRESH_BYTES as usize], 0)
                .unwrap();
            media
        }

        fn file(&self) -> File {
            self.file.try_clone().unwrap()
        }

        fn all(&self) -> Vec<u8> {
            std::fs::read(&self.path).unwrap()
        }
    }

    impl Drop for Media {
        fn drop(&mut self) {
            std::fs::remove_file(&self.path).unwrap();
        }
    }

    fn layout() -> P3Layout {
        P3Layout::new(12 * MIB, [4, 2, 2, 1, 1, 2].map(|n| n * MIB)).unwrap()
    }

    fn assert_zero(region: &TransactionRegion, len: u64) {
        let mut chunk = vec![0; 64 << 10];
        for offset in (0..len).step_by(chunk.len()) {
            let n = (len - offset).min(chunk.len() as u64) as usize;
            region.read(offset, &mut chunk[..n]).unwrap();
            if let Some(at) = chunk[..n].iter().position(|&b| b != 0) {
                panic!(
                    "{:?} exposed an old byte at file offset {}",
                    region.identity().role,
                    offset + at as u64
                );
            }
        }
    }

    #[test]
    fn provisioning_over_old_bytes_never_exposes_them() {
        let media = Media::used(12 * MIB);
        let layout = layout();
        layout.provision_v2(media.file(), POOL, MEMBER).unwrap();

        let roles = layout.open_v2(media.file(), POOL, MEMBER).unwrap();
        for store in [&roles.metadata, &roles.keys, &roles.scratch, &roles.cache] {
            assert!(store.is_empty());
        }
        // Growth into old bytes zeroes them, including after a shrink and a
        // reopen.
        for region in [&roles.journal, &roles.catalog] {
            assert!(region.is_empty().unwrap());
            let capacity = region.capacity();
            region.set_len(capacity).unwrap();
            assert_zero(region, capacity);
            region.write(capacity - 1, &[0x42]).unwrap();
            region.sync_data().unwrap();
            region.set_len(BLOCK_SIZE).unwrap();
        }
        drop(roles);

        let roles = layout.open_v2(media.file(), POOL, MEMBER).unwrap();
        for region in [&roles.catalog, &roles.journal] {
            let capacity = region.capacity();
            region.set_len(capacity).unwrap();
            assert_zero(region, capacity);
            region.set_len(0).unwrap();
        }
        // The journal as the engine opens it, with the payload log above
        // the redb file.
        let ceiling = roles.journal.capacity() / 2 / BLOCK_SIZE * BLOCK_SIZE;
        let journal = roles.journal.with_payload_reserve(ceiling).unwrap();
        journal.set_len(ceiling).unwrap();
        assert_zero(&journal, ceiling);

        // Object regions replay nothing past their durable tail.
        let mut metadata = roles.metadata;
        let mut cache = roles.cache;
        metadata.put(b"m", b"meta").unwrap();
        metadata.flush().unwrap();
        cache.put(b"c", &[7; 5000]).unwrap();
        cache.flush().unwrap();
        drop((
            metadata,
            cache,
            roles.catalog,
            roles.keys,
            roles.scratch,
            journal,
        ));
        let roles = layout.open_v2(media.file(), POOL, MEMBER).unwrap();
        assert_eq!(
            roles.metadata.get(b"m").unwrap().as_deref(),
            Some(&b"meta"[..])
        );
        assert_eq!(roles.cache.get(b"c").unwrap(), Some(vec![7; 5000]));
        assert!(roles.keys.is_empty() && roles.scratch.is_empty());
    }

    #[test]
    fn provisioning_refuses_nonzero_leading_bytes_or_an_earlier_p3_without_writing() {
        let layout = layout();
        let refused = |media: &Media, expected: u64| {
            let before = media.all();
            match layout.provision_v2(media.file(), POOL, MEMBER) {
                Err(Error::NotBlank { offset }) => assert_eq!(offset, expected),
                other => panic!("expected NotBlank at {expected}, got {other:?}"),
            }
            assert!(media.all() == before, "a refused provisioning wrote");
        };
        for offset in [
            0,
            BLOCK_SIZE - 1,
            BLOCK_SIZE,
            2 * BLOCK_SIZE,
            FRESH_BYTES - 1,
        ] {
            let media = Media::used(12 * MIB);
            media.file.write_all_at(&[1], offset).unwrap();
            refused(&media, offset);
        }

        let media = Media::used(12 * MIB);
        layout.provision_v2(media.file(), POOL, MEMBER).unwrap();
        refused(&media, 0);

        // A v1 layout, which a v2 open refuses, is not reinitialized either.
        let media = Media::new(12 * MIB);
        layout.provision(media.file(), POOL, MEMBER).unwrap();
        refused(&media, 0);
    }

    #[test]
    fn an_interrupted_provisioning_can_be_repeated() {
        let media = Media::used(12 * MIB);
        let layout = layout();
        layout.provision_v2(media.file(), POOL, MEMBER).unwrap();
        // A cut before the journal checkpoint, the last write, leaves every
        // other region labelled and the device unopenable.
        media
            .file
            .write_all_at(&[0; FRESH_BYTES as usize], 0)
            .unwrap();
        assert!(layout.open_v2(media.file(), POOL, MEMBER).is_err());
        layout.provision_v2(media.file(), POOL, MEMBER).unwrap();
        let roles = layout.open_v2(media.file(), POOL, MEMBER).unwrap();
        assert!(roles.journal.is_empty().unwrap() && roles.metadata.is_empty());
    }

    #[test]
    fn provisioning_touches_only_region_headers_whatever_the_device_size() {
        // The NAS-fit loop fixture and the exact size of the N5's P3.
        for len in [P3Layout::NAS_FIT_FIXTURE_BYTES, 1_000_204_886_016] {
            let media = Media::new(len);
            let layout = P3Layout::nas_fit(len).unwrap();
            let started = std::time::Instant::now();
            layout.provision_v2(media.file(), POOL, MEMBER).unwrap();
            let elapsed = started.elapsed();
            let allocated = media.file.metadata().unwrap().blocks() * 512;
            assert!(
                allocated <= MIB,
                "{len}-byte device: provisioning allocated {allocated} bytes"
            );
            assert!(
                elapsed < std::time::Duration::from_secs(10),
                "{len}-byte device: provisioning took {elapsed:?}"
            );
            layout.open_v2(media.file(), POOL, MEMBER).unwrap();
        }
    }
}
