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

/// Byte capacities in the order of ROLES. The complete device must be
/// accounted for, including the labels and append overhead in each region.
pub struct P3Layout {
    lengths: [u64; 6],
    device_len: u64,
}

impl P3Layout {
    pub fn new(device_len: u64, lengths: [u64; 6]) -> Result<Self> {
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
    /// A v1 device, including an interrupted v1 initialization, fails the
    /// whole-device blank check before the first label write.
    pub fn provision_v2(&self, file: File, pool: [u8; 16], device: [u8; 16]) -> Result<()> {
        let whole = Region::whole(file.try_clone()?, self.device_len)?;
        crate::store::require_blank(&whole)?;
        let mut offset = 0;
        for (len, role) in self.lengths.into_iter().zip(ROLES) {
            let id = Identity {
                pool_uuid: pool,
                member_uuid: device,
                role,
            };
            if matches!(role, Role::Journal | Role::Catalog) {
                crate::journal_region::TransactionRegion::provision(
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
            offset += len;
        }
        Ok(())
    }

    pub fn open_v2(&self, file: File, pool: [u8; 16], device: [u8; 16]) -> Result<P3v2> {
        Region::whole(file.try_clone()?, self.device_len)?;
        let mut offset = 0;
        let mut transactional = Vec::with_capacity(2);
        let mut objects = Vec::with_capacity(4);
        for (len, role) in self.lengths.into_iter().zip(ROLES) {
            let id = Identity {
                pool_uuid: pool,
                member_uuid: device,
                role,
            };
            if matches!(role, Role::Journal | Role::Catalog) {
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
            offset += len;
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
        let mut offset = 0;
        let mut result = Vec::with_capacity(ROLES.len());
        for (n, role) in self.lengths.into_iter().zip(ROLES) {
            let region = Region::new(file.try_clone()?, self.device_len, offset, n)?;
            result.push((
                region,
                Identity {
                    pool_uuid,
                    member_uuid,
                    role,
                },
            ));
            offset += n;
        }
        Ok(result)
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
