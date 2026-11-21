//! Bounded first raw-journal consumer for immutable segments. Each item names
//! one destination member. The caller owns placement, claims and publication
//! of the filesystem reference after the object's durability barrier.
use crate::{Error, Identity, RawStore, Result, Role};
use sha2::{Digest, Sha256};

const PREFIX: &str = "pending/";
const MAX_PENDING: usize = 256;
pub const MAX_SEGMENT_BYTES: usize = 32 << 20;

pub struct LandingJournal {
    store: RawStore,
}

impl LandingJournal {
    pub fn new(store: RawStore) -> Result<Self> {
        if store.identity().role != Role::Journal {
            return Err(Error::InvalidArgument("expected dedicated journal role"));
        }
        // This device is exclusively a landing journal. Refuse
        // unknown or excessive live records before use.
        if store.len() > MAX_PENDING || !store.keys().all(|key| parse_key(key).is_some()) {
            return Err(Error::InvalidArgument(
                "invalid raw landing journal contents",
            ));
        }
        Ok(Self { store })
    }

    /// Rebuild the drain queue from durable records on a fresh process.
    pub fn pending_segments(&self) -> impl Iterator<Item = ([u8; 16], &str)> {
        self.store
            .keys()
            .map(|key| parse_key(key).expect("validated landing key"))
    }

    fn destination(&self, destination: Identity) -> Result<()> {
        let journal = self.store.identity();
        if destination.role != Role::Data
            || destination.pool_uuid != journal.pool_uuid
            || destination.member_uuid == journal.member_uuid
            || destination.member_uuid == [0; 16]
        {
            return Err(Error::InvalidArgument(
                "journal destination identity mismatch",
            ));
        }
        Ok(())
    }

    /// Success acknowledges a durable local journal copy. Payloads with the
    /// same content and destination share a pending record.
    pub fn stage(&mut self, destination: Identity, bytes: &[u8]) -> Result<String> {
        self.destination(destination)?;
        if bytes.is_empty() || bytes.len() > MAX_SEGMENT_BYTES {
            return Err(Error::InvalidArgument(
                "segment exceeds raw landing bounds or is empty",
            ));
        }
        let digest = format!("{:x}", Sha256::digest(bytes));
        let key = pending_key(destination, &digest);
        if !self.store.contains_key(key.as_bytes()) {
            if self.store.len() >= MAX_PENDING {
                return Err(Error::InvalidArgument("pending segment limit reached"));
            }
            self.store.put(key.as_bytes(), bytes)?;
        } else if self.store.get(key.as_bytes())?.as_deref() != Some(bytes) {
            return Err(Error::InvalidArgument("existing journal object differs"));
        }
        self.store.flush()?;
        Ok(digest)
    }

    /// Publish the content-addressed object, verify it, and only then retire
    /// its journal record. Retrying a cut between the two barriers is safe.
    pub fn drain(&mut self, target: &mut RawStore, digest: &str) -> Result<()> {
        self.destination(target.identity())?;
        if !valid_digest(digest) {
            return Err(Error::InvalidArgument("invalid segment digest"));
        }
        let key = pending_key(target.identity(), digest);
        let object = format!("segment/{digest}");
        let Some(payload) = self.store.get(key.as_bytes())? else {
            // A past drain may already have completed. Require proof on this
            // target before returning success for the idempotent retry.
            let bytes = target
                .get(object.as_bytes())?
                .ok_or(Error::InvalidArgument(
                    "no pending or published segment for this member",
                ))?;
            if format!("{:x}", Sha256::digest(&bytes)) != digest {
                return Err(Error::InvalidArgument("destination digest differs"));
            }
            return target.flush();
        };
        if format!("{:x}", Sha256::digest(&payload)) != digest {
            return Err(Error::InvalidArgument("journal digest differs"));
        }
        match target.get(object.as_bytes())? {
            None => target.put(object.as_bytes(), &payload)?,
            Some(bytes) if bytes == payload => (),
            Some(_) => return Err(Error::InvalidArgument("destination object differs")),
        }
        target.flush()?;
        if target.get(object.as_bytes())?.as_deref() != Some(payload.as_slice()) {
            return Err(Error::InvalidArgument("destination verification failed"));
        }
        self.store.delete(key.as_bytes())?;
        self.store.flush()
    }
}

fn valid_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn pending_key(member: Identity, digest: &str) -> String {
    let uuid = u128::from_be_bytes(member.member_uuid);
    format!("{PREFIX}{uuid:032x}/{digest}")
}

fn parse_key(bytes: &[u8]) -> Option<([u8; 16], &str)> {
    let key = std::str::from_utf8(bytes).ok()?;
    let (member, digest) = key.strip_prefix(PREFIX)?.split_once('/')?;
    if member.len() != 32
        || !member
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || !valid_digest(digest)
    {
        return None;
    }
    let id = u128::from_str_radix(member, 16).ok()?;
    (id != 0).then_some((id.to_be_bytes(), digest))
}
