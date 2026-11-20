//! Room the pool has promised to segment data it has not placed yet.
//!
//! A WRITE is admitted before its reply: it takes a [`Claim`] on one member
//! for an upper bound of the bytes its frames, their directory entries and a
//! segment footer can occupy. Its frames then land in an open segment, whose
//! lease on the same member grows by what the claim funds. The lease stays
//! until the segment's placement commits, so an admitted segment always has
//! room for its seal, whether it is published at once or later by the
//! writeback journal. Every reservation and every new claim counts the
//! outstanding claims and leases, so no two promises share the same bytes.
//!
//! A metadata-survival write into a raw member's log takes a survival claim
//! for its physical cost while its PUT runs, so it cannot take bytes promised
//! to segment data, and no later promise can take the bytes it is writing.
//! Survival bytes count against the member's physical room, not its quota.
//!
//! A reservation is counted here from the moment its member is chosen until
//! a catalog view shows its upload row, which the catalog's reserved bytes
//! count from then on. Admission therefore reads committed catalog rows and
//! this ledger, and never waits for a catalog commit.
//!
//! Nothing here is durable. A restart forgets every claim and lease, with the
//! unflushed data they covered; journaled segments are leased again when the
//! writeback store attaches to the pool.
use super::PoolMember;
use crate::segment::Segid;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Key {
    Claim(u64),
    Segment(Segid),
    /// A metadata-survival PUT. It costs physical bytes but no quota.
    Survival(u64),
    /// A reservation, by upload id, whose catalog commit may be in flight.
    Reserving(Uuid),
}

#[derive(Debug)]
struct Entry {
    member: String,
    bytes: u64,
    /// A catalog reservation for the segment is outstanding and already
    /// counts these bytes.
    placing: bool,
}

/// Promised bytes on one member: logical bytes against its declared capacity
/// and their physical cost against its filesystem.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Promise {
    pub(super) logical: u64,
    pub(super) physical: u64,
}

/// Every member's outstanding promises.
#[derive(Clone, Debug, Default)]
pub(super) struct Admitted(BTreeMap<String, Promise>);

impl Admitted {
    pub(super) fn on(&self, member: &str) -> Promise {
        self.0.get(member).copied().unwrap_or_default()
    }

    /// Physical bytes promised across `members` that are local. Local members
    /// share one physical budget in the allocator, so their promises do too.
    pub(super) fn local_physical(&self, members: &[PoolMember]) -> u64 {
        members
            .iter()
            .filter(|member| member.is_local())
            .fold(0_u64, |sum, member| {
                sum.saturating_add(self.on(&member.id).physical)
            })
    }
}

#[derive(Debug, Default)]
pub(super) struct Ledger {
    entries: HashMap<Key, Entry>,
    next_claim: u64,
}

impl Ledger {
    /// Upload ids of reservations whose catalog commit may be in flight.
    pub(super) fn reserving(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.entries.keys().filter_map(|key| match key {
            Key::Reserving(upload) => Some(*upload),
            _ => None,
        })
    }

    /// Promises outside `except` that no catalog reservation counts yet.
    /// `committed` names the reserving uploads whose rows the caller's
    /// catalog view already shows.
    pub(super) fn admitted(
        &self,
        members: &[PoolMember],
        except: Option<Key>,
        committed: &BTreeSet<Uuid>,
    ) -> Admitted {
        let mut admitted = BTreeMap::<String, Promise>::new();
        for (key, entry) in &self.entries {
            if entry.placing
                || Some(*key) == except
                || matches!(key, Key::Reserving(upload) if committed.contains(upload))
            {
                continue;
            }
            let Some(member) = members.iter().find(|member| member.id == entry.member) else {
                continue;
            };
            let promise = admitted.entry(entry.member.clone()).or_default();
            if !matches!(key, Key::Survival(_)) {
                promise.logical = promise.logical.saturating_add(entry.bytes);
            }
            promise.physical = promise
                .physical
                .saturating_add(member.put_cost(entry.bytes));
        }
        Admitted(admitted)
    }

    pub(super) fn member_of(&self, key: Key) -> Option<&str> {
        self.entries.get(&key).map(|entry| entry.member.as_str())
    }

    /// Record a claim of `kind`: [`Key::Claim`] or [`Key::Survival`].
    pub(super) fn insert_claim(&mut self, kind: fn(u64) -> Key, member: String, bytes: u64) -> Key {
        let key = kind(self.next_claim);
        self.next_claim += 1;
        self.entries.insert(
            key,
            Entry {
                member,
                bytes,
                placing: false,
            },
        );
        key
    }

    /// Lease `segment` for `bytes` on `member` unless it is already leased.
    pub(super) fn insert_lease(&mut self, segment: Segid, member: String, bytes: u64) {
        self.entries.entry(Key::Segment(segment)).or_insert(Entry {
            member,
            bytes,
            placing: false,
        });
    }

    /// Count `bytes` on `member` for the reservation `upload` until its
    /// catalog row is visible.
    pub(super) fn insert_reserving(&mut self, upload: Uuid, member: String, bytes: u64) {
        self.entries.insert(
            Key::Reserving(upload),
            Entry {
                member,
                bytes,
                placing: false,
            },
        );
    }

    pub(super) fn set_placing(&mut self, segment: Segid, placing: bool) {
        if let Some(entry) = self.entries.get_mut(&Key::Segment(segment)) {
            entry.placing = placing;
        }
    }

    pub(super) fn remove(&mut self, key: Key) {
        self.entries.remove(&key);
    }

    /// Grow `segment`'s lease to `total` bytes, paid from `claim`. The lease
    /// is created on the claim's member. `Err` means the segment is leased on
    /// another member, so these bytes must start a new segment instead.
    fn fund(&mut self, claim: Key, segment: Segid, total: u64) -> Result<(), MemberMismatch> {
        let member = match self.entries.get(&claim) {
            Some(entry) => entry.member.clone(),
            None => return Err(MemberMismatch),
        };
        let grown = match self.entries.get_mut(&Key::Segment(segment)) {
            Some(lease) if lease.member != member => return Err(MemberMismatch),
            Some(lease) => {
                let grown = total.saturating_sub(lease.bytes);
                lease.bytes = lease.bytes.max(total);
                grown
            }
            None => {
                self.entries.insert(
                    Key::Segment(segment),
                    Entry {
                        member,
                        bytes: total,
                        placing: false,
                    },
                );
                total
            }
        };
        let claim = self.entries.get_mut(&claim).expect("claim checked above");
        if grown > claim.bytes {
            tracing::error!(
                grown,
                claimed = claim.bytes,
                "segment lease grew past the bound its WRITE claimed"
            );
        }
        claim.bytes = claim.bytes.saturating_sub(grown);
        Ok(())
    }
}

/// The open segment is leased on a different member than the claim.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MemberMismatch;

/// Room one WRITE holds on one member until its frames are in a segment, or
/// one survival PUT holds until it returns. Dropping it returns whatever the
/// segment leases did not take.
#[derive(Debug)]
pub(crate) struct Claim {
    ledger: Arc<Mutex<Ledger>>,
    key: Key,
}

impl Claim {
    pub(super) fn new(ledger: Arc<Mutex<Ledger>>, key: Key) -> Self {
        Self { ledger, key }
    }

    /// The member this claim holds room on.
    pub(crate) fn member(&self) -> String {
        self.ledger
            .lock()
            .unwrap()
            .member_of(self.key)
            .expect("a live claim has a ledger entry")
            .to_owned()
    }

    /// Grow `segment`'s lease to `total` bytes from this claim.
    pub(crate) fn fund(&self, segment: Segid, total: u64) -> Result<(), MemberMismatch> {
        self.ledger.lock().unwrap().fund(self.key, segment, total)
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.ledger.lock().unwrap().remove(self.key);
    }
}
