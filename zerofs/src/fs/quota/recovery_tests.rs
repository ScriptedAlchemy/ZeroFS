use super::*;

#[test]
fn unknown_persistent_usage_cannot_admit_growth_as_if_the_filesystem_were_empty() {
    let quota = LogicalQuota::from_known_usage(100, None);
    assert!(matches!(quota.reserve(1), Err(FsError::IoError)));
    assert!(quota.is_poisoned());
    assert_eq!(quota.pending_bytes(), 0);
    quota.release_committed(50);
    assert!(matches!(quota.reserve(1), Err(FsError::IoError)));
}

#[test]
fn known_usage_still_admits_only_the_remaining_capacity() {
    let quota = LogicalQuota::from_known_usage(100, Some(90));
    let reserved = quota.reserve(10).unwrap();
    assert!(matches!(quota.reserve(1), Err(FsError::NoSpace)));
    drop(reserved);
    assert!(quota.reserve(10).is_ok());
}
