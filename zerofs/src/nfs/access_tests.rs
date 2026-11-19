use super::*;
use crate::fs::types::SetMode;
use zerofs_nfsserve::nfs::ACCESS3_ALL;

fn auth(uid: u32, gid: u32, gids: &[u32]) -> NfsAuthContext {
    NfsAuthContext {
        uid,
        gid,
        gids: gids.to_vec(),
    }
}

async fn fixture() -> (Arc<ZeroFS>, Credentials, fileid3) {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let owner = Credentials::from_auth_context(&AuthContext {
        uid: 1000,
        gid: 1000,
        ..AuthContext::default()
    });
    let (file, _) = fs
        .create(
            &owner,
            0,
            b"access-file",
            &SetAttributes {
                mode: SetMode::Set(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (fs, owner, file)
}

#[tokio::test]
async fn access_honors_modes_shared_identity_and_requested_rights() {
    let (fs, _, file) = fixture().await;
    let adapter = NFSAdapter::new(Arc::clone(&fs));
    let owner = auth(1000, 1000, &[]);
    let stranger = auth(2000, 2000, &[]);
    let writable = ACCESS3_READ | ACCESS3_MODIFY | ACCESS3_EXTEND;
    assert_eq!(
        adapter.access(&owner, file, u32::MAX).await.unwrap().access,
        writable
    );
    assert_eq!(
        adapter
            .access(&stranger, file, u32::MAX)
            .await
            .unwrap()
            .access,
        0
    );
    let requested = adapter.access(&owner, file, ACCESS3_READ).await.unwrap();
    assert_eq!(requested.access, ACCESS3_READ);
    assert_eq!(requested.attributes.fileid, file);
    assert_eq!(requested.attributes.mode, 0o600);
    assert_eq!(adapter.access(&owner, file, 0).await.unwrap().access, 0);
    // Root's file-execute exception still applies, and file LOOKUP/DELETE
    // are not meaningful rights in an ACCESS response.
    assert_eq!(
        adapter
            .access(&auth(0, 0, &[]), file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        writable
    );
    assert!(matches!(
        adapter.access(&owner, u64::MAX, ACCESS3_ALL).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
    let shared = adapter.with_shared_identity(Some(NfsSharedIdentity {
        uid: 1000,
        gid: 1000,
    }));
    assert_eq!(
        shared
            .access(&stranger, file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        writable
    );
}
