/// Generated types for the CSI spec (vendored proto/csi.proto, v1.12.0).
#[allow(
    clippy::double_must_use,
    reason = "tonic generates async_trait methods with redundant must_use"
)]
pub mod csi {
    pub mod v1 {
        tonic::include_proto!("csi.v1");
    }
}

/// Generated client for the ZeroFS admin RPC surface (vendored
/// proto/admin.proto). Client only; the server lives in the zerofs crate.
pub mod admin {
    tonic::include_proto!("zerofs.admin");
}
