pub mod error;
pub mod handler;
pub mod server;

pub use handler::NbdExportGates;
pub use server::NBDServer;

pub(crate) const NBD_STRIPE_MARKER: &str = ".zerofs-nbd-stripe-v1";
pub(crate) const NBD_STRIPE_MIN_BYTES: u64 = 4096;
pub(crate) const NBD_STRIPE_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const NBD_STRIPE_MAX_MEMBERS: usize = 32;

#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
pub(crate) struct StripeManifest {
    pub(crate) version: u32,
    pub(crate) stripe_bytes: u64,
    pub(crate) members: Vec<String>,
}

fn out_of_bounds(offset: u64, length: u32, device_size: u64) -> bool {
    offset
        .checked_add(length as u64)
        .is_none_or(|end| end > device_size)
}

#[cfg(test)]
mod tests {
    use super::out_of_bounds;

    #[test]
    fn bounds_check_handles_edges_and_overflow() {
        assert!(!out_of_bounds(0, 512, 1024));
        assert!(!out_of_bounds(512, 512, 1024));
        assert!(out_of_bounds(513, 512, 1024));
        assert!(out_of_bounds(1024, 1, 1024));
        assert!(!out_of_bounds(1024, 0, 1024));
        assert!(out_of_bounds(u64::MAX, 1, u64::MAX));
        assert!(out_of_bounds(u64::MAX - 1, u32::MAX, u64::MAX));
    }
}
