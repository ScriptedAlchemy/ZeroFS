//! Shared fixture for the filesystem tests.

use crate::fs::permissions::Credentials;
use crate::fs::types::AuthContext;

pub(super) fn test_creds() -> Credentials {
    Credentials::from_auth_context(&(&crate::test_helpers::test_helpers_mod::test_auth()).into())
}

/// [`test_creds`] in `AuthContext` form, for the filesystem entry points that
/// take one. Round-tripping keeps the two identities in step.
pub(super) fn test_auth() -> AuthContext {
    (&test_creds()).into()
}
