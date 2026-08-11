use crate::config::Settings;
use crate::key_management;
use crate::storage_class_object_store::with_storage_class;
use slatedb::object_store::path::Path;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum PasswordError {
    #[error("Password cannot be empty")]
    EmptyPassword,
    #[error("Password must be at least 8 characters long")]
    TooShort,
    #[error("Please choose a secure password, not 'CHANGEME'")]
    DefaultPassword,
    #[error("Current password is still the default. Please update your config file first")]
    CurrentPasswordIsDefault,
    #[error("Failed to change encryption password: {0}")]
    EncryptionError(String),
    #[error("{0}")]
    Other(String),
}

pub fn validate_password(password: &str) -> Result<(), PasswordError> {
    if password.is_empty() {
        return Err(PasswordError::EmptyPassword);
    }
    if password.len() < 8 {
        return Err(PasswordError::TooShort);
    }
    if password == "CHANGEME" {
        return Err(PasswordError::DefaultPassword);
    }
    Ok(())
}

/// Change the encryption password.
///
/// The encryption key is stored in object store (not in SlateDB), so we don't need
/// to open the database to change the password.
pub async fn change_password(
    settings: &Settings,
    new_password: String,
) -> Result<(), PasswordError> {
    let current_password = &settings.storage.encryption_password;

    if current_password == "CHANGEME" {
        return Err(PasswordError::CurrentPasswordIsDefault);
    }
    validate_password(&new_password)?;

    let env_vars = settings.cloud_provider_env_vars();

    let (object_store, path_from_url, sftp_pool) =
        crate::parse_object_store::parse_url_opts_with_sftp(
            &settings
                .storage
                .url
                .parse::<url::Url>()
                .map_err(|e| PasswordError::Other(e.to_string()))?,
            env_vars,
            settings.sftp.as_ref(),
        )
        .await
        .map_err(|e| PasswordError::Other(e.to_string()))?;

    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );
    let db_path = Path::from(path_from_url.to_string());

    let change = key_management::change_encryption_password(
        &object_store,
        &db_path,
        current_password,
        &new_password,
    )
    .await;
    let shutdown = match sftp_pool {
        Some(pool) => pool.shutdown().await,
        None => Ok(()),
    };
    match (change, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(change), Ok(())) => Err(PasswordError::EncryptionError(change.to_string())),
        (Ok(()), Err(shutdown)) => Err(PasswordError::Other(format!(
            "encryption password changed, but SFTP shutdown failed: {shutdown}"
        ))),
        (Err(change), Err(shutdown)) => Err(PasswordError::EncryptionError(format!(
            "{change}; SFTP shutdown also failed: {shutdown}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_validate_password() {
        assert!(validate_password("").is_err());
        assert!(validate_password("short").is_err());
        assert!(validate_password("CHANGEME").is_err());
        assert!(validate_password("goodpassword123").is_ok());
    }

    #[test]
    fn settings_reject_invalid_sftp_url_before_password_change() {
        let config = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "sftp:///data"
encryption_password = "current-password"

[servers]

[sftp]
known_hosts = "/tmp/known_hosts"
"#;
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config).unwrap();
        let error = Settings::from_file(temp_file.path())
            .unwrap_err()
            .to_string();

        assert!(error.contains("must include a host"), "got: {error}");
    }
}
