use crate::fs::name_mode::NameMode;
use crate::task::spawn_blocking_named;
use anyhow::{Context, Result, bail};
use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHasher, SaltString},
};
use bytes::Bytes;
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use rand::{RngCore, thread_rng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ARGON2_MEM_COST: u32 = 65536;
const ARGON2_TIME_COST: u32 = 3;
const ARGON2_PARALLELISM: u32 = 4;

/// Filename for the wrapped encryption key in object store
const WRAPPED_KEY_FILENAME: &str = "zerofs.key";

#[derive(Serialize, Deserialize, Debug)]
pub struct WrappedDataKey {
    /// Salt for Argon2 password derivation
    pub salt: String,
    /// Nonce for XChaCha20-Poly1305 encryption of the DEK
    pub nonce: [u8; 24],
    /// Encrypted data encryption key
    pub wrapped_dek: Vec<u8>,
    /// Version for future compatibility
    pub version: u32,
}

// The legacy decoder reads these bytes as a u64 string length and fails.
const KEY_ENVELOPE_MAGIC: [u8; 8] = [0xff; 8];

#[derive(Debug)]
pub struct KeyEnvelope {
    wrapped_key: WrappedDataKey,
    version: u8,
    name_mode: NameMode,
}

impl KeyEnvelope {
    fn new(wrapped_key: WrappedDataKey, name_mode: NameMode) -> Self {
        Self {
            wrapped_key,
            version: if name_mode == NameMode::Sensitive {
                1
            } else {
                2
            },
            name_mode,
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let key = bincode::serialize(&self.wrapped_key)?;
        if self.version == 1 {
            return Ok(key);
        }
        let mut data = KEY_ENVELOPE_MAGIC.to_vec();
        data.push(self.version);
        data.push(match self.name_mode {
            NameMode::Sensitive => 0,
            NameMode::Insensitive => 1,
        });
        data.extend_from_slice(&key);
        Ok(data)
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if !data.starts_with(&KEY_ENVELOPE_MAGIC) {
            return Ok(Self::new(bincode::deserialize(data)?, NameMode::Sensitive));
        }
        let (&version, rest) = data[8..]
            .split_first()
            .context("missing key envelope version")?;
        if version != 2 {
            bail!("unknown key envelope version {version}");
        }
        let (&mode, key) = rest
            .split_first()
            .context("missing key envelope name mode")?;
        let name_mode = match mode {
            0 => NameMode::Sensitive,
            1 => NameMode::Insensitive,
            _ => bail!("unknown filesystem name mode {mode} in key envelope"),
        };
        Ok(Self {
            wrapped_key: bincode::deserialize(key)?,
            version,
            name_mode,
        })
    }

    fn check_mode(&self, configured: NameMode) -> Result<()> {
        if self.name_mode != configured {
            bail!(
                "filesystem name mode mismatch: stored {:?}, configured {configured:?}; converting an existing filesystem is not supported",
                self.name_mode
            );
        }
        Ok(())
    }
}

pub struct KeyManager {
    argon2: Argon2<'static>,
}

impl KeyManager {
    pub fn new() -> Self {
        let params = Params::new(ARGON2_MEM_COST, ARGON2_TIME_COST, ARGON2_PARALLELISM, None)
            .expect("Valid Argon2 parameters");

        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        Self { argon2 }
    }

    /// Derive a key encryption key (KEK) from a password
    fn derive_kek(&self, password: &str, salt: &SaltString) -> Result<[u8; 32]> {
        let password_hash = self
            .argon2
            .hash_password(password.as_bytes(), salt)
            .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?;

        // Extract the hash bytes
        let hash_bytes = password_hash
            .hash
            .ok_or_else(|| anyhow::anyhow!("No hash in password hash"))?;

        let mut kek = [0u8; 32];
        kek.copy_from_slice(&hash_bytes.as_bytes()[..32]);
        Ok(kek)
    }

    /// Generate a new data encryption key and wrap it with a password
    pub fn generate_and_wrap_key(&self, password: &str) -> Result<(WrappedDataKey, [u8; 32])> {
        // Generate random DEK
        let mut dek = [0u8; 32];
        thread_rng().fill_bytes(&mut dek);

        // Generate random salt for password KDF
        let salt = SaltString::generate(&mut thread_rng());

        // Derive KEK from password
        let kek = self.derive_kek(password, &salt)?;

        // Generate random nonce for wrapping
        let mut nonce_bytes = [0u8; 24];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        // Encrypt DEK with KEK
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&kek));
        let wrapped_dek = cipher
            .encrypt(nonce, dek.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to wrap DEK: {}", e))?;

        let wrapped_key = WrappedDataKey {
            salt: salt.to_string(),
            nonce: nonce_bytes,
            wrapped_dek,
            version: 1,
        };

        Ok((wrapped_key, dek))
    }

    /// Unwrap a data encryption key using a password
    pub fn unwrap_key(&self, password: &str, wrapped_key: &WrappedDataKey) -> Result<[u8; 32]> {
        if wrapped_key.version != 1 {
            return Err(anyhow::anyhow!(
                "Unsupported wrapped key version: {}",
                wrapped_key.version
            ));
        }

        // Parse salt
        let salt = SaltString::from_b64(&wrapped_key.salt)
            .map_err(|e| anyhow::anyhow!("Invalid salt: {}", e))?;

        // Derive KEK from password
        let kek = self.derive_kek(password, &salt)?;

        // Decrypt DEK with KEK
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&kek));
        let nonce = XNonce::from_slice(&wrapped_key.nonce);

        let dek_vec = cipher
            .decrypt(nonce, wrapped_key.wrapped_dek.as_ref())
            .map_err(|_| {
                anyhow::anyhow!("Failed to unwrap DEK: Invalid password or corrupted key")
            })?;

        let mut dek = [0u8; 32];
        dek.copy_from_slice(&dek_vec);
        Ok(dek)
    }

    /// Re-wrap a DEK with a new password (for password changes)
    pub fn rewrap_key(
        &self,
        old_password: &str,
        new_password: &str,
        wrapped_key: &WrappedDataKey,
    ) -> Result<WrappedDataKey> {
        // First unwrap with old password
        let dek = self.unwrap_key(old_password, wrapped_key)?;

        // Generate new salt and wrap with new password
        let salt = SaltString::generate(&mut thread_rng());
        let kek = self.derive_kek(new_password, &salt)?;

        let mut nonce_bytes = [0u8; 24];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        let cipher = XChaCha20Poly1305::new(Key::from_slice(&kek));
        let wrapped_dek = cipher
            .encrypt(nonce, dek.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to rewrap DEK: {}", e))?;

        Ok(WrappedDataKey {
            salt: salt.to_string(),
            nonce: nonce_bytes,
            wrapped_dek,
            version: 1,
        })
    }
}

/// Get the path for the wrapped key file in object store
fn wrapped_key_path(db_path: &Path) -> Path {
    let mut path = db_path.clone();
    path = path.join(WRAPPED_KEY_FILENAME);
    path
}

/// Load wrapped key from object store
pub async fn load_wrapped_key_from_object_store(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
) -> Result<Option<KeyEnvelope>> {
    let key_path = wrapped_key_path(db_path);

    match object_store.get(&key_path).await {
        Ok(result) => {
            let data = result.bytes().await?;
            let wrapped_key = KeyEnvelope::decode(&data)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize wrapped key: {}", e))?;
            Ok(Some(wrapped_key))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("Failed to load wrapped key: {}", e)),
    }
}

/// Save wrapped key to object store
pub async fn save_wrapped_key_to_object_store(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    wrapped_key: &KeyEnvelope,
) -> Result<()> {
    let key_path = wrapped_key_path(db_path);

    let serialized = wrapped_key
        .encode()
        .context("Failed to serialize wrapped key")?;

    object_store
        .put(&key_path, PutPayload::from(Bytes::from(serialized)))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to save wrapped key: {}", e))?;

    Ok(())
}

/// Load the existing key for a case-sensitive database without initializing state.
///
/// Keep the original API for callers that do not select a name mode. It must
/// still reject an insensitive envelope rather than silently opening that store.
#[allow(dead_code)] // Retained for crate callers that use the default name mode.
pub(crate) async fn load_existing_encryption_key(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    password: &str,
) -> Result<[u8; 32]> {
    load_existing_encryption_key_with_mode(object_store, db_path, password, NameMode::Sensitive)
        .await
}

/// Load the existing encryption key without initializing remote state.
pub(crate) async fn load_existing_encryption_key_with_mode(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    password: &str,
    name_mode: NameMode,
) -> Result<[u8; 32]> {
    let wrapped_key = load_wrapped_key_from_object_store(object_store, db_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing existing encryption key at {db_path}"))?;
    wrapped_key.check_mode(name_mode)?;
    unwrap_key_blocking(password, wrapped_key.wrapped_key).await
}

/// Load or initialize the key for a case-sensitive database.
///
/// This preserves the original API and on-disk format for callers that do not
/// select a name mode. An existing insensitive envelope is rejected.
#[allow(dead_code)] // Retained for crate callers that use the default name mode.
pub async fn load_or_init_encryption_key(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    password: &str,
    read_only: bool,
) -> Result<[u8; 32]> {
    load_or_init_encryption_key_with_mode(
        object_store,
        db_path,
        password,
        NameMode::Sensitive,
        read_only,
    )
    .await
}

/// Load or initialize the encryption key from the object store with an explicit name mode.
///
/// This loads the wrapped encryption key from the object store and unwraps it
/// using the provided password. If no key exists, a new one is generated and
/// stored.
pub async fn load_or_init_encryption_key_with_mode(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    password: &str,
    name_mode: NameMode,
    read_only: bool,
) -> Result<[u8; 32]> {
    if let Some(wrapped_key) = load_wrapped_key_from_object_store(object_store, db_path).await? {
        wrapped_key.check_mode(name_mode)?;
        return unwrap_key_blocking(password, wrapped_key.wrapped_key).await;
    }

    if read_only {
        return Err(anyhow::anyhow!(
            "Cannot initialize encryption key in read-only mode. Please initialize the database in read-write mode first."
        ));
    }

    // No key yet: generate a candidate, but commit it with a conditional create so
    // only ONE concurrent initializer wins. Nodes sharing a store MUST share one key
    // (else blocks written by one node can't be decrypted by the other); if we lose
    // the race, adopt the winner's key instead of keeping our own.
    let pw = password.to_string();
    let (wrapped_key, dek) = spawn_blocking_named("argon2-generate", move || {
        KeyManager::new().generate_and_wrap_key(&pw)
    })
    .map_err(|e| anyhow::anyhow!("Failed to spawn task: {}", e))?
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

    let wrapped_key = KeyEnvelope::new(wrapped_key, name_mode);
    if save_wrapped_key_if_absent(object_store, db_path, &wrapped_key).await? {
        return Ok(dek);
    }

    // Lost the init race: another node wrote its key first. Load and use it so every
    // node that shares this store converges on a single key.
    let wrapped_key = load_wrapped_key_from_object_store(object_store, db_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("wrapped key disappeared right after a concurrent init"))?;
    wrapped_key.check_mode(name_mode)?;
    unwrap_key_blocking(password, wrapped_key.wrapped_key).await
}

/// Unwrap a wrapped DEK off the runtime (argon2 is CPU-heavy).
async fn unwrap_key_blocking(password: &str, wrapped_key: WrappedDataKey) -> Result<[u8; 32]> {
    let password = password.to_string();
    spawn_blocking_named("argon2-unwrap", move || {
        KeyManager::new().unwrap_key(&password, &wrapped_key)
    })
    .map_err(|e| anyhow::anyhow!("Failed to spawn task: {}", e))?
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
}

/// Persist the wrapped key only if absent (atomic create). Ok(true) = we wrote it,
/// Ok(false) = another node beat us to it (so the caller must adopt that key).
async fn save_wrapped_key_if_absent(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    wrapped_key: &KeyEnvelope,
) -> Result<bool> {
    let key_path = wrapped_key_path(db_path);
    let serialized = wrapped_key
        .encode()
        .context("Failed to serialize wrapped key")?;
    match object_store
        .put_opts(
            &key_path,
            PutPayload::from(Bytes::from(serialized)),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(e) => Err(anyhow::anyhow!("Failed to save wrapped key: {}", e)),
    }
}

/// Change the password used to encrypt the DEK
pub async fn change_encryption_password(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    old_password: &str,
    new_password: &str,
) -> Result<()> {
    let key_manager = KeyManager::new();

    let wrapped_key = load_wrapped_key_from_object_store(object_store, db_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No encryption key found"))?;

    let old_password = old_password.to_string();
    let new_password = new_password.to_string();
    let new_wrapped_key = spawn_blocking_named("argon2-rewrap", move || {
        let mut envelope = wrapped_key;
        envelope.wrapped_key =
            key_manager.rewrap_key(&old_password, &new_password, &envelope.wrapped_key)?;
        Ok::<_, anyhow::Error>(envelope)
    })
    .map_err(|e| anyhow::anyhow!("Failed to spawn task: {}", e))?
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

    save_wrapped_key_to_object_store(object_store, db_path, &new_wrapped_key).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_key() -> WrappedDataKey {
        WrappedDataKey {
            salt: "salt".into(),
            nonce: [7; 24],
            wrapped_dek: vec![9; 48],
            version: 1,
        }
    }

    #[test]
    fn key_envelopes_fence_insensitive_and_preserve_legacy_bytes() {
        let legacy = bincode::serialize(&example_key()).unwrap();
        let sensitive = KeyEnvelope::new(example_key(), NameMode::Sensitive)
            .encode()
            .unwrap();
        assert_eq!(legacy, sensitive);
        assert_eq!(
            KeyEnvelope::decode(&legacy).unwrap().name_mode,
            NameMode::Sensitive
        );
        let insensitive = KeyEnvelope::new(example_key(), NameMode::Insensitive)
            .encode()
            .unwrap();
        assert_eq!(
            &insensitive[..10],
            &[255, 255, 255, 255, 255, 255, 255, 255, 2, 1]
        );
        assert!(bincode::deserialize::<WrappedDataKey>(&insensitive).is_err());
        let decoded = KeyEnvelope::decode(&insensitive).unwrap();
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.name_mode, NameMode::Insensitive);
        assert_eq!(decoded.encode().unwrap(), insensitive);
    }

    #[tokio::test]
    async fn legacy_key_loading_api_keeps_sensitive_defaults_and_rejects_insensitive() {
        use crate::fault_store::FaultStore;
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        let path = Path::from("legacy-api");
        let key = load_or_init_encryption_key(&store, &path, "pw", false)
            .await
            .unwrap();
        assert_eq!(
            load_or_init_encryption_key(&store, &path, "pw", true)
                .await
                .unwrap(),
            key
        );
        assert_eq!(
            load_existing_encryption_key(&store, &path, "pw")
                .await
                .unwrap(),
            key
        );

        let envelope = load_wrapped_key_from_object_store(&store, &path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(envelope.version, 1);
        assert_eq!(envelope.name_mode, NameMode::Sensitive);
        let insensitive = KeyEnvelope::new(envelope.wrapped_key, NameMode::Insensitive);
        save_wrapped_key_to_object_store(&store, &path, &insensitive)
            .await
            .unwrap();
        let puts = controls.put_count();

        for read_only in [false, true] {
            let error = load_or_init_encryption_key(&store, &path, "pw", read_only)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("name mode mismatch"));
        }
        let error = load_existing_encryption_key(&store, &path, "pw")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("name mode mismatch"));
        assert_eq!(controls.put_count(), puts);
    }

    #[tokio::test]
    async fn invalid_envelopes_never_initialize_a_replacement() {
        use crate::fault_store::FaultStore;
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        let path = Path::from("invalid");
        let valid = KeyEnvelope::new(example_key(), NameMode::Insensitive)
            .encode()
            .unwrap();
        let mut unknown_version = valid.clone();
        unknown_version[8] = 3;
        let mut unknown_mode = valid.clone();
        unknown_mode[9] = 2;
        for bytes in [
            valid[..8].to_vec(),
            valid[..9].to_vec(),
            valid[..10].to_vec(),
            unknown_version,
            unknown_mode,
            b"corrupt".to_vec(),
        ] {
            store
                .put(&wrapped_key_path(&path), bytes.clone().into())
                .await
                .unwrap();
            let puts = controls.put_count();
            for read_only in [false, true] {
                for mode in [NameMode::Sensitive, NameMode::Insensitive] {
                    assert!(
                        load_or_init_encryption_key_with_mode(&store, &path, "pw", mode, read_only)
                            .await
                            .is_err()
                    );
                }
            }
            assert_eq!(controls.put_count(), puts);
            assert_eq!(
                store
                    .get(&wrapped_key_path(&path))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .as_ref(),
                bytes
            );
        }
    }

    #[tokio::test]
    async fn concurrent_different_modes_refuse_the_loser() {
        use crate::fault_store::FaultStore;
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        controls.block_puts();
        let mut tasks = Vec::new();
        for mode in [NameMode::Sensitive, NameMode::Insensitive] {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                load_or_init_encryption_key_with_mode(
                    &store,
                    &Path::from("race"),
                    "pw",
                    mode,
                    false,
                )
                .await
            }));
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while controls.put_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        controls.release_puts();
        let a = tasks.remove(0).await.unwrap();
        let b = tasks.remove(0).await.unwrap();
        assert_ne!(a.is_ok(), b.is_ok());
        let winner = load_wrapped_key_from_object_store(&store, &Path::from("race"))
            .await
            .unwrap()
            .unwrap();
        for (mode, result) in [(NameMode::Sensitive, a), (NameMode::Insensitive, b)] {
            if mode == winner.name_mode {
                let dek = result.unwrap();
                assert_eq!(
                    load_or_init_encryption_key_with_mode(
                        &store,
                        &Path::from("race"),
                        "pw",
                        mode,
                        true
                    )
                    .await
                    .unwrap(),
                    dek
                );
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("name mode mismatch")
                );
            }
        }
    }

    #[tokio::test]
    async fn password_rotation_preserves_each_envelope_format_and_mode() {
        for (version, mode) in [
            (1, NameMode::Sensitive),
            (2, NameMode::Insensitive),
            (2, NameMode::Sensitive),
        ] {
            let store = store();
            let path = Path::from("rotation");
            let (wrapped_key, dek) = KeyManager::new().generate_and_wrap_key("old").unwrap();
            let envelope = KeyEnvelope {
                wrapped_key,
                version,
                name_mode: mode,
            };
            save_wrapped_key_to_object_store(&store, &path, &envelope)
                .await
                .unwrap();
            change_encryption_password(&store, &path, "old", "new")
                .await
                .unwrap();
            let rotated = load_wrapped_key_from_object_store(&store, &path)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(rotated.version, version);
            assert_eq!(rotated.name_mode, mode);
            assert_eq!(
                KeyManager::new()
                    .unwrap_key("new", &rotated.wrapped_key)
                    .unwrap(),
                dek
            );
            assert!(
                KeyManager::new()
                    .unwrap_key("old", &rotated.wrapped_key)
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn read_only_and_mode_mismatch_never_write() {
        use crate::fault_store::FaultStore;
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        let path = Path::from("modes");
        for mode in [NameMode::Sensitive, NameMode::Insensitive] {
            assert!(
                load_or_init_encryption_key_with_mode(&store, &path, "pw", mode, true)
                    .await
                    .is_err()
            );
        }
        assert_eq!(controls.put_count(), 0);
        for stored in [NameMode::Sensitive, NameMode::Insensitive] {
            let envelope = KeyEnvelope::new(example_key(), stored);
            save_wrapped_key_to_object_store(&store, &path, &envelope)
                .await
                .unwrap();
            let puts = controls.put_count();
            let wrong = if stored == NameMode::Sensitive {
                NameMode::Insensitive
            } else {
                NameMode::Sensitive
            };
            for read_only in [false, true] {
                let error =
                    load_or_init_encryption_key_with_mode(&store, &path, "pw", wrong, read_only)
                        .await
                        .unwrap_err();
                assert!(error.to_string().contains("name mode mismatch"));
            }
            assert_eq!(controls.put_count(), puts);
        }
    }

    #[test]
    fn test_key_wrap_unwrap() {
        let key_manager = KeyManager::new();
        let password = "test_password_123!";

        // Generate and wrap key
        let (wrapped_key, original_dek) = key_manager
            .generate_and_wrap_key(password)
            .expect("Failed to generate and wrap key");

        // Unwrap key
        let unwrapped_dek = key_manager
            .unwrap_key(password, &wrapped_key)
            .expect("Failed to unwrap key");

        assert_eq!(original_dek, unwrapped_dek);
    }

    #[test]
    fn test_wrong_password() {
        let key_manager = KeyManager::new();
        let password = "correct_password";
        let wrong_password = "wrong_password";

        let (wrapped_key, _) = key_manager
            .generate_and_wrap_key(password)
            .expect("Failed to generate and wrap key");

        // Should fail with wrong password
        assert!(
            key_manager
                .unwrap_key(wrong_password, &wrapped_key)
                .is_err()
        );
    }

    #[test]
    fn test_password_change() {
        let key_manager = KeyManager::new();
        let old_password = "old_password";
        let new_password = "new_password";

        let (wrapped_key, original_dek) = key_manager
            .generate_and_wrap_key(old_password)
            .expect("Failed to generate and wrap key");

        // Change password
        let new_wrapped_key = key_manager
            .rewrap_key(old_password, new_password, &wrapped_key)
            .expect("Failed to rewrap key");

        // Old password should not work
        assert!(
            key_manager
                .unwrap_key(old_password, &new_wrapped_key)
                .is_err()
        );

        // New password should work
        let unwrapped_dek = key_manager
            .unwrap_key(new_password, &new_wrapped_key)
            .expect("Failed to unwrap with new password");

        assert_eq!(original_dek, unwrapped_dek);
    }

    /// Two nodes initializing the same store concurrently must converge on ONE key.
    /// Before the conditional-create init they each generated + kept their own key,
    /// so blocks one wrote couldn't be decrypted by the other.
    #[tokio::test]
    async fn concurrent_init_converges_on_one_key() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db_path = Path::from("data");
        let pw = "shared-cluster-password";

        let (ka, kb) = tokio::join!(
            load_or_init_encryption_key(&store, &db_path, pw, false),
            load_or_init_encryption_key(&store, &db_path, pw, false),
        );
        let ka = ka.expect("node A init");
        let kb = kb.expect("node B init");
        assert_eq!(ka, kb, "concurrent initializers must converge on one key");

        // A later loader gets that same committed key.
        let kc = load_or_init_encryption_key(&store, &db_path, pw, false)
            .await
            .expect("later load");
        assert_eq!(ka, kc, "a later load must return the committed key");
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    // Password rotation must preserve the DEK (data stays decryptable) and the old
    // password must stop working.
    #[tokio::test]
    async fn change_password_rotates_without_losing_the_dek() {
        let store = store();
        let db_path = Path::from("data");
        let (old_pw, new_pw) = ("old-pw-123", "new-pw-456");

        let dek = load_or_init_encryption_key(&store, &db_path, old_pw, false)
            .await
            .unwrap();

        change_encryption_password(&store, &db_path, old_pw, new_pw)
            .await
            .unwrap();

        assert!(
            load_or_init_encryption_key(&store, &db_path, old_pw, false)
                .await
                .is_err(),
            "the old password must stop unwrapping the key"
        );
        let dek2 = load_or_init_encryption_key(&store, &db_path, new_pw, false)
            .await
            .unwrap();
        assert_eq!(dek, dek2, "rotation must preserve the data key");
    }

    #[tokio::test]
    async fn change_password_without_a_key_errors() {
        let store = store();
        assert!(
            change_encryption_password(&store, &Path::from("data"), "a", "b")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn read_only_init_without_a_key_errors() {
        let store = store();
        let err = load_or_init_encryption_key(&store, &Path::from("data"), "pw", true)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("read-only"), "got: {err:#}");
    }

    #[tokio::test]
    async fn reload_with_wrong_password_errors() {
        let store = store();
        let db_path = Path::from("data");
        load_or_init_encryption_key(&store, &db_path, "right", false)
            .await
            .unwrap();
        assert!(
            load_or_init_encryption_key(&store, &db_path, "wrong", false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn load_wrapped_key_rejects_corrupt_bytes() {
        let store = store();
        let db_path = Path::from("data");
        store
            .put(
                &db_path.clone().join(WRAPPED_KEY_FILENAME),
                PutPayload::from(Bytes::from_static(b"not a bincode wrapped key")),
            )
            .await
            .unwrap();
        assert!(
            load_wrapped_key_from_object_store(&store, &db_path)
                .await
                .is_err()
        );
    }

    #[test]
    fn unwrap_rejects_an_unsupported_version() {
        let km = KeyManager::new();
        let (mut wrapped, _) = km.generate_and_wrap_key("pw").unwrap();
        wrapped.version = 2;
        let err = km.unwrap_key("pw", &wrapped).unwrap_err();
        assert!(format!("{err:#}").contains("version"), "got: {err:#}");
    }

    #[test]
    fn unwrap_rejects_a_corrupt_salt() {
        let km = KeyManager::new();
        let (mut wrapped, _) = km.generate_and_wrap_key("pw").unwrap();
        wrapped.salt = "###not-valid-b64###".to_string();
        assert!(km.unwrap_key("pw", &wrapped).is_err());
    }
}
