use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::sync::Arc;
use uuid::Uuid;

const BUCKET_ID_MARKER: &str = ".zerofs_bucket_id";

/// Manages bucket identity to ensure cache isolation between different bucket instances
#[derive(Debug, Clone)]
pub struct BucketIdentity {
    id: Uuid,
}

impl BucketIdentity {
    /// Load the durable identity for an initialized filesystem namespace.
    ///
    /// Unlike [`Self::get_or_create`], this never creates a missing marker. A
    /// serving path that uses the identity as an external authority must fail
    /// closed rather than silently naming a different filesystem.
    pub async fn load(object_store: &Arc<dyn ObjectStore>, db_path: &str) -> anyhow::Result<Self> {
        let marker_path = Path::from(db_path).join(BUCKET_ID_MARKER);
        let result = object_store.get(&marker_path).await.map_err(|error| {
            if matches!(error, slatedb::object_store::Error::NotFound { .. }) {
                anyhow::anyhow!("missing durable bucket identity at {marker_path}")
            } else {
                anyhow::anyhow!(
                    "failed to read durable bucket identity at {marker_path}: {error:#}"
                )
            }
        })?;
        let bytes = result.bytes().await.map_err(|error| {
            anyhow::anyhow!("failed to read durable bucket identity at {marker_path}: {error:#}")
        })?;
        let value = std::str::from_utf8(&bytes).map_err(|error| {
            anyhow::anyhow!("invalid durable bucket identity at {marker_path}: {error}")
        })?;
        let id = Uuid::parse_str(value.trim()).map_err(|error| {
            anyhow::anyhow!("invalid durable bucket identity at {marker_path}: {error}")
        })?;
        Ok(Self { id })
    }

    /// Gets or creates a unique bucket ID for the given bucket
    /// This ID persists with the bucket and changes if the bucket is recreated
    pub async fn get_or_create(
        object_store: &Arc<dyn ObjectStore>,
        db_path: &str,
    ) -> anyhow::Result<Self> {
        let marker_path = Path::from(db_path).join(BUCKET_ID_MARKER);

        tracing::debug!("Checking for bucket ID at: {}", marker_path);

        let id = match object_store.get(&marker_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let id_str = String::from_utf8(bytes.to_vec())?;
                let uuid = Uuid::parse_str(id_str.trim())
                    .map_err(|e| anyhow::anyhow!("Invalid bucket ID format: {e:#?}"))?;
                tracing::info!("Found existing bucket ID: {}", uuid);
                uuid
            }
            Err(slatedb::object_store::Error::NotFound { .. }) => {
                tracing::debug!("Bucket ID marker not found, creating new one");
                let new_id = Uuid::new_v4();

                // Conditional create: if another node created the marker concurrently,
                // adopt ITS id so both nodes share one bucket identity (and thus one
                // cache namespace) instead of each keeping a different one.
                match object_store
                    .put_opts(
                        &marker_path,
                        new_id.to_string().into(),
                        PutOptions::from(PutMode::Create),
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::info!("Creating new bucket ID: {}", new_id);
                        new_id
                    }
                    Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
                        let bytes = object_store.get(&marker_path).await?.bytes().await?;
                        let id_str = String::from_utf8(bytes.to_vec())?;
                        let uuid = Uuid::parse_str(id_str.trim())
                            .map_err(|e| anyhow::anyhow!("Invalid bucket ID format: {e:#?}"))?;
                        tracing::info!("Adopted concurrently-created bucket ID: {}", uuid);
                        uuid
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to write bucket ID marker: {e:#?}"));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to read bucket ID marker: {e:#?}"));
            }
        };

        Ok(Self { id })
    }

    /// Gets the bucket ID
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Generates a cache-friendly directory name for this bucket
    pub fn cache_directory_name(&self) -> String {
        // Use the first 8 characters of the UUID for readability
        format!("bucket_{}", &self.id.to_string()[..8])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_directory_name() {
        let bucket = BucketIdentity {
            id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        };
        assert_eq!(bucket.cache_directory_name(), "bucket_550e8400");
    }

    #[test]
    fn test_cache_directory_name_with_new_uuid() {
        let uuid = Uuid::new_v4();
        let bucket = BucketIdentity { id: uuid };
        let cache_name = bucket.cache_directory_name();

        assert!(cache_name.starts_with("bucket_"));
        assert_eq!(cache_name.len(), 15);
        let expected = format!("bucket_{}", &uuid.to_string()[..8]);
        assert_eq!(cache_name, expected);
    }

    /// Two nodes initializing the same bucket concurrently must converge on ONE id
    /// (else they'd use different cache namespaces).
    #[tokio::test]
    async fn concurrent_get_or_create_converges() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let (a, b) = tokio::join!(
            BucketIdentity::get_or_create(&store, "data"),
            BucketIdentity::get_or_create(&store, "data"),
        );
        assert_eq!(a.unwrap().id(), b.unwrap().id(), "bucket ids must converge");
    }

    #[tokio::test]
    async fn load_existing_bucket_identity_is_stable_without_writing() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let expected = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        store
            .put(
                &Path::from("data").join(BUCKET_ID_MARKER),
                expected.to_string().into(),
            )
            .await
            .unwrap();

        let first = BucketIdentity::load(&store, "data").await.unwrap();
        let second = BucketIdentity::load(&store, "data").await.unwrap();

        assert_eq!(first.id(), expected);
        assert_eq!(second.id(), expected);
    }

    #[tokio::test]
    async fn load_missing_bucket_identity_fails_without_creating_marker() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());

        let error = BucketIdentity::load(&store, "data").await.unwrap_err();

        assert!(
            format!("{error:#}").contains("missing durable bucket identity"),
            "unexpected error: {error:#}"
        );
        assert!(
            matches!(
                store.get(&Path::from("data").join(BUCKET_ID_MARKER)).await,
                Err(slatedb::object_store::Error::NotFound { .. })
            ),
            "load must not create a missing marker"
        );
    }

    #[tokio::test]
    async fn load_malformed_bucket_identity_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        store
            .put(
                &Path::from("data").join(BUCKET_ID_MARKER),
                "not-a-uuid".into(),
            )
            .await
            .unwrap();

        let error = BucketIdentity::load(&store, "data").await.unwrap_err();

        assert!(
            format!("{error:#}").contains("invalid durable bucket identity"),
            "unexpected error: {error:#}"
        );
    }
}
