// Vendored and slightly modified from: https://github.com/apache/arrow-rs-object-store/blob/c0e241eb95a61d52964f3d7741673b91f86db58b/src/parse.rs
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use object_store::ClientConfigKey;
use object_store::ClientOptions;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;
use url::Url;

const DEFAULT_USER_AGENT: &str = concat!(
    "ZeroFS/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/Barre/ZeroFS)"
);

fn default_client_options() -> ClientOptions {
    ClientOptions::default()
        .with_timeout_disabled()
        .with_config(ClientConfigKey::UserAgent, DEFAULT_USER_AGENT)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unable to recognise URL \"{}\"", url)]
    Unrecognised { url: Url },

    #[error(transparent)]
    Path {
        #[from]
        source: object_store::path::Error,
    },

    #[error("Invalid SFTP URL: a host is required")]
    SftpHostRequired,

    #[error("Invalid SFTP URL: a username is required")]
    SftpUsernameRequired,

    #[error("Invalid SFTP URL: passwords are not allowed; use SSH key authentication")]
    SftpPasswordNotAllowed,
}

impl From<Error> for object_store::Error {
    fn from(e: Error) -> Self {
        Self::Generic {
            store: "URL",
            source: Box::new(e),
        }
    }
}

/// Recognizes various URL formats, identifying the relevant [`ObjectStore`]
///
/// See [`ObjectStoreScheme::parse`] for more details
///
/// # Supported formats:
/// - `file:///path/to/my/file` -> [`LocalFileSystem`]
/// - `memory:///` -> [`InMemory`]
/// - `s3://bucket/path` -> [`AmazonS3`](crate::aws::AmazonS3) (also supports `s3a`)
/// - `gs://bucket/path` -> [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
/// - `az://account/container/path` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure) (also supports `adl`, `azure`, `abfs`, `abfss`)
/// - `http://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)
/// - `https://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)
/// - `sftp://user@host:port/path` -> SFTP-backed object storage
///
/// There are also special cases for AWS and Azure for `https://{host?}/path` paths:
/// - `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
/// - `amazonaws.com` -> [`AmazonS3`](crate::aws::AmazonS3)
/// - `r2.cloudflarestorage.com` -> [`AmazonS3`](crate::aws::AmazonS3)
///
#[non_exhaustive] // permit new variants
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ObjectStoreScheme {
    /// Url corresponding to [`LocalFileSystem`]
    Local,
    /// Url corresponding to [`InMemory`]
    Memory,
    /// Url corresponding to [`AmazonS3`](crate::aws::AmazonS3)
    AmazonS3,
    /// Url corresponding to [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
    GoogleCloudStorage,
    /// Url corresponding to [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
    MicrosoftAzure,
    /// Url corresponding to [`HttpStore`](crate::http::HttpStore)
    Http,
    /// URL corresponding to an SFTP server.
    Sftp,
}

impl ObjectStoreScheme {
    /// Create an [`ObjectStoreScheme`] from the provided [`Url`]
    ///
    /// Returns the [`ObjectStoreScheme`] and the remaining [`Path`]
    ///
    /// # Example
    /// ```
    /// # use url::Url;
    /// # use object_store::ObjectStoreScheme;
    /// let url: Url = "file:///path/to/my/file".parse().unwrap();
    /// let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();
    /// assert_eq!(scheme, ObjectStoreScheme::Local);
    /// assert_eq!(path.as_ref(), "path/to/my/file");
    ///
    /// let url: Url = "https://blob.core.windows.net/container/path/to/my/file".parse().unwrap();
    /// let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();
    /// assert_eq!(scheme, ObjectStoreScheme::MicrosoftAzure);
    /// assert_eq!(path.as_ref(), "path/to/my/file");
    ///
    /// let url: Url = "https://example.com/path/to/my/file".parse().unwrap();
    /// let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();
    /// assert_eq!(scheme, ObjectStoreScheme::Http);
    /// assert_eq!(path.as_ref(), "path/to/my/file");
    /// ```
    pub fn parse(url: &Url) -> Result<(Self, Path), Error> {
        let strip_bucket = || Some(url.path().strip_prefix('/')?.split_once('/')?.1);

        let (scheme, path) = match (url.scheme(), url.host_str()) {
            ("file", None) => (Self::Local, url.path()),
            ("memory", None) => (Self::Memory, url.path()),
            ("s3" | "s3a", Some(_)) => (Self::AmazonS3, url.path()),
            ("gs", Some(_)) => (Self::GoogleCloudStorage, url.path()),
            ("az", Some(_)) => (Self::MicrosoftAzure, strip_bucket().unwrap_or_default()),
            ("adl" | "azure" | "abfs" | "abfss", Some(_)) => (Self::MicrosoftAzure, url.path()),
            ("http", Some(_)) => (Self::Http, url.path()),
            // Hostless sftp URLs still classify as Sftp so the build arm can
            // report the specific URL-shape error.
            ("sftp", _) => (Self::Sftp, url.path()),
            ("https", Some(host)) => {
                if host.ends_with("dfs.core.windows.net")
                    || host.ends_with("blob.core.windows.net")
                    || host.ends_with("dfs.fabric.microsoft.com")
                    || host.ends_with("blob.fabric.microsoft.com")
                {
                    (Self::MicrosoftAzure, strip_bucket().unwrap_or_default())
                } else if host.ends_with("amazonaws.com") {
                    match host.starts_with("s3") {
                        true => (Self::AmazonS3, strip_bucket().unwrap_or_default()),
                        false => (Self::AmazonS3, url.path()),
                    }
                } else if host.ends_with("r2.cloudflarestorage.com") {
                    (Self::AmazonS3, strip_bucket().unwrap_or_default())
                } else {
                    (Self::Http, url.path())
                }
            }
            _ => return Err(Error::Unrecognised { url: url.clone() }),
        };

        Ok((scheme, Path::from_url_path(path)?))
    }
}

/// A parsed storage target: the store, the root path within it, and (for
/// backends with an owned transport, currently SFTP) the session pool whose
/// shutdown the caller owns.
#[derive(Debug)]
pub struct ParsedStore {
    pub store: Box<dyn ObjectStore>,
    pub path: Path,
    pub sftp_pool: Option<crate::sftp_transport::SftpSessionPool>,
}

/// Create an [`ObjectStore`] based on the provided `url` and options
///
/// This method can be used to create an instance of one of the provided
/// `ObjectStore` implementations based on the URL scheme (see
/// [`ObjectStoreScheme`] for more details). It is the single entry point for
/// every scheme, including SFTP; `sftp_config` is ignored for other backends.
///
/// For example
/// * `file:///path/to/my/file` will return a [`LocalFileSystem`] instance
/// * `s3://bucket/path` will return an [`AmazonS3`] instance if the `aws` feature is enabled.
///
/// Arguments:
/// * `url`: The URL to parse
/// * `options`: A list of key-value pairs to pass to the [`ObjectStore`] builder.
///   Note different object stores accept different configuration options, so
///   the options that are read depends on the `url` value. One common pattern
///   is to pass configuration information via process variables using
///   [`std::env::vars`].
/// * `sftp_config`: Transport tuning for `sftp://` URLs; defaults apply when
///   absent.
///
/// [`AmazonS3`]: https://docs.rs/object_store/0.12.0/object_store/aws/struct.AmazonS3.html
pub async fn parse_url_opts<I, K, V>(
    url: &Url,
    options: I,
    sftp_config: Option<&crate::config::SftpConfig>,
) -> Result<ParsedStore, object_store::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    let path = Path::parse(path)?;

    if scheme == ObjectStoreScheme::Sftp {
        return build_sftp_store(url, path, sftp_config).await;
    }

    let store: Box<dyn ObjectStore> = match scheme {
        // `with_fsync(true)` makes a successful write durable on disk before it
        // returns, matching the implicit contract of the cloud backends
        ObjectStoreScheme::Local => Box::new(LocalFileSystem::new().with_fsync(true)),
        ObjectStoreScheme::Memory => Box::new(InMemory::new()),
        ObjectStoreScheme::AmazonS3 => {
            // Collect options so we can intercept an external conditional-put
            // coordinator (a `redis://...` URL) before it reaches the builder, which
            // only understands `etag`/`disabled`. The HEAD+PUT coordination is
            // provided one layer up by RedisConditionalStore instead, so the
            // inner store can stay vanilla `object_store`.
            let mut opts: Vec<(String, String)> = options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.into()))
                .collect();

            let mut commit_url: Option<String> = None;
            opts.retain(|(k, v)| {
                let is_conditional_put = k
                    .parse::<object_store::aws::AmazonS3ConfigKey>()
                    .is_ok_and(|key| {
                        matches!(key, object_store::aws::AmazonS3ConfigKey::ConditionalPut)
                    });
                if is_conditional_put && (v.starts_with("redis://") || v.starts_with("rediss://")) {
                    commit_url = Some(v.clone());
                    false
                } else {
                    true
                }
            });

            // When coordinating externally, the inner store must never emit
            // precondition headers so we serialise HEAD+PUT under the lock instead.
            if commit_url.is_some() {
                opts.push(("conditional_put".to_string(), "disabled".to_string()));
            }

            let builder = opts.into_iter().fold(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_url(url.to_string())
                    .with_client_options(default_client_options()),
                |builder, (key, value)| match key.parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            );
            let inner = builder.build()?;

            match commit_url {
                Some(redis_url) => {
                    let commit = crate::redis_conditional_store::RedisCommit::new(redis_url)?;
                    Box::new(crate::redis_conditional_store::RedisConditionalStore::new(
                        Arc::new(inner),
                        Arc::new(commit),
                    ))
                }
                None => Box::new(inner),
            }
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let builder = options.into_iter().fold(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_url(url.to_string())
                    .with_client_options(default_client_options()),
                |builder, (key, value)| match key.as_ref().parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            );
            Box::new(builder.build()?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let builder = options.into_iter().fold(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_url(url.to_string())
                    .with_client_options(default_client_options()),
                |builder, (key, value)| match key.as_ref().parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            );
            Box::new(builder.build()?)
        }
        ObjectStoreScheme::Sftp => unreachable!("sftp URLs are built above"),
        s => {
            return Err(object_store::Error::Generic {
                store: "parse_url",
                source: format!("feature for {s:?} not enabled").into(),
            });
        }
    };

    Ok(ParsedStore {
        store,
        path,
        sftp_pool: None,
    })
}

/// URL-shape rules specific to the SFTP scheme, enforced where the SFTP
/// store is built so `ObjectStoreScheme::parse` stays table-driven.
fn validate_sftp_url(url: &Url) -> Result<(), Error> {
    if url.password().is_some() {
        return Err(Error::SftpPasswordNotAllowed);
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(Error::SftpHostRequired);
    }
    if url.username().is_empty() {
        return Err(Error::SftpUsernameRequired);
    }
    Ok(())
}

/// Build the native async SFTP transport, pool, and store.
async fn build_sftp_store(
    url: &Url,
    path: Path,
    sftp_config: Option<&crate::config::SftpConfig>,
) -> Result<ParsedStore, object_store::Error> {
    validate_sftp_url(url)?;
    crate::sftp_object_store::SftpObjectStore::validate_prefix(&path)?;

    let config = sftp_config.cloned().unwrap_or_default();
    let endpoint = crate::config::SftpEndpoint {
        host: url
            .host_str()
            .expect("SFTP URL was validated above")
            .to_owned(),
        port: url.port().unwrap_or(22),
        username: url.username().to_owned(),
    };
    tracing::info!(
        host = %endpoint.host,
        port = endpoint.port,
        window_size = crate::sftp_transport::RUSSH_WINDOW_SIZE,
        maximum_packet_size = crate::sftp_transport::RUSSH_MAXIMUM_PACKET_SIZE,
        max_concurrent_writes = crate::sftp_transport::RUSSH_SFTP_MAX_CONCURRENT_WRITES,
        "opening russh SFTP object store"
    );
    let factory: Arc<dyn crate::sftp_transport::SessionFactory> = Arc::new(
        crate::sftp_transport::RusshSessionFactory::new(
            endpoint,
            config.identity_file.clone(),
            config.known_hosts.clone(),
        )
        .map_err(|source| object_store::Error::Generic {
            store: "SFTP",
            source: Box::new(source),
        })?,
    );
    let pool = crate::sftp_transport::SftpSessionPool::from_config_writable(factory, &config)
        .await
        .map_err(|source| object_store::Error::Generic {
            store: "SFTP",
            source: Box::new(source),
        })?;
    let store = crate::sftp_object_store::SftpObjectStore::new(pool.clone(), path.clone())?;
    Ok(ParsedStore {
        store: Box::new(store),
        path,
        sftp_pool: Some(pool),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let cases = [
            ("file:/path", (ObjectStoreScheme::Local, "path")),
            ("file:///path", (ObjectStoreScheme::Local, "path")),
            ("memory:/path", (ObjectStoreScheme::Memory, "path")),
            ("memory:///", (ObjectStoreScheme::Memory, "")),
            ("s3://bucket/path", (ObjectStoreScheme::AmazonS3, "path")),
            ("s3a://bucket/path", (ObjectStoreScheme::AmazonS3, "path")),
            (
                "https://s3.region.amazonaws.com/bucket",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://s3.region.amazonaws.com/bucket/path",
                (ObjectStoreScheme::AmazonS3, "path"),
            ),
            (
                "https://bucket.s3.region.amazonaws.com",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/path",
                (ObjectStoreScheme::AmazonS3, "path"),
            ),
            (
                "abfs://container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "abfs://file_system@account_name.dfs.core.windows.net/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "abfss://file_system@account_name.dfs.core.windows.net/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.dfs.core.windows.net",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.core.windows.net/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.blob.core.windows.net",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.core.windows.net/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "az://account/container",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "az://account/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "gs://bucket/path",
                (ObjectStoreScheme::GoogleCloudStorage, "path"),
            ),
            (
                "gs://test.example.com/path",
                (ObjectStoreScheme::GoogleCloudStorage, "path"),
            ),
            ("http://mydomain/path", (ObjectStoreScheme::Http, "path")),
            ("https://mydomain/path", (ObjectStoreScheme::Http, "path")),
            (
                "s3://bucket/foo%20bar",
                (ObjectStoreScheme::AmazonS3, "foo bar"),
            ),
            (
                "s3://bucket/foo bar",
                (ObjectStoreScheme::AmazonS3, "foo bar"),
            ),
            ("s3://bucket/😀", (ObjectStoreScheme::AmazonS3, "😀")),
            (
                "s3://bucket/%F0%9F%98%80",
                (ObjectStoreScheme::AmazonS3, "😀"),
            ),
            (
                "https://foo/bar%20baz",
                (ObjectStoreScheme::Http, "bar baz"),
            ),
            (
                "file:///bar%252Efoo",
                (ObjectStoreScheme::Local, "bar%2Efoo"),
            ),
            (
                "abfss://file_system@account.dfs.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "abfss://file_system@account.dfs.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.fabric.microsoft.com/container",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.dfs.fabric.microsoft.com/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.blob.fabric.microsoft.com/",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.fabric.microsoft.com/container",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.fabric.microsoft.com/container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
        ];

        for (s, (expected_scheme, expected_path)) in cases {
            let url = Url::parse(s).unwrap();
            let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();

            assert_eq!(scheme, expected_scheme, "{s}");
            assert_eq!(path, Path::parse(expected_path).unwrap(), "{s}");
        }

        let neg_cases = [
            "unix:/run/foo.socket",
            "file://remote/path",
            "memory://remote/",
        ];
        for s in neg_cases {
            let url = Url::parse(s).unwrap();
            assert!(ObjectStoreScheme::parse(&url).is_err());
        }
    }

    #[test]
    fn parse_storage_box_sftp_url_keeps_remote_prefix() {
        let url = Url::parse("sftp://u123456@u123456.your-storagebox.de:23/zerofs/v1").unwrap();

        let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();

        assert_eq!(scheme, ObjectStoreScheme::Sftp);
        assert_eq!(path, Path::parse("zerofs/v1").unwrap());
    }

    #[tokio::test]
    async fn sftp_parser_requires_host() {
        let missing_host = Url::parse("sftp:///data").unwrap();
        let error = parse_url_opts(&missing_host, std::iter::empty::<(&str, &str)>(), None)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Generic URL error: Invalid SFTP URL: a host is required"
        );
    }

    #[tokio::test]
    async fn sftp_parser_requires_username() {
        let missing_username = Url::parse("sftp://example.com/data").unwrap();
        let error = parse_url_opts(&missing_username, std::iter::empty::<(&str, &str)>(), None)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Generic URL error: Invalid SFTP URL: a username is required"
        );
    }

    #[tokio::test]
    async fn sftp_parser_rejects_password_without_leaking_it() {
        let secret = "parser-login-secret";
        let password_url = Url::parse(&format!("sftp://alice:{secret}@example.com/data")).unwrap();
        let error = parse_url_opts(&password_url, std::iter::empty::<(&str, &str)>(), None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("password"), "got: {error}");
        assert!(!error.contains(secret), "password leaked in error: {error}");
    }
}
