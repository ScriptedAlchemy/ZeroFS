//! Canonical authenticated survival records.
//!
//! Records are canonical UTF-8 JSON: sorted object keys, integers only, no
//! insignificant whitespace and lowercase hex bytes. The MAC covers the
//! domain-separated canonical bytes without the `mac` field. A receipt and a
//! predecessor digest are both `SHA-256(complete record file bytes)`.

use anyhow::{Context, Result, bail, ensure};
use hkdf::Hkdf;
use hkdf::hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub(crate) const FORMAT_VERSION: u64 = 1;
pub(crate) const ROOT: &str = ".zerofs-survival/v1";
const MAC_DOMAIN: &[u8] = b"zerofs-metadata-survival-v1\x00record\x00";
const HKDF_INFO: &[u8] = b"zerofs-metadata-survival-v1";
/// Records larger than this are refused before parsing.
pub(crate) const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct MacKey([u8; 32]);

impl std::fmt::Debug for MacKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MacKey(..)")
    }
}

impl MacKey {
    /// Derive the record key from the recovered data key and pool identity.
    pub(crate) fn derive(dek: &[u8; 32], pool_id: &str) -> Self {
        let hkdf = Hkdf::<Sha256>::new(Some(pool_id.as_bytes()), dek);
        let mut key = [0u8; 32];
        hkdf.expand(HKDF_INFO, &mut key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        Self(key)
    }

    fn mac(&self, canonical: &[u8]) -> [u8; 32] {
        let mut mac =
            <Hmac<Sha256> as Mac>::new_from_slice(&self.0).expect("HMAC accepts any key length");
        mac.update(MAC_DOMAIN);
        mac.update(canonical);
        mac.finalize().into_bytes().into()
    }
}

fn check_value(value: &Value) -> Result<()> {
    match value {
        Value::Number(number) => ensure!(
            number.is_u64() || number.is_i64(),
            "survival records contain integers only"
        ),
        Value::Array(items) => items.iter().try_for_each(check_value)?,
        Value::Object(map) => map.values().try_for_each(check_value)?,
        _ => {}
    }
    Ok(())
}

/// Canonical bytes. serde_json's default map is ordered by key.
pub(crate) fn canonical(value: &Value) -> Result<Vec<u8>> {
    check_value(value)?;
    Ok(serde_json::to_vec(value)?)
}

/// Seal a record object: add its MAC and return the final file bytes.
pub(crate) fn seal(mut record: Map<String, Value>, key: &MacKey) -> Result<Vec<u8>> {
    ensure!(!record.contains_key("mac"), "record already sealed");
    let body = canonical(&Value::Object(record.clone()))?;
    record.insert("mac".into(), Value::String(hex_encode(key.mac(&body))));
    canonical(&Value::Object(record))
}

/// Parse canonical bytes without authenticating them.
pub(crate) fn parse(bytes: &[u8]) -> Result<Map<String, Value>> {
    ensure!(
        bytes.len() <= MAX_RECORD_BYTES,
        "survival record is too large"
    );
    let value: Value = serde_json::from_slice(bytes).context("survival record is not JSON")?;
    let Value::Object(map) = value else {
        bail!("survival record is not an object");
    };
    // Rejects duplicate keys, floats, whitespace and non-canonical order.
    ensure!(
        canonical(&Value::Object(map.clone()))? == bytes,
        "survival record is not canonical"
    );
    Ok(map)
}

/// Parse and authenticate a sealed record.
pub(crate) fn open(bytes: &[u8], key: &MacKey) -> Result<Map<String, Value>> {
    let mut map = parse(bytes)?;
    let mac = map
        .remove("mac")
        .and_then(|mac| mac.as_str().map(str::to_owned))
        .context("survival record has no MAC")?;
    let expected = key.mac(&canonical(&Value::Object(map.clone()))?);
    let actual: [u8; 32] = hex_decode(&mac)
        .and_then(|bytes| bytes.try_into().ok())
        .context("survival record MAC is malformed")?;
    // Constant time is unnecessary for an offline integrity check, but cheap.
    let mut verifier =
        <Hmac<Sha256> as Mac>::new_from_slice(&key.0).expect("HMAC accepts any key length");
    verifier.update(MAC_DOMAIN);
    verifier.update(&canonical(&Value::Object(map.clone()))?);
    ensure!(
        verifier.verify_slice(&actual).is_ok() && expected == actual,
        "survival record authentication failed"
    );
    Ok(map)
}

pub(crate) fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(text.get(index..index + 2)?, 16).ok())
        .collect()
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(crate) fn get_str<'a>(map: &'a Map<String, Value>, field: &str) -> Result<&'a str> {
    map.get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("survival record field `{field}` is missing or not a string"))
}

pub(crate) fn get_u64(map: &Map<String, Value>, field: &str) -> Result<u64> {
    map.get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("survival record field `{field}` is missing or not an integer"))
}

pub(crate) fn get_hash(map: &Map<String, Value>, field: &str) -> Result<[u8; 32]> {
    parse_hash(get_str(map, field)?)
}

pub(crate) fn parse_hash(text: &str) -> Result<[u8; 32]> {
    ensure!(
        text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
        "malformed lowercase SHA-256 `{text}`"
    );
    Ok(hex_decode(text)
        .and_then(|bytes| bytes.try_into().ok())
        .expect("64 hex digits"))
}

/// A normalized relative object path: only normal components.
pub(crate) fn safe_relative(path: &str) -> Result<std::path::PathBuf> {
    let relative = std::path::PathBuf::from(path);
    ensure!(
        !path.is_empty()
            && relative
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_))),
        "unsafe survival object path `{path}`"
    );
    Ok(relative)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key() -> MacKey {
        MacKey::derive(&[7; 32], "00000000-0000-0000-0000-000000000001")
    }

    #[test]
    fn golden_record_vector_is_stable() {
        // Computed independently with Python's hmac/hashlib: HKDF-SHA256
        // (salt = pool id, info = domain), then HMAC over the domain-separated
        // canonical body. Any canonicalization, KDF or MAC change breaks it.
        let Value::Object(record) = json!({"sequence": 3, "pool_id": "p", "b": [1, "x"]}) else {
            unreachable!()
        };
        let bytes = seal(record, &key()).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            r#"{"b":[1,"x"],"mac":"47e2bb32f26a987f3d834202c3e90d6191e70d5ebb1a3647061b5fffb5f58be1","pool_id":"p","sequence":3}"#
        );
        assert_eq!(
            hex_encode(digest(&bytes)),
            "bfaf3795052d547c783a4ef5ba049eba1c7f4e7c6ea2d095828606b44fcd5b47"
        );
        let opened = open(&bytes, &key()).unwrap();
        assert_eq!(get_u64(&opened, "sequence").unwrap(), 3);
    }

    #[test]
    fn tampering_whitespace_floats_and_wrong_keys_are_refused() {
        let Value::Object(record) = json!({"sequence": 3}) else {
            unreachable!()
        };
        let bytes = seal(record, &key()).unwrap();
        let mut flipped = bytes.clone();
        let index = flipped.len() - 2;
        flipped[index] = b'4';
        assert!(open(&flipped, &key()).is_err());
        assert!(open(&bytes, &MacKey::derive(&[8; 32], "other")).is_err());
        let mut spaced = bytes.clone();
        spaced.insert(1, b' ');
        assert!(parse(&spaced).is_err());
        assert!(canonical(&json!({"x": 1.5})).is_err());
        assert!(safe_relative("../x").is_err());
        assert!(safe_relative("/x").is_err());
        assert!(safe_relative("a/b").is_ok());
        assert!(parse_hash(&"A".repeat(64)).is_err());
    }
}
