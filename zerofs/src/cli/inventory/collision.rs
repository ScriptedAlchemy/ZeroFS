use icu_normalizer::ComposingNormalizer;
use std::collections::{BTreeMap, BTreeSet};

pub(super) const RULES: [&str; 2] = ["unicode-canonical", "unicode-case-fold"];

pub(super) fn collisions(names: &[Vec<u8>]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let nfc = ComposingNormalizer::new_nfc();
    let mut result = Vec::new();
    for rule in RULES {
        let mut groups = BTreeMap::<String, BTreeSet<Vec<u8>>>::new();
        for name in names {
            let Ok(text) = std::str::from_utf8(name) else {
                continue;
            };
            let canonical = nfc.normalize(text);
            let key = if rule == "unicode-case-fold" {
                unicase::UniCase::unicode(canonical.as_ref()).to_folded_case()
            } else {
                canonical.into_owned()
            };
            groups.entry(key).or_default().insert(name.clone());
        }
        for group in groups.into_values().filter(|g| g.len() > 1) {
            result.push((rule, group.into_iter().collect()));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detect(names: &[&[u8]]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
        collisions(&names.iter().map(|n| n.to_vec()).collect::<Vec<_>>())
    }

    #[test]
    fn canonical_equivalence() {
        let result = detect(&["é".as_bytes(), "e\u{301}".as_bytes()]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "unicode-canonical");
        assert_eq!(result[1].0, "unicode-case-fold");
    }

    #[test]
    fn full_unicode_case_fold() {
        for (a, b) in [
            ("File", "file"),
            ("Maße", "MASSE"),
            ("ﬃ", "FFI"),
            ("ς", "σ"),
        ] {
            let result = detect(&[a.as_bytes(), b.as_bytes()]);
            assert_eq!(result.len(), 1, "{a} / {b}");
            assert_eq!(result[0].0, "unicode-case-fold");
        }
    }

    #[test]
    fn non_utf8_is_byte_exact_only() {
        assert!(detect(&[b"\xffA", b"\xffa", "�a".as_bytes()]).is_empty());
    }

    #[test]
    fn no_false_positives_and_stable_order() {
        assert!(detect(&[b"a", b"ab", b"b", "è".as_bytes(), b"e"]).is_empty());
        assert_eq!(
            detect(&[b"b", b"B", b"a", b"A"]),
            detect(&[b"A", b"a", b"B", b"b"])
        );
        assert!(detect(&[b"same", b"same"]).is_empty());
    }
}
