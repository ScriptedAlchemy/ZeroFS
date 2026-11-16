use icu_normalizer::ComposingNormalizer;
use std::collections::{BTreeMap, BTreeSet};

const OPENZFS_RULE: &str = "openzfs-insensitive-unicode-5.0.0";
pub(super) const RULES: [&str; 3] = [OPENZFS_RULE, "unicode-canonical", "unicode-case-fold"];

pub(super) fn collisions(names: &[Vec<u8>]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let nfc = ComposingNormalizer::new_nfc();
    let mut result = Vec::new();
    for rule in RULES {
        let mut groups = BTreeMap::<Vec<u8>, BTreeSet<Vec<u8>>>::new();
        for name in names {
            if rule == OPENZFS_RULE {
                // Use precisely the filesystem's byte rule, including invalid
                // UTF-8 spans. NFC/full folding would produce a different set
                // of collisions from the source ZFS dataset and destination.
                let key = crate::fs::name_mode::fold_name(name).into_owned();
                groups.entry(key).or_default().insert(name.clone());
                continue;
            }
            let Ok(text) = std::str::from_utf8(name) else {
                continue;
            };
            let canonical = nfc.normalize(text);
            let key = if rule == "unicode-case-fold" {
                unicase::UniCase::unicode(canonical.as_ref()).to_folded_case()
            } else {
                canonical.into_owned()
            };
            groups
                .entry(key.into_bytes())
                .or_default()
                .insert(name.clone());
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
            assert_eq!(
                result
                    .iter()
                    .filter(|(rule, _)| *rule == "unicode-case-fold")
                    .count(),
                1,
                "{a} / {b}"
            );
        }
    }

    #[test]
    fn non_utf8_participates_only_in_the_openzfs_rule() {
        assert_eq!(
            detect(&[b"\xffA", b"\xffa", "�a".as_bytes()]),
            vec![(OPENZFS_RULE, vec![b"\xffA".to_vec(), b"\xffa".to_vec()])]
        );
    }

    #[test]
    fn openzfs_equivalence_has_no_normalization_or_full_fold_expansion() {
        for (a, b, collide) in [
            (b"File".as_slice(), b"file".as_slice(), true),
            ("ı".as_bytes(), b"I".as_slice(), true),
            ("ß".as_bytes(), b"SS".as_slice(), false),
            ("ﬃ".as_bytes(), b"FFI".as_slice(), false),
            ("é".as_bytes(), "e\u{301}".as_bytes(), false),
            (b"\xffa".as_slice(), b"\xffA".as_slice(), true),
            (b"\xc3a".as_slice(), b"\xc3A".as_slice(), false),
        ] {
            let result = detect(&[a, b]);
            assert_eq!(
                result.iter().any(|(rule, _)| *rule == OPENZFS_RULE),
                collide,
                "{a:02x?} / {b:02x?}"
            );
        }
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
