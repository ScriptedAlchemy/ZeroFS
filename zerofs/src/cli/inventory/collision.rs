use super::Manifest;
use super::record::{NameCollision, RawBytes, Record};
use super::spill::{GROUP_ROWS, Rows, Sorter};
use icu_normalizer::ComposingNormalizer;
use anyhow::Result;
use std::io::Write;

const OPENZFS_RULE: &str = "openzfs-insensitive-unicode-5.0.0";
pub(super) const RULES: [&str; 3] = [OPENZFS_RULE, "unicode-canonical", "unicode-case-fold"];

fn key(rule: usize, name: &[u8]) -> Option<Vec<u8>> {
    if rule == 0 {
        return Some(crate::fs::name_mode::fold_name(name).into_owned());
    }
    let text = std::str::from_utf8(name).ok()?;
    let nfc = ComposingNormalizer::new_nfc();
    let canonical = nfc.normalize(text);
    Some(if rule == 2 {
        unicase::UniCase::unicode(canonical.as_ref()).to_folded_case().into_bytes()
    } else {
        canonical.into_owned().into_bytes()
    })
}

pub(super) fn push(sorter: &mut Sorter, name: &[u8]) -> Result<()> {
    for rule in 0..RULES.len() {
        if let Some(key) = key(rule, name) {
            let mut row = Vec::with_capacity(1 + key.len() + 1 + name.len());
            row.push(rule as u8);
            row.extend_from_slice(&key);
            row.push(0);
            row.extend_from_slice(name);
            sorter.push(row)?;
        }
    }
    Ok(())
}

pub(super) fn emit<W: Write>(rows: &Rows, path: &[u8], output: &mut Manifest<W>) -> Result<()> {
    let mut rows = rows.iter()?.peekable();
    while let Some(first) = rows.next() {
        let first = first?;
        let rule = RULES[first[0] as usize];
        let separator = 1 + first[1..].iter().position(|b| *b == 0).unwrap();
        let key = &first[..=separator];
        let mut names = vec![RawBytes::new(&first[separator + 1..])];
        let mut chunk = 0;
        loop {
            let same = match rows.peek() {
                Some(Ok(row)) => row.starts_with(key),
                Some(Err(_)) => {
                    rows.next().unwrap()?;
                    unreachable!()
                }
                None => false,
            };
            if !same {
                if names.len() > 1 || chunk > 0 {
                    output.emit(&Record::NameCollision(NameCollision {
                        dir: RawBytes::new(path),
                        rule,
                        names,
                        chunk,
                        last_chunk: true,
                    }))?;
                }
                break;
            }
            if names.len() == GROUP_ROWS {
                output.emit(&Record::NameCollision(NameCollision {
                    dir: RawBytes::new(path),
                    rule,
                    names,
                    chunk,
                    last_chunk: false,
                }))?;
                names = Vec::new();
                chunk += 1;
            }
            let row = rows.next().unwrap()?;
            names.push(RawBytes::new(&row[separator + 1..]));
        }
    }
    Ok(())
}

#[cfg(test)]
fn collisions(names: &[Vec<u8>]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut result = Vec::new();
    for (rule, label) in RULES.iter().enumerate() {
        let mut groups = BTreeMap::<Vec<u8>, BTreeSet<Vec<u8>>>::new();
        for name in names {
            if let Some(key) = key(rule, name) {
                groups.entry(key).or_default().insert(name.clone());
            }
        }
        for group in groups.into_values().filter(|group| group.len() > 1) {
            result.push((*label, group.into_iter().collect()));
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
