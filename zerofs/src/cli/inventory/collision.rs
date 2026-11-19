use super::Manifest;
use super::record::{NameCollision, RawBytes, Record};
use super::spill::{GROUP_ROWS, Rows, Sorter};
use anyhow::Result;
use icu_normalizer::ComposingNormalizer;
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
        unicase::UniCase::unicode(canonical.as_ref())
            .to_folded_case()
            .into_bytes()
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
        while let Some(row) = rows.next_if(|row| match row {
            Ok(row) => row.starts_with(key),
            Err(_) => true,
        }) {
            let row = row?;
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
            names.push(RawBytes::new(&row[separator + 1..]));
        }
        if names.len() > 1 || chunk > 0 {
            output.emit(&Record::NameCollision(NameCollision {
                dir: RawBytes::new(path),
                rule,
                names,
                chunk,
                last_chunk: true,
            }))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::spill::Workspace;
    use super::*;

    fn detect(names: &[&[u8]]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
        let temp = tempfile::tempdir().unwrap();
        let work = Workspace::new(temp.path(), None, 1024 * 1024).unwrap();
        let mut sorter = Sorter::new(work);
        let mut seen = std::collections::BTreeSet::new();
        for name in names {
            if seen.insert(*name) {
                push(&mut sorter, name).unwrap();
            }
        }
        let rows = sorter.finish().unwrap();
        let mut output = Manifest::new(Vec::new());
        emit(&rows, b"", &mut output).unwrap();
        output
            .writer
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                let record: serde_json::Value = serde_json::from_slice(line).unwrap();
                let rule = RULES
                    .into_iter()
                    .find(|rule| Some(*rule) == record["rule"].as_str())
                    .unwrap();
                let names = record["names"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|name| {
                        super::super::resume::decode_hex(name["hex"].as_str().unwrap()).unwrap()
                    })
                    .collect();
                (rule, names)
            })
            .collect()
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
