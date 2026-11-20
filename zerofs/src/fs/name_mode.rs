use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// The lookup rule is fixed when the filesystem is created.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameMode {
    #[default]
    Sensitive,
    Insensitive,
}

impl NameMode {
    pub fn key<'a>(self, name: &'a [u8]) -> Cow<'a, [u8]> {
        match self {
            Self::Sensitive => Cow::Borrowed(name),
            Self::Insensitive => fold_name(name),
        }
    }
}

/// How one request matches a client-supplied name against a stored entry.
///
/// `Folded` finds the entry whose folded key equals the name's. `Exact` also
/// requires the stored spelling to equal the name byte for byte. On a
/// sensitive filesystem the key is the stored spelling, so both agree.
/// Creation never uses this choice: fold-equal names always collide.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameMatch {
    #[default]
    Folded,
    Exact,
}

/// Apply single-character Unicode uppercase without normalization. A
/// character whose uppercase form expands to several characters keeps its
/// original spelling.
/// Invalid spans consume the lead byte's full width, including any ASCII tail.
/// Illegal lead bytes consume one byte. Truncated spans consume the remainder.
/// Inventory and filesystem lookup must use this same byte rule.
pub fn fold_name(name: &[u8]) -> Cow<'_, [u8]> {
    let mut folded = Vec::with_capacity(name.len());
    let mut rest = name;
    while let Some(&lead) = rest.first() {
        let width = match lead {
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 1,
        };
        let (span, tail) = rest.split_at(width.min(rest.len()));
        match std::str::from_utf8(span) {
            Ok(valid) => fold_valid(valid, &mut folded),
            Err(_) => folded.extend_from_slice(span),
        }
        rest = tail;
    }
    if folded == name {
        Cow::Borrowed(name)
    } else {
        Cow::Owned(folded)
    }
}

fn fold_valid(valid: &str, output: &mut Vec<u8>) {
    for character in valid.chars() {
        let mut mapped = character.to_uppercase();
        let upper = match (mapped.next(), mapped.next()) {
            (Some(single), None) => single,
            _ => character,
        };
        output.extend_from_slice(upper.encode_utf8(&mut [0; 4]).as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_multibyte_sequence_keeps_its_ascii_tail() {
        assert_eq!(NameMode::Insensitive.key(b"\xc3a"), b"\xc3a".as_slice());
        assert_ne!(
            NameMode::Insensitive.key(b"\xc3a"),
            NameMode::Insensitive.key(b"\xc3A")
        );
    }

    #[test]
    fn simple_uppercase_examples() {
        for (name, expected) in [
            ("aAz", "AAZ"),
            ("éàø", "ÉÀØ"),
            ("ß", "ß"),
            ("ı", "I"),
            ("ſ", "S"),
            ("ςσ", "ΣΣ"),
            ("ﬃ", "ﬃ"),
            ("", ""),
        ] {
            assert_eq!(
                NameMode::Insensitive.key(name.as_bytes()),
                expected.as_bytes()
            );
            assert_eq!(NameMode::Sensitive.key(name.as_bytes()), name.as_bytes());
        }
        assert_ne!(
            NameMode::Insensitive.key("é".as_bytes()),
            NameMode::Insensitive.key("e\u{301}".as_bytes())
        );
    }

    #[test]
    fn invalid_utf8_passes_through_between_folded_letters() {
        assert_eq!(
            NameMode::Insensitive.key(b"a\xffb\xc3\xa9\xc0c\xe2\x82"),
            b"A\xffB\xc3\x89\xc0C\xe2\x82".as_slice()
        );
        assert_eq!(NameMode::Sensitive.key(b"a\xffb"), b"a\xffb".as_slice());
    }

    #[test]
    fn folding_is_idempotent() {
        for character in (0..=0x10ffff).filter_map(char::from_u32) {
            let mut bytes = [0; 4];
            let key = NameMode::Insensitive.key(character.encode_utf8(&mut bytes).as_bytes());
            assert_eq!(NameMode::Insensitive.key(&key), key, "{character:?}");
        }
    }
}
