use super::*;
use std::fs;
use std::io::Write;
use std::path::Path;

fn fixture() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("zerofs-inventory-review-")
        .tempdir()
        .unwrap()
}

fn scan(
    root: &Path,
    work: Option<&Path>,
    resume: Option<&Path>,
    cap: usize,
) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    walk::inventory_with_options(
        root,
        false,
        None,
        walk::Options {
            work_dir: work,
            resume_from: resume,
            memory_bytes: cap,
        },
        &mut output,
    )?;
    Ok(output)
}

#[test]
fn spill_matches_memory_and_cleans_workdir() {
    let _guard = super::tests::FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let root = temp.path().join("root");
    let work = temp.path().join("work");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&work).unwrap();
    fs::create_dir(root.join("directory")).unwrap();
    fs::write(root.join("original"), b"data").unwrap();
    for n in 0..600 {
        fs::hard_link(root.join("original"), root.join(format!("link{n:04}"))).unwrap();
    }
    for name in ["file", "FILE", "ı", "I", "ß", "SS", "é", "e\u{301}"] {
        fs::write(root.join("directory").join(name), []).unwrap();
    }
    #[cfg(target_os = "linux")]
    for bits in 0..512 {
        let name: String = "abcdefghi"
            .chars()
            .enumerate()
            .map(|(index, character)| {
                if bits & (1 << index) != 0 {
                    character.to_ascii_uppercase()
                } else {
                    character
                }
            })
            .collect();
        fs::write(root.join("directory").join(name), []).unwrap();
    }
    #[cfg(target_os = "linux")]
    for name in [b"\xffA".as_slice(), b"\xffa", b"\xc3a", b"\xc3A"] {
        use std::os::unix::ffi::OsStrExt;
        fs::write(
            root.join("directory")
                .join(std::ffi::OsStr::from_bytes(name)),
            [],
        )
        .unwrap();
    }
    // Warm access times before comparing the exact observations.
    scan(&root, Some(&work), None, 16 * 1024 * 1024).unwrap();
    let memory = scan(&root, Some(&work), None, 16 * 1024 * 1024).unwrap();
    let spilled = scan(&root, Some(&work), None, 200).unwrap();
    assert!(
        spilled == memory,
        "spill changed output at byte {:?}",
        spilled.iter().zip(&memory).position(|(a, b)| a != b)
    );
    let records: Vec<Value> = spilled
        .split(|b| *b == b'\n')
        .filter(|b| !b.is_empty())
        .map(|b| serde_json::from_slice(b).unwrap())
        .collect();
    #[cfg(target_os = "linux")]
    {
        let collisions: Vec<_> = records
            .iter()
            .filter(|r| r["record"] == "name_collision" && r["rule"] == super::collision::RULES[0])
            .collect();
        assert_eq!(collisions.len(), 5);
        assert_eq!(collisions[0]["names"].as_array().unwrap().len(), 256);
        assert_eq!(collisions[0]["last_chunk"], false);
        assert_eq!(collisions[1]["last_chunk"], true);
        assert_eq!(collisions[2]["names"][0]["display"], "FILE");
        assert_eq!(collisions[3]["names"][0]["display"], "I");
        assert_eq!(collisions[4]["names"][0]["hex"], "ff41");
        assert_eq!(
            records.iter().find(|r| r["record"] == "summary").unwrap()["non_utf8_names"],
            4
        );
    }
    let groups: Vec<_> = records
        .iter()
        .filter(|r| r["record"] == "hardlink_group")
        .collect();
    assert_eq!(groups.len(), 3);
    assert_eq!(
        groups
            .iter()
            .map(|g| g["paths_in_tree"].as_array().unwrap().len())
            .sum::<usize>(),
        601
    );
    assert_eq!(groups[2]["last_chunk"], true);
    assert_eq!(groups[0]["links_outside_tree"]["value"], 0);
    assert_eq!(fs::read_dir(&work).unwrap().count(), 0);
    assert!(
        scan(&root, None, None, 200)
            .unwrap_err()
            .to_string()
            .contains("--work-dir")
    );
}

#[test]
fn workdir_cannot_be_under_source_even_through_a_symlink() {
    let _guard = super::tests::FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    std::os::unix::fs::symlink(&root, temp.path().join("alias")).unwrap();
    for work in [&root, &temp.path().join("alias")] {
        assert!(
            scan(&root, Some(work), None, 200)
                .unwrap_err()
                .to_string()
                .contains("outside")
        );
    }
}

struct Interrupt {
    bytes: Vec<u8>,
    checkpoint_seen: bool,
    target: &'static str,
}
impl Write for Interrupt {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.checkpoint_seen {
            return Err(std::io::Error::other("fixture interruption"));
        }
        self.bytes.extend_from_slice(bytes);
        if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
            self.checkpoint_seen =
                value["record"] == "directory_complete" && value["path"]["display"] == self.target;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn resume_after_interruption_is_byte_exact_and_rejects_mismatches() {
    let _guard = super::tests::FIXTURE_LOCK.lock().unwrap();
    let temp = fixture();
    let root = temp.path().join("root");
    let work = temp.path().join("work");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&work).unwrap();
    for dir in ["a", "b", "b/nested", "c"] {
        fs::create_dir(root.join(dir)).unwrap();
        fs::write(root.join(dir).join("file"), b"data").unwrap();
    }
    fs::hard_link(root.join("a/file"), root.join("c/link")).unwrap();
    scan(&root, Some(&work), None, 200).unwrap();
    let expected = scan(&root, Some(&work), None, 200).unwrap();
    let partial = temp.path().join("partial.jsonl");
    for target in ["a", "b/nested", "b", "c", ""] {
        let mut interrupted = Interrupt {
            bytes: Vec::new(),
            checkpoint_seen: false,
            target,
        };
        assert!(
            walk::inventory_with_options(
                &root,
                false,
                None,
                walk::Options {
                    work_dir: Some(&work),
                    resume_from: None,
                    memory_bytes: 200,
                },
                &mut interrupted
            )
            .is_err()
        );
        assert!(interrupted.checkpoint_seen);
        interrupted
            .bytes
            .extend_from_slice(b"{\"record\":\"entry\"");
        fs::write(&partial, &interrupted.bytes).unwrap();
        let resumed = scan(&root, Some(&work), Some(&partial), 200).unwrap();
        assert!(
            resumed == expected,
            "resume after {target} changed output at byte {:?}",
            resumed.iter().zip(&expected).position(|(a, b)| a != b)
        );
        if target == "a" {
            fs::write(temp.path().join("first.jsonl"), &interrupted.bytes).unwrap();
        }
    }
    let partial = temp.path().join("first.jsonl");
    let original = fs::read(&partial).unwrap();
    let checkpoint: Value = serde_json::from_slice(
        original
            .split(|b| *b == b'\n')
            .find(|line| {
                serde_json::from_slice::<Value>(line)
                    .is_ok_and(|value| value["record"] == "directory_complete")
            })
            .unwrap(),
    )
    .unwrap();
    let digest = checkpoint["prefix_sha256"].as_str().unwrap().to_owned();
    let corrupted = temp.path().join("corrupted.jsonl");
    fs::write(
        &corrupted,
        String::from_utf8(original.clone())
            .unwrap()
            .replace(&digest, &"0".repeat(64)),
    )
    .unwrap();
    assert!(
        scan(&root, Some(&work), Some(&corrupted), 200)
            .unwrap_err()
            .to_string()
            .contains("digest")
    );
    let header_only = temp.path().join("header.jsonl");
    fs::write(
        &header_only,
        original.split_inclusive(|b| *b == b'\n').next().unwrap(),
    )
    .unwrap();
    assert!(
        scan(&root, Some(&work), Some(&header_only), 200)
            .unwrap_err()
            .to_string()
            .contains("no complete directory")
    );
    let mut wrong_hash = Vec::new();
    assert!(
        walk::inventory_with_options(
            &root,
            true,
            None,
            walk::Options {
                work_dir: Some(&work),
                resume_from: Some(&partial),
                memory_bytes: 200,
            },
            &mut wrong_hash
        )
        .unwrap_err()
        .to_string()
        .contains("header")
    );
    assert!(wrong_hash.is_empty());
    let mut wrong_rate = Vec::new();
    assert!(
        walk::inventory_with_options(
            &root,
            false,
            std::num::NonZeroU32::new(100),
            walk::Options {
                work_dir: Some(&work),
                resume_from: Some(&partial),
                memory_bytes: 200,
            },
            &mut wrong_rate
        )
        .unwrap_err()
        .to_string()
        .contains("header")
    );
    assert!(wrong_rate.is_empty());
    let other = temp.path().join("other");
    fs::create_dir(&other).unwrap();
    assert!(
        scan(&other, Some(&work), Some(&partial), 200)
            .unwrap_err()
            .to_string()
            .contains("header")
    );
    fs::write(root.join("new"), []).unwrap();
    assert!(
        scan(&root, Some(&work), Some(&partial), 200)
            .unwrap_err()
            .to_string()
            .contains("changed")
    );
    assert_eq!(fs::read_dir(&work).unwrap().count(), 0);
}
