//! Synthetic Linux loop-device qualification client for the raw store.

#[cfg(target_os = "linux")]
mod probe {
    use std::error::Error;
    use std::io::{self, BufRead, Read, Write};
    use std::path::Path;
    use zerofs_raw_device::claim::Claim;
    use zerofs_raw_device::{Identity, RawStore, Role, landing::LandingJournal, p3::P3Layout};

    const LIMIT: usize = 16 << 20;
    type Result<T> = std::result::Result<T, Box<dyn Error>>;

    fn uuid(text: &str) -> Result<[u8; 16]> {
        let digits: String = text.chars().filter(|&c| c != '-').collect();
        if !text.is_ascii()
            || !digits.bytes().all(|b| b.is_ascii_hexdigit())
            || digits.len() != 32
            || text.len() != 36
            || [8, 13, 18, 23].iter().any(|&i| text.as_bytes()[i] != b'-')
        {
            return Err("expected a canonical UUID".into());
        }
        let mut bytes = [0; 16];
        for (i, part) in bytes.iter_mut().enumerate() {
            *part = u8::from_str_radix(&digits[i * 2..i * 2 + 2], 16)?;
        }
        if bytes == [0; 16] {
            return Err("nil UUID refused".into());
        }
        Ok(bytes)
    }

    fn session(store: &mut RawStore, key: &[u8]) -> Result<()> {
        let mut input = io::stdin().lock();
        let mut output = io::stdout().lock();
        let mut line = String::new();
        loop {
            line.clear();
            if (&mut input).take(64).read_line(&mut line)? == 0 {
                return Ok(());
            }
            let command = line
                .strip_suffix('\n')
                .ok_or("invalid or oversized command")?;
            if let Some(size) = command.strip_prefix("PUT ") {
                let size: usize = size.parse()?;
                if size > LIMIT {
                    return Err("payload exceeds probe budget".into());
                }
                let mut data = vec![0; size];
                input.read_exact(&mut data)?;
                store.put(key, &data)?;
                output.write_all(b"STAGED\n")?;
            } else if command == "FLUSH" {
                store.flush()?;
                output.write_all(b"FLUSHED\n")?;
            } else if command == "GET" {
                let data = store.get(key)?.ok_or("object absent")?;
                writeln!(output, "VALUE {}", data.len())?;
                output.write_all(&data)?;
            } else {
                return Err("unknown session command".into());
            }
            output.flush()?;
        }
    }

    pub(super) fn run() -> Result<()> {
        let args: Vec<_> = std::env::args().skip(1).collect();
        let [op, node, backing, pool, member, role, tail @ ..] = args.as_slice() else {
            return Err("usage: zerofs-raw-device OP LOOP_NODE PRIVATE_TMPFS_INODE POOL_UUID MEMBER_UUID ROLE [KEY]".into());
        };
        let pool_uuid = uuid(pool)?;
        let member_uuid = uuid(member)?;
        let claim = Claim::synthetic(Path::new(node), Path::new(backing))?;
        let (op, layout, tail) = if let Some(op) = op.strip_prefix("p3-") {
            let (plan, rest) = tail
                .split_first()
                .ok_or("missing six P3 region sizes in MiB")?;
            let sizes = plan
                .split(',')
                .map(|s| {
                    s.parse::<u64>()?
                        .checked_mul(1 << 20)
                        .ok_or("P3 size overflow".into())
                })
                .collect::<Result<Vec<_>>>()?;
            let lengths = sizes
                .try_into()
                .map_err(|_| "expected six P3 region sizes")?;
            (op, Some(P3Layout::new(claim.size, lengths)?), rest)
        } else {
            (op.as_str(), None, tail)
        };
        if op == "init-blank" && role == "p3" && tail.is_empty() {
            layout
                .ok_or("P3 initialization needs explicit geometry")?
                .provision(claim.device, pool_uuid, member_uuid)?;
            return Ok(());
        }
        let role = match role.as_str() {
            "payload" => Role::Data,
            "authority" => Role::Metadata,
            "catalog" => Role::Catalog,
            "key-envelope" => Role::KeyEnvelope,
            "scratch" => Role::Scratch,
            "cache" => Role::Cache,
            "journal" => Role::Journal,
            _ => return Err("unknown role".into()),
        };
        let identity = Identity {
            pool_uuid,
            member_uuid,
            role,
        };
        let mut store = if let Some(layout) = layout {
            layout
                .open(claim.device, pool_uuid, member_uuid)?
                .into_role(role)?
        } else if op == "init-blank" && tail.is_empty() {
            RawStore::provision(claim.device, claim.size, identity)?;
            return Ok(());
        } else {
            RawStore::open(claim.device, claim.size, identity)?
        };
        if op == "pending" && tail.is_empty() {
            for (member, digest) in LandingJournal::new(store)?.pending_segments() {
                println!("{:032x} {digest}", u128::from_be_bytes(member));
            }
            return Ok(());
        }
        if op == "drain" {
            let [target_node, target_backing, target_uuid, digest] = tail else {
                return Err("expected target node, backing, UUID and digest".into());
            };
            let target_id = Identity {
                pool_uuid: identity.pool_uuid,
                member_uuid: uuid(target_uuid)?,
                role: Role::Data,
            };
            let target = Claim::synthetic(Path::new(target_node), Path::new(target_backing))?;
            let mut target_store = RawStore::open(target.device, target.size, target_id)?;
            LandingJournal::new(store)?.drain(&mut target_store, digest)?;
            return Ok(());
        }
        let [key] = tail else {
            return Err("expected one object key or destination UUID".into());
        };
        match op {
            "session" => session(&mut store, key.as_bytes())?,
            "stage" | "put-flush" => {
                let mut bytes = Vec::new();
                io::stdin()
                    .lock()
                    .take((LIMIT + 1) as u64)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > LIMIT {
                    return Err("payload exceeds probe budget".into());
                }
                if op == "stage" {
                    let destination = Identity {
                        pool_uuid: identity.pool_uuid,
                        member_uuid: uuid(key)?,
                        role: Role::Data,
                    };
                    println!(
                        "{}",
                        LandingJournal::new(store)?.stage(destination, &bytes)?
                    );
                } else {
                    store.put(key.as_bytes(), &bytes)?;
                    store.flush()?;
                }
            }
            "get" => io::stdout()
                .lock()
                .write_all(&store.get(key.as_bytes())?.ok_or("object absent")?)?,
            _ => return Err("unknown operation".into()),
        }
        Ok(())
    }
}

fn main() {
    #[cfg(target_os = "linux")]
    if let Err(error) = probe::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }

    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("raw block qualification requires Linux");
        std::process::exit(1);
    }
}
