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
        if let [mode, command, socket, uid, gid] = args.as_slice()
            && mode == "physical"
            && command == "serve-claims"
        {
            zerofs_raw_device::claim::broker::serve(Path::new(socket), uid.parse()?, gid.parse()?)?;
            return Ok(());
        }
        if let [mode, command, designation] = args.as_slice()
            && mode == "physical"
            && command == "check-claim"
        {
            let role = designation.parse()?;
            let claim = Claim::physical(role)?;
            println!(
                r#"{{"schema_version":1,"designation":"{designation}","held_device_bytes":{},"exclusive_claim_verified":true}}"#,
                claim.size
            );
            return Ok(());
        }
        let (op, claim, pool, member, role, tail, physical) = match args.as_slice() {
            [mode, op, designation, pool, member, role, tail @ ..] if mode == "physical" => {
                use zerofs_raw_device::claim::PhysicalRole;
                let target: PhysicalRole = designation.parse()?;
                match (target, op.as_str(), role.as_str(), tail) {
                    (PhysicalRole::Nvme, "p3-v2-init-blank" | "p3-v2-inspect", "p3", [_])
                    | (PhysicalRole::Hdd6 | PhysicalRole::Hdd18, "init-blank", "payload", []) => (),
                    _ => return Err("physical operation and storage role must match the fixed designation".into()),
                }
                (op, Claim::physical(target)?, pool, member, role, tail, true)
            }
            [mode, op, node, backing, pool, member, role, tail @ ..] if mode == "fixture" => {
                (op, Claim::allocated_fixture(Path::new(node), Path::new(backing))?, pool, member, role, tail, false)
            }
            [op, node, backing, pool, member, role, tail @ ..] if op != "physical" && op != "fixture" => {
                (op, Claim::synthetic(Path::new(node), Path::new(backing))?, pool, member, role, tail, false)
            }
            _ => return Err("usage: [fixture] OP LOOP_NODE BACKING POOL_UUID MEMBER_UUID ROLE [ARGS], or physical OP DESIGNATION POOL_UUID MEMBER_UUID ROLE [LAYOUT_BYTES]".into()),
        };
        let pool_uuid = uuid(pool)?;
        let member_uuid = uuid(member)?;
        if member_uuid == pool_uuid {
            return Err("pool and member UUIDs must be distinct".into());
        }
        let (op, layout, tail) = if let Some(op) = op.strip_prefix("p3-") {
            let (plan, rest) = tail
                .split_first()
                .ok_or("missing six P3 region sizes in MiB")?;
            let sizes = plan
                .split(',')
                .map(|s| {
                    s.parse::<u64>()?
                        .checked_mul(if physical { 1 } else { 1 << 20 })
                        .ok_or("P3 size overflow".into())
                })
                .collect::<Result<Vec<_>>>()?;
            let lengths = sizes
                .try_into()
                .map_err(|_| "expected six P3 region sizes")?;
            if physical {
                P3Layout::validate_nas_fit(claim.size, lengths)?;
            }
            (op, Some(P3Layout::new(claim.size, lengths)?), rest)
        } else {
            (op.as_str(), None, tail)
        };
        if op == "v2-inspect" && role == "p3" && tail.is_empty() {
            let roles = layout
                .ok_or("P3 inspection needs explicit geometry")?
                .open_v2(claim.device, pool_uuid, member_uuid)?;
            println!("journal_bytes={}", roles.journal.len()?);
            println!("catalog_bytes={}", roles.catalog.len()?);
            for (name, store) in [
                ("authority", &roles.metadata),
                ("keys", &roles.keys),
                ("scratch", &roles.scratch),
                ("cache", &roles.cache),
            ] {
                println!("{name}_objects={}", store.len());
                println!("{name}_tail={}", store.geometry().durable_tail);
            }
            println!(
                "envelope_in_keys={}",
                roles.keys.contains_key(b"db/zerofs.key")
            );
            println!(
                "envelope_in_authority={}",
                roles.metadata.contains_key(b"db/zerofs.key")
            );
            return Ok(());
        }
        if op == "v2-init-blank" && role == "p3" && tail.is_empty() {
            layout
                .ok_or("P3 initialization needs explicit geometry")?
                .provision_v2(claim.device, pool_uuid, member_uuid)?;
            return Ok(());
        }
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
        if op == "verify" && tail.is_empty() && role == Role::Data {
            let reader = store.reader();
            let mut output = io::BufWriter::new(io::stdout().lock());
            for key in store.keys() {
                let object = store.handle(key)?.ok_or("missing indexed object")?;
                reader.read_ranges(&object, &[])?;
                write!(output, "{} ", object.info.len)?;
                for byte in object.info.sha256 {
                    write!(output, "{byte:02x}")?;
                }
                write!(output, " ")?;
                for byte in key {
                    write!(output, "{byte:02x}")?;
                }
                writeln!(output)?;
            }
            output.flush()?;
            return Ok(());
        }
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
