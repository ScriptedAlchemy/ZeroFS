#[cfg(test)]
mod tests {
    use crate::raw_database::RawDatabase;
    use redb::{
        Builder, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition,
    };
    use std::io;
    use zerofs_raw_device::journal_region::TransactionRegion;
    use zerofs_raw_device::{Identity, Role};

    const PAYLOADS: TableDefinition<u64, &[u8]> = TableDefinition::new("payloads");
    const WATERMARK: TableDefinition<u64, u64> = TableDefinition::new("remote_watermark");
    const REGION_LEN: u64 = 16 << 20;
    const RESERVE_BYTES: u64 = 2 << 20;
    const PAYLOAD_LEN: usize = 256 << 10;
    const ROUNDS: u64 = 128;
    const ID: Identity = Identity {
        pool_uuid: [3; 16],
        member_uuid: [4; 16],
        role: Role::Journal,
    };

    #[test]
    fn remote_retirement_reuses_pages_and_committed_state_survives_reopen() {
        assert!(ROUNDS * PAYLOAD_LEN as u64 > REGION_LEN);
        let file = tempfile::tempfile().unwrap();
        file.set_len(REGION_LEN).unwrap();
        let region =
            TransactionRegion::provision(file.try_clone().unwrap(), REGION_LEN, 0, REGION_LEN, ID)
                .unwrap();
        let ceiling = region.capacity() - RESERVE_BYTES;
        let region = region.with_resize_ceiling(ceiling).unwrap();
        let db = Builder::new()
            .create_with_backend(RawDatabase(region))
            .unwrap();

        let mut payload = vec![0x35; PAYLOAD_LEN];
        for seq in 1..=ROUNDS {
            payload[..8].copy_from_slice(&seq.to_be_bytes());

            let mut tx = db.begin_write().unwrap();
            tx.set_durability(Durability::Immediate).unwrap();
            tx.open_table(PAYLOADS)
                .unwrap()
                .insert(seq, payload.as_slice())
                .unwrap();
            tx.commit().unwrap();

            if seq != ROUNDS {
                let mut tx = db.begin_write().unwrap();
                tx.set_durability(Durability::Immediate).unwrap();
                tx.open_table(WATERMARK).unwrap().insert(0, seq).unwrap();
                tx.open_table(PAYLOADS).unwrap().remove(seq).unwrap();
                tx.commit().unwrap();
            }
        }

        // Copy the on-media state while redb's writer is still open. This
        // skips redb's clean-close allocator checkpoint on the recovered copy.
        let mut source = file.try_clone().unwrap();
        let crash_copy = tempfile::tempfile().unwrap();
        io::copy(&mut source, &mut crash_copy.try_clone().unwrap()).unwrap();
        drop(db);
        let region = TransactionRegion::open(crash_copy, REGION_LEN, 0, REGION_LEN, ID).unwrap();
        assert!(region.len().unwrap() <= ceiling);
        let region = region.with_resize_ceiling(ceiling).unwrap();
        let db = Builder::new()
            .create_with_backend(RawDatabase(region))
            .unwrap();
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(WATERMARK)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            ROUNDS - 1
        );
        let table = read.open_table(PAYLOADS).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        assert_eq!(table.get(ROUNDS).unwrap().unwrap().value(), payload);
    }
}
