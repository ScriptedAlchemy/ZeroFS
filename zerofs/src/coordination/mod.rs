//! Shared coordination primitives: a generic FIFO byte-budget admission gate
//! and a typed contiguous-sequence durability barrier. The writeback layer
//! plugs its tier policies and error vocabularies into these; later layers
//! (the filesystem mutation path) consume the same primitives.

pub(crate) mod admission;
pub(crate) mod sequence;
