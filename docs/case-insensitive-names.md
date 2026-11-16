# Case-insensitive names

The experimental `name_mode` option belongs to `[filesystem]`.
Omission or `name_mode = "sensitive"` keeps byte-exact names.
Set `name_mode = "insensitive"` only when creating a filesystem.
The mode cannot be changed on an existing filesystem.

The shared object store holds `zerofs.namemode` beside `zerofs.key` under
the database path. Every node checks this record during startup, before
it starts a replication receiver or opens the database. A fresh writable
store creates the mode record with a conditional create before it initializes
the encryption key. Concurrent initializers read the winning record and
refuse a different configured mode.

The record contains two bytes. The first byte is the format version, currently
1. The second byte is 0 for sensitive mode or 1 for Unicode 5.0.0 simple
uppercase without normalization. Unknown versions, unknown modes and malformed
records prevent startup. A record without a key retains the selected mode
when initialization resumes. Read-only opens never create the mode record.
A key without a record identifies a legacy sensitive filesystem. It accepts
sensitive opens without writing a record and refuses insensitive opens.

Old binaries ignore this record. An insensitive filesystem must never be
served by a binary that lacks this feature. The new binary cannot enforce
that restriction on an old binary. Backups and metadata recovery must preserve
`zerofs.namemode` along with `zerofs.key`.

- Not verified: Linux FUSE, the native kernel client and Samba acceptance have not run on this Mac. Equivalence with a running ZFS filesystem remains unverified.
