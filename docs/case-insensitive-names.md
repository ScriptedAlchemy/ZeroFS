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

Insensitive lookup uses Unicode 5.0.0 simple uppercase. It leaves invalid UTF-8
bytes unchanged and folds valid characters around them. It does not normalize
names, so NFC and NFD spellings can coexist. The 255-byte filename limit applies
to the supplied name, even when its lookup key grows after folding.

Creating a file, directory, symlink, hardlink or special node at a fold-equal
name returns `EEXIST`. A rename onto a different entry replaces that entry.
A rename that changes only the spelling of one entry keeps its inode and cookie
and updates its listing name and inode backpointer. An exact-byte rename remains
a no-op. Renaming `x` onto `Y` when `x` and `y` are distinct hardlinks to the same
inode leaves both entries unchanged. POSIX requires this successful no-op when
the two names already refer to the same file.
