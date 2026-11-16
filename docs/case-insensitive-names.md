# Case-insensitive names

The experimental `name_mode` option belongs to `[filesystem]`.
Omission or `name_mode = "sensitive"` keeps byte-exact names.
Set `name_mode = "insensitive"` only when creating a filesystem.
The mode cannot be changed on an existing filesystem.

The shared object store holds the name mode inside `zerofs.key` under the
database path. Every node checks it while loading the encryption key, before
it starts a replication receiver or opens the database. A fresh writable
store creates the key with a conditional create. Concurrent initializers read
the winning key and refuse a different configured mode. Read-only opens never
create a key. Password changes preserve the envelope version and mode.

Sensitive filesystems keep the legacy bincode `WrappedDataKey` bytes. A legacy
envelope always means sensitive mode. Insensitive filesystems use a version 2
envelope. Its first eight bytes are `ff ff ff ff ff ff ff ff`, followed by the
version byte `02`, mode byte `01`, and the legacy wrapped key fields. The mode
byte `00` means sensitive. Unknown versions, unknown modes, and malformed keys
prevent startup. Older binaries read the magic as an impossible string length
and refuse to open the filesystem. Their key loader propagates that parse
error and never initializes a replacement key.

Insensitive lookup uses Unicode 5.0.0 simple uppercase without normalization.
The byte-span rules match OpenZFS 2.3.4 `u8_textprep_str` with
`U8_TEXTPREP_IGNORE_INVALID`. A recognized UTF-8 lead byte consumes its full
2, 3, or 4-byte span, even when later bytes are invalid. A truncated span copies
through the end of the name. Other invalid bytes consume one byte. Invalid
spans retain every byte, so `c3 61` and `c3 41` remain distinct. Valid spans fold
with the pinned uppercase table. The exported `fs::name_mode::fold_name`
function supplies this rule for lookup and inventory.

NFC and NFD spellings can coexist. The 255-byte filename limit applies to the
supplied name, even when its lookup key grows after folding. The offline
reference generator reads the OpenZFS tables as data and compares every one-
and two-byte string plus 200,000 seeded strings of three to eight bytes.

Creating a file, directory, symlink, hardlink or special node at a fold-equal
name returns `EEXIST`. A rename onto a different entry replaces that entry.
A rename that changes only the spelling of one entry keeps its inode and cookie
and updates its listing name and inode backpointer. An exact-byte rename remains
a no-op. Renaming `x` onto `Y` when `x` and `y` are distinct hardlinks to the same
inode leaves both entries unchanged. POSIX requires this successful no-op when
the two names already refer to the same file.

Private 9P clients receive `9P2000.L.Z.CI` for an insensitive export and
`9P2000.L.Z` for a sensitive export. The userspace client accepts both and
retains the selected mode for the logical session. It rejects a reconnect
candidate with a different mode before replaying requests. The native kernel
client accepts only the sensitive dialect and refuses an insensitive mount.
Older userspace clients also refuse the new dialect.

Local programs on an insensitive FUSE mount use exact-spelling lookup.
For example, a stored `Foo` is visible as `Foo`, while looking up `foo` returns
`ENOENT`. Creating `foo` still returns `EEXIST` because the store excludes
fold-equal collisions. This lets Linux call the filesystem for a case-only
rename instead of treating two spellings of the same inode as a no-op.
The bridge uses a private exact-spelling walk and disables negative-entry
caching, including on relaxed mounts. Ordinary 9P walks retain folded lookup.
Plain `9P2000.L` still passes names as bytes. This change does not alter the
HA replication protocol.

- Not verified: Samba end to end, Linux v9fs client caching, native kernel mount refusal, and equivalence with a running ZFS filesystem require separate acceptance tests.
