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

Insensitive lookup uses single-character Unicode uppercase, from the Rust
standard library, without normalization. A character whose uppercase form
expands to several characters, such as `ß`, keeps its spelling. A recognized
UTF-8 lead byte consumes its full
2, 3, or 4-byte span, even when later bytes are invalid. A truncated span copies
through the end of the name. Other invalid bytes consume one byte. Invalid
spans retain every byte, so `c3 61` and `c3 41` remain distinct. The exported `fs::name_mode::fold_name`
function supplies this rule for lookup and inventory.

NFC and NFD spellings can coexist. The 255-byte filename limit applies to the
supplied name, even when its lookup key grows after folding. A unit test checks
that folding every Unicode scalar value is idempotent.

Creating a file, directory, symlink, hardlink or special node at a fold-equal
name returns `EEXIST`. A rename onto a different entry replaces that entry.
A rename that changes only the spelling of one entry keeps its inode and cookie
and updates its listing name and inode backpointer. An exact-byte rename remains
a no-op. Renaming `x` onto `Y` when `x` and `y` are distinct hardlinks to the same
inode leaves both entries unchanged. POSIX requires this successful no-op when
the two names already refer to the same file.

NFSv3 PATHCONF reports the volume's case-insensitive lookup setting and
case preservation. NFS LOOKUP uses the volume's lookup rule.

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
caching, including on relaxed mounts. Ordinary 9P walks retain folded lookup
unless the listener sets `standard_lookup`, described below. Plain `9P2000.L`
still passes names as bytes. This change does not alter the HA replication
protocol.

## Standard 9P clients

A stock Linux v9fs mount speaks only plain `9P2000.L` and has no exact-spelling
walk. By default the server folds every name such a session sends, so Linux
treats `Foo` and `foo` as one inode and never sends `mv Foo foo` to the server.
The case-only rename silently does nothing.

`standard_lookup` under `[servers.ninep]` takes `"folded"`, the default, or
`"exact"`. With `"exact"`, standard sessions on that listener use the same
stored-spelling check as the private exact walk.

- `Twalk` checks every component. A wrong-case first component returns
  `ENOENT`. A wrong-case later component returns the walked prefix and creates
  no fid, which Linux reports as `ENOENT`.
- `Tunlinkat` and the source name of `Trenameat` match the stored spelling.
  A wrong-case name returns `ENOENT`.
- `Tlcreate`, `Tmkdir`, `Tmknod`, `Tsymlink` and `Tlink` still return `EEXIST`
  for a fold-equal name. The filesystem enforces that rule, not the listener.
- The `Trenameat` destination still resolves by the folded key. A destination
  that is another spelling of the source's own entry performs a case-only
  rename. A destination that is another spelling of a different entry replaces
  that entry under the normal rename rules.
- Directory listings return stored spellings.

Files and directories follow the same rules. The handler holds the directory's
namespace lock across the folded lookup and the spelling check, so an atomic
replacement cannot make a continuously present name return `ENOENT`. Unlink and
rename apply the check during the revalidation they already perform under
their directory locks.

The legacy path-based `Trename` names its source by fid, so its only
client-supplied name is the destination, which follows the destination rule.
The server does not implement `Tremove`. The attach name (`aname`) resolves by
the folded rule in every dialect.

The setting has a cost. A program that opens `movie.mkv` does not find
`Movie.mkv` through that mount, and creating `movie.mkv` beside it returns
`EEXIST` even without `O_EXCL`. A server such as Samba on top of the mount must
match case itself by scanning directories.

The ZeroFS dialects ignore the setting because their clients choose per
request. The web UI connects over WebSocket with the ZeroFS dialect and builds
its handler without the setting. A sensitive filesystem accepts either value
and behaves the same, because its lookup key is the stored spelling. The
setting does not change memory use, and the small NAS package memory policy
admits it.

- Not verified: a stock Linux v9fs mount and Samba with `standard_lookup =
  "exact"`. The tests drive the 9P handler through plain `9P2000.L` sessions
  without a kernel mount.
- Not verified: Samba end to end, Linux v9fs client caching, and native kernel mount refusal require separate acceptance tests.
