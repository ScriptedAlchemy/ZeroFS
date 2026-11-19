#!/usr/bin/env python3
"""Mounted xattr and POSIX ACL checks through the Linux kernel 9P client and
the ZeroFS FUSE client, including a server restart.

Run as root inside a disposable container that has CAP_SYS_ADMIN, /dev/fuse,
the `acl` package (setfacl/getfacl) and a built `zerofs` binary. Every path is
under one new temporary fixture directory, which the script removes with its
mounts and server on exit, including on failure. The kernel 9P mount uses the
app's package options. Output is JSON lines on stdout.

The fixture's `file://` store only holds the engine's objects. Attributes and
ACLs live in the engine's metadata rows inside those objects; nothing in this
test sets or reads xattrs on the fixture's host filesystem.

    acl_xattr_mount_e2e.py /path/to/zerofs
"""

import errno
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

KERNEL_9P = "trans=unix,version=9p2000.L,msize=1048576,cache=none,access=user,nodev,nosuid"
OWNER, NAMED, STRANGER, TEAM = 1000, 2000, 3000, 300
FAILURES = []


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def check(name, ok, **detail):
    detail = {k: repr(v) if isinstance(v, bytes) else v for k, v in detail.items()}
    emit("check", name=name, ok=bool(ok), **detail)
    if not ok:
        FAILURES.append(name)


def as_user(uid, groups, argv, **kw):
    """Run argv with the given uid, primary gid and supplementary groups."""
    gid = groups[0] if groups else 65000 + uid
    cmd = ["setpriv", f"--reuid={uid}", f"--regid={gid}"]
    cmd += [f"--groups={','.join(map(str, groups))}"] if groups else ["--clear-groups"]
    return subprocess.run(cmd + argv, capture_output=True, text=True, **kw)


def can(uid, groups, op, path):
    script = {
        "read": "open(p,'rb').read(1)",
        "write": "open(p,'ab').write(b'x')",
        "list": "import os; os.listdir(p)",
        "create": "open(p,'xb').close()",
    }[op]
    r = as_user(uid, groups, ["python3", "-c", f"import sys; p=sys.argv[1]; {script}", path])
    return r.returncode == 0


def getfacl(path, default=False):
    argv = ["getfacl", "-n", "-p", "-c"] + (["-d"] if default else ["-a"]) + [path]
    r = subprocess.run(argv, capture_output=True, text=True)
    lines = [l.split("#")[0].strip() for l in r.stdout.splitlines()]
    return ",".join(l for l in lines if l) if r.returncode == 0 else f"error: {r.stderr.strip()}"


def xattr_matrix(client, root):
    path = os.path.join(root, f"xattrs-{client}")
    open(path, "wb").close()
    os.chown(path, OWNER, OWNER)
    results = {}

    def step(name, fn, expect):
        try:
            got = fn()
            got = "ok" if got is None else got
        except OSError as e:
            got = errno.errorcode.get(e.errno, str(e.errno))
        results[name] = got
        check(f"{client}: xattr {name}", got == expect, got=got, expected=expect)

    step("create", lambda: os.setxattr(path, "user.k", b"v", os.XATTR_CREATE), "ok")
    step("create existing", lambda: os.setxattr(path, "user.k", b"w", os.XATTR_CREATE), "EEXIST")
    step("replace", lambda: os.setxattr(path, "user.k", b"value", os.XATTR_REPLACE), "ok")
    step("read replaced", lambda: os.getxattr(path, "user.k"), b"value")
    step("replace missing", lambda: os.setxattr(path, "user.m", b"v", os.XATTR_REPLACE), "ENODATA")
    step("empty set flags 0", lambda: os.setxattr(path, "user.e", b""), "ok")
    step("read empty set", lambda: os.getxattr(path, "user.e"), b"")
    step("empty create", lambda: os.setxattr(path, "user.c", b"", os.XATTR_CREATE), "ok")
    step("read empty create", lambda: os.getxattr(path, "user.c"), b"")
    step("empty replace", lambda: os.setxattr(path, "user.k", b"", os.XATTR_REPLACE), "ok")
    if client == "kernel-9p":
        # Linux v9fs sends the same Txattrcreate (size 0, XATTR_REPLACE, no
        # Twrite) for an empty replacement and for removexattr. The server
        # keeps removal, so an empty replacement removes the attribute here.
        # See docs/extended-attributes.md. Assert it so a change is noticed.
        emit("known_incompatible", client=client, case="empty XATTR_REPLACE behaves as removal")
        step("read after empty replace", lambda: os.getxattr(path, "user.k"), "ENODATA")
        step("listed after empty replace", lambda: "user.k" in os.listxattr(path), False)
    else:
        step("read after empty replace", lambda: os.getxattr(path, "user.k"), b"")
        step("listed after empty replace", lambda: "user.k" in os.listxattr(path), True)
    step("remove", lambda: os.removexattr(path, "user.e"), "ok")
    step("read removed", lambda: os.getxattr(path, "user.e"), "ENODATA")
    step("remove missing", lambda: os.removexattr(path, "user.e"), "ENODATA")
    emit("xattr_matrix", client=client, results={k: v if isinstance(v, str) else repr(v) for k, v in results.items()})


def acl_suite(client, root):
    base = os.path.join(root, f"acl-{client}")
    os.mkdir(base, 0o755)
    os.chown(base, OWNER, OWNER)
    owner = lambda *argv: as_user(OWNER, [OWNER], list(argv))

    # Access ACL round trip and per-class decisions.
    f = os.path.join(base, "file")
    owner("python3", "-c", "import sys; open(sys.argv[1],'wb').write(b'data')", f)
    os.chmod(f, 0o600)
    r = owner("setfacl", "-m", f"u:{NAMED}:rw-,g:{TEAM}:r--", f)
    check(f"{client}: setfacl access", r.returncode == 0, stderr=r.stderr.strip())
    acl = getfacl(f)
    check(f"{client}: getfacl access round trip",
          acl == f"user::rw-,user:{NAMED}:rw-,group::---,group:{TEAM}:r--,mask::rw-,other::---", got=acl)
    check(f"{client}: mode group bits are the mask", oct(os.stat(f).st_mode & 0o777) == "0o660",
          got=oct(os.stat(f).st_mode & 0o777))
    decisions = {
        "owner write": (can(OWNER, [OWNER], "write", f), True),
        "named user read": (can(NAMED, [], "read", f), True),
        "named user write": (can(NAMED, [], "write", f), True),
        "named group read": (can(STRANGER, [TEAM], "read", f), True),
        "named group write": (can(STRANGER, [TEAM], "write", f), False),
        "owning group read": (can(STRANGER, [OWNER], "read", f), False),
        "other read": (can(STRANGER, [], "read", f), False),
    }
    for name, (got, want) in decisions.items():
        check(f"{client}: {name}", got == want, got=got, expected=want)

    # chmod narrows the mask; the named user loses write but keeps read.
    owner("chmod", "0640", f)
    acl = getfacl(f)
    check(f"{client}: chmod rewrites mask",
          acl == f"user::rw-,user:{NAMED}:rw-,group::---,group:{TEAM}:r--,mask::r--,other::---", got=acl)
    check(f"{client}: named user write after chmod", not can(NAMED, [], "write", f))
    check(f"{client}: named user read after chmod", can(NAMED, [], "read", f))

    # Non-owners cannot change the ACL.
    r = as_user(NAMED, [], ["setfacl", "-m", "o::rwx", f])
    check(f"{client}: non-owner setfacl refused", r.returncode != 0, stderr=r.stderr.strip())

    # Default ACL inheritance.
    d = os.path.join(base, "shared")
    owner("mkdir", "-m", "0750", d)
    r = owner("setfacl", "-m", f"u:{NAMED}:rwx", "-d", "-m", f"u::rwx,u:{NAMED}:rwx,g::r-x,o::---", d)
    check(f"{client}: setfacl default", r.returncode == 0, stderr=r.stderr.strip())
    dacl = getfacl(d, default=True)
    check(f"{client}: getfacl default round trip",
          dacl == f"user::rwx,user:{NAMED}:rwx,group::r-x,mask::rwx,other::---", got=dacl)
    child = os.path.join(d, "child")
    owner("sh", "-c", 'umask 022; : > "$1"', "sh", child)
    cacl = getfacl(child)
    emit("inherited_file", client=client, acl=cacl, mode=oct(os.stat(child).st_mode & 0o7777))
    check(f"{client}: file inherits named entry", f"user:{NAMED}:rwx" in cacl, got=cacl)
    check(f"{client}: named user creates via access ACL", can(NAMED, [], "create", os.path.join(d, "by-named")))
    sub = os.path.join(d, "sub")
    owner("sh", "-c", 'umask 022; mkdir "$1"', "sh", sub)
    check(f"{client}: directory inherits default", getfacl(sub, default=True) == dacl,
          got=getfacl(sub, default=True))
    # The kernel applied umask 022 before the server saw the mode, so the
    # inherited mask is r-x; a local Linux filesystem would ignore the umask
    # here. Documented narrowing: the named user cannot create in `sub`.
    sacl = getfacl(sub)
    emit("inherited_directory", client=client, acl=sacl, mode=oct(os.stat(sub).st_mode & 0o7777))
    check(f"{client}: inherited directory mask narrowed by umask",
          sacl == f"user::rwx,user:{NAMED}:rwx,group::r-x,mask::r-x,other::---", got=sacl)
    check(f"{client}: named user write narrowed by umask", not can(NAMED, [], "create", os.path.join(sub, "n")))
    owner("setfacl", "-m", "m::rwx", sub)
    check(f"{client}: named user creates after mask widened", can(NAMED, [], "create", os.path.join(sub, "n")))
    check(f"{client}: stranger cannot create", not can(STRANGER, [], "create", os.path.join(sub, "x")))
    check(f"{client}: stranger cannot list", not can(STRANGER, [], "list", sub))
    return f, d


def kernel_9p_acl_support():
    try:
        import gzip
        with gzip.open("/proc/config.gz", "rt") as config:
            return "CONFIG_9P_FS_POSIX_ACL=y" in config.read()
    except OSError:
        return None


def cross_client(kroot, froot, f_rel):
    """ACLs set through FUSE, used through the kernel 9P client. The kernel
    9P client with access=user checks mode bits locally, so a named entry
    grants no more than the mode's group or other bits on that mount."""
    f = os.path.join(kroot, f_rel)
    check("kernel-9p: owner read of FUSE-set ACL file", can(OWNER, [OWNER], "read", f))
    check("kernel-9p: other denied on FUSE-set ACL file", not can(STRANGER, [], "read", f))
    named = can(NAMED, [], "read", f)
    emit("kernel_9p_named_user_read", allowed=named,
         note="denied locally by mode bits; documented in permissions-and-acl-contract.md")
    check("kernel-9p: named user limited by local mode check", not named)
    # Server-side enforcement still applies to what reaches it: give other
    # read in the mode but deny it in the ACL's other entry is not possible
    # (other is the mode), so check a named-user deny instead.
    deny = os.path.join(froot, "deny-named")
    as_user(OWNER, [OWNER], ["sh", "-c", 'umask 022; echo x > "$1"', "sh", deny])
    r = as_user(OWNER, [OWNER], ["setfacl", "-m", f"u:{NAMED}:---", deny])
    check("fuse: setfacl named deny", r.returncode == 0, stderr=r.stderr.strip())
    kdeny = os.path.join(kroot, "deny-named")
    check("kernel-9p: other read allowed by mode", can(STRANGER, [], "read", kdeny))
    check("kernel-9p: server denies named user that mode bits allow", not can(NAMED, [], "read", kdeny))


def persisted(client, f, d):
    acl = getfacl(f)
    check(f"{client}: access ACL after restart",
          acl == f"user::rw-,user:{NAMED}:rw-,group::---,group:{TEAM}:r--,mask::r--,other::---", got=acl)
    check(f"{client}: default ACL after restart",
          getfacl(d, default=True) == f"user::rwx,user:{NAMED}:rwx,group::r-x,mask::rwx,other::---",
          got=getfacl(d, default=True))
    check(f"{client}: named read after restart", can(NAMED, [], "read", f))
    check(f"{client}: other denied after restart", not can(STRANGER, [], "read", f))


class Fixture:
    def __init__(self, binary):
        self.binary = binary
        self.root = tempfile.mkdtemp(prefix="zerofs-acl-e2e-")
        self.sock = os.path.join(self.root, "9p.sock")
        self.kmnt = os.path.join(self.root, "kernel")
        self.fmnt = os.path.join(self.root, "fuse")
        os.makedirs(self.kmnt)
        os.makedirs(self.fmnt)
        os.chmod(self.root, 0o755)
        self.config = os.path.join(self.root, "zerofs.toml")
        with open(self.config, "w") as out:
            out.write(f"""
[cache]
dir = "{self.root}/cache"
disk_size_gb = 0.05
memory_size_gb = 0.05
warm_metadata = "off"

[storage]
url = "file://{self.root}/store"
encryption_password = "disposable-fixture-password"

[servers]

[servers.ninep]
unix_socket = "{self.sock}"

[telemetry]
enabled = false
""")
        self.server = None
        self.fuse = None

    def start(self):
        if os.path.exists(self.sock):
            os.unlink(self.sock)
        log = open(os.path.join(self.root, "server.log"), "ab")
        self.server = subprocess.Popen([self.binary, "run", "--config", self.config],
                                       stdout=log, stderr=log, start_new_session=True)
        for _ in range(600):
            if os.path.exists(self.sock):
                break
            if self.server.poll() is not None:
                raise RuntimeError("server exited: " + open(os.path.join(self.root, "server.log")).read()[-4000:])
            time.sleep(0.1)
        else:
            raise RuntimeError("server socket did not appear")
        os.chmod(self.sock, 0o777)
        subprocess.run(["mount", "-t", "9p", "-o", KERNEL_9P, self.sock, self.kmnt], check=True, timeout=30)
        flog = open(os.path.join(self.root, "fuse.log"), "ab")
        self.fuse = subprocess.Popen([self.binary, "mount", f"unix:{self.sock}", self.fmnt, "--access", "all",
                                      "--relaxed-consistency", "false"],
                                     stdout=flog, stderr=flog, start_new_session=True)
        for _ in range(300):
            if os.path.ismount(self.fmnt):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("FUSE mount did not appear")
        os.chmod(self.kmnt, 0o777)

    def stop(self):
        for mnt in (self.fmnt, self.kmnt):
            if os.path.ismount(mnt):
                subprocess.run(["umount", mnt], timeout=30)
            if os.path.ismount(mnt):
                subprocess.run(["umount", "-l", mnt], timeout=30)
        for proc in (self.fuse, self.server):
            if proc and proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.fuse = self.server = None

    def remove(self):
        self.stop()
        shutil.rmtree(self.root)
        emit("fixture_removed", path=self.root, mounted=[m for m in (self.kmnt, self.fmnt) if os.path.ismount(m)])


def main():
    fixture = Fixture(sys.argv[1])
    emit("provenance", kernel=os.uname().release, kernel_9p_options=KERNEL_9P, fixture=fixture.root)
    try:
        fixture.start()
        clients = [("kernel-9p", fixture.kmnt), ("fuse", fixture.fmnt)]
        for client, mnt in clients:
            xattr_matrix(client, mnt)
        acl_clients = [("fuse", fixture.fmnt)]
        support = kernel_9p_acl_support()
        emit("kernel_9p_client_posix_acl", config_9p_fs_posix_acl=support)
        if support:
            acl_clients.insert(0, ("kernel-9p", fixture.kmnt))
        else:
            emit("skipped", scope="kernel-9p setfacl/getfacl suite",
                 reason="client kernel built without CONFIG_9P_FS_POSIX_ACL; setfacl fails in the client")
        saved = {client: acl_suite(client, mnt) for client, mnt in acl_clients}
        cross_client(fixture.kmnt, fixture.fmnt, os.path.relpath(saved["fuse"][0], fixture.fmnt))
        fixture.stop()
        fixture.start()
        for client, mnt in acl_clients:
            f, d = (os.path.relpath(p, mnt) for p in saved[client])
            persisted(client, os.path.join(mnt, f), os.path.join(mnt, d))
        # Kernel 9P sees the same restored rows through mode bits.
        check("kernel-9p: other denied after restart",
              not can(STRANGER, [], "read", os.path.join(fixture.kmnt, os.path.relpath(saved["fuse"][0], fixture.fmnt))))
    finally:
        fixture.remove()
    emit("summary", failures=FAILURES)
    sys.exit(1 if FAILURES else 0)


if __name__ == "__main__":
    main()
