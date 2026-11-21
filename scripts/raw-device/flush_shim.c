// LD_PRELOAD shim for journal_flush_bench.py. On the one loop device named by
// FLUSH_SHIM_MINOR it logs every positioned write and data sync with the
// fd's last written offset, and optionally adds FLUSH_SHIM_DELAY_US after
// each sync to model a slower device flush. With FLUSH_SHIM_SKIP_SYNC=1 the
// sync on that device is not issued at all, only logged and delayed: the
// bytes stay in the page cache, so the run shows what the engine does above a
// device that is not the limit, and proves nothing about durability. Other
// descriptors pass through.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define MAX_FD 65536
static int target_minor = -1;
static long delay_us;
static int skip_sync;
static int log_fd = -1;
static long long last_offset[MAX_FD];

__attribute__((constructor)) static void init(void) {
    const char *minor = getenv("FLUSH_SHIM_MINOR");
    const char *delay = getenv("FLUSH_SHIM_DELAY_US");
    const char *path = getenv("FLUSH_SHIM_LOG");
    const char *skip = getenv("FLUSH_SHIM_SKIP_SYNC");
    if (skip) skip_sync = atoi(skip);
    if (minor) target_minor = atoi(minor);
    if (delay) delay_us = atol(delay);
    if (path) log_fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
}

static int on_target(int fd) {
    struct stat st;
    return target_minor >= 0 && fd >= 0 && fd < MAX_FD && fstat(fd, &st) == 0 &&
           S_ISBLK(st.st_mode) && major(st.st_rdev) == 7 &&
           (int)minor(st.st_rdev) == target_minor;
}

static void emit(char kind, long long offset, long long len) {
    if (log_fd < 0) return;
    char line[64];
    int n = snprintf(line, sizeof line, "%c %lld %lld\n", kind, offset, len);
    if (write(log_fd, line, n) != n) return;
}

static void wrote(int fd, off_t offset, ssize_t result) {
    if (result > 0 && on_target(fd)) {
        last_offset[fd] = offset;
        emit('W', offset, result);
    }
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
    static ssize_t (*real)(int, const void *, size_t, off_t);
    if (!real) real = dlsym(RTLD_NEXT, "pwrite64");
    ssize_t result = real(fd, buf, count, offset);
    wrote(fd, offset, result);
    return result;
}

ssize_t pwritev64(int fd, const struct iovec *iov, int count, off_t offset) {
    static ssize_t (*real)(int, const struct iovec *, int, off_t);
    if (!real) real = dlsym(RTLD_NEXT, "pwritev64");
    ssize_t result = real(fd, iov, count, offset);
    wrote(fd, offset, result);
    return result;
}

static int synced(int fd, int result) {
    if (on_target(fd)) {
        emit('F', last_offset[fd], 0);
        if (delay_us > 0) {
            struct timespec pause = {delay_us / 1000000, (delay_us % 1000000) * 1000};
            nanosleep(&pause, NULL);
        }
    }
    return result;
}

int fdatasync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fdatasync");
    return synced(fd, skip_sync && on_target(fd) ? 0 : real(fd));
}

int fsync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fsync");
    return synced(fd, skip_sync && on_target(fd) ? 0 : real(fd));
}
