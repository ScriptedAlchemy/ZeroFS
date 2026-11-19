// One reply from a faulty broker, solely inside the disposable QEMU guest.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

static void fail(const char *operation) {
    perror(operation);
    exit(1);
}

int main(int argc, char **argv) {
    if (argc != 3) return 2;
    const char *path = argv[1], *mode = argv[2];
    struct sockaddr_un addr = {.sun_family = AF_UNIX};
    if (strlen(path) >= sizeof(addr.sun_path)) return 2;
    strcpy(addr.sun_path, path);
    int sock = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (sock < 0 || bind(sock, (struct sockaddr *)&addr, sizeof(addr)) ||
        chmod(path, 0666) || listen(sock, 1)) fail("test endpoint");
    int peer = accept(sock, NULL, NULL);
    if (peer < 0) fail("accept");
    if (!strcmp(mode, "disconnect")) return 0;
    uint8_t response[69] = {0};
    if (recv(peer, response, 37, 0) != 37) return 1;
    if (!strcmp(mode, "missing")) return send(peer, response, sizeof(response), MSG_NOSIGNAL) != sizeof(response);
    const char *node = "/dev/disk/by-id/nvme-CT1000P3SSD8_2318E6D18CE7";
    if (!strcmp(mode, "file")) node = "/dev/null";
    int fd = open(node, O_RDWR | O_CLOEXEC | O_EXCL);
    if (fd < 0) fail("guest test descriptor");
    if (!strcmp(mode, "sequence")) {
        struct stat st;
        uint64_t fields[4] = {0};
        if (fstat(fd, &st) || ioctl(fd, BLKGETDISKSEQ, &fields[1]) ||
            ioctl(fd, BLKGETSIZE64, &fields[2])) fail("guest device geometry");
        fields[0] = st.st_rdev;
        fields[1]++;  // All other device fields match the transferred fd.
        memcpy(response + 37, fields, sizeof(fields));
    } else if (strcmp(mode, "file")) {
        return 2;
    }
    union { struct cmsghdr align; unsigned char bytes[CMSG_SPACE(sizeof(fd))]; } control = {0};
    struct iovec iov = {response, sizeof(response)};
    struct msghdr msg = {.msg_iov = &iov, .msg_iovlen = 1, .msg_control = control.bytes,
                         .msg_controllen = sizeof(control.bytes)};
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(fd));
    memcpy(CMSG_DATA(cmsg), &fd, sizeof(fd));
    if (sendmsg(peer, &msg, MSG_NOSIGNAL) != sizeof(response)) fail("sendmsg");
    return 0;
}
