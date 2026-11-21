#include <fcntl.h>
#include <linux/blkpg.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    if (argc != 3 || (strcmp(argv[2], "add") && strcmp(argv[2], "remove")))
        return 2;
    int fd = open(argv[1], O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        perror("open guest partition parent");
        return 2;
    }
    struct blkpg_partition partition = {
        .start = 1048576,
        .length = 1048576,
        .pno = 1,
    };
    struct blkpg_ioctl_arg request = {
        .op = strcmp(argv[2], "add") ? BLKPG_DEL_PARTITION : BLKPG_ADD_PARTITION,
        .datalen = sizeof(partition),
        .data = &partition,
    };
    int result = ioctl(fd, BLKPG, &request);
    if (result < 0)
        perror("change guest kernel partition");
    close(fd);
    return result < 0 ? 2 : 0;
}
