#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    if (argc < 3)
        return 2;
    if (open(argv[1], O_RDWR | O_EXCL) < 0) {
        perror("fixture exclusive open");
        return 2;
    }
    execv(argv[2], &argv[2]);
    perror("fixture exec");
    return 2;
}
