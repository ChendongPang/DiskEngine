#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../src/engine/disk_engine.h"

static void hexdump(const void *buf, size_t len)
{
    const unsigned char *p = (const unsigned char *)buf;
    for (size_t i = 0; i < len; i++) {
        printf("%02x", p[i]);
        if ((i + 1) % 16 == 0) {
            printf("\n");
        } else {
            printf(" ");
        }
    }
    if (len % 16 != 0) {
        printf("\n");
    }
}

int main(int argc, char **argv)
{
    const char *path = "test.img";
    uint64_t dev_size = 64ull * 1024ull * 1024ull;

    if (argc >= 2) {
        path = argv[1];
    }

    disk_engine_t *e = NULL;
    if (disk_engine_open(&e, path, dev_size) != 0) {
        printf("disk_engine_open failed\n");
        return 1;
    }

    printf("opened: %s\n", path);

    const char *k1 = "hello";
    const char *v1 = "world";

    if (disk_engine_put(e, k1, v1, strlen(v1) + 1) != 0) {
        printf("put failed\n");
        disk_engine_close(e);
        return 2;
    }

    void *out = NULL;
    size_t out_len = 0;
    if (disk_engine_get(e, k1, &out, &out_len) != 0) {
        printf("get failed\n");
        disk_engine_close(e);
        return 3;
    }

    printf("get key=%s len=%zu str=%s\n", k1, out_len, (char *)out);
    printf("hexdump:\n");
    hexdump(out, out_len);

    free(out);

    printf("last_blob_id=%llu\n", (unsigned long long)disk_engine_last_blob_id(e));

    disk_engine_close(e);
    return 0;
}
