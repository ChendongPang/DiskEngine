#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "kv/kv_store.h"

static void die(int r, const char *what)
{
    if (r >= 0) return;
    fprintf(stderr, "%s failed: %d (%s)\n", what, r, strerror(-r));
    exit(2);
}

static void show_get(kv_store_t *kv, const char *k)
{
    void *out = NULL;
    size_t out_len = 0;
    int r = kv_get(kv, k, &out, &out_len);
    if (r == -ENOENT) {
        printf("get key=%s -> ENOENT (deleted)\n", k);
        return;
    }
    die(r, "kv_get");

    printf("get key=%s len=%zu val=", k, out_len);
    if (out_len > 0) {
        fwrite(out, 1, out_len, stdout);
    }
    printf("\n");
    free(out);
}

int main(int argc, char **argv)
{
    const char *path = (argc > 1) ? argv[1] : "my_kv.img";
    uint64_t dev_size = 64ULL * 1024 * 1024; // 64MB

    kv_store_t *kv = NULL;
    int r = kv_open(&kv, path, dev_size);
    die(r, "kv_open");

    printf("=== Phase 1: PUT A=v1 ===\n");
    die(kv_put(kv, "A", "v1", 2), "kv_put");
    show_get(kv, "A");

    printf("=== Phase 2: overwrite PUT A=v2 ===\n");
    die(kv_put(kv, "A", "v2", 2), "kv_put");
    show_get(kv, "A");

    printf("=== Phase 3: DEL A (tombstone) ===\n");
    die(kv_del(kv, "A"), "kv_del");
    show_get(kv, "A");

    printf("last_blob_id=%llu\n", (unsigned long long)kv_last_blob_id(kv));

    kv_close(kv);
    kv = NULL;

    printf("=== Phase 4: restart & verify A still deleted ===\n");
    die(kv_open(&kv, path, dev_size), "kv_open");
    show_get(kv, "A");
    printf("last_blob_id=%llu\n", (unsigned long long)kv_last_blob_id(kv));

    kv_close(kv);
    return 0;
}
