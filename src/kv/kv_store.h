#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kv_store_t kv_store_t;

int  kv_open(kv_store_t **out, const char *path, uint64_t dev_size);
void kv_close(kv_store_t *kv);

int kv_put(kv_store_t *kv, const char *key, const void *val, size_t len);
int kv_get(kv_store_t *kv, const char *key, void **out_buf, size_t *out_len);
int kv_del(kv_store_t *kv, const char *key);

uint64_t kv_last_blob_id(kv_store_t *kv);

#ifdef __cplusplus
}
#endif
