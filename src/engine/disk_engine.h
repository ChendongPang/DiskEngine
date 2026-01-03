#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct disk_engine_t disk_engine_t;

// Minimal lifecycle
int disk_engine_open(disk_engine_t **out, const char *path, uint64_t dev_size);
void disk_engine_close(disk_engine_t *e);

// Minimal KV -> blob interface
int disk_engine_put(disk_engine_t *e, const char *key, const void *data, size_t len);
int disk_engine_get(disk_engine_t *e, const char *key, void **out_buf, size_t *out_len);

// For demo/inspection
uint64_t disk_engine_last_blob_id(disk_engine_t *e);

int disk_engine_checkpoint(disk_engine_t *e);

#ifdef __cplusplus
}
#endif
