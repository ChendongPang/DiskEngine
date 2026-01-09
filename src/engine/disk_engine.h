// ===== file: src/engine/disk_engine.h =====
#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct disk_engine_t disk_engine_t;

// Minimal lifecycle
int disk_engine_open(disk_engine_t **out, const char *path, uint64_t dev_size);

/*
 * Open disk engine with SPDK I/O for WAL + data region.
 *
 * - sb_path is still a POSIX file used for superblock A/B writes (MVP/demo).
 * - bdev_name is the SPDK bdev backing the same storage image/device.
 *
 * NOTE: must be called inside an SPDK thread context (spdk_app_start callback)
 *       because io_init() requires spdk_get_thread().
 */
int disk_engine_open_spdk(disk_engine_t **out, const char *sb_path, const char *bdev_name, uint64_t dev_size);
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
