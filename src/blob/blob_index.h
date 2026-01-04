#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// A minimal append-only blob index.
// Key -> {blob_id, off, len}. Stored in WAL as special records.

#define BLOB_KEY_MAX 64

typedef struct blob_loc_t {
    uint64_t blob_id;
    uint64_t off;
    uint64_t len;
    uint64_t alloc_len;
} blob_loc_t;

typedef struct blob_kv_entry_t {
    char key[BLOB_KEY_MAX];     // null-terminated if shorter
    blob_loc_t loc;
} blob_kv_entry_t;

typedef struct blob_index_t blob_index_t;

blob_index_t *blob_index_create(void);
void blob_index_destroy(blob_index_t *idx);

// Update/insert key->loc
int blob_index_put(blob_index_t *idx, const char *key, const blob_loc_t *loc);

// Lookup
int blob_index_get(blob_index_t *idx, const char *key, blob_loc_t *out);

// Replay an entry during WAL recovery
int blob_index_replay_put(blob_index_t *idx, const char *key, const blob_loc_t *loc);

#ifdef __cplusplus
}
#endif
