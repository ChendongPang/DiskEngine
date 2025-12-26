#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ra_range {
  uint64_t off;
  uint64_t len;
} ra_range_t;

typedef struct ra_node {
  ra_range_t r;
  struct ra_node* next;
} ra_node_t;

typedef struct range_alloc {
  ra_node_t* free_head;     // sorted by off, non-overlapping
  uint64_t   free_bytes;    // total free bytes (debug/stat)
  uint64_t   base_off;      // managed region start (for sanity)
  uint64_t   total_len;     // managed region length
} range_alloc_t;

// Initialize allocator with one big free range [base_off, base_off+total_len).
int ra_init(range_alloc_t* a, uint64_t base_off, uint64_t total_len);

// Destroy (free all nodes).
void ra_destroy(range_alloc_t* a);

// Allocate 'len' bytes (must be >0). First-fit.
// Returns 0 on success and fills out_off. Returns -ENOSPC if not enough space.
int ra_alloc(range_alloc_t* a, uint64_t len, uint64_t* out_off);

// Free a previously allocated range [off, off+len).
// Inserts while keeping order; coalesces neighbors.
// Returns 0, or -EINVAL on invalid/overlap.
int ra_free(range_alloc_t* a, uint64_t off, uint64_t len);

// Replay helper: subtract a specific allocated range from free list.
// Used when applying WAL ALLOC_RANGE during recovery.
// Returns 0 if removed successfully, -ENOENT if the range isn't fully free,
// or -EINVAL on invalid params.
int ra_mark_alloc(range_alloc_t* a, uint64_t off, uint64_t len);

// Debug/inspection: dump free list to stderr (optional).
void ra_dump_free(const range_alloc_t* a);

#ifdef __cplusplus
}
#endif
