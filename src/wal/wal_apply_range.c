#include "../wal/wal.h"
#include "../alloc/range_alloc.h"

#include <errno.h>
#include <string.h>

typedef struct __attribute__((packed)) wal_range_payload_t {
  uint64_t off;
  uint64_t len;
} wal_range_payload_t;

// apply callback: apply ALLOC/FREE to range allocator
int wal_apply_range(uint16_t type,
                    const void* payload,
                    uint32_t payload_len,
                    uint64_t lsn,
                    uint64_t seq,
                    void* arg) {
    (void)lsn;
    (void)seq;

    if (!arg) return -EINVAL;
    range_alloc_t* a = (range_alloc_t*)arg;

    if (payload_len != sizeof(wal_range_payload_t) || payload == NULL) return -EINVAL;

    wal_range_payload_t p;
    memcpy(&p, payload, sizeof(p));

    switch (type) {
        case WAL_REC_ALLOC_RANGE:
        // On replay, ALLOC means: subtract [off,len] from free list
        return ra_mark_alloc(a, p.off, p.len);

        case WAL_REC_FREE_RANGE:
        // FREE means: add [off,len] back to free list
        return ra_free(a, p.off, p.len);

        default:
        return -EINVAL;
    }
}
