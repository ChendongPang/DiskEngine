#pragma once
#include <stdint.h>

typedef struct __attribute__((packed)) wal_range_payload_t {
    uint64_t off;
    uint64_t len;
} wal_range_payload_t;

int wal_apply_range(uint16_t type,
                    const void* payload,
                    uint32_t payload_len,
                    uint64_t lsn,
                    uint64_t seq,
                    void* arg);
