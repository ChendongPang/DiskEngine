#pragma once
/*
 * WAL record format (v0)
 *
 * CRC: CRC32(IEEE) over [header with crc32=0] + payload + pad(zeros).
 * LSN: absolute file offset of record start (monotonic).
 */

#include <stdint.h>

#define WAL_MAGIC   0x57414C5245433030ull  // "WALREC00"
#define WAL_VERSION 1

#define WAL_REC_ALIGN 8

enum wal_rec_type {
    WAL_REC_ALLOC_RANGE = 1,
    WAL_REC_FREE_RANGE  = 2,

    // MVP blob index
    WAL_REC_BLOB_PUT    = 100,
};

typedef struct __attribute__((packed)) wal_rec_hdr_t {
    uint64_t magic;        // WAL_MAGIC
    uint32_t version;      // WAL_VERSION
    uint16_t type;         // enum wal_rec_type
    uint16_t flags;        // reserved (0 for now)

    uint32_t header_sz;    // sizeof(wal_rec_hdr_t) for v0
    uint32_t payload_len;  // bytes (not incl pad)

    uint64_t lsn;          // absolute file offset of this record start
    uint64_t seq;          // monotonic sequence
    uint32_t crc32;        // CRC32(IEEE) of crc32=0
    uint32_t record_total_len; // header_sz + payload_len + pad_len (aligned)
} wal_rec_hdr_t;

// Blob put payload: key -> {blob_id, data_off, data_len}
#define WAL_BLOB_KEY_MAX 64

typedef struct __attribute__((packed)) wal_blob_put_payload_t {
    char     key[WAL_BLOB_KEY_MAX]; // NUL-terminated if shorter
    uint64_t blob_id;
    uint64_t data_off;
    uint64_t data_len;
    uint64_t alloc_len;             // bytes reserved in allocator (aligned >= header+payload)
} wal_blob_put_payload_t;
