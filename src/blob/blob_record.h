#pragma once

#include <stdint.h>
#include <stddef.h>

/*
 * Minimal on-disk blob record stored in the data region:
 *
 *      [blob_record_hdr_t][payload bytes]
 *
 * Write ordering (redo-style):
 *      1) write payload
 *      2) write header
 *      3) fdatasync
 * Then commit metadata into WAL (alloc + key->blob mapping).
 *
 * Read verifies:
 *      - magic/version
 *      - header CRC
 *      - payload CRC
 */

#define BLOB_REC_MAGIC 0x424C4F4252454331ull  /* "BLOBREC1" */

typedef struct __attribute__((packed)) blob_record_hdr_t {
    uint64_t magic;            /* BLOB_REC_MAGIC */
    uint16_t version;          /* 1 */
    uint16_t reserved0;
    uint32_t payload_len;      /* bytes */
    uint32_t payload_crc32;    /* crc32 over payload */
    uint32_t header_crc32;     /* crc32 over this header with header_crc32 set to 0 */
} blob_record_hdr_t;

static inline uint64_t blob_record_total_len(uint32_t payload_len) {
    return (uint64_t)sizeof(blob_record_hdr_t) + (uint64_t)payload_len;
}

/* Prepare header with computed CRCs. */
void blob_record_build_header(blob_record_hdr_t* h, const void* payload, uint32_t payload_len);
