#pragma once
#include <stdint.h>
#include <stdbool.h>

#define SB_MAGIC 0x6368656e646f6e67ull  // "chendong"
#define SB_VERSION 0
#define SB_SIZE 4096

typedef struct __attribute__((packed)) {
    uint64_t magic;
    uint32_t version;
    uint32_t hdr_size;

    uint64_t epoch;
    uint64_t uuid_hi, uuid_lo;

    uint64_t wal_off;
    uint64_t wal_len;
    uint64_t wal_ckpt_lsn; //replay checkpoint

    uint64_t data_off;
    uint64_t data_len;

    uint32_t alloc_unit; //4096

    uint32_t reserved0;

    uint32_t crc32;
    uint8_t pad[SB_SIZE-8-4-4-8-8-8-8-8-8-8-8-4-4-4];
} superblock_t;

bool sb_read_best(int fd, superblock_t* out);
int  sb_write_next(int fd, const superblock_t* cur, const superblock_t* next);
