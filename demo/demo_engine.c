#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "superblock/superblock.h"
#include "common/crc32_ieee.h"
#include "wal/wal.h"
#include "wal/wal_apply_range.h"
#include "alloc/range_alloc.h"


static void die_errno(const char* msg)
{
    fprintf(stderr, "%s: %s\n", msg, strerror(errno));
    exit(1);
}

static int ensure_size(int fd, uint64_t size)
{
    struct stat st;
    if (fstat(fd, &st) != 0) return -errno;
    if ((uint64_t)st.st_size == size) return 0;
    if (ftruncate(fd, (off_t)size) != 0) return -errno;
    return 0;
}

static int write_full(int fd, const void* buf, size_t n, off_t off)
{
    const uint8_t* p = (const uint8_t*)buf;
    size_t left = n;

    while (left > 0) {
        ssize_t w = pwrite(fd, p, left, off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -errno;
        }
        if (w == 0) return -EIO;

        p += (size_t)w;
        off += (off_t)w;
        left -= (size_t)w;
    }
    return 0;
}

static int wipe_zero(int fd, uint64_t off, uint64_t len)
{
    const size_t chunk = 1024 * 1024;
    char* buf = (char*)calloc(1, chunk);
    if (!buf) return -ENOMEM;

    uint64_t pos = 0;
    while (pos < len) {
        size_t n = (len - pos > chunk) ? chunk : (size_t)(len - pos);
        ssize_t w = pwrite(fd, buf, n, (off_t)(off + pos));
        if (w < 0) {
            int e = -errno;
            free(buf);
            return e;
        }
        if ((size_t)w != n) {
            free(buf);
            return -EIO;
        }
        pos += (uint64_t)n;
    }

    free(buf);
    return 0;
}

static uint32_t sb_crc_compute(const superblock_t* sb)
{
    superblock_t tmp;
    memcpy(&tmp, sb, SB_SIZE);
    tmp.crc32 = 0;
    return crc32_ieee(&tmp, SB_SIZE);
}

static int format_image(int fd, uint64_t img_size)
{
    /* 简单起见：镜像必须至少容纳 SB(8KB) + WAL(4MB) + DATA(至少 1MB) */
    const uint64_t sb_area  = 2ULL * SB_SIZE;          /* slot0 + slot1 */
    const uint64_t wal_len  = 4ULL * 1024 * 1024;      /* 4MB */
    const uint64_t data_min = 1ULL * 1024 * 1024;

    if (img_size < sb_area + wal_len + data_min) return -EINVAL;

    /* 清零 WAL 区域（以及后面你也可以选择清零 data 区域） */
    {
        int r = wipe_zero(fd, sb_area, wal_len);
        if (r < 0) return r;
    }

    superblock_t sb;
    memset(&sb, 0, sizeof(sb));

    sb.magic   = SB_MAGIC;
    sb.version = SB_VERSION;

    /* 你这里的字段名以你工程为准：你贴的是 hdr_size */
    sb.hdr_size = (uint32_t)sizeof(superblock_t);

    sb.epoch = 1;

    sb.uuid_hi = (uint64_t)time(NULL);
    sb.uuid_lo = ((uint64_t)getpid() << 32) ^ (uint64_t)random();

    sb.wal_off      = sb_area;
    sb.wal_len      = wal_len;
    sb.wal_ckpt_lsn = 0;

    sb.data_off = sb.wal_off + sb.wal_len;
    sb.data_len = img_size - sb.data_off;

    sb.alloc_unit = 4096;
    sb.reserved0  = 0;

    sb.crc32 = 0;
    sb.crc32 = sb_crc_compute(&sb);

    /* slot0 */
    {
        int r = write_full(fd, &sb, SB_SIZE, 0);
        if (r != 0) return r;
    }

    /* slot1：更大 epoch 的副本 */
    superblock_t sb2 = sb;
    sb2.epoch = 2;
    sb2.crc32 = 0;
    sb2.crc32 = sb_crc_compute(&sb2);

    {
        int r = write_full(fd, &sb2, SB_SIZE, (off_t)SB_SIZE);
        if (r != 0) return r;
    }

    if (fsync(fd) != 0) return -errno;
    return 0;
}

static void print_sb(const superblock_t* sb)
{
    printf("SB: epoch=%" PRIu64 " uuid=%" PRIu64 ":%" PRIu64 "\n",
           sb->epoch, sb->uuid_hi, sb->uuid_lo);
    printf("    wal_off=%" PRIu64 " wal_len=%" PRIu64 " wal_ckpt_lsn=%" PRIu64 "\n",
           sb->wal_off, sb->wal_len, sb->wal_ckpt_lsn);
    printf("    data_off=%" PRIu64 " data_len=%" PRIu64 " alloc_unit=%u\n",
           sb->data_off, sb->data_len, sb->alloc_unit);
}

int main(int argc, char** argv)
{
    const char* path = "disk.img";
    uint64_t img_size = 64ULL * 1024 * 1024;
    int do_format = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--format") == 0) {
            do_format = 1;
        } else if (strcmp(argv[i], "--path") == 0 && i + 1 < argc) {
            path = argv[++i];
        } else if (strcmp(argv[i], "--size") == 0 && i + 1 < argc) {
            img_size = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else {
            fprintf(stderr, "Usage: %s [--format] [--path disk.img] [--size bytes]\n", argv[0]);
            return 2;
        }
    }

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) die_errno("open");

    if (ensure_size(fd, img_size) != 0) die_errno("ensure_size");

    if (do_format) {
        int r = format_image(fd, img_size);
        if (r != 0) {
            fprintf(stderr, "format_image failed: %d\n", r);
            close(fd);
            return 1;
        }
        printf("Formatted image %s size=%" PRIu64 "\n", path, img_size);
    }

    superblock_t sb;
    if (!sb_read_best(fd, &sb)) {
        fprintf(stderr, "No valid superblock found. Run with --format first.\n");
        close(fd);
        return 1;
    }
    print_sb(&sb);

    wal_t wal;
    {
        int r = wal_open(fd, &sb, sb.wal_ckpt_lsn, &wal);
        if (r != 0) {
            fprintf(stderr, "wal_open failed: %d\n", r);
            close(fd);
            return 1;
        }
    }

    range_alloc_t a;
    {
        int r = ra_init(&a, sb.data_off, sb.data_len);
        if (r != 0) {
            fprintf(stderr, "ra_init failed: %d\n", r);
            close(fd);
            return 1;
        }
    }

    uint64_t last_lsn = 0, last_seq = 0;
    {
        int r = wal_replay(&wal, sb.wal_ckpt_lsn, wal_apply_range, &a, &last_lsn, &last_seq);
        if (r != 0) {
            fprintf(stderr, "wal_replay failed: %d\n", r);
            ra_destroy(&a);
            close(fd);
            return 1;
        }
    }

    printf("Replay done. last_lsn=%" PRIu64 " last_seq=%" PRIu64 "\n", last_lsn, last_seq);
    ra_dump_free(&a);

    /* demo workload: alloc 64K, alloc 128K, free first */
    int r = 0;
    uint64_t off1 = 0, off2 = 0;

    r = ra_alloc(&a, 64 * 1024, &off1);
    if (r != 0) {
        fprintf(stderr, "ra_alloc(64K) failed: %d\n", r);
        goto out;
    }

    {
        wal_range_payload_t p;
        p.off = off1;
        p.len = 64 * 1024;

        r = wal_append(&wal, WAL_REC_ALLOC_RANGE, &p, (uint32_t)sizeof(p), NULL, NULL);
        if (r != 0) {
            fprintf(stderr, "wal_append ALLOC1 failed: %d\n", r);
            goto out;
        }
        r = wal_sync(&wal);
        if (r != 0) {
            fprintf(stderr, "wal_sync after ALLOC1 failed: %d\n", r);
            goto out;
        }
    }

    r = ra_alloc(&a, 128 * 1024, &off2);
    if (r != 0) {
        fprintf(stderr, "ra_alloc(128K) failed: %d\n", r);
        goto out;
    }

    {
        wal_range_payload_t p;
        p.off = off2;
        p.len = 128 * 1024;

        r = wal_append(&wal, WAL_REC_ALLOC_RANGE, &p, (uint32_t)sizeof(p), NULL, NULL);
        if (r != 0) {
            fprintf(stderr, "wal_append ALLOC2 failed: %d\n", r);
            goto out;
        }
        r = wal_sync(&wal);
        if (r != 0) {
            fprintf(stderr, "wal_sync after ALLOC2 failed: %d\n", r);
            goto out;
        }
    }

    r = ra_free(&a, off1, 64 * 1024);
    if (r != 0) {
        fprintf(stderr, "ra_free(off1) failed: %d\n", r);
        goto out;
    }

    {
        wal_range_payload_t p;
        p.off = off1;
        p.len = 64 * 1024;

        r = wal_append(&wal, WAL_REC_FREE_RANGE, &p, (uint32_t)sizeof(p), NULL, NULL);
        if (r != 0) {
            fprintf(stderr, "wal_append FREE failed: %d\n", r);
            goto out;
        }
        r = wal_sync(&wal);
        if (r != 0) {
            fprintf(stderr, "wal_sync after FREE failed: %d\n", r);
            goto out;
        }
    }

    printf("After ops:\n");
    ra_dump_free(&a);

out:
    ra_destroy(&a);
    close(fd);
    return (r == 0) ? 0 : 1;
}
