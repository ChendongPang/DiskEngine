#include "disk_engine.h"

#include "../superblock/superblock.h"
#include "../wal/wal.h"
#include "../wal/wal_apply_range.h"
#include "../alloc/range_alloc.h"
#include "../blob/blob_index.h"
#include "../blob/blob_record.h"
#include "../common/crc32_ieee.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

struct disk_engine_t {
    int fd;
    superblock_t sb;

    range_alloc_t alloc;
    wal_t wal;

    blob_index_t *idx;
    uint64_t next_blob_id;
};

static inline uint64_t align_up_u64(uint64_t x, uint64_t a)
{
    if (a == 0) return x;
    return (x + (a - 1)) & ~(a - 1);
}

static int ensure_size(int fd, uint64_t size)
{
    struct stat st;
    if (fstat(fd, &st) != 0) return -errno;
    if ((uint64_t)st.st_size >= size) return 0;
    if (ftruncate(fd, (off_t)size) != 0) return -errno;
    return 0;
}

static int pwrite_full(int fd, const void *buf, size_t n, uint64_t off)
{
    const uint8_t *p = (const uint8_t*)buf;
    size_t left = n;
    uint64_t cur = off;
    while (left > 0) {
        ssize_t w = pwrite(fd, p, left, (off_t)cur);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -errno;
        }
        if (w == 0) return -EIO;
        p += (size_t)w;
        left -= (size_t)w;
        cur += (uint64_t)w;
    }
    return 0;
}

static int pread_full(int fd, void *buf, size_t n, uint64_t off)
{
    uint8_t *p = (uint8_t*)buf;
    size_t left = n;
    uint64_t cur = off;
    while (left > 0) {
        ssize_t r = pread(fd, p, left, (off_t)cur);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -errno;
        }
        if (r == 0) return -EIO;
        p += (size_t)r;
        left -= (size_t)r;
        cur += (uint64_t)r;
    }
    return 0;
}

static uint32_t sb_crc_compute(const superblock_t* sb)
{
    superblock_t tmp;
    memcpy(&tmp, sb, SB_SIZE);
    tmp.crc32 = 0;
    return crc32_ieee(&tmp, SB_SIZE);
}

static int wipe_zero(int fd, uint64_t off, uint64_t len)
{
    const size_t chunk = 1024 * 1024;
    void *buf = calloc(1, chunk);
    if (!buf) return -ENOMEM;

    uint64_t pos = 0;
    while (pos < len) {
        size_t n = (len - pos > chunk) ? chunk : (size_t)(len - pos);
        int r = pwrite_full(fd, buf, n, off + pos);
        if (r != 0) { free(buf); return r; }
        pos += (uint64_t)n;
    }
    free(buf);
    return 0;
}

/*
 * If the image is unformatted (no valid SB), do a minimal format:
 * [SB slot0][SB slot1][WAL 4MB][DATA rest]
 *
 * This is intentionally minimal: no dedicated metadata region yet.
 */
static int format_if_needed(int fd, uint64_t dev_size, superblock_t *out_sb)
{
    superblock_t sb;
    if (sb_read_best(fd, &sb)) {
        if (out_sb) memcpy(out_sb, &sb, SB_SIZE);
        return 0;
    }

    const uint64_t sb_area = 2ULL * SB_SIZE;
    const uint64_t wal_len = 4ULL * 1024 * 1024;
    const uint64_t data_min = 1ULL * 1024 * 1024;

    if (dev_size < sb_area + wal_len + data_min) return -EINVAL;

    int r = wipe_zero(fd, sb_area, wal_len);
    if (r != 0) return r;

    memset(&sb, 0, sizeof(sb));
    sb.magic = SB_MAGIC;
    sb.version = SB_VERSION;
    sb.hdr_size = (uint32_t)sizeof(superblock_t);

    sb.epoch = 1;
    sb.uuid_hi = (uint64_t)time(NULL);
    sb.uuid_lo = ((uint64_t)getpid() << 32) ^ (uint64_t)random();

    sb.wal_off = sb_area;
    sb.wal_len = wal_len;
    sb.wal_ckpt_lsn = 0; // MVP: no snapshot, replay from wal_off

    sb.data_off = sb.wal_off + sb.wal_len;
    sb.data_len = dev_size - sb.data_off;

    sb.alloc_unit = 4096;
    sb.reserved0 = 0;

    sb.crc32 = 0;
    sb.crc32 = sb_crc_compute(&sb);

    // slot0
    r = pwrite_full(fd, &sb, SB_SIZE, 0);
    if (r != 0) return r;

    // slot1 with higher epoch
    superblock_t sb2 = sb;
    sb2.epoch = 2;
    sb2.crc32 = 0;
    sb2.crc32 = sb_crc_compute(&sb2);

    r = pwrite_full(fd, &sb2, SB_SIZE, (uint64_t)SB_SIZE);
    if (r != 0) return r;

    if (fsync(fd) != 0) return -errno;

    if (out_sb) memcpy(out_sb, &sb2, SB_SIZE); // newest
    return 0;
}

typedef struct replay_ctx_t {
    disk_engine_t *e;
} replay_ctx_t;

static int wal_apply_engine(uint16_t type,
                            const void* payload,
                            uint32_t payload_len,
                            uint64_t lsn,
                            uint64_t seq,
                            void* arg)
{
    (void)lsn;
    (void)seq;

    replay_ctx_t *ctx = (replay_ctx_t*)arg;
    disk_engine_t *e = ctx ? ctx->e : NULL;
    if (!e) return -EINVAL;

    // Blob index updates
    if (type == WAL_REC_BLOB_PUT) {
        if (payload_len != (uint32_t)sizeof(wal_blob_put_payload_t)) return -EINVAL;
        const wal_blob_put_payload_t *p = (const wal_blob_put_payload_t*)payload;

        // Recovery contract (MVP): a single BLOB_PUT record is the commit point.
        // It must be sufficient to rebuild both allocator state and the key->blob mapping.
        if (p->alloc_len == 0) return -EINVAL;
        int r = ra_mark_alloc(&e->alloc, p->data_off, p->alloc_len);
        if (r != 0) return r;

        blob_loc_t loc;
        loc.blob_id = p->blob_id;
        loc.off = p->data_off;      // blob record start offset
        loc.len = p->data_len;      // user payload len

        int r2 = blob_index_replay_put(e->idx, p->key, &loc);
        if (r2 != 0) return r2;

        if (p->blob_id >= e->next_blob_id) e->next_blob_id = p->blob_id + 1;
        return 0;
    }

    // Unknown record types: ignore for forward compatibility
    return 0;
}

int disk_engine_open(disk_engine_t **out, const char *path, uint64_t dev_size)
{
    if (!out || !path) return -EINVAL;

    disk_engine_t *e = (disk_engine_t*)calloc(1, sizeof(*e));
    if (!e) return -ENOMEM;

    e->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (e->fd < 0) { free(e); return -errno; }

    int r = ensure_size(e->fd, dev_size);
    if (r != 0) { close(e->fd); free(e); return r; }

    r = format_if_needed(e->fd, dev_size, &e->sb);
    if (r != 0) { close(e->fd); free(e); return r; }

    // Open WAL (tail located by scanning)
    r = wal_open(e->fd, &e->sb, e->sb.wal_ckpt_lsn, &e->wal);
    if (r != 0) { close(e->fd); free(e); return r; }

    // Init allocator over data region
    r = ra_init(&e->alloc, e->sb.data_off, e->sb.data_len);
    if (r != 0) { close(e->fd); free(e); return r; }

    e->idx = blob_index_create();
    if (!e->idx) { ra_destroy(&e->alloc); close(e->fd); free(e); return -ENOMEM; }
    e->next_blob_id = 1;

    // Replay WAL to rebuild allocator + blob index
    replay_ctx_t ctx = { .e = e };
    uint64_t last_lsn = 0, last_seq = 0;
    r = wal_replay(&e->wal, e->sb.wal_ckpt_lsn, wal_apply_engine, &ctx, &last_lsn, &last_seq);
    if (r != 0) {
        blob_index_destroy(e->idx);
        ra_destroy(&e->alloc);
        close(e->fd);
        free(e);
        return r;
    }

    *out = e;
    return 0;
}

void disk_engine_close(disk_engine_t *e)
{
    if (!e) return;

    if (e->idx) {
        blob_index_destroy(e->idx);
        e->idx = NULL;
    }
    ra_destroy(&e->alloc);
    // wal_close is no-op
    close(e->fd);
    free(e);
}

static int write_blob_record(disk_engine_t *e, const void *data, uint32_t len,
                             uint64_t *out_rec_off, uint64_t *out_alloc_len)
{
    // Allocate space for [hdr + payload]
    uint64_t need = blob_record_total_len(len);
    uint64_t total = align_up_u64(need, (uint64_t)e->sb.alloc_unit);
    uint64_t rec_off = 0;

    int r = ra_alloc(&e->alloc, total, &rec_off);
    if (r != 0) return r;

    // 1) payload first
    r = pwrite_full(e->fd, data, len, rec_off + (uint64_t)sizeof(blob_record_hdr_t));
    if (r != 0) {
        (void)ra_free(&e->alloc, rec_off, total);
        return r;
    }

    // 2) header last
    blob_record_hdr_t h;
    blob_record_build_header(&h, data, len);
    r = pwrite_full(e->fd, &h, sizeof(h), rec_off);
    if (r != 0) {
        (void)ra_free(&e->alloc, rec_off, total);
        return r;
    }

    // 3) make blob durable before logging metadata (redo-style)
    if (fdatasync(e->fd) != 0) {
        int err = -errno;
        (void)ra_free(&e->alloc, rec_off, total);
        return err;
    }

    *out_rec_off = rec_off;
    if (out_alloc_len) *out_alloc_len = total;
    return 0;
}

int disk_engine_put(disk_engine_t *e, const char *key, const void *data, size_t len)
{
    if (!e || !key || !data || len == 0) return -EINVAL;
    if (len > UINT32_MAX) return -EINVAL;

    uint64_t rec_off = 0;
    uint64_t alloc_len = 0;
    int r = write_blob_record(e, data, (uint32_t)len, &rec_off, &alloc_len);
    if (r != 0) return r;

    uint64_t blob_id = e->next_blob_id++;

    // WAL: one record is the commit point for both allocator replay + key->blob mapping
    wal_blob_put_payload_t bp;
    memset(&bp, 0, sizeof(bp));
    strncpy(bp.key, key, sizeof(bp.key) - 1);
    bp.key[sizeof(bp.key) - 1] = '\0';
    bp.blob_id = blob_id;
    bp.data_off = rec_off;
    bp.data_len = (uint64_t)len;
    bp.alloc_len = alloc_len;

    r = wal_append(&e->wal, WAL_REC_BLOB_PUT, &bp, (uint32_t)sizeof(bp), NULL, NULL);
    if (r != 0) {
        // The blob is durable (fdatasync) but unreferenced; roll back allocator in memory.
        (void)ra_free(&e->alloc, rec_off, alloc_len);
        return r;
    }

    r = wal_sync(&e->wal);
    if (r != 0) {
        (void)ra_free(&e->alloc, rec_off, alloc_len);
        return r;
    }

    // Apply to in-memory index
    blob_loc_t loc;
    loc.blob_id = blob_id;
    loc.off = rec_off;
    loc.len = (uint64_t)len;

    return blob_index_put(e->idx, key, &loc);
}

int disk_engine_checkpoint(disk_engine_t *e)
{
    if (!e) return -EINVAL;

    // Make sure all WAL writes are durable.
    int r = wal_sync(&e->wal);
    if (r != 0) return r;

    // Stop-the-world MVP: wipe WAL region, reset wal_ckpt_lsn to 0.
    // NOTE: This is safe because allocator+index state is already in memory and
    // will be rebuilt by future WAL entries after this checkpoint.
    r = wipe_zero(e->fd, e->sb.wal_off, e->sb.wal_len);
    if (r != 0) return r;
    if (fdatasync(e->fd) != 0) return -errno;

    superblock_t next = e->sb;
    next.epoch = e->sb.epoch + 1;
    next.wal_ckpt_lsn = 0;

    r = sb_write_next(e->fd, &e->sb, &next);
    if (r != 0) return r;

    e->sb = next;
    e->wal.write_off = e->sb.wal_off;
    e->wal.next_seq = 1;
    return 0;
}

int disk_engine_get(disk_engine_t *e, const char *key, void **out_buf, size_t *out_len)
{
    if (!e || !key || !out_buf || !out_len) return -EINVAL;

    blob_loc_t loc;
    int r = blob_index_get(e->idx, key, &loc);
    if (r != 0) return r;

    if (loc.len == 0 || loc.len > (1ull << 31)) return -EINVAL;

    // Read + verify header
    blob_record_hdr_t h;
    r = pread_full(e->fd, &h, sizeof(h), loc.off);
    if (r != 0) return r;

    if (h.magic != BLOB_REC_MAGIC || h.version != 1) return -EIO;
    if (h.payload_len != (uint32_t)loc.len) return -EIO;

    // Verify header CRC
    blob_record_hdr_t tmp = h;
    tmp.header_crc32 = 0;
    if (crc32_ieee(&tmp, sizeof(tmp)) != h.header_crc32) return -EIO;

    void *buf = malloc((size_t)loc.len);
    if (!buf) return -ENOMEM;

    r = pread_full(e->fd, buf, (size_t)loc.len, loc.off + (uint64_t)sizeof(blob_record_hdr_t));
    if (r != 0) { free(buf); return r; }

    if (crc32_ieee(buf, (size_t)loc.len) != h.payload_crc32) {
        free(buf);
        return -EIO;
    }

    *out_buf = buf;
    *out_len = (size_t)loc.len;
    return 0;
}

uint64_t disk_engine_last_blob_id(disk_engine_t *e)
{
    if (!e || e->next_blob_id == 0) return 0;
    return e->next_blob_id - 1;
}
