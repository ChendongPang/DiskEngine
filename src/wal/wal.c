#include "wal.h"
#include "../common/crc32_ieee.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

/*
 * WAL can run on two backends:
 * - POSIX fd (legacy demos)
 * - SPDK io_backend (production path)
 *
 * SPDK backend constraints:
 * - io_read/io_write require off/len aligned to io_block_size()
 * - buffers must be DMA-safe and aligned to io_buf_align()
 */

static inline uint32_t align_up_u32(uint32_t x, uint32_t a) {
  return (x + (a - 1)) & ~(a - 1);
}

static inline uint64_t align_up_u64(uint64_t x, uint64_t a) {
  return (a == 0) ? x : ((x + (a - 1)) & ~(a - 1));
}

static inline uint64_t wal_begin(const wal_t* w) { return w->wal_off; }
static inline uint64_t wal_end  (const wal_t* w) { return w->wal_off + w->wal_len; }

static int wal_pread_full(const wal_t *w, void *buf, uint32_t n, uint64_t off)
{
    if (!w || !buf || n == 0) return -EINVAL;

    if (w->io) {
        /* io_read requires full-block aligned I/O; WAL records are aligned to w->rec_align */
        uint32_t blk = w->rec_align;
        if (blk == 0) return -EINVAL;
        if (off % blk || n % blk) return -EINVAL;

        void *tmp = io_dma_alloc(w->io, n);
        if (!tmp) return -ENOMEM;

        int r = io_read(w->io, off, tmp, n);
        if (r == 0) memcpy(buf, tmp, n);
        io_dma_free(w->io, tmp);
        return r;
    }

    /* POSIX path: exact read */
    ssize_t r = pread(w->fd, buf, (size_t)n, (off_t)off);
    if (r < 0) return -errno;
    if ((size_t)r != (size_t)n) return -EIO;
    return 0;
}

static int wal_pwrite_full(const wal_t *w, const void *buf, uint32_t n, uint64_t off)
{
    if (!w || !buf || n == 0) return -EINVAL;

    if (w->io) {
        uint32_t blk = w->rec_align;
        if (blk == 0) return -EINVAL;
        if (off % blk || n % blk) return -EINVAL;

        /* strict: caller must provide DMA-safe aligned buffer */
        return io_write(w->io, off, buf, n);
    }

    ssize_t r = pwrite(w->fd, buf, (size_t)n, (off_t)off);
    if (r < 0) return -errno;
    if ((size_t)r != (size_t)n) return -EIO;
    return 0;
}

// Validate header fields quickly (not including CRC).
static int wal_hdr_sanity(const wal_t* wal, const wal_rec_hdr_t* h) {
  if (h->magic != WAL_MAGIC) return -1;
  if (h->version != WAL_VERSION) return -1;
  if (h->header_sz != (uint32_t)sizeof(wal_rec_hdr_t)) return -1;

  /* record_total_len must be >= header+payload and aligned to wal->rec_align.
   * (POSIX: 8 bytes; SPDK: >= io_block_size).
   */
  uint32_t want = align_up_u32((uint32_t)(h->header_sz + h->payload_len), wal->rec_align ? wal->rec_align : WAL_REC_ALIGN);
  if (h->record_total_len != want) return -1;

  // bounds within WAL region
  uint64_t start = h->lsn;
  uint64_t end   = start + (uint64_t)h->record_total_len;
  if (start < wal_begin(wal) || end > wal_end(wal)) return -1;

  return 0;
}

// CRC over [hdr with crc32=0] + payload + pad(zeros in file)
static int wal_crc_check(const wal_t* wal, const wal_rec_hdr_t* hdr,
                         const uint8_t* payload_and_pad) {
    (void)wal;
    wal_rec_hdr_t tmp;
    memcpy(&tmp, hdr, sizeof(tmp));
    uint32_t old = tmp.crc32;
    tmp.crc32 = 0;

    uint32_t c = crc32_ieee_init();
    c = crc32_ieee_update(c, &tmp, tmp.header_sz);
    c = crc32_ieee_update(c, payload_and_pad, tmp.record_total_len - tmp.header_sz);
    c = crc32_ieee_final(c);

    return (c == old) ? 0 : -1;
}

static uint32_t wal_crc_build(const wal_rec_hdr_t* hdr_no_crc,
                              const uint8_t* payload_and_pad,
                              uint32_t payload_and_pad_len) {
    wal_rec_hdr_t tmp;
    memcpy(&tmp, hdr_no_crc, sizeof(tmp));
    tmp.crc32 = 0;

    uint32_t c = crc32_ieee_init();
    c = crc32_ieee_update(c, &tmp, tmp.header_sz);
    c = crc32_ieee_update(c, payload_and_pad, payload_and_pad_len);
    return crc32_ieee_final(c);
}

static int wal_open_common(wal_t *w, const superblock_t *sb, uint64_t start_lsn)
{
    if (!w || !sb) return -EINVAL;
    if (sb->wal_len == 0) return -EINVAL;

    w->wal_off = sb->wal_off;
    w->wal_len = sb->wal_len;

    /* Normalize start_lsn: 0 means from wal_off */
    if (start_lsn == 0) start_lsn = w->wal_off;
    if (start_lsn < w->wal_off || start_lsn >= w->wal_off + w->wal_len) return -EINVAL;
    if (w->rec_align == 0) w->rec_align = WAL_REC_ALIGN;
    if (start_lsn % w->rec_align) return -EINVAL;

    /* v0: scan forward until first invalid record; that position becomes append tail. */
    uint64_t cur = start_lsn;
    uint64_t last_seq = 0;

    while (cur + sizeof(wal_rec_hdr_t) <= wal_end(w)) {
      /* Read one full aligned record header.
       * - POSIX: we can read sizeof(hdr) directly
       * - SPDK : records are aligned to w->rec_align; read one block then memcpy header
       */
      wal_rec_hdr_t hdr;
      int r;
      if (w->io) {
          uint8_t *blk = (uint8_t *)malloc(w->rec_align);
          if (!blk) return -ENOMEM;
          r = wal_pread_full(w, blk, w->rec_align, cur);
          if (r != 0) { free(blk); break; }
          memcpy(&hdr, blk, sizeof(hdr));
          free(blk);
      } else {
          r = wal_pread_full(w, &hdr, (uint32_t)sizeof(hdr), cur);
          if (r != 0) break;
      }

      /* If header doesn't even look valid, treat as end-of-log (torn/unused) */
      if (wal_hdr_sanity(w, &hdr) != 0) break;
      if (cur + hdr.record_total_len > wal_end(w)) break;

      /* Read full record for CRC check */
      uint32_t rec_len = hdr.record_total_len;
      uint8_t *rec = (uint8_t *)malloc(rec_len);
      if (!rec) return -ENOMEM;

      if (w->io) {
          /* Need DMA buffer for io_read; wal_pread_full already uses DMA internally */
          r = wal_pread_full(w, rec, rec_len, cur);
      } else {
          /* POSIX: read header already; read tail only */
          memcpy(rec, &hdr, sizeof(hdr));
          uint32_t tail_len = rec_len - hdr.header_sz;
          r = wal_pread_full(w, rec + hdr.header_sz, tail_len, cur + hdr.header_sz);
      }
      if (r != 0) { free(rec); break; }

      const uint8_t *tail = rec + hdr.header_sz;
      if (wal_crc_check(w, &hdr, tail) != 0) {
        free(rec);
        break;
      }
      free(rec);

      if (hdr.lsn != cur) break;
      last_seq = hdr.seq;
      cur += hdr.record_total_len;
    }

    w->write_off = cur;
    w->next_seq  = (last_seq == 0) ? 1 : (last_seq + 1);
    return 0;
}

int wal_open(int fd, const superblock_t* sb, uint64_t start_lsn, wal_t* out) {
    if (!sb || !out) return -EINVAL;
    wal_t w;
    memset(&w, 0, sizeof(w));
    w.fd = fd;
    w.io = NULL;
    w.rec_align = WAL_REC_ALIGN;
    int r = wal_open_common(&w, sb, start_lsn);
    if (r != 0) return r;
    *out = w;
    return 0;
}

int wal_open_io(io_backend_t *io, const superblock_t* sb, uint64_t start_lsn, wal_t* out)
{
    if (!io || !sb || !out) return -EINVAL;
    wal_t w;
    memset(&w, 0, sizeof(w));
    w.fd = -1;
    w.io = io;

    uint32_t blk = io_block_size(io);
    if (blk == 0) return -EINVAL;
    /* records must be at least block-aligned to satisfy io_write/io_read */
    w.rec_align = (blk > WAL_REC_ALIGN) ? blk : WAL_REC_ALIGN;

    int r = wal_open_common(&w, sb, start_lsn);
    if (r != 0) return r;
    *out = w;
    return 0;
}

int wal_append(wal_t* wal, uint16_t type, const void* payload, uint32_t payload_len,
               uint64_t* out_lsn, uint64_t* out_seq) {
    if (!wal) return -EINVAL;
    if (payload_len > 0 && payload == NULL) return -EINVAL;

    uint32_t hdr_sz = (uint32_t)sizeof(wal_rec_hdr_t);
    uint32_t record_total_len = align_up_u32(hdr_sz + payload_len, wal->rec_align ? wal->rec_align : WAL_REC_ALIGN);
    uint32_t pad_len = record_total_len - (hdr_sz + payload_len);

    uint64_t off = wal->write_off;
    if (off < wal_begin(wal) || off > wal_end(wal)) return -EINVAL;
    if (off + record_total_len > wal_end(wal)) return -ENOSPC; // v0 no wrap

    // Build header
    wal_rec_hdr_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic      = WAL_MAGIC;
    hdr.version    = WAL_VERSION;
    hdr.type       = type;
    hdr.flags      = 0;
    hdr.header_sz  = hdr_sz;
    hdr.payload_len= payload_len;
    hdr.lsn        = off;            // absolute offset
    hdr.seq        = wal->next_seq;
    hdr.crc32      = 0;
    hdr.record_total_len    = record_total_len;

    /* Build full record buffer = [hdr][payload][pad zeros]
     * - POSIX : we can write hdr then tail
     * - SPDK  : must write as one aligned chunk from an aligned DMA buffer
     */
    int r;

    if (wal->io) {
        void *rec = io_dma_alloc(wal->io, record_total_len);
        if (!rec) return -ENOMEM;
        memset(rec, 0, record_total_len);
        memcpy(rec, &hdr, hdr_sz);
        if (payload_len) memcpy((uint8_t*)rec + hdr_sz, payload, payload_len);

        /* Compute CRC over tail bytes ([payload][pad]) */
        hdr.crc32 = wal_crc_build(&hdr, (const uint8_t*)rec + hdr_sz, record_total_len - hdr_sz);
        memcpy(rec, &hdr, hdr_sz);

        r = wal_pwrite_full(wal, rec, record_total_len, off);
        io_dma_free(wal->io, rec);
        if (r != 0) return r;
    } else {
        /* POSIX path: keep the old two-write behavior */
        uint8_t* tail = NULL;
        uint32_t tail_len = record_total_len - hdr_sz;
        if (tail_len > 0) {
          tail = (uint8_t*)malloc(tail_len);
          if (!tail) return -ENOMEM;
          if (payload_len) memcpy(tail, payload, payload_len);
          if (pad_len) memset(tail + payload_len, 0, pad_len);
        }

        hdr.crc32 = wal_crc_build(&hdr, tail, tail_len);

        r = wal_pwrite_full(wal, &hdr, hdr_sz, off);
        if (r != 0) { free(tail); return r; }
        if (tail_len > 0) {
          r = wal_pwrite_full(wal, tail, tail_len, off + hdr_sz);
          if (r != 0) { free(tail); return r; }
        }
        free(tail);
    }

    // Advance tail
    wal->write_off += record_total_len;
    wal->next_seq++;

    if (out_lsn) *out_lsn = hdr.lsn;
    if (out_seq) *out_seq = hdr.seq;
    return 0;
}

int wal_sync(wal_t* wal) {
  if (!wal) return -EINVAL;
  if (wal->io) {
    return io_flush(wal->io);
  }
  if (fdatasync(wal->fd) != 0) return -errno;
  return 0;
}

int wal_replay(const wal_t* wal, uint64_t start_lsn, wal_apply_fn apply, void* arg,
               uint64_t* out_last_lsn, uint64_t* out_last_seq) {
    if (!wal || !apply) return -EINVAL;

    uint64_t cur = (start_lsn == 0) ? wal_begin(wal) : start_lsn;
    if (cur < wal_begin(wal) || cur >= wal_end(wal)) return -EINVAL;

    uint64_t last_lsn = 0;
    uint64_t last_seq = 0;

    while (cur + sizeof(wal_rec_hdr_t) <= wal_end(wal)) {
      wal_rec_hdr_t hdr;
      int r;

      if (wal->io) {
        /* read one aligned block to get header */
        uint32_t blk = wal->rec_align;
        uint8_t *tmp = (uint8_t *)malloc(blk);
        if (!tmp) return -ENOMEM;
        r = wal_pread_full(wal, tmp, blk, cur);
        if (r != 0) { free(tmp); break; }
        memcpy(&hdr, tmp, sizeof(hdr));
        free(tmp);
      } else {
        r = wal_pread_full(wal, &hdr, (uint32_t)sizeof(hdr), cur);
        if (r != 0) break;
      }

      if (wal_hdr_sanity(wal, &hdr) != 0) break;
      if (cur + hdr.record_total_len > wal_end(wal)) break;

      uint32_t rec_len = hdr.record_total_len;
      uint8_t* rec = (uint8_t*)malloc(rec_len);
      if (!rec) return -ENOMEM;

      if (wal->io) {
        r = wal_pread_full(wal, rec, rec_len, cur);
        if (r != 0) { free(rec); break; }
      } else {
        memcpy(rec, &hdr, sizeof(hdr));
        uint32_t tail_len = rec_len - hdr.header_sz;
        r = wal_pread_full(wal, rec + hdr.header_sz, tail_len, cur + hdr.header_sz);
        if (r != 0) { free(rec); break; }
      }

      const uint8_t* tail = rec + hdr.header_sz;
      if (wal_crc_check(wal, &hdr, tail) != 0) {
        free(rec);
        break;
      }

      const void* payload = tail;
      uint32_t payload_len = hdr.payload_len;

      r = apply(hdr.type, payload, payload_len, hdr.lsn, hdr.seq, arg);
      free(rec);
      if (r != 0) return r;

      last_lsn = hdr.lsn;
      last_seq = hdr.seq;
      cur += hdr.record_total_len;
    }

    if (out_last_lsn) *out_last_lsn = last_lsn;
    if (out_last_seq) *out_last_seq = last_seq;
    return 0;
}
