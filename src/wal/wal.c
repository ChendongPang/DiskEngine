#include "wal.h"
#include "../common/crc32_ieee.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

static inline uint32_t align_up_u32(uint32_t x, uint32_t a) {
  return (x + (a - 1)) & ~(a - 1);
}

static inline uint64_t wal_begin(const wal_t* w) { return w->wal_off; }
static inline uint64_t wal_end  (const wal_t* w) { return w->wal_off + w->wal_len; }

static int pread_full(int fd, void* buf, size_t n, uint64_t off) {
    ssize_t r = pread(fd, buf, n, (off_t)off);
    if (r < 0) return -errno;
    if ((size_t)r != n) return -EIO; // short read is fatal for fixed-size pieces
    return 0;
}

static int pwrite_full(int fd, const void* buf, size_t n, uint64_t off) {
    ssize_t r = pwrite(fd, buf, n, (off_t)off);
    if (r < 0) return -errno;
    if ((size_t)r != n) return -EIO; // short write treated as error
    return 0;
}

// Validate header fields quickly (not including CRC).
static int wal_hdr_sanity(const wal_t* wal, const wal_rec_hdr_t* h) {
  if (h->magic != WAL_MAGIC) return -1;
  if (h->version != WAL_VERSION) return -1;
  if (h->header_sz != (uint32_t)sizeof(wal_rec_hdr_t)) return -1;

  if (h->rec_len != align_up_u32((uint32_t)(h->header_sz + h->payload_len), WAL_REC_ALIGN)) return -1;

  // bounds within WAL region
  uint64_t start = h->lsn;
  uint64_t end   = start + (uint64_t)h->rec_len;
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
    c = crc32_ieee_update(c, payload_and_pad, tmp.rec_len - tmp.header_sz);
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

int wal_open(int fd, const superblock_t* sb, uint64_t start_lsn, wal_t* out) {
    if (!sb || !out) return -EINVAL;
    if (sb->wal_len == 0) return -EINVAL;

    wal_t w;
    memset(&w, 0, sizeof(w));
    w.fd      = fd;
    w.wal_off = sb->wal_off;
    w.wal_len = sb->wal_len;

    // Normalize start_lsn: 0 means from wal_off
    if (start_lsn == 0) start_lsn = w.wal_off;
    if (start_lsn < w.wal_off || start_lsn >= w.wal_off + w.wal_len) return -EINVAL;

    // v0: scan forward until first invalid record; that position becomes append tail.
    uint64_t cur = start_lsn;
    uint64_t last_seq = 0;

    while (cur + sizeof(wal_rec_hdr_t) <= wal_end(&w)) {
      wal_rec_hdr_t hdr;
      int r = pread_full(fd, &hdr, sizeof(hdr), cur);
      if (r != 0) break;

      // If header doesn't even look valid, treat as end-of-log (torn/unused)
      if (wal_hdr_sanity(&w, &hdr) != 0) break;

      // Read payload+pad for CRC check
      uint32_t tail_len = hdr.rec_len - hdr.header_sz;
      if (cur + hdr.rec_len > wal_end(&w)) break;

      uint8_t* tail = (uint8_t*)malloc(tail_len);
      if (!tail) return -ENOMEM;

      r = pread_full(fd, tail, tail_len, cur + hdr.header_sz);
      if (r != 0) { free(tail); break; }

      if (wal_crc_check(&w, &hdr, tail) != 0) {
        free(tail);
        break; // first bad record => stop
      }
      free(tail);

      // Monotonic sanity (optional but helpful)
      if (hdr.lsn != cur) break;
      last_seq = hdr.seq;

      cur += hdr.rec_len;
    }

    w.write_off = cur;
    w.next_seq  = (last_seq == 0) ? 1 : (last_seq + 1);

    *out = w;
    return 0;
}

int wal_append(wal_t* wal, uint16_t type, const void* payload, uint32_t payload_len,
               uint64_t* out_lsn, uint64_t* out_seq) {
    if (!wal) return -EINVAL;
    if (payload_len > 0 && payload == NULL) return -EINVAL;

    uint32_t hdr_sz = (uint32_t)sizeof(wal_rec_hdr_t);
    uint32_t rec_len = align_up_u32(hdr_sz + payload_len, WAL_REC_ALIGN);
    uint32_t pad_len = rec_len - (hdr_sz + payload_len);

    uint64_t off = wal->write_off;
    if (off < wal_begin(wal) || off > wal_end(wal)) return -EINVAL;
    if (off + rec_len > wal_end(wal)) return -ENOSPC; // v0 no wrap

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
    hdr.rec_len    = rec_len;

    // Build tail buffer = payload + pad(zeros)
    uint8_t* tail = NULL;
    uint32_t tail_len = rec_len - hdr_sz;
    if (tail_len > 0) {
      tail = (uint8_t*)malloc(tail_len);
      if (!tail) return -ENOMEM;
      if (payload_len) memcpy(tail, payload, payload_len);
      if (pad_len) memset(tail + payload_len, 0, pad_len);
    }

    // Compute CRC
    hdr.crc32 = wal_crc_build(&hdr, tail, tail_len);

    // Write: header then tail then (optional) durability by wal_sync()
    int r = pwrite_full(wal->fd, &hdr, hdr_sz, off);
    if (r != 0) { free(tail); return r; }

    if (tail_len > 0) {
      r = pwrite_full(wal->fd, tail, tail_len, off + hdr_sz);
      if (r != 0) { free(tail); return r; }
    }

    free(tail);

    // Advance tail
    wal->write_off += rec_len;
    wal->next_seq++;

    if (out_lsn) *out_lsn = hdr.lsn;
    if (out_seq) *out_seq = hdr.seq;
    return 0;
}

int wal_sync(wal_t* wal) {
  if (!wal) return -EINVAL;
  // v0: simple and safe
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
      int r = pread_full(wal->fd, &hdr, sizeof(hdr), cur);
      if (r != 0) break;

      if (wal_hdr_sanity(wal, &hdr) != 0) break;

      uint32_t tail_len = hdr.rec_len - hdr.header_sz;
      if (cur + hdr.rec_len > wal_end(wal)) break;

      uint8_t* tail = (uint8_t*)malloc(tail_len);
      if (!tail) return -ENOMEM;

      r = pread_full(wal->fd, tail, tail_len, cur + hdr.header_sz);
      if (r != 0) { free(tail); break; }

      if (wal_crc_check(wal, &hdr, tail) != 0) {
        free(tail);
        break; // torn/partial record => stop replay
      }

      // Apply only the payload portion (exclude pad)
      const void* payload = tail;
      uint32_t payload_len = hdr.payload_len;

      r = apply(hdr.type, payload, payload_len, hdr.lsn, hdr.seq, arg);
      free(tail);
      if (r != 0) return r; // apply decides error policy

      last_lsn = hdr.lsn;
      last_seq = hdr.seq;

      cur += hdr.rec_len;
    }

    if (out_last_lsn) *out_last_lsn = last_lsn;
    if (out_last_seq) *out_last_seq = last_seq;
    return 0;
}
