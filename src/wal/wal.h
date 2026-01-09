#pragma once

#include <stdint.h>
#include <stddef.h>

#include "wal_format.h"
#include "../superblock/superblock.h"
#include "../io/io_backend.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct wal_t {
  /* backend: either POSIX fd or SPDK io_backend */
  int          fd;   /* legacy path (fd >= 0) */
  io_backend_t *io;  /* SPDK path (io != NULL) */

  /*
   * Record alignment for this WAL instance.
   * - POSIX path: WAL_REC_ALIGN (8)
   * - SPDK path : max(WAL_REC_ALIGN, io_block_size)
   */
  uint32_t rec_align;

  uint64_t wal_off;     // from superblock
  uint64_t wal_len;     // from superblock

  uint64_t write_off;   // absolute file offset (tail) for next append
  uint64_t next_seq;    // next record sequence

} wal_t;

typedef int (*wal_apply_fn)(uint16_t type,
                            const void* payload,
                            uint32_t payload_len,
                            uint64_t lsn,
                            uint64_t seq,
                            void* arg);

// Open WAL and locate append position by scanning from start_lsn.
// start_lsn: usually sb->wal_ckpt_lsn (MVP can pass 0 to scan from wal_off).
int wal_open(int fd, const superblock_t* sb, uint64_t start_lsn, wal_t* out);

// Open WAL on top of SPDK io_backend.
// NOTE: must be called on the SPDK owner thread of the io_backend.
int wal_open_io(io_backend_t *io, const superblock_t* sb, uint64_t start_lsn, wal_t* out);

// Append one record. Returns 0 and fills out_lsn/out_seq on success.
int wal_append(wal_t* wal, uint16_t type, const void* payload, uint32_t payload_len,
               uint64_t* out_lsn, uint64_t* out_seq);

// Ensure durability for appended records (v0: fdatasync).
int wal_sync(wal_t* wal);

// Close WAL (v0: no-op, kept for API symmetry).
static inline void wal_close(wal_t* wal) { (void)wal; }

// Convenience: append a WAL_REC_BLOB_PUT record.
static inline int wal_append_blob_put(wal_t* wal, const wal_blob_put_payload_t* p,
                                   uint64_t* out_lsn, uint64_t* out_seq) {
  return wal_append(wal, WAL_REC_BLOB_PUT, p, (uint32_t)sizeof(*p), out_lsn, out_seq);
}

// Legacy alias
static inline int wal_fsync(wal_t* wal) { return wal_sync(wal); }

// Replay records starting from start_lsn (inclusive) until first invalid/torn record.
// Returns 0; out_last_lsn set to last successfully applied record's lsn (or 0 if none).
int wal_replay(const wal_t* wal, uint64_t start_lsn, wal_apply_fn apply, void* arg,
               uint64_t* out_last_lsn, uint64_t* out_last_seq);

#ifdef __cplusplus
}
#endif
