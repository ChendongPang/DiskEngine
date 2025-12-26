#include "superblock.h"
#include "common/crc32_ieee.h"
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/file.h>

#ifndef SB_SLOT0_OFF
#define SB_SLOT0_OFF ((off_t)0)
#endif

#ifndef SB_SLOT1_OFF
#define SB_SLOT1_OFF ((off_t)SB_SIZE)
#endif

static inline off_t sb_slot_off(int slot) {
  return (slot == 0) ? SB_SLOT0_OFF : SB_SLOT1_OFF;
}

static uint32_t sb_crc_compute(const superblock_t* sb) {
    superblock_t tmp;
    memcpy(&tmp, sb, SB_SIZE);
    tmp.crc32 = 0;
    return  crc32_ieee(&tmp, SB_SIZE);
}

static int sb_crc_check(const superblock_t* sb) {
    uint32_t r = sb_crc_compute(sb);
    return (r == sb->crc32) ? 0 : -1;
}

static void sb_crc_set(superblock_t* sb) {
    sb->crc32 = 0;
    sb->crc32 = sb_crc_compute(sb);
}

// --- Basic sanity checks (in addition to CRC) ---
static int sb_basic_check(const superblock_t* sb) {
  // 1) magic
  if (sb->magic != SB_MAGIC) return -1;

  // 2) version
  if (sb->version != SB_VERSION) return -1;

  // 3) header_sz sanity:
  if (sb->hdr_size < (uint32_t)offsetof(superblock_t, crc32) + (uint32_t)sizeof(sb->crc32))
    return -1;
  if (sb->hdr_size > (uint32_t)SB_SIZE) return -1;

  if (sb->alloc_unit == 0) return -1;

  return 0;
}

static int sb_is_valid(const superblock_t* sb) {
  if (sb_basic_check(sb) != 0) return -1;
  if (sb_crc_check(sb) != 0) return -1;
  return 0;
}

static int sb_pread(int fd, off_t off, superblock_t* out) {
    ssize_t n = pread(fd, out, SB_SIZE, off);
    if (n < 0) return -errno;
    if ((size_t)n != (size_t)SB_SIZE) return -EIO;
    return 0;
}

static int sb_pwrite(int fd, off_t off, const superblock_t* in) {
    ssize_t n = pwrite(fd, in, SB_SIZE, off);
    if (n < 0) return -errno;
    if ((size_t)n != (size_t)SB_SIZE) return -EIO;
    return 0;
}

/*
Returns:
  true  -> out filled with the "best" valid superblock
  false -> neither slot is valid (unformatted / corrupted)
*/
bool sb_read_best(int fd, superblock_t* out) {
  superblock_t a, b;
  int ra = sb_pread(fd, SB_SLOT0_OFF, &a);
  int rb = sb_pread(fd, SB_SLOT1_OFF, &b);

  // If read fails, treat as invalid slot (but if both fail, return false).
  int va = (ra == 0) ? sb_is_valid(&a) : 0;
  int vb = (rb == 0) ? sb_is_valid(&b) : 0;

  if (va != 0 && vb != 0)
    return false;

  if (va == 0 && vb == 0) {
    // Choose the newer epoch.
    if (b.epoch > a.epoch) {
      memcpy(out, &b, SB_SIZE);
    } else {
      memcpy(out, &a, SB_SIZE);
    }
    return true;
  }

  if (va == 0) {
    memcpy(out, &a, SB_SIZE);
  } else {
    memcpy(out, &b, SB_SIZE);
  }
  return true;
}

/*
fencing check
Write "next" superblock as a safe next generation commit.
Contract:
- Caller provides cur = last known committed SB (e.g., from sb_read_best).
- next must keep invariants (uuid/layout usually same), and epoch should advance.
- Function will protect against stale/ABA by comparing disk-best epoch with cur->epoch.

Returns:
  0 on success
 <0 on error (-errno or -ESTALE etc.)
*/
int sb_write_next(int fd, const superblock_t* cur, const superblock_t* next_in) {
  if (!cur || !next_in) return -EINVAL;

  if (flock(fd, LOCK_EX) != 0) return -errno;

  int ret = 0;

  // Re-read best on disk to prevent stale/ABA.
  superblock_t disk;
  if (!sb_read_best(fd, &disk)) {
    // If disk has no valid SB, treat as unformatted / corrupted.
    ret = -EIO;
    goto out_unlock;
  }

  if (disk.epoch != cur->epoch) {
    // Someone else advanced it, or caller has stale cur.
    ret = -ESTALE;
    goto out_unlock;
  }

  // Basic monotonic requirement
  if (next_in->epoch <= cur->epoch) {
    ret = -EINVAL;
    goto out_unlock;
  }

  if (next_in->magic != SB_MAGIC || next_in->version != SB_VERSION) {
    ret = -EINVAL;
    goto out_unlock;
  }
  if (next_in->uuid_hi != cur->uuid_hi || next_in->uuid_lo != cur->uuid_lo) {
    ret = -EINVAL;
    goto out_unlock;
  }

  // Decide next slot: alternate by epoch.
  // Simple rule: write to (next_epoch % 2).
  int next_slot = (int)(next_in->epoch & 1ULL);
  off_t off = sb_slot_off(next_slot);

  // Prepare a writable copy to set CRC
  superblock_t to_write;
  memcpy(&to_write, next_in, SB_SIZE);

  // Important: ensure padding is deterministic. If caller already memset to 0, great.
  // If not, you *must* avoid random pad making CRC mismatch across runs.
  // Here we assume next_in buffer is fully initialized SB_SIZE bytes.

  sb_crc_set(&to_write);

  // Write the next slot then fsync to make it durable.
  ret = sb_pwrite(fd, off, &to_write);
  if (ret != 0) goto out_unlock;

  if (fsync(fd) != 0) {
    ret = -errno;
    goto out_unlock;
  }

  superblock_t verify;
  if (sb_pread(fd, off, &verify) != 0 || sb_is_valid(&verify) != 0 || verify.epoch != to_write.epoch) {
    ret = -EIO;
    goto out_unlock;
  }

out_unlock:
  (void)flock(fd, LOCK_UN);
  return ret;
}
