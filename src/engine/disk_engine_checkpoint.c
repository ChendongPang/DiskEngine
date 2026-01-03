#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "engine/disk_engine.h"
#include "superblock/superblock.h"
#include "wal/wal.h"

/*
 * Minimal, stop-the-world checkpoint
 *
 * Preconditions:
 *  - No concurrent put()
 *  - All previous puts have completed
 *
 * Effects:
 *  1. Persist WAL up to current lsn
 *  2. Update superblock wal_ckpt_lsn
 *  3. Truncate WAL prefix <= wal_ckpt_lsn
 */
int disk_engine_checkpoint(struct disk_engine *e)
{
    struct superblock new_sb;
    uint64_t ckpt_lsn;

    /*
     * Step 0: make sure WAL is durable
     * (data has already been fdatasync'ed in put path)
     */
    if (wal_sync(&e->wal) != 0)
        return -1;

    /*
     * Step 1: decide checkpoint LSN
     * We checkpoint everything we have replayed/applied.
     */
    ckpt_lsn = e->wal.last_lsn;

    /*
     * Step 2: write new superblock with updated wal_ckpt_lsn
     */
    new_sb = e->sb;
    new_sb.wal_ckpt_lsn = ckpt_lsn;

    if (sb_write_next(e->fd, &new_sb) != 0)
        return -1;

    /*
     * Step 3: update in-memory superblock
     */
    e->sb = new_sb;

    /*
     * Step 4: truncate WAL prefix
     * Safe because superblock already persisted.
     */
    if (wal_truncate_prefix(&e->wal, ckpt_lsn) != 0)
        return -1;

    return 0;
}
