#include "blob_record.h"

#include "common/crc32_ieee.h"

#include <string.h>

void blob_record_build_header(blob_record_hdr_t* h, const void* payload, uint32_t payload_len) {
    if (!h) return;

    memset(h, 0, sizeof(*h));
    h->magic = BLOB_REC_MAGIC;
    h->version = 1;
    h->payload_len = payload_len;
    h->payload_crc32 = crc32_ieee(payload, (size_t)payload_len);

    /* header CRC is computed with header_crc32 set to 0 */
    h->header_crc32 = 0;
    h->header_crc32 = crc32_ieee(h, sizeof(*h));
}
