#pragma once
/*
 * Disk format checksum: CRC-32 (IEEE 802.3), reflected
 * poly=0xEDB88320, init=0xFFFFFFFF, xorout=0xFFFFFFFF
 *
 * This is CRC-32/IEEE (aka "CRC-32", PKZip), NOT CRC32C.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Incremental API (recommended for WAL / streaming)
static inline uint32_t crc32_ieee_init(void) { return 0xFFFFFFFFu; }
uint32_t crc32_ieee_update(uint32_t crc, const void* data, size_t n);
static inline uint32_t crc32_ieee_final(uint32_t crc) { return crc ^ 0xFFFFFFFFu; }

// One-shot helper
static inline uint32_t crc32_ieee(const void* data, size_t n) {
  uint32_t c = crc32_ieee_init();
  c = crc32_ieee_update(c, data, n);
  return crc32_ieee_final(c);
}

#ifdef __cplusplus
}
#endif
