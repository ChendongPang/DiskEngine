#include "kv_store.h"

#include "../engine/disk_engine.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

struct kv_store_t {
    disk_engine_t *e;
};

// KV payload format (v1):
//   magic[4] = 'K''V''R''1'
//   ver[1]   = 1
//   op[1]    = 0x01(PUT) / 0x02(DEL tombstone)
//   val_len[4] little-endian (PUT: value length, DEL: 0)
//   value bytes...
#define KV_MAGIC_0 'K'
#define KV_MAGIC_1 'V'
#define KV_MAGIC_2 'R'
#define KV_MAGIC_3 '1'

#define KV_VER 1
#define KV_OP_PUT 0x01
#define KV_OP_DEL 0x02

#define KV_HDR_SIZE 10  // 4 + 1 + 1 + 4

static void u32_to_le(uint32_t v, uint8_t out[4])
{
    out[0] = (uint8_t)(v & 0xff);
    out[1] = (uint8_t)((v >> 8) & 0xff);
    out[2] = (uint8_t)((v >> 16) & 0xff);
    out[3] = (uint8_t)((v >> 24) & 0xff);
}

static uint32_t u32_from_le(const uint8_t in[4])
{
    return ((uint32_t)in[0]) |
           ((uint32_t)in[1] << 8) |
           ((uint32_t)in[2] << 16) |
           ((uint32_t)in[3] << 24);
}

static int kv_encode_put(const void *val, size_t len, void **out_buf, size_t *out_len)
{
    if (!out_buf || !out_len) return -EINVAL;
    if (len > 0xffffffffu) return -EINVAL;

    size_t total = KV_HDR_SIZE + len;
    uint8_t *buf = (uint8_t *)malloc(total);
    if (!buf) return -ENOMEM;

    buf[0] = KV_MAGIC_0;
    buf[1] = KV_MAGIC_1;
    buf[2] = KV_MAGIC_2;
    buf[3] = KV_MAGIC_3;
    buf[4] = KV_VER;
    buf[5] = KV_OP_PUT;
    u32_to_le((uint32_t)len, &buf[6]);

    if (len > 0) {
        memcpy(&buf[KV_HDR_SIZE], val, len);
    }

    *out_buf = buf;
    *out_len = total;
    return 0;
}

static int kv_encode_del(void **out_buf, size_t *out_len)
{
    if (!out_buf || !out_len) return -EINVAL;

    uint8_t *buf = (uint8_t *)malloc(KV_HDR_SIZE);
    if (!buf) return -ENOMEM;

    buf[0] = KV_MAGIC_0;
    buf[1] = KV_MAGIC_1;
    buf[2] = KV_MAGIC_2;
    buf[3] = KV_MAGIC_3;
    buf[4] = KV_VER;
    buf[5] = KV_OP_DEL;
    u32_to_le(0, &buf[6]);

    *out_buf = buf;
    *out_len = KV_HDR_SIZE;
    return 0;
}

int kv_open(kv_store_t **out, const char *path, uint64_t dev_size)
{
    if (!out || !path) return -EINVAL;

    kv_store_t *kv = (kv_store_t *)calloc(1, sizeof(*kv));
    if (!kv) return -ENOMEM;

    int r = disk_engine_open(&kv->e, path, dev_size);
    if (r != 0) {
        free(kv);
        return r;
    }

    *out = kv;
    return 0;
}

void kv_close(kv_store_t *kv)
{
    if (!kv) return;
    if (kv->e) {
        disk_engine_close(kv->e);
        kv->e = NULL;
    }
    free(kv);
}

int kv_put(kv_store_t *kv, const char *key, const void *val, size_t len)
{
    if (!kv || !kv->e || !key || (!val && len != 0)) return -EINVAL;

    void *payload = NULL;
    size_t payload_len = 0;
    int r = kv_encode_put(val, len, &payload, &payload_len);
    if (r != 0) return r;

    r = disk_engine_put(kv->e, key, payload, payload_len);
    free(payload);
    return r;
}

int kv_del(kv_store_t *kv, const char *key)
{
    if (!kv || !kv->e || !key) return -EINVAL;

    void *payload = NULL;
    size_t payload_len = 0;
    int r = kv_encode_del(&payload, &payload_len);
    if (r != 0) return r;

    r = disk_engine_put(kv->e, key, payload, payload_len);
    free(payload);
    return r;
}

int kv_get(kv_store_t *kv, const char *key, void **out_buf, size_t *out_len)
{
    if (!kv || !kv->e || !key || !out_buf || !out_len) return -EINVAL;

    void *payload = NULL;
    size_t payload_len = 0;

    int r = disk_engine_get(kv->e, key, &payload, &payload_len);
    if (r != 0) return r;

    if (payload_len < KV_HDR_SIZE) {
        free(payload);
        return -EIO;
    }

    const uint8_t *p = (const uint8_t *)payload;
    if (p[0] != KV_MAGIC_0 || p[1] != KV_MAGIC_1 || p[2] != KV_MAGIC_2 || p[3] != KV_MAGIC_3) {
        free(payload);
        return -EIO;
    }
    if (p[4] != KV_VER) {
        free(payload);
        return -EIO;
    }

    uint8_t op = p[5];
    uint32_t vlen = u32_from_le(&p[6]);

    if (op == KV_OP_DEL) {
        free(payload);
        return -ENOENT;
    }
    if (op != KV_OP_PUT) {
        free(payload);
        return -EIO;
    }

    if ((size_t)KV_HDR_SIZE + (size_t)vlen != payload_len) {
        free(payload);
        return -EIO;
    }

    void *out = NULL;
    if (vlen > 0) {
        out = malloc((size_t)vlen);
        if (!out) {
            free(payload);
            return -ENOMEM;
        }
        memcpy(out, &p[KV_HDR_SIZE], (size_t)vlen);
    } else {
        // empty value is allowed
        out = malloc(1);
        if (!out) {
            free(payload);
            return -ENOMEM;
        }
    }

    free(payload);
    *out_buf = out;
    *out_len = (size_t)vlen;
    return 0;
}

uint64_t kv_last_blob_id(kv_store_t *kv)
{
    if (!kv || !kv->e) return 0;
    return disk_engine_last_blob_id(kv->e);
}
