#include "blob_index.h"

#include <stdlib.h>
#include <string.h>

typedef struct blob_index_node_t {
    blob_kv_entry_t ent;
    struct blob_index_node_t *next;
} blob_index_node_t;

struct blob_index_t {
    blob_index_node_t *head;
};

static void blob_key_copy(char dst[BLOB_KEY_MAX], const char *key)
{
    memset(dst, 0, BLOB_KEY_MAX);
    if (key == NULL) {
        return;
    }
    strncpy(dst, key, BLOB_KEY_MAX - 1);
    dst[BLOB_KEY_MAX - 1] = '\0';
}

static int blob_key_equal(const char a[BLOB_KEY_MAX], const char *b)
{
    if (b == NULL) {
        return 0;
    }
    return strncmp(a, b, BLOB_KEY_MAX) == 0;
}

blob_index_t *blob_index_create(void)
{
    blob_index_t *idx = (blob_index_t *)calloc(1, sizeof(blob_index_t));
    return idx;
}

void blob_index_destroy(blob_index_t *idx)
{
    if (!idx) {
        return;
    }
    blob_index_node_t *p = idx->head;
    while (p) {
        blob_index_node_t *n = p->next;
        free(p);
        p = n;
    }
    free(idx);
}

static int blob_index_put_impl(blob_index_t *idx, const char *key, const blob_loc_t *loc)
{
    if (!idx || !key || !loc) {
        return -1;
    }

    // Replace if exists
    for (blob_index_node_t *p = idx->head; p; p = p->next) {
        if (blob_key_equal(p->ent.key, key)) {
            p->ent.loc = *loc;
            return 0;
        }
    }

    // Prepend new node (O(1))
    blob_index_node_t *node = (blob_index_node_t *)calloc(1, sizeof(blob_index_node_t));
    if (!node) {
        return -2;
    }
    blob_key_copy(node->ent.key, key);
    node->ent.loc = *loc;
    node->next = idx->head;
    idx->head = node;
    return 0;
}

int blob_index_put(blob_index_t *idx, const char *key, const blob_loc_t *loc)
{
    return blob_index_put_impl(idx, key, loc);
}

int blob_index_replay_put(blob_index_t *idx, const char *key, const blob_loc_t *loc)
{
    return blob_index_put_impl(idx, key, loc);
}

int blob_index_get(blob_index_t *idx, const char *key, blob_loc_t *out)
{
    if (!idx || !key || !out) {
        return -1;
    }

    for (blob_index_node_t *p = idx->head; p; p = p->next) {
        if (blob_key_equal(p->ent.key, key)) {
            *out = p->ent.loc;
            return 0;
        }
    }
    return -2; // not found
}
