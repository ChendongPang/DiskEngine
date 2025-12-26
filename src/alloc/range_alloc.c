#include "range_alloc.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

static int range_valid(const range_alloc_t* a, uint64_t off, uint64_t len) {
    if (len == 0) return 0;
    // overflow-safe end check
    if (off < a->base_off) return 0;
    uint64_t end;
    if (__builtin_add_overflow(off, len, &end)) return 0;
    uint64_t region_end;
    if (__builtin_add_overflow(a->base_off, a->total_len, &region_end)) return 0;
    if (end > region_end) return 0;
    return 1;
}

int ra_init(range_alloc_t* a, uint64_t base_off, uint64_t total_len) {
    if (!a) return -EINVAL;
    memset(a, 0, sizeof(*a));
    if (total_len == 0) return -EINVAL;

    a->base_off   = base_off;
    a->total_len  = total_len;
    a->free_bytes = total_len;

    ra_node_t* n = (ra_node_t*)calloc(1, sizeof(*n));
    if (!n) return -ENOMEM;
    n->r.off = base_off;
    n->r.len = total_len;
    n->next  = NULL;
    a->free_head = n;
    return 0;
}

void ra_destroy(range_alloc_t* a) {
    if (!a) return;
    ra_node_t* cur = a->free_head;
    while (cur) {
        ra_node_t* nx = cur->next;
        free(cur);
        cur = nx;
    }
    memset(a, 0, sizeof(*a));
}

static int ranges_overlap(uint64_t a_off, uint64_t a_len, uint64_t b_off, uint64_t b_len) {
    uint64_t a_end = a_off + a_len;
    uint64_t b_end = b_off + b_len;
    return !(a_end <= b_off || b_end <= a_off);
}

int ra_alloc(range_alloc_t* a, uint64_t len, uint64_t* out_off) {
    if (!a || !out_off) return -EINVAL;
    if (len == 0) return -EINVAL;

    ra_node_t* prev = NULL;
    ra_node_t* cur  = a->free_head;

    while (cur) {
        if (cur->r.len >= len) {
        uint64_t off = cur->r.off;

        // consume from head of this free range
        cur->r.off += len;
        cur->r.len -= len;
        a->free_bytes -= len;

        // if fully consumed, remove node
        if (cur->r.len == 0) {
            if (prev) prev->next = cur->next;
            else a->free_head = cur->next;
            free(cur);
        }

        *out_off = off;
        return 0;
        }
        prev = cur;
        cur  = cur->next;
    }

    return -ENOSPC;
}

// Insert [off,len] keeping list sorted by off. Also validate no overlap.
int ra_free(range_alloc_t* a, uint64_t off, uint64_t len) {
    if (!a) return -EINVAL;
    if (!range_valid(a, off, len)) return -EINVAL;

    // find insertion point: first node with cur.off > off
    ra_node_t* prev = NULL;
    ra_node_t* cur  = a->free_head;

    while (cur && cur->r.off < off) {
        prev = cur;
        cur  = cur->next;
    }

    // overlap check with prev and cur (neighbors are enough because list is sorted & non-overlapping)
    if (prev && ranges_overlap(prev->r.off, prev->r.len, off, len)) return -EINVAL;
    if (cur  && ranges_overlap(off, len, cur->r.off, cur->r.len)) return -EINVAL;

    // create node
    ra_node_t* n = (ra_node_t*)calloc(1, sizeof(*n));
    if (!n) return -ENOMEM;
    n->r.off = off;
    n->r.len = len;
    n->next  = cur;

    if (prev) prev->next = n;
    else a->free_head = n;

    a->free_bytes += len;

    // coalesce with left if adjacent
    if (prev) {
        uint64_t prev_end = prev->r.off + prev->r.len;
        if (prev_end == n->r.off) {
        prev->r.len += n->r.len;
        prev->next = n->next;
        free(n);
        n = prev;
        }
    }

    // coalesce with right if adjacent (maybe repeatedly, but one step is enough because neighbors were non-overlapping)
    ra_node_t* right = n->next;
    if (right) {
        uint64_t n_end = n->r.off + n->r.len;
        if (n_end == right->r.off) {
        n->r.len += right->r.len;
        n->next = right->next;
        free(right);
        }
    }

    return 0;
}

// Remove a specific [off,len] from free list (must be fully contained in some free ranges).
// This is used for WAL replay: applying an allocation means "this space is no longer free".
int ra_mark_alloc(range_alloc_t* a, uint64_t off, uint64_t len) {
    if (!a) return -EINVAL;
    if (!range_valid(a, off, len)) return -EINVAL;

    ra_node_t* prev = NULL;
    ra_node_t* cur  = a->free_head;

    // find first range that might contain [off,len]
    while (cur && (cur->r.off + cur->r.len) <= off) {
        prev = cur;
        cur  = cur->next;
    }
    if (!cur) return -ENOENT;

    uint64_t cur_off = cur->r.off;
    uint64_t cur_end = cur->r.off + cur->r.len;
    uint64_t req_end = off + len;

    if (!(cur_off <= off && req_end <= cur_end)) {
        // not fully contained in a single free range => inconsistent replay or bug
        return -ENOENT;
    }

    // Case 1: exact match => remove node
    if (cur_off == off && cur_end == req_end) {
        if (prev) prev->next = cur->next;
        else a->free_head = cur->next;
        free(cur);
        a->free_bytes -= len;
        return 0;
    }

    // Case 2: cut from head
    if (cur_off == off) {
        cur->r.off += len;
        cur->r.len -= len;
        a->free_bytes -= len;
        return 0;
    }

    // Case 3: cut from tail
    if (cur_end == req_end) {
        cur->r.len -= len;
        a->free_bytes -= len;
        return 0;
    }

    // Case 4: cut from middle => split into two free ranges
    // left: [cur_off, off-cur_off]
    // right:[req_end, cur_end-req_end]
    uint64_t left_len  = off - cur_off;
    uint64_t right_off = req_end;
    uint64_t right_len = cur_end - req_end;

    ra_node_t* right = (ra_node_t*)calloc(1, sizeof(*right));
    if (!right) return -ENOMEM;

    right->r.off = right_off;
    right->r.len = right_len;
    right->next  = cur->next;

    cur->r.len = left_len;
    cur->next  = right;

    a->free_bytes -= len;
    return 0;
}

void ra_dump_free(const range_alloc_t* a) {
    if (!a) return;
    fprintf(stderr, "free_bytes=%llu\n", (unsigned long long)a->free_bytes);
    const ra_node_t* cur = a->free_head;
    while (cur) {
        fprintf(stderr, "  [off=%llu, len=%llu)\n",
                (unsigned long long)cur->r.off,
                (unsigned long long)cur->r.len);
        cur = cur->next;
    }
}
