#include "pq_inbox.h"

#include <string.h>

/* A saturating add for the drop/discard counters: several producer lanes may
 * bump one at once, and the public stats contract says a counter clamps at
 * UINT64_MAX rather than wrapping. */
static void inbox_count(_Atomic uint64_t *c, uint64_t n)
{
    uint64_t cur = atomic_load_explicit(c, memory_order_relaxed);
    while (cur != UINT64_MAX) {
        uint64_t next = cur + n < cur ? UINT64_MAX : cur + n;
        if (atomic_compare_exchange_weak_explicit(
                c, &cur, next, memory_order_relaxed, memory_order_relaxed))
            return;
    }
}

size_t pq_inbox_bytes(uint32_t cap)
{
    if (cap == 0) return 0;
    size_t bytes = (size_t)cap * sizeof(pq_inbox_slot_t);
    if (bytes / sizeof(pq_inbox_slot_t) != cap) return 0;   /* overflowed */
    return bytes;
}

bool pq_inbox_init(pq_inbox_t *b, const moq_alloc_t *alloc, uint32_t cap)
{
    memset(b, 0, sizeof(*b));
    size_t bytes = pq_inbox_bytes(cap);
    if (bytes == 0) return false;
    b->slots = alloc->alloc(bytes, alloc->ctx);
    if (!b->slots) return false;
    if (pthread_mutex_init(&b->mu, NULL) != 0) {
        alloc->free(b->slots, bytes, alloc->ctx);
        b->slots = NULL;
        return false;
    }
    b->cap = cap;
    return true;
}

void pq_inbox_free(pq_inbox_t *b, const moq_alloc_t *alloc)
{
    if (!b->slots) return;
    pthread_mutex_destroy(&b->mu);
    alloc->free(b->slots, pq_inbox_bytes(b->cap), alloc->ctx);
    b->slots = NULL;
    b->cap = b->head = b->count = 0;
}

bool pq_inbox_push(pq_inbox_t *b, const uint8_t *bytes, size_t len,
                   const struct sockaddr_storage *from,
                   const struct sockaddr_storage *to,
                   int if_index, unsigned char ecn)
{
    if (len == 0 || len > PQ_INBOX_PAYLOAD) {
        inbox_count(&b->full_drops, 1);
        return false;
    }
    pthread_mutex_lock(&b->mu);
    if (b->closed) {
        pthread_mutex_unlock(&b->mu);
        inbox_count(&b->closed_drops, 1);
        return false;
    }
    if (b->count == b->cap) {
        pthread_mutex_unlock(&b->mu);
        inbox_count(&b->full_drops, 1);
        return false;
    }
    pq_inbox_slot_t *s = &b->slots[(b->head + b->count) % b->cap];
    s->from = *from;
    s->to = *to;
    s->if_index = if_index;
    s->ecn = ecn;
    s->len = (uint16_t)len;
    memcpy(s->bytes, bytes, len);
    b->count++;
    pthread_mutex_unlock(&b->mu);
    return true;
}

bool pq_inbox_pop(pq_inbox_t *b, pq_inbox_slot_t *out)
{
    pthread_mutex_lock(&b->mu);
    if (b->count == 0) {
        pthread_mutex_unlock(&b->mu);
        return false;
    }
    pq_inbox_slot_t *s = &b->slots[b->head];
    out->from = s->from;
    out->to = s->to;
    out->if_index = s->if_index;
    out->ecn = s->ecn;
    out->len = s->len;
    memcpy(out->bytes, s->bytes, s->len);
    b->head = (b->head + 1) % b->cap;
    b->count--;
    pthread_mutex_unlock(&b->mu);
    return true;
}

bool pq_inbox_empty(pq_inbox_t *b)
{
    pthread_mutex_lock(&b->mu);
    bool e = b->count == 0;
    pthread_mutex_unlock(&b->mu);
    return e;
}

void pq_inbox_close(pq_inbox_t *b)
{
    if (!b->slots) return;   /* never initialised: no mutex to take */
    pthread_mutex_lock(&b->mu);
    b->closed = true;
    pthread_mutex_unlock(&b->mu);
}

void pq_inbox_discard(pq_inbox_t *b)
{
    if (!b->slots) return;   /* never initialised: no mutex to take */
    pthread_mutex_lock(&b->mu);
    inbox_count(&b->discarded, b->count);
    b->head = 0;
    b->count = 0;
    pthread_mutex_unlock(&b->mu);
}
