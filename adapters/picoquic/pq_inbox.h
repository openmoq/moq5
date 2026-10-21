#ifndef MOQ_PQ_INBOX_H
#define MOQ_PQ_INBOX_H

/*
 * A lane's inbox: a bounded ring of whole datagrams other lanes hand it,
 * with their addresses. Producers are the other lanes' threads, the consumer
 * is the owner's. Admission is serialized by one mutex, so a close is a
 * point in that order: every push either completed before it or is refused
 * after it, and the owner needs no second settling step. Storage is fixed at
 * init; a push copies into a slot and never allocates, a datagram that
 * does not fit a slot is refused whole. PRIVATE to adapters/picoquic.
 */

#include <moq/types.h>

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

#if defined(__GNUC__) || defined(__clang__)
/* The lane group's internals are not part of the adapter's ABI. */
#  pragma GCC visibility push(hidden)
#endif

/* The largest datagram a slot holds: picoquic's maximum packet size. */
#define PQ_INBOX_PAYLOAD 1536u

/* A saturating add of two drop/discard counters. The lane group folds a detect
 * ring's full and closed drops into the public detection_drops stat; the sum
 * reports UINT64_MAX rather than wrapping. */
static inline uint64_t pq_u64_sat_add(uint64_t a, uint64_t b)
{
    uint64_t s = a + b;
    return s < a ? UINT64_MAX : s;
}

typedef struct pq_inbox_slot {
    struct sockaddr_storage from;
    struct sockaddr_storage to;
    int           if_index;
    unsigned char ecn;
    uint16_t      len;
    uint8_t       bytes[PQ_INBOX_PAYLOAD];
} pq_inbox_slot_t;

typedef struct pq_inbox {
    pthread_mutex_t  mu;
    pq_inbox_slot_t *slots;
    uint32_t         cap;
    uint32_t         head;        /* oldest */
    uint32_t         count;
    bool             closed;      /* admission closed: pushes are refused */
    _Atomic uint64_t full_drops;
    _Atomic uint64_t closed_drops;
    _Atomic uint64_t discarded;   /* left at the owner's exit */
} pq_inbox_t;

/* Allocate `cap` slots; false on overflow or OOM. */
bool pq_inbox_init(pq_inbox_t *b, const moq_alloc_t *alloc, uint32_t cap);
void pq_inbox_free(pq_inbox_t *b, const moq_alloc_t *alloc);

/* The bytes `cap` slots take, with overflow reported as 0. */
size_t pq_inbox_bytes(uint32_t cap);

/* Producer: copy one datagram in. False, and counted, when the inbox is
 * closed or full, or when `len` exceeds a slot. */
bool pq_inbox_push(pq_inbox_t *b, const uint8_t *bytes, size_t len,
                   const struct sockaddr_storage *from,
                   const struct sockaddr_storage *to,
                   int if_index, unsigned char ecn);

/* Consumer: copy the oldest datagram out. False when empty. */
bool pq_inbox_pop(pq_inbox_t *b, pq_inbox_slot_t *out);

bool pq_inbox_empty(pq_inbox_t *b);

/* Close admission: no push succeeds after this returns. */
void pq_inbox_close(pq_inbox_t *b);

/* Consumer at exit: count what is left as discarded and empty the ring. */
void pq_inbox_discard(pq_inbox_t *b);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC visibility pop
#endif
#endif /* MOQ_PQ_INBOX_H */
