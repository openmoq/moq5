/*
 * pq_inbox: a lane's bounded inbox under its producers.
 *
 *   - FIFO order and every field of a slot intact;
 *   - a full inbox refuses whole, counted, never truncating;
 *   - a datagram larger than a slot is refused whole;
 *   - close is a point in admission order: a push either completed before
 *     it, and is popped, or is refused after it, and is counted -- with
 *     producers running the whole time;
 *   - discard counts what the owner leaves.
 */

#include "../pq_inbox.h"

#include <moq/types.h>

#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int failures = 0;
#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            failures++;                                                 \
        }                                                               \
    } while (0)

static struct sockaddr_storage addr(uint16_t port)
{
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    struct sockaddr_in *a = (struct sockaddr_in *)&ss;
    a->sin_family = AF_INET;
    a->sin_port = htons(port);
    a->sin_addr.s_addr = htonl(0x0A000001u);
    return ss;
}

typedef struct {
    pq_inbox_t *b;
    _Atomic int stop;
    _Atomic int pushed;   /* set after its first push returns */
    uint64_t attempted;
    uint64_t accepted;
} producer_t;

static void *producer_main(void *arg)
{
    producer_t *p = arg;
    uint8_t dg[64];
    struct sockaddr_storage from = addr(1), to = addr(2);
    while (!atomic_load(&p->stop)) {
        memset(dg, (int)(p->attempted & 0xff), sizeof(dg));
        p->attempted++;
        if (pq_inbox_push(p->b, dg, sizeof(dg), &from, &to, 0, 0))
            p->accepted++;
        atomic_store(&p->pushed, 1);
    }
    return NULL;
}

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    pq_inbox_t b;
    pq_inbox_slot_t out;
    uint8_t dg[PQ_INBOX_PAYLOAD + 1];

    /* capacity and storage */
    CHECK(pq_inbox_bytes(0) == 0);
    CHECK(pq_inbox_bytes(UINT32_MAX) == 0 || pq_inbox_bytes(UINT32_MAX) / sizeof(pq_inbox_slot_t) == UINT32_MAX);
    CHECK(pq_inbox_bytes(128) == 128u * sizeof(pq_inbox_slot_t));
    CHECK(pq_inbox_init(&b, alloc, 4));

    /* order and contents */
    struct sockaddr_storage from = addr(4000), to = addr(443);
    for (int i = 0; i < 4; i++) {
        memset(dg, i, sizeof(dg));
        CHECK(pq_inbox_push(&b, dg, 100 + (size_t)i, &from, &to, 7, (unsigned char)i));
    }
    memset(dg, 9, sizeof(dg));
    CHECK(!pq_inbox_push(&b, dg, 100, &from, &to, 7, 0));   /* full */
    CHECK(atomic_load(&b.full_drops) == 1);
    CHECK(!pq_inbox_empty(&b));
    for (int i = 0; i < 4; i++) {
        CHECK(pq_inbox_pop(&b, &out));
        CHECK(out.len == 100 + i);
        CHECK(out.bytes[0] == i && out.bytes[out.len - 1] == i);
        CHECK(out.if_index == 7 && out.ecn == i);
        CHECK(((struct sockaddr_in *)&out.from)->sin_port == htons(4000));
        CHECK(((struct sockaddr_in *)&out.to)->sin_port == htons(443));
    }
    CHECK(!pq_inbox_pop(&b, &out));
    CHECK(pq_inbox_empty(&b));

    /* oversize and empty datagrams are refused whole */
    CHECK(!pq_inbox_push(&b, dg, PQ_INBOX_PAYLOAD + 1, &from, &to, 0, 0));
    CHECK(!pq_inbox_push(&b, dg, 0, &from, &to, 0, 0));
    CHECK(pq_inbox_push(&b, dg, PQ_INBOX_PAYLOAD, &from, &to, 0, 0));
    CHECK(pq_inbox_pop(&b, &out) && out.len == PQ_INBOX_PAYLOAD);
    CHECK(atomic_load(&b.full_drops) == 3);

    /* discard counts what is left */
    CHECK(pq_inbox_push(&b, dg, 10, &from, &to, 0, 0));
    CHECK(pq_inbox_push(&b, dg, 10, &from, &to, 0, 0));
    pq_inbox_discard(&b);
    CHECK(atomic_load(&b.discarded) == 2);
    CHECK(pq_inbox_empty(&b));

    /* counters saturate at UINT64_MAX, never wrap (the stats contract) */
    atomic_store(&b.full_drops, UINT64_MAX - 1);
    CHECK(!pq_inbox_push(&b, dg, PQ_INBOX_PAYLOAD + 1, &from, &to, 0, 0));
    CHECK(atomic_load(&b.full_drops) == UINT64_MAX);
    CHECK(!pq_inbox_push(&b, dg, PQ_INBOX_PAYLOAD + 1, &from, &to, 0, 0));
    CHECK(atomic_load(&b.full_drops) == UINT64_MAX);
    atomic_store(&b.discarded, UINT64_MAX);
    CHECK(pq_inbox_push(&b, dg, 10, &from, &to, 0, 0));
    pq_inbox_discard(&b);
    CHECK(atomic_load(&b.discarded) == UINT64_MAX);

    /* the closed-admission path saturates too, not only the full path */
    pq_inbox_close(&b);
    atomic_store(&b.closed_drops, UINT64_MAX - 1);
    CHECK(!pq_inbox_push(&b, dg, 10, &from, &to, 0, 0));   /* refused: closed */
    CHECK(atomic_load(&b.closed_drops) == UINT64_MAX);
    CHECK(!pq_inbox_push(&b, dg, 10, &from, &to, 0, 0));
    CHECK(atomic_load(&b.closed_drops) == UINT64_MAX);     /* stays, no wrap */

    /* the lane group folds a detect ring's full and closed drops into the
     * public detection_drops stat with this saturating add; it never wraps */
    CHECK(pq_u64_sat_add(10, 20) == 30);
    CHECK(pq_u64_sat_add(UINT64_MAX - 1, 1) == UINT64_MAX);
    CHECK(pq_u64_sat_add(UINT64_MAX, UINT64_MAX) == UINT64_MAX);
    CHECK(pq_u64_sat_add(UINT64_MAX, 5) == UINT64_MAX);

    /* close under producers: every push either landed before the close or
     * was refused after it, and the owner pops exactly the landed ones */
    pq_inbox_free(&b, alloc);
    CHECK(pq_inbox_init(&b, alloc, 64));
    producer_t prod[3];
    pthread_t th[3];
    for (int i = 0; i < 3; i++) {
        memset(&prod[i], 0, sizeof(prod[i]));
        prod[i].b = &b;
        CHECK(pthread_create(&th[i], NULL, producer_main, &prod[i]) == 0);
    }
    /* The close must fall while every producer is running, and each must
     * then be refused at least once; both waits are bounded (5 s). */
    const struct timespec ms = { .tv_sec = 0, .tv_nsec = 1000000 };
    uint64_t popped = 0;
    bool all_pushed = false;
    for (int round = 0; round < 5000 && !all_pushed; round++) {
        while (pq_inbox_pop(&b, &out)) popped++;
        all_pushed = true;
        for (int i = 0; i < 3; i++)
            if (!atomic_load(&prod[i].pushed)) all_pushed = false;
        if (!all_pushed) nanosleep(&ms, NULL);
    }
    CHECK(all_pushed);
    for (int round = 0; round < 2000; round++) {
        while (pq_inbox_pop(&b, &out)) popped++;
    }
    pq_inbox_close(&b);
    uint64_t landed_after_close = 0;
    for (int spin = 0; spin < 1000; spin++) {
        while (pq_inbox_pop(&b, &out)) { popped++; landed_after_close++; }
    }
    for (int round = 0; round < 5000 && atomic_load(&b.closed_drops) == 0; round++)
        nanosleep(&ms, NULL);
    for (int i = 0; i < 3; i++) atomic_store(&prod[i].stop, 1);
    for (int i = 0; i < 3; i++) pthread_join(th[i], NULL);
    while (pq_inbox_pop(&b, &out)) popped++;
    uint64_t attempted = 0, accepted = 0;
    for (int i = 0; i < 3; i++) { attempted += prod[i].attempted; accepted += prod[i].accepted; }
    uint64_t full = atomic_load(&b.full_drops), closed = atomic_load(&b.closed_drops);
    printf("inbox: %llu attempted, %llu accepted, %llu popped (%llu after the "
           "close), %llu full, %llu closed\n",
           (unsigned long long)attempted, (unsigned long long)accepted,
           (unsigned long long)popped, (unsigned long long)landed_after_close,
           (unsigned long long)full, (unsigned long long)closed);
    CHECK(accepted == popped);
    CHECK(attempted == accepted + full + closed);
    CHECK(closed > 0);
    /* nothing lands after the close returned */
    CHECK(!pq_inbox_push(&b, dg, 10, &from, &to, 0, 0));
    pq_inbox_free(&b, alloc);

    if (failures) {
        printf("FAILED: %d check(s) [pq_inbox]\n", failures);
        return 1;
    }
    printf("PASS: pq_inbox\n");
    return 0;
}
