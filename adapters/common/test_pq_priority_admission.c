/*
 * Stream admission depends on retained bytes, not the number of priority
 * levels. Ranking may coalesce the least urgent keys at the transport's last
 * even level, but must not prevent a more urgent stream from starting.
 *
 * Rows: copy/rcbuf x 126/127 peers x shared/distinct keys, plus byte-cap
 * refusal and bare-FIN controls. These exercise the actual adapter queue;
 * they do not model native transport flow control or packet transmission.
 */
#include "moq_pq_send_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned failures;
static size_t live_bytes;
static uint8_t observed[1024];
static const uint8_t byte = 0x5a;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static void *allocate(size_t n, void *ctx)
{
    (void)ctx;
    void *p = malloc(n);
    if (p) live_bytes += n;
    return p;
}

static void *resize(void *p, size_t old, size_t n, void *ctx)
{
    (void)ctx;
    void *r = realloc(p, n);
    if (r) live_bytes = live_bytes - old + n;
    return r;
}

static void release(void *p, size_t n, void *ctx)
{
    (void)ctx;
    if (p) { live_bytes -= n; free(p); }
}

static const moq_alloc_t alloc = {
    .alloc = allocate, .realloc = resize, .free = release,
};

static int apply(void *ctx, uint64_t sid, uint8_t priority)
{
    (void)ctx;
    if (sid >= sizeof(observed)) return -1;
    observed[sid] = priority;
    return 0;
}

static bool open_key(moq_pq_send_queue_t *q, uint64_t sid, uint32_t key)
{
    return moq_pq_send_queue_opened(q, sid) == 0 &&
           moq_pq_send_queue_set_key(q, sid, key) == 0;
}

static int push(moq_pq_send_queue_t *q, uint64_t sid, moq_rcbuf_t *buf)
{
    return buf ? moq_pq_send_queue_push_rcbuf(q, sid, buf, false) :
                 moq_pq_send_queue_push_copy(q, sid, &byte, 1, false);
}

static void row_admission(bool retained, unsigned peers, bool distinct)
{
    const unsigned before = failures;
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&alloc, 4096);
    moq_rcbuf_t *buf = NULL;
    CHECK(q != NULL);
    if (!q) goto done;
    if (retained) {
        CHECK(moq_rcbuf_create(&alloc, &byte, 1, &buf) == MOQ_OK);
        if (!buf) goto done;
    }
    memset(observed, 0xff, sizeof(observed));
    for (unsigned i = 0; i < peers; ++i) {
        const uint64_t sid = 4u * i + 3u;
        CHECK(open_key(q, sid, UINT32_C(0x80800001) + (distinct ? i : 0)));
        CHECK(push(q, sid, buf) == 1);
    }
    CHECK(moq_pq_send_queue_queued_bytes(q) == peers);
    CHECK(moq_pq_send_queue_apply_priorities(q, apply, NULL) == 0);
    const uint64_t urgent = 4u * peers + 3u;
    CHECK(open_key(q, urgent, UINT32_C(0x00800001)));
    CHECK(push(q, urgent, buf) == 1);
    CHECK(moq_pq_send_queue_queued_bytes(q) == peers + 1u);
    CHECK(moq_pq_send_queue_would_block_count(q) == 0);
    if (buf) CHECK(moq_rcbuf_refcount(buf) == peers + 2u);
    CHECK(moq_pq_send_queue_apply_priorities(q, apply, NULL) == 0);
    CHECK(observed[urgent] < observed[3]);
    for (unsigned i = 0; i < peers; ++i) {
        const uint64_t sid = 4u * i + 3u;
        CHECK((observed[sid] & 1u) == 0);
        if (i) {
            CHECK(observed[sid] >= observed[sid - 4]);
            if (!distinct) CHECK(observed[sid] == observed[sid - 4]);
        }
    }
    /* A queued stream can become more urgent without changing byte or
     * reference ownership, even when every native rank is occupied. */
    CHECK(moq_pq_send_queue_set_key(q, 3, 1) == 0);
    CHECK(moq_pq_send_queue_apply_priorities(q, apply, NULL) == 0);
    CHECK(observed[3] < observed[urgent]);
    CHECK(moq_pq_send_queue_queued_bytes(q) == peers + 1u);
    size_t nbytes = 0;
    bool fin = false, active = false;
    uint8_t out = 0;
    CHECK(moq_pq_send_queue_plan(q, urgent, 1, &nbytes, &fin, &active));
    CHECK(nbytes == 1 && !fin && !active);
    if (nbytes == 1) moq_pq_send_queue_commit(q, urgent, &out, nbytes);
    CHECK(out == byte);
    CHECK(!moq_pq_send_queue_has_data(q, urgent));
    if (buf) CHECK(moq_rcbuf_refcount(buf) == peers + 1u);
done:
    moq_pq_send_queue_destroy(q);
    if (buf) {
        CHECK(moq_rcbuf_refcount(buf) == 1);
        moq_rcbuf_decref(buf);
    }
    CHECK(live_bytes == 0);
    printf("admission retained=%u peers=%u distinct=%u failures=%u\n",
           retained, peers, distinct, failures - before);
}

static void row_byte_cap(bool retained)
{
    const unsigned before = failures;
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&alloc, 2);
    moq_rcbuf_t *buf = NULL;
    CHECK(q != NULL);
    if (!q) goto done;
    if (retained) {
        CHECK(moq_rcbuf_create(&alloc, &byte, 1, &buf) == MOQ_OK);
        if (!buf) goto done;
    }
    CHECK(open_key(q, 3, 20) && open_key(q, 7, 20) && open_key(q, 11, 1));
    CHECK(push(q, 3, buf) == 1 && push(q, 7, buf) == 1);
    CHECK(push(q, 11, buf) == 0);
    CHECK(moq_pq_send_queue_queued_bytes(q) == 2);
    CHECK(moq_pq_send_queue_would_block_count(q) == 1);
    if (buf) CHECK(moq_rcbuf_refcount(buf) == 3);
    /* FIN on an existing chunk needs no new marker. A fresh standalone FIN
     * consumes metadata capacity and must wait while the budget is full. */
    CHECK(moq_pq_send_queue_push_copy(q, 3, NULL, 0, true) == 1);
    CHECK(open_key(q, 15, 30));
    CHECK(moq_pq_send_queue_push_copy(q, 15, NULL, 0, true) == 0);
    moq_pq_send_queue_drop(q, 3);
    CHECK(moq_pq_send_queue_push_copy(q, 15, NULL, 0, true) == 1);
    CHECK(push(q, 11, buf) == 0);
    size_t nbytes = 0;
    bool fin = false, active = false;
    CHECK(moq_pq_send_queue_plan(q, 15, 1, &nbytes, &fin, &active));
    CHECK(nbytes == 0 && fin && !active);
    moq_pq_send_queue_commit(q, 15, NULL, 0);
    CHECK(!moq_pq_send_queue_has_data(q, 15));
    CHECK(push(q, 11, buf) == 1);
    CHECK(moq_pq_send_queue_queued_bytes(q) == 2);
    if (buf) CHECK(moq_rcbuf_refcount(buf) == 3);
done:
    moq_pq_send_queue_destroy(q);
    if (buf) {
        CHECK(moq_rcbuf_refcount(buf) == 1);
        moq_rcbuf_decref(buf);
    }
    CHECK(live_bytes == 0);
    printf("byte_cap retained=%u failures=%u\n", retained, failures - before);
}

static void row_fin_capacity(bool retained)
{
    const unsigned before = failures;
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&alloc, 4);
    moq_rcbuf_t *buf = NULL;
    CHECK(q != NULL);
    if (!q) goto done;
    if (retained) {
        CHECK(moq_rcbuf_create(&alloc, NULL, 0, &buf) == MOQ_OK);
        if (!buf) goto done;
    }
    for (unsigned i = 0; i < 5; ++i) {
        const uint64_t sid = 4u * i + 3u;
        CHECK(open_key(q, sid, 10));
        const int rc = retained ? moq_pq_send_queue_push_rcbuf(q, sid, buf, true)
                                : moq_pq_send_queue_push_copy(q, sid, NULL, 0, true);
        CHECK(rc == (i < 4 ? 1 : 0));
    }
    CHECK(moq_pq_send_queue_queued_bytes(q) == 0);
    if (buf) CHECK(moq_rcbuf_refcount(buf) == 1);
    const size_t held = live_bytes;
    for (unsigned i = 0; i < 10; ++i)
        CHECK(moq_pq_send_queue_push_copy(q, 19, NULL, 0, true) == 0);
    CHECK(live_bytes == held);
    moq_pq_send_queue_drop(q, 3);
    CHECK(moq_pq_send_queue_push_copy(q, 19, NULL, 0, true) == 1);
    CHECK(moq_pq_send_queue_truncate(q, 7, 0) == 0);
    CHECK(open_key(q, 23, 1));
    CHECK(moq_pq_send_queue_push_copy(q, 23, &byte, 1, false) == 1);
    CHECK(moq_pq_send_queue_push_copy(q, 27, &byte, 1, false) == 0);
done:
    moq_pq_send_queue_destroy(q);
    if (buf) moq_rcbuf_decref(buf);
    CHECK(live_bytes == 0);
    printf("fin_capacity retained=%u failures=%u\n", retained, failures - before);
}

int main(void)
{
    for (unsigned retained = 0; retained < 2; ++retained) {
        for (unsigned distinct = 0; distinct < 2; ++distinct) {
            row_admission(retained != 0, 126, distinct != 0);
            row_admission(retained != 0, 127, distinct != 0);
        }
        row_byte_cap(retained != 0);
        row_fin_capacity(retained != 0);
    }
    printf("12 rows, failures=%u, live_bytes=%zu\n", failures, live_bytes);
    return failures ? 1 : 0;
}
