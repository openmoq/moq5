#include "moq_pq_send_queue.h"
#undef NDEBUG
#include <assert.h>
#include <stdlib.h>
#include <string.h>

static void *allocate(size_t n, void *ctx) { (void)ctx; return malloc(n); }
static void *resize(void *p, size_t old, size_t n, void *ctx)
{ (void)old; (void)ctx; return realloc(p, n); }
static void release(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; free(p); }
static uint8_t observed[1024];
static int apply(void *ctx, uint64_t sid, uint8_t priority)
{ (void)ctx; assert(sid < 1024); observed[sid] = priority; return 0; }

int main(void)
{
    const moq_alloc_t alloc = { .alloc = allocate, .realloc = resize, .free = release };
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&alloc, 64);
    assert(q);
    assert(moq_pq_send_queue_priority(q, 2, 0x180c8, apply, NULL) == 0);
    assert(moq_pq_send_queue_priority(q, 6, 0x18010, apply, NULL) == 0);
    assert(observed[6] < observed[2]);
    assert(moq_pq_send_queue_priority(q, 10, 0x17fff, apply, NULL) == 0);
    assert(observed[10] < observed[6]); /* subscriber precedes publisher */
    assert(moq_pq_send_queue_priority(q, 14, 0x18010, apply, NULL) == 0);
    assert(observed[14] == observed[6]);
    assert((observed[14] & 1) == 0); /* equal-priority round robin */
    assert(moq_pq_send_queue_priority(q, 0, 0, apply, NULL) == 0);
    assert(observed[0] < observed[10]);
    assert(moq_pq_send_queue_priority(q, 2, 0x10000, apply, NULL) == 0);
    assert(observed[2] < observed[10]); /* updates reprioritize existing bytes */
    /* Ending a stream keeps the others' order without re-ranking them; the
     * next change ranks densely again. */
    moq_pq_send_queue_drop(q, 2);
    assert(moq_pq_send_queue_priority(q, 6, 0x18010, apply, NULL) == 0);
    assert(observed[10] < observed[6] && observed[6] == observed[14]);
    assert(moq_pq_send_queue_priority(q, 18, 0x18020, apply, NULL) == 0);
    assert(observed[10] == 2 && observed[6] == 4 && observed[14] == 4);
    assert(observed[18] == 6);
    moq_pq_send_queue_destroy(q);

    q = moq_pq_send_queue_create(&alloc, 64);
    assert(q);
    for (unsigned i = 0; i < 140; ++i)
        assert(moq_pq_send_queue_priority(q, i, 0x10000 + i, apply, NULL) == 0);
    for (unsigned i = 1; i < 140; ++i) {
        assert(observed[i] >= observed[i - 1]);
        assert((observed[i] & 1) == 0);
    }
    assert(observed[126] == 254 && observed[139] == 254);
    moq_pq_send_queue_destroy(q);

    /* The cap is a low-water mark: below it a push goes in whole, however
     * large, so the first write offered when room frees is never passed over;
     * at or above it every push waits. */
    q = moq_pq_send_queue_create(&alloc, 64);
    assert(q);
    static uint8_t big[100];
    assert(moq_pq_send_queue_push_copy(q, 1, big, 60, false) == 1);
    assert(moq_pq_send_queue_push_copy(q, 5, big, sizeof(big), false) == 1);
    assert(moq_pq_send_queue_queued_bytes(q) == 160);
    assert(moq_pq_send_queue_push_copy(q, 9, big, 1, false) == 0);
    assert(moq_pq_send_queue_push_copy(q, 1, big, 1, false) == 0);
    moq_pq_send_queue_destroy(q);

    /* More than 127 distinct keys remain admissible within the byte budget.
     * Excess ranks share level 254; existing bytes and control keep moving. */
    memset(observed, 0xff, sizeof(observed));
    q = moq_pq_send_queue_create(&alloc, 0);
    assert(q);
    static const uint8_t byte = 0x5a;
    for (unsigned i = 0; i < 127; ++i) {
        assert(moq_pq_send_queue_opened(q, 4 * i) == 0);
        assert(moq_pq_send_queue_set_key(q, 4 * i, 0x10000 + i) == 0);
        assert(moq_pq_send_queue_push_copy(q, 4 * i, &byte, 1, false) == 1);
    }
    assert(moq_pq_send_queue_opened(q, 508) == 0);
    assert(moq_pq_send_queue_set_key(q, 508, 0x10000 + 127) == 0);
    assert(moq_pq_send_queue_push_copy(q, 508, &byte, 1, false) == 1);
    assert(moq_pq_send_queue_push_copy(q, 0, &byte, 1, false) == 1);
    assert(moq_pq_send_queue_push_copy(q, 2, &byte, 1, false) == 1);
    size_t nb = 0;
    bool fin = false, still = false;
    uint8_t out[8];
    assert(moq_pq_send_queue_plan(q, 0, sizeof(out), &nb, &fin, &still));
    moq_pq_send_queue_commit(q, 0, out, nb);
    assert(!moq_pq_send_queue_has_data(q, 0));
    assert(moq_pq_send_queue_push_copy(q, 508, &byte, 1, false) == 1);
    assert(moq_pq_send_queue_set_key(q, 0, 0x10000) == 0);
    assert(moq_pq_send_queue_push_copy(q, 0, NULL, 0, true) == 1);
    assert(moq_pq_send_queue_opened(q, 512) == 0);
    assert(moq_pq_send_queue_set_key(q, 512, 0x10000 + 128) == 0);
    assert(moq_pq_send_queue_push_copy(q, 512, &byte, 1, false) == 1);
    assert(moq_pq_send_queue_apply_priorities(q, apply, NULL) == 0);
    for (unsigned i = 1; i < 127; ++i)
        assert(observed[4 * i] == 2 + 2 * i);
    assert(observed[0] == 2 && observed[508] == 254);
    assert(observed[2] == 0);
    assert(observed[512] == 254);
    moq_pq_send_queue_destroy(q);
    return 0;
}
