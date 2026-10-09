#include "moq_pq_send_queue.h"
#undef NDEBUG
#include <assert.h>
#include <stdlib.h>

static void *allocate(size_t n, void *ctx) { (void)ctx; return malloc(n); }
static void *resize(void *p, size_t old, size_t n, void *ctx)
{ (void)old; (void)ctx; return realloc(p, n); }
static void release(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; free(p); }
static uint8_t observed[512];
static int apply(void *ctx, uint64_t sid, uint8_t priority)
{ (void)ctx; assert(sid < 512); observed[sid] = priority; return 0; }

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
    moq_pq_send_queue_drop(q, 2);
    assert(moq_pq_send_queue_priority(q, 6, 0x18010, apply, NULL) == 0);
    assert(observed[10] == 2);
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

    /* A newly opened keyed stream starts only while fewer than 126 keyed
     * streams hold bytes, so each keeps its own level; a stream that already
     * started is never held back. */
    q = moq_pq_send_queue_create(&alloc, 0);
    assert(q);
    static const uint8_t byte = 0x5a;
    for (unsigned i = 0; i < 126; ++i) {
        assert(moq_pq_send_queue_opened(q, 4 * i) == 0);
        assert(moq_pq_send_queue_set_key(q, 4 * i, 0x10000 + i) == 0);
        assert(moq_pq_send_queue_push_copy(q, 4 * i, &byte, 1, false) == 1);
    }
    assert(moq_pq_send_queue_opened(q, 504) == 0);
    assert(moq_pq_send_queue_set_key(q, 504, 0x10000 + 126) == 0);
    assert(moq_pq_send_queue_push_copy(q, 504, &byte, 1, false) == 0);
    assert(moq_pq_send_queue_push_copy(q, 0, &byte, 1, false) == 1);
    assert(moq_pq_send_queue_push_copy(q, 2, &byte, 1, false) == 1);
    size_t nb = 0;
    bool fin = false, still = false;
    uint8_t out[8];
    assert(moq_pq_send_queue_plan(q, 0, sizeof(out), &nb, &fin, &still));
    moq_pq_send_queue_commit(q, 0, out, nb);
    assert(!moq_pq_send_queue_has_data(q, 0));
    assert(moq_pq_send_queue_push_copy(q, 504, &byte, 1, false) == 1);
    assert(moq_pq_send_queue_set_key(q, 0, 0x10000) == 0);
    assert(moq_pq_send_queue_push_copy(q, 0, NULL, 0, true) == 1);
    assert(moq_pq_send_queue_opened(q, 508) == 0);
    assert(moq_pq_send_queue_set_key(q, 508, 0x10000 + 127) == 0);
    assert(moq_pq_send_queue_push_copy(q, 508, &byte, 1, false) == 0);
    assert(moq_pq_send_queue_apply_priorities(q, apply, NULL) == 0);
    for (unsigned i = 1; i <= 126; ++i)
        assert(observed[4 * i] > observed[4 * (i - 1)]);
    assert(observed[2] == 0);
    moq_pq_send_queue_destroy(q);
    return 0;
}
