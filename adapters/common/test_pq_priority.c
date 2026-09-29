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
    return 0;
}
