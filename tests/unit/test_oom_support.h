#ifndef MOQ_TEST_OOM_SUPPORT_H
#define MOQ_TEST_OOM_SUPPORT_H

#include <moq/moq.h>
#include <stdlib.h>
#include <stdint.h>

/*
 * Fail-N allocator for OOM sweep testing.
 *
 * Delegates to libc malloc/realloc/free. Tracks allocation count and
 * balance. Fails exactly the Nth alloc/realloc call (1-indexed) by
 * returning NULL. Set fail_at=0 to disable failure injection.
 *
 * alloc(0) returns NULL without counting (the core should not do this).
 * realloc(ptr, old, 0) frees ptr and returns NULL (no fail injection).
 * realloc(NULL, old, new) counts as a new allocation.
 * realloc(ptr, old, new) counts as a reallocation attempt; on injected
 *   failure returns NULL and leaves ptr/balance unchanged.
 */

typedef struct oom_alloc_state {
    int64_t  balance;     /* allocs - frees; 0 after clean destroy */
    uint64_t alloc_count; /* total alloc/realloc attempts */
    uint64_t fail_at;     /* fail the Nth call (1-indexed); 0 = disabled */
} oom_alloc_state_t;

static void *oom_alloc(size_t size, void *ctx)
{
    oom_alloc_state_t *s = (oom_alloc_state_t *)ctx;
    if (size == 0) return NULL;
    s->alloc_count++;
    if (s->fail_at > 0 && s->alloc_count == s->fail_at)
        return NULL;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}

static void *oom_realloc(void *ptr, size_t old_size, size_t new_size,
                          void *ctx)
{
    oom_alloc_state_t *s = (oom_alloc_state_t *)ctx;
    (void)old_size;
    if (new_size == 0) {
        if (ptr) { free(ptr); s->balance--; }
        return NULL;
    }
    s->alloc_count++;
    if (s->fail_at > 0 && s->alloc_count == s->fail_at)
        return NULL;
    if (!ptr) {
        void *p = malloc(new_size);
        if (p) s->balance++;
        return p;
    }
    void *p = realloc(ptr, new_size);
    return p;
}

static void oom_free(void *ptr, size_t size, void *ctx)
{
    oom_alloc_state_t *s = (oom_alloc_state_t *)ctx;
    (void)size;
    if (ptr) s->balance--;
    free(ptr);
}

static inline moq_alloc_t oom_allocator(oom_alloc_state_t *state)
{
    moq_alloc_t alloc = { state, oom_alloc, oom_realloc, oom_free };
    return alloc;
}

#endif /* MOQ_TEST_OOM_SUPPORT_H */
