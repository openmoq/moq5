#ifndef MOQ_TEST_ALLOC_SUPPORT_H
#define MOQ_TEST_ALLOC_SUPPORT_H

#include <moq/types.h>
#include <stdlib.h>

/* Counting allocator only: no session layout or private core helpers. */
typedef struct test_alloc_state {
    int64_t balance;
} test_alloc_state_t;

static inline void *test_alloc(size_t size, void *ctx)
{
    test_alloc_state_t *state = (test_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) state->balance++;
    return p;
}

static inline void *test_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size; (void)ctx;
    return realloc(ptr, new_size);
}

static inline void test_free(void *ptr, size_t size, void *ctx)
{
    test_alloc_state_t *state = (test_alloc_state_t *)ctx;
    (void)size;
    if (ptr) state->balance--;
    free(ptr);
}

static inline moq_alloc_t test_allocator(test_alloc_state_t *state)
{
    moq_alloc_t alloc = { state, test_alloc, test_realloc, test_free };
    return alloc;
}

#endif /* MOQ_TEST_ALLOC_SUPPORT_H */
