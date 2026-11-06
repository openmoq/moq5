/* The rcbuf ownership predicates the receive path relies on:
 *   moq_rcbuf_is_created_root     (core-internal): a buffer the session may
 *                                 hand out as an event payload in place of a
 *                                 copy;
 *   moq_rcbuf_allocated_by        (core-internal): ...and only when it was
 *                                 allocated with the session's allocator;
 *   moq_core_internal_rcbuf_transferable (core SPI): a buffer that may move
 *                                 whole to another thread.
 * Each row starts from a case that holds and changes one thing. */
#include "test_support.h"
#include "../../core/src/spi/core_spi.h"
#include "../../core/src/base/rcbuf_internal.h"

#include <stdlib.h>
#include <string.h>

static int failures = 0;

typedef struct { int64_t live; } ta_t;
static void *ta_alloc(size_t n, void *ctx) { ((ta_t *)ctx)->live++; return malloc(n); }
static void *ta_realloc(void *p, size_t o, size_t n, void *ctx) { (void)o; (void)ctx; return realloc(p, n); }
static void ta_free(void *p, size_t n, void *ctx) { (void)n; if (p) ((ta_t *)ctx)->live--; free(p); }
static void ta_free_other(void *p, size_t n, void *ctx) { ta_free(p, n, ctx); }

static int g_released;
static void release_fn(void *ctx, const uint8_t *d, size_t n) { (void)ctx; (void)d; (void)n; g_released++; }

int main(void)
{
    ta_t sa = {0}, oa = {0};
    moq_alloc_t trusted_alloc = { &sa, ta_alloc, ta_realloc, ta_free };
    moq_alloc_t other_alloc = { &oa, ta_alloc, ta_realloc, ta_free };
    /* A field-for-field equal copy at another address is the same allocator. */
    moq_alloc_t trusted_copy = trusted_alloc;
    const moq_alloc_t *trusted[1] = { &trusted_copy };
    uint8_t bytes[100];
    memset(bytes, 7, sizeof(bytes));

    MOQ_TEST_CHECK(!moq_rcbuf_is_created_root(NULL));
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(NULL, trusted, 1));
    MOQ_TEST_CHECK(!moq_rcbuf_allocated_by(NULL, &trusted_alloc));

    /* Allocator provenance: equal field for field, ctx included. */
    {
        moq_rcbuf_t *b = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&trusted_alloc, bytes, 100, &b) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_allocated_by(b, &trusted_alloc));
        MOQ_TEST_CHECK(moq_rcbuf_allocated_by(b, &trusted_copy));
        MOQ_TEST_CHECK(!moq_rcbuf_allocated_by(b, &other_alloc));   /* same functions, other ctx */
        moq_alloc_t other_free = trusted_alloc;
        other_free.free = ta_free_other;
        MOQ_TEST_CHECK(!moq_rcbuf_allocated_by(b, &other_free));    /* same ctx, other free */
        MOQ_TEST_CHECK(!moq_rcbuf_allocated_by(b, NULL));
        moq_rcbuf_decref(b);
    }

    /* A created root, only reference, trusted allocator: shareable in place
     * of a copy, and transferable. */
    moq_rcbuf_t *root = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&trusted_alloc, bytes, 100, &root) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_is_created_root(root));
    MOQ_TEST_CHECK(moq_core_internal_rcbuf_transferable(root, trusted, 1));
    /* ... but not with no trusted allocator, or a second reference. */
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(root, trusted, 0));
    moq_rcbuf_incref(root);
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(root, trusted, 1));
    moq_rcbuf_decref(root);
    MOQ_TEST_CHECK(moq_core_internal_rcbuf_transferable(root, trusted, 1));

    /* The same, from an allocator not trusted. */
    moq_rcbuf_t *foreign = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&other_alloc, bytes, 100, &foreign) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_is_created_root(foreign));
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(foreign, trusted, 1));
    moq_rcbuf_decref(foreign);

    /* A slice covering the whole root: not a created root (it keeps a second
     * header alive); transferable only once it holds the root's only
     * reference. */
    moq_rcbuf_t *whole = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_slice(&trusted_alloc, root, 0, 100, &whole) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_rcbuf_is_created_root(whole));
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(whole, trusted, 1));
    moq_rcbuf_decref(root);                      /* the slice's is now the only one */
    MOQ_TEST_CHECK(moq_core_internal_rcbuf_transferable(whole, trusted, 1));
    moq_rcbuf_decref(whole);

    /* A slice covering part of its root keeps the root's other bytes: never
     * transferable, even as the only reference. */
    MOQ_TEST_CHECK(moq_rcbuf_create(&trusted_alloc, bytes, 100, &root) == MOQ_OK);
    moq_rcbuf_t *part = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_slice(&trusted_alloc, root, 0, 99, &part) == MOQ_OK);
    moq_rcbuf_decref(root);
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(part, trusted, 1));
    moq_rcbuf_decref(part);

    /* A whole-root slice whose header comes from an allocator not trusted. */
    MOQ_TEST_CHECK(moq_rcbuf_create(&trusted_alloc, bytes, 100, &root) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_slice(&other_alloc, root, 0, 100, &whole) == MOQ_OK);
    moq_rcbuf_decref(root);
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(whole, trusted, 1));
    moq_rcbuf_decref(whole);

    /* A wrapped buffer: external storage and release context. Neither a
     * created root nor transferable, alone or through a slice. */
    g_released = 0;
    moq_rcbuf_t *wrapped = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_wrap(&trusted_alloc, bytes, 100, release_fn, NULL, &wrapped) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_rcbuf_is_created_root(wrapped));
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(wrapped, trusted, 1));
    MOQ_TEST_CHECK(moq_rcbuf_slice(&trusted_alloc, wrapped, 0, 100, &whole) == MOQ_OK);
    moq_rcbuf_decref(wrapped);
    MOQ_TEST_CHECK(!moq_core_internal_rcbuf_transferable(whole, trusted, 1));
    moq_rcbuf_decref(whole);
    MOQ_TEST_CHECK(g_released == 1);

    MOQ_TEST_CHECK(sa.live == 0);
    MOQ_TEST_CHECK(oa.live == 0);
    if (failures) {
        fprintf(stderr, "test_rcbuf_transferable: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_rcbuf_transferable: all passed\n");
    return 0;
}
