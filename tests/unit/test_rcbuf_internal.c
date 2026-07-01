/*
 * White-box tests for the core-internal rcbuf constructors
 * (core/src/base/rcbuf_internal.h). Links moq-core-test-internals so the
 * non-exported symbol is reachable.
 */
#include <moq/moq.h>
#include "test_support.h"
#include "../../core/src/base/rcbuf_internal.h"
#include <stdlib.h>
#include <string.h>

/* -- Counting allocator whose balance lives in ctx --------------------
 * Keeping the balance in ctx (not a global) lets a test model "the session
 * is gone but the allocator ctx is still valid": the rcbuf frees through the
 * copied vtable + ctx, never through any session object. */
static void *ctx_alloc(size_t size, void *ctx)
{
    void *p = malloc(size);
    if (p) (*(int64_t *)ctx)++;
    return p;
}
static void *ctx_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size; (void)ctx;
    return realloc(ptr, new_size);
}
static void ctx_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    if (ptr) { (*(int64_t *)ctx)--; free(ptr); }
}

static void *failing_alloc(size_t size, void *ctx)
{
    (void)size; (void)ctx;
    return NULL;
}

int main(void)
{
    int failures = 0;

    int64_t balance = 0;
    const moq_alloc_t counting = { &balance, ctx_alloc, ctx_realloc, ctx_free };

    /* -- alloc_uninit: writable region, len/data correct, decref balance -- */
    {
        balance = 0;
        moq_rcbuf_t *buf = NULL;
        uint8_t *data = NULL;
        moq_result_t rc = moq_rcbuf_alloc_uninit(&counting, 8, &buf, &data);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(buf != NULL);
        MOQ_TEST_CHECK(data != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 8);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(buf) == 1);
        /* data_out aliases the rcbuf's inline payload region. */
        MOQ_TEST_CHECK((const uint8_t *)data == moq_rcbuf_data(buf));
        MOQ_TEST_CHECK(balance == 1); /* one allocation, header+payload */

        /* Fill directly, then read back through the public accessor. */
        memcpy(data, "ZYXWVUTS", 8);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(buf), "ZYXWVUTS", 8) == 0);

        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(balance == 0);
    }

    /* -- len == 0: data_out set (past header), decref balances ---------- */
    {
        balance = 0;
        moq_rcbuf_t *buf = NULL;
        uint8_t *data = NULL;
        moq_result_t rc = moq_rcbuf_alloc_uninit(&counting, 0, &buf, &data);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(buf != NULL);
        MOQ_TEST_CHECK(data != NULL);            /* points past the header */
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 0);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(balance == 0);
    }

    /* -- create() still behaves after being refactored onto alloc_uninit - */
    {
        balance = 0;
        moq_rcbuf_t *buf = NULL;
        moq_result_t rc = moq_rcbuf_create(&counting,
                                           (const uint8_t *)"hello", 5, &buf);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(buf), "hello", 5) == 0);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(balance == 0);
    }

    /* -- Argument validation: nothing allocated on error ---------------- */
    {
        balance = 0;
        uint8_t *data = (uint8_t *)0x1; /* must be nulled on error */
        moq_rcbuf_t *buf = (moq_rcbuf_t *)0x1;

        /* NULL out. */
        MOQ_TEST_CHECK(moq_rcbuf_alloc_uninit(&counting, 4, NULL, &data)
                       == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(data == NULL);

        /* NULL data_out. */
        buf = (moq_rcbuf_t *)0x1;
        MOQ_TEST_CHECK(moq_rcbuf_alloc_uninit(&counting, 4, &buf, NULL)
                       == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);

        /* NULL allocator. */
        buf = (moq_rcbuf_t *)0x1; data = (uint8_t *)0x1;
        MOQ_TEST_CHECK(moq_rcbuf_alloc_uninit(NULL, 4, &buf, &data)
                       == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);
        MOQ_TEST_CHECK(data == NULL);

        /* Overflow: len that would wrap sizeof(header)+len. */
        buf = (moq_rcbuf_t *)0x1; data = (uint8_t *)0x1;
        MOQ_TEST_CHECK(moq_rcbuf_alloc_uninit(&counting, SIZE_MAX, &buf, &data)
                       == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);
        MOQ_TEST_CHECK(data == NULL);

        MOQ_TEST_CHECK(balance == 0); /* no allocation on any error path */
    }

    /* -- NOMEM: allocator returns NULL --------------------------------- */
    {
        const moq_alloc_t failing = { NULL, failing_alloc, ctx_realloc,
                                      ctx_free };
        moq_rcbuf_t *buf = (moq_rcbuf_t *)0x1;
        uint8_t *data = (uint8_t *)0x1;
        moq_result_t rc = moq_rcbuf_alloc_uninit(&failing, 16, &buf, &data);
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(buf == NULL);
        MOQ_TEST_CHECK(data == NULL);
    }

    /* -- Contract: buffer outlives the "session"; allocator ctx stays
     * valid until final decref. The rcbuf copies the allocator vtable and
     * frees through it + ctx -- it never references a session. Model that by
     * building the rcbuf, discarding an unrelated "session" object, and
     * decref'ing afterwards with ctx still live. ------------------------ */
    {
        /* ctx (balance) is heap-allocated so it can plausibly outlive a
         * session; it must stay valid until the final decref. */
        int64_t *live_balance = (int64_t *)malloc(sizeof(int64_t));
        MOQ_TEST_CHECK(live_balance != NULL);
        *live_balance = 0;
        const moq_alloc_t alloc = { live_balance, ctx_alloc, ctx_realloc,
                                    ctx_free };

        moq_rcbuf_t *buf = NULL;
        uint8_t *data = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_alloc_uninit(&alloc, 4, &buf, &data)
                       == MOQ_OK);
        memcpy(data, "abcd", 4);

        /* Stand-in for "session destroyed": an unrelated object goes away.
         * The rcbuf holds no pointer to it. */
        void *fake_session = malloc(32);
        MOQ_TEST_CHECK(fake_session != NULL);
        free(fake_session);

        /* Now the app cleans up the payload: decref must free through the
         * copied vtable + still-valid ctx. */
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(buf), "abcd", 4) == 0);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(*live_balance == 0);

        free(live_balance);
    }

    MOQ_TEST_PASS("test_rcbuf_internal");
    return failures;
}
