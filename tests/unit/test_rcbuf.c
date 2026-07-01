#include <moq/moq.h>
#include "test_support.h"
#include <stdlib.h>
#include <string.h>

/* -- Counting allocator -------------------------------------------- */

static int64_t g_alloc_balance = 0;

static void *counting_alloc(size_t size, void *ctx)
{
    (void)ctx;
    void *p = malloc(size);
    if (p) g_alloc_balance++;
    return p;
}

static void *counting_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size; (void)ctx;
    return realloc(ptr, new_size);
}

static void counting_free(void *ptr, size_t size, void *ctx)
{
    (void)size; (void)ctx;
    if (ptr) g_alloc_balance--;
    free(ptr);
}

static const moq_alloc_t g_counting = {
    NULL, counting_alloc, counting_realloc, counting_free
};

/* -- Second counting allocator (destination, for clone accounting) -- */

static int64_t g_alloc_balance2 = 0;

static void *counting_alloc2(size_t size, void *ctx)
{
    (void)ctx;
    void *p = malloc(size);
    if (p) g_alloc_balance2++;
    return p;
}

static void counting_free2(void *ptr, size_t size, void *ctx)
{
    (void)size; (void)ctx;
    if (ptr) g_alloc_balance2--;
    free(ptr);
}

static const moq_alloc_t g_counting2 = {
    NULL, counting_alloc2, counting_realloc, counting_free2
};

/* -- Failing allocator --------------------------------------------- */

static void *failing_alloc(size_t size, void *ctx)
{
    (void)size; (void)ctx;
    return NULL;
}

static const moq_alloc_t g_failing = {
    NULL, failing_alloc, counting_realloc, counting_free
};

/* -- Release callback for wrapped buffer test ------------------------ */

static int g_release_count = 0;

static void test_release_fn(void *ctx, const uint8_t *data, size_t len)
{
    (void)data; (void)len;
    int *count = (int *)ctx;
    (*count)++;
}

int main(void)
{
    int failures = 0;

    /* -- Basic create/data/len/decref ------------------------------ */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *buf = NULL;
        moq_result_t rc = moq_rcbuf_create(&g_counting,
                                            (const uint8_t *)"hello", 5, &buf);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(buf != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(buf), "hello", 5) == 0);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(buf) == 1);

        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Incref/decref --------------------------------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"abc", 3, &buf);

        moq_rcbuf_t *ref = moq_rcbuf_incref(buf);
        MOQ_TEST_CHECK(ref == buf);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(buf) == 2);

        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(ref) == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 1);

        moq_rcbuf_decref(ref);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- NULL safety ------------------------------------------------ */
    {
        MOQ_TEST_CHECK(moq_rcbuf_incref(NULL) == NULL);
        moq_rcbuf_decref(NULL); /* no crash */
        MOQ_TEST_CHECK(moq_rcbuf_data(NULL) == NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(NULL) == 0);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(NULL) == 0);
    }

    /* -- Zero-length buffer ----------------------------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *buf = NULL;
        moq_result_t rc = moq_rcbuf_create(&g_counting, NULL, 0, &buf);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(buf != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 0);
        MOQ_TEST_CHECK(moq_rcbuf_data(buf) != NULL); /* points past header */
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Allocation failure ----------------------------------------- */
    {
        moq_rcbuf_t *buf = (moq_rcbuf_t *)0xDEAD; /* sentinel */
        moq_result_t rc = moq_rcbuf_create(&g_failing,
                                            (const uint8_t *)"x", 1, &buf);
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(buf == NULL);
    }

    /* -- Invalid args ----------------------------------------------- */
    {
        moq_rcbuf_t *buf = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(NULL, NULL, 0, &buf) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_rcbuf_create(&g_counting, NULL, 0, NULL) == MOQ_ERR_INVAL);
    }

    /* -- Non-zero len with NULL data is rejected -------------------- */
    {
        moq_rcbuf_t *buf = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&g_counting, NULL, 10, &buf) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);
    }

    /* -- Allocator missing alloc/free is rejected ------------------- */
    {
        moq_alloc_t bad_alloc = { NULL, NULL, NULL, NULL };
        moq_rcbuf_t *buf = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&bad_alloc, NULL, 0, &buf) == MOQ_ERR_INVAL);
    }

    /* -- Wrapped: data pointer is original, no copy -------------------- */
    {
        g_alloc_balance = 0;
        static const uint8_t payload[] = "wrapped";
        moq_rcbuf_t *buf = NULL;
        moq_result_t rc = moq_rcbuf_wrap(&g_counting, payload,
            sizeof(payload) - 1, NULL, NULL, &buf);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(buf != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_data(buf) == payload);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 7);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(buf) == 1);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Wrapped: release callback fires once on final decref --------- */
    {
        g_alloc_balance = 0;
        static const uint8_t data[] = "release";
        g_release_count = 0;

        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_wrap(&g_counting, data, 7, test_release_fn,
            &g_release_count, &buf);

        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_incref(buf);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Wrapped: zero-length ----------------------------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *buf = NULL;
        moq_result_t rc = moq_rcbuf_wrap(&g_counting, NULL, 0,
            NULL, NULL, &buf);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 0);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Wrapped: NULL release, no crash ------------------------------ */
    {
        g_alloc_balance = 0;
        static const uint8_t d[] = "ok";
        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_wrap(&g_counting, d, 2, NULL, NULL, &buf);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Wrapped: zero-length with release callback -------------------- */
    {
        g_alloc_balance = 0;
        g_release_count = 0;
        moq_rcbuf_t *buf = NULL;
        moq_rcbuf_wrap(&g_counting, NULL, 0, test_release_fn,
            &g_release_count, &buf);
        MOQ_TEST_CHECK(buf != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_data(buf) == NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(buf) == 0);
        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_decref(buf);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Wrapped: invalid args clear *out ----------------------------- */
    {
        moq_rcbuf_t *buf = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_wrap(NULL, NULL, 0, NULL, NULL, &buf) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);

        MOQ_TEST_CHECK(moq_rcbuf_wrap(&g_counting, NULL, 0, NULL, NULL, NULL) ==
            MOQ_ERR_INVAL);

        buf = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_wrap(&g_counting, NULL, 10, NULL, NULL, &buf) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);
    }

    /* -- Wrapped: alloc failure clears *out --------------------------- */
    {
        moq_rcbuf_t *buf = (moq_rcbuf_t *)0xDEAD;
        moq_result_t rc = moq_rcbuf_wrap(&g_failing,
            (const uint8_t *)"x", 1, NULL, NULL, &buf);
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(buf == NULL);
    }

    /* -- Create: invalid args clear *out ------------------------------ */
    {
        moq_rcbuf_t *buf = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_create(NULL, NULL, 0, &buf) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);

        buf = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_create(&g_counting, NULL, 10, &buf) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(buf == NULL);
    }

    /* -- Slice: data pointer = parent data + offset -------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *parent = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"abcdefghij", 10, &parent);

        moq_rcbuf_t *slice = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, parent, 3, 4, &slice) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_data(slice) == moq_rcbuf_data(parent) + 3);
        MOQ_TEST_CHECK(moq_rcbuf_len(slice) == 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(slice), "defg", 4) == 0);

        moq_rcbuf_decref(slice);
        moq_rcbuf_decref(parent);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Slice: parent release after all slices decreffed -------------- */
    {
        g_alloc_balance = 0;
        g_release_count = 0;
        static const uint8_t data[] = "wrapped_parent";
        moq_rcbuf_t *parent = NULL;
        moq_rcbuf_wrap(&g_counting, data, 14, test_release_fn,
            &g_release_count, &parent);

        moq_rcbuf_t *s1 = NULL, *s2 = NULL;
        moq_rcbuf_slice(&g_counting, parent, 0, 7, &s1);
        moq_rcbuf_slice(&g_counting, parent, 7, 7, &s2);

        moq_rcbuf_decref(parent);
        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_decref(s1);
        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_decref(s2);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Slice of slice: flattened to root ----------------------------- */
    {
        g_alloc_balance = 0;
        g_release_count = 0;
        static const uint8_t data[] = "0123456789abcdefghij";
        moq_rcbuf_t *root = NULL;
        moq_rcbuf_wrap(&g_counting, data, 20, test_release_fn,
            &g_release_count, &root);

        moq_rcbuf_t *mid = NULL;
        moq_rcbuf_slice(&g_counting, root, 10, 10, &mid);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(mid), "abcdefghij", 10) == 0);

        moq_rcbuf_t *child = NULL;
        moq_rcbuf_slice(&g_counting, mid, 3, 5, &child);
        MOQ_TEST_CHECK(moq_rcbuf_data(child) == data + 13);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(child), "defgh", 5) == 0);

        moq_rcbuf_decref(mid);
        MOQ_TEST_CHECK(moq_rcbuf_data(child) == data + 13);
        MOQ_TEST_CHECK(g_release_count == 0);

        moq_rcbuf_decref(root);
        MOQ_TEST_CHECK(g_release_count == 0);

        moq_rcbuf_decref(child);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Slice: zero-length ------------------------------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *parent = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"abc", 3, &parent);

        moq_rcbuf_t *slice = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, parent, 3, 0, &slice) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(slice) == 0);

        moq_rcbuf_decref(slice);
        moq_rcbuf_decref(parent);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Slice: invalid args ------------------------------------------ */
    {
        moq_rcbuf_t *parent = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"abc", 3, &parent);

        moq_rcbuf_t *slice = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, parent, 2, 2, &slice) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(slice == NULL);

        slice = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, parent, 4, 0, &slice) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(slice == NULL);

        MOQ_TEST_CHECK(moq_rcbuf_slice(NULL, parent, 0, 1, &slice) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, NULL, 0, 1, &slice) ==
            MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, parent, 0, 1, NULL) ==
            MOQ_ERR_INVAL);

        moq_rcbuf_decref(parent);
    }

    /* -- Slice: alloc failure ----------------------------------------- */
    {
        moq_rcbuf_t *parent = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"abc", 3, &parent);

        moq_rcbuf_t *slice = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_failing, parent, 0, 1, &slice) ==
            MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(slice == NULL);

        moq_rcbuf_decref(parent);
    }

    /* -- Slice: zero-length wrapped root with data=NULL ---------------- */
    {
        g_alloc_balance = 0;
        g_release_count = 0;
        moq_rcbuf_t *root = NULL;
        moq_rcbuf_wrap(&g_counting, NULL, 0, test_release_fn,
            &g_release_count, &root);

        moq_rcbuf_t *slice = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g_counting, root, 0, 0, &slice) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(slice) == 0);

        moq_rcbuf_decref(root);
        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_decref(slice);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* ============================================================== */
    /* moq_rcbuf_clone — shard-boundary independent copy               */
    /* ============================================================== */

    /* -- Clone: invalid args clear *out where provided ---------------- */
    {
        moq_rcbuf_t *src = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"x", 1, &src);

        /* out == NULL -> INVAL (no deref). */
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, src, NULL) == MOQ_ERR_INVAL);

        /* dst_alloc == NULL -> INVAL, *out nulled. */
        moq_rcbuf_t *out = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_clone(NULL, src, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);

        /* allocator with NULL alloc -> INVAL, *out nulled. */
        moq_alloc_t no_alloc = { NULL, NULL, counting_realloc, counting_free };
        out = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&no_alloc, src, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);

        /* allocator with NULL free -> INVAL, *out nulled. */
        moq_alloc_t no_free = { NULL, counting_alloc, counting_realloc, NULL };
        out = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&no_free, src, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);

        /* src == NULL -> INVAL, *out nulled. */
        out = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, NULL, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);

        moq_rcbuf_decref(src);
    }

    /* -- Clone: destination allocation failure -> NOMEM, *out NULL ---- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *src = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"data", 4, &src);

        /* Valid src, but the destination allocator fails. */
        moq_rcbuf_t *out = (moq_rcbuf_t *)0xDEAD;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_failing, src, &out) == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(out == NULL);

        moq_rcbuf_decref(src);
        MOQ_TEST_CHECK(g_alloc_balance == 0);  /* src allocator untouched */
    }

    /* -- Clone: zero-length source -> valid zero-length clone --------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *src = NULL;
        moq_rcbuf_create(&g_counting, NULL, 0, &src);

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, src, &clone) == MOQ_OK);
        MOQ_TEST_CHECK(clone != NULL);
        MOQ_TEST_CHECK(moq_rcbuf_len(clone) == 0);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(clone) == 1);

        moq_rcbuf_decref(clone);
        moq_rcbuf_decref(src);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Clone: plain independence (src freed first) ------------------ */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *src = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"hello", 5, &src);

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, src, &clone) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(clone) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), "hello", 5) == 0);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(clone) == 1);
        /* Distinct storage, not an aliased pointer. */
        MOQ_TEST_CHECK(moq_rcbuf_data(clone) != moq_rcbuf_data(src));

        /* Drop src first: clone is fully independent and stays valid. */
        moq_rcbuf_decref(src);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(clone) == 1);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), "hello", 5) == 0);

        moq_rcbuf_decref(clone);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Clone: independence (clone freed first) ---------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *src = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"world", 5, &src);

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, src, &clone) == MOQ_OK);

        /* Drop clone first: src stays valid. */
        moq_rcbuf_decref(clone);
        MOQ_TEST_CHECK(moq_rcbuf_refcount(src) == 1);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(src), "world", 5) == 0);

        moq_rcbuf_decref(src);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Clone: destination-allocator accounting ---------------------- */
    {
        g_alloc_balance = 0;   /* source allocator (g_counting) */
        g_alloc_balance2 = 0;  /* destination allocator (g_counting2) */

        moq_rcbuf_t *src = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"payload", 7, &src);
        MOQ_TEST_CHECK(g_alloc_balance == 1);
        MOQ_TEST_CHECK(g_alloc_balance2 == 0);

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting2, src, &clone) == MOQ_OK);
        /* Clone's storage came from dst (g_counting2), not src's allocator. */
        MOQ_TEST_CHECK(g_alloc_balance == 1);
        MOQ_TEST_CHECK(g_alloc_balance2 == 1);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), "payload", 7) == 0);

        /* Clone frees via dst allocator only. */
        moq_rcbuf_decref(clone);
        MOQ_TEST_CHECK(g_alloc_balance == 1);
        MOQ_TEST_CHECK(g_alloc_balance2 == 0);

        /* Source frees via its own allocator only. */
        moq_rcbuf_decref(src);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
        MOQ_TEST_CHECK(g_alloc_balance2 == 0);
    }

    /* -- Clone: slice copies only the visible range ------------------- */
    {
        g_alloc_balance = 0;
        moq_rcbuf_t *root = NULL;
        moq_rcbuf_create(&g_counting, (const uint8_t *)"abcdefghij", 10, &root);

        moq_rcbuf_t *slice = NULL;
        moq_rcbuf_slice(&g_counting, root, 3, 4, &slice);  /* "defg" */

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, slice, &clone) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(clone) == 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), "defg", 4) == 0);
        /* Standalone copy, not pointing into the root storage. */
        MOQ_TEST_CHECK(moq_rcbuf_data(clone) != moq_rcbuf_data(slice));

        /* Root + slice released independently; clone survives. */
        moq_rcbuf_decref(slice);
        moq_rcbuf_decref(root);
        MOQ_TEST_CHECK(moq_rcbuf_len(clone) == 4);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), "defg", 4) == 0);

        moq_rcbuf_decref(clone);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Clone: wrapped source does NOT inherit release callback ------ */
    {
        g_alloc_balance = 0;
        g_release_count = 0;
        static const uint8_t payload[] = "wrapped-bytes";

        moq_rcbuf_t *src = NULL;
        moq_rcbuf_wrap(&g_counting, payload, sizeof(payload) - 1,
                       test_release_fn, &g_release_count, &src);

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, src, &clone) == MOQ_OK);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), payload,
                              sizeof(payload) - 1) == 0);
        /* Clone is plain owned storage, not the wrapped pointer. */
        MOQ_TEST_CHECK(moq_rcbuf_data(clone) != payload);

        /* Decref clone: wrapped release must NOT fire (not inherited). */
        moq_rcbuf_decref(clone);
        MOQ_TEST_CHECK(g_release_count == 0);

        /* Decref original wrapped: release fires exactly once. */
        moq_rcbuf_decref(src);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    /* -- Clone: wrap x slice x clone ---------------------------------- */
    {
        g_alloc_balance = 0;
        g_release_count = 0;
        static const uint8_t data[] = "0123456789abcdefghij";

        moq_rcbuf_t *root = NULL;
        moq_rcbuf_wrap(&g_counting, data, 20, test_release_fn,
                       &g_release_count, &root);

        moq_rcbuf_t *slice = NULL;
        moq_rcbuf_slice(&g_counting, root, 10, 5, &slice);  /* "abcde" */
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(slice), "abcde", 5) == 0);

        moq_rcbuf_t *clone = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_clone(&g_counting, slice, &clone) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_len(clone) == 5);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(clone), "abcde", 5) == 0);
        MOQ_TEST_CHECK(moq_rcbuf_data(clone) != data + 10);

        /* Releasing the clone must NOT fire the wrapped callback. */
        moq_rcbuf_decref(clone);
        MOQ_TEST_CHECK(g_release_count == 0);

        /* Releasing slice + root (the wrapped owner) fires it exactly once. */
        moq_rcbuf_decref(slice);
        MOQ_TEST_CHECK(g_release_count == 0);
        moq_rcbuf_decref(root);
        MOQ_TEST_CHECK(g_release_count == 1);
        MOQ_TEST_CHECK(g_alloc_balance == 0);
    }

    MOQ_TEST_PASS("test_rcbuf");
    return failures;
}
