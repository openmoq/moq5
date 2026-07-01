#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

int main(void)
{
    int failures = 0;

    /* == Register, lookup, delete ===================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 1024, 8) == MOQ_OK);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 42,
            (const uint8_t *)"abc", 3) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cache.used_bytes == 16 + 3);

        uint64_t type; const uint8_t *val; size_t val_len;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&cache, 1,
            &type, &val, &val_len) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(type == 42);
        MOQ_TEST_CHECK(val_len == 3);
        MOQ_TEST_CHECK(memcmp(val, "abc", 3) == 0);

        MOQ_TEST_CHECK(moq_token_cache_delete(&cache, 1) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cache.used_bytes == 0);
        MOQ_TEST_CHECK(moq_token_cache_lookup(&cache, 1,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Duplicate register detected ================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 8);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            NULL, 0) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            NULL, 0) == MOQ_TOKEN_ERR_DUPLICATE);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Re-register after delete ===================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 8);

        moq_token_cache_register(&cache, 5, 1, (const uint8_t *)"x", 1);
        moq_token_cache_delete(&cache, 5);
        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 5, 2,
            (const uint8_t *)"yy", 2) == MOQ_TOKEN_OK);

        uint64_t type;
        moq_token_cache_lookup(&cache, 5, &type, NULL, NULL);
        MOQ_TEST_CHECK(type == 2);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Unknown lookup and delete ==================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 8);

        MOQ_TEST_CHECK(moq_token_cache_lookup(&cache, 99,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);
        MOQ_TEST_CHECK(moq_token_cache_delete(&cache, 99) == MOQ_TOKEN_ERR_UNKNOWN);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Overflow at exact boundary =================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 16 + 4, 8);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            (const uint8_t *)"abcd", 4) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cache.used_bytes == 20);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 2, 0,
            NULL, 0) == MOQ_TOKEN_ERR_OVERFLOW);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Overflow at boundary + 1 ===================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 16 + 3, 8);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            (const uint8_t *)"abcd", 4) == MOQ_TOKEN_ERR_OVERFLOW);
        MOQ_TEST_CHECK(cache.used_bytes == 0);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == DELETE decrements used bytes ================================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 8);

        moq_token_cache_register(&cache, 1, 0,
            (const uint8_t *)"12345", 5);
        MOQ_TEST_CHECK(cache.used_bytes == 21);

        moq_token_cache_register(&cache, 2, 0,
            (const uint8_t *)"ab", 2);
        MOQ_TEST_CHECK(cache.used_bytes == 21 + 18);

        moq_token_cache_delete(&cache, 1);
        MOQ_TEST_CHECK(cache.used_bytes == 18);

        moq_token_cache_delete(&cache, 2);
        MOQ_TEST_CHECK(cache.used_bytes == 0);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Zero-length value accepted =================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 8);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 0, 7,
            NULL, 0) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cache.used_bytes == 16);

        uint64_t type; const uint8_t *val; size_t vlen;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&cache, 0,
            &type, &val, &vlen) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(type == 7);
        MOQ_TEST_CHECK(vlen == 0);
        MOQ_TEST_CHECK(val == NULL);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Slot exhaustion ============================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 2);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 0, 0,
            NULL, 0) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            NULL, 0) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 2, 0,
            NULL, 0) == MOQ_TOKEN_ERR_OVERFLOW);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == OOM on token value allocation ================================ */
    {
        fail_alloc_state_t fas = { .fail_at = 2 };
        moq_alloc_t alloc = fail_allocator(&fas);
        moq_token_cache_t cache;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 1024, 8) == MOQ_OK);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            (const uint8_t *)"data", 4) == MOQ_TOKEN_ERR_NOMEM);
        MOQ_TEST_CHECK(cache.used_bytes == 0);
        MOQ_TEST_CHECK(moq_token_cache_lookup(&cache, 1,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* == Free releases all values ===================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        moq_token_cache_init(&cache, &alloc, 1024, 8);

        moq_token_cache_register(&cache, 0, 0,
            (const uint8_t *)"aaa", 3);
        moq_token_cache_register(&cache, 1, 0,
            (const uint8_t *)"bbb", 3);
        moq_token_cache_register(&cache, 2, 0,
            (const uint8_t *)"ccc", 3);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 65 zero-length aliases with budget = 16*65 =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 16 * 65, 0) == MOQ_OK);

        for (uint64_t i = 0; i < 65; i++)
            MOQ_TEST_CHECK(moq_token_cache_register(&cache, i, 0,
                NULL, 0) == MOQ_TOKEN_OK);

        MOQ_TEST_CHECK(cache.used_bytes == 16 * 65);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 65, 0,
            NULL, 0) == MOQ_TOKEN_ERR_OVERFLOW);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Exact byte boundary: 16+4=20, budget=20 ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 20, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 0, 0,
            (const uint8_t *)"abcd", 4) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cache.used_bytes == 20);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 1, 0,
            NULL, 0) == MOQ_TOKEN_ERR_OVERFLOW);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Byte budget + 1 fails ======================================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 19, 0) == MOQ_OK);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 0, 0,
            (const uint8_t *)"abcd", 4) == MOQ_TOKEN_ERR_OVERFLOW);
        MOQ_TEST_CHECK(cache.used_bytes == 0);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Large max_bytes does not cause overflow ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        size_t large = 1024 * 1024;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, large, 0) == MOQ_OK);
        MOQ_TEST_CHECK(cache.cap <= 4096);

        MOQ_TEST_CHECK(moq_token_cache_register(&cache, 0, 0,
            (const uint8_t *)"x", 1) == MOQ_TOKEN_OK);

        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Entry-count overflow is rejected, cache stays safe/empty ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_token_cache_t cache;
        /* An entry count whose byte size would wrap size_t. */
        size_t huge = SIZE_MAX / sizeof(moq_token_cache_entry_t) + 1;
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 1024, huge)
                       == MOQ_ERR_INVAL);
        /* No allocation happened; cache is empty and free is a safe no-op. */
        MOQ_TEST_CHECK(cache.entries == NULL);
        MOQ_TEST_CHECK(cache.cap == 0);
        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);

        /* Large-but-valid boundary still initializes. */
        MOQ_TEST_CHECK(moq_token_cache_init(&cache, &alloc, 1024, 4096)
                       == MOQ_OK);
        MOQ_TEST_CHECK(cache.cap == 4096);
        moq_token_cache_free(&cache);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    MOQ_TEST_PASS("test_auth_cache");
    return failures;
}
