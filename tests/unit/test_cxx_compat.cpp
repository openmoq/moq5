/*
 * C++17 compatibility smoke test.
 * Verifies that public headers compile and key macros work from C++.
 */

#include <moq/moq.h>
#include "test_support.h"
#include <cstring>

int main()
{
    int failures = 0;

    /* MOQ_BYTES_LITERAL works from C++. */
    moq_bytes_t lit = MOQ_BYTES_LITERAL("hello");
    MOQ_TEST_CHECK(lit.len == 5);
    MOQ_TEST_CHECK(std::memcmp(lit.data, "hello", 5) == 0);

    /* Handle sentinels work from C++. */
    moq_subscription_t inv = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK(!moq_subscription_is_valid(inv));

    moq_publication_t pinv = MOQ_PUBLICATION_INVALID;
    MOQ_TEST_CHECK(!moq_publication_is_valid(pinv));

    /* Handle equality from C++. */
    moq_subscription_t a = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t b = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK(moq_subscription_eq(a, b));

    /* moq_stream_id_from_u64 from C++. */
    moq_stream_id_t sid = moq_stream_id_from_u64(42);
    MOQ_TEST_CHECK(sid._v == 42);

    /* Result code conventions. */
    MOQ_TEST_CHECK(MOQ_OK == 0);
    MOQ_TEST_CHECK(MOQ_DONE > 0);
    MOQ_TEST_CHECK(MOQ_ERR_NOMEM < 0);

    /* moq_bytes_cstr from C++. */
    moq_bytes_t cstr = moq_bytes_cstr("world");
    MOQ_TEST_CHECK(cstr.len == 5);

    /* Session config initializer works from C++. */
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    MOQ_TEST_CHECK(cfg.struct_size == sizeof(moq_session_cfg_t));
    MOQ_TEST_CHECK(cfg.alloc == nullptr);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.max_actions = 1;
    MOQ_TEST_CHECK(cfg.perspective == MOQ_PERSPECTIVE_CLIENT);
    MOQ_TEST_CHECK(cfg.max_actions == 1);

    MOQ_TEST_PASS("test_cxx_compat");
    return failures;
}
