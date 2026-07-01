#include <moq/moq.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* MOQ_BYTES_LITERAL on a string literal. */
    moq_bytes_t lit = MOQ_BYTES_LITERAL("hello");
    MOQ_TEST_CHECK(lit.len == 5);
    MOQ_TEST_CHECK(memcmp(lit.data, "hello", 5) == 0);

    /* MOQ_BYTES_LITERAL includes no NUL. */
    moq_bytes_t empty_lit = MOQ_BYTES_LITERAL("");
    MOQ_TEST_CHECK(empty_lit.len == 0);

    /* moq_bytes_cstr on a regular string. */
    moq_bytes_t cstr = moq_bytes_cstr("world");
    MOQ_TEST_CHECK(cstr.len == 5);
    MOQ_TEST_CHECK(memcmp(cstr.data, "world", 5) == 0);

    /* moq_bytes_cstr on NULL. */
    moq_bytes_t null_cstr = moq_bytes_cstr(NULL);
    MOQ_TEST_CHECK(null_cstr.data == NULL);
    MOQ_TEST_CHECK(null_cstr.len == 0);

    /* Namespace construction from parts. */
    moq_bytes_t parts[] = {
        MOQ_BYTES_LITERAL("chat.example.com"),
        MOQ_BYTES_LITERAL("room42"),
    };
    moq_namespace_t ns = { .parts = parts, .count = 2 };
    MOQ_TEST_CHECK(ns.count == 2);
    MOQ_TEST_CHECK(ns.parts[0].len == 16);
    MOQ_TEST_CHECK(ns.parts[1].len == 6);

    MOQ_TEST_PASS("test_bytes");
    return failures;
}
