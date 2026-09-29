/*
 * The parser's internal overflow-safe array sizer, at its boundary.
 *
 * moq_msf_checked_array_bytes is not part of the public API: the shipped
 * moq-msf library hides it, and a real JSON input cannot reach a
 * SIZE_MAX-overflowing array on a 64-bit host (the JSON DOM itself would need
 * that many elements). So this target compiles src/msf.c directly, as
 * test_msf_scaling does, and exercises the real helper: just over the limit is
 * refused, exactly the limit and small counts compute the product, and a zero
 * element size never overflows. The public parser and apply_delta overflow
 * checks stay in test_msf, against the shipped library.
 */
#include <moq/msf.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

extern bool moq_msf_checked_array_bytes(size_t count, size_t elem,
                                        size_t *out_bytes);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

int main(void)
{
    {
        const size_t elem = sizeof(moq_msf_track_t);
        size_t bytes = 12345;

        /* Just over the representable limit -> overflow rejected. */
        CHECK(!moq_msf_checked_array_bytes(SIZE_MAX / elem + 1, elem, &bytes));

        /* Exactly the limit -> representable, computes the product. */
        size_t boundary = SIZE_MAX / elem;
        bytes = 0;
        CHECK(moq_msf_checked_array_bytes(boundary, elem, &bytes));
        CHECK(bytes == boundary * elem);

        /* Small, normal counts compute as expected. */
        bytes = 0;
        CHECK(moq_msf_checked_array_bytes(4, elem, &bytes));
        CHECK(bytes == 4 * elem);

        /* elem == 0 never overflows (and yields 0 bytes). */
        bytes = 1;
        CHECK(moq_msf_checked_array_bytes(SIZE_MAX, 0, &bytes));
        CHECK(bytes == 0);
    }

    if (failures == 0) printf("PASS: msf_checked_array\n");
    return failures ? 1 : 0;
}
