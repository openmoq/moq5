#include "h3zero_common.h"

#include <stdio.h>
#include <string.h>

/* Shrinking, growing, equal-sized and empty payloads reuse one accumulator. */
static const uint8_t wire[] = {
    0, 5, 1, 2, 3, 4, 5,
    1, 1, 9,
    2, 8, 10, 11, 12, 13, 14, 15, 16, 17,
    3, 8, 20, 21, 22, 23, 24, 25, 26, 27,
    4, 0,
    5, 2, 30, 31
};
static const struct {
    size_t offset;
    size_t length;
} expected[] = {
    { 2, 5 }, { 9, 1 }, { 12, 8 }, { 22, 8 }, { 32, 0 }, { 34, 2 }
};

static int check_sequence(size_t chunk)
{
    h3zero_capsule_t capsule;
    const uint8_t *p = wire;
    const uint8_t *end = wire + sizeof(wire);
    size_t seen = 0;
    int result = 1;
    memset(&capsule, 0, sizeof(capsule));

    while (p < end) {
        const size_t remaining = (size_t)(end - p);
        const uint8_t *limit = p + (chunk < remaining ? chunk : remaining);
        while (p < limit) {
            const uint8_t *next = h3zero_accumulate_capsule(p, limit, &capsule);
            if (next == NULL || next <= p || next > limit) {
                fprintf(stderr, "chunk %zu: invalid parser progress\n", chunk);
                goto done;
            }
            p = next;
            if (capsule.is_stored) {
                if (seen >= sizeof(expected) / sizeof(expected[0]) ||
                    capsule.capsule_type != seen ||
                    capsule.capsule_length != expected[seen].length ||
                    capsule.value_read != expected[seen].length ||
                    (expected[seen].length != 0 &&
                     memcmp(capsule.capsule_buffer, wire + expected[seen].offset,
                            expected[seen].length) != 0)) {
                    fprintf(stderr, "chunk %zu: capsule %zu has wrong type/length/payload\n",
                            chunk, seen);
                    goto done;
                }
                ++seen;
            }
        }
    }
    if (seen != sizeof(expected) / sizeof(expected[0])) {
        fprintf(stderr, "chunk %zu: only %zu capsules completed\n", chunk, seen);
        goto done;
    }
    result = 0;
done:
    h3zero_release_capsule(&capsule);
    return result;
}

int main(void)
{
    /* Start with the coalesced input that exposed the heap overflow. */
    if (check_sequence(sizeof(wire)) != 0)
        return 1;
    for (size_t chunk = 1; chunk < sizeof(wire); ++chunk) {
        if (check_sequence(chunk) != 0)
            return 1;
    }
    printf("capsule sequence: %zu chunk sizes passed\n", sizeof(wire));
    return 0;
}
