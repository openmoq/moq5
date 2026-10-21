/*
 * pq_recv_share: a lane divides its per-turn receive budget fairly among the
 * sockets ready that turn, so sustained traffic on one address family cannot
 * starve the other. Every ready socket gets a positive share, and the shares
 * together never exceed the budget.
 */

#include "../pq_recv_budget.h"

#include <stdio.h>

static int failures = 0;
#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            failures++;                                                 \
        }                                                               \
    } while (0)

int main(void)
{
    /* one ready family (or none) takes the whole budget */
    CHECK(pq_recv_share(0, 32) == 32);
    CHECK(pq_recv_share(1, 32) == 32);

    /* two ready families split it: the second always has a positive share
     * even when the first is saturated, and the two fit the budget */
    CHECK(pq_recv_share(2, 32) == 16);
    CHECK(pq_recv_share(2, 32) > 0);
    CHECK(2 * pq_recv_share(2, 32) <= 32);

    CHECK(pq_recv_share(3, 32) == 10);
    CHECK(3 * pq_recv_share(3, 32) <= 32);

    /* more ready sockets than the budget: each still gets at least one */
    CHECK(pq_recv_share(64, 32) == 1);
    CHECK(pq_recv_share(2, 1) == 1);

    /* the fairness invariant across a range: every ready socket positive and
     * the shares within the budget (the one-each floor is the only overflow) */
    for (int budget = 1; budget <= 64; budget++) {
        for (int ready = 1; ready <= 8; ready++) {
            int s = pq_recv_share(ready, budget);
            CHECK(s >= 1);
            CHECK(s <= budget);
            CHECK((long)s * ready <= budget || s == 1);
        }
    }

    if (failures) {
        printf("FAILED: %d check(s) [pq_recv_budget]\n", failures);
        return 1;
    }
    printf("PASS: pq_recv_budget\n");
    return 0;
}
