/*
 * Unit test for examples/picoquic/publisher_conn_guard.h - the connection-
 * isolation classifier for the singleton picoquic publisher (security report
 * finding #9, MOQ-EXAMPLE-PICOQUIC-CONNECTION-CONFUSION). Pure C, no picoquic
 * and no network: it exercises the decision the callback delegates to, so the
 * "a second connection cannot reach the first adapter path" property is proven
 * deterministically.
 */
#include "../publisher_conn_guard.h"

#include <stdio.h>

static int failures = 0;
#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
                   failures++; } \
} while (0)

int main(void)
{
    /* Two distinct, non-NULL connection identities. */
    int owner_obj = 0, other_obj = 0;
    const void *owner = &owner_obj;
    const void *other = &other_obj;

    /* Before an owner exists. */
    CHECK(pq_pub_classify(false, true,  other, NULL) == PQ_PUB_CREATE,
          "no owner + ready => CREATE (adopt the first connection)");
    CHECK(pq_pub_classify(false, false, other, NULL) == PQ_PUB_IGNORE,
          "no owner + non-ready => IGNORE (pre-owner handshake, not closed)");

    /* Owner exists: the OWNER connection. */
    CHECK(pq_pub_classify(true, true,  owner, owner) == PQ_PUB_OWNER_READY,
          "owner + repeated ready => OWNER_READY (benign, NOT closed)");
    CHECK(pq_pub_classify(true, false, owner, owner) == PQ_PUB_DISPATCH,
          "owner + non-ready => DISPATCH to the adapter");

    /* Owner exists: a DIFFERENT connection must never reach the adapter. */
    CHECK(pq_pub_classify(true, true,  other, owner) == PQ_PUB_CLOSE_BUSY,
          "different connection + ready => CLOSE_BUSY (refused, not adopted)");
    CHECK(pq_pub_classify(true, false, other, owner) == PQ_PUB_CLOSE_BUSY,
          "different connection + non-ready => CLOSE_BUSY (NOT dispatched)");

    /* The security-critical fact stated directly: a second connection's
     * stream/close/application-close callback is never PQ_PUB_DISPATCH. */
    CHECK(pq_pub_classify(true, false, other, owner) != PQ_PUB_DISPATCH,
          "a non-owner callback is never dispatched to the first adapter");

    if (failures) { fprintf(stderr, "%d check(s) failed\n", failures); return 1; }
    printf("ok\n");
    return 0;
}
