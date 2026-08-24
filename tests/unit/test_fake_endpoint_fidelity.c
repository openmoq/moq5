/*
 * Fake-endpoint fidelity: recorded operations and payload bytes are both
 * bounded, and every place a bound is reached must be observable.
 *
 * The recorder drops operations past its capacity and clamps payloads to the
 * per-record byte limit. Silently, those two losses are indistinguishable
 * from an endpoint that was never called and from a peer that sent a shorter
 * payload -- so a test asserting "exactly these operations" or "these exact
 * bytes" can pass on evidence that was thrown away. Each loss therefore needs
 * a sticky observation: once set it stays set, so a later successful
 * operation cannot mask an earlier loss, and clearing the recorded operations
 * must not erase it.
 *
 * Both observations are required, not optional: when the substrate does not
 * offer them this file reports a missing observation and fails.
 */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "../support/fake_endpoint.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/*
 * Read the two sticky loss observations. A substrate that does not offer
 * them answers "nothing was lost" for every question, which is precisely
 * what the assertions below reject -- so a missing capability is reported
 * as a failing observation rather than skipped.
 */
#ifdef FAKE_EP_HAS_LOSS_FLAGS
#define LOSS_FLAGS_AVAILABLE 1
static bool ep_overflowed(const fake_endpoint_t *ep) { return ep->overflowed; }
static bool ep_truncated(const fake_endpoint_t *ep) { return ep->truncated; }
#else
#define LOSS_FLAGS_AVAILABLE 0
static bool ep_overflowed(const fake_endpoint_t *ep) { (void)ep; return false; }
static bool ep_truncated(const fake_endpoint_t *ep) { (void)ep; return false; }
#endif

enum { PAYLOAD_CAP = sizeof(((fake_op_t *)0)->data) };

static fake_endpoint_t g_ep;
static uint8_t g_big[PAYLOAD_CAP + 64];

static void ep_reset(void)
{
    fake_endpoint_init(&g_ep, 2, 0);
    fake_endpoint_enable_abort(&g_ep);
}

/* Fill the operation recorder to exactly its capacity with a cheap op. */
static void fill_ops(void)
{
    uint64_t id = 0;


    while (g_ep.count < FAKE_EP_MAX_OPS)
        g_ep.vtable.open_uni(&g_ep, &id);
    CHECK(g_ep.count == FAKE_EP_MAX_OPS);
}

/*
 * Every operation-record path that can lose a record must say so. One case
 * per path, each starting from a full recorder.
 */
static void t_every_op_path_reports_overflow(void)
{
    int before = failures;
    uint64_t id = 0;


    CHECK(LOSS_FLAGS_AVAILABLE); /* the missing observation, when absent */
    /* open_uni */
    ep_reset(); fill_ops();
    CHECK(!ep_overflowed(&g_ep));
    g_ep.vtable.open_uni(&g_ep, &id);
    CHECK(ep_overflowed(&g_ep));

    /* open_bidi */
    ep_reset(); fill_ops();
    g_ep.vtable.open_bidi(&g_ep, &id);
    CHECK(ep_overflowed(&g_ep));

    /* write */
    ep_reset(); fill_ops();
    g_ep.vtable.write(&g_ep, 0, g_big, 4, false);
    CHECK(ep_overflowed(&g_ep));

    /* reset_stream */
    ep_reset(); fill_ops();
    g_ep.vtable.reset_stream(&g_ep, 0, 7);
    CHECK(ep_overflowed(&g_ep));

    /* stop_sending */
    ep_reset(); fill_ops();
    g_ep.vtable.stop_sending(&g_ep, 0, 7);
    CHECK(ep_overflowed(&g_ep));

    /* send_datagram */
    ep_reset(); fill_ops();
    g_ep.vtable.send_datagram(&g_ep, g_big, 4);
    CHECK(ep_overflowed(&g_ep));

    /* close_transport */
    ep_reset(); fill_ops();
    g_ep.vtable.close_transport(&g_ep, 7, NULL, 0);
    CHECK(ep_overflowed(&g_ep));

    /* the optional native abort */
    ep_reset(); fill_ops();
    CHECK(g_ep.vtable.abort_stream != NULL);
    if (g_ep.vtable.abort_stream != NULL)
        g_ep.vtable.abort_stream(&g_ep, 0, 7);
    CHECK(ep_overflowed(&g_ep));

    if (failures == before)
        printf("PASS: every_op_path_reports_overflow\n");
}

/*
 * Each byte-bearing path is discriminated independently: an oversized write
 * must not be reported through the datagram or close-reason paths, and vice
 * versa.
 */
static void t_each_byte_path_reports_truncation(void)
{
    int before = failures;

    CHECK(LOSS_FLAGS_AVAILABLE);
    /* write: at the limit is not truncation, one byte past it is */
    ep_reset();
    g_ep.vtable.write(&g_ep, 0, g_big, PAYLOAD_CAP, false);
    CHECK(g_ep.count == 1);
    CHECK(g_ep.ops[0].data_len == PAYLOAD_CAP);
    CHECK(!ep_truncated(&g_ep));
    g_ep.vtable.write(&g_ep, 0, g_big, PAYLOAD_CAP + 1, false);
    CHECK(ep_truncated(&g_ep));
    CHECK(!ep_overflowed(&g_ep)); /* the two losses are distinct */

    /* datagram, on its own recorder */
    ep_reset();
    g_ep.vtable.send_datagram(&g_ep, g_big, PAYLOAD_CAP);
    CHECK(!ep_truncated(&g_ep));
    g_ep.vtable.send_datagram(&g_ep, g_big, PAYLOAD_CAP + 1);
    CHECK(ep_truncated(&g_ep));

    /* close reason, on its own recorder */
    ep_reset();
    g_ep.vtable.close_transport(&g_ep, 7, g_big, PAYLOAD_CAP);
    CHECK(!ep_truncated(&g_ep));
    ep_reset();
    g_ep.vtable.close_transport(&g_ep, 7, g_big, PAYLOAD_CAP + 1);
    CHECK(ep_truncated(&g_ep));

    if (failures == before)
        printf("PASS: each_byte_path_reports_truncation\n");
}

/*
 * The evidence outlives the operations it describes: clearing the recorded
 * operations is a fixture convenience and must not erase what was lost.
 * Initialization may, since it starts a new endpoint.
 */
static void t_loss_evidence_is_sticky(void)
{
    int before = failures;
    uint64_t id = 0;


    CHECK(LOSS_FLAGS_AVAILABLE);
    ep_reset();
    /* truncate first: once the recorder is full the write is dropped whole
     * and never reaches the payload clamp at all */
    g_ep.vtable.write(&g_ep, 0, g_big, PAYLOAD_CAP + 1, false);
    fill_ops();
    g_ep.vtable.open_uni(&g_ep, &id);
    CHECK(ep_overflowed(&g_ep));
    CHECK(ep_truncated(&g_ep));

    fake_endpoint_clear_ops(&g_ep);
    CHECK(g_ep.count == 0);
    CHECK(ep_overflowed(&g_ep)); /* survives the clear */
    CHECK(ep_truncated(&g_ep));

    /* a later well-sized operation cannot mask the earlier loss */
    g_ep.vtable.write(&g_ep, 0, g_big, 4, false);
    CHECK(ep_overflowed(&g_ep));
    CHECK(ep_truncated(&g_ep));

    /* but a fresh endpoint starts clean */
    ep_reset();
    CHECK(!ep_overflowed(&g_ep));
    CHECK(!ep_truncated(&g_ep));

    if (failures == before)
        printf("PASS: loss_evidence_is_sticky\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    memset(g_big, 0xA5, sizeof(g_big));
    t_every_op_path_reports_overflow();
    t_each_byte_path_reports_truncation();
    t_loss_evidence_is_sticky();
    if (failures == 0)
        printf("PASS: fake_endpoint_fidelity\n");
    else
        fprintf(stderr, "FAIL: fake_endpoint_fidelity (%d)\n", failures);
    return failures != 0;
}
