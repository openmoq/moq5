#ifndef MOQ_TEST_HELD_BRIDGE_DRIVER_H
#define MOQ_TEST_HELD_BRIDGE_DRIVER_H

#include "fake_endpoint.h"
#include "held_input.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>

/* Explicit test transport, not a capability change to the common fake.
 * Slots are per translation unit and bounded independently of product memory.
 * Each queued call preserves its bytes/FIN separately from later deliveries.
 * Deliberate direct bridge-result tests should keep calling the raw API. */
typedef struct {
    moq_transport_bridge_t *bridge;
    test_held_input_t input;
} held_bridge_driver_t;
static held_bridge_driver_t held_bridge_drivers[16];

#define HELD_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: required %s\n", __FILE__, __LINE__, #expr); \
        exit(EXIT_FAILURE); \
    } \
} while (0)
#define HELD_REQUIRE_EQ_INT(actual, expected) HELD_REQUIRE((actual) == (expected))

static inline void held_endpoint_init(fake_endpoint_t *ep, uint64_t uni, uint64_t bidi)
{
    fake_endpoint_init(ep, uni, bidi);
    ep->vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
}

static inline held_bridge_driver_t *held_bridge_driver(moq_transport_bridge_t *b)
{
    HELD_REQUIRE(b != NULL);
    for (size_t i = 0; i < 16; i++)
        if (held_bridge_drivers[i].bridge == b) return &held_bridge_drivers[i];
    HELD_REQUIRE(false);
    return NULL;
}

static inline moq_result_t held_bridge_create(const moq_transport_bridge_cfg_t *cfg,
    moq_session_t *s, const moq_transport_endpoint_ops_t *ops, void *ctx,
    moq_transport_bridge_t **out)
{
    moq_result_t rc = moq_transport_bridge_create(cfg, s, ops, ctx, out);
    if (rc != MOQ_OK) return rc;
    for (size_t i = 0; i < 16; i++) {
        if (held_bridge_drivers[i].bridge) continue;
        held_bridge_drivers[i].bridge = *out;
        held_bridge_drivers[i].input.count = 0;
        return rc;
    }
    HELD_REQUIRE(false); /* Fixture resource exhaustion must not silently drop. */
    return MOQ_ERR_BUFFER;
}

static inline void held_bridge_destroy(moq_transport_bridge_t *b)
{
    if (b) {
        held_bridge_driver_t *d = held_bridge_driver(b);
        d->input.count = 0;
        d->bridge = NULL;
    }
    moq_transport_bridge_destroy(b);
}

static inline moq_result_t held_bridge_reset(moq_transport_bridge_t *b,
    uint64_t sid, uint64_t code, uint64_t now)
{
    if (b) {
        test_held_input_t *h = &held_bridge_driver(b)->input;
        for (size_t i = 0; i < h->count;) {
            if (h->chunks[i].sid != sid) { i++; continue; }
            h->count--;
            memmove(&h->chunks[i], &h->chunks[i + 1], (h->count - i) * sizeof(h->chunks[0]));
        }
    }
    return moq_transport_bridge_on_peer_stream_reset(b, sid, code, now);
}

static inline moq_result_t held_bridge_close(moq_transport_bridge_t *b,
    uint64_t code, uint64_t now)
{
    if (b) held_bridge_driver(b)->input.count = 0;
    return moq_transport_bridge_on_transport_close(b, code, now);
}

static inline moq_result_t held_bridge_error(moq_transport_bridge_t *b,
    uint64_t code, uint64_t now)
{
    if (b) held_bridge_driver(b)->input.count = 0;
    return moq_transport_bridge_on_transport_error(b, code, now);
}

static inline moq_result_t held_bridge_uni_bytes(moq_transport_bridge_t *b,
    uint64_t sid, const uint8_t *data, size_t len, bool fin, uint64_t now)
{
    if (!b || moq_transport_bridge_is_terminal(b))
        return moq_transport_bridge_on_peer_uni_bytes(b, sid, data, len, fin, now);
    moq_result_t rc = test_hold_uni(&held_bridge_driver(b)->input, b, sid, data, len, fin, now);
    HELD_REQUIRE(rc != MOQ_ERR_BUFFER);
    return rc;
}

static inline moq_result_t held_bridge_uni_rcbuf(moq_transport_bridge_t *b,
    uint64_t sid, moq_rcbuf_t *buf, bool fin, uint64_t now)
{
    if (!b || !buf || moq_transport_bridge_is_terminal(b))
        return moq_transport_bridge_on_peer_uni_rcbuf(b, sid, buf, fin, now);
    test_held_input_t *h = &held_bridge_driver(b)->input;
    for (size_t i = 0; i < h->count; i++)
        if (h->chunks[i].sid == sid)
            return held_bridge_uni_bytes(b, sid, moq_rcbuf_data(buf), moq_rcbuf_len(buf), fin, now);
    /* Exercise the real rcbuf entry point, retaining a copy only on refusal. */
    moq_result_t rc = moq_transport_bridge_on_peer_uni_rcbuf(b, sid, buf, fin, now);
    if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) return rc;
    size_t len = moq_rcbuf_len(buf);
    HELD_REQUIRE(h->count < 32 && len <= sizeof(h->chunks[0].bytes));
    size_t i = h->count++;
    h->chunks[i].sid = sid;
    h->chunks[i].len = len;
    h->chunks[i].fin = fin;
    if (len) memcpy(h->chunks[i].bytes, moq_rcbuf_data(buf), len);
    return MOQ_OK;
}

static inline void held_bridge_replay(moq_transport_bridge_t *b, uint64_t now)
{
    if (!b) return;
    test_held_input_t *h = &held_bridge_driver(b)->input;
    if (moq_transport_bridge_is_terminal(b)) h->count = 0;
    else HELD_REQUIRE(test_replay_uni(h, b, now) == MOQ_OK);
}

static inline moq_result_t held_bridge_service(moq_transport_bridge_t *b, uint64_t now)
{
    moq_result_t rc = moq_transport_bridge_service(b, now);
    if (rc == MOQ_OK || (b && moq_transport_bridge_is_terminal(b))) held_bridge_replay(b, now);
    return rc;
}

static inline moq_result_t held_bridge_service_budgeted(moq_transport_bridge_t *b,
    uint64_t now, uint32_t budget, moq_bridge_budgeted_result_t *result)
{
    moq_result_t rc = moq_transport_bridge_service_budgeted(b, now, budget, result);
    if (rc == MOQ_OK || (b && moq_transport_bridge_is_terminal(b))) held_bridge_replay(b, now);
    return rc;
}
#endif
