#ifndef MOQ_TEST_HELD_INPUT_H
#define MOQ_TEST_HELD_INPUT_H

#include <moq/transport_bridge.h>
#include <string.h>

/* Test transport storage: preserve call boundaries and FIN while the bridge
 * refuses admission. A full recorder fails visibly instead of losing bytes. */
typedef struct test_held_input {
    struct {
        uint64_t sid;
        size_t len;
        bool fin;
        uint8_t bytes[4096];
    } chunks[32];
    size_t count;
} test_held_input_t;

static inline moq_result_t test_hold_uni(test_held_input_t *h,
    moq_transport_bridge_t *bridge, uint64_t sid, const uint8_t *data,
    size_t len, bool fin, uint64_t now)
{
    bool queued = false;
    for (size_t i = 0; i < h->count; i++) if (h->chunks[i].sid == sid) queued = true;
    if (!queued) {
        moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(bridge, sid, data, len, fin, now);
        if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) return rc;
    }
    if (h->count == 32 || len > sizeof(h->chunks[0].bytes)) return MOQ_ERR_BUFFER;
    size_t i = h->count++;
    h->chunks[i].sid = sid;
    h->chunks[i].len = len;
    h->chunks[i].fin = fin;
    if (len) memcpy(h->chunks[i].bytes, data, len);
    return MOQ_OK;
}

static inline moq_result_t test_replay_uni(test_held_input_t *h,
    moq_transport_bridge_t *bridge, uint64_t now)
{
    for (size_t i = 0; i < h->count;) {
        uint64_t sid = h->chunks[i].sid;
        bool earlier = false;
        for (size_t j = 0; j < i; j++) if (h->chunks[j].sid == sid) earlier = true;
        if (earlier || moq_transport_bridge_stream_has_pending(bridge, sid)) { i++; continue; }
        moq_result_t rc = MOQ_OK;
        if (moq_transport_bridge_find_ref(bridge, sid)._v != 0)
            rc = moq_transport_bridge_on_peer_uni_bytes(bridge, sid,
                h->chunks[i].bytes, h->chunks[i].len, h->chunks[i].fin, now);
        if (rc == MOQ_ERR_INPUT_NOT_CONSUMED) { i++; continue; }
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED) return rc;
        h->count--;
        memmove(&h->chunks[i], &h->chunks[i + 1], (h->count - i) * sizeof(h->chunks[0]));
    }
    return MOQ_OK;
}
#endif
