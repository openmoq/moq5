/* Deterministic pre-CONNECT ownership, replay, and bounded rejection. */
#include "pico_wt_harness.h"
#include "pico_wt_test_seam.h"
#include "../pico_wt_adapter.h"
#include <picoquic_internal.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static int failures;
#define CHECK(c) do { if (!(c)) { fprintf(stderr, "FAIL %d: %s\n", __LINE__, #c); failures++; } } while (0)
static size_t live;
static bool refuse_alloc;
static void *count_alloc(size_t n, void *ctx)
{
    if (refuse_alloc) return NULL;
    void *p = malloc(n);
    if (p) live++;
    return p;
}
static void count_free(void *p, size_t n, void *ctx)
{
    if (p) { live--; free(p); }
}

int main(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0xa8, .request_capacity = 10,
                                 .version = MOQ_VERSION_DRAFT_18 };
    if (pico_wt_harness_setup(&h, &cfg) != 0) return 1;
    moq_alloc_t alloc = *moq_alloc_default();
    alloc.alloc = count_alloc;
    alloc.free = count_free;
    moq_pico_wt_managed_t *m = moq_pico_wt_managed_test_early_create(h.client_conn, &alloc);
    CHECK(m != NULL);
    if (!m) { pico_wt_harness_cleanup(&h); return 1; }

    CHECK(moq_session_start(h.client_session, h.now) == MOQ_OK);
    CHECK(moq_session_start(h.server_session, h.now) == MOQ_OK);
    moq_action_t act;
    CHECK(moq_session_poll_actions(h.server_session, &act, 1) == 1);
    CHECK(act.kind == MOQ_ACTION_OPEN_UNI_CONTROL);
    size_t len = act.u.open_uni_control.len;
    CHECK(len > 1 && len < 64);
    uint8_t original[64], borrowed[64];
    memcpy(original, act.u.open_uni_control.data, len);
    memcpy(borrowed, original, len);
    uint64_t sid = 15;
    CHECK(picoquic_create_stream(h.test_ctx->cnx_client, sid) != NULL);
    /* Split the wire SETUP over two callbacks and poison their borrowed input.
     * Correct replay must preserve both content and callback order. */
    CHECK(moq_pico_wt_managed_test_early_callback(m, sid, borrowed, 1,
                                                picohttp_callback_post_data) == 0);
    CHECK(moq_pico_wt_managed_test_early_callback(m, sid, borrowed + 1, len - 1,
                                                picohttp_callback_post_data) == 0);
    memset(borrowed, 0xff, len);
    CHECK(moq_pico_wt_managed_test_early_pending(m) == 2);
    CHECK(moq_pico_wt_managed_test_early_replay(m, h.client_conn) == 0);
    CHECK(moq_pico_wt_managed_test_early_pending(m) == 0);
    h3zero_stream_ctx_t *sc = h3zero_find_stream(h.client_h3_ctx, sid);
    CHECK(sc && sc->path_callback == moq_pico_wt_callback);
    CHECK(sc && sc->path_callback_ctx == h.client_conn);
    int setups = 0;
    moq_event_t ev;
    while (moq_session_poll_events(h.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) setups++;
        moq_event_cleanup(&ev);
    }
    CHECK(setups == 1);
    CHECK(!moq_pico_wt_conn_is_fatal(h.client_conn));
    CHECK(live == 1); /* only the fixture remains; all copied callbacks freed */
    moq_action_cleanup(&act);

    /* STOP_SENDING affects our send half: it must retain earlier receive data.
     * A later RESET/FREE retires the queue without dereferencing a stale ctx. */
    CHECK(moq_pico_wt_managed_test_early_callback(m, 19, original, 1,
                                                picohttp_callback_post_data) == 0);
    CHECK(moq_pico_wt_managed_test_early_callback(m, 19, NULL, 0,
                                                picohttp_callback_stop_sending) == 0);
    CHECK(moq_pico_wt_managed_test_early_pending(m) == 2);
    CHECK(moq_pico_wt_managed_test_early_callback(m, 19, NULL, 0,
                                                picohttp_callback_reset) == 0);
    CHECK(moq_pico_wt_managed_test_early_pending(m) == 0);
    CHECK(live == 1);
    CHECK(moq_pico_wt_managed_test_early_callback(m, 23, original, 1,
                                                picohttp_callback_post_fin) == 0);
    CHECK(moq_pico_wt_managed_test_early_callback(m, 23, NULL, 0,
                                                picohttp_callback_free) == 0);
    CHECK(moq_pico_wt_managed_test_early_pending(m) == 0);
    CHECK(live == 1);

    /* Stream-count overflow rejects only the new stream with the WT code. */
    for (unsigned i = 0; i < 129; i++) {
        uint64_t stream = 27 + 4 * i;
        CHECK(picoquic_create_stream(h.test_ctx->cnx_client, stream) != NULL);
        CHECK(moq_pico_wt_managed_test_early_callback(m, stream, original, 1,
                                                    picohttp_callback_post_data) == 0);
    }
    CHECK(moq_pico_wt_managed_test_early_pending(m) == 128);
    picoquic_stream_head_t *rejected = picoquic_find_stream(h.test_ctx->cnx_client, 27 + 4 * 128);
    CHECK(rejected && rejected->stop_sending_requested);
    CHECK(rejected && rejected->local_stop_error == H3ZERO_WEBTRANSPORT_BUFFERED_STREAM_REJECTED);
    moq_pico_wt_managed_test_early_destroy(m);
    CHECK(live == 0);

    /* Payload overflow and allocation failure reject that stream and clean all
     * its earlier callbacks while leaving the facade available. */
    m = moq_pico_wt_managed_test_early_create(h.client_conn, &alloc);
    CHECK(m != NULL);
    if (m) {
        CHECK(moq_pico_wt_managed_test_early_callback(m, 19, original, 1,
                                                    picohttp_callback_post_data) == 0);
        CHECK(moq_pico_wt_managed_test_early_callback(m, 19, original, SIZE_MAX,
                                                    picohttp_callback_post_data) == 0);
        CHECK(moq_pico_wt_managed_test_early_pending(m) == 0);
        refuse_alloc = true;
        CHECK(moq_pico_wt_managed_test_early_callback(m, 23, original, 1,
                                                    picohttp_callback_post_data) == 0);
        refuse_alloc = false;
        CHECK(moq_pico_wt_managed_test_early_pending(m) == 0);
        for (unsigned i = 0; i < 1025; i++)
            CHECK(moq_pico_wt_managed_test_early_callback(m, 19, NULL, 0,
                                                        picohttp_callback_post_data) == 0);
        CHECK(moq_pico_wt_managed_test_early_pending(m) == 0);
        CHECK(live == 1);
        moq_pico_wt_managed_test_early_destroy(m);
    }
    CHECK(live == 0);
    pico_wt_harness_cleanup(&h);
    return failures ? 1 : 0;
}
