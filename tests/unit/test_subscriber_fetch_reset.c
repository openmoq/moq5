/*
 * Subscriber facade: a FETCH_RESET terminal reaches the consumer as a
 * MOQ_SUB_FETCH_RESET item carrying the peer's stream code and source flags,
 * survives fetch-item queue pressure as the retained pending item, and
 * releases the request slot when polled (a later moq_sub_fetch reuses it).
 * The publisher side is the raw session (abort is a session-level publisher
 * operation); the subscriber side is the facade over the fetcher session.
 */
#include "test_fetch_abort_rig.h"
#include <moq/subscriber.h>

typedef struct items {
    int ok, object, reset, error, complete, other;
    uint64_t reset_code; bool reset_data, reset_stop;
    moq_sub_fetch_req_t *reset_req;
    int kinds[32]; int n;
} items_t;

static int poll_all(moq_subscriber_t *sub, items_t *it)
{
    moq_sub_fetch_item_t item;
    int got = 0;
    while (moq_sub_poll_fetch(sub, &item) == MOQ_OK) {
        if (it->n < 32) it->kinds[it->n] = (int)item.kind;
        it->n++;
        switch (item.kind) {
        case MOQ_SUB_FETCH_OK: it->ok++; break;
        case MOQ_SUB_FETCH_OBJECT: it->object++; break;
        case MOQ_SUB_FETCH_COMPLETE: it->complete++; break;
        case MOQ_SUB_FETCH_ERROR: it->error++; break;
        case MOQ_SUB_FETCH_RESET:
            it->reset++; it->reset_code = item.u.reset.error_code;
            it->reset_data = item.u.reset.data_stream; it->reset_stop = item.u.reset.stop_sending;
            it->reset_req = item.request; break;
        default: it->other++; break;
        }
        moq_sub_fetch_item_cleanup(&item);
        got++;
    }
    return got;
}

static int facade_row(moq_version_t v, uint32_t max_fetch_items)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    int failures = 0;
    rig_t r;
    if (!rig_up(&r, v, 0)) { printf("FAIL: rig up %s\n", lbl); return 1; }
    moq_sub_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    moq_sub_cfg_init(&cfg);
    cfg.max_fetch_items = max_fetch_items;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_create(r.fet, &r.alloc.vt, &cfg, &sub), (int)MOQ_OK);
    if (!sub) { rig_down(&r); return failures + 1; }

    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
    moq_sub_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_sub_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("A");
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 4;
    moq_sub_fetch_req_t *req = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_fetch(sub, &fc, r.now, &req), (int)MOQ_OK);
    MOQ_TEST_CHECK(pub_await_requests(&r, 1));
    moq_fetch_t pa = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pa, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pa, 1), (int)MOQ_OK);
    for (int i = 0; i < 4; i++) cycle(&r);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fetch(r.pub, pa, 0x7, r.now), (int)MOQ_OK);
    for (int i = 0; i < 4; i++) cycle(&r);

    /* consume with the facade: tick, poll one item at a time (queue pressure
     * when max_fetch_items is small), until the terminal lands */
    items_t it; memset(&it, 0, sizeof(it));
    int blocks = 0;
    for (int i = 0; i < 24 && it.reset == 0; i++) {
        moq_result_t trc = moq_sub_tick(sub, r.now);
        if (trc == MOQ_ERR_WOULD_BLOCK) blocks++;
        poll_all(sub, &it);
        cycle(&r); drain(r.pub, &r.pe, true);
    }
    moq_sub_stats_t st; memset(&st, 0, sizeof(st));
    moq_sub_get_stats(sub, &st, sizeof(st));
    printf("MEASURED %s facade(items=%u): ok=%d object=%d reset=%d(code=0x%llx data=%d stop=%d) error=%d "
           "complete=%d tick_blocks=%d stats.would_blocks=%llu\n", lbl, max_fetch_items, it.ok, it.object,
           it.reset, (unsigned long long)it.reset_code, (int)it.reset_data, (int)it.reset_stop, it.error,
           it.complete, blocks, (unsigned long long)st.tick_would_blocks);
    MOQ_TEST_CHECK_EQ_INT(it.ok, 1);
    MOQ_TEST_CHECK_EQ_INT(it.object, 2);
    MOQ_TEST_CHECK_EQ_INT(it.reset, 1);
    MOQ_TEST_CHECK_EQ_U64(it.reset_code, 0x7);
    MOQ_TEST_CHECK(it.reset_data);
    MOQ_TEST_CHECK(!it.reset_stop);
    MOQ_TEST_CHECK(it.reset_req == req);
    MOQ_TEST_CHECK_EQ_INT(it.error + it.complete + it.other, 0);
    MOQ_TEST_CHECK_EQ_INT(it.kinds[it.n - 1], (int)MOQ_SUB_FETCH_RESET);   /* terminal is last */
    if (max_fetch_items == 1) MOQ_TEST_CHECK(st.tick_would_blocks > 0);  /* pressure was real */
    /* slot released on poll: the next fetch reuses it and runs to completion */
    moq_sub_fetch_req_t *req2 = NULL;
    fc.track_name = MOQ_BYTES_LITERAL("B");
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_fetch(sub, &fc, r.now, &req2), (int)MOQ_OK);
    MOQ_TEST_CHECK(req2 == req);
    MOQ_TEST_CHECK(pub_await_requests(&r, 2));
    moq_fetch_t pb = r.pe.last_request;
    MOQ_TEST_CHECK_EQ_INT((int)pub_accept(&r, pb, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pub_write(&r, pb, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(r.pub, pb, r.now), (int)MOQ_OK);
    memset(&it, 0, sizeof(it));
    for (int i = 0; i < 24 && it.complete == 0; i++) { (void)moq_sub_tick(sub, r.now); poll_all(sub, &it); cycle(&r); drain(r.pub, &r.pe, true); }
    MOQ_TEST_CHECK_EQ_INT(it.complete, 1);
    MOQ_TEST_CHECK_EQ_INT(it.reset, 0);
    moq_sub_destroy(sub);
    rig_down(&r);
    MOQ_TEST_CHECK_EQ_SIZE(r.alloc.live, 0);
    int f = failures;
    if (f == 0) printf("PASS: facade_row %s items=%u\n", lbl, max_fetch_items);
    return f;
}

int main(void)
{
    int failures = 0;
    failures += facade_row(MOQ_VERSION_DRAFT_16, 0);
    failures += facade_row(MOQ_VERSION_DRAFT_18, 0);
    failures += facade_row(MOQ_VERSION_DRAFT_16, 1);
    failures += facade_row(MOQ_VERSION_DRAFT_18, 1);
    printf("%s: subscriber_fetch_reset (%d failures)\n", failures == 0 ? "PASS" : "FAIL", failures);
    return failures == 0 ? 0 : 1;
}
