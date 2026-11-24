/*
 * Standalone fetches of an MPEG2TS media track through the receiver's fetch
 * intake: the production record, item handler, queue and poll paths, fed
 * facade fetch items directly (no wire), as test_media_receiver_ts_fill.c
 * feeds fill items. The record is opened the way admission opens it
 * (receiver_fetch_open_locked); the facade request is a stand-in pointer,
 * compared and never dereferenced.
 *
 * Pinned:
 * - fetched Objects of whole TS packets come out with origin FETCH and the
 *   fetch's request ID, payloads intact (strip_prefix off), in fetch order;
 * - they bypass the track's live reorder window: a running window holds
 *   none of them and its refusal counter does not move; a LIVE Object
 *   released while the fetch is open flows normally, with origin LIVE;
 * - FETCH_TERMINAL COMPLETE only after all of the fetch's Objects are
 *   polled, with no loss summary;
 * - an End of Unknown Range the fetch ends with sets the loss summary on
 *   FETCH_TERMINAL, starting after the fetch's last Object.
 */
#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#include "test_support.h"
#include "test_alloc_support.h"
#include "ts_stream.h"

#include <stdio.h>
#include <string.h>

static int failures;

static const char ts_catalog[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1}]}";

/* Stand-ins, never dereferenced: the subscription and two facade requests. */
static char gen_ts, gen_catalog, req_a, req_b;
#define SUB_TS      ((moq_sub_track_t *)(void *)&gen_ts)
#define SUB_CATALOG ((moq_sub_track_t *)(void *)&gen_catalog)
#define REQ_A       ((moq_sub_fetch_req_t *)(void *)&req_a)
#define REQ_B       ((moq_sub_fetch_req_t *)(void *)&req_b)

#define MAX_PAYLOAD (8 * 188)

typedef struct fx {
    test_alloc_state_t state;
    moq_alloc_t alloc;
    moq_media_receiver_t *r;
    moq_media_track_t *ts;
    ts_gen_t gen;
    /* What each fetched Object carried, for the intact-payload check. */
    uint8_t sent[4][MAX_PAYLOAD];
    size_t sent_len[4];
} fx_t;

static bool fx_open(fx_t *f)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->state);
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    static moq_bytes_t ns[] = { { (const uint8_t *)"tsfetch", 7 } };
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.auto_subscribe = true;
    cfg.mpeg2ts_strip_prefix = false;
    cfg.mpeg2ts_join_psi = false;
    cfg.companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->r != NULL);
    if (!f->r) return false;
    f->r->transport_version = MOQ_VERSION_DRAFT_18;
    moq_media_receiver_test_ingest(f->r, 0, 0, ts_catalog, strlen(ts_catalog));
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f->ts = ev.track;
    MOQ_TEST_CHECK(f->ts != NULL);
    if (!f->ts) return false;
    f->ts->sub_track = SUB_TS;
    f->r->catalog_sub = SUB_CATALOG;
    MOQ_TEST_CHECK(f->ts->desired_subscribed);
    /* The live subscription's reorder window, as reconciliation starts it. */
    MOQ_TEST_CHECK(receiver_ts_start(f->r, f->ts, 0));
    ts_gen_init(&f->gen, 188, 1, false);
    return true;
}

static void fx_close(fx_t *f)
{
    moq_media_track_event_t ev;
    moq_media_object_t o;
    if (!f->r) return;
    while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK)
        moq_media_object_cleanup(&o);
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {}
    moq_media_receiver_test_free(f->r);
    f->r = NULL;
    MOQ_TEST_CHECK(f->state.balance == 0);
}

/* The fetch is sent: its record opens and FETCH_CREATED is queued, which is
 * polled here. */
static void fetch_created(fx_t *f, moq_sub_fetch_req_t *req, uint64_t id)
{
    pthread_mutex_lock(&f->r->mu);
    receiver_fetch_open_locked(f->r, f->ts, req, id, 0);
    pthread_mutex_unlock(&f->r->mu);
    moq_media_track_event_t ev;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_MEDIA_TRACK_FETCH_CREATED);
    MOQ_TEST_CHECK_EQ_U64(ev.initiating_request_id, id);
}

static void feed(fx_t *f, moq_sub_fetch_item_t *it)
{
    receiver_take_fetch_item(f->r, it);
}

/* Object FETCH g:o of whole packets; slot k keeps a copy of its payload. */
static void fetch_object(fx_t *f, moq_sub_fetch_req_t *req, uint64_t g,
                         uint64_t o, unsigned k)
{
    size_t n = ts_gen_gop(&f->gen, f->sent[k], 1);
    f->sent_len[k] = n;
    moq_sub_fetch_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = MOQ_SUB_FETCH_OBJECT;
    it.request = req;
    it.u.object.group_id = g;
    it.u.object.object_id = o;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, f->sent[k], n,
                                    &it.u.object.payload) == MOQ_OK);
    feed(f, &it);
}

static void fetch_range(fx_t *f, moq_sub_fetch_req_t *req, uint32_t kind,
                        uint64_t g, uint64_t o)
{
    moq_sub_fetch_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = MOQ_SUB_FETCH_GAP;
    it.request = req;
    it.u.gap.range_kind = (moq_fetch_range_kind_t)kind;
    it.u.gap.group_id = g;
    it.u.gap.object_id = o;
    feed(f, &it);
}

static void fetch_complete(fx_t *f, moq_sub_fetch_req_t *req)
{
    moq_sub_fetch_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = MOQ_SUB_FETCH_COMPLETE;
    it.request = req;
    feed(f, &it);
}

/* A LIVE Object of the track as its reorder window releases it. */
static void live_object(fx_t *f, uint64_t g)
{
    uint8_t buf[MAX_PAYLOAD];
    size_t n = ts_gen_gop(&f->gen, buf, 1);
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_TS;
    so.group_id = g;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, buf, n, &so.payload) == MOQ_OK);
    receiver_loss_t loss;
    memset(&loss, 0, sizeof(loss));
    receiver_deliver_object(f->r, f->ts, &so, &loss, NULL, MOQ_MEDIA_DELIVERY_LIVE, 0,
                            NULL, NULL);
}

static void expect_no_event(fx_t *f)
{
    moq_media_track_event_t ev;
    moq_result_t rc = moq_media_receiver_poll_track(f->r, &ev, sizeof(ev));
    MOQ_TEST_CHECK(rc == MOQ_DONE || rc == MOQ_ERR_CLOSED);
}

/* The next Object is fetched Object g:o, slot k's payload, untouched. */
static void expect_fetched(fx_t *f, uint64_t id, uint64_t g, uint64_t o, unsigned k)
{
    moq_media_object_t obj;
    memset(&obj, 0, sizeof(obj));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(obj.track == f->ts);
    MOQ_TEST_CHECK_EQ_INT((int)obj.delivery_origin, (int)MOQ_MEDIA_DELIVERY_FETCH);
    MOQ_TEST_CHECK_EQ_U64(obj.initiating_request_id, id);
    MOQ_TEST_CHECK_EQ_U64(obj.group_id, g);
    MOQ_TEST_CHECK_EQ_U64(obj.object_id, o);
    MOQ_TEST_CHECK(obj.payload.len == f->sent_len[k] &&
                   memcmp(obj.payload.data, f->sent[k], f->sent_len[k]) == 0);
    MOQ_TEST_CHECK(obj.has_loss_report && !obj.gap);
    moq_media_object_cleanup(&obj);
}

static void expect_terminal(fx_t *f, uint64_t id, uint32_t causes,
                            uint64_t g, uint64_t o)
{
    moq_media_track_event_t ev;
    memset(&ev, 0xa5, sizeof(ev));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_MEDIA_TRACK_FETCH_TERMINAL);
    MOQ_TEST_CHECK(ev.track == f->ts);
    MOQ_TEST_CHECK_EQ_U64(ev.initiating_request_id, id);
    MOQ_TEST_CHECK_EQ_U64(ev.fill_reason, MOQ_MEDIA_FETCH_COMPLETE);
    MOQ_TEST_CHECK_EQ_U64(ev.fill_error_domain, MOQ_MEDIA_FETCH_ERROR_NONE);
    MOQ_TEST_CHECK_EQ_INT((int)ev.local_result, 0);
    MOQ_TEST_CHECK_EQ_U64(ev.fill_unreported_loss_causes, causes);
    MOQ_TEST_CHECK_EQ_U64(ev.fill_unreported_loss_group, causes ? g : 0);
    MOQ_TEST_CHECK_EQ_U64(ev.fill_unreported_loss_object, causes ? o : 0);
}

static uint64_t reorder_refused(fx_t *f)
{
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(f->r, &st, sizeof(st)),
                          (int)MOQ_OK);
    return st.reorder_refused;
}

/* Three fetched Objects bypass the window, a LIVE Object released meanwhile
 * flows, and COMPLETE comes only after the third fetched Object. */
static void test_ts_fetch_complete(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f)) { fx_close(&f); return; }
    uint64_t refused = reorder_refused(&f);
    fetch_created(&f, REQ_A, 40);
    fetch_object(&f, REQ_A, 3, 0, 0);
    fetch_object(&f, REQ_A, 3, 1, 1);
    /* A LIVE Object released while the fetch is open. */
    live_object(&f, 9);
    fetch_object(&f, REQ_A, 3, 2, 2);
    fetch_complete(&f, REQ_A);
    /* None of them is in the live window, and its counter is unchanged. */
    MOQ_TEST_CHECK(!ts_holds_before(f.ts, UINT64_MAX));
    MOQ_TEST_CHECK_EQ_U64(reorder_refused(&f), refused);
    MOQ_TEST_CHECK_EQ_U64(f.ts->fetch_objects_received, 3);
    expect_no_event(&f);
    expect_fetched(&f, 40, 3, 0, 0);
    expect_fetched(&f, 40, 3, 1, 1);
    expect_no_event(&f);
    /* The LIVE Object is delivered as LIVE, in its arrival place, and does
     * not let the fetch's terminal through. */
    moq_media_object_t o;
    memset(&o, 0, sizeof(o));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(f.r, &o, sizeof(o)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)o.delivery_origin, (int)MOQ_MEDIA_DELIVERY_LIVE);
    MOQ_TEST_CHECK_EQ_U64(o.initiating_request_id, 0);
    MOQ_TEST_CHECK_EQ_U64(o.group_id, 9);
    moq_media_object_cleanup(&o);
    expect_no_event(&f);
    expect_fetched(&f, 40, 3, 2, 2);
    expect_terminal(&f, 40, 0, 0, 0);
    expect_no_event(&f);
    MOQ_TEST_CHECK_EQ_U64(reorder_refused(&f), refused);
    fx_close(&f);
    if (failures == before) MOQ_TEST_PASS("ts_fetch_complete_bypasses_window");
}

/* The peer ends the fetch with an End of Unknown Range after its first
 * Object: FETCH_TERMINAL reports UNKNOWN from the Location after that
 * Object. A second fetch open meanwhile keeps its own, clean summary. */
static void test_ts_fetch_peer_gap(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f)) { fx_close(&f); return; }
    fetch_created(&f, REQ_A, 50);
    fetch_created(&f, REQ_B, 51);
    fetch_object(&f, REQ_A, 4, 0, 0);
    fetch_object(&f, REQ_B, 6, 0, 1);
    fetch_range(&f, REQ_A, MOQ_FETCH_RANGE_UNKNOWN, 4, 9);
    fetch_complete(&f, REQ_A);
    fetch_complete(&f, REQ_B);
    expect_fetched(&f, 50, 4, 0, 0);
    expect_terminal(&f, 50, MOQ_MEDIA_FILL_LOSS_UNKNOWN, 4, 1);
    expect_fetched(&f, 51, 6, 0, 1);
    expect_terminal(&f, 51, 0, 0, 0);
    expect_no_event(&f);
    fx_close(&f);
    if (failures == before) MOQ_TEST_PASS("ts_fetch_peer_unknown_range");
}

int main(void)
{
    test_ts_fetch_complete();
    test_ts_fetch_peer_gap();
    printf("receiver_ts_fetch: %d failures\n", failures);
    return failures ? 1 : 0;
}
