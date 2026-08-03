/*
 * Stream-Count accounting over stream terminations: an
 * identifiable (publication- or subscription-bound) RESET counts as a
 * processed stream exactly like a FIN, exactly once, at successful terminal
 * teardown only -- including across a WOULD_BLOCK'd streaming terminal-RESET
 * event. Pre-header resets are not attributed to anything. A terminal done
 * whose finite, nonzero count is unsatisfied is deferred: the entry stays
 * established and alias-bindable until the advertised streams are processed.
 * Both drafts.
 */
#include <moq/codec.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static int failures = 0;

typedef struct { int64_t balance; bool fail_all; } sc_alloc_state_t;
static void *sc_alloc(size_t n, void *ctx)
{ sc_alloc_state_t *s = ctx; if (s->fail_all) return NULL;
  void *p = malloc(n); if (p) s->balance++; return p; }
static void *sc_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; return realloc(p, n); }
static void sc_free(void *p, size_t n, void *ctx)
{ sc_alloc_state_t *s = ctx; (void)n; if (p) s->balance--; free(p); }
static moq_alloc_t sc_allocator(sc_alloc_state_t *s)
{ moq_alloc_t a = { s, sc_alloc, sc_realloc, sc_free }; return a; }

static void drain_events(moq_session_t *s)
{ moq_event_t e; while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e); }

/* Pair where the SERVER publishes and the CLIENT is the subscriber (so the
 * client_streaming_objects knob governs the receiving side). */
static moq_simpair_t *sc_pair(moq_alloc_t *alloc, moq_version_t ver,
                              bool streaming, uint32_t max_events)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 7; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    cfg.client_streaming_objects = streaming;
    if (max_events) cfg.max_events = max_events;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    return sp;
}

/* Server publishes "live"/<name>; client accepts. Fills both handles. */
static bool sc_establish_named(moq_simpair_t *sp, const char *name,
                               moq_publication_t *sv_pub,
                               moq_publication_t *cl_pub)
{
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 1 };
    pc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    pc.has_forward = true; pc.forward = true;
    if (moq_session_publish(sv, &pc, now, sv_pub) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t e; bool got = false;
    while (moq_session_poll_events(cl, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            *cl_pub = e.u.publish_request.pub;
            got = true;
        }
        moq_event_cleanup(&e);
    }
    if (!got) return false;
    moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(cl, *cl_pub, &ac, now) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(sv); drain_events(cl);
    return true;
}

static bool sc_establish(moq_simpair_t *sp, moq_publication_t *sv_pub,
                         moq_publication_t *cl_pub)
{
    return sc_establish_named(sp, "t", sv_pub, cl_pub);
}

static int count_finished(moq_session_t *s, uint64_t *out_count)
{
    int n = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            n++;
            if (out_count) *out_count = e.u.publish_finished.stream_count;
        }
        moq_event_cleanup(&e);
    }
    return n;
}

static uint64_t cl_processed(moq_session_t *cl, moq_publication_t pub)
{
    int slot = pub_resolve_handle(cl, pub);
    return slot >= 0 ? cl->publishes[slot].processed_stream_count
                     : UINT64_MAX;
}

/* -- done-before-reset AND reset-before-done ------------------------ */
static void test_done_reset_ordering(moq_version_t ver, bool done_first)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));

    /* One publication subgroup with one delivered object; the subgroup stays
     * OPEN so its termination is the RESET under test. */
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now, &sg)
                   == MOQ_OK);
    uint8_t d[] = { 0x5A, 0x5B };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, pay, now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    if (done_first) {
        /* DONE-BEFORE-RESET at the receiver. The sender API (correctly)
         * refuses to finish with an open stream, so the inbound done (status
         * 0x2, count 1) is CRAFTED and fed to the subscriber while its data
         * stream is still live: PUBLISH_FINISHED must DEFER until the
         * advertised stream terminates -- by RESET here. */
        uint8_t buf[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0);
        if (ver == MOQ_VERSION_DRAFT_18) {
            MOQ_TEST_CHECK(moq_d18_encode_publish_done(&w, 0x2, 1,
                (moq_bytes_t){0}) == MOQ_OK);
            moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;
            MOQ_TEST_CHECK(ref._v != 0);
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, buf,
                moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        } else {
            MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w,
                cl->publishes[slot].request_id, 0x2, 1, NULL, 0) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, buf,
                moq_buf_writer_offset(&w), now) == MOQ_OK);
        }
        MOQ_TEST_CHECK_EQ_INT(count_finished(cl, NULL), 0);   /* deferred */
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        uint64_t cnt = 0;
        MOQ_TEST_CHECK_EQ_INT(count_finished(cl, &cnt), 1);
        MOQ_TEST_CHECK_EQ_U64(cnt, 1);
    } else {
        /* Reset first: the count is banked; the sender-side finish (legal
         * once the stream is reset) finalizes at once. */
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK_EQ_U64(cl_processed(cl, cl_pub), 1);
        moq_finish_publish_cfg_t fc; moq_finish_publish_cfg_init(&fc);
        fc.status_code = 0x2; fc.stream_count = 1;
        MOQ_TEST_CHECK(moq_session_finish_publish(sv, sv_pub, &fc, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        uint64_t cnt = 0;
        MOQ_TEST_CHECK_EQ_INT(count_finished(cl, &cnt), 1);
        MOQ_TEST_CHECK_EQ_U64(cnt, 1);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- streaming terminal-RESET event backpressure: exact-once count -- */
static void test_streaming_reset_backpressure(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    /* Tiny client event queue; streaming receive enabled. */
    moq_simpair_t *sp = sc_pair(&alloc, ver, true, 2);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));

    /* Begin a streamed object and deliver a partial chunk: the client is
     * mid-STREAMING_PAYLOAD. */
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now, &sg)
                   == MOQ_OK);
    moq_begin_object_cfg_t bc; moq_begin_object_cfg_init(&bc);
    bc.object_id = 0; bc.payload_length = 4;
    MOQ_TEST_CHECK(moq_session_begin_object_ex(sv, sg, &bc, now) == MOQ_OK);
    uint8_t c1[] = { 0x11, 0x22 };
    moq_rcbuf_t *pc1 = NULL; moq_rcbuf_create(&alloc, c1, sizeof(c1), &pc1);
    MOQ_TEST_CHECK(moq_session_write_object_data(sv, sg, pc1, now) == MOQ_OK);
    moq_rcbuf_decref(pc1);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    /* Leave the chunk events UNDRAINED: the queue (cap 2) is full, so the
     * terminal-RESET chunk cannot be queued yet. */

    /* Publisher resets the stream; the client's terminal-RESET event push
     * WOULD_BLOCKs -- NO count yet (count only at successful teardown). */
    MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK_EQ_U64(cl_processed(cl, cl_pub), 0);

    /* Find the client's data-stream ref (the one live rx). */
    moq_stream_ref_t rref = moq_stream_ref_from_u64(0);
    for (size_t i = 0; i < cl->rx_cap; i++)
        if (cl->rx_streams[i].active) rref = cl->rx_streams[i].stream_ref;
    MOQ_TEST_CHECK(rref._v != 0);

    /* Drain the queue, re-drive the buffered reset: the terminal-RESET chunk
     * is delivered and the stream counts EXACTLY once. */
    drain_events(cl);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, rref, NULL, 0, false, now)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(cl_processed(cl, cl_pub), 1);
    /* Exactly ONE reset-terminal chunk event, with the expected fields. */
    {
        int nterm = 0; moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_OBJECT_CHUNK && e.u.object_chunk.end &&
                e.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_RESET)
                nterm++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(nterm, 1);
    }
    /* The receive slot is reclaimed; a second empty re-drive is an accepted
     * no-op and must not double count. */
    { size_t live = 0;
      for (size_t i = 0; i < cl->rx_cap; i++)
          if (cl->rx_streams[i].active) live++;
      MOQ_TEST_CHECK_EQ_SIZE(live, (size_t)0); }
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, rref, NULL, 0, false, now)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(cl_processed(cl, cl_pub), 1);
    { size_t live = 0;
      for (size_t i = 0; i < cl->rx_cap; i++)
          if (cl->rx_streams[i].active) live++;
      MOQ_TEST_CHECK_EQ_SIZE(live, (size_t)0); }

    /* The done (count 1) now finalizes exactly once. */
    drain_events(cl);
    moq_finish_publish_cfg_t fc; moq_finish_publish_cfg_init(&fc);
    fc.status_code = 0x2; fc.stream_count = 1;
    MOQ_TEST_CHECK(moq_session_finish_publish(sv, sv_pub, &fc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    uint64_t cnt = 0;
    MOQ_TEST_CHECK_EQ_INT(count_finished(cl, &cnt), 1);
    MOQ_TEST_CHECK_EQ_U64(cnt, 1);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- pre-header reset: attributed to nothing, stalls nothing --------- */
static void test_preheader_reset_unattributed(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));

    /* One real, FIN'd stream (processed 1). */
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now, &sg)
                   == MOQ_OK);
    uint8_t d[] = { 0x33 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, pay, now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);
    MOQ_TEST_CHECK_EQ_U64(cl_processed(cl, cl_pub), 1);

    /* PRE-HEADER reset: feed a PARTIAL subgroup header on a fresh ref -- one
     * byte allocates an ACTIVE receive slot that is NOT yet bound to any
     * publication -- then reset that ref: nothing is credited and the slot
     * is reclaimed. */
    {
        moq_stream_ref_t fresh = moq_stream_ref_from_u64(0xD00D);
        uint8_t partial = 0x10;   /* first header byte only */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, fresh, &partial, 1,
                                                 false, now) == MOQ_OK);
        int rxslot = -1;
        for (size_t i = 0; i < cl->rx_cap; i++)
            if (cl->rx_streams[i].active &&
                cl->rx_streams[i].stream_ref._v == fresh._v) rxslot = (int)i;
        MOQ_TEST_CHECK(rxslot >= 0);                        /* active... */
        MOQ_TEST_CHECK(rxslot >= 0 &&
            !moq_publication_is_valid(cl->rx_streams[rxslot].pub_handle));
                                                            /* ...unbound */
        MOQ_TEST_CHECK(moq_session_on_data_reset(cl, fresh, 0, now)
                       == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(cl_processed(cl, cl_pub), 1); /* no credit */
        int live = 0;
        for (size_t i = 0; i < cl->rx_cap; i++)
            if (cl->rx_streams[i].active &&
                cl->rx_streams[i].stream_ref._v == fresh._v) live++;
        MOQ_TEST_CHECK_EQ_INT(live, 0);                     /* reclaimed */
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    /* The done (count 1) is satisfied by the REAL stream alone. */
    moq_finish_publish_cfg_t fc; moq_finish_publish_cfg_init(&fc);
    fc.status_code = 0x2; fc.stream_count = 1;
    MOQ_TEST_CHECK(moq_session_finish_publish(sv, sv_pub, &fc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    uint64_t cnt = 0;
    MOQ_TEST_CHECK_EQ_INT(count_finished(cl, &cnt), 1);
    MOQ_TEST_CHECK_EQ_U64(cnt, 1);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ==================================================================== */
/* Ordinary-SUBSCRIBE Stream-Count gating                 */
/* ==================================================================== */

/* Client subscribes to "live"/<name>; server accepts. Fills both handles. */
static bool sc_sub_establish_named(moq_simpair_t *sp, const char *name,
                                   moq_subscription_t *cl_sub,
                                   moq_subscription_t *sv_sub)
{
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    if (moq_session_subscribe(cl, &sc, now, cl_sub) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_event_t e; bool got = false;
    while (moq_session_poll_events(sv, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            *sv_sub = e.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&e);
    }
    if (!got) return false;
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(sv, *sv_sub, &ac, now) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(sv); drain_events(cl);
    return true;
}

static bool sc_sub_establish(moq_simpair_t *sp, moq_subscription_t *cl_sub,
                             moq_subscription_t *sv_sub)
{
    return sc_sub_establish_named(sp, "t", cl_sub, sv_sub);
}

/* Poll every client event, tallying SUBSCRIBE_DONE (fields captured) and
 * delivered objects. */
typedef struct {
    int      done_n;
    int      obj_n;
    uint64_t done_status;
    uint64_t done_count;
    char     reason[64];
    size_t   reason_len;
} sub_evstats_t;

static void sub_poll_stats(moq_session_t *s, sub_evstats_t *st)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            st->done_n++;
            st->done_status = e.u.subscribe_done.status_code;
            st->done_count = e.u.subscribe_done.stream_count;
            st->reason_len = e.u.subscribe_done.reason.len;
            if (st->reason_len > 0 && st->reason_len <= sizeof(st->reason))
                memcpy(st->reason, e.u.subscribe_done.reason.data,
                       st->reason_len);
        } else if (e.kind == MOQ_EVENT_OBJECT_RECEIVED ||
                   e.kind == MOQ_EVENT_OBJECT_CHUNK) {
            st->obj_n++;
        }
        moq_event_cleanup(&e);
    }
}

static uint64_t cl_sub_processed(moq_session_t *cl, moq_subscription_t sub)
{
    int slot = sub_resolve_handle(cl, sub);
    return slot >= 0 ? cl->subs[slot].processed_stream_count : UINT64_MAX;
}

static bool cl_sub_done_pending(moq_session_t *cl, moq_subscription_t sub)
{
    int slot = sub_resolve_handle(cl, sub);
    return slot >= 0 && cl->subs[slot].done_pending;
}

/* Count of CLOSE_BIDI_STREAM actions stolen by the most recent crafted-done
 * feed (draft-18: the accept queues our close of the request bidi's send
 * half; the deferral must queue it EXACTLY once, and never again). */
static int g_feed_close_n;

/* Encode the done into buf; returns the wire length. Kept separate from the
 * feed so ownership tests can poison the source bytes after feeding. */
static size_t sub_encode_done(moq_session_t *cl, moq_version_t ver,
                              moq_subscription_t cl_sub, uint64_t status,
                              uint64_t count, const char *reason,
                              uint8_t *buf, size_t cap)
{
    size_t rlen = reason ? strlen(reason) : 0;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    int slot = sub_resolve_handle(cl, cl_sub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) return 0;
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_bytes_t rb = { (const uint8_t *)reason, rlen };
        MOQ_TEST_CHECK(moq_d18_encode_publish_done(&w, status, count, rb)
                       == MOQ_OK);
    } else {
        MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w,
            cl->subs[slot].request_id, status, count,
            (const uint8_t *)reason, rlen) == MOQ_OK);
    }
    return moq_buf_writer_offset(&w);
}

/* Feed pre-encoded done bytes to the client's subscriber-role subscription.
 * Under draft-18 our queued close of the request bidi's send half is stolen
 * before SimPair routes it (the crafted done means the server side never
 * sent one, so its live entry would misread the FIN) -- the network simply
 * hasn't delivered our half-close yet. The stolen closes are counted into
 * g_feed_close_n. */
static moq_result_t sub_feed_done_raw(moq_simpair_t *sp, moq_version_t ver,
                                      moq_subscription_t cl_sub,
                                      const uint8_t *buf, size_t len,
                                      bool fin)
{
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_result_t rc;
    g_feed_close_n = 0;
    if (ver == MOQ_VERSION_DRAFT_18) {
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0);
        if (slot < 0) return MOQ_ERR_STALE_HANDLE;
        moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
        MOQ_TEST_CHECK(ref._v != 0);
        rc = moq_session_on_bidi_stream_bytes(cl, ref, buf, len, fin, now);
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(cl, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_CLOSE_BIDI_STREAM)
                    g_feed_close_n++;
                else if (moq_session_state(cl) != MOQ_SESS_CLOSED)
                    MOQ_TEST_CHECK(acts[i].kind
                                   == MOQ_ACTION_CLOSE_BIDI_STREAM);
                moq_action_cleanup(&acts[i]);
            }
    } else {
        rc = moq_session_on_control_bytes(cl, buf, len, now);
    }
    return rc;
}

static moq_result_t sub_feed_done(moq_simpair_t *sp, moq_version_t ver,
                                  moq_subscription_t cl_sub, uint64_t status,
                                  uint64_t count, const char *reason,
                                  bool fin)
{
    uint8_t buf[128];
    size_t len = sub_encode_done(moq_simpair_client(sp), ver, cl_sub,
                                 status, count, reason, buf, sizeof(buf));
    if (len == 0) return MOQ_ERR_STALE_HANDLE;
    return sub_feed_done_raw(sp, ver, cl_sub, buf, len, fin);
}

/* Steal every queued client action, asserting NONE is a (duplicate) close of
 * the request bidi's send half. Use only where no further SimPair routing of
 * client actions is required. */
static void assert_no_close_actions(moq_session_t *cl)
{
    moq_action_t acts[8]; size_t na;
    while ((na = moq_session_poll_actions(cl, acts, 8)) > 0)
        for (size_t i = 0; i < na; i++) {
            MOQ_TEST_CHECK(acts[i].kind != MOQ_ACTION_CLOSE_BIDI_STREAM);
            moq_action_cleanup(&acts[i]);
        }
}

/* Open one subgroup on the server's publisher-role subscription and deliver
 * one small object through it (the subgroup is left open). */
static bool sub_open_stream(moq_simpair_t *sp, moq_subscription_t sv_sub,
                            uint64_t group, moq_subgroup_handle_t *out_sg)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    if (moq_session_open_subgroup(sv, sv_sub, &sgc, now, out_sg) != MOQ_OK)
        return false;
    uint8_t d[] = { 0x7E };
    moq_rcbuf_t *pay = NULL;
    if (moq_rcbuf_create(&sv->alloc, d, sizeof(d), &pay) != MOQ_OK)
        return false;
    bool ok = moq_session_write_object(sv, *out_sg, 0, pay, now) == MOQ_OK;
    moq_rcbuf_decref(pay);
    return ok;
}

/* -- done before termination: deferral, then FIN/reset finalizes ----- */
static void test_sub_done_before_termination(moq_version_t ver, bool use_reset)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    /* Done (status 0x2, count 1) while the stream is live: DEFERRED. */
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(g_feed_close_n,
                          ver == MOQ_VERSION_DRAFT_18 ? 1 : 0);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);                     /* deferred */
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 0);

    /* Terminate the advertised stream; the done finalizes exactly once. */
    if (use_reset)
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, now) == MOQ_OK);
    else
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    memset(&st, 0, sizeof(st));
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.done_status, 0x2);
    MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    if (ver == MOQ_VERSION_DRAFT_18) {
        /* No FIN yet: the entry drains as TERMINATED; the late FIN frees it. */
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0 &&
                       cl->subs[slot].state == MOQ_SUB_TERMINATED);
        moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, NULL, 0,
            true, now) == MOQ_OK);
        assert_no_close_actions(cl);   /* the accept's close was the only one */
    }
    MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);      /* freed */
    memset(&st, 0, sizeof(st));
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);                     /* exactly once */

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- termination before done: banked count, immediate finalize ------- */
static void test_sub_termination_before_done(moq_version_t ver, bool use_reset)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    if (use_reset)
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, now) == MOQ_OK);
    else
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);
    MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 1);  /* banked */

    /* Legal sender-side done now (no open streams): finalizes at once. */
    moq_done_subscribe_cfg_t dc; moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x4; dc.stream_count = 1;
    dc.reason = MOQ_BYTES_LITERAL("done");
    MOQ_TEST_CHECK(moq_session_done_subscribe(sv, sv_sub, &dc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.done_status, 0x4);
    MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)4);
    MOQ_TEST_CHECK(memcmp(st.reason, "done", 4) == 0);
    /* Draft-18 carries the FIN with the done (same call): freed at once.
     * Draft-16 frees on the control-channel path. */
    MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- late streams bind and deliver while the done is deferred -------- */
static void test_sub_late_streams_while_deferred(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    /* Done (count 2, reason) BEFORE any stream even opens: deferred. */
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 2, "gone", false)
                   == MOQ_OK);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);

    /* First late stream: opens AFTER the done, must still alias-bind and
     * deliver its object. */
    moq_subgroup_handle_t sg1;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg1));
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg1, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    memset(&st, 0, sizeof(st));
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 1);            /* delivered while gated */
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);           /* 1 of 2 processed */
    MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 1);

    /* Second late stream satisfies the count: done surfaces with the
     * RETAINED reason (owned copy, released exactly once). */
    moq_subgroup_handle_t sg2;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 1, &sg2));
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg2, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    memset(&st, 0, sizeof(st));
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 1);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.done_status, 0x2);
    MOQ_TEST_CHECK_EQ_U64(st.done_count, 2);
    MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)4);
    MOQ_TEST_CHECK(memcmp(st.reason, "gone", 4) == 0);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- finalize under event backpressure: reap retries exactly once ---- */
static void test_sub_backpressure_reap(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    /* Tiny (cap 2) client event queue. */
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 2);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    /* Defer a done (count 1) first. */
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 1, "late", false)
                   == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));

    /* One stream delivers + FINs with the event queue LEFT FULL: the object
     * and subgroup-finished events occupy both slots, so the finalize at the
     * FIN teardown WOULD_BLOCKs -- the deferral must survive intact. */
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 1);  /* counted */
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));         /* still gated */

    /* Drain the queue; the next advance reaps the deferred done. */
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);           /* nothing lost, none yet */
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 1) == MOQ_OK);
    memset(&st, 0, sizeof(st));
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)4);
    MOQ_TEST_CHECK(memcmp(st.reason, "late", 4) == 0);

    /* Exactly once: further advances surface nothing more, and the reap
     * queues no additional close of the send half. */
    assert_no_close_actions(cl);
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 2) == MOQ_OK);
    memset(&st, 0, sizeof(st));
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);
    assert_no_close_actions(cl);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- count 0 and the 2^62-1 sentinel finalize immediately ------------ */
static void test_sub_zero_and_sentinel(moq_version_t ver)
{
    /* Count 0 via the real sender API (no data streams at all). */
    {
        sc_alloc_state_t as = {0};
        moq_alloc_t alloc = sc_allocator(&as);
        moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *sv = moq_simpair_server(sp);
        moq_session_t *cl = moq_simpair_client(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        moq_done_subscribe_cfg_t dc; moq_done_subscribe_cfg_init(&dc);
        dc.status_code = 0x2; dc.stream_count = 0;
        MOQ_TEST_CHECK(moq_session_done_subscribe(sv, sv_sub, &dc, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st.done_count, 0);
        MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    /* §9.8: the 2^62-1 sentinel now WAITS for the terminal deadline (the
     * timeout IS the drafts' recovery for "unable to be exact"); on expiry
     * the live stream is discarded and the event surfaces with the
     * publisher's original values. */
    {
        sc_alloc_state_t as = {0};
        moq_alloc_t alloc = sc_allocator(&as);
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.seed = 7; scfg.initial_now_us = 1000;
        scfg.client_send_request_capacity = true;
        scfg.client_initial_request_capacity = 16;
        scfg.server_send_request_capacity = true;
        scfg.server_initial_request_capacity = 16;
        scfg.version = ver;
        scfg.done_wait_timeout_us = 2000000;   /* 2s */
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
        if (!sp) return;
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_session_t *cl = moq_simpair_client(sp);
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        uint64_t now = moq_simpair_now_us(sp);
        MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2,
            MOQ_QUIC_VARINT_MAX, NULL, false) == MOQ_OK);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);           /* DEFERRED now */
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
        /* The armed terminal deadline is reported for idle adapters. */
        MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(cl),
                              now + 2000000);
        /* Expiry: one tick past the deadline discards the live stream and
         * surfaces the terminal with the original sentinel count. */
        MOQ_TEST_CHECK(moq_session_tick(cl, now + 2000001) == MOQ_OK);
        memset(&st, 0, sizeof(st));
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st.done_count, MOQ_QUIC_VARINT_MAX);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* -- owned reason: destroy-while-deferred releases it (sub AND pub) -- */
static void test_sub_destroy_while_deferred(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    /* Defer a subscription done with an owned reason... */
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 3, "orphaned", false)
                   == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));

    /* ...and a publication done with one too (both destroy sweeps). */
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));
    {
        uint8_t buf[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0);
        uint64_t now = moq_simpair_now_us(sp);
        if (ver == MOQ_VERSION_DRAFT_18) {
            MOQ_TEST_CHECK(moq_d18_encode_publish_done(&w, 0x2, 3,
                MOQ_BYTES_LITERAL("orphaned")) == MOQ_OK);
            moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;
            MOQ_TEST_CHECK(ref._v != 0);
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, buf,
                moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        } else {
            MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w,
                cl->publishes[slot].request_id, 0x2, 3,
                (const uint8_t *)"orphaned", 8) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, buf,
                moq_buf_writer_offset(&w), now) == MOQ_OK);
        }
        MOQ_TEST_CHECK_EQ_INT(count_finished(cl, NULL), 0);   /* deferred */
    }

    /* Destroy with both deferrals live: the owned copies are released. */
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- OOM on the deferred reason copy: no mutation, retryable --------- */
static void test_sub_oom_reason(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    /* The reason-copy allocation fails: the feed reports the error and the
     * deferral is NOT recorded (no mutation on the failure path). */
    as.fail_all = true;
    moq_result_t rc = sub_feed_done(sp, ver, cl_sub, 0x2, 1, "gone", false);
    as.fail_all = false;
    MOQ_TEST_CHECK(rc < 0);
    MOQ_TEST_CHECK_EQ_INT(g_feed_close_n, 0);   /* no mutation, no close */
    MOQ_TEST_CHECK(!cl_sub_done_pending(cl, cl_sub));

    /* The retry defers cleanly. Draft-18 retained the unconsumed terminal in
     * the request-stream buffer, so the retry is an EMPTY re-drive (a fresh
     * feed would duplicate the message -- a protocol error); draft-16's
     * control message was not consumed, so the retry re-feeds it. */
    if (moq_session_state(cl) == MOQ_SESS_ESTABLISHED &&
        sub_resolve_handle(cl, cl_sub) >= 0) {
        moq_result_t rrc;
        if (ver == MOQ_VERSION_DRAFT_18) {
            int slot = sub_resolve_handle(cl, cl_sub);
            moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
            rrc = moq_session_on_bidi_stream_bytes(cl, ref, NULL, 0, false,
                                                   moq_simpair_now_us(sp));
            /* Steal the queued half-close, as in sub_feed_done. */
            moq_action_t acts[4]; size_t na;
            while ((na = moq_session_poll_actions(cl, acts, 4)) > 0)
                for (size_t i = 0; i < na; i++) {
                    MOQ_TEST_CHECK(acts[i].kind
                                   == MOQ_ACTION_CLOSE_BIDI_STREAM);
                    moq_action_cleanup(&acts[i]);
                }
        } else {
            rrc = sub_feed_done(sp, ver, cl_sub, 0x2, 1, "gone", false);
        }
        MOQ_TEST_CHECK(rrc == MOQ_OK);
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    }

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- draft-18 FIN variants around a deferred done --------------------- */
static void test_sub_d18_fin_variants(void)
{
    /* Same-call FIN: done + FIN in one feed; finalize frees immediately. */
    {
        sc_alloc_state_t as = {0};
        moq_alloc_t alloc = sc_allocator(&as);
        moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *sv = moq_simpair_server(sp);
        moq_session_t *cl = moq_simpair_client(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_18, cl_sub,
            0x2, 1, NULL, true /* FIN with the done */) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(g_feed_close_n, 1);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);                  /* deferred */
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        memset(&st, 0, sizeof(st));
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);   /* freed now */
        assert_no_close_actions(cl);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    /* Split FIN: done, then a bare FIN while still deferred (absorbed
     * quietly), then the stream completes -- finalize frees immediately. */
    {
        sc_alloc_state_t as = {0};
        moq_alloc_t alloc = sc_allocator(&as);
        moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *sv = moq_simpair_server(sp);
        moq_session_t *cl = moq_simpair_client(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_18, cl_sub,
            0x2, 1, NULL, false) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(g_feed_close_n, 1);
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0);
        moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
        /* Bare FIN while pending: accepted, recorded, no event, no close. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, NULL, 0,
            true, now) == MOQ_OK);
        assert_no_close_actions(cl);   /* a re-feed/FIN closes nothing more */
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        memset(&st, 0, sizeof(st));
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);   /* freed */
        assert_no_close_actions(cl);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* -- draft-18: bytes after the terminal while deferred = violation ---- */
static void test_sub_d18_extra_bytes_after_terminal(void)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);
    MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_18, cl_sub,
        0x2, 1, NULL, false) == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    int slot = sub_resolve_handle(cl, cl_sub);
    MOQ_TEST_CHECK(slot >= 0);
    moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
    uint8_t junk = 0x00;
    (void)moq_session_on_bidi_stream_bytes(cl, ref, &junk, 1, false, now);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- pre-header reset: attributed to nothing (subscription side) ------ */
static void test_sub_preheader_reset_unattributed(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    /* One real, FIN'd stream (processed 1). */
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);
    MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 1);

    /* Partial header on a fresh ref -> active, UNBOUND slot; reset it:
     * nothing is credited and the slot is reclaimed. */
    {
        moq_stream_ref_t fresh = moq_stream_ref_from_u64(0xF00D);
        uint8_t partial = 0x10;
        MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, fresh, &partial, 1,
                                                 false, now) == MOQ_OK);
        int rxslot = -1;
        for (size_t i = 0; i < cl->rx_cap; i++)
            if (cl->rx_streams[i].active &&
                cl->rx_streams[i].stream_ref._v == fresh._v) rxslot = (int)i;
        MOQ_TEST_CHECK(rxslot >= 0);
        MOQ_TEST_CHECK(rxslot >= 0 &&
            !moq_subscription_is_valid(cl->rx_streams[rxslot].sub) &&
            !moq_publication_is_valid(cl->rx_streams[rxslot].pub_handle));
        MOQ_TEST_CHECK(moq_session_on_data_reset(cl, fresh, 0, now)
                       == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 1); /* no credit */
    }

    /* The done (count 1) is satisfied by the REAL stream alone. */
    moq_done_subscribe_cfg_t dc; moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x2; dc.stream_count = 1;
    MOQ_TEST_CHECK(moq_session_done_subscribe(sv, sv_sub, &dc, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ==================================================================== */
/* logical-terminal guards + §5.1 pending-termination matrix  */
/* ==================================================================== */

/* Poll every client event, tallying PUBLISH_FINISHED (fields captured) and
 * delivered objects -- the publication-side sibling of sub_poll_stats. */
static void pub_poll_stats(moq_session_t *s, sub_evstats_t *st)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            st->done_n++;
            st->done_status = e.u.publish_finished.status_code;
            st->done_count = e.u.publish_finished.stream_count;
            st->reason_len = e.u.publish_finished.reason.len;
            if (st->reason_len > 0 && st->reason_len <= sizeof(st->reason))
                memcpy(st->reason, e.u.publish_finished.reason.data,
                       st->reason_len);
        } else if (e.kind == MOQ_EVENT_OBJECT_RECEIVED ||
                   e.kind == MOQ_EVENT_OBJECT_CHUNK) {
            st->obj_n++;
        }
        moq_event_cleanup(&e);
    }
}

/* Real subgroup-stream wire bytes (header + one object [+ FIN]) for `alias`,
 * captured from a throwaway ESTABLISHED publish pair of the same draft --
 * avoids hand-crafting per-profile data-plane encodings. */
typedef struct { uint8_t bytes[192]; size_t len; bool fin; } cap_chunk_t;
typedef struct { cap_chunk_t c[8]; size_t n; } cap_stream_t;

static bool capture_pub_stream_gp(moq_version_t ver, uint64_t alias,
                                  uint64_t group, bool with_object,
                                  size_t payload_len, cap_stream_t *out)
{
    out->n = 0;
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    if (!sp) return false;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("t");
    pc.has_forward = true; pc.forward = true;
    pc.has_track_alias = true;
    pc.track_alias = alias;
    moq_publication_t sv_pub;
    if (moq_session_publish(sv, &pc, now, &sv_pub) != MOQ_OK) goto fail;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    {
        moq_publication_t cl_pub; bool got = false;
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                cl_pub = e.u.publish_request.pub; got = true;
            }
            moq_event_cleanup(&e);
        }
        if (!got) goto fail;
        moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
        if (moq_session_accept_publish(cl, cl_pub, &ac, now) != MOQ_OK)
            goto fail;
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(sv); drain_events(cl);
    {
        moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
        sgc.group_id = group; sgc.subgroup_id = 0;
        sgc.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        if (moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now, &sg)
            != MOQ_OK) goto fail;
        if (with_object) {
            uint8_t d[32];
            memset(d, 0x42, sizeof(d));
            if (payload_len == 0 || payload_len > sizeof(d)) goto fail;
            moq_rcbuf_t *pay = NULL;
            if (moq_rcbuf_create(&alloc, d, payload_len, &pay) != MOQ_OK)
                goto fail;
            moq_result_t wrc = moq_session_write_object(sv, sg, 0, pay, now);
            moq_rcbuf_decref(pay);
            if (wrc != MOQ_OK) goto fail;
        }
        if (moq_session_close_subgroup(sv, sg, now) != MOQ_OK) goto fail;
    }
    {
        moq_action_t acts[8]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA && out->n < 8) {
                    cap_chunk_t *ck = &out->c[out->n++];
                    ck->len = 0; ck->fin = acts[i].u.send_data.fin;
                    if (acts[i].u.send_data.header_len > 0) {
                        memcpy(ck->bytes, acts[i].u.send_data.header,
                               acts[i].u.send_data.header_len);
                        ck->len = acts[i].u.send_data.header_len;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(ck->bytes + ck->len,
                               moq_rcbuf_data(acts[i].u.send_data.payload), pl);
                        ck->len += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
    }
    moq_simpair_destroy(sp);
    return out->n > 0 && as.balance == 0;
fail:
    moq_simpair_destroy(sp);
    return false;
}

static bool capture_pub_stream_g(moq_version_t ver, uint64_t alias,
                                 uint64_t group, bool with_object,
                                 cap_stream_t *out)
{
    return capture_pub_stream_gp(ver, alias, group, with_object, 1, out);
}

static bool capture_pub_stream(moq_version_t ver, uint64_t alias,
                               bool with_object, cap_stream_t *out)
{
    return capture_pub_stream_g(ver, alias, 0, with_object, out);
}

/* Flatten captured chunks into one contiguous wire buffer (for byte-level
 * split feeds in the frozen-mid-object tests). */
static size_t cap_flatten(const cap_stream_t *cs, uint8_t *buf, size_t cap)
{
    size_t n = 0;
    for (size_t i = 0; i < cs->n; i++) {
        if (n + cs->c[i].len > cap) return 0;
        memcpy(buf + n, cs->c[i].bytes, cs->c[i].len);
        n += cs->c[i].len;
    }
    return n;
}

/* Real subscription-bound subgroup wire bytes for a PINNED alias, captured
 * from a throwaway subscribe pair (server accepts with the alias, opens one
 * subgroup, writes one object, FINs). */
static bool capture_sub_stream(moq_version_t ver, uint64_t alias,
                               cap_stream_t *out)
{
    out->n = 0;
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    if (!sp) return false;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t cl_sub;
    if (moq_session_subscribe(cl, &sc, now, &cl_sub) != MOQ_OK) goto fail;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    {
        moq_subscription_t sv_sub; bool got = false;
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                sv_sub = e.u.subscribe_request.sub; got = true;
            }
            moq_event_cleanup(&e);
        }
        if (!got) goto fail;
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = alias;
        if (moq_session_accept_subscribe(sv, sv_sub, &ac, now) != MOQ_OK)
            goto fail;
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(sv); drain_events(cl);
        moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(sv, sv_sub, &sgc, now, &sg) != MOQ_OK)
            goto fail;
        uint8_t d[] = { 0x51 };
        moq_rcbuf_t *pay = NULL;
        if (moq_rcbuf_create(&alloc, d, 1, &pay) != MOQ_OK) goto fail;
        moq_result_t wrc = moq_session_write_object(sv, sg, 0, pay, now);
        moq_rcbuf_decref(pay);
        if (wrc != MOQ_OK) goto fail;
        if (moq_session_close_subgroup(sv, sg, now) != MOQ_OK) goto fail;
    }
    {
        moq_action_t acts[8]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_SEND_DATA && out->n < 8) {
                    cap_chunk_t *ck = &out->c[out->n++];
                    ck->len = 0; ck->fin = acts[i].u.send_data.fin;
                    if (acts[i].u.send_data.header_len > 0) {
                        memcpy(ck->bytes, acts[i].u.send_data.header,
                               acts[i].u.send_data.header_len);
                        ck->len = acts[i].u.send_data.header_len;
                    }
                    if (acts[i].u.send_data.payload) {
                        size_t pl = moq_rcbuf_len(acts[i].u.send_data.payload);
                        memcpy(ck->bytes + ck->len,
                               moq_rcbuf_data(acts[i].u.send_data.payload),
                               pl);
                        ck->len += pl;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }
    }
    moq_simpair_destroy(sp);
    return out->n > 0 && as.balance == 0;
fail:
    moq_simpair_destroy(sp);
    return false;
}

/* Feed captured stream chunks; with_fin=false suppresses the trailing FIN so
 * the stream stays open (FIN it later with an empty on_data_bytes). */
static void feed_cap_stream(moq_session_t *cl, moq_stream_ref_t ref,
                            const cap_stream_t *cs, bool with_fin,
                            uint64_t now)
{
    for (size_t i = 0; i < cs->n; i++)
        (void)moq_session_on_data_bytes(cl, ref,
            cs->c[i].len ? cs->c[i].bytes : NULL, cs->c[i].len,
            with_fin && cs->c[i].fin, now);
}

/* Feed only the FIRST captured chunk (the subgroup header): the stream
 * binds and stays live with no further delivery -- once the session issues
 * STOP for it, a compliant adapter delivers nothing more, so tests must
 * not hand-feed trailing bytes past a stop. */
static void feed_cap_header(moq_session_t *cl, moq_stream_ref_t ref,
                            const cap_stream_t *cs, uint64_t now)
{
    if (cs->n > 0 && cs->c[0].len)
        (void)moq_session_on_data_bytes(cl, ref, cs->c[0].bytes,
                                        cs->c[0].len, false, now);
}

/* Server sends PUBLISH (pinned alias, given Forward); client does NOT
 * respond -- the subscription stays in spec state Pending (Publisher). */
static bool sc_publish_pending(moq_simpair_t *sp, uint64_t alias,
                               bool forward, moq_publication_t *sv_pub,
                               moq_publication_t *cl_pub)
{
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("t");
    pc.has_forward = true; pc.forward = forward;
    pc.has_track_alias = true;
    pc.track_alias = alias;
    if (moq_session_publish(sv, &pc, now, sv_pub) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    bool got = false;
    moq_event_t e;
    while (moq_session_poll_events(cl, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            *cl_pub = e.u.publish_request.pub; got = true;
        }
        moq_event_cleanup(&e);
    }
    return got;
}

/* Craft an inbound done for the client's subscriber-role publication (the
 * mirror of sub_feed_done_raw; closes stolen + counted the same way). */
static moq_result_t pub_feed_done(moq_simpair_t *sp, moq_version_t ver,
                                  moq_publication_t cl_pub, uint64_t status,
                                  uint64_t count, const char *reason,
                                  bool fin)
{
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    size_t rlen = reason ? strlen(reason) : 0;
    uint8_t buf[128]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    int slot = pub_resolve_handle(cl, cl_pub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    g_feed_close_n = 0;
    moq_result_t rc;
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_bytes_t rb = { (const uint8_t *)reason, rlen };
        MOQ_TEST_CHECK(moq_d18_encode_publish_done(&w, status, count, rb)
                       == MOQ_OK);
        moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;
        MOQ_TEST_CHECK(ref._v != 0);
        rc = moq_session_on_bidi_stream_bytes(cl, ref, buf,
            moq_buf_writer_offset(&w), fin, now);
        moq_action_t acts[4]; size_t na;
        while ((na = moq_session_poll_actions(cl, acts, 4)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind == MOQ_ACTION_CLOSE_BIDI_STREAM)
                    g_feed_close_n++;
                else if (moq_session_state(cl) != MOQ_SESS_CLOSED)
                    MOQ_TEST_CHECK(acts[i].kind
                                   == MOQ_ACTION_CLOSE_BIDI_STREAM);
                moq_action_cleanup(&acts[i]);
            }
    } else {
        MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w,
            cl->publishes[slot].request_id, status, count,
            (const uint8_t *)reason, rlen) == MOQ_OK);
        rc = moq_session_on_control_bytes(cl, buf,
            moq_buf_writer_offset(&w), now);
    }
    return rc;
}

/* -- §5.1: publisher termination from Pending (Publisher) is accepted -- */
static void test_pub_pending_done(moq_version_t ver, bool forward)
{
    cap_stream_t cs = {0};
    if (forward)
        MOQ_TEST_CHECK(capture_pub_stream(ver, 0x7A, true, &cs));

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x7A, forward, &sv_pub, &cl_pub));
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0xEA57);
    sub_evstats_t st = {0};

    if (forward) {
        /* Early data pre-accept (§5.1: publishers MAY send Objects before
         * PUBLISH_OK): binds via the pending alias and delivers. The stream
         * is left OPEN so the done must defer. */
        feed_cap_stream(cl, dref, &cs, false, now);
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.obj_n, 1);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);

        MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 1, "bye", false)
                       == MOQ_OK);
        if (ver == MOQ_VERSION_DRAFT_18)
            MOQ_TEST_CHECK_EQ_INT(g_feed_close_n, 1);
        memset(&st, 0, sizeof(st));
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);                 /* deferred */
        {
            int slot = pub_resolve_handle(cl, cl_pub);
            MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].done_pending);
        }
        /* Logically terminal: a response may no longer be sent. */
        {
            moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
            MOQ_TEST_CHECK(moq_session_accept_publish(cl, cl_pub, &ac, now)
                           == MOQ_ERR_WRONG_STATE);
            moq_reject_publish_cfg_t rj; moq_reject_publish_cfg_init(&rj);
            MOQ_TEST_CHECK(moq_session_reject_publish(cl, cl_pub, &rj, now)
                           == MOQ_ERR_WRONG_STATE);
        }
        /* The advertised stream completes: FINISHED exactly once. */
        MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, dref, NULL, 0, true, now)
                       == MOQ_OK);
        memset(&st, 0, sizeof(st));
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st.done_status, 0x2);
        MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)3);
        MOQ_TEST_CHECK(memcmp(st.reason, "bye", 3) == 0);
        MOQ_TEST_CHECK(pub_resolve_handle(cl, cl_pub) < 0);  /* freed */
        assert_no_close_actions(cl);
    } else {
        /* Forward 0 covered by test_pub_pending_f0 (empty-subgroup matrix). */
        (void)dref;
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- Forward=0 pending: EMPTY subgroups still bind and count ---------- */
/* Forward State 0 prohibits Objects, not streams: draft-18 §10.11 counts
 * empty subgroups toward Stream Count, so a Forward-0 publication's finite
 * count IS satisfiable and must gate normally in both orders. */
static void test_pub_pending_f0(moq_version_t ver, bool done_first)
{
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_pub_stream(ver, 0x7B, false /* EMPTY */, &cs));

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x7B, false /* Forward 0 */,
                                      &sv_pub, &cl_pub));
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0xF057);
    sub_evstats_t st = {0};

    if (done_first) {
        /* Done (count 1) before the stream: DEFERRED -- the empty subgroup
         * still binds and its FIN satisfies the count. */
        MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 1, NULL, false)
                       == MOQ_OK);
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);                 /* deferred */
        {
            int slot = pub_resolve_handle(cl, cl_pub);
            MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].done_pending);
        }
        feed_cap_stream(cl, dref, &cs, true /* with FIN */, now);
        memset(&st, 0, sizeof(st));
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);                  /* empty */
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
    } else {
        /* Stream first: the empty subgroup binds while pending and its FIN
         * banks the count; the done then finalizes immediately. */
        feed_cap_stream(cl, dref, &cs, true, now);
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);
        {
            int slot = pub_resolve_handle(cl, cl_pub);
            MOQ_TEST_CHECK(slot >= 0 &&
                cl->publishes[slot].processed_stream_count == 1);
        }
        MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 1, NULL, false)
                       == MOQ_OK);
        memset(&st, 0, sizeof(st));
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
        if (ver == MOQ_VERSION_DRAFT_18) {
            /* Immediate path drains the bidi: absorb the FIN to free. */
            int slot = pub_resolve_handle(cl, cl_pub);
            if (slot >= 0) {
                moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;
                MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref,
                    NULL, 0, true, now) == MOQ_OK);
            }
        }
    }
    MOQ_TEST_CHECK(pub_resolve_handle(cl, cl_pub) < 0);      /* freed */
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- §5.1: PUBLISH_DONE on a pending SUBSCRIBE is a violation ---------- */
static void test_sub_pending_done_rejected(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(moq_simpair_server(sp)); drain_events(cl);

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t cl_sub;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now, &cl_sub) == MOQ_OK);
    /* The peer never responds: the subscription stays Pending (Subscriber).
     * A publisher may terminate only Pending (Publisher) or Established
     * (§5.1) -- the first response to a SUBSCRIBE must be SUBSCRIBE_OK or
     * REQUEST_ERROR, so a PUBLISH_DONE here is a protocol violation. */
    uint8_t buf[128];
    size_t len = sub_encode_done(cl, ver, cl_sub, 0x2, 1, NULL,
                                 buf, sizeof(buf));
    MOQ_TEST_CHECK(len > 0);
    if (ver == MOQ_VERSION_DRAFT_18) {
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0);
        moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
        (void)moq_session_on_bidi_stream_bytes(cl, ref, buf, len, false, now);
    } else {
        (void)moq_session_on_control_bytes(cl, buf, len, now);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- logical-terminal guards: subscription family --------------------- */
static void test_sub_terminal_guards(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    MOQ_TEST_CHECK_EQ_INT(g_feed_close_n,
                          ver == MOQ_VERSION_DRAFT_18 ? 1 : 0);

    /* No control may follow the accepted terminal. */
    {
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = true;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc, now)
                       == MOQ_ERR_WRONG_STATE);
    }
    MOQ_TEST_CHECK(moq_session_unsubscribe(cl, cl_sub, now)
                   == MOQ_ERR_WRONG_STATE);
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_request_goaway_cfg_t gc; moq_request_goaway_cfg_init(&gc);
        MOQ_TEST_CHECK(moq_session_request_goaway_subscribe(cl, cl_sub,
            &gc, now) == MOQ_ERR_WRONG_STATE);
    }
    {
        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true; fc.joining_relative = true;
        fc.joining_sub = cl_sub; fc.joining_start = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(cl, &fc, now, &fh)
                       == MOQ_ERR_WRONG_STATE);
    }
    /* Still deferred and still alias-bound after the refused operations. */
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    assert_no_close_actions(cl);

    /* The gate still resolves normally. */
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- duplicate terminal while deferred = protocol violation (d16) ------ */
static void test_d16_duplicate_done_closes(bool publication)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_16, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    if (publication) {
        moq_publication_t sv_pub, cl_pub;
        MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));
        moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now,
                                                     &sg) == MOQ_OK);
        uint8_t d[] = { 0x99 };
        moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, 1, &pay);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, pay, now)
                       == MOQ_OK);
        moq_rcbuf_decref(pay);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        MOQ_TEST_CHECK(pub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_pub,
            0x2, 1, "one", false) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_finished(cl, NULL), 0);   /* deferred */
        /* A second terminal for the same request: violation, and the
         * retained status/count/reason must NOT be replaced. */
        (void)pub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_pub,
            0x4, 9, "two", false);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);
    } else {
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_sub,
            0x2, 1, "one", false) == MOQ_OK);
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
        (void)sub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_sub,
            0x4, 9, "two", false);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);
    }

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- logical-terminal guards: publication family ----------------------- */
static void test_pub_terminal_guards(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));

    /* One live stream keeps the crafted done (count 1) deferred. */
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now, &sg)
                   == MOQ_OK);
    uint8_t d[] = { 0x77 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, 1, &pay);
    MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, pay, now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    if (ver == MOQ_VERSION_DRAFT_18)
        MOQ_TEST_CHECK_EQ_INT(g_feed_close_n, 1);
    MOQ_TEST_CHECK_EQ_INT(count_finished(cl, NULL), 0);       /* deferred */

    /* No control may follow the accepted terminal. */
    {
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = true;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc, now)
                       == MOQ_ERR_WRONG_STATE);
    }
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_request_goaway_cfg_t gc; moq_request_goaway_cfg_init(&gc);
        MOQ_TEST_CHECK(moq_session_request_goaway_publish(cl, cl_pub,
            &gc, now) == MOQ_ERR_WRONG_STATE);
    }
    assert_no_close_actions(cl);

    /* The gate still resolves normally. */
    MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    uint64_t cnt = 0;
    MOQ_TEST_CHECK_EQ_INT(count_finished(cl, &cnt), 1);
    MOQ_TEST_CHECK_EQ_U64(cnt, 1);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- d16: a late update ack crossing the terminal is absorbed ---------- */
static void test_d16_late_update_ack(bool deferred)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_16, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    moq_subgroup_handle_t sg;
    if (deferred) {
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
    }

    /* Send an update but NEVER deliver it (steal the control action): the
     * update stays pending at the client while the peer's done crosses it
     * in flight. */
    uint64_t update_id;
    {
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = true;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc, now)
                       == MOQ_OK);
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0 && cl->subs[slot].update_pending);
        update_id = cl->subs[slot].update_request_id;
        moq_action_t a;
        while (moq_session_poll_actions(cl, &a, 1) > 0) moq_action_cleanup(&a);
    }

    if (deferred) {
        MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_sub,
            0x2, 1, NULL, false) == MOQ_OK);
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    } else {
        /* Count 0 finalizes immediately (entry freed). */
        MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_sub,
            0x2, 0, NULL, false) == MOQ_OK);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);
    }

    /* The publisher's ack for the crossed update arrives AFTER its done:
     * it must be absorbed -- no session close, no update-ok event. */
    {
        uint8_t buf[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, update_id, NULL, 0)
                       == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, buf,
            moq_buf_writer_offset(&w), now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            MOQ_TEST_CHECK(e.kind != MOQ_EVENT_SUBSCRIPTION_UPDATE_OK);
            moq_event_cleanup(&e);
        }
    }

    if (deferred) {
        /* The gate still resolves. */
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- d18: defer under action-queue pressure = retryable no-mutation ---- */
static void test_sub_d18_defer_action_backpressure(void)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);

    /* Full action queue: the close-half cannot be reserved, so the defer
     * must WOULD_BLOCK without mutating anything. */
    test_session_fill_action_queue(cl);
    uint8_t buf[128];
    size_t len = sub_encode_done(cl, MOQ_VERSION_DRAFT_18, cl_sub,
                                 0x2, 1, "gate", buf, sizeof(buf));
    int slot = sub_resolve_handle(cl, cl_sub);
    MOQ_TEST_CHECK(slot >= 0);
    moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, buf, len,
        false, now) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!cl_sub_done_pending(cl, cl_sub));
    MOQ_TEST_CHECK(cl->subs[slot].state == MOQ_SUB_ESTABLISHED);
    {
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);
    }
    /* Drain the fillers (none may be a close-half: nothing was accepted). */
    {
        moq_action_t a;
        while (moq_session_poll_actions(cl, &a, 1) > 0) {
            MOQ_TEST_CHECK(a.kind != MOQ_ACTION_CLOSE_BIDI_STREAM);
            moq_action_cleanup(&a);
        }
    }
    /* Empty re-drive of the retained terminal: defers, closing the send
     * half EXACTLY once. */
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, NULL, 0,
        false, now) == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    {
        int ncl = 0; moq_action_t a;
        while (moq_session_poll_actions(cl, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM) ncl++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(ncl, 1);
    }
    /* The gate resolves; no further close. Steal the server's FIN action
     * (SimPair remaps stream refs, so replaying it under the server-side ref
     * would open a phantom stream) and deliver the FIN on the client's
     * actual live rx ref instead. */
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    {
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }
    {
        moq_stream_ref_t rref = moq_stream_ref_from_u64(0);
        for (size_t i = 0; i < cl->rx_cap; i++)
            if (cl->rx_streams[i].active) rref = cl->rx_streams[i].stream_ref;
        MOQ_TEST_CHECK(rref._v != 0);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, rref, NULL, 0, true, now)
                       == MOQ_OK);
    }
    {
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK(memcmp(st.reason, "gate", 4) == 0);
    }
    assert_no_close_actions(cl);
    MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) >= 0);  /* TERMINATED drain */

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- the retained reason is a deep copy (source poisoned) -------------- */
static void test_sub_reason_deep_copy(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    uint8_t wire[128];
    size_t len = sub_encode_done(cl, ver, cl_sub, 0x2, 1, "keepsake",
                                 wire, sizeof(wire));
    MOQ_TEST_CHECK(len > 0);
    MOQ_TEST_CHECK(sub_feed_done_raw(sp, ver, cl_sub, wire, len, false)
                   == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    /* Poison the fed source bytes: the deferral must have taken an OWNED
     * copy of the reason, not a borrow. */
    memset(wire, 0xEE, sizeof(wire));

    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)8);
    MOQ_TEST_CHECK(memcmp(st.reason, "keepsake", 8) == 0);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- d18: repeated crossed-update terminals must not consume tombstones -- */
/* Draft-18 correlates updates by request stream: an ack can never arrive
 * after the done (they share the publisher's half of the bidi, and the done
 * is followed by FIN), so terminal commit must NOT spend an id tombstone --
 * with one spent per lifecycle, repeated lifecycles would exhaust the pool.
 * One lifecycle per pair (a withheld update id would otherwise trip the
 * next request's strict id sequencing). */
static void test_d18_update_tomb_churn(bool publication)
{
    for (int k = 0; k < 3; k++) {
        sc_alloc_state_t as = {0};
        moq_alloc_t alloc = sc_allocator(&as);
        moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        uint64_t now = moq_simpair_now_us(sp);
        if (publication) {
            moq_publication_t sv_pub, cl_pub;
            MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));
            {
                moq_publication_update_cfg_t uc;
                moq_publication_update_cfg_init(&uc);
                uc.has_forward = true; uc.forward = true;
                MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub,
                    &uc, now) == MOQ_OK);
                moq_action_t a;                    /* update in flight */
                while (moq_session_poll_actions(cl, &a, 1) > 0)
                    moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK(pub_feed_done(sp, MOQ_VERSION_DRAFT_18, cl_pub,
                0x2, 0, NULL, true /* FIN with the done */) == MOQ_OK);
            drain_events(cl);
            MOQ_TEST_CHECK(pub_resolve_handle(cl, cl_pub) < 0);
        } else {
            moq_subscription_t cl_sub, sv_sub;
            MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
            {
                moq_subscription_update_cfg_t uc;
                moq_subscription_update_cfg_init(&uc);
                uc.has_forward = true; uc.forward = true;
                MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub,
                    &uc, now) == MOQ_OK);
                moq_action_t a;
                while (moq_session_poll_actions(cl, &a, 1) > 0)
                    moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_18, cl_sub,
                0x2, 0, NULL, true) == MOQ_OK);
            drain_events(cl);
            MOQ_TEST_CHECK(sub_resolve_handle(cl, cl_sub) < 0);
        }
        MOQ_TEST_CHECK_EQ_SIZE(cl->unsub_tomb_count, (size_t)0);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* -- d16: exactly one late ack absorbed; a duplicate is a violation ---- */
static void test_d16_dup_ack_closes(bool publication)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_16, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    uint64_t update_id = 0;

    if (publication) {
        moq_publication_t sv_pub, cl_pub;
        MOQ_TEST_CHECK(sc_establish(sp, &sv_pub, &cl_pub));
        moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_pub_subgroup(sv, sv_pub, &sgc, now,
                                                     &sg) == MOQ_OK);
        uint8_t d[] = { 0x21 };
        moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, 1, &pay);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, 0, pay, now)
                       == MOQ_OK);
        moq_rcbuf_decref(pay);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        {
            moq_publication_update_cfg_t uc;
            moq_publication_update_cfg_init(&uc);
            uc.has_forward = true; uc.forward = true;
            MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc,
                now) == MOQ_OK);
            int slot = pub_resolve_handle(cl, cl_pub);
            MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].update_pending);
            update_id = cl->publishes[slot].update_request_id;
            moq_action_t a;
            while (moq_session_poll_actions(cl, &a, 1) > 0)
                moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(pub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_pub,
            0x2, 1, NULL, false) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_finished(cl, NULL), 0);   /* deferred */
    } else {
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl);
        {
            moq_subscription_update_cfg_t uc;
            moq_subscription_update_cfg_init(&uc);
            uc.has_forward = true; uc.forward = true;
            MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc,
                now) == MOQ_OK);
            int slot = sub_resolve_handle(cl, cl_sub);
            MOQ_TEST_CHECK(slot >= 0 && cl->subs[slot].update_pending);
            update_id = cl->subs[slot].update_request_id;
            moq_action_t a;
            while (moq_session_poll_actions(cl, &a, 1) > 0)
                moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(sub_feed_done(sp, MOQ_VERSION_DRAFT_16, cl_sub,
            0x2, 1, NULL, false) == MOQ_OK);
        MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    }

    /* Ack #1: the publisher's mandatory single response -- absorbed. */
    uint8_t buf[64]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, update_id, NULL, 0)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, buf,
        moq_buf_writer_offset(&w), now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    {
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            MOQ_TEST_CHECK(e.kind != MOQ_EVENT_SUBSCRIPTION_UPDATE_OK &&
                           e.kind != MOQ_EVENT_PUBLICATION_UPDATE_OK);
            moq_event_cleanup(&e);
        }
    }
    /* Ack #2: exactly-one-response violated -- unknown request, close. */
    (void)moq_session_on_control_bytes(cl, buf, moq_buf_writer_offset(&w),
                                       now);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    (void)sv;
}

/* ==================================================================== */
/* Forward=0 object suppression (bind + count, never deliver) */
/* ==================================================================== */

/* Accept the pending publication with an explicit Forward State. */
static bool pub_accept_fwd(moq_simpair_t *sp, moq_publication_t cl_pub,
                           bool forward)
{
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    ac.has_forward = true; ac.forward = forward;
    if (moq_session_accept_publish(cl, cl_pub, &ac, now) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(moq_simpair_server(sp)); drain_events(cl);
    return true;
}

static uint64_t cl_pub_processed(moq_session_t *cl, moq_publication_t pub)
{
    int slot = pub_resolve_handle(cl, pub);
    return slot >= 0 ? cl->publishes[slot].processed_stream_count
                     : UINT64_MAX;
}

/* -- Forward=0 stream: binds, objects suppressed, terminal counts ------ */
static void test_fwd0_stream_suppressed(moq_version_t ver, bool established,
                                        bool streaming, bool use_reset)
{
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_pub_stream(ver, 0x7C, true /* object */, &cs));

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, streaming, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x7C, false /* Forward 0 */,
                                      &sv_pub, &cl_pub));
    if (established)
        MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, false));

    /* The object-carrying stream binds; its Objects are prohibited by
     * Forward State 0 and must be consumed WITHOUT object/chunk events. */
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0xF13D);
    feed_cap_stream(cl, dref, &cs, !use_reset /* FIN unless resetting */,
                    now);
    if (use_reset)
        MOQ_TEST_CHECK(moq_session_on_data_reset(cl, dref, 0, now)
                       == MOQ_OK);
    sub_evstats_t st = {0};
    pub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);            /* suppressed */
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);
    /* ...but the stream itself counted exactly once. */
    MOQ_TEST_CHECK_EQ_U64(cl_pub_processed(cl, cl_pub), 1);

    /* The banked count satisfies the done immediately. */
    MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    memset(&st, 0, sizeof(st));
    pub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_U64(st.done_count, 1);
    if (ver == MOQ_VERSION_DRAFT_18) {
        int slot = pub_resolve_handle(cl, cl_pub);
        if (slot >= 0) {
            moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, NULL,
                0, true, now) == MOQ_OK);
        }
    }
    MOQ_TEST_CHECK(pub_resolve_handle(cl, cl_pub) < 0);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- Forward=0 datagram: dropped, session healthy ---------------------- */
static void test_fwd0_datagram_suppressed(moq_version_t ver, bool established)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x7D, false, &sv_pub, &cl_pub));
    if (established)
        MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, false));

    uint8_t wire[64]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 0x7D; dg.group_id = 0; dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x"; dg.payload_len = 1;
        MOQ_TEST_CHECK(moq_d18_encode_object_datagram(&w, &dg) == MOQ_OK);
    } else {
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 0x7D; dg.group_id = 0; dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x"; dg.payload_len = 1;
        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_on_datagram(cl, wire,
        moq_buf_writer_offset(&w), now) == MOQ_OK);
    sub_evstats_t st = {0};
    pub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);            /* dropped */
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- Forward update latches at ACK; delivery follows the latch --------- */
static void test_fwd_update_latch(moq_version_t ver)
{
    cap_stream_t cs0 = {0}, cs1 = {0};
    MOQ_TEST_CHECK(capture_pub_stream_g(ver, 0x7E, 0, true, &cs0));
    MOQ_TEST_CHECK(capture_pub_stream_g(ver, 0x7E, 1, true, &cs1));

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x7E, true /* Forward 1 */,
                                      &sv_pub, &cl_pub));
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, true));

    /* Forward 1: objects deliver. */
    moq_stream_ref_t r0 = moq_stream_ref_from_u64(0xFA0);
    feed_cap_stream(cl, r0, &cs0, true, now);
    sub_evstats_t st = {0};
    pub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 1);
    MOQ_TEST_CHECK_EQ_U64(cl_pub_processed(cl, cl_pub), 1);

    /* Update Forward -> 0; the ACK latches the acknowledged state. */
    {
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(moq_simpair_server(sp));
    {
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 && !cl->publishes[slot].update_pending);
        MOQ_TEST_CHECK(slot >= 0 && !cl->publishes[slot].send_allowed);
    }

    /* Forward 0 acknowledged: the next stream's objects are suppressed,
     * but the stream still binds and counts. */
    moq_stream_ref_t r1 = moq_stream_ref_from_u64(0xFA1);
    feed_cap_stream(cl, r1, &cs1, true, now);
    memset(&st, 0, sizeof(st));
    pub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);
    MOQ_TEST_CHECK_EQ_U64(cl_pub_processed(cl, cl_pub), 2);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ==================================================================== */
/* frozen per-object suppression + subscription symmetry      */
/* ==================================================================== */

/* Chunk-event tally for the frozen-mid-object matrix. */
typedef struct {
    int begin_n;
    int end_n;
    int obj_n;
    int upd_ok_n;
} chunk_stats_t;

static void chunk_poll_stats(moq_session_t *s, chunk_stats_t *st)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_OBJECT_CHUNK) {
            if (e.u.object_chunk.begin) st->begin_n++;
            if (e.u.object_chunk.end) st->end_n++;
        } else if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            st->obj_n++;
        } else if (e.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK ||
                   e.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) {
            st->upd_ok_n++;
        }
        moq_event_cleanup(&e);
    }
}

/* -- ordinary SUBSCRIBE Forward=0: streams bind+count, objects don't -- */
static void test_sub_fwd0_stream(moq_version_t ver, bool via_update)
{
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_sub_stream(ver, 0x88, &cs));

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    if (!via_update) { sc.has_forward = true; sc.forward = false; }
    moq_subscription_t cl_sub;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now, &cl_sub) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_subscription_t sv_sub; bool got = false;
    {
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                sv_sub = e.u.subscribe_request.sub; got = true;
            }
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK(got);
    {
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 0x88;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub, &ac, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(sv); drain_events(cl);

    if (via_update) {
        /* Forward 1 -> 0 via update; the ACK latches the paused state. */
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl); drain_events(sv);
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0 && !cl->subs[slot].update_pending);
        MOQ_TEST_CHECK(slot >= 0 && !cl->subs[slot].forward);
    }

    /* A stream the publisher (wrongly, at Forward 0) sends: binds, counts,
     * delivers nothing. */
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0x5B0);
    feed_cap_stream(cl, dref, &cs, true, now);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);            /* suppressed */
    MOQ_TEST_CHECK_EQ_U64(cl_sub_processed(cl, cl_sub), 1);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- ordinary SUBSCRIBE Forward=0: datagram objects dropped ------------ */
static void test_sub_fwd0_datagram(moq_version_t ver, bool via_update)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    if (!via_update) { sc.has_forward = true; sc.forward = false; }
    moq_subscription_t cl_sub;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now, &cl_sub) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_subscription_t sv_sub; bool got = false;
    {
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                sv_sub = e.u.subscribe_request.sub; got = true;
            }
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK(got);
    {
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 0x89;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub, &ac, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(sv); drain_events(cl);
    if (via_update) {
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl); drain_events(sv);
    }

    uint8_t wire[64]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 0x89; dg.group_id = 0; dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x"; dg.payload_len = 1;
        MOQ_TEST_CHECK(moq_d18_encode_object_datagram(&w, &dg) == MOQ_OK);
    } else {
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 0x89; dg.group_id = 0; dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x"; dg.payload_len = 1;
        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_on_datagram(cl, wire,
        moq_buf_writer_offset(&w), now) == MOQ_OK);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);            /* dropped */
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- suppression is FROZEN per object at header admission -------------- */
static void test_frozen_mid_object(moq_version_t ver, bool start_allowed)
{
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_pub_stream_gp(ver, 0x8A, 0, true, 8, &cs));
    uint8_t wire[256];
    size_t wlen = cap_flatten(&cs, wire, sizeof(wire));
    MOQ_TEST_CHECK(wlen > 8);

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    /* Streaming receive: objects surface as chunk sequences. */
    moq_simpair_t *sp = sc_pair(&alloc, ver, true, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x8A, true, &sv_pub, &cl_pub));
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, start_allowed));

    /* First half of the object (header + partial payload): the admission
     * decision is stamped HERE, from the Forward state of this moment. */
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0x5C0);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, dref, wire, wlen - 4,
        false, now) == MOQ_OK);
    chunk_stats_t st = {0};
    chunk_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.begin_n, start_allowed ? 1 : 0);

    /* Flip the Forward state mid-object (acked update). */
    {
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = !start_allowed;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    memset(&st, 0, sizeof(st));
    chunk_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.upd_ok_n, 1);
    {
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 &&
                       cl->publishes[slot].send_allowed == !start_allowed);
    }

    /* Rest of the object + FIN: the FROZEN decision must hold -- an allowed
     * object completes its chunk sequence; a suppressed one stays silent. */
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, dref, wire + (wlen - 4), 4,
        true, now) == MOQ_OK);
    memset(&st, 0, sizeof(st));
    chunk_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.end_n, start_allowed ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(st.begin_n, 0);   /* no orphan begin either way */
    MOQ_TEST_CHECK_EQ_U64(cl_pub_processed(cl, cl_pub), 1); /* counted */

    /* The NEXT object follows the new state: a second stream delivers when
     * the flip enabled forwarding, stays silent when it disabled it. */
    cap_stream_t cs2 = {0};
    MOQ_TEST_CHECK(capture_pub_stream_gp(ver, 0x8A, 1, true, 8, &cs2));
    moq_stream_ref_t dref2 = moq_stream_ref_from_u64(0x5C1);
    feed_cap_stream(cl, dref2, &cs2, true, now);
    memset(&st, 0, sizeof(st));
    chunk_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.begin_n, start_allowed ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(st.end_n, start_allowed ? 0 : 1);
    MOQ_TEST_CHECK_EQ_U64(cl_pub_processed(cl, cl_pub), 2);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- frozen-allowed object under event backpressure -------------------- */
static void test_frozen_mid_object_backpressure(moq_version_t ver)
{
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_pub_stream_gp(ver, 0x8B, 0, true, 8, &cs));
    uint8_t wire[256];
    size_t wlen = cap_flatten(&cs, wire, sizeof(wire));
    MOQ_TEST_CHECK(wlen > 8);

    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    /* Streaming, tiny (cap 2) event queue. */
    moq_simpair_t *sp = sc_pair(&alloc, ver, true, 2);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x8B, true, &sv_pub, &cl_pub));
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, true));

    /* Begin chunk queued (1/2); the acked flip's event fills the queue. */
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0x5D0);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, dref, wire, wlen - 4,
        false, now) == MOQ_OK);
    {
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_forward = true; uc.forward = false;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc, now)
                       == MOQ_OK);
    }
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    /* End chunk cannot be queued (queue full): WOULD_BLOCK, retained. */
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, dref, wire + (wlen - 4), 4,
        true, now) == MOQ_ERR_WOULD_BLOCK);
    chunk_stats_t st = {0};
    chunk_poll_stats(cl, &st);          /* drain: begin + update-ok */
    MOQ_TEST_CHECK_EQ_INT(st.begin_n, 1);
    MOQ_TEST_CHECK_EQ_INT(st.upd_ok_n, 1);

    /* Re-drive: the FROZEN allowed decision still emits the end chunk even
     * though Forward is now 0. */
    MOQ_TEST_CHECK(moq_session_on_data_bytes(cl, dref, NULL, 0, false, now)
                   == MOQ_OK);
    memset(&st, 0, sizeof(st));
    chunk_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.end_n, 1);
    MOQ_TEST_CHECK_EQ_U64(cl_pub_processed(cl, cl_pub), 1);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- Forward-0 publication datagrams: dropped at delivery, never staged,
 * history still advances () ----------------------------------- */
static void test_pub_fwd0_datagram_no_staging(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x90, false, &sv_pub, &cl_pub));
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, false));

    /* An UNRELATED pending forwarding subscription arms the staging ring:
     * a paused publication's datagram must still resolve its OWN alias --
     * dropped at delivery, never mistaken for unknown-alias data. */
    moq_subscription_t cl_sub2;
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ parts, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("u");
        MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now, &cl_sub2)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        /* Server app does NOT accept yet: the subscription stays pending. */
    }

    uint8_t wire[64]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 0x90; dg.group_id = 5; dg.object_id = 7;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x"; dg.payload_len = 1;
        MOQ_TEST_CHECK(moq_d18_encode_object_datagram(&w, &dg) == MOQ_OK);
    } else {
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 0x90; dg.group_id = 5; dg.object_id = 7;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"x"; dg.payload_len = 1;
        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_on_datagram(cl, wire,
        moq_buf_writer_offset(&w), now) == MOQ_OK);

    /* Dropped at delivery: no event, NOTHING staged, and the history merge
     * happened at admission (Largest advances on receipt). */
    {
        sub_evstats_t st = {0};
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(cl->staged_count, (size_t)0);
    {
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].hist != NULL &&
                       cl->publishes[slot].hist->has_largest &&
                       cl->publishes[slot].hist->largest_group == 5 &&
                       cl->publishes[slot].hist->largest_object == 7);
    }

    /* No later resurrection: establishing the unrelated subscription must
     * not replay the dropped datagram. */
    {
        moq_subscription_t sv_sub2; bool got = false;
        moq_event_t e;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                sv_sub2 = e.u.subscribe_request.sub; got = true;
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true; ac.track_alias = 0xB0;
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub2, &ac, now)
                       == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        sub_evstats_t st = {0};
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.obj_n, 0);
        MOQ_TEST_CHECK_EQ_SIZE(cl->staged_count, (size_t)0);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ==================================================================== */
/* Per-type delivery-timeout state, scanner, negotiation ---------------- */
/* ==================================================================== */

#include "../../core/include/moq/kvp.h"

/* Build a property/extension blob with one or two even entries (ascending
 * types) and optionally an IMMUTABLE_PROPERTIES (0x0B) block wrapping
 * `imm`/`imm_len` (d18 only; 0x0B > 0x06 keeps ascent). Draft-16 Track
 * Extensions are QUIC-varint KVPs (the kvp codec); draft-18 Track
 * Properties are vi64 Type-Delta KVPs -- crafted manually here. */
static size_t dt_blob_v(moq_version_t ver, uint8_t *buf, size_t cap,
                        bool with_obj, uint64_t obj_ms,
                        bool with_sub, uint64_t sub_ms,
                        const uint8_t *imm, size_t imm_len)
{
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, cap);
        uint64_t prev = 0;
        if (with_obj) {
            if (moq_buf_write_vi64(&w, 0x02u - prev) < 0) return 0;
            if (moq_buf_write_vi64(&w, obj_ms) < 0) return 0;
            prev = 0x02u;
        }
        if (with_sub) {
            if (moq_buf_write_vi64(&w, 0x06u - prev) < 0) return 0;
            if (moq_buf_write_vi64(&w, sub_ms) < 0) return 0;
            prev = 0x06u;
        }
        if (imm && imm_len) {
            if (moq_buf_write_vi64(&w, 0x0Bu - prev) < 0) return 0;
            if (moq_buf_write_vi64(&w, imm_len) < 0) return 0;
            if (moq_buf_write_raw(&w, imm, imm_len) < 0) return 0;
        }
        return moq_buf_writer_offset(&w);
    }
    size_t off = 0; uint64_t prev = 0;
    if (with_obj) {
        size_t n = moq_kvp_encode_varint_entry(prev, 0x02u, obj_ms,
                                               buf + off, cap - off);
        if (!n) return 0;
        off += n; prev = 0x02u;
    }
    if (with_sub) {
        size_t n = moq_kvp_encode_varint_entry(prev, 0x06u, sub_ms,
                                               buf + off, cap - off);
        if (!n) return 0;
        off += n;
    }
    return off;
}

/* Subscribe (client) and return both handles WITHOUT the server accepting. */
static bool dt_subscribe_pending(moq_simpair_t *sp, const char *name,
                                 moq_subscription_t *cl_sub,
                                 moq_subscription_t *sv_sub)
{
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    if (moq_session_subscribe(cl, &sc, now, cl_sub) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    bool got = false; moq_event_t e;
    while (moq_session_poll_events(sv, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            *sv_sub = e.u.subscribe_request.sub; got = true;
        }
        moq_event_cleanup(&e);
    }
    return got;
}

static moq_result_t dt_accept_with_props(moq_simpair_t *sp,
                                         moq_subscription_t sv_sub,
                                         const uint8_t *props, size_t len)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){ props, len };
    return moq_session_accept_subscribe(sv, sv_sub, &ac, now);
}

/* -- local + inbound publisher-side extraction (incl. d18 nested) ----- */
static void test_dt_extraction(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(dt_subscribe_pending(sp, "t", &cl_sub, &sv_sub));

    uint8_t props[96]; size_t plen;
    if (ver == MOQ_VERSION_DRAFT_18) {
        /* object=1000 in the mutable list; subgroup=8000 NESTED inside
         * IMMUTABLE_PROPERTIES (§12.7: processors MUST search both). */
        uint8_t imm[32];
        size_t ilen = dt_blob_v(ver, imm, sizeof(imm), false, 0,
                                true, 8000, NULL, 0);
        MOQ_TEST_CHECK(ilen > 0);
        plen = dt_blob_v(ver, props, sizeof(props), true, 1000, false, 0,
                         imm, ilen);
    } else {
        /* d16: the single DELIVERY TIMEOUT extension (5000ms). */
        plen = dt_blob_v(ver, props, sizeof(props), true, 5000, false, 0,
                         NULL, 0);
    }
    MOQ_TEST_CHECK(plen > 0);
    MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, props, plen) == MOQ_OK);

    /* LOCAL extraction: the server's entry reflects its own emitted bytes. */
    {
        int slot = sub_resolve_handle(sv, sv_sub);
        MOQ_TEST_CHECK(slot >= 0);
        if (ver == MOQ_VERSION_DRAFT_18) {
            MOQ_TEST_CHECK(sv->subs[slot].dt_pub_has_object &&
                           sv->subs[slot].dt_pub_object_ms == 1000);
            MOQ_TEST_CHECK(sv->subs[slot].dt_pub_has_subgroup &&
                           sv->subs[slot].dt_pub_subgroup_ms == 8000);
        } else {
            MOQ_TEST_CHECK(sv->subs[slot].dt_pub_has_object &&
                           sv->subs[slot].dt_pub_object_ms == 5000);
            MOQ_TEST_CHECK(sv->subs[slot].dt_pub_has_subgroup &&
                           sv->subs[slot].dt_pub_subgroup_ms == 5000);
        }
    }
    /* INBOUND extraction: the client's entry reads the same values from
     * the SUBSCRIBE_OK Track Properties. */
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(sv);
    {
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0);
        uint64_t eo = ver == MOQ_VERSION_DRAFT_18 ? 1000 : 5000;
        uint64_t es = ver == MOQ_VERSION_DRAFT_18 ? 8000 : 5000;
        MOQ_TEST_CHECK(cl->subs[slot].dt_pub_has_object &&
                       cl->subs[slot].dt_pub_object_ms == eo);
        MOQ_TEST_CHECK(cl->subs[slot].dt_pub_has_subgroup &&
                       cl->subs[slot].dt_pub_subgroup_ms == es);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- malformed local properties: INVAL, no mutation, then success ----- */
static void test_dt_local_invalid(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(dt_subscribe_pending(sp, "t", &cl_sub, &sv_sub));

    uint8_t bad[96]; size_t blen;
    if (ver == MOQ_VERSION_DRAFT_18) {
        /* duplicate OBJECT timeout: 0x02 in the mutable list AND inside
         * IMMUTABLE_PROPERTIES. */
        uint8_t imm[32];
        size_t ilen = dt_blob_v(ver, imm, sizeof(imm), true, 700, false, 0,
                                NULL, 0);
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 1000, false, 0,
                         imm, ilen);
    } else {
        /* d16: zero timeout -- categorically illegal on the wire. */
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 0, false, 0,
                         NULL, 0);
    }
    MOQ_TEST_CHECK(blen > 0);
    uint64_t epoch_before = sv->borrow_epoch;
    MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, bad, blen)
                   == MOQ_ERR_INVAL);
    /* Transactional, and the scan is hoisted ABOVE session_begin_advance:
     * a malformed local call performs NO session mutation at all -- not
     * even a clock/borrow-epoch advance or a reap. */
    MOQ_TEST_CHECK_EQ_U64(sv->borrow_epoch, epoch_before);
    /* Still pending, no timeout state, and a subsequent VALID accept
     * succeeds. */
    {
        int slot = sub_resolve_handle(sv, sv_sub);
        MOQ_TEST_CHECK(slot >= 0 &&
                       sv->subs[slot].state == MOQ_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK(!sv->subs[slot].dt_pub_has_object &&
                       !sv->subs[slot].dt_pub_has_subgroup);
    }
    if (ver == MOQ_VERSION_DRAFT_18) {
        /* nested immutable-in-immutable is also INVAL. */
        uint8_t inner[16], mid[32], outer[64];
        size_t nlen = dt_blob_v(ver, inner, sizeof(inner), true, 5, false, 0,
                                NULL, 0);
        size_t mlen, olen;
        {   /* mid = IMMUTABLE(inner); outer = IMMUTABLE(mid) -- vi64 KVPs */
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, mid, sizeof(mid));
            MOQ_TEST_CHECK(moq_buf_write_vi64(&w, 0x0Bu) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_write_vi64(&w, nlen) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_write_raw(&w, inner, nlen) == MOQ_OK);
            mlen = moq_buf_writer_offset(&w);
            moq_buf_writer_init(&w, outer, sizeof(outer));
            MOQ_TEST_CHECK(moq_buf_write_vi64(&w, 0x0Bu) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_write_vi64(&w, mlen) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_write_raw(&w, mid, mlen) == MOQ_OK);
            olen = moq_buf_writer_offset(&w);
        }
        MOQ_TEST_CHECK(olen > 0);
        MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, outer, olen)
                       == MOQ_ERR_INVAL);
    } else {
        /* d16 structural garbage is INVAL on the LOCAL path (strict). */
        uint8_t junk[3] = { 0xC0, 0x01, 0x02 };   /* truncated 8-byte varint */
        MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, junk, sizeof(junk))
                       == MOQ_ERR_INVAL);
    }
    uint8_t good[32];
    size_t glen = dt_blob_v(ver, good, sizeof(good), true, 2000, false, 0,
                            NULL, 0);
    MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, good, glen) == MOQ_OK);
    {
        int slot = sub_resolve_handle(sv, sv_sub);
        MOQ_TEST_CHECK(slot >= 0 &&
                       sv->subs[slot].state == MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK(sv->subs[slot].dt_pub_has_object &&
                       sv->subs[slot].dt_pub_object_ms == 2000);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- subscriber-side update: pending -> latch on ACK / clear on reject - */
static void test_dt_update_latch(moq_version_t ver, bool reject)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));

    moq_subscription_update_cfg_t uc;
    moq_subscription_update_cfg_init(&uc);
    uc.has_delivery_timeout = true;
    uc.delivery_timeout_us = 3000000;   /* 3000ms on the wire */
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc, now)
                   == MOQ_OK);
    int slot = sub_resolve_handle(cl, cl_sub);
    MOQ_TEST_CHECK(slot >= 0);
    /* Pending, not yet latched. */
    MOQ_TEST_CHECK(cl->subs[slot].dt_upd_has_object &&
                   cl->subs[slot].dt_upd_object_ms == 3000);
    MOQ_TEST_CHECK(cl->subs[slot].dt_upd_has_subgroup &&
                   cl->subs[slot].dt_upd_subgroup_ms == 3000);
    MOQ_TEST_CHECK(!cl->subs[slot].dt_sub_has_object &&
                   !cl->subs[slot].dt_sub_has_subgroup);

    if (reject) {
        /* Steal the outbound update; craft a REQUEST_ERROR for it. */
        uint64_t uid = cl->subs[slot].update_request_id;
        { moq_action_t a;
          while (moq_session_poll_actions(cl, &a, 1) > 0)
              moq_action_cleanup(&a); }
        if (ver == MOQ_VERSION_DRAFT_16) {
            uint8_t buf[64]; moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK(moq_d16_encode_request_error(&w, uid, 0x0, 0,
                NULL, 0) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, buf,
                moq_buf_writer_offset(&w), now) == MOQ_OK);
            MOQ_TEST_CHECK(!cl->subs[slot].dt_upd_has_object &&
                           !cl->subs[slot].dt_upd_has_subgroup);
            MOQ_TEST_CHECK(!cl->subs[slot].dt_sub_has_object);
        }
        /* d18 rejection rides the bidi and mandates termination; the
         * clearing there is covered by the crossed-terminal retire test
         * machinery -- skip crafting here. */
    } else {
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        drain_events(cl); drain_events(moq_simpair_server(sp));
        MOQ_TEST_CHECK(!cl->subs[slot].dt_upd_has_object);
        MOQ_TEST_CHECK(cl->subs[slot].dt_sub_has_object &&
                       cl->subs[slot].dt_sub_object_ms == 3000);
        MOQ_TEST_CHECK(cl->subs[slot].dt_sub_has_subgroup &&
                       cl->subs[slot].dt_sub_subgroup_ms == 3000);
        /* Retained legacy projection recomputed from the complete pair. */
        MOQ_TEST_CHECK(cl->subs[slot].delivery_timeout_us == 3000000);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- the subgroup timer uses the exact NEGOTIATED SUBGROUP timeout ----- */
static void test_dt_subgroup_consumer(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(dt_subscribe_pending(sp, "t", &cl_sub, &sv_sub));

    uint8_t props[64]; size_t plen;
    if (ver == MOQ_VERSION_DRAFT_18)
        /* object=1000 SMALLER than subgroup=8000: the subgroup timer must
         * use 8000, never the min_nonzero legacy projection (1000). */
        plen = dt_blob_v(ver, props, sizeof(props), true, 1000, true, 8000,
                         NULL, 0);
    else
        plen = dt_blob_v(ver, props, sizeof(props), true, 8000, false, 0,
                         NULL, 0);
    MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, props, plen) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(sv);

    now = moq_simpair_now_us(sp);
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sgc, now, &sg)
                   == MOQ_OK);
    {
        bool found = false;
        for (size_t i = 0; i < sv->sg_cap; i++) {
            if (sv->subgroups[i].state == MOQ_SG_FREE) continue;
            MOQ_TEST_CHECK_EQ_U64(sv->subgroups[i].delivery_deadline_us,
                                  now + 8000ull * 1000ull);
            found = true;
        }
        MOQ_TEST_CHECK(found);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- d18 differing update values: accepted; event vs retained --------- */
static void test_dt_event_vs_retained_d18(void)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    int svslot = sub_resolve_handle(sv, sv_sub);
    MOQ_TEST_CHECK(svslot >= 0);
    moq_stream_ref_t ref = sv->subs[svslot].request_stream_ref;
    MOQ_TEST_CHECK(ref._v != 0);

    /* Update #1: DIFFERING object/subgroup values in one message --
     * today's mapper rejects this legal shape (session close). */
    {
        moq_d18_msg_params_t pms; memset(&pms, 0, sizeof(pms));
        pms.has_object_delivery_timeout = true;
        pms.object_delivery_timeout_ms = 2000;
        pms.has_subgroup_delivery_timeout = true;
        pms.subgroup_delivery_timeout_ms = 9000;
        uint8_t buf[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_request_update(&w, 2, &pms) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, buf,
            moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t e; bool saw = false;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                saw = true;
                MOQ_TEST_CHECK(e.u.subscribe_updated.has_delivery_timeout);
                /* Event projection: min_nonzero of THIS message. */
                MOQ_TEST_CHECK_EQ_U64(
                    e.u.subscribe_updated.delivery_timeout_us,
                    2000ull * 1000ull);
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK(saw);
        MOQ_TEST_CHECK(sv->subs[svslot].dt_sub_has_object &&
                       sv->subs[svslot].dt_sub_object_ms == 2000);
        MOQ_TEST_CHECK(sv->subs[svslot].dt_sub_has_subgroup &&
                       sv->subs[svslot].dt_sub_subgroup_ms == 9000);
        /* steal the auto-ack so the next crafted update correlates */
        moq_action_t a;
        while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a);
    }
    /* Update #2: SUBGROUP-ONLY (omission = unchanged for object). Event
     * projects only this message's carrier; retained legacy recomputes
     * from the complete pair (object 2000 still installed). */
    {
        moq_d18_msg_params_t pms; memset(&pms, 0, sizeof(pms));
        pms.has_subgroup_delivery_timeout = true;
        pms.subgroup_delivery_timeout_ms = 7000;
        uint8_t buf[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_request_update(&w, 4, &pms) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, buf,
            moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        moq_event_t e; bool saw = false;
        while (moq_session_poll_events(sv, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                saw = true;
                MOQ_TEST_CHECK(e.u.subscribe_updated.has_delivery_timeout);
                MOQ_TEST_CHECK_EQ_U64(
                    e.u.subscribe_updated.delivery_timeout_us,
                    7000ull * 1000ull);           /* message-only */
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK(saw);
        MOQ_TEST_CHECK(sv->subs[svslot].dt_sub_object_ms == 2000);   /* kept */
        MOQ_TEST_CHECK(sv->subs[svslot].dt_sub_subgroup_ms == 7000);
        /* Retained legacy = min_nonzero of the COMPLETE current pair. */
        MOQ_TEST_CHECK_EQ_U64(sv->subs[svslot].delivery_timeout_us,
                              2000ull * 1000ull);
    }
    (void)cl;
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- vi64-max saturation: exact ms retained; deadline never sentinel -- */
static void test_dt_saturation_d18(void)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now;
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(dt_subscribe_pending(sp, "t", &cl_sub, &sv_sub));
    uint8_t props[64];
    size_t plen = dt_blob_v(MOQ_VERSION_DRAFT_18, props, sizeof(props),
                            false, 0, true, MOQ_QUIC_VARINT_MAX, NULL, 0);
    MOQ_TEST_CHECK(plen > 0);
    MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, props, plen) == MOQ_OK);
    {
        int slot = sub_resolve_handle(sv, sv_sub);
        MOQ_TEST_CHECK(slot >= 0 &&
                       sv->subs[slot].dt_pub_subgroup_ms
                           == MOQ_QUIC_VARINT_MAX);   /* exact raw ms */
    }
    now = moq_simpair_now_us(sp);
    moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0; sgc.subgroup_id = 0; sgc.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sgc, now, &sg)
                   == MOQ_OK);
    for (size_t i = 0; i < sv->sg_cap; i++) {
        if (sv->subgroups[i].state == MOQ_SG_FREE) continue;
        /* Saturated duration -> finite deadline via deadline_add: never
         * the UINT64_MAX no-deadline sentinel. */
        MOQ_TEST_CHECK_EQ_U64(sv->subgroups[i].delivery_deadline_us,
                              UINT64_MAX - 1);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ==================================================================== */
/* Deadline-bounded terminal cleanup                                    */
/* ==================================================================== */

/* Pair with a custom terminal wait (µs) and optional tiny queues. */
static moq_simpair_t *ex_pair(moq_alloc_t *alloc, moq_version_t ver,
                              uint64_t wait_us, uint32_t max_actions,
                              uint32_t max_events)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 7; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    cfg.done_wait_timeout_us = wait_us;
    if (max_actions) cfg.max_actions = max_actions;
    if (max_events) cfg.max_events = max_events;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    return sp;
}

static int ex_count_stops(moq_session_t *s)
{
    int n = 0; moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_STOP_DATA) n++;
        moq_action_cleanup(&a);
    }
    return n;
}

/* -- cfg: default / custom / exact pre-append-size heap canary -------- */
static void test_ex_cfg(void)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    /* default (absent-field-equivalent: full size, value 0) */
    {
        moq_session_cfg_t c = MOQ_SESSION_CFG_INIT;
        c.alloc = &alloc; c.perspective = MOQ_PERSPECTIVE_CLIENT;
        c.version = MOQ_VERSION_DRAFT_16;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&c, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(s->done_wait_timeout_us,
                              30ull * 1000ull * 1000ull);
        moq_session_destroy(s);
    }
    /* custom */
    {
        moq_session_cfg_t c = MOQ_SESSION_CFG_INIT;
        c.alloc = &alloc; c.perspective = MOQ_PERSPECTIVE_CLIENT;
        c.version = MOQ_VERSION_DRAFT_16;
        c.done_wait_timeout_us = 5000000;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&c, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(s->done_wait_timeout_us, 5000000);
        moq_session_destroy(s);
    }
    /* EXACT pre-append-size heap canary: an old-size caller must cause no
     * tail read (ASan-guarded heap edge) and selects the new default. The
     * buffer stays a raw byte pointer, fields poked via memcpy at offsetof
     * (a typed deref of a short heap object trips gcc-15 array-bounds and
     * would also weaken what the canary proves). */
    {
        size_t old_size = offsetof(moq_session_cfg_t, done_wait_timeout_us);
        uint8_t *raw = (uint8_t *)malloc(old_size);
        MOQ_TEST_CHECK(raw != NULL);
        moq_session_cfg_t full = MOQ_SESSION_CFG_INIT;
        full.alloc = &alloc; full.perspective = MOQ_PERSPECTIVE_CLIENT;
        full.version = MOQ_VERSION_DRAFT_16;
        memcpy(raw, &full, old_size);
        uint32_t ssz = (uint32_t)old_size;
        memcpy(raw + offsetof(moq_session_cfg_t, struct_size), &ssz,
               sizeof(ssz));
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(
            (const moq_session_cfg_t *)(const void *)raw, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(s->done_wait_timeout_us,
                              30ull * 1000ull * 1000ull);
        moq_session_destroy(s);
        free(raw);
    }
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- one-stream UNCONGESTED expiry: STOP + terminal in the SAME pass -- */
static void test_ex_one_pass(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = ex_pair(&alloc, ver, 1000000 /* 1s */, 0, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 0, &sg));
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl);
    uint64_t now = moq_simpair_now_us(sp);
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 2, "exp", false)
                   == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(cl), now + 1000000);

    /* ONE tick past the deadline: the live stream is STOPPED and the
     * terminal surfaces in the SAME pass -- no second tick, no retry
     * deadline left armed. */
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 1000001) == MOQ_OK);
    sub_evstats_t st = {0};
    sub_poll_stats(cl, &st);
    MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)3);
    MOQ_TEST_CHECK(memcmp(st.reason, "exp", 3) == 0);
    MOQ_TEST_CHECK_EQ_INT(ex_count_stops(cl), 1);
    MOQ_TEST_CHECK_EQ_U64(cl->expiry_retry_deadline_us, UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- action-blocked expiry + count-satisfying FIN/RESET: no early
 * finalize; the reaper stops the rest first ---------------------------- */
static void test_ex_blocked_fin(moq_version_t ver, bool use_reset)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = ex_pair(&alloc, ver, 1000000, 0, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    /* Two live streams via captured wire bytes (real alias 1 -- the
     * subscribe pair assigns it deterministically). */
    cap_stream_t csA = {0}, csB = {0};
    MOQ_TEST_CHECK(capture_sub_stream(ver, 1, &csA));
    MOQ_TEST_CHECK(capture_sub_stream(ver, 1, &csB));
    moq_stream_ref_t rA = moq_stream_ref_from_u64(0xE0A);
    moq_stream_ref_t rB = moq_stream_ref_from_u64(0xE0B);
    uint64_t now = moq_simpair_now_us(sp);
    feed_cap_stream(cl, rA, &csA, false, now);   /* open, no FIN */
    feed_cap_stream(cl, rB, &csB, false, now);
    drain_events(cl);
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));

    /* Expire with the action queue full: stops are blocked. */
    test_session_fill_action_queue(cl);
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 1000001) == MOQ_OK);
    {
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);          /* blocked */
    }
    MOQ_TEST_CHECK(cl->expiry_retry_deadline_us != UINT64_MAX);

    /* A count-satisfying FIN (or identifiable RESET) arrives via input
     * processing: the count is RECORDED but finalization stays with the
     * reaper -- stream B must be stopped BEFORE any terminal. */
    if (use_reset) {
        MOQ_TEST_CHECK(moq_session_on_data_reset(cl, rA, 0, now + 1000002)
                       == MOQ_OK);
    } else {
        /* The expiry sweep may already have marked rA NEED_STOP (stops are
         * action-blocked): the FIN then rides the discard machinery and a
         * WOULD_BLOCK retry is legitimate -- either way NO terminal may
         * surface before the reaper stops the remaining stream. */
        moq_result_t frc = moq_session_on_data_bytes(cl, rA, NULL, 0, true,
                                                     now + 1000002);
        MOQ_TEST_CHECK(frc == MOQ_OK || frc == MOQ_ERR_WOULD_BLOCK);
    }
    {
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);          /* NO early finalize */
    }
    /* Drain the fillers; the bounded retry completes the expiry. */
    { moq_action_t a;
      while (moq_session_poll_actions(cl, &a, 1) > 0) moq_action_cleanup(&a); }
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 1200000) == MOQ_OK);
    {
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);          /* exactly once */
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- event-blocked finalize + late-binding stream: STOP precedes the
 * one terminal ---------------------------------------------------------- */
static void test_ex_event_blocked_late_bind(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = ex_pair(&alloc, ver, 1000000, 0, 2 /* tiny events */);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_sub_stream(ver, 1, &cs));
    uint64_t now = moq_simpair_now_us(sp);
    /* Count 2: the event-queue filler stream below contributes only 1, so
     * the gate is UNSATISFIED when the deadline fires -- expiry with zero
     * live streams attempts the finalize, which the full queue blocks. */
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 2, NULL, false)
                   == MOQ_OK);
    {
        moq_subgroup_handle_t sgf;
        MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 5, &sgf));
        moq_session_t *sv = moq_simpair_server(sp);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sgf, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
    }
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 1000001) == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));   /* still gated/blocked */
    MOQ_TEST_CHECK(cl->expiry_retry_deadline_us != UINT64_MAX);

    /* A LATE stream binds while the finalize is event-blocked. */
    moq_stream_ref_t rl = moq_stream_ref_from_u64(0xE1C);
    feed_cap_header(cl, rl, &cs, now + 1000002);

    /* Drain events; the retry pass must STOP the late stream BEFORE the
     * single terminal event. */
    drain_events(cl);
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 1200000) == MOQ_OK);
    {
        int stops = ex_count_stops(cl);
        MOQ_TEST_CHECK(stops >= 1);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- close matrix: GATED / EXPIRED-with-streams / EXPIRED-blocked ------- */
static void test_ex_close_matrix(moq_version_t ver, int phase)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = ex_pair(&alloc, ver, 1000000, 0,
                                phase == 2 ? 2 : 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_sub_stream(ver, 1, &cs));
    uint64_t now = moq_simpair_now_us(sp);
    moq_stream_ref_t r0 = moq_stream_ref_from_u64(0xE2A);
    feed_cap_stream(cl, r0, &cs, false, now);   /* live stream */
    drain_events(cl);
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 2, "cls", false)
                   == MOQ_OK);
    if (phase >= 1) {
        if (phase == 2) {
            /* consume event capacity so the finalize would block */
            moq_subgroup_handle_t sgf;
            MOQ_TEST_CHECK(sub_open_stream(sp, sv_sub, 6, &sgf));
            moq_session_t *sv = moq_simpair_server(sp);
            MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sgf, now)
                           == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
        }
        MOQ_TEST_CHECK(moq_session_tick(cl, now + 1000001) == MOQ_OK);
    }
    moq_session_on_transport_close(cl, 0, now + 1000002);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(cl), UINT64_MAX);
    { moq_action_t a;
      while (moq_session_poll_actions(cl, &a, 1) > 0) moq_action_cleanup(&a); }
    drain_events(cl);
    (void)moq_session_tick(cl, now + 3000000);
    MOQ_TEST_CHECK_EQ_INT(ex_count_stops(cl), 0);
    {
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 0);          /* no terminal after close */
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);   /* retained reason released once */
}

/* -- two blocked expiries ACROSS POOLS: one finalize never erases the
 * other's wake; ticks driven from next_deadline_us ---------------------- */
static void test_ex_two_entry_wake(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = ex_pair(&alloc, ver, 1000000, 0, 2);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    /* One SUBSCRIPTION + one PUBLICATION, both with gated terminals. */
    moq_subscription_t sA, svA;
    MOQ_TEST_CHECK(sc_sub_establish_named(sp, "tA", &sA, &svA));
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x9B, true, &sv_pub, &cl_pub));
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, true));
    uint64_t now = moq_simpair_now_us(sp);
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, sA, 0x2, 1, NULL, false) == MOQ_OK);
    MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    /* Fill the 2-slot event queue so BOTH finalizes block at expiry. */
    {
        moq_subgroup_handle_t sgf;
        MOQ_TEST_CHECK(sub_open_stream(sp, svA, 7, &sgf));
        moq_session_t *sv = moq_simpair_server(sp);
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sgf, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
    }
    /* Wake 1 (from the reported terminal deadline): both expire, both
     * block; the retry deadline must be armed. */
    uint64_t d1 = moq_session_next_deadline_us(cl);
    MOQ_TEST_CHECK(d1 != UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_tick(cl, d1 + 1) == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, sA));
    {
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].done_pending);
    }
    MOQ_TEST_CHECK(cl->expiry_retry_deadline_us != UINT64_MAX);

    /* Free exactly ONE event slot: on the next reported wake exactly one
     * entry finalizes. STRICT INVARIANT: as long as ANY entry remains
     * pending, the retry deadline stays finite. */
    {
        moq_event_t e;
        if (moq_session_poll_events(cl, &e, 1) == 1) moq_event_cleanup(&e);
    }
    int guard = 0;
    while (guard++ < 6 &&
           (cl_sub_done_pending(cl, sA) ||
            pub_resolve_handle(cl, cl_pub) >= 0)) {
        uint64_t d = moq_session_next_deadline_us(cl);
        MOQ_TEST_CHECK(d != UINT64_MAX);   /* pending => finite wake, always */
        MOQ_TEST_CHECK(moq_session_tick(cl, d + 1) == MOQ_OK);
        moq_event_t e;                     /* free at most one slot per wake */
        if (moq_session_poll_events(cl, &e, 1) == 1) moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK(!cl_sub_done_pending(cl, sA));
    MOQ_TEST_CHECK(pub_resolve_handle(cl, cl_pub) < 0);
    drain_events(cl);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- negotiated floor raises the wait; adapter-shaped wake loop -------- */
static void test_ex_floor_and_adapter_loop(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = ex_pair(&alloc, ver, 1000000 /* 1s cfg */, 0, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    /* Publisher advertises a 60s subgroup timeout: the FLOOR wins over the
     * 1s cfg. */
    moq_subscription_t cl_sub, sv_sub;
    MOQ_TEST_CHECK(dt_subscribe_pending(sp, "t", &cl_sub, &sv_sub));
    uint8_t props[64];
    size_t plen = dt_blob_v(ver, props, sizeof(props), false, 0, true,
                            60000, NULL, 0);
    if (ver != MOQ_VERSION_DRAFT_18)
        plen = dt_blob_v(ver, props, sizeof(props), true, 60000, false, 0,
                         NULL, 0);
    MOQ_TEST_CHECK(dt_accept_with_props(sp, sv_sub, props, plen) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    drain_events(cl); drain_events(sv);
    uint64_t now = moq_simpair_now_us(sp);
    MOQ_TEST_CHECK(sub_feed_done(sp, ver, cl_sub, 0x2, 1, NULL, false)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(cl),
                          now + 60000ull * 1000ull);
    /* Before the floor elapses: nothing fires. */
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 2000000) == MOQ_OK);
    MOQ_TEST_CHECK(cl_sub_done_pending(cl, cl_sub));
    /* Adapter-shaped wake loop: sleep-until-deadline -> tick -> poll. */
    int iters = 0; bool done = false;
    while (!done && iters++ < 4) {
        uint64_t d = moq_session_next_deadline_us(cl);
        MOQ_TEST_CHECK(d != UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_tick(cl, d + 1) == MOQ_OK);
        sub_evstats_t st = {0};
        sub_poll_stats(cl, &st);
        if (st.done_n) done = true;
        { moq_action_t a;
          while (moq_session_poll_actions(cl, &a, 1) > 0)
              moq_action_cleanup(&a); }
    }
    MOQ_TEST_CHECK(done);
    MOQ_TEST_CHECK_EQ_INT(iters, 1);   /* exactly one wake needed */
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ==================================================================== */
/* publication-path timeout matrix + malformed-PUBLISH cleanup */
/* ==================================================================== */

/* -- publication dt_* matrix: local/inbound PUBLISH, PUBLISH_OK values,
 * update ack/reject, and one publication expiry ------------------------ */
static void test_dt_pub_matrix(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);

    /* LOCAL PUBLISH extraction (publisher-role entry mirrors our bytes). */
    uint8_t props[96]; size_t plen;
    if (ver == MOQ_VERSION_DRAFT_18) {
        uint8_t imm[32];
        size_t ilen = dt_blob_v(ver, imm, sizeof(imm), false, 0, true, 8000,
                                NULL, 0);
        plen = dt_blob_v(ver, props, sizeof(props), true, 1000, false, 0,
                         imm, ilen);
    } else {
        plen = dt_blob_v(ver, props, sizeof(props), true, 5000, false, 0,
                         NULL, 0);
    }
    MOQ_TEST_CHECK(plen > 0);
    moq_publication_t sv_pub;
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("t");
        pc.has_forward = true; pc.forward = true;
        pc.has_track_alias = true; pc.track_alias = 0x95;
        pc.track_properties = (moq_bytes_t){ props, plen };
        MOQ_TEST_CHECK(moq_session_publish(sv, &pc, now, &sv_pub) == MOQ_OK);
    }
    {
        int slot = pub_resolve_handle(sv, sv_pub);
        MOQ_TEST_CHECK(slot >= 0);
        uint64_t eo = ver == MOQ_VERSION_DRAFT_18 ? 1000 : 5000;
        uint64_t es = ver == MOQ_VERSION_DRAFT_18 ? 8000 : 5000;
        MOQ_TEST_CHECK(sv->publishes[slot].dt_pub_has_object &&
                       sv->publishes[slot].dt_pub_object_ms == eo);
        MOQ_TEST_CHECK(sv->publishes[slot].dt_pub_has_subgroup &&
                       sv->publishes[slot].dt_pub_subgroup_ms == es);
    }
    /* PUBLISH_OK subscriber values: craft the response BEFORE accepting so
     * the pending publisher-role entry receives per-type parameters. */
    {
        uint8_t okb[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, okb, sizeof(okb));
        int slot = pub_resolve_handle(sv, sv_pub);
        MOQ_TEST_CHECK(slot >= 0);
        if (ver == MOQ_VERSION_DRAFT_18) {
            moq_d18_msg_params_t pms; memset(&pms, 0, sizeof(pms));
            pms.has_object_delivery_timeout = true;
            pms.object_delivery_timeout_ms = 4000;
            pms.has_subgroup_delivery_timeout = true;
            pms.subgroup_delivery_timeout_ms = 9000;
            MOQ_TEST_CHECK(moq_d18_encode_publish_ok(&w, &pms) == MOQ_OK);
            moq_stream_ref_t ref = sv->publishes[slot].request_stream_ref;
            MOQ_TEST_CHECK(ref._v != 0);
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, okb,
                moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        } else {
            uint8_t tov[8]; size_t ton =
                moq_quic_varint_encode(4000, tov, sizeof(tov));
            moq_kvp_entry_t prm = {
                .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
                .value = tov, .value_len = ton, .is_varint = true };
            MOQ_TEST_CHECK(moq_d16_encode_publish_ok(&w,
                sv->publishes[slot].request_id, &prm, 1) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(sv, okb,
                moq_buf_writer_offset(&w), now) == MOQ_OK);
        }
        drain_events(sv);
        MOQ_TEST_CHECK(sv->publishes[slot].dt_sub_has_object);
        if (ver == MOQ_VERSION_DRAFT_18) {
            MOQ_TEST_CHECK_EQ_U64(sv->publishes[slot].dt_sub_object_ms, 4000);
            MOQ_TEST_CHECK(sv->publishes[slot].dt_sub_has_subgroup &&
                           sv->publishes[slot].dt_sub_subgroup_ms == 9000);
            /* Legacy _ms retained projection: EXACT ms, min_nonzero. */
            MOQ_TEST_CHECK_EQ_U64(sv->publishes[slot].delivery_timeout_ms,
                                  4000);
        } else {
            MOQ_TEST_CHECK_EQ_U64(sv->publishes[slot].dt_sub_object_ms, 4000);
            MOQ_TEST_CHECK_EQ_U64(sv->publishes[slot].dt_sub_subgroup_ms,
                                  4000);
        }
        /* Consume the crafted response's fallout at the client (it never
         * saw the PUBLISH; steal the server's queued output). */
        moq_action_t a;
        while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- inbound PUBLISH extraction + publication update ack/reject -------- */
static void test_dt_pub_inbound_and_update(moq_version_t ver, bool reject)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv); drain_events(cl);

    uint8_t props[64];
    size_t plen = dt_blob_v(ver, props, sizeof(props), true, 6000,
                            ver == MOQ_VERSION_DRAFT_18, 2500, NULL, 0);
    moq_publication_t sv_pub, cl_pub;
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("t");
        pc.has_forward = true; pc.forward = true;
        pc.has_track_alias = true; pc.track_alias = 0x96;
        pc.track_properties = (moq_bytes_t){ props, plen };
        MOQ_TEST_CHECK(moq_session_publish(sv, &pc, now, &sv_pub) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        bool got = false; moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                cl_pub = e.u.publish_request.pub; got = true;
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK(got);
    }
    /* INBOUND PUBLISH extraction (subscriber-role entry). */
    {
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0);
        MOQ_TEST_CHECK(cl->publishes[slot].dt_pub_has_object &&
                       cl->publishes[slot].dt_pub_object_ms == 6000);
        if (ver == MOQ_VERSION_DRAFT_18)
            MOQ_TEST_CHECK(cl->publishes[slot].dt_pub_has_subgroup &&
                           cl->publishes[slot].dt_pub_subgroup_ms == 2500);
        else
            MOQ_TEST_CHECK(cl->publishes[slot].dt_pub_has_subgroup &&
                           cl->publishes[slot].dt_pub_subgroup_ms == 6000);
    }
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, true));

    /* Publication update: pending -> ACK latch (or d16 reject clear). */
    {
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_delivery_timeout = true;
        uc.delivery_timeout_us = 3000000;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc, now)
                       == MOQ_OK);
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0);
        MOQ_TEST_CHECK(cl->publishes[slot].dt_upd_has_object &&
                       cl->publishes[slot].dt_upd_object_ms == 3000);
        MOQ_TEST_CHECK(!cl->publishes[slot].dt_sub_has_object);
        if (reject && ver == MOQ_VERSION_DRAFT_16) {
            uint64_t uid = cl->publishes[slot].update_request_id;
            moq_action_t a;
            while (moq_session_poll_actions(cl, &a, 1) > 0)
                moq_action_cleanup(&a);
            uint8_t buf[64]; moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK(moq_d16_encode_request_error(&w, uid, 0x0, 0,
                NULL, 0) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, buf,
                moq_buf_writer_offset(&w), now) == MOQ_OK);
            MOQ_TEST_CHECK(!cl->publishes[slot].dt_upd_has_object &&
                           !cl->publishes[slot].dt_upd_has_subgroup);
            MOQ_TEST_CHECK(!cl->publishes[slot].dt_sub_has_object);
        } else if (!reject) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            drain_events(cl); drain_events(sv);
            MOQ_TEST_CHECK(!cl->publishes[slot].dt_upd_has_object);
            MOQ_TEST_CHECK(cl->publishes[slot].dt_sub_has_object &&
                           cl->publishes[slot].dt_sub_object_ms == 3000);
            MOQ_TEST_CHECK(cl->publishes[slot].dt_sub_has_subgroup &&
                           cl->publishes[slot].dt_sub_subgroup_ms == 3000);
            /* Retained legacy _ms recomputed from the complete pair. */
            MOQ_TEST_CHECK(cl->publishes[slot].has_delivery_timeout &&
                           cl->publishes[slot].delivery_timeout_ms == 3000);
        }
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- one PUBLICATION expiry (default 30s wait, live stream stopped) ---- */
static void test_ex_pub_expiry(moq_version_t ver)
{
    cap_stream_t cs = {0};
    MOQ_TEST_CHECK(capture_pub_stream(ver, 0x97, true, &cs));
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publication_t sv_pub, cl_pub;
    MOQ_TEST_CHECK(sc_publish_pending(sp, 0x97, true, &sv_pub, &cl_pub));
    MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, true));
    moq_stream_ref_t dref = moq_stream_ref_from_u64(0xE9A);
    feed_cap_header(cl, dref, &cs, now);            /* live bound stream */
    MOQ_TEST_CHECK(pub_feed_done(sp, ver, cl_pub, 0x2, 2, "pex", false)
                   == MOQ_OK);
    {
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].done_pending);
    }
    /* Default 30s wait: expiry stops the stream and surfaces the terminal
     * in the same pass. */
    MOQ_TEST_CHECK_EQ_U64(moq_session_next_deadline_us(cl),
                          now + 30000000ull);
    MOQ_TEST_CHECK(moq_session_tick(cl, now + 30000001ull) == MOQ_OK);
    {
        sub_evstats_t st = {0};
        pub_poll_stats(cl, &st);
        MOQ_TEST_CHECK_EQ_INT(st.done_n, 1);
        MOQ_TEST_CHECK_EQ_SIZE(st.reason_len, (size_t)3);
        MOQ_TEST_CHECK(memcmp(st.reason, "pex", 3) == 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ex_count_stops(cl), 1);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- malformed inbound PUBLISH: cleanup_all releases auth staging -------
 * Alias 3 is pre-registered by a VALID request; the malformed PUBLISH then
 * carries USE_ALIAS(3) (whose resolution stages a deep copy freed ONLY by
 * the cleanup path) plus a distinct REGISTER(4) (transaction-owned value).
 * The allocation delta is checked BEFORE destroy -- destroy would mask a
 * mid-session leak of the staged copies. */
static void test_publish_malformed_props_cleanup(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_cfg_t pcfg = MOQ_SIMPAIR_CFG_INIT;
    pcfg.alloc = &alloc;
    pcfg.seed = 7; pcfg.initial_now_us = 1000;
    pcfg.client_send_request_capacity = true;
    pcfg.client_initial_request_capacity = 16;
    pcfg.server_send_request_capacity = true;
    pcfg.server_initial_request_capacity = 16;
    pcfg.version = ver;
    pcfg.auth_token_cache_size = 4096;   /* REGISTER tokens acceptable */
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&pcfg, &sp) == MOQ_OK);
    if (!sp) return;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(cl);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };

    /* #1: VALID PUBLISH registering alias 3. */
    {
        uint8_t msg[256]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        if (ver == MOQ_VERSION_DRAFT_16) {
            uint8_t tb[64]; moq_buf_writer_t tw;
            moq_buf_writer_init(&tw, tb, sizeof(tb));
            moq_d16_auth_token_t tok = {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 3,
                .token_type = 1,
                .token_value = (const uint8_t *)"tv", .token_value_len = 2 };
            MOQ_TEST_CHECK(moq_d16_auth_token_encode(&tw, &tok) == MOQ_OK);
            moq_kvp_entry_t prm = {
                .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                .value = tb, .value_len = moq_buf_writer_offset(&tw),
                .is_varint = false };
            moq_d16_publish_t pub = {
                .request_id = 1,
                .track_namespace = { parts, 1 },
                .track_name = MOQ_BYTES_LITERAL("m1"),
                .track_alias = 0x98,
                .params = &prm, .params_count = 1, .params_cap = 1,
            };
            MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, msg,
                moq_buf_writer_offset(&w), now) == MOQ_OK);
        } else {
            moq_d18_publish_t p; memset(&p, 0, sizeof(p));
            p.request_id = 1;
            p.track_namespace = (moq_namespace_t){ parts, 1 };
            p.track_name = MOQ_BYTES_LITERAL("m1");
            p.track_alias = 0x98;
            p.params.auth_token_count = 1;
            p.params.auth_tokens[0] = (moq_d18_auth_token_t){
                .alias_type = 1 /* REGISTER */, .alias = 3, .token_type = 1,
                .token_value = MOQ_BYTES_LITERAL("tv") };
            MOQ_TEST_CHECK(moq_d18_encode_publish(&w, &p) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl,
                moq_stream_ref_from_u64(0xB0A), msg,
                moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        }
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        drain_events(cl);
    }

    int64_t before = as.balance;

    /* #2: MALFORMED properties + USE_ALIAS(3) + REGISTER(4). */
    uint8_t bad[64]; size_t blen;
    if (ver == MOQ_VERSION_DRAFT_18) {
        uint8_t imm[32];
        size_t ilen = dt_blob_v(ver, imm, sizeof(imm), true, 700, false, 0,
                                NULL, 0);
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 1000, false, 0,
                         imm, ilen);   /* duplicate across lists */
    } else {
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 0, false, 0, NULL, 0);
    }
    MOQ_TEST_CHECK(blen > 0);
    {
        uint8_t msg[256]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        if (ver == MOQ_VERSION_DRAFT_16) {
            uint8_t ta[32], tb[64]; moq_buf_writer_t tw;
            moq_buf_writer_init(&tw, ta, sizeof(ta));
            moq_d16_auth_token_t use = {
                .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 3 };
            MOQ_TEST_CHECK(moq_d16_auth_token_encode(&tw, &use) == MOQ_OK);
            size_t ulen = moq_buf_writer_offset(&tw);
            moq_buf_writer_init(&tw, tb, sizeof(tb));
            moq_d16_auth_token_t reg = {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 4,
                .token_type = 1,
                .token_value = (const uint8_t *)"t4", .token_value_len = 2 };
            MOQ_TEST_CHECK(moq_d16_auth_token_encode(&tw, &reg) == MOQ_OK);
            moq_kvp_entry_t prm[2] = {
                { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                  .value = ta, .value_len = ulen, .is_varint = false },
                { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                  .value = tb, .value_len = moq_buf_writer_offset(&tw),
                  .is_varint = false },
            };
            moq_d16_publish_t pub = {
                .request_id = 3,
                .track_namespace = { parts, 1 },
                .track_name = MOQ_BYTES_LITERAL("m2"),
                .track_alias = 0x99,
                .params = prm, .params_count = 2, .params_cap = 2,
                .track_extensions = bad, .track_extensions_len = blen,
            };
            MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);
            (void)moq_session_on_control_bytes(cl, msg,
                moq_buf_writer_offset(&w), now);
        } else {
            moq_d18_publish_t p; memset(&p, 0, sizeof(p));
            p.request_id = 3;
            p.track_namespace = (moq_namespace_t){ parts, 1 };
            p.track_name = MOQ_BYTES_LITERAL("m2");
            p.track_alias = 0x99;
            p.track_properties = (moq_bytes_t){ bad, blen };
            p.params.auth_token_count = 2;
            p.params.auth_tokens[0] = (moq_d18_auth_token_t){
                .alias_type = 2 /* USE_ALIAS */, .alias = 3 };
            p.params.auth_tokens[1] = (moq_d18_auth_token_t){
                .alias_type = 1 /* REGISTER */, .alias = 4, .token_type = 1,
                .token_value = MOQ_BYTES_LITERAL("t4") };
            MOQ_TEST_CHECK(moq_d18_encode_publish(&w, &p) == MOQ_OK);
            (void)moq_session_on_bidi_stream_bytes(cl,
                moq_stream_ref_from_u64(0xB0B), msg,
                moq_buf_writer_offset(&w), false, now);
        }
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);
    /* The staged USE_ALIAS copy and the transaction-owned REGISTER value
     * must be released by cleanup_all -- delta checked BEFORE destroy. */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(as.balance - before), 0);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- d18 update REJECTION clears pending timeout carriers (both roles) -- */
static void test_dt_d18_update_reject(bool publication)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_18, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_stream_ref_t ref;
    if (publication) {
        moq_publication_t sv_pub, cl_pub;
        MOQ_TEST_CHECK(sc_publish_pending(sp, 0x9C, true, &sv_pub, &cl_pub));
        MOQ_TEST_CHECK(pub_accept_fwd(sp, cl_pub, true));
        /* Install an ACKED value first so rejection provably leaves it. */
        {
            moq_publication_update_cfg_t uc;
            moq_publication_update_cfg_init(&uc);
            uc.has_delivery_timeout = true;
            uc.delivery_timeout_us = 5000000;
            MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc,
                now) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            drain_events(cl); drain_events(moq_simpair_server(sp));
        }
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.has_delivery_timeout = true;
        uc.delivery_timeout_us = 2000000;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, cl_pub, &uc, now)
                       == MOQ_OK);
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 && cl->publishes[slot].dt_upd_has_object);
        { moq_action_t a;                        /* never delivered */
          while (moq_session_poll_actions(cl, &a, 1) > 0)
              moq_action_cleanup(&a); }
        ref = cl->publishes[slot].request_stream_ref;
        uint8_t buf[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_request_error(&w, 0x0, 0,
            (moq_bytes_t){0}) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, buf,
            moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        /* Rejection: pending carriers cleared, ACKED values unchanged,
         * update_failed latched. */
        MOQ_TEST_CHECK(!cl->publishes[slot].dt_upd_has_object &&
                       !cl->publishes[slot].dt_upd_has_subgroup);
        MOQ_TEST_CHECK(cl->publishes[slot].dt_sub_has_object &&
                       cl->publishes[slot].dt_sub_object_ms == 5000);
        MOQ_TEST_CHECK(cl->publishes[slot].update_failed);
    } else {
        moq_subscription_t cl_sub, sv_sub;
        MOQ_TEST_CHECK(sc_sub_establish(sp, &cl_sub, &sv_sub));
        {
            moq_subscription_update_cfg_t uc;
            moq_subscription_update_cfg_init(&uc);
            uc.has_delivery_timeout = true;
            uc.delivery_timeout_us = 5000000;
            MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc,
                now) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            drain_events(cl); drain_events(moq_simpair_server(sp));
        }
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init(&uc);
        uc.has_delivery_timeout = true;
        uc.delivery_timeout_us = 2000000;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, cl_sub, &uc, now)
                       == MOQ_OK);
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0 && cl->subs[slot].dt_upd_has_object);
        { moq_action_t a;
          while (moq_session_poll_actions(cl, &a, 1) > 0)
              moq_action_cleanup(&a); }
        ref = cl->subs[slot].request_stream_ref;
        uint8_t buf[64]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_request_error(&w, 0x0, 0,
            (moq_bytes_t){0}) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(cl, ref, buf,
            moq_buf_writer_offset(&w), false, now) == MOQ_OK);
        MOQ_TEST_CHECK(!cl->subs[slot].dt_upd_has_object &&
                       !cl->subs[slot].dt_upd_has_subgroup);
        MOQ_TEST_CHECK(cl->subs[slot].dt_sub_has_object &&
                       cl->subs[slot].dt_sub_object_ms == 5000);
        MOQ_TEST_CHECK(cl->subs[slot].update_failed);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- d16 inbound duplicate extension: first wins, session survives ----- */
static void test_dt_d16_inbound_dup_ext(void)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, MOQ_VERSION_DRAFT_16, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(cl);

    /* Duplicate DELIVERY TIMEOUT extension: repeatability is an unfinished
     * d16 registry item (§13.3), so inbound stays LENIENT -- first value
     * wins, no close. (Local emission of the same blob is refused; the
     * strict-mode rejection is covered by the local-invalid tests.) */
    uint8_t dup[32]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, dup, sizeof(dup));
    MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0x02) == MOQ_OK);  /* delta 2 */
    MOQ_TEST_CHECK(moq_buf_write_varint(&w, 7000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0x00) == MOQ_OK);  /* delta 0 */
    MOQ_TEST_CHECK(moq_buf_write_varint(&w, 9000) == MOQ_OK);
    size_t dlen = moq_buf_writer_offset(&w);

    /* Inbound PUBLISH carrying the duplicate. */
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_d16_publish_t pub = {
        .request_id = 1,
        .track_namespace = { parts, 1 },
        .track_name = MOQ_BYTES_LITERAL("dp"),
        .track_alias = 0x9D,
        .track_extensions = dup, .track_extensions_len = dlen,
    };
    uint8_t msg[256]; moq_buf_writer_t mw;
    moq_buf_writer_init(&mw, msg, sizeof(msg));
    MOQ_TEST_CHECK(moq_d16_encode_publish(&mw, &pub) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, msg,
        moq_buf_writer_offset(&mw), now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
    {
        moq_event_t e; moq_publication_t cl_pub; bool got = false;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                cl_pub = e.u.publish_request.pub; got = true;
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK(got);
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0 &&
                       cl->publishes[slot].dt_pub_has_object &&
                       cl->publishes[slot].dt_pub_object_ms == 7000);
    }

    /* Inbound SUBSCRIBE_OK carrying the duplicate (fresh subscription). */
    {
        moq_bytes_t p2[] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ p2, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("dq");
        moq_subscription_t cl_sub;
        MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now, &cl_sub)
                       == MOQ_OK);
        { moq_action_t a;                     /* SUBSCRIBE never delivered */
          while (moq_session_poll_actions(cl, &a, 1) > 0)
              moq_action_cleanup(&a); }
        int slot = sub_resolve_handle(cl, cl_sub);
        MOQ_TEST_CHECK(slot >= 0);
        uint8_t ok[128]; moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok, sizeof(ow));
        moq_buf_writer_init(&ow, ok, sizeof(ok));
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&ow,
            cl->subs[slot].request_id, 0x9E, NULL, 0, dup, dlen) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(cl, ok,
            moq_buf_writer_offset(&ow), now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(cl->subs[slot].dt_pub_has_object &&
                       cl->subs[slot].dt_pub_object_ms == 7000);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- malformed SUBSCRIBE_OK properties close BEFORE any event/transition */
static void test_dt_subscribe_ok_malformed(moq_version_t ver,
                                            bool with_mandatory)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(cl);

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("mo");
    moq_subscription_t cl_sub;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now, &cl_sub) == MOQ_OK);
    { moq_action_t a;                       /* SUBSCRIBE never delivered */
      while (moq_session_poll_actions(cl, &a, 1) > 0) moq_action_cleanup(&a); }
    int slot = sub_resolve_handle(cl, cl_sub);
    MOQ_TEST_CHECK(slot >= 0);

    /* Malformed timeout in the OK's properties: d16 zero-valued extension,
     * d18 duplicate across mutable+immutable. */
    uint8_t bad[64]; size_t blen;
    if (ver == MOQ_VERSION_DRAFT_18) {
        uint8_t imm[32];
        size_t ilen = dt_blob_v(ver, imm, sizeof(imm), true, 700, false, 0,
                                NULL, 0);
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 1000, false, 0,
                         imm, ilen);
        if (with_mandatory) {
            /* Append an unknown Mandatory Track Property (0x4000, even ->
             * single varint value) AFTER the duplicate-across-lists timeout:
             * malformed properties must close 0x3 and must NOT be downgraded
             * to the request-level unsupported-mandatory SUBSCRIBE_ERROR. */
            moq_buf_writer_t mw;
            moq_buf_writer_init(&mw, bad + blen, sizeof(bad) - blen);
            MOQ_TEST_CHECK(moq_buf_write_vi64(&mw, 0x4000u - 0x0Bu) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_write_vi64(&mw, 0) == MOQ_OK);
            blen += moq_buf_writer_offset(&mw);
        }
    } else {
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 0, false, 0, NULL, 0);
    }
    if (ver == MOQ_VERSION_DRAFT_16) {
        uint8_t ok[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&w,
            cl->subs[slot].request_id, 0xA1, NULL, 0, bad, blen) == MOQ_OK);
        (void)moq_session_on_control_bytes(cl, ok,
            moq_buf_writer_offset(&w), now);
    } else {
        /* Hand-encoded (the public encoder validates strictly and refuses
         * the malformed / mandatory blobs a hostile peer can still send):
         * Type, uint16 Length, Track Alias, Param Count 0, properties tail. */
        uint8_t ok[128]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        size_t len_off = 0;
        MOQ_TEST_CHECK(moq_buf_write_vi64(&w, 0x04u) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_reserve_uint16(&w, &len_off) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_vi64(&w, 0xA1u) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_vi64(&w, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, bad, blen) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - (len_off + 2))) == MOQ_OK);
        moq_stream_ref_t ref = cl->subs[slot].request_stream_ref;
        (void)moq_session_on_bidi_stream_bytes(cl, ref, ok,
            moq_buf_writer_offset(&w), false, now);
    }
    MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);
    /* NO success or error event surfaced; the entry never established. */
    {
        moq_event_t e;
        while (moq_session_poll_events(cl, &e, 1) == 1) {
            MOQ_TEST_CHECK(e.kind != MOQ_EVENT_SUBSCRIBE_OK &&
                           e.kind != MOQ_EVENT_SUBSCRIBE_ERROR);
            moq_event_cleanup(&e);
        }
    }
    MOQ_TEST_CHECK(cl->subs[slot].state != MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK(!cl->subs[slot].dt_pub_has_object);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* -- publish-side hoist discriminator: INVAL with zero mutation -------- */
static void test_dt_publish_local_invalid(moq_version_t ver)
{
    sc_alloc_state_t as = {0};
    moq_alloc_t alloc = sc_allocator(&as);
    moq_simpair_t *sp = sc_pair(&alloc, ver, false, 0);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv);

    uint8_t bad[64]; size_t blen;
    if (ver == MOQ_VERSION_DRAFT_18) {
        uint8_t imm[32];
        size_t ilen = dt_blob_v(ver, imm, sizeof(imm), true, 700, false, 0,
                                NULL, 0);
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 1000, false, 0,
                         imm, ilen);
    } else {
        blen = dt_blob_v(ver, bad, sizeof(bad), true, 0, false, 0, NULL, 0);
    }
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("pl");
    pc.has_forward = true; pc.forward = true;
    pc.track_properties = (moq_bytes_t){ bad, blen };
    moq_publication_t h; memset(&h, 0, sizeof(h));
    uint64_t epoch_before = sv->borrow_epoch;
    MOQ_TEST_CHECK(moq_session_publish(sv, &pc, now, &h) == MOQ_ERR_INVAL);
    /* Zero mutation: no epoch advance, no handle, no entry, no action. */
    MOQ_TEST_CHECK_EQ_U64(sv->borrow_epoch, epoch_before);
    MOQ_TEST_CHECK(pub_resolve_handle(sv, h) < 0);
    {
        int live = 0;
        for (size_t i = 0; i < sv->pub_cap; i++)
            if (sv->publishes[i].state != MOQ_PUB_FREE) live++;
        MOQ_TEST_CHECK_EQ_INT(live, 0);
    }
    {
        moq_action_t a;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(sv, &a, 1),
                               (size_t)0);
    }
    /* A subsequent VALID publish succeeds. */
    uint8_t good[32];
    size_t glen = dt_blob_v(ver, good, sizeof(good), true, 2000, false, 0,
                            NULL, 0);
    pc.track_properties = (moq_bytes_t){ good, glen };
    MOQ_TEST_CHECK(moq_session_publish(sv, &pc, now, &h) == MOQ_OK);
    {
        int slot = pub_resolve_handle(sv, h);
        MOQ_TEST_CHECK(slot >= 0 && sv->publishes[slot].dt_pub_has_object &&
                       sv->publishes[slot].dt_pub_object_ms == 2000);
    }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

int main(void)
{
    test_done_reset_ordering(MOQ_VERSION_DRAFT_16, true);
    test_done_reset_ordering(MOQ_VERSION_DRAFT_16, false);
    test_done_reset_ordering(MOQ_VERSION_DRAFT_18, true);
    test_done_reset_ordering(MOQ_VERSION_DRAFT_18, false);
    test_streaming_reset_backpressure(MOQ_VERSION_DRAFT_16);
    test_streaming_reset_backpressure(MOQ_VERSION_DRAFT_18);
    test_preheader_reset_unattributed(MOQ_VERSION_DRAFT_16);
    test_preheader_reset_unattributed(MOQ_VERSION_DRAFT_18);

    test_sub_done_before_termination(MOQ_VERSION_DRAFT_16, false);
    test_sub_done_before_termination(MOQ_VERSION_DRAFT_16, true);
    test_sub_done_before_termination(MOQ_VERSION_DRAFT_18, false);
    test_sub_done_before_termination(MOQ_VERSION_DRAFT_18, true);
    test_sub_termination_before_done(MOQ_VERSION_DRAFT_16, false);
    test_sub_termination_before_done(MOQ_VERSION_DRAFT_16, true);
    test_sub_termination_before_done(MOQ_VERSION_DRAFT_18, false);
    test_sub_termination_before_done(MOQ_VERSION_DRAFT_18, true);
    test_sub_late_streams_while_deferred(MOQ_VERSION_DRAFT_16);
    test_sub_late_streams_while_deferred(MOQ_VERSION_DRAFT_18);
    test_sub_backpressure_reap(MOQ_VERSION_DRAFT_16);
    test_sub_backpressure_reap(MOQ_VERSION_DRAFT_18);
    test_sub_zero_and_sentinel(MOQ_VERSION_DRAFT_16);
    test_sub_zero_and_sentinel(MOQ_VERSION_DRAFT_18);
    test_sub_destroy_while_deferred(MOQ_VERSION_DRAFT_16);
    test_sub_destroy_while_deferred(MOQ_VERSION_DRAFT_18);
    test_sub_oom_reason(MOQ_VERSION_DRAFT_16);
    test_sub_oom_reason(MOQ_VERSION_DRAFT_18);
    test_sub_d18_fin_variants();
    test_sub_d18_extra_bytes_after_terminal();
    test_sub_preheader_reset_unattributed(MOQ_VERSION_DRAFT_16);
    test_sub_preheader_reset_unattributed(MOQ_VERSION_DRAFT_18);

    test_pub_pending_done(MOQ_VERSION_DRAFT_16, true);
    test_pub_pending_done(MOQ_VERSION_DRAFT_16, false);
    test_pub_pending_done(MOQ_VERSION_DRAFT_18, true);
    test_pub_pending_done(MOQ_VERSION_DRAFT_18, false);
    test_sub_pending_done_rejected(MOQ_VERSION_DRAFT_16);
    test_sub_pending_done_rejected(MOQ_VERSION_DRAFT_18);
    test_sub_terminal_guards(MOQ_VERSION_DRAFT_16);
    test_sub_terminal_guards(MOQ_VERSION_DRAFT_18);
    test_pub_terminal_guards(MOQ_VERSION_DRAFT_16);
    test_pub_terminal_guards(MOQ_VERSION_DRAFT_18);
    test_d16_duplicate_done_closes(false);
    test_d16_duplicate_done_closes(true);
    test_d16_late_update_ack(false);
    test_d16_late_update_ack(true);
    test_sub_d18_defer_action_backpressure();
    test_sub_reason_deep_copy(MOQ_VERSION_DRAFT_16);
    test_sub_reason_deep_copy(MOQ_VERSION_DRAFT_18);

    test_pub_pending_f0(MOQ_VERSION_DRAFT_16, true);
    test_pub_pending_f0(MOQ_VERSION_DRAFT_16, false);
    test_pub_pending_f0(MOQ_VERSION_DRAFT_18, true);
    test_pub_pending_f0(MOQ_VERSION_DRAFT_18, false);
    test_d18_update_tomb_churn(false);
    test_d18_update_tomb_churn(true);
    test_d16_dup_ack_closes(false);
    test_d16_dup_ack_closes(true);

    for (int v = 0; v < 2; v++) {
        moq_version_t ver = v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_fwd0_stream_suppressed(ver, false, false, false); /* pending */
        test_fwd0_stream_suppressed(ver, true,  false, false); /* estab   */
        test_fwd0_stream_suppressed(ver, true,  true,  false); /* stream  */
        test_fwd0_stream_suppressed(ver, true,  false, true);  /* reset   */
        test_fwd0_datagram_suppressed(ver, false);
        test_fwd0_datagram_suppressed(ver, true);
        test_fwd_update_latch(ver);

        test_sub_fwd0_stream(ver, false);
        test_sub_fwd0_stream(ver, true);
        test_sub_fwd0_datagram(ver, false);
        test_sub_fwd0_datagram(ver, true);
        test_frozen_mid_object(ver, true);   /* 1 -> 0 mid-object */
        test_frozen_mid_object(ver, false);  /* 0 -> 1 mid-object */
        test_frozen_mid_object_backpressure(ver);
        test_pub_fwd0_datagram_no_staging(ver);

        test_dt_extraction(ver);
        test_dt_local_invalid(ver);
        test_dt_update_latch(ver, false);
        if (ver == MOQ_VERSION_DRAFT_16)
            test_dt_update_latch(ver, true);   /* d18 reject rides the bidi;
                                                * clear covered via retire */
        test_dt_subgroup_consumer(ver);
    }
    test_dt_event_vs_retained_d18();
    test_dt_saturation_d18();

    test_ex_cfg();
    for (int v = 0; v < 2; v++) {
        moq_version_t ver = v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_ex_one_pass(ver);
        test_ex_blocked_fin(ver, false);
        test_ex_blocked_fin(ver, true);
        test_ex_event_blocked_late_bind(ver);
        test_ex_close_matrix(ver, 0);
        test_ex_close_matrix(ver, 1);
        test_ex_close_matrix(ver, 2);
        test_ex_two_entry_wake(ver);
        test_ex_floor_and_adapter_loop(ver);

        test_dt_pub_matrix(ver);
        test_dt_pub_inbound_and_update(ver, false);
        if (ver == MOQ_VERSION_DRAFT_16)
            test_dt_pub_inbound_and_update(ver, true);
        test_ex_pub_expiry(ver);
        test_publish_malformed_props_cleanup(ver);
    }
    test_dt_d18_update_reject(false);
    test_dt_d18_update_reject(true);
    test_dt_d16_inbound_dup_ext();
    test_dt_subscribe_ok_malformed(MOQ_VERSION_DRAFT_16, false);
    test_dt_subscribe_ok_malformed(MOQ_VERSION_DRAFT_18, false);
    test_dt_subscribe_ok_malformed(MOQ_VERSION_DRAFT_18, true);
    test_dt_publish_local_invalid(MOQ_VERSION_DRAFT_16);
    test_dt_publish_local_invalid(MOQ_VERSION_DRAFT_18);

    if (failures == 0)
        printf("test_session_stream_count: all passed\n");
    return failures ? 1 : 0;
}
