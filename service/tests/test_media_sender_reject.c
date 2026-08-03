/*
 * Media-sender rejection handling. Drives the PRODUCTION
 * sender_hook against a real SimPair session whose peer rejects/cancels the
 * namespace or rejects a PUBLISH, and asserts the sender becomes fatal with
 * the exact code, fires on_closed(is_fatal=true) once, never becomes ready,
 * and reports terminal state to writers -- exercised through the real
 * publisher-facade event path, not a handler stub.
 */
#include <moq/media_sender.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include "test_session_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

static int failures = 0;

/* Test seam (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);

typedef struct {
    int      closed_n;
    int      ready_n;
    bool     last_fatal;
    uint64_t last_code;
} close_state_t;

static void on_closed(void *ctx, moq_media_sender_t *s, bool is_fatal,
                      uint64_t fatal_code)
{
    (void)s;
    close_state_t *st = (close_state_t *)ctx;
    st->closed_n++;
    st->last_fatal = is_fatal;
    st->last_code = fatal_code;
}

static void on_ready(void *ctx, moq_media_sender_t *s)
{
    (void)s;
    ((close_state_t *)ctx)->ready_n++;
}

static moq_media_sender_t *make_sender(close_state_t *cs, moq_bytes_t *ns_parts)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ ns_parts, 2 };
    cfg.publish_tracks = true;
    cfg.callbacks.ctx = cs;
    cfg.callbacks.on_closed = on_closed;
    cfg.callbacks.on_ready = on_ready;
    return moq_media_sender_test_new_cfg(&cfg);
}

static moq_media_track_t *add_video(moq_media_sender_t *s)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    moq_media_track_t *t = NULL;
    (void)moq_media_sender_add_track(s, &tc, &t);
    return t;
}

static moq_simpair_t *pair(moq_alloc_t *alloc, moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    moq_simpair_t *sp = NULL;
    moq_simpair_create(&cfg, &sp);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

/* Assert the terminal aftermath is uniform for every fatal reason. */
static void assert_terminal(moq_media_sender_t *s, moq_media_track_t *v,
                            close_state_t *cs, uint64_t want_code)
{
    MOQ_TEST_CHECK(moq_media_sender_is_fatal(s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(s), want_code);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(s));   /* never ready after */
    MOQ_TEST_CHECK_EQ_INT(cs->closed_n, 1);          /* once */
    MOQ_TEST_CHECK(cs->last_fatal);
    MOQ_TEST_CHECK_EQ_U64(cs->last_code, want_code);
    /* A write observes terminal state (no silent hang). */
    if (v) {
        uint8_t d[8]; memset(d, 0x11, sizeof(d));
        moq_rcbuf_t *b = NULL; moq_rcbuf_create(moq_alloc_default(),
                                                d, sizeof(d), &b);
        moq_media_send_object_t o; memset(&o, 0, sizeof(o));
        o.struct_size = sizeof(o); o.payload = b;
        o.is_sync = true; o.starts_group = true;
        o.presentation_time_us = 1000;
        moq_result_t wrc = moq_media_sender_write(s, v, &o);
        MOQ_TEST_CHECK(wrc == MOQ_ERR_CLOSED);
        moq_rcbuf_decref(b);
    }
}

/* Namespace rejected -> fatal 0x3. */
static void test_namespace_rejected(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    close_state_t cs = {0};
    moq_media_sender_t *s = make_sender(&cs, ns_parts);
    MOQ_TEST_CHECK(s != NULL);
    moq_media_track_t *v = add_video(s);

    /* Pump the hook: create pub, advertise namespace + publish catalog. */
    moq_media_sender_test_pump(s, cl, now);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* The relay rejects the namespace advertisement. */
    moq_event_t ev;
    bool rejected = false;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_reject_namespace_cfg_t rj; memset(&rj, 0, sizeof(rj));
            rj.struct_size = sizeof(rj);
            rj.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            (void)moq_session_reject_namespace(srv,
                ev.u.namespace_published.ann, &rj, now);
            rejected = true;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(rejected);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Pump again: the facade dispatches NAMESPACE_REJECTED -> sender fatal. */
    moq_media_sender_test_pump(s, cl, now);
    assert_terminal(s, v, &cs, MOQ_MEDIA_SENDER_FATAL_NAMESPACE_REJECTED);

    moq_media_sender_test_free(s);
    { moq_event_t e; while (moq_session_poll_events(cl, &e, 1) == 1)
        moq_event_cleanup(&e);
      while (moq_session_poll_events(srv, &e, 1) == 1)
        moq_event_cleanup(&e); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "sender_namespace_rejected_d18" : "sender_namespace_rejected_d16");
}

/* Namespace accepted then CANCELLED by the peer -> fatal 0x4. */
static void test_namespace_cancelled(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    close_state_t cs = {0};
    moq_media_sender_t *s = make_sender(&cs, ns_parts);
    moq_media_track_t *v = add_video(s);
    moq_media_sender_test_pump(s, cl, now);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Accept the namespace; capture its handle for the later cancel. */
    moq_event_t ev;
    moq_announcement_t ann = {0};
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            ann = ev.u.namespace_published.ann;
            moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
            (void)moq_session_accept_namespace(srv, ann, &ac, now);
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_media_sender_test_pump(s, cl, now);   /* consume acceptance */
    MOQ_TEST_CHECK_EQ_INT(cs.closed_n, 0);

    /* Peer cancels the accepted namespace (d18: no reason on the reset). */
    {
        moq_cancel_namespace_cfg_t cn; moq_cancel_namespace_cfg_init(&cn);
        if (ver == MOQ_VERSION_DRAFT_16) {
            cn.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            cn.reason = MOQ_BYTES_LITERAL("gone");
        }
        (void)moq_session_cancel_namespace(srv, ann, &cn, now);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_media_sender_test_pump(s, cl, now);
    assert_terminal(s, v, &cs, MOQ_MEDIA_SENDER_FATAL_NAMESPACE_CANCELLED);

    moq_media_sender_test_free(s);
    { moq_event_t e; while (moq_session_poll_events(cl, &e, 1) == 1)
        moq_event_cleanup(&e);
      while (moq_session_poll_events(srv, &e, 1) == 1)
        moq_event_cleanup(&e); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "sender_namespace_cancelled_d18" : "sender_namespace_cancelled_d16");
}

/* A rejected PUBLISH (catalog or media) -> fatal 0x5. The relay ACCEPTS the
 * namespace, then rejects the first PUBLISH request it sees. */
static void test_publish_rejected(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    close_state_t cs = {0};
    moq_media_sender_t *s = make_sender(&cs, ns_parts);
    moq_media_track_t *v = add_video(s);

    /* Drive the sender + accept namespace + reject the first PUBLISH, pumping
     * across several cycles until the rejection lands. */
    bool published_rejected = false;
    for (int cycle = 0; cycle < 12 && !cs.closed_n; cycle++) {
        moq_media_sender_test_pump(s, cl, now);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(srv,
                    ev.u.namespace_published.ann, &ac, now);
            } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                moq_reject_publish_cfg_t rp;
                moq_reject_publish_cfg_init(&rp);
                rp.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
                (void)moq_session_reject_publish(srv,
                    ev.u.publish_request.pub, &rp, now);
                published_rejected = true;
            }
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK(published_rejected);
    moq_media_sender_test_pump(s, cl, now);   /* dispatch the PUBLISH_ERROR */
    assert_terminal(s, v, &cs, MOQ_MEDIA_SENDER_FATAL_PUBLISH_REJECTED);

    moq_media_sender_test_free(s);
    { moq_event_t e; while (moq_session_poll_events(cl, &e, 1) == 1)
        moq_event_cleanup(&e);
      while (moq_session_poll_events(srv, &e, 1) == 1)
        moq_event_cleanup(&e); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "sender_publish_rejected_d18" : "sender_publish_rejected_d16");
}

/* Crossed: the peer ACCEPTS the catalog PUBLISH but REJECTS the namespace in
 * the SAME pump cycle. The sender must go fatal (one close callback) and fire
 * ZERO ready callbacks -- the readiness block must not run after the in-tick
 * fatal. */
static void test_crossed_accept_publish_reject_namespace(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    close_state_t cs = {0};
    moq_media_sender_t *s = make_sender(&cs, ns_parts);
    moq_media_track_t *v = add_video(s);

    /* First pump advertises the namespace + publishes the catalog. */
    moq_media_sender_test_pump(s, cl, now);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Peer: ACCEPT the catalog PUBLISH but REJECT the namespace, both queued
     * before the sender's next pump processes either. */
    moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_reject_namespace_cfg_t rj; memset(&rj, 0, sizeof(rj));
            rj.struct_size = sizeof(rj);
            rj.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            (void)moq_session_reject_namespace(srv,
                ev.u.namespace_published.ann, &rj, now);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t ac;
            moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
            (void)moq_session_accept_publish(srv,
                ev.u.publish_request.pub, &ac, now);
        }
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* The pump dispatches BOTH the accepted PUBLISH (would enable readiness)
     * and the namespace rejection (fatal) in one cycle. Fatal must win. */
    moq_media_sender_test_pump(s, cl, now);
    MOQ_TEST_CHECK(moq_media_sender_is_fatal(s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(s),
                          MOQ_MEDIA_SENDER_FATAL_NAMESPACE_REJECTED);
    MOQ_TEST_CHECK_EQ_INT(cs.closed_n, 1);        /* one fatal close */
    MOQ_TEST_CHECK(cs.last_fatal);
    MOQ_TEST_CHECK_EQ_INT(cs.ready_n, 0);         /* ZERO ready callbacks */
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(s));

    (void)v;
    moq_media_sender_test_free(s);
    { moq_event_t e; while (moq_session_poll_events(cl, &e, 1) == 1)
        moq_event_cleanup(&e);
      while (moq_session_poll_events(srv, &e, 1) == 1)
        moq_event_cleanup(&e); }
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "sender_crossed_accept_pub_reject_ns_d18"
        : "sender_crossed_accept_pub_reject_ns_d16");
}

int main(void)
{
    for (int vi = 0; vi < 2; vi++) {
        moq_version_t ver = vi ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_namespace_rejected(ver);
        test_namespace_cancelled(ver);
        test_publish_rejected(ver);
        test_crossed_accept_publish_reject_namespace(ver);
    }
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
