/*
 * Automatic independent catalog refresh: recovers late viewers joining
 * through relays that resolve Joining FETCHes locally. Drives the PRODUCTION
 * sender_hook against a real SimPair peer and a real media_receiver so the
 * refresh is exercised end to end: config/ABI resolution, the demand-gated
 * periodic republish (a NEW independent group whose object 0 is the complete
 * catalog), mutation precedence, the group/deadline ceiling, WOULD_BLOCK
 * exactly-once, an idle pump progressing the refresh, and receiver-side
 * dedup of repeated identical catalogs.
 */
#include <moq/media_sender.h>
#include <moq/media_receiver.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/wire.h>          /* MOQ_QUIC_VARINT_MAX */
#include "test_session_support.h"

#include <stdlib.h>            /* malloc/free for the exact-old-size ABI canary */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

static int failures = 0;

/* -- media_sender test seams (media_sender.c, MOQ_MEDIA_SENDER_TESTING) -- */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void     moq_media_sender_test_pump(moq_media_sender_t *s,
                                    moq_session_t *session, uint64_t now_us);
void     moq_media_sender_test_free(moq_media_sender_t *s);
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *s);
uint64_t moq_media_sender_test_refresh_interval(const moq_media_sender_t *s);
unsigned moq_media_sender_test_retained_installs(const moq_media_sender_t *s);
void     moq_media_sender_test_block_republish_before(moq_media_sender_t *s,
                                                      uint64_t object_id);
void     moq_media_sender_test_block_retained_install_once(
             moq_media_sender_t *s);
void     moq_media_sender_test_set_catalog_group(moq_media_sender_t *s,
                                                 uint64_t g);
uint64_t moq_media_sender_test_next_deadline_us(moq_media_sender_t *s);
void     moq_media_sender_test_fire_closed(moq_media_sender_t *s, bool is_fatal,
                                           uint64_t fatal_code);

/* -- media_receiver test seams (media_receiver.c) -------------------------- */
moq_media_receiver_t *moq_media_receiver_test_new(bool auto_subscribe);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len);
bool moq_media_receiver_test_poll(moq_media_receiver_t *r,
                                  moq_media_track_event_kind_t *kind,
                                  moq_media_track_t **track);
size_t moq_media_receiver_test_track_count(const moq_media_receiver_t *r);

/* ======================================================================== *
 *  Config / ABI resolution (no endpoint, no network)
 * ======================================================================== */

static moq_bytes_t g_ns_parts[2];

static moq_media_sender_t *cfg_sender(uint64_t interval, bool set_interval,
                                      size_t cfg_size)
{
    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    /* Allocate the real struct but present a caller-declared struct_size, so an
     * "old size" caller is modelled exactly (the field is beyond struct_size). */
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, cfg_size);
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    if (set_interval)
        cfg.catalog_refresh_interval_us = interval;
    return moq_media_sender_test_new_cfg(&cfg);
}

/* full-size zero, absent (old size), and explicit 0 all resolve to 1s. */
static void test_cfg_default_interval(void)
{
    /* full-size, field left zero -> default */
    moq_media_sender_t *a = cfg_sender(0, false, sizeof(moq_media_sender_cfg_t));
    MOQ_TEST_CHECK(a != NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(a), 1000000ull);
    moq_media_sender_test_free(a);

    /* explicit 0 -> default */
    moq_media_sender_t *b = cfg_sender(0, true, sizeof(moq_media_sender_cfg_t));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(b), 1000000ull);
    moq_media_sender_test_free(b);

    /* old-size caller whose struct_size predates the field -> default */
    size_t old = offsetof(moq_media_sender_cfg_t, catalog_refresh_interval_us);
    moq_media_sender_t *c = cfg_sender(0, false, old);
    MOQ_TEST_CHECK(c != NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(c), 1000000ull);
    moq_media_sender_test_free(c);
    MOQ_TEST_PASS("refresh_cfg_default_1s");
}

static void test_cfg_custom_and_disable(void)
{
    moq_media_sender_t *a = cfg_sender(250000ull, true,
                                       sizeof(moq_media_sender_cfg_t));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(a), 250000ull);
    moq_media_sender_test_free(a);

    moq_media_sender_t *b = cfg_sender(UINT64_MAX, true,
                                       sizeof(moq_media_sender_cfg_t));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(b), UINT64_MAX);
    moq_media_sender_test_free(b);
    MOQ_TEST_PASS("refresh_cfg_custom_and_disable");
}

/* EXACT old-size ABI canary: allocate EXACTLY the pre-refresh prefix (the field
 * is not part of the allocation) and drive create against it. Under ASan any
 * attempt to read catalog_refresh_interval_us is a heap-buffer-overflow -- so a
 * green run proves the whole-field struct_size gate never reads past struct_size
 * on a genuine old caller. Portable: derives the size, no hard-coded 168. */
static void test_cfg_exact_old_size(void)
{
    size_t old = offsetof(moq_media_sender_cfg_t, catalog_refresh_interval_us);
    /* The field must begin at the aligned boundary after the previous last
     * field -- the same portable contract the _Static_assert pins (no literal). */
    size_t prev_end = offsetof(moq_media_sender_cfg_t, drop_without_demand) +
                      sizeof(((moq_media_sender_cfg_t *)0)->drop_without_demand);
    size_t aligned = (prev_end + _Alignof(uint64_t) - 1) &
                     ~(size_t)(_Alignof(uint64_t) - 1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)old, (uint64_t)aligned);

    moq_media_sender_cfg_t *cfg = (moq_media_sender_cfg_t *)malloc(old);
    MOQ_TEST_CHECK(cfg != NULL);
    moq_media_sender_cfg_init_live_sized(cfg, old);   /* writes only [0, old) */
    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    cfg->namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(cfg);
    MOQ_TEST_CHECK(s != NULL);
    /* Gate never touched the (unallocated) field -> default. */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(s), 1000000ull);
    moq_media_sender_test_free(s);
    free(cfg);
    MOQ_TEST_PASS("refresh_cfg_exact_old_size");
}

/* Poisoned-tail read-gate: a full-size struct whose field bytes are left
 * poisoned but whose struct_size stops at the old prefix. The whole-field gate
 * must ignore the poisoned bytes and resolve the default (not the garbage). */
static void test_cfg_poisoned_tail(void)
{
    size_t old = offsetof(moq_media_sender_cfg_t, catalog_refresh_interval_us);
    moq_media_sender_cfg_t cfg;
    memset(&cfg, 0xAA, sizeof(cfg));                  /* poison incl. the field */
    moq_media_sender_cfg_init_live_sized(&cfg, old);  /* struct_size = old */
    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    MOQ_TEST_CHECK_EQ_U64((uint64_t)cfg.struct_size, (uint64_t)old);
    /* The field still holds 0xAAAA...; the gate must not read it. */
    MOQ_TEST_CHECK(cfg.catalog_refresh_interval_us != 0);   /* really poisoned */
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(s != NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_refresh_interval(s), 1000000ull);
    moq_media_sender_test_free(s);
    MOQ_TEST_PASS("refresh_cfg_poisoned_tail");
}

/* ======================================================================== *
 *  Behavioral (SimPair peer + PRODUCTION hook)
 * ======================================================================== */

typedef struct { int ready_n; } scb_t;
static void on_ready(void *ctx, moq_media_sender_t *s)
{ (void)s; ((scb_t *)ctx)->ready_n++; }

static moq_simpair_t *pair(moq_alloc_t *alloc, moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
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

static moq_media_track_t *add_video(moq_media_sender_t *s)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1500000; tc.is_live = true;
    moq_media_track_t *t = NULL;
    (void)moq_media_sender_add_track(s, &tc, &t);
    return t;
}

/* Build a publish-mode sender (catalog demand = accepted PUBLISH) and drive it
 * to ready against a peer that accepts the namespace and every PUBLISH. Returns
 * the sender; *out_ready reports the ready-callback count. Leaves `now`
 * unchanged. The peer's server session is `srv`, driven from `cl`. */
static moq_media_sender_t *ready_publish_sender(
    moq_simpair_t *sp, scb_t *cb, uint64_t now, bool refresh_disabled,
    uint64_t interval)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.publish_tracks = true;
    cfg.catalog_refresh_interval_us = refresh_disabled ? UINT64_MAX : interval;
    cfg.callbacks.ctx = cb;
    cfg.callbacks.on_ready = on_ready;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    (void)add_video(s);

    for (int cycle = 0; cycle < 12 && !cb->ready_n; cycle++) {
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
                moq_accept_publish_cfg_t ac;
                moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
                (void)moq_session_accept_publish(srv,
                    ev.u.publish_request.pub, &ac, now);
            }
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    return s;
}

static void drain_pair(moq_simpair_t *sp)
{
    moq_event_t e;
    while (moq_session_poll_events(moq_simpair_client(sp), &e, 1) == 1)
        moq_event_cleanup(&e);
    while (moq_session_poll_events(moq_simpair_server(sp), &e, 1) == 1)
        moq_event_cleanup(&e);
}

/* Pump the sender hook and settle the link. */
static void pump(moq_media_sender_t *s, moq_simpair_t *sp, uint64_t now)
{
    moq_media_sender_test_pump(s, moq_simpair_client(sp), now);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
}

/* Build a pull-mode sender (ready) with an established persistent catalog
 * SUBSCRIBE on the peer (LARGEST_OBJECT, forward) -- the demand that drives the
 * refresh AND a real subscriber whose OBJECT_RECEIVED events the caller can
 * count. Returns the sender; *sub_out is the subscription handle. */
static moq_media_sender_t *pull_catalog_sub(moq_simpair_t *sp, uint64_t interval,
                                            uint64_t t0,
                                            moq_subscription_t *sub_out)
{
    moq_session_t *srv = moq_simpair_server(sp);
    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.publish_tracks = false;
    cfg.catalog_refresh_interval_us = interval;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    (void)add_video(s);

    moq_event_t ev;
    for (int c = 0; c < 12 && !moq_media_sender_is_ready(s); c++) {
        pump(s, sp, t0);
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(srv,
                    ev.u.namespace_published.ann, &ac, t0);
            }
            moq_event_cleanup(&ev);
        }
        pump(s, sp, t0);
    }
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));

    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns_parts, 2 };
    sc.track_name = (moq_bytes_t)MOQ_BYTES_LITERAL("catalog");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.has_forward = true; sc.forward = true;
    moq_subscription_t sub; memset(&sub, 0, sizeof(sub));
    MOQ_TEST_CHECK(moq_session_subscribe(srv, &sc, t0, &sub) == MOQ_OK);
    bool sub_ok = false;
    for (int i = 0; i < 16 && !sub_ok; i++) {
        pump(s, sp, t0);
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(sub_ok);
    *sub_out = sub;
    return s;
}

/* Drain the peer's events, counting catalog OBJECT_RECEIVED at (group g, obj o)
 * on the persistent subscribe path. */
static int count_sub_object(moq_simpair_t *sp, uint64_t g, uint64_t o)
{
    moq_session_t *srv = moq_simpair_server(sp);
    int n = 0; moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.group_id == g &&
            ev.u.object_received.object_id == o)
            n++;
        moq_event_cleanup(&ev);
    }
    return n;
}

/* The real relay shape. One persistent catalog
 * SUBSCRIBE (the relay's long-lived upstream subscription), a SINGLE initial
 * Joining FETCH for the current catalog (arriving as FETCH_OBJECT), then a later
 * automatic refresh delivered on the SUBSCRIBE path (arriving as OBJECT_RECEIVED
 * with a higher group) WITHOUT any second FETCH -- exactly the recovery a late
 * viewer needs where the relay resolves further Joining FETCHes locally. Both
 * drafts. */
static void test_refresh_pull_demand(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl  = moq_simpair_client(sp);   /* sender / publisher */
    moq_session_t *srv = moq_simpair_server(sp);   /* the persistent subscriber */
    uint64_t t0 = moq_simpair_now_us(sp);

    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.publish_tracks = false;                    /* pull: advertise + answer */
    cfg.catalog_refresh_interval_us = 1000000ull;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    (void)add_video(s);

    /* Advertise the namespace; the peer accepts it; the sender installs the
     * initial retained catalog and becomes ready. */
    moq_event_t ev;
    for (int cycle = 0; cycle < 12 && !moq_media_sender_is_ready(s); cycle++) {
        pump(s, sp, t0);
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac; moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(srv,
                    ev.u.namespace_published.ann, &ac, t0);
            }
            moq_event_cleanup(&ev);
        }
        pump(s, sp, t0);
    }
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));

    /* The persistent catalog SUBSCRIBE (LARGEST_OBJECT, forward): models a
     * relay's long-lived upstream subscription. The sender's pub facade
     * auto-accepts (ACCEPT_ALL). */
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns_parts, 2 };
    sc.track_name = (moq_bytes_t)MOQ_BYTES_LITERAL("catalog");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.has_forward = true; sc.forward = true;
    moq_subscription_t sub; memset(&sub, 0, sizeof(sub));
    MOQ_TEST_CHECK(moq_session_subscribe(srv, &sc, t0, &sub) == MOQ_OK);

    bool sub_ok = false;
    for (int i = 0; i < 16 && !sub_ok; i++) {
        pump(s, sp, t0);
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(sub_ok);

    /* ONE initial Joining FETCH for the current catalog (group 0). Fetch-path
     * objects arrive as FETCH_OBJECT -- distinct from subscribe delivery. */
    int fetches = 0;
    moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
    fc.is_joining = true; fc.joining_relative = false;
    fc.joining_sub = sub; fc.joining_start = 0;
    moq_fetch_t fetch; memset(&fetch, 0, sizeof(fetch));
    MOQ_TEST_CHECK(moq_session_fetch(srv, &fc, t0, &fetch) == MOQ_OK);
    fetches++;

    bool got_fetch_g0 = false;
    for (int i = 0; i < 16 && !got_fetch_g0; i++) {
        pump(s, sp, t0);
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT &&
                ev.u.fetch_object.group_id == 0)
                got_fetch_g0 = true;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(got_fetch_g0);            /* initial catalog via the fetch */

    /* Advance past the refresh deadline. The later independent catalog (group 1)
     * must arrive on the SUBSCRIBE path (OBJECT_RECEIVED), with NO second fetch. */
    uint64_t td = t0 + 1000000ull;
    bool got_sub_refresh = false;
    for (int i = 0; i < 16 && !got_sub_refresh; i++) {
        pump(s, sp, td);
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.group_id >= 1)
                got_sub_refresh = true;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(got_sub_refresh);         /* refresh delivered on the subscribe */
    MOQ_TEST_CHECK_EQ_INT(fetches, 1);       /* only the initial fetch */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_pull_demand_d18" : "refresh_pull_demand_d16");
}

/* A CLEAN terminal close must disarm the cached managed-adapter wake deadline
 * atomically with the close latch -- otherwise a finite deadline could survive
 * to a later hook cycle that returns early (e.g. session == NULL) before the
 * next recompute, and the idle managed loop would wake forever on a stale time.
 * Arm a finite deadline, fire a clean close via the seam, and require the
 * deadline query to read UINT64_MAX immediately, with NO further pump. */
static void test_refresh_clean_close_disarms(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, 1000000ull);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    /* The end-of-hook recompute has armed a finite cached deadline. */
    MOQ_TEST_CHECK(moq_media_sender_test_next_deadline_us(s) != UINT64_MAX);

    /* A clean (non-fatal) close -- no further pump afterward. */
    moq_media_sender_test_fire_closed(s, false, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_next_deadline_us(s), UINT64_MAX);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_clean_close_disarms_d18" : "refresh_clean_close_disarms_d16");
}

/* Persistent publish-mode demand: no refresh before the deadline, exactly one
 * new independent group at the deadline, then re-armed (cadence resets). */
static void test_refresh_publish_demand(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, 1000000ull);
    MOQ_TEST_CHECK(s != NULL);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);
    unsigned base_installs = moq_media_sender_test_retained_installs(s);

    /* Before the deadline: pump repeatedly, group stays 0. */
    for (uint64_t t = t0; t < t0 + 1000000ull; t += 200000ull) {
        moq_media_sender_test_pump(s, cl, t);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    /* At the deadline: exactly one new independent group. */
    uint64_t td = t0 + 1000000ull;
    moq_media_sender_test_pump(s, cl, td);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), base_installs + 1);

    /* Same instant, pump again: re-armed, not due -> still one group. */
    moq_media_sender_test_pump(s, cl, td);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    /* Next interval elapses -> second group. Cadence did not drift. */
    uint64_t td2 = td + 1000000ull;
    moq_media_sender_test_pump(s, cl, td2);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 2);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_publish_demand_d18" : "refresh_publish_demand_d16");
}

/* No demand: a pull-mode sender whose namespace is accepted but whose catalog
 * is never subscribed must NOT refresh -- no group advance, no install. */
static void test_refresh_no_demand(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    uint64_t t0 = moq_simpair_now_us(sp);

    g_ns_parts[0] = (moq_bytes_t)MOQ_BYTES_LITERAL("svc");
    g_ns_parts[1] = (moq_bytes_t)MOQ_BYTES_LITERAL("demo");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.publish_tracks = false;                 /* pull: advertise only */
    cfg.catalog_refresh_interval_us = 1000000ull;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    (void)add_video(s);

    /* Accept the namespace so the sender becomes ready, but never subscribe. */
    for (int cycle = 0; cycle < 12 && !moq_media_sender_is_ready(s); cycle++) {
        moq_media_sender_test_pump(s, cl, t0);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(srv,
                    ev.u.namespace_published.ann, &ac, t0);
            }
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    unsigned base_installs = moq_media_sender_test_retained_installs(s);

    /* Pump well past several deadlines with no subscriber: no group advance. */
    for (uint64_t t = t0; t < t0 + 5000000ull; t += 500000ull) {
        moq_media_sender_test_pump(s, cl, t);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), base_installs);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_no_demand_d18" : "refresh_no_demand_d16");
}

/* Explicit disable (UINT64_MAX): with demand and time well past any interval,
 * the group never advances. */
static void test_refresh_disabled(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, true, 0);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));

    for (uint64_t t = t0; t < t0 + 10000000ull; t += 1000000ull) {
        moq_media_sender_test_pump(s, cl, t);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_disabled_d18" : "refresh_disabled_d16");
}

/* Demand present, deadline already elapsed by the FIRST post-ready pump: the
 * refresh fires promptly (one group), not skipped. */
static void test_refresh_demand_after_deadline(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    /* Short interval; ready is reached at t0, deadline = t0 + 100000. */
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, 100000ull);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    /* Jump far past the deadline in one step: exactly one group (not many). */
    moq_media_sender_test_pump(s, cl, t0 + 5000000ull);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_demand_after_deadline_d18"
        : "refresh_demand_after_deadline_d16");
}

/* A real mutation takes precedence and RESETS the refresh cadence. Register a
 * post-ready add-track generation WELL BEFORE the refresh deadline; that commit
 * re-arms the deadline one interval past the mutation. At the ORIGINAL refresh
 * deadline the refresh must therefore NOT be due (no extra group); the next
 * refresh lands one interval after the mutation, not after the initial catalog.
 * This proves both precedence (no redundant refresh atop the mutation) and the
 * cadence reset. */
static void test_refresh_mutation_precedence(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *srv = moq_simpair_server(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, 1000000ull);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    /* Add a real second (video) track post-ready -- a genuine catalog change. */
    moq_media_track_cfg_t tc; moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v2", 2 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 800000; tc.is_live = true;
    moq_media_track_t *a = NULL;
    MOQ_TEST_CHECK(moq_media_sender_add_track(s, &tc, &a) == MOQ_OK);

    /* Register + commit the mutation generation at tm, WELL before the refresh
     * deadline (t0 + 1s). Pump generously, accepting the new track's PUBLISH. */
    uint64_t tm = t0 + 200000ull;
    for (int i = 0; i < 12 &&
             moq_media_sender_test_catalog_group(s) == 0; i++) {
        moq_media_sender_test_pump(s, cl, tm);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        while (moq_session_poll_events(srv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                moq_accept_publish_cfg_t ac;
                moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
                (void)moq_session_accept_publish(srv,
                    ev.u.publish_request.pub, &ac, tm);
            }
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    /* Exactly one generation from the mutation (a real add, not a refresh --
     * the deadline has not been reached). */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    /* At the ORIGINAL refresh deadline: the mutation reset the cadence, so the
     * refresh is NOT yet due -> group stays 1 (no redundant refresh). */
    moq_media_sender_test_pump(s, cl, t0 + 1000000ull);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    /* One interval past the MUTATION commit: the refresh now fires -> group 2. */
    moq_media_sender_test_pump(s, cl, tm + 1000000ull);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 2);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_mutation_precedence_d18"
        : "refresh_mutation_precedence_d16");
}

/* WOULD_BLOCK during a refresh generation: the fault seam forces object 0 to
 * block once; the cursor retries the same object next pump with no duplicate /
 * skipped group and no cadence drift (exactly one group results). */
static void test_refresh_would_block(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, 1000000ull);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    unsigned base_installs = moq_media_sender_test_retained_installs(s);

    /* Arm a one-shot WOULD_BLOCK before object 0 of the next generation. */
    moq_media_sender_test_block_republish_before(s, 0);

    uint64_t td = t0 + 1000000ull;
    moq_media_sender_test_pump(s, cl, td);       /* stages, blocks on obj 0 */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    /* The block fired: the generation is staged but not committed. */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    /* Retry at the same instant: the cursor resumes, one group commits. */
    for (int i = 0; i < 4 &&
             moq_media_sender_test_catalog_group(s) == 0; i++) {
        moq_media_sender_test_pump(s, cl, td);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), base_installs + 1);

    /* No second (duplicate) group at the same instant. */
    moq_media_sender_test_pump(s, cl, td);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_would_block_d18" : "refresh_would_block_d16");
}

/* Finite interval never silently becomes disabled. Two distinct arithmetic
 * hazards must BOTH saturate to the armed sentinel UINT64_MAX-1, never the
 * disabled/unarmed UINT64_MAX: (a) now+interval OVERFLOWS, and (b) now+interval
 * equals UINT64_MAX EXACTLY (no overflow, but aliases the disabled sentinel).
 * `mode` selects the case; each drives one sender to its saturated deadline and
 * requires exactly one refresh there. */
static void deadline_saturation_case(moq_version_t ver, int exact_max)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    /* exact-max: interval == UINT64_MAX - t0 so now(t0)+interval == UINT64_MAX
     * exactly. overflow: UINT64_MAX-1 so now(t0)+interval wraps. Either way the
     * armed deadline must be UINT64_MAX-1. */
    uint64_t interval = exact_max ? (UINT64_MAX - t0) : (UINT64_MAX - 1);
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, interval);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    moq_media_sender_test_pump(s, cl, t0 + 1000000ull);   /* before deadline */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);

    moq_media_sender_test_pump(s, cl, UINT64_MAX - 1);     /* at the deadline */
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

static void test_refresh_deadline_saturation(moq_version_t ver)
{
    deadline_saturation_case(ver, 0);   /* overflow */
    deadline_saturation_case(ver, 1);   /* exact UINT64_MAX collision */
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_deadline_saturation_d18" : "refresh_deadline_saturation_d16");
}

/* Retained-install backpressure: a distinct one-shot WOULD_BLOCK at
 * moq_pub_set_retained_group(), AFTER the live object is written. Driven with a
 * real persistent catalog subscriber so the EXACTLY-ONCE property is proven by
 * counting the peer's OBJECT_RECEIVED events -- not just internal counters. The
 * live group-1/object-0 must be delivered EXACTLY ONCE across the blocked
 * attempt AND the retry; the group/deadline must not commit early; the retry
 * installs without re-writing the object. */
static void test_refresh_retained_block(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    uint64_t t0 = moq_simpair_now_us(sp);
    moq_subscription_t sub;
    moq_media_sender_t *s = pull_catalog_sub(sp, 1000000ull, t0, &sub);
    (void)sub;
    unsigned base_installs = moq_media_sender_test_retained_installs(s);
    /* Drain any subscribe-path traffic emitted during setup. */
    int seen_g1o0 = count_sub_object(sp, 1, 0);
    MOQ_TEST_CHECK_EQ_INT(seen_g1o0, 0);

    /* Arm the retained-install block for the next (refresh) generation. */
    moq_media_sender_test_block_retained_install_once(s);

    uint64_t td = t0 + 1000000ull;
    pump(s, sp, td);                  /* live-writes group1/obj0, blocks install */
    seen_g1o0 += count_sub_object(sp, 1, 0);
    /* Not committed early: group unchanged, no install; but the live object WAS
     * sent to the subscriber exactly once already. */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 0);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), base_installs);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
    MOQ_TEST_CHECK_EQ_INT(seen_g1o0, 1);   /* delivered once on the blocked pump */

    /* Retry: install completes, one group commits, exactly one install, and the
     * live object is NOT re-written (cursor already at count). */
    for (int i = 0; i < 4 &&
             moq_media_sender_test_catalog_group(s) == 0; i++) {
        pump(s, sp, td);
        seen_g1o0 += count_sub_object(sp, 1, 0);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), base_installs + 1);

    /* One more pump at the same instant: no duplicate group, no re-delivery. */
    pump(s, sp, td);
    seen_g1o0 += count_sub_object(sp, 1, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s), 1);
    MOQ_TEST_CHECK_EQ_INT(seen_g1o0, 1);   /* EXACTLY ONCE across block + retry */

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_retained_block_d18" : "refresh_retained_block_d16");
}

/* Group ceiling: the real ceiling is the QUIC varint max, not
 * UINT64_MAX -- the facade rejects a location above MOQ_QUIC_VARINT_MAX. A
 * generation counter one BELOW the ceiling may advance once to the ceiling
 * value; AT the ceiling a refresh must refuse cleanly (no advance, no install,
 * NOT fatal -- staging an unencodable group would otherwise turn the sender
 * fatal). */
static void test_refresh_group_ceiling(moq_version_t ver)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = pair(&alloc, ver);
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t t0 = moq_simpair_now_us(sp);
    scb_t cb = {0};
    moq_media_sender_t *s = ready_publish_sender(sp, &cb, t0, false, 1000000ull);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(s));

    /* One below the varint ceiling: a refresh may advance exactly once, to the
     * last encodable group (MOQ_QUIC_VARINT_MAX). */
    moq_media_sender_test_set_catalog_group(s, MOQ_QUIC_VARINT_MAX - 1);
    unsigned installs0 = moq_media_sender_test_retained_installs(s);
    moq_media_sender_test_pump(s, cl, t0 + 1000000ull);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s),
                          MOQ_QUIC_VARINT_MAX);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), installs0 + 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

    /* AT the ceiling: the next group would be unencodable -> clean refusal. */
    unsigned installs1 = moq_media_sender_test_retained_installs(s);
    for (uint64_t t = t0 + 2000000ull; t < t0 + 6000000ull; t += 1000000ull) {
        moq_media_sender_test_pump(s, cl, t);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
    }
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_catalog_group(s),
                          MOQ_QUIC_VARINT_MAX);
    MOQ_TEST_CHECK_EQ_U64(
        moq_media_sender_test_retained_installs(s), installs1);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));   /* NOT fatal */

    moq_media_sender_test_free(s);
    drain_pair(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_18 ?
        "refresh_group_ceiling_d18" : "refresh_group_ceiling_d16");
}

/* ======================================================================== *
 *  Receiver-side: repeated identical independent catalogs must not duplicate
 *  TRACK_ADDED nor replace stable handles (the relay-shape bootstrap: a late
 *  consumer that only ever sees a later refresh group still discovers tracks).
 * ======================================================================== */

#define TRK(name, role) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"" role "\"}"
#define CAT1(t1) "{\"version\":\"1\",\"tracks\":[" t1 "]}"
static const char *CAT_JSON = CAT1(TRK("vide_1", "video"));

static int count_added(moq_media_receiver_t *r, moq_media_track_t **first)
{
    int added = 0;
    moq_media_track_event_kind_t k; moq_media_track_t *t;
    while (moq_media_receiver_test_poll(r, &k, &t)) {
        if (k == MOQ_MEDIA_TRACK_ADDED) {
            if (added == 0 && first) *first = t;
            added++;
        }
    }
    return added;
}

/* A receiver fed the SAME independent catalog in successive groups (as the
 * periodic refresh emits) fires TRACK_ADDED exactly once and keeps one stable
 * handle -- refreshes never churn the track set. */
static void test_receiver_refresh_dedup(void)
{
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    MOQ_TEST_CHECK(r != NULL);

    /* group 0 object 0: the initial independent catalog. */
    moq_media_receiver_test_ingest(r, 0, 0, CAT_JSON, strlen(CAT_JSON));
    moq_media_track_t *h0 = NULL;
    MOQ_TEST_CHECK_EQ_INT(count_added(r, &h0), 1);
    MOQ_TEST_CHECK(h0 != NULL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 1);

    /* Successive refresh groups carrying the identical independent catalog. */
    for (uint64_t g = 1; g <= 4; g++) {
        moq_media_receiver_test_ingest(r, g, 0, CAT_JSON, strlen(CAT_JSON));
        moq_media_track_t *hn = NULL;
        MOQ_TEST_CHECK_EQ_INT(count_added(r, &hn), 0);   /* no new TRACK_ADDED */
    }
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 1);

    moq_media_receiver_test_free(r);
    MOQ_TEST_PASS("receiver_refresh_dedup");
}

/* Relay-shape bootstrap: a LATE consumer that missed group 0 entirely and only
 * ever receives a later refresh group (group 3) still discovers the track from
 * that single independent catalog object -- exactly the recovery a caching relay needs
 * when a relay resolves the Joining FETCH locally against nothing. */
static void test_receiver_late_bootstrap(void)
{
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    MOQ_TEST_CHECK(r != NULL);
    /* No group 0 ever arrives (the relay's local fetch returned nothing). The
     * first object this consumer sees is a periodic refresh at group 3. */
    moq_media_receiver_test_ingest(r, 3, 0, CAT_JSON, strlen(CAT_JSON));
    moq_media_track_t *h = NULL;
    MOQ_TEST_CHECK_EQ_INT(count_added(r, &h), 1);
    MOQ_TEST_CHECK(h != NULL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 1);
    moq_media_receiver_test_free(r);
    MOQ_TEST_PASS("receiver_late_bootstrap");
}

int main(void)
{
    test_cfg_default_interval();
    test_cfg_custom_and_disable();
    test_cfg_exact_old_size();
    test_cfg_poisoned_tail();
    test_receiver_refresh_dedup();
    test_receiver_late_bootstrap();
    for (int vi = 0; vi < 2; vi++) {
        moq_version_t ver = vi ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_refresh_publish_demand(ver);
        test_refresh_pull_demand(ver);
        test_refresh_no_demand(ver);
        test_refresh_disabled(ver);
        test_refresh_demand_after_deadline(ver);
        test_refresh_mutation_precedence(ver);
        test_refresh_would_block(ver);
        test_refresh_deadline_saturation(ver);
        test_refresh_retained_block(ver);
        test_refresh_group_ceiling(ver);
        test_refresh_clean_close_disarms(ver);
    }
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
