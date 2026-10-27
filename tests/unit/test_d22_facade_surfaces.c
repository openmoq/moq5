/*
 * Facade control surfaces:
 *   - the subscriber facade sets the independent delivery timeouts on a
 *     subscribe and on an update, and a Location Filter on an update;
 *   - a REDIRECT refusal or a per-request GOAWAY ends a facade track in a
 *     terminal state that keeps where the request went, on the subscriber
 *     facade (subscription; a fetch ends through its error item) and the
 *     publisher facade (a PUBLISH);
 *   - the publisher facade reports a session-wide GOAWAY's URI.
 */
#include <moq/moq.h>
#include <moq/subscriber.h>
#include <moq/publisher.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/wire/messages_d22_internal.h"
#include "../../core/src/wire/control_d22_internal.h"
#include "test_raw_pair_d22.h"
#include <string.h>

static moq_bytes_t g_parts[] = { { (const uint8_t *)"live", 4 } };

/* -- Subscriber facade ------------------------------------------------------ */

typedef struct {
    int      subscribed, errors, done;
    uint64_t error_code, done_status;
    moq_sub_track_t *track;
} sub_obs_t;

static void on_subscribed(void *ctx, moq_sub_track_t *t)
{
    sub_obs_t *o = (sub_obs_t *)ctx;
    o->subscribed++;
    o->track = t;
}

static void on_subscribe_error(void *ctx, moq_sub_track_t *t, moq_request_error_t code,
                               moq_bytes_t reason)
{
    (void)reason;
    sub_obs_t *o = (sub_obs_t *)ctx;
    o->errors++;
    o->error_code = code;
    o->track = t;
}

static void on_subscribe_done(void *ctx, moq_sub_track_t *t, uint64_t status)
{
    sub_obs_t *o = (sub_obs_t *)ctx;
    o->done++;
    o->done_status = status;
    o->track = t;
}

static moq_subscriber_t *make_sub(moq_session_t *c, sub_obs_t *o)
{
    moq_sub_cfg_t cfg;
    moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_sub_callbacks_init(&cfg.callbacks);
    cfg.callbacks.ctx = o;
    cfg.callbacks.on_subscribed = on_subscribed;
    cfg.callbacks.on_subscribe_error = on_subscribe_error;
    cfg.on_subscribe_done = on_subscribe_done;
    moq_subscriber_t *sub = NULL;
    if (moq_sub_create(c, moq_alloc_default(), &cfg, &sub) != MOQ_OK) return NULL;
    return sub;
}

static void track_cfg(moq_sub_track_cfg_t *t, const char *name)
{
    moq_sub_track_cfg_init(t);
    t->track_namespace = (moq_namespace_t){ g_parts, 1 };
    t->track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
}

/* The facade carries the independent delivery timeouts a subscribe and an
 * update ask for, and an update's semantic Location Filter, exactly as the
 * session's own cfgs do (draft-22 §9.20.4/§9.20.7 OBJECT/SUBGROUP_DELIVERY_
 * TIMEOUT, §9.20.9 LOCATION_FILTER); draft-18 carries the timeouts too
 * (10.2.3, 10.2.4). */
static int test_subscriber_options(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    sub_obs_t o; memset(&o, 0, sizeof(o));
    moq_subscriber_t *sub = make_sub(c, &o);
    MOQ_TEST_CHECK(sub != NULL);
    moq_sub_track_cfg_t tc;
    track_cfg(&tc, "v");
    const uint64_t object_ms = 100, subgroup_ms = 0;   /* exact, zero included */
    tc.object_delivery_timeout_ms = &object_ms;
    tc.subgroup_delivery_timeout_ms = &subgroup_ms;
    moq_sub_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_subscribe(sub, &tc, 2, &t), MOQ_OK);
    run(c, sv, 3);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    moq_subscription_t sh = MOQ_SUBSCRIPTION_INVALID;
    if (got) {
        sh = ev.u.subscribe_request.sub;
        const moq_subscription_admission_options_t *op = ev.u.subscribe_request.options;
        MOQ_TEST_CHECK(op != NULL);
        if (op) {
            MOQ_TEST_CHECK((op->flags & MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT) &&
                           (op->flags & MOQ_SUBSCRIPTION_ADMISSION_SUBGROUP_DELIVERY_TIMEOUT));
            MOQ_TEST_CHECK_EQ_U64(op->object_delivery_timeout_ms, 100);
            MOQ_TEST_CHECK_EQ_U64(op->subgroup_delivery_timeout_ms, 0);
        }
        moq_event_cleanup(&ev);
    }
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sh, &ac, 4), MOQ_OK);
    run(c, sv, 5);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, 5), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.subscribed, 1);

    /* The update: a Location Filter and one timeout; the other stays. */
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_GROUP_END;
    loc.start_group = 6; loc.start_object = 0; loc.end_group = 9;
    moq_sub_update_cfg_t u;
    moq_sub_update_cfg_init_sized(&u, sizeof(u));
    u.location = &loc;
    u.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
    u.object_delivery_timeout_ms = 300;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(sub, t, &u, 7), MOQ_OK);
    run(c, sv, 8);
    got = take(sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        const moq_subscription_update_options_t *op = ev.u.subscribe_updated.options;
        MOQ_TEST_CHECK(op != NULL);
        if (op) {
            MOQ_TEST_CHECK(op->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION);
            MOQ_TEST_CHECK(op->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_OBJECT_DELIVERY_TIMEOUT);
            MOQ_TEST_CHECK(!(op->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_SUBGROUP_DELIVERY_TIMEOUT));
            MOQ_TEST_CHECK_EQ_U64(op->location.kind, MOQ_LOCATION_FILTER_GROUP_END);
            MOQ_TEST_CHECK_EQ_U64(op->location.start_group, 6);
            MOQ_TEST_CHECK_EQ_U64(op->location.end_group, 9);
            MOQ_TEST_CHECK_EQ_U64(op->object_delivery_timeout_ms, 300);
        }
        moq_event_cleanup(&ev);
    }
    /* A caller whose struct ends before `location` sends no filter: the
     * block past its size is never read. */
    run(c, sv, 9);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, 9), MOQ_OK);
    moq_sub_update_cfg_t old;
    moq_sub_update_cfg_init_sized(&old, offsetof(moq_sub_update_cfg_t, _reserved_location));
    old.location = &loc;                        /* past the caller's size */
    old.delivery_timeout_flags = 0xff;          /* likewise never read */
    old.has_subscriber_priority = true;
    old.subscriber_priority = 9;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(sub, t, &old, 10), MOQ_OK);
    run(c, sv, 11);
    got = take(sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        const moq_subscription_update_options_t *op = ev.u.subscribe_updated.options;
        if (op) MOQ_TEST_CHECK(!(op->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION));
        moq_event_cleanup(&ev);
    }
    moq_sub_destroy(sub);
    destroy(c, sv);

    /* Draft-18 defines the independent timeouts too (10.2.3, 10.2.4). */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_18));
    memset(&o, 0, sizeof(o));
    sub = make_sub(c, &o);
    track_cfg(&tc, "v");
    tc.object_delivery_timeout_ms = &object_ms;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_subscribe(sub, &tc, 2, &t), MOQ_OK);
    /* A caller whose struct ends before the timeout pointers sends none; a
     * second track, as draft-18 has one subscription per track. */
    track_cfg(&tc, "w");
    tc.object_delivery_timeout_ms = &object_ms;
    tc.struct_size = (uint32_t)offsetof(moq_sub_track_cfg_t, object_delivery_timeout_ms);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_subscribe(sub, &tc, 3, &t), MOQ_OK);
    moq_sub_destroy(sub);
    destroy(c, sv);
    return failures;
}

static bool bytes_eq(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && (b.len == 0 || memcmp(b.data, s, b.len) == 0);
}

/* A REDIRECT refusal and a per-request GOAWAY each end the subscription,
 * and the track keeps the target (draft-22 §9.4.1 Redirect, §9.2 GOAWAY on a
 * request stream: "re-issue that specific request on a session at the
 * specified URI"). */
static int test_subscriber_migration(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    sub_obs_t o; memset(&o, 0, sizeof(o));
    moq_subscriber_t *sub = make_sub(c, &o);
    moq_sub_track_cfg_t tc;
    track_cfg(&tc, "v");
    moq_sub_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_subscribe(sub, &tc, 2, &t), MOQ_OK);
    run(c, sv, 3);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_bytes_t alt[] = { MOQ_BYTES_LITERAL("alt") };
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_REDIRECT;
    rj.redirect.connect_uri = MOQ_BYTES_LITERAL("moqt://other.example/");
    rj.redirect.track_namespace = (moq_namespace_t){ alt, 1 };
    rj.redirect.track_name = MOQ_BYTES_LITERAL("v2");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(sv, sh, &rj, 4), MOQ_OK);
    run(c, sv, 5);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, 5), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(o.error_code, MOQ_REQUEST_ERROR_REDIRECT);
    moq_request_migration_t mg;
    memset(&mg, 0xa5, sizeof(mg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_get_track_migration(sub, t, &mg, sizeof(mg)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(mg.struct_size, sizeof(mg));
    MOQ_TEST_CHECK(mg.redirect && !mg.goaway);
    MOQ_TEST_CHECK(bytes_eq(mg.target.connect_uri, "moqt://other.example/"));
    MOQ_TEST_CHECK(mg.target.track_namespace.count == 1 &&
                   bytes_eq(mg.target.track_namespace.parts[0], "alt"));
    MOQ_TEST_CHECK(bytes_eq(mg.target.track_name, "v2"));
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_get_track_migration(sub, t, &mg,
                          offsetof(moq_request_migration_t, target)), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_release_track(sub, t), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_get_track_migration(sub, t, &mg, sizeof(mg)), MOQ_ERR_INVAL);

    /* A live subscription migrated by GOAWAY on its request stream. */
    memset(&o, 0, sizeof(o));
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_subscribe(sub, &tc, 6, &t), MOQ_OK);
    run(c, sv, 7);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sh, &ac, 8), MOQ_OK);
    run(c, sv, 9);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, 9), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.subscribed, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_get_track_migration(sub, t, &mg, sizeof(mg)), MOQ_DONE);
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    gc.new_session_uri = MOQ_BYTES_LITERAL("moqt://next.example/");
    gc.timeout_ms = 7000;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_request_goaway_subscribe(sv, sh, &gc, 10), MOQ_OK);
    run(c, sv, 11);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, 11), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.done, 1);
    MOQ_TEST_CHECK_EQ_U64(o.done_status, 0x4);        /* GOING_AWAY, §12.4 */
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_get_track_migration(sub, t, &mg, sizeof(mg)), MOQ_OK);
    MOQ_TEST_CHECK(mg.goaway && !mg.redirect);
    MOQ_TEST_CHECK_EQ_U64(mg.timeout_ms, 7000);
    MOQ_TEST_CHECK(bytes_eq(mg.target.connect_uri, "moqt://next.example/"));
    MOQ_TEST_CHECK(mg.target.track_namespace.count == 0 && mg.target.track_name.len == 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_release_track(sub, t), MOQ_OK);

    /* A fetch refused with REDIRECT ends through its error item. */
    moq_sub_fetch_cfg_t fc;
    moq_sub_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ g_parts, 1 };
    fc.track_name = MOQ_BYTES_LITERAL("v");
    fc.end_group = 4;
    moq_sub_fetch_req_t *fr = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_fetch(sub, &fc, 12, &fr), MOQ_OK);
    run(c, sv, 13);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_FETCH_REQUEST, &ev));
    moq_fetch_t sf = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);
    moq_reject_fetch_cfg_t rf;
    moq_reject_fetch_cfg_init(&rf);
    rf.error_code = MOQ_REQUEST_ERROR_REDIRECT;
    rf.redirect.connect_uri = MOQ_BYTES_LITERAL("moqt://other.example/");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_fetch(sv, sf, &rf, 14), MOQ_OK);
    run(c, sv, 15);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, 15), MOQ_OK);
    moq_sub_fetch_item_t it;
    bool err_item = false;
    while (moq_sub_poll_fetch(sub, &it) == MOQ_OK) {
        if (it.kind == MOQ_SUB_FETCH_ERROR && it.request == fr) {
            err_item = true;
            MOQ_TEST_CHECK_EQ_U64(it.u.error.error_code, MOQ_REQUEST_ERROR_REDIRECT);
        }
        moq_sub_fetch_item_cleanup(&it);
    }
    MOQ_TEST_CHECK(err_item);
    moq_sub_destroy(sub);
    destroy(c, sv);
    return failures;
}

/* Deliver both sides' actions, including STOP_SENDING on request streams. */
static void settle_with_stops(moq_session_t *c, moq_session_t *sv, uint64_t now)
{
    for (int round = 0; round < 8; round++) {
        moq_session_t *from[2] = { c, sv }, *to[2] = { sv, c };
        for (int d = 0; d < 2; d++) {
            moq_action_t a;
            while (moq_session_poll_actions(from[d], &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_STOP_BIDI_STREAM) {
                    /* QUIC: the peer's transport answers STOP_SENDING with
                     * RESET_STREAM on that stream. */
                    (void)moq_session_on_bidi_stream_stop(to[d], a.u.stop_bidi_stream.stream_ref,
                                                          a.u.stop_bidi_stream.error_code, now);
                    (void)moq_session_on_bidi_stream_reset(from[d], a.u.stop_bidi_stream.stream_ref,
                                                           a.u.stop_bidi_stream.error_code, now);
                } else
                    deliver(to[d], &a, d == 1, now);
                moq_action_cleanup(&a);
            }
        }
    }
}

/* A TRACK_STATUS migrated by a GOAWAY on its request stream (draft-22
 * §9.2, draft-18 §10.4) ends with exactly one error outcome, GOING_AWAY,
 * correlated to its request, on either poll API, with or without the
 * peer's FIN after it; polling it frees its slot, so migrations never run
 * the bounded status table dry (more of them than it has slots). */
static int test_subscriber_status_migration(moq_version_t v)
{
    int failures = 0;
    moq_session_t *c, *sv;
    side_cfg_t sc = { .version = v };
    c = session(MOQ_PERSPECTIVE_CLIENT, &sc);
    /* The responder keeps each migrated request until its own side ends;
     * give it room for every one here. */
    moq_session_cfg_t scfg_sv;
    moq_session_cfg_init_sized(&scfg_sv, sizeof(scfg_sv), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg_sv.version = v;
    scfg_sv.send_request_capacity = true;
    scfg_sv.initial_request_capacity = 64;
    scfg_sv.max_track_statuses = 64;
    sv = NULL;
    MOQ_TEST_CHECK(c && moq_session_create(&scfg_sv, 0, &sv) == MOQ_OK);
    if (!c || !sv) { if (c) moq_session_destroy(c); if (sv) moq_session_destroy(sv); return failures; }
    (void)test_bind_if_required(sv);
    (void)moq_session_start(sv, 0);
    run(c, sv, 1);
    drain(c); drain(sv);
    sub_obs_t o; memset(&o, 0, sizeof(o));
    moq_subscriber_t *sub = make_sub(c, &o);
    MOQ_TEST_CHECK(sub != NULL);
    if (!sub) { destroy(c, sv); return failures; }
    for (int i = 0; i < 20; i++) {
        uint64_t now = 10 + (uint64_t)i * 10;
        moq_sub_status_cfg_t scfg;
        moq_sub_status_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ g_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("v");
        moq_sub_status_req_t *req = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_sub_track_status(sub, &scfg, now, &req), MOQ_OK);
        if (!req) break;
        moq_stream_ref_t ref = { 0 };
        moq_action_t a;
        while (moq_session_poll_actions(c, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) ref = a.u.open_bidi_stream.stream_ref;
            deliver(sv, &a, false, now);
            moq_action_cleanup(&a);
        }
        moq_event_t ev;
        bool got = take(sv, MOQ_EVENT_TRACK_STATUS_REQUEST, &ev);
        MOQ_TEST_CHECK(got);
        if (!got) break;
        moq_track_status_handle_t sh = ev.u.track_status_request.handle;
        moq_event_cleanup(&ev);
        moq_request_goaway_cfg_t gc;
        moq_request_goaway_cfg_init(&gc);
        gc.new_session_uri = MOQ_BYTES_LITERAL("moqt://next.example/");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_request_goaway_track_status(sv, sh, &gc, now + 1),
                              MOQ_OK);
        pump(sv, c, true, now + 1, 0);
        if (i % 2)      /* the responder also ends its side */
            (void)moq_session_on_bidi_stream_bytes(c, ref, NULL, 0, true, now + 2);
        MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, now + 2), MOQ_OK);
        int outcomes = 0;
        if (i % 4 < 2) {
            moq_sub_status_outcome_t out;
            while (moq_sub_poll_status_outcome(sub, &out, sizeof(out)) == MOQ_OK) {
                outcomes++;
                MOQ_TEST_CHECK(out.request == req);
                MOQ_TEST_CHECK_EQ_INT((int)out.kind, (int)MOQ_SUB_STATUS_OUTCOME_ERROR);
                MOQ_TEST_CHECK_EQ_U64(out.error_code, MOQ_REQUEST_ERROR_GOING_AWAY);
            }
        } else {
            moq_sub_status_result_t res;
            while (moq_sub_poll_status(sub, &res) == MOQ_OK) {
                outcomes++;
                MOQ_TEST_CHECK(res.request == req);
                MOQ_TEST_CHECK_EQ_INT((int)res.kind, (int)MOQ_SUB_STATUS_ERROR);
                MOQ_TEST_CHECK_EQ_U64(res.error_code, MOQ_REQUEST_ERROR_GOING_AWAY);
            }
        }
        MOQ_TEST_CHECK_EQ_INT(outcomes, 1);
        settle_with_stops(c, sv, now + 3);
        drain(sv);
        MOQ_TEST_CHECK_EQ_INT((int)moq_sub_tick(sub, now + 3), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    }
    moq_sub_destroy(sub);
    destroy(c, sv);
    return failures;
}

/* -- Publisher facade --------------------------------------------------------- */

typedef struct {
    int      ok, errors, finished, draining, goaways;
    uint64_t error_code;
    uint8_t  uri[64];
    size_t   uri_len;
} pub_obs_t;

static void on_publish_ok(void *ctx, moq_pub_track_t *t, bool forward)
{ (void)t; (void)forward; ((pub_obs_t *)ctx)->ok++; }
static void on_publish_error(void *ctx, moq_pub_track_t *t, moq_request_error_t code)
{ (void)t; pub_obs_t *o = (pub_obs_t *)ctx; o->errors++; o->error_code = code; }
static void on_publish_finished(void *ctx, moq_pub_track_t *t)
{ (void)t; ((pub_obs_t *)ctx)->finished++; }
static void on_draining(void *ctx) { ((pub_obs_t *)ctx)->draining++; }
static void on_goaway(void *ctx, moq_bytes_t uri)
{
    pub_obs_t *o = (pub_obs_t *)ctx;
    o->goaways++;
    o->uri_len = uri.len < sizeof(o->uri) ? uri.len : sizeof(o->uri);
    if (o->uri_len) memcpy(o->uri, uri.data, o->uri_len);
}

static moq_publisher_t *make_pub(moq_session_t *c, pub_obs_t *o, bool with_goaway,
                                 moq_pub_track_t **track)
{
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_pub_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = o;
    cfg.callbacks.on_publish_ok = on_publish_ok;
    cfg.callbacks.on_publish_error = on_publish_error;
    cfg.callbacks.on_publish_finished = on_publish_finished;
    cfg.callbacks.on_draining = on_draining;
    if (with_goaway) cfg.on_goaway = on_goaway;
    moq_publisher_t *pub = NULL;
    if (moq_pub_create(c, moq_alloc_default(), &cfg, &pub) != MOQ_OK) return NULL;
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){ g_parts, 1 };
    tc.track_name = MOQ_BYTES_LITERAL("v");
    if (moq_pub_add_track(pub, &tc, 1, track) != MOQ_OK) {
        moq_pub_destroy(pub);
        return NULL;
    }
    return pub;
}

static int pub_slot_of_track(moq_session_t *c)
{
    for (size_t i = 0; i < c->pub_cap; i++)
        if (c->publishes[i].state != MOQ_PUB_FREE &&
            c->publishes[i].role == MOQ_PUB_ROLE_PUBLISHER)
            return (int)i;
    return -1;
}

/* A REDIRECT refusal of the PUBLISH and a GOAWAY on its request stream end
 * the publication; the track keeps the target and can publish again. */
static int test_publisher_migration(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    pub_obs_t o; memset(&o, 0, sizeof(o));
    moq_pub_track_t *t = NULL;
    moq_publisher_t *pub = make_pub(c, &o, false, &t);
    MOQ_TEST_CHECK(pub != NULL);
    moq_pub_publish_cfg_t pc;
    moq_pub_publish_cfg_init(&pc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_publish_track(pub, t, &pc, 2), MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(c, &a, 1) > 0) moq_action_cleanup(&a); }
    int ps = pub_slot_of_track(c);
    MOQ_TEST_CHECK(ps >= 0);
    moq_request_migration_t mg;
    if (ps >= 0) {
        /* The peer answers the PUBLISH with a Redirect (draft-22 §9.4: a
         * REDIRECT may answer any request on this draft). */
        moq_d22_message_t m;
        memset(&m, 0, sizeof(m));
        m.type = MOQ_D22_REQUEST_ERROR;
        m.request_id = c->publishes[ps].request_id;
        m.code = MOQ_REQUEST_ERROR_REDIRECT;
        m.redirect_uri = MOQ_BYTES_LITERAL("moqt://other.example/");
        static const uint8_t empty_ns[] = { 0x00 };    /* same namespace */
        m.ns.encoded = (moq_bytes_t){ empty_ns, sizeof(empty_ns) };
        MOQ_TEST_CHECK_EQ_INT((int)feed(c, c->publishes[ps].rs.ref, &m, NULL, 0,
                                        MOQ_D22_PUBLISH, false, 3), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, 3), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(o.errors, 1);
        MOQ_TEST_CHECK_EQ_U64(o.error_code, MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_get_track_migration(pub, t, &mg, sizeof(mg)), MOQ_OK);
        MOQ_TEST_CHECK(mg.redirect && !mg.goaway);
        MOQ_TEST_CHECK(bytes_eq(mg.target.connect_uri, "moqt://other.example/"));
        MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, t));
    }
    moq_pub_destroy(pub);
    destroy(c, sv);

    /* Accepted, then migrated by a GOAWAY on its request stream. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
    memset(&o, 0, sizeof(o));
    pub = make_pub(c, &o, false, &t);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_publish_track(pub, t, &pc, 2), MOQ_OK);
    run(c, sv, 3);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t sp = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sp, &ac, 4), MOQ_OK);
    run(c, sv, 5);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, 5), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.ok, 1);
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, t));
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_get_track_migration(pub, t, &mg, sizeof(mg)), MOQ_DONE);
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    gc.new_session_uri = MOQ_BYTES_LITERAL("moqt://next.example/");
    gc.timeout_ms = 3000;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_request_goaway_publish(sv, sp, &gc, 6), MOQ_OK);
    run(c, sv, 7);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, 7), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.finished, 1);
    MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, t));
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_get_track_migration(pub, t, &mg, sizeof(mg)), MOQ_OK);
    MOQ_TEST_CHECK(mg.goaway && !mg.redirect && mg.timeout_ms == 3000);
    MOQ_TEST_CHECK(bytes_eq(mg.target.connect_uri, "moqt://next.example/"));
    /* Publishing again starts afresh: the old target is gone. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_publish_track(pub, t, &pc, 8), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_get_track_migration(pub, t, &mg, sizeof(mg)), MOQ_DONE);
    moq_pub_destroy(pub);
    destroy(c, sv);
    return failures;
}

/* A publisher session with one outgoing subgroup slot and a short action
 * queue, against a default server. */
static bool narrow_pair(moq_session_t **c, moq_session_t **sv, uint32_t max_actions)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_22;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.max_open_subgroups = 1;
    cfg.max_actions = max_actions;
    *c = NULL;
    *sv = NULL;
    if (moq_session_create(&cfg, 0, c) != MOQ_OK) return false;
    if (test_bind_if_required(*c) != MOQ_OK || moq_session_start(*c, 0) != MOQ_OK) {
        moq_session_destroy(*c);
        return false;
    }
    side_cfg_t sc = { .version = MOQ_VERSION_DRAFT_22 };
    *sv = session(MOQ_PERSPECTIVE_SERVER, &sc);
    if (!*sv) { moq_session_destroy(*c); return false; }
    run(*c, *sv, 1);
    drain(*c); drain(*sv);
    return moq_session_state(*c) == MOQ_SESS_ESTABLISHED;
}

/* Deliver the client's actions; note how the data stream `data` ended. */
static void pump_data(moq_session_t *c, moq_session_t *sv, uint64_t now, uint64_t *data,
                      int *fins, int *resets)
{
    moq_action_t a;
    while (moq_session_poll_actions(c, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            if (*data == 0) *data = a.u.send_data.stream_ref._v;
            if (a.u.send_data.stream_ref._v == *data && a.u.send_data.fin) (*fins)++;
        } else if (a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == *data) {
            (*resets)++;
        }
        deliver(sv, &a, false, now);
        moq_action_cleanup(&a);
    }
}

/* A per-request GOAWAY moves the publication, not its data (draft-22 §9.2;
 * §11.3.2: a stream handed off before its end is reset, never finished).
 * The facade keeps the subgroup it had open -- an open group, or a partial
 * streaming object -- until its reset is queued, across a full action
 * queue, while the track publishes again; with one subgroup slot, repeated
 * migration keeps working. Exactly one finished callback per migration. */
static int test_publisher_migration_streams(bool partial)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(narrow_pair(&c, &sv, 3));
    pub_obs_t o; memset(&o, 0, sizeof(o));
    moq_pub_track_t *t = NULL;
    moq_publisher_t *pub = make_pub(c, &o, false, &t);
    MOQ_TEST_CHECK(pub != NULL);
    uint8_t bytes[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(moq_alloc_default(), bytes, 4, &payload), MOQ_OK);
    for (uint64_t cycle = 0; cycle < 3; cycle++) {
        uint64_t now = 10 + cycle * 10;
        moq_pub_publish_cfg_t pc;
        moq_pub_publish_cfg_init(&pc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_publish_track(pub, t, &pc, now), MOQ_OK);
        run(c, sv, now);
        moq_event_t ev;
        bool got = take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev);
        MOQ_TEST_CHECK(got);
        if (!got) break;
        moq_publication_t sp = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sp, &ac, now + 1), MOQ_OK);
        run(c, sv, now + 1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, now + 1), MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_track_is_published(pub, t));
        if (partial) {
            moq_pub_begin_object_cfg_t bc;
            moq_pub_begin_object_cfg_init(&bc);
            bc.group_id = cycle;
            bc.object_id = 0;
            bc.payload_length = 8;
            MOQ_TEST_CHECK_EQ_INT((int)moq_pub_begin_object(pub, t, &bc, now + 2), MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_data(pub, t, payload, now + 2), MOQ_OK);
        } else {
            MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_object(pub, t, cycle, 0, payload, now + 2),
                                  MOQ_OK);
        }
        uint64_t data = 0;
        int fins = 0, resets = 0;
        pump_data(c, sv, now + 2, &data, &fins, &resets);
        MOQ_TEST_CHECK(data != 0);
        drain(sv);
        moq_request_goaway_cfg_t gc;
        moq_request_goaway_cfg_init(&gc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_request_goaway_publish(sv, sp, &gc, now + 3),
                              MOQ_OK);
        pump(sv, c, true, now + 3, 0);
        /* The middle migration meets a full action queue. */
        if (cycle == 1) {
            moq_track_status_cfg_t tc;
            moq_track_status_cfg_init(&tc);
            tc.track_namespace = (moq_namespace_t){ g_parts, 1 };
            tc.track_name = MOQ_BYTES_LITERAL("w");
            moq_track_status_handle_t th;
            while (moq_session_track_status(c, &tc, now + 3, &th) == MOQ_OK) {}
            (void)moq_pub_tick(pub, now + 3);
            MOQ_TEST_CHECK_EQ_INT(o.finished, (int)cycle + 1);
            MOQ_TEST_CHECK(!moq_pub_track_is_published(pub, t));
            moq_action_t a;
            while (moq_session_poll_actions(c, &a, 1) > 0) {
                MOQ_TEST_CHECK(a.kind != MOQ_ACTION_RESET_DATA && !(a.kind ==
                               MOQ_ACTION_SEND_DATA && a.u.send_data.fin));
                moq_action_cleanup(&a);
            }
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, now + 4), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(o.finished, (int)cycle + 1);
        pump_data(c, sv, now + 4, &data, &fins, &resets);
        MOQ_TEST_CHECK_EQ_INT(fins, 0);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
        if (partial) {
            /* The application completes its object; it has no destination
             * now, and nothing more reaches the reset stream. */
            MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_data(pub, t, payload, now + 5), MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_pub_end_object(pub, t, now + 5), MOQ_OK);
            moq_action_t a;
            while (moq_session_poll_actions(c, &a, 1) > 0) {
                MOQ_TEST_CHECK(!(a.kind == MOQ_ACTION_SEND_DATA &&
                                 a.u.send_data.stream_ref._v == data));
                deliver(sv, &a, false, now + 5);
                moq_action_cleanup(&a);
            }
        }
        run(c, sv, now + 5);
        drain(sv);
        drain(c);
    }
    MOQ_TEST_CHECK_EQ_INT(o.finished, 3);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), MOQ_SESS_ESTABLISHED);
    moq_rcbuf_decref(payload);
    moq_pub_destroy(pub);
    destroy(c, sv);
    return failures;
}

static void on_joined(void *ctx, moq_pub_track_t *t)
{ (void)t; ((pub_obs_t *)ctx)->ok++; }
static void on_left(void *ctx, moq_pub_track_t *t)
{ (void)t; ((pub_obs_t *)ctx)->finished++; }

/* A subscriber's GOAWAY on an accepted SUBSCRIBE (draft-22 §9.2, draft-18
 * §10.4; empty New Session URI from a client) retires that subscription at
 * the publisher facade: one departure callback, the other subscription to
 * the track untouched, an open subgroup of the departed one reset (never
 * finished), and -- on draft-18, which allows one subscription per track --
 * the track accepts the subscriber again without a write in between. */
static int test_publisher_subscribe_goaway(moq_version_t v)
{
    int failures = 0;
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, v));
    pub_obs_t o; memset(&o, 0, sizeof(o));
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    moq_pub_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &o;
    cfg.callbacks.on_subscriber_joined = on_joined;
    cfg.callbacks.on_subscriber_left = on_left;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_create(sv, moq_alloc_default(), &cfg, &pub), MOQ_OK);
    if (!pub) { destroy(c, sv); return failures; }
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){ g_parts, 1 };
    tc.track_name = MOQ_BYTES_LITERAL("v");
    moq_pub_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_add_track(pub, &tc, 1, &t), MOQ_OK);
    bool d22 = v == MOQ_VERSION_DRAFT_22;
    int subs = d22 ? 2 : 1;
    moq_subscription_t ch[2];
    for (int i = 0; i < subs; i++) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ g_parts, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("v");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &sc, 2, &ch[i]), MOQ_OK);
        run(c, sv, 3);
        (void)moq_pub_tick(pub, 3);
        run(c, sv, 3);
    }
    drain(c);
    MOQ_TEST_CHECK_EQ_INT(o.ok, subs);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, t), (size_t)subs);
    /* An open subgroup on every destination. */
    uint8_t bytes[4] = { 1, 2, 3, 4 };
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                                                &payload), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_object(pub, t, 0, 0, payload, 4), MOQ_OK);
    int fins = 0, resets = 0, data_streams = 0;
    uint64_t seen[2] = { 0, 0 };
    moq_action_t a;
    while (moq_session_poll_actions(sv, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.stream_ref._v != seen[0] &&
            a.u.send_data.stream_ref._v != seen[1] && data_streams < 2)
            seen[data_streams++] = a.u.send_data.stream_ref._v;
        deliver(c, &a, true, 4);
        moq_action_cleanup(&a);
    }
    drain(c);
    MOQ_TEST_CHECK_EQ_INT(data_streams, subs);
    /* The first subscriber migrates its subscription. */
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_request_goaway_subscribe(c, ch[0], &gc, 5), MOQ_OK);
    pump(c, sv, false, 5, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, 6), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o.finished, 1);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, t), (size_t)(subs - 1));
    while (moq_session_poll_actions(sv, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_RESET_DATA) resets++;
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin) fins++;
        deliver(c, &a, true, 6);
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(resets, 1);
    MOQ_TEST_CHECK_EQ_INT(fins, 0);
    run(c, sv, 7);
    drain(c);
    /* The remaining subscriber still receives. */
    if (d22) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_object(pub, t, 0, 1, payload, 8), MOQ_OK);
        run(c, sv, 8);
        moq_event_t ev;
        bool got = take(c, MOQ_EVENT_OBJECT_RECEIVED, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            MOQ_TEST_CHECK(ev.u.object_received.sub._opaque == ch[1]._opaque);
            moq_event_cleanup(&ev);
        }
    }
    /* Subscribing again is accepted (draft-18: the old subscription is gone). */
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("v");
    moq_subscription_t again;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &sc, 9, &again), MOQ_OK);
    run(c, sv, 9);
    (void)moq_pub_tick(pub, 9);
    run(c, sv, 9);
    moq_event_t ev;
    bool ok = take(c, MOQ_EVENT_SUBSCRIBE_OK, &ev);
    MOQ_TEST_CHECK(ok);
    if (ok) moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(o.ok, subs + 1);
    MOQ_TEST_CHECK_EQ_INT(o.finished, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    moq_rcbuf_decref(payload);
    moq_pub_destroy(pub);
    destroy(c, sv);
    return failures;
}

/* The session-wide GOAWAY reaches the publisher facade with its URI; without
 * on_goaway it still reports draining. */
static int test_publisher_goaway(void)
{
    int failures = 0;
    for (int with = 0; with < 2; with++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_22));
        pub_obs_t o; memset(&o, 0, sizeof(o));
        moq_pub_track_t *t = NULL;
        moq_publisher_t *pub = make_pub(c, &o, with != 0, &t);
        static const uint8_t uri[] = "moqt://next.example/";
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(sv, uri, sizeof(uri) - 1, 2), MOQ_OK);
        run(c, sv, 3);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_tick(pub, 3), MOQ_OK);
        MOQ_TEST_CHECK(moq_pub_is_draining(pub));
        if (with) {
            MOQ_TEST_CHECK_EQ_INT(o.goaways, 1);
            MOQ_TEST_CHECK_EQ_INT(o.draining, 0);
            MOQ_TEST_CHECK(o.uri_len == sizeof(uri) - 1 && memcmp(o.uri, uri, o.uri_len) == 0);
        } else {
            MOQ_TEST_CHECK_EQ_INT(o.goaways, 0);
            MOQ_TEST_CHECK_EQ_INT(o.draining, 1);
        }
        moq_pub_destroy(pub);
        destroy(c, sv);
    }
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_subscriber_options();
    failures += test_subscriber_migration();
    failures += test_subscriber_status_migration(MOQ_VERSION_DRAFT_18);
    failures += test_subscriber_status_migration(MOQ_VERSION_DRAFT_22);
    failures += test_publisher_migration();
    failures += test_publisher_goaway();
    failures += test_publisher_migration_streams(false);
    failures += test_publisher_migration_streams(true);
    failures += test_publisher_subscribe_goaway(MOQ_VERSION_DRAFT_18);
    failures += test_publisher_subscribe_goaway(MOQ_VERSION_DRAFT_22);
    if (failures) { fprintf(stderr, "FAIL: test_d22_facade_surfaces (%d)\n", failures); return 1; }
    printf("PASS: test_d22_facade_surfaces\n");
    return 0;
}
