/* Joining FETCH on a multi-shard relay, draft-16 and draft-18: the joined
 * subscription is on a shard that only mirrors the track, so the join is
 * answered by the shard that owns it -- from that shard's cache, and for a
 * run the cache does not hold, from the origin -- over the range the
 * requesting session resolved from the subscription's saved Largest
 * (draft-16 9.16.2.1, 9.16.3). The rig is test_relay_shard_fill's. */
#define main relay_shard_fill_main
#include "test_relay_shard_fill.c"
#undef main
#include "standalone_fetch_observation.h"
#include <moq/relay/moqr_shards_test.h>

/* The origin: announces live/cam, accepts the owner's SUBSCRIBE without a
 * Largest, and records the upstream FETCH the owner sends for a run its
 * cache does not hold. */
typedef struct jso {
    sfconn_t *cn;
    moq_announcement_t ann;
    moq_subscription_t sub;
    moq_fetch_t fetch;
    uint64_t sg, so, eg, eo;
    unsigned namespaces, subscribes, requests, cancelled, closed;
} jso_t;

static void
jso_drain(sfrig_t *r, jso_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(o->cn->peer, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            o->namespaces++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            o->sub = ev.u.subscribe_request.sub;
            o->subscribes++;
            moq_accept_subscribe_cfg_t cfg;
            moq_accept_subscribe_cfg_init(&cfg);
            SF_CHECK(r, moq_session_accept_subscribe(o->cn->peer, o->sub, &cfg,
                                                     r->now) == MOQ_OK);
            break;
        }
        case MOQ_EVENT_FETCH_REQUEST: {
            const moq_fetch_request_event_t *q = &ev.u.fetch_request;
            SF_CHECK(r, rfs_key(q->track_namespace, q->track_name) &&
                        q->joining_sub._opaque == 0 && q->joining_pub._opaque == 0);
            o->requests++;
            o->fetch = q->fetch;
            o->sg = q->start_group;
            o->so = q->start_object;
            o->eg = q->end_group;
            o->eo = q->end_object;
            break;
        }
        case MOQ_EVENT_FETCH_CANCELLED:
            o->cancelled++;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            o->closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* A subscriber: its SUBSCRIBE_OK, live objects, and its FETCH's events. */
typedef struct jss {
    bool subscribe_ok, has_largest;
    uint64_t largest_group, largest_object;
    unsigned live, closed;
    rfs_observation_t obs;
} jss_t;

static void
jss_drain(sfrig_t *r, sfconn_t *cn, jss_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            s->subscribe_ok = true;
            s->has_largest = ev.u.subscribe_ok.has_largest;
            s->largest_group = ev.u.subscribe_ok.largest_group;
            s->largest_object = ev.u.subscribe_ok.largest_object;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            s->live++;
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            /* the live subgroup's end */
        } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            s->closed++;
        } else if (!rfs_observe(&s->obs, &ev, r->now) &&
                   ev.kind != MOQ_EVENT_SETUP_COMPLETE &&
                   ev.kind != MOQ_EVENT_REQUEST_READY &&
                   ev.kind != MOQ_EVENT_SUBSCRIBE_DONE) {
            printf("FIXTURE: join_shard unexpected event=%d\n", (int)ev.kind);
            r->failures++;
        }
        moq_event_cleanup(&ev);
    }
}

static void
js_step(sfrig_t *r, jso_t *o, sfconn_t *prime, jss_t *ps, sfconn_t *cons, jss_t *cs)
{
    sfrig_cycle(r, 1000);
    jso_drain(r, o);
    jss_drain(r, prime, ps);
    jss_drain(r, cons, cs);
}

static moq_subscription_t
js_subscribe(sfrig_t *r, sfconn_t *cn)
{
    moq_subscribe_cfg_t cfg;
    sf_subscribe_cfg(&cfg);
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub = {0};
    SF_CHECK(r, moq_session_subscribe(cn->peer, &cfg, r->now, &sub) == MOQ_OK);
    return sub;
}

/* The objects the join served are exactly (0, ids[k]) with the origin's
 * bytes: { group, id, 0xC3, 0x3C } live, { 0, id, 0xA1, 0x5E } from the
 * upstream FETCH. */
static bool
js_exact(const rfs_observation_t *obs, moq_fetch_t fetch, const uint64_t *ids,
         unsigned n, uint64_t fetched_id)
{
    if (rfs_count(obs, fetch, MOQ_EVENT_FETCH_OBJECT) != n) return false;
    for (unsigned i = 0; i < n; i++) {
        const rfs_event_t *v = rfs_nth(obs, fetch, MOQ_EVENT_FETCH_OBJECT, i);
        bool fetched = ids[i] == fetched_id;
        uint8_t body[4] = { 0, (uint8_t)ids[i], fetched ? 0xA1 : 0xC3,
                            fetched ? 0x5E : 0x3C };
        if (v == NULL || v->group != 0 || v->object != ids[i] ||
            v->bytes != sizeof(body) || memcmp(v->body, body, sizeof(body)) != 0) {
            return false;
        }
    }
    return true;
}

static int
js_case(moq_version_t version, const char *row)
{
    bool hole = strcmp(row, "hole") == 0 || strcmp(row, "cancel") == 0;
    bool cancel = strcmp(row, "cancel") == 0;
    bool later = strcmp(row, "saved-largest") == 0;
    g_sf_version = version;
    ca_t a;
    ca_init(&a);
    sfrig_t r = {0};
    jso_t origin = {0};
    jss_t ps = {0}, cs = {0};
    sfconn_t *prime = NULL, *cons = NULL;
    const char *missing = NULL;
    moq_fetch_t fetch = {0};
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
#define JS_FIX(e) do { if (!(e)) { \
    printf("FIXTURE: %s %s:%d: %s\n", row, lbl, __LINE__, #e); r.failures++; goto cleanup; \
} } while (0)
#define JS_NEED(e, label) do { if (!(e)) { missing = label; goto cleanup; } } while (0)
    JS_FIX(sfrig_create_ex(&r, &a, true, 1, 2) == MOQR_OK);
    origin.cn = sfrig_connect(&r, 0);
    prime = sfrig_connect(&r, 0);
    cons = sfrig_connect(&r, 1);
    JS_FIX(origin.cn != NULL && prime != NULL && cons != NULL);
    sfrig_pump(&r, 4);
    {
        moq_bytes_t parts[] = { B("live"), B("cam") };
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
        JS_FIX(moq_session_publish_namespace(origin.cn->peer, &cfg, r.now,
                                             &origin.ann) == MOQ_OK);
    }
    for (unsigned i = 0; i < 12; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
    JS_FIX(origin.namespaces == 1);
    /* The owner's cache: the prime's subscription brings group 0 in. */
    (void)js_subscribe(&r, prime);
    for (unsigned i = 0; i < 16 && origin.subscribes == 0; i++)
        js_step(&r, &origin, prime, &ps, cons, &cs);
    JS_FIX(origin.subscribes == 1);
    for (unsigned i = 0; i < 8; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
    static const uint64_t islands[] = { 0, 2 }, all[] = { 0, 1, 2 };
    sf_pub_ids(&r, origin.cn, origin.sub, 0, 0, hole ? islands : all, hole ? 2 : 3);
    for (unsigned i = 0; i < 16; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
    JS_FIX(ps.subscribe_ok && ps.live == (hole ? 2u : 3u));
    /* The consumer, on the shard that only mirrors the track. */
    moq_subscription_t csub = js_subscribe(&r, cons);
    for (unsigned i = 0; i < 24 && !cs.subscribe_ok; i++)
        js_step(&r, &origin, prime, &ps, cons, &cs);
    JS_FIX(cs.subscribe_ok && cs.has_largest && cs.largest_group == 0 &&
           cs.largest_object == 2);
    if (later) {
        static const uint64_t next[] = { 0 };
        sf_pub_ids(&r, origin.cn, origin.sub, 1, 0, next, 1);
        for (unsigned i = 0; i < 16; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
    }
    {
        moq_fetch_cfg_t fc;
        memset(&fc, 0, sizeof(fc));
        moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = csub;
        fc.joining_start = 0;
        fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
        JS_FIX(moq_session_fetch(cons->peer, &fc, r.now, &fetch) == MOQ_OK);
    }
    for (unsigned i = 0; i < 24; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
    if (hole) {
        JS_NEED(origin.requests == 1 && origin.sg == 0 && origin.so == 1 &&
                origin.eg == 0 && origin.eo == 2, "owner-asks-origin-for-the-run");
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 0;
        ac.end_object = 2;
        JS_FIX(moq_session_accept_fetch(origin.cn->peer, origin.fetch, &ac, r.now) == MOQ_OK);
        for (unsigned i = 0; i < 4; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
        if (cancel) {
            origin.cn->transport_paused = true;
            JS_FIX(moq_session_fetch_cancel(cons->peer, fetch, r.now) == MOQ_OK);
            for (unsigned i = 0; i < 8; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
            origin.cn->transport_paused = false;
            for (unsigned i = 0; i < 16; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
            JS_NEED(origin.cancelled == 1 &&
                    rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0,
                    "cancel-reaches-origin");
        } else {
            uint8_t body[4] = { 0, 1, 0xA1, 0x5E };
            moq_rcbuf_t *pl = NULL;
            JS_FIX(moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
            moq_fetch_object_cfg_t oc;
            moq_fetch_object_cfg_init(&oc);
            oc.group_id = 0;
            oc.subgroup_id = 1;   /* object 1 was never on subgroup 0's stream */
            oc.object_id = 1;
            oc.payload = pl;
            moq_result_t wrc = moq_session_write_fetch_object(origin.cn->peer,
                                                              origin.fetch, &oc, r.now);
            moq_rcbuf_decref(pl);
            JS_FIX(wrc == MOQ_OK);
            JS_FIX(moq_session_end_fetch(origin.cn->peer, origin.fetch, r.now) == MOQ_OK);
            for (unsigned i = 0; i < 24; i++) js_step(&r, &origin, prime, &ps, cons, &cs);
        }
    } else {
        JS_NEED(origin.requests == 0, "owner-cache-only");
    }
    JS_NEED(cs.closed == 0 && cs.obs.closed == 0 && origin.closed == 0 &&
            moq_session_state(cons->peer) == MOQ_SESS_ESTABLISHED, "request-local-lifetime");
    if (!cancel) {
        const rfs_event_t *ok = rfs_nth(&cs.obs, fetch, MOQ_EVENT_FETCH_OK, 0);
        JS_NEED(ok != NULL && ok->group == 0 && ok->object == 3, "ok-at-saved-largest");
        JS_NEED(rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_ERROR) == 0 &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_RESET) == 0 &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_GAP) == 0, "complete-no-gap");
        JS_NEED(js_exact(&cs.obs, fetch, all, 3, hole ? 1 : UINT64_MAX), "exact-bytes");
    }
    sf_expect_channels_idle(&r);
cleanup:
    printf("OBS: join_shard %s %s origin_requests=%u cancels=%u events=%u closed=%u\n",
           row, lbl, origin.requests, origin.cancelled, cs.obs.n, cs.closed + cs.obs.closed);
    if (r.s != NULL) sfrig_destroy(&r);
    int errors = r.failures + cs.obs.invalid + (a.live != 0);
    printf("%s: join_shard(%s %s) causal=%s fixture_failures=%d\n",
           errors ? "FIXTURE" : missing != NULL ? "FAIL" : "PASS", row, lbl,
           missing != NULL ? missing : "none", errors);
#undef JS_FIX
#undef JS_NEED
    return errors ? 2 : missing != NULL ? 1 : 0;
}

/* -- A PUBLISH origin replaced while a remote join is queued ------------------
 * The owner shard holds the track a publisher pushes with PUBLISH; the
 * consumer's subscription on the other shard reads it through a demand. The
 * consumer's shard admits the join and queues it for the owner. Before the
 * owner takes it, the publisher ends the track and a new publisher replaces
 * it, writing other bytes at the same Locations. The join is then refused
 * (INVALID_JOINING_REQUEST_ID), request-locally, never served the
 * replacement. Without the replacement, the same schedule serves the
 * original Objects. The runtime is stepped by hand with the round barrier:
 * nothing a shard pushes is visible before the round advances, so the owner
 * runs the whole replacement while the queued join stays invisible to it. */

static void
jr_drain_quiet(sfrig_t *r, sfconn_t *cn)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) r->failures++;
        moq_event_cleanup(&ev);
    }
}

/* Transport for every connection, then the shard `only` alone, the round
 * held. */
static void
jr_hold_cycle(sfrig_t *r, uint16_t only)
{
    r->now += 100;
    for (int i = 0; i < SF_MAX_CONNS; i++) {
        sfconn_t *cn = &r->conns[i];
        if (!cn->used) continue;
        size_t steps = 0;
        SF_CHECK(r, moq_simpair_advance_to(cn->sp, r->now) == MOQ_OK);
        SF_CHECK(r, moq_simpair_run_until_quiescent(cn->sp, SF_STEP_BOUND, &steps) ==
                        MOQ_OK);
    }
    uint64_t woke = 0;
    SF_CHECK(r, moqr_shards_debug_step_shard(r->s, only, r->now, &woke) == MOQR_OK);
}

/* Group 0, objects 0..2, on subgroup 0: { 0, id, fill, ~fill }. */
static void
jr_publish(sfrig_t *r, sfconn_t *pub, moq_publication_t ph, uint8_t fill,
           uint16_t hold)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    moq_subgroup_handle_t sgh;
    moq_result_t orc = MOQ_ERR_WRONG_STATE;
    for (int i = 0; i < 16 && orc != MOQ_OK; i++) {
        orc = moq_session_open_pub_subgroup(pub->peer, ph, &sgc, r->now, &sgh);
        if (orc != MOQ_OK) {
            if (hold != UINT16_MAX) jr_hold_cycle(r, hold);
            else sfrig_cycle(r, 1000);
        }
    }
    SF_CHECK(r, orc == MOQ_OK);
    if (orc != MOQ_OK) return;
    for (uint64_t id = 0; id < 3; id++) {
        uint8_t body[4] = { 0, (uint8_t)id, fill, (uint8_t)~fill };
        moq_rcbuf_t *pl = NULL;
        SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) == MOQ_OK);
        SF_CHECK(r, moq_session_write_object(pub->peer, sgh, id, pl, r->now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    SF_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
}

static moq_publication_t
jr_publish_track(sfrig_t *r, sfconn_t *pub)
{
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = g_nsp, .count = 2 };
    pc.track_name = B("v");
    moq_publication_t ph = {0};
    SF_CHECK(r, moq_session_publish(pub->peer, &pc, r->now, &ph) == MOQ_OK);
    return ph;
}

static int
jr_case(moq_version_t version, bool replace)
{
    const char *row = replace ? "replaced-while-queued" : "publish-queued";
    g_sf_version = version;
    ca_t a;
    ca_init(&a);
    sfrig_t r = {0};
    jss_t cs = {0};
    sfconn_t *pub = NULL, *pub2 = NULL, *cons = NULL;
    const char *missing = NULL;
    moq_fetch_t fetch = {0};
    uint32_t queued = 0;
    bool replaced = false;
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
#define JR_FIX(e) do { if (!(e)) { \
    printf("FIXTURE: %s %s:%d: %s\n", row, lbl, __LINE__, #e); r.failures++; goto cleanup; \
} } while (0)
#define JR_NEED(e, label) do { if (!(e)) { missing = label; goto cleanup; } } while (0)
    JR_FIX(sfrig_create_ex(&r, &a, false, 0, 2) == MOQR_OK);
    g_nsp[0] = B("live");
    g_nsp[1] = B("cam");
    pub = sfrig_connect(&r, 0);
    cons = sfrig_connect(&r, 1);
    JR_FIX(pub != NULL && cons != NULL);
    sfrig_pump(&r, 4);
    {
        moq_publish_namespace_cfg_t nc;
        moq_publish_namespace_cfg_init(&nc);
        nc.track_namespace = (moq_namespace_t){ .parts = g_nsp, .count = 2 };
        moq_announcement_t ann;
        JR_FIX(moq_session_publish_namespace(pub->peer, &nc, r.now, &ann) == MOQ_OK);
    }
    for (unsigned i = 0; i < 12; i++) {
        sfrig_cycle(&r, 1000);
        jr_drain_quiet(&r, pub);
    }
    moq_publication_t ph = jr_publish_track(&r, pub);
    for (unsigned i = 0; i < 12; i++) {
        sfrig_cycle(&r, 1000);
        jr_drain_quiet(&r, pub);
    }
    jr_publish(&r, pub, ph, 0xC3, UINT16_MAX);
    for (unsigned i = 0; i < 12; i++) {
        sfrig_cycle(&r, 1000);
        jr_drain_quiet(&r, pub);
    }
    moq_subscription_t csub = js_subscribe(&r, cons);
    for (unsigned i = 0; i < 24 && !cs.subscribe_ok; i++) {
        sfrig_cycle(&r, 1000);
        jr_drain_quiet(&r, pub);
        jss_drain(&r, cons, &cs);
    }
    JR_FIX(cs.subscribe_ok && cs.has_largest && cs.largest_group == 0 &&
           cs.largest_object == 2);
    for (unsigned i = 0; i < 8; i++) {
        sfrig_cycle(&r, 1000);
        jr_drain_quiet(&r, pub);
        jss_drain(&r, cons, &cs);
    }
    JR_FIX(moqr_shards_debug_demand_channel_pending(r.s, 1, 0) == 0);
    {
        moq_fetch_cfg_t fc;
        memset(&fc, 0, sizeof(fc));
        moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = csub;
        fc.joining_start = 0;
        fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
        JR_FIX(moq_session_fetch(cons->peer, &fc, r.now, &fetch) == MOQ_OK);
    }
    /* The consumer's shard admits the join and queues it for the owner. */
    for (unsigned i = 0; i < 8 && queued == 0; i++) {
        jr_hold_cycle(&r, 1);
        queued = moqr_shards_debug_demand_channel_pending(r.s, 1, 0);
    }
    JR_NEED(queued == 1, "join-queued-for-owner");
    if (replace) {
        /* The owner alone, the round held: the track ends, and a new
         * publisher replaces it with other bytes at the same Locations. */
        moq_finish_publish_cfg_t fin;
        moq_finish_publish_cfg_init(&fin);
        fin.status_code = 0x2u;   /* TRACK_ENDED */
        JR_FIX(moq_session_finish_publish(pub->peer, ph, &fin, r.now) == MOQ_OK);
        for (unsigned i = 0; i < 4; i++) {
            jr_hold_cycle(&r, 0);
            jr_drain_quiet(&r, pub);
        }
        pub2 = sfrig_connect(&r, 0);
        JR_FIX(pub2 != NULL);
        for (unsigned i = 0; i < 6; i++) {
            jr_hold_cycle(&r, 0);
            jr_drain_quiet(&r, pub);
        }
        moq_publication_t ph2 = jr_publish_track(&r, pub2);
        bool ok = false;
        for (unsigned i = 0; i < 12 && !ok; i++) {
            jr_hold_cycle(&r, 0);
            jr_drain_quiet(&r, pub);
            moq_event_t ev;
            while (moq_session_poll_events(pub2->peer, &ev, 1) == 1) {
                ok = ok || ev.kind == MOQ_EVENT_PUBLISH_OK;
                moq_event_cleanup(&ev);
            }
        }
        JR_NEED(ok, "replacement-accepted-while-join-queued");
        jr_publish(&r, pub2, ph2, 0x5A, 0);
        for (unsigned i = 0; i < 8; i++) {
            jr_hold_cycle(&r, 0);
            jr_drain_quiet(&r, pub);
            jr_drain_quiet(&r, pub2);
        }
        replaced = true;
    }
    JR_NEED(moqr_shards_debug_demand_channel_pending(r.s, 1, 0) == 1,
            "join-still-queued");
    /* Release: the round advances and every shard runs. */
    moqr_shards_debug_round_advance(r.s);
    for (unsigned i = 0; i < 32; i++) {
        sfrig_cycle(&r, 1000);
        jr_drain_quiet(&r, pub);
        if (pub2 != NULL) jr_drain_quiet(&r, pub2);
        jss_drain(&r, cons, &cs);
    }
    for (unsigned k = 0; k < cs.obs.n; k++) {
        const rfs_event_t *v = &cs.obs.events[k];
        JR_NEED(v->kind != MOQ_EVENT_FETCH_OBJECT || v->bytes != 4 || v->body[2] != 0x5A,
                "never-the-replacement");
    }
    JR_NEED(cs.closed == 0 && cs.obs.closed == 0 &&
            moq_session_state(cons->peer) == MOQ_SESS_ESTABLISHED, "request-local-lifetime");
    if (replaced) {
        const rfs_event_t *err = rfs_nth(&cs.obs, fetch, MOQ_EVENT_FETCH_ERROR, 0);
        JR_NEED(err != NULL && err->code == MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_OK) == 0 &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_OBJECT) == 0,
                "refused-invalid-joining");
    } else {
        static const uint64_t all[] = { 0, 1, 2 };
        const rfs_event_t *ok = rfs_nth(&cs.obs, fetch, MOQ_EVENT_FETCH_OK, 0);
        JR_NEED(ok != NULL && ok->group == 0 && ok->object == 3, "ok-at-saved-largest");
        JR_NEED(rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_ERROR) == 0 &&
                rfs_count(&cs.obs, fetch, MOQ_EVENT_FETCH_RESET) == 0 &&
                js_exact(&cs.obs, fetch, all, 3, UINT64_MAX), "original-exact");
    }
    sf_expect_channels_idle(&r);
cleanup:
    printf("OBS: join_shard %s %s queued=%u replaced=%d events=%u\n", row, lbl, queued,
           (int)replaced, cs.obs.n);
    if (r.s != NULL) sfrig_destroy(&r);
    int errors = r.failures + cs.obs.invalid + (a.live != 0);
    printf("%s: join_shard(%s %s) causal=%s fixture_failures=%d\n",
           errors ? "FIXTURE" : missing != NULL ? "FAIL" : "PASS", row, lbl,
           missing != NULL ? missing : "none", errors);
#undef JR_FIX
#undef JR_NEED
    return errors ? 2 : missing != NULL ? 1 : 0;
}

int
main(int argc, char **argv)
{
    static const char *rows[] = { "cached", "hole", "saved-largest", "cancel" };
    int worst = 0;
    for (int replace = 0; replace < 2; replace++) {
        const char *jrow = replace ? "replaced-while-queued" : "publish-queued";
        if (argc > 1 && strcmp(argv[1], jrow) != 0) continue;
        int f = jr_case(MOQ_VERSION_DRAFT_16, replace != 0);
        if (f > worst) worst = f;
        f = jr_case(MOQ_VERSION_DRAFT_18, replace != 0);
        if (f > worst) worst = f;
    }
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        if (argc > 1 && strcmp(argv[1], rows[i]) != 0) continue;
        int f = js_case(MOQ_VERSION_DRAFT_16, rows[i]);
        if (f > worst) worst = f;
        f = js_case(MOQ_VERSION_DRAFT_18, rows[i]);
        if (f > worst) worst = f;
    }
    return worst;
}
