#include "test_cases.h"

/* Look up the server-session history record for (ns,name). */
static moq_track_hist_t *pub_find_hist(moq_session_t *s, const char *ns0,
                                       const char *name)
{
    moq_bytes_t np = { (const uint8_t *)ns0, strlen(ns0) };
    moq_namespace_t ns = { &np, 1 };
    moq_bytes_t nm = { (const uint8_t *)name, strlen(name) };
    size_t kl = 0;
    uint8_t *k = moq_build_track_key(s, &ns, nm, &kl);
    if (!k && kl > 0) return NULL;
    moq_track_hist_t *r = track_hist_find(s, k, kl);
    if (k) s->alloc.free(k, kl, s->alloc.ctx);
    return r;
}

/* Installing a retained group merges its last object into the track's largest
 * history, and that history is MONOTONIC -- it is a high-water mark of what the
 * endpoint published, not a statement about what is still cached.
 *
 * draft-18 5.1.2: "Largest Object updates when the first byte of an Object with
 * a Location larger than the previous value is published or received". 10.2.11:
 * the parameter "contains the largest Location in the Track observed by the
 * sending endpoint", and if omitted "the sending endpoint has not published or
 * received any Objects in the Track". Neither is conditioned on the Object
 * still being retrievable, so clearing or replacing the retained cache must NOT
 * retract it.
 *
 * This pins the two sequences a general caller can drive through the public
 * API, which the catalog path itself never reaches (catalog generations only
 * ever advance). */
void test_retained_install_merges_monotonic_history(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track)
        == MOQ_OK);

    /* Nothing published yet: no Largest to report (10.2.11's omitted case). */
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);

    uint8_t d[] = { 9, 8, 7 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);

    /* Install retained GROUP 5 with objects 0..2: the last object is the
     * track's latest published Location. */
    {
        moq_pub_retained_object_t objs[3];
        for (int i = 0; i < 3; i++) {
            memset(&objs[i], 0, sizeof(objs[i]));
            objs[i].object_id = (uint64_t)i;
            objs[i].payload = pay;
        }
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 5;
        rg.objects = objs;
        rg.object_count = 3;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &rg) == MOQ_OK);
    }
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest);
    MOQ_TEST_CHECK_EQ_U64(r->largest_group, 5);
    MOQ_TEST_CHECK_EQ_U64(r->largest_object, 2);

    /* CLEAR: the cache is gone, the high-water mark is not. A PUBLISH composed
     * now still reports (5,2), which is what 5.1.2 defines it to be. */
    MOQ_TEST_CHECK(moq_pub_clear_retained_group(pub, track) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest);
    MOQ_TEST_CHECK_EQ_U64(r->largest_group, 5);
    MOQ_TEST_CHECK_EQ_U64(r->largest_object, 2);

    /* REPLACE with a LOWER group: the retained cache becomes group 2, and the
     * history still reports the high-water mark. track_hist_merge is a
     * monotonic max, so the lower install is a no-op on it. */
    {
        moq_pub_retained_object_t objs[1];
        memset(&objs[0], 0, sizeof(objs[0]));
        objs[0].object_id = 0;
        objs[0].payload = pay;
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 2;
        rg.objects = objs;
        rg.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &rg) == MOQ_OK);
    }
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest);
    MOQ_TEST_CHECK_EQ_U64(r->largest_group, 5);
    MOQ_TEST_CHECK_EQ_U64(r->largest_object, 2);

    /* REPLACE with a HIGHER group: the mark advances. */
    {
        moq_pub_retained_object_t objs[2];
        for (int i = 0; i < 2; i++) {
            memset(&objs[i], 0, sizeof(objs[i]));
            objs[i].object_id = (uint64_t)i;
            objs[i].payload = pay;
        }
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 9;
        rg.objects = objs;
        rg.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &rg) == MOQ_OK);
    }
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest);
    MOQ_TEST_CHECK_EQ_U64(r->largest_group, 9);
    MOQ_TEST_CHECK_EQ_U64(r->largest_object, 1);

    moq_rcbuf_decref(pay);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("retained_install_merges_monotonic_history");
}

/* Layer B: the publisher reserves a per-track history record at add_track and
 * advances it on every accepted object -- including zero-destination writes --
 * with an exactly-once, allocation-free merge; empty reservations are
 * reclaimed on remove, observed records stay pinned. */
void test_pub_history_reservation_and_merge(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track)
        == MOQ_OK);

    /* add_track reserved an EMPTY record (no object observed yet). */
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL);
    MOQ_TEST_CHECK(r && r->in_use && r->refs >= 1 && !r->has_largest);

    /* A zero-destination write (no subscriber) still admits the object and
     * advances the track's largest. */
    uint8_t d[] = { 1, 2, 3 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 4, 2, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 4 && r->largest_object == 2);

    /* A greater location advances; a lesser one is a no-op. */
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 4, 5, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 3, 9, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->largest_group == 4 && r->largest_object == 5);
    moq_rcbuf_decref(pay);

    /* A second, never-written track: its empty reservation is reclaimed on
     * remove, while the published track's record stays pinned. */
    moq_pub_track_cfg_t tcfg2;
    moq_pub_track_cfg_init(&tcfg2);
    tcfg2.track_namespace.parts = ns_parts;
    tcfg2.track_namespace.count = 1;
    tcfg2.track_name = MOQ_BYTES_LITERAL("audio");
    moq_pub_track_t *track2 = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg2, moq_simpair_now_us(sp),
        &track2) == MOQ_OK);
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "audio") != NULL);
    moq_pub_remove_track(pub, track2, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "audio") == NULL); /* reclaimed */

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    /* Observed record survives remove_track (pinned until session end). */
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && r->has_largest &&
                   r->largest_group == 4 && r->largest_object == 5);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_reservation_and_merge");
}

/* Layer B: a full registry makes add_track fail NOMEM (the reservation is the
 * capacity-failure point), leaving no track behind. */
void test_pub_history_capacity_at_add_track(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    /* Fill the registry to exhaustion via the public note API (observed
     * records pin their slots). Bounded loop; assert we actually hit full. */
    bool hit_full = false;
    for (int i = 0; i < 100000 && !hit_full; i++) {
        char nm[16];
        int n = 0; unsigned v = (unsigned)i;
        nm[n++] = 'k';
        do { nm[n++] = (char)('0' + (v % 10)); v /= 10; } while (v && n < 15);
        nm[n] = 0;
        moq_bytes_t np = MOQ_BYTES_LITERAL("hist");
        moq_namespace_t ns = { &np, 1 };
        moq_bytes_t nb = { (const uint8_t *)nm, (size_t)n };
        moq_result_t rc = moq_session_note_object_published(sv, &ns, nb, 1, 1);
        if (rc == MOQ_ERR_NOMEM) hit_full = true;
        else MOQ_TEST_CHECK(rc == MOQ_OK);
    }
    MOQ_TEST_CHECK(hit_full);

    /* With the registry full, adding a brand-new track cannot reserve a
     * record: add_track fails NOMEM and creates no track. */
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = (moq_pub_track_t *)0x1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track)
        == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(track == NULL);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_capacity_at_add_track");
}

/* Layer B: deterministic validation runs BEFORE any mutation. An unencodable
 * location is rejected without advancing Largest or leaving an op pending,
 * even with zero destinations; the next valid write then succeeds. Also
 * exercises begin_object history admission. */
void test_pub_history_validate_before_mutate(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    uint8_t d[] = { 9 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);

    /* Unencodable group id: rejected before mutation (zero destinations, so
     * the session write path would never see it). */
    moq_result_t rc = moq_pub_write_object(pub, track,
        MOQ_QUIC_VARINT_MAX + 1, 0, pay, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    /* Largest not poisoned: the record is still an empty reservation. */
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);

    /* Unencodable object id: same. */
    rc = moq_pub_write_object(pub, track, 0, MOQ_QUIC_VARINT_MAX + 1, pay,
        moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);

    /* Unencodable payload LENGTH: a zero-copy wrap can carry a logical length
     * larger than any real buffer. The location is fine, so only the length
     * check can reject it -- and it must, before Largest is touched. Guarded
     * for platforms whose size_t cannot represent 2^62 (the length would
     * truncate); the production check is unconditional. */
#if SIZE_MAX > 0x3FFFFFFFFFFFFFFFULL
    {
        static const uint8_t one = 0x7;
        moq_rcbuf_t *big = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_wrap(&alloc, &one,
            (size_t)MOQ_QUIC_VARINT_MAX + 1, NULL, NULL, &big) == MOQ_OK);
        rc = moq_pub_write_object(pub, track, 1, 1, big,
            moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
        moq_rcbuf_decref(big);
        r = pub_find_hist(sv, "live", "video");
        MOQ_TEST_CHECK(r != NULL && !r->has_largest);   /* not poisoned */
    }
#endif

    /* No op was left pending by the rejected writes: a valid write at a fresh
     * location is a NEW op (not a divergent retry) and succeeds, advancing
     * Largest. */
    rc = moq_pub_write_object(pub, track, 7, 3, pay, moq_simpair_now_us(sp));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 7 && r->largest_object == 3);
    moq_rcbuf_decref(pay);

    /* begin_object admits the streaming object's location into history. */
    moq_pub_begin_object_cfg_t bo;
    moq_pub_begin_object_cfg_init_sized(&bo, sizeof(bo));
    bo.group_id = 8;
    bo.object_id = 0;
    bo.payload_length = 4;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bo,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->largest_group == 8 && r->largest_object == 0);

    moq_pub_remove_track(pub, track, moq_simpair_now_us(sp));
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_validate_before_mutate");
}

/* Layer B: an ACTIVE Forward-0 subscription is a live destination that is
 * excluded from delivery, yet the object is still admitted and advances the
 * track's largest-location history (history observes publication, not
 * transmission). */
void test_pub_history_forward_zero_advances(void) {
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    simpair_setup(&as, &alloc, &sp);
    moq_session_t *sv = moq_simpair_server(sp);

    /* Register an on_subscriber_updated callback so the update being handled
     * by the facade is proven directly (order-independent), not inferred. */
    tick_cb_state_t cbs = {0};
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.callbacks.ctx = &cbs;
    cfg.callbacks.on_subscriber_updated = tick_on_updated;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Client subscribes; publisher accepts (ACCEPT_ALL). */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(sp), &scfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    /* Drop the subscription to Forward 0: it stays an active destination but
     * takes no objects. */
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true;
    ucfg.forward = false;
    MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(sp),
        sub_h, &ucfg, moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_pub_tick(pub, moq_simpair_now_us(sp));
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
          moq_event_cleanup(&ev);
      } }
    /* The facade's on_subscriber_updated fired with Forward=0 -- the update
     * was actually handled (not merely delivered on the wire). */
    MOQ_TEST_CHECK(cbs.updated >= 1);
    MOQ_TEST_CHECK(cbs.last_update.has_forward && !cbs.last_update.forward);
    /* And the slot is STILL active after the Forward-0 update (not retired):
     * with the zero delivery below -- despite an active destination -- this
     * distinguishes Forward-0 exclusion from accidental slot retirement. */
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(pub, track), 1);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));

    /* Write: MOQ_OK, no delivery to the (active) Forward-0 subscriber, but
     * history advances. */
    uint8_t d[] = { 0x61, 0x62 };
    moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
    MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 5, 9, pay,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT(fanout_count_objects(sp, 5, 9, d, sizeof(d)), 0);
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 5 && r->largest_object == 9);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_forward_zero_advances");
}

/* Fail-injecting allocator: fails the Nth alloc onward (0 = never). */
typedef struct { int64_t bal; int calls; int fail_at; } pub_fail_state_t;
static void *pub_fail_alloc(size_t n, void *ctx) {
    pub_fail_state_t *s = (pub_fail_state_t *)ctx;
    s->calls++;
    if (s->fail_at && s->calls >= s->fail_at) return NULL;
    void *p = malloc(n);
    if (p) s->bal++;
    return p;
}
static void *pub_fail_realloc(void *p, size_t o, size_t n, void *c)
{ (void)o; (void)c; return realloc(p, n); }
static void pub_fail_free(void *p, size_t n, void *ctx) {
    pub_fail_state_t *s = (pub_fail_state_t *)ctx; (void)n;
    if (p) s->bal--;
    free(p);
}
static moq_alloc_t pub_fail_allocator(pub_fail_state_t *s) {
    moq_alloc_t a = { s, pub_fail_alloc, pub_fail_realloc, pub_fail_free };
    return a;
}

/* Layer B: add_track's history reservation is the registry allocation site.
 * A temporary-key OOM, a record-key-copy OOM, or a failure AFTER a successful
 * reservation (namespace advertisement) all fail add_track cleanly: *out
 * NULL, no linked track, the empty record reclaimed, allocator balanced, and
 * a retry succeeds. The session uses a fail-injecting allocator (the two
 * reservation allocs are the only session allocs add_track makes with
 * advertise_namespace off); the publisher uses a separate clean allocator so
 * only the session-side reservation path is perturbed. */
void test_pub_history_add_track_oom(void) {
    pub_fail_state_t fs = {0};
    moq_alloc_t sess_alloc = pub_fail_allocator(&fs);
    test_alloc_state_t pas = {0};
    moq_alloc_t pub_alloc = test_allocator(&pas);
    moq_simpair_t *sp = NULL;
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &sess_alloc;
        cfg.seed = 42; cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }
    moq_session_t *sv = moq_simpair_server(sp);
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &pub_alloc, &pcfg, &pub);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");   /* advertise_namespace off */

    /* (a) temporary-key build OOM: the first session alloc in add_track. */
    fs.fail_at = fs.calls + 1;
    moq_pub_track_t *t = (moq_pub_track_t *)0x1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t)
        == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(t == NULL);
    fs.fail_at = 0;
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") == NULL);

    /* (b) record-key-copy OOM: the second session alloc in add_track. */
    fs.fail_at = fs.calls + 2;
    t = (moq_pub_track_t *)0x1;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t)
        == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(t == NULL);
    fs.fail_at = 0;
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") == NULL);

    /* (c) post-reservation failure: advertise the namespace and fail the
     * first session alloc AFTER the two reservation allocs (namespace
     * publish). The reservation succeeded, so this proves free_track_state
     * reclaims the now-empty record on the add_track failure path. */
    moq_pub_track_cfg_t acfg = tcfg;
    acfg.advertise_namespace = true;
    int rollback_fail_at = fs.calls + 3;   /* first alloc after the 2 reserve allocs */
    fs.fail_at = rollback_fail_at;
    t = (moq_pub_track_t *)0x1;
    moq_result_t rc = moq_pub_add_track(pub, &acfg, moq_simpair_now_us(sp), &t);
    /* Must be the injected allocation failure (not an unrelated WOULD_BLOCK),
     * and the injected alloc must actually have been reached -- i.e. the
     * reservation succeeded and a later allocation failed. */
    MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(fs.calls >= rollback_fail_at);
    MOQ_TEST_CHECK(t == NULL);
    fs.fail_at = 0;
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") == NULL); /* reclaimed */

    /* Retry with injection off succeeds and reserves the record. */
    t = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &t)
        == MOQ_OK);
    MOQ_TEST_CHECK(t != NULL);
    MOQ_TEST_CHECK(pub_find_hist(sv, "live", "video") != NULL);
    moq_pub_remove_track(pub, t, moq_simpair_now_us(sp));

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(fs.bal == 0);       /* session allocator balanced */
    MOQ_TEST_CHECK(pas.balance == 0);  /* publisher allocator balanced */
    MOQ_TEST_PASS("pub_history_add_track_oom");
}

/* Layer B: draft-18 malformed object properties (a Mandatory Track Property,
 * 0x4000-0x7FFF, carried as an object property) are rejected before mutation,
 * even with zero destinations -- the profile property hook runs in the
 * pre-commit validation, so Largest is not poisoned. */
void test_pub_history_malformed_properties_d18(void) {
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_simpair_t *sp = NULL;
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 42;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }
    moq_session_t *sv = moq_simpair_server(sp);

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(sv, &alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

    /* Property blob = one KVP {type=0x4000 (Mandatory Track Property), 0}. */
    uint8_t props[8];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, props, sizeof(props));
    moq_buf_write_vi64(&w, 0x4000);
    moq_buf_write_vi64(&w, 0);
    moq_rcbuf_t *pr = NULL;
    moq_rcbuf_create(&alloc, props, moq_buf_writer_offset(&w), &pr);

    uint8_t d[] = { 1 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pay);

    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = 2;
    oc.object_id = 0;
    oc.payload = pay;
    oc.properties = pr;
    /* Rejected before mutation; Largest not advanced. */
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc,
        moq_simpair_now_us(sp)) == MOQ_ERR_INVAL);
    moq_track_hist_t *r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r != NULL && !r->has_largest);
    moq_rcbuf_decref(pr);

    /* A valid write (no properties) then succeeds and advances Largest,
     * proving no op was left pending by the rejection. */
    moq_pub_object_cfg_t oc2;
    moq_pub_object_cfg_init_sized(&oc2, sizeof(oc2));
    oc2.group_id = 2;
    oc2.object_id = 0;
    oc2.payload = pay;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &oc2,
        moq_simpair_now_us(sp)) == MOQ_OK);
    r = pub_find_hist(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest && r->largest_group == 2);
    moq_rcbuf_decref(pay);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("pub_history_malformed_properties_d18");
}

/* White-box admission counters (defined in publisher.c only under
 * MOQ_PUB_TESTING, which moq-core-test-internals compiles with; absent from
 * the shipping library). */
extern unsigned long moq_pub_test_commit_count;
extern unsigned long moq_pub_test_merge_count;
extern unsigned long moq_pub_test_release_count;

/* Layer B: admission is exactly-once across a WOULD_BLOCK retry. A new op
 * commits + merges Largest once and, on completion, releases its retained
 * snapshot once; a byte-identical retry that resumes the pending op commits
 * and merges ZERO additional times. Proven with the white-box counters --
 * value-only assertions cannot discriminate a double (idempotent) merge. */
void test_pub_history_exactly_once_retry(void) {
    /* One-shot object: 3-action queue blocks mid-fan-out on destination B. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);

        moq_pub_test_commit_count = 0;
        moq_pub_test_merge_count = 0;
        moq_pub_test_release_count = 0;

        uint8_t d[] = { 0x41, 0x42, 0x43, 0x44 };
        moq_rcbuf_t *pay = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &pay);
        moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, pay,
            moq_simpair_now_us(sp));
        moq_rcbuf_decref(pay);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        /* New op: committed + merged once; not yet released (still pending). */
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 0);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_rcbuf_t *p2 = NULL; moq_rcbuf_create(&alloc, d, sizeof(d), &p2);
        rc = moq_pub_write_object(pub, track, 0, 0, p2, moq_simpair_now_us(sp));
        moq_rcbuf_decref(p2);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        /* Resume: NO further commit/merge; snapshot released exactly once. */
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 1);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Streaming begin_object: 3-action queue blocks mid-fan-out on the second
     * destination (open A + begin A + open B consume 3; begin B is the 4th). */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        moq_publisher_t *pub; moq_pub_track_t *track;
        fanout_setup(&as, &alloc, &sp, &pub, &track, true, 3);

        moq_pub_test_commit_count = 0;
        moq_pub_test_merge_count = 0;
        moq_pub_test_release_count = 0;

        moq_pub_begin_object_cfg_t bo;
        moq_pub_begin_object_cfg_init_sized(&bo, sizeof(bo));
        bo.group_id = 0;
        bo.object_id = 0;
        bo.payload_length = 4;
        moq_result_t rc = moq_pub_begin_object(pub, track, &bo,
            moq_simpair_now_us(sp));
        /* The begin must actually block mid-fan-out (else this proves no
         * retry). New op committed + merged once; not yet released. */
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 0);

        int guard = 0;
        while (rc == MOQ_ERR_WOULD_BLOCK && guard++ < 16) {
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            rc = moq_pub_begin_object(pub, track, &bo, moq_simpair_now_us(sp));
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);
        /* Retries never re-committed or re-merged; released exactly once. */
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_merge_count, 1);
        MOQ_TEST_CHECK_EQ_U64(moq_pub_test_release_count, 1);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
    MOQ_TEST_PASS("pub_history_exactly_once_retry");
}

/* -- Group history: identity, finality and retained installs ------------ */

/* Install a dense retained group 0..n-1 (n <= 16) and return the result. */
/* Retained payload of Object `o` in these rows; a live write of the same
 * Object uses the same bytes (declare_write_stream with this tag), since an
 * Object's identity includes its payload. */
#define HIST_TAG(o) ((uint8_t)(0xA0u + (o)))

/* Install a dense retained group 0..n-1 (n <= 16); every payload buffer is
 * checked. */
static moq_result_t hist_install(moq_publisher_t *pub, moq_pub_track_t *t,
                                 moq_alloc_t *alloc, uint64_t g, size_t n)
{
    moq_pub_retained_object_t objs[16];
    moq_rcbuf_t *pl[16];
    memset(objs, 0, sizeof(objs));
    memset(pl, 0, sizeof(pl));
    if (n > 16) n = 16;
    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < n && rc == MOQ_OK; i++) {
        uint8_t d[2] = { HIST_TAG(i), (uint8_t)(HIST_TAG(i) ^ 0xFFu) };
        rc = moq_rcbuf_create(alloc, d, sizeof(d), &pl[i]);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        objs[i].object_id = i;
        objs[i].payload = pl[i];
    }
    if (rc == MOQ_OK) {
        moq_pub_retained_group_cfg_t gc;
        moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = g;
        gc.objects = objs;
        gc.object_count = n;
        rc = moq_pub_set_retained_group(pub, t, &gc);
    }
    for (size_t i = 0; i < 16; i++)
        if (pl[i]) moq_rcbuf_decref(pl[i]);
    return rc;
}

/* windows_setup with every prerequisite checked; every output starts NULL.
 * Returns false, with whatever was created left for hist_rig_teardown, when
 * any step fails. */
static bool hist_rig_setup(moq_version_t ver, test_alloc_state_t *as,
                           moq_alloc_t *alloc, moq_simpair_t **sp,
                           moq_publisher_t **pub, moq_pub_track_t **track)
{
    *sp = NULL; *pub = NULL; *track = NULL;
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    if (moq_simpair_create(&cfg, sp) != MOQ_OK || !*sp) return false;
    if (moq_simpair_start(*sp) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    drain_all(*sp);
    moq_pub_cfg_t cfg2; moq_pub_cfg_init_sized(&cfg2, sizeof(cfg2));
    cfg2.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(moq_simpair_server(*sp), alloc, &cfg2, pub) != MOQ_OK ||
        !*pub)
        return false;
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    return moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track) ==
               MOQ_OK && *track;
}

static void hist_rig_teardown(test_alloc_state_t *as, moq_simpair_t *sp,
                              moq_publisher_t *pub)
{
    if (pub) moq_pub_destroy(pub);
    if (sp) {
        drain_all(sp);
        moq_simpair_destroy(sp);
    }
    MOQ_TEST_CHECK(as->balance == 0);
}

/* Checked setup inside a row: on failure, record it, release what exists and
 * leave the row. */
#define HIST_RIG_OR_RETURN(ver) do { \
    if (!hist_rig_setup((ver), &as, &alloc, &sp, &pub, &track)) { \
        MOQ_TEST_CHECK(false); \
        hist_rig_teardown(&as, sp, pub); \
        return; \
    } \
} while (0)

/* A status datagram at {g,o}. */
static moq_result_t hist_status(moq_publisher_t *pub, moq_pub_track_t *t,
                                moq_simpair_t *sp, uint64_t g, uint64_t o,
                                moq_object_status_t status)
{
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init(&oc);
    oc.group_id = g; oc.object_id = o; oc.datagram = true;
    oc.has_status = true; oc.status = status;
    moq_result_t rc = moq_pub_write_object_ex(pub, t, &oc,
                                              moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    return rc;
}

/* Every Object keeps the (Group, Subgroup ID, delivery mode) it was assigned
 * at admission (draft-22 sections 2.1 and 7.1; draft-18 2.1; draft-16 2.4),
 * and nothing is assigned above a group's final Object (draft-22 section
 * 12.1, draft-16 section 2.4.2). Each refusal is before any effect; the
 * subscriber never sees a malformed track. 16 groups, so no record is
 * displaced. */
void test_hist_identity_rules(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    const bool has_bit = ver != MOQ_VERSION_DRAFT_16;
    uint64_t now = moq_simpair_now_us(sp);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    drain_all(sp);
    int ended = 0;
    bool f = false;
    uint64_t sgid = UINT64_MAX;

    /* Delivery mode: a known Object never changes it. */
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 7, 0, 0xB0)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              7, 0, 0xB1), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 8, 0, 0xB2)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 8, 0,
                              0xB3), (int)MOQ_ERR_WRONG_STATE);
    (void)fo_seen(sp, 8, 0, NULL, NULL, &ended);

    /* An END_OF_GROUP status at B: nothing at or above B, either mode. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 9, 0, 0xB4)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 9, 1, 0xB5)
                   == MOQ_OK);
    MOQ_TEST_CHECK(hist_status(pub, track, sp, 9, 2, MOQ_OBJECT_END_OF_GROUP)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              9, 2, 0xB6), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              9, 5, 0xB7), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 9, 3,
                              0xB8), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 9, 5, NULL, NULL, &ended), 0);

    /* END_OF_GROUP header on several Objects, then a clean FIN when the
     * track moves on: Object 1 is the group's final, so even a revisit
     * above every assigned Object is refused. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 10, 0, true,
                                         0xB9) == MOQ_OK);
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 10, 1, true,
                                         0xBA) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 11, 0, 0xBB)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              10, 2, 0xBC), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 10, 2, NULL, NULL, &ended), 0);
    /* A RESET fixes no final: the group continues in a fresh subgroup. */
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 12, 0, true,
                                         0xBD) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10, now) == MOQ_OK);
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 12, 1, false,
                                         0xBE) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 12, 1, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK(f == has_bit);
    MOQ_TEST_CHECK_EQ_U64(sgid, 1);

    /* After a RESET the group's Objects keep their assignments: an
     * admitted Object cannot be assigned again, a larger one opens the next
     * subgroup. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 13, 0, 0xC0)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 13, 1, 0xC1)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10, now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              13, 1, 0xC2), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 13, 2, 0xC3)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 13, 2, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK(f == has_bit);
    MOQ_TEST_CHECK_EQ_U64(sgid, 1);

    /* end_group seals the group for every admission path. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 14, 0, 0xC4)
                   == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              14, 1, 0xC5), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 14, 5,
                              0xC6), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 14, 1),
                          (int)MOQ_ERR_WRONG_STATE);

    /* Retained installs never create a new identity inside a group's
     * history: a gap, a revisited group and a datagram group are refused. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 20, 0, 0xC7)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 20, 3, 0xC8)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 20, 4),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 21, 0, 0xC9)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 22, 0, 0xCA)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 21, 1, 0xCB)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 21, 2),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 23, 0, 0xCC)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 23, 1),
                          (int)MOQ_ERR_WRONG_STATE);

    /* A subgroup that ended with a FIN when the track moved on takes no
     * extension; replaying its unchanged prefix is allowed. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 24, 0, HIST_TAG(0))
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 25, 0, 0xCE)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 24, 2),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 24, 1) == MOQ_OK);
    /* While no stream of the current subgroup has ended, an extension is
     * allowed. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 27, 0, HIST_TAG(0))
                   == MOQ_OK);
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 27, 2) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 26, 0, HIST_TAG(0))
                   == MOQ_OK);
    (void)fo_seen(sp, 26, 0, NULL, NULL, &ended);
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    /* One destination's clean FIN, with the track still in the group,
     * fixes the subgroup's final for that receiver: no extension. */
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
                       MOQ_PUB_DONE_TRACK_ENDED, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 26, 2),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 26, 1) == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_identity_rules_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_identity_rules_d18"
                  : "hist_identity_rules_d16");
}

/* A standalone FETCH on draft-16; counts the objects served (with the
 * largest Subgroup ID seen) and the code of a refusal, if any: a
 * REQUEST_ERROR on the wire, surfaced locally as MOQ_EVENT_FETCH_ERROR. */
static void hist_d16_fetch(moq_simpair_t *sp, moq_publisher_t *pub,
                           uint64_t g, uint64_t end_object, int *nobj,
                           uint64_t *sg_max, bool *complete, uint64_t *err)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace.parts = ns_parts;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.start_group = g; fcfg.start_object = 0;
    fcfg.end_group = g; fcfg.end_object = end_object;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(moq_session_fetch(cl, &fcfg, now, &fh) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, now, 0, NULL, NULL);
    MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    *nobj = 0; *sg_max = 0; *complete = false; *err = UINT64_MAX;
    moq_event_t d[16]; size_t ne;
    while ((ne = moq_session_poll_events(cl, d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                (*nobj)++;
                if (d[i].u.fetch_object.subgroup_id > *sg_max)
                    *sg_max = d[i].u.fetch_object.subgroup_id;
            } else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                *complete = true;
            } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                *err = d[i].u.fetch_error.error_code;
            }
            moq_event_cleanup(&d[i]);
        }
}

/* Draft-16: the last Object sent in a FETCH that requested the entire Group
 * is the Group's final Object (draft-16 section 2.4.2, condition 7). Such a
 * FETCH is answered from the retained Objects only when nothing above them
 * was assigned -- otherwise with REQUEST_ERROR NOT_SUPPORTED (0x3), the
 * session staying open -- and once staged it refuses later Objects above
 * them; a FETCH for an exact range fixes nothing. */
void test_hist_d16_whole_group_fetch(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(MOQ_VERSION_DRAFT_16);
    int n = 0; uint64_t sgm = 0, err = 0; bool done = false;

    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 0, 2) == MOQ_OK);
    /* Exact range 0..1 (End Location {0, 2}): served, nothing fixed. */
    hist_d16_fetch(sp, pub, 0, 2, &n, &sgm, &done, &err);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    MOQ_TEST_CHECK_EQ_U64(sgm, 0);
    MOQ_TEST_CHECK(done);
    /* Entire group 0: served, and Object 1 is now its final. */
    hist_d16_fetch(sp, pub, 0, 0, &n, &sgm, &done, &err);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    MOQ_TEST_CHECK(done);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              0, 2, 0xD1), (int)MOQ_ERR_WRONG_STATE);

    /* Group 5: retained 0..1, then live Object 2 admitted. The entire group
     * cannot be answered from the cache; the refusal leaves history as it
     * was, so the group still continues. */
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 5, 2) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 5, 2, 0xD2)
                   == MOQ_OK);
    hist_d16_fetch(sp, pub, 5, 0, &n, &sgm, &done, &err);
    MOQ_TEST_CHECK_EQ_INT(n, 0);
    MOQ_TEST_CHECK(!done);
    MOQ_TEST_CHECK_EQ_U64(err, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 5, 3, 0xD3)
                   == MOQ_OK);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("hist_d16_whole_group_fetch");
}

/* Count the client's SUBGROUP_FINISHED and SUBGROUP_RESET for group g; every
 * other event is dropped. */
static void hist_stream_ends(moq_simpair_t *sp, uint64_t g, int *fins,
                             int *resets)
{
    moq_event_t ev;
    for (int round = 0; round < 4; round++) {
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED &&
                ev.u.subgroup_finished.group_id == g) (*fins)++;
            if (ev.kind == MOQ_EVENT_SUBGROUP_RESET &&
                ev.u.subgroup_reset.group_id == g) (*resets)++;
            moq_event_cleanup(&ev);
        }
    }
}

/* A destination's stream ends with a FIN only when it carried every Object of
 * its subgroup except those below its Start Location; otherwise it is RESET
 * (draft-22 section 11.3.2; draft-18 and draft-16 the same).
 *   case 0: retained 0..10 installed, then live Object 0 on a destination
 *           whose window admits 0..10: the switch to another group RESETs
 *           (Objects 1..10 are assigned to its subgroup but never sent).
 *   case 1: complete delivery: the switch FINs.
 *   case 2: Object 0 lies below the destination's Start Location {40, 1}:
 *           it is exempt, so the stream that carried Object 1 FINs. */
static void hist_close_case(moq_version_t ver, int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    const bool has_bit = ver != MOQ_VERSION_DRAFT_16;
    int ended = 0, fins = 0, resets = 0;
    bool f = false;
    uint64_t sgid = UINT64_MAX;

    if (c == 0) {
        MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 30, 11) == MOQ_OK);
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                          30, 0, 0);
        drain_all(sp);
        /* Object 0 of the retained-origin subgroup: this stream begins the
         * subgroup, so it carries FIRST_OBJECT. */
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 0,
                                            HIST_TAG(0)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, 0, &f, &sgid, &ended), 1);
        MOQ_TEST_CHECK(f == has_bit);
        MOQ_TEST_CHECK_EQ_U64(sgid, 0);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 31, 0,
                                            0xD5) == MOQ_OK);
        hist_stream_ends(sp, 30, &fins, &resets);
        MOQ_TEST_CHECK_EQ_INT(fins, 0);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
    } else if (c == 1) {
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                          30, 0, 0);
        drain_all(sp);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 0,
                                            0xD6) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 1,
                                            0xD7) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 31, 0,
                                            0xD8) == MOQ_OK);
        hist_stream_ends(sp, 30, &fins, &resets);
        MOQ_TEST_CHECK_EQ_INT(fins, 1);
        MOQ_TEST_CHECK_EQ_INT(resets, 0);
    } else {
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                          40, 1, 0);
        drain_all(sp);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 40, 0,
                                            0xD9) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 40, 1,
                                            0xDA) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 40, 1, &f, &sgid, &ended), 1);
        MOQ_TEST_CHECK(!f);   /* starts mid-subgroup */
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 41, 0,
                                            0xDB) == MOQ_OK);
        hist_stream_ends(sp, 40, &fins, &resets);
        MOQ_TEST_CHECK_EQ_INT(fins, 1);
        MOQ_TEST_CHECK_EQ_INT(resets, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

void test_hist_destination_close(moq_version_t ver)
{
    hist_close_case(ver, 0);
    hist_close_case(ver, 1);
    hist_close_case(ver, 2);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_destination_close_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_destination_close_d18"
                  : "hist_destination_close_d16");
}

/* An admitted END_OF_TRACK status at {50, 3}: nothing at or beyond that
 * Location is admitted afterwards, stream or datagram (draft-22 section
 * 12.1, condition 5), on a plain track with no destinations. */
void test_hist_end_of_track_status(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);

    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 50, 0, 0xDC)
                   == MOQ_OK);
    MOQ_TEST_CHECK(hist_status(pub, track, sp, 50, 3, MOQ_OBJECT_END_OF_TRACK)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              50, 3, 0xDD), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              51, 0, 0xDE), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 50, 4,
                              0xDF), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 60, 0,
                              0xE0), (int)MOQ_ERR_WRONG_STATE);
    {
        moq_pub_begin_object_cfg_t bc;
        moq_pub_begin_object_cfg_init_sized(&bc, sizeof(bc));
        bc.group_id = 52; bc.object_id = 0; bc.payload_length = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_begin_object(pub, track, &bc,
                                  moq_simpair_now_us(sp)),
                              (int)MOQ_ERR_WRONG_STATE);
    }
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 53, 1),
                          (int)MOQ_ERR_WRONG_STATE);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_end_of_track_status_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_end_of_track_status_d18"
                  : "hist_end_of_track_status_d16");
}

/* Boundary rows for the stream-record bounds, with no destinations (the
 * bookkeeping is the same either way): 16 records (16 and 17 groups), at
 * most 15 revisited groups (15 and 16), at most 7 revisits per group (8 and
 * 9 subgroups), the rolling victim (including the group the track just
 * left) and exact per-group maxima. Sealed-run capacity and datagram-only
 * records are not covered here. Same on every draft. */
void test_hist_capacity_sweep(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
#define HW(g, o) ((int)declare_write_stream(pub, track, &alloc, sp, (g), (o), 0x77))

    /* Sixteen groups fill the records; the seventeenth displaces group 0. */
    for (uint64_t g = 0; g < 16; g++)
        MOQ_TEST_CHECK_EQ_INT(HW(g, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(HW(16, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(HW(0, 1), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(HW(0, 0), (int)MOQ_ERR_WRONG_STATE);

    /* Groups 1..15 revisited: fifteen revisited groups. */
    for (uint64_t g = 1; g <= 15; g++)
        MOQ_TEST_CHECK_EQ_INT(HW(g, 1), (int)MOQ_OK);
    /* A sixteenth is refused, and changes nothing. */
    MOQ_TEST_CHECK_EQ_INT(HW(16, 1), (int)MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_INT(HW(16, 1), (int)MOQ_ERR_NOMEM);

    /* One record rolls: a new group displaces 16, then the next new group
     * displaces 17, the group the track just left. */
    MOQ_TEST_CHECK_EQ_INT(HW(17, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(HW(16, 1), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(HW(18, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(HW(17, 1), (int)MOQ_ERR_WRONG_STATE);

    /* Seven revisits per group: groups 1 and 2 (each at Subgroup 1)
     * alternate up to Subgroup 7; an eighth revisit of group 1 is refused. */
    for (uint64_t k = 2; k <= 7; k++) {
        MOQ_TEST_CHECK_EQ_INT(HW(1, k), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(HW(2, k), (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(HW(1, 8), (int)MOQ_ERR_NOMEM);

    /* Revisits stay exact per group: at or below the group's largest
     * Object is refused, above it admitted, with no coupling to other
     * groups. */
    MOQ_TEST_CHECK_EQ_INT(HW(3, 1), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(HW(3, 100), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(HW(4, 2), (int)MOQ_OK);
#undef HW

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_capacity_sweep_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_capacity_sweep_d18"
                  : "hist_capacity_sweep_d16");
}

/* Draft-22 subscription with an inclusive end Object, accepted by the
 * facade. */
static void hist_subscribe_object_end(moq_simpair_t *sp, moq_publisher_t *pub,
                                      uint64_t sg, uint64_t so,
                                      uint64_t eg, uint64_t eo)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init_sized(&loc, sizeof(loc));
    loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    loc.start_group = sg; loc.start_object = so;
    loc.end_group = eg; loc.end_object = eo;
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.location = &loc;
    moq_subscription_t h;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &h)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    drain_all(sp);
}

/* Subgroup completeness on draft-22 windows with an end Object. Only Objects
 * below the Start Location are exempt from completeness (draft-22 section
 * 11.3.2); an Object past the window's end is never delivered, and a stream
 * that ends without it ends with a RESET.
 *   case 0: window {30,0}..{30,1}; Objects 0..2 assigned; end_group: RESET.
 *   case 1: window {30,0}..{30,2}; the same Objects all delivered: FIN.
 *   case 2: window {30,1}..{30,5}; Object 0 is below Start: FIN.
 *   case 3: as case 0, then seventeen later groups displace group 30's
 *           record before the close: the omission recorded before the
 *           displacement still gives RESET. */
static void hist_end_window_case(int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(MOQ_VERSION_DRAFT_22);
    int ended = 0, fins = 0, resets = 0;
    uint64_t now = moq_simpair_now_us(sp);

    if (c == 1) hist_subscribe_object_end(sp, pub, 30, 0, 30, 2);
    else if (c == 2) hist_subscribe_object_end(sp, pub, 30, 1, 30, 5);
    else hist_subscribe_object_end(sp, pub, 30, 0, 30, 1);

    for (uint64_t o = 0; o <= 2; o++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, o,
                                            (uint8_t)(0x60 + o)) == MOQ_OK);
    /* Delivery stays window-bound. */
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, 2, NULL, NULL, &ended),
                          c == 1 || c == 2 ? 1 : 0);
    if (c == 3)
        for (uint64_t g = 31; g <= 47; g++)
            MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, g, 0,
                                                0x70) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    hist_stream_ends(sp, 30, &fins, &resets);
    if (c == 1 || c == 2) {
        MOQ_TEST_CHECK_EQ_INT(fins, 1);
        MOQ_TEST_CHECK_EQ_INT(resets, 0);
    } else {
        MOQ_TEST_CHECK_EQ_INT(fins, 0);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
    }
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

void test_hist_end_window_completeness(void)
{
    for (int c = 0; c < 4; c++) hist_end_window_case(c);
    MOQ_TEST_PASS("hist_end_window_completeness_d22");
}

/* FETCH staging boundaries. The allocator wrapper can, while armed, count
 * the allocations made (mode 1) or fail the first of them (mode 2),
 * recording the size of the first one either way, so a run shows exactly
 * which allocation a staging made or lost. */
static int    hist_alloc_mode;          /* 0 off, 1 count, 2 fail the first */
static int    hist_stage_allocs;        /* allocations while armed */
static size_t hist_stage_first_size;    /* size of the first of them */
static int    hist_failed_allocs;       /* allocations failed while armed */
static void *hist_failing_alloc(size_t n, void *ctx)
{
    if (hist_alloc_mode) {
        if (hist_stage_allocs++ == 0) hist_stage_first_size = n;
        if (hist_alloc_mode == 2 && hist_stage_allocs == 1) {
            hist_failed_allocs++;
            return NULL;
        }
    }
    return test_alloc(n, ctx);
}

static void hist_alloc_arm(int mode)
{
    hist_alloc_mode = mode;
    hist_stage_allocs = 0;
    hist_stage_first_size = 0;
    hist_failed_allocs = 0;
}

/* Pair, publisher and track on `ver`; every output starts NULL. Returns
 * false, with whatever was created left in the outputs for
 * hist_fetch_teardown, when any prerequisite fails. */
static bool hist_fetch_setup(moq_version_t ver, test_alloc_state_t *as,
                             moq_alloc_t *alloc, moq_simpair_t **sp,
                             uint32_t max_actions, moq_publisher_t **pub,
                             moq_pub_track_t **track)
{
    *sp = NULL; *pub = NULL; *track = NULL;
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    alloc->alloc = hist_failing_alloc;
    hist_alloc_arm(0);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.max_actions = max_actions;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    if (moq_simpair_create(&cfg, sp) != MOQ_OK || !*sp) return false;
    if (moq_simpair_start(*sp) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    drain_all(*sp);
    moq_pub_cfg_t cfg2; moq_pub_cfg_init_sized(&cfg2, sizeof(cfg2));
    cfg2.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(moq_simpair_server(*sp), alloc, &cfg2, pub) != MOQ_OK ||
        !*pub)
        return false;
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    return moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track) ==
               MOQ_OK && *track;
}

static void hist_fetch_teardown(test_alloc_state_t *as, moq_simpair_t *sp,
                                moq_publisher_t *pub)
{
    hist_alloc_arm(0);
    if (pub) moq_pub_destroy(pub);
    if (sp) {
        drain_all(sp);
        moq_simpair_destroy(sp);
    }
    MOQ_TEST_CHECK(as->balance == 0);
}

/* Client side of a standalone FETCH of group `g` from Object 0 to End
 * Location {g, end_object} (0: the whole group); the request reaches the
 * server, which has not handled it yet. */
static bool hist_fetch_request(moq_simpair_t *sp, uint64_t g,
                               uint64_t end_object, moq_fetch_t *fh)
{
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace.parts = ns_parts;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = MOQ_BYTES_LITERAL("video");
    fcfg.start_group = g; fcfg.start_object = 0;
    fcfg.end_group = g; fcfg.end_object = end_object;
    moq_result_t rc = moq_session_fetch(moq_simpair_client(sp), &fcfg,
                                        moq_simpair_now_us(sp), fh);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    return rc == MOQ_OK;
}

/* Hand the server's pending FETCH_REQUEST to the facade; its result. */
static moq_result_t hist_fetch_stage(moq_simpair_t *sp, moq_publisher_t *pub)
{
    moq_session_t *sv = moq_simpair_server(sp);
    moq_result_t rc = MOQ_ERR_INVAL;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
            rc = moq_pub_handle_event(pub, &ev, moq_simpair_now_us(sp), &res);
            MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        }
        moq_event_cleanup(&ev);
    }
    return rc;
}

/* What the client saw for FETCH `fh`: FETCH_OK, objects, completion, and a
 * refusal -- a REQUEST_ERROR on the wire, the local event FETCH_ERROR --
 * with its code. Events for other handles are counted as strays. */
typedef struct {
    int      oks, nobj, completes, errors, strays;
    uint64_t err;
} hist_fetch_seen_t;

/* A bounded drive -- eight flush rounds, each running the pair to
 * quiescence -- that collects the outcome. It injects no backpressure: a row
 * that needs a blocked response arranges it itself. */
static void hist_fetch_settle(moq_simpair_t *sp, moq_publisher_t *pub,
                              moq_fetch_t fh, hist_fetch_seen_t *o)
{
    memset(o, 0, sizeof(*o));
    o->err = UINT64_MAX;
    for (int round = 0; round < 8; round++) {
        (void)moq_pub_flush(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            moq_fetch_t h = MOQ_FETCH_INVALID;
            if (ev.kind == MOQ_EVENT_FETCH_OK) h = ev.u.fetch_ok.fetch;
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) h = ev.u.fetch_object.fetch;
            if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) h = ev.u.fetch_complete.fetch;
            if (ev.kind == MOQ_EVENT_FETCH_ERROR) h = ev.u.fetch_error.fetch;
            if (moq_fetch_is_valid(h)) {
                if (!moq_fetch_eq(h, fh)) o->strays++;
                else if (ev.kind == MOQ_EVENT_FETCH_OK) o->oks++;
                else if (ev.kind == MOQ_EVENT_FETCH_OBJECT) o->nobj++;
                else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) o->completes++;
                else {
                    o->errors++;
                    o->err = ev.u.fetch_error.error_code;
                }
            }
            MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_SESSION_CLOSED);
            moq_event_cleanup(&ev);
        }
    }
}

static bool hist_fetch_sessions_alive(moq_simpair_t *sp)
{
    return moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_ESTABLISHED;
}

/* A staging whose snapshot cannot be allocated refuses that exact request
 * with REQUEST_ERROR INTERNAL_ERROR (code 0) and nothing else: no FETCH_OK,
 * no Object, no completion, both sessions alive, every allocation balanced.
 * The refusal is decided by the facade's refusal flag, never by the code,
 * which is zero. The failed allocation is shown to be the snapshot: it has
 * the size of the first allocation a successful staging of the same
 * retained group makes. On draft-16 the request asked for the entire group,
 * and the failed staging fixes no final Object: Object 2 is still admitted.
 * Draft-22 is the control whose refusal decision did not change. */
void test_hist_fetch_snapshot_failure(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    if (!hist_fetch_setup(ver, &as, &alloc, &sp, 0, &pub, &track)) {
        MOQ_TEST_CHECK(false);      /* setup prerequisite failed */
        hist_fetch_teardown(&as, sp, pub);
        return;
    }
    hist_fetch_seen_t seen;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 0, 2) == MOQ_OK);

    /* A successful staging of the exact range 0..1: its first allocation is
     * the snapshot this group needs. */
    size_t snapshot_size = 0;
    if (hist_fetch_request(sp, 0, 2, &fh)) {
        hist_alloc_arm(1);
        MOQ_TEST_CHECK_EQ_INT((int)hist_fetch_stage(sp, pub), (int)MOQ_OK);
        snapshot_size = hist_stage_first_size;
        MOQ_TEST_CHECK(hist_stage_allocs >= 1);
        hist_alloc_arm(0);
        hist_fetch_settle(sp, pub, fh, &seen);
        MOQ_TEST_CHECK(seen.oks == 1 && seen.nobj == 2 && seen.completes == 1 &&
                       seen.errors == 0 && seen.strays == 0);
    }

    /* The same group again, with that allocation failing. */
    if (hist_fetch_request(sp, 0, 0, &fh)) {
        hist_alloc_arm(2);
        moq_result_t st = hist_fetch_stage(sp, pub);
        int consumed = hist_failed_allocs;
        size_t failed_size = hist_stage_first_size;
        hist_alloc_arm(0);
        fprintf(stderr, "hist_fetch_snapshot_failure draft %u: failed %d "
                "allocation(s), size %zu (snapshot %zu), stage result %d\n",
                (unsigned)ver, consumed, failed_size, snapshot_size, (int)st);
        MOQ_TEST_CHECK_EQ_INT(consumed, 1);
        MOQ_TEST_CHECK(snapshot_size != 0 && failed_size == snapshot_size);
        MOQ_TEST_CHECK_EQ_INT((int)st, (int)MOQ_OK);
        hist_fetch_settle(sp, pub, fh, &seen);
        MOQ_TEST_CHECK_EQ_INT(seen.oks, 0);
        MOQ_TEST_CHECK_EQ_INT(seen.nobj, 0);
        MOQ_TEST_CHECK_EQ_INT(seen.completes, 0);
        MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
        MOQ_TEST_CHECK_EQ_INT(seen.strays, 0);
        MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        MOQ_TEST_CHECK(hist_fetch_sessions_alive(sp));
    }
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              0, 2, 0x81), (int)MOQ_OK);

    hist_fetch_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_fetch_snapshot_failure_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_fetch_snapshot_failure_d18"
                  : "hist_fetch_snapshot_failure_d16");
}

/* A retained group above the facade's conservative location guard (Group ID
 * MOQ_QUIC_VARINT_MAX + 1, i.e. 2^62: the facade refuses to answer with a
 * Location beyond the QUIC-varint range its data plane writes; it is not a
 * draft-18/22 wire limit -- those encode Locations as vi64 -- and a draft-16
 * request cannot name the group at all) is refused with REQUEST_ERROR
 * INTERNAL_ERROR (code 0) WITHOUT staging: the facade makes no allocation at
 * all while handling the request -- no snapshot is taken for a refused
 * request -- and the client sees only that refusal. The guard itself is
 * unchanged here. */
void test_hist_fetch_beyond_location_guard(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    if (!hist_fetch_setup(ver, &as, &alloc, &sp, 0, &pub, &track)) {
        MOQ_TEST_CHECK(false);
        hist_fetch_teardown(&as, sp, pub);
        return;
    }
    const uint64_t g = MOQ_QUIC_VARINT_MAX + 1;
    hist_fetch_seen_t seen;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, g, 2) == MOQ_OK);
    if (hist_fetch_request(sp, g, 0, &fh)) {
        hist_alloc_arm(1);
        moq_result_t st = hist_fetch_stage(sp, pub);
        int allocs = hist_stage_allocs;
        hist_alloc_arm(0);
        MOQ_TEST_CHECK_EQ_INT(allocs, 0);
        MOQ_TEST_CHECK_EQ_INT((int)st, (int)MOQ_OK);
        hist_fetch_settle(sp, pub, fh, &seen);
        MOQ_TEST_CHECK_EQ_INT(seen.oks, 0);
        MOQ_TEST_CHECK_EQ_INT(seen.nobj, 0);
        MOQ_TEST_CHECK_EQ_INT(seen.completes, 0);
        MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
        MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        MOQ_TEST_CHECK(hist_fetch_sessions_alive(sp));
    }
    hist_fetch_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_fetch_beyond_location_guard_d22"
                  : "hist_fetch_beyond_location_guard_d18");
}

/* A standalone FETCH for track `name` from {sg, so} to {eg, eo} (eo
 * exclusive, 0: the whole end group), handed to the facade and settled. */
static void fetch_range_outcome(moq_simpair_t *sp, moq_publisher_t *pub,
                                const char *name, uint64_t sg, uint64_t so,
                                uint64_t eg, uint64_t eo,
                                hist_fetch_seen_t *seen)
{
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace.parts = ns_parts;
    fcfg.track_namespace.count = 1;
    fcfg.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    fcfg.start_group = sg; fcfg.start_object = so;
    fcfg.end_group = eg; fcfg.end_object = eo;
    moq_fetch_t fh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(moq_simpair_client(sp), &fcfg,
                              moq_simpair_now_us(sp), &fh), (int)MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)hist_fetch_stage(sp, pub), (int)MOQ_OK);
    hist_fetch_settle(sp, pub, fh, seen);
    MOQ_TEST_CHECK(hist_fetch_sessions_alive(sp));
}

/* FETCH refusals by what the track has published: an unknown track is
 * DOES_NOT_EXIST; a track that exists with nothing published, or a start
 * past its Largest Object, is INVALID_RANGE (draft-22 §3.2, draft-18
 * §10.12.3; draft-16 §9.16.3 for the start past Largest, the no-object
 * standalone answer being library policy there); a start at or below the
 * Largest is served. */
void test_fetch_range_published(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    if (!hist_fetch_setup(ver, &as, &alloc, &sp, 0, &pub, &track)) {
        MOQ_TEST_CHECK(false);
        hist_fetch_teardown(&as, sp, pub);
        return;
    }
    hist_fetch_seen_t seen;
    fetch_range_outcome(sp, pub, "video", 0, 0, 0, 1, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_INVALID_RANGE);
    MOQ_TEST_CHECK_EQ_INT(seen.oks + seen.nobj + seen.completes, 0);

    fetch_range_outcome(sp, pub, "nope", 0, 0, 0, 1, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_DOES_NOT_EXIST);

    /* Largest Object {3, 1}. */
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 3, 2) == MOQ_OK);
    fetch_range_outcome(sp, pub, "video", 4, 0, 5, 0, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_INVALID_RANGE);
    fetch_range_outcome(sp, pub, "video", 3, 2, 3, 5, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_INVALID_RANGE);
    fetch_range_outcome(sp, pub, "video", 3, 0, 3, 2, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 0);
    MOQ_TEST_CHECK_EQ_INT(seen.oks, 1);
    MOQ_TEST_CHECK_EQ_INT(seen.nobj, 2);

    /* Objects were published and the cache no longer holds them: the start
     * is within the Largest, so the refusal is not INVALID_RANGE. */
    MOQ_TEST_CHECK(moq_pub_clear_retained_group(pub, track) == MOQ_OK);
    fetch_range_outcome(sp, pub, "video", 3, 0, 3, 2, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
    fetch_range_outcome(sp, pub, "video", 3, 2, 3, 5, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.errors, 1);
    MOQ_TEST_CHECK_EQ_U64(seen.err, MOQ_REQUEST_ERROR_INVALID_RANGE);

    hist_fetch_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_16 ? "fetch_range_published_d16"
                  : ver == MOQ_VERSION_DRAFT_18 ? "fetch_range_published_d18"
                  : "fetch_range_published_d22");
}

/* Draft-16: a whole-group snapshot staged while its response cannot be
 * accepted yet (action queue full) fixes the group's final Object at once:
 * Object 2 is refused while the response waits, and the response then
 * serves Objects 0..1. */
void test_hist_d16_fetch_staging(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    if (!hist_fetch_setup(MOQ_VERSION_DRAFT_16, &as, &alloc, &sp, 3u, &pub,
                          &track)) {
        MOQ_TEST_CHECK(false);
        hist_fetch_teardown(&as, sp, pub);
        return;
    }
    hist_fetch_seen_t seen;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 0, 2) == MOQ_OK);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    drain_all(sp);
    if (hist_fetch_request(sp, 0, 0, &fh)) {
        /* Fill the server's action queue with live delivery in another
         * group, each write completing. */
        moq_session_t *sv = moq_simpair_server(sp);
        for (uint64_t o = 0; o < 3 && moq_session_action_capacity(sv) >= 2; o++)
            MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc,
                                      sp, 5, o, 0x82), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_session_action_capacity(sv) < 2);
        MOQ_TEST_CHECK_EQ_INT((int)hist_fetch_stage(sp, pub),
                              (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc,
                                  sp, 0, 2, 0x83), (int)MOQ_ERR_WRONG_STATE);
        hist_fetch_settle(sp, pub, fh, &seen);
        MOQ_TEST_CHECK(seen.oks == 1 && seen.nobj == 2 && seen.completes == 1 &&
                       seen.errors == 0);
        MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc,
                                  sp, 0, 2, 0x84), (int)MOQ_ERR_WRONG_STATE);
    }
    hist_fetch_teardown(&as, sp, pub);
    MOQ_TEST_PASS("hist_d16_fetch_staging");
}

/* Internal omissions: a destination's stream that skips an Object assigned
 * to its subgroup -- one it owes, at or after its Start Location -- ends with
 * a RESET even when it carried the subgroup's last Object (draft-22 section
 * 11.3.2). Numbers never assigned are not omissions. Retained Objects 0..10
 * of group 30 are assigned before the live writes.
 *   case 0: Start {30,0}; live 0 then 10 (1..9 skipped)          -> RESET
 *   case 1: Start {30,0}; live 0..10                             -> FIN
 *   case 2: Start {30,5}; live 6..10 (5 skipped)                 -> RESET
 *   case 3: Start {30,5}; live 5..10                             -> FIN
 *   case 4: no retained Objects; Start {30,0}; live 0 then 5     -> FIN
 *           (1..4 were never assigned) */
static void hist_internal_omission_case(moq_version_t ver, int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    int ended = 0, fins = 0, resets = 0;
    if (c != 4)
        MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 30, 11) == MOQ_OK);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                      30, (c == 2 || c == 3) ? 5 : 0, 0);
    drain_all(sp);

    static const uint64_t live0[] = { 0, 10 };
    static const uint64_t live2[] = { 6, 7, 8, 9, 10 };
    static const uint64_t live3[] = { 5, 6, 7, 8, 9, 10 };
    static const uint64_t live4[] = { 0, 5 };
    uint64_t all[11];
    for (uint64_t i = 0; i < 11; i++) all[i] = i;
    const uint64_t *objs = c == 0 ? live0 : c == 1 ? all : c == 2 ? live2
                         : c == 3 ? live3 : live4;
    size_t n = c == 0 ? 2 : c == 1 ? 11 : c == 2 ? 5 : c == 3 ? 6 : 2;
    for (size_t i = 0; i < n; i++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, objs[i],
                                            HIST_TAG(objs[i])) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, objs[n - 1], NULL, NULL, &ended), 1);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 31, 0, 0x5F)
                   == MOQ_OK);
    hist_stream_ends(sp, 30, &fins, &resets);
    bool reset = (c == 0 || c == 2);
    MOQ_TEST_CHECK_EQ_INT(fins, reset ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(resets, reset ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

void test_hist_internal_omission(moq_version_t ver)
{
    for (int c = 0; c < 5; c++) hist_internal_omission_case(ver, c);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_internal_omission_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_internal_omission_d18"
                  : "hist_internal_omission_d16");
}

/* Evidence recorded when a group's record is displaced: retained Objects
 * 0..10 of group 30 are assigned before a draft-22 destination with window
 * {30,0}..{30,end} opens the subgroup; live Objects follow, then seventeen
 * later groups displace group 30's record before end_group closes the
 * stream.
 *   end 1, live 0 only: Objects 1..10 owed and never sent, and no op after
 *        the opening recorded it -- only the displacement can: RESET.
 *   end 10, live 0..10: complete: FIN. */
static void hist_retained_tail_case(bool complete)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(MOQ_VERSION_DRAFT_22);
    int ended = 0, fins = 0, resets = 0;
    uint64_t now = moq_simpair_now_us(sp);
    MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 30, 11) == MOQ_OK);
    hist_subscribe_object_end(sp, pub, 30, 0, 30, complete ? 10 : 1);
    uint64_t last = complete ? 10 : 0;
    for (uint64_t o = 0; o <= last; o++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, o,
                                            HIST_TAG(o)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, last, NULL, NULL, &ended), 1);
    for (uint64_t g = 31; g <= 47; g++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, g, 0,
                                            0x71) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    hist_stream_ends(sp, 30, &fins, &resets);
    MOQ_TEST_CHECK_EQ_INT(fins, complete ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(resets, complete ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

void test_hist_retained_tail_displaced(void)
{
    hist_retained_tail_case(false);
    hist_retained_tail_case(true);
    MOQ_TEST_PASS("hist_retained_tail_displaced_d22");
}

/* Generation identity holds on every write: Objects 0..4 of group 30 are
 * admitted with no destination, the group is reset, Object 5 opens Subgroup
 * 1; a fresh destination then subscribes from {30,0}. Object 1 (its original
 * bytes) belongs to Subgroup 0, so writing it again is refused before any
 * effect -- even though Subgroup 1 is open and this is not its first write
 * -- and nothing reaches the destination; Object 6 continues Subgroup 1. */
void test_hist_generation_identity(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    int ended = 0;
    uint64_t sgid = UINT64_MAX;
    uint64_t now = moq_simpair_now_us(sp);
    for (uint64_t o = 0; o <= 4; o++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, o,
                                            HIST_TAG(o)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_reset_group(pub, track, 0x10, now) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 5,
                                        HIST_TAG(5)) == MOQ_OK);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 30, 0, 0);
    drain_all(sp);

    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              30, 1, HIST_TAG(1)), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, 1, NULL, NULL, &ended), 0);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 6,
                                        HIST_TAG(6)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, 6, NULL, &sgid, &ended), 1);
    MOQ_TEST_CHECK_EQ_U64(sgid, 1);
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    hist_rig_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_generation_identity_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_generation_identity_d18"
                  : "hist_generation_identity_d16");
}

/* Sparse membership. Objects assigned with no destination open; a
 * destination subscribing later from Start {30,5} must not see its stream
 * end with a FIN when an assigned Object at or after 5 was never sent to it
 * (draft-22 section 11.3.2), and must see a FIN when none exists.
 *   case 0: live 0, 5, 10; then the replay of 10 -> RESET (5 omitted)
 *   case 1: live 0, 10; then the replay of 10    -> FIN (5 never assigned)
 *   case 2: retained 0..10, live 20; then 6..20  -> RESET (5 omitted)
 *   case 3: retained 0..10, live 20; then 5..20  -> FIN
 * Exact assignment membership in the group history decides them. */
static void hist_sparse_membership_case(moq_version_t ver, int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    int ended = 0, fins = 0, resets = 0;
    uint64_t now = moq_simpair_now_us(sp);
    if (c <= 1) {
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 0,
                                            HIST_TAG(0)) == MOQ_OK);
        if (c == 0)
            MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 5,
                                                HIST_TAG(5)) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 10,
                                            HIST_TAG(10)) == MOQ_OK);
    } else {
        MOQ_TEST_CHECK(hist_install(pub, track, &alloc, 30, 11) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 20,
                                            HIST_TAG(20)) == MOQ_OK);
    }
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 30, 5, 0);
    drain_all(sp);
    if (c <= 1) {
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 10,
                                            HIST_TAG(10)) == MOQ_OK);
    } else {
        for (uint64_t o = (c == 2 ? 6 : 5); o <= 20; o++)
            MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, o,
                                                HIST_TAG(o)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_pub_end_group(pub, track, now) == MOQ_OK);
    hist_stream_ends(sp, 30, &fins, &resets);
    bool reset = (c == 0 || c == 2);
    MOQ_TEST_CHECK_EQ_INT(fins, reset ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(resets, reset ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(ended, 0);
    hist_rig_teardown(&as, sp, pub);
}

/* Delivery-mode identity needs exact datagram membership: an Object sent as
 * a datagram is never assigned to a subgroup (draft-22 section 2.1).
 *   case 0: datagrams 0, 10, 5; stream Object 5 -> refused
 *   case 1: datagrams 0, 10;    stream Object 5 -> admitted (never assigned)
 *   case 2: datagrams 0, 10, 5; stream Object 10 -> refused */
static void hist_datagram_identity_case(moq_version_t ver, int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 30, 0, HIST_TAG(0))
                   == MOQ_OK);
    MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 30, 10, HIST_TAG(10))
                   == MOQ_OK);
    if (c != 1)
        MOQ_TEST_CHECK(windows_write_dg(pub, track, &alloc, sp, 30, 5,
                                        HIST_TAG(5)) == MOQ_OK);
    uint64_t o = c == 2 ? 10 : 5;
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp, 30,
                              o, HIST_TAG(o)),
                          c == 1 ? (int)MOQ_OK : (int)MOQ_ERR_WRONG_STATE);
    hist_rig_teardown(&as, sp, pub);
}

void test_hist_sparse_membership(moq_version_t ver)
{
    for (int c = 0; c < 3; c++) hist_datagram_identity_case(ver, c);
    for (int c = 0; c < 4; c++) hist_sparse_membership_case(ver, c);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_sparse_membership_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_sparse_membership_d18"
                  : "hist_sparse_membership_d16");
}

/* Membership capacity: a group keeps at most 16 ranges of consecutive
 * Objects of one owner (a subgroup, or the datagram mode). With no
 * destination, stream Objects 0, 2, ..., 30 of group 30 fill the 16 ranges.
 *   - Object 32 would need a 17th range: MOQ_ERR_NOMEM, and nothing is
 *     assigned (32 is later taken as a datagram);
 *   - Object 1 joins the ranges of 0 and 2 (15 ranges), so datagram 32 now
 *     fits (16 ranges, a datagram range);
 *   - at 16 ranges: stream Object 34 is refused (NOMEM); replaying stream
 *     Object 4, extending stream range 30 with 31, and extending datagram
 *     range 32 with datagram 33 are all admitted;
 *   - ranges of different owners never merge: stream Object 33 is a
 *     datagram's (WRONG_STATE), and datagram 28 is a stream Object's. */
void test_hist_membership_capacity(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
#define HM(o) ((int)declare_write_stream(pub, track, &alloc, sp, 30, (o), HIST_TAG(o)))
#define HD(o) ((int)windows_write_dg(pub, track, &alloc, sp, 30, (o), HIST_TAG(o)))
    for (uint64_t o = 0; o <= 30; o += 2)
        MOQ_TEST_CHECK_EQ_INT(HM(o), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(HM(32), (int)MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_INT(HM(1), (int)MOQ_OK);           /* joins 0 and 2 */
    MOQ_TEST_CHECK_EQ_INT(HD(32), (int)MOQ_OK);          /* 32 was free */
    MOQ_TEST_CHECK_EQ_INT(HM(34), (int)MOQ_ERR_NOMEM);   /* 17th range */
    MOQ_TEST_CHECK_EQ_INT(HM(4), (int)MOQ_OK);           /* replay */
    MOQ_TEST_CHECK_EQ_INT(HM(31), (int)MOQ_OK);          /* extension */
    MOQ_TEST_CHECK_EQ_INT(HD(33), (int)MOQ_OK);          /* datagram extension */
    MOQ_TEST_CHECK_EQ_INT(HM(33), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(HD(28), (int)MOQ_ERR_WRONG_STATE);
#undef HM
#undef HD
    hist_rig_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_membership_capacity_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_membership_capacity_d18"
                  : "hist_membership_capacity_d16");
}

/* After a FIN, reinstalling Objects that are already the subgroup's is
 * allowed (it assigns nothing new); extending the subgroup is not. Group 40:
 * live 0..2, then the track moves to group 41 (the subgroup ends with a
 * FIN). */
void test_hist_retained_subset_after_fin(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    for (uint64_t o = 0; o <= 2; o++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 40, o,
                                            HIST_TAG(o)) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 41, 0,
                                        HIST_TAG(0)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 40, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 40, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_install(pub, track, &alloc, 40, 4),
                          (int)MOQ_ERR_WRONG_STATE);
    hist_rig_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_retained_subset_after_fin_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_retained_subset_after_fin_d18"
                  : "hist_retained_subset_after_fin_d16");
}

/* After one destination ends the open subgroup with a FIN (its final Object
 * is fixed for that receiver, draft-22 section 12.1), the group goes on:
 *   case 0: live 0, 1; FIN; a new destination; live 2 (above the group's
 *           maximum) opens Subgroup 1, which the stream begins (FIRST_OBJECT
 *           on drafts 18 and 22);
 *   case 1: live 0, 2; FIN; live 1 (unassigned, below the maximum) would
 *           extend the ended subgroup: refused before any effect;
 *   case 2: live 0, 1 with END_OF_GROUP; FIN; live 2 would follow the
 *           group's largest Object: refused. */
static void hist_fin_rollover_case(moq_version_t ver, int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    const bool has_bit = ver != MOQ_VERSION_DRAFT_16;
    int ended = 0, fins = 0, resets = 0;
    bool f = false;
    uint64_t sgid = UINT64_MAX;
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 30, 0, 0);
    drain_all(sp);
    uint64_t second = c == 1 ? 2 : 1;
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 30, 0, c == 2,
                                         HIST_TAG(0)) == MOQ_OK);
    MOQ_TEST_CHECK(mono_write_stream_eog(pub, track, &alloc, sp, 30, second,
                                         c == 2, HIST_TAG(second)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
                       MOQ_PUB_DONE_TRACK_ENDED, moq_simpair_now_us(sp)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 16, NULL) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(pub, track,
                       MOQ_PUB_DONE_TRACK_ENDED, moq_simpair_now_us(sp)) == MOQ_OK);
    hist_stream_ends(sp, 30, &fins, &resets);
    MOQ_TEST_CHECK_EQ_INT(fins, 1);
    MOQ_TEST_CHECK_EQ_INT(resets, 0);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 30, 0, 0);
    drain_all(sp);
    if (c == 0) {
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 30, 2,
                                            HIST_TAG(2)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, 2, &f, &sgid, &ended), 1);
        MOQ_TEST_CHECK_EQ_U64(sgid, 1);
        MOQ_TEST_CHECK(f == has_bit);
    } else {
        uint64_t o = c == 1 ? 1 : 2;
        MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                                  30, o, HIST_TAG(o)), (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 30, o, NULL, NULL, &ended), 0);
    }
    hist_rig_teardown(&as, sp, pub);
}

void test_hist_fin_rollover(moq_version_t ver)
{
    for (int c = 0; c < 3; c++) hist_fin_rollover_case(ver, c);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_fin_rollover_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_fin_rollover_d18"
                  : "hist_fin_rollover_d16");
}

/* Datagram pressure: groups 1..15 are revisited (15 pinned records) and the
 * track then writes group 16, which holds the only other record. A datagram
 * in new group 100 compacts group 16 into the sealed runs instead of being
 * refused; the destination's open stream of group 16, complete, still ends
 * with a FIN when the track next moves; group 16 then refuses Objects, and a
 * new stream group displaces group 100's record as usual. */
void test_hist_datagram_compacts_current(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    HIST_RIG_OR_RETURN(ver);
    int ended = 0, fins = 0, resets = 0;
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    drain_all(sp);
    /* Each write is delivered before the next, so the pair's action and
     * event queues never fill. */
    for (int pass = 0; pass < 2; pass++)
        for (uint64_t g = 1; g <= 15; g++) {
            MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, g,
                               (uint64_t)pass, HIST_TAG(pass)) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 32, NULL);
            moq_event_t ev;
            while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
                MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_UNSUBSCRIBED &&
                               ev.kind != MOQ_EVENT_SUBSCRIBE_DONE);
                moq_event_cleanup(&ev);
            }
        }
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 16, 0,
                                        HIST_TAG(0)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 16, 0, NULL, NULL, &ended), 1);
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 100, 0,
                              HIST_TAG(0)), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp, 16,
                              1, HIST_TAG(1)), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 17, 0,
                                        HIST_TAG(0)) == MOQ_OK);
    hist_stream_ends(sp, 16, &fins, &resets);
    MOQ_TEST_CHECK_EQ_INT(fins, 1);
    MOQ_TEST_CHECK_EQ_INT(resets, 0);
    MOQ_TEST_CHECK_EQ_INT(ended, 0);
    hist_rig_teardown(&as, sp, pub);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_datagram_compacts_current_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_datagram_compacts_current_d18"
                  : "hist_datagram_compacts_current_d16");
}

/* Drive the pair to quiescence and drop the client's events, checking the
 * subscription never ends. */
static void hist_pump_discard(moq_simpair_t *sp)
{
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_UNSUBSCRIBED &&
                       ev.kind != MOQ_EVENT_SUBSCRIBE_DONE);
        moq_event_cleanup(&ev);
    }
}

/* What the client saw for group 16: Object 0 or 1 (count, Subgroup ID and
 * whether its payload is exactly 11 22 33 44), and the stream terminals. */
typedef struct {
    int      objects, fins, resets, ended;
    uint64_t sgid;
    bool     exact;
} hist_cs_seen_t;

static void hist_cs_observe(moq_simpair_t *sp, hist_cs_seen_t *o)
{
    static const uint8_t want[4] = { 0x11, 0x22, 0x33, 0x44 };
    for (int round = 0; round < 4; round++) {
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.group_id == 16) {
                o->objects++;
                o->sgid = ev.u.object_received.subgroup_id;
                moq_rcbuf_t *pl = ev.u.object_received.payload;
                o->exact = pl && moq_rcbuf_len(pl) == sizeof(want) &&
                           memcmp(moq_rcbuf_data(pl), want, sizeof(want)) == 0;
            }
            if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED &&
                ev.u.subgroup_finished.group_id == 16) o->fins++;
            if (ev.kind == MOQ_EVENT_SUBGROUP_RESET &&
                ev.u.subgroup_reset.group_id == 16) o->resets++;
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED ||
                ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) o->ended++;
            moq_event_cleanup(&ev);
        }
    }
}

static moq_result_t hist_cs_chunk(moq_publisher_t *pub, moq_pub_track_t *t,
                                  moq_alloc_t *alloc, moq_simpair_t *sp,
                                  uint8_t a, uint8_t b)
{
    uint8_t d[2] = { a, b };
    moq_rcbuf_t *c = NULL;
    moq_result_t rc = moq_rcbuf_create(alloc, d, sizeof(d), &c);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) return rc;
    rc = moq_pub_write_data(pub, t, c, moq_simpair_now_us(sp));
    moq_rcbuf_decref(c);
    return rc;
}

/* A streaming Object in flight when its group is compacted. Groups 1..15 are
 * revisited (15 pinned records) and group 16 holds the only other record.
 * Object `obj` of group 16 is begun (4 bytes) and its first 2 bytes sent;
 * a datagram in new group 100 then compacts group 16 into the sealed runs.
 * The Object still finishes on the stream it began on -- same Subgroup ID,
 * exact bytes -- with no terminal before moq_pub_end_group, which closes
 * the stream although group 16 admits nothing more.
 *   case 0: the destination carried every Object of the subgroup: one FIN.
 *   case 1: the destination subscribed after Object 0 and owes it (its
 *           window starts at {16,0}), so its stream omits an owed Object:
 *           one RESET, decided before the record went away.
 *   cases 2, 3: as 0 and 1, with the server's action queue full when
 *           moq_pub_end_group runs: it returns WOULD_BLOCK (the record of
 *           group 16 is already reused by group 100), and the retry after
 *           the queue drains gives exactly one terminal, with the same
 *           verdict. */
static void hist_compact_stream_case(moq_version_t ver, int c)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    const bool blocked = c >= 2;
    const bool omitted = c == 1 || c == 3;
    if (!hist_fetch_setup(ver, &as, &alloc, &sp, blocked ? 4u : 0u, &pub,
                          &track)) {
        MOQ_TEST_CHECK(false);
        hist_fetch_teardown(&as, sp, pub);
        return;
    }
    hist_cs_seen_t seen;
    memset(&seen, 0, sizeof(seen));
    uint64_t now = moq_simpair_now_us(sp);
    if (!omitted) {
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
        drain_all(sp);
    }
    for (int pass = 0; pass < 2; pass++)
        for (uint64_t g = 1; g <= 15; g++) {
            MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, g,
                               (uint64_t)pass, HIST_TAG(pass)) == MOQ_OK);
            hist_pump_discard(sp);
        }
    uint64_t obj = 0;
    if (omitted) {
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 16, 0,
                                            HIST_TAG(0)) == MOQ_OK);
        windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                          16, 0, 0);
        drain_all(sp);
        obj = 1;
    }
    moq_pub_begin_object_cfg_t bc;
    moq_pub_begin_object_cfg_init_sized(&bc, sizeof(bc));
    bc.group_id = 16; bc.object_id = obj; bc.payload_length = 4;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_begin_object(pub, track, &bc, now),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)hist_cs_chunk(pub, track, &alloc, sp, 0x11, 0x22),
                          (int)MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    /* The compacting datagram (record pressure: 15 pinned + group 16). */
    MOQ_TEST_CHECK_EQ_INT((int)windows_write_dg(pub, track, &alloc, sp, 100, 0,
                              HIST_TAG(0)), (int)MOQ_OK);
    /* The Object in flight finishes on its stream. */
    MOQ_TEST_CHECK_EQ_INT((int)hist_cs_chunk(pub, track, &alloc, sp, 0x33, 0x44),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_end_object(pub, track, now), (int)MOQ_OK);
    hist_cs_observe(sp, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 1);
    MOQ_TEST_CHECK(seen.exact);
    MOQ_TEST_CHECK_EQ_U64(seen.sgid, 0);
    MOQ_TEST_CHECK_EQ_INT(seen.fins + seen.resets, 0);   /* no terminal yet */

    /* Group 16 admits nothing more, on any path. */
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp, 16,
                              5, HIST_TAG(5)), (int)MOQ_ERR_WRONG_STATE);
    bc.object_id = 6;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_begin_object(pub, track, &bc, now),
                          (int)MOQ_ERR_WRONG_STATE);

    if (blocked) {
        /* Fill the server's action queue with completed datagrams of group
         * 100, then close: the terminal cannot be queued yet. */
        moq_session_t *sv = moq_simpair_server(sp);
        for (uint64_t o = 1; o <= 8 && moq_session_action_capacity(sv) > 0; o++) {
            moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
            uint8_t d[2] = { HIST_TAG(o), (uint8_t)(HIST_TAG(o) ^ 0xFFu) };
            moq_rcbuf_t *pl = NULL;
            MOQ_TEST_CHECK(moq_rcbuf_create(&alloc, d, sizeof(d), &pl) == MOQ_OK);
            oc.group_id = 100; oc.object_id = o; oc.payload = pl;
            oc.datagram = true;
            MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_object_ex(pub, track, &oc,
                                      now), (int)MOQ_OK);
            moq_rcbuf_decref(pl);
        }
        MOQ_TEST_CHECK_EQ_U64(moq_session_action_capacity(sv), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_end_group(pub, track, now),
                              (int)MOQ_ERR_WOULD_BLOCK);
        hist_cs_observe(sp, &seen);
        MOQ_TEST_CHECK_EQ_INT(seen.fins + seen.resets, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_end_group(pub, track, now), (int)MOQ_OK);
    hist_cs_observe(sp, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.fins, omitted ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(seen.resets, omitted ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(seen.ended, 0);
    /* A second close changes nothing. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_end_group(pub, track, now), (int)MOQ_OK);
    hist_cs_observe(sp, &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.fins + seen.resets, 1);
    hist_fetch_teardown(&as, sp, pub);
}

void test_hist_compact_stream(moq_version_t ver)
{
    for (int c = 0; c < 4; c++) hist_compact_stream_case(ver, c);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "hist_compact_stream_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "hist_compact_stream_d18"
                  : "hist_compact_stream_d16");
}
