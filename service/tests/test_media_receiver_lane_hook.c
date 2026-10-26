/* Receiver ownership through the real pump hook (white-box), on drafts 16
 * and 18, over a SimPair whose server session is driven directly, with the
 * endpoint wake and the facade unsubscribe observed.
 *
 * Pinned:
 * - a track removed from the catalog keeps its facade mapping until the
 *   stream terminals queued for it have reached the lane: a FIN or RESET
 *   queued behind the removal is a record naming the removed handle; a
 *   refused unsubscribe is retried; the slot is released once nothing
 *   names it, and a re-added track is a new handle with its own records;
 * - a receiver whose catalog failed discards the facade's stream terminals
 *   and Group ends with its media, so its subscriptions release in bounded
 *   passes; (MPEG2TS) it discards what its reorder windows hold or have
 *   parked, returning their lane slots, and the records already on the lane
 *   that waited for them become pollable, in order, with a wake;
 * - (MPEG2TS) a status Object parked behind a release that waits for room
 *   is never replaced by a later one: the pass after the application's
 *   polls resumes the window before it takes in more, so both statuses
 *   arrive once, in order, with every reserved slot returned, and a
 *   stalled window reports no deadline.
 */
#include <moq/media_receiver.h>
#include <moq/subscriber.h>
static unsigned g_wakes, g_refuse_unsubscribe, g_unsubscribes;
static void observed_wake(moq_endpoint_t *ep) { (void)ep; g_wakes++; }
static moq_result_t observed_unsubscribe(moq_subscriber_t *s, moq_sub_track_t *t,
                                         uint64_t now_us);
#define moq_endpoint_wake observed_wake
#define moq_sub_unsubscribe observed_unsubscribe
#define main receiver_lane_hook_companion_main
#include "test_media_receiver_companion.c"
#undef main
#undef moq_endpoint_wake
#undef moq_sub_unsubscribe
#ifdef MOQ_TEST_HAVE_TS
#include "ts_stream.h"
#endif

static moq_result_t observed_unsubscribe(moq_subscriber_t *s, moq_sub_track_t *t,
                                         uint64_t now_us)
{
    g_unsubscribes++;
    if (g_refuse_unsubscribe) {
        g_refuse_unsubscribe--;
        return MOQ_ERR_WOULD_BLOCK;
    }
    return moq_sub_unsubscribe(s, t, now_us);
}

#define CHECK_(x) do { if (!(x)) { fprintf(stderr, "FAIL: %s:%d: %s\n", \
    __FILE__, __LINE__, #x); failures++; } } while (0)
#define NEED(x) do { if (!(x)) { fprintf(stderr, "FAIL: %s:%d: %s\n", \
    __FILE__, __LINE__, #x); failures++; goto done; } } while (0)
#define WINDOW_US 1000u

static const char cat_a[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const char cat_z[] = "{\"version\":1,\"generatedAt\":2,\"tracks\":["
    "{\"name\":\"z\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const char cat_a_again[] = "{\"version\":1,\"generatedAt\":3,\"tracks\":["
    "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
#ifdef MOQ_TEST_HAVE_TS
static const char cat_ts[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"t\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1},"
    "{\"name\":\"l\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
#endif

typedef struct lh {
    test_alloc_state_t state;
    moq_alloc_t alloc;
    moq_media_receiver_t *r;
    moq_simpair_t *pair;
    /* Indexed by the track name's first octet. */
    moq_media_track_t *track[128];
    moq_subscription_t peer[128];
} lh_t;

static uint64_t lh_now(lh_t *f) { return moq_simpair_now_us(f->pair); }
static moq_session_t *lh_server(lh_t *f) { return moq_simpair_server(f->pair); }

/* The server accepts every SUBSCRIBE and records each media one. */
static bool lh_network(lh_t *f)
{
    for (unsigned round = 0; round < 3; round++) {
        if (moq_simpair_run_until_quiescent(f->pair, 64, NULL) != MOQ_OK) return false;
        for (unsigned n = 0; n < 64; n++) {
            moq_event_t ev;
            if (moq_session_poll_events(lh_server(f), &ev, 1) == 0) break;
            moq_result_t rc = MOQ_OK;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_bytes_t name = ev.u.subscribe_request.track_name;
                if (name.len == 1) f->peer[name.data[0] & 0x7f] = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                rc = moq_session_accept_subscribe(lh_server(f),
                    ev.u.subscribe_request.sub, &ac, lh_now(f));
            }
            moq_event_cleanup(&ev);
            if (rc != MOQ_OK) return false;
        }
    }
    return true;
}

static void lh_pump(lh_t *f)
{
    receiver_hook(NULL, moq_simpair_client(f->pair), lh_now(f), f->r);
}

static void lh_events(lh_t *f, unsigned *removed)
{
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc->name.len == 1)
            f->track[ev.desc->name.data[0] & 0x7f] = ev.track;
        if (ev.kind == MOQ_MEDIA_TRACK_REMOVED && removed) (*removed)++;
    }
}

/* Subscribe the receiver's track `name` and let the server accept it. */
static bool lh_subscribe(lh_t *f, char name)
{
    moq_media_track_t *t = f->track[(unsigned char)name];
    if (!t) return false;
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                           .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t options;
    moq_media_receiver_track_subscribe_cfg_init_sized(&options, sizeof(options));
    options.location = &location;
    f->peer[(unsigned char)name] = (moq_subscription_t){0};
    if (moq_media_receiver_subscribe_track(f->r, t, &options) != MOQ_OK) return false;
    receiver_reconcile_subscriptions(f->r, lh_now(f));
    if (!t->sub_track || !lh_network(f)) return false;
    lh_pump(f);
    return moq_sub_track_is_active(t->sub_track);
}

/* A DROP_GROUP receiver of `max_objects`, a lane of `lane` records, on a
 * legacy session, with `catalog` and every track in it subscribed. */
static bool lh_start(lh_t *f, moq_version_t ver, const char *catalog,
                     uint32_t max_objects, uint32_t lane)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->state);
    moq_media_receiver_cfg_t cfg;
    init_cfg(&cfg, ver == MOQ_VERSION_DRAFT_22
                       ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
                       : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
    cfg.auto_subscribe = false;
    cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    cfg.overflow.max_objects = max_objects;
    cfg.overflow.max_bytes = 1u << 20;
    cfg.surface_stream_terminals = true;
    cfg.max_stream_terminals = lane;
    cfg.mpeg2ts_reorder_window_us = WINDOW_US;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!f->r) return false;
    f->r->transport_version = ver;
    moq_media_receiver_test_ingest(f->r, 0, 0, catalog, strlen(catalog));
    lh_events(f, NULL);
    f->pair = pair(&f->alloc, ver);
    if (!f->pair) return false;
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
    sc.callbacks.ctx = f->r;
    sc.callbacks.on_subscribed = receiver_on_subscribed;
    sc.callbacks.on_subscribe_error = receiver_on_subscribe_error;
    sc.on_subscribe_done = receiver_on_subscribe_done;
    sc.on_update_ok = receiver_on_update_ok;
    sc.surface_stream_terminals = true;
    sc.surface_group_ends = true;
    if (moq_sub_create(moq_simpair_client(f->pair), &f->alloc, &sc, &f->r->sub) != MOQ_OK)
        return false;
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = f->r->namespace_;
    tc.track_name = MOQ_BYTES_LITERAL("catalog");
    if (moq_sub_subscribe(f->r->sub, &tc, lh_now(f), &f->r->catalog_sub) != MOQ_OK)
        return false;
    for (unsigned c = 'a'; c <= 'z'; c++)
        if (f->track[c] && !lh_subscribe(f, (char)c)) return false;
    return true;
}

static void lh_finish(lh_t *f)
{
    if (f->r)
        moq_media_receiver_test_destroy_with_session(f->r,
            f->pair ? moq_simpair_client(f->pair) : NULL, f->pair ? lh_now(f) : 0);
    if (f->pair) moq_simpair_destroy(f->pair);
    CHECK_(f->state.balance == 0);
}

/* The server opens a subgroup of `name`'s group `g`, one that ends its
 * group with `eog`. */
static bool lh_open_eog(lh_t *f, char name, uint64_t g, bool eog,
                        moq_subgroup_handle_t *sg)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = g;
    sc.end_of_group = eog;
    return moq_session_open_subgroup(lh_server(f), f->peer[(unsigned char)name], &sc,
                                     lh_now(f), sg) == MOQ_OK;
}

static bool lh_open(lh_t *f, char name, uint64_t g, moq_subgroup_handle_t *sg)
{
    return lh_open_eog(f, name, g, false, sg);
}

/* Fail the catalog, as a refused acknowledgement does. */
static void lh_fail_catalog(lh_t *f)
{
    f->r->catalog_ack_failed = true;
    receiver_set_fatal(f->r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
}

static bool lh_write(lh_t *f, moq_subgroup_handle_t sg, uint64_t o,
                     const uint8_t *data, size_t len)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&f->alloc, data, len, &payload) != MOQ_OK) return false;
    moq_result_t rc = moq_session_write_object(lh_server(f), sg, o, payload, lh_now(f));
    moq_rcbuf_decref(payload);
    return rc == MOQ_OK;
}

static const uint8_t lh_body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };

static unsigned lh_drain_objects(lh_t *f)
{
    unsigned n = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)) == MOQ_OK) {
        moq_media_object_cleanup(&obj);
        n++;
    }
    return n;
}

/* Group 0 of track a ends (FIN, or RESET with `reset`), and the facade
 * still holds that terminal when a catalog update removes the track. The
 * terminal is a lane record naming the removed handle. Two refused
 * unsubscribes are retried; once nothing names the slot it is released.
 * The re-added track is a new handle whose records are its own. */
static void test_removal_keeps_terminal(moq_version_t ver, bool reset)
{
    int before = failures;
    lh_t f;
    unsigned removed = 0;
    NEED(lh_start(&f, ver, cat_a, 8, 8));
    moq_media_track_t *old = f.track['a'];
    moq_subgroup_handle_t sg;
    NEED(lh_open(&f, 'a', 0, &sg));
    NEED(lh_write(&f, sg, 0, lh_body, sizeof(lh_body)));
    NEED(reset ? moq_session_reset_subgroup(lh_server(&f), sg, 0x21, lh_now(&f)) == MOQ_OK
               : moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));

    g_refuse_unsubscribe = 2;
    unsigned unsubscribes = g_unsubscribes;
    moq_media_receiver_test_ingest(f.r, 1, 0, cat_z, strlen(cat_z));
    lh_events(&f, &removed);
    CHECK_(removed == 1);
    CHECK_(old->sub_track != NULL);
    for (unsigned i = 0; i < 4; i++) {
        lh_pump(&f);
        NEED(lh_network(&f));
    }
    CHECK_(g_refuse_unsubscribe == 0);
    CHECK_(g_unsubscribes - unsubscribes >= 3);
    CHECK_(lh_drain_objects(&f) == 0);
    moq_media_stream_terminal_t st;
    NEED(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK_(st.track == old && st.group_id == 0);
    CHECK_(st.kind == (reset ? MOQ_MEDIA_STREAM_RESET : MOQ_MEDIA_STREAM_FIN));
    CHECK_(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_DONE);
    lh_pump(&f);
    CHECK_(old->sub_track == NULL);

    moq_media_receiver_test_ingest(f.r, 2, 0, cat_a_again, strlen(cat_a_again));
    lh_events(&f, &removed);
    NEED(f.track['a'] != NULL && f.track['a'] != old);
    NEED(lh_subscribe(&f, 'a'));
    NEED(lh_open(&f, 'a', 4, &sg));
    NEED(lh_write(&f, sg, 0, lh_body, sizeof(lh_body)));
    NEED(moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));
    lh_pump(&f);
    moq_media_object_t obj;
    NEED(moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) == MOQ_OK);
    CHECK_(obj.track == f.track['a'] && obj.group_id == 4);
    moq_media_object_cleanup(&obj);
    NEED(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK_(st.track == f.track['a'] && st.group_id == 4);
done:
    lh_finish(&f);
    if (failures == before)
        printf("PASS: lane_hook_removal_keeps_%s_d%u\n", reset ? "reset" : "fin",
               ver == MOQ_VERSION_DRAFT_16 ? 16u : 18u);
}

/* The catalog fails while the facade still holds a media Object and its
 * stream's FIN. The failure cleanup discards both, so the subscription is
 * released within a few passes. */
static void test_failed_catalog_releases(moq_version_t ver)
{
    int before = failures;
    lh_t f;
    NEED(lh_start(&f, ver, cat_a, 8, 8));
    moq_media_track_t *t = f.track['a'];
    moq_subgroup_handle_t sg;
    NEED(lh_open(&f, 'a', 0, &sg));
    NEED(lh_write(&f, sg, 0, lh_body, sizeof(lh_body)));
    NEED(moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));
    lh_fail_catalog(&f);
    for (unsigned i = 0; i < 4 && t->sub_track; i++) {
        lh_pump(&f);
        NEED(lh_network(&f));
    }
    CHECK_(t->sub_track == NULL);
    CHECK_(f.r->catalog_sub == NULL);
done:
    lh_finish(&f);
    if (failures == before)
        printf("PASS: lane_hook_failed_catalog_releases_d%u\n",
               ver == MOQ_VERSION_DRAFT_16 ? 16u : 18u);
}

#ifdef MOQ_TEST_HAVE_TS
static ts_gen_t g_ts;

static bool lh_write_ts(lh_t *f, moq_subgroup_handle_t sg, uint64_t o)
{
    uint8_t buf[8 * 188];
    size_t n = ts_gen_gop(&g_ts, buf, 1);
    return lh_write(f, sg, o, buf, n);
}

static bool lh_poll_is(lh_t *f, moq_media_track_t *t, uint64_t g, uint64_t o,
                       moq_object_status_t status)
{
    moq_media_object_t obj;
    if (moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)) != MOQ_OK) return false;
    bool ok = obj.track == t && obj.group_id == g && obj.object_id == o &&
              obj.status == status;
    if (!ok)
        fprintf(stderr, "  polled %llu/%llu status %d\n", (unsigned long long)obj.group_id,
                (unsigned long long)obj.object_id, (int)obj.status);
    moq_media_object_cleanup(&obj);
    return ok;
}

/* TS 0/1 and 0/2 wait for 0/0, which never comes; LOC groups 5 to 7 fill
 * a three-Object queue on a four-record lane. Group 0's END_OF_GROUP status
 * makes both due: 0/1 goes out by evicting group 5, 0/2 waits for room,
 * and the status waits behind it, with no deadline. Group 1's status then
 * reaches the facade. The application's polls wake the network thread, and
 * its next pass, with no network input, resumes the window first and then
 * takes in group 1's status: 0/2 and both statuses arrive once, in order,
 * and every reserved slot returns. */
static void test_parked_status_through_hook(moq_version_t ver)
{
    int before = failures;
    lh_t f;
    ts_gen_init(&g_ts, 188, 1, false);
    NEED(lh_start(&f, ver, cat_ts, 3, 4));
    moq_media_track_t *ts = f.track['t'], *loc = f.track['l'];
    moq_subgroup_handle_t g0, g1, l;
    NEED(lh_open(&f, 't', 0, &g0));
    NEED(lh_write_ts(&f, g0, 1));
    NEED(lh_write_ts(&f, g0, 2));
    for (uint64_t g = 5; g <= 7; g++) {
        NEED(lh_open(&f, 'l', g, &l));
        NEED(lh_write(&f, l, 0, lh_body, sizeof(lh_body)));
    }
    NEED(lh_network(&f));
    lh_pump(&f);
    NEED(f.r->obj_tail - f.r->obj_head == 3);

    NEED(moq_simpair_advance_to(f.pair, lh_now(&f) + 2 * WINDOW_US) == MOQ_OK);
    NEED(moq_session_write_status_object(lh_server(&f), g0, 3, MOQ_OBJECT_END_OF_GROUP,
                                         lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));
    lh_pump(&f);
    CHECK_(f.r->ts_stalled == 1);
    CHECK_(receiver_next_deadline_us(f.r) == UINT64_MAX);
    NEED(lh_open(&f, 't', 1, &g1));
    NEED(moq_session_write_status_object(lh_server(&f), g1, 0, MOQ_OBJECT_END_OF_GROUP,
                                         lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));

    unsigned wakes = g_wakes;
    CHECK_(lh_poll_is(&f, loc, 6, 0, MOQ_OBJECT_NORMAL));
    CHECK_(lh_poll_is(&f, loc, 7, 0, MOQ_OBJECT_NORMAL));
    CHECK_(lh_poll_is(&f, ts, 0, 1, MOQ_OBJECT_NORMAL));
    CHECK_(g_wakes > wakes);
    lh_pump(&f);
    CHECK_(f.r->ts_stalled == 0);
    CHECK_(lh_poll_is(&f, ts, 0, 2, MOQ_OBJECT_NORMAL));
    CHECK_(lh_poll_is(&f, ts, 0, 3, MOQ_OBJECT_END_OF_GROUP));
    CHECK_(lh_poll_is(&f, ts, 1, 0, MOQ_OBJECT_END_OF_GROUP));
    CHECK_(lh_drain_objects(&f) == 0);
    CHECK_(f.r->st_reserved == 0);
done:
    lh_finish(&f);
    if (failures == before)
        printf("PASS: lane_hook_parked_status_d%u\n",
               ver == MOQ_VERSION_DRAFT_16 ? 16u : 18u);
}
#endif

#ifdef MOQ_TEST_HAVE_TS
/* TS 0/1 waits for 0/0; its stream's FIN is already on the lane, unready,
 * with the LOC track's FIN behind it. The catalog fails, and a further TS
 * group arrives with its Group end. The failed receiver discards the held
 * Object and returns its slot; the TS FIN becomes pollable, with a wake,
 * then the LOC FIN after its Object; the facade's Group ends are settled
 * without being applied, and every subscription releases. */
static void test_failed_catalog_settles_windows(moq_version_t ver)
{
    int before = failures;
    lh_t f;
    ts_gen_init(&g_ts, 188, 1, false);
    NEED(lh_start(&f, ver, cat_ts, 8, 8));
    moq_media_track_t *ts = f.track['t'], *loc = f.track['l'];
    moq_subgroup_handle_t sg;
    NEED(lh_open(&f, 't', 0, &sg));
    NEED(lh_write_ts(&f, sg, 1));
    NEED(moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    NEED(lh_open(&f, 'l', 5, &sg));
    NEED(lh_write(&f, sg, 0, lh_body, sizeof(lh_body)));
    NEED(moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));
    lh_pump(&f);
    NEED(f.r->st_tail - f.r->st_head == 2);
    NEED(f.r->st_reserved == 1);
    moq_media_stream_terminal_t st;
    NEED(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_DONE);

    lh_fail_catalog(&f);
    NEED(lh_open_eog(&f, 't', 1, true, &sg));
    NEED(lh_write_ts(&f, sg, 0));
    NEED(moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    unsigned wakes = g_wakes;
    for (unsigned i = 0; i < 4; i++) {
        NEED(lh_network(&f));
        lh_pump(&f);
    }
    CHECK_(g_wakes > wakes);
    CHECK_(ts->ts == NULL);
    CHECK_(f.r->st_reserved == 0);
    CHECK_(ts->sub_track == NULL && loc->sub_track == NULL);
    moq_sub_group_end_t ge;
    CHECK_(moq_sub_poll_group_end(f.r->sub, &ge) != MOQ_OK);
    NEED(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK_(st.track == ts && st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 0);
    CHECK_(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_DONE);
    CHECK_(lh_drain_objects(&f) == 1);
    NEED(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK_(st.track == loc && st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 5);
done:
    lh_finish(&f);
    if (failures == before)
        printf("PASS: lane_hook_failed_catalog_settles_windows_d%u\n",
               ver == MOQ_VERSION_DRAFT_16 ? 16u : 18u);
}

/* The catalog fails while a window has a release and a status Object
 * parked for room: both are discarded with their slots, and the drop
 * record already on the lane stays pollable. */
static void test_failed_catalog_settles_parked(moq_version_t ver)
{
    int before = failures;
    lh_t f;
    ts_gen_init(&g_ts, 188, 1, false);
    NEED(lh_start(&f, ver, cat_ts, 3, 4));
    moq_subgroup_handle_t g0, l;
    NEED(lh_open(&f, 't', 0, &g0));
    NEED(lh_write_ts(&f, g0, 1));
    NEED(lh_write_ts(&f, g0, 2));
    for (uint64_t g = 5; g <= 7; g++) {
        NEED(lh_open(&f, 'l', g, &l));
        NEED(lh_write(&f, l, 0, lh_body, sizeof(lh_body)));
    }
    NEED(lh_network(&f));
    lh_pump(&f);
    NEED(moq_simpair_advance_to(f.pair, lh_now(&f) + 2 * WINDOW_US) == MOQ_OK);
    NEED(moq_session_write_status_object(lh_server(&f), g0, 3, MOQ_OBJECT_END_OF_GROUP,
                                         lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));
    lh_pump(&f);
    NEED(f.r->ts_stalled == 1);
    NEED(f.r->st_reserved == 2);

    lh_fail_catalog(&f);
    for (unsigned i = 0; i < 4; i++) {
        lh_pump(&f);
        NEED(lh_network(&f));
    }
    CHECK_(f.r->ts_stalled == 0);
    CHECK_(f.r->st_reserved == 0);
    moq_media_stream_terminal_t st;
    NEED(moq_media_receiver_poll_stream_terminal(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK_(st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP && st.track == f.track['l']);
    CHECK_(st.group_id == 5);
done:
    lh_finish(&f);
    if (failures == before)
        printf("PASS: lane_hook_failed_catalog_settles_parked_d%u\n",
               ver == MOQ_VERSION_DRAFT_16 ? 16u : 18u);
}
#endif

#ifdef MOQ_TEST_HAVE_TS
static const char cat_ts_only[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"t\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1}]}";

/* Draft 22, where a queued Group end also keeps the facade slot: the
 * catalog fails while the facade holds a TS group's Object and its Group
 * end, and the cleanup settles both, so the subscription releases. */
static void test_failed_catalog_group_end_d22(void)
{
    int before = failures;
    lh_t f;
    ts_gen_init(&g_ts, 188, 1, false);
    NEED(lh_start(&f, MOQ_VERSION_DRAFT_22, cat_ts_only, 8, 8));
    moq_media_track_t *ts = f.track['t'];
    moq_subgroup_handle_t sg;
    NEED(lh_open_eog(&f, 't', 0, true, &sg));
    NEED(lh_write_ts(&f, sg, 0));
    NEED(moq_session_close_subgroup(lh_server(&f), sg, lh_now(&f)) == MOQ_OK);
    NEED(lh_network(&f));
    lh_fail_catalog(&f);
    for (unsigned i = 0; i < 4 && ts->sub_track; i++) {
        lh_pump(&f);
        NEED(lh_network(&f));
    }
    CHECK_(ts->sub_track == NULL);
    CHECK_(f.r->st_reserved == 0);
done:
    lh_finish(&f);
    if (failures == before) printf("PASS: lane_hook_failed_catalog_group_end_d22\n");
}
#endif

int main(void)
{
    const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t i = 0; i < 2; i++) {
        test_removal_keeps_terminal(vers[i], false);
        test_removal_keeps_terminal(vers[i], true);
        test_failed_catalog_releases(vers[i]);
#ifdef MOQ_TEST_HAVE_TS
        test_parked_status_through_hook(vers[i]);
        test_failed_catalog_settles_windows(vers[i]);
        test_failed_catalog_settles_parked(vers[i]);
#endif
    }
#ifdef MOQ_TEST_HAVE_TS
    test_failed_catalog_group_end_d22();
#endif
    if (failures) {
        printf("FAIL: %d lane hook failures\n", failures);
        return 1;
    }
    printf("PASS: all lane hook tests\n");
    return 0;
}
