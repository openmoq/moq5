/* Fill loss on the receiver's stream-terminal lane (draft 22): fills of a
 * RAW track through the real receiver hook, over a SimPair whose server
 * session is driven directly, with the endpoint wake observed.
 *
 * Pinned:
 * - a fill's FILL_TERMINAL waits for the fill's queued Objects and for
 *   every RECEIVER_DROP of that fill on the lane: a fill that completes
 *   before a later eviction reports its completion, unchanged, only after
 *   its loss record has been polled, and that poll wakes the network
 *   thread;
 * - an overflow eviction that takes Objects of a LIVE subscription and of
 *   a fill is one RECEIVER_DROP per delivery domain, each naming its origin
 *   and, for the fill, its initiating request;
 * - a fill Object refused at admission (larger than the whole byte budget)
 *   is a RECEIVER_DROP of its own fill, distinct from an overlapping one,
 *   and holds that fill's terminal back the same way;
 * - a fill record freed after its terminal is reused by a later fill,
 *   whose records and terminal are its own;
 * - with the lane off, a fill's terminal keeps its immediate delivery. */
#include <moq/media_receiver.h>
static unsigned g_wakes;
static void observed_wake(moq_endpoint_t *ep) { (void)ep; g_wakes++; }
#define moq_endpoint_wake observed_wake
#define main receiver_fill_lane_companion_main
#include "test_media_receiver_companion.c"
#undef main
#undef moq_endpoint_wake

#define CHECK_(x) do { if (!(x)) { fprintf(stderr, "FAIL: %s:%d: %s\n", \
    __FILE__, __LINE__, #x); failures++; } } while (0)
#define NEED(x) do { if (!(x)) { fprintf(stderr, "FAIL: %s:%d: %s\n", \
    __FILE__, __LINE__, #x); failures++; goto done; } } while (0)

static const char fl_catalog[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const uint8_t fl_body[] = { 0x31, 0, 0x33 };
static const uint8_t fl_video[] = { 0xa4, 0, 0xb5 };

typedef struct fl_fixture {
    test_alloc_state_t state;
    moq_alloc_t alloc;
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
    moq_simpair_t *pair;
    moq_subscription_t peer_sub;
    moq_fill_t fill[3];
    uint64_t initiator[3];
    unsigned fills;
} fl_fixture_t;

#define FL_REQ(x) do { if (!(x)) { fprintf(stderr, "FAIL: %s:%d: setup: %s\n", \
    __FILE__, __LINE__, #x); return false; } } while (0)

static uint64_t fl_now(fl_fixture_t *f) { return moq_simpair_now_us(f->pair); }
static moq_session_t *fl_server(fl_fixture_t *f) { return moq_simpair_server(f->pair); }

/* The server answers SUBSCRIBE (with the track's LOC-04 properties) and
 * records each FILL_REQUEST. */
static bool fl_network(fl_fixture_t *f)
{
    for (unsigned round = 0; round < 3; round++) {
        if (moq_simpair_run_until_quiescent(f->pair, 64, NULL) != MOQ_OK) return false;
        for (unsigned n = 0; n < 64; n++) {
            moq_event_t ev;
            if (moq_session_poll_events(fl_server(f), &ev, 1) == 0) break;
            moq_result_t rc = MOQ_OK;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                bool media = ev.u.subscribe_request.track_name.len == 1 &&
                    ev.u.subscribe_request.track_name.data[0] == 'v';
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                moq_rcbuf_t *properties = NULL;
                if (media) {
                    f->peer_sub = ev.u.subscribe_request.sub;
                    ac.has_largest = true;
                    ac.largest_group = 3;
                    ac.largest_object = 0;
                    moq_loc_metadata_t loc;
                    moq_loc_metadata_init_sized(&loc, sizeof(loc));
                    loc.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG |
                                  MOQ_LOC_METADATA_HAS_TIMESCALE;
                    loc.video_config = (moq_bytes_t){ fl_video, sizeof(fl_video) };
                    loc.timescale = 1000;
                    rc = moq_loc_encode_metadata(&f->alloc, MOQ_VERSION_DRAFT_22,
                        MOQ_LOC_PROFILE_04, &loc, &properties);
                    if (rc == MOQ_OK)
                        ac.track_properties = (moq_bytes_t){ moq_rcbuf_data(properties),
                                                             moq_rcbuf_len(properties) };
                }
                if (rc == MOQ_OK)
                    rc = moq_session_accept_subscribe(fl_server(f),
                        ev.u.subscribe_request.sub, &ac, fl_now(f));
                moq_rcbuf_decref(properties);
            } else if (ev.kind == MOQ_EVENT_FILL_REQUEST && f->fills < 3) {
                f->fill[f->fills] = ev.u.fill_request.fill;
                f->initiator[f->fills++] = ev.u.fill_request.initiating_request_id;
            }
            moq_event_cleanup(&ev);
            if (rc != MOQ_OK) return false;
        }
    }
    return true;
}

static void fl_pump(fl_fixture_t *f)
{
    receiver_hook(NULL, moq_simpair_client(f->pair), fl_now(f), f->receiver);
}

/* One more fill of the LIVE subscription, from group 1. */
static bool fl_request_fill(fl_fixture_t *f)
{
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    location.start_group = 1;
    location.start_object = 0;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &fill;
    unsigned had = f->fills;
    if (moq_sub_update_subscription(f->receiver->sub, f->track->sub_track,
                                    &update, fl_now(f)) != MOQ_OK)
        return false;
    if (!fl_network(f)) return false;
    fl_pump(f);
    return f->fills == had + 1;
}

/* A receiver on a two-Object DROP_GROUP queue, the lane on or off, with a
 * LIVE subscription from group 1 and two fills of it. */
static bool fl_start_with(fl_fixture_t *f, bool lane, uint32_t lane_cap)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->state);
    moq_media_receiver_cfg_t cfg;
    init_cfg(&cfg, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC);
    cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    cfg.overflow.max_objects = 2;
    cfg.overflow.max_bytes = 128;
    cfg.surface_stream_terminals = lane;
    cfg.max_stream_terminals = lane_cap;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->receiver = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    FL_REQ(f->receiver != NULL);
    f->receiver->transport_version = MOQ_VERSION_DRAFT_22;
    moq_media_receiver_test_ingest(f->receiver, 0, 0, fl_catalog, sizeof(fl_catalog) - 1);
    moq_media_track_event_t ev;
    for (unsigned n = 0; n < 8; n++) {
        if (moq_media_receiver_poll_track(f->receiver, &ev, sizeof(ev)) != MOQ_OK) break;
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f->track = ev.track;
    }
    FL_REQ(f->track != NULL);
    f->pair = pair(&f->alloc, MOQ_VERSION_DRAFT_22);
    FL_REQ(f->pair != NULL);
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
    sc.callbacks.ctx = f->receiver;
    sc.callbacks.on_subscribed = receiver_on_subscribed;
    sc.callbacks.on_subscribe_error = receiver_on_subscribe_error;
    sc.on_subscribe_done = receiver_on_subscribe_done;
    sc.on_update_ok = receiver_on_update_ok;
    sc.surface_stream_terminals = lane;
    FL_REQ(moq_sub_create(moq_simpair_client(f->pair), &f->alloc, &sc,
                          &f->receiver->sub) == MOQ_OK);
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    location.start_group = 1;
    location.start_object = 0;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    moq_media_receiver_track_subscribe_cfg_t media;
    moq_media_receiver_track_subscribe_cfg_init_sized(&media, sizeof(media));
    media.location = &location;
    media.fill = &fill;
    FL_REQ(moq_media_receiver_subscribe_track(f->receiver, f->track, &media) == MOQ_OK);
    f->receiver->joins_by_fill = true;
    receiver_reconcile_subscriptions(f->receiver, fl_now(f));
    FL_REQ(f->track->sub_track != NULL);
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = f->receiver->namespace_;
    tc.track_name = MOQ_BYTES_LITERAL("catalog");
    FL_REQ(moq_sub_subscribe(f->receiver->sub, &tc, fl_now(f),
                             &f->receiver->catalog_sub) == MOQ_OK);
    FL_REQ(fl_network(f));
    fl_pump(f);
    if (f->fills != 1 || !moq_sub_track_is_active(f->track->sub_track))
        fprintf(stderr, "  setup: fills=%u state=%d fatal=%d\n", f->fills,
                (int)moq_sub_track_get_state(f->track->sub_track),
                moq_media_receiver_is_fatal(f->receiver));
    FL_REQ(f->fills == 1 && moq_sub_track_is_active(f->track->sub_track));
    FL_REQ(fl_request_fill(f));
    return f->fills == 2;
}

static bool fl_start(fl_fixture_t *f, bool lane)
{
    return fl_start_with(f, lane, 16);
}

static void fl_finish(fl_fixture_t *f)
{
    if (f->receiver)
        moq_media_receiver_test_destroy_with_session(f->receiver,
            f->pair ? moq_simpair_client(f->pair) : NULL,
            f->pair ? fl_now(f) : 0);
    if (f->pair) moq_simpair_destroy(f->pair);
    CHECK_(f->state.balance == 0);
}

static bool fl_buffers(fl_fixture_t *f, size_t size, moq_rcbuf_t **payload,
                       moq_rcbuf_t **properties)
{
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    loc.timestamp = 9000;
    loc.marking_len = 1;
    loc.marking[0] = 0x20;
    if (moq_loc_encode_metadata(&f->alloc, MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_04,
                                &loc, properties) != MOQ_OK)
        return false;
    uint8_t big[256];
    memset(big, 0x5A, sizeof(big));
    if (size > sizeof(big)) size = sizeof(big);
    moq_result_t rc = size ? moq_rcbuf_create(&f->alloc, big, size, payload)
                           : moq_rcbuf_create(&f->alloc, fl_body, sizeof(fl_body), payload);
    if (rc != MOQ_OK) {
        moq_rcbuf_decref(*properties);
        return false;
    }
    return true;
}

/* Fill `child` delivers (group, object), `size` payload octets (0 for the
 * small default), and ends. */
static bool fl_send_fill(fl_fixture_t *f, unsigned child, uint64_t group,
                         uint64_t object, size_t size)
{
    if (moq_session_open_fill(fl_server(f), f->fill[child], fl_now(f)) != MOQ_OK)
        return false;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = group;
    oc.object_id = object;
    if (!fl_buffers(f, size, &oc.payload, &oc.properties)) return false;
    moq_result_t rc = moq_session_write_fill_object(fl_server(f), f->fill[child], &oc, fl_now(f));
    moq_rcbuf_decref(oc.payload);
    moq_rcbuf_decref(oc.properties);
    if (rc != MOQ_OK) return false;
    if (moq_session_end_fill(fl_server(f), f->fill[child], fl_now(f)) != MOQ_OK) return false;
    return fl_network(f);
}

/* The LIVE subscription delivers (group, object) on its own subgroup. */
static bool fl_send_live(fl_fixture_t *f, uint64_t group, uint64_t object)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.object_properties = true;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(fl_server(f), f->peer_sub, &sc, fl_now(f), &sg) != MOQ_OK)
        return false;
    moq_object_cfg_t oc;
    moq_object_cfg_init(&oc);
    oc.object_id = object;
    if (!fl_buffers(f, 0, &oc.payload, &oc.properties)) return false;
    moq_result_t rc = moq_session_write_object_ex(fl_server(f), sg, &oc, fl_now(f));
    moq_rcbuf_decref(oc.payload);
    moq_rcbuf_decref(oc.properties);
    if (rc != MOQ_OK) return false;
    if (moq_session_close_subgroup(fl_server(f), sg, fl_now(f)) != MOQ_OK) return false;
    return fl_network(f);
}

/* Fill terminals now pollable, counted; each must report the peer's
 * successful completion of fill `child`, or of any fill for -1. */
static unsigned fl_fill_terminals_of(fl_fixture_t *f, int child)
{
    unsigned n = 0;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->receiver, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind != MOQ_MEDIA_TRACK_FILL_TERMINAL) continue;
        CHECK_(ev.fill_reason == MOQ_FILL_COMPLETE);
        if (child >= 0) CHECK_(ev.initiating_request_id == f->initiator[child]);
        n++;
    }
    return n;
}

static unsigned fl_fill_terminals(fl_fixture_t *f)
{
    return fl_fill_terminals_of(f, -1);
}

/* Poll lane records until fill `child`'s RECEIVER_DROP: no terminal may be
 * pollable before it, and that fill's is, at once, after it. Returns the
 * records polled before it. */
static unsigned fl_settle_fill(fl_fixture_t *f, unsigned child)
{
    unsigned n = 0;
    moq_media_stream_terminal_t st;
    for (;;) {
        CHECK_(fl_fill_terminals(f) == 0);
        if (moq_media_receiver_poll_stream_terminal(f->receiver, &st, sizeof(st)) != MOQ_OK) {
            CHECK_(false);
            return n;
        }
        if (st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP &&
            st.delivery_origin == MOQ_MEDIA_DELIVERY_FILL &&
            st.initiating_request_id == f->initiator[child])
            break;
        n++;
    }
    CHECK_(fl_fill_terminals_of(f, (int)child) == 1);
    return n;
}

/* Fill 0 completes with its Object queued; group 2's LIVE Object then
 * evicts group 1, taking that Object and the LIVE one: one RECEIVER_DROP
 * per domain. Fill 0's terminal waits for its drop record to be polled;
 * the poll that consumes it makes the terminal pollable and wakes. Fill
 * 1, overlapping, delivers an Object larger than the whole budget: its
 * own drop record, after the LIVE one its admission's eviction made,
 * holds its terminal back the same way. */
static void test_fill_drop_records(void)
{
    int before = failures;
    fl_fixture_t f;
    NEED(fl_start(&f, true));
    NEED(f.initiator[0] != f.initiator[1]);
    NEED(fl_send_fill(&f, 0, 1, 0, 0));
    NEED(fl_send_live(&f, 1, 1));
    fl_pump(&f);
    NEED(f.receiver->obj_tail - f.receiver->obj_head == 2);
    CHECK_(fl_fill_terminals(&f) == 0);

    NEED(fl_send_live(&f, 2, 0));
    fl_pump(&f);
    CHECK_(fl_fill_terminals(&f) == 0);
    unsigned wakes = g_wakes;
    unsigned ahead = fl_settle_fill(&f, 0);
    CHECK_(g_wakes > wakes);
    CHECK_(ahead <= 2);

    NEED(fl_send_fill(&f, 1, 1, 2, 200));
    fl_pump(&f);
    (void)fl_settle_fill(&f, 1);

    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f.receiver, &obj, sizeof(obj)) == MOQ_OK)
        moq_media_object_cleanup(&obj);
    /* What remains is LIVE: no fill owes a record any more. */
    moq_media_stream_terminal_t st;
    while (moq_media_receiver_poll_stream_terminal(f.receiver, &st, sizeof(st)) == MOQ_OK) {
        CHECK_(st.track == f.track);
        CHECK_(st.delivery_origin == MOQ_MEDIA_DELIVERY_LIVE);
        CHECK_(st.initiating_request_id == 0);
    }
    CHECK_(fl_fill_terminals(&f) == 0);
done:
    fl_finish(&f);
    if (failures == before) printf("PASS: fill_lane_drop_records\n");
}

/* Once fill 0's terminal is polled its record is free: a third fill takes
 * it, and that fill's refused Object is a drop record naming the third
 * fill's request, which holds the third fill's terminal back. */
static void test_fill_record_reused(void)
{
    int before = failures;
    fl_fixture_t f;
    NEED(fl_start(&f, true));
    NEED(fl_send_fill(&f, 0, 1, 0, 200));
    fl_pump(&f);
    (void)fl_settle_fill(&f, 0);
    NEED(fl_request_fill(&f));
    NEED(f.initiator[2] != f.initiator[0] && f.initiator[2] != f.initiator[1]);
    int slot = -1;
    for (size_t i = 0; i < RECEIVER_SUB_MAX_FILLS; i++)
        if (f.receiver->fills[i].initiating_request_id == f.initiator[2])
            slot = dom_find_locked(f.receiver, f.receiver->fills[i].token);
    CHECK_(slot == 0);
    NEED(fl_send_fill(&f, 2, 1, 3, 200));
    fl_pump(&f);
    (void)fl_settle_fill(&f, 2);
done:
    fl_finish(&f);
    if (failures == before) printf("PASS: fill_lane_record_reused\n");
}

/* With the lane off, a completed fill's terminal is pollable at once, its
 * Object still queued, as before. */
static void test_lane_off_terminal(void)
{
    int before = failures;
    fl_fixture_t f;
    NEED(fl_start(&f, false));
    NEED(fl_send_fill(&f, 0, 1, 0, 0));
    fl_pump(&f);
    NEED(f.receiver->obj_tail - f.receiver->obj_head == 1);
    CHECK_(fl_fill_terminals(&f) == 1);
done:
    fl_finish(&f);
    if (failures == before) printf("PASS: fill_lane_off_terminal\n");
}

/* The intake reservation counts a fill's queued Objects as a delivery
 * domain of their own. With a LIVE and a fill Object of group 1 queued and
 * the LIVE subgroup's FIN on a three-record lane, two free records are not
 * the room an admission that could drop from both domains needs (two drops
 * and its own record), so group 2's Object waits upstream rather than
 * evicting; once the queue drains it is taken in, with nothing dropped. */
static void test_fill_domain_reserved(void)
{
    int before = failures;
    fl_fixture_t f;
    NEED(fl_start_with(&f, true, 3));
    NEED(fl_send_fill(&f, 0, 1, 0, 0));
    NEED(fl_send_live(&f, 1, 1));
    fl_pump(&f);
    NEED(f.receiver->obj_tail - f.receiver->obj_head == 2);
    NEED(fl_send_live(&f, 2, 0));
    fl_pump(&f);
    moq_media_receiver_stats_t sd;
    CHECK_(moq_media_receiver_get_stats(f.receiver, &sd, sizeof(sd)) == MOQ_OK);
    CHECK_(sd.objects_dropped == 0);
    CHECK_(f.receiver->obj_tail - f.receiver->obj_head == 2);
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f.receiver, &obj, sizeof(obj)) == MOQ_OK)
        moq_media_object_cleanup(&obj);
    fl_pump(&f);
    CHECK_(moq_media_receiver_poll_object(f.receiver, &obj, sizeof(obj)) == MOQ_OK);
    CHECK_(obj.group_id == 2);
    moq_media_object_cleanup(&obj);
    CHECK_(moq_media_receiver_get_stats(f.receiver, &sd, sizeof(sd)) == MOQ_OK);
    CHECK_(sd.objects_dropped == 0);
done:
    fl_finish(&f);
    if (failures == before) printf("PASS: fill_lane_domain_reserved\n");
}

int main(void)
{
    test_fill_drop_records();
    test_fill_record_reused();
    test_lane_off_terminal();
    test_fill_domain_reserved();
    if (failures) {
        printf("FAIL: %d fill lane failures\n", failures);
        return 1;
    }
    printf("PASS: all fill lane tests\n");
    return 0;
}
