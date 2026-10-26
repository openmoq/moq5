/*
 * Fills through the two facades (draft-22 section 3.4), over a simulated
 * pair with the publisher facade on the server and the subscriber facade on
 * the client unless a row says otherwise.
 *
 * Pinned here: on a draft without fills the subscriber facade refuses a fill
 * before anything is sent; on draft 22 a fill whose requested range is fully
 * retained is served and ends COMPLETE; missing requested coverage fails on
 * the wire (header then RESET) and the
 * subscription is untouched; the subscriber cancels one fill without
 * touching the subscription; cancelling the subscription ends an open fill
 * on both sides; the facade's PUBLISH_DONE Stream Count is the application's
 * streams plus the fill stream the session opened; a later update may carry
 * a second fill; two requests progress with one fill job in the historical
 * hybrid dispatch/tick schedule (not the manual-pump contract); a fill
 * handle whose terminal was polled never reaches the later fill that reuses
 * its slot.
 */
#include <moq/subscriber.h>
#include <moq/publisher.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_alloc_support.h"
#include <string.h>
#include <stdio.h>

static int failures = 0;

enum { FILL_OBSERVATIONS = 3, FILL_WIRE_BYTES = 96, POLL_LIMIT = 256 };

typedef struct {
    moq_stream_ref_t ref;
    uint8_t bytes[FILL_WIRE_BYTES];
    size_t len;
    unsigned headers, fins, resets;
    uint64_t reset_code;
} fill_wire_t;

typedef struct {
    moq_pub_fill_request_t requests[FILL_OBSERVATIONS];
    moq_pub_fill_terminal_t terminals[FILL_OBSERVATIONS];
    moq_pub_track_t *tracks[FILL_OBSERVATIONS];
    fill_wire_t wire[FILL_OBSERVATIONS];
    size_t request_count, terminal_count, joined_count;
    unsigned sequence;
    unsigned request_sequence[FILL_OBSERVATIONS];
    unsigned terminal_sequence[FILL_OBSERVATIONS];
    unsigned joined_sequence[FILL_OBSERVATIONS];
    moq_pub_track_t *joined_tracks[FILL_OBSERVATIONS];
} fill_observations_t;

static moq_pub_fill_decision_t observe_fill_request(void *ctx,
    moq_pub_track_t *track, const moq_pub_fill_request_t *request)
{
    fill_observations_t *o = ctx;
    size_t i = o->request_count;
    MOQ_TEST_CHECK_EQ_SIZE(request->struct_size, sizeof(*request));
    MOQ_TEST_CHECK(i < FILL_OBSERVATIONS);
    if (request->struct_size < sizeof(*request) || i >= FILL_OBSERVATIONS)
        return MOQ_PUB_FILL_AUTO_RETAINED;
    /* These rows must never have two request-to-terminal jobs at once. */
    MOQ_TEST_CHECK_EQ_SIZE(i, o->terminal_count);
    MOQ_TEST_CHECK(moq_pub_fill_is_valid(request->fill));
    MOQ_TEST_CHECK(moq_subscription_is_valid(request->sub));
    MOQ_TEST_CHECK(!moq_publication_is_valid(request->pub));
    for (size_t j = 0; j < i; j++)
        MOQ_TEST_CHECK(!moq_pub_fill_eq(request->fill, o->requests[j].fill));
    o->requests[i] = *request;
    o->tracks[i] = track;
    o->request_sequence[i] = ++o->sequence;
    o->request_count++;
    return MOQ_PUB_FILL_AUTO_RETAINED;
}

static void observe_fill_terminal(void *ctx, moq_pub_track_t *track,
                                   const moq_pub_fill_terminal_t *terminal)
{
    fill_observations_t *o = ctx;
    size_t i = o->terminal_count;
    MOQ_TEST_CHECK_EQ_SIZE(terminal->struct_size, sizeof(*terminal));
    MOQ_TEST_CHECK(i < o->request_count && i < FILL_OBSERVATIONS);
    if (terminal->struct_size < sizeof(*terminal) || i >= o->request_count ||
        i >= FILL_OBSERVATIONS) return;
    const moq_pub_fill_request_t *request = &o->requests[i];
    MOQ_TEST_CHECK(track == o->tracks[i]);
    MOQ_TEST_CHECK(moq_pub_fill_eq(terminal->fill, request->fill));
    MOQ_TEST_CHECK(moq_subscription_eq(terminal->sub, request->sub));
    MOQ_TEST_CHECK(moq_publication_eq(terminal->pub, request->pub));
    MOQ_TEST_CHECK_EQ_U64(terminal->initiating_request_id, request->initiating_request_id);
    o->terminals[i] = *terminal;
    o->terminal_sequence[i] = ++o->sequence;
    o->terminal_count++;
}

static void observe_joined(void *ctx, moq_pub_track_t *track)
{
    fill_observations_t *o = ctx;
    size_t i = o->joined_count;
    MOQ_TEST_CHECK(i < FILL_OBSERVATIONS);
    if (i >= FILL_OBSERVATIONS) return;
    o->joined_tracks[i] = track;
    o->joined_sequence[i] = ++o->sequence;
    o->joined_count++;
}

static void observe_fill_wire(void *ctx, const moq_sim_trace_record_t *record)
{
    fill_observations_t *o = ctx;
    if (record->kind != MOQ_SIM_TRACE_ACTION || record->from != MOQ_PERSPECTIVE_SERVER ||
        (record->action_kind != MOQ_ACTION_SEND_DATA &&
         record->action_kind != MOQ_ACTION_RESET_DATA)) return;
    MOQ_TEST_CHECK(record->struct_size >= sizeof(*record));
    if (record->struct_size < sizeof(*record)) return;
    size_t i;
    for (i = 0; i < o->request_count; i++)
        if (o->wire[i].headers && o->wire[i].ref._v == record->stream_ref._v) break;
    if (i == o->request_count) {
        /* Literal D22 FETCH_HEADER; all initiating IDs in these rows are <64. */
        MOQ_TEST_CHECK(record->action_kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK_EQ_SIZE(record->bytes.len, 2);
        if (record->action_kind != MOQ_ACTION_SEND_DATA || record->bytes.len != 2) return;
        MOQ_TEST_CHECK_EQ_INT(record->bytes.data[0], 0x05);
        for (i = 0; i < o->request_count; i++)
            if (o->requests[i].initiating_request_id == record->bytes.data[1]) break;
        MOQ_TEST_CHECK(i < o->request_count);
        if (i == o->request_count) return;
        MOQ_TEST_CHECK_EQ_INT(o->wire[i].headers, 0);
        o->wire[i].headers++;
        o->wire[i].ref = record->stream_ref;
    }
    fill_wire_t *wire = &o->wire[i];
    MOQ_TEST_CHECK_EQ_INT(wire->fins, 0);
    MOQ_TEST_CHECK_EQ_INT(wire->resets, 0);
    if (record->action_kind == MOQ_ACTION_RESET_DATA) {
        wire->resets++;
        wire->reset_code = record->error_code;
        return;
    }
    const moq_bytes_t spans[2] = {record->bytes, record->detail_bytes};
    for (size_t j = 0; j < 2; j++) {
        MOQ_TEST_CHECK(spans[j].len <= sizeof(wire->bytes) - wire->len);
        if (spans[j].len > sizeof(wire->bytes) - wire->len) return;
        if (spans[j].len) memcpy(wire->bytes + wire->len, spans[j].data, spans[j].len);
        wire->len += spans[j].len;
    }
    if (record->fin) wire->fins++;
}

typedef struct {
    test_alloc_state_t as;
    moq_alloc_t        alloc;
    moq_simpair_t     *sp;
    moq_publisher_t   *pub;      /* NULL when the server is driven raw */
    moq_pub_track_t   *ptrack;
    moq_subscriber_t  *sub;      /* NULL when the client is driven raw */
} rig_t;

static moq_bytes_t g_ns[1];

static void discard_events(moq_session_t *session)
{
    moq_event_t event;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_session_poll_events(session, &event, 1) != 1) return;
        moq_event_cleanup(&event);
    }
    MOQ_TEST_CHECK(false);
}

/* max_actions sizes both sessions' action queues and pub_max_fills the
 * publisher facade's fill configuration; zero keeps either default. */
static void rig_open_observed(rig_t *r, moq_version_t ver, bool pub_facade,
                              bool sub_facade, uint32_t max_actions,
                              uint32_t pub_max_fills, fill_observations_t *observations)
{
    memset(r, 0, sizeof(*r));
    r->alloc = test_allocator(&r->as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc; cfg.seed = 7; cfg.initial_now_us = 1000;
    cfg.version = ver;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.max_actions = max_actions;
    if (observations) {
        cfg.trace_fn = observe_fill_wire;
        cfg.trace_ctx = observations;
    }
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &r->sp) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_start(r->sp) == MOQ_OK);
    moq_simpair_run_until_quiescent(r->sp, 8, NULL);
    discard_events(moq_simpair_client(r->sp));
    discard_events(moq_simpair_server(r->sp));

    g_ns[0] = MOQ_BYTES_LITERAL("fill");
    if (pub_facade) {
        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        pcfg.max_fills = pub_max_fills;
        if (observations) {
            moq_pub_callbacks_init_sized(&pcfg.callbacks, sizeof(pcfg.callbacks));
            pcfg.callbacks.ctx = observations;
            pcfg.callbacks.on_subscriber_joined = observe_joined;
            pcfg.on_fill_request = observe_fill_request;
            pcfg.on_fill_request_ctx = observations;
            pcfg.on_fill_terminal = observe_fill_terminal;
            pcfg.on_fill_terminal_ctx = observations;
        }
        MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(r->sp), &r->alloc,
                                      &pcfg, &r->pub) == MOQ_OK);
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = g_ns;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("v");
        MOQ_TEST_CHECK(moq_pub_add_track(r->pub, &tcfg,
            moq_simpair_now_us(r->sp), &r->ptrack) == MOQ_OK);
    }
    if (sub_facade) {
        moq_sub_cfg_t scfg; moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
        MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(r->sp), &r->alloc,
                                      &scfg, &r->sub) == MOQ_OK);
    }
}

static void rig_open_ex(rig_t *r, moq_version_t ver, bool pub_facade,
                        bool sub_facade, uint32_t max_actions,
                        uint32_t pub_max_fills)
{
    rig_open_observed(r, ver, pub_facade, sub_facade, max_actions, pub_max_fills, NULL);
}

static void rig_open(rig_t *r, moq_version_t ver, bool pub_facade, bool sub_facade)
{
    rig_open_ex(r, ver, pub_facade, sub_facade, 0, 0);
}

static void rig_close(rig_t *r)
{
    if (r->sub) moq_sub_destroy(r->sub);
    if (r->pub) moq_pub_destroy(r->pub);
    discard_events(moq_simpair_server(r->sp));
    discard_events(moq_simpair_client(r->sp));
    moq_simpair_destroy(r->sp);
    MOQ_TEST_CHECK(r->as.balance == 0);
}

static uint64_t now(const rig_t *r) { return moq_simpair_now_us(r->sp); }

/* Tick both facades and settle the link, a few rounds. */
static void settle(rig_t *r)
{
    for (int i = 0; i < 6; i++) {
        if (r->pub) (void)moq_pub_tick(r->pub, now(r));
        moq_simpair_run_until_quiescent(r->sp, 8, NULL);
        if (r->sub) (void)moq_sub_tick(r->sub, now(r));
        moq_simpair_run_until_quiescent(r->sp, 8, NULL);
    }
}

static moq_rcbuf_t *mk(const rig_t *r, const char *text)
{
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(&r->alloc, (const uint8_t *)text, strlen(text), &b);
    return b;
}

/* Install objects 0..n-1 of `group` as `track`'s retained group. */
static void retain_on(rig_t *r, moq_pub_track_t *track, uint64_t group, size_t n)
{
    MOQ_TEST_CHECK(n <= 8);
    if (n > 8) return;
    moq_pub_retained_object_t objs[8];
    moq_rcbuf_t *bufs[8];
    const char *texts[8] = { "o0", "o1", "o2", "o3", "o4", "o5", "o6", "o7" };
    for (size_t i = 0; i < n; i++) {
        bufs[i] = mk(r, texts[i]);
        objs[i].object_id = i;
        objs[i].payload = bufs[i];
        objs[i].properties = NULL;
        objs[i].end_of_group = (i + 1 == n);
    }
    moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
    gc.group_id = group; gc.objects = objs; gc.object_count = n;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(r->pub, track, &gc) == MOQ_OK);
    for (size_t i = 0; i < n; i++) moq_rcbuf_decref(bufs[i]);
}

/* Install objects 0..n-1 of `group` as the rig track's retained group. */
static void retain(rig_t *r, uint64_t group, size_t n)
{
    retain_on(r, r->ptrack, group, n);
}

/* The section 3.5 join-at-current-group shape: a Next Object location
 * filter and a fill whose location is the current group (StartGroup=1). */
static moq_result_t subscribe_join_current(rig_t *r, moq_sub_track_t **track,
                                           const moq_location_filter_cfg_t *loc,
                                           const moq_fill_cfg_t *fill)
{
    moq_sub_track_cfg_t tc; moq_sub_track_cfg_init(&tc);
    tc.track_namespace.parts = g_ns;
    tc.track_namespace.count = 1;
    tc.track_name = MOQ_BYTES_LITERAL("v");
    tc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    tc.location = loc;
    tc.fill = fill;
    return moq_sub_subscribe(r->sub, &tc, now(r), track);
}

static size_t queued_actions(moq_session_t *s)
{
    moq_action_t acts[8];
    size_t n, total = 0;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        n = moq_session_poll_actions(s, acts, 8);
        if (n == 0) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        for (size_t i = 0; i < n; i++) moq_action_cleanup(&acts[i]);
        total += n;
    }
    return total;
}

/* -- A draft without fills refuses before anything is sent. --------------- */

static void test_d18_refused(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_18, true, true);
    MOQ_TEST_CHECK(!moq_session_supports_fill(moq_simpair_client(r.sp)));
    MOQ_TEST_CHECK(moq_session_supports_joining_fetch(moq_simpair_client(r.sp)));

    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, NULL, &fill),
                          (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(track == NULL);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(moq_simpair_client(r.sp)), 0);

    /* Nothing leaked: the same subscribe without a fill proceeds, and an
     * update carrying a fill is refused the same way while the track stays
     * active. */
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, NULL, NULL), (int)MOQ_OK);
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    moq_sub_update_cfg_t uc; moq_sub_update_cfg_init_sized(&uc, sizeof(uc));
    uc.has_subscriber_priority = true; uc.subscriber_priority = 3;
    uc.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(r.sub, track, &uc, now(&r)),
                          (int)MOQ_ERR_UNSUPPORTED);
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));
    moq_sub_stats_t st;
    MOQ_TEST_CHECK(moq_sub_get_stats(r.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.fill_items_queued, 0);
    rig_close(&r);
    MOQ_TEST_PASS("d18_refused");
}

/* -- Served from the retained group, ending COMPLETE. ---------------------- */

static void expect_served(rig_t *r, moq_sub_track_t *track,
                          moq_sub_fill_t req, uint64_t group, size_t n)
{
    moq_sub_fill_item_t it;
    size_t seen = 0;
    unsigned created = 0;
    bool terminal = false;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_sub_poll_fill(r->sub, &it, sizeof(it)) != MOQ_OK) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, req));
        MOQ_TEST_CHECK(it.track == track);
        if (it.kind == MOQ_SUB_FILL_CREATED) {
            MOQ_TEST_CHECK(seen == 0 && !terminal);
            created++;
        } else if (it.kind == MOQ_SUB_FILL_OBJECT) {
            MOQ_TEST_CHECK_EQ_INT(created, 1);
            MOQ_TEST_CHECK(!terminal);
            MOQ_TEST_CHECK_EQ_U64(it.u.object.group_id, group);
            MOQ_TEST_CHECK_EQ_U64(it.u.object.object_id, (uint64_t)seen);
            MOQ_TEST_CHECK(it.u.object.payload != NULL);
            if (it.u.object.payload)
                MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(it.u.object.payload), 2);
            seen++;
        } else if (it.kind == MOQ_SUB_FILL_TERMINAL) {
            MOQ_TEST_CHECK_EQ_INT(created, 1);
            MOQ_TEST_CHECK(!terminal);
            terminal = true;
            MOQ_TEST_CHECK_EQ_INT((int)it.u.terminal.reason, (int)MOQ_FILL_COMPLETE);
            MOQ_TEST_CHECK_EQ_INT((int)it.u.terminal.error_domain, (int)MOQ_FILL_ERROR_NONE);
        } else {
            MOQ_TEST_CHECK(false);
        }
        moq_sub_fill_item_cleanup(&it);
    }
    MOQ_TEST_CHECK_EQ_SIZE(seen, n);
    MOQ_TEST_CHECK_EQ_INT(created, 1);
    MOQ_TEST_CHECK(terminal);
}

static void test_d22_served(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, true, true);
    MOQ_TEST_CHECK(moq_session_supports_fill(moq_simpair_client(r.sp)));
    MOQ_TEST_CHECK(!moq_session_supports_joining_fetch(moq_simpair_client(r.sp)));
    retain(&r, 3, 3);

    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    floc.relative_group_offset = 1;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;

    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, &loc, &fill), (int)MOQ_OK);
    moq_sub_fill_t req = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(req));
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    MOQ_TEST_CHECK(moq_sub_fill_eq(moq_sub_track_fill(track), req));

    moq_sub_stats_t st;
    MOQ_TEST_CHECK(moq_sub_get_stats(r.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.fill_objects, 3);
    MOQ_TEST_CHECK_EQ_U64(st.fill_terminals, 1);
    MOQ_TEST_CHECK_EQ_U64(st.fill_items_queued, 5);
    MOQ_TEST_CHECK_EQ_U64(st.bytes_queued, 6);

    expect_served(&r, track, req, 3, 3);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));   /* slot released */
    MOQ_TEST_CHECK(moq_sub_get_stats(r.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.fill_items_polled, 5);
    MOQ_TEST_CHECK_EQ_U64(st.bytes_queued, 0);

    /* The subscription is untouched by the fill's end, on both sides. */
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    MOQ_TEST_CHECK(moq_pub_has_subscriber(r.pub, r.ptrack));

    /* A later update carries a second fill (an absolute range over the same
     * retained group); the track's newest fill is the new request. */
    moq_location_filter_cfg_t floc2; moq_location_filter_cfg_init(&floc2);
    floc2.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    floc2.start_group = 3; floc2.start_object = 0;
    moq_fill_cfg_t fill2; moq_fill_cfg_init(&fill2);
    fill2.location = &floc2;
    moq_sub_update_cfg_t uc; moq_sub_update_cfg_init_sized(&uc, sizeof(uc));
    uc.has_subscriber_priority = true; uc.subscriber_priority = 9;
    uc.fill = &fill2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(r.sub, track, &uc, now(&r)), (int)MOQ_OK);
    moq_sub_fill_t req2 = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(req2));
    settle(&r);
    expect_served(&r, track, req2, 3, 3);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    rig_close(&r);
    MOQ_TEST_PASS("d22_served");
}

static void check_retained_object(const moq_sub_fill_item_t *it,
                                  uint64_t group, uint64_t object)
{
    MOQ_TEST_CHECK_EQ_U64(it->u.object.group_id, group);
    MOQ_TEST_CHECK_EQ_U64(it->u.object.subgroup_id, 0);
    MOQ_TEST_CHECK_EQ_U64(it->u.object.object_id, object);
    MOQ_TEST_CHECK_EQ_INT(it->u.object.publisher_priority, 128);
    MOQ_TEST_CHECK(!it->u.object.datagram);
    MOQ_TEST_CHECK(it->u.object.properties == NULL);
    MOQ_TEST_CHECK(it->u.object.payload != NULL);
    if (!it->u.object.payload) return;
    MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(it->u.object.payload), 2);
    if (moq_rcbuf_len(it->u.object.payload) == 2) {
        const uint8_t expected[2] = {'o', (uint8_t)('0' + object)};
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(it->u.object.payload), expected, 2) == 0);
    }
}

static void check_fill_callbacks(const fill_observations_t *o, size_t i,
    moq_pub_track_t *track, uint64_t id, uint64_t group, uint64_t first,
    uint64_t last, bool failed)
{
    MOQ_TEST_CHECK(i < o->request_count && i < o->terminal_count);
    if (i >= o->request_count || i >= o->terminal_count) return;
    const moq_pub_fill_request_t *request = &o->requests[i];
    const moq_pub_fill_terminal_t *terminal = &o->terminals[i];
    MOQ_TEST_CHECK(o->tracks[i] == track);
    MOQ_TEST_CHECK_EQ_U64(request->initiating_request_id, id);
    MOQ_TEST_CHECK_EQ_U64(request->start_group, group);
    MOQ_TEST_CHECK_EQ_U64(request->start_object, first);
    MOQ_TEST_CHECK_EQ_U64(request->end_group, group);
    MOQ_TEST_CHECK_EQ_U64(request->end_object, last);
    MOQ_TEST_CHECK_EQ_U64(request->processed_at_us, 1000);
    MOQ_TEST_CHECK_EQ_INT(request->group_order, MOQ_GROUP_ORDER_ASCENDING);
    MOQ_TEST_CHECK_EQ_INT(request->subscriber_priority, 128);
    MOQ_TEST_CHECK(!request->has_fill_timeout);
    MOQ_TEST_CHECK_EQ_U64(request->fill_timeout_ms, 0);
    MOQ_TEST_CHECK_EQ_INT(terminal->reason, failed ? MOQ_FILL_LOCAL_ABORTED : MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK_EQ_INT(terminal->error_domain, failed ? MOQ_FILL_ERROR_STREAM : MOQ_FILL_ERROR_NONE);
    MOQ_TEST_CHECK_EQ_U64(terminal->error_code, failed ? MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS : 0);
    MOQ_TEST_CHECK(o->request_sequence[i] < o->terminal_sequence[i]);
    MOQ_TEST_CHECK_EQ_INT(o->wire[i].headers, 1);
    MOQ_TEST_CHECK_EQ_INT(o->wire[i].fins, failed ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(o->wire[i].resets, failed ? 1 : 0);
    MOQ_TEST_CHECK_EQ_U64(o->wire[i].reset_code, failed ? MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS : 0);
    if (failed) {
        const uint8_t header[2] = {0x05, (uint8_t)id};
        MOQ_TEST_CHECK_EQ_SIZE(o->wire[i].len, sizeof(header));
        if (o->wire[i].len == sizeof(header))
            MOQ_TEST_CHECK(memcmp(o->wire[i].bytes, header, sizeof(header)) == 0);
    }
}

/* -- Full requested coverage, or header/RESET without touching the parent. -- */

static void expect_retained_fill(rig_t *r, moq_sub_track_t *track,
    moq_sub_fill_t req, uint64_t id, uint64_t first, size_t count, bool failed)
{
    moq_sub_fill_item_t it;
    size_t objects = 0;
    unsigned created = 0, terminals = 0;
    moq_result_t rc = MOQ_OK;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        rc = moq_sub_poll_fill(r->sub, &it, sizeof(it));
        if (rc != MOQ_OK) break;
        MOQ_TEST_CHECK_EQ_SIZE(it.struct_size, sizeof(it));
        MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, req));
        MOQ_TEST_CHECK(it.track == track);
        MOQ_TEST_CHECK_EQ_U64(it.initiating_request_id, id);
        if (it.kind == MOQ_SUB_FILL_CREATED) {
            MOQ_TEST_CHECK_EQ_SIZE(objects, 0);
            MOQ_TEST_CHECK_EQ_INT(terminals, 0);
            created++;
        } else if (it.kind == MOQ_SUB_FILL_OBJECT) {
            MOQ_TEST_CHECK_EQ_INT(created, 1);
            MOQ_TEST_CHECK_EQ_INT(terminals, 0);
            MOQ_TEST_CHECK(objects < count);
            check_retained_object(&it, 3, first + objects);
            objects++;
        } else if (it.kind == MOQ_SUB_FILL_TERMINAL) {
            MOQ_TEST_CHECK_EQ_INT(created, 1);
            MOQ_TEST_CHECK_EQ_INT(terminals, 0);
            MOQ_TEST_CHECK_EQ_SIZE(objects, count);
            MOQ_TEST_CHECK_EQ_INT(it.u.terminal.reason,
                failed ? MOQ_FILL_PEER_CANCELLED_OR_RESET : MOQ_FILL_COMPLETE);
            MOQ_TEST_CHECK_EQ_INT(it.u.terminal.error_domain,
                failed ? MOQ_FILL_ERROR_STREAM : MOQ_FILL_ERROR_NONE);
            MOQ_TEST_CHECK_EQ_U64(it.u.terminal.error_code,
                failed ? MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS : 0);
            terminals++;
        } else {
            MOQ_TEST_CHECK(false);
        }
        moq_sub_fill_item_cleanup(&it);
    }
    MOQ_TEST_CHECK(rc == MOQ_DONE);
    MOQ_TEST_CHECK_EQ_SIZE(objects, count);
    MOQ_TEST_CHECK_EQ_INT(terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(created, 1);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    MOQ_TEST_CHECK(moq_pub_has_subscriber(r->pub, r->ptrack));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(moq_simpair_client(r->sp)), (int)MOQ_SESS_ESTABLISHED);
}

static void test_d22_failed(void)
{
    fill_observations_t observations = {0};
    rig_t r;
    rig_open_observed(&r, MOQ_VERSION_DRAFT_22, true, true, 0, 0, &observations);
    retain(&r, 3, 3);

    /* Established with the retained group advertised as Largest (3,2). */
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, &loc, NULL), (int)MOQ_OK);
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    /* The retained group is gone when the fill arrives: the publisher has
     * nothing to serve, so the fill fetch stream is opened and reset. */
    MOQ_TEST_CHECK(moq_pub_clear_retained_group(r.pub, r.ptrack) == MOQ_OK);
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    floc.start_group = 3; floc.start_object = 0;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_update_cfg_t uc; moq_sub_update_cfg_init_sized(&uc, sizeof(uc));
    uc.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(r.sub, track, &uc, now(&r)), (int)MOQ_OK);
    moq_sub_fill_t req = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(req));
    settle(&r);
    expect_retained_fill(&r, track, req, 2, 0, 0, true);
    MOQ_TEST_CHECK_EQ_SIZE(observations.request_count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(observations.terminal_count, 1);
    check_fill_callbacks(&observations, 0, r.ptrack, 2, 3, 0, 2, true);

    /* All requested objects1..2 are retained. Generic fill coverage is not
     * the service catalog's independent-base requirement. */
    retain(&r, 3, 3);
    floc.start_object = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(r.sub, track, &uc, now(&r)), (int)MOQ_OK);
    moq_sub_fill_t previous = req;
    req = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(req));
    MOQ_TEST_CHECK(!moq_sub_fill_eq(previous, req));
    settle(&r);
    expect_retained_fill(&r, track, req, 4, 1, 2, false);
    MOQ_TEST_CHECK_EQ_SIZE(observations.request_count, 2);
    MOQ_TEST_CHECK_EQ_SIZE(observations.terminal_count, 2);
    check_fill_callbacks(&observations, 1, r.ptrack, 4, 3, 1, 2, false);
    /* D22 11.4.1: first group/object absolute; next object implicitly +1. */
    const uint8_t suffix_wire[] = {
        0x05, 0x04, 0x1c, 0x03, 0x01, 0x80, 0x02, 'o', '1',
        0x11, 0x80, 0x02, 'o', '2'
    };
    MOQ_TEST_CHECK_EQ_SIZE(observations.wire[1].len, sizeof(suffix_wire));
    if (observations.wire[1].len == sizeof(suffix_wire))
        MOQ_TEST_CHECK(memcmp(observations.wire[1].bytes, suffix_wire, sizeof(suffix_wire)) == 0);

    /* Largest stays(3,2), but the new candidate lacks the requested final
     * object. No partial object1 or FIN may escape before RESET6. */
    retain(&r, 3, 2);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(r.sub, track, &uc, now(&r)), (int)MOQ_OK);
    previous = req;
    req = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(req));
    MOQ_TEST_CHECK(!moq_sub_fill_eq(previous, req));
    settle(&r);
    expect_retained_fill(&r, track, req, 6, 1, 0, true);
    MOQ_TEST_CHECK_EQ_SIZE(observations.request_count, 3);
    MOQ_TEST_CHECK_EQ_SIZE(observations.terminal_count, 3);
    check_fill_callbacks(&observations, 2, r.ptrack, 6, 3, 1, 2, true);
    for (size_t i = 1; i < FILL_OBSERVATIONS; i++)
        MOQ_TEST_CHECK(moq_subscription_eq(observations.requests[0].sub,
                                          observations.requests[i].sub));
    MOQ_TEST_CHECK_EQ_SIZE(observations.joined_count, 1);
    MOQ_TEST_CHECK(observations.joined_tracks[0] == r.ptrack);
    rig_close(&r);
    MOQ_TEST_PASS("d22_failed");
}

/* -- Raw server: accept the SUBSCRIBE with a Largest and open the fill. ---- */

static bool raw_server_open_fill(rig_t *r, moq_fill_t *sfill, moq_subscription_t *ssub)
{
    moq_session_t *sv = moq_simpair_server(r->sp);
    moq_event_t ev;
    bool got_req = false, got_fill = false;
    for (int i = 0; i < 6 && !(got_req && got_fill); i++) {
        moq_simpair_run_until_quiescent(r->sp, 8, NULL);
        for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
            if (moq_session_poll_events(sv, &ev, 1) != 1) break;
            MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                *ssub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
                ac.has_largest = true; ac.largest_group = 2; ac.largest_object = 0;
                MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, *ssub, &ac, now(r)) == MOQ_OK);
                got_req = true;
            } else if (ev.kind == MOQ_EVENT_FILL_REQUEST) {
                *sfill = ev.u.fill_request.fill;
                got_fill = true;
            }
            moq_event_cleanup(&ev);
        }
    }
    if (!got_fill) return false;
    MOQ_TEST_CHECK(moq_session_open_fill(sv, *sfill, now(r)) == MOQ_OK);
    moq_fetch_object_cfg_t oc; moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0; oc.object_id = 0; oc.publisher_priority = 1;
    oc.payload = mk(r, "x0");
    MOQ_TEST_CHECK(moq_session_write_fill_object(sv, *sfill, &oc, now(r)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    moq_simpair_run_until_quiescent(r->sp, 8, NULL);
    return true;
}

static bool raw_take_terminal(moq_session_t *s, moq_fill_t fill, uint32_t *reason)
{
    moq_event_t ev;
    bool found = false;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_session_poll_events(s, &ev, 1) != 1) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        if (!found && ev.kind == MOQ_EVENT_FILL_TERMINAL &&
            moq_fill_eq(ev.u.fill_terminal.fill, fill)) {
            *reason = ev.u.fill_terminal.reason;
            found = true;
        }
        moq_event_cleanup(&ev);
    }
    return found;
}

/* -- The subscriber cancels one fill; the subscription continues. ---------- */

static void test_d22_cancelled(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, false, true);
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, NULL, &fill), (int)MOQ_OK);
    moq_sub_fill_t req = moq_sub_track_fill(track);

    moq_fill_t sfill; moq_subscription_t ssub;
    MOQ_TEST_CHECK(raw_server_open_fill(&r, &sfill, &ssub));
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    /* One object so far, the stream still open. */
    moq_sub_fill_item_t it;
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)it.kind, (int)MOQ_SUB_FILL_CREATED);
    MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, req));
    moq_sub_fill_item_cleanup(&it);
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)it.kind, (int)MOQ_SUB_FILL_OBJECT);
    MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, req));
    moq_sub_fill_item_cleanup(&it);
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_DONE);

    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_cancel_fill(r.sub, req, now(&r)), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_cancel_fill(r.sub, req, now(&r)), (int)MOQ_ERR_WRONG_STATE);
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)it.kind, (int)MOQ_SUB_FILL_TERMINAL);
    MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, req));
    MOQ_TEST_CHECK_EQ_INT((int)it.u.terminal.reason, (int)MOQ_FILL_LOCAL_CANCELLED);
    moq_sub_fill_item_cleanup(&it);
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_DONE);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    uint32_t reason = 0;
    MOQ_TEST_CHECK(raw_take_terminal(moq_simpair_server(r.sp), sfill, &reason));
    MOQ_TEST_CHECK_EQ_INT((int)reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    rig_close(&r);
    MOQ_TEST_PASS("d22_cancelled");
}

/* -- Cancelling the subscription ends the open fill on both sides. --------- */

static void test_d22_parent_cancel(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, false, true);
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, NULL, &fill), (int)MOQ_OK);
    moq_sub_fill_t req = moq_sub_track_fill(track);
    moq_fill_t sfill; moq_subscription_t ssub;
    MOQ_TEST_CHECK(raw_server_open_fill(&r, &sfill, &ssub));
    settle(&r);

    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_unsubscribe(r.sub, track, now(&r)), (int)MOQ_OK);
    settle(&r);
    moq_sub_fill_item_t it;
    unsigned created = 0;
    bool terminal = false;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_sub_poll_fill(r.sub, &it, sizeof(it)) != MOQ_OK) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, req));
        if (it.kind == MOQ_SUB_FILL_CREATED) {
            MOQ_TEST_CHECK(!terminal);
            created++;
        } else if (it.kind == MOQ_SUB_FILL_TERMINAL) {
            terminal = true;
            MOQ_TEST_CHECK_EQ_INT((int)it.u.terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
        }
        moq_sub_fill_item_cleanup(&it);
    }
    MOQ_TEST_CHECK_EQ_INT(created, 1);
    MOQ_TEST_CHECK(terminal);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));

    uint32_t reason = 0;
    MOQ_TEST_CHECK(raw_take_terminal(moq_simpair_server(r.sp), sfill, &reason));
    MOQ_TEST_CHECK_EQ_INT((int)reason, (int)MOQ_FILL_PARENT_TERMINATED);
    rig_close(&r);
    MOQ_TEST_PASS("d22_parent_cancel");
}

/* -- Stream Count: the facade's own streams plus the session's fill stream. */

static void test_d22_stream_count(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, true, false);
    retain(&r, 3, 2);
    moq_session_t *cl = moq_simpair_client(r.sp);

    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    floc.relative_group_offset = 1;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = g_ns; sc.track_namespace.count = 1;
    sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.location = &loc;
    sc.fill = &fill;
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &sc, now(&r), &csub) == MOQ_OK);
    settle(&r);

    /* The fill ran to completion; one live object opens the application's
     * one subgroup stream. */
    moq_event_t ev;
    int fill_objects = 0, fill_terminals = 0;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_session_poll_events(cl, &ev, 1) != 1) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) fill_objects++;
        if (ev.kind == MOQ_EVENT_FILL_TERMINAL) fill_terminals++;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(fill_objects, 2);
    MOQ_TEST_CHECK_EQ_INT(fill_terminals, 1);

    moq_rcbuf_t *p = mk(&r, "live");
    MOQ_TEST_CHECK(moq_pub_write_object(r.pub, r.ptrack, 4, 0, p, now(&r)) == MOQ_OK);
    moq_rcbuf_decref(p);
    settle(&r);
    MOQ_TEST_CHECK(moq_pub_finish_subscribers(r.pub, r.ptrack,
        MOQ_PUB_DONE_TRACK_ENDED, now(&r)) == MOQ_OK);
    settle(&r);

    bool done = false;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_session_poll_events(cl, &ev, 1) != 1) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            done = true;
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, MOQ_PUB_DONE_TRACK_ENDED);
            /* One subgroup the facade opened plus the fill fetch stream the
             * session opened (section 9.9 counts fill fetch streams). */
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, 2);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(done);
    rig_close(&r);
    MOQ_TEST_PASS("d22_stream_count");
}

/* -- Two requests, one job, historical hybrid dispatch/tick schedule. ------ *
 * First forward the queued events without flushing, then drain/flush and tick
 * both facades. One parent waits without ACK while the first job is charged;
 * its real terminal dispatch returns credit. This does not cover the manual
 * flush-to-OK contract or a third pending admission. */

static void test_d22_requests_beyond_configured_fills(void)
{
    fill_observations_t observations = {0};
    rig_t r;
    rig_open_observed(&r, MOQ_VERSION_DRAFT_22, true, true, 6, 1, &observations);
    moq_session_t *sv = moq_simpair_server(r.sp);
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = g_ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("w");
    moq_pub_track_t *ptrack2 = NULL;
    MOQ_TEST_CHECK(moq_pub_add_track(r.pub, &tcfg, now(&r), &ptrack2) == MOQ_OK);
    retain(&r, 3, 8);
    retain_on(&r, ptrack2, 5, 8);

    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    floc.relative_group_offset = 1;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_track_t *tracks[2] = { NULL, NULL };
    moq_sub_fill_t fills[2] = {{0}, {0}};
    const char *names[2] = { "v", "w" };
    for (int i = 0; i < 2; i++) {
        moq_sub_track_cfg_t tc; moq_sub_track_cfg_init(&tc);
        tc.track_namespace.parts = g_ns;
        tc.track_namespace.count = 1;
        tc.track_name.data = (const uint8_t *)names[i];
        tc.track_name.len = 1;
        tc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        tc.location = &loc;
        tc.fill = &fill;
        MOQ_TEST_CHECK_EQ_INT((int)moq_sub_subscribe(r.sub, &tc, now(&r), &tracks[i]),
                              (int)MOQ_OK);
        fills[i] = moq_sub_track_fill(tracks[i]);
        MOQ_TEST_CHECK(moq_sub_fill_is_valid(fills[i]));
    }
    MOQ_TEST_CHECK(!moq_sub_fill_eq(fills[0], fills[1]));
    moq_simpair_run_until_quiescent(r.sp, 8, NULL);

    /* The first ACK, fill header and four objects exhaust six actions.
     * The second admission is CONSUMED but has no ACK or fill job yet. */
    moq_event_t ev;
    unsigned fill_requests = 0, subscriptions = 0, polled;
    moq_subscription_t parents[2] = {{0}, {0}};
    const size_t credit[3] = {5, 5, 0};
    for (polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_session_poll_events(sv, &ev, 1) != 1) break;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            MOQ_TEST_CHECK(subscriptions < 2);
            MOQ_TEST_CHECK_EQ_INT(fill_requests, 0);
            if (subscriptions < 2) {
                parents[subscriptions] = ev.u.subscribe_request.sub;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.track_name.len, 1);
                if (ev.u.subscribe_request.track_name.len == 1)
                    MOQ_TEST_CHECK_EQ_INT(ev.u.subscribe_request.track_name.data[0], names[subscriptions][0]);
            }
            subscriptions++;
        } else if (ev.kind == MOQ_EVENT_FILL_REQUEST) {
            MOQ_TEST_CHECK_EQ_INT(subscriptions, 2);
            MOQ_TEST_CHECK(moq_subscription_eq(ev.u.fill_request.sub, parents[0]));
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.initiating_request_id, 0);
            fill_requests++;
        } else {
            MOQ_TEST_CHECK(false);
        }
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t rc = moq_pub_handle_event(r.pub, &ev, now(&r), &res);
        MOQ_TEST_CHECK_EQ_INT(rc, polled == 0 ? MOQ_OK : MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(res == MOQ_PUB_EVENT_CONSUMED);
        MOQ_TEST_CHECK(polled < 3);
        if (polled < 3) MOQ_TEST_CHECK_EQ_SIZE(moq_session_action_capacity(sv), credit[polled]);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(polled, 3);
    MOQ_TEST_CHECK_EQ_INT(fill_requests, 1);
    MOQ_TEST_CHECK_EQ_SIZE(observations.request_count, 1);
    MOQ_TEST_CHECK_EQ_SIZE(observations.terminal_count, 0);
    MOQ_TEST_CHECK_EQ_SIZE(observations.joined_count, 1);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(r.pub, ptrack2));
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_action_capacity(sv), 0);

    for (int round = 0; round < 4; round++) {
        moq_simpair_run_until_quiescent(r.sp, 8, NULL);
        moq_result_t rc = moq_pub_flush(r.pub, now(&r));
        MOQ_TEST_CHECK_EQ_INT(rc, round == 0 ? MOQ_ERR_WOULD_BLOCK : MOQ_OK);
        if (round == 0) {
            /* Four remaining objects plus FIN are queued, but flush cannot
             * consume the native terminal or ACK the waiting parent. */
            MOQ_TEST_CHECK_EQ_SIZE(moq_session_action_capacity(sv), 1);
            MOQ_TEST_CHECK_EQ_SIZE(observations.request_count, 1);
            MOQ_TEST_CHECK_EQ_SIZE(observations.terminal_count, 0);
            MOQ_TEST_CHECK_EQ_SIZE(observations.joined_count, 1);
            MOQ_TEST_CHECK(!moq_pub_has_subscriber(r.pub, ptrack2));
        }
        settle(&r);
    }

    MOQ_TEST_CHECK_EQ_SIZE(observations.request_count, 2);
    MOQ_TEST_CHECK_EQ_SIZE(observations.terminal_count, 2);
    MOQ_TEST_CHECK_EQ_SIZE(observations.joined_count, 2);
    MOQ_TEST_CHECK(observations.joined_tracks[0] == r.ptrack);
    MOQ_TEST_CHECK(observations.joined_tracks[1] == ptrack2);
    MOQ_TEST_CHECK(observations.joined_sequence[0] < observations.request_sequence[0]);
    MOQ_TEST_CHECK(observations.terminal_sequence[0] < observations.joined_sequence[1]);
    MOQ_TEST_CHECK(observations.joined_sequence[1] < observations.request_sequence[1]);
    for (size_t i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(moq_subscription_eq(observations.requests[i].sub, parents[i]));
        check_fill_callbacks(&observations, i, i ? ptrack2 : r.ptrack,
                             i * 2, i ? 5 : 3, 0, 7, false);
    }
    size_t objects[2] = { 0, 0 };
    int created[2] = { 0, 0 };
    int terminals[2] = { 0, 0 };
    moq_sub_fill_item_t it;
    moq_result_t rc = MOQ_OK;
    for (polled = 0; polled < POLL_LIMIT; polled++) {
        rc = moq_sub_poll_fill(r.sub, &it, sizeof(it));
        if (rc != MOQ_OK) break;
        int k = it.track == tracks[0] ? 0 : it.track == tracks[1] ? 1 : -1;
        MOQ_TEST_CHECK(k >= 0);
        if (k >= 0) {
            MOQ_TEST_CHECK_EQ_SIZE(it.struct_size, sizeof(it));
            MOQ_TEST_CHECK(moq_sub_fill_eq(it.request, fills[k]));
            MOQ_TEST_CHECK_EQ_U64(it.initiating_request_id, (uint64_t)k * 2);
            if (it.kind == MOQ_SUB_FILL_CREATED) {
                MOQ_TEST_CHECK(objects[k] == 0 && terminals[k] == 0);
                created[k]++;
            } else if (it.kind == MOQ_SUB_FILL_OBJECT) {
                MOQ_TEST_CHECK_EQ_INT(created[k], 1);
                MOQ_TEST_CHECK_EQ_INT(terminals[k], 0);
                MOQ_TEST_CHECK(objects[k] < 8);
                check_retained_object(&it, k ? 5 : 3, objects[k]);
                objects[k]++;
            } else if (it.kind == MOQ_SUB_FILL_TERMINAL) {
                MOQ_TEST_CHECK_EQ_INT(created[k], 1);
                MOQ_TEST_CHECK_EQ_INT(terminals[k], 0);
                MOQ_TEST_CHECK_EQ_SIZE(objects[k], 8);
                terminals[k]++;
                MOQ_TEST_CHECK_EQ_INT(it.u.terminal.reason, MOQ_FILL_COMPLETE);
                MOQ_TEST_CHECK_EQ_INT(it.u.terminal.error_domain, MOQ_FILL_ERROR_NONE);
                MOQ_TEST_CHECK_EQ_U64(it.u.terminal.error_code, 0);
            } else {
                MOQ_TEST_CHECK(false);
            }
        }
        moq_sub_fill_item_cleanup(&it);
    }
    MOQ_TEST_CHECK(rc == MOQ_DONE);
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK_EQ_INT(created[k], 1);
        MOQ_TEST_CHECK_EQ_SIZE(objects[k], 8);
        MOQ_TEST_CHECK_EQ_INT(terminals[k], 1);
        MOQ_TEST_CHECK(moq_sub_track_is_active(tracks[k]));
        MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(tracks[k])));
    }
    MOQ_TEST_CHECK(moq_pub_has_subscriber(r.pub, r.ptrack));
    MOQ_TEST_CHECK(moq_pub_has_subscriber(r.pub, ptrack2));
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(r.sp)) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    rig_close(&r);
    MOQ_TEST_PASS("d22_requests_beyond_configured_fills");
}

/* -- A fill's handle never names a later fill. ---------------------------- *
 * Once a fill's terminal is polled its slot is reused by the next request;
 * the earlier fill's handle must not reach the new fill. */

/* Raw server: wait for the next fill request, open it and write one object,
 * leaving its stream open. */
static bool raw_server_next_fill(rig_t *r, moq_fill_t *sfill)
{
    moq_session_t *sv = moq_simpair_server(r->sp);
    moq_event_t ev;
    bool got = false;
    for (int i = 0; i < 6 && !got; i++) {
        moq_simpair_run_until_quiescent(r->sp, 8, NULL);
        for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
            if (moq_session_poll_events(sv, &ev, 1) != 1) break;
            MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
            if (!got && ev.kind == MOQ_EVENT_FILL_REQUEST) {
                *sfill = ev.u.fill_request.fill;
                got = true;
            }
            moq_event_cleanup(&ev);
        }
    }
    if (!got) return false;
    MOQ_TEST_CHECK(moq_session_open_fill(sv, *sfill, now(r)) == MOQ_OK);
    moq_fetch_object_cfg_t oc; moq_fetch_object_cfg_init(&oc);
    oc.group_id = 1; oc.object_id = 0; oc.publisher_priority = 1;
    oc.payload = mk(r, "y0");
    MOQ_TEST_CHECK(moq_session_write_fill_object(sv, *sfill, &oc, now(r)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    moq_simpair_run_until_quiescent(r->sp, 8, NULL);
    return true;
}

/* Poll every queued fill item; returns the terminal reason seen (0 if none)
 * and counts the objects. */
static uint32_t drain_fill_items(rig_t *r, size_t *objects)
{
    uint32_t reason = 0;
    moq_sub_fill_item_t it;
    for (unsigned polled = 0; polled < POLL_LIMIT; polled++) {
        if (moq_sub_poll_fill(r->sub, &it, sizeof(it)) != MOQ_OK) break;
        MOQ_TEST_CHECK(polled + 1 < POLL_LIMIT);
        if (it.kind == MOQ_SUB_FILL_CREATED) {
            moq_sub_fill_item_cleanup(&it);
            continue;
        }
        if (it.kind == MOQ_SUB_FILL_OBJECT && objects) (*objects)++;
        if (it.kind == MOQ_SUB_FILL_TERMINAL) reason = it.u.terminal.reason;
        moq_sub_fill_item_cleanup(&it);
    }
    return reason;
}

static void test_d22_stale_fill_handle(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, false, true);
    moq_session_t *sv = moq_simpair_server(r.sp);
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, NULL, &fill), (int)MOQ_OK);
    moq_sub_fill_t first = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(first));

    /* The first fill completes and its terminal is polled. */
    moq_fill_t sfill1; moq_subscription_t ssub;
    MOQ_TEST_CHECK(raw_server_open_fill(&r, &sfill1, &ssub));
    MOQ_TEST_CHECK(moq_session_end_fill(sv, sfill1, now(&r)) == MOQ_OK);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT((int)drain_fill_items(&r, NULL), (int)MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(track)));

    /* A second fill, carried on an update, is open with one object. */
    moq_sub_update_cfg_t uc; moq_sub_update_cfg_init_sized(&uc, sizeof(uc));
    uc.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_update_subscription(r.sub, track, &uc, now(&r)),
                          (int)MOQ_OK);
    moq_sub_fill_t second = moq_sub_track_fill(track);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(second));
    MOQ_TEST_CHECK(!moq_sub_fill_eq(first, second));
    moq_fill_t sfill2;
    MOQ_TEST_CHECK(raw_server_next_fill(&r, &sfill2));
    settle(&r);

    /* The first fill's handle is stale: it reaches nothing. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_cancel_fill(r.sub, first, now(&r)),
                          (int)MOQ_ERR_STALE_HANDLE);
    moq_sub_fill_t none = { 0 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_sub_cancel_fill(r.sub, none, now(&r)),
                          (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moq_sub_fill_eq(moq_sub_track_fill(track), second));
    settle(&r);

    /* The second fill is untouched and completes. */
    MOQ_TEST_CHECK(moq_session_end_fill(sv, sfill2, now(&r)) == MOQ_OK);
    settle(&r);
    size_t objects = 0;
    MOQ_TEST_CHECK_EQ_INT((int)drain_fill_items(&r, &objects), (int)MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK_EQ_SIZE(objects, 1);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    rig_close(&r);
    MOQ_TEST_PASS("d22_stale_fill_handle");
}

static void test_d22_created_item_credit(void)
{
    int before = failures;
    rig_t r;
    rig_open(&r, MOQ_VERSION_DRAFT_22, false, true);
    if (failures != before || !r.sub) goto done;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    moq_sub_track_t *track = NULL;
    moq_result_t rc = subscribe_join_current(&r, &track, NULL, &fill);
    MOQ_TEST_CHECK(rc == MOQ_OK && track != NULL);
    if (rc != MOQ_OK || !track) goto done;
    MOQ_TEST_CHECK(moq_sub_tick(r.sub, now(&r)) == MOQ_OK);
    moq_sub_stats_t stats;
    rc = moq_sub_get_stats(r.sub, &stats, sizeof(stats));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) goto done;
    printf("ORACLE pre_ack_created_items=%llu\n",
           (unsigned long long)stats.fill_items_queued);
    MOQ_TEST_CHECK_EQ_U64(stats.fill_items_queued, 1);
    MOQ_TEST_CHECK_EQ_U64(stats.fill_objects, 0);
    MOQ_TEST_CHECK_EQ_U64(stats.fill_terminals, 0);
    MOQ_TEST_CHECK_EQ_U64(stats.bytes_queued, 0);
    MOQ_TEST_CHECK(moq_sub_track_get_state(track) == MOQ_SUB_TRACK_PENDING);
done:
    if (r.sp) rig_close(&r);
    if (failures == before) printf("PASS: d22_created_item_credit\n");
}

static void test_d22_foreign_fill_handle(void)
{
    rig_t rigs[2] = {0};
    moq_sub_fill_t children[2] = {{0}, {0}};
    int before = failures;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    for (unsigned i = 0; i < 2; i++) {
        rig_open(&rigs[i], MOQ_VERSION_DRAFT_22, false, true);
        if (failures != before || !rigs[i].sub) goto done;
        moq_sub_track_t *track = NULL;
        moq_result_t rc = subscribe_join_current(&rigs[i], &track, NULL, &fill);
        MOQ_TEST_CHECK(rc == MOQ_OK && track != NULL);
        if (rc != MOQ_OK || track == NULL) goto done;
        children[i] = moq_sub_track_fill(track);
        MOQ_TEST_CHECK(moq_sub_fill_is_valid(children[i]));
        moq_fill_t peer_fill;
        moq_subscription_t peer_sub;
        bool ready = raw_server_open_fill(&rigs[i], &peer_fill, &peer_sub);
        MOQ_TEST_CHECK(ready);
        if (!ready || failures != before) goto done;
        settle(&rigs[i]);
    }
    moq_result_t foreign = moq_sub_cancel_fill(rigs[1].sub, children[0], now(&rigs[1]));
    printf("ORACLE foreign_fill_handle a=%llu b=%llu result=%d\n",
           (unsigned long long)children[0]._opaque,
           (unsigned long long)children[1]._opaque, (int)foreign);
    MOQ_TEST_CHECK(foreign == MOQ_ERR_STALE_HANDLE);
    if (foreign != MOQ_ERR_STALE_HANDLE) goto done;
    for (unsigned i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(moq_sub_cancel_fill(rigs[i].sub, children[i], now(&rigs[i])) == MOQ_OK);
        settle(&rigs[i]);
        MOQ_TEST_CHECK(drain_fill_items(&rigs[i], NULL) == MOQ_FILL_LOCAL_CANCELLED);
    }
done:
    for (unsigned i = 0; i < 2; i++)
        if (rigs[i].sp) rig_close(&rigs[i]);
    if (failures == before) MOQ_TEST_PASS("d22_foreign_fill_handle");
}

/* moq_sub_track_has_queued_input counts input a poll will return and
 * nothing else: a fill's queued OBJECTs (its CREATED and TERMINAL items
 * are not input) and the live subscription's queued objects. It is false
 * once they are polled, for NULL, for a track the subscriber does not own,
 * and for a released track. */
static void test_d22_track_has_queued_input(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, true, true);
    retain(&r, 3, 3);
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    floc.relative_group_offset = 1;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, &loc, &fill), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, track));
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_has_queued_input(r.sub, track));
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(NULL, track));
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, NULL));
    /* Not one of the subscriber's tracks: compared, never read. */
    static const uint64_t elsewhere[16];
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(
        r.sub, (const moq_sub_track_t *)(const void *)elsewhere));

    /* Fill items in order: CREATED, three OBJECTs, TERMINAL. Input remains
     * until the last OBJECT is polled; the TERMINAL alone is not input. */
    moq_sub_fill_item_t it;
    size_t objects = 0;
    while (moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK) {
        bool input = moq_sub_track_has_queued_input(r.sub, track);
        if (it.kind == MOQ_SUB_FILL_OBJECT) objects++;
        if (it.kind == MOQ_SUB_FILL_CREATED || objects < 3)
            MOQ_TEST_CHECK(input);
        else
            MOQ_TEST_CHECK(!input);
        moq_sub_fill_item_cleanup(&it);
    }
    MOQ_TEST_CHECK_EQ_SIZE(objects, 3);
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, track));

    /* A live object is input until polled. */
    moq_rcbuf_t *payload = mk(&r, "live");
    moq_pub_object_cfg_t oc; moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = 4; oc.object_id = 0; oc.payload = payload;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(r.pub, r.ptrack, &oc, now(&r)) == MOQ_OK);
    moq_rcbuf_decref(payload);
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_track_has_queued_input(r.sub, track));
    moq_sub_object_t obj;
    MOQ_TEST_CHECK(moq_sub_poll_object(r.sub, &obj) == MOQ_OK);
    MOQ_TEST_CHECK(obj.track == track);
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, track));
    moq_sub_object_cleanup(&obj);

    /* Released: false, whatever the pointer once named. */
    MOQ_TEST_CHECK(moq_sub_unsubscribe(r.sub, track, now(&r)) == MOQ_OK);
    settle(&r);
    (void)moq_sub_release_track(r.sub, track);
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, track));
    rig_close(&r);
}

/* The pending fill item counts by kind: with room for one fill item, the
 * CREATED item fills the queue and the fill's one OBJECT waits pending,
 * input still to come; once that OBJECT is polled, only the TERMINAL is
 * left pending, which is not input. */
static void test_d22_pending_fill_input(void)
{
    rig_t r; rig_open(&r, MOQ_VERSION_DRAFT_22, true, false);
    moq_sub_cfg_t scfg; moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.max_fill_items = 1;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(r.sp), &r.alloc, &scfg, &r.sub) == MOQ_OK);
    retain(&r, 3, 1);
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_t floc; moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    floc.relative_group_offset = 1;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)subscribe_join_current(&r, &track, &loc, &fill), (int)MOQ_OK);
    settle(&r);
    moq_sub_fill_item_t it;
    MOQ_TEST_CHECK(moq_sub_track_has_queued_input(r.sub, track));
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK);
    MOQ_TEST_CHECK(it.kind == MOQ_SUB_FILL_CREATED);
    moq_sub_fill_item_cleanup(&it);
    /* Only the pending OBJECT is left: still input. */
    MOQ_TEST_CHECK(moq_sub_track_has_queued_input(r.sub, track));
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK);
    MOQ_TEST_CHECK(it.kind == MOQ_SUB_FILL_OBJECT);
    moq_sub_fill_item_cleanup(&it);
    /* Only the TERMINAL is left (pending or queued): not input. */
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, track));
    settle(&r);
    MOQ_TEST_CHECK(moq_sub_poll_fill(r.sub, &it, sizeof(it)) == MOQ_OK);
    MOQ_TEST_CHECK(it.kind == MOQ_SUB_FILL_TERMINAL);
    moq_sub_fill_item_cleanup(&it);
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(r.sub, track));
    rig_close(&r);
}

int main(void)
{
    test_d18_refused();
    test_d22_served();
    test_d22_failed();
    test_d22_cancelled();
    test_d22_parent_cancel();
    test_d22_stream_count();
    test_d22_requests_beyond_configured_fills();
    test_d22_stale_fill_handle();
    test_d22_foreign_fill_handle();
    test_d22_created_item_credit();
    test_d22_track_has_queued_input();
    test_d22_pending_fill_input();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
