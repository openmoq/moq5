#include <moq/media_receiver.h>
#include <stddef.h>

#ifdef __cplusplus
#define ACK_ASSERT static_assert
#define ACK_ALIGNOF alignof
#else
#define ACK_ASSERT _Static_assert
#define ACK_ALIGNOF _Alignof
#endif

/* Independent allocation through parse_drops_delta, including all earlier
 * appended fields. */
typedef struct old_track_event {
    uint32_t struct_size;
    moq_media_track_event_kind_t kind;
    moq_media_track_t *track;
    const moq_media_track_desc_t *desc;
    uint32_t config_generation;
    uint8_t reserved[4];
    bool has_largest;
    uint64_t largest_group, largest_object;
    bool has_expires;
    uint64_t expires_ms;
    moq_media_parse_drop_class_t parse_drop_class;
    uint64_t parse_drops_total, parse_drops_delta;
} old_track_event_t;

typedef struct expected_track_event {
    old_track_event_t old;
    int32_t local_result;
    uint32_t reserved;
} expected_track_event_t;
typedef struct u64_member_alignment { uint64_t value; } u64_member_alignment_t;

ACK_ASSERT(MOQ_MEDIA_TRACK_ADDED == 1 && MOQ_MEDIA_TRACK_UPDATED == 2 &&
           MOQ_MEDIA_TRACK_REMOVED == 3 && MOQ_MEDIA_TRACK_ENDED == 4 &&
           MOQ_MEDIA_CATALOG_READY == 5 && MOQ_MEDIA_TRACK_UPDATE_OK == 6 &&
           MOQ_MEDIA_TRACK_PARSE_DROP == 7,
               "old event values unchanged");
ACK_ASSERT(MOQ_MEDIA_TRACK_STATE_DISCOVERED == 0 && MOQ_MEDIA_TRACK_STATE_PENDING == 1 &&
           MOQ_MEDIA_TRACK_STATE_ACTIVE == 2 && MOQ_MEDIA_TRACK_STATE_PAUSED_APP == 3 &&
           MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW == 4 && MOQ_MEDIA_TRACK_STATE_ENDED == 5,
               "old state values unchanged");
#define ACK_OLD_FIELD(field) \
    ACK_ASSERT(offsetof(moq_media_track_event_t, field) == \
               offsetof(old_track_event_t, field), "unchanged " #field)
ACK_OLD_FIELD(struct_size);
ACK_OLD_FIELD(kind);
ACK_OLD_FIELD(track);
ACK_OLD_FIELD(desc);
ACK_OLD_FIELD(config_generation);
ACK_OLD_FIELD(has_largest);
ACK_OLD_FIELD(largest_group);
ACK_OLD_FIELD(largest_object);
ACK_OLD_FIELD(has_expires);
ACK_OLD_FIELD(expires_ms);
ACK_OLD_FIELD(parse_drop_class);
ACK_OLD_FIELD(parse_drops_total);
#undef ACK_OLD_FIELD
ACK_ASSERT(offsetof(moq_media_track_event_t, parse_drops_delta) ==
               offsetof(old_track_event_t, parse_drops_delta), "old last field unchanged");
ACK_ASSERT(sizeof(old_track_event_t) == (sizeof(void *) == 8 ? 96u :
               (ACK_ALIGNOF(u64_member_alignment_t) == 8 ? 88u : 76u)), "independent old extent");
#ifndef MOQ_RECEIVER_ACK_FAILURE_BASELINE
ACK_ASSERT(MOQ_MEDIA_TRACK_LOCAL_FAILURE == 8 && MOQ_MEDIA_TRACK_STATE_FAILED == 6,
               "additive failure values");
ACK_ASSERT(offsetof(moq_media_track_event_t, local_result) == sizeof(old_track_event_t),
               "failure tail starts beyond complete old allocation");
ACK_ASSERT(sizeof(((moq_media_track_event_t *)0)->local_result) == 4,
               "fixed signed local result");
#ifdef __cplusplus
ACK_ASSERT((decltype(((moq_media_track_event_t *)0)->local_result))(-1) < 0,
           "local result is signed");
#else
ACK_ASSERT(_Generic(((moq_media_track_event_t *)0)->local_result, int32_t: 1, default: 0),
           "local result is int32_t");
#endif
ACK_ASSERT(offsetof(moq_media_track_event_t, _reserved_local_failure) ==
           sizeof(old_track_event_t) + sizeof(int32_t), "reserved32 follows result");
ACK_ASSERT(offsetof(moq_media_track_event_t, initiating_request_id) ==
               sizeof(expected_track_event_t),
               "fill tail preserves the complete local-failure allocation");
#endif

#ifndef MOQ_RECEIVER_ACK_FAILURE_LAYOUT_ONLY
#include <moq/subscriber.h>
#include <moq/sim.h>
#include "test_support.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/* Only configuration limits are fault-injected. Every ACK, callback, query,
 * unsubscribe and release below runs the real facade/session implementation. */
static moq_result_t observed_create(moq_session_t *, const moq_alloc_t *,
    const moq_sub_cfg_t *, moq_subscriber_t **);
static moq_result_t observed_tick(moq_subscriber_t *, uint64_t);
static moq_result_t observed_unsubscribe(moq_subscriber_t *, moq_sub_track_t *, uint64_t);
static moq_result_t observed_release(moq_subscriber_t *, moq_sub_track_t *);
static void observed_wake(moq_endpoint_t *);
#define moq_sub_create observed_create
#define moq_sub_tick observed_tick
#define moq_sub_unsubscribe observed_unsubscribe
#define moq_sub_release_track observed_release
#define moq_endpoint_wake observed_wake
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#undef moq_sub_create
#undef moq_sub_tick
#undef moq_sub_unsubscribe
#undef moq_sub_release_track
#undef moq_endpoint_wake

moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *);

static int failures;
enum { LOCAL_FAILURE = 8, STATE_FAILED = 6 };
static const uint8_t properties[] = {0x78, 0x01, 0x01, 0x03, 0xaa, 0x00, 0xbb};
static const uint8_t properties16[] = {0x40, 0x78, 0x01, 0x01, 0x03, 0xaa, 0x00, 0xbb};
static const char catalog[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"bad\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"good\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const char vod_catalog[] = "{\"version\":1,\"generatedAt\":2,\"tracks\":["
    "{\"name\":\"bad\",\"packaging\":\"loc\",\"isLive\":false,\"trackDuration\":100,\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"good\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";

typedef struct ledger { size_t calls, blocks, bytes; bool fail; } ledger_t;
typedef struct peer_track {
    moq_subscription_t sub;
    moq_fill_t fill;
    unsigned requests;
} peer_track_t;
typedef struct fixture {
    ledger_t ledger;
    moq_simpair_t *pair;
    moq_media_receiver_t *r;
    moq_endpoint_t *ep;
    moq_version_t version;
    peer_track_t peer[3]; /* catalog, bad, good */
    moq_media_track_t *bad, *good;
    uint64_t facade_budget;
    bool oversized_ack;
    uint32_t facade_objects;
    moq_result_t tick_result, query_result, unsubscribe_result;
    moq_sub_track_t *probe;
    unsigned unsubscribes, releases, locals, ends, added, wakes;
} fixture_t;
static fixture_t *current;

static void *ledger_alloc(size_t n, void *ctx)
{
    ledger_t *l = ctx;
    l->calls++;
    if (l->fail) return NULL;
    void *p = malloc(n);
    if (p) { l->blocks++; l->bytes += n; }
    return p;
}
static void ledger_free(void *p, size_t n, void *ctx)
{
    ledger_t *l = ctx;
    if (p) {
        MOQ_TEST_CHECK(l->blocks > 0 && l->bytes >= n);
        l->blocks--; l->bytes -= n;
    }
    free(p);
}
static moq_result_t observed_create(moq_session_t *s, const moq_alloc_t *a,
    const moq_sub_cfg_t *cfg, moq_subscriber_t **out)
{
    moq_sub_cfg_t c = *cfg;
    c.max_queued_object_bytes = current->facade_budget;
    if (current->facade_objects) c.max_objects = current->facade_objects;
    return moq_sub_create(s, a, &c, out);
}
static moq_result_t observed_tick(moq_subscriber_t *s, uint64_t now)
{
    moq_result_t rc = moq_sub_tick(s, now);
    current->tick_result = rc;
    if (current->probe) {
        moq_sub_track_result_t result;
        current->query_result = moq_sub_get_track_result(s, current->probe, &result, sizeof(result));
    }
    return rc;
}
static moq_result_t observed_unsubscribe(moq_subscriber_t *s, moq_sub_track_t *t, uint64_t now)
{
    moq_result_t rc = moq_sub_unsubscribe(s, t, now);
    if (t == current->probe) {
        current->unsubscribes++;
        current->unsubscribe_result = rc;
    }
    return rc;
}
static moq_result_t observed_release(moq_subscriber_t *s, moq_sub_track_t *t)
{
    if (t == current->probe) {
        current->releases++;
        MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(t)));
        MOQ_TEST_CHECK(moq_sub_track_get_state(t) == MOQ_SUB_TRACK_DONE ||
                       moq_sub_track_get_state(t) == MOQ_SUB_TRACK_ERROR);
    }
    return moq_sub_release_track(s, t);
}
static void observed_wake(moq_endpoint_t *ep)
{
    current->wakes++;
    moq_endpoint_wake(ep);
}
static uint64_t now(fixture_t *f) { return moq_simpair_now_us(f->pair); }
static moq_session_t *client(fixture_t *f) { return moq_simpair_client(f->pair); }
static moq_session_t *server(fixture_t *f) { return moq_simpair_server(f->pair); }
static void network(fixture_t *f)
{
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    moq_event_t ev;
    while (moq_session_poll_events(server(f), &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_bytes_t name = ev.u.subscribe_request.track_name;
            unsigned i = name.len == 3 && memcmp(name.data, "bad", 3) == 0 ? 1u :
                         name.len == 4 && memcmp(name.data, "good", 4) == 0 ? 2u : 0u;
            f->peer[i].sub = ev.u.subscribe_request.sub;
            f->peer[i].requests++;
        } else if (ev.kind == MOQ_EVENT_FILL_REQUEST) {
            for (unsigned i = 0; i < 3; i++)
                if (moq_subscription_eq(f->peer[i].sub, ev.u.fill_request.sub))
                    f->peer[i].fill = ev.u.fill_request.fill;
        }
        moq_event_cleanup(&ev);
    }
}
static void pump(fixture_t *f)
{
    moq_media_receiver_test_pump(f->r, client(f), now(f));
}
static void settle(fixture_t *f)
{
    for (unsigned i = 0; i < 8; i++) { network(f); pump(f); }
}
static bool start_configured(fixture_t *f, moq_version_t version, uint64_t budget,
                             bool small_flow_queue)
{
    memset(f, 0, sizeof(*f)); current = f;
    f->version = version; f->facade_budget = budget;
    /* D22 delivery admission also enforces this budget. Leave room for the
     * catalog, then exceed it with the ACK itself to isolate the ACK failure. */
    if (version == MOQ_VERSION_DRAFT_22 && budget == 1) {
        f->facade_budget = 1024;
        f->oversized_ack = true;
    }
    f->facade_objects = 1;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default(); pc.version = version;
    pc.max_actions = 8; /* No more than the track-status table: pressure fills actions. */
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    if (moq_simpair_create(&pc, &f->pair) != MOQ_OK ||
        moq_simpair_start(f->pair) != MOQ_OK ||
        moq_simpair_run_until_quiescent(f->pair, 64, NULL) != MOQ_OK) return false;
    moq_session_t *ends[] = {client(f), server(f)};
    for (unsigned i = 0; i < 2; i++) {
        if (moq_session_state(ends[i]) != MOQ_SESS_ESTABLISHED) return false;
        moq_event_t ev;
        while (moq_session_poll_events(ends[i], &ev, 1)) moq_event_cleanup(&ev);
    }
    moq_alloc_t alloc = {.ctx = &f->ledger, .alloc = ledger_alloc, .free = ledger_free};
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    if (small_flow_queue) {
        cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
        cfg.overflow.max_objects = 2;
    }
    moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("ack-failure")};
    cfg.namespace_ = (moq_namespace_t){ns, 1};
    cfg.auto_subscribe = true;
    cfg.max_track_events = 3; /* Two discoveries + READY fill the ordinary queue. */
    cfg.companion_profile = version == MOQ_VERSION_DRAFT_22
        ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    moq_media_receiver_test_set_alloc(&alloc);
    f->r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    f->ep = moq_endpoint_test_make_bare();
    if (!f->r || !f->ep) return false;
    f->r->ep = f->ep;
    pump(f); network(f);
    return f->peer[0].requests == 1 && !moq_media_receiver_is_fatal(f->r);
}
static bool start(fixture_t *f, moq_version_t version, uint64_t budget)
{
    return start_configured(f, version, budget, false);
}
static void finish(fixture_t *f)
{
    f->ledger.fail = false; f->probe = NULL;
    if (f->pair) network(f);
    if (f->r) {
        f->r->ep = NULL;
        moq_media_receiver_test_destroy_with_session(f->r,
            f->pair ? client(f) : NULL, f->pair ? now(f) : 0);
    }
    if (f->ep) moq_endpoint_test_free_bare(f->ep);
    if (f->pair) moq_simpair_destroy(f->pair);
    MOQ_TEST_CHECK(f->ledger.blocks == 0 && f->ledger.bytes == 0);
    current = NULL;
}
static void accept(fixture_t *f, unsigned i, bool props)
{
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 0; ac.largest_object = 0;
    if (props)
        ac.track_properties = f->version == MOQ_VERSION_DRAFT_16
            ? (moq_bytes_t){properties16, sizeof(properties16)}
            : (moq_bytes_t){properties, sizeof(properties)};
    moq_rcbuf_t *large_ack = NULL;
    if (props && f->oversized_ack) {
        uint8_t config[1025] = {0};
        moq_loc_metadata_t metadata;
        moq_loc_metadata_init_sized(&metadata, sizeof(metadata));
        metadata.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        metadata.video_config = (moq_bytes_t){config, sizeof(config)};
        MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), f->version,
            MOQ_LOC_PROFILE_04, &metadata, &large_ack) == MOQ_OK);
        ac.track_properties = (moq_bytes_t){moq_rcbuf_data(large_ack), moq_rcbuf_len(large_ack)};
    }
    MOQ_TEST_CHECK(moq_session_accept_subscribe(server(f), f->peer[i].sub, &ac, now(f)) == MOQ_OK);
    moq_rcbuf_decref(large_ack);
    network(f);
}
static void send_payload(fixture_t *f, unsigned i, uint64_t group,
                         const char *data, size_t len, bool media)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc); sc.group_id = group; sc.object_properties = media;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(server(f), f->peer[i].sub, &sc, now(f), &sg) == MOQ_OK);
    moq_object_cfg_t oc;
    moq_object_cfg_init(&oc);
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)data, len, &oc.payload) == MOQ_OK);
    if (media) {
        moq_loc_metadata_t loc;
        moq_loc_metadata_init_sized(&loc, sizeof(loc));
        loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
        loc.timestamp = 12345; loc.marking_len = 1; loc.marking[0] = 0x20;
        MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), f->version,
            f->version == MOQ_VERSION_DRAFT_22 ? MOQ_LOC_PROFILE_04 : MOQ_LOC_PROFILE_01,
            &loc, &oc.properties) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_write_object_ex(server(f), sg, &oc, now(f)) == MOQ_OK);
    if (oc.payload) moq_rcbuf_decref(oc.payload);
    if (oc.properties) moq_rcbuf_decref(oc.properties);
    MOQ_TEST_CHECK(moq_session_close_subgroup(server(f), sg, now(f)) == MOQ_OK);
    network(f);
}
static bool bootstrap(fixture_t *f)
{
    if (f->version == MOQ_VERSION_DRAFT_22) {
        /* Start an actually empty catalog, then publish its first live base. */
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(server(f), f->peer[0].sub,
            &ac, now(f)) == MOQ_OK);
        network(f);
    } else {
        accept(f, 0, false);
    }
    pump(f);
    send_payload(f, 0, 0, catalog, sizeof(catalog) - 1, false);
    pump(f); network(f);
    if (f->r->track_count != 2 || f->peer[1].requests != 1 || f->peer[2].requests != 1)
        return false;
    f->bad = f->r->tracks[0]; f->good = f->r->tracks[1];
    f->probe = f->bad->sub_track;
    return f->probe != NULL;
}
static void drain_events(fixture_t *f, moq_result_t expected)
{
    expected_track_event_t ev;
    for (unsigned i = 0; i < 16; i++) {
        memset(&ev, 0xa5, sizeof(ev));
        moq_result_t rc = moq_media_receiver_poll_track(f->r, (moq_media_track_event_t *)&ev, sizeof(ev));
        if (rc != MOQ_OK) break;
        if (ev.old.kind == MOQ_MEDIA_TRACK_ADDED) f->added++;
        if (ev.old.kind == MOQ_MEDIA_TRACK_ENDED) f->ends++;
        if ((unsigned)ev.old.kind == LOCAL_FAILURE) {
            f->locals++;
            MOQ_TEST_CHECK(f->added == 2);
            MOQ_TEST_CHECK(ev.old.track == f->bad && ev.old.desc == &f->bad->desc);
            MOQ_TEST_CHECK(ev.old.struct_size == sizeof(ev));
            MOQ_TEST_CHECK(ev.local_result == expected && ev.reserved == 0);
        } else if (ev.old.struct_size >= sizeof(ev)) {
            MOQ_TEST_CHECK(ev.local_result == 0 && ev.reserved == 0);
        }
    }
}
static void expect_failed(fixture_t *f)
{
    moq_media_track_state_t state = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(f->r, f->bad, &state) == MOQ_OK);
    MOQ_TEST_CHECK((unsigned)state == STATE_FAILED);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f->r, f->bad, NULL) == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(f->r, f->bad) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(f->r, f->bad) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_track_desc_get(f->bad) == &f->bad->desc);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f->r));
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(f->ep) && !moq_endpoint_is_fatal(f->ep));
    MOQ_TEST_CHECK(moq_session_state(client(f)) == MOQ_SESS_ESTABLISHED);
}
static void fail_ack(fixture_t *f, moq_result_t reason)
{
    accept(f, 1, true);
    size_t calls = f->ledger.calls;
    unsigned wakes = f->wakes;
    f->ledger.fail = reason == MOQ_ERR_NOMEM;
    pump(f);
    f->ledger.fail = false;
    MOQ_TEST_CHECK(f->tick_result == reason && f->query_result == reason);
    MOQ_TEST_CHECK(f->ledger.calls - calls == (reason == MOQ_ERR_NOMEM ? 1u : 0u));
    MOQ_TEST_CHECK(f->wakes == wakes + 1);
    MOQ_TEST_CHECK(!f->bad->established); /* No fabricated success callback. */
}
static void test_media(moq_version_t version, moq_result_t reason)
{
    fixture_t f;
    bool ready = start(&f, version, reason == MOQ_ERR_BUFFER ? 1 : 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    fail_ack(&f, reason);
    MOQ_TEST_CHECK(f.r->stats.objects_received == 0);
    MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_OK);
    drain_events(&f, reason);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0);
    expect_failed(&f);
    accept(&f, 2, false); settle(&f);
    MOQ_TEST_CHECK(f.good->established);
    send_payload(&f, 2, 1, "good", 4, true); settle(&f);
    moq_media_object_t object;
    moq_result_t rc = moq_media_receiver_poll_object(f.r, &object, sizeof(object));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(object.track == f.good && object.payload.len == 4);
        MOQ_TEST_CHECK(memcmp(object.payload.data, "good", 4) == 0);
        MOQ_TEST_CHECK(object.has_capture_time && object.capture_time_us == 12345);
        moq_media_object_cleanup(&object);
    }
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    send_payload(&f, 0, 1, vod_catalog, sizeof(vod_catalog) - 1, false);
    settle(&f); drain_events(&f, reason);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0 && f.peer[1].requests == 1);
    expect_failed(&f);
    finish(&f);
    MOQ_TEST_PASS("media_ack_failure_identity_progress_and_no_rearm");
}
static void fill_action_queue(fixture_t *f)
{
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("pressure")};
    cfg.track_namespace = (moq_namespace_t){ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("unused");
    moq_result_t rc = MOQ_OK;
    for (unsigned i = 0; i < 32 && rc == MOQ_OK; i++) {
        moq_track_status_handle_t handle;
        rc = moq_session_track_status(client(f), &cfg, now(f), &handle);
    }
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_BUFFER);
}
static void test_retry(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    accept(&f, 1, true); fill_action_queue(&f);
    f.ledger.fail = true; pump(&f); f.ledger.fail = false;
    MOQ_TEST_CHECK(f.query_result == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.unsubscribes > 0 &&
        (f.unsubscribe_result == MOQ_ERR_WOULD_BLOCK || f.unsubscribe_result == MOQ_ERR_BUFFER));
    MOQ_TEST_CHECK(f.bad->sub_track == f.probe && f.releases == 0);
    drain_events(&f, MOQ_ERR_NOMEM); expect_failed(&f);
    settle(&f);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    drain_events(&f, MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0);
    finish(&f);
    MOQ_TEST_PASS("failed_unsubscribe_retains_handle_until_retry_and_child_drain");
}
static void test_terminal(moq_version_t version, uint64_t status)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    drain_events(&f, MOQ_OK);
    accept(&f, 1, true);
    /* Retain a real failed ACK, then let the service's next tick see terminal
     * success. This isolates the query requirement from tick's return value. */
    f.ledger.fail = true;
    MOQ_TEST_CHECK(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_NOMEM);
    f.ledger.fail = false;
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc); dc.status_code = status;
    MOQ_TEST_CHECK(moq_session_done_subscribe(server(&f), f.peer[1].sub, &dc, now(&f)) == MOQ_OK);
    network(&f); pump(&f);
    MOQ_TEST_CHECK(f.tick_result == MOQ_OK && f.query_result == MOQ_ERR_NOMEM);
    drain_events(&f, MOQ_ERR_NOMEM); settle(&f); drain_events(&f, MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0 && f.peer[1].requests == 1);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    expect_failed(&f); finish(&f);
    MOQ_TEST_PASS("terminal_tick_success_cannot_mask_ack_failure_or_rearm");
}
static void test_catalog(moq_version_t version, moq_result_t reason)
{
    fixture_t f;
    bool ready = start(&f, version, reason == MOQ_ERR_BUFFER ? 1 : 0);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    f.probe = f.r->catalog_sub;
    accept(&f, 0, true); fill_action_queue(&f);
    f.ledger.fail = reason == MOQ_ERR_NOMEM;
    pump(&f); f.ledger.fail = false;
    MOQ_TEST_CHECK(f.query_result == reason);
    MOQ_TEST_CHECK(moq_media_receiver_is_fatal(f.r));
    MOQ_TEST_CHECK(moq_media_receiver_fatal_code(f.r) == MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
    MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_ERR_CLOSED);
    MOQ_TEST_CHECK(f.r->catalog_sub == f.probe && f.releases == 0);
    settle(&f);
    MOQ_TEST_CHECK(f.r->catalog_sub == NULL && f.releases == 1);
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(f.ep) && !moq_endpoint_is_fatal(f.ep));
    MOQ_TEST_CHECK(moq_session_state(client(&f)) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(f.r->track_count == 0);
    finish(&f);
    MOQ_TEST_PASS("catalog_ack_failure_receiver_local_with_executor_retry");
}
static void test_controls(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    moq_sub_track_result_t result;
    MOQ_TEST_CHECK(moq_sub_get_track_result(f.r->sub, f.probe, &result, sizeof(result)) == MOQ_OK);
    MOQ_TEST_CHECK(!(result.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN));
    pump(&f); drain_events(&f, MOQ_OK);
    moq_media_track_state_t state;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(f.r, f.bad, &state) == MOQ_OK);
    MOQ_TEST_CHECK(state == MOQ_MEDIA_TRACK_STATE_PENDING && f.locals == 0);
    accept(&f, 1, false);
    size_t calls = f.ledger.calls;
    f.ledger.fail = true; pump(&f); f.ledger.fail = false;
    MOQ_TEST_CHECK(f.ledger.calls == calls);
    MOQ_TEST_CHECK(moq_sub_get_track_result(f.r->sub, f.probe, &result, sizeof(result)) == MOQ_OK);
    MOQ_TEST_CHECK(result.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN);
    MOQ_TEST_CHECK(result.track_properties.len == 0 && result.track_properties.data == NULL);
    MOQ_TEST_CHECK(moq_media_receiver_track_state(f.r, f.bad, &state) == MOQ_OK);
    MOQ_TEST_CHECK(state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    accept(&f, 2, true); settle(&f); drain_events(&f, MOQ_OK);
    MOQ_TEST_CHECK(f.locals == 0 && f.ends == 0 && f.good->established);
    finish(&f);
    MOQ_TEST_PASS("no_ack_empty_ack_and_retained_nonempty_ack_distinct");
}

static void test_backpressure(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    accept(&f, 1, true);
    f.ledger.fail = true;
    MOQ_TEST_CHECK(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_NOMEM);
    f.ledger.fail = false;
    accept(&f, 2, false);
    send_payload(&f, 2, 1, "one", 3, true);
    send_payload(&f, 2, 2, "two", 3, true);
    pump(&f);
    MOQ_TEST_CHECK(f.tick_result == MOQ_ERR_WOULD_BLOCK && f.query_result == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.bad->sub_track == f.probe && f.releases == 0);
    drain_events(&f, MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0);
    settle(&f);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    for (unsigned i = 0; i < 2; i++) {
        moq_media_object_t object;
        moq_result_t rc = moq_media_receiver_poll_object(f.r, &object, sizeof(object));
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            MOQ_TEST_CHECK(object.track == f.good && object.group_id == i + 1);
            MOQ_TEST_CHECK(object.payload.len == 3);
            moq_media_object_cleanup(&object);
        }
    }
    expect_failed(&f); finish(&f);
    MOQ_TEST_PASS("queue_backpressure_masks_tick_not_query_and_keeps_pending_borrows");
}

static void test_fill_intake(void)
{
    fixture_t f;
    bool ready = start(&f, MOQ_VERSION_DRAFT_22, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    accept(&f, 1, true);
    f.ledger.fail = true;
    MOQ_TEST_CHECK(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_NOMEM);
    f.ledger.fail = false;
    MOQ_TEST_CHECK(moq_fill_is_valid(f.peer[1].fill));
    MOQ_TEST_CHECK(moq_session_open_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP; loc.timestamp = 12345;
    MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), f.version,
        MOQ_LOC_PROFILE_04, &loc, &oc.properties) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"fill", 4, &oc.payload) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_fill_object(server(&f), f.peer[1].fill, &oc, now(&f)) == MOQ_OK);
    if (oc.payload) moq_rcbuf_decref(oc.payload);
    if (oc.properties) moq_rcbuf_decref(oc.properties);
    network(&f); pump(&f);
    MOQ_TEST_CHECK(f.query_result == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.bad->sub_track == f.probe && f.releases == 0);
    MOQ_TEST_CHECK(f.r->stats.objects_received == 0 && f.r->stats.parse_drops == 0);
    moq_media_object_t object;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_DONE);
    drain_events(&f, MOQ_ERR_NOMEM); settle(&f); drain_events(&f, MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0);
    expect_failed(&f); finish(&f);
    MOQ_TEST_PASS("failed_ack_precedes_fill_conversion_and_open_child_retirement");
}

static void test_live_intake(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    accept(&f, 1, true);
    f.ledger.fail = true;
    MOQ_TEST_CHECK(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_NOMEM);
    f.ledger.fail = false;
    send_payload(&f, 1, 1, "bad", 3, true);
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg); cfg.group_id = 2;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(server(&f), f.peer[1].sub, &cfg, now(&f), &sg) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_status_object(server(&f), sg, 0, MOQ_OBJECT_END_OF_TRACK, now(&f)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_close_subgroup(server(&f), sg, now(&f)) == MOQ_OK);
    network(&f); pump(&f);
    drain_events(&f, MOQ_ERR_NOMEM); settle(&f); drain_events(&f, MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(f.locals == 1 && f.ends == 0);
    MOQ_TEST_CHECK(f.r->stats.objects_received == 0 && f.r->stats.parse_drops == 0);
    moq_media_object_t object;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_DONE);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    expect_failed(&f); finish(&f);
    MOQ_TEST_PASS("failed_ack_precedes_live_conversion_and_end_of_track_projection");
}
static void test_sizes(void)
{
    const size_t floor = offsetof(old_track_event_t, config_generation) + sizeof(uint32_t);
    const size_t fixed[] = {floor, 32, offsetof(old_track_event_t, parse_drop_class)};
    for (size_t row = 0; row < 3 + sizeof(expected_track_event_t) - sizeof(old_track_event_t) + 2; row++) {
        size_t size = row < 3 ? fixed[row] : sizeof(old_track_event_t) + row - 3;
        if (row == 3 + sizeof(expected_track_event_t) - sizeof(old_track_event_t) + 1)
            size = sizeof(moq_media_track_event_t) + 17; /* Future caller. */
        fixture_t f;
        bool ready = start(&f, MOQ_VERSION_DRAFT_22, 0) && bootstrap(&f);
        MOQ_TEST_CHECK(ready);
        if (!ready) { finish(&f); continue; }
        drain_events(&f, MOQ_OK);
        fail_ack(&f, MOQ_ERR_NOMEM);
        /* Only the pending local observation can make wait ready now. */
        MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_OK);
        unsigned char *out = malloc(size + 16);
        MOQ_TEST_CHECK(out != NULL);
        if (out) {
            memset(out, 0xa5, size + 16);
            MOQ_TEST_CHECK(moq_media_receiver_poll_track(f.r,
                (moq_media_track_event_t *)out, floor - 1) == MOQ_ERR_INVAL);
            for (size_t j = 0; j < size + 16; j++) MOQ_TEST_CHECK(out[j] == 0xa5);
            MOQ_TEST_CHECK(moq_media_receiver_poll_track(f.r,
                (moq_media_track_event_t *)out, size) == MOQ_OK);
            expected_track_event_t expected;
            memset(&expected, 0, sizeof(expected));
            size_t n = size < sizeof(moq_media_track_event_t) ? size : sizeof(moq_media_track_event_t);
            expected.old.struct_size = (uint32_t)n;
            expected.old.kind = (moq_media_track_event_kind_t)LOCAL_FAILURE;
            expected.old.track = f.bad; expected.old.desc = &f.bad->desc;
            expected.local_result = MOQ_ERR_NOMEM;
            size_t prefix = n < sizeof(expected) ? n : sizeof(expected);
            MOQ_TEST_CHECK(memcmp(out, &expected, prefix) == 0);
            /* Successor event fields are present but zero for LOCAL_FAILURE. */
            for (size_t j = prefix; j < n; j++) MOQ_TEST_CHECK(out[j] == 0);
            for (size_t j = n; j < size + 16; j++) MOQ_TEST_CHECK(out[j] == 0xa5);
            free(out);
        }
        drain_events(&f, MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(f.locals == 0 && f.ends == 0); /* Already drained once. */
        finish(&f);
    }
    MOQ_TEST_PASS("old_partial_future_event_outputs_and_poisoned_padding");
}
int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (unsigned i = 0; i < 3; i++) {
        test_controls(versions[i]);
        test_media(versions[i], MOQ_ERR_NOMEM);
        test_media(versions[i], MOQ_ERR_BUFFER);
        test_retry(versions[i]);
        test_terminal(versions[i], 0);
        test_terminal(versions[i], RECEIVER_DONE_TRACK_ENDED);
        test_backpressure(versions[i]);
        test_live_intake(versions[i]);
        test_catalog(versions[i], MOQ_ERR_NOMEM);
        test_catalog(versions[i], MOQ_ERR_BUFFER);
    }
    test_fill_intake();
    test_sizes();
    fprintf(stderr, "receiver ACK failure: %d failures\n", failures);
    return failures ? 1 : 0;
}
#endif
