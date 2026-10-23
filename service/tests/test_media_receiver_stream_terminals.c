/* The receiver's stream-terminal lane (moq_media_receiver_cfg_t.
 * surface_stream_terminals), over a real publisher and SimPair, on drafts
 * 16, 18 and 22.
 *
 * Invariants pinned here:
 *   - FIN, FIN with END_OF_GROUP and RESET each yield one record with the
 *     facade's values, and a record is pollable only once every Object of
 *     its track taken in before it has left the receiver;
 *   - simultaneous resets stay separate records, in order;
 *   - END_OF_TRACK is a record at its Location, after the queued media;
 *   - an overflow eviction is a RECEIVER_DROP record of the evicted groups,
 *     and settles what it evicted: records behind evicted Objects become
 *     pollable without those Objects being polled;
 *   - a full lane stops intake under FLOW_CONTROL and DROP_GROUP alike:
 *     nothing is evicted or lost, and polling records resumes it;
 *   - no eviction happens unless its record goes on the lane at once;
 *   - with the lane off, or outside an older caller's struct_size, nothing
 *     changes: no record, no stopped intake;
 *   - a refused facade-slot release keeps the track's mapping on every
 *     draft, so nothing queued behind it is lost and the track re-arms;
 *   - a track removed from the catalog still reports the stream terminal
 *     queued behind the removal, under its removed handle; the track added
 *     back is a new handle that delivers again;
 *   - an MPEG2TS track's records wait for its reorder window: a record is
 *     not pollable while the window holds an earlier Object, and the pass
 *     that releases that Object on its timer makes it pollable, with no
 *     further network input; its END_OF_TRACK goes through the window;
 *   - a subscription that completes while its window holds an Object the
 *     lane has no room to release keeps its done until the application's
 *     polls make that room: the Object arrives once, then the track
 *     re-arms.
 */
#include <moq/media_receiver.h>
#include <moq/publisher.h>
#include <moq/sim.h>

#include <stdio.h>
#include <string.h>
#ifdef MOQ_TEST_HAVE_TS
#include "ts_stream.h"
#endif

moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_free(moq_media_receiver_t *r);

static unsigned failures;
static const char *row;

static bool check(bool ok, const char *expr, int line)
{
    if (!ok) {
        failures++;
        fprintf(stderr, "FAIL %s line %d: %s\n", row, line, expr);
    }
    return ok;
}
#define CHECK(e) check((e), #e, __LINE__)
#define REQUIRE(e) do { if (!CHECK(e)) goto out; } while (0)

typedef struct {
    moq_simpair_t        *sp;
    moq_session_t        *rx;
    moq_publisher_t      *pub;
    moq_pub_track_t      *catalog, *video, *side;
    moq_media_receiver_t *r;
    moq_media_track_t    *track, *side_track;
    uint64_t              now;
    bool                  ready;
} fx_t;

static const char k_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\","
    "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const char k_vod_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\","
    "\"packaging\":\"loc\",\"isLive\":false,\"trackDuration\":5000,"
    "\"role\":\"video\",\"codec\":\"av01\"}]}";
#ifdef MOQ_TEST_HAVE_TS
static const char k_ts_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\","
    "\"packaging\":\"mpeg2ts\",\"isLive\":true,\"mimeType\":\"video/mp2t\","
    "\"mpeg2tsMode\":\"unmodified-program\",\"mpeg2tsPacketSize\":188,"
    "\"mpeg2tsProgramNumber\":1}]}";
#endif
/* An MPEG2TS "video" track and a LOC "side" track. */
static const char k_mixed_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\","
    "\"packaging\":\"mpeg2ts\",\"isLive\":true,\"mimeType\":\"video/mp2t\","
    "\"mpeg2tsMode\":\"unmodified-program\",\"mpeg2tsPacketSize\":188,"
    "\"mpeg2tsProgramNumber\":1},{\"name\":\"side\",\"packaging\":\"loc\","
    "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
#ifdef MOQ_TEST_HAVE_TS
static const char k_mixed_vod_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"video\","
    "\"packaging\":\"mpeg2ts\",\"isLive\":false,\"trackDuration\":5000,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1},"
    "{\"name\":\"side\",\"packaging\":\"loc\","
    "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
#endif
/* The catalog the next fx_setup publishes, and its reorder window. */
static const char *g_catalog = k_catalog;
#define TS_WINDOW_US 1000u

/* One step of everything except the application's receiver polls. */
static void cycle(fx_t *f)
{
    f->now++;
    CHECK(moq_simpair_advance_to(f->sp, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) >= 0);
    if (f->pub) CHECK(moq_pub_tick(f->pub, f->now) >= 0);
    if (f->r) {
        CHECK(moq_session_process_pending(f->rx, f->now) >= 0);
        moq_media_receiver_test_pump(f->r, f->rx, f->now);
    }
}

static void cycles(fx_t *f, unsigned n)
{
    for (unsigned i = 0; i < n; i++) cycle(f);
}

static void track_events(fx_t *f)
{
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.desc->name.len == 4)
            f->side_track = ev.track;
        else if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f->track = ev.track;
        else if (ev.kind == MOQ_MEDIA_CATALOG_READY) f->ready = true;
    }
}

/* `lane`: 0 off, 1 on, 2 on but outside the caller's struct_size. */
static bool fx_setup(fx_t *f, moq_version_t ver, moq_media_overflow_policy_t policy,
                     uint32_t max_objects, int lane, uint32_t max_terminals)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = ver;
    cfg.seed = 0x5713;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    if (!CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK)) return false;
    if (!CHECK(moq_simpair_start(f->sp) == MOQ_OK)) return false;
    f->rx = moq_simpair_server(f->sp);
    cycles(f, 8);
    if (!CHECK(moq_session_state(f->rx) == MOQ_SESS_ESTABLISHED)) return false;

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (!CHECK(moq_pub_create(moq_simpair_client(f->sp), moq_alloc_default(),
                              &pcfg, &f->pub) == MOQ_OK)) return false;
    static moq_bytes_t parts[] = { { (const uint8_t *)"lane", 4 },
                                   { (const uint8_t *)"test", 4 } };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->catalog) == MOQ_OK))
        return false;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->video) == MOQ_OK))
        return false;
    bool mixed = g_catalog == k_mixed_catalog;
    tcfg.track_name = MOQ_BYTES_LITERAL("side");
    if (mixed && !CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->side) == MOQ_OK))
        return false;
    moq_rcbuf_t *cat = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)g_catalog,
                     strlen(g_catalog), &cat);
    moq_pub_retained_object_t retained = {
        .object_id = 0, .payload = cat, .properties = NULL, .end_of_group = true };
    moq_pub_retained_group_cfg_t group;
    moq_pub_retained_group_cfg_init(&group);
    group.objects = &retained;
    group.object_count = 1;
    moq_result_t crc = moq_pub_set_retained_group(f->pub, f->catalog, &group);
    moq_rcbuf_decref(cat);
    if (!CHECK(crc == MOQ_OK)) return false;
    cycles(f, 8);

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    rcfg.namespace_ = tcfg.track_namespace;
    rcfg.auto_subscribe = false;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = policy;
    rcfg.overflow.max_objects = max_objects;
    rcfg.overflow.max_bytes = 1u << 20;
    rcfg.surface_stream_terminals = lane != 0;
    rcfg.mpeg2ts_reorder_window_us = TS_WINDOW_US;
    rcfg.max_stream_terminals = max_terminals;
    if (lane == 2)
        rcfg.struct_size = (uint32_t)offsetof(moq_media_receiver_cfg_t,
                                              _reserved_companion);
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    if (!CHECK(f->r != NULL)) return false;
    for (unsigned i = 0; i < 64 && !f->ready; i++) {
        cycle(f);
        track_events(f);
    }
    if (!CHECK(f->ready && f->track && (!mixed || f->side_track))) return false;
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                           .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t options;
    moq_media_receiver_track_subscribe_cfg_init_sized(&options, sizeof(options));
    options.location = &location;
    if (!CHECK(moq_media_receiver_subscribe_track(f->r, f->track, &options) == MOQ_OK))
        return false;
    if (mixed && !CHECK(moq_media_receiver_subscribe_track(f->r, f->side_track,
                                                           &options) == MOQ_OK))
        return false;
    cycles(f, 24);
    return true;
}

/* The receiver's subscriber facade is torn down through the session, as
 * the endpoint does; freeing the receiver alone would leave it behind. */
static void fx_teardown(fx_t *f)
{
    if (f->r) moq_media_receiver_test_destroy_with_session(f->r, f->rx, f->now);
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) moq_simpair_destroy(f->sp);
}

static void write_obj(fx_t *f, uint64_t g, uint64_t o, bool eog)
{
    uint8_t b[8] = { (uint8_t)g, (uint8_t)o, 1, 2, 3, 4, 5, 6 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    oc.end_of_group = eog;
    CHECK(moq_pub_write_object_ex(f->pub, f->video, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

static void end_group(fx_t *f)
{
    CHECK(moq_pub_end_group(f->pub, f->video, f->now) == MOQ_OK);
}

static moq_result_t poll_st(fx_t *f, moq_media_stream_terminal_t *st)
{
    memset(st, 0xEE, sizeof(*st));
    return moq_media_receiver_poll_stream_terminal(f->r, st, sizeof(*st));
}

static bool poll_obj(fx_t *f, uint64_t g, uint64_t o)
{
    moq_media_object_t obj;
    if (moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)) != MOQ_OK)
        return false;
    bool ok = obj.track == f->track && obj.group_id == g && obj.object_id == o;
    if (!ok)
        fprintf(stderr, "  %s: polled %llu/%llu, wanted %llu/%llu\n", row,
                (unsigned long long)obj.group_id, (unsigned long long)obj.object_id,
                (unsigned long long)g, (unsigned long long)o);
    moq_media_object_cleanup(&obj);
    return ok;
}

static unsigned drain_objects(fx_t *f)
{
    unsigned n = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)) == MOQ_OK) {
        moq_media_object_cleanup(&obj);
        n++;
    }
    return n;
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22"
         : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

/* FIN with and without END_OF_GROUP, each behind its stream's Objects. */
static void test_fin_records(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "fin_records_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    write_obj(&f, 0, 0, true);
    write_obj(&f, 0, 1, true);
    end_group(&f);
    write_obj(&f, 1, 0, false);
    end_group(&f);
    cycles(&f, 16);

    moq_media_stream_terminal_t st;
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 0, 0));
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 0, 1));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.struct_size == sizeof(st));
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN_END_OF_GROUP);
    CHECK(st.track == f.track);
    CHECK(st.group_id == 0 && st.subgroup_id == 0);
    CHECK(st.has_last_object && st.last_object_id == 1 && !st.has_cut_object);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 1, 0));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN);
    CHECK(st.group_id == 1 && st.has_last_object && st.last_object_id == 0);
    CHECK(poll_st(&f, &st) == MOQ_DONE);

    CHECK(moq_media_receiver_poll_stream_terminal(NULL, &st, sizeof(st)) == MOQ_ERR_INVAL);
    CHECK(moq_media_receiver_poll_stream_terminal(f.r, NULL, sizeof(st)) == MOQ_ERR_INVAL);
    CHECK(moq_media_receiver_poll_stream_terminal(f.r, &st,
              MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE - 1) == MOQ_ERR_ABI_MISMATCH);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Two resets, each its own record with its own code and last Object. */
static void test_reset_records(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "reset_records_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    write_obj(&f, 2, 0, false);
    write_obj(&f, 2, 1, false);
    CHECK(moq_pub_reset_group(f.pub, f.video, 0x10, f.now) == MOQ_OK);
    write_obj(&f, 3, 0, false);
    CHECK(moq_pub_reset_group(f.pub, f.video, 0x11, f.now) == MOQ_OK);
    cycles(&f, 16);
    CHECK(drain_objects(&f) == 3);
    moq_media_stream_terminal_t st;
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_RESET && st.group_id == 2);
    CHECK(st.error_code == 0x10);
    CHECK(st.has_last_object && st.last_object_id == 1);
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_RESET && st.group_id == 3);
    CHECK(st.error_code == 0x11);
    CHECK(st.has_last_object && st.last_object_id == 0);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* END_OF_TRACK, received while media is still queued, is pollable only
 * after that media. */
static void test_end_of_track_record(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "end_of_track_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    write_obj(&f, 4, 0, false);
    write_obj(&f, 4, 1, false);
    CHECK(moq_pub_end_track(f.pub, f.video, f.now) == MOQ_OK);
    cycles(&f, 16);
    moq_media_stream_terminal_t st;
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 4, 0));
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 4, 1));
    /* The stream's terminal, then the end at its Location, in either order
     * the session reported them; the end is after the media. */
    bool saw_end = false;
    unsigned records = 0;
    while (poll_st(&f, &st) == MOQ_OK) {
        records++;
        if (st.kind == MOQ_MEDIA_STREAM_END_OF_TRACK) {
            saw_end = true;
            CHECK(st.track == f.track);
            CHECK(st.group_id > 4 || (st.group_id == 4 && st.object_id >= 2));
            printf("  %s: END_OF_TRACK at %llu/%llu\n", row,
                   (unsigned long long)st.group_id, (unsigned long long)st.object_id);
        }
    }
    CHECK(saw_end);
    CHECK(records >= 1);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* DROP_GROUP with a two-Object queue, Objects never polled: group 1's
 * arrival finds the queue full and evicts group 0. The eviction is a
 * RECEIVER_DROP of group 0 ahead of group 1, and settles group 0, so its
 * FIN is pollable although its Objects were never polled. Group 1 and 2
 * stay queued, and their FINs wait for them. */
static void test_eviction_records(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "eviction_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 2, 1, 0));
    write_obj(&f, 0, 0, false);
    write_obj(&f, 0, 1, false);
    end_group(&f);
    cycles(&f, 16);
    write_obj(&f, 1, 0, false);
    end_group(&f);
    cycles(&f, 16);
    write_obj(&f, 2, 0, false);
    end_group(&f);
    cycles(&f, 16);

    moq_media_receiver_stats_t sd;
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_received == 4 && sd.objects_dropped == 2);
    CHECK(sd.groups_dropped == 1);
    moq_media_stream_terminal_t st;
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 0);
    CHECK(st.has_last_object && st.last_object_id == 1);
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP && st.track == f.track);
    CHECK(st.group_id == 0 && st.last_group_id == 0);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 1, 0));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 1);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 2, 0));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 2);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* Loss is reserved before it happens. A three-record lane holds group 0's
 * pollable FIN, unpolled, and group 1's FIN behind its queued Object; the
 * one-Object queue is full. Group 2's arrival would evict group 1, but the
 * lane has no room for both that drop and group 2's own record, so intake
 * stops instead and nothing is dropped. Polling group 0's
 * FIN makes the room: the arrival evicts, its RECEIVER_DROP goes on the
 * lane at once, ahead of the Object that caused it, and group 1's FIN,
 * settled by the eviction, becomes pollable. */
static void test_reserve_before_evict(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "reserve_before_evict_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 1, 1, 3));
    write_obj(&f, 0, 0, false);
    end_group(&f);
    cycles(&f, 16);
    CHECK(poll_obj(&f, 0, 0));
    write_obj(&f, 1, 0, false);
    end_group(&f);
    write_obj(&f, 2, 0, false);
    end_group(&f);
    cycles(&f, 32);
    moq_media_receiver_stats_t sd;
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_received == 2 && sd.objects_dropped == 0);

    moq_media_stream_terminal_t st;
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 0);
    cycles(&f, 32);
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_received == 3 && sd.objects_dropped == 1);
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 1);
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP);
    CHECK(st.group_id == 1 && st.last_group_id == 1);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 2, 0));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 2);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A two-record lane that is not polled: intake stops before a record could
 * be lost, Objects included, then resumes as records are polled. An intake
 * needs room for a record per delivery domain with queued Objects and one
 * for the Object itself: with Objects polled at once, groups 0 and 1 are
 * taken in, their FINs fill the lane, and intake stops. Every Object and
 * every record arrives, in order, with nothing dropped. */
static void test_full_lane(moq_version_t ver, moq_media_overflow_policy_t policy)
{
    char label[64];
    snprintf(label, sizeof(label), "full_lane_%s_%s", vname(ver),
             policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL ? "flow" : "drop_group");
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, policy, 64, 1, 2));
    for (uint64_t g = 0; g < 4; g++) {
        write_obj(&f, g, 0, false);
        end_group(&f);
    }
    unsigned objects = 0;
    for (unsigned i = 0; i < 32; i++) {
        cycle(&f);
        objects += drain_objects(&f);
    }
    CHECK(objects == 2);
    moq_media_receiver_stats_t stats;
    CHECK(moq_media_receiver_get_stats(f.r, &stats, sizeof(stats)) == MOQ_OK);
    CHECK(stats.objects_dropped == 0);

    moq_media_stream_terminal_t st;
    uint64_t want = 0;
    for (unsigned i = 0; i < 64 && want < 4; i++) {
        while (poll_st(&f, &st) == MOQ_OK) {
            CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == want);
            want++;
        }
        cycle(&f);
        objects += drain_objects(&f);
    }
    CHECK(want == 4);
    CHECK(objects == 4);
    CHECK(moq_media_receiver_get_stats(f.r, &stats, sizeof(stats)) == MOQ_OK);
    CHECK(stats.objects_dropped == 0);
    CHECK(stats.objects_received == 4);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* A one-record lane, the smallest: the catalog bootstraps through it, then
 * one Object at a time is taken in, each followed by its FIN. Draining
 * Objects and records resumes intake; everything arrives, in order, with
 * nothing dropped. */
static void test_one_record_lane(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "one_record_lane_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 1));
    for (uint64_t g = 0; g < 3; g++) {
        write_obj(&f, g, 0, false);
        end_group(&f);
    }
    cycles(&f, 16);
    moq_media_receiver_stats_t sd;
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_received == 1);
    moq_media_stream_terminal_t st;
    uint64_t next = 0;
    for (unsigned i = 0; i < 64 && next < 3; i++) {
        if (poll_obj(&f, next, 0)) {
            CHECK(poll_st(&f, &st) == MOQ_OK);
            CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == next);
            next++;
        }
        cycle(&f);
    }
    CHECK(next == 3);
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_received == 3 && sd.objects_dropped == 0);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

static const char k_side_catalog[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"side\","
    "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";

static void write_catalog(fx_t *f, uint64_t g, const char *json)
{
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)json, strlen(json), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.payload = pay;
    oc.end_of_group = true;
    CHECK(moq_pub_write_object_ex(f->pub, f->catalog, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    CHECK(moq_pub_end_group(f->pub, f->catalog, f->now) == MOQ_OK);
}

/* A catalog update removes the video track while the facade still holds
 * a group of it and that group's FIN behind the update. The FIN is a
 * record naming the removed handle, and the removed track's media is not
 * delivered. Adding the track back gives a new handle, which subscribes
 * and delivers again. */
static void test_removal_keeps_terminal(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "removal_keeps_terminal_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 0));
    moq_media_track_t *old = f.track;
    write_catalog(&f, 1, k_side_catalog);
    write_obj(&f, 0, 0, false);
    end_group(&f);
    bool removed = false;
    moq_media_stream_terminal_t st;
    bool saw_fin = false;
    unsigned objects = 0;
    for (unsigned i = 0; i < 32; i++) {
        cycle(&f);
        moq_media_track_event_t ev;
        while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK)
            removed |= ev.kind == MOQ_MEDIA_TRACK_REMOVED && ev.track == old;
        objects += drain_objects(&f);
        while (poll_st(&f, &st) == MOQ_OK) {
            CHECK(st.track == old && st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 0);
            saw_fin = true;
        }
    }
    CHECK(removed);
    CHECK(saw_fin);
    CHECK(objects == 0);

    f.track = NULL;
    write_catalog(&f, 2, k_catalog);
    for (unsigned i = 0; i < 32 && !f.track; i++) {
        cycle(&f);
        track_events(&f);
    }
    REQUIRE(f.track != NULL && f.track != old);
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                           .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t options;
    moq_media_receiver_track_subscribe_cfg_init_sized(&options, sizeof(options));
    options.location = &location;
    REQUIRE(moq_media_receiver_subscribe_track(f.r, f.track, &options) == MOQ_OK);
    cycles(&f, 24);
    write_obj(&f, 3, 0, false);
    end_group(&f);
    cycles(&f, 16);
    CHECK(poll_obj(&f, 3, 0));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.track == f.track && st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 3);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The lane off, or enabled outside the caller's struct_size: no record,
 * and an unpolled lane never stops intake. */
static void test_lane_off(moq_version_t ver, int lane)
{
    char label[64];
    snprintf(label, sizeof(label), "lane_off_%d_%s", lane, vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, lane, 1));
    for (uint64_t g = 0; g < 4; g++) {
        write_obj(&f, g, 0, false);
        end_group(&f);
    }
    cycles(&f, 32);
    CHECK(drain_objects(&f) == 4);
    moq_media_stream_terminal_t st;
    CHECK(poll_st(&f, &st) == MOQ_DONE);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The subscription completes (the publisher finishes its subscribers with
 * Track Ended) while the lane is unpolled and full enough to stop intake:
 * the facade still holds the last group's Object and its FIN when the done
 * is processed, so releasing the track's facade slot is refused. The
 * receiver keeps the mapping and retries, so once the lane drains every
 * Object and record arrives, in order; then the track re-arms and its
 * next subscription delivers again. */
static void test_done_with_queued_terminals(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "done_with_queued_terminals_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 1, 2));
    /* Conversion permits rearm, but cannot release the original slot while
     * the full lane still holds its last Object and FIN. */
    write_catalog(&f, 1, k_vod_catalog);
    cycles(&f, 16);
    moq_media_track_desc_t desc;
    REQUIRE(moq_media_receiver_track_desc_copy(f.r, f.track, &desc, sizeof(desc)) == MOQ_OK);
    REQUIRE(!desc.is_live && desc.has_track_duration && desc.track_duration_ms == 5000);
    for (uint64_t g = 0; g < 3; g++) {
        write_obj(&f, g, 0, false);
        end_group(&f);
    }
    CHECK(moq_pub_finish_subscribers(f.pub, f.video, MOQ_PUB_DONE_TRACK_ENDED,
                                     f.now) == MOQ_OK);
    unsigned objects = 0;
    for (unsigned i = 0; i < 32; i++) {
        cycle(&f);
        objects += drain_objects(&f);
    }
    CHECK(objects < 3);
    moq_media_stream_terminal_t st;
    uint64_t fins = 0;
    for (unsigned i = 0; i < 64 && fins < 3; i++) {
        while (poll_st(&f, &st) == MOQ_OK) {
            CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == fins);
            fins++;
        }
        cycle(&f);
        objects += drain_objects(&f);
    }
    CHECK(fins == 3);
    CHECK(objects == 3);
    /* Re-armed: the next subscription delivers the next group. */
    cycles(&f, 32);
    write_obj(&f, 3, 0, false);
    end_group(&f);
    for (unsigned i = 0; i < 32 && objects < 4; i++) {
        cycle(&f);
        objects += drain_objects(&f);
    }
    CHECK(objects == 4);
    CHECK(!moq_media_receiver_is_fatal(f.r));
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

#ifdef MOQ_TEST_HAVE_TS
static ts_gen_t g_ts;

/* An Object of the MPEG2TS track: one generated GOP. */
static void write_ts(fx_t *f, uint64_t g, uint64_t o)
{
    uint8_t buf[16 * 188];
    size_t n = ts_gen_gop(&g_ts, buf, 1);
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), buf, n, &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    CHECK(moq_pub_write_object_ex(f->pub, f->video, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

/* One pass at `t` with no network input: only the receiver runs. */
static void pump_at(fx_t *f, uint64_t t)
{
    f->now = t;
    moq_media_receiver_test_pump(f->r, f->rx, f->now);
}

static moq_media_track_state_t track_state(fx_t *f, moq_media_track_t *t)
{
    moq_media_track_state_t s = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    CHECK(moq_media_receiver_track_state(f->r, t, &s) == MOQ_OK);
    return s;
}

static void write_side(fx_t *f, uint64_t g)
{
    uint8_t b[8] = { (uint8_t)g, 0, 1, 2, 3, 4, 5, 6 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(moq_alloc_default(), b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.payload = pay;
    CHECK(moq_pub_write_object_ex(f->pub, f->side, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    CHECK(moq_pub_end_group(f->pub, f->side, f->now) == MOQ_OK);
}

static bool fx_setup_ts(fx_t *f, moq_version_t ver, const char *catalog,
                        uint32_t max_objects, uint32_t max_terminals)
{
    g_catalog = catalog;
    ts_gen_init(&g_ts, 188, 1, false);
    bool ok = fx_setup(f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, max_objects, 1,
                       max_terminals);
    g_catalog = k_catalog;
    return ok;
}

/* A reset group's Object 1 waits in the reorder window for Object 0, which
 * never comes, and the RESET's record waits with it. The pass whose timer
 * releases the Object makes the record pollable behind it, with no network
 * input after it. Then a FIN's record follows its Object, which waits for
 * group 2's end until the window gives up on it. */
static void test_ts_records(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "ts_records_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup_ts(&f, ver, k_ts_catalog, 64, 4));
    write_ts(&f, 2, 1);
    CHECK(moq_pub_reset_group(f.pub, f.video, 0x10, f.now) == MOQ_OK);
    cycles(&f, 16);
    moq_media_stream_terminal_t st;
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(!poll_obj(&f, 2, 1));
    pump_at(&f, f.now + TS_WINDOW_US + 1);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 2, 1));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_RESET && st.track == f.track);
    CHECK(st.group_id == 2 && st.error_code == 0x10);
    CHECK(st.has_last_object && st.last_object_id == 1);
    CHECK(poll_st(&f, &st) == MOQ_DONE);

    write_ts(&f, 3, 0);
    end_group(&f);
    cycles(&f, 16);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    pump_at(&f, f.now + TS_WINDOW_US + 1);
    CHECK(poll_st(&f, &st) == MOQ_DONE);
    CHECK(poll_obj(&f, 3, 0));
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.group_id == 3);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* END_OF_TRACK of a TS track takes its place in the window behind a held
 * Object, and is a record at its Location once the timer releases both. */
static void test_ts_end_of_track(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "ts_end_of_track_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup_ts(&f, ver, k_ts_catalog, 64, 4));
    write_ts(&f, 4, 0);
    CHECK(moq_pub_end_track(f.pub, f.video, f.now) == MOQ_OK);
    cycles(&f, 16);
    moq_media_stream_terminal_t st;
    bool saw_end = false;
    while (poll_st(&f, &st) == MOQ_OK)
        saw_end |= st.kind == MOQ_MEDIA_STREAM_END_OF_TRACK;
    CHECK(!saw_end);
    pump_at(&f, f.now + TS_WINDOW_US + 1);
    CHECK(poll_obj(&f, 4, 0));
    while (poll_st(&f, &st) == MOQ_OK) {
        if (st.kind != MOQ_MEDIA_STREAM_END_OF_TRACK) continue;
        saw_end = true;
        CHECK(st.track == f.track);
        CHECK(st.group_id > 4 || (st.group_id == 4 && st.object_id >= 1));
    }
    CHECK(saw_end);
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

/* The TS track's subscription completes (Track Ended) while its window
 * holds group 0's Object 1, waiting for Object 0. A one-Object queue holds the side track's
 * Object, whose FIN fills the lane next to the held Object's reserved
 * slot, so releasing the TS Object would need to evict with no room for
 * the drop: the window waits, and the done waits with it. Polling the side
 * Object lets one pass, with no network input, release the TS Object into
 * the empty queue and retire the subscription; nothing is dropped, the
 * Object arrives once, and the re-armed track delivers again. */
static void test_ts_done_waits_for_room(moq_version_t ver)
{
    char label[64];
    snprintf(label, sizeof(label), "ts_done_waits_for_room_%s", vname(ver));
    row = label;
    unsigned before = failures;
    fx_t f;
    REQUIRE(fx_setup_ts(&f, ver, k_mixed_catalog, 1, 2));
    write_ts(&f, 0, 1);
    cycles(&f, 8);
    /* The conversion arrives while the ORIGINAL TS window holds Object 1.
     * Its later done must still wait for lane room before any replacement. */
    write_catalog(&f, 1, k_mixed_vod_catalog);
    cycles(&f, 8);
    moq_media_track_desc_t desc;
    REQUIRE(moq_media_receiver_track_desc_copy(f.r, f.track, &desc, sizeof(desc)) == MOQ_OK);
    REQUIRE(!desc.is_live && desc.has_track_duration && desc.track_duration_ms == 5000);
    write_side(&f, 7);
    cycles(&f, 8);
    CHECK(moq_pub_finish_subscribers(f.pub, f.video, MOQ_PUB_DONE_TRACK_ENDED,
                                     f.now) == MOQ_OK);
    cycles(&f, 16);
    pump_at(&f, f.now + TS_WINDOW_US + 1);
    CHECK(moq_simpair_run_until_quiescent(f.sp, 64, NULL) >= 0);
    pump_at(&f, f.now + 1);

    moq_media_receiver_stats_t sd;
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_dropped == 0);
    moq_media_object_t obj;
    REQUIRE(moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) == MOQ_OK);
    CHECK(obj.track == f.side_track && obj.group_id == 7);
    moq_media_object_cleanup(&obj);
    CHECK(moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) != MOQ_OK);
    CHECK(track_state(&f, f.track) == MOQ_MEDIA_TRACK_STATE_ACTIVE);

    pump_at(&f, f.now + 1);
    CHECK(poll_obj(&f, 0, 1));
    CHECK(moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) != MOQ_OK);
    moq_media_stream_terminal_t st;
    REQUIRE(poll_st(&f, &st) == MOQ_OK);
    CHECK(st.kind == MOQ_MEDIA_STREAM_FIN && st.track == f.side_track);
    CHECK(moq_media_receiver_get_stats(f.r, &sd, sizeof(sd)) == MOQ_OK);
    CHECK(sd.objects_dropped == 0);
    CHECK(track_state(&f, f.track) != MOQ_MEDIA_TRACK_STATE_ACTIVE);

    /* Re-armed: the next subscription delivers the next group. */
    cycles(&f, 32);
    write_ts(&f, 1, 0);
    end_group(&f);
    unsigned objects = 0;
    for (unsigned i = 0; i < 32 && objects == 0; i++) {
        cycle(&f);
        while (poll_st(&f, &st) == MOQ_OK) {}
        objects += drain_objects(&f);
    }
    CHECK(objects == 1);
    CHECK(!moq_media_receiver_is_fatal(f.r));
out:
    fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}
#endif

int main(void)
{
    const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                   MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < 3; i++) {
        test_fin_records(vers[i]);
        test_reset_records(vers[i]);
        test_end_of_track_record(vers[i]);
        test_eviction_records(vers[i]);
        test_full_lane(vers[i], MOQ_MEDIA_OVERFLOW_FLOW_CONTROL);
        test_full_lane(vers[i], MOQ_MEDIA_OVERFLOW_DROP_GROUP);
        test_reserve_before_evict(vers[i]);
        test_one_record_lane(vers[i]);
        test_done_with_queued_terminals(vers[i]);
        test_removal_keeps_terminal(vers[i]);
        test_lane_off(vers[i], 0);
        test_lane_off(vers[i], 2);
#ifdef MOQ_TEST_HAVE_TS
        test_ts_records(vers[i]);
        test_ts_end_of_track(vers[i]);
        test_ts_done_waits_for_room(vers[i]);
#endif
    }
    if (failures) {
        printf("FAIL: %u receiver stream-terminal failures\n", failures);
        return 1;
    }
    printf("PASS: all receiver stream-terminal tests\n");
    return 0;
}
