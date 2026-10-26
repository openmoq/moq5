/* A completed broadcast (MSF 11.3) on the receiver, over a real publisher and
 * SimPair, on drafts 16, 18 and 22.
 *
 * A terminal catalog (isComplete, empty tracks) says the broadcast is over,
 * not that its last Objects have arrived. Invariants pinned here:
 *   - a terminal catalog that overtakes a track's tail removes the track only
 *     after the tail and its END_OF_TRACK: every Object arrives once, then
 *     TRACK_ENDED, then TRACK_REMOVED, each once;
 *   - while a full stream-terminal lane stops intake, the end stays behind
 *     the queued media for as long as the application does not poll -- far
 *     past any fixed budget -- with no end or removal reported meanwhile, and
 *     nothing lost; under FLOW_CONTROL, whose soft ceiling degrades to a
 *     counted drop, every written Object is delivered or counted;
 *   - a session that fails before the tail arrives never reports the track as
 *     ended: every subscription is over with the session, so each track is
 *     removed once, and what was taken in stays pollable;
 *   - each track (a generated timeline included) is removed after its own
 *     end, independently of the others;
 *   - a track that ended before the terminal catalog is removed by it with
 *     its queued media still pollable;
 *   - a done-only live end followed by a terminal catalog settles the original
 *     handle without issuing a replacement before a validated conversion;
 *   - a confirmed VOD replacement keeps its unfinished first or later Object
 *     through a terminal catalog, until that subscription completes; even an
 *     empty or still-pending replacement waits for its own completion;
 *   - the application's unsubscribe of a track still waiting for its tail
 *     is the usual disable: its queued media is purged, nothing more of it is
 *     delivered, a late end reports no TRACK_ENDED, and the track is removed
 *     once while its siblings carry on; its subscription is retired on the
 *     wire, a release refused while the facade still holds its media is
 *     retried, and a second unsubscribe is MOQ_ERR_WRONG_STATE;
 *   - an END_OF_TRACK that overtakes its track's last Objects (it travels
 *     on its own stream) does not end the track's subscription: the
 *     receiver keeps it until the subscription's done, whose Stream Count
 *     the facade has met, so the overtaken tail is still delivered;
 *   - every allocation the receiver, the publisher and the pair make is
 *     released at teardown.
 */
#include <moq/media_receiver.h>
#include <moq/publisher.h>
#include <moq/sim.h>
#include "test_alloc_support.h"

#include <stdio.h>
#include <string.h>

moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
    moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);

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

enum { T_VIDEO, T_SIDE, T_SAP, T_N };
static const char *const k_names[T_N] = { "video", "side", "video.sap" };

static const char k_catalog[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"side\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"video.sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
    "\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\","
    "\"depends\":[\"video\"]}]}";
static const char k_terminal[] =
    "{\"version\":\"1\",\"isComplete\":true,\"tracks\":[]}";
static const char k_video_vod[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"trackDuration\":5000,\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"side\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"video.sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
    "\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\","
    "\"depends\":[\"video\"]}]}";
static const char k_half_conversion[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"side\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\"},"
    "{\"name\":\"video.sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
    "\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\","
    "\"depends\":[\"video\"]}]}";

/* One observation, in the order the application saw it. */
typedef enum { EV_OBJ, EV_ENDED, EV_REMOVED, EV_OTHER, EV_EOT } ev_kind_t;
typedef struct { ev_kind_t kind; int track; uint64_t g, o; } ev_t;

typedef struct {
    test_alloc_state_t    as;
    moq_alloc_t           alloc;
    moq_simpair_t        *sp;
    moq_session_t        *rx;
    moq_publisher_t      *pub;
    moq_pub_track_t      *catalog, *pt[T_N];
    moq_media_receiver_t *r;
    moq_media_track_t    *mt[T_N];
    uint64_t              now;
    bool                  ready;
    ev_t                  log[64];
    unsigned              n;
    bool                  overflow;
    bool                  closed;   /* the pair's session was closed */
    unsigned              forward0[T_N];
    unsigned              requested[T_N];
    unsigned              joined[T_N];
    unsigned              left[T_N]; /* peer cancellation, not publisher finish */
    bool                  has_done_status[T_N];
    uint64_t              done_status[T_N];
    /* Data-stream inputs the receiver's session took, in order: the stream
     * and whether the input carried the FIN. `phase` is the test's marker. */
    struct { uint64_t ref; bool fin; size_t len; int phase; } in[256];
    unsigned              in_n;
    int                   phase;
} fx_t;

/* Pair options for one row: faults are configured here and enabled later. */
static uint64_t g_seed = 0xC0DE;
static uint32_t g_fault_flags, g_fault_per_mille;

static void fx_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    fx_t *f = ctx;
    if (r->kind != MOQ_SIM_TRACE_INPUT || r->to != MOQ_PERSPECTIVE_SERVER ||
        r->input_kind != MOQ_SIM_INPUT_DATA_BYTES)
        return;
    if (r->struct_size < offsetof(moq_sim_trace_record_t, fin) + sizeof(r->fin))
        return;
    if (f->in_n >= sizeof(f->in) / sizeof(f->in[0])) return;
    f->in[f->in_n].ref = r->stream_ref._v;
    f->in[f->in_n].fin = r->fin;
    f->in[f->in_n].len = r->bytes.len;
    f->in[f->in_n].phase = f->phase;
    f->in_n++;
}

static int track_index(const fx_t *f, const moq_media_track_t *t)
{
    for (int i = 0; i < T_N; i++)
        if (t && f->mt[i] == t) return i;
    return -1;
}

static void note(fx_t *f, ev_kind_t k, int track, uint64_t g, uint64_t o)
{
    if (f->n >= sizeof(f->log) / sizeof(f->log[0])) { f->overflow = true; return; }
    f->log[f->n++] = (ev_t){ k, track, g, o };
}

/* One step of everything except the application's polls; `dt` advances the
 * clock. */
static void step(fx_t *f, uint64_t dt)
{
    f->now += dt;
    moq_result_t arc = moq_simpair_advance_to(f->sp, f->now);
    CHECK(f->closed || arc == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) >= 0 || f->closed);
    if (f->pub) CHECK(moq_pub_tick(f->pub, f->now) >= 0);
    if (f->r) {
        (void)moq_session_process_pending(f->rx, f->now);
        moq_media_receiver_test_pump(f->r, f->rx, f->now);
    }
}

static void steps(fx_t *f, unsigned n)
{
    for (unsigned i = 0; i < n; i++) step(f, 1);
}

static void poll_tracks(fx_t *f)
{
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) {
            for (int i = 0; i < T_N; i++)
                if (ev.desc->name.len == strlen(k_names[i]) &&
                    memcmp(ev.desc->name.data, k_names[i], ev.desc->name.len) == 0)
                    f->mt[i] = ev.track;
        } else if (ev.kind == MOQ_MEDIA_CATALOG_READY) {
            f->ready = true;
        } else {
            int track = track_index(f, ev.track);
            if (ev.kind == MOQ_MEDIA_TRACK_ENDED && track >= 0) {
                f->has_done_status[track] = ev.has_done_status;
                f->done_status[track] = ev.done_status;
            }
            note(f, ev.kind == MOQ_MEDIA_TRACK_ENDED ? EV_ENDED
                    : ev.kind == MOQ_MEDIA_TRACK_REMOVED ? EV_REMOVED : EV_OTHER,
                 track, 0, 0);
        }
    }
}

static unsigned poll_objects(fx_t *f)
{
    unsigned n = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)) == MOQ_OK) {
        const uint8_t want[] = { (uint8_t)obj.group_id, (uint8_t)obj.object_id,
                                 1, 2, 3, 4, 5, 6 };
        CHECK(obj.payload.len == sizeof(want) &&
              memcmp(obj.payload.data, want, sizeof(want)) == 0);
        note(f, EV_OBJ, track_index(f, obj.track), obj.group_id, obj.object_id);
        moq_media_object_cleanup(&obj);
        n++;
    }
    return n;
}

static void write_to(fx_t *f, moq_pub_track_t *t, uint64_t g, uint64_t o)
{
    uint8_t b[8] = { (uint8_t)g, (uint8_t)o, 1, 2, 3, 4, 5, 6 };
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&f->alloc, b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    CHECK(moq_pub_write_object_ex(f->pub, t, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

/* One SAP-timeline Object (CMSF 3.6.1): media Location g/0, SAP type 1. */
static void write_sap(fx_t *f, uint64_t g)
{
    char j[64];
    int n = snprintf(j, sizeof(j), "[{\"l\":[%llu,0],\"data\":[1,0]}]",
                     (unsigned long long)g);
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&f->alloc, (const uint8_t *)j, (size_t)n, &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.payload = pay;
    oc.end_of_group = true;
    CHECK(moq_pub_write_object_ex(f->pub, f->pt[T_SAP], &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

static void write_catalog(fx_t *f, uint64_t g, const char *json)
{
    moq_rcbuf_t *pay = NULL;
    moq_rcbuf_create(&f->alloc, (const uint8_t *)json, strlen(json), &pay);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.payload = pay;
    oc.end_of_group = true;
    CHECK(moq_pub_write_object_ex(f->pub, f->catalog, &oc, f->now) == MOQ_OK);
    moq_rcbuf_decref(pay);
    CHECK(moq_pub_end_group(f->pub, f->catalog, f->now) == MOQ_OK);
}

static void on_subscriber_updated(void *ctx, moq_pub_track_t *track,
                                  const moq_pub_subscribe_update_info_t *info)
{
    fx_t *f = ctx;
    if (!info->has_forward || info->forward) return;
    for (int i = 0; i < T_N; i++)
        if (f->pt[i] == track) f->forward0[i]++;
}

static void on_subscriber_joined(void *ctx, moq_pub_track_t *track)
{
    fx_t *f = ctx;
    for (int i = 0; i < T_N; i++)
        if (f->pt[i] == track) f->joined[i]++;
}

static moq_pub_accept_decision_t on_subscribe(void *ctx,
    const moq_pub_subscribe_info_t *info, moq_request_error_t *error)
{
    fx_t *f = ctx;
    (void)error;
    for (int i = 0; i < T_N; i++)
        if (f->pt[i] == info->track) f->requested[i]++;
    return MOQ_PUB_DECISION_ACCEPT;
}

static void on_subscriber_left(void *ctx, moq_pub_track_t *track)
{
    fx_t *f = ctx;
    for (int i = 0; i < T_N; i++)
        if (f->pt[i] == track) f->left[i]++;
}

/* `terminals`: 0 leaves the stream-terminal lane off; otherwise its size. */
static bool fx_setup_tracks(fx_t *f, moq_version_t ver,
                            moq_media_overflow_policy_t policy,
                            uint32_t max_objects, uint32_t terminals,
                            bool subscribe_side)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = ver;
    cfg.seed = g_seed;
    cfg.fault_flags = g_fault_flags;
    cfg.fault_per_mille = g_fault_per_mille;
    cfg.trace_fn = fx_trace;
    cfg.trace_ctx = f;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    if (!CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK)) return false;
    if (!CHECK(moq_simpair_start(f->sp) == MOQ_OK)) return false;
    f->rx = moq_simpair_server(f->sp);
    steps(f, 8);
    if (!CHECK(moq_session_state(f->rx) == MOQ_SESS_ESTABLISHED)) return false;

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_CALLBACK;
    pcfg.on_subscribe = on_subscribe;
    pcfg.on_subscribe_ctx = f;
    moq_pub_callbacks_init_sized(&pcfg.callbacks, sizeof(pcfg.callbacks));
    pcfg.callbacks.ctx = f;
    pcfg.callbacks.on_subscriber_updated = on_subscriber_updated;
    pcfg.callbacks.on_subscriber_joined = on_subscriber_joined;
    pcfg.callbacks.on_subscriber_left = on_subscriber_left;
    if (!CHECK(moq_pub_create(moq_simpair_client(f->sp), &f->alloc, &pcfg,
                              &f->pub) == MOQ_OK)) return false;
    static moq_bytes_t parts[] = { { (const uint8_t *)"done", 4 },
                                   { (const uint8_t *)"test", 4 } };
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->catalog) == MOQ_OK))
        return false;
    for (int i = 0; i < T_N; i++) {
        tcfg.track_name = (moq_bytes_t){ (const uint8_t *)k_names[i],
                                         strlen(k_names[i]) };
        if (!CHECK(moq_pub_add_track(f->pub, &tcfg, f->now, &f->pt[i]) == MOQ_OK))
            return false;
    }
    moq_rcbuf_t *cat = NULL;
    moq_rcbuf_create(&f->alloc, (const uint8_t *)k_catalog, strlen(k_catalog), &cat);
    moq_pub_retained_object_t retained = {
        .object_id = 0, .payload = cat, .properties = NULL, .end_of_group = true };
    moq_pub_retained_group_cfg_t group;
    moq_pub_retained_group_cfg_init(&group);
    group.objects = &retained;
    group.object_count = 1;
    moq_result_t crc = moq_pub_set_retained_group(f->pub, f->catalog, &group);
    moq_rcbuf_decref(cat);
    if (!CHECK(crc == MOQ_OK)) return false;
    steps(f, 8);

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    rcfg.namespace_ = tcfg.track_namespace;
    rcfg.auto_subscribe = false;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = policy;
    rcfg.overflow.max_objects = max_objects;
    rcfg.overflow.max_bytes = 1u << 20;
    rcfg.surface_stream_terminals = terminals != 0;
    rcfg.max_stream_terminals = terminals;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&rcfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!CHECK(f->r != NULL)) return false;
    for (unsigned i = 0; i < 64 && !f->ready; i++) {
        step(f, 1);
        poll_tracks(f);
    }
    if (!CHECK(f->ready && f->mt[T_VIDEO] && f->mt[T_SIDE] && f->mt[T_SAP]))
        return false;
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                           .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t options;
    moq_media_receiver_track_subscribe_cfg_init_sized(&options, sizeof(options));
    options.location = &location;
    for (int i = 0; i < T_N; i++) {
        if (i == T_SIDE && !subscribe_side) continue;
        if (!CHECK(moq_media_receiver_subscribe_track(f->r, f->mt[i], &options) ==
                   MOQ_OK))
            return false;
    }
    steps(f, 24);
    poll_tracks(f);
    return CHECK(f->n == 0);
}

static bool fx_setup(fx_t *f, moq_version_t ver, moq_media_overflow_policy_t policy,
                     uint32_t max_objects, uint32_t terminals)
{
    return fx_setup_tracks(f, ver, policy, max_objects, terminals, true);
}

static void fx_teardown(fx_t *f)
{
    if (f->r) moq_media_receiver_test_destroy_with_session(f->r, f->rx, f->now);
    if (f->pub) moq_pub_destroy(f->pub);
    if (f->sp) moq_simpair_destroy(f->sp);
    CHECK(f->as.balance == 0);
}

static moq_media_track_state_t state_of(fx_t *f, int i)
{
    moq_media_track_state_t s = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    CHECK(moq_media_receiver_track_state(f->r, f->mt[i], &s) == MOQ_OK);
    return s;
}

static void dump(const fx_t *f)
{
    static const char *const k[] = { "obj", "ENDED", "REMOVED", "other", "EOT" };
    for (unsigned i = 0; i < f->n; i++)
        fprintf(stderr, "  %s [%u] %s %s %llu/%llu\n", row, i, k[f->log[i].kind],
                f->log[i].track >= 0 ? k_names[f->log[i].track] : "?",
                (unsigned long long)f->log[i].g, (unsigned long long)f->log[i].o);
}

static int count(const fx_t *f, ev_kind_t k, int track)
{
    int c = 0;
    for (unsigned i = 0; i < f->n; i++)
        c += f->log[i].kind == k && f->log[i].track == track;
    return c;
}

static int find(const fx_t *f, ev_kind_t k, int track)
{
    for (unsigned i = 0; i < f->n; i++)
        if (f->log[i].kind == k && f->log[i].track == track) return (int)i;
    return -1;
}

static int find_last(const fx_t *f, ev_kind_t k, int track)
{
    int at = -1;
    for (unsigned i = 0; i < f->n; i++)
        if (f->log[i].kind == k && f->log[i].track == track) at = (int)i;
    return at;
}

/* The track's Objects, in order, are exactly `want`. */
static bool objects_are(const fx_t *f, int track, const uint64_t (*want)[2],
                        unsigned n)
{
    unsigned k = 0;
    bool ok = true;
    for (unsigned i = 0; i < f->n; i++) {
        if (f->log[i].kind != EV_OBJ || f->log[i].track != track) continue;
        ok &= k < n && f->log[i].g == want[k][0] && f->log[i].o == want[k][1];
        k++;
    }
    return ok && k == n;
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22"
         : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

#define BEGIN(name) \
    char label[64]; \
    snprintf(label, sizeof(label), "%s_%s", name, vname(ver)); \
    row = label; \
    unsigned before = failures; \
    fx_t f
#define END() \
    out: \
    if (failures != before) dump(&f); \
    fx_teardown(&f); \
    if (failures == before) printf("PASS: %s\n", row)

/* Poll the stream-terminal lane, noting each END_OF_TRACK record. */
static unsigned poll_terminals(fx_t *f)
{
    unsigned n = 0;
    moq_media_stream_terminal_t st;
    while (moq_media_receiver_poll_stream_terminal(f->r, &st, sizeof(st)) == MOQ_OK) {
        if (st.kind == MOQ_MEDIA_STREAM_END_OF_TRACK)
            note(f, EV_EOT, track_index(f, st.track), st.group_id, st.object_id);
        n++;
    }
    return n;
}

/* `n` steps; each polls track events and then, when `poll`, every lane. The
 * events come first, so an Object still queued behind TRACK_REMOVED is seen
 * to be delivered after it. */
static void settle(fx_t *f, unsigned n, bool poll)
{
    for (unsigned i = 0; i < n; i++) {
        step(f, 1);
        poll_tracks(f);
        if (poll) {
            poll_objects(f);
            poll_terminals(f);
            moq_media_sap_record_t sap;
            while (moq_media_receiver_poll_sap(f->r, &sap, sizeof(sap)) == MOQ_OK)
                note(f, EV_OTHER, sap.track == f->mt[T_SAP] ? -2 : -3, sap.group, sap.object);
        }
    }
}

/* End a track as the media sender does: its END_OF_TRACK, then the
 * removal that completes every subscription with TRACK_ENDED and the exact
 * Stream Count. */
static void end_and_remove(fx_t *f, int track)
{
    CHECK(moq_pub_end_track(f->pub, f->pt[track], f->now) == MOQ_OK);
    CHECK(moq_pub_remove_track(f->pub, f->pt[track], f->now) == MOQ_OK);
    f->pt[track] = NULL;
}

/* Once each: TRACK_ENDED, then TRACK_REMOVED. */
static void ended_then_removed(const fx_t *f, int track)
{
    CHECK(count(f, EV_ENDED, track) == 1);
    CHECK(count(f, EV_REMOVED, track) == 1);
    CHECK(find(f, EV_ENDED, track) < find(f, EV_REMOVED, track));
}

static void done_ended_then_removed(const fx_t *f, int track)
{
    ended_then_removed(f, track);
    CHECK(f->has_done_status[track] && f->done_status[track] == 0x2);
}

static bool catalog_complete(fx_t *f)
{
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    CHECK(moq_media_receiver_get_stats(f->r, &st, sizeof(st)) == MOQ_OK);
    return st.catalog_complete;
}

/* The terminal catalog arrives before the track's last Object and its end:
 * the track waits for them, then ends, then is removed. */
static void test_overtake(moq_version_t ver)
{
    BEGIN("overtake");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 0));
    write_to(&f, f.pt[T_VIDEO], 0, 0);
    write_to(&f, f.pt[T_VIDEO], 0, 1);
    settle(&f, 16, true);
    write_catalog(&f, 1, k_terminal);
    settle(&f, 32, true);
    CHECK(catalog_complete(&f));
    CHECK(f.n == 2);   /* the two Objects; no end, no removal */
    CHECK(state_of(&f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    write_to(&f, f.pt[T_VIDEO], 1, 0);
    end_and_remove(&f, T_VIDEO);
    settle(&f, 16, true);
    static const uint64_t want[3][2] = { { 0, 0 }, { 0, 1 }, { 1, 0 } };
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    ended_then_removed(&f, T_VIDEO);
    /* The last Object is still delivered after the removal. */
    CHECK(find_last(&f, EV_OBJ, T_VIDEO) > find(&f, EV_REMOVED, T_VIDEO));
    CHECK(f.n == 5 && !f.overflow);
    END();
}

/* The terminal catalog is taken in first. Then a two-record stream-terminal
 * lane the application does not poll fills and stops intake (for every
 * track), so the facade keeps the rest of the media and its end. Forty
 * seconds pass: nothing ends and nothing is removed. Polling then delivers
 * every Object, once, in order, then the end, then the removal. */
static void test_full_lane_slow_poll(moq_version_t ver)
{
    BEGIN("full_lane_slow_poll");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 2));
    write_catalog(&f, 1, k_terminal);
    settle(&f, 16, false);
    CHECK(catalog_complete(&f));
    for (uint64_t g = 0; g < 6; g++) {
        write_to(&f, f.pt[T_VIDEO], g, 0);
        CHECK(moq_pub_end_group(f.pub, f.pt[T_VIDEO], f.now) == MOQ_OK);
    }
    end_and_remove(&f, T_VIDEO);
    for (unsigned i = 0; i < 40; i++) {
        step(&f, 1000000);
        poll_tracks(&f);
    }
    CHECK(f.n == 0);
    CHECK(state_of(&f, T_VIDEO) != MOQ_MEDIA_TRACK_STATE_ENDED);
    {
        /* Intake really stopped: the facade still holds most of the media. */
        moq_media_receiver_stats_t st;
        memset(&st, 0, sizeof(st));
        CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
        CHECK(st.objects_received < 6 && st.objects_dropped == 0);
    }
    settle(&f, 64, true);
    static const uint64_t want[6][2] = { { 0, 0 }, { 1, 0 }, { 2, 0 },
                                         { 3, 0 }, { 4, 0 }, { 5, 0 } };
    CHECK(objects_are(&f, T_VIDEO, want, 6));
    ended_then_removed(&f, T_VIDEO);
    CHECK(count(&f, EV_EOT, T_VIDEO) == 1);
    CHECK(find(&f, EV_EOT, T_VIDEO) > find_last(&f, EV_OBJ, T_VIDEO));
    CHECK(!f.overflow);
    END();
}

/* FLOW_CONTROL over a two-Object queue the application does not poll: its
 * soft ceiling degrades to a counted group drop, so the end is not held --
 * but every written Object is delivered or counted, and the track still
 * ends before it is removed. */
static void test_flow_control_counted(moq_version_t ver)
{
    BEGIN("flow_control_counted");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, 2, 0));
    for (uint64_t g = 0; g < 6; g++) write_to(&f, f.pt[T_VIDEO], g, 0);
    end_and_remove(&f, T_VIDEO);
    write_catalog(&f, 1, k_terminal);
    for (unsigned i = 0; i < 40; i++) {
        step(&f, 1000000);
        poll_tracks(&f);
    }
    settle(&f, 64, true);
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    unsigned got = (unsigned)count(&f, EV_OBJ, T_VIDEO);
    CHECK(got >= 1 && got + st.objects_dropped == 6);
    CHECK(st.objects_received == 6);
    ended_then_removed(&f, T_VIDEO);
    END();
}

/* The publisher's session closes after the terminal catalog but before the
 * track's tail. The track is never reported ended; every subscription is
 * over with the session, so each track is removed exactly once, and what
 * was taken in stays pollable. (A receiver on an endpoint reports the
 * session's end as the endpoint's failure.) */
static void test_session_failure(moq_version_t ver)
{
    BEGIN("session_failure");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 0));
    write_to(&f, f.pt[T_VIDEO], 0, 0);
    write_catalog(&f, 1, k_terminal);
    settle(&f, 16, false);
    CHECK(catalog_complete(&f));
    CHECK(moq_session_close(moq_simpair_client(f.sp), 0x1, "gone", f.now) == MOQ_OK);
    f.closed = true;
    for (unsigned i = 0; i < 40; i++) {
        step(&f, 1000000);
        poll_tracks(&f);
    }
    CHECK(moq_session_state(f.rx) != MOQ_SESS_ESTABLISHED);
    for (int i = 0; i < T_N; i++) {
        CHECK(count(&f, EV_ENDED, i) == 0);
        CHECK(count(&f, EV_REMOVED, i) == 1);
    }
    CHECK(poll_objects(&f) == 1);
    static const uint64_t want[1][2] = { { 0, 0 } };
    CHECK(objects_are(&f, T_VIDEO, want, 1));
    END();
}

/* Three tracks, the generated SAP timeline among them: video ends before
 * the terminal catalog, its timeline and the side track after it, each at
 * its own time. Each is removed after its own end and no earlier. */
static void test_multi(moq_version_t ver)
{
    BEGIN("multi");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 0));
    write_to(&f, f.pt[T_VIDEO], 0, 0);
    write_sap(&f, 0);
    write_to(&f, f.pt[T_SIDE], 0, 0);
    end_and_remove(&f, T_VIDEO);
    settle(&f, 16, true);
    write_catalog(&f, 1, k_terminal);
    settle(&f, 16, true);
    ended_then_removed(&f, T_VIDEO);
    CHECK(count(&f, EV_ENDED, T_SAP) == 0 && count(&f, EV_REMOVED, T_SAP) == 0);
    CHECK(count(&f, EV_ENDED, T_SIDE) == 0 && count(&f, EV_REMOVED, T_SIDE) == 0);
    CHECK(state_of(&f, T_SAP) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CHECK(state_of(&f, T_SIDE) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    end_and_remove(&f, T_SAP);
    settle(&f, 16, true);
    ended_then_removed(&f, T_SAP);
    CHECK(count(&f, EV_REMOVED, T_SIDE) == 0);
    write_to(&f, f.pt[T_SIDE], 1, 0);
    end_and_remove(&f, T_SIDE);
    settle(&f, 16, true);
    ended_then_removed(&f, T_SIDE);
    static const uint64_t v[1][2] = { { 0, 0 } };
    static const uint64_t side[2][2] = { { 0, 0 }, { 1, 0 } };
    CHECK(objects_are(&f, T_VIDEO, v, 1));
    CHECK(objects_are(&f, T_SIDE, side, 2));
    /* The one timeline record, delivered once through its own lane. */
    CHECK(count(&f, EV_OTHER, -2) == 1);
    CHECK(count(&f, EV_OTHER, T_SAP) == 0 && count(&f, EV_OTHER, T_VIDEO) == 0 &&
          count(&f, EV_OTHER, T_SIDE) == 0);
    CHECK(!f.overflow);
    END();
}

/* A track that ended before the terminal catalog, its media not yet
 * polled: the catalog removes it, and the media is still delivered. */
static void test_ended_then_removed(moq_version_t ver)
{
    BEGIN("ended_then_removed");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 0));
    for (uint64_t g = 0; g < 3; g++) write_to(&f, f.pt[T_VIDEO], g, 0);
    end_and_remove(&f, T_VIDEO);
    settle(&f, 16, false);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 1 && count(&f, EV_REMOVED, T_VIDEO) == 0);
    write_catalog(&f, 1, k_terminal);
    settle(&f, 16, false);
    CHECK(poll_objects(&f) == 3);
    static const uint64_t want[3][2] = { { 0, 0 }, { 1, 0 }, { 2, 0 } };
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    ended_then_removed(&f, T_VIDEO);
    CHECK(f.n == 5);
    END();
}

/* Deliver session traffic and run the receiver, withholding ONLY the publisher
 * facade's tick. Its existing catalog subscription can send data, but a new
 * media SUBSCRIBE cannot be admitted until the test resumes moq_pub_tick. */
static bool step_without_publisher(fx_t *f)
{
    f->now++;
    if (!CHECK(moq_simpair_advance_to(f->sp, f->now) == MOQ_OK)) return false;
    if (!CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) == MOQ_OK))
        return false;
    if (!CHECK(moq_session_process_pending(f->rx, f->now) >= 0)) return false;
    moq_media_receiver_test_pump(f->r, f->rx, f->now);
    poll_tracks(f);
    return CHECK(!moq_media_receiver_is_fatal(f->r) &&
                 moq_session_state(f->rx) == MOQ_SESS_ESTABLISHED);
}

/* These rows do not poll media until after removal, so every received Object
 * must remain in the queue, with no policy or parse drops. */
static bool queued_media(fx_t *f, uint64_t n)
{
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    if (!CHECK(moq_media_receiver_get_stats(f->r, &st, sizeof(st)) == MOQ_OK))
        return false;
    return CHECK(st.objects_received == n && st.objects_queued == n &&
                 st.objects_dropped == 0 && st.parse_drops == 0);
}

/* Queue live media, then finish that subscription WITHOUT END_OF_TRACK.
 * Stop with the original handle pending, before the publisher runs again. */
static bool finish_live_without_end_of_track(fx_t *f)
{
    if (!CHECK(f->joined[T_VIDEO] == 1 && f->left[T_VIDEO] == 0 &&
               state_of(f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_ACTIVE))
        return false;
    for (uint64_t o = 0; o < 3; o++) write_to(f, f->pt[T_VIDEO], 0, o);
    settle(f, 16, false);
    if (!queued_media(f, 3) || !CHECK(f->n == 0)) return false;
    if (!CHECK(moq_pub_finish_subscribers(f->pub, f->pt[T_VIDEO],
                   MOQ_PUB_DONE_TRACK_ENDED, f->now) == MOQ_OK)) return false;
    if (!CHECK(!moq_pub_has_subscriber(f->pub, f->pt[T_VIDEO]))) return false;
    for (unsigned i = 0; i < 16 &&
         state_of(f, T_VIDEO) != MOQ_MEDIA_TRACK_STATE_PENDING; i++)
        if (!step_without_publisher(f)) return false;
    if (!CHECK(state_of(f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_PENDING &&
               f->joined[T_VIDEO] == 1 && f->left[T_VIDEO] == 0 &&
               count(f, EV_ENDED, T_VIDEO) == 0 &&
               count(f, EV_REMOVED, T_VIDEO) == 0)) return false;
    return queued_media(f, 3);
}

static bool accept_replacement(fx_t *f)
{
    settle(f, 16, false);
    return CHECK(f->requested[T_VIDEO] == 2 && f->joined[T_VIDEO] == 2 &&
                 f->left[T_VIDEO] == 0 &&
                 moq_pub_has_subscriber(f->pub, f->pt[T_VIDEO]) &&
                 state_of(f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
}

/* With the publisher running, no request or admission may follow the original
 * done until conversion. PENDING alone does not distinguish an issued request. */
static bool awaiting_conversion(fx_t *f)
{
    settle(f, 16, false);
    return CHECK(f->requested[T_VIDEO] == 1 && f->joined[T_VIDEO] == 1 &&
                 f->left[T_VIDEO] == 0 &&
                 !moq_pub_has_subscriber(f->pub, f->pt[T_VIDEO]) &&
                 state_of(f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_PENDING &&
                 count(f, EV_ENDED, T_VIDEO) == 0 &&
                 count(f, EV_REMOVED, T_VIDEO) == 0);
}

static bool video_converted(fx_t *f, moq_media_track_t *original)
{
    moq_media_track_desc_t desc;
    return CHECK(f->mt[T_VIDEO] == original &&
        moq_media_receiver_track_desc_copy(f->r, original, &desc, sizeof(desc)) == MOQ_OK &&
        !desc.is_live && desc.has_track_duration && desc.track_duration_ms == 5000 &&
        count(f, EV_OTHER, T_VIDEO) == 1 && !catalog_complete(f));
}

/* MSF-01 11.3: the done-only live end is followed by permanent completion,
 * not conversion. No replacement request, second done or END_OF_TRACK is
 * needed to end the original handle and preserve its three queued Objects. */
static void test_done_only_then_removed(moq_version_t ver, bool publisher_running)
{
    BEGIN(publisher_running ? "done_only_terminal_publisher_running"
                            : "done_only_terminal_before_publisher_tick");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16));
    moq_media_track_t *original = f.mt[T_VIDEO];
    REQUIRE(finish_live_without_end_of_track(&f));
    if (publisher_running) REQUIRE(awaiting_conversion(&f));

    write_catalog(&f, 1, k_terminal);
    if (!publisher_running) {
        for (unsigned i = 0; i < 16 && !catalog_complete(&f); i++)
            REQUIRE(step_without_publisher(&f));
        /* The terminal catalog is adopted before the publisher ticks again. */
        REQUIRE(catalog_complete(&f));
        REQUIRE(f.joined[T_VIDEO] == 1);
        REQUIRE(!moq_pub_has_subscriber(f.pub, f.pt[T_VIDEO]));
    }
    settle(&f, 32, false);
    REQUIRE(catalog_complete(&f));
    CHECK(f.mt[T_VIDEO] == original);
    done_ended_then_removed(&f, T_VIDEO);
    moq_media_track_state_t state = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    CHECK(moq_media_receiver_track_state(f.r, original, &state) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_media_receiver_subscribe_track(f.r, original, NULL) == MOQ_ERR_WRONG_STATE);
    CHECK(!moq_pub_has_subscriber(f.pub, f.pt[T_VIDEO]));
    CHECK(f.requested[T_VIDEO] == 1 && f.joined[T_VIDEO] == 1 && f.left[T_VIDEO] == 0);
    CHECK(queued_media(&f, 3));
    CHECK(poll_objects(&f) == 3);
    static const uint64_t want[3][2] = { { 0, 0 }, { 0, 1 }, { 0, 2 } };
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    CHECK(find(&f, EV_OBJ, T_VIDEO) > find(&f, EV_REMOVED, T_VIDEO));
    settle(&f, 16, true);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    CHECK(count(&f, EV_EOT, T_VIDEO) == 0);
    CHECK(!moq_media_receiver_is_fatal(f.r) &&
          moq_session_state(f.rx) == MOQ_SESS_ESTABLISHED &&
          moq_session_state(moq_simpair_client(f.sp)) == MOQ_SESS_ESTABLISHED);
    CHECK(!f.overflow);
    END();
}

static bool write_video_chunk(fx_t *f, const uint8_t *data, size_t len)
{
    moq_rcbuf_t *chunk = NULL;
    if (!CHECK(moq_rcbuf_create(&f->alloc, data, len, &chunk) == MOQ_OK))
        return false;
    moq_result_t rc = moq_pub_write_data(f->pub, f->pt[T_VIDEO], chunk, f->now);
    moq_rcbuf_decref(chunk);
    return CHECK(rc == MOQ_OK);
}

static bool saw_unfinished_input(const fx_t *f, unsigned start)
{
    for (unsigned i = start; i < f->in_n; i++)
        if (f->in[i].len && !f->in[i].fin) return true;
    return false;
}

/* The replacement owns a partial first or later Object before the terminal
 * catalog. Remaining bytes model a tail still in flight; no new Object is
 * assigned after completion. A whole-object observation cannot prove safety. */
static void test_vod_replacement_tail(moq_version_t ver, bool first_object)
{
    BEGIN(first_object ? "vod_replacement_first_tail" : "vod_replacement_tail");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16));
    moq_media_track_t *original = f.mt[T_VIDEO];
    REQUIRE(finish_live_without_end_of_track(&f));
    write_catalog(&f, 1, k_video_vod);
    for (unsigned i = 0; i < 16; i++) REQUIRE(step_without_publisher(&f));
    REQUIRE(video_converted(&f, original));
    REQUIRE(f.requested[T_VIDEO] == 1 && f.joined[T_VIDEO] == 1);
    REQUIRE(accept_replacement(&f));
    unsigned tail_inputs = f.in_n;
    if (!first_object) write_to(&f, f.pt[T_VIDEO], 1, 0);
    moq_pub_begin_object_cfg_t begin;
    moq_pub_begin_object_cfg_init_sized(&begin, sizeof(begin));
    begin.group_id = 1;
    begin.object_id = first_object ? 0 : 1;
    const uint8_t tail[] = { 1, (uint8_t)begin.object_id, 1, 2, 3, 4, 5, 6 };
    begin.payload_length = sizeof(tail);
    REQUIRE(moq_pub_begin_object(f.pub, f.pt[T_VIDEO], &begin, f.now) == MOQ_OK);
    REQUIRE(write_video_chunk(&f, tail, 4));
    settle(&f, 16, false);
    REQUIRE(saw_unfinished_input(&f, tail_inputs));
    REQUIRE(queued_media(&f, first_object ? 3 : 4));
    REQUIRE(state_of(&f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_ACTIVE);

    write_catalog(&f, 2, k_terminal);
    settle(&f, 16, false);
    REQUIRE(catalog_complete(&f));
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 0);
    CHECK(f.joined[T_VIDEO] == 2 && f.left[T_VIDEO] == 0);
    CHECK(moq_pub_has_subscriber(f.pub, f.pt[T_VIDEO]));
    CHECK(queued_media(&f, first_object ? 3 : 4));

    REQUIRE(write_video_chunk(&f, tail + 4, sizeof(tail) - 4));
    REQUIRE(moq_pub_end_object(f.pub, f.pt[T_VIDEO], f.now) == MOQ_OK);
    REQUIRE(moq_pub_finish_subscribers(f.pub, f.pt[T_VIDEO],
                MOQ_PUB_DONE_TRACK_ENDED, f.now) == MOQ_OK);
    settle(&f, 32, false);
    done_ended_then_removed(&f, T_VIDEO);
    unsigned objects = first_object ? 4 : 5;
    CHECK(queued_media(&f, objects));
    CHECK(poll_objects(&f) == objects);
    static const uint64_t want[5][2] = {
        { 0, 0 }, { 0, 1 }, { 0, 2 }, { 1, 0 }, { 1, 1 } };
    CHECK(objects_are(&f, T_VIDEO, want, objects));
    CHECK(find(&f, EV_OBJ, T_VIDEO) > find(&f, EV_REMOVED, T_VIDEO));
    settle(&f, 16, true);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(objects_are(&f, T_VIDEO, want, objects));
    CHECK(f.requested[T_VIDEO] == 2 && f.joined[T_VIDEO] == 2 && f.left[T_VIDEO] == 0);
    CHECK(count(&f, EV_EOT, T_VIDEO) == 0);
    CHECK(!moq_media_receiver_is_fatal(f.r) &&
          moq_session_state(f.rx) == MOQ_SESS_ESTABLISHED &&
          moq_session_state(moq_simpair_client(f.sp)) == MOQ_SESS_ESTABLISHED);
    CHECK(!f.overflow);
    END();
}

/* Acceptance and an empty whole-object queue do not complete a confirmed
 * replacement. The terminal catalog can also arrive before its acceptance. */
static void test_vod_replacement_empty(moq_version_t ver, bool accepted)
{
    BEGIN(accepted ? "vod_empty_active" : "vod_empty_pending");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16));
    moq_media_track_t *original = f.mt[T_VIDEO];
    REQUIRE(finish_live_without_end_of_track(&f));
    write_catalog(&f, 1, k_video_vod);
    for (unsigned i = 0; i < 16; i++) REQUIRE(step_without_publisher(&f));
    REQUIRE(video_converted(&f, original));
    REQUIRE(state_of(&f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_PENDING);
    REQUIRE(f.requested[T_VIDEO] == 1 && f.joined[T_VIDEO] == 1);
    if (accepted) REQUIRE(accept_replacement(&f));
    write_catalog(&f, 2, k_terminal);
    for (unsigned i = 0; i < 16; i++) REQUIRE(step_without_publisher(&f));
    REQUIRE(catalog_complete(&f));
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 0);
    CHECK(f.left[T_VIDEO] == 0);
    REQUIRE(accept_replacement(&f));
    settle(&f, 32, false);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 0);
    CHECK(queued_media(&f, 3));
    REQUIRE(moq_pub_finish_subscribers(f.pub, f.pt[T_VIDEO],
                MOQ_PUB_DONE_TRACK_ENDED, f.now) == MOQ_OK);
    settle(&f, 32, false);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(poll_objects(&f) == 3);
    static const uint64_t want[3][2] = { { 0, 0 }, { 0, 1 }, { 0, 2 } };
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    settle(&f, 16, true);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    CHECK(f.requested[T_VIDEO] == 2 && f.joined[T_VIDEO] == 2 && f.left[T_VIDEO] == 0);
    CHECK(count(&f, EV_EOT, T_VIDEO) == 0 && !f.overflow);
    END();
}

/* Both catalog Objects reach the session before the receiver pumps. Conversion
 * must not create an obligation between their adoption and terminal removal. */
static void test_conversion_terminal_before_reconcile(moq_version_t ver)
{
    BEGIN("conversion_terminal_before_reconcile");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16));
    moq_media_track_t *original = f.mt[T_VIDEO];
    REQUIRE(finish_live_without_end_of_track(&f));
    REQUIRE(awaiting_conversion(&f));
    write_catalog(&f, 1, k_video_vod);
    write_catalog(&f, 2, k_terminal);
    for (unsigned i = 0; i < 8; i++) {
        f.now++;
        REQUIRE(moq_simpair_advance_to(f.sp, f.now) == MOQ_OK);
        REQUIRE(moq_simpair_run_until_quiescent(f.sp, 64, NULL) == MOQ_OK);
        REQUIRE(moq_pub_tick(f.pub, f.now) >= 0);
    }
    REQUIRE(!catalog_complete(&f) && count(&f, EV_OTHER, T_VIDEO) == 0);
    REQUIRE(moq_session_process_pending(f.rx, f.now) >= 0);
    moq_media_receiver_test_pump(f.r, f.rx, f.now);
    poll_tracks(&f);
    REQUIRE(catalog_complete(&f));
    REQUIRE(count(&f, EV_OTHER, T_VIDEO) == 1 && f.mt[T_VIDEO] == original);
    moq_media_track_desc_t desc;
    REQUIRE(moq_media_receiver_track_desc_copy(f.r, original, &desc, sizeof(desc)) == MOQ_OK);
    CHECK(!desc.is_live && desc.has_track_duration && desc.track_duration_ms == 5000);
    done_ended_then_removed(&f, T_VIDEO);
    settle(&f, 32, false);
    CHECK(f.requested[T_VIDEO] == 1 && f.joined[T_VIDEO] == 1 && f.left[T_VIDEO] == 0);
    CHECK(!moq_pub_has_subscriber(f.pub, f.pt[T_VIDEO]));
    CHECK(queued_media(&f, 3));
    CHECK(poll_objects(&f) == 3);
    static const uint64_t want[3][2] = { { 0, 0 }, { 0, 1 }, { 0, 2 } };
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    settle(&f, 16, true);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(objects_are(&f, T_VIDEO, want, 3));
    CHECK(count(&f, EV_EOT, T_VIDEO) == 0 && !f.overflow);
    END();
}

/* Conversion precedes the original done while the original owns an unfinished
 * Object. A terminal catalog before its tail drains needs no replacement;
 * otherwise the confirmed replacement starts only after the original done. */
static void test_conversion_original_tail(moq_version_t ver, bool terminal_first)
{
    BEGIN(terminal_first ? "conversion_original_tail_terminal"
                         : "conversion_original_tail_rearm");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16));
    moq_media_track_t *original = f.mt[T_VIDEO];
    unsigned tail_inputs = f.in_n;
    write_to(&f, f.pt[T_VIDEO], 0, 0);
    moq_pub_begin_object_cfg_t begin;
    moq_pub_begin_object_cfg_init_sized(&begin, sizeof(begin));
    begin.group_id = 0;
    begin.object_id = 1;
    static const uint8_t tail[] = { 0, 1, 1, 2, 3, 4, 5, 6 };
    begin.payload_length = sizeof(tail);
    REQUIRE(moq_pub_begin_object(f.pub, f.pt[T_VIDEO], &begin, f.now) == MOQ_OK);
    REQUIRE(write_video_chunk(&f, tail, 4));
    settle(&f, 16, false);
    REQUIRE(saw_unfinished_input(&f, tail_inputs));
    REQUIRE(queued_media(&f, 1));
    write_catalog(&f, 1, k_video_vod);
    settle(&f, 16, false);
    REQUIRE(video_converted(&f, original));
    REQUIRE(state_of(&f, T_VIDEO) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    REQUIRE(f.requested[T_VIDEO] == 1 && f.joined[T_VIDEO] == 1 && f.left[T_VIDEO] == 0);
    if (terminal_first) {
        write_catalog(&f, 2, k_terminal);
        settle(&f, 16, false);
        REQUIRE(catalog_complete(&f));
    }
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 0);
    CHECK(queued_media(&f, 1));
    REQUIRE(write_video_chunk(&f, tail + 4, sizeof(tail) - 4));
    REQUIRE(moq_pub_end_object(f.pub, f.pt[T_VIDEO], f.now) == MOQ_OK);
    REQUIRE(moq_pub_finish_subscribers(f.pub, f.pt[T_VIDEO],
                MOQ_PUB_DONE_TRACK_ENDED, f.now) == MOQ_OK);
    if (!terminal_first) {
        REQUIRE(accept_replacement(&f));
        REQUIRE(queued_media(&f, 2));
        write_catalog(&f, 2, k_terminal);
        settle(&f, 16, false);
        REQUIRE(catalog_complete(&f));
        CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 0);
        REQUIRE(moq_pub_finish_subscribers(f.pub, f.pt[T_VIDEO],
                    MOQ_PUB_DONE_TRACK_ENDED, f.now) == MOQ_OK);
    }
    settle(&f, 32, false);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(queued_media(&f, 2));
    CHECK(poll_objects(&f) == 2);
    static const uint64_t want[2][2] = { { 0, 0 }, { 0, 1 } };
    CHECK(objects_are(&f, T_VIDEO, want, 2));
    settle(&f, 16, true);
    done_ended_then_removed(&f, T_VIDEO);
    CHECK(objects_are(&f, T_VIDEO, want, 2));
    CHECK(f.requested[T_VIDEO] == (terminal_first ? 1u : 2u));
    CHECK(f.joined[T_VIDEO] == f.requested[T_VIDEO] && f.left[T_VIDEO] == 0);
    CHECK(count(&f, EV_EOT, T_VIDEO) == 0 && !f.overflow);
    END();
}

/* Unchanged and rejected half-conversion catalogs leave the wait intact while
 * a sibling delivers. Only the later validated conversion admits replacement. */
static void test_waiting_conversion_catalogs(moq_version_t ver)
{
    BEGIN("waiting_conversion_catalogs");
    REQUIRE(fx_setup_tracks(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16, false));
    moq_media_track_t *original = f.mt[T_VIDEO];
    REQUIRE(finish_live_without_end_of_track(&f));
    REQUIRE(awaiting_conversion(&f));
    write_catalog(&f, 1, k_catalog);
    REQUIRE(awaiting_conversion(&f));
    write_catalog(&f, 2, k_half_conversion);
    /* A newly wanted sibling must also reconcile past the waiting video. */
    REQUIRE(state_of(&f, T_SIDE) == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                           .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t options;
    moq_media_receiver_track_subscribe_cfg_init_sized(&options, sizeof(options));
    options.location = &location;
    REQUIRE(moq_media_receiver_subscribe_track(f.r, f.mt[T_SIDE], &options) == MOQ_OK);
    REQUIRE(awaiting_conversion(&f));
    REQUIRE(state_of(&f, T_SIDE) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    REQUIRE(f.requested[T_SIDE] == 1 && f.joined[T_SIDE] == 1);
    write_to(&f, f.pt[T_SIDE], 0, 0);
    REQUIRE(awaiting_conversion(&f));
    moq_media_receiver_stats_t st;
    REQUIRE(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.catalog_drops == 1 && st.objects_dropped == 0 && st.parse_drops == 0);
    moq_media_track_desc_t desc;
    REQUIRE(moq_media_receiver_track_desc_copy(f.r, original, &desc, sizeof(desc)) == MOQ_OK);
    CHECK(desc.is_live && !desc.has_track_duration && count(&f, EV_OTHER, T_VIDEO) == 0);
    REQUIRE(state_of(&f, T_SIDE) == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CHECK(poll_objects(&f) == 4);
    static const uint64_t side[1][2] = { { 0, 0 } };
    CHECK(objects_are(&f, T_SIDE, side, 1));
    write_catalog(&f, 3, k_video_vod);
    REQUIRE(accept_replacement(&f));
    REQUIRE(video_converted(&f, original));
    write_to(&f, f.pt[T_VIDEO], 1, 0);
    settle(&f, 16, false);
    write_catalog(&f, 4, k_terminal);
    settle(&f, 16, false);
    REQUIRE(catalog_complete(&f));
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 0);
    REQUIRE(moq_pub_finish_subscribers(f.pub, f.pt[T_VIDEO],
                MOQ_PUB_DONE_TRACK_ENDED, f.now) == MOQ_OK);
    end_and_remove(&f, T_SIDE);
    settle(&f, 32, true);
    done_ended_then_removed(&f, T_VIDEO);
    ended_then_removed(&f, T_SIDE);
    static const uint64_t want[4][2] = { { 0, 0 }, { 0, 1 }, { 0, 2 }, { 1, 0 } };
    CHECK(objects_are(&f, T_VIDEO, want, 4));
    CHECK(objects_are(&f, T_SIDE, side, 1));
    CHECK(f.requested[T_VIDEO] == 2 && f.joined[T_VIDEO] == 2 && f.left[T_VIDEO] == 0);
    CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.objects_received == 5 && st.objects_dropped == 0 && st.parse_drops == 0);
    CHECK(count(&f, EV_EOT, T_VIDEO) == 0 && !f.overflow);
    END();
}

/* An app cancellation during the conversion wait purges the original media;
 * a later terminal catalog removes it without projecting a successful end. */
static void test_cancel_awaiting_conversion(moq_version_t ver)
{
    BEGIN("cancel_awaiting_conversion");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 16));
    REQUIRE(finish_live_without_end_of_track(&f));
    REQUIRE(awaiting_conversion(&f));
    write_to(&f, f.pt[T_SIDE], 0, 0);
    settle(&f, 16, false);
    REQUIRE(moq_media_receiver_unsubscribe_track(f.r, f.mt[T_VIDEO]) == MOQ_OK);
    CHECK(poll_objects(&f) == 1);
    CHECK(count(&f, EV_OBJ, T_VIDEO) == 0);
    write_catalog(&f, 1, k_terminal);
    settle(&f, 32, false);
    REQUIRE(catalog_complete(&f));
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 1);
    CHECK(moq_media_receiver_subscribe_track(f.r, f.mt[T_VIDEO], NULL) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_media_receiver_unsubscribe_track(f.r, f.mt[T_VIDEO]) == MOQ_ERR_WRONG_STATE);
    end_and_remove(&f, T_SIDE);
    settle(&f, 32, true);
    static const uint64_t side[1][2] = { { 0, 0 } };
    CHECK(objects_are(&f, T_SIDE, side, 1));
    ended_then_removed(&f, T_SIDE);
    CHECK(count(&f, EV_OBJ, T_VIDEO) == 0 && count(&f, EV_EOT, T_VIDEO) == 0);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 1);
    CHECK(f.requested[T_VIDEO] == 1 && f.joined[T_VIDEO] == 1 && f.left[T_VIDEO] == 0);
    CHECK(!moq_media_receiver_is_fatal(f.r) && !f.overflow);
    END();
}

/* The terminal catalog removed video and side while their tails are owed;
 * the application unsubscribes video. `lane_full`: a two-record lane the
 * application has not polled has stopped intake, so the facade still holds
 * video's media and its END_OF_TRACK, and its slot cannot be released yet;
 * that end reaches the receiver only after the unsubscribe. */
static void test_cancel_pending(moq_version_t ver, bool lane_full)
{
    BEGIN(lane_full ? "cancel_pending_full_lane" : "cancel_pending");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, lane_full ? 2 : 0));
    write_catalog(&f, 1, k_terminal);
    settle(&f, 16, false);
    REQUIRE(catalog_complete(&f));
    write_to(&f, f.pt[T_SIDE], 0, 0);
    if (lane_full) {
        for (uint64_t g = 0; g < 4; g++) {
            write_to(&f, f.pt[T_VIDEO], g, 0);
            CHECK(moq_pub_end_group(f.pub, f.pt[T_VIDEO], f.now) == MOQ_OK);
        }
        end_and_remove(&f, T_VIDEO);
    } else {
        write_to(&f, f.pt[T_VIDEO], 0, 0);
        write_to(&f, f.pt[T_VIDEO], 0, 1);
    }
    settle(&f, 16, false);
    CHECK(f.n == 0);
    {
        moq_media_receiver_stats_t st;
        memset(&st, 0, sizeof(st));
        CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
        if (lane_full)   /* intake stopped: the facade holds the rest */
            CHECK(st.objects_received < 5 && st.objects_dropped == 0);
        else             /* video's two and side's one are queued */
            CHECK(st.objects_queued == 3);
    }
    if (!lane_full) CHECK(moq_pub_has_subscriber(f.pub, f.pt[T_VIDEO]));

    CHECK(moq_media_receiver_unsubscribe_track(f.r, f.mt[T_VIDEO]) == MOQ_OK);
    /* The purge is immediate, on the application's thread. */
    unsigned purged_check = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(f.r, &obj, sizeof(obj)) == MOQ_OK) {
        note(&f, EV_OBJ, track_index(&f, obj.track), obj.group_id, obj.object_id);
        moq_media_object_cleanup(&obj);
        purged_check++;
    }
    CHECK(count(&f, EV_OBJ, T_VIDEO) == 0);
    /* Settled without the application polling anything more. */
    for (unsigned i = 0; i < 16; i++) { step(&f, 1); poll_tracks(&f); }
    CHECK(count(&f, EV_REMOVED, T_VIDEO) == 1);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0);
    CHECK(moq_media_receiver_unsubscribe_track(f.r, f.mt[T_VIDEO]) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_media_receiver_subscribe_track(f.r, f.mt[T_VIDEO], NULL) ==
          MOQ_ERR_WRONG_STATE);

    /* The tails are published: video's reaches nobody, side's completes. */
    if (!lane_full) {
        write_to(&f, f.pt[T_VIDEO], 9, 0);
        end_and_remove(&f, T_VIDEO);
    }
    write_to(&f, f.pt[T_SIDE], 1, 0);
    end_and_remove(&f, T_SIDE);
    settle(&f, 64, true);
    CHECK(count(&f, EV_OBJ, T_VIDEO) == 0);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0 && count(&f, EV_REMOVED, T_VIDEO) == 1);
    static const uint64_t side[2][2] = { { 0, 0 }, { 1, 0 } };
    CHECK(objects_are(&f, T_SIDE, side, 2));
    ended_then_removed(&f, T_SIDE);
    CHECK(!f.overflow);
    (void)purged_check;
    END();
}

/* An unsubscribe committed before the terminal catalog is still the app's
 * intent when that catalog arrives. Removal must not wait for an unwanted
 * tail, whether Forward=0 already reached the publisher or is still pending. */
static void test_cancel_before_terminal(moq_version_t ver, bool forward_applied)
{
    BEGIN(forward_applied ? "cancel_before_terminal_forward0"
                          : "cancel_before_terminal_pending_update");
    REQUIRE(fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 0));
    write_to(&f, f.pt[T_VIDEO], 0, 0);
    write_to(&f, f.pt[T_SIDE], 0, 0);
    settle(&f, 16, false);
    REQUIRE(moq_pub_has_subscriber(f.pub, f.pt[T_VIDEO]));
    CHECK(moq_media_receiver_unsubscribe_track(f.r, f.mt[T_VIDEO]) == MOQ_OK);
    if (forward_applied) {
        settle(&f, 16, false);
        REQUIRE(f.forward0[T_VIDEO] == 1);
    }
    else REQUIRE(f.forward0[T_VIDEO] == 0);
    CHECK(count(&f, EV_REMOVED, T_VIDEO) == 0);
    write_catalog(&f, 1, k_terminal);
    settle(&f, 32, true);
    REQUIRE(catalog_complete(&f));
    CHECK(count(&f, EV_REMOVED, T_VIDEO) == 1);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0);
    CHECK(count(&f, EV_OBJ, T_VIDEO) == 0);
    CHECK(moq_media_receiver_unsubscribe_track(f.r, f.mt[T_VIDEO]) ==
          MOQ_ERR_WRONG_STATE);
    CHECK(count(&f, EV_REMOVED, T_SIDE) == 0);
    write_to(&f, f.pt[T_SIDE], 1, 0);
    end_and_remove(&f, T_VIDEO);
    end_and_remove(&f, T_SIDE);
    settle(&f, 32, true);
    CHECK(count(&f, EV_REMOVED, T_VIDEO) == 1);
    CHECK(count(&f, EV_ENDED, T_VIDEO) == 0);
    static const uint64_t side[2][2] = { { 0, 0 }, { 1, 0 } };
    CHECK(objects_are(&f, T_SIDE, side, 2));
    ended_then_removed(&f, T_SIDE);
    CHECK(!f.overflow);
    END();
}

/* The END_OF_TRACK, on its own stream, overtakes the track's last Object:
 * the simulator's seeded delay fault, enabled once everything but the
 * tail is delivered, lets the end's stream mature before the tail's. The
 * receiver must not end the subscription on the END_OF_TRACK (the peer's
 * cancel would stop the tail's stream): it keeps it until the done, which
 * the facade surfaces only once every counted stream is processed, so the
 * tail is delivered and the track still ends once, then is removed once.
 * The first seed that produces the overtake (checked in the trace) is the
 * one used; the row fails if none of the first 48 does. */
static void test_tail_overtaken(moq_version_t ver)
{
    BEGIN("tail_overtaken");
    bool found = false;
    memset(&f, 0, sizeof(f));
    for (uint64_t seed = 1; seed <= 48 && !found; seed++) {
        g_seed = seed;
        g_fault_flags = MOQ_SIM_FAULT_DELAY;
        g_fault_per_mille = 1000;
        bool ok = fx_setup(&f, ver, MOQ_MEDIA_OVERFLOW_DROP_GROUP, 64, 0);
        g_seed = 0xC0DE; g_fault_flags = 0; g_fault_per_mille = 0;
        if (!ok) { failures++; goto out; }
        write_catalog(&f, 1, k_terminal);
        settle(&f, 16, false);
        CHECK(catalog_complete(&f));
        for (uint64_t o = 0; o < 4; o++) write_to(&f, f.pt[T_VIDEO], 0, o);
        settle(&f, 16, false);
        /* Everything so far is in: four Objects on one stream. */
        {
            moq_media_receiver_stats_t st;
            memset(&st, 0, sizeof(st));
            CHECK(moq_media_receiver_get_stats(f.r, &st, sizeof(st)) == MOQ_OK);
            CHECK(st.objects_queued == 4);
        }
        f.phase = 1;
        moq_simpair_enable_faults(f.sp);
        write_to(&f, f.pt[T_VIDEO], 0, 4);
        end_and_remove(&f, T_VIDEO);
        /* Every delayed input matures within a millisecond. */
        for (unsigned i = 0; i < 40; i++) { step(&f, 100); poll_tracks(&f); }
        /* The order the receiver's session took the stream inputs in: the
         * end's stream is the one not seen before the marker; the tail is
         * the old stream's first input after it, its FIN that stream's
         * last. */
        int i_eot = -1, i_tail = -1, i_fin = -1, i_old_last = -1;
        for (unsigned i = 0; i < f.in_n; i++) {
            if (f.in[i].phase != 1) continue;
            bool old_stream = false;
            for (unsigned j = 0; j < f.in_n; j++)
                old_stream |= f.in[j].phase == 0 && f.in[j].ref == f.in[i].ref;
            if (!old_stream) { if (i_eot < 0) i_eot = (int)i; continue; }
            if (i_tail < 0) i_tail = (int)i;
            if (f.in[i].fin && i_fin < 0) i_fin = (int)i;
            i_old_last = (int)i;
        }
        bool overtaken = i_eot >= 0 && i_tail >= 0 && i_fin >= 0 &&
                         i_eot < i_tail && i_fin == i_old_last;
        if (!overtaken) {
            fx_teardown(&f);
            memset(&f, 0, sizeof(f));
            continue;
        }
        found = true;
        settle(&f, 32, true);
        static const uint64_t want[5][2] = { { 0, 0 }, { 0, 1 }, { 0, 2 }, { 0, 3 }, { 0, 4 } };
        CHECK(objects_are(&f, T_VIDEO, want, 5));
        ended_then_removed(&f, T_VIDEO);
        CHECK(!f.overflow);
        if (failures != before)
            fprintf(stderr, "  %s: seed %llu, inputs eot=%d tail=%d fin=%d\n", row,
                    (unsigned long long)seed, i_eot, i_tail, i_fin);
    }
    CHECK(found);
out:
    if (failures != before) dump(&f);
    if (f.sp) fx_teardown(&f);
    if (failures == before) printf("PASS: %s\n", row);
}

int main(void)
{
    static const moq_version_t vers[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < sizeof(vers) / sizeof(vers[0]); i++) {
        test_overtake(vers[i]);
        test_full_lane_slow_poll(vers[i]);
        test_flow_control_counted(vers[i]);
        test_session_failure(vers[i]);
        test_multi(vers[i]);
        test_ended_then_removed(vers[i]);
        test_done_only_then_removed(vers[i], false);
        test_done_only_then_removed(vers[i], true);
        test_vod_replacement_tail(vers[i], false);
        test_vod_replacement_tail(vers[i], true);
        test_vod_replacement_empty(vers[i], false);
        test_vod_replacement_empty(vers[i], true);
        test_conversion_terminal_before_reconcile(vers[i]);
        test_conversion_original_tail(vers[i], false);
        test_conversion_original_tail(vers[i], true);
        test_waiting_conversion_catalogs(vers[i]);
        test_cancel_awaiting_conversion(vers[i]);
        test_cancel_pending(vers[i], false);
        test_cancel_pending(vers[i], true);
        test_cancel_before_terminal(vers[i], false);
        test_cancel_before_terminal(vers[i], true);
        test_tail_overtaken(vers[i]);
    }
    if (failures) {
        fprintf(stderr, "FAILURES: %u\n", failures);
        return 1;
    }
    return 0;
}
