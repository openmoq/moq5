/*
 * Input admission rows: stale rejection, released-history dedupe, entry
 * anchoring, the draft-16/18/22 LOC parse paths and next_deadline_us.
 *
 * Every row drives the engine through its public calls only and checks the
 * commands and events it emits, plus the buffered-object count and retained
 * bytes through the white-box helpers.
 */
#include <moq/playback.h>
#include <moq/loc.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb);
extern size_t moq_playback_test_buffered_count(const moq_playback_t *pb);
extern uint64_t moq_playback_test_anchor_group(const moq_playback_t *pb,
                                                moq_playback_track_t h);
extern uint64_t moq_playback_test_anchor_object(const moq_playback_t *pb,
                                                 moq_playback_track_t h);
extern moq_result_t moq_playback_test_push_cmd(moq_playback_t *pb,
                                                const moq_playback_cmd_t *cmd);
extern moq_result_t moq_playback_test_push_event(moq_playback_t *pb,
                                                  const moq_playback_event_t *evt);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Counting allocator ---------------------------------------------- */

typedef struct {
    int64_t  balance;
    uint64_t alloc_count;
    uint64_t fail_at;
} count_state_t;

static void *count_alloc(size_t sz, void *ctx)
{
    count_state_t *s = (count_state_t *)ctx;
    if (sz == 0) return NULL;
    s->alloc_count++;
    if (s->fail_at > 0 && s->alloc_count == s->fail_at) return NULL;
    void *p = malloc(sz);
    if (p) s->balance++;
    return p;
}

static void count_free(void *p, size_t sz, void *ctx)
{
    count_state_t *s = (count_state_t *)ctx;
    (void)sz;
    if (p) s->balance--;
    free(p);
}

static count_state_t g_state;
static moq_alloc_t g_alloc = { &g_state, count_alloc, NULL, count_free };

/* -- Engine helpers --------------------------------------------------- */

#define T0 UINT64_C(1700000000000000)
#define FRAME_US UINT64_C(33333)

static moq_playback_t *make_pb_with(uint32_t max_objects, uint32_t max_events)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    if (max_objects) cfg.max_buffered_objects = max_objects;
    if (max_events) cfg.max_events = max_events;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

static moq_playback_track_t add_track_v(moq_playback_t *pb,
                                        moq_playback_media_type_t media,
                                        moq_version_t version)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = media;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = (uint32_t)version;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    return t;
}

static moq_result_t poll_cmd(moq_playback_t *pb, moq_playback_cmd_t *c)
{
    return moq_playback_poll_command(pb, c, sizeof(*c));
}

static moq_result_t poll_evt(moq_playback_t *pb, moq_playback_event_t *e)
{
    return moq_playback_poll_event(pb, e, sizeof(*e));
}

/* Legacy LOC-01 properties (drafts 16/18): a UNIX-microsecond timestamp and,
 * when `marking` is set, an RFC 9626 frame marking with `independent`. */
static moq_rcbuf_t *loc01_props(moq_version_t v, uint64_t ts, bool marking,
                                bool independent)
{
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = ts;
    if (marking) {
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = independent;
        h.video_frame_marking.start_of_frame = true;
        h.video_frame_marking.end_of_frame = true;
    }
    moq_rcbuf_t *p = NULL;
    CHECK(moq_loc_encode(&g_alloc, v, MOQ_LOC_PROFILE_01, &h, &p) == MOQ_OK);
    return p;
}

/* LOC-04 properties (draft 22). `present` selects the fields written. */
static moq_rcbuf_t *loc04_props(uint32_t present, uint64_t ts,
                                uint64_t timescale, bool independent)
{
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = present;
    m.timestamp = ts;
    m.timescale = timescale;
    if (present & MOQ_LOC_METADATA_HAS_MARKING) {
        m.marking_len = 1;
        /* RFC 9626 short form: S, E, then I. */
        m.marking[0] = (uint8_t)(0xC0u | (independent ? 0x20u : 0u));
    }
    moq_rcbuf_t *p = NULL;
    CHECK(moq_loc_encode_metadata(&g_alloc, MOQ_VERSION_DRAFT_22,
                                  MOQ_LOC_PROFILE_04, &m, &p) == MOQ_OK);
    return p;
}

static moq_result_t push_props_at(moq_playback_t *pb, moq_playback_track_t t,
                                  uint64_t g, uint64_t o, moq_rcbuf_t *props,
                                  uint64_t now_us)
{
    static const uint8_t data[4] = { 1, 2, 3, 4 };
    moq_rcbuf_t *payload = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, data, sizeof(data), &payload) == MOQ_OK);
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = g;
    obj.object_id = o;
    obj.payload = payload;
    obj.properties = props;
    moq_result_t rc = moq_playback_push_object(pb, &obj, now_us);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    return rc;
}

static moq_result_t push_props(moq_playback_t *pb, moq_playback_track_t t,
                               uint64_t g, uint64_t o, moq_rcbuf_t *props)
{
    return push_props_at(pb, t, g, o, props, 0);
}

/* A draft-16 video object; `kf` marks it independent. */
static moq_result_t push_v(moq_playback_t *pb, moq_playback_track_t t,
                           uint64_t g, uint64_t o, bool kf)
{
    return push_props(pb, t, g, o,
        loc01_props(MOQ_VERSION_DRAFT_16, T0 + (g * 30 + o) * FRAME_US,
                    true, kf));
}

/* push_v arriving at now_us. */
static moq_result_t push_v_at(moq_playback_t *pb, moq_playback_track_t t,
                              uint64_t g, uint64_t o, bool kf, uint64_t now_us)
{
    return push_props_at(pb, t, g, o,
        loc01_props(MOQ_VERSION_DRAFT_16, T0 + (g * 30 + o) * FRAME_US,
                    true, kf), now_us);
}

/* A FIN of group g's stream after object `last`, for track t. */
static moq_result_t stream_fin(moq_playback_t *pb, moq_playback_track_t t,
                               uint64_t g, uint64_t last, uint64_t now_us)
{
    moq_playback_stream_terminal_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_PLAYBACK_STREAM_FIN;
    r.group_id = g;
    r.has_last_object = true;
    r.last_object_id = last;
    return moq_playback_on_stream_terminal(pb, t, &r, now_us);
}

static moq_result_t push_status(moq_playback_t *pb, moq_playback_track_t t,
                                uint64_t g, uint64_t o,
                                moq_object_status_t status)
{
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = g;
    obj.object_id = o;
    obj.status = status;
    return moq_playback_push_object(pb, &obj, 0);
}

/* Drain every command; record DECODE object ids in order. */
typedef struct {
    size_t   configures;
    size_t   decodes;
    uint64_t group[32];
    uint64_t object[32];
    uint64_t dts[32];
    bool     has_capture[32];
} cmd_log_t;

static void drain_cmds(moq_playback_t *pb, cmd_log_t *log)
{
    memset(log, 0, sizeof(*log));
    moq_playback_cmd_t c;
    while (poll_cmd(pb, &c) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO ||
            c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO) {
            log->configures++;
        } else if (c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO &&
                   log->decodes < 32) {
            log->group[log->decodes] = c.u.decode_video.group_id;
            log->object[log->decodes] = c.u.decode_video.object_id;
            log->dts[log->decodes] = c.u.decode_video.decode_time_us;
            log->has_capture[log->decodes] = c.u.decode_video.has_capture_time;
            log->decodes++;
        } else if (c.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO &&
                   log->decodes < 32) {
            log->group[log->decodes] = c.u.decode_audio.group_id;
            log->object[log->decodes] = c.u.decode_audio.object_id;
            log->dts[log->decodes] = c.u.decode_audio.decode_time_us;
            log->has_capture[log->decodes] = c.u.decode_audio.has_capture_time;
            log->decodes++;
        }
        moq_playback_cmd_cleanup(&c);
    }
}

/* Drain every event; count each kind and the drops by reason. */
typedef struct {
    size_t   by_kind[16];
    size_t   drops[16];
    uint64_t last_drop_group;
    uint64_t last_drop_object;
} evt_log_t;

static void drain_evts(moq_playback_t *pb, evt_log_t *log)
{
    memset(log, 0, sizeof(*log));
    moq_playback_event_t e;
    while (poll_evt(pb, &e) == MOQ_OK) {
        if (e.kind < 16) log->by_kind[e.kind]++;
        if (e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED) {
            if (e.u.object_dropped.reason < 16)
                log->drops[e.u.object_dropped.reason]++;
            log->last_drop_group = e.u.object_dropped.group_id;
            log->last_drop_object = e.u.object_dropped.object_id;
        }
    }
}

/* Release group 1 object 0, leaving the cursor at group 1 object 1. */
static moq_playback_track_t start_at_g1(moq_playback_t *pb)
{
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 1, 0, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.object[0] == 0);
    return t;
}

/* -- Stale rejection and dedupe -------------------------------------- */

/* A duplicate of an object the engine already released is absorbed
 * silently: it is not buffered and it produces no event. */
static void test_duplicate_after_release_absorbed(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = start_at_g1(pb);

    CHECK(push_v(pb, t, 1, 0, true) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);

    moq_playback_destroy(pb);
}

/* An object below the release cursor that the engine never held is dropped
 * with reason STALE and is not buffered. */
static void test_stale_object_dropped(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = start_at_g1(pb);

    CHECK(push_v(pb, t, 0, 4, false) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(el.last_drop_group == 0 && el.last_drop_object == 4);

    moq_playback_destroy(pb);
}

/* Stale objects never occupy buffer slots, so a full stream of late
 * arrivals cannot block admission of the object the cursor waits for. */
static void test_stale_objects_do_not_exhaust_buffer(void)
{
    moq_playback_t *pb = make_pb_with(2, 0);
    moq_playback_track_t t = start_at_g1(pb);

    CHECK(push_v(pb, t, 0, 1, false) == MOQ_OK);
    CHECK(push_v(pb, t, 0, 2, false) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 2);

    CHECK(push_v(pb, t, 1, 1, false) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.group[0] == 1 && cl.object[0] == 1);

    moq_playback_destroy(pb);
}

/* A status marker below the cursor carries no media; it is consumed
 * without buffering or an event. */
static void test_stale_status_marker_absorbed(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = start_at_g1(pb);

    CHECK(push_status(pb, t, 0, 9, MOQ_OBJECT_END_OF_GROUP) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);

    moq_playback_destroy(pb);
}

/* A STALE drop needs an event slot. With the event queue full the push is
 * refused with WOULD_BLOCK and nothing is retained; after the caller
 * drains, the same push succeeds and reports the drop. */
static void test_stale_drop_under_event_pressure(void)
{
    moq_playback_t *pb = make_pb_with(0, 1);
    moq_playback_track_t t = start_at_g1(pb);

    CHECK(push_v(pb, t, 0, 1, false) == MOQ_OK);   /* fills the ring */
    CHECK(push_v(pb, t, 0, 2, false) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);

    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(push_v(pb, t, 0, 2, false) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(el.last_drop_object == 2);

    moq_playback_destroy(pb);
}

/* The released history is bounded by max_buffered_objects. A duplicate of
 * an object still in the history is absorbed; one that has aged out is
 * indistinguishable from a late object and is reported STALE. */
static void test_released_history_is_bounded(void)
{
    moq_playback_t *pb = make_pb_with(2, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    for (uint64_t o = 0; o < 3; o++) {
        CHECK(push_v(pb, t, 1, o, o == 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    }
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 3);

    CHECK(push_v(pb, t, 1, 2, false) == MOQ_OK);   /* still remembered */
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);

    CHECK(push_v(pb, t, 1, 0, true) == MOQ_OK);    /* aged out */
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    moq_playback_destroy(pb);
}

/* -- Entry anchoring -------------------------------------------------- */

/* A live-edge object that arrives before its group's independent object
 * does not strand the group's prefix: until the engine releases anything,
 * an earlier independent object moves the entry back, and the dependent
 * objects after it are played in order. */
static void test_independent_earlier_object_reanchors(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 3, false) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 0, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 1, false) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 2, false) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.configures == 1);
    CHECK(cl.decodes == 4);
    for (size_t i = 0; i < cl.decodes && i < 4; i++)
        CHECK(cl.group[i] == 5 && cl.object[i] == i);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING] == 0);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);

    moq_playback_destroy(pb);
}

/* An earlier independent object also wins when the current entry is itself
 * independent. */
static void test_earlier_independent_object_wins(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 5, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 3, true) == MOQ_OK);
    CHECK(moq_playback_test_anchor_object(pb, t) == 3);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes >= 1 && cl.object[0] == 3);

    moq_playback_destroy(pb);
}

/* A dependent object below the entry never becomes the entry: the entry
 * stays on the independent object and the earlier dependent object is
 * dropped as STALE once release starts. */
static void test_dependent_earlier_object_does_not_reanchor(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 5, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 3, false) == MOQ_OK);
    CHECK(moq_playback_test_anchor_object(pb, t) == 5);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.object[0] == 5);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(el.last_drop_group == 5 && el.last_drop_object == 3);

    moq_playback_destroy(pb);
}

/* Object 0 carries no independence of its own: an object 0 whose marking
 * says it is dependent is not an entry. */
static void test_object_zero_marked_dependent_is_not_entry(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 5, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 0, false) == MOQ_OK);
    CHECK(moq_playback_test_anchor_object(pb, t) == 5);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.object[0] == 5);
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    moq_playback_destroy(pb);
}

/* Audio needs no independence evidence: any earlier unit becomes the entry
 * before release starts. */
static void test_audio_any_earlier_unit_reanchors(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_AUDIO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_props(pb, t, 2, 3,
        loc01_props(MOQ_VERSION_DRAFT_16, T0 + 3000, false, false)) == MOQ_OK);
    CHECK(push_props(pb, t, 2, 1,
        loc01_props(MOQ_VERSION_DRAFT_16, T0 + 1000, false, false)) == MOQ_OK);
    CHECK(moq_playback_test_anchor_object(pb, t) == 1);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes >= 1 && cl.object[0] == 1);

    moq_playback_destroy(pb);
}

/* Below-entry objects held before release are dropped one at a time, each
 * with its event, once the first action at the entry has committed. With
 * the event queue full, tick returns WOULD_BLOCK partway through the drops;
 * the entry's decode stands, and after the caller drains, the next tick
 * finishes the drops. */
static void test_entry_cleanup_under_event_pressure(void)
{
    moq_playback_t *pb = make_pb_with(0, 1);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 5, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 3, false) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 2, false) == MOQ_OK);

    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.object[0] == 5);
    CHECK(moq_playback_test_buffered_count(pb) == 1);

    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    moq_playback_destroy(pb);
}

/* -- Initial entry, on every draft parse path ------------------------- */

static const moq_version_t k_versions[3] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22
};

/* A video object on draft `v` with a UNIX-microsecond timestamp and a
 * frame marking saying whether it is independent. */
static moq_result_t push_vv(moq_playback_t *pb, moq_playback_track_t t,
                            moq_version_t v, uint64_t g, uint64_t o, bool kf)
{
    uint64_t ts = T0 + (g * 30 + o) * FRAME_US;
    moq_rcbuf_t *props = (v == MOQ_VERSION_DRAFT_22)
        ? loc04_props(MOQ_LOC_METADATA_HAS_TIMESTAMP |
                      MOQ_LOC_METADATA_HAS_MARKING, ts, 0, kf)
        : loc01_props(v, ts, true, kf);
    return push_props(pb, t, g, o, props);
}

/* A dependent first arrival is not an entry: a later, higher independent
 * object becomes the entry, and the dependent object below it is dropped
 * STALE when release starts. */
static void test_dependent_first_then_higher_independent(void)
{
    for (size_t i = 0; i < 3; i++) {
        moq_version_t v = k_versions[i];
        moq_playback_t *pb = make_pb_with(0, 0);
        moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, v);
        CHECK(push_vv(pb, t, v, 5, 3, false) == MOQ_OK);
        CHECK(push_vv(pb, t, v, 5, 5, true) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 1 && cl.group[0] == 5 && cl.object[0] == 5);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        evt_log_t el;
        drain_evts(pb, &el);
        CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
        CHECK(el.last_drop_object == 3);
        moq_playback_destroy(pb);
    }
}

/* Without any independent object nothing is released and nothing is lost:
 * the dependent objects wait, and the first independent object, in a later
 * group, becomes the entry. */
static void test_no_entry_yet_waits(void)
{
    for (size_t i = 0; i < 3; i++) {
        moq_version_t v = k_versions[i];
        moq_playback_t *pb = make_pb_with(0, 0);
        moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, v);
        CHECK(push_vv(pb, t, v, 5, 3, false) == MOQ_OK);
        CHECK(push_vv(pb, t, v, 5, 4, false) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 10000000) == MOQ_OK);
        moq_playback_cmd_t c;
        CHECK(poll_cmd(pb, &c) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);

        CHECK(push_vv(pb, t, v, 6, 0, true) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 10000000) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 1 && cl.group[0] == 6 && cl.object[0] == 0);
        evt_log_t el;
        drain_evts(pb, &el);
        CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        moq_playback_destroy(pb);
    }
}

/* Positive control: an independent first arrival is the entry and its
 * dependents follow it. */
static void test_independent_first_arrival(void)
{
    for (size_t i = 0; i < 3; i++) {
        moq_version_t v = k_versions[i];
        moq_playback_t *pb = make_pb_with(0, 0);
        moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, v);
        CHECK(push_vv(pb, t, v, 5, 0, true) == MOQ_OK);
        CHECK(push_vv(pb, t, v, 5, 1, false) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 2 && cl.object[0] == 0 && cl.object[1] == 1);
        moq_playback_destroy(pb);
    }
}

/* Audio: any first arrival is the entry and is released at once. */
static void test_audio_first_arrival_is_entry(void)
{
    for (size_t i = 0; i < 3; i++) {
        moq_version_t v = k_versions[i];
        moq_playback_t *pb = make_pb_with(0, 0);
        moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_AUDIO, v);
        moq_rcbuf_t *props = (v == MOQ_VERSION_DRAFT_22)
            ? loc04_props(MOQ_LOC_METADATA_HAS_TIMESTAMP, T0, 0, false)
            : loc01_props(v, T0, false, false);
        CHECK(push_props(pb, t, 2, 3, props) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 1 && cl.object[0] == 3);
        moq_playback_destroy(pb);
    }
}

/* Status markers are not entries. An END_OF_GROUP marker waits for an
 * entry; an END_OF_TRACK marker with no entry ends the track when tick
 * reaches it. */
static void test_status_markers_before_entry(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_status(pb, t, 5, 9, MOQ_OBJECT_END_OF_GROUP) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 0, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 1, false) == MOQ_OK);
    CHECK(moq_playback_test_anchor_object(pb, t) == 0);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 2 && cl.object[0] == 0 && cl.object[1] == 1);
    moq_playback_destroy(pb);

    pb = make_pb_with(0, 0);
    t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 9, 1, false) == MOQ_OK);
    CHECK(push_status(pb, t, 9, 2, MOQ_OBJECT_END_OF_TRACK) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    moq_playback_destroy(pb);
}

/* -- Buffer pressure from tracks without an entry --------------------- */

static moq_playback_t *bounded_pb(uint32_t objects, uint64_t bytes,
                                  uint32_t events)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    if (objects) cfg.max_buffered_objects = objects;
    if (bytes) cfg.max_buffered_bytes = bytes;
    if (events) cfg.max_events = events;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

/* A draft-16 video object with a payload of `size` bytes; `payload_out`,
 * when set, receives one extra ref for the caller to check and release. */
static moq_result_t push_sized(moq_playback_t *pb, moq_playback_track_t t,
                               uint64_t g, uint64_t o, bool kf, size_t size,
                               moq_rcbuf_t **payload_out)
{
    static const uint8_t data[16] = { 0 };
    moq_rcbuf_t *payload = NULL;
    CHECK(size <= sizeof(data));
    CHECK(moq_rcbuf_create(&g_alloc, data, size, &payload) == MOQ_OK);
    moq_rcbuf_t *props = loc01_props(MOQ_VERSION_DRAFT_16,
        T0 + (g * 30 + o) * FRAME_US, true, kf);
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = g;
    obj.object_id = o;
    obj.payload = payload;
    obj.properties = props;
    moq_result_t rc = moq_playback_push_object(pb, &obj, 0);
    moq_rcbuf_decref(props);
    if (payload_out) *payload_out = payload;
    else moq_rcbuf_decref(payload);
    return rc;
}

/* An object larger than the whole byte budget is refused MOQ_ERR_LIMIT and
 * changes nothing: the buffered objects, their refs, the retained bytes
 * and the event queue are as they were, and tick drops nothing for it. */
static void test_refused_push_has_no_effect(void)
{
    moq_playback_t *pb = bounded_pb(0, 8, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_rcbuf_t *p1 = NULL, *p2 = NULL;
    CHECK(push_sized(pb, t, 5, 1, false, 4, &p1) == MOQ_OK);
    CHECK(push_sized(pb, t, 5, 2, false, 4, &p2) == MOQ_OK);
    CHECK(push_sized(pb, t, 5, 3, true, 9, NULL) == MOQ_ERR_LIMIT);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    CHECK(moq_playback_test_retained_bytes(pb) == 8);
    CHECK(moq_rcbuf_refcount(p1) == 2 && moq_rcbuf_refcount(p2) == 2);
    moq_playback_event_t e;
    CHECK(poll_evt(pb, &e) == MOQ_DONE);

    /* Impossible: nothing is dropped to make room for it. (The track
     * reports that it waits for a keyframe; that is all.) */
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING] == 1);

    moq_rcbuf_decref(p1);
    moq_rcbuf_decref(p2);
    moq_playback_destroy(pb);
}

/* Byte pressure with a one-slot event queue: the refused push changes
 * nothing; ticks then drop the waiting objects one per free event slot,
 * oldest first, each naming the track, and a retry is admitted and
 * played. The queue holds one event, so each step drains it first. */
static void test_byte_pressure_relieved_by_tick(void)
{
    moq_playback_t *pb = bounded_pb(0, 8, 1);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_sized(pb, t, 5, 1, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, t, 5, 2, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, t, 6, 0, true, 8, NULL) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    CHECK(moq_playback_test_retained_bytes(pb) == 8);
    moq_playback_event_t e;
    CHECK(poll_evt(pb, &e) == MOQ_DONE);

    uint64_t dropped[2] = { 0, 0 };
    size_t drops = 0;
    for (int step = 0; step < 6 && drops < 2; step++) {
        (void)moq_playback_tick(pb, 0);
        size_t polled = 0;
        while (poll_evt(pb, &e) == MOQ_OK) {
            polled++;
            if (e.kind != MOQ_PLAYBACK_EVENT_OBJECT_DROPPED) continue;
            CHECK(e.u.object_dropped.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
            CHECK(e.track._opaque == t._opaque);
            if (drops < 2) dropped[drops] = e.u.object_dropped.object_id;
            drops++;
        }
        CHECK(polled <= 1);
    }
    CHECK(drops == 2 && dropped[0] == 1 && dropped[1] == 2);
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    CHECK(push_sized(pb, t, 6, 0, true, 8, NULL) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.group[0] == 6);
    moq_playback_destroy(pb);
}

/* Object-count pressure, same contract. */
static void test_count_pressure_relieved_by_tick(void)
{
    moq_playback_t *pb = bounded_pb(2, 0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 1, false) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 2, false) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 3, false) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    moq_playback_event_t e;
    CHECK(poll_evt(pb, &e) == MOQ_DONE);

    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_KEYFRAME_WAIT] == 1);
    CHECK(el.last_drop_object == 1);
    CHECK(push_v(pb, t, 5, 3, false) == MOQ_OK);
    CHECK(push_v(pb, t, 6, 0, true) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(push_v(pb, t, 6, 0, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.group[0] == 6);
    moq_playback_destroy(pb);
}

/* One track's waiting objects cannot block another track: tick drops
 * track A's waiting objects to admit track B's entry, and each drop names
 * track A. By object count and by bytes. */
static void test_other_track_waiting_objects_make_room(void)
{
    for (int by_bytes = 0; by_bytes < 2; by_bytes++) {
        moq_playback_t *pb = by_bytes ? bounded_pb(0, 8, 0)
                                      : bounded_pb(2, 0, 0);
        moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                             MOQ_VERSION_DRAFT_16);
        moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                             MOQ_VERSION_DRAFT_16);
        CHECK(push_sized(pb, a, 5, 1, false, 4, NULL) == MOQ_OK);
        CHECK(push_sized(pb, a, 5, 2, false, 4, NULL) == MOQ_OK);
        CHECK(push_sized(pb, b, 5, 0, true, 4, NULL) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        evt_log_t el;
        moq_playback_event_t e;
        size_t a_drops = 0;
        memset(&el, 0, sizeof(el));
        while (poll_evt(pb, &e) == MOQ_OK)
            if (e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED &&
                e.track._opaque == a._opaque)
                a_drops++;
        CHECK(a_drops == 1);
        CHECK(push_sized(pb, b, 5, 0, true, 4, NULL) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 1);
        moq_playback_destroy(pb);
    }
}

/* Positive control: a track that has an entry is never dropped from for
 * room. Track A has released its entry and holds two objects waiting on a
 * missing one; track B's entry stays refused. */
static void test_anchored_track_is_not_dropped_for_room(void)
{
    moq_playback_t *pb = bounded_pb(2, 0, 0);
    moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, a, 1, 0, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(push_v(pb, a, 1, 2, false) == MOQ_OK);
    CHECK(push_v(pb, a, 1, 3, false) == MOQ_OK);
    CHECK(push_v(pb, b, 5, 0, true) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    CHECK(push_v(pb, b, 5, 0, true) == MOQ_ERR_WOULD_BLOCK);
    moq_playback_destroy(pb);

    /* Mixed: track A has an entry and holds two waiting objects, track C
     * has none and holds one. Room for track B comes from C, although A
     * holds more. */
    pb = bounded_pb(3, 0, 0);
    a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_VERSION_DRAFT_16);
    b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_VERSION_DRAFT_16);
    moq_playback_track_t c = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, a, 1, 0, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &cl);
    CHECK(push_v(pb, a, 1, 2, false) == MOQ_OK);
    CHECK(push_v(pb, a, 1, 3, false) == MOQ_OK);
    CHECK(push_v(pb, c, 7, 4, false) == MOQ_OK);
    CHECK(push_v(pb, b, 5, 0, true) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_event_t e;
    size_t c_drops = 0, other_drops = 0;
    while (poll_evt(pb, &e) == MOQ_OK) {
        if (e.kind != MOQ_PLAYBACK_EVENT_OBJECT_DROPPED) continue;
        if (e.track._opaque == c._opaque) c_drops++;
        else other_drops++;
    }
    CHECK(c_drops == 1 && other_drops == 0);
    CHECK(push_v(pb, b, 5, 0, true) == MOQ_OK);
    moq_playback_destroy(pb);

    /* Insufficient: C's 4 bytes cannot make room for an 8-byte object while
     * A's 8 bytes are protected, so nothing is dropped at all. */
    pb = bounded_pb(0, 12, 0);
    a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_VERSION_DRAFT_16);
    b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_VERSION_DRAFT_16);
    c = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_VERSION_DRAFT_16);
    CHECK(push_sized(pb, a, 1, 0, true, 4, NULL) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &cl);
    CHECK(push_sized(pb, a, 1, 2, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, a, 1, 3, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, c, 7, 4, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, b, 5, 0, true, 8, NULL) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 3);
    evt_log_t el2;
    drain_evts(pb, &el2);
    CHECK(el2.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    moq_playback_destroy(pb);
}

/* -- Lifetime of the refused-object note ------------------------------- */

/* The refused object's track is removed, or removed and its slot reused by
 * a new track: there is no request left, so tick drops nothing. */
static void test_removed_requester_does_not_evict(void)
{
    for (int reuse = 0; reuse < 2; reuse++) {
        moq_playback_t *pb = bounded_pb(2, 0, 0);
        moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                             MOQ_VERSION_DRAFT_16);
        moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                             MOQ_VERSION_DRAFT_16);
        CHECK(push_v(pb, a, 5, 1, false) == MOQ_OK);
        CHECK(push_v(pb, a, 5, 2, false) == MOQ_OK);
        CHECK(push_v(pb, b, 5, 0, true) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_remove_track(pb, b) == MOQ_OK);
        if (reuse) {
            moq_playback_track_t r = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                                 MOQ_VERSION_DRAFT_16);
            CHECK(r._opaque != b._opaque);
        }
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        evt_log_t el;
        drain_evts(pb, &el);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
        moq_playback_destroy(pb);
    }
}

/* The refused object's track ends before relief: an ended track needs no
 * room. The marker that ends it holds no payload, so ending it frees no
 * bytes; only the ended owner makes the request moot. */
static void test_ended_requester_does_not_evict(void)
{
    moq_playback_t *pb = bounded_pb(0, 8, 0);
    moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_sized(pb, b, 1, 0, true, 1, NULL) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(push_status(pb, b, 1, 1, MOQ_OBJECT_END_OF_TRACK) == MOQ_OK);
    CHECK(push_sized(pb, a, 5, 1, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, a, 5, 2, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, b, 1, 2, false, 4, NULL) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    moq_playback_destroy(pb);
}

/* The refused object's track moves its cursor past it (here the gap timer
 * abandons its group) in the same tick that would make room: the request
 * is settled, so nothing is dropped for it, then or later. The retry is
 * dropped STALE, or refused only for an event slot; neither revives the
 * request. */
static void test_settled_retry_clears_request(void)
{
    for (int event_blocked = 0; event_blocked < 2; event_blocked++) {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_objects = 2;
        cfg.gap_timeout_us = 100;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                             MOQ_VERSION_DRAFT_16);
        moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                             MOQ_VERSION_DRAFT_16);
        CHECK(push_v(pb, b, 1, 0, true) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        CHECK(push_v(pb, a, 5, 1, false) == MOQ_OK);
        CHECK(push_v(pb, a, 5, 2, false) == MOQ_OK);
        CHECK(push_v(pb, b, 1, 1, false) == MOQ_ERR_WOULD_BLOCK);
        /* Group 1's stream ended after object 1: delivery is past b's
         * cursor, so b's wait runs. */
        CHECK(stream_fin(pb, b, 1, 1, 0) == MOQ_OK);
        /* b's gap timer abandons group 1; relief then finds the request
         * settled and drops nothing. */
        CHECK(moq_playback_tick(pb, 101) == MOQ_OK);
        CHECK(moq_playback_test_anchor_group(pb, b) == 2);
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        if (event_blocked) {
            for (uint32_t i = 0; i < cfg.max_events; i++) {
                moq_playback_event_t f;
                memset(&f, 0, sizeof(f));
                f.struct_size = sizeof(f);
                f.kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED;
                CHECK(moq_playback_test_push_event(pb, &f) == MOQ_OK);
            }
            CHECK(push_v(pb, b, 1, 1, false) == MOQ_ERR_WOULD_BLOCK);
            drain_evts(pb, &el);
        } else {
            CHECK(push_v(pb, b, 1, 1, false) == MOQ_OK);
            drain_evts(pb, &el);
            CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
        }
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        CHECK(moq_playback_tick(pb, 101) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        drain_evts(pb, &el);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
        moq_playback_destroy(pb);
    }
}

/* The refused object is admitted once relief made room, and stays buffered
 * (its track has no entry yet). The request is settled: the next tick drops
 * nothing more for it. */
static void test_admitted_request_is_settled(void)
{
    moq_playback_t *pb = bounded_pb(2, 0, 0);
    moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, a, 5, 1, false) == MOQ_OK);
    CHECK(push_v(pb, a, 5, 2, false) == MOQ_OK);
    CHECK(push_v(pb, b, 3, 4, false) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_KEYFRAME_WAIT] == 1);
    CHECK(push_v(pb, b, 3, 4, false) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    moq_playback_destroy(pb);
}

/* With a one-slot event queue relief spans several ticks. When the
 * requester is removed between them, the remaining relief is abandoned:
 * one object was dropped for a live request, no more. */
static void test_requester_retired_between_relief_ticks(void)
{
    moq_playback_t *pb = bounded_pb(0, 8, 1);
    moq_playback_track_t a = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_playback_track_t b = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_sized(pb, a, 5, 1, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, a, 5, 2, false, 4, NULL) == MOQ_OK);
    CHECK(push_sized(pb, b, 6, 0, true, 8, NULL) == MOQ_ERR_WOULD_BLOCK);
    size_t drops = 0;
    moq_playback_event_t e;
    for (int i = 0; i < 4 && drops == 0; i++) {
        (void)moq_playback_tick(pb, 0);
        while (poll_evt(pb, &e) == MOQ_OK)
            if (e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED) drops++;
    }
    CHECK(drops == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
    CHECK(moq_playback_remove_track(pb, b) == MOQ_OK);
    for (int i = 0; i < 3; i++) {
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        while (poll_evt(pb, &e) == MOQ_OK)
            if (e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED) drops++;
    }
    CHECK(drops == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
    moq_playback_destroy(pb);
}

/* -- Release start is the first committed action ---------------------- */

/* A tick that can commit only the CONFIGURE (one free command slot)
 * consumes no object, so release has not started: an earlier independent
 * object arriving afterwards still becomes the entry rather than being
 * reported STALE. */
static void test_blocked_first_decode_does_not_start_release(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_commands = 2;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_playback_cmd_t filler;
    memset(&filler, 0, sizeof(filler));
    filler.struct_size = sizeof(filler);
    filler.kind = MOQ_PLAYBACK_CMD_RESET;
    CHECK(moq_playback_test_push_cmd(pb, &filler) == MOQ_OK);

    CHECK(push_v(pb, t, 5, 5, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
    moq_playback_cmd_t c;
    CHECK(poll_cmd(pb, &c) == MOQ_OK && c.kind == MOQ_PLAYBACK_CMD_RESET);
    moq_playback_cmd_cleanup(&c);
    CHECK(poll_cmd(pb, &c) == MOQ_OK &&
          c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
    moq_playback_cmd_cleanup(&c);
    CHECK(poll_cmd(pb, &c) == MOQ_DONE);

    CHECK(push_v(pb, t, 5, 3, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes >= 1 && cl.object[0] == 3);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 0);
    moq_playback_destroy(pb);
}

/* The same for event capacity: a track whose only object is an END_OF_TRACK
 * marker needs an event slot for TRACK_ENDED; refused, it consumes nothing,
 * and an independent object arriving below the marker becomes the entry. */
static void test_blocked_first_event_does_not_start_release(void)
{
    moq_playback_t *pb = make_pb_with(0, 1);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    moq_playback_event_t filler;
    memset(&filler, 0, sizeof(filler));
    filler.struct_size = sizeof(filler);
    filler.kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED;
    CHECK(moq_playback_test_push_event(pb, &filler) == MOQ_OK);

    CHECK(push_status(pb, t, 9, 0, MOQ_OBJECT_END_OF_TRACK) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
    moq_playback_event_t e;
    CHECK(poll_evt(pb, &e) == MOQ_OK && e.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED);

    CHECK(push_v(pb, t, 5, 0, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.group[0] == 5 && cl.object[0] == 0);
    moq_playback_destroy(pb);
}

/* Once a first action commits, release has started even if the same tick
 * then blocks: the object it released is not offered again, and an object
 * below the cursor is STALE. */
static void test_committed_action_starts_release(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_commands = 2;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 5, 5, true) == MOQ_OK);
    CHECK(push_v(pb, t, 5, 6, false) == MOQ_OK);
    /* CONFIGURE + DECODE(5) fill the queue; DECODE(6) blocks. */
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.object[0] == 5);
    CHECK(push_v(pb, t, 5, 3, true) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_STALE] == 1);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.object[0] == 6);
    moq_playback_destroy(pb);
}

/* -- Draft-specific LOC parsing --------------------------------------- */

/* A draft-18 track parses its objects' LOC properties with the draft-18
 * integer codec, so a UNIX-microsecond timestamp keeps its value. */
static void test_draft18_timestamp(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_18);
    CHECK(push_props(pb, t, 1, 0,
        loc01_props(MOQ_VERSION_DRAFT_18, T0, true, true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1);
    CHECK(cl.dts[0] == T0);
    CHECK(cl.has_capture[0]);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    moq_playback_destroy(pb);
}

/* A draft-22 object whose LOC-04 timestamp has a timescale carries
 * application media time: it is playable although it has no capture
 * time, and its decode time is the scaled timestamp. */
static void test_draft22_application_time(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_22);
    CHECK(push_props(pb, t, 1, 0,
        loc04_props(MOQ_LOC_METADATA_HAS_TIMESTAMP |
                    MOQ_LOC_METADATA_HAS_TIMESCALE |
                    MOQ_LOC_METADATA_HAS_MARKING,
                    90000, 90000, true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1);
    CHECK(cl.dts[0] == 1000000);
    CHECK(!cl.has_capture[0]);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 0);
    moq_playback_destroy(pb);
}

/* A draft-22 timestamp without a timescale is UNIX microseconds. */
static void test_draft22_unix_time(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_22);
    CHECK(push_props(pb, t, 1, 0,
        loc04_props(MOQ_LOC_METADATA_HAS_TIMESTAMP |
                    MOQ_LOC_METADATA_HAS_MARKING, T0, 0, true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1);
    CHECK(cl.dts[0] == T0);
    CHECK(cl.has_capture[0]);
    moq_playback_destroy(pb);
}

/* -- Rows that need the appended track-config fields ------------------ */

/* A track-level LOC-04 timescale applies to objects that carry only a
 * timestamp. */
static void test_draft22_track_timescale_inherited(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_rcbuf_t *track_props = loc04_props(MOQ_LOC_METADATA_HAS_TIMESCALE,
                                           0, 1000, false);
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = MOQ_VERSION_DRAFT_22;
    tc.track_properties.data = moq_rcbuf_data(track_props);
    tc.track_properties.len = moq_rcbuf_len(track_props);
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    moq_rcbuf_decref(track_props);   /* copied at add_track */

    CHECK(push_props(pb, t, 1, 0,
        loc04_props(MOQ_LOC_METADATA_HAS_TIMESTAMP |
                    MOQ_LOC_METADATA_HAS_MARKING, 2500, 0, true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1);
    CHECK(cl.dts[0] == 2500000);
    CHECK(!cl.has_capture[0]);
    moq_playback_destroy(pb);
}

/* A draft-22 object without usable media time is dropped MISSING_TIMESTAMP:
 * no timestamp at all, or a zero timescale that leaves the time unusable. */
static void test_draft22_unusable_time_dropped(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_22);
    CHECK(push_props(pb, t, 1, 0,
        loc04_props(MOQ_LOC_METADATA_HAS_MARKING, 0, 0, true)) == MOQ_OK);
    CHECK(push_props(pb, t, 1, 1,
        loc04_props(MOQ_LOC_METADATA_HAS_TIMESTAMP |
                    MOQ_LOC_METADATA_HAS_TIMESCALE |
                    MOQ_LOC_METADATA_HAS_MARKING, 5, 0, false)) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP] == 2);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    moq_playback_destroy(pb);
}

/* A draft-22 object that repeats a known LOC-04 property is malformed peer
 * data: it is dropped MALFORMED_LOC, not refused as a caller error. The
 * block repeats the frame marking (key 9, zero key delta the second time)
 * before the timestamp. */
static void test_draft22_repeated_property_dropped(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_22);
    static const uint8_t dup[9] = { 0x09, 0x01, 0xe0, 0x00, 0x01, 0xe0,
                                    0x07, 0x83, 0xe8 };
    moq_rcbuf_t *props = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, dup, sizeof(dup), &props) == MOQ_OK);
    CHECK(push_props(pb, t, 1, 0, props) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.drops[MOQ_PLAYBACK_DROP_MALFORMED_LOC] == 1);
    moq_playback_destroy(pb);
}

/* Track-config validation of the appended fields: a caller whose struct
 * ends at the v0 floor gets the draft-16 contract; a covered version must
 * be a supported draft; track properties exist only on draft 22. */
static void test_track_cfg_version_rules(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    moq_playback_track_cfg_t tc;
    moq_playback_track_t t;

    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    CHECK(tc.transport_version == MOQ_VERSION_DRAFT_16);

    tc.transport_version = 0;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
    tc.transport_version = 17;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);

    static const uint8_t props[2] = { 0, 0 };
    tc.transport_version = MOQ_VERSION_DRAFT_18;
    tc.track_properties.data = props;
    tc.track_properties.len = sizeof(props);
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
    tc.track_properties.data = NULL;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
    tc.track_properties.len = 0;

    /* An old caller: version and properties beyond its struct_size are
     * never read, whatever the bytes there hold. */
    tc.transport_version = 99;
    tc.track_properties.data = props;
    tc.track_properties.len = sizeof(props);
    tc.struct_size = (uint32_t)MOQ_PLAYBACK_TRACK_CFG_V0_SIZE;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    CHECK(push_v(pb, t, 1, 0, true) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.dts[0] == T0 + 30 * FRAME_US);

    moq_playback_destroy(pb);
}

/* Malformed draft-22 track properties are refused at add_track, and a
 * failed copy allocation consumes no track slot and leaks nothing. */
static void test_track_properties_refusals(void)
{
    moq_playback_t *pb = make_pb_with(0, 0);
    static const uint8_t junk[3] = { 0xff, 0xff, 0xff };
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = MOQ_VERSION_DRAFT_22;
    tc.track_properties.data = junk;
    tc.track_properties.len = sizeof(junk);
    moq_playback_track_t t;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_PROTO);

    moq_rcbuf_t *props = loc04_props(MOQ_LOC_METADATA_HAS_TIMESCALE, 0,
                                     1000, false);
    tc.track_properties.data = moq_rcbuf_data(props);
    tc.track_properties.len = moq_rcbuf_len(props);
    int64_t before = g_state.balance;
    g_state.fail_at = g_state.alloc_count + 1;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_NOMEM);
    g_state.fail_at = 0;
    CHECK(g_state.balance == before);
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    moq_rcbuf_decref(props);

    moq_playback_destroy(pb);
}

/* -- next_deadline_us ------------------------------------------------- */

/* The only time-driven work is the gap timer. No timer: UINT64_MAX. A
 * started timer: its expiry, measured from the evidence that delivery
 * moved past the missing id (here the later object's arrival). A track
 * paused by decoder pressure has no deadline. When the timer fires,
 * abandoning group 1 moves the cursor to group 2, where nothing shows an
 * object missing yet: no deadline. */
static void test_next_deadline(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.gap_timeout_us = 500;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    CHECK(moq_playback_next_deadline_us(NULL) == UINT64_MAX);

    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 1, 0, true) == MOQ_OK);
    CHECK(push_v_at(pb, t, 1, 2, false, 1000) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    CHECK(moq_playback_tick(pb, 1000) == MOQ_OK);   /* waits on object 1 */
    CHECK(moq_playback_next_deadline_us(pb) == 1500);

    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
    fb.track = t;
    fb.u.queue_pressure.depth = 9;
    fb.u.queue_pressure.max_recommended = 1;
    CHECK(moq_playback_handle_feedback(pb, &fb, 1000) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    fb.u.queue_pressure.depth = 0;
    CHECK(moq_playback_handle_feedback(pb, &fb, 1000) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == 1500);

    CHECK(moq_playback_tick(pb, 1500) == MOQ_OK);   /* timer fires */
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    moq_playback_destroy(pb);
}

/* A timer whose expiry cannot be represented reports no deadline rather
 * than a wrapped one. */
static void test_next_deadline_overflow(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.gap_timeout_us = UINT64_MAX - 10;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t t = add_track_v(pb, MOQ_PLAYBACK_MEDIA_VIDEO,
                                         MOQ_VERSION_DRAFT_16);
    CHECK(push_v(pb, t, 1, 0, true) == MOQ_OK);
    CHECK(push_v_at(pb, t, 1, 2, false, 1000) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 1000) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    moq_playback_destroy(pb);
}

int main(void)
{
    test_duplicate_after_release_absorbed();
    test_stale_object_dropped();
    test_stale_objects_do_not_exhaust_buffer();
    test_stale_status_marker_absorbed();
    test_stale_drop_under_event_pressure();
    test_released_history_is_bounded();
    test_independent_earlier_object_reanchors();
    test_earlier_independent_object_wins();
    test_dependent_earlier_object_does_not_reanchor();
    test_object_zero_marked_dependent_is_not_entry();
    test_audio_any_earlier_unit_reanchors();
    test_entry_cleanup_under_event_pressure();
    test_dependent_first_then_higher_independent();
    test_no_entry_yet_waits();
    test_independent_first_arrival();
    test_audio_first_arrival_is_entry();
    test_status_markers_before_entry();
    test_refused_push_has_no_effect();
    test_byte_pressure_relieved_by_tick();
    test_count_pressure_relieved_by_tick();
    test_other_track_waiting_objects_make_room();
    test_anchored_track_is_not_dropped_for_room();
    test_removed_requester_does_not_evict();
    test_ended_requester_does_not_evict();
    test_settled_retry_clears_request();
    test_admitted_request_is_settled();
    test_requester_retired_between_relief_ticks();
    test_blocked_first_decode_does_not_start_release();
    test_blocked_first_event_does_not_start_release();
    test_committed_action_starts_release();
    test_draft18_timestamp();
    test_draft22_application_time();
    test_draft22_unix_time();
    test_draft22_track_timescale_inherited();
    test_draft22_unusable_time_dropped();
    test_draft22_repeated_property_dropped();
    test_track_cfg_version_rules();
    test_track_properties_refusals();
    test_next_deadline();
    test_next_deadline_overflow();

    CHECK(g_state.balance == 0);
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("playback input: all passed\n");
    return 0;
}
