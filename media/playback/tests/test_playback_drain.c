/*
 * moq_playback_drain_track: a track whose input is over releases what it
 * holds in decode order and ends.
 *
 * Pinned:
 * - a missing unit is not waited for: the gap policy's step is taken at
 *   once, with GAP_DETECTED, where an undrained track keeps waiting;
 * - with nothing missing, every held unit is released, then TRACK_ENDED;
 * - a video track with no entry drops each held object KEYFRAME_WAIT, then
 *   ends, releasing nothing;
 * - CONTINUE audio crosses the hole with GAP_CONTINUED, without a reset;
 * - QUEUE_PRESSURE still pauses a drained track;
 * - a one-slot event queue progresses to the end across ticks;
 * - an object pushed after the drain is still released before the end;
 * - drain is idempotent, refuses NULL and a stale handle, and leaves no
 *   deadline;
 * - a settled track waits for nothing either, but goes on taking and
 *   releasing input and never ends by itself; a later drain ends it.
 */
#include <moq/playback.h>
#include <moq/loc.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

typedef struct {
    int64_t  balance;
} count_state_t;

static void *count_alloc(size_t sz, void *ctx)
{
    count_state_t *s = (count_state_t *)ctx;
    void *p = sz ? malloc(sz) : NULL;
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

#define T0       UINT64_C(1700000000000000)
#define FRAME_US UINT64_C(33333)
#define WAIT_US  UINT64_C(1000)

static moq_playback_t *new_pb(uint32_t max_ranges, uint32_t max_events)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.gap_timeout_us = WAIT_US;
    cfg.max_known_ranges = max_ranges;
    if (max_events) cfg.max_events = max_events;
    cfg.max_backlog_groups = 0;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

static moq_playback_track_t add_track(moq_playback_t *pb,
                                      moq_playback_media_type_t media,
                                      moq_playback_gap_policy_t policy)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = media;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.gap_policy = policy;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    return t;
}

static moq_playback_track_t add_video(moq_playback_t *pb)
{
    return add_track(pb, MOQ_PLAYBACK_MEDIA_VIDEO, MOQ_PLAYBACK_GAP_DISCONTINUITY);
}

/* Object (g, o) at now_us: a LOC-01 timestamp and, for video, a marking
 * saying whether it is independent. */
static moq_result_t push_at(moq_playback_t *pb, moq_playback_track_t t,
                            uint64_t g, uint64_t o, bool independent,
                            uint64_t now_us)
{
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = T0 + (g * 30 + o) * FRAME_US;
    h.has_video_frame_marking = true;
    h.video_frame_marking.independent = independent;
    h.video_frame_marking.start_of_frame = true;
    h.video_frame_marking.end_of_frame = true;
    moq_rcbuf_t *props = NULL;
    CHECK(moq_loc_encode(&g_alloc, MOQ_VERSION_DRAFT_16, MOQ_LOC_PROFILE_01,
                         &h, &props) == MOQ_OK);
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

typedef struct {
    size_t   resets;
    size_t   decodes;
    uint64_t group[32];
    uint64_t object[32];
    uint64_t dts[32];
    size_t   reset_before[32];   /* resets seen before each decode */
} cmd_log_t;

static void drain_cmds(moq_playback_t *pb, cmd_log_t *log)
{
    memset(log, 0, sizeof(*log));
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_RESET) {
            log->resets++;
        } else if ((c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO ||
                    c.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO) && log->decodes < 32) {
            bool v = c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO;
            log->group[log->decodes] = v ? c.u.decode_video.group_id
                                         : c.u.decode_audio.group_id;
            log->object[log->decodes] = v ? c.u.decode_video.object_id
                                          : c.u.decode_audio.object_id;
            log->dts[log->decodes] = v ? c.u.decode_video.decode_time_us
                                       : c.u.decode_audio.decode_time_us;
            log->reset_before[log->decodes] = log->resets;
            log->decodes++;
        }
        moq_playback_cmd_cleanup(&c);
    }
}

typedef struct {
    size_t by_kind[16];
    moq_playback_event_t last[16];
} evt_log_t;

static void drain_evts(moq_playback_t *pb, evt_log_t *log)
{
    memset(log, 0, sizeof(*log));
    moq_playback_event_t e;
    while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK)
        if (e.kind < 16) {
            log->by_kind[e.kind]++;
            log->last[e.kind] = e;
        }
}


static void release_two(moq_playback_t *pb, moq_playback_track_t t, uint64_t g)
{
    CHECK(push_at(pb, t, g, 0, true, 0) == MOQ_OK);
    CHECK(push_at(pb, t, g, 1, false, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 2);
}

/* (0, 2) never comes; (0, 3) is a delta and (1, 0) independent. Without a
 * drain the track waits on (0, 2). Drained, it reports the gap, abandons
 * group 0, resumes at (1, 0) behind a reset, then ends. */
static void test_gap_not_waited(void)
{
    for (int drained = 0; drained <= 1; drained++) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_video(pb);
        release_two(pb, t, 0);
        CHECK(push_at(pb, t, 0, 3, false, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 1, 0, true, 0) == MOQ_OK);
        if (drained) CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        if (!drained) {
            CHECK(cl.decodes == 0);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 0);
            CHECK(moq_playback_next_deadline_us(pb) == WAIT_US);
        } else {
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
            CHECK(el.last[MOQ_PLAYBACK_EVENT_GAP_DETECTED].u.gap_detected.object_id == 2);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 1);
            CHECK(cl.decodes == 1 && cl.group[0] == 1 && cl.object[0] == 0);
            CHECK(cl.resets == 1 && cl.reset_before[0] == 1);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
            CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
            CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
            drain_cmds(pb, &cl);
            drain_evts(pb, &el);
            CHECK(cl.decodes == 0 && el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 0);
        }
        moq_playback_destroy(pb);
    }
}

/* Everything held is present: all of it is released, then the track ends,
 * and an object pushed after the drain is still released first. */
static void test_release_then_end(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(push_at(pb, t, 0, 0, true, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 0, 1, false, 0) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(push_at(pb, t, 0, 2, false, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 3 && cl.object[2] == 2);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    moq_playback_destroy(pb);
}

/* A video track holding only deltas has no entry: draining drops each one
 * KEYFRAME_WAIT and ends the track without a decode. */
static void test_no_entry(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(push_at(pb, t, 0, 1, false, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 0, 2, false, 0) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 0);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == 2);
    CHECK(el.last[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED].u.object_dropped.reason ==
          MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    moq_playback_destroy(pb);
}

/* CONTINUE audio crosses the hole at once, keeping the decoder. */
static void test_continue_audio(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_MEDIA_AUDIO,
                                       MOQ_PLAYBACK_GAP_CONTINUE);
    CHECK(push_at(pb, t, 0, 0, true, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 0, 2, true, 0) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 2 && cl.resets == 0);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_CONTINUED] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    moq_playback_destroy(pb);
}

/* Decoder pressure still pauses a drained track; lifting it lets the track
 * end. */
static void test_pressure_pauses(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(push_at(pb, t, 0, 0, true, 0) == MOQ_OK);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
    fb.track = t;
    fb.u.queue_pressure.depth = 9;
    fb.u.queue_pressure.max_recommended = 4;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 0 && el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 0);
    fb.u.queue_pressure.depth = 0;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 1 && el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    moq_playback_destroy(pb);
}

/* With one event slot, the drained gap of test_gap_not_waited reaches the
 * end over successive ticks, each event polled in between. */
static void test_one_event_slot(void)
{
    moq_playback_t *pb = new_pb(0, 1);
    moq_playback_track_t t = add_video(pb);
    release_two(pb, t, 0);
    CHECK(push_at(pb, t, 0, 3, false, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 1, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    size_t ended = 0, decodes = 0;
    for (int i = 0; i < 8 && !ended; i++) {
        moq_result_t rc = moq_playback_tick(pb, 0);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        decodes += cl.decodes;
        ended += el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED];
    }
    CHECK(ended == 1 && decodes == 1);
    moq_playback_destroy(pb);
}

/* Settled, the track takes the gap step at once like a drained one, but
 * an object pushed after it is placed and released, and it does not end
 * until it is drained. */
static void test_settle_then_drain(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    release_two(pb, t, 0);
    CHECK(push_at(pb, t, 0, 3, false, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 1, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_settle_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_settle_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
    CHECK(cl.decodes == 1 && cl.group[0] == 1 && cl.object[0] == 0);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 0);
    /* Still taking input, and still not ending with nothing buffered. */
    CHECK(push_at(pb, t, 1, 1, false, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 1 && cl.group[0] == 1 && cl.object[0] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 0);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 0);
    /* A missing unit after the settle is not waited for either. */
    CHECK(push_at(pb, t, 1, 3, false, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    drain_cmds(pb, &cl);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    moq_playback_destroy(pb);
}

static void test_arguments(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(moq_playback_drain_track(NULL, t) == MOQ_ERR_INVAL);
    CHECK(moq_playback_settle_track(NULL, t) == MOQ_ERR_INVAL);
    CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_drain_track(pb, t) == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_playback_settle_track(pb, t) == MOQ_ERR_STALE_HANDLE);
    moq_playback_destroy(pb);
}

int main(void)
{
    test_gap_not_waited();
    test_release_then_end();
    test_no_entry();
    test_continue_audio();
    test_pressure_pauses();
    test_one_event_slot();
    test_settle_then_drain();
    test_arguments();
    CHECK(g_state.balance == 0);
    if (failures) {
        fprintf(stderr, "playback drain: %d failure(s)\n", failures);
        return 1;
    }
    printf("playback drain: all passed\n");
    return 0;
}
