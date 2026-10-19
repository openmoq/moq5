/*
 * Stream facts (moq_playback_on_stream_terminal), knowledge and the wait on
 * a missing unit.
 *
 * Invariants pinned here:
 *   - an END_OF_GROUP FIN makes the ids above its last object absent: the
 *     cursor passes them without a wait, an event or a decoder reset; a
 *     plain FIN infers nothing, so the same ids are waited on;
 *   - a reset or a receiver drop leaves ids unresolved: they are waited on
 *     like unknown ones, GAP_DETECTED names the cause, and an object that
 *     arrives late, within the wait, is released normally;
 *   - END_OF_TRACK ends the track once the queued media before it is out;
 *     objects past it are dropped;
 *   - the wait runs from the first evidence that delivery moved past the
 *     missing id (a later object's arrival, or a stream fact), and without
 *     evidence there is no wait to time out;
 *   - after the wait, a DISCONTINUITY track resumes inside the group only
 *     at an independent unit, behind a reset, and otherwise abandons the
 *     group; a CONTINUE track resumes at the next unit with no reset and
 *     reports the hole;
 *   - knowledge is bounded: touching ranges merge, and a full set coalesces
 *     its closest pair into an unresolved range, reporting any absence it
 *     forgets; refused for event room, the fact changes nothing;
 *   - invalid input is refused without effect.
 */
#include <moq/playback.h>
#include <moq/loc.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern size_t moq_playback_test_buffered_count(const moq_playback_t *pb);
extern uint64_t moq_playback_test_anchor_group(const moq_playback_t *pb,
                                                moq_playback_track_t h);
extern uint64_t moq_playback_test_anchor_object(const moq_playback_t *pb,
                                                 moq_playback_track_t h);
extern bool moq_playback_test_gap_waiting(const moq_playback_t *pb,
                                          moq_playback_track_t h);
extern uint32_t moq_playback_test_known_ranges(const moq_playback_t *pb,
                                               moq_playback_track_t h);
extern uint64_t moq_playback_test_absence_forgotten(const moq_playback_t *pb);
extern moq_result_t moq_playback_test_push_event(moq_playback_t *pb,
                                                  const moq_playback_event_t *evt);

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

/* An END_OF_GROUP status object at (g, o). */
static moq_result_t push_end_of_group(moq_playback_t *pb, moq_playback_track_t t,
                                      uint64_t g, uint64_t o)
{
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = g;
    obj.object_id = o;
    obj.status = MOQ_OBJECT_END_OF_GROUP;
    return moq_playback_push_object(pb, &obj, 0);
}

static moq_playback_stream_terminal_t fact(moq_playback_stream_terminal_kind_t k,
                                           uint64_t g)
{
    moq_playback_stream_terminal_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = k;
    r.group_id = g;
    return r;
}

static moq_result_t feed(moq_playback_t *pb, moq_playback_track_t t,
                         moq_playback_stream_terminal_t r, uint64_t now_us)
{
    return moq_playback_on_stream_terminal(pb, t, &r, now_us);
}

static moq_result_t fin(moq_playback_t *pb, moq_playback_track_t t, uint64_t g,
                        uint64_t last, bool end_of_group, uint64_t now_us)
{
    moq_playback_stream_terminal_t r = fact(end_of_group
        ? MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP : MOQ_PLAYBACK_STREAM_FIN, g);
    r.has_last_object = true;
    r.last_object_id = last;
    return feed(pb, t, r, now_us);
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

/* Release (g, 0) and (g, 1), leaving the cursor at (g, 2). */
static void release_two(moq_playback_t *pb, moq_playback_track_t t, uint64_t g)
{
    CHECK(push_at(pb, t, g, 0, true, 0) == MOQ_OK);
    CHECK(push_at(pb, t, g, 1, false, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 2);
}

/* An END_OF_GROUP FIN after object 1: ids above it are absent, and the
 * cursor goes on to group 2 at once, seamlessly. A plain FIN in the same
 * place infers nothing: the cursor waits on (1, 2) until the wait, which
 * the FIN itself started, runs out. */
static void test_fin_end_of_group_vs_fin(void)
{
    for (int eog = 1; eog >= 0; eog--) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_video(pb);
        release_two(pb, t, 1);
        CHECK(push_at(pb, t, 2, 0, false, 0) == MOQ_OK);
        CHECK(fin(pb, t, 1, 1, eog, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 10) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        if (eog) {
            CHECK(cl.decodes == 1 && cl.group[0] == 2 && cl.object[0] == 0);
            CHECK(cl.resets == 0);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 0);
            CHECK(!moq_playback_test_gap_waiting(pb, t));
        } else {
            CHECK(cl.decodes == 0);
            CHECK(moq_playback_test_gap_waiting(pb, t));
            CHECK(moq_playback_next_deadline_us(pb) == WAIT_US);
            CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
            drain_evts(pb, &el);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
            CHECK(el.last[MOQ_PLAYBACK_EVENT_GAP_DETECTED].u.gap_detected.object_id == 2);
            CHECK(el.last[MOQ_PLAYBACK_EVENT_GAP_DETECTED].u.gap_detected.cause ==
                  MOQ_PLAYBACK_GAP_CAUSE_UNKNOWN);
        }
        moq_playback_destroy(pb);
    }
}

/* A reset after object 1 leaves (1, 2) on unresolved: it is waited on, and
 * arriving late, inside the wait, it is released normally, without a reset.
 * Left missing, the wait ends with GAP_DETECTED naming the reset. */
static void test_reset_unresolved(void)
{
    for (int late = 1; late >= 0; late--) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_video(pb);
        release_two(pb, t, 1);
        CHECK(push_at(pb, t, 1, 3, false, 0) == MOQ_OK);
        moq_playback_stream_terminal_t r = fact(MOQ_PLAYBACK_STREAM_RESET, 1);
        r.has_last_object = true;
        r.last_object_id = 1;
        r.error_code = 0x7;
        CHECK(feed(pb, t, r, 0) == MOQ_OK);
        CHECK(moq_playback_test_known_ranges(pb, t) == 1);
        CHECK(moq_playback_tick(pb, 10) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 0);
        if (late) {
            CHECK(push_at(pb, t, 1, 2, false, 20) == MOQ_OK);
            CHECK(moq_playback_tick(pb, 30) == MOQ_OK);
            drain_cmds(pb, &cl);
            drain_evts(pb, &el);
            CHECK(cl.decodes == 2 && cl.object[0] == 2 && cl.object[1] == 3);
            CHECK(cl.resets == 0);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
        } else {
            CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
            drain_evts(pb, &el);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
            CHECK(el.last[MOQ_PLAYBACK_EVENT_GAP_DETECTED].u.gap_detected.cause ==
                  MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET);
        }
        moq_playback_destroy(pb);
    }
}

/* A refused object leaves its one id unresolved: nothing reads it as
 * absent, so the cursor waits on it; a copy of it that arrives in time
 * still plays, seamlessly; otherwise the wait reports LOCAL_REFUSAL. The
 * refusal says nothing of the next id, which plays when present. */
static void test_object_refused(void)
{
    for (int late = 1; late >= 0; late--) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_video(pb);
        release_two(pb, t, 1);
        CHECK(push_at(pb, t, 1, 3, true, 0) == MOQ_OK);
        moq_playback_stream_terminal_t r = fact(MOQ_PLAYBACK_STREAM_OBJECT_REFUSED, 1);
        r.object_id = 2;
        CHECK(feed(pb, t, r, 0) == MOQ_OK);
        CHECK(moq_playback_test_known_ranges(pb, t) == 1);
        CHECK(moq_playback_tick(pb, 10) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 0);
        if (late) {
            CHECK(push_at(pb, t, 1, 2, false, 20) == MOQ_OK);
            CHECK(moq_playback_tick(pb, 30) == MOQ_OK);
            drain_cmds(pb, &cl);
            drain_evts(pb, &el);
            CHECK(cl.decodes == 2 && cl.object[0] == 2 && cl.object[1] == 3);
            CHECK(cl.resets == 0);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
        } else {
            CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
            drain_cmds(pb, &cl);
            drain_evts(pb, &el);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
            CHECK(el.last[MOQ_PLAYBACK_EVENT_GAP_DETECTED].u.gap_detected.object_id == 2);
            CHECK(el.last[MOQ_PLAYBACK_EVENT_GAP_DETECTED].u.gap_detected.cause ==
                  MOQ_PLAYBACK_GAP_CAUSE_LOCAL_REFUSAL);
            CHECK(cl.decodes == 1 && cl.object[0] == 3 && cl.reset_before[0] == 1);
        }
        moq_playback_destroy(pb);
    }
}

/* A refusal settles the push it refuses: in a one-object engine, video
 * (1, 1) is buffered with no entry yet, and independent (2, 0) is refused
 * for room, which would evict (1, 1) to admit it. Once (2, 0)'s refusal
 * is accepted nothing is evicted for it, and (1, 1) stays buffered; the
 * refusal adds one unresolved id and anchors nothing. Without the
 * refusal, or with a refusal of another object, (1, 1) is evicted as
 * before. A later copy of (2, 0) is still admitted. */
static void test_refusal_settles_admission(void)
{
    for (int mode = 0; mode < 4; mode++) {   /* none, its own, another, later copy */
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_objects = 1;
        cfg.gap_timeout_us = WAIT_US;
        cfg.max_backlog_groups = 0;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_track_t t = add_video(pb);
        CHECK(push_at(pb, t, 1, 1, false, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 2, 0, true, 0) == MOQ_ERR_WOULD_BLOCK);
        if (mode == 1 || mode == 3) {
            moq_playback_stream_terminal_t r = fact(MOQ_PLAYBACK_STREAM_OBJECT_REFUSED, 2);
            r.object_id = 0;
            CHECK(feed(pb, t, r, 0) == MOQ_OK);
            CHECK(moq_playback_test_known_ranges(pb, t) == 1);
        } else if (mode == 2) {
            moq_playback_stream_terminal_t r = fact(MOQ_PLAYBACK_STREAM_OBJECT_REFUSED, 3);
            r.object_id = 0;
            CHECK(feed(pb, t, r, 0) == MOQ_OK);
        }
        (void)moq_playback_tick(pb, 0);
        evt_log_t el;
        drain_evts(pb, &el);
        bool kept = mode == 1 || mode == 3;
        CHECK(moq_playback_test_buffered_count(pb) == (kept ? 1u : 0u));
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_OBJECT_DROPPED] == (kept ? 0u : 1u));
        if (mode == 3) {
            CHECK(push_at(pb, t, 2, 0, true, 0) == MOQ_ERR_WOULD_BLOCK);
            (void)moq_playback_tick(pb, 0);
            CHECK(push_at(pb, t, 2, 0, true, 0) == MOQ_OK);
        }
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        moq_playback_destroy(pb);
    }
}

/* Giving up a unit inside its group is reported when it happens: with
 * (1, 2) missing and (1, 3) independent, the wait's end commits
 * GAP_DETECTED and then GAP_SKIPPED_IN_GROUP {1, 2, 3} with the jump, and
 * (1, 3) decodes behind a RESET. With one event slot the announcement
 * goes alone and the step waits for room, nothing changed: polled, the
 * step follows once, with no second announcement. If (1, 2) arrives while
 * the step waits, it plays, with no reset and no skip. */
static void test_gap_skipped_in_group(void)
{
    for (int mode = 0; mode < 3; mode++) {   /* 0 room, 1 one slot, 2 arrives */
        moq_playback_t *pb = new_pb(0, mode ? 1 : 0);
        moq_playback_track_t t = add_video(pb);
        release_two(pb, t, 1);
        CHECK(push_at(pb, t, 1, 3, true, 0) == MOQ_OK);
        CHECK(fin(pb, t, 1, 3, true, 0) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        moq_result_t rc = moq_playback_tick(pb, WAIT_US);
        if (mode == 0) {
            CHECK(rc == MOQ_OK);
            drain_cmds(pb, &cl);
            drain_evts(pb, &el);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP] == 1);
            const moq_playback_event_t *e = &el.last[MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP];
            CHECK(e->track._opaque == t._opaque);
            CHECK(e->u.gap_skipped_in_group.group_id == 1);
            CHECK(e->u.gap_skipped_in_group.from_object_id == 2);
            CHECK(e->u.gap_skipped_in_group.to_object_id == 3);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 0);
            CHECK(cl.decodes == 1 && cl.object[0] == 3 && cl.reset_before[0] == 1);
            moq_playback_destroy(pb);
            continue;
        }
        CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 0 && cl.resets == 0);
        drain_evts(pb, &el);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP] == 0);
        if (mode == 2) CHECK(push_at(pb, t, 1, 2, false, WAIT_US) == MOQ_OK);
        /* One slot: tick and poll until the engine has nothing more. */
        cmd_log_t all = {0};
        evt_log_t evs = {0};
        for (int i = 0; i < 8; i++) {
            rc = moq_playback_tick(pb, WAIT_US + 1);
            drain_cmds(pb, &cl);
            drain_evts(pb, &el);
            for (size_t k = 0; k < cl.decodes && all.decodes < 32; k++) {
                all.object[all.decodes] = cl.object[k];
                all.reset_before[all.decodes] = all.resets + cl.reset_before[k];
                all.decodes++;
            }
            all.resets += cl.resets;
            for (size_t k = 0; k < 16; k++) evs.by_kind[k] += el.by_kind[k];
            if (rc != MOQ_ERR_WOULD_BLOCK) break;
        }
        CHECK(rc == MOQ_OK);
        cl = all;
        el = evs;
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
        if (mode == 1) {
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP] == 1);
            CHECK(cl.decodes == 1 && cl.object[0] == 3 && cl.reset_before[0] == 1);
        } else {
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP] == 0);
            CHECK(cl.decodes == 2 && cl.object[0] == 2 && cl.object[1] == 3);
            CHECK(cl.resets == 0);
        }
        moq_playback_destroy(pb);
    }
}

/* Two groups reset on their own streams, a third group's objects dropped
 * by the receiver: each missing id reports the cause that left it out. */
static void test_causes_per_group(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    release_two(pb, t, 1);
    moq_playback_stream_terminal_t r1 = fact(MOQ_PLAYBACK_STREAM_RESET, 1);
    r1.has_last_object = true;
    r1.last_object_id = 1;
    moq_playback_stream_terminal_t r2 = fact(MOQ_PLAYBACK_STREAM_RESET, 2);
    moq_playback_stream_terminal_t d = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 3);
    d.last_group_id = 3;
    CHECK(feed(pb, t, r1, 0) == MOQ_OK);
    CHECK(feed(pb, t, r2, 0) == MOQ_OK);
    CHECK(feed(pb, t, d, 0) == MOQ_OK);
    /* The two resets touch (group 1's rest, then group 2 from its start)
     * and share a cause: one range. The drop is its own. */
    CHECK(moq_playback_test_known_ranges(pb, t) == 2);
    CHECK(push_at(pb, t, 4, 0, true, 0) == MOQ_OK);

    moq_playback_gap_cause_t want[3] = { MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET,
                                         MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET,
                                         MOQ_PLAYBACK_GAP_CAUSE_RECEIVER_DROP };
    uint64_t want_group[3] = { 1, 2, 3 };
    size_t seen = 0;
    evt_log_t el;
    moq_playback_event_t e;
    CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
    while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK) {
        if (e.kind != MOQ_PLAYBACK_EVENT_GAP_DETECTED) continue;
        if (seen < 3) {
            CHECK(e.u.gap_detected.group_id == want_group[seen]);
            CHECK(e.u.gap_detected.cause == want[seen]);
        }
        seen++;
    }
    CHECK(seen == 3);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.group[0] == 4);
    drain_evts(pb, &el);
    moq_playback_destroy(pb);
}

/* END_OF_TRACK at (2, 0) arrives while group 1's objects are still queued:
 * they are released first, then the track ends. An object past the end is
 * dropped with the track. */
static void test_end_of_track_after_queued_media(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(push_at(pb, t, 1, 0, true, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 1, 1, false, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 3, 0, true, 0) == MOQ_OK);
    moq_playback_stream_terminal_t e = fact(MOQ_PLAYBACK_STREAM_END_OF_TRACK, 2);
    e.object_id = 0;
    CHECK(feed(pb, t, e, 0) == MOQ_OK);
    CHECK(fin(pb, t, 1, 1, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 1) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.decodes == 2 && cl.group[1] == 1 && cl.object[1] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_TRACK_ENDED] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    /* A fact for the ended track changes nothing. */
    uint32_t ranges = moq_playback_test_known_ranges(pb, t);
    moq_playback_stream_terminal_t r = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 3);
    r.last_group_id = 9;
    CHECK(feed(pb, t, r, 2) == MOQ_OK);
    CHECK(moq_playback_test_known_ranges(pb, t) == ranges);
    moq_playback_destroy(pb);
}

/* The wait runs from the evidence, not from the tick that finds the gap:
 * a later object that arrived long before the first tick lets the wait end
 * in that tick. Without any evidence nothing times out; a stream fact
 * starts the wait at its own arrival. */
static void test_wait_from_evidence(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(push_at(pb, t, 1, 0, true, 0) == MOQ_OK);
    CHECK(push_at(pb, t, 1, 2, false, 5) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 5 + WAIT_US) == MOQ_OK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
    moq_playback_destroy(pb);

    pb = new_pb(0, 0);
    t = add_video(pb);
    CHECK(push_at(pb, t, 1, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 100 * WAIT_US) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
    CHECK(!moq_playback_test_gap_waiting(pb, t));
    CHECK(moq_playback_next_deadline_us(pb) == UINT64_MAX);
    CHECK(fin(pb, t, 1, 0, false, 200 * WAIT_US) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 200 * WAIT_US) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == 201 * WAIT_US);
    CHECK(moq_playback_tick(pb, 201 * WAIT_US) == MOQ_OK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
    /* A fact behind the cursor is no evidence. */
    CHECK(moq_playback_test_anchor_group(pb, t) == 2);
    CHECK(fin(pb, t, 1, 4, false, 300 * WAIT_US) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 400 * WAIT_US) == MOQ_OK);
    CHECK(!moq_playback_test_gap_waiting(pb, t));
    moq_playback_destroy(pb);
}

/* After the wait on (1, 2): an independent (1, 3) follows behind a reset,
 * within the group, and group 1's END_OF_GROUP FIN then takes the cursor
 * on to group 2 without another reset. A dependent (1, 3) cannot follow,
 * so the group is abandoned and the track resumes at group 2's
 * independent unit. */
static void test_dependency_rule(void)
{
    for (int independent = 1; independent >= 0; independent--) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_video(pb);
        release_two(pb, t, 1);
        CHECK(push_at(pb, t, 1, 3, independent, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 2, 0, true, 0) == MOQ_OK);
        CHECK(fin(pb, t, 1, 3, true, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
        cmd_log_t cl;
        evt_log_t el;
        drain_cmds(pb, &cl);
        drain_evts(pb, &el);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
        if (independent) {
            CHECK(cl.decodes == 2);
            CHECK(cl.group[0] == 1 && cl.object[0] == 3);
            CHECK(cl.group[1] == 2 && cl.object[1] == 0);
            CHECK(cl.reset_before[0] == 1 && cl.resets == 1);
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 0);
        } else {
            CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 1);
            CHECK(cl.decodes == 1 && cl.group[0] == 2 && cl.object[0] == 0);
            CHECK(cl.reset_before[0] == 1);
        }
        moq_playback_destroy(pb);
    }
}

/* A CONTINUE audio track passes the hole at (1, 2) without a reset and
 * reports it: from the end of the last released unit to (1, 3). */
static void test_continue_policy(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_MEDIA_AUDIO,
                                       MOQ_PLAYBACK_GAP_CONTINUE);
    release_two(pb, t, 1);
    CHECK(push_at(pb, t, 1, 3, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.resets == 0);
    CHECK(cl.decodes == 1 && cl.object[0] == 3);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_CONTINUED] == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 0);
    const moq_playback_event_t *h = &el.last[MOQ_PLAYBACK_EVENT_GAP_CONTINUED];
    /* RAW units carry no duration: the hole starts at the last decode time. */
    CHECK(h->u.gap_continued.hole_start_us == T0 + 1 * 30 * FRAME_US + FRAME_US);
    CHECK(h->u.gap_continued.hole_duration_us == 2 * FRAME_US);
    moq_playback_destroy(pb);

    /* The default is DISCONTINUITY: the same audio track resets. */
    pb = new_pb(0, 0);
    t = add_track(pb, MOQ_PLAYBACK_MEDIA_AUDIO, MOQ_PLAYBACK_GAP_DISCONTINUITY);
    release_two(pb, t, 1);
    CHECK(push_at(pb, t, 1, 3, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_OK);
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(cl.resets == 1);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_CONTINUED] == 0);
    moq_playback_destroy(pb);
}

/* A CONTINUE track keeps its decoder state on every recovery path: a group
 * closed by a buffered END_OF_GROUP bit with an object missing is
 * abandoned, and the next group follows without a reset. */
static void test_continue_closed_group(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_MEDIA_AUDIO,
                                       MOQ_PLAYBACK_GAP_CONTINUE);
    release_two(pb, t, 1);
    CHECK(push_end_of_group(pb, t, 1, 3) == MOQ_OK);
    CHECK(push_at(pb, t, 2, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 1) == MOQ_OK);
    cmd_log_t cl;
    evt_log_t el;
    drain_cmds(pb, &cl);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 1);
    CHECK(cl.decodes == 1 && cl.group[0] == 2);
    CHECK(cl.resets == 0);
    moq_playback_destroy(pb);
}

/* Touching ranges of one kind merge into one; ranges behind the cursor go. */
static void test_ranges_merge_and_prune(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    moq_playback_stream_terminal_t d = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 3);
    d.last_group_id = 4;
    CHECK(feed(pb, t, d, 0) == MOQ_OK);
    d.group_id = 5;
    d.last_group_id = 6;
    CHECK(feed(pb, t, d, 0) == MOQ_OK);
    CHECK(moq_playback_test_known_ranges(pb, t) == 1);
    /* Absence next to them stays its own range. */
    CHECK(fin(pb, t, 7, 2, true, 0) == MOQ_OK);
    CHECK(moq_playback_test_known_ranges(pb, t) == 2);
    /* Group 9 is anchored past both: they no longer matter. */
    CHECK(push_at(pb, t, 9, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(fin(pb, t, 9, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_test_known_ranges(pb, t) == 1);
    moq_playback_destroy(pb);
}

/* With room for two ranges, absences at groups 10 and 30 and a drop of
 * group 31: the closest pair (group 30's absence and the drop) coalesces
 * into one unresolved range, and the forgotten absence is reported. With
 * the event queue full, the fact is refused and changes nothing. */
static void test_exhaustion(void)
{
    for (int blocked = 1; blocked >= 0; blocked--) {
        moq_playback_t *pb = new_pb(2, 2);
        moq_playback_track_t t = add_video(pb);
        CHECK(fin(pb, t, 10, 3, true, 0) == MOQ_OK);
        CHECK(fin(pb, t, 30, 3, true, 0) == MOQ_OK);
        CHECK(moq_playback_test_known_ranges(pb, t) == 2);
        moq_playback_stream_terminal_t d = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 31);
        d.last_group_id = 31;
        if (blocked) {
            moq_playback_event_t f;
            memset(&f, 0, sizeof(f));
            f.struct_size = sizeof(f);
            f.kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED;
            CHECK(moq_playback_test_push_event(pb, &f) == MOQ_OK);
            CHECK(moq_playback_test_push_event(pb, &f) == MOQ_OK);
            CHECK(feed(pb, t, d, 0) == MOQ_ERR_WOULD_BLOCK);
            CHECK(moq_playback_test_known_ranges(pb, t) == 2);
            CHECK(moq_playback_test_absence_forgotten(pb) == 0);
            evt_log_t el;
            drain_evts(pb, &el);
        }
        CHECK(feed(pb, t, d, 0) == MOQ_OK);
        CHECK(moq_playback_test_known_ranges(pb, t) == 2);
        CHECK(moq_playback_test_absence_forgotten(pb) == 1);
        evt_log_t el;
        drain_evts(pb, &el);
        CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN] == 1);
        const moq_playback_event_t *k = &el.last[MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN];
        /* The coalesced range: group 30 from object 4 through group 31. */
        CHECK(k->u.knowledge_forgotten.first_group_id == 30);
        CHECK(k->u.knowledge_forgotten.last_group_id == 31);

        /* Group 10's absence survives: the cursor passes it seamlessly.
         * Group 30's ids are now only unresolved: waited on. */
        CHECK(push_at(pb, t, 10, 0, true, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 10, 1, false, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 10, 2, false, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 10, 3, false, 0) == MOQ_OK);
        CHECK(push_at(pb, t, 11, 0, true, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 1) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl);
        CHECK(cl.decodes == 5 && cl.group[4] == 11);
        CHECK(cl.resets == 0);
        moq_playback_destroy(pb);
    }
}

/* Refused input changes nothing. */
static void test_invalid_input(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    moq_playback_stream_terminal_t r = fact(MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP, 1);
    r.has_last_object = true;
    CHECK(moq_playback_on_stream_terminal(NULL, t, &r, 0) == MOQ_ERR_INVAL);
    CHECK(moq_playback_on_stream_terminal(pb, t, NULL, 0) == MOQ_ERR_INVAL);
    moq_playback_stream_terminal_t s = r;
    s.struct_size = MOQ_PLAYBACK_STREAM_TERMINAL_V0_SIZE - 1;
    CHECK(moq_playback_on_stream_terminal(pb, t, &s, 0) == MOQ_ERR_INVAL);
    s = r;
    s.kind = 0;
    CHECK(moq_playback_on_stream_terminal(pb, t, &s, 0) == MOQ_ERR_INVAL);
    s.kind = MOQ_PLAYBACK_STREAM_OBJECT_REFUSED + 1;
    CHECK(moq_playback_on_stream_terminal(pb, t, &s, 0) == MOQ_ERR_INVAL);
    s = fact(MOQ_PLAYBACK_STREAM_OBJECT_REFUSED, 1);
    s.object_id = UINT64_MAX;
    CHECK(moq_playback_on_stream_terminal(pb, t, &s, 0) == MOQ_ERR_INVAL);
    s = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 5);
    s.last_group_id = 4;
    CHECK(moq_playback_on_stream_terminal(pb, t, &s, 0) == MOQ_ERR_INVAL);
    CHECK(moq_playback_on_stream_terminal(pb, MOQ_PLAYBACK_TRACK_INVALID, &r, 0) ==
          MOQ_ERR_STALE_HANDLE);
    CHECK(moq_playback_test_known_ranges(pb, t) == 0);

    /* gap_policy: zero and unknown values are refused. */
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    CHECK(tc.gap_policy == MOQ_PLAYBACK_GAP_DISCONTINUITY);
    moq_playback_track_t u;
    tc.gap_policy = 0;
    CHECK(moq_playback_add_track(pb, &tc, &u) == MOQ_ERR_INVAL);
    tc.gap_policy = 3;
    CHECK(moq_playback_add_track(pb, &tc, &u) == MOQ_ERR_INVAL);
    /* A caller whose struct ends before gap_policy gets DISCONTINUITY. */
    tc.gap_policy = 0;
    tc.struct_size = (uint32_t)offsetof(moq_playback_track_cfg_t, gap_policy);
    CHECK(moq_playback_add_track(pb, &tc, &u) == MOQ_OK);
    moq_playback_destroy(pb);
}

/* With room for two ranges, absences above object 3 of groups 1 and 3 and
 * a drop of group 4: the closest pair (group 3's absence and the drop)
 * coalesces into one unresolved range. Played up to it, group 1's absence
 * is still passed seamlessly, but group 3's object 4 is now waited on. */
static void test_coalesced_range_is_unresolved(void)
{
    moq_playback_t *pb = new_pb(2, 0);
    moq_playback_track_t t = add_video(pb);
    CHECK(fin(pb, t, 1, 3, true, 0) == MOQ_OK);
    CHECK(fin(pb, t, 3, 3, true, 0) == MOQ_OK);
    moq_playback_stream_terminal_t d = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 4);
    d.last_group_id = 4;
    CHECK(feed(pb, t, d, 0) == MOQ_OK);
    CHECK(moq_playback_test_absence_forgotten(pb) == 1);
    for (uint64_t g = 1; g <= 3; g++)
        for (uint64_t o = 0; o < 4; o++)
            CHECK(push_at(pb, t, g, o, o == 0, 0) == MOQ_OK);
    CHECK(push_end_of_group(pb, t, 2, 4) == MOQ_OK);
    CHECK(push_at(pb, t, 5, 0, true, 0) == MOQ_OK);
    /* Within the wait, several ticks: release is bounded per tick. */
    cmd_log_t cl, all;
    evt_log_t el;
    memset(&all, 0, sizeof(all));
    for (int i = 0; i < 4; i++) {
        CHECK(moq_playback_tick(pb, 1) == MOQ_OK);
        drain_cmds(pb, &cl);
        for (size_t k = 0; k < cl.decodes && all.decodes < 32; k++) {
            all.group[all.decodes] = cl.group[k];
            all.object[all.decodes] = cl.object[k];
            all.decodes++;
        }
        all.resets += cl.resets;
    }
    cl = all;
    drain_evts(pb, &el);
    CHECK(cl.decodes == 12);
    CHECK(cl.decodes && cl.group[cl.decodes - 1] == 3);
    CHECK(cl.resets == 0);
    CHECK(moq_playback_test_gap_waiting(pb, t));
    CHECK(moq_playback_test_anchor_group(pb, t) == 3);
    CHECK(moq_playback_test_anchor_object(pb, t) == 4);
    moq_playback_destroy(pb);
}

/* Each fact keeps its own time. Group 1's FIN at 0 and group 2's at 10000:
 * at 10000 the wait on group 1 has run out, but group 2's evidence is only
 * as old as its FIN, so the cursor waits at group 2 until 11000. */
static void test_evidence_keeps_its_time(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_video(pb);
    release_two(pb, t, 1);
    CHECK(fin(pb, t, 1, 1, false, 0) == MOQ_OK);
    CHECK(fin(pb, t, 2, 0, false, 10000) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 10000) == MOQ_OK);
    CHECK(moq_playback_test_anchor_group(pb, t) == 2);
    CHECK(moq_playback_test_gap_waiting(pb, t));
    CHECK(moq_playback_next_deadline_us(pb) == 10000 + WAIT_US);
    CHECK(moq_playback_tick(pb, 10000 + WAIT_US - 1) == MOQ_OK);
    CHECK(moq_playback_test_anchor_group(pb, t) == 2);
    CHECK(moq_playback_tick(pb, 10000 + WAIT_US) == MOQ_OK);
    CHECK(moq_playback_test_anchor_group(pb, t) == 3);
    moq_playback_destroy(pb);

    /* Genuinely early evidence reaching several groups: a receiver drop of
     * groups 1 to 3 at 0. At 10000 every wait has run out at once. */
    pb = new_pb(0, 0);
    t = add_video(pb);
    release_two(pb, t, 1);
    moq_playback_stream_terminal_t d = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 1);
    d.last_group_id = 3;
    CHECK(feed(pb, t, d, 0) == MOQ_OK);
    for (int i = 0; i < 4; i++) {
        CHECK(moq_playback_tick(pb, 10000) == MOQ_OK);
        evt_log_t el;
        drain_evts(pb, &el);
    }
    CHECK(moq_playback_test_anchor_group(pb, t) == 4);
    moq_playback_destroy(pb);

    /* A later fact that reaches less far adds nothing: a receiver drop of
     * groups 1 to 3 at 0, then group 1's FIN at 500. The wait on group 1
     * still counts from 0. */
    pb = new_pb(0, 0);
    t = add_video(pb);
    release_two(pb, t, 1);
    d = fact(MOQ_PLAYBACK_STREAM_RECEIVER_DROP, 1);
    d.last_group_id = 3;
    CHECK(feed(pb, t, d, 0) == MOQ_OK);
    CHECK(fin(pb, t, 1, 1, false, 500) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 500) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == WAIT_US);
    moq_playback_destroy(pb);

    /* A fact refused for event room records nothing: the running wait and
     * the knowledge stay as they were, and the retry is accepted. (A full
     * range set is facts ahead of the cursor, themselves earlier evidence,
     * so a refusal never withholds the first evidence.) */
    pb = new_pb(1, 1);
    t = add_video(pb);
    release_two(pb, t, 1);
    CHECK(fin(pb, t, 3, 3, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == WAIT_US);
    moq_playback_event_t f;
    memset(&f, 0, sizeof(f));
    f.struct_size = sizeof(f);
    f.kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED;
    CHECK(moq_playback_test_push_event(pb, &f) == MOQ_OK);
    CHECK(fin(pb, t, 1, 1, true, 500) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_test_known_ranges(pb, t) == 1);
    CHECK(moq_playback_test_absence_forgotten(pb) == 0);
    CHECK(moq_playback_next_deadline_us(pb) == WAIT_US);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(fin(pb, t, 1, 1, true, 600) == MOQ_OK);
    CHECK(moq_playback_test_absence_forgotten(pb) == 2);
    CHECK(moq_playback_tick(pb, 600) == MOQ_OK);
    CHECK(moq_playback_next_deadline_us(pb) == WAIT_US);
    moq_playback_destroy(pb);
}

/* The smallest event queue progresses: with one slot, a wait that ends in
 * an abandoned group commits GAP_DETECTED, then the abandon once the slot
 * is drained, without a second report; and two absences coalesce into one
 * range with the single event that reports it. */
static void test_one_event_slot_progresses(void)
{
    moq_playback_t *pb = new_pb(0, 1);
    moq_playback_track_t t = add_video(pb);
    release_two(pb, t, 1);
    CHECK(push_at(pb, t, 2, 0, true, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_ERR_WOULD_BLOCK);
    evt_log_t el;
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 1);
    CHECK(moq_playback_tick(pb, WAIT_US) == MOQ_ERR_WOULD_BLOCK);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_GAP_DETECTED] == 0);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED] == 1);
    for (int i = 0; i < 4; i++) {
        (void)moq_playback_tick(pb, WAIT_US);
        drain_evts(pb, &el);
    }
    cmd_log_t cl;
    drain_cmds(pb, &cl);
    CHECK(cl.decodes == 1 && cl.group[0] == 2);
    moq_playback_destroy(pb);

    pb = new_pb(1, 1);
    t = add_video(pb);
    CHECK(fin(pb, t, 10, 3, true, 0) == MOQ_OK);
    CHECK(fin(pb, t, 30, 3, true, 0) == MOQ_OK);
    CHECK(moq_playback_test_known_ranges(pb, t) == 1);
    drain_evts(pb, &el);
    CHECK(el.by_kind[MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN] == 1);
    CHECK(el.last[MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN].u.knowledge_forgotten.first_group_id == 10);
    CHECK(el.last[MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN].u.knowledge_forgotten.last_group_id == 30);
    CHECK(moq_playback_test_absence_forgotten(pb) == 2);
    moq_playback_destroy(pb);
}

/* The range tables' count is checked before it is formed: a bound whose
 * tables cannot be sized is refused, never wrapped into a small one. */
static void *refuse_huge(size_t sz, void *ctx)
{
    count_state_t *st = (count_state_t *)ctx;
    if (sz > ((size_t)1 << 30)) return NULL;
    return count_alloc(sz, st);
}

static void test_range_table_bounds(void)
{
    count_state_t st = { 0 };
    moq_alloc_t a = { &st, refuse_huge, NULL, count_free };
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_tracks = 2;
    cfg.max_known_ranges = 0x80000000u;
    moq_playback_t *pb = NULL;
    moq_result_t rc = moq_playback_create(&a, &cfg, &pb);
#if SIZE_MAX == UINT32_MAX
    CHECK(rc == MOQ_ERR_INVAL);
#else
    CHECK(rc == MOQ_ERR_NOMEM);
#endif
    CHECK(pb == NULL);
    cfg.max_tracks = 0xFFFF;
    cfg.max_known_ranges = UINT32_MAX;
    rc = moq_playback_create(&a, &cfg, &pb);
    CHECK(rc == MOQ_ERR_INVAL || rc == MOQ_ERR_NOMEM);
    CHECK(pb == NULL);
    CHECK(st.balance == 0);
}

int main(void)
{
    test_fin_end_of_group_vs_fin();
    test_reset_unresolved();
    test_object_refused();
    test_gap_skipped_in_group();
    test_refusal_settles_admission();
    test_causes_per_group();
    test_end_of_track_after_queued_media();
    test_wait_from_evidence();
    test_dependency_rule();
    test_continue_policy();
    test_continue_closed_group();
    test_ranges_merge_and_prune();
    test_exhaustion();
    test_coalesced_range_is_unresolved();
    test_evidence_keeps_its_time();
    test_one_event_slot_progresses();
    test_range_table_bounds();
    test_invalid_input();
    CHECK(g_state.balance == 0);
    if (failures) {
        printf("playback facts: %d failure(s)\n", failures);
        return 1;
    }
    printf("playback facts: all passed\n");
    return 0;
}
