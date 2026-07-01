/*
 * Seeded deterministic playback scenario runner.
 *
 * Feeds randomized object/tick/feedback sequences into moq_playback_t
 * and compares two runs for identical command/event trace hashes.
 * Each seed runs twice; trace hash mismatch = non-determinism bug.
 *
 * Env vars:
 *   MOQ_SCENARIO_SEED   — starting seed (hex, default 1)
 *   MOQ_SCENARIO_RUNS   — number of seeds (default 256)
 *   MOQ_SCENARIO_STEPS  — steps per seed (default 200)
 */

#include <moq/playback.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

/* -- Test helpers (private, guarded by MOQ_PLAYBACK_TESTING) ---------- */

extern uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb);
extern size_t   moq_playback_test_buffered_count(const moq_playback_t *pb);

/* -- RNG ------------------------------------------------------------- */

typedef struct { uint64_t s; } rng_t;
static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}
static uint32_t rng_u32(rng_t *r, uint32_t max) {
    return (uint32_t)(rng_next(r) % max);
}

/* -- Trace hash ------------------------------------------------------ */

#define MIX(h,v) do { (h) ^= (uint64_t)(v); (h) *= 0x100000001B3ULL; } while(0)
#define INIT_HASH 0xCBF29CE484222325ULL

typedef struct {
    uint64_t hash;
    size_t   cmds;
    size_t   evts;
} trace_t;

static void trace_cmd(trace_t *t, const moq_playback_cmd_t *c) {
    uint64_t h = t->hash;
    MIX(h, c->kind);
    switch (c->kind) {
    case MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO:
        MIX(h, c->u.configure_video.width);
        MIX(h, c->u.configure_video.height);
        break;
    case MOQ_PLAYBACK_CMD_DECODE_VIDEO:
        MIX(h, c->u.decode_video.group_id);
        MIX(h, c->u.decode_video.object_id);
        MIX(h, c->u.decode_video.decode_time_us);
        MIX(h, c->u.decode_video.keyframe);
        break;
    case MOQ_PLAYBACK_CMD_DECODE_CMAF:
        MIX(h, c->u.decode_cmaf.group_id);
        MIX(h, c->u.decode_cmaf.object_id);
        MIX(h, c->u.decode_cmaf.decode_time_us);
        MIX(h, c->u.decode_cmaf.keyframe);
        MIX(h, c->u.decode_cmaf.mdat_len);
        break;
    case MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO:
        MIX(h, c->u.configure_audio.samplerate);
        MIX(h, c->u.configure_audio.channel_count);
        break;
    case MOQ_PLAYBACK_CMD_DECODE_AUDIO:
        MIX(h, c->u.decode_audio.group_id);
        MIX(h, c->u.decode_audio.object_id);
        MIX(h, c->u.decode_audio.decode_time_us);
        MIX(h, c->u.decode_audio.mdat_len);
        break;
    case MOQ_PLAYBACK_CMD_RESET:
        MIX(h, c->u.reset.reason);
        break;
    }
    t->hash = h;
    t->cmds++;
}

static void trace_evt(trace_t *t, const moq_playback_event_t *e) {
    uint64_t h = t->hash;
    MIX(h, e->kind);
    switch (e->kind) {
    case MOQ_PLAYBACK_EVENT_GAP_DETECTED:
        MIX(h, e->u.gap_detected.group_id);
        break;
    case MOQ_PLAYBACK_EVENT_SKIP_FORWARD:
        MIX(h, e->u.skip_forward.from_group_id);
        MIX(h, e->u.skip_forward.to_group_id);
        break;
    case MOQ_PLAYBACK_EVENT_OBJECT_DROPPED:
        MIX(h, e->u.object_dropped.reason);
        MIX(h, e->u.object_dropped.group_id);
        MIX(h, e->u.object_dropped.object_id);
        break;
    case MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED:
        MIX(h, e->u.partial_group_abandoned.from_group_id);
        break;
    case MOQ_PLAYBACK_EVENT_BACKLOG_SHED:
        MIX(h, e->u.backlog_shed.dropped_groups);
        break;
    default:
        break;
    }
    t->hash = h;
    t->evts++;
}

/* -- Allocator with balance tracking --------------------------------- */

typedef struct { int64_t balance; } scen_alloc_t;
static void *sa_alloc(size_t sz, void *ctx) {
    void *p = malloc(sz);
    if (p) ((scen_alloc_t *)ctx)->balance++;
    return p;
}
static void sa_free(void *p, size_t sz, void *ctx) {
    (void)sz;
    if (p) ((scen_alloc_t *)ctx)->balance--;
    free(p);
}
static moq_alloc_t sa_allocator(scen_alloc_t *sa) {
    return (moq_alloc_t){ sa, sa_alloc, NULL, sa_free };
}

/* -- LOC property helper --------------------------------------------- */

static moq_rcbuf_t *make_props(const moq_alloc_t *alloc,
                                uint64_t ts, bool kf)
{
    moq_loc_headers_t lh;
    moq_loc_headers_init(&lh);
    lh.has_timestamp = true;
    lh.timestamp = ts;
    if (kf) {
        lh.has_video_frame_marking = true;
        lh.video_frame_marking.independent = true;
        lh.video_frame_marking.start_of_frame = true;
        lh.video_frame_marking.end_of_frame = true;
    }
    moq_rcbuf_t *out = NULL;
    if (moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &out) != MOQ_OK)
        return NULL;
    return out;
}

/* -- CMAF fragment helper -------------------------------------------- */

static void wr32(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)(v>>24); p[1]=(uint8_t)(v>>16);
    p[2]=(uint8_t)(v>>8);  p[3]=(uint8_t)v;
}

static size_t build_cmaf_frag(uint8_t *buf, uint64_t bdt,
                               uint32_t dur, uint32_t flags,
                               const uint8_t *mdat, size_t mdat_len)
{
    size_t p = 0;
    uint32_t tf = 0x100|0x200|0x400;
    size_t trun_sz = 8+8+12, tfdt_sz = 8+4+8, tfhd_sz = 8+8;
    size_t traf_sz = 8+tfhd_sz+tfdt_sz+trun_sz;
    size_t moof_sz = 8+traf_sz;
    wr32(buf+p, (uint32_t)moof_sz); memcpy(buf+p+4,"moof",4); p+=8;
    wr32(buf+p, (uint32_t)traf_sz); memcpy(buf+p+4,"traf",4); p+=8;
    wr32(buf+p, (uint32_t)tfhd_sz); memcpy(buf+p+4,"tfhd",4); p+=8;
    wr32(buf+p,0); p+=4; wr32(buf+p,1); p+=4;
    wr32(buf+p, (uint32_t)tfdt_sz); memcpy(buf+p+4,"tfdt",4); p+=8;
    wr32(buf+p, 0x01000000); p+=4;
    wr32(buf+p, (uint32_t)(bdt>>32)); p+=4;
    wr32(buf+p, (uint32_t)bdt); p+=4;
    wr32(buf+p, (uint32_t)trun_sz); memcpy(buf+p+4,"trun",4); p+=8;
    wr32(buf+p, tf); p+=4; wr32(buf+p, 1); p+=4;
    wr32(buf+p, dur); p+=4;
    wr32(buf+p, (uint32_t)mdat_len); p+=4;
    wr32(buf+p, flags); p+=4;
    wr32(buf+p, (uint32_t)(8+mdat_len)); memcpy(buf+p+4,"mdat",4); p+=8;
    memcpy(buf+p, mdat, mdat_len); p+=mdat_len;
    return p;
}

/* -- Per-run state --------------------------------------------------- */

typedef struct {
    moq_playback_t       *pb;
    moq_playback_track_t  track;
    moq_alloc_t           alloc;
    uint64_t              next_group;
    uint64_t              next_object;
    uint64_t              now_us;
    uint64_t              ts_counter;
    trace_t               trace;
    uint64_t              seed;
    int                   step;
    int                  *failures;
    bool                  ended;
} run_state_t;

#define SCEN_FAIL(rs, fmt, ...) do { \
    fprintf(stderr, "FAIL [seed=0x%" PRIx64 " step=%d]: " fmt "\n", \
            (rs)->seed, (rs)->step, ##__VA_ARGS__); \
    (*(rs)->failures)++; \
} while(0)

static void drain_outputs(run_state_t *rs) {
    moq_playback_cmd_t cmd;
    while (moq_playback_poll_command(rs->pb, &cmd) == MOQ_OK) {
        trace_cmd(&rs->trace, &cmd);
        moq_playback_cmd_cleanup(&cmd);
    }
    moq_playback_event_t evt;
    while (moq_playback_poll_event(rs->pb, &evt) == MOQ_OK)
        trace_evt(&rs->trace, &evt);
}

/* -- Operations ------------------------------------------------------ */

static void op_push_normal(run_state_t *rs, rng_t *rng) {
    bool kf = (rs->next_object == 0) || rng_u32(rng, 4) == 0;
    rs->ts_counter += 33000 + rng_u32(rng, 1000);

    moq_rcbuf_t *props = make_props(&rs->alloc, rs->ts_counter, kf);
    if (!props) { SCEN_FAIL(rs, "make_props failed"); return; }

    uint8_t data[4] = { 0xDE, 0xAD, 0xBE, 0xEF };
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&rs->alloc, data, 4, &payload) != MOQ_OK) {
        moq_rcbuf_decref(props);
        SCEN_FAIL(rs, "rcbuf_create failed");
        return;
    }

    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = rs->track;
    obj.group_id = rs->next_group;
    obj.object_id = rs->next_object;
    obj.payload = payload;
    obj.properties = props;
    moq_result_t rc = moq_playback_push_object(rs->pb, &obj, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_WRONG_STATE)
        SCEN_FAIL(rs, "push_normal unexpected rc=%d", rc);

    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    if (rc == MOQ_OK) rs->next_object++;
}

static void op_push_out_of_order(run_state_t *rs, rng_t *rng) {
    uint64_t skip = 1 + rng_u32(rng, 3);
    rs->next_object += skip;
    op_push_normal(rs, rng);
}

static void op_push_end_of_group(run_state_t *rs) {
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = rs->track;
    obj.group_id = rs->next_group;
    obj.object_id = rs->next_object;
    obj.status = MOQ_OBJECT_END_OF_GROUP;
    moq_result_t rc = moq_playback_push_object(rs->pb, &obj, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_WRONG_STATE)
        SCEN_FAIL(rs, "push_eog unexpected rc=%d", rc);
    if (rc == MOQ_OK) {
        rs->next_group++;
        rs->next_object = 0;
    }
}

static void op_push_end_of_track(run_state_t *rs) {
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = rs->track;
    obj.group_id = rs->next_group;
    obj.object_id = rs->next_object;
    obj.status = MOQ_OBJECT_END_OF_TRACK;
    moq_result_t rc = moq_playback_push_object(rs->pb, &obj, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_WRONG_STATE)
        SCEN_FAIL(rs, "push_eot unexpected rc=%d", rc);
    if (rc == MOQ_OK) rs->ended = true;
}

static void op_push_malformed(run_state_t *rs) {
    uint8_t bad[] = { 0xFF, 0xFF };
    moq_rcbuf_t *props = NULL;
    if (moq_rcbuf_create(&rs->alloc, bad, 2, &props) != MOQ_OK) {
        SCEN_FAIL(rs, "malformed props alloc failed");
        return;
    }

    uint8_t data[] = { 0x01 };
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&rs->alloc, data, 1, &payload) != MOQ_OK) {
        moq_rcbuf_decref(props);
        SCEN_FAIL(rs, "malformed payload alloc failed");
        return;
    }

    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = rs->track;
    obj.group_id = rs->next_group;
    obj.object_id = rs->next_object;
    obj.payload = payload;
    obj.properties = props;
    moq_result_t rc = moq_playback_push_object(rs->pb, &obj, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_WRONG_STATE)
        SCEN_FAIL(rs, "push_malformed unexpected rc=%d", rc);

    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    if (rc == MOQ_OK) rs->next_object++;
}

static void op_push_no_loc(run_state_t *rs) {
    uint8_t data[] = { 0x01 };
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&rs->alloc, data, 1, &payload) != MOQ_OK) {
        SCEN_FAIL(rs, "no_loc payload alloc failed");
        return;
    }

    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = rs->track;
    obj.group_id = rs->next_group;
    obj.object_id = rs->next_object;
    obj.payload = payload;
    moq_result_t rc = moq_playback_push_object(rs->pb, &obj, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK &&
        rc != MOQ_ERR_WRONG_STATE)
        SCEN_FAIL(rs, "push_no_loc unexpected rc=%d", rc);

    moq_rcbuf_decref(payload);
    if (rc == MOQ_OK) rs->next_object++;
}

static void op_tick(run_state_t *rs) {
    moq_result_t rc = moq_playback_tick(rs->pb, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK)
        SCEN_FAIL(rs, "tick unexpected rc=%d", rc);
}

static void op_advance_time(run_state_t *rs, rng_t *rng) {
    rs->now_us += 10000 + rng_u32(rng, 200000);
}

static void op_pressure_high(run_state_t *rs) {
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.track = rs->track;
    fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
    fb.u.queue_pressure.depth = 20;
    fb.u.queue_pressure.max_recommended = 5;
    moq_result_t rc = moq_playback_handle_feedback(rs->pb, &fb, rs->now_us);
    if (rc != MOQ_OK)
        SCEN_FAIL(rs, "pressure_high unexpected rc=%d", rc);
}

static void op_pressure_low(run_state_t *rs) {
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.track = rs->track;
    fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
    fb.u.queue_pressure.depth = 2;
    fb.u.queue_pressure.max_recommended = 5;
    moq_result_t rc = moq_playback_handle_feedback(rs->pb, &fb, rs->now_us);
    if (rc != MOQ_OK)
        SCEN_FAIL(rs, "pressure_low unexpected rc=%d", rc);
}

static void op_decode_error(run_state_t *rs) {
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.track = rs->track;
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    moq_result_t rc = moq_playback_handle_feedback(rs->pb, &fb, rs->now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK)
        SCEN_FAIL(rs, "decode_error unexpected rc=%d", rc);
}

/* -- Single run ------------------------------------------------------ */

static trace_t run_scenario(uint64_t seed, int steps,
                             scen_alloc_t *sa, int *failures)
{
    moq_alloc_t alloc = sa_allocator(sa);

    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_tracks = 1;
    cfg.max_buffered_objects = 32;
    cfg.max_buffered_bytes = 4096;
    cfg.max_commands = 32;
    cfg.max_events = 16;
    cfg.max_backlog_groups = 3;
    cfg.gap_timeout_us = 200000;
    cfg.max_release_per_tick = 4;

    moq_playback_t *pb = NULL;
    if (moq_playback_create(&alloc, &cfg, &pb) != MOQ_OK) {
        fprintf(stderr, "FAIL: seed 0x%" PRIx64 " create failed\n", seed);
        (*failures)++;
        return (trace_t){ INIT_HASH, 0, 0 };
    }

    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    moq_playback_track_t track = { 0 };
    if (moq_playback_add_track(pb, &tc, &track) != MOQ_OK) {
        fprintf(stderr, "FAIL: seed 0x%" PRIx64 " add_track failed\n", seed);
        (*failures)++;
        moq_playback_destroy(pb);
        return (trace_t){ INIT_HASH, 0, 0 };
    }

    run_state_t rs;
    memset(&rs, 0, sizeof(rs));
    rs.pb = pb;
    rs.track = track;
    rs.alloc = alloc;
    rs.now_us = 1000000;
    rs.ts_counter = 1000000;
    rs.trace.hash = INIT_HASH;
    rs.seed = seed;
    rs.failures = failures;

    rng_t rng = { seed };

    for (int step = 0; step < steps && !rs.ended; step++) {
        rs.step = step;
        uint32_t op = rng_u32(&rng, 100);

        if (op < 30)       op_push_normal(&rs, &rng);
        else if (op < 38)  op_push_out_of_order(&rs, &rng);
        else if (op < 43)  op_push_end_of_group(&rs);
        else if (op < 44)  op_push_end_of_track(&rs);
        else if (op < 48)  op_push_malformed(&rs);
        else if (op < 52)  op_push_no_loc(&rs);
        else if (op < 70)  op_tick(&rs);
        else if (op < 78)  op_advance_time(&rs, &rng);
        else if (op < 85)  drain_outputs(&rs);
        else if (op < 90)  op_pressure_high(&rs);
        else if (op < 95)  op_pressure_low(&rs);
        else               op_decode_error(&rs);
    }

    op_tick(&rs);
    drain_outputs(&rs);

    trace_t result = rs.trace;
    moq_playback_destroy(pb);
    return result;
}

/* -- Fixed sub-scenarios --------------------------------------------- */

#define FCHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while(0)

static int fixed_scenarios(void)
{
    int failures = 0;

    /* 1. Live join: first object at group 500/object 3. */
    {
        scen_alloc_t sa = { 0 };
        moq_alloc_t a = sa_allocator(&sa);
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        FCHECK(moq_playback_create(&a, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = { 0 };
        FCHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = make_props(&a, 9000, true);
        FCHECK(props != NULL);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        FCHECK(moq_rcbuf_create(&a, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 500; obj.object_id = 3;
        obj.payload = p; obj.properties = props;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_rcbuf_decref(p); moq_rcbuf_decref(props);

        FCHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        bool got_decode = false;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            if (cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) {
                FCHECK(cmd.u.decode_video.group_id == 500);
                FCHECK(cmd.u.decode_video.object_id == 3);
                got_decode = true;
            }
            moq_playback_cmd_cleanup(&cmd);
        }
        FCHECK(got_decode);

        moq_playback_event_t evt;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK)
            FCHECK(evt.kind != MOQ_PLAYBACK_EVENT_GAP_DETECTED);

        moq_playback_destroy(pb);
        FCHECK(sa.balance == 0);
    }

    /* 2. Out-of-order repair before timeout. */
    {
        scen_alloc_t sa = { 0 };
        moq_alloc_t a = sa_allocator(&sa);
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 500000;
        moq_playback_t *pb = NULL;
        FCHECK(moq_playback_create(&a, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = { 0 };
        FCHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *pr0 = make_props(&a, 1000, true);
        moq_rcbuf_t *pr1 = make_props(&a, 2000, false);
        moq_rcbuf_t *pr2 = make_props(&a, 3000, false);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0=NULL, *p1=NULL, *p2=NULL;
        FCHECK(moq_rcbuf_create(&a, data, 1, &p0) == MOQ_OK);
        FCHECK(moq_rcbuf_create(&a, data, 1, &p1) == MOQ_OK);
        FCHECK(moq_rcbuf_create(&a, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 0; obj.payload = p0; obj.properties = pr0;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 2; obj.payload = p2; obj.properties = pr2;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        FCHECK(moq_playback_tick(pb, 1000000) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 1; obj.payload = p1; obj.properties = pr1;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        FCHECK(moq_playback_tick(pb, 1100000) == MOQ_OK);

        int decode_count = 0;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            if (cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) decode_count++;
            moq_playback_cmd_cleanup(&cmd);
        }
        FCHECK(decode_count == 2);

        moq_playback_event_t evt;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK)
            FCHECK(evt.kind != MOQ_PLAYBACK_EVENT_GAP_DETECTED);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(pr0); moq_rcbuf_decref(pr1); moq_rcbuf_decref(pr2);
        moq_playback_destroy(pb);
        FCHECK(sa.balance == 0);
    }

    /* 3. Gap timeout recovery. */
    {
        scen_alloc_t sa = { 0 };
        moq_alloc_t a = sa_allocator(&sa);
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 100000;
        moq_playback_t *pb = NULL;
        FCHECK(moq_playback_create(&a, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = { 0 };
        FCHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *pr0 = make_props(&a, 1000, true);
        moq_rcbuf_t *pr2 = make_props(&a, 3000, false);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0=NULL, *p2=NULL;
        FCHECK(moq_rcbuf_create(&a, data, 1, &p0) == MOQ_OK);
        FCHECK(moq_rcbuf_create(&a, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 0; obj.payload = p0; obj.properties = pr0;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 2; obj.payload = p2; obj.properties = pr2;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        FCHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        FCHECK(moq_playback_tick(pb, 200000) == MOQ_OK);
        bool got_gap = false, got_partial = false;
        moq_playback_event_t evt;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK) {
            if (evt.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED) got_gap = true;
            if (evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED)
                got_partial = true;
        }
        FCHECK(got_gap);
        FCHECK(got_partial);

        moq_rcbuf_t *pr_kf = make_props(&a, 5000, true);
        moq_rcbuf_t *pk = NULL;
        FCHECK(moq_rcbuf_create(&a, data, 1, &pk) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = pk; obj.properties = pr_kf;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        FCHECK(moq_playback_tick(pb, 300000) == MOQ_OK);

        bool got_reset = false, got_decode = false;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            if (cmd.kind == MOQ_PLAYBACK_CMD_RESET) got_reset = true;
            if (cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) got_decode = true;
            moq_playback_cmd_cleanup(&cmd);
        }
        FCHECK(got_reset);
        FCHECK(got_decode);
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK) (void)0;

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p2); moq_rcbuf_decref(pk);
        moq_rcbuf_decref(pr0); moq_rcbuf_decref(pr2); moq_rcbuf_decref(pr_kf);
        moq_playback_destroy(pb);
        FCHECK(sa.balance == 0);
    }

    /* 4. CMAF one-sample decode through playback. */
    {
        scen_alloc_t sa = { 0 };
        moq_alloc_t a = sa_allocator(&sa);
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        FCHECK(moq_playback_create(&a, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = { 0 };
        FCHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0xCA, 0xFE };
        uint8_t buf[512];
        size_t len = build_cmaf_frag(buf, 180000, 3000, 0x02000000,
                                      mdat, 2);
        moq_rcbuf_t *payload = NULL;
        FCHECK(moq_rcbuf_create(&a, buf, len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_rcbuf_decref(payload);

        FCHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        bool got_configure = false, got_cmaf = false;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            if (cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO)
                got_configure = true;
            if (cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF) {
                FCHECK(cmd.u.decode_cmaf.decode_time_us == 2000000);
                FCHECK(cmd.u.decode_cmaf.keyframe == true);
                FCHECK(cmd.u.decode_cmaf.mdat_len == 2);
                got_cmaf = true;
            }
            moq_playback_cmd_cleanup(&cmd);
        }
        FCHECK(got_configure);
        FCHECK(got_cmaf);

        moq_playback_event_t evt;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK) (void)0;

        moq_playback_destroy(pb);
        FCHECK(sa.balance == 0);
    }

    /* 5. Mixed audio+video: audio decodes while video keyframe-waits. */
    {
        scen_alloc_t sa = { 0 };
        moq_alloc_t a = sa_allocator(&sa);
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 2;
        moq_playback_t *pb = NULL;
        FCHECK(moq_playback_create(&a, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t vtc;
        moq_playback_track_cfg_init(&vtc);
        vtc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        vtc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t vt = { 0 };
        FCHECK(moq_playback_add_track(pb, &vtc, &vt) == MOQ_OK);

        moq_playback_track_cfg_t atc;
        moq_playback_track_cfg_init(&atc);
        atc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        atc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t at = { 0 };
        FCHECK(moq_playback_add_track(pb, &atc, &at) == MOQ_OK);

        /* Push audio (non-keyframe): should decode immediately. */
        moq_rcbuf_t *aprops = make_props(&a, 1000, false);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *ap = NULL;
        FCHECK(moq_rcbuf_create(&a, data, 1, &ap) == MOQ_OK);
        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = at; obj.group_id = 1; obj.object_id = 0;
        obj.payload = ap; obj.properties = aprops;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_rcbuf_decref(ap); moq_rcbuf_decref(aprops);

        /* Push video non-keyframe: dropped by keyframe wait. */
        moq_rcbuf_t *vprops = make_props(&a, 1000, false);
        moq_rcbuf_t *vp = NULL;
        FCHECK(moq_rcbuf_create(&a, data, 1, &vp) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = vt; obj.group_id = 1; obj.object_id = 0;
        obj.payload = vp; obj.properties = vprops;
        FCHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_rcbuf_decref(vp); moq_rcbuf_decref(vprops);

        FCHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        bool got_audio_cfg = false, got_audio_dec = false;
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            if (cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO) got_audio_cfg = true;
            if (cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO) got_audio_dec = true;
            FCHECK(cmd.kind != MOQ_PLAYBACK_CMD_DECODE_VIDEO);
            moq_playback_cmd_cleanup(&cmd);
        }
        FCHECK(got_audio_cfg);
        FCHECK(got_audio_dec);

        moq_playback_event_t evt;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK) (void)0;

        moq_playback_destroy(pb);
        FCHECK(sa.balance == 0);
    }

    return failures;
}

/* -- Main ------------------------------------------------------------ */

#define REPLAY(seed, steps) fprintf(stderr, \
    "  Replay: MOQ_SCENARIO_SEED=0x%" PRIx64 " " \
    "MOQ_SCENARIO_RUNS=1 MOQ_SCENARIO_STEPS=%d " \
    "./build/media/playback/test_playback_scenario\n", \
    (uint64_t)(seed), (int)(steps))

int main(void)
{
    int failures = 0;

    failures += fixed_scenarios();

    const char *seed_env  = getenv("MOQ_SCENARIO_SEED");
    const char *runs_env  = getenv("MOQ_SCENARIO_RUNS");
    const char *steps_env = getenv("MOQ_SCENARIO_STEPS");

    uint64_t start_seed = seed_env  ? strtoull(seed_env, NULL, 0) : 1;
    int      num_runs   = runs_env  ? atoi(runs_env) : 256;
    int      num_steps  = steps_env ? atoi(steps_env) : 200;

    for (int r = 0; r < num_runs; r++) {
        uint64_t seed = start_seed + (uint64_t)r;

        scen_alloc_t sa1 = { 0 }, sa2 = { 0 };
        int f1 = 0, f2 = 0;
        trace_t t1 = run_scenario(seed, num_steps, &sa1, &f1);
        trace_t t2 = run_scenario(seed, num_steps, &sa2, &f2);

        if (t1.hash != t2.hash) {
            fprintf(stderr,
                "FAIL: seed 0x%" PRIx64 " non-deterministic "
                "(hash1=0x%" PRIx64 " hash2=0x%" PRIx64 " "
                "cmds1=%zu cmds2=%zu evts1=%zu evts2=%zu)\n",
                seed, t1.hash, t2.hash,
                t1.cmds, t2.cmds, t1.evts, t2.evts);
            REPLAY(seed, num_steps);
            failures++;
        }

        if (sa1.balance != 0 || sa2.balance != 0) {
            fprintf(stderr,
                "FAIL: seed 0x%" PRIx64 " alloc leak "
                "(balance1=%" PRId64 " balance2=%" PRId64 ")\n",
                seed, sa1.balance, sa2.balance);
            REPLAY(seed, num_steps);
            failures++;
        }

        failures += f1 + f2;
    }

    printf("%s: %d failures (%d seeds x %d steps)\n",
           failures ? "FAIL" : "PASS", failures, num_runs, num_steps);
    return failures ? 1 : 0;
}
