/*
 * Admission observation rows: moq_playback_get_admission's output contract
 * and validation parity with push_parsed, observation purity, and the
 * capacity classification against the engine's current owners -- relief
 * victims, paused tracks' media, queued commands and command holds versus
 * object slots, and configuration reuse a release keeps or destroys.
 */
#include <moq/playback.h>
#include <moq/media_object.h>
#include <moq/loc.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb);
extern size_t moq_playback_test_buffered_count(const moq_playback_t *pb);
extern void moq_playback_test_set_copy_serial(moq_playback_t *pb, uint64_t serial);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Counting allocator ---------------------------------------------- */

static int64_t g_balance;
static uint64_t g_allocs;
static void *c_alloc(size_t n, void *c)
{
    (void)c;
    g_allocs++;
    void *p = n ? malloc(n) : NULL;
    if (p) g_balance++;
    return p;
}
static void c_free(void *p, size_t n, void *c)
{
    (void)n; (void)c;
    if (p) g_balance--;
    free(p);
}
static moq_alloc_t g_alloc = { NULL, c_alloc, NULL, c_free };

/* -- Engine helpers --------------------------------------------------- */

#define T0 UINT64_C(1700000000000000)

typedef struct {
    uint32_t max_objects, max_events, max_held;
    uint64_t max_bytes;
} pb_opts_t;

static moq_playback_t *new_pb(pb_opts_t o)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_commands = 8;
    if (o.max_objects) cfg.max_buffered_objects = o.max_objects;
    if (o.max_events) cfg.max_events = o.max_events;
    if (o.max_bytes) cfg.max_buffered_bytes = o.max_bytes;
    cfg.max_held_commands = o.max_held;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

static moq_playback_track_t add_track(moq_playback_t *pb)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = (uint32_t)MOQ_VERSION_DRAFT_22;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    return t;
}

static void set_paused(moq_playback_t *pb, moq_playback_track_t t, bool paused)
{
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
    fb.track = t;
    fb.u.queue_pressure.depth = paused ? 2 : 0;
    fb.u.queue_pressure.max_recommended = 1;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
}

static size_t drain_cmds(moq_playback_t *pb)
{
    size_t n = 0;
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        moq_playback_cmd_cleanup(&c);
        n++;
    }
    return n;
}

/* Poll every event: how many were of `kind` (any, for 0), drops only for
 * `reason`. */
static size_t count_events(moq_playback_t *pb, uint32_t kind, uint32_t reason)
{
    size_t n = 0;
    moq_playback_event_t e;
    while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK)
        if (kind == 0 ||
            (e.kind == kind &&
            (kind != MOQ_PLAYBACK_EVENT_OBJECT_DROPPED ||
              e.u.object_dropped.reason == reason)))
            n++;
    return n;
}

/* -- Objects ---------------------------------------------------------- */

/* A draft-22 LOC-04 RAW video object as push_parsed takes it: a payload of
 * `plen` bytes, a UNIX timestamp (absent when !has_ts), a frame marking
 * and, when cfg is set, a video configuration. The record borrows the
 * properties, which live as long as the object. */
typedef struct {
    moq_rcbuf_t                *payload;
    moq_rcbuf_t                *props;
    moq_media_parsed_object_t   parsed;
    moq_playback_parsed_input_t in;
} obj_t;

static void obj_make_ts(obj_t *x, moq_playback_track_t t, uint64_t g, uint64_t o,
                        size_t plen, bool kf, const uint8_t *cfg, size_t cfg_len,
                        bool has_ts)
{
    static const uint8_t frame[64] = { 0, 0, 1, 0x65 };
    memset(x, 0, sizeof(*x));
    CHECK(plen <= sizeof(frame));
    CHECK(moq_rcbuf_create(&g_alloc, frame, plen, &x->payload) == MOQ_OK);

    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = MOQ_LOC_METADATA_HAS_MARKING;
    m.marking_len = 1;
    m.marking[0] = (uint8_t)(0xC0u | (kf ? 0x20u : 0u));
    if (has_ts) {
        m.present |= MOQ_LOC_METADATA_HAS_TIMESTAMP;
        m.timestamp = T0 + g * 1000000 + o * 1000;
    }
    if (cfg) {
        m.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        m.video_config = (moq_bytes_t){ cfg, cfg_len };
    }
    CHECK(moq_loc_encode_metadata(&g_alloc, MOQ_VERSION_DRAFT_22,
                                  MOQ_LOC_PROFILE_04, &m, &x->props) == MOQ_OK);

    moq_media_track_info_t ti;
    moq_media_track_info_init_sized(&ti, sizeof(ti));
    ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
    ti.packaging = MOQ_MEDIA_PACKAGING_RAW;
    ti.transport_version = MOQ_VERSION_DRAFT_22;
    moq_media_object_input_t mi;
    moq_media_object_input_init(&mi);
    mi.group_id = g;
    mi.object_id = o;
    mi.payload = x->payload;
    mi.properties = x->props;
    moq_media_object_parse_cfg_t pc = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    pc.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    moq_media_drop_reason_t why = 0;
    CHECK(moq_media_object_parse_sized(&ti, &mi, &pc, NULL, 0, &x->parsed,
                                       sizeof(x->parsed), &why) == MOQ_OK);

    moq_playback_parsed_input_init(&x->in);
    x->in.track = t;
    x->in.group_id = g;
    x->in.object_id = o;
    x->in.parsed = &x->parsed;
    x->in.payload = x->payload;
}

static void obj_make(obj_t *x, moq_playback_track_t t, uint64_t g, uint64_t o,
                     size_t plen, bool kf, const uint8_t *cfg, size_t cfg_len)
{
    obj_make_ts(x, t, g, o, plen, kf, cfg, cfg_len, true);
}

static void obj_free(obj_t *x)
{
    moq_rcbuf_decref(x->payload);
    moq_rcbuf_decref(x->props);
    memset(x, 0, sizeof(*x));
}

/* Push one object and free it: what push_parsed returned. */
static moq_result_t push(moq_playback_t *pb, moq_playback_track_t t,
                         uint64_t g, uint64_t o, size_t plen, bool kf,
                         const uint8_t *cfg, size_t cfg_len)
{
    obj_t x;
    obj_make(&x, t, g, o, plen, kf, cfg, cfg_len);
    moq_result_t rc = moq_playback_push_parsed(pb, &x.in, 0);
    obj_free(&x);
    return rc;
}

/* The observation of `x`, which must succeed. */
static moq_playback_admission_info_t adm(moq_playback_t *pb, const obj_t *x)
{
    moq_playback_admission_info_t info;
    memset(&info, 0, sizeof(info));
    CHECK(moq_playback_get_admission(pb, &x->in, &info, sizeof(info)) == MOQ_OK);
    CHECK(info.struct_size == sizeof(info));
    return info;
}

static const uint8_t CFG_A[5] = { 0xA1, 1, 2, 3, 4 };
static const uint8_t CFG_B[3] = { 0xB2, 5, 6 };
static const uint8_t CFG_C[6] = { 0xC6, 1, 2, 3, 4, 5 };

/* A track whose first object, keyframe (1,0) with `cfg`, has been released
 * and its commands polled: anchored, expecting (1,1), with the
 * configuration held by its decoder slot. */
static moq_playback_track_t started_track(moq_playback_t *pb, const uint8_t *cfg,
                                          size_t cfg_len)
{
    moq_playback_track_t t = add_track(pb);
    CHECK(push(pb, t, 1, 0, 4, true, cfg, cfg_len) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(drain_cmds(pb) == (cfg ? 2u : 1u));
    return t;
}

/* -- Output contract -------------------------------------------------- */

static bool all_bytes(const uint8_t *p, size_t n, uint8_t v)
{
    for (size_t i = 0; i < n; i++)
        if (p[i] != v) return false;
    return true;
}

/* NULL arguments and an output below the v0 floor are refused with
 * nothing written. The floor writes exactly struct_size and kind; a full
 * output adds the charge; an oversized one is written only through the
 * current size, with struct_size the bytes written. The engine is left as
 * it was, and a push then charges what was reported. */
static void test_admission_output_contract(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ 0 });
    moq_playback_track_t t = add_track(pb);
    obj_t x;
    obj_make(&x, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A));

    enum { FULL = sizeof(moq_playback_admission_info_t) };
    union {
        moq_playback_admission_info_t info;
        uint8_t                       b[FULL + 16];
    } out;
    memset(&out, 0xAB, sizeof(out));
    CHECK(moq_playback_get_admission(NULL, &x.in, &out.info, FULL) == MOQ_ERR_INVAL);
    CHECK(moq_playback_get_admission(pb, NULL, &out.info, FULL) == MOQ_ERR_INVAL);
    CHECK(moq_playback_get_admission(pb, &x.in, NULL, FULL) == MOQ_ERR_INVAL);
    CHECK(moq_playback_get_admission(pb, &x.in, &out.info, 0) == MOQ_ERR_INVAL);
    CHECK(moq_playback_get_admission(pb, &x.in, &out.info,
                                     MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE - 1) ==
          MOQ_ERR_INVAL);
    CHECK(all_bytes(out.b, sizeof(out.b), 0xAB));

    CHECK(MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE ==
          offsetof(moq_playback_admission_info_t, kind) +
          sizeof(moq_playback_admission_t));
    CHECK(moq_playback_get_admission(pb, &x.in, &out.info,
                                     MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE) == MOQ_OK);
    CHECK(out.info.struct_size == MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE);
    CHECK(out.info.kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(all_bytes(out.b + MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE,
                    sizeof(out.b) - MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE, 0xAB));

    memset(&out, 0xAB, sizeof(out));
    CHECK(moq_playback_get_admission(pb, &x.in, &out.info, FULL) == MOQ_OK);
    CHECK(out.info.struct_size == FULL);
    CHECK(out.info.kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(out.info.charge_bytes == 4 + sizeof(CFG_A));
    CHECK(all_bytes(out.b + FULL, 16, 0xAB));

    memset(&out, 0xAB, sizeof(out));
    CHECK(moq_playback_get_admission(pb, &x.in, &out.info, sizeof(out)) == MOQ_OK);
    CHECK(out.info.struct_size == FULL);
    CHECK(out.info.charge_bytes == 4 + sizeof(CFG_A));
    CHECK(all_bytes(out.b + FULL, 16, 0xAB));

    CHECK(moq_playback_test_buffered_count(pb) == 0);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    CHECK(moq_rcbuf_refcount(x.payload) == 1);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 4 + sizeof(CFG_A));
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Every refusal push_parsed gives an input before placement, the
 * observation gives too, writing nothing: a malformed record, a stale
 * handle (the track removed, its slot reused by a new track), an ended
 * track. */
static void test_admission_validation_parity(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ 0 });
    moq_playback_track_t t = add_track(pb);
    obj_t x;
    obj_make(&x, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A));
    moq_rcbuf_t *other = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, (const uint8_t *)"abc", 3, &other) == MOQ_OK);

    moq_playback_admission_info_t info;
    for (int k = 0; k < 6; k++) {
        moq_playback_parsed_input_t in = x.in;
        moq_media_parsed_object_t parsed = x.parsed;
        switch (k) {
        case 0: in.parsed = NULL; break;
        case 1: in.struct_size = (uint32_t)(MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE - 1); break;
        case 2: parsed.packaging = MOQ_MEDIA_PACKAGING_CMAF; in.parsed = &parsed; break;
        case 3: in.payload = other; break;
        case 4: in.payload = NULL; break;
        default: parsed.status = MOQ_OBJECT_END_OF_GROUP; in.parsed = &parsed; break;
        }
        memset(&info, 0xAB, sizeof(info));
        moq_result_t a = moq_playback_get_admission(pb, &in, &info, sizeof(info));
        moq_result_t p = moq_playback_push_parsed(pb, &in, 0);
        CHECK(a != MOQ_OK && a == p);
        CHECK(all_bytes((const uint8_t *)&info, sizeof(info), 0xAB));
    }
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    /* Ended: drained with nothing buffered. */
    CHECK(moq_playback_drain_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_TRACK_ENDED, 0) == 1);
    memset(&info, 0xAB, sizeof(info));
    CHECK(moq_playback_get_admission(pb, &x.in, &info, sizeof(info)) ==
          MOQ_ERR_WRONG_STATE);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(all_bytes((const uint8_t *)&info, sizeof(info), 0xAB));

    /* Stale: removed and replaced. */
    CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
    moq_playback_track_t t2 = add_track(pb);
    CHECK(t2._opaque != t._opaque);
    CHECK(moq_playback_get_admission(pb, &x.in, &info, sizeof(info)) ==
          MOQ_ERR_STALE_HANDLE);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_STALE_HANDLE);
    CHECK(all_bytes((const uint8_t *)&info, sizeof(info), 0xAB));
    x.in.track = t2;
    info = adm(pb, &x);
    CHECK(info.kind == MOQ_PLAYBACK_ADMISSION_FITS);

    moq_rcbuf_decref(other);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Inputs that need no placement are FITS with no charge: one already
 * buffered (a replacement at its location is absorbed), one already
 * released (absorbed by history), one below the cursor never seen (push
 * drops it STALE), one the media checks drop (no timestamp).
 * Observing them raises no event, even with the event queue full, and
 * leaves the buffer and budget as they were. */
static void test_admission_no_placement_purity(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_events = 1 });
    moq_playback_track_t t = started_track(pb, CFG_A, sizeof(CFG_A));
    CHECK(push(pb, t, 1, 2, 8, false, NULL, 0) == MOQ_OK);   /* waits for (1,1) */
    (void)count_events(pb, 0, 0);
    uint64_t retained = moq_playback_test_retained_bytes(pb);
    CHECK(retained == sizeof(CFG_A) + 8);

    obj_t dup, done, stale, nots;
    obj_make(&dup, t, 1, 2, 4, false, NULL, 0);
    obj_make(&done, t, 1, 0, 4, true, NULL, 0);
    obj_make(&stale, t, 0, 5, 4, true, NULL, 0);
    obj_make_ts(&nots, t, 1, 1, 4, false, NULL, 0, false);
    for (int round = 0; round < 2; round++) {
        moq_playback_admission_info_t a = adm(pb, &dup);
        CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_FITS && a.charge_bytes == 0);
        a = adm(pb, &done);
        CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_FITS && a.charge_bytes == 0);
        a = adm(pb, &stale);
        CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_FITS && a.charge_bytes == 0);
        a = adm(pb, &nots);
        CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_FITS && a.charge_bytes == 0);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == retained);
        if (round == 0) {
            /* Fill the one-slot event queue with push's STALE drop. */
            CHECK(moq_playback_push_parsed(pb, &stale.in, 0) == MOQ_OK);
        }
    }
    moq_playback_event_t ev;
    CHECK(moq_playback_poll_event(pb, &ev, sizeof(ev)) == MOQ_OK);
    CHECK(ev.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED &&
          ev.u.object_dropped.reason == MOQ_PLAYBACK_DROP_STALE);
    CHECK(moq_playback_poll_event(pb, &ev, sizeof(ev)) != MOQ_OK);
    CHECK(drain_cmds(pb) == 0);
    obj_free(&dup);
    obj_free(&done);
    obj_free(&stale);
    obj_free(&nots);
    moq_playback_destroy(pb);
}

/* Placement's LIMIT is NEVER, with no charge: a configuration over
 * max_track_config_bytes, an object over the whole byte budget, and one
 * that cannot fit beside configuration the tracks keep with nothing
 * buffered. */
static void test_admission_never_is_limit(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_buffered_bytes = 20;
    cfg.max_track_config_bytes = 5;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t k = started_track(pb, CFG_A, sizeof(CFG_A)); /* keeps 5 */
    moq_playback_track_t t = add_track(pb);
    obj_t x[3];
    obj_make(&x[0], t, 1, 0, 4, true, CFG_C, sizeof(CFG_C));   /* config 6 > 5 */
    obj_make(&x[1], t, 1, 0, 21, true, NULL, 0);               /* 21 > 20 */
    obj_make(&x[2], t, 1, 0, 16, true, NULL, 0);               /* 16 > 20 - 5 */
    for (int i = 0; i < 3; i++) {
        moq_playback_admission_info_t a = adm(pb, &x[i]);
        CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_NEVER && a.charge_bytes == 0);
        CHECK(moq_playback_push_parsed(pb, &x[i].in, 0) == MOQ_ERR_LIMIT);
        obj_free(&x[i]);
    }
    obj_make(&x[0], t, 1, 0, 15, true, NULL, 0);               /* exactly fits */
    CHECK(adm(pb, &x[0]).kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(moq_playback_push_parsed(pb, &x[0].in, 0) == MOQ_OK);
    obj_free(&x[0]);
    (void)k;
    moq_playback_destroy(pb);
}

/* -- Release sets ----------------------------------------------------- */

/* A paused track's buffered media would make room, but so would relief
 * alone (a track without an entry has a waiting object): the
 * classification is RELIEF, application release is not necessary. The
 * observation leaves a refused push's note in place: the next tick
 * relieves for it exactly as it would without the observation, and the
 * retry is admitted. */
static void test_admission_relief_alone(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 20 });
    moq_playback_track_t p = started_track(pb, CFG_B, sizeof(CFG_B));    /* 3 */
    set_paused(pb, p, true);
    CHECK(push(pb, p, 1, 1, 4, false, NULL, 0) == MOQ_OK);               /* +4 */
    moq_playback_track_t w = add_track(pb);
    CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);  /* +10 */
    CHECK(moq_playback_test_retained_bytes(pb) == 17);

    moq_playback_track_t e = add_track(pb);
    obj_t x;
    obj_make(&x, e, 1, 0, 4, true, CFG_A, sizeof(CFG_A));               /* 9 */
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    moq_playback_admission_info_t a = adm(pb, &x);
    CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_RELIEF);
    CHECK(a.charge_bytes == 4 + sizeof(CFG_A));
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                       MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 1);
    CHECK(moq_playback_test_retained_bytes(pb) == 7);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Object slots: a paused track's two buffered objects fill both. Only
 * their release makes room: APPLICATION. After the decoder resumes, the
 * same media is the engine's to release: ENGINE until a tick releases it,
 * then FITS. A classification is of the moment it is asked. */
static void test_admission_paused_application_then_resume(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_objects = 2 });
    moq_playback_track_t p = started_track(pb, CFG_B, sizeof(CFG_B));
    set_paused(pb, p, true);
    CHECK(push(pb, p, 1, 1, 4, false, NULL, 0) == MOQ_OK);
    CHECK(push(pb, p, 1, 2, 4, false, NULL, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(drain_cmds(pb) == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 2);

    moq_playback_track_t e = add_track(pb);
    obj_t x;
    obj_make(&x, e, 1, 0, 4, true, CFG_A, sizeof(CFG_A));
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    moq_playback_admission_info_t a = adm(pb, &x);
    CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_APPLICATION);
    CHECK(a.charge_bytes == 4 + sizeof(CFG_A));

    set_paused(pb, p, false);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_ENGINE);
    set_paused(pb, p, true);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_APPLICATION);
    set_paused(pb, p, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                       MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 0);
    drain_cmds(pb);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Bytes: a paused track's buffered object is not enough when an unpaused
 * track's object waiting on a gap holds the rest. Room waits on engine
 * progress: ENGINE, not APPLICATION. */
static void test_admission_paused_but_insufficient(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 30 });
    moq_playback_track_t p = started_track(pb, CFG_B, sizeof(CFG_B));    /* 3 */
    set_paused(pb, p, true);
    CHECK(push(pb, p, 1, 1, 4, false, NULL, 0) == MOQ_OK);               /* +4 */
    moq_playback_track_t g = started_track(pb, CFG_A, sizeof(CFG_A));    /* +5 */
    CHECK(push(pb, g, 1, 2, 16, false, NULL, 0) == MOQ_OK);              /* +16 */
    CHECK(moq_playback_test_retained_bytes(pb) == 28);

    moq_playback_track_t e = add_track(pb);
    obj_t x;
    obj_make(&x, e, 1, 0, 18, true, NULL, 0);
    moq_playback_admission_info_t a = adm(pb, &x);
    CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_ENGINE);
    CHECK(a.charge_bytes == 18);
    obj_free(&x);
    /* 24 + 6 fits: the paused object's release suffices. */
    obj_make(&x, e, 1, 0, 6, true, NULL, 0);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_APPLICATION);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Queued commands and command holds free bytes when the application polls
 * or releases them, but no object slot. */
static void test_admission_commands_versus_slots(void)
{
    /* Bytes held by queued commands: APPLICATION, FITS once polled. */
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 20 });
    moq_playback_track_t t = add_track(pb);
    CHECK(push(pb, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(push(pb, t, 1, 1, 4, false, CFG_B, sizeof(CFG_B)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 16);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    obj_t x;
    obj_make(&x, t, 1, 2, 4, false, CFG_C, sizeof(CFG_C));
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_APPLICATION);
    CHECK(drain_cmds(pb) == 4);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    obj_free(&x);
    moq_playback_destroy(pb);

    /* The only slot is an unpaused track's object waiting on a gap; queued
     * commands hold bytes besides. Polling them frees no slot: ENGINE. */
    pb = new_pb((pb_opts_t){ .max_objects = 1 });
    moq_playback_track_t g = add_track(pb);
    CHECK(push(pb, g, 1, 0, 4, true, CFG_A, sizeof(CFG_A)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(push(pb, g, 1, 2, 4, false, NULL, 0) == MOQ_OK);
    moq_playback_track_t e = add_track(pb);
    obj_make(&x, e, 1, 0, 4, true, CFG_B, sizeof(CFG_B));
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_ENGINE);
    CHECK(drain_cmds(pb) == 2);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_ENGINE);
    obj_free(&x);
    moq_playback_destroy(pb);

    /* Bytes held by command holds: APPLICATION, FITS once released. */
    pb = new_pb((pb_opts_t){ .max_bytes = 12, .max_held = 4 });
    t = add_track(pb);
    CHECK(push(pb, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t c[2];
    moq_playback_command_hold_t h[2];
    for (int i = 0; i < 2; i++)
        CHECK(moq_playback_poll_command_held(pb, &c[i], sizeof(c[i]), &h[i]) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 9);
    obj_make(&x, t, 1, 1, 4, false, NULL, 0);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_APPLICATION);
    for (int i = 0; i < 2; i++) {
        moq_playback_release_command_hold(pb, h[i]);
        moq_playback_cmd_cleanup(&c[i]);
    }
    CHECK(adm(pb, &x).kind == MOQ_PLAYBACK_ADMISSION_FITS);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* -- Configuration reuse ---------------------------------------------- */

/* A configuration the decoder slot holds is reused and survives every
 * release: the charge is the payload alone. */
static void test_admission_reuse_of_a_track_slot(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ 0 });
    moq_playback_track_t t = started_track(pb, CFG_A, sizeof(CFG_A));
    obj_t x;
    obj_make(&x, t, 1, 1, 4, false, CFG_A, sizeof(CFG_A));
    moq_playback_admission_info_t a = adm(pb, &x);
    CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_FITS && a.charge_bytes == 4);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_A) + 4);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* The copy that prices reuse is one the release itself frees. The ring
 * holds CONFIGURE(A), a unit, CONFIGURE(B), a unit (16 bytes; the track
 * keeps B). An object with A is charged its payload alone now, but once
 * the application polls the ring, A is gone and must be copied again:
 * 3 + 12 + 5 = 20. Over a 19-byte budget that is ENGINE, and NEVER after
 * the poll; over 20 it is APPLICATION, and fits after the poll. */
static void test_admission_release_destroys_queued_reuse(void)
{
    for (uint64_t budget = 19; budget <= 20; budget++) {
        moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = budget });
        moq_playback_track_t t = add_track(pb);
        CHECK(push(pb, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(push(pb, t, 1, 1, 4, false, CFG_B, sizeof(CFG_B)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 16);
        obj_t x;
        obj_make(&x, t, 1, 2, 12, false, CFG_A, sizeof(CFG_A));
        moq_playback_admission_info_t a = adm(pb, &x);
        CHECK(a.charge_bytes == 12);
        CHECK(a.kind == (budget == 19 ? MOQ_PLAYBACK_ADMISSION_ENGINE
                                      : MOQ_PLAYBACK_ADMISSION_APPLICATION));
        CHECK(drain_cmds(pb) == 4);
        a = adm(pb, &x);
        if (budget == 19) {
            CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_NEVER);
            CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_LIMIT);
        } else {
            CHECK(a.kind == MOQ_PLAYBACK_ADMISSION_FITS);
            CHECK(a.charge_bytes == 12 + sizeof(CFG_A));
            CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
        }
        obj_free(&x);
        moq_playback_destroy(pb);
    }
}

/* The same for relief: a track without an entry waits with C (4 + 6). Its
 * keyframe reuses C, charged 8 now, but relief would free the waiting
 * object and C with it: 8 + 6 = 14. Over 13 bytes relief does not make
 * room (ENGINE); over 14 it does (RELIEF). */
static void test_admission_relief_destroys_reuse(void)
{
    for (uint64_t budget = 13; budget <= 14; budget++) {
        moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = budget });
        moq_playback_track_t w = add_track(pb);
        CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        obj_t x;
        obj_make(&x, w, 1, 0, 8, true, CFG_C, sizeof(CFG_C));
        moq_playback_admission_info_t a = adm(pb, &x);
        CHECK(a.charge_bytes == 8);
        CHECK(a.kind == (budget == 13 ? MOQ_PLAYBACK_ADMISSION_ENGINE
                                      : MOQ_PLAYBACK_ADMISSION_RELIEF));
        obj_free(&x);
        moq_playback_destroy(pb);
    }
}

/* Relief makes room or drops nothing, also when the copy that priced the
 * refused object's reuse is one relief would free: a track without an
 * entry waits with (1, 1) and C (4 + 6); its keyframe (1, 0) reuses C and
 * is refused at 8 bytes, but once (1, 1) is dropped C goes with it and the
 * keyframe needs 8 + 6. Over 13 bytes relief cannot make room: the tick
 * drops nothing, (1, 1) and C stay, and the retry is still refused for
 * room (WOULD_BLOCK, not LIMIT). Over 14 it can: one drop, and the retry
 * is admitted with a new copy. */
static void test_relief_prices_lost_reuse(void)
{
    for (uint64_t budget = 13; budget <= 14; budget++) {
        moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = budget });
        moq_playback_track_t w = add_track(pb);
        CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        obj_t x;
        obj_make(&x, w, 1, 0, 8, true, CFG_C, sizeof(CFG_C));
        CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        size_t drops = count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                                    MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
        moq_result_t retry = moq_playback_push_parsed(pb, &x.in, 0);
        if (budget == 13) {
            CHECK(drops == 0);
            CHECK(moq_playback_test_buffered_count(pb) == 1);
            CHECK(moq_playback_test_retained_bytes(pb) == 4 + sizeof(CFG_C));
            CHECK(retry == MOQ_ERR_WOULD_BLOCK);
        } else {
            CHECK(drops == 1);
            CHECK(retry == MOQ_OK);
            CHECK(moq_playback_test_retained_bytes(pb) == 8 + sizeof(CFG_C));
        }
        obj_free(&x);
        drain_cmds(pb);
        moq_playback_destroy(pb);
    }
}

/* The copy that priced the reuse is gone and its table slot holds a
 * later copy: the reuse is lost, not mistaken for that copy. A track's
 * ring holds CONFIGURE(A), a unit, CONFIGURE(B), a unit (16 bytes, the
 * track keeps B); its (1, 2) with A reuses the queued A and is refused at
 * 4 bytes. Polling the ring frees A; another track then keeps a new 6-byte
 * copy D in A's slot, and a track without an entry waits with 4 bytes
 * (13 retained). The retry needs 4 + 5 again, which relief cannot make
 * room for: nothing is dropped and the retry stays refused for room. With
 * `untold` the engine's serials are spent before either copy, so neither
 * can be told apart: the same. */
static void test_relief_reuse_slot_reused(bool untold)
{
    static const uint8_t CFG_D[6] = { 0xD6, 9, 8, 7, 6, 5 };
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 16 });
    if (untold) moq_playback_test_set_copy_serial(pb, UINT64_MAX);
    moq_playback_track_t t = add_track(pb);
    CHECK(push(pb, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(push(pb, t, 1, 1, 4, false, CFG_B, sizeof(CFG_B)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 16);
    obj_t x;
    obj_make(&x, t, 1, 2, 4, false, CFG_A, sizeof(CFG_A));
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(drain_cmds(pb) == 4);
    (void)started_track(pb, CFG_D, sizeof(CFG_D));
    moq_playback_track_t w = add_track(pb);
    CHECK(push(pb, w, 1, 1, 4, false, NULL, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 13);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                       MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
    CHECK(moq_playback_test_retained_bytes(pb) == 13);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Spent serials never come round again: A is the engine's first copy
 * (serial 1) and prices a reuse; the serials are then spent, a copy X is
 * registered elsewhere, A is freed, and a copy D another track keeps takes
 * A's slot, with an 8-byte object waiting on a track without an entry.
 * D is not taken for A: the retry needs 20 + 5, which relief cannot make
 * room for, so the waiting object is not dropped. */
static void test_relief_reuse_serial_saturates(void)
{
    static const uint8_t CFG_D[6] = { 0xD6, 9, 8, 7, 6, 5 };
    static const uint8_t CFG_X[7] = { 0x77, 1, 2, 3, 4, 5, 6 };
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 40 });
    moq_playback_track_t t = add_track(pb);
    CHECK(push(pb, t, 1, 0, 4, true, CFG_A, sizeof(CFG_A)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(push(pb, t, 1, 1, 4, false, CFG_B, sizeof(CFG_B)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_test_set_copy_serial(pb, UINT64_MAX);
    moq_playback_track_t z = add_track(pb);
    CHECK(push(pb, z, 1, 0, 1, true, CFG_X, sizeof(CFG_X)) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 24);
    obj_t x;
    obj_make(&x, t, 1, 2, 20, false, CFG_A, sizeof(CFG_A));
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(drain_cmds(pb) == 4);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(drain_cmds(pb) == 2);
    (void)started_track(pb, CFG_D, sizeof(CFG_D));
    CHECK(moq_playback_test_retained_bytes(pb) == 16);
    moq_playback_track_t w = add_track(pb);
    CHECK(push(pb, w, 1, 1, 8, false, NULL, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                       MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    obj_free(&x);
    moq_playback_destroy(pb);
}

/* Relief re-prices after every drop: the track with the most waiting
 * objects holds (1, 1) and (1, 2) with C (4 + 4 + 6), another without an
 * entry one 4-byte object, 18 of 18 bytes. A 10-byte keyframe reusing C
 * fits after the first two drops as first priced, but they free C, so it
 * needs 16 and the third drop too: in one tick, three drops and the retry
 * is admitted. */
static void test_relief_reprices_after_each_drop(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 18 });
    moq_playback_track_t w = add_track(pb);
    moq_playback_track_t u = add_track(pb);
    CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
    CHECK(push(pb, w, 1, 2, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
    CHECK(push(pb, u, 1, 1, 4, false, NULL, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 18);
    obj_t x;
    obj_make(&x, w, 1, 0, 10, true, CFG_C, sizeof(CFG_C));
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                       MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 3);
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 10 + sizeof(CFG_C));
    obj_free(&x);
    drain_cmds(pb);
    moq_playback_destroy(pb);
}

/* A copy registered once the engine's serials are spent cannot be told
 * from a later one, so a reuse it priced counts as lost: never
 * underpriced. The 13-byte relief case with that copy: nothing dropped,
 * the retry still refused for room; the 14-byte case: one drop, admitted. */
static void test_relief_reuse_untold(void)
{
    for (uint64_t budget = 13; budget <= 14; budget++) {
        moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = budget });
        moq_playback_test_set_copy_serial(pb, UINT64_MAX);
        moq_playback_track_t w = add_track(pb);
        CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        obj_t x;
        obj_make(&x, w, 1, 0, 8, true, CFG_C, sizeof(CFG_C));
        CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        size_t drops = count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                                    MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
        moq_result_t retry = moq_playback_push_parsed(pb, &x.in, 0);
        CHECK(drops == (budget == 13 ? 0u : 1u));
        CHECK(retry == (budget == 13 ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
        obj_free(&x);
        drain_cmds(pb);
        moq_playback_destroy(pb);
    }
}

/* A later refusal replaces the note whole: a keyframe that reused C is
 * refused, then another track's 8-byte keyframe with no configuration.
 * Relief prices the second exactly (no reuse to lose): it drops the waiting
 * object and the retry is admitted. */
static void test_relief_note_replaced(void)
{
    moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 13 });
    moq_playback_track_t w = add_track(pb);
    CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
    obj_t x, y;
    obj_make(&x, w, 1, 0, 8, true, CFG_C, sizeof(CFG_C));
    CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
    moq_playback_track_t e = add_track(pb);
    obj_make(&y, e, 1, 0, 8, true, NULL, 0);
    CHECK(moq_playback_push_parsed(pb, &y.in, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                       MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 1);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    CHECK(moq_playback_push_parsed(pb, &y.in, 0) == MOQ_OK);
    obj_free(&x);
    obj_free(&y);
    drain_cmds(pb);
    moq_playback_destroy(pb);
}

/* Relief across ticks with a one-slot event queue: two waiting objects
 * share C (4 + 4 + 6 of 14 bytes). An 8-byte keyframe reusing C needs both
 * dropped and C's bytes again, 14: one drop per tick, then admitted. A
 * 9-byte one would need 15 once C is gone, so relief drops nothing at all,
 * on any tick, and the retry stays refused for room. */
static void test_relief_lost_reuse_across_ticks(void)
{
    for (size_t plen = 8; plen <= 9; plen++) {
        moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 14, .max_events = 1 });
        moq_playback_track_t w = add_track(pb);
        CHECK(push(pb, w, 1, 1, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        CHECK(push(pb, w, 1, 2, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        obj_t x;
        obj_make(&x, w, 1, 0, plen, true, CFG_C, sizeof(CFG_C));
        CHECK(moq_playback_push_parsed(pb, &x.in, 0) == MOQ_ERR_WOULD_BLOCK);
        size_t drops = 0, ticks_with_drop = 0;
        for (int i = 0; i < 4; i++) {
            (void)moq_playback_tick(pb, 0);
            size_t d = count_events(pb, MOQ_PLAYBACK_EVENT_OBJECT_DROPPED,
                                    MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
            drops += d;
            ticks_with_drop += d > 0;
        }
        moq_result_t retry = moq_playback_push_parsed(pb, &x.in, 0);
        if (plen == 8) {
            CHECK(drops == 2 && ticks_with_drop == 2);
            CHECK(retry == MOQ_OK);
            CHECK(moq_playback_test_retained_bytes(pb) == 14);
        } else {
            CHECK(drops == 0);
            CHECK(moq_playback_test_buffered_count(pb) == 2);
            CHECK(retry == MOQ_ERR_WOULD_BLOCK);
        }
        obj_free(&x);
        drain_cmds(pb);
        moq_playback_destroy(pb);
    }
}

/* Mixed holders. A paused track's ring holds CONFIGURE(C), a unit,
 * CONFIGURE(D), a unit; the track keeps D; its buffered object reuses C
 * from the ring. C has an object holder and a ring holder, both the
 * application's to release, so it is freed with them; D, kept by the track,
 * is not: 21 retained, 3 after the release. Another track's keyframe with
 * a new 3-byte configuration fits a 21-byte budget after the release with
 * a 12-byte payload (APPLICATION), not with 16 (ENGINE). */
static void test_admission_mixed_holders(void)
{
    for (int big = 0; big < 2; big++) {
        moq_playback_t *pb = new_pb((pb_opts_t){ .max_bytes = 21 });
        moq_playback_track_t p = add_track(pb);
        CHECK(push(pb, p, 1, 0, 4, true, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(push(pb, p, 1, 1, 4, false, CFG_B, sizeof(CFG_B)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        set_paused(pb, p, true);
        CHECK(push(pb, p, 1, 2, 4, false, CFG_C, sizeof(CFG_C)) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 21);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        moq_playback_track_t e = add_track(pb);
        obj_t x;
        obj_make(&x, e, 1, 0, big ? 16 : 12, true, CFG_B, sizeof(CFG_B));
        moq_playback_admission_info_t a = adm(pb, &x);
        CHECK(a.charge_bytes == (big ? 16u : 12u) + sizeof(CFG_B));
        CHECK(a.kind == (big ? MOQ_PLAYBACK_ADMISSION_ENGINE
                             : MOQ_PLAYBACK_ADMISSION_APPLICATION));
        obj_free(&x);
        drain_cmds(pb);
        moq_playback_destroy(pb);
    }
}

int main(void)
{
    test_admission_output_contract();
    test_admission_validation_parity();
    test_admission_no_placement_purity();
    test_admission_never_is_limit();
    test_admission_relief_alone();
    test_admission_paused_application_then_resume();
    test_admission_paused_but_insufficient();
    test_admission_commands_versus_slots();
    test_admission_reuse_of_a_track_slot();
    test_admission_release_destroys_queued_reuse();
    test_admission_relief_destroys_reuse();
    test_admission_mixed_holders();
    test_relief_prices_lost_reuse();
    test_relief_reuse_slot_reused(false);
    test_relief_reuse_slot_reused(true);
    test_relief_reuse_serial_saturates();
    test_relief_reprices_after_each_drop();
    test_relief_reuse_untold();
    test_relief_note_replaced();
    test_relief_lost_reuse_across_ticks();
    CHECK(g_balance == 0);
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("playback admission: all passed\n");
    return 0;
}
