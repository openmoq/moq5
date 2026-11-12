/* Playback on LOC-05 tracks: each object decodes under its codec (its own
 * effective Codec String, else the catalog codec) and a configuration
 * declared for that codec; CONFIGURE carries the pair and follows any change
 * of either half; codec copies are charged and released like configuration
 * copies. Both push paths are exercised. */
#include <moq/playback.h>
#include <moq/media_object.h>
#include <moq/loc.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb);

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static int64_t g_balance;
static uint64_t g_allocs, g_fail_at;
static void *c_alloc(size_t n, void *c)
{
    (void)c;
    g_allocs++;
    if (g_fail_at && g_allocs == g_fail_at) return NULL;
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

#define T0 UINT64_C(1700000000000000)
#define CAT "avc1.64001f"              /* the catalog codec, 11 bytes */
static const uint8_t CFG_CAT[4] = { 0xC0, 9, 9, 9 };   /* catalog init_data */
static const uint8_t CFG_A[5] = { 0xA1, 1, 2, 3, 4 };
static const uint8_t CFG_B[3] = { 0xB2, 5, 6 };
#define X "avc1.640028"                /* another codec, 11 bytes */
#define Y "hev1.1.6.L93"               /* a third, 12 bytes */

static moq_playback_t *new_pb(uint32_t max_commands, uint32_t max_config,
                              uint32_t max_held)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    if (max_commands) cfg.max_commands = max_commands;
    if (max_config) cfg.max_track_config_bytes = max_config;
    cfg.max_held_commands = max_held;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

/* A RAW draft-22 track naming LOC-05 (or the draft default for 0), with
 * the catalog codec and configuration, and optional Track Properties. */
static moq_playback_track_t add_track(moq_playback_t *pb, uint32_t loc_profile,
                                      moq_rcbuf_t *track_props)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = (uint32_t)MOQ_VERSION_DRAFT_22;
    tc.codec = (moq_bytes_t){ (const uint8_t *)CAT, 11 };
    tc.init_data = (moq_bytes_t){ CFG_CAT, sizeof(CFG_CAT) };
    if (track_props)
        tc.track_properties = (moq_bytes_t){ moq_rcbuf_data(track_props),
                                             moq_rcbuf_len(track_props) };
    tc.loc_profile = loc_profile;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    return t;
}

/* LOC-05 object properties: a UNIX timestamp and marking, an optional video
 * configuration (has_cfg, possibly empty) and an optional Codec String. */
static moq_rcbuf_t *props05(uint64_t ts, bool kf, const uint8_t *cfg,
                            size_t cfg_len, bool has_cfg, const char *codec)
{
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    m.timestamp = ts;
    m.marking_len = 1;
    m.marking[0] = (uint8_t)(0xC0u | (kf ? 0x20u : 0u));
    if (has_cfg) {
        m.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        m.video_config = (moq_bytes_t){ cfg, cfg_len };
    }
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    if (codec) {
        e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
        e.codec_string = (moq_bytes_t){ (const uint8_t *)codec, strlen(codec) };
    }
    moq_rcbuf_t *p = NULL;
    CHECK(moq_loc_encode_metadata_ex(&g_alloc, MOQ_VERSION_DRAFT_22,
                                     MOQ_LOC_PROFILE_05, &m, codec ? &e : NULL,
                                     &p) == MOQ_OK);
    return p;
}

#define UNIT 4u

static moq_result_t push05(moq_playback_t *pb, moq_playback_track_t t,
                           bool via_parsed, uint64_t o, moq_rcbuf_t *props,
                           const moq_loc_ext_t *track_ext)
{
    static const uint8_t frame[UNIT] = { 0, 0, 1, 0x65 };
    moq_rcbuf_t *payload = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, frame, sizeof(frame), &payload) == MOQ_OK);
    moq_result_t rc;
    if (via_parsed) {
        moq_media_track_info_t ti;
        moq_media_track_info_init_sized(&ti, sizeof(ti));
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;
        ti.transport_version = MOQ_VERSION_DRAFT_22;
        moq_media_object_input_t in;
        moq_media_object_input_init(&in);
        in.group_id = 1;
        in.object_id = o;
        in.payload = payload;
        in.properties = props;
        moq_media_object_parse_cfg_t pc = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
        pc.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
        pc.track_loc_ext = track_ext;
        moq_media_parsed_object_t parsed;
        moq_media_drop_reason_t why = 0;
        CHECK(moq_media_object_parse_sized(&ti, &in, &pc, NULL, 0, &parsed,
                                           sizeof(parsed), &why) == MOQ_OK);
        moq_playback_parsed_input_t pin;
        moq_playback_parsed_input_init(&pin);
        pin.track = t;
        pin.group_id = 1;
        pin.object_id = o;
        pin.parsed = &parsed;
        pin.payload = payload;
        rc = moq_playback_push_parsed(pb, &pin, 0);
        memset(&parsed, 0xEE, sizeof(parsed));   /* nothing borrowed survives */
    } else {
        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = o;
        obj.payload = payload;
        obj.properties = props;
        rc = moq_playback_push_object(pb, &obj, 0);
    }
    moq_rcbuf_decref(props);
    moq_rcbuf_decref(payload);
    return rc;
}

/* Every command: its kind, CONFIGURE's codec and configuration, or the
 * decoded object. */
typedef struct {
    size_t   n;
    uint32_t kind[16];
    char     codec[16][16];
    bool     cfg_null[16];
    uint8_t  cfg0[16];
    size_t   cfg_len[16];
    uint64_t object[16];
} seq_t;

static void drain(moq_playback_t *pb, seq_t *q)
{
    memset(q, 0, sizeof(*q));
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (q->n < 16) {
            q->kind[q->n] = c.kind;
            if (c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO) {
                moq_rcbuf_t *codec = c.u.configure_video.codec;
                moq_rcbuf_t *cfg = c.u.configure_video.codec_config;
                if (codec && moq_rcbuf_len(codec) < 16)
                    memcpy(q->codec[q->n], moq_rcbuf_data(codec), moq_rcbuf_len(codec));
                q->cfg_null[q->n] = cfg == NULL;
                q->cfg_len[q->n] = cfg ? moq_rcbuf_len(cfg) : 0;
                q->cfg0[q->n] = cfg && moq_rcbuf_len(cfg) ? moq_rcbuf_data(cfg)[0] : 0;
            } else if (c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) {
                q->object[q->n] = c.u.decode_video.object_id;
            }
            q->n++;
        }
        moq_playback_cmd_cleanup(&c);
    }
}

static bool configure(const seq_t *q, size_t i, const char *codec, uint8_t cfg0)
{
    return q->kind[i] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO &&
           strcmp(q->codec[i], codec) == 0 &&
           (cfg0 ? q->cfg0[i] == cfg0 : q->cfg_null[i]);
}

static bool decode(const seq_t *q, size_t i, uint64_t object)
{
    return q->kind[i] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q->object[i] == object;
}

/* Without any Codec String a configuration carries forward and the
 * catalog codec stays. */
static void test_same_codec_keeps_released_configuration(void)
{
    for (int via = 0; via < 2; via++) {
        moq_playback_t *pb = new_pb(0, 0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
        CHECK(push05(pb, t, via, 0, props05(T0, true, CFG_A, 5, true, NULL), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 1, props05(T0 + 1, false, NULL, 0, false, NULL), NULL) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        drain(pb, &q);
        CHECK(q.n == 3 && configure(&q, 0, CAT, 0xA1) && decode(&q, 1, 0) && decode(&q, 2, 1));
        moq_playback_destroy(pb);
    }
    CHECK(g_balance == 0);
}

/* A new codec without a declared configuration gets none: never the
 * previous codec's bytes, never the catalog's. Back on the catalog codec,
 * with no released pair of that codec, the catalog configuration returns. */
static void test_codec_change_without_configuration(void)
{
    for (int via = 0; via < 2; via++) {
        moq_playback_t *pb = new_pb(0, 0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
        CHECK(push05(pb, t, via, 0, props05(T0, true, CFG_A, 5, true, NULL), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 1, props05(T0 + 1, false, NULL, 0, false, X), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 2, props05(T0 + 2, false, NULL, 0, false, NULL), NULL) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        drain(pb, &q);
        CHECK(q.n == 6);
        CHECK(configure(&q, 0, CAT, 0xA1) && decode(&q, 1, 0));
        CHECK(configure(&q, 2, X, 0) && decode(&q, 3, 1));
        CHECK(configure(&q, 4, CAT, 0xC0) && decode(&q, 5, 2));
        moq_playback_destroy(pb);
    }
    CHECK(g_balance == 0);
}

/* A declared configuration wins, a present-empty one included, and a
 * codec-only change emits CONFIGURE even with equal configuration bytes. */
static void test_declared_configuration_and_codec_only_change(void)
{
    for (int via = 0; via < 2; via++) {
        moq_playback_t *pb = new_pb(0, 0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
        CHECK(push05(pb, t, via, 0, props05(T0, true, NULL, 0, true, X), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 1, props05(T0 + 1, false, CFG_B, 3, true, X), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 2, props05(T0 + 2, false, CFG_B, 3, true, Y), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 3, props05(T0 + 3, false, CFG_B, 3, true, NULL), NULL) == MOQ_OK);
        CHECK(push05(pb, t, via, 4, props05(T0 + 4, false, CFG_B, 3, true, NULL), NULL) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        drain(pb, &q);
        CHECK(q.n == 9);
        CHECK(configure(&q, 0, X, 0) && q.cfg_len[0] == 0 && decode(&q, 1, 0));
        CHECK(configure(&q, 2, X, 0xB2) && decode(&q, 3, 1));
        CHECK(configure(&q, 4, Y, 0xB2) && decode(&q, 5, 2));
        CHECK(configure(&q, 6, CAT, 0xB2) && decode(&q, 7, 3));
        CHECK(decode(&q, 8, 4));      /* same pair: no CONFIGURE */
        moq_playback_destroy(pb);
    }
    CHECK(g_balance == 0);
}

/* A Codec String among the Track Properties applies to every object without
 * its own, on push_object (playback parses) and push_parsed (the caller
 * parsed with the track's record). One equal to the catalog codec needs no
 * copy. */
static void test_track_codec_and_catalog_equal(void)
{
    moq_loc_metadata_t none;
    moq_loc_metadata_init_sized(&none, sizeof(none));
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    e.codec_string = (moq_bytes_t){ (const uint8_t *)X, 11 };
    for (int via = 0; via < 2; via++) {
        moq_rcbuf_t *tp = NULL;
        CHECK(moq_loc_encode_metadata_ex(&g_alloc, MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_05,
                                         &none, &e, &tp) == MOQ_OK);
        moq_playback_t *pb = new_pb(0, 0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, tp);
        CHECK(push05(pb, t, via, 0, props05(T0, true, CFG_A, 5, true, NULL), &e) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        drain(pb, &q);
        CHECK(q.n == 2 && configure(&q, 0, X, 0xA1) && decode(&q, 1, 0));
        moq_playback_destroy(pb);
        moq_rcbuf_decref(tp);
    }
    moq_playback_t *pb = new_pb(0, 0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
    CHECK(push05(pb, t, false, 0, props05(T0, true, NULL, 0, false, CAT), NULL) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT);   /* no codec copy */
    moq_playback_destroy(pb);
    /* A LOC-04 track (the draft default) skips the Codec String. */
    pb = new_pb(0, 0, 0);
    t = add_track(pb, 0, NULL);
    CHECK(push05(pb, t, false, 0, props05(T0, true, NULL, 0, false, X), NULL) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    seq_t q;
    drain(pb, &q);
    CHECK(q.n == 2 && configure(&q, 0, CAT, 0xC0));
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);
}

/* The track's configuration belongs to the track's codec: its own Codec
 * String, else the catalog's (draft-ietf-moq-loc-05 2.3.1.3, 2.3.2.1). An
 * object naming another codec decodes with none, never with it; one naming
 * the track's codec, or none, decodes with it. */
static void test_track_configuration_stays_with_its_codec(void)
{
    moq_loc_metadata_t tm;
    moq_loc_metadata_init_sized(&tm, sizeof(tm));
    tm.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    tm.video_config = (moq_bytes_t){ CFG_B, sizeof(CFG_B) };
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    e.codec_string = (moq_bytes_t){ (const uint8_t *)X, 11 };
    for (int named = 0; named < 2; named++) {
        moq_rcbuf_t *tp = NULL;
        CHECK(moq_loc_encode_metadata_ex(&g_alloc, MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_05,
                                         &tm, named ? &e : NULL, &tp) == MOQ_OK);
        moq_playback_t *pb = new_pb(0, 0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, tp);
        const char *own = named ? X : CAT;   /* the track's codec */
        CHECK(push05(pb, t, false, 0, props05(T0, true, NULL, 0, false, NULL), NULL) == MOQ_OK);
        CHECK(push05(pb, t, false, 1, props05(T0 + 1, false, NULL, 0, false, Y), NULL) == MOQ_OK);
        CHECK(push05(pb, t, false, 2, props05(T0 + 2, false, NULL, 0, false, own), NULL) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        drain(pb, &q);
        CHECK(q.n == 6);
        CHECK(configure(&q, 0, own, 0xB2) && decode(&q, 1, 0));
        CHECK(configure(&q, 2, Y, 0) && decode(&q, 3, 1));
        CHECK(configure(&q, 4, own, 0xB2) && decode(&q, 5, 2));
        moq_playback_destroy(pb);
        moq_rcbuf_decref(tp);
    }
    CHECK(g_balance == 0);
}

/* A CONFIGURE that committed while its DECODE could not: the decoder's pair
 * moves with the CONFIGURE, the released pair only with released media. */
static void test_configure_committed_decode_blocked(void)
{
    moq_playback_t *pb = new_pb(1, 0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
    CHECK(push05(pb, t, false, 0, props05(T0, true, CFG_A, 5, true, X), NULL) == MOQ_OK);
    CHECK(push05(pb, t, false, 1, props05(T0 + 1, false, NULL, 0, false, NULL), NULL) == MOQ_OK);
    seq_t q;
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);   /* CONFIGURE only */
    drain(pb, &q);
    CHECK(q.n == 1 && configure(&q, 0, X, 0xA1));
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);   /* decode 0 */
    drain(pb, &q);
    CHECK(q.n == 1 && decode(&q, 0, 0));
    /* Object 1 is on the catalog codec: the released (X, A) does not apply. */
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
    drain(pb, &q);
    CHECK(q.n == 1 && configure(&q, 0, CAT, 0xC0));
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain(pb, &q);
    CHECK(q.n == 1 && decode(&q, 0, 1));
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);
}

/* Codec copies are charged once while any holder keeps them, through the
 * object, the track's slots, a queued CONFIGURE and a command hold. */
static void test_codec_copy_accounting(void)
{
    moq_playback_t *pb = new_pb(0, 0, 4);
    moq_playback_track_t t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
    CHECK(push05(pb, t, true, 0, props05(T0, true, CFG_A, 5, true, X), NULL) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT + 5 + 11);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    /* Held CONFIGURE: its codec and configuration stay charged until the
     * hold ends; the track keeps them as its decoder and release pair. */
    moq_playback_cmd_t c;
    moq_playback_command_hold_t h;
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_OK);
    CHECK(c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
    CHECK(c.u.configure_video.codec && moq_rcbuf_len(c.u.configure_video.codec) == 11);
    moq_playback_cmd_cleanup(&c);
    moq_playback_release_command_hold(pb, h);
    seq_t q;
    drain(pb, &q);                       /* the decode */
    CHECK(q.n == 1 && decode(&q, 0, 0));
    CHECK(moq_playback_test_retained_bytes(pb) == 5 + 11);
    CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);

    /* Buffered objects with codec copies are freed with the engine. */
    pb = new_pb(0, 0, 0);
    t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
    CHECK(push05(pb, t, false, 0, props05(T0, true, NULL, 0, false, X), NULL) == MOQ_OK);
    CHECK(push05(pb, t, false, 1, props05(T0 + 1, false, NULL, 0, false, Y), NULL) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 2 * UNIT + 11 + 12);
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);
}

/* Refusals retain nothing: a Codec String over the per-track cap can never
 * be admitted; a failed copy is NOMEM, and the same push then succeeds. */
static void test_refusals(void)
{
    moq_playback_t *pb = new_pb(0, 8, 0);
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = (uint32_t)MOQ_VERSION_DRAFT_22;
    tc.loc_profile = MOQ_LOC_PROFILE_05;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    CHECK(push05(pb, t, false, 0, props05(T0, true, NULL, 0, false, X), NULL) == MOQ_ERR_LIMIT);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);

    pb = new_pb(0, 0, 0);
    t = add_track(pb, MOQ_LOC_PROFILE_05, NULL);
    moq_rcbuf_t *props = props05(T0, true, NULL, 0, false, X);
    moq_rcbuf_t *payload = NULL;
    static const uint8_t frame[UNIT] = { 0, 0, 1, 0x65 };
    CHECK(moq_rcbuf_create(&g_alloc, frame, UNIT, &payload) == MOQ_OK);
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = 1;
    obj.payload = payload;
    obj.properties = props;
    /* Fail each allocation the push makes in turn until one succeeds. */
    moq_result_t rc = MOQ_ERR_NOMEM;
    for (uint64_t k = 1; rc == MOQ_ERR_NOMEM && k < 16; k++) {
        g_fail_at = g_allocs + k;
        rc = moq_playback_push_object(pb, &obj, 0);
        g_fail_at = 0;
        if (rc == MOQ_ERR_NOMEM)
            CHECK(moq_playback_test_retained_bytes(pb) == 0);
    }
    CHECK(rc == MOQ_OK);
    moq_rcbuf_decref(props);
    moq_rcbuf_decref(payload);
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);

    /* A named revision must belong to the track's draft. */
    pb = new_pb(0, 0, 0);
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = (uint32_t)MOQ_VERSION_DRAFT_18;
    tc.loc_profile = MOQ_LOC_PROFILE_05;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
    tc.transport_version = (uint32_t)MOQ_VERSION_DRAFT_22;
    tc.loc_profile = MOQ_LOC_PROFILE_01;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
    tc.loc_profile = 3;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
    moq_playback_destroy(pb);
    CHECK(g_balance == 0);
}

int main(void)
{
    test_same_codec_keeps_released_configuration();
    test_codec_change_without_configuration();
    test_declared_configuration_and_codec_only_change();
    test_track_codec_and_catalog_equal();
    test_track_configuration_stays_with_its_codec();
    test_configure_committed_decode_blocked();
    test_codec_copy_accounting();
    test_refusals();
    printf("%s: %d failed checks\n", failures ? "FAIL" : "PASS", failures);
    return failures != 0;
}
