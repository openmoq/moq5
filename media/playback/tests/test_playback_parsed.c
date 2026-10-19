/*
 * Parsed-input rows: push_parsed ownership and validation, permanent
 * refusal (MOQ_ERR_LIMIT) without a cursor jump, multi-sample CMAF released
 * through a small command ring with exact per-sample timing, and multi-chunk
 * CMAF refused rather than truncated.
 */
#include <moq/playback.h>
#include <moq/media_object.h>
#include <moq/loc.h>
#include <moq/rcbuf.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb);
extern void moq_playback_test_set_hold_generation(moq_playback_t *pb, uint32_t slot,
                                                  uint32_t gen);
extern uint64_t moq_playback_test_skip_epochs(uint64_t n);
extern void moq_playback_test_local_issuer(moq_playback_t *pb, uint64_t remaining);
extern uint16_t moq_playback_test_creation_tag(const moq_playback_t *pb);
extern size_t moq_playback_test_buffered_count(const moq_playback_t *pb);
extern uint64_t moq_playback_test_fragment_reads(const moq_playback_t *pb);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Counting allocator ---------------------------------------------- */

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

/* -- Engine helpers --------------------------------------------------- */

#define T0 UINT64_C(1700000000000000)

static moq_playback_t *new_pb(uint32_t max_commands, uint64_t max_bytes)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    if (max_commands) cfg.max_commands = max_commands;
    if (max_bytes) cfg.max_buffered_bytes = max_bytes;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

static moq_playback_track_t add_track(moq_playback_t *pb,
                                      moq_playback_packaging_t packaging,
                                      moq_version_t version,
                                      uint32_t timescale)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = packaging;
    tc.transport_version = (uint32_t)version;
    tc.timescale = timescale;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    return t;
}

typedef struct {
    size_t   configures;
    size_t   decodes;
    uint64_t object[32];
    uint64_t dts[32];
    size_t   mdat_off[32];
    size_t   mdat_len[32];
    bool     keyframe[32];
} cmd_log_t;

static void drain_cmds(moq_playback_t *pb, cmd_log_t *log, bool append)
{
    if (!append) memset(log, 0, sizeof(*log));
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO) {
            log->configures++;
        } else if (log->decodes < 32 &&
                   c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) {
            log->object[log->decodes] = c.u.decode_video.object_id;
            log->dts[log->decodes] = c.u.decode_video.decode_time_us;
            log->keyframe[log->decodes] = c.u.decode_video.keyframe;
            log->decodes++;
        } else if (log->decodes < 32 &&
                   c.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF) {
            log->object[log->decodes] = c.u.decode_cmaf.object_id;
            log->dts[log->decodes] = c.u.decode_cmaf.decode_time_us;
            log->mdat_off[log->decodes] = c.u.decode_cmaf.mdat_offset;
            log->mdat_len[log->decodes] = c.u.decode_cmaf.mdat_len;
            log->keyframe[log->decodes] = c.u.decode_cmaf.keyframe;
            log->decodes++;
        }
        moq_playback_cmd_cleanup(&c);
    }
}

static size_t count_drops(moq_playback_t *pb, moq_playback_drop_reason_t r)
{
    size_t n = 0;
    moq_playback_event_t e;
    while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK)
        if (e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED &&
            e.u.object_dropped.reason == r)
            n++;
    return n;
}

/* -- Object builders -------------------------------------------------- */

static moq_rcbuf_t *rc_of(const uint8_t *d, size_t n)
{
    moq_rcbuf_t *b = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, d, n, &b) == MOQ_OK);
    return b;
}

static moq_rcbuf_t *loc01(moq_version_t v, uint64_t ts, bool kf)
{
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = ts;
    h.has_video_frame_marking = true;
    h.video_frame_marking.independent = kf;
    moq_rcbuf_t *p = NULL;
    CHECK(moq_loc_encode(&g_alloc, v, MOQ_LOC_PROFILE_01, &h, &p) == MOQ_OK);
    return p;
}

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static size_t box(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}

/* One CMAF chunk (moof + mdat) of `n` samples of `size` bytes and
 * `duration` ticks from `base`. The first sample is a sync sample. */
static size_t cmaf_chunk(uint8_t *buf, uint64_t base, uint32_t duration,
                         size_t n, uint32_t size)
{
    size_t p = 0;
    size_t trun_sz = 8 + 4 + 4 + 4 + n * 12;
    size_t tfdt_sz = 8 + 4 + 8;
    size_t tfhd_sz = 8 + 8;
    size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
    size_t moof_sz = 8 + traf_sz;
    p += box(buf + p, (uint32_t)moof_sz, "moof");
    p += box(buf + p, (uint32_t)traf_sz, "traf");
    p += box(buf + p, (uint32_t)tfhd_sz, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box(buf + p, (uint32_t)tfdt_sz, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base); p += 4;
    p += box(buf + p, (uint32_t)trun_sz, "trun");
    wr32(buf + p, 0x000701); p += 4;            /* offset, duration, size, flags */
    wr32(buf + p, (uint32_t)n); p += 4;
    wr32(buf + p, (uint32_t)(moof_sz + 8)); p += 4;   /* data offset into mdat */
    for (size_t i = 0; i < n; i++) {
        wr32(buf + p, duration); p += 4;
        wr32(buf + p, size); p += 4;
        wr32(buf + p, i == 0 ? 0x02000000u : 0x01010000u); p += 4;
    }
    p += box(buf + p, (uint32_t)(8 + n * size), "mdat");
    memset(buf + p, 0xAB, n * size);
    p += n * size;
    return p;
}

/* One sample whose presentation differs from its decode time by a signed
 * composition offset (trun version 1). */
static size_t cmaf_chunk_cto(uint8_t *buf, uint64_t base, uint32_t duration,
                             int64_t cto, bool sync, uint32_t size)
{
    size_t p = 0;
    size_t trun_sz = 8 + 4 + 4 + 4 + 16;
    size_t tfdt_sz = 8 + 4 + 8;
    size_t tfhd_sz = 8 + 8;
    size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
    size_t moof_sz = 8 + traf_sz;
    p += box(buf + p, (uint32_t)moof_sz, "moof");
    p += box(buf + p, (uint32_t)traf_sz, "traf");
    p += box(buf + p, (uint32_t)tfhd_sz, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box(buf + p, (uint32_t)tfdt_sz, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base); p += 4;
    p += box(buf + p, (uint32_t)trun_sz, "trun");
    wr32(buf + p, 0x01000F01); p += 4;   /* v1: offset, duration, size, flags, cto */
    wr32(buf + p, 1); p += 4;
    wr32(buf + p, (uint32_t)(moof_sz + 8)); p += 4;
    wr32(buf + p, duration); p += 4;
    wr32(buf + p, size); p += 4;
    wr32(buf + p, sync ? 0x02000000u : 0x01010000u); p += 4;
    wr32(buf + p, (uint32_t)(int32_t)cto); p += 4;
    p += box(buf + p, (uint32_t)(8 + size), "mdat");
    memset(buf + p, 0xAB, size);
    p += size;
    return p;
}

static moq_result_t push_obj(moq_playback_t *pb, moq_playback_track_t t,
                             uint64_t g, uint64_t o, moq_rcbuf_t *payload,
                             moq_rcbuf_t *props)
{
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = g;
    obj.object_id = o;
    obj.payload = payload;
    obj.properties = props;
    return moq_playback_push_object(pb, &obj, 0);
}

/* Parse an object as a caller of push_parsed would, with the frozen parser
 * (drafts 16/18) or the sized D22 companion parser. */
static moq_result_t parse_obj(moq_version_t v, moq_media_packaging_t packaging,
                              uint32_t timescale, uint64_t g, uint64_t o,
                              moq_rcbuf_t *payload, moq_rcbuf_t *props,
                              moq_cmaf_sample_t *samples, size_t cap,
                              moq_media_parsed_object_t *out)
{
    moq_media_track_info_t ti;
    moq_media_track_info_init_sized(&ti, sizeof(ti));
    ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
    ti.packaging = packaging;
    ti.timescale = timescale;
    ti.transport_version = v;
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.group_id = g;
    in.object_id = o;
    in.payload = payload;
    in.properties = props;
    moq_media_drop_reason_t why = 0;
    if (v != MOQ_VERSION_DRAFT_22)
        return moq_media_object_parse(&ti, &in, samples, cap, out, &why);
    moq_media_object_parse_cfg_t pc = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    pc.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    return moq_media_object_parse_sized(&ti, &in, &pc, samples, cap, out,
                                        sizeof(*out), &why);
}

static moq_playback_parsed_input_t parsed_in(moq_playback_track_t t,
                                             uint64_t g, uint64_t o,
                                             const moq_media_parsed_object_t *p,
                                             moq_rcbuf_t *payload)
{
    moq_playback_parsed_input_t in;
    moq_playback_parsed_input_init(&in);
    in.track = t;
    in.group_id = g;
    in.object_id = o;
    in.parsed = p;
    in.payload = payload;
    return in;
}

/* Draft-22 LOC-04 properties: a UNIX timestamp, a frame marking, and, when
 * cfg is set, a video or audio configuration of cfg_len bytes (zero for a
 * present-but-empty configuration). */
static moq_rcbuf_t *d22_props(uint64_t ts, bool kf, bool audio,
                              const uint8_t *cfg, size_t cfg_len,
                              bool has_cfg)
{
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    m.timestamp = ts;
    if (!audio) {
        m.present |= MOQ_LOC_METADATA_HAS_MARKING;
        m.marking_len = 1;
        m.marking[0] = (uint8_t)(0xC0u | (kf ? 0x20u : 0u));
    }
    if (has_cfg) {
        m.present |= audio ? MOQ_LOC_METADATA_HAS_AUDIO_CONFIG
                           : MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        moq_bytes_t b = { cfg, cfg_len };
        if (audio) m.audio_config = b; else m.video_config = b;
    }
    moq_rcbuf_t *p = NULL;
    CHECK(moq_loc_encode_metadata(&g_alloc, MOQ_VERSION_DRAFT_22,
                                  MOQ_LOC_PROFILE_04, &m, &p) == MOQ_OK);
    return p;
}

/* Every command, in order, as (kind, config bytes or object id). */
typedef struct {
    size_t   n;
    uint32_t kind[32];
    size_t   cfg_len[32];
    uint8_t  cfg0[32];
    bool     cfg_null[32];
    uint64_t object[32];
} seq_t;

static void drain_seq(moq_playback_t *pb, seq_t *q)
{
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (q->n < 32) {
            q->kind[q->n] = c.kind;
            moq_rcbuf_t *cfg = c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO
                ? c.u.configure_video.codec_config
                : c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO
                    ? c.u.configure_audio.codec_config : NULL;
            q->cfg_null[q->n] = cfg == NULL;
            q->cfg_len[q->n] = cfg ? moq_rcbuf_len(cfg) : 0;
            q->cfg0[q->n] = cfg && moq_rcbuf_len(cfg) ? moq_rcbuf_data(cfg)[0] : 0;
            q->object[q->n] = c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO
                ? c.u.decode_video.object_id
                : c.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO
                    ? c.u.decode_audio.object_id : 0;
            q->n++;
        }
        moq_playback_cmd_cleanup(&c);
    }
}

/* The payload bytes of each unit push_d22 pushes. */
#define UNIT 4u

static moq_result_t push_d22(moq_playback_t *pb, moq_playback_track_t t,
                             bool via_parsed, uint64_t g, uint64_t o,
                             moq_rcbuf_t *props)
{
    static const uint8_t frame[UNIT] = { 0, 0, 1, 0x65 };
    moq_rcbuf_t *payload = rc_of(frame, sizeof(frame));
    moq_result_t rc;
    if (via_parsed) {
        moq_media_parsed_object_t parsed;
        CHECK(parse_obj(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW, 0, g, o,
                        payload, props, NULL, 0, &parsed) == MOQ_OK);
        moq_playback_parsed_input_t in = parsed_in(t, g, o, &parsed, payload);
        rc = moq_playback_push_parsed(pb, &in, 0);
        memset(&parsed, 0xEE, sizeof(parsed));
    } else {
        rc = push_obj(pb, t, g, o, payload, props);
    }
    /* Neither the properties nor the parsed record outlive the push. */
    moq_rcbuf_decref(props);
    moq_rcbuf_decref(payload);
    return rc;
}

/* -- RAW decode order ---------------------------------------------------- */

/* A RAW (LOC) group in decode order I P B B presents at 0, 99, 33 and 66 ms:
 * LOC carries no decode timestamp, so the object's media time decreases in
 * decode order with frame reordering. On every companion -- LOC-01 on
 * drafts 16/18 (pushed whole and pre-parsed), LOC-04 on draft 22 with a
 * UNIX time and with application media time -- the engine releases all four
 * in object order with their own payloads and unchanged times, drops
 * nothing, and frees everything. The objects arrive 0, 2, 1, 3 with a tick
 * after each, and the command queue holds two, so no single batch carries
 * the order. */
static moq_rcbuf_t *raw_order_props(int mode, uint64_t ms, bool kf)
{
    switch (mode) {
    case 0: return loc01(MOQ_VERSION_DRAFT_16, T0 + ms * 1000, kf);
    case 1: return loc01(MOQ_VERSION_DRAFT_18, T0 + ms * 1000, kf);
    case 2: return d22_props(T0 + ms * 1000, kf, false, NULL, 0, false);
    default: {
        moq_loc_metadata_t m;
        moq_loc_metadata_init_sized(&m, sizeof(m));
        m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_TIMESCALE |
                    MOQ_LOC_METADATA_HAS_MARKING;
        m.timestamp = ms;
        m.timescale = 1000;
        m.marking_len = 1;
        m.marking[0] = (uint8_t)(0xC0u | (kf ? 0x20u : 0u));
        moq_rcbuf_t *p = NULL;
        CHECK(moq_loc_encode_metadata(&g_alloc, MOQ_VERSION_DRAFT_22,
                                      MOQ_LOC_PROFILE_04, &m, &p) == MOQ_OK);
        return p;
    }
    }
}

static void test_raw_decode_order_is_object_order(void)
{
    static const uint64_t pts_ms[4] = { 0, 99, 33, 66 };
    static const uint64_t arrival[4] = { 0, 2, 1, 3 };
    static const moq_version_t version[4] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
        MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_22 };
    for (int mode = 0; mode < 4; mode++) {
        int64_t before = g_balance;
        moq_playback_t *pb = new_pb(2, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW, version[mode], 0);
        uint64_t object[4], dts[4], pts[4];
        uint8_t byte[4];
        size_t decodes = 0, drops = 0;
        for (int i = 0; i < 4; i++) {
            uint64_t o = arrival[i];
            const uint8_t frame[5] = { 0, 0, 1, 0x65, (uint8_t)(0x10 + o) };
            moq_rcbuf_t *payload = rc_of(frame, sizeof(frame));
            moq_rcbuf_t *props = raw_order_props(mode, pts_ms[o], o == 0);
            if (mode == 0) {
                CHECK(push_obj(pb, t, 1, o, payload, props) == MOQ_OK);
            } else {
                moq_media_parsed_object_t parsed;
                CHECK(parse_obj(version[mode], MOQ_MEDIA_PACKAGING_RAW, 0, 1, o,
                                payload, props, NULL, 0, &parsed) == MOQ_OK);
                moq_playback_parsed_input_t in = parsed_in(t, 1, o, &parsed, payload);
                CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
            }
            moq_rcbuf_decref(props);
            moq_rcbuf_decref(payload);
            for (int round = 0; round < 8; round++) {
                moq_result_t rc = moq_playback_tick(pb, 0);
                CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
                moq_playback_cmd_t c;
                while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
                    if (c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO && decodes < 4) {
                        object[decodes] = c.u.decode_video.object_id;
                        dts[decodes] = c.u.decode_video.decode_time_us;
                        pts[decodes] = c.u.decode_video.presentation_time_us;
                        byte[decodes] = moq_rcbuf_len(c.u.decode_video.payload) == 5
                            ? moq_rcbuf_data(c.u.decode_video.payload)[4] : 0;
                        decodes++;
                    }
                    moq_playback_cmd_cleanup(&c);
                }
                moq_playback_event_t e;
                while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK)
                    if (e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED) drops++;
                if (rc == MOQ_OK) break;
            }
            /* Object 2 arrived before 1: nothing past object 0 goes out
             * until 1 is in. */
            if (i == 1) CHECK(decodes == 1);
        }
        CHECK(decodes == 4 && drops == 0);
        uint64_t base = mode == 3 ? 0 : T0;
        for (size_t k = 0; k < decodes && k < 4; k++) {
            CHECK(object[k] == k);
            CHECK(byte[k] == 0x10 + k);
            CHECK(pts[k] == base + pts_ms[k] * 1000);
            CHECK(dts[k] == pts[k]);
        }
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);
        moq_playback_destroy(pb);
        CHECK(g_balance == before);
    }
}

/* A CMAF decode clock that never decreases with presentation times that do
 * (composition offsets): every sample decodes in decode order, keeping its
 * offset, and nothing is dropped. */
static void test_cmaf_monotonic_dts_reordered_pts(void)
{
    moq_playback_t *pb = new_pb(16, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                       MOQ_VERSION_DRAFT_16, 1000);
    /* Decode at 0, 10, 20, 30 ms; present at 0, 30, 10, 20 ms. */
    static const int64_t cto_ms[4] = { 0, 20, -10, -10 };
    for (uint64_t o = 0; o < 4; o++) {
        uint8_t frag[1024];
        size_t flen = cmaf_chunk_cto(frag, o * 10, 10, cto_ms[o], o == 0, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_cmaf_sample_t samples[16];
        moq_media_parsed_object_t parsed;
        CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 1000,
                        1, o, fp, NULL, samples, 16, &parsed) == MOQ_OK);
        moq_playback_parsed_input_t in = parsed_in(t, 1, o, &parsed, fp);
        in.timescale = 1000;
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        moq_rcbuf_decref(fp);
    }
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    size_t n = 0;
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF) {
            CHECK(c.u.decode_cmaf.object_id == n);
            CHECK(c.u.decode_cmaf.decode_time_us == n * 10000);
            CHECK(c.u.decode_cmaf.presentation_time_us ==
                  (uint64_t)((int64_t)(n * 10) + cto_ms[n]) * 1000);
            n++;
        }
        moq_playback_cmd_cleanup(&c);
    }
    CHECK(n == 4);
    moq_playback_event_t e;
    while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK)
        CHECK(e.kind != MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
    moq_playback_destroy(pb);
}

/* -- In-band decoder configuration ------------------------------------ */

static const uint8_t CFG_A[5] = { 0xA1, 1, 2, 3, 4 };
static const uint8_t CFG_B[3] = { 0xB2, 5, 6 };

/* A draft-22 object's effective video configuration is kept with the
 * object and governs the CONFIGURE before it, on both push paths, after
 * its property bytes are gone. The copy is charged with the payload. */
static void test_object_config_initial(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                           MOQ_VERSION_DRAFT_22, 0);
        CHECK(push_d22(pb, t, via_parsed, 1, 0,
              d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 4 + sizeof(CFG_A));
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        memset(&q, 0, sizeof(q));
        drain_seq(pb, &q);
        CHECK(q.n == 2 && q.kind[0] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        CHECK(q.cfg_len[0] == sizeof(CFG_A) && q.cfg0[0] == 0xA1);
        /* The payload is gone; the configuration copy stays charged while
         * the track keeps it as its current configuration. */
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_A));
        moq_playback_destroy(pb);
    }
}

/* Configuration changes take effect in release order: a CONFIGURE precedes
 * exactly the first unit that carries a different configuration, an
 * unchanged configuration emits nothing, a present-but-empty configuration
 * is a change to empty, and an absent one changes nothing. Objects pushed
 * out of order are configured in release order. */
static void test_object_config_changes_in_order(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                           MOQ_VERSION_DRAFT_22, 0);
        /* Arrival order 2, 0, 4, 1, 3. */
        CHECK(push_d22(pb, t, via_parsed, 1, 2,
              d22_props(T0 + 2, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
        CHECK(push_d22(pb, t, via_parsed, 1, 0,
              d22_props(T0 + 0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        CHECK(push_d22(pb, t, via_parsed, 1, 4,
              d22_props(T0 + 4, false, false, NULL, 0, false)) == MOQ_OK);
        CHECK(push_d22(pb, t, via_parsed, 1, 1,
              d22_props(T0 + 1, false, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        CHECK(push_d22(pb, t, via_parsed, 1, 3,
              d22_props(T0 + 3, false, false, NULL, 0, true)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        seq_t q;
        memset(&q, 0, sizeof(q));
        drain_seq(pb, &q);
        /* Configure A, decode 0 and 1, configure B, decode 2, configure
         * empty, decode 3 and 4. */
        CHECK(q.n == 8);
        CHECK(q.kind[0] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO && q.cfg0[0] == 0xA1);
        CHECK(q.kind[1] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[1] == 0);
        CHECK(q.kind[2] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[2] == 1);
        CHECK(q.kind[3] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO && q.cfg0[3] == 0xB2);
        CHECK(q.kind[4] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[4] == 2);
        CHECK(q.kind[5] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO && q.cfg_null[5]);
        CHECK(q.kind[6] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[6] == 3);
        CHECK(q.kind[7] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[7] == 4);
        moq_playback_destroy(pb);
    }
}

/* Audio uses the effective audio configuration the same way; with the
 * track's catalog configuration equal to the object's, no second CONFIGURE
 * is emitted, and an object configuration replaces the catalog one. */
static void test_object_config_audio_and_catalog(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = MOQ_VERSION_DRAFT_22;
    tc.init_data = (moq_bytes_t){ CFG_A, sizeof(CFG_A) };
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, false, true, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1,
          d22_props(T0 + 1, false, true, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    seq_t q;
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    /* Configure A, decode object 0, configure B, decode object 1. */
    CHECK(q.n == 4);
    CHECK(q.kind[0] == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO && q.cfg0[0] == 0xA1);
    CHECK(q.kind[1] == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
    CHECK(q.kind[2] == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO && q.cfg0[2] == 0xB2);
    CHECK(q.kind[3] == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
    moq_playback_destroy(pb);
}

/* The configuration copy is bounded and its allocation can fail: a
 * configuration larger than max_track_config_bytes is MOQ_ERR_LIMIT, and a
 * failed copy is MOQ_ERR_NOMEM; neither retains anything. */
static void test_object_config_bounds(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_track_config_bytes = 4;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_ERR_LIMIT);
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    moq_rcbuf_t *props = d22_props(T0, true, false, CFG_B, sizeof(CFG_B), true);
    static const uint8_t frame[4] = { 0, 0, 1, 0x65 };
    moq_rcbuf_t *payload = rc_of(frame, sizeof(frame));
    moq_media_parsed_object_t parsed;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 0,
                    payload, props, NULL, 0, &parsed) == MOQ_OK);
    moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, payload);
    int64_t before = g_balance;
    g_fail_at = g_allocs + 1;
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_NOMEM);
    g_fail_at = 0;
    CHECK(g_balance == before);
    CHECK(moq_rcbuf_refcount(payload) == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 4 + sizeof(CFG_B));
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* -- One-slot command ring -------------------------------------------- */

/* The smallest accepted command ring still makes progress: CONFIGURE is
 * committed on its own, then one sample per drain, on both input paths,
 * with exact sample times; a RAW object likewise. */
static void test_one_slot_ring(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(1, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 3);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 0, 1, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        if (via_parsed) {
            moq_cmaf_sample_t samples[16];
            moq_media_parsed_object_t parsed;
            CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 3,
                            1, 0, fp, NULL, samples, 16, &parsed) == MOQ_OK);
            moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, fp);
            in.timescale = 3;
            CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
            memset(samples, 0xEE, sizeof(samples));
            memset(&parsed, 0xEE, sizeof(parsed));
        } else {
            CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
        }
        uint64_t reads = moq_playback_test_fragment_reads(pb);
        cmd_log_t cl;
        memset(&cl, 0, sizeof(cl));
        for (int i = 0; i < 8 && cl.decodes < 3; i++) {
            moq_result_t rc = moq_playback_tick(pb, 0);
            CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
            drain_cmds(pb, &cl, true);
        }
        CHECK(cl.configures == 1 && cl.decodes == 3);
        CHECK(cl.dts[0] == 0 && cl.dts[1] == 333333 && cl.dts[2] == 666666);
        /* Release reads the owned table: no fragment is parsed again. */
        CHECK(moq_playback_test_fragment_reads(pb) == reads);
        CHECK(moq_rcbuf_refcount(fp) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }

    moq_playback_t *pb = new_pb(1, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_16, 0);
    static const uint8_t d[2] = { 1, 2 };
    moq_rcbuf_t *payload = rc_of(d, sizeof(d));
    moq_rcbuf_t *props = loc01(MOQ_VERSION_DRAFT_16, T0, true);
    CHECK(push_obj(pb, t, 1, 0, payload, props) == MOQ_OK);
    cmd_log_t cl;
    memset(&cl, 0, sizeof(cl));
    for (int i = 0; i < 4 && cl.decodes < 1; i++) {
        (void)moq_playback_tick(pb, 0);
        drain_cmds(pb, &cl, true);
    }
    CHECK(cl.configures == 1 && cl.decodes == 1 && cl.dts[0] == T0);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* A decode error between the committed CONFIGURE and the first decode, and
 * again between two samples, on a one-slot ring: the purged commands are
 * re-emitted from the start of the object behind the reset. */
static void test_one_slot_ring_decode_error(void)
{
    moq_playback_t *pb = new_pb(1, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                       MOQ_VERSION_DRAFT_16, 1000);
    uint8_t frag[1024];
    size_t flen = cmaf_chunk(frag, 0, 10, 3, 4);
    moq_rcbuf_t *fp = rc_of(frag, flen);
    CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;

    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);   /* CONFIGURE */
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    seq_t q;
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    CHECK(q.n == 1 && q.kind[0] == MOQ_PLAYBACK_CMD_RESET);

    cmd_log_t cl;
    memset(&cl, 0, sizeof(cl));
    for (int i = 0; i < 3; i++) {                 /* CONFIGURE, samples 0 and 1 */
        (void)moq_playback_tick(pb, 0);
        drain_cmds(pb, &cl, true);
    }
    CHECK(cl.configures == 1 && cl.decodes == 2);
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    CHECK(q.n == 1 && q.kind[0] == MOQ_PLAYBACK_CMD_RESET);
    memset(&cl, 0, sizeof(cl));
    for (int i = 0; i < 6 && cl.decodes < 3; i++) {
        (void)moq_playback_tick(pb, 0);
        drain_cmds(pb, &cl, true);
    }
    CHECK(cl.configures == 1 && cl.decodes == 3);
    CHECK(cl.dts[0] == 0 && cl.dts[1] == 10000 && cl.dts[2] == 20000);
    CHECK(moq_rcbuf_refcount(fp) == 1);
    moq_rcbuf_decref(fp);
    moq_playback_destroy(pb);
}

/* -- CMAF timing context ---------------------------------------------- */

/* push_parsed admits a CMAF record only when it was produced with the
 * track's timescale: a track without one refuses it, a record parsed at
 * another scale is refused, and a matching record releases exactly the
 * record's decode time. Refusals change no ref, buffer or event. */
static void test_parsed_cmaf_timing_context(void)
{
    static const uint32_t track_scale[3] = { 0, 1, 2 };
    for (size_t i = 0; i < 3; i++) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16,
                                           track_scale[i]);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 2, 1, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_cmaf_sample_t samples[16];
        moq_media_parsed_object_t parsed;
        CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 2, 1, 0,
                        fp, NULL, samples, 16, &parsed) == MOQ_OK);
        CHECK(parsed.decode_time_us == 1000000);
        moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, fp);
        in.timescale = 2;
        if (track_scale[i] == 2) {
            /* A record whose sample table is not this payload's. */
            moq_cmaf_sample_t forged[16];
            memcpy(forged, samples, sizeof(forged));
            forged[1].size += 1;
            moq_media_parsed_object_t other = parsed;
            other.samples = forged;
            in.parsed = &other;
            CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
            CHECK(moq_rcbuf_refcount(fp) == 1);
            in.parsed = &parsed;
        }
        moq_result_t rc = moq_playback_push_parsed(pb, &in, 0);
        if (track_scale[i] != 2) {
            CHECK(rc == MOQ_ERR_INVAL);
            CHECK(moq_rcbuf_refcount(fp) == 1);
            CHECK(moq_playback_test_buffered_count(pb) == 0);
            moq_playback_event_t e;
            CHECK(moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_DONE);
        } else {
            CHECK(rc == MOQ_OK);
            CHECK(moq_playback_test_retained_bytes(pb) ==
                  flen + 3 * sizeof(moq_cmaf_sample_t));
            CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
            cmd_log_t cl;
            drain_cmds(pb, &cl, false);
            CHECK(cl.decodes == 3);
            CHECK(cl.dts[0] == 1000000 && cl.dts[1] == 1500000 &&
                  cl.dts[2] == 2000000);
        }
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* -- Explicit CMAF timing context ------------------------------------ */

/* Parse a CMAF fragment at `scale` and build the parsed input, naming the
 * scale the caller parsed with. */
static moq_playback_parsed_input_t cmaf_in(moq_playback_track_t t,
                                           moq_rcbuf_t *fp, uint32_t scale,
                                           moq_cmaf_sample_t *samples,
                                           moq_media_parsed_object_t *parsed)
{
    CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, scale,
                    1, 0, fp, NULL, samples, 16, parsed) == MOQ_OK);
    moq_playback_parsed_input_t in = parsed_in(t, 1, 0, parsed, fp);
    in.timescale = scale;
    return in;
}

/* The parsed input names the timescale it was parsed with, and it must be
 * the track's. A base tick of zero makes the decode time identical at
 * every scale, so only the explicit context (and the per-sample duration
 * it implies) tells a record parsed at 2 from one at 1. Refusals change
 * nothing; matching records release exact times on single- and
 * multi-sample objects alike. */
static void test_parsed_timescale_is_explicit(void)
{
    uint8_t frag[1024];
    moq_cmaf_sample_t samples[16];
    moq_media_parsed_object_t parsed;

    /* Base 0, parsed at 2, offered to a scale-1 track: refused whether the
     * caller names its real scale or claims the track's. */
    for (int claim_track = 0; claim_track < 2; claim_track++) {
        moq_playback_t *pb = new_pb(8, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 1);
        size_t flen = cmaf_chunk(frag, 0, 1, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_playback_parsed_input_t in = cmaf_in(t, fp, 2, samples, &parsed);
        if (claim_track) in.timescale = 1;
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(fp) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        moq_playback_event_t e;
        CHECK(moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_DONE);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }

    /* A CMAF input naming no timescale has no context and is refused. */
    {
        moq_playback_t *pb = new_pb(8, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 2);
        size_t flen = cmaf_chunk(frag, 0, 1, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_playback_parsed_input_t in = cmaf_in(t, fp, 2, samples, &parsed);
        in.timescale = 0;
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(fp) == 1);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }

    /* Matching context: multi-sample at 2 from base 0, and a single-sample
     * object at 3 (fractional conversion). */
    {
        moq_playback_t *pb = new_pb(8, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 2);
        size_t flen = cmaf_chunk(frag, 0, 1, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_playback_parsed_input_t in = cmaf_in(t, fp, 2, samples, &parsed);
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t c;
        size_t n = 0;
        while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
            if (c.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF) {
                CHECK(c.u.decode_cmaf.decode_time_us == n * UINT64_C(500000));
                CHECK(c.u.decode_cmaf.sample_duration_us == 500000);
                n++;
            }
            moq_playback_cmd_cleanup(&c);
        }
        CHECK(n == 3);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
    {
        moq_playback_t *pb = new_pb(8, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 3);
        size_t flen = cmaf_chunk(frag, 1, 1, 1, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_playback_parsed_input_t in = cmaf_in(t, fp, 3, samples, &parsed);
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl, false);
        CHECK(cl.decodes == 1 && cl.dts[0] == 333333);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* The context check precedes the sample-count disposition: an over-cap
 * record on a track without a timescale is INVAL with no drop event; with
 * a matching context the over-cap record is dropped as before. */
static void test_context_precedes_over_cap_drop(void)
{
    for (int matching = 0; matching < 2; matching++) {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_samples_per_object = 2;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16,
                                           matching ? 2 : 0);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 2, 1, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_cmaf_sample_t samples[16];
        moq_media_parsed_object_t parsed;
        moq_playback_parsed_input_t in = cmaf_in(t, fp, 2, samples, &parsed);
        moq_result_t rc = moq_playback_push_parsed(pb, &in, 0);
        size_t drops = count_drops(pb, MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE);
        if (matching) {
            CHECK(rc == MOQ_OK && drops == 1);
        } else {
            CHECK(rc == MOQ_ERR_INVAL && drops == 0);
        }
        CHECK(moq_rcbuf_refcount(fp) == 1);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* -- CMAF media range -------------------------------------------------- */

/* The record's media range must be the fragment's: a shortened or shifted
 * range, or one whose samples overrun it, is refused before any effect. */
static void test_parsed_media_range(void)
{
    for (int variant = 0; variant < 3; variant++) {
        moq_playback_t *pb = new_pb(8, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 1000);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 0, 10, 3, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_cmaf_sample_t samples[16];
        moq_media_parsed_object_t parsed;
        moq_playback_parsed_input_t in = cmaf_in(t, fp, 1000, samples, &parsed);
        if (variant == 0) parsed.mdat_len = 1;
        else if (variant == 1) parsed.mdat_offset = 0;
        else parsed.mdat_offset += 1;      /* still inside the payload */
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(fp) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* A sample table that overruns its mdat is refused. The fragment index
 * reader accepts such a table, so the checks are the engine's own:
 * push_object's parser drops the object MALFORMED_CMAF, and a parsed
 * record describing that payload exactly (its sample table and its 8-byte
 * mdat) is refused at admission, its samples needing 12 bytes of media. */
static void test_samples_overrunning_mdat(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(8, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 1000);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 0, 10, 3, 4);  /* 12 media bytes */
        size_t mdat = flen - 12 - 8;
        wr32(frag + mdat, 8 + 8);                     /* mdat holds only 8 */
        flen -= 4;
        moq_rcbuf_t *fp = rc_of(frag, flen);
        if (via_parsed) {
            uint8_t good[1024];
            size_t glen = cmaf_chunk(good, 0, 10, 3, 4);
            moq_rcbuf_t *gp = rc_of(good, glen);
            moq_cmaf_sample_t samples[16];
            moq_media_parsed_object_t parsed;
            moq_playback_parsed_input_t in = cmaf_in(t, gp, 1000, samples,
                                                     &parsed);
            parsed.mdat_len = 8;           /* the overrun payload's mdat */
            in.payload = fp;
            CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
            CHECK(moq_playback_test_buffered_count(pb) == 0);
            moq_rcbuf_decref(gp);
        } else {
            CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
            CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_MALFORMED_CMAF) == 1);
            CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
            cmd_log_t cl;
            drain_cmds(pb, &cl, false);
            CHECK(cl.decodes == 0);
        }
        CHECK(moq_rcbuf_refcount(fp) == 1);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* After a decode error, an object without its own configuration is
 * configured with the one released media established (B, from the
 * previous object), not the catalog one (A). */
static void test_reset_restores_release_configuration(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = MOQ_VERSION_DRAFT_22;
    tc.init_data = (moq_bytes_t){ CFG_A, sizeof(CFG_A) };
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    seq_t q;
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1,
          d22_props(T0 + 1, true, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    /* RESET, CONFIGURE(B), decode */
    CHECK(q.n == 3);
    CHECK(q.kind[1] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO && q.cfg0[1] == 0xB2);
    CHECK(q.kind[2] == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
    moq_playback_destroy(pb);
}

/* -- Release-order configuration -------------------------------------- */

/* CONFIGURE for object 2 (config B) is committed and drained before its
 * decode; then an earlier independent object 0 without its own config
 * becomes the entry. It decodes under the catalog configuration A, which
 * is restored first; object 2 then gets B again. Also with a present-empty
 * configuration standing in for A, and on both push paths. */
static void test_configure_then_reanchor(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        for (int empty_base = 0; empty_base < 2; empty_base++) {
            moq_playback_t *pb = new_pb(1, 0);
            moq_playback_track_cfg_t tc;
            moq_playback_track_cfg_init(&tc);
            tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
            tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
            tc.transport_version = MOQ_VERSION_DRAFT_22;
            if (!empty_base) tc.init_data = (moq_bytes_t){ CFG_A, sizeof(CFG_A) };
            moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
            CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
            CHECK(push_d22(pb, t, via_parsed, 1, 2,
                  d22_props(T0 + 2, true, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
            (void)moq_playback_tick(pb, 0);
            seq_t q;
            memset(&q, 0, sizeof(q));
            drain_seq(pb, &q);
            CHECK(q.n == 1 && q.kind[0] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO &&
                  q.cfg0[0] == 0xB2);
            /* Object 0: absent config (catalog A), or present-empty. */
            CHECK(push_d22(pb, t, via_parsed, 1, 0,
                  d22_props(T0, true, false, NULL, 0, empty_base != 0)) == MOQ_OK);
            CHECK(push_d22(pb, t, via_parsed, 1, 1,
                  d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
            memset(&q, 0, sizeof(q));
            for (int i = 0; i < 8; i++) {
                (void)moq_playback_tick(pb, 0);
                drain_seq(pb, &q);
            }
            /* CONFIGURE(base), decode 0, decode 1, CONFIGURE(B), decode 2 */
            CHECK(q.n == 5);
            CHECK(q.kind[0] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
            if (empty_base) CHECK(q.cfg_null[0]);
            else CHECK(q.cfg0[0] == 0xA1);
            CHECK(q.kind[1] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[1] == 0);
            CHECK(q.kind[2] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[2] == 1);
            CHECK(q.kind[3] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO && q.cfg0[3] == 0xB2);
            CHECK(q.kind[4] == MOQ_PLAYBACK_CMD_DECODE_VIDEO && q.object[4] == 2);
            moq_playback_destroy(pb);
        }
    }
}

/* A decode error after CONFIGURE was drained but before the first decode:
 * the decoder lost it, so the same configuration is emitted again before
 * that decode. */
static void test_decode_error_between_configure_and_decode(void)
{
    moq_playback_t *pb = new_pb(1, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
    (void)moq_playback_tick(pb, 0);
    seq_t q;
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    CHECK(q.n == 1 && q.kind[0] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    memset(&q, 0, sizeof(q));
    for (int i = 0; i < 4; i++) {
        (void)moq_playback_tick(pb, 0);
        drain_seq(pb, &q);
    }
    /* RESET, CONFIGURE(B), decode 0 */
    CHECK(q.n == 3);
    CHECK(q.kind[0] == MOQ_PLAYBACK_CMD_RESET);
    CHECK(q.kind[1] == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO && q.cfg0[1] == 0xB2);
    CHECK(q.kind[2] == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
    moq_playback_destroy(pb);
}

/* -- Configuration charge --------------------------------------------- */

/* A configuration copy stays charged while the engine keeps it, after its
 * object has gone; an identical configuration reuses it at no extra charge;
 * a replacement that fits beside it progresses and frees the old one; and
 * with nothing buffered, an object that cannot fit beside it is
 * MOQ_ERR_LIMIT (nothing the engine does could make room), not a retry
 * forever. Removing the track returns the charge. */
static void test_config_charge_follows_the_allocation(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(8, 13);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                           MOQ_VERSION_DRAFT_22, 0);
        seq_t q;
        CHECK(push_d22(pb, t, via_parsed, 1, 0,
              d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 4 + sizeof(CFG_A));
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        memset(&q, 0, sizeof(q));
        drain_seq(pb, &q);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_A));

        /* Identical config: reused, only the payload is charged. */
        CHECK(push_d22(pb, t, via_parsed, 1, 1,
              d22_props(T0 + 1, false, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_A) + 4);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        drain_seq(pb, &q);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_A));

        /* Replacement that fits beside A: admitted, released, A freed. */
        CHECK(push_d22(pb, t, via_parsed, 1, 2,
              d22_props(T0 + 2, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) ==
              sizeof(CFG_A) + 4 + sizeof(CFG_B));
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        drain_seq(pb, &q);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B));

        /* 4 + 9 bytes of config beside B's 3 exceed 13 with nothing
         * buffered: permanent for this object. */
        static const uint8_t CFG_C[9] = { 0xC3, 1, 2, 3, 4, 5, 6, 7, 8 };
        CHECK(push_d22(pb, t, via_parsed, 1, 3,
              d22_props(T0 + 3, false, false, CFG_C, sizeof(CFG_C), true)) == MOQ_ERR_LIMIT);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B));

        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);
        moq_playback_destroy(pb);
    }
}

/* -- Reclaimable configuration under pressure ------------------------- */

/* Track without an entry holds one dependent object (payload 4 + a unique
 * 15-byte config: the whole 19-byte budget). An entry for the same track
 * (payload 4 + config 5) is refused; tick finds that dropping the waiting
 * object frees its config copy too, drops it, and the retry is admitted.
 * Both push paths. */
static void test_relief_reclaims_config_bytes(void)
{
    static const uint8_t big[15] = { 0xD4 };
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(8, 19);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                           MOQ_VERSION_DRAFT_22, 0);
        CHECK(push_d22(pb, t, via_parsed, 1, 1,
              d22_props(T0 + 1, false, false, big, sizeof(big), true)) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 19);
        CHECK(push_d22(pb, t, via_parsed, 1, 0,
              d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) ==
              MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);
        CHECK(push_d22(pb, t, via_parsed, 1, 0,
              d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        moq_playback_destroy(pb);
    }
}

/* Two waiting objects share one config copy (6 bytes, reused). Dropping
 * the first frees only its payload; dropping both frees the copy as well.
 * The 10 bytes an entry on another track needs therefore take both. */
static void test_relief_counts_shared_config_once(void)
{
    static const uint8_t C6[6] = { 0xC6, 1, 2, 3, 4, 5 };
    static const uint8_t ENTRY_CFG[6] = { 0xD6, 1, 2, 3, 4, 5 };
    moq_playback_t *pb = new_pb(8, 14);
    moq_playback_track_t w = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    moq_playback_track_t e = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, w, true, 1, 1,
          d22_props(T0 + 1, false, false, C6, sizeof(C6), true)) == MOQ_OK);
    CHECK(push_d22(pb, w, true, 1, 2,
          d22_props(T0 + 2, false, false, C6, sizeof(C6), true)) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 4 + 4 + 6);
    CHECK(push_d22(pb, e, true, 1, 0,
          d22_props(T0, true, false, ENTRY_CFG, sizeof(ENTRY_CFG), true)) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 2);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    CHECK(push_d22(pb, e, true, 1, 0,
          d22_props(T0, true, false, ENTRY_CFG, sizeof(ENTRY_CFG), true)) == MOQ_OK);
    moq_playback_destroy(pb);
}

/* Not enough can be reclaimed: the waiting object's 9 bytes cannot free the
 * 14 an entry needs while another track's cached configuration (10 bytes,
 * not eligible) stays. Nothing is dropped. */
static void test_relief_insufficient_reclaim_drops_nothing(void)
{
    static const uint8_t T10[10] = { 0x10 };
    moq_playback_t *pb = new_pb(8, 19);
    moq_playback_track_t a = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    moq_playback_track_t w = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    moq_playback_track_t e = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, a, true, 1, 0,
          d22_props(T0, true, false, T10, sizeof(T10), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    seq_t q;
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);                       /* a keeps its 10-byte copy */
    CHECK(moq_playback_test_retained_bytes(pb) == 10);
    CHECK(push_d22(pb, w, true, 1, 1,
          d22_props(T0 + 1, false, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, e, true, 1, 0,
          d22_props(T0, true, false, T10, sizeof(T10), true)) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_KEYFRAME_WAIT) == 0);
    CHECK(moq_playback_test_buffered_count(pb) == 1);
    CHECK(moq_playback_test_retained_bytes(pb) == 19);
    moq_playback_destroy(pb);
}

/* Relief that needs two drops with a one-slot event queue proceeds one drop
 * per drained tick, and the retry is admitted afterwards. */
static void test_relief_with_config_under_event_pressure(void)
{
    static const uint8_t C6[6] = { 0xC6, 1, 2, 3, 4, 5 };
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_buffered_bytes = 14;
    cfg.max_events = 1;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t w = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    moq_playback_track_t e = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, w, true, 1, 1,
          d22_props(T0 + 1, false, false, C6, sizeof(C6), true)) == MOQ_OK);
    CHECK(push_d22(pb, w, true, 1, 2,
          d22_props(T0 + 2, false, false, C6, sizeof(C6), true)) == MOQ_OK);
    CHECK(push_d22(pb, e, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_ERR_WOULD_BLOCK);
    size_t drops = 0;
    for (int i = 0; i < 6 && drops < 2; i++) {
        (void)moq_playback_tick(pb, 0);
        drops += count_drops(pb, MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
    }
    CHECK(drops == 2);
    CHECK(push_d22(pb, e, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    moq_playback_destroy(pb);
}

/* -- Configuration owned by queued commands --------------------------- */

static size_t poll_one(moq_playback_t *pb, uint32_t *kind)
{
    moq_playback_cmd_t c;
    if (moq_playback_poll_command(pb, &c, sizeof(c)) != MOQ_OK) return 0;
    *kind = c.kind;
    moq_playback_cmd_cleanup(&c);
    return 1;
}

/* Undrained configuration changes: CONFIGURE(A) and CONFIGURE(B) both sit
 * in the command ring, so both copies stay charged, as do the two queued
 * units' payloads. Polling CONFIGURE(A) hands the engine's last ref to the
 * app and ends A's charge; polling a unit ends its own; B stays charged by
 * the track. Both push paths. */
static void test_queued_configure_keeps_charge(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(8, 32);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                           MOQ_VERSION_DRAFT_22, 0);
        CHECK(push_d22(pb, t, via_parsed, 1, 0,
              d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        CHECK(push_d22(pb, t, via_parsed, 1, 1,
              d22_props(T0 + 1, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) ==
              sizeof(CFG_A) + sizeof(CFG_B) + 2 * UNIT);
        uint32_t kind = 0;
        CHECK(poll_one(pb, &kind) && kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B) + 2 * UNIT);
        CHECK(poll_one(pb, &kind) && kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B) + UNIT);
        while (poll_one(pb, &kind)) {}
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B));
        moq_playback_destroy(pb);
    }
}

/* A configuration identical to one only a queued CONFIGURE still owns is
 * reused rather than copied again, and is charged once. */
static void test_queued_configure_reuse(void)
{
    moq_playback_t *pb = new_pb(8, 32);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1,
          d22_props(T0 + 1, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);       /* ring: CFG(A) .. CFG(B) */
    CHECK(push_d22(pb, t, true, 1, 2,
          d22_props(T0 + 2, false, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    /* A is charged once; the two queued units and the buffered one each
     * carry their payload. */
    CHECK(moq_playback_test_retained_bytes(pb) ==
          sizeof(CFG_A) + sizeof(CFG_B) + 3 * UNIT);
    moq_playback_destroy(pb);
}

/* Purge on decode error, track removal before the ring is drained, and
 * engine destruction all end the queued commands' charges exactly once; a
 * catalog configuration is never charged, in the ring or after its track
 * is gone. */
static void test_queued_configure_lifetimes(void)
{
    moq_playback_t *pb = new_pb(8, 32);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1,
          d22_props(T0 + 1, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B));  /* A purged */
    uint32_t kind;
    while (poll_one(pb, &kind)) {}
    moq_playback_destroy(pb);

    /* Removal before drainage. */
    pb = new_pb(8, 32);
    t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW, MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_A) + UNIT);  /* ring */
    while (poll_one(pb, &kind)) {}
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    moq_playback_destroy(pb);

    /* Catalog configuration: never charged, also after removal. */
    pb = new_pb(8, 32);
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = MOQ_VERSION_DRAFT_22;
    tc.init_data = (moq_bytes_t){ CFG_A, sizeof(CFG_A) };
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT);           /* the unit */
    CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT);
    while (poll_one(pb, &kind)) {}
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    moq_playback_destroy(pb);

    /* Destruction with queued copies returns everything (balance check at
     * the end of the run). */
    pb = new_pb(8, 32);
    t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW, MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_destroy(pb);
}

/* Near the budget boundary with nothing buffered: bytes owned only by
 * queued commands (a CONFIGURE's copy, units' payloads) can be freed by
 * polling, so they make an object WOULD_BLOCK, not LIMIT, and after polling
 * it is admitted. */
static void test_queued_config_pressure_is_transient(void)
{
    moq_playback_t *pb = new_pb(8, 20);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1,
          d22_props(T0 + 1, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    /* Retained: A (ring only, 5) + B (track and ring, 3) + two queued
     * units = 16 of 20. */
    CHECK(moq_playback_test_retained_bytes(pb) ==
          sizeof(CFG_A) + sizeof(CFG_B) + 2 * UNIT);
    static const uint8_t C5[5] = { 0xC5, 1, 2, 3, 4 };
    CHECK(push_d22(pb, t, true, 1, 2,
          d22_props(T0 + 2, false, false, C5, sizeof(C5), true)) ==
          MOQ_ERR_WOULD_BLOCK);
    uint32_t kind;
    while (poll_one(pb, &kind)) {}
    CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B));
    CHECK(push_d22(pb, t, true, 1, 2,
          d22_props(T0 + 2, false, false, C5, sizeof(C5), true)) == MOQ_OK);
    moq_playback_destroy(pb);
}

/* -- Units owned by queued commands ----------------------------------- */

static moq_playback_t *new_pb_held(uint32_t max_commands, uint64_t max_bytes,
                                   uint32_t max_held)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    if (max_commands) cfg.max_commands = max_commands;
    if (max_bytes) cfg.max_buffered_bytes = max_bytes;
    cfg.max_held_commands = max_held;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

/* Held polls into `c[]`/`h[]` until one is refused; how many were taken. */
static size_t poll_held_all(moq_playback_t *pb, moq_playback_cmd_t *c,
                            moq_playback_command_hold_t *h, size_t cap)
{
    size_t n = 0;
    while (n < cap && moq_playback_poll_command_held(pb, &c[n], sizeof(c[n]), &h[n]) == MOQ_OK)
        n++;
    return n;
}

/* The oracle: bytes the held commands themselves reference, each backing
 * buffer counted once (a shared configuration copy once). */
static uint64_t referenced_bytes(const moq_playback_cmd_t *c, size_t n)
{
    const moq_rcbuf_t *seen[32];
    size_t ns = 0;
    uint64_t total = 0;
    for (size_t i = 0; i < n; i++) {
        const moq_rcbuf_t *b = NULL;
        if (c[i].kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO) b = c[i].u.configure_video.codec_config;
        else if (c[i].kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO) b = c[i].u.configure_audio.codec_config;
        else if (c[i].kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) b = c[i].u.decode_video.payload;
        else if (c[i].kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO) b = c[i].u.decode_audio.payload;
        if (!b) continue;
        bool dup = false;
        for (size_t k = 0; k < ns; k++) dup = dup || seen[k] == b;
        if (dup) continue;
        if (ns < 32) seen[ns++] = b;
        total += moq_rcbuf_len(b);
    }
    return total;
}

static void cleanup_all(moq_playback_cmd_t *c, size_t n)
{
    for (size_t i = 0; i < n; i++) moq_playback_cmd_cleanup(&c[i]);
}

/* A held poll keeps the command's bytes charged until its hold is
 * released: the budget stays full and the empty engine still refuses with
 * WOULD_BLOCK; releasing one unit's hold makes room for one unit. The hold
 * observer counts what the held commands reference, the configuration
 * copy once although the track holds it too. Releasing twice, through a
 * copy of the handle, or an invalid handle changes nothing. */
static void test_command_holds(void)
{
    moq_playback_t *pb = new_pb_held(8, 2 * UNIT + sizeof(CFG_A), 8);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t c[8];
    moq_playback_command_hold_t h[8];
    size_t n = poll_held_all(pb, c, h, 8);
    CHECK(n == 3);
    for (size_t i = 0; i < n; i++) {
        CHECK(moq_playback_command_hold_is_valid(h[i]));
        for (size_t k = 0; k < i; k++) CHECK(!moq_playback_command_hold_eq(h[i], h[k]));
    }
    CHECK(moq_playback_command_hold_bytes(pb) == referenced_bytes(c, n));
    CHECK(moq_playback_command_hold_bytes(pb) == 2 * UNIT + sizeof(CFG_A));
    CHECK(moq_playback_test_retained_bytes(pb) == 2 * UNIT + sizeof(CFG_A));
    CHECK(push_d22(pb, t, true, 1, 2, d22_props(T0 + 2, false, false, NULL, 0, false)) ==
          MOQ_ERR_WOULD_BLOCK);
    moq_playback_command_hold_t copy = h[1];
    moq_playback_release_command_hold(pb, copy);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT + sizeof(CFG_A));
    moq_playback_release_command_hold(pb, h[1]);
    moq_playback_release_command_hold(pb, MOQ_PLAYBACK_COMMAND_HOLD_INVALID);
    moq_playback_release_command_hold(NULL, h[2]);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT + sizeof(CFG_A));
    CHECK(moq_playback_command_hold_bytes(pb) == UNIT + sizeof(CFG_A));
    CHECK(push_d22(pb, t, true, 1, 2, d22_props(T0 + 2, false, false, NULL, 0, false)) == MOQ_OK);
    /* Release before the command's cleanup, and cleanup before release. */
    moq_playback_release_command_hold(pb, h[0]);
    moq_playback_cmd_cleanup(&c[0]);
    moq_playback_cmd_cleanup(&c[2]);
    moq_playback_release_command_hold(pb, h[2]);
    moq_playback_cmd_cleanup(&c[1]);
    CHECK(moq_playback_command_hold_bytes(pb) == 0);
    CHECK(moq_playback_test_retained_bytes(pb) == UNIT + sizeof(CFG_A));
    moq_playback_destroy(pb);
}

/* A held CONFIGURE keeps its copy charged through a replacement, in either
 * order: replaced while queued, then polled; or polled, then replaced. The
 * charge leaves with the hold's release. */
static void test_command_hold_config_replacement(void)
{
    for (int poll_first = 0; poll_first < 2; poll_first++) {
        moq_playback_t *pb = new_pb_held(8, 64, 8);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                           MOQ_VERSION_DRAFT_22, 0);
        CHECK(push_d22(pb, t, true, 1, 0,
              d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
        moq_playback_cmd_t c[8];
        moq_playback_command_hold_t h[8];
        size_t n = 0;
        if (poll_first) {
            CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
            n = poll_held_all(pb, c, h, 8);            /* CONFIGURE(A), unit 0 */
            CHECK(n == 2);
        }
        CHECK(push_d22(pb, t, true, 1, 1,
              d22_props(T0 + 1, false, false, CFG_B, sizeof(CFG_B), true)) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        n += poll_held_all(pb, c + n, h + n, 8 - n);
        CHECK(n == 4);                                 /* + CONFIGURE(B), unit 1 */
        CHECK(c[0].kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        /* A is held by its CONFIGURE's hold only; B by the track too. */
        CHECK(moq_playback_command_hold_bytes(pb) == referenced_bytes(c, n));
        CHECK(moq_playback_test_retained_bytes(pb) ==
              sizeof(CFG_A) + sizeof(CFG_B) + 2 * UNIT);
        moq_playback_cmd_cleanup(&c[0]);
        CHECK(moq_playback_test_retained_bytes(pb) ==
              sizeof(CFG_A) + sizeof(CFG_B) + 2 * UNIT);
        moq_playback_release_command_hold(pb, h[0]);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B) + 2 * UNIT);
        for (size_t i = 1; i < n; i++) moq_playback_release_command_hold(pb, h[i]);
        CHECK(moq_playback_test_retained_bytes(pb) == sizeof(CFG_B));
        CHECK(moq_playback_command_hold_bytes(pb) == 0);
        cleanup_all(c + 1, n - 1);
        moq_playback_destroy(pb);
    }
}

/* Two held CONFIGUREs of one copy (the second re-emitted for the next
 * keyframe after a decode error) count it once; releasing one leaves it held by the other. A
 * decode-error purge of queued commands leaves held ones alone. */
static void test_command_hold_sharing(void)
{
    moq_playback_t *pb = new_pb_held(8, 64, 8);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t c[8];
    moq_playback_command_hold_t h[8];
    CHECK(moq_playback_poll_command_held(pb, &c[0], sizeof(c[0]), &h[0]) == MOQ_OK);
    CHECK(c[0].kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
    uint64_t before = moq_playback_command_hold_bytes(pb);
    CHECK(before == sizeof(CFG_A));
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);   /* purges the units */
    CHECK(moq_playback_command_hold_bytes(pb) == before);
    CHECK(push_d22(pb, t, true, 1, 2, d22_props(T0 + 2, true, false, NULL, 0, false)) == MOQ_OK);
    for (int i = 0; i < 4; i++) (void)moq_playback_tick(pb, 0);
    size_t n = 1 + poll_held_all(pb, c + 1, h + 1, 7);
    size_t configs = 0;
    for (size_t i = 0; i < n; i++) configs += c[i].kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO;
    CHECK(configs == 2);
    CHECK(moq_playback_command_hold_bytes(pb) == referenced_bytes(c, n));
    moq_playback_release_command_hold(pb, h[0]);
    CHECK(moq_playback_command_hold_bytes(pb) == referenced_bytes(c + 1, n - 1));
    CHECK(moq_playback_command_hold_bytes(pb) >= sizeof(CFG_A));
    for (size_t i = 1; i < n; i++) moq_playback_release_command_hold(pb, h[i]);
    CHECK(moq_playback_command_hold_bytes(pb) == 0);
    cleanup_all(c, n);
    moq_playback_destroy(pb);
}

/* Result precedence; every refusal dequeues nothing, takes no hold and
 * leaves both outputs untouched. A zero-byte command (RESET) takes a real
 * hold, which counts against the cap. */
static void test_command_hold_refusals(void)
{
    moq_playback_cmd_t c;
    moq_playback_command_hold_t h;
    moq_playback_t *off = new_pb_held(8, 0, 0);
    memset(&c, 0x5A, sizeof(c));
    h._opaque = 0x1234;
    CHECK(moq_playback_poll_command_held(off, &c, sizeof(c), &h) == MOQ_ERR_UNSUPPORTED);
    CHECK(h._opaque == 0x1234 && c.struct_size == 0x5A5A5A5Au);
    moq_playback_destroy(off);

    moq_playback_t *pb = new_pb_held(8, 0, 2);
    CHECK(moq_playback_poll_command_held(NULL, &c, sizeof(c), &h) == MOQ_ERR_INVAL);
    CHECK(moq_playback_poll_command_held(pb, NULL, sizeof(c), &h) == MOQ_ERR_INVAL);
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), NULL) == MOQ_ERR_INVAL);
    CHECK(moq_playback_poll_command_held(pb, &c, MOQ_PLAYBACK_CMD_V0_SIZE - 1, &h) ==
          MOQ_ERR_ABI_MISMATCH);
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_DONE);
    CHECK(h._opaque == 0x1234);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t got[3];
    moq_playback_command_hold_t hh[3];
    CHECK(poll_held_all(pb, got, hh, 3) == 2);           /* the cap */
    memset(&c, 0x5A, sizeof(c));
    h._opaque = 0x1234;
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_ERR_WOULD_BLOCK);
    CHECK(h._opaque == 0x1234 && c.struct_size == 0x5A5A5A5Au);
    moq_playback_release_command_hold(pb, hh[1]);
    CHECK(moq_playback_poll_command_held(pb, &got[2], sizeof(got[2]), &hh[2]) == MOQ_OK);
    CHECK(got[2].kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO && got[2].u.decode_video.object_id == 1);
    /* A RESET carries no bytes and still takes a hold. */
    moq_playback_release_command_hold(pb, hh[0]);
    moq_playback_release_command_hold(pb, hh[2]);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    uint64_t bytes = moq_playback_command_hold_bytes(pb);
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_OK);
    CHECK(c.kind == MOQ_PLAYBACK_CMD_RESET && moq_playback_command_hold_is_valid(h));
    CHECK(moq_playback_command_hold_bytes(pb) == bytes);
    moq_playback_release_command_hold(pb, h);
    cleanup_all(got, 3);
    moq_playback_cmd_cleanup(&c);
    moq_playback_destroy(pb);
}

/* Handles are per engine and per use of a slot: another engine's handle,
 * and an old handle of a reused slot, release nothing. A slot whose
 * generation is spent retires; with no slot left, a held poll is LIMIT,
 * nothing dequeued, and an ordinary poll still takes the command. */
static void test_command_hold_identity(void)
{
    moq_playback_t *a = new_pb_held(8, 0, 1);
    moq_playback_t *b = new_pb_held(8, 0, 1);
    moq_playback_track_t ta = add_track(a, MOQ_PLAYBACK_PACKAGING_RAW, MOQ_VERSION_DRAFT_22, 0);
    moq_playback_track_t tb = add_track(b, MOQ_PLAYBACK_PACKAGING_RAW, MOQ_VERSION_DRAFT_22, 0);
    for (uint64_t o = 0; o < 3; o++) {
        CHECK(push_d22(a, ta, true, 1, o, d22_props(T0 + o, o == 0, false,
              o == 0 ? CFG_A : NULL, o == 0 ? sizeof(CFG_A) : 0, o == 0)) == MOQ_OK);
        CHECK(push_d22(b, tb, true, 1, o, d22_props(T0 + o, o == 0, false,
              o == 0 ? CFG_A : NULL, o == 0 ? sizeof(CFG_A) : 0, o == 0)) == MOQ_OK);
    }
    CHECK(moq_playback_tick(a, 0) == MOQ_OK);
    CHECK(moq_playback_tick(b, 0) == MOQ_OK);
    moq_playback_cmd_t ca[3], cb;
    moq_playback_command_hold_t ha[3], hb;
    CHECK(moq_playback_poll_command_held(a, &ca[0], sizeof(ca[0]), &ha[0]) == MOQ_OK);
    CHECK(moq_playback_poll_command_held(b, &cb, sizeof(cb), &hb) == MOQ_OK);
    uint64_t a_bytes = moq_playback_command_hold_bytes(a);
    moq_playback_release_command_hold(a, hb);              /* foreign */
    CHECK(moq_playback_command_hold_bytes(a) == a_bytes);
    moq_playback_release_command_hold(a, ha[0]);
    CHECK(moq_playback_poll_command_held(a, &ca[1], sizeof(ca[1]), &ha[1]) == MOQ_OK);
    CHECK(!moq_playback_command_hold_eq(ha[0], ha[1]));
    a_bytes = moq_playback_command_hold_bytes(a);
    CHECK(a_bytes > 0);
    moq_playback_release_command_hold(a, ha[0]);           /* stale, same slot */
    CHECK(moq_playback_command_hold_bytes(a) == a_bytes);
    moq_playback_release_command_hold(a, ha[1]);
    /* Spend the slot's generations: one more use, then it retires. */
    moq_playback_test_set_hold_generation(a, 0, 0xFFFFFFFu);
    CHECK(moq_playback_poll_command_held(a, &ca[2], sizeof(ca[2]), &ha[2]) == MOQ_OK);
    moq_playback_release_command_hold(a, ha[2]);
    moq_playback_cmd_t c;
    moq_playback_command_hold_t h = MOQ_PLAYBACK_COMMAND_HOLD_INVALID;
    CHECK(push_d22(a, ta, true, 1, 3, d22_props(T0 + 3, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(a, 0) == MOQ_OK);
    CHECK(moq_playback_poll_command_held(a, &c, sizeof(c), &h) == MOQ_ERR_LIMIT);
    CHECK(h._opaque == 0);
    CHECK(moq_playback_poll_command(a, &c, sizeof(c)) == MOQ_OK);
    CHECK(c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
    moq_playback_cmd_cleanup(&c);
    cleanup_all(ca, 3);
    moq_playback_release_command_hold(b, hb);
    moq_playback_cmd_cleanup(&cb);
    moq_playback_destroy(a);
    moq_playback_destroy(b);
}

/* One CONFIGURE held from a fresh one-hold engine; *pb, *c and *h out. */
static void one_held(moq_playback_t **pb, moq_playback_cmd_t *c,
                     moq_playback_command_hold_t *h)
{
    *pb = new_pb_held(8, 0, 1);
    moq_playback_track_t t = add_track(*pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(*pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(*pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(*pb, 0) == MOQ_OK);
    CHECK(moq_playback_poll_command_held(*pb, c, sizeof(*c), h) == MOQ_OK);
}

/* Holds belong to their engine whatever the engines' creation tags: at the
 * epoch boundary (65536, then 65537) and 65536 creations apart, two live
 * engines share a creation tag, and in the same slot. Their handles still
 * differ, and releasing one engine's hold into the other changes nothing
 * there: its one hold stays active (the cap still refuses) until its own
 * handle releases it. */
static void test_command_hold_owner_boundary(void)
{
    for (int distant = 0; distant < 2; distant++) {
        uint64_t last = moq_playback_test_skip_epochs(0);
        uint64_t to_boundary = 65535 - (last % 65536);   /* next epoch: k * 65536 */
        moq_playback_test_skip_epochs(to_boundary);
        moq_playback_t *a, *b;
        moq_playback_cmd_t ca, cb, x;
        moq_playback_command_hold_t ha, hb, hx;
        one_held(&a, &ca, &ha);
        if (distant) moq_playback_test_skip_epochs(65535);
        one_held(&b, &cb, &hb);
        CHECK(moq_playback_test_creation_tag(a) == moq_playback_test_creation_tag(b));
        CHECK(!moq_playback_command_hold_eq(ha, hb));
        moq_playback_release_command_hold(a, hb);
        CHECK(moq_playback_poll_command_held(a, &x, sizeof(x), &hx) == MOQ_ERR_WOULD_BLOCK);
        moq_playback_release_command_hold(a, ha);
        CHECK(moq_playback_poll_command_held(a, &x, sizeof(x), &hx) == MOQ_OK);
        moq_playback_release_command_hold(a, hx);
        moq_playback_cmd_cleanup(&x);
        moq_playback_release_command_hold(b, hb);
        moq_playback_cmd_cleanup(&ca);
        moq_playback_cmd_cleanup(&cb);
        moq_playback_destroy(a);
        moq_playback_destroy(b);
    }
}

/* A handle stays dead across its engine's end: holds taken from a new
 * engine, in the same slot, after the old engine was destroyed, are never
 * named by the old engine's handles (or their copies). */
static void test_command_hold_across_recreation(void)
{
    moq_playback_t *a;
    moq_playback_cmd_t ca;
    moq_playback_command_hold_t ha;
    one_held(&a, &ca, &ha);
    moq_playback_command_hold_t copy = ha;
    moq_playback_release_command_hold(a, ha);
    moq_playback_cmd_cleanup(&ca);
    moq_playback_destroy(a);
    for (int i = 0; i < 3; i++) {
        moq_playback_t *b;
        moq_playback_cmd_t cb, x;
        moq_playback_command_hold_t hb, hx;
        one_held(&b, &cb, &hb);
        CHECK(!moq_playback_command_hold_eq(copy, hb));
        moq_playback_release_command_hold(b, copy);
        CHECK(moq_playback_poll_command_held(b, &x, sizeof(x), &hx) == MOQ_ERR_WOULD_BLOCK);
        moq_playback_release_command_hold(b, hb);
        moq_playback_cmd_cleanup(&cb);
        moq_playback_destroy(b);
    }
}

/* The last identity is issued once; after it a held poll is LIMIT with
 * nothing changed (the command stays first, no ref, no charge), and an
 * ordinary poll still takes the command. A private issuer, so the
 * library's own is not spent. */
static void test_command_hold_final_issue(void)
{
    moq_playback_t *pb = new_pb_held(8, 0, 4);
    moq_playback_test_local_issuer(pb, 1);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t c, d;
    moq_playback_command_hold_t h, g;
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_OK);
    CHECK(moq_playback_command_hold_is_valid(h));
    uint64_t retained = moq_playback_test_retained_bytes(pb);
    uint64_t held = moq_playback_command_hold_bytes(pb);
    memset(&d, 0x5A, sizeof(d));
    g._opaque = 0x1234;
    CHECK(moq_playback_poll_command_held(pb, &d, sizeof(d), &g) == MOQ_ERR_LIMIT);
    CHECK(g._opaque == 0x1234 && d.struct_size == 0x5A5A5A5Au);
    CHECK(moq_playback_test_retained_bytes(pb) == retained);
    CHECK(moq_playback_command_hold_bytes(pb) == held);
    moq_playback_release_command_hold(pb, h);
    CHECK(moq_playback_poll_command_held(pb, &d, sizeof(d), &g) == MOQ_ERR_LIMIT);
    CHECK(moq_playback_poll_command(pb, &d, sizeof(d)) == MOQ_OK);
    CHECK(d.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO && d.u.decode_video.object_id == 0);
    moq_playback_cmd_cleanup(&c);
    moq_playback_cmd_cleanup(&d);
    moq_playback_destroy(pb);
}

typedef struct {
    uint64_t handles[256];
    size_t   n;
} hold_run_t;

/* One engine on its own thread (the default allocator: the test's
 * counting allocator is not shared across threads), taking and releasing
 * holds in turn. */
static void *hold_runner(void *arg)
{
    hold_run_t *run = (hold_run_t *)arg;
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_held_commands = 2;
    moq_playback_t *pb = NULL;
    if (moq_playback_create(moq_alloc_default(), &cfg, &pb) != MOQ_OK) return NULL;
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.transport_version = MOQ_VERSION_DRAFT_22;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    if (moq_playback_add_track(pb, &tc, &t) != MOQ_OK) { moq_playback_destroy(pb); return NULL; }
    moq_playback_cmd_t c;
    for (uint64_t o = 0; run->n < 256; o++) {
        while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) moq_playback_cmd_cleanup(&c);
        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;   /* a RESET to hold */
        fb.track = t;
        if (moq_playback_handle_feedback(pb, &fb, 0) != MOQ_OK) break;
        moq_playback_command_hold_t h;
        while (run->n < 256 && moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_OK) {
            run->handles[run->n++] = h._opaque;
            moq_playback_release_command_hold(pb, h);
            moq_playback_cmd_cleanup(&c);
        }
        moq_playback_event_t e;
        while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK) {}
        if (o > 4096) break;
    }
    moq_playback_destroy(pb);
    return NULL;
}

/* Engines issuing holds on four threads at once never mint the same
 * identity. */
static void test_command_hold_concurrent_issue(void)
{
    static hold_run_t runs[4];
    pthread_t th[4];
    memset(runs, 0, sizeof(runs));
    for (int i = 0; i < 4; i++) CHECK(pthread_create(&th[i], NULL, hold_runner, &runs[i]) == 0);
    for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);
    size_t total = 0, dups = 0;
    for (int i = 0; i < 4; i++) {
        CHECK(runs[i].n == 256);
        total += runs[i].n;
        for (size_t k = 0; k < runs[i].n; k++)
            for (int j = 0; j <= i; j++)
                for (size_t m = 0; m < (j == i ? k : runs[j].n); m++)
                    dups += runs[i].handles[k] == runs[j].handles[m];
    }
    CHECK(total == 1024);
    CHECK(dups == 0);
}

/* Destroying an engine with holds outstanding ends them; the commands'
 * own refs stay the caller's, cleaned up afterwards. */
static void test_command_hold_destroy(void)
{
    moq_playback_t *pb = new_pb_held(8, 0, 8);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, CFG_A, sizeof(CFG_A), true)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t c[8];
    moq_playback_command_hold_t h[8];
    size_t n = poll_held_all(pb, c, h, 8);
    CHECK(n == 3);
    moq_playback_destroy(pb);
    for (size_t i = 0; i < n; i++)
        if (c[i].kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO)
            CHECK(moq_rcbuf_refcount(c[i].u.decode_video.payload) == 1);
    cleanup_all(c, n);
}

/* max_held_commands: a cfg that does not cover it (an older caller) has
 * held polls disabled whatever lies past its end; 65536 is accepted and
 * 65537 refused. */
static void test_command_hold_cfg(void)
{
    struct { moq_playback_cfg_t cfg; } big;
    moq_playback_cfg_init(&big.cfg);
    big.cfg.max_held_commands = 4;
    big.cfg.struct_size = (uint32_t)offsetof(moq_playback_cfg_t, max_held_commands);
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &big.cfg, &pb) == MOQ_OK);
    moq_playback_cmd_t c;
    moq_playback_command_hold_t h;
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_ERR_UNSUPPORTED);
    moq_playback_destroy(pb);
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_held_commands = 0x10000u;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_DONE);
    moq_playback_destroy(pb);
    cfg.max_held_commands = 0x10001u;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_ERR_INVAL);
}

/* Held polls stop at a sample of a multi-sample CMAF object, its last
 * included, and ordinary polls take it unchanged; a single-sample object
 * is held like any command. */
static void test_command_hold_cmaf_boundary(void)
{
    for (size_t samples = 1; samples <= 3; samples += 2) {
        moq_playback_t *pb = new_pb_held(8, 0, 8);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 1000);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 0, 10, samples, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
        size_t held = 0, refused = 0, plain = 0;
        moq_playback_command_hold_t hs[8];
        for (int i = 0; i < 8; i++) {
            (void)moq_playback_tick(pb, 0);
            moq_playback_cmd_t c;
            moq_playback_command_hold_t h;
            moq_result_t rc = moq_playback_poll_command_held(pb, &c, sizeof(c), &h);
            if (rc == MOQ_OK) {
                hs[held++] = h;
                moq_playback_cmd_cleanup(&c);
            } else if (rc == MOQ_ERR_UNSUPPORTED) {
                refused++;
                CHECK(moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK);
                CHECK(c.kind != MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
                plain++;
                moq_playback_cmd_cleanup(&c);
            }
        }
        if (samples == 1) CHECK(refused == 0 && held >= 2);
        else CHECK(refused == 3 && plain == 3 && held == 1);   /* CONFIGURE only */
        for (size_t i = 0; i < held; i++) moq_playback_release_command_hold(pb, hs[i]);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* A unit released into the command ring stays charged until it is polled,
 * so released units count against the budget like buffered ones: with a
 * budget of three units, three queued units refuse a fourth object until
 * one is polled. A decode-error purge returns the purged units' charges. */
static void test_queued_units_stay_charged(void)
{
    moq_playback_t *pb = new_pb(8, 3 * UNIT);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    CHECK(push_d22(pb, t, true, 1, 0, d22_props(T0, true, false, NULL, 0, false)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 1, d22_props(T0 + 1, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(push_d22(pb, t, true, 1, 2, d22_props(T0 + 2, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 3 * UNIT);
    /* Polling frees these bytes, so the refusal is transient. */
    CHECK(push_d22(pb, t, true, 1, 3, d22_props(T0 + 3, false, false, NULL, 0, false)) ==
          MOQ_ERR_WOULD_BLOCK);
    uint32_t kind = 0;
    CHECK(poll_one(pb, &kind) && kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
    CHECK(moq_playback_test_retained_bytes(pb) == 3 * UNIT);
    CHECK(poll_one(pb, &kind) && kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
    CHECK(moq_playback_test_retained_bytes(pb) == 2 * UNIT);
    CHECK(push_d22(pb, t, true, 1, 3, d22_props(T0 + 3, false, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    while (poll_one(pb, &kind)) {}
    CHECK(moq_playback_test_retained_bytes(pb) == 0);
    moq_playback_destroy(pb);
}

/* -- Parsed-input size floor ----------------------------------------- */

/* The parsed input's floor is its complete layout through timescale:
 * storage without the field or ending one byte short is refused, storage
 * ending exactly at the
 * field is accepted (CMAF with its context, RAW without one). Exact-size
 * heap blocks, so any read past them is a sanitizer error. */
static void test_parsed_input_exact_floor(void)
{
    moq_playback_t *pb = new_pb(8, 0);
    moq_playback_track_t c = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                       MOQ_VERSION_DRAFT_16, 1000);
    moq_playback_track_t r = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_16, 0);
    uint8_t frag[1024];
    size_t flen = cmaf_chunk(frag, 0, 10, 1, 4);
    moq_rcbuf_t *fp = rc_of(frag, flen);
    moq_cmaf_sample_t samples[16];
    moq_media_parsed_object_t parsed;
    moq_playback_parsed_input_t in = cmaf_in(c, fp, 1000, samples, &parsed);

    size_t floor = MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE;
    size_t missing = offsetof(moq_playback_parsed_input_t, timescale);
    unsigned char *nobuf = malloc(missing);
    in.struct_size = (uint32_t)missing;
    memcpy(nobuf, &in, missing);
    CHECK(moq_playback_push_parsed(pb, (const moq_playback_parsed_input_t *)nobuf,
                                   0) == MOQ_ERR_INVAL);
    free(nobuf);
    unsigned char *shortbuf = malloc(floor - 1);
    in.struct_size = (uint32_t)(floor - 1);
    memcpy(shortbuf, &in, floor - 1);
    CHECK(moq_playback_push_parsed(pb, (const moq_playback_parsed_input_t *)shortbuf,
                                   0) == MOQ_ERR_INVAL);
    free(shortbuf);

    unsigned char *exact = malloc(floor);
    in.struct_size = (uint32_t)floor;
    memcpy(exact, &in, floor);
    CHECK(moq_playback_push_parsed(pb, (const moq_playback_parsed_input_t *)exact,
                                   0) == MOQ_OK);
    free(exact);

    static const uint8_t d[2] = { 1, 2 };
    moq_rcbuf_t *payload = rc_of(d, sizeof(d));
    moq_rcbuf_t *props = loc01(MOQ_VERSION_DRAFT_16, T0, true);
    moq_media_parsed_object_t rp;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 0,
                    payload, props, NULL, 0, &rp) == MOQ_OK);
    moq_playback_parsed_input_t rin = parsed_in(r, 1, 0, &rp, payload);
    rin.struct_size = (uint32_t)floor;
    exact = malloc(floor);
    memcpy(exact, &rin, floor);
    CHECK(moq_playback_push_parsed(pb, (const moq_playback_parsed_input_t *)exact,
                                   0) == MOQ_OK);
    free(exact);

    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    moq_rcbuf_decref(fp);
    moq_playback_destroy(pb);
}

/* Rounded decode times can collide across scales: base tick 1 is 0 us at
 * both 3000000 and 4000000 Hz, and so is a one-tick duration. Only the
 * explicit context separates them: a record parsed at 4000000 is refused
 * by a 3000000 Hz track. */
static void test_rounded_collision_needs_explicit_context(void)
{
    moq_playback_t *pb = new_pb(8, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                       MOQ_VERSION_DRAFT_16, 3000000);
    uint8_t frag[1024];
    size_t flen = cmaf_chunk(frag, 1, 1, 3, 4);
    moq_rcbuf_t *fp = rc_of(frag, flen);
    moq_cmaf_sample_t samples[16];
    moq_media_parsed_object_t parsed;
    moq_playback_parsed_input_t in = cmaf_in(t, fp, 4000000, samples, &parsed);
    CHECK(parsed.decode_time_us == 0 && parsed.sample_duration_us == 0);
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    CHECK(moq_rcbuf_refcount(fp) == 1);
    moq_rcbuf_decref(fp);
    moq_playback_destroy(pb);
}

/* -- The hole a CONTINUE track passes -------------------------------- */

/* CMAF samples carry durations: a CONTINUE track missing object 2 reports
 * the hole from the end of object 1 (its decode time plus its duration) to
 * object 3, and resumes without a reset. */
static void test_continue_hole_from_unit_end(void)
{
    moq_playback_t *pb = new_pb(16, 0);
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
    tc.timescale = 1000;
    tc.gap_policy = MOQ_PLAYBACK_GAP_CONTINUE;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    uint64_t objs[3] = { 0, 1, 3 };
    for (int i = 0; i < 3; i++) {
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, objs[i] * 10, 10, 1, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        moq_cmaf_sample_t samples[16];
        moq_media_parsed_object_t parsed;
        CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 1000,
                        1, objs[i], fp, NULL, samples, 16, &parsed) == MOQ_OK);
        moq_playback_parsed_input_t in = parsed_in(t, 1, objs[i], &parsed, fp);
        in.timescale = 1000;
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        moq_rcbuf_decref(fp);
    }
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 1000000) == MOQ_OK);
    size_t resets = 0, decodes = 0;
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_RESET) resets++;
        if (c.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF) decodes++;
        moq_playback_cmd_cleanup(&c);
    }
    CHECK(resets == 0 && decodes == 3);
    bool saw = false;
    moq_playback_event_t e;
    while (moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK) {
        if (e.kind != MOQ_PLAYBACK_EVENT_GAP_CONTINUED) continue;
        saw = true;
        CHECK(e.u.gap_continued.hole_start_us == 20000);
        CHECK(e.u.gap_continued.hole_duration_us == 10000);
    }
    CHECK(saw);
    moq_playback_destroy(pb);
}

/* -- Draft-22 record floor -------------------------------------------- */

/* On a draft-22 track every parsed record must cover the metadata, before
 * any admission effect: a short status marker, a short duplicate of a
 * buffered object and a short record below the cursor are all INVAL and
 * change nothing; a full-size marker is admitted. */
static void test_d22_record_floor(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    moq_media_parsed_object_t marker;
    moq_media_parsed_object_init(&marker);
    marker.packaging = MOQ_MEDIA_PACKAGING_RAW;
    marker.status = MOQ_OBJECT_END_OF_TRACK;
    moq_playback_parsed_input_t in = parsed_in(t, 9, 0, &marker, NULL);
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    CHECK(moq_playback_test_buffered_count(pb) == 0);

    /* Admit and release group 1 object 0, keep object 2 buffered. */
    CHECK(push_d22(pb, t, true, 1, 0,
          d22_props(T0, true, false, NULL, 0, false)) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    seq_t q;
    memset(&q, 0, sizeof(q));
    drain_seq(pb, &q);
    CHECK(push_d22(pb, t, true, 1, 2,
          d22_props(T0 + 2, false, false, NULL, 0, false)) == MOQ_OK);

    static const uint8_t frame[4] = { 0, 0, 1, 0x65 };
    moq_rcbuf_t *payload = rc_of(frame, sizeof(frame));
    moq_rcbuf_t *props = d22_props(T0 + 2, false, false, NULL, 0, false);
    moq_media_parsed_object_t full;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 2,
                    payload, props, NULL, 0, &full) == MOQ_OK);
    moq_media_parsed_object_t shortrec = full;
    shortrec.struct_size = (uint32_t)MOQ_MEDIA_PARSED_OBJECT_V0_SIZE;
    in = parsed_in(t, 1, 2, &shortrec, payload);           /* duplicate */
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in = parsed_in(t, 0, 7, &shortrec, payload);           /* below cursor */
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    moq_playback_event_t e;
    CHECK(moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_DONE);
    CHECK(moq_rcbuf_refcount(payload) == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 1);

    moq_media_parsed_object_t eot;
    moq_media_parsed_object_init_sized(&eot, sizeof(eot));
    eot.packaging = MOQ_MEDIA_PACKAGING_RAW;
    eot.status = MOQ_OBJECT_END_OF_TRACK;
    in = parsed_in(t, 1, 3, &eot, NULL);
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
    CHECK(moq_playback_test_buffered_count(pb) == 2);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* -- push_parsed ------------------------------------------------------ */

/* push_parsed admits a parsed RAW object: one ref on the payload, nothing
 * else retained (the parsed record lives on the caller's stack), and the
 * commands match push_object's for the same object. */
static void test_push_parsed_matches_push_object(void)
{
    static const uint8_t d[6] = { 0, 0, 0, 1, 0x65, 0x88 };
    cmd_log_t via_object, via_parsed;

    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_18, 0);
    moq_rcbuf_t *payload = rc_of(d, sizeof(d));
    moq_rcbuf_t *props = loc01(MOQ_VERSION_DRAFT_18, T0, true);
    CHECK(push_obj(pb, t, 1, 0, payload, props) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &via_object, false);
    moq_playback_destroy(pb);

    pb = new_pb(0, 0);
    t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW, MOQ_VERSION_DRAFT_18, 0);
    {
        moq_media_parsed_object_t parsed;
        CHECK(parse_obj(MOQ_VERSION_DRAFT_18, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 0,
                        payload, props, NULL, 0, &parsed) == MOQ_OK);
        moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, payload);
        CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        memset(&parsed, 0xEE, sizeof(parsed));   /* not retained */
    }
    CHECK(moq_rcbuf_refcount(payload) == 2);
    CHECK(moq_playback_test_retained_bytes(pb) == sizeof(d));
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    drain_cmds(pb, &via_parsed, false);
    CHECK(moq_rcbuf_refcount(payload) == 1);

    CHECK(via_object.decodes == 1 && via_parsed.decodes == 1);
    CHECK(via_object.configures == via_parsed.configures);
    CHECK(via_parsed.dts[0] == T0 && via_object.dts[0] == T0);
    CHECK(via_parsed.keyframe[0] && via_object.keyframe[0]);

    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* A draft-22 parsed object needs the parsed record's metadata, so its
 * struct_size must cover it; LOC-04 application time is then playable. */
static void test_push_parsed_draft22(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_22, 0);
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_TIMESCALE |
                MOQ_LOC_METADATA_HAS_MARKING;
    m.timestamp = 90000;
    m.timescale = 90000;
    m.marking_len = 1;
    m.marking[0] = 0xE0;
    moq_rcbuf_t *props = NULL;
    CHECK(moq_loc_encode_metadata(&g_alloc, MOQ_VERSION_DRAFT_22,
                                  MOQ_LOC_PROFILE_04, &m, &props) == MOQ_OK);
    static const uint8_t d[2] = { 1, 2 };
    moq_rcbuf_t *payload = rc_of(d, sizeof(d));
    moq_media_parsed_object_t parsed;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 0,
                    payload, props, NULL, 0, &parsed) == MOQ_OK);

    moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, payload);
    moq_media_parsed_object_t legacy = parsed;
    legacy.struct_size = (uint32_t)MOQ_MEDIA_PARSED_OBJECT_V0_SIZE;
    in.parsed = &legacy;
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    CHECK(moq_rcbuf_refcount(payload) == 1);

    in.parsed = &parsed;
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl, false);
    CHECK(cl.decodes == 1 && cl.dts[0] == 1000000);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* Inconsistent parsed input is a caller error and transfers nothing:
 * missing record, packaging that is not the track's, a payload the record
 * does not describe, a status that contradicts the payload, and a CMAF
 * media range outside the payload. */
static void test_push_parsed_validation(void)
{
    moq_playback_t *pb = new_pb(0, 0);
    moq_playback_track_t raw = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                         MOQ_VERSION_DRAFT_16, 0);
    moq_playback_track_t cmaf = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                          MOQ_VERSION_DRAFT_16, 1000);
    static const uint8_t d[4] = { 1, 2, 3, 4 };
    moq_rcbuf_t *payload = rc_of(d, sizeof(d));
    moq_rcbuf_t *other = rc_of(d, 3);
    moq_rcbuf_t *props = loc01(MOQ_VERSION_DRAFT_16, T0, true);
    moq_media_parsed_object_t parsed;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 0,
                    payload, props, NULL, 0, &parsed) == MOQ_OK);

    moq_playback_parsed_input_t in = parsed_in(raw, 1, 0, &parsed, payload);
    CHECK(moq_playback_push_parsed(NULL, &in, 0) == MOQ_ERR_INVAL);
    CHECK(moq_playback_push_parsed(pb, NULL, 0) == MOQ_ERR_INVAL);
    in.parsed = NULL;
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in.parsed = &parsed;
    in.struct_size = (uint32_t)(MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE - 1);
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in.struct_size = sizeof(in);
    in.track = cmaf;                                   /* packaging mismatch */
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in.track = raw;
    moq_media_parsed_object_t wrong = parsed;          /* claims CMAF */
    wrong.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    in.parsed = &wrong;
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in.parsed = &parsed;
    in.payload = other;                                /* not the parsed bytes */
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in.payload = NULL;                                 /* NORMAL needs payload */
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);
    in.payload = payload;
    moq_media_parsed_object_t marker = parsed;
    marker.status = MOQ_OBJECT_END_OF_GROUP;           /* marker with payload */
    in.parsed = &marker;
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_INVAL);

    uint8_t frag[512];
    size_t flen = cmaf_chunk(frag, 0, 10, 1, 8);
    moq_rcbuf_t *fp = rc_of(frag, flen);
    moq_cmaf_sample_t samples[4];
    moq_media_parsed_object_t cp;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 1000, 1, 0,
                    fp, NULL, samples, 4, &cp) == MOQ_OK);
    cp.mdat_len = flen;                                /* beyond the payload */
    moq_playback_parsed_input_t cin = parsed_in(cmaf, 1, 0, &cp, fp);
    cin.timescale = 1000;
    CHECK(moq_playback_push_parsed(pb, &cin, 0) == MOQ_ERR_INVAL);

    CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(fp) == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    moq_rcbuf_decref(fp);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(other);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* -- Permanent refusal ------------------------------------------------ */

/* An object larger than the whole byte budget can never be admitted: both
 * push paths return MOQ_ERR_LIMIT at once, even for an empty engine,
 * retain nothing and report nothing. The cursor does not move: the
 * objects before the refused one are still admitted and played. */
static void test_oversize_is_limit_without_cursor_jump(void)
{
    moq_playback_t *pb = new_pb(0, 16);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_16, 0);
    static const uint8_t big[17] = { 0 };
    static const uint8_t small[2] = { 1, 2 };
    moq_rcbuf_t *bp = rc_of(big, sizeof(big));
    moq_rcbuf_t *props = loc01(MOQ_VERSION_DRAFT_16, T0, true);
    CHECK(push_obj(pb, t, 1, 5, bp, props) == MOQ_ERR_LIMIT);
    moq_media_parsed_object_t parsed;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_RAW, 0, 1, 5,
                    bp, props, NULL, 0, &parsed) == MOQ_OK);
    moq_playback_parsed_input_t in = parsed_in(t, 1, 5, &parsed, bp);
    CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_ERR_LIMIT);
    CHECK(moq_rcbuf_refcount(bp) == 1);
    CHECK(moq_playback_test_buffered_count(pb) == 0);
    moq_playback_event_t e;
    CHECK(moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_DONE);

    for (uint64_t o = 0; o < 5; o++) {
        moq_rcbuf_t *sp = rc_of(small, sizeof(small));
        moq_rcbuf_t *pp = loc01(MOQ_VERSION_DRAFT_16, T0 + o, o == 0);
        CHECK(push_obj(pb, t, 1, o, sp, pp) == MOQ_OK);
        moq_rcbuf_decref(sp);
        moq_rcbuf_decref(pp);
    }
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    cmd_log_t cl;
    drain_cmds(pb, &cl, false);
    CHECK(cl.decodes == 5);
    for (size_t i = 0; i < 5 && i < cl.decodes; i++)
        CHECK(cl.object[i] == i);
    CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_STALE) == 0);

    moq_rcbuf_decref(bp);
    moq_rcbuf_decref(props);
    moq_playback_destroy(pb);
}

/* -- Multi-sample CMAF through a small command ring ------------------- */

/* A five-sample object drains through a two-slot command ring across
 * ticks: CONFIGURE and the first sample, then two samples per tick. Each
 * sample's decode time is converted from its own tick position (a 3 Hz
 * timescale makes rounded accumulation visible), and the media ranges
 * walk the mdat. Push_object and push_parsed release the same commands. */
static void test_multi_sample_through_small_ring(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(2, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 3);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 0, 1, 5, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        if (via_parsed) {
            moq_cmaf_sample_t samples[16];
            moq_media_parsed_object_t parsed;
            CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 3,
                            1, 0, fp, NULL, samples, 16, &parsed) == MOQ_OK);
            moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, fp);
            in.timescale = 3;
            CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        } else {
            CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
        }

        cmd_log_t cl;
        memset(&cl, 0, sizeof(cl));
        for (int i = 0; i < 6 && cl.decodes < 5; i++) {
            moq_result_t rc = moq_playback_tick(pb, 0);
            CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
            drain_cmds(pb, &cl, true);
        }
        CHECK(cl.configures == 1);
        CHECK(cl.decodes == 5);
        static const uint64_t want[5] = { 0, 333333, 666666, 1000000, 1333333 };
        for (size_t i = 0; i < 5 && i < cl.decodes; i++) {
            CHECK(cl.dts[i] == want[i]);
            CHECK(cl.mdat_len[i] == 4);
            CHECK(cl.mdat_off[i] == cl.mdat_off[0] + i * 4);
            CHECK(cl.keyframe[i] == (i == 0));
        }
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(fp) == 1);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

/* A decode error while an object is part-way out of the command ring:
 * the decoder lost its state, so after the reset the object is released
 * again from its first (sync) sample, and no later sample of it is
 * emitted before that. */
static void test_decode_error_restarts_partial_object(void)
{
    moq_playback_t *pb = new_pb(2, 0);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                       MOQ_VERSION_DRAFT_16, 1000);
    uint8_t frag[1024];
    size_t flen = cmaf_chunk(frag, 0, 10, 4, 4);
    moq_rcbuf_t *fp = rc_of(frag, flen);
    CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
    CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);  /* CONFIGURE and sample 0 */
    cmd_log_t cl;
    drain_cmds(pb, &cl, false);
    CHECK(cl.decodes == 1 && cl.dts[0] == 0);

    moq_playback_feedback_t fb;
    moq_playback_feedback_init(&fb);
    fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    fb.track = t;
    CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
    moq_playback_cmd_t c;
    CHECK(moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK &&
          c.kind == MOQ_PLAYBACK_CMD_RESET);
    moq_playback_cmd_cleanup(&c);

    memset(&cl, 0, sizeof(cl));
    for (int i = 0; i < 6 && cl.decodes < 4; i++) {
        (void)moq_playback_tick(pb, 0);
        drain_cmds(pb, &cl, true);
    }
    CHECK(cl.configures == 1);
    CHECK(cl.decodes == 4);
    for (size_t i = 0; i < 4 && i < cl.decodes; i++)
        CHECK(cl.dts[i] == i * 10000);
    CHECK(moq_rcbuf_refcount(fp) == 1);
    moq_rcbuf_decref(fp);
    moq_playback_destroy(pb);
}

/* push_parsed shares push_object's refusals: a full buffer is WOULD_BLOCK
 * with no ref taken, and a record with more samples than the engine's
 * sample cap is dropped UNSUPPORTED_MULTI_SAMPLE. */
static void test_push_parsed_refusals(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.max_buffered_objects = 1;
    cfg.max_samples_per_object = 2;
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_RAW,
                                       MOQ_VERSION_DRAFT_16, 0);
    moq_playback_track_t c = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                       MOQ_VERSION_DRAFT_16, 1000);
    static const uint8_t d[2] = { 1, 2 };
    for (uint64_t o = 0; o < 2; o++) {
        moq_rcbuf_t *payload = rc_of(d, sizeof(d));
        moq_rcbuf_t *props = loc01(MOQ_VERSION_DRAFT_16, T0 + o, true);
        moq_media_parsed_object_t parsed;
        CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_RAW, 0, 1, o,
                        payload, props, NULL, 0, &parsed) == MOQ_OK);
        moq_playback_parsed_input_t in = parsed_in(t, 1, o, &parsed, payload);
        CHECK(moq_playback_push_parsed(pb, &in, 0) ==
              (o == 0 ? MOQ_OK : MOQ_ERR_WOULD_BLOCK));
        CHECK(moq_rcbuf_refcount(payload) == (o == 0 ? 2u : 1u));
        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    uint8_t frag[1024];
    size_t flen = cmaf_chunk(frag, 0, 10, 3, 4);
    moq_rcbuf_t *fp = rc_of(frag, flen);
    moq_cmaf_sample_t samples[8];
    moq_media_parsed_object_t cp;
    CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF, 1000, 1, 0,
                    fp, NULL, samples, 8, &cp) == MOQ_OK);
    moq_playback_parsed_input_t cin = parsed_in(c, 1, 0, &cp, fp);
    cin.timescale = 1000;
    CHECK(moq_playback_push_parsed(pb, &cin, 0) == MOQ_OK);
    CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE) == 1);
    CHECK(moq_rcbuf_refcount(fp) == 1);
    moq_rcbuf_decref(fp);
    moq_playback_destroy(pb);
}

/* -- Multi-chunk CMAF ------------------------------------------------- */

/* An object of two CMAF chunks is refused with UNSUPPORTED_MULTI_CHUNK on
 * both push paths: its later chunks are never silently dropped while the
 * first one plays. */
static void test_multi_chunk_refused(void)
{
    for (int via_parsed = 0; via_parsed < 2; via_parsed++) {
        moq_playback_t *pb = new_pb(0, 0);
        moq_playback_track_t t = add_track(pb, MOQ_PLAYBACK_PACKAGING_CMAF,
                                           MOQ_VERSION_DRAFT_16, 1000);
        uint8_t frag[1024];
        size_t flen = cmaf_chunk(frag, 0, 10, 2, 4);
        flen += cmaf_chunk(frag + flen, 20, 10, 2, 4);
        moq_rcbuf_t *fp = rc_of(frag, flen);
        if (via_parsed) {
            moq_cmaf_sample_t samples[16];
            moq_media_parsed_object_t parsed;
            CHECK(parse_obj(MOQ_VERSION_DRAFT_16, MOQ_MEDIA_PACKAGING_CMAF,
                            1000, 1, 0, fp, NULL, samples, 16,
                            &parsed) == MOQ_OK);
            CHECK(parsed.chunk_count == 2);
            moq_playback_parsed_input_t in = parsed_in(t, 1, 0, &parsed, fp);
            in.timescale = 1000;
            CHECK(moq_playback_push_parsed(pb, &in, 0) == MOQ_OK);
        } else {
            CHECK(push_obj(pb, t, 1, 0, fp, NULL) == MOQ_OK);
        }
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(count_drops(pb, MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_CHUNK) == 1);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        cmd_log_t cl;
        drain_cmds(pb, &cl, false);
        CHECK(cl.decodes == 0);
        CHECK(moq_rcbuf_refcount(fp) == 1);
        moq_rcbuf_decref(fp);
        moq_playback_destroy(pb);
    }
}

int main(void)
{
    test_raw_decode_order_is_object_order();
    test_cmaf_monotonic_dts_reordered_pts();
    test_push_parsed_matches_push_object();
    test_push_parsed_draft22();
    test_push_parsed_validation();
    test_oversize_is_limit_without_cursor_jump();
    test_multi_sample_through_small_ring();
    test_multi_chunk_refused();
    test_decode_error_restarts_partial_object();
    test_push_parsed_refusals();
    test_object_config_initial();
    test_object_config_changes_in_order();
    test_object_config_audio_and_catalog();
    test_object_config_bounds();
    test_one_slot_ring();
    test_one_slot_ring_decode_error();
    test_parsed_cmaf_timing_context();
    test_d22_record_floor();
    test_parsed_timescale_is_explicit();
    test_context_precedes_over_cap_drop();
    test_parsed_media_range();
    test_configure_then_reanchor();
    test_decode_error_between_configure_and_decode();
    test_config_charge_follows_the_allocation();
    test_samples_overrunning_mdat();
    test_reset_restores_release_configuration();
    test_relief_reclaims_config_bytes();
    test_relief_counts_shared_config_once();
    test_relief_insufficient_reclaim_drops_nothing();
    test_relief_with_config_under_event_pressure();
    test_queued_configure_keeps_charge();
    test_queued_configure_reuse();
    test_queued_configure_lifetimes();
    test_queued_config_pressure_is_transient();
    test_queued_units_stay_charged();
    test_command_holds();
    test_command_hold_config_replacement();
    test_command_hold_sharing();
    test_command_hold_refusals();
    test_command_hold_identity();
    test_command_hold_owner_boundary();
    test_command_hold_across_recreation();
    test_command_hold_final_issue();
    test_command_hold_concurrent_issue();
    test_command_hold_destroy();
    test_command_hold_cfg();
    test_command_hold_cmaf_boundary();
    test_parsed_input_exact_floor();
    test_rounded_collision_needs_explicit_context();
    test_continue_hole_from_unit_end();

    CHECK(g_balance == 0);
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("playback parsed: all passed\n");
    return 0;
}
