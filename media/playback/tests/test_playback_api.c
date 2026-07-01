#include <moq/playback.h>
#include <moq/subscriber.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Test-only helpers — not in the public header. */
extern moq_result_t moq_playback_test_push_cmd(moq_playback_t *pb,
                                                const moq_playback_cmd_t *cmd);
extern moq_result_t moq_playback_test_push_event(moq_playback_t *pb,
                                                  const moq_playback_event_t *evt);
extern uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb);
extern size_t moq_playback_test_buffered_count(const moq_playback_t *pb);
extern bool moq_playback_test_track_anchored(const moq_playback_t *pb,
                                              moq_playback_track_t h);
extern uint64_t moq_playback_test_anchor_group(const moq_playback_t *pb,
                                                moq_playback_track_t h);
extern uint64_t moq_playback_test_anchor_object(const moq_playback_t *pb,
                                                 moq_playback_track_t h);
extern moq_rcbuf_t *moq_playback_test_track_codec(const moq_playback_t *pb,
                                                    moq_playback_track_t h);
extern moq_rcbuf_t *moq_playback_test_track_init_data(const moq_playback_t *pb,
                                                       moq_playback_track_t h);
extern uint16_t moq_playback_test_creation_tag(const moq_playback_t *pb);
extern uint32_t moq_playback_test_generation_seed(const moq_playback_t *pb);
extern bool moq_playback_test_gap_waiting(const moq_playback_t *pb,
                                           moq_playback_track_t h);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- OOM allocator --------------------------------------------------- */

typedef struct {
    int64_t  balance;
    uint64_t alloc_count;
    uint64_t fail_at;
} oom_state_t;

static void *oom_alloc(size_t sz, void *ctx) {
    oom_state_t *s = (oom_state_t *)ctx;
    if (sz == 0) return NULL;
    s->alloc_count++;
    if (s->fail_at > 0 && s->alloc_count == s->fail_at) return NULL;
    void *p = malloc(sz);
    if (p) s->balance++;
    return p;
}
static void oom_free(void *p, size_t sz, void *ctx) {
    oom_state_t *s = (oom_state_t *)ctx;
    (void)sz;
    if (p) s->balance--;
    free(p);
}
static moq_alloc_t oom_allocator(oom_state_t *s) {
    return (moq_alloc_t){ s, oom_alloc, NULL, oom_free };
}

/* -- Helpers --------------------------------------------------------- */

static moq_playback_t *make_pb(const moq_alloc_t *alloc,
                                moq_playback_cfg_t *cfg)
{
    moq_playback_t *pb = NULL;
    moq_result_t rc = moq_playback_create(alloc, cfg, &pb);
    if (rc != MOQ_OK) return NULL;
    return pb;
}

static moq_playback_track_cfg_t make_track_cfg(void)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    return tc;
}

static bool handle_valid(moq_playback_track_t h)
{
    return h._opaque != 0;
}

/* -- CMAF fragment builder ------------------------------------------- */

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v;
}

static size_t box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}

static size_t build_avc_init(uint8_t *buf,
                              uint32_t timescale, uint16_t w, uint16_t h,
                              const uint8_t *avcc, size_t avcc_len)
{
    size_t p = 0;
    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    size_t avcc_box = 8 + avcc_len;
    size_t avc1_sz = 8 + 78 + avcc_box;
    size_t stsd_sz = 8 + 8 + avc1_sz;
    size_t stbl_sz = 8 + stsd_sz;
    size_t minf_sz = 8 + stbl_sz;
    size_t mdhd_sz = 8 + 4 + 12;
    size_t mdia_sz = 8 + mdhd_sz + minf_sz;
    size_t trak_sz = 8 + mdia_sz;
    size_t moov_sz = 8 + trak_sz;

    p += box_hdr(buf + p, (uint32_t)moov_sz, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_sz, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_sz, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_sz, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, timescale); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_sz, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_sz, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_sz, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)avc1_sz, "avc1");
    memset(buf + p, 0, 78);
    wr16(buf + p + 24, w);
    wr16(buf + p + 26, h);
    p += 78;
    p += box_hdr(buf + p, (uint32_t)avcc_box, "avcC");
    memcpy(buf + p, avcc, avcc_len);
    p += avcc_len;
    return p;
}

static size_t build_cmaf_fragment(uint8_t *buf, uint64_t base_time,
                                   uint32_t duration, uint32_t sample_size,
                                   uint32_t flags, int32_t comp_offset,
                                   const uint8_t *mdat_data, size_t mdat_len)
{
    size_t p = 0;
    uint32_t trun_flags = 0x100 | 0x200 | 0x400 | 0x800;
    size_t trun_sz = 8 + 8 + 16;
    size_t tfdt_sz = 8 + 4 + 8;
    size_t tfhd_sz = 8 + 8;
    size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
    size_t moof_sz = 8 + traf_sz;

    p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");
    p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)tfdt_sz, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;
    p += box_hdr(buf + p, (uint32_t)trun_sz, "trun");
    wr32(buf + p, trun_flags); p += 4;
    wr32(buf + p, 1); p += 4;
    wr32(buf + p, duration); p += 4;
    wr32(buf + p, sample_size); p += 4;
    wr32(buf + p, flags); p += 4;
    wr32(buf + p, (uint32_t)comp_offset); p += 4;
    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    memcpy(buf + p, mdat_data, mdat_len);
    p += mdat_len;
    return p;
}

static size_t build_multi_sample_frag(uint8_t *buf, uint64_t base_time,
                                       uint32_t duration, size_t count,
                                       const uint8_t *mdat_data,
                                       size_t mdat_len)
{
    size_t p = 0;
    uint32_t trun_flags = 0x100;
    size_t trun_sz = 8 + 8 + count * 4;
    size_t tfdt_sz = 8 + 4 + 8;
    size_t tfhd_sz = 8 + 8;
    size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
    size_t moof_sz = 8 + traf_sz;

    p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");
    p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)tfdt_sz, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;
    p += box_hdr(buf + p, (uint32_t)trun_sz, "trun");
    wr32(buf + p, trun_flags); p += 4;
    wr32(buf + p, (uint32_t)count); p += 4;
    for (size_t i = 0; i < count; i++) {
        wr32(buf + p, duration); p += 4;
    }
    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    memcpy(buf + p, mdat_data, mdat_len);
    p += mdat_len;
    return p;
}

static size_t build_aac_init(uint8_t *buf, uint32_t timescale,
                              uint16_t samplerate, uint16_t channels,
                              const uint8_t *asc, size_t asc_len)
{
    size_t p = 0;
    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    size_t dec_spec_len = 2 + asc_len;
    size_t dec_cfg_len = 2 + 13 + dec_spec_len;
    size_t es_desc_len = 2 + 3 + dec_cfg_len;
    size_t esds_body = 4 + es_desc_len;
    size_t esds_size = 8 + esds_body;
    size_t mp4a_size = 8 + 28 + esds_size;
    size_t stsd_size = 8 + 8 + mp4a_size;
    size_t stbl_size = 8 + stsd_size;
    size_t minf_size = 8 + stbl_size;
    size_t mdhd_size = 8 + 4 + 12;
    size_t mdia_size = 8 + mdhd_size + minf_size;
    size_t trak_size = 8 + mdia_size;
    size_t moov_size = 8 + trak_size;

    p += box_hdr(buf + p, (uint32_t)moov_size, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_size, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_size, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_size, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, timescale); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_size, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_size, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_size, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)mp4a_size, "mp4a");
    memset(buf + p, 0, 28);
    wr16(buf + p + 8, channels);
    wr16(buf + p + 24, samplerate);
    p += 28;
    p += box_hdr(buf + p, (uint32_t)esds_size, "esds");
    wr32(buf + p, 0); p += 4;
    buf[p++] = 0x03; buf[p++] = (uint8_t)(es_desc_len - 2);
    wr16(buf + p, 0); p += 2; buf[p++] = 0;
    buf[p++] = 0x04; buf[p++] = (uint8_t)(dec_cfg_len - 2);
    buf[p++] = 0x40; buf[p++] = 0x15;
    buf[p++] = 0; buf[p++] = 0; buf[p++] = 0;
    wr32(buf + p, 0); p += 4; wr32(buf + p, 0); p += 4;
    buf[p++] = 0x05; buf[p++] = (uint8_t)asc_len;
    memcpy(buf + p, asc, asc_len); p += asc_len;
    return p;
}

#define MAKE_LOC_PROPS(alloc_p, ts_us, is_kf, out_ptr) do { \
    moq_loc_headers_t _lh;                                    \
    moq_loc_headers_init(&_lh);                               \
    _lh.has_timestamp = true;                                 \
    _lh.timestamp = (ts_us);                                  \
    if (is_kf) {                                              \
        _lh.has_video_frame_marking = true;                   \
        _lh.video_frame_marking.independent = true;           \
        _lh.video_frame_marking.start_of_frame = true;        \
        _lh.video_frame_marking.end_of_frame = true;          \
    }                                                         \
    CHECK(moq_loc_encode((alloc_p), MOQ_LOC_PROFILE_01,       \
                         &_lh, (out_ptr)) == MOQ_OK);         \
} while (0)

static void test_sparse_future_group(const moq_alloc_t *alloc);

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();

    /* ================================================================ */
    /*  PB1 tests (retained)                                           */
    /* ================================================================ */

    /* -- init functions null-safe ------------------------------------- */
    {
        moq_playback_cfg_init(NULL);
        moq_playback_track_cfg_init(NULL);
        moq_playback_object_init(NULL);
        moq_playback_feedback_init(NULL);
        moq_playback_cmd_cleanup(NULL);
    }

    /* -- cfg defaults ------------------------------------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        CHECK(cfg.struct_size == sizeof(moq_playback_cfg_t));
        CHECK(cfg.max_tracks == 4);
        CHECK(cfg.max_commands == 64);
        CHECK(cfg.max_events == 16);
        CHECK(cfg.max_buffered_bytes == 16u * 1024 * 1024);
        CHECK(cfg.max_track_config_bytes == 4096);
    }

    /* -- create/destroy with defaults -------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);
        CHECK(pb != NULL);
        moq_playback_destroy(pb);
    }

    /* -- create rejects NULL args ------------------------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(NULL, &cfg, &pb) == MOQ_ERR_INVAL);
        CHECK(moq_playback_create(alloc, NULL, &pb) == MOQ_ERR_INVAL);
        CHECK(moq_playback_create(alloc, &cfg, NULL) == MOQ_ERR_INVAL);
    }

    /* -- create rejects NULL alloc/free function pointers ------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        moq_alloc_t bad = { NULL, NULL, NULL, NULL };
        CHECK(moq_playback_create(&bad, &cfg, &pb) == MOQ_ERR_INVAL);
        bad.alloc = oom_alloc;
        CHECK(moq_playback_create(&bad, &cfg, &pb) == MOQ_ERR_INVAL);
    }

    /* -- create rejects struct_size too small ------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.struct_size = 4;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_ERR_INVAL);
    }

    /* -- create with zero caps defaults them -------------------------- */
    {
        moq_playback_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.struct_size = sizeof(cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_destroy(pb);
    }

    /* -- create with explicit small caps ----------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;
        cfg.max_events = 2;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_destroy(pb);
    }

    /* -- poll empty returns DONE ------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        moq_playback_event_t ev;
        CHECK(moq_playback_poll_event(pb, &ev) == MOQ_DONE);
        moq_playback_destroy(pb);
    }

    /* -- poll NULL args returns INVAL -------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(NULL, &cmd) == MOQ_ERR_INVAL);
        CHECK(moq_playback_poll_command(pb, NULL) == MOQ_ERR_INVAL);
        moq_playback_event_t ev;
        CHECK(moq_playback_poll_event(NULL, &ev) == MOQ_ERR_INVAL);
        CHECK(moq_playback_poll_event(pb, NULL) == MOQ_ERR_INVAL);
        moq_playback_destroy(pb);
    }

    /* -- tick OK, NULL INVAL ------------------------------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 1000) == MOQ_OK);
        CHECK(moq_playback_tick(NULL, 0) == MOQ_ERR_INVAL);
        moq_playback_destroy(pb);
    }

    /* -- destroy drains queued commands with owned refs --------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 4;

        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);

        moq_rcbuf_t *buf = NULL;
        uint8_t data[] = { 0xCA, 0xFE };
        CHECK(moq_rcbuf_create(alloc, data, 2, &buf) == MOQ_OK);
        moq_rcbuf_incref(buf);
        CHECK(moq_rcbuf_refcount(buf) == 2);

        moq_playback_cmd_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        cmd.kind = MOQ_PLAYBACK_CMD_DECODE_VIDEO;
        cmd.u.decode_video.payload = buf;
        CHECK(moq_playback_test_push_cmd(pb, &cmd) == MOQ_OK);

        moq_playback_destroy(pb);
        CHECK(moq_rcbuf_refcount(buf) == 1);
        moq_rcbuf_decref(buf);
    }

    /* -- poll_command transfers ownership ---------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 4;

        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);

        moq_rcbuf_t *buf = NULL;
        uint8_t data[] = { 0xAB };
        CHECK(moq_rcbuf_create(alloc, data, 1, &buf) == MOQ_OK);
        moq_rcbuf_incref(buf);
        CHECK(moq_rcbuf_refcount(buf) == 2);

        moq_playback_cmd_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        cmd.kind = MOQ_PLAYBACK_CMD_DECODE_VIDEO;
        cmd.u.decode_video.payload = buf;
        CHECK(moq_playback_test_push_cmd(pb, &cmd) == MOQ_OK);

        moq_playback_cmd_t polled;
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(polled.u.decode_video.payload == buf);
        CHECK(moq_rcbuf_refcount(buf) == 2);

        moq_playback_cmd_t empty;
        CHECK(moq_playback_poll_command(pb, &empty) == MOQ_DONE);

        moq_playback_cmd_cleanup(&polled);
        CHECK(moq_rcbuf_refcount(buf) == 1);

        moq_rcbuf_decref(buf);
        moq_playback_destroy(pb);
    }

    /* -- event queue ordering ---------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 4;

        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);

        moq_playback_event_t e1 = { .kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED };
        e1.u.gap_detected.group_id = 10;
        moq_playback_event_t e2 = { .kind = MOQ_PLAYBACK_EVENT_SKIP_FORWARD };
        e2.u.skip_forward.from_group_id = 10;
        e2.u.skip_forward.to_group_id = 15;
        moq_playback_event_t e3 = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };

        CHECK(moq_playback_test_push_event(pb, &e1) == MOQ_OK);
        CHECK(moq_playback_test_push_event(pb, &e2) == MOQ_OK);
        CHECK(moq_playback_test_push_event(pb, &e3) == MOQ_OK);

        moq_playback_event_t out;
        CHECK(moq_playback_poll_event(pb, &out) == MOQ_OK);
        CHECK(out.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED);
        CHECK(out.u.gap_detected.group_id == 10);
        CHECK(moq_playback_poll_event(pb, &out) == MOQ_OK);
        CHECK(out.kind == MOQ_PLAYBACK_EVENT_SKIP_FORWARD);
        CHECK(out.u.skip_forward.to_group_id == 15);
        CHECK(moq_playback_poll_event(pb, &out) == MOQ_OK);
        CHECK(out.kind == MOQ_PLAYBACK_EVENT_TRACK_ENDED);
        CHECK(moq_playback_poll_event(pb, &out) == MOQ_DONE);

        moq_playback_destroy(pb);
    }

    /* -- command queue full WOULD_BLOCK ------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;

        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);

        moq_playback_cmd_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        cmd.kind = MOQ_PLAYBACK_CMD_RESET;
        CHECK(moq_playback_test_push_cmd(pb, &cmd) == MOQ_OK);
        CHECK(moq_playback_test_push_cmd(pb, &cmd) == MOQ_OK);
        CHECK(moq_playback_test_push_cmd(pb, &cmd) == MOQ_ERR_WOULD_BLOCK);

        moq_playback_cmd_t polled;
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);
        CHECK(moq_playback_test_push_cmd(pb, &cmd) == MOQ_OK);

        moq_playback_destroy(pb);
    }

    /* -- event queue full WOULD_BLOCK --------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 2;

        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &cfg, &pb) == MOQ_OK);

        moq_playback_event_t evt = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &evt) == MOQ_OK);
        CHECK(moq_playback_test_push_event(pb, &evt) == MOQ_OK);
        CHECK(moq_playback_test_push_event(pb, &evt) == MOQ_ERR_WOULD_BLOCK);

        moq_playback_event_t polled;
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        CHECK(moq_playback_test_push_event(pb, &evt) == MOQ_OK);

        moq_playback_destroy(pb);
    }

    /* -- OOM: fail at each create allocation point -------------------- */
    /* create does 6 allocs: struct, cmd_ring, evt_ring, tracks, objects, sample_scratch */
    for (uint64_t fail_pt = 1; fail_pt <= 6; fail_pt++) {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        oom_state_t oom = { 0, 0, fail_pt };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_ERR_NOMEM);
        CHECK(pb == NULL);
        CHECK(oom.balance == 0);
    }

    /* -- destroy NULL safe ------------------------------------------- */
    {
        moq_playback_destroy(NULL);
    }

    /* -- enum values ------------------------------------------------- */
    {
        CHECK(MOQ_PLAYBACK_MEDIA_VIDEO == 1);
        CHECK(MOQ_PLAYBACK_PACKAGING_RAW == 1);
        CHECK(MOQ_PLAYBACK_PACKAGING_CMAF == 2);
        CHECK(MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO == 1);
        CHECK(MOQ_PLAYBACK_CMD_DECODE_CMAF == 3);
        CHECK(MOQ_PLAYBACK_DROP_MALFORMED_LOC == 1);
        CHECK(MOQ_PLAYBACK_DROP_KEYFRAME_WAIT == 7);
        CHECK(MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE == 1);
    }

    /* ================================================================ */
    /*  PB2a: Track pool tests                                         */
    /* ================================================================ */

    /* -- add_track succeeds for minimal RAW video config -------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);
        CHECK(pb != NULL);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(handle_valid(t));

        moq_playback_destroy(pb);
    }

    /* -- add_track copies codec/init_data (caller can mutate after) --- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t codec_data[] = "avc1.42e01e";
        uint8_t init_bytes[] = { 0x00, 0x01, 0x02, 0x03 };
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec_data, .len = sizeof(codec_data) - 1 };
        tc.init_data = (moq_bytes_t){ .data = init_bytes, .len = sizeof(init_bytes) };

        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        memset(codec_data, 0xFF, sizeof(codec_data));
        memset(init_bytes, 0xFF, sizeof(init_bytes));

        moq_rcbuf_t *stored_codec = moq_playback_test_track_codec(pb, t);
        CHECK(stored_codec != NULL);
        const uint8_t *cd = moq_rcbuf_data(stored_codec);
        CHECK(cd[0] == 'a');
        CHECK(cd[1] == 'v');

        moq_rcbuf_t *stored_init = moq_playback_test_track_init_data(pb, t);
        CHECK(stored_init != NULL);
        const uint8_t *id = moq_rcbuf_data(stored_init);
        CHECK(id[0] == 0x00);
        CHECK(id[3] == 0x03);

        moq_playback_destroy(pb);
    }

    /* -- add_track pool full returns WOULD_BLOCK ---------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t1, t2, t3;
        CHECK(moq_playback_add_track(pb, &tc, &t1) == MOQ_OK);
        CHECK(moq_playback_add_track(pb, &tc, &t2) == MOQ_OK);
        t3 = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t3) == MOQ_ERR_WOULD_BLOCK);
        CHECK(!handle_valid(t3));

        moq_playback_destroy(pb);
    }

    /* -- add_track rejects too-small struct_size ---------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.struct_size = 4;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
    }

    /* -- add_track rejects invalid media_type / packaging ------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.media_type = (moq_playback_media_type_t)99;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);

        tc = make_track_cfg();
        tc.packaging = (moq_playback_packaging_t)99;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);

        moq_playback_destroy(pb);
    }

    /* -- add_track accepts AUDIO --------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(handle_valid(t));

        moq_playback_destroy(pb);
    }

    /* -- add_track rejects {data=NULL, len>0} codec/init_data -------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = NULL, .len = 10 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
        CHECK(!handle_valid(t));

        tc = make_track_cfg();
        tc.init_data = (moq_bytes_t){ .data = NULL, .len = 5 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
    }

    /* -- add_track enforces max_track_config_bytes -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_track_config_bytes = 16;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t big[20];
        memset(big, 0xAA, sizeof(big));

        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = big, .len = 20 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);

        tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = big, .len = 10 };
        tc.init_data = (moq_bytes_t){ .data = big, .len = 10 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);

        tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = big, .len = 8 };
        tc.init_data = (moq_bytes_t){ .data = big, .len = 8 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_destroy(pb);
    }

    /* -- add_track NULL args ----------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);
        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;

        CHECK(moq_playback_add_track(NULL, &tc, &t) == MOQ_ERR_INVAL);
        CHECK(moq_playback_add_track(pb, NULL, &t) == MOQ_ERR_INVAL);
        CHECK(moq_playback_add_track(pb, &tc, NULL) == MOQ_ERR_INVAL);

        moq_playback_destroy(pb);
    }

    /* -- add_track empty codec/init_data is OK ----------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(moq_playback_test_track_codec(pb, t) == NULL);
        CHECK(moq_playback_test_track_init_data(pb, t) == NULL);

        moq_playback_destroy(pb);
    }

    /* -- add_track accepts CMAF packaging ----------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_destroy(pb);
    }

    /* -- OOM during codec copy: no leak ------------------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        cfg.max_buffered_objects = 1;

        /* create uses 6 allocs. codec copy is alloc #7. */
        oom_state_t oom = { 0, 0, 7 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        uint8_t codec[] = "avc1";
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec, .len = 4 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_NOMEM);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* -- OOM during init_data copy: releases codec, no leak ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        cfg.max_buffered_objects = 1;

        /* create uses 6. codec copy #7 (succeeds). init_data copy #8. */
        oom_state_t oom = { 0, 0, 8 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        uint8_t codec[] = "avc1";
        uint8_t init[] = { 0x00, 0x01 };
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec, .len = 4 };
        tc.init_data = (moq_bytes_t){ .data = init, .len = 2 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_NOMEM);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /*  PB2a: remove_track tests                                       */
    /* ================================================================ */

    /* -- remove_track releases config refs --------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t codec[] = "avc1";
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec, .len = 4 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *stored = moq_playback_test_track_codec(pb, t);
        CHECK(stored != NULL);
        moq_rcbuf_incref(stored);
        CHECK(moq_rcbuf_refcount(stored) == 2);

        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(stored) == 1);
        moq_rcbuf_decref(stored);

        moq_playback_destroy(pb);
    }

    /* -- remove_track makes handle stale ----------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);

        CHECK(moq_playback_remove_track(pb, t) == MOQ_ERR_STALE_HANDLE);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.payload = NULL;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_STALE_HANDLE);

        moq_playback_destroy(pb);
    }

    /* -- remove_track NULL pb ---------------------------------------- */
    {
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_remove_track(NULL, t) == MOQ_ERR_INVAL);
    }

    /* -- stale after slot reuse (max_tracks=1) ----------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t1 = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t1) == MOQ_OK);
        CHECK(moq_playback_remove_track(pb, t1) == MOQ_OK);

        moq_playback_track_t t2 = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t2) == MOQ_OK);

        /* Old handle must not remove/push to new track. */
        CHECK(moq_playback_remove_track(pb, t1) == MOQ_ERR_STALE_HANDLE);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t1;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_STALE_HANDLE);
        CHECK(moq_rcbuf_refcount(p) == 1); /* not retained */

        /* New handle still works. */
        obj.track = t2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- cross-playback: track from pb1 rejected by pb2 -------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb1 = make_pb(alloc, &cfg);
        moq_playback_t *pb2 = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t1 = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb1, &tc, &t1) == MOQ_OK);

        CHECK(moq_playback_remove_track(pb2, t1) == MOQ_ERR_STALE_HANDLE);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t1;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb2, &obj, 0) == MOQ_ERR_STALE_HANDLE);
        CHECK(moq_rcbuf_refcount(p) == 1);

        /* Original pb1 track still intact. */
        obj.track = t1;
        CHECK(moq_playback_push_object(pb1, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p);
        moq_playback_destroy(pb1);
        moq_playback_destroy(pb2);
    }

    /* ================================================================ */
    /*  PB2a: push_object tests                                        */
    /* ================================================================ */

    /* -- push_object retains payload exactly once -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0xDE, 0xAD };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &payload) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 2);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- push_object does not retain properties ---------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- push_object rejects NULL payload for NORMAL status ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_NORMAL;
        obj.payload = NULL;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_INVAL);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_destroy(pb);
    }

    /* -- push_object accepts NULL payload for terminal status --------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        obj.payload = NULL;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 2;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_END_OF_TRACK;
        obj.payload = NULL;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 2);

        moq_playback_destroy(pb);
    }

    /* -- push_object object-count full WOULD_BLOCK ------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_objects = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p1 = NULL, *p2 = NULL, *p3 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p3) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.payload = p1;
        obj.properties = props;
        obj.group_id = 1; obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        obj.payload = p2;
        obj.object_id = 1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        obj.payload = p3;
        obj.object_id = 2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_rcbuf_refcount(p3) == 1);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(p2);
        moq_rcbuf_decref(p3);
        moq_playback_destroy(pb);
    }

    /* -- push_object byte-budget full WOULD_BLOCK -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_bytes = 4;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data3[] = { 0x01, 0x02, 0x03 };
        uint8_t data2[] = { 0x04, 0x05 };
        moq_rcbuf_t *big = NULL, *small = NULL;
        CHECK(moq_rcbuf_create(alloc, data3, 3, &big) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data2, 2, &small) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.payload = big;
        obj.properties = props;
        obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 3);

        obj.payload = small;
        obj.object_id = 1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_rcbuf_refcount(small) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 3);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(big);
        moq_rcbuf_decref(small);
        moq_playback_destroy(pb);
    }

    /* -- push_object duplicate: refcount unchanged ------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0xBE, 0xEF };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 5;
        obj.object_id = 3;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        moq_rcbuf_t *payload2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &payload2) == MOQ_OK);
        obj.payload = payload2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload2) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(payload2);
        moq_playback_destroy(pb);
    }

    /* -- push_object NULL args --------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        CHECK(moq_playback_push_object(NULL, NULL, 0) == MOQ_ERR_INVAL);
        CHECK(moq_playback_push_object(pb, NULL, 0) == MOQ_ERR_INVAL);

        moq_playback_destroy(pb);
    }

    /* -- push_object bad struct_size --------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.struct_size = 4;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_INVAL);

        moq_playback_destroy(pb);
    }

    /* -- push_object with stale handle returns STALE_HANDLE ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_STALE_HANDLE);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- remove_track releases buffered payload refs ----------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x11, 0x22, 0x33 };
        moq_rcbuf_t *p1 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 3, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 3, &p2) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.payload = p1;
        obj.properties = props;
        obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        obj.payload = p2;
        obj.object_id = 1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_rcbuf_refcount(p1) == 2);
        CHECK(moq_rcbuf_refcount(p2) == 2);
        CHECK(moq_playback_test_retained_bytes(pb) == 6);

        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p1) == 1);
        CHECK(moq_rcbuf_refcount(p2) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(p2);
        moq_playback_destroy(pb);
    }

    /* -- destroy releases buffered payload refs and track config ------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t codec[] = "avc1";
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec, .len = 4 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *stored_codec = moq_playback_test_track_codec(pb, t);
        moq_rcbuf_incref(stored_codec);
        CHECK(moq_rcbuf_refcount(stored_codec) == 2);

        uint8_t data[] = { 0xAA };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);

        CHECK(moq_rcbuf_refcount(stored_codec) == 1);
        CHECK(moq_rcbuf_refcount(payload) == 1);
        moq_rcbuf_decref(stored_codec);
        moq_rcbuf_decref(payload);
    }

    /* ================================================================ */
    /*  PB2a: live-join anchor tests                                    */
    /* ================================================================ */

    /* -- first object at group 500/object 3 anchors there ------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(!moq_playback_test_track_anchored(pb, t));

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 500;
        obj.object_id = 3;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_test_track_anchored(pb, t));
        CHECK(moq_playback_test_anchor_group(pb, t) == 500);
        CHECK(moq_playback_test_anchor_object(pb, t) == 3);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- second object does not re-anchor ----------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p1 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.payload = p1;
        obj.properties = props;
        obj.group_id = 100;
        obj.object_id = 5;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        obj.payload = p2;
        obj.group_id = 200;
        obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_test_anchor_group(pb, t) == 100);
        CHECK(moq_playback_test_anchor_object(pb, t) == 5);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(p2);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB2a: multi-track isolation test                                */
    /* ================================================================ */

    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t1 = MOQ_PLAYBACK_TRACK_INVALID;
        moq_playback_track_t t2 = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t1) == MOQ_OK);
        CHECK(moq_playback_add_track(pb, &tc, &t2) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p1 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.payload = p1;
        obj.properties = props;
        obj.track = t1;
        obj.group_id = 1; obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        obj.payload = p2;
        obj.track = t2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_test_buffered_count(pb) == 2);

        CHECK(moq_playback_remove_track(pb, t1) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p1) == 1);
        CHECK(moq_rcbuf_refcount(p2) == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(p2);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB2a: OOM balance for full lifecycle                            */
    /* ================================================================ */

    {
        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 2;
        cfg.max_buffered_objects = 4;

        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        uint8_t codec[] = "avc1";
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec, .len = 4 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01, 0x02 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(&fa, data, 2, &payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /*  PB2a amendment: duplicate at full buffer/byte budget            */
    /* ================================================================ */

    /* -- duplicate at full object buffer returns OK ------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_objects = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p1 = NULL, *p2 = NULL, *p_dup = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p_dup) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.payload = p1;
        obj.properties = props;
        obj.group_id = 1; obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        obj.payload = p2;
        obj.object_id = 1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 2);

        /* Buffer is full. A new object would WOULD_BLOCK. */
        obj.payload = p_dup;
        obj.object_id = 2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WOULD_BLOCK);

        /* But a duplicate of an existing object returns OK. */
        obj.payload = p_dup;
        obj.object_id = 0; /* same as p1 */
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p_dup) == 1); /* not retained */
        CHECK(moq_playback_test_buffered_count(pb) == 2);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(p2);
        moq_rcbuf_decref(p_dup);
        moq_playback_destroy(pb);
    }

    /* -- duplicate at full byte budget returns OK -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_bytes = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data2[] = { 0x01, 0x02 };
        uint8_t data1[] = { 0x03 };
        moq_rcbuf_t *big = NULL, *dup_payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data2, 2, &big) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data1, 1, &dup_payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.payload = big;
        obj.properties = props;
        obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_test_retained_bytes(pb) == 2);

        /* Budget is full. A new object WOULD_BLOCK. */
        obj.payload = dup_payload;
        obj.object_id = 1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Duplicate of existing returns OK despite full budget. */
        obj.payload = dup_payload;
        obj.object_id = 0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(dup_payload) == 1);
        CHECK(moq_playback_test_retained_bytes(pb) == 2);

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(big);
        moq_rcbuf_decref(dup_payload);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB2a amendment: creation tag changes across creates             */
    /* ================================================================ */

    /* -- two playback instances get different creation tags ----------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb1 = make_pb(alloc, &cfg);
        moq_playback_t *pb2 = make_pb(alloc, &cfg);
        CHECK(pb1 != NULL);
        CHECK(pb2 != NULL);

        uint16_t tag1 = moq_playback_test_creation_tag(pb1);
        uint16_t tag2 = moq_playback_test_creation_tag(pb2);
        CHECK(tag1 != 0);
        CHECK(tag2 != 0);
        CHECK(tag1 != tag2);

        moq_playback_destroy(pb1);
        moq_playback_destroy(pb2);
    }

    /* -- destroy+recreate yields different tag (address reuse safe) --- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb1 = make_pb(alloc, &cfg);
        uint16_t tag1 = moq_playback_test_creation_tag(pb1);
        moq_playback_destroy(pb1);

        moq_playback_t *pb2 = make_pb(alloc, &cfg);
        uint16_t tag2 = moq_playback_test_creation_tag(pb2);
        CHECK(tag1 != tag2);
        moq_playback_destroy(pb2);
    }

    /* -- different generation seeds across creates --------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);

        moq_playback_t *pb1 = make_pb(alloc, &cfg);
        moq_playback_t *pb2 = make_pb(alloc, &cfg);

        uint32_t seed1 = moq_playback_test_generation_seed(pb1);
        uint32_t seed2 = moq_playback_test_generation_seed(pb2);
        CHECK(seed1 != 0);
        CHECK(seed2 != 0);
        CHECK((seed1 & 1u) == 1); /* odd */
        CHECK((seed2 & 1u) == 1); /* odd */
        CHECK(seed1 != seed2);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t h1 = MOQ_PLAYBACK_TRACK_INVALID;
        moq_playback_track_t h2 = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb1, &tc, &h1) == MOQ_OK);
        CHECK(moq_playback_add_track(pb2, &tc, &h2) == MOQ_OK);
        CHECK(h1._opaque != h2._opaque);

        moq_playback_destroy(pb1);
        moq_playback_destroy(pb2);
    }

    /* ================================================================ */
    /*  PB2a amendment: invalid status validation                       */
    /* ================================================================ */

    /* -- push_object rejects invalid status value -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.status = (moq_object_status_t)99;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        /* Status 3 (undefined) should also be rejected. */
        obj.status = (moq_object_status_t)3;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB3a: RAW + LOC parse and in-order decode release              */
    /* ================================================================ */

    /* -- RAW push+tick emits CONFIGURE then DECODE with exact fields -- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t codec_bytes[] = "avc1.42e01e";
        uint8_t init_bytes[] = { 0xDE, 0xAD };
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec_bytes, .len = 11 };
        tc.init_data = (moq_bytes_t){ .data = init_bytes, .len = 2 };
        tc.width = 1920;
        tc.height = 1080;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t frame[] = { 0x00, 0x00, 0x01, 0x65 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frame, 4, &payload) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000000, true, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = payload;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);
        CHECK(moq_rcbuf_refcount(props) == 1);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* First command: CONFIGURE_VIDEO */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.struct_size == sizeof(moq_playback_cmd_t));
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        CHECK(cmd.track._opaque == t._opaque);
        CHECK(cmd.u.configure_video.width == 1920);
        CHECK(cmd.u.configure_video.height == 1080);
        CHECK(cmd.u.configure_video.codec != NULL);
        CHECK(cmd.u.configure_video.codec_config != NULL);
        moq_playback_cmd_cleanup(&cmd);

        /* Second command: DECODE_VIDEO */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.struct_size == sizeof(moq_playback_cmd_t));
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.track._opaque == t._opaque);
        CHECK(cmd.u.decode_video.group_id == 1);
        CHECK(cmd.u.decode_video.object_id == 0);
        CHECK(cmd.u.decode_video.decode_time_us == 1000000);
        CHECK(cmd.u.decode_video.composition_offset_us == 0);
        CHECK(cmd.u.decode_video.presentation_time_us == 1000000);
        CHECK(cmd.u.decode_video.has_capture_time == true);
        CHECK(cmd.u.decode_video.capture_time_us == 1000000);
        CHECK(cmd.u.decode_video.keyframe == true);
        CHECK(cmd.u.decode_video.payload == payload);

        /* Payload ref transferred to command; buffer released it. */
        CHECK(moq_rcbuf_refcount(payload) == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);

        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- zero-copy: push increfs, tick transfers, cleanup decrefs ----- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0xAA };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 5000, true, &props);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* CONFIGURE */
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* DECODE */
        CHECK(cmd.u.decode_video.payload == p);
        CHECK(moq_rcbuf_refcount(p) == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- out-of-order arrival: tick releases only contiguous prefix --- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL, *props1 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);
        MAKE_LOC_PROPS(alloc, 2000, false, &props1);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;

        /* Push object 0, then 2 (skip 1). */
        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        obj.object_id = 2; obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Should release object 0 only (CONFIGURE + DECODE). */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 1); /* obj 2 held */

        /* Now push object 1. */
        obj.object_id = 1; obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Should release objects 1 and 2. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 1);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props1); moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- live join at group 500/object 3: releases without expecting 0..2 */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 9000, true, &props);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 500;
        obj.object_id = 3;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 500);
        CHECK(cmd.u.decode_video.object_id == 3);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- missing timestamp: OBJECT_DROPPED event, nothing retained --- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = NULL; /* no LOC properties */
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_rcbuf_refcount(p) == 1); /* not retained */
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP);
        CHECK(evt.u.object_dropped.group_id == 1);
        CHECK(evt.u.object_dropped.object_id == 0);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- malformed LOC: OBJECT_DROPPED event, nothing retained ------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        /* Construct malformed properties: truncated varint. */
        uint8_t bad_props[] = { 0xFF };
        moq_rcbuf_t *props = NULL;
        CHECK(moq_rcbuf_create(alloc, bad_props, 1, &props) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 2;
        obj.object_id = 5;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_MALFORMED_LOC);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- event queue full during drop: WOULD_BLOCK commit-last ------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* Fill event queue. */
        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = NULL; /* missing timestamp -> drop -> event full */
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- command queue full before release: WOULD_BLOCK commit-last --- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Fill 1 of 2 command slots. CONFIGURE+DECODE needs 2 → WOULD_BLOCK. */
        moq_playback_cmd_t filler;
        memset(&filler, 0, sizeof(filler));
        filler.kind = MOQ_PLAYBACK_CMD_RESET;
        CHECK(moq_playback_test_push_cmd(pb, &filler) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        /* Drain filler, retry: both CONFIGURE + DECODE fit. */
        moq_playback_cmd_t polled;
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&polled);
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        moq_playback_cmd_cleanup(&polled);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- CONFIGURE_VIDEO cleanup releases owned refs ----------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t codec[] = "avc1";
        uint8_t init[] = { 0x01, 0x02 };
        moq_playback_track_cfg_t tc = make_track_cfg();
        tc.codec = (moq_bytes_t){ .data = codec, .len = 4 };
        tc.init_data = (moq_bytes_t){ .data = init, .len = 2 };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *stored_codec = moq_playback_test_track_codec(pb, t);
        moq_rcbuf_t *stored_init = moq_playback_test_track_init_data(pb, t);
        CHECK(moq_rcbuf_refcount(stored_codec) == 1);
        CHECK(moq_rcbuf_refcount(stored_init) == 1);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* CONFIGURE_VIDEO increfed track config. */
        CHECK(moq_rcbuf_refcount(stored_codec) == 2);
        CHECK(moq_rcbuf_refcount(stored_init) == 2);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_rcbuf_refcount(stored_codec) == 1);
        CHECK(moq_rcbuf_refcount(stored_init) == 1);

        /* Drain DECODE too. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- max_release_per_tick bounds decode command emission ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_release_per_tick = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props[4];
        moq_rcbuf_t *payloads[4];
        uint8_t data[] = { 0x01 };

        for (int i = 0; i < 4; i++) {
            MAKE_LOC_PROPS(alloc, (uint64_t)(1000 * (i + 1)), i == 0, &props[i]);
            CHECK(moq_rcbuf_create(alloc, data, 1, &payloads[i]) == MOQ_OK);

            moq_playback_object_t obj;
            moq_playback_object_init(&obj);
            obj.track = t;
            obj.group_id = 1;
            obj.object_id = (uint64_t)i;
            obj.payload = payloads[i];
            obj.properties = props[i];
            CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        }

        /* First tick: max 2 releases → CONFIGURE + DECODE(0) + DECODE(1). */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 1);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 2);

        /* Second tick: releases objects 2 and 3. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.u.decode_video.object_id == 2);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.u.decode_video.object_id == 3);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        for (int i = 0; i < 4; i++) {
            moq_rcbuf_decref(payloads[i]);
            moq_rcbuf_decref(props[i]);
        }
        moq_playback_destroy(pb);
    }

    /* -- non-keyframe without VFM: keyframe=false, not malformed ------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* First push a keyframe to exit keyframe-waiting state. */
        moq_rcbuf_t *kf_props = NULL;
        MAKE_LOC_PROPS(alloc, 5000, true, &kf_props);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *kf_p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &kf_p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = kf_p;
        obj.properties = kf_props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Second object: LOC with timestamp but no VFM (non-keyframe). */
        moq_loc_headers_t lh;
        moq_loc_headers_init(&lh);
        lh.has_timestamp = true;
        lh.timestamp = 7000;
        moq_rcbuf_t *props = NULL;
        CHECK(moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &props) == MOQ_OK);

        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 1;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* CONFIGURE */
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* DECODE (keyframe) */
        CHECK(cmd.u.decode_video.keyframe == true);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* DECODE (non-kf) */
        CHECK(cmd.u.decode_video.keyframe == false);
        CHECK(cmd.u.decode_video.decode_time_us == 7000);
        CHECK(cmd.u.decode_video.object_id == 1);
        moq_playback_cmd_cleanup(&cmd);

        /* No OBJECT_DROPPED event. */
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(kf_p);
        moq_rcbuf_decref(kf_props);
        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB3b: END_OF_GROUP / END_OF_TRACK / partial abandon            */
    /* ================================================================ */

    /* -- normal end_of_group=true releases DECODE and advances group -- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props_g2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 2000, false, &props_g2);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p0;
        obj.properties = props0;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Push group 2 object 0. */
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 2;
        obj.object_id = 0;
        obj.payload = p1;
        obj.properties = props_g2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 1);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        /* Group advanced: object from group 2 released. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 2);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(props0);
        moq_rcbuf_decref(props_g2);
        moq_playback_destroy(pb);
    }

    /* -- END_OF_GROUP marker advances group with no DECODE ------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p0;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* END_OF_GROUP marker at object 1. */
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 1;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 1);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        /* No DECODE for the END_OF_GROUP marker. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        CHECK(moq_playback_test_anchor_group(pb, t) == 2);
        CHECK(moq_playback_test_anchor_object(pb, t) == 0);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- normal end_of_group drops stranded later same-group object --- */
    /* o0 closes group 1 via end_of_group=true; o1 (same group, higher id)
     * is already buffered. After advancing past group 1, o1 must be dropped,
     * not stranded; o1 must not stay buffered forever. */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props1 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 1500, false, &props1);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p0; obj.properties = props0;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Same group, higher object id — arrives before the closer is ticked. */
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 1;
        obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p1) == 2);   /* caller + buffer */

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);
        /* No decode for o1 — it was a stranded leftover, dropped not decoded. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(p1) == 1);   /* dropped → buffer ref gone */
        CHECK(moq_playback_test_anchor_group(pb, t) == 2);
        CHECK(moq_playback_test_anchor_object(pb, t) == 0);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(props0);
        moq_rcbuf_decref(props1);
        moq_playback_destroy(pb);
    }

    /* -- END_OF_GROUP marker drops stranded later same-group object --- */
    /* o0 normal, o1 END_OF_GROUP marker, o2 same-group later object. The
     * marker closes group 1; o2 must be dropped on advance. (RED: before
     * the fix o2 stays buffered.) */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 2000, false, &props2);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 1;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 2;
        obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p2) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);
        /* No decode for the marker, and none for stranded o2. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(p2) == 1);
        CHECK(moq_playback_test_anchor_group(pb, t) == 2);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0);
        moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- group-close stranding cannot exhaust the buffer (DoS guard) --- */
    /* Repeating close+stranded-leftover must not accumulate: the leftover is
     * dropped each tick, so the bounded buffer never fills and every new
     * group's object is accepted (otherwise the strands pile up and a later
     * push returns MOQ_ERR_WOULD_BLOCK). */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_buffered_objects = 4;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        for (uint64_t g = 1; g <= 8; g++) {
            moq_rcbuf_t *pa = NULL, *pb_strand = NULL, *pra = NULL, *prb = NULL;
            MAKE_LOC_PROPS(alloc, g * 1000, true, &pra);
            MAKE_LOC_PROPS(alloc, g * 1000 + 1, false, &prb);
            CHECK(moq_rcbuf_create(alloc, data, 1, &pa) == MOQ_OK);
            CHECK(moq_rcbuf_create(alloc, data, 1, &pb_strand) == MOQ_OK);

            moq_playback_object_t obj;
            moq_playback_object_init(&obj);
            obj.track = t; obj.group_id = g; obj.object_id = 0;
            obj.payload = pa; obj.properties = pra;
            obj.end_of_group = true;
            CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

            moq_playback_object_init(&obj);
            obj.track = t; obj.group_id = g; obj.object_id = 1;
            obj.payload = pb_strand; obj.properties = prb;
            /* The stranded o1 from prior groups must not accumulate and fill
             * the 4-slot buffer; this push must succeed. */
            CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

            CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
            CHECK(moq_playback_test_buffered_count(pb) == 0);

            moq_playback_cmd_t cmd;
            while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
                moq_playback_cmd_cleanup(&cmd);

            moq_rcbuf_decref(pa);
            moq_rcbuf_decref(pb_strand);
            moq_rcbuf_decref(pra);
            moq_rcbuf_decref(prb);
        }

        moq_playback_destroy(pb);
    }

    /* -- group close commit-last: no drop when decode can't emit ------ */
    /* If the closing object cannot emit CONFIGURE+DECODE (command queue
     * full), tick returns WOULD_BLOCK BEFORE advancing, so neither the
     * closer nor the stranded same-group object is dropped. After draining,
     * a retry decodes the closer and drops the leftover. */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props1 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 1500, false, &props1);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p0; obj.properties = props0;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 1;
        obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Pre-fill one of two command slots: CONFIGURE+DECODE (2) won't fit. */
        moq_playback_cmd_t filler;
        memset(&filler, 0, sizeof(filler));
        filler.kind = MOQ_PLAYBACK_CMD_RESET;
        CHECK(moq_playback_test_push_cmd(pb, &filler) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        /* Nothing advanced or dropped: both objects still buffered. */
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        CHECK(moq_rcbuf_refcount(p1) == 2);
        CHECK(moq_playback_test_anchor_group(pb, t) == 1);

        /* Drain and retry: now the closer decodes and the leftover drops. */
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(p1) == 1);
        CHECK(moq_playback_test_anchor_group(pb, t) == 2);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(props0);
        moq_rcbuf_decref(props1);
        moq_playback_destroy(pb);
    }

    /* -- partial group: objects 0 and 2, object 2 closes group ------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;

        /* Object 0 normal. */
        obj.object_id = 0;
        obj.payload = p0;
        obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Object 2 normal with end_of_group (object 1 missing). */
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 2;
        obj.payload = p2;
        obj.properties = props2;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Object 0 released as DECODE. */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        /* Object 1 missing, group is closed → PARTIAL_GROUP_ABANDONED. */
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);
        CHECK(evt.u.partial_group_abandoned.from_group_id == 1);
        CHECK(evt.u.partial_group_abandoned.to_group_id == 1);

        /* Object 2 dropped, refs released. */
        CHECK(moq_rcbuf_refcount(p2) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);

        /* Advanced to group 2. */
        CHECK(moq_playback_test_anchor_group(pb, t) == 2);
        CHECK(moq_playback_test_anchor_object(pb, t) == 0);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0);
        moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- partial abandon event-full: WOULD_BLOCK commit-last --------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 1;
        cfg.max_commands = 16;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* Push normal object 0 (anchors at 0), then END_OF_GROUP at 2
           (gap at object 1 → partial abandon). */
        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p0;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 2;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Fill event queue. */
        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);

        /* Tick releases object 0, then WOULD_BLOCK on PARTIAL_GROUP_ABANDONED. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Object 0 was released (CONFIGURE + DECODE queued), but marker
           still buffered and group not advanced. */
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_playback_test_anchor_group(pb, t) == 1);

        /* Drain event + commands, retry. */
        moq_playback_event_t polled_evt;
        CHECK(moq_playback_poll_event(pb, &polled_evt) == MOQ_OK);

        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_event(pb, &polled_evt) == MOQ_OK);
        CHECK(polled_evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_anchor_group(pb, t) == 2);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- END_OF_TRACK emits TRACK_ENDED; later push returns WRONG_STATE */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_END_OF_TRACK;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_TRACK_ENDED);
        CHECK(evt.track._opaque == t._opaque);

        CHECK(moq_playback_test_buffered_count(pb) == 0);

        /* Push on ended track. */
        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 5000, false, &props);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 2;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WRONG_STATE);
        CHECK(moq_rcbuf_refcount(p) == 1);

        /* remove_track still works on ended track. */
        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- END_OF_TRACK drops already-buffered later objects ------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* Push END_OF_TRACK at object 0 (anchors there). */
        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_END_OF_TRACK;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Push a later normal object for the same track. */
        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 2000, false, &props);
        uint8_t data[] = { 0x01, 0x02 };
        moq_rcbuf_t *later = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &later) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 1;
        obj.payload = later;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(later) == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        CHECK(moq_playback_test_retained_bytes(pb) == 2);

        /* Event-full commit-last: both objects stay buffered. */
        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 2);
        CHECK(moq_rcbuf_refcount(later) == 2);

        /* Drain event, retry tick. */
        moq_playback_event_t polled;
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* TRACK_ENDED emitted. */
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        CHECK(polled.struct_size == sizeof(moq_playback_event_t));
        CHECK(polled.kind == MOQ_PLAYBACK_EVENT_TRACK_ENDED);

        /* Both marker and later object released. */
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);
        CHECK(moq_rcbuf_refcount(later) == 1);

        /* Push after ended. */
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 2;
        obj.object_id = 0;
        obj.payload = later;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_WRONG_STATE);

        moq_rcbuf_decref(later);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- terminal status with payload returns INVAL, no retain ------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        obj.status = MOQ_OBJECT_END_OF_TRACK;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- max_release_per_tick bounds terminal consumption ------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_release_per_tick = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        /* Object 0 normal, object 1 END_OF_GROUP marker. */
        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 1;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Tick 1: max_release=1, releases object 0 only. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        /* Marker still buffered. */
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        /* Tick 2: consumes END_OF_GROUP marker. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_anchor_group(pb, t) == 2);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB4a: CMAF support                                             */
    /* ================================================================ */

    /* -- CMAF track with init_data: CONFIGURE + DECODE_CMAF ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t avcc[] = { 0x01, 0x42, 0xE0, 0x1E };
        uint8_t init_seg[512];
        size_t init_len = build_avc_init(init_seg, 90000, 1920, 1080,
                                          avcc, sizeof(avcc));

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.codec = (moq_bytes_t){ .data = (const uint8_t *)"avc1", .len = 4 };
        tc.init_data = (moq_bytes_t){ .data = init_seg, .len = init_len };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0xCA, 0xFE };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 180000, 3000, 2,
                                               0x00000000, 1000, mdat, 2);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.struct_size == sizeof(moq_playback_cmd_t));
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        CHECK(cmd.u.configure_video.width == 1920);
        CHECK(cmd.u.configure_video.height == 1080);
        CHECK(cmd.u.configure_video.codec != NULL);
        CHECK(moq_rcbuf_len(cmd.u.configure_video.codec) == 4);
        CHECK(memcmp(moq_rcbuf_data(cmd.u.configure_video.codec), "avc1", 4) == 0);
        CHECK(cmd.u.configure_video.codec_config != NULL);
        CHECK(moq_rcbuf_len(cmd.u.configure_video.codec_config) == sizeof(avcc));
        CHECK(memcmp(moq_rcbuf_data(cmd.u.configure_video.codec_config),
                     avcc, sizeof(avcc)) == 0);
        moq_playback_cmd_cleanup(&cmd);

        /* Raw init_data released after CMAF parse; only codec_config kept. */
        CHECK(moq_playback_test_track_init_data(pb, t) == NULL);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.struct_size == sizeof(moq_playback_cmd_t));
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.group_id == 1);
        CHECK(cmd.u.decode_cmaf.object_id == 0);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 2000000);
        CHECK(cmd.u.decode_cmaf.composition_offset_us == 11111);
        CHECK(cmd.u.decode_cmaf.presentation_time_us == 2000000 + 11111);
        CHECK(cmd.u.decode_cmaf.sample_duration_us == 33333);
        CHECK(cmd.u.decode_cmaf.keyframe == true);
        CHECK(cmd.u.decode_cmaf.fragment == payload);
        CHECK(cmd.u.decode_cmaf.mdat_len == 2);
        CHECK(cmd.u.decode_cmaf.mdat_offset > 0);

        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_playback_test_retained_bytes(pb) == 0);

        CHECK(moq_rcbuf_refcount(payload) == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- CMAF refcount proof: push increfs, tick transfers, cleanup decrefs */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 1000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 1000, 33, 1,
                                               0x00000000, 0, mdat, 1);
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &p) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* CONFIGURE */
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK); /* DECODE_CMAF */
        CHECK(cmd.u.decode_cmaf.fragment == p);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- CMAF malformed fragment -> OBJECT_DROPPED -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t bad[] = { 0x00, 0x00, 0x00, 0x01, 'b', 'a', 'd', '!' };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, bad, 8, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- CMAF multi-sample overflow -> OBJECT_DROPPED ----------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_samples_per_object = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01, 0x02 };
        uint8_t frag_buf[512];
        size_t frag_len = build_multi_sample_frag(frag_buf, 90000, 3000, 2,
                                                   mdat, 2);
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason ==
              MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- CMAF video 3-sample: emits 3 DECODE_CMAF with exact offsets -- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
        uint8_t frag_buf[512];
        size_t frag_len;
        {
            size_t p = 0;
            uint32_t trun_flags = 0x100 | 0x200 | 0x400;
            size_t trun_sz = 8 + 8 + 3 * 12;
            size_t tfdt_sz = 8 + 4 + 8;
            size_t tfhd_sz = 8 + 8;
            size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
            size_t moof_sz = 8 + traf_sz;
            wr32(frag_buf+p,(uint32_t)moof_sz); memcpy(frag_buf+p+4,"moof",4); p+=8;
            wr32(frag_buf+p,(uint32_t)traf_sz); memcpy(frag_buf+p+4,"traf",4); p+=8;
            wr32(frag_buf+p,(uint32_t)tfhd_sz); memcpy(frag_buf+p+4,"tfhd",4); p+=8;
            wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,1); p+=4;
            wr32(frag_buf+p,(uint32_t)tfdt_sz); memcpy(frag_buf+p+4,"tfdt",4); p+=8;
            wr32(frag_buf+p,0x01000000); p+=4;
            wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,0); p+=4;
            wr32(frag_buf+p,(uint32_t)trun_sz); memcpy(frag_buf+p+4,"trun",4); p+=8;
            wr32(frag_buf+p,trun_flags); p+=4; wr32(frag_buf+p,3); p+=4;
            /* sample 0: dur=3000, size=2, flags=0x02000000 (keyframe) */
            wr32(frag_buf+p,3000); p+=4; wr32(frag_buf+p,2); p+=4;
            wr32(frag_buf+p,0x02000000); p+=4;
            /* sample 1: dur=3000, size=3, flags=0x01000000 (non-key) */
            wr32(frag_buf+p,3000); p+=4; wr32(frag_buf+p,3); p+=4;
            wr32(frag_buf+p,0x01000000); p+=4;
            /* sample 2: dur=3000, size=1, flags=0x01000000 (non-key) */
            wr32(frag_buf+p,3000); p+=4; wr32(frag_buf+p,1); p+=4;
            wr32(frag_buf+p,0x01000000); p+=4;
            wr32(frag_buf+p,(uint32_t)(8+6)); memcpy(frag_buf+p+4,"mdat",4); p+=8;
            memcpy(frag_buf+p,mdat,6); p+=6;
            frag_len = p;
        }

        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 4); /* 3 cmds + 1 caller */
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        /* Sample 0: keyframe, size=2 */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 0);
        CHECK(cmd.u.decode_cmaf.keyframe == true);
        CHECK(cmd.u.decode_cmaf.mdat_len == 2);
        size_t s0_off = cmd.u.decode_cmaf.mdat_offset;
        moq_playback_cmd_cleanup(&cmd);

        /* Sample 1: non-key, size=3 */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 33333);
        CHECK(cmd.u.decode_cmaf.keyframe == false);
        CHECK(cmd.u.decode_cmaf.mdat_len == 3);
        CHECK(cmd.u.decode_cmaf.mdat_offset == s0_off + 2);
        moq_playback_cmd_cleanup(&cmd);

        /* Sample 2: non-key, size=1 */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 66666);
        CHECK(cmd.u.decode_cmaf.keyframe == false);
        CHECK(cmd.u.decode_cmaf.mdat_len == 1);
        CHECK(cmd.u.decode_cmaf.mdat_offset == s0_off + 5);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- CMAF command queue full -> WOULD_BLOCK commit-last ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 1000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 1000, 33, 1,
                                               0x00000000, 0, mdat, 1);
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Fill 1 of 2 slots. CONFIGURE+DECODE needs 2 → WOULD_BLOCK. */
        moq_playback_cmd_t filler;
        memset(&filler, 0, sizeof(filler));
        filler.kind = MOQ_PLAYBACK_CMD_RESET;
        CHECK(moq_playback_test_push_cmd(pb, &filler) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        moq_playback_cmd_t polled;
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&polled);
        CHECK(moq_playback_poll_command(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        moq_playback_cmd_cleanup(&polled);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- CMAF END_OF_GROUP/END_OF_TRACK works ------------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 1000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 1000, 33, 1,
                                               0x00000000, 0, mdat, 1);
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 2;
        obj.object_id = 0;
        obj.status = MOQ_OBJECT_END_OF_TRACK;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.group_id == 1);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_TRACK_ENDED);

        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- add_track malformed init_data -> PROTO, no leak ------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        uint8_t bad_init[] = { 0x00, 0x00, 0x00, 0x08, 'b', 'a', 'd', '!' };
        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.init_data = (moq_bytes_t){ .data = bad_init, .len = sizeof(bad_init) };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_PROTO);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* -- add_track CMAF init_data OOM on codec_config copy ----------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        cfg.max_buffered_objects = 1;

        uint8_t avcc[] = { 0x01, 0x42 };
        uint8_t init_seg[512];
        size_t init_len = build_avc_init(init_seg, 90000, 320, 240,
                                          avcc, sizeof(avcc));

        /* create=6 allocs, codec_config copy=#7 (fail) */
        oom_state_t oom = { 0, 0, 7 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.init_data = (moq_bytes_t){ .data = init_seg, .len = init_len };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_NOMEM);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* -- CMAF init larger than budget is REJECTED (codec+init cap) ----- */
    /* The documented per-track codec+init cap applies to the raw init_data
     * for CMAF too: a valid init segment larger than the budget must be
     * rejected by the cap, not parsed -- the oversized buffer must never reach
     * the parser. */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_track_config_bytes = 16;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t avcc[] = { 0x01, 0x42 };
        uint8_t init_seg[512];
        size_t init_len = build_avc_init(init_seg, 90000, 320, 240,
                                          avcc, sizeof(avcc));
        CHECK(init_len > 16);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.init_data = (moq_bytes_t){ .data = init_seg, .len = init_len };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
    }

    /* -- Oversized MALFORMED CMAF init: cap fires BEFORE the parser ---- */
    /* The size cap must reject before moq_cmaf_parse_init() runs, so an
     * oversized buffer is INVAL (cap) — not PROTO (parser verdict on the bytes). */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_track_config_bytes = 16;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t junk[64];
        memset(junk, 0xCC, sizeof(junk));   /* not a valid MP4 init segment */

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.init_data = (moq_bytes_t){ .data = junk, .len = sizeof(junk) };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_INVAL);
        CHECK(!handle_valid(t));

        moq_playback_destroy(pb);
    }

    /* -- Positive: CMAF init within budget still parses + configures --- */
    /* A valid CMAF init whose length fits the cap (codec.len + init_data.len
     * <= budget, here at the exact boundary) must still succeed AND derive
     * configure metadata from the parsed init segment. */
    {
        uint8_t avcc[] = { 0x01, 0x42 };
        uint8_t init_seg[512];
        size_t init_len = build_avc_init(init_seg, 90000, 320, 240,
                                          avcc, sizeof(avcc));

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_track_config_bytes = (uint32_t)init_len;   /* exact boundary */
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.init_data = (moq_bytes_t){ .data = init_seg, .len = init_len };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(handle_valid(t));

        /* Push a fragment so the configure command emits, proving the
         * in-budget init was parsed (width/height derived from it). */
        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 0, 3000, 1,
                                               0x02000000, 0, mdat, 1);
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        CHECK(cmd.u.configure_video.width == 320);
        CHECK(cmd.u.configure_video.height == 240);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB5a: keyframe gating and decode safety                        */
    /* ================================================================ */

    /* -- initial RAW non-keyframe is dropped -------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- initial RAW keyframe decodes, no KEYFRAME_WAITING event ----- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- initial CMAF non-keyframe dropped --------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 1000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 1000, 33, 1,
                                               0x01000000, 0, mdat, 1);
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);

        CHECK(moq_rcbuf_refcount(p) == 1);
        moq_rcbuf_decref(p);
        moq_playback_destroy(pb);
    }

    /* -- CMAF keyframe exits wait after non-keyframe drop ------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 1000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];

        /* Object 0: non-keyframe. */
        size_t len0 = build_cmaf_fragment(frag_buf, 1000, 33, 1,
                                           0x01000000, 0, mdat, 1);
        moq_rcbuf_t *p0 = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, len0, &p0) == MOQ_OK);

        /* Object 1: keyframe. */
        size_t len1 = build_cmaf_fragment(frag_buf, 1033, 33, 1,
                                           0x02000000, 0, mdat, 1);
        moq_rcbuf_t *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, len1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;

        obj.object_id = 0; obj.payload = p0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        obj.object_id = 1; obj.payload = p1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Non-keyframe dropped, keyframe decoded. */
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.object_id == 1);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_playback_destroy(pb);
    }

    /* -- gap recovery: partial abandon + RESET + keyframe wait ------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* Group 1 object 0: keyframe, end_of_group. Decode it. */
        moq_rcbuf_t *props_kf = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props_kf);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p1_0 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1_0) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p1_0;
        obj.properties = props_kf;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Group 2: object 0 and object 2 (end_of_group), missing 1.
           Also push group 3 non-keyframe so RESET is emitted when it's found. */
        moq_rcbuf_t *props2_0 = NULL, *props2_2 = NULL, *props3 = NULL;
        MAKE_LOC_PROPS(alloc, 2000, false, &props2_0);
        MAKE_LOC_PROPS(alloc, 4000, false, &props2_2);
        MAKE_LOC_PROPS(alloc, 5000, false, &props3);
        moq_rcbuf_t *p2_0 = NULL, *p2_2 = NULL, *p3_0 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2_0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2_2) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p3_0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2_0; obj.properties = props2_0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 2;
        obj.payload = p2_2; obj.properties = props2_2;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 3; obj.object_id = 0;
        obj.payload = p3_0; obj.properties = props3;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Object 2/0 decoded. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 2);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        /* Gap at 2/1 → partial abandon. */
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);

        /* RESET emitted when group 3 object found. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_GAP);

        /* Group 3 non-keyframe dropped (keyframe wait). */
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);

        /* Group 4 object 0: keyframe → SKIP_FORWARD + decode. */
        moq_rcbuf_t *props4 = NULL;
        MAKE_LOC_PROPS(alloc, 6000, true, &props4);
        moq_rcbuf_t *p4_0 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p4_0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 4; obj.object_id = 0;
        obj.payload = p4_0; obj.properties = props4;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_SKIP_FORWARD);
        CHECK(evt.u.skip_forward.from_group_id == 2);
        CHECK(evt.u.skip_forward.to_group_id == 4);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 4);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p1_0); moq_rcbuf_decref(p2_0);
        moq_rcbuf_decref(p2_2); moq_rcbuf_decref(p3_0);
        moq_rcbuf_decref(p4_0);
        moq_rcbuf_decref(props_kf); moq_rcbuf_decref(props2_0);
        moq_rcbuf_decref(props2_2); moq_rcbuf_decref(props3);
        moq_rcbuf_decref(props4);
        moq_playback_destroy(pb);
    }

    /* -- non-monotonic DTS dropped ----------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props1 = NULL;
        MAKE_LOC_PROPS(alloc, 2000, true, &props0);
        MAKE_LOC_PROPS(alloc, 1000, false, &props1);

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;

        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        obj.object_id = 1; obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.decode_time_us == 2000);
        moq_playback_cmd_cleanup(&cmd);

        /* Object 1 has DTS 1000 < 2000 → dropped. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS);
        CHECK(evt.u.object_dropped.object_id == 1);

        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props1);
        moq_playback_destroy(pb);
    }

    /* -- backpressure: event queue full during KEYFRAME_WAITING ------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p;
        obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Fill the single event slot. */
        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);

        /* Tick: can't emit KEYFRAME_WAITING → WOULD_BLOCK. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_rcbuf_refcount(p) == 2);

        /* Drain, retry: KEYFRAME_WAITING fills the slot. */
        moq_playback_event_t polled;
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);

        /* Tick: emits KEYFRAME_WAITING, then OBJECT_DROPPED needs
           another slot → WOULD_BLOCK. Object still buffered. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);

        /* Drain, retry: OBJECT_DROPPED succeeds, object dropped. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(p) == 1);

        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- backpressure: cmd queue full during RESET after partial ------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* Group 1: keyframe, end_of_group. */
        moq_rcbuf_t *props_kf = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props_kf);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t;
        obj.group_id = 1;
        obj.object_id = 0;
        obj.payload = p1;
        obj.properties = props_kf;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Decode group 1: CONFIGURE+DECODE fits in 2 slots. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        /* Group 2: objects 0 and 2(end_of_group), missing 1.
           Also push group 3 non-keyframe so RESET is triggered. */
        moq_rcbuf_t *props2_0 = NULL, *props2_2 = NULL, *props3 = NULL;
        MAKE_LOC_PROPS(alloc, 2000, false, &props2_0);
        MAKE_LOC_PROPS(alloc, 4000, false, &props2_2);
        MAKE_LOC_PROPS(alloc, 5000, false, &props3);
        moq_rcbuf_t *p2_0 = NULL, *p2_2 = NULL, *p3 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2_0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2_2) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p3) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2_0; obj.properties = props2_0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 2;
        obj.payload = p2_2; obj.properties = props2_2;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 3; obj.object_id = 0;
        obj.payload = p3; obj.properties = props3;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Fill 1 of 2 cmd slots. Tick decodes 2/0 (fills to 2),
           then partial abandon, then RESET can't fit → WOULD_BLOCK. */
        moq_playback_cmd_t filler;
        memset(&filler, 0, sizeof(filler));
        filler.kind = MOQ_PLAYBACK_CMD_RESET;
        CHECK(moq_playback_test_push_cmd(pb, &filler) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);

        /* Drain filler + DECODE. */
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);

        /* Retry: RESET succeeds now. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_GAP);

        moq_rcbuf_decref(p1); moq_rcbuf_decref(p2_0);
        moq_rcbuf_decref(p2_2); moq_rcbuf_decref(p3);
        moq_rcbuf_decref(props_kf); moq_rcbuf_decref(props2_0);
        moq_rcbuf_decref(props2_2); moq_rcbuf_decref(props3);
        moq_playback_destroy(pb);
    }

    /* -- gap reset clears DTS baseline (CMAF timeline discontinuity) -- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01 };
        uint8_t frag_buf[512];

        /* Group 1 object 0: keyframe at base_decode_time=900000
           (= 10000000 us at 90kHz), end_of_group. */
        size_t len1 = build_cmaf_fragment(frag_buf, 900000, 3000, 1,
                                           0x02000000, 0, mdat, 1);
        moq_rcbuf_t *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, len1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p1;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Group 2: objects 0 and 2(end_of_group), missing 1.
           Group 3: keyframe at base_decode_time=0 (reset timeline). */
        size_t len2_0 = build_cmaf_fragment(frag_buf, 903000, 3000, 1,
                                             0x01000000, 0, mdat, 1);
        moq_rcbuf_t *p2_0 = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, len2_0, &p2_0) == MOQ_OK);

        size_t len2_2 = build_cmaf_fragment(frag_buf, 909000, 3000, 1,
                                             0x01000000, 0, mdat, 1);
        moq_rcbuf_t *p2_2 = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, len2_2, &p2_2) == MOQ_OK);

        size_t len3 = build_cmaf_fragment(frag_buf, 0, 3000, 1,
                                           0x02000000, 0, mdat, 1);
        moq_rcbuf_t *p3 = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, len3, &p3) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2_0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 2;
        obj.payload = p2_2;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 3; obj.object_id = 0;
        obj.payload = p3;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Expect: DECODE_CMAF(2/0), PARTIAL_GROUP_ABANDONED,
           RESET(GAP), SKIP_FORWARD(2→3), CONFIGURE,
           DECODE_CMAF(3/0 at DTS 0).
           NOT: OBJECT_DROPPED(NON_MONOTONIC_DTS). */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.group_id == 2);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 10033333);
        moq_playback_cmd_cleanup(&cmd);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_GAP);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_SKIP_FORWARD);
        CHECK(evt.u.skip_forward.from_group_id == 2);
        CHECK(evt.u.skip_forward.to_group_id == 3);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.group_id == 3);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 0);
        moq_playback_cmd_cleanup(&cmd);

        /* No NON_MONOTONIC_DTS drop. */
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(p1); moq_rcbuf_decref(p2_0);
        moq_rcbuf_decref(p2_2); moq_rcbuf_decref(p3);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB5b: gap timeout and backlog shedding                         */
    /* ================================================================ */

    /* -- A. gap timer starts but does not fire early ------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 500000;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;

        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        obj.object_id = 2; obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        uint64_t T = 1000000;
        CHECK(moq_playback_tick(pb, T) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        CHECK(moq_playback_test_gap_waiting(pb, t));
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        /* Tick just before timeout: no events. */
        CHECK(moq_playback_tick(pb, T + 499999) == MOQ_OK);
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- B. missing object arrives before timeout -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 500000;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props1 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 2000, false, &props1);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;

        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        obj.object_id = 2; obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        uint64_t T = 1000000;
        CHECK(moq_playback_tick(pb, T) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_test_gap_waiting(pb, t));

        /* Missing object arrives before timeout. */
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 1; obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, T + 100000) == MOQ_OK);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.object_id == 1);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.u.decode_video.object_id == 2);
        moq_playback_cmd_cleanup(&cmd);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props1);
        moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- C. gap timeout fires ---------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 500000;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;

        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        obj.object_id = 2; obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        uint64_t T = 1000000;
        CHECK(moq_playback_tick(pb, T) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Timeout fires. */
        CHECK(moq_playback_tick(pb, T + 500000) == MOQ_OK);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED);
        CHECK(evt.u.gap_detected.group_id == 1);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);

        CHECK(moq_rcbuf_refcount(p2) == 1);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        /* Group 2 keyframe recovers. */
        moq_rcbuf_t *props_kf = NULL;
        MAKE_LOC_PROPS(alloc, 5000, true, &props_kf);
        moq_rcbuf_t *pk = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &pk) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = pk; obj.properties = props_kf;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, T + 600000) == MOQ_OK);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_GAP);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_SKIP_FORWARD);
        CHECK(evt.u.skip_forward.from_group_id == 1);
        CHECK(evt.u.skip_forward.to_group_id == 2);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 2);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p2); moq_rcbuf_decref(pk);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props2);
        moq_rcbuf_decref(props_kf);
        moq_playback_destroy(pb);
    }

    /* -- D. event queue full during gap timeout → WOULD_BLOCK -------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 100000;
        cfg.max_events = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 2; obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Fill 1 of 2 event slots. Gap timeout needs 2 atomically. */
        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);

        /* Only 1 free slot, need 2 → WOULD_BLOCK. */
        CHECK(moq_playback_tick(pb, 200000) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_rcbuf_refcount(p2) == 2);

        /* Drain filler, now 2 free slots → both events succeed. */
        moq_playback_event_t polled;
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 200000) == MOQ_OK);
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED);
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        CHECK(polled.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);
        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(p2) == 1);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- E. backlog shed --------------------------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_backlog_groups = 2;
        cfg.gap_timeout_us = 999999999;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };

        /* Group 1: keyframe, end_of_group. */
        moq_rcbuf_t *props1 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props1);
        moq_rcbuf_t *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p1; obj.properties = props1;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Group 2: object 0 only, gap at 1 blocks release.
           Groups 3, 4, 5: keyframes. */
        moq_rcbuf_t *p2 = NULL, *p3 = NULL, *p4 = NULL, *p5 = NULL;
        moq_rcbuf_t *pr2 = NULL, *pr3 = NULL, *pr4 = NULL, *pr5 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p3) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p4) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p5) == MOQ_OK);
        MAKE_LOC_PROPS(alloc, 2000, false, &pr2);
        MAKE_LOC_PROPS(alloc, 3000, true, &pr3);
        MAKE_LOC_PROPS(alloc, 4000, true, &pr4);
        MAKE_LOC_PROPS(alloc, 5000, true, &pr5);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2; obj.properties = pr2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 3; obj.object_id = 0;
        obj.payload = p3; obj.properties = pr3;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 4; obj.object_id = 0;
        obj.payload = p4; obj.properties = pr4;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 5; obj.object_id = 0;
        obj.payload = p5; obj.properties = pr5;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* tick: group 2/0 decoded first, then blocked on 2/1 → shed. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Group 2 object 0 decoded before shed. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 2);
        CHECK(cmd.u.decode_video.object_id == 0);
        moq_playback_cmd_cleanup(&cmd);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_BACKLOG_SHED);
        CHECK(evt.u.backlog_shed.dropped_groups > 0);

        /* Recovery: RESET + keyframe in remaining groups. */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);

        while (moq_playback_poll_event(pb, &evt) == MOQ_OK)
            (void)0;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(p3); moq_rcbuf_decref(p4); moq_rcbuf_decref(p5);
        moq_rcbuf_decref(props1); moq_rcbuf_decref(pr2);
        moq_rcbuf_decref(pr3); moq_rcbuf_decref(pr4); moq_rcbuf_decref(pr5);
        moq_playback_destroy(pb);
    }

    /* -- F. backlog shed event queue full → WOULD_BLOCK --------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_backlog_groups = 1;
        cfg.max_events = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t data[] = { 0x01 };

        /* Group 1: keyframe, end_of_group → decode first. */
        moq_rcbuf_t *p1 = NULL, *pr1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);
        MAKE_LOC_PROPS(alloc, 1000, true, &pr1);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p1; obj.properties = pr1;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Group 2: only obj 0 (missing obj 1 → blocked).
           Groups 3+4: keyframes. highest=4, expected=2, 4-2 > 1 → shed. */
        moq_rcbuf_t *p2 = NULL, *p3 = NULL, *p4 = NULL;
        moq_rcbuf_t *pr2 = NULL, *pr3 = NULL, *pr4 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p3) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p4) == MOQ_OK);
        MAKE_LOC_PROPS(alloc, 2000, false, &pr2);
        MAKE_LOC_PROPS(alloc, 3000, true, &pr3);
        MAKE_LOC_PROPS(alloc, 4000, true, &pr4);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2; obj.properties = pr2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 3; obj.object_id = 0;
        obj.payload = p3; obj.properties = pr3;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 4; obj.object_id = 0;
        obj.payload = p4; obj.properties = pr4;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Fill event queue before tick. */
        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);

        /* tick: decodes 2/0, then blocked on 2/1, shed needs event → WOULD_BLOCK. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);

        moq_playback_event_t polled;
        CHECK(moq_playback_poll_event(pb, &polled) == MOQ_OK);
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Drain and iterate until complete. */
        while (moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK) {
            while (moq_playback_poll_event(pb, &polled) == MOQ_OK)
                (void)0;
            while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
                moq_playback_cmd_cleanup(&cmd);
        }

        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);
        while (moq_playback_poll_event(pb, &polled) == MOQ_OK)
            (void)0;

        moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(p3); moq_rcbuf_decref(p4);
        moq_rcbuf_decref(pr1); moq_rcbuf_decref(pr2);
        moq_rcbuf_decref(pr3); moq_rcbuf_decref(pr4);
        moq_playback_destroy(pb);
    }

    /* -- G. closed group clears gap timer ----------------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 500000;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 3000, false, &props2);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 2; obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        uint64_t T = 1000000;
        CHECK(moq_playback_tick(pb, T) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_test_gap_waiting(pb, t));

        /* Close the group with END_OF_GROUP at object 3. */
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 3;
        obj.status = MOQ_OBJECT_END_OF_GROUP;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* tick uses explicit partial abandon, not timeout. */
        CHECK(moq_playback_tick(pb, T + 100) == MOQ_OK);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED);

        /* No GAP_DETECTED — closed-group path took precedence. */
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB6: feedback handling                                         */
    /* ================================================================ */

    /* -- NULL/invalid/stale feedback validation ----------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        CHECK(moq_playback_handle_feedback(NULL, &fb, 0) == MOQ_ERR_INVAL);
        CHECK(moq_playback_handle_feedback(pb, NULL, 0) == MOQ_ERR_INVAL);

        fb.struct_size = 4;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_ERR_INVAL);

        moq_playback_feedback_init(&fb);
        fb.track = MOQ_PLAYBACK_TRACK_INVALID;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_ERR_STALE_HANDLE);

        moq_playback_feedback_init(&fb);
        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        fb.track = t;
        fb.kind = (moq_playback_feedback_kind_t)99;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_ERR_INVAL);

        moq_playback_destroy(pb);
    }

    /* -- QUEUE_PRESSURE pauses and resumes release -------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p; obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Pressure high: depth > max_recommended. */
        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
        fb.u.queue_pressure.depth = 10;
        fb.u.queue_pressure.max_recommended = 5;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        /* Tick: no commands emitted (paused). */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        /* Pressure low: depth <= max_recommended. */
        fb.u.queue_pressure.depth = 3;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_test_buffered_count(pb) == 0);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- QUEUE_PRESSURE does not start gap timers while paused -------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.gap_timeout_us = 100000;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p; obj.properties = props;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Push group 2 obj 0 only (missing obj 1) before first tick. */
        moq_rcbuf_t *props2 = NULL, *p2 = NULL;
        MAKE_LOC_PROPS(alloc, 2000, false, &props2);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Decode group 1. Group 2 obj 0 decoded, gap at 2/1 starts. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_test_gap_waiting(pb, t));

        /* Pause. */
        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
        fb.u.queue_pressure.depth = 10;
        fb.u.queue_pressure.max_recommended = 5;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        /* Tick while paused: gap timer must not fire. */
        CHECK(moq_playback_tick(pb, 1000000) == MOQ_OK);
        moq_playback_event_t gap_evt;
        CHECK(moq_playback_poll_event(pb, &gap_evt) == MOQ_DONE);

        /* Resume. Gap timer should fire now. */
        fb.u.queue_pressure.depth = 2;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 1000000) == MOQ_OK);

        CHECK(moq_playback_poll_event(pb, &gap_evt) == MOQ_OK);
        CHECK(gap_evt.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED);
        while (moq_playback_poll_event(pb, &gap_evt) == MOQ_OK)
            (void)0;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props); moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- DECODE_ERROR emits RESET + KEYFRAME_WAITING ----------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p; obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Decode first object. */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Report decode error. */
        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.struct_size == sizeof(moq_playback_cmd_t));
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_DECODE_ERROR);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.struct_size == sizeof(moq_playback_event_t));
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- DECODE_ERROR purges stale commands for the track ------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = NULL, *props1 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        MAKE_LOC_PROPS(alloc, 2000, false, &props1);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 1; obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* tick emits CONFIGURE + DECODE(0) + DECODE(1). */
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Stale payload still held by queued DECODE commands. */
        CHECK(moq_rcbuf_refcount(p0) == 2);
        CHECK(moq_rcbuf_refcount(p1) == 2);

        /* DECODE_ERROR: purges stale CONFIGURE + DECODE commands. */
        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        /* Stale payload refs released by purge. */
        CHECK(moq_rcbuf_refcount(p0) == 1);
        CHECK(moq_rcbuf_refcount(p1) == 1);

        /* Next polled command is RESET, not stale DECODE. */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_DECODE_ERROR);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props1);
        moq_playback_destroy(pb);
    }

    /* -- DECODE_ERROR cmd queue full → WOULD_BLOCK ------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_cmd_t filler;
        memset(&filler, 0, sizeof(filler));
        filler.kind = MOQ_PLAYBACK_CMD_RESET;
        CHECK(moq_playback_test_push_cmd(pb, &filler) == MOQ_OK);

        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_ERR_WOULD_BLOCK);

        /* No RESET or KEYFRAME_WAITING queued. */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET); /* filler */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_playback_destroy(pb);
    }

    /* -- DECODE_ERROR event queue full → WOULD_BLOCK ------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_events = 1;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_playback_event_t fill = { .kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED };
        CHECK(moq_playback_test_push_event(pb, &fill) == MOQ_OK);

        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_ERR_WOULD_BLOCK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_TRACK_ENDED); /* filler */
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_playback_destroy(pb);
    }

    /* -- After DECODE_ERROR: non-keyframe dropped, keyframe resumes -- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* Decode initial keyframe. */
        moq_rcbuf_t *props0 = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props0);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p0) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p0; obj.properties = props0;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        /* Report decode error. */
        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            (void)0;
        moq_playback_event_t evt;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK)
            (void)0;

        /* Push non-keyframe for group 2. */
        moq_rcbuf_t *props1 = NULL, *p1 = NULL;
        MAKE_LOC_PROPS(alloc, 2000, false, &props1);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p1) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p1; obj.properties = props1;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_KEYFRAME_WAIT);
        CHECK(moq_rcbuf_refcount(p1) == 1);

        /* Push keyframe for group 3. */
        moq_rcbuf_t *props2 = NULL, *p2 = NULL;
        MAKE_LOC_PROPS(alloc, 3000, true, &props2);
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 3; obj.object_id = 0;
        obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.group_id == 3);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props0); moq_rcbuf_decref(props1);
        moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  PB12: audio track support                                      */
    /* ================================================================ */

    /* -- RAW audio: CONFIGURE_AUDIO then DECODE_AUDIO, no keyframe wait */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 5000, false, &props);
        uint8_t data[] = { 0xAA, 0xBB };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p; obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.struct_size == sizeof(moq_playback_cmd_t));
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        CHECK(cmd.u.decode_audio.decode_time_us == 5000);
        CHECK(cmd.u.decode_audio.payload == p);
        CHECK(cmd.u.decode_audio.mdat_offset == 0);
        CHECK(cmd.u.decode_audio.mdat_len == 0);
        CHECK(moq_rcbuf_refcount(p) == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(p) == 1);

        /* No KEYFRAME_WAITING event for audio. */
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(p); moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- CMAF audio: CONFIGURE_AUDIO + DECODE_AUDIO with timing ------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        uint8_t asc[] = { 0x12, 0x10 };
        uint8_t init_seg[512];
        size_t init_len = build_aac_init(init_seg, 48000, 48000, 2,
                                          asc, sizeof(asc));

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.codec = (moq_bytes_t){ .data = (const uint8_t *)"mp4a", .len = 4 };
        tc.init_data = (moq_bytes_t){ .data = init_seg, .len = init_len };
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01, 0x02, 0x03 };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_fragment(frag_buf, 48000, 1024, 3,
                                               0x00000000, 0, mdat, 3);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        CHECK(cmd.u.configure_audio.samplerate == 48000);
        CHECK(cmd.u.configure_audio.channel_count == 2);
        CHECK(cmd.u.configure_audio.codec != NULL);
        CHECK(cmd.u.configure_audio.codec_config != NULL);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        CHECK(cmd.u.decode_audio.decode_time_us == 1000000);
        CHECK(cmd.u.decode_audio.mdat_len == 3);
        CHECK(cmd.u.decode_audio.payload == payload);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- Audio DECODE_ERROR: RESET without KEYFRAME_WAITING ---------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = p; obj.properties = props;
        obj.end_of_group = true;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
            moq_playback_cmd_cleanup(&cmd);

        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_DECODE_ERROR);

        /* No KEYFRAME_WAITING for audio. */
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        /* Next audio object decodes immediately (no keyframe wait). */
        moq_rcbuf_t *props2 = NULL;
        MAKE_LOC_PROPS(alloc, 2000, false, &props2);
        moq_rcbuf_t *p2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p2) == MOQ_OK);

        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 2; obj.object_id = 0;
        obj.payload = p2; obj.properties = props2;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p); moq_rcbuf_decref(p2);
        moq_rcbuf_decref(props); moq_rcbuf_decref(props2);
        moq_playback_destroy(pb);
    }

    /* -- Mixed audio+video: independent tracks, no cross-blocking ---- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t vtc = make_track_cfg();
        moq_playback_track_t vt = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &vtc, &vt) == MOQ_OK);

        moq_playback_track_cfg_t atc;
        moq_playback_track_cfg_init(&atc);
        atc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        atc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t at = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &atc, &at) == MOQ_OK);

        /* Push audio (non-keyframe LOC): should decode immediately. */
        moq_rcbuf_t *aprops = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &aprops);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *ap = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &ap) == MOQ_OK);
        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = at; obj.group_id = 1; obj.object_id = 0;
        obj.payload = ap; obj.properties = aprops;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* Push video non-keyframe: should be dropped (keyframe wait). */
        moq_rcbuf_t *vprops = NULL;
        MAKE_LOC_PROPS(alloc, 1000, false, &vprops);
        moq_rcbuf_t *vp = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &vp) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = vt; obj.group_id = 1; obj.object_id = 0;
        obj.payload = vp; obj.properties = vprops;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* Audio decoded. */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        CHECK(cmd.track._opaque == at._opaque);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        CHECK(cmd.track._opaque == at._opaque);
        moq_playback_cmd_cleanup(&cmd);

        /* No video decode (keyframe wait dropped it). */
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        bool got_kf_wait = false;
        while (moq_playback_poll_event(pb, &evt) == MOQ_OK) {
            if (evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING)
                got_kf_wait = true;
        }
        CHECK(got_kf_wait);

        moq_rcbuf_decref(ap); moq_rcbuf_decref(vp);
        moq_rcbuf_decref(aprops); moq_rcbuf_decref(vprops);
        moq_playback_destroy(pb);
    }

    /* -- Audio command cleanup releases owned refs -------------------- */
    {
        moq_playback_cmd_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        cmd.kind = MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO;
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *c1 = NULL, *c2 = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &c1) == MOQ_OK);
        CHECK(moq_rcbuf_create(alloc, data, 1, &c2) == MOQ_OK);
        cmd.u.configure_audio.codec = c1;
        cmd.u.configure_audio.codec_config = c2;
        CHECK(moq_rcbuf_refcount(c1) == 1);
        CHECK(moq_rcbuf_refcount(c2) == 1);
        moq_playback_cmd_cleanup(&cmd);
        /* Both refs released — would crash if accessed, so just check
           they were set to NULL. */
        CHECK(cmd.u.configure_audio.codec == NULL);
        CHECK(cmd.u.configure_audio.codec_config == NULL);

        cmd.kind = MOQ_PLAYBACK_CMD_DECODE_AUDIO;
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);
        moq_rcbuf_incref(p);
        cmd.u.decode_audio.payload = p;
        CHECK(moq_rcbuf_refcount(p) == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(p) == 1);
        moq_rcbuf_decref(p);
    }

    /* ================================================================ */
    /*  push_sub_object convenience helper                             */
    /* ================================================================ */

    /* -- NULL args ----------------------------------------------------- */
    {
        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_push_sub_object(NULL, t, &src, 0) == MOQ_ERR_INVAL);

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);
        CHECK(moq_playback_push_sub_object(pb, t, NULL, 0) == MOQ_ERR_INVAL);
        moq_playback_destroy(pb);
    }

    /* -- RAW video via push_sub_object produces CONFIGURE + DECODE ---- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0xDE, 0xAD };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &p) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 1;
        src.object_id = 0;
        src.payload = p;
        src.properties = props;

        CHECK(moq_playback_push_sub_object(pb, t, &src, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p) == 2);
        CHECK(moq_rcbuf_refcount(props) == 1);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.decode_time_us == 1000);
        CHECK(cmd.u.decode_video.payload == p);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- stale track returns STALE_HANDLE, no retain ------------------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        CHECK(moq_playback_remove_track(pb, t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 1000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 1;
        src.payload = p;
        src.properties = props;

        CHECK(moq_playback_push_sub_object(pb, t, &src, 0) == MOQ_ERR_STALE_HANDLE);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- terminal END_OF_GROUP via push_sub_object --------------------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 1;
        src.object_id = 0;
        src.status = MOQ_OBJECT_END_OF_GROUP;

        CHECK(moq_playback_push_sub_object(pb, t, &src, 0) == MOQ_OK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);

        moq_playback_destroy(pb);
    }

    /* -- terminal END_OF_TRACK via push_sub_object: TRACK_ENDED + WRONG_STATE */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc = make_track_cfg();
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 1;
        src.object_id = 0;
        src.status = MOQ_OBJECT_END_OF_TRACK;

        CHECK(moq_playback_push_sub_object(pb, t, &src, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_TRACK_ENDED);

        /* Later push returns WRONG_STATE. */
        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 2000, true, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_sub_object_t src2;
        memset(&src2, 0, sizeof(src2));
        src2.group_id = 2;
        src2.payload = p;
        src2.properties = props;

        CHECK(moq_playback_push_sub_object(pb, t, &src2, 0) == MOQ_ERR_WRONG_STATE);
        CHECK(moq_rcbuf_refcount(p) == 1);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* -- RAW audio via push_sub_object: CONFIGURE_AUDIO + DECODE_AUDIO */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = NULL;
        MAKE_LOC_PROPS(alloc, 3000, false, &props);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 1;
        src.payload = p;
        src.properties = props;

        CHECK(moq_playback_push_sub_object(pb, t, &src, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);

        moq_rcbuf_decref(p);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  Multi-sample CMAF additional tests                             */
    /* ================================================================ */

    /* -- CMAF audio 3-sample emits 3 DECODE_AUDIO commands ----------- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 48000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 };
        uint8_t frag_buf[512];
        size_t p = 0;
        uint32_t tf = 0x100 | 0x200;
        size_t trun_sz = 8 + 8 + 3 * 8;
        size_t tfdt_sz = 8 + 4 + 8, tfhd_sz = 8 + 8;
        size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
        size_t moof_sz = 8 + traf_sz;
        wr32(frag_buf+p,(uint32_t)moof_sz); memcpy(frag_buf+p+4,"moof",4); p+=8;
        wr32(frag_buf+p,(uint32_t)traf_sz); memcpy(frag_buf+p+4,"traf",4); p+=8;
        wr32(frag_buf+p,(uint32_t)tfhd_sz); memcpy(frag_buf+p+4,"tfhd",4); p+=8;
        wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,(uint32_t)tfdt_sz); memcpy(frag_buf+p+4,"tfdt",4); p+=8;
        wr32(frag_buf+p,0x01000000); p+=4; wr32(frag_buf+p,0); p+=4;
        wr32(frag_buf+p,48000); p+=4;
        wr32(frag_buf+p,(uint32_t)trun_sz); memcpy(frag_buf+p+4,"trun",4); p+=8;
        wr32(frag_buf+p,tf); p+=4; wr32(frag_buf+p,3); p+=4;
        wr32(frag_buf+p,1024); p+=4; wr32(frag_buf+p,2); p+=4;
        wr32(frag_buf+p,1024); p+=4; wr32(frag_buf+p,3); p+=4;
        wr32(frag_buf+p,1024); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,(uint32_t)(8+6)); memcpy(frag_buf+p+4,"mdat",4); p+=8;
        memcpy(frag_buf+p,mdat,6); p+=6;
        size_t frag_len = p;

        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        CHECK(cmd.u.decode_audio.decode_time_us == 1000000);
        CHECK(cmd.u.decode_audio.mdat_len == 2);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        CHECK(cmd.u.decode_audio.decode_time_us == 1021333);
        CHECK(cmd.u.decode_audio.mdat_len == 3);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        CHECK(cmd.u.decode_audio.decode_time_us == 1042666);
        CHECK(cmd.u.decode_audio.mdat_len == 1);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- Multi-sample command queue full: WOULD_BLOCK commit-last ---- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 3;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* 3-sample fragment needs CONFIGURE + 3 DECODE = 4 slots.
           max_commands=3 → WOULD_BLOCK. */
        uint8_t mdat[] = { 0x01, 0x02, 0x03 };
        uint8_t frag_buf[512];
        size_t p = 0;
        uint32_t trun_flags = 0x100 | 0x200 | 0x400;
        size_t trun_sz = 8 + 8 + 3 * 12;
        size_t tfdt_sz = 8 + 4 + 8, tfhd_sz = 8 + 8;
        size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
        size_t moof_sz = 8 + traf_sz;
        wr32(frag_buf+p,(uint32_t)moof_sz); memcpy(frag_buf+p+4,"moof",4); p+=8;
        wr32(frag_buf+p,(uint32_t)traf_sz); memcpy(frag_buf+p+4,"traf",4); p+=8;
        wr32(frag_buf+p,(uint32_t)tfhd_sz); memcpy(frag_buf+p+4,"tfhd",4); p+=8;
        wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,(uint32_t)tfdt_sz); memcpy(frag_buf+p+4,"tfdt",4); p+=8;
        wr32(frag_buf+p,0x01000000); p+=4; wr32(frag_buf+p,0); p+=4;
        wr32(frag_buf+p,0); p+=4;
        wr32(frag_buf+p,(uint32_t)trun_sz); memcpy(frag_buf+p+4,"trun",4); p+=8;
        wr32(frag_buf+p,trun_flags); p+=4; wr32(frag_buf+p,3); p+=4;
        for (int i = 0; i < 3; i++) {
            wr32(frag_buf+p,3000); p+=4;
            wr32(frag_buf+p,1); p+=4;
            wr32(frag_buf+p, i == 0 ? 0x02000000 : 0x01000000); p+=4;
        }
        wr32(frag_buf+p,(uint32_t)(8+3)); memcpy(frag_buf+p+4,"mdat",4); p+=8;
        memcpy(frag_buf+p,mdat,3); p+=3;
        size_t frag_len = p;

        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        /* tick: CONFIGURE uses 1 slot, 3 DECODE needs 3 more = 4 total > 3 */
        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- Multi-sample validation failure drops, no CONFIGURE leaked --- */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 1;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* 2-sample fragment: sample 0 OK, sample 1 has duration that
           causes DTS overflow at timescale=1. */
        uint8_t mdat[] = { 0x01, 0x02 };
        uint8_t frag_buf[512];
        size_t p = 0;
        uint32_t trun_flags = 0x100 | 0x200 | 0x400;
        size_t trun_sz = 8 + 8 + 2 * 12;
        size_t tfdt_sz = 8 + 4 + 8, tfhd_sz = 8 + 8;
        size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
        size_t moof_sz = 8 + traf_sz;
        wr32(frag_buf+p,(uint32_t)moof_sz); memcpy(frag_buf+p+4,"moof",4); p+=8;
        wr32(frag_buf+p,(uint32_t)traf_sz); memcpy(frag_buf+p+4,"traf",4); p+=8;
        wr32(frag_buf+p,(uint32_t)tfhd_sz); memcpy(frag_buf+p+4,"tfhd",4); p+=8;
        wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,(uint32_t)tfdt_sz); memcpy(frag_buf+p+4,"tfdt",4); p+=8;
        wr32(frag_buf+p,0x01000000); p+=4;
        wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,0); p+=4;
        wr32(frag_buf+p,(uint32_t)trun_sz); memcpy(frag_buf+p+4,"trun",4); p+=8;
        wr32(frag_buf+p,trun_flags); p+=4; wr32(frag_buf+p,2); p+=4;
        /* sample 0: dur=1, size=1, flags=0x02000000 */
        wr32(frag_buf+p,1); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,0x02000000); p+=4;
        /* sample 1: dur=0xFFFFFFFF → DTS 1+0xFFFFFFFF at ts=1 → overflow */
        wr32(frag_buf+p,0xFFFFFFFF); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,0x01000000); p+=4;
        wr32(frag_buf+p,(uint32_t)(8+2)); memcpy(frag_buf+p+4,"mdat",4); p+=8;
        memcpy(frag_buf+p,mdat,2); p+=2;
        size_t frag_len = p;

        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        /* No CONFIGURE or DECODE queued — object dropped. */
        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
        CHECK(evt.u.object_dropped.reason == MOQ_PLAYBACK_DROP_MALFORMED_CMAF);

        CHECK(moq_playback_test_buffered_count(pb) == 0);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* -- Multi-sample valid but cmd queue too small: WOULD_BLOCK ------ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_commands = 2;
        moq_playback_t *pb = make_pb(alloc, &cfg);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        /* 3-sample valid fragment. CONFIGURE + 3 DECODE = 4 > max 2. */
        uint8_t mdat[] = { 0x01, 0x02, 0x03 };
        uint8_t frag_buf[512];
        size_t p = 0;
        uint32_t trun_flags = 0x100 | 0x200 | 0x400;
        size_t trun_sz = 8 + 8 + 3 * 12;
        size_t tfdt_sz = 8 + 4 + 8, tfhd_sz = 8 + 8;
        size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
        size_t moof_sz = 8 + traf_sz;
        wr32(frag_buf+p,(uint32_t)moof_sz); memcpy(frag_buf+p+4,"moof",4); p+=8;
        wr32(frag_buf+p,(uint32_t)traf_sz); memcpy(frag_buf+p+4,"traf",4); p+=8;
        wr32(frag_buf+p,(uint32_t)tfhd_sz); memcpy(frag_buf+p+4,"tfhd",4); p+=8;
        wr32(frag_buf+p,0); p+=4; wr32(frag_buf+p,1); p+=4;
        wr32(frag_buf+p,(uint32_t)tfdt_sz); memcpy(frag_buf+p+4,"tfdt",4); p+=8;
        wr32(frag_buf+p,0x01000000); p+=4; wr32(frag_buf+p,0); p+=4;
        wr32(frag_buf+p,0); p+=4;
        wr32(frag_buf+p,(uint32_t)trun_sz); memcpy(frag_buf+p+4,"trun",4); p+=8;
        wr32(frag_buf+p,trun_flags); p+=4; wr32(frag_buf+p,3); p+=4;
        for (int i = 0; i < 3; i++) {
            wr32(frag_buf+p,3000); p+=4;
            wr32(frag_buf+p,1); p+=4;
            wr32(frag_buf+p,0x02000000); p+=4;
        }
        wr32(frag_buf+p,(uint32_t)(8+3)); memcpy(frag_buf+p+4,"mdat",4); p+=8;
        memcpy(frag_buf+p,mdat,3); p+=3;
        size_t frag_len = p;

        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frag_buf, frag_len, &payload) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_playback_test_buffered_count(pb) == 1);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_DONE);
        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_DONE);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
    }

    /* ================================================================ */
    /*  Open-group mode: eog=true on all objects, strict vs open        */
    /* ================================================================ */

    /* Strict mode: eog=true on object 0 closes the group, so objects 1-2 are
     * dropped (not decoded) — only object 0 emits CONFIGURE + DECODE. */
    {
        moq_playback_cfg_t c;
        moq_playback_cfg_init(&c);
        c.max_release_per_tick = 0;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &c, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1", 4 };
        moq_playback_track_t t;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0xCA, 0xFE };
        for (int i = 0; i < 3; i++) {
            uint8_t buf[512];
            size_t len = build_cmaf_fragment(buf, (uint64_t)i * 3000,
                3000, 2, i == 0 ? 0 : 0x00010000, 0, mdat, 2);
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(alloc, buf, len, &p);
            moq_playback_object_t obj;
            moq_playback_object_init(&obj);
            obj.track = t; obj.group_id = 0; obj.object_id = i;
            obj.payload = p; obj.end_of_group = true;
            CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
            moq_rcbuf_decref(p);
        }
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        int n = 0;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            n++;
            moq_playback_cmd_cleanup(&cmd);
        }
        CHECK(n == 2);
        moq_playback_destroy(pb);
    }

    /* Open-group mode: eog=true ignored, all 3 decode. */
    {
        moq_playback_cfg_t c;
        moq_playback_cfg_init(&c);
        c.max_release_per_tick = 0;
        c.ignore_eog_bit = true;
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(alloc, &c, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1", 4 };
        moq_playback_track_t t;
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0xCA, 0xFE };
        for (int i = 0; i < 3; i++) {
            uint8_t buf[512];
            size_t len = build_cmaf_fragment(buf, (uint64_t)i * 3000,
                3000, 2, i == 0 ? 0 : 0x00010000, 0, mdat, 2);
            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(alloc, buf, len, &p);
            moq_playback_object_t obj;
            moq_playback_object_init(&obj);
            obj.track = t; obj.group_id = 0; obj.object_id = i;
            obj.payload = p; obj.end_of_group = true;
            CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
            moq_rcbuf_decref(p);
        }
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        moq_playback_cmd_t cmd;
        int n = 0;
        while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
            n++;
            moq_playback_cmd_cleanup(&cmd);
        }
        CHECK(n == 4);
        moq_playback_destroy(pb);
    }

    test_sparse_future_group(alloc);

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}

/* Sparse future group: N+2 buffered but N+1 missing.
 * Should not stall — backlog shedding should handle. */
static void test_sparse_future_group(const moq_alloc_t *alloc)
{
    moq_playback_cfg_t c;
    moq_playback_cfg_init(&c);
    c.max_release_per_tick = 0;
    c.ignore_eog_bit = true;
    c.infer_end_of_group_from_next_group = true;
    c.max_backlog_groups = 2;
    c.gap_timeout_us = 100;

    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(alloc, &c, &pb) == MOQ_OK);

    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"opus", 4 };
    moq_playback_track_t t;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

    /* Group 0 object 0 */
    moq_rcbuf_t *props0 = NULL;
    MAKE_LOC_PROPS(alloc, 1000000, false, &props0);
    uint8_t d[] = { 0xAA };
    moq_rcbuf_t *p0 = NULL;
    moq_rcbuf_create(alloc, d, 1, &p0);
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t; obj.group_id = 0; obj.object_id = 0;
    obj.payload = p0; obj.properties = props0;
    CHECK(moq_playback_push_object(pb, &obj, 1000000) == MOQ_OK);
    moq_rcbuf_decref(props0);

    CHECK(moq_playback_tick(pb, 1000000) == MOQ_OK);
    moq_playback_cmd_t cmd;
    while (moq_playback_poll_command(pb, &cmd) == MOQ_OK)
        moq_playback_cmd_cleanup(&cmd);

    /* Skip group 1 entirely. Push group 2 object 0. */
    moq_rcbuf_t *props2 = NULL;
    MAKE_LOC_PROPS(alloc, 3000000, false, &props2);
    moq_rcbuf_t *p2 = NULL;
    moq_rcbuf_create(alloc, d, 1, &p2);
    moq_playback_object_init(&obj);
    obj.track = t; obj.group_id = 2; obj.object_id = 0;
    obj.payload = p2; obj.properties = props2;
    CHECK(moq_playback_push_object(pb, &obj, 3000000) == MOQ_OK);
    moq_rcbuf_decref(props2);

    /* First tick: starts gap timer. */
    CHECK(moq_playback_tick(pb, 3000000) == MOQ_OK);

    /* Second tick: past gap timeout — gap fires, recovery, group 2
     * should become reachable. */
    CHECK(moq_playback_tick(pb, 3200000) == MOQ_OK);

    /* Third tick: after recovery, group 2 should decode. */
    CHECK(moq_playback_tick(pb, 3300000) == MOQ_OK);

    moq_playback_event_t evt;
    bool had_gap = false, had_abandon = false;
    while (moq_playback_poll_event(pb, &evt) == MOQ_OK) {
        if (evt.kind == MOQ_PLAYBACK_EVENT_GAP_DETECTED)
            had_gap = true;
        if (evt.kind == MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED)
            had_abandon = true;
    }
    CHECK(had_gap);
    CHECK(had_abandon);

    /* Group 2 should decode after recovery. */
    bool had_decode = false;
    while (moq_playback_poll_command(pb, &cmd) == MOQ_OK) {
        if (cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO)
            had_decode = true;
        moq_playback_cmd_cleanup(&cmd);
    }
    CHECK(had_decode);

    moq_rcbuf_decref(p0);
    moq_rcbuf_decref(p2);
    moq_playback_destroy(pb);
}
