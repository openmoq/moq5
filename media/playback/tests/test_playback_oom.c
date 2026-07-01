/*
 * Playback OOM and no-allocation tests.
 *
 * Proves allocation behavior is intentional:
 * - create/add_track handle every failure point cleanly
 * - steady-state push/tick/feedback do not allocate
 * - no leaks on any failure path
 */

#include <moq/playback.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
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
    moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &out);
    return out;
}

/* -- CMAF init builder ----------------------------------------------- */

static void wr32(uint8_t *p, uint32_t v) {
    p[0]=(uint8_t)(v>>24); p[1]=(uint8_t)(v>>16);
    p[2]=(uint8_t)(v>>8);  p[3]=(uint8_t)v;
}
static void wr16(uint8_t *p, uint16_t v) {
    p[0]=(uint8_t)(v>>8); p[1]=(uint8_t)v;
}
static size_t box_hdr(uint8_t *p, uint32_t size, const char *type) {
    wr32(p, size); memcpy(p+4, type, 4); return 8;
}

static size_t build_avc_init(uint8_t *buf, uint32_t timescale,
                              uint16_t w, uint16_t h,
                              const uint8_t *avcc, size_t avcc_len)
{
    size_t p = 0;
    p += box_hdr(buf+p, 20, "ftyp");
    memcpy(buf+p, "isom", 4); p += 4;
    wr32(buf+p, 0); p += 4;
    memcpy(buf+p, "isom", 4); p += 4;

    size_t avcc_box = 8+avcc_len;
    size_t avc1_sz = 8+78+avcc_box;
    size_t stsd_sz = 8+8+avc1_sz;
    size_t stbl_sz = 8+stsd_sz;
    size_t minf_sz = 8+stbl_sz;
    size_t mdhd_sz = 8+4+12;
    size_t mdia_sz = 8+mdhd_sz+minf_sz;
    size_t trak_sz = 8+mdia_sz;
    size_t moov_sz = 8+trak_sz;

    p += box_hdr(buf+p, (uint32_t)moov_sz, "moov");
    p += box_hdr(buf+p, (uint32_t)trak_sz, "trak");
    p += box_hdr(buf+p, (uint32_t)mdia_sz, "mdia");
    p += box_hdr(buf+p, (uint32_t)mdhd_sz, "mdhd");
    wr32(buf+p, 0); p += 4;
    wr32(buf+p, 0); p += 4;
    wr32(buf+p, 0); p += 4;
    wr32(buf+p, timescale); p += 4;
    p += box_hdr(buf+p, (uint32_t)minf_sz, "minf");
    p += box_hdr(buf+p, (uint32_t)stbl_sz, "stbl");
    p += box_hdr(buf+p, (uint32_t)stsd_sz, "stsd");
    wr32(buf+p, 0); p += 4;
    wr32(buf+p, 1); p += 4;
    p += box_hdr(buf+p, (uint32_t)avc1_sz, "avc1");
    memset(buf+p, 0, 78);
    wr16(buf+p+24, w);
    wr16(buf+p+26, h);
    p += 78;
    p += box_hdr(buf+p, (uint32_t)avcc_box, "avcC");
    memcpy(buf+p, avcc, avcc_len);
    p += avcc_len;
    return p;
}

/* -- CMAF fragment builder ------------------------------------------- */

static size_t build_cmaf_frag(uint8_t *buf, uint64_t bdt,
                               uint32_t dur, uint32_t flags,
                               const uint8_t *mdat, size_t mdat_len)
{
    size_t p = 0;
    uint32_t tf = 0x100|0x200|0x400;
    size_t trun_sz=8+8+12, tfdt_sz=8+4+8, tfhd_sz=8+8;
    size_t traf_sz=8+tfhd_sz+tfdt_sz+trun_sz, moof_sz=8+traf_sz;
    wr32(buf+p,(uint32_t)moof_sz); memcpy(buf+p+4,"moof",4); p+=8;
    wr32(buf+p,(uint32_t)traf_sz); memcpy(buf+p+4,"traf",4); p+=8;
    wr32(buf+p,(uint32_t)tfhd_sz); memcpy(buf+p+4,"tfhd",4); p+=8;
    wr32(buf+p,0); p+=4; wr32(buf+p,1); p+=4;
    wr32(buf+p,(uint32_t)tfdt_sz); memcpy(buf+p+4,"tfdt",4); p+=8;
    wr32(buf+p,0x01000000); p+=4;
    wr32(buf+p,(uint32_t)(bdt>>32)); p+=4;
    wr32(buf+p,(uint32_t)bdt); p+=4;
    wr32(buf+p,(uint32_t)trun_sz); memcpy(buf+p+4,"trun",4); p+=8;
    wr32(buf+p,tf); p+=4; wr32(buf+p,1); p+=4;
    wr32(buf+p,dur); p+=4;
    wr32(buf+p,(uint32_t)mdat_len); p+=4;
    wr32(buf+p,flags); p+=4;
    wr32(buf+p,(uint32_t)(8+mdat_len)); memcpy(buf+p+4,"mdat",4); p+=8;
    memcpy(buf+p,mdat,mdat_len); p+=mdat_len;
    return p;
}

static size_t build_aac_init(uint8_t *buf, uint32_t timescale,
                              uint16_t samplerate, uint16_t channels,
                              const uint8_t *asc, size_t asc_len)
{
    size_t p = 0;
    p += box_hdr(buf+p, 20, "ftyp");
    memcpy(buf+p,"isom",4); p+=4; wr32(buf+p,0); p+=4;
    memcpy(buf+p,"isom",4); p+=4;
    size_t dsl=2+asc_len, dcl=2+13+dsl, esl=2+3+dcl;
    size_t eb=4+esl, esz=8+eb, mp=8+28+esz;
    size_t ss=8+8+mp, sb=8+ss, mn=8+sb, md=8+4+12;
    size_t mda=8+md+mn, tr=8+mda, mv=8+tr;
    p+=box_hdr(buf+p,(uint32_t)mv,"moov");
    p+=box_hdr(buf+p,(uint32_t)tr,"trak");
    p+=box_hdr(buf+p,(uint32_t)mda,"mdia");
    p+=box_hdr(buf+p,(uint32_t)md,"mdhd");
    wr32(buf+p,0); p+=4; wr32(buf+p,0); p+=4;
    wr32(buf+p,0); p+=4; wr32(buf+p,timescale); p+=4;
    p+=box_hdr(buf+p,(uint32_t)mn,"minf");
    p+=box_hdr(buf+p,(uint32_t)sb,"stbl");
    p+=box_hdr(buf+p,(uint32_t)ss,"stsd");
    wr32(buf+p,0); p+=4; wr32(buf+p,1); p+=4;
    p+=box_hdr(buf+p,(uint32_t)mp,"mp4a");
    memset(buf+p,0,28);
    wr16(buf+p+8,channels); wr16(buf+p+24,samplerate); p+=28;
    p+=box_hdr(buf+p,(uint32_t)esz,"esds");
    wr32(buf+p,0); p+=4;
    buf[p++]=0x03; buf[p++]=(uint8_t)(esl-2);
    wr16(buf+p,0); p+=2; buf[p++]=0;
    buf[p++]=0x04; buf[p++]=(uint8_t)(dcl-2);
    buf[p++]=0x40; buf[p++]=0x15;
    buf[p++]=0; buf[p++]=0; buf[p++]=0;
    wr32(buf+p,0); p+=4; wr32(buf+p,0); p+=4;
    buf[p++]=0x05; buf[p++]=(uint8_t)asc_len;
    memcpy(buf+p,asc,asc_len); p+=asc_len;
    return p;
}

int main(void)
{

    /* ================================================================ */
    /*  1. create/destroy OOM sweep                                    */
    /* ================================================================ */
    {
        /* Baseline: count allocations. */
        oom_state_t baseline = { 0, 0, 0 };
        moq_alloc_t ba = oom_allocator(&baseline);
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&ba, &cfg, &pb) == MOQ_OK);
        uint64_t create_allocs = baseline.alloc_count;
        CHECK(create_allocs >= 5);
        moq_playback_destroy(pb);
        CHECK(baseline.balance == 0);

        /* Sweep every allocation point. */
        for (uint64_t fail = 1; fail <= create_allocs; fail++) {
            oom_state_t oom = { 0, 0, fail };
            moq_alloc_t fa = oom_allocator(&oom);
            pb = NULL;
            CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_ERR_NOMEM);
            CHECK(pb == NULL);
            CHECK(oom.alloc_count >= fail);
            CHECK(oom.balance == 0);
        }
    }

    /* ================================================================ */
    /*  2. RAW add_track OOM sweep                                     */
    /* ================================================================ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        cfg.max_buffered_objects = 4;

        uint8_t codec[] = "avc1.42e01e";
        uint8_t init[] = { 0xDE, 0xAD, 0xBE, 0xEF };

        /* Baseline: count add_track allocations. */
        oom_state_t baseline = { 0, 0, 0 };
        moq_alloc_t ba = oom_allocator(&baseline);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&ba, &cfg, &pb) == MOQ_OK);
        uint64_t pre_allocs = baseline.alloc_count;

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ .data = codec, .len = sizeof(codec)-1 };
        tc.init_data = (moq_bytes_t){ .data = init, .len = sizeof(init) };
        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        uint64_t track_allocs = baseline.alloc_count - pre_allocs;
        CHECK(track_allocs >= 2);

        moq_playback_destroy(pb);
        CHECK(baseline.balance == 0);

        /* Sweep add_track failure points. */
        for (uint64_t fail = 1; fail <= track_allocs; fail++) {
            oom_state_t oom = { 0, 0, 0 };
            moq_alloc_t fa = oom_allocator(&oom);
            pb = NULL;
            CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

            oom.fail_at = oom.alloc_count + fail;
            t = (moq_playback_track_t){ 0 };
            moq_result_t rc = moq_playback_add_track(pb, &tc, &t);
            CHECK(rc == MOQ_ERR_NOMEM);
            CHECK(t._opaque == 0);
            CHECK(oom.alloc_count >= oom.fail_at);

            moq_playback_destroy(pb);
            CHECK(oom.balance == 0);
        }
    }

    /* ================================================================ */
    /*  3. CMAF add_track OOM sweep                                    */
    /* ================================================================ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        cfg.max_buffered_objects = 4;

        uint8_t avcc[] = { 0x01, 0x42, 0xE0, 0x1E };
        uint8_t init_seg[512];
        size_t init_len = build_avc_init(init_seg, 90000, 320, 240,
                                          avcc, sizeof(avcc));

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.codec = (moq_bytes_t){ .data = (const uint8_t *)"avc1", .len = 4 };
        tc.init_data = (moq_bytes_t){ .data = init_seg, .len = init_len };

        /* Baseline. */
        oom_state_t baseline = { 0, 0, 0 };
        moq_alloc_t ba = oom_allocator(&baseline);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&ba, &cfg, &pb) == MOQ_OK);
        uint64_t pre = baseline.alloc_count;

        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        uint64_t track_allocs = baseline.alloc_count - pre;
        CHECK(track_allocs >= 2);

        moq_playback_destroy(pb);
        CHECK(baseline.balance == 0);

        /* Sweep. */
        for (uint64_t fail = 1; fail <= track_allocs; fail++) {
            oom_state_t oom = { 0, 0, 0 };
            moq_alloc_t fa = oom_allocator(&oom);
            pb = NULL;
            CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

            oom.fail_at = oom.alloc_count + fail;
            t = (moq_playback_track_t){ 0 };
            moq_result_t rc = moq_playback_add_track(pb, &tc, &t);
            CHECK(rc == MOQ_ERR_NOMEM);
            CHECK(t._opaque == 0);
            CHECK(oom.alloc_count >= oom.fail_at);

            moq_playback_destroy(pb);
            CHECK(oom.balance == 0);
        }

        /* Malformed init returns PROTO, not NOMEM. */
        {
            uint8_t bad[] = { 0x00, 0x00, 0x00, 0x08, 'b', 'a', 'd', '!' };
            moq_playback_track_cfg_t tc_bad;
            moq_playback_track_cfg_init(&tc_bad);
            tc_bad.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
            tc_bad.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
            tc_bad.init_data = (moq_bytes_t){ .data = bad, .len = sizeof(bad) };

            oom_state_t oom = { 0, 0, 0 };
            moq_alloc_t fa = oom_allocator(&oom);
            pb = NULL;
            CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);
            t = (moq_playback_track_t){ 0 };
            CHECK(moq_playback_add_track(pb, &tc_bad, &t) == MOQ_ERR_PROTO);
            moq_playback_destroy(pb);
            CHECK(oom.balance == 0);
        }
    }

    /* ================================================================ */
    /*  4. RAW steady-state: no allocation during push/tick/feedback    */
    /* ================================================================ */
    {
        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = make_props(&fa, 1000, true);
        CHECK(props != NULL);
        uint8_t data[] = { 0xDE, 0xAD };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(&fa, data, 2, &payload) == MOQ_OK);

        uint64_t before = oom.alloc_count;
        oom.fail_at = before + 1;

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload; obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
        CHECK(cmd.u.decode_video.payload == payload);
        CHECK(moq_rcbuf_refcount(payload) == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
        fb.u.queue_pressure.depth = 10;
        fb.u.queue_pressure.max_recommended = 5;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        fb.u.queue_pressure.depth = 2;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        CHECK(oom.alloc_count == before);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /*  5. DECODE_ERROR: no allocation, stale commands purged           */
    /* ================================================================ */
    {
        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props0 = make_props(&fa, 1000, true);
        moq_rcbuf_t *props1 = make_props(&fa, 2000, false);
        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        CHECK(moq_rcbuf_create(&fa, data, 1, &p0) == MOQ_OK);
        CHECK(moq_rcbuf_create(&fa, data, 1, &p1) == MOQ_OK);

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 0; obj.payload = p0; obj.properties = props0;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1;
        obj.object_id = 1; obj.payload = p1; obj.properties = props1;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(p0) == 2);
        CHECK(moq_rcbuf_refcount(p1) == 2);

        uint64_t before = oom.alloc_count;
        oom.fail_at = before + 1;

        moq_playback_feedback_t fb;
        moq_playback_feedback_init(&fb);
        fb.track = t;
        fb.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
        CHECK(moq_playback_handle_feedback(pb, &fb, 0) == MOQ_OK);

        CHECK(oom.alloc_count == before);
        CHECK(moq_rcbuf_refcount(p0) == 1);
        CHECK(moq_rcbuf_refcount(p1) == 1);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_RESET);
        CHECK(cmd.u.reset.reason == MOQ_PLAYBACK_RESET_DECODE_ERROR);

        moq_playback_event_t evt;
        CHECK(moq_playback_poll_event(pb, &evt) == MOQ_OK);
        CHECK(evt.kind == MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING);

        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_rcbuf_decref(props0);
        moq_rcbuf_decref(props1);
        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /*  6. CMAF steady-state: no allocation during push/tick           */
    /* ================================================================ */
    {
        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
        tc.timescale = 90000;
        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        uint8_t mdat[] = { 0xCA, 0xFE };
        uint8_t frag_buf[512];
        size_t frag_len = build_cmaf_frag(frag_buf, 180000, 3000,
                                           0x02000000, mdat, 2);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(&fa, frag_buf, frag_len, &payload) == MOQ_OK);

        uint64_t before = oom.alloc_count;
        oom.fail_at = before + 1;

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(payload) == 2);

        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_CMAF);
        CHECK(cmd.u.decode_cmaf.decode_time_us == 2000000);
        CHECK(cmd.u.decode_cmaf.mdat_len == 2);
        CHECK(moq_rcbuf_refcount(payload) == 2);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        CHECK(oom.alloc_count == before);

        moq_rcbuf_decref(payload);
        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /*  7. RAW audio steady-state: no allocation                       */
    /* ================================================================ */
    {
        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);

        moq_playback_track_cfg_t tc;
        moq_playback_track_cfg_init(&tc);
        tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
        tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);

        moq_rcbuf_t *props = make_props(&fa, 5000, false);
        CHECK(props != NULL);
        uint8_t data[] = { 0xAA };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(&fa, data, 1, &payload) == MOQ_OK);

        uint64_t before = oom.alloc_count;
        oom.fail_at = before + 1;

        moq_playback_object_t obj;
        moq_playback_object_init(&obj);
        obj.track = t; obj.group_id = 1; obj.object_id = 0;
        obj.payload = payload; obj.properties = props;
        CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
        CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

        moq_playback_cmd_t cmd;
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);
        CHECK(moq_playback_poll_command(pb, &cmd) == MOQ_OK);
        CHECK(cmd.kind == MOQ_PLAYBACK_CMD_DECODE_AUDIO);
        moq_playback_cmd_cleanup(&cmd);

        CHECK(oom.alloc_count == before);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
        moq_playback_destroy(pb);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /*  8. CMAF audio add_track OOM sweep                              */
    /* ================================================================ */
    {
        moq_playback_cfg_t cfg;
        moq_playback_cfg_init(&cfg);
        cfg.max_tracks = 1;
        cfg.max_buffered_objects = 4;

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

        oom_state_t baseline = { 0, 0, 0 };
        moq_alloc_t ba = oom_allocator(&baseline);
        moq_playback_t *pb = NULL;
        CHECK(moq_playback_create(&ba, &cfg, &pb) == MOQ_OK);
        uint64_t pre = baseline.alloc_count;

        moq_playback_track_t t = { 0 };
        CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
        uint64_t track_allocs = baseline.alloc_count - pre;
        CHECK(track_allocs >= 2);
        moq_playback_destroy(pb);
        CHECK(baseline.balance == 0);

        for (uint64_t fail = 1; fail <= track_allocs; fail++) {
            oom_state_t oom = { 0, 0, 0 };
            moq_alloc_t fa = oom_allocator(&oom);
            pb = NULL;
            CHECK(moq_playback_create(&fa, &cfg, &pb) == MOQ_OK);
            oom.fail_at = oom.alloc_count + fail;
            t = (moq_playback_track_t){ 0 };
            CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_ERR_NOMEM);
            CHECK(t._opaque == 0);
            CHECK(oom.alloc_count >= oom.fail_at);
            moq_playback_destroy(pb);
            CHECK(oom.balance == 0);
        }
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
