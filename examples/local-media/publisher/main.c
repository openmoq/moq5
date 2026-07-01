/*
 * Local media demo — publisher.
 *
 * Listens as a QUIC server. Publishes an MSF catalog with a
 * base64-encoded CMAF init segment in initData, then publishes
 * synthetic CMAF media fragments as video objects with LOC-01
 * properties (timestamp + video frame marking).
 *
 * Usage: moq_demo_publisher <cert.pem> <key.pem> [port] [frames]
 */

#include <moq/picoquic.h>
#include <moq/publisher.h>
#include <moq/loc.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <picoquic_packet_loop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static volatile int running = 1;
static void sigint_handler(int sig) { (void)sig; running = 0; }

#define KEYFRAME_INTERVAL 10
#define TIMESCALE 90000
#define FRAME_DURATION 3000  /* 90000 / 30 fps */
#define VIDEO_WIDTH 1920
#define VIDEO_HEIGHT 1080
#define DRAIN_GRACE_US 500000

/* -- Synthetic CMAF box builders ------------------------------------ */

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

static size_t build_init_segment(uint8_t *buf, size_t cap)
{
    static const uint8_t avcc_data[] = { 0x01, 0x42, 0xE0, 0x1E, 0xFF };
    size_t avcc_sz = 8 + sizeof(avcc_data);
    size_t avc1_sz = 8 + 78 + avcc_sz;
    size_t stsd_sz = 8 + 8 + avc1_sz;
    size_t stbl_sz = 8 + stsd_sz;
    size_t minf_sz = 8 + stbl_sz;
    size_t mdhd_sz = 8 + 4 + 12;
    size_t mdia_sz = 8 + mdhd_sz + minf_sz;
    size_t trak_sz = 8 + mdia_sz;
    size_t moov_sz = 8 + trak_sz;
    size_t total = 20 + moov_sz;
    if (total > cap) return 0;
    size_t p = 0;

    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    p += box_hdr(buf + p, (uint32_t)moov_sz, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_sz, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_sz, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_sz, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, TIMESCALE); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_sz, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_sz, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_sz, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)avc1_sz, "avc1");
    memset(buf + p, 0, 78);
    wr16(buf + p + 24, VIDEO_WIDTH);
    wr16(buf + p + 26, VIDEO_HEIGHT);
    p += 78;
    p += box_hdr(buf + p, (uint32_t)avcc_sz, "avcC");
    memcpy(buf + p, avcc_data, sizeof(avcc_data));
    p += sizeof(avcc_data);

    return p;
}

static size_t build_fragment(uint8_t *buf, size_t cap,
                              uint64_t frame_num,
                              const uint8_t *mdat_payload,
                              size_t mdat_len)
{
    uint64_t base_time = frame_num * FRAME_DURATION;
    size_t trun_sz = 8 + 8 + 8; /* ver/flags + count + per-sample dur+size */
    size_t tfdt_sz = 8 + 4 + 8;
    size_t tfhd_sz = 8 + 8;
    size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
    size_t moof_sz = 8 + traf_sz;
    size_t total = moof_sz + 8 + mdat_len;
    if (total > cap) return 0;
    size_t p = 0;

    p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");
    p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)tfdt_sz, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;

    /* trun: flags 0x300 = per-sample duration + size */
    p += box_hdr(buf + p, (uint32_t)trun_sz, "trun");
    wr32(buf + p, 0x00000300); p += 4;
    wr32(buf + p, 1); p += 4;
    wr32(buf + p, FRAME_DURATION); p += 4;
    wr32(buf + p, (uint32_t)mdat_len); p += 4;

    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    memcpy(buf + p, mdat_payload, mdat_len);
    p += mdat_len;

    return p;
}

/* -- App context ---------------------------------------------------- */

typedef struct {
    moq_pq_conn_t   *adapter;
    picoquic_cnx_t   *cnx;          /* the one QUIC connection the adapter owns */
    moq_publisher_t  *pub;
    moq_pub_track_t  *catalog_track;
    moq_pub_track_t  *video_track;
    moq_alloc_t       alloc;
    uint64_t          frame_count;
    uint64_t          max_frames;
    uint64_t          first_pub_time;
    uint64_t          drain_start;
    bool              ready;
    bool              draining;
    uint8_t           init_seg[512];
    size_t            init_seg_len;
} pub_ctx_t;

static moq_result_t publish_catalog(pub_ctx_t *ctx, uint64_t now)
{
    moq_rcbuf_t *init_b64 = NULL;
    moq_result_t rc = moq_msf_encode_init_data(&ctx->alloc,
        (moq_bytes_t){ ctx->init_seg, ctx->init_seg_len }, &init_b64);
    if (rc != MOQ_OK) return rc;

    moq_msf_track_t vt;
    memset(&vt, 0, sizeof(vt));
    vt.struct_size = sizeof(vt);
    vt.name = MOQ_BYTES_LITERAL("video");
    vt.packaging = MOQ_BYTES_LITERAL("cmaf");
    vt.is_live = true;
    vt.has_role = true;
    vt.role = MOQ_BYTES_LITERAL("video");
    vt.has_codec = true;
    vt.codec = MOQ_BYTES_LITERAL("avc1.42e01e");
    vt.has_width = true;
    vt.width = VIDEO_WIDTH;
    vt.has_height = true;
    vt.height = VIDEO_HEIGHT;
    vt.has_framerate = true;
    vt.framerate_millis = 30000;
    vt.has_target_latency = true;
    vt.target_latency = 2000;
    vt.has_timescale = true;
    vt.timescale = TIMESCALE;
    vt.has_init_data = true;
    vt.init_data = (moq_bytes_t){
        moq_rcbuf_data(init_b64), moq_rcbuf_len(init_b64) };

    moq_msf_catalog_t cat;
    memset(&cat, 0, sizeof(cat));
    cat.struct_size = sizeof(cat);
    cat.version = MOQ_MSF_VERSION;
    cat.tracks = &vt;
    cat.track_count = 1;

    moq_rcbuf_t *json = NULL;
    rc = moq_msf_catalog_encode(&ctx->alloc, &cat, &json);
    moq_rcbuf_decref(init_b64);
    if (rc != MOQ_OK) return rc;

    moq_pub_retained_object_t robj = { .object_id = 0, .payload = json };
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = &robj;
    rg.object_count = 1;
    rc = moq_pub_set_retained_group(ctx->pub, ctx->catalog_track, &rg);
    moq_rcbuf_decref(json);

    if (rc == MOQ_OK)
        fprintf(stderr, "  catalog retained for Joining FETCH (initData=%zuB)\n",
            ctx->init_seg_len);
    else
        fprintf(stderr, "  catalog retain failed: %d\n", rc);
    return rc;
}

static void on_subscriber_joined(void *ctx, moq_pub_track_t *track) {
    (void)ctx; (void)track;
    fprintf(stderr, "  subscriber joined\n");
}

static void on_subscriber_left(void *ctx, moq_pub_track_t *track) {
    (void)ctx; (void)track;
    fprintf(stderr, "  subscriber left\n");
    running = 0;
}

static void on_closed(void *ctx, uint64_t code) {
    (void)ctx;
    fprintf(stderr, "  session closed (code=%llu)\n", (unsigned long long)code);
    running = 0;
}

static int server_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    pub_ctx_t *ctx = (pub_ctx_t *)callback_ctx;
    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));

    if (event == picoquic_callback_almost_ready ||
        event == picoquic_callback_ready) {
        if (ctx->adapter) {
            /* Single-session demo: the adapter is bound to one connection.
             * Refuse any additional connection rather than mixing its events
             * into the first session (a later almost_ready/ready for the SAME
             * connection is fine). */
            if (cnx != ctx->cnx) picoquic_close(cnx, PICOQUIC_TRANSPORT_SERVER_BUSY);
            return 0;
        }

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &ctx->alloc, MOQ_PERSPECTIVE_SERVER);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 64;

        moq_session_t *session = NULL;
        if (moq_session_create(&scfg, now, &session) != MOQ_OK) return -1;

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init(&acfg);
        acfg.session = session;
        acfg.cnx = cnx;
        acfg.alloc = &ctx->alloc;
        if (moq_pq_conn_create(&acfg, &ctx->adapter) != 0) {
            moq_session_destroy(session);
            return -1;
        }
        ctx->cnx = cnx;

        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        pcfg.callbacks.ctx = ctx;
        pcfg.callbacks.on_subscriber_joined = on_subscriber_joined;
        pcfg.callbacks.on_subscriber_left = on_subscriber_left;
        pcfg.callbacks.on_closed = on_closed;

        if (moq_pub_create(session, &ctx->alloc, &pcfg, &ctx->pub) != MOQ_OK) {
            moq_pq_conn_destroy(ctx->adapter);
            moq_session_destroy(session);
            ctx->adapter = NULL;
            return -1;
        }

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("demo"),
            MOQ_BYTES_LITERAL("camera-1"),
        };

        moq_pub_track_cfg_t ctcfg;
        moq_pub_track_cfg_init(&ctcfg);
        ctcfg.track_namespace.parts = ns_parts;
        ctcfg.track_namespace.count = 2;
        ctcfg.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_pub_add_track(ctx->pub, &ctcfg, now, &ctx->catalog_track) != MOQ_OK)
            return -1;

        moq_pub_track_cfg_t vtcfg;
        moq_pub_track_cfg_init(&vtcfg);
        vtcfg.track_namespace.parts = ns_parts;
        vtcfg.track_namespace.count = 2;
        vtcfg.track_name = MOQ_BYTES_LITERAL("video");
        if (moq_pub_add_track(ctx->pub, &vtcfg, now, &ctx->video_track) != MOQ_OK)
            return -1;

        if (publish_catalog(ctx, now) != MOQ_OK) return -1;

        ctx->ready = true;
        fprintf(stderr, "  connection ready, catalog + video tracks added\n");
        return 0;
    }

    /* Drive the session only from the connection that owns the adapter; refuse
     * events from any other connection instead of feeding them to the wrong
     * session. */
    if (ctx->adapter && cnx == ctx->cnx)
        return moq_pq_callback(cnx, stream_id, bytes, length,
                                event, ctx->adapter, stream_ctx);
    if (ctx->adapter && cnx != ctx->cnx)
        picoquic_close(cnx, PICOQUIC_TRANSPORT_SERVER_BUSY);
    return 0;
}

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    pub_ctx_t *ctx = (pub_ctx_t *)callback_ctx;
    (void)callback_arg;

    if (cb_mode != picoquic_packet_loop_after_receive &&
        cb_mode != picoquic_packet_loop_after_send)
        return 0;
    if (!ctx->adapter || !ctx->ready) return 0;

    uint64_t now = picoquic_get_quic_time(quic);

    if (moq_pq_service(ctx->adapter, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }
    moq_pub_tick(ctx->pub, now);
    if (moq_pq_service(ctx->adapter, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (moq_pub_has_subscriber(ctx->pub, ctx->video_track) &&
        ctx->frame_count < ctx->max_frames) {

        if (ctx->first_pub_time == 0) ctx->first_pub_time = now;
        uint64_t target = ctx->first_pub_time + ctx->frame_count * 33333;
        if (now < target) return 0;

        bool is_key = (ctx->frame_count % KEYFRAME_INTERVAL) == 0;
        uint64_t timestamp_us = 1000000 + ctx->frame_count * 33333;

        /* LOC properties. */
        moq_loc_headers_t lh;
        moq_loc_headers_init(&lh);
        lh.has_timestamp = true;
        lh.timestamp = timestamp_us;
        lh.has_video_frame_marking = true;
        lh.video_frame_marking.start_of_frame = true;
        lh.video_frame_marking.end_of_frame = true;
        lh.video_frame_marking.independent = is_key;

        moq_rcbuf_t *props = NULL;
        if (moq_loc_encode(&ctx->alloc, MOQ_LOC_PROFILE_01, &lh,
                            &props) != MOQ_OK) {
            running = 0;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }

        /* Build CMAF fragment with "frame=N" as mdat. */
        char mdat_str[64];
        int mdat_len = snprintf(mdat_str, sizeof(mdat_str),
            "frame=%llu", (unsigned long long)ctx->frame_count);

        uint8_t frag_buf[256];
        size_t frag_len = build_fragment(frag_buf, sizeof(frag_buf),
            ctx->frame_count, (const uint8_t *)mdat_str, (size_t)mdat_len);

        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(&ctx->alloc, frag_buf, frag_len,
                              &payload) != MOQ_OK) {
            moq_rcbuf_decref(props);
            running = 0;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }

        moq_pub_object_cfg_t obj;
        moq_pub_object_cfg_init(&obj);
        obj.group_id = ctx->frame_count;
        obj.object_id = 0;
        obj.payload = payload;
        obj.properties = props;
        obj.end_of_group = true;

        moq_result_t rc = moq_pub_write_object_ex(
            ctx->pub, ctx->video_track, &obj, now);

        if (rc == MOQ_OK) {
            fprintf(stderr, "  pub: f=%llu ts=%llu %s %zuB\n",
                (unsigned long long)ctx->frame_count,
                (unsigned long long)timestamp_us,
                is_key ? "KEY" : "delta",
                frag_len);
            ctx->frame_count++;
        }

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);

        if (moq_pq_service(ctx->adapter, now) < 0) {
            running = 0;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    if (ctx->frame_count >= ctx->max_frames && ctx->ready && !ctx->draining) {
        ctx->draining = true;
        ctx->drain_start = now;
        fprintf(stderr, "  published %llu frames, draining...\n",
            (unsigned long long)ctx->frame_count);
    }

    if (ctx->draining && now - ctx->drain_start >= DRAIN_GRACE_US) {
        fprintf(stderr, "  drain complete, exiting\n");
        running = 0;
    }

    if (!running) return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <cert.pem> <key.pem> [port] [frames]\n",
            argv[0]);
        return 1;
    }

    int port = argc > 3 ? atoi(argv[3]) : 4443;
    int max_frames = argc > 4 ? atoi(argv[4]) : 60;
    if (max_frames <= 0) max_frames = 60;

    signal(SIGINT, sigint_handler);

    pub_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.alloc = *moq_alloc_default();
    ctx.max_frames = (uint64_t)max_frames;
    ctx.init_seg_len = build_init_segment(ctx.init_seg, sizeof(ctx.init_seg));

    uint64_t now = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        8, argv[1], argv[2], NULL, MOQ_PQ_ALPN_DEFAULT,
        server_callback, &ctx,
        NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) { fprintf(stderr, "picoquic_create failed\n"); return 1; }

    fprintf(stderr, "Publisher listening on port %d (%d frames)...\n",
        port, max_frames);

    int rc = picoquic_packet_loop(quic, port, 0, 0, 0, 0,
                                   loop_callback, &ctx);

    if (ctx.pub) moq_pub_destroy(ctx.pub);
    if (ctx.adapter) {
        moq_session_t *s = moq_pq_conn_session(ctx.adapter);
        moq_pq_conn_destroy(ctx.adapter);
        if (s) moq_session_destroy(s);
    }
    picoquic_free(quic);

    fprintf(stderr, "publisher done (rc=%d, frames=%llu)\n",
        rc, (unsigned long long)ctx.frame_count);
    return (ctx.frame_count >= ctx.max_frames) ? 0 : 1;
}
