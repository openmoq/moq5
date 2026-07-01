/*
 * Local media demo — client.
 *
 * Connects to a relay, subscribes to the catalog track, parses MSF
 * catalog, decodes initData (base64 CMAF init segment), then
 * subscribes to the discovered video track. Uses moq-playback for
 * all video object processing: LOC/CMAF parsing, timing, keyframe
 * gating, and gap recovery are handled by the pipeline.
 *
 * Usage: moq_demo_client [host] [port] [min-frames]
 */

#include <moq/picoquic.h>
#include <moq/subscriber.h>
#include <moq/playback.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <picoquic_packet_loop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static volatile int running = 1;
static void sigint_handler(int sig) { (void)sig; running = 0; }

/* The demo publisher marks every Nth frame independent (keyframe); the client
 * cross-checks the received keyframe flag against this expected cadence so a peer
 * that mislabels keyframes (e.g. marks every frame independent) is caught rather
 * than passing on keyframe_count alone. Keep in sync with the publisher. */
#define KEYFRAME_INTERVAL 10

typedef struct {
    moq_pq_conn_t    *adapter;
    moq_subscriber_t *sub;
    moq_sub_track_t  *catalog_track;
    moq_sub_track_t  *video_track;
    moq_session_t    *session;
    moq_alloc_t       alloc;
    moq_playback_t   *pb;
    moq_playback_track_t pb_track;
    uint64_t          objects_received;
    uint64_t          decode_cmds;
    uint64_t          min_frames;
    uint64_t          next_expected_seq;
    bool              seq_initialized;
    uint64_t          prev_decode_time;
    int               seq_errors;
    int               playback_errors;
    int               cadence_errors;
    int               keyframe_count;
    bool              catalog_subscribed;
    bool              catalog_parsed;
    bool              init_decoded;
    bool              init_parsed;
    bool              video_subscribed;
    bool              playback_created;
    bool              playback_configured;
    moq_namespace_t   catalog_ns;
    moq_bytes_t       catalog_ns_parts[8];
    uint8_t           catalog_ns_data[128];
    uint8_t           init_data_buf[4096];
    size_t            init_data_len;
} client_ctx_t;

static void on_subscribed(void *ctx, moq_sub_track_t *track) {
    (void)track;
    client_ctx_t *app = (client_ctx_t *)ctx;
    if (track == app->catalog_track)
        fprintf(stderr, "  catalog subscribed\n");
    else
        fprintf(stderr, "  video subscribed\n");
}

static void on_error(void *ctx, moq_sub_track_t *track,
                      moq_request_error_t code, moq_bytes_t reason) {
    (void)ctx; (void)track;
    fprintf(stderr, "  subscribe error: %u \"%.*s\"\n",
        code, (int)reason.len, reason.data ? (const char *)reason.data : "");
    running = 0;
}

static void on_closed(void *ctx, uint64_t code) {
    (void)ctx;
    fprintf(stderr, "  session closed (code=%llu)\n", (unsigned long long)code);
    running = 0;
}

static int client_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    client_ctx_t *ctx = (client_ctx_t *)callback_ctx;
    if (!ctx->adapter) return 0;
    return moq_pq_callback(cnx, stream_id, bytes, length,
                            event, ctx->adapter, stream_ctx);
}

static void handle_catalog_object(client_ctx_t *ctx, moq_sub_object_t *obj,
                                   uint64_t now)
{
    if (!obj->payload) return;

    moq_bytes_t json = {
        moq_rcbuf_data(obj->payload), moq_rcbuf_len(obj->payload) };

    moq_msf_catalog_t cat;
    moq_result_t rc = moq_msf_catalog_parse(&ctx->alloc, json, &cat);
    if (rc != MOQ_OK) {
        fprintf(stderr, "  catalog parse failed: %d\n", rc);
        return;
    }
    fprintf(stderr, "  catalog: version=%d, %zu tracks\n",
        cat.version, cat.track_count);

    const moq_msf_track_t *vt = moq_msf_catalog_find_role(&cat, "video");
    if (!vt) {
        fprintf(stderr, "  catalog: no video track found\n");
        moq_msf_catalog_cleanup(&ctx->alloc, &cat);
        return;
    }
    fprintf(stderr, "  catalog: discovered video track \"%.*s\"\n",
        (int)vt->name.len, (const char *)vt->name.data);

    if (vt->packaging.len != 4 ||
        memcmp(vt->packaging.data, "cmaf", 4) != 0) {
        fprintf(stderr, "  catalog: expected cmaf packaging, got \"%.*s\"\n",
            (int)vt->packaging.len, (const char *)vt->packaging.data);
        moq_msf_catalog_cleanup(&ctx->alloc, &cat);
        return;
    }
    fprintf(stderr, "  catalog: packaging=cmaf\n");

    if (!vt->has_init_data || vt->init_data.len == 0) {
        fprintf(stderr, "  catalog: missing initData for cmaf track\n");
        moq_msf_catalog_cleanup(&ctx->alloc, &cat);
        return;
    }

    /* Decode initData (base64 → raw init segment). */
    {
        moq_rcbuf_t *init_raw = NULL;
        rc = moq_msf_decode_init_data(&ctx->alloc, vt->init_data, &init_raw);
        if (rc == MOQ_OK && init_raw) {
            ctx->init_decoded = true;
            size_t ilen = moq_rcbuf_len(init_raw);
            fprintf(stderr, "  initData decoded: %zuB\n", ilen);

            if (ilen <= sizeof(ctx->init_data_buf)) {
                memcpy(ctx->init_data_buf, moq_rcbuf_data(init_raw), ilen);
                ctx->init_data_len = ilen;
                ctx->init_parsed = true;
            }
            moq_rcbuf_decref(init_raw);
        } else {
            fprintf(stderr, "  initData decode failed: %d\n", rc);
        }
    }

    /* Create playback pipeline with CMAF track. */
    if (ctx->init_parsed && !ctx->playback_created) {
        moq_playback_cfg_t pcfg;
        moq_playback_cfg_init(&pcfg);
        pcfg.max_tracks = 1;
        pcfg.max_buffered_objects = 128;
        pcfg.max_backlog_groups = 64;
        pcfg.gap_timeout_us = 60000000;

        rc = moq_playback_create(&ctx->alloc, &pcfg, &ctx->pb);
        if (rc != MOQ_OK) {
            fprintf(stderr, "  playback create failed: %d\n", rc);
        } else {
            moq_playback_track_cfg_t tcfg;
            moq_playback_track_cfg_init(&tcfg);
            tcfg.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
            tcfg.packaging = MOQ_PLAYBACK_PACKAGING_CMAF;
            tcfg.init_data = (moq_bytes_t){
                .data = ctx->init_data_buf, .len = ctx->init_data_len };

            rc = moq_playback_add_track(ctx->pb, &tcfg, &ctx->pb_track);
            if (rc != MOQ_OK) {
                fprintf(stderr, "  playback add_track failed: %d\n", rc);
            } else {
                ctx->playback_created = true;
                fprintf(stderr, "  playback pipeline created\n");
            }
        }
    }

    /* Subscribe to discovered video track. */
    if (!ctx->video_subscribed) {
        fprintf(stderr, "  using catalog namespace (inherited)\n");
        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        tcfg.track_namespace = ctx->catalog_ns;
        tcfg.track_name = vt->name;

        moq_result_t src = moq_sub_subscribe(ctx->sub, &tcfg, now,
                                               &ctx->video_track);
        if (src == MOQ_OK) {
            ctx->video_subscribed = true;
            fprintf(stderr, "  subscribed to discovered video track\n");
        } else {
            fprintf(stderr, "  video subscribe failed: %d\n", src);
        }
        moq_pq_service(ctx->adapter, now);
    }

    ctx->catalog_parsed = true;
    moq_msf_catalog_cleanup(&ctx->alloc, &cat);
}

static void handle_video_object(client_ctx_t *ctx, moq_sub_object_t *obj,
                                 uint64_t now)
{
    if (!ctx->playback_created) return;

    moq_result_t rc = moq_playback_push_sub_object(
        ctx->pb, ctx->pb_track, obj, now);
    if (rc != MOQ_OK) {
        fprintf(stderr, "  playback push error: rc=%d g=%llu o=%llu\n",
            rc, (unsigned long long)obj->group_id,
            (unsigned long long)obj->object_id);
        ctx->playback_errors++;
        return;
    }

    ctx->objects_received++;
}

static void process_playback(client_ctx_t *ctx, uint64_t now)
{
    if (!ctx->playback_created) return;

    moq_result_t tr = moq_playback_tick(ctx->pb, now);
    if (tr != MOQ_OK && tr != MOQ_ERR_WOULD_BLOCK) {
        fprintf(stderr, "  playback tick error: rc=%d\n", tr);
        ctx->playback_errors++;
    }

    moq_playback_cmd_t cmd;
    while (moq_playback_poll_command(ctx->pb, &cmd) == MOQ_OK) {
        switch (cmd.kind) {
        case MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO:
            ctx->playback_configured = true;
            fprintf(stderr, "  playback configure: %ux%u\n",
                cmd.u.configure_video.width, cmd.u.configure_video.height);
            if (cmd.u.configure_video.width != 1920 ||
                cmd.u.configure_video.height != 1080) {
                fprintf(stderr, "  playback configure ERROR: unexpected dims\n");
                ctx->playback_errors++;
            }
            if (!cmd.u.configure_video.codec_config) {
                fprintf(stderr, "  playback configure ERROR: missing codec_config\n");
                ctx->playback_errors++;
            }
            break;

        case MOQ_PLAYBACK_CMD_DECODE_CMAF: {
            bool is_key = cmd.u.decode_cmaf.keyframe;
            if (is_key) ctx->keyframe_count++;

            if (ctx->decode_cmds > 0 &&
                cmd.u.decode_cmaf.decode_time_us < ctx->prev_decode_time) {
                fprintf(stderr, "  playback DTS ERROR: non-monotonic\n");
                ctx->playback_errors++;
            }
            ctx->prev_decode_time = cmd.u.decode_cmaf.decode_time_us;

            /* Verify mdat contains frame marker. */
            if (!cmd.u.decode_cmaf.fragment || cmd.u.decode_cmaf.mdat_len == 0) {
                fprintf(stderr, "  playback ERROR: missing fragment/mdat\n");
                ctx->seq_errors++;
            } else {
                const uint8_t *fdata = moq_rcbuf_data(cmd.u.decode_cmaf.fragment);
                const uint8_t *mdat = fdata + cmd.u.decode_cmaf.mdat_offset;
                size_t mlen = cmd.u.decode_cmaf.mdat_len;
                char mdat_str[128];
                if (mlen > sizeof(mdat_str) - 1) mlen = sizeof(mdat_str) - 1;
                memcpy(mdat_str, mdat, mlen);
                mdat_str[mlen] = '\0';

                unsigned long long parsed_seq = 0;
                if (sscanf(mdat_str, "frame=%llu", &parsed_seq) != 1) {
                    fprintf(stderr, "  playback ERROR: bad mdat marker\n");
                    ctx->seq_errors++;
                } else {
                    uint64_t seq = (uint64_t)parsed_seq;
                    if (!ctx->seq_initialized) {
                        ctx->next_expected_seq = seq;
                        ctx->seq_initialized = true;
                    }
                    if (seq != ctx->next_expected_seq) {
                        fprintf(stderr, "  SEQ ERROR: got %llu expected %llu\n",
                            (unsigned long long)seq,
                            (unsigned long long)ctx->next_expected_seq);
                        ctx->seq_errors++;
                    }
                    ctx->next_expected_seq = seq + 1;

                    /* Cross-check the keyframe flag against the publisher's
                     * cadence (frame N is independent iff N % KEYFRAME_INTERVAL
                     * == 0). Counting keyframes alone would accept a peer that
                     * mislabels them (e.g. marks every frame independent). */
                    bool expect_key = (seq % KEYFRAME_INTERVAL) == 0;
                    if (is_key != expect_key) {
                        fprintf(stderr, "  CADENCE ERROR: frame %llu key=%d "
                            "expected %d\n", (unsigned long long)seq,
                            is_key, expect_key);
                        ctx->cadence_errors++;
                    }
                }
            }

            fprintf(stderr, "  playback decode: f=%llu dts=%llu %s mdat=%zuB\n",
                (unsigned long long)(ctx->next_expected_seq - 1),
                (unsigned long long)cmd.u.decode_cmaf.decode_time_us,
                is_key ? "KEY" : "delta",
                cmd.u.decode_cmaf.mdat_len);

            ctx->decode_cmds++;
            break;
        }

        case MOQ_PLAYBACK_CMD_RESET:
            fprintf(stderr, "  playback RESET: reason=%d\n",
                cmd.u.reset.reason);
            ctx->playback_errors++;
            break;

        default:
            break;
        }
        moq_playback_cmd_cleanup(&cmd);
    }

    moq_playback_event_t evt;
    while (moq_playback_poll_event(ctx->pb, &evt) == MOQ_OK) {
        switch (evt.kind) {
        case MOQ_PLAYBACK_EVENT_OBJECT_DROPPED:
            fprintf(stderr, "  playback dropped: reason=%d g=%llu o=%llu\n",
                evt.u.object_dropped.reason,
                (unsigned long long)evt.u.object_dropped.group_id,
                (unsigned long long)evt.u.object_dropped.object_id);
            ctx->playback_errors++;
            break;
        case MOQ_PLAYBACK_EVENT_GAP_DETECTED:
            fprintf(stderr, "  playback GAP: group=%llu\n",
                (unsigned long long)evt.u.gap_detected.group_id);
            ctx->playback_errors++;
            break;
        case MOQ_PLAYBACK_EVENT_BACKLOG_SHED:
            fprintf(stderr, "  playback BACKLOG_SHED: dropped=%u\n",
                evt.u.backlog_shed.dropped_groups);
            ctx->playback_errors++;
            break;
        case MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING:
            fprintf(stderr, "  playback KEYFRAME_WAITING\n");
            break;
        case MOQ_PLAYBACK_EVENT_TRACK_ENDED:
            fprintf(stderr, "  playback TRACK_ENDED\n");
            break;
        default:
            break;
        }
    }
}

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    client_ctx_t *ctx = (client_ctx_t *)callback_ctx;
    (void)callback_arg;

    if (cb_mode != picoquic_packet_loop_after_receive &&
        cb_mode != picoquic_packet_loop_after_send)
        return 0;
    if (!ctx->adapter) return 0;

    uint64_t now = picoquic_get_quic_time(quic);

    if (moq_pq_service(ctx->adapter, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }
    moq_sub_tick(ctx->sub, now);

    if (moq_session_state(ctx->session) == MOQ_SESS_ESTABLISHED &&
        !ctx->catalog_subscribed) {
        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("demo"),
            MOQ_BYTES_LITERAL("camera-1"),
        };
        size_t ns_off = 0;
        for (size_t i = 0; i < 2; i++) {
            memcpy(ctx->catalog_ns_data + ns_off,
                   ns_parts[i].data, ns_parts[i].len);
            ctx->catalog_ns_parts[i] = (moq_bytes_t){
                ctx->catalog_ns_data + ns_off, ns_parts[i].len };
            ns_off += ns_parts[i].len;
        }
        ctx->catalog_ns.parts = ctx->catalog_ns_parts;
        ctx->catalog_ns.count = 2;

        moq_sub_track_cfg_t tcfg;
        moq_sub_track_cfg_init(&tcfg);
        tcfg.track_namespace = ctx->catalog_ns;
        tcfg.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);

        moq_result_t crc = moq_sub_subscribe(ctx->sub, &tcfg, now,
                                               &ctx->catalog_track);
        if (crc == MOQ_OK) {
            ctx->catalog_subscribed = true;
            fprintf(stderr, "  catalog subscribe sent\n");
        } else {
            fprintf(stderr, "  catalog subscribe failed: %d\n", crc);
        }
        if (moq_pq_service(ctx->adapter, now) < 0) {
            running = 0;
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    moq_sub_object_t obj;
    while (moq_sub_poll_object(ctx->sub, &obj) == MOQ_OK) {
        if (obj.track == ctx->catalog_track)
            handle_catalog_object(ctx, &obj, now);
        else if (obj.track == ctx->video_track)
            handle_video_object(ctx, &obj, now);
        moq_sub_object_cleanup(&obj);
    }

    process_playback(ctx, now);

    if (moq_pq_service(ctx->adapter, now) < 0) {
        running = 0;
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (ctx->decode_cmds >= ctx->min_frames)
        running = 0;

    if (!running) return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    return 0;
}

int main(int argc, char *argv[])
{
    const char *host = argc > 1 ? argv[1] : "localhost";
    int port = argc > 2 ? atoi(argv[2]) : 4443;
    int min_frames = argc > 3 ? atoi(argv[3]) : 20;
    if (min_frames <= 0) min_frames = 20;

    signal(SIGINT, sigint_handler);

    client_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.alloc = *moq_alloc_default();
    ctx.min_frames = (uint64_t)min_frames;

    uint64_t now = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        1, NULL, NULL, NULL, MOQ_PQ_ALPN_DEFAULT,
        NULL, NULL, NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    if (!quic) { fprintf(stderr, "picoquic_create failed\n"); return 1; }
    picoquic_set_null_verifier(quic);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    picoquic_cnx_t *cnx = picoquic_create_client_cnx(
        quic, (struct sockaddr *)&addr, now, 0,
        host, MOQ_PQ_ALPN_DEFAULT, client_callback, &ctx);
    if (!cnx) { picoquic_free(quic); return 1; }

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &ctx.alloc, MOQ_PERSPECTIVE_CLIENT);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;

    if (moq_session_create(&scfg, now, &ctx.session) != MOQ_OK) {
        picoquic_free(quic); return 1;
    }

    moq_pq_conn_cfg_t acfg;
    moq_pq_conn_cfg_init(&acfg);
    acfg.session = ctx.session;
    acfg.cnx = cnx;
    acfg.alloc = &ctx.alloc;
    if (moq_pq_conn_create(&acfg, &ctx.adapter) != 0) {
        moq_session_destroy(ctx.session);
        picoquic_free(quic); return 1;
    }

    moq_sub_cfg_t subcfg;
    moq_sub_cfg_init(&subcfg);
    subcfg.callbacks.ctx = &ctx;
    subcfg.callbacks.on_subscribed = on_subscribed;
    subcfg.callbacks.on_subscribe_error = on_error;
    subcfg.callbacks.on_closed = on_closed;

    if (moq_sub_create(ctx.session, &ctx.alloc, &subcfg, &ctx.sub) != MOQ_OK) {
        moq_pq_conn_destroy(ctx.adapter);
        moq_session_destroy(ctx.session);
        picoquic_free(quic); return 1;
    }

    if (moq_session_start(ctx.session, now) != MOQ_OK ||
        moq_pq_service(ctx.adapter, now) < 0) {
        moq_sub_destroy(ctx.sub);
        moq_pq_conn_destroy(ctx.adapter);
        moq_session_destroy(ctx.session);
        picoquic_free(quic); return 1;
    }
    /* picoquic_create_client_cnx already started the TLS handshake. */

    fprintf(stderr, "Connecting to %s:%d (need %d frames)...\n",
        host, port, min_frames);

    int rc = picoquic_packet_loop(quic, 0, 0, 0, 0, 0,
                                   loop_callback, &ctx);

    if (ctx.pb) moq_playback_destroy(ctx.pb);
    if (ctx.sub) moq_sub_destroy(ctx.sub);
    if (ctx.adapter) {
        moq_session_t *s = moq_pq_conn_session(ctx.adapter);
        moq_pq_conn_destroy(ctx.adapter);
        if (s) moq_session_destroy(s);
    }
    picoquic_free(quic);

    fprintf(stderr, "\n=== Client Summary ===\n");
    fprintf(stderr, "  catalog:         %s\n", ctx.catalog_parsed ? "parsed" : "missing");
    fprintf(stderr, "  initData:        %s\n", ctx.init_decoded ? "decoded" : "missing");
    fprintf(stderr, "  CMAF init:       %s\n", ctx.init_parsed ? "parsed" : "missing");
    fprintf(stderr, "  video discovery: %s\n", ctx.video_subscribed ? "yes" : "no");
    fprintf(stderr, "  playback:        %s\n", ctx.playback_created ? "active" : "inactive");
    fprintf(stderr, "  configured:      %s\n", ctx.playback_configured ? "yes" : "no");
    fprintf(stderr, "  objects pushed:  %llu\n",
        (unsigned long long)ctx.objects_received);
    fprintf(stderr, "  decode commands: %llu\n",
        (unsigned long long)ctx.decode_cmds);
    fprintf(stderr, "  keyframes:       %d\n", ctx.keyframe_count);
    fprintf(stderr, "  seq errors:      %d\n", ctx.seq_errors);
    fprintf(stderr, "  cadence errors:  %d\n", ctx.cadence_errors);
    fprintf(stderr, "  playback errors: %d\n", ctx.playback_errors);

    bool pass = ctx.catalog_parsed && ctx.init_decoded && ctx.init_parsed &&
                ctx.video_subscribed && ctx.playback_created &&
                ctx.playback_configured &&
                ctx.decode_cmds >= ctx.min_frames &&
                ctx.seq_errors == 0 && ctx.playback_errors == 0 &&
                ctx.cadence_errors == 0 &&
                ctx.keyframe_count > 0;
    fprintf(stderr, "  result:          %s\n", pass ? "PASS" : "FAIL");
    if (pass) fprintf(stderr, "PASS: client received %llu validated frames\n",
        (unsigned long long)ctx.decode_cmds);

    return pass ? 0 : 1;
}
