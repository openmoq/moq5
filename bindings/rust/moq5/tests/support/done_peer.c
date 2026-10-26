/* A native publisher peer for the Rust relay rows: it announces a namespace
 * to a moq5-relay on loopback, serves the MSF catalog and one raw video
 * track to whoever subscribes through the relay, writes a few objects and
 * then completes the track's subscription with a chosen status code -- the
 * completion the media sender's public API never issues on its own (its
 * tracks end with an END_OF_TRACK first). Built by the relay row against
 * the picoquic SDK's own pkg-config closure; never installed.
 *
 *   done_peer --port <relay port> --draft <18|22> --namespace a/b
 *             --status <wire code> --objects <count>
 *
 * Progress goes to stdout, one line per step, which the Rust row reads:
 * "announced", "subscribed catalog", "subscribed video", "wrote <n>",
 * "done <status>". The peer then stays connected until SIGTERM (or 60 s),
 * so the relay forwards the completion before the session closes. Exit 0
 * on SIGTERM after the done, 1 on a fatal session, 2 on bad arguments or a
 * timeout. MOQ5_DONE_PEER_EXIT_STATUS=<n> (0..255) makes the exit status
 * <n> once the done is sent, however the peer then ends: the Rust row's
 * control that a peer failing after its progress fails the row. */
#define _POSIX_C_SOURCE 200809L

#include <moq/loc.h>
#include <moq/msf.h>
#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/types.h>
#include <moq/version.h>

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_NS_PARTS 8
#define OBJECT_BYTES 32
#define LINGER_LIMIT_US (60u * 1000u * 1000u)

static volatile sig_atomic_t g_stop;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

typedef struct peer {
    moq_version_t version;
    moq_bytes_t   ns_parts[MAX_NS_PARTS];
    size_t        ns_count;
    uint64_t      status;
    uint32_t      objects;

    bool announced, ns_accepted;
    bool catalog_live, catalog_opened, catalog_written, catalog_closed;
    moq_subscription_t   catalog_sub;
    moq_subgroup_handle_t catalog_sg;
    bool video_live, video_opened, video_closed;
    moq_subscription_t   video_sub;
    moq_subgroup_handle_t video_sg;
    uint32_t video_written;
    moq_rcbuf_t *catalog_json;
    /* Read by the main thread: the pump thread's completion and its view of
     * the session, published through atomics; the main thread never touches
     * the session itself. */
    atomic_bool done_sent;
    atomic_int  session_state;
    atomic_int  fatal;
} peer_t;

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

static bool span_is(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && memcmp(b.data, s, b.len) == 0;
}

static void fatal(peer_t *p, const char *what, moq_result_t rc)
{
    fprintf(stdout, "fatal %s rc=%d\n", what, (int)rc);
    fflush(stdout);
    atomic_store(&p->fatal, 1);
}

static void step(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);
    fputc('\n', stdout);
    fflush(stdout);
}

static uint64_t wallclock_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

static uint64_t monotonic_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* The catalog the Rust receiver discovers the track from: one live raw
 * ("loc") video track, the fields the media sender emits for one. */
static bool build_catalog(peer_t *p)
{
    moq_msf_track_t mt;
    memset(&mt, 0, sizeof mt);
    mt.struct_size = sizeof mt;
    mt.name = lit("video");
    mt.packaging = lit("loc");
    mt.is_live = true;
    mt.has_role = true;
    mt.role = lit("video");
    mt.has_codec = true;
    mt.codec = lit("avc1.42e01e");
    mt.has_bitrate = true;
    mt.bitrate = 500000;
    moq_msf_catalog_t cat;
    memset(&cat, 0, sizeof cat);
    cat.struct_size = sizeof cat;
    cat.version = MOQ_MSF_VERSION;
    cat.tracks = &mt;
    cat.track_count = 1;
    cat.has_generated_at = true;
    cat.generated_at = wallclock_ms();
    moq_result_t rc = moq_msf_catalog_encode(moq_alloc_default(), &cat, &p->catalog_json);
    if (rc != MOQ_OK) {
        fatal(p, "catalog encode", rc);
        return false;
    }
    return true;
}

/* The object properties the media sender puts on a raw video object for
 * the draft: LOC-01 headers on draft 18, typed LOC-04 metadata on draft 22.
 * NULL means none; false means a refusal. */
static bool object_properties(peer_t *p, uint32_t index, moq_rcbuf_t **out)
{
    *out = NULL;
    moq_result_t rc;
    if (p->version == MOQ_VERSION_DRAFT_22) {
        moq_loc_metadata_t m;
        moq_loc_metadata_init_sized(&m, sizeof m);
        m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_TIMESCALE |
                    MOQ_LOC_METADATA_HAS_MARKING;
        m.timestamp = (uint64_t)index * 1000u;
        m.timescale = 1000000u;
        m.marking_len = 1;
        m.marking[0] = index == 0 ? 0x20u : 0x00u;   /* the I bit on the sync object */
        rc = moq_loc_encode_metadata(moq_alloc_default(), MOQ_VERSION_DRAFT_22,
                                     MOQ_LOC_PROFILE_04, &m, out);
    } else {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = (uint64_t)index * 1000u;
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = index == 0;
        rc = moq_loc_encode(moq_alloc_default(), p->version, MOQ_LOC_PROFILE_01, &h, out);
    }
    if (rc != MOQ_OK) {
        fatal(p, "object properties", rc);
        return false;
    }
    return true;
}

/* One step of the publishing flow; a WOULD_BLOCK leaves the step for the
 * next pump. */
static void drive(peer_t *p, moq_session_t *s, uint64_t now_us)
{
    moq_result_t rc;
    if (!p->announced) {
        moq_publish_namespace_cfg_t pc;
        moq_publish_namespace_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ .parts = p->ns_parts, .count = p->ns_count };
        moq_announcement_t ann;
        rc = moq_session_publish_namespace(s, &pc, now_us, &ann);
        if (rc == MOQ_OK) {
            p->announced = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_REQUEST_BLOCKED) {
            fatal(p, "publish_namespace", rc);
            return;
        }
    }

    /* One event per poll: a request's borrowed fields are invalidated by
     * the next advancing call. */
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            if (!p->ns_accepted) {
                p->ns_accepted = true;
                step("announced");
            }
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            fprintf(stdout, "fatal session closed code=%" PRIu64 " reason=\"%.*s\"\n",
                    ev.u.closed.code, (int)ev.u.closed.reason.len,
                    ev.u.closed.reason.data ? (const char *)ev.u.closed.reason.data
                                                    : "");
            fflush(stdout);
            atomic_store(&p->fatal, 1);
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            const moq_subscribe_request_event_t *req = &ev.u.subscribe_request;
            bool catalog = span_is(req->track_name, "catalog");
            bool video = span_is(req->track_name, "video");
            if ((catalog && p->catalog_live) || (video && p->video_live) || (!catalog && !video)) {
                fprintf(stdout, "fatal unexpected SUBSCRIBE for \"%.*s\"\n",
                        (int)req->track_name.len, (const char *)req->track_name.data);
                fflush(stdout);
                atomic_store(&p->fatal, 1);
                break;
            }
            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            rc = moq_session_accept_subscribe(s, req->sub, &ac, now_us);
            if (rc != MOQ_OK) {
                fatal(p, "accept_subscribe", rc);
                break;
            }
            if (catalog) {
                p->catalog_sub = req->sub;
                p->catalog_live = true;
                step("subscribed catalog");
            } else {
                p->video_sub = req->sub;
                p->video_live = true;
                step("subscribed video");
            }
            break;
        }
        default:
            step("event kind=%u", (unsigned)ev.kind);
            break;
        }
        moq_event_cleanup(&ev);
        if (atomic_load(&p->fatal)) return;
    }

    /* The catalog: one object on one subgroup, then the stream's FIN. */
    if (p->catalog_live && !p->catalog_closed) {
        if (!p->catalog_opened) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.subgroup_id = 0;
            sgc.publisher_priority = 100;
            rc = moq_session_open_subgroup(s, p->catalog_sub, &sgc, now_us, &p->catalog_sg);
            if (rc == MOQ_ERR_WOULD_BLOCK) return;
            if (rc != MOQ_OK) { fatal(p, "open catalog subgroup", rc); return; }
            p->catalog_opened = true;
        }
        if (!p->catalog_written) {
            rc = moq_session_write_object(s, p->catalog_sg, 0, p->catalog_json, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) return;
            if (rc != MOQ_OK) { fatal(p, "write catalog", rc); return; }
            p->catalog_written = true;
        }
        rc = moq_session_close_subgroup(s, p->catalog_sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;
        if (rc != MOQ_OK) { fatal(p, "close catalog subgroup", rc); return; }
        p->catalog_closed = true;
    }

    /* The video track: the objects on one subgroup with their properties,
     * the stream's FIN, then the completion with the chosen status. */
    if (!p->video_live || atomic_load(&p->done_sent)) return;
    if (!p->video_opened) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        sgc.object_properties = true;
        rc = moq_session_open_subgroup(s, p->video_sub, &sgc, now_us, &p->video_sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;
        if (rc != MOQ_OK) { fatal(p, "open video subgroup", rc); return; }
        p->video_opened = true;
    }
    while (p->video_written < p->objects) {
        uint8_t body[OBJECT_BYTES];
        memset(body, (int)(p->video_written & 0xff), sizeof body);
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_t *properties = NULL;
        rc = moq_rcbuf_create(moq_alloc_default(), body, sizeof body, &payload);
        if (rc != MOQ_OK) { fatal(p, "payload", rc); return; }
        if (!object_properties(p, p->video_written, &properties)) {
            moq_rcbuf_decref(payload);
            return;
        }
        moq_object_cfg_t oc;
        memset(&oc, 0, sizeof oc);
        oc.struct_size = sizeof oc;
        oc.object_id = p->video_written;
        oc.payload = payload;
        oc.properties = properties;
        rc = moq_session_write_object_ex(s, p->video_sg, &oc, now_us);
        moq_rcbuf_decref(payload);
        if (properties) moq_rcbuf_decref(properties);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;
        if (rc != MOQ_OK) { fatal(p, "write object", rc); return; }
        p->video_written++;
    }
    if (!p->video_closed) {
        rc = moq_session_close_subgroup(s, p->video_sg, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;
        if (rc != MOQ_OK) { fatal(p, "close video subgroup", rc); return; }
        p->video_closed = true;
        step("wrote %" PRIu32, p->video_written);
    }
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = p->status;
    dc.stream_count = 1;   /* the one subgroup stream */
    rc = moq_session_done_subscribe(s, p->video_sub, &dc, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return;
    if (rc != MOQ_OK) { fatal(p, "done_subscribe", rc); return; }
    atomic_store(&p->done_sent, true);
    step("done %" PRIu64, p->status);
}

static int on_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane, uint64_t now_us,
                   void *ctx)
{
    (void)lane;
    peer_t *p = ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (s == NULL || atomic_load(&p->fatal)) return 0;
    atomic_store(&p->session_state, (int)moq_session_state(s));
    if (moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        /* A session that ended after the announce: report the close the
         * session queued, so the row sees the reason. */
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                fprintf(stdout, "fatal session closed code=%" PRIu64 " reason=\"%.*s\"\n",
                        ev.u.closed.code, (int)ev.u.closed.reason.len,
                        ev.u.closed.reason.data
                            ? (const char *)ev.u.closed.reason.data : "");
                fflush(stdout);
                atomic_store(&p->fatal, 1);
            }
            moq_event_cleanup(&ev);
        }
        if (p->announced && !atomic_load(&p->fatal)) {
            step("fatal session state=%d after the announce", (int)moq_session_state(s));
            atomic_store(&p->fatal, 1);
        }
        return 0;
    }
    drive(p, s, now_us);
    return 0;
}

static bool parse_namespace(peer_t *p, char *spec)
{
    p->ns_count = 0;
    for (char *part = strtok(spec, "/"); part != NULL; part = strtok(NULL, "/")) {
        if (p->ns_count == MAX_NS_PARTS || *part == '\0') return false;
        p->ns_parts[p->ns_count++] = lit(part);
    }
    return p->ns_count > 0;
}

int main(int argc, char **argv)
{
    static peer_t p;
    int port = 0;
    int draft = 22;
    char *namespace = NULL;
    bool have_status = false;
    p.objects = 3;
    for (int i = 1; i + 1 < argc; i += 2) {
        if (strcmp(argv[i], "--port") == 0) port = atoi(argv[i + 1]);
        else if (strcmp(argv[i], "--draft") == 0) draft = atoi(argv[i + 1]);
        else if (strcmp(argv[i], "--namespace") == 0) namespace = argv[i + 1];
        else if (strcmp(argv[i], "--status") == 0) {
            p.status = strtoull(argv[i + 1], NULL, 0);
            have_status = true;
        } else if (strcmp(argv[i], "--objects") == 0) p.objects = (uint32_t)atoi(argv[i + 1]);
        else { fprintf(stderr, "done_peer: unknown option %s\n", argv[i]); return 2; }
    }
    if (port <= 0 || port > 65535 || namespace == NULL || !have_status ||
        (draft != 18 && draft != 22) || !parse_namespace(&p, namespace)) {
        fprintf(stderr, "usage: done_peer --port P --draft 18|22 --namespace a/b --status N "
                        "[--objects N]\n");
        return 2;
    }
    p.version = draft == 18 ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22;
    int exit_after_done = -1;
    const char *exit_status = getenv("MOQ5_DONE_PEER_EXIT_STATUS");
    if (exit_status != NULL) {
        char *end = NULL;
        errno = 0;
        long v = strtol(exit_status, &end, 10);
        if (errno != 0 || end == exit_status || *end != '\0' || v < 0 || v > 255) {
            fprintf(stderr, "done_peer: MOQ5_DONE_PEER_EXIT_STATUS must be 0..255\n");
            return 2;
        }
        exit_after_done = (int)v;
    }
    if (!build_catalog(&p)) return 1;

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    static const char *alpn[1];
    alpn[0] = draft == 18 ? "moqt-18" : "moqt-22";
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;   /* a loopback test peer, the test-only certificate */
    cfg.alpn_list = alpn;
    cfg.alpn_count = 1;
    cfg.send_request_capacity = true;
    cfg.on_lane_pump = on_pump;
    cfg.on_lane_pump_ctx = &p;
    moq_pq_threaded_t *t = NULL;
    moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
    if (rc != MOQ_OK) {
        fprintf(stdout, "fatal create rc=%d\n", (int)rc);
        return 1;
    }

    int exit_code = 0;
    uint64_t started_us = monotonic_us();
    uint64_t done_at_us = 0;
    while (!g_stop) {
        rc = moq_pq_threaded_wait(t, 50000);
        if (rc == MOQ_ERR_CLOSED || moq_pq_threaded_is_fatal(t) || atomic_load(&p.fatal)) {
            step("fatal session code=%" PRIu64 " transport=%" PRIu64,
                 moq_pq_threaded_fatal_code(t), moq_pq_threaded_terminal_error(t));
            exit_code = 1;
            break;
        }
        (void)moq_pq_threaded_wake(t);
        uint64_t now_us = monotonic_us();
        if (atomic_load(&p.done_sent) && done_at_us == 0) done_at_us = now_us;
        if (done_at_us != 0 && now_us - done_at_us >= LINGER_LIMIT_US) {
            step("timeout lingering after the done");
            exit_code = 2;
            break;
        }
        if (done_at_us == 0 && now_us - started_us >= LINGER_LIMIT_US) {
            step("timeout before the done (session state %d as the pump last saw it)",
                 atomic_load(&p.session_state));
            exit_code = 2;
            break;
        }
    }
    (void)moq_pq_threaded_stop(t);
    moq_pq_threaded_destroy(t);
    if (p.catalog_json) moq_rcbuf_decref(p.catalog_json);
    if (exit_after_done >= 0 && atomic_load(&p.done_sent)) {
        step("exiting %d after the done (MOQ5_DONE_PEER_EXIT_STATUS)", exit_after_done);
        exit_code = exit_after_done;
    }
    return exit_code;
}
