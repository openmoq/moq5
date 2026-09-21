/*
 * moq5_runtime_publisher: the independent reference publisher for the Python
 * finite receiver runtime fixture. Public service API only (moq::service).
 *
 *   publisher <url> <namespace-part>[/<part>...] <track> <ca_file> <sni>
 *             <objects_file> positive|negative push|pull
 *
 * Reads the declared objects from <objects_file> (records: u32 len, u8
 * is_sync, u64 pts_us, bytes), connects with the picoquic threaded backend
 * on exact draft 18 with strict verification against <ca_file> and <sni>,
 * attaches a lossless (BLOCK_TIMEOUT: no drop) sender in the mode named by
 * the last argument (pull: announce, the relay subscribes upstream; push:
 * PUBLISH tracks + live catalog), adds one RAW video track, and then follows
 * a line protocol on stdin/stdout:
 *
 *   stdout (one JSON object per line): connected, ready, demand, wrote,
 *          producer-finished (negative), end_track, complete, stats, shutdown
 *   stdin  (one word per line):  go            receiver's track is ACTIVE
 *                                ack-all       receiver observed all N objects
 *                                ended-observed receiver drained TRACK_ENDED
 *                                done          receiver finished; shut down
 *
 * Every wait is bounded; a missing, partial, overlong, EOF or byte-inexact
 * control line (an embedded NUL included) is a failure (exit 4), never a
 * silent retry. Callbacks record state only, as
 * mutex-protected observations (track pointer + count) independent of the
 * add_track output, which the app thread compares afterwards. Every wait and
 * lifecycle result is classified; an unexpected code fails by code. Ownership:
 * write() transfers the payload on MOQ_OK; on any other return the payload is
 * released here. Receipt/control output failures fail the run (exit 6).
 *
 *   publisher --control-selftest   read one 'go' line with a 2 s deadline and
 *                                  print its classification (offline test)
 */
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum { STEP_WAIT_US = 250000, CONTROL_TIMEOUT_MS = 30000, READY_TIMEOUT_S = 20, DEMAND_TIMEOUT_S = 30,
       WRITE_ATTEMPTS = 40, DRAIN_TIMEOUT_US = 5000000, MAX_OBJECTS = 64, CONTROL_MAX = 32, DEMAND_LOG = 32 };
static int g_control_timeout_ms = CONTROL_TIMEOUT_MS;
static int g_out_failed;

typedef struct {
    uint8_t *data;
    uint32_t len;
    bool     is_sync;
    uint64_t pts_us;
} declared_t;

/* Demand observations: one mutex-protected log written by the network-thread
 * callbacks, holding the callback's own track pointer and count. It never
 * reads the app's add_track output, so a callback that lands before
 * add_track returns is kept and compared later on the app thread. */
typedef struct { const void *track; size_t active; int joined; } demand_entry_t;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static demand_entry_t g_log[DEMAND_LOG];
static size_t g_log_n, g_log_dropped;
static _Atomic int    g_ready;
static _Atomic int    g_closed;
static _Atomic int    g_fatal;
static _Atomic uint64_t g_fatal_code;

static void demand_record(const moq_media_track_t *t, size_t active, int joined)
{
    pthread_mutex_lock(&g_mu);
    if (g_log_n < DEMAND_LOG) g_log[g_log_n++] = (demand_entry_t){ t, active, joined };
    else g_log_dropped++;
    pthread_mutex_unlock(&g_mu);
}
static void on_joined(void *ctx, moq_media_sender_t *s, moq_media_track_t *t, size_t active)
{
    (void)ctx; (void)s;
    demand_record(t, active, 1);
}
static void on_left(void *ctx, moq_media_sender_t *s, moq_media_track_t *t, size_t active)
{
    (void)ctx; (void)s;
    demand_record(t, active, 0);
}
/* App thread: does the log hold a joined entry for THIS returned handle with
 * at least one active subscription? */
static int demand_seen_for(const moq_media_track_t *returned, size_t *active_out)
{
    int seen = 0;
    pthread_mutex_lock(&g_mu);
    for (size_t i = 0; i < g_log_n; i++)
        if (g_log[i].track == returned && g_log[i].joined && g_log[i].active >= 1) { seen = 1; *active_out = g_log[i].active; }
    pthread_mutex_unlock(&g_mu);
    return seen;
}
static void on_ready(void *ctx, moq_media_sender_t *s) { (void)ctx; (void)s; atomic_store(&g_ready, 1); }
static void on_closed(void *ctx, moq_media_sender_t *s, bool is_fatal, uint64_t code)
{
    (void)ctx; (void)s;
    atomic_store(&g_closed, 1);
    if (is_fatal) { atomic_store(&g_fatal, 1); atomic_store(&g_fatal_code, code); }
}

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* Receipt output: every line is checked; a failed write or flush is recorded
 * and turns a would-be success into exit 6 (a broken receipt pipe must never
 * masquerade as success). */
static void emitf(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vprintf(fmt, ap);
    va_end(ap);
    if (n < 0 || fputc('\n', stdout) == EOF || fflush(stdout) != 0 || ferror(stdout)) {
        g_out_failed = 1;
        fprintf(stderr, "publisher: receipt output failed: %s\n", strerror(errno));
    }
}
static void emit(const char *json) { emitf("%s", json); }

/* Read one control line from the stdin FD, byte by byte (no stdio read-ahead
 * past the newline), against an absolute monotonic deadline and a length
 * limit. Classifies: ok / timeout / partial / overlength / eof / mismatch /
 * error. Returns 0 only for an exact match. */
static int read_control(char *buf, size_t cap, const char *expected, const char **why)
{
    size_t len = 0;
    double deadline = now_s() + (double)g_control_timeout_ms / 1000.0;
    for (;;) {
        double remaining = deadline - now_s();
        if (remaining <= 0) { *why = len ? "partial" : "timeout"; break; }
        struct pollfd p = { .fd = STDIN_FILENO, .events = POLLIN };
        int prc = poll(&p, 1, (int)(remaining * 1000.0) + 1);
        if (prc < 0) { if (errno == EINTR) continue; *why = "error"; break; }
        if (prc == 0) { *why = len ? "partial" : "timeout"; break; }
        char c;
        ssize_t n = read(STDIN_FILENO, &c, 1);
        if (n < 0) { if (errno == EINTR) continue; *why = "error"; break; }
        if (n == 0) { *why = len ? "eof-partial" : "eof"; break; }
        if (c == '\n') {
            /* byte-exact: the same length and the same bytes, so an embedded
             * NUL or any prefix/suffix variant is a mismatch, not a match */
            size_t want = strlen(expected);
            *why = (len == want && memcmp(buf, expected, want) == 0) ? "ok" : "mismatch";
            buf[len] = '\0';
            break;
        }
        if (len + 1 >= cap) { *why = "overlength"; break; }
        buf[len++] = c;
    }
    if (strcmp(*why, "ok") != 0) {
        buf[len < cap ? len : cap - 1] = '\0';
        fprintf(stderr, "publisher: control '%s': %s (got %zu bytes '%s')\n", expected, *why, len, buf);
        return -1;
    }
    return 0;
}

/* A sender wait outcome: OK and DONE (timeout) are ordinary; everything else
 * is a classified failure by code. */
static int wait_ok(moq_media_sender_t *tx, const char *where)
{
    moq_result_t rc = moq_media_sender_wait(tx, STEP_WAIT_US);
    if (rc == MOQ_OK || rc == MOQ_DONE) return 0;
    emitf("{\"event\":\"wait_error\",\"where\":\"%s\",\"rc\":%d}", where, (int)rc);
    return -1;
}

static size_t split_namespace(char *buf, moq_bytes_t *parts, size_t max)
{
    size_t n = 0;
    char *p = buf;
    while (*p && n < max) {
        char *slash = strchr(p, '/');
        if (slash) *slash = '\0';
        parts[n].data = (const uint8_t *)p;
        parts[n].len = strlen(p);
        n++;
        if (!slash) break;
        p = slash + 1;
    }
    return n;
}

static int load_objects(const char *path, declared_t *out, size_t *count)
{
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "publisher: cannot open %s: %s\n", path, strerror(errno)); return -1; }
    size_t n = 0;
    for (;;) {
        uint32_t len; uint8_t sync; uint64_t pts;
        if (fread(&len, 4, 1, f) != 1) break;
        if (fread(&sync, 1, 1, f) != 1 || fread(&pts, 8, 1, f) != 1 || n >= MAX_OBJECTS || len > 65536) { fclose(f); return -1; }
        out[n].data = malloc(len ? len : 1);
        if (!out[n].data || fread(out[n].data, 1, len, f) != len) { fclose(f); return -1; }
        out[n].len = len; out[n].is_sync = sync != 0; out[n].pts_us = pts;
        n++;
    }
    fclose(f);
    *count = n;
    return 0;
}

static int emit_stats(moq_media_sender_t *tx)
{
    moq_media_sender_stats_t st;
    memset(&st, 0, sizeof(st));
    moq_result_t rc = moq_media_sender_get_stats(tx, &st, sizeof(st));
    emitf("{\"event\":\"stats\",\"rc\":%d,\"written\":%" PRIu64 ",\"sent\":%" PRIu64 ",\"queued\":%" PRIu64
          ",\"dropped\":%" PRIu64 ",\"groups_dropped\":%" PRIu64 ",\"groups_abandoned\":%" PRIu64
          ",\"stalls\":%" PRIu64 ",\"last_error\":%d}",
          (int)rc, st.objects_written, st.objects_sent, st.objects_queued, st.objects_dropped,
          st.groups_dropped, st.groups_abandoned, st.backpressure_stalls, (int)st.last_error);
    return rc == MOQ_OK ? 0 : -1;
}

static void emit_demand_log(const moq_media_track_t *returned)
{
    pthread_mutex_lock(&g_mu);
    size_t n = g_log_n, dropped = g_log_dropped;
    demand_entry_t copy[DEMAND_LOG];
    memcpy(copy, g_log, sizeof(copy));
    pthread_mutex_unlock(&g_mu);
    printf("{\"event\":\"demand_log\",\"dropped\":%zu,\"entries\":[", dropped);
    for (size_t i = 0; i < n; i++)
        printf("%s{\"joined\":%s,\"active\":%zu,\"track_is_returned\":%s}", i ? "," : "",
               copy[i].joined ? "true" : "false", copy[i].active, copy[i].track == returned ? "true" : "false");
    emitf("]}");
}

int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--control-selftest") == 0) {
        char line[CONTROL_MAX];
        const char *why = "";
        g_control_timeout_ms = 2000;
        int rc = read_control(line, sizeof(line), "go", &why);
        emitf("{\"event\":\"control_selftest\",\"rc\":%d,\"class\":\"%s\"}", rc, why);
        return g_out_failed ? 6 : 0;
    }
    if (argc != 9) {
        fprintf(stderr, "usage: %s <url> <namespace> <track> <ca_file> <sni> <objects_file> positive|negative push|pull\n", argv[0]);
        return 2;
    }
    bool push = strcmp(argv[8], "push") == 0;
    if (!push && strcmp(argv[8], "pull") != 0) { fprintf(stderr, "publisher: mode must be push|pull\n"); return 2; }
    const char *url = argv[1], *track_name = argv[3], *ca_file = argv[4], *sni = argv[5], *objects_path = argv[6];
    bool negative = strcmp(argv[7], "negative") == 0;
    if (!negative && strcmp(argv[7], "positive") != 0) { fprintf(stderr, "publisher: mode must be positive|negative\n"); return 2; }

    declared_t objects[MAX_OBJECTS];
    size_t count = 0;
    if (load_objects(objects_path, objects, &count) != 0) { fprintf(stderr, "publisher: bad objects file\n"); return 2; }

    char nsbuf[256];
    snprintf(nsbuf, sizeof(nsbuf), "%s", argv[2]);
    moq_bytes_t ns_parts[8];
    size_t ns_count = split_namespace(nsbuf, ns_parts, 8);

    /* endpoint: picoquic threaded, exact draft 18, strict verification */
    const moq_version_t versions[1] = { MOQ_VERSION_DRAFT_18 };
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init_sized(&ec, sizeof(ec));
    ec.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    ec.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    ec.backend = MOQ_TRANSPORT_BACKEND_PICOQUIC;
    ec.versions.struct_size = sizeof(ec.versions);
    ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
    ec.versions.versions = versions;
    ec.versions.version_count = 1;
    ec.sni = (moq_bytes_t){ (const uint8_t *)sni, strlen(sni) };
    ec.ca_file = (moq_bytes_t){ (const uint8_t *)ca_file, strlen(ca_file) };
    ec.insecure_skip_verify = false;

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&ec, &ep);
    if (rc != MOQ_OK) { fprintf(stderr, "publisher: endpoint connect failed: %d\n", (int)rc); return 3; }
    emit("{\"event\":\"connected\"}");

    /* sender: lossless preset (BLOCK_TIMEOUT, never drops on its own); push or pull per argv */
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_lossless_sized(&scfg, sizeof(scfg));
    scfg.endpoint = NULL;
    scfg.namespace_.parts = ns_parts;
    scfg.namespace_.count = ns_count;
    scfg.publish_tracks = push;      /* push: PUBLISH tracks + live catalog; pull: announce, relay subscribes upstream */
    scfg.drop_without_demand = false;
    moq_media_sender_callbacks_init_sized(&scfg.callbacks, sizeof(scfg.callbacks));
    scfg.callbacks.on_subscriber_joined = on_joined;
    scfg.callbacks.on_subscriber_left = on_left;
    scfg.callbacks.on_ready = on_ready;
    scfg.callbacks.on_closed = on_closed;

    moq_media_sender_t *tx = NULL;
    rc = moq_media_sender_attach(ep, &scfg, &tx);
    if (rc != MOQ_OK) {
        fprintf(stderr, "publisher: sender attach failed: %d\n", (int)rc);
        moq_endpoint_stop(ep); moq_endpoint_destroy(ep);
        return 3;
    }

    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)track_name, strlen(track_name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.42E01E", 11 };
    tc.timescale = 90000;
    tc.width = 320; tc.height = 240;
    tc.framerate_millis = 24000;
    tc.bitrate = 400000;
    tc.is_live = true;
    moq_media_track_t *track = NULL;
    rc = moq_media_sender_add_track(tx, &tc, &track);
    if (rc != MOQ_OK) {
        fprintf(stderr, "publisher: add_track failed: %d\n", (int)rc);
        moq_media_sender_destroy(tx); moq_endpoint_stop(ep); moq_endpoint_destroy(ep);
        return 3;
    }

    int exit_code = 0;
    char line[CONTROL_MAX];
    const char *why = "";
    double t0 = now_s();
    /* readiness: announced + catalog accepted (not demand); every wait classified */
    while (!moq_media_sender_is_ready(tx)) {
        if (atomic_load(&g_fatal) || atomic_load(&g_closed)) { fprintf(stderr, "publisher: terminal before ready (fatal=%d code=%" PRIu64 ")\n", atomic_load(&g_fatal), atomic_load(&g_fatal_code)); exit_code = 3; goto shutdown; }
        if (now_s() - t0 > READY_TIMEOUT_S) { fprintf(stderr, "publisher: not ready within %d s\n", READY_TIMEOUT_S); exit_code = 4; goto shutdown; }
        if (wait_ok(tx, "ready") != 0) { exit_code = 3; goto shutdown; }
    }
    emitf("{\"event\":\"ready\",\"on_ready_callback\":%d,\"push\":%s}", atomic_load(&g_ready), push ? "true" : "false");

    /* demand barrier, two independent facts: (a) the receiver-side control
     * 'go' (its track_state is ACTIVE: SUBSCRIBE_OK reached the receiver);
     * (b) this sender's public demand callback for the media track (the
     * relay's subscription reached this publisher). In push mode (b) can
     * precede (a); both are required before the first write. */
    if (read_control(line, sizeof(line), "go", &why) != 0) { exit_code = 4; goto shutdown; }
    t0 = now_s();
    size_t active = 0;
    while (!demand_seen_for(track, &active)) {
        if (now_s() - t0 > DEMAND_TIMEOUT_S) { fprintf(stderr, "publisher: no media demand within %d s\n", DEMAND_TIMEOUT_S); exit_code = 4; goto shutdown; }
        if (wait_ok(tx, "demand") != 0) { exit_code = 3; goto shutdown; }
        if (atomic_load(&g_fatal)) { exit_code = 3; goto shutdown; }
    }
    emitf("{\"event\":\"demand\",\"active\":%zu,\"query\":%zu}", active, moq_media_sender_track_subscriptions(tx, track));

    if (!negative) {
        for (size_t i = 0; i < count; i++) {
            moq_rcbuf_t *payload = NULL;
            if (moq_rcbuf_create(moq_alloc_default(), objects[i].data, objects[i].len, &payload) != MOQ_OK) { fprintf(stderr, "publisher: rcbuf create failed\n"); exit_code = 3; goto shutdown; }
            moq_media_send_object_t o;
            memset(&o, 0, sizeof(o));
            o.struct_size = sizeof(o);
            o.payload = payload;
            o.is_sync = objects[i].is_sync;
            o.starts_group = objects[i].is_sync;
            o.presentation_time_us = objects[i].pts_us;
            o.decode_time_us = objects[i].pts_us;      /* advisory in v0 */
            o.has_capture_time = false;                /* LOC timestamp = presentation fallback */
            int attempts = 0;
            moq_result_t wr;
            for (;;) {
                attempts++;
                wr = moq_media_sender_write(tx, track, &o);
                if (wr == MOQ_OK) break;                                   /* payload transferred */
                if (wr == MOQ_ERR_WOULD_BLOCK && attempts < WRITE_ATTEMPTS) {   /* bounded, explicit */
                    if (wait_ok(tx, "write") != 0) { moq_rcbuf_decref(payload); exit_code = 3; goto shutdown; }
                    continue;
                }
                break;
            }
            emitf("{\"event\":\"wrote\",\"index\":%zu,\"len\":%u,\"is_sync\":%s,\"pts_us\":%" PRIu64 ",\"rc\":%d,\"attempts\":%d}",
                  i, objects[i].len, objects[i].is_sync ? "true" : "false", objects[i].pts_us, (int)wr, attempts);
            if (wr != MOQ_OK) {
                moq_rcbuf_decref(payload);                                 /* no transfer occurred */
                fprintf(stderr, "publisher: write %zu failed: %d after %d attempts\n", i, (int)wr, attempts);
                exit_code = 3; goto shutdown;
            }
        }
        /* completion phase is gated on the receiver's explicit acknowledgement of all N */
        if (read_control(line, sizeof(line), "ack-all", &why) != 0) { exit_code = 4; goto shutdown; }
    } else {
        emit("{\"event\":\"producer-finished\",\"objects\":0}");   /* deliberate: zero media objects */
    }

    {
        int attempts = 0;
        moq_result_t er;
        for (;;) {
            attempts++;
            er = moq_media_sender_end_track(tx, track);
            if (er == MOQ_ERR_WOULD_BLOCK && attempts < WRITE_ATTEMPTS) { if (wait_ok(tx, "end_track") != 0) { exit_code = 3; goto shutdown; } continue; }
            break;
        }
        emitf("{\"event\":\"end_track\",\"rc\":%d,\"attempts\":%d}", (int)er, attempts);
        if (er != MOQ_OK) { exit_code = 3; goto shutdown; }
    }
    /* complete() only after the receiver observed TRACK_ENDED, so its own
     * effect (REMOVED + catalog isComplete) is measured separately */
    if (read_control(line, sizeof(line), "ended-observed", &why) != 0) { exit_code = 4; goto shutdown; }
    rc = moq_media_sender_complete(tx);
    emitf("{\"event\":\"complete\",\"rc\":%d}", (int)rc);
    if (rc != MOQ_OK) { exit_code = 3; goto shutdown; }
    if (read_control(line, sizeof(line), "done", &why) != 0) { exit_code = 4; goto shutdown; }

shutdown:
    {
        int stats_failed = emit_stats(tx) != 0;
        emit_demand_log(track);
        moq_media_sender_destroy(tx);
        moq_result_t dr = moq_endpoint_drain(ep, DRAIN_TIMEOUT_US);   /* local stream flush, not receiver ack */
        moq_result_t sr = moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        /* a shutdown failure affects the result without hiding an earlier one */
        int shutdown_failed = stats_failed || dr != MOQ_OK || sr != MOQ_OK;
        if (exit_code == 0 && shutdown_failed) exit_code = 5;
        emitf("{\"event\":\"shutdown\",\"stats_rc_ok\":%s,\"drain_rc\":%d,\"stop_rc\":%d,\"fatal\":%d,\"fatal_code\":%" PRIu64 ",\"exit\":%d}",
              stats_failed ? "false" : "true", (int)dr, (int)sr, atomic_load(&g_fatal), atomic_load(&g_fatal_code), exit_code);
    }
    for (size_t i = 0; i < count; i++) free(objects[i].data);
    if (exit_code == 0 && g_out_failed) exit_code = 6;
    return exit_code;
}
