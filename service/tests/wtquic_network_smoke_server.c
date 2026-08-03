/*
 * WebTransport MoQ media server for the .wtquicNetwork runtime proof
 * (test support only -- never installed).
 *
 * A wtquic MsQuic ATTACH server on 127.0.0.1:<ephemeral> serving the
 * REAL media vertical the Swift service tier consumes: an MSF catalog
 * track (retained group) declaring one LOC track "v", and one
 * deterministic 256-byte object (group 0, object 0, independent,
 * end-of-group) published on "v" once it is subscribed -- the byte
 * pattern the proof verifies exactly. Built on the same publisher
 * facade the C service integration tests use (test_media_receiver*),
 * not a second protocol implementation.
 *
 * --version 16|18 selects the MoQ draft: the serve offer becomes
 * moqt-16 or moqt-18 and the pre-created server session pins the same
 * version, so a fresh process per connection covers both drafts.
 *
 * Lifecycle is SIGNAL-driven (no stdin dependency): the driver
 * (scripts/check_wtquic_network_runtime.sh) spawns a fresh process per
 * connection and sends SIGTERM after the flow, so a moq_session_t /
 * moq_wtquic_conn_t is never reused across reconnects. Prints
 * "PORT=<n>" on stdout once listening; timestamped stderr diagnostics
 * let a stalled client be attributed (did the peer reach us, finish
 * MoQ setup, get the publish, observe the close?).
 */

#include <moq/loc.h>
#include <moq/moq.h>
#include <moq/msf.h>
#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/wtquic.h>

#include <wtquic/wtquic_msquic.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum { OBJECT_SIZE = 256 };

static volatile sig_atomic_t g_stop = 0;
static void on_signal(int sig) { (void)sig; g_stop = 1; }

/* Timestamped stderr diagnostics: the runtime-proof driver captures
 * this stream and dumps it on a failed verdict. */
static uint64_t t0_ms;
static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}
static void diag(const char *what)
{
    fprintf(stderr, "[smoke +%llums] %s\n",
            (unsigned long long)(now_ms() - t0_ms), what);
    fflush(stderr);
}

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* One LOC media catalog: a single RAW video track "v" (the same shape
 * the C service integration tests serve). */
static const char k_catalog_json[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000}"
    "]}";

struct srv {
    moq_session_t *ms;
    moq_wtquic_conn_t *conn;
    moq_publisher_t *pub;
    moq_pub_track_t *catalog_track;
    moq_pub_track_t *media_track;
    int published;
    int saw_hook;
    int saw_terminal;
    int failed;
};

/* The deterministic proof payload: byte i = i*131+47 (mod 256). */
static void fill_pattern(uint8_t *bytes, size_t n)
{
    for (size_t i = 0; i < n; i++)
        bytes[i] = (uint8_t)(i * 131u + 47u);
}

/* Publisher bring-up: catalog track with the retained MSF catalog +
 * the media track. Runs lazily from the hook (the session's thread). */
static int srv_pub_up(struct srv *v, moq_session_t *s, uint64_t now)
{
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(s, moq_alloc_default(), &pcfg, &v->pub) != MOQ_OK)
        return -1;

    static const moq_bytes_t parts[] = {
        MOQ_BYTES_LITERAL("proof"), MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { (moq_bytes_t *)parts, 2 };

    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init(&tc);
    tc.track_namespace = ns;
    tc.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
    if (moq_pub_add_track(v->pub, &tc, now, &v->catalog_track) != MOQ_OK)
        return -1;
    moq_pub_track_cfg_init(&tc);
    tc.track_namespace = ns;
    tc.track_name = MOQ_BYTES_LITERAL("v");
    if (moq_pub_add_track(v->pub, &tc, now, &v->media_track) != MOQ_OK)
        return -1;

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(moq_alloc_default(),
                         (const uint8_t *)k_catalog_json,
                         strlen(k_catalog_json), &payload) < 0)
        return -1;
    moq_pub_retained_object_t robj = { .object_id = 0, .payload = payload };
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = &robj;
    rg.object_count = 1;
    moq_result_t rc =
        moq_pub_set_retained_group(v->pub, v->catalog_track, &rg);
    moq_rcbuf_decref(payload);
    return rc == MOQ_OK ? 0 : -1;
}

/* Publish THE object (group 0, object 0, independent, end-of-group)
 * once the media track has a subscriber. WOULD_BLOCK retries on the
 * next hook pass. */
static void srv_publish_once(struct srv *v, uint64_t now)
{
    if (v->published || v->media_track == NULL ||
        !moq_pub_has_subscriber(v->pub, v->media_track))
        return;

    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = 0;
    h.has_video_frame_marking = true;
    h.video_frame_marking.independent = true;
    moq_rcbuf_t *props = NULL;
    if (moq_loc_encode(moq_alloc_default(), MOQ_LOC_PROFILE_01, &h,
                       &props) != MOQ_OK) {
        v->failed = 1;
        return;
    }

    uint8_t bytes[OBJECT_SIZE];
    fill_pattern(bytes, sizeof(bytes));
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                         &payload) < 0) {
        moq_rcbuf_decref(props);
        v->failed = 1;
        return;
    }
    moq_pub_object_cfg_t ocfg;
    moq_pub_object_cfg_init(&ocfg);
    ocfg.group_id = 0;
    ocfg.object_id = 0;
    ocfg.payload = payload;
    ocfg.properties = props;
    ocfg.end_of_group = true;
    moq_result_t wrc =
        moq_pub_write_object_ex(v->pub, v->media_track, &ocfg, now);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(props);
    if (wrc == MOQ_OK) {
        v->published = 1;
        diag("published the proof object on \"v\"");
    } else if (wrc != MOQ_ERR_WOULD_BLOCK && wrc != MOQ_ERR_WRONG_STATE) {
        v->failed = 1;
        diag("publish FAILED");
    }
}

static void hook(moq_wtquic_conn_t *conn, void *user)
{
    struct srv *v = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    uint64_t now = mono_us();
    moq_event_t ev;

    if (!v->saw_hook) {
        v->saw_hook = 1;
        diag("peer reached the server (first adapter hook)");
    }
    if (v->pub == NULL && !v->failed && srv_pub_up(v, s, now) != 0) {
        v->failed = 1;
        diag("publisher bring-up FAILED");
    }

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            diag("MoQ SETUP complete");
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            diag("MoQ session closed");
            break;
        default:
            break;
        }
        if (v->pub != NULL) {
            moq_pub_event_result_t pres;
            (void)moq_pub_handle_event(v->pub, &ev, now, &pres);
        }
        moq_event_cleanup(&ev);
    }
    if (v->pub != NULL && !v->failed) {
        (void)moq_pub_tick(v->pub, now);
        srv_publish_once(v, now);
    }
    if (!v->saw_terminal &&
        (moq_wtquic_conn_is_closed(conn) || moq_wtquic_conn_is_fatal(conn))) {
        v->saw_terminal = 1;
        diag(moq_wtquic_conn_is_fatal(conn) ? "transport terminal: FATAL"
                                            : "transport terminal: closed");
    }
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    int version = 16;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
        else if (!strcmp(argv[i], "--version") && i + 1 < argc)
            version = atoi(argv[++i]);
    }
    if (!cert || !key || (version != 16 && version != 18)) {
        fprintf(stderr,
                "usage: --cert <pem> --key <pem> [--version 16|18]\n");
        return 2;
    }

    const char *proto = version == 18 ? "moqt-18" : "moqt-16";
    const char *const protos[] = { proto };
    struct srv v;
    memset(&v, 0, sizeof(v));

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = 1;
    scfg.initial_request_capacity = 16;
    scfg.version = version == 18 ? MOQ_VERSION_DRAFT_18
                                 : MOQ_VERSION_DRAFT_16;
    if (moq_session_create(&scfg, 0, &v.ms) < 0)
        return 1;

    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.session = v.ms;
    ccfg.hook = hook;
    ccfg.hook_user = &v;
    if (moq_wtquic_conn_create(&ccfg, &v.conn) < 0)
        return 1;

    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    wtq_msquic_env_t *env = NULL;
    if (wtq_msquic_env_open(&ecfg, &env) != WTQ_OK)
        return 1;

    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = "/moq";
    serve.subprotocols = protos;
    serve.subprotocol_count = 1;
    serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert;
    lcfg.key_file = key;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = v.conn;
    wtq_msquic_listener_t *listener = NULL;
    if (wtq_msquic_listener_start(env, &lcfg, &listener) != WTQ_OK)
        return 1;

    signal(SIGTERM, on_signal);
    signal(SIGINT, on_signal);

    t0_ms = now_ms();
    printf("PORT=%u\n", (unsigned)wtq_msquic_listener_port(listener));
    fflush(stdout);
    diag(version == 18 ? "listening (moqt-18)" : "listening (moqt-16)");

    /* Run until SIGTERM/SIGINT. Poll a flag (set by the handler) on a
     * short sleep -- no pause() race, no stdin dependency. */
    while (!g_stop) {
        struct timespec ts = { 0, 50 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    wtq_msquic_listener_stop(listener);
    wtq_msquic_env_close(env);
    if (v.pub != NULL)
        moq_pub_destroy(v.pub);
    moq_wtquic_conn_destroy(v.conn);
    moq_session_destroy(v.ms);
    return v.failed ? 1 : 0;
}
