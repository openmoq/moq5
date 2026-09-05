/*
 * mvfst backend: the client session outlives a fatal loop exit until the
 * endpoint is destroyed, so a media sender still attached to it can be torn
 * down afterwards.
 *
 * Reproduces the moqxr crash against red5-moq-relay: the relay closed the
 * session, the mvfst facade's network thread destroyed the MoQ session on its
 * way out, and moq_media_sender_destroy() (which runs moq_pub_destroy() inline
 * once the endpoint is terminal -- nothing can be posted to a dead pump)
 * dereferenced the freed session in track_hist_release. The picoquic facades
 * keep the session until destroy(); the mvfst facade now does the same.
 *
 * Shape: a loopback picoquic threaded server accepts the mvfst client, the
 * sender announces its namespace and a track (which reserves session-owned
 * history), then the server closes the connection with an application error.
 * The client goes terminal; the sender is destroyed in the documented order
 * (sender, then endpoint stop/destroy). Under ASan the old facade reports a
 * heap-use-after-free in moq_pub_destroy. Args: --cert <file> --key <file>.
 */
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/picoquic_threaded.h>

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "test_support.h"

#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED

typedef struct {
    uint64_t accepted_at_us;   /* first pump that saw a connection */
    int      closed;           /* close issued */
} server_ctx_t;

/* Close the (single) accepted connection about a second after it appeared:
 * long enough for SETUP, PUBLISH_NAMESPACE and the track registration. */
static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now_us, void *ctx)
{
    (void)t;
    server_ctx_t *sc = (server_ctx_t *)ctx;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        if (sc->accepted_at_us == 0) sc->accepted_at_us = now_us;
        if (!sc->closed && now_us - sc->accepted_at_us > 1000000) {
            sc->closed = 1;
            (void)moq_pq_threaded_conn_close(c, 0x10);
        }
    }
    return 0;
}

static moq_pq_threaded_t *start_server(const char *cert, const char *key,
                                       server_ctx_t *sc, int *out_port)
{
    int base = 15300 + (int)(getpid() % 997);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 17;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_lane_pump = server_pump;
        cfg.on_lane_pump_ctx = sc;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return NULL;
}

static int run(const char *cert, const char *key)
{
    int failures = 0;
    server_ctx_t sc; memset(&sc, 0, sizeof(sc));
    int port = 0;
    moq_pq_threaded_t *srv = start_server(cert, key, &sc, &port);
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) return failures;
    for (int i = 0; i < 3; i++) moq_pq_threaded_wait(srv, 100000);

    char url[64];
    snprintf(url, sizeof(url), "moqt://127.0.0.1:%d", port);
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init(&cfg);
    cfg.url = (moq_bytes_t){(const uint8_t *)url, strlen(url)};
    cfg.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    cfg.backend = MOQ_TRANSPORT_BACKEND_MVFST;
    moq_version_t v[1] = {MOQ_VERSION_DRAFT_16};
    cfg.versions.struct_size = sizeof(cfg.versions);
    cfg.versions.policy = MOQ_VERSION_POLICY_EXACT;   /* mvfst: exact only */
    cfg.versions.versions = v;
    cfg.versions.version_count = 1;
    cfg.insecure_skip_verify = true;                  /* self-signed loopback */

    moq_endpoint_t *ep = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&cfg, &ep), (int)MOQ_OK);
    if (!ep) { moq_pq_threaded_stop(srv); moq_pq_threaded_destroy(srv); return failures + 1; }

    static const moq_bytes_t ns_parts[] = {
        {(const uint8_t *)"mvfst", 5}, {(const uint8_t *)"fatal", 5}};
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    scfg.namespace_ = (moq_namespace_t){ns_parts, 2};
    moq_media_sender_t *tx = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &scfg, &tx), (int)MOQ_OK);

    /* The track's largest-location history record is reserved in the session
     * registry at add_track; releasing it is what the crash dereferenced. */
    moq_media_track_t *track = NULL;
    if (tx) {
        moq_media_track_cfg_t tcfg;
        moq_media_track_cfg_init(&tcfg);
        tcfg.name = (moq_bytes_t){(const uint8_t *)"vide_1", 6};
        tcfg.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tcfg.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        tcfg.codec = (moq_bytes_t){(const uint8_t *)"avc1.64000D", 11};
        tcfg.timescale = 1000000;
        tcfg.bitrate = 500000;
        tcfg.width = 320;
        tcfg.height = 240;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(tx, &tcfg, &track),
                              (int)MOQ_OK);
    }

    /* Established first (the server accepted us), then terminal (it closed). */
    int established = 0, terminal = 0;
    for (int i = 0; i < 100 && !established; i++) {
        (void)moq_endpoint_wait(ep, 100000);
        established = moq_endpoint_state(ep) == MOQ_ENDPOINT_ESTABLISHED;
        if (moq_endpoint_is_fatal(ep)) break;
    }
    if (!established) {
        moq_endpoint_terminal_t t; memset(&t, 0, sizeof t);
        moq_endpoint_get_terminal(ep, &t, sizeof t);
        fprintf(stderr, "not established: state=%d fatal=%d code=0x%llx reason=%d detail=0x%llx\n",
                (int)moq_endpoint_state(ep), moq_endpoint_is_fatal(ep),
                (unsigned long long)moq_endpoint_fatal_code(ep), (int)t.reason,
                (unsigned long long)t.detail_code);
    }
    MOQ_TEST_CHECK(established);
    for (int i = 0; i < 150 && !terminal; i++) {
        (void)moq_endpoint_wait(ep, 100000);
        terminal = moq_endpoint_is_fatal(ep) || moq_endpoint_is_closed(ep) ||
                   moq_endpoint_state(ep) == MOQ_ENDPOINT_CLOSED;
    }
    MOQ_TEST_CHECK(terminal);
    MOQ_TEST_CHECK(sc.closed);

    /* The crash site. */
    if (tx) moq_media_sender_destroy(tx);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
    moq_endpoint_destroy(ep);

    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return failures;
}
#endif

int main(int argc, char **argv)
{
#ifndef MOQ_SERVICE_HAVE_MVFST_MANAGED
    (void)argc; (void)argv;
    fprintf(stderr, "mvfst backend not built; skipping\n");
    return 77;
#else
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: %s --cert <file> --key <file>\n", argv[0]);
        return 2;
    }
    int failures = run(cert, key);
    if (failures) return failures;
    MOQ_TEST_PASS("test_endpoint_mvfst_fatal_teardown");
    return 0;
#endif
}
