/*
 * The session outlives the facade's stop() until destroy() on every backend.
 *
 * Documented teardown order: destroy the media sender, then stop and destroy
 * the endpoint. While the endpoint is live, moq_media_sender_destroy() POSTS
 * its publisher-facade destroy to the pump; moq_endpoint_stop() then stops
 * the facade and drains the accepted tasks with the closed marker, so the
 * posted moq_pub_destroy() runs after the facade stopped and releases each
 * track's history through the publisher's session pointer. A facade that
 * frees the session in stop() (the msquic facade did, in its child reclaim)
 * hands that drain freed memory -- ASan: heap-use-after-free in
 * track_hist_release <- free_track_state <- moq_pub_destroy <-
 * sender_destroy_pub_task <- ep_drain_terminal <- moq_endpoint_stop, seen
 * from the moqxr publisher over msquic as a corrupted heap in MsQuicClose.
 *
 * Loopback picoquic threaded server; --backend picoquic|mvfst|msquic (a
 * backend that is not built skips with 77). Args: --cert --key.
 */
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/picoquic_threaded.h>

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "test_support.h"

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now_us, void *ctx)
{
    (void)t; (void)lane; (void)now_us; (void)ctx;
    return 0;
}

static moq_pq_threaded_t *start_server(const char *cert, const char *key,
                                       int *out_port)
{
    int base = 16600 + (int)(getpid() % 997);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 19;
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
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return NULL;
}

static int run(const char *cert, const char *key, moq_transport_backend_t backend)
{
    int failures = 0;
    int port = 0;
    moq_pq_threaded_t *srv = start_server(cert, key, &port);
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) return failures;
    for (int i = 0; i < 3; i++) moq_pq_threaded_wait(srv, 100000);

    char url[64];
    snprintf(url, sizeof(url), "moqt://127.0.0.1:%d", port);
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init(&cfg);
    cfg.url = (moq_bytes_t){(const uint8_t *)url, strlen(url)};
    cfg.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    cfg.backend = backend;
    moq_version_t v[1] = {MOQ_VERSION_DRAFT_16};   /* the threaded server's default ALPN */
    cfg.versions.struct_size = sizeof(cfg.versions);
    cfg.versions.policy = MOQ_VERSION_POLICY_EXACT;
    cfg.versions.versions = v;
    cfg.versions.version_count = 1;
    cfg.insecure_skip_verify = true;

    moq_endpoint_t *ep = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&cfg, &ep), (int)MOQ_OK);
    if (!ep) { moq_pq_threaded_stop(srv); moq_pq_threaded_destroy(srv); return failures + 1; }

    static const moq_bytes_t ns_parts[] = {
        {(const uint8_t *)"stop", 4}, {(const uint8_t *)"order", 5}};
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    scfg.namespace_ = (moq_namespace_t){ns_parts, 2};
    moq_media_sender_t *tx = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &scfg, &tx), (int)MOQ_OK);
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
        moq_media_track_t *track = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(tx, &tcfg, &track),
                              (int)MOQ_OK);
    }

    int established = 0;
    for (int i = 0; i < 100 && !established; i++) {
        (void)moq_endpoint_wait(ep, 100000);
        established = moq_endpoint_state(ep) == MOQ_ENDPOINT_ESTABLISHED;
        if (moq_endpoint_is_fatal(ep)) break;
    }
    MOQ_TEST_CHECK(established);
    /* let the announce and the track registration run on the pump */
    for (int i = 0; i < 5; i++) (void)moq_endpoint_wait(ep, 100000);

    /* Documented order: the sender's destroy is POSTED (endpoint live) and
     * runs from stop()'s terminal drain, after the facade stopped. */
    if (tx) moq_media_sender_destroy(tx);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
    moq_endpoint_destroy(ep);

    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    return failures;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL, *backend_name = "picoquic";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
        else if (!strcmp(argv[i], "--backend") && i + 1 < argc) backend_name = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: %s --cert <file> --key <file> [--backend picoquic|mvfst|msquic]\n", argv[0]);
        return 2;
    }
    moq_transport_backend_t backend;
    if (!strcmp(backend_name, "picoquic")) {
        backend = MOQ_TRANSPORT_BACKEND_PICOQUIC;
    } else if (!strcmp(backend_name, "mvfst")) {
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
        backend = MOQ_TRANSPORT_BACKEND_MVFST;
#else
        fprintf(stderr, "mvfst backend not built; skipping\n"); return 77;
#endif
    } else if (!strcmp(backend_name, "msquic")) {
#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
        backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
#else
        fprintf(stderr, "msquic backend not built; skipping\n"); return 77;
#endif
    } else {
        fprintf(stderr, "unknown --backend %s\n", backend_name); return 2;
    }
    int failures = run(cert, key, backend);
    if (failures) return failures;
    MOQ_TEST_PASS("test_endpoint_stop_after_sender_destroy");
    return 0;
}
