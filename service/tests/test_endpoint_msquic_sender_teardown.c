/*
 * Attached media-sender teardown over the managed MsQuic service backend.
 *
 * Regression for the client-session lifetime class reported in openmoq/moq5#25:
 * moq_media_sender_destroy() posts moq_pub_destroy() to the endpoint network
 * thread; moq_endpoint_stop() then drains that accepted task after quiescing the
 * transport. The publisher releases track histories through its session, so the
 * MsQuic client session wrapper must outlive transport stop and remain valid
 * until endpoint destroy.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/msquic_managed.h>
#include "test_support.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int failures = 0;

struct srv {
    pthread_mutex_t mu;
    int setup;
    int namespaces;
    int session_closed;
    int errors;
};

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now_us,
                       void *ctx)
{
    struct srv *sv = (struct srv *)ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c ? moq_msquic_managed_conn_session(c) : NULL;
    moq_event_t ev;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            sv->setup++;
            break;
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            if (moq_session_accept_namespace(
                    s, ev.u.namespace_published.ann, &acc, now_us) != MOQ_OK) {
                sv->errors++;
            } else {
                sv->namespaces++;
            }
            break;
        }
        case MOQ_EVENT_SESSION_CLOSED:
            sv->session_closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

static moq_msquic_managed_t *start_server(const char *cert, const char *key,
                                          struct srv *sv, uint16_t *out_port)
{
    memset(sv, 0, sizeof(*sv));
    pthread_mutex_init(&sv->mu, NULL);

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_user = sv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.version = MOQ_VERSION_DRAFT_16;

    moq_msquic_managed_t *m = NULL;
    if (moq_msquic_managed_create(&cfg, &m) != MOQ_OK)
        return NULL;
    *out_port = moq_msquic_managed_port(m);
    return m;
}

static moq_endpoint_cfg_t client_cfg(char *url, size_t cap, uint16_t port)
{
    snprintf(url, cap, "moqt://127.0.0.1:%u", (unsigned)port);
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init(&cfg);
    cfg.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    cfg.insecure_skip_verify = true;
    cfg.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
    static const moq_version_t v16 = MOQ_VERSION_DRAFT_16;
    cfg.versions.struct_size = sizeof(moq_version_offer_t);
    cfg.versions.policy = MOQ_VERSION_POLICY_EXACT;
    cfg.versions.versions = &v16;
    cfg.versions.version_count = 1;
    return cfg;
}

static void fill_sender_cfg(moq_media_sender_cfg_t *cfg, moq_bytes_t *parts)
{
    moq_media_sender_cfg_init_live_sized(cfg, sizeof(*cfg));
    parts[0] = (moq_bytes_t){ (const uint8_t *)"msq", 3 };
    parts[1] = (moq_bytes_t){ (const uint8_t *)"sender-teardown", 15 };
    cfg->namespace_ = (moq_namespace_t){ parts, 2 };
}

static void add_video_track(moq_media_sender_t *sender,
                            moq_media_track_t **out)
{
    moq_media_track_cfg_t track;
    moq_media_track_cfg_init(&track);
    track.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    track.media_type = MOQ_MEDIA_TYPE_VIDEO;
    track.packaging = MOQ_MEDIA_PACKAGING_RAW;
    track.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    track.bitrate = 1000000;
    track.width = 640;
    track.height = 360;
    track.timescale = 1000000;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(sender, &track, out),
                          (int)MOQ_OK);
}

static bool wait_endpoint_state(moq_endpoint_t *ep, moq_endpoint_state_t want)
{
    time_t deadline = time(NULL) + 20;
    while (time(NULL) < deadline) {
        if (moq_endpoint_state(ep) == want)
            return true;
        moq_result_t rc = moq_endpoint_wait(ep, 100000);
        if (rc == MOQ_ERR_CLOSED)
            return false;
    }
    return moq_endpoint_state(ep) == want;
}

static bool wait_sender_ready(moq_endpoint_t *ep, moq_media_sender_t *sender)
{
    time_t deadline = time(NULL) + 20;
    while (time(NULL) < deadline) {
        if (moq_media_sender_is_ready(sender))
            return true;
        moq_result_t rc = moq_endpoint_wait(ep, 100000);
        if (rc == MOQ_ERR_CLOSED)
            return false;
    }
    return moq_media_sender_is_ready(sender);
}

int main(int argc, char **argv)
{
    const char *cert = NULL;
    const char *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) {
            cert = argv[++i];
        } else if (!strcmp(argv[i], "--key") && i + 1 < argc) {
            key = argv[++i];
        }
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 1;
    }

    struct srv sv;
    uint16_t port = 0;
    moq_msquic_managed_t *srv = start_server(cert, key, &sv, &port);
    MOQ_TEST_CHECK(srv != NULL);
    MOQ_TEST_CHECK(port != 0);
    if (!srv)
        return 1;

    char url[64];
    moq_endpoint_cfg_t ecfg = client_cfg(url, sizeof(url), port);
    moq_endpoint_t *ep = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&ecfg, &ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (!ep) {
        moq_msquic_managed_stop(srv);
        moq_msquic_managed_destroy(srv);
        return 1;
    }

    MOQ_TEST_CHECK(wait_endpoint_state(ep, MOQ_ENDPOINT_ESTABLISHED));
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_negotiated_version(ep),
                          (int)MOQ_VERSION_DRAFT_16);

    moq_bytes_t parts[2];
    moq_media_sender_cfg_t scfg;
    fill_sender_cfg(&scfg, parts);
    moq_media_sender_t *sender = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &scfg, &sender),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(sender != NULL);
    if (!sender) {
        (void)moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        moq_msquic_managed_stop(srv);
        moq_msquic_managed_destroy(srv);
        return 1;
    }

    moq_media_track_t *track = NULL;
    add_video_track(sender, &track);
    MOQ_TEST_CHECK(track != NULL);
    MOQ_TEST_CHECK(wait_sender_ready(ep, sender));
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(sender));

    pthread_mutex_lock(&sv.mu);
    MOQ_TEST_CHECK_EQ_INT(sv.setup, 1);
    MOQ_TEST_CHECK_EQ_INT(sv.namespaces, 1);
    MOQ_TEST_CHECK_EQ_INT(sv.errors, 0);
    pthread_mutex_unlock(&sv.mu);

    /* Public repro sequence: destroy the attachment, then stop the still-live
     * endpoint. endpoint_stop drains the queued publisher-destroy task. */
    moq_media_sender_destroy(sender);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_endpoint_is_closed(ep));
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(ep));
    moq_endpoint_destroy(ep);

    moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);
    pthread_mutex_lock(&sv.mu);
    MOQ_TEST_CHECK_EQ_INT(sv.errors, 0);
    pthread_mutex_unlock(&sv.mu);
    pthread_mutex_destroy(&sv.mu);

    MOQ_TEST_PASS("endpoint_msquic_sender_teardown");
    return failures != 0;
}
