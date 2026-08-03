/*
 * Service-tier endpoint over the managed MsQuic facade, explicitly selected.
 *
 * The service endpoint is client-only; picoquic is its default RAW_QUIC
 * backend. This test pins the opt-in path: cfg.backend = MSQUIC routes the
 * endpoint through the msquic-managed registry row (never reached by AUTO).
 * A managed MsQuic SERVER (direct facade -- the endpoint is client-only) loops
 * back one real MoQ flow: SETUP both sides, the endpoint SUBSCRIBEs, the server
 * publishes one object, the endpoint receives it byte-exact, then closes
 * cleanly. The endpoint's session is driven on its own network thread through a
 * self-reposting post() task (the only place moq_session_* is legal).
 *
 * Built only when MOQ_BUILD_MSQUIC_MANAGED is on, so a "missing MsQuic" build
 * exercises the resolver's UNSUPPORTED branch instead (test_endpoint_resolve).
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/endpoint.h>
#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include "test_support.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

enum { OBJECT_SIZE = 1024 };

static void payload_fill(uint8_t *dst, size_t size)
{
    for (size_t i = 0; i < size; i++)
        dst[i] = (uint8_t)(i * 131u + 47u);
}

static const moq_bytes_t k_ns_parts[] = { { (const uint8_t *)"msq", 3 } };
static const moq_bytes_t k_track = { (const uint8_t *)"track", 5 };

/* -- server side (direct managed facade) ----------------------------- */

struct srv {
    pthread_mutex_t mu;
    int setup;
    bool have_sub;
    bool published;
    int publish_errors;
    int session_closed;
    moq_subscription_t sub;
};

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct srv *sv = ctx;
    /* A server drives its accepted session through lane iteration -- never the
     * client-only managed_session() accessor. This scenario has one conn. */
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c) : NULL;
    moq_event_t ev;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:    sv->setup++; break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            sv->sub = ev.u.subscribe_request.sub;
            sv->have_sub = true;
            break;
        case MOQ_EVENT_SESSION_CLOSED:    sv->session_closed++; break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sv->have_sub && !sv->published) {
        sv->published = true;

        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, sv->sub, &ac, now) < 0)
            sv->publish_errors++;

        moq_subgroup_cfg_t sgc;
        moq_subgroup_handle_t sg;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, sv->sub, &sgc, now, &sg) < 0) {
            sv->publish_errors++;
        } else {
            uint8_t *bytes = malloc(OBJECT_SIZE);
            moq_rcbuf_t *buf = NULL;
            if (bytes != NULL)
                payload_fill(bytes, OBJECT_SIZE);
            if (bytes == NULL ||
                moq_rcbuf_create(moq_alloc_default(), bytes,
                                 OBJECT_SIZE, &buf) < 0) {
                sv->publish_errors++;
            } else {
                if (moq_session_write_object(s, sg, 0, buf, now) < 0)
                    sv->publish_errors++;
                moq_rcbuf_decref(buf);
                if (moq_session_close_subgroup(s, sg, now) < 0)
                    sv->publish_errors++;
            }
            free(bytes);
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

/* -- client side (service endpoint, driven via post()) ---------------- */

struct client_drive {
    moq_endpoint_t *ep;
    bool            subscribed;
    bool            close_sent;
    atomic_int      setup;
    atomic_int      sub_ok;
    atomic_int      objects;
    atomic_int      object_errors;
    atomic_bool     done;      /* object seen (or session gone): stop reposting */
};

static void observe_object(struct client_drive *cd, const moq_event_t *ev)
{
    const moq_rcbuf_t *payload = ev->u.object_received.payload;
    if (payload == NULL || moq_rcbuf_len(payload) != OBJECT_SIZE) {
        atomic_fetch_add(&cd->object_errors, 1);
        return;
    }
    const uint8_t *data = moq_rcbuf_data(payload);
    for (size_t i = 0; i < OBJECT_SIZE; i++)
        if (data[i] != (uint8_t)(i * 131u + 47u)) {
            atomic_fetch_add(&cd->object_errors, 1);
            return;
        }
}

static moq_result_t drive_task(moq_endpoint_t *ep, moq_session_t *s,
                               uint64_t now, void *ctx)
{
    struct client_drive *cd = ctx;
    moq_event_t ev;

    (void)ep;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:  atomic_fetch_add(&cd->setup, 1); break;
        case MOQ_EVENT_SUBSCRIBE_OK:    atomic_fetch_add(&cd->sub_ok, 1); break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            observe_object(cd, &ev);
            atomic_fetch_add(&cd->objects, 1);
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            atomic_store(&cd->done, true);
            break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && atomic_load(&cd->setup) > 0 && !cd->subscribed) {
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)k_ns_parts;
        sc.track_namespace.count = 1;
        sc.track_name = k_track;
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        cd->subscribed = true;
        (void)moq_session_subscribe(s, &sc, now, &sub);
    }
    if (s != NULL && atomic_load(&cd->objects) >= 1 && !cd->close_sent) {
        cd->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
        atomic_store(&cd->done, true);
    }
    return MOQ_OK;
}

/* -- server bring-up -------------------------------------------------- */

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
    cfg.port = 0;                 /* ephemeral */
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

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 1;
    }

    /* == TLS knobs the MsQuic backend cannot honor are rejected (no network:
     *    ep_create_msquic returns before the facade/transport exists). Both use
     *    EXACT draft-16 so the single-ALPN check passes and the TLS guard is
     *    what fails. == */
    {
        static const moq_version_t v16 = MOQ_VERSION_DRAFT_16;
        /* Custom CA roots with verification on -> UNSUPPORTED. */
        moq_endpoint_cfg_t rc;
        moq_endpoint_cfg_init(&rc);
        static const char *rurl = "moqt://127.0.0.1:4433";
        rc.url = (moq_bytes_t){ (const uint8_t *)rurl, strlen(rurl) };
        rc.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
        rc.insecure_skip_verify = false;
        rc.ca_file = (moq_bytes_t){ (const uint8_t *)cert, strlen(cert) };
        rc.versions.struct_size = sizeof(moq_version_offer_t);
        rc.versions.policy = MOQ_VERSION_POLICY_EXACT;
        rc.versions.versions = &v16;
        rc.versions.version_count = 1;
        moq_endpoint_t *rep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&rc, &rep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(rep == NULL);

        /* Explicit SNI override != URL host -> UNSUPPORTED (even insecure). */
        moq_endpoint_cfg_t sc;
        moq_endpoint_cfg_init(&sc);
        sc.url = (moq_bytes_t){ (const uint8_t *)rurl, strlen(rurl) };
        sc.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
        sc.insecure_skip_verify = true;
        sc.sni = (moq_bytes_t){ (const uint8_t *)"other.example", 13 };
        sc.versions.struct_size = sizeof(moq_version_offer_t);
        sc.versions.policy = MOQ_VERSION_POLICY_EXACT;
        sc.versions.versions = &v16;
        sc.versions.version_count = 1;
        moq_endpoint_t *sep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&sc, &sep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(sep == NULL);
    }

    struct srv sv;
    uint16_t port = 0;
    moq_msquic_managed_t *srv_m = start_server(cert, key, &sv, &port);
    MOQ_TEST_CHECK(srv_m != NULL);
    if (!srv_m) return 1;
    MOQ_TEST_CHECK(port != 0);

    /* Explicit MsQuic selection: EXACT draft-16 (msquic is exact-version, so
     * the endpoint must offer a single ALPN) + backend = MSQUIC. */
    char url[64];
    snprintf(url, sizeof(url), "moqt://127.0.0.1:%u", (unsigned)port);
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);
    c.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    c.insecure_skip_verify = true;
    c.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
    static const moq_version_t v16 = MOQ_VERSION_DRAFT_16;
    c.versions.struct_size = sizeof(moq_version_offer_t);
    c.versions.policy = MOQ_VERSION_POLICY_EXACT;
    c.versions.versions = &v16;
    c.versions.version_count = 1;

    moq_endpoint_t *ep = NULL;
    /* The registry row must accept explicit MsQuic; a fall-through to picoquic
     * (or an UNSUPPORTED) fails right here. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&c, &ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (!ep) { moq_msquic_managed_stop(srv_m); moq_msquic_managed_destroy(srv_m); return 1; }

    /* Establish, then confirm this is really the MsQuic exact-version path. */
    bool established = false;
    for (int i = 0; i < 300 && !established; i++) {
        if (moq_endpoint_state(ep) == MOQ_ENDPOINT_ESTABLISHED) { established = true; break; }
        moq_result_t rc = moq_endpoint_wait(ep, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MOQ_TEST_CHECK(established);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_negotiated_version(ep),
                          (int)MOQ_VERSION_DRAFT_16);
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(ep));

    /* Drive SUBSCRIBE + object receipt on the network thread. Each post runs
     * drive_task once (poll events, subscribe once, close after the object);
     * the paced sleep lets the doorbell go idle between passes so inbound is
     * serviced -- a self-reposting task would peg the doorbell and starve it.
     * Session events queue until the next drive_task polls them, so a 50 ms
     * cadence against a real (wall-clock-bounded) deadline suffices. */
    struct client_drive cd;
    memset(&cd, 0, sizeof(cd));
    cd.ep = ep;
    atomic_init(&cd.setup, 0);
    atomic_init(&cd.sub_ok, 0);
    atomic_init(&cd.objects, 0);
    atomic_init(&cd.object_errors, 0);
    atomic_init(&cd.done, false);

    time_t deadline = time(NULL) + 20;
    while (atomic_load(&cd.objects) < 1 && time(NULL) < deadline) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_post(ep, drive_task, &cd),
                              (int)MOQ_OK);
        usleep(50000);
    }
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.setup), 1);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.sub_ok), 1);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.objects), 1);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.object_errors), 0);

    /* One more pass to send the client-side close, then let it settle. */
    for (int i = 0; i < 40 && !atomic_load(&cd.done); i++) {
        (void)moq_endpoint_post(ep, drive_task, &cd);
        usleep(25000);
    }

    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_endpoint_is_closed(ep));

    /* Terminal classification: the graceful close must map to CLEAN with no
     * detail code -- MsQuic exposes is_closed(), so this is not a NONE. */
    moq_endpoint_terminal_t term;
    term.struct_size = sizeof(term);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_get_terminal(ep, &term, sizeof(term)), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)term.reason,
                          (int)MOQ_ENDPOINT_TERMINAL_CLEAN);
    MOQ_TEST_CHECK_EQ_U64(term.detail_code, 0);

    moq_endpoint_destroy(ep);

    moq_msquic_managed_stop(srv_m);
    moq_msquic_managed_destroy(srv_m);
    pthread_mutex_lock(&sv.mu);
    MOQ_TEST_CHECK_EQ_INT(sv.setup, 1);
    MOQ_TEST_CHECK_EQ_INT(sv.publish_errors, 0);
    pthread_mutex_unlock(&sv.mu);
    pthread_mutex_destroy(&sv.mu);

    MOQ_TEST_PASS("endpoint_msquic_smoke");
    return failures != 0;
}
