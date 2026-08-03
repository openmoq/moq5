/*
 * Service-tier endpoint over the managed wtquic-over-MsQuic facade, explicitly
 * selected.
 *
 * The service endpoint is client-only; pico_wt is its default WEBTRANSPORT
 * backend. This test pins the opt-in path: cfg.backend = WTQUIC_MSQUIC routes
 * the endpoint through the wtquic-msquic registry row (never reached by AUTO).
 * A managed wtquic-MsQuic SERVER (the same facade in server mode -- the endpoint
 * is client-only) loops back one real MoQ flow: SETUP both sides, the endpoint
 * SUBSCRIBEs, the server publishes one object, the endpoint receives it
 * byte-exact, then closes cleanly. The endpoint's session is driven on its own
 * network thread through a post() task (the only place moq_session_* is legal).
 *
 * Unlike the MsQuic raw-QUIC backend (exact-version, single ALPN), this facade
 * negotiates WT-Protocol for real, so the server offers BOTH drafts and the
 * flow runs once pinned to draft-16 and once to draft-18, asserting the
 * negotiated version each time.
 *
 * Built only when MOQ_BUILD_WTQUIC_MSQUIC_MANAGED is on, so a "missing backend"
 * build exercises the resolver's UNSUPPORTED branch instead
 * (test_endpoint_resolve).
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/endpoint.h>
#include <moq/wtquic_msquic_managed.h>
#include <moq/rcbuf.h>
#include "test_support.h"

#include <errno.h>
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

/* nanosleep, not usleep: under _POSIX_C_SOURCE=200809L usleep is not declared
 * (a glibc warnings-as-errors build would fail on the implicit declaration). */
static void sleep_us(uint64_t us)
{
    struct timespec ts;
    ts.tv_sec = (time_t)(us / 1000000u);
    ts.tv_nsec = (long)((us % 1000000u) * 1000u);
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
        ; /* resume the remainder on a signal */
}

static const moq_bytes_t k_ns_parts[] = { { (const uint8_t *)"wtq", 3 } };
static const moq_bytes_t k_track = { (const uint8_t *)"track", 5 };

/* -- server side (the same managed facade, server mode) -------------- */

struct srv {
    pthread_mutex_t mu;
    int setup;
    bool have_sub;
    bool published;
    int publish_errors;
    int session_closed;
    moq_subscription_t sub;
};

static int server_pump(moq_wtquic_msquic_managed_t *m,
                       moq_wtquic_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct srv *sv = ctx;
    /* A server drives its accepted session through lane iteration -- never a
     * client-only accessor. This scenario has one conn at a time. */
    moq_wtquic_msquic_managed_conn_t *c = moq_wtquic_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_wtquic_msquic_managed_conn_session(c) : NULL;
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
    atomic_int      subscribe_errors;
    atomic_int      close_errors;
    atomic_bool     done;
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
        if (moq_session_subscribe(s, &sc, now, &sub) < 0)
            atomic_fetch_add(&cd->subscribe_errors, 1);
    }
    if (s != NULL && atomic_load(&cd->objects) >= 1 && !cd->close_sent) {
        cd->close_sent = true;
        if (moq_session_close(s, 0, NULL, now) < 0)
            atomic_fetch_add(&cd->close_errors, 1);
        atomic_store(&cd->done, true);
    }
    return MOQ_OK;
}

/* -- server bring-up (offers BOTH drafts for real negotiation) -------- */

/* sv is caller-owned: already zeroed with its mutex initialized, so a create
 * failure here leaves the caller's single cleanup path to destroy the mutex. */
static moq_wtquic_msquic_managed_t *start_server(const char *cert,
                                                 const char *key,
                                                 struct srv *sv,
                                                 uint16_t *out_port,
                                                 uint32_t wt_profile)
{
    static const char *const server_protos[] = { "moqt-18", "moqt-16" };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;                 /* ephemeral */
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.wt_protocols = server_protos;
    cfg.wt_protocol_count = 2;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_user = sv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    cfg.webtransport_profile = wt_profile; /* matched to the client's below */
    moq_wtquic_msquic_managed_t *m = NULL;
    if (moq_wtquic_msquic_managed_create(&cfg, &m) != MOQ_OK)
        return NULL;
    *out_port = moq_wtquic_msquic_managed_port(m);
    return m;
}

/* One full flow at an exact pinned version + WebTransport profile (client and
 * server share the profile -- the two dialects are mutually exclusive), against
 * a fresh server. */
static void run_case(const char *cert, const char *key, moq_version_t version,
                     uint32_t wt_profile)
{
    struct srv sv;
    memset(&sv, 0, sizeof(sv));
    if (pthread_mutex_init(&sv.mu, NULL) != 0) {
        MOQ_TEST_CHECK(0); /* mutex init must succeed */
        return;
    }

    moq_endpoint_t *ep = NULL;
    bool completed = false;
    uint16_t port = 0;
    /* The client speaks the SERVICE profile enum (wt_profile is a service
     * moq_wt_profile_t); the raw wtquic server speaks the ADAPTER enum. They no
     * longer share values, so map service -> adapter for the matched server. */
    uint32_t srv_profile =
        (wt_profile == (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT)
            ? (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT
            : (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT;
    moq_wtquic_msquic_managed_t *srv_m = start_server(cert, key, &sv, &port,
                                                      srv_profile);
    MOQ_TEST_CHECK(srv_m != NULL);
    if (!srv_m) goto cleanup;
    MOQ_TEST_CHECK(port != 0);

    char url[64];
    snprintf(url, sizeof(url), "https://127.0.0.1:%u", (unsigned)port);
    moq_endpoint_cfg_t c;
    /* Sized init: wt_profile is a struct_size-gated tail field. */
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    c.insecure_skip_verify = true;
    c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
    c.wt_profile = wt_profile;                 /* matched to the server above */
    c.versions.struct_size = sizeof(moq_version_offer_t);
    c.versions.policy = MOQ_VERSION_POLICY_EXACT;
    c.versions.versions = &version;
    c.versions.version_count = 1;

    /* The registry row must accept explicit WTQUIC_MSQUIC; a fall-through to
     * pico_wt (or an UNSUPPORTED) fails right here. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&c, &ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (!ep) goto cleanup;

    bool established = false;
    for (int i = 0; i < 300 && !established; i++) {
        if (moq_endpoint_state(ep) == MOQ_ENDPOINT_ESTABLISHED) { established = true; break; }
        moq_result_t rc = moq_endpoint_wait(ep, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MOQ_TEST_CHECK(established);
    /* Real WT-Protocol negotiation landed on exactly the pinned version. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_negotiated_version(ep),
                          (int)version);
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(ep));

    /* Drain has no truthful outbound-flush probe on this backend, so even a
     * live established client reports UNSUPPORTED rather than a false DONE that
     * would truncate the flush. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(ep, 1000000),
                          (int)MOQ_ERR_UNSUPPORTED);

    struct client_drive cd;
    memset(&cd, 0, sizeof(cd));
    cd.ep = ep;
    atomic_init(&cd.setup, 0);
    atomic_init(&cd.sub_ok, 0);
    atomic_init(&cd.objects, 0);
    atomic_init(&cd.object_errors, 0);
    atomic_init(&cd.subscribe_errors, 0);
    atomic_init(&cd.close_errors, 0);
    atomic_init(&cd.done, false);

    time_t deadline = time(NULL) + 20;
    while (atomic_load(&cd.objects) < 1 && time(NULL) < deadline) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_post(ep, drive_task, &cd),
                              (int)MOQ_OK);
        sleep_us(50000);
    }
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.setup), 1);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.sub_ok), 1);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.objects), 1);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.object_errors), 0);
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.subscribe_errors), 0);

    for (int i = 0; i < 40 && !atomic_load(&cd.done); i++) {
        (void)moq_endpoint_post(ep, drive_task, &cd);
        sleep_us(25000);
    }
    MOQ_TEST_CHECK_EQ_INT(atomic_load(&cd.close_errors), 0);

    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_endpoint_is_closed(ep));

    /* Terminal classification: the graceful close maps to CLEAN with no detail
     * code -- the facade exposes is_closed(), so this is not a NONE. */
    moq_endpoint_terminal_t term;
    term.struct_size = sizeof(term);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_get_terminal(ep, &term, sizeof(term)), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)term.reason,
                          (int)MOQ_ENDPOINT_TERMINAL_CLEAN);
    MOQ_TEST_CHECK_EQ_U64(term.detail_code, 0);
    completed = true;

cleanup:
    if (ep != NULL)
        moq_endpoint_destroy(ep);
    if (srv_m != NULL) {
        /* Stop the server first so its pump quiesces before we read sv. */
        moq_wtquic_msquic_managed_stop(srv_m);
        moq_wtquic_msquic_managed_destroy(srv_m);
    }
    if (completed) {
        pthread_mutex_lock(&sv.mu);
        MOQ_TEST_CHECK_EQ_INT(sv.setup, 1);
        MOQ_TEST_CHECK_EQ_INT(sv.publish_errors, 0);
        pthread_mutex_unlock(&sv.mu);
    }
    pthread_mutex_destroy(&sv.mu);
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

    /* == TLS knobs the wtquic-MsQuic backend cannot honor are rejected (no
     *    network: ep_create_wtquic_msquic returns before the facade/transport
     *    exists). WEBTRANSPORT (https) so the resolver routes to this backend. == */
    {
        static const char *rurl = "https://127.0.0.1:4433";
        /* Custom CA roots with verification on -> UNSUPPORTED. */
        moq_endpoint_cfg_t rc;
        moq_endpoint_cfg_init(&rc);
        rc.url = (moq_bytes_t){ (const uint8_t *)rurl, strlen(rurl) };
        rc.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
        rc.insecure_skip_verify = false;
        rc.ca_file = (moq_bytes_t){ (const uint8_t *)cert, strlen(cert) };
        moq_endpoint_t *rep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&rc, &rep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(rep == NULL);

        /* Explicit SNI override != URL host -> UNSUPPORTED (even insecure). */
        moq_endpoint_cfg_t sc;
        moq_endpoint_cfg_init(&sc);
        sc.url = (moq_bytes_t){ (const uint8_t *)rurl, strlen(rurl) };
        sc.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
        sc.insecure_skip_verify = true;
        sc.sni = (moq_bytes_t){ (const uint8_t *)"other.example", 13 };
        moq_endpoint_t *sep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&sc, &sep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(sep == NULL);
    }

    /* Real multi-version negotiation, both WebTransport wire profiles: run the
     * whole flow pinned to each draft under matched current/current and
     * d13-14/d13-14 loopbacks (the two dialects are mutually exclusive). */
    run_case(cert, key, MOQ_VERSION_DRAFT_16, MOQ_WT_PROFILE_CURRENT);
    run_case(cert, key, MOQ_VERSION_DRAFT_18, MOQ_WT_PROFILE_CURRENT);
    run_case(cert, key, MOQ_VERSION_DRAFT_16, MOQ_WT_PROFILE_D13_14_COMPAT);
    run_case(cert, key, MOQ_VERSION_DRAFT_18, MOQ_WT_PROFILE_D13_14_COMPAT);

    MOQ_TEST_PASS("endpoint_wtquic_msquic_smoke");
    return failures != 0;
}
