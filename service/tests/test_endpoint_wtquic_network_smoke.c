/*
 * Service-tier endpoint over the managed wtquic Network.framework client,
 * explicitly selected.
 *
 * The service endpoint is client-only; pico_wt is its default WEBTRANSPORT
 * backend. This test pins the opt-in path: cfg.backend = WTQUIC_NETWORK
 * routes the endpoint through the wtquic-Network registry row (never reached
 * by AUTO). A wtquic MsQuic ATTACH server (the facade is client-only) loops
 * back one real MoQ flow over WebTransport: the endpoint's AUTO offer
 * ("moqt-18, moqt-16") negotiates against the server's moqt-16, SETUP both
 * sides, the endpoint SUBSCRIBEs, the server publishes one object, the
 * endpoint receives it byte-exact, then closes cleanly. The endpoint's
 * session is driven through post() tasks; with this backend they run on
 * wtquic's serial-queue domain (there is no network thread), and pump
 * cycles begin at establishment.
 *
 * Built only when MOQ_BUILD_WTQUIC_NETWORK_MANAGED is on (Apple-only), so
 * other builds exercise the resolver's UNSUPPORTED branch instead
 * (test_endpoint_resolve).
 */

#include <moq/endpoint.h>
#include <moq/moq.h>
#include <moq/rcbuf.h>
#include <moq/wtquic.h>
#include "test_support.h"

#include <wtquic/wtquic_msquic.h>

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

static const moq_bytes_t k_ns_parts[] = { { (const uint8_t *)"wtn", 3 } };
static const moq_bytes_t k_track = { (const uint8_t *)"track", 5 };

/* -- server side (wtquic MsQuic attach shape) ------------------------- */

struct srv {
    pthread_mutex_t mu;
    int accepted;
    int publish_errors;

    moq_session_t *ms;
    moq_wtquic_conn_t *conn;
    wtq_msquic_env_t *env;
    wtq_msquic_listener_t *listener;
    wtq_serve_config_t serve;
    uint16_t port;
};

static void srv_hook(moq_wtquic_conn_t *conn, void *user)
{
    struct srv *v = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    moq_event_t ev;

    pthread_mutex_lock(&v->mu);
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !v->accepted) {
            moq_subscription_t sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;

            moq_accept_subscribe_cfg_init(&acfg);
            if (moq_session_accept_subscribe(s, sub, &acfg, 0) < 0) {
                v->publish_errors++;
            } else {
                moq_subgroup_cfg_t sgc;
                moq_subgroup_handle_t sg;

                moq_subgroup_cfg_init(&sgc);
                sgc.group_id = 0;
                sgc.publisher_priority = 200;
                if (moq_session_open_subgroup(s, sub, &sgc, 0, &sg) < 0) {
                    v->publish_errors++;
                } else {
                    uint8_t *bytes = malloc(OBJECT_SIZE);
                    moq_rcbuf_t *buf = NULL;

                    if (bytes != NULL)
                        payload_fill(bytes, OBJECT_SIZE);
                    if (bytes == NULL ||
                        moq_rcbuf_create(moq_alloc_default(), bytes,
                                         OBJECT_SIZE, &buf) < 0) {
                        v->publish_errors++;
                    } else {
                        if (moq_session_write_object(s, sg, 0, buf, 0) < 0)
                            v->publish_errors++;
                        moq_rcbuf_decref(buf);
                        if (moq_session_close_subgroup(s, sg, 0) < 0)
                            v->publish_errors++;
                    }
                    free(bytes);
                }
                v->accepted = 1;
            }
        }
        moq_event_cleanup(&ev);
    }
    pthread_mutex_unlock(&v->mu);
}

static int srv_start(struct srv *v, const char *cert, const char *key)
{
    /* d16 server against the endpoint's AUTO ("moqt-18, moqt-16") offer:
     * negotiation must land on moqt-16. */
    static const char *const protos[] = { "moqt-16" };

    memset(v, 0, sizeof(*v));
    pthread_mutex_init(&v->mu, NULL);

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = 1;
    scfg.initial_request_capacity = 16;
    scfg.version = MOQ_VERSION_DRAFT_16;
    if (moq_session_create(&scfg, 0, &v->ms) < 0)
        return -1;

    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.session = v->ms;
    ccfg.hook = srv_hook;
    ccfg.hook_user = v;
    if (moq_wtquic_conn_create(&ccfg, &v->conn) < 0)
        return -1;

    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    if (wtq_msquic_env_open(&ecfg, &v->env) != WTQ_OK)
        return -1;

    v->serve = (wtq_serve_config_t)WTQ_SERVE_CONFIG_INIT;
    v->serve.path = "/moq";
    v->serve.subprotocols = protos;
    v->serve.subprotocol_count = 1;
    v->serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert;
    lcfg.key_file = key;
    lcfg.paths = &v->serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = v->conn;
    if (wtq_msquic_listener_start(v->env, &lcfg, &v->listener) != WTQ_OK)
        return -1;
    v->port = wtq_msquic_listener_port(v->listener);
    return 0;
}

static void srv_stop(struct srv *v)
{
    if (v->listener != NULL)
        wtq_msquic_listener_stop(v->listener);
    if (v->env != NULL)
        wtq_msquic_env_close(v->env);
    if (v->conn != NULL)
        moq_wtquic_conn_destroy(v->conn);
    if (v->ms != NULL)
        moq_session_destroy(v->ms);
    pthread_mutex_destroy(&v->mu);
}

/* -- client side (service endpoint, driven via post()) ---------------- */

struct client_drive {
    bool            subscribed;
    bool            close_sent;
    atomic_int      setup;
    atomic_int      sub_ok;
    atomic_int      objects;
    atomic_int      object_errors;
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
        (void)moq_session_subscribe(s, &sc, now, &sub);
    }
    if (s != NULL && atomic_load(&cd->objects) >= 1 && !cd->close_sent) {
        cd->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
        atomic_store(&cd->done, true);
    }
    return MOQ_OK;
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

    /* == TLS knobs the facade cannot honor are rejected before any network
     *    (ep_create_wtquic_network returns before the facade dials). == */
    {
        static const char *rurl = "https://127.0.0.1:4433/moq";
        /* Custom CA roots with verification on -> UNSUPPORTED. */
        moq_endpoint_cfg_t rc;
        moq_endpoint_cfg_init(&rc);
        rc.url = (moq_bytes_t){ (const uint8_t *)rurl, strlen(rurl) };
        rc.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
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
        sc.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
        sc.insecure_skip_verify = true;
        sc.sni = (moq_bytes_t){ (const uint8_t *)"other.example", 13 };
        moq_endpoint_t *sep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&sc, &sep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(sep == NULL);

        /* The wtquic Network client is WebTransport-only: an explicit
         * moqt:// (RAW_QUIC) selection is UNSUPPORTED at resolve. */
        static const char *qurl = "moqt://127.0.0.1:4433";
        moq_endpoint_cfg_t qc;
        moq_endpoint_cfg_init(&qc);
        qc.url = (moq_bytes_t){ (const uint8_t *)qurl, strlen(qurl) };
        qc.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
        moq_endpoint_t *qep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&qc, &qep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(qep == NULL);
    }

    struct srv sv;
    MOQ_TEST_CHECK(srv_start(&sv, cert, key) == 0);
    MOQ_TEST_CHECK(sv.port != 0);
    if (sv.port == 0) { srv_stop(&sv); return 1; }

    /* Explicit wtquic-Network selection with the DEFAULT (AUTO) version
     * offer: real multi-version negotiation must land on the server's
     * moqt-16. */
    char url[64];
    snprintf(url, sizeof(url), "https://127.0.0.1:%u/moq",
             (unsigned)sv.port);
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);
    c.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    c.insecure_skip_verify = true;
    c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;

    moq_endpoint_t *ep = NULL;
    /* The registry row must accept the explicit selection; a fall-through
     * to pico_wt (or an UNSUPPORTED) fails right here. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&c, &ep), (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (!ep) { srv_stop(&sv); return 1; }

    /* Establish, then confirm negotiation landed on the server's version. */
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

    /* Drive SUBSCRIBE + object receipt via post() tasks (with this backend
     * they run on wtquic's serial-queue domain). The paced sleep lets the
     * domain service inbound between passes. */
    struct client_drive cd;
    memset(&cd, 0, sizeof(cd));
    atomic_init(&cd.setup, 0);
    atomic_init(&cd.sub_ok, 0);
    atomic_init(&cd.objects, 0);
    atomic_init(&cd.object_errors, 0);
    atomic_init(&cd.done, false);

    time_t deadline = time(NULL) + 20;
    while (atomic_load(&cd.objects) < 1 && time(NULL) < deadline) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_post(ep, drive_task, &cd),
                              (int)MOQ_OK);
        (void)moq_endpoint_wait(ep, 50000);
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
     * detail code -- the facade exposes is_closed(), so this is not a NONE. */
    moq_endpoint_terminal_t term;
    term.struct_size = sizeof(term);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_get_terminal(ep, &term, sizeof(term)), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)term.reason,
                          (int)MOQ_ENDPOINT_TERMINAL_CLEAN);
    MOQ_TEST_CHECK_EQ_U64(term.detail_code, 0);

    moq_endpoint_destroy(ep);

    /* == Wrong certificate: with verification ON, the self-signed server
     *    must produce a REAL TLS terminal (wtquic's explicit trust
     *    evaluator rejects the chain, seals the NW_TRUST record, and
     *    fails the dial fast). A hang here means the backend cannot
     *    surface trust rejection and is not ready for exposure. == */
    {
        moq_endpoint_cfg_t vc;
        moq_endpoint_cfg_init(&vc);
        vc.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
        vc.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
        vc.insecure_skip_verify = false; /* system trust: must fail */
        moq_endpoint_t *vep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&vc, &vep),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(vep != NULL);
        if (vep != NULL) {
            bool fatal = false;
            for (int i = 0; i < 200 && !fatal; i++) {
                if (moq_endpoint_is_fatal(vep)) { fatal = true; break; }
                if (i % 20 == 19)
                    printf("WRONG_CERT_PROBE,i=%d,state=%d,negotiated=%d\n",
                           i, (int)moq_endpoint_state(vep),
                           (int)moq_endpoint_negotiated_version(vep));
                (void)moq_endpoint_wait(vep, 100000);
            }
            MOQ_TEST_CHECK(fatal); /* trust rejection must terminate */
            moq_endpoint_terminal_t vterm;
            vterm.struct_size = sizeof(vterm);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_endpoint_get_terminal(vep, &vterm, sizeof(vterm)),
                (int)MOQ_OK);
            printf("WRONG_CERT,reason=%d,detail=%lld\n", (int)vterm.reason,
                   (long long)(int64_t)vterm.detail_code);
            /* the verifier seals {NW_TRUST, errSecNotTrusted} for a
             * self-signed peer: the TRUST provenance classifies as
             * TLS_CERTIFICATE, with the raw OSStatus bits as detail */
            MOQ_TEST_CHECK_EQ_INT((int)vterm.reason,
                                  (int)MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE);
            MOQ_TEST_CHECK((int64_t)vterm.detail_code == -67843);
            MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(vep), (int)MOQ_OK);
            moq_endpoint_destroy(vep);
        }
    }

    /* Snapshot the server observations BEFORE srv_stop destroys sv.mu. */
    pthread_mutex_lock(&sv.mu);
    int srv_accepted = sv.accepted;
    int srv_publish_errors = sv.publish_errors;
    pthread_mutex_unlock(&sv.mu);
    srv_stop(&sv);
    MOQ_TEST_CHECK_EQ_INT(srv_accepted, 1);
    MOQ_TEST_CHECK_EQ_INT(srv_publish_errors, 0);

    MOQ_TEST_PASS("endpoint_wtquic_network_smoke");
    return failures != 0;
}
