/*
 * Managed MsQuic multi-ALPN server:
 *
 * - one managed server offers draft-18 and draft-16 on one listener;
 * - exact-version clients for both drafts complete a real subscribe/object flow;
 * - the accepted child reports the negotiated draft immutably from the
 *   per-connection pump window;
 * - unsupported/no-overlap still fails at TLS ALPN negotiation;
 * - malformed version-list configuration is rejected before use.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>

static int failures;
static const QUIC_API_TABLE *g_msquic_api;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

enum { PAYLOAD_SIZE = 4096 };

static void payload_fill(uint8_t *dst, size_t size, uint8_t salt)
{
    for (size_t i = 0; i < size; i++)
        dst[i] = (uint8_t)(salt + i * 37u);
}

struct client_side {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    moq_version_t want_version;
    int setup;
    int sub_ok;
    int objects;
    int object_errors;
    uint64_t bytes;
    bool subscribed;
    bool close_sent;
    bool negotiated_ok;
};

struct server_slot {
    moq_version_t version;
    moq_msquic_managed_conn_t *conn;
    moq_subscription_t sub;
    bool have_sub;
    bool published;
    bool closed;
};

struct server_side {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    struct server_slot slots[4];
    int next_slot;
    int version16;
    int version18;
    int published;
    int closed;
    int version_errors;
    _Atomic(moq_msquic_managed_conn_t *) probe_conn;
    atomic_bool hold_publish_for_probe;
    atomic_bool external_probe_done;
    atomic_int activity_checked;
    atomic_int activity_nonzero;
};

struct client_snapshot {
    int setup;
    int sub_ok;
    int objects;
    int object_errors;
    uint64_t bytes;
    bool negotiated_ok;
};

struct server_snapshot {
    int version16;
    int version18;
    int published;
    int closed;
    int version_errors;
};

struct raw_alpn_client {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    const QUIC_API_TABLE *api;
    HQUIC registration;
    HQUIC configuration;
    HQUIC connection;
    QUIC_BUFFER alpns[MOQ_MSQUIC_MANAGED_MAX_VERSIONS];
    bool connected;
    bool shutdown_complete;
    bool transport_shutdown;
    bool selected_valid;
    moq_version_t selected;
    uint8_t selected_bytes[16];
    uint8_t selected_len;
    int errors;
};

static void client_init(struct client_side *c, moq_version_t version)
{
    memset(c, 0, sizeof(*c));
    pthread_mutex_init(&c->mu, NULL);
    c->want_version = version;
}

static void client_destroy(struct client_side *c)
{
    pthread_mutex_destroy(&c->mu);
}

static void server_init(struct server_side *s)
{
    memset(s, 0, sizeof(*s));
    pthread_mutex_init(&s->mu, NULL);
    atomic_init(&s->probe_conn, NULL);
    atomic_init(&s->hold_publish_for_probe, false);
    atomic_init(&s->external_probe_done, false);
    atomic_init(&s->activity_checked, 0);
    atomic_init(&s->activity_nonzero, 0);
}

static void server_destroy(struct server_side *s)
{
    pthread_mutex_destroy(&s->mu);
}

static bool client_done(struct client_side *c)
{
    pthread_mutex_lock(&c->mu);
    bool done = c->objects >= 1 || c->object_errors != 0;
    pthread_mutex_unlock(&c->mu);
    return done;
}

static bool server_seen_versions(struct server_side *s)
{
    pthread_mutex_lock(&s->mu);
    bool ok = s->version16 == 1 && s->version18 == 1 && s->published == 2 &&
              s->version_errors == 0;
    pthread_mutex_unlock(&s->mu);
    return ok;
}

static bool server_seen_single(struct server_side *s, moq_version_t version)
{
    pthread_mutex_lock(&s->mu);
    bool ok = s->published == 1 && s->version_errors == 0 &&
              ((version == MOQ_VERSION_DRAFT_16 && s->version16 == 1 &&
                s->version18 == 0) ||
               (version == MOQ_VERSION_DRAFT_18 && s->version18 == 1 &&
                s->version16 == 0));
    pthread_mutex_unlock(&s->mu);
    return ok;
}

static void client_snapshot(struct client_side *c, struct client_snapshot *out)
{
    pthread_mutex_lock(&c->mu);
    out->setup = c->setup;
    out->sub_ok = c->sub_ok;
    out->objects = c->objects;
    out->object_errors = c->object_errors;
    out->bytes = c->bytes;
    out->negotiated_ok = c->negotiated_ok;
    pthread_mutex_unlock(&c->mu);
}

static void server_snapshot(struct server_side *s, struct server_snapshot *out)
{
    pthread_mutex_lock(&s->mu);
    out->version16 = s->version16;
    out->version18 = s->version18;
    out->published = s->published;
    out->closed = s->closed;
    out->version_errors = s->version_errors;
    pthread_mutex_unlock(&s->mu);
}

static bool server_seen_raw_conn(struct server_side *s, moq_version_t version)
{
    pthread_mutex_lock(&s->mu);
    bool ok = s->next_slot == 1 && s->published == 0 &&
              s->version_errors == 0 &&
              ((version == MOQ_VERSION_DRAFT_16 && s->version16 == 1 &&
                s->version18 == 0) ||
               (version == MOQ_VERSION_DRAFT_18 && s->version18 == 1 &&
                s->version16 == 0));
    pthread_mutex_unlock(&s->mu);
    return ok;
}

static bool wait_for_probe_conn(struct server_side *srv,
                                struct client_side *c16,
                                struct client_side *c18, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (atomic_load(&srv->probe_conn) != NULL)
            return true;
        if (c16->m != NULL)
            (void)moq_msquic_managed_wait(c16->m, 10 * 1000);
        if (c18->m != NULL)
            (void)moq_msquic_managed_wait(c18->m, 0);
        (void)moq_msquic_managed_wait(srv->m, 0);
    }
    return atomic_load(&srv->probe_conn) != NULL;
}

static bool wait_for_multi_flow(struct server_side *srv,
                                struct client_side *c16,
                                struct client_side *c18, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (client_done(c16) && client_done(c18) &&
            server_seen_versions(srv))
            return true;
        (void)moq_msquic_managed_wait(c16->m, 10 * 1000);
        (void)moq_msquic_managed_wait(c18->m, 10 * 1000);
        (void)moq_msquic_managed_wait(srv->m, 0);
    }
    return client_done(c16) && client_done(c18) && server_seen_versions(srv);
}

static bool wait_for_single_flow(struct server_side *srv,
                                 struct client_side *c,
                                 moq_version_t version, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (client_done(c) && server_seen_single(srv, version))
            return true;
        (void)moq_msquic_managed_wait(c->m, 10 * 1000);
        (void)moq_msquic_managed_wait(srv->m, 0);
    }
    return client_done(c) && server_seen_single(srv, version);
}

static bool wait_for_raw_server_conn(struct server_side *srv,
                                     moq_version_t version, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (server_seen_raw_conn(srv, version))
            return true;
        (void)moq_msquic_managed_wait(srv->m, 10 * 1000);
    }
    return server_seen_raw_conn(srv, version);
}

static bool wait_terminal(moq_msquic_managed_t *m, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (moq_msquic_managed_is_closed(m) ||
            moq_msquic_managed_is_fatal(m))
            return true;
        (void)moq_msquic_managed_wait(m, 10 * 1000);
    }
    return moq_msquic_managed_is_closed(m) ||
           moq_msquic_managed_is_fatal(m);
}

static void observe_object(struct client_side *c, const moq_event_t *ev)
{
    const moq_rcbuf_t *payload = ev->u.object_received.payload;

    if (payload == NULL) {
        c->object_errors++;
        return;
    }
    size_t len = moq_rcbuf_len(payload);
    const uint8_t *data = moq_rcbuf_data(payload);
    uint8_t salt = c->want_version == MOQ_VERSION_DRAFT_18 ? 0x18u : 0x16u;

    c->bytes += len;
    if (len != PAYLOAD_SIZE) {
        c->object_errors++;
        return;
    }
    for (size_t i = 0; i < len; i++)
        if (data[i] != (uint8_t)(salt + i * 37u)) {
            c->object_errors++;
            return;
        }
}

static const char *version_alpn(moq_version_t version)
{
    switch (version) {
    case MOQ_VERSION_DRAFT_16:
        return MOQ_MSQUIC_ALPN_DRAFT16;
    case MOQ_VERSION_DRAFT_18:
        return MOQ_MSQUIC_ALPN_DRAFT18;
    default:
        return NULL;
    }
}

static bool alpn_to_version(const uint8_t *alpn, uint8_t len,
                            moq_version_t *out)
{
    if (len == strlen(MOQ_MSQUIC_ALPN_DRAFT16) &&
        memcmp(alpn, MOQ_MSQUIC_ALPN_DRAFT16, len) == 0) {
        *out = MOQ_VERSION_DRAFT_16;
        return true;
    }
    if (len == strlen(MOQ_MSQUIC_ALPN_DRAFT18) &&
        memcmp(alpn, MOQ_MSQUIC_ALPN_DRAFT18, len) == 0) {
        *out = MOQ_VERSION_DRAFT_18;
        return true;
    }
    return false;
}

static void raw_alpn_client_init(struct raw_alpn_client *rc)
{
    memset(rc, 0, sizeof(*rc));
    pthread_mutex_init(&rc->mu, NULL);
    pthread_cond_init(&rc->cv, NULL);
    rc->api = g_msquic_api;
}

static void raw_alpn_signal(struct raw_alpn_client *rc)
{
    pthread_cond_broadcast(&rc->cv);
}

static void *raw_stream_cb_ptr(QUIC_STREAM_CALLBACK_HANDLER fn)
{
    union { QUIC_STREAM_CALLBACK_HANDLER fn; void *obj; } u = { .fn = fn };
    return u.obj;
}

static QUIC_STATUS QUIC_API raw_stream_cb(
    HQUIC stream, void *ctx, QUIC_STREAM_EVENT *ev)
{
    struct raw_alpn_client *rc = ctx;

    switch (ev->Type) {
    case QUIC_STREAM_EVENT_RECEIVE:
        ev->RECEIVE.TotalBufferLength = 0;
        return QUIC_STATUS_CONTINUE;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        rc->api->StreamClose(stream);
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API raw_alpn_client_cb(
    HQUIC connection, void *ctx, QUIC_CONNECTION_EVENT *ev)
{
    struct raw_alpn_client *rc = ctx;

    (void)connection;
    pthread_mutex_lock(&rc->mu);
    switch (ev->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED:
        rc->connected = true;
        if (ev->CONNECTED.NegotiatedAlpnLength >
            sizeof(rc->selected_bytes)) {
            rc->errors++;
            break;
        }
        rc->selected_len = ev->CONNECTED.NegotiatedAlpnLength;
        memcpy(rc->selected_bytes, ev->CONNECTED.NegotiatedAlpn,
               rc->selected_len);
        rc->selected_valid =
            alpn_to_version(rc->selected_bytes, rc->selected_len,
                            &rc->selected);
        if (!rc->selected_valid)
            rc->errors++;
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        rc->transport_shutdown = true;
        rc->errors++;
        break;
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
        rc->api->SetCallbackHandler(
            ev->PEER_STREAM_STARTED.Stream, raw_stream_cb_ptr(raw_stream_cb),
            rc);
        (void)rc->api->StreamShutdown(
            ev->PEER_STREAM_STARTED.Stream,
            QUIC_STREAM_SHUTDOWN_FLAG_ABORT |
                QUIC_STREAM_SHUTDOWN_FLAG_INLINE,
            0);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        rc->shutdown_complete = true;
        break;
    default:
        break;
    }
    raw_alpn_signal(rc);
    pthread_mutex_unlock(&rc->mu);
    return QUIC_STATUS_SUCCESS;
}

static bool raw_wait_connected(struct raw_alpn_client *rc, int max_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += max_ms / 1000;
    ts.tv_nsec += (long)(max_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&rc->mu);
    while (!rc->connected && !rc->shutdown_complete &&
           !rc->transport_shutdown)
        if (pthread_cond_timedwait(&rc->cv, &rc->mu, &ts) == ETIMEDOUT)
            break;
    bool ok = rc->connected;
    pthread_mutex_unlock(&rc->mu);
    return ok;
}

static bool raw_wait_shutdown(struct raw_alpn_client *rc, int max_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += max_ms / 1000;
    ts.tv_nsec += (long)(max_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&rc->mu);
    while (!rc->shutdown_complete)
        if (pthread_cond_timedwait(&rc->cv, &rc->mu, &ts) == ETIMEDOUT)
            break;
    bool ok = rc->shutdown_complete;
    pthread_mutex_unlock(&rc->mu);
    return ok;
}

static void raw_alpn_client_cleanup(struct raw_alpn_client *rc)
{
    if (rc->connection != NULL) {
        if (!raw_wait_shutdown(rc, 100)) {
            rc->api->ConnectionShutdown(rc->connection,
                                        QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT,
                                        0);
            (void)raw_wait_shutdown(rc, 5000);
        }
        rc->api->ConnectionClose(rc->connection);
        rc->connection = NULL;
    }
    if (rc->configuration != NULL) {
        rc->api->ConfigurationClose(rc->configuration);
        rc->configuration = NULL;
    }
    if (rc->registration != NULL) {
        rc->api->RegistrationClose(rc->registration);
        rc->registration = NULL;
    }
    pthread_cond_destroy(&rc->cv);
    pthread_mutex_destroy(&rc->mu);
}

static bool raw_alpn_client_connect(struct raw_alpn_client *rc,
                                    uint16_t port,
                                    const moq_version_t *versions,
                                    size_t version_count)
{
    QUIC_REGISTRATION_CONFIG rcfg = {
        .AppName = "moq-msquic-multi-alpn-test",
        .ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY,
    };
    QUIC_SETTINGS settings;
    QUIC_CREDENTIAL_CONFIG cred;

    if (rc->api == NULL || versions == NULL || version_count == 0 ||
        version_count > MOQ_MSQUIC_MANAGED_MAX_VERSIONS)
        return false;
    for (size_t i = 0; i < version_count; i++) {
        const char *alpn = version_alpn(versions[i]);

        if (alpn == NULL)
            return false;
        rc->alpns[i].Length = (uint32_t)strlen(alpn);
        rc->alpns[i].Buffer = (uint8_t *)(uintptr_t)alpn;
    }

    if (QUIC_FAILED(rc->api->RegistrationOpen(&rcfg, &rc->registration)))
        return false;
    moq_msquic_settings_init(&settings);
    if (QUIC_FAILED(rc->api->ConfigurationOpen(
            rc->registration, rc->alpns, (uint32_t)version_count, &settings,
            sizeof(settings), NULL, &rc->configuration)))
        return false;
    memset(&cred, 0, sizeof(cred));
    cred.Type = QUIC_CREDENTIAL_TYPE_NONE;
    cred.Flags = QUIC_CREDENTIAL_FLAG_CLIENT |
                 QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
    if (QUIC_FAILED(rc->api->ConfigurationLoadCredential(
            rc->configuration, &cred)))
        return false;
    if (QUIC_FAILED(rc->api->ConnectionOpen(
            rc->registration, raw_alpn_client_cb, rc, &rc->connection)))
        return false;
    if (QUIC_FAILED(rc->api->ConnectionStart(
            rc->connection, rc->configuration, QUIC_ADDRESS_FAMILY_UNSPEC,
            "127.0.0.1", port)))
        return false;
    return true;
}

static int client_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct client_side *c = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;
    pthread_mutex_lock(&c->mu);
    if (moq_msquic_managed_negotiated_version(m) == c->want_version)
        c->negotiated_ok = true;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            c->setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            c->sub_ok++;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            observe_object(c, &ev);
            c->objects++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && c->setup > 0 && !c->subscribed) {
        static const moq_bytes_t ns[] = {
            { (const uint8_t *)"multi", 5 },
        };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"alpn", 4 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        c->subscribed = true;
        if (moq_session_subscribe(s, &sc, now, &sub) < 0)
            c->object_errors++;
    }
    if (s != NULL && c->objects >= 1 && !c->close_sent) {
        c->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
    }
    pthread_mutex_unlock(&c->mu);
    return 0;
}

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct server_side *srv = ctx;

    (void)m;
    pthread_mutex_lock(&srv->mu);
    for (moq_msquic_managed_conn_t *conn =
             moq_msquic_lane_next_conn(lane, NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        moq_version_t v = moq_msquic_managed_conn_negotiated_version(conn);
        int slot = (int)(intptr_t)moq_msquic_managed_conn_user(conn) - 1;

        if (v != MOQ_VERSION_DRAFT_16 && v != MOQ_VERSION_DRAFT_18)
            srv->version_errors++;
        if (atomic_load(&srv->hold_publish_for_probe) &&
            atomic_load(&srv->probe_conn) == NULL)
            atomic_store(&srv->probe_conn, conn);
        if (slot < 0) {
            if (srv->next_slot >= 4) {
                srv->version_errors++;
                continue;
            }
            slot = srv->next_slot++;
            srv->slots[slot].version = v;
            srv->slots[slot].conn = conn;
            moq_msquic_managed_conn_set_user(
                conn, (void *)(intptr_t)(slot + 1));
            if (v == MOQ_VERSION_DRAFT_16)
                srv->version16++;
            else if (v == MOQ_VERSION_DRAFT_18)
                srv->version18++;
        } else if (srv->slots[slot].version != v) {
            srv->version_errors++;
        }

        moq_event_t ev;
        while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                srv->slots[slot].sub = ev.u.subscribe_request.sub;
                srv->slots[slot].have_sub = true;
            } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                if (!srv->slots[slot].closed) {
                    if (atomic_load(&srv->probe_conn) ==
                        srv->slots[slot].conn)
                        atomic_store(&srv->probe_conn, NULL);
                    srv->slots[slot].closed = true;
                    srv->closed++;
                }
            }
            moq_event_cleanup(&ev);
        }
        if (s != NULL && srv->slots[slot].have_sub &&
            !srv->slots[slot].published) {
            if (atomic_load(&srv->hold_publish_for_probe) &&
                !atomic_load(&srv->external_probe_done))
                continue;
            srv->slots[slot].published = true;
            srv->published++;

            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            if (moq_session_accept_subscribe(s, srv->slots[slot].sub, &ac,
                                             now) < 0) {
                srv->version_errors++;
                continue;
            }

            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = (uint64_t)slot;
            sgc.publisher_priority = 100;
            if (moq_session_open_subgroup(s, srv->slots[slot].sub, &sgc,
                                          now, &sg) < 0) {
                srv->version_errors++;
                continue;
            }

            uint8_t *bytes = malloc(PAYLOAD_SIZE);
            moq_rcbuf_t *buf = NULL;
            uint8_t salt = v == MOQ_VERSION_DRAFT_18 ? 0x18u : 0x16u;

            if (bytes != NULL)
                payload_fill(bytes, PAYLOAD_SIZE, salt);
            if (bytes == NULL ||
                moq_rcbuf_create(moq_alloc_default(), bytes, PAYLOAD_SIZE,
                                 &buf) < 0) {
                srv->version_errors++;
            } else {
                if (moq_session_write_object(s, sg, 0, buf, now) < 0)
                    srv->version_errors++;
                moq_rcbuf_decref(buf);
            }
            free(bytes);
            (void)moq_session_close_subgroup(s, sg, now);
        }
    }
    pthread_mutex_unlock(&srv->mu);
    return 0;
}

static void server_activity(moq_msquic_managed_t *m, void *ctx)
{
    struct server_side *srv = ctx;
    moq_msquic_managed_conn_t *conn = atomic_load(&srv->probe_conn);

    (void)m;
    if (conn == NULL)
        return;
    if (moq_msquic_managed_conn_negotiated_version(conn) != 0)
        atomic_fetch_add(&srv->activity_nonzero, 1);
    atomic_fetch_add(&srv->activity_checked, 1);
}

static moq_msquic_managed_t *make_server(struct server_side *srv,
                                         const char *cert, const char *key,
                                         const moq_version_t *versions,
                                         size_t version_count)
{
    moq_msquic_managed_cfg_t cfg;

    server_init(srv);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_user = srv;
    cfg.on_activity = server_activity;
    cfg.on_activity_ctx = srv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = 4;
    cfg.versions = versions;
    cfg.version_count = version_count;
    if (moq_msquic_managed_create(&cfg, &srv->m) != MOQ_OK)
        return NULL;
    return srv->m;
}

static moq_msquic_managed_t *make_exact_server(struct server_side *srv,
                                               const char *cert,
                                               const char *key,
                                               moq_version_t version)
{
    moq_msquic_managed_cfg_t cfg;

    server_init(srv);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_user = srv;
    cfg.on_activity = server_activity;
    cfg.on_activity_ctx = srv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = 4;
    cfg.version = version;
    if (moq_msquic_managed_create(&cfg, &srv->m) != MOQ_OK)
        return NULL;
    return srv->m;
}

static moq_msquic_managed_t *make_client(struct client_side *c,
                                         uint16_t port, moq_version_t version)
{
    moq_msquic_managed_cfg_t cfg;

    client_init(c, version);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = client_pump;
    cfg.on_lane_pump_user = c;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.version = version;
    if (moq_msquic_managed_create(&cfg, &c->m) != MOQ_OK)
        return NULL;
    return c->m;
}

static void stop_destroy_server(struct server_side *srv)
{
    if (srv->m != NULL) {
        (void)moq_msquic_managed_stop(srv->m);
        moq_msquic_managed_destroy(srv->m);
        srv->m = NULL;
    }
    server_destroy(srv);
}

static void stop_destroy_client(struct client_side *c)
{
    if (c->m != NULL) {
        (void)moq_msquic_managed_stop(c->m);
        moq_msquic_managed_destroy(c->m);
        c->m = NULL;
    }
    client_destroy(c);
}

static void t_multi_alpn_two_drafts(const char *cert, const char *key)
{
    int before = failures;
    struct server_side srv;
    struct client_side c16, c18;
    struct client_snapshot s16, s18;
    struct server_snapshot ss;
    moq_version_t offered[] = {
        MOQ_VERSION_DRAFT_18,
        MOQ_VERSION_DRAFT_16,
    };

    CHECK(make_server(&srv, cert, key, offered, 2) != NULL);
    if (srv.m == NULL)
        return;
    CHECK(moq_msquic_managed_negotiated_version(srv.m) == 0);

    uint16_t port = moq_msquic_managed_port(srv.m);
    CHECK(port != 0);
    atomic_store(&srv.hold_publish_for_probe, true);

    /* The facade must own the list. Mutating the caller's array after create
     * must not affect later accepted children. */
    offered[0] = (moq_version_t)77;
    offered[1] = (moq_version_t)78;

    CHECK(make_client(&c16, port, MOQ_VERSION_DRAFT_16) != NULL);
    CHECK(make_client(&c18, port, MOQ_VERSION_DRAFT_18) != NULL);
    if (c16.m == NULL || c18.m == NULL)
        goto out;

    CHECK(wait_for_probe_conn(&srv, &c16, &c18, 30000));
    moq_msquic_managed_conn_t *probe = atomic_load(&srv.probe_conn);
    CHECK(probe != NULL);
    if (probe != NULL)
        CHECK(moq_msquic_managed_conn_negotiated_version(probe) == 0);
    atomic_store(&srv.external_probe_done, true);
    CHECK(moq_msquic_managed_wake(srv.m) == MOQ_OK);

    CHECK(wait_for_multi_flow(&srv, &c16, &c18, 30000));
    client_snapshot(&c16, &s16);
    client_snapshot(&c18, &s18);
    server_snapshot(&srv, &ss);
    CHECK(s16.setup == 1);
    CHECK(s18.setup == 1);
    CHECK(s16.sub_ok == 1);
    CHECK(s18.sub_ok == 1);
    CHECK(s16.objects == 1);
    CHECK(s18.objects == 1);
    CHECK(s16.bytes == PAYLOAD_SIZE);
    CHECK(s18.bytes == PAYLOAD_SIZE);
    CHECK(s16.object_errors == 0);
    CHECK(s18.object_errors == 0);
    CHECK(s16.negotiated_ok);
    CHECK(s18.negotiated_ok);
    CHECK(ss.version16 == 1);
    CHECK(ss.version18 == 1);
    CHECK(ss.version_errors == 0);
    CHECK(atomic_load(&srv.activity_checked) > 0);
    CHECK(atomic_load(&srv.activity_nonzero) == 0);

    CHECK(wait_terminal(c16.m, 30000));
    CHECK(wait_terminal(c18.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c16.m));
    CHECK(!moq_msquic_managed_is_fatal(c16.m));
    CHECK(moq_msquic_managed_is_closed(c18.m));
    CHECK(!moq_msquic_managed_is_fatal(c18.m));

out:
    stop_destroy_client(&c16);
    stop_destroy_client(&c18);
    stop_destroy_server(&srv);
    if (failures == before)
        printf("PASS: multi_alpn_two_drafts\n");
}

static void t_exact_server_control(const char *cert, const char *key,
                                   moq_version_t version)
{
    int before = failures;
    struct server_side srv;
    struct client_side c;
    struct client_snapshot cs;
    struct server_snapshot ss;

    CHECK(make_exact_server(&srv, cert, key, version) != NULL);
    if (srv.m == NULL)
        return;
    CHECK(moq_msquic_managed_negotiated_version(srv.m) == version);

    uint16_t port = moq_msquic_managed_port(srv.m);
    CHECK(port != 0);

    CHECK(make_client(&c, port, version) != NULL);
    if (c.m == NULL)
        goto out;
    CHECK(moq_msquic_managed_negotiated_version(c.m) == version);

    CHECK(wait_for_single_flow(&srv, &c, version, 30000));
    client_snapshot(&c, &cs);
    server_snapshot(&srv, &ss);
    CHECK(cs.setup == 1);
    CHECK(cs.sub_ok == 1);
    CHECK(cs.objects == 1);
    CHECK(cs.bytes == PAYLOAD_SIZE);
    CHECK(cs.object_errors == 0);
    CHECK(cs.negotiated_ok);
    CHECK(ss.published == 1);
    CHECK(ss.version_errors == 0);
    if (version == MOQ_VERSION_DRAFT_16) {
        CHECK(ss.version16 == 1);
        CHECK(ss.version18 == 0);
    } else {
        CHECK(ss.version18 == 1);
        CHECK(ss.version16 == 0);
    }

    CHECK(wait_terminal(c.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c.m));
    CHECK(!moq_msquic_managed_is_fatal(c.m));

out:
    stop_destroy_client(&c);
    stop_destroy_server(&srv);
    if (failures == before)
        printf("PASS: exact_server_control_%u\n", (unsigned)version);
}

static void t_no_overlap_alert(const char *cert, const char *key)
{
    int before = failures;
    struct server_side srv;
    struct client_side c16;
    moq_version_t d18_only[] = { MOQ_VERSION_DRAFT_18 };

    CHECK(make_server(&srv, cert, key, d18_only, 1) != NULL);
    if (srv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv.m);

    CHECK(make_client(&c16, port, MOQ_VERSION_DRAFT_16) != NULL);
    if (c16.m == NULL)
        goto out;
    CHECK(wait_terminal(c16.m, 30000));
    CHECK(moq_msquic_managed_is_fatal(c16.m));
    CHECK(moq_msquic_managed_fatal_code(c16.m) == 0x178u);
    CHECK(c16.setup == 0);
    CHECK(moq_msquic_managed_conn_count(srv.m) == 0);

out:
    stop_destroy_client(&c16);
    stop_destroy_server(&srv);
    if (failures == before)
        printf("PASS: no_overlap_alert\n");
}

static moq_version_t run_raw_preference_case(const char *cert,
                                             const char *key,
                                             const moq_version_t *server_order,
                                             size_t server_count,
                                             const moq_version_t *client_order,
                                             size_t client_count)
{
    struct server_side srv;
    struct raw_alpn_client raw;
    moq_version_t selected = 0;

    CHECK(make_server(&srv, cert, key, server_order, server_count) != NULL);
    if (srv.m == NULL)
        return 0;
    uint16_t port = moq_msquic_managed_port(srv.m);
    CHECK(port != 0);

    raw_alpn_client_init(&raw);
    CHECK(raw_alpn_client_connect(&raw, port, client_order, client_count));
    CHECK(raw_wait_connected(&raw, 30000));
    pthread_mutex_lock(&raw.mu);
    CHECK(raw.errors == 0);
    CHECK(raw.connected);
    CHECK(raw.selected_valid);
    selected = raw.selected;
    pthread_mutex_unlock(&raw.mu);
    CHECK(selected != 0);
    CHECK(wait_for_raw_server_conn(&srv, selected, 30000));

    raw_alpn_client_cleanup(&raw);
    stop_destroy_server(&srv);
    return selected;
}

static void t_raw_multi_offer_preference(const char *cert, const char *key)
{
    int before = failures;
    const moq_version_t client_order[] = {
        MOQ_VERSION_DRAFT_16,
        MOQ_VERSION_DRAFT_18,
    };
    const moq_version_t server_18_first[] = {
        MOQ_VERSION_DRAFT_18,
        MOQ_VERSION_DRAFT_16,
    };
    const moq_version_t server_16_first[] = {
        MOQ_VERSION_DRAFT_16,
        MOQ_VERSION_DRAFT_18,
    };

    moq_version_t a = run_raw_preference_case(
        cert, key, server_18_first, 2, client_order, 2);
    moq_version_t b = run_raw_preference_case(
        cert, key, server_16_first, 2, client_order, 2);

    /* With the client order held at {D16, D18}, MsQuic selects the first
     * mutually supported ALPN in the SERVER configuration order. This is a
     * measured transport property, not a MoQ-level promise. */
    CHECK(a == MOQ_VERSION_DRAFT_18);
    CHECK(b == MOQ_VERSION_DRAFT_16);
    CHECK(a != b);

    if (failures == before)
        printf("PASS: raw_multi_offer_preference\n");
}

static void fill_server_cfg(moq_msquic_managed_cfg_t *cfg, const char *cert,
                            const char *key, size_t init_size)
{
    memset(cfg, 0xff, sizeof(*cfg));
    moq_msquic_managed_cfg_init_sized(cfg, init_size);
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_SERVER;
    cfg->host = "127.0.0.1";
    cfg->port = 0;
    cfg->cert_path = cert;
    cfg->key_path = key;
    cfg->on_lane_pump = server_pump;
    cfg->send_request_capacity = true;
    cfg->initial_request_capacity = 16;
    cfg->max_events = 64;
}

static void expect_create_rc(const char *cert, const char *key,
                             moq_msquic_managed_cfg_t *cfg,
                             moq_result_t want)
{
    moq_msquic_managed_t *m = NULL;
    moq_result_t rc = moq_msquic_managed_create(cfg, &m);

    CHECK(rc == want);
    if (m != NULL) {
        (void)moq_msquic_managed_stop(m);
        moq_msquic_managed_destroy(m);
    }
    (void)cert;
    (void)key;
}

static void t_config_validation(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_version_t one[] = { MOQ_VERSION_DRAFT_18 };
    moq_version_t dup[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_18 };
    moq_version_t unknown[] = { (moq_version_t)77 };
    moq_version_t too_many[MOQ_MSQUIC_MANAGED_MAX_VERSIONS + 1];

    for (size_t i = 0; i < sizeof(too_many) / sizeof(too_many[0]); i++)
        too_many[i] = (i & 1u) ? MOQ_VERSION_DRAFT_16
                               : MOQ_VERSION_DRAFT_18;

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.versions = NULL;
    cfg.version_count = 1;
    expect_create_rc(cert, key, &cfg, MOQ_ERR_INVAL);

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.versions = one;
    cfg.version_count = 0;
    expect_create_rc(cert, key, &cfg, MOQ_ERR_INVAL);

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.versions = one;
    cfg.version_count = 1;
    cfg.version = MOQ_VERSION_DRAFT_18;
    expect_create_rc(cert, key, &cfg, MOQ_ERR_INVAL);

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.versions = unknown;
    cfg.version_count = 1;
    expect_create_rc(cert, key, &cfg, MOQ_ERR_UNSUPPORTED);

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.versions = dup;
    cfg.version_count = 2;
    expect_create_rc(cert, key, &cfg, MOQ_ERR_INVAL);

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.versions = too_many;
    cfg.version_count = sizeof(too_many) / sizeof(too_many[0]);
    expect_create_rc(cert, key, &cfg, MOQ_ERR_INVAL);

    fill_server_cfg(&cfg, cert, key, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 9;
    cfg.cert_path = NULL;
    cfg.key_path = NULL;
    cfg.insecure_skip_verify = true;
    cfg.versions = one;
    cfg.version_count = 1;
    expect_create_rc(cert, key, &cfg, MOQ_ERR_INVAL);

    /* Full prefix before the appended list block: poison in the tail is ignored
     * and legacy exact-version server creation still succeeds. */
    fill_server_cfg(&cfg, cert, key,
                    offsetof(moq_msquic_managed_cfg_t, versions));
    expect_create_rc(cert, key, &cfg, MOQ_OK);

    /* Struct sizes that land inside the pointer/count block must be treated as
     * absent and must never read the poisoned partial bytes. */
    fill_server_cfg(&cfg, cert, key,
                    offsetof(moq_msquic_managed_cfg_t, versions) + 1);
    expect_create_rc(cert, key, &cfg, MOQ_OK);

    fill_server_cfg(&cfg, cert, key,
                    offsetof(moq_msquic_managed_cfg_t, version_count) + 1);
    expect_create_rc(cert, key, &cfg, MOQ_OK);

    if (failures == before)
        printf("PASS: config_validation\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }

    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }
    g_msquic_api = lib_pin;

    t_config_validation(argv[1], argv[2]);
    t_exact_server_control(argv[1], argv[2], MOQ_VERSION_DRAFT_16);
    t_exact_server_control(argv[1], argv[2], MOQ_VERSION_DRAFT_18);
    t_multi_alpn_two_drafts(argv[1], argv[2]);
    t_raw_multi_offer_preference(argv[1], argv[2]);
    t_no_overlap_alert(argv[1], argv[2]);

    if (failures == 0)
        printf("PASS: msquic_multi_alpn\n");
    else
        fprintf(stderr, "FAIL: msquic_multi_alpn (%d)\n", failures);
    return failures;
}
