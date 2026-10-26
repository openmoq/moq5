#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fake_msq_managed.h"

#define FAKE_MGD_REQUIRE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "fake_msq_managed: %s:%d: requirement failed: %s\n", \
                __FILE__, __LINE__, #cond); \
        abort(); \
    } \
} while (0)

/* handle discrimination: which of this fake's objects is `h`? */
static fake_mgd_conn_t *conn_of(fake_mgd_t *g, HQUIC h)
{
    for (int i = 0; i < g->conn_hwm; i++)
        if (g->conns[i].in_use && (HQUIC)&g->conns[i].fake == h)
            return &g->conns[i];
    return NULL;
}

/* The one live fake per process-test: the API entry points carry no user
 * context, so the instance is threaded through a file-scope pointer, set by
 * fake_mgd_init. Tests are single-instance by construction. */
static fake_mgd_t *g_live;

static QUIC_STATUS QUIC_API g_registration_open(
    const QUIC_REGISTRATION_CONFIG *cfg, HQUIC *out)
{
    (void)cfg;
    FAKE_MGD_REQUIRE(g_live != NULL);
    g_live->reg_open = true;
    *out = (HQUIC)&g_live->reg_obj;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API g_registration_close(HQUIC h)
{
    (void)h;
    if (g_live != NULL)
        g_live->reg_open = false;
}

static QUIC_STATUS QUIC_API g_configuration_open(
    HQUIC reg, const QUIC_BUFFER *const alpns, uint32_t alpn_count,
    const QUIC_SETTINGS *settings, uint32_t settings_size, void *ctx,
    HQUIC *out)
{
    (void)reg; (void)alpns; (void)alpn_count; (void)settings;
    (void)settings_size; (void)ctx;
    FAKE_MGD_REQUIRE(g_live != NULL);
    g_live->cfg_open = true;
    *out = (HQUIC)&g_live->cfg_obj;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API g_configuration_close(HQUIC h)
{
    (void)h;
    if (g_live != NULL)
        g_live->cfg_open = false;
}

static QUIC_STATUS QUIC_API g_configuration_load_credential(
    HQUIC cfg, const QUIC_CREDENTIAL_CONFIG *cred)
{
    (void)cfg; (void)cred;
    FAKE_MGD_REQUIRE(g_live != NULL);
    g_live->cred_loaded = true; /* no file is opened: paths are never read */
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API g_listener_open(HQUIC reg,
                                            QUIC_LISTENER_CALLBACK_HANDLER cb,
                                            void *ctx, HQUIC *out)
{
    (void)reg;
    FAKE_MGD_REQUIRE(g_live != NULL);
    g_live->listener_cb = cb;
    g_live->listener_ctx = ctx;
    g_live->listener_open = true;
    g_live->listener_handle = (HQUIC)&g_live->listener_obj;
    *out = g_live->listener_handle;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API g_listener_start(HQUIC h, const QUIC_BUFFER *alpns,
                                             uint32_t alpn_count,
                                             const QUIC_ADDR *addr)
{
    (void)h; (void)alpns; (void)alpn_count; (void)addr;
    FAKE_MGD_REQUIRE(g_live != NULL);
    g_live->listener_started = true;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API g_listener_close(HQUIC h)
{
    (void)h;
    if (g_live != NULL)
        g_live->listener_open = false;
}

static void QUIC_API g_connection_close(HQUIC h)
{
    fake_mgd_conn_t *c = g_live != NULL ? conn_of(g_live, h) : NULL;

    if (c != NULL) {
        if (c->fake.shutdown_complete) {
            FAKE_MGD_REQUIRE(fake_msq_pending_sends(&c->fake) == 0 &&
                              fake_msq_pending_dgrams(&c->fake) == 0);
        } else {
            fake_msq_cancel_pending(&c->fake, c->cb, c->ctx);
        }
        fake_msq_mark_connection_closed(&c->fake);
        c->closed = true;
        c->in_use = false;   /* the slot is reusable by a later accept */
        g_live->conn_count--;
    }
}

static QUIC_STATUS QUIC_API g_connection_open(HQUIC reg,
    QUIC_CONNECTION_CALLBACK_HANDLER cb, void *ctx, HQUIC *out)
{
    (void)reg;
    FAKE_MGD_REQUIRE(g_live != NULL);
    for (int i = 0; i < FAKE_MGD_MAX_CONNS; ++i) {
        fake_mgd_conn_t *c = &g_live->conns[i];
        if (c->in_use) continue;
        memset(c, 0, sizeof(*c));
        fake_msq_init(&c->fake, true);
        c->in_use = true;
        c->cb = cb;
        c->ctx = ctx;
        ++g_live->conn_count;
        if (g_live->conn_hwm < i + 1) g_live->conn_hwm = i + 1;
        *out = (HQUIC)&c->fake;
        return QUIC_STATUS_SUCCESS;
    }
    return QUIC_STATUS_OUT_OF_MEMORY;
}

static QUIC_STATUS QUIC_API g_connection_start(HQUIC h, HQUIC cfg,
    QUIC_ADDRESS_FAMILY family, const char *host, uint16_t port)
{
    (void)cfg; (void)family; (void)host; (void)port;
    fake_mgd_conn_t *c = g_live != NULL ? conn_of(g_live, h) : NULL;
    FAKE_MGD_REQUIRE(c != NULL && c->cb != NULL);
    c->configured = true;
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API g_connection_shutdown(
    HQUIC h, QUIC_CONNECTION_SHUTDOWN_FLAGS flags, QUIC_UINT62 code)
{
    fake_mgd_conn_t *c = g_live != NULL ? conn_of(g_live, h) : NULL;

    (void)flags; (void)code;
    if (c != NULL)
        c->shutdowns++;
}

static QUIC_STATUS QUIC_API g_connection_set_configuration(HQUIC h, HQUIC cfg)
{
    fake_mgd_conn_t *c = g_live != NULL ? conn_of(g_live, h) : NULL;

    (void)cfg;
    FAKE_MGD_REQUIRE(c != NULL);
    c->configured = true;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_CONNECTION_CALLBACK_HANDLER ptr_to_conn_cb(void *cb)
{
    union { void *obj; QUIC_CONNECTION_CALLBACK_HANDLER fn; } u = { .obj = cb };
    return u.fn;
}

static QUIC_STREAM_CALLBACK_HANDLER ptr_to_stream_cb(void *cb)
{
    union { void *obj; QUIC_STREAM_CALLBACK_HANDLER fn; } u = { .obj = cb };
    return u.fn;
}

/* SetCallbackHandler is the one entry point whose handle kind is ambiguous:
 * the facade sets CONNECTION handlers on accepted children while the adapter
 * sets STREAM handlers on accepted streams. Discriminate by registry. */
static void QUIC_API g_set_callback_handler(HQUIC h, void *cb, void *ctx)
{
    fake_mgd_conn_t *c = g_live != NULL ? conn_of(g_live, h) : NULL;

    if (c != NULL) {
        c->cb = ptr_to_conn_cb(cb);
        c->ctx = ctx;
        return;
    }
    fake_msq_stream_t *st = (fake_msq_stream_t *)h;
    st->cb = ptr_to_stream_cb(cb);
    st->ctx = ctx;
}

static QUIC_STATUS QUIC_API g_set_param(HQUIC h, uint32_t param, uint32_t len,
                                        const void *buf)
{
    (void)h; (void)param; (void)len; (void)buf;
    return QUIC_STATUS_SUCCESS; /* the global-floor write: recorded nowhere,
                                 * succeeds, touches no process state */
}

static QUIC_STATUS QUIC_API g_get_param(HQUIC h, uint32_t param,
                                        uint32_t *len, void *buf)
{
    /* a connection with QUIC DATAGRAM negotiated both ways, as the real
     * library reports it once the handshake has completed */
    if ((param == QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED ||
         param == QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED) &&
        *len >= sizeof(uint8_t)) {
        *(uint8_t *)buf = 1;
        *len = sizeof(uint8_t);
        return QUIC_STATUS_SUCCESS;
    }
    if (param == QUIC_PARAM_STREAM_ID && *len >= sizeof(uint64_t)) {
        *(uint64_t *)buf = ((fake_msq_stream_t *)h)->id;
        *len = sizeof(uint64_t);
        return QUIC_STATUS_SUCCESS;
    }
    return QUIC_STATUS_NOT_SUPPORTED; /* the bound-port read: the facade
                                       * tolerates failure (port stays 0) */
}

void fake_mgd_init(fake_mgd_t *g)
{
    fake_msq_t scratch;

    memset(g, 0, sizeof(*g));
    /* stream-level entries are shared statics: borrow them from a scratch
     * per-conn fake so stream behavior is identical to the existing fake */
    fake_msq_init(&scratch, false);
    g->api = scratch.api;
    g->api.RegistrationOpen = g_registration_open;
    g->api.RegistrationClose = g_registration_close;
    g->api.ConfigurationOpen = g_configuration_open;
    g->api.ConfigurationClose = g_configuration_close;
    g->api.ConfigurationLoadCredential = g_configuration_load_credential;
    g->api.ListenerOpen = g_listener_open;
    g->api.ListenerStart = g_listener_start;
    g->api.ListenerClose = g_listener_close;
    g->api.ConnectionClose = g_connection_close;
    g->api.ConnectionOpen = g_connection_open;
    g->api.ConnectionStart = g_connection_start;
    g->api.ConnectionShutdown = g_connection_shutdown;
    g->api.ConnectionSetConfiguration = g_connection_set_configuration;
    g->api.SetCallbackHandler = g_set_callback_handler;
    g->api.SetParam = g_set_param;
    g->api.GetParam = g_get_param;
    g_live = g;
}

const QUIC_API_TABLE *fake_mgd_table(fake_mgd_t *g)
{
    return &g->api;
}

fake_mgd_conn_t *fake_mgd_accept(fake_mgd_t *g, const char *alpn)
{
    FAKE_MGD_REQUIRE(g->listener_cb != NULL && g->listener_started);
    FAKE_MGD_REQUIRE(g->conn_count < FAKE_MGD_MAX_CONNS);
    fake_mgd_conn_t *c = NULL;

    for (int i = 0; i < FAKE_MGD_MAX_CONNS; i++) {
        if (!g->conns[i].in_use) {
            c = &g->conns[i];
            if (i + 1 > g->conn_hwm)
                g->conn_hwm = i + 1;
            break;
        }
    }
    FAKE_MGD_REQUIRE(c != NULL);
    g->conn_count++;

    memset(c, 0, sizeof(*c));
    fake_msq_init(&c->fake, false);
    c->in_use = true;

    QUIC_NEW_CONNECTION_INFO info;
    QUIC_ADDR remote, local;
    memset(&info, 0, sizeof(info));
    memset(&remote, 0, sizeof(remote));
    memset(&local, 0, sizeof(local));
    info.RemoteAddress = &remote;
    info.LocalAddress = &local;
    info.NegotiatedAlpn = (const uint8_t *)alpn;
    info.NegotiatedAlpnLength = (uint8_t)strlen(alpn);

    QUIC_LISTENER_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_LISTENER_EVENT_NEW_CONNECTION;
    ev.NEW_CONNECTION.Info = &info;
    ev.NEW_CONNECTION.Connection = (HQUIC)&c->fake;

    QUIC_STATUS st = g->listener_cb(g->listener_handle, g->listener_ctx, &ev);
    if (QUIC_FAILED(st) || c->cb == NULL) {
        c->in_use = false;
        g->conn_count--;
        return NULL;
    }
    return c;
}

void fake_mgd_deliver_connected(fake_mgd_conn_t *c, const char *alpn)
{
    QUIC_CONNECTION_EVENT ev;

    FAKE_MGD_REQUIRE(c->cb != NULL);
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_CONNECTED;
    ev.CONNECTED.NegotiatedAlpn = (const uint8_t *)alpn;
    ev.CONNECTED.NegotiatedAlpnLength = (uint8_t)strlen(alpn);
    (void)c->cb((HQUIC)&c->fake, c->ctx, &ev);
}

fake_msq_stream_t *
fake_mgd_peer_stream_started(fake_mgd_conn_t *c, uint64_t id, bool uni)
{
    fake_msq_stream_t *st = fake_msq_peer_stream(&c->fake, id, uni);
    QUIC_CONNECTION_EVENT ev;

    FAKE_MGD_REQUIRE(c->cb != NULL);
    if (st == NULL) {
        return NULL;
    }
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED;
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags =
        uni ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAG_NONE;
    (void)c->cb((HQUIC)&c->fake, c->ctx, &ev);
    return st;
}

void
fake_mgd_deliver_peer_send_aborted(fake_mgd_conn_t *c, fake_msq_stream_t *st,
                                   uint64_t code)
{
    QUIC_STREAM_EVENT ev;

    (void)c;
    FAKE_MGD_REQUIRE(st->cb != NULL);
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_STREAM_EVENT_PEER_SEND_ABORTED;
    ev.PEER_SEND_ABORTED.ErrorCode = code;
    (void)st->cb((HQUIC)st, st->ctx, &ev);
}

void
fake_mgd_deliver_peer_recv_aborted(fake_mgd_conn_t *c, fake_msq_stream_t *st,
                                   uint64_t code)
{
    QUIC_STREAM_EVENT ev;

    (void)c;
    FAKE_MGD_REQUIRE(st->cb != NULL);
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED;
    ev.PEER_RECEIVE_ABORTED.ErrorCode = code;
    (void)st->cb((HQUIC)st, st->ctx, &ev);
}

void fake_mgd_deliver_peer_close(fake_mgd_conn_t *c, uint64_t code)
{
    QUIC_CONNECTION_EVENT ev;

    FAKE_MGD_REQUIRE(c->cb != NULL);
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER;
    ev.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = code;
    (void)c->cb((HQUIC)&c->fake, c->ctx, &ev);
}

void fake_mgd_deliver_shutdown_complete(fake_mgd_conn_t *c)
{
    QUIC_CONNECTION_EVENT ev;

    FAKE_MGD_REQUIRE(c->cb != NULL);
    fake_msq_cancel_pending(&c->fake, c->cb, c->ctx);
    FAKE_MGD_REQUIRE(fake_msq_pending_sends(&c->fake) == 0 &&
                      fake_msq_pending_dgrams(&c->fake) == 0);
    fake_msq_mark_shutdown_complete(&c->fake);
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE;
    (void)c->cb((HQUIC)&c->fake, c->ctx, &ev);
}
