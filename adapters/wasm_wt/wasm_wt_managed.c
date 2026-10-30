/*
 * moq_wasm_wt_managed: managed WebTransport facade for a browser build.
 * See <moq/wasm_wt_managed.h>. The browser side is wasm_wt_browser.js.
 */
#include <moq/wasm_wt_managed.h>
#include <moq/transport_bridge.h>
#include <moq/transport_binding.h>
#include <moq/rcbuf.h>

#include "../common/moq_adapter_binding.h"
#include "../common/moq_alpn.h"
#include "../common/moq_setup_tokens.h"

#include <emscripten.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- Browser side (wasm_wt_browser.js) ------------------------------- */

extern int  moq_wasm_wt_js_supported(void);
extern void moq_wasm_wt_js_connect(moq_wasm_wt_managed_t *m, const char *url, const char *protocols);
extern int  moq_wasm_wt_js_open_stream(moq_wasm_wt_managed_t *m, int bidi);
extern void moq_wasm_wt_js_write(moq_wasm_wt_managed_t *m, int id, const uint8_t *p, int len, int fin);
extern void moq_wasm_wt_js_reset(moq_wasm_wt_managed_t *m, int id, int code);
extern void moq_wasm_wt_js_stop(moq_wasm_wt_managed_t *m, int id, int code);
extern void moq_wasm_wt_js_datagram(moq_wasm_wt_managed_t *m, const uint8_t *p, int len);
extern int  moq_wasm_wt_js_datagram_max(moq_wasm_wt_managed_t *m);
extern void moq_wasm_wt_js_close(moq_wasm_wt_managed_t *m, int code);
extern void moq_wasm_wt_js_schedule(moq_wasm_wt_managed_t *m, double delay_ms);
extern void moq_wasm_wt_js_release(moq_wasm_wt_managed_t *m);

/* -- State ------------------------------------------------------------ */

struct moq_wasm_wt_managed {
    moq_alloc_t alloc;

    char *host;
    int   port;
    char *path;
    char *wt_protocols;

    bool     send_request_capacity;
    uint64_t initial_request_capacity;
    uint64_t goaway_timeout_us;

    moq_connection_target_t target;     /* borrows target_bytes */
    uint8_t *target_bytes;
    size_t   target_bytes_len;
    moq_setup_tokens_t setup_tokens;

    int      (*on_pump)(moq_wasm_wt_managed_t *, uint64_t, void *);
    void    *on_pump_ctx;
    uint64_t (*app_deadline_us)(void *);
    void    *app_deadline_ctx;

    moq_session_t          *session;
    moq_transport_bridge_t *bridge;
    moq_transport_endpoint_ops_t ops;
    moq_version_t version;
    size_t  datagram_max;

    bool ready;
    bool fatal;
    bool closed;
    bool stopped;
    bool pump_ended;
    bool in_service;
    bool again;
    bool released;                      /* JS side told to forget this facade */
    uint64_t fatal_code;
    uint64_t close_code;
};

static uint64_t wwm_now_us(void) { return (uint64_t)(emscripten_get_now() * 1000.0); }

static char *wwm_strdup(const moq_alloc_t *a, const char *s)
{
    size_t n = strlen(s) + 1;
    char *p = (char *)a->alloc(n, a->ctx);
    if (p) memcpy(p, s, n);
    return p;
}

/* -- Endpoint ops for the transport bridge (all through JavaScript) ---- */

static moq_transport_result_t op_open_uni(void *c, uint64_t *id)
{
    *id = (uint64_t)moq_wasm_wt_js_open_stream((moq_wasm_wt_managed_t *)c, 0);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t op_open_bidi(void *c, uint64_t *id)
{
    *id = (uint64_t)moq_wasm_wt_js_open_stream((moq_wasm_wt_managed_t *)c, 1);
    return MOQ_TRANSPORT_OK;
}

/* A browser write is always accepted: JavaScript queues it behind the stream's
 * earlier writes and the browser applies the stream's flow control. */
static moq_transport_result_t op_write(void *c, uint64_t id, const uint8_t *d, size_t n, bool fin)
{
    moq_wasm_wt_js_write((moq_wasm_wt_managed_t *)c, (int)id, d, (int)n, fin ? 1 : 0);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t op_write_payload(void *c, uint64_t id, moq_rcbuf_t *b, bool fin)
{
    moq_wasm_wt_js_write((moq_wasm_wt_managed_t *)c, (int)id, moq_rcbuf_data(b), (int)moq_rcbuf_len(b),
                         fin ? 1 : 0);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t op_reset(void *c, uint64_t id, uint64_t code)
{
    moq_wasm_wt_js_reset((moq_wasm_wt_managed_t *)c, (int)id, (int)code);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t op_stop(void *c, uint64_t id, uint64_t code)
{
    moq_wasm_wt_js_stop((moq_wasm_wt_managed_t *)c, (int)id, (int)code);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t op_datagram(void *c, const uint8_t *d, size_t n)
{
    moq_wasm_wt_managed_t *m = (moq_wasm_wt_managed_t *)c;
    if (n > m->datagram_max) return MOQ_TRANSPORT_TOO_LARGE;
    moq_wasm_wt_js_datagram(m, d, (int)n);
    return MOQ_TRANSPORT_OK;
}

static size_t op_max_datagram(void *c) { return ((moq_wasm_wt_managed_t *)c)->datagram_max; }

static moq_transport_result_t op_close(void *c, uint64_t code, const uint8_t *r, size_t n)
{
    (void)r; (void)n;
    moq_wasm_wt_js_close((moq_wasm_wt_managed_t *)c, (int)code);
    return MOQ_TRANSPORT_OK;
}

/* -- Pump cycle ---------------------------------------------------------- */

static void wwm_note_terminal(moq_wasm_wt_managed_t *m)
{
    if (!m->bridge) return;
    if (!m->fatal && moq_transport_bridge_is_fatal(m->bridge)) {
        m->fatal = true;
        m->fatal_code = moq_transport_bridge_fatal_code(m->bridge);
    }
    if (!m->closed && moq_transport_bridge_is_closed(m->bridge)) {
        m->closed = true;
        m->close_code = moq_transport_bridge_close_code(m->bridge);
    }
}

static bool wwm_terminal(const moq_wasm_wt_managed_t *m) { return m->fatal || m->closed || m->pump_ended; }

static void wwm_arm_timer(moq_wasm_wt_managed_t *m)
{
    if (m->released || m->stopped || wwm_terminal(m)) return;
    uint64_t d = UINT64_MAX;
    if (m->session) d = moq_session_next_deadline_us(m->session);
    if (m->app_deadline_us) {
        uint64_t a = m->app_deadline_us(m->app_deadline_ctx);
        if (a < d) d = a;
    }
    if (d == UINT64_MAX) return;       /* nothing time-based pending: events drive the next cycle */
    uint64_t now = wwm_now_us();
    moq_wasm_wt_js_schedule(m, d > now ? (double)(d - now) / 1000.0 : 0.0);
}

void moq_wasm_wt_managed_service(moq_wasm_wt_managed_t *m)
{
    if (!m || m->released) return;
    if (m->in_service) {               /* re-entered from a callback of this very cycle */
        m->again = true;
        return;
    }
    m->in_service = true;
    do {
        m->again = false;
        uint64_t now = wwm_now_us();
        if (m->bridge && !m->fatal && !m->closed) {
            if (moq_transport_bridge_service(m->bridge, now) < 0) wwm_note_terminal(m);
        }
        if (m->on_pump && !m->pump_ended && m->on_pump(m, now, m->on_pump_ctx) != 0)
            m->pump_ended = true;
        if (m->bridge && !m->fatal && !m->closed) {
            if (moq_transport_bridge_service(m->bridge, now) < 0) wwm_note_terminal(m);
        }
        wwm_note_terminal(m);
    } while (m->again);
    m->in_service = false;
    wwm_arm_timer(m);
}

/* -- Browser to libmoq -------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE uint8_t *moq_wasm_wt_alloc(int n) { return (uint8_t *)malloc(n > 0 ? (size_t)n : 1u); }
EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_free(uint8_t *p) { free(p); }

static void wwm_fail(moq_wasm_wt_managed_t *m, uint64_t code)
{
    if (!m->fatal) {
        m->fatal = true;
        m->fatal_code = code;
    }
    moq_wasm_wt_managed_service(m);
}

/* The connection is up and the relay named the subprotocol ("" = none). */
EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_on_ready(moq_wasm_wt_managed_t *m, const char *protocol, int datagrams)
{
    if (!m || m->released || m->stopped) return;
    moq_version_t v = (moq_version_t)0;
    size_t plen = protocol ? strlen(protocol) : 0;
    /* A relay that names no subprotocol, or one we did not offer, cannot be
     * served: a browser has no in-band version negotiation to fall back on. */
    if (!moq_alpn_to_version(protocol, plen, &v)) { wwm_fail(m, 0x10); return; }

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &m->alloc, MOQ_PERSPECTIVE_CLIENT);
    scfg.version = v;
    scfg.send_request_capacity = m->send_request_capacity;
    scfg.initial_request_capacity = m->initial_request_capacity ? m->initial_request_capacity : 64;
    scfg.goaway_timeout_us = m->goaway_timeout_us;
    scfg.setup_auth_tokens = m->setup_tokens.tokens;
    scfg.setup_auth_token_count = m->setup_tokens.count;
    uint64_t now = wwm_now_us();
    if (moq_session_create(&scfg, now, &m->session) != MOQ_OK) { m->session = NULL; wwm_fail(m, 0x11); return; }
    m->version = v;

    m->datagram_max = datagrams ? (size_t)moq_wasm_wt_js_datagram_max(m) : 0;
    memset(&m->ops, 0, sizeof(m->ops));
    m->ops.struct_size = sizeof(m->ops);
    m->ops.capabilities = MOQ_TRANSPORT_CAP_WRITE_PAYLOAD | MOQ_TRANSPORT_CAP_HOLD_INPUT |
                          (m->datagram_max ? MOQ_TRANSPORT_CAP_DATAGRAM : 0u);
    m->ops.open_uni = op_open_uni;
    m->ops.open_bidi = op_open_bidi;
    m->ops.write = op_write;
    m->ops.write_payload = op_write_payload;
    m->ops.reset_stream = op_reset;
    m->ops.stop_sending = op_stop;
    m->ops.send_datagram = op_datagram;
    m->ops.max_datagram_size = op_max_datagram;
    m->ops.close_transport = op_close;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &m->alloc);
    if (moq_transport_bridge_create(&bc, m->session, &m->ops, m, &m->bridge) != MOQ_OK) {
        m->bridge = NULL;
        wwm_fail(m, 0x12);
        return;
    }

    /* A WebTransport session names its target and tells the profile what the
     * connection supports. The browser exposes datagrams through the same
     * object, so local and peer support are one fact here. */
    uint32_t known = MOQ_BINDING_FACTS_ALL;
    uint32_t supported = MOQ_BINDING_HANDSHAKE_COMPLETE;
    if (m->datagram_max)
        supported |= MOQ_BINDING_LOCAL_QUIC_DATAGRAM | MOQ_BINDING_PEER_QUIC_DATAGRAM |
                     MOQ_BINDING_DATAGRAM_SEND | MOQ_BINDING_DATAGRAM_RECEIVE;
    if (moq_adapter_bind_session(m->session, MOQ_BINDING_WEBTRANSPORT_H3, known, supported, &m->target) != MOQ_OK) {
        wwm_fail(m, 0x13);
        return;
    }
    if (moq_session_start(m->session, now) != MOQ_OK) { wwm_fail(m, 0x14); return; }
    m->ready = true;
    moq_wasm_wt_managed_service(m);
}

/* The connection never came up (CONNECT refused, certificate rejected, ...). */
EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_on_failed(moq_wasm_wt_managed_t *m, int code)
{
    if (!m || m->released) return;
    wwm_fail(m, (uint64_t)(unsigned)code);
}

/* The connection ended: `clean` when the peer closed it with a close code. */
EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_on_closed(moq_wasm_wt_managed_t *m, int code, int clean)
{
    if (!m || m->released) return;
    if (m->bridge) {
        if (clean) (void)moq_transport_bridge_on_transport_close(m->bridge, (uint64_t)code, wwm_now_us());
        else       (void)moq_transport_bridge_on_transport_error(m->bridge, (uint64_t)code, wwm_now_us());
    } else {
        if (clean) { m->closed = true; m->close_code = (uint64_t)code; }
        else       { m->fatal = true; m->fatal_code = (uint64_t)code; }
    }
    moq_wasm_wt_managed_service(m);
}

/* Bytes of a peer stream. kind: 0 unidirectional, 1 bidirectional. Returns the
 * bridge's result; MOQ_ERR_WOULD_BLOCK and MOQ_ERR_INPUT_NOT_CONSUMED mean the
 * chunk was not taken and JavaScript redelivers it after the next pump cycle. */
EMSCRIPTEN_KEEPALIVE int moq_wasm_wt_on_stream_bytes(moq_wasm_wt_managed_t *m, int id, int kind,
                                                      const uint8_t *d, int n, int fin)
{
    if (!m || m->released || !m->bridge) return MOQ_ERR_CLOSED;
    uint64_t now = wwm_now_us();
    moq_result_t rc = kind ? moq_transport_bridge_on_peer_bidi_bytes(m->bridge, (uint64_t)id, d, (size_t)n, fin != 0, now)
                           : moq_transport_bridge_on_peer_uni_bytes(m->bridge, (uint64_t)id, d, (size_t)n, fin != 0, now);
    moq_wasm_wt_managed_service(m);
    return rc;
}

EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_on_stream_reset(moq_wasm_wt_managed_t *m, int id, int code)
{
    if (!m || m->released || !m->bridge) return;
    (void)moq_transport_bridge_on_peer_stream_reset(m->bridge, (uint64_t)id, (uint64_t)(unsigned)code, wwm_now_us());
    moq_wasm_wt_managed_service(m);
}

EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_on_datagram(moq_wasm_wt_managed_t *m, const uint8_t *d, int n)
{
    if (!m || m->released || !m->bridge) return;
    (void)moq_transport_bridge_on_peer_datagram(m->bridge, d, (size_t)n, wwm_now_us());
    moq_wasm_wt_managed_service(m);
}

/* True while the bridge holds retry state for the stream: JavaScript does not
 * deliver more bytes for it until this clears. */
EMSCRIPTEN_KEEPALIVE int moq_wasm_wt_stream_pending(moq_wasm_wt_managed_t *m, int id)
{
    if (!m || m->released || !m->bridge) return 0;
    return moq_transport_bridge_stream_has_pending(m->bridge, (uint64_t)id) ? 1 : 0;
}

EMSCRIPTEN_KEEPALIVE void moq_wasm_wt_on_timer(moq_wasm_wt_managed_t *m)
{
    if (m) moq_wasm_wt_managed_service(m);
}

/* -- Public API ---------------------------------------------------------- */

void moq_wasm_wt_managed_cfg_init(moq_wasm_wt_managed_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

static void wwm_free_all(moq_wasm_wt_managed_t *m)
{
    const moq_alloc_t *a = &m->alloc;
    if (m->host) a->free(m->host, strlen(m->host) + 1, a->ctx);
    if (m->path) a->free(m->path, strlen(m->path) + 1, a->ctx);
    if (m->wt_protocols) a->free(m->wt_protocols, strlen(m->wt_protocols) + 1, a->ctx);
    if (m->target_bytes) a->free(m->target_bytes, m->target_bytes_len, a->ctx);
    moq_setup_tokens_free(a, &m->setup_tokens);
    a->free(m, sizeof(*m), a->ctx);
}

moq_result_t moq_wasm_wt_managed_create(const moq_wasm_wt_managed_cfg_t *cfg, moq_wasm_wt_managed_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!cfg || cfg->struct_size < sizeof(uint32_t) || !cfg->alloc || !cfg->alloc->alloc ||
        !cfg->alloc->realloc || !cfg->alloc->free || !cfg->host || !cfg->host[0] ||
        cfg->port < 1 || cfg->port > 65535 || !cfg->wt_protocols || !cfg->wt_protocols[0] || !cfg->on_pump)
        return MOQ_ERR_INVAL;
    if (!moq_wasm_wt_js_supported()) return MOQ_ERR_UNSUPPORTED;

    const moq_alloc_t *a = cfg->alloc;
    moq_wasm_wt_managed_t *m = (moq_wasm_wt_managed_t *)a->alloc(sizeof(*m), a->ctx);
    if (!m) return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *a;
    m->host = wwm_strdup(a, cfg->host);
    m->path = wwm_strdup(a, cfg->path && cfg->path[0] ? cfg->path : "/moq");
    m->wt_protocols = wwm_strdup(a, cfg->wt_protocols);
    if (!m->host || !m->path || !m->wt_protocols) { wwm_free_all(m); return MOQ_ERR_NOMEM; }
    m->port = cfg->port;
    m->send_request_capacity = cfg->send_request_capacity;
    m->initial_request_capacity = cfg->initial_request_capacity;
    m->goaway_timeout_us = cfg->goaway_timeout_us;
    m->on_pump = cfg->on_pump;
    m->on_pump_ctx = cfg->on_pump_ctx;
    m->app_deadline_us = cfg->app_deadline_us;
    m->app_deadline_ctx = cfg->app_deadline_ctx;

    moq_result_t rc = moq_setup_tokens_copy(a, cfg->setup_auth_tokens, cfg->setup_auth_token_count, &m->setup_tokens);
    if (rc != MOQ_OK) { wwm_free_all(m); return rc; }

    if (cfg->target) {
        rc = moq_adapter_target_copy(a, cfg->target, &m->target, &m->target_bytes, &m->target_bytes_len);
        if (rc != MOQ_OK) { wwm_free_all(m); return rc; }
    } else {
        char buf[512];
        moq_connection_target_t t;
        if (!moq_adapter_target_from_dial(buf, sizeof(buf), cfg->host, cfg->port, m->path, &t)) {
            wwm_free_all(m);
            return MOQ_ERR_INVAL;
        }
        rc = moq_adapter_target_copy(a, &t, &m->target, &m->target_bytes, &m->target_bytes_len);
        if (rc != MOQ_OK) { wwm_free_all(m); return rc; }
    }

    char url[640];
    char auth[512];
    if (!moq_adapter_connect_authority(auth, sizeof(auth), &m->target, m->host, m->port) ||
        snprintf(url, sizeof(url), "https://%s%s", auth, m->path) >= (int)sizeof(url)) {
        wwm_free_all(m);
        return MOQ_ERR_INVAL;
    }
    *out = m;
    moq_wasm_wt_js_connect(m, url, m->wt_protocols);   /* asynchronous; callbacks follow */
    return MOQ_OK;
}

moq_result_t moq_wasm_wt_managed_stop(moq_wasm_wt_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->stopped) return MOQ_OK;
    m->stopped = true;
    if (!wwm_terminal(m)) moq_wasm_wt_js_close(m, 0);
    m->closed = true;
    return MOQ_OK;
}

void moq_wasm_wt_managed_destroy(moq_wasm_wt_managed_t *m)
{
    if (!m) return;
    m->released = true;
    moq_wasm_wt_js_release(m);          /* closes the connection, clears its timer, forgets the facade */
    if (m->bridge) moq_transport_bridge_destroy(m->bridge);   /* before its session */
    if (m->session) moq_session_destroy(m->session);
    wwm_free_all(m);
}

moq_session_t *moq_wasm_wt_managed_session(moq_wasm_wt_managed_t *m) { return m ? m->session : NULL; }
moq_version_t moq_wasm_wt_managed_negotiated_version(const moq_wasm_wt_managed_t *m) { return m ? m->version : (moq_version_t)0; }
bool moq_wasm_wt_managed_is_fatal(const moq_wasm_wt_managed_t *m) { return m && m->fatal; }
uint64_t moq_wasm_wt_managed_fatal_code(const moq_wasm_wt_managed_t *m) { return m ? m->fatal_code : 0; }
bool moq_wasm_wt_managed_is_closed(const moq_wasm_wt_managed_t *m) { return m && m->closed; }
uint64_t moq_wasm_wt_managed_close_code(const moq_wasm_wt_managed_t *m) { return m ? m->close_code : 0; }

moq_result_t moq_wasm_wt_managed_wake(moq_wasm_wt_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->released || wwm_terminal(m)) return MOQ_ERR_CLOSED;
    moq_wasm_wt_js_schedule(m, 0.0);
    return MOQ_OK;
}

moq_result_t moq_wasm_wt_managed_wait(moq_wasm_wt_managed_t *m, uint64_t timeout_us)
{
    (void)m; (void)timeout_us;
    return MOQ_ERR_UNSUPPORTED;
}
