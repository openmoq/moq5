/*
 * Managed MoQ client over wtquic's Network.framework backend.
 *
 * The facade proxies the wtquic session events: before establishment
 * they latch terminal state locally (refusal, failure, pre-
 * establishment close — no MoQ session is ever constructed for
 * those); at on_established the selected WT-Protocol token is
 * validated against the offer, the MoQ session is created with the
 * NEGOTIATED version, the attach adapter binds it, and establishment
 * (plus every later event) forwards into the adapter's own table.
 * Everything runs on the wtq_nw_conn domain; wake() maps onto
 * wtq_nw_conn_post. Final teardown destroys the adapter and the owned
 * session ON-DOMAIN inside the backend's on_stopped — the last domain
 * block — before join()/wait() observe completion.
 */
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <moq/wtquic_network_managed.h>

#include "../common/moq_alpn.h"

#define NW_MAX_OFFERS 8

/* The single lane and the single connection view: stable objects
 * embedded in the facade, published through the lane API. Both are
 * back-pointers only — every access gates on the facade's state. */
struct moq_wtquic_network_managed_lane {
    moq_wtquic_network_managed_t *owner;
};

struct moq_wtquic_network_managed_conn {
    moq_wtquic_network_managed_t *owner;
};

struct moq_wtquic_network_managed {
    moq_alloc_t alloc;

    wtq_nw_conn_t *conn;
    moq_session_t *ms;       /* domain-confined; NULL until negotiated */
    moq_wtquic_conn_t *mc;   /* attach adapter; domain-confined */
    const wtq_session_events_t *fwd; /* the adapter's event table */

    struct moq_wtquic_network_managed_lane lane;  /* the one lane */
    struct moq_wtquic_network_managed_conn cview; /* the one conn view */
    /* The strict lane window marker: set only around the on_lane_pump
     * invocation, on the domain. Domain-confined — written on the
     * domain, and read only AFTER wtq_nw_conn_is_on_domain confirmed
     * the reader is the domain (off-domain readers never reach the
     * read; they already returned NULL). */
    bool in_lane_pump;

    pthread_mutex_t mu;
    pthread_cond_t cv;
    uint64_t pumps;          /* wait()/wake() activity counter */
    bool activity_pending;   /* LEVEL-RETAINED, COALESCED activity: any
                                number of pumps between two waits
                                collapse into ONE retained level (a
                                drain-until-idle consumer needs "work
                                may exist", never one token per pump);
                                consumed by exactly one wait().
                                The pumps counter alone is EDGE-based
                                (advanced-since-entry): a pump landing
                                BETWEEN two waits would be invisible to
                                the next one, and on an idle connection
                                no later pump ever arrives to repeat
                                the signal — the endpoint engine then
                                sleeps a full wait slice on top of a
                                delivered object (measured: the runtime
                                proof's missed-wake object timeouts). */
    bool fatal;
    bool closed;
    bool pump_exit;
    bool stop_started;
    bool torn_down;          /* adapter+session destroyed on-domain */
    uint64_t fatal_code;
    uint64_t close_code;
    moq_version_t version;   /* 0 until negotiated */
    bool terr_set;
    wtq_transport_error_t terr;

    /* copied config */
    char *host;
    char *path;
    char *authority;
    char *protos_buf;                    /* mutable copy, NUL-split */
    size_t protos_buf_size;              /* ORIGINAL allocation size:
                                            NUL-splitting makes strlen
                                            useless for the sized free */
    const char *offers[NW_MAX_OFFERS];   /* into protos_buf */
    size_t offer_len[NW_MAX_OFFERS];
    size_t offer_count;
    bool send_request_capacity;
    uint64_t initial_request_capacity;

    moq_wtquic_network_lane_pump_fn on_lane_pump;
    void *on_lane_pump_user;
    moq_wtquic_network_activity_fn on_activity;
    void *on_activity_ctx;
    void (*on_stopped)(void *ctx);
    void *on_stopped_ctx;

    /* App service-deadline query + its cached fold. app_deadline_us is
     * the opaque callback (ctx = the service endpoint); wake_deadline_us
     * is the ABSOLUTE combined deadline last armed on the native delayed
     * doorbell (UINT64_MAX = none/unarmed). Domain-confined: written and
     * read only on the domain. */
    uint64_t (*app_deadline_us)(void *ctx);
    void *app_deadline_ctx;
    uint64_t wake_deadline_us;
};

static uint64_t nwm_now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static char *nwm_strdup(moq_wtquic_network_managed_t *m, const char *s)
{
    if (s == NULL)
        return NULL;
    size_t n = strlen(s) + 1;
    char *d = m->alloc.alloc(n, m->alloc.ctx);
    if (d != NULL)
        memcpy(d, s, n);
    return d;
}

static void nwm_free_str(moq_wtquic_network_managed_t *m, char *s)
{
    if (s != NULL)
        m->alloc.free(s, strlen(s) + 1, m->alloc.ctx);
}

/* --- terminal latching (under mu; fatal beats closed) ----------------------- */

static void nwm_snapshot_terr(moq_wtquic_network_managed_t *m, wtq_session_t *s)
{
    if (s == NULL)
        return;
    wtq_transport_error_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.struct_size = (uint32_t)sizeof(rec);
    if (wtq_session_transport_error(s, &rec) != WTQ_OK)
        return;
    /* published under mu: the off-domain snapshot readers take the
     * same lock, and the first capture wins (first-causal, like the
     * record wtquic itself seals) */
    pthread_mutex_lock(&m->mu);
    if (!m->terr_set) {
        m->terr = rec;
        m->terr_set = true;
    }
    pthread_mutex_unlock(&m->mu);
}

static void nwm_latch_fatal(moq_wtquic_network_managed_t *m, uint64_t code)
{
    pthread_mutex_lock(&m->mu);
    if (!m->fatal) {
        m->fatal = true;
        m->fatal_code = code;
    }
    m->pumps++;
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
}

static void nwm_latch_closed(moq_wtquic_network_managed_t *m, uint64_t code)
{
    pthread_mutex_lock(&m->mu);
    if (!m->fatal && !m->closed) {
        m->closed = true;
        m->close_code = code;
    }
    m->pumps++;
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
}

static bool nwm_terminal(moq_wtquic_network_managed_t *m)
{
    pthread_mutex_lock(&m->mu);
    bool t = m->fatal || m->closed || m->pump_exit || m->stop_started;
    pthread_mutex_unlock(&m->mu);
    return t;
}

/*
 * The backend handle, read under mu. create() hands &m->conn to
 * wtq_nw_conn_create, which publishes it (NULL, then the handle) BEFORE
 * starting the connection, all while create() holds mu — so a domain
 * callback that runs before create() returns still finds the handle
 * published once it takes the lock. Every reader takes mu for that
 * ordering edge. The handle itself never changes once published
 * (released in destroy, after join).
 */
static wtq_nw_conn_t *nwm_conn(const moq_wtquic_network_managed_t *m)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    wtq_nw_conn_t *conn = mm->conn;
    pthread_mutex_unlock(&mm->mu);
    return conn;
}

/* --- white-box test seam (MOQ_WTQ_MM_TESTING only; never shipped) ------------ */
#ifdef MOQ_WTQ_MM_TESTING
#include "tests/wtquic_network_managed_test_internal.h"

static void nwm_pump(moq_wtquic_network_managed_t *m);  /* fwd */

static moq_wtquic_nwm_op_recorder_fn g_nwm_rec;
static void *g_nwm_rec_ctx;
static moq_wtquic_network_managed_t *g_nwm_test_m;

void moq_wtquic_network_managed_test_set_op_recorder(
    moq_wtquic_nwm_op_recorder_fn fn, void *ctx)
{
    g_nwm_rec = fn;
    g_nwm_rec_ctx = ctx;
}

static void nwm_rec(moq_wtquic_nwm_op_t op, uint64_t arg)
{
    if (g_nwm_rec != NULL)
        g_nwm_rec(g_nwm_rec_ctx, op, arg);
}

/* Stubs standing in for the transport + adapter primitives so the deadline
 * state machine runs with no real connection. conn_service records SERVICE
 * and drives the between-pass hook (the pump), exactly as the real service
 * cycle does. */
static void nwm_stub_service(moq_wtquic_conn_t *mc)
{
    (void)mc;
    nwm_rec(MOQ_WTQ_NWM_OP_SERVICE, 0);
    if (g_nwm_test_m != NULL)
        nwm_pump(g_nwm_test_m);
}
static bool nwm_stub_is_fatal(const moq_wtquic_conn_t *mc) { (void)mc; return false; }
static bool nwm_stub_is_closed(const moq_wtquic_conn_t *mc) { (void)mc; return false; }
static wtq_result_t nwm_stub_ring_after(wtq_nw_conn_t *c, uint64_t d)
{ (void)c; nwm_rec(MOQ_WTQ_NWM_OP_RING, d); return WTQ_OK; }
static void nwm_stub_cancel_after(wtq_nw_conn_t *c)
{ (void)c; nwm_rec(MOQ_WTQ_NWM_OP_CANCEL, 0); }

/* The synthetic facade's on_lane_pump: records that the app pump ran. */
static int nwm_test_pump_rec(moq_wtquic_network_managed_t *m,
                             moq_wtquic_network_managed_lane_t *lane,
                             uint64_t now, void *ctx)
{ (void)m; (void)lane; (void)now; (void)ctx; nwm_rec(MOQ_WTQ_NWM_OP_PUMP, 0); return 0; }

#define moq_wtquic_conn_service            nwm_stub_service
#define moq_wtquic_conn_is_fatal           nwm_stub_is_fatal
#define moq_wtquic_conn_is_closed          nwm_stub_is_closed
#define wtq_nw_conn_doorbell_ring_after    nwm_stub_ring_after
#define wtq_nw_conn_doorbell_cancel_after  nwm_stub_cancel_after
#endif /* MOQ_WTQ_MM_TESTING */

/* --- the pump (adapter hook + wake target), on-domain ------------------------ */

/* Recompute the combined session+app deadline and (re)arm or cancel the
 * native delayed doorbell. ON-DOMAIN only. Called at the end of nwm_pump,
 * which normally runs as conn_service's BETWEEN-PASS hook: the first
 * service pass has already ticked any due session deadline to the future
 * (transport_bridge.c ticks when dl <= now), and the hook has just updated
 * app state via on_lane_pump — so moq_session_next_deadline_us and the app
 * query both read post-advance values here, and neither can leave a
 * past-due deadline that would self-spin the loop. (The second service
 * pass then flushes whatever the hook queued.) On the not-due doorbell
 * path nwm_pump also runs once explicitly before conn_service; the hook
 * invocation inside conn_service re-arms last, so the settled arm wins.
 * A recompute REPLACES the previous arm (the doorbell has one slot). */
static void nwm_rearm_deadline(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m->conn;
    if (conn == NULL)
        return;

    /* Terminal / teardown latched: never consult the application deadline
     * again and drop any pending arm. A generic app callback keeps its own
     * cadence and may return a finite / already-due deadline past terminal
     * (the adapter cannot rely on the app clearing it); without this guard a
     * terminal pump would rearm the doorbell and spin until stop_begin(). */
    if (nwm_terminal(m)) {
        m->wake_deadline_us = UINT64_MAX;
        wtq_nw_conn_doorbell_cancel_after(conn);
        return;
    }

    uint64_t deadline = m->ms != NULL ? moq_session_next_deadline_us(m->ms)
                                      : UINT64_MAX;
    if (m->app_deadline_us != NULL) {
        uint64_t ad = m->app_deadline_us(m->app_deadline_ctx);
        if (ad < deadline)
            deadline = ad;
    }
    m->wake_deadline_us = deadline;

    if (deadline == UINT64_MAX) {
        wtq_nw_conn_doorbell_cancel_after(conn);
        return;
    }

    uint64_t now = nwm_now_us();
    uint64_t delay = deadline > now ? deadline - now : 0;
    wtq_result_t rc = wtq_nw_conn_doorbell_ring_after(conn, delay);
    if (rc == WTQ_OK)
        return;  /* armed */
    if (rc == WTQ_ERR_CLOSED) {
        m->wake_deadline_us = UINT64_MAX;  /* stop latched: no arm exists */
        return;
    }
    /* WTQ_ERR_INVALID_ARG / WTQ_ERR_UNSUPPORTED are contract violations
     * (this facade always configures on_doorbell and holds a live conn),
     * not transient states — take the terminal path rather than leave a
     * deadline that never arms and never spin retrying it. */
    nwm_latch_fatal(m, 0);
    (void)moq_wtquic_network_managed_stop_begin(m);
}

static void nwm_pump(moq_wtquic_network_managed_t *m)
{
    int rc = 0;

    /* the lane window opens only once the negotiated session + attach
     * adapter exist — on_lane_pump never fires pre-establishment */
    if (m->on_lane_pump != NULL && m->mc != NULL) {
        m->in_lane_pump = true; /* the exclusive lane access window */
        rc = m->on_lane_pump(m, &m->lane, nwm_now_us(),
                             m->on_lane_pump_user);
        m->in_lane_pump = false; /* closed before on_activity — it is
                                    notification-only, not a window */
    }
    if (m->on_activity != NULL)
        m->on_activity(m, m->on_activity_ctx);

    /* mirror the attach loop's terminal observation */
    if (m->mc != NULL) {
        if (moq_wtquic_conn_is_fatal(m->mc))
            nwm_latch_fatal(m, 0);
        else if (moq_wtquic_conn_is_closed(m->mc))
            nwm_latch_closed(m, m->close_code);
    }

    pthread_mutex_lock(&m->mu);
    m->pumps++;
    m->activity_pending = true;
    bool want_stop = false;
    if (rc != 0 && !m->pump_exit) {
        m->pump_exit = true;
        want_stop = true;
    }
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
    if (want_stop) {
        (void)moq_wtquic_network_managed_stop_begin(m);
        return;
    }
    /* end of this pump: as conn_service's between-pass hook, the first
     * pass already advanced due session state and on_lane_pump just ran,
     * so fold the (post-advance) session + app deadlines and (re)arm the
     * native delayed doorbell (replaces any prior arm) */
    nwm_rearm_deadline(m);
}

static void nwm_hook(moq_wtquic_conn_t *conn, void *user)
{
    (void)conn;
    nwm_pump(user);
}

/* The doorbell delivery: the wake-initiated pump. Runs OUTSIDE the
 * adapter's service cycle (unlike the hook-invoked pump, where the
 * adapter services again after the hook returns), so it flushes what
 * the pump queued — or a control message written here would sit until
 * the next transport event. Reentrancy-safe by the adapter's
 * coalescing. Delivery is COALESCED by the wtquic doorbell: rings
 * between deliveries collapse into one pass; a ring during this pass
 * re-arms exactly one more. */
static void nwm_doorbell(void *ctx)
{
    moq_wtquic_network_managed_t *m = ctx;

    /* Derive `due` from the cached ABSOLUTE deadline (no permanent flag):
     * either the delayed arm fired at/after its deadline, or an ordinary
     * immediate ring landed after it. */
    uint64_t now = nwm_now_us();
    bool due = m->wake_deadline_us != UINT64_MAX && now >= m->wake_deadline_us;

    if (due && m->mc != NULL) {
        /* Time-based work is ready: SERVICE FIRST. conn_service's first
         * pass ticks the session's due deadline, its between-pass hook
         * runs the app pump (which arms the next deadline), and its second
         * pass flushes — so no explicit pump here. */
        moq_wtquic_conn_service(m->mc);
    } else {
        /* Ordinary transport wake before the deadline: pump then flush
         * (the pre-existing behavior). This does NOT consume the delayed
         * arm — nwm_pump's rearm recomputes the still-pending deadline. */
        nwm_pump(m);
        if (m->mc != NULL)
            moq_wtquic_conn_service(m->mc);
    }
}

/* --- negotiation + establishment (on-domain) --------------------------------- */

static bool nwm_token_offered(const moq_wtquic_network_managed_t *m,
                              const char *tok, size_t len)
{
    for (size_t i = 0; i < m->offer_count; i++)
        if (m->offer_len[i] == len &&
            memcmp(m->offers[i], tok, len) == 0)
            return true;
    return false;
}

static void nwm_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    moq_wtquic_network_managed_t *m = user;
    moq_version_t version = 0;

    if (sub.len == 0) {
        /*
         * No WT-Protocol token. LEGACY FALLBACK is draft-16 and only
         * legitimate when draft-16 was actually on the table: either
         * no offer was configured (pure legacy peer) or the offer
         * included moqt-16. An exact draft-18 offer is NEVER silently
         * downgraded.
         */
        if (m->offer_count == 0 ||
            nwm_token_offered(m, "moqt-16", 7)) {
            version = MOQ_VERSION_DRAFT_16;
        } else {
            nwm_latch_fatal(m, 0);
            (void)wtq_session_close(s, 0, NULL, 0);
            return;
        }
    } else {
        /* the selected token must be RECOGNIZED and must have been
         * OFFERED (byte-exact) — a server inventing tokens is fatal */
        if (!moq_alpn_to_version(sub.data, sub.len, &version) ||
            !nwm_token_offered(m, sub.data, sub.len)) {
            nwm_latch_fatal(m, 0);
            (void)wtq_session_close(s, 0, NULL, 0);
            return;
        }
    }

    /* create the MoQ session with the NEGOTIATED version, then attach */
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &m->alloc,
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = version;
    scfg.send_request_capacity = m->send_request_capacity;
    if (m->initial_request_capacity != 0)
        scfg.initial_request_capacity = m->initial_request_capacity;
    moq_session_t *ms = NULL;
    if (moq_session_create(&scfg, nwm_now_us(), &ms) < 0) {
        nwm_latch_fatal(m, 0);
        (void)wtq_session_close(s, 0, NULL, 0);
        return;
    }
    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &m->alloc;
    ccfg.session = ms;
    ccfg.hook = nwm_hook;
    ccfg.hook_user = m;
    moq_wtquic_conn_t *mc = NULL;
    if (moq_wtquic_conn_create(&ccfg, &mc) < 0) {
        moq_session_destroy(ms);
        nwm_latch_fatal(m, 0);
        (void)wtq_session_close(s, 0, NULL, 0);
        return;
    }
    m->ms = ms;
    m->mc = mc;
    pthread_mutex_lock(&m->mu);
    m->version = version;
    pthread_mutex_unlock(&m->mu);

    /* forward establishment into the adapter (it drives the bridge
     * SETUP and then the hook — the first pump) */
    m->fwd->on_established(s, sub, mc);
}

/* --- pre/post-establishment event proxies (on-domain) ------------------------ */

static void nwm_refused(wtq_session_t *s, uint16_t status, void *user)
{
    moq_wtquic_network_managed_t *m = user;

    nwm_snapshot_terr(m, s);
    if (m->mc != NULL) {
        m->fwd->on_refused(s, status, m->mc);
        nwm_latch_fatal(m, 0);
        return;
    }
    nwm_latch_fatal(m, (uint64_t)status);
}

static void nwm_failed(wtq_session_t *s, wtq_connect_failure_t why,
                       void *user)
{
    moq_wtquic_network_managed_t *m = user;

    nwm_snapshot_terr(m, s);
    if (m->mc != NULL) {
        m->fwd->on_failed(s, why, m->mc);
        nwm_latch_fatal(m, 0);
        return;
    }
    nwm_latch_fatal(m, 0);
}

static void nwm_draining(wtq_session_t *s, void *user)
{
    moq_wtquic_network_managed_t *m = user;

    if (m->mc != NULL && m->fwd->on_draining != NULL)
        m->fwd->on_draining(s, m->mc);
}

static void nwm_closed(wtq_session_t *s, uint32_t code,
                       const uint8_t *reason, size_t reason_len,
                       bool clean, void *user)
{
    moq_wtquic_network_managed_t *m = user;

    nwm_snapshot_terr(m, s);
    if (m->mc != NULL) {
        /* the adapter drives the bridge close; the hook after it
         * latches the facade terminal from the bridge state */
        pthread_mutex_lock(&m->mu);
        if (!m->closed)
            m->close_code = code;
        pthread_mutex_unlock(&m->mu);
        m->fwd->on_closed(s, code, reason, reason_len, clean, m->mc);
        if (clean)
            nwm_latch_closed(m, code);
        else
            nwm_latch_fatal(m, code);
        return;
    }
    /* pre-establishment close: terminal WITHOUT a MoQ session */
    if (clean)
        nwm_latch_closed(m, code);
    else
        nwm_latch_fatal(m, code);
}

#define NWM_FWD(name, ...)                                              \
    do {                                                                \
        moq_wtquic_network_managed_t *m = user;                              \
        if (m->mc != NULL && m->fwd->name != NULL)                      \
            m->fwd->name(__VA_ARGS__, m->mc);                           \
    } while (0)

static void nwm_stream_opened(wtq_session_t *s, wtq_stream_t *st,
                              bool bidi, void *user)
{
    NWM_FWD(on_stream_opened, s, st, bidi);
}

static void nwm_stream_data(wtq_session_t *s, wtq_stream_t *st,
                            const uint8_t *data, size_t len, bool fin,
                            void *user)
{
    NWM_FWD(on_stream_data, s, st, data, len, fin);
}

static void nwm_stream_reset(wtq_session_t *s, wtq_stream_t *st,
                             uint32_t code, void *user)
{
    NWM_FWD(on_stream_reset, s, st, code);
}

static void nwm_stream_stop(wtq_session_t *s, wtq_stream_t *st,
                            uint32_t code, void *user)
{
    NWM_FWD(on_stream_stop, s, st, code);
}

static void nwm_stream_closed(wtq_session_t *s, wtq_stream_t *st,
                              void *user)
{
    NWM_FWD(on_stream_closed, s, st);
}

static void nwm_send_complete(wtq_session_t *s, void *send_ctx,
                              bool canceled, void *user)
{
    NWM_FWD(on_send_complete, s, send_ctx, canceled);
}

static void nwm_datagram(wtq_session_t *s, const uint8_t *data,
                         size_t len, void *user)
{
    NWM_FWD(on_datagram, s, data, len);
}

static void nwm_stream_writable(wtq_session_t *s, wtq_stream_t *st,
                                void *user)
{
    NWM_FWD(on_stream_writable, s, st);
}

/* --- final teardown (on-domain, the backend's LAST block) -------------------- */

static void nwm_on_stopped(void *ctx)
{
    moq_wtquic_network_managed_t *m = ctx;

    /* adapter first, then the owned session (the session must outlive
     * the conn) — ON-DOMAIN, before completion is observable */
    if (m->mc != NULL) {
        moq_wtquic_conn_destroy(m->mc);
        m->mc = NULL;
    }
    if (m->ms != NULL) {
        moq_session_destroy(m->ms);
        m->ms = NULL;
    }
    /* snapshot the continuation BEFORE completion becomes observable:
     * once torn_down broadcasts, a joiner may destroy the facade */
    void (*cb)(void *) = m->on_stopped;
    void *cb_ctx = m->on_stopped_ctx;
    pthread_mutex_lock(&m->mu);
    m->torn_down = true;
    m->pumps++;
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
    /* LAST facade access: the continuation may resume an actor that
     * destroys the facade immediately — nothing here touches m after
     * this call (or after the unlock above, if cb is NULL) */
    if (cb != NULL)
        cb(cb_ctx);
}

/* --- public API --------------------------------------------------------------- */

void moq_wtquic_network_managed_cfg_init(moq_wtquic_network_managed_cfg_t *cfg)
{
    if (cfg == NULL)
        return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

void moq_wtquic_network_managed_cfg_init_sized(
    moq_wtquic_network_managed_cfg_t *cfg, size_t cfg_size)
{
    if (cfg == NULL)
        return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size))   /* too small to even stamp */
        return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

moq_result_t moq_wtquic_network_managed_create(
    const moq_wtquic_network_managed_cfg_t *cfg, moq_wtquic_network_managed_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
#define NWM_CFG_HAS(field)                                              \
    ((size_t)cfg->struct_size >=                                        \
     offsetof(moq_wtquic_network_managed_cfg_t, field) + sizeof(cfg->field))
    const size_t min_size =
        offsetof(moq_wtquic_network_managed_cfg_t, insecure_skip_verify) +
        sizeof(((const moq_wtquic_network_managed_cfg_t *)0)
                   ->insecure_skip_verify);
    if (cfg == NULL || (size_t)cfg->struct_size < min_size ||
        cfg->alloc == NULL || cfg->alloc->alloc == NULL ||
        cfg->alloc->free == NULL || cfg->alloc->realloc == NULL ||
        cfg->host == NULL || cfg->port <= 0 || cfg->port > 65535)
        return MOQ_ERR_INVAL;

    /* CLIENT-ONLY, SINGLE-LANE — explicit, not silently ignored. A
     * server perspective needs the future MsQuic-backed wtquic facade
     * and more than one lane would need more than one connection:
     * both are well-formed requests this facade cannot serve
     * (MOQ_ERR_UNSUPPORTED, the mvfst single-lane precedent); a
     * malformed perspective value is MOQ_ERR_INVAL. */
    if (NWM_CFG_HAS(perspective)) {
        if (cfg->perspective == MOQ_PERSPECTIVE_SERVER)
            return MOQ_ERR_UNSUPPORTED;
        if (cfg->perspective != 0 &&
            cfg->perspective != MOQ_PERSPECTIVE_CLIENT)
            return MOQ_ERR_INVAL;
    }
    if (NWM_CFG_HAS(lane_count) && cfg->lane_count > 1)
        return MOQ_ERR_UNSUPPORTED;

    moq_wtquic_network_managed_t *m =
        cfg->alloc->alloc(sizeof(*m), cfg->alloc->ctx);
    if (m == NULL)
        return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *cfg->alloc;
    m->wake_deadline_us = UINT64_MAX;  /* no deadline armed yet */
    m->lane.owner = m;
    m->cview.owner = m;
    pthread_mutex_init(&m->mu, NULL);
    pthread_cond_init(&m->cv, NULL);
    m->fwd = moq_wtquic_conn_events();
    /* every optional tail field gates on ITS OWN complete fit — a
     * struct that ends between a callback and its context yields the
     * callback with a NULL context, never a read past the caller's
     * struct */
    if (NWM_CFG_HAS(on_lane_pump))
        m->on_lane_pump = cfg->on_lane_pump;
    if (NWM_CFG_HAS(on_lane_pump_user))
        m->on_lane_pump_user = cfg->on_lane_pump_user;
    if (NWM_CFG_HAS(on_activity))
        m->on_activity = cfg->on_activity;
    if (NWM_CFG_HAS(on_activity_ctx))
        m->on_activity_ctx = cfg->on_activity_ctx;
    if (NWM_CFG_HAS(send_request_capacity))
        m->send_request_capacity = cfg->send_request_capacity;
    if (NWM_CFG_HAS(initial_request_capacity))
        m->initial_request_capacity = cfg->initial_request_capacity;
    if (NWM_CFG_HAS(on_stopped))
        m->on_stopped = cfg->on_stopped;
    if (NWM_CFG_HAS(on_stopped_ctx))
        m->on_stopped_ctx = cfg->on_stopped_ctx;
    /* app_deadline_us + app_deadline_ctx are ONE ABI block: gated on the
     * LAST field's fit, so the pair is read together or not at all —
     * never the callback with a context read past the caller's struct. */
    if (NWM_CFG_HAS(app_deadline_ctx)) {
        m->app_deadline_us = cfg->app_deadline_us;
        m->app_deadline_ctx = cfg->app_deadline_ctx;
    }
#undef NWM_CFG_HAS

    m->host = nwm_strdup(m, cfg->host);
    m->path = nwm_strdup(m, cfg->path != NULL ? cfg->path : "/moq");
    m->authority = nwm_strdup(
        m, cfg->authority != NULL ? cfg->authority : cfg->host);
    if (m->host == NULL || m->path == NULL || m->authority == NULL)
        goto oom;

    if (cfg->wt_protocols != NULL) {
        m->protos_buf = nwm_strdup(m, cfg->wt_protocols);
        if (m->protos_buf == NULL)
            goto oom;
        m->protos_buf_size = strlen(cfg->wt_protocols) + 1;
        /*
         * STRICT list validation, before anything dials: tokens
         * separated by single commas (optional spaces around them),
         * every token a RECOGNIZED MoQ WT-Protocol, no empties, no
         * duplicates, and never more than the offer capacity — a
         * malformed or overlong offer is an error, not a silently
         * weakened negotiation policy.
         */
        char *p = m->protos_buf;
        for (;;) {
            while (*p == ' ')
                p++;
            char *tok = p;
            while (*p != '\0' && *p != ',' && *p != ' ')
                p++;
            size_t len = (size_t)(p - tok);
            moq_version_t v;
            if (len == 0 || !moq_alpn_to_version(tok, len, &v))
                goto inval;
            for (size_t i = 0; i < m->offer_count; i++)
                if (m->offer_len[i] == len &&
                    memcmp(m->offers[i], tok, len) == 0)
                    goto inval;
            if (m->offer_count == NW_MAX_OFFERS)
                goto inval;
            /* classify what follows BEFORE terminating the token */
            char *q = p;
            while (*q == ' ')
                q++;
            bool more = false;
            if (*q == ',') {
                more = true;
                q++;
            } else if (*q != '\0') {
                goto inval; /* bare space inside the list */
            }
            tok[len] = '\0';
            m->offers[m->offer_count] = tok;
            m->offer_len[m->offer_count] = len;
            m->offer_count++;
            if (!more)
                break;
            p = q;
        }
    }

    wtq_connect_config_t connect = WTQ_CONNECT_CONFIG_INIT;
    connect.authority = m->authority;
    connect.path = m->path;
    connect.subprotocols = m->offer_count > 0 ? m->offers : NULL;
    connect.subprotocol_count = m->offer_count;
    connect.require_subprotocol = false; /* legacy fallback stays legal;
                                            negotiation is validated at
                                            establishment */

    wtq_session_events_t ev;
    wtq_session_events_init(&ev);
    ev.on_established = nwm_established;
    ev.on_refused = nwm_refused;
    ev.on_failed = nwm_failed;
    ev.on_draining = nwm_draining;
    ev.on_closed = nwm_closed;
    ev.on_stream_opened = nwm_stream_opened;
    ev.on_stream_data = nwm_stream_data;
    ev.on_stream_reset = nwm_stream_reset;
    ev.on_stream_stop = nwm_stream_stop;
    ev.on_stream_closed = nwm_stream_closed;
    ev.on_send_complete = nwm_send_complete;
    ev.on_datagram = nwm_datagram;
    ev.on_stream_writable = nwm_stream_writable;

    wtq_nw_conn_cfg_t ncfg = WTQ_NW_CONN_CFG_INIT;
    ncfg.server_name = m->host;
    ncfg.port = (uint16_t)cfg->port;
    ncfg.insecure_skip_verify = cfg->insecure_skip_verify;
    ncfg.connect = &connect;
    ncfg.events = &ev;
    ncfg.user = m;
    ncfg.on_stopped = nwm_on_stopped;
    ncfg.stopped_ctx = m;
    /* the preallocated coalescing doorbell: wake() rings it without
     * allocating and without a failure path */
    ncfg.on_doorbell = nwm_doorbell;
    ncfg.doorbell_ctx = m;
    /*
     * Publish the handle into m->conn BEFORE the backend starts its
     * domain callbacks. wtq_nw_conn_create writes *conn_out (NULL, then
     * the handle on success) BEFORE nw_connection_group_start, so passing
     * &m->conn hands the callback-visible slot to wtquic directly rather
     * than storing it only after create() returns. Holding m->mu across
     * the call supplies the ordering edge every other reader already
     * takes: an earliest callback that stops/wakes/posts (or whose pump
     * latches pump_exit and calls stop_begin) blocks on the lock, then
     * observes the published handle instead of a NULL that would leave a
     * live transport running behind a terminal facade.
     */
    pthread_mutex_lock(&m->mu);
    wtq_result_t wrc = wtq_nw_conn_create(&ncfg, &m->conn);
    pthread_mutex_unlock(&m->mu);
    if (wrc != WTQ_OK) {
        moq_result_t rc = wrc == WTQ_ERR_NOMEM ? MOQ_ERR_NOMEM
                                               : MOQ_ERR_INTERNAL;
        moq_wtquic_network_managed_destroy(m);
        return rc;
    }
    *out = m;
    return MOQ_OK;
oom:
    moq_wtquic_network_managed_destroy(m);
    return MOQ_ERR_NOMEM;
inval:
    moq_wtquic_network_managed_destroy(m);
    return MOQ_ERR_INVAL;
}

bool moq_wtquic_network_managed_stop_begin(moq_wtquic_network_managed_t *m)
{
    if (m == NULL)
        return false;
    pthread_mutex_lock(&m->mu);
    wtq_nw_conn_t *conn = m->conn;
    if (conn != NULL) {
        m->stop_started = true;
        pthread_cond_broadcast(&m->cv);
    }
    pthread_mutex_unlock(&m->mu);
    if (conn == NULL)
        return false;
    /* Drop any pending delayed doorbell arm before latching stop. Idempotent
     * and nonblocking (safe from any thread, including the domain); the
     * facade owns no timer thread to join. */
    wtq_nw_conn_doorbell_cancel_after(conn);
    return wtq_nw_conn_stop_begin(conn);
}

moq_result_t moq_wtquic_network_managed_join(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);
    if (conn == NULL)
        return MOQ_ERR_INVAL;
    if (wtq_nw_conn_is_on_domain(conn))
        return MOQ_ERR_WRONG_STATE;
    /* the backend's join waits for on_stopped — which is where the
     * facade tore down the adapter/session — then confirm locally */
    wtq_result_t rc = wtq_nw_conn_join(conn);
    if (rc != WTQ_OK)
        return MOQ_ERR_WRONG_STATE;
    pthread_mutex_lock(&m->mu);
    while (!m->torn_down)
        pthread_cond_wait(&m->cv, &m->mu);
    pthread_mutex_unlock(&m->mu);
    return MOQ_OK;
}

moq_result_t moq_wtquic_network_managed_stop(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);
    if (conn == NULL)
        return MOQ_ERR_INVAL;
    if (wtq_nw_conn_is_on_domain(conn))
        return MOQ_ERR_WRONG_STATE;
    (void)moq_wtquic_network_managed_stop_begin(m);
    return moq_wtquic_network_managed_join(m);
}

void moq_wtquic_network_managed_destroy(moq_wtquic_network_managed_t *m)
{
    if (m == NULL)
        return;
    wtq_nw_conn_t *conn = nwm_conn(m);
    if (conn != NULL)
        wtq_nw_conn_release(conn);
    nwm_free_str(m, m->host);
    nwm_free_str(m, m->path);
    nwm_free_str(m, m->authority);
    if (m->protos_buf != NULL)
        m->alloc.free(m->protos_buf, m->protos_buf_size, m->alloc.ctx);
    pthread_mutex_destroy(&m->mu);
    pthread_cond_destroy(&m->cv);
    moq_alloc_t alloc = m->alloc;
    alloc.free(m, sizeof(*m), alloc.ctx);
}

moq_session_t *moq_wtquic_network_managed_session(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);

    if (conn == NULL || !wtq_nw_conn_is_on_domain(conn))
        return NULL;
    return m->ms;
}

moq_wtquic_conn_t *moq_wtquic_network_managed_adapter(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);

    if (conn == NULL || !wtq_nw_conn_is_on_domain(conn))
        return NULL;
    return m->mc;
}

/* --- the lane interface (one lane, one connection) --------------------------- */

/* True only on the domain AND inside this facade's on_lane_pump. The
 * in_lane_pump read is domain-confined: it happens only after the
 * domain check confirmed this thread IS the domain. */
static bool nwm_in_pump(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);

    return conn != NULL && wtq_nw_conn_is_on_domain(conn) &&
           m->in_lane_pump;
}

uint32_t moq_wtquic_network_managed_lane_count(const moq_wtquic_network_managed_t *m)
{
    return m == NULL ? 0 : 1;
}

moq_wtquic_network_managed_lane_t *moq_wtquic_network_managed_lane(
    moq_wtquic_network_managed_t *m, uint32_t index)
{
    if (m == NULL || index != 0)
        return NULL;
    return &m->lane;
}

uint32_t moq_wtquic_network_lane_index(const moq_wtquic_network_managed_lane_t *lane)
{
    (void)lane;
    return 0;
}

moq_wtquic_network_managed_conn_t *moq_wtquic_network_lane_next_conn(
    moq_wtquic_network_managed_lane_t *lane, moq_wtquic_network_managed_conn_t *prev)
{
    if (lane == NULL || !nwm_in_pump(lane->owner))
        return NULL;
    if (prev != NULL)
        return NULL; /* one connection: iteration ends after it */
    /* inside the window the adapter always exists (the window opens
     * only post-establishment); a terminal connection stays iterable
     * until final teardown so the pump that observes the terminal can
     * still poll its final events */
    return lane->owner->mc != NULL ? &lane->owner->cview : NULL;
}

moq_result_t moq_wtquic_network_lane_wake(moq_wtquic_network_managed_lane_t *lane)
{
    if (lane == NULL)
        return MOQ_ERR_INVAL;
    /* exactly managed_wake's contract (one lane): deferred, coalesced
     * delivery — never inline, so a wake from inside the current pump
     * schedules another pass without re-entering — and an MOQ_OK
     * racing stop_begin may be absorbed by teardown */
    return moq_wtquic_network_managed_wake(lane->owner);
}

moq_session_t *moq_wtquic_network_managed_conn_session(
    moq_wtquic_network_managed_conn_t *conn)
{
    if (conn == NULL || !nwm_in_pump(conn->owner))
        return NULL;
    return conn->owner->ms;
}

moq_wtquic_conn_t *moq_wtquic_network_managed_conn_adapter(
    moq_wtquic_network_managed_conn_t *conn)
{
    if (conn == NULL || !nwm_in_pump(conn->owner))
        return NULL;
    return conn->owner->mc;
}

moq_wtquic_network_managed_lane_t *moq_wtquic_network_managed_conn_lane(
    moq_wtquic_network_managed_conn_t *conn)
{
    if (conn == NULL || !nwm_in_pump(conn->owner))
        return NULL;
    return &conn->owner->lane;
}

moq_version_t moq_wtquic_network_managed_negotiated_version(
    const moq_wtquic_network_managed_t *m)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    if (mm == NULL)
        return 0;
    pthread_mutex_lock(&mm->mu);
    moq_version_t v = mm->version;
    pthread_mutex_unlock(&mm->mu);
    return v;
}

bool moq_wtquic_network_managed_is_fatal(const moq_wtquic_network_managed_t *m)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    if (mm == NULL)
        return false;
    pthread_mutex_lock(&mm->mu);
    bool f = mm->fatal;
    pthread_mutex_unlock(&mm->mu);
    return f;
}

uint64_t moq_wtquic_network_managed_fatal_code(const moq_wtquic_network_managed_t *m)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    if (mm == NULL)
        return 0;
    pthread_mutex_lock(&mm->mu);
    uint64_t c = mm->fatal_code;
    pthread_mutex_unlock(&mm->mu);
    return c;
}

bool moq_wtquic_network_managed_is_closed(const moq_wtquic_network_managed_t *m)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    if (mm == NULL)
        return false;
    pthread_mutex_lock(&mm->mu);
    bool c = mm->closed && !mm->fatal;
    pthread_mutex_unlock(&mm->mu);
    return c;
}

uint64_t moq_wtquic_network_managed_close_code(const moq_wtquic_network_managed_t *m)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    if (mm == NULL)
        return 0;
    pthread_mutex_lock(&mm->mu);
    uint64_t c = mm->close_code;
    pthread_mutex_unlock(&mm->mu);
    return c;
}

bool moq_wtquic_network_managed_transport_error(
    const moq_wtquic_network_managed_t *m, wtq_transport_error_t *out)
{
    moq_wtquic_network_managed_t *mm = (moq_wtquic_network_managed_t *)m;
    if (mm == NULL || out == NULL ||
        out->struct_size < offsetof(wtq_transport_error_t, kind) +
                               sizeof(out->kind))
        return false;
    /* caller-sized copy-out, the same discipline as wtquic itself:
     * fill only the bytes that fit, preserve the caller's struct_size,
     * leave trailing fields untouched */
    uint32_t caller_size = out->struct_size;
    size_t n = caller_size < sizeof(mm->terr) ? caller_size
                                              : sizeof(mm->terr);
    pthread_mutex_lock(&mm->mu);
    bool set = mm->terr_set;
    if (set)
        memcpy(out, &mm->terr, n);
    pthread_mutex_unlock(&mm->mu);
    if (set)
        out->struct_size = caller_size;
    return set;
}

moq_result_t moq_wtquic_network_managed_post(moq_wtquic_network_managed_t *m,
                                        void (*fn)(void *ctx), void *ctx)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);

    if (conn == NULL || fn == NULL)
        return MOQ_ERR_INVAL;
    /* straight onto the backend post: submission order, deferred
     * on-domain execution, accepted-runs-once / rejected-never are
     * the backend's own guarantees — nothing is layered on top */
    wtq_result_t rc = wtq_nw_conn_post(conn, fn, ctx);
    if (rc == WTQ_OK)
        return MOQ_OK;
    return rc == WTQ_ERR_CLOSED ? MOQ_ERR_CLOSED : MOQ_ERR_NOMEM;
}

moq_result_t moq_wtquic_network_managed_wake(moq_wtquic_network_managed_t *m)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);

    if (conn == NULL)
        return MOQ_ERR_INVAL;
    if (nwm_terminal(m))
        return MOQ_ERR_CLOSED;
    /* The doorbell ring is a void, stop-aware operation: non-allocating
     * and infallible to call, but NOT a delivery receipt. The terminal
     * check above makes MOQ_ERR_CLOSED mean "stop/terminal observed
     * before the ring was attempted"; an MOQ_OK that RACES stop_begin
     * may still be absorbed by teardown (the ring no-ops once stop wins
     * the doorbell latch). Without a concurrent stop, MOQ_OK guarantees
     * at least one deferred, coalesced pump pass. */
    wtq_nw_conn_doorbell_ring(conn);
    return MOQ_OK;
}

moq_result_t moq_wtquic_network_managed_wait(moq_wtquic_network_managed_t *m,
                                        uint64_t timeout_us)
{
    wtq_nw_conn_t *conn = m == NULL ? NULL : nwm_conn(m);

    if (conn == NULL)
        return MOQ_ERR_INVAL;
    if (wtq_nw_conn_is_on_domain(conn))
        return MOQ_ERR_WRONG_STATE;
    /* a REQUESTED stop is not completion: only a session outcome or
     * COMPLETED teardown (torn_down) reads as terminal, so a CLOSED
     * result after a stop means the facade is safe to destroy */
    pthread_mutex_lock(&m->mu);
    if (m->fatal || m->closed || m->pump_exit || m->torn_down) {
        pthread_mutex_unlock(&m->mu);
        return MOQ_ERR_CLOSED;
    }
    /* LEVEL-RETAINED, COALESCED activity: a pump that landed before
     * this call — even long before — satisfies the wait immediately,
     * and any number of pumps since the last consuming wait read as
     * ONE level (drain-until-idle semantics, not one token per pump).
     * Only when no activity is pending does the wait block. */
    moq_result_t rc = MOQ_DONE;
    if (m->activity_pending) {
        m->activity_pending = false;
        pthread_mutex_unlock(&m->mu);
        return MOQ_OK;
    }
    if (timeout_us == UINT64_MAX) {
        while (!m->activity_pending && !m->fatal && !m->closed &&
               !m->pump_exit && !m->torn_down)
            pthread_cond_wait(&m->cv, &m->mu);
    } else if (timeout_us > 0) {
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_sec += (time_t)(timeout_us / 1000000u);
        dl.tv_nsec += (long)(timeout_us % 1000000u) * 1000L;
        if (dl.tv_nsec >= 1000000000L) {
            dl.tv_sec++;
            dl.tv_nsec -= 1000000000L;
        }
        while (!m->activity_pending && !m->fatal && !m->closed &&
               !m->pump_exit && !m->torn_down)
            if (pthread_cond_timedwait(&m->cv, &m->mu, &dl) == ETIMEDOUT)
                break;
    }
    if (m->fatal || m->closed || m->pump_exit || m->torn_down)
        rc = MOQ_ERR_CLOSED;
    else if (m->activity_pending) {
        m->activity_pending = false;
        rc = MOQ_OK;
    }
    pthread_mutex_unlock(&m->mu);
    return rc;
}

/* --- white-box test seam implementations (MOQ_WTQ_MM_TESTING only) ----------- */
#ifdef MOQ_WTQ_MM_TESTING
/* Non-NULL sentinels so the deadline path treats the synthetic facade as
 * having a live connection + adapter without dereferencing either (every
 * primitive that would touch them is stubbed above). */
static char nwm_test_conn_sentinel;
static char nwm_test_mc_sentinel;

moq_wtquic_network_managed_t *moq_wtquic_network_managed_test_new(
    uint64_t (*app_deadline_us)(void *), void *app_deadline_ctx)
{
    moq_wtquic_network_managed_t *m =
        (moq_wtquic_network_managed_t *)calloc(1, sizeof(*m));
    if (m == NULL)
        return NULL;
    pthread_mutex_init(&m->mu, NULL);
    pthread_cond_init(&m->cv, NULL);
    m->wake_deadline_us = UINT64_MAX;
    m->conn = (wtq_nw_conn_t *)&nwm_test_conn_sentinel;
    m->mc = (moq_wtquic_conn_t *)&nwm_test_mc_sentinel;
    m->ms = NULL;                       /* no session -> session dl = UINT64_MAX */
    m->on_lane_pump = nwm_test_pump_rec;
    m->app_deadline_us = app_deadline_us;
    m->app_deadline_ctx = app_deadline_ctx;
    g_nwm_test_m = m;
    return m;
}

void moq_wtquic_network_managed_test_free(moq_wtquic_network_managed_t *m)
{
    if (m == NULL)
        return;
    if (g_nwm_test_m == m)
        g_nwm_test_m = NULL;
    pthread_mutex_destroy(&m->mu);
    pthread_cond_destroy(&m->cv);
    free(m);
}

void moq_wtquic_network_managed_test_doorbell(moq_wtquic_network_managed_t *m)
{ nwm_doorbell(m); }

void moq_wtquic_network_managed_test_rearm(moq_wtquic_network_managed_t *m)
{ nwm_rearm_deadline(m); }

void moq_wtquic_network_managed_test_latch_closed(
    moq_wtquic_network_managed_t *m, uint64_t code)
{ nwm_latch_closed(m, code); }

void moq_wtquic_network_managed_test_latch_fatal(
    moq_wtquic_network_managed_t *m, uint64_t code)
{ nwm_latch_fatal(m, code); }

void moq_wtquic_network_managed_test_set_wake_deadline(
    moq_wtquic_network_managed_t *m, uint64_t v)
{ m->wake_deadline_us = v; }

uint64_t moq_wtquic_network_managed_test_get_wake_deadline(
    const moq_wtquic_network_managed_t *m)
{ return m->wake_deadline_us; }

uint64_t moq_wtquic_network_managed_test_now_us(void)
{ return nwm_now_us(); }
#endif /* MOQ_WTQ_MM_TESTING */
