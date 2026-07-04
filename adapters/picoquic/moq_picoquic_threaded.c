/*
 * moq_pq_threaded — threaded picoquic adapter helper.
 *
 * CLIENT mode: creates the picoquic context and client connection at
 * create time and starts the network thread immediately. Without an
 * ALPN offer list (legacy single-version draft-16) the session and
 * adapter are created at create time too; with cfg.alpn_list set,
 * session creation is DEFERRED to the network thread until the
 * handshake negotiates an ALPN (or the peer's first data arrives,
 * whichever is first) so the session version matches the negotiation.
 *
 * SERVER mode: creates picoquic context and starts listening. Session
 * and adapter are created lazily on the network thread when the first
 * inbound connection fires picoquic_callback_ready. v0 supports one
 * active server connection.
 */

#include <moq/picoquic_threaded.h>
#include <picoquic_packet_loop.h>

#include "../common/moq_alpn.h"
#include "../common/moq_pq_stream_backlog.h"
#include "picoquic_endpoint.h"   /* moq_pq_send_stats_t + per-conn getter */

#include <errno.h>
#include <time.h>
#include <netdb.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Advertised inbound flow-control credit, per unidirectional stream and
 * connection-wide. Sized to the session's default receive budget so a large
 * inbound MoQ object is not held back by too small a window; see create. */
#define MOQ_PQ_RECV_FLOW_CONTROL (16u * 1024u * 1024u)

/* Default server connection cap when cfg.max_connections is 0. Mirrors the
 * mvfst managed server default. */
#define MOQ_PQ_THREADED_DEFAULT_MAX_CONNECTIONS 1024u

/* One accepted server connection. Heap-owned so the public opaque handle
 * (moq_pq_threaded_conn_t*) stays stable while the internal list grows/compacts.
 * All fields are touched only on the network thread except where noted. */
struct moq_pq_threaded_conn {
    struct moq_pq_threaded *parent;    /* owning server (for thread-guard) */
    picoquic_cnx_t *cnx;
    moq_session_t  *session;
    moq_pq_conn_t  *conn;
    moq_version_t   negotiated;
    bool            close_requested;   /* conn_close: prune after on_pump */
    bool            dead_observed;     /* terminal AND an app pump has run
                                          since (SESSION_CLOSED pollable);
                                          prune precondition for non-app
                                          closes */
    uint64_t        close_error_code;
    bool            fatal;             /* per-conn service failure: prune */
#ifdef MOQ_PQ_THREADED_TESTING
    bool            test_service_fatal; /* seam: next service pass fatals */
#endif
};

struct moq_pq_threaded {
    moq_alloc_t         alloc;

    /* Copied config */
    moq_perspective_t   perspective;
    int                 port;
    bool                send_request_capacity;
    uint64_t            initial_request_capacity;
    uint32_t            max_actions;
    uint32_t            max_events;
    uint32_t            max_data_streams;
    uint32_t            max_subscriptions;
    uint32_t            send_buffer_size;
    uint32_t            recv_buffer_size;
    uint64_t            goaway_timeout_us;
    bool                insecure_skip_verify;
    int               (*configure_quic_fn)(picoquic_quic_t *, void *);
    void               *configure_quic_ctx;
    int               (*on_pump)(moq_pq_threaded_t *, uint64_t, void *);
    void               *on_pump_ctx;
    void              (*on_activity)(moq_pq_threaded_t *, void *);
    void               *on_activity_ctx;

    /* Version negotiation (owned copies of the cfg ALPN offer list, in
     * preference order). Empty list = legacy single-ALPN behavior
     * (MOQ_PQ_ALPN_DEFAULT, eager client session, draft-16). */
    char              **alpn_list;
    size_t              alpn_count;
    size_t              alpn_array_bytes;
    bool                client_deferred;  /* client session awaits the
                                           * negotiated-ALPN readback */

    /* Resources (owned) */
    picoquic_quic_t    *quic;
    picoquic_packet_loop_param_t loop_param;

    /* Mutex-protected state */
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
    picoquic_network_thread_ctx_t *thread_ctx;
    /* Client mode: the single session/conn/cnx (eager or deferred). */
    moq_session_t      *session;
    moq_pq_conn_t      *conn;
    picoquic_cnx_t     *active_cnx;
    /* Server mode: per-connection records (heap-owned; stable handles).
     * Grown on accept, compacted on prune -- both on the network thread; read
     * under mutex by conn_count / the legacy first-conn accessors. */
    struct moq_pq_threaded_conn **conns;
    size_t              conn_count;
    size_t              conn_cap;
    size_t              max_connections;
    bool                fatal;
    uint64_t            fatal_code;
    bool                stopped;
    bool                pump_exit;
    bool                tx_drained;     /* network thread: local stream flush
                                         * done (no queued/ready stream data or
                                         * unsent FIN). Read by drain_state. */
    bool                loop_exited;    /* loop acknowledged stop request */
    bool                loop_came_up;   /* packet loop reached its ready
                                           callback (latched; a later loop
                                           exit does not clear it) */
    bool                setup_reached;  /* client: session hit ESTABLISHED */
    moq_version_t       negotiated;     /* 0 until the version is known */
    bool                activity_pending;
    bool                wake_pending;
    int                 wake_in_flight;
    pthread_t           network_thread_id;
    bool                network_thread_id_set;

    /* Characterization telemetry (env MOQ_PQ_THREADED_STATS=1). Written only on
     * the network thread; read after the thread is joined at stop. */
    bool                stats_enabled;
    uint64_t            st_after_send;      /* after_send loop callbacks */
    uint64_t            st_after_send_nz;   /* after_send phases that sent >0 pkts */
    uint64_t            st_after_recv;      /* after_receive loop callbacks */
    uint64_t            st_wake;            /* wake_up loop callbacks */
    uint64_t            st_after_send_bytes;/* sum of bytes_sent reported */
    uint64_t            st_service_all;     /* server_service_all invocations */
    uint64_t            st_pkts_total;      /* packets sent (path-quality delta) */
    uint64_t            st_pkts_last;       /* last summed path-quality `sent` */
    uint64_t            st_pkts_max_burst;  /* max packets in one after_send */
    /* Per-connection stats accumulated from conns freed before stop, so a pruned
     * connection's prepare/queue counts are not lost. Final = acc + live conns. */
    uint64_t            st_acc_prepare;
    uint64_t            st_acc_provided;
    uint64_t            st_acc_would_block;
    uint64_t            st_acc_high_water;  /* max, not sum */
};

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static bool cfg_has_field(const moq_pq_threaded_cfg_t *cfg,
                           size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}

#define CFG_HAS(cfg, field) \
    cfg_has_field(cfg, offsetof(moq_pq_threaded_cfg_t, field), \
                  sizeof((cfg)->field))

static void mark_activity(moq_pq_threaded_t *t)
{
    pthread_mutex_lock(&t->mutex);
    t->activity_pending = true;
    pthread_cond_broadcast(&t->cond);
    pthread_mutex_unlock(&t->mutex);
    if (t->on_activity)
        t->on_activity(t, t->on_activity_ctx);
}

static void build_session_cfg(moq_pq_threaded_t *t,
                                moq_session_cfg_t *scfg,
                                moq_perspective_t persp)
{
    moq_session_cfg_init_sized(scfg, sizeof(*scfg), &t->alloc, persp);
    scfg->send_request_capacity = t->send_request_capacity;
    scfg->initial_request_capacity = t->initial_request_capacity;
    if (t->max_actions) scfg->max_actions = t->max_actions;
    if (t->max_events) scfg->max_events = t->max_events;
    if (t->max_data_streams) scfg->max_data_streams = t->max_data_streams;
    if (t->max_subscriptions) scfg->max_subscriptions = t->max_subscriptions;
    if (t->send_buffer_size) scfg->send_buffer_size = t->send_buffer_size;
    if (t->recv_buffer_size) scfg->recv_buffer_size = t->recv_buffer_size;
    if (t->goaway_timeout_us) scfg->goaway_timeout_us = t->goaway_timeout_us;
}

/* ------------------------------------------------------------------ */
/* Server connection list (all mutations on the network thread)        */
/* ------------------------------------------------------------------ */

/* Mirror the first live connection into the legacy single-connection accessors
 * (moq_pq_threaded_session/_conn). Keeping this in sync under the mutex lets
 * those accessors stay thread-safe without the app thread ever touching conns[]
 * (which only the network thread mutates). Caller holds t->mutex. */
static void server_publish_first_locked(moq_pq_threaded_t *t)
{
    if (t->conn_count > 0) {
        t->session    = t->conns[0]->session;
        t->conn       = t->conns[0]->conn;
        t->active_cnx = t->conns[0]->cnx;
    } else {
        t->session = NULL;
        t->conn = NULL;
        t->active_cnx = NULL;
    }
}

/* Append a heap-owned record. Caller holds t->mutex (conn_count is read
 * cross-thread). Returns false on OOM. */
static bool server_conns_append(moq_pq_threaded_t *t,
                                struct moq_pq_threaded_conn *c)
{
    if (t->conn_count == t->conn_cap) {
        size_t ncap = t->conn_cap ? t->conn_cap * 2 : 8;
        struct moq_pq_threaded_conn **na = t->alloc.realloc(
            t->conns, t->conn_cap * sizeof(*na),
            ncap * sizeof(*na), t->alloc.ctx);
        if (!na) return false;
        t->conns = na;
        t->conn_cap = ncap;
    }
    t->conns[t->conn_count++] = c;
    server_publish_first_locked(t);
    return true;
}

/* Tear down one connection record. do_close sends a picoquic CONNECTION_CLOSE
 * (close_code) first -- required for an app-requested close AND for a local /
 * service / bridge fatal (moq_pq_service only reports bridge-fatal; it does not
 * close the cnx), so the QUIC connection is not left dangling. Callers skip the
 * close only when the peer already closed it. moq_pq_conn_destroy then unbinds
 * the cnx callback, so no callback can route into the freed adapter. Network
 * thread. */
static void server_conn_free(moq_pq_threaded_t *t,
                             struct moq_pq_threaded_conn *c,
                             bool do_close,
                             uint64_t close_code)
{
    if (!c) return;
    if (do_close && c->cnx)
        picoquic_close(c->cnx, close_code);
    /* Fold this conn's send stats into the accumulator before it is destroyed,
     * so a pruned connection's counts survive to the stop-time report. */
    if (t->stats_enabled && c->conn) {
        moq_pq_send_stats_t s;
        moq_pq_conn_get_send_stats(c->conn, &s);
        t->st_acc_prepare += s.prepare_count;
        t->st_acc_provided += s.provided_bytes;
        t->st_acc_would_block += s.queue_would_block;
        if (s.queue_high_water > t->st_acc_high_water)
            t->st_acc_high_water = s.queue_high_water;
    }
    if (c->conn) moq_pq_conn_destroy(c->conn);
    if (c->session) moq_session_destroy(c->session);
    t->alloc.free(c, sizeof(*c), t->alloc.ctx);
}

/* Accept one new server connection: create its session + adapter at the
 * negotiated ALPN version, start it, and append a heap-owned record. Returns
 * the record or NULL (caller returns -1 so picoquic closes the cnx). Network
 * thread. */
static struct moq_pq_threaded_conn *server_accept(moq_pq_threaded_t *t,
                                                  picoquic_cnx_t *cnx,
                                                  uint64_t now)
{
    moq_session_cfg_t scfg;
    build_session_cfg(t, &scfg, MOQ_PERSPECTIVE_SERVER);
    /* Negotiated ALPN names the session version (§13). */
    const char *alpn = picoquic_tls_get_negotiated_alpn(cnx);
    moq_version_t v = (moq_version_t)0;
    if (alpn && moq_alpn_to_version(alpn, strlen(alpn), &v))
        scfg.version = v;
    else if (alpn)
        return NULL;   /* unmappable ALPN: refuse rather than guess */

    moq_session_t *session = NULL;
    if (moq_session_create(&scfg, now, &session) != MOQ_OK)
        return NULL;

    moq_pq_conn_cfg_t acfg;
    moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
    acfg.session = session;
    acfg.cnx = cnx;
    acfg.alloc = &t->alloc;
    moq_pq_conn_t *conn = NULL;
    if (moq_pq_conn_create(&acfg, &conn) != 0) {
        moq_session_destroy(session);
        return NULL;
    }

    /* Start the server session (draft-18 sends its own SETUP; draft-16 is a
     * WRONG_STATE no-op). */
    moq_result_t src = moq_session_start(session, now);
    if (src != MOQ_OK && src != MOQ_ERR_WRONG_STATE) {
        moq_pq_conn_destroy(conn);
        moq_session_destroy(session);
        return NULL;
    }
    if (src == MOQ_OK && moq_pq_service(conn, now) < 0) {
        moq_pq_conn_destroy(conn);
        moq_session_destroy(session);
        return NULL;
    }

    struct moq_pq_threaded_conn *c = t->alloc.alloc(sizeof(*c), t->alloc.ctx);
    if (!c) {
        moq_pq_conn_destroy(conn);
        moq_session_destroy(session);
        return NULL;
    }
    memset(c, 0, sizeof(*c));
    c->parent = t;
    c->cnx = cnx;
    c->session = session;
    c->conn = conn;
    c->negotiated = scfg.version ? scfg.version : MOQ_VERSION_DRAFT_16;

    pthread_mutex_lock(&t->mutex);
    bool ok = server_conns_append(t, c);
    pthread_mutex_unlock(&t->mutex);
    if (!ok) {
        moq_pq_conn_destroy(conn);
        moq_session_destroy(session);
        t->alloc.free(c, sizeof(*c), t->alloc.ctx);
        return NULL;
    }
    return c;
}

/* Service every live server connection; a per-conn failure marks it for prune
 * (does NOT terminate the whole threaded server). Network thread; conns[] is
 * mutated only on this thread, so no lock is needed to iterate. */
static void server_service_all(moq_pq_threaded_t *t, uint64_t now)
{
    if (t->stats_enabled) t->st_service_all++;
    for (size_t i = 0; i < t->conn_count; i++) {
        struct moq_pq_threaded_conn *c = t->conns[i];
#ifdef MOQ_PQ_THREADED_TESTING
        if (c->test_service_fatal) {
            c->test_service_fatal = false;
            /* Model a transport close surfacing in THIS service pass with
             * the REAL artifact: moq_session_on_transport_close queues
             * MOQ_EVENT_SESSION_CLOSED and the conn then classifies dead
             * via is_closed -- so the lifecycle test asserts the EVENT is
             * pollable in the observation window, not mere visibility. */
            if (c->session)
                (void)moq_session_on_transport_close(c->session, 0, now);
            c->fatal = true;    /* dead THIS pass, event queued above */
            continue;
        }
#endif
        if (c->fatal || c->close_requested) continue;
        if (moq_pq_service(c->conn, now) < 0)
            c->fatal = true;
    }
}

#ifdef MOQ_PQ_THREADED_TESTING
/* Test seam (never shipped; the test-internals build only): model a
 * transport close that surfaces in the NEXT service pass, queuing the real
 * MOQ_EVENT_SESSION_CLOSED. Armed from on_pump, that is the post-pump
 * pass -- the exact window where a conn can die with no app pump between
 * its close event being queued and the prune destroying the session. */
void moq_pq_threaded_test_arm_service_fatal(moq_pq_threaded_conn_t *c);
void moq_pq_threaded_test_arm_service_fatal(moq_pq_threaded_conn_t *c)
{
    c->test_service_fatal = true;
}
#endif

/* True once a connection should leave iteration: app-closed, service-fatal,
 * bridge-fatal, or cleanly closed by the peer. */
static bool server_conn_dead(const struct moq_pq_threaded_conn *c)
{
    return c->close_requested || c->fatal ||
           (c->conn && (moq_pq_conn_is_fatal(c->conn) ||
                        moq_pq_conn_is_closed(c->conn)));
}

/* Record that an app pump has run with the currently-dead connections
 * still iterable: they may be pruned now. Network-thread only. */
static void server_mark_dead_observed(moq_pq_threaded_t *t)
{
    for (size_t i = 0; i < t->conn_count; i++) {
        struct moq_pq_threaded_conn *c = t->conns[i];
        if (server_conn_dead(c))
            c->dead_observed = true;
    }
}

/* True when some connection died WITHOUT an app pump since (peer close or
 * service/bridge fatal in the post-pump service pass). App-requested
 * closes don't count: the app initiated them. Network-thread only. */
static bool server_has_unobserved_dead(const moq_pq_threaded_t *t)
{
    for (size_t i = 0; i < t->conn_count; i++) {
        const struct moq_pq_threaded_conn *c = t->conns[i];
        if (!c->dead_observed && !c->close_requested && server_conn_dead(c))
            return true;
    }
    return false;
}

/* Remove dead connections after on_pump, so next_conn() stayed stable during
 * the callback. Only connections whose terminal state an app pump could
 * observe (dead_observed; MOQ_EVENT_SESSION_CLOSED pollable) or that the
 * app itself closed are freed -- never a dead conn the app has not had a
 * pump to see. Compaction is network-thread-only; only conn_count is
 * published under the mutex for the cross-thread count accessor. */
static void server_prune(moq_pq_threaded_t *t)
{
    size_t w = 0;
    for (size_t r = 0; r < t->conn_count; r++) {
        struct moq_pq_threaded_conn *c = t->conns[r];
        if (server_conn_dead(c) && (c->dead_observed || c->close_requested)) {
            /* Close the cnx unless the peer already closed it. An app close
             * carries its requested code; a local/service/bridge fatal carries
             * the bridge fatal code (moq_pq_service left the cnx open). */
            bool already_closed = c->conn && moq_pq_conn_is_closed(c->conn);
            uint64_t code = c->close_requested
                                ? c->close_error_code
                                : (c->conn ? moq_pq_conn_fatal_code(c->conn) : 0);
            server_conn_free(t, c, !already_closed, code);
        } else
            t->conns[w++] = c;
    }
    if (w != t->conn_count) {
        pthread_mutex_lock(&t->mutex);
        t->conn_count = w;
        server_publish_first_locked(t);
        pthread_mutex_unlock(&t->mutex);
    }
}

/* ------------------------------------------------------------------ */
/* Picoquic client callback                                            */
/* ------------------------------------------------------------------ */

static void free_alpn_list(moq_pq_threaded_t *t)
{
    if (!t->alpn_list) return;
    for (size_t i = 0; i < t->alpn_count; i++) {
        if (t->alpn_list[i])
            t->alloc.free(t->alpn_list[i], strlen(t->alpn_list[i]) + 1,
                          t->alloc.ctx);
    }
    /* The pointer array was sized for the cfg count, but alpn_count tracks
     * how many entries were filled; the array itself was allocated with the
     * cfg count. Free with the filled count is wrong only if allocation
     * aborted midway -- store the array size alongside instead. */
    t->alloc.free(t->alpn_list, t->alpn_array_bytes, t->alloc.ctx);
    t->alpn_list = NULL;
    t->alpn_count = 0;
    t->alpn_array_bytes = 0;
}

/* Server-side ALPN selection with an offer list: walk the CLIENT's proposed
 * list in order and pick the first entry this server supports, so the
 * client's preference order decides (mirrors the WebTransport rule). Returns
 * `count` when nothing matches, which fails the handshake. */
static size_t threaded_alpn_select(picoquic_quic_t *quic,
                                   picoquic_iovec_t *list, size_t count)
{
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)
        picoquic_get_default_callback_context(quic);
    if (!t) return count;
    for (size_t i = 0; i < count; i++) {
        for (size_t j = 0; j < t->alpn_count; j++) {
            size_t n = strlen(t->alpn_list[j]);
            if (list[i].len == n &&
                memcmp(list[i].base, t->alpn_list[j], n) == 0)
                return i;
        }
    }
    return count;
}

/* Fail the (not yet sessioned) client connection: latch fatal so wait()
 * returns in bounded time, and return -1 so picoquic closes the cnx. */
static int client_fail_deferred(moq_pq_threaded_t *t)
{
    pthread_mutex_lock(&t->mutex);
    if (!t->fatal) {
        t->fatal = true;
        t->fatal_code = 0;   /* transport/handshake-level failure */
    }
    pthread_mutex_unlock(&t->mutex);
    mark_activity(t);
    return -1;
}

static int client_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)callback_ctx;

    /* Multi-version offer: the connection was created with a NULL ALPN, so
     * picoquic asks for the proposal list here (bytes = the TLS context).
     * Offer in cfg order -- the list IS the preference order. */
    if (event == picoquic_callback_request_alpn_list) {
        for (size_t i = 0; i < t->alpn_count; i++) {
            if (picoquic_add_proposed_alpn((void *)bytes,
                                           t->alpn_list[i]) != 0)
                return -1;
        }
        return 0;
    }

    /* Deferred client session (§13 ALPN readback): with an offer list the
     * session is created only once the handshake has negotiated an ALPN, so
     * moq_session_cfg_t.version can be set to match BEFORE any SETUP byte is
     * parsed. An unknown ALPN, or one outside the offered set, fails the
     * connection here -- never a silently mis-versioned session.
     *
     * The trigger includes the FIRST INBOUND DATA events, not just
     * almost_ready/ready: a draft-18 peer sends its SETUP on its own
     * unidirectional control stream immediately, and those bytes can arrive
     * in the same packet batch as handshake completion -- picoquic then
     * delivers stream data BEFORE the ready callbacks. Creating the session
     * here (1-RTT data implies the ALPN is negotiated) lets the event fall
     * through to the fresh adapter instead of being dropped, which would
     * lose the peer's SETUP forever. */
    if (t->client_deferred && !t->conn &&
        (event == picoquic_callback_almost_ready ||
         event == picoquic_callback_ready ||
         event == picoquic_callback_stream_data ||
         event == picoquic_callback_stream_fin ||
         event == picoquic_callback_datagram)) {
        const char *alpn = picoquic_tls_get_negotiated_alpn(cnx);
        moq_version_t v = (moq_version_t)0;
        bool offered = false;
        if (!alpn || !moq_alpn_to_version(alpn, strlen(alpn), &v))
            return client_fail_deferred(t);
        for (size_t i = 0; i < t->alpn_count; i++) {
            if (strcmp(alpn, t->alpn_list[i]) == 0) { offered = true; break; }
        }
        if (!offered)
            return client_fail_deferred(t);

        uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));
        moq_session_cfg_t scfg;
        build_session_cfg(t, &scfg, MOQ_PERSPECTIVE_CLIENT);
        scfg.version = v;
        moq_session_t *session = NULL;
        if (moq_session_create(&scfg, now, &session) != MOQ_OK)
            return client_fail_deferred(t);

        moq_pq_conn_cfg_t acfg;
        moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
        acfg.session = session;
        acfg.cnx = cnx;
        acfg.alloc = &t->alloc;
        moq_pq_conn_t *conn = NULL;
        if (moq_pq_conn_create(&acfg, &conn) != 0) {
            moq_session_destroy(session);
            return client_fail_deferred(t);
        }
        if (moq_session_start(session, now) != MOQ_OK ||
            moq_pq_service(conn, now) < 0) {
            moq_pq_conn_destroy(conn);
            moq_session_destroy(session);
            return client_fail_deferred(t);
        }

        pthread_mutex_lock(&t->mutex);
        t->session = session;
        t->conn = conn;
        t->negotiated = v;
        pthread_mutex_unlock(&t->mutex);
        mark_activity(t);
        /* Fall through: forward this event to the fresh adapter too. */
    }

    if (!t->conn) return 0;
    return moq_pq_callback(cnx, stream_id, bytes, length,
                            event, t->conn, stream_ctx);
}

/* ------------------------------------------------------------------ */
/* Picoquic server callback (lazy session/adapter creation)            */
/* ------------------------------------------------------------------ */

static int server_callback(picoquic_cnx_t *cnx,
    uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t event, void *callback_ctx,
    void *stream_ctx)
{
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)callback_ctx;
    uint64_t now = picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));

    /* Existing record for this cnx? conns[] is mutated only on this (network)
     * thread, so the scan needs no lock. */
    struct moq_pq_threaded_conn *rec = NULL;
    for (size_t i = 0; i < t->conn_count; i++)
        if (t->conns[i]->cnx == cnx) { rec = t->conns[i]; break; }

    if (event == picoquic_callback_almost_ready ||
        event == picoquic_callback_ready) {
        /* Already accepted (almost_ready then ready both fire): benign. */
        if (rec) return 0;
        /* Connection cap: reject a new connection past max_connections.
         * Returning -1 makes picoquic close the not-yet-bound cnx. */
        if (t->conn_count >= t->max_connections)
            return -1;
        /* Accept: create session+adapter for THIS cnx and append a record. A
         * per-connection accept failure rejects only this cnx (-1) -- it does
         * NOT make the whole threaded server fatal. moq_pq_conn_create rebinds
         * the cnx callback to moq_pq_callback(conn), so subsequent events for
         * this cnx route straight to its own connection. */
        struct moq_pq_threaded_conn *c = server_accept(t, cnx, now);
        if (!c) return -1;
        pthread_mutex_lock(&t->mutex);
        if (t->negotiated == 0) t->negotiated = c->negotiated;
        pthread_mutex_unlock(&t->mutex);
        mark_activity(t);
        return 0;
    }

    /* Safety net: route a non-ready event to its connection (normally already
     * rebound to moq_pq_callback by moq_pq_conn_create). */
    if (rec && rec->conn && !server_conn_dead(rec))
        return moq_pq_callback(cnx, stream_id, bytes, length,
                                event, rec->conn, stream_ctx);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Packet loop callback                                                */
/* ------------------------------------------------------------------ */

/* Sum picoquic's cumulative packets-sent across the instance's connection(s),
 * via the public default-path quality accessor. Network-thread only. */
static uint64_t pq_sum_pkts_sent(moq_pq_threaded_t *t)
{
    uint64_t sum = 0;
    picoquic_path_quality_t q;
    if (t->perspective == MOQ_PERSPECTIVE_SERVER) {
        for (size_t i = 0; i < t->conn_count; i++) {
            if (!t->conns[i]->cnx) continue;
            memset(&q, 0, sizeof(q));
            picoquic_get_default_path_quality(t->conns[i]->cnx, &q);
            sum += q.sent;
        }
    } else if (t->active_cnx) {
        memset(&q, 0, sizeof(q));
        picoquic_get_default_path_quality(t->active_cnx, &q);
        sum += q.sent;
    }
    return sum;
}

static int loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg)
{
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)callback_ctx;

    if (cb_mode == picoquic_packet_loop_ready) {
        pthread_mutex_lock(&t->mutex);
        t->network_thread_id = pthread_self();
        t->network_thread_id_set = true;
        t->loop_came_up = true;
        pthread_cond_broadcast(&t->cond);
        pthread_mutex_unlock(&t->mutex);
        return 0;
    }

    if (cb_mode != picoquic_packet_loop_after_receive &&
        cb_mode != picoquic_packet_loop_after_send &&
        cb_mode != picoquic_packet_loop_wake_up)
        return 0;

    if (t->stats_enabled) {
        if (cb_mode == picoquic_packet_loop_after_send) {
            t->st_after_send++;
            if (callback_arg)
                t->st_after_send_bytes += *(size_t *)callback_arg;
            uint64_t cur = pq_sum_pkts_sent(t);
            uint64_t d = (cur >= t->st_pkts_last) ? cur - t->st_pkts_last : 0;
            t->st_pkts_total += d;
            if (d > 0) t->st_after_send_nz++;   /* phase that actually sent */
            if (d > t->st_pkts_max_burst) t->st_pkts_max_burst = d;
            t->st_pkts_last = cur;
        } else if (cb_mode == picoquic_packet_loop_after_receive) {
            t->st_after_recv++;
        } else {
            t->st_wake++;
        }
    }

    /* Clear wake_pending before any early return so the flag
     * doesn't stay stale when conn is not yet available. Remember the
     * mode: a wake-up cycle with no session/conn yet (deferred client
     * mid-handshake, server before the first accept) is still a pump
     * cycle by the public wake contract, so the pre-session early
     * returns below must mark activity for it -- otherwise a parked
     * wait() sleeps out the handshake deadline instead of waking. */
    bool was_wake_up = (cb_mode == picoquic_packet_loop_wake_up);
    if (was_wake_up) {
        pthread_mutex_lock(&t->mutex);
        t->wake_pending = false;
        pthread_mutex_unlock(&t->mutex);
    }

    /* stop() requested exit and pipe-woke the loop: leave promptly. The
     * close-based wake inside picoquic_delete_network_thread does not
     * reliably interrupt a blocked wait on every platform, so without
     * this the join would stall until the loop's natural timer. */
    pthread_mutex_lock(&t->mutex);
    bool stop_requested = t->stopped;
    if (stop_requested) {
        t->loop_exited = true;
        pthread_cond_broadcast(&t->cond);
    }
    pthread_mutex_unlock(&t->mutex);
    if (stop_requested)
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;

    /* Client mode (deferred-offer): before the session exists, the only
     * terminal signal is the connection dying (e.g. ALPN no-overlap ends the
     * handshake). Detect it here -- the conn-gated check below cannot run. */
    if (t->perspective == MOQ_PERSPECTIVE_CLIENT && !t->conn &&
        t->active_cnx &&
        picoquic_get_cnx_state(t->active_cnx) == picoquic_state_disconnected) {
        pthread_mutex_lock(&t->mutex);
        if (!t->fatal && !t->pump_exit) {
            t->fatal = true;
            t->fatal_code = 0;
        }
        pthread_mutex_unlock(&t->mutex);
        mark_activity(t);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    /* --- SERVER: service every connection, one on_pump, then prune. A
     * per-connection failure only prunes that connection; the threaded server
     * stays up (only endpoint-wide failures terminate the packet loop). --- */
    if (t->perspective == MOQ_PERSPECTIVE_SERVER) {
        uint64_t snow = picoquic_get_quic_time(quic);
        if (t->conn_count == 0) {
            /* No accepted connection yet: on_pump not called, but a
             * requested wake still completes as a no-op pump cycle. */
            if (was_wake_up)
                mark_activity(t);
            return 0;
        }

        server_service_all(t, snow);

        if (t->on_pump) {
            int prc = t->on_pump(t, snow, t->on_pump_ctx);
            if (prc != 0) {
                pthread_mutex_lock(&t->mutex);
                t->pump_exit = true;
                pthread_mutex_unlock(&t->mutex);
                mark_activity(t);
                return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
            }
        }
        /* Every conn dead before/during that pump was observable by it. */
        server_mark_dead_observed(t);

        server_service_all(t, snow);

        /* Terminal-conn observation window: a conn that turned dead in the
         * second service pass (peer close processed there, service/bridge
         * fatal) has had NO app pump since its MOQ_EVENT_SESSION_CLOSED was
         * queued, and the prune below would destroy the session with the
         * event un-polled -- an app that releases per-connection state on
         * SESSION_CLOSED would leak it permanently. Run ONE extra
         * SEQUENTIAL on_pump so the app observes it in this same
         * packet-loop callback; the conn never survives into a later
         * iteration where the app could re-attach to a dying session.
         * App-requested closes (close_requested) skip the window: the app
         * initiated them and prunes after the current pump as before. */
        if (t->on_pump && server_has_unobserved_dead(t)) {
            int prc = t->on_pump(t, snow, t->on_pump_ctx);
            if (prc != 0) {
                pthread_mutex_lock(&t->mutex);
                t->pump_exit = true;
                pthread_mutex_unlock(&t->mutex);
                mark_activity(t);
                return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
            }
            server_mark_dead_observed(t);
        }

        server_prune(t);   /* safe: after on_pump returned */

        /* Per-connection app-wake times (each cnx has its own deadline) and an
         * aggregate drain state for moq_endpoint_drain(). */
        bool all_drained = true;
        for (size_t i = 0; i < t->conn_count; i++) {
            struct moq_pq_threaded_conn *c = t->conns[i];
            moq_session_t *sess = moq_pq_conn_session(c->conn);
            if (sess) {
                uint64_t dl = moq_session_next_deadline_us(sess);
                picoquic_set_app_wake_time(c->cnx,
                                            dl == UINT64_MAX ? 0 : dl);
            }
            if (!(picoquic_get_cnx_state(c->cnx) == picoquic_state_ready &&
                  moq_pq_cnx_stream_backlog_empty(c->cnx)))
                all_drained = false;
        }
        pthread_mutex_lock(&t->mutex);
        t->tx_drained = all_drained;
        pthread_mutex_unlock(&t->mutex);

        mark_activity(t);
        return 0;
    }

    if (!t->conn) {
        /* Deferred client before ALPN negotiation: no session to service,
         * but a requested wake still completes as a no-op pump cycle. */
        if (was_wake_up)
            mark_activity(t);
        return 0;
    }

    uint64_t now = picoquic_get_quic_time(quic);

    /* Client mode: a transport disconnect before the MoQ session reaches
     * ESTABLISHED is a handshake/connect failure (cert rejection, connection
     * refused, dead peer).  picoquic delivers it as picoquic_callback_close,
     * which the bridge treats as a clean close (session CLOSED, not fatal) —
     * indistinguishable from a normal post-setup close.  Latch it as fatal
     * here so is_fatal() observes it and wait() returns in bounded time,
     * matching pico WT managed.  setup_reached is sticky: once the session
     * has been ESTABLISHED (or DRAINING, which only follows ESTABLISHED), a
     * later disconnect is a normal close and stays non-fatal. */
    if (t->perspective == MOQ_PERSPECTIVE_CLIENT && t->active_cnx &&
        !t->setup_reached) {
        moq_session_t *sess = moq_pq_conn_session(t->conn);
        moq_session_state_t st = sess ? moq_session_state(sess)
                                      : MOQ_SESS_IDLE;
        if (st == MOQ_SESS_ESTABLISHED || st == MOQ_SESS_DRAINING) {
            t->setup_reached = true;
        } else if (picoquic_get_cnx_state(t->active_cnx) ==
                   picoquic_state_disconnected) {
            pthread_mutex_lock(&t->mutex);
            if (!t->fatal && !t->pump_exit) {
                t->fatal = true;
                t->fatal_code = 0;  /* transport/handshake-level failure */
            }
            pthread_mutex_unlock(&t->mutex);
            mark_activity(t);
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    /* service → on_pump → service */
    if (moq_pq_service(t->conn, now) < 0) {
        pthread_mutex_lock(&t->mutex);
        t->fatal = true;
        t->fatal_code = moq_pq_conn_fatal_code(t->conn);
        pthread_mutex_unlock(&t->mutex);
        mark_activity(t);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    if (t->on_pump) {
        int prc = t->on_pump(t, now, t->on_pump_ctx);
        if (prc != 0) {
            pthread_mutex_lock(&t->mutex);
            t->pump_exit = true;
            pthread_mutex_unlock(&t->mutex);
            mark_activity(t);
            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        }
    }

    if (moq_pq_service(t->conn, now) < 0) {
        pthread_mutex_lock(&t->mutex);
        t->fatal = true;
        t->fatal_code = moq_pq_conn_fatal_code(t->conn);
        pthread_mutex_unlock(&t->mutex);
        mark_activity(t);
        return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
    }

    /* Set app wake time for session deadlines. */
    if (t->active_cnx) {
        moq_session_t *sess = moq_pq_conn_session(t->conn);
        if (sess) {
            uint64_t dl = moq_session_next_deadline_us(sess);
            picoquic_set_app_wake_time(t->active_cnx,
                                        dl == UINT64_MAX ? 0 : dl);
        }
    }

    /* Publish graceful-drain state (network thread) for moq_endpoint_drain():
     * local stream flush is done once no stream has queued/ready reliable data
     * or an unsent FIN -- every reliable stream byte + FIN handed to the
     * transport. Deliberately NOT the packet-ACK backlog, which can stay
     * non-empty on an idle connection after the bytes are sent. The probe is
     * network-thread-only and side-effect free. */
    {
        bool drained =
            t->active_cnx &&
            picoquic_get_cnx_state(t->active_cnx) == picoquic_state_ready &&
            moq_pq_cnx_stream_backlog_empty(t->active_cnx);
        pthread_mutex_lock(&t->mutex);
        t->tx_drained = drained;
        pthread_mutex_unlock(&t->mutex);
    }

    mark_activity(t);
    return 0;
}

/* ------------------------------------------------------------------ */
/* cfg_init                                                            */
/* ------------------------------------------------------------------ */

/* Frozen prefix size: the layout that existed before goaway_timeout_us was
 * appended. The pointer-only initializer cannot know the caller's storage size,
 * so it touches only this prefix -- safe for an old binary whose
 * moq_pq_threaded_cfg_t ended before goaway_timeout_us. (sni/alpn_* predate
 * goaway and stay inside this prefix, so they remain enabled under the
 * pointer-only init; every field appended past it -- goaway_timeout_us,
 * max_connections, idle_timeout_ms -- defaults to disabled/zero behind
 * CFG_HAS.) */
#define MOQ_PQ_THREADED_CFG_V0_SIZE \
    (offsetof(moq_pq_threaded_cfg_t, goaway_timeout_us))

void moq_pq_threaded_cfg_init(moq_pq_threaded_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen prefix: writing sizeof(*cfg) here would
     * overflow an old caller that allocated the smaller pre-goaway struct.
     * Appended fields past the prefix stay disabled (struct_size == prefix);
     * callers that want them use moq_pq_threaded_cfg_init_sized(). */
    memset(cfg, 0, MOQ_PQ_THREADED_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PQ_THREADED_CFG_V0_SIZE;
}

void moq_pq_threaded_cfg_init_sized(moq_pq_threaded_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. A caller older than the library passes a smaller
     * cfg_size (we clear/stamp that prefix); a caller newer than the library
     * passes a larger one (we clamp to our sizeof and leave its extra fields
     * to its own initializer). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

/* ------------------------------------------------------------------ */
/* create                                                              */
/* ------------------------------------------------------------------ */

moq_result_t moq_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                     moq_pq_threaded_t **out)
{
    if (out) *out = NULL;
    if (!cfg || !out) return MOQ_ERR_INVAL;

    if (cfg->struct_size < offsetof(moq_pq_threaded_cfg_t, alloc) +
        sizeof(cfg->alloc))
        return MOQ_ERR_INVAL;

    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free ||
        !cfg->alloc->realloc)
        return MOQ_ERR_INVAL;

    moq_perspective_t persp = CFG_HAS(cfg, perspective)
        ? cfg->perspective : 0;
    if (persp != MOQ_PERSPECTIVE_CLIENT && persp != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;

    if (!CFG_HAS(cfg, on_pump) || !cfg->on_pump)
        return MOQ_ERR_INVAL;

    int port = CFG_HAS(cfg, port) ? cfg->port : 0;
    if (port <= 0 || port > 65535)
        return MOQ_ERR_INVAL;

    const char *cert = CFG_HAS(cfg, cert_path) ? cfg->cert_path : NULL;
    const char *key  = CFG_HAS(cfg, key_path)  ? cfg->key_path  : NULL;

    if (persp == MOQ_PERSPECTIVE_CLIENT) {
        const char *host = CFG_HAS(cfg, host) ? cfg->host : NULL;
        if (!host || host[0] == '\0')
            return MOQ_ERR_INVAL;
    }

    if (persp == MOQ_PERSPECTIVE_SERVER) {
        if (!cert || cert[0] == '\0' || !key || key[0] == '\0')
            return MOQ_ERR_INVAL;
    }

    /* Allocate wrapper. */
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)cfg->alloc->alloc(
        sizeof(moq_pq_threaded_t), cfg->alloc->ctx);
    if (!t) return MOQ_ERR_NOMEM;
    memset(t, 0, sizeof(*t));

    t->alloc = *cfg->alloc;
    t->perspective = persp;
    t->port = port;
    {   /* Characterization telemetry: opt-in, off unless env set to non-"0". */
        const char *se = getenv("MOQ_PQ_THREADED_STATS");
        t->stats_enabled = se && *se && strcmp(se, "0") != 0;
    }

    /* Copy session tuning. */
    if (CFG_HAS(cfg, send_request_capacity))
        t->send_request_capacity = cfg->send_request_capacity;
    if (CFG_HAS(cfg, initial_request_capacity))
        t->initial_request_capacity = cfg->initial_request_capacity;
    if (CFG_HAS(cfg, max_actions))
        t->max_actions = cfg->max_actions;
    if (CFG_HAS(cfg, max_events))
        t->max_events = cfg->max_events;
    if (CFG_HAS(cfg, max_data_streams))
        t->max_data_streams = cfg->max_data_streams;
    if (CFG_HAS(cfg, max_subscriptions))
        t->max_subscriptions = cfg->max_subscriptions;
    if (CFG_HAS(cfg, send_buffer_size))
        t->send_buffer_size = cfg->send_buffer_size;
    if (CFG_HAS(cfg, recv_buffer_size))
        t->recv_buffer_size = cfg->recv_buffer_size;
    if (CFG_HAS(cfg, goaway_timeout_us))
        t->goaway_timeout_us = cfg->goaway_timeout_us;
    t->max_connections =
        (CFG_HAS(cfg, max_connections) && cfg->max_connections)
            ? cfg->max_connections
            : MOQ_PQ_THREADED_DEFAULT_MAX_CONNECTIONS;
    if (CFG_HAS(cfg, insecure_skip_verify))
        t->insecure_skip_verify = cfg->insecure_skip_verify;
    if (CFG_HAS(cfg, configure_quic)) {
        t->configure_quic_fn = cfg->configure_quic;
        t->configure_quic_ctx = cfg->configure_quic_ctx;
    }
    t->on_pump = cfg->on_pump;
    t->on_pump_ctx = CFG_HAS(cfg, on_pump_ctx) ? cfg->on_pump_ctx : NULL;
    if (CFG_HAS(cfg, on_activity)) {
        t->on_activity = cfg->on_activity;
        t->on_activity_ctx = cfg->on_activity_ctx;
    }

    /* Init mutex and condvar. */
    if (pthread_mutex_init(&t->mutex, NULL) != 0) {
        t->alloc.free(t, sizeof(*t), t->alloc.ctx);
        return MOQ_ERR_INTERNAL;
    }
    if (pthread_cond_init(&t->cond, NULL) != 0) {
        pthread_mutex_destroy(&t->mutex);
        t->alloc.free(t, sizeof(*t), t->alloc.ctx);
        return MOQ_ERR_INTERNAL;
    }

    /* ---- Version offer (ABI-appended cfg fields) ---- */

    if (CFG_HAS(cfg, alpn_count) && cfg->alpn_count > 0) {
        if (!CFG_HAS(cfg, alpn_list) || !cfg->alpn_list ||
            cfg->alpn_count > 8)
            goto fail_cfg_strings;
        t->alpn_list = (char **)t->alloc.alloc(
            cfg->alpn_count * sizeof(char *), t->alloc.ctx);
        if (!t->alpn_list) goto fail_cfg_strings;
        t->alpn_array_bytes = cfg->alpn_count * sizeof(char *);
        memset(t->alpn_list, 0, cfg->alpn_count * sizeof(char *));
        for (size_t i = 0; i < cfg->alpn_count; i++) {
            const char *a = cfg->alpn_list[i];
            if (!a || a[0] == '\0') goto fail_cfg_strings;
            size_t n = strlen(a) + 1;
            t->alpn_list[i] = (char *)t->alloc.alloc(n, t->alloc.ctx);
            if (!t->alpn_list[i]) goto fail_cfg_strings;
            memcpy(t->alpn_list[i], a, n);
            t->alpn_count = i + 1;
        }
    }
    const char *sni = (CFG_HAS(cfg, sni) && cfg->sni) ? cfg->sni : cfg->host;

    /* ---- Common: create picoquic context ---- */

    uint64_t now = picoquic_current_time();

    if (persp == MOQ_PERSPECTIVE_SERVER) {
        /* With an offer list the first entry is the default ALPN and the
         * select function matches the client's proposal order against the
         * whole list; without one, the legacy single draft-16 ALPN. */
        const char *default_alpn = t->alpn_count > 0 ? t->alpn_list[0]
                                                     : MOQ_PQ_ALPN_DEFAULT;
        /* The transport cap must match cfg.max_connections: picoquic fixes
         * max_nb_connections at context create (adjust can only lower it),
         * so a smaller value here would silently override the configured
         * cap before server_callback ever saw the connection. Fits uint32_t:
         * sourced from the uint32_t cfg field or the 1024 default. */
        t->quic = picoquic_create((uint32_t)t->max_connections, cert, key,
            NULL, default_alpn, server_callback, t,
            NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
        if (t->quic && t->alpn_count > 1)
            picoquic_set_alpn_select_fn_v2(t->quic, threaded_alpn_select);
    } else {
        t->quic = picoquic_create(1, NULL, NULL, NULL,
            MOQ_PQ_ALPN_DEFAULT, NULL, NULL,
            NULL, NULL, NULL, now, NULL, NULL, NULL, 0);
    }
    if (!t->quic) goto fail_quic;

    /* Negotiate QUIC DATAGRAM in both directions so the adapter's advertised
     * CAP_DATAGRAM is real rather than silently dropped: this is the max
     * datagram frame size we will accept (picoquic also auto-mirrors a peer's
     * nonzero value on the server, but we set it explicitly on both client and
     * server contexts). Applied to the default transport parameters before any
     * connection is created, so every cnx (the client cnx below, or inbound
     * server cnxs) inherits it. A configure_quic_fn override still runs after
     * and may change it. */
    picoquic_set_default_tp_value(t->quic, picoquic_tp_max_datagram_frame_size,
                                  PICOQUIC_MAX_PACKET_SIZE);

    /* Advertise enough inbound QUIC receive credit for large MoQ objects. MoQ
     * subgroup objects arrive on unidirectional streams; picoquic's defaults
     * (initial_max_stream_data_uni = 65535, initial_max_data = 1 MiB) cap how
     * much of an inbound object can be received before the stream blocks on
     * flow control. Raise both to match the session's receive budget so a
     * large object is admitted in full. Applied to the default TPs before any
     * cnx is created (the client cnx below and inbound server cnxs inherit
     * it); a configure_quic_fn override still runs after and may change it. */
    picoquic_set_default_tp_value(t->quic,
        picoquic_tp_initial_max_stream_data_uni, MOQ_PQ_RECV_FLOW_CONTROL);
    picoquic_set_default_tp_value(t->quic,
        picoquic_tp_initial_max_data, MOQ_PQ_RECV_FLOW_CONTROL);

    /* QUIC idle timeout (cfg.idle_timeout_ms): defense-in-depth for peers
     * that vanish WITHOUT a CONNECTION_CLOSE -- picoquic reaps the quiet
     * connection after this long and the adapter surfaces SESSION_CLOSED
     * through the terminal-conn observation window. Graceful peers should
     * still close at the MoQ/transport level; aggressive values can kill
     * legitimately quiet-but-live connections (keepalive is a separate
     * knob, not implied here). Applied to the context default before any
     * cnx is created; a configure_quic_fn override still runs after and
     * may change it. 0 = picoquic's default (~30s). */
    if (CFG_HAS(cfg, idle_timeout_ms) && cfg->idle_timeout_ms)
        picoquic_set_default_idle_timeout(t->quic, cfg->idle_timeout_ms);

    if (t->insecure_skip_verify)
        picoquic_set_null_verifier(t->quic);

    if (t->configure_quic_fn) {
        if (t->configure_quic_fn(t->quic, t->configure_quic_ctx) != 0)
            goto fail_configure;
    }

    /* ---- CLIENT: create connection + session + adapter ---- */

    if (persp == MOQ_PERSPECTIVE_CLIENT) {
        struct addrinfo hints, *res = NULL;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        char port_str[8];
        snprintf(port_str, sizeof(port_str), "%d", port);
        if (getaddrinfo(cfg->host, port_str, &hints, &res) != 0 || !res)
            goto fail_configure;

        /* With an ALPN offer list the connection starts with a NULL ALPN
         * (picoquic asks client_callback for the proposal list) and the
         * session is created at the ready callback once the negotiated
         * ALPN is known; _session() returns NULL until then. The legacy
         * single-ALPN path keeps the eager draft-16 session. The SNI is
         * carried separately from the host (TLS server-name + verifier). */
        picoquic_cnx_t *cnx = picoquic_create_client_cnx(
            t->quic, res->ai_addr, now, 0,
            sni, t->alpn_count > 0 ? NULL : MOQ_PQ_ALPN_DEFAULT,
            client_callback, t);
        freeaddrinfo(res);
        if (!cnx) goto fail_configure;

        if (t->alpn_count > 0) {
            t->client_deferred = true;
            pthread_mutex_lock(&t->mutex);
            t->active_cnx = cnx;
            pthread_mutex_unlock(&t->mutex);
        } else {
            moq_session_cfg_t scfg;
            build_session_cfg(t, &scfg, MOQ_PERSPECTIVE_CLIENT);

            moq_session_t *session = NULL;
            if (moq_session_create(&scfg, now, &session) != MOQ_OK)
                goto fail_configure;

            moq_pq_conn_cfg_t acfg;
            moq_pq_conn_cfg_init_sized(&acfg, sizeof(acfg));
            acfg.session = session;
            acfg.cnx = cnx;
            acfg.alloc = &t->alloc;
            moq_pq_conn_t *conn = NULL;
            if (moq_pq_conn_create(&acfg, &conn) != 0) {
                moq_session_destroy(session);
                goto fail_configure;
            }

            if (moq_session_start(session, now) != MOQ_OK ||
                moq_pq_service(conn, now) < 0) {
                moq_pq_conn_destroy(conn);
                moq_session_destroy(session);
                goto fail_configure;
            }

            pthread_mutex_lock(&t->mutex);
            t->session = session;
            t->conn = conn;
            t->active_cnx = cnx;
            t->negotiated = MOQ_VERSION_DRAFT_16;
            pthread_mutex_unlock(&t->mutex);
        }
    }

    /* ---- Common: start network thread ----
     * The packet loop opens its sockets ON the network thread; a bind
     * failure there (e.g. the dual-stack ephemeral pairing losing its
     * v6 port to another process) kills the loop BEFORE the ready
     * callback with no error surfaced to the creator, leaving a zombie
     * facade that never connects and never reports. Wait (bounded) for
     * the loop's ready flag and retry the start on a startup death --
     * a client rebind draws a fresh ephemeral port pair; a server with
     * a genuinely taken port fails create() so the caller can act. */
    memset(&t->loop_param, 0, sizeof(t->loop_param));
    if (persp == MOQ_PERSPECTIVE_SERVER)
        t->loop_param.local_port = (uint16_t)port;

    picoquic_network_thread_ctx_t *tctx = NULL;
    for (int attempt = 0; attempt < 3 && !tctx; attempt++) {
        pthread_mutex_lock(&t->mutex);
        t->loop_came_up = false;
        pthread_mutex_unlock(&t->mutex);
        int thread_ret = 0;
        picoquic_network_thread_ctx_t *cand =
            picoquic_start_network_thread(t->quic, &t->loop_param,
                loop_callback, t, &thread_ret);
        if (!cand) continue;
        /* Wait on OUR ready latch, not thread_ctx->thread_is_ready: the
         * loop clears that flag again when it exits, so a loop that came
         * up and exited promptly (e.g. on_pump requested exit on the
         * first cycle -- a legitimate lifecycle) would read as a failed
         * start. The latch only ever sets; return_code != 0 is the
         * pre-ready death signal. Socket open is a handful of syscalls;
         * 2s is a generous bound. */
        bool came_up = false;
        for (int waited_ms = 0; waited_ms < 2000; waited_ms++) {
            pthread_mutex_lock(&t->mutex);
            came_up = t->loop_came_up;
            pthread_mutex_unlock(&t->mutex);
            if (came_up || cand->return_code != 0) break;
            usleep(1000);
        }
        if (came_up)
            tctx = cand;
        else
            picoquic_delete_network_thread(cand);
    }
    if (!tctx) {
        moq_pq_conn_t *fc = NULL;
        moq_session_t *fs = NULL;
        pthread_mutex_lock(&t->mutex);
        fc = t->conn;    t->conn = NULL;
        fs = t->session; t->session = NULL;
        pthread_mutex_unlock(&t->mutex);
        if (fc) moq_pq_conn_destroy(fc);
        if (fs) moq_session_destroy(fs);
        goto fail_configure;
    }

    pthread_mutex_lock(&t->mutex);
    t->thread_ctx = tctx;
    pthread_mutex_unlock(&t->mutex);

    *out = t;
    return MOQ_OK;

fail_configure:
    picoquic_free(t->quic);
    t->quic = NULL;
fail_quic:
fail_cfg_strings:
    free_alpn_list(t);
    pthread_cond_destroy(&t->cond);
    pthread_mutex_destroy(&t->mutex);
    t->alloc.free(t, sizeof(*t), t->alloc.ctx);
    return MOQ_ERR_INTERNAL;
}

/* ------------------------------------------------------------------ */
/* stop                                                                */
/* ------------------------------------------------------------------ */

moq_result_t moq_pq_threaded_stop(moq_pq_threaded_t *t)
{
    if (!t) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&t->mutex);

    /* Detect stop from network thread. */
    if (t->network_thread_id_set &&
        pthread_equal(pthread_self(), t->network_thread_id)) {
        pthread_mutex_unlock(&t->mutex);
        return MOQ_ERR_WRONG_STATE;
    }

    if (t->stopped) {
        bool was_fatal = t->fatal;
        pthread_mutex_unlock(&t->mutex);
        return was_fatal ? MOQ_ERR_CLOSED : MOQ_OK;
    }

    picoquic_network_thread_ctx_t *ctx = t->thread_ctx;
    t->thread_ctx = NULL;
    t->stopped = true;
    t->activity_pending = true;
    pthread_cond_broadcast(&t->cond);

    while (t->wake_in_flight > 0)
        pthread_cond_wait(&t->cond, &t->mutex);

    pthread_mutex_unlock(&t->mutex);

    if (ctx) {
        /* Pipe-wake the loop so it observes t->stopped, and wait for the
         * acknowledgment BEFORE picoquic_delete_network_thread: delete
         * closes the wake pipe ahead of the join, and closing a polled
         * fd is not a reliable wakeup on every platform -- the byte just
         * written would die with the pipe and the join would sleep out
         * the loop's natural timer (up to 10s on a quiescent
         * connection). A loop that already exited (fatal / pump_exit)
         * never acknowledges; those flags satisfy the wait, and a timed
         * backstop covers any exit path that bypasses the callback. */
        (void)picoquic_wake_up_network_thread(ctx);
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 3;
        pthread_mutex_lock(&t->mutex);
        while (!t->loop_exited && !t->fatal && !t->pump_exit) {
            if (pthread_cond_timedwait(&t->cond, &t->mutex,
                                       &deadline) == ETIMEDOUT)
                break;
        }
        pthread_mutex_unlock(&t->mutex);
        picoquic_delete_network_thread(ctx);
    }

    /* Characterization telemetry: the network thread is joined, so the loop
     * counters and picoquic state are race-free to read here (conns are still
     * alive; freed later in destroy). One compact, parseable line to stderr. */
    if (t->stats_enabled) {
        /* Seed from conns pruned before stop, then fold in the live conns. */
        moq_pq_send_stats_t agg, one;
        agg.prepare_count = t->st_acc_prepare;
        agg.provided_bytes = t->st_acc_provided;
        agg.queue_would_block = t->st_acc_would_block;
        agg.queue_high_water = t->st_acc_high_water;
        picoquic_cnx_t *rep = NULL;
        if (t->perspective == MOQ_PERSPECTIVE_SERVER) {
            for (size_t i = 0; i < t->conn_count; i++) {
                moq_pq_conn_get_send_stats(t->conns[i]->conn, &one);
                agg.prepare_count += one.prepare_count;
                agg.provided_bytes += one.provided_bytes;
                agg.queue_would_block += one.queue_would_block;
                if (one.queue_high_water > agg.queue_high_water)
                    agg.queue_high_water = one.queue_high_water;
                if (!rep) rep = t->conns[i]->cnx;
            }
        } else {
            moq_pq_conn_get_send_stats(t->conn, &one);
            agg.prepare_count += one.prepare_count;
            agg.provided_bytes += one.provided_bytes;
            agg.queue_would_block += one.queue_would_block;
            if (one.queue_high_water > agg.queue_high_water)
                agg.queue_high_water = one.queue_high_water;
            rep = t->active_cnx;
        }
        picoquic_path_quality_t q; memset(&q, 0, sizeof(q));
        if (rep) picoquic_get_default_path_quality(rep, &q);
        /* pkts/positive-send is the real cadence signal: no-output loop turns
         * (after_send with 0 packets) do not dilute it. */
        double ppw = t->st_after_send ?
            (double)t->st_pkts_total / (double)t->st_after_send : 0.0;
        double ppp = t->st_after_send_nz ?
            (double)t->st_pkts_total / (double)t->st_after_send_nz : 0.0;
        fprintf(stderr,
            "MOQ_PQ_STATS role=%s after_send=%llu after_send_nonzero=%llu "
            "after_send_bytes=%llu pkts=%llu pkts_per_send_avg=%.2f "
            "pkts_per_positive_send_avg=%.2f pkts_per_send_max=%llu "
            "after_recv=%llu wake=%llu service_all=%llu "
            "prepare=%llu provided=%llu q_high_water=%llu q_would_block=%llu "
            "cwin=%llu rtt_us=%llu pacing_Bps=%llu bytes_sent=%llu\n",
            t->perspective == MOQ_PERSPECTIVE_SERVER ? "server" : "client",
            (unsigned long long)t->st_after_send,
            (unsigned long long)t->st_after_send_nz,
            (unsigned long long)t->st_after_send_bytes,
            (unsigned long long)t->st_pkts_total, ppw, ppp,
            (unsigned long long)t->st_pkts_max_burst,
            (unsigned long long)t->st_after_recv,
            (unsigned long long)t->st_wake,
            (unsigned long long)t->st_service_all,
            (unsigned long long)agg.prepare_count,
            (unsigned long long)agg.provided_bytes,
            (unsigned long long)agg.queue_high_water,
            (unsigned long long)agg.queue_would_block,
            (unsigned long long)q.cwin, (unsigned long long)q.rtt,
            (unsigned long long)q.pacing_rate,
            (unsigned long long)q.bytes_sent);
    }

    pthread_mutex_lock(&t->mutex);
    bool was_fatal = t->fatal;
    pthread_mutex_unlock(&t->mutex);

    return was_fatal ? MOQ_ERR_CLOSED : MOQ_OK;
}

/* ------------------------------------------------------------------ */
/* destroy                                                             */
/* ------------------------------------------------------------------ */

void moq_pq_threaded_destroy(moq_pq_threaded_t *t)
{
    if (!t) return;

    /* The network thread is already stopped and joined (moq_pq_threaded_stop),
     * so nothing races this teardown. Server connections: destroy each record
     * (unbinds its cnx callback); the picoquic_free below closes/deletes the
     * cnx objects, so no ordered close is needed here. The legacy client
     * single conn (t->conn/t->session) is a mirror of conns[0] in server mode,
     * so only destroy it directly in client mode to avoid a double free. */
    if (t->perspective == MOQ_PERSPECTIVE_SERVER) {
        for (size_t i = 0; i < t->conn_count; i++)
            server_conn_free(t, t->conns[i], false, 0);
        t->conn_count = 0;
    } else {
        if (t->conn) moq_pq_conn_destroy(t->conn);
        if (t->session) moq_session_destroy(t->session);
    }
    if (t->conns)
        t->alloc.free(t->conns, t->conn_cap * sizeof(*t->conns), t->alloc.ctx);
    if (t->quic)
        picoquic_free(t->quic);
    free_alpn_list(t);

    pthread_cond_destroy(&t->cond);
    pthread_mutex_destroy(&t->mutex);
    moq_alloc_t alloc = t->alloc;
    alloc.free(t, sizeof(*t), alloc.ctx);
}

/* ------------------------------------------------------------------ */
/* Accessors                                                           */
/* ------------------------------------------------------------------ */

moq_session_t *moq_pq_threaded_session(moq_pq_threaded_t *t)
{
    if (!t) return NULL;
    pthread_mutex_lock(&t->mutex);
    moq_session_t *s = t->session;
    pthread_mutex_unlock(&t->mutex);
    return s;
}

moq_version_t moq_pq_threaded_negotiated_version(const moq_pq_threaded_t *t)
{
    if (!t) return (moq_version_t)0;
    moq_pq_threaded_t *mt = (moq_pq_threaded_t *)t;
    pthread_mutex_lock(&mt->mutex);
    moq_version_t v = t->negotiated;
    pthread_mutex_unlock(&mt->mutex);
    return v;
}

moq_pq_conn_t *moq_pq_threaded_conn(moq_pq_threaded_t *t)
{
    if (!t) return NULL;
    pthread_mutex_lock(&t->mutex);
    moq_pq_conn_t *c = t->conn;
    pthread_mutex_unlock(&t->mutex);
    return c;
}

/* -- Per-connection API (server) --------------------------------------- *
 * next_conn / conn_session / conn_close are network-thread-only (inside
 * on_pump), where conns[] is stable (only this thread appends/prunes, and
 * prune runs after on_pump returns) -- so they need no lock. Off-thread (or
 * post-teardown) callers are rejected via pq_on_network_thread() to avoid
 * racing prune/realloc/free. conn_count is a cross-thread read, taken under
 * the mutex. */

/* True only for the picoquic loop thread. The id is published once the loop
 * starts; the loop thread observes its own write, so it always matches. Any
 * other thread sees the id unset or mismatched -- both reject -- so no false
 * accept is possible and, unlike drain_state's locked check, no lock is
 * needed here (a racy read can only produce a spurious reject, never a
 * false accept). */
static bool pq_on_network_thread(const moq_pq_threaded_t *t)
{
    return t->network_thread_id_set &&
           pthread_equal(pthread_self(), t->network_thread_id);
}

moq_pq_threaded_conn_t *moq_pq_threaded_next_conn(moq_pq_threaded_t *t,
                                                  moq_pq_threaded_conn_t *prev)
{
    if (!t || !pq_on_network_thread(t) || t->conn_count == 0) return NULL;
    if (!prev) return t->conns[0];
    for (size_t i = 0; i + 1 < t->conn_count; i++)
        if (t->conns[i] == prev) return t->conns[i + 1];
    return NULL;   /* prev was the last live conn, or is stale */
}

moq_session_t *moq_pq_threaded_conn_session(moq_pq_threaded_conn_t *conn)
{
    if (!conn || !pq_on_network_thread(conn->parent)) return NULL;
    return conn->session;
}

moq_result_t moq_pq_threaded_conn_close(moq_pq_threaded_conn_t *conn,
                                        uint64_t error_code)
{
    if (!conn || !pq_on_network_thread(conn->parent)) return MOQ_ERR_INVAL;
    /* Deferred: the record is pruned (with an ordered CONNECTION_CLOSE) after
     * the current on_pump returns, so next_conn() iteration stays valid. */
    conn->close_requested = true;
    conn->close_error_code = error_code;
    return MOQ_OK;
}

size_t moq_pq_threaded_conn_count(const moq_pq_threaded_t *t)
{
    if (!t) return 0;
    moq_pq_threaded_t *mt = (moq_pq_threaded_t *)t;
    pthread_mutex_lock(&mt->mutex);
    size_t n = mt->conn_count;
    pthread_mutex_unlock(&mt->mutex);
    return n;
}

/* Graceful-drain probe for moq_endpoint_drain()'s backend vtable. 1 = local
 * stream flush done (no queued/ready stream data or unsent FIN; NOT a packet-ACK
 * check), 0 = still draining, -2 = called on the network thread. Computed on the
 * network thread (loop) and read here under the mutex. */
int moq_pq_threaded_drain_state(moq_pq_threaded_t *t)
{
    if (!t) return 0;
    pthread_mutex_lock(&t->mutex);
    if (t->network_thread_id_set &&
        pthread_equal(pthread_self(), t->network_thread_id)) {
        pthread_mutex_unlock(&t->mutex);
        return -2;
    }
    int drained = t->tx_drained ? 1 : 0;
    pthread_mutex_unlock(&t->mutex);
    return drained;
}

bool moq_pq_threaded_is_fatal(const moq_pq_threaded_t *t)
{
    if (!t) return false;
    moq_pq_threaded_t *mt = (moq_pq_threaded_t *)t;
    pthread_mutex_lock(&mt->mutex);
    bool f = mt->fatal;
    pthread_mutex_unlock(&mt->mutex);
    return f;
}

uint64_t moq_pq_threaded_fatal_code(const moq_pq_threaded_t *t)
{
    if (!t) return 0;
    moq_pq_threaded_t *mt = (moq_pq_threaded_t *)t;
    pthread_mutex_lock(&mt->mutex);
    uint64_t c = mt->fatal_code;
    pthread_mutex_unlock(&mt->mutex);
    return c;
}

/* ------------------------------------------------------------------ */
/* wake                                                                */
/* ------------------------------------------------------------------ */

moq_result_t moq_pq_threaded_wake(moq_pq_threaded_t *t)
{
    if (!t) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&t->mutex);
    if (t->stopped || t->fatal || t->pump_exit || !t->thread_ctx) {
        pthread_mutex_unlock(&t->mutex);
        return MOQ_ERR_CLOSED;
    }
    if (t->wake_pending) {
        pthread_mutex_unlock(&t->mutex);
        return MOQ_OK;
    }
    t->wake_pending = true;
    t->wake_in_flight++;
    picoquic_network_thread_ctx_t *ctx = t->thread_ctx;
    pthread_mutex_unlock(&t->mutex);

    int rc = picoquic_wake_up_network_thread(ctx);

    pthread_mutex_lock(&t->mutex);
    t->wake_in_flight--;
    if (t->wake_in_flight == 0)
        pthread_cond_broadcast(&t->cond);
    if (rc != 0) {
        t->wake_pending = false;
        pthread_mutex_unlock(&t->mutex);
        return MOQ_ERR_INTERNAL;
    }
    pthread_mutex_unlock(&t->mutex);
    return MOQ_OK;
}

/* ------------------------------------------------------------------ */
/* wait                                                                */
/* ------------------------------------------------------------------ */

moq_result_t moq_pq_threaded_wait(moq_pq_threaded_t *t,
                                    uint64_t timeout_us)
{
    if (!t) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&t->mutex);

    if (t->stopped || t->fatal || t->pump_exit) {
        pthread_mutex_unlock(&t->mutex);
        return MOQ_ERR_CLOSED;
    }
    if (t->activity_pending) {
        t->activity_pending = false;
        pthread_mutex_unlock(&t->mutex);
        return MOQ_OK;
    }

    if (timeout_us == 0) {
        pthread_mutex_unlock(&t->mutex);
        return MOQ_DONE;
    }

    struct timespec deadline;
    bool has_deadline = (timeout_us != UINT64_MAX);
    if (has_deadline) {
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec  += (time_t)(timeout_us / 1000000ULL);
        deadline.tv_nsec += (long)((timeout_us % 1000000ULL) * 1000ULL);
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
    }

    while (!t->activity_pending && !t->stopped &&
           !t->fatal && !t->pump_exit) {
        if (has_deadline) {
            int wrc = pthread_cond_timedwait(&t->cond, &t->mutex,
                                              &deadline);
            if (wrc == ETIMEDOUT) break;
        } else {
            pthread_cond_wait(&t->cond, &t->mutex);
        }
    }

    moq_result_t result;
    if (t->stopped || t->fatal || t->pump_exit) {
        result = MOQ_ERR_CLOSED;
    } else if (t->activity_pending) {
        t->activity_pending = false;
        result = MOQ_OK;
    } else {
        result = MOQ_DONE;
    }
    pthread_mutex_unlock(&t->mutex);
    return result;
}
