/*
 * Managed MoQ-over-MsQuic facade.
 *
 * Threading: connections are partitioned across `lane_count` lanes, each
 * an independent lock domain — its own mutex + doorbell — owning a set of
 * connections. MsQuic delivers a connection's events serialized on its
 * own workers; the attach adapter brackets every one of those callbacks
 * with the connection's LANE mutex (guard_enter/guard_leave), and that
 * lane's doorbell takes the same mutex for wake-requested and
 * deadline-driven service cycles. on_lane_pump therefore always runs
 * under the owning lane's mutex — from a worker after a transport batch,
 * or from the lane doorbell — and those two contexts exclude each other.
 * Distinct lanes run concurrently and never take each other's mutexes.
 * There is no facade network thread and no event marshaling: the
 * transport path runs where MsQuic put it. Lock order is a lane mutex
 * (lane->mu) before the facade mutex (m->mu), never the reverse; each
 * lane's bell_mu (the wake-only signaling lock) is a strict leaf below
 * both — nothing is ever acquired while holding it.
 *
 * Teardown ordering (the part that can deadlock if done wrong):
 * MsQuic's ConnectionClose/ListenerClose BLOCK until the handle's
 * callbacks drain, and those callbacks take a lane mutex — so the
 * blocking closes must never run with a lane mutex held. stop() records
 * the request, then per lane issues the async ConnectionShutdown under
 * that lane's mutex and WAITS (condvar, mutex released) for each
 * connection's SHUTDOWN_COMPLETE — the last event MsQuic ever delivers
 * for it — then joins every doorbell and only then calls the blocking
 * closes, lock-free. destroy() closes registration/table after stop().
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "msquic_internal.h"

#include <moq/msquic_managed.h>

#include "../common/moq_alpn.h"

#ifdef MOQ_MSQUIC_TESTING
#include "tests/support/msq_test_seams.h"
#endif

/* Which facade's callback context this thread is currently inside (the
 * one of this facade's lane mutexes is held whenever this matches). Set
 * for the whole transport batch (guard) and around doorbell service
 * cycles, so on_lane_pump AND on_activity are both covered. Lets the
 * public entry points tell "already inside my callbacks" apart from
 * "external thread" without racy field reads. Managed callbacks never
 * nest across facades on one thread (MsQuic never upcalls inline from
 * downcalls), so a plain set/clear suffices. */
static _Thread_local moq_msquic_managed_t *mgd_tls_cb;
/* The lane whose pump/guard is running on this thread (NULL otherwise).
 * lane_next_conn gates on this so a pump only ever sees its own lane. */
static _Thread_local moq_msquic_managed_lane_t *mgd_tls_lane;

/* One accepted (or, for a client, the one initiated) connection: its
 * own MoQ session + attach adapter conn, serialized by its OWNING
 * LANE's mutex (the attach guard points at the conn, which carries its
 * lane). Freed only after SHUTDOWN_COMPLETE — MsQuic's last event —
 * and outside all locks (ConnectionClose blocks on callback drain). */
struct moq_msquic_managed_conn {
    struct moq_msquic_managed_conn *next;
    moq_msquic_managed_t *owner;
    moq_msquic_managed_lane_t *lane; /* owning lane; set at accept, stable */
    HQUIC connection;
    moq_session_t *session;
    moq_msquic_conn_t *conn;
    moq_version_t negotiated_version;
    void *user; /* application slot; the facade never touches it */
    bool reapable; /* the TRANSPORT half only: this connection's terminal
                      completed. Reclamation additionally needs the
                      application half for a server child — see
                      mgd_child_reclaimable. */
    bool app_terminal_acked; /* the application asserted, from this
                                connection's own lane pump, that it is done
                                with the child. A SERVER child is not
                                reclaimed until this is set; the single
                                client connection does not use it. */
    uint64_t pump_pre_token; /* doorbell scratch: event-progress token snapshot
                                taken before on_lane_pump, for the re-drive
                                predicate after the post-pump service pass */
};

/* A lane: an independent lock domain + doorbell owning a set of
 * connections. Distinct lanes never take each other's locks. This is a
 * copy of the single-facade machinery, one per lane. Lock order across
 * the facade is ALWAYS lane->mu before m->mu, never the reverse. */
struct moq_msquic_managed_lane {
    moq_msquic_managed_t *owner;
    uint32_t index;
    moq_msquic_managed_conn_t *conns; /* this lane's conns, oldest first */
    size_t conn_count;

    pthread_mutex_t mu;
    pthread_cond_t cv;
    pthread_t doorbell;
    bool doorbell_running;

    bool wake_pending;      /* coalesced app wake for this lane */
    bool pump_pending;      /* coalesced app pump, armed by worker batches */
    /* lock-free cross-lane wake: set (without lane->mu) by a wake issued
     * from a DIFFERENT lane's callback, where taking lane->mu could
     * deadlock. The doorbell consumes it in its predicate; the bell below
     * delivers it PROMPTLY, and the bounded idle wait remains only a
     * robustness backstop. */
    atomic_bool ext_wake;
    /* The bell: a dedicated wake-only signaling domain, independent of
     * lane->mu, so every wake source — including the cross-lane arm,
     * which must never touch lane->mu — can deliver a durable prompt
     * signal to the doorbell. bell_mu is a strict LEAF lock: no code
     * path acquires ANY other lock while holding it, so ringing it from
     * inside another lane's locked callback cannot create a lock-order
     * cycle. bell_gen is guarded by bell_mu; the doorbell snapshots it
     * BEFORE its work predicate and re-checks it under bell_mu before
     * sleeping, so a ring landing anywhere in the check->wait window
     * turns the wait into an immediate return — a wake can coalesce,
     * never be lost. */
    pthread_mutex_t bell_mu;
    pthread_cond_t bell_cv;
    uint64_t bell_gen;
#ifdef MOQ_MSQUIC_TESTING
    /* test-only: doorbell idle waits that returned because the generation
     * advanced (a ring delivered a wake) rather than the deadline expiring.
     * The structural discriminator for prompt wake delivery — a bell wake,
     * not a cap timeout. Never present in production builds. */
    _Atomic uint64_t st_bell_gen_wakes;
#endif
    bool in_sweep;          /* a pump/service sweep is running on this lane */
    /* True ONLY while cfg.on_lane_pump is executing (a strict subset of
     * in_sweep, which also spans service + on_activity). The per-connection
     * lane API is valid only in this window: on_activity runs on the same
     * doorbell thread with mgd_tls_lane still set and the sweep snapshot
     * still live, but with in_lane_pump cleared, so it cannot iterate the
     * lane or touch its conns. Written/read only by the doorbell thread
     * under lane->mu (paired with mgd_tls_lane, the thread marker). */
    bool in_lane_pump;
    bool pump_exit;         /* on_lane_pump returned nonzero */
    bool stop_requested;    /* stop() asked this lane's doorbell to quiesce */

    /* sweep snapshot (the conns whose lane->mu the running pump holds);
     * lane_next_conn iterates this. Buffer sized to max_connections. */
    moq_msquic_managed_conn_t **sweep;
    size_t sweep_count;
    moq_msquic_managed_conn_t **sweep_buf;

    /* m->mu-protected aggregate mirrors (this lane's contribution to the
     * facade pending probes), refreshed by the pump under lane->mu then
     * m->mu — so the facade accessors never lock a lane. */
    int m_pending_sends;
    int m_pending_dgrams;

    /* --- doorbell statistics (moq_msquic_lane_get_stats) ------------- *
     * Plain fields are written only under lane->mu (doorbell context or a
     * mutex-holding wake arm). The cross-lane wake arm cannot take the
     * mutex, so its counters and the pending-cycle stamp are C11 atomics:
     * st_wake_stamp_us is the OPEN-CYCLE marker — the first accepted
     * explicit wake CASes it from 0 to now (an arm that loses the CAS
     * found an open cycle -> st_wakes_coalesced); the consuming pump
     * exchanges it back to 0 and records exactly one latency sample. */
    uint64_t st_wakes_same_lane;
    uint64_t st_wakes_external;
    _Atomic uint64_t st_wakes_cross_lane;
    _Atomic uint64_t st_wakes_coalesced;
    _Atomic uint64_t st_wake_stamp_us;
    uint64_t st_pump_sweeps;
    uint64_t st_deadline_sweeps;
#ifdef MOQ_MSQUIC_TESTING
    uint64_t st_deadline_queries;
    uint64_t st_deadline_class_none;
    uint64_t st_deadline_class_future;
    uint64_t st_deadline_class_due;
#endif
    uint64_t st_idle_cap_wakes;
    uint64_t st_wake_to_pump_max_us;
    uint64_t st_wake_to_pump_total_us;
    uint64_t st_wake_to_pump_samples;
    uint64_t st_service_passes;
    /* flush totals of RECLAIMED children, latched under lane->mu before
     * the child leaves the list; live children are summed at read time */
    uint64_t st_flush_sends_reaped;
    uint64_t st_flush_bytes_reaped;
};

struct moq_msquic_managed {
    moq_alloc_t alloc;
    moq_msquic_managed_cfg_t cfg; /* shallow; strings owned below */
    moq_version_t version;        /* exact endpoint version, or 0 for multi-server */
    moq_version_t versions[MOQ_MSQUIC_MANAGED_MAX_VERSIONS];
    size_t version_count;
    QUIC_BUFFER alpns[MOQ_MSQUIC_MANAGED_MAX_VERSIONS];
    uint32_t alpn_count;
    size_t max_connections;       /* resolved cap (0 -> 1), facade-wide */
    char *host;
    char *cert_path;
    char *key_path;

    const QUIC_API_TABLE *api; /* owned via MsQuicOpen2 unless test-borrowed */
#ifdef MOQ_MSQUIC_TESTING
    bool api_borrowed; /* caller-owned table; never passed to MsQuicClose */
    /* The no-doorbell decision, FROZEN once per facade at create. Read
     * per lane it could produce a half-threaded facade if the control
     * changed mid-creation, so the loop consults this, never the global. */
    bool doorbell_suppressed;
#endif
    HQUIC registration;
    HQUIC configuration;
    HQUIC listener;

    moq_msquic_managed_lane_t *lanes; /* [lane_count], owned */
    uint32_t lane_count;
    uint32_t next_lane;               /* round-robin cursor (under m->mu) */
    size_t conn_count;                /* facade-wide reserve (under m->mu) */
    bool draining;                    /* refuse new accepts */

    /* CLIENT compat latch: the single client connection's terminal,
     * latched when it becomes reapable (client-only accessors). */
    bool term_fatal;
    bool term_closed;
    uint64_t term_fatal_code;
    uint64_t term_close_code;

    /* facade-level coordination (cross-lane): stop barrier, wait(), the
     * reserve counter, activity signalling. */
    pthread_mutex_t mu;
    pthread_cond_t cv;

    /* facade stopping. Atomic because the lane doorbells read it from
     * their own lane domain WITHOUT m->mu; cross-domain access uses
     * release stores (writers, under m->mu) and acquire loads (readers)
     * to stay data-race-free. */
    atomic_bool stop_requested;
    bool stopped;
    bool activity_pending;  /* coalesced for wait() */
    bool pump_exit;         /* any lane's on_lane_pump returned nonzero */
    uint16_t bound_port;
};

static uint64_t mgd_now_us(void)
{
#ifdef MOQ_MSQUIC_TESTING
    if (moq_msq_test_now_us != NULL)
        return moq_msq_test_now_us();
#endif
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static char *mgd_strdup(const moq_alloc_t *alloc, const char *s)
{
    if (s == NULL)
        return NULL;
    size_t n = strlen(s) + 1;
    char *d = alloc->alloc(n, alloc->ctx);

    if (d != NULL)
        memcpy(d, s, n);
    return d;
}

static void mgd_strfree(const moq_alloc_t *alloc, char *s)
{
    if (s != NULL)
        alloc->free(s, strlen(s) + 1, alloc->ctx);
}

#define MGD_CFG_FIELD_END(field) \
    (offsetof(moq_msquic_managed_cfg_t, field) + \
     sizeof(((moq_msquic_managed_cfg_t *)0)->field))

static bool mgd_cfg_covers_field(size_t struct_size, size_t field_end)
{
    return struct_size >= field_end;
}

static bool mgd_cfg_has_version_list_block(const moq_msquic_managed_cfg_t *cfg)
{
    return mgd_cfg_covers_field(cfg->struct_size,
                                MGD_CFG_FIELD_END(version_count));
}

static bool mgd_version_list_contains(const moq_version_t *versions,
                                      size_t count, moq_version_t version)
{
    for (size_t i = 0; i < count; i++)
        if (versions[i] == version)
            return true;
    return false;
}

static moq_result_t mgd_validate_version_list(
    const moq_version_t *versions, size_t count)
{
    if (versions == NULL || count == 0 ||
        count > MOQ_MSQUIC_MANAGED_MAX_VERSIONS)
        return MOQ_ERR_INVAL;

    for (size_t i = 0; i < count; i++) {
        if (moq_alpn_for_version(versions[i]) == NULL)
            return MOQ_ERR_UNSUPPORTED;
        for (size_t j = 0; j < i; j++)
            if (versions[i] == versions[j])
                return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}

static moq_result_t mgd_store_alpns(moq_msquic_managed_t *m,
                                    const moq_version_t *versions,
                                    size_t count)
{
    if (count == 0 || count > MOQ_MSQUIC_MANAGED_MAX_VERSIONS)
        return MOQ_ERR_INVAL;

    m->version_count = count;
    m->alpn_count = (uint32_t)count;
    for (size_t i = 0; i < count; i++) {
        const char *alpn = moq_alpn_for_version(versions[i]);

        if (alpn == NULL)
            return MOQ_ERR_UNSUPPORTED;
        m->versions[i] = versions[i];
        m->alpns[i].Length = (uint32_t)strlen(alpn);
        m->alpns[i].Buffer = (uint8_t *)(uintptr_t)alpn;
    }
    return MOQ_OK;
}

static bool mgd_offered_version(const moq_msquic_managed_t *m,
                                moq_version_t version)
{
    return mgd_version_list_contains(m->versions, m->version_count, version);
}

/* Bump facade activity for wait(). Takes m->mu briefly; callers hold at
 * most a lane->mu (lane->mu -> m->mu order). */
static void mgd_bump_activity(moq_msquic_managed_t *m)
{
    pthread_mutex_lock(&m->mu);
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
    if (m->cfg.on_activity != NULL)
        m->cfg.on_activity(m, m->cfg.on_activity_ctx);
}

/* --- guard: the attach adapter brackets every transport callback ---- *
 * Per-connection lock domain: a worker takes the connection's LANE
 * mutex, so distinct lanes' callbacks run concurrently. */

#ifdef MOQ_MSQUIC_TESTING
/* Test-only: the transport guard is about to take this lane's mutex. Runs on
 * the transport worker with no lane mutex held, so a test can rendezvous at
 * the exact lock boundary the guard serializes on, instead of at the caller's
 * intent to reach it. Never compiled into production libraries. */
void (*moq_msq_test_guard_pre_lock)(moq_msquic_managed_lane_t *lane);

/* Test-only: an external stats reader is about to take this lane's mutex.
 * Same purpose, for the application-thread arm of the snapshot. */
void (*moq_msq_test_stats_pre_lock)(moq_msquic_managed_lane_t *lane);

/* Test-only: an external snapshot is about to take this lane's mutex. Same
 * purpose again, for the seam's own reader -- a test can rendezvous exactly
 * at the lock boundary the snapshot serializes on, rather than at its intent
 * to reach it. Never compiled into production libraries. */
void (*moq_msq_test_snapshot_pre_lock)(moq_msquic_managed_lane_t *lane);
#endif

static void mgd_guard_enter(void *user)
{
    moq_msquic_managed_conn_t *mc = user;

#ifdef MOQ_MSQUIC_TESTING
    if (moq_msq_test_guard_pre_lock != NULL)
        moq_msq_test_guard_pre_lock(mc->lane);
#endif
    pthread_mutex_lock(&mc->lane->mu);
    mgd_tls_cb = mc->owner;
    /* mark this lane held for the whole batch, so lane_wake() (which the
     * app may call from on_activity) recognizes its own held mutex */
    mgd_tls_lane = mc->lane;
}

static void mgd_bell_ring(moq_msquic_managed_lane_t *lane);

static void mgd_guard_leave(void *user)
{
    moq_msquic_managed_conn_t *mc = user;
    moq_msquic_managed_lane_t *lane = mc->lane;

    /* a transport-event batch was just serviced on this lane: arm ONE
     * coalesced pump on the lane's doorbell (however many worker
     * batches, across the lane's connections, land before it runs) and
     * signal facade activity. The lane's own pump sweeps do not pass
     * through this guard, so a sweep cannot re-arm itself. Markers stay
     * set across mgd_bump_activity so a same-lane lane_wake() from
     * on_activity arms without re-locking. */
    lane->pump_pending = true;
    pthread_cond_broadcast(&lane->cv);
    mgd_bell_ring(lane);
    mgd_bump_activity(mc->owner);
    mgd_tls_lane = NULL;
    mgd_tls_cb = NULL;
    pthread_mutex_unlock(&lane->mu);
}

/* --- hook: per-connection service bookkeeping (lane->mu held) -------- *
 * The app's on_lane_pump does NOT run here — it runs coalesced on the
 * lane doorbell (guard_leave arms it). The hook only latches the
 * transport terminal and the client terminal compat view. */

static void mgd_hook(moq_msquic_conn_t *conn, void *user)
{
    moq_msquic_managed_conn_t *mc = user;
    moq_msquic_managed_t *m = mc->owner;

    (void)conn;
    /* Latch the TRANSPORT terminal, and only that. Whether the child may be
     * reclaimed is mgd_child_reclaimable's decision: for a server it also
     * needs the application's acknowledgment, so a terminal connection stays
     * visible to its lane's pump for as long as the application needs it. */
    if (!mc->reapable && mc->conn->shutdown_complete &&
        (moq_msquic_conn_is_fatal(mc->conn) ||
         moq_msquic_conn_is_closed(mc->conn))) {
        mc->reapable = true;
        /* CLIENT compat latch: the single connection's terminal */
        if (m->lane_count == 1 && mc == m->lanes[0].conns) {
            pthread_mutex_lock(&m->mu);
            m->term_fatal = moq_msquic_conn_is_fatal(mc->conn);
            m->term_closed = moq_msquic_conn_is_closed(mc->conn);
            m->term_fatal_code = moq_msquic_conn_fatal_code(mc->conn);
            m->term_close_code = moq_msquic_conn_close_code(mc->conn);
            pthread_mutex_unlock(&m->mu);
        }
    }
}

/* --- session + attach-conn construction ------------------------------- */

/* Create a child (session + attach conn) — NOT yet placed on a lane or
 * visible. The guard points at the child, so it takes the child's lane
 * mutex; the caller sets mc->lane and appends under lane->mu before any
 * callback can fire (i.e. before ConnectionSetConfiguration / Start). */
static moq_result_t mgd_make_child(moq_msquic_managed_t *m,
                                   moq_version_t negotiated_version,
                                   moq_msquic_managed_conn_t **out)
{
    moq_msquic_managed_conn_t *mc =
        m->alloc.alloc(sizeof(*mc), m->alloc.ctx);

    if (mc == NULL)
        return MOQ_ERR_NOMEM;
    memset(mc, 0, sizeof(*mc));
    mc->owner = m;
    mc->negotiated_version = negotiated_version;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &m->alloc,
                               m->cfg.perspective);
    scfg.version = negotiated_version; /* matches the negotiated ALPN */
    if (m->cfg.send_request_capacity)
        scfg.send_request_capacity = 1;
    if (m->cfg.initial_request_capacity != 0)
        scfg.initial_request_capacity = m->cfg.initial_request_capacity;
    if (m->cfg.max_events != 0)
        scfg.max_events = m->cfg.max_events;
    if (m->cfg.max_actions != 0)
        scfg.max_actions = m->cfg.max_actions;
    /* m->cfg is zeroed then min(struct_size,sizeof)-copied at create(), so a
     * prefix-sized caller reads false here. */
    if (m->cfg.streaming_objects)
        scfg.streaming_objects = true;
    if (m->cfg.session_idle_timeout_us != 0)
        scfg.idle_timeout_us = m->cfg.session_idle_timeout_us;
    /* Same prefix rule: m->cfg was zeroed then min(struct_size,sizeof)-copied,
     * so an old-sized caller's poison beyond its struct_size never reaches
     * here and the session keeps its default pool. */
    if (m->cfg.max_open_subgroups != 0)
        scfg.max_open_subgroups = m->cfg.max_open_subgroups;
    if (moq_session_create(&scfg, mgd_now_us(), &mc->session) < 0) {
        m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
        return MOQ_ERR_INTERNAL;
    }

    moq_msquic_conn_cfg_t ccfg;
    moq_msquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &m->alloc;
    ccfg.session = mc->session;
    ccfg.api = m->api;
    ccfg.hook = mgd_hook;
    ccfg.hook_user = mc;
    ccfg.guard_enter = mgd_guard_enter;
    ccfg.guard_leave = mgd_guard_leave;
    ccfg.guard_user = mc; /* the guard takes mc->lane->mu */
    moq_result_t rc = moq_msquic_conn_create(&ccfg, &mc->conn);
    if (rc != MOQ_OK) {
        moq_session_destroy(mc->session);
        m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
        return rc;
    }
    *out = mc;
    return MOQ_OK;
}

/* Append a child to its lane's list (lane->mu held). */
static void mgd_lane_append(moq_msquic_managed_lane_t *lane,
                            moq_msquic_managed_conn_t *mc)
{
    mc->lane = lane;
    moq_msquic_managed_conn_t **tail = &lane->conns;
    while (*tail != NULL)
        tail = &(*tail)->next;
    *tail = mc;
    lane->conn_count++;
}

/* Remove a child from its lane's list (lane->mu held). */
static void mgd_lane_remove(moq_msquic_managed_lane_t *lane,
                            moq_msquic_managed_conn_t *mc)
{
    for (moq_msquic_managed_conn_t **p = &lane->conns; *p != NULL;
         p = &(*p)->next)
        if (*p == mc) {
            *p = mc->next;
            mc->next = NULL;
            lane->conn_count--;
            break;
        }
}

/* Release the facade-wide reserve for a rejected/gone connection. */
static void mgd_release_reserve(moq_msquic_managed_t *m)
{
    pthread_mutex_lock(&m->mu);
    m->conn_count--;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
}

/* Free a fully quiesced child: SHUTDOWN_COMPLETE has been observed, so
 * no callback can fire — the blocking ConnectionClose and the adapter
 * teardown are safe. Must be called with NO locks held. */
static void mgd_free_child(moq_msquic_managed_t *m,
                           moq_msquic_managed_conn_t *mc)
{
    if (mc->connection != NULL)
        m->api->ConnectionClose(mc->connection);
    moq_msquic_conn_destroy(mc->conn);
    moq_session_destroy(mc->session);
    m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
}

/* --- listener (server) -------------------------------------------------- */

static QUIC_STATUS QUIC_API mgd_listener_cb(HQUIC listener, void *ctx,
                                            QUIC_LISTENER_EVENT *ev)
{
    moq_msquic_managed_t *m = ctx;

    (void)listener;
    if (ev->Type != QUIC_LISTENER_EVENT_NEW_CONNECTION)
        return QUIC_STATUS_SUCCESS;

    moq_version_t selected_version = 0;
    const QUIC_NEW_CONNECTION_INFO *info = ev->NEW_CONNECTION.Info;
    if (info == NULL || info->NegotiatedAlpn == NULL ||
        !moq_alpn_to_version((const char *)info->NegotiatedAlpn,
                             info->NegotiatedAlpnLength,
                             &selected_version) ||
        !mgd_offered_version(m, selected_version))
        return QUIC_STATUS_CONNECTION_REFUSED;

    /* (b) reserve facade capacity under m->mu, then RELEASE it — so
     * choose_lane and the lane append never run under m->mu (which
     * would reverse the lane->mu -> m->mu order). The reserve
     * (conn_count++) makes the cap race-free without a lane lock. */
    pthread_mutex_lock(&m->mu);
    if (m->conn_count >= m->max_connections || m->draining ||
        atomic_load_explicit(&m->stop_requested, memory_order_acquire)) {
        pthread_mutex_unlock(&m->mu);
        return QUIC_STATUS_CONNECTION_REFUSED;
    }
    m->conn_count++;
    uint32_t rr_lane = m->next_lane++ % m->lane_count; /* default pick */
    pthread_mutex_unlock(&m->mu);

    /* (c) choose the lane with NO locks held. An out-of-range chooser
     * return REFUSES the connection (no silent clamp). */
    uint32_t idx = rr_lane;
    if (m->cfg.choose_lane != NULL) {
        moq_msquic_accept_info_t info = { (uint32_t)sizeof(info) };
        idx = m->cfg.choose_lane(m, &info, m->cfg.choose_lane_user);
    }
    if (idx >= m->lane_count) {
        mgd_release_reserve(m);
        return QUIC_STATUS_CONNECTION_REFUSED;
    }

    /* (c2) choose_lane ran unlocked and may have blocked in app code;
     * re-check the facade stop/drain state before committing — a stop or
     * drain that landed meanwhile must refuse this accept, not admit it. */
    pthread_mutex_lock(&m->mu);
    bool refuse = m->draining ||
        atomic_load_explicit(&m->stop_requested, memory_order_acquire);
    pthread_mutex_unlock(&m->mu);
    if (refuse) {
        mgd_release_reserve(m);
        return QUIC_STATUS_CONNECTION_REFUSED;
    }

    /* (a) create the child (no locks; not visible yet). */
    moq_msquic_managed_conn_t *mc = NULL;
    if (mgd_make_child(m, selected_version, &mc) != MOQ_OK) {
        mgd_release_reserve(m);
        return QUIC_STATUS_OUT_OF_MEMORY;
    }

    /* (d) publish to the lane AND wire the transport under the lane's mu,
     * so the connection goes live atomically with the lane's stop check.
     * stop() sets lane->stop_requested under lane->mu before its doorbell
     * quiesces, so we either observe it and refuse, or finish wiring and
     * the doorbell quiesces a fully-formed connection — never a half-wired
     * one racing teardown. ConnectionSetConfiguration only schedules the
     * handshake on the connection's worker (no synchronous upcall on this
     * thread), so holding lane->mu across it cannot self-deadlock; the
     * first real upcall's guard simply waits for this unlock. */
    moq_msquic_managed_lane_t *lane = &m->lanes[idx];
    HQUIC h = ev->NEW_CONNECTION.Connection;
    pthread_mutex_lock(&lane->mu);
    if (lane->stop_requested) {
        pthread_mutex_unlock(&lane->mu);
        moq_msquic_conn_destroy(mc->conn);
        moq_session_destroy(mc->session);
        mgd_release_reserve(m);
        m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
        return QUIC_STATUS_CONNECTION_REFUSED;
    }
    mgd_lane_append(lane, mc);
    m->api->SetCallbackHandler(h, moq_msq_conn_cb_ptr(moq_msquic_conn_callback()),
                               mc->conn);
    if (moq_msquic_conn_bind(mc->conn, h) != MOQ_OK ||
        QUIC_FAILED(m->api->ConnectionSetConfiguration(
            h, m->configuration))) {
        /* (f) failure: MsQuic closes the conn and never delivers to the
         * handler we set — unwind fully (no live transport handle). The
         * blocking destroy runs after the lane mu is dropped. */
        mgd_lane_remove(lane, mc);
        pthread_mutex_unlock(&lane->mu);
        moq_msquic_conn_destroy(mc->conn);
        moq_session_destroy(mc->session);
        mgd_release_reserve(m);
        m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
        return QUIC_STATUS_CONNECTION_REFUSED;
    }
    mc->connection = h;
    /* wake the lane so its pump sees the new connection */
    lane->wake_pending = true;
    pthread_cond_broadcast(&lane->cv);
    mgd_bell_ring(lane);
    pthread_mutex_unlock(&lane->mu);
    mgd_bump_activity(m);
    return QUIC_STATUS_SUCCESS;
}

/* --- per-lane doorbell: pump delivery + session deadlines ------------- */

/* Fold a child's accepted-flush totals into its lane's LIFETIME
 * accumulators before it is unlinked and freed. Lane stats are lifetime
 * totals that must survive connection reclamation, so BOTH the doorbell
 * reap and stop cleanup latch them here — otherwise a snapshot taken
 * after stop() could regress below one taken before it. The caller holds
 * lane->mu (doorbell reap) or has joined every doorbell (stop cleanup),
 * so this write is unshared either way. */
static void mgd_lane_latch_flush(moq_msquic_managed_lane_t *lane,
                                 const moq_msquic_managed_conn_t *mc)
{
    if (mc->conn != NULL) {
        lane->st_flush_sends_reaped += mc->conn->flush_sends;
        lane->st_flush_bytes_reaped += mc->conn->flush_bytes;
    }
}

/* The lane's work predicate: an explicit wake, a worker-armed coalesced pump, or
 * a cross-lane wake. ONE definition, used by both the doorbell's loop-top branch
 * and doorbell_reap's post-relock yield, so the reaper can never disagree with
 * the scheduler about what counts as owed work. lane->mu held. */
static bool mgd_lane_work_pending(const moq_msquic_managed_lane_t *lane)
{
    return lane->wake_pending || lane->pump_pending ||
           atomic_load_explicit(&lane->ext_wake, memory_order_acquire);
}

#ifdef MOQ_MSQUIC_TESTING
/* Test-only: runs inside doorbell_reap's unlock window, right after the lane is
 * relocked and before the next victim is selected. Lets a test inject exactly
 * what a worker batch would do in that window (arm a pump) and prove the reaper
 * yields instead of reclaiming another child. Never compiled into production
 * libraries. */
void (*moq_msq_test_reap_gap)(moq_msquic_managed_lane_t *lane);

/* Test-only: fires inside moq_msquic_managed_stop() the moment the FACADE stop
 * flag is published, and before stop takes any lane lock — the one window in
 * which stop is visible while a lane pump can still be running with lane->mu
 * held. Lets a test order "stop is latched" against a callback it is holding,
 * with no timing involved. Called with no facade or lane lock held. Never
 * compiled into production libraries. */
void (*moq_msq_test_stop_latched)(moq_msquic_managed_t *m);

/* Test-only: arm this lane's coalesced pump exactly as a worker batch's
 * guard_leave would. Callable ONLY from the reap-gap hook above, which runs with
 * lane->mu held (so this is a plain flag store, not a wake path). */
void moq_msq_test_lane_arm_pump(moq_msquic_managed_lane_t *lane)
{
    lane->pump_pending = true;
}

/* Test-only: how many of this lane's children have reached the TRANSPORT
 * terminal and quiesced. That is one of the two facts reclamation needs — it
 * deliberately says nothing about the application acknowledgment, so a test can
 * observe a child that is transport-done and still legitimately retained. Pure
 * observation: it takes the lane lock, reads, and changes no scheduling or
 * lifetime state. Lets a test distinguish "the transport terminal has
 * completed" from the earlier state where the session is already closed but
 * SHUTDOWN_COMPLETE has not arrived, without inferring it from elapsed time.
 * Call with lane->mu NOT held. */
size_t moq_msq_test_lane_reapable(moq_msquic_managed_lane_t *lane)
{
    size_t n = 0;

    pthread_mutex_lock(&lane->mu);
    for (const moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
         mc = mc->next)
        if (mc->reapable && mc->conn->shutdown_complete)
            n++;
    pthread_mutex_unlock(&lane->mu);
    return n;
}

/* Test-only: SYNTHESIZE the ownership and terminal facts a scheduling test
 * needs from a terminal child — the facade reserve, a real session, lane
 * linkage, a closed bridge, and the reapability latch — using the production
 * functions where they apply.
 *
 * It is NOT a live accept and does not claim to be one. There is no lane
 * choice, no transport handle, no configuration, no callback, and no real
 * SHUTDOWN_COMPLETE: `shutdown_complete` is written directly because no
 * transport is present to set it. What IS production here is the reserve
 * accounting, mgd_make_child, mgd_lane_append, the bridge's own
 * on_transport_close (so the session really closes and really queues
 * SESSION_CLOSED), mgd_hook's reapability rule, and the pump arm a worker
 * batch's guard_leave would leave behind.
 *
 * Returns false if the facade reserve is exhausted or the child cannot be
 * built. Call with NO lane or facade lock held. */
bool moq_msq_test_lane_inject_terminal_child(moq_msquic_managed_lane_t *lane,
                                             uint64_t close_code)
{
    moq_msquic_managed_t *m = lane->owner;
    moq_msquic_managed_conn_t *mc = NULL;

    pthread_mutex_lock(&m->mu);
    if (m->conn_count >= m->max_connections) {
        pthread_mutex_unlock(&m->mu);
        return false;
    }
    m->conn_count++;
    pthread_mutex_unlock(&m->mu);

    if (mgd_make_child(m, m->version, &mc) != MOQ_OK) {
        mgd_release_reserve(m);
        return false;
    }

    pthread_mutex_lock(&lane->mu);
    mgd_lane_append(lane, mc);
    /* the terminal itself, through the entry the attach adapter uses */
    (void)moq_transport_bridge_on_transport_close(mc->conn->bridge, close_code,
                                                  mgd_now_us());
    mc->conn->shutdown_complete = true;
    mgd_hook(mc->conn, mc); /* the real reapability latch */
    /* arm one coalesced pump, exactly as a worker batch's guard_leave would */
    lane->pump_pending = true;
    pthread_cond_broadcast(&lane->cv);
    mgd_bell_ring(lane);
    pthread_mutex_unlock(&lane->mu);
    return true;
}

/* A live child that is present and idle: same production construction and
 * lane-membership path as the terminal injector above, and nothing else. It
 * does NOT close, terminalize, latch reapability, or arm a pump, because the
 * scan under test is the one the doorbell runs when there is no work — a
 * child that arms anything would take the pump path instead and the scan
 * would never be reached. */
bool moq_msq_test_lane_inject_idle_child(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_t *m = lane->owner;
    moq_msquic_managed_conn_t *mc = NULL;

    pthread_mutex_lock(&m->mu);
    if (m->conn_count >= m->max_connections) {
        pthread_mutex_unlock(&m->mu);
        return false;
    }
    m->conn_count++;
    pthread_mutex_unlock(&m->mu);

    if (mgd_make_child(m, m->version, &mc) != MOQ_OK) {
        mgd_release_reserve(m);
        return false;
    }

    pthread_mutex_lock(&lane->mu);
    mgd_lane_append(lane, mc);
    pthread_mutex_unlock(&lane->mu);
    return true;
}
#endif

/* May this child be reclaimed? (lane->mu held.)
 *
 * TWO independent facts, and both are required for a SERVER child:
 *   - the TRANSPORT terminal completed and quiesced (reapable +
 *     shutdown_complete), so no callback can still touch it; and
 *   - the APPLICATION acknowledged it (moq_msquic_managed_conn_ack_terminal),
 *     so a bounded consumer that has not finished with the child cannot have
 *     it destroyed underneath it.
 *
 * A pump OPPORTUNITY is not the second fact: nothing obliges an application to
 * poll on any particular pump, and a consumer that returns before draining
 * SESSION_CLOSED still needs its child, its session and its per-connection
 * state on a later pump.
 *
 * The single CLIENT connection is exempt. It is not a per-child reclamation
 * surface — the facade owns its lifetime, moq_msquic_managed_session() is
 * pump-scoped and simply reads NULL once the child is gone, so a client that
 * follows the documented handle rules cannot retain a destroyed session. */
static bool mgd_child_reclaimable(const moq_msquic_managed_conn_t *mc)
{
    if (!mc->reapable || !mc->conn->shutdown_complete)
        return false;
    if (mc->owner->cfg.perspective == MOQ_PERSPECTIVE_CLIENT)
        return true;
    return mc->app_terminal_acked;
}

/* Reclaim this lane's reclaimable children (lane->mu held on entry).
 * Unlink under lane->mu; the blocking ConnectionClose + reserve release
 * run with NO locks. Restart after relocking — the list may change.
 *
 * Scheduling rule this enforces: a child is not reclaimed while a pump is OWED.
 * The doorbell only reaps when its loop-top predicate found no armed work, so a
 * child reapable at entry had a pump OPPORTUNITY after it became terminal. That
 * holds per PASS, so the unlock window below must not be used to reclaim a child
 * whose pump was armed INSIDE it and has not run. After relocking, yield whenever
 * lane work is armed and let the doorbell pump first; the next pass reaps.
 *
 * That rule is about ORDER WITHIN a pass; mgd_child_reclaimable is what decides
 * eligibility at all. Forced facade teardown (stop()) reclaims every remaining
 * child regardless, after every doorbell has been joined. */
static void doorbell_reap(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_t *m = lane->owner;

    for (;;) {
        moq_msquic_managed_conn_t *victim = NULL;

        for (moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
             mc = mc->next)
            if (mgd_child_reclaimable(mc)) {
                victim = mc;
                break;
            }
        if (victim == NULL)
            return;
        /* Latch the child's flush totals into the lane while it is still
         * linked (lane->mu held): lane stats are lifetime totals that must
         * survive connection reclamation. */
        mgd_lane_latch_flush(lane, victim);
        mgd_lane_remove(lane, victim);
        pthread_mutex_unlock(&lane->mu);
        mgd_free_child(m, victim);
        mgd_release_reserve(m); /* a freed slot may admit an accept */
        pthread_mutex_lock(&lane->mu);
#ifdef MOQ_MSQUIC_TESTING
        if (moq_msq_test_reap_gap != NULL)
            moq_msq_test_reap_gap(lane); /* observe/inject at the reap window */
#endif
        /* Lane work appeared while unlocked (a worker batch arms a pump and can
         * mark another child reapable in the same window): yield so that pump
         * runs before any further reclamation. */
        if (mgd_lane_work_pending(lane))
            return;
    }
}

/* Snapshot the lane's conns into its sweep buffer (lane->mu held): the
 * conns whose lane->mu the pump holds for its whole duration.
 * lane_next_conn iterates exactly this. */
static void sweep_begin(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_t *m = lane->owner;
    size_t n = 0;

    for (moq_msquic_managed_conn_t *mc = lane->conns;
         mc != NULL && n < m->max_connections; mc = mc->next)
        lane->sweep_buf[n++] = mc;
    lane->sweep = lane->sweep_buf;
    lane->sweep_count = n;
    mgd_tls_cb = m;
    mgd_tls_lane = lane;
    lane->in_sweep = true;
}

/* Close the sweep: refresh this lane's pending mirrors (lane->mu held,
 * then m->mu briefly — lane->mu -> m->mu order), clear markers. */
static void sweep_end(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_t *m = lane->owner;
    int ps = 0, pd = 0;

    for (size_t i = 0; i < lane->sweep_count; i++) {
        ps += moq_msquic_conn_pending_sends(lane->sweep[i]->conn);
        pd += moq_msquic_conn_pending_datagrams(lane->sweep[i]->conn);
    }
    lane->in_sweep = false;
    mgd_tls_lane = NULL;
    mgd_tls_cb = NULL;
    lane->sweep = NULL;
    lane->sweep_count = 0;
    pthread_mutex_lock(&m->mu);
    lane->m_pending_sends = ps;
    lane->m_pending_dgrams = pd;
    pthread_mutex_unlock(&m->mu);
}

/* A full service tick across the lane (deadline-driven). */
static void doorbell_service_all(moq_msquic_managed_lane_t *lane)
{
    sweep_begin(lane);
    for (size_t i = 0; i < lane->sweep_count; i++) {
        lane->st_service_passes++;
        moq_msquic_conn_service(lane->sweep[i]->conn);
    }
    sweep_end(lane);
}

/* The coalesced lane pump: ONE on_lane_pump over ALL the lane's conns,
 * then one drain pass per conn so what the pump queued reaches the
 * wire. Runs with lane->mu held (doorbell context). */
static void doorbell_pump_sweep(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_t *m = lane->owner;

    lane->st_pump_sweeps++;
    sweep_begin(lane);
    /* Snapshot each conn's event-progress token BEFORE the app pump, so the
     * re-drive predicate below spans the WHOLE pump+service cycle: the pump may
     * DEQUEUE events (draining a pre-existing backlog a large queue never
     * backpressured) and the post-pump service may ENQUEUE events (refilling a
     * small queue) — either is progress that owes another pump. */
    for (size_t i = 0; i < lane->sweep_count; i++)
        lane->sweep[i]->pump_pre_token =
            moq_msq_conn_event_token(lane->sweep[i]->conn);

    if (!lane->pump_exit && !lane->stop_requested &&
        !atomic_load_explicit(&m->stop_requested, memory_order_acquire) &&
        m->cfg.on_lane_pump != NULL) {
        lane->in_lane_pump = true; /* the exclusive per-conn window */
        int rc = m->cfg.on_lane_pump(m, lane, mgd_now_us(),
                                     m->cfg.on_lane_pump_user);
        lane->in_lane_pump = false; /* cleared before service + on_activity */
        if (rc != 0) {
            lane->pump_exit = true;
            pthread_mutex_lock(&m->mu);
            m->pump_exit = true;
            pthread_cond_broadcast(&m->cv);
            pthread_mutex_unlock(&m->mu);
        }
    }
    bool progressed = false;
    for (size_t i = 0; i < lane->sweep_count; i++) {
        lane->st_service_passes++;
        moq_msq_conn_service_wake(lane->sweep[i]->conn);
        /* Re-drive predicate for this conn: the token moved across the cycle
         * (the pump dequeued events and/or service enqueued them) AND events
         * still remain to drain. Either without the other is not owed a pump. */
        if (moq_msq_conn_event_token(lane->sweep[i]->conn) !=
                lane->sweep[i]->pump_pre_token &&
            moq_msq_conn_has_events(lane->sweep[i]->conn))
            progressed = true;
    }
    /* Arm one more coalesced pump when any conn made progress and still has
     * events queued. This closes BOTH the small-queue case (service refilled the
     * queue after the pump) and the large-queue case (the bounded pump drained
     * some pre-existing events but left more). The doorbell holds lane->mu and
     * immediately re-evaluates its predicate, so pump_pending alone re-drives —
     * no extra wake mechanism. Bounded and spin-free: a stalled or non-draining
     * app moves no events once its queue is full, so the token does not advance
     * and nothing re-arms; the re-armed pump's own cycle re-evaluates the
     * predicate, so the re-drive settles the instant the queue empties. Never
     * re-arm into teardown (stop / pump_exit). */
    if (progressed && !lane->pump_exit && !lane->stop_requested &&
        !atomic_load_explicit(&m->stop_requested, memory_order_acquire))
        lane->pump_pending = true;
    /* bump activity while the lane markers are still set: a same-lane
     * lane_wake() from on_activity must arm, not re-lock the doorbell-held
     * mutex. sweep_end then refreshes the mirrors and clears the markers. */
    mgd_bump_activity(m);
    sweep_end(lane);
}

/* Shut this lane's connections down + wait for their SHUTDOWN_COMPLETE
 * (lane->mu held). Runs on the lane doorbell once stop is requested. */
static void doorbell_quiesce(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_t *m = lane->owner;

    for (moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
         mc = mc->next)
        if (mc->connection != NULL && !mc->conn->shutdown_complete) {
            /* a facade-initiated stop is a deliberate local close: a
             * locally shut connection delivers only SHUTDOWN_COMPLETE,
             * which would otherwise read as an unclean death — record
             * the clean terminal first */
            if (!mc->conn->close_pending) {
                mc->conn->close_pending = true;
                mc->conn->close_clean = true;
                mc->conn->close_code = 0;
            }
            m->api->ConnectionShutdown(
                mc->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        }
    for (;;) {
        bool all_done = true;

        for (moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
             mc = mc->next)
            if (mc->connection != NULL && !mc->conn->shutdown_complete)
                all_done = false;
        if (all_done)
            break;
        pthread_cond_wait(&lane->cv, &lane->mu);
    }
}

/* --- the bell (wake-only signaling) ---------------------------------------
 * Ring/wait protocol: a waker rings by bumping bell_gen under bell_mu and
 * broadcasting; the doorbell snapshots the generation BEFORE checking its
 * work predicate and sleeps only while the generation is unchanged. A ring
 * that lands between the predicate check and the sleep therefore makes the
 * sleep return immediately — the notification is durable (a generation,
 * not an edge) and cannot be lost to the check->wait race. bell_mu is a
 * strict leaf: nothing else is ever acquired under it, so any context —
 * even one holding another lane's mu — may ring without a lock-order
 * cycle. The waiter releases bell_mu before it reacquires lane->mu. */

static void mgd_bell_ring(moq_msquic_managed_lane_t *lane)
{
    pthread_mutex_lock(&lane->bell_mu);
    lane->bell_gen++;
    pthread_cond_broadcast(&lane->bell_cv);
    pthread_mutex_unlock(&lane->bell_mu);
}

#ifdef MOQ_MSQUIC_TESTING
/* Test-only: deliver a bell generation without arming any explicit wake
 * cause. This drives the real wait/classification boundary while keeping
 * wake accounting at zero. */
void moq_msq_test_bell_ring_raw(moq_msquic_managed_lane_t *lane)
{
    if (lane != NULL)
        mgd_bell_ring(lane);
}
#endif

static uint64_t mgd_bell_snapshot(moq_msquic_managed_lane_t *lane)
{
    pthread_mutex_lock(&lane->bell_mu);
    uint64_t gen = lane->bell_gen;
    pthread_mutex_unlock(&lane->bell_mu);
    return gen;
}

/* Sleep until the bell generation moves past `seen` or `abs` expires.
 * Returns with no locks held. Returns true if the generation advanced (a
 * ring delivered a wake), false on deadline expiry. */
static bool mgd_bell_wait(moq_msquic_managed_lane_t *lane, uint64_t seen,
                          const struct timespec *abs)
{
    pthread_mutex_lock(&lane->bell_mu);
    while (lane->bell_gen == seen &&
           pthread_cond_timedwait(&lane->bell_cv, &lane->bell_mu,
                                  abs) != ETIMEDOUT)
        ;
    bool advanced = lane->bell_gen != seen;
    pthread_mutex_unlock(&lane->bell_mu);
    return advanced;
}

#ifdef MOQ_MSQUIC_TESTING
/* Test-only: observe the bell generation through the SAME leaf-mutex snapshot
 * path the doorbell uses. A held pre-wait test can therefore prove a ring was
 * delivered before releasing the waiter, without advancing through the idle
 * cap. Never compiled into production libraries. */
uint64_t moq_msq_test_bell_generation(moq_msquic_managed_lane_t *lane)
{
    return lane != NULL ? mgd_bell_snapshot(lane) : 0;
}

/* Test-only: runs in the doorbell's check->wait race window — after the
 * loop-top predicate observed no work, with lane->mu released, immediately
 * before the bell wait. Lets a test pin the doorbell in exactly the window
 * the bell protocol must cover. Never compiled into production libraries. */
void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);

/* Test-only: an API table the facade BORROWS instead of calling MsQuicOpen2.
 * Read once, at create; the table stays the caller's and is never passed to
 * MsQuicClose. Lets a test stand the whole facade on a fake table with no
 * MsQuic library, socket or credential. Never compiled into production
 * libraries. */
const QUIC_API_TABLE *moq_msq_test_api_override;

/* Test-only: suppress doorbell thread creation, so every iteration is driven
 * explicitly through moq_msq_test_lane_step instead. Read once, at create;
 * teardown stays valid with no thread, because every join site is already
 * guarded on doorbell_running. Never compiled into production libraries. */
bool moq_msq_test_no_doorbell;

/* Test-only: count of idle waits this lane returned from because the bell
 * generation advanced (a ring delivered the wake), not because the deadline
 * expired. A prompt delivery increments it; a wake left to the idle cap does
 * not. The structural discriminator for the check->wait race. */
uint64_t moq_msq_test_bell_wakes(moq_msquic_managed_lane_t *lane)
{
    return atomic_load(&lane->st_bell_gen_wakes);
}
#endif

/* Cap on an idle lane doorbell's wait: bounds worst-case latency for a
 * wake whose delivery path failed in some unforeseen way; with the bell,
 * explicit wakes no longer depend on it. */
#define MGD_IDLE_WAIT_US 200000u

/* One doorbell iteration's decision, taken with lane->mu held.
 *
 * This is the whole per-iteration policy: consume a wake or worker batch and
 * pump, reap, honor a due deadline, or report that the lane is idle. The
 * thread's only remaining job is to wait, so nothing here is a model of the
 * decision -- doorbell_main and the deterministic test driver run the very
 * same function, and any transition one performs the other performs too.
 *
 * On MGD_STEP_IDLE it reports the instant to wait until and whether that
 * instant is the idle cap rather than a session deadline, which is what the
 * caller needs to classify the wake it eventually gets. */
typedef enum {
    MGD_STEP_PUMPED = 0,
    MGD_STEP_TICKED = 1,
    MGD_STEP_STOP = 2,
    MGD_STEP_IDLE = 3,
} mgd_step_kind_t;

static mgd_step_kind_t mgd_doorbell_step(moq_msquic_managed_lane_t *lane,
                                         uint64_t *until_us, bool *at_cap)
{
    moq_msquic_managed_t *m = lane->owner;

    if (lane->stop_requested ||
        atomic_load_explicit(&m->stop_requested, memory_order_acquire))
        return MGD_STEP_STOP;

    if (mgd_lane_work_pending(lane)) {
        /* a wake or worker-armed batch: ONE coalesced pump over the
         * whole lane. Run it BEFORE reaping so terminal children
         * stay visible to the pump that polls their final
         * SESSION_CLOSED event. ext_wake is the lock-free cross-lane
         * wake; consuming it here (after a pump, or after the bounded
         * idle wait) is what makes that wake guaranteed. */
        lane->wake_pending = false;
        lane->pump_pending = false;
        atomic_store_explicit(&lane->ext_wake, false,
                              memory_order_release);
        /* Close the explicit pending-wake cycle (if one is open —
         * pump_pending alone is a worker batch, not an explicit wake)
         * and record EXACTLY ONE latency sample for it. */
        uint64_t stamp =
            atomic_exchange(&lane->st_wake_stamp_us, (uint64_t)0);
        if (stamp != 0) {
            uint64_t nowd = mgd_now_us();
            uint64_t delay = nowd > stamp ? nowd - stamp : 0;
            lane->st_wake_to_pump_samples++;
            lane->st_wake_to_pump_total_us += delay;
            if (delay > lane->st_wake_to_pump_max_us)
                lane->st_wake_to_pump_max_us = delay;
        }
        doorbell_pump_sweep(lane);
        /* Reap opportunity between pump passes. Without it the only reap
         * site is the no-work iteration below, so a lane whose callback
         * arms work on every pass — a same-lane lane_wake(), or event
         * progress rearming the coalesced pump — never reaches it, and an
         * eligible child keeps its admission reserve for as long as the
         * lane stays busy.
         *
         * Placed after the sweep has RETURNED: the app callback is over
         * and no handle it was shown is in use. How much it reclaims is
         * doorbell_reap's own rule, not a bound this site adds — with work
         * armed (the case this exists for) it yields after its first
         * victim, so the next pump still runs first; with the lane quiet
         * after the pump it drains every eligible child, exactly as the
         * no-work site does.
         *
         * Skipped once the FACADE stop is latched. That flag is published
         * before stop takes any lane lock, so it can become visible while
         * the pump that just returned was still running; from there on
         * reclamation belongs to stop's own cleanup, which runs after it
         * has joined this thread. The lane flag needs no test of its own:
         * stop sets it under lane->mu, which this thread holds unbroken
         * from the callback to here, and the loop-top predicate consumes
         * it. Pump exit needs none either — it changes which site reclaims
         * an eligible child, never whether, because a suppressed callback
         * can no longer rearm the lane and the no-work site below is
         * reached on the next pass. */
        if (!atomic_load_explicit(&m->stop_requested,
                                  memory_order_acquire))
            doorbell_reap(lane);
        return MGD_STEP_PUMPED;
    }
    doorbell_reap(lane);
    if (lane->stop_requested ||
        atomic_load_explicit(&m->stop_requested, memory_order_acquire))
        return MGD_STEP_STOP;

#ifdef MOQ_MSQUIC_TESTING
    /* ONE instant for the whole scan, sampled before the loop: a clock read
     * per connection would add an O(N) cost to the exact O(N) path this
     * instrumentation exists to measure, and would classify different
     * connections against different instants. The scheduler's own decision
     * still uses its later timestamp below, so the diagnostic changes no
     * behaviour. */
    const uint64_t scan_now = mgd_now_us();
#endif
    uint64_t deadline = UINT64_MAX;
    for (moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
         mc = mc->next) {
        uint64_t dl = moq_session_next_deadline_us(mc->session);
#ifdef MOQ_MSQUIC_TESTING
        /* Counted at the query itself, not at the branch below: the branch
         * counts a deadline that fired, while the cost being measured is this
         * scan, which runs whether or not anything is due. The classes
         * partition the queries exactly — every query lands in one. */
        lane->st_deadline_queries++;
        if (dl == UINT64_MAX)
            lane->st_deadline_class_none++;
        else if (dl > scan_now)
            lane->st_deadline_class_future++;
        else
            lane->st_deadline_class_due++;
#endif

        if (dl < deadline)
            deadline = dl;
    }
    /* Fold the application service deadline (e.g. media_sender's periodic
     * catalog refresh) so a purely time-based deadline wakes this lane
     * with no transport event. Pure cached read; the loop already broke
     * above on stop, and lane->mu -> ep->mu here matches the pump's order.
     * Recomputed every iteration, so there is no stale arm to guard. */
    if (m->cfg.app_deadline_us != NULL) {
        uint64_t ad = m->cfg.app_deadline_us(m->cfg.app_deadline_ctx);
        if (ad < deadline)
            deadline = ad;
    }
    uint64_t now = mgd_now_us();

    /* a due session deadline: tick every session, then pump. Reaching
     * this branch means NO explicit wake was pending (the predicate
     * above wins), so counting the cause here cannot double-count. */
    if (deadline != UINT64_MAX && deadline <= now) {
        lane->st_deadline_sweeps++;
        doorbell_service_all(lane);
        doorbell_pump_sweep(lane);
        return MGD_STEP_TICKED;
    }

    /* Idle: report when to wake and whether that instant is the cap rather
     * than a session deadline, so the caller can classify what wakes it. */
    uint64_t cap = now + MGD_IDLE_WAIT_US;
    uint64_t until = (deadline != UINT64_MAX && deadline < cap) ? deadline
                                                                : cap;

    *until_us = until;
    *at_cap = (until == cap);
    return MGD_STEP_IDLE;
}

static void *doorbell_main(void *arg)
{
    moq_msquic_managed_lane_t *lane = arg;

    pthread_mutex_lock(&lane->mu);
    for (;;) {
        /* Snapshot the bell BEFORE the decision: any ring that lands after
         * this line -- including one racing the check->wait window below --
         * moves the generation and turns this iteration's bell wait into an
         * immediate return. (bell_mu is a leaf; taking it briefly under
         * lane->mu is the one sanctioned nesting.) */
        uint64_t bell_seen = mgd_bell_snapshot(lane);
        uint64_t until = 0;
        bool at_cap = false;
        mgd_step_kind_t kind = mgd_doorbell_step(lane, &until, &at_cap);

        if (kind == MGD_STEP_STOP)
            break;
        if (kind != MGD_STEP_IDLE)
            continue;

        /* Wait on the BELL for a wake or the next deadline, but never
         * longer than the idle cap. Every wake source rings the bell, and
         * the generation snapshot taken above (before the decision) makes a
         * ring in the check->wait window return immediately -- explicit
         * wakes no longer depend on the cap; it remains as a robustness
         * backstop and bounds the deadline scan cadence. lane->mu is
         * released across the sleep (the doorbell never holds it while
         * blocked on the bell, and never holds bell_mu while reacquiring
         * it). On return we simply loop: the next decision consumes
         * wake/pump/ext_wake, and a now-due deadline is handled there. */
        uint64_t now = mgd_now_us();
        uint64_t delay = until > now ? until - now : 0;
        struct timespec abs;

        clock_gettime(CLOCK_REALTIME, &abs);
        abs.tv_sec += (time_t)(delay / 1000000u);
        abs.tv_nsec += (long)(delay % 1000000u) * 1000;
        if (abs.tv_nsec >= 1000000000L) {
            abs.tv_sec += abs.tv_nsec / 1000000000L;
            abs.tv_nsec %= 1000000000L;
        }
        pthread_mutex_unlock(&lane->mu);
#ifdef MOQ_MSQUIC_TESTING
        if (moq_msq_test_prewait != NULL)
            moq_msq_test_prewait(lane);
        if (mgd_bell_wait(lane, bell_seen, &abs))
            atomic_fetch_add(&lane->st_bell_gen_wakes, 1);
#else
        (void)mgd_bell_wait(lane, bell_seen, &abs);
#endif
        pthread_mutex_lock(&lane->mu);
        /* An idle-cap expiry with no work: the wait ran to the CAP (not a
         * due deadline, which the decision counts) and no explicit wake /
         * worker batch arrived. Classified here, at the decision point, so
         * an explicit wake consumed above is never also counted as an idle
         * wake. */
        if (!lane->wake_pending && !lane->pump_pending &&
            !atomic_load_explicit(&lane->ext_wake, memory_order_acquire) &&
            at_cap && mgd_now_us() >= until) {
            lane->st_idle_cap_wakes++;
        }
    }
    /* stop: quiesce this lane's transports; stop() joins us, then does
     * the blocking closes lock-free */
    doorbell_quiesce(lane);
    pthread_mutex_unlock(&lane->mu);
    return NULL;
}

/* Install the MsQuic API table: borrow a caller-supplied one, or open our
 * own. Read ONCE here, at create. A borrowed table remains the caller's
 * property, which is what api_borrowed records for mgd_close_transport. */
static moq_result_t mgd_open_api(moq_msquic_managed_t *m,
                                 const QUIC_API_TABLE *borrowed)
{
#ifdef MOQ_MSQUIC_TESTING
    if (borrowed == NULL)
        borrowed = moq_msq_test_api_override;
#endif
    if (borrowed != NULL) {
        m->api = borrowed;
#ifdef MOQ_MSQUIC_TESTING
        m->api_borrowed = true;
#endif
        return MOQ_OK;
    }
    if (QUIC_FAILED(MsQuicOpen2(&m->api)))
        return MOQ_ERR_INTERNAL;
    return MOQ_OK;
}

/* Freeze this facade's doorbell decision. Taken ONCE, at create, before any
 * lane is spawned, so every lane of one facade agrees. */
static void mgd_freeze_doorbell_decision(moq_msquic_managed_t *m)
{
#ifdef MOQ_MSQUIC_TESTING
    m->doorbell_suppressed = moq_msq_test_no_doorbell;
#else
    (void)m;
#endif
}

/* Spawn one lane's doorbell thread, recording that it is running. A facade
 * whose frozen decision suppressed the worker is simply a lane with no
 * thread, which every join site already handles. */
static bool mgd_spawn_doorbell(moq_msquic_managed_t *m,
                               moq_msquic_managed_lane_t *lane)
{
#ifdef MOQ_MSQUIC_TESTING
    if (m->doorbell_suppressed)
        return true;
#else
    (void)m;
#endif
    if (pthread_create(&lane->doorbell, NULL, doorbell_main, lane) != 0)
        return false;
    lane->doorbell_running = true;
    return true;
}

/* --- create ---------------------------------------------------------------- */

/* Allocate + init the lane array (mu/cv/sweep_buf per lane). Doorbell
 * threads are spawned later, once the transport is up. */
static moq_result_t mgd_lanes_alloc(moq_msquic_managed_t *m)
{
    m->lanes = m->alloc.alloc(m->lane_count * sizeof(*m->lanes),
                              m->alloc.ctx);
    if (m->lanes == NULL)
        return MOQ_ERR_NOMEM;
    memset(m->lanes, 0, m->lane_count * sizeof(*m->lanes));
    for (uint32_t i = 0; i < m->lane_count; i++) {
        moq_msquic_managed_lane_t *lane = &m->lanes[i];

        /* mu/cv first, so a later sweep_buf failure still leaves this
         * lane's mutex/cond safe to destroy (owner marks it init'd) */
        lane->owner = m;
        lane->index = i;
        pthread_mutex_init(&lane->mu, NULL);
        pthread_cond_init(&lane->cv, NULL);
        pthread_mutex_init(&lane->bell_mu, NULL);
        pthread_cond_init(&lane->bell_cv, NULL);
        lane->sweep_buf = m->alloc.alloc(
            m->max_connections * sizeof(*lane->sweep_buf), m->alloc.ctx);
        if (lane->sweep_buf == NULL)
            return MOQ_ERR_NOMEM; /* mgd_free cleans partial init */
    }
    return MOQ_OK;
}

static void mgd_lanes_free(moq_msquic_managed_t *m)
{
    if (m->lanes == NULL)
        return;
    for (uint32_t i = 0; i < m->lane_count; i++) {
        moq_msquic_managed_lane_t *lane = &m->lanes[i];

        if (lane->owner == NULL)
            continue; /* never init'd (allocation stopped before it) */
        if (lane->sweep_buf != NULL)
            m->alloc.free(lane->sweep_buf,
                          m->max_connections * sizeof(*lane->sweep_buf),
                          m->alloc.ctx);
        pthread_mutex_destroy(&lane->mu);
        pthread_cond_destroy(&lane->cv);
        pthread_mutex_destroy(&lane->bell_mu);
        pthread_cond_destroy(&lane->bell_cv);
    }
    m->alloc.free(m->lanes, m->lane_count * sizeof(*m->lanes),
                  m->alloc.ctx);
    m->lanes = NULL;
}

static void mgd_free(moq_msquic_managed_t *m)
{
    moq_alloc_t alloc = m->alloc;

    mgd_lanes_free(m);
    mgd_strfree(&alloc, m->host);
    mgd_strfree(&alloc, m->cert_path);
    mgd_strfree(&alloc, m->key_path);
    pthread_mutex_destroy(&m->mu);
    pthread_cond_destroy(&m->cv);
    alloc.free(m, sizeof(*m), alloc.ctx);
}

/* Close transport handles during a failed create: nothing is started,
 * no callbacks are in flight, and the mutex is not held. */
static void mgd_close_transport(moq_msquic_managed_t *m)
{
    if (m->listener != NULL) {
        m->api->ListenerClose(m->listener);
        m->listener = NULL;
    }
    if (m->configuration != NULL) {
        m->api->ConfigurationClose(m->configuration);
        m->configuration = NULL;
    }
    if (m->registration != NULL) {
        m->api->RegistrationClose(m->registration);
        m->registration = NULL;
    }
    if (m->api != NULL) {
#ifdef MOQ_MSQUIC_TESTING
        if (!m->api_borrowed)
            MsQuicClose(m->api);
#else
        MsQuicClose(m->api);
#endif
        m->api = NULL;
    }
}

void moq_msquic_managed_cfg_init_sized(moq_msquic_managed_cfg_t *cfg,
                                       size_t size)
{
    /* size is sizeof(*cfg) in the CALLER's compilation unit: any size
     * covering the frozen v0 prefix is valid — zero exactly the
     * caller's bytes and stamp the matching struct_size, so an old
     * binary against a newer library keeps working (appended fields
     * stay disabled by the smaller size) */
    if (cfg == NULL ||
        size < offsetof(moq_msquic_managed_cfg_t, version))
        return;
    if (size > sizeof(*cfg))
        size = sizeof(*cfg);
    memset(cfg, 0, size);
    cfg->struct_size = (uint32_t)size;
}

/* Copy the caller-sized configuration and enforce every multi-field ABI
 * block in one place. Test-only constructors use this helper too, so their
 * prefix tests exercise the production gate rather than a copied model. */
/* True when the caller's struct_size covers all of `field`. Every appended
 * field is read through this, so a prefix-sized caller reads the documented
 * default instead of whatever its stack held past the frozen prefix. */
#define MGD_CFG_HAS(cfg, field)                                              \
    ((cfg)->struct_size >=                                                   \
     offsetof(moq_msquic_managed_cfg_t, field) + sizeof((cfg)->field))

/* The size-qualified reading of the appended config block. One derivation,
 * consumed by both the public constructor and the lanes-only test
 * constructor, so a test facade can never adopt a value the public path
 * would ignore. */
typedef struct mgd_derived_cfg {
    moq_version_t version;
    size_t max_connections;
    uint32_t lane_count;
} mgd_derived_cfg_t;

static void mgd_derive_cfg(const moq_msquic_managed_cfg_t *cfg,
                           bool is_client, mgd_derived_cfg_t *out)
{
    out->version = MOQ_VERSION_DRAFT_16;
    if (MGD_CFG_HAS(cfg, version) && cfg->version != 0)
        out->version = cfg->version;

    out->max_connections = 1;
    if (MGD_CFG_HAS(cfg, max_connections) && cfg->max_connections != 0)
        out->max_connections = cfg->max_connections;

    /* lane count: clients always use exactly one lane; servers take
     * cfg.lane_count (0 -> 1). Cap lanes at the connection cap — more
     * lanes than connections buys nothing. */
    out->lane_count = 1;
    if (!is_client && MGD_CFG_HAS(cfg, lane_count) && cfg->lane_count > 1)
        out->lane_count = cfg->lane_count;
    if ((size_t)out->lane_count > out->max_connections)
        out->lane_count = (uint32_t)out->max_connections;
}

static void mgd_apply_derived_cfg(moq_msquic_managed_t *m,
                                  const mgd_derived_cfg_t *derived)
{
    m->version = derived->version;
    m->max_connections = derived->max_connections;
    m->lane_count = derived->lane_count;
}

static void mgd_copy_cfg(moq_msquic_managed_t *m,
                         const moq_msquic_managed_cfg_t *cfg)
{
    memcpy(&m->cfg, cfg,
           cfg->struct_size < sizeof(m->cfg) ? cfg->struct_size
                                             : sizeof(m->cfg));
    if (cfg->struct_size <
        offsetof(moq_msquic_managed_cfg_t, app_deadline_ctx) +
            sizeof(cfg->app_deadline_ctx)) {
        m->cfg.app_deadline_us = NULL;
        m->cfg.app_deadline_ctx = NULL;
    }
    /* A partial appended scalar is poison, not a value. Keep the session
     * default unless the caller-sized prefix covers the whole field. */
    if (!MGD_CFG_HAS(cfg, max_open_subgroups))
        m->cfg.max_open_subgroups = 0;
    /* The caller's version array is borrowed only through create(). Public
     * multi-version construction repoints this pair at facade-owned storage
     * after validating and copying it; test-only exact constructors retain no
     * caller pointer. */
    m->cfg.versions = NULL;
    m->cfg.version_count = 0;
}

/*
 * Path MTU the managed LISTENER pins, in IP-packet bytes.
 *
 * 1280 is a PATH MTU, not a UDP payload size. Subtracting the protocol headers
 * gives the UDP payloads it actually caps:
 *
 *     IPv4: 1280 - 20 (IPv4 header) - 8 (UDP header) = 1252
 *     IPv6: 1280 - 40 (IPv6 header) - 8 (UDP header) = 1232
 *
 * Those are the RFC's own numbers: §14 assumes a minimum IP packet size of
 * 1280 bytes, which is where 1252 and 1232 come from.
 *
 * Why a listener needs it. A client may open with an Initial that carries no
 * ClientHello, so the server's first reply goes out before any peer transport
 * parameter has been received -- there is no advertised max_udp_payload_size to
 * consult yet, and §14 warns that sending above 1200 bytes before learning it
 * risks loss. At MsQuic's default the reply measured 1260 bytes of UDP payload.
 * The observed peer sent exclusively 1252-byte datagrams and did not process
 * that reply; its handshake stalled and it retransmitted until it gave up.
 * Dropping server datagrams above 1252 in a proxy, with no source change, took
 * that peer from 0/6 to 6/6.
 *
 * This configuration caps every CONNECTION-SCOPED datagram once ALPN/SNI has
 * selected it, but it does NOT govern the server's very first reply: the new
 * connection copies MsQuic's library-global settings and seeds its path MTU on
 * the pre-configuration CONNECTION path, before any configuration is attached,
 * so that first reply is sized from the process-global default MinimumMtu and
 * stayed 1260 bytes with this configuration alone in place. The pre-configuration
 * connection reply is lowered separately by the process-global floor a managed
 * SERVER installs at create time (see mgd_build_global_server_floor /
 * mgd_apply_global_server_floor below); the two together reproduce the 1252-byte
 * IPv4 cap on both the first reply and every later datagram.
 *
 * RFC 9000 §18.2 defines max_udp_payload_size as the largest UDP payload an
 * endpoint is willing to receive and notes that larger datagrams are unlikely
 * to be processed; §14 permits a receiver to discard datagrams that exceed its
 * size constraints and forbids closing the connection solely for that. So the
 * peer's behavior is permitted, not mandated -- the fix is ours to make because
 * a conservative listener interoperates and an oversized one does not.
 *
 * Why BOTH bounds are set: MaximumMtu and MinimumMtu are both IP-path MTUs, and
 * this MsQuic revision defaults MinimumMtu to 1288 (QUIC_INITIAL_PACKET_LENGTH
 * 1240 + 40 IPv6 + 8 UDP). Lowering only the maximum would leave a minimum
 * greater than the maximum. Pinning both also bounds later DPLPMTUD probing.
 */
/* Static in production; externally visible only under the testing seam so a
 * focused test can assert the SAME construction the create path calls. */
#ifdef MOQ_MSQUIC_TESTING
#  define MGD_SETTINGS_LINKAGE
#else
#  define MGD_SETTINGS_LINKAGE static
#endif

#define MGD_LISTENER_PATH_MTU 1280u

/*
 * The one settings construction the create path uses. Kept as a single
 * function so the listener MTU policy cannot drift from what is actually
 * handed to ConfigurationOpen(), and taking the configuration alone so the
 * perspective it acts on is the configuration's own.
 *
 * Precondition, established by moq_msquic_managed_create() before it calls
 * this: cfg is non-NULL and its perspective is CLIENT or SERVER.
 */
MGD_SETTINGS_LINKAGE void mgd_build_settings(const moq_msquic_managed_cfg_t *cfg,
                                             QUIC_SETTINGS *out)
{
    moq_msquic_settings_init(out);
    if (cfg->idle_timeout_ms != 0) {
        /* bounds both idle phases, so a dead peer fails within the
         * configured window whether or not the handshake started */
        out->IdleTimeoutMs = cfg->idle_timeout_ms;
        out->IsSet.IdleTimeoutMs = TRUE;
        out->HandshakeIdleTimeoutMs = cfg->idle_timeout_ms;
        out->IsSet.HandshakeIdleTimeoutMs = TRUE;
    }
    if (cfg->perspective == MOQ_PERSPECTIVE_SERVER) {
        out->MinimumMtu = MGD_LISTENER_PATH_MTU;
        out->IsSet.MinimumMtu = TRUE;
        out->MaximumMtu = MGD_LISTENER_PATH_MTU;
        out->IsSet.MaximumMtu = TRUE;
    }
    /* The managed CLIENT declares no MTU policy: both IsSet bits stay clear so
     * MsQuic's own defaults apply, exactly as before this listener fix. */
}

/*
 * The listener CONFIGURATION above governs connection-scoped datagrams once
 * ALPN/SNI has selected it. But a server's FIRST reply to a client Initial that
 * carries no ClientHello is sized on the pre-configuration CONNECTION path: the
 * new connection copies MsQuic's library-global settings and seeds its path MTU
 * from the process-global default MinimumMtu (1288 in this revision) before any
 * configuration is attached -- so it measured 1260 bytes and the peer discarded
 * it. There is no registration-scoped settings parameter, so the only lever for
 * that first packet is the process-global floor.
 *
 * This builds the SINGLE global write the managed SERVER create issues: a zeroed
 * QUIC_SETTINGS declaring ONLY MinimumMtu = 1280. It deliberately does NOT go
 * through mgd_build_settings()/moq_msquic_settings_init(), which would also
 * declare SendBufferingEnabled -- that must not leak into process-global policy.
 * No global MaximumMtu is set, so clients and other registrations keep the
 * ability to grow via DPLPMTUD; only the conservative minimum moves.
 */
MGD_SETTINGS_LINKAGE void
mgd_build_global_server_floor(QUIC_SETTINGS *out)
{
    memset(out, 0, sizeof(*out));
    out->MinimumMtu = MGD_LISTENER_PATH_MTU;
    out->IsSet.MinimumMtu = TRUE;
}

#ifdef MOQ_MSQUIC_TESTING
/* Test-only post-write probe. Invoked with the LIVE api table immediately after
 * the real global SetParam succeeds and before the create tears anything down,
 * so a socket-free test can GetParam(QUIC_PARAM_GLOBAL_SETTINGS) and prove the
 * partial update actually took effect (MinimumMtu 1280 active, the pre-existing
 * global MaximumMtu unchanged, nothing else moved). It does NOT replace the real
 * SetParam -- that call always runs. Absent from production builds. */
void (*moq_msq_test_after_global_write)(const QUIC_API_TABLE *api) = NULL;
/* Test-only fail-closed injector. When active, the real SetParam still runs, but
 * its returned status is overridden with this forced value so the create's
 * fail-closed path can be exercised without a real global-write failure. Left
 * inactive by the positive probe, whose readback stays authoritative. Absent
 * from production builds. */
bool        moq_msq_test_force_global_status_active = false;
QUIC_STATUS moq_msq_test_force_global_status = QUIC_STATUS_SUCCESS;
/* Test-only: when true, the create path aborts (cleanly) right after the
 * global-floor decision point, before RegistrationOpen and any socket. Absent
 * from production builds. */
bool moq_msq_test_abort_after_global = false;
/* Test-only: set TRUE by the create path just before RegistrationOpen. Lets a
 * socket-free test prove the global-floor decision is honored BEFORE the
 * registration is opened -- so a SERVER whose global write fails (or which
 * aborts at the decision point) never reaches RegistrationOpen. Absent from
 * production builds. */
bool moq_msq_test_registration_reached = false;
#endif

/*
 * Install the process-global minimum-MTU floor for a managed SERVER, issued
 * immediately after MsQuicOpen2 and before RegistrationOpen. Fails closed: a
 * rejected global write aborts the create rather than starting a listener that
 * reproduces the oversized pre-configuration connection reply.
 */
static moq_result_t
mgd_apply_global_server_floor(moq_msquic_managed_t *m)
{
    QUIC_SETTINGS floor;
    mgd_build_global_server_floor(&floor);
    QUIC_STATUS st = m->api->SetParam(NULL, QUIC_PARAM_GLOBAL_SETTINGS,
                                      (uint32_t)sizeof(floor), &floor);
#ifdef MOQ_MSQUIC_TESTING
    if (QUIC_SUCCEEDED(st) && moq_msq_test_after_global_write != NULL)
        moq_msq_test_after_global_write(m->api);
    if (moq_msq_test_force_global_status_active)
        st = moq_msq_test_force_global_status;
#endif
    return QUIC_FAILED(st) ? MOQ_ERR_INTERNAL : MOQ_OK;
}

moq_result_t moq_msquic_managed_create(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    /* the frozen prefix ends where the appended fields begin: an older
     * caller's smaller struct is valid, its appended fields defaulted */
    if (cfg == NULL ||
        cfg->struct_size < offsetof(moq_msquic_managed_cfg_t, version))
        return MOQ_ERR_INVAL;
    if (cfg->alloc == NULL || cfg->alloc->alloc == NULL ||
        cfg->alloc->realloc == NULL || cfg->alloc->free == NULL)
        return MOQ_ERR_INVAL;
    if (cfg->on_lane_pump == NULL)
        return MOQ_ERR_INVAL;
    bool is_client = cfg->perspective == MOQ_PERSPECTIVE_CLIENT;
    if (!is_client && cfg->perspective != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;
    if (is_client && (cfg->host == NULL || cfg->port == 0))
        return MOQ_ERR_INVAL;
    if (!is_client &&
        (cfg->cert_path == NULL || cfg->key_path == NULL))
        return MOQ_ERR_INVAL;

    mgd_derived_cfg_t derived;

    mgd_derive_cfg(cfg, is_client, &derived);
    moq_version_t exact_version = derived.version;
    if (moq_alpn_for_version(exact_version) == NULL)
        return MOQ_ERR_UNSUPPORTED;

    const moq_version_t *offered_versions = &exact_version;
    size_t offered_count = 1;
    bool multi_version_server = false;
    if (mgd_cfg_has_version_list_block(cfg)) {
        if ((cfg->versions == NULL) != (cfg->version_count == 0))
            return MOQ_ERR_INVAL;
        if (cfg->version_count != 0) {
            if (is_client || cfg->version != 0)
                return MOQ_ERR_INVAL;
            moq_result_t vr =
                mgd_validate_version_list(cfg->versions, cfg->version_count);
            if (vr != MOQ_OK)
                return vr;
            offered_versions = cfg->versions;
            offered_count = cfg->version_count;
            multi_version_server = true;
        }
    }

    /* Reject an out-of-range subgroup pool HERE -- before any allocation or
     * transport setup -- so the caller sees the core's own MOQ_ERR_INVAL
     * instead of a downstream INTERNAL (client) or a late child-creation
     * failure that reads as transport OOM (server). Only when struct_size
     * covers the whole appended field; a prefix/partial size still ignores
     * whatever occupies those bytes. */
    if (cfg->struct_size >=
            offsetof(moq_msquic_managed_cfg_t, max_open_subgroups) +
                sizeof(cfg->max_open_subgroups) &&
        cfg->max_open_subgroups > 0xffffu)
        return MOQ_ERR_INVAL;

    moq_msquic_managed_t *m =
        cfg->alloc->alloc(sizeof(*m), cfg->alloc->ctx);
    if (m == NULL)
        return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *cfg->alloc;
    /* The helper also applies the app-deadline whole-block ABI gate. */
    mgd_copy_cfg(m, cfg);
    mgd_apply_derived_cfg(m, &derived);
    m->version = multi_version_server ? (moq_version_t)0 : exact_version;
    pthread_mutex_init(&m->mu, NULL);
    pthread_cond_init(&m->cv, NULL);
    moq_result_t ar = mgd_store_alpns(m, offered_versions, offered_count);
    if (ar != MOQ_OK) {
        mgd_free(m);
        return ar;
    }
    if (multi_version_server) {
        m->cfg.versions = m->versions;
        m->cfg.version_count = m->version_count;
    }
    if (mgd_lanes_alloc(m) != MOQ_OK) {
        mgd_free(m);
        return MOQ_ERR_NOMEM;
    }
    m->host = mgd_strdup(&m->alloc, cfg->host);
    m->cert_path = mgd_strdup(&m->alloc, cfg->cert_path);
    m->key_path = mgd_strdup(&m->alloc, cfg->key_path);
    if ((cfg->host != NULL && m->host == NULL) ||
        (cfg->cert_path != NULL && m->cert_path == NULL) ||
        (cfg->key_path != NULL && m->key_path == NULL)) {
        mgd_free(m);
        return MOQ_ERR_NOMEM;
    }

    moq_result_t apirc = mgd_open_api(m, NULL);

    if (apirc != MOQ_OK) {
        mgd_free(m);
        return apirc;
    }
    /* A managed SERVER lowers the PROCESS-GLOBAL minimum MTU floor to 1280 here,
     * after MsQuicOpen2 and before RegistrationOpen, so the pre-configuration
     * connection reply is not sized from MsQuic's 1288 default (the new
     * connection seeds its path MTU from the library-global settings before a
     * configuration is attached). This is process-wide (there is no
     * registration-scoped settings param): clients later created in the same
     * process inherit the conservative 1280 initial floor, though their own
     * configuration declares no MTU override and no global maximum is imposed.
     * A managed CLIENT does NOT issue this write. */
    if (!is_client && mgd_apply_global_server_floor(m) != MOQ_OK) {
        mgd_close_transport(m); /* closes the opened API table (nothing else open yet) */
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }
#ifdef MOQ_MSQUIC_TESTING
    /* Test-only: abort immediately after the perspective-gated global-floor
     * decision, before RegistrationOpen and any socket. Lets a focused test
     * prove -- socket-free -- that a SERVER ran the real global write (verified
     * by reading the settings back with GetParam in the post-write probe) and a
     * CLIENT did not, then tears the create down cleanly. Compiled only under
     * MOQ_MSQUIC_TESTING; absent from the production symbol surface. */
    if (moq_msq_test_abort_after_global) {
        mgd_close_transport(m);
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }
#endif
#ifdef MOQ_MSQUIC_TESTING
    moq_msq_test_registration_reached = true;
#endif
    QUIC_REGISTRATION_CONFIG rcfg = {
        .AppName = "moq-msquic-managed",
        .ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY,
    };
    if (QUIC_FAILED(m->api->RegistrationOpen(&rcfg,
                                             &m->registration))) {
        mgd_close_transport(m);
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }

    QUIC_SETTINGS settings;
    mgd_build_settings(cfg, &settings);
    if (QUIC_FAILED(m->api->ConfigurationOpen(
            m->registration, m->alpns, m->alpn_count, &settings,
            sizeof(settings), NULL, &m->configuration))) {
        mgd_close_transport(m);
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }

    QUIC_CREDENTIAL_CONFIG cred;
    QUIC_CERTIFICATE_FILE cert_file;
    memset(&cred, 0, sizeof(cred));
    if (is_client) {
        cred.Type = QUIC_CREDENTIAL_TYPE_NONE;
        cred.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
        if (cfg->insecure_skip_verify)
            cred.Flags |=
                QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
    } else {
        memset(&cert_file, 0, sizeof(cert_file));
        cert_file.CertificateFile = m->cert_path;
        cert_file.PrivateKeyFile = m->key_path;
        cred.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
        cred.CertificateFile = &cert_file;
    }
    if (QUIC_FAILED(m->api->ConfigurationLoadCredential(
            m->configuration, &cred))) {
        mgd_close_transport(m);
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }

    if (is_client) {
        moq_msquic_managed_conn_t *mc = NULL;

        if (mgd_make_child(m, m->version, &mc) != MOQ_OK) {
            mgd_close_transport(m);
            mgd_free(m);
            return MOQ_ERR_INTERNAL;
        }
        /* the client's one connection lives on lane 0; reserve its slot
         * and place it before the transport can deliver callbacks */
        m->conn_count = 1;
        pthread_mutex_lock(&m->lanes[0].mu);
        mgd_lane_append(&m->lanes[0], mc);
        pthread_mutex_unlock(&m->lanes[0].mu);
        if (QUIC_FAILED(m->api->ConnectionOpen(
                m->registration, moq_msquic_conn_callback(), mc->conn,
                &mc->connection)) ||
            moq_msquic_conn_bind(mc->conn, mc->connection) != MOQ_OK ||
            QUIC_FAILED(m->api->ConnectionStart(
                mc->connection, m->configuration,
                QUIC_ADDRESS_FAMILY_UNSPEC, m->host, cfg->port))) {
            /* close the transport FIRST: ConnectionClose drains any
             * callbacks still referencing the conn about to die */
            if (mc->connection != NULL) {
                m->api->ConnectionClose(mc->connection);
                mc->connection = NULL;
            }
            mgd_close_transport(m);
            pthread_mutex_lock(&m->lanes[0].mu);
            mgd_lane_remove(&m->lanes[0], mc);
            pthread_mutex_unlock(&m->lanes[0].mu);
            m->conn_count = 0;
            moq_msquic_conn_destroy(mc->conn);
            moq_session_destroy(mc->session);
            m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
            mgd_free(m);
            return MOQ_ERR_INTERNAL;
        }
    } else {
        QUIC_ADDR addr;

        memset(&addr, 0, sizeof(addr));
        QuicAddrSetFamily(&addr, QUIC_ADDRESS_FAMILY_UNSPEC);
        if (m->host != NULL &&
            !QuicAddrFromString(m->host, cfg->port, &addr)) {
            mgd_close_transport(m);
            mgd_free(m);
            return MOQ_ERR_INVAL;
        }
        QuicAddrSetPort(&addr, cfg->port);
        if (QUIC_FAILED(m->api->ListenerOpen(
                m->registration, mgd_listener_cb, m, &m->listener)) ||
            QUIC_FAILED(m->api->ListenerStart(m->listener, m->alpns,
                                              m->alpn_count, &addr))) {
            mgd_close_transport(m);
            mgd_free(m);
            return MOQ_ERR_INTERNAL;
        }
        QUIC_ADDR bound;
        uint32_t blen = sizeof(bound);
        if (QUIC_SUCCEEDED(m->api->GetParam(
                m->listener, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, &blen,
                &bound)))
            m->bound_port = QuicAddrGetPort(&bound);
    }

    /* spawn one doorbell per lane */
    bool spawn_ok = true;
    mgd_freeze_doorbell_decision(m);
    for (uint32_t i = 0; i < m->lane_count && spawn_ok; i++)
        spawn_ok = mgd_spawn_doorbell(m, &m->lanes[i]);
    if (!spawn_ok) {
        /* undo: transport is live. Signal stop so any spawned doorbell
         * quiesces + exits, join them, close the listener, then shut +
         * free every connection lock-free. */
        pthread_mutex_lock(&m->mu);
        atomic_store_explicit(&m->stop_requested, true,
                              memory_order_release);
        pthread_mutex_unlock(&m->mu);
        for (uint32_t i = 0; i < m->lane_count; i++) {
            pthread_mutex_lock(&m->lanes[i].mu);
            pthread_cond_broadcast(&m->lanes[i].cv);
            mgd_bell_ring(&m->lanes[i]);
            pthread_mutex_unlock(&m->lanes[i].mu);
            if (m->lanes[i].doorbell_running) {
                pthread_join(m->lanes[i].doorbell, NULL);
                m->lanes[i].doorbell_running = false;
            }
        }
        if (m->listener != NULL) {
            m->api->ListenerClose(m->listener);
            m->listener = NULL;
        }
        for (uint32_t i = 0; i < m->lane_count; i++)
            while (m->lanes[i].conns != NULL) {
                moq_msquic_managed_conn_t *mc = m->lanes[i].conns;

                m->lanes[i].conns = mc->next;
                mgd_free_child(m, mc);
            }
        mgd_close_transport(m);
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }
    *out = m;
    return MOQ_OK;
}

#ifdef MOQ_MSQUIC_TESTING
/* Test-only: build the facade's LANE AND DOORBELL state and nothing else — no
 * registration, no configuration, no credential, no listener, no socket, no
 * certificate file. A scheduling test can then drive the real
 * doorbell_main -> doorbell_pump_sweep -> doorbell_reap transition on a host
 * where no UDP bind is possible.
 *
 * The default helper opens the MsQuic API table because every child needs one;
 * the borrowed-table variant instead uses a caller-owned fake. Neither path
 * opens a registration, configuration, listener, or socket.
 *
 * What this facade CANNOT do, by construction: listen, connect, or carry a
 * single byte over a real transport. Tests may drive the production listener
 * callback over a synthesized identity, inject a terminal-only child, or bind
 * a live child to a fake connection handle through the helpers below.
 * Teardown is the ordinary public stop()/destroy() path — mgd_close_transport
 * skips the handles that were never opened.
 *
 * Never compiled into production libraries. */
static moq_result_t mgd_test_create_lanes_only(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *borrowed_api,
    moq_msquic_managed_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    if (cfg == NULL || cfg->alloc == NULL || cfg->on_lane_pump == NULL ||
        cfg->struct_size < offsetof(moq_msquic_managed_cfg_t, version))
        return MOQ_ERR_INVAL;

    /* Same size-qualified derivation the public constructor uses, so a
     * prefix-sized cfg defaults identically on both paths. */
    mgd_derived_cfg_t derived;

    mgd_derive_cfg(cfg, cfg->perspective == MOQ_PERSPECTIVE_CLIENT, &derived);
    if (moq_alpn_for_version(derived.version) == NULL)
        return MOQ_ERR_UNSUPPORTED;

    moq_msquic_managed_t *m = cfg->alloc->alloc(sizeof(*m), cfg->alloc->ctx);

    if (m == NULL)
        return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *cfg->alloc;
    mgd_copy_cfg(m, cfg);
    mgd_apply_derived_cfg(m, &derived);
    pthread_mutex_init(&m->mu, NULL);
    pthread_cond_init(&m->cv, NULL);
    moq_result_t ar = mgd_store_alpns(m, &m->version, 1);
    if (ar != MOQ_OK) {
        mgd_free(m);
        return ar;
    }
    if (mgd_lanes_alloc(m) != MOQ_OK) {
        mgd_free(m);
        return MOQ_ERR_NOMEM;
    }
    moq_result_t apirc = mgd_open_api(m, borrowed_api);

    if (apirc != MOQ_OK) {
        mgd_free(m);
        return apirc;
    }

    bool spawn_ok = true;

    mgd_freeze_doorbell_decision(m);
    for (uint32_t i = 0; i < m->lane_count && spawn_ok; i++)
        spawn_ok = mgd_spawn_doorbell(m, &m->lanes[i]);
    if (!spawn_ok) {
        atomic_store_explicit(&m->stop_requested, true, memory_order_release);
        for (uint32_t i = 0; i < m->lane_count; i++) {
            pthread_mutex_lock(&m->lanes[i].mu);
            m->lanes[i].stop_requested = true;
            pthread_cond_broadcast(&m->lanes[i].cv);
            mgd_bell_ring(&m->lanes[i]);
            pthread_mutex_unlock(&m->lanes[i].mu);
            if (m->lanes[i].doorbell_running) {
                pthread_join(m->lanes[i].doorbell, NULL);
                m->lanes[i].doorbell_running = false;
            }
        }
        mgd_close_transport(m);
        mgd_free(m);
        return MOQ_ERR_INTERNAL;
    }
    *out = m;
    return MOQ_OK;
}

moq_result_t moq_msq_test_managed_create_lanes_only(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out)
{
    return mgd_test_create_lanes_only(cfg, NULL, out);
}

/* Test-only borrowed-table variant. The fake table remains owned by the test;
 * the facade uses it for real adapter downcalls but never closes it. */
moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out)
{
    if (api == NULL) {
        if (out != NULL)
            *out = NULL;
        return MOQ_ERR_INVAL;
    }
    return mgd_test_create_lanes_only(cfg, api, out);
}

/* Run exactly one production doorbell iteration on the calling thread. This
 * is the SAME mgd_doorbell_step the doorbell thread executes -- the only
 * thing the thread adds is the wait -- so a single-threaded test drives real
 * transitions rather than a model of them. */
moq_msq_test_step_t moq_msq_test_lane_step(moq_msquic_managed_lane_t *lane)
{
    uint64_t until = 0;
    bool at_cap = false;
    mgd_step_kind_t kind;

    if (lane == NULL)
        return MOQ_MSQ_TEST_STEP_IDLE;
    pthread_mutex_lock(&lane->mu);
    kind = mgd_doorbell_step(lane, &until, &at_cap);
    pthread_mutex_unlock(&lane->mu);
    /* Mapped explicitly, never cast: the seam is a source contract of its own
     * and must not silently inherit an internal enum's ordinal order. */
    switch (kind) {
    case MGD_STEP_PUMPED:
        return MOQ_MSQ_TEST_STEP_PUMPED;
    case MGD_STEP_TICKED:
        return MOQ_MSQ_TEST_STEP_TICKED;
    case MGD_STEP_STOP:
        return MOQ_MSQ_TEST_STEP_STOP;
    case MGD_STEP_IDLE:
        break;
    }
    return MOQ_MSQ_TEST_STEP_IDLE;
}

/* Observation only, under the lane lock: every field below is read, none is
 * written, and terminal state comes from the bridge's own facts rather than
 * from any acknowledging call. Returns the number of children actually
 * linked, which may exceed `cap` -- a caller seeing that must treat the
 * picture as incomplete rather than read a silently truncated set. */
size_t moq_msq_test_lane_snapshot(moq_msquic_managed_lane_t *lane,
                                  moq_msq_test_lane_row_t *lane_row,
                                  moq_msq_test_child_row_t *rows, size_t cap)
{
    size_t linked = 0;

    if (lane == NULL)
        return 0;
    if (moq_msq_test_snapshot_pre_lock != NULL)
        moq_msq_test_snapshot_pre_lock(lane);
    pthread_mutex_lock(&lane->mu);
    if (lane_row != NULL) {
        moq_msquic_managed_t *m = lane->owner;

        memset(lane_row, 0, sizeof(*lane_row));
        lane_row->doorbell_running = lane->doorbell_running;
        lane_row->wake_pending = lane->wake_pending;
        lane_row->pump_pending = lane->pump_pending;
        lane_row->ext_wake =
            atomic_load_explicit(&lane->ext_wake, memory_order_acquire);
        lane_row->pump_exit = lane->pump_exit;
        lane_row->lane_stop = lane->stop_requested;
        lane_row->facade_stop =
            m != NULL && atomic_load_explicit(&m->stop_requested,
                                              memory_order_acquire);
        lane_row->in_sweep = lane->in_sweep;
        lane_row->in_lane_pump = lane->in_lane_pump;
        lane_row->bell_gen = mgd_bell_snapshot(lane);
        lane_row->conn_count = lane->conn_count;
        lane_row->pump_sweeps = lane->st_pump_sweeps;
        lane_row->service_passes = lane->st_service_passes;
        lane_row->deadline_sweeps = lane->st_deadline_sweeps;
        lane_row->idle_cap_wakes = lane->st_idle_cap_wakes;
    }
    for (moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
         mc = mc->next) {
        if (rows != NULL && linked < cap) {
            moq_msq_test_child_row_t *r = &rows[linked];
            bool observed = false;

            memset(r, 0, sizeof(*r));
            r->id = mc;
            r->user = mc->user;
            r->reapable = mc->reapable;
            r->app_terminal_acked = mc->app_terminal_acked;
            if (mc->conn != NULL) {
                r->shutdown_complete = mc->conn->shutdown_complete;
                r->has_events = moq_msq_conn_has_events(mc->conn);
                r->close_feed_commits = mc->conn->test_close_feed_commits;
                if (mc->conn->bridge != NULL) {
                    r->terminal_enqueued =
                        moq_transport_bridge_terminal_facts(mc->conn->bridge,
                                                            &observed);
                    r->terminal_observed = observed;
                    r->bridge_fatal =
                        moq_transport_bridge_is_fatal(mc->conn->bridge);
                    r->bridge_fatal_code =
                        moq_transport_bridge_fatal_code(mc->conn->bridge);
                }
            }
        }
        linked++;
    }
    pthread_mutex_unlock(&lane->mu);
    return linked;
}

/* Test-only synthesized accept: hand the EXACT production listener callback a
 * NEW_CONNECTION event for a supplied connection handle and return its real
 * status. Reserve, choose_lane, the range check, the stop/drain re-check, the
 * child construction, the lane append, the handler registration, the bind and
 * the wake all run unchanged — nothing about the selection path is reproduced
 * here. */
QUIC_STATUS moq_msq_test_listener_accept(moq_msquic_managed_t *m,
                                         HQUIC connection)
{
    QUIC_LISTENER_EVENT ev;
    QUIC_NEW_CONNECTION_INFO info;

    if (m == NULL || m->alpn_count == 0)
        return QUIC_STATUS_CONNECTION_REFUSED;

    memset(&ev, 0, sizeof(ev));
    memset(&info, 0, sizeof(info));
    info.NegotiatedAlpn = m->alpns[0].Buffer;
    info.NegotiatedAlpnLength = (uint16_t)m->alpns[0].Length;
    ev.Type = QUIC_LISTENER_EVENT_NEW_CONNECTION;
    ev.NEW_CONNECTION.Connection = connection;
    ev.NEW_CONNECTION.Info = &info;
    return mgd_listener_cb(NULL, m, &ev);
}

/* Test-only live child: reserve and construct through the production child
 * path, bind it to a supplied fake connection handle, publish it to the lane,
 * and arm the same initial pump a real accept does. No callback handler is
 * registered here because the fake test drives the production connection
 * callback explicitly. */
moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out)
{
    if (lane == NULL || connection == NULL || out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    moq_msquic_managed_t *m = lane->owner;
    moq_msquic_managed_conn_t *mc = NULL;

    pthread_mutex_lock(&m->mu);
    if (m->conn_count >= m->max_connections || m->draining ||
        atomic_load_explicit(&m->stop_requested, memory_order_acquire)) {
        pthread_mutex_unlock(&m->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    m->conn_count++;
    pthread_mutex_unlock(&m->mu);

    moq_result_t rc = mgd_make_child(m, m->version, &mc);
    if (rc != MOQ_OK) {
        mgd_release_reserve(m);
        return rc;
    }

    pthread_mutex_lock(&lane->mu);
    if (lane->stop_requested) {
        pthread_mutex_unlock(&lane->mu);
        moq_msquic_conn_destroy(mc->conn);
        moq_session_destroy(mc->session);
        m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
        mgd_release_reserve(m);
        return MOQ_ERR_WRONG_STATE;
    }
    mgd_lane_append(lane, mc);
    rc = moq_msquic_conn_bind(mc->conn, connection);
    if (rc != MOQ_OK) {
        mgd_lane_remove(lane, mc);
        pthread_mutex_unlock(&lane->mu);
        moq_msquic_conn_destroy(mc->conn);
        moq_session_destroy(mc->session);
        m->alloc.free(mc, sizeof(*mc), m->alloc.ctx);
        mgd_release_reserve(m);
        return rc;
    }
    mc->connection = connection;
    lane->wake_pending = true;
    pthread_cond_broadcast(&lane->cv);
    mgd_bell_ring(lane);
    pthread_mutex_unlock(&lane->mu);
    mgd_bump_activity(m);
    *out = mc;
    return MOQ_OK;
}

moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn)
{
    return conn != NULL ? conn->conn : NULL;
}
#endif

/* --- stop / destroy --------------------------------------------------------- */

moq_result_t moq_msquic_managed_stop(moq_msquic_managed_t *m)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    /* stop from inside any managed callback (on_lane_pump, on_activity)
     * would deadlock on the lane mutex this thread already holds and then
     * join the doorbell it is running on */
    if (mgd_tls_cb == m)
        return MOQ_ERR_WRONG_STATE;

    pthread_mutex_lock(&m->mu);
    if (m->stopped) {
        pthread_mutex_unlock(&m->mu);
        return MOQ_OK;
    }
    atomic_store_explicit(&m->stop_requested, true, memory_order_release);
    m->draining = true;
    pthread_mutex_unlock(&m->mu);
#ifdef MOQ_MSQUIC_TESTING
    if (moq_msq_test_stop_latched != NULL)
        moq_msq_test_stop_latched(m); /* latched, and no lane lock taken yet */
#endif

    /* signal every lane's doorbell: each shuts its own connections down
     * and waits (under its own lane->mu) for their SHUTDOWN_COMPLETE,
     * then exits — so quiescence is per-lane, no cross-lane locking */
    for (uint32_t i = 0; i < m->lane_count; i++) {
        pthread_mutex_lock(&m->lanes[i].mu);
        m->lanes[i].stop_requested = true;
        pthread_cond_broadcast(&m->lanes[i].cv);
        mgd_bell_ring(&m->lanes[i]);
        pthread_mutex_unlock(&m->lanes[i].mu);
    }
    for (uint32_t i = 0; i < m->lane_count; i++)
        if (m->lanes[i].doorbell_running) {
            pthread_join(m->lanes[i].doorbell, NULL);
            m->lanes[i].doorbell_running = false;
        }

    /* every doorbell is gone and every connection quiesced: the
     * blocking closes (which drain callbacks) are now lock-free */
    if (m->listener != NULL) {
        m->api->ListenerClose(m->listener);
        m->listener = NULL;
    }
    for (uint32_t i = 0; i < m->lane_count; i++)
        while (m->lanes[i].conns != NULL) {
            moq_msquic_managed_conn_t *mc = m->lanes[i].conns;

            /* fold flush totals before the child is freed, exactly as the
             * doorbell reap does — lifetime stats must survive stop() */
            mgd_lane_latch_flush(&m->lanes[i], mc);
            m->lanes[i].conns = mc->next;
            m->lanes[i].conn_count--;
            m->conn_count--;
            mgd_free_child(m, mc);
        }
    /* every connection quiesced (SHUTDOWN_COMPLETE) then freed: all sends
     * have completed or canceled, so the true pending counts are 0. The
     * per-lane mirrors were last refreshed by a sweep that may have seen
     * in-flight bytes and no sweep runs after quiescence — zero them so
     * the aggregate accessors reflect the drained state. */
    pthread_mutex_lock(&m->mu);
    for (uint32_t i = 0; i < m->lane_count; i++) {
        m->lanes[i].m_pending_sends = 0;
        m->lanes[i].m_pending_dgrams = 0;
    }
    /* under m->mu: m->stopped's reader (stop's idempotency check) holds
     * m->mu, so its writer must too. */
    m->stopped = true;
    pthread_mutex_unlock(&m->mu);
    return MOQ_OK;
}

void moq_msquic_managed_destroy(moq_msquic_managed_t *m)
{
    if (m == NULL)
        return;
    (void)moq_msquic_managed_stop(m);
    mgd_close_transport(m);
    mgd_free(m);
}

/* --- accessors ---------------------------------------------------------------- */

static bool mgd_in_lane_pump(const moq_msquic_managed_lane_t *lane);

moq_session_t *moq_msquic_managed_session(moq_msquic_managed_t *m)
{
    /* CLIENT ONLY: the single connection lives on lane 0 and this is
     * valid only inside its lane pump (which holds lane 0's mu). On a
     * server this is not a correctness path — return NULL. */
    if (m == NULL || m->cfg.perspective != MOQ_PERSPECTIVE_CLIENT)
        return NULL;
    /* Pump window only, like conn_session: NULL outside lane 0's on_lane_pump
     * (e.g. from on_activity or an app thread) so a session handle can never
     * escape its lock domain. conns != NULL also covers "no live conn yet". */
    if (!mgd_in_lane_pump(&m->lanes[0]))
        return NULL;
    return m->lanes[0].conns != NULL ? m->lanes[0].conns->session : NULL;
}

static moq_msquic_managed_t *mgd_mut(const moq_msquic_managed_t *m)
{
    return (moq_msquic_managed_t *)(uintptr_t)m;
}

/* -- Lanes ---------------------------------------------------------- */

uint32_t moq_msquic_managed_lane_count(const moq_msquic_managed_t *m)
{
    return m != NULL ? m->lane_count : 0; /* immutable after create */
}

moq_msquic_managed_lane_t *moq_msquic_managed_lane(
    moq_msquic_managed_t *m, uint32_t index)
{
    if (m == NULL || index >= m->lane_count)
        return NULL;
    return &m->lanes[index];
}

uint32_t moq_msquic_lane_index(const moq_msquic_managed_lane_t *lane)
{
    return lane != NULL ? lane->index : 0;
}

/* Iterate exactly the connections whose lane->mu the RUNNING pump for
 * THIS lane holds (the sweep snapshot). Only meaningful inside this
 * lane's on_lane_pump (mgd_tls_lane == lane), so a pump never sees
 * another lane's connections, and sees ALL of its own in one pass. */
/* The per-connection lane API is valid ONLY inside cfg.on_lane_pump: on
 * the owning lane's doorbell thread (mgd_tls_lane == lane) AND within the
 * pump call (lane->in_lane_pump). on_activity runs on the same thread with
 * mgd_tls_lane still set, but in_lane_pump cleared — so it is refused. */
static bool mgd_in_lane_pump(const moq_msquic_managed_lane_t *lane)
{
    return lane != NULL && mgd_tls_lane == lane && lane->in_lane_pump;
}

moq_msquic_managed_conn_t *moq_msquic_lane_next_conn(
    moq_msquic_managed_lane_t *lane, moq_msquic_managed_conn_t *prev)
{
    if (!mgd_in_lane_pump(lane))
        return NULL;
    if (prev == NULL)
        return lane->sweep_count > 0 ? lane->sweep[0] : NULL;
    for (size_t i = 0; i < lane->sweep_count; i++)
        if (lane->sweep[i] == prev)
            return i + 1 < lane->sweep_count ? lane->sweep[i + 1] : NULL;
    return NULL;
}

/* Open (or re-arm) the explicit pending-wake cycle: the FIRST accepted
 * wake since the last consuming pump CASes the stamp from 0 and starts
 * the cycle; a loser found the cycle already open — that is the
 * coalesced case, and it must not restart the latency clock. Callable
 * with or without lane->mu (the stamp and the coalesced counter are the
 * only stats state the lock-free cross-lane arm touches). */
static void mgd_wake_cycle_arm(moq_msquic_managed_lane_t *lane)
{
    uint64_t expected = 0;
    if (!atomic_compare_exchange_strong(&lane->st_wake_stamp_us, &expected,
                                        mgd_now_us()))
        atomic_fetch_add(&lane->st_wakes_coalesced, 1);
}

moq_result_t moq_msquic_lane_wake(moq_msquic_managed_lane_t *lane)
{
    if (lane == NULL)
        return MOQ_ERR_INVAL;
    /* same lane as the callback context on this thread: its mutex is
     * already held — a lane pump, a transport-event guard, or on_activity
     * fired inside either — and the doorbell re-checks wake/pump when it
     * regains control. Just arm; never re-lock the held mutex. */
    if (mgd_tls_lane == lane) {
        lane->st_wakes_same_lane++;
        mgd_wake_cycle_arm(lane);
        lane->wake_pending = true;
        return MOQ_OK;
    }
    /* inside a DIFFERENT lane's callback on this thread: that lane's mutex
     * is held, so blocking on THIS lane's mutex risks an A->B / B->A
     * deadlock against the other lane waking us back. Never block on
     * lane->mu — set the lock-free ext_wake flag for the doorbell's
     * predicate, then ring the bell: its leaf mutex is safe from any
     * context, its generation makes the signal durable across the
     * doorbell's check->wait window, and an idle target therefore pumps
     * promptly instead of sleeping to the idle cap. */
    if (mgd_tls_cb == lane->owner) {
        atomic_fetch_add(&lane->st_wakes_cross_lane, 1);
        mgd_wake_cycle_arm(lane);
        atomic_store_explicit(&lane->ext_wake, true, memory_order_release);
        mgd_bell_ring(lane);
        return MOQ_OK;
    }
    /* external (app) thread, holding no lane mutex: safe to block. */
    pthread_mutex_lock(&lane->mu);
    lane->st_wakes_external++;
    mgd_wake_cycle_arm(lane);
    lane->wake_pending = true;
    pthread_cond_broadcast(&lane->cv);
    mgd_bell_ring(lane);
    pthread_mutex_unlock(&lane->mu);
    return MOQ_OK;
}

/* Assemble the full stats view (lane->mu held, or the calling context IS
 * this lane's callback and the doorbell holds it). Live children's flush
 * totals are summed on top of the reaped latch. */
static void mgd_lane_stats_fill(const moq_msquic_managed_lane_t *lane,
                                moq_msquic_lane_stats_t *full)
{
    memset(full, 0, sizeof(*full));
    full->struct_size = (uint32_t)sizeof(*full);
    full->wakes_same_lane = lane->st_wakes_same_lane;
    full->wakes_cross_lane = atomic_load(&lane->st_wakes_cross_lane);
    full->wakes_external = lane->st_wakes_external;
    full->wakes_coalesced = atomic_load(&lane->st_wakes_coalesced);
    full->pump_sweeps = lane->st_pump_sweeps;
    full->deadline_sweeps = lane->st_deadline_sweeps;
#ifdef MOQ_MSQUIC_TESTING
    full->deadline_queries = lane->st_deadline_queries;
    full->deadline_class_none = lane->st_deadline_class_none;
    full->deadline_class_future = lane->st_deadline_class_future;
    full->deadline_class_due = lane->st_deadline_class_due;
#endif
    full->idle_cap_wakes = lane->st_idle_cap_wakes;
    full->wake_to_pump_max_us = lane->st_wake_to_pump_max_us;
    full->wake_to_pump_total_us = lane->st_wake_to_pump_total_us;
    full->wake_to_pump_samples = lane->st_wake_to_pump_samples;
    full->service_passes = lane->st_service_passes;
    full->flush_sends = lane->st_flush_sends_reaped;
    full->flush_bytes = lane->st_flush_bytes_reaped;
    for (const moq_msquic_managed_conn_t *mc = lane->conns; mc != NULL;
         mc = mc->next)
        if (mc->conn != NULL) {
            full->flush_sends += mc->conn->flush_sends;
            full->flush_bytes += mc->conn->flush_bytes;
        }
}

moq_result_t moq_msquic_lane_get_stats(moq_msquic_managed_lane_t *lane,
                                       moq_msquic_lane_stats_t *out,
                                       size_t out_size)
{
    if (lane == NULL || out == NULL ||
        out_size < MOQ_MSQUIC_LANE_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;

    moq_msquic_lane_stats_t full;

    /* Caller-context rule (mirrors moq_msquic_lane_wake's arms): this
     * lane's own callback already holds the mutex — read directly, never
     * re-lock. A DIFFERENT lane's callback must not block on this lane's
     * mutex (A->B/B->A deadlock against a wake coming back): refused. An
     * external thread snapshots under the lock. */
    if (mgd_tls_lane == lane) {
        mgd_lane_stats_fill(lane, &full);
    } else if (mgd_tls_cb == lane->owner) {
        return MOQ_ERR_WRONG_STATE;
    } else {
#ifdef MOQ_MSQUIC_TESTING
        if (moq_msq_test_stats_pre_lock != NULL)
            moq_msq_test_stats_pre_lock(lane);
#endif
        pthread_mutex_lock(&lane->mu);
        mgd_lane_stats_fill(lane, &full);
        pthread_mutex_unlock(&lane->mu);
    }

    /* Sized copy: write at most out_size bytes, at most what this library
     * knows; zero any caller remainder beyond that (a larger future struct
     * against this lib reads its appended fields as zero, never garbage);
     * stamp the bytes written. out_size >= V0_SIZE is guaranteed above. */
    size_t n = out_size < sizeof(full) ? out_size : sizeof(full);
    full.struct_size = (uint32_t)n;
    memcpy(out, &full, n);
    if (out_size > sizeof(full))
        memset((uint8_t *)out + sizeof(full), 0, out_size - sizeof(full));
    return MOQ_OK;
}

/* -- Connections ---------------------------------------------------- */

moq_session_t *moq_msquic_managed_conn_session(
    moq_msquic_managed_conn_t *conn)
{
    /* pump window only: a handle observed during on_lane_pump must not be
     * used from on_activity or any other context */
    if (conn == NULL || !mgd_in_lane_pump(conn->lane))
        return NULL;
    return conn->session;
}

moq_msquic_managed_lane_t *moq_msquic_managed_conn_lane(
    moq_msquic_managed_conn_t *conn)
{
    return conn != NULL ? conn->lane : NULL;
}

void moq_msquic_managed_conn_set_user(moq_msquic_managed_conn_t *conn,
                                      void *user)
{
    if (conn != NULL)
        conn->user = user;
}

void *moq_msquic_managed_conn_user(const moq_msquic_managed_conn_t *conn)
{
    return conn != NULL ? conn->user : NULL;
}

moq_result_t moq_msquic_managed_conn_ack_terminal(
    moq_msquic_managed_conn_t *conn)
{
    if (conn == NULL)
        return MOQ_ERR_INVAL;
    /* The single client connection is not reclaimed per-child, so there is
     * nothing to acknowledge — refuse rather than record a fact the reap
     * gate never reads. */
    if (conn->owner->cfg.perspective == MOQ_PERSPECTIVE_CLIENT)
        return MOQ_ERR_WRONG_STATE;
    /* Owning lane's pump only: that window is what makes `conn` a valid
     * handle at all, and it is the lock domain this flag lives in. */
    if (!mgd_in_lane_pump(conn->lane))
        return MOQ_ERR_WRONG_STATE;
    /* The terminal must already have been TRANSFERRED to the application by a
     * poll of this connection's session. A queued-but-unpolled SESSION_CLOSED
     * is not enough, and neither is a completed transport shutdown: the two
     * facts are unordered, and reclaiming on the transport one is exactly the
     * defect this gate closes. Observation is a durable session fact, so the
     * acknowledging pump need not be the one that polled. */
    bool observed = false;

    (void)moq_transport_bridge_terminal_facts(conn->conn->bridge, &observed);
    if (!observed)
        return MOQ_ERR_WRONG_STATE;
    conn->app_terminal_acked = true; /* monotonic; a duplicate is accepted */
    return MOQ_OK;
}

void moq_msquic_managed_conn_close(moq_msquic_managed_conn_t *conn,
                                   uint64_t code)
{
    /* valid only inside the owning lane's on_lane_pump (lane->mu held, so
     * the conn state is touched safely) — a no-op from on_activity or any
     * other context, so a stray close there cannot tear down a live conn */
    if (conn == NULL || !mgd_in_lane_pump(conn->lane))
        return;
    if (conn->connection == NULL || conn->conn->shutdown_complete)
        return;
    if (!conn->conn->close_pending) {
        conn->conn->close_pending = true;
        conn->conn->close_clean = true;
        conn->conn->close_code = code;
    }
    /* async, never blocks; final events arrive on a later lane pump */
    conn->owner->api->ConnectionShutdown(
        conn->connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, code);
}

size_t moq_msquic_managed_conn_count(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    size_t v;

    if (m == NULL)
        return 0;
    /* facade-wide reserve; m->mu is never held across a user callback,
     * and taking it while a lane pump holds lane->mu respects the
     * lane->mu -> m->mu order */
    pthread_mutex_lock(&m->mu);
    v = m->conn_count;
    pthread_mutex_unlock(&m->mu);
    return v;
}

void moq_msquic_managed_drain(moq_msquic_managed_t *m)
{
    if (m == NULL)
        return;
    pthread_mutex_lock(&m->mu);
    m->draining = true;
    pthread_mutex_unlock(&m->mu);
}

uint16_t moq_msquic_managed_port(const moq_msquic_managed_t *m)
{
    return m != NULL ? m->bound_port : 0;
}

moq_result_t moq_msquic_managed_wake(moq_msquic_managed_t *m)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    /* a server facade outlives any one connection: only stop/pump-exit
     * are terminal for it; a CLIENT's life is its single connection
     * (the client terminal latch) */
    pthread_mutex_lock(&m->mu);
    bool fatal = m->cfg.perspective == MOQ_PERSPECTIVE_CLIENT &&
                 (m->term_fatal || m->term_closed);
    bool closed =
        atomic_load_explicit(&m->stop_requested, memory_order_acquire) ||
        m->pump_exit || fatal;
    pthread_mutex_unlock(&m->mu);
    if (closed)
        return MOQ_ERR_CLOSED;
    /* wake every lane (per-lane targeting is moq_msquic_lane_wake) */
    for (uint32_t i = 0; i < m->lane_count; i++)
        (void)moq_msquic_lane_wake(&m->lanes[i]);
    return MOQ_OK;
}

/* The facade-level terminal for wait(): stop or any-lane pump-exit
 * always; a CLIENT additionally dies with its single connection (the
 * client terminal latch). A server is not "done" while the listener
 * serves. */
static bool mgd_terminal(moq_msquic_managed_t *m)
{
    if (atomic_load_explicit(&m->stop_requested, memory_order_acquire) ||
        m->pump_exit)
        return true;
    if (m->cfg.perspective != MOQ_PERSPECTIVE_CLIENT)
        return false;
    return m->term_fatal || m->term_closed;
}

moq_result_t moq_msquic_managed_wait(moq_msquic_managed_t *m,
                                     uint64_t timeout_us)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    /* blocking inside a managed callback would stall the very context
     * that produces the activity being waited for */
    if (mgd_tls_cb == m)
        return MOQ_ERR_WRONG_STATE;
    struct timespec abs;
    bool bounded = timeout_us != UINT64_MAX;

    if (bounded) {
        clock_gettime(CLOCK_REALTIME, &abs);
        abs.tv_sec += (time_t)(timeout_us / 1000000u);
        abs.tv_nsec += (long)(timeout_us % 1000000u) * 1000;
        if (abs.tv_nsec >= 1000000000L) {
            abs.tv_sec += 1;
            abs.tv_nsec -= 1000000000L;
        }
    }
    pthread_mutex_lock(&m->mu);
    for (;;) {
        if (m->activity_pending) {
            m->activity_pending = false;
            pthread_mutex_unlock(&m->mu);
            return MOQ_OK;
        }
        if (mgd_terminal(m)) {
            pthread_mutex_unlock(&m->mu);
            return MOQ_ERR_CLOSED;
        }
        if (bounded) {
            if (pthread_cond_timedwait(&m->cv, &m->mu, &abs) != 0) {
                bool activity = m->activity_pending;

                if (activity)
                    m->activity_pending = false;
                bool terminal = !activity && mgd_terminal(m);
                pthread_mutex_unlock(&m->mu);
                return activity ? MOQ_OK
                                : (terminal ? MOQ_ERR_CLOSED : MOQ_DONE);
            }
        } else {
            pthread_cond_wait(&m->cv, &m->mu);
        }
    }
}

int moq_msquic_managed_pending_sends(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    int v = 0;

    if (m == NULL)
        return 0;
    /* facade aggregate: sum the per-lane mirrors (refreshed by each
     * lane's pump under lane->mu, stored under m->mu) — the accessor
     * never locks a lane, so no cross-lane ordering hazard */
    pthread_mutex_lock(&m->mu);
    for (uint32_t i = 0; i < m->lane_count; i++)
        v += m->lanes[i].m_pending_sends;
    pthread_mutex_unlock(&m->mu);
    return v;
}

int moq_msquic_managed_pending_datagrams(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    int v = 0;

    if (m == NULL)
        return 0;
    pthread_mutex_lock(&m->mu);
    for (uint32_t i = 0; i < m->lane_count; i++)
        v += m->lanes[i].m_pending_dgrams;
    pthread_mutex_unlock(&m->mu);
    return v;
}

/* Terminal accessors — CLIENT convenience (the single connection's
 * latched terminal). On a SERVER they are not a per-connection
 * correctness API (a server polls SESSION_CLOSED in the lane pump). */
bool moq_msquic_managed_is_fatal(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    bool v;

    if (m == NULL)
        return false;
    pthread_mutex_lock(&m->mu);
    v = m->term_fatal;
    pthread_mutex_unlock(&m->mu);
    return v;
}

uint64_t moq_msquic_managed_fatal_code(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    uint64_t v;

    if (m == NULL)
        return 0;
    pthread_mutex_lock(&m->mu);
    v = m->term_fatal_code;
    pthread_mutex_unlock(&m->mu);
    return v;
}

bool moq_msquic_managed_is_closed(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    bool v;

    if (m == NULL)
        return false;
    pthread_mutex_lock(&m->mu);
    v = m->term_closed;
    pthread_mutex_unlock(&m->mu);
    return v;
}

uint64_t moq_msquic_managed_close_code(const moq_msquic_managed_t *cm)
{
    moq_msquic_managed_t *m = mgd_mut(cm);
    uint64_t v;

    if (m == NULL)
        return 0;
    pthread_mutex_lock(&m->mu);
    v = m->term_close_code;
    pthread_mutex_unlock(&m->mu);
    return v;
}

moq_version_t moq_msquic_managed_negotiated_version(
    const moq_msquic_managed_t *m)
{
    /* Exact endpoints fix m->version at create(); multi-version servers keep
     * it at 0 because version is per accepted connection. Immutable after
     * create, so no lock is needed. */
    return m != NULL ? m->version : (moq_version_t)0;
}

moq_version_t moq_msquic_managed_conn_negotiated_version(
    const moq_msquic_managed_conn_t *conn)
{
    if (conn == NULL || !mgd_in_lane_pump(conn->lane))
        return (moq_version_t)0;
    return conn->negotiated_version;
}
