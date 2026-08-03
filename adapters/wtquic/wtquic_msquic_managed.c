/*
 * moq_wtquic_msquic_managed — managed MoQ-over-wtquic (MsQuic) facade.
 *
 * Implemented here: the config/ownership/lifecycle skeleton (sized-config
 * parsing, owned copies of every borrowed input, create() with clean rollback
 * from any partial state, the teardown COORDINATOR whose invocation of
 * on_stopped is its LAST facade access); and the CLIENT transport: env open
 * with the bridged allocator, dial via wtq_msquic_client_connect, WT-Protocol
 * negotiation, and lazy MoQ-session + attach-adapter creation at the negotiated
 * version; the SERVER listener/admission/lane-placement/quiesce path; the
 * per-lane pump window that exposes each connection's terminal events inside the
 * exclusive on_lane_pump callback; live reap — a fully terminal, quiesced,
 * pump-observed server child is torn down and its slot published for reuse; and
 * the per-lane DOORBELL: one coalescing worker thread per lane, armed by
 * lane_wake/managed_wake, the transport-activity nudge from the adapter hook, AND
 * MoQ-session deadlines (the worker arms a timed wait on the lane's next session
 * deadline; the adapter service pass ticks it), so lanes pump concurrently; each
 * pump latches wait()-visible activity and fires on_activity; and the per-lane
 * wake/pump STATISTICS (moq_wtquic_msquic_lane_get_stats), all maintained under
 * the leaf bell lock. This completes the managed-server facade.
 */
#include <moq/wtquic_msquic_managed.h>

#include "../wtquic_adapter_internal.h"   /* private DSO SPI: event-progress */

#include <wtquic/wtquic_msquic.h>

#include "../common/moq_alpn.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Offered-subprotocol list cap (a malformed/overlong offer is rejected, not
 * silently truncated) — the public limit, so the two can't drift. Lane fan-out
 * is NOT capped here: the caller's requested topology is honored, bounded only
 * by checked allocation. */
#define MM_MAX_WT_PROTOCOLS MOQ_WTQUIC_MSQUIC_MANAGED_MAX_WT_PROTOCOLS

/* count * elem with overflow detection. */
static bool mm_arr_bytes(size_t count, size_t elem, size_t *out)
{
    if (count != 0 && elem > SIZE_MAX / count)
        return false;
    *out = count * elem;
    return true;
}

/*
 * Frozen v0 config prefix — the exact layout MOQ_WTQUIC_MSQUIC_MANAGED_CFG_V0_SIZE
 * pins. The floor is derived from the CURRENT struct, so a field inserted or
 * reordered before on_activity_ctx would silently move the floor and reject old
 * binaries. These static asserts catch any such drift: every v0 field keeps its
 * frozen offset, the floor equals the mirror's size, and the optional tail
 * begins at or after the mirror.
 */
typedef struct {
    uint32_t struct_size;
    const moq_alloc_t *alloc;
    moq_perspective_t perspective;
    const char *host;
    uint16_t port;
    const char *cert_path;
    const char *key_path;
    bool insecure_skip_verify;
    uint32_t idle_timeout_ms;
    const char *wt_path;
    const char *const *wt_protocols;
    size_t wt_protocol_count;
    moq_wtquic_msquic_lane_pump_fn on_lane_pump;
    void *on_lane_pump_user;
    moq_wtquic_msquic_activity_fn on_activity;
    void *on_activity_ctx;
} mm_cfg_v0_t;

#define MM_FROZEN_OFF(f)                                                     \
    _Static_assert(offsetof(moq_wtquic_msquic_managed_cfg_t, f) ==           \
                       offsetof(mm_cfg_v0_t, f),                             \
                   "v0 config field " #f " moved from its frozen offset")
MM_FROZEN_OFF(struct_size);
MM_FROZEN_OFF(alloc);
MM_FROZEN_OFF(perspective);
MM_FROZEN_OFF(host);
MM_FROZEN_OFF(port);
MM_FROZEN_OFF(cert_path);
MM_FROZEN_OFF(key_path);
MM_FROZEN_OFF(insecure_skip_verify);
MM_FROZEN_OFF(idle_timeout_ms);
MM_FROZEN_OFF(wt_path);
MM_FROZEN_OFF(wt_protocols);
MM_FROZEN_OFF(wt_protocol_count);
MM_FROZEN_OFF(on_lane_pump);
MM_FROZEN_OFF(on_lane_pump_user);
MM_FROZEN_OFF(on_activity);
MM_FROZEN_OFF(on_activity_ctx);
#undef MM_FROZEN_OFF
_Static_assert(MOQ_WTQUIC_MSQUIC_MANAGED_CFG_V0_SIZE == sizeof(mm_cfg_v0_t),
               "v0 floor must equal the frozen v0 layout size");
_Static_assert(offsetof(moq_wtquic_msquic_managed_cfg_t, on_stopped) >=
                   sizeof(mm_cfg_v0_t),
               "the optional tail must begin at/after the frozen v0 prefix");

/* The libmoq WebTransport-profile enum maps 1:1 onto wtquic's wire-profile enum:
 * the forwarding paths assign the value verbatim (conn/lcfg.webtransport_profile
 * = m->webtransport_profile), so the two enumerations MUST share values. Pin it
 * so a divergence upstream is a compile error, not a silent wrong dialect. */
_Static_assert((int)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT ==
                   (int)WTQ_WEBTRANSPORT_PROFILE_H3_CURRENT,
               "profile CURRENT must map to wtquic H3_CURRENT");
_Static_assert((int)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT ==
                   (int)WTQ_WEBTRANSPORT_PROFILE_H3_DRAFT_13_14_COMPAT,
               "profile D13_14_COMPAT must map to wtquic H3_DRAFT_13_14_COMPAT");

/*
 * Frozen v0 lane-stats layout — the exact layout MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE
 * pins. Like the config floor, the public V0_SIZE is derived from the CURRENT
 * struct, so a counter inserted or reordered before flush_bytes would silently
 * move the floor and break the sized-copy contract for old callers. These static
 * asserts catch any such drift: every v0 counter keeps its frozen offset and the
 * floor equals the mirror's size. A new counter is appended AFTER flush_bytes and
 * does NOT extend this mirror (that is what keeps the v0 floor frozen).
 */
typedef struct {
    uint32_t struct_size;
    uint64_t wakes_same_lane;
    uint64_t wakes_cross_lane;
    uint64_t wakes_external;
    uint64_t wakes_coalesced;
    uint64_t pump_sweeps;
    uint64_t deadline_sweeps;
    uint64_t idle_cap_wakes;
    uint64_t wake_to_pump_max_us;
    uint64_t wake_to_pump_total_us;
    uint64_t wake_to_pump_samples;
    uint64_t service_passes;
    uint64_t flush_sends;
    uint64_t flush_bytes;
} mm_lane_stats_v0_t;

#define MM_STATS_FROZEN_OFF(f)                                               \
    _Static_assert(offsetof(moq_wtquic_msquic_lane_stats_t, f) ==            \
                       offsetof(mm_lane_stats_v0_t, f),                      \
                   "v0 lane-stats field " #f " moved from its frozen offset")
MM_STATS_FROZEN_OFF(struct_size);
MM_STATS_FROZEN_OFF(wakes_same_lane);
MM_STATS_FROZEN_OFF(wakes_cross_lane);
MM_STATS_FROZEN_OFF(wakes_external);
MM_STATS_FROZEN_OFF(wakes_coalesced);
MM_STATS_FROZEN_OFF(pump_sweeps);
MM_STATS_FROZEN_OFF(deadline_sweeps);
MM_STATS_FROZEN_OFF(idle_cap_wakes);
MM_STATS_FROZEN_OFF(wake_to_pump_max_us);
MM_STATS_FROZEN_OFF(wake_to_pump_total_us);
MM_STATS_FROZEN_OFF(wake_to_pump_samples);
MM_STATS_FROZEN_OFF(service_passes);
MM_STATS_FROZEN_OFF(flush_sends);
MM_STATS_FROZEN_OFF(flush_bytes);
#undef MM_STATS_FROZEN_OFF
_Static_assert(MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE == sizeof(mm_lane_stats_v0_t),
               "v0 lane-stats floor must equal the frozen v0 layout size");

/*
 * Private pthread failure injection (test builds only, never installed, not
 * exported from the shared library). Set the 1-based ordinal of the pthread
 * primitive within create() — mutex/cond init OR pthread_create (lane bells,
 * doorbell workers, the coordinator) — that should fail; 0 disables it.
 */
#if defined(MOQ_WTQ_MM_TESTING)
static _Atomic int g_mm_test_pthread_fail;
void moq_wtquic_msquic_managed_test_fail_pthread(int nth)
{
    atomic_store(&g_mm_test_pthread_fail, nth);
}
static bool mm_test_pthread_should_fail(void)
{
    int n = atomic_load(&g_mm_test_pthread_fail);
    if (n <= 0)
        return false;
    atomic_store(&g_mm_test_pthread_fail, n - 1);
    return n == 1;
}
#else
static bool mm_test_pthread_should_fail(void)
{
    return false;
}
#endif

static int mm_mutex_init(pthread_mutex_t *mu)
{
    if (mm_test_pthread_should_fail())
        return -1;
    return pthread_mutex_init(mu, NULL);
}
static int mm_cond_init(pthread_cond_t *cv)
{
    if (mm_test_pthread_should_fail())
        return -1;
    return pthread_cond_init(cv, NULL);
}
static int mm_thread_create(pthread_t *th, void *(*fn)(void *), void *arg)
{
    if (mm_test_pthread_should_fail())
        return -1;
    return pthread_create(th, NULL, fn, arg);
}

/*
 * Connect/release seam (test builds only, never installed/exported). White-box
 * tests install overrides for wtq_msquic_client_connect / wtq_session_release so
 * the guard-held publication path can be exercised deterministically without a
 * real transport. When a connect override is present mm_client_start calls it,
 * and mm_reap_client uses the paired release override for whatever handle it
 * returned.
 */
#if defined(MOQ_WTQ_MM_TESTING)
typedef wtq_result_t (*mm_test_connect_fn)(wtq_msquic_env_t *,
                                           const wtq_msquic_client_cfg_t *,
                                           wtq_session_t **);
static mm_test_connect_fn g_mm_test_connect;
static void (*g_mm_test_release)(wtq_session_t *);
void moq_wtquic_msquic_managed_test_set_transport(
    mm_test_connect_fn connect, void (*release)(wtq_session_t *))
{
    g_mm_test_connect = connect;
    g_mm_test_release = release;
}

/* When set, mm_server_start opens the env and allocates the child pool but skips
 * the real listener, so server admission (accept_prepare/abandon) can be driven
 * white-box without a certificate or the network. */
static bool g_mm_test_no_listener;
void moq_wtquic_msquic_managed_test_no_listener(bool on)
{
    g_mm_test_no_listener = on;
}

/* The webtransport_profile last forwarded onto a server listener cfg, captured
 * in mm_server_start so a test can prove the SERVER path forwards it (mirrors
 * the client-side observation via cfg->connect->webtransport_profile). Sentinel
 * 0xFFFFFFFF until a server start runs. */
static uint32_t g_mm_test_last_listener_profile = 0xFFFFFFFFu;
uint32_t moq_wtquic_msquic_managed_test_last_listener_profile(void)
{
    return g_mm_test_last_listener_profile;
}

/* When set, the pump's adapter service passes call this per connection instead
 * of moq_wtquic_conn_service (phase 0 = pre-callback, 1 = post-callback), so a
 * test can prove the due path services BEFORE on_lane_pump without a real
 * adapter. */
static void (*g_mm_test_service)(void *conn, int phase);
void moq_wtquic_msquic_managed_test_set_service_op(
    void (*op)(void *conn, int phase))
{
    g_mm_test_service = op;
}

/* When set, the pump's deadline recompute calls this per connection instead of
 * moq_session_next_deadline_us, so a test drives the deadline path (arming,
 * recompute, rearm) deterministically without a real session. */
static uint64_t (*g_mm_test_next_deadline)(void *conn);
void moq_wtquic_msquic_managed_test_set_next_deadline_op(
    uint64_t (*op)(void *conn))
{
    g_mm_test_next_deadline = op;
}

/* When set, the live-reap loop calls this right after publishing a slot free
 * (in_use = false), before advancing to the captured next — so a test can
 * reassign the just-freed slot mid-traversal and prove it is never re-read. */
static void (*g_mm_test_after_reap)(void *conn);
void moq_wtquic_msquic_managed_test_set_after_reap(void (*op)(void *conn))
{
    g_mm_test_after_reap = op;
}

/* When set, the deferred-close execution in mm_pump_lane calls this instead of
 * wtq_session_close, so a test can prove the connection's transport close ran
 * exactly once, with the right code, outside the pump window. */
static void (*g_mm_test_close)(wtq_session_t *, uint32_t);
void moq_wtquic_msquic_managed_test_set_close_op(
    void (*op)(wtq_session_t *, uint32_t))
{
    g_mm_test_close = op;
}

/* One-shot hook fired by a doorbell worker UNDER bell_mu, at the idle
 * check-to-wait boundary (after finding no work, immediately before
 * pthread_cond_wait). A test uses it to prove an arm landing exactly there is
 * not lost. Atomic exchange so the worker's read+clear never races the setter. */
static void (*_Atomic g_mm_test_prewait)(void);
void moq_wtquic_msquic_managed_test_set_prewait_hook(void (*hook)(void))
{
    atomic_store(&g_mm_test_prewait, hook);
}

/* Fire the transport-activity hook for a connection, exactly as a MsQuic worker
 * would after servicing a transport event (mm_in_service is false on the test
 * thread, so this arms the connection's lane). */
static void mm_hook(moq_wtquic_conn_t *conn, void *user);
void moq_wtquic_msquic_managed_test_fire_hook(void *conn)
{
    mm_hook(NULL, conn);
}
#endif

/* --- internal structures -------------------------------------------------- */

struct moq_wtquic_msquic_managed_lane {
    moq_wtquic_msquic_managed_t *m;         /* owning facade (stable) */
    uint32_t index;
    pthread_mutex_t mu;                      /* the lane's guard domain */
    moq_wtquic_msquic_lane_stats_t stats;    /* monotonic; zero until a pump runs */

    /* Per-lane doorbell: an independently armed, coalescing worker thread. The
     * bell is a LEAF lock (never held across the pump or any other lock, so no
     * cross-lane deadlock): a wake bumps `pending` and signals; the worker sets
     * `served = pending` BEFORE pumping, so a wake arriving during a pump leaves
     * pending > served and the worker rearms without waiting (no lost wake), and
     * a burst between pumps collapses into one (`served = pending` consumes it). */
    pthread_t worker;
    bool worker_started;
    bool bell_inited;
    pthread_mutex_t bell_mu;
    pthread_cond_t bell_cv;
    uint64_t pending;                        /* wake generation (armed) */
    uint64_t served;                         /* generation last consumed */
    bool bell_stop;                          /* worker exit latch */
    bool activated;                          /* worker may pump only once create()
                                              * has fully succeeded; arms queued
                                              * before this are RETAINED, not run */
    uint64_t deadline_us;                    /* next MoQ-session deadline this lane
                                              * must tick at (MONOTONIC absolute;
                                              * UINT64_MAX = none). Worker-confined:
                                              * set by the pump, read at the wait. */
    uint64_t wake_since_us;                  /* time the first arm of the current
                                              * unconsumed batch was posted, for the
                                              * wake->pump latency stat (bell_mu). */
};

struct moq_wtquic_msquic_managed_conn {
    moq_wtquic_msquic_managed_t *m;          /* owning facade */
    moq_wtquic_msquic_managed_lane_t *lane;
    bool in_use;                              /* server: slot reserved at accept */
    wtq_session_t *ws;                        /* the wtquic session */
    moq_session_t *session;                   /* created lazily at establish */
    moq_wtquic_conn_t *adapter;               /* attach adapter over `session` */
    moq_version_t version;                    /* 0 until negotiated */
    bool owns_ws_ref;                         /* release once at reap */
    bool quiesce_requested;                   /* teardown asked this child to go
                                               * away; set + read under the lane
                                               * guard so a late on_established
                                               * converges instead of going live */
    bool logical_terminal;                    /* on_closed/refused/failed seen */
    bool transport_quiesced;                  /* on_transport_quiesced seen */
#if defined(MOQ_WTQ_MM_TESTING)
    bool test_terminal_observed;              /* injected observation for a
                                               * synthetic child with no session */
#endif
    bool session_backed;                      /* a MoQ session was published for
                                               * this child. Durable: it stays
                                               * true after reap nulls `session`,
                                               * so the reap gate and the
                                               * accounting cannot disagree about
                                               * which contract applies. */
#if defined(MOQ_WTQ_MM_TESTING)
    bool terminal_hold_counted;               /* THIS child published a hold on
                                               * the facade's terminal_held_count
                                               * and has not withdrawn it. Written
                                               * under the lane guard while m->mu
                                               * is held, so a child only ever
                                               * adds or removes its OWN hold. */
#endif
    bool app_terminal_acked;                  /* the app asserted, from this
                                               * conn's owning lane callback, that
                                               * its terminal processing is done,
                                               * it no longer needs the child kept
                                               * iterable, and it will not use the
                                               * borrowed handles afterwards.
                                               * Monotonic; libmoq records that the
                                               * assertion was made, it cannot
                                               * verify it. */
    bool close_pending;                       /* conn_close latched a close in the
                                               * pump; executed after the callback */
    uint32_t close_pending_code;              /* the app close code to apply */
    void *user;                               /* app slot */

    /* Per-pump snapshot linkage. The pump captures this lane's live connections
     * into a chain (snap_next) at entry so iteration is stable even if an accept
     * adds one mid-sweep; in_window gates the pump-confined accessors in O(1). A connection belongs
     * to exactly one lane, so its own lane's (serialized) worker is the only
     * writer — this holds with lanes pumping concurrently, WITHOUT any per-lane
     * arrays (total O(max_connections), not O(lane_count * max_connections)). */
    struct moq_wtquic_msquic_managed_conn *snap_next;
    bool in_window;
    uint64_t pump_pre_token;                  /* event-progress token snapshot
                                                 taken before on_lane_pump, for
                                                 the post-service re-drive check */
};

struct moq_wtquic_msquic_managed {
    moq_alloc_t alloc;                       /* thread-safe; owned by value */
    moq_perspective_t perspective;

    /* owned copies of borrowed config (freed at destroy) */
    char *host;
    char *cert_path;
    char *key_path;
    char *wt_path;
    char **protos;                           /* deep-copied offer list */
    size_t proto_count;

    uint16_t port;
    bool insecure_skip_verify;
    uint32_t idle_timeout_ms;

    /* session tuning */
    bool send_request_capacity;
    uint32_t initial_request_capacity;
    uint32_t max_events;
    uint32_t max_actions;
    uint32_t max_connections;
    bool streaming_objects;
    uint64_t session_idle_timeout_us;
    uint32_t webtransport_profile;  /* wtq_webtransport_profile_t; 0 = current */

    /* callbacks */
    moq_wtquic_msquic_lane_pump_fn on_lane_pump;
    void *on_lane_pump_user;
    moq_wtquic_msquic_activity_fn on_activity;
    void *on_activity_ctx;
    moq_wtquic_msquic_choose_lane_fn choose_lane;
    void *choose_lane_user;
    /* app service-deadline query (ONE ABI block; ctx = the service endpoint).
     * Folded facade-wide into each lane's recomputed next deadline. */
    uint64_t (*app_deadline_us)(void *ctx);
    void *app_deadline_ctx;
    void (*on_stopped)(void *ctx);
    void *on_stopped_ctx;

    /* transport. The env owns the MsQuic worker threads. wt_alloc bridges the
     * facade's owned allocator to the wtquic env so transport/session
     * allocations honor it. */
    wtq_msquic_env_t *env;
    wtq_alloc_t wt_alloc;                     /* cfg.alloc bridged for wtquic */
    moq_wtquic_msquic_managed_conn_t *client; /* client: the one connection */
    const wtq_session_events_t *fwd;          /* attach-adapter event table */

    /* server admission (all NULL/0 for a client). The children pool is
     * max_connections fixed slots reserved at accept_prepare and freed at
     * abandon/reap; conn_count is the live admitted count; refuse_admissions is
     * latched at drain/stop_begin so no child is admitted past the barrier. */
    wtq_msquic_listener_t *listener;
    moq_wtquic_msquic_managed_conn_t *children; /* max_connections slots */
    uint32_t conn_count;                      /* live admitted (guarded by mu) */
    uint32_t rr_lane;                         /* round-robin cursor */
    bool refuse_admissions;                   /* drain/stop latched */

    /* Terminal accounting. ALL of these are owned by `mu` and are only ever
     * read or written with it held. The per-child facts they summarise live on
     * the child and are owned by its lane guard, so the facade must never scan
     * them: each lane transition publishes its effect here instead, under mu,
     * in the established lane -> facade order.
     *
     * Each name states exactly what it counts -- in particular
     * `transport_terminal_session_backed` counts a TRANSPORT terminal on a
     * child that had a session, which is neither "the session terminal was
     * enqueued" nor "it was observed". */
#if defined(MOQ_WTQ_MM_TESTING)
    /* Conservation evidence only. None of these has a production reader: the
     * shipped facade's capacity behaviour rests on in_use / conn_count and the
     * reap predicate, and there is no getter in the installed API. */
    uint32_t terminal_held_count;             /* admitted children that are
                                               * transport-terminal and not yet
                                               * acknowledged: they hold their
                                               * capacity slot by contract */
    uint64_t st_transport_terminal_session_backed;
    uint64_t st_ack_accepted;
    uint64_t st_children_reaped;
    uint64_t st_accepts_refused_terminal_held;
#endif
    uint16_t bound_port;                      /* listener's bound port */


    /* lanes */
    uint32_t lane_count;
    uint32_t lanes_inited;                    /* mutexes successfully created */
    moq_wtquic_msquic_managed_lane_t *lanes;

    /* lifecycle / coordinator */
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool mu_inited;
    bool cv_inited;
    pthread_t coord;
    bool coord_started;
    bool on_stopped_owns;                     /* on_stopped present -> join refused */
    bool drain_requested;
    bool stop_requested;                      /* latched by stop_begin */
    bool stopped;                             /* coordinator finished teardown */

    /* terminal / negotiated (negotiated_version set at establishment; the
     * terminal flags latched by the forwarding callbacks) */
    moq_version_t negotiated_version;
    bool activity_pending;                    /* level-retained wake for wait();
                                                 set at establishment, consumed
                                                 by wait() as MOQ_OK */
    bool fatal;
    bool closed;
    uint64_t fatal_code;
    uint64_t close_code;
};

/* --- small helpers -------------------------------------------------------- */

static char *mm_strdup(moq_wtquic_msquic_managed_t *m, const char *s)
{
    if (s == NULL)
        return NULL;
    size_t n = strlen(s) + 1;
    char *d = m->alloc.alloc(n, m->alloc.ctx);
    if (d != NULL)
        memcpy(d, s, n);
    return d;
}

static void mm_free_str(moq_wtquic_msquic_managed_t *m, char *s)
{
    if (s != NULL)
        m->alloc.free(s, strlen(s) + 1, m->alloc.ctx);
}

static void mm_free_protos(moq_wtquic_msquic_managed_t *m)
{
    if (m->protos == NULL)
        return;
    for (size_t i = 0; i < m->proto_count; i++)
        mm_free_str(m, m->protos[i]);
    m->alloc.free(m->protos, m->proto_count * sizeof(m->protos[0]), m->alloc.ctx);
    m->protos = NULL;
    m->proto_count = 0;
}

/* Deep-copy the offered subprotocol list so the caller's array/strings need
 * not outlive create(). proto_count/protos are published as they grow, so a
 * mid-loop OOM is cleaned by destroy(). A NULL entry is a malformed offer. */
static moq_result_t mm_dup_protos(moq_wtquic_msquic_managed_t *m,
                                  const char *const *src, size_t n)
{
    if (n == 0)
        return MOQ_OK; /* caller supplied none; the client default is
                        * materialized separately in create() */
    /* pointer/count consistency + a hard cap that also bounds the allocation
     * (n <= MM_MAX_WT_PROTOCOLS makes n*sizeof(char*) unable to overflow; the
     * checked multiply below is belt-and-suspenders). */
    if (src == NULL || n > MM_MAX_WT_PROTOCOLS)
        return MOQ_ERR_INVAL;
    size_t bytes;
    if (!mm_arr_bytes(n, sizeof(*src), &bytes))
        return MOQ_ERR_INVAL;
    char **arr = m->alloc.alloc(bytes, m->alloc.ctx);
    if (arr == NULL)
        return MOQ_ERR_NOMEM;
    memset(arr, 0, bytes);
    m->protos = arr;
    m->proto_count = n;
    for (size_t i = 0; i < n; i++) {
        if (src[i] == NULL)
            return MOQ_ERR_INVAL;
        arr[i] = mm_strdup(m, src[i]);
        if (arr[i] == NULL)
            return MOQ_ERR_NOMEM;
    }
    return MOQ_OK;
}

/* The lane whose on_lane_pump is running ON THIS THREAD, plus the head of the
 * connection snapshot chain (linked through conn->snap_next). Published only
 * across a pump cycle and thread-local, so lanes pumping concurrently each have
 * their own window — this is the multi-lane analogue of the network facade's
 * single in_lane_pump flag. Membership is O(1) via conn->in_window, so no array
 * is needed and storage stays O(max_connections). */
typedef struct mm_pump_window {
    moq_wtquic_msquic_managed_lane_t *lane;
    moq_wtquic_msquic_managed_conn_t *head;
} mm_pump_window_t;

static _Thread_local const mm_pump_window_t *mm_pump_win;

/* Arm a lane's doorbell (defined with the worker below; forward-declared here
 * for the transport-activity hook). */
static void mm_lane_arm(moq_wtquic_msquic_managed_lane_t *lane);

/* True while THIS thread is inside the pump's own adapter service pass. The
 * adapter's post-service hook (mm_hook) fires there too, so this flag lets it
 * distinguish "the pump servicing" (do NOT re-arm — that would self-perpetuate
 * pumps) from a real transport event (arm the lane). */
static _Thread_local bool mm_in_service;

/* True while THIS thread is inside an on_activity callback, where — like a pump
 * callback — the BLOCKING teardown calls must be refused (see mm_in_callback). */
static _Thread_local bool mm_in_activity;

/* True when this thread is inside THIS lane's pump window. */
static bool mm_in_lane_pump(const moq_wtquic_msquic_managed_lane_t *lane)
{
    return mm_pump_win != NULL && mm_pump_win->lane == lane;
}

/* True when conn is in the snapshot of the window open on THIS thread. in_window
 * is set only by the conn's own (serialized) lane worker, and the lane match
 * confirms this thread is that worker mid-pump. */
static bool mm_conn_in_window(const moq_wtquic_msquic_managed_conn_t *conn)
{
    return mm_pump_win != NULL && conn != NULL &&
           mm_pump_win->lane == conn->lane && conn->in_window;
}

/* Latch a child's transport-terminal fact exactly once.
 *
 * Counted ONLY for a session-backed SERVER child. A pre-session failure has no
 * session terminal to enqueue, observe or acknowledge, and the single client is
 * excluded from live reap, so neither belongs in the admission-capacity
 * relation.
 *
 * The facts are unordered: an application that closes the session locally can
 * reach acknowledgment BEFORE the transport terminal arrives. So the hold is
 * published only if this child has not already been acknowledged, and it is
 * recorded per child -- a child never adds or removes a hold belonging to
 * another. That keeps
 *     terminal_held_count == #{ server children with terminal && !acknowledged }
 * true at every observable point, in either order. */
static void mm_mark_logical_terminal(moq_wtquic_msquic_managed_conn_t *c)
{
    if (c->logical_terminal)
        return;                        /* monotonic: latch once */
    if (!c->session_backed || c == c->m->client) {
        c->logical_terminal = true;    /* outside the capacity accounting */
        return;
    }
    /* The fact and its aggregate move together, in ONE m->mu section taken
     * while the lane guard is already held (lane -> facade order). Publishing
     * the flag first would let an admission or a snapshot that takes m->mu see
     * a child that is terminal while terminal_held_count still says otherwise.
     * Nothing may observe the child fact and the aggregate disagreeing. */
    pthread_mutex_lock(&c->m->mu);
    c->logical_terminal = true;
#if defined(MOQ_WTQ_MM_TESTING)
    c->m->st_transport_terminal_session_backed++;
    if (!c->app_terminal_acked) {
        c->m->terminal_held_count++;
        c->terminal_hold_counted = true;
    }
#endif
    pthread_mutex_unlock(&c->m->mu);
}

/* The session-terminal OBSERVED fact for a child, read through the private
 * adapter SPI so this facade never touches the bridge or the public session API.
 * A child with no adapter yet (pre-session failure) has no session fact to
 * observe and reads false — that path reclaims through its own route. */
static bool mm_conn_terminal_observed(const moq_wtquic_msquic_managed_conn_t *c)
{
    bool observed = false;
    if (c == NULL)
        return false;
#if defined(MOQ_WTQ_MM_TESTING)
    /* Synthetic children in the white-box tests have no adapter/session to poll;
     * the injected fact stands in for a real observation. */
    if (c->test_terminal_observed)
        return true;
#endif
    if (c->adapter == NULL)
        return false;
    (void)moq_wtquic_conn_terminal_facts(c->adapter, &observed);
    return observed;
}

/* True when this thread is inside a lane pump window OR an on_activity callback,
 * where the BLOCKING teardown calls (wait/stop/join) must be refused — running
 * one on the doorbell worker would deadlock the coordinator. */
static bool mm_in_callback(const moq_wtquic_msquic_managed_t *m)
{
    (void)m;
    return mm_pump_win != NULL || mm_in_activity;
}

static void mm_abstime(struct timespec *ts, uint64_t timeout_us)
{
    clock_gettime(CLOCK_REALTIME, ts);
    uint64_t ns = (uint64_t)ts->tv_nsec + (timeout_us % 1000000u) * 1000u;
    ts->tv_sec += (time_t)(timeout_us / 1000000u) + (time_t)(ns / 1000000000u);
    ts->tv_nsec = (long)(ns % 1000000000u);
}

static uint64_t mm_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* --- client transport + WT-Protocol negotiation -------------------------- */

/* The lane guard is the lane mutex: entering blocks any callback racing on the
 * same lane, so a client can publish its session handle before its callbacks
 * run (held across wtq_msquic_client_connect through publication). */
static void mm_guard_enter(void *ctx)
{
    moq_wtquic_msquic_managed_lane_t *lane = ctx;
    pthread_mutex_lock(&lane->mu);
}

static void mm_guard_leave(void *ctx)
{
    moq_wtquic_msquic_managed_lane_t *lane = ctx;
    pthread_mutex_unlock(&lane->mu);
}

static wtq_guard_t mm_lane_guard(moq_wtquic_msquic_managed_lane_t *lane)
{
    wtq_guard_t g = { mm_guard_enter, mm_guard_leave, lane };
    return g;
}

/* Terminal latches (facade-wide, first-cause wins). A fatal cause is never
 * overwritten and outranks a clean close; a clean close is recorded only when
 * nothing terminal was latched first. Both wake the coordinator/waiters. */
static void mm_latch_fatal(moq_wtquic_msquic_managed_t *m, uint64_t code)
{
    pthread_mutex_lock(&m->mu);
    if (!m->fatal) {
        m->fatal = true;
        m->fatal_code = code;
    }
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
}

static void mm_latch_closed(moq_wtquic_msquic_managed_t *m, uint64_t code)
{
    pthread_mutex_lock(&m->mu);
    if (!m->fatal && !m->closed) {
        m->closed = true;
        m->close_code = code;
    }
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
}

/* Byte-exact membership test against the offered WT-Protocol tokens. */
static bool mm_offered(const moq_wtquic_msquic_managed_t *m, const char *tok,
                       size_t len)
{
    for (size_t i = 0; i < m->proto_count; i++)
        if (strlen(m->protos[i]) == len &&
            memcmp(m->protos[i], tok, len) == 0)
            return true;
    return false;
}

/*
 * Validate the negotiated WT-Protocol token and map it to a MoQ version.
 * Returns true and sets *out ONLY for a recognized, offered selection (or the
 * draft-16 legacy fallback when draft-16 was on the table). No token is invented
 * or silently downgraded: an unrecognized token, an un-offered token, or a
 * missing token when draft-16 was not offered all return false — the caller
 * builds no MoQ session in that case.
 */
static bool mm_negotiate(const moq_wtquic_msquic_managed_t *m, wtq_str_t sub,
                         moq_version_t *out)
{
    if (sub.len == 0) {
        /* Legacy fallback to draft-16 is legitimate only when draft-16 was
         * actually offered (or no explicit offer was configured). An exact
         * draft-18 offer is never silently downgraded. */
        if (m->proto_count == 0 || mm_offered(m, "moqt-16", 7)) {
            *out = MOQ_VERSION_DRAFT_16;
            return true;
        }
        return false;
    }
    moq_version_t v;
    if (!moq_alpn_to_version(sub.data, sub.len, &v))
        return false; /* unrecognized token */
    if (!mm_offered(m, sub.data, sub.len))
        return false; /* server invented a token we never offered */
    *out = v;
    return true;
}

/* The adapter's post-service hook: the transport-activity nudge. It fires from
 * two contexts:
 *   - a real transport event (on a MsQuic worker) that fed+serviced the
 *     connection — schedule a pump so the app can consume the new work;
 *   - the pump's OWN service pass (mm_in_service set on the doorbell worker) —
 *     do NOT re-arm, or every pump would schedule another and spin.
 * Arming only touches the lane's leaf bell, which is initialized BEFORE the
 * transport is brought up, so a hook firing during create() safely queues a
 * pump; the worker (started after create finishes bring-up) runs it later — no
 * application pump can run before the facade and workers are fully initialized. */
static void mm_hook(moq_wtquic_conn_t *conn, void *user)
{
    (void)conn;
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (!mm_in_service && c != NULL && c->lane != NULL)
        mm_lane_arm(c->lane);
}

/*
 * Establishment: validate the negotiated token, and only on success create the
 * MoQ session at the negotiated version plus the attach adapter, then forward
 * establishment into the adapter (it drives the bridge SETUP). Any failure
 * before the adapter exists latches the first fatal cause and closes the
 * transport WITHOUT building a session.
 */
/* An establishment failure before the adapter exists: mark the child terminal
 * and close the transport. Facade-fatal for a CLIENT; child-local for a server
 * (the server outlives one bad child). */
static void mm_establish_fail(moq_wtquic_msquic_managed_t *m,
                              moq_wtquic_msquic_managed_conn_t *c,
                              wtq_session_t *s)
{
    mm_mark_logical_terminal(c);
    if (m->perspective == MOQ_PERSPECTIVE_CLIENT)
        mm_latch_fatal(m, 0);
#if defined(MOQ_WTQ_MM_TESTING)
    /* a fake-transport session (driven through the establishment seam) is not a
     * real handle; the installed release seam marks that case */
    if (g_mm_test_release == NULL)
#endif
        (void)wtq_session_close(s, 0, NULL, 0);
}

static void mm_on_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    moq_wtquic_msquic_managed_t *m = c->m;

    /* Teardown may have requested this child's quiescence after it was admitted
     * but before it established. This callback and the quiesce scan both run
     * under the lane guard, so the flag is stable here: if it is set, converge
     * immediately (mark terminal, close the transport) rather than building a
     * live session the barrier would then have to wait on. The scan closes an
     * already-established child's ws; this closes a late-establishing one. */
    if (c->quiesce_requested) {
        mm_mark_logical_terminal(c);
#if defined(MOQ_WTQ_MM_TESTING)
        if (g_mm_test_release == NULL)
#endif
            (void)wtq_session_close(s, 0, NULL, 0);
        return;
    }

    moq_version_t version = 0;
    if (!mm_negotiate(m, sub, &version)) {
        mm_establish_fail(m, c, s);
        return;
    }

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &m->alloc,
                               m->perspective);
    scfg.version = version;
    scfg.send_request_capacity = m->send_request_capacity;
    if (m->initial_request_capacity != 0)
        scfg.initial_request_capacity = m->initial_request_capacity;
    if (m->max_events != 0)
        scfg.max_events = m->max_events;
    if (m->max_actions != 0)
        scfg.max_actions = m->max_actions;
    scfg.streaming_objects = m->streaming_objects;
    scfg.idle_timeout_us = m->session_idle_timeout_us;

    moq_session_t *ms = NULL;
    if (moq_session_create(&scfg, mm_now_us(), &ms) < 0) {
        mm_establish_fail(m, c, s);
        return;
    }

    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &m->alloc;
    ccfg.session = ms;
    ccfg.hook = mm_hook;
    ccfg.hook_user = c;
    moq_wtquic_conn_t *mc = NULL;
    if (moq_wtquic_conn_create(&ccfg, &mc) < 0) {
        moq_session_destroy(ms);
        mm_establish_fail(m, c, s);
        return;
    }

    c->session = ms;
    c->session_backed = true;
    c->adapter = mc;
    c->version = version;
    /* Server: retain + store the backend-owned session so teardown can close it
     * (the pre-barrier quiesce) and release it at reap. A client already owns
     * its ref from connect (stored in mm_client_start). */
    if (m->perspective == MOQ_PERSPECTIVE_SERVER) {
        wtq_session_add_ref(s);
        c->ws = s;
        c->owns_ws_ref = true;
    }

    /* drive the attach adapter's bridge setup BEFORE publishing establishment,
     * so an observer woken by the latch below sees a fully-established
     * connection (not one mid-setup on this worker) */
    m->fwd->on_established(s, sub, mc);

    /* Negotiation is per-connection: c->version above (read via
     * conn_negotiated_version) holds for both perspectives. Only a CLIENT
     * mirrors it facade-wide and latches the wait() activity — a server has
     * many children and outlives each, so its facade accessor stays 0. */
    if (m->perspective == MOQ_PERSPECTIVE_CLIENT) {
        pthread_mutex_lock(&m->mu);
        m->negotiated_version = version;
        m->activity_pending = true;
        pthread_cond_broadcast(&m->cv);
        pthread_mutex_unlock(&m->mu);
    }
}

/* Terminal side effects are per-connection (logical_terminal); only a CLIENT
 * also latches the facade-wide terminal, since a server outlives an individual
 * child failing or closing. */
static void mm_on_refused(wtq_session_t *s, uint16_t http_status, void *user)
{
    (void)s;
    moq_wtquic_msquic_managed_conn_t *c = user;
    mm_mark_logical_terminal(c);
    /* refusal happens before establishment: terminal with no MoQ session */
    if (c->m->perspective == MOQ_PERSPECTIVE_CLIENT)
        mm_latch_fatal(c->m, http_status);
}

static void mm_on_failed(wtq_session_t *s, wtq_connect_failure_t why,
                         void *user)
{
    (void)s;
    moq_wtquic_msquic_managed_conn_t *c = user;
    mm_mark_logical_terminal(c);
    if (c->m->perspective == MOQ_PERSPECTIVE_CLIENT)
        mm_latch_fatal(c->m, (uint64_t)why);
}

static void mm_on_draining(wtq_session_t *s, void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    /* on_draining is optional in the attach table (left NULL there); a NULL
     * member is skipped like any other, so guard it before calling. */
    if (c->adapter != NULL && c->m->fwd->on_draining != NULL)
        c->m->fwd->on_draining(s, c->adapter);
}

static void mm_on_closed(wtq_session_t *s, uint32_t code,
                         const uint8_t *reason, size_t reason_len, bool clean,
                         void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    moq_wtquic_msquic_managed_t *m = c->m;
    mm_mark_logical_terminal(c);
    /* forward to the adapter (drives the bridge close) before latching the
     * facade terminal, mirroring the established-session teardown order */
    if (c->adapter != NULL)
        m->fwd->on_closed(s, code, reason, reason_len, clean, c->adapter);
    /* client only: a server outlives an individual child closing */
    if (m->perspective == MOQ_PERSPECTIVE_CLIENT) {
        if (clean)
            mm_latch_closed(m, code);
        else
            mm_latch_fatal(m, code);
    }
}

static void mm_on_stream_opened(wtq_session_t *s, wtq_stream_t *st, bool bidi,
                                void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_stream_opened(s, st, bidi, c->adapter);
}

static void mm_on_stream_data(wtq_session_t *s, wtq_stream_t *st,
                              const uint8_t *data, size_t len, bool fin,
                              void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_stream_data(s, st, data, len, fin, c->adapter);
}

static void mm_on_stream_reset(wtq_session_t *s, wtq_stream_t *st,
                               uint32_t app_code, void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_stream_reset(s, st, app_code, c->adapter);
}

static void mm_on_stream_stop(wtq_session_t *s, wtq_stream_t *st,
                              uint32_t app_code, void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_stream_stop(s, st, app_code, c->adapter);
}

static void mm_on_stream_closed(wtq_session_t *s, wtq_stream_t *st, void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_stream_closed(s, st, c->adapter);
}

static void mm_on_send_complete(wtq_session_t *s, void *send_ctx, bool canceled,
                                void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_send_complete(s, send_ctx, canceled, c->adapter);
}

static void mm_on_datagram(wtq_session_t *s, const uint8_t *data, size_t len,
                           void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_datagram(s, data, len, c->adapter);
}

static void mm_on_stream_writable(wtq_session_t *s, wtq_stream_t *st,
                                  void *user)
{
    moq_wtquic_msquic_managed_conn_t *c = user;
    if (c->adapter != NULL)
        c->m->fwd->on_stream_writable(s, st, c->adapter);
}

static void mm_on_quiesced(wtq_session_t *s, void *user)
{
    (void)s;
    moq_wtquic_msquic_managed_conn_t *c = user;
    pthread_mutex_lock(&c->m->mu);
    c->transport_quiesced = true;
    pthread_cond_broadcast(&c->m->cv);
    pthread_mutex_unlock(&c->m->mu);
}

static const wtq_session_events_t g_mm_events = {
    .struct_size = (uint32_t)sizeof(wtq_session_events_t),
    .on_established = mm_on_established,
    .on_refused = mm_on_refused,
    .on_failed = mm_on_failed,
    .on_draining = mm_on_draining,
    .on_closed = mm_on_closed,
    .on_stream_opened = mm_on_stream_opened,
    .on_stream_data = mm_on_stream_data,
    .on_stream_reset = mm_on_stream_reset,
    .on_stream_stop = mm_on_stream_stop,
    .on_stream_closed = mm_on_stream_closed,
    .on_send_complete = mm_on_send_complete,
    .on_datagram = mm_on_datagram,
    .on_stream_writable = mm_on_stream_writable,
};

/*
 * Reap one connection's MoQ + wtquic objects: destroy the attach adapter and the
 * MoQ session, then release the retained wtquic ref. Idempotent (safe to run
 * again after everything is NULL).
 *
 * The caller must guarantee no callback can still hold the adapter. There are
 * two valid preconditions:
 *   - LIVE SERVER REAP: the child is transport_quiesced (SHUTDOWN_COMPLETE fired)
 *     AND this runs under the owning lane guard — so the transport is done with
 *     it and no other callback on the lane can race.
 *   - GLOBAL TEARDOWN: the env barrier (wtq_msquic_env_close) has joined the
 *     MsQuic workers, so no callback runs at all.
 */
static void mm_reap_conn(moq_wtquic_msquic_managed_conn_t *c)
{
    if (c == NULL)
        return;
    /* Withdraw a still-published hold. Normal reclamation cannot reach here
     * with one (acknowledgment clears it and is a gate condition), but the
     * forced stop/destroy path tears down unacknowledged terminal children --
     * and the count must stay exactly #{server children with terminal && !ack}
     * at every observable point, including during teardown. */
#if defined(MOQ_WTQ_MM_TESTING)
    if (c->terminal_hold_counted) {
        pthread_mutex_lock(&c->m->mu);
        c->terminal_hold_counted = false;
        if (c->m->terminal_held_count > 0)
            c->m->terminal_held_count--;
        pthread_mutex_unlock(&c->m->mu);
    }
#endif
    if (c->adapter != NULL) {
        moq_wtquic_conn_destroy(c->adapter);
        c->adapter = NULL;
    }
    if (c->session != NULL) {
#if defined(MOQ_WTQ_MM_TESTING)
        /* under the fake transport seam the session is a sentinel, not a real
         * handle — the release seam being installed marks that case */
        if (g_mm_test_release == NULL)
#endif
            moq_session_destroy(c->session);
        c->session = NULL;
    }
    if (c->ws != NULL && c->owns_ws_ref) {
#if defined(MOQ_WTQ_MM_TESTING)
        if (g_mm_test_release != NULL)
            g_mm_test_release(c->ws);
        else
#endif
            wtq_session_release(c->ws);
        c->ws = NULL;
        c->owns_ws_ref = false;
    }
}

static void mm_reap_client(moq_wtquic_msquic_managed_t *m)
{
    mm_reap_conn(m->client);
}

/* Pre-barrier quiesce: initiate shutdown of one connection UNDER its owning
 * lane guard. Two things happen under the guard, both load-bearing:
 *   - set the persistent quiesce_requested flag, so a child that establishes
 *     LATE (its on_established has not run yet) observes it and converges
 *     instead of going live after the scan has passed it;
 *   - close an already-established ws.
 * wtq_session_close may SYNCHRONOUSLY deliver the session terminal callback
 * (only the backend ConnectionShutdown is queue-only); holding the lane guard
 * across it is what keeps that reentrant callback serialized with every other
 * callback on this lane, so it is safe. */
static void mm_quiesce_conn(moq_wtquic_msquic_managed_conn_t *c)
{
    if (c == NULL || c->lane == NULL)
        return;
    mm_guard_enter(c->lane);
    c->quiesce_requested = true;
    if (c->ws != NULL) {
#if defined(MOQ_WTQ_MM_TESTING)
        /* a fake-transport ws (from the connect seam) is not a real session; the
         * paired release seam being installed marks that case */
        if (g_mm_test_release == NULL)
#endif
            (void)wtq_session_close(c->ws, 0, NULL, 0);
    }
    mm_guard_leave(c->lane);
}

/* Runs AFTER listener_stop has joined in-flight accepts, so no accept_prepare /
 * accept_abandon can still mutate the admission fields (in_use / lane); reading
 * them here without m->mu is therefore race-free. */
static void mm_quiesce_all(moq_wtquic_msquic_managed_t *m)
{
    mm_quiesce_conn(m->client);
    if (m->children != NULL)
        for (uint32_t i = 0; i < m->max_connections; i++)
            if (m->children[i].in_use)
                mm_quiesce_conn(&m->children[i]);
}

/*
 * Client bring-up: open the MsQuic environment and dial the connection.
 *
 * The lane guard is installed in the client cfg (so wtquic serializes every
 * callback into the lane, the facade's per-lane single-threaded model) AND held
 * across wtq_msquic_client_connect through publication — see mm_client_start.
 *
 * On any failure the env stays open for destroy()/teardown to close (never
 * under a lane guard); this keeps the create-failure path a plain unwind.
 */
/* Map a wtquic result to a moq result. Caller-caused input errors (bad or
 * oversized offer strings, etc.) surface as INVAL; allocation failure as NOMEM;
 * anything else as INTERNAL. */
static moq_result_t mm_map_wtq_result(wtq_result_t r)
{
    switch (r) {
    case WTQ_OK:
        return MOQ_OK;
    case WTQ_ERR_NOMEM:
        return MOQ_ERR_NOMEM;
    case WTQ_ERR_INVALID_ARG:
    case WTQ_ERR_TOO_LARGE:
        return MOQ_ERR_INVAL;
    default:
        return MOQ_ERR_INTERNAL;
    }
}

/* Fill the env config: the bridged allocator (stored in m->wt_alloc for the
 * env's lifetime), the app name, and — only when the caller set a nonzero
 * idle_timeout_ms — the tuning with wtquic defaults plus that one override. A
 * zero idle timeout leaves tuning.struct_size == 0, which wtquic reads as "use
 * defaults". */
static void mm_fill_env_cfg(moq_wtquic_msquic_managed_t *m,
                            wtq_msquic_env_cfg_t *ecfg)
{
    m->wt_alloc.ctx = m->alloc.ctx;
    m->wt_alloc.alloc = m->alloc.alloc;
    m->wt_alloc.realloc = m->alloc.realloc;
    m->wt_alloc.free = m->alloc.free;

    memset(ecfg, 0, sizeof(*ecfg));
    ecfg->struct_size = (uint32_t)sizeof(*ecfg);
    ecfg->alloc = &m->wt_alloc;
    ecfg->app_name = "libmoq";
    if (m->idle_timeout_ms != 0) {
        wtq_msquic_tuning_init(&ecfg->tuning);
        ecfg->tuning.idle_timeout_ms = m->idle_timeout_ms;
    }
}

static moq_result_t mm_client_start(moq_wtquic_msquic_managed_t *m)
{
    wtq_msquic_env_cfg_t ecfg;
    mm_fill_env_cfg(m, &ecfg);
    wtq_result_t er = wtq_msquic_env_open(&ecfg, &m->env);
    if (er != WTQ_OK)
        return mm_map_wtq_result(er);

    moq_wtquic_msquic_managed_conn_t *c =
        m->alloc.alloc(sizeof(*c), m->alloc.ctx);
    if (c == NULL)
        return MOQ_ERR_NOMEM;
    memset(c, 0, sizeof(*c));
    c->m = m;
    c->lane = &m->lanes[0];
    m->client = c;
    m->fwd = moq_wtquic_conn_events();

    wtq_connect_config_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.struct_size = (uint32_t)sizeof(conn);
    conn.authority = m->host;
    conn.path = m->wt_path;
    conn.subprotocols = (const char *const *)m->protos;
    conn.subprotocol_count = m->proto_count;
    /* Speak the configured WebTransport-over-H3 dialect (default H3_CURRENT).
     * The libmoq profile values mirror wtq_webtransport_profile_t 1:1. memset
     * zeroed the v2 tail; struct_size (= sizeof(conn)) covers it. */
    conn.webtransport_profile = m->webtransport_profile;

    wtq_msquic_client_cfg_t ccfg;
    memset(&ccfg, 0, sizeof(ccfg));
    ccfg.struct_size = (uint32_t)sizeof(ccfg);
    ccfg.server_name = m->host;
    ccfg.port = m->port;
    ccfg.insecure_skip_verify = m->insecure_skip_verify;
    ccfg.connect = &conn;
    ccfg.events = &g_mm_events;
    ccfg.user = c;
    ccfg.guard = mm_lane_guard(c->lane);
    ccfg.on_transport_quiesced = mm_on_quiesced;

    /* Hold the lane guard across connect AND through publishing c->ws: a
     * connect-time callback blocks on the lane until the handle is stored, so
     * it is always visible before any event runs on it. wtquic's client-connect
     * abandon path keeps this safe — a synchronous close-time SHUTDOWN_COMPLETE
     * on an opened-but-unpublished connection is handled without entering the
     * guard, so holding it here cannot deadlock. */
    mm_guard_enter(c->lane);
    wtq_session_t *ws = NULL;
    wtq_result_t r;
#if defined(MOQ_WTQ_MM_TESTING)
    if (g_mm_test_connect != NULL)
        r = g_mm_test_connect(m->env, &ccfg, &ws);
    else
#endif
        r = wtq_msquic_client_connect(m->env, &ccfg, &ws);
    if (r == WTQ_OK) {
        c->ws = ws;
        c->owns_ws_ref = true;
    }
    mm_guard_leave(c->lane);
    return mm_map_wtq_result(r);
}

/* --- server admission ----------------------------------------------------- */

/* Reserve a free child slot from the fixed pool. Caller holds m->mu. A slot is
 * free either because it was never used or because live reap published it back;
 * the memset gives every reused slot a clean generation (no stale version, user,
 * terminal/quiesce/pump-seen, or close latch from its predecessor). */
static moq_wtquic_msquic_managed_conn_t *mm_reserve_child(
    moq_wtquic_msquic_managed_t *m)
{
    for (uint32_t i = 0; i < m->max_connections; i++) {
        if (!m->children[i].in_use) {
            moq_wtquic_msquic_managed_conn_t *c = &m->children[i];
            memset(c, 0, sizeof(*c));
            c->m = m;
            c->in_use = true;
            return c;
        }
    }
    return NULL;
}

/*
 * Admission (wtquic accept_prepare): runs at NEW_CONNECTION, BEFORE any wtquic
 * session exists. Reserve a child + assign its lane, and hand back the per-child
 * user and lane guard; refuse once the facade is draining/stopping or already at
 * max_connections. It touches only facade state — never wtq_session_*.
 */
static wtq_result_t mm_accept_prepare(void *luser,
                                      const wtq_msquic_accept_info_t *info,
                                      wtq_msquic_accept_decision_t *out)
{
    moq_wtquic_msquic_managed_t *m = luser;
    (void)info; /* peer addr / ALPN available; a facade info rides choose_lane */

    /* 1. RESERVE under the lock: refuse if draining or full, else take a slot
     * and the admission count. */
    pthread_mutex_lock(&m->mu);
    if (m->refuse_admissions || m->conn_count >= m->max_connections) {
        /* Attribute the refusal when the reserve is consumed by children that
         * are terminal but not yet acknowledged: that is the intentional
         * resource hold the acknowledgment contract creates, and it must be
         * observable rather than looking like ordinary load. */
#if defined(MOQ_WTQ_MM_TESTING)
        if (!m->refuse_admissions && m->terminal_held_count > 0)
            m->st_accepts_refused_terminal_held++;
#endif
        pthread_mutex_unlock(&m->mu);
        out->accepted = false; /* refuse -> QUIC_STATUS_CONNECTION_REFUSED */
        return WTQ_OK;
    }
    moq_wtquic_msquic_managed_conn_t *c = mm_reserve_child(m);
    if (c == NULL) { /* defensive: count < max implies a free slot */
        pthread_mutex_unlock(&m->mu);
        return WTQ_ERR_NOMEM;
    }
    m->conn_count++;
    uint32_t lane_idx = 0;
    bool have_lane = false;
    if (m->choose_lane == NULL) {
        /* round-robin is internal — no app code, safe under the lock */
        lane_idx = m->rr_lane;
        m->rr_lane = (m->rr_lane + 1u) % m->lane_count;
        have_lane = true;
    }
    pthread_mutex_unlock(&m->mu);

    /* 2. CALLBACK with NO locks held (choose_lane is application code that may
     * call any-thread APIs such as conn_count()/drain()). 3. VALIDATE: an
     * out-of-range index refuses the connection. */
    bool valid = true;
    if (!have_lane) {
        moq_wtquic_msquic_accept_info_t finfo = { .struct_size =
                                                      (uint32_t)sizeof(finfo) };
        lane_idx = m->choose_lane(m, &finfo, m->choose_lane_user);
        valid = (lane_idx < m->lane_count);
    }

    /* 4. RE-CHECK drain/stop (may have latched during the callback) and the
     * validity, then ROLLBACK or PUBLISH. */
    pthread_mutex_lock(&m->mu);
    if (!valid || m->refuse_admissions) {
        c->in_use = false;
        m->conn_count--;
        pthread_cond_broadcast(&m->cv); /* wake a drain/stop waiter */
        pthread_mutex_unlock(&m->mu);
        out->accepted = false;
        return WTQ_OK;
    }
    c->lane = &m->lanes[lane_idx];
    pthread_mutex_unlock(&m->mu);

    out->accepted = true;
    out->user = c;
    out->guard = mm_lane_guard(c->lane);
    return WTQ_OK;
}

/*
 * Guarded rollback (wtquic accept_abandon): fires EXACTLY ONCE, under the
 * child's guard, for a child admitted but failed before any callback. No
 * session/adapter exists yet — release the slot and the admission count.
 */
static void mm_accept_abandon(void *luser, void *user)
{
    moq_wtquic_msquic_managed_t *m = luser;
    moq_wtquic_msquic_managed_conn_t *c = user;
    pthread_mutex_lock(&m->mu);
    c->in_use = false;
    if (m->conn_count > 0)
        m->conn_count--;
    pthread_cond_broadcast(&m->cv); /* wake a drain/stop waiter */
    pthread_mutex_unlock(&m->mu);
}

/*
 * Open the env and start the listener. The children pool is allocated up front
 * (max_connections fixed slots) so accept_prepare only picks a slot. On any
 * failure the env/listener/pool stay for destroy()/teardown to release.
 */
static moq_result_t mm_server_start(moq_wtquic_msquic_managed_t *m)
{
    wtq_msquic_env_cfg_t ecfg;
    mm_fill_env_cfg(m, &ecfg);
    wtq_result_t er = wtq_msquic_env_open(&ecfg, &m->env);
    if (er != WTQ_OK)
        return mm_map_wtq_result(er);
    m->fwd = moq_wtquic_conn_events();

    size_t bytes;
    if (!mm_arr_bytes(m->max_connections, sizeof(*m->children), &bytes))
        return MOQ_ERR_INVAL;
    m->children = m->alloc.alloc(bytes, m->alloc.ctx);
    if (m->children == NULL)
        return MOQ_ERR_NOMEM;
    memset(m->children, 0, bytes);

    wtq_serve_config_t serve;
    memset(&serve, 0, sizeof(serve));
    serve.struct_size = (uint32_t)sizeof(serve);
    serve.path = m->wt_path;
    serve.subprotocols = (const char *const *)m->protos;
    serve.subprotocol_count = m->proto_count;
    serve.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg;
    memset(&lcfg, 0, sizeof(lcfg));
    lcfg.struct_size = (uint32_t)sizeof(lcfg);
    lcfg.bind_address = m->host; /* NULL binds the wildcard address */
    lcfg.port = m->port;
    lcfg.cert_file = m->cert_path;
    lcfg.key_file = m->key_path;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = &g_mm_events;
    lcfg.user = m;
    lcfg.accept_prepare = mm_accept_prepare;
    lcfg.accept_abandon = mm_accept_abandon;
    lcfg.on_transport_quiesced = mm_on_quiesced;
    /* Serve the SAME WebTransport dialect the client speaks, so a wtquic-msquic
     * client and server (e.g. the loopback smoke) share one profile and a
     * compat listener accepts only the matching ":protocol = webtransport"
     * CONNECT. Listener-wide; memset zeroed the v3 tail, struct_size covers it. */
    lcfg.webtransport_profile = m->webtransport_profile;

#if defined(MOQ_WTQ_MM_TESTING)
    g_mm_test_last_listener_profile = lcfg.webtransport_profile;
    if (g_mm_test_no_listener) {
        m->listener = NULL; /* accept callbacks are driven directly in tests */
        m->bound_port = 0;
        return MOQ_OK;
    }
#endif
    wtq_result_t lr = wtq_msquic_listener_start(m->env, &lcfg, &m->listener);
    if (lr != WTQ_OK)
        return mm_map_wtq_result(lr);
    m->bound_port = wtq_msquic_listener_port(m->listener);
    return MOQ_OK;
}

/* --- the pump: the exclusive per-lane access window ----------------------- */

/* One adapter service pass over the snapshot chain. moq_wtquic_conn_service is
 * what actually drives the bridge: it drains queued session actions, TICKS due
 * session deadlines (with the bridge's own WOULD_BLOCK/error/fatal handling),
 * and flushes to the wire. mm_in_service suppresses the adapter hook so this
 * pass does not re-arm the lane. phase (0 = pre-callback, 1 = post-callback) is
 * only consumed by the test service seam. Returns the number of PER-CONNECTION
 * service invocations (0 when the snapshot has no serviceable connection). */
static uint64_t mm_service_pass(moq_wtquic_msquic_managed_conn_t *head, int phase)
{
#if !defined(MOQ_WTQ_MM_TESTING)
    (void)phase;
#endif
    uint64_t n = 0;
    mm_in_service = true;
    for (moq_wtquic_msquic_managed_conn_t *c = head; c != NULL; c = c->snap_next) {
#if defined(MOQ_WTQ_MM_TESTING)
        if (g_mm_test_service != NULL) {
            g_mm_test_service(c, phase);
            n++;
            continue;
        }
#endif
        if (c->adapter != NULL) {
            moq_wtquic_conn_service(c->adapter);
            n++;
        }
    }
    mm_in_service = false;
    return n;
}

/*
 * One pump cycle for a lane, on the lane's owning (coordinator/doorbell) thread.
 * Mirrors the attach loop's shape. deadline_sweep is the pump CAUSE: only a
 * deadline-driven sweep runs the PRE-pump service pass (to deliver the due
 * deadline before on_lane_pump); an explicit wake, transport nudge, or teardown
 * pump runs pump → post-service only (the transport that nudged already
 * serviced). Returns the pump callback's rc.
 *
 * when the callback ran — those the app actually observed this pump. A terminal
 * caused DURING the callback or the service pass (e.g. a latched close, or a
 * synchronous on_closed from the service) was not observed and requires another
 * pump, so its state is snapshotted before the callback and applied after.
 */
static int mm_pump_lane(moq_wtquic_msquic_managed_t *m,
                        moq_wtquic_msquic_managed_lane_t *lane,
                        bool deadline_sweep)
{
    mm_guard_enter(lane);

    /* Build a stable snapshot CHAIN of this lane's connections, under m->mu (the
     * lane->facade lock order mm_on_quiesced uses). Each conn links via snap_next
     * in_window
     * gates the accessors. A concurrent accept adds to the pool, not this frozen
     * chain, so the sweep is stable — and there is no per-lane array, so total
     * scratch stays O(max_connections) even with lanes pumping concurrently. */
    moq_wtquic_msquic_managed_conn_t *head = NULL, **tail = &head, *c;
    pthread_mutex_lock(&m->mu);
    if (m->client != NULL && m->client->lane == lane) {
        *tail = m->client;
        tail = &m->client->snap_next;
    }
    if (m->children != NULL)
        for (uint32_t i = 0; i < m->max_connections; i++) {
            c = &m->children[i];
            if (c->in_use && c->lane == lane) {
                *tail = c;
                tail = &c->snap_next;
            }
        }
    *tail = NULL;
    pthread_mutex_unlock(&m->mu);

    /* terminal state BEFORE the callback: only these were observed this pump.
     * logical_terminal is only mutated under the lane guard (held here), so this
     * is stable. */
    for (c = head; c != NULL; c = c->snap_next) {
        c->in_window = true;
        /* Snapshot the event-progress token BEFORE the app pump so the
         * post-service re-drive check spans the whole pump+service cycle. */
        c->pump_pre_token = (c->adapter != NULL)
            ? moq_wtquic_conn_event_progress(c->adapter, NULL) : 0;
    }

    /* PRE-pump service pass — ONLY on a deadline sweep: service each adapter so
     * the bridge delivers the DUE session deadline (ticking through its own
     * result handling) and its events are pollable inside on_lane_pump. Ordinary
     * wakes skip this — the transport that nudged already serviced. */
    uint64_t svc = 0; /* per-connection service invocations, for the stat */
    if (deadline_sweep)
        svc += mm_service_pass(head, 0);

    const mm_pump_window_t win = { lane, head };
    mm_pump_win = &win;
    int rc = 0;
    if (m->on_lane_pump != NULL)
        rc = m->on_lane_pump(m, lane, mm_now_us(), m->on_lane_pump_user);
    /* the exclusive window closes WITH the callback: nothing below (a latched
     * close, adapter service, a synchronous on_closed) runs inside it */
    mm_pump_win = NULL;
    for (c = head; c != NULL; c = c->snap_next)
        c->in_window = false;

    /* execute a close the app latched during the callback. Deferring it here
     * (not inside conn_close) keeps the contract that wtquic's synchronous
     * on_closed — and thus the connection's final events — arrives on a LATER
     * pump, not the one that requested the close. The request is RETAINED across
     * pumps until the connection is established (a live ws + MoQ session); it is
     * cleared only after an actual close attempt or a terminal that pre-empts it
     * (a connection that never established has no session to close). */
    for (c = head; c != NULL; c = c->snap_next) {
        if (!c->close_pending)
            continue;
        if (c->logical_terminal) {
            c->close_pending = false; /* terminal already — nothing to close */
            continue;
        }
        if (c->ws == NULL || c->session == NULL)
            continue; /* not established yet — retain for a later pump */
#if defined(MOQ_WTQ_MM_TESTING)
        if (g_mm_test_close != NULL)
            g_mm_test_close(c->ws, c->close_pending_code);
        else if (g_mm_test_release == NULL)
            (void)wtq_session_close(c->ws, c->close_pending_code, NULL, 0);
#else
        (void)wtq_session_close(c->ws, c->close_pending_code, NULL, 0);
#endif
        c->close_pending = false;
    }

    /* POST-pump service pass: flush what the app's pump queued and feed any
     * pending terminal (on_transport_quiesced is marks-only — servicing here is
     * what delivers it). */
    svc += mm_service_pass(head, 1);

    /* Whole-cycle re-drive (mirrors the raw-MsQuic rule): a live child whose
     * event-progress token MOVED across this pump+service (its bounded pump
     * dequeued and/or service refilled) AND still holds undelivered events is
     * owed another pump. Re-arm ONE coalesced pass. Adapters are still alive
     * here — reap runs below. Bounded and spin-free: a non-draining app moves
     * no events once its queue is full, so the token stays put and nothing
     * re-arms. Not into teardown (facade stop/drain latched). */
    bool redrive = false;
    for (c = head; c != NULL; c = c->snap_next) {
        if (c->adapter == NULL || c->logical_terminal)
            continue;
        bool has_events = false;
        uint64_t tok = moq_wtquic_conn_event_progress(c->adapter, &has_events);
        if (tok != c->pump_pre_token && has_events) {
            redrive = true;
            break;
        }
    }
    if (redrive) {
        pthread_mutex_lock(&m->mu);
        bool live = !m->stop_requested && !m->drain_requested;
        pthread_mutex_unlock(&m->mu);
        if (live && !lane->bell_stop)
            mm_lane_arm(lane);
    }

    /*
     * Live reap (SERVER children only — the single client is held for global
     * teardown). A child is reaped once it satisfies ALL FOUR reap-gate
     * conditions, which can only be true now, under this lane's guard, AFTER the
     * callback and the service pass above. They are INDEPENDENT facts, deliberately
     * not one ordinal state: the session terminal can be enqueued before or after
     * the transport reaches its terminal.
     *   logical_terminal   — its transport reached a terminal;
     *   transport_quiesced — SHUTDOWN_COMPLETE fired, so no callback can still
     *                        touch its adapter/session (this is what makes the
     *                        destroy below safe under the guard);
     *   session terminal OBSERVED — the app actually polled
     *                        MOQ_EVENT_SESSION_CLOSED, read through the private
     *                        adapter SPI. A queued-but-unpolled terminal does not
     *                        count;
     *   app_terminal_acked — the app asserted its terminal processing is done
     *                        and it will not use the borrowed handles again.
     * Reclamation depends on these facts alone. A pump merely having presented
     * the child while it was terminal proves nothing -- an application that
     * never polls satisfies that -- so it is not part of the gate.
     * Its adapter/session are destroyed and the retained wtquic ref released
     * BEFORE the slot is published free; the count/in_use are then updated under
     * m->mu in lane->facade order, waking any drain/stop waiter. The slot is
     * reusable the instant in_use clears, so the child is never touched after
     * (snap_next of a reaped conn is not followed once we advance past it).
     */
    /* Recompute the lane's next deadline in the SAME traversal as live reap: a
     * SURVIVING child contributes its next session deadline (read while it is
     * still in_use — never after its slot is published free, which a concurrent
     * admission could already have memset + reassigned, corrupting snap_next); a
     * REAPED child is torn down instead. head is walked exactly once, and each
     * `next` is captured before any in_use = false. */
    uint64_t next_dl = UINT64_MAX;
    for (c = head; c != NULL;) {
        moq_wtquic_msquic_managed_conn_t *next = c->snap_next;
        /* Two reclamation contracts, by whether the child ever had a MoQ
         * session:
         *   session-backed  F1 && F2 && F4 && F5 -- the application must have
         *                   observed the terminal event and acknowledged;
         *   pre-session     F1 && F2 only -- a child whose negotiation or setup
         *                   failed before any session existed has no terminal
         *                   event to observe and nothing to acknowledge, so
         *                   requiring them would strand its capacity slot until
         *                   facade shutdown. */
        if (c != m->client && c->logical_terminal && c->transport_quiesced &&
            (!c->session_backed ||
             (mm_conn_terminal_observed(c) && c->app_terminal_acked))) {
            mm_reap_conn(c); /* destroy adapter/session, release ws — pre-publish */
            pthread_mutex_lock(&m->mu);
#if defined(MOQ_WTQ_MM_TESTING)
            m->st_children_reaped++;
#endif
            if (m->conn_count > 0)
                m->conn_count--;
            c->in_use = false; /* slot reusable: do not touch c after this */
            pthread_cond_broadcast(&m->cv);
            pthread_mutex_unlock(&m->mu);
#if defined(MOQ_WTQ_MM_TESTING)
            /* a test may reuse the just-published slot HERE (memset + reassign),
             * before we advance to the captured `next` — proving the traversal
             * never re-reads a reaped slot */
            if (g_mm_test_after_reap != NULL)
                g_mm_test_after_reap(c);
#endif
        } else if (c->session != NULL) {
            uint64_t d = UINT64_MAX;
#if defined(MOQ_WTQ_MM_TESTING)
            if (g_mm_test_next_deadline != NULL)
                d = g_mm_test_next_deadline(c);
            else if (g_mm_test_release == NULL) /* real session, not a sentinel */
                d = moq_session_next_deadline_us(c->session);
#else
            d = moq_session_next_deadline_us(c->session);
#endif
            if (d < next_dl)
                next_dl = d;
        }
        c = next;
    }

    mm_guard_leave(lane);

    /* Fold the facade-wide application service deadline (e.g. media_sender's
     * periodic catalog refresh) into the recomputed lane deadline, so a purely
     * time-based deadline wakes an otherwise-idle lane with no transport event.
     * Pure cached read, off the lane guard; recomputed every pump, so there is
     * no stale arm to guard against terminal state (the worker breaks on stop).
     * lane-guard is released, so this only takes ep->mu — the same lock the
     * pump already acquires via on_lane_pump. */
    if (m->app_deadline_us != NULL) {
        uint64_t ad = m->app_deadline_us(m->app_deadline_ctx);
        if (ad < next_dl)
            next_dl = ad;
    }

    /* Publish the recomputed deadline AND commit this pump's stats under bell_mu,
     * so the worker's timed-wait read, the test seam, and lane_get_stats all see
     * them consistently. service_passes counts the adapter service passes this
     * pump ran (post always; pre only on a deadline sweep). */
    pthread_mutex_lock(&lane->bell_mu);
    lane->deadline_us = next_dl;
    lane->stats.pump_sweeps++;
    if (deadline_sweep)
        lane->stats.deadline_sweeps++;
    lane->stats.service_passes += svc; /* actual per-connection invocations */
    pthread_mutex_unlock(&lane->bell_mu);

    /* Latch facade activity BEFORE notifying: a pump (transport-triggered or an
     * explicit wake) is exactly the level-retained activity moq_wtquic_msquic_
     * managed_wait() waits for, so wake any blocked waiter. */
    pthread_mutex_lock(&m->mu);
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);

    /* on_activity: signal-only notification that a pump ran, delivered OUTSIDE
     * the lane guard and the window. The pump-confined per-connection accessors
     * therefore return NULL from it, and the blocking teardown calls are refused
     * (mm_in_activity), so it cannot mutate sessions or self-deadlock; lane_wake
     * and the any-thread accessors remain available. */
    if (m->on_activity != NULL) {
        mm_in_activity = true;
        m->on_activity(m, m->on_activity_ctx);
        mm_in_activity = false;
    }
    return rc;
}

/* --- per-lane doorbell worker --------------------------------------------- */

/* Arm a lane's bell (any thread, including inside a pump). Leaf lock only, so it
 * can never deadlock against the lane guard or m->mu. A wake during the lane's
 * own pump leaves pending > served and rearms the worker for one more pass. */
static void mm_lane_arm(moq_wtquic_msquic_managed_lane_t *lane)
{
    pthread_mutex_lock(&lane->bell_mu);
    /* classify the wake by the ARMING thread's context (all stat writes are under
     * bell_mu, the leaf lock lane_get_stats reads under) */
    if (mm_pump_win != NULL && mm_pump_win->lane == lane)
        lane->stats.wakes_same_lane++;
    else if (mm_pump_win != NULL)
        lane->stats.wakes_cross_lane++;
    else
        lane->stats.wakes_external++;
    if (lane->pending == lane->served)
        lane->wake_since_us = mm_now_us(); /* first arm of a new batch */
    else
        lane->stats.wakes_coalesced++; /* folded into an unconsumed arm */
    lane->pending++;
    pthread_cond_signal(&lane->bell_cv);
    pthread_mutex_unlock(&lane->bell_mu);
}

/* The doorbell worker: wait for an arm, consume all pending arms (coalesce),
 * pump the lane OUTSIDE the bell lock, repeat until stopped. A nonzero pump rc
 * requests whole-facade teardown WITHOUT self-joining (stop_begin only latches
 * and signals the coordinator). */
static void *mm_lane_worker(void *arg)
{
    moq_wtquic_msquic_managed_lane_t *lane = arg;
    moq_wtquic_msquic_managed_t *m = lane->m;
    for (;;) {
        bool deadline_sweep = false;
        pthread_mutex_lock(&lane->bell_mu);
        /* Wait until: an arm (pending != served), the lane's MoQ-session deadline
         * arrives, or stop. Do NOT pump until activated — create() may still be
         * running its remaining fallible steps and has not published *out; an arm
         * that arrived meanwhile stays queued (pending untouched) and is
         * delivered on the first post-activation pass. An arm is checked FIRST,
         * so a wake racing the deadline is treated as a wake, not a sweep. */
        while (!lane->bell_stop) {
            if (lane->activated && lane->pending != lane->served)
                break; /* an arm -> pump (deadline_sweep stays false) */
            if (lane->activated && lane->deadline_us != UINT64_MAX) {
                uint64_t now = mm_now_us();
                if (now >= lane->deadline_us) {
                    deadline_sweep = true;
                    break; /* deadline reached -> pump (deadline sweep) */
                }
                /* Convert the MONOTONIC remaining duration to a REALTIME abstime
                 * — moq deadlines live in the monotonic domain but
                 * pthread_cond_timedwait uses CLOCK_REALTIME by default. */
                struct timespec ts;
                mm_abstime(&ts, lane->deadline_us - now);
                (void)pthread_cond_timedwait(&lane->bell_cv, &lane->bell_mu, &ts);
                continue; /* re-evaluate: arm, deadline, or spurious wake */
            }
#if defined(MOQ_WTQ_MM_TESTING)
            /* fire a one-shot test hook at the check-to-wait boundary, still
             * under bell_mu, so a test can arm exactly here (its arm blocks on
             * bell_mu until this cond_wait releases it — proving the arm cannot
             * be lost between the idle check and the wait) */
            void (*h)(void) = atomic_exchange(&g_mm_test_prewait, NULL);
            if (h != NULL)
                h();
#endif
            pthread_cond_wait(&lane->bell_cv, &lane->bell_mu);
        }
        if (lane->bell_stop) {
            pthread_mutex_unlock(&lane->bell_mu);
            break;
        }
        /* wake->pump latency stat (bell_mu held) — only for arm-driven pumps; a
         * deadline sweep has no wake to time against */
        if (!deadline_sweep) {
            uint64_t s = mm_now_us() - lane->wake_since_us;
            lane->stats.wake_to_pump_total_us += s;
            if (s > lane->stats.wake_to_pump_max_us)
                lane->stats.wake_to_pump_max_us = s;
            lane->stats.wake_to_pump_samples++;
        }
        lane->served = lane->pending; /* consume arms (deadline-only: a no-op) */
        pthread_mutex_unlock(&lane->bell_mu);

        int rc = mm_pump_lane(m, lane, deadline_sweep);
        if (rc != 0)
            (void)moq_wtquic_msquic_managed_stop_begin(m);
    }
    return NULL;
}

/* Spawn one worker per lane. On partial failure the caller unwinds via
 * mm_stop_workers (join whatever started). */
static moq_result_t mm_start_workers(moq_wtquic_msquic_managed_t *m)
{
    for (uint32_t i = 0; i < m->lane_count; i++) {
        if (mm_thread_create(&m->lanes[i].worker, mm_lane_worker,
                             &m->lanes[i]) != 0)
            return MOQ_ERR_INTERNAL;
        m->lanes[i].worker_started = true;
    }
    return MOQ_OK;
}

/* Release the doorbell workers to pump. Called ONLY after every fallible create()
 * step has succeeded and *out is published, so a worker never runs application
 * code on a facade whose construction can still fail. Any arm queued during
 * bring-up is delivered on the worker's first pass after this. */
static void mm_activate_workers(moq_wtquic_msquic_managed_t *m)
{
    for (uint32_t i = 0; i < m->lane_count; i++) {
        moq_wtquic_msquic_managed_lane_t *lane = &m->lanes[i];
        pthread_mutex_lock(&lane->bell_mu);
        lane->activated = true;
        pthread_cond_signal(&lane->bell_cv);
        pthread_mutex_unlock(&lane->bell_mu);
    }
}

/* Stop and JOIN every doorbell worker. Idempotent. Runs with NO lane lock held,
 * so it is safe to call before the transport barrier; after it returns, only the
 * coordinator pumps. */
static void mm_stop_workers(moq_wtquic_msquic_managed_t *m)
{
    if (m->lanes == NULL)
        return;
    for (uint32_t i = 0; i < m->lane_count; i++) {
        moq_wtquic_msquic_managed_lane_t *lane = &m->lanes[i];
        if (!lane->bell_inited)
            continue;
        pthread_mutex_lock(&lane->bell_mu);
        lane->bell_stop = true;
        pthread_cond_signal(&lane->bell_cv);
        pthread_mutex_unlock(&lane->bell_mu);
        if (lane->worker_started) {
            /* A failed join means the worker is still live but unjoinable;
             * proceeding would tear down the bell/lane and free the facade out
             * from under it (UAF). There is no safe recovery — the only join
             * errors are programming invariants (EDEADLK/EINVAL/ESRCH) — so this
             * is a hard-abort invariant, not a continuable error. */
            if (pthread_join(lane->worker, NULL) != 0)
                abort();
            lane->worker_started = false;
        }
    }
}

/* --- coordinator ---------------------------------------------------------- */

/* Server: reap any child STILL in use at global teardown — those the final pump
 * did not already live-reap (e.g. a child that never reached transport_quiesced,
 * so it missed the live gate). A live-reaped slot has in_use == false and is
 * skipped here, so it is never reaped twice. The env barrier has joined all
 * workers and the final pump has run, so destroying each adapter/session is
 * safe. The pool storage itself is freed in destroy(). */
static void mm_reap_children(moq_wtquic_msquic_managed_t *m)
{
    if (m->children == NULL)
        return;
    for (uint32_t i = 0; i < m->max_connections; i++) {
        moq_wtquic_msquic_managed_conn_t *c = &m->children[i];
        if (!c->in_use)
            continue;
        mm_reap_conn(c);
        c->in_use = false;
    }
    /* every live child is now reaped; the count must reflect that (admissions
     * are refused before this runs, and the barrier joined all workers, so no
     * accept can race). */
    pthread_mutex_lock(&m->mu);
    m->conn_count = 0;
    pthread_mutex_unlock(&m->mu);
}

static void mm_run_teardown(moq_wtquic_msquic_managed_t *m)
{
    /* Ordered teardown (admissions are already refused via stop_begin/drain):
     *   1. STOP THE LISTENER and join in-flight accepts, so no accept_prepare /
     *      accept_abandon can still reserve a slot or mutate admission fields.
     *      A child that was already admitted may still be mid-establishment;
     *      env_close (step 4) has not run, so its on_established can still fire.
     *   2. STOP + JOIN the doorbell workers, so from here ONLY the coordinator
     *      pumps: the quiesce, barrier, final pump, and reap below own the lane
     *      guards without racing a worker. Joined with no lane lock held.
     *   3. QUIESCE every live connection under its owning lane guard: set the
     *      persistent quiesce flag (a late on_established converges on it) and
     *      close an already-established ws. This is the pre-barrier quiesce, so
     *      the transport barrier below is not left waiting on a session an
     *      adapter still references.
     *   4. run the TRANSPORT BARRIER with no lane guard held (env_close joins
     *      the MsQuic workers — no callback can then hold an adapter).
     *   5. exactly ONE explicit coordinator-owned final pump per lane, so the
     *      app sees each connection's final events and acknowledgment is
     *      recorded.
     *   6. REAP the adapters/sessions.
     * This runs BEFORE the coordinator publishes `stopped`, so no pump or
     * application callback runs after on_stopped/join completes. */
    if (m->listener != NULL) {
        wtq_msquic_listener_stop(m->listener);
        m->listener = NULL;
    }
    mm_stop_workers(m);
    mm_quiesce_all(m);
    if (m->env != NULL) {
        wtq_msquic_env_close(m->env);
        m->env = NULL;
    }
    for (uint32_t i = 0; i < m->lane_count; i++)
        (void)mm_pump_lane(m, &m->lanes[i], false); /* teardown pump, not a sweep */
    mm_reap_client(m);   /* client: reaps m->client */
    mm_reap_children(m); /* server: reaps the pool + zeroes the count */
}

static void *mm_coord_main(void *arg)
{
    moq_wtquic_msquic_managed_t *m = arg;

    pthread_mutex_lock(&m->mu);
    while (!m->stop_requested)
        pthread_cond_wait(&m->cv, &m->mu);
    pthread_mutex_unlock(&m->mu);

    mm_run_teardown(m);

    /* Read the continuation BEFORE publishing completion: once `stopped`
     * broadcasts, a joiner (or the on_stopped continuation) may free m. */
    void (*cb)(void *) = m->on_stopped;
    void *cb_ctx = m->on_stopped_ctx;
    pthread_mutex_lock(&m->mu);
    m->stopped = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
    /* LAST facade access. join path (cb == NULL): a joiner may already be
     * freeing m, so nothing below touches it. on_stopped path: join is
     * refused, so no free races us; cb() is the final access and may destroy
     * m — after which we touch nothing. */
    if (cb != NULL)
        cb(cb_ctx);
    return NULL;
}

/* --- public API ----------------------------------------------------------- */

void moq_wtquic_msquic_managed_cfg_init_sized(
    moq_wtquic_msquic_managed_cfg_t *cfg, size_t size)
{
    if (cfg == NULL)
        return;
    if (size > sizeof(*cfg))
        size = sizeof(*cfg);
    memset(cfg, 0, size);
    if (size >= sizeof(uint32_t))
        cfg->struct_size = (uint32_t)size;
}

moq_result_t moq_wtquic_msquic_managed_create(
    const moq_wtquic_msquic_managed_cfg_t *cfg,
    moq_wtquic_msquic_managed_t **out)
{
    if (out == NULL)
        return MOQ_ERR_INVAL;
    *out = NULL;
    if (cfg == NULL)
        return MOQ_ERR_INVAL;
#define MM_CFG_HAS(field)                                                   \
    ((size_t)cfg->struct_size >=                                            \
     offsetof(moq_wtquic_msquic_managed_cfg_t, field) + sizeof(cfg->field))
    /* v0 floor: the required prefix runs through on_activity_ctx (so alloc,
     * perspective, host/port, cert/key, and on_lane_pump are always present);
     * everything past it is optional and gated field-by-field. */
    if ((size_t)cfg->struct_size < MOQ_WTQUIC_MSQUIC_MANAGED_CFG_V0_SIZE ||
        cfg->alloc == NULL || cfg->alloc->alloc == NULL ||
        cfg->alloc->free == NULL || cfg->alloc->realloc == NULL ||
        cfg->on_lane_pump == NULL)
        return MOQ_ERR_INVAL;
    if (cfg->perspective != MOQ_PERSPECTIVE_CLIENT &&
        cfg->perspective != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;
    if (cfg->perspective == MOQ_PERSPECTIVE_CLIENT) {
        if (cfg->host == NULL || cfg->port == 0)
            return MOQ_ERR_INVAL;
    } else {
        if (cfg->cert_path == NULL || cfg->key_path == NULL)
            return MOQ_ERR_INVAL;
    }

    uint32_t max_connections = 1;
    if (MM_CFG_HAS(max_connections) && cfg->max_connections > 0)
        max_connections = cfg->max_connections;
    uint32_t lane_count = 1;
    if (MM_CFG_HAS(lane_count) && cfg->lane_count > 0)
        lane_count = cfg->lane_count;
    if (cfg->perspective == MOQ_PERSPECTIVE_CLIENT) {
        lane_count = 1; /* one connection -> one lane */
    } else if (lane_count > max_connections) {
        lane_count = max_connections; /* no more lanes than connections */
    }
    /* No arbitrary lane cap: the requested topology is honored and bounded
     * only by checked allocation — a count so large it overflows the array
     * size is rejected (INVAL), and one that merely exhausts memory fails at
     * the allocation (NOMEM). Neither silently changes the count. */
    size_t lanes_bytes;
    if (!mm_arr_bytes(lane_count, sizeof(moq_wtquic_msquic_managed_lane_t),
                      &lanes_bytes))
        return MOQ_ERR_INVAL;

    moq_wtquic_msquic_managed_t *m =
        cfg->alloc->alloc(sizeof(*m), cfg->alloc->ctx);
    if (m == NULL)
        return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *cfg->alloc;
    m->max_connections = max_connections;
    /* Sync primitives with checked results: on failure unwind through
     * destroy(), which only tears down what was actually initialized. */
    if (mm_mutex_init(&m->mu) != 0) {
        moq_wtquic_msquic_managed_destroy(m);
        return MOQ_ERR_INTERNAL;
    }
    m->mu_inited = true;
    if (mm_cond_init(&m->cv) != 0) {
        moq_wtquic_msquic_managed_destroy(m);
        return MOQ_ERR_INTERNAL;
    }
    m->cv_inited = true;
    m->perspective = cfg->perspective;
    m->port = cfg->port;
    m->insecure_skip_verify = cfg->insecure_skip_verify;
    m->idle_timeout_ms = cfg->idle_timeout_ms;
    m->on_lane_pump = cfg->on_lane_pump;
    m->on_lane_pump_user = cfg->on_lane_pump_user;
    m->on_activity = cfg->on_activity;
    m->on_activity_ctx = cfg->on_activity_ctx;

    /* each optional tail field gates on ITS OWN complete fit */
    if (MM_CFG_HAS(on_stopped))
        m->on_stopped = cfg->on_stopped;
    if (MM_CFG_HAS(on_stopped_ctx))
        m->on_stopped_ctx = cfg->on_stopped_ctx;
    if (MM_CFG_HAS(send_request_capacity))
        m->send_request_capacity = cfg->send_request_capacity;
    if (MM_CFG_HAS(initial_request_capacity))
        m->initial_request_capacity = cfg->initial_request_capacity;
    if (MM_CFG_HAS(max_events))
        m->max_events = cfg->max_events;
    if (MM_CFG_HAS(max_actions))
        m->max_actions = cfg->max_actions;
    /* max_connections / lane_count were validated + capped above */
    if (MM_CFG_HAS(choose_lane))
        m->choose_lane = cfg->choose_lane;
    if (MM_CFG_HAS(choose_lane_user))
        m->choose_lane_user = cfg->choose_lane_user;
    if (MM_CFG_HAS(streaming_objects))
        m->streaming_objects = cfg->streaming_objects;
    if (MM_CFG_HAS(session_idle_timeout_us))
        m->session_idle_timeout_us = cfg->session_idle_timeout_us;
    if (MM_CFG_HAS(webtransport_profile)) {
        /* Only the two defined profiles; reject anything else up front (both
         * the client connect and the server listener would otherwise reject it
         * later, less directly). */
        if (cfg->webtransport_profile > MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT)
            goto inval;
        m->webtransport_profile = cfg->webtransport_profile;
    }
    /* app_deadline_us + app_deadline_ctx are ONE ABI block: gated on the LAST
     * field's fit, so the pair is read together or not at all — never the
     * callback with a context read past the caller's struct. */
    if (MM_CFG_HAS(app_deadline_ctx)) {
        m->app_deadline_us = cfg->app_deadline_us;
        m->app_deadline_ctx = cfg->app_deadline_ctx;
    }
    m->on_stopped_owns = m->on_stopped != NULL;

    /* owned copies of every borrowed input */
    m->host = mm_strdup(m, cfg->host);
    m->cert_path = mm_strdup(m, cfg->cert_path);
    m->key_path = mm_strdup(m, cfg->key_path);
    m->wt_path = mm_strdup(m, MM_CFG_HAS(wt_path) && cfg->wt_path != NULL
                                  ? cfg->wt_path
                                  : "/moq");
    if ((cfg->host != NULL && m->host == NULL) ||
        (cfg->cert_path != NULL && m->cert_path == NULL) ||
        (cfg->key_path != NULL && m->key_path == NULL) || m->wt_path == NULL)
        goto oom;

    /* Always run the validator on the size-gated members, so a nonzero count
     * with a NULL array is REJECTED (mm_dup_protos) rather than silently
     * falling back to defaults. */
    {
        const char *const *protos =
            MM_CFG_HAS(wt_protocols) ? cfg->wt_protocols : NULL;
        size_t proto_n =
            MM_CFG_HAS(wt_protocol_count) ? cfg->wt_protocol_count : 0;
        moq_result_t prc = mm_dup_protos(m, protos, proto_n);
        if (prc == MOQ_ERR_NOMEM)
            goto oom;
        if (prc != MOQ_OK)
            goto inval;
    }
#undef MM_CFG_HAS

    /* Default subprotocol list. The header documents NULL/0 wt_protocols as the
     * facade default "draft-18 then draft-16"; materialize it as a concrete list
     * so both the wire offer (client) / advertised set (server) and the
     * offered-token check at establishment see the same bytes. An explicit list
     * is left exactly as given. */
    if (m->proto_count == 0) {
        static const char *const mm_default_protos[] = { "moqt-18", "moqt-16" };
        moq_result_t prc = mm_dup_protos(m, mm_default_protos, 2);
        if (prc != MOQ_OK)
            goto oom; /* the only failure for the fixed defaults is NOMEM */
    }

    /* lanes (lanes_bytes was overflow-checked above) */
    m->lane_count = lane_count;
    m->lanes = m->alloc.alloc(lanes_bytes, m->alloc.ctx);
    if (m->lanes == NULL)
        goto oom;
    memset(m->lanes, 0, lanes_bytes);
    for (uint32_t i = 0; i < lane_count; i++) {
        if (mm_mutex_init(&m->lanes[i].mu) != 0)
            goto fail;
        if (mm_mutex_init(&m->lanes[i].bell_mu) != 0) {
            pthread_mutex_destroy(&m->lanes[i].mu);
            goto fail;
        }
        if (mm_cond_init(&m->lanes[i].bell_cv) != 0) {
            pthread_mutex_destroy(&m->lanes[i].bell_mu);
            pthread_mutex_destroy(&m->lanes[i].mu);
            goto fail;
        }
        m->lanes[i].bell_inited = true;
        m->lanes_inited = i + 1;
        m->lanes[i].m = m;
        m->lanes[i].index = i;
        m->lanes[i].deadline_us = UINT64_MAX; /* no deadline until a pump computes one */
        m->lanes[i].stats.struct_size =
            (uint32_t)sizeof(m->lanes[i].stats);
    }

    /* Bring up the transport BEFORE the coordinator so a bring-up failure is a
     * plain unwind: destroy() closes the env/listener on the !coord_started path
     * (never under a lane guard). Once the coordinator is running, the transport
     * barrier moves to its ordered teardown. */
    {
        moq_result_t crc = (m->perspective == MOQ_PERSPECTIVE_CLIENT)
                               ? mm_client_start(m)
                               : mm_server_start(m);
        if (crc != MOQ_OK) {
            /* propagate the exact cause (NOMEM / INVAL / INTERNAL) rather than
             * flattening it through the generic fail/oom labels */
            moq_wtquic_msquic_managed_destroy(m);
            return crc;
        }
    }

    /* Start the per-lane doorbell workers before the coordinator, so a spawn
     * failure unwinds through destroy() (which joins whatever started) with no
     * coordinator to tear down. Each worker is spawned UNACTIVATED: it parks and
     * will not pump — even if a transport hook queues an arm during bring-up —
     * until mm_activate_workers below, after every fallible step has succeeded. */
    if (mm_start_workers(m) != MOQ_OK)
        goto fail;

    /* Spawn the coordinator LAST: once create() has succeeded the coordinator
     * exists, so stop initiation can never fail; and no create-failure path
     * ever has to tear a running coordinator down (rollback closes the env, if
     * opened, then frees). */
    if (mm_thread_create(&m->coord, mm_coord_main, m) != 0)
        goto fail;
    m->coord_started = true;

    /* All fallible construction has succeeded. Publish the handle, then release
     * the workers — so no application pump/on_activity can run while create()
     * could still fail. */
    *out = m;
    mm_activate_workers(m);
    return MOQ_OK;
oom:
    moq_wtquic_msquic_managed_destroy(m);
    return MOQ_ERR_NOMEM;
inval:
    moq_wtquic_msquic_managed_destroy(m);
    return MOQ_ERR_INVAL;
fail:
    moq_wtquic_msquic_managed_destroy(m);
    return MOQ_ERR_INTERNAL;
}

bool moq_wtquic_msquic_managed_stop_begin(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return false;
    /* Only latch + signal the coordinator: legal from ANY thread including a
     * callback, never blocks, cannot fail. */
    bool first = false;
    pthread_mutex_lock(&m->mu);
    if (!m->stop_requested) {
        m->stop_requested = true;
        m->drain_requested = true;
        m->refuse_admissions = true; /* no child admitted past the barrier */
        first = true;
        pthread_cond_broadcast(&m->cv);
    }
    pthread_mutex_unlock(&m->mu);
    return first;
}

moq_result_t moq_wtquic_msquic_managed_join(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    if (mm_in_callback(m))
        return MOQ_ERR_WRONG_STATE;
    if (m->on_stopped_owns)
        return MOQ_ERR_WRONG_STATE; /* completion is delivered via on_stopped */
    pthread_mutex_lock(&m->mu);
    while (!m->stopped)
        pthread_cond_wait(&m->cv, &m->mu);
    pthread_mutex_unlock(&m->mu);
    return MOQ_OK;
}

moq_result_t moq_wtquic_msquic_managed_stop(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    if (mm_in_callback(m))
        return MOQ_ERR_WRONG_STATE;
    /* stop() is the join-based composition. When on_stopped owns completion
     * join() cannot run, so refuse WITHOUT side effects (do not latch): the
     * actor host must drive teardown with stop_begin() + on_stopped. */
    if (m->on_stopped_owns)
        return MOQ_ERR_WRONG_STATE;
    (void)moq_wtquic_msquic_managed_stop_begin(m);
    return moq_wtquic_msquic_managed_join(m);
}

void moq_wtquic_msquic_managed_destroy(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return;
    if (m->coord_started) {
        (void)moq_wtquic_msquic_managed_stop_begin(m); /* idempotent */
        if (pthread_equal(pthread_self(), m->coord)) {
            /* called from inside on_stopped (the coordinator itself): it is
             * finishing and touches nothing after the continuation returns, so
             * detach it rather than self-join, then free. */
            pthread_detach(m->coord);
        } else {
            pthread_mutex_lock(&m->mu);
            while (!m->stopped)
                pthread_cond_wait(&m->cv, &m->mu);
            pthread_mutex_unlock(&m->mu);
            pthread_join(m->coord, NULL);
        }
    }
    /* Create-failure path only: the coordinator never ran its teardown, so run
     * the transport barrier (stop the listener, close the env — no lane held)
     * and reap here. On every path where the coordinator ran, its teardown
     * already did this — listener/env NULL, adapters/sessions released — so
     * these are no-ops. The child/pool storage itself is freed here in all
     * cases. */
    if (!m->coord_started) {
        /* same order as mm_run_teardown: stop the listener (join accepts), join
         * the doorbell workers, THEN quiesce + barrier. Usually a no-op here —
         * create failed before any child established. */
        if (m->listener != NULL) {
            wtq_msquic_listener_stop(m->listener);
            m->listener = NULL;
        }
        mm_stop_workers(m);
        mm_quiesce_all(m);
        if (m->env != NULL) {
            wtq_msquic_env_close(m->env);
            m->env = NULL;
        }
    }
    mm_reap_client(m);
    mm_reap_children(m);
    if (m->client != NULL) {
        m->alloc.free(m->client, sizeof(*m->client), m->alloc.ctx);
        m->client = NULL;
    }
    if (m->children != NULL) {
        m->alloc.free(m->children,
                      (size_t)m->max_connections * sizeof(*m->children),
                      m->alloc.ctx);
        m->children = NULL;
    }
    mm_free_str(m, m->host);
    mm_free_str(m, m->cert_path);
    mm_free_str(m, m->key_path);
    mm_free_str(m, m->wt_path);
    mm_free_protos(m);
    if (m->lanes != NULL) {
        for (uint32_t i = 0; i < m->lanes_inited; i++)
            pthread_mutex_destroy(&m->lanes[i].mu);
        for (uint32_t i = 0; i < m->lane_count; i++)
            if (m->lanes[i].bell_inited) {
                pthread_cond_destroy(&m->lanes[i].bell_cv);
                pthread_mutex_destroy(&m->lanes[i].bell_mu);
            }
        m->alloc.free(m->lanes, m->lane_count * sizeof(*m->lanes),
                      m->alloc.ctx);
    }
    /* only destroy sync primitives that were actually initialized */
    if (m->cv_inited)
        pthread_cond_destroy(&m->cv);
    if (m->mu_inited)
        pthread_mutex_destroy(&m->mu);
    moq_alloc_t alloc = m->alloc;
    alloc.free(m, sizeof(*m), alloc.ctx);
}

/* --- client accessors ----------------------------------------------------- */

/* Single-client convenience accessors. The handles are NULL until the client
 * establishes (negotiation succeeds) and again once it is reaped; they are
 * pump-window-only, per the header confinement contract — NULL unless read
 * inside the client's on_lane_pump. */
moq_session_t *moq_wtquic_msquic_managed_session(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL || m->client == NULL || !mm_conn_in_window(m->client))
        return NULL;
    return m->client->session;
}

moq_wtquic_conn_t *moq_wtquic_msquic_managed_adapter(
    moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL || m->client == NULL || !mm_conn_in_window(m->client))
        return NULL;
    return m->client->adapter;
}

/* --- lanes ---------------------------------------------------------------- */

uint32_t moq_wtquic_msquic_managed_lane_count(
    const moq_wtquic_msquic_managed_t *m)
{
    return m == NULL ? 0 : m->lane_count;
}

moq_wtquic_msquic_managed_lane_t *moq_wtquic_msquic_managed_lane(
    moq_wtquic_msquic_managed_t *m, uint32_t index)
{
    if (m == NULL || index >= m->lane_count)
        return NULL;
    return &m->lanes[index];
}

uint32_t moq_wtquic_msquic_lane_index(
    const moq_wtquic_msquic_managed_lane_t *lane)
{
    return lane == NULL ? 0 : lane->index;
}

moq_wtquic_msquic_managed_conn_t *moq_wtquic_msquic_lane_next_conn(
    moq_wtquic_msquic_managed_lane_t *lane,
    moq_wtquic_msquic_managed_conn_t *prev)
{
    /* only valid inside THIS lane's pump; walk the stable snapshot chain */
    if (lane == NULL || !mm_in_lane_pump(lane))
        return NULL;
    if (prev == NULL)
        return mm_pump_win->head;
    if (prev->lane != lane || !prev->in_window)
        return NULL; /* not a member of this window */
    return prev->snap_next;
}

moq_result_t moq_wtquic_msquic_lane_wake(
    moq_wtquic_msquic_managed_lane_t *lane)
{
    if (lane == NULL)
        return MOQ_ERR_INVAL;
    moq_wtquic_msquic_managed_t *m = lane->m;
    /* Refuse once facade teardown is observed (the worker is stopping/joined);
     * otherwise arm the lane's bell — coalesced, deferred (never inline, so a
     * wake from inside this lane's own pump schedules one more pass without
     * re-entering). An MOQ_OK racing stop_begin may be absorbed by teardown. */
    pthread_mutex_lock(&m->mu);
    bool closed = m->stop_requested;
    pthread_mutex_unlock(&m->mu);
    if (closed)
        return MOQ_ERR_CLOSED;
    mm_lane_arm(lane);
    return MOQ_OK;
}

moq_result_t moq_wtquic_msquic_lane_get_stats(
    moq_wtquic_msquic_managed_lane_t *lane,
    moq_wtquic_msquic_lane_stats_t *out, size_t out_size)
{
    if (lane == NULL || out == NULL ||
        out_size < MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    /* Caller-context discrimination (mirrors moq_wtquic_msquic_lane_wake's arms):
     * refuse a DIFFERENT lane's callback (WRONG_STATE) per the documented
     * contract; from this lane's own pump or an external/coordinator thread, read
     * the counters under bell_mu — the leaf lock every stat write also holds, so
     * a snapshot is consistent and cannot invert against the lane guard. */
    if (!mm_in_lane_pump(lane) && mm_pump_win != NULL)
        return MOQ_ERR_WRONG_STATE; /* a different lane's callback */
    size_t n = out_size < sizeof(lane->stats) ? out_size : sizeof(lane->stats);
    pthread_mutex_lock(&lane->bell_mu);
    memset(out, 0, out_size);
    memcpy(out, &lane->stats, n);
    out->struct_size = (uint32_t)n;
    pthread_mutex_unlock(&lane->bell_mu);
    return MOQ_OK;
}

/* --- connections ---------------------------------------------------------- */

moq_session_t *moq_wtquic_msquic_managed_conn_session(
    moq_wtquic_msquic_managed_conn_t *conn)
{
    /* borrowed + pump-confined: valid only inside the owning lane's pump */
    return mm_conn_in_window(conn) ? conn->session : NULL;
}

moq_wtquic_conn_t *moq_wtquic_msquic_managed_conn_adapter(
    moq_wtquic_msquic_managed_conn_t *conn)
{
    return mm_conn_in_window(conn) ? conn->adapter : NULL;
}

moq_wtquic_msquic_managed_lane_t *moq_wtquic_msquic_managed_conn_lane(
    moq_wtquic_msquic_managed_conn_t *conn)
{
    /* pump-window-only, like every conn accessor */
    return mm_conn_in_window(conn) ? conn->lane : NULL;
}

moq_version_t moq_wtquic_msquic_managed_conn_negotiated_version(
    moq_wtquic_msquic_managed_conn_t *conn)
{
    /* pump-window-only, like every conn accessor */
    return mm_conn_in_window(conn) ? conn->version : 0;
}

void moq_wtquic_msquic_managed_conn_set_user(
    moq_wtquic_msquic_managed_conn_t *conn, void *user)
{
    /* pump-window-only, like every conn accessor */
    if (mm_conn_in_window(conn))
        conn->user = user;
}

void *moq_wtquic_msquic_managed_conn_user(
    const moq_wtquic_msquic_managed_conn_t *conn)
{
    return mm_conn_in_window(conn) ? conn->user : NULL;
}

/*
 * Terminal acknowledgment. From this connection's OWNING lane callback the
 * application asserts that its terminal processing for the connection is
 * COMPLETE, that it no longer needs the child kept iterable, and that it will
 * not use the borrowed conn/session/adapter handles after the callback returns.
 * Those handles are borrowed views owned by the facade, not reference-counted
 * possessions -- nothing is released here, and independently owned objects the
 * application obtained earlier are unaffected. libmoq records that the
 * assertion was made; it cannot verify it.
 *
 * NULL is MOQ_ERR_INVAL. The single client connection is not reaped per-child
 * and is excluded from admission-capacity accounting, so it is
 * MOQ_ERR_WRONG_STATE. Accepted only for a connection in the caller's own pump window
 * (mm_conn_in_window), which is exactly "this connection, during its owning
 * lane callback" -- so no lane-plus-token parameter is needed and the handle
 * cannot escape the callback. Before the session terminal has been OBSERVED
 * (polled, not merely queued) this returns MOQ_ERR_WRONG_STATE even when the
 * transport is fully terminal. A repeat call in the same valid callback is
 * harmless and returns MOQ_OK. A stale or released handle is invalid input:
 * no rejection is promised and none is attempted.
 *
 * See moq/wtquic_msquic_managed.h for the normative contract.
 */
moq_result_t moq_wtquic_msquic_managed_conn_ack_terminal(
    moq_wtquic_msquic_managed_conn_t *conn)
{
    if (conn == NULL)
        return MOQ_ERR_INVAL;
    if (!mm_conn_in_window(conn))
        return MOQ_ERR_WRONG_STATE;
    if (conn == conn->m->client)
        return MOQ_ERR_WRONG_STATE;   /* the client is not reaped per-child */
    if (conn->app_terminal_acked)
        return MOQ_OK;                 /* idempotent within this callback */
    if (!mm_conn_terminal_observed(conn))
        return MOQ_ERR_WRONG_STATE;    /* queued is not observed */
    /* Same rule as the terminal latch: the flag and the aggregate move in one
     * m->mu section, so no observer sees an acknowledged child still counted. */
    pthread_mutex_lock(&conn->m->mu);
    conn->app_terminal_acked = true;
#if defined(MOQ_WTQ_MM_TESTING)
    conn->m->st_ack_accepted++;
#endif
    /* Withdraw only THIS child's hold, and only if it published one. An
     * acknowledgment that arrives before the transport terminal has none to
     * withdraw; the later terminal then sees the child already acknowledged and
     * publishes nothing. */
#if defined(MOQ_WTQ_MM_TESTING)
    if (conn->terminal_hold_counted) {
        conn->terminal_hold_counted = false;
        if (conn->m->terminal_held_count > 0)
            conn->m->terminal_held_count--;
    }
#endif
    pthread_mutex_unlock(&conn->m->mu);
    return MOQ_OK;
}

#if defined(MOQ_WTQ_MM_TESTING)
void moq_wtquic_msquic_managed_test_mark_logical_terminal(
    moq_wtquic_msquic_managed_t *m, void *conn)
{
    (void)m;
    if (conn != NULL)
        mm_mark_logical_terminal((moq_wtquic_msquic_managed_conn_t *)conn);
}

void moq_wtquic_msquic_managed_test_mark_terminal_observed(
    moq_wtquic_msquic_managed_t *m, void *conn)
{
    (void)m;
    if (conn != NULL)
        ((moq_wtquic_msquic_managed_conn_t *)conn)->test_terminal_observed = true;
}

bool moq_wtquic_msquic_managed_test_conn_terminal_observed(
    const moq_wtquic_msquic_managed_conn_t *conn)
{
    return mm_conn_terminal_observed(conn);
}

bool moq_wtquic_msquic_managed_test_conn_acked(
    const moq_wtquic_msquic_managed_conn_t *conn)
{
    return conn != NULL && conn->app_terminal_acked;
}

void moq_wtquic_msquic_managed_test_terminal_counters(
    moq_wtquic_msquic_managed_t *m,
    uint64_t *out_transport_terminal_session_backed, uint64_t *out_ack_accepted,
    uint64_t *out_children_reaped, uint64_t *out_accepts_refused_terminal_held,
    uint32_t *out_terminal_held_now)
{
    uint64_t tt = 0, ack = 0, reap = 0, refused = 0;
    uint32_t held = 0;
    if (m != NULL) {
        /* every one of these is owned by mu: take it, exactly as the
         * admission path and the lane transitions do */
        pthread_mutex_lock(&m->mu);
        tt = m->st_transport_terminal_session_backed;
        ack = m->st_ack_accepted;
        reap = m->st_children_reaped;
        refused = m->st_accepts_refused_terminal_held;
        held = m->terminal_held_count;
        pthread_mutex_unlock(&m->mu);
    }
    if (out_transport_terminal_session_backed)
        *out_transport_terminal_session_backed = tt;
    if (out_ack_accepted) *out_ack_accepted = ack;
    if (out_children_reaped) *out_children_reaped = reap;
    if (out_accepts_refused_terminal_held) *out_accepts_refused_terminal_held = refused;
    if (out_terminal_held_now) *out_terminal_held_now = held;
}
#endif /* MOQ_WTQ_MM_TESTING */

void moq_wtquic_msquic_managed_conn_close(
    moq_wtquic_msquic_managed_conn_t *conn, uint32_t code)
{
    /* Pump-window-only. Latch the close rather than acting now: wtquic delivers
     * on_closed SYNCHRONOUSLY, so mm_pump_lane runs the actual wtq_session_close
     * AFTER the callback returns — keeping the contract that the connection's
     * final events arrive on a LATER pump, not this one. */
    if (!mm_conn_in_window(conn))
        return;
    conn->close_pending = true;
    conn->close_pending_code = code;
}

/* --- facade-wide observation (safe from any thread) ----------------------- */

size_t moq_wtquic_msquic_managed_conn_count(
    const moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return 0;
    moq_wtquic_msquic_managed_t *mm = (moq_wtquic_msquic_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    size_t n = m->conn_count;
    pthread_mutex_unlock(&mm->mu);
    return n;
}

void moq_wtquic_msquic_managed_drain(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return;
    pthread_mutex_lock(&m->mu);
    m->drain_requested = true;
    m->refuse_admissions = true; /* no new admissions once draining */
    pthread_mutex_unlock(&m->mu);
}

uint16_t moq_wtquic_msquic_managed_port(const moq_wtquic_msquic_managed_t *m)
{
    return m == NULL ? 0 : m->bound_port;
}

moq_result_t moq_wtquic_msquic_managed_wake(moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    /* Coalesced cross-thread wake of EVERY lane. Same stop-race contract as
     * lane_wake. */
    pthread_mutex_lock(&m->mu);
    bool closed = m->stop_requested;
    pthread_mutex_unlock(&m->mu);
    if (closed)
        return MOQ_ERR_CLOSED;
    for (uint32_t i = 0; i < m->lane_count; i++)
        mm_lane_arm(&m->lanes[i]);
    return MOQ_OK;
}

moq_result_t moq_wtquic_msquic_managed_wait(moq_wtquic_msquic_managed_t *m,
                                            uint64_t timeout_us)
{
    if (m == NULL)
        return MOQ_ERR_INVAL;
    if (mm_in_callback(m))
        return MOQ_ERR_WRONG_STATE;
    struct timespec ts;
    bool timed = timeout_us != UINT64_MAX;
    if (timed)
        mm_abstime(&ts, timeout_us);
    /* Resolves to MOQ_OK on a pending activity latch (client establishment, or
     * any doorbell pump — transport-triggered or an explicit wake; consumed
     * level-retained), MOQ_ERR_CLOSED on a terminal state (client fatal/closed
     * or a completed stop), or MOQ_DONE on timeout. */
    moq_result_t rc = MOQ_DONE;
    pthread_mutex_lock(&m->mu);
    for (;;) {
        /* A terminal state OUTRANKS a pending activity latch: once stopped/
         * fatal/closed, wait resolves CLOSED even if a pump (e.g. teardown's
         * final pump) left activity latched. Otherwise a pump's activity
         * resolves OK (level-retained, consumed once). */
        if (m->stopped || m->fatal || m->closed) {
            rc = MOQ_ERR_CLOSED;
            break;
        }
        if (m->activity_pending) {
            m->activity_pending = false;
            rc = MOQ_OK;
            break;
        }
        if (!timed) {
            pthread_cond_wait(&m->cv, &m->mu);
            continue;
        }
        if (timeout_us == 0 ||
            pthread_cond_timedwait(&m->cv, &m->mu, &ts) == ETIMEDOUT) {
            /* re-evaluate: activity/terminal may have raced the timeout */
            if (m->stopped || m->fatal || m->closed) {
                rc = MOQ_ERR_CLOSED;
            } else if (m->activity_pending) {
                m->activity_pending = false;
                rc = MOQ_OK;
            } else {
                rc = MOQ_DONE;
            }
            break;
        }
    }
    pthread_mutex_unlock(&m->mu);
    return rc;
}

bool moq_wtquic_msquic_managed_is_fatal(const moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return false;
    moq_wtquic_msquic_managed_t *mm = (moq_wtquic_msquic_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    bool v = m->fatal;
    pthread_mutex_unlock(&mm->mu);
    return v;
}

uint64_t moq_wtquic_msquic_managed_fatal_code(
    const moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return 0;
    moq_wtquic_msquic_managed_t *mm = (moq_wtquic_msquic_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    uint64_t v = m->fatal_code;
    pthread_mutex_unlock(&mm->mu);
    return v;
}

bool moq_wtquic_msquic_managed_is_closed(const moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return false;
    moq_wtquic_msquic_managed_t *mm = (moq_wtquic_msquic_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    bool v = m->closed;
    pthread_mutex_unlock(&mm->mu);
    return v;
}

uint64_t moq_wtquic_msquic_managed_close_code(
    const moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return 0;
    moq_wtquic_msquic_managed_t *mm = (moq_wtquic_msquic_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    uint64_t v = m->close_code;
    pthread_mutex_unlock(&mm->mu);
    return v;
}

moq_version_t moq_wtquic_msquic_managed_negotiated_version(
    const moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL)
        return 0;
    /* written under mu on the transport thread at establishment; read under it
     * so an any-thread caller never races the store */
    moq_wtquic_msquic_managed_t *mm = (moq_wtquic_msquic_managed_t *)m;
    pthread_mutex_lock(&mm->mu);
    moq_version_t v = m->negotiated_version;
    pthread_mutex_unlock(&mm->mu);
    return v;
}

/* --- private test inspection (test builds only, never installed/exported) -- */
#if defined(MOQ_WTQ_MM_TESTING)
const char *moq_wtquic_msquic_managed_test_host(
    const moq_wtquic_msquic_managed_t *m)
{
    return m == NULL ? NULL : m->host;
}
size_t moq_wtquic_msquic_managed_test_proto_count(
    const moq_wtquic_msquic_managed_t *m)
{
    return m == NULL ? 0 : m->proto_count;
}
const char *moq_wtquic_msquic_managed_test_proto(
    const moq_wtquic_msquic_managed_t *m, size_t i)
{
    return (m != NULL && i < m->proto_count) ? m->protos[i] : NULL;
}
uint32_t moq_wtquic_msquic_managed_test_lane_count(
    const moq_wtquic_msquic_managed_t *m)
{
    return m == NULL ? 0 : m->lane_count;
}

/* Drive the pure WT-Protocol negotiation directly (token validation / offered
 * enforcement / legacy fallback) without a transport. */
bool moq_wtquic_msquic_managed_test_negotiate(
    const moq_wtquic_msquic_managed_t *m, const char *tok, size_t len,
    moq_version_t *out)
{
    wtq_str_t s = { .data = tok, .len = len };
    return mm_negotiate(m, s, out);
}

/* Drive the first-cause terminal latches directly (read back with the public
 * is_fatal / fatal_code / is_closed / close_code accessors). */
void moq_wtquic_msquic_managed_test_latch_fatal(
    moq_wtquic_msquic_managed_t *m, uint64_t code)
{
    mm_latch_fatal(m, code);
}
void moq_wtquic_msquic_managed_test_latch_closed(
    moq_wtquic_msquic_managed_t *m, uint64_t code)
{
    mm_latch_closed(m, code);
}

/* Raise the level-retained activity latch that establishment sets, so the
 * wait() state machine can be driven without a transport. */
void moq_wtquic_msquic_managed_test_set_activity(moq_wtquic_msquic_managed_t *m)
{
    pthread_mutex_lock(&m->mu);
    m->activity_pending = true;
    pthread_cond_broadcast(&m->cv);
    pthread_mutex_unlock(&m->mu);
}

/* Drive server admission directly: accept returns true + the admitted child in
 * *out_conn (NULL when refused / NOMEM); abandon rolls one back. */
bool moq_wtquic_msquic_managed_test_accept(moq_wtquic_msquic_managed_t *m,
                                           void **out_conn)
{
    wtq_msquic_accept_info_t info;
    memset(&info, 0, sizeof(info));
    info.struct_size = (uint32_t)sizeof(info);
    wtq_msquic_accept_decision_t dec;
    memset(&dec, 0, sizeof(dec));
    wtq_result_t r = mm_accept_prepare(m, &info, &dec);
    bool ok = (r == WTQ_OK && dec.accepted);
    if (out_conn != NULL)
        *out_conn = ok ? dec.user : NULL;
    return ok;
}
void moq_wtquic_msquic_managed_test_abandon(moq_wtquic_msquic_managed_t *m,
                                            void *conn)
{
    mm_accept_abandon(m, conn);
}

/* The lane index a child was placed on (avoids the pump-confined public
 * conn_lane accessor from outside a pump). */
uint32_t moq_wtquic_msquic_managed_test_conn_lane_index(void *conn)
{
    moq_wtquic_msquic_managed_conn_t *c = conn;
    return (c == NULL || c->lane == NULL) ? UINT32_MAX : c->lane->index;
}

/* The client's published wtquic session handle (NULL before establish/publish
 * or after reap). */
void *moq_wtquic_msquic_managed_test_client_ws(
    const moq_wtquic_msquic_managed_t *m)
{
    return (m == NULL || m->client == NULL) ? NULL : m->client->ws;
}

/* Observe the first established server child under its owning lane guard: the
 * version/adapter/ws it read there were written by on_established under the same
 * guard, so a concurrent callback cannot tear them out mid-read. Returns true
 * only when a child has an established MoQ session (session != NULL). */
bool moq_wtquic_msquic_managed_test_server_child(
    const moq_wtquic_msquic_managed_t *m, moq_version_t *out_version,
    bool *out_has_adapter, bool *out_has_ws)
{
    if (m == NULL || m->children == NULL)
        return false;
    for (uint32_t i = 0; i < m->max_connections; i++) {
        moq_wtquic_msquic_managed_conn_t *c = &m->children[i];
        if (!c->in_use || c->lane == NULL)
            continue;
        mm_guard_enter(c->lane);
        bool established = (c->session != NULL);
        if (established) {
            if (out_version != NULL)
                *out_version = c->version;
            if (out_has_adapter != NULL)
                *out_has_adapter = (c->adapter != NULL);
            if (out_has_ws != NULL)
                *out_has_ws = (c->ws != NULL);
        }
        mm_guard_leave(c->lane);
        if (established)
            return true;
    }
    return false;
}

/* Drive an admitted child through on_established with the given token. A
 * sentinel session stands in for the transport: an unrecognized token fails
 * negotiation before any real session is used (child-local failure branch); a
 * recognized token is only safe once quiescence has been requested, which
 * converges before the session is built. The guarded close (release seam) keeps
 * the sentinel untouched. */
void moq_wtquic_msquic_managed_test_deliver_established(
    moq_wtquic_msquic_managed_t *m, void *conn, const char *tok)
{
    (void)m;
    wtq_str_t sub = { tok, tok != NULL ? strlen(tok) : 0 };
    mm_on_established((wtq_session_t *)0x1, sub, conn);
}

/* Request quiescence of one admitted child, exactly as the teardown scan does. */
void moq_wtquic_msquic_managed_test_quiesce_conn(
    moq_wtquic_msquic_managed_t *m, void *conn)
{
    (void)m;
    mm_quiesce_conn(conn);
}

/* Run one explicit (non-sweep) pump cycle for a lane, as teardown's final pump. */
int moq_wtquic_msquic_managed_test_pump_lane(
    moq_wtquic_msquic_managed_t *m, uint32_t lane_index)
{
    if (m == NULL || lane_index >= m->lane_count)
        return 0;
    return mm_pump_lane(m, &m->lanes[lane_index], false);
}

uint64_t moq_wtquic_msquic_managed_test_lane_deadline_us(
    moq_wtquic_msquic_managed_t *m, uint32_t lane_index)
{
    if (m == NULL || lane_index >= m->lane_count)
        return UINT64_MAX;
    moq_wtquic_msquic_managed_lane_t *lane = &m->lanes[lane_index];
    pthread_mutex_lock(&lane->bell_mu);
    uint64_t d = lane->deadline_us;
    pthread_mutex_unlock(&lane->bell_mu);
    return d;
}

/* Force a lane's next deadline to duration_us from now (monotonic) and wake the
 * worker, so a deadline sweep can be driven without a real session. The next
 * pump resets the deadline (no real sessions), so this yields exactly one. */
void moq_wtquic_msquic_managed_test_set_deadline(void *lane, uint64_t duration_us)
{
    moq_wtquic_msquic_managed_lane_t *l = lane;
    if (l == NULL)
        return;
    pthread_mutex_lock(&l->bell_mu);
    l->deadline_us = mm_now_us() + duration_us;
    pthread_cond_signal(&l->bell_cv);
    pthread_mutex_unlock(&l->bell_mu);
}

/* Mark a child's transport quiesced, exactly as on_transport_quiesced. */
void moq_wtquic_msquic_managed_test_mark_quiesced(
    moq_wtquic_msquic_managed_t *m, void *conn)
{
    (void)m;
    mm_on_quiesced(NULL, conn);
}

/* Read a child's reap-gate facts: the two transport facts plus whether it is
 * session-backed (which selects the session-backed or pre-session contract).
 * Observation and acknowledgment have their own accessors. */
void moq_wtquic_msquic_managed_test_conn_gate(
    void *conn, bool *out_logical_terminal, bool *out_transport_quiesced,
    bool *out_session_backed)
{
    const moq_wtquic_msquic_managed_conn_t *c = conn;
    if (out_logical_terminal != NULL)
        *out_logical_terminal = c != NULL && c->logical_terminal;
    if (out_transport_quiesced != NULL)
        *out_transport_quiesced = c != NULL && c->transport_quiesced;
    if (out_session_backed != NULL)
        *out_session_backed = c != NULL && c->session_backed;
}

/* Whether a child has a close latched but not yet executed by a pump. */
bool moq_wtquic_msquic_managed_test_conn_close_pending(void *conn)
{
    const moq_wtquic_msquic_managed_conn_t *c = conn;
    return c != NULL && c->close_pending;
}

/* Whether a child slot is currently reserved. */
bool moq_wtquic_msquic_managed_test_conn_in_use(void *conn)
{
    const moq_wtquic_msquic_managed_conn_t *c = conn;
    return c != NULL && c->in_use;
}

/* Seed a child's per-generation state + a retained wtquic ref. */
void moq_wtquic_msquic_managed_test_seed_conn(
    void *conn, wtq_session_t *ws, moq_version_t version, void *user,
    bool quiesce_requested, bool close_pending)
{
    moq_wtquic_msquic_managed_conn_t *c = conn;
    if (c == NULL)
        return;
    c->ws = ws;
    c->owns_ws_ref = (ws != NULL);
    c->version = version;
    c->user = user;
    c->quiesce_requested = quiesce_requested;
    c->close_pending = close_pending;
}

/* Read a child's per-generation state directly. */
void moq_wtquic_msquic_managed_test_conn_gen_state(
    void *conn, moq_version_t *out_version, void **out_user,
    bool *out_quiesce_requested, bool *out_close_pending)
{
    const moq_wtquic_msquic_managed_conn_t *c = conn;
    if (out_version != NULL)
        *out_version = c != NULL ? c->version : 0;
    if (out_user != NULL)
        *out_user = c != NULL ? c->user : NULL;
    if (out_quiesce_requested != NULL)
        *out_quiesce_requested = c != NULL && c->quiesce_requested;
    if (out_close_pending != NULL)
        *out_close_pending = c != NULL && c->close_pending;
}

/* True while a pump window is open on this thread. */
bool moq_wtquic_msquic_managed_test_in_pump_window(void)
{
    return mm_pump_win != NULL;
}

/* Force a child's established handles to the given sentinels. */
void moq_wtquic_msquic_managed_test_set_conn_established(
    void *conn, wtq_session_t *ws, void *session)
{
    moq_wtquic_msquic_managed_conn_t *c = conn;
    if (c == NULL)
        return;
    c->ws = ws;
    c->session = session;
    c->session_backed = (session != NULL);
}

/* A connection's published wtquic handle, read from the connection directly
 * (usable from a connect-seam probe before create() returns the facade). */
void *moq_wtquic_msquic_managed_test_conn_ws(
    const moq_wtquic_msquic_managed_conn_t *conn)
{
    return conn == NULL ? NULL : conn->ws;
}

/* Deliver on_draining to the client with a forced non-NULL adapter, so the
 * forwarder runs against the real attach table (where on_draining is NULL).
 * Exercises the NULL-callback guard: it must not dereference the NULL member.
 * The sentinel adapter is only compared, never dereferenced. */
void moq_wtquic_msquic_managed_test_deliver_draining(
    moq_wtquic_msquic_managed_t *m)
{
    if (m == NULL || m->client == NULL)
        return;
    moq_wtquic_conn_t *saved = m->client->adapter;
    m->client->adapter = (moq_wtquic_conn_t *)0x1;
    mm_on_draining(NULL, m->client);
    m->client->adapter = saved;
}

/* Build the env config the client would open with (allocator bridge + tuning),
 * so a test can assert the forwarded idle timeout and bridged allocator. */
void moq_wtquic_msquic_managed_test_fill_env_cfg(
    moq_wtquic_msquic_managed_t *m, wtq_msquic_env_cfg_t *out)
{
    mm_fill_env_cfg(m, out);
}
#endif
