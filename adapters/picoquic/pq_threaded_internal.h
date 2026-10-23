#ifndef MOQ_PQ_THREADED_INTERNAL_H
#define MOQ_PQ_THREADED_INTERNAL_H

/*
 * The threaded facade's private structures, shared by moq_picoquic_threaded.c
 * and the lane group (pq_lane_group.c). PRIVATE to adapters/picoquic.
 */

#include <moq/picoquic_threaded.h>
#include <picoquic_packet_loop.h>

#include "../common/moq_setup_tokens.h"
#include "picoquic_endpoint.h"
#include "pq_wake_sched.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

/* Advertised inbound flow-control credit, per unidirectional stream and
 * connection-wide. Sized to the session's default receive budget so a large
 * inbound MoQ object is not held back by too small a window; see create. */
#define MOQ_PQ_RECV_FLOW_CONTROL (16u * 1024u * 1024u)

/* Default server connection cap when cfg.max_connections is 0. Mirrors the
 * mvfst managed server default. */
#define MOQ_PQ_THREADED_DEFAULT_MAX_CONNECTIONS 1024u
#ifndef MOQ_PQ_THREADED_LOCAL_CLOSE_FLUSH_US
#define MOQ_PQ_THREADED_LOCAL_CLOSE_FLUSH_US 250000ull
#endif

/* The wait, in microseconds, until an absolute deadline at a given now: never
 * negative and never past INT64_MAX. An unbounded delta cast straight to
 * int64_t would wrap negative and busy-spin the loop this value bounds. Shared
 * by the facade loop and the group lane loop so both bound the wait the same. */
static inline int64_t deadline_until_us(uint64_t deadline_us, uint64_t now_us)
{
    if (deadline_us <= now_us) return 0;
    uint64_t delta = deadline_us - now_us;
    return delta > (uint64_t)INT64_MAX ? INT64_MAX : (int64_t)delta;
}

/* One accepted server connection. Heap-owned so the public opaque handle
 * (moq_pq_threaded_conn_t*) stays stable while the internal list grows/compacts.
 * All fields are touched only on the network thread except where noted. */
struct moq_pq_threaded_conn {
    struct moq_pq_threaded *parent;    /* owning server (for thread-guard) */
    picoquic_cnx_t *cnx;
    moq_session_t  *session;
    moq_pq_conn_t  *conn;
    moq_version_t   negotiated;
    bool            close_requested;   /* conn_close: prune after on_lane_pump */
    bool            dead_observed;     /* terminal AND an app pump has run
                                          since (SESSION_CLOSED pollable);
                                          prune precondition for non-app
                                          closes */
    uint64_t        close_error_code;
    bool            fatal;             /* per-conn service failure: prune */
    void           *user;              /* application slot; never read */
    /* cfg.retain_terminal_until_ack: the transport is retired (closed,
     * released, the session told) but the record and session are kept until
     * the application acknowledges; then acked, and freed after the pump. */
    bool            retired;
    bool            acked;
#ifdef MOQ_PQ_THREADED_TESTING
    bool            test_service_fatal; /* seam: next service pass fatals */
#endif
};

/* The single lane of a managed facade. picoquic_threaded runs one network
 * thread, so a facade has exactly one lane; it just points back at its
 * owner (the pump/iteration operate on the owner's one connection set). */
struct moq_pq_threaded_lane {
    struct moq_pq_threaded *owner;
    uint32_t index;
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
    uint32_t            keep_alive_interval_ms;  /* 0 = keepalive disabled */
    uint64_t            send_queue_cap_bytes;    /* 0 = adapter default */
    bool                insecure_skip_verify;
    int               (*configure_quic_fn)(picoquic_quic_t *, void *);
    void               *configure_quic_ctx;
    int               (*on_lane_pump)(moq_pq_threaded_t *,
                                       moq_pq_threaded_lane_t *,
                                       uint64_t, void *);
    void               *on_lane_pump_ctx;
    uint64_t          (*app_deadline_us)(void *);   /* optional service deadline */
    void               *app_deadline_ctx;
    /* optional per-lane service deadline; this facade's lane is `lane.index`
     * (0 for the one-lane facade), folded alongside app_deadline_us */
    uint64_t          (*app_lane_deadline_us)(void *, uint32_t lane_index);
    void               *app_lane_deadline_ctx;
    uint64_t            deadline_served;   /* app deadline instant last woken for */
    void              (*on_activity)(moq_pq_threaded_t *, void *);
    void               *on_activity_ctx;

    /* the facade's single lane (owner set in create) */
    struct moq_pq_threaded_lane lane;
    /* True ONLY while on_lane_pump is executing (set around each of the
     * three call sites, cleared before on_activity / service). The
     * per-connection API is valid only in this window; the flag is
     * written and read solely on the network thread, so a plain bool
     * needs no lock (the accessors read it only after confirming they are
     * on the network thread). */
    bool                in_lane_pump;

    /* Version negotiation (owned copies of the cfg ALPN offer list, in
     * preference order). Empty list = legacy single-ALPN behavior
     * (MOQ_PQ_ALPN_DEFAULT, eager client session, draft-16). */
    char              **alpn_list;
    size_t              alpn_count;
    size_t              alpn_array_bytes;
    bool                client_deferred;  /* client session awaits the
                                           * negotiated-ALPN readback */
    /* The connection target each session is bound with when its profile
     * requires a binding: the client's authority and path/query (copied
     * into target_bytes) or the server's URI policy. */
    moq_setup_tokens_t  setup_tokens;   /* presented in each SETUP */
    moq_connection_target_t target;
    uint8_t            *target_bytes;
    size_t              target_bytes_len;

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
    bool                retain_terminal;  /* cfg.retain_terminal_until_ack */
    bool                streaming_objects; /* cfg.streaming_objects */
    uint32_t            socket_buffer_bytes; /* cfg.socket_buffer_bytes */
    struct sockaddr_storage bind_address;   /* cfg.bind_address; family 0 = any */
    bool                fatal;
    uint64_t            fatal_code;
    uint64_t            terminal_error;  /* local or peer transport error at a
                                          * client handshake/transport failure;
                                          * 0 otherwise. Classified by the
                                          * service endpoint (TLS vs transport). */
    bool                stopped;
    bool                pump_exit;
    bool                loop_running;   /* packet loop is active; stop waits
                                          * for the callback to clear it */
    bool                local_close_started;    /* network thread only */
    uint64_t            local_close_deadline_us; /* network thread only */
    /* The application error code the local close carries: 0 for a stop or a
     * pump exit, the recorded fatal code for a client service fatal. */
    uint64_t            local_close_code;       /* network thread only */
    bool                fatal_close;            /* network thread only */
#ifdef MOQ_PQ_THREADED_TESTING
    /* Test-only pass stages: called on the network thread with no facade
     * lock held, at (1) after the pass's pump returns, before the post-pump
     * service, and (2) after the drain snapshot is published. */
    void              (*test_stage)(struct moq_pq_threaded *t, int stage,
                                    void *ctx);
    void               *test_stage_ctx;
    uint64_t            test_pass_gen;  /* the current pass's pass_gen */
#endif
    bool                tx_drained;     /* network thread: the stream output
                                         * is flushed and acknowledged, as of a
                                         * pass no wake overtook. Read by
                                         * drain_state. */
    uint64_t            drain_gen;      /* advanced by every wake (mutex): a
                                         * pass publishes tx_drained only if
                                         * none arrived since it began */
    bool                loop_exited;    /* loop acknowledged stop request */
    bool                loop_came_up;   /* packet loop reached its ready
                                           callback (latched; a later loop
                                           exit does not clear it) */
    bool                setup_reached;  /* client: session hit ESTABLISHED */
    moq_version_t       negotiated;     /* 0 until the version is known */
    bool                activity_pending;
    bool                wake_pending;
    /* Wakes and picoquic's wake pipe (mutex; see pq_wake_sched.h). */
    pq_wake_sched_t     sched;
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
    picoquic_path_quality_t st_quality_last; /* last live path-quality snapshot */
    /* Per-connection stats accumulated from conns freed before stop, so a pruned
     * connection's prepare/queue counts are not lost. Final = acc + live conns. */
    uint64_t            st_acc_prepare;
    uint64_t            st_acc_provided;
    uint64_t            st_acc_would_block;
    uint64_t            st_acc_high_water;  /* max, not sum */

    /* Lane group (cfg.lane_count > 1, server): this facade is either the
     * group -- `group_ctx` set, no context or thread of its own, its public
     * calls dispatched over the lanes -- or one of the group's lanes -- an
     * internal facade whose `group` points at it. A lane runs the group's
     * loop on its own thread, not picoquic's packet loop: `own_loop` is set,
     * `thread_ctx` stays NULL, and a wake writes `wake_fd`, the loop's pipe.
     * The application's hooks are handed the group facade as `t`. */
    struct moq_pq_threaded *group;
    struct pq_lane_group   *group_ctx;
    bool                    own_loop;
    int                     wake_fd;
};


#if defined(__GNUC__) || defined(__clang__)
/* The lane group's internals are not part of the adapter's ABI. */
#  pragma GCC visibility push(hidden)
#endif
/* The loop callback the facade hands picoquic's packet loop; a lane group
 * drives it from its own loop in the same callback order. */
int moq_pq_threaded_loop_callback(picoquic_quic_t *quic,
    picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg);
#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC visibility pop
#endif

/* The facade whose hooks see this one as `t`: the group for a lane. */
static inline moq_pq_threaded_t *pq_threaded_pump_self(moq_pq_threaded_t *t)
{
    return t->group ? t->group : t;
}

#endif /* MOQ_PQ_THREADED_INTERNAL_H */
