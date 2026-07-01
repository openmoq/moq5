#ifndef MOQ_SIM_H
#define MOQ_SIM_H

/*
 * Deterministic simulation helpers for libmoq.
 *
 * This API is intentionally separate from moq/moq.h. Production embedders
 * can link only moq-core; tests, conformance harnesses, and tools can link
 * moq-sim for reproducible back-to-back sessions.
 */

#include <moq/moq.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_simpair moq_simpair_t;

typedef enum moq_sim_trace_kind {
    MOQ_SIM_TRACE_INPUT      = 1,
    MOQ_SIM_TRACE_ACTION     = 2,
    MOQ_SIM_TRACE_QUIESCENT  = 3,
    MOQ_SIM_TRACE_FAULT_DROP   = 4,
    MOQ_SIM_TRACE_FAULT_MUTATE  = 5,
    MOQ_SIM_TRACE_FAULT_REORDER = 6,
    MOQ_SIM_TRACE_FAULT_INJECT    = 7,
    MOQ_SIM_TRACE_FAULT_TRUNCATE  = 8,
    MOQ_SIM_TRACE_DELAY_ENQUEUE   = 9,
    MOQ_SIM_TRACE_DELAY_STALE     = 10,
} moq_sim_trace_kind_t;

/* Transport fault flags for fault_flags config field. */
#define MOQ_SIM_FAULT_DROP_CONTROL  (1u << 0)
#define MOQ_SIM_FAULT_DROP_DATA     (1u << 1)
#define MOQ_SIM_FAULT_DROP_RESET    (1u << 2)
#define MOQ_SIM_FAULT_DROP_STOP     (1u << 3)
#define MOQ_SIM_FAULT_SPLIT_DATA      (1u << 4)
#define MOQ_SIM_FAULT_MUTATE_CONTROL   (1u << 5)
#define MOQ_SIM_FAULT_REORDER_ACTION  (1u << 6)
#define MOQ_SIM_FAULT_MUTATE_DATA     (1u << 7)
#define MOQ_SIM_FAULT_INJECT_RESET    (1u << 8)
#define MOQ_SIM_FAULT_INJECT_STOP     (1u << 9)
#define MOQ_SIM_FAULT_INJECT_CLOSE    (1u << 10)
#define MOQ_SIM_FAULT_TRUNCATE_CONTROL (1u << 11)
#define MOQ_SIM_FAULT_TRUNCATE_DATA    (1u << 12)
#define MOQ_SIM_FAULT_SPLIT_CONTROL    (1u << 13)
#define MOQ_SIM_FAULT_SPLIT_BIDI       (1u << 14)
#define MOQ_SIM_FAULT_DELAY            (1u << 15)
#define MOQ_SIM_FAULT_ALL           (MOQ_SIM_FAULT_DROP_CONTROL    | \
                                     MOQ_SIM_FAULT_DROP_DATA       | \
                                     MOQ_SIM_FAULT_DROP_RESET      | \
                                     MOQ_SIM_FAULT_DROP_STOP       | \
                                     MOQ_SIM_FAULT_SPLIT_DATA      | \
                                     MOQ_SIM_FAULT_MUTATE_CONTROL  | \
                                     MOQ_SIM_FAULT_REORDER_ACTION  | \
                                     MOQ_SIM_FAULT_MUTATE_DATA     | \
                                     MOQ_SIM_FAULT_TRUNCATE_CONTROL | \
                                     MOQ_SIM_FAULT_TRUNCATE_DATA   | \
                                     MOQ_SIM_FAULT_SPLIT_CONTROL   | \
                                     MOQ_SIM_FAULT_SPLIT_BIDI)
#define MOQ_SIM_FAULT_ALL_DELAY     MOQ_SIM_FAULT_DELAY
#define MOQ_SIM_FAULT_ALL_INJECT    (MOQ_SIM_FAULT_INJECT_RESET | \
                                     MOQ_SIM_FAULT_INJECT_STOP  | \
                                     MOQ_SIM_FAULT_INJECT_CLOSE)

typedef enum moq_sim_trace_input_kind {
    MOQ_SIM_INPUT_START         = 1,
    MOQ_SIM_INPUT_CONTROL_BYTES = 2,
    MOQ_SIM_INPUT_TICK          = 3,
    MOQ_SIM_INPUT_DATA_BYTES    = 4,
    MOQ_SIM_INPUT_DATA_RESET    = 5,
    MOQ_SIM_INPUT_DATA_STOP     = 6,
    MOQ_SIM_INPUT_BIDI_BYTES    = 7,
    MOQ_SIM_INPUT_BIDI_RESET    = 8,
    MOQ_SIM_INPUT_DATAGRAM      = 9,
    MOQ_SIM_INPUT_BIDI_STOP     = 10,
} moq_sim_trace_input_kind_t;

typedef struct moq_sim_trace_record {
    uint32_t                    struct_size;
    moq_sim_trace_kind_t        kind;
    uint64_t                    seed;
    uint64_t                    step;
    uint64_t                    now_us;
    moq_perspective_t           from;
    moq_perspective_t           to;
    moq_action_kind_t           action_kind;
    moq_sim_trace_input_kind_t  input_kind;
    moq_bytes_t                 bytes;       /* borrowed until callback returns */
    uint64_t                    code;
    size_t                      count;
    moq_result_t                result;
} moq_sim_trace_record_t;

typedef void (*moq_sim_trace_fn)(void *ctx,
                                  const moq_sim_trace_record_t *record);

typedef struct moq_simpair_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;       /* required; copied */
    uint64_t           seed;        /* trace/replay identity; no faults yet */
    uint64_t           initial_now_us;

    bool               client_send_request_capacity;
    uint64_t           client_initial_request_capacity;
    bool               server_send_request_capacity;
    uint64_t           server_initial_request_capacity;

    moq_sim_trace_fn   trace_fn;    /* optional; must not reenter SimPair */
    void              *trace_ctx;

    uint32_t           fault_per_mille;  /* 0..1000; fault probability */
    uint32_t           fault_flags;      /* bitmask of MOQ_SIM_FAULT_* */

    bool               client_streaming_objects;  /* client session streaming_objects flag */

    uint64_t           client_goaway_timeout_us;  /* 0 = disabled */
    uint64_t           server_goaway_timeout_us;  /* 0 = disabled */

    uint32_t           max_actions;  /* 0 = default */
    uint32_t           max_events;   /* 0 = default */

    /* Protocol version for both sessions. 0 (absent) uses the session default. */
    moq_version_t      version;

    /* Server subscription pool size (0 = session default). */
    uint32_t           server_max_subscriptions;

    /* Client subscription pool size (0 = session default). Mirror of
     * server_max_subscriptions for the client session. */
    uint32_t           client_max_subscriptions;
} moq_simpair_cfg_t;

#ifdef __cplusplus
#define MOQ_SIMPAIR_CFG_INIT \
    (moq_simpair_cfg_t{ sizeof(moq_simpair_cfg_t), 0, 0, 0, false, 0, false, 0, 0, 0, 0, 0, false, 0, 0, 0, 0, (moq_version_t)0, 0, 0 })
#else
#define MOQ_SIMPAIR_CFG_INIT \
    ((moq_simpair_cfg_t){ .struct_size = sizeof(moq_simpair_cfg_t) })
#endif

MOQ_SIM_API moq_result_t moq_simpair_create(const moq_simpair_cfg_t *cfg,
                                         moq_simpair_t **out);
MOQ_SIM_API void moq_simpair_destroy(moq_simpair_t *sp);

MOQ_SIM_API moq_session_t *moq_simpair_client(moq_simpair_t *sp);
MOQ_SIM_API moq_session_t *moq_simpair_server(moq_simpair_t *sp);
MOQ_SIM_API uint64_t       moq_simpair_now_us(const moq_simpair_t *sp);
MOQ_SIM_API uint64_t       moq_simpair_seed(const moq_simpair_t *sp);

/*
 * Start the client side by initiating the setup handshake.
 */
MOQ_SIM_API moq_result_t moq_simpair_start(moq_simpair_t *sp);

/*
 * Pump one deterministic step:
 *   client actions -> server inputs, then server actions -> client inputs.
 *
 * out_delivered receives the number of actions handled when non-NULL.
 */
MOQ_SIM_API moq_result_t moq_simpair_step(moq_simpair_t *sp,
                                       size_t *out_delivered);

/*
 * Run steps until a step delivers no actions.
 * Returns MOQ_ERR_WOULD_BLOCK if max_steps is reached first.
 */
MOQ_SIM_API moq_result_t moq_simpair_run_until_quiescent(moq_simpair_t *sp,
                                                      size_t max_steps,
                                                      size_t *out_steps);

/*
 * Advance virtual time for both sessions. now_us must be monotonic.
 */
MOQ_SIM_API moq_result_t moq_simpair_advance_to(moq_simpair_t *sp,
                                             uint64_t now_us);

/*
 * Enable transport fault injection. Faults are configured at create
 * time but suppressed until this call so setup/handshake can complete.
 */
MOQ_SIM_API void moq_simpair_enable_faults(moq_simpair_t *sp);

/*
 * Returns the earliest deadline across delayed inputs and both session
 * timers. Returns UINT64_MAX when nothing is pending.
 */
MOQ_SIM_API uint64_t moq_simpair_next_deadline_us(const moq_simpair_t *sp);

/*
 * Returns the number of pending delayed input entries. Test helper.
 */
MOQ_SIM_API size_t moq_simpair_delayed_count(const moq_simpair_t *sp);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_SIM_H */
