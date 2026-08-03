#ifndef WTQUIC_CONTRACT_PAIR_H
#define WTQUIC_CONTRACT_PAIR_H

/*
 * Production-contract conformance pair for the wtquic adapter.
 *
 * This is NOT the shared deterministic conformance rail
 * (tests/conformance/moq_adapter_pair.h): that interface hands
 * scenarios raw moq_session_t* to drive from the caller thread between
 * pumps, which fits sans-I/O/simulated adapters but violates the
 * wtquic adapter's confinement contract — MoQ session mutation belongs
 * on the wtquic transport worker, inside the adapter hook.
 *
 * This rail tests the adapter in exactly the mode applications run it:
 *   - real MsQuic loopback, real threads, real TLS;
 *   - every moq_session_* call happens inside the adapter hook on the
 *     transport worker, driven by a per-side SCRIPT of
 *     when-this-event -> do-this-action steps loaded before the client
 *     ever connects (each step is triggered by an event, and actions
 *     generate the traffic that triggers subsequent steps, so no
 *     external wakeups are needed);
 *   - the main thread only loads scripts, waits on condition-variable
 *     progress signals from the hook, and asserts recorded
 *     observations after teardown.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include <moq/wtquic.h>

#include <wtquic/wtquic_msquic.h>

/* Script step triggers, evaluated in the adapter hook after each event
 * batch. A step fires once, in order. */
typedef enum wtqc_when {
    WTQC_ON_SETUP = 1,     /* MOQ_EVENT_SETUP_COMPLETE seen */
    WTQC_ON_SUB_REQUEST,   /* SUBSCRIBE_REQUEST recorded */
    WTQC_ON_SUB_OK,        /* SUBSCRIBE_OK seen */
    WTQC_ON_OBJECTS,       /* objects received >= arg */
    WTQC_ON_OBJECTS_G1,    /* objects received in group 1 >= arg */
    WTQC_ON_CLOSED,        /* adapter reports closed or fatal */
} wtqc_when_t;

typedef enum wtqc_do {
    WTQC_DO_NOTHING = 0,
    WTQC_DO_SUBSCRIBE,        /* fixed test namespace/track */
    WTQC_DO_ACCEPT,           /* accept the recorded subscription */
    WTQC_DO_PUBLISH,          /* a1 objects of a2 bytes, one subgroup,
                                 finished with FIN */
    WTQC_DO_PUBLISH_RESET,    /* one object of a2 bytes, then
                                 reset_subgroup with code a1 */
    WTQC_DO_WT_CLOSE,         /* clean WebTransport close, code a1 */
    WTQC_DO_PROBE_DEAD,       /* moq_session_subscribe attempt on the
                                 (expected-dead) session; rc recorded */
} wtqc_do_t;

typedef struct wtqc_step {
    wtqc_when_t when;
    uint64_t when_arg;
    wtqc_do_t act;
    uint64_t a1;
    uint64_t a2;
} wtqc_step_t;

#define WTQC_MAX_STEPS 8

/* Everything a scenario can assert on, recorded by the hook. Read from
 * the main thread only after wtqc_pair_teardown (the quiescence
 * barrier) — mid-run WAITING goes through wtqc_wait_*. */
typedef struct wtqc_obs {
    int setup;             /* SETUP_COMPLETE events */
    int sub_request;
    int sub_ok;
    int session_closed;    /* MOQ_EVENT_SESSION_CLOSED events */
    int objects;
    int objects_g1;        /* of which: group id 1 */
    uint64_t object_bytes;
    int object_errors;     /* payload length/content mismatches */
    int publish_errors;    /* publish-side call failures */
    bool closed;           /* adapter terminal, clean transport close */
    bool fatal;            /* adapter terminal, fatal */
    int probe_rc;          /* WTQC_DO_PROBE_DEAD result */
    bool probe_ran;
} wtqc_obs_t;

typedef struct wtqc_side {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool sync_init;        /* mu/cv created (partial-teardown guard) */
    wtqc_obs_t obs;

    /* script (loaded before connect; hook-private cursor) */
    wtqc_step_t steps[WTQC_MAX_STEPS];
    size_t step_count;
    size_t cursor;

    /* hook-private state */
    moq_wtquic_conn_t *conn;
    moq_subscription_t sub;      /* recorded SUBSCRIBE_REQUEST */
    bool have_sub;
    uint64_t next_group;         /* one group per publish action */
    uint64_t expect_objects;     /* payload-verify object count hint */
    uint64_t expect_size;
} wtqc_side_t;

typedef struct wtqc_pair {
    wtqc_side_t client;
    wtqc_side_t server;
    moq_session_t *client_ms;
    moq_session_t *server_ms;
    wtq_msquic_env_t *env;
    wtq_msquic_listener_t *listener;
    wtq_session_t *cs;
} wtqc_pair_t;

typedef struct wtqc_pair_cfg {
    const char *cert;
    const char *key;
    const char *client_offer;   /* WT subprotocol offered (required) */
    const char *server_serve;   /* WT subprotocol served (required) */
} wtqc_pair_cfg_t;

/* Bring the pair up (scripts must already be loaded into the sides).
 * Returns 0 on success. */
int wtqc_pair_setup(wtqc_pair_t *p, const wtqc_pair_cfg_t *cfg);

/* Tear everything down; blocks until the transport is quiescent, after
 * which obs may be read directly. Safe on a partially set-up pair. */
void wtqc_pair_teardown(wtqc_pair_t *p);

/* Wait (bounded) until an int observation reaches at least `least`, or
 * a bool observation becomes true. Return false on timeout. */
bool wtqc_wait_count(wtqc_side_t *sd, const int *field, int least);
bool wtqc_wait_flag(wtqc_side_t *sd, const bool *field);

/* The deterministic object payload (index-tagged), shared by publish
 * and verify. */
void wtqc_payload_fill(uint8_t *dst, uint64_t size, uint64_t idx);

#endif /* WTQUIC_CONTRACT_PAIR_H */
