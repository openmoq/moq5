#ifndef MSQUIC_CONTRACT_PAIR_H
#define MSQUIC_CONTRACT_PAIR_H

/*
 * Production-contract conformance pair for the direct MsQuic adapter.
 *
 * This is NOT the shared deterministic conformance rail
 * (tests/conformance/moq_adapter_pair.h): that interface hands
 * scenarios raw moq_session_t* to drive from the caller thread between
 * pumps, which fits sans-I/O/simulated adapters but violates the
 * managed MsQuic facade's confinement contract — every moq_session_*
 * call belongs inside on_lane_pump, on the facade's serialized context.
 *
 * This rail tests the adapter in exactly the mode applications run it:
 *   - real MsQuic loopback, real worker threads, real TLS, managed
 *     client + managed server (listener on port 0);
 *   - every moq_session_* call happens inside the managed on_lane_pump,
 *     driven by a per-side SCRIPT of when-this-event -> do-this-action
 *     steps loaded before the client ever connects (each step is
 *     triggered by an observed event, and actions generate the traffic
 *     that triggers subsequent steps, so no external wakeups are
 *     needed);
 *   - the main thread only loads scripts, waits on condition-variable
 *     observation predicates with bounded timeouts, and asserts the
 *     recorded observations after teardown (managed stop/destroy is
 *     the quiescence barrier).
 *
 * It is semantic conformance over the real transport, not the
 * deterministic sim rail: scenarios assert protocol outcomes, never
 * event interleavings or timings.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include <moq/msquic_managed.h>

/* Script step triggers, evaluated inside on_lane_pump after each event
 * batch. A step fires once, in order. */
typedef enum msqc_when {
    MSQC_ON_SETUP = 1,     /* MOQ_EVENT_SETUP_COMPLETE seen */
    MSQC_ON_SUB_REQUEST,   /* SUBSCRIBE_REQUEST recorded */
    MSQC_ON_SUB_OK,        /* SUBSCRIBE_OK seen */
    MSQC_ON_OBJECTS,       /* objects received >= arg */
    MSQC_ON_OBJECTS_G1,    /* objects received in group 1 >= arg */
    MSQC_ON_CLOSED,        /* facade reports closed or fatal */
} msqc_when_t;

typedef enum msqc_do {
    MSQC_DO_NOTHING = 0,
    MSQC_DO_SUBSCRIBE,        /* fixed test namespace/track */
    MSQC_DO_ACCEPT,           /* accept the recorded subscription */
    MSQC_DO_PUBLISH,          /* a1 objects of a2 bytes, one subgroup,
                                 finished with FIN */
    MSQC_DO_PUBLISH_RESET,    /* one object of a2 bytes, then
                                 reset_subgroup with code a1 */
    MSQC_DO_PUBLISH_DGRAM,    /* one datagram object: group a1,
                                 object 0, a2 payload bytes */
    MSQC_DO_PUBLISH_DGRAM_STATUS, /* one status-only datagram (no
                                 payload): group a1, object 0 */
    MSQC_DO_CLOSE,            /* moq_session_close with code a1 */
    MSQC_DO_PROBE_DEAD,       /* moq_session_subscribe attempt on the
                                 (expected-dead) session; rc recorded */
} msqc_do_t;

typedef struct msqc_step {
    msqc_when_t when;
    uint64_t when_arg;
    msqc_do_t act;
    uint64_t a1;
    uint64_t a2;
} msqc_step_t;

#define MSQC_MAX_STEPS 8

/* Everything a scenario can assert on, recorded inside on_lane_pump. Read
 * from the main thread only after msqc_pair_teardown (the quiescence
 * barrier) — mid-run waiting goes through msqc_wait_*. */
typedef struct msqc_obs {
    int setup;             /* SETUP_COMPLETE events */
    int sub_request;
    int sub_ok;
    int session_closed;    /* MOQ_EVENT_SESSION_CLOSED events */
    int objects;
    int objects_g1;        /* of which: group id 1 */
    uint64_t object_bytes;
    uint64_t seen_g0;      /* bitmask of object ids seen in group 0 */
    uint64_t seen_g1;      /* bitmask of object ids seen in group 1 */
    int dgram_objects;     /* OBJECT_RECEIVED with datagram=true */
    int status_objects;    /* payload-less datagrams matching the
                              side's expected status metadata exactly;
                              any other payload-less arrival is an
                              object_error */
    int object_errors;     /* id/length/content mismatches, duplicates */
    int publish_errors;    /* publish-side call failures */
    bool closed;           /* facade terminal, clean close */
    bool fatal;            /* facade terminal, fatal */
    uint64_t close_code;   /* facade close code once closed */
    uint64_t fatal_code;   /* facade fatal code once fatal */
    int pending_sends;     /* drain probes, last pump batch's values —
                              zero at the terminal proves every
                              accepted send/datagram completed or was
                              cleanly canceled */
    int pending_dgrams;
    int probe_rc;          /* MSQC_DO_PROBE_DEAD result */
    bool probe_ran;
} msqc_obs_t;

typedef struct msqc_side {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool sync_init;        /* mu/cv created (partial-teardown guard) */
    msqc_obs_t obs;

    moq_msquic_managed_t *m;

    /* script (loaded before connect; pump-private cursor) */
    msqc_step_t steps[MSQC_MAX_STEPS];
    size_t step_count;
    size_t cursor;

    /* pump-private state */
    moq_subscription_t sub;      /* recorded SUBSCRIBE_REQUEST */
    bool have_sub;
    uint64_t next_group;         /* one group per publish action */
    uint64_t expect_size;        /* group-0 object size (0 = unchecked) */
    uint64_t expect_size_g1;     /* group-1 object size (0 = unchecked) */
    /* the one status datagram a scenario may expect: group/object id
     * and status value, all pinned exactly */
    bool expect_status_set;
    uint64_t expect_status_group;
    uint64_t expect_status_object;
    moq_object_status_t expect_status_value;
} msqc_side_t;

typedef struct msqc_pair {
    msqc_side_t client;
    msqc_side_t server;
} msqc_pair_t;

typedef struct msqc_pair_cfg {
    const char *cert;
    const char *key;
    moq_version_t client_version; /* 0 = facade default (draft-16) */
    moq_version_t server_version;
} msqc_pair_cfg_t;

/* Bring the pair up (scripts must already be loaded into the sides).
 * Returns 0 on success; safe to tear down a partially set-up pair. */
int msqc_pair_setup(msqc_pair_t *p, const msqc_pair_cfg_t *cfg);

/* Tear everything down; managed stop/destroy quiesce the transport,
 * after which obs may be read directly. */
void msqc_pair_teardown(msqc_pair_t *p);

/* Wait (bounded) until an int observation reaches at least `least`, or
 * a bool observation becomes true. Return false on timeout. */
bool msqc_wait_count(msqc_side_t *sd, const int *field, int least);
bool msqc_wait_flag(msqc_side_t *sd, const bool *field);

/* The deterministic object payload (index-tagged), shared by publish
 * and verify. */
void msqc_payload_fill(uint8_t *dst, uint64_t size, uint64_t idx);

#endif /* MSQUIC_CONTRACT_PAIR_H */
