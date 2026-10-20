/*
 * Shared rig for the relay session-binding suites: the relay core driven by
 * real moq_session_t pairs over SimPair. Each connection is one SimPair — the
 * client side is the test peer (publisher / subscriber / watcher), the server
 * side is the relay's session, attached to the production relay binding
 * — the same translation layer the relay executable runs. Correctness
 * parity comes free: every peer is the same strict session engine the
 * conformance suite trusts, and the code under test is the shipped
 * binding, not a copy.
 *
 * The harness holds the counting allocator, the rig and its configuration
 * knobs, the peer-side event drain, the fixtures more than one suite uses
 * (deferred and revalidating authorization hooks, data-plane senders, FETCH
 * ingest helpers, the forwarded Joining FETCH rig, the draft-22 track
 * fixtures), and the flows one suite runs as a fixture control for another.
 * Every knob starts at its default and every row that changes one restores
 * it before returning, so each suite starts from the same state and the
 * order of the suites never matters. Test-only: linked into the suite
 * executables, never into a library.
 */
#ifndef RELAY_SESSION_BINDING_HARNESS_H
#define RELAY_SESSION_BINDING_HARNESS_H

#include <moq/relay/relay.h>

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif
#include "../src/bind/moqr_bind_auth.h"
#include "../src/bind/moqr_bind_retrieval.h"
#include "../src/bind/moqr_bind_metadata.h"

#include <moq/relay/auth_toy.h>

#include <moq/moq.h>
#include <moq/sim.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* counting allocator */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
    long attempts;   /* every alloc/realloc attempt (success or injected fail) */
    long fail_at;    /* fail EXACTLY the fail_at-th attempt; 0 = never. Test-
                      * local OOM injection; the single failure lets later
                      * cleanup allocations still succeed.                     */
    size_t fail_size;/* fail the FIRST alloc of EXACTLY this many bytes, once
                      * (cleared on firing); 0 = never. Targets a distinctly-
                      * sized allocation (the FETCH coalesce buffer) where an
                      * attempt index is not predictable through the SimPair.   */
} ca_t;

static inline moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

/* -- rig: SimPair conns attached to the production binding ------------------ */

#define MAX_CONNS 8

typedef struct conn {
    bool           used;
    /* Test-only: when true, relay_bind_test_rig_cycle does not transport this pair, so the
     * relay's writes to it stay blocked across pumps. Only which existing
     * SimPair is stepped changes; no session or bind behaviour is altered. */
    bool           frozen;
    moq_simpair_t *sp;
    moq_session_t *peer;    /* client side: the test's endpoint          */
    moq_session_t *rsess;   /* server side: the relay's session          */
} conn_t;

typedef struct rig {
    ca_t         *alloc;
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;
    conn_t        conns[MAX_CONNS];
    uint64_t      now;
    int           failures;
} rig_t;

#define R_CHECK(rig, expr)                                                \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            (rig)->failures++;                                            \
        }                                                                 \
    } while (0)

/* -- peer-side helpers -------------------------------------------------------- */

/* Data-plane delivery oracle record: one delivered object, captured from the
 * wire-observable OBJECT_RECEIVED event ALONE (no relay internals). A test
 * replays this log against a known publish matrix to prove exactly-once
 * delivery, per-(group, subgroup) object-id order, byte-faithful payloads, and
 * no delivery after a subscription terminal. */
typedef struct dlv_rec {
    uint64_t sub;          /* subscription handle (._opaque)            */
    uint64_t group, subgroup, object_id;
    uint8_t  fp;           /* payload fingerprint (first byte); 0 = none */
    uint8_t  status;       /* moq_object_status_t (NORMAL/EOG/EOT)       */
    bool     payload_null;
    bool     eog;          /* END_OF_GROUP bit on the delivered object   */
    bool     first;        /* the object opened its subgroup (FIRST_OBJECT) */
    bool     datagram;
    bool     after_done;   /* arrived after SUBSCRIBE_DONE (terminal)    */
} dlv_rec_t;
#define DLV_LOG_MAX 256

/* Drain peer events, remembering delivered objects and answering the
 * relay's upstream SUBSCRIBE (the peer is the origin publisher). */
typedef struct peer_state {
    moq_subscription_t up_sub;    /* relay's subscription at this peer     */
    bool               up_seen;
    int                up_subs;   /* SUBSCRIBE requests seen (coalescing)  */
    int                unsubscribed;
    int                objects;   /* objects delivered to this peer        */
    uint64_t           last_group;
    uint64_t           last_object;
    uint8_t            last_byte;
    size_t             last_len;      /* reassembled payload length            */
    int                chunks;        /* OBJECT_CHUNK data chunks received      */
    uint8_t            last_chunk_byte;
    bool               obj_complete;  /* a streamed object ended terminal NORMAL */
    bool               saw_reset;     /* a streamed object ended terminal RESET  */
    uint64_t           last_reset_code; /* the RESET terminal's error_code       */
    bool               last_datagram;
    uint8_t            last_status;
    bool               last_payload_null;
    size_t             last_props_len;
    uint8_t            last_props[16];
    bool               subscribe_ok;
    int                subscribe_errors;
    bool               publish_error;
    bool               has_largest;
    uint64_t           largest_group, largest_object;
    int                ns_found;
    char               order[64];   /* arrival order of control events      */
    int                order_len;
    /* Exact bytes of the last NAMESPACE suffix seen, for byte-identity. */
    size_t             ns_seen_count;
    size_t             ns_seen_lens[8];
    uint8_t            ns_seen_bytes[64];
    size_t             ns_seen_total;
    /* Identity of each NAMESPACE as it arrives: the first byte of the
     * suffix's first field. Lets a test assert WHICH namespace arrived in
     * WHICH order, not merely how many. */
    char               ns_ids[16];
    int                ns_id_len;
    int                ns_gone;
    bool               publish_ok;
    bool               ts_ok;
    int                ts_error;
    uint64_t           ts_largest_group;
    bool               done_seen;
    int                update_oks;         /* # of SUBSCRIPTION_UPDATE_OK    */
    bool               uok_has_largest;    /* the last one's LARGEST_OBJECT  */
    uint64_t           uok_largest_group, uok_largest_object;
    int                done_count;         /* # of SUBSCRIBE_DONE events     */
    uint64_t           done_code;          /* SUBSCRIBE_DONE status_code    */
    int                ns_cancelled;       /* NAMESPACE_CANCELLED count     */
    uint64_t           ns_cancelled_code;  /* its error_code                */
    /* auth-reject observation */
    bool               session_closed;
    uint64_t           close_code;
    uint64_t           sub_error_code;
    bool               sub_error_can_retry;
    uint64_t           sub_error_retry_ms;
    size_t             sub_error_reason_len;
    bool               sub_error_reason_truncated;
    uint8_t            sub_error_reason[64];
    uint64_t           publish_error_code;
    bool               publish_error_can_retry;
    uint64_t           publish_error_retry_ms;
    int                publish_errors;
    uint64_t           ts_error_code;
    int                ns_rejected;
    uint64_t           ns_rejected_code;
    bool               ns_rejected_can_retry;
    uint64_t           ns_rejected_retry_ms;
    char               ns_rejected_reason[64];
    int                ns_sub_error;
    uint64_t           ns_sub_error_code;
    int                fetch_error;
    uint64_t           fetch_error_code;
    bool               fetch_error_can_retry;
    uint64_t           fetch_error_retry_ms;
    /* FETCH response capture */
    bool               fetch_ok;
    bool               fok_eot;             /* FETCH_OK End Of Track          */
    uint64_t           fok_end_group, fok_end_object;
    int                fetch_complete;      /* FETCH_COMPLETE (stream FIN)     */
    int                fetch_resets;        /* FETCH_RESET                     */
    uint64_t           fetch_reset_code;
    struct {
        uint64_t group, subgroup, object;
        uint8_t  fp;                        /* payload first byte             */
        bool     datagram;
        uint64_t len;                       /* full payload length            */
        uint32_t fp_all;                    /* FNV-1a over all payload bytes   */
    }                  fdlv[64];            /* FETCH_OBJECT delivery log       */
    int                fdlv_n;
    struct {
        moq_fetch_range_kind_t kind;
        uint64_t               group, object;
    }                  fgap[8];             /* FETCH_GAP marker log            */
    int                fgap_n;
    int                tracks_error;
    uint64_t           tracks_error_code;
    bool               namespace_accepted;   /* PUBLISH_NAMESPACE accepted   */
    bool               ns_sub_ok;            /* SUBSCRIBE_NAMESPACE accepted */
    int                namespace_accepts;    /* NAMESPACE_ACCEPTED count     */
    int                ns_sub_oks;           /* NS_SUB_OK count              */
    /* subgroup-stream churn observation: a (group, subgroup) that FIN'd and then
     * received another object means the relay closed and reopened the stream. */
    uint64_t           finished_gsg[16][2];  /* (group, subgroup) FIN'd       */
    int                finished_count;
    int                subgroup_finished;    /* total SUBGROUP_FINISHED       */
    int                subgroup_resets;      /* total SUBGROUP_RESET          */
    uint64_t           reset_gsg[16][2];     /* (group, subgroup) reset       */
    uint64_t           reset_codes[16];      /* each reset's error_code       */
    int                resets_after_done;    /* resets that followed the DONE */
    bool               reopen_after_finish;  /* object on a FIN'd subgroup    */
    /* data-plane delivery oracle log (see dlv_rec_t) */
    dlv_rec_t          dlv[DLV_LOG_MAX];
    int                dlv_n;
    int                dlv_overflow;         /* deliveries beyond the cap     */
    /* fill fetch stream observation (subscriber side, draft-22) */
    int                fill_objects;
    int                fill_gaps;
    int                fill_terminals;
    uint32_t           fill_term_reason;
    uint32_t           fill_term_domain;
    uint64_t           fill_term_code;
    uint64_t           fill_min_group, fill_max_group;
    uint32_t           fill_gap_kind;
    uint64_t           fill_gap_group, fill_gap_object;
    struct {
        uint32_t kind;
        uint64_t group, object;
    }                  fill_gap_log[8];       /* FILL_GAP markers in order   */
} peer_state_t;

#define DP_SG 2u   /* subgroups per group                                    */
#define DP_O  3u   /* data objects per subgroup                              */
#define DP_OPG (DP_SG * DP_O)   /* objects per group (ids are group-unique)  */

/* Under a small origin action queue the origin's own writes hit WOULD_BLOCK; a
 * pump drains the queue (and incidentally advances delivery), so retry. */
#define DP_RETRY(r, expr)                                                     \
    do {                                                                      \
        moq_result_t rc_;                                                     \
        for (int t_ = 0; t_ < 2000; t_++) {                                   \
            rc_ = (expr);                                                     \
            if (rc_ != MOQ_ERR_WOULD_BLOCK) {                                 \
                break;                                                        \
            }                                                                 \
            relay_bind_test_rig_pump((r), 1);                                 \
        }                                                                     \
        R_CHECK((r), rc_ == MOQ_OK);                                          \
    } while (0)

/* ---- Retained generation fixtures shared by the forwarding rows ---------- *
 * (0,0) base bytes and (0,1) delta bytes of the catalog generation an origin
 * retains, as exact seeds a downstream observation can check against. */
#define BOOT_BASE_LEN  48
#define BOOT_DELTA_LEN 24

/* ---- Forwarded Joining FETCH over the production binding -------------------- *
 * A raw-session ORIGIN announces boot/cat and answers the relay's upstream
 * SUBSCRIBE (catalog Largest {largest_group, largest_object}) and its forwarded FETCH by
 * script; raw-session DOWNSTREAM peers subscribe LargestObject and issue the
 * Relative Joining FETCH(0) the catalog bootstrap uses. Every row asserts the
 * exact downstream ledger, the origin's observed requests/cancellations, and
 * the binding's forwarding gauges (transactions and pool bytes back to zero;
 * the counting allocator back to zero after destroy). */

#define JF_LEDGER 40
#define JF_MAX_REQ 8

typedef struct jf_origin {
    conn_t            *cn;
    bool               gone;          /* closed at the binding: not drained */
    uint64_t           largest_group, largest_object;
    int                ns_accepted;
    int                sub_requests;
    moq_subscription_t cat_sub, live_sub;
    bool               cat_seen, live_seen;
    int                fetch_requests;
    moq_fetch_t        req[JF_MAX_REQ];
    uint64_t           req_sg[JF_MAX_REQ], req_so[JF_MAX_REQ];
    uint64_t           req_eg[JF_MAX_REQ], req_eo[JF_MAX_REQ];
    int                cancelled;
} jf_origin_t;

typedef struct jf_down {
    conn_t            *cn;
    moq_subscription_t sh;            /* the catalog subscription */
    bool               sub_ok;
    int                sub_err;
    int                done;
    moq_subscription_t live_sh;
    bool               live_ok;
    int                live_objects;
    bool               live_exact;
    moq_fetch_t        fh;
    bool               fh_set;
    int                ok, objects, gaps, complete, error, reset, foreign;
    uint64_t           error_code, reset_code;
    bool               can_retry;
    uint8_t            reason[32];
    size_t             reason_len;
    bool               ok_eot;
    uint64_t           ok_eg, ok_eo;
    uint64_t           obj_g[8], obj_o[8];
    size_t             obj_len[8];
    uint8_t            obj_first[8];
    bool               obj_exact[8];
    size_t             props_len[8];
    bool               props_exact[8];
    int                ledger[JF_LEDGER];
    int                ledger_n;
    bool               overflow;
    int                closed;
    uint64_t           close_code;
    char               close_reason[48];
    int                ns_accepted;     /* NAMESPACE_ACCEPTED for the peer's own announces */
    int                ns_rejected;
    uint64_t           ns_rejected_code;
    char               ns_rejected_reason[64];
    /* Further requests this peer issued besides fh: their FETCH_ERROR counts. */
    moq_fetch_t        req_fh[4];
    int                req_err[4];
    int                req_n;
} jf_down_t;

typedef struct jf {
    ca_t        a;
    rig_t       rig;
    jf_origin_t o;
    jf_down_t   d[4];
    int         n_down;
    jf_down_t   live;            /* live-path control subscriber (own conn) */
    bool        has_live;
    moq_version_t version;
    const char *lbl;
} jf_t;

#define JF_KEY_LEN (4u + 3u + 7u)   /* "boot" "cat" "catalog": one owned copy */
#define JF_PROPS_LEN 4u   /* two non-mandatory varint KVPs */

/* Origin-side scripted answers on request k. Every call retries across
 * cycles while the origin's own action queue refuses (never a relay path). */
#define JF_ORIGIN_RETRY(j, expr, var)                                      \
    do {                                                                  \
        var = (expr);                                                     \
        for (int r_ = 0; r_ < 8 && var == MOQ_ERR_WOULD_BLOCK; r_++) {    \
            relay_bind_test_jf_cycle(j);                                  \
            var = (expr);                                                 \
        }                                                                 \
    } while (0)

/* Announce "live"/"cam", pull track "v" through a priming subscriber and
 * publish `groups` groups of three objects each, so the relay's retained log
 * and Largest Object are populated before the subscriber under test arrives. */
typedef struct d22_track {
    conn_t      *pub;
    conn_t      *prime;
    peer_state_t pub_ps;
    peer_state_t prime_ps;
    moq_bytes_t  nsp[2];
} d22_track_t;

/* Rig configuration knobs and per-family state shared by the suites; each is
 * documented at its definition in relay_session_binding_harness.c. */
extern uint32_t relay_bind_test_max_actions;
extern bool relay_bind_test_server_streaming_objects;
extern bool relay_bind_test_client_streaming_objects;
extern uint32_t relay_bind_test_max_open_subgroups;
extern uint32_t relay_bind_test_log_max_groups;
extern uint64_t relay_bind_test_log_max_bytes;
extern moq_bytes_t relay_bind_test_accept_track_properties;
extern uint64_t relay_bind_test_accept_track_alias;
extern uint32_t relay_bind_test_initial_request_capacity;
extern uint32_t relay_bind_test_grant_window;
extern uint64_t relay_bind_test_fill_timeout_default_ms;
extern uint64_t relay_bind_test_fill_timeout_max_ms;
extern size_t relay_bind_test_bind_cfg_size;
extern bool   relay_bind_test_bind_cfg_exact;
extern uint32_t relay_bind_test_max_events;
extern uint32_t relay_bind_test_server_max_open_subgroups;
extern uint32_t relay_bind_test_linger_us;
extern moq_sim_trace_fn relay_bind_test_trace_fn;
extern void *relay_bind_test_trace_ctx;
extern uint32_t relay_bind_test_fault_flags;
extern uint32_t relay_bind_test_fault_per_mille;
extern moqr_authorize_fn relay_bind_test_authorize;
extern void *relay_bind_test_authorize_ctx;
extern uint32_t relay_bind_test_grant_bytes;
extern uint64_t relay_bind_test_fetch_forward_bytes;
extern uint32_t relay_bind_test_max_fetches;
extern uint32_t relay_bind_test_max_tracks;
extern uint32_t relay_bind_test_max_ns_nodes;
extern uint32_t relay_bind_test_max_parked;
extern uint32_t relay_bind_test_max_announces;
extern uint32_t relay_bind_test_max_ns_subs;
extern moqr_auth_action_t relay_bind_test_defer_action;
extern uint64_t           relay_bind_test_defer_last;
extern uint64_t           relay_bind_test_defer_fixed;
extern moqr_auth_decision_t relay_bind_test_rv_decision;
extern uint64_t             relay_bind_test_rv_lease_sub;
extern uint64_t             relay_bind_test_rv_lease_ann;
extern uint32_t             relay_bind_test_rv_deny_code;
extern bool relay_bind_test_rv_ann_always_allow;
extern const char *relay_bind_test_rv_deny_track;
extern bool relay_bind_test_rv_second_track;
extern const moq_bytes_t relay_bind_test_JF_NS[2];
extern bool relay_bind_test_jf_setup_usub_key_oom;

/* Rig, peer and per-family fixture helpers, and the flows more than one suite
 * runs (documented at their definitions). */
void *relay_bind_test_ca_a(size_t n, void *c);

void *relay_bind_test_ca_r(void *p, size_t o, size_t n, void *c);

void relay_bind_test_ca_f(void *p, size_t n, void *c);

void relay_bind_test_ca_init(ca_t *a);

conn_t *
relay_bind_test_rig_connect(rig_t *r, moq_version_t version);

void
relay_bind_test_rig_cycle(rig_t *r);

void
relay_bind_test_rig_pump(rig_t *r, int cycles);

void
relay_bind_test_rig_step_pair(rig_t *r, conn_t *cn);

void
relay_bind_test_rig_bind_pump_once(rig_t *r);

void
relay_bind_test_rig_pump_except(rig_t *r, const conn_t *skip, int cycles);

moqr_result_t
relay_bind_test_rig_create_ex(rig_t *r, ca_t *a, uint32_t bind_dsubs, uint32_t bind_usubs,
                              uint32_t bind_pubs, uint32_t core_intents);

moqr_result_t
relay_bind_test_rig_create(rig_t *r, ca_t *a);

void
relay_bind_test_rig_destroy(rig_t *r);

bool
relay_bind_test_sub_reason_eq(const peer_state_t *ps, const char *s);

void
relay_bind_test_peer_drain(rig_t *r, conn_t *cn, peer_state_t *ps, bool auto_accept,
                           bool accept_with_largest, uint64_t lg, uint64_t lo);

int
relay_bind_test_parity_flow(moq_version_t version);

bool
relay_bind_test_rig_publish(rig_t *r, moq_version_t version, moqr_ns_t ns, moq_bytes_t name);

void
relay_bind_test_defer_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out);

int
relay_bind_test_run_defer_case(moqr_auth_action_t action, moqr_auth_decision_t decision,
                               const char *name);

void
relay_bind_test_rv_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out);

void
relay_bind_test_rv_reset(void);

void
relay_bind_test_reval_make_subscribe_grant(rig_t *rig, conn_t *pub, conn_t *sub,
                                           peer_state_t *pub_ps, peer_state_t *sub_ps,
                                           moq_bytes_t *nsp2);

void
relay_bind_test_pub_subgroup_plain_fin(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                           uint64_t group, uint64_t subgroup, uint64_t object_id);

void
relay_bind_test_pub_subgroup_n(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                   uint64_t group, uint64_t subgroup, uint64_t n, bool eog);

uint8_t
relay_bind_test_dp_tag(uint64_t g, uint64_t oid);

void
relay_bind_test_dp_send_subgroup(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                                 uint64_t group, uint64_t subgroup);

uint8_t
relay_bind_test_se_tag(uint64_t g, uint64_t o);

void
relay_bind_test_se_send_group(rig_t *r, conn_t *pub, bool push, moq_subscription_t up_sub,
                              moq_publication_t pubh, uint64_t group, uint64_t n);

/* As relay_bind_test_se_send_group, with the subgroup's FIN sent only when
 * `fin`; otherwise the subgroup is left open and its handle is stored in
 * *open_out (may be NULL) for the caller to close later. */
void
relay_bind_test_se_send_group_fin(rig_t *r, conn_t *pub, bool push,
                                  moq_subscription_t up_sub,
                                  moq_publication_t pubh, uint64_t group,
                                  uint64_t n, bool fin,
                                  moq_subgroup_handle_t *open_out);

int
relay_bind_test_dataplane_mixed_draft_oracle(moq_version_t pub_ver, moq_version_t sub_ver,
                                             uint32_t max_actions);

moqr_binding_t
relay_bind_test_fetch_pub_open(rig_t *r);

moqr_track_t
relay_bind_test_fetch_pub_track(rig_t *r, moqr_binding_t pb, const char *name);

void
relay_bind_test_fetch_ing(rig_t *r, moqr_track_t tk, uint64_t g, uint64_t sg, uint64_t o,
                          uint8_t fp);

void
relay_bind_test_fetch_issue(rig_t *r, conn_t *cn, const char *name, uint64_t sg, uint64_t so,
                            uint64_t eg, uint64_t eo, moq_fetch_t *out);

void
relay_bind_test_parity_write_chunk(rig_t *r, conn_t *pub, moq_subgroup_handle_t sg, uint8_t byte,
                                   size_t len);

void
relay_bind_test_boot_fill(uint8_t *buf, size_t len, uint8_t seed);

void
relay_bind_test_jf_origin_drain(rig_t *r, jf_origin_t *o);

void
relay_bind_test_jf_down_drain(rig_t *r, jf_down_t *d);

void
relay_bind_test_jf_cycle(jf_t *j);

void
relay_bind_test_jf_stats(jf_t *j, moqr_bind_fwd_debug_t *st);

void
relay_bind_test_jf_subscribe_catalog(jf_t *j, jf_down_t *d);

void
relay_bind_test_jf_subscribe_live(jf_t *j, jf_down_t *d);

moq_result_t
relay_bind_test_jf_join_ex(jf_t *j, jf_down_t *d, moq_group_order_t order);

void
relay_bind_test_jf_join(jf_t *j, jf_down_t *d);

bool
relay_bind_test_jf_setup_at(jf_t *j, moq_version_t version, int n_down, bool live_control,
                            uint64_t largest_group, uint64_t largest_object, bool origin_last);

bool
relay_bind_test_jf_setup_ex(jf_t *j, moq_version_t version, int n_down, bool live_control,
                            uint64_t largest_object, bool origin_last);

bool
relay_bind_test_jf_setup(jf_t *j, moq_version_t version, int n_down, bool live_control,
                         uint64_t largest_object);

void
relay_bind_test_jf_live_control(jf_t *j);

int
relay_bind_test_jf_teardown(jf_t *j, const char *row);

void
relay_bind_test_jf_origin_accept(jf_t *j, int k, uint64_t eg, uint64_t eo, bool eot, bool with_props);

moq_result_t
relay_bind_test_jf_origin_write_raw(jf_t *j, int k, uint64_t g, uint64_t oid, uint8_t seed,
                                    size_t len, bool with_props);

void
relay_bind_test_jf_origin_end(jf_t *j, int k);

void
relay_bind_test_jf_wait_requests(jf_t *j, int n);

void
relay_bind_test_d22_track_setup(rig_t *rig, d22_track_t *t, moq_version_t version,
                                uint64_t groups);

moq_result_t
relay_bind_test_d22_subscribe(rig_t *rig, conn_t *cn, const d22_track_t *t,
                              const moq_location_filter_cfg_t *loc, const moq_fill_cfg_t *fill,
                              moq_subscription_t *out);

int
relay_bind_test_d22_fill_origin_tail(rig_t *rig, d22_track_t *t, bool answer);

void
relay_bind_test_d22_pub_ids(rig_t *rig, d22_track_t *t, uint64_t group, uint64_t subgroup,
                            const uint64_t *ids, size_t n);

void
relay_bind_test_d22_hole_setup(rig_t *rig, d22_track_t *t);

moq_result_t
relay_bind_test_d22_subscribe_fill(rig_t *rig, conn_t *cn, const d22_track_t *t,
                                   bool has_timeout, uint64_t timeout_ms,
                                   moq_subscription_t *out);

#endif /* RELAY_SESSION_BINDING_HARNESS_H */
