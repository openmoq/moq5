#ifndef FIN_HANDOFF_TEST_INTERNAL_H
#define FIN_HANDOFF_TEST_INTERNAL_H

/*
 * Draft-18 request-stream FIN ownership: a FIN that arrives in the same chunk
 * as the request must survive the transition from the receiving staging owner
 * to the destination owner the request creates, and every terminal that closes
 * such an owner must consult it before reserving a drain reference.
 */
#include <moq/moq.h>

#include <moq/control_d18.h>

#include <moq/transport_bridge.h>

#include "../test_support.h"

#include "../../support/ownership_graph.h"

#include "../../support/fin_case.h"

#include "../../support/txn_snapshot.h"

#include "../../../core/src/session/session_internal.h"



/* The obligations do not share one shape, so each class is its own top-level
 * descriptor with its own callback signatures and its own validator. A tagged
 * union was rejected: once every member carries populated hook pointers, a
 * descriptor reinterpreted as the wrong member can validate by accident,
 * because the overlapping fields are all non-null. Separate types make that
 * mistake unrepresentable rather than merely detected.
 *
 * The session descriptors prove SEMANTIC FIN delivery and consumption; the
 * bridge descriptor proves PHYSICAL stream retirement. Neither is inferred
 * from the other, and neither is inferred from the owner disappearing. */

/* Which terminals a family offers. A family with no internal pre-visibility
 * terminal (pending-join FETCH has one; PUBLISH does not) declares its
 * absence rather than leaving a callback silently unset, and the validator
 * rejects any disagreement between a bit and its callback. */
typedef enum fin_terminal_caps {
    FIN_TERM_APP_REJECT      = 1u << 0,
    FIN_TERM_INTERNAL_REJECT = 1u << 1,
    FIN_TERM_APP_ACCEPT      = 1u << 2,
} fin_terminal_caps_t;

/* Namespace subscriptions carry no role, so their descriptor declares that
 * explicitly rather than leaving a zero that reads as a real role. */
#define FIN_OWNER_ROLE_NA (-1)


/* Owner-transition case: a producer P1-P7 and one of its role-valid
 * terminals, on a session. */
typedef struct fin_owner_case {
    const char        *name;
    void              *ctx;
    unsigned           caps;
    txs_op_hooks_t     hooks;          /* composed, not replaced */
    moq_result_t     (*feed)(moq_session_t *s, void *ctx,
                             moq_stream_ref_t ref, bool fin);
    moq_request_kind_t owner_kind;     /* destination the producer creates */
    int                owner_role;
    int                owner_state;
    moq_result_t     (*app_reject)(moq_session_t *s, void *ctx,
                                   uint64_t handle);
    moq_result_t     (*internal_reject)(moq_session_t *s, void *ctx,
                                        moq_stream_ref_t ref);
    /* A family whose accepting terminal also consults the FIN latch declares
     * it here rather than having the runner call it behind the model. */
    moq_result_t     (*app_accept)(moq_session_t *s, void *ctx,
                                   uint64_t handle);
} fin_owner_case_t;


/* Non-destination case: an Axis 3 terminal that can never carry a marker, so
 * it has no producer and no destination owner. */
typedef struct fin_nondest_case {
    const char     *name;
    void           *ctx;
    txs_op_hooks_t  hooks;
    moq_result_t  (*arrange)(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                             bool fin);
    moq_result_t  (*terminal)(moq_session_t *s, void *ctx,
                              moq_stream_ref_t ref);
} fin_nondest_case_t;




/* -- PUBLISH as the first owner-transition descriptor ---------------- */

/* -- PUBLISH family hooks ------------------------------------------- */

/* Mutation inventory beyond the generic snapshot: the publication entry's
 * role, its request-bidi identity, its durable FIN latch, and the transitional
 * FIN ownership a same-call handoff installs. The two FIN facts are captured
 * separately so a refusal cannot silently move the peer's close from one to
 * the other. State and generation are already covered by txs_owner_capture. */
typedef struct pub_hook_state {
    int      role;
    uint64_t handle;
    uint64_t req_stream_ref;
    struct {
        int recv_fin;
    } rs;
    int      handoff_fin;
    int      busy;
} pub_hook_state_t;


typedef struct pub_ctx {
    uint64_t         request_id;
    uint64_t         alias;
    moq_stream_ref_t ref;
    uint64_t         last_pub;   /* handle the request event surfaced */
} pub_ctx_t;



/* -- FETCH (P1 standalone) ------------------------------------------- */

/* FETCH's owner-present FIN outcome is LATCH AND RETAIN, not teardown: the
 * fetch entry survives the peer's close and records it, because the response
 * and any data stream are still to come. Owner disappearance is therefore
 * never the evidence of FIN consumption here -- the latch is. */

typedef struct fetch_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_fetch;   /* handle the request event surfaced */
} fetch_ctx_t;


typedef struct fetch_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    struct {
        int recv_fin;
    } rs;
    /* The transitional FIN ownership a same-call handoff installs. Both
     * publisher-role FETCH states consume it synchronously into req_recv_fin,
     * so every observation point must find it already clear. */
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} fetch_hook_state_t;


/* The FETCH destination owner's identity, DECLARED from pool state before the
 * request is fed. Every field is derived, never read back: the slot the free
 * scan will choose, the live generation that slot will carry, the handle those
 * two pack into, and the request id / stream ref the fixture itself supplies.
 * One family-owned record so P1 and P2 assert the same absolute shape. */
typedef struct fetch_ident {
    int              slot;
    uint32_t         gen;
    uint64_t         handle;
    uint64_t         request_id;
    moq_stream_ref_t ref;
} fetch_ident_t;



/* The ring's exact contents -- every (reference, reason) pair -- so
 * conservation and release can be proven, not just counted. */
#define DRAIN_SNAP_MAX 512


typedef struct drain_snap {
    size_t   count;
    uint64_t ref[DRAIN_SNAP_MAX];
    uint8_t  reason[DRAIN_SNAP_MAX];
    int      overflow;
} drain_snap_t;


/* The subscription half of the two-owner transaction, read through the
 * bounds-safe registry path: a slot is validated before the pool is indexed,
 * so a stale key cannot become an out-of-range read. */
typedef struct sub_owner_state {
    int      present;
    int      state;
    int      role;
    uint64_t handle;
    uint64_t req_stream_ref;
    struct {
        int recv_fin;
    } rs;
    int      slot;
    int      has_stream_ref;
    uint64_t registry_ref;
    int      busy;
} sub_owner_state_t;



/* -- PUBLISH_NAMESPACE (P4) ------------------------------------------ */

/* Like FETCH, the announcement's owner-present FIN outcome is LATCH AND
 * RETAIN: an inbound PUBLISH_NAMESPACE creates a receiver-role announcement
 * the application still has to answer, so the peer closing its send half is
 * recorded on the owner rather than tearing it down. */

typedef struct pn_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_ann;   /* handle the request event surfaced */
} pn_ctx_t;



/* -- SUBSCRIBE_TRACKS (P5) ------------------------------------------- */

/* Unlike FETCH and PUBLISH_NAMESPACE, this family's owner-present FIN outcome
 * is TEARDOWN: the peer closing its send half cancels the track subscription,
 * surfacing exactly one cancellation and reclaiming the entry. When the event
 * queue cannot take that cancellation the owner and the observed close must
 * both survive for the retry. */

typedef struct st_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_handle;
} st_ctx_t;



/* -- the declared namespace-sub owner inventory ---------------------- */

/* The DECLARED mutation inventory of one namespace-sub entry: its identity
 * (state, stream kind, generation, handle, request id and ref, the whole
 * request endpoint), its request shape (interest, forward), its lifecycle and
 * auth flags, the exact prefix it stored, and the request bytes it retained.
 * Deliberately NOT covered, because nothing on these routes touches them: the
 * announced-suffix set and its receive-budget charge, the resolved-token
 * storage itself (only its count), and the auth transaction's overlay. Every
 * bound is checked BEFORE a dereference, so a corrupt entry becomes an
 * incomparable record rather than an out-of-range read. */
#define NSO_BUF_MAX 256

/* draft-18 caps a Track Namespace at 32 fields. */
#define NSO_MAX_PARTS 32


typedef struct nso_snap {
    int      present;
    int      state;
    int      stream_kind;
    uint32_t generation;
    uint64_t handle;
    uint64_t request_id;
    uint64_t stream_ref;
    int      ep_kind;
    int      ep_slot;
    int      ep_has_request_id;
    uint64_t ep_request_id;
    int      ep_has_stream_ref;
    uint64_t ep_stream_ref;
    int      interest;
    int      forward;
    int      pending_fin;
    int      parse_complete;
    int      got_response;
    int      handoff_fin;
    int      closing_remote_error;
    int      goaway_sent;
    int      auth_processed;
    int      auth_committed;
    uint64_t auth_reject_code;
    size_t   token_count;
    int      prefix_valid;
    size_t   prefix_count;
    size_t   prefix_buf_len; /* the owned extent the parts must sum to */
    size_t   prefix_len;      /* encoded (u8 len, bytes) per part */
    uint8_t  prefix[NSO_BUF_MAX];
    const void *recv_buf;     /* preserved across reuse: identity matters */
    size_t   recv_cap;
    size_t   recv_len;
    uint8_t  recv[NSO_BUF_MAX];
    int      invalid;
} nso_snap_t;

typedef struct nss_ctx {
    uint64_t         request_id;
    moq_stream_ref_t ref;
    uint64_t         last_handle;
    uint8_t          last_req[192];
    size_t           last_req_len;
} nss_ctx_t;


/* -- Seeded-owner conservation --------------------------------------- */

/* A seeded owner must be preserved WHOLE, not merely present: occupancy and a
 * lookup by stream ref would miss a changed role, state, generation, handle,
 * request identity, FIN latch or registry linkage. Each family captures every
 * durable field it owns and the whole capture is re-taken and compared.
 *
 * The storage is a plain union rather than three parameters. Nothing about the
 * union prevents a mismatched pair: that is enforced by seed_ops_t, which owns
 * a family's seed, capture and check together, so a case names one bundle or
 * none. */

#define REQ_SEED_BUF_MAX 256


typedef struct req_seed_snap {
    int      busy;          /* occupied entries in the family's pool */
    int      found;         /* an entry keyed by the seeded stream ref */
    int      state;
    int      role;
    uint32_t generation;
    uint64_t handle;
    uint64_t request_id;
    uint64_t stream_ref;
    int      fin;           /* the seeded owner's own durable FIN latch */
    size_t   recv_len;      /* buffered response bytes on its request bidi */
    /* Registry linkage, an independent key: the whole endpoint the lookup
     * returns, not only its kind and slot. */
    int      reg_kind;
    int      reg_slot;
    int      reg_has_stream_ref;
    uint64_t reg_stream_ref;
    /* The by-REQUEST-ID registry is an independent key: a leaked entry there
     * would survive a retirement that only cleared the stream-ref index. */
    int      id_kind;
    int      id_slot;
    /* The by-ID lookup reconstructs the request-id half of the endpoint and
     * leaves the stream-ref half zero, so those are the fields worth pinning. */
    int      id_has_request_id;
    uint64_t id_request_id;
    /* Namespace-subscription lifecycle scalars; other families leave these
     * zero, so only the owner that sets them can diverge on them. */
    int      ns_got_response;
    int      ns_parse_complete;
    int      ns_closing_remote_error;
    /* The durable local-teardown obligation marker. Distinct from the
     * `fin` latch above: `fin` records that the peer's send half closed, while
     * this records that an extra-bytes/re-fed teardown is owed on this bidi. A
     * capacity-refused teardown must retain BOTH before the empty re-feed. */
    int      ns_local_teardown_pending;
    /* The retained request bytes themselves. A length alone would let an
     * altered but still-decodable message replay to the same terminal. */
    int      buf_invalid;   /* longer than the snapshot, or absent while
                             * claiming a length: nothing is copied and the
                             * snapshot can never compare equal */
    uint8_t  buf[REQ_SEED_BUF_MAX];
} req_seed_snap_t;


#define HIST_SEED_KEY_MAX 128


typedef struct hist_seed_snap {
    int      occupied;      /* in-use records in the registry */
    int      found;
    /* The key could not be captured -- longer than the snapshot, or absent
     * while claiming a length. No bytes are copied, and a snapshot carrying it
     * can never compare equal, so an over-long length can never index the
     * fixed array. */
    int      invalid;
    int      in_use;
    int      has_largest;
    uint32_t refs;
    uint64_t largest_group;
    uint64_t largest_object;
    size_t   key_len;
    uint8_t  key[HIST_SEED_KEY_MAX];
} hist_seed_snap_t;


typedef union seed_snap {
    req_seed_snap_t  req;
    hist_seed_snap_t hist;
} seed_snap_t;


/* Seeding and conservation are ONE family-owned object, so a case cannot pair
 * one family's capture with another's check: it names the bundle or nothing. A
 * case that establishes no prior state carries no bundle at all. */
typedef struct seed_ops {
    void (*seed)(moq_session_t *s, moq_stream_ref_t seed_ref);
    void (*capture)(const moq_session_t *s, moq_stream_ref_t seed_ref,
                    seed_snap_t *out);
    void (*check)(const moq_session_t *s, moq_stream_ref_t seed_ref,
                  const seed_snap_t *want, const char *what);
} seed_ops_t;


/* -- The branch table ------------------------------------------------- */

/* One pre-commit reject branch. The expected REQUEST_ERROR payload -- code and
 * reason phrase, or the documented absence of a reason -- is what proves the
 * intended branch answered rather than a neighbour, so it is declared here and
 * encoded independently rather than read back from the session. */
typedef struct precommit_case {
    const char *name;
    moq_session_t *(*make)(void);
    /* Establishes the exhaustion the branch reports and conserves it; NULL
     * when the branch needs no prior state. */
    const seed_ops_t *seed_ops;
    /* Asserts the branch will be selected, before the request is fed. */
    void        (*precondition)(moq_session_t *s);
    size_t      (*encode)(uint8_t *buf, size_t cap);
    uint64_t      err_code;
    uint64_t      err_retry;     /* Retry Interval as sent */
    const char   *err_reason;    /* NULL: the branch carries no reason */
    /* No destination owner exists for the rejected stream, and any owner the
     * seed created still does. */
    void        (*check_owners)(const moq_session_t *s, moq_stream_ref_t ref,
                                moq_stream_ref_t seed_ref);
} precommit_case_t;


/* The owner a terminal condemns, and everything that must survive a refusal. */
typedef struct owner_ops {
    void (*capture)(const moq_session_t *s, moq_stream_ref_t ref,
                    seed_snap_t *out);
    void (*check)(const moq_session_t *s, moq_stream_ref_t ref,
                  const seed_snap_t *want, const char *what);
    void (*check_retired)(const moq_session_t *s, moq_stream_ref_t ref);
} owner_ops_t;


/* Durable state outside the generic snapshot, captured once and compared in
 * both directions. */
typedef struct extra_ops {
    void (*capture)(const moq_session_t *s);
    int  (*check_blocked)(const moq_session_t *s, const char *what);
    int  (*check_committed)(const moq_session_t *s, const char *what);
} extra_ops_t;


/* One Axis 3 terminal reached by one inbound message. */
/* The owner's exact edge set plus the identity the runner needs to prove
 * COMPLETE retirement -- no edge anywhere still points at it -- without knowing
 * the family. `owner_slot` is a pointer because the arm declares the slot. */
typedef struct terminal_graph_ops {
    int (*owner_edges)(const og_graph_t *g, moq_stream_ref_t ref,
                       const char *what);
    int         owner_kind;
    const int  *owner_slot;
} terminal_graph_ops_t;


typedef struct terminal_case {
    const char *name;
    moq_session_t *(*make)(void);
    /* Establishes the committed owner in the state the terminal requires and
     * RETURNS its stream ref: a locally-opened request bidi gets its ref from
     * the transport action, so the runner must not assume one. */
    moq_stream_ref_t (*arm)(moq_session_t *s);
    /* The message whose arrival drives the terminal. */
    size_t      (*encode)(uint8_t *buf, size_t cap);
    /* The reason the retirement reserves, DECLARED rather than read back. */
    uint8_t       drain_reason;
    /* Everything the terminal owes, events first then actions. Some terminals
     * surface an event and some do not, so the runner cannot assume either. */
    void        (*want_output)(txs_norm_vec_t *v, moq_stream_ref_t ref);
    /* Normalizers for exactly the records this terminal can produce; an
     * unexpected kind fails rather than being counted. */
    const txs_op_hooks_t *hooks;
    const owner_ops_t *owner;
    /* Optional, and indivisible: durable state the generic snapshot does not
     * model -- data streams, their counters and deadlines, the send buffer.
     * A refusal must leave it untouched and a completion must leave it in the
     * committed shape, so the two comparisons ship together with the capture
     * that feeds them. Declaring any one alone is a descriptor error. */
    const extra_ops_t *extra;
    /* Graph conservation is an EXPLICIT, VALIDATED capability, and its three
     * dependent members live in ONE descriptor so none can be dropped while the
     * rest stay valid: a NULL `graph` disables the capability, a non-NULL one
     * must be complete and its kind supported. When present, the runner applies
     * the same integrity, exact-owner-edge and topology conservation as the
     * durable runner -- at arm, under blockage, after the filler release, and
     * after every retirement. Lookup-based registry fields prove a key
     * resolves, never that no duplicate, hidden or unreachable edge exists. */
    const terminal_graph_ops_t *graph;
} terminal_case_t;



/* -- Failed subscription REQUEST_UPDATE (§10.9.1) --------------------- */

/*
 * A REQUEST_UPDATE the publisher cannot satisfy terminates the subscription.
 * draft-18 §10.9.1: "When a REQUEST_UPDATE is unsuccessful, the publisher MUST
 * also terminate the subscription by sending a PUBLISH_DONE with error code
 * UPDATE_FAILED." The source reaches that through the shared
 * sub_cancel_subgroups, so the subscription's open data streams are cancelled
 * first. FIN-closed streams retain their delivery timers after the subscription
 * retires, until transport commitment or expiry.
 *
 * Like the rest of Axis 3, this terminal reserves its drain FIN-aware: on a
 * stream-correlated profile it takes one NORMAL reference only when the peer
 * half is still open (req_recv_fin false), so a genuinely open peer half can
 * absorb a late in-flight message, a same-call FIN owes none, and an exhausted
 * ring makes the terminal wait exactly when a reference is owed.
 */

#define FU_SUBGROUPS 2



/* -- the reject descriptor and its runner ---------------------------- */

typedef struct nsr_case {
    const char *name;
    /* Prepares the session and returns the request id the trigger carries. */
    uint64_t (*arm)(moq_session_t *s);
    size_t (*encode)(uint8_t *buf, size_t cap, uint64_t rid);
    uint64_t err_code;
    /* Applies this branch's own mutations to the expected transient record,
     * which the runner has already seeded from the PRE-CALL slot state. */
    void (*declare_transient)(nso_snap_t *want, const nso_snap_t *pre);
} nsr_case_t;



/* -- Axis 3 durable-latch terminals ---------------------------------- */

/*
 * These terminals run LATER than the FIN that populates their owner's latch:
 * a legal earlier exchange leaves `req_recv_fin` set on a still-live,
 * role-valid owner, and the terminal is then driven through its own public or
 * internal route. That is why they cannot use the current-call runner, which
 * assumes the terminal and the FIN arrive in one operation.
 *
 * SCOPE: DIRECT API terminals only -- routes 2, 4, 5, 6 and 7. Its fire()
 * contract assumes a direct result and zero mutation on refusal, which is
 * false for the deferred publish finalize (route 1, which defers behind
 * MOQ_OK after legitimate data progress) and for the announcement error
 * (route 3, whose terminal message itself supplies bytes and the latch).
 * Those two get their own runners.
 *
 * The declared rule is the same one every Axis 3 row uses: a genuinely open
 * peer half owes exactly one releasable MOQ_DRAIN_NORMAL reference, an
 * observed FIN owes none, and an exhausted ring must refuse rather than
 * terminate without one.
 */

typedef struct durable_case {
    const char *name;
    moq_session_t *(*make)(void);
    /* Establishes the role-valid owner with the peer FIN in the requested
     * OBSERVED state and returns its request-bidi ref; the owner handle comes
     * back through `out_handle` so the terminal can be fired through its real
     * API.
     *
     * `fin_observed` is the unified rule's `peer_fin_observed` -- the FIN has
     * been seen on this owner's request bidi -- NOT specifically a durable
     * `req_recv_fin` latch. A route whose FIN fact is a handoff marker rather
     * than a latch is legal here, but every arm must ASSERT which of the two it
     * established, so the runner's rows can never silently claim a latch a
     * route does not hold. */
    moq_stream_ref_t (*arm)(moq_session_t *s, bool fin_observed,
                            uint64_t *out_handle);
    /* Fires the terminal through its own production route. */
    moq_result_t (*fire)(moq_session_t *s, uint64_t handle);
    void (*want_output)(txs_norm_vec_t *v, moq_stream_ref_t ref,
                        uint64_t handle);
    const txs_op_hooks_t *hooks;
    const owner_ops_t *owner;
    /* The owner's EXACT edge set, declared per family. */
    int (*owner_edges)(const og_graph_t *g, moq_stream_ref_t ref,
                       const char *what);
    /* Optional, indivisible: durable owner state the family snapshot does not
     * model. A refusal owes ZERO owner mutation, so a route may snapshot the
     * whole entry representation plus its owned bytes rather than enumerate
     * fields. */
    const extra_ops_t *extra;
    /* Set when the terminal runs inside a sweep that DISCARDS its result: the
     * refusal is then reported as MOQ_OK and is proven by the surviving owner,
     * the absent output and the unchanged ring alone. */
    int refusal_is_silent;
} durable_case_t;



/* The WHOLE established subscription: its object representation plus the bytes
 * it owns. A refused terminal owes zero owner mutation, so this is compared
 * verbatim rather than field by field -- nothing in the entry may move,
 * including the fields req_seed_snap_t does not model (track alias, resolved
 * window, filter/forward state, dynamic groups, timeouts, history identity). */
#define R6_OWN_MAX 256


#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

extern int failures;

extern pub_ctx_t pub_ctx;

extern fetch_ctx_t fetch_ctx;

extern pn_ctx_t pn_ctx;

extern st_ctx_t st_ctx;

extern nss_ctx_t nss_ctx;

extern txs_op_hooks_t pub_hooks_for_actions;

extern const seed_ops_t pub_seed_ops;

extern const owner_ops_t ann_owner_ops;

extern const owner_ops_t st_owner_ops;

extern txs_op_hooks_t goaway_hooks;

extern const owner_ops_t sub_owner_ops;

extern uint64_t g_ns_sub_request_id;

extern const owner_ops_t nsg_owner_ops;

extern moq_stream_ref_t g_fu_sg_refs[FU_SUBGROUPS];

extern size_t g_fu_sg_count;

extern uint64_t g_fu_sg_group[FU_SUBGROUPS];

extern const extra_ops_t fu_extra_ops;

extern int g_ann_want_slot;

extern const uint8_t k_ann_ns0[6];

extern const uint8_t k_ann_ns1[4];

extern txs_op_hooks_t durable_abort_hooks;

extern const extra_ops_t r6_full_ops;

extern const owner_ops_t fetch_owner_ops;

extern const extra_ops_t r5_full_ops;

extern const extra_ops_t r7_full_ops;

extern txs_op_hooks_t r7_hooks;

extern const owner_ops_t r4_owner_ops;

extern const extra_ops_t r4_full_ops;

extern const extra_ops_t r1_full_ops;

extern txs_op_hooks_t r1_hooks;

moq_session_t *make_session_full(moq_perspective_t persp,
                                 uint32_t max_events,
                                 uint32_t max_actions);

moq_session_t *make_session_caps(moq_perspective_t persp,
                                 uint32_t max_events);

moq_session_t *make_session(moq_perspective_t persp);

int pub_busy_count(const moq_session_t *s);

const moq_pub_entry_t *pub_by_ref(const moq_session_t *s,
                                  moq_stream_ref_t ref);

void check_registered(const moq_session_t *s, moq_stream_ref_t ref,
                      moq_request_kind_t kind, int slot);

void check_unregistered(const moq_session_t *s, moq_stream_ref_t ref);

int fin_owner_problems(const fin_owner_case_t *f);

int fin_nondest_problems(const fin_nondest_case_t *f);

void expect_after_call_prepare(txs_snapshot_t *snap);

void fill_drain_ring(moq_session_t *s);

moq_result_t feed_publish(moq_session_t *s, moq_stream_ref_t ref,
                          uint64_t request_id, uint64_t alias, bool fin);

int pub_owner_present_problems(const moq_session_t *s,
                               moq_stream_ref_t ref,
                               moq_request_kind_t want_kind,
                               int want_role, int want_state);

void check_pub_owner_retired(moq_session_t *s, moq_stream_ref_t ref);

void collect_events(moq_session_t *s, const txs_op_hooks_t *h,
                    txs_norm_vec_t *out);

void collect_actions(moq_session_t *s, const txs_op_hooks_t *h,
                     txs_norm_vec_t *out);

void check_no_output(moq_session_t *s, const txs_op_hooks_t *h);

void pub_hook_capture(const moq_session_t *s, void *vctx, void *vst);

int pub_hook_check(const moq_session_t *s, void *vctx, const void *vst);

bool pub_norm_event(const moq_event_t *ev, void *vctx,
                    txs_norm_vec_t *out);

bool pub_norm_action(const moq_action_t *a, void *vctx,
                     txs_norm_vec_t *out);

void want_publish_request(txs_norm_vec_t *v, uint64_t handle);

void want_publish_finished(txs_norm_vec_t *v, uint64_t handle);

void want_reject_error_code(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t error_code);

void want_reject_error_reason(txs_norm_vec_t *v, moq_stream_ref_t ref,
                              uint64_t error_code, uint64_t retry,
                              const char *reason);

void want_reject_error(txs_norm_vec_t *v, moq_stream_ref_t ref);

void check_reject_only_output(moq_session_t *s,
                              const txs_op_hooks_t *h,
                              moq_stream_ref_t ref, const char *name);

fin_owner_case_t pub_family(void);

size_t enc_request_error(uint8_t *buf, size_t cap, uint64_t code,
                         uint64_t retry, const char *reason);

size_t enc_typed_envelope(uint8_t *buf, size_t cap, uint64_t msg_type,
                          const uint8_t *payload, size_t payload_len);

int fetch_busy_count(const moq_session_t *s);

const moq_fetch_entry_t *fetch_by_ref(const moq_session_t *s,
                                      moq_stream_ref_t ref);

fetch_ident_t fetch_declare_ident(const moq_session_t *s,
                                  uint64_t request_id,
                                  moq_stream_ref_t ref);

int fetch_ident_problems(const moq_session_t *s,
                         const fetch_ident_t *d,
                         int want_role, int want_state,
                         const char *what);

int fetch_ident_edges(const og_graph_t *g, const fetch_ident_t *d,
                      const char *what);

moq_result_t fetch_feed(moq_session_t *s, void *ctx,
                        moq_stream_ref_t ref, bool fin);

void fetch_hook_capture(const moq_session_t *s, void *vctx, void *vst);

int fetch_hook_check(const moq_session_t *s, void *vctx,
                     const void *vst);

bool fetch_norm_event(const moq_event_t *ev, void *vctx,
                      txs_norm_vec_t *out);

int fetch_owner_present_problems(const moq_session_t *s,
                                 moq_stream_ref_t ref,
                                 moq_request_kind_t want_kind,
                                 int want_role, int want_state);

void drain_snap(const moq_session_t *s, drain_snap_t *d);

int drain_multiset_equals(const drain_snap_t *have,
                          const drain_snap_t *want, const char *what);

void drain_snap_plus(const drain_snap_t *base, moq_stream_ref_t extra,
                     uint8_t reason, drain_snap_t *out);

void drain_snap_minus(const drain_snap_t *base, moq_stream_ref_t gone,
                      uint8_t reason, drain_snap_t *out);

void check_drain_ref_reason(const moq_session_t *s,
                            moq_stream_ref_t ref, uint8_t reason);

int sub_busy_count(const moq_session_t *s);

void sub_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                       sub_owner_state_t *st);

int ann_busy_count(const moq_session_t *s);

const moq_ann_entry_t *ann_by_ref(const moq_session_t *s,
                                  moq_stream_ref_t ref);

moq_result_t pn_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                     bool fin);

int pn_owner_present_problems(const moq_session_t *s,
                              moq_stream_ref_t ref,
                              moq_request_kind_t want_kind,
                              int want_role, int want_state);

int st_busy_count(const moq_session_t *s);

const moq_track_sub_entry_t *st_by_ref(const moq_session_t *s,
                                       moq_stream_ref_t ref);

moq_result_t st_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                     bool fin);

void nso_read(const moq_session_t *s, int slot, nso_snap_t *v);

int nso_diff(const nso_snap_t *have, const nso_snap_t *want,
             const char *what);

const moq_ns_sub_entry_t *ns_by_ref(const moq_session_t *s,
                                    moq_stream_ref_t ref);

moq_result_t nss_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                      bool fin);

bool nss_norm_action(const moq_action_t *a, void *vctx,
                     txs_norm_vec_t *out);

void want_close_bidi(txs_norm_vec_t *v, moq_stream_ref_t ref);

bool no_event_expected(const moq_event_t *ev, void *ctx,
                       txs_norm_vec_t *out);

moq_session_t *make_session_default(void);

size_t enc_publish_unknown_alias(uint8_t *buf, size_t cap);

size_t enc_pns_full(uint8_t *buf, size_t cap, uint64_t rid,
                    const char *part0,
                    const moq_d18_msg_params_t *params);

void owners_publish_none(const moq_session_t *s, moq_stream_ref_t ref,
                         moq_stream_ref_t seed_ref);

void req_seed_diff(const req_seed_snap_t *have,
                   const req_seed_snap_t *want, const char *what);

void req_seed_capture_registry(req_seed_snap_t *r,
                               const moq_session_t *s,
                               moq_stream_ref_t ref, uint64_t request_id);

void req_seed_append(req_seed_snap_t *r, const uint8_t *tail,
                     size_t tail_len);

void req_seed_copy_buf(req_seed_snap_t *r, const uint8_t *buf,
                       size_t len);

int precommit_problems(const precommit_case_t *c);

size_t enc_update_next(uint8_t *buf, size_t cap);

void arm_output_exact(moq_session_t *s, uint32_t want_event_kind,
                      int want_events, int want_actions,
                      const char *what, uint64_t *out_handle);

moq_stream_ref_t arm_established_announcement(moq_session_t *s);

moq_stream_ref_t arm_established_track_sub(moq_session_t *s);

void want_update_rejected(txs_norm_vec_t *v, moq_stream_ref_t ref);

int terminal_problems(const terminal_case_t *c);

void check_terminal_output(moq_session_t *s, const terminal_case_t *c,
                           moq_stream_ref_t ref, const char *what);

void run_terminal_matrix(const terminal_case_t *c, size_t split);

size_t enc_goaway_request(uint8_t *buf, size_t cap);

void want_goaway_output(txs_norm_vec_t *v, moq_stream_ref_t ref);

moq_stream_ref_t arm_goaway_announcement(moq_session_t *s);

size_t enc_goaway_request_uri(uint8_t *buf, size_t cap);

void want_goaway_output_uri(txs_norm_vec_t *v, moq_stream_ref_t ref);

moq_session_t *make_session_client(void);

int sub_pool_busy(const moq_session_t *s);

const moq_sub_entry_t *sub_entry_by_ref(const moq_session_t *s,
                                        moq_stream_ref_t ref);

void check_sub_terminal_retired(const moq_session_t *s,
                                moq_stream_ref_t ref);

moq_stream_ref_t arm_goaway_local_subscribe(moq_session_t *s);

int ns_sub_pool_busy(const moq_session_t *s);

void nsg_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                       seed_snap_t *o);

void nsg_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                     const seed_snap_t *want, const char *what);

void check_nsg_retired(const moq_session_t *s, moq_stream_ref_t ref);

moq_stream_ref_t arm_goaway_local_ns_sub(moq_session_t *s);

void run_ns_goaway_latched(void);

bool nsl_norm_action(const moq_action_t *a, void *vctx,
                     txs_norm_vec_t *out);

void want_nsl_output(txs_norm_vec_t *v, moq_stream_ref_t ref);

size_t enc_subscribe_pub_role(uint8_t *buf, size_t cap, uint64_t rid);

size_t enc_update_unknown_alias(uint8_t *buf, size_t cap);

moq_stream_ref_t arm_failed_update_completed_subscription(moq_session_t *s);

void fu_extra_capture(const moq_session_t *s);

int fu_extra_check_committed(const moq_session_t *s, const char *what);

void want_bidi_msg(txs_norm_vec_t *v, moq_stream_ref_t ref, bool fin,
                   const uint8_t *msg, size_t len);

void want_failed_update_output(txs_norm_vec_t *v, moq_stream_ref_t ref);

size_t enc_sns_auth(uint8_t *buf, size_t cap, uint64_t rid);

int nsr_problems(const nsr_case_t *c);

void nsr_auth_declare(nso_snap_t *want, const nso_snap_t *pre);

uint64_t nsr_arm_auth(moq_session_t *s);

int durable_problems(const durable_case_t *c);

void run_durable_matrix(const durable_case_t *c);

moq_stream_ref_t arm_announcer_established(moq_session_t *s,
                                           bool fin_observed,
                                           uint64_t *out_handle);

int ann_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                    const char *what);

moq_result_t fire_publish_namespace_done(moq_session_t *s,
                                         uint64_t handle);

void want_announcer_teardown(txs_norm_vec_t *v, moq_stream_ref_t ref,
                             uint64_t handle);

int sub_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                    const char *what);

moq_stream_ref_t arm_subscriber_established(moq_session_t *s,
                                            bool fin_observed,
                                            uint64_t *out_handle);

moq_result_t fire_unsubscribe(moq_session_t *s, uint64_t handle);

void want_bidi_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                      uint64_t handle);

int fetch_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                      const char *what);

moq_stream_ref_t arm_fetch_pending(moq_session_t *s, bool fin_observed,
                                   uint64_t *out_handle);

moq_result_t fire_fetch_cancel(moq_session_t *s, uint64_t handle);

void want_fetch_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                       uint64_t handle);

int r7_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what);

moq_stream_ref_t arm_accepted_subscription(moq_session_t *s,
                                           bool fin_observed,
                                           uint64_t *out_handle);

moq_result_t fire_done_subscribe(moq_session_t *s, uint64_t handle);

void want_done_subscribe(txs_norm_vec_t *v, moq_stream_ref_t ref,
                         uint64_t handle);

int r4_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what);

moq_stream_ref_t arm_publisher_established(moq_session_t *s,
                                           bool fin_observed,
                                           uint64_t *out_handle);

moq_result_t fire_finish_publish(moq_session_t *s, uint64_t handle);

void want_finish_publish(txs_norm_vec_t *v, moq_stream_ref_t ref,
                         uint64_t handle);

int r3_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what);

int r1_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what);

moq_stream_ref_t arm_deferred_publish(moq_session_t *s, bool fin_observed,
                                      uint64_t *out_handle);

moq_result_t fire_deferred_sweep(moq_session_t *s, uint64_t handle);

void want_deferred_finalize(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t handle);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* FIN_HANDOFF_TEST_INTERNAL_H */
