#ifndef BRIDGE_TEST_INTERNAL_H
#define BRIDGE_TEST_INTERNAL_H

#include <moq/moq.h>

#include <moq/transport_bridge.h>

#include <moq/control.h>

#include <moq/control_d18.h>

#include <moq/buf.h>

#include "../test_support.h"

#include "../../support/held_bridge_driver.h"

#include "../../../core/src/session/session_internal.h"

#include "../../../core/src/session/session_transport.h"

#include "../../../core/src/bridge/transport_bridge_internal.h"

#include "../../support/sweep_arm.h"

#include "../../support/failpoint.h"

#include "../../support/fin_case.h"

#include "../../support/ownership_graph.h"

#include "../../support/txn_snapshot.h"

#include "../../support/ns_owner_inventory.h"

#include <string.h>


/*
 * Bridge contracts and pending-state invariants: stream mapping, inbound
 * retry retention, control-channel topology, deferred close, terminal facts,
 * and the budgeted service entry. Each test builds a bridge over a fake
 * endpoint and exercises one scenario.
 */

/* -- Helpers -------------------------------------------------------- */

typedef struct {
    moq_session_t *client;
    moq_session_t *server;
    fake_endpoint_t client_ep;
    fake_endpoint_t server_ep;
    moq_transport_bridge_t *client_bridge;
    moq_transport_bridge_t *server_bridge;
} test_pair_t;


/*
 * Subscribed client with its single event slot already occupied, plus the
 * server-side subgroup whose wire bytes the per-family tests deliver.
 *
 * The occupied slot is the client's own SUBSCRIBE_OK, deliberately left
 * unpolled: it is what makes the next session call that needs to emit return
 * WOULD_BLOCK, which is how each inbound pending flag is established for real
 * rather than written by hand.
 *
 * hdr holds the subgroup header alone (parsing it emits nothing, so it lands a
 * stream entry with no pending flag); obj additionally carries one object,
 * which cannot emit against a full queue.
 */
typedef struct {
    test_pair_t tp;
    uint64_t    sid;                  /* peer uni stream id on the client */
    uint8_t     hdr[512]; size_t hdr_len;
    uint8_t     obj[512]; size_t obj_len;
} uni_pending_fixture_t;


/*
 * Durable teardown obligation: the bridge's own retry must complete the
 * obligation the refused call created, not silently discard it.
 *
 * Extra bytes on a publisher-side namespace-sub bidi tear that bidi down
 * (session_namespace_sub.c:1078-1080 -> ns_sub_local_teardown), and the
 * teardown can refuse for action capacity before recording anything durable
 * (:667-668). Bridge ingress correctly recognises the owner and retains the
 * retry (transport_bridge.c:2662-2669) -- so the stream-ownership predicate
 * (moq_session_has_transport_stream) answers correctly here, which this
 * fixture also pins. Service then retries with NULL/0 (:1934-1949); the
 * session's durable teardown marker makes that empty retry complete the
 * teardown, so pending_retry clears only once the abort
 * has dispatched.
 *
 * Recovery is SERVICE-ONLY: the bridge never re-delivers peer bytes, so the
 * fixture feeds none after the refusal.
 */
/* -- Owner-graph and inventory oracles for the ns_sub bidi ----------- */

/* Exact drain-ring membership: the declared refs, each with its declared
 * reason, and no others. */
typedef struct drain_spec { uint64_t ref; moq_drain_reason_t reason; } drain_spec_t;

#ifdef MOQ_BRIDGE_TEST_INJECT


extern void moq_bridge_test_fail_retain_after(unsigned n);

#endif


/* -- Axis 4: physical bridge retirement ------------------------------
 *
 * SEMANTIC consumption and PHYSICAL retirement are different facts, asserted
 * separately. The causal sequence is the Axis 4 one: the peer FIN arrives WITH
 * the request, so the destination owner is created with the FIN transferred
 * onto it; the application then retires that owner, which -- the FIN having
 * been observed -- owes NO drain; and the bridge's own service must afterwards
 * close our half and retire the physical mapping exactly once.
 *
 * Successful FIN ingress records `peer_send_closed` and leaves the entry ACTIVE
 * until the local half closes (transport_bridge.c:2677), so retirement is a
 * real transition, not a formality.
 *
 * No peer bytes follow the FIN. The only continuation is bridge service.
 */

typedef struct fin_bridge_run {
    test_pair_t tp;
    uint64_t    transport_id;   /* the TRANSPORT stream the peer bytes ride */
    moq_stream_ref_t ref;       /* the SESSION's internal ref -- a DIFFERENT
                                 * value, captured once at ingress and never
                                 * re-derived from an entry that may be gone */
    int         want_slot;
    uint32_t    want_gen;
    uint64_t    want_handle;
} fin_bridge_run_t;


/* The owner record plus the bytes it owns. The raw compare is SHALLOW by
 * itself -- it would see a moved `track_id_buf` pointer but not edited bytes --
 * so the identity buffer is deep-copied alongside it. Bounds-safe: an
 * over-long or unbacked buffer makes the record incomparable rather than being
 * read. */
#define P7_OWN_MAX 256

typedef struct p7_snap {
    int      valid;
    moq_ts_entry_t raw;
    size_t   tid_len;
    uint8_t  tid[P7_OWN_MAX];
} p7_snap_t;


_Static_assert(sizeof(p7_snap_t) <= FIN_BR_SNAP_MAX,
               "p7 snapshot exceeds the shared bounded storage");


/*
 * The runner. Every hook the descriptor declares is CONSUMED: the family
 * bundle derives and identifies the owner, `capture`/`check` pin its record
 * across the EVENT POLL, and both normalizers build the images compared
 * against the declared output. Repeated bridge service is legal; post-FIN peer
 * bytes are not, and none are ever fed.
 */
/* The four phases the bridge entry passes through. */
typedef enum fin_br_phase {
    FIN_BR_AFTER_INGRESS = 0,
    FIN_BR_AFTER_TERMINAL,
    FIN_BR_AFTER_SERVICE,
    FIN_BR_AFTER_RESERVICE
} fin_br_phase_t;



/* One framed REQUEST_ERROR with non-default code, retry interval and reason. */
#define LSTOP_ERR_CODE   0x3u

#define LSTOP_ERR_RETRY  4200u

#define LSTOP_ERR_REASON "no such track"



/* -- exact endpoint-output oracle ---------------------------------------
 *
 * A phase declares the COMPLETE list of endpoint operations it may produce,
 * in order. Everything the fake endpoint holds is classified against that
 * list and then consumed, so an extra record -- of any kind, on any stream --
 * fails the phase that produced it rather than surviving into the next.
 */
typedef struct {
    fake_op_kind_t kind;
    uint64_t       stream_id;
    bool           has_code;
    uint64_t       error_code;
    bool           check_fin;
    bool           fin;
    const uint8_t *data;      /* NULL = payload not compared */
    size_t         data_len;
} ep_rec_t;


#define P3_OWN_MAX 256

typedef struct p3_snap {
    int      valid;
    moq_pub_entry_t raw;
    size_t   tid_len;
    uint8_t  tid[P3_OWN_MAX];
} p3_snap_t;


_Static_assert(sizeof(p3_snap_t) <= FIN_BR_SNAP_MAX,
               "p3 snapshot exceeds the shared bounded storage");


/* Local drain-multiset snapshot (order-insensitive), mirroring the direct
 * suite's contract: every expected ring is DERIVED from declared members. */
#define NOB_RING_MAX 256

typedef struct nob_ring {
    size_t   count;
    uint64_t ref[NOB_RING_MAX];
    uint8_t  reason[NOB_RING_MAX];
    int      overflow;
} nob_ring_t;


/* ================= per-stream head-of-line: acceptance battery =============
 * The bridge's documented service ordering blocks the drain of ALL new session
 * actions while the outbound pending queue is non-empty. g192 showed the cost:
 * one parked unidirectional open froze every other stream on the connection.
 * These fixtures pin what must hold before and after narrowing that scope to
 * per-stream. Sans-I/O: one publisher session over a fake endpoint, no peer
 * feedback needed, no wall clock, exact operation-count oracles.
 */
typedef struct {
    test_pair_t        tp;
    moq_subscription_t ssub;
} hol_fix_t;


/* -- Phase A: close intent, conservation, and the seeded explorer --------- */

/* Counting allocator: the conservation oracle. Every allocation the bridge and
 * session make on this path must be released, so `live` returns to zero once
 * both are destroyed. A leak, a double free (negative), or a stranded pending
 * item all show up here. */
typedef struct {
    int64_t live_bytes;
    int64_t live_blocks;
    int64_t frees;
    int64_t allocs;
    bool    negative;      /* sticky: a free without a matching alloc */
} hol_acct_t;

#ifdef MOQ_BRIDGE_SCAN_COUNTERS

#ifdef MOQ_BRIDGE_TEST_INJECT


/* -- complexity: operation counts at pending depths 0, 1 and deep --------- */
extern bool moq_bridge_test_inject_pending(moq_transport_bridge_t *b, int kind,
                                           uint64_t ref, uint64_t sid);

#endif


#endif

#ifdef MOQ_BRIDGE_TEST_INJECT


/* ================= the enrolled action/domain table ======================
 * Every enqueued pending kind, its domain class, where its identity comes
 * from, and what it blocks. Driven through the real enqueue boundary and the
 * real classifier via the test-only seams, so a future kind that falls to the
 * default case is caught here rather than in the field.
 */
extern bool moq_bridge_test_inject_pending(moq_transport_bridge_t *b, int kind,
                                           uint64_t ref, uint64_t sid);

extern bool moq_bridge_test_domain_blocked(moq_transport_bridge_t *b,
                                           uint64_t ref, uint64_t sid,
                                           bool have_sid);

extern uint32_t moq_bridge_test_pending_depth(const moq_transport_bridge_t *b);

extern uint64_t moq_bridge_test_ref_for_id(moq_transport_bridge_t *b,
                                          uint64_t sid);

extern void moq_bridge_test_fail_retain_after(unsigned n);

extern int moq_bridge_test_pending_domain(const moq_transport_bridge_t *b,
                                          uint32_t idx);

extern void moq_bridge_test_pending_identity(const moq_transport_bridge_t *b,
                                             uint32_t idx, uint64_t *out_ref,
                                             uint64_t *out_sid,
                                             bool *out_have_sid);


extern bool moq_bridge_test_inject_pending_ex(moq_transport_bridge_t *b,
                                              int kind, uint64_t ref,
                                              uint64_t sid, bool have_sid);

#endif

typedef struct {
    moq_stream_ref_t stream_ref;
    uint8_t header[32];
    uint8_t header_len;
    moq_rcbuf_t *payload;
    bool fin;
} priority_legacy_send_data_t;

_Static_assert(offsetof(moq_send_data_action_t, scheduling_priority) >=
               sizeof(priority_legacy_send_data_t), "priority overlaps legacy padding");

_Static_assert(sizeof(moq_send_data_action_t) <= MOQ_ACTION_DETAIL_MAX,
               "priority changes the action union ABI");


/* -- Held peer request streams (MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT) ------ *
 *
 * White-box rows on a draft-22 server session whose every stream mapping is
 * occupied (two mappings, two hold records), with both control channels
 * mapped so no entry is reserved for either. A new peer request stream finds no
 * mapping: it is held, offered oldest first when a mapping frees, refused
 * for the adapter to cancel when every record is in use, and cancelled with
 * EXCESSIVE_LOAD when it reaches the five-second hold age. The hold age is
 * reported through the session's next deadline, which a direct session tick
 * does not consume. */

typedef struct {
    moq_session_t          *s;
    fake_endpoint_t         ep;
    moq_transport_bridge_t *b;
} hb_rig_t;

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

extern const moq_alloc_t *g_server_alloc_override;

extern uint32_t g_client_rx_capacity;

extern const fin_bridge_family_t p7_family;

int test_pair_init_full(test_pair_t *tp, uint32_t client_max_events,
                        bool client_streaming,
                        uint32_t client_max_actions,
                        uint64_t client_idle_us,
                        uint32_t server_max_actions);

int test_pair_init_stream(test_pair_t *tp, uint32_t client_max_events,
                          bool client_streaming);

int test_pair_init_pub_cap(test_pair_t *tp, uint32_t server_max_actions);

int test_pair_init_ex(test_pair_t *tp, uint32_t client_max_events);

int test_pair_init(test_pair_t *tp);

void test_pair_destroy(test_pair_t *tp);

int pump_until_quiescent(test_pair_t *tp, int max, uint64_t now);

bool setup_handshake(test_pair_t *tp);

int d18_pair_init_caps(test_pair_t *tp, uint32_t client_max_events,
                       const moq_alloc_t *client_alloc,
                       const moq_alloc_t *server_alloc,
                       uint32_t server_max_actions,
                       bool server_native_abort,
                       uint32_t server_max_events);

int d18_pair_init_alloc(test_pair_t *tp, uint32_t client_max_events,
                        const moq_alloc_t *client_alloc,
                        const moq_alloc_t *server_alloc);

int d18_pair_init(test_pair_t *tp, uint32_t client_max_events);

void d18_feed(moq_transport_bridge_t *to, fake_endpoint_t *from,
              uint64_t uni_base, uint64_t bidi_base, uint64_t now,
              size_t *delivered);

void d18_shuttle_until_quiescent(test_pair_t *tp, int max,
                                 uint64_t now);

moq_result_t d18_server_subscribe(test_pair_t *tp);

int uni_pending_fixture_init_ex(uni_pending_fixture_t *f,
                                bool streaming, size_t payload_len);

int uni_pending_fixture_init(uni_pending_fixture_t *f);

int d18_request_bidi_fixture(test_pair_t *tp, uint64_t *bidi_out);

int check_drain_membership(const moq_session_t *s,
                           const drain_spec_t *want, size_t n,
                           const char *what);

int d18_strict_shuttle(test_pair_t *tp, int max, uint64_t now,
                       const char *what);

int test_pair_init_client_alloc(test_pair_t *tp,
                                const moq_alloc_t *client_alloc);

int br_fatal_postcondition(moq_transport_bridge_t *b,
                           uint64_t probe_id, fp_alloc_state_t *fs,
                           const char *op);

int br_alloc_zeroed(const fp_alloc_state_t *fs, const char *op);

uint64_t br_ns_establish(test_pair_t *tp, int *failures_out);

size_t br_encode_d16_namespace(uint8_t *buf, size_t cap,
                               const char *field);

moq_result_t p7_feed(moq_transport_bridge_t *b, void *vctx,
                     uint64_t transport_id, bool fin);

moq_result_t p7_terminal(moq_transport_bridge_t *b, void *vctx,
                         uint64_t transport_id);

int run_fin_bridge(const fin_bridge_case_t *f, fin_bridge_run_t *r);

size_t encode_request_error(uint8_t *buf, size_t cap);

int ep_expect(fake_endpoint_t *ep, const ep_rec_t *want, size_t n,
              const char *what);

int ep_expect_none(fake_endpoint_t *ep, const char *what);

int check_mapping_gone(moq_transport_bridge_t *b, uint64_t sid,
                       moq_stream_ref_t ref, const char *what);

ep_rec_t ep_reset(uint64_t sid, uint64_t code);

void nob_ring_snap(const moq_session_t *s, nob_ring_t *d);

void nob_ring_plus(const nob_ring_t *base, moq_stream_ref_t extra,
                   moq_drain_reason_t reason, nob_ring_t *out);

int nob_ring_equals(const nob_ring_t *have, const nob_ring_t *want,
                    const char *what);

int hol_subscribe_accept(hol_fix_t *f);

int hol_init(hol_fix_t *f);

moq_result_t hol_open_sg(hol_fix_t *f, uint64_t group,
                         moq_subgroup_handle_t *out);

moq_result_t hol_write(hol_fix_t *f, moq_subgroup_handle_t sg,
                       uint64_t obj, size_t len);

void hol_service(hol_fix_t *f, int passes);

uint64_t hol_nth_open(const hol_fix_t *f, size_t n);

uint32_t hol_pending(const hol_fix_t *f);

uint32_t hol_retained(const hol_fix_t *f);

moq_alloc_t hol_acct_allocator(hol_acct_t *a);

int hol_budget_case(uint32_t cap, unsigned *out_first,
                    unsigned *out_offered, int *out_fail);

void priority_pump(test_pair_t *tp, bool d18, uint64_t now);

int d22_pair_init(test_pair_t *tp);

void hb_close(hb_rig_t *r);

moq_result_t hb_bidi(hb_rig_t *r, uint64_t id, uint64_t now);

size_t hb_ops(const fake_endpoint_t *ep, fake_op_kind_t kind,
              uint64_t id, uint64_t code);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* BRIDGE_TEST_INTERNAL_H */
