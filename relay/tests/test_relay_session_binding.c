/*
 * Real-session parity: the relay core driven by real moq_session_t pairs
 * over SimPair. Each connection is one SimPair — the client side is the
 * test peer (publisher / subscriber / watcher), the server side is the
 * relay's session, attached to the production relay binding
 * — the same translation layer the relay executable runs. Correctness
 * parity comes free: every peer is the same strict session engine the
 * conformance suite trusts, and the code under test is the shipped
 * binding, not a copy.
 *
 * Covered flows, each through real wire bytes: announce/accept, subscribe
 * coalescing (one upstream SUBSCRIBE for two downstreams), upstream
 * resolution, retained + live delivery with exact payload bytes, filters,
 * publish push, namespace discovery, track status, unsubscribe/linger,
 * close/retire, on draft-16 AND draft-18 profiles, plus a run-twice trace
 * hash check.
 */

#include <moq/relay/relay.h>

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif
#include "../src/bind/moqr_bind_auth.h"

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
static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* injected OOM at this attempt index */
    }
    if (a->fail_size != 0 && n == a->fail_size) {
        a->fail_size = 0;   /* one-shot: later cleanup allocs succeed */
        return NULL;
    }
    void *p = malloc(n);
    if (p) {
        a->allocs++;
        a->live += (long)n;
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* realloc failure: caller keeps the old buffer p */
    }
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
static void ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

/* -- rig: SimPair conns attached to the production binding ------------------ */

#define MAX_CONNS 8

typedef struct conn {
    bool           used;
    /* Test-only: when true, rig_cycle does not transport this pair, so the
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

/* 0 = default; a small value constrains the SimPair sessions' action queues so
 * a burst of publisher-side cancels backs up (cancel_namespace WOULD_BLOCK). */
static uint32_t g_test_max_actions = 0;

/* false = whole-object OBJECT_RECEIVED ingest (default); true creates the
 * relay-side (server) session with streaming_objects so the relay receives
 * OBJECT_CHUNK and ingests into OPEN records. Set before rig_connect. */
static bool g_test_server_streaming_objects = false;

/* true creates CLIENT sessions with streaming_objects so a subscriber observes
 * downstream OBJECT_CHUNK events (live-edge chunks, terminal RESET). Set before
 * rig_connect; live-edge tests use it to see the wire-visible chunk stream. */
static bool g_test_client_streaming_objects = false;

/* 0 = default; a small value constrains the binding's downstream subgroup slot
 * pool so a burst of distinct (group, subgroup) streams exhausts it. */
static uint32_t g_test_max_open_subgroups = 0;

/* 0 = default (8); a small value makes the log evict old groups quickly so the
 * eviction / subgroup-cleanup paths are exercised. */
static uint32_t g_test_log_max_groups = 0;

/* 0 = default (1024); the initial MAX_REQUEST_ID advertised by both sides. A
 * small value exhausts the d16 request-credit window quickly. */
static uint32_t g_test_initial_request_capacity = 0;

/* 0 = binding default; the d16 request-capacity auto-grant window. */
static uint32_t g_test_grant_window = 0;

/* 0 = session default; the session event-queue depth. Bulk fixtures that
 * burst hundreds of objects down one stream need the consumer's queue
 * deeper than a delivery pass, or the sim's inbound pause sticks. */
static uint32_t g_test_max_events = 0;

/* 0 = session default; the RELAY-side session's outgoing subgroup pool. A
 * small value makes the SESSION pool (not the bind slot table) the
 * downstream blocker. */
static uint32_t g_test_server_max_open_subgroups = 0;

/* 0 = default (500); the warm-track linger. A large value makes the window
 * between a last-sub retire and the warm transition span several pumps, so a
 * retire that armed the deadline with a stale clock (now_us == 0) warms
 * PREMATURELY while a correctly-clocked one still lingers. */
static uint32_t g_test_linger_us = 0;

static conn_t *
rig_connect(rig_t *r, moq_version_t version)
{
    int slot = -1;
    for (int i = 0; i < MAX_CONNS; i++) {
        if (!r->conns[i].used) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        return NULL;
    }
    conn_t *cn = &r->conns[slot];
    memset(cn, 0, sizeof(*cn));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc->vt;
    cfg.seed = 0x5EED0000u + (uint64_t)slot;
    cfg.version = version;
    cfg.max_actions = g_test_max_actions; /* 0 = session default */
    cfg.max_events = g_test_max_events;   /* 0 = session default */
    cfg.server_max_open_subgroups = g_test_server_max_open_subgroups;
    cfg.server_streaming_objects = g_test_server_streaming_objects;
    cfg.client_streaming_objects = g_test_client_streaming_objects;
    uint64_t init_cap = g_test_initial_request_capacity != 0
                            ? g_test_initial_request_capacity
                            : 1024;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = init_cap;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = init_cap;
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return NULL;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(r->bind, cn->rsess, version) != MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        return NULL;
    }
    cn->used = true;
    return cn;
}

/* One deterministic pump cycle: transport steps, then the production
 * binding does everything else (events -> core -> intents -> deliveries). */
static void
rig_cycle(rig_t *r)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        if (cn->frozen) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

static void
rig_pump(rig_t *r, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        rig_cycle(r);
    }
}

/* One cycle that transports every SimPair EXCEPT one, while still running the
 * real bind pump. Nothing about the session or bind path changes -- only which
 * existing pair is stepped -- so a connection's outbound queue can be held
 * full across pumps and the relay's writes to it genuinely meet WOULD_BLOCK. */
static void
rig_cycle_except(rig_t *r, const conn_t *skip)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used || cn == skip) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

/* Advance ONE pair's transport without running the bind pump, so several
 * publisher-side requests can be delivered to the relay and then drained by a
 * single bind pump. Needed to put two ordered intents in ONE pump batch. */
static void
rig_step_pair(rig_t *r, conn_t *cn)
{
    r->now += 1000;
    if (cn == NULL || !cn->used) {
        return;
    }
    (void)moq_simpair_advance_to(cn->sp, r->now);
    size_t steps = 0;
    (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
}

/* Exactly one bind pump, no transport stepping. */
static void
rig_bind_pump_once(rig_t *r)
{
    (void)moqr_bind_pump(r->bind, r->now);
}

static void
rig_pump_except(rig_t *r, const conn_t *skip, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        rig_cycle_except(r, skip);
    }
}

/* Optional auth hook installed into the rig's core (NULL = allow-all). Tests
 * set it (and, optionally, a context) before rig_create and clear it after. */
static moqr_authorize_fn g_test_authorize = NULL;
static void *g_test_authorize_ctx = NULL;
/* 0 = leave the cfg default; a tiny value forces grant_reserve to fail on the
 * byte budget so the reserve-capacity fail-closed path is exercised. */
static uint32_t g_test_grant_bytes = 0;

static moqr_result_t
rig_create_ex(rig_t *r, ca_t *a, uint32_t bind_dsubs, uint32_t bind_usubs,
              uint32_t bind_pubs, uint32_t core_intents)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    if (moqr_trace_create(&a->vt, 512, &r->trace) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = r->trace;
    cfg.log_budget.max_groups = g_test_log_max_groups != 0
                                    ? g_test_log_max_groups
                                    : 8;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = g_test_linger_us != 0 ? g_test_linger_us : 500;
    cfg.max_intents = core_intents;
    cfg.authorize = g_test_authorize;   /* NULL = allow-all (default)      */
    cfg.authorize_ctx = g_test_authorize_ctx;
    if (g_test_grant_bytes != 0) {
        cfg.grant_bytes = g_test_grant_bytes;
    }
    if (moqr_core_create(&cfg, &r->core) != MOQR_OK) {
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a->vt);
    bcfg.core = r->core;
    bcfg.max_conns = MAX_CONNS;
    bcfg.max_downstream_subs = bind_dsubs;
    bcfg.max_upstream_subs = bind_usubs;
    bcfg.max_publishes = bind_pubs;
    bcfg.max_open_subgroups = g_test_max_open_subgroups; /* 0 = default */
    bcfg.request_grant_window = g_test_grant_window;      /* 0 = default */
    if (moqr_bind_create(&bcfg, &r->bind) != MOQR_OK) {
        moqr_core_destroy(r->core);
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    return MOQR_OK;
}

static moqr_result_t
rig_create(rig_t *r, ca_t *a)
{
    return rig_create_ex(r, a, 0, 0, 0, 0);
}

static void
rig_destroy(rig_t *r)
{
    moqr_bind_destroy(r->bind);
    moqr_core_destroy(r->core);
    for (int i = 0; i < MAX_CONNS; i++) {
        if (r->conns[i].used) {
            moq_simpair_destroy(r->conns[i].sp);
            r->conns[i].used = false;
        }
    }
    moqr_trace_destroy(r->trace);
}

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
    int                done_count;         /* # of SUBSCRIBE_DONE events     */
    uint64_t           done_code;          /* SUBSCRIBE_DONE status_code    */
    int                ns_cancelled;       /* NAMESPACE_CANCELLED count     */
    uint64_t           ns_cancelled_code;  /* its error_code                */
    /* auth-reject observation */
    bool               session_closed;
    uint64_t           close_code;
    uint64_t           sub_error_code;
    size_t             sub_error_reason_len;
    bool               sub_error_reason_truncated;
    uint8_t            sub_error_reason[64];
    uint64_t           publish_error_code;
    uint64_t           ts_error_code;
    int                ns_rejected;
    uint64_t           ns_rejected_code;
    int                ns_sub_error;
    uint64_t           ns_sub_error_code;
    int                fetch_error;
    uint64_t           fetch_error_code;
    /* FETCH response capture */
    bool               fetch_ok;
    bool               fok_eot;             /* FETCH_OK End Of Track          */
    uint64_t           fok_end_group, fok_end_object;
    int                fetch_complete;      /* FETCH_COMPLETE (stream FIN)     */
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
    /* subgroup-stream churn observation: a (group, subgroup) that FIN'd and then
     * received another object means the relay closed and reopened the stream. */
    uint64_t           finished_gsg[16][2];  /* (group, subgroup) FIN'd       */
    int                finished_count;
    int                subgroup_finished;    /* total SUBGROUP_FINISHED       */
    bool               reopen_after_finish;  /* object on a FIN'd subgroup    */
    /* data-plane delivery oracle log (see dlv_rec_t) */
    dlv_rec_t          dlv[DLV_LOG_MAX];
    int                dlv_n;
    int                dlv_overflow;         /* deliveries beyond the cap     */
} peer_state_t;

static bool
sub_reason_eq(const peer_state_t *ps, const char *s)
{
    size_t len = strlen(s);
    return !ps->sub_error_reason_truncated &&
           ps->sub_error_reason_len == len &&
           (len == 0 || memcmp(ps->sub_error_reason, s, len) == 0);
}

static void
peer_drain(rig_t *r, conn_t *cn, peer_state_t *ps, bool auto_accept,
           bool accept_with_largest, uint64_t lg, uint64_t lo)
{
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                ps->up_subs++;
                ps->up_sub = ev->u.subscribe_request.sub; /* even if not accepted */
                if (auto_accept) {
                    moq_accept_subscribe_cfg_t cfg;
                    moq_accept_subscribe_cfg_init(&cfg);
                    cfg.has_largest = accept_with_largest;
                    cfg.largest_group = lg;
                    cfg.largest_object = lo;
                    R_CHECK(r, moq_session_accept_subscribe(
                                   cn->peer, ev->u.subscribe_request.sub,
                                   &cfg, r->now) == MOQ_OK);
                    ps->up_seen = true;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                ps->subscribe_errors++;
                ps->sub_error_code = ev->u.subscribe_error.error_code;
                ps->sub_error_reason_len = ev->u.subscribe_error.reason.len;
                if (ps->sub_error_reason_len > sizeof(ps->sub_error_reason)) {
                    ps->sub_error_reason_len = sizeof(ps->sub_error_reason);
                    ps->sub_error_reason_truncated = true;
                } else {
                    ps->sub_error_reason_truncated = false;
                }
                if (ps->sub_error_reason_len != 0) {
                    memcpy(ps->sub_error_reason,
                           ev->u.subscribe_error.reason.data,
                           ps->sub_error_reason_len);
                }
                break;
            case MOQ_EVENT_PUBLISH_ERROR:
                ps->publish_error = true;
                ps->publish_error_code = ev->u.publish_error.error_code;
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                ps->session_closed = true;
                ps->close_code = ev->u.closed.code;
                break;
            case MOQ_EVENT_NAMESPACE_REJECTED:
                ps->ns_rejected++;
                ps->ns_rejected_code = ev->u.namespace_rejected.error_code;
                break;
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                ps->namespace_accepted = true;
                break;
            case MOQ_EVENT_NS_SUB_ERROR:
                ps->ns_sub_error++;
                ps->ns_sub_error_code = ev->u.ns_sub_error.error_code;
                break;
            case MOQ_EVENT_FETCH_ERROR:
                ps->fetch_error++;
                ps->fetch_error_code = ev->u.fetch_error.error_code;
                break;
            case MOQ_EVENT_FETCH_OK:
                ps->fetch_ok = true;
                ps->fok_eot = ev->u.fetch_ok.end_of_track;
                ps->fok_end_group = ev->u.fetch_ok.end_group;
                ps->fok_end_object = ev->u.fetch_ok.end_object;
                break;
            case MOQ_EVENT_FETCH_COMPLETE:
                ps->fetch_complete++;
                break;
            case MOQ_EVENT_FETCH_GAP:
                if (ps->fgap_n <
                    (int)(sizeof(ps->fgap) / sizeof(ps->fgap[0]))) {
                    int i = ps->fgap_n++;
                    ps->fgap[i].kind = ev->u.fetch_gap.range_kind;
                    ps->fgap[i].group = ev->u.fetch_gap.group_id;
                    ps->fgap[i].object = ev->u.fetch_gap.object_id;
                }
                break;
            case MOQ_EVENT_FETCH_OBJECT:
                if (ps->fdlv_n <
                    (int)(sizeof(ps->fdlv) / sizeof(ps->fdlv[0]))) {
                    int i = ps->fdlv_n++;
                    ps->fdlv[i].group = ev->u.fetch_object.group_id;
                    ps->fdlv[i].subgroup = ev->u.fetch_object.subgroup_id;
                    ps->fdlv[i].object = ev->u.fetch_object.object_id;
                    ps->fdlv[i].datagram = ev->u.fetch_object.datagram;
                    const moq_rcbuf_t *fpl = ev->u.fetch_object.payload;
                    uint64_t plen = (uint64_t)moq_rcbuf_len(fpl);
                    uint32_t h = 2166136261u;   /* FNV-1a, order-sensitive */
                    if (fpl != NULL) {
                        const uint8_t *pd = moq_rcbuf_data(fpl);
                        for (uint64_t k = 0; k < plen; k++) {
                            h = (h ^ pd[k]) * 16777619u;
                        }
                    }
                    ps->fdlv[i].fp = fpl != NULL ? moq_rcbuf_data(fpl)[0] : 0u;
                    ps->fdlv[i].len = plen;
                    ps->fdlv[i].fp_all = h;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR:
                ps->tracks_error++;
                ps->tracks_error_code =
                    ev->u.subscribe_tracks_error.error_code;
                break;
            case MOQ_EVENT_NS_SUB_OK:
                ps->ns_sub_ok = true;
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                ps->subscribe_ok = true;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'S';
                }
                ps->has_largest = ev->u.subscribe_ok.has_largest;
                ps->largest_group = ev->u.subscribe_ok.largest_group;
                ps->largest_object = ev->u.subscribe_ok.largest_object;
                break;
            case MOQ_EVENT_SUBGROUP_FINISHED:
                ps->subgroup_finished++;
                if (ps->finished_count <
                    (int)(sizeof(ps->finished_gsg) /
                          sizeof(ps->finished_gsg[0]))) {
                    ps->finished_gsg[ps->finished_count][0] =
                        ev->u.subgroup_finished.group_id;
                    ps->finished_gsg[ps->finished_count][1] =
                        ev->u.subgroup_finished.subgroup_id;
                    ps->finished_count++;
                }
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                ps->objects++;
                for (int fi = 0; fi < ps->finished_count; fi++) {
                    if (ps->finished_gsg[fi][0] ==
                            ev->u.object_received.group_id &&
                        ps->finished_gsg[fi][1] ==
                            ev->u.object_received.subgroup_id) {
                        ps->reopen_after_finish = true;
                    }
                }
                ps->last_group = ev->u.object_received.group_id;
                ps->last_object = ev->u.object_received.object_id;
                ps->last_datagram = ev->u.object_received.datagram;
                ps->last_status = ev->u.object_received.status;
                ps->last_payload_null =
                    ev->u.object_received.payload == NULL;
                ps->last_len = 0;
                if (ev->u.object_received.payload != NULL) {
                    ps->last_byte = moq_rcbuf_data(
                        ev->u.object_received.payload)[0];
                    ps->last_len = moq_rcbuf_len(
                        ev->u.object_received.payload);
                }
                ps->last_props_len = 0;
                if (ev->u.object_received.properties != NULL) {
                    ps->last_props_len = moq_rcbuf_len(
                        ev->u.object_received.properties);
                    size_t keep = ps->last_props_len < sizeof(ps->last_props)
                                      ? ps->last_props_len
                                      : sizeof(ps->last_props);
                    memcpy(ps->last_props,
                           moq_rcbuf_data(ev->u.object_received.properties),
                           keep);
                }
                if (ps->dlv_n < DLV_LOG_MAX) {
                    dlv_rec_t *dr = &ps->dlv[ps->dlv_n++];
                    dr->sub = ev->u.object_received.sub._opaque;
                    dr->group = ev->u.object_received.group_id;
                    dr->subgroup = ev->u.object_received.subgroup_id;
                    dr->object_id = ev->u.object_received.object_id;
                    dr->payload_null =
                        ev->u.object_received.payload == NULL;
                    dr->fp = dr->payload_null
                                 ? 0u
                                 : moq_rcbuf_data(
                                       ev->u.object_received.payload)[0];
                    dr->status = (uint8_t)ev->u.object_received.status;
                    dr->eog = ev->u.object_received.end_of_group;
                    dr->datagram = ev->u.object_received.datagram;
                    dr->after_done = ps->done_seen;
                } else {
                    ps->dlv_overflow++;
                }
                break;
            case MOQ_EVENT_OBJECT_CHUNK: {
                /* Streaming-receive subscriber: count data chunks, and
                 * classify the object's terminal — NORMAL completes it, RESET is
                 * the wire-visible abandon the relay propagates downstream. */
                const moq_object_chunk_event_t *oc = &ev->u.object_chunk;
                if (oc->chunk != NULL && moq_rcbuf_len(oc->chunk) > 0) {
                    ps->chunks++;
                    ps->last_chunk_byte = moq_rcbuf_data(oc->chunk)[0];
                }
                if (oc->end) {
                    if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                        ps->obj_complete = true;
                        ps->objects++;
                    } else if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                        ps->saw_reset = true;
                        ps->last_reset_code = oc->error_code;
                    }
                }
                break;
            }
            case MOQ_EVENT_UNSUBSCRIBED:
                ps->unsubscribed++;
                break;
            case MOQ_EVENT_NAMESPACE_GONE:
                ps->ns_gone++;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'G';
                }
                break;
            case MOQ_EVENT_PUBLISH_OK:
                ps->publish_ok = true;
                break;
            case MOQ_EVENT_NAMESPACE_FOUND:
                ps->ns_found++;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'N';
                }
                {
                    const moq_namespace_t *sfx =
                        &ev->u.namespace_found.track_namespace_suffix;
                    if (sfx->count > 0 && sfx->parts[0].len > 0 &&
                        ps->ns_id_len < (int)sizeof(ps->ns_ids) - 1) {
                        ps->ns_ids[ps->ns_id_len++] =
                            (char)sfx->parts[0].data[0];
                    }
                    ps->ns_seen_count = sfx->count;
                    ps->ns_seen_total = 0;
                    for (size_t pi = 0; pi < sfx->count && pi < 8; pi++) {
                        ps->ns_seen_lens[pi] = sfx->parts[pi].len;
                        for (size_t bi = 0; bi < sfx->parts[pi].len &&
                                            ps->ns_seen_total < 64; bi++) {
                            ps->ns_seen_bytes[ps->ns_seen_total++] =
                                sfx->parts[pi].data[bi];
                        }
                    }
                }
                break;
            case MOQ_EVENT_TRACK_STATUS_OK:
                ps->ts_ok = true;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'T';
                }
                ps->ts_largest_group =
                    ev->u.track_status_ok.largest_group;
                break;
            case MOQ_EVENT_TRACK_STATUS_ERROR:
                ps->ts_error++;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'E';
                }
                ps->ts_error_code = ev->u.track_status_error.error_code;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                ps->done_seen = true;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'D';
                }
                ps->done_count++;
                ps->done_code = ev->u.subscribe_done.status_code;
                break;
            case MOQ_EVENT_NAMESPACE_CANCELLED:
                ps->ns_cancelled++;
                ps->ns_cancelled_code = ev->u.namespace_cancelled.error_code;
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* Transport-step every connection once with NO bind pump — used to stage
 * multiple inbound events into one subsequent bind pump. */
static void
rig_transport_only(rig_t *r)
{
    for (int i = 0; i < MAX_CONNS; i++) {
        if (r->conns[i].used) {
            (void)moq_simpair_advance_to(r->conns[i].sp, r->now + 1);
            size_t st = 0;
            (void)moq_simpair_run_until_quiescent(r->conns[i].sp, 64, &st);
        }
    }
}

/* -- the parity test ------------------------------------------------------------ */

static int
parity_flow(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (version %d)\n", (int)version);
        return 1;
    }
    int failures = 0;

    conn_t *pub = rig_connect(&rig, version);
    conn_t *sub1 = rig_connect(&rig, version);
    conn_t *sub2 = rig_connect(&rig, version);
    conn_t *watch = rig_connect(&rig, version);
    R_CHECK(&rig, pub && sub1 && sub2 && watch);
    peer_state_t pub_ps, s1_ps, s2_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s1_ps, 0, sizeof(s1_ps));
    memset(&s2_ps, 0, sizeof(s2_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    rig_pump(&rig, 4);   /* handshakes */

    /* Watcher subscribes to the namespace prefix BEFORE the announce. */
    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = 1 };
    /* Portable across drafts: draft-18's SUBSCRIBE_NAMESPACE carries no
     * interest field and the profile requires NAMESPACE_STATE exactly;
     * draft-16 accepts it too. */
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg,
                                                  rig.now, &nsh) == MOQ_OK);
    rig_pump(&rig, 4);

    /* Publisher announces. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ns_found == 1);   /* discovery through real wire */

    /* Two subscribers subscribe before the publisher answers: coalescing
     * must produce exactly ONE upstream SUBSCRIBE at the publisher. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1h, s2h;
    R_CHECK(&rig, moq_session_subscribe(sub1->peer, &scfg, rig.now,
                                        &s1h) == MOQ_OK);
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &scfg, rig.now,
                                        &s2h) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, true, 0, 2);   /* accept, largest {0,2} */
    R_CHECK(&rig, pub_ps.up_seen);
    R_CHECK(&rig, pub_ps.up_subs == 1);   /* coalesced: EXACTLY one */
    rig_pump(&rig, 4);
    peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.subscribe_ok && s2_ps.subscribe_ok);
    R_CHECK(&rig, s1_ps.has_largest && s1_ps.largest_group == 0 &&
                      s1_ps.largest_object == 2);

    /* Publisher writes objects on the relay's upstream subscription. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 1;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                            rig.now, &sgh) == MOQ_OK);
    for (uint64_t o = 0; o < 3; o++) {
        uint8_t body[64];
        memset(body, (uint8_t)(0xA0 + o), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(pub->peer, sgh, o, pl,
                                               rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.objects == 3 && s2_ps.objects == 3);
    R_CHECK(&rig, s1_ps.last_byte == 0xA2);   /* exact bytes end-to-end */

    /* Forward-latency is observed on each successful downstream write: the
     * histogram count tracks deliveries_written exactly (every DELIVERED,
     * none on WOULD_BLOCK/STREAM_ERROR) and is nonzero once objects flow. */
    {
        moqr_bind_stats_t bstats;
        moqr_bind_get_stats(rig.bind, &bstats);
        R_CHECK(&rig, bstats.forward_latency.count > 0);
        R_CHECK(&rig, bstats.forward_latency.count ==
                          bstats.deliveries_written);
    }

    /* Datagram-preference object WITH properties relays intact. */
    {
        uint8_t body[32];
        /* One valid property entry, identical wire form in both drafts:
         * type 2 (even => varint value), value 9. Single-byte varints
         * 0-127 encode literally (vi64.h length table; kvp.h even-type
         * rule), so the block is exactly {0x02, 0x09}. */
        uint8_t props[2] = { 0x02, 0x09 };
        memset(body, 0xD7, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_send_object_datagram(
                          pub->peer, pub_ps.up_sub, 1, 10, 90, false, pl,
                          props, sizeof(props), rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 6);
    memset(&s1_ps, 0, sizeof(s1_ps));
    peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.objects == 1);
    R_CHECK(&rig, s1_ps.last_datagram);
    R_CHECK(&rig, s1_ps.last_byte == 0xD7);
    {
        const uint8_t want_props[2] = { 0x02, 0x09 };
        R_CHECK(&rig, s1_ps.last_props_len == sizeof(want_props) &&
                          memcmp(s1_ps.last_props, want_props,
                                 sizeof(want_props)) == 0);
    }

    /* Status-only datagram relays as a status datagram (payload NULL). */
    R_CHECK(&rig, moq_session_send_status_datagram(
                      pub->peer, pub_ps.up_sub, 1, 11, 90,
                      MOQ_OBJECT_END_OF_GROUP, rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    memset(&s1_ps, 0, sizeof(s1_ps));
    peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.objects == 1);
    R_CHECK(&rig, s1_ps.last_datagram);
    R_CHECK(&rig, s1_ps.last_payload_null);
    R_CHECK(&rig, s1_ps.last_status == MOQ_OBJECT_END_OF_GROUP);

    /* Track status answered from live relay state. */
    moq_track_status_cfg_t tsc;
    memset(&tsc, 0, sizeof(tsc));
    moq_track_status_cfg_init(&tsc);
    tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    tsc.track_name = B("video");
    moq_track_status_handle_t tsh;
    R_CHECK(&rig, moq_session_track_status(watch->peer, &tsc, rig.now,
                                           &tsh) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ts_ok && w_ps.ts_largest_group == 1);

    /* TRUE warm rejoin: every subscriber leaves, linger expires, the
     * relay unsubscribes upstream (the publisher observes it on the
     * wire), and a rejoining subscriber is served the retained group
     * while a fresh upstream SUBSCRIBE goes out. */
    R_CHECK(&rig, moq_session_unsubscribe(sub2->peer, s2h, rig.now) ==
                      MOQ_OK);
    R_CHECK(&rig, moq_session_unsubscribe(sub1->peer, s1h, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 6);   /* > linger_us (500) at 1000us per cycle */
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.unsubscribed == 1);   /* relay left upstream */
    R_CHECK(&rig, pub_ps.up_subs == 1);   /* and did NOT re-subscribe yet */

    moq_subscription_t s2h2;
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &scfg, rig.now,
                                        &s2h2) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, true, 1, 12);   /* re-subscribe */
    R_CHECK(&rig, pub_ps.up_subs == 2);   /* exactly one, from the rejoin */
    rig_pump(&rig, 8);
    memset(&s2_ps, 0, sizeof(s2_ps));
    peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);
    R_CHECK(&rig, s2_ps.subscribe_ok);
    R_CHECK(&rig, s2_ps.objects >= 3);   /* retained group replayed */

    /* Publish push ingest (both drafts carry PUBLISH). */
    moq_publish_cfg_t pubc;
    memset(&pubc, 0, sizeof(pubc));
    moq_publish_cfg_init(&pubc);
    moq_bytes_t nsp2[2] = { B("live"), B("cam2") };
    pubc.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    pubc.track_name = B("push");
    moq_publication_t pubh;
    moq_result_t prc = moq_session_publish(pub->peer, &pubc, rig.now,
                                           &pubh);
    R_CHECK(&rig, prc == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok);   /* accepted on the wire */

    /* Session close retires the publisher: announce withdrawn (watcher
     * sees NAMESPACE_GONE via real wire) and the track goes warm. */
    (void)moq_session_on_transport_close(pub->rsess, 0, rig.now);
    rig_pump(&rig, 6);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ns_gone == 1);   /* withdrawal seen on the wire */

    int f = rig.failures + failures;
    rig_destroy(&rig);
    if (a.live != 0) {
        printf("FAIL: allocator leak (version %d): %ld bytes\n",
               (int)version, a.live);
        f++;
    }
    if (f == 0) {
        printf("PASS: parity_flow(draft-%d)\n", (int)version);
    }
    return f;
}

static uint64_t
parity_hash_run(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        return 0;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub1 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    peer_state_t pub_ps, s1_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s1_ps, 0, sizeof(s1_ps));
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("h"), B("x") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    (void)moq_session_publish_namespace(pub->peer, &pcfg, rig.now, &ann);
    rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("t");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    (void)moq_session_subscribe(sub1->peer, &scfg, rig.now, &sh);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    rig_pump(&rig, 4);
    if (pub_ps.up_seen) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 1;
        moq_subgroup_handle_t sgh;
        if (moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                      rig.now, &sgh) == MOQ_OK) {
            uint8_t body[32];
            memset(body, 0x77, sizeof(body));
            moq_rcbuf_t *pl = NULL;
            (void)moq_rcbuf_create(&a.vt, body, sizeof(body), &pl);
            (void)moq_session_write_object(pub->peer, sgh, 0, pl, rig.now);
            moq_rcbuf_decref(pl);
            (void)moq_session_close_subgroup(pub->peer, sgh, rig.now);
        }
    }
    rig_pump(&rig, 8);
    peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    uint64_t h = moqr_trace_hash(rig.trace);
    if (s1_ps.objects != 1) {
        h = 0;   /* delivery failure collapses the hash: test fails */
    }
    rig_destroy(&rig);
    if (a.live != 0) {
        h = 0;
    }
    return h;
}

/* Undersized binding maps: the wire answer is a refusal, never a silent
 * orphan. Each dimension gets its own minimal rig. */
static int
small_caps_flow(void)
{
    int failures = 0;

    /* Downstream-sub cap: two subscribers to distinct published tracks on
     * ONE connection; dsubs=1 forces the second to REJECT. */
    {
        ca_t a;
        ca_init(&a);
        rig_t rig;
        R_CHECK(&rig, rig_create_ex(&rig, &a, 1, 0, 0, 0) == MOQR_OK);
        conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
        conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
        R_CHECK(&rig, pub && sub);
        rig_pump(&rig, 4);
        moq_bytes_t nsp[2] = { B("cap"), B("d") };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ nsp, 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg,
                                                    rig.now, &ann) ==
                          MOQ_OK);
        rig_pump(&rig, 4);
        peer_state_t pub_ps, sub_ps;
        memset(&pub_ps, 0, sizeof(pub_ps));
        memset(&sub_ps, 0, sizeof(sub_ps));
        moq_subscribe_cfg_t scfg;
        memset(&scfg, 0, sizeof(scfg));
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ nsp, 2 };
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        scfg.track_name = B("t1");
        moq_subscription_t h1;
        R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now,
                                            &h1) == MOQ_OK);
        rig_pump(&rig, 4);
        peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept t1 */
        rig_pump(&rig, 4);
        scfg.track_name = B("t2");
        moq_subscription_t h2;
        R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now,
                                            &h2) == MOQ_OK);
        rig_pump(&rig, 4);
        peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept t2 */
        rig_pump(&rig, 6);
        peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        R_CHECK(&rig, sub_ps.subscribe_ok);            /* t1 established */
        R_CHECK(&rig, sub_ps.subscribe_errors == 1);   /* t2 refused      */
        R_CHECK(&rig, sub_ps.sub_error_code ==
                          MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        R_CHECK(&rig, sub_reason_eq(&sub_ps, "internal error"));
        moqr_core_stats_t cs;
        moqr_core_get_stats(rig.core, &cs);
        R_CHECK(&rig, cs.subs == 1);   /* refused sub left no live state */
        failures += rig.failures;
        rig_destroy(&rig);
        R_CHECK(&rig, a.live == 0);
    }

    /* Publish cap: two PUBLISH pushes on one connection; pubs=1 forces the
     * second to REJECT on the wire. */
    {
        ca_t a;
        ca_init(&a);
        rig_t rig;
        R_CHECK(&rig, rig_create_ex(&rig, &a, 0, 0, 1, 0) == MOQR_OK);
        conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
        R_CHECK(&rig, pub != NULL);
        rig_pump(&rig, 4);
        moq_bytes_t nsp[2] = { B("cap"), B("p") };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ nsp, 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg,
                                                    rig.now, &ann) ==
                          MOQ_OK);
        rig_pump(&rig, 4);
        peer_state_t pub_ps;
        memset(&pub_ps, 0, sizeof(pub_ps));
        moq_publish_cfg_t pubc;
        memset(&pubc, 0, sizeof(pubc));
        moq_publish_cfg_init(&pubc);
        pubc.track_namespace = (moq_namespace_t){ nsp, 2 };
        pubc.track_name = B("p1");
        moq_publication_t ph1;
        R_CHECK(&rig, moq_session_publish(pub->peer, &pubc, rig.now,
                                          &ph1) == MOQ_OK);
        rig_pump(&rig, 4);
        pubc.track_name = B("p2");
        moq_publication_t ph2;
        R_CHECK(&rig, moq_session_publish(pub->peer, &pubc, rig.now,
                                          &ph2) == MOQ_OK);
        rig_pump(&rig, 4);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        R_CHECK(&rig, pub_ps.publish_ok);      /* first accepted     */
        R_CHECK(&rig, pub_ps.publish_error);   /* second refused     */
        failures += rig.failures;
        rig_destroy(&rig);
        R_CHECK(&rig, a.live == 0);
    }

    /* Upstream-sub cap: two subscriptions to distinct announced-but-
     * unpublished tracks; usubs=1 forces the second's upstream to fail,
     * surfacing as SUBSCRIBE_ERROR downstream (not a lost subscription). */
    {
        ca_t a;
        ca_init(&a);
        rig_t rig;
        R_CHECK(&rig, rig_create_ex(&rig, &a, 0, 1, 0, 0) == MOQR_OK);
        conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
        conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
        R_CHECK(&rig, pub && sub);
        rig_pump(&rig, 4);
        moq_bytes_t nsp[2] = { B("cap"), B("u") };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ nsp, 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg,
                                                    rig.now, &ann) ==
                          MOQ_OK);
        rig_pump(&rig, 4);
        peer_state_t sub_ps;
        memset(&sub_ps, 0, sizeof(sub_ps));
        moq_subscribe_cfg_t scfg;
        memset(&scfg, 0, sizeof(scfg));
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ nsp, 2 };
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        scfg.track_name = B("t1");
        moq_subscription_t h1;
        R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now,
                                            &h1) == MOQ_OK);
        rig_pump(&rig, 4);   /* t1 takes the one upstream slot (parks)    */
        scfg.track_name = B("t2");
        moq_subscription_t h2;
        R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now,
                                            &h2) == MOQ_OK);
        rig_pump(&rig, 6);   /* t2's upstream cannot be tracked -> ERROR  */
        peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        R_CHECK(&rig, sub_ps.subscribe_errors == 1);
        R_CHECK(&rig, sub_ps.sub_error_code ==
                          MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        R_CHECK(&rig, sub_reason_eq(&sub_ps, "internal error"));
        failures += rig.failures;
        rig_destroy(&rig);
        R_CHECK(&rig, a.live == 0);
    }

    if (failures == 0) {
        printf("PASS: small_caps_flow\n");
    }
    return failures;
}

/* Shared fixture for the ring-pressure tests. With max_subs=2,
 * max_ns_subs=1, max_ns_nodes=2 the ring clamps to 2 (the largest single
 * atomic reservation). We build: a watcher on prefix {"n"}, a publisher of
 * {"n"} with two subscribers parked on {"n"}/"t" pending the relay's
 * upstream, and capture that upstream unaccepted. Two drivers exercise it:
 *   - teardown: close the publisher. binding_close is resumable — it rejects
 *     the parked subs and fans out NS_GONE across as many retries as the
 *     2-slot ring needs; every withdrawal must still land.
 *   - ring-full: accept the captured upstream. That fans out 2 ACCEPT_SUB
 *     (== ring size) atomically, so a later-slot connection's control
 *     request in the SAME pump meets a full ring and must drain+retry
 *     rather than drop.
 */
typedef struct ring_arm {
    conn_t *watch;
    conn_t *pub;
    conn_t *s1;
    conn_t *s2;
    moq_subscription_t up;   /* relay's upstream subscription (captured) */
    bool               up_valid;
} ring_arm_t;

static moqr_result_t
ring_rig_create(rig_t *r, ca_t *a, uint32_t ns_nodes)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    if (moqr_trace_create(&a->vt, 512, &r->trace) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = r->trace;
    cfg.max_subs = 2;
    cfg.max_ns_subs = 1;
    cfg.max_ns_nodes = ns_nodes;
    cfg.max_intents = 1;   /* clamps up to the atomic-reservation min:
                            * max_subs + 1 (a track's terminals + its
                            * upstream release) == 3 here */
    cfg.log_budget.max_groups = 4;
    if (moqr_core_create(&cfg, &r->core) != MOQR_OK) {
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a->vt);
    bcfg.core = r->core;
    bcfg.max_conns = MAX_CONNS;
    if (moqr_bind_create(&bcfg, &r->bind) != MOQR_OK) {
        moqr_core_destroy(r->core);
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    /* Tight clamp: max(2, max_subs + 1, max_ns_subs, max_ns_nodes) = 3. */
    moqr_core_limits_t lim;
    moqr_core_get_limits(r->core, &lim);
    R_CHECK(r, lim.max_intents == 3);
    return MOQR_OK;
}

static void
ring_arm(rig_t *r, ring_arm_t *out)
{
    out->watch = rig_connect(r, MOQ_VERSION_DRAFT_18);
    out->pub = rig_connect(r, MOQ_VERSION_DRAFT_18);
    out->s1 = rig_connect(r, MOQ_VERSION_DRAFT_18);
    out->s2 = rig_connect(r, MOQ_VERSION_DRAFT_18);
    R_CHECK(r, out->watch && out->pub && out->s1 && out->s2);
    rig_pump(r, 4);

    moq_bytes_t pfx[1] = { B("n") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ pfx, 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(r, moq_session_subscribe_namespace(out->watch->peer, &nscfg,
                                               r->now, &nsh) == MOQ_OK);
    rig_pump(r, 4);

    moq_bytes_t nsp[1] = { B("n") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ nsp, 1 };
    moq_announcement_t ann;
    R_CHECK(r, moq_session_publish_namespace(out->pub->peer, &pcfg, r->now,
                                             &ann) == MOQ_OK);
    rig_pump(r, 4);   /* watcher gets NS_FOUND; drain it */
    peer_state_t w0;
    memset(&w0, 0, sizeof(w0));
    peer_drain(r, out->watch, &w0, false, false, 0, 0);
    R_CHECK(r, w0.ns_found == 1);

    conn_t *sc[2] = { out->s1, out->s2 };
    for (int i = 0; i < 2; i++) {
        moq_subscribe_cfg_t scfg;
        memset(&scfg, 0, sizeof(scfg));
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ nsp, 1 };
        scfg.track_name = B("t");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sh;
        R_CHECK(r, moq_session_subscribe(sc[i]->peer, &scfg, r->now, &sh) ==
                       MOQ_OK);
        rig_pump(r, 4);   /* park (upstream stays unanswered)             */
    }
    /* Capture the relay's upstream SUBSCRIBE at the publisher WITHOUT
     * accepting (subs stay parked). The close test ignores it; the
     * accept-fill tests resolve it to fan out 2 ACCEPT_SUB (== ring size). */
    out->up_valid = false;
    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(out->pub->peer, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !out->up_valid) {
                out->up = evs[e].u.subscribe_request.sub;
                out->up_valid = true;
            }
            moq_event_cleanup(&evs[e]);
        }
    }
    R_CHECK(r, out->up_valid);
}

/* Accept the captured upstream at the publisher peer, so the relay's
 * SUBSCRIBE_OK fans out ACCEPT_SUB to the parked subs. */
static void
ring_accept_upstream(rig_t *r, const ring_arm_t *arm)
{
    moq_accept_subscribe_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    moq_accept_subscribe_cfg_init(&cfg);
    R_CHECK(r, moq_session_accept_subscribe(arm->pub->peer, arm->up, &cfg,
                                            r->now) == MOQ_OK);
}

/* binding_close executes (never discards) pending intents even when its
 * fan-out exceeds the ring: the close drains and retries as needed, and the
 * closing publisher's parked-sub rejects + NS_GONE all reach their peers. */
static int
teardown_pending_intent_flow(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (ring_rig_create(&rig, &a, 2) != MOQR_OK) {
        printf("FAIL: teardown rig create\n");
        return 1;
    }
    ring_arm_t arm;
    ring_arm(&rig, &arm);

    (void)moq_session_on_transport_close(arm.pub->rsess, 0, rig.now);
    rig.now += 1000;
    (void)moqr_bind_pump(rig.bind, rig.now);
    rig_pump(&rig, 6);

    peer_state_t w, p1, p2;
    memset(&w, 0, sizeof(w));
    memset(&p1, 0, sizeof(p1));
    memset(&p2, 0, sizeof(p2));
    peer_drain(&rig, arm.watch, &w, false, false, 0, 0);
    peer_drain(&rig, arm.s1, &p1, false, false, 0, 0);
    peer_drain(&rig, arm.s2, &p2, false, false, 0, 0);
    R_CHECK(&rig, w.ns_gone == 1);            /* withdrawal delivered      */
    R_CHECK(&rig, p1.subscribe_errors == 1);  /* parked sub rejected       */
    R_CHECK(&rig, p2.subscribe_errors == 1);

    int f = rig.failures;
    rig_destroy(&rig);
    if (a.live != 0) {
        printf("FAIL: teardown leak: %ld\n", a.live);
        f++;
    }
    if (f == 0) {
        printf("PASS: teardown_pending_intent_flow\n");
    }
    return f;
}

/* Ring-full TRACK_STATUS_REQUEST must not be dropped: resolving the parked
 * subs fans out 2 ACCEPT_SUB (== ring size) in the same pump the asker's
 * status request is translated, so the request meets a full ring and must
 * drain+retry rather than drop. */
static int
ring_full_track_status_flow(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (ring_rig_create(&rig, &a, 2) != MOQR_OK) {
        printf("FAIL: track-status rig create\n");
        return 1;
    }
    ring_arm_t arm;
    ring_arm(&rig, &arm);
    conn_t *asker = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, asker != NULL);
    rig_pump(&rig, 2);

    /* Same pump: accept upstream (2 ACCEPT_SUB fill the ring) + asker
     * TRACK_STATUS. asker is a higher slot than the publisher. */
    ring_accept_upstream(&rig, &arm);
    moq_bytes_t nsp[1] = { B("n") };
    moq_track_status_cfg_t tsc;
    memset(&tsc, 0, sizeof(tsc));
    moq_track_status_cfg_init(&tsc);
    tsc.track_namespace = (moq_namespace_t){ nsp, 1 };
    tsc.track_name = B("t");
    moq_track_status_handle_t tsh;
    R_CHECK(&rig, moq_session_track_status(asker->peer, &tsc, rig.now,
                                           &tsh) == MOQ_OK);
    rig_transport_only(&rig);
    rig.now += 1000;
    (void)moqr_bind_pump(rig.bind, rig.now);
    rig_pump(&rig, 6);

    peer_state_t ask_ps;
    memset(&ask_ps, 0, sizeof(ask_ps));
    peer_drain(&rig, asker, &ask_ps, false, false, 0, 0);
    /* The track resolved ACTIVE, so the answer is TRACK_STATUS_OK — an
     * answer, not a dropped request. */
    R_CHECK(&rig, ask_ps.ts_ok);

    int f = rig.failures;
    rig_destroy(&rig);
    if (a.live != 0) {
        printf("FAIL: track-status ring leak: %ld\n", a.live);
        f++;
    }
    if (f == 0) {
        printf("PASS: ring_full_track_status_flow\n");
    }
    return f;
}

/* Ring-full PUBLISH_REQUEST must not be dropped: same fill, a pusher's
 * PUBLISH meets the full ring and is answered. */
static int
ring_full_publish_flow(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (ring_rig_create(&rig, &a, 2) != MOQR_OK) {
        printf("FAIL: publish rig create\n");
        return 1;
    }
    ring_arm_t arm;
    ring_arm(&rig, &arm);
    conn_t *pusher = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pusher != NULL);
    rig_pump(&rig, 2);

    ring_accept_upstream(&rig, &arm);
    moq_bytes_t nsp2[1] = { B("p") };
    moq_publish_cfg_t pubc;
    memset(&pubc, 0, sizeof(pubc));
    moq_publish_cfg_init(&pubc);
    pubc.track_namespace = (moq_namespace_t){ nsp2, 1 };
    pubc.track_name = B("push");
    moq_publication_t ph;
    R_CHECK(&rig, moq_session_publish(pusher->peer, &pubc, rig.now, &ph) ==
                      MOQ_OK);
    rig_transport_only(&rig);
    rig.now += 1000;
    (void)moqr_bind_pump(rig.bind, rig.now);
    rig_pump(&rig, 6);

    peer_state_t push_ps;
    memset(&push_ps, 0, sizeof(push_ps));
    peer_drain(&rig, pusher, &push_ps, false, false, 0, 0);
    R_CHECK(&rig, push_ps.publish_ok);   /* answered despite full ring */

    int f = rig.failures;
    rig_destroy(&rig);
    if (a.live != 0) {
        printf("FAIL: publish ring leak: %ld\n", a.live);
        f++;
    }
    if (f == 0) {
        printf("PASS: ring_full_publish_flow\n");
    }
    return f;
}

/* Shutdown safety: moqr_bind_destroy is strictly memory-only (no session
 * and no core calls). The transport (and its sessions) is destroyed FIRST,
 * with relay work still pending, then the binding — proving destroy never
 * reaches into a freed session. ASan-clean under this ordering is the
 * regression lock. */
static int
shutdown_no_session_flow(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: shutdown rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);
    moq_bytes_t nsp[1] = { B("n") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ nsp, 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    /* Drive a subscribe so intents (UPSTREAM_SUBSCRIBE / ACCEPT_SUB) are in
     * flight, then STOP pumping — leave work pending in the core ring. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ nsp, 1 };
    scfg.track_name = B("t");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_transport_only(&rig);
    (void)moqr_bind_pump(rig.bind, rig.now);   /* queues an upstream sub */

    /* Simulate adapter teardown: destroy the SimPairs (freeing all
     * sessions) BEFORE the binding. A binding that touched sessions in
     * destroy would use-after-free here (caught by ASan). */
    for (int i = 0; i < MAX_CONNS; i++) {
        if (rig.conns[i].used) {
            moq_simpair_destroy(rig.conns[i].sp);
            rig.conns[i].used = false;
        }
    }
    moqr_bind_destroy(rig.bind);   /* must make ZERO session calls */
    rig.bind = NULL;
    moqr_core_destroy(rig.core);
    rig.core = NULL;
    moqr_trace_destroy(rig.trace);
    rig.trace = NULL;

    if (a.live != 0) {
        printf("FAIL: shutdown leak: %ld\n", a.live);
        return 1;
    }
    printf("PASS: shutdown_no_session_flow\n");
    return 0;
}

/* Latency accounting records one observation per successful delivery even
 * when the delivery clock is BEHIND the record's arrival time — the latency
 * saturates to 0us instead of the observation being dropped, so
 * forward_latency.count stays equal to deliveries_written. The monotonic wire
 * flow can't deliver before ingest, so this ingests a FUTURE-arrival record
 * directly into an active track and delivers it through the binding at the
 * current (lower) clock. */
static int
test_latency_clock_regression(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: regression rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 4);

    /* Publish an ACTIVE track directly on the core (the upstream). */
    moqr_binding_t pubb;
    R_CHECK(&rig, moqr_core_binding_open(rig.core, 999, &pubb) == MOQR_OK);
    moq_bytes_t nsp[1] = { B("reg") };
    R_CHECK(&rig, moqr_core_announce(rig.core, pubb,
                                     (moqr_ns_t){ nsp, 1 }) == MOQR_OK);
    moqr_track_t track;
    R_CHECK(&rig, moqr_core_publish_open(rig.core, pubb,
                                         (moqr_ns_t){ nsp, 1 }, B("t"), 5,
                                         &track) == MOQR_OK);

    /* Subscriber subscribes; the track is already ACTIVE so it resolves. */
    moq_bytes_t snsp[1] = { B("reg") };
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = snsp, .count = 1 };
    scfg.track_name = B("t");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);

    /* Ingest one object whose arrival_us is far in the FUTURE. */
    uint8_t body[16];
    memset(body, 0xAB, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == 0);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 1;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 1;
    d.payload = pl;
    d.now_us = rig.now + 10000000;   /* 10s ahead of the delivery clock */
    if (moqr_core_ingest(rig.core, track, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        R_CHECK(&rig, 0);
    }

    /* Deliver at the current (lower) clock: the observation saturates to 0. */
    rig_pump(&rig, 8);

    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.deliveries_written >= 1);
    R_CHECK(&rig, bs.forward_latency.count == bs.deliveries_written);
    R_CHECK(&rig, bs.forward_latency.sum_us == 0);   /* all saturated to 0 */
    R_CHECK(&rig, bs.forward_latency.bucket[0] == bs.forward_latency.count);

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0) {
        printf("PASS: latency_clock_regression\n");
    }
    return f;
}

/* -- token-mirror seam: the auth token set is never silently truncated ----- */

#define AUTH_MIRROR_IDX      8u        /* first index past the old cap of 8  */
#define AUTH_MIRROR_SENTINEL 0xA5A5u   /* deciding token_type at that index  */

typedef struct {
    int    calls;
    size_t last_count;
    bool   decider_seen;   /* token_type at AUTH_MIRROR_IDX == sentinel      */
    bool   corrupt;        /* any mirrored token diverged from fill pattern  */
} auth_probe_t;

/* Recording hook: verifies the whole mirror matches fill_tokens() and ALLOWs
 * only when the deciding token at AUTH_MIRROR_IDX survived intact. */
static void
auth_probe_hook(void *ctx, const moqr_auth_request_t *req,
                moqr_auth_verdict_t *out)
{
    auth_probe_t *p = (auth_probe_t *)ctx;
    p->calls++;
    p->last_count = req->token_count;
    p->corrupt = false;
    for (size_t i = 0; i < req->token_count; i++) {
        uint64_t want = (i == AUTH_MIRROR_IDX) ? (uint64_t)AUTH_MIRROR_SENTINEL
                                               : (uint64_t)(1000 + i);
        if (req->tokens[i].token_type != want ||
            req->tokens[i].token_value.len != 1) {
            p->corrupt = true;
        }
    }
    p->decider_seen =
        req->token_count > AUTH_MIRROR_IDX &&
        req->tokens[AUTH_MIRROR_IDX].token_type == (uint64_t)AUTH_MIRROR_SENTINEL;
    if (p->decider_seen && !p->corrupt) {
        out->decision = MOQR_AUTH_ALLOW;
        out->reason = MOQR_AUTH_REASON_OK;
    } else {
        out->decision = MOQR_AUTH_DENY;
        out->reason = MOQR_AUTH_REASON_UNSCOPED;
    }
}

/* Distinct-typed resolved tokens; the deciding token at AUTH_MIRROR_IDX carries
 * the sentinel type, so the hook can ALLOW only if the mirror kept that index. */
static void
fill_tokens(moq_resolved_token_t *t, size_t count)
{
    for (size_t i = 0; i < count; i++) {
        t[i].token_type = (i == AUTH_MIRROR_IDX) ? (uint64_t)AUTH_MIRROR_SENTINEL
                                                 : (uint64_t)(1000 + i);
        t[i].token_value = B("v");
    }
}

static int
test_auth_token_mirror(void)
{
    ca_t a;
    ca_init(&a);
    auth_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    g_test_authorize = auth_probe_hook;
    g_test_authorize_ctx = &probe;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_test_authorize_ctx = NULL;
        printf("FAIL: token-mirror rig create\n");
        return 1;
    }

    moq_bytes_t nsp[1] = { B("ns") };
    moqr_ns_t ns = { nsp, 1 };
    moq_resolved_token_t toks[20];
    memset(toks, 0, sizeof(toks));
    moqr_auth_verdict_t v;
    bool consulted;

    /* (1) A deciding token past index 7 reaches the hook: 12 tokens, decider
     * at index 8. Truncating the set to 8 would drop it and force a spurious
     * DENY, so this guards against exactly that regression. */
    fill_tokens(toks, 12);
    memset(&probe, 0, sizeof(probe));
    consulted = moqr_bind_auth_eval(rig.core, MOQR_AUTH_SUBSCRIBE, ns, B("t"),
                                    toks, 12, 1, rig.now, &v);
    R_CHECK(&rig, consulted);
    R_CHECK(&rig, probe.calls == 1);
    R_CHECK(&rig, probe.last_count == 12);
    R_CHECK(&rig, probe.decider_seen);
    R_CHECK(&rig, !probe.corrupt);
    R_CHECK(&rig, v.decision == MOQR_AUTH_ALLOW);

    /* (2) Exactly the cap (16) is accepted and every token mirrored intact. */
    fill_tokens(toks, 16);
    memset(&probe, 0, sizeof(probe));
    consulted = moqr_bind_auth_eval(rig.core, MOQR_AUTH_SUBSCRIBE, ns, B("t"),
                                    toks, 16, 1, rig.now, &v);
    R_CHECK(&rig, consulted);
    R_CHECK(&rig, probe.last_count == 16);
    R_CHECK(&rig, !probe.corrupt);
    R_CHECK(&rig, v.decision == MOQR_AUTH_ALLOW);

    /* (3) One past the cap (17) fails closed WITHOUT consulting the hook. */
    fill_tokens(toks, 17);
    memset(&probe, 0, sizeof(probe));
    consulted = moqr_bind_auth_eval(rig.core, MOQR_AUTH_SUBSCRIBE, ns, B("t"),
                                    toks, 17, 1, rig.now, &v);
    R_CHECK(&rig, !consulted);
    R_CHECK(&rig, probe.calls == 0);            /* hook never saw a truncated set */
    R_CHECK(&rig, v.decision == MOQR_AUTH_DENY);

    /* (4) A positive count with a NULL array fails closed, hook untouched. */
    memset(&probe, 0, sizeof(probe));
    consulted = moqr_bind_auth_eval(rig.core, MOQR_AUTH_SUBSCRIBE, ns, B("t"),
                                    NULL, 5, 1, rig.now, &v);
    R_CHECK(&rig, !consulted);
    R_CHECK(&rig, probe.calls == 0);
    R_CHECK(&rig, v.decision == MOQR_AUTH_DENY);

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_token_mirror\n");
    }
    return f;
}

/* -- deny/defer matrix: every auth point fails closed on non-ALLOW --------- */

/* Rules on one target action (DENY or DEFER), allows every other — so the
 * setup handshake succeeds and exactly the action under test is refused. */
typedef struct {
    moqr_auth_action_t   target;
    moqr_auth_decision_t decision;
    int                  calls;
} auth_gate_t;

static void
auth_gate_hook(void *ctx, const moqr_auth_request_t *req,
               moqr_auth_verdict_t *out)
{
    auth_gate_t *g = (auth_gate_t *)ctx;
    g->calls++;
    if (req->action == g->target) {
        out->decision = g->decision;
        out->reason = MOQR_AUTH_REASON_POLICY;
    } else {
        out->decision = MOQR_AUTH_ALLOW;
        out->reason = MOQR_AUTH_REASON_OK;
    }
}

/* Drive one denied/deferred request through the real wire and prove: the peer
 * sees the action's specific rejection carrying UNAUTHORIZED, no persistent
 * core state was created, and the decision was counted under the right verdict
 * (never ALLOW). DENY and DEFER share the fail-closed path, so both run here. */
static int
run_deny_case(moqr_auth_action_t action, moqr_auth_decision_t decision,
              const char *name)
{
    ca_t a;
    ca_init(&a);
    auth_gate_t gate;
    memset(&gate, 0, sizeof(gate));
    gate.target = action;
    gate.decision = decision;
    g_test_authorize = auth_gate_hook;
    g_test_authorize_ctx = &gate;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_test_authorize_ctx = NULL;
        printf("FAIL: %s rig create\n", name);
        return 1;
    }
    conn_t *cn = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, cn != NULL);
    rig_pump(&rig, 4); /* setup handshake — ALLOWED (target != CLIENT_SETUP) */

    /* Baseline after the (allowed) handshake, before the refused op. "Core not
     * mutated" == counters unchanged from here, which also catches a node/sub
     * leaked on the reject path. */
    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_namespace_t ns = { .parts = nsp, .count = 2 };

    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        moq_publish_namespace_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_publish_namespace_cfg_init(&c);
        c.track_namespace = ns;
        moq_announcement_t h;
        R_CHECK(&rig, moq_session_publish_namespace(cn->peer, &c, rig.now,
                                                    &h) == MOQ_OK);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moq_subscribe_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_subscribe_cfg_init(&c);
        c.track_namespace = ns;
        c.track_name = B("v");
        c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        R_CHECK(&rig, moq_session_subscribe(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: {
        moq_bytes_t pfx[1] = { B("live") };
        moq_subscribe_namespace_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_subscribe_namespace_cfg_init(&c);
        c.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
        c.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        R_CHECK(&rig, moq_session_subscribe_namespace(cn->peer, &c, rig.now,
                                                      &h) == MOQ_OK);
        break;
    }
    case MOQR_AUTH_TRACK_STATUS: {
        moq_track_status_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_track_status_cfg_init(&c);
        c.track_namespace = ns;
        c.track_name = B("v");
        moq_track_status_handle_t h;
        R_CHECK(&rig, moq_session_track_status(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    case MOQR_AUTH_PUBLISH: {
        moq_publish_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_publish_cfg_init(&c);
        c.track_namespace = ns;
        c.track_name = B("v");
        moq_publication_t h;
        R_CHECK(&rig, moq_session_publish(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    default:
        R_CHECK(&rig, false); /* unreachable */
        break;
    }
    rig_pump(&rig, 8);

    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, cn, &ps, false, false, 0, 0);
    moqr_core_stats_t st;
    moqr_core_get_stats(rig.core, &st);

    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE:
        R_CHECK(&rig, ps.ns_rejected == 1);
        R_CHECK(&rig, ps.ns_rejected_code == MOQ_REQUEST_ERROR_UNAUTHORIZED);
        R_CHECK(&rig, st.ns_nodes == st0.ns_nodes); /* nothing announced */
        break;
    case MOQR_AUTH_SUBSCRIBE:
        R_CHECK(&rig, ps.subscribe_errors == 1);
        R_CHECK(&rig, ps.sub_error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED);
        R_CHECK(&rig, sub_reason_eq(&ps, "unauthorized"));
        R_CHECK(&rig, st.subs == st0.subs); /* no subscription created */
        break;
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
        R_CHECK(&rig, ps.ns_sub_error == 1);
        R_CHECK(&rig, ps.ns_sub_error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED);
        R_CHECK(&rig, st.ns_subs == st0.ns_subs); /* no namespace watcher */
        break;
    case MOQR_AUTH_TRACK_STATUS:
        R_CHECK(&rig, ps.ts_error == 1);
        R_CHECK(&rig, ps.ts_error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED);
        break;
    case MOQR_AUTH_PUBLISH:
        R_CHECK(&rig, ps.publish_error);
        R_CHECK(&rig, ps.publish_error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED);
        R_CHECK(&rig, st.tracks == st0.tracks); /* no publish track opened */
        break;
    default:
        break;
    }
    R_CHECK(&rig, st.auth_decisions[action][decision] >= 1);
    R_CHECK(&rig, st.auth_decisions[action][MOQR_AUTH_ALLOW] == 0);

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_deny %s\n", name);
    }
    return f;
}

/* Setup DENY/DEFER: the relay hard-closes with the session UNAUTHORIZED code
 * (0x2, distinct from the request-level 0x1), the CLOSE reaches the peer, and
 * the relay core binding is torn down. */
static int
run_setup_deny_case(moqr_auth_decision_t decision, const char *name)
{
    ca_t a;
    ca_init(&a);
    auth_gate_t gate;
    memset(&gate, 0, sizeof(gate));
    gate.target = MOQR_AUTH_CLIENT_SETUP;
    gate.decision = decision;
    g_test_authorize = auth_gate_hook;
    g_test_authorize_ctx = &gate;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_test_authorize_ctx = NULL;
        printf("FAIL: %s rig create\n", name);
        return 1;
    }
    conn_t *cn = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, cn != NULL);
    rig_pump(&rig, 8); /* setup completes on the relay -> DENY -> close */

    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, cn, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.session_closed);       /* CLOSE_SESSION reached the peer */
    R_CHECK(&rig, ps.close_code == 0x2u);   /* session UNAUTHORIZED, not 0x1  */
    moqr_core_stats_t st;
    moqr_core_get_stats(rig.core, &st);
    R_CHECK(&rig, st.bindings == 0);        /* relay binding torn down        */
    R_CHECK(&rig,
            st.auth_decisions[MOQR_AUTH_CLIENT_SETUP][decision] >= 1);

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_setup_deny %s\n", name);
    }
    return f;
}

static int
test_auth_deny_matrix(void)
{
    int f = 0;
    f += run_setup_deny_case(MOQR_AUTH_DENY, "setup/deny");
    f += run_setup_deny_case(MOQR_AUTH_DEFER, "setup/defer");
    f += run_deny_case(MOQR_AUTH_PUBLISH_NAMESPACE, MOQR_AUTH_DENY,
                       "announce/deny");
    f += run_deny_case(MOQR_AUTH_PUBLISH_NAMESPACE, MOQR_AUTH_DEFER,
                       "announce/defer");
    f += run_deny_case(MOQR_AUTH_SUBSCRIBE, MOQR_AUTH_DENY, "subscribe/deny");
    f += run_deny_case(MOQR_AUTH_SUBSCRIBE, MOQR_AUTH_DEFER, "subscribe/defer");
    f += run_deny_case(MOQR_AUTH_SUBSCRIBE_NAMESPACE, MOQR_AUTH_DENY,
                       "ns_sub/deny");
    f += run_deny_case(MOQR_AUTH_SUBSCRIBE_NAMESPACE, MOQR_AUTH_DEFER,
                       "ns_sub/defer");
    f += run_deny_case(MOQR_AUTH_TRACK_STATUS, MOQR_AUTH_DENY,
                       "track_status/deny");
    f += run_deny_case(MOQR_AUTH_TRACK_STATUS, MOQR_AUTH_DEFER,
                       "track_status/defer");
    f += run_deny_case(MOQR_AUTH_PUBLISH, MOQR_AUTH_DENY, "publish/deny");
    f += run_deny_case(MOQR_AUTH_PUBLISH, MOQR_AUTH_DEFER, "publish/defer");
    return f;
}

/* -- static-toy verifier: deterministic action×prefix policy --------------- */

static int
test_auth_toy_verifier(void)
{
    ca_t a;
    ca_init(&a);

    /* Policy: deny SUBSCRIBE under the "live" prefix, deny PUBLISH anywhere;
     * else the default (ALLOW). Rule order is significant — first match wins. */
    moq_bytes_t live[1] = { B("live") };
    moqr_auth_toy_rule_t rules[] = {
        { MOQR_AUTH_SUBSCRIBE, { live, 1 }, MOQR_AUTH_DENY,
          MOQR_AUTH_REASON_UNSCOPED },
        { MOQR_AUTH_PUBLISH, { NULL, 0 }, MOQR_AUTH_DENY,
          MOQR_AUTH_REASON_POLICY },
    };
    moqr_auth_toy_t toy = { rules, 2, MOQR_AUTH_ALLOW, MOQR_AUTH_REASON_OK };
    g_test_authorize = moqr_auth_toy_authorize;
    g_test_authorize_ctx = &toy;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_test_authorize_ctx = NULL;
        printf("FAIL: toy rig create\n");
        return 1;
    }

    moqr_auth_verdict_t v;
    moq_bytes_t livecam[2] = { B("live"), B("cam") };
    moq_bytes_t vod[1] = { B("vod") };

    /* SUBSCRIBE under live/ -> rule 0 -> DENY(UNSCOPED). */
    moqr_bind_auth_eval(rig.core, MOQR_AUTH_SUBSCRIBE,
                        (moqr_ns_t){ livecam, 2 }, B("t"), NULL, 0, 1, rig.now,
                        &v);
    R_CHECK(&rig, v.decision == MOQR_AUTH_DENY);
    R_CHECK(&rig, v.reason == MOQR_AUTH_REASON_UNSCOPED);

    /* SUBSCRIBE elsewhere -> no rule -> default ALLOW. */
    moqr_bind_auth_eval(rig.core, MOQR_AUTH_SUBSCRIBE, (moqr_ns_t){ vod, 1 },
                        B("t"), NULL, 0, 1, rig.now, &v);
    R_CHECK(&rig, v.decision == MOQR_AUTH_ALLOW);

    /* PUBLISH anywhere -> rule 1 (any prefix) -> DENY(POLICY). */
    moqr_bind_auth_eval(rig.core, MOQR_AUTH_PUBLISH, (moqr_ns_t){ vod, 1 },
                        B("t"), NULL, 0, 1, rig.now, &v);
    R_CHECK(&rig, v.decision == MOQR_AUTH_DENY);
    R_CHECK(&rig, v.reason == MOQR_AUTH_REASON_POLICY);

    /* Announce under live/ -> action mismatch on both rules -> default ALLOW.
     * (Same namespace as the denied SUBSCRIBE — proves the action is part of
     * the match, not just the prefix.) */
    moqr_bind_auth_eval(rig.core, MOQR_AUTH_PUBLISH_NAMESPACE,
                        (moqr_ns_t){ livecam, 2 }, (moq_bytes_t){ NULL, 0 },
                        NULL, 0, 1, rig.now, &v);
    R_CHECK(&rig, v.decision == MOQR_AUTH_ALLOW);

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_toy_verifier\n");
    }
    return f;
}

/* The toy contract is ALLOW/DENY only: a rule or default mis-authored with
 * DEFER must fail closed as DENY, never surface DEFER. */
static int
test_auth_toy_clamps_defer(void)
{
    int failures = 0;
    moqr_auth_toy_rule_t rules[] = {
        { MOQR_AUTH_SUBSCRIBE, { NULL, 0 }, MOQR_AUTH_DEFER,
          MOQR_AUTH_REASON_OK },
    };
    moqr_auth_toy_t toy = { rules, 1, MOQR_AUTH_DEFER, MOQR_AUTH_REASON_OK };

    moqr_auth_request_t req;
    memset(&req, 0, sizeof(req));
    req.struct_size = (uint32_t)sizeof(req);
    moqr_auth_verdict_t v;

    /* Hits the DEFER rule -> clamped to DENY. */
    req.action = MOQR_AUTH_SUBSCRIBE;
    memset(&v, 0, sizeof(v));
    moqr_auth_toy_authorize(&toy, &req, &v);
    if (v.decision != MOQR_AUTH_DENY) {
        printf("FAIL: toy DEFER rule not clamped (decision=%u)\n", v.decision);
        failures++;
    }

    /* No rule -> DEFER default -> clamped to DENY. */
    req.action = MOQR_AUTH_PUBLISH;
    memset(&v, 0, sizeof(v));
    moqr_auth_toy_authorize(&toy, &req, &v);
    if (v.decision != MOQR_AUTH_DENY) {
        printf("FAIL: toy DEFER default not clamped (decision=%u)\n",
               v.decision);
        failures++;
    }

    if (failures == 0) {
        printf("PASS: auth_toy_clamps_defer\n");
    }
    return failures;
}

/* -- deferred (DEFER) auth: park + resolve end-to-end --------------------- */

/* DEFER one target action with a fresh recorded ticket; allow everything else
 * (so setup completes). */
static moqr_auth_action_t g_defer_action = 0xFFFFFFFFu;
static uint64_t           g_defer_seq = 7000;
static uint64_t           g_defer_last = 0;
static uint64_t           g_defer_fixed = 0; /* nonzero => reuse this ticket */

static void
defer_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    (void)ctx;
    if (req->action == g_defer_action) {
        out->decision = MOQR_AUTH_DEFER;
        out->ticket = g_defer_fixed != 0 ? g_defer_fixed : ++g_defer_seq;
        g_defer_last = out->ticket;
    } else {
        out->decision = MOQR_AUTH_ALLOW;
        out->reason = MOQR_AUTH_REASON_OK;
    }
}

/* Publish an ACTIVE track directly on the rig core so a resumed subscribe can
 * land (a direct core call, so it bypasses the auth hook). */
static void
rig_publish(rig_t *r, uint64_t cookie, moqr_ns_t ns, moq_bytes_t name)
{
    moqr_binding_t pb;
    R_CHECK(r, moqr_core_binding_open(r->core, cookie, &pb) == MOQR_OK);
    R_CHECK(r, moqr_core_announce(r->core, pb, ns) == MOQR_OK);
    moqr_track_t tk;
    R_CHECK(r, moqr_core_publish_open(r->core, pb, ns, name, 5, &tk) == MOQR_OK);
}

/* Drive a subscribe that the hook DEFERs; return the parked ticket. Asserts the
 * request is parked (not rejected) and left no core subscription. */
static uint64_t
defer_a_subscribe(rig_t *rig, conn_t *sub, moq_subscription_t *sh)
{
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moqr_core_stats_t st0;
    moqr_core_get_stats(rig->core, &st0);
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    R_CHECK(rig, moq_session_subscribe(sub->peer, &sc, rig->now, sh) == MOQ_OK);
    g_defer_last = 0;
    rig_pump(rig, 6);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(rig, sub, &ps, false, false, 0, 0);
    R_CHECK(rig, ps.subscribe_errors == 0);   /* parked, not rejected */
    R_CHECK(rig, !ps.subscribe_ok);           /* not accepted yet     */
    moqr_core_stats_t st1;
    moqr_core_get_stats(rig->core, &st1);
    R_CHECK(rig, st1.subs == st0.subs);        /* no core sub on park  */
    return g_defer_last;
}

/* DEFER a subscribe, then resolve ALLOW -> it resumes and the peer is accepted;
 * a core subscription now exists. Also proves the request is NOT rejected and
 * creates NO core state while parked. */
static int
test_auth_defer_resume(void)
{
    ca_t a;
    ca_init(&a);
    g_defer_action = MOQR_AUTH_SUBSCRIBE;
    g_test_authorize = defer_hook;
    g_test_authorize_ctx = NULL;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_defer_action = 0xFFFFFFFFu;
        printf("FAIL: defer rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    rig_publish(&rig, 999, (moqr_ns_t){ nsp, 2 }, B("video"));
    rig_pump(&rig, 2);
    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    moq_subscription_t sh;
    uint64_t tkt = defer_a_subscribe(&rig, sub, &sh);
    R_CHECK(&rig, tkt != 0);

    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW },
                      rig.now) == MOQR_OK);
    rig_pump(&rig, 8);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.subscribe_ok);            /* resumed + accepted */
    moqr_core_stats_t st2;
    moqr_core_get_stats(rig.core, &st2);
    R_CHECK(&rig, st2.subs > st0.subs);        /* core sub created   */

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    g_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer_resume\n");
    }
    return f;
}

/* DEFER a subscribe, then resolve DENY with a custom wire code -> the peer is
 * rejected carrying exactly that code. */
static int
test_auth_defer_reject(void)
{
    ca_t a;
    ca_init(&a);
    g_defer_action = MOQR_AUTH_SUBSCRIBE;
    g_test_authorize = defer_hook;
    g_test_authorize_ctx = NULL;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_defer_action = 0xFFFFFFFFu;
        printf("FAIL: defer rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    rig_publish(&rig, 999, (moqr_ns_t){ nsp, 2 }, B("video"));
    rig_pump(&rig, 2);

    moq_subscription_t sh;
    uint64_t tkt = defer_a_subscribe(&rig, sub, &sh);
    R_CHECK(&rig, tkt != 0);

    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_DENY,
                                              .error_code = 0x20 },
                      rig.now) == MOQR_OK);
    rig_pump(&rig, 6);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.subscribe_errors == 1);
    /* UNINTERESTED (0x20): a code draft-18 assigns, so the peer reads it
     * verbatim instead of applying Section 15's unknown-code rule. */
    R_CHECK(&rig, ps.sub_error_code == 0x20);   /* custom code preserved */
    R_CHECK(&rig, sub_reason_eq(&ps, "uninterested"));

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    g_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer_reject\n");
    }
    return f;
}

/* Binding close before resolve retires the ticket; resolving it (or any
 * unknown ticket) is a stale no-op that touches no session. */
static int
test_auth_defer_stale(void)
{
    ca_t a;
    ca_init(&a);
    g_defer_action = MOQR_AUTH_SUBSCRIBE;
    g_test_authorize = defer_hook;
    g_test_authorize_ctx = NULL;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_defer_action = 0xFFFFFFFFu;
        printf("FAIL: defer rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    rig_publish(&rig, 999, (moqr_ns_t){ nsp, 2 }, B("video"));
    rig_pump(&rig, 2);

    moq_subscription_t sh;
    uint64_t tkt = defer_a_subscribe(&rig, sub, &sh);
    R_CHECK(&rig, tkt != 0);

    /* Close the connection: its parked ticket is retired. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, sub->rsess) == MOQR_OK);
    rig_pump(&rig, 2);

    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW },
                      rig.now) == MOQR_ERR_STALE_HANDLE);
    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, 424242,
                      &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW },
                      rig.now) == MOQR_ERR_STALE_HANDLE);

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    g_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer_stale\n");
    }
    return f;
}

/* A park failure (here a duplicate live ticket) must fail closed with a
 * DEFINITE wire code, never 0: a valid DEFER leaves the site's err at 0. */
static int
test_auth_defer_park_fail(void)
{
    ca_t a;
    ca_init(&a);
    g_defer_action = MOQR_AUTH_SUBSCRIBE;
    g_defer_fixed = 5000;   /* every DEFER reuses one ticket -> 2nd park dups */
    g_test_authorize = defer_hook;
    g_test_authorize_ctx = NULL;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_defer_action = 0xFFFFFFFFu;
        g_defer_fixed = 0;
        printf("FAIL: defer rig create\n");
        return 1;
    }
    conn_t *s1 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *s2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, s1 != NULL && s2 != NULL);
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    rig_publish(&rig, 999, (moqr_ns_t){ nsp, 2 }, B("video"));
    rig_pump(&rig, 2);

    /* s1 parks ticket 5000. */
    moq_subscription_t sh1;
    (void)defer_a_subscribe(&rig, s1, &sh1);

    /* s2's DEFER reuses ticket 5000 -> park duplicate -> reject fail-closed. */
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh2;
    R_CHECK(&rig, moq_session_subscribe(s2->peer, &sc, rig.now, &sh2) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, s2, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.subscribe_errors == 1);
    R_CHECK(&rig, ps.sub_error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED); /* not 0 */

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    g_defer_action = 0xFFFFFFFFu;
    g_defer_fixed = 0;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer_park_fail\n");
    }
    return f;
}

/* Park a DEFERred request of `action`, prove it did not reject or mutate core
 * state, then resolve with `decision`: ALLOW resumes the op (core state now
 * exists / peer answered), DENY rejects it carrying a custom wire code. Covers
 * all five request paths through the real binding. */
static int
run_defer_case(moqr_auth_action_t action, moqr_auth_decision_t decision,
               const char *name)
{
    ca_t a;
    ca_init(&a);
    g_defer_action = action;
    g_defer_fixed = 0;
    g_test_authorize = defer_hook;
    g_test_authorize_ctx = NULL;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_defer_action = 0xFFFFFFFFu;
        printf("FAIL: %s rig create\n", name);
        return 1;
    }
    conn_t *cn = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, cn != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("d"), B("x") };
    moq_namespace_t wns = { .parts = nsp, .count = 2 };
    /* SUBSCRIBE / TRACK_STATUS need a live track for an ALLOW to answer. */
    if (action == MOQR_AUTH_SUBSCRIBE || action == MOQR_AUTH_TRACK_STATUS) {
        rig_publish(&rig, 999, (moqr_ns_t){ nsp, 2 }, B("v"));
        rig_pump(&rig, 2);
    }
    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        moq_publish_namespace_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_publish_namespace_cfg_init(&c);
        c.track_namespace = wns;
        moq_announcement_t h;
        R_CHECK(&rig, moq_session_publish_namespace(cn->peer, &c, rig.now,
                                                    &h) == MOQ_OK);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moq_subscribe_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_subscribe_cfg_init(&c);
        c.track_namespace = wns;
        c.track_name = B("v");
        c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        R_CHECK(&rig, moq_session_subscribe(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: {
        moq_bytes_t pfx[1] = { B("d") };
        moq_subscribe_namespace_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_subscribe_namespace_cfg_init(&c);
        c.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
        c.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        R_CHECK(&rig, moq_session_subscribe_namespace(cn->peer, &c, rig.now,
                                                      &h) == MOQ_OK);
        break;
    }
    case MOQR_AUTH_TRACK_STATUS: {
        moq_track_status_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_track_status_cfg_init(&c);
        c.track_namespace = wns;
        c.track_name = B("v");
        moq_track_status_handle_t h;
        R_CHECK(&rig, moq_session_track_status(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    case MOQR_AUTH_PUBLISH: {
        moq_publish_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_publish_cfg_init(&c);
        c.track_namespace = wns;
        c.track_name = B("v");
        moq_publication_t h;
        R_CHECK(&rig, moq_session_publish(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    default:
        R_CHECK(&rig, false);
        break;
    }
    g_defer_last = 0;
    rig_pump(&rig, 6);
    uint64_t tkt = g_defer_last;
    R_CHECK(&rig, tkt != 0);

    /* Parked: nothing rejected, no persistent core state created. */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, cn, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.ns_rejected == 0 && ps.subscribe_errors == 0 &&
                      ps.ns_sub_error == 0 && ps.ts_error == 0 &&
                      !ps.publish_error);
    moqr_core_stats_t stp;
    moqr_core_get_stats(rig.core, &stp);
    R_CHECK(&rig, stp.ns_nodes == st0.ns_nodes && stp.subs == st0.subs &&
                      stp.ns_subs == st0.ns_subs && stp.tracks == st0.tracks);

    /* Resolve. */
    uint64_t custom = decision == MOQR_AUTH_DENY ? 0x20u : 0u;
    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = decision,
                                              .error_code = custom },
                      rig.now) == MOQR_OK);
    rig_pump(&rig, 8);
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, cn, &ps, false, false, 0, 0);
    moqr_core_stats_t st1;
    moqr_core_get_stats(rig.core, &st1);

    if (decision == MOQR_AUTH_ALLOW) {
        switch (action) {
        case MOQR_AUTH_PUBLISH_NAMESPACE:
            R_CHECK(&rig, st1.ns_nodes > st0.ns_nodes); /* core mutated */
            R_CHECK(&rig, ps.namespace_accepted);       /* AND wire-accepted */
            break;
        case MOQR_AUTH_SUBSCRIBE:
            R_CHECK(&rig, st1.subs > st0.subs);
            break;
        case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
            R_CHECK(&rig, st1.ns_subs > st0.ns_subs);
            R_CHECK(&rig, ps.ns_sub_ok);
            break;
        case MOQR_AUTH_TRACK_STATUS:
            R_CHECK(&rig, ps.ts_ok);
            break;
        case MOQR_AUTH_PUBLISH:
            R_CHECK(&rig, st1.tracks > st0.tracks);
            R_CHECK(&rig, ps.publish_ok);
            break;
        default:
            break;
        }
    } else {
        switch (action) {
        case MOQR_AUTH_PUBLISH_NAMESPACE:
            R_CHECK(&rig, ps.ns_rejected == 1 && ps.ns_rejected_code == 0x20);
            break;
        case MOQR_AUTH_SUBSCRIBE:
            R_CHECK(&rig, ps.subscribe_errors == 1 && ps.sub_error_code == 0x20);
            break;
        case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
            R_CHECK(&rig, ps.ns_sub_error == 1 && ps.ns_sub_error_code == 0x20);
            break;
        case MOQR_AUTH_TRACK_STATUS:
            R_CHECK(&rig, ps.ts_error == 1 && ps.ts_error_code == 0x20);
            break;
        case MOQR_AUTH_PUBLISH:
            R_CHECK(&rig, ps.publish_error && ps.publish_error_code == 0x20);
            break;
        default:
            break;
        }
    }

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    g_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer %s\n", name);
    }
    return f;
}

static int
test_auth_defer_matrix(void)
{
    int f = 0;
    f += run_defer_case(MOQR_AUTH_PUBLISH_NAMESPACE, MOQR_AUTH_ALLOW,
                        "announce/allow");
    f += run_defer_case(MOQR_AUTH_PUBLISH_NAMESPACE, MOQR_AUTH_DENY,
                        "announce/deny");
    f += run_defer_case(MOQR_AUTH_SUBSCRIBE_NAMESPACE, MOQR_AUTH_ALLOW,
                        "ns_sub/allow");
    f += run_defer_case(MOQR_AUTH_SUBSCRIBE_NAMESPACE, MOQR_AUTH_DENY,
                        "ns_sub/deny");
    f += run_defer_case(MOQR_AUTH_TRACK_STATUS, MOQR_AUTH_ALLOW,
                        "track_status/allow");
    f += run_defer_case(MOQR_AUTH_TRACK_STATUS, MOQR_AUTH_DENY,
                        "track_status/deny");
    f += run_defer_case(MOQR_AUTH_PUBLISH, MOQR_AUTH_ALLOW, "publish/allow");
    f += run_defer_case(MOQR_AUTH_PUBLISH, MOQR_AUTH_DENY, "publish/deny");
    return f;
}

/* -- CAT moqt-reval lease revalidation (grant lifecycle in the binding) ----- */

/* A controllable revalidation hook. Setup and every non-grantable action always
 * ALLOW with no lease; SUBSCRIBE / PUBLISH_NAMESPACE return the current decision
 * with a per-action lease, so a test creates a grant (ALLOW + lease) and then
 * flips the decision to observe the binding revoking it on the next due tick. */
static moqr_auth_decision_t g_rv_decision = MOQR_AUTH_ALLOW;
static uint64_t             g_rv_lease_sub = 0;
static uint64_t             g_rv_lease_ann = 0;
static uint32_t             g_rv_deny_code = 0; /* 0 => no custom DENY code   */

/* See the PUBLISH_NAMESPACE branch below. */
static bool g_rv_ann_always_allow = false;
/* NULL = the decision is global (g_rv_decision). When set, ONLY the
 * subscription whose track name matches is revoked, so independent
 * SUBSCRIBE_DONE terminals can be produced one at a time. */
static const char *g_rv_deny_track = NULL;
/* true = reval_make_subscribe_grant also subscribes a second track, so two
 * independent SUBSCRIBE_DONE terminals are available. */
static bool g_rv_second_track = false;

static void
rv_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    (void)ctx;
    if (req->action == MOQR_AUTH_SUBSCRIBE) {
        if (g_rv_deny_track != NULL) {
            size_t tl = strlen(g_rv_deny_track);
            bool hit = req->name.len == tl && req->name.data != NULL &&
                       memcmp(req->name.data, g_rv_deny_track, tl) == 0;
            out->decision = hit ? MOQR_AUTH_DENY : MOQR_AUTH_ALLOW;
        } else {
            out->decision = g_rv_decision;
        }
        out->revalidate_after_us = g_rv_lease_sub;
    } else if (req->action == MOQR_AUTH_PUBLISH_NAMESPACE) {
        /* Opt-in: keep announces allowed while a subscribe revocation is being
         * driven, so a test can revoke one subscription and still publish a
         * new namespace. Default false leaves every other test unchanged. */
        out->decision = g_rv_ann_always_allow ? MOQR_AUTH_ALLOW : g_rv_decision;
        out->revalidate_after_us = g_rv_lease_ann;
    } else {
        out->decision = MOQR_AUTH_ALLOW; /* setup + non-grantable actions */
        return;
    }
    if (out->decision == MOQR_AUTH_DENY && g_rv_deny_code != 0) {
        out->error_code = g_rv_deny_code;
        /* A revoked ACTIVE subscribe grant ends a subscription: that terminal
         * is PUBLISH_DONE-domain and must be stated as a tagged descriptor.
         * The scalar beside it stays REQUEST_ERROR-domain and is never read as
         * a status. */
        {
            moqr_pd_desc_t t;
            if (moqr_pd_desc_extension(g_rv_deny_code, &t) ==
                MOQR_OK) {
                out->revoke_terminal = t;
            }
        }
    }
}

static void
rv_reset(void)
{
    g_test_authorize = NULL;
    g_test_authorize_ctx = NULL;
    g_test_grant_bytes = 0;
    g_test_max_actions = 0;
    g_rv_decision = MOQR_AUTH_ALLOW;
    g_rv_lease_sub = 0;
    g_rv_lease_ann = 0;
    g_rv_deny_code = 0;
    g_rv_ann_always_allow = false;
    g_rv_deny_track = NULL;
    g_rv_second_track = false;
}

/* Announce (no grant when lease_ann==0) + subscribe (ALLOW + lease -> grant)
 * over real sessions; leaves the subscription accepted with a live grant. */
static void
reval_make_subscribe_grant(rig_t *rig, conn_t *pub, conn_t *sub,
                           peer_state_t *pub_ps, peer_state_t *sub_ps,
                           moq_bytes_t *nsp2)
{
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(rig, moq_session_publish_namespace(pub->peer, &pcfg, rig->now,
                                               &ann) == MOQ_OK);
    rig_pump(rig, 4);
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(rig, moq_session_subscribe(sub->peer, &scfg, rig->now, &sh) ==
                     MOQ_OK);
    if (g_rv_second_track) {
        rig_pump(rig, 4);   /* the first subscribe must clear the queue */
        moq_subscribe_cfg_t scfg2 = scfg;
        scfg2.track_name = B("audio");
        moq_subscription_t sh2;
        R_CHECK(rig, moq_session_subscribe(sub->peer, &scfg2, rig->now,
                                           &sh2) == MOQ_OK);
    }
    rig_pump(rig, 4);
    peer_drain(rig, pub, pub_ps, true, false, 0, 0); /* accept upstream */
    rig_pump(rig, 4);
    peer_drain(rig, sub, sub_ps, false, false, 0, 0);
    R_CHECK(rig, sub_ps->subscribe_ok); /* accepted: grant is live */
}

/* Subscribe ALLOW + lease creates a grant; flipping the hook to DENY (custom
 * code) revokes it on the next due tick, and the subscriber sees a wire
 * SUBSCRIBE_DONE carrying that code. */
static int
test_reval_subscribe_revoke(void)
{
    ca_t a;
    ca_init(&a);
    g_rv_lease_sub = 3000; /* announce carries no grant: isolate the sub */
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        printf("FAIL: reval_subscribe rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    reval_make_subscribe_grant(&rig, pub, sub, &pub_ps, &sub_ps, nsp);
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Flip to DENY (custom code) and let the lease elapse. */
    g_rv_decision = MOQR_AUTH_DENY;
    g_rv_deny_code = 0x7;
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.done_seen);        /* wire SUBSCRIBE_DONE */
    R_CHECK(&rig, sub_ps.done_code == 0x7); /* custom denial code preserved */

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: reval_subscribe_revoke\n");
    }
    return f;
}

/* Announce ALLOW + lease creates a grant; flipping the hook to `decision`
 * revokes it on the next due tick, withdrawing the namespace: the publisher
 * sees a wire NAMESPACE_CANCELLED carrying `expect_code`. */
static int
announce_reval_case(moq_version_t version, moqr_auth_decision_t decision,
                    uint32_t deny_code, uint64_t expect_code, const char *name)
{
    ca_t a;
    ca_init(&a);
    g_rv_lease_ann = 3000;
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        printf("FAIL: reval_announce %s rig create\n", name);
        return 1;
    }
    conn_t *pub = rig_connect(&rig, version);
    R_CHECK(&rig, pub != NULL);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted); /* accepted: grant is live */
    R_CHECK(&rig, pub_ps.ns_cancelled == 0);

    /* Flip and let the lease elapse -> revalidation revokes -> withdraw. */
    g_rv_decision = decision;
    g_rv_deny_code = deny_code;
    rig_pump(&rig, 8);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.ns_cancelled == 1);
    R_CHECK(&rig, pub_ps.ns_cancelled_code == expect_code);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: reval_announce_%s\n", name);
    }
    return f;
}

static int
test_reval_announce_revoke(void)
{
    int f = 0;
    /* draft-16 carries a REQUEST_ERROR code on PUBLISH_NAMESPACE_CANCEL, so the
     * relay's denial code reaches the publisher: custom DENY preserved, DEFER
     * defaults to UNAUTHORIZED. */
    f += announce_reval_case(MOQ_VERSION_DRAFT_16, MOQR_AUTH_DENY, 0x7u, 0x7u,
                             "d16_deny_custom");
    f += announce_reval_case(MOQ_VERSION_DRAFT_16, MOQR_AUTH_DEFER, 0u, 0x1u,
                             "d16_defer_default");
    /* draft-18 revokes by cancelling the request bidi (RESET+STOP), which
     * carries only the fixed §3.3.3 CANCELLED code, NOT a REQUEST_ERROR code:
     * the namespace is still withdrawn but the denial code is not conveyable on
     * the wire (protocol constraint, not a binding defect). */
    f += announce_reval_case(MOQ_VERSION_DRAFT_18, MOQR_AUTH_DENY, 0x7u, 0x0u,
                             "d18_deny_withdraw");
    return f;
}

/* A relay-topology force-withdraw (the shard manager's split-brain loser
 * enforcement) reaches a LIVE publisher as a wire NAMESPACE_CANCELLED. This is
 * the PENDING-CANCEL source, distinct from a revoked grant: no auth hook and
 * no lease exist here, the cancel is queued by moqr_core_force_withdraw and
 * rides the binding's peek/ack drain, targeting the announcement's real wire
 * handle via the session cookie the binding stored at announce. draft-16
 * carries the GOING_AWAY request-error code; draft-18 cancels the request
 * bidi, which carries only the fixed CANCELLED code (protocol constraint). */
static int
forcewithdraw_cancel_case(moq_version_t version, uint64_t expect_code,
                          const char *name)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: forcewithdraw_cancel %s rig create\n", name);
        return 1;
    }
    conn_t *pub = rig_connect(&rig, version);
    R_CHECK(&rig, pub != NULL);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted);
    R_CHECK(&rig, pub_ps.ns_cancelled == 0);

    /* The manager-style topology withdrawal, straight against the core. */
    moqr_ns_t rns = { nsp, 2 };
    R_CHECK(&rig, moqr_core_force_withdraw(rig.core, rns, 0x6u, rig.now) ==
                      MOQR_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.ns_cancelled == 1);
    R_CHECK(&rig, pub_ps.ns_cancelled_code == expect_code);

    /* Withdrawn and acked: a repeat is a no-op and cancels nothing twice. */
    R_CHECK(&rig, moqr_core_force_withdraw(rig.core, rns, 0x6u, rig.now) ==
                      MOQR_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.ns_cancelled == 1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: forcewithdraw_cancel_%s\n", name);
    }
    return f;
}

static int
test_forcewithdraw_publisher_cancel(void)
{
    int f = 0;
    f += forcewithdraw_cancel_case(MOQ_VERSION_DRAFT_16, 0x6u, "d16");
    f += forcewithdraw_cancel_case(MOQ_VERSION_DRAFT_18, 0x0u, "d18");
    return f;
}

/* grant_reserve failure (byte budget) fails the request closed BEFORE the core
 * op: no wire accept and no active core state, for both announce and subscribe. */
static int
test_reval_reserve_capacity(void)
{
    ca_t a;
    ca_init(&a);
    g_rv_lease_sub = 3000;
    g_rv_lease_ann = 3000;
    g_test_authorize = rv_hook;
    g_test_grant_bytes = 1; /* any grant material overflows -> CAPACITY */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        printf("FAIL: reval_capacity rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);
    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    /* Announce: reserve fails -> wire NAMESPACE_REJECTED(UNAUTHORIZED), no ns. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, !pub_ps.namespace_accepted);
    R_CHECK(&rig, pub_ps.ns_rejected == 1 &&
                      pub_ps.ns_rejected_code == 0x1u); /* UNAUTHORIZED */

    /* Subscribe: reserve fails -> wire SUBSCRIBE_ERROR(UNAUTHORIZED), and the
     * relay never subscribed upstream (no active core state). */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* would accept upstream */
    R_CHECK(&rig, !sub_ps.subscribe_ok);
    R_CHECK(&rig, sub_ps.subscribe_errors == 1 &&
                      sub_ps.sub_error_code == 0x1u); /* UNAUTHORIZED */
    R_CHECK(&rig, sub_reason_eq(&sub_ps, "unauthorized"));
    R_CHECK(&rig, pub_ps.up_subs == 0); /* relay never subscribed upstream */

    /* Neither failed request created active core state. */
    moqr_core_stats_t st1;
    moqr_core_get_stats(rig.core, &st1);
    R_CHECK(&rig, st1.ns_nodes == st0.ns_nodes && st1.subs == st0.subs);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: reval_reserve_capacity\n");
    }
    return f;
}

/* Integration guard: closing a connection that owns a live grant, then ticking
 * past the lease with a now-DENY hook, runs the real conn_detach -> retire path
 * cleanly (no crash, no spurious wire teardown, balanced allocation). The
 * decisive "a retired grant is never revalidated" assertion lives at the core
 * level in test_relay_control's grant_retire_no_reval. */
static int
test_reval_conn_close_retires(void)
{
    ca_t a;
    ca_init(&a);
    g_rv_lease_sub = 3000;
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        printf("FAIL: reval_conn_close rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    reval_make_subscribe_grant(&rig, pub, sub, &pub_ps, &sub_ps, nsp);

    /* Close the subscriber connection: conn_detach retires its grant. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, sub->rsess) == MOQR_OK);
    rig_pump(&rig, 4);

    moqr_bind_stats_t bs0;
    moqr_bind_get_stats(rig.bind, &bs0);
    /* Flip to DENY and advance well past the lease. With the grant retired the
     * tick must not revoke anything (no crash, no spurious wire teardown). */
    g_rv_decision = MOQR_AUTH_DENY;
    g_rv_deny_code = 0x7;
    rig_pump(&rig, 8);
    moqr_bind_stats_t bs1;
    moqr_bind_get_stats(rig.bind, &bs1);
    R_CHECK(&rig, bs1.session_errors == bs0.session_errors);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0); /* a non-retired grant would leak here */
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: reval_conn_close_retires\n");
    }
    return f;
}

/* Finding #1: an async ALLOW that carries a lease (a DEFER resolved with
 * ALLOW + revalidate_after_us) must create a revalidation grant, exactly like a
 * synchronous ALLOW — otherwise an async-approved subscription is permanently
 * authorized. Proof over real sessions: DEFER a subscribe, resolve it ALLOW +
 * lease, and a later revalidation (the hook DEFERs again -> revoke) tears the
 * subscription down on the wire (SUBSCRIBE_DONE). */
static int
test_reval_resume_creates_grant(void)
{
    ca_t a;
    ca_init(&a);
    g_defer_action = MOQR_AUTH_SUBSCRIBE; /* DEFER subscribes, allow the rest */
    g_defer_fixed = 0;
    g_test_authorize = defer_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_authorize = NULL;
        g_defer_action = 0xFFFFFFFFu;
        printf("FAIL: reval_resume rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    /* Subscribe -> DEFER -> parked. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    g_defer_last = 0;
    rig_pump(&rig, 4);
    uint64_t tkt = g_defer_last;
    R_CHECK(&rig, tkt != 0); /* parked, not answered */

    /* Resolve ALLOW + lease: the resume path must reserve + commit a grant. The
     * lease is generous so the resume + upstream-accept round-trip completes
     * before the first revalidation is due. */
    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW,
                                              .revalidate_after_us = 20000 },
                      rig.now) == MOQR_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0); /* accept upstream */
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.subscribe_ok); /* resumed subscribe */
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Tick past the lease: revalidation re-invokes the hook (DEFER -> revoke).
     * Without a resume-path grant, nothing here would revoke. */
    rig_pump(&rig, 20);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.done_seen); /* the async ALLOW was revalidatable */

    rig_destroy(&rig);
    g_test_authorize = NULL;
    g_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: reval_resume_creates_grant\n");
    }
    return f;
}

/* Finding #2: a publisher-side cancel that hits session backpressure
 * (cancel_namespace WOULD_BLOCK) must not be lost. The core grant is peeked, not
 * drained, so the cancel is retried each pump until it lands. Constrain the
 * action queue and keep it full with unrelated responses across the revoke
 * window: the cancel is withheld the whole time, then delivered once the queue
 * drains. Under the old drain-on-poll behavior the first blocked attempt would
 * lose the grant and the cancel would never arrive. */
static int
test_reval_cancel_backpressure(void)
{
    ca_t a;
    ca_init(&a);
    g_rv_lease_ann = 3000;
    g_test_authorize = rv_hook;
    g_test_max_actions = 1; /* one action slot: a filler blocks the cancel */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        printf("FAIL: reval_backpressure rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_16); /* d16 carries code */
    R_CHECK(&rig, pub != NULL);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    rig_pump(&rig, 12); /* setup spreads over more pumps at max_actions 1 */

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 12);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted);

    /* Flip to DENY, then keep the pub session's action queue full while the
     * lease elapses: each cycle the pub asks track_status (the relay answers ->
     * 1 queued action), so the revoke's cancel that follows in the same pump
     * WOULD_BLOCKs. */
    g_rv_decision = MOQR_AUTH_DENY;
    g_rv_deny_code = 0x7;
    for (int i = 0; i < 12; i++) {
        /* Filler: a throwaway announce the relay DENIES. The reject is a
         * synchronous session write in the same pump step, queued BEFORE the
         * revoke's cancel, so the one-slot action queue is full when the cancel
         * is attempted -> cancel_namespace WOULD_BLOCKs. (track_status answers
         * via an intent, which lands after the cancel, so it would not block.) */
        char jn[2] = { 'j', (char)('0' + i) };
        moq_bytes_t jns[2] = { B("junk"),
                               (moq_bytes_t){ (const uint8_t *)jn, 2 } };
        moq_publish_namespace_cfg_t jc;
        memset(&jc, 0, sizeof(jc));
        moq_publish_namespace_cfg_init(&jc);
        jc.track_namespace = (moq_namespace_t){ .parts = jns, .count = 2 };
        moq_announcement_t jh;
        (void)moq_session_publish_namespace(pub->peer, &jc, rig.now, &jh);
        rig_pump(&rig, 1);
    }
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.ns_cancelled == 0); /* cancel withheld under backpressure */

    /* Stop the fillers: the queue drains and the withheld cancel is delivered. */
    rig_pump(&rig, 12);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.ns_cancelled == 1);
    R_CHECK(&rig, pub_ps.ns_cancelled_code == 0x7u);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: reval_cancel_backpressure\n");
    }
    return f;
}

/* A publisher's PUBLISH_NAMESPACE_DONE must remove the relay
 * route and notify watchers. Without the announcement-handle map, the binding
 * cannot translate the done event back to namespace bytes, so the route stays
 * occupied forever. */
static int
test_namespace_done_releases_route(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: namespace-done rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *watch = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && watch != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t pfx[1] = { B("gone") };
    moq_subscribe_namespace_cfg_t nsc;
    memset(&nsc, 0, sizeof(nsc));
    moq_subscribe_namespace_cfg_init(&nsc);
    nsc.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = 1 };
    nsc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nsc,
                                                  rig.now, &nsh) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_bytes_t ns[2] = { B("gone"), B("cam") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    moq_announcement_t ann1;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &ann1) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, watch_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&watch_ps, 0, sizeof(watch_ps));
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, watch, &watch_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted);
    R_CHECK(&rig, watch_ps.ns_found == 1);

    R_CHECK(&rig, moq_session_publish_namespace_done(pub->peer, ann1,
                                                     rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    memset(&watch_ps, 0, sizeof(watch_ps));
    peer_drain(&rig, watch, &watch_ps, false, false, 0, 0);
    R_CHECK(&rig, watch_ps.ns_gone == 1);

    moq_announcement_t ann2;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &ann2) == MOQ_OK);
    rig_pump(&rig, 6);
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&watch_ps, 0, sizeof(watch_ps));
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, watch, &watch_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted);
    R_CHECK(&rig, pub_ps.ns_rejected == 0);
    R_CHECK(&rig, watch_ps.ns_found == 1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: namespace_done_releases_route\n");
    }
    return f;
}

/* An inbound publisher finishing a PUBLISH must release the
 * relay's active-source state. Without the PUBLISH_FINISHED handler, a second
 * PUBLISH for the same track is rejected as a duplicate active source. */
static int
test_publish_finished_releases_track(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: publish-finished rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t ns[1] = { B("finish") };
    moq_publish_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    pc.track_name = B("push");

    moq_publication_t p1;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &p1) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, pub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.publish_ok);
    R_CHECK(&rig, !ps.publish_error);

    moq_finish_publish_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_finish_publish_cfg_init(&fc);
    R_CHECK(&rig, moq_session_finish_publish(pub->peer, p1, &fc, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 6);

    moq_publication_t p2;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &p2) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, pub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.publish_ok);
    R_CHECK(&rig, !ps.publish_error);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: publish_finished_releases_track\n");
    }
    return f;
}

/* Request types the relay does not yet implement must receive a
 * terminal wire error. Falling through bind_on_event's default leaves the peer
 * waiting forever. */
static int
test_unsupported_requests_reject(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: unsupported rig create\n");
        return 1;
    }
    conn_t *cn = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, cn != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t ns[1] = { B("unsupported") };

    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    fc.track_name = B("fetch");
    fc.start_group = 0;
    fc.start_object = 0;
    fc.end_group = 1;
    fc.end_object = 0;
    moq_fetch_t fh;
    R_CHECK(&rig, moq_session_fetch(cn->peer, &fc, rig.now, &fh) == MOQ_OK);

    moq_subscribe_tracks_cfg_t stc;
    memset(&stc, 0, sizeof(stc));
    moq_subscribe_tracks_cfg_init(&stc);
    stc.track_namespace_prefix =
        (moq_namespace_t){ .parts = ns, .count = 1 };
    moq_track_sub_handle_t sth;
    R_CHECK(&rig, moq_session_subscribe_tracks(cn->peer, &stc, rig.now, &sth) ==
                      MOQ_OK);

    rig_pump(&rig, 8);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, cn, &ps, false, false, 0, 0);
    /* FETCH is wired now: a fetch for a track with no publisher rejects
     * DOES_NOT_EXIST (the unknown-track terminal), not the old blanket
     * NOT_SUPPORTED. SUBSCRIBE_TRACKS is still unsupported. */
    R_CHECK(&rig, ps.fetch_error == 1);
    R_CHECK(&rig, ps.fetch_error_code == MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
    R_CHECK(&rig, ps.tracks_error == 1);
    R_CHECK(&rig, ps.tracks_error_code == MOQ_REQUEST_ERROR_NOT_SUPPORTED);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: unsupported_requests_reject\n");
    }
    return f;
}

/* When the origin ends the relay's upstream subscription
 * (SUBSCRIBE_DONE), the relay must terminate downstream subscribers on the wire
 * and release the upstream — no zombie ACTIVE track, no hung subscriber. The
 * subscriber uses an open-ended (LARGEST_OBJECT) filter, which never self-
 * completes, so only an explicit wire DONE keeps it from hanging. */
static int
test_upstream_done_terminates_downstream(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: upstream-done rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT; /* open-ended */
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, true, 0, 0); /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen && pub_ps.up_subs == 1);
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.subscribe_ok);
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Origin ends the upstream subscription with a specific status code. */
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x4u;
    R_CHECK(&rig, moq_session_done_subscribe(pub->peer, pub_ps.up_sub, &dc,
                                             rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.done_seen);         /* terminal DONE, not a hang */
    R_CHECK(&rig, sub_ps.done_code == 0x4u); /* origin's status forwarded */

    /* Upstream tracking slot released: a fresh subscribe re-triggers a NEW
     * upstream SUBSCRIBE (the publisher is still announced). */
    conn_t *sub2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub2 != NULL);
    peer_state_t sub2_ps;
    memset(&sub2_ps, 0, sizeof(sub2_ps));
    rig_pump(&rig, 4);
    moq_subscription_t sh2;
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &sc, rig.now, &sh2) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, true, 0, 0); /* accept the NEW upstream */
    R_CHECK(&rig, pub_ps.up_subs == 2);               /* a second upstream sub */
    rig_pump(&rig, 4);
    peer_drain(&rig, sub2, &sub2_ps, false, false, 0, 0);
    R_CHECK(&rig, sub2_ps.subscribe_ok); /* rejoin path works */

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: upstream_done_terminates_downstream\n");
    }
    return f;
}

/* Symmetry: a push publisher finishing (PUBLISH_FINISHED) is
 * the same "source ended" contract as an upstream SUBSCRIBE_DONE — its
 * downstream subscribers must get a wire SUBSCRIBE_DONE, not be left hanging on
 * a WARM track. The subscriber uses an open-ended filter so only a wire DONE
 * saves it. (test_publish_finished_releases_track covers republish alone; this
 * adds the downstream-terminal case.) */
static int
test_publish_finished_terminates_downstream(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: publish-finished-terminates rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    /* Publisher pushes a track (creates the ACTIVE source). */
    moq_bytes_t nsp[1] = { B("pushed") };
    moq_publish_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    pc.track_name = B("track");
    moq_publication_t p1;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &p1) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok);

    /* Downstream subscribes open-ended to the pushed track. */
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    sc.track_name = B("track");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT; /* open-ended */
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.subscribe_ok);
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Publisher finishes the push with a specific status code. */
    moq_finish_publish_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_finish_publish_cfg_init(&fc);
    fc.status_code = 0x4u;
    R_CHECK(&rig, moq_session_finish_publish(pub->peer, p1, &fc, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.done_seen);         /* terminal DONE, not a hang */
    R_CHECK(&rig, sub_ps.done_code == 0x4u); /* finish status forwarded */

    /* The source is released: the same track can be pushed again. */
    moq_publication_t p2;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &p2) == MOQ_OK);
    rig_pump(&rig, 6);
    memset(&pub_ps, 0, sizeof(pub_ps));
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok && !pub_ps.publish_error);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: publish_finished_terminates_downstream\n");
    }
    return f;
}

/* A REDIRECT rejection of the relay's upstream SUBSCRIBE (before
 * OK) is terminal — the relay does not follow the redirect, so parked downstream
 * subscribers must be rejected on the wire (GOING_AWAY), not left waiting. */
static int
test_upstream_redirect_rejects_downstream(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: upstream-redirect rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    /* Capture the relay's upstream SUBSCRIBE at the origin WITHOUT accepting it,
     * then reject it with REDIRECT. */
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_subs == 1);
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_REDIRECT;
    /* All-empty redirect target: a client REDIRECT must carry a zero-length URI
     * (reuse current) — still surfaces REQUEST_REDIRECT at the relay. */
    R_CHECK(&rig, moq_session_reject_subscribe(pub->peer, pub_ps.up_sub, &rj,
                                               rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, !sub_ps.subscribe_ok);
    R_CHECK(&rig, sub_ps.subscribe_errors == 1); /* parked downstream rejected */
    /* REQUEST_ERROR INTERNAL_ERROR: the relay carries no Redirect structure
     * and follows no redirect, so it can neither restate REDIRECT nor claim a
     * TIMEOUT that never elapsed. 0x0 is assigned identically in both
     * registries. */
    R_CHECK(&rig, sub_ps.sub_error_code == 0x0u);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: upstream_redirect_rejects_downstream\n");
    }
    return f;
}

/* A per-request GOAWAY on the relay's ESTABLISHED upstream
 * SUBSCRIBE is terminal — the relay does not migrate the request, so its active
 * (open-ended) downstream subscribers must receive a wire SUBSCRIBE_DONE
 * (GOING_AWAY), not hang. */
static int
test_upstream_goaway_terminates_downstream(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: upstream-goaway rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT; /* open-ended */
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, true, 0, 0); /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.subscribe_ok);
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Origin sends a per-request GOAWAY on the established upstream sub. */
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    R_CHECK(&rig, moq_session_request_goaway_subscribe(pub->peer, pub_ps.up_sub,
                                                       &gc, rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.done_seen);         /* terminal DONE, not a hang */
    /* PUBLISH_DONE GOING_AWAY (0x4): the upstream publisher issued a GOAWAY —
     * the status registry's own value, not REQUEST_ERROR's. */
    R_CHECK(&rig, sub_ps.done_code == 0x4u);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: upstream_goaway_terminates_downstream\n");
    }
    return f;
}

/* -- The deferred-output ring must never hold a borrowed-payload intent ------- */

/* Advance ONE connection's transport (deliver its queued wire bytes to the
 * relay) WITHOUT pumping the binding — so several peer messages can be staged
 * into a single subsequent bind pump. */
static void
red_deliver(rig_t *r, conn_t *cn)
{
    r->now += 1000;
    (void)moq_simpair_advance_to(cn->sp, r->now);
    size_t steps = 0;
    (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
}

/* Bring one track "live"/<ns2>/"v" to WARM with a retained record: announce,
 * downstream subscribe, upstream accept + one object, downstream unsubscribe,
 * then let the linger expire (a tick warms the track, record kept). A later
 * subscribe to a WARM track is the fan-out that emits ACCEPT_SUB THEN
 * UPSTREAM_SUBSCRIBE (relay.c:1935/1952) — the scalar-then-borrowed pairing. */
static void
red_warm_track(rig_t *r, conn_t *pub, conn_t *sub, moq_bytes_t ns2)
{
    moq_bytes_t nsp[2] = { B("live"), ns2 };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now, &ann) ==
                   MOQ_OK);
    rig_pump(r, 16);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sh;
    R_CHECK(r, moq_session_subscribe(sub->peer, &scfg, r->now, &sh) == MOQ_OK);
    rig_pump(r, 20);

    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    peer_drain(r, pub, &pub_ps, true, false, 0, 0);   /* accept the upstream */
    R_CHECK(r, pub_ps.up_seen);
    rig_pump(r, 20);

    /* Origin writes one object so the track log keeps a record (survives WARM). */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc, r->now,
                                         &sgh) == MOQ_OK);
    uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, sgh, 0, pl, r->now) == MOQ_OK);
    moq_rcbuf_decref(pl);
    (void)moq_session_close_subgroup(pub->peer, sgh, r->now);
    rig_pump(r, 20);

    /* Downstream leaves; linger (500us) expires within a 1000us cycle, so the
     * next tick transitions the track to WARM with its record retained. */
    R_CHECK(r, moq_session_unsubscribe(sub->peer, sh, r->now) == MOQ_OK);
    rig_pump(r, 20);
}

/* A warm-track rejoin emits ACCEPT_SUB then UPSTREAM_SUBSCRIBE in one fan-out;
 * the UPSTREAM_SUBSCRIBE's ns_parts BORROW the track key (relay.h:102), valid
 * only until the next mutating core call. If the ACCEPT blocks on a full
 * downstream queue, the binding must execute the borrowed UPSTREAM inline and
 * NEVER park it across pumps. We stage two warm-track rejoins into a single
 * pump on a depth-1 subscriber: the first rejoin's ACCEPT fills the queue, the
 * second's ACCEPT WOULD_BLOCKs, leaving the second's UPSTREAM_SUBSCRIBE as a
 * borrowed intent sitting behind a blocked scalar in the same drain. The
 * binding's pending_nonscalar_blocked telemetry (0 iff pending[] only ever held
 * scalar-safe kinds) is the discriminating invariant. */
static int
borrowed_intent_never_parked(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    g_test_max_actions = 0;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: borrowed_remainder rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* roomy */
    conn_t *warm = rig_connect(&rig, MOQ_VERSION_DRAFT_18);  /* roomy: warms   */
    g_test_max_actions = 1;                                  /* depth-1 rejoin */
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 0;
    R_CHECK(&rig, pub && warm && sub);
    rig_pump(&rig, 12);

    /* Warm the tracks with a ROOMY subscriber (WARM is a track-side state, so a
     * different, depth-1 subscriber can later rejoin it — this decouples the
     * easy setup from the depth-1 pressure the rejoin needs). */
    red_warm_track(&rig, pub, warm, B("a"));
    red_warm_track(&rig, pub, warm, B("b"));

    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.pending_nonscalar_blocked == 0);   /* clean after setup   */

    /* Stage BOTH warm-rejoin subscribes (from the depth-1 sub) into the relay
     * before it pumps. red_deliver drains the depth-1 peer queue between them. */
    moq_subscribe_cfg_t rc;
    memset(&rc, 0, sizeof(rc));
    moq_subscribe_cfg_init(&rc);
    rc.track_name = B("v");
    rc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

    moq_bytes_t nsa[2] = { B("live"), B("a") };
    rc.track_namespace = (moq_namespace_t){ .parts = nsa, .count = 2 };
    moq_subscription_t rha;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &rc, rig.now, &rha) == MOQ_OK);
    red_deliver(&rig, sub);

    moq_bytes_t nsb[2] = { B("live"), B("b") };
    rc.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 2 };
    moq_subscription_t rhb;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &rc, rig.now, &rhb) == MOQ_OK);
    red_deliver(&rig, sub);

    /* One pump processes both rejoins on a full depth-1 downstream queue. */
    (void)moqr_bind_pump(rig.bind, rig.now);

    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.pending_nonscalar_blocked == 0);   /* THE invariant       */
    R_CHECK(&rig, bs.pending_high_water > 0);           /* a scalar DID block  */

    /* Drain to quiescence (retries any parked scalar; under the pre-fix bug a
     * parked UPSTREAM_SUBSCRIBE would dangle here — ASan-visible). */
    rig_pump(&rig, 40);
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.pending_nonscalar_blocked == 0);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: borrowed_intent_never_parked\n");
    }
    return f;
}

#define REENTRY_NS_WATCHERS 33u
#define REENTRY_STATUS_INTENTS 33u
#define REENTRY_STATUS_COOKIE_BASE 0x4E700000u

typedef struct intent_reentry_router_rec {
    uint64_t status_cookie[REENTRY_STATUS_INTENTS + 4u];
    uint32_t status_n;
    uint32_t ns_found;
    uint32_t ns_gone;
    uint32_t other;
    char     order[96];
    uint32_t order_len;
} intent_reentry_router_rec_t;

static void
intent_reentry_router_reset(intent_reentry_router_rec_t *rec)
{
    memset(rec, 0, sizeof(*rec));
}

static void
intent_reentry_router_mark(intent_reentry_router_rec_t *rec, char c)
{
    if (rec->order_len + 1u < sizeof(rec->order)) {
        rec->order[rec->order_len++] = c;
        rec->order[rec->order_len] = '\0';
    }
}

static bool
intent_reentry_router(void *ctx, const moqr_intent_t *it, uint64_t now_us)
{
    (void)now_us;
    intent_reentry_router_rec_t *rec = ctx;
    switch (it->kind) {
    case MOQR_INTENT_TRACK_STATUS_ERROR:
        if (rec->status_n <
            (uint32_t)(sizeof(rec->status_cookie) /
                       sizeof(rec->status_cookie[0]))) {
            rec->status_cookie[rec->status_n] = it->cookie;
        }
        rec->status_n++;
        intent_reentry_router_mark(rec, 'E');
        break;
    case MOQR_INTENT_NS_FOUND:
        rec->ns_found++;
        intent_reentry_router_mark(rec, 'N');
        break;
    case MOQR_INTENT_NS_GONE:
        rec->ns_gone++;
        intent_reentry_router_mark(rec, 'G');
        break;
    default:
        rec->other++;
        intent_reentry_router_mark(rec, '?');
        break;
    }
    return true;
}

static moqr_result_t
intent_reentry_rig_create(rig_t *r, ca_t *a,
                          intent_reentry_router_rec_t *rec)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    if (moqr_trace_create(&a->vt, 512, &r->trace) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = r->trace;
    cfg.max_bindings = 80;
    cfg.max_tracks = 4;
    cfg.max_subs = 2;
    cfg.max_ns_nodes = 8;
    cfg.max_ns_subs = REENTRY_NS_WATCHERS;
    cfg.max_intents = REENTRY_STATUS_INTENTS + 1u;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1u << 20;
    cfg.linger_us = 500;
    if (moqr_core_create(&cfg, &r->core) != MOQR_OK) {
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a->vt);
    bcfg.core = r->core;
    bcfg.max_conns = MAX_CONNS;
    bcfg.max_downstream_subs = 2;
    bcfg.max_upstream_subs = 2;
    bcfg.max_publishes = 2;
    bcfg.max_announces = 2;
    bcfg.max_open_subgroups = 4;
    bcfg.router = intent_reentry_router;
    bcfg.router_ctx = rec;
    if (moqr_bind_create(&bcfg, &r->bind) != MOQR_OK) {
        moqr_core_destroy(r->core);
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    moqr_core_limits_t lim;
    moqr_core_get_limits(r->core, &lim);
    R_CHECK(r, lim.max_intents == REENTRY_STATUS_INTENTS + 1u);
    R_CHECK(r, lim.max_ns_subs == REENTRY_NS_WATCHERS);
    return MOQR_OK;
}

static void
intent_reentry_install_watchers(rig_t *r, moqr_binding_t watchers[])
{
    moq_bytes_t pfx[1] = { B("dead") };
    moqr_ns_t prefix = { .parts = pfx, .count = 1 };
    for (uint32_t i = 0; i < REENTRY_NS_WATCHERS; i++) {
        uint64_t cookie = MOQR_SHARD_COOKIE_BASE + 0x100u + i;
        R_CHECK(r, moqr_core_binding_open(r->core, cookie, &watchers[i]) ==
                       MOQR_OK);
        R_CHECK(r, moqr_core_ns_subscribe(r->core, watchers[i], prefix,
                                          0xD000u + i) == MOQR_OK);
    }
}

typedef struct detach_pending_observation {
    int           calls;
    moqr_result_t close_rc;
    bool          open_after_close;
} detach_pending_observation_t;

static void
observe_detach_pending(moqr_bind_t *bind, moq_session_t *session, void *ctx)
{
    detach_pending_observation_t *obs = ctx;
    obs->calls++;
    obs->close_rc = moqr_bind_conn_close(bind, session);
    obs->open_after_close = moqr_bind_conn_is_open(bind, session);
}

/* The bind-owned intent scratch must not be reused by a retry helper while an
 * outer intent batch is live. This drives the real path:
 *   SUB_DONE -> injected reset failure -> conn_detach -> binding_close WOULD_BLOCK
 * because the core ring still holds the tail of the outer batch and the closing
 * connection owns a namespace with a 33-watcher NS_GONE fanout. The old shared
 * scratch recursively polled that tail into b->pump_intents, overwriting the
 * outer batch; status cookie 0 was skipped and the tail duplicated. */
static int
intent_retry_reentry_preserves_outer_batch(void)
{
    ca_t a;
    ca_init(&a);
    intent_reentry_router_rec_t rec;
    intent_reentry_router_reset(&rec);
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    rig_t rig;
    if (intent_reentry_rig_create(&rig, &a, &rec) != MOQR_OK) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        printf("FAIL: intent_retry_reentry rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moqr_binding_t watchers[REENTRY_NS_WATCHERS];
    intent_reentry_install_watchers(&rig, watchers);

    /* The future closing connection owns a namespace observed only through the
     * router. Its close therefore has a large, deterministic NS_GONE fanout. */
    moq_bytes_t dead[2] = { B("dead"), B("owned") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = dead, .count = 2 };
    moq_announcement_t dead_ann;
    R_CHECK(&rig, moq_session_publish_namespace(sub->peer, &pcfg, rig.now,
                                                &dead_ann) == MOQ_OK);
    rig_pump(&rig, 12);
    R_CHECK(&rig, rec.ns_found == REENTRY_NS_WATCHERS);
    R_CHECK(&rig, rec.ns_gone == 0);
    peer_state_t sub_ctrl;
    memset(&sub_ctrl, 0, sizeof(sub_ctrl));
    peer_drain(&rig, sub, &sub_ctrl, false, false, 0, 0);
    R_CHECK(&rig, sub_ctrl.namespace_accepted);
    intent_reentry_router_reset(&rec);

    /* The same connection is also a downstream subscriber with a begun subgroup,
     * so SUB_DONE's reset failure closes that namespace-owning binding. */
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    moq_bytes_t live[2] = { B("live"), B("cam1") };
    reval_make_subscribe_grant(&rig, pub, sub, &pub_ps, &sub_ps, live);
    R_CHECK(&rig, !sub_ps.done_seen);

    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1);

    uint32_t up_slot = 0;
    uint64_t up_handle = 0;
    R_CHECK(&rig, moqr_bind_debug_first_usub(rig.bind, &up_slot, &up_handle));
    moqr_bind_debug_fail_sg_reset(1);
    uint64_t suppressed0 =
        moqr_bind_debug_intent_retry_suppressed(rig.bind);
    detach_pending_observation_t detach_obs;
    memset(&detach_obs, 0, sizeof(detach_obs));
    moqr_bind_debug_on_detach_pending(observe_detach_pending, &detach_obs);
    moqr_bind_debug_upstream_terminated(rig.bind, up_slot,
                                        MOQ_REQUEST_FAMILY_SUBSCRIBE,
                                        up_handle, 0, rig.now);

    moq_bytes_t miss_ns_part[1] = { B("missing") };
    moqr_ns_t miss_ns = { .parts = miss_ns_part, .count = 1 };
    for (uint32_t i = 0; i < REENTRY_STATUS_INTENTS; i++) {
        R_CHECK(&rig, moqr_core_track_status(
                          rig.core, watchers[i], miss_ns, B("track"),
                          REENTRY_STATUS_COOKIE_BASE + i) == MOQR_OK);
    }
    moqr_core_stats_t cs;
    moqr_core_get_stats(rig.core, &cs);
    R_CHECK(&rig, cs.intent_highwater >= REENTRY_STATUS_INTENTS + 1u);

    rig.now += 1000;
    (void)moqr_bind_pump(rig.bind, rig.now);
    moqr_bind_debug_on_detach_pending(NULL, NULL);

    uint64_t suppressed1 =
        moqr_bind_debug_intent_retry_suppressed(rig.bind);
    R_CHECK(&rig, detach_obs.calls == 1);
    R_CHECK(&rig, detach_obs.close_rc == MOQR_ERR_WOULD_BLOCK);
    R_CHECK(&rig, detach_obs.open_after_close);
    R_CHECK(&rig, suppressed1 == suppressed0 + 1u);
    R_CHECK(&rig, rec.status_n == REENTRY_STATUS_INTENTS);
    for (uint32_t i = 0; i < REENTRY_STATUS_INTENTS &&
                         i < rec.status_n; i++) {
        R_CHECK(&rig, rec.status_cookie[i] == REENTRY_STATUS_COOKIE_BASE + i);
    }
    R_CHECK(&rig, rec.ns_gone == REENTRY_NS_WATCHERS);
    R_CHECK(&rig, rec.other == 0);
    for (uint32_t i = 0; i < rec.status_n && i < sizeof(rec.order); i++) {
        if (i < REENTRY_STATUS_INTENTS) {
            R_CHECK(&rig, rec.order[i] == 'E');
        }
    }
    R_CHECK(&rig, rec.order_len == REENTRY_STATUS_INTENTS +
                                  REENTRY_NS_WATCHERS);

    moqr_bind_debug_fail_sg_reset(0);
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: intent_retry_reentry_preserves_outer_batch\n");
    }
    return f;
}

/* -- Downstream subgroup-slot reclamation ----------------------------------- */

/* Publish one object on a FRESH subgroup stream (group, subgroup), then close
 * the stream with a PLAIN FIN — no END_OF_GROUP. This is the legal sequence the
 * relay cannot currently reclaim: the object carries end_of_group=false, so the
 * downstream subgroup slot stays open forever. */
static void
red_pub_subgroup_plain_fin(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                           uint64_t group, uint64_t subgroup, uint64_t object_id)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                         &sgh) == MOQ_OK);
    uint8_t body[4] = { (uint8_t)group, (uint8_t)subgroup, 0xC3, 0x3C };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, sgh, object_id, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
    /* Plain FIN: closes the stream, but the last object was NOT END_OF_GROUP. */
    R_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
}

/* Subgroup-slot reclamation: a downstream subgroup slot pool of 2, fed THREE distinct
 * (group, subgroup) streams each ending in a plain FIN (no EOG). Before the fix
 * the relay froze a slot per (group, subgroup) until EOG/SUB_DONE, so the third
 * candidate hit `freei < 0` → WOULD_BLOCK forever (only two objects delivered,
 * no terminal). Now the upstream `MOQ_EVENT_SUBGROUP_FINISHED` seals the log
 * subgroup and the acknowledged SEAL notice — the only durable FIN path
 * (records carry `subgroup_end` as advisory metadata) — closes the
 * downstream stream and reclaims the slot, so all three objects flow.
 * RED-verified: asserting `objects == 3` fails at two without the seal
 * pipeline. */
static int
subgroup_slot_reclamation(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_open_subgroups = 2;   /* tiny downstream subgroup slot pool */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_open_subgroups = 0;
        printf("FAIL: subgroup_slot_reclamation rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    /* Three distinct (group, subgroup) streams, each a plain FIN (no EOG). */
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 2, 0, 0);
    rig_pump(&rig, 40);   /* generous: prove the stall is permanent, not slow */
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* All three objects reach the subscriber (the pool of 2 was reclaimed twice
     * across three streams), with no error and no premature DONE. */
    R_CHECK(&rig, s_ps.objects == 3);
    R_CHECK(&rig, s_ps.subscribe_errors == 0);
    R_CHECK(&rig, !s_ps.done_seen);

    g_test_max_open_subgroups = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: subgroup_slot_reclamation\n");
    }
    return f;
}

/* Publish a subgroup carrying `n` normal objects, optionally a terminal EOG
 * status object, then close (plain FIN). Exercises: subgroup_end must fire only
 * on the LAST record (a multi-object subgroup must deliver every object before
 * the downstream stream closes), and the EOG path still reclaims. */
static void
red_pub_subgroup_n(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                   uint64_t group, uint64_t subgroup, uint64_t n, bool eog)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                         &sgh) == MOQ_OK);
    for (uint64_t o = 0; o < n; o++) {
        uint8_t body[4] = { (uint8_t)group, (uint8_t)o, 0xC3, 0x3C };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        R_CHECK(r, moq_session_write_object(pub->peer, sgh, o, pl, r->now) ==
                       MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    if (eog) {
        R_CHECK(r, moq_session_write_status_object(
                       pub->peer, sgh, n, MOQ_OBJECT_END_OF_GROUP, r->now) ==
                       MOQ_OK);
    }
    R_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
}

/* Multi-object and EOG shapes through a pool of 2: a 3-object sealed subgroup
 * must deliver all three BEFORE its stream closes (subgroup_end only on the last
 * record), an EOG subgroup still reclaims, and a further plain-FIN subgroup
 * reclaims after the pool has churned. */
static int
subgroup_reclamation_shapes(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_open_subgroups = 2;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_open_subgroups = 0;
        printf("FAIL: subgroup_reclamation_shapes rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    red_pub_subgroup_n(&rig, pub, pub_ps.up_sub, 0, 0, 3, false); /* 3 obj, FIN */
    red_pub_subgroup_n(&rig, pub, pub_ps.up_sub, 1, 0, 0, true);  /* EOG only   */
    red_pub_subgroup_n(&rig, pub, pub_ps.up_sub, 2, 0, 1, false); /* 1 obj, FIN */
    rig_pump(&rig, 40);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* 3 + 1 (EOG status) + 1 = 5 objects, all reclaimed through the pool of 2. */
    R_CHECK(&rig, s_ps.objects == 5);
    R_CHECK(&rig, s_ps.subscribe_errors == 0);
    R_CHECK(&rig, !s_ps.done_seen);

    g_test_max_open_subgroups = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: subgroup_reclamation_shapes\n");
    }
    return f;
}

/* Write one object on an already-open subgroup handle (the stream stays open —
 * no FIN, so no SUBGROUP_FINISHED / seal). */
static void
red_write_obj(rig_t *r, conn_t *pub, moq_subgroup_handle_t h, uint64_t object_id)
{
    uint8_t body[4] = { (uint8_t)object_id, 0x11, 0x22, 0x33 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, h, object_id, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
}

/* Eviction cleanup must not close a LIVE lower-group subgroup. Three subgroup
 * streams (groups 0/1/2, each held OPEN — no FIN/EOG) with a log of only 2
 * groups: ingesting group 2 evicts group 0 and arms the sub's eviction jump. A
 * naive cleanup that closed every open subgroup below the next-delivered group
 * would close group 1 (still retained) when the acknowledged EVICT_WATERMARK
 * notice lands — then a later group-1 object would reopen the FIN'd stream and
 * fault the subscriber. The correct watermark is the oldest RETAINED group, so group 1
 * survives and its second object is delivered on the same stream. */
static int
subgroup_eviction_spares_live_lower_group(void)
{
    ca_t a;
    ca_init(&a);
    g_test_log_max_groups = 2;   /* group 0 evicts when group 2 is ingested */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_log_max_groups = 0;
        printf("FAIL: subgroup_eviction_spares_live_lower_group rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    /* Open three subgroup streams (groups 0,1,2), held OPEN — no FIN. */
    moq_subgroup_handle_t h0, h1, h2;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 0;
    sc.subgroup_id = 0;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h0) == MOQ_OK);
    red_write_obj(&rig, pub, h0, 0);
    sc.group_id = 1;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h1) == MOQ_OK);
    red_write_obj(&rig, pub, h1, 0);
    rig_pump(&rig, 8);   /* deliver g0o0, g1o0: downstream subgroups 0,1 open */

    sc.group_id = 2;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h2) == MOQ_OK);
    red_write_obj(&rig, pub, h2, 0);   /* group 2 ingest evicts group 0 */
    rig_pump(&rig, 8);   /* the watermark notice is acknowledged, then group 2 */

    red_write_obj(&rig, pub, h1, 1);   /* more data on the RETAINED group 1 */
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* All four objects (g0o0, g1o0, g2o0, g1o1) reach the subscriber, and — the
     * discriminating invariant — group 1's still-retained subgroup stream was
     * NEVER closed then reopened (the buggy `group < delivered` watermark closes
     * it at the EVICT_WATERMARK notice, so g1o1 arrives on a FIN'd subgroup). */
    R_CHECK(&rig, s_ps.objects == 4);
    R_CHECK(&rig, !s_ps.reopen_after_finish);
    R_CHECK(&rig, s_ps.subscribe_errors == 0);
    R_CHECK(&rig, !s_ps.session_closed);

    (void)moq_session_close_subgroup(pub->peer, h1, rig.now);
    (void)moq_session_close_subgroup(pub->peer, h2, rig.now);
    rig_pump(&rig, 8);

    g_test_log_max_groups = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: subgroup_eviction_spares_live_lower_group\n");
    }
    return f;
}

/* A long-lived connection must keep sending peer-originated requests past
 * the initial request-credit window. The subscriber fires far more FETCHes than
 * the tiny initial MAX_REQUEST_ID allows; on draft-16 the binding must raise the
 * ceiling (auto-grant) so the peer never permanently blocks, and every request
 * reaches a terminal (fetch_error NOT_SUPPORTED). Draft-18 has no MAX_REQUEST_ID
 * so the peer never blocks at all. `max_actions` drives the backpressure case (a
 * depth-1 session queue where the grant itself can WOULD_BLOCK and must retry). */
static int
long_lived_request_credit(moq_version_t version, uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_initial_request_capacity = 32;   /* 16 requests before exhaustion */
    g_test_grant_window = 32;
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_initial_request_capacity = 0;
        g_test_grant_window = 0;
        g_test_max_actions = 0;
        printf("FAIL: long_lived_request_credit rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 6);

    moq_bytes_t nsp[1] = { B("nx") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = B("t");
    fc.start_group = 0;
    fc.start_object = 0;
    fc.end_group = 1;
    fc.end_object = 0;

    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    const int target = 120;              /* >> the 16-request initial window */
    int sent = 0, consec_block = 0;
    for (int guard = 0; sent < target && guard < target * 12; guard++) {
        moq_fetch_t fh;
        moq_result_t rc = moq_session_fetch(sub->peer, &fc, rig.now, &fh);
        if (rc == MOQ_OK) {
            sent++;
            consec_block = 0;
        } else if (rc == MOQ_ERR_REQUEST_BLOCKED) {
            /* Peer out of credit: pumping lets the relay grant more and the
             * peer receive it. A permanent block (no grant) fails fast. */
            if (++consec_block > 40) {
                break;
            }
        } else {
            R_CHECK(&rig, false);   /* unexpected send outcome */
            break;
        }
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &ps, false, false, 0, 0);
    }
    rig_pump(&rig, 12);
    peer_drain(&rig, sub, &ps, false, false, 0, 0);

    /* Every request was accepted for send (credit stayed ahead of consumption)
     * and every one reached a terminal — no permanent block, no hang. */
    R_CHECK(&rig, sent == target);
    R_CHECK(&rig, ps.fetch_error == target);
    R_CHECK(&rig, !ps.session_closed);

    g_test_initial_request_capacity = 0;
    g_test_grant_window = 0;
    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: long_lived_request_credit(draft-%d%s)\n", (int)version,
               max_actions == 1 ? ", depth-1" : "");
    }
    return f;
}

/* ---- Data-plane delivery oracle ------------------------------------------ *
 * Drive a KNOWN object matrix over the production SimPair binding and prove,
 * from wire-observable events ALONE (no relay internals), that the relay's data
 * plane is correct: every in-range object delivered exactly once, in
 * per-(group, subgroup) object-id order, byte-faithful, nothing out of range,
 * nothing after the subscription terminal, and every in-range subgroup sealed.
 * Runs clean and under a small session action queue — WOULD_BLOCK churn must
 * delay delivery, never corrupt or duplicate the stream. */
#define DP_G  2u   /* in-range groups 0..DP_G-1 (group DP_G is out of range) */
#define DP_SG 2u   /* subgroups per group                                    */
#define DP_O  3u   /* data objects per subgroup                              */
#define DP_OPG (DP_SG * DP_O)   /* objects per group (ids are group-unique)  */

/* Object ids are unique within a GROUP: subgroups partition a group's object-id
 * space, they do NOT restart it, so subgroup sg carries ids
 * [sg*DP_O, sg*DP_O + DP_O). The tag binds payload bytes to (group, id) so a
 * mis-routed payload is caught. */
static uint8_t
dp_tag(uint64_t g, uint64_t oid)
{
    return (uint8_t)(0x40u + g * DP_OPG + oid);   /* distinct, nonzero       */
}

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
            rig_pump((r), 1);                                                 \
        }                                                                     \
        R_CHECK((r), rc_ == MOQ_OK);                                          \
    } while (0)

/* One subgroup: DP_O tagged data objects (payload byte 0 = dp_tag binds the
 * bytes to the identity, so a mis-routed payload is caught), then plain FIN. */
static void
dp_send_subgroup(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                 uint64_t group, uint64_t subgroup)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh));
    for (uint64_t o = 0; o < DP_O; o++) {
        uint64_t oid = subgroup * DP_O + o;   /* group-unique object id */
        uint8_t body[4] = { dp_tag(group, oid), (uint8_t)group,
                            (uint8_t)subgroup, (uint8_t)oid };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, oid, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

static int
dataplane_delivery_oracle(uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_delivery_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;   /* backpressure needs more turns */
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    /* Bounded range [0, DP_G-1]; the out-of-range group DP_G completes it. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.end_group = DP_G - 1;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);

    /* Publish the in-range matrix, then one out-of-range group to move the live
     * edge past the range end so the subscription terminates cleanly. */
    for (uint64_t g = 0; g < DP_G; g++) {
        for (uint64_t sg = 0; sg < DP_SG; sg++) {
            dp_send_subgroup(&rig, pub, pub_ps.up_sub, g, sg);
            rig_pump(&rig, 4);                                  /* flush live  */
            peer_drain(&rig, sub, &s_ps, false, false, 0, 0);   /* no pile-up  */
        }
    }
    /* Out-of-range group moves the live edge past the range end -> terminate. */
    dp_send_subgroup(&rig, pub, pub_ps.up_sub, DP_G, 0);
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);  /* keep origin live */
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);   /* sweep any tail */

    /* ---- oracle (wire-observable only) ---- */
    const uint32_t expect = DP_G * DP_SG * DP_O;
    R_CHECK(&rig, s_ps.done_seen);                     /* range terminated    */
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, (uint32_t)s_ps.dlv_n == expect);     /* exact count         */

    int seen[DP_G][DP_OPG];
    memset(seen, 0, sizeof(seen));
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);            /* nothing after terminal   */
        R_CHECK(&rig, d->sub == sh._opaque);      /* attributed to this sub   */
        R_CHECK(&rig, d->group < DP_G);           /* in range (no group DP_G) */
        R_CHECK(&rig, d->object_id < DP_OPG);
        R_CHECK(&rig, d->subgroup == d->object_id / DP_O);  /* right stream   */
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, d->fp == dp_tag(d->group, d->object_id));
        if (d->group < DP_G && d->object_id < DP_OPG) {
            seen[d->group][d->object_id]++;
        }
    }
    for (uint64_t g = 0; g < DP_G; g++) {
        for (uint64_t oid = 0; oid < DP_OPG; oid++) {
            R_CHECK(&rig, seen[g][oid] == 1);         /* exactly once         */
        }
        for (uint64_t sg = 0; sg < DP_SG; sg++) {
            int64_t last = -1;                        /* ascending per stream  */
            for (int i = 0; i < s_ps.dlv_n; i++) {
                dlv_rec_t *d = &s_ps.dlv[i];
                if (d->group == g && d->subgroup == sg) {
                    R_CHECK(&rig, (int64_t)d->object_id > last);
                    last = (int64_t)d->object_id;
                }
            }
        }
    }
    /* Exactly the in-range subgroups sealed — each once, none extra. A weaker
     * >= would let an out-of-range group's FIN (or a duplicate) inflate the
     * total unnoticed; assert the recorded (group, subgroup) FIN set precisely. */
    R_CHECK(&rig, s_ps.subgroup_finished == (int)(DP_G * DP_SG));
    for (uint64_t g = 0; g < DP_G; g++) {
        for (uint64_t sg = 0; sg < DP_SG; sg++) {
            int fins = 0;
            for (int fi = 0; fi < s_ps.finished_count; fi++) {
                if (s_ps.finished_gsg[fi][0] == g &&
                    s_ps.finished_gsg[fi][1] == sg) {
                    fins++;
                }
            }
            R_CHECK(&rig, fins == 1);   /* this in-range subgroup, sealed once */
        }
    }

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_delivery_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- Datagram + status-object delivery oracle ---------------------------- *
 * Extends the delivery-oracle framework to the object shapes it left open:
 * datagram-preference objects, datagram status objects, and stream status
 * objects.
 * Proves the relay forwards each shape with the right datagram flag, payload
 * presence, and status code, and that out-of-range shapes are filtered. */
static int
dataplane_datagram_status_oracle(uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_datagram_status_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.end_group = 1;   /* in-range groups 0,1; group 2 is the terminal */
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);

    /* group 0: a datagram object (o0), then a datagram status EOG (o1). */
    {
        uint8_t body[4] = { 0xA0, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_send_object_datagram(
                           pub->peer, pub_ps.up_sub, 0, 0, 7, false, pl, NULL,
                           0, rig.now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(&rig, moq_session_send_status_datagram(
                       pub->peer, pub_ps.up_sub, 0, 1, 7,
                       MOQ_OBJECT_END_OF_GROUP, rig.now));
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* group 1: a stream subgroup with a normal object (o0), then a stream
     * status EOG object (o1), then close. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 1;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t sgh;
        DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                 &sgc, rig.now, &sgh));
        uint8_t body[4] = { 0xB0, 1, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
        DP_RETRY(&rig, moq_session_write_status_object(
                           pub->peer, sgh, 1, MOQ_OBJECT_END_OF_GROUP,
                           rig.now));
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));
    }
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* group 2 (out of range): a datagram object, moving the live edge past the
     * range end so the subscription terminates. Must NOT be delivered. */
    {
        uint8_t body[4] = { 0xC0, 2, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_send_object_datagram(
                           pub->peer, pub_ps.up_sub, 2, 0, 7, false, pl, NULL,
                           0, rig.now));
        moq_rcbuf_decref(pl);
    }
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * The terminal is the range completion (the group-2 datagram moves the live
     * edge past end_group=1); the EOG statuses end their group/subgroup, not the
     * subscription. Each in-range shape is delivered exactly once with the right
     * datagram flag, payload presence, status code, and payload fingerprint. EOG
     * is carried by the STATUS field (st == END_OF_GROUP), NOT the end_of_group
     * header bit, so every delivered record has eog == false. */
    static const struct {
        uint64_t g, o;
        bool     dg, pn;
        uint8_t  st, fp;
    } want[4] = {
        { 0, 0, true,  false, MOQ_OBJECT_NORMAL,       0xA0 },  /* datagram obj  */
        { 0, 1, true,  true,  MOQ_OBJECT_END_OF_GROUP, 0x00 },  /* datagram stat */
        { 1, 0, false, false, MOQ_OBJECT_NORMAL,       0xB0 },  /* stream obj    */
        { 1, 1, false, true,  MOQ_OBJECT_END_OF_GROUP, 0x00 },  /* stream stat   */
    };
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 4);
    int seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);       /* nothing after terminal   */
        R_CHECK(&rig, d->sub == sh._opaque); /* attributed to this sub   */
        R_CHECK(&rig, d->group < 2);         /* in range: group 2 filtered */
        R_CHECK(&rig, !d->eog);              /* EOG via status, not the bit */
        int m = -1;
        for (int w = 0; w < 4; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);               /* matched an expected shape */
        if (m >= 0) {
            seen[m]++;
            R_CHECK(&rig, d->datagram == want[m].dg);
            R_CHECK(&rig, d->payload_null == want[m].pn);
            R_CHECK(&rig, d->status == want[m].st);
            R_CHECK(&rig, d->fp == want[m].fp);
        }
    }
    for (int w = 0; w < 4; w++) {
        R_CHECK(&rig, seen[w] == 1);         /* each shape exactly once  */
    }
    /* The one and only subgroup FIN is exactly (group 1, subgroup 0) — the
     * group-1 stream subgroup. Group-0 datagram/status shapes carry no subgroup
     * and the out-of-range group 2 is filtered, so neither may FIN. A total-only
     * check would pass on a bogus FIN (e.g. for the datagram group) masking a
     * missing real one; assert the recorded FIN identity precisely. */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_1_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 1 && s_ps.finished_gsg[fi][1] == 0) {
            fin_1_0++;
        }
    }
    R_CHECK(&rig, fin_1_0 == 1);   /* the FIN is (group 1, subgroup 0), once */

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_datagram_status_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- Retained pre-subscribe replay oracle -------------------------------- *
 * A subscriber that joins a WARM track (after the original subscriber left and
 * the relay released upstream) must be replayed the retained in-range history
 * exactly once, then the live tail — no duplicate, no gap, no out-of-filter
 * leak, and exactly one warm-track upstream rejoin. */
static uint8_t
re_tag(uint64_t g, uint64_t oid)
{
    return (uint8_t)(0x50u + g * 16u + oid);   /* distinct, nonzero */
}

/* A subgroup of `n` tagged objects with ids [oid_base, oid_base+n), plain FIN. */
static void
re_send_subgroup(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                 uint64_t group, uint64_t subgroup, uint64_t oid_base,
                 uint64_t n)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh));
    for (uint64_t i = 0; i < n; i++) {
        uint64_t oid = oid_base + i;
        uint8_t body[4] = { re_tag(group, oid), (uint8_t)group,
                            (uint8_t)subgroup, (uint8_t)oid };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, oid, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

static int
dataplane_retained_replay_oracle(uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_retained_replay_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *subA = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *subB = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && subA && subB);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    /* Sub A pulls the retained history into the relay log. */
    moq_subscribe_cfg_t scfgA;
    memset(&scfgA, 0, sizeof(scfgA));
    moq_subscribe_cfg_init(&scfgA);
    scfgA.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfgA.track_name = B("v");
    scfgA.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfgA.start_group = 0;
    scfgA.start_object = 0;
    moq_subscription_t shA;
    R_CHECK(&rig, moq_session_subscribe(subA->peer, &scfgA, rig.now, &shA) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, sA_ps, sB_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sA_ps, 0, sizeof(sA_ps));
    memset(&sB_ps, 0, sizeof(sB_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);

    /* Retained matrix: group 0 (sg0 o0-2), group 1 (sg0 o0-2, sg1 o3-5). */
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 0, 0, 0, 3);
    rig_pump(&rig, 4);
    peer_drain(&rig, subA, &sA_ps, false, false, 0, 0);
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 1, 0, 0, 3);
    rig_pump(&rig, 4);
    peer_drain(&rig, subA, &sA_ps, false, false, 0, 0);
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 1, 1, 3, 3);
    rig_pump(&rig, 4);
    peer_drain(&rig, subA, &sA_ps, false, false, 0, 0);

    /* Sub A leaves; the track lingers then warms (relay releases upstream). */
    R_CHECK(&rig, moq_session_unsubscribe(subA->peer, shA, rig.now) == MOQ_OK);
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        if (pub_ps.unsubscribed >= 1) {
            break;
        }
    }
    R_CHECK(&rig, pub_ps.unsubscribed == 1);   /* warm: upstream released */
    int up_subs_before = pub_ps.up_subs;

    /* Sub B joins the WARM track with a subset range [1,2]: it must replay
     * retained group 1 (NOT the retained group 0, before its start), then the
     * live tail (group 2). */
    moq_subscribe_cfg_t scfgB;
    memset(&scfgB, 0, sizeof(scfgB));
    moq_subscribe_cfg_init(&scfgB);
    scfgB.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfgB.track_name = B("v");
    scfgB.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfgB.start_group = 1;
    scfgB.start_object = 0;
    scfgB.end_group = 2;
    moq_subscription_t shB;
    R_CHECK(&rig, moq_session_subscribe(subB->peer, &scfgB, rig.now, &shB) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_drain(&rig, pub, &pub_ps, true, true, 1, 5);   /* warm rejoin */
    R_CHECK(&rig, pub_ps.up_subs == up_subs_before + 1);   /* exactly one */
    rig_pump(&rig, 6);
    peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);

    /* Live tail (group 2), then out-of-range group 3 completes the range. */
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 2, 0, 0, 3);
    rig_pump(&rig, 4);
    peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);
    re_send_subgroup(&rig, pub, pub_ps.up_sub, 3, 0, 0, 3);
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);
        if (sB_ps.done_seen) {
            break;
        }
    }
    peer_drain(&rig, subB, &sB_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * Sub B's delivered set is exactly the retained group 1 (both subgroups)
     * plus the live group-2 tail: 9 objects. The retained group 0 (before B's
     * start) and the out-of-range group 3 (which completes the range) are both
     * absent. Exactly one warm-track upstream rejoin happened (A's initial
     * SUBSCRIBE + B's rejoin = 2), i.e. no resubscribe storm. */
    static const struct { uint64_t g, o; } want[9] = {
        { 1, 0 }, { 1, 1 }, { 1, 2 },   /* retained group 1, subgroup 0 */
        { 1, 3 }, { 1, 4 }, { 1, 5 },   /* retained group 1, subgroup 1 */
        { 2, 0 }, { 2, 1 }, { 2, 2 },   /* live tail  group 2, subgroup 0 */
    };
    R_CHECK(&rig, sB_ps.done_seen);
    R_CHECK(&rig, sB_ps.dlv_overflow == 0);
    R_CHECK(&rig, sB_ps.dlv_n == 9);
    R_CHECK(&rig, pub_ps.up_subs == 2);   /* one warm rejoin, no storm */
    int seen[9] = { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    for (int i = 0; i < sB_ps.dlv_n; i++) {
        dlv_rec_t *d = &sB_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);           /* nothing after terminal   */
        R_CHECK(&rig, d->sub == shB._opaque);    /* attributed to sub B      */
        R_CHECK(&rig, d->group == 1 || d->group == 2); /* in range, not 0/3  */
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->status == MOQ_OBJECT_NORMAL);
        R_CHECK(&rig, d->subgroup == d->object_id / 3);  /* stream placement */
        R_CHECK(&rig, d->fp == re_tag(d->group, d->object_id));
        int m = -1;
        for (int w = 0; w < 9; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
        }
    }
    for (int w = 0; w < 9; w++) {
        R_CHECK(&rig, seen[w] == 1);   /* each expected object exactly once  */
    }
    /* Per-stream object order + exact subgroup FIN identity. The three replayed/
     * live streams each seal exactly once; no extra or missing FIN. */
    static const struct { uint64_t g, sg; } streams[3] = {
        { 1, 0 }, { 1, 1 }, { 2, 0 },
    };
    R_CHECK(&rig, sB_ps.subgroup_finished == 3);
    for (int s = 0; s < 3; s++) {
        int64_t last = -1;
        for (int i = 0; i < sB_ps.dlv_n; i++) {
            dlv_rec_t *d = &sB_ps.dlv[i];
            if (d->group == streams[s].g && d->subgroup == streams[s].sg) {
                R_CHECK(&rig, (int64_t)d->object_id > last);
                last = (int64_t)d->object_id;
            }
        }
        int fins = 0;
        for (int fi = 0; fi < sB_ps.finished_count; fi++) {
            if (sB_ps.finished_gsg[fi][0] == streams[s].g &&
                sB_ps.finished_gsg[fi][1] == streams[s].sg) {
                fins++;
            }
        }
        R_CHECK(&rig, fins == 1);   /* this stream sealed exactly once */
    }

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_retained_replay_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- Late-FIN seal oracle -------------------------------------------------- *
 * A subgroup FIN that reaches the relay only AFTER the subscriber already
 * received the subgroup's last object must still FIN the downstream stream.
 * The seal lands with nothing left to deliver, so no record delivery will
 * ever carry the final-record flag — the FIN rides the acknowledged SEAL
 * notice instead. Before that notice existed, this stream never closed. */
static int
dataplane_late_fin_oracle(uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_late_fin_oracle rig create (aq=%u)\n",
               max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);

    /* One object on (group 0, subgroup 0); the upstream stream stays OPEN. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sgh));
    {
        uint8_t body[4] = { 0x77, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    /* The subscriber receives the object while the subgroup is unsealed —
     * this delivery cannot carry the final-record flag. */
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.dlv_n >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.dlv_n == 1);
    R_CHECK(&rig, s_ps.dlv[0].fp == 0x77);
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subgroup_finished == 0);   /* genuinely late FIN */

    /* The late FIN: sealed with nothing left to deliver. */
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.subgroup_finished >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_0_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 0 && s_ps.finished_gsg[fi][1] == 0) {
            fin_0_0++;
        }
    }
    R_CHECK(&rig, fin_0_0 == 1);       /* exactly (group 0, subgroup 0)   */
    R_CHECK(&rig, s_ps.dlv_n == 1);    /* the notice delivered no record  */
    rig_pump(&rig, 4);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subgroup_finished == 1);   /* acknowledged once   */

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_late_fin_oracle (aq=%u)\n", max_actions);
    }
    return f;
}

/* ---- FIN-under-backpressure oracle ---------------------------------------- *
 * The final object's write fits the pump's action quota but the subgroup
 * close would not: with a quota of ONE action per pump, the close can never
 * share a pump with the delivery. The FIN must still arrive — it rides the
 * acknowledged SEAL notice, whose close retries on WOULD_BLOCK — and the
 * object must not be re-delivered. (A close piggybacked on the record
 * delivery and swallowed on WOULD_BLOCK would lose the FIN forever.) */
static int
dataplane_fin_backpressure_oracle(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 1;   /* one session action per pump */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_fin_backpressure_oracle rig create\n");
        return 1;
    }
    int pumps = 300;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    for (int k = 0; k < pumps && !pub_ps.up_seen; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    }
    R_CHECK(&rig, pub_ps.up_seen);

    /* One object, then the upstream FIN right behind it — the seal reaches
     * the relay while the record is still owed downstream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sgh));
    {
        uint8_t body[4] = { 0x66, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));

    /* Under the 1-action quota the delivery and the close land on different
     * pumps; both must eventually arrive, each exactly once. */
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.dlv_n >= 1 && s_ps.subgroup_finished >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.dlv_n == 1);
    R_CHECK(&rig, s_ps.dlv[0].fp == 0x66);
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_0_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 0 && s_ps.finished_gsg[fi][1] == 0) {
            fin_0_0++;
        }
    }
    R_CHECK(&rig, fin_0_0 == 1);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.dlv_n == 1);              /* no repeated object   */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);  /* exactly one FIN      */

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_fin_backpressure_oracle\n");
    }
    return f;
}

/* ---- Header-EOG cadence oracle ---------------------------------------------- *
 * end_of_group is subgroup-header METADATA: opening an EOG subgroup does not
 * make its first object a terminal, and objects keep flowing until the actual
 * upstream FIN. Under a one-action-per-pump quota: object 0 arrives with the
 * header bit preserved and NO FIN; object 1 follows; only the explicit
 * upstream close produces the (notice-driven, retry-safe) downstream FIN —
 * exactly one, with no duplicate object afterwards. */
static int
dataplane_eog_backpressure_oracle(bool streaming, moq_version_t ver)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 1;   /* one session action per pump */
    /* streaming = the production relay ingest representation (chunk-through
     * OPEN records); the header-EOG metadata must survive it identically. */
    g_test_server_streaming_objects = streaming;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        g_test_server_streaming_objects = false;
        printf("FAIL: dataplane_eog_backpressure_oracle rig create\n");
        return 1;
    }
    int pumps = 300;
    conn_t *pub = rig_connect(&rig, ver);
    conn_t *sub = rig_connect(&rig, ver);
    g_test_server_streaming_objects = false;
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    for (int k = 0; k < pumps && !pub_ps.up_seen; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    }
    R_CHECK(&rig, pub_ps.up_seen);

    /* An EOG subgroup: the header bit rides every record; nothing about it
     * terminates the stream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    sgc.end_of_group = true;
    moq_subgroup_handle_t sgh;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sgh));
    {
        uint8_t body[4] = { 0x55, 0, 0, 0 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 0, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    /* Object 0 arrives with the header bit — and NO FIN: an EOG header must
     * never be treated as a per-record terminal. */
    for (int k = 0; k < pumps && s_ps.dlv_n < 1; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, s_ps.dlv_n == 1);
    R_CHECK(&rig, s_ps.dlv[0].fp == 0x55);
    R_CHECK(&rig, s_ps.dlv[0].eog);              /* header bit preserved */
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subgroup_finished == 0);  /* stream still open    */

    /* Object 1, then the actual graceful terminal. */
    {
        uint8_t body[4] = { 0x56, 0, 0, 1 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh, 1, pl,
                                                rig.now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now));

    /* Under the quota the delivery and the close land on different pumps;
     * both objects exactly once, then exactly one FIN. */
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.dlv_n >= 2 && s_ps.subgroup_finished >= 1) {
            break;
        }
    }
    R_CHECK(&rig, s_ps.dlv_n == 2);
    R_CHECK(&rig, s_ps.dlv[1].fp == 0x56);
    R_CHECK(&rig, s_ps.dlv[1].eog);
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    int fin_0_0 = 0;
    for (int fi = 0; fi < s_ps.finished_count; fi++) {
        if (s_ps.finished_gsg[fi][0] == 0 && s_ps.finished_gsg[fi][1] == 0) {
            fin_0_0++;
        }
    }
    R_CHECK(&rig, fin_0_0 == 1);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.dlv_n == 2);              /* no repeated object   */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);  /* exactly one FIN      */

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_eog_backpressure_oracle (streaming=%d v=%d)\n",
               streaming ? 1 : 0, (int)ver);
    }
    return f;
}

static void ev_write_obj(rig_t *r, conn_t *pub, moq_subgroup_handle_t h,
                         uint64_t group, uint64_t object_id);

/* ---- Header-EOG contradictory-final oracle ---------------------------------- *
 * An EOG subgroup reaches object 20 while ANOTHER subgroup admits object 21
 * before the EOG FIN. The FIN's deferred final (20) would contradict retained
 * object 21: the seal must reject atomically and the relay must fail the
 * publisher closed — a cache must never retain an object beyond its declared
 * final location. */
static int
dataplane_eog_conflict_fail_closed(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: dataplane_eog_conflict_fail_closed rig create\n");
        return 1;
    }
    int pumps = 120;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);

    /* EOG subgroup 0 with object 20 (left open)... */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    sgc.end_of_group = true;
    moq_subgroup_handle_t sg0;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sg0));
    ev_write_obj(&rig, pub, sg0, 0, 20);
    rig_pump(&rig, 6);
    /* ...another subgroup admits object 21 BEFORE the EOG FIN... */
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 1;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sg1;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                             rig.now, &sg1));
    ev_write_obj(&rig, pub, sg1, 0, 21);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ...the FIN's deferred final (20) contradicts retained 21: fail closed. */
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sg0, rig.now));
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        if (pub_ps.session_closed) {
            break;
        }
    }
    R_CHECK(&rig, pub_ps.session_closed);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_eog_conflict_fail_closed\n");
    }
    return f;
}

/* ---- Eviction / skip correctness oracle ---------------------------------- *
 * When retention evicts data under a live subscriber, the relay must surface the
 * skip (observability), deliver only the coherent set (no dup / no gap / no
 * stale group), and NOT close a still-retained lower group's downstream stream.
 * Seeded from subgroup_eviction_spares_live_lower_group. */
static uint8_t
ev_tag(uint64_t g, uint64_t o)
{
    return (uint8_t)(0x90u + g * 8u + o);   /* distinct per (group, object) */
}

static void
ev_write_obj(rig_t *r, conn_t *pub, moq_subgroup_handle_t h, uint64_t group,
             uint64_t object_id)
{
    uint8_t body[4] = { ev_tag(group, object_id), (uint8_t)group,
                        (uint8_t)object_id, 0x33 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, h, object_id, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
}

/* Count MOQR_TRACE_CURSOR_SKIP records in the relay's attached trace ring. The
 * production delivery path (the gpos scheduler in moqr_core_next_delivery) emits
 * this trace when it jumps a subscriber past evicted content, so a skip is
 * observable both here and via the downstream eviction-close. */
static int
ev_skip_count(rig_t *r)
{
    moqr_trace_rec_t recs[512];
    size_t n = moqr_trace_read(r->trace, recs, 512);
    int skips = 0;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].kind == MOQR_TRACE_CURSOR_SKIP) {
            skips++;
        }
    }
    return skips;
}

static int
dataplane_eviction_skip_oracle(void)
{
    ca_t a;
    ca_init(&a);
    g_test_log_max_groups = 2;   /* group 0 evicts when group 2 is ingested */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_log_max_groups = 0;
        printf("FAIL: dataplane_eviction_skip_oracle rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;   /* open-ended */
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    /* Deliver g0o0 AND g1o0 first: both downstream subgroups open, both cursors
     * advanced (held OPEN, no FIN). */
    moq_subgroup_handle_t h0, h1, h2;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 0;
    sc.subgroup_id = 0;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h0) == MOQ_OK);
    ev_write_obj(&rig, pub, h0, 0, 0);
    sc.group_id = 1;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h1) == MOQ_OK);
    ev_write_obj(&rig, pub, h1, 1, 0);
    rig_pump(&rig, 8);   /* deliver g0o0, g1o0; both cursors advanced */
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    int skips_before = ev_skip_count(&rig);

    /* Bundle a NEW group-0 object (g0o1, which the g0 cursor now wants) with
     * group 2, ingested in ONE bind pump: group 2's arrival evicts group 0,
     * dropping g0o1 undelivered, and the acknowledged EVICT_WATERMARK notice
     * precedes group 2's record while group 1 stays RETAINED and open — the
     * exact shape that exposed the watermark bug (a `group < delivered`
     * cleanup would wrongly close group 1). */
    ev_write_obj(&rig, pub, h0, 0, 1);   /* g0o1: evicted undelivered */
    sc.group_id = 2;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                            rig.now, &h2) == MOQ_OK);
    ev_write_obj(&rig, pub, h2, 2, 0);
    rig_pump(&rig, 8);   /* g0 evicts (g0o1 dropped); skip lands on group 2 */
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    int skips_after = ev_skip_count(&rig);

    /* More data on the still-RETAINED group 1 (must stay on its open stream). */
    ev_write_obj(&rig, pub, h1, 1, 1);
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * Coherent delivered set: the evicted-undelivered g0 o1 is ABSENT, so the set
     * is exactly {g0o0, g1o0, g2o0, g1o1} — no duplicate, no stale/out-of-window
     * object, fingerprints exact. The skip is REPORTED, not silently swallowed:
     * the relay closed the evicted group 0's downstream subgroup (a
     * SUBGROUP_FINISHED for (0,0) the origin never sent — h0 stays open), so the
     * subscriber sees group 0 end cleanly. The still-retained lower group 1 keeps
     * its OPEN stream: NOT closed/reopened (the watermark invariant).
     *
     * The skip is ALSO surfaced in the relay's trace ring: the production gpos
     * scheduler emits exactly one MOQR_TRACE_CURSOR_SKIP for the jump past evicted
     * group 0 (skips_after == skips_before + 1). */
    static const struct { uint64_t g, o; } want[4] = {
        { 0, 0 }, { 1, 0 }, { 2, 0 }, { 1, 1 },
    };
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 4);
    R_CHECK(&rig, !s_ps.done_seen);             /* open-ended: no early terminal */
    R_CHECK(&rig, s_ps.subscribe_errors == 0);
    R_CHECK(&rig, !s_ps.session_closed);
    R_CHECK(&rig, !s_ps.reopen_after_finish);   /* retained g1 stream not reopened */
    R_CHECK(&rig, skips_after == skips_before + 1);  /* one skip, traced */
    int seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, d->sub == sh._opaque);
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->subgroup == 0);
        R_CHECK(&rig, d->fp == ev_tag(d->group, d->object_id));
        int m = -1;
        for (int w = 0; w < 4; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
        }
    }
    for (int w = 0; w < 4; w++) {
        R_CHECK(&rig, seen[w] == 1);   /* each object exactly once, no dup/dropped */
    }
    int64_t last = -1;   /* group 1 subgroup 0 objects ascending in delivery */
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        if (d->group == 1 && d->subgroup == 0) {
            R_CHECK(&rig, (int64_t)d->object_id > last);
            last = (int64_t)d->object_id;
        }
    }
    /* The skip's wire signal: exactly the evicted group 0's subgroup was closed
     * by the relay (not the origin), and only that one. */
    R_CHECK(&rig, s_ps.subgroup_finished == 1);
    R_CHECK(&rig, s_ps.finished_count == 1);
    R_CHECK(&rig, s_ps.finished_gsg[0][0] == 0 && s_ps.finished_gsg[0][1] == 0);

    (void)moq_session_close_subgroup(pub->peer, h1, rig.now);
    (void)moq_session_close_subgroup(pub->peer, h2, rig.now);
    rig_pump(&rig, 8);

    g_test_log_max_groups = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_eviction_skip_oracle\n");
    }
    return f;
}

/* ---- Source-end termination oracle -------------------------------------- *
 * When the source ends — a PULL upstream sends SUBSCRIBE_DONE, or a PUSH
 * publisher sends PUBLISH_FINISHED — the relay must terminate open-ended
 * downstream subscribers with a wire SUBSCRIBE_DONE carrying the forwarded
 * status, after delivering every pre-end object exactly once. Both source kinds
 * route through moqr_core_source_done. */
static uint8_t
se_tag(uint64_t g, uint64_t o)
{
    return (uint8_t)(0x30u + g * 8u + o);   /* distinct per (group, object) */
}

/* One group's subgroup 0 with `n` tagged objects, plain FIN. Writes on the
 * pull subscription (open_subgroup) or the push publication (open_pub_subgroup). */
static void
se_send_group(rig_t *r, conn_t *pub, bool push, moq_subscription_t up_sub,
              moq_publication_t pubh, uint64_t group, uint64_t n)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    if (push) {
        DP_RETRY(r, moq_session_open_pub_subgroup(pub->peer, pubh, &sgc,
                                                  r->now, &sgh));
    } else {
        DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                              &sgh));
    }
    for (uint64_t o = 0; o < n; o++) {
        uint8_t body[4] = { se_tag(group, o), (uint8_t)group, 0, (uint8_t)o };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, o, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

static int
dataplane_source_end_oracle(bool push, uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_source_end_oracle rig create (push=%d aq=%u)\n",
               (int)push, max_actions);
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t up_sub = { 0 };
    moq_publication_t pubh = { 0 };

    if (push) {
        /* PUSH: the origin publishes the track; the relay accepts it. */
        moq_publish_cfg_t pubc;
        memset(&pubc, 0, sizeof(pubc));
        moq_publish_cfg_init(&pubc);
        pubc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        pubc.track_name = B("v");
        R_CHECK(&rig, moq_session_publish(pub->peer, &pubc, rig.now, &pubh) ==
                          MOQ_OK);
        rig_pump(&rig, 6);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        R_CHECK(&rig, pub_ps.publish_ok);
    } else {
        /* PULL: the origin announces; the relay subscribes upstream. */
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                    &ann) == MOQ_OK);
        rig_pump(&rig, 4);
    }

    /* Open-ended downstream subscriber. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    if (!push) {
        peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
        R_CHECK(&rig, pub_ps.up_seen);
        up_sub = pub_ps.up_sub;
        rig_pump(&rig, 6);
    }
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Deterministic matrix: group 0 and group 1, each subgroup 0 with objects
     * 0,1, plain FIN. */
    se_send_group(&rig, pub, push, up_sub, pubh, 0, 2);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    se_send_group(&rig, pub, push, up_sub, pubh, 1, 2);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Source end with a nonzero status. */
    if (push) {
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        fcfg.status_code = 0x4;
        R_CHECK(&rig, moq_session_finish_publish(pub->peer, pubh, &fcfg,
                                                 rig.now) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x4;
        R_CHECK(&rig, moq_session_done_subscribe(pub->peer, up_sub, &dcfg,
                                                 rig.now) == MOQ_OK);
    }
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * Every pre-end object was delivered exactly once (correct fingerprint, per-
     * stream order), then exactly one wire SUBSCRIBE_DONE carrying the forwarded
     * source status (0x4), with nothing delivered after it, and both plain-FIN
     * subgroups sealed by identity. Identical for the pull (SUBSCRIBE_DONE) and
     * push (PUBLISH_FINISHED) source kinds. */
    static const struct { uint64_t g, o; } want[4] = {
        { 0, 0 }, { 0, 1 }, { 1, 0 }, { 1, 1 },
    };
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 4);
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.done_count == 1);    /* exactly one SUBSCRIBE_DONE     */
    R_CHECK(&rig, s_ps.done_code == 0x4);   /* source status forwarded        */
    int seen[4] = { 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);       /* nothing after terminal        */
        R_CHECK(&rig, d->sub == sh._opaque);
        R_CHECK(&rig, !d->payload_null);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->status == MOQ_OBJECT_NORMAL);
        R_CHECK(&rig, d->subgroup == 0);
        R_CHECK(&rig, d->fp == se_tag(d->group, d->object_id));
        int m = -1;
        for (int w = 0; w < 4; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
        }
    }
    for (int w = 0; w < 4; w++) {
        R_CHECK(&rig, seen[w] == 1);         /* each object exactly once      */
    }
    for (uint64_t g = 0; g < 2; g++) {
        int64_t last = -1;                   /* per-stream ascending order    */
        for (int i = 0; i < s_ps.dlv_n; i++) {
            dlv_rec_t *d = &s_ps.dlv[i];
            if (d->group == g && d->subgroup == 0) {
                R_CHECK(&rig, (int64_t)d->object_id > last);
                last = (int64_t)d->object_id;
            }
        }
    }
    /* Exact FIN identity: (0,0) and (1,0), each sealed once, before source end. */
    R_CHECK(&rig, s_ps.subgroup_finished == 2);
    for (uint64_t g = 0; g < 2; g++) {
        int fins = 0;
        for (int fi = 0; fi < s_ps.finished_count; fi++) {
            if (s_ps.finished_gsg[fi][0] == g &&
                s_ps.finished_gsg[fi][1] == 0) {
                fins++;
            }
        }
        R_CHECK(&rig, fins == 1);
    }

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_source_end_oracle (push=%d aq=%u)\n", (int)push,
               max_actions);
    }
    return f;
}

/* A PUBLISH-created source is not a pull subscription.  The last downstream
 * subscriber leaving must therefore not run the pull-source linger teardown:
 * there is no upstream SUBSCRIBE to cancel, and warming the core track while
 * the accepted PUBLISH remains live turns every later pushed object into an
 * ingest refusal.  This is the exact lifecycle observed with the physical
 * MoQCam publisher and moq5-relay on moqbox.
 *
 * Keep the policy neutral: this oracle does not require a demand-gated
 * PUBLISH_UPDATE.  It requires only the current accepted-PUBLISH contract to
 * stay coherent -- while that publication remains permitted to send, its
 * backing relay track remains ACTIVE and accepts data.  Two downstream churn
 * cycles pin the accumulation class seen on the physical Camera relay without
 * making this deterministic unit test depend on live deployment. */
static int
dataplane_push_survives_downstream_linger(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: push-survives-linger rig create\n");
        return 1;
    }

    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("push"), B("linger") };
    /* Match media_sender's real topology: the catalog track advertises the
     * namespace, while the media track is also opened with PUBLISH.  This is
     * what makes a stale WARM transition especially destructive: a later
     * viewer can manufacture a pull SUBSCRIBE alongside the live PUBLISH. */
    moq_publish_namespace_cfg_t ncfg;
    moq_publish_namespace_cfg_init(&ncfg);
    ncfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &ncfg, rig.now,
                                                 &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    pc.track_name = B("video");
    moq_publication_t pubh;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok && !pub_ps.publish_error);

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.subscribe_ok);
    R_CHECK(&rig, pub_ps.up_subs == 0); /* PUBLISH already owns the source. */

    se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 0, 1);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.objects == 1 && sub_ps.last_group == 0);

    moqr_bind_stats_t before;
    moqr_bind_get_stats(rig.bind, &before);

    /* Retire the only downstream and cross the configured 500us linger. */
    R_CHECK(&rig, moq_session_unsubscribe(sub->peer, sh, rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);

    /* The accepted PUBLISH remains the source, hence the track must not have
     * been turned WARM by a pull-only teardown. */
    char route[4096];
    size_t route_len = 0;
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, route, sizeof(route),
                                             &route_len) == MOQR_OK);
    R_CHECK(&rig, route_len < sizeof(route));
    R_CHECK(&rig, strstr(route,
                         "\"name\":\"video\",\"state\":\"active\"") != NULL);

    /* Data still permitted by the publication must remain acceptable.  The
     * buggy implementation increments ingest_refusals here because the core
     * track is WARM even though the wire publication is still established. */
    se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 1, 1);
    rig_pump(&rig, 6);
    moqr_bind_stats_t after;
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);

    /* A new downstream binds directly to the still-live push source.  It must
     * not manufacture a pull SUBSCRIBE alongside the existing PUBLISH. */
    conn_t *sub2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub2 != NULL);
    peer_state_t sub2_ps;
    memset(&sub2_ps, 0, sizeof(sub2_ps));
    rig_pump(&rig, 4);
    moq_subscription_t sh2;
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &sc, rig.now, &sh2) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, sub2, &sub2_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_subs == 0);
    R_CHECK(&rig, sub2_ps.subscribe_ok);

    int sub2_objects = sub2_ps.objects;
    se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 2, 1);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub2, &sub2_ps, false, false, 0, 0);
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);
    R_CHECK(&rig, sub2_ps.objects == sub2_objects + 1 &&
                  sub2_ps.last_group == 2);

    /* Repeat the churn once more.  The deployed unfixed relay failed only
     * after accumulated viewer sessions, so keep the deterministic regression
     * from becoming a single-transition special case. */
    R_CHECK(&rig, moq_session_unsubscribe(sub2->peer, sh2, rig.now) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);

    route_len = 0;
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, route, sizeof(route),
                                             &route_len) == MOQR_OK);
    R_CHECK(&rig, route_len < sizeof(route));
    R_CHECK(&rig, strstr(route,
                         "\"name\":\"video\",\"state\":\"active\"") != NULL);

    se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 3, 1);
    rig_pump(&rig, 6);
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);

    conn_t *sub3 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, sub3 != NULL);
    peer_state_t sub3_ps;
    memset(&sub3_ps, 0, sizeof(sub3_ps));
    rig_pump(&rig, 4);
    moq_subscription_t sh3;
    R_CHECK(&rig, moq_session_subscribe(sub3->peer, &sc, rig.now, &sh3) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, sub3, &sub3_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_subs == 0);
    R_CHECK(&rig, sub3_ps.subscribe_ok);

    int sub3_objects = sub3_ps.objects;
    se_send_group(&rig, pub, true, MOQ_SUBSCRIPTION_INVALID, pubh, 4, 1);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub3, &sub3_ps, false, false, 0, 0);
    moqr_bind_get_stats(rig.bind, &after);
    R_CHECK(&rig, after.ingest_refusals == before.ingest_refusals);
    R_CHECK(&rig, sub3_ps.objects == sub3_objects + 1 &&
                  sub3_ps.last_group == 4);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_push_survives_downstream_linger\n");
    }
    return f;
}

/* ---- Mixed d16/d18 object bridging oracle -------------------------------- *
 * Real wire objects from a publisher on one draft must be delivered correctly to
 * a subscriber on the other draft, through the production binding — the relay
 * ingests on the publisher's profile and re-emits on the subscriber's. Runs both
 * directions (d16->d18 and d18->d16). */
static uint8_t
md_tag(uint64_t g, uint64_t o)
{
    return (uint8_t)(0x20u + g * 8u + o);   /* distinct per (group, object) */
}

/* One group's subgroup 0: `n` normal tagged objects (ids 0..n-1), optionally a
 * terminal EOG status object (id n), plain FIN. */
static void
md_send_group(rig_t *r, conn_t *pub, moq_subscription_t up_sub, uint64_t group,
              uint64_t n, bool eog)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh));
    for (uint64_t o = 0; o < n; o++) {
        uint8_t body[4] = { md_tag(group, o), (uint8_t)group, 0, (uint8_t)o };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, o, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    if (eog) {
        DP_RETRY(r, moq_session_write_status_object(pub->peer, sgh, n,
                                                    MOQ_OBJECT_END_OF_GROUP,
                                                    r->now));
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

static int
dataplane_mixed_draft_oracle(moq_version_t pub_ver, moq_version_t sub_ver,
                             uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: dataplane_mixed_draft_oracle rig create\n");
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = rig_connect(&rig, pub_ver);
    conn_t *sub = rig_connect(&rig, sub_ver);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    moq_subscription_t up_sub = pub_ps.up_sub;
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Matrix: group 0 (2 normal objects, plain FIN), group 1 (2 normal + a
     * terminal EOG status object, plain FIN). */
    md_send_group(&rig, pub, up_sub, 0, 2, false);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    md_send_group(&rig, pub, up_sub, 1, 2, true);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Source end (forwarded across drafts). */
    moq_done_subscribe_cfg_t dcfg;
    moq_done_subscribe_cfg_init(&dcfg);
    dcfg.status_code = 0x4;
    R_CHECK(&rig, moq_session_done_subscribe(pub->peer, up_sub, &dcfg,
                                             rig.now) == MOQ_OK);
    for (int k = 0; k < pumps; k++) {
        rig_pump(&rig, 1);
        peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * The publisher's draft-X wire objects arrive at the draft-Y subscriber
     * unchanged in identity: exactly {g0 o0,o1; g1 o0,o1 normal; g1 o2 EOG
     * status} = 5 records, each once, exact fingerprint, status (NORMAL for the
     * four data objects, END_OF_GROUP for the status object), per-stream order,
     * both plain-FIN subgroups sealed by identity, and the source-end
     * SUBSCRIBE_DONE with its status forwarded — all bridged across drafts. */
    static const struct {
        uint64_t g, o;
        uint8_t  st, fp;
        bool     pn;
    } want[5] = {
        { 0, 0, MOQ_OBJECT_NORMAL,       0x20, false },
        { 0, 1, MOQ_OBJECT_NORMAL,       0x21, false },
        { 1, 0, MOQ_OBJECT_NORMAL,       0x28, false },
        { 1, 1, MOQ_OBJECT_NORMAL,       0x29, false },
        { 1, 2, MOQ_OBJECT_END_OF_GROUP, 0x00, true  },
    };
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 5);
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.done_count == 1);    /* exactly one SUBSCRIBE_DONE     */
    R_CHECK(&rig, s_ps.done_code == 0x4);   /* source status forwarded        */
    int seen[5] = { 0, 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);
        R_CHECK(&rig, d->sub == sh._opaque);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->subgroup == 0);
        int m = -1;
        for (int w = 0; w < 5; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
            R_CHECK(&rig, d->status == want[m].st);
            R_CHECK(&rig, d->payload_null == want[m].pn);
            R_CHECK(&rig, d->fp == want[m].fp);
        }
    }
    for (int w = 0; w < 5; w++) {
        R_CHECK(&rig, seen[w] == 1);
    }
    for (uint64_t g = 0; g < 2; g++) {
        int64_t last = -1;                   /* per-stream ascending order    */
        for (int i = 0; i < s_ps.dlv_n; i++) {
            dlv_rec_t *d = &s_ps.dlv[i];
            if (d->group == g && d->subgroup == 0) {
                R_CHECK(&rig, (int64_t)d->object_id > last);
                last = (int64_t)d->object_id;
            }
        }
    }
    R_CHECK(&rig, s_ps.subgroup_finished == 2);   /* exact FIN identity */
    for (uint64_t g = 0; g < 2; g++) {
        int fins = 0;
        for (int fi = 0; fi < s_ps.finished_count; fi++) {
            if (s_ps.finished_gsg[fi][0] == g &&
                s_ps.finished_gsg[fi][1] == 0) {
                fins++;
            }
        }
        R_CHECK(&rig, fins == 1);
    }

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_mixed_draft_oracle (pub=%d sub=%d aq=%u)\n",
               (int)pub_ver, (int)sub_ver, max_actions);
    }
    return f;
}

/* ---- Close churn while work is in flight --------------------------------- *
 * Connections disappearing mid-delivery / mid-lifecycle must leave no hang, no
 * stale-session touch (ASan), no duplicate terminal, no leaked allocator ref,
 * and no retained downstream route state. Deterministic SimPair, no wall clock. */
static int
close_churn_downstream(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: close_churn_downstream rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    moq_subscription_t up_sub = pub_ps.up_sub;
    rig_pump(&rig, 6);

    /* Publish a small matrix, then pump only PARTIALLY: objects + FINs are still
     * in flight when the subscriber vanishes. */
    dp_send_subgroup(&rig, pub, up_sub, 0, 0);
    dp_send_subgroup(&rig, pub, up_sub, 1, 0);
    rig_pump(&rig, 3);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects >= 1);   /* delivery genuinely in flight */

    /* Subscriber disappears mid-delivery. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, sub->rsess) == MOQR_OK);
    rig_pump(&rig, 30);   /* to quiescence — relay must not touch the dead conn */

    moqr_core_stats_t cs;
    moqr_core_get_stats(rig.core, &cs);
    /* The relay dropped the downstream subscription — no retained active sub. */
    R_CHECK(&rig, cs.subs == 0);
    /* No NEW delivery is produced for the departed peer after the close is
     * processed: drain, pump more, drain again — the count must not grow. */
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    int settled = s_ps.objects;
    rig_pump(&rig, 20);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == settled);   /* relay stopped delivering */

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);   /* no leaked refs across the churn */
    if (f == 0 && rig.failures == 0) {
        printf("PASS: close_churn_downstream\n");
    }
    return f + rig.failures;
}

static int
close_churn_publisher(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: close_churn_publisher rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    moq_subscription_t up_sub = pub_ps.up_sub;
    rig_pump(&rig, 6);

    /* Publisher serves an object, then its transport drops before source-end. */
    dp_send_subgroup(&rig, pub, up_sub, 0, 0);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects >= 1);

    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, pub->rsess) == MOQR_OK);
    rig_pump(&rig, 30);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    moqr_core_stats_t cs;
    moqr_core_get_stats(rig.core, &cs);

    /* The failover contract: an abrupt publisher close with no alternate
     * announce TERMINATES the downstream subscriber explicitly — exactly
     * one SUBSCRIBE_DONE, never a silently retained sourceless
     * subscription. The namespace stays reclaimable (asserted below). */
    R_CHECK(&rig, cs.subs == 0);            /* retired via explicit terminal */
    R_CHECK(&rig, !s_ps.session_closed);
    R_CHECK(&rig, s_ps.subscribe_errors == 0);
    R_CHECK(&rig, s_ps.done_count == 1);    /* the one explicit terminal     */

    /* Reclaimability: a fresh publisher re-announces the same namespace and a
     * fresh subscriber is served — the abrupt close did not wedge the
     * namespace/track routing. */
    conn_t *pub2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub2 && sub2);
    rig_pump(&rig, 4);
    moq_publish_namespace_cfg_t pcfg2;
    memset(&pcfg2, 0, sizeof(pcfg2));
    moq_publish_namespace_cfg_init(&pcfg2);
    pcfg2.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann2;
    R_CHECK(&rig, moq_session_publish_namespace(pub2->peer, &pcfg2, rig.now,
                                                &ann2) == MOQ_OK);
    rig_pump(&rig, 4);
    moq_subscription_t sh2;
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &scfg, rig.now, &sh2) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub2_ps, s2_ps;
    memset(&pub2_ps, 0, sizeof(pub2_ps));
    memset(&s2_ps, 0, sizeof(s2_ps));
    peer_drain(&rig, pub2, &pub2_ps, true, false, 0, 0);
    R_CHECK(&rig, pub2_ps.up_seen);
    rig_pump(&rig, 6);
    dp_send_subgroup(&rig, pub2, pub2_ps.up_sub, 5, 0);
    rig_pump(&rig, 8);
    peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);

    R_CHECK(&rig, s2_ps.objects >= 1);   /* namespace reclaimable + serving */
    (void)cs;

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: close_churn_publisher\n");
    }
    return f + rig.failures;
}

static int
close_churn_pending_scalar(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 0;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: close_churn_pending_scalar rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* roomy */
    conn_t *warm = rig_connect(&rig, MOQ_VERSION_DRAFT_18);  /* roomy: warms */
    g_test_max_actions = 1;                                  /* depth-1 rejoin */
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 0;
    R_CHECK(&rig, pub && warm && sub);
    rig_pump(&rig, 4);

    /* Warm two tracks with a roomy subscriber (WARM is a track-side state). */
    red_warm_track(&rig, pub, warm, B("a"));
    red_warm_track(&rig, pub, warm, B("b"));

    /* Stage BOTH warm-rejoin subscribes from the depth-1 sub, then ONE pump so a
     * scalar (the 2nd rejoin's ACCEPT_SUB — this is exactly the queued
     * ACCEPT_SUB + UPSTREAM_SUBSCRIBE churn) blocks into the pending ring. */
    moq_subscribe_cfg_t rc;
    memset(&rc, 0, sizeof(rc));
    moq_subscribe_cfg_init(&rc);
    rc.track_name = B("v");
    rc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_bytes_t nsa[2] = { B("live"), B("a") };
    rc.track_namespace = (moq_namespace_t){ .parts = nsa, .count = 2 };
    moq_subscription_t rha;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &rc, rig.now, &rha) ==
                      MOQ_OK);
    red_deliver(&rig, sub);
    moq_bytes_t nsb[2] = { B("live"), B("b") };
    rc.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 2 };
    moq_subscription_t rhb;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &rc, rig.now, &rhb) ==
                      MOQ_OK);
    red_deliver(&rig, sub);
    (void)moqr_bind_pump(rig.bind, rig.now);

    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.pending_nonscalar_blocked == 0);
    R_CHECK(&rig, bs.pending_high_water > 0);   /* a scalar IS pending */

    /* Close the depth-1 subscriber WITH the scalar still pending: the purge must
     * drop it — never retry it against the dead session. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, sub->rsess) == MOQR_OK);
    rig_pump(&rig, 40);   /* quiescence — no touch of the closed session */

    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.pending_nonscalar_blocked == 0);
    moqr_core_stats_t cs;
    moqr_core_get_stats(rig.core, &cs);
    R_CHECK(&rig, cs.subs == 0);   /* the closed sub's subscriptions are gone */

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);   /* purge freed the pending, no leak */
    if (f == 0 && rig.failures == 0) {
        printf("PASS: close_churn_pending_scalar\n");
    }
    return f + rig.failures;
}

/* ---- Route/subscription projection oracle -------------------------------- *
 * The relay's route dump + stats must reflect the SAME logical state as the
 * wire-observable scenario. A small local model is updated at each step and
 * compared against get_stats, route_dump_json, route-epoch monotonicity, and the
 * wire events (namespace found/gone, subscribe OK/terminal). */
#define J_HAS(hay, needle) (strstr((hay), (needle)) != NULL)

static int
route_projection_oracle(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: route_projection_oracle rig create\n");
        return 1;
    }
    char buf[4096];
    size_t w = 0;
    moqr_core_stats_t cs;

    /* Empty baseline. */
    moqr_core_get_stats(rig.core, &cs);
    uint64_t epoch = cs.route_epoch;
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, buf, sizeof(buf), &w) ==
                      MOQR_OK);
    R_CHECK(&rig, J_HAS(buf, "\"announces\":[]"));
    R_CHECK(&rig, J_HAS(buf, "\"namespace_subscriptions\":[]"));

    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* cookie 1 */
    conn_t *watch = rig_connect(&rig, MOQ_VERSION_DRAFT_18); /* cookie 2 */
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* cookie 3 */
    R_CHECK(&rig, pub && watch && sub);
    rig_pump(&rig, 4);

    /* Watcher subscribes to namespace prefix "live". */
    moq_subscribe_namespace_cfg_t nsc;
    memset(&nsc, 0, sizeof(nsc));
    moq_subscribe_namespace_cfg_init(&nsc);
    moq_bytes_t pfx[1] = { B("live") };
    nsc.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nsc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nsc, rig.now,
                                                  &nsh) == MOQ_OK);
    rig_pump(&rig, 6);

    /* Publisher announces live/cam. */
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t w_ps, pub_ps, s_ps;
    memset(&w_ps, 0, sizeof(w_ps));
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ns_found >= 1);   /* NAMESPACE_FOUND on the wire */

    /* Model: announce live/cam (binding 1) and the namespace watcher on "live"
     * (binding 2) are both present; no track yet; the route epoch advanced. */
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, buf, sizeof(buf), &w) ==
                      MOQR_OK);
    R_CHECK(&rig, J_HAS(buf, "\"namespace\":[\"live\",\"cam\"],\"binding\":1"));
    R_CHECK(&rig, J_HAS(buf, "\"prefix\":[\"live\"],\"binding\":2"));
    R_CHECK(&rig, J_HAS(buf, "\"tracks\":[]"));
    moqr_core_get_stats(rig.core, &cs);
    R_CHECK(&rig, cs.route_epoch > epoch);   /* advanced on the mutations */
    epoch = cs.route_epoch;
    R_CHECK(&rig, cs.subs == 0);             /* no subscriber yet */

    /* Subscriber subscribes to live/cam track v; publisher serves a matrix. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);
    dp_send_subgroup(&rig, pub, pub_ps.up_sub, 0, 0);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Model: track live/cam "v" is ACTIVE (upstream binding 1) and carries one
     * ACTIVE subscriber (binding 3); stats agree and the epoch advanced. */
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, buf, sizeof(buf), &w) ==
                      MOQR_OK);
    R_CHECK(&rig,
            J_HAS(buf, "\"name\":\"v\",\"state\":\"active\",\"upstream_binding\":1"));
    R_CHECK(&rig, J_HAS(buf, "\"binding\":3,\"state\":\"active\""));
    moqr_core_get_stats(rig.core, &cs);
    R_CHECK(&rig, cs.subs == 1 && cs.subs_active == 1 && cs.subs_parked == 0);
    R_CHECK(&rig, cs.subs == cs.subs_active + cs.subs_parked);   /* invariant */
    R_CHECK(&rig, cs.tracks == 1);
    R_CHECK(&rig, cs.route_epoch > epoch);
    epoch = cs.route_epoch;

    /* Unsubscribe; then close the publisher (withdraws the announce). */
    R_CHECK(&rig, moq_session_unsubscribe(sub->peer, sh, rig.now) == MOQ_OK);
    rig_pump(&rig, 8);
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, pub->rsess) == MOQR_OK);
    rig_pump(&rig, 10);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    /* Model: unsubscribe removed the active subscriber; the publisher close
     * withdrew the announce (watcher saw NAMESPACE_GONE); the track dropped to
     * WARM with no upstream and no subscriptions; the watcher route persists. */
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, buf, sizeof(buf), &w) ==
                      MOQR_OK);
    R_CHECK(&rig, J_HAS(buf, "\"announces\":[]"));            /* announce gone */
    R_CHECK(&rig, !J_HAS(buf, "\"binding\":3,\"state\":\"active\"")); /* sub gone */
    R_CHECK(&rig, J_HAS(buf, "\"state\":\"warm\",\"upstream_binding\":null"));
    R_CHECK(&rig, J_HAS(buf, "\"prefix\":[\"live\"],\"binding\":2")); /* watcher */
    R_CHECK(&rig, w_ps.ns_gone >= 1);   /* NAMESPACE_GONE on the wire */
    moqr_core_get_stats(rig.core, &cs);
    R_CHECK(&rig, cs.subs == 0 && cs.subs_active == 0);
    R_CHECK(&rig, cs.route_epoch > epoch);
    epoch = cs.route_epoch;

    /* Re-announce the same namespace with a fresh publisher: dump + wire become
     * coherent again (announce back, watcher re-notified, epoch advances). */
    conn_t *pub2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub2 != NULL);
    rig_pump(&rig, 4);
    moq_announcement_t ann2;
    R_CHECK(&rig, moq_session_publish_namespace(pub2->peer, &pcfg, rig.now,
                                                &ann2) == MOQ_OK);
    rig_pump(&rig, 6);
    memset(&w_ps, 0, sizeof(w_ps));
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ns_found >= 1);   /* watcher re-notified */
    R_CHECK(&rig, moqr_core_route_dump_json(rig.core, buf, sizeof(buf), &w) ==
                      MOQR_OK);
    R_CHECK(&rig, !J_HAS(buf, "\"announces\":[]"));   /* announce back */
    R_CHECK(&rig, J_HAS(buf, "\"namespace\":[\"live\",\"cam\"],\"binding\":"));
    moqr_core_get_stats(rig.core, &cs);
    R_CHECK(&rig, cs.route_epoch > epoch);

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: route_projection_oracle\n");
    }
    return f + rig.failures;
}

/* ---- Production-binding allocation-failure sweeps ------------------------- *
 * A fail-at-N test allocator (see ca_t.fail_at) injects a single OOM at each
 * allocation index; a focused production-binding flow is replayed for every N.
 * At each injection point the relay must fail closed — no leak, no UAF, no stale
 * route state — and a no-failure replay must still pass. */

/* Best-effort drain (no R_CHECK — every op may fail under injected OOM): polls a
 * peer, accepts any upstream SUBSCRIBE (capturing its handle), and cleans up. */
static void
oom_drain(rig_t *r, conn_t *cn, moq_subscription_t *out_up)
{
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                if (out_up != NULL) {
                    *out_up = evs[e].u.subscribe_request.sub;
                }
                moq_accept_subscribe_cfg_t cfg;
                moq_accept_subscribe_cfg_init(&cfg);
                (void)moq_session_accept_subscribe(
                    cn->peer, evs[e].u.subscribe_request.sub, &cfg, r->now);
            }
            moq_event_cleanup(&evs[e]);
        }
    }
}

/* Announce + namespace-watch + subscribe + upstream accept + one object +
 * unsubscribe over the production binding. Every op is best-effort so a mid-flow
 * OOM just bails; the caller checks the allocator balance after rig_destroy. */
static void
oom_bind_flow(rig_t *rig)
{
    conn_t *pub = rig_connect(rig, MOQ_VERSION_DRAFT_18);
    conn_t *watch = rig_connect(rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(rig, MOQ_VERSION_DRAFT_18);
    if (!pub || !watch || !sub) {
        return;   /* OOM during connect — rig_connect already cleaned up */
    }
    rig_pump(rig, 4);

    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nsc;
    memset(&nsc, 0, sizeof(nsc));
    moq_subscribe_namespace_cfg_init(&nsc);
    nsc.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nsc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    (void)moq_session_subscribe_namespace(watch->peer, &nsc, rig->now, &nsh);
    rig_pump(rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    (void)moq_session_publish_namespace(pub->peer, &pcfg, rig->now, &ann);
    rig_pump(rig, 4);
    oom_drain(rig, watch, NULL);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh = { 0 };
    bool subscribed =
        moq_session_subscribe(sub->peer, &scfg, rig->now, &sh) == MOQ_OK;
    rig_pump(rig, 6);
    moq_subscription_t up_sub = { 0 };
    oom_drain(rig, pub, &up_sub);
    rig_pump(rig, 6);

    if (up_sub._opaque != 0) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        moq_subgroup_handle_t sgh;
        if (moq_session_open_subgroup(pub->peer, up_sub, &sgc, rig->now, &sgh) ==
            MOQ_OK) {
            uint8_t body[4] = { 0xAB, 1, 2, 3 };
            moq_rcbuf_t *pl = NULL;
            if (moq_rcbuf_create(&rig->alloc->vt, body, sizeof(body), &pl) ==
                MOQ_OK) {
                (void)moq_session_write_object(pub->peer, sgh, 0, pl, rig->now);
                moq_rcbuf_decref(pl);
            }
            (void)moq_session_close_subgroup(pub->peer, sgh, rig->now);
        }
    }
    rig_pump(rig, 6);
    oom_drain(rig, sub, NULL);

    if (subscribed) {
        (void)moq_session_unsubscribe(sub->peer, sh, rig->now);
    }
    rig_pump(rig, 8);
}

/* Focused unit test: the fail-at allocator fires at exactly the intended
 * attempt index, later attempts still succeed, and byte accounting balances. */
static int
test_oom_allocator(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    a.fail_at = 3;   /* fail exactly the 3rd attempt */
    void *p1 = a.vt.alloc(16, a.vt.ctx);
    void *p2 = a.vt.alloc(16, a.vt.ctx);
    void *p3 = a.vt.alloc(16, a.vt.ctx);   /* injected failure */
    void *p4 = a.vt.alloc(16, a.vt.ctx);   /* succeeds again */
    if (!(p1 != NULL && p2 != NULL && p3 == NULL && p4 != NULL)) {
        printf("FAIL: oom allocator fired at the wrong index\n");
        failures++;
    }
    if (a.attempts != 4 || a.allocs != 3) {
        printf("FAIL: oom allocator accounting attempts=%ld allocs=%ld\n",
               a.attempts, a.allocs);
        failures++;
    }
    a.vt.free(p1, 16, a.vt.ctx);
    a.vt.free(p2, 16, a.vt.ctx);
    a.vt.free(p4, 16, a.vt.ctx);
    if (a.live != 0) {
        printf("FAIL: oom allocator live=%ld after frees\n", a.live);
        failures++;
    }
    if (failures == 0) {
        printf("PASS: test_oom_allocator\n");
    }
    return failures;
}

static int
test_oom_binding_flow(void)
{
    int failures = 0;

    /* No-failure control + measurement of the flow's allocation count. */
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: test_oom_binding_flow control rig create\n");
        return 1;
    }
    oom_bind_flow(&rig);
    rig_destroy(&rig);
    if (a.live != 0) {
        printf("FAIL: oom control leaked %ld bytes\n", a.live);
        failures++;
    }
    long total = a.attempts;

    /* Sweep: inject a single OOM at each allocation index. */
    int fired = 0;
    for (long fa = 1; fa <= total; fa++) {
        ca_t af;
        ca_init(&af);
        af.fail_at = fa;
        rig_t rf;
        if (rig_create(&rf, &af) == MOQR_OK) {
            oom_bind_flow(&rf);
            rig_destroy(&rf);
        }
        if (af.attempts >= fa) {
            fired++;   /* the injected failure actually fired */
        }
        if (af.live != 0) {
            printf("FAIL: oom sweep fail_at=%ld leaked %ld bytes\n", fa,
                   af.live);
            failures++;
        }
    }
    if (fired != (int)total) {   /* every injection point must actually fire */
        printf("FAIL: oom sweep injected %d of %d points\n", fired,
               (int)total);
        failures++;
    }
    if (failures == 0) {
        printf("PASS: test_oom_binding_flow (swept fail_at 1..%ld)\n", total);
    }
    return failures;
}

/* A retire path must arm the warm-track linger with the REAL clock. When
 * the last subscriber of an ACTIVE track retires because its bounded range
 * completed (a core-internal retire in next_delivery, not an app unsubscribe),
 * the track should linger for `linger_us` before the tick warms it (so a quick
 * rejoin reuses the upstream). If that retire arms the deadline with a stale
 * clock (now_us == 0), the deadline is already in the past and the very next
 * tick warms the track PREMATURELY — the relay drops the upstream immediately.
 * Observed at the origin as an early UNSUBSCRIBED. */
static int
range_completion_linger(void)
{
    ca_t a;
    ca_init(&a);
    g_test_linger_us = 15000;   /* linger spans ~15 pumps at 1000us/cycle */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_linger_us = 0;
        printf("FAIL: range_completion_linger rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    /* Bounded range: group 0 only. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.end_group = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 6);

    /* Origin publishes group 0 (in range, EOG) then group 1 (past the range —
     * its arrival is the live edge that completes the subscription). */
    red_pub_subgroup_n(&rig, pub, pub_ps.up_sub, 0, 0, 1, true);
    red_pub_subgroup_n(&rig, pub, pub_ps.up_sub, 1, 0, 1, false);
    rig_pump(&rig, 8);   /* deliver group 0, ingest group 1 -> range completes */
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.done_seen);       /* the range completed (SUBSCRIBE_DONE) */

    /* The last subscriber of the ACTIVE track just retired (range complete).
     * A correctly clocked retire armed the warm deadline at now_us + linger_us
     * (now_us is well past linger_us here), so the track is STILL lingering and
     * the origin has NOT been unsubscribed. The buggy now_us==0 retire armed it
     * at 0 + linger_us — already in the past — so the very next tick warmed the
     * track and dropped the upstream. */
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.unsubscribed == 0);

    /* Fewer cycles than the linger window: still lingering under the fix. */
    rig_pump(&rig, 3);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.unsubscribed == 0);   /* RED with the literal-0 retire */

    /* Past the linger deadline the track warms and releases the upstream. */
    rig_pump(&rig, 15);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.unsubscribed == 1);   /* eventually expires */

    g_test_linger_us = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: range_completion_linger\n");
    }
    return f;
}

/* -- FETCH served over real sessions (SimPair) ---------------------------- *
 * A downstream peer issues moq_session_fetch; the bind serves it from the core
 * retained cursor and the peer observes FETCH_OK / FETCH_OBJECT / FETCH_COMPLETE
 * (or FETCH_ERROR). Publisher content is set up directly on the core (a push
 * publisher needs no session). */

static moqr_binding_t
fetch_pub_open(rig_t *r)
{
    moqr_binding_t pb;
    moq_bytes_t nsb[1] = { B("demo") };
    R_CHECK(r, moqr_core_binding_open(r->core, 900, &pb) == MOQR_OK);
    R_CHECK(r, moqr_core_announce(r->core, pb, (moqr_ns_t){ nsb, 1 }) ==
                   MOQR_OK);
    return pb;
}

static moqr_track_t
fetch_pub_track(rig_t *r, moqr_binding_t pb, const char *name)
{
    moq_bytes_t nsb[1] = { B("demo") };
    moqr_track_t tk = MOQR_TRACK_INVALID;
    R_CHECK(r, moqr_core_publish_open(r->core, pb, (moqr_ns_t){ nsb, 1 },
                                      B(name), 901, &tk) == MOQR_OK);
    return tk;
}

static void
fetch_ing(rig_t *r, moqr_track_t tk, uint64_t g, uint64_t sg, uint64_t o,
          uint8_t fp)
{
    uint8_t body[8];
    memset(body, fp, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) == 0);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 1;
    d.payload = pl;
    d.now_us = r->now;
    if (moqr_core_ingest(r->core, tk, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        R_CHECK(r, 0);
    }
}

/* Ingest a NORMAL object flagged with datagram preference (a d18 fetcher keeps
 * the bit; a d16 fetcher clears it). Otherwise identical to fetch_ing. */
static void
fetch_ing_dg(rig_t *r, moqr_track_t tk, uint64_t g, uint64_t sg, uint64_t o,
             uint8_t fp)
{
    uint8_t body[8];
    memset(body, fp, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) == 0);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 1;
    d.datagram_pref = true;
    d.payload = pl;
    d.now_us = r->now;
    if (moqr_core_ingest(r->core, tk, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        R_CHECK(r, 0);
    }
}

static void
fetch_issue(rig_t *r, conn_t *cn, const char *name, uint64_t sg, uint64_t so,
            uint64_t eg, uint64_t eo, moq_fetch_t *out)
{
    moq_bytes_t nsb[1] = { B("demo") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 1 };
    fc.track_name = B(name);
    fc.start_group = sg;
    fc.start_object = so;
    fc.end_group = eg;
    fc.end_object = eo;
    R_CHECK(r, moq_session_fetch(cn->peer, &fc, r->now, out) == MOQ_OK);
}

/* Exact retained replay + multi-subgroup merge + INVALID_RANGE, per draft. */
static int
fetch_flow(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != 0) {
        printf("FAIL: fetch rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *sub = rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 4);

    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t t1 = fetch_pub_track(&rig, pb, "v1");
    moqr_track_t t2 = fetch_pub_track(&rig, pb, "v2");

    /* T1: exact retained replay over one subgroup -> ids + payload bytes. */
    for (uint64_t o = 0; o < 3; o++) {
        fetch_ing(&rig, t1, 0, 0, o, (uint8_t)(0x10 + o));
    }
    rig_pump(&rig, 2);
    moq_fetch_t fh1;
    fetch_issue(&rig, sub, "v1", 0, 0, 0, 3, &fh1);   /* [0:0 .. 0:3) */
    rig_pump(&rig, 10);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.fetch_ok);
    R_CHECK(&rig, ps.fok_end_group == 0 && ps.fok_end_object == 3);
    R_CHECK(&rig, !ps.fok_eot);
    R_CHECK(&rig, ps.fetch_complete == 1);
    R_CHECK(&rig, ps.fetch_error == 0);
    R_CHECK(&rig, ps.fdlv_n == 3);
    for (int i = 0; i < 3 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].group == 0);
        R_CHECK(&rig, ps.fdlv[i].object == (uint64_t)i);
        R_CHECK(&rig, ps.fdlv[i].fp == (uint8_t)(0x10 + i));
    }

    /* T3: multi-subgroup merge -> strict object-id order 0..5. */
    fetch_ing(&rig, t2, 5, 0, 0, 0xA0);
    fetch_ing(&rig, t2, 5, 0, 2, 0xA2);
    fetch_ing(&rig, t2, 5, 0, 4, 0xA4);
    fetch_ing(&rig, t2, 5, 1, 1, 0xA1);
    fetch_ing(&rig, t2, 5, 1, 3, 0xA3);
    fetch_ing(&rig, t2, 5, 1, 5, 0xA5);
    rig_pump(&rig, 2);
    moq_fetch_t fh2;
    fetch_issue(&rig, sub, "v2", 5, 0, 5, 0, &fh2);   /* whole group 5 */
    rig_pump(&rig, 10);
    peer_state_t ps2;
    memset(&ps2, 0, sizeof(ps2));
    peer_drain(&rig, sub, &ps2, false, false, 0, 0);
    R_CHECK(&rig, ps2.fetch_ok && ps2.fetch_complete == 1);
    R_CHECK(&rig, ps2.fdlv_n == 6);
    for (int i = 0; i < 6 && i < ps2.fdlv_n; i++) {
        R_CHECK(&rig, ps2.fdlv[i].object == (uint64_t)i);
    }

    /* T6: INVALID_RANGE — start past the largest object of the track. */
    moq_fetch_t fh3;
    fetch_issue(&rig, sub, "v1", 9, 0, 9, 1, &fh3);
    rig_pump(&rig, 8);
    peer_state_t ps3;
    memset(&ps3, 0, sizeof(ps3));
    peer_drain(&rig, sub, &ps3, false, false, 0, 0);
    R_CHECK(&rig, ps3.fetch_error == 1);
    R_CHECK(&rig, ps3.fetch_error_code == MOQ_REQUEST_ERROR_INVALID_RANGE);
    R_CHECK(&rig, !ps3.fetch_ok);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_flow (v%d)\n", (int)version);
    }
    return rig.failures;
}

/* Evicted-prefix fetch. The Start falls below the retention horizon, so
 * the relay serves the retained suffix led by one UNKNOWN marker before the
 * first retained group -- via the profile-owned seam. Proves the profile max
 * crosses the wire per draft (asserted here as literals, since the relay
 * boundary forbids the wire headers; the session test pins them symbolically:
 * d16 = MOQ_QUIC_VARINT_MAX = 2^62-1, d18 = MOQ_VI64_MAX = UINT64_MAX). With
 * max_actions != 0 the marker write WOULD_BLOCKs first and must re-peek/retry --
 * never skip. */
static int
fetch_evicted_prefix(moq_version_t version, uint64_t expect_max,
                     uint32_t max_actions)
{
    ca_t a;
    ca_init(&a);
    g_test_log_max_groups = 3;   /* retain 3 whole groups -> prefix evicts */
    rig_t rig;
    int rc = rig_create(&rig, &a);
    g_test_log_max_groups = 0;
    if (rc != 0) {
        printf("FAIL: evicted-prefix rig create (v%d)\n", (int)version);
        return 1;
    }
    g_test_max_actions = max_actions;   /* 0 = default; >0 forces WOULD_BLOCK */
    conn_t *sub = rig_connect(&rig, version);
    g_test_max_actions = 0;
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 8);

    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t tk = fetch_pub_track(&rig, pb, "v");
    /* Groups 0..5, one object each. max_groups=3 evicts 0,1,2 -> oldest == 3. */
    for (uint64_t g = 0; g <= 5; g++) {
        fetch_ing(&rig, tk, g, 0, 0, (uint8_t)(0x30 + g));
    }
    rig_pump(&rig, 4);

    moq_fetch_t fh;
    fetch_issue(&rig, sub, "v", 0, 0, 5, 1, &fh);   /* Start below the horizon */
    rig_pump(&rig, 60);   /* enough for any WOULD_BLOCK write to drain */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);

    R_CHECK(&rig, ps.fetch_ok);
    R_CHECK(&rig, ps.fok_end_group == 5 && ps.fok_end_object == 1);
    R_CHECK(&rig, ps.fetch_error == 0);
    /* Exactly one UNKNOWN marker at { oldest-1 = 2, profile_max }. */
    R_CHECK(&rig, ps.fgap_n == 1);
    R_CHECK(&rig, ps.fgap[0].kind == MOQ_FETCH_RANGE_UNKNOWN);
    R_CHECK(&rig, ps.fgap[0].group == 2);
    R_CHECK(&rig, ps.fgap[0].object == expect_max);
    /* Retained objects from group 3 onward (3,4,5), object 0 each. The marker
     * precedes them: d18 is ascending-only, so a marker at group 2 arriving
     * after an object at group 3 would fault -- a clean completion is the
     * ordering proof. */
    R_CHECK(&rig, ps.fdlv_n == 3);
    for (int i = 0; i < 3 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].group == (uint64_t)(3 + i));
        R_CHECK(&rig, ps.fdlv[i].object == 0);
        R_CHECK(&rig, ps.fdlv[i].fp == (uint8_t)(0x30 + 3 + i));
    }
    R_CHECK(&rig, ps.fetch_complete == 1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_evicted_prefix (v%d%s)\n", (int)version,
               max_actions ? ", backpressure" : "");
    }
    return rig.failures;
}

/* A retained object carrying datagram preference, fetched per draft. The
 * d18 fetch header can encode the bit (expect_dg=true); the d16 header cannot,
 * so the relay clears it and serves the object normally (expect_dg=false) WITHOUT
 * a session close. Identity/payload are the same either way. */
static int
fetch_datagram_pref(moq_version_t version, bool expect_dg)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != 0) {
        printf("FAIL: dg-pref rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *sub = rig_connect(&rig, version);
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 4);
    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t tk = fetch_pub_track(&rig, pb, "vg");
    fetch_ing_dg(&rig, tk, 7, 0, 0, 0xD7);   /* a datagram-preference object */
    rig_pump(&rig, 2);
    moq_fetch_t fh;
    fetch_issue(&rig, sub, "vg", 7, 0, 7, 1, &fh);   /* [7:0 .. 7:0] */
    rig_pump(&rig, 10);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);

    R_CHECK(&rig, !ps.session_closed);       /* d16 must NOT hard-close */
    R_CHECK(&rig, ps.fetch_ok);
    R_CHECK(&rig, ps.fetch_error == 0);
    R_CHECK(&rig, ps.fdlv_n == 1);
    R_CHECK(&rig, ps.fdlv[0].group == 7 && ps.fdlv[0].object == 0);
    R_CHECK(&rig, ps.fdlv[0].fp == 0xD7);
    R_CHECK(&rig, ps.fdlv[0].datagram == expect_dg);
    R_CHECK(&rig, ps.fetch_complete == 1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_datagram_pref_%s\n",
               version == MOQ_VERSION_DRAFT_18 ? "d18" : "d16_clear");
    }
    return rig.failures;
}

/* Cross-draft parity. The SAME retained matrix (mixed datagram preference)
 * fetched by a d16 and a d18 fetcher delivers identical group/object/payload in
 * identical order; the only difference is the datagram bit -- preserved on the
 * pref object for d18, cleared for d16. */
static int
fetch_datagram_parity(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != 0) {
        printf("FAIL: dg-parity rig create\n");
        return 1;
    }
    conn_t *s16 = rig_connect(&rig, MOQ_VERSION_DRAFT_16);
    conn_t *s18 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, s16 != NULL && s18 != NULL);
    rig_pump(&rig, 4);
    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t tk = fetch_pub_track(&rig, pb, "vp");
    fetch_ing(&rig, tk, 8, 0, 0, 0x80);        /* pref = false */
    fetch_ing_dg(&rig, tk, 8, 0, 1, 0x81);     /* pref = true  */
    fetch_ing(&rig, tk, 8, 0, 2, 0x82);        /* pref = false */
    rig_pump(&rig, 2);
    moq_fetch_t f16, f18;
    fetch_issue(&rig, s16, "vp", 8, 0, 8, 0, &f16);   /* whole group 8 */
    fetch_issue(&rig, s18, "vp", 8, 0, 8, 0, &f18);
    rig_pump(&rig, 14);
    peer_state_t p16, p18;
    memset(&p16, 0, sizeof(p16));
    memset(&p18, 0, sizeof(p18));
    peer_drain(&rig, s16, &p16, false, false, 0, 0);
    peer_drain(&rig, s18, &p18, false, false, 0, 0);

    R_CHECK(&rig, !p16.session_closed && !p18.session_closed);
    R_CHECK(&rig, p16.fetch_complete == 1 && p18.fetch_complete == 1);
    R_CHECK(&rig, p16.fdlv_n == 3 && p18.fdlv_n == 3);
    for (int i = 0; i < 3 && i < p16.fdlv_n && i < p18.fdlv_n; i++) {
        /* Identity + order + payload identical across drafts. */
        R_CHECK(&rig, p16.fdlv[i].group == 8 && p18.fdlv[i].group == 8);
        R_CHECK(&rig, p16.fdlv[i].object == (uint64_t)i);
        R_CHECK(&rig, p18.fdlv[i].object == (uint64_t)i);
        R_CHECK(&rig, p16.fdlv[i].fp == (uint8_t)(0x80 + i));
        R_CHECK(&rig, p18.fdlv[i].fp == (uint8_t)(0x80 + i));
    }
    /* Only the datagram bit differs: d18 preserves it on the pref object (1);
     * d16 clears all three. */
    R_CHECK(&rig, !p16.fdlv[0].datagram && !p16.fdlv[1].datagram &&
                      !p16.fdlv[2].datagram);
    R_CHECK(&rig, !p18.fdlv[0].datagram && p18.fdlv[1].datagram &&
                      !p18.fdlv[2].datagram);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_datagram_parity\n");
    }
    return rig.failures;
}

/* Chunked COMPLETE delivery. With the relay-side session in streaming-
 * receive mode, an inbound object surfaces as OBJECT_CHUNK and the bind records
 * it into an OPEN record: a normal object completes (chunked COMPLETE, bytes
 * retained) and is DELIVERED whole on the subscribe path, while a mid-object
 * reset abandons (chunks released) and is never delivered. (OPEN live-edge and
 * RESET/STOP propagation are exercised separately, and retained FETCH still
 * skips chunked records.) */
static int
chunk_ingest_flow(void)
{
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = true;   /* read in rig_connect, below */
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        printf("FAIL: chunk ingest rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;   /* restore after both connects */
    R_CHECK(&rig, pub != NULL && sub != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream sub */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    /* Group 1, subgroup 0 carries TWO streamed objects the relay ingests as
     * chunked COMPLETE records and delivers whole via
     * begin/write_object_data/end, reassembled by the subscriber. Two objects
     * on ONE subgroup prove the relay ends object 0 downstream before beginning
     * object 1 (so end_object is load-bearing, not just bookkeeping).
     *   object 0: three 32-byte chunks (96 bytes, 0xC5)
     *   object 1: two   32-byte chunks (64 bytes, 0xD6) */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    for (int ci = 0; ci < 3; ci++) {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 1, 64, rig.now) ==
                      MOQ_OK);
    for (int ci = 0; ci < 2; ci++) {
        uint8_t body[32];
        memset(body, 0xD6, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sga, rig.now) == MOQ_OK);
    rig_pump(&rig, 8);

    /* Object B (group 2): begin, partial data, then reset mid-object -> the
     * relay records ABANDONED (partial chunks released), never COMPLETE. */
    moq_subgroup_cfg_t scb;
    moq_subgroup_cfg_init(&scb);
    scb.group_id = 2;
    scb.subgroup_id = 0;
    moq_subgroup_handle_t sgb;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &scb,
                                            rig.now, &sgb) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sgb, 0, 64, rig.now) == MOQ_OK);
    uint8_t half[32];
    memset(half, 0xB7, sizeof(half));
    moq_rcbuf_t *hp = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, half, sizeof(half), &hp) == MOQ_OK);
    R_CHECK(&rig, moq_session_write_object_data(pub->peer, sgb, hp, rig.now) == MOQ_OK);
    moq_rcbuf_decref(hp);
    R_CHECK(&rig, moq_session_reset_subgroup(pub->peer, sgb, 0, rig.now) == MOQ_OK);
    rig_pump(&rig, 8);

    /* Relay log state: objects 0+1 retained/counted; B's partial bytes released. */
    moqr_core_stats_t st;
    moqr_core_get_stats(rig.core, &st);
    R_CHECK(&rig, st.retained_bytes == 96 + 64);   /* obj0 + obj1; B released */
    R_CHECK(&rig, st.ingested_total == 2);          /* obj0 + obj1 completed */

    /* Both chunked COMPLETE records are delivered whole to the subscriber
     * (streamed downstream chunk-by-chunk on one subgroup, reassembled); the
     * ABANDONED record B is never delivered. The last object is 1 (0xD6, 64B). */
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.objects == 2);
    R_CHECK(&rig, sub_ps.last_group == 1 && sub_ps.last_object == 1);
    R_CHECK(&rig, !sub_ps.last_payload_null && sub_ps.last_byte == 0xD6);
    R_CHECK(&rig, sub_ps.last_len == 64);
    R_CHECK(&rig, !sub_ps.session_closed);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_ingest_flow\n");
    }
    return rig.failures;
}

/* Backpressure: with a constrained action queue the relay's downstream
 * write_object_data WOULD_BLOCKs partway through a chunked object; the delivery
 * holds and resumes the SAME chunk (no duplicate, no skip), so the subscriber
 * still reassembles the exact object. Deterministic under SimPair. */
static int
chunk_delivery_backpressure(void)
{
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = true;
    g_test_max_actions = 3;   /* forces WOULD_BLOCK during the chunked delivery */
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        g_test_max_actions = 0;
        printf("FAIL: chunk backpressure rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_max_actions = 0;   /* restore after both connects read the knobs */
    R_CHECK(&rig, pub != NULL && sub != NULL);
    rig_pump(&rig, 8);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1;
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    rig_pump(&rig, 8);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 8);

    /* Stream a 3-chunk (96-byte) object, pumping between writes so the PUBLISHER
     * never blocks on the constrained queue; the relay accumulates an OPEN
     * record and only delivers once it completes. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    rig_pump(&rig, 4);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 4);
    for (int ci = 0; ci < 3; ci++) {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        rig_pump(&rig, 4);
    }
    R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sga, rig.now) == MOQ_OK);
    /* Generous, bounded drain: the constrained relay delivers the object across
     * several WOULD_BLOCK / resume cycles. */
    rig_pump(&rig, 40);

    /* Reassembled exactly: one object, 96 bytes, correct identity/bytes — proof
     * the held chunk cursor never duplicated or skipped a chunk. */
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.objects == 1);
    R_CHECK(&rig, sub_ps.last_group == 1 && sub_ps.last_object == 0);
    R_CHECK(&rig, !sub_ps.last_payload_null && sub_ps.last_byte == 0xC5);
    R_CHECK(&rig, sub_ps.last_len == 96);
    R_CHECK(&rig, !sub_ps.session_closed);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_delivery_backpressure\n");
    }
    return rig.failures;
}

/* Live edge + abandon over SimPair with a streaming-receive subscriber.
 * `abort_mid` == false: stream a 3-chunk object incrementally; the subscriber
 * observes the first downstream chunk BEFORE the object completes, then the
 * object completes cleanly. `abort_mid` == true: after the first chunk the
 * publisher resets the object; the subscriber observes a downstream RESET (the
 * relay's minimal abandon propagation), never a clean completion. */
static int
chunk_liveedge_case(bool abort_mid)
{
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        printf("FAIL: chunk liveedge rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    R_CHECK(&rig, pub != NULL && sub != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t s1;
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    /* Begin the object and stream only the FIRST chunk. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 8);

    /* Live edge: the subscriber has the first chunk BEFORE the object is done. */
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1);
    R_CHECK(&rig, sub_ps.last_chunk_byte == 0xC5);
    R_CHECK(&rig, !sub_ps.obj_complete);
    R_CHECK(&rig, !sub_ps.saw_reset);

    if (abort_mid) {
        /* Reset the object mid-stream: the relay abandons the record and resets
         * the already-begun downstream subgroup. */
        R_CHECK(&rig,
                moq_session_reset_subgroup(pub->peer, sga, 0x99, rig.now) ==
                    MOQ_OK);
        rig_pump(&rig, 8);
        peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        R_CHECK(&rig, sub_ps.saw_reset);        /* wire-visible downstream reset */
        R_CHECK(&rig, sub_ps.last_reset_code == 0x99); /* real code propagated   */
        R_CHECK(&rig, !sub_ps.obj_complete);    /* never a clean completion      */
    } else {
        /* Finish the object: two more chunks + end. */
        for (int ci = 0; ci < 2; ci++) {
            uint8_t body[32];
            memset(body, 0xC5, sizeof(body));
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                              MOQ_OK);
            R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                        rig.now) == MOQ_OK);
            moq_rcbuf_decref(pl);
        }
        R_CHECK(&rig, moq_session_end_object(pub->peer, sga, rig.now) == MOQ_OK);
        R_CHECK(&rig,
                moq_session_close_subgroup(pub->peer, sga, rig.now) == MOQ_OK);
        rig_pump(&rig, 8);
        peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        R_CHECK(&rig, sub_ps.obj_complete);     /* completes cleanly */
        R_CHECK(&rig, !sub_ps.saw_reset);
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_liveedge_%s\n", abort_mid ? "abort" : "complete");
    }
    return rig.failures;
}

/* Eviction after live-edge, end-to-end: a streaming subscriber gets the
 * first chunk of an OPEN object, then that object's group is capacity-evicted
 * before it completes — the relay must reset the begun downstream subgroup (the
 * bind's evict-reset path), so the subscriber observes a RESET rather than a
 * silently-stuck object. */
static int
chunk_liveedge_evict(void)
{
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    g_test_log_max_groups = 2;   /* group 1 evicts once group 3 is ingested */
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        g_test_log_max_groups = 0;
        printf("FAIL: chunk liveedge evict rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    g_test_log_max_groups = 0;
    R_CHECK(&rig, pub != NULL && sub != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t s1;
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);

    /* Group 1: begin + one chunk (stays OPEN); the relay live-delivers it. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1 && !sub_ps.obj_complete);

    /* Groups 2 and 3: whole objects on fresh subgroups. Ingesting group 3 evicts
     * group 1 (max_groups == 2), truncating the begun OPEN object. */
    for (uint64_t g = 2; g <= 3; g++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = g;
        sc.subgroup_id = 0;
        moq_subgroup_handle_t sgh;
        R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sc,
                                                rig.now, &sgh) == MOQ_OK);
        uint8_t body[16];
        memset(body, (uint8_t)(0xE0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object(pub->peer, sgh, 0, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
        R_CHECK(&rig,
                moq_session_close_subgroup(pub->peer, sgh, rig.now) == MOQ_OK);
        rig_pump(&rig, 8);
    }

    /* The begun OPEN object's group was evicted → the relay reset its downstream
     * subgroup, so the subscriber saw a RESET (not a silent hang). */
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.saw_reset);
    R_CHECK(&rig, !sub_ps.session_closed);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: chunk_liveedge_evict\n");
    }
    return rig.failures;
}

/* A subscription terminated while a downstream subgroup is mid-object: the relay
 * must RESET the begun subgroup (the streaming-safe terminal) so the peer sees a
 * clean reset before the SUBSCRIBE_DONE — never a begun object left with no
 * terminal. Before the SUB_DONE reset hardening this is RED: the SUB_DONE handler
 * only closes subgroups, and close returns WRONG_STATE on a streaming subgroup,
 * so the open object is dropped with no downstream reset. */
static int
sub_done_resets_begun_subgroup(void)
{
    ca_t a;
    ca_init(&a);
    g_rv_lease_sub = 3000; /* a subscribe grant with a live revalidation lease */
    g_test_authorize = rv_hook;
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        rv_reset();
        printf("FAIL: sub_done_reset rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    R_CHECK(&rig, pub != NULL && sub != NULL);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    rig_pump(&rig, 4);

    /* Announce + subscribe under a subscribe grant with a revalidation lease. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    reval_make_subscribe_grant(&rig, pub, sub, &pub_ps, &sub_ps, nsp);
    R_CHECK(&rig, !sub_ps.done_seen);

    /* Begin an object, ship only the first chunk → the downstream subgroup is open
     * and streaming (obj_begun) at the subscriber. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1);
    R_CHECK(&rig, !sub_ps.obj_complete);
    R_CHECK(&rig, !sub_ps.saw_reset);

    /* Revoke the subscription (auth DENY on revalidation) WHILE the downstream
     * subgroup is mid-object → the relay fans a downstream SUB_DONE. The begun
     * subgroup must be RESET before the SUBSCRIBE_DONE — with the reset
     * registry's CANCELLED (0x1): the detailed cause rides the PUBLISH_DONE
     * status, never the reset code (distinct registries). */
    g_rv_decision = MOQR_AUTH_DENY;
    g_rv_deny_code = 0x7;
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.saw_reset);               /* begun object RESET, not stranded */
    R_CHECK(&rig, sub_ps.last_reset_code == 0x1);  /* reset registry: CANCELLED         */
    R_CHECK(&rig, !sub_ps.obj_complete);           /* never a clean FIN                 */
    R_CHECK(&rig, sub_ps.done_seen);               /* SUBSCRIBE_DONE still delivered    */
    R_CHECK(&rig, sub_ps.done_code == 0x7);        /* the revoke code, as DONE status   */

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: sub_done_resets_begun_subgroup\n");
    }
    return rig.failures;
}

/* WOULD_BLOCK backpressure: a tiny session action queue forces the fetch writes
 * to back up; every object is still delivered exactly once, in order, across
 * pumps (the peek-holds-on-WOULD_BLOCK invariant end-to-end). */
static int
fetch_backpressure(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 2;   /* constrain both SimPair sessions' queues */
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_max_actions = 0;
        printf("FAIL: fetch bp rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* reads the knob */
    g_test_max_actions = 0;   /* restore for later tests */
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 8);
    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t tk = fetch_pub_track(&rig, pb, "vb");
    for (uint64_t o = 0; o < 8; o++) {
        fetch_ing(&rig, tk, 0, 0, o, (uint8_t)(0x20 + o));
    }
    rig_pump(&rig, 4);
    moq_fetch_t fh;
    fetch_issue(&rig, sub, "vb", 0, 0, 0, 8, &fh);
    rig_pump(&rig, 60);   /* plenty of pumps for the backed-up writes to drain */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.fetch_ok && ps.fetch_complete == 1);
    R_CHECK(&rig, ps.fdlv_n == 8);   /* exactly once each */
    for (int i = 0; i < 8 && i < ps.fdlv_n; i++) {
        R_CHECK(&rig, ps.fdlv[i].object == (uint64_t)i);          /* in order */
        R_CHECK(&rig, ps.fdlv[i].fp == (uint8_t)(0x20 + i));      /* payload   */
    }
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_backpressure\n");
    }
    return rig.failures;
}

/* No stale state after close: tear the rig down with a fetch in flight (a tiny
 * action queue guarantees a pinned object is still held) -> conn_detach ->
 * binding_close retires the core fetch (pins released), the bind table frees.
 * No crash, allocator balance clean. */
static int
fetch_close_midstream(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 1;
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_max_actions = 0;
        printf("FAIL: fetch close rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* reads the knob */
    g_test_max_actions = 0;
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 8);
    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t tk = fetch_pub_track(&rig, pb, "vc");
    for (uint64_t o = 0; o < 8; o++) {
        fetch_ing(&rig, tk, 0, 0, o, (uint8_t)(0x30 + o));
    }
    rig_pump(&rig, 4);
    moq_fetch_t fh;
    fetch_issue(&rig, sub, "vc", 0, 0, 0, 8, &fh);
    rig_pump(&rig, 3);   /* accept + a few objects, then still in flight */
    rig_destroy(&rig);   /* teardown mid-fetch */
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_close_midstream\n");
    }
    return rig.failures;
}

/* Terminal liveness: an ACCEPTED fetch that hits an unrecoverable write error
 * must give the peer a wire terminal (a hard session close), never a silent
 * hang or a false FETCH_COMPLETE. Forced deterministically with a
 * accepted fetch whose retained range is evicted (capacity eviction from new
 * ingest on the active track) before it drains can no longer be served fully:
 * peek returns TOO_OLD and the binding hard-closes the session (INTERNAL_ERROR)
 * rather than FIN a truncated range or hang. A tiny action queue holds the fetch
 * mid-stream so the eviction lands while it is still open. (Earlier this was
 * driven by the d16 datagram-preference INVAL; now that case is clear-and-
 * serve, so mid-fetch eviction is now the post-admission terminal under test.) */
static int
fetch_error_terminates(void)
{
    ca_t a;
    ca_init(&a);
    g_test_log_max_groups = 2;   /* retain 2 groups -> new ingest evicts fast */
    g_test_max_actions = 3;      /* FETCH_OK flushes, but the drain backs up */
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_log_max_groups = 0;
        g_test_max_actions = 0;
        printf("FAIL: fetch terminate rig create\n");
        return 1;
    }
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* reads the knobs */
    g_test_max_actions = 0;
    g_test_log_max_groups = 0;
    R_CHECK(&rig, sub != NULL);
    rig_pump(&rig, 8);
    moqr_binding_t pb = fetch_pub_open(&rig);
    moqr_track_t tk = fetch_pub_track(&rig, pb, "vd");
    for (uint64_t o = 0; o < 24; o++) {               /* too big to drain in a few */
        fetch_ing(&rig, tk, 5, 0, o, (uint8_t)(0x50 + o));  /* pumps before evict  */
    }
    rig_pump(&rig, 4);
    moq_fetch_t fh;
    fetch_issue(&rig, sub, "vd", 5, 0, 5, 0, &fh);   /* whole group 5 */
    rig_pump(&rig, 3);   /* fetch_open + accept + a few objects; then it backs up */
    /* New groups evict group 5 (max_groups=2) underneath the open fetch. */
    fetch_ing(&rig, tk, 6, 0, 0, 0x60);
    fetch_ing(&rig, tk, 7, 0, 0, 0x70);
    rig_pump(&rig, 20);   /* peek now sees cur_group(5) < oldest(6) -> TOO_OLD */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    peer_drain(&rig, sub, &ps, false, false, 0, 0);
    /* Terminal: session close (INTERNAL_ERROR), never a truncated FETCH_COMPLETE
     * or a false FETCH_ERROR over a range that couldn't be fully served. (The
     * hard close preempts the buffered FETCH_OK/objects in this sequence, which
     * is fine: the session close is itself the terminal.) */
    R_CHECK(&rig, ps.session_closed);
    R_CHECK(&rig, ps.close_code == 0x1u);      /* session INTERNAL_ERROR        */
    R_CHECK(&rig, ps.fetch_complete == 0);
    R_CHECK(&rig, ps.fetch_error == 0);
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: fetch_error_terminates\n");
    }
    return rig.failures;
}

/* ---- Retained FETCH representation parity ---------------------------------- *
 * With production streaming ingest on, a warmed NORMAL object is
 * stored as a chunked COMPLETE record (obj_state COMPLETE, chunk_count > 0,
 * payload NULL, bytes in the log chunk list). Retained standalone FETCH must
 * serve that representation exactly like a whole-object record. This oracle warms
 * the SAME object both ways — flipping only g_test_server_streaming_objects, with
 * identical streamed publish code — and proves an identical FETCH result (End
 * Location, object identity/order, payload length + bytes, completion, no false
 * error/close). The gauntlet repro was: chunked run yields FETCH_OK + zero
 * objects + FETCH_COMPLETE. RED: restore the `chunk_count > 0` skip in
 * fetch_scan_next and the chunked run drops to n == 0, so parity fails. */

typedef struct {
    bool     fetch_ok, fok_eot, session_closed;
    uint64_t fok_end_group, fok_end_object;
    int      fetch_complete, fetch_error, n;
    uint64_t fetch_error_code;
    struct {
        uint64_t group, subgroup, object, len;
        uint32_t fp_all;
        bool     datagram;
    } obj[16];
} fetch_res_t;

static int
fetch_res_eq(const fetch_res_t *x, const fetch_res_t *y)
{
    if (x->fetch_ok != y->fetch_ok || x->fok_eot != y->fok_eot ||
        x->fok_end_group != y->fok_end_group ||
        x->fok_end_object != y->fok_end_object ||
        x->fetch_complete != y->fetch_complete ||
        x->fetch_error != y->fetch_error ||
        x->fetch_error_code != y->fetch_error_code ||
        x->session_closed != y->session_closed || x->n != y->n) {
        return 0;
    }
    for (int i = 0; i < x->n && i < 16; i++) {
        if (x->obj[i].group != y->obj[i].group ||
            x->obj[i].subgroup != y->obj[i].subgroup ||
            x->obj[i].object != y->obj[i].object ||
            x->obj[i].len != y->obj[i].len ||
            x->obj[i].fp_all != y->obj[i].fp_all ||
            x->obj[i].datagram != y->obj[i].datagram) {
            return 0;
        }
    }
    return 1;
}

/* Stream one chunk (len bytes of `byte`) into the active object. */
static void
parity_write_chunk(rig_t *r, conn_t *pub, moq_subgroup_handle_t sg, uint8_t byte,
                   size_t len)
{
    uint8_t body[256];
    memset(body, byte, len);
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, len, &pl) == MOQ_OK);
    R_CHECK(r, moq_session_write_object_data(pub->peer, sg, pl, r->now) == MOQ_OK);
    moq_rcbuf_decref(pl);
}

/* Warm one track over the production SimPair binding and capture a standalone
 * FETCH's wire result. `streaming` selects whole-object (false) vs chunked
 * COMPLETE (true) storage; `max_actions` (0 = default) constrains ONLY the
 * fetcher's session, driving fetch-egress WOULD_BLOCK so a coalesced object is
 * held + replayed across pumps without drop/dup. */
static int
retained_fetch_parity_run(moq_version_t version, bool streaming,
                          uint32_t max_actions, fetch_res_t *out)
{
    memset(out, 0, sizeof(*out));
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = streaming;
    g_test_client_streaming_objects = streaming;
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        printf("FAIL: parity rig create (stream=%d v%d)\n", (int)streaming,
               (int)version);
        return 1;
    }
    /* Only the publisher's relay-side session ingests, so streaming is scoped to
     * its connect; the warmer and fetcher are plain sessions. */
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    conn_t *warmer = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = max_actions;   /* constrain only the fetcher */
    conn_t *fetcher = rig_connect(&rig, version);
    g_test_max_actions = 0;
    R_CHECK(&rig, pub != NULL && warmer != NULL && fetcher != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    /* Warmer subscribes so the relay opens an upstream SUBSCRIBE to pull the
     * publisher's objects into the retained log. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t s1;
    R_CHECK(&rig, moq_session_subscribe(warmer->peer, &scfg, rig.now, &s1) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, warm_ps, fetch_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&warm_ps, 0, sizeof(warm_ps));
    memset(&fetch_ps, 0, sizeof(fetch_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);   /* accept upstream */
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);
    peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* Same streamed publish for BOTH representations: group 0 / subgroup 0 holds
     * obj0 (one 32-byte chunk) and obj1 (three DISTINCT 32-byte chunks, so a drop
     * or reorder changes the coalesced length/fingerprint). */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 0;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sg) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sg, 0, 32, rig.now) ==
                      MOQ_OK);
    parity_write_chunk(&rig, pub, sg, 0xA0, 32);
    R_CHECK(&rig, moq_session_end_object(pub->peer, sg, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sg, 1, 96, rig.now) ==
                      MOQ_OK);
    parity_write_chunk(&rig, pub, sg, 0xB1, 32);
    parity_write_chunk(&rig, pub, sg, 0xB2, 32);
    parity_write_chunk(&rig, pub, sg, 0xB3, 32);
    R_CHECK(&rig, moq_session_end_object(pub->peer, sg, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sg, rig.now) == MOQ_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* Fetcher issues an identical whole-group-0 standalone FETCH. */
    moq_fetch_t fh;
    fetch_issue(&rig, fetcher, "v", 0, 0, 0, 0, &fh);
    for (int p = 0; p < 32; p++) {
        rig_pump(&rig, 2);
        peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);
        peer_drain(&rig, fetcher, &fetch_ps, false, false, 0, 0);
        if (fetch_ps.fetch_complete || fetch_ps.fetch_error ||
            fetch_ps.session_closed) {
            break;
        }
    }

    out->fetch_ok = fetch_ps.fetch_ok;
    out->fok_eot = fetch_ps.fok_eot;
    out->fok_end_group = fetch_ps.fok_end_group;
    out->fok_end_object = fetch_ps.fok_end_object;
    out->fetch_complete = fetch_ps.fetch_complete;
    out->fetch_error = fetch_ps.fetch_error;
    out->fetch_error_code = fetch_ps.fetch_error_code;
    out->session_closed = fetch_ps.session_closed;
    out->n = fetch_ps.fdlv_n;
    for (int i = 0; i < fetch_ps.fdlv_n && i < 16; i++) {
        out->obj[i].group = fetch_ps.fdlv[i].group;
        out->obj[i].subgroup = fetch_ps.fdlv[i].subgroup;
        out->obj[i].object = fetch_ps.fdlv[i].object;
        out->obj[i].len = fetch_ps.fdlv[i].len;
        out->obj[i].fp_all = fetch_ps.fdlv[i].fp_all;
        out->obj[i].datagram = fetch_ps.fdlv[i].datagram;
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    return rig.failures;
}

static int
retained_fetch_representation_parity_oracle(moq_version_t version,
                                            uint32_t max_actions)
{
    fetch_res_t whole, chunked;
    int f = 0;
    f += retained_fetch_parity_run(version, false, max_actions, &whole);
    f += retained_fetch_parity_run(version, true, max_actions, &chunked);

    int bad = 0;
    /* Absolute sanity on BOTH representations. */
    if (!whole.fetch_ok || !chunked.fetch_ok) bad = 1;
    if (whole.n != 2 || chunked.n != 2) bad = 1;
    if (whole.fetch_complete != 1 || chunked.fetch_complete != 1) bad = 1;
    if (whole.fetch_error != 0 || chunked.fetch_error != 0) bad = 1;
    if (whole.session_closed || chunked.session_closed) bad = 1;
    /* The multi-chunk object carries all 96 bytes in BOTH representations. */
    if (whole.n == 2 && (whole.obj[1].len != 96 || chunked.obj[1].len != 96)) {
        bad = 1;
    }
    /* Representation parity: byte-identical wire result. */
    if (!fetch_res_eq(&whole, &chunked)) bad = 1;
    f += bad;

    if (f == 0) {
        printf("PASS: retained_fetch_representation_parity_oracle(v%d%s)\n",
               (int)version, max_actions ? ", aq" : "");
    } else {
        printf("FAIL: retained_fetch_representation_parity_oracle(v%d%s) "
               "whole{ok=%d n=%d comp=%d err=%d o1len=%llu} "
               "chunked{ok=%d n=%d comp=%d err=%d o1len=%llu}\n",
               (int)version, max_actions ? ", aq" : "",
               (int)whole.fetch_ok, whole.n, whole.fetch_complete,
               whole.fetch_error,
               (unsigned long long)(whole.n == 2 ? whole.obj[1].len : 0),
               (int)chunked.fetch_ok, chunked.n, chunked.fetch_complete,
               chunked.fetch_error,
               (unsigned long long)(chunked.n == 2 ? chunked.obj[1].len : 0));
    }
    return f;
}

/* Binding-level fail-closed under OOM. A chunked COMPLETE track is
 * warmed; a fetcher's standalone FETCH is admitted (FETCH_OK), then the coalesce
 * buffer allocation is forced to fail (a distinctively sized alloc — 3x111=333 B).
 * The relay must terminate the fetcher VISIBLY (session close via
 * bind_fetch_terminate) and NEVER emit a false FETCH_COMPLETE or a truncated
 * object; teardown must leave the allocator balanced (no leaked coalesce buffer). */
static int
chunked_fetch_oom_terminates(void)
{
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    rig_t rig;
    int cr = rig_create(&rig, &a);
    if (cr != 0) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        printf("FAIL: chunked_fetch_oom rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    conn_t *warmer = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *fetcher = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL && warmer != NULL && fetcher != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t s1;
    R_CHECK(&rig, moq_session_subscribe(warmer->peer, &scfg, rig.now, &s1) ==
                      MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t pub_ps, warm_ps, fetch_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&warm_ps, 0, sizeof(warm_ps));
    memset(&fetch_ps, 0, sizeof(fetch_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 4);
    peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* One chunked object, distinctively sized (3 x 111 = 333 bytes) so the
     * coalesce buffer alloc is unambiguous to target by size. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 0;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sg) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sg, 0, 333, rig.now) ==
                      MOQ_OK);
    parity_write_chunk(&rig, pub, sg, 0xC1, 111);
    parity_write_chunk(&rig, pub, sg, 0xC2, 111);
    parity_write_chunk(&rig, pub, sg, 0xC3, 111);
    R_CHECK(&rig, moq_session_end_object(pub->peer, sg, rig.now) == MOQ_OK);
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sg, rig.now) == MOQ_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);

    /* Issue the FETCH, then arm the coalesce-buffer OOM (exactly 333 bytes). The
     * fetch is admitted (FETCH_OK) before the coalesce runs. */
    moq_fetch_t fh;
    fetch_issue(&rig, fetcher, "v", 0, 0, 0, 0, &fh);
    a.fail_size = 333;
    for (int p = 0; p < 32; p++) {
        rig_pump(&rig, 2);
        peer_drain(&rig, warmer, &warm_ps, false, false, 0, 0);
        peer_drain(&rig, fetcher, &fetch_ps, false, false, 0, 0);
        if (fetch_ps.session_closed || fetch_ps.fetch_complete) {
            break;
        }
    }
    /* The one-shot cleared fail_size => the 333-byte coalesce buffer alloc was
     * actually hit (not a vacuous pass). Combined with the assertions below, only
     * the coalesce failing can produce this state. */
    R_CHECK(&rig, a.fail_size == 0);
    a.fail_size = 0;

    /* Fail-closed: a VISIBLE terminal (session close), never a false completion
     * or a truncated object. */
    R_CHECK(&rig, fetch_ps.session_closed);        /* bind_fetch_terminate close */
    R_CHECK(&rig, fetch_ps.fetch_complete == 0);   /* no false FETCH_COMPLETE    */
    R_CHECK(&rig, fetch_ps.fdlv_n == 0);           /* no (truncated) object      */

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);                    /* coalesce buffer not leaked */
    if (rig.failures == 0) {
        printf("PASS: chunked_fetch_oom_terminates\n");
    }
    return rig.failures;
}

/* Records what the bind's intent router received, and can ask the binding to
 * park a routed ACCEPT_SUB (return false) a bounded number of times. */
typedef struct rtr_rec {
    int                count;        /* total intents routed                  */
    int                accept_count; /* ACCEPT_SUB intents routed             */
    moqr_intent_kind_t last_kind;
    uint64_t           last_cookie;
    moqr_track_t       last_track;
    uint64_t           last_track_gen;
    int                park_accepts; /* return false for this many ACCEPT_SUB */
} rtr_rec_t;

static bool
rtr_route(void *ctx, const moqr_intent_t *it, uint64_t now_us)
{
    (void)now_us;
    rtr_rec_t *r = ctx;
    r->count++;
    r->last_kind = it->kind;
    r->last_cookie = it->binding_cookie;
    r->last_track = it->track;
    r->last_track_gen = it->track_gen;
    if (it->kind == MOQR_INTENT_ACCEPT_SUB) {
        r->accept_count++;
        if (r->park_accepts > 0) {
            r->park_accepts--;
            return false; /* not now: the binding parks it for retry */
        }
    }
    return true; /* consumed */
}

/* R8 intent-router seam: an output intent whose binding_cookie is a reserved
 * shard-manager cookie (>= MOQR_SHARD_COOKIE_BASE) is handed to the router
 * instead of a session; a normal cookie is not; and a router that declines a
 * deferrable ACCEPT_SUB has it parked and retried, like a WOULD_BLOCKed write.
 * RED: drop the router hook in bind_try_intent -> reserved intents are silently
 * moot and the router never fires. */
/* -- Attach-time draft validation ------------------------------------------ *
 * The binding encodes every terminal for the draft the reading peer speaks, so
 * a connection whose draft is unknown has no safe emission. Attach must refuse
 * it outright rather than substitute a plausible default: a silently assumed
 * draft is exactly the wrong-numbering failure this parameter exists to stop.
 * The rejection must also happen before any slot or core binding is taken, or a
 * refused attach would still cost capacity. */
static int
test_conn_version_fail_closed(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &core) == MOQR_OK);

    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a.vt);
    bcfg.core = core;
    bcfg.max_conns = 1;   /* the capacity boundary a rejected attach must not cost */
    moqr_bind_t *bind = NULL;
    MOQ_TEST_CHECK(moqr_bind_create(&bcfg, &bind) == MOQR_OK);

    /* Two real sessions; the second only ever attaches after the rejections. */
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &a.vt;
    scfg.seed = 0x11117777u;
    scfg.version = MOQ_VERSION_DRAFT_18;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg, &sp) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
    moq_session_t *sess = moq_simpair_server(sp);

    /* Unattached and null lookups name no draft at all. */
    MOQ_TEST_CHECK(moqr_bind_conn_version(NULL, sess) == 0);
    MOQ_TEST_CHECK(moqr_bind_conn_version(bind, NULL) == 0);
    MOQ_TEST_CHECK(moqr_bind_conn_version(bind, sess) == 0);

    /* Every unsupported value is refused, including the zero a caller gets from
     * a connection whose version is not yet negotiated. */
    MOQ_TEST_CHECK(moqr_bind_conn_open(bind, sess, (moq_version_t)0) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_bind_conn_open(bind, sess, (moq_version_t)17) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_bind_conn_open(bind, sess, (moq_version_t)0xFFFFu) ==
                   MOQR_ERR_INVAL);
    /* Refused attaches named no draft and, at max_conns == 1, cost no slot. */
    MOQ_TEST_CHECK(moqr_bind_conn_version(bind, sess) == 0);

    /* d18 round-trips exactly, and the boundary was still free. */
    MOQ_TEST_CHECK(moqr_bind_conn_open(bind, sess, MOQ_VERSION_DRAFT_18) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_conn_version(bind, sess) == MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK(moqr_bind_conn_close(bind, sess) == MOQR_OK);

    /* d16 round-trips exactly on the same reclaimed slot. */
    moq_simpair_cfg_t scfg16 = MOQ_SIMPAIR_CFG_INIT;
    scfg16.alloc = &a.vt;
    scfg16.seed = 0x22228888u;
    scfg16.version = MOQ_VERSION_DRAFT_16;
    moq_simpair_t *sp16 = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&scfg16, &sp16) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_start(sp16) == MOQ_OK);
    moq_session_t *sess16 = moq_simpair_server(sp16);
    MOQ_TEST_CHECK(moqr_bind_conn_open(bind, sess16, MOQ_VERSION_DRAFT_16) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_conn_version(bind, sess16) == MOQ_VERSION_DRAFT_16);

    moq_simpair_destroy(sp16);
    moq_simpair_destroy(sp);
    moqr_bind_destroy(bind);
    moqr_core_destroy(core);
    return failures;
}

static int
test_intent_router(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    rtr_rec_t rec;
    memset(&rec, 0, sizeof(rec));

    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 8;
    cfg.log_budget.max_bytes = 1u << 20;
    cfg.linger_us = 500;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &core) == MOQR_OK);

    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a.vt);
    bcfg.core = core;
    bcfg.max_conns = 8;
    bcfg.router = rtr_route;
    bcfg.router_ctx = &rec;
    /* Leave router_cookie_base 0: a router with base 0 defaults to
     * MOQR_SHARD_COOKIE_BASE, so real connection cookies still reach sessions
     * (proven by (2) below — a normal-cookie ACCEPT_SUB is not routed). */
    moqr_bind_t *bind = NULL;
    MOQ_TEST_CHECK(moqr_bind_create(&bcfg, &bind) == MOQR_OK);
    uint64_t now = 1;

    /* (1) A reserved-cookie announcer's UPSTREAM_SUBSCRIBE is routed. */
    moqr_binding_t pb;   /* reserved pseudo-binding (announcer) */
    MOQ_TEST_CHECK(moqr_core_binding_open(core, MOQR_SHARD_COOKIE_BASE | 5u,
                                          &pb) == MOQR_OK);
    moqr_binding_t subb; /* a normal binding (subscriber) */
    MOQ_TEST_CHECK(moqr_core_binding_open(core, 1, &subb) == MOQR_OK);
    moq_bytes_t nsp[1] = { B("demo") };
    moqr_ns_t ns = { nsp, 1 };
    MOQ_TEST_CHECK(moqr_core_announce(core, pb, ns) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = ns;
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 100;
    moqr_sub_t s;
    MOQ_TEST_CHECK(moqr_core_subscribe(core, subb, &rq, &s) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_pump(bind, now) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(rec.count, 1);
    MOQ_TEST_CHECK(rec.last_kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK_EQ_U64(rec.last_cookie, MOQR_SHARD_COOKIE_BASE | 5u);
    moqr_track_t track = rec.last_track;
    uint64_t tgen = rec.last_track_gen;

    /* (2) The subscriber is a NORMAL cookie: resolving the upstream accepts it,
     * and that ACCEPT_SUB (cookie 1) is NOT routed. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(core, track, tgen, 777, true, 4, 9) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_pump(bind, now) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(rec.count, 1);   /* still 1: cookie 1 went to a session */

    /* (3) A reserved-cookie SUBSCRIBER's ACCEPT_SUB is routed, and a router that
     * declines it once has it parked and retried. Both announcer and subscriber
     * are reserved here so both of their intents route (giving us the track
     * handle from the routed UPSTREAM_SUBSCRIBE). */
    rec.park_accepts = 1;   /* decline the first routed ACCEPT_SUB once */
    moqr_binding_t pubb;    /* reserved announcer */
    MOQ_TEST_CHECK(moqr_core_binding_open(core, MOQR_SHARD_COOKIE_BASE | 7u,
                                          &pubb) == MOQR_OK);
    moqr_binding_t psub;    /* reserved pump subscriber */
    MOQ_TEST_CHECK(moqr_core_binding_open(core, MOQR_SHARD_COOKIE_BASE | 6u,
                                          &psub) == MOQR_OK);
    moq_bytes_t nsp2[1] = { B("demo2") };
    moqr_ns_t ns2 = { nsp2, 1 };
    MOQ_TEST_CHECK(moqr_core_announce(core, pubb, ns2) == MOQR_OK);
    moqr_subscribe_req_t rq2;
    moqr_subscribe_req_init(&rq2);
    rq2.ns = ns2;
    rq2.name = B("video");
    rq2.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq2.cookie = 200;
    moqr_sub_t s2;
    MOQ_TEST_CHECK(moqr_core_subscribe(core, psub, &rq2, &s2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_pump(bind, now) == MOQR_OK); /* UPSTREAM_SUBSCRIBE -> pubb, routed */
    MOQ_TEST_CHECK(rec.last_kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_track_t track2 = rec.last_track;
    uint64_t tgen2 = rec.last_track_gen;
    int accepts_before = rec.accept_count;
    /* Resolve upstream: psub's ACCEPT_SUB targets a reserved cookie -> routed;
     * the router declines it once (parked), the retry consumes it. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(core, track2, tgen2, 777, true, 4, 9) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_pump(bind, now) == MOQR_OK); /* route ACCEPT_SUB -> false -> park */
    MOQ_TEST_CHECK_EQ_INT(rec.accept_count, accepts_before + 1);
    MOQ_TEST_CHECK(moqr_bind_pump(bind, now) == MOQR_OK); /* retry parked ACCEPT_SUB -> consumed */
    MOQ_TEST_CHECK_EQ_INT(rec.accept_count, accepts_before + 2);
    MOQ_TEST_CHECK_EQ_INT(rec.park_accepts, 0);

    moqr_bind_destroy(bind);
    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("intent_router");
    return failures;
}

/* ==== ready-delivery scheduler (Phase F consumes the core ready set) ======= */

/* Retry a peer-session call that can hit its own action-queue limit while a
 * fixture holds the transport still: pace it with pump cycles (bounded). */
#define RDY_RETRY(r, expr)                                                   \
    do {                                                                     \
        moq_result_t rrc_ = (expr);                                          \
        for (int t_ = 0; rrc_ == MOQ_ERR_WOULD_BLOCK && t_ < 64; t_++) {     \
            rig_cycle(r);                                                    \
            rrc_ = (expr);                                                   \
        }                                                                    \
        R_CHECK(r, rrc_ == MOQ_OK);                                          \
    } while (0)

/* Shared mini-fixture: optionally announce ns {n0,n1} from `pub`, subscribe
 * `name` from `sub`, auto-accept upstream, and quiesce. Fills pub_ps. */
static void
rdy_wire_track(rig_t *r, conn_t *pub, conn_t *sub, const char *n0,
               const char *n1, const char *name, bool announce,
               peer_state_t *pub_ps, moq_subscription_t *sh)
{
    moq_bytes_t nsp[2] = { B(n0), B(n1) };
    if (announce) {
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        RDY_RETRY(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now,
                                                   &ann));
        rig_pump(r, 6);
    }
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B(name);
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    RDY_RETRY(r, moq_session_subscribe(sub->peer, &scfg, r->now, sh));
    rig_pump(r, 6);
    peer_drain(r, pub, pub_ps, true, false, 0, 0);
    rig_pump(r, 6);
}

/* Retry a publisher-session call by advancing ONLY that connection's
 * transport (plus the bind pump): the other connections' queues stay
 * exactly as the fixture left them. */
#define RDY_RETRY_ONE(r, cn, expr)                                           \
    do {                                                                     \
        moq_result_t rrc_ = (expr);                                          \
        for (int t_ = 0; rrc_ == MOQ_ERR_WOULD_BLOCK && t_ < 64; t_++) {     \
            (r)->now += 1000;                                                \
            (void)moq_simpair_advance_to((cn)->sp, (r)->now);                \
            size_t st_ = 0;                                                  \
            (void)moq_simpair_run_until_quiescent((cn)->sp, 64, &st_);       \
            (void)moqr_bind_pump((r)->bind, (r)->now);                       \
            rrc_ = (expr);                                                   \
        }                                                                    \
        R_CHECK(r, rrc_ == MOQ_OK);                                          \
    } while (0)

/* One pump cycle with a DEEP transport budget: bulk fixtures move hundreds
 * of objects per stream, far past rig_cycle's 64-step quiescence cap. */
static void
rdy_deep_cycle(rig_t *r)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 65536, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

/* Publish one whole object into an open-and-FINished subgroup, paced (each
 * session call retries through pump cycles when the peer queue is tiny). */
static void
rdy_pub_one(rig_t *r, conn_t *pub, moq_subscription_t up_sub, uint64_t group)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t h;
    RDY_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                           &h));
    uint8_t body[4] = { (uint8_t)group, 1, 2, 3 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    RDY_RETRY(r, moq_session_write_object(pub->peer, h, 0, pl, r->now));
    moq_rcbuf_decref(pl);
    RDY_RETRY(r, moq_session_close_subgroup(pub->peer, h, r->now));
}

/* Idle ACTIVE bindings cost ZERO delivery probes: after one full delivery
 * quiesces, repeated pumps must not touch the delivery half of ANY conn —
 * including a second, never-trafficked subscriber on an unrelated track.
 * A fresh publish then resumes within one pump (the mark is the wake). */
static int
ready_idle_zero_probes(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_idle_zero_probes rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *idle = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub && idle);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps, i_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    memset(&i_ps, 0, sizeof(i_ps));
    moq_subscription_t sh, ih;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    R_CHECK(&rig, pub_ps.up_seen);
    /* The unrelated subscriber: its own track, never any traffic. */
    peer_state_t pub_ps2;
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    rdy_wire_track(&rig, pub, idle, "live", "cam", "u", false,
                   &pub_ps2, &ih);

    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    rig_pump(&rig, 12);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    /* Steady idle: N pumps, zero probes. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    rig_pump(&rig, 20);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);

    /* A fresh publish wakes exactly through the mark. */
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    rig_pump(&rig, 12);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() > p0);
    peer_drain(&rig, idle, &i_ps, false, false, 0, 0);
    R_CHECK(&rig, i_ps.objects == 0);

    /* And idle again: the wake did not leave a standing poll behind. */
    uint64_t p1 = moqr_bind_debug_delivery_probes();
    rig_pump(&rig, 20);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_idle_zero_probes\n");
    }
    return f;
}

/* ACTION_CAP: a delivery blocked on the session action queue parks with a
 * capacity floor. While the transport does not drain (capacity at the
 * floor), pumps cost ZERO probes; the first pump after the drain retries
 * once and completes. */
static int
ready_action_cap_park(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 1;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: ready_action_cap_park rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    rig_pump(&rig, 8);   /* setup spreads over more pumps at max_actions 1 */
    R_CHECK(&rig, pub_ps.up_seen);

    /* Publish paced through ONLY the publisher's transport: the object
     * ingests, the delivery attempt fills the subscriber session's single
     * action slot, and the binding parks — the subscriber's transport never
     * drains during this window. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t h;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, pub_ps.up_sub, &sgc, rig.now,
                                     &h));
        uint8_t body[4] = { 0xAC, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_write_object(pub->peer, h, 0, pl, rig.now));
        moq_rcbuf_decref(pl);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_close_subgroup(pub->peer, h, rig.now));
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    /* Parked: with the queue full and no drain, pumps are probe-free —
     * and the blocked conn (rig slot 1) shows EXACTLY ONE action-cap park
     * with no further park entries across the frozen window. */
    uint64_t bc0[3];
    moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc0);
    R_CHECK(&rig, bc0[0] == 1 && bc0[1] == 0 && bc0[2] == 0);
    /* The LIVE aggregate reports this as the CURRENT exceptional set: two
     * live conns, exactly one reached action-cap and is parked on it right
     * now; no session-sg park anywhere. This pins parked_* against a real
     * park (seeding-only tests cannot exercise the park bits). */
    {
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 2);
        R_CHECK(&rig, agg.conns_action_cap == 1 && agg.action_cap_total == 1);
        R_CHECK(&rig, agg.parked_action_cap == 1);
        R_CHECK(&rig, agg.conns_session_sg == 0 && agg.session_sg_total == 0);
        R_CHECK(&rig, agg.conns_bind_sg == 0 && agg.parked_session_sg == 0);
    }
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    for (int i = 0; i < 6; i++) {
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    {
        uint64_t bc1[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc1);
        R_CHECK(&rig, bc1[0] == bc0[0]);   /* zero new parks while frozen */
    }

    /* Drain the subscriber transport once: capacity rises above the floor;
     * the next pump makes exactly one recovery pass. */
    rig.now += 1000;
    (void)moq_simpair_advance_to(sub->sp, rig.now);
    size_t steps = 0;
    (void)moq_simpair_run_until_quiescent(sub->sp, 64, &steps);
    uint64_t p1 = moqr_bind_debug_delivery_probes();
    (void)moqr_bind_pump(rig.bind, rig.now);
    uint64_t p2 = moqr_bind_debug_delivery_probes();
    R_CHECK(&rig, p2 > p1);
    R_CHECK(&rig, p2 - p1 <= 4);   /* one bounded pass, not a poll storm */

    rig_pump(&rig, 12);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_action_cap_park\n");
    }
    return f;
}

/* The LIVE blocked aggregate: seeded per-conn counters drive the summation,
 * LIVE-only exclusion of a detached slot that keeps stale counters, clean
 * reuse of a recycled slot, the fail-closed NULL guards, and the checked-add
 * overflow refusal. (Park populations are pinned separately against a REAL
 * park in ready_action_cap_park — seeding sets only the history counters.) */
static int
ready_blocked_aggregate(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_blocked_aggregate rig create\n");
        return 1;
    }
    conn_t *c0 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *c1 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *c2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, c0 && c1 && c2);
    rig_pump(&rig, 4);

    /* Seed each live slot with a distinct reason's history. */
    const uint64_t s0[3] = { 5, 0, 0 };   /* action-cap */
    const uint64_t s1[3] = { 0, 3, 0 };   /* session-sg */
    const uint64_t s2[3] = { 0, 0, 7 };   /* bind-sg     */
    R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 0, s0));
    R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 1, s1));
    R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 2, s2));

    {
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 3);
        R_CHECK(&rig, agg.conns_action_cap == 1 && agg.action_cap_total == 5);
        R_CHECK(&rig, agg.conns_session_sg == 1 && agg.session_sg_total == 3);
        R_CHECK(&rig, agg.conns_bind_sg == 1 && agg.bind_sg_total == 7);
        R_CHECK(&rig, agg.parked_action_cap == 0 &&
                          agg.parked_session_sg == 0);
    }

    /* Seeding a non-live or out-of-range slot is refused. */
    R_CHECK(&rig, !moqr_bind_debug_seed_blocked(rig.bind, 99, s0));
    R_CHECK(&rig, !moqr_bind_debug_seed_blocked(NULL, 0, s0));

    /* NULL guards fail closed with a zeroed output. */
    {
        moqr_bind_blocked_agg_t agg;
        memset(&agg, 0xAB, sizeof(agg));
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(NULL, &agg) ==
                          MOQR_ERR_INVAL);
        R_CHECK(&rig, agg.live_conns == 0 && agg.action_cap_total == 0);
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, NULL) ==
                          MOQR_ERR_INVAL);
    }

    /* Overflow: two live slots each at UINT64_MAX for the same reason make
     * the total's checked add refuse; the output is zeroed (no partial sum). */
    {
        const uint64_t big[3] = { UINT64_MAX, 0, 0 };
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 0, big));
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 2, big));
        moqr_bind_blocked_agg_t agg;
        memset(&agg, 0xAB, sizeof(agg));
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_ERR_INTERNAL);
        R_CHECK(&rig, agg.live_conns == 0 && agg.action_cap_total == 0 &&
                          agg.conns_action_cap == 0);
        /* restore sane seeds */
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 0, s0));
        R_CHECK(&rig, moqr_bind_debug_seed_blocked(rig.bind, 2, s2));
    }

    /* Detach slot 1: its session-sg counter stays on the slot (stale), but a
     * LIVE aggregate must exclude it. Prove the counter is still physically
     * present via the raw per-slot getter, yet absent from the aggregate. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, c1->rsess) == MOQR_OK);
    rig_pump(&rig, 4);   /* let any WOULD_BLOCK detach settle */
    {
        uint64_t raw[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, raw);
        R_CHECK(&rig, raw[1] == 3);   /* counter retained on the dead slot */
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 2);
        R_CHECK(&rig, agg.conns_session_sg == 0 && agg.session_sg_total == 0);
        R_CHECK(&rig, agg.conns_action_cap == 1 && agg.action_cap_total == 5);
        R_CHECK(&rig, agg.conns_bind_sg == 1 && agg.bind_sg_total == 7);
    }

    /* Reuse: a new conn takes the lowest free bind slot (slot 1), whose
     * counters were memset clean on open — the stale history is gone. */
    conn_t *c3 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, c3 != NULL);
    rig_pump(&rig, 4);
    {
        uint64_t raw[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, raw);
        R_CHECK(&rig, raw[0] == 0 && raw[1] == 0 && raw[2] == 0);
        moqr_bind_blocked_agg_t agg;
        R_CHECK(&rig, moqr_bind_debug_blocked_aggregate(rig.bind, &agg) ==
                          MOQR_OK);
        R_CHECK(&rig, agg.live_conns == 3);
        R_CHECK(&rig, agg.conns_session_sg == 0 && agg.session_sg_total == 0);
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_blocked_aggregate\n");
    }
    return f;
}

/* BIND_SG: with a one-slot downstream subgroup pool, a second concurrent
 * subgroup blocks WITHOUT parking or polling (zero probes while the slot is
 * held) and resumes from the slot-release re-arm when the first subgroup
 * seals — no new object is published between block and resume. */
static int
ready_bind_sg_rearm(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_open_subgroups = 1;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_open_subgroups = 0;
        printf("FAIL: ready_bind_sg_rearm rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Subgroup A: one object, stream left OPEN (holds the only slot). */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                            rig.now, &sga) == MOQ_OK);
    {
        uint8_t body[4] = { 0xA0, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(pub->peer, sga, 0, pl,
                                               rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    /* Subgroup B (group 1): blocked on the bind slot pool. */
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);   /* still held out */

    /* No park, no poll: zero probes while the slot stays held — and the
     * blocked conn (rig slot 1) shows EXACTLY ONE bind-sg refusal, with no
     * growth across the quiet window. */
    uint64_t bc0[3];
    moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc0);
    R_CHECK(&rig, bc0[2] == 1 && bc0[0] == 0 && bc0[1] == 0);
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    rig_pump(&rig, 10);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    {
        uint64_t bc1[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc1);
        R_CHECK(&rig, bc1[2] == bc0[2]);
    }

    /* FIN A upstream: the SEAL closes the downstream stream, the freed slot
     * re-arms the binding, and B flows — with no new ingest. */
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sga, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 12);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);

    g_test_max_open_subgroups = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_bind_sg_rearm\n");
    }
    return f;
}

/* A hot binding with >256 queued deliveries cannot starve an intermittent
 * one: the guard ends the hot pass, the hot binding self-rearms (BUDGET),
 * and the intermittent binding is served in the same pump. */
static int
ready_hot_not_starving(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_events = 512;   /* consumers must outlive a 256-burst pass */
    g_test_max_actions = 1024; /* the guard, not the queue, must bind      */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_events = 0;
        g_test_max_actions = 0;
        printf("FAIL: ready_hot_not_starving rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *starter = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *hot = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *cold = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && starter && hot && cold);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, st_ps, h_ps, c_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&st_ps, 0, sizeof(st_ps));
    memset(&h_ps, 0, sizeof(h_ps));
    memset(&c_ps, 0, sizeof(c_ps));
    moq_subscription_t sth, hh, ch;
    rdy_wire_track(&rig, pub, starter, "live", "cam", "hotv", true, &pub_ps,
                   &sth);
    R_CHECK(&rig, pub_ps.up_seen);
    peer_state_t pub_ps2;
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    rdy_wire_track(&rig, pub, cold, "live", "cam", "coldv", false,
                   &pub_ps2, &ch);
    R_CHECK(&rig, pub_ps2.up_seen);

    /* Retain 300 objects in ONE subgroup (paced; the starter consumes them
     * live), then LATE-JOIN the hot subscriber: its accept mark schedules a
     * 300-object catch-up — deeper than the 256-per-pass guard. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t h;
        RDY_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                  &sgc, rig.now, &h));
        for (uint64_t o = 0; o < 300; o++) {
            uint8_t body[4] = { (uint8_t)o, 1, 2, 3 };
            moq_rcbuf_t *pl = NULL;
            R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                              MOQ_OK);
            RDY_RETRY(&rig, moq_session_write_object(pub->peer, h, o, pl,
                                                     rig.now));
            moq_rcbuf_decref(pl);
            if ((o & 15u) == 15u) {
                rig_cycle(&rig);   /* pace the wire burst */
            }
        }
        /* The subgroup stays OPEN: a FIN's SEAL notice would mark the
         * binding and mask a lost BUDGET self-rearm. */
    }
    for (int w = 0; w < 200 && st_ps.objects < 300; w++) {
        rdy_deep_cycle(&rig);
        peer_drain(&rig, starter, &st_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, st_ps.objects == 300);

    /* Late-join with the transports held still around the catch-up: the
     * first delivery pass must write EXACTLY the 256-delivery guard and the
     * SECOND pass (same pump cadence, no transport help, no new mark) must
     * make progress — that progress exists ONLY through the BUDGET
     * self-rearm. */
    {
        moq_bytes_t nsp[2] = { B("live"), B("cam") };
        moq_subscribe_cfg_t scfg;
        memset(&scfg, 0, sizeof(scfg));
        moq_subscribe_cfg_init(&scfg);
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        scfg.track_name = B("hotv");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        R_CHECK(&rig, moq_session_subscribe(hot->peer, &scfg, rig.now,
                                            &hh) == MOQ_OK);
    }
    rig.now += 1000;
    (void)moq_simpair_advance_to(hot->sp, rig.now);
    {
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(hot->sp, 4096, &steps);
    }
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        uint64_t w0 = bs.deliveries_written;
        (void)moqr_bind_pump(rig.bind, rig.now);   /* accept + pass 1 */
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.deliveries_written - w0 == 256);   /* the guard */
        uint64_t w1 = bs.deliveries_written;
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);   /* pass 2: self-rearm */
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.deliveries_written > w1);
    }
    rdy_pub_one(&rig, pub, pub_ps2.up_sub, 0);
    rig_pump(&rig, 3);
    peer_drain(&rig, cold, &c_ps, false, false, 0, 0);
    peer_drain(&rig, hot, &h_ps, false, false, 0, 0);
    R_CHECK(&rig, c_ps.objects == 1);   /* served despite hot's backlog */
    for (int w = 0; w < 200 && h_ps.objects < 300; w++) {
        rdy_deep_cycle(&rig);
        peer_drain(&rig, hot, &h_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, h_ps.objects == 300);   /* BUDGET self-rearm completed it */

    g_test_max_events = 0;
    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_hot_not_starving\n");
    }
    return f;
}

/* SESSION_SG: with the relay session's outgoing subgroup pool at 2 and the
 * bind slot pool roomy, a third concurrent subgroup blocks on the SESSION
 * pool (WOULD_BLOCK with action-queue room). The parked binding gets
 * EXACTLY ONE bounded attempt per pump — no more — and recovers through the
 * reap performed by a later attempt's own open prologue once an upstream
 * FIN closes a downstream stream; the action-queue capacity round-trips to
 * the same value across that cycle, so no capacity gate could see it. */
static int
ready_session_sg_poll(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_open_subgroups = 8;          /* bind slots: NOT the blocker */
    g_test_server_max_open_subgroups = 2;   /* session pool: the blocker   */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_open_subgroups = 0;
        g_test_server_max_open_subgroups = 0;
        printf("FAIL: ready_session_sg_poll rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Three concurrent upstream subgroups (groups 0..2), one object each,
     * streams left OPEN: two occupy the session pool, the third blocks. */
    moq_subgroup_handle_t sg[3];
    for (uint64_t g = 0; g < 3; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        RDY_RETRY(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub,
                                                  &sgc, rig.now, &sg[g]));
        uint8_t body[4] = { (uint8_t)g, 5, 6, 7 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY(&rig,
                  moq_session_write_object(pub->peer, sg[g], 0, pl, rig.now));
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 10);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 2);   /* the third is pool-blocked */

    /* EXACTLY one SESSION_SG re-attempt per blocked binding per pump (the
     * reason-specific gauge counts pump-granted attempts), and the
     * exceptional set holds exactly this one binding. */
    {
        uint64_t bc0[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc0);
        for (int i = 0; i < 4; i++) {
            uint64_t g0 = moqr_bind_debug_sg_attempts();
            rig_cycle(&rig);
            R_CHECK(&rig, moqr_bind_debug_sg_attempts() - g0 == 1);
            R_CHECK(&rig, moqr_bind_debug_sg_park_count(rig.bind) == 1);
        }
        uint64_t bc1[3];
        moqr_bind_debug_conn_blocked_counts(rig.bind, 1, bc1);
        /* every granted attempt re-blocked in the session pool: exact */
        R_CHECK(&rig, bc1[1] - bc0[1] == 4);
    }

    /* FIN subgroup 0: the SEAL closes one downstream stream (CLOSING); the
     * next attempt's own prologue reaps it and the third subgroup opens —
     * the action queue ends where it started (same-value cycle). */
    RDY_RETRY(&rig, moq_session_close_subgroup(pub->peer, sg[0], rig.now));
    rig_pump(&rig, 10);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 3);

    g_test_max_open_subgroups = 0;
    g_test_server_max_open_subgroups = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_session_sg_poll\n");
    }
    return f;
}

/* The control-path SUB_DONE re-arm (the one recovery edge with NO core mark
 * guarantee): subscription X holds the ONLY bind subgroup slot mid-object;
 * sibling Y (same connection, other track) is BIND_SG-blocked. X's track is
 * force-withdrawn while the subscriber's transport is held still, so the
 * SUB_DONE intent DEFERS (action queue full) past the pump that consumed
 * X's retirement mark. When the deferred intent finally executes, its slot
 * release must re-arm the binding — nothing else ever will. */
static int
ready_subdone_rearm(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_open_subgroups = 1;
    g_test_max_actions = 1;
    g_test_server_streaming_objects = true;   /* X stays mid-object (begun) */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_open_subgroups = 0;
        g_test_max_actions = 0;
        g_test_server_streaming_objects = false;
        printf("FAIL: ready_subdone_rearm rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);
    peer_state_t px, py, s_ps;
    memset(&px, 0, sizeof(px));
    memset(&py, 0, sizeof(py));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t xh, yh;
    /* X on namespace {wa,x}; Y on {wb,y} — force_withdraw hits X only. */
    rdy_wire_track(&rig, pub, sub, "wa", "x", "tx", true, &px, &xh);
    rig_pump(&rig, 8);
    R_CHECK(&rig, px.up_seen);
    rdy_wire_track(&rig, pub, sub, "wb", "y", "ty", true, &py, &yh);
    rig_pump(&rig, 8);
    R_CHECK(&rig, py.up_seen);

    /* X: open subgroup + one object, stream open — X holds the only bind
     * slot with a begun object downstream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgx;
    RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                 pub->peer, px.up_sub, &sgc, rig.now, &sgx));
    {
        uint8_t body[4] = { 0x58, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub, moq_session_write_object(pub->peer, sgx, 0,
                                                          pl, rig.now));
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 10);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects >= 1 || s_ps.chunks >= 1);

    /* Y: one whole object — blocked on the bind slot X holds. */
    {
        moq_subgroup_cfg_t syc;
        moq_subgroup_cfg_init(&syc);
        syc.group_id = 0;
        syc.subgroup_id = 0;
        syc.publisher_priority = 0;
        moq_subgroup_handle_t sgy;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, py.up_sub, &syc, rig.now,
                                     &sgy));
        uint8_t body[4] = { 0x59, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub, moq_session_write_object(pub->peer, sgy, 0,
                                                          pl, rig.now));
        moq_rcbuf_decref(pl);
        /* Y's subgroup stays OPEN: a FIN's SEAL notice would mark the
         * binding and mask a lost slot-release re-arm. */
    }
    for (int i = 0; i < 4; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }

    /* Withdraw X's namespace with the SUBSCRIBER transport held still: the
     * SUB_DONE reset needs the single action slot, so the intent defers
     * while the retirement mark is consumed by a still-blocked attempt. */
    {
        moq_bytes_t nsp[2] = { B("wa"), B("x") };
        moqr_result_t wrc = moqr_core_force_withdraw(
            rig.core, (moqr_ns_t){ nsp, 2 }, 0x10, rig.now);
        R_CHECK(&rig, wrc == MOQR_OK || wrc == MOQR_ERR_WOULD_BLOCK);
        for (int i = 0; wrc == MOQR_ERR_WOULD_BLOCK && i < 100; i++) {
            rig.now += 1000;
            (void)moqr_bind_pump(rig.bind, rig.now);
            wrc = moqr_core_force_withdraw(rig.core, (moqr_ns_t){ nsp, 2 },
                                           0x10, rig.now);
        }
        R_CHECK(&rig, wrc == MOQR_OK);
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);   /* mark consumed here */
    }
    /* Release ONLY the subscriber's transport: the deferred SUB_DONE
     * executes over the next pumps (its reset consumes the single action
     * slot, so the DONE itself may re-defer once more, holding Phase F
     * off), and Y's held delivery resumes purely through the slot-release
     * re-arm — no other transport moves, so no mark can rescue a lost one. */
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        uint64_t w0 = bs.deliveries_written;
        bool resumed = false;
        for (int i = 0; i < 6 && !resumed; i++) {
            rig.now += 1000;
            (void)moq_simpair_advance_to(sub->sp, rig.now);
            size_t steps = 0;
            (void)moq_simpair_run_until_quiescent(sub->sp, 4096, &steps);
            (void)moqr_bind_pump(rig.bind, rig.now);
            moqr_bind_get_stats(rig.bind, &bs);
            resumed = bs.deliveries_written > w0;
        }
        R_CHECK(&rig, resumed);   /* Y resumed with the sub transport only */
    }
    rig_pump(&rig, 12);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects >= 1);     /* Y's object flowed after */
    R_CHECK(&rig, s_ps.last_byte == 0x59);

    g_test_max_open_subgroups = 0;
    g_test_max_actions = 0;
    g_test_server_streaming_objects = false;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_subdone_rearm\n");
    }
    return f;
}

/* A failed delivery confirmation is a consistency failure: the connection
 * must FAIL CLOSED (session closed, conn detached) — never survive as a
 * live, unschedulable zombie. The slot must be fully reusable after. */
static int
ready_confirm_failure_closes(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_confirm_failure_closes rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Inject: the next confirmation misfires mid-delivery. */
    moqr_bind_debug_fail_confirm(1);
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    rig_pump(&rig, 8);
    moqr_bind_debug_fail_confirm(0);
    R_CHECK(&rig, !moqr_bind_conn_is_open(rig.bind, sub->rsess));
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.session_errors >= 1);
    }

    /* The slot is reusable and probe-clean from its first pump. */
    moq_simpair_destroy(sub->sp);
    sub->used = false;
    conn_t *re = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, re != NULL);
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    rig_pump(&rig, 8);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    peer_state_t r_ps, pub_ps2;
    memset(&r_ps, 0, sizeof(r_ps));
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    moq_subscription_t rh;
    rdy_wire_track(&rig, pub, re, "live", "cam", "w", false, &pub_ps2, &rh);
    R_CHECK(&rig, pub_ps2.up_seen);
    rdy_pub_one(&rig, pub, pub_ps2.up_sub, 0);
    rig_pump(&rig, 10);
    peer_drain(&rig, re, &r_ps, false, false, 0, 0);
    R_CHECK(&rig, r_ps.objects == 1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_confirm_failure_closes\n");
    }
    return f;
}

/* The probe-error classification is CAPACITY-only: an injected CAPACITY
 * result self-rearms and the delivery completes on the next pump; every
 * other injected error (NOMEM included — the signed state machine does not
 * allow it as transient) fails the connection closed. */
static int
ready_probe_error_policy(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_probe_error_policy rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    R_CHECK(&rig, pub_ps.up_seen);

    /* CAPACITY: transient — the object still arrives. */
    moqr_bind_debug_fail_probe(1, MOQR_ERR_CAPACITY);
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    rig_pump(&rig, 10);
    moqr_bind_debug_fail_probe(0, MOQR_OK);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);
    R_CHECK(&rig, moqr_bind_conn_is_open(rig.bind, sub->rsess));

    /* NOMEM: NOT transient — the connection fails closed. */
    moqr_bind_debug_fail_probe(1, MOQR_ERR_NOMEM);
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 1, 0, 0);
    rig_pump(&rig, 10);
    moqr_bind_debug_fail_probe(0, MOQR_OK);
    R_CHECK(&rig, !moqr_bind_conn_is_open(rig.bind, sub->rsess));
    {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rig.bind, &bs);
        R_CHECK(&rig, bs.session_errors >= 1);
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_probe_error_policy\n");
    }
    return f;
}

/* A property-bearing object write needs TWO action slots, so it refuses
 * with capacity 1: the park must record THAT floor and make ZERO retries
 * until capacity exceeds it — a merely-nonzero gate would retry every
 * pump. */
static int
ready_property_write_floor(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 2;   /* open takes one slot; props need two */
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: ready_property_write_floor rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps, &sh);
    rig_pump(&rig, 8);
    R_CHECK(&rig, pub_ps.up_seen);

    /* One object WITH properties, paced through the publisher only: the
     * downstream open consumes one of two slots, the property write then
     * sees capacity 1 < 2 and refuses. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        sgc.object_properties = true;
        moq_subgroup_handle_t h;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, pub_ps.up_sub, &sgc, rig.now,
                                     &h));
        uint8_t body[4] = { 0xF1, 1, 2, 3 };
        /* One valid property entry (type 2, varint value 9) — the same
         * wire form both drafts accept. */
        uint8_t props[2] = { 0x02, 0x09 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        moq_object_cfg_t ocfg;
        moq_object_cfg_init(&ocfg);
        ocfg.object_id = 0;
        ocfg.payload = pl;
        moq_rcbuf_t *pr = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, props, sizeof(props), &pr) ==
                          MOQ_OK);
        ocfg.properties = pr;
        RDY_RETRY_ONE(&rig, pub, moq_session_write_object_ex(pub->peer, h,
                                                             &ocfg, rig.now));
        moq_rcbuf_decref(pl);
        moq_rcbuf_decref(pr);
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    /* Parked with the OBSERVED floor: with the subscriber transport held,
     * capacity stays AT the floor and pumps make zero probes/attempts. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    for (int i = 0; i < 6; i++) {
        rig.now += 1000;
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);

    /* Drain once: capacity rises past the floor; the delivery completes. */
    rig.now += 1000;
    (void)moq_simpair_advance_to(sub->sp, rig.now);
    {
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(sub->sp, 4096, &steps);
    }
    (void)moqr_bind_pump(rig.bind, rig.now);
    rig_pump(&rig, 10);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);
    R_CHECK(&rig, s_ps.last_props_len == 2);

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_property_write_floor\n");
    }
    return f;
}

/* force_withdraw against an IDLE system must wake the subscriber's binding:
 * the terminal reaches the wire although no delivery mark preceded it. */
static int
ready_withdraw_idle_wake(void)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: ready_withdraw_idle_wake rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    R_CHECK(&rig, pub_ps.up_seen);
    red_pub_subgroup_plain_fin(&rig, pub, pub_ps.up_sub, 0, 0, 0);
    rig_pump(&rig, 10);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.objects == 1);

    /* Idle... then a relay-topology withdrawal mid-idle. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    rig_pump(&rig, 10);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    R_CHECK(&rig, moqr_core_force_withdraw(rig.core,
                                           (moqr_ns_t){ nsp, 2 }, 0x10,
                                           rig.now) == MOQR_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.done_seen);   /* the DONE reached the wire */

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_withdraw_idle_wake\n");
    }
    return f;
}

/* Detach with standing ready/parked state must not leak into the slot's next
 * claimant: a replacement connection starts probe-free and fully usable. */
static int
ready_detach_replacement_clean(void)
{
    ca_t a;
    ca_init(&a);
    g_test_max_actions = 1;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = 0;
        printf("FAIL: ready_detach_replacement_clean rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    moq_subscription_t sh;
    rdy_wire_track(&rig, pub, sub, "live", "cam", "v", true, &pub_ps,
                   &sh);
    rig_pump(&rig, 8);
    R_CHECK(&rig, pub_ps.up_seen);

    /* Park the subscriber's binding (blocked delivery, undrained queue). */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 0;
        moq_subgroup_handle_t h;
        RDY_RETRY_ONE(&rig, pub, moq_session_open_subgroup(
                                     pub->peer, pub_ps.up_sub, &sgc, rig.now,
                                     &h));
        uint8_t body[4] = { 0xDC, 1, 2, 3 };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_write_object(pub->peer, h, 0, pl, rig.now));
        moq_rcbuf_decref(pl);
        RDY_RETRY_ONE(&rig, pub,
                      moq_session_close_subgroup(pub->peer, h, rig.now));
    }
    for (int i = 0; i < 3; i++) {
        rig.now += 1000;
        (void)moq_simpair_advance_to(pub->sp, rig.now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(pub->sp, 64, &steps);
        (void)moqr_bind_pump(rig.bind, rig.now);
    }
    /* Detach mid-park; the conn slot is reclaimed by a fresh connection. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, sub->rsess) == MOQR_OK);
    rig_pump(&rig, 4);
    moq_simpair_destroy(sub->sp);
    sub->used = false;
    conn_t *re = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, re != NULL);

    /* The replacement inherits nothing — asserted from its very first
     * pump: even a single stray probe means state leaked across reuse. */
    uint64_t p0 = moqr_bind_debug_delivery_probes();
    rig_pump(&rig, 10);
    R_CHECK(&rig, moqr_bind_debug_delivery_probes() == p0);

    /* And it is fully usable end-to-end. */
    peer_state_t r_ps;
    memset(&r_ps, 0, sizeof(r_ps));
    moq_subscription_t rh;
    peer_state_t pub_ps2;
    memset(&pub_ps2, 0, sizeof(pub_ps2));
    rdy_wire_track(&rig, pub, re, "live", "cam", "w", false, &pub_ps2,
                   &rh);
    rig_pump(&rig, 8);
    R_CHECK(&rig, pub_ps2.up_seen);
    rdy_pub_one(&rig, pub, pub_ps2.up_sub, 0);
    rig_pump(&rig, 12);
    peer_drain(&rig, re, &r_ps, false, false, 0, 0);
    R_CHECK(&rig, r_ps.objects == 1);

    g_test_max_actions = 0;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ready_detach_replacement_clean\n");
    }
    return f;
}

/* ============================ R8H-FORWARD ==================================
 * Downstream Forward-state (SUBSCRIBE_UPDATE §10.2.12): Forward=0 PAUSES
 * object delivery for one subscription without terminating it; Forward=1
 * resumes. Before the fix a paused sub's next write drew MOQ_ERR_WRONG_STATE,
 * which the bind mapped to STREAM_ERROR and the core retired the sub. These
 * scenarios drive real SUBSCRIBE_UPDATE bytes over SimPair and assert the
 * subscription survives, emits nothing while paused, and resumes exact-once.
 * Backlog policy (stated, not spec-required): an object published DURING the
 * pause is retained and delivered on resume (the cursor never advances past
 * it); a FRESH post-resume object is the unambiguous exact-once anchor.
 * Neuter hooks (RED proof) are gated on env so each moving part can be
 * independently disabled to show a dedicated failure. */

static const moq_bytes_t FWD_NS[2] = { { (const uint8_t *)"live", 4 },
                                       { (const uint8_t *)"cam1", 4 } };

static void
fwd_announce(rig_t *r, conn_t *pub, peer_state_t *pub_ps)
{
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace =
        (moq_namespace_t){ .parts = (moq_bytes_t *)FWD_NS, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now,
                                             &ann) == MOQ_OK);
    rig_pump(r, 4);
    (void)pub_ps;
}

/* Subscribe `sub` to live/cam1 video; optionally set the initial Forward bit.
 * Accepts the relay's upstream SUBSCRIBE at the publisher (largest {0,0}).
 * Returns the client subscription handle for later updates. */
static void
fwd_subscribe_named(rig_t *r, conn_t *pub, peer_state_t *pub_ps, conn_t *sub,
                    peer_state_t *sub_ps, moq_subscription_t *sh,
                    moq_bytes_t name, bool set_forward, bool forward)
{
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace =
        (moq_namespace_t){ .parts = (moq_bytes_t *)FWD_NS, .count = 2 };
    scfg.track_name = name;
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    scfg.has_forward = set_forward;
    scfg.forward = forward;
    R_CHECK(r, moq_session_subscribe(sub->peer, &scfg, r->now, sh) == MOQ_OK);
    rig_pump(r, 4);
    peer_drain(r, pub, pub_ps, true, true, 0, 0);   /* accept upstream */
    rig_pump(r, 4);
    peer_drain(r, sub, sub_ps, false, false, 0, 0);
    R_CHECK(r, sub_ps->subscribe_ok);
}

/* Video-track shorthand for the single-subscription oracle. */
static void
fwd_subscribe(rig_t *r, conn_t *pub, peer_state_t *pub_ps, conn_t *sub,
              peer_state_t *sub_ps, moq_subscription_t *sh, bool set_forward,
              bool forward)
{
    fwd_subscribe_named(r, pub, pub_ps, sub, sub_ps, sh, B("video"),
                        set_forward, forward);
}

/* Publish one whole object (its own subgroup) on the relay's upstream sub. */
static void
fwd_pub_whole(rig_t *r, ca_t *a, conn_t *pub, peer_state_t *pub_ps,
              uint64_t group, uint8_t byte)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, pub_ps->up_sub, &sgc,
                                         r->now, &sgh) == MOQ_OK);
    uint8_t body[32];
    memset(body, byte, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&a->vt, body, sizeof(body), &pl) == MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, sgh, 0, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
    R_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
    rig_pump(r, 6);
}

static void
fwd_update_forward(rig_t *r, conn_t *sub, moq_subscription_t sh, bool forward)
{
    moq_subscription_update_cfg_t ucfg;
    moq_subscription_update_cfg_init(&ucfg);
    ucfg.has_forward = true;
    ucfg.forward = forward;
    R_CHECK(r, moq_session_update_subscription(sub->peer, sh, &ucfg,
                                               r->now) == MOQ_OK);
    rig_pump(r, 6);
}

/* (1) Initial Forward=0: established, no emission, no retirement; Forward=1
 *     resumes. (2) 1->0->1: pause with data pending, nothing emitted while
 *     paused, sub+conn live, backlog + a fresh object resume exact-once.
 *     (5) idempotence: 0->0, 1->1, and an omit-FORWARD update are no-ops. */
static int
forward_pause_resume_oracle(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: fwd rig create (v%d)\n", (int)version);
        return 1;
    }
    int failures = 0;
    conn_t *pub = rig_connect(&rig, version);
    conn_t *sub = rig_connect(&rig, version);
    R_CHECK(&rig, pub && sub);
    peer_state_t pub_ps, sps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sps, 0, sizeof(sps));
    rig_pump(&rig, 4);
    fwd_announce(&rig, pub, &pub_ps);

    /* --- initial Forward=0: established but paused --- */
    (void)failures;
    moq_subscription_t sh;
    fwd_subscribe(&rig, pub, &pub_ps, sub, &sps, &sh, true, false);
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 0, 0xA0);   /* data while paused */
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 0);       /* no emission while paused */
    R_CHECK(&rig, !sps.done_seen);         /* NOT retired */
    R_CHECK(&rig, !sps.session_closed);    /* NOT disconnected */
    /* resume -> the backlog object delivers exactly once */
    fwd_update_forward(&rig, sub, sh, true);
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 1 && sps.last_byte == 0xA0);
    R_CHECK(&rig, !sps.done_seen);
    /* idempotent 1->1 and omit-FORWARD: no effect, no retire, no dup */
    fwd_update_forward(&rig, sub, sh, true);   /* 1->1 */
    {
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;   /* update WITHOUT forward */
        ucfg.subscriber_priority = 7;
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, sh, &ucfg,
                                                      rig.now) == MOQ_OK);
        rig_pump(&rig, 4);
    }
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 1 && !sps.done_seen);

    /* --- 1->0->1 on the live subscription --- */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 1, 0xB1);   /* delivered live */
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 2 && sps.last_byte == 0xB1);
    fwd_update_forward(&rig, sub, sh, false);   /* PAUSE */
    fwd_update_forward(&rig, sub, sh, false);   /* 0->0 idempotent */
    /* several earned pumps with data pending: nothing emitted, still live */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 2, 0xC2);   /* during pause */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 3, 0xC3);
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 2);        /* frozen while paused */
    R_CHECK(&rig, !sps.done_seen && !sps.session_closed);
    /* RESUME: backlog (g2,g3) replays, then a fresh object -> exact once */
    fwd_update_forward(&rig, sub, sh, true);
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 4);        /* 2 backlog delivered once each */
    fwd_pub_whole(&rig, &a, pub, &pub_ps, 4, 0xD4);   /* fresh post-resume */
    peer_drain(&rig, sub, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 5 && sps.last_byte == 0xD4);   /* exact-once */
    R_CHECK(&rig, !sps.done_seen);

    R_CHECK(&rig, moq_session_unsubscribe(sub->peer, sh, rig.now) == MOQ_OK);
    rig_pump(&rig, 4);
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: forward_pause_resume_oracle v%d\n", (int)version);
    }
    return rig.failures;
}

/* (3) Sibling isolation: two subs on one binding; pausing one must not stop
 *     the other from delivering, and neither is retired. */
static int
forward_sibling_oracle(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: fwd sibling rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *pub = rig_connect(&rig, version);
    conn_t *s = rig_connect(&rig, version);   /* ONE downstream session/binding */
    R_CHECK(&rig, pub && s);
    /* Two upstream subs (one per track) need distinct pub-side state to hold
     * each track's relay subscription handle. */
    peer_state_t pub_v, pub_au, sps;
    memset(&pub_v, 0, sizeof(pub_v));
    memset(&pub_au, 0, sizeof(pub_au));
    memset(&sps, 0, sizeof(sps));
    rig_pump(&rig, 4);
    fwd_announce(&rig, pub, &pub_v);
    /* Two subscriptions to DIFFERENT tracks on the SAME session => two subs on
     * one relay binding, so pausing one exercises binding-wide HOL. */
    moq_subscription_t hv, hau;
    fwd_subscribe_named(&rig, pub, &pub_v, s, &sps, &hv, B("video"), false, true);
    fwd_subscribe_named(&rig, pub, &pub_au, s, &sps, &hau, B("audio"), false,
                        true);
    /* both tracks deliver a first object over the shared session */
    fwd_pub_whole(&rig, &a, pub, &pub_v, 0, 0x50);    /* video */
    fwd_pub_whole(&rig, &a, pub, &pub_au, 0, 0xA0);   /* audio */
    peer_drain(&rig, s, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 2);
    /* pause the video sub; publish on BOTH tracks. The audio sibling must keep
     * flowing through the shared binding; video is frozen; neither retired. */
    fwd_update_forward(&rig, s, hv, false);
    fwd_pub_whole(&rig, &a, pub, &pub_v, 1, 0x51);    /* video, during pause */
    fwd_pub_whole(&rig, &a, pub, &pub_au, 1, 0xA1);   /* audio sibling */
    peer_drain(&rig, s, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 3 && sps.last_byte == 0xA1);   /* audio flowed */
    R_CHECK(&rig, !sps.done_seen && !sps.session_closed);
    /* resume video: it catches up to its paused object exactly once */
    fwd_update_forward(&rig, s, hv, true);
    peer_drain(&rig, s, &sps, false, false, 0, 0);
    R_CHECK(&rig, sps.objects == 4 && sps.last_byte == 0x51 && !sps.done_seen);
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: forward_sibling_oracle v%d\n", (int)version);
    }
    return rig.failures;
}

/* Cross-layer RED that makes the bind's begun-checkpoint call load-bearing, end
 * to end over SimPair through the real bind. A multi-chunk object is delivered
 * downstream under an action-constrained relay session (max_actions=2), so the
 * relay writes begin + some chunks and then WOULD_BLOCKs — and the bind drives
 * moqr_core_delivery_note_emitted, advancing the core's note_emitted_total.
 *
 * LOAD-BEARING assertion (the RED): note_emitted_total must advance. Deleting
 * the bind's production note_emitted call leaves it flat and fails this test —
 * verified. (NB: SimPair cannot make a *behavioral* RED here — the bind writes
 * chunks ahead into the session and reaches STALLED before a Forward=0 pause can
 * propagate, and STALLED marks begun independently, so the downstream RESET
 * survives deleting the call. The note_emitted_total counter is therefore the
 * load-bearing signal; the RESET below is integration coverage, not the RED.)
 *
 * The object is left OPEN and then abandoned (reset_subgroup) while paused: the
 * paused subscriber still observes a downstream RESET of the begun-but-unfinished
 * object and stays live. */
static int
forward_begin_zero_reset(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        printf("FAIL: fwd begin-zero rig create (v%d)\n", (int)version);
        return 1;
    }
    conn_t *pub = rig_connect(&rig, version);
    g_test_max_actions = 2;   /* constrain ONLY the downstream (sub) session */
    conn_t *sub = rig_connect(&rig, version);
    g_test_max_actions = 0;
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 8);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 8);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1;
    R_CHECK(&rig,
            moq_session_subscribe(sub->peer, &scfg, rig.now, &s1) == MOQ_OK);
    rig_pump(&rig, 8);
    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    rig_pump(&rig, 8);

    /* Group 1: an OPEN object whose 40 chunks are all available (the publisher
     * is not action-limited, so they land at the relay); the relay delivers it
     * downstream across several WOULD_BLOCK/resume cycles because the sub session
     * is action-limited. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 1280, rig.now) ==
                      MOQ_OK);
    /* 40 chunks are all AVAILABLE but the object is left OPEN (no end/close):
     * so the relay WOULD_BLOCKs per cycle as it delivers (recording begun via
     * note_emitted) and the object never completes. */
    for (int ci = 0; ci < 40; ci++) {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object_data(pub->peer, sga, pl,
                                                    rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }

    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    /* A couple cycles: the relay begins the object downstream and delivers some
     * chunks, WOULD_BLOCKing on the rest — begun downstream, not complete. */
    rig_pump(&rig, 2);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, !sub_ps.obj_complete && !sub_ps.saw_reset);
    /* Load-bearing cross-layer assertion: the bind hit a downstream WOULD_BLOCK
     * and drove moqr_core_delivery_note_emitted, so the CORE's begun-checkpoint
     * counter advanced. Deleting the bind's note_emitted call leaves this at
     * zero (the counter is incremented only inside the core function). */
    moqr_core_stats_t st1;
    moqr_core_get_stats(rig.core, &st1);
    R_CHECK(&rig, st1.note_emitted_total > st0.note_emitted_total);

    /* PAUSE releases the held delivery while the source object remains open,
     * leaving it begun but unfinished downstream (incomplete, and reset below). */
    {
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true;
        ucfg.forward = false;
        R_CHECK(&rig, moq_session_update_subscription(sub->peer, s1, &ucfg,
                                                      rig.now) == MOQ_OK);
    }
    rig_pump(&rig, 30);   /* let the pause settle; delivery freezes partway */
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, !sub_ps.obj_complete && !sub_ps.saw_reset);   /* still open */

    /* ABANDON the begun-but-unfinished object upstream (one action — no fresh
     * subgroups, so the publisher never overflows). */
    R_CHECK(&rig, moq_session_reset_subgroup(pub->peer, sga, 0x99, rig.now) ==
                      MOQ_OK);
    rig_pump(&rig, 20);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    /* The begun-but-unfinished downstream object is RESET, never silently left
     * hanging, and it never completed. */
    R_CHECK(&rig, sub_ps.saw_reset);
    R_CHECK(&rig, sub_ps.last_reset_code == 0x99);
    R_CHECK(&rig, !sub_ps.obj_complete);
    R_CHECK(&rig, !sub_ps.done_seen && !sub_ps.session_closed);   /* sub live */

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: forward_begin_zero_reset v%d\n", (int)version);
    }
    return rig.failures;
}

/* A namespace discovered for an existing prefix subscription must reach the
 * subscriber even when the downstream control path is momentarily blocked.
 * draft-ietf-moq-transport-18 Section 6.2: a relay that has received an
 * authorized PUBLISH_NAMESPACE "MUST send a NAMESPACE message to any
 * subscriber that has sent SUBSCRIBE_NAMESPACE for that namespace, or a
 * prefix of that namespace" -- a MUST, with no best-effort exemption for a
 * congested downstream.
 *
 * blocked=false is the healthy control (the same ordering on an unconstrained
 * queue); blocked=true constrains the session action queue so the relay's
 * send_namespace meets WOULD_BLOCK at the moment the announce lands.
 */
static int
ns_found_survives_blocked_control(bool blocked)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved_actions = g_test_max_actions;
    /* A depth-1 action queue: the prefix-subscription response occupies the
     * single slot, so the NAMESPACE that follows it has nowhere to go. */
    g_test_max_actions = blocked ? 1u : 0u;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (ns_found_survives_blocked_control)\n");
        g_test_max_actions = saved_actions;
        return 1;
    }

    g_test_max_actions = 0u;   /* publisher: normal queue */
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;   /* watcher: the blocked downstream path */
    conn_t *watch = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);   /* handshakes */

    /* Watcher registers interest in the prefix BEFORE anything is announced:
     * this is the subscribe-before-announce ordering. */
    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg,
                                                  rig.now, &nsh) == MOQ_OK);
    if (!blocked) {
        rig_pump(&rig, 4);
    }

    /* The publisher announces a matching namespace. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);

    /* Pump well past any transient block: a merely deferred NAMESPACE would
     * be delivered here, so a still-missing one was dropped, not delayed. */
    rig_pump(&rig, 64);
    peer_state_t pub_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    printf("  ns_found_survives_blocked_control(blocked=%d): "
           "namespace_accepted=%d ns_found=%d\n",
           (int)blocked, (int)pub_ps.namespace_accepted, w_ps.ns_found);

    /* The announce itself must succeed either way -- otherwise the ordering
     * never reached the state under test and the check below is vacuous. */
    R_CHECK(&rig, pub_ps.namespace_accepted);
    R_CHECK(&rig, w_ps.ns_found == 1);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved_actions;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ns_found_survives_blocked_control(blocked=%d)\n",
               (int)blocked);
    }
    return f;
}

/* --- root Track Namespace, end to end through session + bind + core --------
 *
 * draft-18 Section 2.4.1 gives a Track Namespace 0..32 fields, so the ROOT
 * namespace (zero fields) is a legal announce that a relay must accept, store,
 * route to matching prefix subscribers, and withdraw. draft-16 Section 2.4.1
 * gives 1..32, so the same announce is a protocol error there; that floor is
 * enforced by the session profile, ahead of the relay.
 *
 * This drives the real production entry points -- moq_session_publish_namespace
 * over a real session, the bind event path, and moqr_core_announce/unannounce
 * -- not a helper model.
 *
 * d16 = the draft-16 arm, where the root announce must be REFUSED. */
static int
root_namespace_end_to_end(bool d16)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = 0u;
    g_rv_ann_always_allow = true;
    g_test_authorize = rv_hook;
    moq_version_t ver = d16 ? MOQ_VERSION_DRAFT_16 : MOQ_VERSION_DRAFT_18;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (root_namespace_end_to_end)\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, ver);
    conn_t *watch = rig_connect(&rig, ver);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);

    /* A ROOT prefix subscription: it matches every namespace, including the
     * root namespace itself. */
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = NULL,
                                                      .count = 0 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg,
                                                  rig.now, &nsh) == MOQ_OK);
    rig_pump(&rig, 6);

    peer_state_t pub_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&w_ps, 0, sizeof(w_ps));

    /* A present-but-EMPTY field is a protocol error in BOTH profiles: 0..32
     * fields is about the FIELD COUNT, never about zero-length fields. */
    moq_bytes_t empty_part[1] = { { NULL, 0 } };
    moq_publish_namespace_cfg_t bad;
    memset(&bad, 0, sizeof(bad));
    moq_publish_namespace_cfg_init(&bad);
    bad.track_namespace = (moq_namespace_t){ .parts = empty_part, .count = 1 };
    moq_announcement_t bad_ann;
    int bad_rc = moq_session_publish_namespace(pub->peer, &bad, rig.now,
                                               &bad_ann);
    R_CHECK(&rig, bad_rc == MOQ_ERR_INVAL);   /* exact, not merely non-OK */

    /* The ROOT namespace itself. */
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = NULL, .count = 0 };
    moq_announcement_t ann;
    int rc = moq_session_publish_namespace(pub->peer, &pc, rig.now, &ann);
    printf("  root_ns(%s): empty_field_rc=%d root_rc=%d\n",
           d16 ? "d16" : "d18", bad_rc, rc);

    if (d16) {
        /* draft-16 Section 2.4.1: 1..32 fields. The profile refuses it before
         * the relay ever sees an event, so nothing is announced or routed. */
        R_CHECK(&rig, rc == MOQ_ERR_INVAL);   /* exact profile refusal */
        rig_pump(&rig, 32);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        printf("  root_ns(d16): accepted=%d ns_found=%d\n",
               (int)pub_ps.namespace_accepted, w_ps.ns_found);
        R_CHECK(&rig, !pub_ps.namespace_accepted);
        R_CHECK(&rig, w_ps.ns_found == 0);
        rig_destroy(&rig);
        rv_reset();
        R_CHECK(&rig, a.live == 0);
        g_test_max_actions = saved;
        int fd = rig.failures;
        if (fd == 0) {
            printf("PASS: root_namespace_end_to_end(d16 refused)\n");
        }
        return fd;
    }

    /* draft-18: accepted end to end, and ROUTED to the matching prefix. */
    R_CHECK(&rig, rc == MOQ_OK);
    rig_pump(&rig, 32);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    printf("  root_ns(d18): accepted=%d ns_found=%d\n",
           (int)pub_ps.namespace_accepted, w_ps.ns_found);
    R_CHECK(&rig, pub_ps.namespace_accepted);   /* the relay accepted it     */
    R_CHECK(&rig, w_ps.ns_found == 1);          /* and routed it, exactly once */

    /* A second announce of the same root namespace on the same session is
     * refused by the SESSION with MOQ_ERR_INVAL -- one live announcement per
     * namespace per session. So no second event reaches the relay, and the
     * unchanged ns_found below records session-level de-duplication; it is NOT
     * a proof of relay-side idempotence, which this path cannot reach. */
    moq_announcement_t ann2;
    int rc2 = moq_session_publish_namespace(pub->peer, &pc, rig.now, &ann2);
    R_CHECK(&rig, rc2 == MOQ_ERR_INVAL);
    rig_pump(&rig, 24);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    printf("  root_ns(d18): duplicate refused rc=%d, ns_found=%d\n",
           rc2, w_ps.ns_found);
    R_CHECK(&rig, w_ps.ns_found == 1);

    /* Withdrawal: the root namespace goes away exactly once. */
    R_CHECK(&rig, moq_session_publish_namespace_done(pub->peer, ann,
                                                     rig.now) == MOQ_OK);
    rig_pump(&rig, 32);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    printf("  root_ns(d18): ns_gone=%d\n", w_ps.ns_gone);
    R_CHECK(&rig, w_ps.ns_gone == 1);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: root_namespace_end_to_end(d18 accepted+routed)\n");
    }
    return f;
}

/* bind's defense-in-depth on Track Namespace Fields.
 *
 * The session parser rejects a malformed present field before any event reaches
 * the relay, so ann_store's arm cannot be driven through a real production
 * path. This drives the PRODUCTION validator directly through the gated probe
 * rather than copying its predicate: every present field must have len > 0 and
 * non-NULL data, while a zero-FIELD root namespace stays acceptable. */
static int
ann_store_rejects_empty_fields(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = 0u;
    g_rv_ann_always_allow = true;
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (ann_store_rejects_empty_fields)\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub != NULL);
    rig_pump(&rig, 4);

    static const uint8_t backing[4] = { 'l', 'i', 'v', 'e' };
    moq_bytes_t nul0[1]     = { { NULL, 0 } };
    moq_bytes_t nonnull0[1] = { { backing, 0 } };
    moq_bytes_t trailing[2] = { { backing, 4 }, { backing, 0 } };
    moq_bytes_t good[1]     = { { backing, 4 } };

    bool r_nul0     = moqr_bind_debug_ann_store_probe(rig.bind, 0, 9001,
                          (moqr_ns_t){ (const moq_bytes_t *)nul0, 1 });
    bool r_nonnull0 = moqr_bind_debug_ann_store_probe(rig.bind, 0, 9002,
                          (moqr_ns_t){ (const moq_bytes_t *)nonnull0, 1 });
    bool r_trailing = moqr_bind_debug_ann_store_probe(rig.bind, 0, 9003,
                          (moqr_ns_t){ (const moq_bytes_t *)trailing, 2 });
    bool r_root     = moqr_bind_debug_ann_store_probe(rig.bind, 0, 9004,
                          (moqr_ns_t){ NULL, 0 });
    bool r_good     = moqr_bind_debug_ann_store_probe(rig.bind, 0, 9005,
                          (moqr_ns_t){ (const moq_bytes_t *)good, 1 });
    printf("  ann_store: nul0=%d nonnull0=%d trailing=%d root=%d good=%d\n",
           (int)r_nul0, (int)r_nonnull0, (int)r_trailing, (int)r_root,
           (int)r_good);

    R_CHECK(&rig, !r_nul0);       /* NULL, zero-length                       */
    R_CHECK(&rig, !r_nonnull0);   /* NON-NULL, zero-length                   */
    R_CHECK(&rig, !r_trailing);   /* every field inspected, not only field 0 */
    R_CHECK(&rig, r_root);        /* zero FIELDS is the legal root namespace */
    R_CHECK(&rig, r_good);        /* a well-formed field still stores        */

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: ann_store_rejects_empty_fields\n");
    }
    return f;
}

/* A root FULL TRACK NAME over a real d18 session, through bind and core.
 *
 * draft-18 Section 2.4.1 builds a Full Track Name from a 0..32-field Track
 * Namespace plus a Track Name, so a track may legitimately live directly under
 * the ROOT namespace. This goes further than namespace discovery: it drives
 * subscribe -> upstream publish/open -> routed acceptance through the
 * production entry points. */
static int
root_full_track_name_session_flow(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = 0u;
    g_rv_ann_always_allow = true;
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (root_full_track_name_session_flow)\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && sub);
    rig_pump(&rig, 4);

    peer_state_t pub_ps, sub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));

    /* The publisher owns the ROOT namespace. */
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = NULL, .count = 0 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted);

    /* A subscriber asks for a track whose namespace IS the root. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = NULL, .count = 0 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    int src = moq_session_subscribe(sub->peer, &scfg, rig.now, &sh);
    R_CHECK(&rig, src == MOQ_OK);
    rig_pump(&rig, 8);

    /* The relay must resolve it to the root publisher and issue the upstream
     * subscribe, which the publisher accepts. */
    peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    printf("  root_ftn: up_subs=%d up_seen=%d subscribe_ok=%d errors=%d\n",
           pub_ps.up_subs, (int)pub_ps.up_seen, (int)sub_ps.subscribe_ok,
           sub_ps.subscribe_errors);
    R_CHECK(&rig, pub_ps.up_subs == 1);       /* routed upstream exactly once */
    R_CHECK(&rig, pub_ps.up_seen);            /* and the publisher accepted it */
    R_CHECK(&rig, sub_ps.subscribe_ok);       /* accepted downstream           */
    R_CHECK(&rig, sub_ps.subscribe_errors == 0);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: root_full_track_name_session_flow\n");
    }
    return f;
}

/* --- mixed-profile namespace discovery ------------------------------------
 *
 * One listener serves draft-16 and draft-18 sessions concurrently over the same
 * relay core and bind. draft-18 Section 2.4.1 allows a 0-field Track Namespace;
 * draft-16 Section 2.4.1 requires 1..32. So a ROOT namespace announced by a d18
 * publisher has NO representation a d16 peer may legally receive: sending it a
 * reconstructed zero-field Track Namespace would put an invalid namespace on a
 * d16 wire.
 *
 * This records what the assembled stack actually does. The acceptable outcome is
 * that the root FOUND/GONE pair is suppressed for the d16 subscriber while its
 * session stays healthy, and that NON-root discovery is unchanged for both. */
static int
mixed_profile_root_discovery(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = 0u;
    g_rv_ann_always_allow = true;
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (mixed_profile_root_discovery)\n");
        return 1;
    }
    conn_t *pub18 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *pub16 = rig_connect(&rig, MOQ_VERSION_DRAFT_16);
    conn_t *sub16 = rig_connect(&rig, MOQ_VERSION_DRAFT_16);
    conn_t *sub18 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub18 && pub16 && sub16 && sub18);
    rig_pump(&rig, 6);

    /* Both subscribers take the EMPTY prefix, which is legal in both drafts and
     * matches every namespace. */
    for (int i = 0; i < 2; i++) {
        conn_t *w = (i == 0) ? sub16 : sub18;
        moq_subscribe_namespace_cfg_t nscfg;
        memset(&nscfg, 0, sizeof(nscfg));
        moq_subscribe_namespace_cfg_init(&nscfg);
        nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = NULL,
                                                          .count = 0 };
        nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t nsh;
        R_CHECK(&rig, moq_session_subscribe_namespace(w->peer, &nscfg, rig.now,
                                                      &nsh) == MOQ_OK);
    }
    rig_pump(&rig, 8);

    peer_state_t p18, p16, s16, s18;
    memset(&p18, 0, sizeof(p18));
    memset(&p16, 0, sizeof(p16));
    memset(&s16, 0, sizeof(s16));
    memset(&s18, 0, sizeof(s18));

    /* 1. d18 publisher announces the ROOT namespace. */
    moq_publish_namespace_cfg_t root;
    memset(&root, 0, sizeof(root));
    moq_publish_namespace_cfg_init(&root);
    root.track_namespace = (moq_namespace_t){ .parts = NULL, .count = 0 };
    moq_announcement_t root_ann;
    int root_rc = moq_session_publish_namespace(pub18->peer, &root, rig.now,
                                                &root_ann);
    rig_pump(&rig, 32);
    peer_drain(&rig, pub18, &p18, false, false, 0, 0);
    peer_drain(&rig, sub16, &s16, false, false, 0, 0);
    peer_drain(&rig, sub18, &s18, false, false, 0, 0);
    printf("  mixed: root_rc=%d d18pub_accepted=%d | d16sub found=%d "
           "gone=%d | d18sub found=%d gone=%d\n",
           root_rc, (int)p18.namespace_accepted, s16.ns_found, s16.ns_gone,
           s18.ns_found, s18.ns_gone);
    R_CHECK(&rig, root_rc == MOQ_OK);          /* exact d18 root publish     */
    R_CHECK(&rig, p18.namespace_accepted);     /* the relay accepted it      */
    R_CHECK(&rig, s18.ns_found == 1);          /* d18 sees it exactly once   */
    R_CHECK(&rig, s18.ns_gone == 0);
    /* The production contract: a peer whose profile cannot represent a
     * zero-field Track Namespace is never sent one. */
    R_CHECK(&rig, s16.ns_found == 0);   /* no root FOUND on a d16 wire */
    R_CHECK(&rig, s16.ns_gone == 0);
    int s16_root_found = 0;
    /* Suppression happens BEFORE retain/owed accounting, so the d16 peer's
     * root update was never queued, retained or counted as owed. */
    {
        moqr_bind_stats_t rs;
        moqr_bind_get_stats(rig.bind, &rs);
        printf("  mixed: after root live=%u retained=%llu failed=%llu\n",
               rs.nsu_live, (unsigned long long)rs.nsu_retained,
               (unsigned long long)rs.nsu_failed_closed);
        R_CHECK(&rig, rs.nsu_live == 0);
        R_CHECK(&rig, rs.nsu_retained == 0);
        R_CHECK(&rig, rs.nsu_failed_closed == 0);
    }

    /* 2. Withdraw it, and see whether a GONE appears without a FOUND. */
    if (root_rc == MOQ_OK) {
        R_CHECK(&rig, moq_session_publish_namespace_done(
                          pub18->peer, root_ann, rig.now) == MOQ_OK);
        rig_pump(&rig, 32);
        peer_drain(&rig, sub16, &s16, false, false, 0, 0);
        peer_drain(&rig, sub18, &s18, false, false, 0, 0);
        printf("  mixed: after root withdraw | d16sub found=%d gone=%d | "
               "d18sub found=%d gone=%d\n",
               s16.ns_found, s16.ns_gone, s18.ns_found, s18.ns_gone);
        R_CHECK(&rig, s18.ns_found == 1);   /* still exactly one FOUND ...   */
        R_CHECK(&rig, s18.ns_gone == 1);    /* ... matched by exactly one GONE */
        /* Suppression is SYMMETRIC: no GONE either, so the d16 peer is never
         * left holding half a pair. */
        R_CHECK(&rig, s16.ns_found == 0);
        R_CHECK(&rig, s16.ns_gone == 0);
    }

    /* 3. Health + unchanged non-root discovery: a d18 publisher's ordinary
     *    namespace must still reach BOTH subscribers. */
    moq_bytes_t nsa[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsa, .count = 2 };
    moq_announcement_t a18;
    R_CHECK(&rig, moq_session_publish_namespace(pub18->peer, &pc, rig.now,
                                                &a18) == MOQ_OK);
    rig_pump(&rig, 32);
    peer_drain(&rig, sub16, &s16, false, false, 0, 0);
    peer_drain(&rig, sub18, &s18, false, false, 0, 0);
    printf("  mixed: d18 non-root | d16sub found=%d | d18sub found=%d\n",
           s16.ns_found, s18.ns_found);
    /* Exact cumulative counts for BOTH profiles. The d16 root baseline is zero
     * because the gate suppressed it, so these totals count only the non-root
     * updates that peer is legitimately owed. */
    R_CHECK(&rig, s18.ns_found == 2);
    R_CHECK(&rig, s16.ns_found == s16_root_found + 1);

    /* 4. The inverse healthy arm: a d16 publisher's non-root namespace is
     *    visible to both empty-prefix subscribers. */
    moq_bytes_t nsb[2] = { B("live"), B("cam2") };
    pc.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 2 };
    moq_announcement_t a16;
    R_CHECK(&rig, moq_session_publish_namespace(pub16->peer, &pc, rig.now,
                                                &a16) == MOQ_OK);
    rig_pump(&rig, 32);
    peer_drain(&rig, pub16, &p16, false, false, 0, 0);
    peer_drain(&rig, sub16, &s16, false, false, 0, 0);
    peer_drain(&rig, sub18, &s18, false, false, 0, 0);
    printf("  mixed: d16 non-root | d16sub found=%d | d18sub found=%d\n",
           s16.ns_found, s18.ns_found);
    R_CHECK(&rig, p16.namespace_accepted);            /* d16 publisher OK    */
    R_CHECK(&rig, s18.ns_found == 3);
    R_CHECK(&rig, s16.ns_found == s16_root_found + 2);

    moqr_bind_stats_t st;
    moqr_bind_get_stats(rig.bind, &st);
    printf("  mixed: conns=%u session_errors=%llu\n", st.conns,
           (unsigned long long)st.session_errors);
    R_CHECK(&rig, st.conns == 4);              /* all four still live        */
    R_CHECK(&rig, st.session_errors == 0);     /* d16 session stayed healthy */

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    return rig.failures;
}

/* --- draft-18 Section 6.2 durability battery -------------------------------
 * A namespace update owed to a matching prefix subscriber is a MUST, so a
 * blocked control path may delay it but must never drop it, duplicate it,
 * reorder it, or let it escape the ring's byte bound.
 * blocked=false arms are the same-order healthy controls. */

/* Park an update, then keep it parked for several pumps before letting the
 * peer drain: repeated WOULD_BLOCK must stay ONE logical update. */
static int
nsu_case(moq_version_t version, bool blocked, bool with_gone, int stall_pumps)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = blocked ? 1u : 0u;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (nsu_case)\n");
        g_test_max_actions = saved;
        return 1;
    }
    g_test_max_actions = 0u;                 /* publisher: normal queue */
    conn_t *pub = rig_connect(&rig, version);
    g_test_max_actions = blocked ? 1u : 0u;  /* watcher: the blocked path */
    conn_t *watch = rig_connect(&rig, version);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);

    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    if (!blocked) {
        rig_pump(&rig, 4);
    }

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    /* Stall: pump WITHOUT draining the watcher so the retry re-blocks. */
    for (int i = 0; i < stall_pumps; i++) {
        rig_pump(&rig, 1);
    }
    if (with_gone) {
        /* The publisher's own depth-1 queue may still hold its announce; the
         * subject under test is the WATCHER's blocked path, so let the
         * publisher drain and retry its client-side call. */
        moq_result_t drc = MOQ_ERR_WOULD_BLOCK;
        for (int t = 0; t < 32 && drc == MOQ_ERR_WOULD_BLOCK; t++) {
            drc = moq_session_publish_namespace_done(pub->peer, ann, rig.now);
            if (drc == MOQ_ERR_WOULD_BLOCK) {
                /* Free the PUBLISHER's own depth-1 queue by consuming its
                 * events; the watcher is deliberately left undrained. */
                peer_state_t p_ps;
                memset(&p_ps, 0, sizeof(p_ps));
                peer_drain(&rig, pub, &p_ps, false, false, 0, 0);
                rig_pump(&rig, 2);
            }
        }
        R_CHECK(&rig, drc == MOQ_OK);
    }
    rig_pump(&rig, 64);

    peer_state_t w_ps;
    memset(&w_ps, 0, sizeof(w_ps));
    for (int d = 0; d < 6; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 32);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    printf("  nsu_case(d%d blocked=%d gone=%d stall=%d): found=%d gone=%d "
           "order=%s retained=%llu failed_closed=%llu\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, (int)blocked,
           (int)with_gone, stall_pumps, w_ps.ns_found, w_ps.ns_gone,
           w_ps.order, (unsigned long long)bs.nsu_retained,
           (unsigned long long)bs.nsu_failed_closed);

    /* Exactly once, never duplicated by the retries. */
    R_CHECK(&rig, w_ps.ns_found == 1);
    if (with_gone) {
        R_CHECK(&rig, w_ps.ns_gone == 1);
        /* GONE must never overtake or cancel the retained FOUND. */
        {
        const char *n = strchr(w_ps.order, 'N');
        const char *g = strchr(w_ps.order, 'G');
        R_CHECK(&rig, n != NULL && g != NULL && n < g);   /* FOUND before GONE */
    }
    }
    /* A recoverable block is not a session error. */
    R_CHECK(&rig, bs.session_errors == 0);
    R_CHECK(&rig, bs.nsu_failed_closed == 0);
    if (!blocked) {
        /* Healthy discovery allocates and parks nothing. */
        R_CHECK(&rig, bs.nsu_retained == 0);
    } else {
        /* Repeated WOULD_BLOCK stays ONE logical update. */
        R_CHECK(&rig, bs.nsu_retained >= 1u);
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);   /* every retained byte released */
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_case(d%d blocked=%d gone=%d stall=%d)\n",
               version == MOQ_VERSION_DRAFT_16 ? 16 : 18, (int)blocked,
               (int)with_gone, stall_pumps);
    }
    return f;
}

/* A multi-part suffix with binary and empty components must arrive exactly
 * once and byte-identically after being retained and replayed. */
static int
nsu_multipart_bytes_exact(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = 1u;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (nsu_multipart)\n");
        g_test_max_actions = saved;
        return 1;
    }
    g_test_max_actions = 0u;   /* publisher: normal queue */
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;   /* watcher: the blocked downstream path */
    conn_t *watch = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);

    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);

    /* Binary bytes including NUL and high bits, plus a zero-length part. */
    static const uint8_t bin[3] = { 0xFF, 0x00, 0x7F };
    moq_bytes_t nsp[3];
    nsp[0] = B("live");
    nsp[1].data = bin;
    nsp[1].len = sizeof(bin);
    nsp[2] = B("z");
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 3 };
    moq_announcement_t ann;
    moq_result_t arc = moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                     &ann);
    printf("  nsu_multipart announce rc=%d\n", (int)arc);
    R_CHECK(&rig, arc == MOQ_OK);
    rig_pump(&rig, 64);
    peer_state_t w_ps;
    memset(&w_ps, 0, sizeof(w_ps));
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    rig_pump(&rig, 32);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    printf("  nsu_multipart: found=%d parts=%zu lens=%zu,%zu total=%zu\n",
           w_ps.ns_found, w_ps.ns_seen_count, w_ps.ns_seen_lens[0],
           w_ps.ns_seen_lens[1], w_ps.ns_seen_total);
    R_CHECK(&rig, w_ps.ns_found == 1);
    R_CHECK(&rig, w_ps.ns_seen_count == 2);           /* suffix after "live" */
    R_CHECK(&rig, w_ps.ns_seen_lens[0] == sizeof(bin));
    R_CHECK(&rig, w_ps.ns_seen_lens[1] == 1);
    R_CHECK(&rig, w_ps.ns_seen_total == sizeof(bin) + 1);
    R_CHECK(&rig, memcmp(w_ps.ns_seen_bytes, bin, sizeof(bin)) == 0);
    R_CHECK(&rig, w_ps.ns_seen_bytes[sizeof(bin)] == 'z');

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_multipart_bytes_exact\n");
    }
    return f;
}

/* Closing the subscriber while an update is held must purge it: no leak, and
 * no stale handle reused by a later connection. */
static int
nsu_close_purges_held(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_test_max_actions = 1u;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (nsu_close_purges)\n");
        g_test_max_actions = saved;
        return 1;
    }
    g_test_max_actions = 0u;   /* publisher: normal queue */
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;   /* watcher: the blocked downstream path */
    conn_t *watch = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);

    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 2);   /* the update is now held, undelivered */

    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);
    R_CHECK(&rig, held.nsu_retained >= 1);

    /* Tear the subscriber down while the update is still held. */
    R_CHECK(&rig, moqr_bind_conn_close(rig.bind, watch->rsess) == MOQR_OK);
    rig_pump(&rig, 32);

    moqr_bind_stats_t after;
    moqr_bind_get_stats(rig.bind, &after);
    printf("  nsu_close_purges: retained=%llu failed_closed=%llu "
           "session_errors=%llu\n",
           (unsigned long long)after.nsu_retained,
           (unsigned long long)after.nsu_failed_closed,
           (unsigned long long)after.session_errors);
    R_CHECK(&rig, after.nsu_failed_closed == 0);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);   /* purge released the held bytes */
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_close_purges_held\n");
    }
    return f;
}

/* Blocked NS_GONE, deterministic: the watcher's SimPair is simply not stepped
 * while the publisher and the real bind pump keep running, so the relay's
 * NAMESPACE_DONE write genuinely meets WOULD_BLOCK and must be retained.
 * draft-18 Section 6.2 owes the withdrawal exactly as it owes the arrival. */
static int
nsu_gone_survives_block(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (nsu_gone_survives_block)\n");
        g_test_max_actions = saved;
        return 1;
    }
    g_test_max_actions = 0u;   /* publisher: never throttled */
    conn_t *pub = rig_connect(&rig, version);
    g_test_max_actions = 1u;   /* watcher: single-slot outbound edge */
    conn_t *watch = rig_connect(&rig, version);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);

    /* -- 1. establish and drain phase 1 completely ------------------------ */
    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    rig_pump(&rig, 8);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 16);

    /* ONE cumulative watcher ledger for the whole test: the phase-1 FOUND is
     * never erased, so the final assertion is a true cumulative order. */
    peer_state_t pub_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.namespace_accepted);
    R_CHECK(&rig, w_ps.ns_found == 1);
    R_CHECK(&rig, w_ps.ns_gone == 0);

    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_retained == 0);   /* healthy phase 1 parked nothing */

    /* -- 2/3. queue exactly one relay->watcher response, then stop stepping
     *         the watcher so that response can never drain. --------------- */
    moq_track_status_cfg_t tcfg;
    memset(&tcfg, 0, sizeof(tcfg));
    moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    tcfg.track_name = B("video");
    moq_track_status_handle_t tsh;
    R_CHECK(&rig, moq_session_track_status(watch->peer, &tcfg, rig.now,
                                           &tsh) == MOQ_OK);
    rig_pump(&rig, 1);   /* deliver the request; the answer now occupies the slot */

    /* -- 4. withdraw through the publisher while the watcher stays frozen -- */
    R_CHECK(&rig, moq_session_publish_namespace_done(pub->peer, ann,
                                                     rig.now) == MOQ_OK);
    rig_pump_except(&rig, watch, 8);

    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);
    peer_state_t frozen;
    memset(&frozen, 0, sizeof(frozen));
    peer_drain(&rig, watch, &frozen, false, false, 0, 0);
    printf("  nsu_gone(d%d): held retained=%llu (base %llu) gone_seen=%d\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18,
           (unsigned long long)held.nsu_retained,
           (unsigned long long)base.nsu_retained, frozen.ns_gone);

    /* -- 5. exactly one retained update, and no GONE observed yet --------- */
    R_CHECK(&rig, held.nsu_retained == base.nsu_retained + 1u);
    R_CHECK(&rig, frozen.ns_gone == 0);
    R_CHECK(&rig, held.nsu_failed_closed == 0);
    R_CHECK(&rig, held.session_errors == 0);

    /* Keep pumping publisher+bind with the watcher still frozen: the retry
     * must re-block, never duplicate the held update. */
    rig_pump_except(&rig, watch, 24);
    moqr_bind_stats_t still;
    moqr_bind_get_stats(rig.bind, &still);
    R_CHECK(&rig, still.nsu_retained == base.nsu_retained + 1u);
    R_CHECK(&rig, still.nsu_failed_closed == 0);

    /* -- 6. resume the watcher: one FOUND then one GONE, exactly once ----- */
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    printf("  nsu_gone(d%d): cumulative found=%d gone=%d order=%s\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, w_ps.ns_found,
           w_ps.ns_gone, w_ps.order);
    R_CHECK(&rig, w_ps.ns_found == 1);
    R_CHECK(&rig, w_ps.ns_gone == 1);
    {
        const char *n = strchr(w_ps.order, 'N');
        const char *g = strchr(w_ps.order, 'G');
        R_CHECK(&rig, n != NULL && g != NULL && n < g);   /* FOUND before GONE */
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_gone_survives_block(d%d)\n",
               version == MOQ_VERSION_DRAFT_16 ? 16 : 18);
    }
    return f;
}

/* A zero-part suffix: the watcher subscribes to the EXACT namespace, so the
 * update the relay owes carries no tuple fields and no bytes at all. This is
 * what makes the sidecar's explicit occupancy bit load-bearing -- a live slot
 * whose byte pointer is legitimately NULL. */
static int
nsu_zero_suffix_retained(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (nsu_zero_suffix)\n");
        g_test_max_actions = saved;
        return 1;
    }
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, version);
    g_test_max_actions = 1u;
    conn_t *watch = rig_connect(&rig, version);
    R_CHECK(&rig, pub && watch);
    rig_pump(&rig, 4);

    /* Prefix == the whole namespace, so the suffix is empty. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = nsp, .count = 2 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    rig_pump(&rig, 8);
    peer_state_t pub_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    for (int d = 0; d < 4; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 4);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);

    /* Occupy the watcher's single outbound slot, then freeze its pair. */
    moq_track_status_cfg_t tcfg;
    memset(&tcfg, 0, sizeof(tcfg));
    moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    tcfg.track_name = B("video");
    moq_track_status_handle_t tsh;
    R_CHECK(&rig, moq_session_track_status(watch->peer, &tcfg, rig.now,
                                           &tsh) == MOQ_OK);
    rig_pump(&rig, 1);

    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump_except(&rig, watch, 8);

    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);
    peer_state_t frozen;
    memset(&frozen, 0, sizeof(frozen));
    peer_drain(&rig, watch, &frozen, false, false, 0, 0);
    printf("  nsu_zero_suffix(d%d): held retained=%llu (base %llu) found=%d\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18,
           (unsigned long long)held.nsu_retained,
           (unsigned long long)base.nsu_retained, frozen.ns_found);
    /* Retained with an occupied slot even though it owns zero bytes. */
    R_CHECK(&rig, held.nsu_retained == base.nsu_retained + 1u);
    /* Occupancy is tracked even though this payload owns no bytes at all. */
    R_CHECK(&rig, held.nsu_live == 1);
    R_CHECK(&rig, frozen.ns_found == 0);
    R_CHECK(&rig, held.nsu_failed_closed == 0);
    R_CHECK(&rig, held.session_errors == 0);

    /* Repeated block must not duplicate the zero-byte payload. */
    rig_pump_except(&rig, watch, 24);
    moqr_bind_stats_t still;
    moqr_bind_get_stats(rig.bind, &still);
    R_CHECK(&rig, still.nsu_retained == base.nsu_retained + 1u);
    R_CHECK(&rig, still.nsu_failed_closed == 0);

    /* Reopen: delivered exactly once, and the suffix really is empty. */
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    printf("  nsu_zero_suffix(d%d): found=%d parts=%zu total=%zu\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, w_ps.ns_found,
           w_ps.ns_seen_count, w_ps.ns_seen_total);
    R_CHECK(&rig, w_ps.ns_found == 1);
    R_CHECK(&rig, w_ps.ns_seen_count == 0);
    R_CHECK(&rig, w_ps.ns_seen_total == 0);
    /* Release decremented the live-slot ledger: a zero-byte payload is
     * reclaimed exactly like a byte-owning one. */
    moqr_bind_stats_t done;
    moqr_bind_get_stats(rig.bind, &done);
    printf("  nsu_zero_suffix(d%d): live_after=%u\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, done.nsu_live);
    R_CHECK(&rig, done.nsu_live == 0);

    /* The WITHDRAWAL takes the same blocked-retained-replayed path, so the
     * empty-suffix update is proven for GONE as well as FOUND. Occupy the
     * watcher's slot again, freeze it, withdraw, then release. */
    {
        moq_track_status_cfg_t tcfg2;
        memset(&tcfg2, 0, sizeof(tcfg2));
        moq_track_status_cfg_init(&tcfg2);
        tcfg2.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        tcfg2.track_name = B("again");
        moq_track_status_handle_t th2;
        (void)moq_session_track_status(watch->peer, &tcfg2, rig.now, &th2);
        rig_pump(&rig, 1);
        watch->frozen = true;
        R_CHECK(&rig, moq_session_publish_namespace_done(pub->peer, ann,
                                                         rig.now) == MOQ_OK);
        rig_pump_except(&rig, watch, 8);
        moqr_bind_stats_t gheld;
        moqr_bind_get_stats(rig.bind, &gheld);
        R_CHECK(&rig, gheld.nsu_retained == done.nsu_retained + 1u);
        R_CHECK(&rig, gheld.nsu_live == 1);        /* the GONE is held        */
        R_CHECK(&rig, gheld.session_errors == 0);
        watch->frozen = false;
        for (int d = 0; d < 10; d++) {
            peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
            rig_pump(&rig, 8);
        }
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        moqr_bind_stats_t gdone;
        moqr_bind_get_stats(rig.bind, &gdone);
        printf("  nsu_zero_suffix(d%d): gone=%d found=%d live_after=%u\n",
               version == MOQ_VERSION_DRAFT_16 ? 16 : 18, w_ps.ns_gone,
               w_ps.ns_found, gdone.nsu_live);
        R_CHECK(&rig, w_ps.ns_gone == 1);   /* delivered exactly once, in order */
        R_CHECK(&rig, w_ps.ns_found == 1);  /* and no extra FOUND appeared      */
        R_CHECK(&rig, gdone.nsu_live == 0); /* owed accounting drains to zero   */
        R_CHECK(&rig, gdone.nsu_failed_closed == 0);
        R_CHECK(&rig, gdone.session_errors == 0);
    }

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);   /* release decremented the live ledger */
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_zero_suffix_retained(d%d)\n",
               version == MOQ_VERSION_DRAFT_16 ? 16 : 18);
    }
    return f;
}

/* A hard failure on a ring-HEAD retry detaches its own connection, and detach
 * purges + compacts the ordered ring underneath the caller. If the caller then
 * pops "the head" it discards whatever entry compaction moved there -- another
 * connection's retained namespace update. The purge-epoch stamp is what makes
 * the caller notice the slot it was retrying is gone.
 *
 * A = the subscriber whose parked SUB_DONE hard-fails on retry.
 * B = an independent watcher whose retained NAMESPACE sits behind it. */
static int
nsu_purge_during_ring_retry(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_rv_lease_sub = 3000;
    g_rv_ann_always_allow = true;   /* revoke the SUBSCRIBE only */
    g_test_authorize = rv_hook;
    g_test_server_streaming_objects = true;
    g_test_client_streaming_objects = true;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_server_streaming_objects = false;
        g_test_client_streaming_objects = false;
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (nsu_purge_during_ring_retry)\n");
        return 1;
    }
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;                 /* A: single-slot outbound edge */
    conn_t *sub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_server_streaming_objects = false;
    g_test_client_streaming_objects = false;
    conn_t *watch = rig_connect(&rig, MOQ_VERSION_DRAFT_18);   /* B */
    R_CHECK(&rig, pub && sub && watch);
    peer_state_t pub_ps, sub_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&sub_ps, 0, sizeof(sub_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    rig_pump(&rig, 4);

    /* A subscribes under a revalidation lease; B watches the prefix. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    reval_make_subscribe_grant(&rig, pub, sub, &pub_ps, &sub_ps, nsp);
    R_CHECK(&rig, !sub_ps.done_seen);

    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    rig_pump(&rig, 8);
    for (int d = 0; d < 4; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
        rig_pump(&rig, 4);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);

    /* A begins an object so its SUB_DONE must RESET a begun subgroup. */
    moq_subgroup_cfg_t sca;
    moq_subgroup_cfg_init(&sca);
    sca.group_id = 1;
    sca.subgroup_id = 0;
    moq_subgroup_handle_t sga;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sca,
                                            rig.now, &sga) == MOQ_OK);
    R_CHECK(&rig, moq_session_begin_object(pub->peer, sga, 0, 96, rig.now) ==
                      MOQ_OK);
    {
        uint8_t body[32];
        memset(body, 0xC5, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig,
                moq_session_write_object_data(pub->peer, sga, pl, rig.now) ==
                    MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    rig_pump(&rig, 8);
    peer_drain(&rig, sub, &sub_ps, false, false, 0, 0);
    R_CHECK(&rig, sub_ps.chunks >= 1);

    /* The subscribe-grant setup already announced live/cam1, so drain the
     * watcher until that first NAMESPACE is delivered and nothing is held;
     * B's blocked update below must be a NEW one. */
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_live == 0);

    /* Occupy both subscriber edges, then freeze them: from here the relay's
     * writes to A and B genuinely block, so both entries park in the ring. */
    for (int i = 0; i < 2; i++) {
        conn_t *c = (i == 0) ? sub : watch;
        moq_track_status_cfg_t tcfg;
        memset(&tcfg, 0, sizeof(tcfg));
        moq_track_status_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        tcfg.track_name = B("video");
        moq_track_status_handle_t th;
        (void)moq_session_track_status(c->peer, &tcfg, rig.now, &th);
    }
    rig_pump(&rig, 1);
    sub->frozen = true;
    watch->frozen = true;

    /* A's SUB_DONE (revoke) parks FIRST, at the ring head... */
    g_rv_decision = MOQR_AUTH_DENY;
    g_rv_deny_code = 0x7;
    rig_pump(&rig, 2);
    /* ...then B's NAMESPACE parks behind it. */
    moq_bytes_t nsp2[2] = { B("live"), B("cam2") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 4);

    moqr_bind_stats_t parked;
    moqr_bind_get_stats(rig.bind, &parked);
    printf("  purge_retry: parked retained=%llu (base %llu) live=%u\n",
           (unsigned long long)parked.nsu_retained,
           (unsigned long long)base.nsu_retained, parked.nsu_live);
    /* Non-vacuity: B's update really is held before A's retry runs. */
    R_CHECK(&rig, parked.nsu_retained == base.nsu_retained + 1u);
    R_CHECK(&rig, parked.nsu_live == 1);

    /* A's NEXT ring-head retry hard-fails inside the real reset call. */
    moqr_bind_debug_fail_sg_reset(1);
    rig_pump(&rig, 8);

    moqr_bind_stats_t after;
    moqr_bind_get_stats(rig.bind, &after);
    printf("  purge_retry: after A-fail live=%u sess_err=%llu "
           "conns=%u\n",
           after.nsu_live, (unsigned long long)after.session_errors,
           after.conns);
    /* A was closed and detached by the production fail-closed path... */
    R_CHECK(&rig, after.session_errors >= 1);
    R_CHECK(&rig, after.conns < parked.conns);
    /* ...and B's retained update SURVIVED the purge+compaction. */
    R_CHECK(&rig, after.nsu_live == 1);

    /* Reopen B: its namespace arrives exactly once. */
    watch->frozen = false;
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    printf("  purge_retry: B found=%d order=%s\n", w_ps.ns_found, w_ps.order);
    /* One NAMESPACE from setup, plus the one that survived the purge. */
    R_CHECK(&rig, w_ps.ns_found == 2);

    moqr_bind_stats_t fin;
    moqr_bind_get_stats(rig.bind, &fin);
    R_CHECK(&rig, fin.nsu_live == 0);   /* queue drained cleanly */

    sub->frozen = false;
    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);         /* no leak */
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_purge_during_ring_retry\n");
    }
    return f;
}

/* Ordered cross-kind FIFO: namespace updates and scalar terminals share ONE
 * ordered ring, so output queued for a connection must leave in the order it
 * was produced regardless of kind. The watcher's pair is frozen, so every
 * output below is genuinely parked rather than raced through.
 *
 * ns_first=true : namespace A -> scalar B -> namespace C
 * ns_first=false: scalar A -> namespace B -> scalar C
 *
 * The scalar-first arm needs two INDEPENDENT scalar terminals produced while
 * the watcher edge is frozen. Revoking one subscription at a time (the test
 * hook keys its DENY on the requested track name) yields exactly that: one
 * SUBSCRIBE_DONE per revocation, around a publisher-driven NAMESPACE.
 * The recorded arrival string is the proof: T=TRACK_STATUS_OK, N=NAMESPACE,
 * D=SUBSCRIBE_DONE. */
static int
nsu_cross_kind_order(bool ns_first)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_rv_lease_sub = 3000;
    g_rv_ann_always_allow = true;
    g_rv_second_track = !ns_first;   /* two revocable subscriptions */
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (nsu_cross_kind_order)\n");
        return 1;
    }
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;   /* the watcher: single-slot outbound edge */
    conn_t *w = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && w);
    peer_state_t pub_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    rig_pump(&rig, 4);

    /* The watcher both subscribes to a track (so a revoke yields a scalar
     * SUBSCRIBE_DONE) and watches the prefix (so an announce yields a
     * NAMESPACE). */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    reval_make_subscribe_grant(&rig, pub, w, &pub_ps, &w_ps, nsp);
    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(w->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, w, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, w, &w_ps, false, false, 0, 0);

    /* Everything above is delivered; the order string starts fresh here. */
    memset(&w_ps, 0, sizeof(w_ps));
    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_live == 0);

    /* Occupy the single slot, then freeze: from here nothing drains. */
    moq_track_status_cfg_t tcfg;
    memset(&tcfg, 0, sizeof(tcfg));
    moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    tcfg.track_name = B("video");
    moq_track_status_handle_t th1;
    R_CHECK(&rig, moq_session_track_status(w->peer, &tcfg, rig.now, &th1) ==
                      MOQ_OK);
    rig_pump(&rig, 1);
    w->frozen = true;

    moq_bytes_t ns2[2] = { B("live"), B("cam2") };
    moq_bytes_t ns3[2] = { B("live"), B("cam3") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    moq_announcement_t a2, a3;

    if (!ns_first) {
        /* A = scalar: revoke ONLY the "video" subscription. Revocation lands
         * on the next revalidation tick, so pump past the lease BEFORE the
         * namespace is produced, or B would be queued first. */
        g_rv_deny_code = 0x7;
        g_rv_deny_track = "video";
        rig_pump_except(&rig, w, 12);
        /* Non-vacuity for THIS arm: scalar A must already occupy the ordered
         * ring before the namespace is produced, and no namespace may have
         * been retained yet. */
        moqr_bind_stats_t after_a;
        moqr_bind_get_stats(rig.bind, &after_a);
        printf("    [A scalar] pending_hw=%u retained=%llu\n",
               after_a.pending_high_water,
               (unsigned long long)after_a.nsu_retained);
        R_CHECK(&rig, after_a.pending_high_water >= 1);
        /* B = namespace. */
        pc.track_namespace = (moq_namespace_t){ .parts = ns2, .count = 2 };
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                    &a2) == MOQ_OK);
        rig_pump_except(&rig, w, 6);
        /* B really did join behind A rather than replacing it. */
        {
            moqr_bind_stats_t after_b;
            moqr_bind_get_stats(rig.bind, &after_b);
            R_CHECK(&rig, after_b.nsu_retained > after_a.nsu_retained);
        }
        /* C = scalar: now revoke ONLY the "audio" subscription. */
        g_rv_deny_track = "audio";
        rig_pump_except(&rig, w, 12);
    } else {
        /* A = namespace, then B = scalar (revoke), then C = namespace. */
        pc.track_namespace = (moq_namespace_t){ .parts = ns2, .count = 2 };
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                    &a2) == MOQ_OK);
        rig_pump_except(&rig, w, 2);
        g_rv_decision = MOQR_AUTH_DENY;
        g_rv_deny_code = 0x7;
        rig_pump_except(&rig, w, 4);
        g_rv_decision = MOQR_AUTH_ALLOW;
        pc.track_namespace = (moq_namespace_t){ .parts = ns3, .count = 2 };
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                    &a3) == MOQ_OK);
        rig_pump_except(&rig, w, 4);
    }

    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);
    peer_state_t frozen;
    memset(&frozen, 0, sizeof(frozen));
    peer_drain(&rig, w, &frozen, false, false, 0, 0);
    printf("  cross_kind(ns_first=%d): held live=%u retained=%llu frozen_order=%s\n",
           (int)ns_first, held.nsu_live,
           (unsigned long long)held.nsu_retained, frozen.order);
    /* Non-vacuity: the namespace part really is retained, and nothing was
     * attempted inline past the block. */
    R_CHECK(&rig, held.nsu_retained > base.nsu_retained);
    R_CHECK(&rig, frozen.order_len == 0);

    /* Reopen: the arrival order is the proof. */
    w->frozen = false;
    for (int d = 0; d < 10; d++) {
        peer_drain(&rig, w, &w_ps, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, w, &w_ps, false, false, 0, 0);
    printf("  cross_kind(ns_first=%d): order=%s\n", (int)ns_first, w_ps.order);
    /* ns_first=0: T(occupier) E(scalar A) N(namespace B) D(scalar C)
     * ns_first=1: T(occupier) N(namespace A) D(scalar B) N(namespace C)
     * Scalar A is a TRACK_STATUS_ERROR (the track is unannounced) -- still a
     * scalar terminal on the same ordered ring, which is what matters here. */
    /* ns_first=1: T(occupier) N(namespace A) D(scalar B) N(namespace C)
     * ns_first=0: T(occupier) D(scalar A) N(namespace B) D(scalar C) */
    R_CHECK(&rig, strcmp(w_ps.order, ns_first ? "TNDN" : "TDND") == 0);

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_cross_kind_order(ns_first=%d)\n", (int)ns_first);
    }
    return f;
}

/* Held-state exhaustion must fail CLOSED and stay isolated. Two watchers hold
 * the same prefix, so one announce owes a NAMESPACE to both; the suffix copy
 * for whichever is served first is made to fail its allocation. That watcher
 * must be closed and detached, while the other's retained update survives and
 * is delivered exactly once. */
static int
nsu_exhaustion_fail_closed(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = saved;
        printf("FAIL: rig create (nsu_exhaustion_fail_closed)\n");
        return 1;
    }
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;
    conn_t *w1 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *w2 = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && w1 && w2);
    peer_state_t p1, p2;
    memset(&p1, 0, sizeof(p1));
    memset(&p2, 0, sizeof(p2));
    rig_pump(&rig, 4);

    moq_bytes_t pfx[1] = { B("live") };
    for (int i = 0; i < 2; i++) {
        conn_t *w = (i == 0) ? w1 : w2;
        moq_subscribe_namespace_cfg_t nscfg;
        memset(&nscfg, 0, sizeof(nscfg));
        moq_subscribe_namespace_cfg_init(&nscfg);
        nscfg.track_namespace_prefix =
            (moq_namespace_t){ .parts = pfx, .count = 1 };
        nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t nsh;
        R_CHECK(&rig, moq_session_subscribe_namespace(w->peer, &nscfg, rig.now,
                                                      &nsh) == MOQ_OK);
    }
    for (int d = 0; d < 6; d++) {
        peer_drain(&rig, w1, &p1, false, false, 0, 0);
        peer_drain(&rig, w2, &p2, false, false, 0, 0);
        rig_pump(&rig, 6);
    }
    peer_drain(&rig, w1, &p1, false, false, 0, 0);
    peer_drain(&rig, w2, &p2, false, false, 0, 0);

    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_live == 0);
    R_CHECK(&rig, base.nsu_failed_closed == 0);
    uint32_t conns0 = base.conns;

    /* Occupy both edges and freeze, so the announce below must be retained. */
    moq_bytes_t ns2[2] = { B("live"), B("cam2") };
    for (int i = 0; i < 2; i++) {
        conn_t *w = (i == 0) ? w1 : w2;
        moq_track_status_cfg_t tcfg;
        memset(&tcfg, 0, sizeof(tcfg));
        moq_track_status_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ .parts = ns2, .count = 2 };
        tcfg.track_name = B("video");
        moq_track_status_handle_t th;
        (void)moq_session_track_status(w->peer, &tcfg, rig.now, &th);
    }
    rig_pump(&rig, 1);
    w1->frozen = true;
    w2->frozen = true;

    /* Fail the next retention exactly once, so ONE watcher's update cannot be
     * held. Injected at the ownership boundary rather than by allocation size,
     * because the announce path itself allocates same-sized blocks. */
    moqr_bind_debug_fail_nsu_store(1);

    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = ns2, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &ann) == MOQ_OK);
    /* Both watchers are frozen, so rig_pump alone leaves their edges shut:
     * the survivor's update must still be HELD when we look. */
    rig_pump(&rig, 8);

    moqr_bind_stats_t hit;
    moqr_bind_get_stats(rig.bind, &hit);
    printf("  exhaustion: failed_closed=%llu ordered_failed=%llu live=%u "
           "conns %u->%u\n",
           (unsigned long long)hit.nsu_failed_closed,
           (unsigned long long)hit.ordered_failed_closed, hit.nsu_live,
           conns0, hit.conns);
    /* Fail closed, not drop-and-continue: the affected conn is gone. */
    R_CHECK(&rig, hit.nsu_failed_closed >= 1);
    R_CHECK(&rig, hit.conns < conns0);
    /* Isolation: the OTHER watcher's update is still owned. */
    R_CHECK(&rig, hit.nsu_live == 1);

    /* And it delivers exactly once when its edge reopens. */
    w1->frozen = false;
    w2->frozen = false;
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, w1, &p1, false, false, 0, 0);
        peer_drain(&rig, w2, &p2, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, w1, &p1, false, false, 0, 0);
    peer_drain(&rig, w2, &p2, false, false, 0, 0);
    printf("  exhaustion: survivor found w1=%d w2=%d\n", p1.ns_found,
           p2.ns_found);
    R_CHECK(&rig, p1.ns_found + p2.ns_found == 1);

    moqr_bind_stats_t fin;
    moqr_bind_get_stats(rig.bind, &fin);
    R_CHECK(&rig, fin.nsu_live == 0);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);   /* every allocation returned */
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_exhaustion_fail_closed\n");
    }
    return f;
}

/* The ordered ring is SHARED by namespace updates and scalar terminals, so a
 * full ring must fail closed on EITHER insertion kind, and must take down only
 * the connection whose output could not be queued. The injection drives the
 * same bind_ordered_room() predicate production uses, so an injected full and
 * a genuinely full ring cannot diverge.
 *
 * ns_insert=true : the refused insertion is a NAMESPACE update.
 * ns_insert=false: the refused insertion is a scalar terminal. */
static int
nsu_ring_full_fail_closed(bool ns_insert)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_rv_lease_sub = 3000;
    g_rv_ann_always_allow = true;
    g_rv_second_track = !ns_insert;
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (nsu_ring_full_fail_closed)\n");
        return 1;
    }
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;
    conn_t *victim = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *other = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && victim && other);
    peer_state_t pub_ps, vp, op;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&vp, 0, sizeof(vp));
    memset(&op, 0, sizeof(op));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_bytes_t pfx[1] = { B("live") };
    /* The victim also holds subscriptions, so a revoke gives a scalar. */
    reval_make_subscribe_grant(&rig, pub, victim, &pub_ps, &vp, nsp);
    for (int i = 0; i < 2; i++) {
        conn_t *w = (i == 0) ? victim : other;
        moq_subscribe_namespace_cfg_t nscfg;
        memset(&nscfg, 0, sizeof(nscfg));
        moq_subscribe_namespace_cfg_init(&nscfg);
        nscfg.track_namespace_prefix =
            (moq_namespace_t){ .parts = pfx, .count = 1 };
        nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t nsh;
        R_CHECK(&rig, moq_session_subscribe_namespace(w->peer, &nscfg, rig.now,
                                                      &nsh) == MOQ_OK);
    }
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, victim, &vp, false, false, 0, 0);
        peer_drain(&rig, other, &op, false, false, 0, 0);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        rig_pump(&rig, 6);
    }
    peer_drain(&rig, victim, &vp, false, false, 0, 0);
    peer_drain(&rig, other, &op, false, false, 0, 0);

    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_live == 0);
    uint32_t conns0 = base.conns;

    /* Occupy both watcher edges and freeze them. */
    moq_bytes_t ns2[2] = { B("live"), B("cam2") };
    for (int i = 0; i < 2; i++) {
        conn_t *w = (i == 0) ? victim : other;
        moq_track_status_cfg_t tcfg;
        memset(&tcfg, 0, sizeof(tcfg));
        moq_track_status_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        tcfg.track_name = B("video");
        moq_track_status_handle_t th;
        (void)moq_session_track_status(w->peer, &tcfg, rig.now, &th);
    }
    rig_pump(&rig, 1);
    victim->frozen = true;
    other->frozen = true;

    /* Give `other` a genuinely HELD namespace first: it must survive. */
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = ns2, .count = 2 };
    moq_announcement_t a2;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &a2) == MOQ_OK);
    rig_pump(&rig, 6);
    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);
    R_CHECK(&rig, held.nsu_live >= 1);   /* non-vacuity: something IS held */

    /* Now refuse exactly one insertion of the kind under test. */
    moqr_bind_debug_ring_full(1, ns_insert);
    if (ns_insert) {
        moq_bytes_t ns3[2] = { B("live"), B("cam3") };
        pc.track_namespace = (moq_namespace_t){ .parts = ns3, .count = 2 };
        moq_announcement_t a3;
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                    &a3) == MOQ_OK);
    } else {
        g_rv_deny_track = "video";
        g_rv_deny_code = 0x7;
    }
    rig_pump(&rig, 12);

    moqr_bind_stats_t hit;
    moqr_bind_get_stats(rig.bind, &hit);
    printf("  ring_full(ns=%d): nsu_fc=%llu ord_fc=%llu live=%u conns %u->%u\n",
           (int)ns_insert, (unsigned long long)hit.nsu_failed_closed,
           (unsigned long long)hit.ordered_failed_closed, hit.nsu_live,
           conns0, hit.conns);
    /* Fail closed: the refused connection is gone, in the RIGHT counter
     * domain (namespace failures never counted as generic, or vice versa). */
    R_CHECK(&rig, hit.conns < conns0);
    if (ns_insert) {
        R_CHECK(&rig, hit.nsu_failed_closed >= 1);
    } else {
        R_CHECK(&rig, hit.ordered_failed_closed >= 1);
    }
    /* Isolation: the other connection's held entry was NOT disturbed. */
    R_CHECK(&rig, hit.nsu_live >= 1);

    /* Reopen: the survivor still delivers. */
    victim->frozen = false;
    other->frozen = false;
    for (int d = 0; d < 10; d++) {
        peer_drain(&rig, victim, &vp, false, false, 0, 0);
        peer_drain(&rig, other, &op, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, other, &op, false, false, 0, 0);
    printf("  ring_full(ns=%d): survivor other found=%d\n", (int)ns_insert,
           op.ns_found);
    R_CHECK(&rig, op.ns_found >= 1);

    moqr_bind_stats_t fin;
    moqr_bind_get_stats(rig.bind, &fin);
    R_CHECK(&rig, fin.nsu_live == 0);   /* queue drained, no sidecar stranded */

    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);         /* teardown returns every allocation */
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_ring_full_fail_closed(ns_insert=%d)\n",
               (int)ns_insert);
    }
    return f;
}

/* One insertion, ONE admission decision. A namespace update admitted as a
 * namespace must not additionally be admitted as a scalar: a second, scalar-
 * classified room check would let a namespace insertion answer to a refusal
 * aimed at scalars, and would leave a live sidecar with no FIFO entry behind it
 * -- a dropped mandatory update on a session that stays open.
 *
 * A scalar-targeted refusal is armed while a namespace update is the very next
 * thing to park. The namespace must park intact and keep the refusal armed for
 * the scalar that follows. */
static int
nsu_ns_insert_keeps_scalar_full(void)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    g_rv_lease_sub = 3000;
    g_rv_ann_always_allow = true;
    g_rv_second_track = true;    /* two tracks: revoking one gives a scalar */
    g_test_authorize = rv_hook;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        rv_reset();
        g_test_max_actions = saved;
        printf("FAIL: rig create (nsu_ns_insert_keeps_scalar_full)\n");
        return 1;
    }
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    g_test_max_actions = 1u;
    conn_t *watcher = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *victim = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && watcher && victim);
    peer_state_t pub_ps, wp, vp;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&wp, 0, sizeof(wp));
    memset(&vp, 0, sizeof(vp));
    rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_bytes_t pfx[1] = { B("live") };
    /* The victim carries the subscriptions whose revoke yields the scalar. */
    reval_make_subscribe_grant(&rig, pub, victim, &pub_ps, &vp, nsp);
    /* Only the watcher owes NAMESPACE updates. */
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watcher->peer, &nscfg,
                                                  rig.now, &nsh) == MOQ_OK);
    for (int d = 0; d < 8; d++) {
        peer_drain(&rig, watcher, &wp, false, false, 0, 0);
        peer_drain(&rig, victim, &vp, false, false, 0, 0);
        peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
        rig_pump(&rig, 6);
    }
    peer_drain(&rig, watcher, &wp, false, false, 0, 0);
    peer_drain(&rig, victim, &vp, false, false, 0, 0);

    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_live == 0);
    uint32_t conns0 = base.conns;
    int found0 = wp.ns_found;

    /* Occupy both edges and freeze them, so the next output of either kind
     * must join the ordered ring rather than go out inline. */
    for (int i = 0; i < 2; i++) {
        conn_t *w = (i == 0) ? watcher : victim;
        moq_track_status_cfg_t tcfg;
        memset(&tcfg, 0, sizeof(tcfg));
        moq_track_status_cfg_init(&tcfg);
        tcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        tcfg.track_name = B("video");
        moq_track_status_handle_t th;
        (void)moq_session_track_status(w->peer, &tcfg, rig.now, &th);
    }
    rig_pump(&rig, 1);
    watcher->frozen = true;
    victim->frozen = true;

    /* Arm a SCALAR-targeted refusal, then park a NAMESPACE update. */
    moqr_bind_debug_ring_full(1, false);
    moq_bytes_t ns2[2] = { B("live"), B("cam2") };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = ns2, .count = 2 };
    moq_announcement_t a2;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &a2) == MOQ_OK);
    rig_pump(&rig, 6);

    moqr_bind_stats_t held;
    moqr_bind_get_stats(rig.bind, &held);
    printf("  ns_keeps_scalar: held retained=%llu live=%u nsu_fc=%llu "
           "ord_fc=%llu conns %u->%u\n",
           (unsigned long long)(held.nsu_retained - base.nsu_retained),
           held.nsu_live, (unsigned long long)held.nsu_failed_closed,
           (unsigned long long)held.ordered_failed_closed, conns0, held.conns);
    /* The namespace parked as a namespace: one held update, nothing failed
     * closed, and the connection it targets is untouched. */
    R_CHECK(&rig, held.nsu_retained == base.nsu_retained + 1u);
    R_CHECK(&rig, held.nsu_live == 1u);
    R_CHECK(&rig, held.nsu_failed_closed == base.nsu_failed_closed);
    R_CHECK(&rig, held.ordered_failed_closed == base.ordered_failed_closed);
    R_CHECK(&rig, held.conns == conns0);

    /* The scalar refusal must STILL be armed: the next scalar terminal is the
     * one that consumes it, on the scalar fail-closed path. */
    g_rv_deny_track = "video";
    g_rv_deny_code = 0x7;
    rig_pump(&rig, 12);

    moqr_bind_stats_t hit;
    moqr_bind_get_stats(rig.bind, &hit);
    printf("  ns_keeps_scalar: scalar ord_fc=%llu nsu_fc=%llu live=%u "
           "conns %u->%u\n",
           (unsigned long long)hit.ordered_failed_closed,
           (unsigned long long)hit.nsu_failed_closed, hit.nsu_live,
           conns0, hit.conns);
    R_CHECK(&rig, hit.ordered_failed_closed >= base.ordered_failed_closed + 1u);
    R_CHECK(&rig, hit.nsu_failed_closed == base.nsu_failed_closed);
    R_CHECK(&rig, hit.conns < conns0);        /* the VICTIM closed, not both */
    R_CHECK(&rig, hit.nsu_live == 1u);        /* the held update is untouched */

    /* The watcher stayed live and still owns exactly one queued update. */
    watcher->frozen = false;
    for (int d = 0; d < 10; d++) {
        peer_drain(&rig, watcher, &wp, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, watcher, &wp, false, false, 0, 0);
    printf("  ns_keeps_scalar: watcher found %d->%d\n", found0, wp.ns_found);
    R_CHECK(&rig, wp.ns_found == found0 + 1);   /* delivered exactly once */

    moqr_bind_stats_t fin;
    moqr_bind_get_stats(rig.bind, &fin);
    R_CHECK(&rig, fin.nsu_live == 0);   /* no sidecar stranded off the FIFO */

    moqr_bind_debug_ring_full(0, false);
    rig_destroy(&rig);
    rv_reset();
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_ns_insert_keeps_scalar_full\n");
    }
    return f;
}

/* Operation-specific WOULD_BLOCK: byte capacity, not a connection-wide fact.
 *
 * `send_namespace` stack-encodes into MOQ_FULL_TRACK_NAME_MAX+128 and treats a
 * send-arena shortfall as temporary WOULD_BLOCK. So with the arena mostly
 * consumed, a LONG namespace is refused while a SHORT one still fits the same
 * residual -- the case the frozen-pair fixtures could not express, because
 * they made every refusal uniform.
 *
 * O (large) queues and holds the arena; A (long) is byte-refused and parks;
 * B (small) provably fits that same residual. Production must still deliver
 * exactly O, A, B once each and in that order. Identity is asserted, not
 * counts: each namespace's suffix begins with its own marker byte.
 *
 * matched_control=true runs the same state but issues B INSTEAD of A, proving
 * B really crosses at the identical pre-drain point. */
static int
nsu_byte_capacity_order(bool matched_control)
{
    ca_t a;
    ca_init(&a);
    uint32_t saved = g_test_max_actions;
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        g_test_max_actions = saved;
        printf("FAIL: rig create (nsu_byte_capacity_order)\n");
        return 1;
    }
    /* Action slots are deliberately GENEROUS: any refusal below must be byte
     * capacity, never slot exhaustion. */
    g_test_max_actions = 0u;
    conn_t *pub = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    conn_t *w = rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, pub && w);
    peer_state_t pub_ps, wp;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&wp, 0, sizeof(wp));
    rig_pump(&rig, 4);

    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(w->peer, &nscfg, rig.now,
                                                  &nsh) == MOQ_OK);
    for (int d = 0; d < 6; d++) {
        peer_drain(&rig, w, &wp, false, false, 0, 0);
        rig_pump(&rig, 6);
    }
    peer_drain(&rig, w, &wp, false, false, 0, 0);
    memset(&wp, 0, sizeof(wp));

    moqr_bind_stats_t base;
    moqr_bind_get_stats(rig.bind, &base);
    R_CHECK(&rig, base.nsu_live == 0);

    /* Three namespaces under the same prefix, each individually inside the
     * full-track-name ceiling, distinguished by their first suffix byte. */
    static uint8_t big_o[2400], big_a[1900], small_b[8];
    memset(big_o, 'o', sizeof(big_o));   big_o[0] = 'O';
    memset(big_a, 'a', sizeof(big_a));   big_a[0] = 'A';
    memset(small_b, 'b', sizeof(small_b)); small_b[0] = 'B';
    R_CHECK(&rig, 4 + sizeof(big_o) < MOQ_FULL_TRACK_NAME_MAX);
    R_CHECK(&rig, 4 + sizeof(big_a) < MOQ_FULL_TRACK_NAME_MAX);

    moq_bytes_t nO[2] = { B("live"), { big_o, sizeof(big_o) } };
    moq_bytes_t nA[2] = { B("live"), { big_a, sizeof(big_a) } };
    moq_bytes_t nB[2] = { B("live"), { small_b, sizeof(small_b) } };

    /* Freeze the watcher direction so queued send bytes stay resident. */
    w->frozen = true;

    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    moq_announcement_t aO, aB;

    /* (1) O prefills the arena and must really queue. */
    pc.track_namespace = (moq_namespace_t){ .parts = nO, .count = 2 };
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                &aO) == MOQ_OK);
    rig_pump(&rig, 6);
    moqr_bind_stats_t afterO;
    moqr_bind_get_stats(rig.bind, &afterO);

    /* (2) the second namespace: long A (refused on bytes) or, in the matched
     *     control, small B at the IDENTICAL residual state. */
    if (!matched_control) {
        /* A and B are queued BACK-TO-BACK with no intervening pump, then the
         * publisher pair is advanced so both reach the relay, then the bind
         * pump runs EXACTLY ONCE. So A fresh-attempts and parks, and B is the
         * immediately following intent in that same batch -- the only shape
         * in which `parked -> blocked` can be the thing that stops B. */
        /* One SimPair advancement does not deterministically carry two
         * publisher requests, so the two announces are driven as CORE inputs
         * in order. The output path under test -- bind intent execution and
         * the real session sends -- is unchanged. */
        R_CHECK(&rig, afterO.nsu_live == 0);   /* no prior pending A existed */
        moqr_binding_t ab;
        R_CHECK(&rig, moqr_core_binding_open(rig.core, 9001, &ab) == MOQR_OK);
        R_CHECK(&rig, moqr_core_announce(
                          rig.core, ab,
                          (moqr_ns_t){ (const moq_bytes_t *)nA, 2 }) ==
                          MOQR_OK);
        R_CHECK(&rig, moqr_core_announce(
                          rig.core, ab,
                          (moqr_ns_t){ (const moq_bytes_t *)nB, 2 }) ==
                          MOQR_OK);
        rig_bind_pump_once(&rig);
    } else {
        pc.track_namespace = (moq_namespace_t){ .parts = nB, .count = 2 };
        R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now,
                                                    &aB) == MOQ_OK);
        rig_step_pair(&rig, pub);
        rig_bind_pump_once(&rig);
    }
    moqr_bind_stats_t after2;
    moqr_bind_get_stats(rig.bind, &after2);
    printf("  byte_cap(control=%d): afterO live=%u retained=%llu | after2 "
           "live=%u retained=%llu errors=%llu\n",
           (int)matched_control, afterO.nsu_live,
           (unsigned long long)afterO.nsu_retained, after2.nsu_live,
           (unsigned long long)after2.nsu_retained,
           (unsigned long long)after2.session_errors);
    /* Whatever happened, it was NOT a hard error and NOT fail-closed. */
    R_CHECK(&rig, after2.session_errors == 0);
    R_CHECK(&rig, after2.nsu_failed_closed == 0);
    R_CHECK(&rig, after2.ordered_failed_closed == 0);
    if (!matched_control) {
        /* BOTH A and B were retained in that single pump: A on real byte
         * capacity, B because the propagation-set `blocked` force-parked it.
         * Action slots are unconstrained, so neither refusal is slot-driven. */
        R_CHECK(&rig, after2.nsu_retained == afterO.nsu_retained + 2u);
        R_CHECK(&rig, after2.nsu_live == 2);
        R_CHECK(&rig, after2.pending_high_water >= 2);
    } else {
        /* The matched control crosses at the same residual: nothing parked. */
        R_CHECK(&rig, after2.nsu_retained == afterO.nsu_retained);
    }

    /* Nothing may have crossed while frozen. */
    peer_state_t frozen;
    memset(&frozen, 0, sizeof(frozen));
    peer_drain(&rig, w, &frozen, false, false, 0, 0);
    R_CHECK(&rig, frozen.ns_found == 0);

    /* Release and observe identity + order. */
    w->frozen = false;
    for (int d = 0; d < 12; d++) {
        peer_drain(&rig, w, &wp, false, false, 0, 0);
        rig_pump(&rig, 8);
    }
    peer_drain(&rig, w, &wp, false, false, 0, 0);
    printf("  byte_cap(control=%d): found=%d ids=%s\n", (int)matched_control,
           wp.ns_found, wp.ns_ids);
    if (matched_control) {
        /* B really crosses at that residual: O then B, once each. */
        R_CHECK(&rig, strcmp(wp.ns_ids, "OB") == 0);
    } else {
        R_CHECK(&rig, strcmp(wp.ns_ids, "OAB") == 0);
    }

    moqr_bind_stats_t fin;
    moqr_bind_get_stats(rig.bind, &fin);
    R_CHECK(&rig, fin.nsu_live == 0);

    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    g_test_max_actions = saved;
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: nsu_byte_capacity_order(control=%d)\n",
               (int)matched_control);
    }
    return f;
}

/* An omitted SUBSCRIPTION_FILTER is explicitly unfiltered in drafts 16 and
 * 18. The session surfaces omission as FILTER_NONE; the binding must preserve
 * the core request initializer's live-edge default instead of forwarding zero
 * as an invalid core filter type. */
static int
subscribe_omitted_filter_is_unfiltered(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: subscribe_omitted_filter rig create\n");
        return 1;
    }
    conn_t *pub = rig_connect(&rig, version);
    conn_t *sub = rig_connect(&rig, version);
    R_CHECK(&rig, pub != NULL && sub != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t parts[2] = { B("show"), B("camera") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    rig_pump(&rig, 6);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    scfg.track_name = B("video");
    R_CHECK(&rig, scfg.filter == MOQ_SUBSCRIBE_FILTER_NONE);
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 8);

    peer_state_t pub_state;
    peer_state_t sub_state;
    memset(&pub_state, 0, sizeof(pub_state));
    memset(&sub_state, 0, sizeof(sub_state));
    peer_drain(&rig, pub, &pub_state, true, false, 0, 0);
    rig_pump(&rig, 6);
    peer_drain(&rig, sub, &sub_state, false, false, 0, 0);
    R_CHECK(&rig, pub_state.up_seen);
    R_CHECK(&rig, sub_state.subscribe_ok);
    R_CHECK(&rig, sub_state.subscribe_errors == 0);

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: subscribe_omitted_filter_is_unfiltered (draft=%llu)\n",
               (unsigned long long)version);
    }
    return f + rig.failures;
}

/* This relay has one source slot per exact namespace. Drafts 16 and 18 permit
 * establishment/idle time to select which publisher to reject or disconnect
 * when an implementation cannot retain every publisher. Pin the relay's
 * deterministic policy: a newly established publisher replaces the incumbent,
 * the old generation is cancelled and purged, and subsequent demand is routed
 * only to the replacement. */
static int
publish_namespace_newest_wins(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: publish_namespace_newest_wins rig create\n");
        return 1;
    }
    conn_t *old_pub = rig_connect(&rig, version);
    conn_t *new_pub = rig_connect(&rig, version);
    conn_t *sub = rig_connect(&rig, version);
    R_CHECK(&rig, old_pub != NULL && new_pub != NULL && sub != NULL);
    rig_pump(&rig, 4);

    moq_bytes_t parts[2] = { B("show"), B("camera") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    moq_announcement_t old_ann;
    moq_announcement_t new_ann;
    R_CHECK(&rig, moq_session_publish_namespace(old_pub->peer, &pcfg, rig.now,
                                                &old_ann) == MOQ_OK);
    rig_pump(&rig, 6);
    peer_state_t old_state;
    peer_state_t new_state;
    memset(&old_state, 0, sizeof(old_state));
    memset(&new_state, 0, sizeof(new_state));
    peer_drain(&rig, old_pub, &old_state, false, false, 0, 0);
    R_CHECK(&rig, old_state.namespace_accepted);

    /* Establish an old-generation track before replacement. The takeover must
     * retire this source and terminate its downstream subscriber, not merely
     * swap the namespace owner while stale track state survives. */
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t old_sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &old_sh) ==
                      MOQ_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, old_pub, &old_state, true, false, 0, 0);
    R_CHECK(&rig, old_state.up_subs == 1 && old_state.up_seen);
    rig_pump(&rig, 6);
    peer_state_t old_sub_state;
    memset(&old_sub_state, 0, sizeof(old_sub_state));
    peer_drain(&rig, sub, &old_sub_state, false, false, 0, 0);
    R_CHECK(&rig, old_sub_state.subscribe_ok);

    R_CHECK(&rig, moq_session_publish_namespace(new_pub->peer, &pcfg, rig.now,
                                                &new_ann) == MOQ_OK);
    rig_pump(&rig, 10);
    peer_drain(&rig, old_pub, &old_state, false, false, 0, 0);
    peer_drain(&rig, new_pub, &new_state, false, false, 0, 0);
    R_CHECK(&rig, old_state.ns_cancelled == 1);
    R_CHECK(&rig, old_state.ns_cancelled_code ==
                      (version == MOQ_VERSION_DRAFT_18
                           ? 0u
                           : MOQ_REQUEST_ERROR_UNINTERESTED));
    R_CHECK(&rig, new_state.namespace_accepted);
    R_CHECK(&rig, new_state.ns_rejected == 0);
    peer_drain(&rig, sub, &old_sub_state, false, false, 0, 0);
    R_CHECK(&rig, old_sub_state.done_count == 1);

    /* A duplicate from the winning session is still invalid and must not
     * evict that session's own accepted announcement. */
    moq_announcement_t duplicate;
    R_CHECK(&rig, moq_session_publish_namespace(new_pub->peer, &pcfg, rig.now,
                                                &duplicate) == MOQ_ERR_INVAL);

    conn_t *new_sub = rig_connect(&rig, version);
    R_CHECK(&rig, new_sub != NULL);
    rig_pump(&rig, 4);
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(new_sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    rig_pump(&rig, 8);
    peer_drain(&rig, old_pub, &old_state, true, false, 0, 0);
    peer_drain(&rig, new_pub, &new_state, true, false, 0, 0);
    R_CHECK(&rig, old_state.up_subs == 1);
    R_CHECK(&rig, new_state.up_subs == 1);
    R_CHECK(&rig, new_state.up_seen);

    /* Cancellation frees the losing session's request. It may publish the
     * namespace again later; the binding mirror must have retired the old
     * handle or repeated reconnect/takeover cycles consume slots and refuse a
     * publisher whose wire state is valid. */
    memset(&old_state, 0, sizeof(old_state));
    memset(&new_state, 0, sizeof(new_state));
    moq_announcement_t third_ann;
    R_CHECK(&rig, moq_session_publish_namespace(old_pub->peer, &pcfg, rig.now,
                                                &third_ann) == MOQ_OK);
    rig_pump(&rig, 10);
    peer_drain(&rig, old_pub, &old_state, false, false, 0, 0);
    peer_drain(&rig, new_pub, &new_state, false, false, 0, 0);
    R_CHECK(&rig, old_state.namespace_accepted);
    R_CHECK(&rig, old_state.ns_rejected == 0);
    R_CHECK(&rig, new_state.ns_cancelled == 1);

    int f = rig.failures;
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (f == 0 && rig.failures == 0) {
        printf("PASS: publish_namespace_newest_wins (draft=%llu)\n",
               (unsigned long long)version);
    }
    return f + rig.failures;
}

int
main(void)
{
    int failures = 0;
    failures += subscribe_omitted_filter_is_unfiltered(MOQ_VERSION_DRAFT_16);
    failures += subscribe_omitted_filter_is_unfiltered(MOQ_VERSION_DRAFT_18);
    failures += publish_namespace_newest_wins(MOQ_VERSION_DRAFT_16);
    failures += publish_namespace_newest_wins(MOQ_VERSION_DRAFT_18);
    failures += forward_pause_resume_oracle(MOQ_VERSION_DRAFT_16);
    failures += forward_pause_resume_oracle(MOQ_VERSION_DRAFT_18);
    failures += forward_sibling_oracle(MOQ_VERSION_DRAFT_16);
    failures += forward_sibling_oracle(MOQ_VERSION_DRAFT_18);
    failures += forward_begin_zero_reset(MOQ_VERSION_DRAFT_16);
    failures += forward_begin_zero_reset(MOQ_VERSION_DRAFT_18);
    failures += ready_idle_zero_probes();
    failures += ready_action_cap_park();
    failures += ready_blocked_aggregate();
    failures += ready_bind_sg_rearm();
    failures += ready_hot_not_starving();
    failures += ready_session_sg_poll();
    failures += ready_subdone_rearm();
    failures += ready_confirm_failure_closes();
    failures += ready_probe_error_policy();
    failures += ready_property_write_floor();
    failures += ready_withdraw_idle_wake();
    failures += ready_detach_replacement_clean();
    failures += test_conn_version_fail_closed();
    failures += test_intent_router();
    failures += test_oom_allocator();
    failures += test_oom_binding_flow();
    failures += route_projection_oracle();
    failures += close_churn_downstream();
    failures += close_churn_publisher();
    failures += close_churn_pending_scalar();
    failures += dataplane_delivery_oracle(0);   /* clean               */
    failures += dataplane_delivery_oracle(3);   /* under backpressure  */
    failures += dataplane_datagram_status_oracle(0);
    failures += dataplane_datagram_status_oracle(3);
    failures += dataplane_retained_replay_oracle(0);
    failures += dataplane_retained_replay_oracle(3);
    failures += dataplane_late_fin_oracle(0);
    failures += dataplane_late_fin_oracle(3);
    failures += dataplane_fin_backpressure_oracle();
    failures += dataplane_eog_backpressure_oracle(false, MOQ_VERSION_DRAFT_18);
    failures += dataplane_eog_backpressure_oracle(true, MOQ_VERSION_DRAFT_18);
    failures += dataplane_eog_backpressure_oracle(false, MOQ_VERSION_DRAFT_16);
    failures += dataplane_eog_backpressure_oracle(true, MOQ_VERSION_DRAFT_16);
    failures += dataplane_eog_conflict_fail_closed();
    failures += dataplane_eviction_skip_oracle();
    failures += dataplane_source_end_oracle(false, 0);   /* pull            */
    failures += dataplane_source_end_oracle(false, 3);   /* pull + backpressure */
    failures += dataplane_source_end_oracle(true, 0);    /* push            */
    failures += dataplane_source_end_oracle(true, 3);    /* push + backpressure */
    failures += dataplane_push_survives_downstream_linger();
    failures += dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_16,
                                             MOQ_VERSION_DRAFT_18, 0);
    failures += dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_18,
                                             MOQ_VERSION_DRAFT_16, 0);
    failures += dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_16,
                                             MOQ_VERSION_DRAFT_18, 3);
    failures += dataplane_mixed_draft_oracle(MOQ_VERSION_DRAFT_18,
                                             MOQ_VERSION_DRAFT_16, 3);
    failures += range_completion_linger();
    failures += long_lived_request_credit(MOQ_VERSION_DRAFT_16, 0);
    failures += long_lived_request_credit(MOQ_VERSION_DRAFT_18, 0);
    failures += long_lived_request_credit(MOQ_VERSION_DRAFT_16, 1);
    failures += borrowed_intent_never_parked();
    failures += intent_retry_reentry_preserves_outer_batch();
    failures += subgroup_slot_reclamation();
    failures += subgroup_reclamation_shapes();
    failures += subgroup_eviction_spares_live_lower_group();
    failures += parity_flow(MOQ_VERSION_DRAFT_16);
    failures += parity_flow(MOQ_VERSION_DRAFT_18);

    failures += small_caps_flow();
    failures += teardown_pending_intent_flow();
    failures += ring_full_track_status_flow();
    failures += ring_full_publish_flow();
    failures += shutdown_no_session_flow();
    failures += test_latency_clock_regression();
    failures += test_auth_token_mirror();
    failures += test_auth_deny_matrix();
    failures += test_auth_toy_verifier();
    failures += test_auth_toy_clamps_defer();
    failures += test_auth_defer_resume();
    failures += test_auth_defer_reject();
    failures += test_auth_defer_stale();
    failures += test_auth_defer_park_fail();
    failures += test_auth_defer_matrix();
    failures += test_reval_subscribe_revoke();
    failures += test_reval_announce_revoke();
    failures += test_forcewithdraw_publisher_cancel();
    failures += test_reval_reserve_capacity();
    failures += test_reval_conn_close_retires();
    failures += test_reval_resume_creates_grant();
    failures += test_reval_cancel_backpressure();
    failures += test_namespace_done_releases_route();
    failures += test_publish_finished_releases_track();
    failures += test_unsupported_requests_reject();
    failures += fetch_flow(MOQ_VERSION_DRAFT_16);
    failures += fetch_flow(MOQ_VERSION_DRAFT_18);
    /* Evicted-prefix marker, both drafts + a d18 backpressure variant. The
     * marker object id is the draft max (literals; see fetch_evicted_prefix):
     * d16 = 2^62-1 (MOQ_QUIC_VARINT_MAX), d18 = UINT64_MAX (MOQ_VI64_MAX). */
    failures += fetch_evicted_prefix(MOQ_VERSION_DRAFT_16, 0x3FFFFFFFFFFFFFFFull, 0);
    failures += fetch_evicted_prefix(MOQ_VERSION_DRAFT_18, UINT64_MAX, 0);
    failures += fetch_evicted_prefix(MOQ_VERSION_DRAFT_18, UINT64_MAX, 2);
    /* Fetch datagram-preference cross-draft (d18 preserves, d16 clears). */
    failures += fetch_datagram_pref(MOQ_VERSION_DRAFT_18, true);
    failures += fetch_datagram_pref(MOQ_VERSION_DRAFT_16, false);
    failures += fetch_datagram_parity();
    failures += chunk_ingest_flow();   /* chunked COMPLETE delivery + abandon-skip */
    failures += chunk_delivery_backpressure();   /* WOULD_BLOCK resume */
    failures += chunk_liveedge_case(false);   /* live edge + clean complete */
    failures += chunk_liveedge_case(true);    /* abandon → downstream reset */
    failures += chunk_liveedge_evict();       /* eviction-after-stall → reset */
    failures += sub_done_resets_begun_subgroup(); /* SUB_DONE mid-object → reset */
    /* Retained FETCH over chunked COMPLETE records —
     * parity with whole-object storage across d16/d18 + a backpressure variant. */
    failures += retained_fetch_representation_parity_oracle(MOQ_VERSION_DRAFT_18, 0);
    failures += retained_fetch_representation_parity_oracle(MOQ_VERSION_DRAFT_16, 0);
    failures += retained_fetch_representation_parity_oracle(MOQ_VERSION_DRAFT_18, 3);
    failures += chunked_fetch_oom_terminates();   /* coalesce OOM fails closed */
    failures += fetch_backpressure();
    failures += fetch_close_midstream();
    failures += fetch_error_terminates();
    failures += test_upstream_done_terminates_downstream();
    failures += test_publish_finished_terminates_downstream();
    failures += test_upstream_redirect_rejects_downstream();
    failures += test_upstream_goaway_terminates_downstream();

    failures += ns_found_survives_blocked_control(false);   /* control */
    failures += ns_found_survives_blocked_control(true);
    failures += nsu_case(MOQ_VERSION_DRAFT_18, false, false, 0);  /* control */
    failures += nsu_case(MOQ_VERSION_DRAFT_18, true, false, 0);
    failures += nsu_case(MOQ_VERSION_DRAFT_16, false, false, 0);  /* control */
    failures += nsu_case(MOQ_VERSION_DRAFT_16, true, false, 0);
    failures += nsu_gone_survives_block(MOQ_VERSION_DRAFT_18);
    failures += nsu_gone_survives_block(MOQ_VERSION_DRAFT_16);
    failures += nsu_zero_suffix_retained(MOQ_VERSION_DRAFT_18);
    failures += nsu_zero_suffix_retained(MOQ_VERSION_DRAFT_16);
    failures += nsu_purge_during_ring_retry();
    failures += nsu_cross_kind_order(true);
    failures += nsu_cross_kind_order(false);
    failures += nsu_byte_capacity_order(true);    /* matched control */
    failures += nsu_byte_capacity_order(false);
    failures += nsu_exhaustion_fail_closed();
    failures += nsu_ring_full_fail_closed(true);
    failures += nsu_ring_full_fail_closed(false);
    failures += nsu_ns_insert_keeps_scalar_full();
    failures += root_namespace_end_to_end(false);
    failures += root_namespace_end_to_end(true);
    failures += ann_store_rejects_empty_fields();
    failures += root_full_track_name_session_flow();
    failures += mixed_profile_root_discovery();
    failures += nsu_case(MOQ_VERSION_DRAFT_18, true, false, 6);  /* repeated */
    failures += nsu_multipart_bytes_exact();
    failures += nsu_close_purges_held();

    uint64_t h1 = parity_hash_run();
    uint64_t h2 = parity_hash_run();
    if (h1 == 0 || h1 != h2) {
        printf("FAIL: parity hash: %llx vs %llx\n",
               (unsigned long long)h1, (unsigned long long)h2);
        failures++;
    } else {
        printf("PASS: parity_hash (run-twice equal)\n");
    }
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
