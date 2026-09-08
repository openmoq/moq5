/*
 * Deterministic production-binding lifecycle scenario: seeded random control-
 * plane operations over real moq_session_t pairs (SimPair) attached to the
 * production relay binding, with invariants checked at bounded
 * intervals and run-twice projection determinism per seed (a cookie-independent
 * trace projection; raw trace-hash is opt-in — see the RULING comment below).
 *
 * Purpose: the individual binding tests prove individual flows; this harness
 * exists to catch OMISSION — lifecycle events the binding silently ignores,
 * requests left waiting forever, state diverging under churn. Every relay-side
 * lifecycle event in the REQUIRED list below must be exercised at least once
 * across the seed sweep, proven by its wire-visible consequence at a test peer
 * (the binding owns relay-side event polling, so the test never polls relay
 * sessions directly; each exercised[] increment is annotated with the wire
 * proof it rests on).
 *
 * REQUIRED exercised events (asserted cumulatively across the sweep):
 *   NAMESPACE_PUBLISHED   <- publisher observed namespace_accepted/rejected
 *   NAMESPACE_DONE        <- watcher observed NAMESPACE_GONE after unannounce
 *   SUBSCRIBE_REQUEST     <- subscriber observed subscribe_ok / subscribe_error
 *   SUBSCRIBE_OK          <- downstream OK for a sub that required an upstream
 *   SUBSCRIBE_ERROR       <- downstream error carrying the origin's reject code
 *   SUBSCRIBE_DONE        <- downstream DONE carrying the origin's done status
 *   PUBLISH_REQUEST       <- pusher observed publish_ok / publish_error
 *   PUBLISH_FINISHED      <- downstream DONE carrying the finish status, or a
 *                            successful re-publish of a finished track
 *   FETCH_REQUEST         <- FETCH_OK + FETCH_COMPLETE (served from retained),
 *                            or fetch_error (unknown track / invalid range)
 *   SUBSCRIBE_TRACKS_REQUEST <- tracks error NOT_SUPPORTED at the requester
 *   REQUEST_REDIRECT      <- downstream subscribe_error INTERNAL_ERROR after
 *                            the origin rejected the upstream with REDIRECT
 *   REQUEST_GOAWAY        <- downstream PUBLISH_DONE GOING_AWAY after the origin sent a
 *                            per-request GOAWAY on the accepted upstream
 *   SESSION_CLOSED        <- binding conn gauge dropped after a peer close
 *
 * Out of scope here (deliberately not exercised):
 *   OBJECT_RECEIVED / OBJECT_CHUNK (data plane; covered by parity_flow and the
 *   core scenario suite), UNSUBSCRIBED / NS_SUB_REQUEST / TRACK_STATUS_REQUEST
 *   (already covered elsewhere; not in the required lifecycle list),
 *   SETUP_COMPLETE (implicit in every connect), REQUEST_READY (draft-16
 *   OUTBOUND request headroom — not consumed; request-capacity auto-grant
 *   counts inbound requests instead, see bind_maybe_grant_capacity), and the
 *   peer-side-only kinds the relay never receives (it initiates no
 *   announce/fetch/publish/ns-sub/tracks upstream). There is no public
 *   ns-unsubscribe API, so watcher churn is connect/close only.
 *
 * Invariants (every INV_INTERVAL ops and at end):
 *   - core stats sanity: subs == parked+active, pool gauges <= limits,
 *     retained_bytes <= the closed-form payload ceiling, route_epoch monotonic
 *   - route dump JSON+text render safely (OK or exact-truncation CAPACITY;
 *     JSON brace-balanced — names here are plain ASCII)
 *   - end: no outstanding peer request without a terminal outcome, no queued
 *     core intents after quiescence, allocator balance zero
 *   - per seed: run twice, identical trace hash + exercised set + stats
 *
 * Env: MOQ_LIFECYCLE_SEEDS (default 25), MOQ_LIFECYCLE_SEED_START (0),
 * MOQ_LIFECYCLE_OPS (default 200). Local deep sweep: MOQ_LIFECYCLE_SEEDS=500.
 * On failure prints a REPLAY line with the exact env to reproduce one seed.
 */

#include <moq/relay/relay.h>
#include <moq/relay/trace.h>

#include <moq/relay/moqr_bind.h>

#include <moq/moq.h>
#include <moq/sim.h>

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- deterministic prng (splitmix64, as the core scenario suite) ------------ */

typedef struct rng {
    uint64_t s;
} rng_t;

static uint64_t
rng_next(rng_t *r)
{
    r->s += 0x9E3779B97F4A7C15ull;
    uint64_t z = r->s;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

/* -- counting allocator ------------------------------------------------------ */

typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
} ca_t;
static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
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

/* -- exercised-event ledger --------------------------------------------------- */

enum {
    EX_NAMESPACE_PUBLISHED = 0,
    EX_NAMESPACE_DONE,
    EX_SUBSCRIBE_REQUEST,
    EX_SUBSCRIBE_OK,
    EX_SUBSCRIBE_ERROR,
    EX_SUBSCRIBE_DONE,
    EX_PUBLISH_REQUEST,
    EX_PUBLISH_FINISHED,
    EX_FETCH_REQUEST,
    EX_SUBSCRIBE_TRACKS_REQUEST,
    EX_REQUEST_REDIRECT,
    EX_REQUEST_GOAWAY,
    EX_SESSION_CLOSED,
    EX__COUNT
};

static const char *const ex_name[EX__COUNT] = {
    "NAMESPACE_PUBLISHED",  "NAMESPACE_DONE",  "SUBSCRIBE_REQUEST",
    "SUBSCRIBE_OK",         "SUBSCRIBE_ERROR", "SUBSCRIBE_DONE",
    "PUBLISH_REQUEST",      "PUBLISH_FINISHED", "FETCH_REQUEST",
    "SUBSCRIBE_TRACKS_REQUEST", "REQUEST_REDIRECT", "REQUEST_GOAWAY",
    "SESSION_CLOSED",
};

/* Status codes chosen so each wire outcome maps unambiguously to the relay-side
 * event that produced it (the relay's own codes for subscribe are DOES_NOT_EXIST
 * 0x10 / UNAUTHORIZED 0x1 / INTERNAL 0x0 / GOING_AWAY 0x6; range-complete DONE
 * is 0). */
#define LC_ORIGIN_REJECT_CODE 0x12u /* origin plain reject  -> SUBSCRIBE_ERROR */
#define LC_ORIGIN_DONE_CODE   0x2u  /* origin done_subscribe -> SUBSCRIBE_DONE */
#define LC_FINISH_CODE        0x5u  /* pusher finish_publish -> PUBLISH_FINISHED */
#define LC_REDIRECT_REJECT    0x0u  /* REQUEST_ERROR INTERNAL_ERROR: pre-OK REDIRECT */
#define LC_GOAWAY_DONE        0x4u  /* PUBLISH_DONE GOING_AWAY: origin GOAWAY */

/* -- rig: peer pool over SimPair + the production binding ---------------------- */

#define LC_CONNS       5
#define LC_ORIGIN      0 /* d18: announces; answers/goaways/dones upstreams   */
#define LC_PUSHER      1 /* d18: PUBLISH push + finish                        */
#define LC_WATCHER     2 /* d18: namespace subscription                       */
#define LC_SUB_A       3 /* draft by seed                                     */
#define LC_SUB_B       4 /* draft by seed                                     */

#define LC_NS_COUNT    3  /* announced namespaces {lc, a|b|c}                 */
#define LC_PUSH_COUNT  4  /* pushed namespaces {push, p0..p3}                 */
#define LC_MAX_OUT     96 /* outstanding peer requests awaiting a terminal    */
#define LC_MAX_USUB    16 /* origin-accepted upstream subscriptions           */
#define INV_INTERVAL   16
#define LC_END_DRAIN   24
#define LC_CLOSE_WAIT  16

typedef struct lc_conn {
    bool           used;
    moq_simpair_t *sp;
    moq_session_t *peer;  /* test endpoint                                    */
    moq_session_t *rsess; /* relay side, owned by the binding after attach    */
    moq_version_t  version;
    uint32_t       epoch; /* reconnect count (SimPair seed derivation)        */
} lc_conn_t;

/* One outstanding peer request awaiting a terminal wire outcome. */
enum { OR_SUB = 0, OR_FETCH, OR_TRACKS, OR_ANN, OR_NSSUB, OR_PUB };
static const char *const or_name[] = { "subscribe", "fetch",     "tracks",
                                       "announce",  "ns_sub",    "publish" };

typedef struct out_req {
    bool     used;
    uint8_t  kind;
    uint8_t  conn;
    uint32_t conn_epoch; /* voided if the conn is closed/replaced             */
    uint32_t op_index;
    uint64_t handle;      /* peer-side handle _opaque                         */
    bool     asked_origin; /* OR_SUB: target ns was announced -> upstream ran */
} out_req_t;

/* Deterministic fault/backpressure profile, derived from the seed (or forced by
 * env). All knobs use ONLY public SimPair/session/relay config — no injected
 * hooks. A zero field means "library default". */
typedef struct lc_faults {
    bool     on;
    uint32_t label;                 /* for replay lines                       */
    uint32_t max_actions[LC_CONNS]; /* SimPair per-session action queue       */
    /* core capacity overrides */
    uint32_t max_subs, max_tracks, max_ns_nodes, max_ns_subs, max_intents;
    uint32_t log_max_groups, log_max_bytes;
    /* bind capacity overrides */
    uint32_t bind_max_dsubs, bind_max_usubs, bind_max_pubs, bind_max_anns;
    uint32_t bind_max_sgs;   /* downstream subgroup slot pool (reclamation)   */
    bool     close_pressure; /* close/reconnect far more aggressively         */
} lc_faults_t;

typedef struct lc {
    ca_t         *alloc;
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;
    lc_conn_t     conns[LC_CONNS];
    uint64_t      now;
    rng_t         rng;
    uint64_t      seed;
    lc_faults_t   f;
    uint32_t      op_index;
    int           failures;

    /* generator bookkeeping */
    bool     announced[LC_NS_COUNT];
    bool     pushed[LC_PUSH_COUNT];       /* live publication               */
    uint64_t push_handle[LC_PUSH_COUNT];  /* peer moq_publication_t._opaque  */
    bool     push_finished_once[LC_PUSH_COUNT];
    bool     watcher_subscribed;
    uint64_t origin_usub[LC_MAX_USUB]; /* accepted upstream subs (peer side) */
    uint32_t origin_usub_count;
    out_req_t out[LC_MAX_OUT];

    /* exercised proofs (copied into the run result) */
    uint32_t ex[EX__COUNT];
    /* pending-proof counters (set when the op fires, consumed by the drain) */
    uint32_t redirects_pending;  /* origin redirect-rejects issued           */
    uint32_t unannounce_pending; /* unannounces issued while watcher watched */
    uint64_t last_route_epoch;

    /* Op-log replay: every generator choice goes through lc_rng, which
     * records the raw rng value (generate) or replays it from log_vals (replay),
     * so a run reproduces WITHOUT the PRNG. op_start[] marks per-op boundaries
     * for a human-readable, per-op log. */
    uint64_t     *log_vals;
    uint32_t      log_cap, log_n, log_replay_i, setup_end;
    uint32_t     *op_start;   /* [ops+1]: start index of each op's values */
    bool          replay;
    bool          replay_underflow;   /* replay asked for a value past log_n */
    /* Synthetic-failure arm serialized WITH the log (poison=<T> in the
     * LCLOG1 header), so a minimized artifact reproduces standalone. -1 = off.
     * Sourced from the parsed log on replay, from g_lc_poison_at_op on generate. */
    int           poison;
} lc_t;

/* The one generator entropy source: record (generate) or replay (from log).
 * In replay, a read past the recorded stream is NOT silently zeroed — it sets
 * replay_underflow so run_one can reject a truncated/diverging log loudly. */
static uint64_t
lc_rng(lc_t *lc)
{
    if (lc->replay) {
        if (lc->log_replay_i >= lc->log_n) {
            lc->replay_underflow = true;
            return 0;
        }
        return lc->log_vals[lc->log_replay_i++];
    }
    uint64_t v = rng_next(&lc->rng);
    if (lc->log_vals != NULL && lc->log_n < lc->log_cap) {
        lc->log_vals[lc->log_n++] = v;
    }
    return v;
}

/* Test-only synthetic-failure arm. -1 = disabled (production/default);
 * the minimizer self-test sets it to T so a run fails on reaching op index T. */
static int g_lc_poison_at_op = -1;

#define L_CHECK(lc, expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            printf("FAIL: %s:%d: %s (seed 0x%" PRIx64 " op %u)\n", __FILE__,  \
                   __LINE__, #expr, (lc)->seed, (lc)->op_index);              \
            (lc)->failures++;                                                 \
        }                                                                     \
    } while (0)

static const char *
lc_ns_part2(int k)
{
    static const char *const p2[LC_NS_COUNT] = { "a", "b", "c" };
    return p2[k];
}

static const char *
lc_push_part2(int k)
{
    static const char *const p2[LC_PUSH_COUNT] = { "p0", "p1", "p2", "p3" };
    return p2[k];
}

static bool
lc_connect(lc_t *lc, int slot, moq_version_t version)
{
    lc_conn_t *cn = &lc->conns[slot];
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &lc->alloc->vt;
    cfg.seed = lc->seed * 2654435761ull ^ ((uint64_t)slot << 8) ^
               ((uint64_t)cn->epoch << 16);
    cfg.version = version;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 4096;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 4096;
    cfg.max_actions = lc->f.max_actions[slot]; /* 0 = session default */
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return false;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(lc->bind, cn->rsess, version) != MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        cn->sp = NULL;
        return false;
    }
    cn->used = true;
    cn->version = version;
    return true;
}

/* One deterministic pump cycle: transports step, then the production binding
 * does everything else. */
static void
lc_cycle(lc_t *lc)
{
    lc->now += 1000;
    for (int i = 0; i < LC_CONNS; i++) {
        lc_conn_t *cn = &lc->conns[i];
        if (!cn->used || cn->sp == NULL) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, lc->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_bind_pump(lc->bind, lc->now);
}

static void
lc_pump(lc_t *lc, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        lc_cycle(lc);
    }
}

/* -- outstanding-request ledger ----------------------------------------------- */

static void
out_add(lc_t *lc, uint8_t kind, int conn, uint64_t handle, bool asked_origin)
{
    for (int i = 0; i < LC_MAX_OUT; i++) {
        if (!lc->out[i].used) {
            lc->out[i] = (out_req_t){ .used = true,
                                      .kind = kind,
                                      .conn = (uint8_t)conn,
                                      .conn_epoch = lc->conns[conn].epoch,
                                      .op_index = lc->op_index,
                                      .handle = handle,
                                      .asked_origin = asked_origin };
            return;
        }
    }
    /* Ledger full: deterministic — count it as a failure so the scale knobs
     * get fixed rather than silently dropping accounting. */
    L_CHECK(lc, !"outstanding-request ledger full");
}

static out_req_t *
out_find(lc_t *lc, uint8_t kind, int conn, uint64_t handle)
{
    for (int i = 0; i < LC_MAX_OUT; i++) {
        if (lc->out[i].used && lc->out[i].kind == kind &&
            lc->out[i].conn == conn && lc->out[i].handle == handle &&
            lc->out[i].conn_epoch == lc->conns[conn].epoch) {
            return &lc->out[i];
        }
    }
    return NULL;
}

static void
out_clear_conn(lc_t *lc, int conn)
{
    for (int i = 0; i < LC_MAX_OUT; i++) {
        if (lc->out[i].used && lc->out[i].conn == conn) {
            lc->out[i].used = false; /* the close is the terminal outcome */
        }
    }
}

/* -- peer drains: bookkeeping + exercised inference ---------------------------- */

static void
lc_drain_conn(lc_t *lc, int slot)
{
    lc_conn_t *cn = &lc->conns[slot];
    if (!cn->used || cn->sp == NULL) {
        return;
    }
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            out_req_t *o;
            switch (ev->kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                /* The relay's upstream SUBSCRIBE arriving at the origin.
                 * Deterministic answer policy from the seeded rng. */
                if (slot == LC_ORIGIN) {
                    uint64_t r = lc_rng(lc) % 8;
                    if (r == 5) {
                        moq_reject_subscribe_cfg_t rj;
                        moq_reject_subscribe_cfg_init(&rj);
                        rj.error_code = LC_ORIGIN_REJECT_CODE;
                        (void)moq_session_reject_subscribe(
                            cn->peer, ev->u.subscribe_request.sub, &rj,
                            lc->now);
                    } else if (r == 6 &&
                               cn->version == MOQ_VERSION_DRAFT_18) {
                        /* All-empty redirect target: a client REDIRECT must
                         * carry a zero-length URI (reuse current). */
                        moq_reject_subscribe_cfg_t rj;
                        moq_reject_subscribe_cfg_init(&rj);
                        rj.error_code = MOQ_REQUEST_ERROR_REDIRECT;
                        if (moq_session_reject_subscribe(
                                cn->peer, ev->u.subscribe_request.sub, &rj,
                                lc->now) == MOQ_OK) {
                            lc->redirects_pending++;
                        }
                    } else {
                        moq_accept_subscribe_cfg_t ac;
                        moq_accept_subscribe_cfg_init(&ac);
                        if (moq_session_accept_subscribe(
                                cn->peer, ev->u.subscribe_request.sub, &ac,
                                lc->now) == MOQ_OK &&
                            lc->origin_usub_count < LC_MAX_USUB) {
                            lc->origin_usub[lc->origin_usub_count++] =
                                ev->u.subscribe_request.sub._opaque;
                            /* Push one object so the relay's track accrues a
                             * log record: when the last downstream leaves, the
                             * track lingers to WARM (record retained) instead of
                             * freeing, so a later subscribe is a warm REJOIN —
                             * the fan-out that emits ACCEPT_SUB then the
                             * borrowed UPSTREAM_SUBSCRIBE. This is what makes the
                             * fuzzer explore the borrowed-remainder class the
                             * pending_nonscalar_blocked invariant guards. */
                            moq_subgroup_cfg_t sgc;
                            moq_subgroup_cfg_init(&sgc);
                            moq_subgroup_handle_t sgh;
                            if (moq_session_open_subgroup(
                                    cn->peer, ev->u.subscribe_request.sub, &sgc,
                                    lc->now, &sgh) == MOQ_OK) {
                                uint8_t body[4] = { 0xA5, 0x5A, 0xC3, 0x3C };
                                moq_rcbuf_t *pl = NULL;
                                if (moq_rcbuf_create(&lc->alloc->vt, body,
                                                     sizeof(body), &pl) ==
                                    MOQ_OK) {
                                    (void)moq_session_write_object(
                                        cn->peer, sgh, 0, pl, lc->now);
                                    moq_rcbuf_decref(pl);
                                }
                                (void)moq_session_close_subgroup(cn->peer, sgh,
                                                                 lc->now);
                            }
                        }
                    }
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                o = out_find(lc, OR_SUB, slot, ev->u.subscribe_ok.sub._opaque);
                if (o != NULL) {
                    /* Wire proof: the relay observed SUBSCRIBE_REQUEST and
                     * accepted this downstream. */
                    lc->ex[EX_SUBSCRIBE_REQUEST]++;
                    if (o->asked_origin) {
                        /* The track needed an upstream, so the OK proves the
                         * relay consumed the origin's SUBSCRIBE_OK. */
                        lc->ex[EX_SUBSCRIBE_OK]++;
                    }
                    o->used = false;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR: {
                uint64_t code = ev->u.subscribe_error.error_code;
                o = out_find(lc, OR_SUB, slot,
                             ev->u.subscribe_error.sub._opaque);
                if (o != NULL) {
                    lc->ex[EX_SUBSCRIBE_REQUEST]++;
                    if (code == LC_ORIGIN_REJECT_CODE) {
                        /* The origin's plain-reject code forwarded verbatim
                         * proves the relay consumed SUBSCRIBE_ERROR. */
                        lc->ex[EX_SUBSCRIBE_ERROR]++;
                    } else if (code == LC_REDIRECT_REJECT &&
                               lc->redirects_pending > 0) {
                        /* A pre-OK generic reject is the REDIRECT path
                         * (goaway is only ever sent on ACCEPTED upstreams). */
                        lc->ex[EX_REQUEST_REDIRECT]++;
                        lc->redirects_pending--;
                    }
                    o->used = false;
                }
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_DONE: {
                uint64_t code = ev->u.subscribe_done.status_code;
                /* DONE arrives after the OK already cleared the ledger entry;
                 * map by the status code the relay forwarded. */
                if (code == LC_ORIGIN_DONE_CODE) {
                    lc->ex[EX_SUBSCRIBE_DONE]++;
                } else if (code == LC_FINISH_CODE) {
                    lc->ex[EX_PUBLISH_FINISHED]++;
                } else if (code == LC_GOAWAY_DONE) {
                    lc->ex[EX_REQUEST_GOAWAY]++;
                }
                break;
            }
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                o = out_find(lc, OR_ANN, slot,
                             ev->u.namespace_accepted.ann._opaque);
                if (o != NULL) {
                    lc->ex[EX_NAMESPACE_PUBLISHED]++;
                    o->used = false;
                }
                break;
            case MOQ_EVENT_NAMESPACE_REJECTED:
                o = out_find(lc, OR_ANN, slot,
                             ev->u.namespace_rejected.ann._opaque);
                if (o != NULL) {
                    lc->ex[EX_NAMESPACE_PUBLISHED]++; /* observed + rejected */
                    o->used = false;
                }
                break;
            case MOQ_EVENT_NAMESPACE_GONE:
                if (slot == LC_WATCHER && lc->unannounce_pending > 0) {
                    /* Wire proof: the relay consumed NAMESPACE_DONE and fanned
                     * the withdrawal to the watcher. (Origin-close withdrawals
                     * never overlap: unannounce_pending is cleared on close.) */
                    lc->ex[EX_NAMESPACE_DONE]++;
                    lc->unannounce_pending--;
                }
                break;
            case MOQ_EVENT_NS_SUB_OK:
                o = out_find(lc, OR_NSSUB, slot,
                             ev->u.ns_sub_ok.handle._opaque);
                if (o != NULL) {
                    o->used = false;
                }
                break;
            case MOQ_EVENT_NS_SUB_ERROR:
                o = out_find(lc, OR_NSSUB, slot,
                             ev->u.ns_sub_error.handle._opaque);
                if (o != NULL) {
                    o->used = false;
                }
                break;
            case MOQ_EVENT_PUBLISH_OK:
                o = out_find(lc, OR_PUB, slot, ev->u.publish_ok.pub._opaque);
                if (o != NULL) {
                    lc->ex[EX_PUBLISH_REQUEST]++;
                    o->used = false;
                }
                /* A publish_ok for a track that was finished before proves the
                 * relay released the source on PUBLISH_FINISHED. */
                for (int k = 0; k < LC_PUSH_COUNT; k++) {
                    if (lc->push_handle[k] == ev->u.publish_ok.pub._opaque &&
                        lc->push_finished_once[k]) {
                        lc->ex[EX_PUBLISH_FINISHED]++;
                        lc->push_finished_once[k] = false;
                    }
                }
                break;
            case MOQ_EVENT_PUBLISH_ERROR:
                o = out_find(lc, OR_PUB, slot,
                             ev->u.publish_error.pub._opaque);
                if (o != NULL) {
                    lc->ex[EX_PUBLISH_REQUEST]++; /* observed + rejected */
                    o->used = false;
                }
                for (int k = 0; k < LC_PUSH_COUNT; k++) {
                    if (lc->push_handle[k] == ev->u.publish_error.pub._opaque) {
                        lc->pushed[k] = false; /* rejected: slot reusable */
                    }
                }
                break;
            case MOQ_EVENT_FETCH_OK:
                /* FETCH is wired: a served fetch is accepted then
                 * FETCH_COMPLETE-terminated. Count the exercise on the first
                 * response; the out-request is consumed by FETCH_COMPLETE. */
                o = out_find(lc, OR_FETCH, slot, ev->u.fetch_ok.fetch._opaque);
                if (o != NULL) {
                    lc->ex[EX_FETCH_REQUEST]++;
                }
                break;
            case MOQ_EVENT_FETCH_COMPLETE:
                o = out_find(lc, OR_FETCH, slot,
                             ev->u.fetch_complete.fetch._opaque);
                if (o != NULL) {
                    o->used = false;
                }
                break;
            case MOQ_EVENT_FETCH_ERROR:
                /* Unknown-track / invalid-range reject is also a valid outcome. */
                o = out_find(lc, OR_FETCH, slot,
                             ev->u.fetch_error.fetch._opaque);
                if (o != NULL) {
                    lc->ex[EX_FETCH_REQUEST]++;
                    o->used = false;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR:
                o = out_find(lc, OR_TRACKS, slot,
                             ev->u.subscribe_tracks_error.handle._opaque);
                if (o != NULL) {
                    if (ev->u.subscribe_tracks_error.error_code ==
                        MOQ_REQUEST_ERROR_NOT_SUPPORTED) {
                        lc->ex[EX_SUBSCRIBE_TRACKS_REQUEST]++;
                    }
                    o->used = false;
                }
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

static void
lc_drain_all(lc_t *lc)
{
    for (int i = 0; i < LC_CONNS; i++) {
        lc_drain_conn(lc, i);
    }
}

/* -- invariants ----------------------------------------------------------------- */

static void
lc_check_stats(lc_t *lc, const moqr_core_capacity_t *cap,
               const moqr_core_limits_t *lim)
{
    moqr_core_stats_t st;
    moqr_core_get_stats(lc->core, &st);
    L_CHECK(lc, st.subs == st.subs_parked + st.subs_active);
    L_CHECK(lc, st.subs <= lim->max_subs);
    L_CHECK(lc, st.tracks <= lim->max_tracks);
    L_CHECK(lc, st.ns_subs <= lim->max_ns_subs);
    L_CHECK(lc, st.bindings <= lim->max_bindings);
    L_CHECK(lc, st.intent_highwater <= lim->max_intents);
    L_CHECK(lc, st.retained_bytes <= cap->payload_bytes);
    L_CHECK(lc, st.route_epoch >= lc->last_route_epoch);
    lc->last_route_epoch = st.route_epoch;

    /* The deferred-output ring must NEVER hold a borrowed-payload intent kind:
     * those views dangle across pumps (relay.h:102). This is the discriminating
     * invariant for the borrowed-remainder bug. */
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(lc->bind, &bs);
    L_CHECK(lc, bs.pending_nonscalar_blocked == 0);
}

static void
lc_check_dumps(lc_t *lc)
{
    static char buf[64 * 1024];
    size_t written = 0;
    moqr_result_t rc =
        moqr_core_route_dump_json(lc->core, buf, sizeof(buf), &written);
    L_CHECK(lc, rc == MOQR_OK || rc == MOQR_ERR_CAPACITY);
    if (rc == MOQR_OK) {
        L_CHECK(lc, buf[0] == '{');
        long bal = 0;
        for (const char *p = buf; *p; p++) {
            if (*p == '{') {
                bal++;
            } else if (*p == '}') {
                bal--;
            }
        }
        L_CHECK(lc, bal == 0); /* names are plain ASCII in this scenario */
    }
    rc = moqr_core_route_dump_text(lc->core, buf, sizeof(buf), &written);
    L_CHECK(lc, rc == MOQR_OK || rc == MOQR_ERR_CAPACITY);
}

/* -- the operation generator ------------------------------------------------------ */

static void
lc_op_announce(lc_t *lc)
{
    int k = (int)(lc_rng(lc) % LC_NS_COUNT);
    /* Duplicate announces are deliberately allowed at low frequency (the relay
     * rejects the second as occupied — still a NAMESPACE_PUBLISHED proof). */
    moq_bytes_t nsp[2] = { B("lc"), B(lc_ns_part2(k)) };
    moq_publish_namespace_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    if (moq_session_publish_namespace(lc->conns[LC_ORIGIN].peer, &pc, lc->now,
                                      &ann) == MOQ_OK) {
        out_add(lc, OR_ANN, LC_ORIGIN, ann._opaque, false);
        if (!lc->announced[k]) {
            lc->announced[k] = true;
        }
    }
}

static void
lc_op_subscribe(lc_t *lc)
{
    int who = (lc_rng(lc) % 2 == 0) ? LC_SUB_A : LC_SUB_B;
    lc_conn_t *cn = &lc->conns[who];
    if (!cn->used) {
        return;
    }
    uint64_t pick = lc_rng(lc) % 10;
    moq_bytes_t nsp[2];
    bool asked_origin = false;
    if (pick < 6) {
        int k = (int)(lc_rng(lc) % LC_NS_COUNT);
        nsp[0] = B("lc");
        nsp[1] = B(lc_ns_part2(k));
        asked_origin = lc->announced[k] && lc->conns[LC_ORIGIN].used;
    } else if (pick < 8) {
        int k = (int)(lc_rng(lc) % LC_PUSH_COUNT);
        nsp[0] = B("push");
        nsp[1] = B(lc_push_part2(k));
        /* Pushed tracks are relay-local ACTIVE: no upstream consulted. */
    } else {
        nsp[0] = B("nx");
        nsp[1] = B("void");
    }
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = B("t");
    uint64_t f = lc_rng(lc) % 3;
    sc.filter = f == 0 ? MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT
                : f == 1 ? MOQ_SUBSCRIBE_FILTER_NEXT_GROUP
                         : MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    if (moq_session_subscribe(cn->peer, &sc, lc->now, &sh) == MOQ_OK) {
        out_add(lc, OR_SUB, who, sh._opaque, asked_origin);
    }
}

static void
lc_op_origin_done(lc_t *lc)
{
    if (lc->origin_usub_count == 0 || !lc->conns[LC_ORIGIN].used) {
        return;
    }
    uint32_t i = (uint32_t)(lc_rng(lc) % lc->origin_usub_count);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = LC_ORIGIN_DONE_CODE;
    (void)moq_session_done_subscribe(
        lc->conns[LC_ORIGIN].peer,
        (moq_subscription_t){ ._opaque = lc->origin_usub[i] }, &dc, lc->now);
    lc->origin_usub[i] = lc->origin_usub[--lc->origin_usub_count];
}

static void
lc_op_origin_goaway(lc_t *lc)
{
    if (lc->origin_usub_count == 0 || !lc->conns[LC_ORIGIN].used) {
        return;
    }
    uint32_t i = (uint32_t)(lc_rng(lc) % lc->origin_usub_count);
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    (void)moq_session_request_goaway_subscribe(
        lc->conns[LC_ORIGIN].peer,
        (moq_subscription_t){ ._opaque = lc->origin_usub[i] }, &gc, lc->now);
    lc->origin_usub[i] = lc->origin_usub[--lc->origin_usub_count];
}

static void
lc_op_publish(lc_t *lc)
{
    if (!lc->conns[LC_PUSHER].used) {
        return;
    }
    int k = (int)(lc_rng(lc) % LC_PUSH_COUNT);
    if (lc->pushed[k]) {
        return; /* live publication for this track */
    }
    moq_bytes_t nsp[2] = { B("push"), B(lc_push_part2(k)) };
    moq_publish_cfg_t pc;
    memset(&pc, 0, sizeof(pc));
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    pc.track_name = B("t");
    moq_publication_t ph;
    if (moq_session_publish(lc->conns[LC_PUSHER].peer, &pc, lc->now, &ph) ==
        MOQ_OK) {
        out_add(lc, OR_PUB, LC_PUSHER, ph._opaque, false);
        lc->pushed[k] = true;
        lc->push_handle[k] = ph._opaque;
    }
}

static void
lc_op_finish_publish(lc_t *lc)
{
    if (!lc->conns[LC_PUSHER].used) {
        return;
    }
    for (int off = 0, k0 = (int)(lc_rng(lc) % LC_PUSH_COUNT);
         off < LC_PUSH_COUNT; off++) {
        int k = (k0 + off) % LC_PUSH_COUNT;
        if (!lc->pushed[k]) {
            continue;
        }
        moq_finish_publish_cfg_t fc;
        memset(&fc, 0, sizeof(fc));
        moq_finish_publish_cfg_init(&fc);
        fc.status_code = LC_FINISH_CODE;
        if (moq_session_finish_publish(
                lc->conns[LC_PUSHER].peer,
                (moq_publication_t){ ._opaque = lc->push_handle[k] }, &fc,
                lc->now) == MOQ_OK) {
            lc->pushed[k] = false;
            lc->push_finished_once[k] = true;
        }
        return;
    }
}

static void
lc_op_fetch(lc_t *lc)
{
    int who = (int)(lc_rng(lc) % 2) ? LC_SUB_A : LC_SUB_B;
    if (!lc->conns[who].used) {
        return;
    }
    moq_bytes_t nsp[1] = { B("lc") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = B("t");
    fc.start_group = 0;
    fc.start_object = 0;
    fc.end_group = 1;
    fc.end_object = 0;
    moq_fetch_t fh;
    if (moq_session_fetch(lc->conns[who].peer, &fc, lc->now, &fh) == MOQ_OK) {
        out_add(lc, OR_FETCH, who, fh._opaque, false);
    }
}

static void
lc_op_tracks(lc_t *lc)
{
    int who = (int)(lc_rng(lc) % 2) ? LC_SUB_A : LC_SUB_B;
    lc_conn_t *cn = &lc->conns[who];
    if (!cn->used || cn->version != MOQ_VERSION_DRAFT_18) {
        return; /* SUBSCRIBE_TRACKS is draft-18 only */
    }
    moq_bytes_t nsp[1] = { B("lc") };
    moq_subscribe_tracks_cfg_t tc;
    memset(&tc, 0, sizeof(tc));
    moq_subscribe_tracks_cfg_init(&tc);
    tc.track_namespace_prefix = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_track_sub_handle_t th;
    if (moq_session_subscribe_tracks(cn->peer, &tc, lc->now, &th) == MOQ_OK) {
        out_add(lc, OR_TRACKS, who, th._opaque, false);
    }
}

static void
lc_op_ns_subscribe(lc_t *lc)
{
    if (lc->watcher_subscribed || !lc->conns[LC_WATCHER].used) {
        return;
    }
    moq_bytes_t pfx[1] = { B("lc") };
    moq_subscribe_namespace_cfg_t nc;
    memset(&nc, 0, sizeof(nc));
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h;
    if (moq_session_subscribe_namespace(lc->conns[LC_WATCHER].peer, &nc,
                                        lc->now, &h) == MOQ_OK) {
        out_add(lc, OR_NSSUB, LC_WATCHER, h._opaque, false);
        lc->watcher_subscribed = true;
    }
}

/* Close one peer connection (transport-level close observed by the relay as
 * SESSION_CLOSED), verify the binding detached it, then reconnect the role.
 * Returns the closed slot, or -1 if nothing was closed. */
static int
lc_op_close_reconnect(lc_t *lc)
{
    int slot = (int)(lc_rng(lc) % LC_CONNS);
    lc_conn_t *cn = &lc->conns[slot];
    if (!cn->used) {
        return -1;
    }
    moqr_bind_stats_t before;
    moqr_bind_get_stats(lc->bind, &before);
    (void)moq_session_close(cn->peer, 0x0, "lifecycle churn", lc->now);
    /* Under tiny action queues binding_close makes bounded progress per pump
     * (its reject / NS_GONE fan-out is reserve-before-mutate), so give detach a
     * generous but bounded budget — the invariant is that it DOES complete. */
    int budget = lc->f.on ? LC_CLOSE_WAIT * 6 : LC_CLOSE_WAIT;
    bool detached = false;
    for (int i = 0; i < budget && !detached; i++) {
        lc_cycle(lc);
        lc_drain_all(lc);
        moqr_bind_stats_t now_st;
        moqr_bind_get_stats(lc->bind, &now_st);
        detached = now_st.conns < before.conns;
    }
    /* Wire proof: the binding observed SESSION_CLOSED and released the conn.
     * Detach that never completes is a hang — a real failure, seed preserved. */
    L_CHECK(lc, detached);
    if (detached) {
        lc->ex[EX_SESSION_CLOSED]++;
    }
    out_clear_conn(lc, slot); /* outcomes died with the connection */
    if (slot == LC_ORIGIN) {
        memset(lc->announced, 0, sizeof(lc->announced));
        lc->origin_usub_count = 0;
        lc->redirects_pending = 0;   /* answered by binding-close rejects */
        lc->unannounce_pending = 0;  /* GONEs may arrive from the close    */
    }
    if (slot == LC_PUSHER) {
        memset(lc->pushed, 0, sizeof(lc->pushed));
        memset(lc->push_finished_once, 0, sizeof(lc->push_finished_once));
    }
    if (slot == LC_WATCHER) {
        lc->watcher_subscribed = false;
        lc->unannounce_pending = 0; /* nobody left to observe the GONE */
    }
    moq_simpair_destroy(cn->sp);
    cn->sp = NULL;
    cn->used = false;
    cn->epoch++;
    moq_version_t v = cn->version;
    if (slot == LC_SUB_A || slot == LC_SUB_B) {
        v = (lc_rng(lc) % 2 == 0) ? MOQ_VERSION_DRAFT_16
                                          : MOQ_VERSION_DRAFT_18;
    }
    L_CHECK(lc, lc_connect(lc, slot, v));
    lc_pump(lc, lc->f.on ? 6 : 2); /* handshake (slower under tiny queues) */
    return slot;
}

/* -- one full seeded run ------------------------------------------------------------ */

/* Run-twice comparison uses a cookie-independent projection of the trace
 * (per-kind record counts + totals) rather than the raw FNV hash. This is a
 * deliberate RULING, NOT a temporary compromise: session-handle
 * cookies embed an ADDRESS-derived session tag (core/src/session/session.c:
 * 1666-1674) that the relay core folds into some trace records (e.g. the ns_sub
 * route_add in relay/src/core/relay.c). That tag is an internal cross-session
 * misuse-detection cookie with NO wire-visible/protocol meaning, so a byte-
 * identical trace hash would prove nothing the wire cares about — the projection
 * compares exactly the behavior required to be deterministic (failures, trace
 * count, per-kind counts, route epoch, exercised proofs, gauges). Verified
 * empirically: seed 0x11fec0de diverges in exactly that record with equal
 * counts. MOQ_LIFECYCLE_STRICT_HASH=1 stays an OPT-IN diagnostic, meaningful
 * only on local/address-stable runs; deterministic session tags (a libmoq-core
 * cfg/API change) are an optional future item, needed only for byte-exact golden
 * trace corpora — deliberately NOT the default. */
#define LC_TRACE_KINDS 64
#define LC_TRACE_RING  4096

typedef struct lc_result {
    uint64_t trace_hash; /* compared only under MOQ_LIFECYCLE_STRICT_HASH */
    uint64_t trace_count;
    uint32_t kind_counts[LC_TRACE_KINDS];
    uint32_t ex[EX__COUNT];
    uint64_t route_epoch;
    int      failures;
    /* `genuine_failures` counts real lifecycle failures (L_CHECK /
     * ledger / poison) EXCLUDING the replay stream-consumption check; a replay
     * that under/overshoots the recorded stream sets `replay_desync`. The
     * minimizer's predicate is (genuine_failures || leak) && !replay_desync, so
     * a deletion that merely desyncs the positional stream is rejected rather
     * than mistaken for a reproducer of the original failure. */
    int      genuine_failures;
    bool     replay_desync;
    /* Op-log (generate mode only; the caller owns + frees these): the
     * recorded rng value stream + per-op boundaries, enough to serialize or
     * replay the exact run. NULL/0 in replay mode and on early-abort. */
    uint64_t   *log_vals;
    uint32_t   *op_start;
    uint32_t    log_n, setup_end;
    uint64_t    seed;
    uint32_t    ops;
    lc_faults_t faults;
    /* Synthetic-failure arm carried out of the run so serialize can
     * write poison=<T> into the header (-1 = off). Makes minimized/replayed logs
     * standalone: the failure trigger rides the artifact, not a process-global. */
    int         poison;
} lc_result_t;

/* Parsed op-log fed to run_one for replay (borrowed value stream). `quiet`
 * suppresses the expected underflow/leftover diagnostic (negative tests that
 * inject a deliberately mismatched stream and assert on the result). */
typedef struct lc_replay {
    const uint64_t *vals;
    uint32_t        n;
    bool            quiet;
    /* Synthetic-failure arm for this replay (-1 = off). Sourced from the
     * parsed log's poison= header, NOT g_lc_poison_at_op — this is what makes a
     * replayed/minimized artifact reproduce standalone, with no env/global set. */
    int             poison;
} lc_replay_t;

/* Announce handles needed for unannounce live on the ledger (OR_ANN entries are
 * cleared once accepted). Track the most recent accepted announce per ns for
 * publish_namespace_done: the origin peer keeps handles valid until done. */
typedef struct lc_anns {
    bool     have[LC_NS_COUNT];
    uint64_t handle[LC_NS_COUNT];
} lc_anns_t;

#define LC_APPLY(field, src) \
    do {                      \
        if ((src) != 0) {     \
            field = (src);    \
        }                     \
    } while (0)

static void
run_one(uint64_t seed, uint32_t ops, const lc_faults_t *faults, ca_t *ca,
        lc_result_t *out, const lc_replay_t *rp)
{
    memset(out, 0, sizeof(*out));
    out->poison = -1;   /* default off; overwritten with lc.poison at finalize */
    lc_t lc;
    memset(&lc, 0, sizeof(lc));
    lc.alloc = ca;
    lc.seed = seed;
    lc.rng.s = seed;
    lc.now = 1;
    lc.f = *faults;
    /* The failure arm rides the replay struct (parsed from the log) on
     * replay, and the generate-time global on generate — so it round-trips
     * through serialize/parse and a minimized log reproduces without any env. */
    lc.poison = (rp != NULL) ? rp->poison : g_lc_poison_at_op;

    if (moqr_trace_create(&ca->vt, LC_TRACE_RING, &lc.trace) != MOQR_OK) {
        printf("FAIL: trace create (seed 0x%" PRIx64 ")\n", seed);
        out->failures = 1;
        return;
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &ca->vt);
    cfg.trace = lc.trace;
    cfg.log_budget.max_groups = 8;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 2000;
    /* Capacity faults (0 = keep the roomy default). The intent ring is clamped
     * up to max(2, subs, ns_subs, ns_nodes) at create, so a tiny max_intents is
     * a request for "as small as the pools allow" — legal, and the reserve-
     * before-mutate paths must still refuse on the wire, never hang. */
    LC_APPLY(cfg.max_subs, lc.f.max_subs);
    LC_APPLY(cfg.max_tracks, lc.f.max_tracks);
    LC_APPLY(cfg.max_ns_nodes, lc.f.max_ns_nodes);
    LC_APPLY(cfg.max_ns_subs, lc.f.max_ns_subs);
    LC_APPLY(cfg.max_intents, lc.f.max_intents);
    LC_APPLY(cfg.log_budget.max_groups, lc.f.log_max_groups);
    LC_APPLY(cfg.log_budget.max_bytes, lc.f.log_max_bytes);
    moqr_core_capacity_t cap;
    moqr_core_capacity_describe(&cfg, &cap);
    if (moqr_core_create(&cfg, &lc.core) != MOQR_OK) {
        printf("FAIL: core create (seed 0x%" PRIx64 ")\n", seed);
        moqr_trace_destroy(lc.trace);
        out->failures = 1;
        return;
    }
    moqr_core_limits_t lim;
    moqr_core_get_limits(lc.core, &lim);
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &ca->vt);
    bcfg.core = lc.core;
    bcfg.max_conns = LC_CONNS;
    LC_APPLY(bcfg.max_downstream_subs, lc.f.bind_max_dsubs);
    LC_APPLY(bcfg.max_upstream_subs, lc.f.bind_max_usubs);
    LC_APPLY(bcfg.max_publishes, lc.f.bind_max_pubs);
    LC_APPLY(bcfg.max_announces, lc.f.bind_max_anns);
    LC_APPLY(bcfg.max_open_subgroups, lc.f.bind_max_sgs);
    if (moqr_bind_create(&bcfg, &lc.bind) != MOQR_OK) {
        printf("FAIL: bind create (seed 0x%" PRIx64 ")\n", seed);
        moqr_core_destroy(lc.core);
        moqr_trace_destroy(lc.trace);
        out->failures = 1;
        return;
    }

    /* Op-log: allocate (generate) or borrow (replay) the value stream
     * before the first generator choice (the subscriber-version picks below). */
    if (rp != NULL) {
        lc.replay = true;
        lc.log_vals = (uint64_t *)(uintptr_t)rp->vals;   /* read-only in replay */
        lc.log_n = rp->n;
    } else {
        /* Test scaffolding, kept OUT of the ca accounting (so the leak check
         * stays about the SUT); the caller frees it via lc_result_free_log. */
        lc.log_cap = ops * 32u + 64u;
        lc.log_vals = malloc((size_t)lc.log_cap * sizeof(uint64_t));
        lc.op_start = malloc(((size_t)ops + 1u) * sizeof(uint32_t));
        if (lc.log_vals == NULL || lc.op_start == NULL) {
            free(lc.log_vals);
            free(lc.op_start);
            moqr_bind_destroy(lc.bind);
            moqr_core_destroy(lc.core);
            moqr_trace_destroy(lc.trace);
            out->failures = 1;
            return;
        }
    }

    /* Peer pool. Origin/pusher/watcher are d18 (REDIRECT and per-request GOAWAY
     * ride d18 request streams); subscriber drafts come from the seed. */
    bool up = lc_connect(&lc, LC_ORIGIN, MOQ_VERSION_DRAFT_18) &&
              lc_connect(&lc, LC_PUSHER, MOQ_VERSION_DRAFT_18) &&
              lc_connect(&lc, LC_WATCHER, MOQ_VERSION_DRAFT_18) &&
              lc_connect(&lc, LC_SUB_A,
                         (lc_rng(&lc) % 2 == 0) ? MOQ_VERSION_DRAFT_16
                                                      : MOQ_VERSION_DRAFT_18) &&
              lc_connect(&lc, LC_SUB_B,
                         (lc_rng(&lc) % 2 == 0) ? MOQ_VERSION_DRAFT_16
                                                      : MOQ_VERSION_DRAFT_18);
    L_CHECK(&lc, up);
    lc_pump(&lc, 4); /* handshakes */

    lc_anns_t anns;
    memset(&anns, 0, sizeof(anns));

    if (!lc.replay) {
        lc.setup_end = lc.log_n;   /* values consumed by setup precede op 0 */
    }
    for (lc.op_index = 0; lc.op_index < ops && lc.failures == 0;
         lc.op_index++) {
        if (!lc.replay) {
            lc.op_start[lc.op_index] = lc.log_n;
        }
        uint64_t op = lc_rng(&lc) % 16;
        /* Test-only synthetic failure (lc.poison; -1 = disabled). It
         * fails the run deterministically on reaching op index T — an op-count-
         * based failure whose minimal reproducer is exactly T+1 ops, independent
         * of op kinds or relay state. `continue` (not break) lets op T record its
         * selector, so out->ops includes it and the emitted log replays the
         * failure. lc.poison is sourced from the LOG (poison= header) on replay,
         * so it fires identically on every minimizer candidate WITHOUT any
         * process-global — the minimized artifact is standalone-replayable. */
        if (lc.poison >= 0 && (int)lc.op_index >= lc.poison) {
            lc.failures++;
            continue;
        }
        switch (op) {
        case 0:
        case 1: { /* announce (records the handle for a later unannounce) */
            int k = (int)(lc_rng(&lc) % LC_NS_COUNT);
            if (!lc.conns[LC_ORIGIN].used) {
                break;
            }
            moq_bytes_t nsp[2] = { B("lc"), B(lc_ns_part2(k)) };
            moq_publish_namespace_cfg_t pc;
            memset(&pc, 0, sizeof(pc));
            moq_publish_namespace_cfg_init(&pc);
            pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
            moq_announcement_t ann;
            if (moq_session_publish_namespace(lc.conns[LC_ORIGIN].peer, &pc,
                                              lc.now, &ann) == MOQ_OK) {
                out_add(&lc, OR_ANN, LC_ORIGIN, ann._opaque, false);
                if (!lc.announced[k]) {
                    lc.announced[k] = true;
                    anns.have[k] = true;
                    anns.handle[k] = ann._opaque;
                }
            }
            break;
        }
        case 2: { /* unannounce an announced namespace */
            if (!lc.conns[LC_ORIGIN].used) {
                break;
            }
            int k0 = (int)(lc_rng(&lc) % LC_NS_COUNT);
            for (int off = 0; off < LC_NS_COUNT; off++) {
                int k = (k0 + off) % LC_NS_COUNT;
                if (!anns.have[k]) {
                    continue;
                }
                if (moq_session_publish_namespace_done(
                        lc.conns[LC_ORIGIN].peer,
                        (moq_announcement_t){ ._opaque = anns.handle[k] },
                        lc.now) == MOQ_OK) {
                    anns.have[k] = false;
                    lc.announced[k] = false;
                    if (lc.watcher_subscribed) {
                        lc.unannounce_pending++;
                    }
                }
                break;
            }
            break;
        }
        case 3:
        case 4:
        case 5:
            lc_op_subscribe(&lc);
            break;
        case 6:
            lc_op_origin_done(&lc);
            break;
        case 7:
            lc_op_origin_goaway(&lc);
            break;
        case 8:
            lc_op_publish(&lc);
            break;
        case 9:
            lc_op_finish_publish(&lc);
            break;
        case 10:
            lc_op_fetch(&lc);
            break;
        case 11:
            lc_op_tracks(&lc);
            break;
        case 12:
            lc_op_ns_subscribe(&lc);
            break;
        case 13: /* stale/duplicate at low probability: double-done a random
                  * already-consumed origin upstream handle, or re-announce. */
            if (lc_rng(&lc) % 2 == 0) {
                lc_op_origin_done(&lc); /* may be empty: benign */
                lc_op_origin_done(&lc);
            } else {
                lc_op_announce(&lc); /* possible duplicate announce */
            }
            break;
        case 14: {
            /* Close/reconnect pressure: aggressive under close_pressure so
             * requests are in flight when the connection drops. */
            uint32_t mod = lc.f.close_pressure ? 1u : 4u;
            if (lc_rng(&lc) % mod == 0) {
                int closed = lc_op_close_reconnect(&lc);
                if (closed == LC_ORIGIN) {
                    memset(&anns, 0, sizeof(anns)); /* origin handles stale */
                }
            }
            break;
        }
        default: /* extra pump/tick pressure */
            lc_pump(&lc, 1);
            break;
        }
        lc_pump(&lc, 2);
        lc_drain_all(&lc);
        if (lc.op_index % INV_INTERVAL == INV_INTERVAL - 1) {
            lc_check_stats(&lc, &cap, &lim);
            lc_check_dumps(&lc);
        }
    }

    /* Quiescence drain: every outstanding request must reach a terminal. */
    for (int i = 0; i < LC_END_DRAIN; i++) {
        lc_cycle(&lc);
        lc_drain_all(&lc);
    }
    int hung = 0;
    for (int i = 0; i < LC_MAX_OUT; i++) {
        if (lc.out[i].used) {
            if (hung < 6) { /* cap the flood; the count is what matters */
                printf("FAIL: outstanding %s (seed 0x%" PRIx64
                       " op %u conn %d) never reached a terminal outcome\n",
                       or_name[lc.out[i].kind], seed, lc.out[i].op_index,
                       (int)lc.out[i].conn);
            }
            hung++;
            lc.failures++;
        }
    }
    if (hung > 6) {
        printf("FAIL: ... and %d more outstanding requests (seed 0x%" PRIx64
               ")\n",
               hung - 6, seed);
    }
    /* No stuck core intents after quiescence (the pump drains them; peeking an
     * empty ring steals nothing). */
    moqr_intent_t leftover[8];
    L_CHECK(&lc, moqr_core_poll_intents(lc.core, leftover, 8) == 0);
    lc_check_stats(&lc, &cap, &lim);
    lc_check_dumps(&lc);

    out->trace_hash = moqr_trace_hash(lc.trace);
    out->trace_count = moqr_trace_count(lc.trace);
    /* Per-kind projection needs the FULL record stream in the ring. */
    L_CHECK(&lc, out->trace_count <= LC_TRACE_RING);
    {
        static moqr_trace_rec_t recs[LC_TRACE_RING];
        size_t n = moqr_trace_read(lc.trace, recs, LC_TRACE_RING);
        for (size_t i = 0; i < n; i++) {
            out->kind_counts[recs[i].kind % LC_TRACE_KINDS]++;
        }
    }
    if (getenv("MOQ_LIFECYCLE_DUMP") != NULL) {
        static char dump[128 * 1024];
        size_t w = 0;
        if (moqr_trace_write_jsonl(lc.trace, dump, sizeof(dump), &w) ==
            MOQR_OK) {
            printf("--- trace (seed 0x%" PRIx64 ") ---\n%s", seed, dump);
        }
    }
    memcpy(out->ex, lc.ex, sizeof(lc.ex));
    moqr_core_stats_t st;
    moqr_core_get_stats(lc.core, &st);
    out->route_epoch = st.route_epoch;

    /* A replayed log must drive the SAME code path the generator took.
     * Underflow (a value read past the recorded stream) means the log was too
     * short or the run diverged — always a hard failure. Unconsumed leftover
     * values on an otherwise-clean run mean the log was too long / tampered.
     * (A reproduced failure legitimately breaks the op loop early, leaving a
     * tail unconsumed, so only check leftovers when failures == 0.) */
    int genuine = lc.failures; /* real failures, before the consumption check */
    bool desync = false;
    if (lc.replay) {
        bool quiet = rp->quiet;
        if (lc.replay_underflow) {
            if (!quiet) {
                printf("FAIL: replay underflow (seed 0x%" PRIx64
                       ": log too short for op %u)\n",
                       seed, lc.op_index);
            }
            desync = true;
            lc.failures++;
        } else if (genuine == 0 && lc.log_replay_i != lc.log_n) {
            if (!quiet) {
                printf("FAIL: replay left %u/%u values unconsumed (seed 0x%" PRIx64
                       ")\n",
                       lc.log_n - lc.log_replay_i, lc.log_n, seed);
            }
            desync = true;
            lc.failures++;
        }
    }
    out->genuine_failures = genuine;
    out->replay_desync = desync;
    out->failures = lc.failures;
    out->poison = lc.poison;   /* serialize writes this into the log's header */

    /* Hand the recorded op-log to the caller (generate mode) to
     * serialize/replay — the caller owns and frees log_vals/op_start. Replay
     * borrowed the stream, so there is nothing to transfer or free here. */
    if (!lc.replay) {
        lc.op_start[lc.op_index] = lc.log_n;   /* end boundary of the last op */
        out->log_vals = lc.log_vals;
        out->op_start = lc.op_start;
        out->log_n = lc.log_n;
        out->setup_end = lc.setup_end;
        out->ops = lc.op_index;
        out->seed = seed;
        out->faults = *faults;
    }

    moqr_bind_destroy(lc.bind);
    moqr_core_destroy(lc.core);
    for (int i = 0; i < LC_CONNS; i++) {
        if (lc.conns[i].sp != NULL) {
            moq_simpair_destroy(lc.conns[i].sp);
        }
    }
    moqr_trace_destroy(lc.trace);
}

/* -- Op-log serialize / parse / replay ------------------------------------- */

static const char *
lc_op_name(uint64_t k)
{
    switch (k % 16) {
    case 0:
    case 1:  return "announce";
    case 2:  return "unannounce";
    case 3:
    case 4:
    case 5:  return "subscribe";
    case 6:  return "origin_done";
    case 7:  return "origin_goaway";
    case 8:  return "publish";
    case 9:  return "finish_publish";
    case 10: return "fetch";
    case 11: return "tracks";
    case 12: return "ns_subscribe";
    case 13: return "stale_dup";
    case 14: return "close_reconnect";
    default: return "pump";
    }
}

static bool
lc_kind_known(const char *k)
{
    static const char *const names[] = {
        "announce",  "unannounce", "subscribe", "origin_done", "origin_goaway",
        "publish",   "finish_publish", "fetch",  "tracks",  "ns_subscribe",
        "stale_dup", "close_reconnect", "pump",
    };
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        if (strcmp(k, names[i]) == 0) {
            return true;
        }
    }
    return false;
}

static void
lc_result_free_log(lc_result_t *r)
{
    free(r->log_vals);
    free(r->op_start);
    r->log_vals = NULL;
    r->op_start = NULL;
}

/* Serialize the recorded op-log to `f`: a header line (seed, ops, full fault
 * profile) then a `setup` line and one `op` line per operation, each listing the
 * raw generator values it consumed (hex). Deterministic; no addresses/cookies. */
static void
lc_log_serialize(const lc_result_t *r, FILE *f)
{
    const lc_faults_t *x = &r->faults;
    fprintf(f, "LCLOG1 seed=%" PRIx64 " ops=%u on=%u label=%u cp=%u poison=%d ma=",
            r->seed, r->ops, (unsigned)x->on, x->label,
            (unsigned)x->close_pressure, r->poison);
    for (int i = 0; i < LC_CONNS; i++) {
        fprintf(f, "%s%u", i ? "," : "", x->max_actions[i]);
    }
    fprintf(f, " ms=%u mt=%u mn=%u mns=%u mi=%u lg=%u lb=%u bd=%u bu=%u bp=%u"
               " ba=%u bs=%u\n",
            x->max_subs, x->max_tracks, x->max_ns_nodes, x->max_ns_subs,
            x->max_intents, x->log_max_groups, x->log_max_bytes,
            x->bind_max_dsubs, x->bind_max_usubs, x->bind_max_pubs,
            x->bind_max_anns, x->bind_max_sgs);
    fputs("setup", f);
    for (uint32_t i = 0; i < r->setup_end; i++) {
        fprintf(f, " %" PRIx64, r->log_vals[i]);
    }
    fputc('\n', f);
    for (uint32_t op = 0; op < r->ops; op++) {
        uint32_t s = r->op_start[op], e = r->op_start[op + 1];
        uint64_t kind = s < r->log_n ? r->log_vals[s] : 0;
        fprintf(f, "op %u %s", op, lc_op_name(kind));
        for (uint32_t i = s; i < e; i++) {
            fprintf(f, " %" PRIx64, r->log_vals[i]);
        }
        fputc('\n', f);
    }
}

/* Parse an op-log into faults/seed/ops + a flat value stream for replay. Strict:
 * returns false on a bad magic, a missing header field, an unknown op kind, a
 * kind that disagrees with its first value, non-hex/garbage in a value token, an
 * overflowing value (errno == ERANGE), a valueless op, a value-count overflow,
 * a non-sequential op index, a wrong op count, or a missing/duplicate setup
 * line — so a tampered or truncated artifact can never masquerade as a clean
 * run. Parses in place — no strdup/strtok. */
static bool
lc_log_parse(const char *text, lc_faults_t *fo, uint64_t *seedo, uint32_t *opso,
             uint64_t *vals, uint32_t vals_cap, uint32_t *nvo,
             uint32_t *setup_end_o, uint32_t *op_start_o, int *poison_o)
{
    memset(fo, 0, sizeof(*fo));
    uint64_t seed = 0;
    unsigned ops = 0, on = 0, label = 0, cp = 0, ma[LC_CONNS] = { 0 };
    unsigned ms, mt, mn, mns, mi, lg, lb, bd, bu, bp, ba, bs;
    int poison = -1;
    int got = sscanf(text,
        "LCLOG1 seed=%" SCNx64 " ops=%u on=%u label=%u cp=%u poison=%d"
        " ma=%u,%u,%u,%u,%u ms=%u mt=%u mn=%u mns=%u mi=%u lg=%u lb=%u"
        " bd=%u bu=%u bp=%u ba=%u bs=%u",
        &seed, &ops, &on, &label, &cp, &poison, &ma[0], &ma[1], &ma[2], &ma[3],
        &ma[4], &ms, &mt, &mn, &mns, &mi, &lg, &lb, &bd, &bu, &bp, &ba, &bs);
    if (got != 23) {
        return false;   /* bad magic or missing header field */
    }
    *seedo = seed;
    *opso = ops;
    if (poison_o != NULL) {
        *poison_o = poison;   /* synthetic-failure arm rides the log (-1 = off) */
    }
    fo->on = on != 0;
    fo->label = label;
    fo->close_pressure = cp != 0;
    for (int i = 0; i < LC_CONNS; i++) {
        fo->max_actions[i] = ma[i];
    }
    fo->max_subs = ms;
    fo->max_tracks = mt;
    fo->max_ns_nodes = mn;
    fo->max_ns_subs = mns;
    fo->max_intents = mi;
    fo->log_max_groups = lg;
    fo->log_max_bytes = lb;
    fo->bind_max_dsubs = bd;
    fo->bind_max_usubs = bu;
    fo->bind_max_pubs = bp;
    fo->bind_max_anns = ba;
    fo->bind_max_sgs = bs;

    uint32_t nv = 0;
    bool saw_setup = false;
    uint32_t op_lines = 0;
    const char *p = strchr(text, '\n');
    for (p = (p != NULL) ? p + 1 : NULL; p != NULL && *p != '\0';) {
        const char *nl = strchr(p, '\n');
        const char *end = nl != NULL ? nl : p + strlen(p);
        const char *v;
        bool is_op = false;
        bool is_setup = false;
        char kind[32] = { 0 };
        if (strncmp(p, "setup", 5) == 0 && (p[5] == ' ' || p + 5 >= end)) {
            if (saw_setup) {
                return false;   /* exactly one setup line */
            }
            saw_setup = true;
            is_setup = true;
            v = p + 5;
        } else if (strncmp(p, "op ", 3) == 0) {
            unsigned idx;
            int off = 0;
            if (sscanf(p, "op %u %31s%n", &idx, kind, &off) < 2 ||
                !lc_kind_known(kind)) {
                return false;
            }
            if (!saw_setup || op_lines >= ops || idx != op_lines) {
                return false;   /* op before setup, too many, or out of order */
            }
            is_op = true;
            v = p + off;
        } else {
            return false;   /* unknown line kind */
        }
        /* Strict value scan: space-separated, pure-hex tokens only. Reject any
         * non-hex/non-space garbage, an overflowing value (errno == ERANGE), or
         * a token strtoull stops short of consuming. */
        uint32_t line_v0 = nv;
        while (v < end) {
            if (*v == ' ') {
                v++;
                continue;
            }
            const char *tok = v;
            while (v < end && *v != ' ') {
                if (!isxdigit((unsigned char)*v)) {
                    return false;   /* non-hex garbage in a value token */
                }
                v++;
            }
            if (nv >= vals_cap) {
                return false;
            }
            errno = 0;
            char *ve;
            unsigned long long val = strtoull(tok, &ve, 16);
            if (errno == ERANGE || ve != v) {
                return false;   /* overflow, or strtoull stopped short */
            }
            vals[nv++] = (uint64_t)val;
        }
        if (is_setup && setup_end_o != NULL) {
            *setup_end_o = nv; /* setup values are vals[0, setup_end) */
        }
        if (is_op) {
            if (nv == line_v0) {
                return false;   /* an op must record >= 1 value (its selector) */
            }
            if (strcmp(lc_op_name(vals[line_v0]), kind) != 0) {
                return false;   /* first value must map to the printed kind */
            }
            if (op_start_o != NULL) {
                op_start_o[op_lines] = line_v0; /* op i values start at line_v0 */
            }
            op_lines++;
        }
        p = nl != NULL ? nl + 1 : NULL;
    }
    if (!saw_setup || op_lines != ops) {
        return false;   /* exactly one setup line and exactly `ops` op lines */
    }
    if (op_start_o != NULL) {
        op_start_o[ops] = nv; /* terminating boundary: op_start[ops] == nv */
    }
    *nvo = nv;
    return true;
}

/* Deterministic fault profile from a seed. Capacities are small but legal, and
 * action queues are MODERATE by default (8..40) — enough to stress
 * reserve-before-mutate and binding_close's bounded-progress path while still
 * leaving room for every wire terminal (so the default fault matrix stays green
 * and the ledger has teeth). MOQ_LIFECYCLE_MAX_ACTIONS forces a fixed queue on
 * every session (drive it to 1/2 to hunt fire-and-forget drops). */
static lc_faults_t
lc_derive_faults(uint64_t seed, uint32_t force_actions, bool close_pressure)
{
    lc_faults_t f;
    memset(&f, 0, sizeof(f));
    f.on = true;
    f.label = force_actions ? force_actions : 0xFF;
    rng_t r = { seed ^ 0xFA0175EEDull };
    for (int i = 0; i < LC_CONNS; i++) {
        f.max_actions[i] =
            force_actions ? force_actions : 8u + (uint32_t)(rng_next(&r) % 33);
    }
    f.max_subs = 4u + (uint32_t)(rng_next(&r) % 8);
    f.max_tracks = 3u + (uint32_t)(rng_next(&r) % 6);
    f.max_ns_nodes = 6u + (uint32_t)(rng_next(&r) % 10);
    f.max_ns_subs = 2u + (uint32_t)(rng_next(&r) % 4);
    f.max_intents = 2u; /* clamped up to the pool floor at create */
    f.log_max_groups = 2u + (uint32_t)(rng_next(&r) % 4);
    f.log_max_bytes = 4096u + (uint32_t)(rng_next(&r) % 8192);
    /* Match the bind tables to the core pools so the core's admission control
     * (not an arbitrary bind-table shortfall) governs refusal. */
    f.bind_max_dsubs = f.max_subs;
    f.bind_max_usubs = f.max_tracks;
    f.bind_max_pubs = f.max_tracks;
    f.bind_max_anns = f.max_ns_nodes;
    /* A small downstream subgroup pool so plain-FIN reclamation is exercised
     * under pressure: without it, exhaustion would stall a delivery and the
     * no-request-waits-forever ledger would catch the stuck subscriber. */
    f.bind_max_sgs = 2u + (uint32_t)(rng_next(&r) % 3);
    f.close_pressure = close_pressure;
    return f;
}

/* One seed, run twice, all cross-run + leak invariants. Returns failures and
 * (when ex_total != NULL) accumulates the exercised ledger. */
static int
run_seed_pair(uint64_t s_index, uint64_t seed, uint32_t ops,
              const lc_faults_t *f, uint32_t *ex_total)
{
    int failures = 0;
    ca_t a1, a2;
    ca_init(&a1);
    ca_init(&a2);
    lc_result_t r1, r2;
    run_one(seed, ops, f, &a1, &r1, NULL);
    run_one(seed, ops, f, &a2, &r2, NULL);
    failures += r1.failures + r2.failures;
    bool leak = a1.live != 0 || a2.live != 0;
    if (leak) {
        printf("FAIL: allocator leak (seed 0x%" PRIx64 ", live %ld / %ld)\n",
               seed, a1.live, a2.live);
        failures++;
    }
    bool strict = getenv("MOQ_LIFECYCLE_STRICT_HASH") != NULL;
    bool diverged = (strict && r1.trace_hash != r2.trace_hash) ||
                    r1.trace_count != r2.trace_count ||
                    r1.route_epoch != r2.route_epoch ||
                    memcmp(r1.kind_counts, r2.kind_counts,
                           sizeof(r1.kind_counts)) != 0 ||
                    memcmp(r1.ex, r2.ex, sizeof(r1.ex)) != 0;
    if (diverged) {
        printf("FAIL: run-twice divergence (seed 0x%" PRIx64 "): hash %" PRIx64
               "/%" PRIx64 " count %" PRIu64 "/%" PRIu64 "\n",
               seed, r1.trace_hash, r2.trace_hash, r1.trace_count,
               r2.trace_count);
        failures++;
    }
    /* Emit a replay artifact for ANY failure — including a divergence-only
     * failure (both runs individually "pass" but disagree), which previously
     * printed nothing and left a debugger without a reproducer. */
    bool r1_bad = r1.failures != 0 || a1.live != 0;
    bool r2_bad = r2.failures != 0 || a2.live != 0;
    if (r1_bad || r2_bad || diverged) {
        /* A uniform action queue across all conns means it was forced via env;
         * echo it so the replay reproduces the exact pressure. */
        bool forced = f->on && f->max_actions[0] != 0 &&
                      f->max_actions[0] == f->max_actions[LC_CONNS - 1];
        printf("REPLAY: MOQ_LIFECYCLE_SEED_START=%" PRIu64
               " MOQ_LIFECYCLE_FAULT_SEEDS=1 MOQ_LIFECYCLE_OPS=%u",
               s_index, ops);
        if (f->on) {
            printf(" MOQ_LIFECYCLE_FAULTS=1");
        }
        if (forced) {
            printf(" MOQ_LIFECYCLE_MAX_ACTIONS=%u", f->max_actions[0]);
        }
        if (f->close_pressure) {
            printf(" MOQ_LIFECYCLE_CLOSE_PRESSURE=1");
        }
        printf(" ./test_relay_lifecycle\n");

        /* Serialize the run that actually FAILED (prefer run 1 on a tie), so the
         * artifact reproduces the failure rather than a clean sibling run. For a
         * divergence-only failure both runs share the same seed (hence the same
         * op stream), so run 1's log is a faithful single-run reproducer — but
         * the divergence itself is only visible across the run-twice pair, so
         * say that explicitly. */
        const lc_result_t *rep = (!r1_bad && r2_bad) ? &r2 : &r1;
        if (diverged && !r1_bad && !r2_bad) {
            printf("NOTE: divergence-only failure — the run-twice seed-pair (two "
                   "runs + projection compare) is the reproducer; the op-log "
                   "below replays one deterministic side.\n");
        }

        /* Op-log replay artifact: the exact operation stream, replayable without
         * the PRNG. Written to a file if MOQ_LIFECYCLE_LOG_PATH is set (and its
         * MOQ_LIFECYCLE_REPLAY= command printed), else dumped inline. */
        const char *lp = getenv("MOQ_LIFECYCLE_LOG_PATH");
        FILE *lf = lp != NULL ? fopen(lp, "w") : NULL;
        if (lf != NULL) {
            lc_log_serialize(rep, lf);
            fclose(lf);
            printf("REPLAY-LOG: MOQ_LIFECYCLE_REPLAY=%s ./test_relay_lifecycle\n",
                   lp);
        } else {
            printf("--- op-log (seed 0x%" PRIx64 ") — save to a file and rerun "
                   "with MOQ_LIFECYCLE_REPLAY=<file> ---\n",
                   seed);
            lc_log_serialize(rep, stdout);
        }
    }
    /* Opt-in: dump the op-log for EVERY run (local experiments only). */
    if (getenv("MOQ_LIFECYCLE_LOG") != NULL) {
        lc_log_serialize(&r1, stdout);
    }
    if (ex_total != NULL) {
        for (int i = 0; i < EX__COUNT; i++) {
            ex_total[i] += r1.ex[i];
        }
    }
    lc_result_free_log(&r1);
    lc_result_free_log(&r2);
    return failures;
}

static int
lc_projection_eq(const lc_result_t *a, const lc_result_t *b)
{
    return a->failures == b->failures && a->trace_count == b->trace_count &&
           a->route_epoch == b->route_epoch &&
           memcmp(a->kind_counts, b->kind_counts, sizeof(a->kind_counts)) == 0 &&
           memcmp(a->ex, b->ex, sizeof(a->ex)) == 0;
}

/* Generate a run, serialize its op-log, parse it back, replay, and assert the
 * replay reproduces the generated run's projection EXACTLY — without the PRNG. */
static int
lc_replay_selftest(void)
{
    int fails = 0;
    uint64_t seed = 0x5151ABCDull;
    uint32_t ops = 60;
    lc_faults_t f = lc_derive_faults(seed, 0, false);
    ca_t a1;
    ca_init(&a1);
    lc_result_t g;
    run_one(seed, ops, &f, &a1, &g, NULL);

    FILE *tf = tmpfile();
    if (tf == NULL) {
        printf("FAIL: lc_replay_selftest tmpfile\n");
        lc_result_free_log(&g);
        return 1;
    }
    lc_log_serialize(&g, tf);
    long sz = ftell(tf);
    rewind(tf);
    char *buf = malloc((size_t)sz + 1);
    size_t rd = fread(buf, 1, (size_t)sz, tf);
    buf[rd] = '\0';
    fclose(tf);

    lc_faults_t pf;
    uint64_t pseed = 0;
    uint32_t pops = 0, pn = 0;
    int ppoison = 0;
    uint64_t *pvals = malloc((size_t)(g.log_n + 1) * sizeof(uint64_t));
    if (pvals == NULL) {
        printf("FAIL: lc_replay_selftest alloc\n");
        free(buf);
        lc_result_free_log(&g);
        return 1;
    }
    bool ok = lc_log_parse(buf, &pf, &pseed, &pops, pvals, g.log_n + 1, &pn, NULL,
                           NULL, &ppoison);
    if (!ok || pseed != seed || pops != g.ops || pn != g.log_n ||
        memcmp(&pf, &f, sizeof(f)) != 0 || ppoison != g.poison) {
        printf("FAIL: lc_replay_selftest round-trip (ok=%d pn=%u/%u pois=%d/%d)\n",
               (int)ok, pn, g.log_n, ppoison, g.poison);
        fails++;
    }
    lc_replay_t rp = { .vals = pvals, .n = pn, .poison = ppoison };
    ca_t a2;
    ca_init(&a2);
    lc_result_t rr;
    run_one(pseed, pops, &pf, &a2, &rr, &rp);
    if (!lc_projection_eq(&g, &rr)) {
        printf("FAIL: lc_replay_selftest projection diverged "
               "(gen fail=%d count=%" PRIu64 " vs replay fail=%d count=%" PRIu64
               ")\n",
               g.failures, g.trace_count, rr.failures, rr.trace_count);
        fails++;
    }
    if (a1.live != 0 || a2.live != 0) {
        printf("FAIL: lc_replay_selftest leak %ld/%ld\n", a1.live, a2.live);
        fails++;
    }
    free(buf);
    free(pvals);
    lc_result_free_log(&g);
    if (fails == 0) {
        printf("PASS: lc_replay_selftest\n");
    }
    return fails;
}

/* The parser accepts a well-formed log and rejects every malformed one loudly:
 * bad magic, unknown/mismatched op kind, non-hex garbage, an overflowing value,
 * a valueless op, a wrong op count, a non-sequential index, and a missing or
 * duplicate setup line. */
static int
lc_parse_reject_test(void)
{
    int fails = 0;
    lc_faults_t pf;
    uint64_t seed;
    uint32_t ops, nv;
    uint64_t vals[64];
    const char *hdr =
        "LCLOG1 seed=1 ops=1 on=0 label=0 cp=0 poison=-1 ma=8,8,8,8,8 ms=4 mt=3 "
        "mn=6 mns=2 mi=2 lg=2 lb=4096 bd=4 bu=3 bp=3 ba=6 bs=2\n";

#define REJECT(desc, ...)                                                     \
    do {                                                                      \
        char b_[600];                                                         \
        snprintf(b_, sizeof(b_), __VA_ARGS__);                                \
        if (lc_log_parse(b_, &pf, &seed, &ops, vals, 64, &nv, NULL, NULL,     \
                         NULL)) {                                             \
            printf("FAIL: parser accepted %s\n", desc);                       \
            fails++;                                                          \
        }                                                                     \
    } while (0)

    /* op selector 0 maps to "announce" (lc_op_name), so a valid announce op
     * leads with a value whose low nibble is 0 or 1. */
    REJECT("a bad magic", "garbage\n");
    REJECT("an unknown op kind", "%ssetup 5\nop 0 bogus_kind 0\n", hdr);
    REJECT("a kind that mismatches its first value",
           "%ssetup 5\nop 0 unannounce 0\n", hdr); /* 0 -> announce, not unann. */
    REJECT("a non-hex value token", "%ssetup 5\nop 0 announce 0 zz\n", hdr);
    REJECT("an overflowing value",
           "%ssetup 5\nop 0 announce 0 ffffffffffffffffffff\n", hdr);
    REJECT("a valueless op", "%ssetup 5\nop 0 announce\n", hdr);
    REJECT("too many op lines (ops=1)",
           "%ssetup 5\nop 0 announce 0\nop 1 pump f\n", hdr);
    REJECT("too few op lines (ops=1)", "%ssetup 5\n", hdr);
    REJECT("a non-sequential op index", "%ssetup 5\nop 5 announce 0\n", hdr);
    REJECT("a missing setup line", "%sop 0 announce 0\n", hdr);
    REJECT("a duplicate setup line",
           "%ssetup 5\nsetup 6\nop 0 announce 0\n", hdr);
    REJECT("a header missing the poison field",
           "LCLOG1 seed=1 ops=1 on=0 label=0 cp=0 ma=8,8,8,8,8 ms=4 mt=3 mn=6 "
           "mns=2 mi=2 lg=2 lb=4096 bd=4 bu=3 bp=3 ba=6 bs=2\nsetup 5\n"
           "op 0 announce 0\n");
#undef REJECT

    char good[512];
    int pp = 0;
    snprintf(good, sizeof(good), "%ssetup 5\nop 0 announce 0 a\n", hdr);
    if (!lc_log_parse(good, &pf, &seed, &ops, vals, 64, &nv, NULL, NULL, &pp)) {
        printf("FAIL: parser rejected a valid log\n");
        fails++;
    } else if (seed != 1 || ops != 1 || nv != 3 || pp != -1) {
        printf("FAIL: parser fields wrong seed=%" PRIx64 " ops=%u nv=%u pois=%d\n",
               seed, ops, nv, pp);
        fails++;
    }
    if (fails == 0) {
        printf("PASS: lc_parse_reject_test\n");
    }
    return fails;
}

/* Replay must reject a value stream that does not match the code path: a stream
 * shorter than the run consumes (underflow) or one with unconsumed trailing
 * values on an otherwise-clean run. Both must fail loudly and leak nothing. */
static int
lc_replay_underflow_test(void)
{
    int fails = 0;
    uint64_t seed = 0x5151ABCDull;
    uint32_t ops = 40;
    lc_faults_t f = lc_derive_faults(seed, 0, false);
    ca_t a1;
    ca_init(&a1);
    lc_result_t g;
    run_one(seed, ops, &f, &a1, &g, NULL);
    if (g.failures != 0 || a1.live != 0 || g.log_n < 8) {
        printf("FAIL: lc_replay_underflow_test precondition (fail=%d live=%ld "
               "n=%u)\n",
               g.failures, a1.live, g.log_n);
        lc_result_free_log(&g);
        return 1;
    }

    /* Too short: the run reads past the recorded stream -> underflow. (.poison
     * = -1: this test exercises stream desync, not the synthetic failure arm.) */
    lc_replay_t rshort = {
        .vals = g.log_vals, .n = g.log_n - 4, .quiet = true, .poison = -1
    };
    ca_t a2;
    ca_init(&a2);
    lc_result_t rr2;
    run_one(seed, ops, &f, &a2, &rr2, &rshort);
    if (rr2.failures == 0) {
        printf("FAIL: truncated replay did not fail (underflow undetected)\n");
        fails++;
    }
    if (a2.live != 0) {
        printf("FAIL: underflow replay leaked %ld\n", a2.live);
        fails++;
    }

    /* Too long: an extra trailing value is left unconsumed on the clean run. */
    uint64_t *ext = malloc((size_t)(g.log_n + 1) * sizeof(uint64_t));
    if (ext == NULL) {
        printf("FAIL: lc_replay_underflow_test alloc\n");
        lc_result_free_log(&g);
        return fails + 1;
    }
    memcpy(ext, g.log_vals, (size_t)g.log_n * sizeof(uint64_t));
    ext[g.log_n] = 0;
    lc_replay_t rlong = {
        .vals = ext, .n = g.log_n + 1, .quiet = true, .poison = -1
    };
    ca_t a3;
    ca_init(&a3);
    lc_result_t rr3;
    run_one(seed, ops, &f, &a3, &rr3, &rlong);
    if (rr3.failures == 0) {
        printf("FAIL: over-long replay did not fail (leftover undetected)\n");
        fails++;
    }
    if (a3.live != 0) {
        printf("FAIL: leftover replay leaked %ld\n", a3.live);
        fails++;
    }
    free(ext);
    lc_result_free_log(&g);
    if (fails == 0) {
        printf("PASS: lc_replay_underflow_test\n");
    }
    return fails;
}

/* Replay a saved op-log (MOQ_LIFECYCLE_REPLAY=<file>): parse it, drive the same
 * production binding through the exact operation sequence, report the outcome.
 * Returns process exit code (0 = replay reproduced a clean run). */
static int
lc_replay_from_file(const char *path)
{
    FILE *rf = fopen(path, "r");
    if (rf == NULL) {
        printf("FAIL: cannot open replay log %s\n", path);
        return 1;
    }
    fseek(rf, 0, SEEK_END);
    long sz = ftell(rf);
    rewind(rf);
    if (sz <= 0) {
        printf("FAIL: empty replay log %s\n", path);
        fclose(rf);
        return 1;
    }
    char *buf = malloc((size_t)sz + 1);
    if (buf == NULL) {
        printf("FAIL: replay log alloc (%s)\n", path);
        fclose(rf);
        return 1;
    }
    size_t rd = fread(buf, 1, (size_t)sz, rf);
    buf[rd] = '\0';
    fclose(rf);

    unsigned ops_peek = 0;
    uint64_t seed_peek = 0;
    if (sscanf(buf, "LCLOG1 seed=%" SCNx64 " ops=%u", &seed_peek, &ops_peek) !=
        2) {
        printf("FAIL: malformed replay header in %s\n", path);
        free(buf);
        return 1;
    }
    (void)seed_peek;
    uint32_t vcap = ops_peek * 32u + 64u;
    uint64_t *vals = malloc((size_t)vcap * sizeof(uint64_t));
    if (vals == NULL) {
        printf("FAIL: replay value alloc (%s)\n", path);
        free(buf);
        return 1;
    }
    lc_faults_t pf;
    uint64_t seed = 0;
    uint32_t ops = 0, nv = 0;
    int poison = -1;
    if (!lc_log_parse(buf, &pf, &seed, &ops, vals, vcap, &nv, NULL, NULL,
                      &poison)) {
        printf("FAIL: malformed replay log %s\n", path);
        free(buf);
        free(vals);
        return 1;
    }
    /* Standalone: the failure arm comes from the LOG (poison=), never the env —
     * so `MOQ_LIFECYCLE_REPLAY=<file>` reproduces regardless of the environment. */
    lc_replay_t rp = { .vals = vals, .n = nv, .poison = poison };
    ca_t a;
    ca_init(&a);
    lc_result_t r;
    run_one(seed, ops, &pf, &a, &r, &rp);
    printf("REPLAY seed 0x%" PRIx64 " ops %u: failures=%d live=%ld\n", seed, ops,
           r.failures, a.live);
    int rc = (r.failures != 0 || a.live != 0) ? 1 : 0;
    free(buf);
    free(vals);
    return rc;
}

/* -- Op-log minimization / shrink ----------------------------------------- */

/* An op segment: a contiguous slice of the immutable parsed value stream. The
 * minimizer holds a compactible array of these and deletes whole ops; it never
 * touches values within an op (each op keeps its values + printed kind). */
typedef struct {
    const uint64_t *p;
    uint32_t        n;
} lc_seg_t;

/* Flatten setup values + the surviving op segments into one replay stream. */
static uint32_t
lc_flatten(uint64_t *dst, const uint64_t *setup, uint32_t setup_n,
           const lc_seg_t *segs, uint32_t n_segs)
{
    uint32_t k = 0;
    for (uint32_t i = 0; i < setup_n; i++) {
        dst[k++] = setup[i];
    }
    for (uint32_t s = 0; s < n_segs; s++) {
        for (uint32_t i = 0; i < segs[s].n; i++) {
            dst[k++] = segs[s].p[i];
        }
    }
    return k;
}

/* Minimization predicate: does (setup + n_segs ops) still reproduce the ORIGINAL
 * failure under an exact single-log replay? True iff replay yields a genuine
 * failure or a leak AND consumed the stream exactly. A deletion that desyncs the
 * positional stream (under/overshoot) is NOT a valid reproducer — rejecting it
 * keeps the minimizer from slipping onto a replay artifact instead of the bug. */
static bool
lc_reproduces(uint64_t seed, const lc_faults_t *f, int poison,
              const uint64_t *setup, uint32_t setup_n, const lc_seg_t *segs,
              uint32_t n_segs, uint64_t *scratch)
{
    uint32_t nv = lc_flatten(scratch, setup, setup_n, segs, n_segs);
    lc_replay_t rp = { .vals = scratch, .n = nv, .quiet = true, .poison = poison };
    ca_t a;
    ca_init(&a);
    lc_result_t r;
    run_one(seed, n_segs, f, &a, &r, &rp);
    return !r.replay_desync && (r.genuine_failures != 0 || a.live != 0);
}

/* Deterministic delta-shrink: delete op ranges of size `chunk`, halving down to
 * a final single-op pass, keeping any deletion that still reproduces. Compacts
 * segs[] in place and returns the minimized op count. Bounded by `attempt_cap`
 * (0 = unbounded); logs progress when `verbose`. */
static uint32_t
lc_shrink(uint64_t seed, const lc_faults_t *f, int poison, const uint64_t *setup,
          uint32_t setup_n, lc_seg_t *segs, uint32_t n_segs, uint64_t *scratch,
          lc_seg_t *trial, uint32_t attempt_cap, bool verbose)
{
    uint32_t attempts = 0;
    uint32_t chunk = n_segs / 2u;
    if (chunk < 1u) {
        chunk = 1u;
    }
    for (;;) {
        bool any = false;
        uint32_t i = 0;
        while (i < n_segs && n_segs > 1u) {
            uint32_t rm = chunk < (n_segs - i) ? chunk : (n_segs - i);
            uint32_t t = 0;
            for (uint32_t j = 0; j < n_segs; j++) {
                if (j < i || j >= i + rm) {
                    trial[t++] = segs[j];
                }
            }
            attempts++;
            if (lc_reproduces(seed, f, poison, setup, setup_n, trial, t,
                              scratch)) {
                memcpy(segs, trial, (size_t)t * sizeof(lc_seg_t));
                n_segs = t;
                any = true;
                if (verbose) {
                    printf("  shrink: removed %u op(s) at %u -> %u ops\n", rm, i,
                           n_segs);
                }
                /* i stays: the next window slides into position i */
            } else {
                i += rm;
            }
            if (attempt_cap != 0 && attempts >= attempt_cap) {
                if (verbose) {
                    printf("  shrink: attempt cap %u reached, stopping at %u "
                           "ops\n",
                           attempt_cap, n_segs);
                }
                goto done;
            }
        }
        if (chunk == 1u) {
            if (!any) {
                break; /* a full single-op pass removed nothing: minimal */
            }
        } else {
            chunk /= 2u;
            if (chunk < 1u) {
                chunk = 1u;
            }
        }
    }
done:
    if (verbose) {
        printf("  shrink: %u candidate replays\n", attempts);
    }
    return n_segs;
}

/* Minimize a parsed failing log. `poison` is the synthetic-failure arm carried by
 * the log (-1 = off); it drives the predicate AND is stamped into `out` so the
 * emitted log reproduces standalone. On success fills `out` with a serializable
 * minimized log (caller frees via lc_result_free_log) and returns its op count.
 * Returns -1 (out left zeroed) if the input does not reproduce under single-log
 * replay — e.g. a divergence-only seed-pair failure, which is out of scope — and
 * -2 on an allocation failure (both reported loudly by the caller). */
static int
lc_minimize(uint64_t seed, const lc_faults_t *f, int poison,
            const uint64_t *vals, uint32_t nv, uint32_t setup_end,
            const uint32_t *op_start, uint32_t ops, lc_result_t *out,
            uint32_t attempt_cap, bool verbose)
{
    memset(out, 0, sizeof(*out));
    out->poison = -1;
    /* All working buffers up front, checked together: on OOM we fail loudly
     * (-2) rather than dereference NULL on an unusable artifact. */
    lc_seg_t *segs = malloc((size_t)(ops ? ops : 1u) * sizeof(lc_seg_t));
    uint64_t *scratch = malloc((size_t)(nv ? nv : 1u) * sizeof(uint64_t));
    lc_seg_t *trial = malloc((size_t)(ops ? ops : 1u) * sizeof(lc_seg_t));
    if (segs == NULL || scratch == NULL || trial == NULL) {
        printf("FAIL: lc_minimize working-set allocation failed (ops=%u)\n", ops);
        free(segs);
        free(scratch);
        free(trial);
        return -2;
    }
    for (uint32_t i = 0; i < ops; i++) {
        segs[i].p = vals + op_start[i];
        segs[i].n = op_start[i + 1] - op_start[i];
    }
    const uint64_t *setup = vals; /* setup values are vals[0, setup_end) */

    if (!lc_reproduces(seed, f, poison, setup, setup_end, segs, ops, scratch)) {
        free(segs);
        free(scratch);
        free(trial);
        return -1; /* input does not reproduce under single-log replay */
    }
    uint32_t n_min = lc_shrink(seed, f, poison, setup, setup_end, segs, ops,
                               scratch, trial, attempt_cap, verbose);

    /* Materialize the minimized log (setup + surviving segments) into `out`. */
    uint32_t out_nv = setup_end;
    for (uint32_t i = 0; i < n_min; i++) {
        out_nv += segs[i].n;
    }
    out->log_vals = malloc((size_t)(out_nv ? out_nv : 1u) * sizeof(uint64_t));
    out->op_start = malloc(((size_t)n_min + 1u) * sizeof(uint32_t));
    if (out->log_vals == NULL || out->op_start == NULL) {
        printf("FAIL: lc_minimize output allocation failed (ops=%u)\n", n_min);
        free(out->log_vals);
        free(out->op_start);
        out->log_vals = NULL;
        out->op_start = NULL;
        free(segs);
        free(scratch);
        free(trial);
        return -2;
    }
    uint32_t k = 0;
    for (uint32_t i = 0; i < setup_end; i++) {
        out->log_vals[k++] = vals[i];
    }
    for (uint32_t i = 0; i < n_min; i++) {
        out->op_start[i] = k;
        for (uint32_t j = 0; j < segs[i].n; j++) {
            out->log_vals[k++] = segs[i].p[j];
        }
    }
    out->op_start[n_min] = k;
    out->log_n = k;
    out->setup_end = setup_end;
    out->ops = n_min;
    out->seed = seed;
    out->faults = *f;
    out->poison = poison;   /* rides the emitted log => standalone-replayable */
    free(segs);
    free(scratch);
    free(trial);
    return (int)n_min;
}

/* Manufacture a synthetic failing log (poison at op T over a green seed),
 * minimize it, and prove: a clean input is refused; the minimized log is
 * strictly smaller (exactly T+1 ops); and — the load-bearing property Codex
 * flagged — it is STANDALONE-replayable: serialized, strict-parsed back, and
 * replayed with the process-global poison arm cleared, it still reproduces
 * because poison= rides the log. A negative control (replay with poison=-1)
 * must NOT reproduce, proving the arm is sourced from the artifact, not luck. */
static int
lc_minimize_selftest(void)
{
    int fails = 0;
    uint64_t seed = 0x51ABCDE5ull;
    uint32_t ops = 24;
    int T = 4; /* poison at op T => minimal reproducer is exactly T+1 ops */
    lc_faults_t f = lc_derive_faults(seed, 0, false);
    /* Hermetic: the arm is driven as DATA here, never via the global — force it
     * off so a stray MOQ_LIFECYCLE_POISON_AT in the environment can't taint the
     * baseline green-log generation. Restored before returning. */
    int saved_poison = g_lc_poison_at_op;
    g_lc_poison_at_op = -1;

    ca_t ag;
    ca_init(&ag);
    lc_result_t g;
    run_one(seed, ops, &f, &ag, &g, NULL); /* clean full log (poison disabled) */
    if (g.failures != 0 || ag.live != 0 || g.ops != ops || g.poison != -1) {
        printf("FAIL: lc_minimize_selftest baseline (fail=%d live=%ld ops=%u "
               "poison=%d)\n",
               g.failures, ag.live, g.ops, g.poison);
        lc_result_free_log(&g);
        g_lc_poison_at_op = saved_poison;
        return 1;
    }

    /* (a) A clean input (poison=-1) must be refused: no single-log reproducer. */
    lc_result_t refj;
    int rc0 = lc_minimize(g.seed, &g.faults, -1, g.log_vals, g.log_n,
                          g.setup_end, g.op_start, g.ops, &refj, 0, false);
    if (rc0 != -1) {
        printf("FAIL: minimizer accepted a non-reproducing input (rc=%d)\n", rc0);
        fails++;
    }
    lc_result_free_log(&refj);

    /* (b) Minimize the same recorded stream WITH the failure arm (poison=T). The
     * arm is passed as data — no process-global is touched — so the shrunk log
     * carries poison=T in its header. */
    lc_result_t m;
    int rc = lc_minimize(g.seed, &g.faults, T, g.log_vals, g.log_n, g.setup_end,
                         g.op_start, g.ops, &m, 0, false);
    if (rc < 0) {
        printf("FAIL: minimizer refused a reproducing input (rc=%d)\n", rc);
        fails++;
    } else {
        if (m.ops >= g.ops) {
            printf("FAIL: minimized not smaller (%u -> %u)\n", g.ops, m.ops);
            fails++;
        }
        if (m.ops != (uint32_t)T + 1u) {
            printf("FAIL: minimized op count %u != expected %d\n", m.ops, T + 1);
            fails++;
        }

        /* Standalone proof: serialize, strict-parse back (enforces ops= and
         * sequential op <i> lines AND round-trips poison=), then replay with the
         * process-global arm explicitly disabled. This is exactly the
         * `MOQ_LIFECYCLE_REPLAY=<file>` path with a hostile (cleared) env — the
         * regression guard for Codex's finding. */
        g_lc_poison_at_op = -1; /* prove the arm rides the LOG, not this global */
        FILE *tf = tmpfile();
        if (tf == NULL) {
            printf("FAIL: lc_minimize_selftest tmpfile\n");
            fails++;
        } else {
            lc_log_serialize(&m, tf);
            long sz = ftell(tf);
            rewind(tf);
            char *buf = malloc((size_t)sz + 1);
            if (buf == NULL) {
                printf("FAIL: lc_minimize_selftest alloc\n");
                fclose(tf);
                fails++;
            } else {
                size_t rd = fread(buf, 1, (size_t)sz, tf);
                buf[rd] = '\0';
                fclose(tf);
                lc_faults_t pf;
                uint64_t ps = 0;
                uint32_t po = 0, pn = 0;
                int ppoison = 0;
                uint64_t pv[256];
                bool ok = lc_log_parse(buf, &pf, &ps, &po, pv, 256, &pn, NULL,
                                       NULL, &ppoison);
                if (!ok || po != m.ops || ppoison != T) {
                    printf("FAIL: minimized log parse-back (ok=%d ops=%u/%u "
                           "poison=%d/%d)\n",
                           (int)ok, po, m.ops, ppoison, T);
                    fails++;
                }
                /* replay from the parsed log — poison from the ARTIFACT */
                lc_replay_t rp = {
                    .vals = pv, .n = pn, .quiet = true, .poison = ppoison
                };
                ca_t am;
                ca_init(&am);
                lc_result_t rr;
                run_one(ps, po, &pf, &am, &rr, &rp);
                if (rr.replay_desync ||
                    (rr.genuine_failures == 0 && am.live == 0)) {
                    printf("FAIL: minimized log not standalone-reproducible "
                           "(desync=%d gf=%d live=%ld)\n",
                           (int)rr.replay_desync, rr.genuine_failures, am.live);
                    fails++;
                }
                if (am.live != 0) {
                    printf("FAIL: standalone replay leaked %ld\n", am.live);
                    fails++;
                }

                /* Negative control: same log, poison arm forced off -> the
                 * GENUINE (poison-sourced) failure must vanish, proving poison=
                 * is what drives the reproduction. (A benign stream desync may
                 * appear when replaying an op subset without the recorded arm;
                 * that is not a genuine reproduction, so gate on
                 * genuine_failures, not total failures.) */
                lc_replay_t rn = {
                    .vals = pv, .n = pn, .quiet = true, .poison = -1
                };
                ca_t an;
                ca_init(&an);
                lc_result_t rrn;
                run_one(ps, po, &pf, &an, &rrn, &rn);
                if (rrn.genuine_failures != 0 || an.live != 0) {
                    printf("FAIL: negative control had a genuine failure without "
                           "poison (gf=%d live=%ld)\n",
                           rrn.genuine_failures, an.live);
                    fails++;
                }
                free(buf);
            }
        }
        lc_result_free_log(&m);
    }
    g_lc_poison_at_op = saved_poison; /* restore the incoming env arm, if any */
    lc_result_free_log(&g);
    if (fails == 0) {
        printf("PASS: lc_minimize_selftest\n");
    }
    return fails;
}

/* MOQ_LIFECYCLE_MINIMIZE=<file>: parse a failing LCLOG1, shrink it, and emit the
 * minimized log (to MOQ_LIFECYCLE_MINIMIZE_OUT if set, else stdout) with a
 * replay command. Returns 0 on success, 1 if the input cannot be read/parsed or
 * does not reproduce under single-log replay. */
static int
lc_minimize_from_file(const char *path)
{
    FILE *rf = fopen(path, "r");
    if (rf == NULL) {
        printf("FAIL: cannot open minimize input %s\n", path);
        return 1;
    }
    fseek(rf, 0, SEEK_END);
    long sz = ftell(rf);
    rewind(rf);
    if (sz <= 0) {
        printf("FAIL: empty minimize input %s\n", path);
        fclose(rf);
        return 1;
    }
    char *buf = malloc((size_t)sz + 1);
    if (buf == NULL) {
        printf("FAIL: minimize input alloc (%s)\n", path);
        fclose(rf);
        return 1;
    }
    size_t rd = fread(buf, 1, (size_t)sz, rf);
    buf[rd] = '\0';
    fclose(rf);

    unsigned ops_peek = 0;
    uint64_t seed_peek = 0;
    if (sscanf(buf, "LCLOG1 seed=%" SCNx64 " ops=%u", &seed_peek, &ops_peek) !=
        2) {
        printf("FAIL: malformed minimize header in %s\n", path);
        free(buf);
        return 1;
    }
    (void)seed_peek;
    uint32_t vcap = ops_peek * 32u + 64u;
    uint64_t *vals = malloc((size_t)vcap * sizeof(uint64_t));
    uint32_t *op_start = malloc(((size_t)ops_peek + 1u) * sizeof(uint32_t));
    if (vals == NULL || op_start == NULL) {
        printf("FAIL: minimize working alloc (%s)\n", path);
        free(buf);
        free(vals);
        free(op_start);
        return 1;
    }
    lc_faults_t pf;
    uint64_t seed = 0;
    uint32_t ops = 0, nv = 0, setup_end = 0;
    int poison = -1;
    if (!lc_log_parse(buf, &pf, &seed, &ops, vals, vcap, &nv, &setup_end,
                      op_start, &poison)) {
        printf("FAIL: malformed minimize input %s\n", path);
        free(buf);
        free(vals);
        free(op_start);
        return 1;
    }
    /* The failure arm rides the log (poison=). MOQ_LIFECYCLE_POISON_AT may
     * OVERRIDE it, which is how a captured GREEN log is turned into a failing
     * one to minimize — the override is then stamped into the output header so
     * the minimized artifact stays standalone. */
    const char *pe = getenv("MOQ_LIFECYCLE_POISON_AT");
    if (pe != NULL) {
        poison = (int)strtol(pe, NULL, 0);
    }

    uint32_t cap = 0;
    const char *ce = getenv("MOQ_LIFECYCLE_MIN_ATTEMPTS");
    if (ce != NULL) {
        cap = (uint32_t)strtoul(ce, NULL, 0);
    }
    lc_result_t m;
    int rc = lc_minimize(seed, &pf, poison, vals, nv, setup_end, op_start, ops,
                         &m, cap, true);
    free(buf);
    free(vals);
    free(op_start);
    if (rc == -2) {
        printf("FAIL: minimizer allocation failure on %s\n", path);
        return 1;
    }
    if (rc < 0) {
        printf("FAIL: input does not reproduce under single-log replay: %s\n"
               "  (divergence-only seed-pair failures are out of scope)\n",
               path);
        return 1;
    }
    printf("MINIMIZED: %u ops -> %u ops (seed 0x%" PRIx64 ")\n", ops, m.ops,
           seed);
    const char *outp = getenv("MOQ_LIFECYCLE_MINIMIZE_OUT");
    if (outp != NULL) {
        FILE *of = fopen(outp, "w");
        if (of == NULL) {
            printf("FAIL: cannot write minimized log to %s\n", outp);
            lc_result_free_log(&m);
            return 1;
        }
        lc_log_serialize(&m, of);
        fclose(of);
        printf("MINIMIZED-LOG: MOQ_LIFECYCLE_REPLAY=%s ./test_relay_lifecycle\n",
               outp);
    } else {
        printf("--- minimized op-log (seed 0x%" PRIx64 ") — save and replay with "
               "MOQ_LIFECYCLE_REPLAY=<file> ---\n",
               seed);
        lc_log_serialize(&m, stdout);
    }
    lc_result_free_log(&m);
    return 0;
}

int
main(void)
{
    /* Test-only synthetic-failure arm (defaults disabled). Lets a
     * human manufacture a failing log for the MINIMIZE/REPLAY paths. The arm is
     * SERIALIZED into the log (poison=<T> header), so a captured/minimized log
     * reproduces standalone — MOQ_LIFECYCLE_REPLAY/MINIMIZE read the arm from the
     * FILE, never from this global. Two routes: (a) capture WITH the arm
     * (MOQ_LIFECYCLE_POISON_AT=<T> MOQ_LIFECYCLE_LOG=1 -> the dumped log already
     * carries poison=T); (b) minimize a captured GREEN log with
     * MOQ_LIFECYCLE_POISON_AT=<T> as an override (stamped into the output). This
     * global only seeds GENERATE-mode runs (capture); replay/minimize are
     * file-driven. */
    const char *pe = getenv("MOQ_LIFECYCLE_POISON_AT");
    if (pe != NULL) {
        g_lc_poison_at_op = (int)strtol(pe, NULL, 0);
    }
    const char *replay_path = getenv("MOQ_LIFECYCLE_REPLAY");
    if (replay_path != NULL) {
        return lc_replay_from_file(replay_path);
    }
    const char *min_path = getenv("MOQ_LIFECYCLE_MINIMIZE");
    if (min_path != NULL) {
        return lc_minimize_from_file(min_path);
    }
    uint64_t seeds = 25, seed_start = 0;
    uint32_t ops = 200;
    uint32_t force_actions = 0;
    const char *e;
    if ((e = getenv("MOQ_LIFECYCLE_SEEDS")) != NULL) {
        seeds = strtoull(e, NULL, 0);
    }
    if ((e = getenv("MOQ_LIFECYCLE_SEED_START")) != NULL) {
        seed_start = strtoull(e, NULL, 0);
    }
    if ((e = getenv("MOQ_LIFECYCLE_OPS")) != NULL) {
        ops = (uint32_t)strtoul(e, NULL, 0);
    }
    if ((e = getenv("MOQ_LIFECYCLE_MAX_ACTIONS")) != NULL) {
        force_actions = (uint32_t)strtoul(e, NULL, 0);
    }
    /* The fault matrix runs by DEFAULT (small, moderate queues — green);
     * MOQ_LIFECYCLE_NO_FAULTS disables it, MOQ_LIFECYCLE_MAX_ACTIONS /
     * MOQ_LIFECYCLE_CLOSE_PRESSURE make it more adversarial. */
    bool faults = getenv("MOQ_LIFECYCLE_NO_FAULTS") == NULL;
    bool close_pressure = getenv("MOQ_LIFECYCLE_CLOSE_PRESSURE") != NULL;

    int failures = 0;
    failures += lc_replay_selftest();      /* generate == replay               */
    failures += lc_parse_reject_test();    /* parser rejects bad logs          */
    failures += lc_replay_underflow_test(); /* replay stream mismatch          */
    failures += lc_minimize_selftest();    /* op-log shrink                    */
    uint32_t ex_total[EX__COUNT];
    memset(ex_total, 0, sizeof(ex_total));

    /* Baseline sweep (no faults): gates the event-surface accounting and the
     * clean-path invariants. */
    lc_faults_t nof;
    memset(&nof, 0, sizeof(nof));
    for (uint64_t s = seed_start; s < seed_start + seeds; s++) {
        uint64_t seed = 0x11FEC0DEull ^ (s * 0x9E3779B97F4A7C15ull);
        failures += run_seed_pair(s, seed, ops, &nof, ex_total);
    }
    for (int i = 0; i < EX__COUNT; i++) {
        if (ex_total[i] == 0) {
            printf("FAIL: lifecycle event %s was never exercised across the "
                   "baseline sweep\n",
                   ex_name[i]);
            failures++;
        }
    }

    /* Fault matrix: smaller sweep under capacity + action-queue + close
     * pressure. Gates the pressure invariants (terminal progress, no hang, no
     * divergence, no leak, wire refusal for capacity) — NOT event coverage,
     * which the baseline owns (pressure can legitimately shrink some counts).
     * Every requirement is either met on the wire or preserved as a failing
     * seed with a fault-tagged replay line. */
    uint32_t fault_ex[EX__COUNT];
    memset(fault_ex, 0, sizeof(fault_ex));
    uint64_t fault_seeds = 0;
    if (faults) {
        fault_seeds = seeds < 12 ? seeds : 12;
        if ((e = getenv("MOQ_LIFECYCLE_FAULT_SEEDS")) != NULL) {
            fault_seeds = strtoull(e, NULL, 0);
        }
        uint32_t fops = ops < 120 ? ops : 120;
        for (uint64_t s = seed_start; s < seed_start + fault_seeds; s++) {
            uint64_t seed = 0xFA017C0DEull ^ (s * 0x9E3779B97F4A7C15ull);
            lc_faults_t f = lc_derive_faults(seed, force_actions, close_pressure);
            failures += run_seed_pair(s, seed, fops, &f, fault_ex);
        }
    }

    /* Depth-1 action-queue gate: the tightest fire-and-forget pressure — one
     * in-flight session action, so any output that drops on WOULD_BLOCK leaves
     * a peer waiting forever. This is where the borrowed-remainder bug lived;
     * durable intent retry now holds it, so it is a STANDING gate (a regression
     * to fire-and-forget fails the default run). Skipped only when the depth is
     * already env-forced (the caller drives their own pressure) or faults are
     * off. */
    uint32_t depth1_ex[EX__COUNT];
    memset(depth1_ex, 0, sizeof(depth1_ex));
    uint64_t depth1_seeds = 0;
    if (faults && force_actions == 0) {
        depth1_seeds = fault_seeds < 12 ? fault_seeds : 12;
        uint32_t fops = ops < 120 ? ops : 120;
        for (uint64_t s = seed_start; s < seed_start + depth1_seeds; s++) {
            uint64_t seed = 0xD3E97C0DEull ^ (s * 0x9E3779B97F4A7C15ull);
            lc_faults_t f = lc_derive_faults(seed, 1u, close_pressure);
            failures += run_seed_pair(s, seed, fops, &f, depth1_ex);
        }
    }

    if (failures == 0) {
        printf("PASS: lifecycle (baseline %" PRIu64 " seeds x %u ops",
               seeds, ops);
        if (faults) {
            printf(" + fault matrix %" PRIu64 " seeds%s", fault_seeds,
                   close_pressure ? " close-pressure" : "");
        }
        if (depth1_seeds) {
            printf(" + depth-1 gate %" PRIu64 " seeds", depth1_seeds);
        }
        printf(", run-twice)\n");
        printf("  exercised:");
        for (int i = 0; i < EX__COUNT; i++) {
            printf(" %s=%u", ex_name[i], ex_total[i]);
        }
        printf("\n");
        if (faults) {
            printf("  fault-exercised:");
            for (int i = 0; i < EX__COUNT; i++) {
                printf(" %s=%u", ex_name[i], fault_ex[i]);
            }
            printf("\n");
        }
    } else {
        printf("FAIL: lifecycle: %d total failures\n", failures);
    }
    /* Exit status truncates to 8 bits: a raw count that is a multiple of 256
     * would read as success. Clamp. */
    return failures == 0 ? 0 : 1;
}
