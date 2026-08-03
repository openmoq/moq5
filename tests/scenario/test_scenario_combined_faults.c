/*
 * Combined allocation + transport fault injection scenario runner, with
 * CAUSAL result classification.
 *
 * Exercises publisher facade + SimPair under seeded allocator failures
 * and seeded transport faults (drop / mutate / reorder / inject / delay /
 * split / truncate) in the same run. Injection policy, call scopes,
 * unconditional unscoped-attempt latching, non-injecting event polls,
 * the typed per-operation classifiers, the endpoint-split close
 * attribution, and the exact namespace lifecycle all follow
 * test_scenario_faults.c; the call-contract table there applies
 * unchanged. The combined-lane deltas:
 *
 *   Transport faults occur only inside pump/advance scopes (they fire at
 *   action-delivery decision time), so every fault is attributable to
 *   the exact call that observed it, PER DIRECTION: each fault record
 *   carries its receiver, and the summary keeps divergence-proving
 *   fault counts toward each endpoint separately. Every injected fault
 *   -- RESET, STOP and CLOSE alike -- propagates a hard delivery error
 *   out of the pump, so an allocation failure inside any injection feed
 *   surfaces in the pump's own result.
 *
 *   DIVERGENCE, per receiver. A drop, mutation, truncation, reorder, or
 *   injected stream/close fault toward an endpoint -- and a failed
 *   delivery toward it (SimPair consumes the failing action and cleans
 *   the rest of its batch) -- provably loses or corrupts wire state that
 *   endpoint should have seen. Prior divergence toward an endpoint is
 *   accepted ONLY as the cause of that endpoint's later protocol
 *   close or a PROTO result; it never authorizes WRONG_STATE,
 *   STALE_HANDLE, or any other ordinary API result, and divergence
 *   toward one endpoint never excuses the opposite endpoint. Delays and
 *   splits preserve content and order (per-carrier FIFO is forced) and
 *   prove nothing.
 *
 *   MOQ_ERR_PROTO from a pump is valid only when that call observed a
 *   negative PROTO delivery result AND corruption toward that same
 *   receiver was produced in that call or already proven (a delayed
 *   corrupted chunk delivers later than its fault record).
 *
 *   The namespace lifecycle is the same event-observed cycle model as
 *   the allocation lane: a cycle settles ONLY when the facade CONSUMES
 *   the announcer's own response event with this cycle's announcement
 *   handle, so a dropped, corrupted, or still-delayed response simply
 *   never settles it -- no fault- or delay-derived inference is
 *   involved.
 *
 * Each seed runs twice; trace hash, allocation counts, per-kind and
 * per-direction transport fault counts, and the causal outcome counts
 * must all match. Allocator balance must return to zero.
 */

#include <moq/publisher.h>
#include <moq/sim.h>
#include "../../tests/unit/test_support.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- splitmix64 PRNG ------------------------------------------------ */

typedef struct { uint64_t s; } rng_t;

static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

static uint64_t mix64(uint64_t z) {
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* -- Summary -------------------------------------------------------- */

typedef struct {
    uint64_t trace_hash;
    size_t   trace_count;
    uint64_t alloc_calls;
    uint64_t alloc_failures;
    size_t   drop_control;
    size_t   drop_data;
    size_t   drop_reset;
    size_t   drop_stop;
    size_t   mutate_control;
    size_t   mutate_data;
    size_t   reorder_action;
    size_t   inject_reset;
    size_t   inject_stop;
    size_t   inject_close;
    size_t   truncate_control;
    size_t   truncate_data;
    size_t   delay_enqueues;

    /* Causal accounting, compared across twin runs. */
    uint64_t scoped_calls;
    uint64_t injected_calls;
    uint64_t outcome_ok;
    uint64_t outcome_nomem;
    uint64_t outcome_wblock;
    uint64_t outcome_terminal;
    uint64_t unscoped_violations;
    uint64_t nested_violations;
    uint64_t pend_exact_calls;      /* track ops classified EXACT retry */
    uint64_t pend_divergent_calls;  /* track ops classified DIVERGENT */
    uint64_t pend_created_calls;    /* pending track ops CREATED (a write
                                     * or end-group NOMEM pended) */

    /* Per-direction observations: divergence-proving transport faults
     * and corruption toward each endpoint, and failed deliveries (the
     * consumed action + cleaned batch). */
    size_t div_to_client, div_to_server;
    size_t corrupt_to_client, corrupt_to_server;
    size_t loss_to_client, loss_to_server;

    /* Per-scope observation: first negative delivery result and its
     * receiver. */
    moq_result_t scope_neg_input;
    int          scope_neg_to;
} run_summary_t;

/* Every trace-observed transport fault class, for the coverage floor. */
static size_t observed_transport_faults(const run_summary_t *s) {
    return s->drop_control + s->drop_data + s->drop_reset + s->drop_stop +
           s->mutate_control + s->mutate_data + s->reorder_action +
           s->inject_reset + s->inject_stop + s->inject_close +
           s->truncate_control + s->truncate_data + s->delay_enqueues;
}

static size_t total_faults(const run_summary_t *s) {
    return (size_t)s->alloc_failures + observed_transport_faults(s);
}

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r) {
    run_summary_t *s = (run_summary_t *)ctx;
    uint64_t h = s->trace_hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
    s->trace_hash = h;
    s->trace_count++;

    bool to_client = r->to == MOQ_PERSPECTIVE_CLIENT;

    if (r->kind == MOQ_SIM_TRACE_INPUT && r->result < 0) {
        if (to_client) s->loss_to_client++;
        else           s->loss_to_server++;
        if (s->scope_neg_input == 0) {
            s->scope_neg_input = r->result;
            s->scope_neg_to = (int)r->to;
        }
    }

    bool divergent = false;
    bool corrupt = false;
    if (r->kind == MOQ_SIM_TRACE_FAULT_DROP) {
        divergent = true;
        switch (r->action_kind) {
        case MOQ_ACTION_SEND_CONTROL: s->drop_control++; break;
        case MOQ_ACTION_SEND_DATA:    s->drop_data++;    break;
        case MOQ_ACTION_RESET_DATA:   s->drop_reset++;   break;
        case MOQ_ACTION_STOP_DATA:    s->drop_stop++;    break;
        default: break;
        }
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_MUTATE) {
        divergent = true;
        corrupt = true;
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->mutate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->mutate_data++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_REORDER) {
        divergent = true;
        s->reorder_action++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_INJECT) {
        divergent = true;
        if (r->action_kind == MOQ_ACTION_RESET_DATA)
            s->inject_reset++;
        else if (r->action_kind == MOQ_ACTION_STOP_DATA)
            s->inject_stop++;
        else if (r->action_kind == MOQ_ACTION_CLOSE_SESSION)
            s->inject_close++;
    }
    if (r->kind == MOQ_SIM_TRACE_FAULT_TRUNCATE) {
        divergent = true;
        corrupt = true;
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->truncate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->truncate_data++;
    }
    if (r->kind == MOQ_SIM_TRACE_DELAY_ENQUEUE)
        s->delay_enqueues++;

    if (divergent) {
        if (to_client) s->div_to_client++;
        else           s->div_to_server++;
    }
    if (corrupt) {
        if (to_client) s->corrupt_to_client++;
        else           s->corrupt_to_server++;
    }
}

/* -- Fault allocator with one-shot call scopes ---------------------- */

typedef struct {
    uint64_t seed;
    uint64_t calls;
    uint64_t failures;
    uint64_t per_mille;
    int64_t  balance;
    bool     enabled;

    bool     in_scope;
    bool     suppressed;
    uint64_t scope_injects;
    uint64_t unscoped_violations;
    uint64_t nested_violations;
} fault_alloc_state_t;

static bool fault_should_fail(fault_alloc_state_t *s, size_t size,
                              uint64_t kind)
{
    s->calls++;
    if (!s->enabled)
        return false;
    if (s->suppressed)
        return false;               /* sanctioned non-injecting scope */
    if (!s->in_scope) {
        /* Latched unconditionally -- see test_scenario_faults.c. */
        s->unscoped_violations++;
        return false;
    }
    if (s->per_mille == 0)
        return false;
    if (s->scope_injects >= 1)
        return false;
    uint64_t x = s->seed;
    x ^= s->calls * 0xD6E8FEB86659FD93ULL;
    x ^= kind * 0xA0761D6478BD642FULL;
    x ^= (uint64_t)size * 0xE7037ED1A0B428DBULL;
    if ((mix64(x) % 1000) >= s->per_mille)
        return false;
    s->scope_injects++;
    s->failures++;
    return true;
}

static void *fa_alloc(size_t sz, void *ctx) {
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    if (fault_should_fail(s, sz, 1)) return NULL;
    void *p = malloc(sz);
    if (p) s->balance++;
    return p;
}

static void *fa_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx) {
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    (void)old_sz;
    if (new_sz == 0) {
        if (ptr) { s->balance--; free(ptr); }
        return NULL;
    }
    if (fault_should_fail(s, new_sz, 2)) return NULL;
    void *p = realloc(ptr, new_sz);
    if (p && !ptr) s->balance++;
    return p;
}

static void fa_free(void *ptr, size_t sz, void *ctx) {
    fault_alloc_state_t *s = (fault_alloc_state_t *)ctx;
    (void)sz;
    if (ptr) { s->balance--; free(ptr); }
}

/* -- Call scopes ---------------------------------------------------- */

static void scope_begin(fault_alloc_state_t *fa, run_summary_t *sum)
{
    if (fa->in_scope)
        fa->nested_violations++;
    fa->in_scope = true;
    fa->suppressed = false;
    fa->scope_injects = 0;
    sum->scoped_calls++;
    sum->scope_neg_input = 0;
    sum->scope_neg_to = 0;
}

static uint64_t scope_end(fault_alloc_state_t *fa, run_summary_t *sum)
{
    uint64_t inj = fa->scope_injects;
    fa->in_scope = false;
    fa->suppressed = false;
    fa->scope_injects = 0;
    if (inj > 0)
        sum->injected_calls++;
    return inj;
}

static void poll_scope_begin(fault_alloc_state_t *fa)
{
    if (fa->in_scope)
        fa->nested_violations++;
    fa->in_scope = true;
    fa->suppressed = true;
    fa->scope_injects = 0;
}

static void poll_scope_end(fault_alloc_state_t *fa)
{
    fa->in_scope = false;
    fa->suppressed = false;
}

/* -- Shadow model --------------------------------------------------- */

#define MAX_TRACKS 4

#define TRACK_PEND_NONE      0
#define TRACK_PEND_WRITE     1
#define TRACK_PEND_END_GROUP 2

/* Relationship of the call being classified to the track's retained
 * pending operation, per the facade admission peek: a byte-identical
 * retry RESUMES; a different operation is refused WRONG_STATE before
 * any mutation. */
typedef enum {
    PEND_REL_NONE = 0,     /* no pending operation on this track */
    PEND_REL_EXACT,        /* byte-identical retry of the pending op */
    PEND_REL_DIVERGENT,    /* a different operation against it */
} pend_rel_t;

typedef struct {
    bool             active;
    bool             has_sub;
    bool             group_open;
    int              pend_kind;      /* TRACK_PEND_*: the retained
                                      * retryable operation an injected
                                      * hard failure left behind; its
                                      * full identity (ids + bytes) is
                                      * kept so exact retries and
                                      * divergent calls classify
                                      * differently. Cleared when the
                                      * exact retry completes, on
                                      * removal, or teardown */
    uint64_t         pend_group;
    uint64_t         pend_object;
    uint8_t          pend_payload[16];
    size_t           pend_payload_len;
    bool             remove_pending;  /* an injected NOMEM interrupted
                                       * remove_track: the teardown state
                                       * is genuinely unobservable, so
                                       * later track ops accept OK or
                                       * WRONG_STATE until a remove retry
                                       * succeeds */
    bool             client_sub_live;
    uint64_t         client_sub;
    bool             advertised;
    int              ns_cycle;        /* the announce cycle this track's
                                       * advertisement joined */
    moq_pub_track_t *track;
    char             name[16];
    uint64_t         group_id;
    uint64_t         object_id;
} shadow_track_t;

/* The one namespace all tracks share: an explicit lifecycle cycle --
 * IDLE, then PENDING from the first advertised ref, then SETTLED only
 * once the announcer's own response event (NAMESPACE_ACCEPTED /
 * REJECTED / CANCELLED) was successfully handled, then IDLE again after
 * the last advertised track's successful removal. */
typedef enum {
    NS_PENDING = 1,        /* announce sent, response not yet handled */
    NS_LIVE,               /* NAMESPACE_ACCEPTED handled */
    NS_DEAD,               /* NAMESPACE_REJECTED / CANCELLED handled --
                            * the next advertised add starts a NEW cycle
                            * (a fresh announce with its own entry) even
                            * while older advertised tracks still hold
                            * the dead one */
} shadow_ns_state_t;

typedef struct {
    int               cur_cycle;   /* 0 = no cycle yet */
    shadow_ns_state_t cur_state;
    int               cur_refs;    /* advertised tracks in the CURRENT
                                    * cycle */
    /* The announcer-side announcement handle is facade-internal until
     * its first response event, so the current cycle's handle is
     * learned from that event; handles of settled past cycles are kept
     * so a stale late event can never settle the current cycle. */
    uint64_t          cur_ann;       /* 0 = not yet learned */
    uint64_t          old_ann[64];
    int               old_ann_count;
    bool              old_ann_overflow;  /* evidence store overflowed:
                                          * the run fails loudly */
} shadow_ns_t;

typedef struct {
    shadow_track_t tracks[MAX_TRACKS];
    shadow_ns_t    ns;
    uint64_t       next_track_id;
    uint64_t       subscribes_issued;
    bool           client_closed;
    bool           server_closed;
} shadow_state_t;

static bool shadow_closed(const shadow_state_t *st)
{
    return st->client_closed || st->server_closed;
}

static int pick_track(const shadow_state_t *st, rng_t *rng,
                      bool require_sub, bool require_group)
{
    int candidates[MAX_TRACKS];
    int n = 0;
    for (int i = 0; i < MAX_TRACKS; i++) {
        if (!st->tracks[i].active) continue;
        if (require_sub && !st->tracks[i].has_sub) continue;
        if (require_group && !st->tracks[i].group_open) continue;
        candidates[n++] = i;
    }
    if (n == 0) return -1;
    return candidates[rng_next(rng) % (uint64_t)n];
}

static void refresh_subs(moq_publisher_t *pub, shadow_state_t *st) {
    for (int i = 0; i < MAX_TRACKS; i++) {
        if (!st->tracks[i].active) continue;
        st->tracks[i].has_sub =
            moq_pub_active_subscriptions(pub, st->tracks[i].track) > 0;
        if (!st->tracks[i].has_sub)
            st->tracks[i].group_open = false;
    }
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP, OP_ADD_TRACK, OP_REMOVE_TRACK, OP_SUBSCRIBE,
    OP_WRITE_OBJECT, OP_END_GROUP, OP_DRAIN_EVENTS, OP_FLUSH,
    OP_ADVANCE_TIME, OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *n[] = {
        "PUMP","ADD_TRACK","RM_TRACK","SUBSCRIBE","WRITE_OBJ",
        "END_GROUP","DRAIN_EV","FLUSH","ADV_TIME",
    };
    return op < OP_COUNT ? n[op] : "?";
}

#define MAX_LOG 80
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *l, scenario_op_t op, uint64_t p) {
    if (l->n < MAX_LOG) { l->e[l->n].op = op; l->e[l->n].p = p; l->n++; }
}

/* -- Causal classification ------------------------------------------ */

typedef struct {
    uint64_t     inj;
    moq_result_t neg_input;
    int          neg_to;
    bool         c_pre, s_pre;
    bool         c_post, s_post;
    /* Prior (pre-call) divergence + loss toward each endpoint, and the
     * in-call deltas. */
    size_t       divloss_c_pre, divloss_s_pre;
    size_t       divloss_c_delta, divloss_s_delta;
    size_t       corrupt_c_pre, corrupt_s_pre;
    size_t       corrupt_c_delta, corrupt_s_delta;
} callobs_t;

typedef struct scen {
    moq_simpair_t       *sp;
    moq_publisher_t     *pub;
    fault_alloc_state_t *fa;
    run_summary_t       *sum;
    shadow_state_t      *st;
    op_log_t            *log;
    uint64_t             seed;
    int                  run_id;
    int                  max_steps;
    uint64_t             alloc_pm;
    uint64_t             transport_pm;
    int                  failures;
    bool                 quiet;
} scen_t;

static void dump_log(const scen_t *sc) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)sc->seed, sc->run_id);
    for (size_t i = 0; i < sc->log->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(sc->log->e[i].op),
                (unsigned long long)sc->log->e[i].p);
    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
            "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=%llu "
            "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=%llu "
            "./build/tests/test_scenario_combined_faults\n",
            (unsigned long long)sc->seed, sc->max_steps,
            (unsigned long long)sc->alloc_pm,
            (unsigned long long)sc->transport_pm);
}

static void causal_fail(scen_t *sc, const char *where, moq_result_t rc,
                        const callobs_t *o, const char *why)
{
    sc->failures++;
    if (sc->quiet) return;
    fprintf(stderr, "FAIL seed=0x%llx run=%d: %s rc=%d inj=%llu "
            "neg=%d/to=%d closed(c=%d s=%d) divloss(c=%zu+%zu s=%zu+%zu): %s\n",
            (unsigned long long)sc->seed, sc->run_id, where, (int)rc,
            (unsigned long long)o->inj, (int)o->neg_input, o->neg_to,
            (int)o->c_pre, (int)o->s_pre,
            o->divloss_c_pre, o->divloss_c_delta,
            o->divloss_s_pre, o->divloss_s_delta, why);
    dump_log(sc);
}

/*
 * Per-endpoint close-transition attribution. An endpoint's transition
 * needs an injection in this call, a failed delivery or a divergence-
 * proving fault toward THAT endpoint in this call, prior proven
 * divergence toward THAT endpoint (protocol-consequence close), or the
 * peer endpoint closed before or during the call. Divergence toward one
 * endpoint never excuses the other.
 */
static void check_close_transitions(scen_t *sc, const char *where,
                                    const callobs_t *o)
{
    struct { bool pre, post; int persp; size_t prior, delta;
             bool peer_pre; } ep[2] = {
        { o->c_pre, o->c_post, MOQ_PERSPECTIVE_CLIENT,
          o->divloss_c_pre, o->divloss_c_delta, o->s_pre },
        { o->s_pre, o->s_post, MOQ_PERSPECTIVE_SERVER,
          o->divloss_s_pre, o->divloss_s_delta, o->c_pre },
    };
    /* Independent roots first; a peer's NEW close may only explain a
     * cascade when that peer's own transition was independently
     * justified -- two new closes cannot justify each other. */
    bool root[2];
    for (int i = 0; i < 2; i++)
        root[i] = (!ep[i].pre && ep[i].post) &&
            (o->inj > 0 ||
             (o->neg_input < 0 && o->neg_to == ep[i].persp) ||
             ep[i].delta > 0 || ep[i].prior > 0 ||
             ep[i].peer_pre);
    for (int i = 0; i < 2; i++) {
        if (ep[i].pre || !ep[i].post) continue;
        if (!root[i] && !root[1 - i])
            causal_fail(sc, where, 0, o,
                        i == 0 ? "client closed without a cause"
                               : "server closed without a cause");
    }
    if (sc->st) {
        sc->st->client_closed = o->c_post;
        sc->st->server_closed = o->s_post;
    }
}

static void count_outcome(scen_t *sc, moq_result_t rc)
{
    if (rc == MOQ_OK)                     sc->sum->outcome_ok++;
    else if (rc == MOQ_ERR_NOMEM)         sc->sum->outcome_nomem++;
    else if (rc == MOQ_ERR_WOULD_BLOCK)   sc->sum->outcome_wblock++;
    else                                  sc->sum->outcome_terminal++;
}

static void chk_ok(scen_t *sc, const char *where, const callobs_t *o)
{
    if (o->inj > 0)
        causal_fail(sc, where, MOQ_OK, o,
                    "injected allocation failure absorbed into MOQ_OK");
}

static void chk_nomem(scen_t *sc, const char *where, const callobs_t *o)
{
    if (o->inj != 1)
        causal_fail(sc, where, MOQ_ERR_NOMEM, o,
                    "NOMEM without an injected allocation failure");
}

/* Every ordinary (non-NOMEM, non-terminal-transition) outcome must be
 * injection-free: an injected allocation failure may surface only as
 * NOMEM or as a separately proven terminal. */
static void chk_no_inj(scen_t *sc, const char *where, moq_result_t rc,
                       const callobs_t *o)
{
    if (o->inj != 0)
        causal_fail(sc, where, rc, o,
                    "ordinary outcome beside an injected allocation "
                    "failure in the same call");
}

/* run_until_quiescent: OK / WOULD_BLOCK(steps==max) / NOMEM(injected) /
 * PROTO(corruption toward the failing receiver, in-call or prior) /
 * CLOSED(an endpoint closed before or during the call). */
static void classify_pump(scen_t *sc, const char *where, moq_result_t rc,
                          const callobs_t *o, size_t steps, size_t max)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, where, o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, where, o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, where, o); break;
    case MOQ_ERR_WOULD_BLOCK:
        chk_no_inj(sc, where, rc, o);
        if (steps != max)
            causal_fail(sc, where, rc, o, "WOULD_BLOCK before max_steps "
                        "was exhausted (unproven blocker)");
        break;
    case MOQ_ERR_PROTO: {
        chk_no_inj(sc, where, rc, o);
        bool to_client = o->neg_to == MOQ_PERSPECTIVE_CLIENT;
        size_t corrupt = to_client
            ? o->corrupt_c_pre + o->corrupt_c_delta
            : o->corrupt_s_pre + o->corrupt_s_delta;
        if (o->neg_input != MOQ_ERR_PROTO || corrupt == 0)
            causal_fail(sc, where, rc, o, "PROTO without an observed "
                        "corrupted delivery toward that receiver");
        break;
    }
    case MOQ_ERR_CLOSED: {
        /* With an injection, CLOSED is legal only when the injection's
         * close transition happened in this very call. */
        bool in_call_close = (!o->c_pre && o->c_post) ||
                             (!o->s_pre && o->s_post);
        if (o->inj != 0 && !in_call_close)
            chk_no_inj(sc, where, rc, o);
        if (!(o->c_pre || o->s_pre || o->c_post || o->s_post))
            causal_fail(sc, where, rc, o, "CLOSED with both sessions open");
        break;
    }
    default:
        causal_fail(sc, where, rc, o, "pump result outside the declared set");
    }
}

static void classify_advance(scen_t *sc, moq_result_t rc,
                             const callobs_t *o)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, "simpair_advance_to", o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, "simpair_advance_to", o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, "simpair_advance_to", o); break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, "simpair_advance_to", rc, o);
        if (!o->c_pre && !o->s_pre)
            causal_fail(sc, "simpair_advance_to", rc, o,
                        "CLOSED with both sessions open before the call");
        break;
    default:
        causal_fail(sc, "simpair_advance_to", rc, o,
                    "result outside advance_to's declared set");
    }
}

static void classify_rcbuf(scen_t *sc, moq_result_t rc, const callobs_t *o)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, "rcbuf_create", o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, "rcbuf_create", o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, "rcbuf_create", o); break;
    default:
        causal_fail(sc, "rcbuf_create", rc, o,
                    "result outside rcbuf_create's declared set");
    }
}

static void classify_add_track(scen_t *sc, moq_result_t rc,
                               const callobs_t *o)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, "pub_add_track", o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, "pub_add_track", o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, "pub_add_track", o); break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, "pub_add_track", rc, o);
        if (!o->s_pre)
            causal_fail(sc, "pub_add_track", rc, o,
                        "CLOSED with the server session open");
        break;
    default:
        causal_fail(sc, "pub_add_track", rc, o,
                    "result outside add_track's declared set");
    }
}

static void classify_remove_track(scen_t *sc, moq_result_t rc,
                                  const callobs_t *o, bool op_pending,
                                  bool ns_unsettled_sole)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, "pub_remove_track", o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, "pub_remove_track", o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, "pub_remove_track", o); break;
    case MOQ_ERR_WRONG_STATE:
        chk_no_inj(sc, "pub_remove_track", rc, o);
        if (!op_pending && !ns_unsettled_sole)
            causal_fail(sc, "pub_remove_track", rc, o,
                        "WRONG_STATE with no retained op and no unsettled "
                        "pending namespace on this track");
        break;
    case MOQ_ERR_WOULD_BLOCK:
        /* Production blocks remove only while pending/deferred facade
         * work belongs to THIS track; every drain converges staged work
         * to completion before ordinary ops, so no such work can exist
         * here. */
        causal_fail(sc, "pub_remove_track", rc, o,
                    "WOULD_BLOCK with no staged facade work possible");
        break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, "pub_remove_track", rc, o);
        if (!o->s_pre)
            causal_fail(sc, "pub_remove_track", rc, o,
                        "CLOSED with the server session open");
        break;
    default:
        causal_fail(sc, "pub_remove_track", rc, o,
                    "result outside remove_track's declared set");
    }
}

/* write_object / end_group, judged against the exact relationship to
 * the track's retained pending operation (the facade admission peek):
 * an EXACT retry resumes, a DIVERGENT call is refused WRONG_STATE
 * before any mutation, and a remove interrupted by an injected NOMEM
 * leaves a genuinely unobservable partial-teardown state where OK and
 * WRONG_STATE both stay legal. */
static void classify_track_op(scen_t *sc, const char *where,
                              moq_result_t rc, const callobs_t *o,
                              pend_rel_t rel, bool remove_pending)
{
    if (rel == PEND_REL_EXACT)      sc->sum->pend_exact_calls++;
    if (rel == PEND_REL_DIVERGENT)  sc->sum->pend_divergent_calls++;
    count_outcome(sc, rc);
    check_close_transitions(sc, where, o);
    switch (rc) {
    case MOQ_OK:
        chk_ok(sc, where, o);
        if (rel == PEND_REL_DIVERGENT)
            causal_fail(sc, where, rc, o,
                        "a divergent call executed against a retained "
                        "pending op (the peek must refuse it)");
        break;
    case MOQ_ERR_NOMEM:
        chk_nomem(sc, where, o);
        if (rel == PEND_REL_DIVERGENT)
            causal_fail(sc, where, rc, o,
                        "a divergent call allocated against a retained "
                        "pending op (the peek refuses before mutation)");
        break;
    case MOQ_ERR_WRONG_STATE:
        chk_no_inj(sc, where, rc, o);
        if (rel == PEND_REL_EXACT)
            causal_fail(sc, where, rc, o,
                        "an exact retry of the pending op was refused");
        else if (rel == PEND_REL_NONE && !remove_pending)
            causal_fail(sc, where, rc, o,
                        "WRONG_STATE with no retained op on this track");
        break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, where, rc, o);
        if (!o->s_pre)
            causal_fail(sc, where, rc, o,
                        "CLOSED with the server session open");
        break;
    default:
        causal_fail(sc, where, rc, o,
                    "result outside the write/end-group declared set");
    }
}

static void classify_subscribe(scen_t *sc, moq_result_t rc,
                               const callobs_t *o, bool dup_live)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, "session_subscribe", o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, "session_subscribe", o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, "session_subscribe", o); break;
    case MOQ_ERR_WRONG_STATE:
        chk_no_inj(sc, "session_subscribe", rc, o);
        if (!o->c_pre)
            causal_fail(sc, "session_subscribe", rc, o,
                        "WRONG_STATE with the client session active");
        break;
    case MOQ_ERR_INVAL:
        chk_no_inj(sc, "session_subscribe", rc, o);
        if (!dup_live)
            causal_fail(sc, "session_subscribe", rc, o,
                        "INVAL with no live duplicate subscription");
        break;
    default:
        causal_fail(sc, "session_subscribe", rc, o,
                    "result outside subscribe's declared set");
    }
}

static void classify_ns_response(scen_t *sc, const char *where,
                                 moq_result_t rc, const callobs_t *o)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, where, o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, where, o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, where, o); break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, where, rc, o);
        if (!o->c_pre)
            causal_fail(sc, where, rc, o,
                        "CLOSED with the client session active");
        break;
    default:
        causal_fail(sc, where, rc, o,
                    "result outside the namespace-response declared set");
    }
}

/* handle_event: OK / NOMEM(injected) / WOULD_BLOCK strictly with
 * CONSUMED (the staged-ownership creation point; under this serialized
 * drain an IGNORED blocked result always indicates broken
 * serialization and fails) / CLOSED(interior propagation). */
static void classify_handle_event(scen_t *sc, moq_result_t rc,
                                  moq_pub_event_result_t res,
                                  const callobs_t *o)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, "pub_handle_event", o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, "pub_handle_event", o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, "pub_handle_event", o); break;
    case MOQ_ERR_WOULD_BLOCK:
        chk_no_inj(sc, "pub_handle_event", rc, o);
        /* The drain is serialized (each event driven to flush MOQ_OK
         * before the next poll), so IGNORED here always indicates
         * broken serialization: only CONSUMED is legal. */
        if (res != MOQ_PUB_EVENT_CONSUMED)
            causal_fail(sc, "pub_handle_event", rc, o,
                        "WOULD_BLOCK without CONSUMED under the "
                        "serialized drain");
        break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, "pub_handle_event", rc, o);
        if (!o->s_pre)
            causal_fail(sc, "pub_handle_event", rc, o,
                        "CLOSED with the server session open");
        break;
    default:
        causal_fail(sc, "pub_handle_event", rc, o,
                    "result outside handle_event's declared set");
    }
}

static void classify_facade_pump(scen_t *sc, const char *where,
                                 moq_result_t rc, const callobs_t *o,
                                 bool staged_possible)
{
    count_outcome(sc, rc);
    check_close_transitions(sc, where, o);
    switch (rc) {
    case MOQ_OK:        chk_ok(sc, where, o); break;
    case MOQ_ERR_NOMEM: chk_nomem(sc, where, o); break;
    case MOQ_ERR_WOULD_BLOCK:
        chk_no_inj(sc, where, rc, o);
        if (!staged_possible)
            causal_fail(sc, where, rc, o,
                        "WOULD_BLOCK with no staged facade work possible");
        break;
    case MOQ_ERR_CLOSED:
        chk_no_inj(sc, where, rc, o);
        if (!o->s_pre)
            causal_fail(sc, where, rc, o,
                        "CLOSED with the server session open");
        break;
    default:
        causal_fail(sc, where, rc, o,
                    "result outside the facade-progress declared set");
    }
}

/* -- Observation bracket -------------------------------------------- */

static void obs_begin(scen_t *sc, callobs_t *o)
{
    memset(o, 0, sizeof(*o));
    o->c_pre = moq_session_state(moq_simpair_client(sc->sp)) ==
               MOQ_SESS_CLOSED;
    o->s_pre = moq_session_state(moq_simpair_server(sc->sp)) ==
               MOQ_SESS_CLOSED;
    o->divloss_c_pre = sc->sum->div_to_client + sc->sum->loss_to_client;
    o->divloss_s_pre = sc->sum->div_to_server + sc->sum->loss_to_server;
    o->corrupt_c_pre = sc->sum->corrupt_to_client;
    o->corrupt_s_pre = sc->sum->corrupt_to_server;
    scope_begin(sc->fa, sc->sum);
}

static void obs_end(scen_t *sc, callobs_t *o)
{
    o->inj = scope_end(sc->fa, sc->sum);
    o->neg_input = sc->sum->scope_neg_input;
    o->neg_to = sc->sum->scope_neg_to;
    o->c_post = moq_session_state(moq_simpair_client(sc->sp)) ==
                MOQ_SESS_CLOSED;
    o->s_post = moq_session_state(moq_simpair_server(sc->sp)) ==
                MOQ_SESS_CLOSED;
    o->divloss_c_delta = sc->sum->div_to_client + sc->sum->loss_to_client -
                         o->divloss_c_pre;
    o->divloss_s_delta = sc->sum->div_to_server + sc->sum->loss_to_server -
                         o->divloss_s_pre;
    o->corrupt_c_delta = sc->sum->corrupt_to_client - o->corrupt_c_pre;
    o->corrupt_s_delta = sc->sum->corrupt_to_server - o->corrupt_s_pre;
}

static moq_result_t pump_once(scen_t *sc, const char *where)
{
    callobs_t o;
    obs_begin(sc, &o);
    size_t steps = 0;
    moq_result_t rc = moq_simpair_run_until_quiescent(sc->sp, 8, &steps);
    obs_end(sc, &o);
    classify_pump(sc, where, rc, &o, steps, 8);
    return rc;
}

static void pump_converge(scen_t *sc, const char *where)
{
    for (int i = 0; i < 32; i++) {
        moq_result_t rc = pump_once(sc, where);
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_PROTO)
            return;
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_NOMEM)
            continue;
        return;
    }
    sc->failures++;
    if (!sc->quiet) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %s: pump never quiesced\n",
                (unsigned long long)sc->seed, sc->run_id, where);
        dump_log(sc);
    }
}

static size_t poll_events_quiet(scen_t *sc, moq_session_t *s,
                                moq_event_t *evs, size_t cap)
{
    poll_scope_begin(sc->fa);
    size_t n = moq_session_poll_events(s, evs, cap);
    poll_scope_end(sc->fa);
    return n;
}

/* -- Facade flush convergence --------------------------------------- */

/* Drive staged facade work to flush MOQ_OK. `staged` is TRUE only when
 * the caller just observed WOULD_BLOCK + CONSUMED (the contract's
 * staged-ownership creation); it is LOCAL to this convergence -- at
 * every ordinary operation boundary no staged work exists, so ordinary
 * flushes and remove_track never accept a blocked result. */
static void flush_converge(scen_t *sc, const char *where, bool staged)
{
    for (int i = 0; i < 8; i++) {
        pump_converge(sc, where);

        callobs_t o;
        obs_begin(sc, &o);
        moq_result_t rc = moq_pub_flush(sc->pub, moq_simpair_now_us(sc->sp));
        obs_end(sc, &o);
        classify_facade_pump(sc, where, rc, &o, staged);

        if (rc == MOQ_OK)
            return;                 /* staged work cleared */
        if (rc == MOQ_ERR_NOMEM)
            continue;   /* staged truth is unchanged: only WOULD_BLOCK +
                         * CONSUMED creates it, only flush MOQ_OK clears */
        if (rc == MOQ_ERR_WOULD_BLOCK && staged)
            continue;
        return;
    }
    sc->failures++;
    if (!sc->quiet)
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %s: flush never "
                "converged\n", (unsigned long long)sc->seed, sc->run_id,
                where);
}

/* -- Event draining ------------------------------------------------- */

static void drain_events(scen_t *sc, rng_t *rng)
{
    moq_session_t *client = moq_simpair_client(sc->sp);
    moq_session_t *server = moq_simpair_server(sc->sp);

    moq_event_t ev;

    while (poll_events_quiet(sc, client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR ||
            ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            uint64_t sub_bits = ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR
                ? ev.u.subscribe_error.sub._opaque
                : ev.u.subscribe_done.sub._opaque;
            for (int i = 0; i < MAX_TRACKS; i++)
                if (sc->st->tracks[i].client_sub_live &&
                    sc->st->tracks[i].client_sub == sub_bits)
                    sc->st->tracks[i].client_sub_live = false;
            moq_event_cleanup(&ev);
            continue;
        }
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            uint64_t now = moq_simpair_now_us(sc->sp);
            callobs_t o;
            moq_result_t rc;
            const char *where;
            obs_begin(sc, &o);
            if ((rng_next(rng) & 1) == 0) {
                moq_accept_namespace_cfg_t cfg;
                moq_accept_namespace_cfg_init(&cfg);
                rc = moq_session_accept_namespace(client,
                    ev.u.namespace_published.ann, &cfg, now);
                where = "accept_namespace";
            } else {
                moq_reject_namespace_cfg_t cfg;
                moq_reject_namespace_cfg_init(&cfg);
                cfg.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
                rc = moq_session_reject_namespace(client,
                    ev.u.namespace_published.ann, &cfg, now);
                where = "reject_namespace";
            }
            obs_end(sc, &o);
            classify_ns_response(sc, where, rc, &o);
            moq_event_cleanup(&ev);
            pump_converge(sc, "drain_ns_pump");
            continue;
        }
        moq_event_cleanup(&ev);
    }

    /* Server side: serialized per the moq_pub_handle_event contract. */
    while (poll_events_quiet(sc, server, &ev, 1) > 0) {
        uint64_t now = moq_simpair_now_us(sc->sp);
        uint32_t ev_kind = ev.kind;
        uint64_t ns_ev_ann = 0;
        if (ev_kind == MOQ_EVENT_NAMESPACE_ACCEPTED)
            ns_ev_ann = ev.u.namespace_accepted.ann._opaque;
        else if (ev_kind == MOQ_EVENT_NAMESPACE_REJECTED)
            ns_ev_ann = ev.u.namespace_rejected.ann._opaque;
        else if (ev_kind == MOQ_EVENT_NAMESPACE_CANCELLED)
            ns_ev_ann = ev.u.namespace_cancelled.ann._opaque;
        callobs_t o;
        obs_begin(sc, &o);
        moq_pub_event_result_t res;
        moq_result_t rc = moq_pub_handle_event(sc->pub, &ev, now, &res);
        obs_end(sc, &o);
        classify_handle_event(sc, rc, res, &o);
        moq_event_cleanup(&ev);

        /* The namespace cycle settles ONLY when the facade CONSUMED
         * the announcer's own response event, and only for the handle
         * this cycle owns: the first consumed response learns the
         * current cycle's handle; a later event must match it, and a
         * handle from a past settled cycle is stale and ignored. */
        if (rc == MOQ_OK && res == MOQ_PUB_EVENT_CONSUMED &&
            ns_ev_ann != 0 && sc->st->ns.cur_cycle != 0) {
            shadow_ns_t *ns = &sc->st->ns;
            bool stale = false;
            for (int i = 0; i < ns->old_ann_count; i++)
                if (ns->old_ann[i] == ns_ev_ann) stale = true;
            if (!stale) {
                if (ns->cur_ann == 0 && ns->cur_state == NS_PENDING)
                    ns->cur_ann = ns_ev_ann;
                if (ns->cur_ann == ns_ev_ann) {
                    if (ev_kind == MOQ_EVENT_NAMESPACE_ACCEPTED &&
                        ns->cur_state == NS_PENDING)
                        ns->cur_state = NS_LIVE;
                    else if (ev_kind == MOQ_EVENT_NAMESPACE_REJECTED ||
                             ev_kind == MOQ_EVENT_NAMESPACE_CANCELLED)
                        ns->cur_state = NS_DEAD;
                }
            }
        }

        /* Staged ownership is created exactly by WOULD_BLOCK +
         * CONSUMED (the contract) and is local to this convergence; an
         * injected NOMEM creates none -- its follow-up flush probes,
         * and an unexplained WOULD_BLOCK there fails loudly. */
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_NOMEM)
            flush_converge(sc, "handle_event_flush",
                           rc == MOQ_ERR_WOULD_BLOCK &&
                           res == MOQ_PUB_EVENT_CONSUMED);
    }
    refresh_subs(sc->pub, sc->st);
}

/* -- Execute one step ----------------------------------------------- */

static void execute_step(scen_t *sc, rng_t *rng)
{
    shadow_state_t *st = sc->st;
    if (shadow_closed(st)) return;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *client = moq_simpair_client(sc->sp);
    uint64_t now = moq_simpair_now_us(sc->sp);
    callobs_t o;

    switch (op) {
    case OP_PUMP:
        log_op(sc->log, op, 0);
        pump_converge(sc, "op_pump");
        break;

    case OP_ADD_TRACK: {
        int slot = -1;
        for (int i = 0; i < MAX_TRACKS; i++)
            if (!st->tracks[i].active) { slot = i; break; }
        if (slot < 0) { log_op(sc->log, OP_PUMP, 0); break; }

        char name[16];
        snprintf(name, sizeof(name), "cf%llu",
                 (unsigned long long)st->next_track_id++);

        moq_pub_track_cfg_t cfg;
        moq_pub_track_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("cns") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(name);
        cfg.advertise_namespace = (rng_next(rng) % 4) == 0;
        bool advertised = cfg.advertise_namespace;

        moq_pub_track_t *track = NULL;
        obs_begin(sc, &o);
        moq_result_t rc = moq_pub_add_track(sc->pub, &cfg, now, &track);
        obs_end(sc, &o);
        log_op(sc->log, op, rc == MOQ_OK ? 1 : 0);
        classify_add_track(sc, rc, &o);
        if (rc == MOQ_OK) {
            st->tracks[slot].active = true;
            st->tracks[slot].track = track;
            st->tracks[slot].has_sub = false;
            st->tracks[slot].group_open = false;
            st->tracks[slot].pend_kind = TRACK_PEND_NONE;
            st->tracks[slot].remove_pending = false;
            st->tracks[slot].client_sub_live = false;
            st->tracks[slot].advertised = advertised;
            st->tracks[slot].group_id = 0;
            st->tracks[slot].object_id = 0;
            memcpy(st->tracks[slot].name, name, sizeof(name));
            if (advertised) {
                /* A fresh announce cycle begins when none exists, the
                 * current one is DEAD, or a LIVE one lost its last
                 * advertised track (its entry was released). */
                if (st->ns.cur_cycle == 0 ||
                    st->ns.cur_state == NS_DEAD ||
                    st->ns.cur_refs == 0) {
                    if (st->ns.cur_ann != 0) {
                        if (st->ns.old_ann_count <
                            (int)(sizeof(st->ns.old_ann) /
                                  sizeof(st->ns.old_ann[0])))
                            st->ns.old_ann[st->ns.old_ann_count++] =
                                st->ns.cur_ann;
                        else
                            st->ns.old_ann_overflow = true;
                    }
                    st->ns.cur_cycle++;
                    st->ns.cur_state = NS_PENDING;
                    st->ns.cur_refs = 0;
                    st->ns.cur_ann = 0;
                }
                st->tracks[slot].ns_cycle = st->ns.cur_cycle;
                st->ns.cur_refs++;
            }
        }
        break;
    }

    case OP_REMOVE_TRACK: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0) { log_op(sc->log, OP_PUMP, 0); break; }
        log_op(sc->log, op, (uint64_t)idx);
        bool adv = st->tracks[idx].advertised;
        bool in_cur = adv &&
            st->tracks[idx].ns_cycle == st->ns.cur_cycle;
        bool ns_unsettled_sole = in_cur &&
            st->ns.cur_state == NS_PENDING && st->ns.cur_refs == 1;
        obs_begin(sc, &o);
        moq_result_t rc = moq_pub_remove_track(sc->pub,
                                               st->tracks[idx].track, now);
        obs_end(sc, &o);
        classify_remove_track(sc, rc, &o,
                              st->tracks[idx].remove_pending,
                              ns_unsettled_sole);
        if (rc == MOQ_OK) {
            if (in_cur)
                st->ns.cur_refs--;
            memset(&st->tracks[idx], 0, sizeof(st->tracks[idx]));
        } else if (rc == MOQ_ERR_NOMEM) {
            st->tracks[idx].remove_pending = true;
        }
        break;
    }

    case OP_SUBSCRIBE: {
        int idx = pick_track(st, rng, false, false);
        if (idx < 0 || st->tracks[idx].has_sub ||
            st->tracks[idx].client_sub_live) {
            log_op(sc->log, OP_PUMP, 0); break;
        }

        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("cns") };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;
        cfg.track_name = moq_bytes_cstr(st->tracks[idx].name);

        moq_subscription_t sub;
        obs_begin(sc, &o);
        moq_result_t rc = moq_session_subscribe(client, &cfg, now, &sub);
        obs_end(sc, &o);
        st->subscribes_issued++;
        log_op(sc->log, op, rc == MOQ_OK ? (uint64_t)idx : UINT64_MAX);
        classify_subscribe(sc, rc, &o, st->tracks[idx].client_sub_live);
        if (rc == MOQ_OK) {
            st->tracks[idx].client_sub_live = true;
            st->tracks[idx].client_sub = sub._opaque;
            pump_converge(sc, "sub_pump1");
            drain_events(sc, rng);
            pump_converge(sc, "sub_pump2");
            drain_events(sc, rng);
        }
        break;
    }

    case OP_WRITE_OBJECT: {
        int idx = pick_track(st, rng, true, false);
        if (idx < 0) { log_op(sc->log, OP_PUMP, 0); break; }

        uint8_t pdata[16];
        size_t plen = (rng_next(rng) % sizeof(pdata)) + 1;
        for (size_t j = 0; j < plen; j++)
            pdata[j] = (uint8_t)(rng_next(rng) & 0xFF);

        moq_rcbuf_t *payload = NULL;
        obs_begin(sc, &o);
        moq_result_t rc = moq_rcbuf_create(&(moq_alloc_t){
                sc->fa, fa_alloc, fa_realloc, fa_free },
                pdata, plen, &payload);
        obs_end(sc, &o);
        classify_rcbuf(sc, rc, &o);
        if (rc != MOQ_OK) {
            log_op(sc->log, op, UINT64_MAX);
            break;
        }

        shadow_track_t *t = &st->tracks[idx];
        /* A pending WRITE is retried byte-identically (the contract's
         * resume: a fresh buffer with identical bytes is fine); a
         * pending END_GROUP makes this write divergent. */
        pend_rel_t rel = PEND_REL_NONE;
        if (t->pend_kind == TRACK_PEND_WRITE) {
            rel = PEND_REL_EXACT;
            plen = t->pend_payload_len;
            memcpy(pdata, t->pend_payload, plen);
            moq_rcbuf_decref(payload);
            payload = NULL;
            obs_begin(sc, &o);
            rc = moq_rcbuf_create(&(moq_alloc_t){
                    sc->fa, fa_alloc, fa_realloc, fa_free },
                    pdata, plen, &payload);
            obs_end(sc, &o);
            classify_rcbuf(sc, rc, &o);
            if (rc != MOQ_OK) {
                log_op(sc->log, op, UINT64_MAX);
                break;
            }
        } else if (t->pend_kind == TRACK_PEND_END_GROUP) {
            rel = PEND_REL_DIVERGENT;
        }
        uint64_t obj = t->object_id;
        obs_begin(sc, &o);
        rc = moq_pub_write_object(sc->pub, t->track, t->group_id, obj,
                                  payload, now);
        obs_end(sc, &o);
        log_op(sc->log, op, rc == MOQ_OK ? obj : UINT64_MAX);
        classify_track_op(sc, "pub_write_object", rc, &o, rel,
                          t->remove_pending);
        if (rc == MOQ_OK) {
            t->pend_kind = TRACK_PEND_NONE;
            t->object_id++;
            t->group_open = true;
        } else if (rc == MOQ_ERR_NOMEM && rel != PEND_REL_DIVERGENT) {
            sc->sum->pend_created_calls++;
            t->pend_kind = TRACK_PEND_WRITE;   /* op stays pending */
            t->pend_group = t->group_id;
            t->pend_object = obj;
            t->pend_payload_len = plen;
            memcpy(t->pend_payload, pdata, plen);
        }
        moq_rcbuf_decref(payload);
        break;
    }

    case OP_END_GROUP: {
        int idx = pick_track(st, rng, true, true);
        if (idx < 0) { log_op(sc->log, OP_PUMP, 0); break; }
        shadow_track_t *t = &st->tracks[idx];
        /* end_group carries no bytes: a retry against a pending
         * END_GROUP is exact by identity; against a pending WRITE it
         * is divergent. */
        pend_rel_t rel = PEND_REL_NONE;
        if (t->pend_kind == TRACK_PEND_END_GROUP)
            rel = PEND_REL_EXACT;
        else if (t->pend_kind == TRACK_PEND_WRITE)
            rel = PEND_REL_DIVERGENT;
        obs_begin(sc, &o);
        moq_result_t rc = moq_pub_end_group(sc->pub, t->track, now);
        obs_end(sc, &o);
        log_op(sc->log, op, rc == MOQ_OK ? t->group_id : UINT64_MAX);
        classify_track_op(sc, "pub_end_group", rc, &o, rel,
                          t->remove_pending);
        if (rc == MOQ_OK) {
            t->pend_kind = TRACK_PEND_NONE;
            t->group_id++;
            t->object_id = 0;
            t->group_open = false;
        } else if (rc == MOQ_ERR_NOMEM && rel != PEND_REL_DIVERGENT) {
            sc->sum->pend_created_calls++;
            t->pend_kind = TRACK_PEND_END_GROUP;
        }
        break;
    }

    case OP_DRAIN_EVENTS:
        log_op(sc->log, op, 0);
        drain_events(sc, rng);
        break;

    case OP_FLUSH:
        log_op(sc->log, op, 0);
        flush_converge(sc, "op_flush", false);
        refresh_subs(sc->pub, sc->st);
        break;

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(sc->log, op, delta);
        obs_begin(sc, &o);
        moq_result_t rc = moq_simpair_advance_to(sc->sp, now + delta);
        obs_end(sc, &o);
        classify_advance(sc, rc, &o);
        break;
    }

    default: break;
    }
}

/* -- Self-checks (quiet) -------------------------------------------- */

static int self_checks(void)
{
    int failures = 0;
    run_summary_t sum;
    fault_alloc_state_t fa;
    op_log_t log = {0};
    scen_t sc;

    memset(&sum, 0, sizeof(sum));
    memset(&fa, 0, sizeof(fa));
    memset(&sc, 0, sizeof(sc));
    sc.sum = &sum; sc.fa = &fa; sc.log = &log;
    sc.quiet = true;

    callobs_t o;
    memset(&o, 0, sizeof(o));

    /* Rows the retired permissive sets accepted, now rejected. */
    sc.failures = 0;
    classify_rcbuf(&sc, MOQ_ERR_NOMEM, &o);
    classify_track_op(&sc, "selfcheck", MOQ_ERR_WRONG_STATE, &o,
                      PEND_REL_NONE, false);
    classify_facade_pump(&sc, "selfcheck", MOQ_ERR_WOULD_BLOCK, &o, false);
    classify_remove_track(&sc, MOQ_ERR_STALE_HANDLE, &o, true, true);
    if (sc.failures != 4) failures++;

    /* Pending-op relationships: an EXACT retry refused with
     * WRONG_STATE fails; a DIVERGENT call that executed fails; the
     * DIVERGENT refusal and the EXACT resume are accepted. */
    sc.failures = 0;
    classify_track_op(&sc, "selfcheck", MOQ_ERR_WRONG_STATE, &o,
                      PEND_REL_EXACT, false);
    classify_track_op(&sc, "selfcheck", MOQ_OK, &o,
                      PEND_REL_DIVERGENT, false);
    if (sc.failures != 2) failures++;
    sc.failures = 0;
    classify_track_op(&sc, "selfcheck", MOQ_ERR_WRONG_STATE, &o,
                      PEND_REL_DIVERGENT, false);
    classify_track_op(&sc, "selfcheck", MOQ_OK, &o,
                      PEND_REL_EXACT, false);
    if (sc.failures != 0) failures++;
    /* Injected NOMEM per relationship: an EXACT retry may fail NOMEM
     * again; a DIVERGENT call is refused before any allocation, so its
     * NOMEM is impossible. */
    sc.failures = 0;
    o.inj = 1;
    classify_track_op(&sc, "selfcheck", MOQ_ERR_NOMEM, &o,
                      PEND_REL_EXACT, false);
    if (sc.failures != 0) failures++;
    sc.failures = 0;
    classify_track_op(&sc, "selfcheck", MOQ_ERR_NOMEM, &o,
                      PEND_REL_DIVERGENT, false);
    if (sc.failures != 1) failures++;
    o.inj = 0;

    sc.failures = 0;
    o.inj = 1;
    classify_rcbuf(&sc, MOQ_ERR_NOMEM, &o);
    if (sc.failures != 0) failures++;
    sc.failures = 0;
    classify_rcbuf(&sc, MOQ_OK, &o);
    if (sc.failures != 1) failures++;

    /* Injection equivalence: ordinary outcomes beside an injection are
     * rejected even when their own cause is present. */
    sc.failures = 0;
    classify_track_op(&sc, "selfcheck", MOQ_ERR_WRONG_STATE, &o,
                      PEND_REL_DIVERGENT, false);
    classify_facade_pump(&sc, "selfcheck", MOQ_ERR_WOULD_BLOCK, &o, true);
    classify_subscribe(&sc, MOQ_ERR_INVAL, &o, true);
    if (sc.failures != 3) failures++;
    o.inj = 0;

    sc.failures = 0;
    classify_pump(&sc, "selfcheck", MOQ_ERR_WOULD_BLOCK, &o, 3, 8);
    if (sc.failures != 1) failures++;
    sc.failures = 0;
    classify_pump(&sc, "selfcheck", MOQ_ERR_WOULD_BLOCK, &o, 8, 8);
    if (sc.failures != 0) failures++;

    /* PROTO needs the negative PROTO input AND corruption toward THAT
     * receiver; corruption toward the other endpoint is insufficient. */
    sc.failures = 0;
    classify_pump(&sc, "selfcheck", MOQ_ERR_PROTO, &o, 2, 8);
    if (sc.failures != 1) failures++;
    sc.failures = 0;
    o.neg_input = MOQ_ERR_PROTO;
    o.neg_to = MOQ_PERSPECTIVE_SERVER;
    o.corrupt_s_delta = 1;
    classify_pump(&sc, "selfcheck", MOQ_ERR_PROTO, &o, 2, 8);
    if (sc.failures != 0) failures++;
    sc.failures = 0;
    o.corrupt_s_delta = 0;
    o.corrupt_c_delta = 1;   /* wrong direction */
    classify_pump(&sc, "selfcheck", MOQ_ERR_PROTO, &o, 2, 8);
    if (sc.failures != 1) failures++;
    memset(&o, 0, sizeof(o));

    /* Close transitions: unattributed rejected; divergence toward THAT
     * endpoint accepts; divergence toward the OTHER endpoint does not. */
    sc.failures = 0;
    o.s_post = true;
    check_close_transitions(&sc, "selfcheck", &o);
    if (sc.failures != 1) failures++;
    sc.failures = 0;
    o.divloss_s_pre = 1;
    check_close_transitions(&sc, "selfcheck", &o);
    if (sc.failures != 0) failures++;
    sc.failures = 0;
    o.divloss_s_pre = 0;
    o.divloss_c_pre = 1;
    check_close_transitions(&sc, "selfcheck", &o);
    if (sc.failures != 1) failures++;
    memset(&o, 0, sizeof(o));

    /* Two simultaneous new closes cannot justify each other: with no
     * independent root, BOTH are rejected; with one endpoint's own root
     * the peer's cascade is accepted. */
    sc.failures = 0;
    o.c_post = true;
    o.s_post = true;
    check_close_transitions(&sc, "selfcheck", &o);
    if (sc.failures != 2) failures++;
    sc.failures = 0;
    o.divloss_s_pre = 1;
    check_close_transitions(&sc, "selfcheck", &o);
    if (sc.failures != 0) failures++;
    memset(&o, 0, sizeof(o));


    /* handle_event's WOULD_BLOCK contract: CONSUMED accepted (staged
     * creation); IGNORED always rejected under the serialized drain. */
    sc.failures = 0;
    classify_handle_event(&sc, MOQ_ERR_WOULD_BLOCK,
                          MOQ_PUB_EVENT_CONSUMED, &o);
    if (sc.failures != 0) failures++;
    sc.failures = 0;
    classify_handle_event(&sc, MOQ_ERR_WOULD_BLOCK,
                          MOQ_PUB_EVENT_IGNORED, &o);
    if (sc.failures != 1) failures++;


    /* Injected CLOSED: without an in-call terminal transition it is
     * rejected; the explicitly allowed shape -- the injection closed an
     * endpoint during this very call -- passes. */
    sc.failures = 0;
    o.inj = 1;
    o.s_pre = true;
    o.s_post = true;             /* pre-closed, no transition */
    classify_pump(&sc, "selfcheck", MOQ_ERR_CLOSED, &o, 2, 8);
    if (sc.failures != 1) failures++;
    sc.failures = 0;
    o.s_pre = false;             /* the in-call injected close */
    classify_pump(&sc, "selfcheck", MOQ_ERR_CLOSED, &o, 2, 8);
    if (sc.failures != 0) failures++;
    memset(&o, 0, sizeof(o));

    /* Scope machinery. */
    memset(&fa, 0, sizeof(fa));
    fa.per_mille = 1000;
    fa.enabled = true;
    scope_begin(&fa, &sum);
    bool first = fault_should_fail(&fa, 64, 1);
    bool second = fault_should_fail(&fa, 64, 1);
    uint64_t delta = scope_end(&fa, &sum);
    if (!first || second || delta != 1) failures++;
    fa.per_mille = 1;
    if (fault_should_fail(&fa, 64, 1)) failures++;
    if (fa.unscoped_violations != 1) failures++;
    fa.per_mille = 0;
    if (fault_should_fail(&fa, 64, 1)) failures++;
    if (fa.unscoped_violations != 2) failures++;
    fa.per_mille = 1000;
    poll_scope_begin(&fa);
    if (fault_should_fail(&fa, 64, 1)) failures++;
    poll_scope_end(&fa);
    if (fa.unscoped_violations != 2) failures++;
    scope_begin(&fa, &sum);
    scope_begin(&fa, &sum);
    (void)scope_end(&fa, &sum);
    if (fa.nested_violations != 1) failures++;

    if (failures)
        fprintf(stderr, "FAIL: combined classifier self-checks (%d)\n",
                failures);
    return failures;
}


/* Deterministic public-surface pending-operation probe, run once per
 * invocation. Live exact-retry/divergent coverage is NOT reachable
 * through this surface today: the public write path performs no heap
 * allocations, so a one-shot injected allocation failure cannot make a
 * write's operation pend -- this probe PINS that fact at full-rate
 * injection (zero allocator attempts inside the write's scope) and
 * fails loudly the day the facade write path starts allocating, at
 * which point the live relationship coverage must be activated. Until
 * then the relationship oracle is pinned by the synthetic self-check
 * matrix, and the seed runs assert zero EXACT/DIVERGENT
 * classifications. */
static int pending_op_case(void)
{
    int failures = 0;
    run_summary_t sum;
    fault_alloc_state_t fa;
    shadow_state_t st;
    op_log_t log = {0};
    scen_t sc;
    rng_t rng = { 7 };

    memset(&sum, 0, sizeof(sum));
    sum.trace_hash = 0xCBF29CE484222325ULL;
    memset(&fa, 0, sizeof(fa));
    memset(&st, 0, sizeof(st));
    memset(&sc, 0, sizeof(sc));
    moq_alloc_t alloc = { &fa, fa_alloc, fa_realloc, fa_free };

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = 7;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = &sum;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK || !sp) return 1;
    if (moq_simpair_start(sp) != MOQ_OK) { moq_simpair_destroy(sp); return 1; }
    (void)moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    if (moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub)
            != MOQ_OK || !pub) {
        moq_simpair_destroy(sp);
        return 1;
    }

    sc.sp = sp; sc.pub = pub; sc.fa = &fa; sc.sum = &sum;
    sc.st = &st; sc.log = &log; sc.seed = 7; sc.max_steps = 1;

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("cns") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = moq_bytes_cstr("pcase");
    moq_pub_track_t *track = NULL;
    if (moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track)
            != MOQ_OK) {
        moq_pub_destroy(pub); moq_simpair_destroy(sp);
        return 1;
    }
    st.tracks[0].active = true;
    st.tracks[0].track = track;
    memcpy(st.tracks[0].name, "pcase", 6);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = moq_bytes_cstr("pcase");
    moq_subscription_t sub;
    if (moq_session_subscribe(moq_simpair_client(sp), &scfg,
                              moq_simpair_now_us(sp), &sub) != MOQ_OK) {
        moq_pub_destroy(pub); moq_simpair_destroy(sp);
        return 1;
    }
    st.tracks[0].client_sub_live = true;
    st.tracks[0].client_sub = sub._opaque;
    pump_converge(&sc, "pending_case_pump");
    drain_events(&sc, &rng);
    pump_converge(&sc, "pending_case_pump");
    drain_events(&sc, &rng);
    if (!st.tracks[0].has_sub) {
        fprintf(stderr, "FAIL: pending-op case: subscription not "
                "established\n");
        moq_pub_destroy(pub); moq_simpair_destroy(sp);
        return failures + sc.failures + 1;
    }

    static const uint8_t bytes[4] = { 0x11, 0x22, 0x33, 0x44 };
    uint64_t now = moq_simpair_now_us(sp);
    callobs_t o;

    /* UNREACHABILITY PIN. At full-rate one-shot injection the public
     * write performs ZERO allocator attempts: moq_pub_write_object does
     * not allocate on this path, so an injected-NOMEM pending WRITE --
     * and therefore live exact-retry/divergent coverage -- is not
     * reachable through this public surface. The relationship oracle is
     * pinned synthetically instead (the self-check matrix), and the
     * seed runs must classify zero EXACT/DIVERGENT calls. The day the
     * facade write path starts allocating, this case fails loudly and
     * the live coverage must be activated. */
    fa.enabled = true;
    fa.per_mille = 1000;
    uint64_t calls_before = fa.calls;
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes),
                         &payload) != MOQ_OK) {
        moq_pub_destroy(pub); moq_simpair_destroy(sp);
        return failures + 1;
    }
    obs_begin(&sc, &o);
    moq_result_t rc = moq_pub_write_object(pub, track, 0, 0, payload, now);
    obs_end(&sc, &o);
    moq_rcbuf_decref(payload);
    classify_track_op(&sc, "pending_case_write", rc, &o,
                      PEND_REL_NONE, false);
    if (rc != MOQ_OK || o.inj != 0 || fa.calls != calls_before) {
        fprintf(stderr, "FAIL: pending-op unreachability pin: write "
                "rc=%d inj=%llu attempts=%llu -- the public write path "
                "now allocates; activate live exact-retry/divergent "
                "coverage\n", (int)rc, (unsigned long long)o.inj,
                (unsigned long long)(fa.calls - calls_before));
        failures++;
    }

    /* A second, different write commits normally. */
    fa.per_mille = 0;
    uint8_t next_bytes[4] = { 0x55, 0x66, 0x77, 0x88 };
    if (moq_rcbuf_create(moq_alloc_default(), next_bytes,
                         sizeof(next_bytes), &payload) != MOQ_OK) {
        moq_pub_destroy(pub); moq_simpair_destroy(sp);
        return failures + sc.failures + 1;
    }
    obs_begin(&sc, &o);
    rc = moq_pub_write_object(pub, track, 0, 1, payload, now);
    obs_end(&sc, &o);
    moq_rcbuf_decref(payload);
    classify_track_op(&sc, "pending_case_next", rc, &o,
                      PEND_REL_NONE, false);
    if (rc != MOQ_OK)
        failures++;

    fa.enabled = false;
    pump_converge(&sc, "pending_case_pump");
    drain_events(&sc, &rng);
    failures += sc.failures;

    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);
    if (fa.balance != 0) {
        fprintf(stderr, "FAIL: pending-op probe: alloc balance=%lld "
                "after teardown\n", (long long)fa.balance);
        failures++;
    }
    if (fa.unscoped_violations != 0 || fa.nested_violations != 0) {
        fprintf(stderr, "FAIL: pending-op probe: %llu unscoped / %llu "
                "nested scope violations\n",
                (unsigned long long)fa.unscoped_violations,
                (unsigned long long)fa.nested_violations);
        failures++;
    }
    if (failures)
        fprintf(stderr, "FAIL: pending-op probe (%d)\n", failures);
    return failures;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, uint64_t alloc_pm,
                         uint64_t transport_pm,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->trace_hash = 0xCBF29CE484222325ULL;

    fault_alloc_state_t fas = {
        .seed = seed ^ 0xC0AB1DFA017ULL,
        .per_mille = alloc_pm,
    };
    moq_alloc_t alloc = { &fas, fa_alloc, fa_realloc, fa_free };

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    /* Request capacity and subscription pools sized past the run's op
     * budget so neither REQUEST_BLOCKED nor pool exhaustion can occur
     * at any supported shape. */
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = (uint64_t)max_steps + 8;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = (uint64_t)max_steps + 8;
    cfg.client_max_subscriptions = (uint32_t)max_steps + 8;
    cfg.server_max_subscriptions = (uint32_t)max_steps + 8;
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = summary;
    cfg.fault_per_mille = (uint32_t)transport_pm;
    cfg.fault_flags = MOQ_SIM_FAULT_ALL | MOQ_SIM_FAULT_ALL_INJECT |
                      MOQ_SIM_FAULT_ALL_DELAY;

    /* Faults are disabled during setup -- these must succeed. */
    moq_simpair_t *sp = NULL;
    int failures = 0;
    op_log_t log = {0};

    scen_t sc;
    memset(&sc, 0, sizeof(sc));
    sc.fa = &fas; sc.sum = summary; sc.log = &log;
    sc.seed = seed; sc.run_id = run_id; sc.max_steps = max_steps;
    sc.alloc_pm = alloc_pm; sc.transport_pm = transport_pm;

    moq_result_t rc = moq_simpair_create(&cfg, &sp);
    if (rc != MOQ_OK || !sp) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: simpair_create rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }
    sc.sp = sp;

    rc = moq_simpair_start(sp);
    if (rc != MOQ_OK) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: simpair_start rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        moq_simpair_destroy(sp);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }

    rc = moq_simpair_run_until_quiescent(sp, 8, NULL);
    if (rc != MOQ_OK) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: setup pump rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        moq_simpair_destroy(sp);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }

    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub = NULL;
    rc = moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);
    if (rc != MOQ_OK || !pub) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: pub_create rc=%d\n",
                (unsigned long long)seed, run_id, (int)rc);
        moq_simpair_destroy(sp);
        if (fas.balance != 0) failures++;
        summary->alloc_calls = fas.calls;
        summary->alloc_failures = fas.failures;
        return failures + 1;
    }
    sc.pub = pub;

    fas.enabled = true;
    moq_simpair_enable_faults(sp);

    shadow_state_t st;
    memset(&st, 0, sizeof(st));
    sc.st = &st;
    rng_t rng = { seed };

    for (int step = 0; step < max_steps; step++) {
        execute_step(&sc, &rng);
        if (sc.failures > 0)
            break;
        pump_converge(&sc, "step_pump");
        drain_events(&sc, &rng);
        if (sc.failures > 0)
            break;
        if (shadow_closed(&st)) {
            if (total_faults(summary) == 0 && fas.failures == 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "closed without any fault\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(&sc);
                sc.failures++;
            }
            break;
        }
    }

    for (int i = 0; i < MAX_TRACKS; i++) {
        if (st.tracks[i].active && st.tracks[i].group_open) {
            callobs_t o;
            obs_begin(&sc, &o);
            moq_result_t egrc = moq_pub_end_group(pub, st.tracks[i].track,
                moq_simpair_now_us(sp));
            obs_end(&sc, &o);
            {
                pend_rel_t rel = PEND_REL_NONE;
                if (st.tracks[i].pend_kind == TRACK_PEND_END_GROUP)
                    rel = PEND_REL_EXACT;
                else if (st.tracks[i].pend_kind == TRACK_PEND_WRITE)
                    rel = PEND_REL_DIVERGENT;
                classify_track_op(&sc, "cleanup_end_group", egrc, &o, rel,
                                  st.tracks[i].remove_pending);
            }
            if (egrc == MOQ_OK)
                st.tracks[i].pend_kind = TRACK_PEND_NONE;
            else if (egrc == MOQ_ERR_NOMEM &&
                     st.tracks[i].pend_kind != TRACK_PEND_WRITE) {
                summary->pend_created_calls++;
                st.tracks[i].pend_kind = TRACK_PEND_END_GROUP;
            }
        }
    }

    pump_converge(&sc, "cleanup_pump");
    drain_events(&sc, &rng);
    {
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_server(sp),
                                              acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(moq_simpair_client(sp),
                                              acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    failures += sc.failures;

    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);

    if (fas.balance != 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: alloc balance=%lld\n",
                (unsigned long long)seed, run_id, (long long)fas.balance);
        dump_log(&sc);
        failures++;
    }
    if (fas.unscoped_violations != 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %llu enabled allocation "
                "attempts outside every call scope\n",
                (unsigned long long)seed, run_id,
                (unsigned long long)fas.unscoped_violations);
        dump_log(&sc);
        failures++;
    }
    if (fas.nested_violations != 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %llu nested call scopes\n",
                (unsigned long long)seed, run_id,
                (unsigned long long)fas.nested_violations);
        failures++;
    }
    if (summary->pend_exact_calls != 0 ||
        summary->pend_divergent_calls != 0 ||
        summary->pend_created_calls != 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: %llu exact / %llu "
                "divergent / %llu created pending-op transitions in a "
                "surface where pending track ops are unreachable (see "
                "the pending-op probe)\n", (unsigned long long)seed,
                run_id,
                (unsigned long long)summary->pend_exact_calls,
                (unsigned long long)summary->pend_divergent_calls,
                (unsigned long long)summary->pend_created_calls);
        failures++;
    }
    if (st.ns.old_ann_overflow) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: namespace handle "
                "evidence store overflowed\n",
                (unsigned long long)seed, run_id);
        failures++;
    }

    summary->alloc_calls = fas.calls;
    summary->alloc_failures = fas.failures;
    summary->unscoped_violations = fas.unscoped_violations;
    summary->nested_violations = fas.nested_violations;
    return failures;
}

/* -- Main ----------------------------------------------------------- */

int main(void)
{
    int failures = self_checks();
    failures += pending_op_case();
    const char *env_seeds = getenv("MOQ_SCENARIO_SEEDS");
    const char *env_start = getenv("MOQ_SCENARIO_SEED_START");
    const char *env_steps = getenv("MOQ_SCENARIO_STEPS");
    const char *env_alloc = getenv("MOQ_SCENARIO_ALLOC_FAULT_PERMILLE");
    const char *env_xport = getenv("MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE");

    uint64_t num_seeds = env_seeds ? strtoull(env_seeds, NULL, 0) : 100;
    uint64_t seed_start = env_start ? strtoull(env_start, NULL, 0) : 0;
    int max_steps = env_steps ? atoi(env_steps) : 50;
    uint64_t alloc_pm = env_alloc ? strtoull(env_alloc, NULL, 0) : 25;
    uint64_t transport_pm = env_xport ? strtoull(env_xport, NULL, 0) : 40;
    if (alloc_pm > 1000) alloc_pm = 1000;
    if (transport_pm > 1000) transport_pm = 1000;
    if (max_steps < 1) max_steps = 1;

    size_t total_ctrl = 0, total_data = 0, total_rst = 0, total_stp = 0;
    size_t total_mut_c = 0, total_mut_d = 0, total_reord = 0;
    size_t total_inj_rst = 0, total_inj_stp = 0, total_inj_cls = 0;
    size_t total_trunc_ctl = 0, total_trunc_data = 0;
    size_t total_delay = 0, total_observed = 0;
    uint64_t total_alloc_fail = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        run_summary_t sums[2];

        for (int run = 0; run < 2; run++)
            failures += run_scenario(seed, alloc_pm, transport_pm,
                                      &sums[run], run, max_steps);

        total_ctrl += sums[0].drop_control;
        total_data += sums[0].drop_data;
        total_rst  += sums[0].drop_reset;
        total_stp  += sums[0].drop_stop;
        total_mut_c += sums[0].mutate_control;
        total_mut_d += sums[0].mutate_data;
        total_reord += sums[0].reorder_action;
        total_inj_rst += sums[0].inject_reset;
        total_inj_stp += sums[0].inject_stop;
        total_inj_cls += sums[0].inject_close;
        total_trunc_ctl += sums[0].truncate_control;
        total_trunc_data += sums[0].truncate_data;
        total_delay += sums[0].delay_enqueues;
        total_observed += observed_transport_faults(&sums[0]);
        total_alloc_fail += sums[0].alloc_failures;

        if (sums[0].trace_hash != sums[1].trace_hash ||
            sums[0].trace_count != sums[1].trace_count ||
            sums[0].alloc_calls != sums[1].alloc_calls ||
            sums[0].alloc_failures != sums[1].alloc_failures ||
            sums[0].drop_control != sums[1].drop_control ||
            sums[0].drop_data != sums[1].drop_data ||
            sums[0].drop_reset != sums[1].drop_reset ||
            sums[0].drop_stop != sums[1].drop_stop ||
            sums[0].mutate_control != sums[1].mutate_control ||
            sums[0].mutate_data != sums[1].mutate_data ||
            sums[0].reorder_action != sums[1].reorder_action ||
            sums[0].inject_reset != sums[1].inject_reset ||
            sums[0].inject_stop != sums[1].inject_stop ||
            sums[0].inject_close != sums[1].inject_close ||
            sums[0].truncate_control != sums[1].truncate_control ||
            sums[0].truncate_data != sums[1].truncate_data ||
            sums[0].delay_enqueues != sums[1].delay_enqueues ||
            sums[0].scoped_calls != sums[1].scoped_calls ||
            sums[0].injected_calls != sums[1].injected_calls ||
            sums[0].outcome_ok != sums[1].outcome_ok ||
            sums[0].outcome_nomem != sums[1].outcome_nomem ||
            sums[0].outcome_wblock != sums[1].outcome_wblock ||
            sums[0].outcome_terminal != sums[1].outcome_terminal ||
            sums[0].div_to_client != sums[1].div_to_client ||
            sums[0].div_to_server != sums[1].div_to_server ||
            sums[0].corrupt_to_client != sums[1].corrupt_to_client ||
            sums[0].corrupt_to_server != sums[1].corrupt_to_server ||
            sums[0].loss_to_client != sums[1].loss_to_client ||
            sums[0].loss_to_server != sums[1].loss_to_server ||
            sums[0].pend_exact_calls != sums[1].pend_exact_calls ||
            sums[0].pend_divergent_calls != sums[1].pend_divergent_calls ||
            sums[0].pend_created_calls != sums[1].pend_created_calls ||
            sums[0].unscoped_violations != sums[1].unscoped_violations ||
            sums[0].nested_violations != sums[1].nested_violations) {
            fprintf(stderr, "FAIL seed=0x%llx: nondeterministic\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  run0 trace=0x%llx/%zu alloc=%llu/%llu "
                    "scoped=%llu inj=%llu ok=%llu nomem=%llu wb=%llu "
                    "term=%llu\n",
                    (unsigned long long)sums[0].trace_hash,
                    sums[0].trace_count,
                    (unsigned long long)sums[0].alloc_calls,
                    (unsigned long long)sums[0].alloc_failures,
                    (unsigned long long)sums[0].scoped_calls,
                    (unsigned long long)sums[0].injected_calls,
                    (unsigned long long)sums[0].outcome_ok,
                    (unsigned long long)sums[0].outcome_nomem,
                    (unsigned long long)sums[0].outcome_wblock,
                    (unsigned long long)sums[0].outcome_terminal);
            fprintf(stderr, "  run1 trace=0x%llx/%zu alloc=%llu/%llu "
                    "scoped=%llu inj=%llu ok=%llu nomem=%llu wb=%llu "
                    "term=%llu\n",
                    (unsigned long long)sums[1].trace_hash,
                    sums[1].trace_count,
                    (unsigned long long)sums[1].alloc_calls,
                    (unsigned long long)sums[1].alloc_failures,
                    (unsigned long long)sums[1].scoped_calls,
                    (unsigned long long)sums[1].injected_calls,
                    (unsigned long long)sums[1].outcome_ok,
                    (unsigned long long)sums[1].outcome_nomem,
                    (unsigned long long)sums[1].outcome_wblock,
                    (unsigned long long)sums[1].outcome_terminal);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=%llu "
                    "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=%llu "
                    "./build/tests/test_scenario_combined_faults\n",
                    (unsigned long long)seed, max_steps,
                    (unsigned long long)alloc_pm,
                    (unsigned long long)transport_pm);
            failures++;
        }
    }

    if (num_seeds >= 10) {
        if (alloc_pm > 0 && total_alloc_fail == 0) {
            fprintf(stderr, "FAIL: no allocation faults injected\n");
            failures++;
        }
        /* Coverage floor over EVERY trace-observed transport fault
         * class (drops, mutations, truncations, reorders, injected
         * stream/close faults, delay enqueues). */
        if (transport_pm > 0 && total_observed == 0) {
            fprintf(stderr, "FAIL: no transport faults observed\n");
            failures++;
        }
        /* Delay faults are enabled; a run wide enough to observe any
         * transport fault class must also observe delays. */
        if (transport_pm > 0 && num_seeds >= 100 && total_delay == 0) {
            fprintf(stderr, "FAIL: no delay faults observed\n");
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_combined_faults "
                "(%llu seeds, alloc_fail=%llu, "
                "ctrl=%zu data=%zu rst=%zu stp=%zu "
                "mut_c=%zu mut_d=%zu reord=%zu "
                "inj_rst=%zu inj_stp=%zu inj_cls=%zu "
                "trunc_ctl=%zu trunc_data=%zu delay=%zu)\n",
                (unsigned long long)num_seeds,
                (unsigned long long)total_alloc_fail,
                total_ctrl, total_data, total_rst, total_stp,
                total_mut_c, total_mut_d, total_reord,
                total_inj_rst, total_inj_stp, total_inj_cls,
                total_trunc_ctl, total_trunc_data, total_delay);
    else
        fprintf(stderr, "FAIL: test_scenario_combined_faults "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
