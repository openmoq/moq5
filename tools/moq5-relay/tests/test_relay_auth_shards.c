/*
 * Cross-shard authorization: a deterministic K=2, multi-bind SimPair rig
 * proving authorization stays REQUESTER-LOCAL and lane-safe when a
 * subscription crosses shards. Shard 0 owns the publisher and the source
 * track; shard 1 owns the subscriber connections; each shard's sessions
 * attach to that shard's own production binding (moqr_shards_bind), and the
 * runtime is the real admitted cross-shard pump (this test owns the
 * admit_remote_demand flag directly; the production CLI turns it on for every
 * multi-lane serve).
 *
 * The verifier seam: ONE dispatching hook (installed through the shard
 * runtime's core template) routes verdicts to two separately controllable
 * role verifiers — SUBSCRIBE/SUBSCRIBE_NAMESPACE decisions come from the
 * REQUESTER verifier, PUBLISH_NAMESPACE/PUBLISH decisions from the OWNER
 * verifier; setup always allows. Each role verifier also RECORDS every
 * credential it is shown, which is what proves token isolation: the
 * requester verifier only ever sees subscriber credentials, the owner
 * verifier only publisher credentials, and the owner's internal pump-sub is
 * never authorized at all (its subscribe enters the core below the binding's
 * auth gate — replication, not a user request). Verifier state is mutated
 * only from the single deterministic driver thread, never concurrently.
 *
 * Scenarios: requester DENY (no demand may exist anywhere), ALLOW (the exact
 * DEMAND -> pump-sub -> ACK -> activation -> data order via per-kind enqueue
 * counters), DEFER (parked emits nothing until moqr_bind_auth_resolve),
 * requester revalidation DENY mid-object (full quiet cleanup chain), owner
 * announce revalidation DENY (withdraw + GOING_AWAY termination + exactly
 * one publisher cancel, both drafts), and token isolation.
 */

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif
#include "../cli/blockedstats.h"

#include <moq/moq.h>
#include <moq/sim.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

/* counting allocator (single-threaded: the deterministic runner) */
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

#define AMAX_CONNS 4

typedef struct aconn {
    bool           used;
    uint16_t       shard;   /* which shard's binding owns rsess */
    moq_simpair_t *sp;
    moq_session_t *peer;    /* client side: the test's endpoint */
    moq_session_t *rsess;   /* server side: the relay's session */
} aconn_t;

/* One ROLE's controllable verifier + credential observation log. Mutated
 * only from the deterministic driver thread (the rig is single-threaded). */
typedef struct role_auth {
    moqr_auth_decision_t decision;
    uint64_t             lease_us;    /* revalidate_after_us on ALLOW      */
    uint32_t             deny_code;   /* custom wire code on DENY (0=def)  */
    uint64_t             ticket;      /* DEFER ticket (nonzero)            */
    int                  calls;       /* verdicts issued for this role     */
    /* Credential observation: every token this role's verifier was shown
     * across all calls (bounded log; overflow counts). */
    int                  tok_seen;
    uint64_t             tok_type[8];
    uint8_t              tok_first[8];
} role_auth_t;

typedef struct arig {
    ca_t          *alloc;
    moqr_shards_t *s;
    aconn_t        conns[AMAX_CONNS];
    uint64_t       now;
    int            failures;
    bool           streaming;   /* set BEFORE arig_connect               */
    uint32_t       conn_max_actions;   /* 0 = session default; set BEFORE
                                        * arig_connect (large batches)   */
    uint32_t       conn_max_events;    /* ditto, for the client's queue  */
    uint32_t       conn_max_open_subgroups;   /* ditto, the SERVER session's
                                               * own subgroup pool        */
    role_auth_t    sub_auth;    /* the REQUESTER verifier (subscribes)   */
    role_auth_t    pub_auth;    /* the OWNER verifier (announce/publish) */
} arig_t;

#define A_CHECK(rig, expr)                                                \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            (rig)->failures++;                                            \
        }                                                                 \
    } while (0)

static void
role_init(role_auth_t *ra)
{
    memset(ra, 0, sizeof(*ra));
    ra->decision = MOQR_AUTH_ALLOW;
}

static void
role_record(role_auth_t *ra, const moqr_auth_request_t *req)
{
    ra->calls++;
    for (size_t i = 0; i < req->token_count; i++) {
        if (ra->tok_seen < 8) {
            ra->tok_type[ra->tok_seen] = req->tokens[i].token_type;
            ra->tok_first[ra->tok_seen] =
                req->tokens[i].token_value.len > 0
                    ? req->tokens[i].token_value.data[0]
                    : 0u;
        }
        ra->tok_seen++;
    }
}

/* The dispatching hook: role by ACTION. Installed on BOTH shard cores via
 * the runtime's core template — which is exactly what makes the isolation
 * assertions meaningful: if any core consulted the wrong role (or
 * authorized an internal pump-sub), the role's call/credential log would
 * show it. */
static void
arig_authorize(void *ctx, const moqr_auth_request_t *req,
               moqr_auth_verdict_t *out)
{
    arig_t *r = ctx;
    role_auth_t *role;
    switch (req->action) {
    case MOQR_AUTH_SUBSCRIBE:
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
        role = &r->sub_auth;
        break;
    case MOQR_AUTH_PUBLISH_NAMESPACE:
    case MOQR_AUTH_PUBLISH:
        role = &r->pub_auth;
        break;
    default:
        out->decision = MOQR_AUTH_ALLOW;   /* setups etc. */
        return;
    }
    role_record(role, req);
    out->decision = role->decision;
    out->reason = role->decision == MOQR_AUTH_DENY ? MOQR_AUTH_REASON_POLICY
                                                   : MOQR_AUTH_REASON_OK;
    out->revalidate_after_us =
        role->decision == MOQR_AUTH_ALLOW ? role->lease_us : 0;
    if (role->decision == MOQR_AUTH_DENY && role->deny_code != 0) {
        out->error_code = role->deny_code;
        /* A revoked ACTIVE subscribe grant ends a subscription: that terminal
         * is PUBLISH_DONE-domain and must be stated as a tagged descriptor.
         * The scalar beside it stays REQUEST_ERROR-domain and is never read as
         * a status. */
        {
            moqr_pd_desc_t t;
            if (moqr_pd_desc_extension(role->deny_code, &t) ==
                MOQR_OK) {
                out->revoke_terminal = t;
            }
        }
    }
    if (role->decision == MOQR_AUTH_DEFER) {
        out->ticket = role->ticket;
    }
}

/* bind_max_sgs / pump_sg_slots: 0 = library default. The recovery scenario
 * pins the DESTINATION bind's downstream subgroup pool at 1 while keeping
 * the owner's cross-shard progress capacity explicitly above the two open
 * records, so the destination slot table is provably the only constraint. */
static moqr_result_t
arig_create_ex5(arig_t *r, ca_t *a, uint32_t bind_max_sgs,
                uint32_t pump_sg_slots, bool live_visibility,
                uint32_t log_max_groups, uint32_t demand_channel_entries,
                uint64_t log_max_bytes)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    role_init(&r->sub_auth);
    role_init(&r->pub_auth);
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.shards = 2;
    cfg.admit_remote_demand = true;   /* test-owned runtime ONLY */
    cfg.core_cfg.log_budget.max_groups = log_max_groups;
    cfg.core_cfg.log_budget.max_bytes = log_max_bytes;
    cfg.core_cfg.linger_us = 500;
    cfg.core_cfg.authorize = arig_authorize;
    cfg.core_cfg.authorize_ctx = r;
    cfg.bind_cfg.max_open_subgroups = bind_max_sgs;
    cfg.pump_subgroup_slots = pump_sg_slots;
    cfg.live_visibility = live_visibility;
    cfg.demand_channel_entries = demand_channel_entries;   /* 0 = default */
    return moqr_shards_create(&cfg, &r->s);
}

static moqr_result_t
arig_create_ex4(arig_t *r, ca_t *a, uint32_t bind_max_sgs,
                uint32_t pump_sg_slots, bool live_visibility,
                uint32_t log_max_groups, uint32_t demand_channel_entries)
{
    return arig_create_ex5(r, a, bind_max_sgs, pump_sg_slots,
                           live_visibility, log_max_groups,
                           demand_channel_entries, 1 << 20);
}

static moqr_result_t
arig_create_ex3(arig_t *r, ca_t *a, uint32_t bind_max_sgs,
                uint32_t pump_sg_slots, bool live_visibility,
                uint32_t log_max_groups)
{
    return arig_create_ex4(r, a, bind_max_sgs, pump_sg_slots,
                           live_visibility, log_max_groups, 0);
}

static moqr_result_t
arig_create_ex2(arig_t *r, ca_t *a, uint32_t bind_max_sgs,
                uint32_t pump_sg_slots, bool live_visibility)
{
    return arig_create_ex3(r, a, bind_max_sgs, pump_sg_slots,
                           live_visibility, 4);
}

static moqr_result_t
arig_create_ex(arig_t *r, ca_t *a, uint32_t bind_max_sgs,
               uint32_t pump_sg_slots)
{
    return arig_create_ex2(r, a, bind_max_sgs, pump_sg_slots, false);
}

static moqr_result_t
arig_create(arig_t *r, ca_t *a)
{
    return arig_create_ex(r, a, 0, 0);
}

static void
arig_destroy(arig_t *r)
{
    moqr_shards_destroy(r->s);
    for (int i = 0; i < AMAX_CONNS; i++) {
        if (r->conns[i].used) {
            moq_simpair_destroy(r->conns[i].sp);
            r->conns[i].used = false;
        }
    }
}

static aconn_t *
arig_connect(arig_t *r, uint16_t shard, moq_version_t version)
{
    int slot = -1;
    for (int i = 0; i < AMAX_CONNS; i++) {
        if (!r->conns[i].used) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        return NULL;
    }
    aconn_t *cn = &r->conns[slot];
    memset(cn, 0, sizeof(*cn));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc->vt;
    cfg.seed = 0xA07 + (uint64_t)slot;
    cfg.version = version;
    cfg.server_streaming_objects = r->streaming;
    cfg.client_streaming_objects = r->streaming;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    cfg.max_actions = r->conn_max_actions;   /* 0 = session default */
    cfg.max_events = r->conn_max_events;
    cfg.server_max_open_subgroups = r->conn_max_open_subgroups;
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return NULL;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(moqr_shards_bind(r->s, shard), cn->rsess,
                            version) !=
            MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        return NULL;
    }
    cn->shard = shard;
    cn->used = true;
    return cn;
}

/* One deterministic pump cycle: transport steps for every connection, then
 * ONE round of the multi-shard runtime (both binds pump inside it, plus the
 * cross-shard phases under the round barrier). */
static void
arig_cycle(arig_t *r)
{
    r->now += 1000;
    for (int i = 0; i < AMAX_CONNS; i++) {
        aconn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_shards_step(r->s, r->now);
}

static void
arig_pump(arig_t *r, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        arig_cycle(r);
    }
}

/* -- peer observation --------------------------------------------------------- */

typedef struct aps {
    moq_subscription_t up_sub;   /* relay's upstream sub at the publisher */
    bool               up_seen;
    int                up_subs;
    int                unsubscribed;
    bool               namespace_accepted;
    int                ns_cancelled;
    uint64_t           ns_cancelled_code;
    bool               subscribe_ok;
    int                subscribe_errors;
    uint64_t           sub_error_code;
    bool               done_seen;
    int                done_count;
    uint64_t           done_code;
    int                objects;      /* completed objects, either mode */
    uint8_t            obj_byte[8];  /* first byte of each, in order    */
    uint8_t            last_byte;
    size_t             last_len;
    int                chunks;       /* streamed data chunks            */
    int                sg_finished;  /* clean subgroup FINs observed    */
    uint64_t           sg_fin_group[8];  /* their group ids, in order    */
    uint64_t           sg_fin_sub[8];    /* their subgroup ids           */
    bool               saw_reset;
#ifdef MOQ_EVENT_SUBGROUP_RESET
    int                sg_reset;         /* abnormal subgroup terminals     */
    uint64_t           sg_reset_group;   /* last one's identity + code      */
    uint64_t           sg_reset_sub;
    uint64_t           sg_reset_code;
#endif
    uint64_t           cur_bytes;    /* streamed object reassembly      */
    uint8_t            cur_first;
} aps_t;

static void
aps_drain(arig_t *r, aconn_t *cn, aps_t *ps, bool auto_accept)
{
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                ps->up_subs++;
                ps->up_sub = ev->u.subscribe_request.sub;
                if (auto_accept) {
                    moq_accept_subscribe_cfg_t cfg;
                    moq_accept_subscribe_cfg_init(&cfg);
                    A_CHECK(r, moq_session_accept_subscribe(
                                   cn->peer, ev->u.subscribe_request.sub,
                                   &cfg, r->now) == MOQ_OK);
                    ps->up_seen = true;
                }
                break;
            case MOQ_EVENT_UNSUBSCRIBED:
                ps->unsubscribed++;
                break;
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                ps->namespace_accepted = true;
                break;
            case MOQ_EVENT_NAMESPACE_CANCELLED:
                ps->ns_cancelled++;
                ps->ns_cancelled_code = ev->u.namespace_cancelled.error_code;
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                ps->subscribe_ok = true;
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                ps->subscribe_errors++;
                ps->sub_error_code = ev->u.subscribe_error.error_code;
                break;
            case MOQ_EVENT_SUBGROUP_FINISHED:
                if (ps->sg_finished < 8) {
                    ps->sg_fin_group[ps->sg_finished] =
                        ev->u.subgroup_finished.group_id;
                    ps->sg_fin_sub[ps->sg_finished] =
                        ev->u.subgroup_finished.subgroup_id;
                }
                ps->sg_finished++;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                ps->done_seen = true;
                ps->done_count++;
                ps->done_code = ev->u.subscribe_done.status_code;
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                ps->objects++;
                ps->last_len = 0;
                if (ev->u.object_received.payload != NULL) {
                    ps->last_byte =
                        moq_rcbuf_data(ev->u.object_received.payload)[0];
                    ps->last_len =
                        moq_rcbuf_len(ev->u.object_received.payload);
                    if (ps->objects <= 8) {
                        ps->obj_byte[ps->objects - 1] = ps->last_byte;
                    }
                }
                break;
            case MOQ_EVENT_OBJECT_CHUNK: {
                const moq_object_chunk_event_t *oc = &ev->u.object_chunk;
                if (oc->begin) {
                    ps->cur_bytes = 0;
                    ps->cur_first = 0;
                }
                if (oc->chunk != NULL && moq_rcbuf_len(oc->chunk) > 0) {
                    ps->chunks++;
                    if (ps->cur_bytes == 0) {
                        ps->cur_first = moq_rcbuf_data(oc->chunk)[0];
                    }
                    ps->cur_bytes += moq_rcbuf_len(oc->chunk);
                }
                if (oc->end) {
                    if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                        ps->objects++;
                        ps->last_byte = ps->cur_first;
                        ps->last_len = (size_t)ps->cur_bytes;
                    } else if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                        ps->saw_reset = true;
                    }
                }
                break;
            }
#ifdef MOQ_EVENT_SUBGROUP_RESET
            case MOQ_EVENT_SUBGROUP_RESET:
                ps->sg_reset++;
                ps->sg_reset_group = ev->u.subgroup_reset.group_id;
                ps->sg_reset_sub = ev->u.subgroup_reset.subgroup_id;
                ps->sg_reset_code = ev->u.subgroup_reset.error_code;
                break;
#endif
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* -- client-side operations ---------------------------------------------------- */

/* Publisher announce with an optional credential. */
static void
apub_announce(arig_t *r, aconn_t *pub, moq_bytes_t *nsp, uint32_t count,
              const moq_auth_token_t *tok, size_t ntok)
{
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = count };
    pcfg.auth_tokens = tok;
    pcfg.auth_token_count = ntok;
    moq_announcement_t ann;
    A_CHECK(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now,
                                             &ann) == MOQ_OK);
}

/* Subscriber subscribe with an optional credential. */
static void
asub_subscribe(arig_t *r, aconn_t *sub, moq_bytes_t *nsp, uint32_t count,
               const char *name, const moq_auth_token_t *tok, size_t ntok,
               moq_subscription_t *out)
{
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = count };
    scfg.track_name = B(name);
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.auth_tokens = tok;
    scfg.auth_token_count = ntok;
    A_CHECK(r, moq_session_subscribe(sub->peer, &scfg, r->now, out) ==
                   MOQ_OK);
}

/* Total occupancy across every directed demand channel (0 = quiesced). */
static uint32_t
adch_total(arig_t *r)
{
    uint32_t n = 0;
    for (uint16_t i = 0; i < 2; i++) {
        for (uint16_t d = 0; d < 2; d++) {
            n += moqr_shards_debug_demand_channel_pending(r->s, i, d);
        }
    }
    return n;
}

/* Sum of all demand-channel enqueue counters on `shard` (any kind). */
static uint64_t
aenq_total(arig_t *r, uint16_t shard)
{
    moqr_shards_stats_t st;
    if (moqr_shards_get_stats(r->s, shard, &st) != MOQR_OK) {
        return UINT64_MAX;
    }
    uint64_t n = 0;
    for (uint32_t k = 0; k < MOQR_SHARDS_MSG__COUNT; k++) {
        n += st.enqueued[k];
    }
    return n;
}

/* The rig's converged idle state: no demands, no pump-subs, no queued
 * messages, no open objects, and both quiet-teardown terminal counters
 * still zero (policy teardown is never a loss). */
static void
a_assert_drained(arig_t *r)
{
    moqr_shards_stats_t st0, st1;
    A_CHECK(r, moqr_shards_get_stats(r->s, 0, &st0) == MOQR_OK);
    A_CHECK(r, moqr_shards_get_stats(r->s, 1, &st1) == MOQR_OK);
    A_CHECK(r, st1.pending_demands == 0);
    A_CHECK(r, moqr_shards_debug_owner_pump_subs(r->s, 0) == 0);
    A_CHECK(r, st0.owner_progress_slots == 0);
    A_CHECK(r, st1.requester_open_objects == 0);
    A_CHECK(r, adch_total(r) == 0);
    A_CHECK(r, st0.term_capacity == 0 && st0.term_overrun == 0);
    A_CHECK(r, st1.term_capacity == 0 && st1.term_overrun == 0);
}

/* -- shared stage: publisher on shard 0, announced; subscriber conn on 1 ---- */

static void
arig_stage(arig_t *r, moq_version_t version, aconn_t **pub, aconn_t **sub,
           moq_bytes_t *nsp, const moq_auth_token_t *pub_tok, size_t npub)
{
    *pub = arig_connect(r, 0, version);
    *sub = arig_connect(r, 1, version);
    A_CHECK(r, *pub != NULL && *sub != NULL);
    arig_pump(r, 4);   /* handshakes (setup ALLOWs) */
    apub_announce(r, *pub, nsp, 1, pub_tok, npub);
    arig_pump(r, 6);   /* announce -> shard 0 core -> shard 1 mirror */
    moqr_shards_jinfo_t j;
    moqr_shards_debug_journal(r->s, 1, nsp, 1, &j);
    A_CHECK(r, j.present && j.mirror == 0);   /* shard 1 mirrors shard 0 */
}

/* =============================== scenarios ================================ */

/* (1) Requester DENY: the subscriber gets SUBSCRIBE_ERROR with the
 * configured wire code, and NO cross-shard state may exist anywhere — no
 * DEMAND in either channel, no owner pump-sub, owner track state untouched,
 * and the denial counted on the REQUESTER core under DENY (never ALLOW). */
static int
scenario_requester_deny(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: deny rig create\n");
        return 1;
    }
    aconn_t *pub, *sub;
    moq_bytes_t nsp[1] = { B("auD") };
    arig_stage(&r, MOQ_VERSION_DRAFT_18, &pub, &sub, nsp, NULL, 0);

    moqr_core_stats_t own0;
    moqr_core_get_stats(moqr_shards_core(r.s, 0), &own0);

    r.sub_auth.decision = MOQR_AUTH_DENY;
    r.sub_auth.deny_code = 0x20;
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 6);

    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_errors == 1);
    A_CHECK(&r, sps.sub_error_code == 0x20);
    A_CHECK(&r, !sps.subscribe_ok);

    /* No demand became observable anywhere, in flight or counted. */
    moqr_shards_stats_t st0, st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &st0) == MOQR_OK);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_DEMAND] == 0);
    A_CHECK(&r, aenq_total(&r, 0) == 0 && aenq_total(&r, 1) == 0);
    A_CHECK(&r, st1.pending_demands == 0);
    A_CHECK(&r, moqr_shards_debug_owner_pump_subs(r.s, 0) == 0);
    A_CHECK(&r, adch_total(&r) == 0);

    /* Owner track state unaffected (no track, no sub, no churn). */
    moqr_core_stats_t own1;
    moqr_core_get_stats(moqr_shards_core(r.s, 0), &own1);
    A_CHECK(&r, own1.tracks == own0.tracks);
    A_CHECK(&r, own1.subs == own0.subs);
    A_CHECK(&r, own1.ingested_total == own0.ingested_total);

    /* Counted on the REQUESTER core, as DENY, never ALLOW. */
    moqr_core_stats_t req;
    moqr_core_get_stats(moqr_shards_core(r.s, 1), &req);
    A_CHECK(&r, req.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY] == 1);
    A_CHECK(&r,
            req.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_ALLOW] == 0);
    A_CHECK(&r, r.sub_auth.calls == 1);

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards requester_deny\n");
    }
    return r.failures;
}

/* (2) Requester ALLOW: the full admitted sequence, asserted at each stage
 * through the exact per-kind enqueue counters and gauges — never inferred
 * from eventual delivery. ACK-before-data is pinned by a checkpoint where
 * the ACK counter is 1 while every data counter is still 0. */
static int
scenario_requester_allow(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: allow rig create\n");
        return 1;
    }
    aconn_t *pub, *sub;
    moq_bytes_t nsp[1] = { B("auA") };
    arig_stage(&r, MOQ_VERSION_DRAFT_18, &pub, &sub, nsp, NULL, 0);

    /* client SUBSCRIBE -> (same round) requester records + pushes DEMAND. */
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_cycle(&r);
    moqr_shards_stats_t st0, st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_DEMAND] == 1);
    A_CHECK(&r, st1.pending_demands == 1);
    A_CHECK(&r, moqr_shards_debug_demand_channel_pending(r.s, 1, 0) == 1);
    A_CHECK(&r, moqr_shards_debug_owner_pump_subs(r.s, 0) == 0);
    A_CHECK(&r, r.sub_auth.calls == 1);

    /* Owner admits: the pump-sub exists (PARKED behind the publisher's
     * accept) and NOTHING has been enqueued by the owner yet. */
    arig_cycle(&r);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &st0) == MOQR_OK);
    A_CHECK(&r, moqr_shards_debug_owner_pump_subs(r.s, 0) == 1);
    A_CHECK(&r, st0.pump_subs_parked == 1);
    A_CHECK(&r, aenq_total(&r, 0) == 0);
    /* The pump-sub was NOT re-authorized: still exactly one SUBSCRIBE
     * verdict, the requester's. */
    A_CHECK(&r, r.sub_auth.calls == 1);

    /* Publisher accepts the relay's upstream subscribe -> ACK crosses,
     * and at this checkpoint the ACK exists while every data counter is
     * still zero: ACK-before-data, asserted. (One more cycle first: the
     * owner's wire SUBSCRIBE reaches the publisher on the next transport
     * step.) */
    arig_cycle(&r);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    arig_pump(&r, 2);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &st0) == MOQR_OK);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_ACK] == 1);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_OBJ] == 0);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] == 0);
    A_CHECK(&r, st0.pump_subs_active == 1);

    /* Requester activation: the subscriber is accepted on the wire. */
    arig_pump(&r, 2);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);
    A_CHECK(&r, st1.pending_demands == 1);   /* ACKED entry stays counted */

    /* Data: one whole 64-byte object crosses exactly once, byte-faithful. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sgh) == MOQ_OK);
    uint8_t body[64];
    memset(body, 0xA7, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sgh, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    arig_pump(&r, 6);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &st0) == MOQR_OK);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_OBJ] == 1);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_ACK] == 1);
    A_CHECK(&r, st0.pump_messages == 1);
    A_CHECK(&r, st0.pump_bytes == 64);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 1);
    A_CHECK(&r, sps.last_byte == 0xA7);
    A_CHECK(&r, sps.last_len == 64);

    /* Terminal behavior: the idle teardown funnels through exactly one
     * UNDEMAND and drains everything, with zero loss terminals. */
    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    arig_pump(&r, 8);   /* past the 500us linger */
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_UNDEMAND] == 1);
    a_assert_drained(&r);

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards requester_allow\n");
    }
    return r.failures;
}

/* (3) Requester DEFER: the request parks on shard 1 and NOTHING crosses —
 * no DEMAND, no pump-sub, no data — until moqr_bind_auth_resolve(ALLOW)
 * completes it; only then does the admitted sequence proceed. */
static int
scenario_requester_defer(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: defer rig create\n");
        return 1;
    }
    aconn_t *pub, *sub;
    moq_bytes_t nsp[1] = { B("auF") };
    arig_stage(&r, MOQ_VERSION_DRAFT_18, &pub, &sub, nsp, NULL, 0);

    r.sub_auth.decision = MOQR_AUTH_DEFER;
    r.sub_auth.ticket = 777;
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 6);

    /* Parked: the subscriber has NO answer, and no cross-shard state of
     * any kind exists — over several rounds, not one. */
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, !sps.subscribe_ok && sps.subscribe_errors == 0);
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_DEMAND] == 0);
    A_CHECK(&r, st1.pending_demands == 0);
    A_CHECK(&r, moqr_shards_debug_owner_pump_subs(r.s, 0) == 0);
    A_CHECK(&r, adch_total(&r) == 0);
    A_CHECK(&r, aenq_total(&r, 0) == 0 && aenq_total(&r, 1) == 0);
    A_CHECK(&r, r.sub_auth.calls == 1);

    /* Resolve ALLOW on the REQUESTER's binding: only now may the demand
     * round-trip begin, and the full sequence completes. */
    moqr_auth_verdict_t verdict;
    memset(&verdict, 0, sizeof(verdict));
    verdict.decision = MOQR_AUTH_ALLOW;
    A_CHECK(&r, moqr_bind_auth_resolve(moqr_shards_bind(r.s, 1), 777,
                                       &verdict, r.now) == MOQR_OK);
    arig_pump(&r, 3);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_DEMAND] == 1);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen);
    arig_pump(&r, 4);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);
    moqr_shards_stats_t st0;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &st0) == MOQR_OK);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_ACK] == 1);

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards requester_defer\n");
    }
    return r.failures;
}

/* (4) Requester revalidation DENY mid-object: an ALLOW+lease grant goes
 * ACTIVE, an OPEN object with one chunk crosses live, then the verifier
 * flips to DENY and the lease elapses. The complete QUIET cleanup chain:
 * grant revokes -> wire SUBSCRIBE_DONE with the denial code + the begun
 * downstream subgroup resets -> the partial requester record is abandoned
 * (retained bytes return to zero) -> the idle linger produces exactly one
 * UNDEMAND -> owner pump-sub and demand state retire -> progress tables and
 * channels drain — and the capacity/overrun terminals stay ZERO (policy
 * teardown, never a loss). */
static int
scenario_reval_deny_mid_object(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: reval rig create\n");
        return 1;
    }
    r.streaming = true;   /* live-edge: OPEN records + chunk events */
    aconn_t *pub, *sub;
    moq_bytes_t nsp[1] = { B("auR") };
    arig_stage(&r, MOQ_VERSION_DRAFT_18, &pub, &sub, nsp, NULL, 0);

    r.sub_auth.lease_us = 3000;
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 3);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen);
    arig_pump(&r, 4);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* The mid-object state: an OPEN object, first half only, held. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sgh) == MOQ_OK);
    A_CHECK(&r, moq_session_begin_object(pub->peer, sgh, 0, 128, r.now) ==
                    MOQ_OK);
    uint8_t half[64];
    memset(half, 0xC4, sizeof(half));
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, half, sizeof(half), &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sgh, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    arig_pump(&r, 6);
    moqr_shards_stats_t st0, st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &st0) == MOQR_OK);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] == 1);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK] == 1);
    A_CHECK(&r, st0.enqueued[MOQR_SHARDS_MSG_OBJ_END] == 0);
    A_CHECK(&r, st1.requester_open_objects == 1);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.chunks >= 1);   /* the live edge reached the client */
    moqr_core_stats_t rq;
    moqr_core_get_stats(moqr_shards_core(r.s, 1), &rq);
    A_CHECK(&r, rq.retained_bytes > 0);   /* the partial record is held */

    /* Flip the requester verifier to DENY and let the lease elapse. */
    r.sub_auth.decision = MOQR_AUTH_DENY;
    /* PUBLISH_DONE domain, not REQUEST_ERROR: 0x7 is unassigned in BOTH
     * drafts' PUBLISH_DONE registries, so it is a legal relay-authored
     * extension and reaches the peer verbatim. */
    r.sub_auth.deny_code = 0x7;
    arig_pump(&r, 10);

    /* The local client saw the revocation: SUBSCRIBE_DONE with the denial
     * code, and the begun downstream subgroup reset. */
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.done_seen);
    A_CHECK(&r, sps.done_code == 0x7);
    A_CHECK(&r, sps.saw_reset);

    /* The full quiet drain: partial record abandoned, retained bytes zero,
     * one UNDEMAND, owner + requester state gone, zero loss terminals. */
    moqr_core_get_stats(moqr_shards_core(r.s, 1), &rq);
    A_CHECK(&r, rq.retained_bytes == 0);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_UNDEMAND] == 1);
    a_assert_drained(&r);
    moqr_core_stats_t req;
    moqr_core_get_stats(moqr_shards_core(r.s, 1), &req);
    A_CHECK(&r, req.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY] >= 1);

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards reval_deny_mid_object\n");
    }
    return r.failures;
}

/* (5) Owner announce revalidation DENY: the owner's PUBLISH_NAMESPACE grant
 * revokes mid-stream. The announce is withdrawn and sourced tracks purged,
 * NS_GONE propagates and the mirror converges away, the remote demand
 * terminates GOING_AWAY at the requester (open object abandoned), owner
 * pump-sub and requester demand both drain, and the LIVE publisher receives
 * exactly one namespace cancellation — repeating the withdrawal does not
 * duplicate it. Per draft: d16 carries the chosen GOING_AWAY code on the
 * cancel; d18's request-bidi cancel exposes only the fixed CANCELLED code. */
static int
scenario_owner_announce_reval(moq_version_t version, uint64_t expect_cancel,
                              const char *name)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: owner reval rig create (%s)\n", name);
        return 1;
    }
    r.streaming = true;
    r.pub_auth.lease_us = 3000;   /* the announce grant under test */
    aconn_t *pub, *sub;
    moq_bytes_t nsp[1] = { B("auO") };
    arig_stage(&r, version, &pub, &sub, nsp, NULL, 0);

    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 3);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen);
    A_CHECK(&r, pps.namespace_accepted);
    arig_pump(&r, 4);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Begin an object so the revocation lands mid-stream. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sgh) == MOQ_OK);
    A_CHECK(&r, moq_session_begin_object(pub->peer, sgh, 0, 128, r.now) ==
                    MOQ_OK);
    uint8_t half[64];
    memset(half, 0xE2, sizeof(half));
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, half, sizeof(half), &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sgh, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    arig_pump(&r, 6);
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.requester_open_objects == 1);

    /* Deny the owner-side revalidation: the chosen code is GOING_AWAY. */
    r.pub_auth.decision = MOQR_AUTH_DENY;
    r.pub_auth.deny_code = 0x6;
    arig_pump(&r, 12);

    /* The announce is withdrawn and the mirror converged away. */
    moqr_shards_jinfo_t j;
    moqr_shards_debug_journal(r.s, 1, nsp, 1, &j);
    A_CHECK(&r, !j.present || (j.candidates == 0 && j.mirror == -1));
    /* The publisher received EXACTLY ONE cancellation, per-draft code. */
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, false);
    A_CHECK(&r, pps.ns_cancelled == 1);
    A_CHECK(&r, pps.ns_cancelled_code == expect_cancel);
    /* The requester terminated with PUBLISH_DONE TRACK_ENDED (the revoked
     * announce stops publishing), its open object abandoned. */
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.done_seen);
    A_CHECK(&r, sps.done_code == 0x2);
    A_CHECK(&r, sps.saw_reset);
    a_assert_drained(&r);

    /* Repeat the withdrawal/revalidation: no duplicate cancel. */
    moqr_ns_t rns = { nsp, 1 };
    A_CHECK(&r, moqr_core_force_withdraw(moqr_shards_core(r.s, 0), rns, 0x6,
                                         r.now) == MOQR_OK);
    arig_pump(&r, 6);
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, false);
    A_CHECK(&r, pps.ns_cancelled == 0);   /* nothing new: still one total */

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards owner_announce_reval_%s\n", name);
    }
    return r.failures;
}

/* (7) Token isolation: distinguishable credentials never cross roles or
 * shards. The requester verifier sees ONLY the subscriber's credential; the
 * publisher verifier ONLY the publisher's; the owner's internal pump-sub is
 * never authorized (one SUBSCRIBE verdict total); and the crossing DEMAND
 * carries canon name bytes only (its enqueue happened without any further
 * verifier consultation anywhere). */
static int
scenario_token_isolation(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: token rig create\n");
        return 1;
    }
    aconn_t *pub, *sub;
    moq_bytes_t nsp[1] = { B("auT") };
    moq_auth_token_t pub_tok = { .token_type = 5,
                                 .token_value = B("PUBCRED") };
    arig_stage(&r, MOQ_VERSION_DRAFT_18, &pub, &sub, nsp, &pub_tok, 1);

    moq_auth_token_t sub_tok = { .token_type = 9,
                                 .token_value = B("SUBCRED") };
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", &sub_tok, 1, &sh);
    arig_pump(&r, 3);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen);
    arig_pump(&r, 4);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* The requester verifier: exactly one call (the client subscribe; the
     * owner's pump-sub was never authorized), shown exactly the
     * subscriber's credential. */
    A_CHECK(&r, r.sub_auth.calls == 1);
    A_CHECK(&r, r.sub_auth.tok_seen == 1);
    A_CHECK(&r, r.sub_auth.tok_type[0] == 9);
    A_CHECK(&r, r.sub_auth.tok_first[0] == (uint8_t)'S');
    /* The owner verifier: the publisher's announce only, its credential
     * only — the subscriber's bytes never reached shard 0's verifier. */
    A_CHECK(&r, r.pub_auth.calls == 1);
    A_CHECK(&r, r.pub_auth.tok_seen == 1);
    A_CHECK(&r, r.pub_auth.tok_type[0] == 5);
    A_CHECK(&r, r.pub_auth.tok_first[0] == (uint8_t)'P');
    /* The demand crossed and was admitted with NO further consultation:
     * credentials are requester-local, never channel cargo. */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_DEMAND] == 1);
    A_CHECK(&r, moqr_shards_debug_owner_pump_subs(r.s, 0) == 1);

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards token_isolation\n");
    }
    return r.failures;
}

/* (7) Cross-shard BIND_SG recovery to the ORIGINAL subscriber. The
 * destination bind's downstream subgroup pool is 1 and the owner's
 * cross-shard progress capacity is explicitly above the two open records,
 * so the destination slot table is provably the only constraint. Publisher
 * on shard 1, subscriber on shard 0; two subgroups A/B, one whole object
 * each, both left OPEN. A delivers and holds the only slot; B's delivery
 * refuses BIND_SG exactly once and does NOT park (slot-release is the only
 * re-arm). FIN A alone must push A's SG_SEAL across the boundary, ingest it
 * on shard 0, free/re-arm the destination slot, and deliver B to the SAME
 * subscriber with no new publication. This is the K=2 twin of the
 * single-shard ready_bind_sg_rearm recovery contract. */
static int
scenario_bindsg_recovery(bool same_group)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex(&r, &a, 1 /* dest bind subgroup pool */,
                       4 /* owner progress slots: > 2 open records */) !=
        MOQR_OK) {
        printf("FAIL: bindsg recovery rig create\n");
        return 1;
    }
    /* Publisher on SHARD 1, subscriber on SHARD 0 (spec order — the
     * opposite of arig_stage). The subscriber is the first (only) conn on
     * bind 0: bind slot 0. */
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    arig_pump(&r, 4);
    moq_bytes_t nsp[1] = { B("auR") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    arig_pump(&r, 6);
    moqr_shards_jinfo_t j;
    moqr_shards_debug_journal(r.s, 0, nsp, 1, &j);
    A_CHECK(&r, j.present && j.mirror == 1);   /* shard 0 mirrors shard 1 */

    /* Subscribe from shard 0; the owner (shard 1) admits; the publisher
     * accepts the relay's upstream sub; the subscriber activates. */
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 3);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    arig_pump(&r, 4);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Two distinct RECORDS, one whole object each, BOTH LEFT OPEN. Two
     * shapes, both required to recover: distinct groups (the single-shard
     * precedent: A=(g0,sg0), B=(g1,sg0)) and same-group sibling subgroups
     * (A=(g0,sg0,obj0), B=(g0,sg1,obj1) — object ids stay unique within the
     * group; a REUSED id would be a duplicate identity the owner correctly
     * refuses to forward, which is a scenario-authoring error, not a relay
     * defect). */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sga, sgb;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc, r.now,
                                          &sga) == MOQ_OK);
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = same_group ? 0 : 1;
    sgc.subgroup_id = same_group ? 1 : 0;
    sgc.publisher_priority = 100;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc, r.now,
                                          &sgb) == MOQ_OK);
    uint8_t body_a[64], body_b[48];
    memset(body_a, 0xA1, sizeof(body_a));
    memset(body_b, 0xB2, sizeof(body_b));
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body_a, sizeof(body_a), &pl) ==
                    MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sga, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body_b, sizeof(body_b), &pl) ==
                    MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sgb, same_group ? 1 : 0,
                                         pl, r.now) == MOQ_OK);
    moq_rcbuf_decref(pl);
    arig_pump(&r, 8);

    /* Mid-state: both objects crossed the channel (the boundary is NOT the
     * constraint) and both records hold owner progress slots (< capacity 4);
     * A delivered to the subscriber and holds the only destination slot;
     * B refused BIND_SG exactly once, unparked. */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_OBJ] == 2);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 0);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 1);
    A_CHECK(&r, sps.last_byte == 0xA1 && sps.last_len == 64);
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    uint64_t bc[3];
    moqr_bind_debug_conn_blocked_counts(b0, 0, bc);
    A_CHECK(&r, bc[0] == 0 && bc[1] == 0);
    A_CHECK(&r, bc[2] == 1);   /* exactly one BIND_SG refusal */
    bool ready, parked;
    uint8_t reason;
    moqr_bind_debug_dl_state(b0, 0, &ready, &parked, &reason);
    A_CHECK(&r, !parked);      /* BIND_SG never parks */
    /* The LIVE aggregate agrees: one live conn on this bind, counted. */
    moqr_bind_blocked_agg_t agg;
    A_CHECK(&r, moqr_bind_debug_blocked_aggregate(b0, &agg) == MOQR_OK);
    A_CHECK(&r, agg.live_conns == 1);
    A_CHECK(&r, agg.conns_bind_sg == 1 && agg.bind_sg_total == 1);

    /* The refusal must not respin while nothing releases the slot. */
    arig_pump(&r, 6);
    moqr_bind_debug_conn_blocked_counts(b0, 0, bc);
    A_CHECK(&r, bc[2] == 1);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 0);

    /* FIN A ONLY: the seal crosses, ingests on shard 0, frees the slot,
     * re-arms, and B reaches the ORIGINAL subscriber — no new publication. */
    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sga, r.now) == MOQ_OK);
    arig_pump(&r, 10);
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 1);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_OBJ] == 2);   /* nothing new */
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 1);
    A_CHECK(&r, sps.last_byte == 0xB2 && sps.last_len == 48);
    moqr_bind_debug_conn_blocked_counts(b0, 0, bc);
    A_CHECK(&r, bc[2] == 1);   /* recovery added no further refusal */
    /* Liveness through recovery: the subscriber connection MUST remain in
     * the live aggregate with its historical counter INTACT — these are
     * lifetime counters over live conns, so a 1 -> 0 transition could only
     * mean the conn closed or its slot was reclaimed, never "recovered". */
    A_CHECK(&r, moqr_bind_debug_blocked_aggregate(b0, &agg) == MOQR_OK);
    A_CHECK(&r, agg.live_conns == 1);
    A_CHECK(&r, agg.conns_bind_sg == 1 && agg.bind_sg_total == 1);
    A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));

    /* Hygiene: the flow tears down through the usual funnel. */
    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sgb, r.now) == MOQ_OK);
    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    arig_pump(&r, 8);

    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_recovery_%s\n",
               same_group ? "same_group" : "cross_group");
    }
    return r.failures;
}

/* (8) The transport-observed failure shape, deterministically: capacity 2,
 * FOUR records all left open — two delivered + held, two PENDING (at least
 * one counted BIND_SG refusal; one blocked pass may cover both pending
 * records) — then FIN ALL FOUR AT ONCE. Every record must complete to the
 * ORIGINAL subscriber with its exact group identity ({g0,g1} pre-FIN,
 * {g2,g3} post-FIN — counts alone could be satisfied by a duplicate while
 * a record stays missing), the connection must stay LIVE, and the
 * historical BIND_SG counters must persist — a live-aggregate transition
 * to zero would mean the conn closed/was reclaimed, which is exactly the
 * defect shape under investigation, never "recovery". */
static int
scenario_bindsg_recovery_burst(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex(&r, &a, 2 /* dest pool */, 8 /* progress slots */) !=
        MOQR_OK) {
        printf("FAIL: bindsg burst rig create\n");
        return 1;
    }
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    arig_pump(&r, 4);
    moq_bytes_t nsp[1] = { B("auB") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    arig_pump(&r, 6);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 3);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    arig_pump(&r, 4);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Four records (one group each), one whole object each, ALL LEFT OPEN. */
    moq_subgroup_handle_t sg[4];
    for (uint64_t g = 0; g < 4; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg[g]) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xC0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg[g], 0, pl,
                                             r.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    arig_pump(&r, 10);

    /* Steady blocked state: exactly the pool delivers; refusals counted on
     * the LIVE conn; nothing parked. */
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 2);
    unsigned gmask = 0;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        gmask |= 1u << (sps.obj_byte[o] - 0xC0);
    }
    A_CHECK(&r, gmask == 0x3u);   /* exactly groups {0,1}, no duplicate */
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    moqr_bind_blocked_agg_t agg;
    A_CHECK(&r, moqr_bind_debug_blocked_aggregate(b0, &agg) == MOQR_OK);
    A_CHECK(&r, agg.live_conns == 1);
    A_CHECK(&r, agg.conns_bind_sg == 1);
    A_CHECK(&r, agg.bind_sg_total >= 1);
    A_CHECK(&r, agg.parked_action_cap == 0 && agg.parked_session_sg == 0);
    uint64_t refusals_at_hold = agg.bind_sg_total;

    /* Quiet window: no respin while every slot stays held. */
    arig_pump(&r, 6);
    A_CHECK(&r, moqr_bind_debug_blocked_aggregate(b0, &agg) == MOQR_OK);
    A_CHECK(&r, agg.bind_sg_total == refusals_at_hold);

    /* FIN ALL FOUR AT ONCE (the transport scenario's drain shape). */
    for (uint64_t g = 0; g < 4; g++) {
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[g], r.now) ==
                        MOQ_OK);
    }
    arig_pump(&r, 14);

    /* FULL recovery to the ORIGINAL subscriber: all four objects, the conn
     * still LIVE, the historical counters INTACT. */
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 2);   /* the two pending records arrived */
    gmask = 0;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        gmask |= 1u << (sps.obj_byte[o] - 0xC0);
    }
    A_CHECK(&r, gmask == 0xCu);   /* exactly groups {2,3}, no duplicate */
    A_CHECK(&r, moqr_bind_debug_blocked_aggregate(b0, &agg) == MOQR_OK);
    A_CHECK(&r, agg.live_conns == 1);
    A_CHECK(&r, agg.conns_bind_sg == 1);
    A_CHECK(&r, agg.bind_sg_total == refusals_at_hold);
    A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 4);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_OBJ] == 4);

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    arig_pump(&r, 8);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_recovery_burst\n");
    }
    return r.failures;
}

/* ===================== event-paced (doorbell) driver ====================== */

/* Production pacing, modelled deterministically: a lane pump runs ONLY when
 * an event earned it — a transport receive on one of the lane\'s connections
 * (any SimPair progress for that shard\'s conns rings its doorbell latch) or
 * a cross-shard push/credit wake (the step\'s pushed_dst_mask). The latch is
 * COALESCED (bool), exactly like the managed doorbell. There is no idle-cap
 * timer here on purpose: the transport evidence shows the stall outliving
 * any idle rescue, so the fixture must reveal work that only an un-rung
 * doorbell would ever serve. Requires live_visibility = true. */
typedef struct ep {
    bool owed[2];
    int  dest_passes;   /* pumps shard 0 (the destination) actually ran */
    /* Wake-neuter seam (fixture sensitivity RED): while nonzero, the next
     * cross-shard push wake TOWARD shard 0 is dropped instead of latched —
     * modelling a lost doorbell. Dropped wakes are counted so the test can
     * release them explicitly (re-ring) and prove recovery resumes. */
    int  suppress_dest;
    int  suppressed;
} ep_t;

static bool
ep_flush_transports(arig_t *r, ep_t *e)
{
    bool any = false;
    for (int i = 0; i < AMAX_CONNS; i++) {
        aconn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        /* Step until nothing DELIVERS, split by DIRECTION: only
         * client->server delivery is a relay RECEIVE and rings this
         * shard's doorbell. Server->client delivery (the relay's own sends
         * reaching the test client) earns NOTHING — production send
         * completion does not wake a lane. Cross-shard pushes remain the
         * only other wake source (the step masks in ep_run). */
        size_t to_server_total = 0;
        for (int k = 0; k < 64; k++) {
            size_t ts = 0, tc = 0;
            if (moq_simpair_step_directional(cn->sp, &ts, &tc) < 0 ||
                ts + tc == 0) {
                break;
            }
            to_server_total += ts;
        }
        if (to_server_total > 0) {
            e->owed[cn->shard] = true;   /* the receive rings the doorbell */
            any = true;
        }
    }
    return any;
}

/* Run earned pumps until the system FREEZES: no doorbell owed and no
 * transport progress. Returns the number of destination (shard 0) pumps.
 * A frozen system with retained deliverable work is exactly the stranding
 * under investigation — the caller asserts which state it froze in. */
static void
ep_run(arig_t *r, ep_t *e)
{
    for (int it = 0; it < 400; it++) {
        /* Flush transports AT THE CURRENT TIME until quiescent: bytes in
         * flight ring doorbells; time does NOT advance here, so sim ticks
         * never masquerade as receive activity. */
        for (int f = 0; f < 64; f++) {
            if (!ep_flush_transports(r, e)) {
                break;
            }
        }
        int s = -1;
        for (int i = 0; i < 2; i++) {
            if (e->owed[i]) {
                s = i;
                break;
            }
        }
        if (s < 0) {
            return;   /* frozen: transports quiescent, no doorbell owed */
        }
        e->owed[s] = false;
        r->now += 1000;   /* time advances per EARNED pump only */
        uint64_t mask = 0;
        A_CHECK(r, moqr_shards_step_shard(r->s, (uint16_t)s, r->now,
                                          &mask) == MOQR_OK);
        if (s == 0) {
            e->dest_passes++;
        }
        for (int d = 0; d < 2; d++) {
            if (mask & (1ull << d)) {
                if (d == 0 && e->suppress_dest > 0) {
                    e->suppress_dest--;
                    e->suppressed++;   /* lost doorbell: latch NOT set */
                    continue;
                }
                e->owed[d] = true;
            }
        }
    }
    A_CHECK(r, !"event-paced driver exceeded its iteration bound");
}

/* One frozen-state observation of the blocked subscriber (bind 0 slot 0). */
typedef struct ep_obs {
    uint32_t occ;        /* downstream subgroup slots held      */
    bool     ready;      /* dl_ready bit                        */
    bool     parked;
    uint64_t refusals;   /* dbg_blocked[BIND_SG]                */
} ep_obs_t;

static void
ep_observe(arig_t *r, ep_obs_t *o)
{
    moqr_bind_t *b0 = moqr_shards_bind(r->s, 0);
    uint64_t bc[3];
    moqr_bind_debug_conn_blocked_counts(b0, 0, bc);
    o->refusals = bc[2];
    o->occ = moqr_bind_debug_conn_open_sgs(b0, 0);
    uint8_t reason;
    moqr_bind_debug_dl_state(b0, 0, &o->ready, &o->parked, &reason);
}

/* Run earned pumps for the SOURCE shard (1) ONLY, leaving the destination
 * wake LATCHED: transports flush and shard-1 doorbells are consumed to
 * quiescence, while owed[0] — however many pushes fed it — coalesces into
 * its single bool and is NEVER consumed here. This is the batching half of
 * the coalesced-wake shape: everything the source will ever push is in the
 * channel before the destination's one permitted step. */
static void
ep_run_src(arig_t *r, ep_t *e)
{
    for (int it = 0; it < 400; it++) {
        for (int f = 0; f < 64; f++) {
            if (!ep_flush_transports(r, e)) {
                break;
            }
        }
        if (!e->owed[1]) {
            return;   /* source quiescent; owed[0] (if any) stays latched */
        }
        e->owed[1] = false;
        r->now += 1000;
        uint64_t mask = 0;
        A_CHECK(r, moqr_shards_step_shard(r->s, 1, r->now, &mask) ==
                       MOQR_OK);
        for (int d = 0; d < 2; d++) {
            if (mask & (1ull << d)) {
                e->owed[d] = true;
            }
        }
    }
    A_CHECK(r, !"source-only driver exceeded its iteration bound");
}

/* One earned destination pump: consume the single latched shard-0 doorbell,
 * step shard 0 exactly once, and report the step's wake mask so the caller
 * can assert what (if any) continuation the step itself requested. */
static void
ep_step_dest(arig_t *r, ep_t *e, uint64_t *mask_out)
{
    A_CHECK(r, e->owed[0]);
    e->owed[0] = false;
    r->now += 1000;
    uint64_t mask = 0;
    A_CHECK(r, moqr_shards_step_shard(r->s, 0, r->now, &mask) == MOQR_OK);
    e->dest_passes++;
    for (int d = 0; d < 2; d++) {
        if (mask & (1ull << d)) {
            e->owed[d] = true;
        }
    }
    if (mask_out != NULL) {
        *mask_out = mask;
    }
}

/* (9) The PRODUCTION interleaving, event-paced: capacity 4, SIX records
 * (four delivered + held, two pending), then the refused record\'s FIN
 * FIRST — its seal earns a pump that re-attempts and re-refuses
 * (bind_sg_total 1 -> 2) while delivery stays 4/6 — followed by the four
 * admitted FINs INCREMENTALLY, each earning only its own pump cascade.
 * Every step records slot occupancy and dl_ready so a stall distinguishes:
 * no slot released yet / slot released but retained work unmarked /
 * dl_ready set with no doorbell continuation. Nothing here force-pumps:
 * if recovery needs a pump no event earned, this test fails exactly where
 * production stalled. */
static int
scenario_bindsg_recovery_paced(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex2(&r, &a, 4 /* dest pool */, 8 /* progress slots */,
                        true /* live_visibility: per-shard stepping */) !=
        MOQR_OK) {
        printf("FAIL: bindsg paced rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);   /* handshakes */
    moq_bytes_t nsp[1] = { B("auP") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Six records: SIX SIBLING SUBGROUPS OF ONE GROUP (the transport
     * scenario's exact shape — and immune to group-budget eviction, which
     * would silently free slots and mask the capacity pressure), one whole
     * object each (object id = subgroup index; ids are group-unique), ALL
     * LEFT OPEN. */
    moq_subgroup_handle_t sg[6];
    for (uint64_t g = 0; g < 6; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = g;
        sgc.publisher_priority = 100;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg[g]) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg[g], g, pl,
                                             r.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        ep_run(&r, &e);   /* each write earns its own cascade */
    }

    /* Frozen hold state: exactly the pool delivered ({g0..g3}), both
     * pending, at least one counted refusal, all four slots held, nothing
     * ready or parked (BIND_SG neither parks nor self-rearms). */
    int objs = 0;
    unsigned gmask = 0;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    objs += sps.objects;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        gmask |= 1u << (sps.obj_byte[o] - 0xD0);
    }
    {
        moqr_shards_stats_t d1;
        moqr_core_stats_t c0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        moqr_core_get_stats(moqr_shards_core(r.s, 0), &c0);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ] == 6);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 0);
        A_CHECK(&r, c0.ingested_total == 6);   /* the boundary is NOT it */
    }
    A_CHECK(&r, objs == 4);
    A_CHECK(&r, gmask == 0xFu);   /* exactly {g0,g1,g2,g3} */
    ep_obs_t ob;
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 4);
    A_CHECK(&r, ob.refusals == 2);   /* one refusal-ended pass per pending */
    A_CHECK(&r, !ob.ready && !ob.parked);
    uint64_t hold_refusals = ob.refusals;

    /* THE ADVERSARIAL EDGE: FIN a PENDING record FIRST. Its seal earns a
     * destination pump; that pump re-derives the pending work, finds every
     * slot still held, and re-refuses: the historical counter must GROW
     * while delivery stays 4/6 and every slot stays occupied. */
    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[4], r.now) ==
                    MOQ_OK);
    ep_run(&r, &e);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 0);   /* still 4/6 */
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 4);
    A_CHECK(&r, ob.refusals == hold_refusals + 1);   /* the 1->2 signature */
    A_CHECK(&r, !ob.parked);
    uint64_t postfin_refusals = ob.refusals;

    /* FIXTURE SENSITIVITY RED (the wake-neuter): drop the destination wake
     * the FIRST admitted FIN's seal push earns. The seal sits ingested-side
     * of the channel with NO pump earned: the system must FREEZE in the
     * lost-doorbell shape — delivery still 4/6, every slot still held
     * (occ=4: the seal notice that would free g0's slot was never pumped),
     * dl_ready unset, refusals unchanged. Then re-ring the dropped doorbell
     * explicitly: recovery must resume and deliver BOTH pending records.
     * This proves the fixture DETECTS a lost wake — a pass here is not
     * vacuous pacing. */
    e.suppress_dest = 1;
    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[0], r.now) ==
                    MOQ_OK);
    ep_run(&r, &e);
    A_CHECK(&r, e.suppressed == 1);   /* the wake WAS produced and dropped */
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 0);    /* frozen at 4/6 */
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 4);         /* no seal notice pumped: no release */
    A_CHECK(&r, !ob.ready && !ob.parked);
    A_CHECK(&r, ob.refusals == postfin_refusals);
    /* Release the lost doorbell: one explicit ring, then earned pacing. */
    e.owed[0] = true;
    ep_run(&r, &e);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 2);    /* both pending records recovered */
    unsigned relmask = 0;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        relmask |= 1u << (sps.obj_byte[o] - 0xD0);
    }
    A_CHECK(&r, relmask == 0x30u);    /* exactly {g4,g5} */
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 4);         /* {g1,g2,g3,g5} still open */
    A_CHECK(&r, ob.refusals == postfin_refusals);

    /* Remaining admitted FINs INCREMENTALLY: each seal frees one slot
     * inside its own earned cascade — pure drain now that both pending
     * records are recovered. */
    static const uint32_t want_occ[3] = { 3, 2, 1 };
    for (int i = 1; i < 4; i++) {
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[i], r.now) ==
                        MOQ_OK);
        ep_run(&r, &e);
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        A_CHECK(&r, sps.objects == 0);   /* nothing pending: drain only */
        ep_observe(&r, &ob);
        if (ob.occ != want_occ[i - 1]) {
            printf("FAIL: paced drain stalled after admitted FIN %d: "
                   "occ=%u (want %u) ready=%d parked=%d refusals=%llu "
                   "dest_passes=%d\n",
                   i, ob.occ, want_occ[i - 1], (int)ob.ready,
                   (int)ob.parked, (unsigned long long)ob.refusals,
                   e.dest_passes);
            r.failures++;
            break;
        }
        A_CHECK(&r, ob.refusals == postfin_refusals);
    }
    /* Full recovery held: the conn stayed LIVE, historical counters
     * persisted (no growth after capacity became available), no park. */
    {
        moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
        moqr_bind_blocked_agg_t agg;
        A_CHECK(&r, moqr_bind_debug_blocked_aggregate(b0, &agg) == MOQR_OK);
        A_CHECK(&r, agg.live_conns == 1);
        A_CHECK(&r, agg.conns_bind_sg == 1);
        A_CHECK(&r, agg.bind_sg_total == postfin_refusals);
        A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));
    }

    /* Recorder-level seal evidence (the SEALLOG's source of truth): after
     * the five FINs so far, the destination shard ingested exactly five
     * seals, contiguous seq 0..4, all from the owner lane, all carrying ONE
     * nonzero demand id (exact attribution — a second demand would be
     * visibly distinct), in the exact FIN order sg4, sg0, sg1, sg2, sg3. */
    uint64_t seal_demand = 0;
    {
        moqr_shards_seal_ev_t evs[32];
        uint64_t total = 0;
        uint32_t n = moqr_shards_debug_seal_log(r.s, 0, evs, 32, &total);
        A_CHECK(&r, total == 5 && n == 5);
        static const uint64_t want_sg[5] = { 4, 0, 1, 2, 3 };
        seal_demand = n > 0 ? evs[0].demand_id : 0;
        A_CHECK(&r, seal_demand != 0);
        for (uint32_t k = 0; k < n && k < 5; k++) {
            A_CHECK(&r, evs[k].seq == k);
            A_CHECK(&r, evs[k].src == 1);
            A_CHECK(&r, evs[k].demand_id == seal_demand);
            A_CHECK(&r, evs[k].group_id == 0);
            A_CHECK(&r, evs[k].subgroup_id == want_sg[k]);
        }
    }

    /* SGDIAG→SEALLOG demand JOIN (the emitter's exact two-step): the bind
     * captured the refused delivery's TRACK at the BIND_SG refusal (NOT the
     * delivery's sub_cookie — that is the downstream subscription handle, a
     * different id space; capturing it was RED-proven to break this very
     * equality), and the shard's demand table resolves track → demand. The
     * result must equal the recorder's seal attribution EXACTLY and
     * unambiguously — the diagnostic identifies the blocked demand, it
     * never asks the reader to already know it. The blocked slot is found
     * the same way the SGDIAG emitter finds it (the one bind_sg_total > 0
     * conn — the aggregate above proved there is exactly one). */
    {
        moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
        uint32_t hits = 0;
        uint64_t got_demand = 0;
        bool got_amb = true;
        for (uint32_t sl = 0; sl < moqr_bind_debug_max_conns(b0); sl++) {
            uint64_t bc[3];
            moqr_bind_debug_conn_blocked_counts(b0, sl, bc);
            if (bc[2] > 0) {
                uint64_t btr = 0, btg = 0;
                hits++;
                if (moqr_bind_debug_conn_bind_sg_track(b0, sl, &btr, &btg,
                                                       &got_amb)) {
                    got_demand = moqr_shards_debug_track_demand(r.s, 0, btr,
                                                                btg);
                }
            }
        }
        A_CHECK(&r, hits == 1);
        A_CHECK(&r, got_demand != 0);             /* the join resolved */
        A_CHECK(&r, got_demand == seal_demand);   /* the exact join */
        A_CHECK(&r, !got_amb);                    /* one demand only */

        /* DUPLICATE-RESOLUTION pin: one demand per {track, gen} is a core
         * invariant, but the resolver must FAIL CLOSED (0) — never silently
         * choose — if it ever breaks. Forge a duplicate via the verify-only
         * invariant-breaker, observe 0, drop it, observe the join again. */
        uint64_t btr = 0, btg = 0;
        A_CHECK(&r, moqr_bind_debug_conn_bind_sg_track(b0, 0, &btr, &btg,
                                                       NULL));
        A_CHECK(&r, moqr_shards_debug_pend_dup(r.s, 0, seal_demand,
                                               seal_demand + 1000));
        A_CHECK(&r, moqr_shards_debug_track_demand(r.s, 0, btr, btg) == 0);
        A_CHECK(&r, moqr_shards_debug_pend_drop(r.s, 0, seal_demand + 1000));
        A_CHECK(&r, moqr_shards_debug_track_demand(r.s, 0, btr, btg) ==
                        seal_demand);
    }

    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[5], r.now) ==
                    MOQ_OK);
    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_recovery_paced\n");
    }
    return r.failures;
}

/* (10) The coalesced-batch shape, deterministic: six DISTINCT groups
 * (0..5, subgroup 0, one object each) freeze at 4/6 under a capacity-4
 * destination pool; then ALL SIX publisher FINs are processed on the
 * source shard while the destination notification coalesces into ONE
 * latched doorbell — the destination is not stepped between individual
 * FINs. The single earned destination step applies every seal and its one
 * bind-pump pass closes every sealed subgroup, but the two remaining
 * records outlive the pass: without a local continuation that is the
 * production stall (dl_ready set, no doorbell owed anywhere — the
 * pre-registered DOORBELL_UNCONSUMED row; the core ready mark was produced
 * AND consumed inside the same step, so "mark missing" is ruled out).
 * Pinned here is the continuation contract that closes it: the step that
 * applied the batch requests EXACTLY ONE coalesced self-wake (a batch is
 * one, never six); the self-earned follow-up delivers 6/6 exactly,
 * applies nothing, and requests nothing — no spin, and source-side credit
 * wakes conjure no destination doorbell. Cadence and the
 * wake_requests_local counter stay exactly reconciled. */
static int
scenario_bindsg_batched_fin_wake(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex3(&r, &a, 4 /* dest pool */, 64 /* pump sg slots */,
                        true /* live_visibility */,
                        64 /* log max_groups */) != MOQR_OK) {
        printf("FAIL: bindsg batched-fin rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);   /* handshakes */
    moq_bytes_t nsp[1] = { B("auB") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Six DISTINCT groups 0..5, subgroup 0, one whole object each, all left
     * open. max_groups=64 keeps group-budget eviction out of the picture
     * (an evicted group would silently free slots and mask the pressure). */
    moq_subgroup_handle_t sg[6];
    for (uint64_t g = 0; g < 6; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg[g]) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg[g], 0, pl,
                                             r.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        ep_run(&r, &e);   /* each write earns its own cascade */
    }

    /* The exact frozen 4/6 hold: pool-many delivered ({g0..g3}), both
     * pending refused at least once, all four slots held, nothing ready or
     * parked. */
    int objs = 0;
    unsigned gmask = 0;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    objs += sps.objects;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        gmask |= 1u << (sps.obj_byte[o] - 0xD0);
    }
    A_CHECK(&r, objs == 4);
    A_CHECK(&r, gmask == 0xFu);
    ep_obs_t ob;
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 4);
    A_CHECK(&r, ob.refusals >= 1);
    A_CHECK(&r, !ob.ready && !ob.parked);

    /* ALL SIX publisher FINs queued, then the source shard processed to
     * quiescence with the destination doorbell COALESCING into one latch —
     * the destination is never stepped in between. */
    for (uint64_t g = 0; g < 6; g++) {
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[g], r.now) ==
                        MOQ_OK);
    }
    ep_run_src(&r, &e);
    A_CHECK(&r, e.owed[0]);   /* exactly one coalesced destination wake */
    {
        moqr_shards_stats_t d1;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 6);
    }
    {
        moqr_shards_seal_ev_t evs[32];
        uint64_t total = 0;
        (void)moqr_shards_debug_seal_log(r.s, 0, evs, 32, &total);
        A_CHECK(&r, total == 0);   /* nothing applied before the dest step */
    }

    int passes0 = e.dest_passes;
    uint64_t wl0;
    {
        /* wake_requests_local is a lifetime counter and the setup's own
         * object ingests legitimately raised continuations — the batch
         * pins its DELTA. */
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        wl0 = d0.wake_requests_local;
    }

    /* The ONE externally-earned destination step. Deterministic effect:
     * the inbound phase applies ALL SIX seals; the bind pump (one pass)
     * closes every sealed downstream subgroup — releasing all four held
     * slots — but ends with the two remaining records undelivered and the
     * bind's own ready latch set. Without the local continuation this is
     * the production stall (retained 4/6 forever: dl_ready=1 persisting
     * with no doorbell owed — the pre-registered DOORBELL_UNCONSUMED row;
     * the CORE ready mark was produced by the applications and CONSUMED by
     * the same step's pump, so "mark missing" is ruled out). The fix makes
     * the step itself request EXACTLY ONE coalesced self-wake for the
     * whole six-message batch. */
    uint64_t mask = 0;
    ep_step_dest(&r, &e, &mask);
    {
        moqr_shards_seal_ev_t evs[32];
        uint64_t total = 0;
        uint32_t n = moqr_shards_debug_seal_log(r.s, 0, evs, 32, &total);
        A_CHECK(&r, total == 6 && n == 6);   /* all six seals applied */
    }
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 0);   /* still 4/6 after the one pass */
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 0);        /* every sealed subgroup closed */
    A_CHECK(&r, ob.ready);           /* work remains, self-signalled */
    A_CHECK(&r, !ob.parked);
    A_CHECK(&r, (mask & 1ull) != 0);   /* ONE local continuation requested */
    A_CHECK(&r, e.owed[0]);
    {
        /* The core ready mark is NOT the residual signal — the pump
         * consumed it in-pass; the bind's dl_ready latch is. An EMPTY
         * ready set plus a set latch plus a requested continuation is the
         * complete post-step signature. The drain doubles as the check:
         * it must return zero cookies (and on a wrong nonzero result the
         * test has already failed — the drained state is moot). */
        uint64_t rdy[8];
        A_CHECK(&r, moqr_core_drain_ready(moqr_shards_core(r.s, 0), rdy,
                                          8) == 0);
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        A_CHECK(&r, d0.wake_requests_local == wl0 + 1);   /* batch => ONE */
    }

    /* The self-earned follow-up: consumes the continuation, delivers the
     * remaining two records exactly, applies NOTHING inbound — so it must
     * request no further self-wake. No spin: after it, no doorbell toward
     * the destination exists anywhere. (Transports are flushed to hand the
     * delivered bytes to the test client; server->client delivery earns no
     * doorbell, so the flush cannot disturb the cadence being pinned.) */
    uint64_t mask2 = 0;
    ep_step_dest(&r, &e, &mask2);
    for (int f = 0; f < 64; f++) {
        if (!ep_flush_transports(&r, &e)) {
            break;
        }
    }
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 2);   /* 6/6 exactly */
    unsigned relmask = 0;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        relmask |= 1u << (sps.obj_byte[o] - 0xD0);
    }
    A_CHECK(&r, relmask == 0x30u);   /* exactly {g4,g5} */
    A_CHECK(&r, (mask2 & 1ull) == 0);   /* empty follow-up: no rearm */
    A_CHECK(&r, !e.owed[0]);
    ep_run_src(&r, &e);                 /* source credits conjure none */
    A_CHECK(&r, !e.owed[0]);
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 0);
    A_CHECK(&r, !ob.ready && !ob.parked);
    {
        uint64_t rdy[8];
        A_CHECK(&r, moqr_core_drain_ready(moqr_shards_core(r.s, 0), rdy,
                                          8) == 0);
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        A_CHECK(&r, d0.wake_requests_local == wl0 + 1);   /* still that one */
        moqr_shards_stats_t d1;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        A_CHECK(&r, d1.wake_requests_local == 0);   /* source applied none */
    }
    A_CHECK(&r, e.dest_passes == passes0 + 2);   /* deterministic cadence:
                                                  * the earned step + its
                                                  * one continuation */
    A_CHECK(&r, moqr_bind_conn_is_open(moqr_shards_bind(r.s, 0),
                                       sub->rsess));

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_batched_fin_wake\n");
    }
    return r.failures;
}

/* (10a) The DRAINED-batch negative pin — the complement of the coalesced-FIN
 * batch above. Same topology, but a pool ROOMY enough (8 >= 6) that all six
 * objects deliver during setup, so the destination never freezes. The six
 * publisher FINs still batch into ONE coalesced destination step, and that step
 * applies all six seals (inbound data applied) — but every subgroup is already
 * drained, so the single bind pass closes them with NOTHING left in the ready
 * set. The step must therefore request NO local self-wake: applying inbound data
 * is not sufficient; only a pass that leaves residual work earns a continuation.
 * Before the residual-work gate this was RED (any applied batch requested one),
 * which is the spurious steady-traffic wake the gate removes. */
static int
scenario_bindsg_drained_batch_no_wake(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex3(&r, &a, 8 /* roomy dest pool: no freeze */,
                        64 /* pump sg slots */, true /* live_visibility */,
                        64 /* log max_groups */) != MOQR_OK) {
        printf("FAIL: bindsg drained-batch rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auB") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Six groups, one whole object each, all DELIVERED into the roomy pool
     * (never frozen — no delivery ever blocks on the subgroup-slot pool, so no
     * bind_sg_waiting latch is ever set). Handles are RETAINED so the FINs can
     * be sent as a coalesced SEAL batch. */
    moq_subgroup_handle_t sg[6];
    for (uint64_t g = 0; g < 6; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg[g]) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg[g], 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        ep_run(&r, &e);
    }
    int objs = 0;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    objs += sps.objects;
    A_CHECK(&r, objs == 6);          /* all six delivered, nothing frozen */
    ep_obs_t ob;
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 6 && !ob.ready && !ob.parked);

    /* All six publisher FINs, coalesced into ONE destination step. */
    for (uint64_t g = 0; g < 6; g++) {
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[g], r.now) ==
                        MOQ_OK);
    }
    ep_run_src(&r, &e);
    A_CHECK(&r, e.owed[0]);
    uint64_t wl0;
    {
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        wl0 = d0.wake_requests_local;
    }

    /* The one earned destination step applies all six SEALs (inbound data
     * applied) and its single bind pass closes every subgroup — but nothing was
     * pool-blocked, so those slot releases re-arm NOTHING and the pass DRAINS.
     * A drained seal-close batch must request NO local self-wake. This is the
     * case ordinary seal-close traffic hits; before the bind_sg_waiting gate the
     * unconditional slot-release re-arm made this RED (one wake per batch). */
    uint64_t mask = 0;
    ep_step_dest(&r, &e, &mask);
    {
        moqr_shards_seal_ev_t evs[32];
        uint64_t total = 0;
        uint32_t n = moqr_shards_debug_seal_log(r.s, 0, evs, 32, &total);
        A_CHECK(&r, total == 6 && n == 6);   /* all six seals applied */
    }
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 0);   /* nothing new: the objects already landed */
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 0);        /* every subgroup closed */
    A_CHECK(&r, !ob.ready && !ob.parked);   /* drained: no residual, no pending */
    A_CHECK(&r, (mask & 1ull) == 0);   /* NO local continuation requested */
    {
        uint64_t rdy[8];
        A_CHECK(&r, moqr_core_drain_ready(moqr_shards_core(r.s, 0), rdy, 8) ==
                        0);
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        A_CHECK(&r, d0.wake_requests_local == wl0);   /* drained batch => none */
    }

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_drained_batch_no_wake\n");
    }
    return r.failures;
}

/* (10c) ACTION_CAP is NOT continuation-worthy. The destination session's action
 * queue is small; delivering objects WITHOUT flushing the transport fills it,
 * so the delivery parks on ACTION_CAP at an observed capacity floor with an
 * empty ready set. The continuation-eligibility query
 * (moqr_bind_pump_continuation_pending) is then pinned DIRECTLY to return false:
 * the stepper ANDs it with inbound_data_applied, so a false there guarantees no
 * applied batch can earn a self-wake while the floor is unchanged. (This is the
 * narrower, deterministic proof — it does not drive a live inbound-while-parked
 * step, which SimPair channel backpressure blocks: a same-track push cannot
 * reach the parked destination.) An ACTION_CAP park recovers only on a
 * downstream transport-capacity edge — the managed adapter arms the lane pump
 * from a transport-event batch, NOT a shard push/credit wake — so a real edge
 * (flushing the transport, then an earned pump) recovers it here. RED: widen
 * eligibility to any parked conn and the direct pin flips true. */
static int
scenario_actioncap_no_wake(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex3(&r, &a, 64 /* ample bind pool: not BIND_SG */,
                        64 /* pump sg slots */, true /* live_visibility */,
                        64 /* log max_groups */) != MOQR_OK) {
        printf("FAIL: actioncap rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    r.conn_max_actions = 6;   /* small dest action queue -> ACTION_CAP */
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    r.conn_max_actions = 0;
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auA") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Publish 8 whole objects on the SOURCE only (coalesced toward the dest). */
    for (uint64_t g = 0; g < 8; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sgh;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sgh) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sgh, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    ep_run_src(&r, &e);
    A_CHECK(&r, e.owed[0]);

    /* Destination steps WITHOUT flushing the transport: the bind's writes fill
     * the small action queue and delivery parks on ACTION_CAP. */
    for (int k = 0; k < 4 && e.owed[0]; k++) {
        uint64_t m = 0;
        ep_step_dest(&r, &e, &m);
    }
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    bool rd = false, pk = false;
    uint8_t reason = 0;
    moqr_bind_debug_dl_state(b0, 0, &rd, &pk, &reason);
    A_CHECK(&r, pk && reason == 1);   /* parked as ACTION_CAP (B_PARK_ACTION_CAP) */
    int objs_frozen = 0;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    objs_frozen = sps.objects;

    /* THE reason-aware pin: parked on ACTION_CAP with an EMPTY ready set at an
     * unchanged capacity floor, a self-continuation is NOT warranted — an
     * ACTION_CAP park recovers only on a downstream transport-capacity edge (the
     * managed adapter arms the lane pump from a transport-event batch, NOT a
     * shard push/credit wake), never on an arbitrary pump. The stepper gates its
     * applied-inbound continuation on this query with an AND, so a false here
     * means NO inbound batch — however unrelated — can earn a spurious self-wake
     * while the floor is unchanged. RED: widen eligibility to any parked conn
     * (dl_parked without the reason check) and this flips true. */
    A_CHECK(&r, !rd);   /* ready set empty at the ACTION_CAP floor */
    A_CHECK(&r, !moqr_bind_pump_continuation_pending(b0));

    /* A REAL capacity edge recovers it. Each round flushes the transport (the
     * action queue drains, so capacity rises above the floor) and earns a pump
     * (a small nudge object rings the doorbell — standing in for the transport-
     * event batch that arms the lane pump in the managed adapter); the recovered
     * pump re-attempts the parked conn and delivers. Bounded loop until the park
     * clears. */
    int objs_after = objs_frozen;
    for (int round = 0; round < 16; round++) {
        for (int f = 0; f < 64; f++) {
            if (!ep_flush_transports(&r, &e)) {
                break;
            }
        }
        moqr_bind_debug_dl_state(b0, 0, &rd, &pk, &reason);
        if (!pk) {
            break;
        }
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 100 + (uint64_t)round;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sgh;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sgh) == MOQ_OK);
        uint8_t body[8];
        memset(body, 0x5A, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sgh, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        ep_run_src(&r, &e);
        ep_run(&r, &e);
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        objs_after += sps.objects;
    }
    A_CHECK(&r, objs_after > objs_frozen);   /* delivery progressed past freeze */
    moqr_bind_debug_dl_state(b0, 0, &rd, &pk, &reason);
    A_CHECK(&r, !pk);   /* the capacity edge cleared the ACTION_CAP park */

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards actioncap_no_wake\n");
    }
    return r.failures;
}

/* (10b) The STREAMED twin of the coalesced-FIN-batch scenario: identical
 * topology, capacities, barrier, FIN batching, and acceptance — the ONE
 * variable is streaming ingest on the publisher's relay session
 * (streaming_objects=true, production's default), so the six records cross
 * the demand channel as OBJ_OPEN/OBJ_CHUNK/OBJ_END and land at the
 * destination as chunked-COMPLETE records instead of whole ones. The
 * whole-object scenario above is the passing control. Beyond the control's
 * pins, this arm RECONCILES the streamed application end to end: every
 * OPEN/END/SEAL message enqueued and the destination holding SIX COMPLETE
 * records (seal presence alone cannot prove a streamed record completed —
 * a record stuck OPEN makes its seal notice permanently ineligible, which
 * is indistinguishable from a wake bug unless completion is pinned). */
static int
scenario_bindsg_batched_fin_wake_streamed(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex3(&r, &a, 4 /* dest pool */, 64 /* pump sg slots */,
                        true /* live_visibility */,
                        64 /* log max_groups */) != MOQR_OK) {
        printf("FAIL: bindsg streamed rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    r.streaming = true;    /* the publisher's relay session ingests chunked */
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    r.streaming = false;   /* the subscriber consumes classic whole objects */
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);   /* handshakes */
    moq_bytes_t nsp[1] = { B("auS") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Six DISTINCT groups 0..5, subgroup 0, one whole object each, all left
     * open — identical to the control; the relay session streams them. */
    moq_subgroup_handle_t sg[6];
    for (uint64_t g = 0; g < 6; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg[g]) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg[g], 0, pl,
                                             r.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        ep_run(&r, &e);
    }

    /* The exact frozen 4/6 hold, as the control pins it. */
    int objs = 0;
    unsigned gmask = 0;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    objs += sps.objects;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        gmask |= 1u << (sps.obj_byte[o] - 0xD0);
    }
    A_CHECK(&r, objs == 4);
    A_CHECK(&r, gmask == 0xFu);
    ep_obs_t ob;
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 4);
    A_CHECK(&r, ob.refusals >= 1);
    A_CHECK(&r, !ob.ready && !ob.parked);

    /* STREAMED-APPLICATION RECONCILIATION, pre-FIN: the six records crossed
     * as OPEN/.../END (not whole OBJ) and the destination holds SIX
     * COMPLETE records — completion is pinned BEFORE the seals so a
     * stuck-OPEN record cannot masquerade as a recovery/wake failure. */
    {
        moqr_shards_stats_t d1;
        moqr_core_stats_t c0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        moqr_core_get_stats(moqr_shards_core(r.s, 0), &c0);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ] == 0);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] == 6);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ_END] == 6);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK] >= 6);
        A_CHECK(&r, c0.ingested_total == 6);   /* six COMPLETE at the dest */
    }

    /* ALL SIX FINs coalesced into one destination doorbell, source
     * processed to quiescence — identical to the control. */
    for (uint64_t g = 0; g < 6; g++) {
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[g], r.now) ==
                        MOQ_OK);
    }
    ep_run_src(&r, &e);
    A_CHECK(&r, e.owed[0]);
    {
        moqr_shards_stats_t d1;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 6);
    }

    /* Earned pumps only: the doorbell chain must recover 6/6 exactly. */
    int total = 0;
    int steps = 0;
    while (e.owed[0] && steps < 8) {
        uint64_t m = 0;
        ep_step_dest(&r, &e, &m);
        steps++;
        for (int f = 0; f < 64; f++) {
            if (!ep_flush_transports(&r, &e)) {
                break;
            }
        }
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        total += sps.objects;
        for (int o = 0; o < sps.objects && o < 8; o++) {
            gmask |= 1u << (sps.obj_byte[o] - 0xD0);
        }
    }
    /* The behavioral post-recovery pins below (exact 6/6, every subgroup
     * closed, all six seals applied and the join stable) ARE the contract;
     * the position-level introspection that first named the defect was
     * deliberately removed from the production core once the diagnosis
     * landed. */
    A_CHECK(&r, total == 2);        /* 6/6 exactly: the last two groups */
    A_CHECK(&r, gmask == 0x3Fu);
    A_CHECK(&r, !e.owed[0]);        /* no spin */
    ep_run_src(&r, &e);
    A_CHECK(&r, !e.owed[0]);
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 0);       /* every sealed subgroup closed */
    A_CHECK(&r, !ob.parked);
    uint64_t seal_demand = 0;
    {
        moqr_shards_seal_ev_t evs[32];
        uint64_t seal_total = 0;
        uint32_t n = moqr_shards_debug_seal_log(r.s, 0, evs, 32,
                                                &seal_total);
        A_CHECK(&r, seal_total == 6 && n == 6);   /* all six seals applied */
        seal_demand = n > 0 ? evs[0].demand_id : 0;
        A_CHECK(&r, seal_demand != 0);
    }
    /* The SGDIAG demand join must hold under STREAMED refusals exactly as
     * it does for whole objects: the refusal-latched track resolves to the
     * recorder's seal demand, unambiguously. */
    {
        uint64_t btr = 0, btg = 0;
        bool amb = true;
        A_CHECK(&r, moqr_bind_debug_conn_bind_sg_track(
                        moqr_shards_bind(r.s, 0), 0, &btr, &btg, &amb));
        A_CHECK(&r, !amb);
        A_CHECK(&r, moqr_shards_debug_track_demand(r.s, 0, btr, btg) ==
                        seal_demand);
    }
    A_CHECK(&r, moqr_bind_conn_is_open(moqr_shards_bind(r.s, 0),
                                       sub->rsess));

    /* Demand-retirement zeroes the join FAIL-CLOSED — the transport
     * campaign's post-drain "zero demand" observed exactly this: the
     * blocked SUBSCRIBER's connection idle-closed during the long drain,
     * its departure undemanded the track and retired the pending entry —
     * while the detached slot's latched counters persist until reuse. The
     * still-latched (stable, unambiguous) track then legitimately resolves
     * to NO demand: zero attribution after retirement is the contract (the
     * join is exact or absent, never a guess); a live-window snapshot is
     * where attribution must be read. Under the failover contract a
     * PUBLISHER departure with no alternate announce TERMINATES the
     * standing subscriber explicitly, which retires the demand at the pub
     * death itself — the subscriber's own close afterwards is moot for
     * demand. */
    A_CHECK(&r, moq_session_close(pub->peer, 0x0, "publisher leaves",
                                  r.now) == MOQ_OK);
    ep_run(&r, &e);
    A_CHECK(&r, moq_session_close(sub->peer, 0x0, "subscriber leaves",
                                  r.now) == MOQ_OK);
    ep_run(&r, &e);
    /* The undemand is LINGER-gated (the track keeps its upstream briefly
     * for a rejoin) and its expiry is time-driven — production's idle
     * sweeps earn that pump; this fixture has none by design, so ring the
     * lanes explicitly once past the linger to model the sweep. This pin
     * is about the RESOLVER contract after retirement, not the wake
     * discipline (which the scenarios above own). */
    r.now += 1000000;
    e.owed[0] = true;
    e.owed[1] = true;
    ep_run(&r, &e);
    {
        uint64_t btr = 0, btg = 0;
        bool amb = true;
        A_CHECK(&r, moqr_bind_debug_conn_bind_sg_track(
                        moqr_shards_bind(r.s, 0), 0, &btr, &btg, &amb));
        A_CHECK(&r, !amb);   /* the latch persists on the detached slot */
        A_CHECK(&r, moqr_shards_debug_track_demand(r.s, 0, btr, btg) == 0);
    }

    /* Both sessions closed above; teardown only. */
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_batched_fin_wake_streamed\n");
    }
    return r.failures;
}

/* (10c) The SESSION_SG streamed twin: identical to (10b) except the
 * capacity under test moves from the BIND pool to the destination SESSION's
 * own subgroup pool (bind pool safely above the workload; session pool 4).
 * The refusal happens INSIDE moq_session_open_subgroup with positive action
 * capacity, so the bind classifies SESSION_SG and PARKS the connection with
 * one bounded re-attempt per pump. The recovery contract: the FIN batch's
 * seals close the four held downstream subgroups, freeing the session pool,
 * and the parked re-attempt then delivers 6/6 exactly and clears the park —
 * across the earned-pump chain only, with no idle spin. */
static int
scenario_sessionsg_batched_fin_wake_streamed(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex3(&r, &a, 64 /* bind pool: safely above 6 */,
                        64 /* pump sg slots */, true /* live_visibility */,
                        64 /* log max_groups */) != MOQR_OK) {
        printf("FAIL: sessionsg streamed rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    r.streaming = true;    /* the publisher's relay session ingests chunked */
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    r.streaming = false;
    r.conn_max_open_subgroups = 4;   /* the SESSION pool under test */
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    r.conn_max_open_subgroups = 0;
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);   /* handshakes */
    moq_bytes_t nsp[1] = { B("auT") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    moq_subgroup_handle_t sg[6];
    for (uint64_t g = 0; g < 6; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg[g]) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg[g], 0, pl,
                                             r.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        ep_run(&r, &e);
    }

    /* Frozen 4/6 with the SESSION_SG signature: parked with reason
     * SESSION_SG (positive action capacity at refusal is what routed the
     * classification there), exclusively — no ACTION_CAP or BIND_SG
     * counts anywhere on the conn. */
    int objs = 0;
    unsigned gmask = 0;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    objs += sps.objects;
    for (int o = 0; o < sps.objects && o < 8; o++) {
        gmask |= 1u << (sps.obj_byte[o] - 0xD0);
    }
    A_CHECK(&r, objs == 4);
    A_CHECK(&r, gmask == 0xFu);
    {
        moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
        bool rd = false, pk = false;
        uint8_t reason = 0;
        moqr_bind_debug_dl_state(b0, 0, &rd, &pk, &reason);
        A_CHECK(&r, pk);
        A_CHECK(&r, reason == 2);   /* parked as SESSION_SG */
        uint64_t bc[3];
        moqr_bind_debug_conn_blocked_counts(b0, 0, bc);
        A_CHECK(&r, bc[1] >= 1);            /* SESSION_SG counted   */
        A_CHECK(&r, bc[0] == 0 && bc[2] == 0);   /* exclusively */
    }
    /* Streamed-application reconciliation: six COMPLETE at the dest. */
    {
        moqr_shards_stats_t d1;
        moqr_core_stats_t c0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        moqr_core_get_stats(moqr_shards_core(r.s, 0), &c0);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ] == 0);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] == 6);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ_END] == 6);
        A_CHECK(&r, c0.ingested_total == 6);
    }

    /* ALL SIX FINs coalesced into one destination doorbell. */
    for (uint64_t g = 0; g < 6; g++) {
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg[g], r.now) ==
                        MOQ_OK);
    }
    ep_run_src(&r, &e);
    A_CHECK(&r, e.owed[0]);
    {
        moqr_shards_stats_t d1;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 6);
    }

    /* Earned pumps only: the doorbell chain must recover 6/6 exactly,
     * with exactly ONE SESSION_SG re-attempt per parked pump. */
    uint64_t att0 = moqr_bind_debug_sg_attempts();
    int total = 0;
    int steps = 0;
    while (e.owed[0] && steps < 8) {
        uint64_t m = 0;
        ep_step_dest(&r, &e, &m);
        steps++;
        for (int f = 0; f < 64; f++) {
            if (!ep_flush_transports(&r, &e)) {
                break;
            }
        }
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        total += sps.objects;
        for (int o = 0; o < sps.objects && o < 8; o++) {
            gmask |= 1u << (sps.obj_byte[o] - 0xD0);
        }
    }
    /* Exactly ONE bounded SESSION_SG re-attempt per parked pump before the
     * release — the attempt that finds the freed pool IS the delivery. */
    A_CHECK(&r, moqr_bind_debug_sg_attempts() - att0 == (uint64_t)steps);
    {
        moqr_shards_seal_ev_t evs[32];
        uint64_t st = 0;
        uint32_t n = moqr_shards_debug_seal_log(r.s, 0, evs, 32, &st);
        A_CHECK(&r, st == 6 && n == 6);   /* all six seals applied */
    }
    A_CHECK(&r, total == 2);        /* 6/6 exactly */
    A_CHECK(&r, gmask == 0x3Fu);
    A_CHECK(&r, !e.owed[0]);        /* no spin */
    ep_run_src(&r, &e);
    A_CHECK(&r, !e.owed[0]);
    {
        moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
        bool rd = false, pk = false;
        uint8_t reason = 0;
        moqr_bind_debug_dl_state(b0, 0, &rd, &pk, &reason);
        A_CHECK(&r, !pk);           /* the SESSION_SG park CLEARED */
        uint64_t bc[3];
        moqr_bind_debug_conn_blocked_counts(b0, 0, bc);
        A_CHECK(&r, bc[0] == 0 && bc[2] == 0);   /* still exclusive */
    }
    ep_obs_t ob;
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 0);
    A_CHECK(&r, moqr_bind_conn_is_open(moqr_shards_bind(r.s, 0),
                                       sub->rsess));

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards sessionsg_batched_fin_wake_streamed\n");
    }
    return r.failures;
}

/* (11) The continuation CHAIN: a batch too large for two bind-pump passes.
 * 300 groups (one whole object + FIN each) cross while the destination is
 * never stepped — 600 deliverables land in ONE inbound batch, but a pump
 * pass is bounded by the 256-delivery guard, so completion needs THREE
 * passes. The batch-application trigger earns only the FIRST continuation;
 * a follow-up pass that ends ON THE BUDGET GUARD applies no inbound data,
 * so without a budget-outcome trigger it leaves dl_ready set with no
 * doorbell owed — the same lost-continuation class, one hop later. Pinned:
 * every budget-bounded pass requests exactly one further self-wake, the
 * chain is FINITE (exactly as many continuations as extra passes), the
 * final drained pass requests nothing (zero idle spin), and delivery
 * completes 300/300 exactly. */
enum { CHAIN_GROUPS = 300 };

static int
scenario_bindsg_budget_chain(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex4(&r, &a, 8 /* dest pool */, 64 /* pump sg slots */,
                        true /* live_visibility */, 512 /* log max_groups */,
                        2048 /* demand channel entries */) != MOQR_OK) {
        printf("FAIL: bindsg budget-chain rig create\n");
        return 1;
    }
    r.conn_max_actions = 4096;
    r.conn_max_events = 4096;
    r.conn_max_open_subgroups = 4096;
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);   /* handshakes */
    moq_bytes_t nsp[1] = { B("auC") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    int passes0 = e.dest_passes;
    uint64_t wl0;
    {
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        wl0 = d0.wake_requests_local;
    }

    /* 300 groups, each one whole object + FIN, processed on the SOURCE
     * only: every message is in the channel before the destination's one
     * earned step, and the destination doorbell coalesces into one latch. */
    for (uint64_t g = 0; g < CHAIN_GROUPS; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        uint8_t body[8];
        memset(body, (int)(g & 0xFF), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run_src(&r, &e);   /* source-only: owed[0] only ever LATCHES */
    }
    ep_run_src(&r, &e);
    A_CHECK(&r, e.owed[0]);   /* the one coalesced destination wake */
    {
        moqr_shards_stats_t d1;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &d1) == MOQR_OK);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_OBJ] == CHAIN_GROUPS);
        A_CHECK(&r, d1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == CHAIN_GROUPS);
    }

    /* The earned step + the continuation chain: each pass that ends on the
     * 256-delivery budget guard must request one further self-wake; the
     * final pass drains and requests nothing. 600 deliverables / 256 =
     * three passes = two continuations. Drive ONLY doorbells the steps
     * themselves earn. */
    int total = 0;
    int steps = 0;
    while (e.owed[0] && steps < 8) {
        uint64_t m = 0;
        ep_step_dest(&r, &e, &m);
        steps++;
        for (int f = 0; f < 64; f++) {
            if (!ep_flush_transports(&r, &e)) {
                break;
            }
        }
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        total += sps.objects;
    }
    A_CHECK(&r, total == CHAIN_GROUPS);   /* exact completion, no externals */
    A_CHECK(&r, steps == 3);              /* finite: 3 passes, 2 wakes */
    A_CHECK(&r, !e.owed[0]);              /* zero idle spin */
    ep_run_src(&r, &e);                   /* source credits conjure none */
    A_CHECK(&r, !e.owed[0]);
    A_CHECK(&r, e.dest_passes == passes0 + 3);
    {
        moqr_shards_stats_t d0;
        A_CHECK(&r, moqr_shards_get_stats(r.s, 0, &d0) == MOQR_OK);
        /* one application-earned wake + one budget-earned wake — the
         * drained final pass adds none */
        A_CHECK(&r, d0.wake_requests_local == wl0 + 2);
    }
    ep_obs_t ob;
    ep_observe(&r, &ob);
    A_CHECK(&r, ob.occ == 0);
    A_CHECK(&r, !ob.parked);
    /* dl_ready may hold a STALE hint here — the final pass's own
     * slot-release re-arms land after its selection loop, and a ready mark
     * is a hint, not a work claim. It is provably stale: every object is
     * delivered (total above), the core ready set is empty, and no doorbell
     * is owed — the next EARNED pump clears it for free. */
    {
        uint64_t rdy[8];
        A_CHECK(&r, moqr_core_drain_ready(moqr_shards_core(r.s, 0), rdy,
                                          8) == 0);
    }
    A_CHECK(&r, moqr_bind_conn_is_open(moqr_shards_bind(r.s, 0),
                                       sub->rsess));

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    ep_observe(&r, &ob);
    A_CHECK(&r, !ob.ready && !ob.parked);   /* the earned pump cleared it */
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards bindsg_budget_chain\n");
    }
    return r.failures;
}

/* SEALLOG interval completeness through the REAL recorder: drive the actual
 * 32-entry seal ring through wrap and prove the acceptance boundary on ring
 * output, not on fabricated rows. 40 background seals establish a baseline
 * cursor whose early history has ALREADY wrapped out; +6 target seals must
 * ACCEPT against that cursor (interval-complete despite lifetime wrap); +33
 * further seals must REJECT (the cursor itself overwritten); a fresh
 * baseline then re-establishes a usable cursor. Rows and header are built
 * from the accessor exactly as the emitter builds them. */
static void
seal_ring_snapshot(arig_t *r, moqr_cli_seallog_meta_t *meta,
                   moqr_cli_seallog_row_t *rows, size_t *count)
{
    moqr_shards_seal_ev_t evs[32];
    uint64_t total = 0;
    uint32_t n = moqr_shards_debug_seal_log(r->s, 0, evs, 32, &total);
    meta->epoch = 7;
    meta->lane = 0;
    meta->first_retained_seq = total - n;
    meta->lifetime_total = total;
    meta->retained_count = n;
    for (uint32_t k = 0; k < n; k++) {
        memset(&rows[k], 0, sizeof(rows[k]));
        rows[k].epoch = 7;
        rows[k].lane = 0;
        rows[k].seq = evs[k].seq;
        rows[k].total = total;
        rows[k].src = evs[k].src;
        rows[k].demand = evs[k].demand_id;
        rows[k].group_id = evs[k].group_id;
        rows[k].subgroup_id = evs[k].subgroup_id;
    }
    *count = n;
}

static void
seal_burst(arig_t *r, aconn_t *pub, moq_subscription_t up_sub, uint64_t sg_lo,
           uint64_t sg_hi, ca_t *a)
{
    /* One record per GROUP (sg 0, obj 0): the per-group subgroup-record cap
     * (default 16) would silently starve a one-group burst at 16 seals;
     * group churn only EVICTS old sealed groups, which the ring — a record
     * of INGESTS — is immune to. */
    for (uint64_t g = sg_lo; g < sg_hi; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t h;
        A_CHECK(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                             &h) == MOQ_OK);
        uint8_t body[16];
        memset(body, (int)(g & 0xFF), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(r, moq_rcbuf_create(&a->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        A_CHECK(r, moq_session_write_object(pub->peer, h, 0, pl, r->now) ==
                       MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(r, moq_session_close_subgroup(pub->peer, h, r->now) ==
                       MOQ_OK);
        arig_pump(r, 3);
    }
}

static int
scenario_seal_interval_wrap(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create(&r, &a) != MOQR_OK) {
        printf("FAIL: seal interval rig create\n");
        return 1;
    }
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    arig_pump(&r, 4);
    moq_bytes_t nsp[1] = { B("auW") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    arig_pump(&r, 6);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    arig_pump(&r, 3);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    arig_pump(&r, 4);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    const char *why = NULL;
    moqr_cli_seallog_meta_t meta;
    moqr_cli_seallog_row_t rows[32];
    size_t n = 0;

    /* 40 background seals: the ring (32) has ALREADY wrapped at baseline. */
    seal_burst(&r, pub, pps.up_sub, 0, 40, &a);
    seal_ring_snapshot(&r, &meta, rows, &n);
    A_CHECK(&r, meta.lifetime_total == 40 && meta.retained_count == 32 &&
                    meta.first_retained_seq == 8);
    uint64_t cursor = 0;
    A_CHECK(&r, moqr_cli_seallog_accept_interval(&meta, rows, n, 7, 0, 0,
                                                 true, &cursor, &why));
    A_CHECK(&r, cursor == 40);

    /* +6 target seals: interval [40,46) ACCEPTS across the lifetime wrap. */
    seal_burst(&r, pub, pps.up_sub, 40, 46, &a);
    seal_ring_snapshot(&r, &meta, rows, &n);
    A_CHECK(&r, meta.lifetime_total == 46 && meta.first_retained_seq == 14);
    uint64_t cursor2 = 0;
    A_CHECK(&r, moqr_cli_seallog_accept_interval(&meta, rows, n, 7, 0,
                                                 cursor, false, &cursor2,
                                                 &why));
    A_CHECK(&r, cursor2 == 46);

    /* +33 seals: the cursor (46) is OVERWRITTEN (first becomes 47) — the
     * REAL ring now fails the interval closed, exactly as the pure RED
     * pinned. */
    seal_burst(&r, pub, pps.up_sub, 46, 79, &a);
    seal_ring_snapshot(&r, &meta, rows, &n);
    A_CHECK(&r, meta.lifetime_total == 79 && meta.first_retained_seq == 47);
    A_CHECK(&r, !moqr_cli_seallog_accept_interval(&meta, rows, n, 7, 0,
                                                  cursor2, false, NULL,
                                                  &why));

    /* Recovery: a FRESH baseline re-establishes a cursor at 79. */
    uint64_t cursor3 = 0;
    A_CHECK(&r, moqr_cli_seallog_accept_interval(&meta, rows, n, 7, 0, 0,
                                                 true, &cursor3, &why));
    A_CHECK(&r, cursor3 == 79);

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    arig_pump(&r, 8);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards seal_interval_wrap\n");
    }
    return r.failures;
}

/* Eviction overtaking a lagging destination must surface as a GAP, never a
 * silent forever-wait. The owner journal keeps 2 groups and the demand
 * channel 2 entries; the destination is not pumped while the publisher runs
 * 8 groups ahead, so the owner's eviction watermark passes groups it never
 * managed to forward. When the destination finally drains, its local
 * subscriber must receive every SURVIVING object and reach quiescence: the
 * evicted range is a gap it skips, not a position it parks on. The core
 * cannot infer this from its own log — a never-received group reads as
 * "not yet arrived" — so the owner's watermark, which the destination
 * demonstrably receives, is the only carrier of "never will arrive".
 *
 * Fixture-sensitivity guards: the scenario asserts the overtake REALLY
 * happened (owner emitted GRP_EVICT and the destination ingested fewer
 * objects than were published), so a mistuned budget or throttle cannot
 * let the delivery assertions pass vacuously. */
static int
scenario_evict_overtake_gap(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex4(&r, &a, 0 /* default dest pool */,
                        8 /* progress slots */,
                        true /* live_visibility: per-shard stepping */,
                        2 /* owner journal: 2 groups */,
                        1 /* demand channel: 1 entry */) != MOQR_OK) {
        printf("FAIL: evict overtake rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);   /* handshakes */
    moq_bytes_t nsp[1] = { B("auE") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Eight groups, one whole FINISHED object each (payload byte 0xD0+g),
     * published while ONLY the owner shard is pumped: the channel admits
     * two entries and then the owner's forwarding stalls, the journal
     * budget evicts everything older than the last two groups, and the
     * watermark walks straight over the unforwarded middle. */
    for (uint64_t g = 0; g < 24; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run_src(&r, &e);   /* owner only: the destination stays parked */
    }

    /* Release the destination: drain to a full freeze. */
    ep_run(&r, &e);

    /* Overtake proof (fixture sensitivity): the owner really evicted — the
     * watermark notice needs a channel slot, so it can only be OBSERVED
     * enqueued after the drain — and the destination is really short.
     * Without both, the delivery assertions below would be vacuous. */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] >= 1);

    moqr_core_stats_t c0;
    moqr_core_get_stats(moqr_shards_core(r.s, 0), &c0);
    A_CHECK(&r, c0.ingested_total < 24);   /* the hole is real */
    A_CHECK(&r, c0.ingested_total >= 2);  /* survivors did arrive */

    /* THE invariant: the subscriber received every SURVIVING object — in
     * particular the newest groups, which sit BEYOND the evicted hole — and
     * the system froze in quiescence rather than parking the subscriber on
     * a group that will never arrive. The newest group's payload is the
     * unforgeable witness: a subscriber stuck at the hole can never emit
     * 0xD7. */
    int objs = 0;
    uint8_t last = 0;
    for (int rounds = 0; rounds < 8; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        if (sps.objects > 0) {
            objs += sps.objects;
            last = sps.last_byte;
        }
        ep_run(&r, &e);
    }
    A_CHECK(&r, last == 0xD0 + 23);
    A_CHECK(&r, objs >= 3);   /* the two channel survivors + the tail */
    A_CHECK(&r, (uint64_t)objs == c0.ingested_total);   /* everything the
                                     * destination HAS reached the app —
                                     * no retained-but-undelivered waiter */

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards evict_overtake_gap\n");
    }
    return r.failures;
}


/* The g64 defect, end to end and deterministic: eviction overtakes the
 * owner's forwarding to a lagging destination, the destination keeps
 * everything it received (it stays BELOW its own journal budget, so its own
 * eviction machinery — which would jump its subscriber past the missing
 * range and mask the defect — never runs), the track ends with a forwarded
 * END_OF_TRACK, and the destination's subscriber must then observe an
 * EXPLICIT terminal: local SUBSCRIBE_DONE (wire PUBLISH_DONE in draft-18
 * terms) with fewer objects than the track carried — never a silent
 * forever-wait. The relay fabricates nothing to achieve this: no payloads,
 * no publisher-originated Prior Gap properties (draft-18 §12.8/12.9) — the
 * owner's eviction watermark, which the destination demonstrably receives,
 * plus the forwarded END_OF_TRACK are the complete justification for the
 * terminal.
 *
 * Fixture guards prove the masking condition is absent and the wait is not
 * some other stall: the owner really evicted (GRP_EVICT crossed), the
 * destination is really short (loss > 0), the destination delivered every
 * object it retained INCLUDING group 0 — deliverable only if the
 * destination never self-evicted, since its own eviction jump would have
 * skipped it — and quiescence was reached (no wake/capacity/session stall:
 * the drain loop ran the system to a freeze with everything available
 * consumed). */
static int
scenario_evict_overtake_silent_wait(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    /* Owner eviction is BYTE-driven so the asymmetry is structural: the
     * owner sees every published byte (24 groups: one 16-byte group, then
     * 1 KiB each) and holds only the newest ~4 KiB; the destination sees
     * only the tiny first group, the surviving tail, and the terminal —
     * comfortably under the same budget, so it can never evict. */
    if (arig_create_ex5(&r, &a, 0, 8, true /* per-shard stepping */,
                        64 /* group budget: never binds */,
                        1 /* channel: 1 entry so forwarding lags */,
                        4700 /* byte budget: ~4 surviving groups */) !=
        MOQR_OK) {
        printf("FAIL: overtake silent-wait rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auW") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* 24 finished whole-object groups, owner-only pumping throughout. */
    static uint8_t body[1024];
    for (uint64_t g = 0; g < 24; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        size_t len = g == 0 ? 16 : sizeof(body);
        memset(body, (int)(0xD0 + g), len);
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, len, &pl) == MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run_src(&r, &e);
    }
    /* The reliable stream terminal: END_OF_TRACK on a fresh subgroup of the
     * final group. */
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 23;
        sgc.subgroup_id = 1;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        A_CHECK(&r, moq_session_write_status_object(
                        pub->peer, sg, 1, MOQ_OBJECT_END_OF_TRACK, r.now) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run_src(&r, &e);
    }

    /* Release the destination and drain the whole system to a freeze. */
    ep_run(&r, &e);
    int objs = 0;
    bool done = false;
    uint64_t done_code = 0;
    uint8_t first = 0, last = 0;
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        if (sps.objects > 0) {
            if (objs == 0) {
                first = sps.obj_byte[0];
            }
            objs += sps.objects;
            last = sps.last_byte;
        }
        if (sps.done_seen) {
            done = true;
            done_code = sps.done_code;
        }
        ep_run(&r, &e);
    }

    /* Masking-absence guards (each failure means the fixture, not the
     * relay, is wrong — see the header comment). */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] >= 1);
    moqr_core_stats_t c0;
    moqr_core_get_stats(moqr_shards_core(r.s, 0), &c0);
    A_CHECK(&r, c0.ingested_total < 25);   /* loss is real (24 objs + EOT) */
    A_CHECK(&r, c0.ingested_total >= 4);   /* survivors + terminal arrived */
    A_CHECK(&r, first == 0xD0);   /* group 0 delivered: the destination never
                                   * self-evicted, or its jump would have
                                   * skipped it */
    A_CHECK(&r, last == 0xD0 + 23);   /* the surviving tail fully delivered:
                                       * no wake/capacity/session stall */
    A_CHECK(&r, objs < 24);           /* the shortfall is subscriber-visible */

    /* THE invariant (the g64 fix contract): the subscriber must observe an
     * explicit terminal for a finished track it can never fully receive —
     * local SUBSCRIBE_DONE (wire PUBLISH_DONE) — instead of waiting
     * forever. */
    A_CHECK(&r, done);
    (void)done_code;
    /* The DONE retired the subscription; there is nothing to unsubscribe. */
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards evict_overtake_silent_wait\n");
    }
    return r.failures;
}


/* The retirement seam alone, no eviction anywhere: an unbounded subscriber
 * that received EVERY object of a finished track (forwarded END_OF_TRACK
 * included) must observe local SUBSCRIBE_DONE (wire PUBLISH_DONE) — full
 * delivery of a finished track is completion, not a state to idle in
 * forever. Pins the seam independently of loss. */
static int
scenario_eot_complete_retires(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex4(&r, &a, 0, 8, true, 64, 0) != MOQR_OK) {
        printf("FAIL: eot complete rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auC") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    for (uint64_t g = 0; g < 4; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xA0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run(&r, &e);
    }
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 3;
        sgc.subgroup_id = 1;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        A_CHECK(&r, moq_session_write_status_object(
                        pub->peer, sg, 1, MOQ_OBJECT_END_OF_TRACK, r.now) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
    }
    int objs = 0;
    bool done = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        ep_run(&r, &e);
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        objs += sps.objects;
        if (sps.done_seen) {
            done = true;
        }
    }
    /* 4 payload objects; the zero-length END_OF_TRACK status object may
     * surface as a fifth object event — both shapes are full delivery. */
    A_CHECK(&r, objs == 4 || objs == 5);
    A_CHECK(&r, done);        /* full delivery of a finished track completes */

    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards eot_complete_retires\n");
    }
    return r.failures;
}

/* The retirement seam must FAIL CLOSED: no terminal may be synthesized
 * without EOT, and none under EOT while an UNKNOWN gap remains (draft-18
 * §2.1: unknown is neither known-existent nor known-nonexistent — only
 * authoritative closure counts; an unsealed subgroup under EOT is exactly
 * such an unknown). Two arms:
 *   A: finished-looking but NO EOT -> no SUBSCRIBE_DONE ever;
 *   B: EOT present but a subgroup left UNSEALED (its object range unknown)
 *      -> no SUBSCRIBE_DONE either. */
static int
scenario_eot_negative_no_synthesis(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex4(&r, &a, 0, 8, true, 64, 0) != MOQR_OK) {
        printf("FAIL: eot negative rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auN") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Arm A state: two delivered, FIN'd groups and NO terminal. */
    for (uint64_t g = 0; g < 2; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        uint8_t body[32];
        memset(body, (int)(0xB0 + g), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run(&r, &e);
    }
    bool done = false;
    for (int rounds = 0; rounds < 6; rounds++) {
        ep_run(&r, &e);
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        if (sps.done_seen) {
            done = true;
        }
    }
    A_CHECK(&r, !done);   /* arm A: no EOT, no synthesized terminal */

    /* Arm B state: group 2 subgroup 0 LEFT OPEN (unsealed: its object range
     * is unknown under §2.1), then EOT on a sibling subgroup. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 2;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg_open;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg_open) == MOQ_OK);
    uint8_t body[32];
    memset(body, 0xB2, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sg_open, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    /* deliberately NOT closed */
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 2;
    sgc.subgroup_id = 1;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg_eot;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg_eot) == MOQ_OK);
    A_CHECK(&r, moq_session_write_status_object(
                    pub->peer, sg_eot, 1, MOQ_OBJECT_END_OF_TRACK, r.now) ==
                    MOQ_OK);
    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg_eot, r.now) ==
                    MOQ_OK);
    done = false;
    for (int rounds = 0; rounds < 6; rounds++) {
        ep_run(&r, &e);
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        if (sps.done_seen) {
            done = true;
        }
    }
    A_CHECK(&r, !done);   /* arm B: EOT + unknown gap: fail closed */

    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg_open, r.now) ==
                    MOQ_OK);
    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards eot_negative_no_synthesis\n");
    }
    return r.failures;
}


/* The g64/frozen-blaster arm: overtake loss with NO track terminal anywhere.
 * The publisher FINs a subgroup only after the owner already evicted its
 * group, so the seal is dropped (moqr_core_seal_subgroup returns MOQR_DONE
 * for a gone group) and the destination's downstream subgroup — already
 * opened to the client by a delivered object — dangles OPEN forever with no
 * close, no reset, no progress signal. Post-fix contract (0057): applying
 * the owner watermark at the destination lets the EXISTING local
 * eviction/notice machinery close the dangling below-watermark subgroup
 * (gap/resync), while the subscription itself stays LIVE — no EOT means no
 * terminal may be synthesized. */
static int
scenario_evict_overtake_dangling_subgroup(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex5(&r, &a, 0, 8, true, 64 /* group budget: unbound */,
                        2 /* channel entries */,
                        4700 /* byte budget: ~4 surviving groups */) !=
        MOQR_OK) {
        printf("FAIL: dangling subgroup rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auD") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    static uint8_t body[1024];
    /* Two normal FIN'd groups, fully pumped. */
    for (uint64_t g = 0; g < 2; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.subgroup_id = 0;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                              r.now, &sg) == MOQ_OK);
        memset(body, (int)(0xD0 + g), 16);
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
        ep_run(&r, &e);
    }
    /* Group 2: one delivered object, subgroup left OPEN — the client-side
     * subgroup stream is now open and waiting for more. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 2;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg2;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg2) == MOQ_OK);
    memset(body, 0xD2, 16);
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sg2, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 3);   /* g0..g2 object 0 all delivered */
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 1);   /* g2 open */

    /* Destination parked; five more FIN'd groups in one burst: the byte
     * budget evicts groups 0..2 during ingest (before any delivery scan can
     * hold a record of a group about to die), so group 2 dies with its
     * subgroup unsealed. */
    for (uint64_t g = 3; g < 8; g++) {
        moq_subgroup_cfg_t sgc2;
        moq_subgroup_cfg_init(&sgc2);
        sgc2.group_id = g;
        sgc2.subgroup_id = 0;
        sgc2.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc2,
                                              r.now, &sg) == MOQ_OK);
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl2 = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl2) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl2, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl2);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
    }
    ep_run_src(&r, &e);
    /* The LATE FIN: the owner already evicted group 2, so this seal is
     * dropped at ingest — the destination will never learn the subgroup
     * ended. */
    A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg2, r.now) == MOQ_OK);
    ep_run_src(&r, &e);

    /* Release and drain to quiescence. */
    ep_run(&r, &e);
    int objs = 0;
    bool done = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        objs += sps.objects;
        if (sps.done_seen) {
            done = true;
        }
        ep_run(&r, &e);
    }

    /* Guards: the overtake is real and nothing else stalled. */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] >= 1);
    A_CHECK(&r, objs >= 3);   /* the surviving tail reached the client */

    /* No terminal without a track terminal fact — the subscription must
     * remain LIVE with or without the fix (0057 rule 3). */
    A_CHECK(&r, !done);

    /* THE invariant (the no-EOT g64 contract): the below-watermark dangling
     * subgroup must be CLOSED (gap/resync progress), never left open
     * forever. */
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 0);

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards evict_overtake_dangling_subgroup\n");
    }
    return r.failures;
}


/* D2 — the begun/partial whole-object reset arm: the client's subgroup had
 * a complete object delivered AND partial bytes of a later object on the
 * wire when the owner's eviction killed the record. The relay's ONLY honest
 * terminal is a stream RESET (a FIN would frame the truncation as clean),
 * and libmoq's whole-object receive mode surfaces no event for a reset
 * subgroup — a known library-surface gap, queued separately. This pin
 * therefore asserts the RELAY-side truth: the downstream slot is torn down
 * (no dangle), the subscription stays live (no EOT), no false FIN is
 * emitted for the reset subgroup, the tracked partial is cleaned up, and
 * the connection survives. The streaming twin below observes the same reset
 * in the mode where libmoq CAN expose it today. */
static int
scenario_evict_reset_whole_object_pin(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex5(&r, &a, 0, 8, true, 64 /* group budget: unbound */,
                        2 /* channel entries */,
                        4700 /* byte budget */) != MOQR_OK) {
        printf("FAIL: whole-object reset-tail rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    /* The PUBLISHER leg streams (so the relay ingests the begun object's
     * open head); the SUBSCRIBER leg stays whole-object — the mode under
     * test. */
    r.streaming = true;
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    r.streaming = false;
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auR") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    static uint8_t body[1024];
    /* Group 0: one small COMPLETE object — opens the client's subgroup. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg0;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg0) == MOQ_OK);
    memset(body, 0xD0, 16);
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sg0, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 1);
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 1);

    /* Object 1 on the SAME subgroup: BEGUN (header + first chunk) and never
     * ended — the owner forwards the open head, so the record is begun on
     * both sides when the eviction lands. */
    A_CHECK(&r, moq_session_begin_object(pub->peer, sg0, 1, 64, r.now) ==
                    MOQ_OK);
    memset(body, 0xD1, 32);
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 32, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sg0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);

    /* Five 1 KiB groups in one burst: the byte budget evicts group 0 with
     * its second object still OPEN — the begun record dies, the reset
     * crosses, then the watermark follows. */
    for (uint64_t g = 3; g < 8; g++) {
        moq_subgroup_cfg_t sgc2;
        moq_subgroup_cfg_init(&sgc2);
        sgc2.group_id = g;
        sgc2.subgroup_id = 0;
        sgc2.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc2,
                                              r.now, &sg) == MOQ_OK);
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl2 = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl2) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl2, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl2);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
    }
    ep_run(&r, &e);
    int objs = 0;
    bool done = false;
    int fins = 0;
    bool g0_fin = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        objs += sps.objects;
        for (int f = 0; f < sps.sg_finished && f < 8; f++) {
            if (sps.sg_fin_group[f] == 0) {
                g0_fin = true;
            }
        }
        fins += sps.sg_finished;
        if (sps.done_seen) {
            done = true;
        }
        ep_run(&r, &e);
    }
    (void)fins;

    /* Guards: the mid-object overtake really happened. */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] >= 1);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_RESET] +
                    st1.enqueued[MOQR_SHARDS_MSG_OBJ_RESET] >= 1);
    A_CHECK(&r, objs >= 3);    /* g0 obj0 + surviving tail delivered */
    A_CHECK(&r, !done);        /* no EOT: the subscription stays live */

    /* Liveness first: a detached connection would make every later check
     * vacuous — the reset must not be a session teardown. */
    A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));

    /* Relay-side truth for the reset arm: slot torn down, subscription
     * live, and NO false FIN for the reset subgroup (its termination is
     * whole-object-invisible until the queued libmoq surface fix). */
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 0);
    A_CHECK(&r, !g0_fin);
    A_CHECK(&r, moqr_shards_debug_requester_open_objects(r.s, 0) == 0);   /* GROUP 0's stream specifically: the reset-tail
                            * subgroup must FIN observably, not just vanish */

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards evict_reset_whole_object_pin\n");
    }
    return r.failures;
}


#ifdef MOQ_EVENT_SUBGROUP_RESET
/* Whole-object upstream ingest: a publisher RESET of a subgroup mid-object
 * must reach the downstream subscriber as an abnormal terminal carrying the
 * publisher's error code — never as a clean FIN, and never as silence. The
 * relay's upstream session runs whole-object (streaming=false), where the
 * partial object is dropped by the session and the ONLY ingest-side evidence
 * of the stream ending is MOQ_EVENT_SUBGROUP_RESET. Retained complete
 * objects stay servable; the terminal follows them. */
static int
scenario_upstream_reset_whole_object_terminal(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex5(&r, &a, 0, 8, true, 64, 0 /* default entries */,
                        1u << 20 /* ample byte budget: no eviction */) !=
        MOQR_OK) {
        printf("FAIL: upstream-reset whole-object rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    /* BOTH legs whole-object: the upstream ingest mode under test, and a
     * downstream whose abnormal terminal is observable as SUBGROUP_RESET. */
    r.streaming = false;
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auW") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Object 0: COMPLETE — retained, delivered, opens the downstream
     * subgroup. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg0;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg0) == MOQ_OK);
    static uint8_t body[64];
    memset(body, 0xE0, 16);
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sg0, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 1);
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 1);

    /* Object 1: header + partial payload only, then a publisher RESET with
     * a distinctive code. The relay's whole-object upstream session drops
     * the partial (no OBJECT_RECEIVED for it) and surfaces the abnormal end
     * as SUBGROUP_RESET — after the resolved subgroup carried real data. */
    A_CHECK(&r, moq_session_begin_object(pub->peer, sg0, 1, 64, r.now) ==
                    MOQ_OK);
    memset(body, 0xE1, 32);
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 32, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sg0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    A_CHECK(&r, moq_session_reset_subgroup(pub->peer, sg0, 0x7E57, r.now) ==
                    MOQ_OK);
    ep_run(&r, &e);

    int resets = 0;
    bool g0_fin = false;
    bool done = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        if (sps.sg_reset > 0) {
            resets += sps.sg_reset;
            A_CHECK(&r, sps.sg_reset_group == 0);
            A_CHECK(&r, sps.sg_reset_sub == 0);
            A_CHECK(&r, sps.sg_reset_code == 0x7E57);
        }
        for (int f = 0; f < sps.sg_finished && f < 8; f++) {
            if (sps.sg_fin_group[f] == 0) {
                g0_fin = true;
            }
        }
        if (sps.done_seen) {
            done = true;
        }
        ep_run(&r, &e);
    }

    /* Liveness first: the upstream reset must not tear down the session. */
    A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));
    A_CHECK(&r, !done);       /* the subscription stays live             */

    /* The terminal crossed shards as a seal message (the publisher ingests
     * on shard 1, the subscriber consumes on shard 0), and no per-object
     * reset rode the channel — there is no record to abandon. */
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_SG_SEAL] >= 1);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_OBJ_RESET] == 0);

    /* The abnormal terminal reached the subscriber exactly once, with the
     * publisher's code — and never as a clean FIN. */
    A_CHECK(&r, resets == 1);
    A_CHECK(&r, !g0_fin);
    /* The downstream subgroup slot is reclaimed: nothing dangles. */
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 0);

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards upstream_reset_whole_object_terminal\n");
    }
    return r.failures;
}
#endif /* MOQ_EVENT_SUBGROUP_RESET */

#ifdef MOQ_EVENT_SUBGROUP_RESET
/* Ordering and late-join coherence for the reset-terminal subgroup: a
 * subscriber that arrives AFTER the upstream reset must first receive every
 * retained COMPLETE object of the subgroup, and only then the abnormal
 * terminal with the upstream code. The reset-terminal list must look
 * neither open (a dangling downstream stream) nor clean-sealed (a FIN). */
static int
scenario_upstream_reset_retained_then_terminal(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex5(&r, &a, 0, 8, true, 64, 0, 1u << 20) != MOQR_OK) {
        printf("FAIL: upstream-reset late-join rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    r.streaming = false;
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auL") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    /* A first subscriber activates the track so ingest retains records. */
    moq_subscription_t sh0;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh0);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    /* Two COMPLETE objects, then a partial third, then the RESET. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg0;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg0) == MOQ_OK);
    static uint8_t body[64];
    for (uint64_t o = 0; o < 2; o++) {
        memset(body, (int)(0xA0 + o), 16);
        moq_rcbuf_t *pl = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg0, o, pl,
                                             r.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    ep_run(&r, &e);
    A_CHECK(&r, moq_session_begin_object(pub->peer, sg0, 2, 64, r.now) ==
                    MOQ_OK);
    memset(body, 0xA2, 32);
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 32, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sg0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    A_CHECK(&r, moq_session_reset_subgroup(pub->peer, sg0, 0xBAD, r.now) ==
                    MOQ_OK);
    ep_run(&r, &e);

    /* The first subscriber observes its terminal; drain it out. */
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        ep_run(&r, &e);
    }

    /* Late join AFTER the terminal: the retained objects arrive first, then
     * the reset — order pinned by observing objects strictly before the
     * terminal, and the terminal only after ALL retained objects. */
    aconn_t *late = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, late != NULL);
    ep_run(&r, &e);
    moq_subscription_t sh1;
    asub_subscribe(&r, late, nsp, 1, "video", NULL, 0, &sh1);
    ep_run(&r, &e);
    int objs = 0;
    int resets = 0;
    bool fin_seen = false;
    bool reset_before_all_objs = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        aps_t lps;
        memset(&lps, 0, sizeof(lps));
        aps_drain(&r, late, &lps, false);
        if (lps.sg_reset > 0 && objs + lps.objects < 2) {
            reset_before_all_objs = true;   /* terminal overtook a record */
        }
        objs += lps.objects;
        if (lps.sg_reset > 0) {
            resets += lps.sg_reset;
            A_CHECK(&r, lps.sg_reset_group == 0);
            A_CHECK(&r, lps.sg_reset_code == 0xBAD);
        }
        if (lps.sg_finished > 0) {
            fin_seen = true;
        }
        ep_run(&r, &e);
    }
    A_CHECK(&r, objs == 2);                  /* both retained objects     */
    A_CHECK(&r, resets == 1);                /* exactly one terminal      */
    A_CHECK(&r, !reset_before_all_objs);     /* records precede terminal  */
    A_CHECK(&r, !fin_seen);                  /* never a clean FIN         */
    /* Nothing dangles on the late connection. */
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 1) == 0);

    A_CHECK(&r, moq_session_unsubscribe(late->peer, sh1, r.now) == MOQ_OK);
    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh0, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards upstream_reset_retained_then_terminal\n");
    }
    return r.failures;
}
#endif /* MOQ_EVENT_SUBGROUP_RESET */


/* D1 — the not-begun whole-object FIN arm: subgroup 0 carries only COMPLETE
 * delivered objects and its seal dies with the owner's eviction; subgroup 1
 * of the same group carries the begun/partial casualty. The reset honestly
 * terminalizes sg1 (whole-object-invisible until the queued libmoq surface
 * fix); sg0 owes the client a CLEAN, VISIBLE FIN — the watermark seals it
 * through the ordinary seal-note path and the client observes
 * SUBGROUP_FINISHED for (g0, sg0). Subscription stays live throughout. */
static int
scenario_evict_overtake_notbegun_fin(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex5(&r, &a, 0, 8, true, 64, 2, 4700) != MOQR_OK) {
        printf("FAIL: notbegun fin rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    r.streaming = true;    /* publisher leg streams the partial */
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    r.streaming = false;   /* subscriber leg: whole-object, the mode under test */
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auF") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    static uint8_t body[1024];
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg0;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg0) == MOQ_OK);
    memset(body, 0xD0, 16);
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sg0, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.objects == 1);
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) >= 1);

    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 1;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg1;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg1) == MOQ_OK);
    A_CHECK(&r, moq_session_begin_object(pub->peer, sg1, 0, 64, r.now) ==
                    MOQ_OK);
    memset(body, 0xD1, 32);
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 32, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sg1, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);

    for (uint64_t g = 3; g < 8; g++) {
        moq_subgroup_cfg_t sgc2;
        moq_subgroup_cfg_init(&sgc2);
        sgc2.group_id = g;
        sgc2.subgroup_id = 0;
        sgc2.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc2,
                                              r.now, &sg) == MOQ_OK);
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl2 = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl2) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl2, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl2);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
    }
    ep_run(&r, &e);
    int objs = 0;
    bool done = false;
    bool g0sg0_fin = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        objs += sps.objects;
        for (int f = 0; f < sps.sg_finished && f < 8; f++) {
            if (sps.sg_fin_group[f] == 0 && sps.sg_fin_sub[f] == 0) {
                g0sg0_fin = true;
            }
        }
        if (sps.done_seen) {
            done = true;
        }
        ep_run(&r, &e);
    }

    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] >= 1);
    /* Casualty proof for THIS arm: the begun partial died never-forwarded
     * (a correctly silent owner-side skip — the crossed-reset path is D2's
     * pin), so the destination holds fewer objects than were published. */
    {
        moqr_core_stats_t c0;
        moqr_core_get_stats(moqr_shards_core(r.s, 0), &c0);
        A_CHECK(&r, c0.ingested_total < 7);   /* 6 complete + partial published */
        A_CHECK(&r, c0.ingested_total >= 4);  /* survivors + g0 obj0 arrived */
    }
    A_CHECK(&r, objs >= 3);
    A_CHECK(&r, !done);
    A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 0);
    /* THE invariant: (g0, sg0) — complete content, dead seal — FINs
     * VISIBLY for the whole-object client. */
    A_CHECK(&r, g0sg0_fin);

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards evict_overtake_notbegun_fin\n");
    }
    return r.failures;
}

/* D2s — the begun/reset arm in STREAMING mode: the one mode where libmoq can
 * expose the reset today. Same shape as the whole-object pin; the client
 * observes the reset terminal (OBJECT_CHUNK terminal=RESET). */
static int
scenario_evict_reset_streaming_observed(void)
{
    ca_t a;
    ca_init(&a);
    arig_t r;
    if (arig_create_ex5(&r, &a, 0, 8, true, 64, 2, 4700) != MOQR_OK) {
        printf("FAIL: streaming reset rig create\n");
        return 1;
    }
    ep_t e;
    memset(&e, 0, sizeof(e));
    r.streaming = true;   /* BOTH legs stream: the observable mode */
    aconn_t *pub = arig_connect(&r, 1, MOQ_VERSION_DRAFT_18);
    aconn_t *sub = arig_connect(&r, 0, MOQ_VERSION_DRAFT_18);
    A_CHECK(&r, pub != NULL && sub != NULL);
    ep_run(&r, &e);
    moq_bytes_t nsp[1] = { B("auS") };
    apub_announce(&r, pub, nsp, 1, NULL, 0);
    ep_run(&r, &e);
    moq_subscription_t sh;
    asub_subscribe(&r, sub, nsp, 1, "video", NULL, 0, &sh);
    ep_run(&r, &e);
    aps_t pps;
    memset(&pps, 0, sizeof(pps));
    aps_drain(&r, pub, &pps, true);
    A_CHECK(&r, pps.up_seen && pps.up_subs == 1);
    ep_run(&r, &e);
    aps_t sps;
    memset(&sps, 0, sizeof(sps));
    aps_drain(&r, sub, &sps, false);
    A_CHECK(&r, sps.subscribe_ok);

    static uint8_t body[1024];
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sg0;
    A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc,
                                          r.now, &sg0) == MOQ_OK);
    memset(body, 0xD0, 16);
    moq_rcbuf_t *pl = NULL;
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 16, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object(pub->peer, sg0, 0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    A_CHECK(&r, moq_session_begin_object(pub->peer, sg0, 1, 64, r.now) ==
                    MOQ_OK);
    memset(body, 0xD1, 32);
    A_CHECK(&r, moq_rcbuf_create(&a.vt, body, 32, &pl) == MOQ_OK);
    A_CHECK(&r, moq_session_write_object_data(pub->peer, sg0, pl, r.now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
    ep_run(&r, &e);
    for (uint64_t g = 3; g < 8; g++) {
        moq_subgroup_cfg_t sgc2;
        moq_subgroup_cfg_init(&sgc2);
        sgc2.group_id = g;
        sgc2.subgroup_id = 0;
        sgc2.publisher_priority = 100;
        moq_subgroup_handle_t sg;
        A_CHECK(&r, moq_session_open_subgroup(pub->peer, pps.up_sub, &sgc2,
                                              r.now, &sg) == MOQ_OK);
        memset(body, (int)(0xD0 + g), sizeof(body));
        moq_rcbuf_t *pl2 = NULL;
        A_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl2) ==
                        MOQ_OK);
        A_CHECK(&r, moq_session_write_object(pub->peer, sg, 0, pl2, r.now) ==
                        MOQ_OK);
        moq_rcbuf_decref(pl2);
        A_CHECK(&r, moq_session_close_subgroup(pub->peer, sg, r.now) ==
                        MOQ_OK);
    }
    ep_run(&r, &e);
    bool done = false;
    bool reset_seen = false;
    for (int rounds = 0; rounds < 10; rounds++) {
        memset(&sps, 0, sizeof(sps));
        aps_drain(&r, sub, &sps, false);
        if (sps.saw_reset) {
            reset_seen = true;
        }
        if (sps.done_seen) {
            done = true;
        }
        ep_run(&r, &e);
    }
    moqr_shards_stats_t st1;
    A_CHECK(&r, moqr_shards_get_stats(r.s, 1, &st1) == MOQR_OK);
    A_CHECK(&r, st1.enqueued[MOQR_SHARDS_MSG_GRP_RESET] +
                    st1.enqueued[MOQR_SHARDS_MSG_OBJ_RESET] >= 1);
    A_CHECK(&r, !done);
    moqr_bind_t *b0 = moqr_shards_bind(r.s, 0);
    A_CHECK(&r, moqr_bind_conn_is_open(b0, sub->rsess));
    A_CHECK(&r, moqr_bind_debug_conn_open_sgs(b0, 0) == 0);
    /* THE invariant: streaming mode OBSERVES the honest reset. */
    A_CHECK(&r, reset_seen);

    A_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    ep_run(&r, &e);
    arig_destroy(&r);
    A_CHECK(&r, a.live == 0);
    if (r.failures == 0) {
        printf("PASS: auth_shards evict_reset_streaming_observed\n");
    }
    return r.failures;
}

int
main(void)
{
    int failures = 0;
    failures += scenario_requester_deny();
    failures += scenario_requester_allow();
    failures += scenario_requester_defer();
    failures += scenario_reval_deny_mid_object();
    failures += scenario_owner_announce_reval(MOQ_VERSION_DRAFT_16, 0x6u,
                                              "d16");
    failures += scenario_owner_announce_reval(MOQ_VERSION_DRAFT_18, 0x0u,
                                              "d18");
    failures += scenario_token_isolation();
    failures += scenario_bindsg_recovery(false);   /* A=(g0), B=(g1)     */
    failures += scenario_bindsg_recovery(true);    /* A=(g0,sg0) B=(g0,sg1) */
    failures += scenario_bindsg_recovery_burst();  /* all-at-once FIN drain */
    failures += scenario_bindsg_recovery_paced();  /* event-paced doorbells */
    failures += scenario_bindsg_batched_fin_wake();   /* coalesced FIN batch */
    failures += scenario_bindsg_drained_batch_no_wake();   /* drained: no wake */
    failures += scenario_actioncap_no_wake();   /* ACTION_CAP not continuation */
    failures += scenario_bindsg_batched_fin_wake_streamed();   /* streamed twin */
    failures += scenario_sessionsg_batched_fin_wake_streamed();   /* session pool */
    failures += scenario_bindsg_budget_chain();   /* multi-pass continuation */
    failures += scenario_seal_interval_wrap();    /* recorder-level interval RED */
    failures += scenario_evict_overtake_gap();    /* watermark gap, not silence */
    failures += scenario_evict_overtake_dangling_subgroup();  /* no-EOT arm: gap, stay live */
    failures += scenario_evict_reset_whole_object_pin();      /* D2: honest reset, no false FIN */
#ifdef MOQ_EVENT_SUBGROUP_RESET
    failures += scenario_upstream_reset_whole_object_terminal();
    failures += scenario_upstream_reset_retained_then_terminal();
#endif
    failures += scenario_evict_overtake_notbegun_fin();       /* D1: dead seal, visible FIN */
    failures += scenario_evict_reset_streaming_observed();    /* D2s: reset observable when it can be */
    failures += scenario_evict_overtake_silent_wait();    /* explicit terminal, never silence */
    failures += scenario_eot_complete_retires();      /* finished track completes */
    failures += scenario_eot_negative_no_synthesis(); /* fail-closed: no EOT / unknown gap */
    if (failures == 0) {
        printf("ALL PASS\n");
    }
    return failures == 0 ? 0 : 1;
}
