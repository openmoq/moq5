/*
 * Deterministic request-credit cycling scenario runner.
 *
 * Proves that SUBSCRIBE and PUBLISH_NAMESPACE share request ID
 * credit correctly under random ordering with tiny initial credit
 * and periodic MAX_REQUEST_ID increases from the peer.
 *
 * Uses raw establish_pair (not SimPair) with initial_request_capacity
 * low enough that the first or second outbound request exhausts
 * credit. A seeded random phase interleaves subscribe, publish
 * namespace, accept/reject/done/cancel, MAX_REQUEST_ID grants,
 * pump, and drain. Each seed runs twice; all counters and the
 * FNV-1a hash must match.
 *
 * Core-only (no SimPair dependency); runs in no-sim builds.
 */

#include <moq/session.h>
#include <moq/codec.h>
#include "../../tests/unit/test_support.h"
#include "../../tests/unit/test_session_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* -- splitmix64 PRNG ------------------------------------------------ */

typedef struct { uint64_t s; } rng_t;

static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* -- Operation hash ------------------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   ops;
    size_t   credit_blocked_sub;
    size_t   credit_blocked_ns;
    size_t   credit_grants;
    size_t   retry_after_credit;
    size_t   sub_ok;
    size_t   ns_ok;
    size_t   request_errors;
} run_summary_t;

static void hash_op(run_summary_t *s, uint64_t v) {
    s->hash ^= v; s->hash *= 0x100000001B3ULL;
    s->ops++;
}

/* -- Counting allocator --------------------------------------------- */

typedef struct { int64_t balance; } scen_alloc_t;

static void *sa_alloc(size_t sz, void *ctx) {
    scen_alloc_t *s = (scen_alloc_t *)ctx;
    void *p = malloc(sz); if (p) s->balance++;
    return p;
}
static void *sa_realloc(void *p, size_t o, size_t n, void *ctx) {
    scen_alloc_t *s = (scen_alloc_t *)ctx;
    (void)o;
    if (!p) { void *r = realloc(NULL, n); if (r) s->balance++; return r; }
    return realloc(p, n);
}
static void sa_free(void *p, size_t sz, void *ctx) {
    scen_alloc_t *s = (scen_alloc_t *)ctx;
    (void)sz; if (p) s->balance--;
    free(p);
}

/* -- Drain helpers -------------------------------------------------- */

static void drain_events(moq_session_t *s) {
    moq_event_t evts[8]; size_t ne;
    while ((ne = moq_session_poll_events(s, evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++)
            moq_event_cleanup(&evts[i]);
}

static void drain_both(moq_session_t *c, moq_session_t *sv) {
    drain_events(c); drain_events(sv);
    moq_action_t acts[8]; size_t na;
    while ((na = moq_session_poll_actions(c, acts, 8)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    while ((na = moq_session_poll_actions(sv, acts, 8)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
}

/* -- Shadow model --------------------------------------------------- */

#define MAX_SUBS 8
#define MAX_ANNS 8

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    bool has_server_handle;
    moq_subscription_t client_handle;
    moq_subscription_t server_handle;
} shadow_sub_t;

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    bool has_client_handle;
    bool has_server_handle;
    moq_announcement_t server_handle;
    moq_announcement_t client_handle;
} shadow_ann_t;

typedef struct {
    shadow_sub_t subs[MAX_SUBS];
    shadow_ann_t anns[MAX_ANNS];
    bool closed;
    uint64_t max_credit_sent;
    bool has_blocked_sub;
    bool has_blocked_ns;
} shadow_state_t;

/* -- Credit grant helper -------------------------------------------- */

static moq_result_t grant_credit(moq_session_t *granter,
                                  moq_session_t *receiver,
                                  uint64_t new_max)
{
    moq_result_t rc = moq_session_update_max_request_id(granter, new_max, 0);
    if (rc < 0) return rc;
    pump_actions_to_peer(granter, receiver, 0);
    return MOQ_OK;
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_SUBSCRIBE,
    OP_PUB_NS,
    OP_ACCEPT_SUB,
    OP_REJECT_SUB,
    OP_ACCEPT_NS,
    OP_NS_DONE,
    OP_GRANT_CREDIT,
    OP_PUMP,
    OP_DRAIN_C,
    OP_DRAIN_S,
    OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *n[] = {
        "SUB","PUB_NS","ACC_SUB","REJ_SUB","ACC_NS","NS_DONE",
        "GRANT","PUMP","DRAIN_C","DRAIN_S",
    };
    return op < OP_COUNT ? n[op] : "?";
}

#define MAX_LOG 128
typedef struct { scenario_op_t op; uint64_t p; } log_entry_t;
typedef struct { log_entry_t e[MAX_LOG]; size_t n; } op_log_t;

static void log_op(op_log_t *l, scenario_op_t op, uint64_t p) {
    if (l->n < MAX_LOG) { l->e[l->n].op = op; l->e[l->n].p = p; l->n++; }
}

static void dump_log(uint64_t seed, int run, const op_log_t *l) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < l->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(l->e[i].op), (unsigned long long)l->e[i].p);
}

/* -- Execute step --------------------------------------------------- */

static int execute_step(moq_session_t *c, moq_session_t *sv,
                         shadow_state_t *st, rng_t *rng,
                         op_log_t *log, run_summary_t *ts)
{
    if (st->closed) return 0;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);

    switch (op) {
    case OP_SUBSCRIBE: {
        int slot = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (!st->subs[i].active) { slot = i; break; }
        if (slot < 0) { log_op(log, OP_PUMP, 0); break; }

        char name[16];
        snprintf(name, sizeof(name), "s%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("rc") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = moq_bytes_cstr(name);
        cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

        moq_subscription_t h;
        moq_result_t rc = moq_session_subscribe(c, &cfg, 0, &h);
        hash_op(ts, (uint64_t)rc);
        log_op(log, op, (uint64_t)rc);

        if (rc == MOQ_OK) {
            st->subs[slot].active = true;
            st->subs[slot].pending = true;
            st->subs[slot].client_handle = h;
            ts->sub_ok++;

            pump_actions_to_peer(c, sv, 0);
            moq_event_t ev;
            if (moq_session_poll_events(sv, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    st->subs[slot].server_handle = ev.u.subscribe_request.sub;
                    st->subs[slot].has_server_handle = true;
                }
                moq_event_cleanup(&ev);
            }
        } else if (rc == MOQ_ERR_REQUEST_BLOCKED) {
            ts->credit_blocked_sub++;
            st->has_blocked_sub = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_INVAL) {
            return -1;
        }
        break;
    }

    case OP_PUB_NS: {
        int slot = -1;
        for (int i = 0; i < MAX_ANNS; i++)
            if (!st->anns[i].active) { slot = i; break; }
        if (slot < 0) { log_op(log, OP_PUMP, 0); break; }

        char ns_name[16];
        snprintf(ns_name, sizeof(ns_name), "n%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { moq_bytes_cstr(ns_name) };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;

        moq_announcement_t h;
        moq_result_t rc = moq_session_publish_namespace(c, &cfg, 0, &h);
        hash_op(ts, (uint64_t)rc);
        log_op(log, op, (uint64_t)rc);

        if (rc == MOQ_OK) {
            st->anns[slot].active = true;
            st->anns[slot].pending = true;
            st->anns[slot].client_handle = h;
            st->anns[slot].has_client_handle = true;
            ts->ns_ok++;

            pump_actions_to_peer(c, sv, 0);
            moq_event_t ev;
            if (moq_session_poll_events(sv, &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                    st->anns[slot].server_handle = ev.u.namespace_published.ann;
                    st->anns[slot].has_server_handle = true;
                }
                moq_event_cleanup(&ev);
            }
        } else if (rc == MOQ_ERR_REQUEST_BLOCKED) {
            ts->credit_blocked_ns++;
            st->has_blocked_ns = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_INVAL) {
            return -1;
        }
        break;
    }

    case OP_ACCEPT_SUB: {
        int idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && st->subs[i].pending &&
                st->subs[i].has_server_handle) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(sv,
            st->subs[idx].server_handle, &acc, 0);
        hash_op(ts, (uint64_t)rc);
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->subs[idx].pending = false;
            st->subs[idx].accepted = true;
            pump_actions_to_peer(sv, c, 0);
            drain_events(c);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            pump_actions_to_peer(sv, c, 0);
        } else if (rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_STALE_HANDLE) {
            return -1;
        }
        break;
    }

    case OP_REJECT_SUB: {
        int idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && st->subs[i].pending &&
                st->subs[i].has_server_handle) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        rej.reason = MOQ_BYTES_LITERAL("no");
        moq_result_t rc = moq_session_reject_subscribe(sv,
            st->subs[idx].server_handle, &rej, 0);
        hash_op(ts, (uint64_t)rc);
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->subs[idx].active = false;
            ts->request_errors++;
            pump_actions_to_peer(sv, c, 0);
            drain_events(c);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            pump_actions_to_peer(sv, c, 0);
        } else if (rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_STALE_HANDLE) {
            return -1;
        }
        break;
    }

    case OP_ACCEPT_NS: {
        int idx = -1;
        for (int i = 0; i < MAX_ANNS; i++)
            if (st->anns[i].active && st->anns[i].pending &&
                st->anns[i].has_server_handle) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_accept_namespace_cfg_t nacc;
        moq_accept_namespace_cfg_init(&nacc);
        moq_result_t rc = moq_session_accept_namespace(sv,
            st->anns[idx].server_handle, &nacc, 0);
        hash_op(ts, (uint64_t)rc);
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->anns[idx].pending = false;
            st->anns[idx].accepted = true;
            pump_actions_to_peer(sv, c, 0);
            drain_events(c);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            pump_actions_to_peer(sv, c, 0);
        } else if (rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_STALE_HANDLE) {
            return -1;
        }
        break;
    }

    case OP_NS_DONE: {
        int idx = -1;
        for (int i = 0; i < MAX_ANNS; i++)
            if (st->anns[i].active && st->anns[i].accepted) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        moq_result_t rc = moq_session_publish_namespace_done(c,
            st->anns[idx].client_handle, 0);
        hash_op(ts, (uint64_t)rc);
        log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_OK) {
            st->anns[idx].active = false;
            pump_actions_to_peer(c, sv, 0);
            drain_events(sv);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            pump_actions_to_peer(c, sv, 0);
        } else if (rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_STALE_HANDLE &&
                   rc != MOQ_ERR_WRONG_STATE) {
            return -1;
        }
        if (rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_WRONG_STATE)
            st->anns[idx].active = false;
        break;
    }

    case OP_GRANT_CREDIT: {
        uint64_t bump = 2 + (rng_next(rng) % 4) * 2;
        uint64_t new_max = st->max_credit_sent + bump;
        log_op(log, op, new_max);

        moq_result_t rc = moq_session_update_max_request_id(sv, new_max, 0);
        hash_op(ts, (uint64_t)rc);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            pump_actions_to_peer(sv, c, 0);
            rc = moq_session_update_max_request_id(sv, new_max, 0);
            hash_op(ts, (uint64_t)rc);
        }
        if (rc < 0 && rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_INVAL)
            return -1;
        if (rc == MOQ_OK) {
            pump_actions_to_peer(sv, c, 0);
            st->max_credit_sent = new_max;
            ts->credit_grants++;
        }
        drain_events(c);

        /* Retry a previously blocked operation now that credit
         * is available. Alternate between sub and ns based on
         * what was blocked. */
        if (st->has_blocked_sub) {
            int slot = -1;
            for (int i = 0; i < MAX_SUBS; i++)
                if (!st->subs[i].active) { slot = i; break; }
            if (slot >= 0) {
                char name[16];
                snprintf(name, sizeof(name), "cr%llu",
                         (unsigned long long)(rng_next(rng) % 100));
                moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("rc") };
                moq_namespace_t ns = { ns_parts, 1 };
                moq_subscribe_cfg_t cfg;
                moq_subscribe_cfg_init(&cfg);
                cfg.track_namespace = ns;
                cfg.track_name = moq_bytes_cstr(name);
                cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
                moq_subscription_t h;
                rc = moq_session_subscribe(c, &cfg, 0, &h);
                hash_op(ts, (uint64_t)rc);
                if (rc == MOQ_OK) {
                    st->subs[slot].active = true;
                    st->subs[slot].pending = true;
                    st->subs[slot].client_handle = h;
                    ts->sub_ok++;
                    ts->retry_after_credit++;
                    st->has_blocked_sub = false;
                    pump_actions_to_peer(c, sv, 0);
                    moq_event_t ev;
                    if (moq_session_poll_events(sv, &ev, 1) == 1) {
                        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                            st->subs[slot].server_handle = ev.u.subscribe_request.sub;
                            st->subs[slot].has_server_handle = true;
                        }
                        moq_event_cleanup(&ev);
                    }
                } else if (rc == MOQ_ERR_REQUEST_BLOCKED) {
                    ts->credit_blocked_sub++;
                }
            }
        } else if (st->has_blocked_ns) {
            int slot = -1;
            for (int i = 0; i < MAX_ANNS; i++)
                if (!st->anns[i].active) { slot = i; break; }
            if (slot >= 0) {
                char ns_name[16];
                snprintf(ns_name, sizeof(ns_name), "cr%llu",
                         (unsigned long long)(rng_next(rng) % 100));
                moq_publish_namespace_cfg_t cfg;
                moq_publish_namespace_cfg_init(&cfg);
                moq_bytes_t ns_parts[] = { moq_bytes_cstr(ns_name) };
                cfg.track_namespace.parts = ns_parts;
                cfg.track_namespace.count = 1;
                moq_announcement_t h;
                rc = moq_session_publish_namespace(c, &cfg, 0, &h);
                hash_op(ts, (uint64_t)rc);
                if (rc == MOQ_OK) {
                    st->anns[slot].active = true;
                    st->anns[slot].pending = true;
                    st->anns[slot].client_handle = h;
                    st->anns[slot].has_client_handle = true;
                    ts->ns_ok++;
                    ts->retry_after_credit++;
                    st->has_blocked_ns = false;
                    pump_actions_to_peer(c, sv, 0);
                    moq_event_t ev;
                    if (moq_session_poll_events(sv, &ev, 1) == 1) {
                        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                            st->anns[slot].server_handle = ev.u.namespace_published.ann;
                            st->anns[slot].has_server_handle = true;
                        }
                        moq_event_cleanup(&ev);
                    }
                } else if (rc == MOQ_ERR_REQUEST_BLOCKED) {
                    ts->credit_blocked_ns++;
                }
            }
        }
        break;
    }

    case OP_PUMP:
        log_op(log, op, 0);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);
        break;

    case OP_DRAIN_C:
        log_op(log, op, 0);
        drain_events(c);
        break;

    case OP_DRAIN_S:
        log_op(log, op, 0);
        drain_events(sv);
        break;

    default:
        log_op(log, OP_PUMP, 0);
        break;
    }

    if (moq_session_state(c) == MOQ_SESS_CLOSED ||
        moq_session_state(sv) == MOQ_SESS_CLOSED)
        st->closed = true;

    return 0;
}

/* -- Deterministic prelude ------------------------------------------ */

static int run_prelude(moq_session_t *c, moq_session_t *sv,
                       run_summary_t *ts)
{
    /* Client has initial_request_capacity=2 from server → credit for
     * one request (ID 0; next would be 2, blocked at >= 2).
     * Both subscribe and publish_namespace share the client's
     * next_local_request_id counter. */

    /* 1. Client subscribes → OK (uses request ID 0, next=2). */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("rc") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("pre");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t csub;
    moq_result_t rc = moq_session_subscribe(c, &sub_cfg, 0, &csub);
    if (rc != MOQ_OK) return -1;
    ts->sub_ok++;

    pump_actions_to_peer(c, sv, 0);
    if (moq_session_state(sv) == MOQ_SESS_CLOSED) return -1;
    drain_events(c);

    /* 2. Client subscribes again → REQUEST_BLOCKED (2 >= 2). */
    sub_cfg.track_name = MOQ_BYTES_LITERAL("blk");
    moq_subscription_t csub2;
    rc = moq_session_subscribe(c, &sub_cfg, 0, &csub2);
    if (rc != MOQ_ERR_REQUEST_BLOCKED) return -1;
    ts->credit_blocked_sub++;

    /* 3. Client publish_namespace → also blocked (same counter). */
    moq_publish_namespace_cfg_t pn;
    moq_publish_namespace_cfg_init(&pn);
    moq_bytes_t pn_parts[] = { MOQ_BYTES_LITERAL("blk") };
    pn.track_namespace.parts = pn_parts;
    pn.track_namespace.count = 1;
    moq_announcement_t ann;
    rc = moq_session_publish_namespace(c, &pn, 0, &ann);
    if (rc != MOQ_ERR_REQUEST_BLOCKED) return -1;
    ts->credit_blocked_ns++;

    /* 4. Server grants client credit (MAX_REQUEST_ID=4) → subscribe retries. */
    rc = grant_credit(sv, c, 4);
    if (rc < 0) return -1;
    ts->credit_grants++;
    drain_events(c);

    rc = moq_session_subscribe(c, &sub_cfg, 0, &csub2);
    if (rc != MOQ_OK) return -1;
    ts->sub_ok++;
    ts->retry_after_credit++;

    pump_actions_to_peer(c, sv, 0);
    if (moq_session_state(sv) == MOQ_SESS_CLOSED) return -1;
    drain_events(c);

    /* 5. Client publish_namespace → OK (uses ID 4, next=6). Credit
     * was 4 so ID 4 < 4 is false... grant more first. */
    rc = grant_credit(sv, c, 6);
    if (rc < 0) return -1;
    ts->credit_grants++;
    drain_events(c);

    rc = moq_session_publish_namespace(c, &pn, 0, &ann);
    if (rc != MOQ_OK) return -1;
    ts->ns_ok++;
    ts->retry_after_credit++;

    pump_actions_to_peer(c, sv, 0);
    if (moq_session_state(sv) == MOQ_SESS_CLOSED) return -1;

    /* 6. Accept the subscribe requests and namespace on the server.
     * Explicitly assert each event to prove cross-family registry
     * routing: REQUEST_OK dispatches to the right family. */
    {
        moq_event_t ev;
        int sub_requests = 0, ns_published = 0;

        while (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                if (moq_session_accept_subscribe(sv,
                    ev.u.subscribe_request.sub, &acc, 0) != MOQ_OK) {
                    moq_event_cleanup(&ev); return -1;
                }
                sub_requests++;
            } else if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                moq_accept_namespace_cfg_t nacc;
                moq_accept_namespace_cfg_init(&nacc);
                if (moq_session_accept_namespace(sv,
                    ev.u.namespace_published.ann, &nacc, 0) != MOQ_OK) {
                    moq_event_cleanup(&ev); return -1;
                }
                ns_published++;
            }
            moq_event_cleanup(&ev);
        }
        if (sub_requests != 2) return -1;
        if (ns_published != 1) return -1;

        pump_actions_to_peer(sv, c, 0);

        /* Client must receive SUBSCRIBE_OK for both subs and
         * NAMESPACE_ACCEPTED for the namespace. */
        int sub_ok_events = 0, ns_accepted_events = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok_events++;
            else if (ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED) ns_accepted_events++;
            moq_event_cleanup(&ev);
        }
        if (sub_ok_events != 2) return -1;
        if (ns_accepted_events != 1) return -1;
    }

    drain_both(c, sv);
    return 0;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, moq_alloc_t *alloc,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->hash = 0xCBF29CE484222325ULL;

    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(alloc, 2, 2, &c, &sv, NULL, NULL);
    drain_both(c, sv);

    int failures = 0;
    int prelude_rc = run_prelude(c, sv, summary);
    if (prelude_rc < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_request_credit\n",
                (unsigned long long)seed, max_steps);
        failures++;
    }

    drain_both(c, sv);
    moq_session_destroy(c);
    moq_session_destroy(sv);

    /* Random phase: fresh pair with tiny credit. */
    if (prelude_rc >= 0) {
        establish_pair(alloc, 2, 2, &c, &sv, NULL, NULL);
        drain_both(c, sv);

        shadow_state_t st;
        memset(&st, 0, sizeof(st));
        st.max_credit_sent = 2;
        rng_t rng = { seed };
        op_log_t log = {0};

        for (int step = 0; step < max_steps && !st.closed; step++) {
            int src = execute_step(c, sv, &st, &rng, &log, summary);
            summary->ops++;

            if (src < 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "unexpected error\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_request_credit\n",
                        (unsigned long long)seed, max_steps);
                failures++;
                break;
            }
        }

        drain_both(c, sv);
        moq_session_destroy(c);
        moq_session_destroy(sv);
    }

    return failures;
}

/* -- Main ----------------------------------------------------------- */

int main(void)
{
    int failures = 0;
    const char *env_seeds = getenv("MOQ_SCENARIO_SEEDS");
    const char *env_start = getenv("MOQ_SCENARIO_SEED_START");
    const char *env_steps = getenv("MOQ_SCENARIO_STEPS");
    uint64_t num_seeds = env_seeds ? (uint64_t)strtoull(env_seeds, NULL, 0) : 100;
    uint64_t seed_start = env_start ? (uint64_t)strtoull(env_start, NULL, 0) : 0;
    int max_steps = env_steps ? atoi(env_steps) : 50;

    size_t total_blocked_sub = 0, total_blocked_ns = 0;
    size_t total_grants = 0, total_retry = 0;
    size_t total_sub_ok = 0, total_ns_ok = 0;
    size_t total_errors = 0;

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        run_summary_t sums[2];

        for (int run = 0; run < 2; run++) {
            scen_alloc_t as = {0};
            moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };

            int rf = run_scenario(seed, &alloc, &sums[run], run, max_steps);
            failures += rf;

            if (as.balance != 0) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: "
                        "alloc balance=%lld\n",
                        (unsigned long long)seed, run,
                        (long long)as.balance);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_request_credit\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        total_blocked_sub += sums[0].credit_blocked_sub;
        total_blocked_ns += sums[0].credit_blocked_ns;
        total_grants += sums[0].credit_grants;
        total_retry += sums[0].retry_after_credit;
        total_sub_ok += sums[0].sub_ok;
        total_ns_ok += sums[0].ns_ok;
        total_errors += sums[0].request_errors;

        if (sums[0].hash != sums[1].hash ||
            sums[0].ops != sums[1].ops ||
            sums[0].credit_blocked_sub != sums[1].credit_blocked_sub ||
            sums[0].credit_blocked_ns != sums[1].credit_blocked_ns ||
            sums[0].credit_grants != sums[1].credit_grants ||
            sums[0].retry_after_credit != sums[1].retry_after_credit ||
            sums[0].sub_ok != sums[1].sub_ok ||
            sums[0].ns_ok != sums[1].ns_ok ||
            sums[0].request_errors != sums[1].request_errors) {
            fprintf(stderr, "FAIL seed=0x%llx: counter/hash mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_request_credit\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }
    }

    if (num_seeds >= 1) {
        if (total_blocked_sub == 0) {
            fprintf(stderr, "FAIL: no credit_blocked_sub\n");
            failures++;
        }
        if (total_blocked_ns == 0) {
            fprintf(stderr, "FAIL: no credit_blocked_ns\n");
            failures++;
        }
        if (total_grants == 0) {
            fprintf(stderr, "FAIL: no credit_grants\n");
            failures++;
        }
        if (total_retry == 0) {
            fprintf(stderr, "FAIL: no retry_after_credit\n");
            failures++;
        }
    }

    if (num_seeds >= 10) {
        if (total_sub_ok == 0) {
            fprintf(stderr, "FAIL: no sub_ok\n");
            failures++;
        }
        if (total_ns_ok == 0) {
            fprintf(stderr, "FAIL: no ns_ok\n");
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_request_credit "
                "(%llu seeds, blk_sub=%zu blk_ns=%zu grants=%zu "
                "retry=%zu sub_ok=%zu ns_ok=%zu errors=%zu)\n",
                (unsigned long long)num_seeds,
                total_blocked_sub, total_blocked_ns,
                total_grants, total_retry,
                total_sub_ok, total_ns_ok, total_errors);
    else
        fprintf(stderr, "FAIL: test_scenario_request_credit "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
