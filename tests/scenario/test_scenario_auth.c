/*
 * Deterministic AUTH_TOKEN scenario runner.
 *
 * Generates seeded sequences of AUTH_TOKEN operations (REGISTER,
 * USE_ALIAS, DELETE, USE_VALUE) across SUBSCRIBE messages fed as
 * raw encoded control bytes. Maintains a shadow alias model to
 * predict expected outcomes (success, request reject, session close).
 * Checks invariants after every step.
 *
 * Does NOT use public subscribe API for AUTH_TOKEN (no outbound
 * token support yet). Instead, encodes SUBSCRIBE messages with
 * AUTH_TOKEN params directly via codec helpers.
 */

#include <moq/codec.h>
#include "../../tests/unit/test_support.h"
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

/* -- Counting allocator --------------------------------------------- */

typedef struct { int64_t balance; } scen_alloc_state_t;

static void *scen_alloc(size_t size, void *ctx) {
    scen_alloc_state_t *s = (scen_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}
static void *scen_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx) {
    scen_alloc_state_t *s = (scen_alloc_state_t *)ctx;
    (void)old_sz;
    if (!ptr) { void *r = realloc(NULL, new_sz); if (r) s->balance++; return r; }
    return realloc(ptr, new_sz);
}
static void scen_free(void *ptr, size_t size, void *ctx) {
    scen_alloc_state_t *s = (scen_alloc_state_t *)ctx;
    (void)size;
    if (ptr) s->balance--;
    free(ptr);
}

/* -- Shadow alias model --------------------------------------------- */

#define MAX_SHADOW_ALIASES 32
#define MAX_SHADOW_SUBS    8
#define MAX_PENDING_TOKENS 4

typedef struct {
    uint64_t alias;
    uint64_t token_type;
    uint8_t  value[8];
    size_t   value_len;
    bool     active;
} shadow_alias_t;

typedef struct {
    bool               active;
    moq_subscription_t handle;
} shadow_sub_t;

typedef struct {
    moq_d16_auth_token_t tokens[MAX_PENDING_TOKENS];
    uint8_t              values[MAX_PENDING_TOKENS][8];
    size_t               count;
} pending_tokens_t;

typedef struct {
    shadow_alias_t aliases[MAX_SHADOW_ALIASES];
    shadow_sub_t   subs[MAX_SHADOW_SUBS];
    size_t         sub_count;
    uint64_t       next_peer_request_id;
    bool           closed;
    pending_tokens_t pending;
} shadow_state_t;

static bool shadow_alias_active(const shadow_state_t *st, uint64_t alias) {
    for (size_t i = 0; i < MAX_SHADOW_ALIASES; i++)
        if (st->aliases[i].active && st->aliases[i].alias == alias)
            return true;
    return false;
}

static void shadow_register(shadow_state_t *st, uint64_t alias,
                             uint64_t type, const uint8_t *val, size_t vlen) {
    for (size_t i = 0; i < MAX_SHADOW_ALIASES; i++) {
        if (!st->aliases[i].active) {
            st->aliases[i].active = true;
            st->aliases[i].alias = alias;
            st->aliases[i].token_type = type;
            size_t copy = vlen < sizeof(st->aliases[i].value) ? vlen : sizeof(st->aliases[i].value);
            if (copy > 0) memcpy(st->aliases[i].value, val, copy);
            st->aliases[i].value_len = copy;
            return;
        }
    }
}

static void shadow_delete(shadow_state_t *st, uint64_t alias) {
    for (size_t i = 0; i < MAX_SHADOW_ALIASES; i++) {
        if (st->aliases[i].active && st->aliases[i].alias == alias) {
            st->aliases[i].active = false;
            return;
        }
    }
}

/* -- Operation types ------------------------------------------------ */

typedef enum {
    SCEN_OP_PUMP,
    SCEN_OP_SUBSCRIBE,
    SCEN_OP_ACCEPT_SUBSCRIBE,
    SCEN_OP_DRAIN_EVENTS,
    SCEN_OP_AUTH_REGISTER,
    SCEN_OP_AUTH_USE_ALIAS,
    SCEN_OP_AUTH_DELETE,
    SCEN_OP_AUTH_USE_VALUE,
    SCEN_OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    switch (op) {
    case SCEN_OP_PUMP: return "PUMP";
    case SCEN_OP_SUBSCRIBE: return "SUBSCRIBE";
    case SCEN_OP_ACCEPT_SUBSCRIBE: return "ACCEPT_SUB";
    case SCEN_OP_DRAIN_EVENTS: return "DRAIN_EVENTS";
    case SCEN_OP_AUTH_REGISTER: return "AUTH_REGISTER";
    case SCEN_OP_AUTH_USE_ALIAS: return "AUTH_USE_ALIAS";
    case SCEN_OP_AUTH_DELETE: return "AUTH_DELETE";
    case SCEN_OP_AUTH_USE_VALUE: return "AUTH_USE_VALUE";
    default: return "?";
    }
}

/* -- Operation log -------------------------------------------------- */

#define MAX_LOG_ENTRIES 64

typedef struct {
    scenario_op_t op;
    uint64_t      param;
} log_entry_t;

typedef struct {
    log_entry_t entries[MAX_LOG_ENTRIES];
    size_t      count;
} op_log_t;

static void log_op(op_log_t *log, scenario_op_t op, uint64_t param) {
    if (log->count < MAX_LOG_ENTRIES) {
        log->entries[log->count].op = op;
        log->entries[log->count].param = param;
        log->count++;
    }
}

static void dump_log(uint64_t seed, const op_log_t *log) {
    fprintf(stderr, "  seed=0x%llx ops:\n", (unsigned long long)seed);
    for (size_t i = 0; i < log->count; i++)
        fprintf(stderr, "    %zu: %s param=%llu\n", i,
                op_name(log->entries[i].op),
                (unsigned long long)log->entries[i].param);
}

/* -- Encode helpers ------------------------------------------------- */

static size_t encode_auth_token(uint8_t *out, size_t cap,
                                 const moq_d16_auth_token_t *tok) {
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, cap);
    moq_d16_auth_token_encode(&w, tok);
    return moq_buf_writer_offset(&w);
}

static void feed_subscribe_with_auth(moq_session_t *sv,
                                      uint64_t request_id,
                                      const char *track_name,
                                      const moq_d16_auth_token_t *tokens,
                                      size_t token_count)
{
    moq_kvp_entry_t params[MAX_PENDING_TOKENS];
    uint8_t tok_bufs[MAX_PENDING_TOKENS][64];

    for (size_t i = 0; i < token_count && i < MAX_PENDING_TOKENS; i++) {
        size_t vlen = encode_auth_token(tok_bufs[i], sizeof(tok_bufs[i]), &tokens[i]);
        params[i].type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN;
        params[i].value = tok_bufs[i];
        params[i].value_len = vlen;
        params[i].is_varint = false;
        params[i].raw = NULL;
        params[i].raw_len = 0;
    }

    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_d16_encode_subscribe(&w, request_id, &ns,
        moq_bytes_cstr(track_name), params, token_count);
    moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
}

/* -- Predict outcome ------------------------------------------------ */

typedef enum {
    EXPECT_SUCCESS,
    EXPECT_REQUEST_REJECT,
    EXPECT_SESSION_CLOSE,
} expected_outcome_t;

static expected_outcome_t predict_auth_outcome(
    const shadow_state_t *st,
    const moq_d16_auth_token_t *tokens, size_t count)
{
    /* Simulate sequential processing with a local copy of alias state. */
    bool local_active[16];
    memset(local_active, 0, sizeof(local_active));
    for (size_t i = 0; i < MAX_SHADOW_ALIASES; i++)
        if (st->aliases[i].active && st->aliases[i].alias < 16)
            local_active[st->aliases[i].alias] = true;

    expected_outcome_t reject = EXPECT_SUCCESS;

    for (size_t i = 0; i < count; i++) {
        switch (tokens[i].alias_type) {
        case MOQ_AUTH_TOKEN_REGISTER:
            /* REGISTER always executes. Duplicate = alias already active
             * in local_active (which tracks DELETE effects within this
             * message). */
            if (tokens[i].alias < 16 && local_active[tokens[i].alias])
                return EXPECT_SESSION_CLOSE;
            if (tokens[i].alias < 16)
                local_active[tokens[i].alias] = true;
            break;
        case MOQ_AUTH_TOKEN_USE_ALIAS:
            if (reject == EXPECT_SUCCESS) {
                if (tokens[i].alias >= 16 || !local_active[tokens[i].alias])
                    reject = EXPECT_REQUEST_REJECT;
            }
            break;
        case MOQ_AUTH_TOKEN_DELETE:
            /* DELETE always executes. */
            if (tokens[i].alias < 16 && local_active[tokens[i].alias]) {
                local_active[tokens[i].alias] = false;
            } else if (reject == EXPECT_SUCCESS) {
                reject = EXPECT_REQUEST_REJECT;
            }
            break;
        case MOQ_AUTH_TOKEN_USE_VALUE:
            break;
        }
    }

    /* Check duplicate resolved (type, value) after alias resolution. */
    if (reject == EXPECT_SUCCESS) {
        struct { uint64_t type; size_t vlen; } resolved[MAX_PENDING_TOKENS];
        const uint8_t *rvals[MAX_PENDING_TOKENS];
        size_t rcount = 0;

        for (size_t i = 0; i < count; i++) {
            if (tokens[i].alias_type == MOQ_AUTH_TOKEN_DELETE) continue;
            uint64_t rtype = tokens[i].token_type;
            const uint8_t *rval = tokens[i].token_value;
            size_t rvlen = tokens[i].token_value_len;

            if (tokens[i].alias_type == MOQ_AUTH_TOKEN_USE_ALIAS) {
                for (size_t a = 0; a < MAX_SHADOW_ALIASES; a++) {
                    if (st->aliases[a].active && st->aliases[a].alias == tokens[i].alias) {
                        rtype = st->aliases[a].token_type;
                        rval = st->aliases[a].value;
                        rvlen = st->aliases[a].value_len;
                        break;
                    }
                }
            }

            for (size_t j = 0; j < rcount; j++) {
                if (resolved[j].type == rtype && resolved[j].vlen == rvlen &&
                    (rvlen == 0 || memcmp(rvals[j], rval, rvlen) == 0)) {
                    return EXPECT_REQUEST_REJECT;
                }
            }
            if (rcount < MAX_PENDING_TOKENS) {
                resolved[rcount].type = rtype;
                resolved[rcount].vlen = rvlen;
                rvals[rcount] = rval;
                rcount++;
            }
        }
    }

    return reject;
}

/* -- Execute one step ----------------------------------------------- */

static void pump_control(moq_session_t *from, moq_session_t *to) {
    moq_action_t acts[16]; size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(to,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
}

static void execute_step(moq_session_t *client, moq_session_t *sv,
                          shadow_state_t *st, rng_t *rng,
                          op_log_t *log)
{
    scenario_op_t op = (scenario_op_t)(rng_next(rng) % SCEN_OP_COUNT);

    switch (op) {
    case SCEN_OP_PUMP:
        log_op(log, op, 0);
        pump_control(sv, client); pump_control(client, sv);
        break;

    case SCEN_OP_SUBSCRIBE: {
        if (st->closed || st->pending.count == 0) {
            log_op(log, SCEN_OP_PUMP, 0);
            pump_control(sv, client); pump_control(client, sv);
            break;
        }

        char name[16];
        snprintf(name, sizeof(name), "t%llu",
                 (unsigned long long)st->next_peer_request_id);

        expected_outcome_t expect = predict_auth_outcome(st,
            st->pending.tokens, st->pending.count);

        log_op(log, op, st->next_peer_request_id);

        feed_subscribe_with_auth(sv, st->next_peer_request_id,
            name, st->pending.tokens, st->pending.count);

        if (expect == EXPECT_SESSION_CLOSE) {
            st->closed = true;
        } else {
            /* Apply side effects matching process_auth_tokens semantics:
             * REGISTER always executes (commits alias).
             * DELETE always executes (removes if exists).
             * USE_ALIAS lookup skipped after first reject. */
            bool rejected = false;
            for (size_t i = 0; i < st->pending.count; i++) {
                switch (st->pending.tokens[i].alias_type) {
                case MOQ_AUTH_TOKEN_REGISTER:
                    if (!shadow_alias_active(st, st->pending.tokens[i].alias)) {
                        shadow_register(st, st->pending.tokens[i].alias,
                            st->pending.tokens[i].token_type,
                            st->pending.tokens[i].token_value,
                            st->pending.tokens[i].token_value_len);
                    }
                    break;
                case MOQ_AUTH_TOKEN_DELETE:
                    if (shadow_alias_active(st, st->pending.tokens[i].alias)) {
                        shadow_delete(st, st->pending.tokens[i].alias);
                    } else if (!rejected) {
                        rejected = true;
                    }
                    break;
                case MOQ_AUTH_TOKEN_USE_ALIAS:
                    if (!rejected &&
                        !shadow_alias_active(st, st->pending.tokens[i].alias))
                        rejected = true;
                    break;
                default:
                    break;
                }
            }
            st->next_peer_request_id += 2;
        }
        st->pending.count = 0;
        break;
    }

    case SCEN_OP_ACCEPT_SUBSCRIBE: {
        if (st->closed) { log_op(log, SCEN_OP_PUMP, 0); break; }
        moq_event_t ev;
        if (moq_session_poll_events(sv, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                log_op(log, op, 0);
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
                if (st->sub_count < MAX_SHADOW_SUBS) {
                    st->subs[st->sub_count].active = true;
                    st->subs[st->sub_count].handle = ev.u.subscribe_request.sub;
                    st->sub_count++;
                }
                pump_control(sv, client); pump_control(client, sv);
                moq_event_t drain;
                if (moq_session_poll_events(client, &drain, 1) == 1)
                    moq_event_cleanup(&drain);
            } else {
                log_op(log, SCEN_OP_PUMP, 0);
            }
            moq_event_cleanup(&ev);
        } else {
            log_op(log, SCEN_OP_PUMP, 0);
        }
        break;
    }

    case SCEN_OP_DRAIN_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        break;
    }

    case SCEN_OP_AUTH_REGISTER: {
        if (st->pending.count >= MAX_PENDING_TOKENS) {
            log_op(log, SCEN_OP_PUMP, 0); break;
        }
        uint64_t alias = rng_next(rng) % 16;
        size_t idx = st->pending.count;
        uint64_t r = rng_next(rng);
        memcpy(st->pending.values[idx], &r, 4);
        size_t vlen = (rng_next(rng) % 5);

        log_op(log, op, alias);

        moq_d16_auth_token_t *t = &st->pending.tokens[st->pending.count++];
        t->alias_type = MOQ_AUTH_TOKEN_REGISTER;
        t->alias = alias;
        t->token_type = rng_next(rng) % 10;
        t->token_value = st->pending.values[idx];
        t->token_value_len = vlen;
        break;
    }

    case SCEN_OP_AUTH_USE_ALIAS: {
        if (st->pending.count >= MAX_PENDING_TOKENS) {
            log_op(log, SCEN_OP_PUMP, 0); break;
        }
        uint64_t alias = rng_next(rng) % 16;
        log_op(log, op, alias);

        moq_d16_auth_token_t *t = &st->pending.tokens[st->pending.count++];
        t->alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        t->alias = alias;
        break;
    }

    case SCEN_OP_AUTH_DELETE: {
        if (st->pending.count >= MAX_PENDING_TOKENS) {
            log_op(log, SCEN_OP_PUMP, 0); break;
        }
        uint64_t alias = rng_next(rng) % 16;
        log_op(log, op, alias);

        moq_d16_auth_token_t *t = &st->pending.tokens[st->pending.count++];
        t->alias_type = MOQ_AUTH_TOKEN_DELETE;
        t->alias = alias;
        break;
    }

    case SCEN_OP_AUTH_USE_VALUE: {
        if (st->pending.count >= MAX_PENDING_TOKENS) {
            log_op(log, SCEN_OP_PUMP, 0); break;
        }
        log_op(log, op, 0);

        size_t idx = st->pending.count;
        uint64_t r = rng_next(rng);
        memcpy(st->pending.values[idx], &r, 4);

        moq_d16_auth_token_t *t = &st->pending.tokens[st->pending.count++];
        t->alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        t->token_type = rng_next(rng) % 10;
        t->token_value = st->pending.values[idx];
        t->token_value_len = rng_next(rng) % 5;
        break;
    }

    default:
        log_op(log, SCEN_OP_PUMP, 0);
        break;
    }
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

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        scen_alloc_state_t as = {0};
        moq_alloc_t alloc = { &as, scen_alloc, scen_realloc, scen_free };

        /* Create sessions directly (not via SimPair) to configure auth cache. */
        moq_session_cfg_t ccfg;
        moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 100;
        ccfg.send_auth_token_cache_size = true;
        ccfg.auth_token_cache_size = 4096;

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &alloc, MOQ_PERSPECTIVE_SERVER);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 100;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 4096;

        moq_session_t *client = NULL, *server = NULL;
        if (moq_session_create(&ccfg, 0, &client) < 0) continue;
        if (moq_session_create(&scfg, 0, &server) < 0) {
            moq_session_destroy(client); continue;
        }
        moq_session_start(client, 0);
        pump_control(client, server);
        pump_control(server, client);

        {
            moq_event_t ev;
            if (moq_session_poll_events(client, &ev, 1) == 1)
                moq_event_cleanup(&ev);
            if (moq_session_poll_events(server, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }

        shadow_state_t st;
        memset(&st, 0, sizeof(st));
        st.next_peer_request_id = 0;

        rng_t rng = { seed };
        op_log_t log = {0};

        for (int step = 0; step < max_steps; step++) {
            execute_step(client, server, &st, &rng, &log);

            if (st.closed) {
                if (moq_session_state(server) != MOQ_SESS_CLOSED) {
                    fprintf(stderr, "FAIL seed=0x%llx step=%d: "
                            "expected CLOSED but got %d\n",
                            (unsigned long long)seed, step,
                            moq_session_state(server));
                    dump_log(seed, &log);
                    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                            "./build/tests/test_scenario_auth\n",
                            (unsigned long long)seed, max_steps);
                    failures++;
                }
                break;
            } else {
                if (moq_session_state(server) == MOQ_SESS_CLOSED) {
                    fprintf(stderr, "FAIL seed=0x%llx step=%d: "
                            "unexpected session close\n",
                            (unsigned long long)seed, step);
                    dump_log(seed, &log);
                    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                            "./build/tests/test_scenario_auth\n",
                            (unsigned long long)seed, max_steps);
                    failures++;
                    st.closed = true;
                    break;
                }
            }
        }

        if (!st.closed) {
            moq_event_t drain[16];
            size_t ne;
            while ((ne = moq_session_poll_events(server, drain, 16)) > 0)
                for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
            while ((ne = moq_session_poll_events(client, drain, 16)) > 0)
                for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
            pump_control(server, client);
            pump_control(client, server);
        }

        moq_session_destroy(client);
        moq_session_destroy(server);

        if (as.balance != 0) {
            fprintf(stderr, "FAIL seed=0x%llx: alloc balance=%lld\n",
                    (unsigned long long)seed, (long long)as.balance);
            dump_log(seed, &log);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_auth\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_auth (%llu seeds)\n",
                (unsigned long long)num_seeds);
    else
        fprintf(stderr, "FAIL: test_scenario_auth (%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
