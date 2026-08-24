/*
 * Deterministic object delivery scenario runner.
 *
 * Two-tier oracle:
 * 1. Deterministic prelude: guaranteed-delivery path with exact
 *    expected == received check (group_id, object_id, payload).
 * 2. Random phase: randomized subscribe/accept/open/write/close/
 *    reset with checked chunked data feeding. Oracle checks
 *    received <= fed and deterministic counts/hashes across runs.
 *
 * Each seed runs twice; trace hash + all oracle counters must match.
 */

#include <moq/sim.h>
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

/* -- Trace hash ----------------------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   count;
    /* Prelude: exact delivery oracle (expected == received). */
    uint64_t pre_expected_hash;
    size_t   pre_expected_count;
    uint64_t pre_received_hash;
    size_t   pre_received_count;
    /* Random: no-phantom oracle (received <= fed). */
    uint64_t rnd_fed_hash;
    size_t   rnd_fed_count;
    uint64_t rnd_received_hash;
    size_t   rnd_received_count;
    /* Deterministic reset-block arm only: how many times the retained closure
     * was refused before it landed. The arm requires at least two, so it is a
     * genuine ordered-drain test rather than a lucky empty queue. */
    int      reset_block_refusals;
} trace_summary_t;

static void hash_object(uint64_t *h, uint64_t group_id,
                         uint64_t object_id, size_t payload_len,
                         const uint8_t *payload)
{
    *h ^= group_id;    *h *= 0x100000001B3ULL;
    *h ^= object_id;   *h *= 0x100000001B3ULL;
    *h ^= payload_len; *h *= 0x100000001B3ULL;
    for (size_t i = 0; i < payload_len; i++) {
        *h ^= payload[i]; *h *= 0x100000001B3ULL;
    }
}

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r) {
    trace_summary_t *s = (trace_summary_t *)ctx;
    uint64_t h = s->hash;
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
    s->hash = h;
    s->count++;
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

/* -- Shadow model --------------------------------------------------- */

#define MAX_SUBS 4
#define MAX_SGS  4

typedef struct {
    bool active;
    bool accepted;
    moq_subscription_t server_handle;
    moq_subscription_t client_handle;
} shadow_sub_t;

typedef struct {
    bool active;
    moq_subgroup_handle_t handle;
    moq_subscription_t sub;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint64_t next_object_id;
} shadow_sg_t;

typedef struct {
    shadow_sub_t subs[MAX_SUBS];
    shadow_sg_t  sgs[MAX_SGS];
    bool         closed;
    uint64_t     next_group;
} shadow_state_t;

/* -- Optional STRICT AUDIT for the deterministic reset-block arm ---------- *
 * The random runner drains with audit == NULL and behaves exactly as before.
 * The arm passes an audit so the SAME production helper below is what it
 * exercises -- there is no second retry implementation. The audit declares, per
 * drain, the ONE object event that drain must contain, and names anything else:
 * a foreign event kind, a missing or extra event, a wrong group/object id, a
 * NULL or wrong-length payload, or wrong bytes. */
#define OBJ_AUDIT_MAX 4
typedef struct {
    size_t   drain_idx;      /* which drain pass must carry it (1-based) */
    uint64_t group_id;
    uint64_t object_id;
    uint8_t  payload[8];
    size_t   payload_len;
} obj_audit_expect_t;

typedef struct {
    /* The ORDERED objects this recovery must deliver, and the drain pass each
     * must appear in. Order and drain placement are both load-bearing: one
     * standing expectation would accept A and B arriving in the same pass. */
    obj_audit_expect_t exp[OBJ_AUDIT_MAX];
    size_t   exp_count;
    size_t   cursor;          /* next expectation to match */
    size_t   drain_index;     /* bumped once per drain pass by the helper */
    /* Observations. */
    size_t   objects_seen;
    size_t   foreign_events;
    int      failures;
} obj_drain_audit_t;

static void obj_audit_expect(obj_drain_audit_t *au, size_t drain_idx,
                             uint64_t group, uint64_t object,
                             const uint8_t *payload, size_t len) {
    if (au->exp_count >= OBJ_AUDIT_MAX) { au->failures++; return; }
    obj_audit_expect_t *e = &au->exp[au->exp_count++];
    e->drain_idx = drain_idx;
    e->group_id = group;
    e->object_id = object;
    e->payload_len = len <= sizeof(e->payload) ? len : sizeof(e->payload);
    memcpy(e->payload, payload, e->payload_len);
}

/* Compare one drained object event against the next standing expectation. */
static void obj_audit_object(obj_drain_audit_t *au,
                             const moq_object_received_event_t *ore) {
    if (au->cursor >= au->exp_count) {
        fprintf(stderr, "FAIL reset-block: extra object event in drain %zu -- "
                "group=%llu object=%llu\n", au->drain_index,
                (unsigned long long)ore->group_id,
                (unsigned long long)ore->object_id);
        au->failures++;
        return;
    }
    const obj_audit_expect_t *e = &au->exp[au->cursor++];
    au->objects_seen++;
    if (au->drain_index != e->drain_idx) {
        fprintf(stderr, "FAIL reset-block: object group=%llu object=%llu "
                "surfaced in drain %zu, expected drain %zu\n",
                (unsigned long long)ore->group_id,
                (unsigned long long)ore->object_id,
                au->drain_index, e->drain_idx);
        au->failures++;
    }
    if (ore->group_id != e->group_id || ore->object_id != e->object_id) {
        fprintf(stderr, "FAIL reset-block: object identity -- got "
                "group=%llu object=%llu want group=%llu object=%llu\n",
                (unsigned long long)ore->group_id,
                (unsigned long long)ore->object_id,
                (unsigned long long)e->group_id,
                (unsigned long long)e->object_id);
        au->failures++;
    }
    if (!ore->payload) {
        fprintf(stderr, "FAIL reset-block: object group=%llu object=%llu has a "
                "NULL payload, expected %zu bytes\n",
                (unsigned long long)ore->group_id,
                (unsigned long long)ore->object_id, e->payload_len);
        au->failures++;
        return;
    }
    size_t pl = moq_rcbuf_len(ore->payload);
    if (pl != e->payload_len) {
        fprintf(stderr, "FAIL reset-block: object group=%llu object=%llu "
                "payload length %zu, expected %zu\n",
                (unsigned long long)ore->group_id,
                (unsigned long long)ore->object_id, pl, e->payload_len);
        au->failures++;
        return;
    }
    if (memcmp(moq_rcbuf_data(ore->payload), e->payload, pl) != 0) {
        fprintf(stderr, "FAIL reset-block: object group=%llu object=%llu "
                "payload bytes differ\n",
                (unsigned long long)ore->group_id,
                (unsigned long long)ore->object_id);
        au->failures++;
    }
}

/* Drain the client's events, ACCOUNTING for every received object exactly as
 * OP_DRAIN_CLIENT_EVENTS does -- factored so the reset retry below drains
 * through the same hashing, since a drain that skipped it would leave the
 * received-object oracle silently blind to the objects it freed. With an audit
 * attached, every drained event is additionally classified. Returns the events
 * drained. */
static size_t obj_drain_client_accounted(moq_session_t *client,
                                         uint64_t *recv_hash,
                                         size_t *recv_count,
                                         obj_drain_audit_t *audit) {
    moq_event_t evts[8];
    size_t ne, total = 0;
    if (audit) audit->drain_index++;
    while ((ne = moq_session_poll_events(client, evts, 8)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                evts[i].u.object_received.payload && recv_hash && recv_count) {
                hash_object(recv_hash,
                    evts[i].u.object_received.group_id,
                    evts[i].u.object_received.object_id,
                    moq_rcbuf_len(evts[i].u.object_received.payload),
                    moq_rcbuf_data(evts[i].u.object_received.payload));
                (*recv_count)++;
            }
            if (audit) {
                if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    obj_audit_object(audit, &evts[i].u.object_received);
                } else {
                    /* Named where it happens: the aggregate count alone cannot
                     * say WHICH kind arrived, or in which drain. */
                    fprintf(stderr, "FAIL reset-block: unexpected event kind %u "
                            "drained in pass %zu\n",
                            (unsigned)evts[i].kind, audit->drain_index);
                    audit->foreign_events++;
                }
            }
            moq_event_cleanup(&evts[i]);
            total++;
        }
    return total;
}

/* Drive a WOULD_BLOCK on a peer RESET to completion. Per the contract on
 * moq_session_on_data_reset(), the reset owes a visible closure, that output can
 * be refused, and the recovery is to drain output and repeat the SAME call with
 * the SAME ref and code. A generic advancing drive is also intentionally
 * permitted to service the retained obligation (test_session_receive.c F1.2),
 * but repeating the reset is the public, operation-local recovery. Drains
 * through the accounted helper above; bounded so a genuine stall cannot spin;
 * hard errors are returned unchanged.
 *
 * out_refusals, when given, receives the number of times the closure was
 * refused -- the deterministic arm requires an exact count. audit, when given,
 * turns on the per-drain classification above; the random runner passes NULL
 * for both and is unaffected. */
static moq_result_t obj_resolve_reset_would_block(moq_simpair_t *sp,
        moq_session_t *client, moq_stream_ref_t ref, uint64_t code,
        moq_result_t rrc, uint64_t *recv_hash, size_t *recv_count,
        int *out_refusals, obj_drain_audit_t *audit) {
    int guard = 0;
    int refusals = (rrc == MOQ_ERR_WOULD_BLOCK) ? 1 : 0;
    while (rrc == MOQ_ERR_WOULD_BLOCK && guard++ < 256) {
        /* Under the strict audit, EVERY refused state is bracketed: while the
         * closure is still owed the stream must still be known to the session.
         * Checking only the first refusal leaves the helper's own later
         * refusals unobserved -- and the point is to distinguish PREMATURE
         * RETIREMENT from a missing terminal, not to infer one from the other.
         * The random runner (audit == NULL) takes none of this. */
        if (audit && !moq_session_has_transport_stream(client, ref)) {
            fprintf(stderr, "FAIL reset-block: stream retired at refusal %d "
                    "(before drain pass %zu) while the closure was still "
                    "owed\n", refusals, audit->drain_index + 1);
            audit->failures++;
        }
        obj_drain_client_accounted(client, recv_hash, recv_count, audit);
        rrc = moq_session_on_data_reset(client, ref, code,
                                       moq_simpair_now_us(sp));
        if (rrc == MOQ_ERR_WOULD_BLOCK) refusals++;
    }
    if (out_refusals) *out_refusals = refusals;
    return rrc;
}

static int shadow_find_accepted_sub(const shadow_state_t *st) {
    for (int i = 0; i < MAX_SUBS; i++)
        if (st->subs[i].active && st->subs[i].accepted) return i;
    return -1;
}

static int shadow_find_open_sg(const shadow_state_t *st) {
    for (int i = 0; i < MAX_SGS; i++)
        if (st->sgs[i].active) return i;
    return -1;
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP,
    OP_SUBSCRIBE,
    OP_ACCEPT_SUBSCRIBE,
    OP_OPEN_SUBGROUP,
    OP_WRITE_OBJECT,
    OP_CLOSE_SUBGROUP,
    OP_RESET_SUBGROUP,
    OP_DRAIN_CLIENT_EVENTS,
    OP_DRAIN_SERVER_EVENTS,
    OP_ADVANCE_TIME,
    OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *n[] = {
        "PUMP","SUBSCRIBE","ACCEPT_SUB","OPEN_SG","WRITE_OBJ",
        "CLOSE_SG","RESET_SG","DRAIN_C","DRAIN_S","ADV_TIME",
    };
    return op < OP_COUNT ? n[op] : "?";
}

/* -- Log ------------------------------------------------------------ */

#define MAX_LOG 64
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

/* -- Chunked data pump ----------------------------------------------
 *
 * Instead of using SimPair's fixed header+payload routing, we poll
 * actions manually from the server and feed SEND_DATA bytes to the
 * client in randomized chunk sizes. Control actions still route
 * normally. This stresses the receive parser's incremental buffering.
 */

static int pump_with_chunking(moq_simpair_t *sp, rng_t *chunk_rng,
                               uint64_t *fed_hash, size_t *fed_count,
                               uint64_t *recv_hash, size_t *recv_count) {
    moq_session_t *server = moq_simpair_server(sp);
    moq_session_t *client = moq_simpair_client(sp);

    for (int rounds = 0; rounds < 4; rounds++) {
        moq_action_t acts[16];
        size_t na = moq_session_poll_actions(server, acts, 16);
        if (na == 0) {
            /* Also pump client→server control. */
            moq_action_t cacts[16];
            size_t cn = moq_session_poll_actions(client, cacts, 16);
            for (size_t i = 0; i < cn; i++) {
                if (cacts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                    moq_result_t crc = moq_session_on_control_bytes(
                        server, cacts[i].u.send_control.data,
                        cacts[i].u.send_control.len,
                        moq_simpair_now_us(sp));
                    if (crc < 0) {
                        moq_action_cleanup(&cacts[i]);
                        for (size_t j = i + 1; j < cn; j++)
                            moq_action_cleanup(&cacts[j]);
                        return -1;
                    }
                }
                moq_action_cleanup(&cacts[i]);
            }
            if (cn == 0) break;
            continue;
        }

        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_result_t crc = moq_session_on_control_bytes(
                    client, acts[i].u.send_control.data,
                    acts[i].u.send_control.len,
                    moq_simpair_now_us(sp));
                if (crc < 0) {
                    moq_action_cleanup(&acts[i]);
                    for (size_t j = i + 1; j < na; j++)
                        moq_action_cleanup(&acts[j]);
                    return -1;
                }
            } else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                /* Combine header + payload into one buffer. */
                uint8_t combined[4096];
                size_t clen = 0;
                uint8_t hlen = acts[i].u.send_data.header_len;
                if (hlen > 0) {
                    memcpy(combined, acts[i].u.send_data.header, hlen);
                    clen = hlen;
                }
                if (acts[i].u.send_data.payload) {
                    size_t plen = moq_rcbuf_len(acts[i].u.send_data.payload);
                    const uint8_t *pdata = moq_rcbuf_data(acts[i].u.send_data.payload);
                    if (clen + plen <= sizeof(combined)) {
                        memcpy(combined + clen, pdata, plen);
                        clen += plen;
                    }
                }
                bool fin = acts[i].u.send_data.fin;

                /* Feed in random chunks, checking return codes. */
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    acts[i].u.send_data.stream_ref._v + 10000);
                size_t off = 0;
                while (off < clen) {
                    size_t chunk = (rng_next(chunk_rng) % 8) + 1;
                    if (chunk > clen - off) chunk = clen - off;
                    bool chunk_fin = fin && (off + chunk >= clen);
                    moq_result_t drc = moq_session_on_data_bytes(
                        client, ref, combined + off, chunk, chunk_fin,
                        moq_simpair_now_us(sp));
                    if (drc < 0) {
                        moq_action_cleanup(&acts[i]);
                        for (size_t j = i + 1; j < na; j++)
                            moq_action_cleanup(&acts[j]);
                        return -1;
                    }
                    off += chunk;
                }
                if (clen == 0 && fin) {
                    moq_result_t frc = moq_session_on_data_bytes(
                        client, ref, NULL, 0, true,
                        moq_simpair_now_us(sp));
                    if (frc < 0) {
                        moq_action_cleanup(&acts[i]);
                        for (size_t j = i + 1; j < na; j++)
                            moq_action_cleanup(&acts[j]);
                        return -1;
                    }
                }
                /* If payload was present and all chunks fed without
                 * error, hash payload into fed oracle. group/object
                 * IDs are 0 because we can't decode them from the
                 * raw stream — this tracks payload-fed determinism,
                 * not object identity. No-phantom check is count-based. */
                if (off == clen && acts[i].u.send_data.payload &&
                    moq_session_state(client) != MOQ_SESS_CLOSED &&
                    fed_hash && fed_count) {
                    const uint8_t *pd = moq_rcbuf_data(
                        acts[i].u.send_data.payload);
                    size_t pl = moq_rcbuf_len(
                        acts[i].u.send_data.payload);
                    hash_object(fed_hash, 0, 0, pl, pd);
                    (*fed_count)++;
                }
            } else if (acts[i].kind == MOQ_ACTION_RESET_DATA) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    acts[i].u.reset_data.stream_ref._v + 10000);
                moq_result_t rrc = moq_session_on_data_reset(client, ref,
                    acts[i].u.reset_data.error_code,
                    moq_simpair_now_us(sp));
                /* A refused reset closure is backpressure, not a data
                 * error: see moq_session_on_data_reset()'s contract and
                 * obj_resolve_reset_would_block(). */
                rrc = obj_resolve_reset_would_block(
                    sp, client, ref, acts[i].u.reset_data.error_code, rrc,
                    recv_hash, recv_count, NULL, NULL);
                if (rrc < 0) {
                    moq_action_cleanup(&acts[i]);
                    for (size_t j = i + 1; j < na; j++)
                        moq_action_cleanup(&acts[j]);
                    return -1;
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }
    return 0;
}

/* -- Execute one step ----------------------------------------------- */

static int execute_step(moq_simpair_t *sp, shadow_state_t *st,
                         rng_t *rng, rng_t *chunk_rng, op_log_t *log,
                         trace_summary_t *ts, const moq_alloc_t *alloc)
{
    if (st->closed) return 0;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log, op, 0);
        if (pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count) < 0) return -1;
        break;

    case OP_SUBSCRIBE: {
        char name[16];
        snprintf(name, sizeof(name), "t%llu",
                 (unsigned long long)(rng_next(rng) % 1000));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns;
        cfg.track_name = moq_bytes_cstr(name);

        moq_subscription_t h;
        moq_result_t rc = moq_session_subscribe(client, &cfg,
            moq_simpair_now_us(sp), &h);
        log_op(log, op, rc >= 0 ? 1 : 0);

        if (rc == MOQ_OK) {
            pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count);

            moq_event_t evts[4];
            size_t ne = moq_session_poll_events(server, evts, 4);
            for (size_t j = 0; j < ne; j++) {
                if (evts[j].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    for (int k = 0; k < MAX_SUBS; k++) {
                        if (!st->subs[k].active) {
                            st->subs[k].active = true;
                            st->subs[k].accepted = false;
                            st->subs[k].server_handle = evts[j].u.subscribe_request.sub;
                            st->subs[k].client_handle = h;
                            break;
                        }
                    }
                }
                moq_event_cleanup(&evts[j]);
            }
        }
        break;
    }

    case OP_ACCEPT_SUBSCRIBE: {
        int idx = -1;
        for (int i = 0; i < MAX_SUBS; i++)
            if (st->subs[i].active && !st->subs[i].accepted) { idx = i; break; }
        if (idx < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count); break; }

        log_op(log, op, (uint64_t)idx);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(server,
            st->subs[idx].server_handle, &acc, moq_simpair_now_us(sp));
        if (rc == MOQ_OK) {
            st->subs[idx].accepted = true;
            pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count);
            moq_event_t evts[4];
            size_t ne = moq_session_poll_events(client, evts, 4);
            for (size_t j = 0; j < ne; j++)
                moq_event_cleanup(&evts[j]);
        }
        break;
    }

    case OP_OPEN_SUBGROUP: {
        int sub_idx = shadow_find_accepted_sub(st);
        if (sub_idx < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count); break; }

        int sg_slot = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (!st->sgs[i].active) { sg_slot = i; break; }
        if (sg_slot < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count); break; }

        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = st->next_group;
        cfg.subgroup_id = 0;
        cfg.publisher_priority = 128;

        moq_subgroup_handle_t h;
        moq_result_t rc = moq_session_open_subgroup(server,
            st->subs[sub_idx].server_handle, &cfg,
            moq_simpair_now_us(sp), &h);
        log_op(log, op, rc >= 0 ? st->next_group : UINT64_MAX);

        if (rc == MOQ_OK) {
            st->sgs[sg_slot].active = true;
            st->sgs[sg_slot].handle = h;
            st->sgs[sg_slot].sub = st->subs[sub_idx].server_handle;
            st->sgs[sg_slot].group_id = st->next_group;
            st->sgs[sg_slot].subgroup_id = 0;
            st->sgs[sg_slot].next_object_id = 0;
            st->next_group++;
        }
        break;
    }

    case OP_WRITE_OBJECT: {
        int sg_idx = shadow_find_open_sg(st);
        if (sg_idx < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count); break; }

        uint64_t obj_id = st->sgs[sg_idx].next_object_id;
        uint64_t r = rng_next(rng);
        bool gap = (r % 8 == 0) && obj_id > 0;
        if (gap) obj_id += (r % 3) + 1;

        size_t payload_len = rng_next(rng) % 64;
        if (rng_next(rng) % 10 == 0) payload_len = 0;

        uint8_t payload_data[64];
        for (size_t j = 0; j < payload_len; j++)
            payload_data[j] = (uint8_t)(rng_next(rng) & 0xFF);

        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(alloc, payload_data, payload_len, &payload);
        if (!payload) { log_op(log, OP_PUMP, 0); break; }

        moq_result_t rc = moq_session_write_object(server,
            st->sgs[sg_idx].handle, obj_id, payload,
            moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);

        log_op(log, op, rc >= 0 ? obj_id : UINT64_MAX);

        if (rc == MOQ_OK)
            st->sgs[sg_idx].next_object_id = obj_id + 1;
        break;
    }

    case OP_CLOSE_SUBGROUP: {
        int sg_idx = shadow_find_open_sg(st);
        if (sg_idx < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count); break; }

        log_op(log, op, st->sgs[sg_idx].group_id);
        moq_result_t rc = moq_session_close_subgroup(server,
            st->sgs[sg_idx].handle, moq_simpair_now_us(sp));
        if (rc == MOQ_OK)
            st->sgs[sg_idx].active = false;
        break;
    }

    case OP_RESET_SUBGROUP: {
        int sg_idx = shadow_find_open_sg(st);
        if (sg_idx < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count); break; }

        log_op(log, op, st->sgs[sg_idx].group_id);
        moq_result_t rc = moq_session_reset_subgroup(server,
            st->sgs[sg_idx].handle, 0x1, moq_simpair_now_us(sp));
        if (rc == MOQ_OK)
            st->sgs[sg_idx].active = false;
        break;
    }

    case OP_DRAIN_CLIENT_EVENTS: {
        log_op(log, op, 0);
        (void)obj_drain_client_accounted(client, &ts->rnd_received_hash,
                                         &ts->rnd_received_count, NULL);
        break;
    }

    case OP_DRAIN_SERVER_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(server, evts, 8);
        for (size_t i = 0; i < ne; i++)
            moq_event_cleanup(&evts[i]);
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(log, op, delta);
        moq_simpair_advance_to(sp, moq_simpair_now_us(sp) + delta);
        break;
    }

    default:
        log_op(log, OP_PUMP, 0);
        if (pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count,
                                &ts->rnd_received_hash, &ts->rnd_received_count) < 0) return -1;
        break;
    }
    return 0;
}

/* -- Exact delivery prelude ----------------------------------------- */

static int run_exact_prelude(uint64_t seed, const moq_alloc_t *alloc,
                              trace_summary_t *ts)
{
    int rc_out = -1;

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed ^ 0x50524C44ULL;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 10;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;

    if (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    if (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

    /* Subscribe + accept. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("pre");
    moq_subscription_t csub;
    if (moq_session_subscribe(c, &sub_cfg, 1000, &csub) != MOQ_OK)
        goto cleanup;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(sv, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_event_cleanup(&ev); goto cleanup;
    }
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, ssub, &acc, 1000) != MOQ_OK)
        goto cleanup;
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    if (moq_session_poll_events(c, &ev, 1) != 1) goto cleanup;
    if (ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
        moq_event_cleanup(&ev); goto cleanup;
    }
    moq_event_cleanup(&ev);

    /* Open subgroup + pump header. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 1000, &sg) != MOQ_OK)
        goto cleanup;
    pump_with_chunking(sp, &(rng_t){ seed ^ 0x1111 }, NULL, NULL, NULL, NULL);

    /* Write objects: non-empty, zero-length, ID gap. */
    static const struct { uint64_t id; const char *d; size_t l; } objs[] = {
        { 0, "hello", 5 },
        { 1, "",      0 },
        { 5, "gap!",  4 },
    };

    for (size_t oi = 0; oi < 3; oi++) {
        moq_rcbuf_t *p = NULL;
        if (moq_rcbuf_create(alloc,
                objs[oi].l > 0 ? (const uint8_t *)objs[oi].d : NULL,
                objs[oi].l, &p) != MOQ_OK) goto cleanup;
        if (moq_session_write_object(sv, sg, objs[oi].id, p, 1000)
            != MOQ_OK) {
            moq_rcbuf_decref(p);
            goto cleanup;
        }
        moq_rcbuf_decref(p);

        /* Hash expected from known object list. */
        hash_object(&ts->pre_expected_hash, 0, objs[oi].id,
                     objs[oi].l, (const uint8_t *)objs[oi].d);
        ts->pre_expected_count++;

        /* Pump + drain; assert exactly one OBJECT_RECEIVED. */
        rng_t cr = { seed ^ (0x2222 + oi) };
        if (pump_with_chunking(sp, &cr, NULL, NULL, NULL, NULL) < 0) goto cleanup;

        bool got_obj = false;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const uint8_t *pd = ev.u.object_received.payload ?
                    moq_rcbuf_data(ev.u.object_received.payload) : NULL;
                size_t pl = ev.u.object_received.payload ?
                    moq_rcbuf_len(ev.u.object_received.payload) : 0;
                if (ev.u.object_received.group_id != 0 ||
                    ev.u.object_received.object_id != objs[oi].id ||
                    pl != objs[oi].l ||
                    (pl > 0 && memcmp(pd, objs[oi].d, pl) != 0)) {
                    moq_event_cleanup(&ev); goto cleanup;
                }
                hash_object(&ts->pre_received_hash,
                    ev.u.object_received.group_id,
                    ev.u.object_received.object_id, pl, pd);
                ts->pre_received_count++;
                got_obj = true;
            }
            moq_event_cleanup(&ev);
        }
        if (!got_obj) goto cleanup;
    }

    moq_session_close_subgroup(sv, sg, 1000);
    pump_with_chunking(sp, &(rng_t){ seed ^ 0x3333 }, NULL, NULL, NULL, NULL);
    rc_out = 0;

cleanup:
    { moq_event_t d[16]; size_t ne;
      while ((ne = moq_session_poll_events(c, d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
      while ((ne = moq_session_poll_events(sv, d, 16)) > 0)
          for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]);
      moq_action_t a[16]; size_t na;
      while ((na = moq_session_poll_actions(sv, a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
      while ((na = moq_session_poll_actions(c, a, 16)) > 0)
          for (size_t i = 0; i < na; i++) moq_action_cleanup(&a[i]);
    }
    moq_simpair_destroy(sp);
    return rc_out;
}

/* -- Run one scenario ----------------------------------------------- */

/* ===================================================================== *
 *  Deterministic arm: make the retained reset closure BLOCK on purpose.
 *
 *  The random ops do not reliably reach this state, and a sampled
 *  non-occurrence is not an invariant -- so the contract on
 *  moq_session_on_data_reset() is exercised here by construction instead.
 *
 *  Shape, on a ONE-slot client event queue:
 *    - object A is fed whole      -> OBJECT_RECEIVED(A) queued, slot FULL;
 *    - object B is fed whole      -> cannot queue, owed ahead of the reset;
 *    - the peer resets the stream -> B must surface first: refusal #1;
 *      after draining A, B fills the slot again:            refusal #2;
 *      after draining B, the SUBGROUP_RESET finally lands.
 *  Two refused outputs, so recovery needs ordered draining rather than a
 *  lucky empty queue, and a one-iteration cap cannot pass.
 *
 *  Returns the number of failures.
 * ===================================================================== */
static int run_reset_block_arm(const moq_alloc_t *alloc,
                               int *out_refusals)
{
    int failures = 0;
    trace_summary_t ts;
    memset(&ts, 0, sizeof(ts));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 0x5E7;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 10;
    cfg.max_events = 1;                 /* the smallest queue: one slot */

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) {
        fprintf(stderr, "FAIL reset-block: simpair create\n");
        return 1;
    }
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *c = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_event_t ev;

    /* Drain BOTH sides to EMPTY first -- with one slot, a single leftover
     * handshake event is enough to refuse the subscribe delivery, and then
     * nothing downstream happens at all. Then let the pair settle again. */
    while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

    /* Establish a real subscription. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("blk");
    moq_subscription_t csub;
    if (moq_session_subscribe(c, &sub_cfg, 1000, &csub) != MOQ_OK) {
        fprintf(stderr, "FAIL reset-block: subscribe\n");
        failures++; goto done;
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_subscription_t ssub = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
            if (moq_session_accept_subscribe(sv, ssub, &acc, 1000) != MOQ_OK) {
                fprintf(stderr, "FAIL reset-block: accept\n");
                failures++; goto done;
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            /* Publish two complete objects on ONE bound subgroup. */
            moq_subgroup_cfg_t sg_cfg;
            moq_subgroup_cfg_init(&sg_cfg);
            sg_cfg.group_id = 7;
            sg_cfg.subgroup_id = 3;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 1000, &sg)
                != MOQ_OK) {
                fprintf(stderr, "FAIL reset-block: open subgroup\n");
                failures++; goto done;
            }
            for (int o = 0; o < 2; o++) {
                moq_rcbuf_t *p = NULL;
                const uint8_t body[2] = { (uint8_t)('A' + o), (uint8_t)o };
                if (moq_rcbuf_create(alloc, body, sizeof(body), &p) != MOQ_OK) {
                    fprintf(stderr, "FAIL reset-block: payload\n");
                    failures++; goto done;
                }
                moq_result_t wrc = moq_session_write_object(sv, sg,
                                                            (uint64_t)o, p, 1000);
                moq_rcbuf_decref(p);
                if (wrc != MOQ_OK) {
                    fprintf(stderr, "FAIL reset-block: write object %d\n", o);
                    failures++; goto done;
                }
            }
            break;
        }
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);

    /* Classify the server's COMPLETE action queue: exactly one subgroup-header
     * SEND_DATA (no payload) and exactly two payload-bearing object SEND_DATA
     * records, all on the same source stream, no duplicates, nothing else, and
     * no FIN on any of them (the subgroup is reset, never closed). Every
     * pointer and length is checked before it is copied: malformed or changed
     * product output must produce a diagnostic, not an out-of-bounds write or a
     * fixture that authorizes itself. */
    {
        uint8_t hdr[64]; size_t hdr_len = 0; int hdr_seen = 0;
        uint8_t obj[2][64]; size_t obj_len[2] = { 0, 0 }; int obj_seen = 0;
        bool have_src = false; uint64_t src_ref = 0;
        int other_actions = 0, fin_seen = 0, malformed = 0;
        moq_action_t acts[16]; size_t na;

        while ((na = moq_session_poll_actions(sv, acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) {
                if (acts[i].kind != MOQ_ACTION_SEND_DATA) {
                    other_actions++;
                    moq_action_cleanup(&acts[i]);
                    continue;
                }
                const moq_send_data_action_t *sd = &acts[i].u.send_data;
                size_t hl = sd->header_len;
                if (!have_src) { have_src = true; src_ref = sd->stream_ref._v; }
                else if (sd->stream_ref._v != src_ref) other_actions++;
                if (sd->fin) fin_seen++;

                if (!sd->payload) {
                    /* the stream-opening subgroup header */
                    if (hdr_seen++ > 0) { malformed++; }
                    else if (hl == 0 || hl > sizeof(hdr)) { malformed++; }
                    else { memcpy(hdr, sd->header, hl); hdr_len = hl; }
                } else {
                    size_t pl = moq_rcbuf_len(sd->payload);
                    const uint8_t *pd = moq_rcbuf_data(sd->payload);
                    if (obj_seen >= 2) { malformed++; }
                    else if (hl == 0 || (pl > 0 && !pd) ||
                             hl > sizeof(obj[0]) ||
                             pl > sizeof(obj[0]) - hl) { malformed++; }
                    else {
                        memcpy(obj[obj_seen], sd->header, hl);
                        if (pl) memcpy(obj[obj_seen] + hl, pd, pl);
                        obj_len[obj_seen] = hl + pl;
                        obj_seen++;
                    }
                }
                moq_action_cleanup(&acts[i]);
            }

        if (hdr_seen != 1 || obj_seen != 2 || hdr_len == 0 ||
            other_actions != 0 || fin_seen != 0 || malformed != 0) {
            fprintf(stderr, "FAIL reset-block: action shape -- headers=%d "
                    "objects=%d header_len=%zu other=%d fin=%d malformed=%d\n",
                    hdr_seen, obj_seen, hdr_len, other_actions, fin_seen,
                    malformed);
            failures++; goto done;
        }

        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5E7);
        const uint64_t code = 0xABCDEF;
        const uint8_t body_a[2] = { 'A', 0 };
        const uint8_t body_b[2] = { 'B', 1 };

        /* Header + object A, with the combined length checked before copying. */
        uint8_t first[128]; size_t fl = 0;
        if (hdr_len > sizeof(first) || obj_len[0] > sizeof(first) - hdr_len) {
            fprintf(stderr, "FAIL reset-block: header+A does not fit "
                    "(%zu + %zu)\n", hdr_len, obj_len[0]);
            failures++; goto done;
        }
        memcpy(first, hdr, hdr_len); fl += hdr_len;
        memcpy(first + fl, obj[0], obj_len[0]); fl += obj_len[0];

        if (moq_session_on_data_bytes(c, ref, first, fl, false, 1000)
            != MOQ_OK) {
            fprintf(stderr, "FAIL reset-block: feeding header+A\n");
            failures++; goto done;
        }
        /* Object B: owed, the only slot is taken by A. */
        moq_result_t brc = moq_session_on_data_bytes(c, ref, obj[1],
                                                     obj_len[1], false, 1000);
        if (brc != MOQ_ERR_WOULD_BLOCK) {
            fprintf(stderr, "FAIL reset-block: B should be owed, rc=%d\n",
                    (int)brc);
            failures++; goto done;
        }

        /* The stream is live and KNOWN to the session throughout the blocked
         * phases -- asserted directly rather than inferred. */
        if (!moq_session_has_transport_stream(c, ref)) {
            fprintf(stderr, "FAIL reset-block: stream absent before reset\n");
            failures++; goto done;
        }

        /* First refusal: the closure waits behind B. */
        moq_result_t rrc = moq_session_on_data_reset(c, ref, code, 1000);
        if (rrc != MOQ_ERR_WOULD_BLOCK) {
            fprintf(stderr, "FAIL reset-block: reset should be refused, "
                    "rc=%d\n", (int)rrc);
            failures++; goto done;
        }
        if (!moq_session_has_transport_stream(c, ref)) {
            fprintf(stderr, "FAIL reset-block: stream retired while the "
                    "closure was still owed\n");
            failures++;
        }

        /* Recover through the RUNNER'S OWN helper, with the strict audit on.
         * Drain 1 must contain exactly A, drain 2 exactly B. */
        obj_drain_audit_t audit;
        memset(&audit, 0, sizeof(audit));
        obj_audit_expect(&audit, 1, 7, 0, body_a, sizeof(body_a));
        obj_audit_expect(&audit, 2, 7, 1, body_b, sizeof(body_b));

        int refusals = 0;
        rrc = obj_resolve_reset_would_block(sp, c, ref, code, rrc,
                                            &ts.rnd_received_hash,
                                            &ts.rnd_received_count,
                                            &refusals, &audit);
        if (rrc != MOQ_OK) {
            fprintf(stderr, "FAIL reset-block: reset never completed, rc=%d\n",
                    (int)rrc);
            failures++; goto done;
        }
        /* EXACTLY two refusals: one for B, one for the closure. A third is a
         * topology change, not something to normalize. */
        if (refusals != 2) {
            fprintf(stderr, "FAIL reset-block: expected exactly 2 refusals, "
                    "got %d\n", refusals);
            failures++;
        }
        if (out_refusals) *out_refusals = refusals;
        ts.reset_block_refusals = refusals;

        /* Retired: the ref is gone the moment the closure lands. */
        if (moq_session_has_transport_stream(c, ref)) {
            fprintf(stderr, "FAIL reset-block: stream still present after the "
                    "closure landed\n");
            failures++;
        }

        /* The audit's own verdict: exactly the two declared objects, nothing
         * foreign drained, no named mismatch, and no expectation left unmet. */
        if (audit.failures != 0 || audit.objects_seen != 2 ||
            audit.foreign_events != 0 || audit.cursor != audit.exp_count) {
            fprintf(stderr, "FAIL reset-block: audit -- failures=%d objects=%zu "
                    "foreign=%zu matched=%zu/%zu\n",
                    audit.failures, audit.objects_seen, audit.foreign_events,
                    audit.cursor, audit.exp_count);
            failures++;
        }

        /* The factored accounting must agree with an INDEPENDENTLY built hash
         * and count over the same two objects in the same order. */
        {
            uint64_t want_hash = 0;
            size_t   want_count = 0;
            hash_object(&want_hash, 7, 0, sizeof(body_a), body_a);
            want_count++;
            hash_object(&want_hash, 7, 1, sizeof(body_b), body_b);
            want_count++;
            if (ts.rnd_received_count != want_count ||
                ts.rnd_received_hash != want_hash) {
                fprintf(stderr, "FAIL reset-block: accounted objects "
                        "%zu/0x%llx, expected %zu/0x%llx\n",
                        ts.rnd_received_count,
                        (unsigned long long)ts.rnd_received_hash,
                        want_count, (unsigned long long)want_hash);
                failures++;
            }
        }

        /* Exactly one SUBGROUP_RESET, this subgroup, the ORIGINAL code. */
        int sgresets = 0, others = 0;
        while (moq_session_poll_events(c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
                sgresets++;
                if (ev.u.subgroup_reset.group_id != 7 ||
                    ev.u.subgroup_reset.subgroup_id != 3 ||
                    ev.u.subgroup_reset.error_code != code) {
                    fprintf(stderr, "FAIL reset-block: identity/code -- "
                            "group=%llu subgroup=%llu code=0x%llx\n",
                            (unsigned long long)ev.u.subgroup_reset.group_id,
                            (unsigned long long)ev.u.subgroup_reset.subgroup_id,
                            (unsigned long long)ev.u.subgroup_reset.error_code);
                    failures++;
                }
            } else {
                others++;
            }
            moq_event_cleanup(&ev);
        }
        if (sgresets != 1 || others != 0) {
            fprintf(stderr, "FAIL reset-block: expected exactly 1 "
                    "SUBGROUP_RESET and nothing else, got %d/%d\n",
                    sgresets, others);
            failures++;
        }
        /* Still retired after the terminal is consumed. */
        if (moq_session_has_transport_stream(c, ref)) {
            fprintf(stderr, "FAIL reset-block: stream reappeared after the "
                    "terminal was consumed\n");
            failures++;
        }

        /* A repeat reset is the documented harmless no-op: no duplicate
         * terminal, and the ref stays absent. */
        if (moq_session_on_data_reset(c, ref, code, 1000) != MOQ_OK) {
            fprintf(stderr, "FAIL reset-block: repeat reset not a no-op\n");
            failures++;
        }
        if (moq_session_poll_events(c, &ev, 1) != 0) {
            fprintf(stderr, "FAIL reset-block: duplicate terminal emitted\n");
            moq_event_cleanup(&ev);
            failures++;
        }
        if (moq_session_has_transport_stream(c, ref)) {
            fprintf(stderr, "FAIL reset-block: repeat reset resurrected the "
                    "stream\n");
            failures++;
        }
        if (moq_session_state(c) != MOQ_SESS_ESTABLISHED ||
            moq_session_state(sv) != MOQ_SESS_ESTABLISHED) {
            fprintf(stderr, "FAIL reset-block: session did not survive\n");
            failures++;
        }
    }

done:
    while (moq_session_poll_events(c, &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(sv, &ev, 1) == 1) moq_event_cleanup(&ev);
    moq_simpair_destroy(sp);
    return failures;
}

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         trace_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->hash = 0xCBF29CE484222325ULL;
    summary->pre_expected_hash = 0xCBF29CE484222325ULL;
    summary->pre_received_hash = 0xCBF29CE484222325ULL;
    summary->rnd_fed_hash = 0xCBF29CE484222325ULL;
    summary->rnd_received_hash = 0xCBF29CE484222325ULL;

    if (run_exact_prelude(seed, alloc, summary) < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_object\n",
                (unsigned long long)seed, max_steps);
        return 1;
    }

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = summary;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return -1;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    {
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    shadow_state_t st;
    memset(&st, 0, sizeof(st));

    rng_t rng = { seed };
    rng_t chunk_rng = { seed ^ 0xDEADBEEFCAFE1234ULL };
    op_log_t log = {0};
    int failures = 0;

    for (int step = 0; step < max_steps; step++) {
        int src = execute_step(sp, &st, &rng, &chunk_rng, &log,
                               summary, alloc);
        if (src < 0) {
            fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                    "data input error\n",
                    (unsigned long long)seed, run_id, step);
            dump_log(seed, run_id, &log);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
            st.closed = true;
            break;
        }

        if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
            moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED) {
            if (!st.closed) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "unexpected close\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_object\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
            st.closed = true;
            break;
        }
    }

    /* Final pump to flush any remaining data through the parser.
     * Drain client events between rounds so event queue doesn't block. */
    if (!st.closed) {
        for (int f = 0; f < 16; f++) {
            int prc = pump_with_chunking(sp, &chunk_rng,
                                &summary->rnd_fed_hash,
                                &summary->rnd_fed_count,
                                &summary->rnd_received_hash,
                                &summary->rnd_received_count);
            if (prc < 0) { failures++; break; }
            moq_event_t evts[16]; size_t ne;
            while ((ne = moq_session_poll_events(
                    moq_simpair_client(sp), evts, 16)) > 0)
                for (size_t i = 0; i < ne; i++) {
                    if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                        evts[i].u.object_received.payload) {
                        hash_object(&summary->rnd_received_hash,
                            evts[i].u.object_received.group_id,
                            evts[i].u.object_received.object_id,
                            moq_rcbuf_len(evts[i].u.object_received.payload),
                            moq_rcbuf_data(evts[i].u.object_received.payload));
                        summary->rnd_received_count++;
                    }
                    moq_event_cleanup(&evts[i]);
                }
        }
    }

    /* Drain all remaining owned events and actions, capturing objects. */
    {
        moq_event_t drain[16];
        size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (drain[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                    drain[i].u.object_received.payload) {
                    hash_object(&summary->rnd_received_hash,
                        drain[i].u.object_received.group_id,
                        drain[i].u.object_received.object_id,
                        moq_rcbuf_len(drain[i].u.object_received.payload),
                        moq_rcbuf_data(drain[i].u.object_received.payload));
                    summary->rnd_received_count++;
                }
                moq_event_cleanup(&drain[i]);
            }
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), drain, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        moq_action_t acts[16];
        size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    moq_simpair_destroy(sp);
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

    size_t total_pre = 0, total_rnd_fed = 0, total_rnd_rcv = 0;

    /* Deterministic arm first: it does not depend on the seed range, and a
     * broken retained-reset recovery should be reported before 1000 seeds of
     * random ops scroll past. Run twice, so its own behaviour is pinned as
     * deterministic like every other summary here. */
    int arm_refusals[2] = { 0, 0 };
    for (int run = 0; run < 2; run++) {
        scen_alloc_t as = {0};
        moq_alloc_t alloc = { &as, sa_alloc, sa_realloc, sa_free };
        failures += run_reset_block_arm(&alloc, &arm_refusals[run]);
        if (as.balance != 0) {
            fprintf(stderr, "FAIL reset-block run=%d: alloc balance=%lld\n",
                    run, (long long)as.balance);
            failures++;
        }
    }
    if (arm_refusals[0] != arm_refusals[1]) {
        fprintf(stderr, "FAIL reset-block: refusal count not deterministic "
                "(%d vs %d)\n", arm_refusals[0], arm_refusals[1]);
        failures++;
    }

    for (uint64_t seed = seed_start; seed < seed_start + num_seeds; seed++) {
        trace_summary_t sums[2];

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
                        "./build/tests/test_scenario_object\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        /* Determinism: all fields must match across runs. */
        if (sums[0].hash != sums[1].hash ||
            sums[0].count != sums[1].count ||
            sums[0].pre_expected_count != sums[1].pre_expected_count ||
            sums[0].pre_received_count != sums[1].pre_received_count ||
            sums[0].pre_expected_hash != sums[1].pre_expected_hash ||
            sums[0].pre_received_hash != sums[1].pre_received_hash ||
            sums[0].rnd_fed_count != sums[1].rnd_fed_count ||
            sums[0].rnd_received_count != sums[1].rnd_received_count ||
            sums[0].rnd_fed_hash != sums[1].rnd_fed_hash ||
            sums[0].rnd_received_hash != sums[1].rnd_received_hash) {
            fprintf(stderr, "FAIL seed=0x%llx: determinism mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        /* Prelude: exact delivery oracle. */
        if (sums[0].pre_expected_count != sums[0].pre_received_count ||
            sums[0].pre_expected_hash != sums[0].pre_received_hash) {
            fprintf(stderr, "FAIL seed=0x%llx: prelude oracle "
                    "exp=%zu/%llx rcv=%zu/%llx\n",
                    (unsigned long long)seed,
                    sums[0].pre_expected_count,
                    (unsigned long long)sums[0].pre_expected_hash,
                    sums[0].pre_received_count,
                    (unsigned long long)sums[0].pre_received_hash);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        /* Random: no phantom objects. */
        if (sums[0].rnd_received_count > sums[0].rnd_fed_count) {
            fprintf(stderr, "FAIL seed=0x%llx: phantom objects "
                    "fed=%zu received=%zu\n",
                    (unsigned long long)seed,
                    sums[0].rnd_fed_count,
                    sums[0].rnd_received_count);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_object\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        total_pre += sums[0].pre_received_count;
        total_rnd_fed += sums[0].rnd_fed_count;
        total_rnd_rcv += sums[0].rnd_received_count;
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_object "
                "(%llu seeds, prelude=%zu random=%zu/%zu)\n",
                (unsigned long long)num_seeds,
                total_pre, total_rnd_fed, total_rnd_rcv);
    else
        fprintf(stderr, "FAIL: test_scenario_object (%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
