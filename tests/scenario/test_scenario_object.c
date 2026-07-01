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
                               uint64_t *fed_hash, size_t *fed_count) {
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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count);

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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count); break; }

        log_op(log, op, (uint64_t)idx);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(server,
            st->subs[idx].server_handle, &acc, moq_simpair_now_us(sp));
        if (rc == MOQ_OK) {
            st->subs[idx].accepted = true;
            pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count);
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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count); break; }

        int sg_slot = -1;
        for (int i = 0; i < MAX_SGS; i++)
            if (!st->sgs[i].active) { sg_slot = i; break; }
        if (sg_slot < 0) { log_op(log, OP_PUMP, 0); pump_with_chunking(sp, chunk_rng,
                                &ts->rnd_fed_hash, &ts->rnd_fed_count); break; }

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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count); break; }

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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count); break; }

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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count); break; }

        log_op(log, op, st->sgs[sg_idx].group_id);
        moq_result_t rc = moq_session_reset_subgroup(server,
            st->sgs[sg_idx].handle, 0x1, moq_simpair_now_us(sp));
        if (rc == MOQ_OK)
            st->sgs[sg_idx].active = false;
        break;
    }

    case OP_DRAIN_CLIENT_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[8];
        size_t ne;
        while ((ne = moq_session_poll_events(client, evts, 8)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                    evts[i].u.object_received.payload) {
                    hash_object(&ts->rnd_received_hash,
                        evts[i].u.object_received.group_id,
                        evts[i].u.object_received.object_id,
                        moq_rcbuf_len(evts[i].u.object_received.payload),
                        moq_rcbuf_data(evts[i].u.object_received.payload));
                    ts->rnd_received_count++;
                }
                moq_event_cleanup(&evts[i]);
            }
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
                                &ts->rnd_fed_hash, &ts->rnd_fed_count) < 0) return -1;
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
    pump_with_chunking(sp, &(rng_t){ seed ^ 0x1111 }, NULL, NULL);

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
        if (pump_with_chunking(sp, &cr, NULL, NULL) < 0) goto cleanup;

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
    pump_with_chunking(sp, &(rng_t){ seed ^ 0x3333 }, NULL, NULL);
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
                                &summary->rnd_fed_count);
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
