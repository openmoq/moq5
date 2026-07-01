/*
 * Deterministic publisher facade scenario runner.
 *
 * Generates seeded sequences of facade operations (add/remove track,
 * write objects, group changes, namespace accept/reject) through
 * SimPair. Client subscribes via session API and verifies received
 * objects via payload content hash.
 *
 * Each seed runs twice; trace hash must match. Object payloads are
 * verified via a separate content hash (not SimPair trace) so data
 * delivery correctness is covered even through SimPair's control-only
 * trace.
 */

#include <moq/publisher.h>
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

/* -- FNV-1a hash ---------------------------------------------------- */

static uint64_t fnv_init(void) { return 0xCBF29CE484222325ULL; }

static uint64_t fnv_feed(uint64_t h, const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        h ^= data[i]; h *= 0x100000001B3ULL;
    }
    return h;
}

static uint64_t fnv_u64(uint64_t h, uint64_t v) {
    uint8_t buf[8];
    memcpy(buf, &v, 8);
    return fnv_feed(h, buf, 8);
}

/* -- Trace hash ----------------------------------------------------- */

typedef struct {
    uint64_t trace_hash;
    size_t   trace_count;
    uint64_t expected_hash;
    size_t   expected_count;
    uint64_t received_hash;
    size_t   received_count;
    size_t   ns_accepted;
    size_t   ns_rejected;
    size_t   remove_accepted;
    size_t   remove_terminal;
} run_summary_t;

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

#define MAX_TRACKS 4

typedef enum {
    SHADOW_NS_NONE = 0,
    SHADOW_NS_PENDING,
    SHADOW_NS_ACCEPTED,
    SHADOW_NS_TERMINAL,
} shadow_ns_state_t;

typedef struct {
    bool               active;
    moq_pub_track_t   *handle;
    char               name[8];
    bool               advertised;
    shadow_ns_state_t  ns_state;
    bool               has_sub;
    uint64_t           cur_group;
    uint64_t           next_obj;
    bool               group_open;
} shadow_track_t;

typedef struct {
    shadow_track_t tracks[MAX_TRACKS];
    bool           closed;
} shadow_state_t;

static int shadow_find_active(const shadow_state_t *st) {
    for (int i = 0; i < MAX_TRACKS; i++)
        if (st->tracks[i].active) return i;
    return -1;
}

static int shadow_find_with_sub(const shadow_state_t *st) {
    for (int i = 0; i < MAX_TRACKS; i++)
        if (st->tracks[i].active && st->tracks[i].has_sub) return i;
    return -1;
}

static int shadow_find_with_group(const shadow_state_t *st) {
    for (int i = 0; i < MAX_TRACKS; i++)
        if (st->tracks[i].active && st->tracks[i].has_sub &&
            st->tracks[i].group_open) return i;
    return -1;
}

static int shadow_find_removable(const shadow_state_t *st) {
    for (int i = 0; i < MAX_TRACKS; i++)
        if (st->tracks[i].active &&
            st->tracks[i].ns_state != SHADOW_NS_PENDING)
            return i;
    return -1;
}

/* -- Operations ----------------------------------------------------- */

typedef enum {
    OP_PUMP,
    OP_ADD_TRACK,
    OP_REMOVE_TRACK,
    OP_SUBSCRIBE,
    OP_WRITE_OBJECT,
    OP_END_GROUP,
    OP_DRAIN_CLIENT_EVENTS,
    OP_DRAIN_SERVER_EVENTS,
    OP_ADVANCE_TIME,
    OP_COUNT,
} scenario_op_t;

static const char *op_name(scenario_op_t op) {
    static const char *n[] = {
        "PUMP","ADD_TRACK","RM_TRACK","SUBSCRIBE","WRITE_OBJ",
        "END_GROUP","DRAIN_C","DRAIN_S","ADV_TIME",
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

static void dump_log(uint64_t seed, int run, const op_log_t *l,
                      int max_steps) {
    fprintf(stderr, "  seed=0x%llx run=%d ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < l->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                op_name(l->e[i].op), (unsigned long long)l->e[i].p);
    fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
            "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
            "./build/tests/test_scenario_publisher\n",
            (unsigned long long)seed, max_steps);
}

/* -- Server event forwarding with shadow update -------------------- */

static int forward_server_event(moq_publisher_t *pub,
                                 shadow_state_t *st,
                                 const moq_event_t *ev,
                                 uint64_t now,
                                 run_summary_t *summary)
{
    moq_pub_event_result_t res;
    moq_result_t rc = moq_pub_handle_event(pub, ev, now, &res);

    if (ev->kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
        if (rc != MOQ_OK || res != MOQ_PUB_EVENT_CONSUMED) return -1;
        bool found = false;
        for (int i = 0; i < MAX_TRACKS; i++) {
            if (st->tracks[i].active && st->tracks[i].advertised &&
                st->tracks[i].ns_state == SHADOW_NS_PENDING) {
                st->tracks[i].ns_state = SHADOW_NS_ACCEPTED;
                summary->ns_accepted++;
                found = true;
                break;
            }
        }
        if (!found) return -1;
    } else if (ev->kind == MOQ_EVENT_NAMESPACE_REJECTED ||
               ev->kind == MOQ_EVENT_NAMESPACE_CANCELLED) {
        if (rc != MOQ_OK || res != MOQ_PUB_EVENT_CONSUMED) return -1;
        bool found = false;
        for (int i = 0; i < MAX_TRACKS; i++) {
            if (st->tracks[i].active && st->tracks[i].advertised &&
                (st->tracks[i].ns_state == SHADOW_NS_PENDING ||
                 st->tracks[i].ns_state == SHADOW_NS_ACCEPTED)) {
                st->tracks[i].ns_state = SHADOW_NS_TERMINAL;
                summary->ns_rejected++;
                found = true;
                break;
            }
        }
        if (!found) return -1;
    }
    return 0;
}

/* -- Execute one step ----------------------------------------------- */

static int execute_step(moq_simpair_t *sp, moq_publisher_t *pub,
                         shadow_state_t *st, rng_t *rng, op_log_t *log,
                         const moq_alloc_t *alloc, run_summary_t *summary)
{
    if (st->closed) return 0;
    int step_failures = 0;

    scenario_op_t op = (scenario_op_t)(rng_next(rng) % OP_COUNT);
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    switch (op) {
    case OP_PUMP:
        log_op(log, op, 0);
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        break;

    case OP_ADD_TRACK: {
        int slot = -1;
        for (int i = 0; i < MAX_TRACKS; i++)
            if (!st->tracks[i].active) { slot = i; break; }
        if (slot < 0) { log_op(log, OP_PUMP, 0); moq_simpair_run_until_quiescent(sp, 4, NULL); break; }

        char name[8];
        snprintf(name, sizeof(name), "t%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        bool advertise = (rng_next(rng) % 3) == 0;

        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        tcfg.track_namespace.parts = ns_parts;
        tcfg.track_namespace.count = 1;
        tcfg.track_name = moq_bytes_cstr(name);
        tcfg.advertise_namespace = advertise;

        moq_pub_track_t *th = NULL;
        moq_result_t rc = moq_pub_add_track(pub, &tcfg, now, &th);
        log_op(log, op, rc >= 0 ? 1 : 0);

        if (rc == MOQ_OK) {
            st->tracks[slot].active = true;
            st->tracks[slot].handle = th;
            memcpy(st->tracks[slot].name, name, sizeof(name));
            st->tracks[slot].advertised = advertise;
            st->tracks[slot].ns_state = advertise ? SHADOW_NS_PENDING : SHADOW_NS_NONE;
            st->tracks[slot].has_sub = false;
            st->tracks[slot].cur_group = 0;
            st->tracks[slot].next_obj = 0;
            st->tracks[slot].group_open = false;
        }
        break;
    }

    case OP_REMOVE_TRACK: {
        int idx = shadow_find_removable(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); moq_simpair_run_until_quiescent(sp, 4, NULL); break; }

        log_op(log, op, (uint64_t)idx);
        shadow_ns_state_t ns_before = st->tracks[idx].ns_state;
        moq_result_t rc = moq_pub_remove_track(pub, st->tracks[idx].handle, now);
        if (rc == MOQ_OK) {
            if (ns_before == SHADOW_NS_ACCEPTED)
                summary->remove_accepted++;
            else if (ns_before == SHADOW_NS_TERMINAL)
                summary->remove_terminal++;
            st->tracks[idx].active = false;
        }
        break;
    }

    case OP_SUBSCRIBE: {
        int idx = shadow_find_active(st);
        if (idx < 0 || st->tracks[idx].has_sub) {
            log_op(log, OP_PUMP, 0);
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            break;
        }

        log_op(log, op, (uint64_t)idx);

        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = moq_bytes_cstr(st->tracks[idx].name);

        moq_subscription_t sub_h;
        moq_result_t rc = moq_session_subscribe(client, &scfg, now, &sub_h);
        if (rc != MOQ_OK) break;

        moq_simpair_run_until_quiescent(sp, 4, NULL);

        /* Forward server events to facade + shadow. */
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(server, evts, 8);
        for (size_t j = 0; j < ne; j++) {
            if (forward_server_event(pub, st, &evts[j], now, summary) < 0) step_failures++;
            moq_event_cleanup(&evts[j]);
        }

        if (moq_pub_active_subscriptions(pub, st->tracks[idx].handle) > 0)
            st->tracks[idx].has_sub = true;

        moq_simpair_run_until_quiescent(sp, 4, NULL);
        /* Drain client SUBSCRIBE_OK/ERROR. */
        ne = moq_session_poll_events(client, evts, 8);
        for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
        break;
    }

    case OP_WRITE_OBJECT: {
        int idx = shadow_find_with_sub(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        shadow_track_t *t = &st->tracks[idx];
        uint64_t group = t->cur_group;
        uint64_t obj_id = t->next_obj;

        /* Random payload 1-16 bytes. */
        size_t plen = (rng_next(rng) % 16) + 1;
        uint8_t pdata[16];
        for (size_t j = 0; j < plen; j++)
            pdata[j] = (uint8_t)(rng_next(rng) & 0xFF);

        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(alloc, pdata, plen, &payload);
        if (!payload) { log_op(log, OP_PUMP, 0); break; }

        moq_result_t rc = moq_pub_write_object(pub, t->handle,
            group, obj_id, payload, now);
        log_op(log, op, rc >= 0 ? obj_id : UINT64_MAX);

        if (rc == MOQ_OK) {
            t->next_obj = obj_id + 1;
            t->group_open = true;
            summary->expected_count++;
            summary->expected_hash = fnv_u64(summary->expected_hash, group);
            summary->expected_hash = fnv_u64(summary->expected_hash, obj_id);
            summary->expected_hash = fnv_u64(summary->expected_hash, plen);
            summary->expected_hash = fnv_feed(summary->expected_hash, pdata, plen);
        }
        moq_rcbuf_decref(payload);
        break;
    }

    case OP_END_GROUP: {
        int idx = shadow_find_with_group(st);
        if (idx < 0) { log_op(log, OP_PUMP, 0); break; }

        log_op(log, op, st->tracks[idx].cur_group);
        moq_result_t rc = moq_pub_end_group(pub, st->tracks[idx].handle, now);
        if (rc == MOQ_OK) {
            st->tracks[idx].group_open = false;
            st->tracks[idx].cur_group++;
            st->tracks[idx].next_obj = 0;
        }
        break;
    }

    case OP_DRAIN_CLIENT_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(client, evts, 8);
        bool pumped_ns_response = false;
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                summary->received_count++;
                moq_rcbuf_t *p = evts[i].u.object_received.payload;
                uint64_t g = evts[i].u.object_received.group_id;
                uint64_t o = evts[i].u.object_received.object_id;
                size_t pl = p ? moq_rcbuf_len(p) : 0;
                summary->received_hash = fnv_u64(summary->received_hash, g);
                summary->received_hash = fnv_u64(summary->received_hash, o);
                summary->received_hash = fnv_u64(summary->received_hash, pl);
                if (p)
                    summary->received_hash = fnv_feed(summary->received_hash,
                        moq_rcbuf_data(p), pl);
            } else if (evts[i].kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                bool accept = (rng_next(rng) % 2) == 0;
                if (accept) {
                    moq_accept_namespace_cfg_t acc;
                    moq_accept_namespace_cfg_init(&acc);
                    moq_session_accept_namespace(client,
                        evts[i].u.namespace_published.ann, &acc, now);
                } else {
                    moq_reject_namespace_cfg_t rej;
                    moq_reject_namespace_cfg_init(&rej);
                    rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
                    moq_session_reject_namespace(client,
                        evts[i].u.namespace_published.ann, &rej, now);
                }
                pumped_ns_response = true;
            }
            moq_event_cleanup(&evts[i]);
        }
        if (pumped_ns_response) {
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            moq_event_t sevts[8];
            size_t sne = moq_session_poll_events(server, sevts, 8);
            for (size_t i = 0; i < sne; i++) {
                if (forward_server_event(pub, st, &sevts[i], now, summary) < 0)
                    step_failures++;
                moq_event_cleanup(&sevts[i]);
            }
        }
        break;
    }

    case OP_DRAIN_SERVER_EVENTS: {
        log_op(log, op, 0);
        moq_event_t evts[8];
        size_t ne = moq_session_poll_events(server, evts, 8);
        for (size_t i = 0; i < ne; i++) {
            if (forward_server_event(pub, st, &evts[i], now, summary) < 0)
                step_failures++;
            moq_event_cleanup(&evts[i]);
        }
        break;
    }

    case OP_ADVANCE_TIME: {
        uint64_t delta = (rng_next(rng) % 100) + 1;
        log_op(log, op, delta);
        moq_simpair_advance_to(sp, now + delta);
        break;
    }

    default:
        log_op(log, OP_PUMP, 0);
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        break;
    }

    return step_failures;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, const moq_alloc_t *alloc,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->trace_hash = fnv_init();
    summary->expected_hash = fnv_init();
    summary->received_hash = fnv_init();

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

    /* Create facade on server. */
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;

    moq_publisher_t *pub = NULL;
    if (moq_pub_create(moq_simpair_server(sp), alloc, &pcfg, &pub) < 0) {
        moq_simpair_destroy(sp);
        return -1;
    }

    shadow_state_t st;
    memset(&st, 0, sizeof(st));

    rng_t rng = { seed };
    op_log_t log = {0};
    int failures = 0;

    /* -- Deterministic coverage prelude ----------------------------- */
    /* Exercises both namespace-accepted and namespace-rejected removal
     * paths every run, independent of seed randomness. Tracks are
     * inserted into the shadow model so forward_server_event can
     * match and update ns_state. */
    {
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_result_t rc;

        /* Track A: advertise → accept → remove (ACCEPTED path). */
        moq_pub_track_cfg_t ta_cfg;
        moq_pub_track_cfg_init(&ta_cfg);
        ta_cfg.track_namespace.parts = ns_parts;
        ta_cfg.track_namespace.count = 1;
        ta_cfg.track_name = MOQ_BYTES_LITERAL("prelude_a");
        ta_cfg.advertise_namespace = true;

        moq_pub_track_t *ta = NULL;
        rc = moq_pub_add_track(pub, &ta_cfg, now, &ta);
        if (rc != MOQ_OK || !ta) { failures++; goto prelude_done; }

        st.tracks[0].active = true;
        st.tracks[0].handle = ta;
        st.tracks[0].advertised = true;
        st.tracks[0].ns_state = SHADOW_NS_PENDING;

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        { moq_event_t ev;
          size_t ne = moq_session_poll_events(client, &ev, 1);
          if (ne != 1 || ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
              if (ne) moq_event_cleanup(&ev);
              failures++; goto prelude_done;
          }
          moq_accept_namespace_cfg_t acc;
          moq_accept_namespace_cfg_init(&acc);
          rc = moq_session_accept_namespace(client,
              ev.u.namespace_published.ann, &acc, now);
          moq_event_cleanup(&ev);
          if (rc != MOQ_OK) { failures++; goto prelude_done; }
        }

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        { moq_event_t ev;
          size_t ne = moq_session_poll_events(server, &ev, 1);
          if (ne != 1 || ev.kind != MOQ_EVENT_NAMESPACE_ACCEPTED) {
              fprintf(stderr, "  prelude A: server got ne=%zu kind=%u (expected NAMESPACE_ACCEPTED=%u)\n",
                      ne, ne ? ev.kind : 0, MOQ_EVENT_NAMESPACE_ACCEPTED);
              if (ne) moq_event_cleanup(&ev);
              failures++; goto prelude_done;
          }
          { int frc = forward_server_event(pub, &st, &ev, now, summary);
            if (frc < 0) {
                fprintf(stderr, "  prelude A: forward_server_event failed\n");
                failures++;
            }
          }
          moq_event_cleanup(&ev);
        }

        rc = moq_pub_remove_track(pub, ta, now);
        if (rc == MOQ_OK) {
            summary->remove_accepted++;
            st.tracks[0].active = false;
        } else {
            fprintf(stderr, "  prelude A: remove_track rc=%d\n", (int)rc);
            failures++;
        }

        /* Drain stale events between A and B (NAMESPACE_DONE from A). */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t drain[8]; size_t dne;
          while ((dne = moq_session_poll_events(client, drain, 8)) > 0)
              for (size_t i = 0; i < dne; i++) moq_event_cleanup(&drain[i]);
          while ((dne = moq_session_poll_events(server, drain, 8)) > 0)
              for (size_t i = 0; i < dne; i++) moq_event_cleanup(&drain[i]);
        }

        /* Track B: advertise → reject → remove (TERMINAL path). */
        moq_pub_track_cfg_t tb_cfg;
        moq_pub_track_cfg_init(&tb_cfg);
        tb_cfg.track_namespace.parts = ns_parts;
        tb_cfg.track_namespace.count = 1;
        tb_cfg.track_name = MOQ_BYTES_LITERAL("prelude_b");
        tb_cfg.advertise_namespace = true;

        moq_pub_track_t *tb = NULL;
        rc = moq_pub_add_track(pub, &tb_cfg, now, &tb);
        if (rc != MOQ_OK || !tb) {
            fprintf(stderr, "  prelude B: add_track rc=%d\n", (int)rc);
            failures++; goto prelude_done;
        }

        st.tracks[0].active = true;
        st.tracks[0].handle = tb;
        st.tracks[0].advertised = true;
        st.tracks[0].ns_state = SHADOW_NS_PENDING;

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        { moq_event_t ev;
          size_t ne = moq_session_poll_events(client, &ev, 1);
          if (ne != 1 || ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
              fprintf(stderr, "  prelude B: client got ne=%zu kind=%u\n",
                      ne, ne ? ev.kind : 0);
              if (ne) moq_event_cleanup(&ev);
              failures++; goto prelude_done;
          }
          moq_reject_namespace_cfg_t rej;
          moq_reject_namespace_cfg_init(&rej);
          rej.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
          rc = moq_session_reject_namespace(client,
              ev.u.namespace_published.ann, &rej, now);
          moq_event_cleanup(&ev);
          if (rc != MOQ_OK) { failures++; goto prelude_done; }
        }

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        { moq_event_t sevts[8];
          size_t sne = moq_session_poll_events(server, sevts, 8);
          bool found_rejected = false;
          for (size_t si = 0; si < sne; si++) {
              if (sevts[si].kind == MOQ_EVENT_NAMESPACE_REJECTED) {
                  int frc = forward_server_event(pub, &st, &sevts[si], now, summary);
                  if (frc < 0) {
                      fprintf(stderr, "  prelude B: forward_server_event failed\n");
                      failures++;
                  }
                  found_rejected = true;
              }
              moq_event_cleanup(&sevts[si]);
          }
          if (!found_rejected) {
              fprintf(stderr, "  prelude B: no NAMESPACE_REJECTED in %zu events\n", sne);
              failures++; goto prelude_done;
          }
        }

        rc = moq_pub_remove_track(pub, tb, now);
        if (rc == MOQ_OK) {
            summary->remove_terminal++;
            st.tracks[0].active = false;
        } else {
            fprintf(stderr, "  prelude B: remove_track rc=%d\n", (int)rc);
            failures++;
        }

prelude_done:
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t drain[8]; size_t ne;
          while ((ne = moq_session_poll_events(client, drain, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
          while ((ne = moq_session_poll_events(server, drain, 8)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&drain[i]);
        }
        memset(&st, 0, sizeof(st));
    }

    /* -- Seeded random loop ----------------------------------------- */
    for (int step = 0; step < max_steps; step++) {
        failures += execute_step(sp, pub, &st, &rng, &log, alloc, summary);

        /* Pump and drain client objects after every step so data
         * written by OP_WRITE_OBJECT is delivered before a later
         * OP_REMOVE_TRACK can reset the subgroup stream. */
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        {
            moq_event_t cevts[8];
            size_t cne = moq_session_poll_events(moq_simpair_client(sp), cevts, 8);
            for (size_t ci = 0; ci < cne; ci++) {
                if (cevts[ci].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    summary->received_count++;
                    moq_rcbuf_t *p = cevts[ci].u.object_received.payload;
                    uint64_t g = cevts[ci].u.object_received.group_id;
                    uint64_t o = cevts[ci].u.object_received.object_id;
                    size_t pl = p ? moq_rcbuf_len(p) : 0;
                    summary->received_hash = fnv_u64(summary->received_hash, g);
                    summary->received_hash = fnv_u64(summary->received_hash, o);
                    summary->received_hash = fnv_u64(summary->received_hash, pl);
                    if (p)
                        summary->received_hash = fnv_feed(summary->received_hash,
                            moq_rcbuf_data(p), pl);
                }
                moq_event_cleanup(&cevts[ci]);
            }
        }

        if (moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED ||
            moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED) {
            if (!st.closed) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d step=%d: "
                        "unexpected close\n",
                        (unsigned long long)seed, run_id, step);
                dump_log(seed, run_id, &log, max_steps);
                failures++;
            }
            st.closed = true;
            break;
        }
    }

    /* Final: close open subgroups so FIN flushes data, then pump
     * until all actions/data are delivered. */
    for (int i = 0; i < MAX_TRACKS; i++) {
        if (st.tracks[i].active && st.tracks[i].group_open &&
            st.tracks[i].has_sub)
            moq_pub_end_group(pub, st.tracks[i].handle,
                moq_simpair_now_us(sp));
    }
    for (int p = 0; p < 16; p++)
        moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Drain remaining client objects into received hash. */
    {
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    summary->received_count++;
                    moq_rcbuf_t *p = evts[i].u.object_received.payload;
                    uint64_t g = evts[i].u.object_received.group_id;
                    uint64_t o = evts[i].u.object_received.object_id;
                    size_t pl = p ? moq_rcbuf_len(p) : 0;
                    summary->received_hash = fnv_u64(summary->received_hash, g);
                    summary->received_hash = fnv_u64(summary->received_hash, o);
                    summary->received_hash = fnv_u64(summary->received_hash, pl);
                    if (p)
                        summary->received_hash = fnv_feed(summary->received_hash,
                            moq_rcbuf_data(p), pl);
                }
                moq_event_cleanup(&evts[i]);
            }
        while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
        moq_action_t acts[16]; size_t na;
        while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
            for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    }

    moq_pub_destroy(pub);
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

    size_t total_ns_accepted = 0, total_ns_rejected = 0;
    size_t total_rm_accepted = 0, total_rm_terminal = 0;

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
                        "./build/tests/test_scenario_publisher\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        if (sums[0].trace_hash != sums[1].trace_hash ||
            sums[0].trace_count != sums[1].trace_count) {
            fprintf(stderr, "FAIL seed=0x%llx: trace mismatch "
                    "hash1=0x%llx hash2=0x%llx count1=%zu count2=%zu\n",
                    (unsigned long long)seed,
                    (unsigned long long)sums[0].trace_hash,
                    (unsigned long long)sums[1].trace_hash,
                    sums[0].trace_count, sums[1].trace_count);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_publisher\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        /* Delivery oracle: every written object must be received. */
        for (int run = 0; run < 2; run++) {
            if (sums[run].expected_count != sums[run].received_count ||
                sums[run].expected_hash  != sums[run].received_hash) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d: delivery mismatch "
                        "expected=%zu received=%zu "
                        "ehash=0x%llx rhash=0x%llx\n",
                        (unsigned long long)seed, run,
                        sums[run].expected_count, sums[run].received_count,
                        (unsigned long long)sums[run].expected_hash,
                        (unsigned long long)sums[run].received_hash);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_publisher\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        total_ns_accepted  += sums[0].ns_accepted;
        total_ns_rejected  += sums[0].ns_rejected;
        total_rm_accepted  += sums[0].remove_accepted;
        total_rm_terminal  += sums[0].remove_terminal;
    }

    if (total_ns_accepted == 0) {
        fprintf(stderr, "FAIL: no namespace accepted events across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }
    if (total_ns_rejected == 0) {
        fprintf(stderr, "FAIL: no namespace rejected events across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }
    if (total_rm_accepted == 0) {
        fprintf(stderr, "FAIL: no remove-track with accepted namespace across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }
    if (total_rm_terminal == 0) {
        fprintf(stderr, "FAIL: no remove-track with terminal namespace across %llu seeds\n",
                (unsigned long long)num_seeds);
        failures++;
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_publisher (%llu seeds)\n",
                (unsigned long long)num_seeds);
    else
        fprintf(stderr, "FAIL: test_scenario_publisher (%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
