/*
 * Deterministic queue-backpressure scenario runner.
 *
 * Uses tiny session capacities (max_actions=2, max_events=3)
 * via raw establish_pair (not SimPair) to force WOULD_BLOCK on
 * accept/reject/namespace action-queue paths.
 *
 * Deterministic prelude guarantees nonzero coverage of:
 *   - accept_subscribe → WOULD_BLOCK → drain → retry OK
 *   - reject_subscribe → WOULD_BLOCK → drain → retry OK
 *   - accept_namespace → WOULD_BLOCK → drain → retry OK
 *   - control input (SUBSCRIBE) → WOULD_BLOCK → drain → process_pending OK
 *   - data input (OBJECT_RECEIVED) → WOULD_BLOCK → drain → zero-byte retry OK
 *
 * Seeded random phase interleaves subscribe, accept/reject, namespace,
 * subgroup, write, drain, pump, and process_pending operations under
 * the same tiny capacities. Explores mixed-operation WOULD_BLOCK
 * recovery: blocked inputs retried after drains, payload preservation
 * through WB cycles, and cross-feature registry pressure.
 *
 * Each seed runs twice; an FNV-1a hash of operation results
 * must match. No SimPair trace (raw sessions, core-only).
 */

#include <moq/session.h>
#include <moq/codec.h>
#include "../../tests/unit/test_support.h"
#include "../../tests/unit/test_session_support.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* -- Operation hash ------------------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   ops;
    size_t   accept_wb;
    size_t   reject_wb;
    size_t   namespace_wb;
    size_t   control_event_wb;
    size_t   object_event_wb;
    size_t   retry_success;
    /* Random phase. */
    size_t   rnd_ops;
    size_t   rnd_wb_action;
    size_t   rnd_wb_control_input;
    size_t   rnd_wb_data_input;
    size_t   rnd_retries;
    size_t   rnd_objects_sent;
    size_t   rnd_objects_received;
    size_t   rnd_namespace_ops;
    size_t   rnd_closes;
    uint64_t rnd_sent_hash;
    uint64_t rnd_recv_hash;
} run_summary_t;

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

/* -- splitmix64 PRNG ------------------------------------------------ */

typedef struct { uint64_t s; } rng_t;

static uint64_t rng_next(rng_t *r) {
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* -- Drain helpers -------------------------------------------------- */

static void drain_events(moq_session_t *s) {
    moq_event_t evts[4]; size_t ne;
    while ((ne = moq_session_poll_events(s, evts, 4)) > 0)
        for (size_t i = 0; i < ne; i++)
            moq_event_cleanup(&evts[i]);
}

static void drain_actions(moq_session_t *s) {
    moq_action_t acts[4]; size_t na;
    while ((na = moq_session_poll_actions(s, acts, 4)) > 0)
        for (size_t i = 0; i < na; i++)
            moq_action_cleanup(&acts[i]);
}

static void drain_session(moq_session_t *s) {
    drain_events(s);
    drain_actions(s);
}

static void drain_both(moq_session_t *c, moq_session_t *sv) {
    drain_session(c);
    drain_session(sv);
}

/* -- Subscribe helper ----------------------------------------------- */

static moq_result_t do_subscribe(moq_session_t *c, const char *name,
                                  moq_subscription_t *out)
{
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bp") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = ns;
    cfg.track_name = moq_bytes_cstr(name);
    cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    return moq_session_subscribe(c, &cfg, 0, out);
}

/* -- Deterministic prelude ------------------------------------------ */

static int run_prelude(moq_session_t *c, moq_session_t *sv,
                       run_summary_t *ts, const moq_alloc_t *alloc)
{
    moq_event_t ev;
    moq_result_t rc;

    /* --- Segment 1: accept_subscribe WOULD_BLOCK ---
     * Subscribe 3 tracks, pump each. Accept A and B fills the action
     * queue (2/2). Accept C → WOULD_BLOCK. Drain → retry OK. */

    moq_subscription_t csub_a, csub_b, csub_c;
    rc = do_subscribe(c, "a", &csub_a);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(c, sv, 0);
    drain_events(c);

    rc = do_subscribe(c, "b", &csub_b);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(c, sv, 0);
    drain_events(c);

    rc = do_subscribe(c, "c", &csub_c);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(c, sv, 0);
    drain_events(c);

    moq_subscription_t ssub[3];
    for (int i = 0; i < 3; i++) {
        if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_event_cleanup(&ev); return -1;
        }
        ssub[i] = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);

    rc = moq_session_accept_subscribe(sv, ssub[0], &acc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;

    rc = moq_session_accept_subscribe(sv, ssub[1], &acc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;

    rc = moq_session_accept_subscribe(sv, ssub[2], &acc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_ERR_WOULD_BLOCK) return -1;
    ts->accept_wb++;

    drain_actions(sv);
    pump_actions_to_peer(sv, c, 0);
    drain_events(c);

    rc = moq_session_accept_subscribe(sv, ssub[2], &acc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;
    ts->retry_success++;

    pump_actions_to_peer(sv, c, 0);
    drain_both(c, sv);

    /* --- Segment 2: reject_subscribe WOULD_BLOCK ---
     * Same pattern: 3 subscribes, reject A and B fills queue,
     * reject C → WOULD_BLOCK, drain → retry OK. */

    moq_subscription_t csub_d, csub_e, csub_f;
    rc = do_subscribe(c, "d", &csub_d);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(c, sv, 0);
    drain_events(c);

    rc = do_subscribe(c, "e", &csub_e);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(c, sv, 0);
    drain_events(c);

    rc = do_subscribe(c, "f", &csub_f);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(c, sv, 0);
    drain_events(c);

    moq_subscription_t ssub2[3];
    for (int i = 0; i < 3; i++) {
        if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_event_cleanup(&ev); return -1;
        }
        ssub2[i] = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }

    moq_reject_subscribe_cfg_t rej;
    moq_reject_subscribe_cfg_init(&rej);
    rej.error_code = 1;
    rej.reason = MOQ_BYTES_LITERAL("no");

    rc = moq_session_reject_subscribe(sv, ssub2[0], &rej, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;

    rc = moq_session_reject_subscribe(sv, ssub2[1], &rej, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;

    rc = moq_session_reject_subscribe(sv, ssub2[2], &rej, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_ERR_WOULD_BLOCK) return -1;
    ts->reject_wb++;

    drain_actions(sv);
    pump_actions_to_peer(sv, c, 0);
    drain_events(c);

    rc = moq_session_reject_subscribe(sv, ssub2[2], &rej, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;
    ts->retry_success++;

    pump_actions_to_peer(sv, c, 0);
    drain_both(c, sv);

    /* --- Segment 3: accept_namespace WOULD_BLOCK ---
     * Publish 3 namespaces (pump each, drain server only so client
     * accumulates 3 events). Accept A and B fills client action
     * queue, accept C → WOULD_BLOCK, drain → retry OK. */

    moq_publish_namespace_cfg_t pn_cfg;
    moq_publish_namespace_cfg_init(&pn_cfg);

    moq_bytes_t ns_a[] = { MOQ_BYTES_LITERAL("na") };
    moq_bytes_t ns_b[] = { MOQ_BYTES_LITERAL("nb") };
    moq_bytes_t ns_c[] = { MOQ_BYTES_LITERAL("nc") };

    moq_announcement_t sv_ann_a, sv_ann_b, sv_ann_c;

    pn_cfg.track_namespace.parts = ns_a;
    pn_cfg.track_namespace.count = 1;
    rc = moq_session_publish_namespace(sv, &pn_cfg, 0, &sv_ann_a);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(sv, c, 0);
    drain_session(sv);

    pn_cfg.track_namespace.parts = ns_b;
    rc = moq_session_publish_namespace(sv, &pn_cfg, 0, &sv_ann_b);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(sv, c, 0);
    drain_session(sv);

    pn_cfg.track_namespace.parts = ns_c;
    rc = moq_session_publish_namespace(sv, &pn_cfg, 0, &sv_ann_c);
    if (rc != MOQ_OK) return -1;
    pump_actions_to_peer(sv, c, 0);
    drain_session(sv);

    moq_announcement_t c_ann[3];
    for (int i = 0; i < 3; i++) {
        if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_event_cleanup(&ev); return -1;
        }
        c_ann[i] = ev.u.namespace_published.ann;
        moq_event_cleanup(&ev);
    }

    moq_accept_namespace_cfg_t nacc;
    moq_accept_namespace_cfg_init(&nacc);

    rc = moq_session_accept_namespace(c, c_ann[0], &nacc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;

    rc = moq_session_accept_namespace(c, c_ann[1], &nacc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;

    rc = moq_session_accept_namespace(c, c_ann[2], &nacc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_ERR_WOULD_BLOCK) return -1;
    ts->namespace_wb++;

    drain_actions(c);
    pump_actions_to_peer(c, sv, 0);
    drain_events(sv);

    rc = moq_session_accept_namespace(c, c_ann[2], &nacc, 0);
    hash_op(ts, (uint64_t)rc);
    if (rc != MOQ_OK) return -1;
    ts->retry_success++;

    pump_actions_to_peer(c, sv, 0);
    drain_both(c, sv);

    /* --- Segment 4: control input WOULD_BLOCK (SUBSCRIBE event) ---
     * Server has max_events=3. Fill with 3 SUBSCRIBE_REQUEST events
     * (don't drain server events between pumps). Next SUBSCRIBE
     * on_control_bytes → WOULD_BLOCK. Drain events, retry via
     * process_pending → SUBSCRIBE_REQUEST emitted. */
    {
        moq_subscription_t csub_g, csub_h, csub_i, csub_j;
        rc = do_subscribe(c, "g", &csub_g);
        if (rc != MOQ_OK) return -1;
        pump_actions_to_peer(c, sv, 0);
        drain_events(c);

        rc = do_subscribe(c, "h", &csub_h);
        if (rc != MOQ_OK) return -1;
        pump_actions_to_peer(c, sv, 0);
        drain_events(c);

        rc = do_subscribe(c, "i", &csub_i);
        if (rc != MOQ_OK) return -1;
        pump_actions_to_peer(c, sv, 0);
        drain_events(c);
        /* Server now has 3 SUBSCRIBE_REQUEST events (3/3 full). */

        rc = do_subscribe(c, "j", &csub_j);
        if (rc != MOQ_OK) return -1;

        /* Manually feed client's SUBSCRIBE action to server. */
        moq_action_t acts[4]; size_t na;
        moq_result_t ctrl_rc = MOQ_OK;
        while ((na = moq_session_poll_actions(c, acts, 4)) > 0)
            for (size_t ai = 0; ai < na; ai++) {
                if (acts[ai].kind == MOQ_ACTION_SEND_CONTROL) {
                    moq_result_t r = moq_session_on_control_bytes(sv,
                        acts[ai].u.send_control.data,
                        acts[ai].u.send_control.len, 0);
                    if (r < 0) ctrl_rc = r;
                    hash_op(ts, (uint64_t)r);
                }
                moq_action_cleanup(&acts[ai]);
            }

        if (ctrl_rc != MOQ_ERR_WOULD_BLOCK) return -1;
        ts->control_event_wb++;

        /* Drain 3 server events to make room. */
        for (int di = 0; di < 3; di++) {
            if (moq_session_poll_events(sv, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }

        /* Retry via process_pending. */
        rc = moq_session_process_pending(sv, 0);
        hash_op(ts, (uint64_t)rc);
        if (rc != MOQ_OK) return -1;

        /* The 4th SUBSCRIBE_REQUEST should now be emitted. */
        if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_event_cleanup(&ev); return -1;
        }
        moq_event_cleanup(&ev);
        ts->retry_success++;
    }

    drain_both(c, sv);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED)
        return -1;

    /* --- Segment 5: data input WOULD_BLOCK (OBJECT_RECEIVED event) ---
     * Client has max_events=3. Create a fresh subscription whose
     * SUBSCRIBE_OK was actually delivered to the client (segments 1-2
     * discard some accepts via drain_actions). Fill client event queue
     * with objects, then feed one more → WOULD_BLOCK. Drain events,
     * retry with zero bytes → OBJECT_RECEIVED with correct payload. */
    {
        /* Fresh subscription for data delivery. */
        moq_subscription_t data_csub;
        rc = do_subscribe(c, "data", &data_csub);
        if (rc != MOQ_OK) return -1;
        pump_actions_to_peer(c, sv, 0);
        drain_events(c);

        if (moq_session_poll_events(sv, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_event_cleanup(&ev); return -1;
        }
        moq_subscription_t data_ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t data_acc;
        moq_accept_subscribe_cfg_init(&data_acc);
        rc = moq_session_accept_subscribe(sv, data_ssub, &data_acc, 0);
        if (rc != MOQ_OK) return -1;
        pump_actions_to_peer(sv, c, 0);

        if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
            moq_event_cleanup(&ev); return -1;
        }
        moq_event_cleanup(&ev);
        drain_both(c, sv);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        rc = moq_session_open_subgroup(sv, data_ssub, &sg_cfg, 0, &sg);
        if (rc != MOQ_OK) return -1;

        /* Feed the subgroup header to the client, capturing stream_ref. */
        moq_stream_ref_t data_ref = moq_stream_ref_from_u64(0);
        {
            moq_action_t acts[4]; size_t na;
            while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
                for (size_t ai = 0; ai < na; ai++) {
                    if (acts[ai].kind == MOQ_ACTION_SEND_DATA) {
                        data_ref = acts[ai].u.send_data.stream_ref;
                        FEED_SEND_DATA(c, data_ref, acts[ai], 0);
                    }
                    moq_action_cleanup(&acts[ai]);
                }
        }

        for (uint64_t obj = 0; obj < 3; obj++) {
            moq_rcbuf_t *p = NULL;
            if (moq_rcbuf_create(alloc, (const uint8_t *)"x", 1, &p)
                != MOQ_OK) return -1;
            rc = moq_session_write_object(sv, sg, obj, p, 0);
            moq_rcbuf_decref(p);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                drain_actions(sv);
                p = NULL;
                if (moq_rcbuf_create(alloc, (const uint8_t *)"x", 1, &p)
                    != MOQ_OK) return -1;
                rc = moq_session_write_object(sv, sg, obj, p, 0);
                moq_rcbuf_decref(p);
            }
            if (rc != MOQ_OK) return -1;

            /* Poll server actions and feed data to client. */
            moq_action_t acts[4]; size_t na;
            while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
                for (size_t ai = 0; ai < na; ai++) {
                    if (acts[ai].kind == MOQ_ACTION_SEND_DATA)
                        FEED_SEND_DATA(c, data_ref, acts[ai], 0);
                    moq_action_cleanup(&acts[ai]);
                }
        }
        /* Client now has 3 OBJECT_RECEIVED events (3/3 full). */

        /* Write 4th object. */
        {
            moq_rcbuf_t *p = NULL;
            if (moq_rcbuf_create(alloc, (const uint8_t *)"wb", 2, &p)
                != MOQ_OK) return -1;
            rc = moq_session_write_object(sv, sg, 3, p, 0);
            moq_rcbuf_decref(p);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                drain_actions(sv);
                p = NULL;
                if (moq_rcbuf_create(alloc, (const uint8_t *)"wb", 2, &p)
                    != MOQ_OK) return -1;
                rc = moq_session_write_object(sv, sg, 3, p, 0);
                moq_rcbuf_decref(p);
            }
            if (rc != MOQ_OK) return -1;
        }

        /* Feed 4th object data to client, checking return codes. */
        moq_result_t data_rc = MOQ_OK;
        {
            moq_action_t acts[4]; size_t na;
            while ((na = moq_session_poll_actions(sv, acts, 4)) > 0)
                for (size_t ai = 0; ai < na; ai++) {
                    if (acts[ai].kind == MOQ_ACTION_SEND_DATA) {
                        bool hp = (acts[ai].u.send_data.payload != NULL);
                        bool fin = acts[ai].u.send_data.fin;
                        if (acts[ai].u.send_data.header_len > 0) {
                            moq_result_t r = moq_session_on_data_bytes(
                                c, data_ref,
                                acts[ai].u.send_data.header,
                                acts[ai].u.send_data.header_len,
                                fin && !hp, 0);
                            if (r < 0) data_rc = r;
                            hash_op(ts, (uint64_t)r);
                        }
                        if (data_rc >= 0 && hp) {
                            moq_result_t r = moq_session_on_data_bytes(
                                c, data_ref,
                                moq_rcbuf_data(acts[ai].u.send_data.payload),
                                moq_rcbuf_len(acts[ai].u.send_data.payload),
                                fin, 0);
                            if (r < 0) data_rc = r;
                            hash_op(ts, (uint64_t)r);
                        }
                    }
                    moq_action_cleanup(&acts[ai]);
                }
        }
        if (data_rc != MOQ_ERR_WOULD_BLOCK) return -1;
        ts->object_event_wb++;

        /* Drain client events to free space. */
        drain_events(c);

        /* Retry with zero-byte on_data_bytes. */
        rc = moq_session_on_data_bytes(c, data_ref, NULL, 0, false, 0);
        hash_op(ts, (uint64_t)rc);
        if (rc != MOQ_OK) return -1;

        /* The 4th OBJECT_RECEIVED should now be emitted. */
        if (moq_session_poll_events(c, &ev, 1) != 1) return -1;
        if (ev.kind != MOQ_EVENT_OBJECT_RECEIVED) {
            moq_event_cleanup(&ev); return -1;
        }
        if (moq_rcbuf_len(ev.u.object_received.payload) != 2 ||
            memcmp(moq_rcbuf_data(ev.u.object_received.payload), "wb", 2) != 0) {
            moq_event_cleanup(&ev); return -1;
        }
        moq_event_cleanup(&ev);
        ts->retry_success++;
    }

    drain_both(c, sv);
    return 0;
}

/* -- Random phase shadow model -------------------------------------- */

#define RND_MAX_SUBS 8
#define RND_MAX_ANNS 8
#define RND_MAX_SGS  4

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    bool has_server_handle;
    moq_subscription_t server_handle;
    moq_subscription_t client_handle;
} rnd_sub_t;

typedef struct {
    bool active;
    bool pending;
    bool accepted;
    bool has_client_handle;
    moq_announcement_t server_handle;
    moq_announcement_t client_handle;
} rnd_ann_t;

typedef struct {
    bool active;
    moq_subgroup_handle_t handle;
    uint64_t group_id;
    uint64_t next_obj;
} rnd_sg_t;

typedef struct {
    rnd_sub_t subs[RND_MAX_SUBS];
    rnd_ann_t anns[RND_MAX_ANNS];
    rnd_sg_t  sgs[RND_MAX_SGS];
    bool closed;
    uint64_t next_group;
} rnd_state_t;

static int rnd_find_pending_sub(const rnd_state_t *st) {
    for (int i = 0; i < RND_MAX_SUBS; i++)
        if (st->subs[i].active && st->subs[i].pending &&
            st->subs[i].has_server_handle) return i;
    return -1;
}

static int rnd_find_accepted_sub(const rnd_state_t *st) {
    for (int i = 0; i < RND_MAX_SUBS; i++)
        if (st->subs[i].active && st->subs[i].accepted) return i;
    return -1;
}

static int rnd_find_pending_ann(const rnd_state_t *st) {
    for (int i = 0; i < RND_MAX_ANNS; i++)
        if (st->anns[i].active && st->anns[i].pending &&
            st->anns[i].has_client_handle) return i;
    return -1;
}

static int rnd_find_accepted_ann(const rnd_state_t *st) {
    for (int i = 0; i < RND_MAX_ANNS; i++)
        if (st->anns[i].active && st->anns[i].accepted) return i;
    return -1;
}

static int rnd_find_open_sg(const rnd_state_t *st) {
    for (int i = 0; i < RND_MAX_SGS; i++)
        if (st->sgs[i].active) return i;
    return -1;
}

/* -- Random phase operations ---------------------------------------- */

typedef enum {
    RND_SUBSCRIBE,
    RND_ACCEPT_SUB,
    RND_REJECT_SUB,
    RND_PUB_NS,
    RND_ACCEPT_NS,
    RND_NS_DONE,
    RND_NS_CANCEL,
    RND_OPEN_SG,
    RND_WRITE_OBJ,
    RND_CLOSE_SG,
    RND_RESET_SG,
    RND_PUMP,
    RND_DRAIN_C,
    RND_DRAIN_S,
    RND_PROC_PEND_S,
    RND_PROC_PEND_C,
    RND_OP_COUNT,
} rnd_op_t;

static const char *rnd_op_name(rnd_op_t op) {
    static const char *n[] = {
        "SUB","ACC_SUB","REJ_SUB","PUB_NS","ACC_NS","NS_DONE",
        "NS_CANCEL","OPEN_SG","WRITE","CLOSE_SG","RESET_SG",
        "PUMP","DRAIN_C","DRAIN_S","PROC_PEND_S","PROC_PEND_C",
    };
    return op < RND_OP_COUNT ? n[op] : "?";
}

#define RND_MAX_LOG 128
typedef struct { rnd_op_t op; uint64_t p; } rnd_log_entry_t;
typedef struct { rnd_log_entry_t e[RND_MAX_LOG]; size_t n; } rnd_log_t;

static void rnd_log_op(rnd_log_t *l, rnd_op_t op, uint64_t p) {
    if (l->n < RND_MAX_LOG) { l->e[l->n].op = op; l->e[l->n].p = p; l->n++; }
}

static void rnd_dump_log(uint64_t seed, int run, const rnd_log_t *l) {
    fprintf(stderr, "  seed=0x%llx run=%d rnd_ops:\n",
            (unsigned long long)seed, run);
    for (size_t i = 0; i < l->n; i++)
        fprintf(stderr, "    %zu: %s p=%llu\n", i,
                rnd_op_name(l->e[i].op), (unsigned long long)l->e[i].p);
}

/* -- Random phase drain --------------------------------------------- */

static void rnd_drain_client(moq_session_t *c, run_summary_t *ts) {
    moq_event_t evts[4]; size_t ne;
    while ((ne = moq_session_poll_events(c, evts, 4)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                ts->rnd_objects_received++;
                if (evts[i].u.object_received.payload) {
                    hash_object(&ts->rnd_recv_hash,
                        evts[i].u.object_received.group_id,
                        evts[i].u.object_received.object_id,
                        moq_rcbuf_len(evts[i].u.object_received.payload),
                        moq_rcbuf_data(evts[i].u.object_received.payload));
                } else {
                    hash_object(&ts->rnd_recv_hash,
                        evts[i].u.object_received.group_id,
                        evts[i].u.object_received.object_id, 0, NULL);
                }
            }
            moq_event_cleanup(&evts[i]);
        }
}

/* -- Random phase pump with WB retry -------------------------------- */

static int rnd_feed_data(moq_session_t *c, const moq_action_t *act,
                          run_summary_t *ts)
{
    moq_stream_ref_t ref = act->u.send_data.stream_ref;
    bool hp = (act->u.send_data.payload != NULL);
    bool fin = act->u.send_data.fin;
    moq_result_t drc = MOQ_OK;

    if (act->u.send_data.header_len > 0) {
        drc = moq_session_on_data_bytes(c, ref,
            act->u.send_data.header, act->u.send_data.header_len,
            fin && !hp, 0);
        hash_op(ts, (uint64_t)drc);
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_data_input++;
            /* A blocked delivery may need more than one drain+retry cycle: the
             * final object/chunk and the trailing SUBGROUP_FINISHED can each
             * need their own event-queue slot. Retry until it resolves. */
            while (drc == MOQ_ERR_WOULD_BLOCK) {
                rnd_drain_client(c, ts);
                drc = moq_session_on_data_bytes(c, ref, NULL, 0, false, 0);
                hash_op(ts, (uint64_t)drc);
            }
            if (drc == MOQ_OK) ts->rnd_retries++;
            else if (drc != MOQ_ERR_CLOSED) return -1;
        }
        if (drc < 0 && drc != MOQ_ERR_CLOSED) return -1;
        if (drc < 0) return 0;
    }

    if (hp) {
        drc = moq_session_on_data_bytes(c, ref,
            moq_rcbuf_data(act->u.send_data.payload),
            moq_rcbuf_len(act->u.send_data.payload), fin, 0);
        hash_op(ts, (uint64_t)drc);
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_data_input++;
            /* May need multiple drain+retry cycles (final object/chunk, then
             * the trailing SUBGROUP_FINISHED). Retry until it resolves. */
            while (drc == MOQ_ERR_WOULD_BLOCK) {
                rnd_drain_client(c, ts);
                drc = moq_session_on_data_bytes(c, ref, NULL, 0, false, 0);
                hash_op(ts, (uint64_t)drc);
            }
            if (drc == MOQ_OK) ts->rnd_retries++;
            else if (drc != MOQ_ERR_CLOSED) return -1;
        }
        if (drc < 0 && drc != MOQ_ERR_CLOSED) return -1;
        return 0;
    }

    if (!hp && act->u.send_data.header_len == 0 && fin) {
        drc = moq_session_on_data_bytes(c, ref, NULL, 0, true, 0);
        hash_op(ts, (uint64_t)drc);
        /* A bare FIN finishes the subgroup and now emits SUBGROUP_FINISHED,
         * which can WOULD_BLOCK on the tiny queue. Drain and retry until it
         * queues (or the session closes). */
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_data_input++;
            while (drc == MOQ_ERR_WOULD_BLOCK) {
                rnd_drain_client(c, ts);
                drc = moq_session_on_data_bytes(c, ref, NULL, 0, false, 0);
                hash_op(ts, (uint64_t)drc);
            }
            if (drc == MOQ_OK) ts->rnd_retries++;
            else if (drc != MOQ_ERR_CLOSED) return -1;
        }
        if (drc < 0 && drc != MOQ_ERR_CLOSED) return -1;
    }
    return 0;
}

static int rnd_pump_s_to_c(moq_session_t *sv, moq_session_t *c,
                             run_summary_t *ts)
{
    moq_action_t acts[4]; size_t n;
    while ((n = moq_session_poll_actions(sv, acts, 4)) > 0)
        for (size_t i = 0; i < n; i++) {
            int err = 0;
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_result_t rc = moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
                hash_op(ts, (uint64_t)rc);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    ts->rnd_wb_control_input++;
                    rnd_drain_client(c, ts);
                    rc = moq_session_process_pending(c, 0);
                    hash_op(ts, (uint64_t)rc);
                    if (rc == MOQ_OK) ts->rnd_retries++;
                    else if (rc != MOQ_ERR_CLOSED) err = -1;
                } else if (rc < 0 && rc != MOQ_ERR_CLOSED) {
                    err = -1;
                }
            } else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                err = rnd_feed_data(c, &acts[i], ts);
            } else if (acts[i].kind == MOQ_ACTION_RESET_DATA) {
                moq_result_t rc = moq_session_on_data_reset(c,
                    acts[i].u.reset_data.stream_ref,
                    acts[i].u.reset_data.error_code, 0);
                if (rc < 0 && rc != MOQ_ERR_CLOSED) err = -1;
            }
            moq_action_cleanup(&acts[i]);
            if (err < 0) {
                for (size_t j = i + 1; j < n; j++)
                    moq_action_cleanup(&acts[j]);
                return -1;
            }
        }
    return 0;
}

static int rnd_pump_c_to_s(moq_session_t *c, moq_session_t *sv,
                             run_summary_t *ts)
{
    moq_action_t acts[4]; size_t n;
    while ((n = moq_session_poll_actions(c, acts, 4)) > 0)
        for (size_t i = 0; i < n; i++) {
            int err = 0;
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_result_t rc = moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
                hash_op(ts, (uint64_t)rc);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    ts->rnd_wb_control_input++;
                    drain_events(sv);
                    rc = moq_session_process_pending(sv, 0);
                    hash_op(ts, (uint64_t)rc);
                    if (rc == MOQ_OK) ts->rnd_retries++;
                    else if (rc != MOQ_ERR_CLOSED) err = -1;
                } else if (rc < 0 && rc != MOQ_ERR_CLOSED) {
                    err = -1;
                }
            }
            moq_action_cleanup(&acts[i]);
            if (err < 0) {
                for (size_t j = i + 1; j < n; j++)
                    moq_action_cleanup(&acts[j]);
                return -1;
            }
        }
    return 0;
}

/* -- Random phase step ---------------------------------------------- */

#define RND_TRY_PUMP(expr) do { if ((expr) < 0) return -1; } while (0)

static int rnd_execute_step(moq_session_t *c, moq_session_t *sv,
                             rnd_state_t *st, rng_t *rng,
                             rnd_log_t *log, run_summary_t *ts,
                             const moq_alloc_t *alloc)
{
    if (st->closed) return 0;

    rnd_op_t op = (rnd_op_t)(rng_next(rng) % RND_OP_COUNT);
    bool defer_pump = (rng_next(rng) % 3 == 0);

    switch (op) {
    case RND_SUBSCRIBE: {
        int slot = -1;
        for (int i = 0; i < RND_MAX_SUBS; i++)
            if (!st->subs[i].active) { slot = i; break; }
        if (slot < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        char name[16];
        snprintf(name, sizeof(name), "r%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        moq_subscription_t h;
        moq_result_t rc = do_subscribe(c, name, &h);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)rc);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
            rc = do_subscribe(c, name, &h);
            hash_op(ts, (uint64_t)rc);
            if (rc != MOQ_OK && rc != MOQ_ERR_REQUEST_BLOCKED)
                return -1;
            if (rc == MOQ_OK) ts->rnd_retries++;
        }

        if (rc == MOQ_OK) {
            st->subs[slot].active = true;
            st->subs[slot].pending = true;
            st->subs[slot].client_handle = h;

            if (!defer_pump) {
                RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
                moq_event_t ev;
                if (moq_session_poll_events(sv, &ev, 1) == 1) {
                    if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                        st->subs[slot].server_handle = ev.u.subscribe_request.sub;
                        st->subs[slot].has_server_handle = true;
                    }
                    moq_event_cleanup(&ev);
                }
            }
        }
        break;
    }

    case RND_ACCEPT_SUB: {
        int idx = rnd_find_pending_sub(st);
        if (idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_subscribe(sv,
            st->subs[idx].server_handle, &acc, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_accept_subscribe(sv,
                st->subs[idx].server_handle, &acc, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->subs[idx].pending = false;
            st->subs[idx].accepted = true;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            moq_event_t ev;
            if (moq_session_poll_events(c, &ev, 1) == 1)
                moq_event_cleanup(&ev);
        }
        break;
    }

    case RND_REJECT_SUB: {
        int idx = rnd_find_pending_sub(st);
        if (idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        rej.reason = MOQ_BYTES_LITERAL("no");
        moq_result_t rc = moq_session_reject_subscribe(sv,
            st->subs[idx].server_handle, &rej, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_reject_subscribe(sv,
                st->subs[idx].server_handle, &rej, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->subs[idx].active = false;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
        }
        break;
    }

    case RND_PUB_NS: {
        int slot = -1;
        for (int i = 0; i < RND_MAX_ANNS; i++)
            if (!st->anns[i].active) { slot = i; break; }
        if (slot < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        char ns_name[16];
        snprintf(ns_name, sizeof(ns_name), "n%llu",
                 (unsigned long long)(rng_next(rng) % 100));
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t ns_parts[] = { moq_bytes_cstr(ns_name) };
        cfg.track_namespace.parts = ns_parts;
        cfg.track_namespace.count = 1;

        moq_announcement_t h;
        moq_result_t rc = moq_session_publish_namespace(sv, &cfg, 0, &h);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)rc);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_publish_namespace(sv, &cfg, 0, &h);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->anns[slot].active = true;
            st->anns[slot].pending = true;
            st->anns[slot].server_handle = h;
            ts->rnd_namespace_ops++;

            if (!defer_pump) {
                RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
                moq_event_t ev;
                if (moq_session_poll_events(c, &ev, 1) == 1) {
                    if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                        st->anns[slot].client_handle = ev.u.namespace_published.ann;
                        st->anns[slot].has_client_handle = true;
                    }
                    moq_event_cleanup(&ev);
                }
            }
        }
        break;
    }

    case RND_ACCEPT_NS: {
        int idx = rnd_find_pending_ann(st);
        if (idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_accept_namespace_cfg_t nacc;
        moq_accept_namespace_cfg_init(&nacc);
        moq_result_t rc = moq_session_accept_namespace(c,
            st->anns[idx].client_handle, &nacc, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
            rc = moq_session_accept_namespace(c,
                st->anns[idx].client_handle, &nacc, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->anns[idx].pending = false;
            st->anns[idx].accepted = true;
            ts->rnd_namespace_ops++;
            RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
        }
        break;
    }

    case RND_NS_DONE: {
        int idx = rnd_find_accepted_ann(st);
        if (idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_result_t rc = moq_session_publish_namespace_done(sv,
            st->anns[idx].server_handle, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_publish_namespace_done(sv,
                st->anns[idx].server_handle, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->anns[idx].active = false;
            ts->rnd_namespace_ops++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
        }
        break;
    }

    case RND_NS_CANCEL: {
        int idx = rnd_find_accepted_ann(st);
        if (idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }
        if (!st->anns[idx].has_client_handle) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_cancel_namespace_cfg_t cfg;
        moq_cancel_namespace_cfg_init(&cfg);
        cfg.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        moq_result_t rc = moq_session_cancel_namespace(c,
            st->anns[idx].client_handle, &cfg, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
            rc = moq_session_cancel_namespace(c,
                st->anns[idx].client_handle, &cfg, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->anns[idx].active = false;
            ts->rnd_namespace_ops++;
            RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
        }
        break;
    }

    case RND_OPEN_SG: {
        int sub_idx = rnd_find_accepted_sub(st);
        if (sub_idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }
        int sg_slot = -1;
        for (int i = 0; i < RND_MAX_SGS; i++)
            if (!st->sgs[i].active) { sg_slot = i; break; }
        if (sg_slot < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = st->next_group;
        cfg.subgroup_id = 0;
        cfg.publisher_priority = 128;
        moq_subgroup_handle_t h;
        moq_result_t rc = moq_session_open_subgroup(sv,
            st->subs[sub_idx].server_handle, &cfg, 0, &h);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, st->next_group);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_open_subgroup(sv,
                st->subs[sub_idx].server_handle, &cfg, 0, &h);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->sgs[sg_slot].active = true;
            st->sgs[sg_slot].handle = h;
            st->sgs[sg_slot].group_id = st->next_group;
            st->sgs[sg_slot].next_obj = 0;
            st->next_group++;
            if (!defer_pump) { RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts)); }
        }
        break;
    }

    case RND_WRITE_OBJ: {
        int sg_idx = rnd_find_open_sg(st);
        if (sg_idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        size_t plen = rng_next(rng) % 32;
        if (rng_next(rng) % 10 == 0) plen = 0;
        uint8_t pdata[32];
        for (size_t j = 0; j < plen; j++)
            pdata[j] = (uint8_t)(rng_next(rng) & 0xFF);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, pdata, plen, &p);
        if (!p) { rnd_log_op(log, RND_PUMP, 0); break; }

        uint64_t obj_id = st->sgs[sg_idx].next_obj;
        moq_result_t rc = moq_session_write_object(sv,
            st->sgs[sg_idx].handle, obj_id, p, 0);
        moq_rcbuf_decref(p);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, obj_id);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            p = NULL;
            moq_rcbuf_create(alloc, pdata, plen, &p);
            if (!p) break;
            rc = moq_session_write_object(sv,
                st->sgs[sg_idx].handle, obj_id, p, 0);
            moq_rcbuf_decref(p);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED) return -1;
        }

        if (rc == MOQ_OK) {
            st->sgs[sg_idx].next_obj++;
            ts->rnd_objects_sent++;
            hash_object(&ts->rnd_sent_hash,
                st->sgs[sg_idx].group_id, obj_id, plen, pdata);
            if (!defer_pump) { RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts)); }
        }
        break;
    }

    case RND_CLOSE_SG: {
        int sg_idx = rnd_find_open_sg(st);
        if (sg_idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_result_t rc = moq_session_close_subgroup(sv,
            st->sgs[sg_idx].handle, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)sg_idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_close_subgroup(sv,
                st->sgs[sg_idx].handle, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_STALE_HANDLE)
                return -1;
        }

        if (rc == MOQ_OK) {
            st->sgs[sg_idx].active = false;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
        }
        break;
    }

    case RND_RESET_SG: {
        int sg_idx = rnd_find_open_sg(st);
        if (sg_idx < 0) { rnd_log_op(log, RND_PUMP, 0); break; }

        moq_result_t rc = moq_session_reset_subgroup(sv,
            st->sgs[sg_idx].handle, 0x1, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)sg_idx);

        if (rc == MOQ_ERR_WOULD_BLOCK) {
            ts->rnd_wb_action++;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
            rc = moq_session_reset_subgroup(sv,
                st->sgs[sg_idx].handle, 0x1, 0);
            hash_op(ts, (uint64_t)rc);
            if (rc == MOQ_OK) ts->rnd_retries++;
            else if (rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_STALE_HANDLE)
                return -1;
        }

        if (rc == MOQ_OK) {
            st->sgs[sg_idx].active = false;
            RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
        }
        break;
    }

    case RND_PUMP:
        rnd_log_op(log, op, 0);
        RND_TRY_PUMP(rnd_pump_s_to_c(sv, c, ts));
        RND_TRY_PUMP(rnd_pump_c_to_s(c, sv, ts));
        break;

    case RND_DRAIN_C:
        rnd_log_op(log, op, 0);
        rnd_drain_client(c, ts);
        break;

    case RND_DRAIN_S: {
        rnd_log_op(log, op, 0);
        moq_event_t evts[4]; size_t ne;
        while ((ne = moq_session_poll_events(sv, evts, 4)) > 0)
            for (size_t i = 0; i < ne; i++)
                moq_event_cleanup(&evts[i]);
        break;
    }

    case RND_PROC_PEND_S: {
        drain_events(sv);
        moq_result_t rc = moq_session_process_pending(sv, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)rc);
        break;
    }

    case RND_PROC_PEND_C: {
        rnd_drain_client(c, ts);
        moq_result_t rc = moq_session_process_pending(c, 0);
        hash_op(ts, (uint64_t)rc);
        rnd_log_op(log, op, (uint64_t)rc);
        break;
    }

    default:
        rnd_log_op(log, RND_PUMP, 0);
        break;
    }

    return 0;
}

/* -- Random phase --------------------------------------------------- */

static int run_random_phase(uint64_t seed, moq_alloc_t *alloc,
                             run_summary_t *ts, int run_id, int max_steps)
{
    moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
    tiny.alloc = alloc;
    tiny.max_actions = 2;
    tiny.max_events = 3;
    tiny.output_scratch_size = 4096;
    tiny.max_subscriptions = 16;
    tiny.max_announcements = 8;
    tiny.max_open_subgroups = 4;
    tiny.send_buffer_size = 512;
    tiny.recv_buffer_size = 512;
    tiny.max_data_streams = 4;
    tiny.max_object_payload_size = 256;
    tiny.max_receive_buffer_bytes = 1024;

    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(alloc, 64, 64, &c, &sv, &tiny, &tiny);
    drain_both(c, sv);

    rnd_state_t st;
    memset(&st, 0, sizeof(st));
    rng_t rng = { seed };
    rnd_log_t log = {0};
    int failures = 0;

    for (int step = 0; step < max_steps && !st.closed; step++) {
        int src = rnd_execute_step(c, sv, &st, &rng, &log, ts, alloc);
        ts->rnd_ops++;

        if (src < 0) {
            fprintf(stderr, "FAIL seed=0x%llx run=%d rnd_step=%d: "
                    "unexpected retry failure\n",
                    (unsigned long long)seed, run_id, step);
            rnd_dump_log(seed, run_id, &log);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_backpressure\n",
                    (unsigned long long)seed, max_steps);
            failures++;
            break;
        }

        if (moq_session_state(c) == MOQ_SESS_CLOSED ||
            moq_session_state(sv) == MOQ_SESS_CLOSED) {
            if (!st.closed) {
                fprintf(stderr, "FAIL seed=0x%llx run=%d rnd_step=%d: "
                        "unexpected session close\n",
                        (unsigned long long)seed, run_id, step);
                rnd_dump_log(seed, run_id, &log);
                fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                        "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                        "./build/tests/test_scenario_backpressure\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
            st.closed = true;
            ts->rnd_closes++;
        }
    }

    /* Final pump + drain to flush any pending data. */
    if (!st.closed) {
        for (int f = 0; f < 8; f++) {
            if (rnd_pump_s_to_c(sv, c, ts) < 0 ||
                rnd_pump_c_to_s(c, sv, ts) < 0) {
                failures++;
                break;
            }
            rnd_drain_client(c, ts);
            drain_events(sv);
        }
    }

    drain_both(c, sv);
    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/* -- Run one scenario ----------------------------------------------- */

static int run_scenario(uint64_t seed, moq_alloc_t *alloc,
                         run_summary_t *summary, int run_id,
                         int max_steps)
{
    memset(summary, 0, sizeof(*summary));
    summary->hash = 0xCBF29CE484222325ULL;
    summary->rnd_sent_hash = 0xCBF29CE484222325ULL;
    summary->rnd_recv_hash = 0xCBF29CE484222325ULL;

    moq_session_cfg_t tiny = MOQ_SESSION_CFG_INIT;
    tiny.alloc = alloc;
    tiny.max_actions = 2;
    tiny.max_events = 3;
    tiny.output_scratch_size = 4096;
    tiny.max_subscriptions = 16;
    tiny.max_announcements = 8;
    tiny.max_open_subgroups = 4;
    tiny.send_buffer_size = 512;
    tiny.recv_buffer_size = 512;
    tiny.max_data_streams = 4;
    tiny.max_object_payload_size = 256;
    tiny.max_receive_buffer_bytes = 1024;

    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(alloc, 64, 64, &c, &sv, &tiny, &tiny);

    drain_both(c, sv);

    int failures = 0;
    int prelude_rc = run_prelude(c, sv, summary, alloc);
    if (prelude_rc < 0) {
        fprintf(stderr, "FAIL seed=0x%llx run=%d: prelude failed\n",
                (unsigned long long)seed, run_id);
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "./build/tests/test_scenario_backpressure\n",
                (unsigned long long)seed, max_steps);
        failures++;
    }

    drain_both(c, sv);
    moq_session_destroy(c);
    moq_session_destroy(sv);

    if (prelude_rc >= 0)
        failures += run_random_phase(seed, alloc, summary, run_id,
                                      max_steps);

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

    size_t total_accept_wb = 0, total_reject_wb = 0;
    size_t total_namespace_wb = 0;
    size_t total_control_wb = 0, total_object_wb = 0;
    size_t total_retry = 0;
    size_t total_rnd_ops = 0, total_rnd_wb_action = 0;
    size_t total_rnd_wb_ctrl = 0, total_rnd_wb_data = 0;
    size_t total_rnd_retries = 0;
    size_t total_rnd_obj_sent = 0, total_rnd_obj_recv = 0;
    size_t total_rnd_ns_ops = 0;

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
                        "./build/tests/test_scenario_backpressure\n",
                        (unsigned long long)seed, max_steps);
                failures++;
            }
        }

        total_accept_wb += sums[0].accept_wb;
        total_reject_wb += sums[0].reject_wb;
        total_namespace_wb += sums[0].namespace_wb;
        total_control_wb += sums[0].control_event_wb;
        total_object_wb += sums[0].object_event_wb;
        total_retry += sums[0].retry_success;

        if (sums[0].hash != sums[1].hash ||
            sums[0].ops != sums[1].ops ||
            sums[0].accept_wb != sums[1].accept_wb ||
            sums[0].reject_wb != sums[1].reject_wb ||
            sums[0].namespace_wb != sums[1].namespace_wb ||
            sums[0].control_event_wb != sums[1].control_event_wb ||
            sums[0].object_event_wb != sums[1].object_event_wb ||
            sums[0].retry_success != sums[1].retry_success ||
            sums[0].rnd_ops != sums[1].rnd_ops ||
            sums[0].rnd_wb_action != sums[1].rnd_wb_action ||
            sums[0].rnd_wb_control_input != sums[1].rnd_wb_control_input ||
            sums[0].rnd_wb_data_input != sums[1].rnd_wb_data_input ||
            sums[0].rnd_retries != sums[1].rnd_retries ||
            sums[0].rnd_objects_sent != sums[1].rnd_objects_sent ||
            sums[0].rnd_objects_received != sums[1].rnd_objects_received ||
            sums[0].rnd_namespace_ops != sums[1].rnd_namespace_ops ||
            sums[0].rnd_closes != sums[1].rnd_closes ||
            sums[0].rnd_sent_hash != sums[1].rnd_sent_hash ||
            sums[0].rnd_recv_hash != sums[1].rnd_recv_hash) {
            fprintf(stderr, "FAIL seed=0x%llx: counter/hash mismatch\n",
                    (unsigned long long)seed);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_backpressure\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        if (sums[0].rnd_objects_received > sums[0].rnd_objects_sent) {
            fprintf(stderr, "FAIL seed=0x%llx: phantom objects "
                    "sent=%zu received=%zu\n",
                    (unsigned long long)seed,
                    sums[0].rnd_objects_sent,
                    sums[0].rnd_objects_received);
            fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                    "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                    "./build/tests/test_scenario_backpressure\n",
                    (unsigned long long)seed, max_steps);
            failures++;
        }

        total_rnd_ops += sums[0].rnd_ops;
        total_rnd_wb_action += sums[0].rnd_wb_action;
        total_rnd_wb_ctrl += sums[0].rnd_wb_control_input;
        total_rnd_wb_data += sums[0].rnd_wb_data_input;
        total_rnd_retries += sums[0].rnd_retries;
        total_rnd_obj_sent += sums[0].rnd_objects_sent;
        total_rnd_obj_recv += sums[0].rnd_objects_received;
        total_rnd_ns_ops += sums[0].rnd_namespace_ops;
    }

    if (num_seeds >= 1) {
        if (total_accept_wb == 0) {
            fprintf(stderr, "FAIL: no accept_subscribe WOULD_BLOCK\n");
            failures++;
        }
        if (total_reject_wb == 0) {
            fprintf(stderr, "FAIL: no reject_subscribe WOULD_BLOCK\n");
            failures++;
        }
        if (total_namespace_wb == 0) {
            fprintf(stderr, "FAIL: no namespace accept WOULD_BLOCK\n");
            failures++;
        }
        if (total_control_wb == 0) {
            fprintf(stderr, "FAIL: no control input WOULD_BLOCK\n");
            failures++;
        }
        if (total_object_wb == 0) {
            fprintf(stderr, "FAIL: no object event WOULD_BLOCK\n");
            failures++;
        }
        if (total_retry == 0) {
            fprintf(stderr, "FAIL: no retry_success\n");
            failures++;
        }
        size_t expected_retry = total_accept_wb + total_reject_wb +
            total_namespace_wb + total_control_wb + total_object_wb;
        if (total_retry != expected_retry) {
            fprintf(stderr, "FAIL: retry=%zu != expected=%zu\n",
                    total_retry, expected_retry);
            failures++;
        }
    }

    if (num_seeds >= 10) {
        if (total_rnd_wb_action == 0) {
            fprintf(stderr, "FAIL: no rnd_wb_action\n");
            failures++;
        }
        if (total_rnd_wb_ctrl == 0) {
            fprintf(stderr, "FAIL: no rnd_wb_control_input\n");
            failures++;
        }
        if (total_rnd_wb_data == 0) {
            fprintf(stderr, "FAIL: no rnd_wb_data_input\n");
            failures++;
        }
    }

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_backpressure "
                "(%llu seeds, accept_wb=%zu reject_wb=%zu "
                "ns_wb=%zu ctrl_wb=%zu obj_wb=%zu retry=%zu "
                "rnd_ops=%zu rnd_wb_act=%zu rnd_wb_ctrl=%zu "
                "rnd_wb_data=%zu rnd_retry=%zu "
                "rnd_obj=%zu/%zu rnd_ns=%zu)\n",
                (unsigned long long)num_seeds,
                total_accept_wb, total_reject_wb,
                total_namespace_wb, total_control_wb,
                total_object_wb, total_retry,
                total_rnd_ops, total_rnd_wb_action,
                total_rnd_wb_ctrl, total_rnd_wb_data,
                total_rnd_retries,
                total_rnd_obj_sent, total_rnd_obj_recv,
                total_rnd_ns_ops);
    else
        fprintf(stderr, "FAIL: test_scenario_backpressure "
                "(%d failures in %llu seeds)\n",
                failures, (unsigned long long)num_seeds);

    return failures;
}
