/*
 * Loopback smoke: one publisher through the relay to two subscribers over
 * real MsQuic on localhost, draft-16 and draft-18. Correctness only —
 * exact object bytes and counts; no timing claims. Peers are managed
 * MsQuic clients whose logic runs as small state machines inside their
 * own on_lane_pump; the relay runs the production binding inside the
 * server's lane pump (single lane, one binding, many sessions — the
 * exact shape cli/main.c ships). The MsQuic listener is exact-version
 * (one ALPN), so each case runs its own server+peers on ONE draft; both
 * drafts are covered as separate cases, never mixed on one listener.
 *
 * The streaming cases prove chunk-through with live-edge delivery
 * end-to-end: the publisher HOLDS one object half-written until a
 * chunk-receiving subscriber has already seen its first slice through
 * the relay — impossible unless the relay forwards chunks before the
 * object completes. Cross-thread signaling is a handful of C11 atomics;
 * the main thread only correlates completion flags with a generous
 * deadline.
 */

#include <moq/relay/moqr_bind.h>
#include "../cli/config.h"
#include "../cli/conn_reap.h"
#include "drain_wait.h"
#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/relay/moqr_obs.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>

#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#define OBJECTS 3          /* objects 0,1 whole; object 2 chunked/held */
#define SMALL_SIZE 64
#define BIG_SIZE 128       /* object 2: two 64-byte halves */

static int g_failures;

#define L_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

static uint8_t
obj_fill(uint64_t object_id)
{
    return (uint8_t)(0xB0 + object_id);
}

/* -- relay side ---------------------------------------------------------- */

typedef struct relay_ctx {
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;
    /* Terminal-acknowledgment observation. `stall` starves the binding pump so
     * a queued SESSION_CLOSED stays unconsumed; the counters record what the
     * reap pass saw, since an acknowledgment is only valid inside the pump. */
    atomic_int    stall;
    atomic_int    ack_ok;
    atomic_int    ack_not_ready;
    atomic_int    ack_unexpected;
    /* Each fail-closed operand, probed once from inside the owning pump —
     * the only place a lane handle is valid at all. */
    atomic_int    probed;
    atomic_int    null_bind_failed_closed;
    atomic_int    null_lane_failed_closed;
} relay_ctx_t;

/* Per-connection relay state is a TAG in the adapter's conn_user slot — the
 * same discipline as cli/main.c (no pointer-keyed maps; conn/session pointer
 * values can be reused by a successor connection). */
#define RELAY_CONN_OPENED MOQR_CONN_OPENED
#define RELAY_CONN_DEAD   MOQR_CONN_DEAD


static int
relay_lane_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                uint64_t now_us, void *vctx)
{
    relay_ctx_t *ctx = vctx;
    (void)m;
    if (moq_msquic_lane_index(lane) != 0) {
        return 0;   /* single-lane relay: only lane 0 carries connections */
    }
    moqr_reap_stats_t st = { 0, 0 };
    /* Probe each fail-closed operand once, from the only context where a lane
     * handle exists: a missing binding and a missing lane must each be refused
     * on its own, not only when both are absent. */
    if (atomic_exchange(&ctx->probed, 1) == 0) {
        if (!moqr_relay_reap_pass(NULL, lane, NULL)) {
            atomic_store(&ctx->null_bind_failed_closed, 1);
        }
        if (!moqr_relay_reap_pass(ctx->bind, NULL, NULL)) {
            atomic_store(&ctx->null_lane_failed_closed, 1);
        }
    }
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane,
                                                                     NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == RELAY_CONN_DEAD) {
            continue;
        }
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(ctx->bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_OPENED);
            } else {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    /* A starved binding pump leaves a queued SESSION_CLOSED unconsumed, which
     * is the bounded-consumer shape the retention contract must survive. */
    if (atomic_load(&ctx->stall) == 0) {
        (void)moqr_bind_pump(ctx->bind, now_us);
    }
    if (!moqr_relay_reap_pass(ctx->bind, lane, &st)) {
        atomic_fetch_add(&ctx->ack_unexpected, 1);
        return 1;
    }
    atomic_fetch_add(&ctx->ack_ok, (int)st.acked);
    atomic_fetch_add(&ctx->ack_not_ready, (int)st.retained);
    return 0;
}

/* -- publisher peer -------------------------------------------------------- */

typedef struct pub_ctx {
    atomic_int phase;      /* 0=announce 1=await-sub 2=holding 3=done
                            * (+do_reset: 4=reset-held 5=reset-sent)       */
    atomic_int release;    /* main thread: finish the held object          */
    atomic_int reset_go;   /* main thread: fire the begun-object reset     */
    atomic_int established; /* set once the session reaches ESTABLISHED     */
    atomic_int ns_cancelled;   /* NAMESPACE_CANCELLED count                */
    atomic_uint_least64_t ns_cancel_code;
    atomic_int fatal;
    bool       live_edge;  /* hold object 2 half-written until release     */
    bool       do_reset;   /* after the data, begin group 2 and RESET it   */
    bool       drip;       /* after the data, keep writing one small whole
                            * object per pump (group 3, ids from 3) — the
                            * organic activity that keeps every lane pumping
                            * so a revocation's recheck always runs; stops
                            * on the first refused write (post-withdrawal) */
    uint64_t   drip_oid;   /* lane-thread only                             */
    moq_subscription_t up_sub;
    moq_subgroup_handle_t sgh;
    moq_subgroup_handle_t sgh2;
} pub_ctx_t;

/* The 62-bit begun-object reset code (> UINT32_MAX, < 2^62). */
#define LB_WIDE_RESET 0x23456789ABCDEFull

static void
pub_write_whole(moq_session_t *s, moq_subgroup_handle_t sgh, uint64_t oid,
                size_t size, uint64_t now_us, atomic_int *fatal)
{
    uint8_t body[BIG_SIZE];
    memset(body, obj_fill(oid), size);
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, size, &pl) != MOQ_OK ||
        moq_session_write_object(s, sgh, oid, pl, now_us) != MOQ_OK) {
        atomic_store(fatal, 1);
    }
    if (pl != NULL) {
        moq_rcbuf_decref(pl);
    }
}

static bool
pub_write_half(moq_session_t *s, moq_subgroup_handle_t sgh, uint64_t now_us)
{
    uint8_t body[SMALL_SIZE];
    memset(body, obj_fill(2), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) !=
        MOQ_OK) {
        return false;
    }
    bool ok = moq_session_write_object_data(s, sgh, pl, now_us) == MOQ_OK;
    moq_rcbuf_decref(pl);
    return ok;
}

static int
pub_on_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
            uint64_t now_us, void *vctx)
{
    pub_ctx_t *ctx = vctx;
    (void)lane;
    moq_session_t *s = moq_msquic_managed_session(m);
    if (s == NULL || moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        return 0;
    }
    atomic_store(&ctx->established, 1);
    if (atomic_load(&ctx->phase) == 0) {
        moq_bytes_t nsp[2] = { MOQ_BYTES_LITERAL("loop"),
                               MOQ_BYTES_LITERAL("cam") };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        if (moq_session_publish_namespace(s, &pcfg, now_us, &ann) ==
            MOQ_OK) {
            atomic_store(&ctx->phase, 1);
        }
        return 0;
    }
    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(s, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (evs[e].kind == MOQ_EVENT_NAMESPACE_CANCELLED) {
                atomic_store(&ctx->ns_cancel_code,
                             evs[e].u.namespace_cancelled.error_code);
                atomic_fetch_add(&ctx->ns_cancelled, 1);
            }
            if (evs[e].kind == MOQ_EVENT_UNSUBSCRIBED && ctx->drip) {
                atomic_store(&ctx->phase, 6);   /* withdrawn: stop the drip */
            }
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                atomic_load(&ctx->phase) == 1) {
                /* The relay's upstream subscription: accept and serve. */
                moq_accept_subscribe_cfg_t cfg;
                memset(&cfg, 0, sizeof(cfg));
                moq_accept_subscribe_cfg_init(&cfg);
                if (moq_session_accept_subscribe(
                        s, evs[e].u.subscribe_request.sub, &cfg, now_us) !=
                    MOQ_OK) {
                    atomic_store(&ctx->fatal, 1);
                    moq_event_cleanup(&evs[e]);
                    continue;
                }
                ctx->up_sub = evs[e].u.subscribe_request.sub;
                moq_subgroup_cfg_t sgc;
                memset(&sgc, 0, sizeof(sgc));
                moq_subgroup_cfg_init(&sgc);
                sgc.group_id = 1;
                sgc.publisher_priority = 100;
                if (moq_session_open_subgroup(s, ctx->up_sub, &sgc, now_us,
                                              &ctx->sgh) != MOQ_OK) {
                    atomic_store(&ctx->fatal, 1);
                    moq_event_cleanup(&evs[e]);
                    continue;
                }
                pub_write_whole(s, ctx->sgh, 0, SMALL_SIZE, now_us,
                                &ctx->fatal);
                pub_write_whole(s, ctx->sgh, 1, SMALL_SIZE, now_us,
                                &ctx->fatal);
                if (ctx->live_edge) {
                    /* Object 2: declare BIG_SIZE, ship only the first half,
                     * then HOLD. The relay must forward that half live for
                     * the flow to ever complete (main releases the rest only
                     * after a subscriber saw a chunk of it). */
                    if (moq_session_begin_object(s, ctx->sgh, 2, BIG_SIZE,
                                                 now_us) != MOQ_OK ||
                        !pub_write_half(s, ctx->sgh, now_us)) {
                        atomic_store(&ctx->fatal, 1);
                    }
                    atomic_store(&ctx->phase, 2);
                } else {
                    pub_write_whole(s, ctx->sgh, 2, BIG_SIZE, now_us,
                                    &ctx->fatal);
                    (void)moq_session_close_subgroup(s, ctx->sgh, now_us);
                    atomic_store(&ctx->phase, 3);
                }
            }
            moq_event_cleanup(&evs[e]);
        }
    }
    if (atomic_load(&ctx->phase) == 2 && atomic_load(&ctx->release)) {
        if (!pub_write_half(s, ctx->sgh, now_us) ||
            moq_session_end_object(s, ctx->sgh, now_us) != MOQ_OK) {
            atomic_store(&ctx->fatal, 1);
        }
        (void)moq_session_close_subgroup(s, ctx->sgh, now_us);
        atomic_store(&ctx->phase, 3);
    }
    if (ctx->drip && atomic_load(&ctx->phase) == 3) {
        /* Open the drip subgroup once. */
        moq_subgroup_cfg_t sgc;
        memset(&sgc, 0, sizeof(sgc));
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 3;
        sgc.publisher_priority = 100;
        if (moq_session_open_subgroup(s, ctx->up_sub, &sgc, now_us,
                                      &ctx->sgh2) != MOQ_OK) {
            atomic_store(&ctx->phase, 6);
        } else {
            ctx->drip_oid = 3;   /* never 2: the size oracle expects BIG
                                  * only for object id 2 */
            atomic_store(&ctx->phase, 7);
        }
    }
    if (ctx->drip && atomic_load(&ctx->phase) == 7) {
        uint8_t body[SMALL_SIZE];
        memset(body, obj_fill(ctx->drip_oid), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) !=
                MOQ_OK ||
            moq_session_write_object(s, ctx->sgh2, ctx->drip_oid, pl,
                                     now_us) != MOQ_OK) {
            atomic_store(&ctx->phase, 6);   /* refused: withdrawn upstream */
        } else {
            ctx->drip_oid++;
            if (ctx->drip_oid > 900) {
                atomic_store(&ctx->phase, 6);   /* bounded */
            }
        }
        if (pl != NULL) {
            moq_rcbuf_decref(pl);
        }
    }
    if (ctx->do_reset && atomic_load(&ctx->phase) == 3) {
        /* Group 2: begin an object, ship half, and HOLD — the reset must
         * land on an object the subscriber has BEGUN receiving. */
        moq_subgroup_cfg_t sgc;
        memset(&sgc, 0, sizeof(sgc));
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 2;
        sgc.publisher_priority = 100;
        if (moq_session_open_subgroup(s, ctx->up_sub, &sgc, now_us,
                                      &ctx->sgh2) != MOQ_OK ||
            moq_session_begin_object(s, ctx->sgh2, 0, BIG_SIZE, now_us) !=
                MOQ_OK ||
            !pub_write_half(s, ctx->sgh2, now_us)) {
            atomic_store(&ctx->fatal, 1);
        }
        atomic_store(&ctx->phase, 4);
    }
    if (ctx->do_reset && atomic_load(&ctx->phase) == 4 &&
        atomic_load(&ctx->reset_go)) {
        if (moq_session_reset_subgroup(s, ctx->sgh2, LB_WIDE_RESET,
                                       now_us) != MOQ_OK) {
            atomic_store(&ctx->fatal, 1);
        }
        atomic_store(&ctx->phase, 5);
    }
    return 0;
}

/* -- subscriber peer --------------------------------------------------------- */

typedef struct sub_ctx {
    atomic_int subscribed;
    atomic_int received;      /* completed objects (either receive mode)   */
    atomic_int byte_errors;
    atomic_int obj2_chunk;    /* chunk client saw a slice of object 2      */
    atomic_int g2_chunk;      /* chunk client began group 2's reset target */
    atomic_int reset_seen;    /* end events carrying TERMINAL_RESET        */
    atomic_uint_least64_t reset_code_seen; /* oc->error_code at that end   */
    atomic_int sg_finished;   /* MOQ_EVENT_SUBGROUP_FINISHED count          */
    atomic_int sg_fin_bad;    /* FINISHED events with the wrong identity   */
    atomic_int rejects;       /* SUBSCRIBE_ERROR count (retried, bounded)  */
    atomic_int last_reject_code; /* error_code of the most recent reject   */
    atomic_int done_seen;     /* SUBSCRIBE_DONE count                      */
    atomic_uint_least64_t done_code;
    atomic_int established;   /* set once the session reaches ESTABLISHED  */
    atomic_int fatal;
    bool       chunked;       /* this peer receives OBJECT_CHUNK slices    */
    uint64_t   cur_bytes;     /* lane-thread only: current object progress */
    uint64_t   cur_group;     /* lane-thread only: current object's group  */
} sub_ctx_t;

static int
sub_on_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
            uint64_t now_us, void *vctx)
{
    sub_ctx_t *ctx = vctx;
    (void)lane;
    moq_session_t *s = moq_msquic_managed_session(m);
    if (s == NULL || moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        return 0;
    }
    atomic_store(&ctx->established, 1);
    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(s, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (evs[e].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_object_received_event_t *ob =
                    &evs[e].u.object_received;
                size_t want = ob->object_id == 2 ? BIG_SIZE : SMALL_SIZE;
                if (ob->payload == NULL ||
                    moq_rcbuf_len(ob->payload) != want ||
                    moq_rcbuf_data(ob->payload)[0] !=
                        obj_fill(ob->object_id)) {
                    atomic_fetch_add(&ctx->byte_errors, 1);
                }
                atomic_fetch_add(&ctx->received, 1);
            } else if (evs[e].kind == MOQ_EVENT_OBJECT_CHUNK) {
                /* Streaming receive: verify every slice byte against the
                 * object's fill, track completion by end+NORMAL. */
                const moq_object_chunk_event_t *oc = &evs[e].u.object_chunk;
                if (oc->begin) {
                    ctx->cur_bytes = 0;
                    ctx->cur_group = oc->group_id;
                }
                if (oc->chunk != NULL) {
                    size_t len = moq_rcbuf_len(oc->chunk);
                    const uint8_t *data = moq_rcbuf_data(oc->chunk);
                    /* Group 2 is the reset target: its halves carry the
                     * writer's fixed fill (obj_fill(2)), not the id fill. */
                    uint8_t want = ctx->cur_group == 2 ? obj_fill(2)
                                                       : obj_fill(
                                                             oc->object_id);
                    for (size_t i = 0; i < len; i++) {
                        if (data[i] != want) {
                            atomic_fetch_add(&ctx->byte_errors, 1);
                            break;
                        }
                    }
                    ctx->cur_bytes += len;
                }
                if (oc->object_id == 2) {
                    atomic_store(&ctx->obj2_chunk, 1);
                }
                if (oc->begin && oc->group_id == 2) {
                    atomic_store(&ctx->g2_chunk, 1);
                }
                if (oc->end) {
                    if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                        /* The begun-object reset: a terminal, never a
                         * completed delivery. The peer's exact 62-bit code
                         * rides the event. */
                        atomic_store(&ctx->reset_code_seen, oc->error_code);
                        atomic_fetch_add(&ctx->reset_seen, 1);
                    } else {
                        if (oc->terminal != MOQ_OBJECT_TERMINAL_NORMAL ||
                            ctx->cur_bytes !=
                                (oc->object_id == 2 ? BIG_SIZE
                                                    : SMALL_SIZE)) {
                            atomic_fetch_add(&ctx->byte_errors, 1);
                        }
                        atomic_fetch_add(&ctx->received, 1);
                    }
                }
            } else if (evs[e].kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                /* The actual downstream FIN — the graceful subgroup
                 * terminal, distinct from any object's end. */
                const moq_subgroup_finished_event_t *sf =
                    &evs[e].u.subgroup_finished;
                if (sf->group_id != 1) {
                    atomic_fetch_add(&ctx->sg_fin_bad, 1);
                }
                atomic_fetch_add(&ctx->sg_finished, 1);
            } else if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                atomic_store(&ctx->done_code,
                             evs[e].u.subscribe_done.status_code);
                atomic_fetch_add(&ctx->done_seen, 1);
            } else if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
                /* The relay fails closed on a namespace nobody has announced
                 * yet; a subscriber that raced the publisher's announce just
                 * retries (the reject arrived as inbound activity, so this
                 * pump — and the retry below — keep the loop self-driving).
                 * The final code stabilizes (a cross-lane subscriber to a
                 * mirror-only namespace ends at NOT_SUPPORTED). */
                atomic_fetch_add(&ctx->rejects, 1);
                atomic_store(&ctx->last_reject_code,
                             (int)evs[e].u.subscribe_error.error_code);
                atomic_store(&ctx->subscribed, 0);
            }
            moq_event_cleanup(&evs[e]);
        }
    }
    if (!atomic_load(&ctx->subscribed)) {
        moq_bytes_t nsp[2] = { MOQ_BYTES_LITERAL("loop"),
                               MOQ_BYTES_LITERAL("cam") };
        moq_subscribe_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        cfg.track_name = MOQ_BYTES_LITERAL("video");
        cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        if (moq_session_subscribe(s, &cfg, now_us, &h) == MOQ_OK) {
            atomic_store(&ctx->subscribed, 1);
        }
    }
    return 0;
}

/* -- one version run ----------------------------------------------------------- */

static const char *g_cert;
static const char *g_key;

static moq_msquic_managed_t *
mk_client(moq_version_t version, uint16_t port, bool streaming,
          moq_msquic_lane_pump_fn pump, void *ctx)
{
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 1024;
    cfg.version = version;
    cfg.streaming_objects = streaming;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = ctx;
    moq_msquic_managed_t *t = NULL;
    if (moq_msquic_managed_create(&cfg, &t) != MOQ_OK) {
        return NULL;
    }
    return t;
}

/* streaming=true is the production relay mode (chunk-through ingest with
 * live-edge forwarding, proven by the held object); false is the whole-object
 * receive regression. sub1 receives OBJECT_CHUNK slices in streaming mode. */
static int
run_case(moq_version_t version, const char *alpn, bool streaming)
{
    int before = g_failures;

    relay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    moqr_core_relay_cfg_t core_cfg;
    moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg),
                                   moq_alloc_default());
    L_CHECK(moqr_trace_create(moq_alloc_default(), 256, &rctx.trace) ==
            MOQR_OK);
    core_cfg.trace = rctx.trace;
    L_CHECK(moqr_core_create(&core_cfg, &rctx.core) == MOQR_OK);
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = rctx.core;   /* defaulted: max_conns resolves from core limits */
    L_CHECK(moqr_bind_create(&bcfg, &rctx.bind) == MOQR_OK);

    /* Production shape: the transport admission cap tracks the binding's own
     * resolved connection table, both read from the core's max_bindings. */
    moqr_core_limits_t lim;
    moqr_core_get_limits(rctx.core, &lim);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;   /* ephemeral */
    scfg.cert_path = g_cert;
    scfg.key_path = g_key;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = version;
    scfg.lane_count = 1;                       /* the production single-lane shape */
    scfg.max_connections = lim.max_bindings;   /* == the binding's resolved cap */
    scfg.streaming_objects = streaming;
    scfg.on_lane_pump = relay_lane_pump;
    scfg.on_lane_pump_user = &rctx;
    moq_msquic_managed_t *relay = NULL;
    L_CHECK(moq_msquic_managed_create(&scfg, &relay) == MOQ_OK);
    if (relay == NULL) {
        moqr_bind_destroy(rctx.bind);
        moqr_core_destroy(rctx.core);
        moqr_trace_destroy(rctx.trace);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    pub_ctx_t pctx;
    memset(&pctx, 0, sizeof(pctx));
    pctx.live_edge = streaming;
    sub_ctx_t s1ctx, s2ctx;
    memset(&s1ctx, 0, sizeof(s1ctx));
    memset(&s2ctx, 0, sizeof(s2ctx));
    s1ctx.chunked = streaming;   /* sub1 renders chunks in streaming mode */
    moq_msquic_managed_t *pub =
        mk_client(version, port, false, pub_on_pump, &pctx);
    moq_msquic_managed_t *sub1 =
        mk_client(version, port, s1ctx.chunked, sub_on_pump, &s1ctx);
    moq_msquic_managed_t *sub2 =
        mk_client(version, port, false, sub_on_pump, &s2ctx);
    L_CHECK(pub != NULL && sub1 != NULL && sub2 != NULL);

    /* Generous, bounded wait: correctness only, no timing claims. The main
     * thread is also the live-edge coordinator: it releases the held second
     * half of object 2 ONLY once sub1 saw a chunk of it — so a completed flow
     * proves the relay forwarded the first half while the publisher held. */
    bool released_after_live_chunk = false;
    for (int waited = 0; waited < 300; waited++) {   /* <= 30 s */
        if (streaming && !released_after_live_chunk &&
            atomic_load(&s1ctx.obj2_chunk)) {
            released_after_live_chunk = true;
            atomic_store(&pctx.release, 1);
            /* App work queued for a lane must WAKE that lane: the idle
             * publisher has no inbound traffic, so without this wake the
             * release is never observed and the flow stalls. */
            (void)moq_msquic_managed_wake(pub);
        }
        if (atomic_load(&s1ctx.received) >= OBJECTS &&
            atomic_load(&s2ctx.received) >= OBJECTS) {
            break;
        }
        if (moq_msquic_managed_is_fatal(relay) ||
            (pub != NULL && moq_msquic_managed_is_fatal(pub))) {
            break;
        }
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }

    if (atomic_load(&s1ctx.received) != OBJECTS ||
        atomic_load(&s2ctx.received) != OBJECTS) {
        /* Diagnostic context for a short count (kept: failure-path only). */
        fprintf(stderr,
                "loopback %s: s1 recv=%d sub=%d fatal=%d | s2 recv=%d sub=%d "
                "fatal=%d | pub phase=%d fatal=%d\n",
                alpn, atomic_load(&s1ctx.received),
                atomic_load(&s1ctx.subscribed), atomic_load(&s1ctx.fatal),
                atomic_load(&s2ctx.received), atomic_load(&s2ctx.subscribed),
                atomic_load(&s2ctx.fatal), atomic_load(&pctx.phase),
                atomic_load(&pctx.fatal));
    }
    L_CHECK(atomic_load(&s1ctx.received) == OBJECTS);
    L_CHECK(atomic_load(&s2ctx.received) == OBJECTS);
    L_CHECK(atomic_load(&s1ctx.byte_errors) == 0);
    L_CHECK(atomic_load(&s2ctx.byte_errors) == 0);
    L_CHECK(atomic_load(&s1ctx.fatal) == 0);
    L_CHECK(atomic_load(&s2ctx.fatal) == 0);
    L_CHECK(atomic_load(&pctx.fatal) == 0);
    L_CHECK(!moq_msquic_managed_is_fatal(relay));
    if (streaming) {
        /* Live edge: the held object's first half reached sub1 before the
         * publisher was allowed to finish it. */
        L_CHECK(released_after_live_chunk);
        L_CHECK(atomic_load(&s1ctx.obj2_chunk) == 1);
    }

    if (sub1 != NULL) {
        (void)moq_msquic_managed_stop(sub1);
        moq_msquic_managed_destroy(sub1);
    }
    if (sub2 != NULL) {
        (void)moq_msquic_managed_stop(sub2);
        moq_msquic_managed_destroy(sub2);
    }
    if (pub != NULL) {
        (void)moq_msquic_managed_stop(pub);
        moq_msquic_managed_destroy(pub);
    }
    /* Terminal-before-reap: every closing peer must be observed by the lane
     * pump (SESSION_CLOSED -> the binding detaches) before the adapter reaps
     * it, so the binding drains to zero connections BEFORE the relay stops. */
    {
        moqr_drain_result_t dr = moqr_drain_to_count(relay, 0, 200);
        if (dr == MOQR_DRAIN_CLOSED) {
            printf("FAIL: facade CLOSED during drain — a lane pump fail-stop "
                   "or stop, not a stall\n");
            g_failures++;
        }
    }
    if (moq_msquic_managed_conn_count(relay) != 0) {
    }
    L_CHECK(moq_msquic_managed_conn_count(relay) == 0);

    /* Server-outlives-its-connections (the contract the production serve loop
     * relies on). Every connection has now reached terminal and been reaped,
     * so the first-accepted connection has latched the facade's per-connection
     * convenience terminal (is_closed on a clean close, is_fatal on an abort) —
     * a naive `while (!is_fatal/!is_closed)` server loop would exit HERE. But
     * that is NOT a server lifetime signal: wait() must still not report the
     * facade terminal, because the listener outlives every connection. The
     * production loop breaks only on wait()==MOQ_ERR_CLOSED (stop / pump-exit),
     * never on is_fatal — so one bad client cannot take the relay down. */
    L_CHECK(moq_msquic_managed_is_closed(relay) ||
            moq_msquic_managed_is_fatal(relay));
    L_CHECK(moq_msquic_managed_wait(relay, 0) != MOQ_ERR_CLOSED);
    (void)moq_msquic_managed_stop(relay);

    /* The lane thread is joined, so the core and trace are safe to read
     * from this thread. Render the observability dumps against the real
     * post-flow relay state — the exact functions the SIGUSR path invokes —
     * proving they work on a live relay, not only hand-built stats. */
    {
        static char dbuf[65536];
        size_t w = 0;
        moqr_core_stats_t cs;
        moqr_core_get_stats(rctx.core, &cs);
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(rctx.bind, &bs);
        L_CHECK(cs.ingested_total > 0);        /* objects really flowed  */
        L_CHECK(cs.delivered_total > 0);       /* ... and were delivered */
        /* One lane-level binding served every accepted conn (pub + 2 subs)
         * and emptied via SESSION_CLOSED before the adapter reaped them. */
        L_CHECK(bs.conns == 0);

        moqr_obs_labels_t lb = { 0, "msquic", alpn };
        L_CHECK(moqr_metrics_write_prometheus(&cs, &bs, &lb, dbuf,
                                              sizeof(dbuf), &w) == MOQR_OK);
        L_CHECK(strstr(dbuf, "moqrelay_objects_ingested_total{shard=\"0\","
                             "transport=\"msquic\"") != NULL);
        /* binding-tier series appear only when bind stats are passed */
        L_CHECK(strstr(dbuf, "moqrelay_connections{") != NULL);
        /* Forward-latency observed real deliveries over the wire. */
        L_CHECK(bs.forward_latency.count > 0);
        L_CHECK(strstr(dbuf,
                       "moqrelay_forward_latency_seconds_count{") != NULL);

        L_CHECK(moqr_core_route_dump_text(rctx.core, dbuf, sizeof(dbuf),
                                          &w) == MOQR_OK);
        L_CHECK(strstr(dbuf, "epochs: node=0") != NULL);   /* text header */

        L_CHECK(moqr_trace_write_jsonl(rctx.trace, dbuf, sizeof(dbuf),
                                       &w) == MOQR_OK);
        L_CHECK(w > 0);                        /* the flow left a trace  */
        L_CHECK(strstr(dbuf, "\"kind\":") != NULL);
    }

    /* Bindings/core teardown after the lane thread is joined. */
    moqr_bind_destroy(rctx.bind);
    moqr_core_destroy(rctx.core);
    moqr_trace_destroy(rctx.trace);
    moq_msquic_managed_destroy(relay);

    if (g_failures == before) {
        printf("PASS: loopback %s %s (2 subscribers x %d objects)\n", alpn,
               streaming ? "streaming" : "whole-object/legacy", OBJECTS);
    }
    return g_failures - before;
}

/* -- lanes > 1: lane i drives shard i (the exact cli/main.c multi-lane shape) - */

/* Deterministic accept placement: the main thread sets this before each
 * sequential connect and waits for that peer to establish, so the accept that
 * reads it belongs to exactly that peer. */
static atomic_uint g_place_lane;

static uint32_t
choose_lane_fixed(moq_msquic_managed_t *m, const moq_msquic_accept_info_t *info,
                  void *user)
{
    (void)m;
    (void)info;
    (void)user;
    return atomic_load(&g_place_lane);
}

typedef struct relay_lanes_ctx {
    moqr_shards_t *shards;
    uint32_t       lanes;
    atomic_long    pump_calls[2];     /* lane pump turns, per shard        */
    atomic_long    producer_wakes;    /* shard-1 step masks naming shard 0:
                                       * the producer-credit wake observed
                                       * through the returned mask         */
} relay_lanes_ctx_t;

static int
relay_lanes_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                 uint64_t now_us, void *vctx)
{
    relay_lanes_ctx_t *ctx = vctx;
    uint32_t shard = moq_msquic_lane_index(lane);
    if (shard >= moqr_shards_count(ctx->shards)) {
        return 0;
    }
    moqr_bind_t *bind = moqr_shards_bind(ctx->shards, (uint16_t)shard);
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane,
                                                                     NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == RELAY_CONN_DEAD) {
            continue;
        }
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_OPENED);
            } else {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    uint64_t mask = 0;
    moqr_result_t rc =
        moqr_shards_step_shard(ctx->shards, (uint16_t)shard, now_us, &mask);
    atomic_fetch_add(&ctx->pump_calls[shard & 1u], 1);
    if (shard == 1 && (mask & 1ull) != 0) {
        atomic_fetch_add(&ctx->producer_wakes, 1);
    }
    for (uint32_t d = 0; mask != 0 && d < ctx->lanes; d++) {
        if (mask & (1ull << d)) {
            (void)moq_msquic_lane_wake(moq_msquic_managed_lane(m, d));
        }
    }
    if (rc != MOQR_OK) {
        return 1;
    }
    /* A binding this step detached must be retired now: nothing else wakes
     * this lane on its behalf. */
    if (!moqr_relay_reap_pass(bind, lane, NULL)) {
        return 1;
    }
    return 0;
}

/* Connect one client, place it on `lane`, and wait until it is ESTABLISHED
 * (so its accept — which read g_place_lane — has completed before the caller
 * moves the placement for the next peer). */
static moq_msquic_managed_t *
connect_on_lane(moq_version_t version, uint16_t port, uint32_t lane,
                bool streaming, moq_msquic_lane_pump_fn pump, void *ctx,
                atomic_int *established)
{
    atomic_store(&g_place_lane, lane);
    moq_msquic_managed_t *c = mk_client(version, port, streaming, pump, ctx);
    if (c == NULL) {
        return NULL;
    }
    for (int w = 0; w < 100; w++) {
        if (established != NULL && atomic_load(established)) {
            break;
        }
        (void)moq_msquic_managed_wait(c, 100 * 1000);
    }
    return c;
}

/* Two-lane relay with admission EXPLICITLY OFF — a deliberate refusal-policy
 * fixture, NOT production behaviour (production turns admission on whenever
 * lanes > 1). With admit_remote_demand=false the owner refuses every
 * forwarded demand: a publisher on lane 0 announces "loop/cam"; a subscriber
 * placed on `sub_lane` either receives (same lane, shard-local) or is refused
 * NOT_SUPPORTED (cross lane: shard 1 only mirrors the namespace, so the demand
 * is remote-owned and — under this off policy — fails closed). This keeps the
 * refusal round-trip covered after the production flip: the 0x3 the subscriber
 * sees comes back THROUGH the owner shard (proven by the resolved-refusal
 * counter below), and it also proves the announce replicated across shards
 * over real transport. */
static int
run_lanes_case(moq_version_t version, const char *alpn, uint32_t sub_lane)
{
    int before = g_failures;
    const bool cross = sub_lane != 0;

    moqr_shards_cfg_t shcfg;
    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.live_visibility = true;
    shcfg.admit_remote_demand = false;   /* the deliberate refusal policy */
    moqr_shards_t *shards = NULL;
    L_CHECK(moqr_shards_create(&shcfg, &shards) == MOQR_OK);
    if (shards == NULL) {
        return g_failures - before;
    }
    relay_lanes_ctx_t rctx = { .shards = shards, .lanes = 2 };

    moqr_core_limits_t lim;
    moqr_core_get_limits(moqr_shards_core(shards, 0), &lim);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = g_cert;
    scfg.key_path = g_key;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = version;
    scfg.streaming_objects = true;
    scfg.lane_count = 2;
    scfg.max_connections = 2u * lim.max_bindings;
    scfg.choose_lane = choose_lane_fixed;
    scfg.on_lane_pump = relay_lanes_pump;
    scfg.on_lane_pump_user = &rctx;
    moq_msquic_managed_t *relay = NULL;
    L_CHECK(moq_msquic_managed_create(&scfg, &relay) == MOQ_OK);
    if (relay == NULL) {
        moqr_shards_destroy(shards);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    /* Publisher -> lane 0 (sequential connect so placement is deterministic). */
    pub_ctx_t pctx;
    memset(&pctx, 0, sizeof(pctx));
    moq_msquic_managed_t *pub = connect_on_lane(
        version, port, 0, false, pub_on_pump, &pctx, &pctx.established);
    /* Let the publisher announce + the announce replicate to shard 1's mirror
     * before the cross-lane subscriber arrives. */
    for (int w = 0; w < 30 && atomic_load(&pctx.phase) < 1; w++) {
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    sub_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    moq_msquic_managed_t *sub = connect_on_lane(
        version, port, sub_lane, true, sub_on_pump, &sctx, &sctx.established);
    L_CHECK(pub != NULL && sub != NULL);

    bool ok = false;
    for (int waited = 0; waited < 300 && !ok; waited++) {
        if (cross) {
            /* Refusal proof: at least one reject whose terminal code is
             * NOT_SUPPORTED (0x3), and no object ever delivered. */
            ok = atomic_load(&sctx.rejects) > 0 &&
                 atomic_load(&sctx.last_reject_code) == 0x3;
        } else {
            ok = atomic_load(&sctx.received) >= OBJECTS;
        }
        if (moq_msquic_managed_is_fatal(relay)) {
            break;
        }
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }

    if (cross) {
        L_CHECK(atomic_load(&sctx.rejects) > 0);
        L_CHECK(atomic_load(&sctx.last_reject_code) == 0x3);   /* NOT_SUPPORTED */
        L_CHECK(atomic_load(&sctx.received) == 0);             /* never served */
    } else {
        L_CHECK(atomic_load(&sctx.received) == OBJECTS);
        L_CHECK(atomic_load(&sctx.byte_errors) == 0);
    }
    L_CHECK(!moq_msquic_managed_is_fatal(relay));

    if (sub != NULL) {
        (void)moq_msquic_managed_stop(sub);
        moq_msquic_managed_destroy(sub);
    }
    if (pub != NULL) {
        (void)moq_msquic_managed_stop(pub);
        moq_msquic_managed_destroy(pub);
    }
    /* Server outlives its connections on every lane: drain to zero conns, then
     * the facade is still live (only our stop ends it). */
    {
        moqr_drain_result_t dr = moqr_drain_to_count(relay, 0, 200);
        if (dr == MOQR_DRAIN_CLOSED) {
            printf("FAIL: facade CLOSED during drain — a lane pump fail-stop "
                   "or stop, not a stall\n");
            g_failures++;
        }
    }
    if (moq_msquic_managed_conn_count(relay) != 0) {
    }
    L_CHECK(moq_msquic_managed_conn_count(relay) == 0);
    L_CHECK(moq_msquic_managed_wait(relay, 0) != MOQ_ERR_CLOSED);
    (void)moq_msquic_managed_stop(relay);
    if (cross) {
        /* Round-trip proof (quiesced: every lane pump joined by stop). The
         * refused counter increments ONLY when the requester shard resolves
         * the OWNER's answer over the demand channel — a locally-refused
         * demand no longer bumps it — so a nonzero count here means the 0x3
         * the subscriber saw came back through the owner shard. */
        L_CHECK(moqr_shards_debug_remote_demand_refused(
                    shards, (uint16_t)sub_lane) >= 1);
    }
    moq_msquic_managed_destroy(relay);
    moqr_shards_destroy(shards);

    if (g_failures == before) {
        printf("PASS: loopback %s lanes=2 %s\n", alpn,
               cross ? "cross-lane refusal" : "same-lane delivery");
    }
    return g_failures - before;
}

/* Two-lane relay with admission ON: the completed cross-shard data pump
 * over real MsQuic. The publisher rides lane 0/shard 0; the subscriber rides
 * lane 1/shard 1, so every byte crosses the demand channel. Proves per
 * draft: exactly-once bytes+identity, live-edge chunk-through (the publisher
 * holds mid-object until the remote subscriber has SEEN the first half),
 * the graceful subgroup terminal (downstream FIN with terminal NORMAL), a
 * begun-object reset with its 62-bit code (bit-exact at the requester
 * shard; the subscriber sees TERMINAL_RESET — the public chunk event does
 * not surface the wire code), full teardown quiescence, a relay that
 * outlives its connections, and producer-credit wakes observed through the
 * step's returned mask. */
/* Drive the full admitted lifecycle over a PRE-BUILT shard runtime and a
 * facade capped at `max_conns`. Two setups feed it: the hand-built runtime
 * (run_lanes_admit_case) and the CLI serve composition
 * (run_lanes_serve_shaped_case). It owns `shards` (destroys it). `label`
 * distinguishes the row/PASS line. */
static int
run_lanes_admit_drive(moqr_shards_t *shards, uint32_t max_conns,
                      moq_version_t version, const char *alpn,
                      const char *label)
{
    int before = g_failures;
    relay_lanes_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.shards = shards;
    rctx.lanes = 2;

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = g_cert;
    scfg.key_path = g_key;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = version;
    scfg.streaming_objects = true;
    scfg.lane_count = 2;
    scfg.max_connections = max_conns;
    scfg.choose_lane = choose_lane_fixed;
    scfg.on_lane_pump = relay_lanes_pump;
    scfg.on_lane_pump_user = &rctx;
    moq_msquic_managed_t *relay = NULL;
    L_CHECK(moq_msquic_managed_create(&scfg, &relay) == MOQ_OK);
    if (relay == NULL) {
        moqr_shards_destroy(shards);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    pub_ctx_t pctx;
    memset(&pctx, 0, sizeof(pctx));
    pctx.live_edge = true;
    pctx.do_reset = true;
    moq_msquic_managed_t *pub = connect_on_lane(
        version, port, 0, false, pub_on_pump, &pctx, &pctx.established);
    for (int w = 0; w < 30 && atomic_load(&pctx.phase) < 1; w++) {
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    sub_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.chunked = true;
    moq_msquic_managed_t *sub = connect_on_lane(
        version, port, 1, true, sub_on_pump, &sctx, &sctx.established);
    L_CHECK(pub != NULL && sub != NULL);

    /* Live edge: the REMOTE subscriber must see object 2's first half while
     * the publisher still holds the rest. */
    bool edge = false;
    for (int w = 0; w < 300 && !edge; w++) {
        edge = atomic_load(&sctx.obj2_chunk) != 0;
        if (moq_msquic_managed_is_fatal(relay)) {
            break;
        }
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(edge);
    L_CHECK(atomic_load(&sctx.received) < 3);   /* object 2 still open */
    /* Producer-credit window: from here to the drain's end, shard 1 pushes
     * nothing toward shard 0 (the ACK is long past, no teardown yet), so any
     * growth in this counter is the credit wake itself. */
    long pw0 = atomic_load(&rctx.producer_wakes);
    atomic_store(&pctx.release, 1);
    (void)moq_msquic_managed_wake(pub);
    bool done = false;
    for (int w = 0; w < 300 && !done; w++) {
        done = atomic_load(&sctx.received) >= 3;
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(atomic_load(&sctx.received) == 3);   /* exactly once each */
    L_CHECK(atomic_load(&sctx.byte_errors) == 0);
    /* The held tail's messages were popped after the snapshot: the credit
     * wake must have fired within the pure-data window. */
    L_CHECK(atomic_load(&rctx.producer_wakes) > pw0);
    /* The graceful subgroup terminal: exactly one downstream FIN, on the
     * graceful subgroup's identity. */
    bool fin = false;
    for (int w = 0; w < 300 && !fin; w++) {
        fin = atomic_load(&sctx.sg_finished) != 0;
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(atomic_load(&sctx.sg_finished) == 1);
    L_CHECK(atomic_load(&sctx.sg_fin_bad) == 0);

    /* The begun-object reset: wait for the subscriber to BEGIN group 2,
     * then fire, then observe exactly one TERMINAL_RESET. */
    bool began = false;
    for (int w = 0; w < 300 && !began; w++) {
        began = atomic_load(&sctx.g2_chunk) != 0;
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(began);
    atomic_store(&pctx.reset_go, 1);
    (void)moq_msquic_managed_wake(pub);
    bool rs = false;
    for (int w = 0; w < 300 && !rs; w++) {
        rs = atomic_load(&sctx.reset_seen) != 0;
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(atomic_load(&sctx.reset_seen) == 1);
    /* The peer-visible reset code, bit-exact end to end over the wire. */
    L_CHECK(atomic_load(&sctx.reset_code_seen) == LB_WIDE_RESET);
    L_CHECK(atomic_load(&sctx.received) == 3);   /* the reset added none */
    L_CHECK(atomic_load(&sctx.sg_finished) == 1);   /* the reset added no FIN */
    L_CHECK(!atomic_load(&pctx.fatal));
    L_CHECK(!atomic_load(&sctx.fatal));

    if (sub != NULL) {
        (void)moq_msquic_managed_stop(sub);
        moq_msquic_managed_destroy(sub);
    }
    if (pub != NULL) {
        (void)moq_msquic_managed_stop(pub);
        moq_msquic_managed_destroy(pub);
    }
    {
        moqr_drain_result_t dr = moqr_drain_to_count(relay, 0, 200);
        if (dr == MOQR_DRAIN_CLOSED) {
            printf("FAIL: facade CLOSED during drain — a lane pump fail-stop "
                   "or stop, not a stall\n");
            g_failures++;
        }
    }
    if (moq_msquic_managed_conn_count(relay) != 0) {
    }
    L_CHECK(moq_msquic_managed_conn_count(relay) == 0);
    L_CHECK(moq_msquic_managed_wait(relay, 0) != MOQ_ERR_CLOSED);
    /* Let the idle teardown (linger -> UNDEMAND -> owner teardown) pump
     * through before stopping the lanes. */
    for (int waited = 0; waited < 50; waited++) {
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(!moq_msquic_managed_is_fatal(relay));
    (void)moq_msquic_managed_stop(relay);

    /* Quiesced (every lane pump joined by stop): the full drain proof plus
     * the bit-exact reset code at the requester shard, and the credit wake
     * observed through the returned mask. */
    L_CHECK(moqr_shards_debug_pending_demand(shards, 1) == 0);
    L_CHECK(moqr_shards_debug_owner_pump_subs(shards, 0) == 0);
    L_CHECK(moqr_shards_debug_owner_progress_slots(shards, 0) == 0);
    L_CHECK(moqr_shards_debug_requester_open_objects(shards, 1) == 0);
    for (uint16_t i = 0; i < 2; i++) {
        for (uint16_t d = 0; d < 2; d++) {
            L_CHECK(moqr_shards_debug_demand_channel_pending(shards, i, d) ==
                    0);
        }
    }
    L_CHECK(atomic_load(&rctx.producer_wakes) >= 1);

    /* Cadence, not Mbps. What crossed is FULLY deterministic even over the
     * real wire (the schedule cannot change WHICH messages the owner must
     * push): every per-kind count, the owner message total, and the owner
     * byte total are asserted EXACTLY. What varies with the transport
     * schedule — owner pump TURNS and the mask-level wake-request counts
     * (observed 4↔7 turns, 5↔6 push wakes across runs) plus the raw lane
     * pump counts — is printed ADVISORY-only and never asserted. No
     * throughput, no wire-ceiling, no flush-counter claim. */
    {
        moqr_shards_stats_t s0, s1;
        L_CHECK(moqr_shards_get_stats(shards, 0, &s0) == MOQR_OK);
        L_CHECK(moqr_shards_get_stats(shards, 1, &s1) == MOQR_OK);
        /* The complete deterministic per-kind vector: three objects crossed
         * chunked (OPEN/CHUNK.../END), one begun-object reset (its own
         * OPEN + CHUNK + RESET, no END), and one subgroup seal — plus the
         * admission's one ACK and one DEMAND. Nothing whole-object or
         * group-level crossed. */
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_OBJ] == 0);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] == 4);   /* 3 + reset */
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK] == 5);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_OBJ_END] == 3);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_OBJ_RESET] == 1);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_GRP_RESET] == 0);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] == 0);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_SG_SEAL] == 1);
        L_CHECK(s0.enqueued[MOQR_SHARDS_MSG_ACK] == 1);
        L_CHECK(s1.enqueued[MOQR_SHARDS_MSG_DEMAND] == 1);
        /* Owner data-pump totals: the 4 OPEN + 5 CHUNK + 3 END + 1 RESET +
         * 1 SEAL = 14 messages, and exactly 320 payload bytes (five chunks:
         * two SMALL_SIZE halves of object 2 + two of the reset object's
         * begun half + object 0/1 each one SMALL_SIZE chunk — the wire
         * fragmentation cannot change the total payload that crossed). */
        L_CHECK(s0.pump_messages == 14);
        L_CHECK(s0.pump_bytes == 5u * SMALL_SIZE);
        L_CHECK(s0.pump_messages == s0.enqueued[MOQR_SHARDS_MSG_OBJ] +
                                        s0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] +
                                        s0.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK] +
                                        s0.enqueued[MOQR_SHARDS_MSG_OBJ_END] +
                                        s0.enqueued[MOQR_SHARDS_MSG_OBJ_RESET] +
                                        s0.enqueued[MOQR_SHARDS_MSG_GRP_RESET] +
                                        s0.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] +
                                        s0.enqueued[MOQR_SHARDS_MSG_SG_SEAL]);
        /* Wake requests: schedule-dependent, advisory only (>= 1). */
        L_CHECK(s0.wake_requests_push >= 1);
        L_CHECK(s1.wake_requests_credit >= 1);
        uint64_t delivered = 3;   /* the three completed objects */
        printf("CADENCE_MSQUIC,%s,lanes=2,admit,owner_msgs=%" PRIu64
               ",owner_bytes=%" PRIu64 ",owner_turns=%" PRIu64
               ",obj_open=%" PRIu64 ",obj_chunk=%" PRIu64 ",obj_end=%"
               PRIu64 ",obj_reset=%" PRIu64 ",sg_seal=%" PRIu64
               ",push_wakes=%" PRIu64 ",credit_wakes=%" PRIu64
               ",delivered=%" PRIu64 ",msgs_per_obj=%.2f,turns_per_obj=%.2f"
               " | advisory: lane_pumps_s0=%ld lane_pumps_s1=%ld "
               "producer_credit_wakes=%ld\n",
               alpn, s0.pump_messages, s0.pump_bytes, s0.pump_turns,
               s0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN],
               s0.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK],
               s0.enqueued[MOQR_SHARDS_MSG_OBJ_END],
               s0.enqueued[MOQR_SHARDS_MSG_OBJ_RESET],
               s0.enqueued[MOQR_SHARDS_MSG_SG_SEAL],
               s0.wake_requests_push, s1.wake_requests_credit, delivered,
               (double)s0.pump_messages / (double)delivered,
               (double)s0.pump_turns / (double)delivered,
               atomic_load(&rctx.pump_calls[0]),
               atomic_load(&rctx.pump_calls[1]),
               atomic_load(&rctx.producer_wakes));
    }
    moq_msquic_managed_destroy(relay);
    moqr_shards_destroy(shards);

    if (g_failures == before) {
        printf("PASS: loopback %s lanes=2 %s\n", alpn, label);
    }
    return g_failures - before;
}

/* Hand-built runtime (admit=true set directly), then drive the admitted
 * lifecycle. admit=true matches what the production CLI sets for lanes > 1;
 * the CLI-composition proof lives in run_lanes_serve_shaped_case. */
static int
run_lanes_admit_case(moq_version_t version, const char *alpn)
{
    moqr_shards_cfg_t shcfg;
    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.live_visibility = true;
    shcfg.admit_remote_demand = true;
    moqr_shards_t *shards = NULL;
    if (moqr_shards_create(&shcfg, &shards) != MOQR_OK || shards == NULL) {
        L_CHECK(false);
        return 1;
    }
    moqr_core_limits_t lim;
    moqr_core_get_limits(moqr_shards_core(shards, 0), &lim);
    return run_lanes_admit_drive(shards, 2u * lim.max_bindings, version, alpn,
                                 "cross-lane admitted delivery");
}

/* THE production-flip regression: the shard runtime and facade cap come from
 * the REAL CLI serve composition (moqr_cli_serve_compose over a parsed
 * listener.lanes = 2 config) — the exact builder output cmd_serve_lanes
 * consumes, with admission set by the lanes > 1 rule and the facade capped
 * at lanes * usable_bindings_per_shard. A direct admit=true would not prove
 * the CLI path; this consumes it. Capacity for the same config is checked to
 * report admission on with the admission storage delta included. */
static int
run_lanes_serve_shaped_case(moq_version_t version, const char *alpn)
{
    int before = g_failures;
    const moq_alloc_t *alloc = moq_alloc_default();

    char js[128];
    snprintf(js, sizeof(js),
             "{\"listener\":{\"port\":1,\"lanes\":2,\"versions\":[%d]}}",
             version == MOQ_VERSION_DRAFT_16 ? 16 : 18);
    moqr_cli_config_t cfg;
    char cerr[128];
    L_CHECK(moqr_cli_config_parse(js, strlen(js), &cfg, cerr,
                                  sizeof(cerr)) == MOQR_OK);

    moqr_shards_cfg_t scfg;
    uint32_t max_conns = 0;
    L_CHECK(moqr_cli_serve_compose(&cfg, alloc, &scfg, &max_conns) ==
            MOQR_OK);
    /* The composition IS the production rule: admission on for lanes > 1. */
    L_CHECK(scfg.admit_remote_demand);
    /* Capacity for the same config: admission on, storage delta included. */
    moqr_shards_capacity_t cap;
    L_CHECK(moqr_shards_capacity_describe(&scfg, &cap) == MOQR_OK);
    L_CHECK(cap.admission);
    L_CHECK(cap.shards_structure_bytes > 0);

    moqr_shards_t *shards = NULL;
    if (moqr_shards_create(&scfg, &shards) != MOQR_OK || shards == NULL) {
        L_CHECK(false);
        return g_failures - before;
    }
    return run_lanes_admit_drive(shards, max_conns, version, alpn,
                                 "CLI serve-shaped admitted delivery");
}

/* Owner-side announce revalidation over real MsQuic. The authorize hook is
 * PURE per call; the only cross-thread choreography is one atomic flag the
 * main thread release-stores (the same pattern as pctx.release/reset_go) —
 * no shared mutable verifier state beyond it, nothing lane-to-lane. */
typedef struct auth_flip {
    atomic_int deny;   /* 0: ALLOW with a lease; 1: DENY (GOING_AWAY) */
} auth_flip_t;

static void
lanes_auth_hook(void *ctx, const moqr_auth_request_t *req,
                moqr_auth_verdict_t *out)
{
    auth_flip_t *f = ctx;
    if (req->action != MOQR_AUTH_PUBLISH_NAMESPACE) {
        out->decision = MOQR_AUTH_ALLOW;
        return;
    }
    if (!atomic_load(&f->deny)) {
        out->decision = MOQR_AUTH_ALLOW;
        out->revalidate_after_us = 100 * 1000;   /* 100ms lease */
    } else {
        out->decision = MOQR_AUTH_DENY;
        out->reason = MOQR_AUTH_REASON_POLICY;
        out->error_code = 0x6;   /* the chosen GOING_AWAY code */
    }
}

/* Two-lane admitted relay, both drafts: the owner's PUBLISH_NAMESPACE
 * revalidation flips to DENY while cross-shard data keeps flowing (the
 * publisher DRIPS objects, which also keeps every lane pumping so the due
 * recheck always runs — lanes only pump on activity). The subscriber's
 * demand terminates GOING_AWAY (SUBSCRIBE_DONE 0x6); the LIVE publisher
 * receives exactly one namespace cancellation — draft 16 carries the chosen
 * GOING_AWAY code, draft 18's request-bidi cancel exposes only the
 * protocol's fixed CANCELLED code — and continued pumping never duplicates
 * it. Post-stop the runtime is fully drained with ZERO capacity/overrun
 * terminals (policy teardown, not loss). */
static int
run_lanes_auth_revoke_case(moq_version_t version, const char *alpn,
                           uint64_t expect_cancel)
{
    int before = g_failures;

    auth_flip_t flip;
    memset(&flip, 0, sizeof(flip));
    moqr_shards_cfg_t shcfg;
    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.live_visibility = true;
    shcfg.admit_remote_demand = true;   /* test-owned runtime ONLY */
    shcfg.core_cfg.authorize = lanes_auth_hook;
    shcfg.core_cfg.authorize_ctx = &flip;
    moqr_shards_t *shards = NULL;
    L_CHECK(moqr_shards_create(&shcfg, &shards) == MOQR_OK);
    if (shards == NULL) {
        return g_failures - before;
    }
    relay_lanes_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.shards = shards;
    rctx.lanes = 2;

    moqr_core_limits_t lim;
    moqr_core_get_limits(moqr_shards_core(shards, 0), &lim);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = g_cert;
    scfg.key_path = g_key;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = version;
    scfg.streaming_objects = true;
    scfg.lane_count = 2;
    scfg.max_connections = 2u * lim.max_bindings;
    scfg.choose_lane = choose_lane_fixed;
    scfg.on_lane_pump = relay_lanes_pump;
    scfg.on_lane_pump_user = &rctx;
    moq_msquic_managed_t *relay = NULL;
    L_CHECK(moq_msquic_managed_create(&scfg, &relay) == MOQ_OK);
    if (relay == NULL) {
        moqr_shards_destroy(shards);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    pub_ctx_t pctx;
    memset(&pctx, 0, sizeof(pctx));
    pctx.drip = true;   /* the flow (and pumping) never idles */
    moq_msquic_managed_t *pub = connect_on_lane(
        version, port, 0, false, pub_on_pump, &pctx, &pctx.established);
    for (int w = 0; w < 30 && atomic_load(&pctx.phase) < 1; w++) {
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    sub_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.chunked = true;
    moq_msquic_managed_t *sub = connect_on_lane(
        version, port, 1, true, sub_on_pump, &sctx, &sctx.established);
    L_CHECK(pub != NULL && sub != NULL);

    /* The admitted cross-shard flow is live before anything is revoked. */
    bool flowing = false;
    for (int w = 0; w < 300 && !flowing; w++) {
        flowing = atomic_load(&sctx.received) >= 3;
        if (moq_msquic_managed_is_fatal(relay)) {
            break;
        }
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(flowing);
    L_CHECK(atomic_load(&sctx.done_seen) == 0);
    L_CHECK(atomic_load(&pctx.ns_cancelled) == 0);

    /* Deny the owner-side revalidation (one atomic release-store; the drip
     * keeps lane 0 pumping, so the due recheck revokes and withdraws). */
    atomic_store(&flip.deny, 1);

    /* The publisher receives exactly one cancellation, per-draft code. */
    bool cancelled = false;
    for (int w = 0; w < 300 && !cancelled; w++) {
        cancelled = atomic_load(&pctx.ns_cancelled) != 0;
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(atomic_load(&pctx.ns_cancelled) == 1);
    L_CHECK(atomic_load(&pctx.ns_cancel_code) == expect_cancel);

    /* The cross-shard demand ends the downstream track at the subscriber:
     * PUBLISH_DONE TRACK_ENDED. The revocation's own REQUEST_ERROR-domain code
     * rides the namespace cancel above and never becomes this status. */
    bool done = false;
    for (int w = 0; w < 300 && !done; w++) {
        done = atomic_load(&sctx.done_seen) != 0;
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(atomic_load(&sctx.done_seen) >= 1);
    L_CHECK(atomic_load(&sctx.done_code) == 0x2u);
    L_CHECK(atomic_load(&sctx.byte_errors) == 0);

    /* Continued pumping duplicates nothing. */
    for (int w = 0; w < 20; w++) {
        (void)moq_msquic_managed_wait(relay, 100 * 1000);
    }
    L_CHECK(atomic_load(&pctx.ns_cancelled) == 1);
    L_CHECK(!atomic_load(&pctx.fatal));
    L_CHECK(!atomic_load(&sctx.fatal));

    if (sub != NULL) {
        (void)moq_msquic_managed_stop(sub);
        moq_msquic_managed_destroy(sub);
    }
    if (pub != NULL) {
        (void)moq_msquic_managed_stop(pub);
        moq_msquic_managed_destroy(pub);
    }
    {
        moqr_drain_result_t dr = moqr_drain_to_count(relay, 0, 200);
        if (dr == MOQR_DRAIN_CLOSED) {
            printf("FAIL: facade CLOSED during drain — a lane pump fail-stop "
                   "or stop, not a stall\n");
            g_failures++;
        }
    }
    if (moq_msquic_managed_conn_count(relay) != 0) {
    }
    L_CHECK(moq_msquic_managed_conn_count(relay) == 0);
    L_CHECK(!moq_msquic_managed_is_fatal(relay));
    (void)moq_msquic_managed_stop(relay);

    /* Quiesced (lanes joined): the policy teardown drained everything and
     * counted NO loss terminals. */
    moqr_shards_stats_t st0, st1;
    L_CHECK(moqr_shards_get_stats(shards, 0, &st0) == MOQR_OK);
    L_CHECK(moqr_shards_get_stats(shards, 1, &st1) == MOQR_OK);
    L_CHECK(st1.pending_demands == 0);
    L_CHECK(moqr_shards_debug_owner_pump_subs(shards, 0) == 0);
    L_CHECK(st0.owner_progress_slots == 0);
    L_CHECK(st1.requester_open_objects == 0);
    for (uint16_t i = 0; i < 2; i++) {
        for (uint16_t d = 0; d < 2; d++) {
            L_CHECK(moqr_shards_debug_demand_channel_pending(shards, i, d) ==
                    0);
        }
    }
    L_CHECK(st0.term_capacity == 0 && st0.term_overrun == 0);
    L_CHECK(st1.term_capacity == 0 && st1.term_overrun == 0);

    moq_msquic_managed_destroy(relay);
    moqr_shards_destroy(shards);

    if (g_failures == before) {
        printf("PASS: loopback %s lanes=2 owner announce revocation\n",
               alpn);
    }
    return g_failures - before;
}

/* -- terminal acknowledgment ---------------------------------------------- *
 * A managed child is reclaimed only once the application has polled its
 * SESSION_CLOSED and acknowledged it. These arms drive the real single-lane
 * pump over real transport and pin the paths where the relay stops polling
 * before that terminal arrives — an unacknowledged child holds its admission
 * slot forever, so each arm ends by proving the slot came back. */

/* Deny every client setup: the binding closes and detaches the session
 * BEFORE any SESSION_CLOSED is polled. */
static void
deny_setup_authz(void *ctx, const moqr_auth_request_t *req,
                 moqr_auth_verdict_t *out)
{
    (void)ctx;
    memset(out, 0, sizeof(*out));
    out->decision = req->action == MOQR_AUTH_CLIENT_SETUP ? MOQR_AUTH_DENY
                                                          : MOQR_AUTH_ALLOW;
    out->error_code = 0x10u;
    out->reason = MOQR_AUTH_REASON_POLICY;
}

/* Bounded wait for the facade's live-connection count, driven by the relay's
 * own wait (no sleeps, no unrelated wake). */
static bool
wait_conn_count(moq_msquic_managed_t *m, uint32_t want, int rounds)
{
    moqr_drain_result_t dr = moqr_drain_to_count(m, want, rounds);
    if (dr == MOQR_DRAIN_OK) {
        return true;
    }
    if (dr == MOQR_DRAIN_CLOSED) {
        printf("FAIL: facade CLOSED during drain — a lane pump fail-stop or "
               "stop, not a stall\n");
        g_failures++;
    }
    return false;
}

/* Build the single-lane relay used by the acknowledgment arms. `authorize`
 * may be NULL (allow-all). `bind_conns` caps the binding's connection table
 * independently of the facade admission cap. */
static moq_msquic_managed_t *
mk_ack_relay(relay_ctx_t *rctx, moq_version_t version, uint32_t facade_conns,
             uint32_t bind_conns, moqr_authorize_fn authorize)
{
    memset(rctx, 0, sizeof(*rctx));
    moqr_core_relay_cfg_t core_cfg;
    moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg),
                                   moq_alloc_default());
    if (moqr_trace_create(moq_alloc_default(), 256, &rctx->trace) != MOQR_OK) {
        return NULL;
    }
    core_cfg.trace = rctx->trace;
    core_cfg.authorize = authorize;
    if (moqr_core_create(&core_cfg, &rctx->core) != MOQR_OK) {
        return NULL;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = rctx->core;
    bcfg.max_conns = bind_conns;
    if (moqr_bind_create(&bcfg, &rctx->bind) != MOQR_OK) {
        return NULL;
    }

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = g_cert;
    scfg.key_path = g_key;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = version;
    scfg.lane_count = 1;
    scfg.max_connections = facade_conns;
    scfg.on_lane_pump = relay_lane_pump;
    scfg.on_lane_pump_user = rctx;
    moq_msquic_managed_t *relay = NULL;
    if (moq_msquic_managed_create(&scfg, &relay) != MOQ_OK) {
        return NULL;
    }
    return relay;
}

static void
ack_relay_destroy(relay_ctx_t *rctx, moq_msquic_managed_t *relay)
{
    if (relay != NULL) {
        (void)moq_msquic_managed_stop(relay);
        moq_msquic_managed_destroy(relay);
    }
    if (rctx->bind != NULL) {
        moqr_bind_destroy(rctx->bind);
    }
    if (rctx->core != NULL) {
        moqr_core_destroy(rctx->core);
    }
    if (rctx->trace != NULL) {
        moqr_trace_destroy(rctx->trace);
    }
}

/* Setup denial: the binding detaches before the terminal exists, so nothing
 * bound ever polls it. The child must still be drained, acknowledged and
 * reclaimed, and its admission slot must be reusable. */
static int
run_ack_setup_denial_case(moq_version_t version)
{
    int before = g_failures;
    relay_ctx_t rctx;
    moq_msquic_managed_t *relay =
        mk_ack_relay(&rctx, version, 1, 1, deny_setup_authz);
    L_CHECK(relay != NULL);
    if (relay == NULL) {
        ack_relay_destroy(&rctx, relay);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    sub_ctx_t c1;
    memset(&c1, 0, sizeof(c1));
    moq_msquic_managed_t *cl = mk_client(version, port, false, sub_on_pump,
                                         &c1);
    L_CHECK(cl != NULL);
    /* The denial is reclaimed with no unrelated wake and no second client. */
    L_CHECK(wait_conn_count(relay, 0, 400));
    if (cl != NULL) {
        (void)moq_msquic_managed_stop(cl);
        moq_msquic_managed_destroy(cl);
    }

    /* The slot came back: a further connection is still admissible. */
    sub_ctx_t c2;
    memset(&c2, 0, sizeof(c2));
    moq_msquic_managed_t *cl2 = mk_client(version, port, false, sub_on_pump,
                                          &c2);
    L_CHECK(cl2 != NULL);
    L_CHECK(wait_conn_count(relay, 1, 400));
    L_CHECK(wait_conn_count(relay, 0, 400));
    if (cl2 != NULL) {
        (void)moq_msquic_managed_stop(cl2);
        moq_msquic_managed_destroy(cl2);
    }
    printf("ACK setup-denial: ack_ok=%d not_ready=%d unexpected=%d\n",
           atomic_load(&rctx.ack_ok), atomic_load(&rctx.ack_not_ready),
           atomic_load(&rctx.ack_unexpected));
    L_CHECK(atomic_load(&rctx.ack_unexpected) == 0);
    L_CHECK(atomic_load(&rctx.ack_ok) >= 1);
    /* Each fail-closed operand refused on its own. */
    L_CHECK(atomic_load(&rctx.null_bind_failed_closed) == 1);
    L_CHECK(atomic_load(&rctx.null_lane_failed_closed) == 1);

    ack_relay_destroy(&rctx, relay);
    if (g_failures == before) {
        printf("PASS: ack_setup_denial\n");
    }
    return g_failures - before;
}

/* Bind admission failure: the facade admits more children than the binding's
 * table holds, so moqr_bind_conn_open refuses one. That child was never bound,
 * so only the orphan drain can retire it. */
static int
run_ack_admission_failure_case(moq_version_t version)
{
    int before = g_failures;
    relay_ctx_t rctx;
    moq_msquic_managed_t *relay = mk_ack_relay(&rctx, version, 2, 1, NULL);
    L_CHECK(relay != NULL);
    if (relay == NULL) {
        ack_relay_destroy(&rctx, relay);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    sub_ctx_t c1, c2;
    memset(&c1, 0, sizeof(c1));
    memset(&c2, 0, sizeof(c2));
    moq_msquic_managed_t *a = mk_client(version, port, false, sub_on_pump,
                                        &c1);
    moq_msquic_managed_t *b = mk_client(version, port, false, sub_on_pump,
                                        &c2);
    L_CHECK(a != NULL && b != NULL);
    /* The refused child is reclaimed while the admitted one keeps serving. */
    L_CHECK(wait_conn_count(relay, 1, 400));
    if (a != NULL) {
        (void)moq_msquic_managed_stop(a);
        moq_msquic_managed_destroy(a);
    }
    if (b != NULL) {
        (void)moq_msquic_managed_stop(b);
        moq_msquic_managed_destroy(b);
    }
    L_CHECK(wait_conn_count(relay, 0, 400));

    sub_ctx_t c3;
    memset(&c3, 0, sizeof(c3));
    moq_msquic_managed_t *c = mk_client(version, port, false, sub_on_pump,
                                        &c3);
    L_CHECK(c != NULL);
    L_CHECK(wait_conn_count(relay, 1, 400));
    if (c != NULL) {
        (void)moq_msquic_managed_stop(c);
        moq_msquic_managed_destroy(c);
    }
    L_CHECK(wait_conn_count(relay, 0, 400));
    printf("ACK admission-failure: ack_ok=%d not_ready=%d unexpected=%d\n",
           atomic_load(&rctx.ack_ok), atomic_load(&rctx.ack_not_ready),
           atomic_load(&rctx.ack_unexpected));
    L_CHECK(atomic_load(&rctx.ack_unexpected) == 0);
    L_CHECK(atomic_load(&rctx.ack_ok) >= 1);

    ack_relay_destroy(&rctx, relay);
    if (g_failures == before) {
        printf("PASS: ack_admission_failure\n");
    }
    return g_failures - before;
}

/* Bounded consumer: while the binding pump is starved the terminal is never
 * transferred, so the child must be RETAINED and the acknowledgment refused.
 * Resuming consumption must then reclaim it. */
static int
run_ack_bounded_retention_case(moq_version_t version)
{
    int before = g_failures;
    relay_ctx_t rctx;
    moq_msquic_managed_t *relay = mk_ack_relay(&rctx, version, 1, 1, NULL);
    L_CHECK(relay != NULL);
    if (relay == NULL) {
        ack_relay_destroy(&rctx, relay);
        return g_failures - before;
    }
    uint16_t port = moq_msquic_managed_port(relay);
    L_CHECK(port != 0);

    sub_ctx_t c1;
    memset(&c1, 0, sizeof(c1));
    moq_msquic_managed_t *cl = mk_client(version, port, false, sub_on_pump,
                                         &c1);
    L_CHECK(cl != NULL);
    L_CHECK(wait_conn_count(relay, 1, 400));

    /* Starve the binding pump, then close the peer: the terminal is queued
     * but never polled. */
    atomic_store(&rctx.stall, 1);
    if (cl != NULL) {
        (void)moq_msquic_managed_stop(cl);
        moq_msquic_managed_destroy(cl);
    }
    for (int i = 0; i < 40; i++) {
        (void)moq_msquic_managed_wait(relay, 25 * 1000);
    }
    /* The binding still owns this session, so the retirement pass must leave
     * it alone: the child is held by the unconsumed terminal, and nothing has
     * been acknowledged. */
    L_CHECK(moq_msquic_managed_conn_count(relay) == 1);
    L_CHECK(atomic_load(&rctx.ack_ok) == 0);

    /* Resume consumption: the terminal transfers, the acknowledgment is
     * accepted, and the child is reclaimed. */
    atomic_store(&rctx.stall, 0);
    (void)moq_msquic_managed_wake(relay);
    L_CHECK(wait_conn_count(relay, 0, 400));
    printf("ACK bounded-retention: ack_ok=%d not_ready=%d unexpected=%d\n",
           atomic_load(&rctx.ack_ok), atomic_load(&rctx.ack_not_ready),
           atomic_load(&rctx.ack_unexpected));
    L_CHECK(atomic_load(&rctx.ack_unexpected) == 0);
    L_CHECK(atomic_load(&rctx.ack_ok) >= 1);

    ack_relay_destroy(&rctx, relay);
    if (g_failures == before) {
        printf("PASS: ack_bounded_retention\n");
    }
    return g_failures - before;
}

int
main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem> [case]\n", argv[0]);
        return 2;
    }
    g_cert = argv[1];
    g_key = argv[2];

    /* Optional focused selector: run ONE existing scenario, exactly as the
     * full suite runs it (same function, same arguments — no copied or
     * simplified workload). An unknown name is an error, never a silent
     * full run: a harness that misspells a case must not report the whole
     * suite's verdict under that case's name. */
    if (argc > 3) {
        const char *sel = argv[3];

        if (strcmp(sel, "lanes_cross_d18") == 0) {
            return run_lanes_case(MOQ_VERSION_DRAFT_18, "moqt-18", 1);
        }
        if (strcmp(sel, "lanes_same_d18") == 0) {
            return run_lanes_case(MOQ_VERSION_DRAFT_18, "moqt-18", 0);
        }
        fprintf(stderr, "%s: unknown case '%s'\n", argv[0], sel);
        return 2;
    }

    /* Streaming receive is the production forwarding path: the relay ingests
     * OBJECT_CHUNK and forwards chunk-by-chunk with live-edge delivery. Both
     * drafts, each on its own exact-version listener. */
    (void)run_case(MOQ_VERSION_DRAFT_16, "moqt-16", true);
    (void)run_case(MOQ_VERSION_DRAFT_18, "moqt-18", true);
    /* Focused whole-object receive regression (legacy / non-production mode). */
    (void)run_case(MOQ_VERSION_DRAFT_18, "moqt-18", false);
    /* Multi-lane substrate (d18): lane i -> shard i. Same-lane delivers; a
     * cross-lane subscriber is refused NOT_SUPPORTED (which also proves the
     * announce replicated across shards over real transport). */
    (void)run_lanes_case(MOQ_VERSION_DRAFT_18, "moqt-18", 0);
    (void)run_lanes_case(MOQ_VERSION_DRAFT_18, "moqt-18", 1);
    /* Admission ON, hand-built runtime (production reaches the same admit
     * state via the CLI builder for lanes > 1): the completed cross-shard
     * pump over real transport, both drafts. */
    (void)run_lanes_admit_case(MOQ_VERSION_DRAFT_16, "moqt-16");
    (void)run_lanes_admit_case(MOQ_VERSION_DRAFT_18, "moqt-18");
    /* Owner announce revocation over real transport, both drafts: the
     * per-draft publisher-cancel codes and the cross-shard GOING_AWAY
     * termination. */
    (void)run_lanes_auth_revoke_case(MOQ_VERSION_DRAFT_16, "moqt-16", 0x6u);
    (void)run_lanes_auth_revoke_case(MOQ_VERSION_DRAFT_18, "moqt-18", 0x0u);
    /* THE production-flip gate: the serve-shaped composition (real CLI
     * builder + facade cap) drives the full admitted lifecycle over real
     * transport, both drafts, ten repetitions each for stability. */
    for (int rep = 0; rep < 10; rep++) {
        (void)run_lanes_serve_shaped_case(MOQ_VERSION_DRAFT_16, "moqt-16");
        (void)run_lanes_serve_shaped_case(MOQ_VERSION_DRAFT_18, "moqt-18");
    }
    /* With nothing valid to retire against, the retirement pass reports
     * failure rather than a successful no-op: a pump that lost its binding
     * must stop, not serve on. */
    L_CHECK(!moqr_relay_reap_pass(NULL, NULL, NULL));
    printf("PASS: ack_reap_pass_fails_closed\n");

    /* Terminal acknowledgment on the single-lane pump: the close paths that
     * stop polling before the terminal arrives, and the bounded consumer that
     * has not polled it yet. */
    (void)run_ack_setup_denial_case(MOQ_VERSION_DRAFT_18);
    (void)run_ack_admission_failure_case(MOQ_VERSION_DRAFT_18);
    (void)run_ack_bounded_retention_case(MOQ_VERSION_DRAFT_18);
    return g_failures;
}
