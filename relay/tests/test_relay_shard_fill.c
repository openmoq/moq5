/*
 * Draft-22 fills answered by a sharded relay whatever shard the subscriber
 * lands on: a K=2 SimPair rig where every client session attaches to a
 * shard's production binding (moqr_shards_bind), the cross-shard demand path
 * is admitted, and time is the rig's own clock.
 *
 * In the cross-shard cases the publisher and a priming subscriber sit on one
 * shard (the owner), so the track's retained Objects live there, and the
 * subscriber under test asks for a fill from the other shard, whose own log
 * holds nothing of the range. In the late-range cases the range reaches the
 * relay only after the fill request. The contract (draft-ietf-moq-transport-22
 * Sections 3.2.2, 3.2.3, 3.4, 3.4.1 and 9.20.5): a fill is answered by a fill
 * fetch stream that serves the range (Objects, End of Range markers, FIN) or
 * is opened and reset; the FILL_TIMEOUT budgets only the wait for Objects the
 * relay does not hold, and Objects held anywhere in the relay are available.
 * So a fill served on a shard that does not own the track retrieves the
 * missing range from the owner and answers item for item what the owner shard
 * would answer, bounded by the same deadline. The retrieval never enters the
 * serving shard's log, so live subscribers there see exactly what they saw
 * before.
 */

#include <moq/relay/moqr_shards.h>

#include <moq/relay/moqr_bind.h>

#include <moq/moq.h>
#include <moq/sim.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* counting allocator (single-threaded: the deterministic runner) */
typedef struct ca {
    moq_alloc_t vt;
    long        live;
    size_t      fail_size;
    bool        watching;
    size_t      sizes[2];
    moq_bytes_t expected[2];
    uintptr_t   copies[2];
    unsigned    attempts[2], releases[2];
    bool        released_exact[2];
} ca_t;

static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    bool refuse = a->fail_size != 0 && n == a->fail_size;
    if (refuse) a->fail_size = 0;
    void *p = refuse ? NULL : malloc(n);
    if (p != NULL) {
        a->live += (long)n;
    }
    for (size_t i = 0; a->watching && i < 2; i++) {
        if (n == a->sizes[i]) {
            a->attempts[i]++;
            if (p != NULL) a->copies[i] = (uintptr_t)p;
        }
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, n);
    if (q != NULL) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    for (size_t i = 0; a->watching && p != NULL && i < 2; i++) {
        if ((uintptr_t)p == a->copies[i]) {
            a->releases[i]++;
            a->released_exact[i] = n == a->sizes[i] && n >= a->expected[i].len &&
                memcmp((const uint8_t *)p + n - a->expected[i].len,
                       a->expected[i].data, a->expected[i].len) == 0;
        }
    }
    if (p != NULL) {
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

/* -- the rig ------------------------------------------------------------------ */

#define SF_MAX_CONNS 6
/* Live Objects a subscriber's observation keeps by position. */
#define SF_LIVE_MAX 96
#define SF_STEP_BOUND 64

typedef struct sfconn {
    bool           used;
    bool           closed;  /* the test closed its session: transport steps
                             * past the close are not checked */
    bool           transport_paused; /* retain wire work, not bind ownership */
    uint16_t       shard;
    moq_simpair_t *sp;
    moq_session_t *peer;    /* client side: the test's endpoint */
    moq_session_t *rsess;   /* server side: the relay's session */
} sfconn_t;

typedef struct sfrig {
    ca_t          *alloc;
    moqr_shards_t *s;
    sfconn_t       conns[SF_MAX_CONNS];
    uint64_t       now;
    bool           earned;   /* run a shard only on a wake it earned */
    /* Wake-driven runtime only: shards in `frozen` are not stepped; the
     * wakes they earn meanwhile are carried until they thaw. With
     * `freeze_owner_on_ack`, shard 0 freezes right after the step that
     * pushed its first ACK beyond `ack_base`. */
    uint64_t       frozen;
    uint64_t       carried;
    bool           freeze_owner_on_ack;
    uint64_t       ack_base;
    int            failures;
} sfrig_t;

#define SF_CHECK(rig, expr)                                         \
    do {                                                            \
        if (!(expr)) {                                              \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            (rig)->failures++;                                      \
        }                                                           \
    } while (0)

/* The relay's fill waits: 50 ms without FILL_TIMEOUT, at most 1 s with one. */
#define SF_DEFAULT_WAIT_MS 50u
#define SF_MAX_WAIT_MS     1000u

/* The relay-side session of the next connection receives objects as chunks
 * (OPEN records), so an Object can be in the log before it is whole. */
static bool g_sf_server_streaming = false;
static uint64_t g_sf_log_bytes = 0;

static moqr_result_t
sfrig_create_ex(sfrig_t *r, ca_t *a, bool earned, uint32_t channel_entries,
                uint32_t max_fetches)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    r->earned = earned;
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.shards = 2;
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 4;
    cfg.core_cfg.log_budget.max_bytes = g_sf_log_bytes != 0
                                          ? g_sf_log_bytes : 1u << 20;
    cfg.core_cfg.linger_us = 500;
    cfg.fill_timeout_default_ms = SF_DEFAULT_WAIT_MS;
    cfg.fill_timeout_max_ms = SF_MAX_WAIT_MS;
    cfg.live_visibility = earned;
    cfg.demand_channel_entries = channel_entries;
    cfg.core_cfg.max_fetches = max_fetches;
    return moqr_shards_create(&cfg, &r->s);
}

static moqr_result_t
sfrig_create(sfrig_t *r, ca_t *a, bool earned)
{
    return sfrig_create_ex(r, a, earned, 0, 0);
}

/* ACKs shard 0 has pushed toward any requester so far. */
static uint64_t
sfrig_owner_acks(sfrig_t *r)
{
    moqr_shards_stats_t st;
    memset(&st, 0, sizeof(st));
    if (moqr_shards_get_stats(r->s, 0, &st) != MOQR_OK) {
        r->failures++;
        return 0;
    }
    return st.enqueued[MOQR_SHARDS_MSG_ACK];
}

static void
sfrig_destroy(sfrig_t *r)
{
    moqr_shards_destroy(r->s);
    for (int i = 0; i < SF_MAX_CONNS; i++) {
        if (r->conns[i].used) {
            moq_simpair_destroy(r->conns[i].sp);
            r->conns[i].used = false;
        }
    }
}

static sfconn_t *
sfrig_connect(sfrig_t *r, uint16_t shard)
{
    int slot = -1;
    for (int i = 0; i < SF_MAX_CONNS; i++) {
        if (!r->conns[i].used) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        return NULL;
    }
    sfconn_t *cn = &r->conns[slot];
    memset(cn, 0, sizeof(*cn));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc->vt;
    cfg.seed = 0xF111 + (uint64_t)slot;
    cfg.version = MOQ_VERSION_DRAFT_22;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    cfg.server_streaming_objects = g_sf_server_streaming;
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return NULL;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(moqr_shards_bind(r->s, shard), cn->rsess,
                            MOQ_VERSION_DRAFT_22) != MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        return NULL;
    }
    cn->shard = shard;
    cn->used = true;
    return cn;
}

/* One deterministic cycle `dt_us` later: transport steps for every
 * connection, then one round of the multi-shard runtime. */
static void
sfrig_cycle_round(sfrig_t *r)
{
    for (int i = 0; i < SF_MAX_CONNS; i++) {
        sfconn_t *cn = &r->conns[i];
        if (!cn->used || cn->transport_paused) {
            continue;
        }
        moq_result_t arc = moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        moq_result_t qrc =
            moq_simpair_run_until_quiescent(cn->sp, SF_STEP_BOUND, &steps);
        SF_CHECK(r, cn->closed || (arc == MOQ_OK && qrc == MOQ_OK));
    }
    SF_CHECK(r, moqr_shards_step(r->s, r->now) == MOQR_OK);
}

/* One cycle of the wake-driven runtime, the shape of the threaded relay: a
 * shard runs only when its own transport delivered work toward a relay-side
 * session, another shard's step named it in the returned wake set, or its
 * binding's fill deadline is due (the deadline the runtime folds into the
 * lane's wait). Each earned step is followed by one more step of the same
 * shard: the managed facades re-run a lane's pump while the pump left
 * session events queued (a request accepted inside a pump queues the events
 * it owes for the next one), and one extra step stands in for that re-drive.
 * Nothing else steps a shard. */
static void
sfrig_step_earned(sfrig_t *r, uint16_t sh, uint64_t *pending)
{
    if ((r->frozen >> sh) & 1u) {
        r->carried |= 1ull << sh;
        return;
    }
    uint64_t woke = 0;
    SF_CHECK(r, moqr_shards_step_shard(r->s, sh, r->now, &woke) == MOQR_OK);
    *pending |= woke;
    if (r->freeze_owner_on_ack && sh == 0 &&
        sfrig_owner_acks(r) > r->ack_base) {
        r->frozen |= 1u;
        r->freeze_owner_on_ack = false;
    }
}

static void
sfrig_cycle_earned(sfrig_t *r)
{
    uint64_t pending = r->carried & ~r->frozen;
    r->carried &= r->frozen;
    for (int i = 0; i < SF_MAX_CONNS; i++) {
        sfconn_t *cn = &r->conns[i];
        if (!cn->used || cn->transport_paused) {
            continue;
        }
        moq_result_t arc = moq_simpair_advance_to(cn->sp, r->now);
        SF_CHECK(r, cn->closed || arc == MOQ_OK);
        bool quiesced = false;
        for (int st = 0; st < SF_STEP_BOUND; st++) {
            size_t to_srv = 0, to_cli = 0;
            moq_result_t src = moq_simpair_step_directional(cn->sp, &to_srv,
                                                            &to_cli);
            SF_CHECK(r, cn->closed || src == MOQ_OK);
            if (src != MOQ_OK) {
                quiesced = true;
                break;
            }
            if (to_srv > 0) {
                pending |= 1ull << cn->shard;
            }
            if (to_srv == 0 && to_cli == 0) {
                quiesced = true;
                break;
            }
        }
        SF_CHECK(r, quiesced);
    }
    for (uint16_t sh = 0; sh < moqr_shards_count(r->s); sh++) {
        uint64_t d = moqr_bind_next_deadline_us(moqr_shards_bind(r->s, sh));
        if (d <= r->now) {
            pending |= 1ull << sh;
        }
    }
    int guard = 0;
    for (; (pending & ~r->frozen) != 0 && guard < SF_STEP_BOUND; guard++) {
        uint16_t sh = 0;
        while (((pending >> sh) & 1u) == 0 || ((r->frozen >> sh) & 1u)) {
            sh++;
        }
        pending &= ~(1ull << sh);
        uint64_t again = 0;
        sfrig_step_earned(r, sh, &pending);
        sfrig_step_earned(r, sh, &again);
        pending |= again & ~(1ull << sh);
    }
    r->carried |= pending & r->frozen;
    SF_CHECK(r, (pending & ~r->frozen) == 0);
}

static void
sfrig_cycle(sfrig_t *r, uint64_t dt_us)
{
    r->now += dt_us;
    if (r->earned) {
        sfrig_cycle_earned(r);
    } else {
        sfrig_cycle_round(r);
    }
}

static void
sfrig_pump(sfrig_t *r, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        sfrig_cycle(r, 1000);
    }
}

/* The earliest fill deadline any shard's binding reports. */
static uint64_t
sfrig_next_deadline(sfrig_t *r)
{
    uint64_t next = UINT64_MAX;
    for (uint16_t i = 0; i < moqr_shards_count(r->s); i++) {
        uint64_t d = moqr_bind_next_deadline_us(moqr_shards_bind(r->s, i));
        if (d < next) {
            next = d;
        }
    }
    return next;
}

/* The earliest relay-core deadline over the shards: the time-driven work the
 * cores keep for themselves (upstream linger, idle track retention). */
static uint64_t
sfrig_core_deadline(sfrig_t *r)
{
    uint64_t next = UINT64_MAX;
    for (uint16_t i = 0; i < moqr_shards_count(r->s); i++) {
        uint64_t d = moqr_core_next_deadline_us(moqr_shards_core(r->s, i));
        if (d < next) {
            next = d;
        }
    }
    return next;
}

/* -- peer observation ---------------------------------------------------------- */

#define SF_IT_OBJ  1u
#define SF_IT_GAP  2u
#define SF_IT_TERM 3u

typedef struct sfps {
    /* origin side: the Largest Object its acceptance reports, if any */
    bool               accept_with_largest;
    uint64_t           accept_lg, accept_lo;
    moq_subscription_t up_sub;     /* the relay's subscription at the origin */
    bool               up_seen;
    bool               namespace_accepted;
    bool               subscribe_ok;
    int                subscribe_errors;
    bool               has_largest;
    uint64_t           largest_group, largest_object;
    int                objects;            /* subscription (live) Objects   */
    int                fill_created;
    moq_fill_t         fill;
    int                fill_objects;
    uint64_t           fill_obj[8][2];     /* (group, object) in order      */
    int                fill_gaps;
    struct {
        uint32_t kind;
        uint64_t group, object;
    }                  fill_gap[8];
    /* The fill stream as the subscriber saw it, item by item. */
    struct {
        uint8_t  kind;                     /* SF_IT_*                       */
        uint64_t group, object;
        uint32_t detail;                   /* gap kind / terminal reason    */
        uint8_t  body[4];
        size_t   len;
        uint64_t subgroup;
        uint8_t  priority;
        bool     datagram;
        uint8_t  properties[8];
        size_t   properties_len;
    }                  items[16];
    int                n_items;
    uint64_t           first_fill_obj_at;  /* rig time of the first Object */
    uint64_t           live_obj[SF_LIVE_MAX][2]; /* live (group, object) */
    int                fill_terminals;
    uint64_t           fill_term_at;       /* rig time the terminal was seen */
    uint32_t           fill_term_reason;
    uint32_t           fill_term_domain;
    bool               done_seen;
    bool               session_closed;
    /* A FETCH's answer. */
    bool               fetch_ok;
    uint64_t           fok_end_group, fok_end_object;
    int                fetch_errors;
    uint64_t           fetch_error_code;
    int                fetch_n;
    uint64_t           fetch_obj[8][2];    /* (group, object) in order      */
    int                fetch_gap_n;
    struct {
        uint32_t kind;
        uint64_t group, object;
    }                  fetch_gap[8];
    int                fetch_complete;
} sfps_t;

static void
sfps_drain(sfrig_t *r, sfconn_t *cn, sfps_t *ps)
{
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST: {
                ps->up_sub = ev->u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t cfg;
                moq_accept_subscribe_cfg_init(&cfg);
                cfg.has_largest = ps->accept_with_largest;
                cfg.largest_group = ps->accept_lg;
                cfg.largest_object = ps->accept_lo;
                SF_CHECK(r, moq_session_accept_subscribe(
                                cn->peer, ev->u.subscribe_request.sub, &cfg,
                                r->now) == MOQ_OK);
                ps->up_seen = true;
                break;
            }
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                ps->namespace_accepted = true;
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                ps->subscribe_ok = true;
                ps->has_largest = ev->u.subscribe_ok.has_largest;
                ps->largest_group = ev->u.subscribe_ok.largest_group;
                ps->largest_object = ev->u.subscribe_ok.largest_object;
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                ps->subscribe_errors++;
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                if (ps->objects < SF_LIVE_MAX) {
                    ps->live_obj[ps->objects][0] =
                        ev->u.object_received.group_id;
                    ps->live_obj[ps->objects][1] =
                        ev->u.object_received.object_id;
                }
                ps->objects++;
                break;
            case MOQ_EVENT_FILL_CREATED:
                ps->fill_created++;
                ps->fill = ev->u.fill_created.fill;
                break;
            case MOQ_EVENT_FILL_OBJECT:
                if (ps->n_items < 16) {
                    int k = ps->n_items++;
                    ps->items[k].kind = SF_IT_OBJ;
                    ps->items[k].group = ev->u.fill_object.group_id;
                    ps->items[k].object = ev->u.fill_object.object_id;
                    ps->items[k].subgroup = ev->u.fill_object.subgroup_id;
                    ps->items[k].priority = ev->u.fill_object.publisher_priority;
                    ps->items[k].datagram = ev->u.fill_object.datagram;
                    size_t pn = moq_rcbuf_len(ev->u.fill_object.properties);
                    ps->items[k].properties_len = pn;
                    if (pn > 0 && pn <= sizeof(ps->items[k].properties)) {
                        memcpy(ps->items[k].properties,
                               moq_rcbuf_data(ev->u.fill_object.properties), pn);
                    }
                    size_t len = moq_rcbuf_len(ev->u.fill_object.payload);
                    ps->items[k].len = len;
                    if (len > 0 && len <= sizeof(ps->items[k].body)) {
                        memcpy(ps->items[k].body,
                               moq_rcbuf_data(ev->u.fill_object.payload), len);
                    }
                }
                if (ps->fill_objects == 0) {
                    ps->first_fill_obj_at = r->now;
                }
                if (ps->fill_objects < 8) {
                    ps->fill_obj[ps->fill_objects][0] =
                        ev->u.fill_object.group_id;
                    ps->fill_obj[ps->fill_objects][1] =
                        ev->u.fill_object.object_id;
                }
                ps->fill_objects++;
                break;
            case MOQ_EVENT_FILL_GAP:
                if (ps->n_items < 16) {
                    int k = ps->n_items++;
                    ps->items[k].kind = SF_IT_GAP;
                    ps->items[k].group = ev->u.fill_gap.group_id;
                    ps->items[k].object = ev->u.fill_gap.object_id;
                    ps->items[k].detail = ev->u.fill_gap.kind;
                }
                if (ps->fill_gaps < 8) {
                    ps->fill_gap[ps->fill_gaps].kind = ev->u.fill_gap.kind;
                    ps->fill_gap[ps->fill_gaps].group = ev->u.fill_gap.group_id;
                    ps->fill_gap[ps->fill_gaps].object =
                        ev->u.fill_gap.object_id;
                }
                ps->fill_gaps++;
                break;
            case MOQ_EVENT_FILL_TERMINAL:
                if (ps->n_items < 16) {
                    int k = ps->n_items++;
                    ps->items[k].kind = SF_IT_TERM;
                    ps->items[k].detail = ev->u.fill_terminal.reason;
                }
                ps->fill_terminals++;
                ps->fill_term_at = r->now;
                ps->fill_term_reason = ev->u.fill_terminal.reason;
                ps->fill_term_domain = ev->u.fill_terminal.error_domain;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                ps->done_seen = true;
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                ps->session_closed = true;
                break;
            case MOQ_EVENT_FETCH_OK:
                ps->fetch_ok = true;
                ps->fok_end_group = ev->u.fetch_ok.end_group;
                ps->fok_end_object = ev->u.fetch_ok.end_object;
                break;
            case MOQ_EVENT_FETCH_ERROR:
                ps->fetch_errors++;
                ps->fetch_error_code = ev->u.fetch_error.error_code;
                break;
            case MOQ_EVENT_FETCH_OBJECT:
                if (ps->fetch_n < 8) {
                    ps->fetch_obj[ps->fetch_n][0] = ev->u.fetch_object.group_id;
                    ps->fetch_obj[ps->fetch_n][1] = ev->u.fetch_object.object_id;
                }
                ps->fetch_n++;
                break;
            case MOQ_EVENT_FETCH_GAP:
                if (ps->fetch_gap_n < 8) {
                    ps->fetch_gap[ps->fetch_gap_n].kind = ev->u.fetch_gap.range_kind;
                    ps->fetch_gap[ps->fetch_gap_n].group = ev->u.fetch_gap.group_id;
                    ps->fetch_gap[ps->fetch_gap_n].object =
                        ev->u.fetch_gap.object_id;
                }
                ps->fetch_gap_n++;
                break;
            case MOQ_EVENT_FETCH_COMPLETE:
                ps->fetch_complete++;
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* -- client-side operations ----------------------------------------------------- */

/* Publish exactly the given object ids on one subgroup of `group`, then close
 * the subgroup with a plain FIN (no End of Group). */
static void
sf_pub_ids(sfrig_t *r, sfconn_t *pub, moq_subscription_t up_sub,
           uint64_t group, uint64_t subgroup, const uint64_t *ids, size_t n)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    SF_CHECK(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh) == MOQ_OK);
    for (size_t i = 0; i < n; i++) {
        uint8_t body[4] = { (uint8_t)group, (uint8_t)ids[i], 0xC3, 0x3C };
        moq_rcbuf_t *pl = NULL;
        SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body),
                                     &pl) == MOQ_OK);
        SF_CHECK(r, moq_session_write_object(pub->peer, sgh, ids[i], pl,
                                             r->now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    SF_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
}

static moq_bytes_t g_nsp[2];

static void
sf_subscribe_cfg(moq_subscribe_cfg_t *scfg)
{
    g_nsp[0] = B("live");
    g_nsp[1] = B("cam");
    moq_subscribe_cfg_init(scfg);
    scfg->track_namespace = (moq_namespace_t){ .parts = g_nsp, .count = 2 };
    scfg->track_name = B("v");
}

/* Subscribe Next Object with a fill over {0,0}..Largest carrying the given
 * FILL_TIMEOUT (or none). */
static void
sf_subscribe_fill_ordered(sfrig_t *r, sfconn_t *cn, bool has_timeout,
                          uint64_t timeout_ms, bool descending)
{
    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    fill.has_fill_timeout = has_timeout;
    fill.fill_timeout_ms = timeout_ms;
    moq_subscribe_cfg_t scfg;
    sf_subscribe_cfg(&scfg);
    scfg.location = &loc;
    scfg.fill = &fill;
    scfg.group_order = descending ? MOQ_GROUP_ORDER_DESCENDING
                                  : MOQ_GROUP_ORDER_ASCENDING;
    moq_subscription_t h;
    SF_CHECK(r, moq_session_subscribe(cn->peer, &scfg, r->now, &h) == MOQ_OK);
}

static void
sf_subscribe_fill(sfrig_t *r, sfconn_t *cn, bool has_timeout,
                  uint64_t timeout_ms)
{
    sf_subscribe_fill_ordered(r, cn, has_timeout, timeout_ms, false);
}

/* The origin layout every case starts from: the publisher and a priming
 * subscriber (Absolute Start) on shard 0; the publisher writes {0,0} and
 * {0,2} of group 0 on one subgroup, closes it and falls silent. Largest
 * Object is {0,2}; {0,1} is a hole inside group 0's extent. */
typedef struct sf_origin {
    sfconn_t *pub;
    sfconn_t *prime;
    sfps_t    pub_ps;
    sfps_t    prime_ps;
} sf_origin_t;

/* Connect the publisher on shard 0 and have its namespace accepted. */
static void
sf_announce(sfrig_t *r, sf_origin_t *o)
{
    o->pub = sfrig_connect(r, 0);
    SF_CHECK(r, o->pub != NULL);
    if (o->pub == NULL) {
        return;
    }
    sfrig_pump(r, 4);
    g_nsp[0] = B("live");
    g_nsp[1] = B("cam");
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = g_nsp, .count = 2 };
    moq_announcement_t ann;
    SF_CHECK(r, moq_session_publish_namespace(o->pub->peer, &pcfg, r->now,
                                              &ann) == MOQ_OK);
    sfrig_pump(r, 8);
    sfps_drain(r, o->pub, &o->pub_ps);
    SF_CHECK(r, o->pub_ps.namespace_accepted);
}

static void
sf_origin_setup_ids(sfrig_t *r, sf_origin_t *o, const uint64_t *ids,
                    size_t n);

static void
sf_origin_setup(sfrig_t *r, sf_origin_t *o)
{
    static const uint64_t ids[2] = { 0, 2 };
    sf_origin_setup_ids(r, o, ids, 2);
}

static void
sf_origin_setup_ids(sfrig_t *r, sf_origin_t *o, const uint64_t *ids,
                    size_t n)
{
    memset(o, 0, sizeof(*o));
    sf_announce(r, o);
    o->prime = sfrig_connect(r, 0);
    SF_CHECK(r, o->pub != NULL && o->prime != NULL);
    if (o->pub == NULL || o->prime == NULL) {
        return;
    }
    sfrig_pump(r, 4);
    moq_subscribe_cfg_t scfg;
    sf_subscribe_cfg(&scfg);
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t ph;
    SF_CHECK(r, moq_session_subscribe(o->prime->peer, &scfg, r->now, &ph) ==
                    MOQ_OK);
    sfrig_pump(r, 6);
    sfps_drain(r, o->pub, &o->pub_ps);
    SF_CHECK(r, o->pub_ps.up_seen);
    sfrig_pump(r, 6);
    sf_pub_ids(r, o->pub, o->pub_ps.up_sub, 0, 0, ids, n);
    sfrig_pump(r, 8);
    sfps_drain(r, o->prime, &o->prime_ps);
    SF_CHECK(r, o->prime_ps.subscribe_ok && o->prime_ps.objects == (int)n);
}

/* Advance the rig in 1 ms cycles until the fill's terminal arrives or
 * `limit_us` of rig time passes. */
static void
sf_run_until_terminal(sfrig_t *r, sfconn_t *cn, sfps_t *ps, uint64_t limit_us)
{
    uint64_t until = r->now + limit_us;
    while (ps->fill_terminals == 0 && r->now < until && r->failures == 0) {
        sfrig_cycle(r, 1000);
        sfps_drain(r, cn, ps);
    }
}


/* Advance the rig in 1 ms cycles until `until_us`, draining both peers. */
static void
sf_run_to(sfrig_t *r, sfconn_t *a, sfps_t *aps, sfconn_t *b, sfps_t *bps,
          uint64_t until_us)
{
    while (r->now + 1000u <= until_us && r->failures == 0) {
        sfrig_cycle(r, 1000);
        sfps_drain(r, a, aps);
        if (b != NULL) {
            sfps_drain(r, b, bps);
        }
    }
}

/* The live edge still flows to the fill subscriber after its fill ended, and
 * nothing on either binding was counted as an error. */
static void
sf_expect_subscription_lives(sfrig_t *r, sf_origin_t *o, sfconn_t *sub,
                             sfps_t *ps, uint64_t live_group)
{
    static const uint64_t live[1] = { 0 };
    int before = ps->objects;
    sf_pub_ids(r, o->pub, o->pub_ps.up_sub, live_group, 0, live, 1);
    sfrig_pump(r, 10);
    sfps_drain(r, sub, ps);
    SF_CHECK(r, ps->objects == before + 1);
    SF_CHECK(r, ps->fill_terminals == 1);
    SF_CHECK(r, !ps->done_seen && !ps->session_closed);
    for (uint16_t i = 0; i < 2; i++) {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(moqr_shards_bind(r->s, i), &bs);
        SF_CHECK(r, bs.session_errors == 0);
    }
}

static int
sf_finish(sfrig_t *r, ca_t *a, const char *label)
{
    int f = r->failures;
    sfrig_destroy(r);
    if (a->live != 0) {
        printf("FAIL: %s: %ld bytes leaked\n", label, a->live);
        f++;
    }
    if (f == 0) {
        printf("PASS: %s\n", label);
    }
    return f;
}

/* -- cases ---------------------------------------------------------------------- */

/* An item of a fill stream, for the expectations below. */
typedef struct sf_item {
    uint8_t  kind;
    uint64_t group, object;
    uint32_t detail;
} sf_item_t;

/* The subscriber saw exactly `want` (Objects also carry the body the
 * publisher wrote for that Location). */
static void
sf_expect_items(sfrig_t *r, const sfps_t *ps, const sf_item_t *want, int n,
                const char *who)
{
    bool ok = n >= 0 && ps->n_items == n;
    for (int i = 0; ok && i < n; i++) {
        ok = ps->items[i].kind == want[i].kind &&
             ps->items[i].detail == want[i].detail &&
             (want[i].kind == SF_IT_TERM ||
              (ps->items[i].group == want[i].group &&
               ps->items[i].object == want[i].object));
        if (ok && want[i].kind == SF_IT_OBJ) {
            ok = ps->items[i].len == 4 &&
                 ps->items[i].body[0] == (uint8_t)want[i].group &&
                 ps->items[i].body[1] == (uint8_t)want[i].object;
        }
    }
    if (!ok) {
        printf("  [%s] got %d item(s):", who, ps->n_items);
        for (int i = 0; i < ps->n_items && i < 16; i++) {
            printf(" %u:{%llu,%llu}/%u", ps->items[i].kind,
                   (unsigned long long)ps->items[i].group,
                   (unsigned long long)ps->items[i].object,
                   ps->items[i].detail);
        }
        printf("\n");
    }
    SF_CHECK(r, ok);
}

/* Two fill streams are the same item for item, bodies included. */
static void
sf_expect_same_items(sfrig_t *r, const sfps_t *a, const sfps_t *b)
{
    bool ok = a->n_items == b->n_items;
    for (int i = 0; ok && i < a->n_items && i < 16; i++) {
        ok = a->items[i].kind == b->items[i].kind &&
             a->items[i].group == b->items[i].group &&
             a->items[i].object == b->items[i].object &&
             a->items[i].detail == b->items[i].detail &&
             a->items[i].len == b->items[i].len &&
             memcmp(a->items[i].body, b->items[i].body, a->items[i].len) == 0;
    }
    SF_CHECK(r, ok);
}

static uint64_t
sf_ingested(sfrig_t *r, uint16_t shard)
{
    moqr_core_stats_t st;
    memset(&st, 0, sizeof(st));
    moqr_core_get_stats(moqr_shards_core(r->s, shard), &st);
    return st.ingested_total;
}

/* Nothing is queued or held on any demand channel: every retrieval answer
 * the fills consumed returned its charge to the channel that carried it. */
static void
sf_expect_channels_idle(sfrig_t *r)
{
    for (uint16_t i = 0; i < moqr_shards_count(r->s); i++) {
        moqr_shards_stats_t st;
        memset(&st, 0, sizeof(st));
        SF_CHECK(r, moqr_shards_get_stats(r->s, i, &st) == MOQR_OK);
        SF_CHECK(r, st.inbound_channel_entries == 0 &&
                        st.inbound_channel_bytes == 0);
    }
}

/* Subscribe at the live edge without a fill. */
static void
sf_subscribe_live(sfrig_t *r, sfconn_t *cn)
{
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscribe_cfg_t scfg;
    sf_subscribe_cfg(&scfg);
    scfg.location = &loc;
    moq_subscription_t h;
    SF_CHECK(r, moq_session_subscribe(cn->peer, &scfg, r->now, &h) == MOQ_OK);
}

/* A draft-22 FETCH of live/cam/v in the given semantic form (UINT32_MAX: no
 * filter) with a FILL_TIMEOUT of 0, answered from a clean observation. */
static void
sf_fetch(sfrig_t *r, sfconn_t *cn, sfps_t *ps, uint32_t kind, uint64_t rel)
{
    g_nsp[0] = B("live");
    g_nsp[1] = B("cam");
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = kind == UINT32_MAX ? MOQ_LOCATION_FILTER_NONE : kind;
    loc.relative_group_offset = rel;
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.track_namespace = (moq_namespace_t){ .parts = g_nsp, .count = 2 };
    fc.track_name = B("v");
    fc.location = &loc;
    fc.has_fill_timeout = true;
    fc.fill_timeout_ms = 0;
    moq_fetch_t fh;
    SF_CHECK(r, moq_session_fetch(cn->peer, &fc, r->now, &fh) == MOQ_OK);
    sfrig_pump(r, 12);
    memset(ps, 0, sizeof(*ps));
    sfps_drain(r, cn, ps);
}

/* The answer to a whole-track FETCH while the relay holds {0,0} and {0,2}
 * (Largest {0,2}), or also {1,0} (Largest {1,0}): the hole at {0,1} and, once
 * group 1 exists, the rest of group 0 (no END_OF_GROUP) are what a
 * FILL_TIMEOUT of 0 marks Timed-Out without asking upstream. */
static void
sf_expect_whole_track(sfrig_t *r, const sfps_t *ps, bool group1)
{
    SF_CHECK(r, ps->fetch_ok && ps->fetch_errors == 0 && ps->fetch_complete == 1);
    SF_CHECK(r, ps->fok_end_group == (group1 ? 1u : 0u) &&
                    ps->fok_end_object == (group1 ? 1u : 3u));
    SF_CHECK(r, ps->fetch_n == (group1 ? 3 : 2) &&
                    ps->fetch_gap_n == (group1 ? 2 : 1));
    if (ps->fetch_n == (group1 ? 3 : 2) && ps->fetch_gap_n == (group1 ? 2 : 1)) {
        SF_CHECK(r, ps->fetch_obj[0][0] == 0 && ps->fetch_obj[0][1] == 0 &&
                        ps->fetch_obj[1][0] == 0 && ps->fetch_obj[1][1] == 2);
        SF_CHECK(r, ps->fetch_gap[0].kind == MOQ_FETCH_RANGE_TIMED_OUT &&
                        ps->fetch_gap[0].group == 0 && ps->fetch_gap[0].object == 1);
        if (group1) {
            SF_CHECK(r, ps->fetch_obj[2][0] == 1 && ps->fetch_obj[2][1] == 0);
            SF_CHECK(r, ps->fetch_gap[1].kind == MOQ_FETCH_RANGE_TIMED_OUT &&
                            ps->fetch_gap[1].group == 0 &&
                            ps->fetch_gap[1].object == UINT64_MAX);
        }
    }
}

/* A FETCH is answered with the relay's Largest Object and everything the
 * relay holds whatever shard it lands on. The owner shard resolves it against
 * its own log. Any other shard -- holding no copy of the track, holding a
 * copy the owner feeds, or holding one the owner no longer feeds -- has the
 * owner answer it, so the response never depends on which shard the
 * subscriber's connection landed on, and no shard's copy bounds it by a
 * Largest that lags. */
static int
cross_shard_fetch_range(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t rig;
    if (sfrig_create(&rig, &a, false) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&rig, &o);   /* group 0: {0,0}, {0,2}; Largest {0,2} */
    sfconn_t *f1 = sfrig_connect(&rig, 1);
    sfconn_t *f0 = sfrig_connect(&rig, 0);
    SF_CHECK(&rig, f1 != NULL && f0 != NULL);
    sfrig_pump(&rig, 6);
    sfps_t ps;

    /* No copy on shard 1: the owner answers. */
    sf_fetch(&rig, f1, &ps, UINT32_MAX, 0);
    sf_expect_whole_track(&rig, &ps, false);

    /* The owner resolves against its own log: the same answer. */
    sf_fetch(&rig, f0, &ps, UINT32_MAX, 0);
    sf_expect_whole_track(&rig, &ps, false);

    /* A live subscriber on shard 1 has the owner feed it; group 1 arrives
     * through the feed. */
    sfconn_t *live = sfrig_connect(&rig, 1);
    SF_CHECK(&rig, live != NULL);
    sfrig_pump(&rig, 4);
    sf_subscribe_live(&rig, live);
    sfrig_pump(&rig, 12);
    sfps_t live_ps;
    memset(&live_ps, 0, sizeof(live_ps));
    sfps_drain(&rig, live, &live_ps);
    SF_CHECK(&rig, live_ps.subscribe_ok);
    static const uint64_t g1[1] = { 0 };
    sf_pub_ids(&rig, o.pub, o.pub_ps.up_sub, 1, 0, g1, 1);
    sfrig_pump(&rig, 12);
    sfps_drain(&rig, live, &live_ps);
    SF_CHECK(&rig, live_ps.objects == 1);

    /* Fed: still the owner's answer, through its Largest {1,0}, including
     * group 0, which was never fed here. */
    sf_fetch(&rig, f1, &ps, UINT32_MAX, 0);
    sf_expect_whole_track(&rig, &ps, true);
    /* One group back from the next: group 1 alone. */
    sf_fetch(&rig, f1, &ps, MOQ_LOCATION_FILTER_RELATIVE_GROUP, 1);
    SF_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 1 && ps.fok_end_object == 1);
    SF_CHECK(&rig, ps.fetch_gap_n == 0 && ps.fetch_n == 1 && ps.fetch_complete == 1);

    /* The live subscriber leaves: the copy is no longer fed, and the owner
     * still answers. */
    SF_CHECK(&rig, moq_session_close(live->peer, 0, "done", rig.now) == MOQ_OK);
    live->closed = true;
    sfrig_pump(&rig, 20);
    sf_fetch(&rig, f1, &ps, MOQ_LOCATION_FILTER_RELATIVE_GROUP, 1);
    SF_CHECK(&rig, ps.fetch_ok && ps.fok_end_group == 1 && ps.fok_end_object == 1);
    SF_CHECK(&rig, ps.fetch_gap_n == 0 && ps.fetch_n == 1 && ps.fetch_complete == 1);
    SF_CHECK(&rig, !ps.session_closed);
    for (uint16_t i = 0; i < 2; i++) {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(moqr_shards_bind(rig.s, i), &bs);
        SF_CHECK(&rig, bs.session_errors == 0);
    }
    sf_expect_channels_idle(&rig);
    return sf_finish(&rig, &a, label);
}

/* Write `len` bytes of the active streamed object. */
static void
sf_write_data(sfrig_t *r, sfconn_t *pub, moq_subgroup_handle_t sgh, uint8_t b0,
              uint8_t b1, size_t len)
{
    uint8_t body[4] = { b0, b1, 0xC3, 0x3C };
    moq_rcbuf_t *pl = NULL;
    SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, len, &pl) == MOQ_OK);
    SF_CHECK(r, moq_session_write_object_data(pub->peer, sgh, pl, r->now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
}

/* The owner's Largest Object is an Object still arriving: a fill on the other
 * shard is bounded by it (the owner's acceptance reports it) and retrieves it
 * from the owner, which waits for it as for any Object it does not hold yet
 * and answers it whole once it completes -- or, if it is still arriving at
 * the fill's deadline, answers its Location as a run it does not hold, which
 * the fill reports Timed-Out. */
static int
cross_shard_fill_in_progress(bool completes, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t rig;
    if (sfrig_create(&rig, &a, false) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    memset(&o, 0, sizeof(o));
    g_sf_server_streaming = true;   /* the publisher's objects arrive chunked */
    sf_announce(&rig, &o);
    g_sf_server_streaming = false;
    o.prime = sfrig_connect(&rig, 0);
    SF_CHECK(&rig, o.pub != NULL && o.prime != NULL);
    sfrig_pump(&rig, 4);
    moq_subscribe_cfg_t scfg;
    sf_subscribe_cfg(&scfg);
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t ph;
    SF_CHECK(&rig, moq_session_subscribe(o.prime->peer, &scfg, rig.now, &ph) ==
                       MOQ_OK);
    sfrig_pump(&rig, 6);
    sfps_drain(&rig, o.pub, &o.pub_ps);
    SF_CHECK(&rig, o.pub_ps.up_seen);
    sfrig_pump(&rig, 6);

    /* {0,0} whole, then {0,1} begun: two of its four bytes. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 0;
    sgc.subgroup_id = 0;
    moq_subgroup_handle_t sgh;
    SF_CHECK(&rig, moq_session_open_subgroup(o.pub->peer, o.pub_ps.up_sub, &sgc,
                                             rig.now, &sgh) == MOQ_OK);
    SF_CHECK(&rig, moq_session_begin_object(o.pub->peer, sgh, 0, 4, rig.now) ==
                       MOQ_OK);
    sf_write_data(&rig, o.pub, sgh, 0, 0, 4);
    SF_CHECK(&rig, moq_session_end_object(o.pub->peer, sgh, rig.now) == MOQ_OK);
    SF_CHECK(&rig, moq_session_begin_object(o.pub->peer, sgh, 1, 4, rig.now) ==
                       MOQ_OK);
    sf_write_data(&rig, o.pub, sgh, 0, 1, 2);
    sfrig_pump(&rig, 8);

    sfconn_t *sub = sfrig_connect(&rig, 1);
    SF_CHECK(&rig, sub != NULL);
    sfrig_pump(&rig, 4);
    sf_subscribe_fill(&rig, sub, true, completes ? 500 : 60);
    sfps_t ps;
    memset(&ps, 0, sizeof(ps));
    sf_run_to(&rig, sub, &ps, NULL, NULL, rig.now + 40000u);
    SF_CHECK(&rig, ps.subscribe_ok && ps.has_largest &&
                       ps.largest_group == 0 && ps.largest_object == 1);
    SF_CHECK(&rig, ps.fill_objects == 1 && ps.fill_gaps == 0 &&
                       ps.fill_terminals == 0);
    if (!completes) {
        sf_run_until_terminal(&rig, sub, &ps, 200000u);
        const sf_item_t gave_up[3] = {
            { SF_IT_OBJ, 0, 0, 0 },
            { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
            { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
        };
        sf_expect_items(&rig, &ps, gave_up, 3, "fill");
        SF_CHECK(&rig, !ps.done_seen && !ps.session_closed);
        return sf_finish(&rig, &a, label);
    }

    /* The rest of {0,1} arrives: answered whole, and the fill ends. */
    uint8_t rest[2] = { 0xC3, 0x3C };
    moq_rcbuf_t *pl = NULL;
    SF_CHECK(&rig, moq_rcbuf_create(&a.vt, rest, 2, &pl) == MOQ_OK);
    SF_CHECK(&rig, moq_session_write_object_data(o.pub->peer, sgh, pl, rig.now) ==
                       MOQ_OK);
    moq_rcbuf_decref(pl);
    SF_CHECK(&rig, moq_session_end_object(o.pub->peer, sgh, rig.now) == MOQ_OK);
    sf_run_until_terminal(&rig, sub, &ps, 100000u);
    const sf_item_t want[3] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_OBJ, 0, 1, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&rig, &ps, want, 3, "fill");
    SF_CHECK(&rig, !ps.done_seen && !ps.session_closed);
    return sf_finish(&rig, &a, label);
}

/* A FILL_TIMEOUT of zero leaves no time to retrieve anything: on the shard
 * that does not own the track the whole range is one End of Timed-Out Range
 * at Largest at once, then FIN, and the subscription is untouched. */
static int
cross_shard_fill_timeout_zero(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create(&r, &a, false) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&r, &o);
    sfconn_t *sub = sfrig_connect(&r, 1);
    SF_CHECK(&r, sub != NULL);
    if (sub == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, sub, true, 0);
    sfps_t ps;
    memset(&ps, 0, sizeof(ps));
    sf_run_until_terminal(&r, sub, &ps, 200000u);
    SF_CHECK(&r, ps.subscribe_ok && ps.fill_created == 1);
    static const sf_item_t want[] = {
        { SF_IT_GAP, 0, 2, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&r, &ps, want, 2, label);
    SF_CHECK(&r, ps.fill_term_at <= t0 + 10000u);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sf_expect_subscription_lives(&r, &o, sub, &ps, 1);
    return sf_finish(&r, &a, label);
}

/* The same fill asked of both shards at the same instant: one subscriber on
 * the owner shard (beside the publisher and the priming subscriber), one on
 * the other shard. Both streams are {0,0}, then {0,1} Timed-Out at the
 * deadline, then {0,2}, then FIN -- the same items, the same bodies, the same
 * timing bound. With `live_first` a live subscriber on the other shard has
 * pulled the track there before the fills; without it the fill subscriber is
 * the track's first subscriber on that shard. The retrieval never enters the
 * other shard's log: it has ingested nothing when the fills end, a live
 * Object afterwards reaches every subscriber exactly once, and a later
 * Absolute Start subscriber there receives only what that shard ingested. */
static int
cross_shard_fill_matches_owner(bool earned, bool live_first, bool has_timeout,
                               uint64_t timeout_ms, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create(&r, &a, earned) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&r, &o);
    sfconn_t *live = NULL;
    sfps_t lps;
    memset(&lps, 0, sizeof(lps));
    if (live_first) {
        live = sfrig_connect(&r, 1);
        SF_CHECK(&r, live != NULL);
        if (live == NULL) {
            return sf_finish(&r, &a, label);
        }
        sfrig_pump(&r, 4);
        sf_subscribe_live(&r, live);
        sfrig_pump(&r, 10);
        sfps_drain(&r, live, &lps);
        SF_CHECK(&r, lps.subscribe_ok && lps.has_largest &&
                         lps.largest_group == 0 && lps.largest_object == 2);
    }
    sfconn_t *of = sfrig_connect(&r, 0);
    sfconn_t *rf = sfrig_connect(&r, 1);
    SF_CHECK(&r, of != NULL && rf != NULL);
    if (of == NULL || rf == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    SF_CHECK(&r, sf_ingested(&r, 1) == 0);

    uint64_t t0 = r.now;
    uint64_t wait_us = (has_timeout ? timeout_ms : SF_DEFAULT_WAIT_MS) * 1000u;
    sf_subscribe_fill(&r, of, has_timeout, timeout_ms);
    sf_subscribe_fill(&r, rf, has_timeout, timeout_ms);
    sfps_t ops, rps;
    memset(&ops, 0, sizeof(ops));
    memset(&rps, 0, sizeof(rps));
    uint64_t until = r.now + wait_us + 200000u;
    while ((ops.fill_terminals == 0 || rps.fill_terminals == 0) &&
           r.now < until && r.failures == 0) {
        sfrig_cycle(&r, 1000);
        sfps_drain(&r, of, &ops);
        sfps_drain(&r, rf, &rps);
        if (live != NULL) {
            sfps_drain(&r, live, &lps);
        }
    }
    static const sf_item_t want[] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&r, &ops, want, 4, "owner shard");
    sf_expect_items(&r, &rps, want, 4, "other shard");
    sf_expect_same_items(&r, &ops, &rps);
    const sfps_t *both[2] = { &ops, &rps };
    for (int i = 0; i < 2; i++) {
        SF_CHECK(&r, both[i]->subscribe_ok && both[i]->fill_created == 1);
        SF_CHECK(&r, both[i]->has_largest && both[i]->largest_group == 0 &&
                         both[i]->largest_object == 2);
        /* what the relay holds is served at once, not at the deadline */
        SF_CHECK(&r, both[i]->first_fill_obj_at != 0 &&
                         both[i]->first_fill_obj_at < t0 + 20000u);
        SF_CHECK(&r, both[i]->fill_term_at >= t0 + wait_us);
        SF_CHECK(&r, both[i]->fill_term_at <= t0 + wait_us + 10000u);
        SF_CHECK(&r, both[i]->objects == 0);
    }
    SF_CHECK(&r, sf_ingested(&r, 1) == 0);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sfrig_pump(&r, 2);
    sf_expect_channels_idle(&r);

    /* One live Object: every subscriber gets exactly it. */
    static const uint64_t live_ids[1] = { 0 };
    sf_pub_ids(&r, o.pub, o.pub_ps.up_sub, 1, 0, live_ids, 1);
    sfrig_pump(&r, 10);
    sfps_drain(&r, of, &ops);
    sfps_drain(&r, rf, &rps);
    SF_CHECK(&r, ops.objects == 1 && ops.live_obj[0][0] == 1 &&
                     ops.live_obj[0][1] == 0);
    SF_CHECK(&r, rps.objects == 1 && rps.live_obj[0][0] == 1 &&
                     rps.live_obj[0][1] == 0);
    if (live != NULL) {
        sfps_drain(&r, live, &lps);
        SF_CHECK(&r, lps.objects == 1 && lps.live_obj[0][0] == 1 &&
                         lps.live_obj[0][1] == 0);
        SF_CHECK(&r, lps.n_items == 0);
    }
    SF_CHECK(&r, sf_ingested(&r, 1) == 1);
    /* A later Absolute Start subscriber on the other shard: that shard's
     * log holds only what its own feed brought. */
    sfconn_t *abs = sfrig_connect(&r, 1);
    SF_CHECK(&r, abs != NULL);
    if (abs != NULL) {
        sfrig_pump(&r, 4);
        moq_subscribe_cfg_t scfg;
        sf_subscribe_cfg(&scfg);
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        SF_CHECK(&r, moq_session_subscribe(abs->peer, &scfg, r.now, &h) ==
                         MOQ_OK);
        sfrig_pump(&r, 10);
        sfps_t aps;
        memset(&aps, 0, sizeof(aps));
        sfps_drain(&r, abs, &aps);
        SF_CHECK(&r, aps.subscribe_ok);
        SF_CHECK(&r, aps.objects == 1 && aps.live_obj[0][0] == 1 &&
                         aps.live_obj[0][1] == 0);
    }
    for (uint16_t i = 0; i < 2; i++) {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(moqr_shards_bind(r.s, i), &bs);
        SF_CHECK(&r, bs.session_errors == 0);
    }
    SF_CHECK(&r, !rps.done_seen && !rps.session_closed);
    return sf_finish(&r, &a, label);
}

/* A fill range that starts before the other shard's feed of the track and
 * ends inside it: the live subscriber pulled the track there when Largest
 * was {0,2}, then {1,0} and {1,1} were published and reached both shards.
 * The other shard retrieves only what precedes its feed and serves the rest
 * from its own log: both shards answer {0,0}, {0,1} Timed-Out at the
 * deadline, {0,2}, {1,0}, {1,1}, FIN. */
static int
cross_shard_fill_split(bool descending, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create(&r, &a, false) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&r, &o);
    sfconn_t *live = sfrig_connect(&r, 1);
    SF_CHECK(&r, live != NULL);
    if (live == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    sf_subscribe_live(&r, live);
    sfrig_pump(&r, 10);
    sfps_t lps;
    memset(&lps, 0, sizeof(lps));
    sfps_drain(&r, live, &lps);
    SF_CHECK(&r, lps.subscribe_ok && lps.largest_object == 2);
    static const uint64_t g1[2] = { 0, 1 };
    sf_pub_ids(&r, o.pub, o.pub_ps.up_sub, 1, 0, g1, 2);
    sfrig_pump(&r, 10);
    sfps_drain(&r, live, &lps);
    SF_CHECK(&r, lps.objects == 2);
    SF_CHECK(&r, sf_ingested(&r, 1) == 2);

    sfconn_t *of = sfrig_connect(&r, 0);
    sfconn_t *rf = sfrig_connect(&r, 1);
    SF_CHECK(&r, of != NULL && rf != NULL);
    if (of == NULL || rf == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    const uint64_t wait_us = 100000u;
    uint64_t t0 = r.now;
    sf_subscribe_fill_ordered(&r, of, true, wait_us / 1000u, descending);
    sf_subscribe_fill_ordered(&r, rf, true, wait_us / 1000u, descending);
    sfps_t ops, rps;
    memset(&ops, 0, sizeof(ops));
    memset(&rps, 0, sizeof(rps));
    uint64_t until = r.now + wait_us + 200000u;
    while ((ops.fill_terminals == 0 || rps.fill_terminals == 0) &&
           r.now < until && r.failures == 0) {
        sfrig_cycle(&r, 1000);
        sfps_drain(&r, of, &ops);
        sfps_drain(&r, rf, &rps);
    }
    /* Group 0 ends with no END_OF_GROUP evidence: the rest of it is of
     * unknown state, marked as soon as the fill reaches it -- after the wait
     * at {0,1} has spent the budget, so Timed-Out -- the same on both
     * shards. */
    static const sf_item_t want[] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_GAP, 0, UINT64_MAX, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 1, 0, 0 },
        { SF_IT_OBJ, 1, 1, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    static const sf_item_t want_descending[] = {
        { SF_IT_OBJ, 1, 0, 0 },
        { SF_IT_OBJ, 1, 1, 0 },
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_GAP, 0, UINT64_MAX, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    const sf_item_t *expected = descending ? want_descending : want;
    sf_expect_items(&r, &ops, expected, 7, "owner shard");
    sf_expect_items(&r, &rps, expected, 7, "other shard");
    sf_expect_same_items(&r, &ops, &rps);
    SF_CHECK(&r, rps.largest_group == 1 && rps.largest_object == 1);
    SF_CHECK(&r, rps.fill_term_at >= t0 + wait_us &&
                     rps.fill_term_at <= t0 + wait_us + 10000u);
    SF_CHECK(&r, sf_ingested(&r, 1) == 2);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sfrig_pump(&r, 2);
    sf_expect_channels_idle(&r);
    return sf_finish(&r, &a, label);
}

/* The owner shard stops running right after it accepted the demand, so it
 * never answers the retrieval before the fill's deadline: at the deadline
 * the fill is the whole range Timed-Out, then FIN. When the owner runs again
 * its late answer changes nothing on that fill, the live edge still flows,
 * and nothing is left behind. */
static int
cross_shard_answer_after_deadline(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create(&r, &a, true) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&r, &o);
    sfconn_t *sub = sfrig_connect(&r, 1);
    SF_CHECK(&r, sub != NULL);
    if (sub == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    const uint64_t wait_us = 60000u;
    r.ack_base = sfrig_owner_acks(&r);
    r.freeze_owner_on_ack = true;
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, sub, true, wait_us / 1000u);
    sfps_t ps;
    memset(&ps, 0, sizeof(ps));
    sf_run_until_terminal(&r, sub, &ps, wait_us + 200000u);
    SF_CHECK(&r, (r.frozen & 1u) != 0);
    SF_CHECK(&r, ps.subscribe_ok && ps.fill_created == 1);
    static const sf_item_t want[] = {
        { SF_IT_GAP, 0, 2, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&r, &ps, want, 2, label);
    SF_CHECK(&r, ps.fill_term_at >= t0 + wait_us &&
                     ps.fill_term_at <= t0 + wait_us + 10000u);
    /* The owner runs again and answers what it was asked. */
    r.frozen = 0;
    sfrig_pump(&r, 20);
    sfps_drain(&r, sub, &ps);
    sf_expect_items(&r, &ps, want, 2, label);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sf_expect_subscription_lives(&r, &o, sub, &ps, 1);
    return sf_finish(&r, &a, label);
}

/* Each directed channel holds one message, and the owner shard stops running
 * right after it accepted the demand. Two fills on the other shard each owe
 * the owner a retrieval request: the first fills the channel toward the
 * owner, so the second is deferred -- refused by the full channel, kept, and
 * never dropped.
 *
 * With `thaw_ms` past the deadline neither answer can begin in time, so both
 * fills end at their deadline with the whole range Timed-Out; when the owner
 * runs again it drains the first request and its withdrawal and nothing
 * changes on either fill. With `thaw_ms` inside the wait the owner runs again
 * first: the deferred request goes out once the channel has room, and both
 * fills receive the owner's answer -- {0,0} on the way, {0,1} Timed-Out at
 * the deadline, {0,2}, FIN. While the owner holds at {0,1} it is the owner
 * shard's binding that reports the deadline (the owner wakes for it by
 * itself), and the other shard's reports none (its answers have begun and
 * need no time there). Either way the live edge flows and nothing is left
 * behind. */
static int
cross_shard_full_channel_defers(uint64_t thaw_ms, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, true, 1, 0) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&r, &o);
    sfconn_t *f1 = sfrig_connect(&r, 1);
    sfconn_t *f2 = sfrig_connect(&r, 1);
    SF_CHECK(&r, f1 != NULL && f2 != NULL);
    if (f1 == NULL || f2 == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    moqr_shards_pair_stats_t before;
    memset(&before, 0, sizeof(before));
    SF_CHECK(&r, moqr_shards_get_pair_stats(r.s, 1, 0, &before,
                                            sizeof(before)) == MOQR_OK);
    const uint64_t wait_us = 60000u;
    r.ack_base = sfrig_owner_acks(&r);
    r.freeze_owner_on_ack = true;
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, f1, true, wait_us / 1000u);
    sf_subscribe_fill(&r, f2, true, wait_us / 1000u);
    sfps_t p1, p2;
    memset(&p1, 0, sizeof(p1));
    memset(&p2, 0, sizeof(p2));
    uint64_t until = r.now + wait_us + 200000u;
    bool was_frozen = false;
    bool sampled = false;
    while ((p1.fill_terminals == 0 || p2.fill_terminals == 0) &&
           r.now < until && r.failures == 0) {
        if ((r.frozen & 1u) != 0) {
            was_frozen = true;
        }
        if (r.now >= t0 + thaw_ms * 1000u) {
            r.frozen = 0;
        }
        if (thaw_ms * 1000u < wait_us && !sampled &&
            r.now >= t0 + (thaw_ms * 1000u + wait_us) / 2u) {
            sampled = true;
            uint64_t d0 = moqr_bind_next_deadline_us(moqr_shards_bind(r.s, 0));
            uint64_t d1 = moqr_bind_next_deadline_us(moqr_shards_bind(r.s, 1));
            SF_CHECK(&r, p1.fill_objects == 1 && p2.fill_objects == 1);
            SF_CHECK(&r, d0 >= t0 + wait_us && d0 <= t0 + wait_us + 5000u);
            SF_CHECK(&r, d1 == UINT64_MAX);
        }
        sfrig_cycle(&r, 1000);
        sfps_drain(&r, f1, &p1);
        sfps_drain(&r, f2, &p2);
    }
    SF_CHECK(&r, was_frozen);
    static const sf_item_t timed_out[] = {
        { SF_IT_GAP, 0, 2, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    static const sf_item_t answered[] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    const bool thawed = thaw_ms * 1000u < wait_us;
    const sf_item_t *want = thawed ? answered : timed_out;
    const int nwant = thawed ? 4 : 2;
    const sfps_t *both[2] = { &p1, &p2 };
    for (int i = 0; i < 2; i++) {
        SF_CHECK(&r, both[i]->subscribe_ok && both[i]->fill_created == 1);
        sf_expect_items(&r, both[i], want, nwant, label);
        SF_CHECK(&r, both[i]->fill_term_at >= t0 + wait_us &&
                         both[i]->fill_term_at <= t0 + wait_us + 10000u);
        if (thawed) {
            SF_CHECK(&r, both[i]->first_fill_obj_at >= t0 + thaw_ms * 1000u &&
                             both[i]->first_fill_obj_at < t0 + wait_us);
        }
    }
    moqr_shards_pair_stats_t after;
    memset(&after, 0, sizeof(after));
    SF_CHECK(&r, moqr_shards_get_pair_stats(r.s, 1, 0, &after,
                                            sizeof(after)) == MOQR_OK);
    SF_CHECK(&r, after.refused_entries > before.refused_entries);
    /* The owner runs again. */
    r.frozen = 0;
    sfrig_pump(&r, 20);
    sfps_drain(&r, f1, &p1);
    sfps_drain(&r, f2, &p2);
    sf_expect_items(&r, &p1, want, nwant, label);
    sf_expect_items(&r, &p2, want, nwant, label);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sf_expect_channels_idle(&r);
    sf_expect_subscription_lives(&r, &o, f1, &p1, 1);
    sfps_drain(&r, f2, &p2);
    SF_CHECK(&r, p2.objects == 1);
    return sf_finish(&r, &a, label);
}

/* Run a live stream on one open subgroup of group 1, one Object per rig
 * millisecond, from `from_us` to `to_us` of the cycles below; `fill_at_us`
 * (relative to the first cycle) subscribes `sub` with a fill whose
 * FILL_TIMEOUT is `wait_ms`, recording its instant in *t0. Then drain until
 * every live Object reached `live` and the fill ended. */
typedef struct sf_stream {
    sfconn_t *live, *sub, *of;   /* of: the same fill on the owner shard */
    sfps_t    lps, ps, ops;
    uint64_t  published;
    uint64_t  t0;
} sf_stream_t;

static void
sf_stream_run(sfrig_t *r, sf_origin_t *o, sf_stream_t *st, uint64_t from_ms,
              uint64_t to_ms, uint64_t fill_at_ms, uint64_t wait_ms)
{
    moq_subgroup_handle_t sgh;
    bool open = false;
    for (uint64_t ms = 0; ms <= to_ms && r->failures == 0; ms++) {
        if (ms == fill_at_ms) {
            st->t0 = r->now;
            sf_subscribe_fill(r, st->sub, true, wait_ms);
            sf_subscribe_fill(r, st->of, true, wait_ms);
        }
        if (ms == from_ms) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 1;
            sgc.subgroup_id = 0;
            SF_CHECK(r, moq_session_open_subgroup(o->pub->peer,
                                                  o->pub_ps.up_sub, &sgc,
                                                  r->now, &sgh) == MOQ_OK);
            open = true;
        }
        if (open) {
            uint8_t body[4] = { 1, (uint8_t)st->published, 0xC3, 0x3C };
            moq_rcbuf_t *pl = NULL;
            SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body),
                                         &pl) == MOQ_OK);
            SF_CHECK(r, moq_session_write_object(o->pub->peer, sgh,
                                                 st->published, pl,
                                                 r->now) == MOQ_OK);
            moq_rcbuf_decref(pl);
            st->published++;
        }
        sfrig_cycle(r, 1000);
        sfps_drain(r, o->pub, &o->pub_ps);
        sfps_drain(r, o->prime, &o->prime_ps);
        sfps_drain(r, st->live, &st->lps);
        sfps_drain(r, st->sub, &st->ps);
        sfps_drain(r, st->of, &st->ops);
    }
    if (open) {
        SF_CHECK(r, moq_session_close_subgroup(o->pub->peer, sgh, r->now) ==
                        MOQ_OK);
    }
    for (int i = 0; i < 400 && r->failures == 0 &&
                    (st->lps.objects < (int)st->published ||
                     st->ps.fill_terminals == 0 ||
                     st->ops.fill_terminals == 0);
         i++) {
        sfrig_cycle(r, 1000);
        sfps_drain(r, o->pub, &o->pub_ps);
        sfps_drain(r, o->prime, &o->prime_ps);
        sfps_drain(r, st->live, &st->lps);
        sfps_drain(r, st->sub, &st->ps);
        sfps_drain(r, st->of, &st->ops);
    }
}

/* Both live subscribers received every live Object exactly once, in order:
 * the live subscriber all of them, the fill subscriber exactly those after
 * the Largest its subscription started from. */
static void
sf_expect_live_exact(sfrig_t *r, const sf_stream_t *st)
{
    SF_CHECK(r, st->lps.objects == (int)st->published);
    for (int i = 0; i < st->lps.objects && i < SF_LIVE_MAX; i++) {
        SF_CHECK(r, st->lps.live_obj[i][0] == 1 &&
                        st->lps.live_obj[i][1] == (uint64_t)i);
    }
    uint64_t first = st->ps.largest_group == 1 ? st->ps.largest_object + 1u
                                               : 0u;
    SF_CHECK(r, st->ps.objects == (int)(st->published - first));
    for (int i = 0; i < st->ps.objects && i < SF_LIVE_MAX; i++) {
        SF_CHECK(r, st->ps.live_obj[i][0] == 1 &&
                        st->ps.live_obj[i][1] == first + (uint64_t)i);
    }
    for (uint16_t i = 0; i < 2; i++) {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(moqr_shards_bind(r->s, i), &bs);
        SF_CHECK(r, bs.session_errors == 0);
    }
}

static bool
sf_stream_setup(sfrig_t *r, sf_origin_t *o, sf_stream_t *st)
{
    memset(st, 0, sizeof(*st));
    sf_origin_setup(r, o);
    st->live = sfrig_connect(r, 1);
    st->sub = sfrig_connect(r, 1);
    st->of = sfrig_connect(r, 0);
    SF_CHECK(r, st->live != NULL && st->sub != NULL && st->of != NULL);
    if (st->live == NULL || st->sub == NULL || st->of == NULL) {
        return false;
    }
    sfrig_pump(r, 4);
    sf_subscribe_live(r, st->live);
    sfrig_pump(r, 10);
    sfps_drain(r, st->live, &st->lps);
    SF_CHECK(r, st->lps.subscribe_ok && st->lps.largest_object == 2);
    return true;
}

/* The fill on both shards ended by its deadline, the owner's part served at
 * once on the other shard too. */
static void
sf_expect_stream_timing(sfrig_t *r, const sf_stream_t *st, uint64_t wait_ms)
{
    const sfps_t *both[2] = { &st->ps, &st->ops };
    for (int i = 0; i < 2; i++) {
        SF_CHECK(r, both[i]->first_fill_obj_at != 0 &&
                        both[i]->first_fill_obj_at < st->t0 + 20000u);
        SF_CHECK(r, both[i]->fill_term_at >= st->t0 + wait_ms * 1000u &&
                        both[i]->fill_term_at <=
                            st->t0 + wait_ms * 1000u + 10000u);
    }
}

/* Each directed channel holds one message and a live stream that started
 * before the fill keeps the owner's channel toward the other shard busy.
 * Retrieval answers go ahead of live data, bounded by the credit window, so
 * the fill on the other shard answers what the owner shard answers -- {0,0},
 * {0,1} Timed-Out at the deadline, {0,2}, then group 1 up to the Largest the
 * subscription started from (the owner's part first, then what the other
 * shard's own feed brought). That shard's feed lags the owner, so its
 * Largest {1,k} is earlier; up to it the two streams are identical. Both
 * live subscribers still receive every live Object exactly once, in
 * order. */
static int
cross_shard_live_busy_channel(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, false, 1, 0) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_stream_t st;
    if (!sf_stream_setup(&r, &o, &st)) {
        return sf_finish(&r, &a, label);
    }
    const uint64_t wait_ms = 40;
    sf_stream_run(&r, &o, &st, 0, 6 + wait_ms + 20, 6, wait_ms);
    const sfps_t *ps = &st.ps;
    SF_CHECK(&r, ps->subscribe_ok && ps->fill_created == 1 &&
                     ps->has_largest && ps->largest_group == 1);
    SF_CHECK(&r, st.ops.largest_group == 1 &&
                     st.ops.largest_object >= ps->largest_object);
    const sfps_t *fills[2] = { ps, &st.ops };
    const char *who[2] = { label, "owner shard" };
    for (int f = 0; f < 2; f++) {
        uint64_t k = fills[f]->largest_object;
        SF_CHECK(&r, k + 6 <= 16);
        sf_item_t want[16];
        int n = 0;
        want[n++] = (sf_item_t){ SF_IT_OBJ, 0, 0, 0 };
        want[n++] = (sf_item_t){ SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT };
        want[n++] = (sf_item_t){ SF_IT_OBJ, 0, 2, 0 };
        /* group 0's tail: no END_OF_GROUP evidence, reached after the
         * wait at {0,1} spent the budget */
        want[n++] = (sf_item_t){ SF_IT_GAP, 0, UINT64_MAX,
                                 MOQ_FETCH_RANGE_TIMED_OUT };
        for (uint64_t i = 0; i <= k && n < 15; i++) {
            want[n++] = (sf_item_t){ SF_IT_OBJ, 1, i, 0 };
        }
        want[n++] = (sf_item_t){ SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE };
        sf_expect_items(&r, fills[f], want, n, who[f]);
    }
    /* identical up to the other shard's Largest, bodies included */
    for (int i = 0; i + 1 < ps->n_items && i < 16; i++) {
        SF_CHECK(&r, ps->items[i].kind == st.ops.items[i].kind &&
                         ps->items[i].group == st.ops.items[i].group &&
                         ps->items[i].object == st.ops.items[i].object &&
                         ps->items[i].detail == st.ops.items[i].detail &&
                         ps->items[i].len == st.ops.items[i].len &&
                         memcmp(ps->items[i].body, st.ops.items[i].body,
                                ps->items[i].len) == 0);
    }
    sf_expect_stream_timing(&r, &st, wait_ms);
    sf_expect_live_exact(&r, &st);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    return sf_finish(&r, &a, label);
}

/* The same one-message channels, with the live stream starting while the
 * fill waits: the owner has answered {0,0} and holds at the run {0,1} it
 * does not hold, waiting out the deadline as a fill on the owner shard does.
 * At the deadline the other shard has the owner look again, and the rest of
 * the answer -- {0,1} Timed-Out, {0,2}, the end -- crosses a channel the
 * live stream keeps busy, ahead of live data, so both shards end the fill by
 * the deadline with the same items. Live delivery is exact. */
static int
cross_shard_hole_under_live_stream(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, false, 1, 0) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_stream_t st;
    if (!sf_stream_setup(&r, &o, &st)) {
        return sf_finish(&r, &a, label);
    }
    const uint64_t wait_ms = 40;
    sf_stream_run(&r, &o, &st, 10, wait_ms + 40, 0, wait_ms);
    SF_CHECK(&r, st.ps.subscribe_ok && st.ps.has_largest &&
                     st.ps.largest_group == 0 && st.ps.largest_object == 2);
    static const sf_item_t want[] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&r, &st.ps, want, 4, label);
    sf_expect_items(&r, &st.ops, want, 4, "owner shard");
    sf_expect_same_items(&r, &st.ps, &st.ops);
    sf_expect_stream_timing(&r, &st, wait_ms);
    sf_expect_live_exact(&r, &st);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    return sf_finish(&r, &a, label);
}

/* A range of ten Locations with a hole at {0,5}: more items than one credit
 * window carries, so the owner's answer crosses in several windows, each
 * granted once the fill has consumed the one before. Both shards answer
 * {0,0}..{0,4}, {0,5} Timed-Out at the deadline, {0,6}..{0,9}, FIN, item for
 * item, and every window's charge is back on its channel at the end. */
static int
cross_shard_fill_window(bool earned, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create(&r, &a, earned) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    static const uint64_t ids[9] = { 0, 1, 2, 3, 4, 6, 7, 8, 9 };
    sf_origin_t o;
    sf_origin_setup_ids(&r, &o, ids, 9);
    sfconn_t *of = sfrig_connect(&r, 0);
    sfconn_t *rf = sfrig_connect(&r, 1);
    SF_CHECK(&r, of != NULL && rf != NULL);
    if (of == NULL || rf == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    const uint64_t wait_us = 100000u;
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, of, true, wait_us / 1000u);
    sf_subscribe_fill(&r, rf, true, wait_us / 1000u);
    sfps_t ops, rps;
    memset(&ops, 0, sizeof(ops));
    memset(&rps, 0, sizeof(rps));
    uint64_t until = r.now + wait_us + 200000u;
    while ((ops.fill_terminals == 0 || rps.fill_terminals == 0) &&
           r.now < until && r.failures == 0) {
        sfrig_cycle(&r, 1000);
        sfps_drain(&r, of, &ops);
        sfps_drain(&r, rf, &rps);
    }
    sf_item_t want[12];
    int n = 0;
    for (uint64_t i = 0; i < 5; i++) {
        want[n++] = (sf_item_t){ SF_IT_OBJ, 0, i, 0 };
    }
    want[n++] = (sf_item_t){ SF_IT_GAP, 0, 5, MOQ_FETCH_RANGE_TIMED_OUT };
    for (uint64_t i = 6; i < 10; i++) {
        want[n++] = (sf_item_t){ SF_IT_OBJ, 0, i, 0 };
    }
    want[n++] = (sf_item_t){ SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE };
    sf_expect_items(&r, &ops, want, n, "owner shard");
    sf_expect_items(&r, &rps, want, n, label);
    sf_expect_same_items(&r, &ops, &rps);
    SF_CHECK(&r, rps.first_fill_obj_at != 0 &&
                     rps.first_fill_obj_at < t0 + 20000u);
    SF_CHECK(&r, rps.fill_term_at >= t0 + wait_us &&
                     rps.fill_term_at <= t0 + wait_us + 10000u);
    SF_CHECK(&r, sf_ingested(&r, 1) == 0);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sfrig_pump(&r, 2);
    sf_expect_channels_idle(&r);
    return sf_finish(&r, &a, label);
}

/* Each shard's core has one fetch cursor, so the shard serving two fills of
 * a remote-owned track can retrieve for only one at a time. The second fill
 * waits, as a fill whose range is not held yet waits; when the first fill's
 * retrieval ends, its slot frees and the waiting fill is brought back to
 * claim it at once -- in the wake-driven runtime nothing else would run that
 * shard before the deadline -- and both fills are answered {0,0}, {0,1},
 * {0,2}, FIN well before their deadline. */
static int
cross_shard_fill_slot_wait(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, true, 0, 1) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    static const uint64_t ids[3] = { 0, 1, 2 };
    sf_origin_t o;
    sf_origin_setup_ids(&r, &o, ids, 3);
    sfconn_t *f1 = sfrig_connect(&r, 1);
    sfconn_t *f2 = sfrig_connect(&r, 1);
    SF_CHECK(&r, f1 != NULL && f2 != NULL);
    if (f1 == NULL || f2 == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    const uint64_t wait_us = 300000u;
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, f1, true, wait_us / 1000u);
    sf_subscribe_fill(&r, f2, true, wait_us / 1000u);
    sfps_t p1, p2;
    memset(&p1, 0, sizeof(p1));
    memset(&p2, 0, sizeof(p2));
    uint64_t until = r.now + wait_us + 200000u;
    while ((p1.fill_terminals == 0 || p2.fill_terminals == 0) &&
           r.now < until && r.failures == 0) {
        sfrig_cycle(&r, 1000);
        sfps_drain(&r, f1, &p1);
        sfps_drain(&r, f2, &p2);
    }
    static const sf_item_t want[] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_OBJ, 0, 1, 0 },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&r, &p1, want, 4, label);
    sf_expect_items(&r, &p2, want, 4, label);
    SF_CHECK(&r, p1.fill_term_at < t0 + 30000u);
    SF_CHECK(&r, p2.fill_term_at < t0 + 30000u);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sfrig_pump(&r, 2);
    sf_expect_channels_idle(&r);
    return sf_finish(&r, &a, label);
}

/* A fill source that claims nothing. */
static int
sf_noop_close_called;
static moqr_result_t
sf_noop_open(void *ctx, const moqr_bind_fill_query_t *q,
             moqr_bind_fill_claim_t *c)
{
    (void)ctx;
    (void)q;
    (void)c;
    return MOQR_DONE;
}
static moqr_result_t
sf_noop_peek(void *ctx, uint64_t h, uint64_t now, moqr_fetch_item_t *out)
{
    (void)ctx;
    (void)h;
    (void)now;
    (void)out;
    return MOQR_ERR_STALE_HANDLE;
}
static moqr_result_t
sf_noop_commit(void *ctx, uint64_t h)
{
    (void)ctx;
    (void)h;
    return MOQR_ERR_STALE_HANDLE;
}
static void
sf_noop_close(void *ctx, uint64_t h)
{
    (void)ctx;
    (void)h;
    sf_noop_close_called++;
}

/* The fill-source setter's refusals, checked while the binding of shard 1
 * has a fill reading through the runtime's source: a NULL binding, a table
 * too small for its four entries, or a missing entry is INVAL; replacing or
 * removing the source while a fill reads through it is WRONG_STATE. None of
 * them changes the installed source (the fill goes on being served). */
static void
sf_expect_fill_source_refusals(sfrig_t *r)
{
    moqr_bind_t *b = moqr_shards_bind(r->s, 1);
    moqr_bind_fill_source_t src;
    memset(&src, 0, sizeof(src));
    src.struct_size = (uint32_t)sizeof(src);
    src.open = sf_noop_open;
    src.peek = sf_noop_peek;
    src.commit = sf_noop_commit;
    src.close = sf_noop_close;
    SF_CHECK(r, moqr_bind_set_fill_source(NULL, &src) == MOQR_ERR_INVAL);
    moqr_bind_fill_source_t small = src;
    small.struct_size = (uint32_t)offsetof(moqr_bind_fill_source_t, close);
    SF_CHECK(r, moqr_bind_set_fill_source(b, &small) == MOQR_ERR_INVAL);
    moqr_bind_fill_source_t missing = src;
    missing.commit = NULL;
    SF_CHECK(r, moqr_bind_set_fill_source(b, &missing) == MOQR_ERR_INVAL);
    SF_CHECK(r, moqr_bind_set_fill_source(b, &src) ==
                    MOQR_ERR_WRONG_STATE);
    SF_CHECK(r, moqr_bind_set_fill_source(b, NULL) == MOQR_ERR_WRONG_STATE);
    SF_CHECK(r, sf_noop_close_called == 0);
}

/* Each shard's core has one fetch cursor. A fill on the shard without the
 * track claims the only retrieval slot there and the owner's only cursor,
 * then its subscriber's connection goes away while the fill waits out a
 * long timeout. Its claim ends with the connection: the slot frees and the
 * owner is told to drop its cursor, so a second subscriber's fill right
 * after is answered at once -- {0,0}, {0,1} Timed-Out at its own deadline,
 * {0,2}, FIN -- rather than waiting for the first fill's deadline. */
static int
cross_shard_fill_conn_gone(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, false, 0, 1) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    sf_origin_setup(&r, &o);
    sfconn_t *f1 = sfrig_connect(&r, 1);
    sfconn_t *f2 = sfrig_connect(&r, 1);
    SF_CHECK(&r, f1 != NULL && f2 != NULL);
    if (f1 == NULL || f2 == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);
    sf_subscribe_fill(&r, f1, true, SF_MAX_WAIT_MS);
    sfps_t p1, p2;
    memset(&p1, 0, sizeof(p1));
    memset(&p2, 0, sizeof(p2));
    sf_run_to(&r, f1, &p1, NULL, NULL, r.now + 20000u);
    SF_CHECK(&r, p1.fill_objects == 1 && p1.fill_terminals == 0);
    sf_expect_fill_source_refusals(&r);
    SF_CHECK(&r, moq_session_close(f1->peer, 0, "gone", r.now) == MOQ_OK);
    f1->closed = true;
    sf_run_to(&r, f2, &p2, NULL, NULL, r.now + 20000u);
    SF_CHECK(&r, !moqr_bind_conn_is_open(moqr_shards_bind(r.s, 1),
                                         f1->rsess));
    const uint64_t wait_us = 300000u;
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, f2, true, wait_us / 1000u);
    sf_run_until_terminal(&r, f2, &p2, wait_us + 200000u);
    static const sf_item_t want[] = {
        { SF_IT_OBJ, 0, 0, 0 },
        { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_TIMED_OUT },
        { SF_IT_OBJ, 0, 2, 0 },
        { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(&r, &p2, want, 4, label);
    SF_CHECK(&r, p2.first_fill_obj_at != 0 &&
                     p2.first_fill_obj_at < t0 + 20000u);
    SF_CHECK(&r, p2.fill_term_at >= t0 + wait_us &&
                     p2.fill_term_at <= t0 + wait_us + 10000u);
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    sfrig_pump(&r, 2);
    sf_expect_channels_idle(&r);
    return sf_finish(&r, &a, label);
}

/* What a late-range fill must show, Objects by (group, object) in order. */
typedef struct sf_late_expect {
    int      objects;
    uint64_t obj[3][2];
    bool     timed_out;          /* one End of Timed-Out Range ...        */
    uint64_t to_group, to_object;/* ... at this Location                  */
    bool     at_deadline;        /* ended at the deadline, else before it */
} sf_late_expect_t;

/* The range reaches the relay only after the fill request: the fill
 * subscriber is the track's first subscriber, the origin's acceptance
 * reports Largest {0,2} before any Object of it has arrived, and the origin
 * then sends `ids` of group 0. The fill waits for them, bounded by its
 * deadline: what arrives in time is served, a hole the later Objects
 * evidence is one End of Timed-Out Range at the deadline, and the stream
 * ends with FIN -- the same answer the shard would give had it held them at
 * the request.
 *
 * `fill_shard` 0 puts the subscriber beside the publisher. 1 puts it on the
 * other shard, whose feed of the track starts after Largest {0,2}: the range
 * reaches it only through the retrieval from the owner, which waits for the
 * Objects exactly as a fill on the owner shard would, so the answer is the
 * same. */
static int
late_range_fill_ex(uint16_t fill_shard, const uint64_t *ids, size_t n,
                   const uint64_t *later_ids, size_t later_n,
                   const sf_late_expect_t *x, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create(&r, &a, false) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_origin_t o;
    memset(&o, 0, sizeof(o));
    sf_announce(&r, &o);
    o.pub_ps.accept_with_largest = true;
    o.pub_ps.accept_lg = 0;
    o.pub_ps.accept_lo = 2;
    sfconn_t *sub = sfrig_connect(&r, fill_shard);
    SF_CHECK(&r, o.pub != NULL && sub != NULL);
    if (o.pub == NULL || sub == NULL) {
        return sf_finish(&r, &a, label);
    }
    sfrig_pump(&r, 4);

    const uint64_t wait_us = 300000u;
    uint64_t t0 = r.now;
    sf_subscribe_fill(&r, sub, true, wait_us / 1000u);
    sfps_t ps;
    memset(&ps, 0, sizeof(ps));
    sf_run_to(&r, sub, &ps, o.pub, &o.pub_ps, t0 + 20000u);
    SF_CHECK(&r, o.pub_ps.up_seen);
    SF_CHECK(&r, ps.subscribe_ok && ps.has_largest &&
                     ps.largest_group == 0 && ps.largest_object == 2);
    SF_CHECK(&r, ps.fill_created == 1 && ps.fill_terminals == 0 &&
                     ps.fill_objects == 0 && ps.fill_gaps == 0);

    sf_pub_ids(&r, o.pub, o.pub_ps.up_sub, 0, 0, ids, n);
    if (later_n > 0) {
        /* the hole's Object arrives later, on another subgroup, well
         * before the deadline */
        sf_run_to(&r, sub, &ps, o.pub, &o.pub_ps, r.now + 50000u);
        sf_pub_ids(&r, o.pub, o.pub_ps.up_sub, 0, 1, later_ids, later_n);
    }
    sf_run_until_terminal(&r, sub, &ps, wait_us + 200000u);
    if (ps.fill_terminals != 1) {
        printf("  [%s] fill_objects=%d gaps=%d terminals=%d after %llu us\n",
               label, ps.fill_objects, ps.fill_gaps, ps.fill_terminals,
               (unsigned long long)(r.now - t0));
    }
    SF_CHECK(&r, ps.fill_terminals == 1);
    SF_CHECK(&r, ps.fill_term_reason == MOQ_FILL_COMPLETE);
    if (x->at_deadline) {
        SF_CHECK(&r, ps.fill_term_at >= t0 + wait_us &&
                         ps.fill_term_at <= t0 + wait_us + 10000u);
    } else {
        SF_CHECK(&r, ps.fill_term_at < t0 + wait_us);
    }
    SF_CHECK(&r, ps.fill_objects == x->objects);
    for (int i = 0; i < x->objects && i < ps.fill_objects; i++) {
        SF_CHECK(&r, ps.fill_obj[i][0] == x->obj[i][0] &&
                         ps.fill_obj[i][1] == x->obj[i][1]);
    }
    if (x->timed_out) {
        SF_CHECK(&r, ps.fill_gaps == 1 &&
                         ps.fill_gap[0].kind == MOQ_FETCH_RANGE_TIMED_OUT &&
                         ps.fill_gap[0].group == x->to_group &&
                         ps.fill_gap[0].object == x->to_object);
    } else {
        SF_CHECK(&r, ps.fill_gaps == 0);
    }
    SF_CHECK(&r, sfrig_next_deadline(&r) == UINT64_MAX);
    if (fill_shard == 1) {
        SF_CHECK(&r, sf_ingested(&r, 1) == 0);
    }
    sf_expect_subscription_lives(&r, &o, sub, &ps, 1);
    return sf_finish(&r, &a, label);
}

static int
late_range_fill(uint16_t fill_shard, const uint64_t *ids, size_t n,
                const sf_late_expect_t *x, const char *label)
{
    return late_range_fill_ex(fill_shard, ids, n, NULL, 0, x, label);
}

/* Retained-only answers use real upstream FETCH requests. The origin keeps one
 * payload before fill demand, never publishing that payload onto the live track. */
typedef struct sf_retained_origin {
    sfconn_t *cn;
    moq_announcement_t ann;
    moq_subscription_t sub;
    moq_fetch_t fetch;
    moq_rcbuf_t *payload;
    uint64_t largest, missing, fetch_end;
    unsigned namespaces, subscribes, requests, cancelled;
} sf_retained_origin_t;

static bool
sf_retained_key(moq_namespace_t ns, moq_bytes_t name)
{
    return ns.count == 2 && ns.parts != NULL &&
           ns.parts[0].len == 4 && ns.parts[0].data != NULL &&
           memcmp(ns.parts[0].data, "live", 4) == 0 &&
           ns.parts[1].len == 3 && ns.parts[1].data != NULL &&
           memcmp(ns.parts[1].data, "cam", 3) == 0 &&
           name.len == 1 && name.data != NULL && name.data[0] == 'v';
}

static void
sf_retained_drain(sfrig_t *r, sf_retained_origin_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(o->cn->peer, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            SF_CHECK(r, ev.u.namespace_accepted.ann._opaque == o->ann._opaque);
            o->namespaces++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            const moq_subscribe_request_event_t *q = &ev.u.subscribe_request;
            SF_CHECK(r, sf_retained_key(q->track_namespace, q->track_name));
            SF_CHECK(r, q->options != NULL &&
                        (q->options->flags & MOQ_SUBSCRIPTION_ADMISSION_LOCATION) != 0 &&
                        q->options->location.kind == MOQ_LOCATION_FILTER_NEXT_OBJECT &&
                        (q->options->flags & MOQ_SUBSCRIPTION_ADMISSION_FILL) == 0);
            o->sub = q->sub;
            o->subscribes++;
            moq_accept_subscribe_cfg_t cfg;
            moq_accept_subscribe_cfg_init(&cfg);
            cfg.has_largest = o->payload != NULL;
            cfg.largest_group = 0;
            cfg.largest_object = o->largest;
            SF_CHECK(r, moq_session_accept_subscribe(o->cn->peer, o->sub,
                                                     &cfg, r->now) == MOQ_OK);
            break;
        }
        case MOQ_EVENT_FETCH_REQUEST: {
            const moq_fetch_request_event_t *q = &ev.u.fetch_request;
            SF_CHECK(r, o->subscribes == 1 && o->sub._opaque != 0);
            SF_CHECK(r, sf_retained_key(q->track_namespace, q->track_name));
            SF_CHECK(r, q->fetch._opaque != 0 && q->joining_sub._opaque == 0 &&
                        q->joining_pub._opaque == 0);
            SF_CHECK(r, q->start_group == 0 && q->start_object == o->missing &&
                        q->end_group == 0 && q->end_object == o->fetch_end);
            SF_CHECK(r, q->group_order == MOQ_GROUP_ORDER_ASCENDING);
            SF_CHECK(r, q->subscriber_priority == 128);
            o->requests++;
            SF_CHECK(r, o->requests == 1);
            if (o->requests == 1) o->fetch = q->fetch;
            break;
        }
        case MOQ_EVENT_FETCH_CANCELLED:
            SF_CHECK(r, o->requests == 1 &&
                        ev.u.fetch_cancelled.fetch._opaque == o->fetch._opaque);
            o->cancelled++;
            break;
        case MOQ_EVENT_FILL_REQUEST:
        case MOQ_EVENT_NAMESPACE_REJECTED:
        case MOQ_EVENT_SESSION_CLOSED:
            SF_CHECK(r, false);
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static void
sf_retained_keep(sfrig_t *r, sf_retained_origin_t *o, uint8_t tag)
{
    const uint8_t body[4] = { 0, (uint8_t)o->missing, tag, (uint8_t)(tag ^ 0xffu) };
    SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body),
                                 &o->payload) == MOQ_OK);
}

static void
sf_retained_announce(sfrig_t *r, sf_retained_origin_t *o,
                     bool partial, uint8_t tag)
{
    memset(o, 0, sizeof(*o));
    o->largest = partial ? 2 : 0;
    o->missing = partial ? 1 : 0;
    o->fetch_end = o->missing + 1;
    /* The empty-cache origin retains before any demand. The partial-cache
     * origin first publishes 0/0 and 0/2 live, then retains 0/1 before fill. */
    if (!partial) sf_retained_keep(r, o, tag);
    o->cn = sfrig_connect(r, 0);
    SF_CHECK(r, o->cn != NULL);
    if (o->cn == NULL) return;
    sfrig_pump(r, 4);
    moq_bytes_t parts[] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t cfg;
    moq_publish_namespace_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    SF_CHECK(r, moq_session_publish_namespace(o->cn->peer, &cfg, r->now,
                                               &o->ann) == MOQ_OK);
    sfrig_pump(r, 8);
    sf_retained_drain(r, o);
    SF_CHECK(r, o->namespaces == 1 && o->subscribes == 0 && o->requests == 0);
}

/* The prime subscribes Absolute Start without a fill; the remote subscriber
 * requests Next Object plus {0,0}..Largest. Both use the supported ascending
 * order. The returned real handle is retained through teardown. */
static moq_subscription_t
sf_retained_subscribe(sfrig_t *r, sfconn_t *cn, bool with_fill)
{
    moq_bytes_t parts[] = { B("live"), B("cam") };
    moq_location_filter_cfg_t loc, start;
    moq_location_filter_cfg_init(&loc);
    loc.kind = with_fill ? MOQ_LOCATION_FILTER_NEXT_OBJECT
                         : MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_location_filter_cfg_init(&start);
    start.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &start;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = 100;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
    cfg.track_name = B("v");
    cfg.location = &loc;
    cfg.group_order = MOQ_GROUP_ORDER_ASCENDING;
    cfg.fill = with_fill ? &fill : NULL;
    moq_subscription_t sub = {0};
    SF_CHECK(r, moq_session_subscribe(cn->peer, &cfg, r->now, &sub) == MOQ_OK);
    return sub;
}

static void
sf_retained_step(sfrig_t *r, sf_retained_origin_t *o,
                 sfconn_t *prime, sfps_t *pps, sfconn_t *sub, sfps_t *ps)
{
    sfrig_cycle(r, 1000);
    sf_retained_drain(r, o);
    if (prime != NULL) sfps_drain(r, prime, pps);
    if (sub != NULL) sfps_drain(r, sub, ps);
}

static void
sf_retained_deadline(sfrig_t *r, uint64_t t0, uint64_t *saved)
{
    uint64_t d = sfrig_next_deadline(r);
    if (d == UINT64_MAX) return;
    if (*saved == UINT64_MAX) *saved = d;
    SF_CHECK(r, d == *saved);
    SF_CHECK(r, d >= t0 + 100000u && d <= t0 + 105000u);
}

static bool
sf_retained_requested(sfrig_t *r, const sf_retained_origin_t *o,
                      const char *label)
{
    if (o->requests != 1) {
        printf("FAIL: %s origin_requests expected=1 actual=%u\n", label, o->requests);
        r->failures++;
        return false;
    }
    return r->failures == 0;
}

static void
sf_retained_accept(sfrig_t *r, sf_retained_origin_t *o)
{
    moq_accept_fetch_cfg_t cfg;
    moq_accept_fetch_cfg_init(&cfg);
    cfg.end_group = 0;
    cfg.end_object = o->fetch_end;
    SF_CHECK(r, moq_session_accept_fetch(o->cn->peer, o->fetch, &cfg,
                                         r->now) == MOQ_OK);
}

static void
sf_retained_write(sfrig_t *r, sf_retained_origin_t *o, bool fin)
{
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    cfg.group_id = 0;
    cfg.object_id = o->missing;
    cfg.payload = o->payload;
    SF_CHECK(r, moq_session_write_fetch_object(o->cn->peer, o->fetch,
                                                &cfg, r->now) == MOQ_OK);
    if (fin) {
        SF_CHECK(r, moq_session_end_fetch(o->cn->peer, o->fetch, r->now) == MOQ_OK);
    }
}

static void
sf_retained_exact(sfrig_t *r, const sfps_t *ps, bool partial, uint8_t tag,
                   const char *label)
{
    static const sf_item_t empty[] = {
        { SF_IT_OBJ, 0, 0, 0 }, { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    static const sf_item_t islands[] = {
        { SF_IT_OBJ, 0, 0, 0 }, { SF_IT_OBJ, 0, 1, 0 },
        { SF_IT_OBJ, 0, 2, 0 }, { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
    };
    sf_expect_items(r, ps, partial ? islands : empty, partial ? 4 : 2, label);
    SF_CHECK(r, ps->subscribe_ok && ps->fill_created == 1 &&
                ps->fill_terminals == 1 && ps->fill_gaps == 0);
    for (int i = 0; i < (partial ? 3 : 1); i++) {
        uint8_t want_tag = partial && i != 1 ? 0xC3 : tag;
        const uint8_t want[4] = { 0, (uint8_t)i, want_tag,
                                 (uint8_t)(want_tag ^ 0xffu) };
        SF_CHECK(r, ps->items[i].len == sizeof(want) &&
                    memcmp(ps->items[i].body, want, sizeof(want)) == 0);
    }
    SF_CHECK(r, ps->fill_term_domain == MOQ_FILL_ERROR_NONE &&
                !ps->done_seen && !ps->session_closed);
}

/* Final round stepping is only the existing linger/UNDEMAND teardown, not a
 * substitute for the earned-wake and original-deadline observations above. */
static void
sf_retained_quiescent(sfrig_t *r)
{
    r->earned = false;
    sfrig_pump(r, 12);
    sf_expect_channels_idle(r);
    /* No fill deadline is left: whatever deadline remains is the cores' own
     * retention of the now idle track. */
    SF_CHECK(r, sfrig_next_deadline(r) == sfrig_core_deadline(r));
    for (uint16_t sh = 0; sh < 2; sh++) {
        moqr_shards_stats_t st = {0};
        SF_CHECK(r, moqr_shards_get_stats(r->s, sh, &st) == MOQR_OK);
        SF_CHECK(r, st.pending_demands == 0 && st.pump_subs_active == 0 &&
                    st.pump_subs_parked == 0 && st.owner_progress_slots == 0 &&
                    st.requester_open_objects == 0);
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(moqr_shards_bind(r->s, sh), &bs);
        if (bs.session_errors != 0 || bs.ingest_refusals != 0) {
            printf("FIXTURE: sf_retained_quiescent shard=%u "
                   "session_errors=%llu ingest_refusals=%llu\n",
                   (unsigned)sh, (unsigned long long)bs.session_errors,
                   (unsigned long long)bs.ingest_refusals);
        }
        SF_CHECK(r, bs.session_errors == 0 && bs.ingest_refusals == 0);
    }
}

/* Distinguish no cache reader from a frozen cache hole. In both cases only a
 * decoded exact-span FETCH unlocks the origin payload; time alone cannot
 * turn the old cache-only path into a passing test. */
static int
cross_shard_retained_origin(bool partial, const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, true, 1, 1) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_retained_origin_t o;
    sf_retained_announce(&r, &o, partial, 0xA1);
    sfconn_t *prime = sfrig_connect(&r, 0);
    sfconn_t *sub = sfrig_connect(&r, 1);
    SF_CHECK(&r, prime != NULL && sub != NULL);
    if (r.failures != 0) goto done;
    sfrig_pump(&r, 4);
    sfps_t pps = {0}, ps = {0};
    moq_subscription_t ph = sf_retained_subscribe(&r, prime, false);
    for (int i = 0; i < 12; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    }
    SF_CHECK(&r, pps.subscribe_ok && o.subscribes == 1 && o.requests == 0);
    if (r.failures != 0) goto done;
    if (partial) {
        static const uint64_t ids[] = { 0, 2 };
        sf_pub_ids(&r, o.cn, o.sub, 0, 0, ids, 2);
        for (int i = 0; i < 8; i++) {
            sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        }
        sf_retained_keep(&r, &o, 0xA1);
    }
    SF_CHECK(&r, pps.objects == (partial ? 2 : 0));
    SF_CHECK(&r, sf_ingested(&r, 0) == (partial ? 2u : 0u) &&
                  sf_ingested(&r, 1) == 0);
    const uint64_t t0 = r.now;
    uint64_t deadline = UINT64_MAX;
    moq_subscription_t sh = sf_retained_subscribe(&r, sub, true);
    for (int i = 0; i < 20; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        sf_retained_deadline(&r, t0, &deadline);
    }
    SF_CHECK(&r, ps.subscribe_ok && ps.has_largest &&
                  ps.largest_group == 0 && ps.largest_object == o.largest);
    SF_CHECK(&r, ps.fill_created == 1 && ps.fill_terminals == 0 &&
                  ps.fill_gaps == 0 && ps.fill_objects == (partial ? 1 : 0));
    SF_CHECK(&r, deadline != UINT64_MAX && sfrig_next_deadline(&r) == deadline);
    if (!sf_retained_requested(&r, &o, label)) goto done;
    sf_retained_accept(&r, &o);
    /* Header acceptance does not reset the deadline or prove trailing absence. */
    for (int i = 0; i < 10; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        SF_CHECK(&r, sfrig_next_deadline(&r) == deadline);
        SF_CHECK(&r, ps.fill_terminals == 0);
    }
    sf_retained_write(&r, &o, true);
    for (int i = 0; i < 16; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    }
    sf_retained_exact(&r, &ps, partial, 0xA1, label);
    SF_CHECK(&r, ps.first_fill_obj_at > t0 && ps.fill_term_at < deadline);
    SF_CHECK(&r, o.requests == 1 && o.cancelled == 0 && o.subscribes == 1);
    SF_CHECK(&r, ps.objects == 0 && pps.objects == (partial ? 2 : 0));
    SF_CHECK(&r, sf_ingested(&r, 0) == (partial ? 2u : 0u) &&
                  sf_ingested(&r, 1) == 0);
    static const uint64_t live[] = { 0 };
    sf_pub_ids(&r, o.cn, o.sub, 1, 0, live, 1);
    for (int i = 0; i < 10; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    }
    SF_CHECK(&r, ps.objects == 1 && ps.live_obj[0][0] == 1 &&
                  ps.live_obj[0][1] == 0 && pps.objects == (partial ? 3 : 1));
    SF_CHECK(&r, ps.fill_terminals == 1 && !ps.done_seen && !ps.session_closed);
    SF_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    SF_CHECK(&r, moq_session_unsubscribe(prime->peer, ph, r.now) == MOQ_OK);
    sf_retained_quiescent(&r);
    sf_retained_drain(&r, &o);
    sfps_drain(&r, prime, &pps);
    sfps_drain(&r, sub, &ps);
    SF_CHECK(&r, ps.fill_created == 1 && ps.fill_terminals == 1 &&
                  ps.fill_objects == (partial ? 3 : 1) && ps.fill_gaps == 0);
    SF_CHECK(&r, moq_session_state(o.cn->peer) == MOQ_SESS_ESTABLISHED &&
                  moq_session_state(prime->peer) == MOQ_SESS_ESTABLISHED &&
                  moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED);
    SF_CHECK(&r, o.requests == 1 && o.cancelled == 0);
done:
    moq_rcbuf_decref(o.payload);
    return sf_finish(&r, &a, label);
}

enum sf_typed_control {
    SF_TYPED_NON_EXISTENT,
    SF_TYPED_UNKNOWN,
    SF_TYPED_TIMED_OUT,
    SF_TYPED_RESPONSE_ERROR,
    SF_TYPED_RESET,
    SF_TYPED_PUSH_REFUSED,
    SF_TYPED_PROPERTIES_OOM,
    SF_TYPED_OVERSIZE,
    SF_TYPED_QUIESCED,
    SF_TYPED_MIXED_GAP,
    SF_TYPED_COUNT
};

/* The same public rig, with only the owner data phase separated from the
 * bind's event collection. This isolates a cross-thread clone failure from
 * session decoding and the already-qualified collector's own allocations. */
static int
cross_shard_retained_typed(enum sf_typed_control mode)
{
    static const char *const labels[] = {
        "retained_typed_non_existent", "retained_typed_unknown",
        "retained_typed_timed_out", "retained_typed_response_error",
        "retained_typed_reset", "retained_typed_push_refused",
        "retained_typed_properties_oom", "retained_typed_oversize",
        "retained_typed_quiesced", "retained_typed_nonterminal_ni_unknown",
    };
    static const uint8_t properties[] = { 0x20, 0x80, 0x40 };
    static const uint8_t error_reason[] = { 'd', 'e', 'n', 'i', 'e', 'd', 0, 'x' };
    const char *label = labels[mode];
    const bool twice = mode == SF_TYPED_PUSH_REFUSED || mode == SF_TYPED_QUIESCED;
    const bool mixed_gap = mode == SF_TYPED_MIXED_GAP;
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    g_sf_log_bytes = mode == SF_TYPED_OVERSIZE ? 128 : 0;
    moqr_result_t created = sfrig_create_ex(&r, &a, true, 1, 1);
    g_sf_log_bytes = 0;
    if (created != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_retained_origin_t o;
    sf_retained_announce(&r, &o, false, 0xA1);
    if (twice || mixed_gap) { o.largest = 1; o.fetch_end = 2; }
    moq_rcbuf_t *props = NULL, *second = NULL;
    sfconn_t *prime = sfrig_connect(&r, 0);
    sfconn_t *sub = sfrig_connect(&r, 1);
    SF_CHECK(&r, prime != NULL && sub != NULL);
    if (r.failures != 0) goto done;
    sfrig_pump(&r, 4);
    sfps_t pps = {0}, ps = {0};
    moq_subscription_t ph = sf_retained_subscribe(&r, prime, false);
    for (int i = 0; i < 12; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    SF_CHECK(&r, pps.subscribe_ok && o.subscribes == 1 && o.requests == 0);
    const uint64_t t0 = r.now;
    uint64_t deadline = UINT64_MAX;
    moq_subscription_t sh = sf_retained_subscribe(&r, sub, true);
    for (int i = 0; i < 20; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        sf_retained_deadline(&r, t0, &deadline);
    }
    SF_CHECK(&r, ps.subscribe_ok && ps.fill_created == 1 &&
                  ps.fill_objects == 0 && ps.fill_gaps == 0 && ps.fill_terminals == 0 &&
                  deadline != UINT64_MAX);
    if (mixed_gap) {
        SF_CHECK(&r, ps.has_largest && ps.largest_group == 0 && ps.largest_object == 1);
    }
    if (!sf_retained_requested(&r, &o, label)) goto done;
    if (mode == SF_TYPED_RESPONSE_ERROR) {
        uint8_t reason[sizeof(error_reason)];
        memcpy(reason, error_reason, sizeof(reason));
        moq_reject_fetch_cfg_t cfg;
        moq_reject_fetch_cfg_init(&cfg);
        cfg.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        cfg.reason = (moq_bytes_t){ reason, sizeof(reason) };
        SF_CHECK(&r, moq_session_reject_fetch(o.cn->peer, o.fetch, &cfg, r.now) == MOQ_OK);
        memset(reason, 0xCC, sizeof(reason));
    } else {
        sf_retained_accept(&r, &o);
        if (mixed_gap) {
            /* NI must split the known-absent prefix from the unknown suffix. */
            SF_CHECK(&r, moq_session_write_fetch_range(o.cn->peer, o.fetch,
                MOQ_FETCH_RANGE_NON_EXISTENT, 0, 0, r.now) == MOQ_OK);
            SF_CHECK(&r, moq_session_write_fetch_range(o.cn->peer, o.fetch,
                MOQ_FETCH_RANGE_UNKNOWN, 0, 1, r.now) == MOQ_OK);
            SF_CHECK(&r, moq_session_end_fetch(o.cn->peer, o.fetch, r.now) == MOQ_OK);
        } else if (mode <= SF_TYPED_TIMED_OUT) {
            moq_fetch_range_kind_t kind = mode == SF_TYPED_NON_EXISTENT
                ? MOQ_FETCH_RANGE_NON_EXISTENT : mode == SF_TYPED_UNKNOWN
                ? MOQ_FETCH_RANGE_UNKNOWN : MOQ_FETCH_RANGE_TIMED_OUT;
            SF_CHECK(&r, moq_session_write_fetch_range(o.cn->peer, o.fetch,
                                                       kind, 0, 0, r.now) == MOQ_OK);
            SF_CHECK(&r, moq_session_end_fetch(o.cn->peer, o.fetch, r.now) == MOQ_OK);
        } else if (mode == SF_TYPED_RESET) {
            SF_CHECK(&r, moq_session_abort_fetch(o.cn->peer, o.fetch, 0x2, r.now) == MOQ_OK);
        } else {
            /* Settle FETCH_OK metadata before isolating object pressure. */
            for (int i = 0; i < 4; i++) {
                sf_retained_step(&r, &o, prime, &pps, sub, &ps);
                SF_CHECK(&r, sfrig_next_deadline(&r) == deadline);
                SF_CHECK(&r, ps.fill_objects == 0 && ps.fill_gaps == 0 &&
                              ps.fill_terminals == 0);
            }
            sf_expect_channels_idle(&r);
            if (r.failures != 0) goto done;
            SF_CHECK(&r, moq_rcbuf_create(&a.vt, properties, sizeof(properties), &props) == MOQ_OK);
            if (mode == SF_TYPED_OVERSIZE) {
                uint8_t body[129];
                memset(body, 0xE7, sizeof(body));
                moq_rcbuf_decref(o.payload);
                o.payload = NULL;
                SF_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &o.payload) == MOQ_OK);
            }
            moq_fetch_object_cfg_t oc;
            moq_fetch_object_cfg_init(&oc);
            oc.subgroup_id = 7;
            oc.publisher_priority = 19;
            oc.payload = o.payload;
            oc.properties = props;
            SF_CHECK(&r, moq_session_write_fetch_object(o.cn->peer, o.fetch, &oc, r.now) == MOQ_OK);
            if (twice) {
                static const uint8_t body[] = { 0, 1, 0xB2, 0x4D };
                SF_CHECK(&r, moq_rcbuf_create(&a.vt, body, sizeof(body), &second) == MOQ_OK);
                oc.object_id = 1;
                oc.payload = second;
                SF_CHECK(&r, moq_session_write_fetch_object(o.cn->peer, o.fetch, &oc, r.now) == MOQ_OK);
            }
            SF_CHECK(&r, moq_session_end_fetch(o.cn->peer, o.fetch, r.now) == MOQ_OK);
        }
    }
    if (r.failures != 0) goto done;

    /* Transport decodes the answer while both shard data phases are frozen.
     * The owner's ordinary bind then collects it, without peeking the cursor. */
    r.frozen = 3;
    sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    SF_CHECK(&r, moqr_bind_pump(moqr_shards_bind(r.s, 0), r.now) == MOQR_OK);
    SF_CHECK(&r, moqr_bind_pump(moqr_shards_bind(r.s, 0), r.now) == MOQR_OK);
    sf_expect_channels_idle(&r);
    moqr_shards_pair_stats_t before = {0}, after = {0};
    SF_CHECK(&r, moqr_shards_get_pair_stats(r.s, 0, 1, &before, sizeof(before)) == MOQR_OK);
    if (mode == SF_TYPED_PROPERTIES_OOM) {
        a.expected[0] = (moq_bytes_t){ moq_rcbuf_data(o.payload), moq_rcbuf_len(o.payload) };
        a.expected[1] = (moq_bytes_t){ properties, sizeof(properties) };
        SF_CHECK(&r, moq_rcbuf_allocation_size(a.expected[0].len, &a.sizes[0]) == MOQ_OK);
        SF_CHECK(&r, moq_rcbuf_allocation_size(a.expected[1].len, &a.sizes[1]) == MOQ_OK);
        a.fail_size = a.sizes[1];
        a.watching = true;
    } else if (mode == SF_TYPED_RESPONSE_ERROR) {
        a.expected[0] = (moq_bytes_t){ error_reason, sizeof(error_reason) };
        SF_CHECK(&r, moq_rcbuf_allocation_size(sizeof(error_reason), &a.sizes[0]) == MOQ_OK);
        a.watching = true;
    }
    uint64_t wakes = 0;
    SF_CHECK(&r, moqr_shards_step_shard(r.s, 0, r.now, &wakes) == MOQR_OK);
    r.carried |= wakes;
    if (mode == SF_TYPED_PROPERTIES_OOM) {
        SF_CHECK(&r, a.fail_size == 0 && a.attempts[0] == 1 && a.attempts[1] == 1);
        SF_CHECK(&r, a.copies[0] != 0 && a.copies[1] == 0 &&
                      a.releases[0] == 1 && a.releases[1] == 0 && a.released_exact[0]);
        a.watching = false;
        a.fail_size = 0;
    }
    SF_CHECK(&r, moqr_shards_get_pair_stats(r.s, 0, 1, &after, sizeof(after)) == MOQR_OK);
    if (mode == SF_TYPED_RESPONSE_ERROR) {
        SF_CHECK(&r, a.attempts[0] == 1 && a.copies[0] != 0 && a.releases[0] == 0 &&
                      after.data_bytes == before.data_bytes + sizeof(error_reason));
    } else if (mode == SF_TYPED_PROPERTIES_OOM || mode == SF_TYPED_OVERSIZE) {
        SF_CHECK(&r, after.data_messages == before.data_messages + 1 &&
                      after.data_bytes == before.data_bytes);
    }
    if (twice) {
        /* One full FIFO owns object 0; the refused object 1 still belongs
         * to the cursor. Thaw must deliver each exactly once, not just FIN. */
        SF_CHECK(&r, after.data_messages == before.data_messages + 1 &&
                      after.data_bytes == before.data_bytes + 7 &&
                      after.refused_entries > before.refused_entries);
        SF_CHECK(&r, ps.fill_objects == 0 && ps.fill_terminals == 0);
        if (mode == SF_TYPED_QUIESCED) goto done;
    }
    r.frozen = 0;
    r.carried |= 3;
    for (int i = 0; i < 16; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    if (mode == SF_TYPED_RESPONSE_ERROR) {
        SF_CHECK(&r, a.releases[0] == 1 && a.released_exact[0]);
        a.watching = false;
    }
    SF_CHECK(&r, ps.fill_terminals == 1 && ps.fill_term_at < deadline &&
                  ps.subscribe_ok && !ps.done_seen && !ps.session_closed);
    if (mode == SF_TYPED_NON_EXISTENT) {
        /* FIN alone establishes this unfiltered terminal non-existent range. */
        static const sf_item_t want[] = {
            { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
        };
        sf_expect_items(&r, &ps, want, 1, label);
        SF_CHECK(&r, ps.fill_objects == 0 && ps.fill_gaps == 0 &&
                      ps.fill_term_domain == MOQ_FILL_ERROR_NONE);
    } else if (mixed_gap) {
        static const sf_item_t want[] = {
            { SF_IT_GAP, 0, 0, MOQ_FETCH_RANGE_NON_EXISTENT },
            { SF_IT_GAP, 0, 1, MOQ_FETCH_RANGE_UNKNOWN },
            { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
        };
        sf_expect_items(&r, &ps, want, 3, label);
        SF_CHECK(&r, ps.fill_objects == 0 && ps.fill_gaps == 2 &&
                      ps.fill_term_domain == MOQ_FILL_ERROR_NONE);
    } else if (mode <= SF_TYPED_TIMED_OUT) {
        uint32_t kind = mode == SF_TYPED_UNKNOWN
            ? MOQ_FETCH_RANGE_UNKNOWN : MOQ_FETCH_RANGE_TIMED_OUT;
        const sf_item_t want[] = {
            { SF_IT_GAP, 0, 0, kind }, { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
        };
        sf_expect_items(&r, &ps, want, 2, label);
        SF_CHECK(&r, ps.fill_objects == 0 && ps.fill_gaps == 1 &&
                      ps.fill_term_domain == MOQ_FILL_ERROR_NONE);
    } else if (mode == SF_TYPED_PUSH_REFUSED) {
        if (ps.n_items == 2 && ps.fill_objects == 1 && ps.fill_gaps == 0 &&
            ps.items[0].kind == SF_IT_OBJ && ps.items[0].group == 0 &&
            ps.items[0].object == 0 && ps.items[1].kind == SF_IT_TERM &&
            ps.items[1].detail == MOQ_FILL_COMPLETE) {
            printf("FAIL: retained_typed_push_refused object1 missing after refused push\n");
            r.failures++;
            goto done;
        }
        static const sf_item_t want[] = {
            { SF_IT_OBJ, 0, 0, 0 }, { SF_IT_OBJ, 0, 1, 0 },
            { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
        };
        sf_expect_items(&r, &ps, want, 3, label);
        SF_CHECK(&r, ps.fill_objects == 2 && ps.fill_gaps == 0 &&
                      ps.fill_term_domain == MOQ_FILL_ERROR_NONE);
        for (int i = 0; i < 2; i++) {
            uint8_t tag = i == 0 ? 0xA1 : 0xB2;
            const uint8_t want_body[] = { 0, (uint8_t)i, tag, (uint8_t)(tag ^ 0xffu) };
            SF_CHECK(&r, ps.items[i].len == sizeof(want_body) &&
                          memcmp(ps.items[i].body, want_body, sizeof(want_body)) == 0);
            SF_CHECK(&r, ps.items[i].subgroup == 7 && ps.items[i].priority == 19 &&
                          !ps.items[i].datagram && ps.items[i].properties_len == sizeof(properties) &&
                          memcmp(ps.items[i].properties, properties, sizeof(properties)) == 0);
        }
    } else {
        static const sf_item_t want[] = {
            { SF_IT_TERM, 0, 0, MOQ_FILL_PEER_CANCELLED_OR_RESET },
        };
        sf_expect_items(&r, &ps, want, 1, label);
        SF_CHECK(&r, ps.fill_objects == 0 && ps.fill_gaps == 0 &&
                      ps.fill_term_domain == MOQ_FILL_ERROR_STREAM);
    }
    SF_CHECK(&r, o.requests == 1 && o.cancelled == 0 && pps.objects == 0 &&
                  ps.objects == 0 && sf_ingested(&r, 0) == 0 && sf_ingested(&r, 1) == 0);
    /* A local refusal must leave the parent and both sessions usable. */
    static const uint64_t live[] = { 0 };
    sf_pub_ids(&r, o.cn, o.sub, 1, 0, live, 1);
    for (int i = 0; i < 10; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    SF_CHECK(&r, pps.objects == 1 && ps.objects == 1 &&
                  ps.live_obj[0][0] == 1 && ps.live_obj[0][1] == 0 && ps.fill_terminals == 1);
    SF_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    SF_CHECK(&r, moq_session_unsubscribe(prime->peer, ph, r.now) == MOQ_OK);
    sf_retained_quiescent(&r);
done:
    a.fail_size = 0;
    a.watching = false;
    moq_rcbuf_decref(props);
    moq_rcbuf_decref(second);
    moq_rcbuf_decref(o.payload);
    return sf_finish(&r, &a, label);
}

/* Four cached objects consume the entire credit window. With the requester
 * frozen, the owner still starts and expires the missing-span collector at
 * the original deadline. Cancellation also works before that deadline. */
static int
cross_shard_retained_zero_credit(bool cancel)
{
    const char *label = cancel ? "retained_zero_credit_cancel" : "retained_zero_credit_deadline";
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, true, 8, 1) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_retained_origin_t o;
    sf_retained_announce(&r, &o, false, 0xA1);
    o.largest = 6;
    o.missing = 4;
    o.fetch_end = 7;
    sfconn_t *prime = sfrig_connect(&r, 0);
    sfconn_t *sub = sfrig_connect(&r, 1);
    SF_CHECK(&r, prime != NULL && sub != NULL);
    if (r.failures != 0) goto done;
    sfrig_pump(&r, 4);
    sfps_t pps = {0}, ps = {0};
    moq_subscription_t ph = sf_retained_subscribe(&r, prime, false);
    for (int i = 0; i < 12; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    static const uint64_t ids[] = { 0, 1, 2, 3 };
    sf_pub_ids(&r, o.cn, o.sub, 0, 0, ids, 4);
    for (int i = 0; i < 8; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    SF_CHECK(&r, pps.objects == 4 && o.requests == 0);
    r.ack_base = sfrig_owner_acks(&r);
    r.freeze_owner_on_ack = true;
    const uint64_t t0 = r.now;
    uint64_t deadline = UINT64_MAX;
    moq_subscription_t sh = sf_retained_subscribe(&r, sub, true);
    for (int i = 0; i < 12; i++) {
        sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        sf_retained_deadline(&r, t0, &deadline);
    }
    SF_CHECK(&r, r.frozen == 1 && ps.subscribe_ok && ps.fill_created == 1 &&
                  ps.largest_object == 6 && ps.fill_objects == 0 && deadline != UINT64_MAX);
    moqr_shards_pair_stats_t before = {0}, after = {0};
    SF_CHECK(&r, moqr_shards_get_pair_stats(r.s, 0, 1, &before, sizeof(before)) == MOQR_OK);
    r.frozen = 2;
    r.carried |= 1;
    for (int i = 0; i < 12; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    if (!sf_retained_requested(&r, &o, label)) goto done;
    SF_CHECK(&r, moqr_shards_get_pair_stats(r.s, 0, 1, &after, sizeof(after)) == MOQR_OK);
    SF_CHECK(&r, after.data_messages == before.data_messages + 5 &&
                  after.data_bytes == before.data_bytes + 16 &&
                  ps.fill_objects == 0 && ps.fill_terminals == 0);
    sf_retained_accept(&r, &o);
    for (int i = 0; i < 4; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
    SF_CHECK(&r, sfrig_next_deadline(&r) == deadline && r.now < deadline);
    if (cancel) {
        SF_CHECK(&r, moq_session_cancel_fill(sub->peer, ps.fill, r.now) == MOQ_OK);
        r.frozen = 0;
        r.carried |= 3;
        for (int i = 0; i < 16; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        SF_CHECK(&r, o.cancelled == 1 && ps.fill_terminals == 1 &&
                      ps.fill_term_reason == MOQ_FILL_LOCAL_CANCELLED &&
                      ps.fill_objects == 0 && ps.fill_gaps == 0 && r.now < deadline);
    } else {
        for (int i = 0; i < 120 && r.now < deadline + 4000u; i++) {
            sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        }
        SF_CHECK(&r, r.frozen == 2 && o.cancelled == 1 && ps.fill_objects == 0 &&
                      ps.fill_gaps == 0 && ps.fill_terminals == 0);
        r.frozen = 0;
        r.carried |= 3;
        for (int i = 0; i < 20; i++) sf_retained_step(&r, &o, prime, &pps, sub, &ps);
        static const sf_item_t want[] = {
            { SF_IT_OBJ, 0, 0, 0 }, { SF_IT_OBJ, 0, 1, 0 },
            { SF_IT_OBJ, 0, 2, 0 }, { SF_IT_OBJ, 0, 3, 0 },
            { SF_IT_GAP, 0, 6, MOQ_FETCH_RANGE_TIMED_OUT },
            { SF_IT_TERM, 0, 0, MOQ_FILL_COMPLETE },
        };
        sf_expect_items(&r, &ps, want, 6, label);
        for (int i = 0; i < 4; i++) {
            const uint8_t body[] = { 0, (uint8_t)i, 0xC3, 0x3C };
            SF_CHECK(&r, ps.items[i].len == sizeof(body) &&
                          memcmp(ps.items[i].body, body, sizeof(body)) == 0);
        }
        SF_CHECK(&r, ps.fill_objects == 4 && ps.fill_gaps == 1 && ps.fill_terminals == 1 &&
                      ps.fill_term_at >= deadline && ps.fill_term_at <= deadline + 24000u &&
                      ps.fill_term_domain == MOQ_FILL_ERROR_NONE);
    }
    SF_CHECK(&r, o.requests == 1 && o.cancelled == 1 &&
                  moq_session_end_fetch(o.cn->peer, o.fetch, r.now) == MOQ_ERR_STALE_HANDLE);
    SF_CHECK(&r, !ps.done_seen && !ps.session_closed && pps.objects == 4 && ps.objects == 0 &&
                  sf_ingested(&r, 0) == 4 && sf_ingested(&r, 1) == 0);
    SF_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    SF_CHECK(&r, moq_session_unsubscribe(prime->peer, ph, r.now) == MOQ_OK);
    sf_retained_quiescent(&r);
done:
    moq_rcbuf_decref(o.payload);
    return sf_finish(&r, &a, label);
}

static int
cross_shard_retained_controls(void)
{
    for (int mode = 0; mode < SF_TYPED_COUNT; mode++) {
        if (cross_shard_retained_typed((enum sf_typed_control)mode) != 0) return 1;
    }
    if (cross_shard_retained_zero_credit(false) != 0) return 1;
    return cross_shard_retained_zero_credit(true) != 0;
}

/* The old origin stays connected with a FETCH header already accepted. Its
 * real subscription completes and namespace withdraws before a replacement
 * advertises the same key. Pause only old transport delivery so its late
 * payload crosses after a fresh demand has reached the replacement. */
static int
cross_shard_retained_replacement(const char *label)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    if (sfrig_create_ex(&r, &a, true, 1, 1) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sf_retained_origin_t old, fresh = {0};
    sf_retained_announce(&r, &old, false, 0xE9);
    sfconn_t *prime = sfrig_connect(&r, 0);
    sfconn_t *sub = sfrig_connect(&r, 1);
    SF_CHECK(&r, prime != NULL && sub != NULL);
    if (r.failures != 0) goto done;
    sfrig_pump(&r, 4);
    sfps_t pps = {0}, ps = {0};
    (void)sf_retained_subscribe(&r, prime, false);
    for (int i = 0; i < 12; i++) {
        sf_retained_step(&r, &old, prime, &pps, sub, &ps);
    }
    SF_CHECK(&r, pps.subscribe_ok && old.subscribes == 1 && old.requests == 0);
    if (r.failures != 0) goto done;
    const uint64_t t0 = r.now;
    uint64_t deadline = UINT64_MAX;
    (void)sf_retained_subscribe(&r, sub, true);
    for (int i = 0; i < 20; i++) {
        sf_retained_step(&r, &old, prime, &pps, sub, &ps);
        sf_retained_deadline(&r, t0, &deadline);
    }
    SF_CHECK(&r, ps.subscribe_ok && ps.fill_created == 1 && deadline != UINT64_MAX);
    SF_CHECK(&r, ps.fill_terminals == 0 && ps.fill_objects == 0 && ps.fill_gaps == 0);
    if (!sf_retained_requested(&r, &old, label)) goto done;
    sf_retained_accept(&r, &old);
    for (int i = 0; i < 4; i++) {
        sf_retained_step(&r, &old, prime, &pps, sub, &ps);
        SF_CHECK(&r, sfrig_next_deadline(&r) == deadline);
    }
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x2; /* Track Ended, not connection loss. */
    SF_CHECK(&r, moq_session_done_subscribe(old.cn->peer, old.sub, &dc, r.now) == MOQ_OK);
    SF_CHECK(&r, moq_session_publish_namespace_done(old.cn->peer, old.ann, r.now) == MOQ_OK);
    /* The cycle delivers the source terminal; bind work queues cancellation
     * afterwards. Hold that outbound work while the old peer can still write. */
    sfrig_cycle(&r, 1000);
    old.cn->transport_paused = true;
    for (int i = 0; i < 8; i++) {
        sfrig_cycle(&r, 1000);
        sfps_drain(&r, prime, &pps);
        sfps_drain(&r, sub, &ps);
    }
    SF_CHECK(&r, ps.done_seen && ps.fill_terminals == 1 && ps.fill_objects == 0 &&
                  ps.fill_gaps == 0 && ps.fill_term_at < deadline);
    SF_CHECK(&r, (ps.fill_term_reason == MOQ_FILL_PARENT_TERMINATED &&
                   ps.fill_term_domain == MOQ_FILL_ERROR_PARENT_DONE) ||
                 (ps.fill_term_reason == MOQ_FILL_PEER_CANCELLED_OR_RESET &&
                   ps.fill_term_domain == MOQ_FILL_ERROR_STREAM));
    SF_CHECK(&r, !ps.session_closed && pps.done_seen && !pps.session_closed);
    if (r.failures != 0) goto done;
    sf_retained_announce(&r, &fresh, false, 0xB2);
    sfconn_t *retry = sfrig_connect(&r, 1);
    SF_CHECK(&r, retry != NULL);
    if (r.failures != 0) goto done;
    sfrig_pump(&r, 4);
    sfps_t rps = {0};
    const uint64_t retry_t0 = r.now;
    uint64_t retry_deadline = UINT64_MAX;
    moq_subscription_t rh = sf_retained_subscribe(&r, retry, true);
    for (int i = 0; i < 20; i++) {
        sf_retained_step(&r, &fresh, NULL, NULL, retry, &rps);
        sf_retained_deadline(&r, retry_t0, &retry_deadline);
        sfps_drain(&r, sub, &ps);
    }
    if (!sf_retained_requested(&r, &fresh, label)) goto done;
    SF_CHECK(&r, fresh.cn != old.cn && fresh.subscribes == 1 &&
                  rps.subscribe_ok && rps.fill_created == 1);
    SF_CHECK(&r, retry_deadline != UINT64_MAX && rps.fill_objects == 0 &&
                  rps.fill_terminals == 0);
    /* No FIN: the pending cancellation must still retire this exact old
     * handle, not be hidden by a locally completed origin response. */
    sf_retained_write(&r, &old, false);
    old.cn->transport_paused = false;
    for (int i = 0; i < 8; i++) {
        sf_retained_step(&r, &fresh, NULL, NULL, retry, &rps);
        sf_retained_drain(&r, &old);
        sfps_drain(&r, sub, &ps);
        SF_CHECK(&r, sfrig_next_deadline(&r) == retry_deadline);
    }
    SF_CHECK(&r, old.cancelled == 1 && old.requests == 1);
    SF_CHECK(&r, moq_session_end_fetch(old.cn->peer, old.fetch, r.now) ==
                  MOQ_ERR_STALE_HANDLE);
    SF_CHECK(&r, rps.fill_objects == 0 && rps.fill_gaps == 0 &&
                  rps.fill_terminals == 0);
    sf_retained_accept(&r, &fresh);
    sf_retained_write(&r, &fresh, true);
    for (int i = 0; i < 16; i++) {
        sf_retained_step(&r, &fresh, NULL, NULL, retry, &rps);
        sf_retained_drain(&r, &old);
        sfps_drain(&r, prime, &pps);
        sfps_drain(&r, sub, &ps);
    }
    sf_retained_exact(&r, &rps, false, 0xB2, label);
    SF_CHECK(&r, rps.fill_term_at < retry_deadline);
    SF_CHECK(&r, ps.fill_terminals == 1 && ps.fill_objects == 0 && ps.fill_gaps == 0);
    SF_CHECK(&r, pps.objects == 0 && ps.objects == 0 && rps.objects == 0);
    SF_CHECK(&r, sf_ingested(&r, 0) == 0 && sf_ingested(&r, 1) == 0);
    SF_CHECK(&r, old.cancelled == 1 && fresh.cancelled == 0 && fresh.requests == 1);
    SF_CHECK(&r, moq_session_state(old.cn->peer) == MOQ_SESS_ESTABLISHED &&
                  moq_session_state(fresh.cn->peer) == MOQ_SESS_ESTABLISHED &&
                  moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED &&
                  moq_session_state(retry->peer) == MOQ_SESS_ESTABLISHED);
    static const uint64_t live[] = { 0 };
    sf_pub_ids(&r, fresh.cn, fresh.sub, 1, 0, live, 1);
    for (int i = 0; i < 10; i++) {
        sf_retained_step(&r, &fresh, NULL, NULL, retry, &rps);
        sf_retained_drain(&r, &old);
        sfps_drain(&r, sub, &ps);
    }
    SF_CHECK(&r, rps.objects == 1 && rps.live_obj[0][0] == 1 &&
                  rps.live_obj[0][1] == 0 && ps.objects == 0);
    SF_CHECK(&r, rps.fill_terminals == 1 && ps.fill_terminals == 1 &&
                  !rps.done_seen && !rps.session_closed);
    SF_CHECK(&r, moq_session_unsubscribe(retry->peer, rh, r.now) == MOQ_OK);
    sf_retained_quiescent(&r);
    sf_retained_drain(&r, &old);
    sf_retained_drain(&r, &fresh);
    sfps_drain(&r, sub, &ps);
    sfps_drain(&r, retry, &rps);
    SF_CHECK(&r, ps.fill_created == 1 && ps.fill_terminals == 1 &&
                  ps.fill_objects == 0 && ps.fill_gaps == 0);
    SF_CHECK(&r, rps.fill_created == 1 && rps.fill_terminals == 1 &&
                  rps.fill_objects == 1 && rps.fill_gaps == 0);
    SF_CHECK(&r, old.cancelled == 1 && fresh.cancelled == 0);
done:
    moq_rcbuf_decref(old.payload);
    moq_rcbuf_decref(fresh.payload);
    return sf_finish(&r, &a, label);
}

/* Initial Track Properties cross the owner/requester ACK independently of
 * Object Properties. The Immutable container's inner type 0x7a/value 42
 * deliberately use nonminimal vi64 encodings; outer delta 0x6d selects
 * unknown nonmandatory type 0x78/value 17. Expectations are literal bytes,
 * not an encoder round trip or the origin's mutable input buffer. */
static const uint8_t sf_metadata_expected[] = {
    0x0b, 0x04, 0x80, 0x7a, 0x80, 0x2a, 0x6d, 0x11
};
static const uint8_t sf_metadata_object_properties[] = { 0x7c, 0x23 };
static const uint8_t sf_metadata_payload[] = { 0x44, 0x00, 0xab, 0x19 };

typedef struct sf_metadata_peer {
    bool origin;
    moq_subscription_t sub;
    moq_announcement_t ann;
    int setups, announcements, requests, acks, objects, unsubs;
    size_t properties_len;
    bool properties_match;
} sf_metadata_peer_t;

static void
sf_metadata_expect(sfrig_t *r, const char *row, const char *subject,
                    const char *fact, uint64_t expected, uint64_t actual)
{
    if (actual != expected) {
        printf("FIXTURE: cross_shard_track_properties(%s) %s.%s "
               "expected=%llu actual=%llu\n", row, subject, fact,
               (unsigned long long)expected, (unsigned long long)actual);
        r->failures++;
    }
}

/* A missing ACK, refusal, wrong identity or session failure is fixture
 * failure, never the expected metadata failure. No event-scratch view escapes. */
static void
sf_metadata_drain(sfrig_t *r, sfconn_t *cn, sf_metadata_peer_t *ps,
                   bool with_properties)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) != 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            ps->setups++;
            break;
        case MOQ_EVENT_REQUEST_READY:
            break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            SF_CHECK(r, ps->origin &&
                            ev.u.namespace_accepted.ann._opaque == ps->ann._opaque);
            ps->announcements++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            const moq_subscribe_request_event_t *q = &ev.u.subscribe_request;
            SF_CHECK(r, ps->origin && ps->requests == 0 && q->sub._opaque != 0);
            SF_CHECK(r, q->track_namespace.count == 2 &&
                            q->track_namespace.parts != NULL &&
                            q->track_namespace.parts[0].len == 5 &&
                            q->track_namespace.parts[0].data != NULL &&
                            memcmp(q->track_namespace.parts[0].data, "props", 5) == 0 &&
                            q->track_namespace.parts[1].len == 4 &&
                            q->track_namespace.parts[1].data != NULL &&
                            memcmp(q->track_namespace.parts[1].data, "live", 4) == 0);
            SF_CHECK(r, q->track_name.len == 5 && q->track_name.data != NULL &&
                            memcmp(q->track_name.data, "video", 5) == 0);
            ps->sub = q->sub;
            ps->requests++;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_OK: {
            const moq_subscribe_ok_event_t *ok = &ev.u.subscribe_ok;
            SF_CHECK(r, !ps->origin && ps->sub._opaque != 0 &&
                            ok->sub._opaque == ps->sub._opaque);
            SF_CHECK(r, ok->has_largest && ok->largest_group == 0 &&
                            ok->largest_object == 0);
            ps->acks++;
            ps->properties_len = ok->track_properties.len;
            ps->properties_match = with_properties
                ? ok->track_properties.len == sizeof(sf_metadata_expected) &&
                      ok->track_properties.data != NULL &&
                      memcmp(ok->track_properties.data, sf_metadata_expected,
                             sizeof(sf_metadata_expected)) == 0
                : ok->track_properties.len == 0;
            break;
        }
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *ob = &ev.u.object_received;
            SF_CHECK(r, !ps->origin && ob->sub._opaque == ps->sub._opaque &&
                            ob->group_id == 1 && ob->object_id == 0 &&
                            ob->datagram && ob->publisher_priority == 90 &&
                            ob->status == MOQ_OBJECT_NORMAL);
            SF_CHECK(r, ob->payload != NULL &&
                            moq_rcbuf_len(ob->payload) == sizeof(sf_metadata_payload) &&
                            memcmp(moq_rcbuf_data(ob->payload), sf_metadata_payload,
                                   sizeof(sf_metadata_payload)) == 0);
            SF_CHECK(r, ob->properties != NULL &&
                            moq_rcbuf_len(ob->properties) ==
                                sizeof(sf_metadata_object_properties) &&
                            memcmp(moq_rcbuf_data(ob->properties),
                                   sf_metadata_object_properties,
                                   sizeof(sf_metadata_object_properties)) == 0);
            ps->objects++;
            break;
        }
        case MOQ_EVENT_UNSUBSCRIBED:
            SF_CHECK(r, ps->origin &&
                            ev.u.unsubscribed.sub._opaque == ps->sub._opaque);
            ps->unsubs++;
            break;
        default:
            printf("FIXTURE: cross_shard_track_properties unexpected event %d\n",
                   (int)ev.kind);
            r->failures++;
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* 0 = pass, 1 = metadata mismatch only, 2 = fixture/control failure. */
static int
cross_shard_track_properties(bool with_properties)
{
    ca_t a;
    ca_init(&a);
    sfrig_t r;
    const char *row = with_properties ? "metadata" : "empty";
    if (sfrig_create(&r, &a, true) != MOQR_OK) {
        printf("FIXTURE: cross_shard_track_properties(%s) rig create\n", row);
        sfrig_destroy(&r);
        return 2;
    }
    sfconn_t *cn[3] = {0};   /* origin, first requester, later requester */
    sf_metadata_peer_t ps[3] = {0};
    ps[0].origin = true;
    moq_rcbuf_t *payload = NULL;
    moqr_shards_stats_t owner_stats, requester_stats;
#define SF_METADATA_REQUIRE(expr)                                            \
    do {                                                                     \
        if (!(expr)) {                                                       \
            printf("FIXTURE: cross_shard_track_properties(%s): %s\n",        \
                   row, #expr);                                              \
            r.failures++;                                                    \
            goto cleanup;                                                    \
        }                                                                    \
    } while (0)
    for (size_t i = 0; i < 3; i++) {
        uint16_t shard = i == 0 ? 0u : 1u;
        cn[i] = sfrig_connect(&r, shard);
        SF_METADATA_REQUIRE(cn[i] != NULL && cn[i]->shard == shard);
        SF_METADATA_REQUIRE(moqr_bind_conn_is_open(moqr_shards_bind(r.s, shard),
                                                   cn[i]->rsess));
        SF_METADATA_REQUIRE(!moqr_bind_conn_is_open(
            moqr_shards_bind(r.s, (uint16_t)(shard ^ 1u)), cn[i]->rsess));
    }
    SF_METADATA_REQUIRE(cn[0]->shard != cn[1]->shard &&
                        cn[1]->shard == cn[2]->shard);
    sfrig_pump(&r, 4);
    for (size_t i = 0; i < 3; i++) {
        sf_metadata_drain(&r, cn[i], &ps[i], with_properties);
        SF_METADATA_REQUIRE(ps[i].setups == 1);
        SF_METADATA_REQUIRE(moq_session_state(cn[i]->peer) == MOQ_SESS_ESTABLISHED &&
                            moq_session_state(cn[i]->rsess) == MOQ_SESS_ESTABLISHED);
    }
    SF_METADATA_REQUIRE(r.failures == 0);

    moq_bytes_t ns[] = { B("props"), B("live") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    SF_METADATA_REQUIRE(moq_session_publish_namespace(
        cn[0]->peer, &pcfg, r.now, &ps[0].ann) == MOQ_OK);
    sfrig_pump(&r, 8);
    sf_metadata_drain(&r, cn[0], &ps[0], with_properties);
    SF_METADATA_REQUIRE(ps[0].announcements == 1 && r.failures == 0);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.location = &loc;
    SF_METADATA_REQUIRE(moq_session_subscribe(cn[1]->peer, &scfg, r.now,
                                              &ps[1].sub) == MOQ_OK);
    sfrig_pump(&r, 8);
    sf_metadata_drain(&r, cn[0], &ps[0], with_properties);
    sf_metadata_drain(&r, cn[1], &ps[1], with_properties);
    SF_METADATA_REQUIRE(ps[0].requests == 1 && ps[1].acks == 0 && r.failures == 0);

    /* Keep the requester's actual shard frozen, not just its observation:
     * the owner ACK must remain queued while upstream event scratch advances. */
    r.frozen = 1ull << cn[1]->shard;
    uint8_t input_properties[] = {
        0x0b, 0x04, 0x80, 0x7a, 0x80, 0x2a, 0x6d, 0x11
    };
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_largest = true;
    acfg.largest_group = 0;
    acfg.largest_object = 0;
    if (with_properties) {
        acfg.track_properties = (moq_bytes_t){
            .data = input_properties, .len = sizeof(input_properties)
        };
    }
    SF_METADATA_REQUIRE(moq_session_accept_subscribe(cn[0]->peer, ps[0].sub,
                                                     &acfg, r.now) == MOQ_OK);
    memset(input_properties, 0xe6, sizeof(input_properties));
    sfrig_pump(&r, 4);
    SF_METADATA_REQUIRE(sfrig_owner_acks(&r) == 1 && r.failures == 0);

    moq_bytes_t overwrite_ns[] = { B("scratch-reuse-after-shard-track-properties") };
    pcfg.track_namespace = (moq_namespace_t){ .parts = overwrite_ns, .count = 1 };
    SF_METADATA_REQUIRE(moq_session_publish_namespace(
        cn[0]->peer, &pcfg, r.now, &ps[0].ann) == MOQ_OK);
    sfrig_pump(&r, 8);
    sf_metadata_drain(&r, cn[0], &ps[0], with_properties);
    sf_metadata_drain(&r, cn[1], &ps[1], with_properties);
    SF_METADATA_REQUIRE(ps[0].announcements == 2 && ps[1].acks == 0 &&
                        ps[0].requests == 1 && r.failures == 0);
    SF_METADATA_REQUIRE(moqr_shards_get_stats(r.s, 1, &requester_stats) == MOQR_OK);
    SF_METADATA_REQUIRE(requester_stats.inbound_channel_entries > 0 &&
                        requester_stats.enqueued[MOQR_SHARDS_MSG_DEMAND] == 1);
    r.frozen = 0;
    sfrig_pump(&r, 8);
    sf_metadata_drain(&r, cn[1], &ps[1], with_properties);
    SF_METADATA_REQUIRE(ps[1].acks == 1 && r.failures == 0);

    /* The first remote subscriber stays live. No second owner ACK or
     * upstream SUBSCRIBE may refresh the later ACTIVE requester's snapshot. */
    SF_METADATA_REQUIRE(moq_session_subscribe(cn[2]->peer, &scfg, r.now,
                                              &ps[2].sub) == MOQ_OK);
    sfrig_pump(&r, 8);
    for (size_t i = 0; i < 3; i++) {
        sf_metadata_drain(&r, cn[i], &ps[i], with_properties);
    }
    SF_METADATA_REQUIRE(ps[0].requests == 1 && ps[0].unsubs == 0 &&
                        ps[1].acks == 1 && ps[2].acks == 1 && r.failures == 0);
    SF_METADATA_REQUIRE(moqr_shards_get_stats(r.s, 0, &owner_stats) == MOQR_OK);
    SF_METADATA_REQUIRE(moqr_shards_get_stats(r.s, 1, &requester_stats) == MOQR_OK);
    SF_METADATA_REQUIRE(owner_stats.enqueued[MOQR_SHARDS_MSG_ACK] == 1 &&
                        owner_stats.pump_subs_active == 1 &&
                        requester_stats.enqueued[MOQR_SHARDS_MSG_DEMAND] == 1 &&
                        requester_stats.pending_demands == 1);

    /* Observe the real origin and routed source, without supplying metadata
     * or making an unmanaged downstream binding into a source. */
    for (uint16_t shard = 0; shard < 2; shard++) {
        moqr_core_t *core = moqr_shards_core(r.s, shard);
        moqr_binding_t observer;
        moqr_sub_t sub;
        moqr_track_t track;
        uint64_t generation = 0, current = 0;
        bool managed = false;
        SF_METADATA_REQUIRE(moqr_core_binding_open(core, 900, &observer) == MOQR_OK);
        moqr_subscribe_req_t request;
        moqr_subscribe_req_init(&request);
        request.ns = (moqr_ns_t){ .parts = ns, .count = 2 };
        request.name = B("video");
        request.forward = false;
        SF_METADATA_REQUIRE(moqr_core_subscribe(core, observer, &request, &sub) == MOQR_OK);
        SF_METADATA_REQUIRE(moqr_core_sub_track(core, sub, &track, &generation) == MOQR_OK);
        SF_METADATA_REQUIRE(moqr_core_track_current_generation(core, track, &current) == MOQR_OK &&
                            current == generation);
        SF_METADATA_REQUIRE(moqr_core_track_metadata_managed(core, track, &managed) == MOQR_OK &&
                            managed);
        SF_METADATA_REQUIRE(moqr_core_unsubscribe(core, sub, r.now) == MOQR_OK);
        SF_METADATA_REQUIRE(moqr_core_binding_close(core, observer, r.now) == MOQR_OK);
    }

    const uint8_t body[] = { 0x44, 0x00, 0xab, 0x19 };
    const uint8_t object_properties[] = { 0x7c, 0x23 };
    SF_METADATA_REQUIRE(moq_rcbuf_create(&a.vt, body, sizeof(body), &payload) == MOQ_OK);
    moq_result_t sent = moq_session_send_object_datagram(
        cn[0]->peer, ps[0].sub, 1, 0, 90, false, payload,
        object_properties, sizeof(object_properties), r.now);
    moq_rcbuf_decref(payload);
    payload = NULL;
    SF_METADATA_REQUIRE(sent == MOQ_OK);
    sfrig_pump(&r, 8);
    for (size_t i = 0; i < 3; i++) {
        sf_metadata_drain(&r, cn[i], &ps[i], with_properties);
    }
    SF_METADATA_REQUIRE(ps[1].objects == 1 && ps[2].objects == 1 && r.failures == 0);

    SF_METADATA_REQUIRE(moq_session_unsubscribe(cn[1]->peer, ps[1].sub, r.now) == MOQ_OK);
    SF_METADATA_REQUIRE(moq_session_unsubscribe(cn[2]->peer, ps[2].sub, r.now) == MOQ_OK);
    /* The earned-wake rig schedules fill deadlines, not core linger ticks.
     * An ACKED pull demand needs a later requester tick to send UNDEMAND,
     * then a later owner tick to unsubscribe upstream. Drive only teardown
     * with the existing round stepper; keep the same twelve 1 ms cycles. */
    r.earned = false;
    sfrig_pump(&r, 12);
    const char *peer_names[] = { "origin", "initial", "active" };
    for (size_t i = 0; i < 3; i++) {
        sf_metadata_drain(&r, cn[i], &ps[i], with_properties);
        sf_metadata_expect(&r, row, peer_names[i], "setups", 1,
                           (uint64_t)ps[i].setups);
        sf_metadata_expect(&r, row, peer_names[i], "peer_state",
                           MOQ_SESS_ESTABLISHED, moq_session_state(cn[i]->peer));
        sf_metadata_expect(&r, row, peer_names[i], "relay_state",
                           MOQ_SESS_ESTABLISHED, moq_session_state(cn[i]->rsess));
    }
    sf_metadata_expect(&r, row, "origin", "requests", 1, (uint64_t)ps[0].requests);
    sf_metadata_expect(&r, row, "origin", "unsubs", 1, (uint64_t)ps[0].unsubs);
    for (size_t i = 1; i < 3; i++) {
        sf_metadata_expect(&r, row, peer_names[i], "acks", 1, (uint64_t)ps[i].acks);
        sf_metadata_expect(&r, row, peer_names[i], "objects", 1,
                           (uint64_t)ps[i].objects);
    }
    for (uint16_t shard = 0; shard < 2; shard++) {
        const char *side = shard == 0 ? "owner" : "requester";
        moqr_bind_stats_t stats;
        moqr_bind_get_stats(moqr_shards_bind(r.s, shard), &stats);
        sf_metadata_expect(&r, row, side, "session_errors", 0, stats.session_errors);
        sf_metadata_expect(&r, row, side, "ingest_refusals", 0, stats.ingest_refusals);
        moqr_shards_stats_t shards_stats;
        SF_METADATA_REQUIRE(moqr_shards_get_stats(r.s, shard, &shards_stats) == MOQR_OK);
        sf_metadata_expect(&r, row, side, "pending_demands", 0,
                           shards_stats.pending_demands);
        sf_metadata_expect(&r, row, side, "pump_subs_active", 0,
                           shards_stats.pump_subs_active);
        sf_metadata_expect(&r, row, side, "pump_subs_parked", 0,
                           shards_stats.pump_subs_parked);
    }
    sf_expect_channels_idle(&r);

cleanup:
    moq_rcbuf_decref(payload);
    sfrig_destroy(&r);
    SF_CHECK(&r, a.live == 0);
#undef SF_METADATA_REQUIRE
    if (r.failures != 0) {
        printf("FIXTURE: cross_shard_track_properties(%s) failures=%d\n",
               row, r.failures);
        return 2;
    }
    int metadata_failures = 0;
    for (size_t i = 1; i < 3; i++) {
        if (!ps[i].properties_match) {
            printf("FAIL: cross_shard_track_properties(%s) %s_ack_properties "
                   "expected_len=%zu actual_len=%zu\n", row,
                   i == 1 ? "initial" : "active",
                   with_properties ? sizeof(sf_metadata_expected) : (size_t)0,
                   ps[i].properties_len);
            metadata_failures++;
        }
    }
    printf("%s: cross_shard_track_properties(%s) metadata_failures=%d fixture_failures=0\n",
           metadata_failures == 0 ? "PASS" : "FAIL", row, metadata_failures);
    return metadata_failures == 0 ? 0 : 1;
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--descending-fill") == 0) {
        return cross_shard_fill_split(true, "cross_shard_fill_split_descending") != 0;
    }
    if (argc == 2 && strcmp(argv[1], "--retained-empty") == 0) {
        return cross_shard_retained_origin(false, "cross_shard_retained_empty") ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--retained-hole") == 0) {
        return cross_shard_retained_origin(true, "cross_shard_retained_hole") ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--retained-replacement") == 0) {
        return cross_shard_retained_replacement("cross_shard_retained_replacement") ? 1 : 0;
    }
    if (argc == 2 && strcmp(argv[1], "--retained-controls") == 0) {
        return cross_shard_retained_controls();
    }
    if (argc == 2 && strcmp(argv[1], "--retained-push-refused") == 0) {
        return cross_shard_retained_typed(SF_TYPED_PUSH_REFUSED) != 0;
    }
    if (argc == 2 && strcmp(argv[1], "--track-properties") == 0) {
        if (cross_shard_track_properties(false) != 0) {
            printf("FIXTURE: cross-shard track-properties empty control failure\n");
            return 2;
        }
        return cross_shard_track_properties(true);
    }
    bool cache_only = argc == 2 && strcmp(argv[1], "--cache-controls") == 0;
    if (argc != 1 && !cache_only) {
        fprintf(stderr, "usage: %s [--track-properties|--retained-empty|"
                        "--retained-hole|--retained-replacement|--retained-controls|"
                        "--retained-push-refused|--cache-controls]\n", argv[0]);
        return 2;
    }
    int failures = 0;
    if (!cache_only) {
        failures += cross_shard_track_properties(false);
        failures += cross_shard_track_properties(true);
        failures += cross_shard_retained_origin(false, "cross_shard_retained_empty");
        failures += cross_shard_retained_origin(true, "cross_shard_retained_hole");
        failures += cross_shard_retained_replacement("cross_shard_retained_replacement");
        failures += cross_shard_retained_controls();
    }
    failures += cross_shard_fill_matches_owner(false, false, true, 300,
                                               "cross_shard_fill_matches_owner");
    failures += cross_shard_fill_matches_owner(false, false, false, 0,
                                               "cross_shard_fill_default_wait");
    failures += cross_shard_fill_matches_owner(true, false, true, 300,
                                               "cross_shard_fill_earned");
    failures += cross_shard_fill_matches_owner(false, true, true, 300,
                                               "cross_shard_fill_track_active");
    failures += cross_shard_fill_matches_owner(true, true, true, 300,
                                               "cross_shard_fill_track_active_earned");
    failures += cross_shard_fill_timeout_zero("cross_shard_fill_timeout_zero");
    failures += cross_shard_fetch_range("cross_shard_fetch_range");
    failures += cross_shard_fill_in_progress(true, "cross_shard_fill_in_progress");
    failures += cross_shard_fill_in_progress(false,
                                             "cross_shard_fill_in_progress_deadline");
    failures += cross_shard_fill_split(false, "cross_shard_fill_split");
    failures += cross_shard_fill_split(true, "cross_shard_fill_split_descending");
    failures += cross_shard_answer_after_deadline(
        "cross_shard_answer_after_deadline");
    failures += cross_shard_full_channel_defers(
        200, "cross_shard_full_channel_defers");
    failures += cross_shard_full_channel_defers(
        20, "cross_shard_full_channel_deferred_answer");
    failures += cross_shard_fill_window(false, "cross_shard_fill_window");
    failures += cross_shard_fill_window(true, "cross_shard_fill_window_earned");
    failures += cross_shard_fill_slot_wait("cross_shard_fill_slot_wait");
    failures += cross_shard_fill_conn_gone("cross_shard_fill_conn_gone");
    failures += cross_shard_live_busy_channel("cross_shard_live_busy_channel");
    failures += cross_shard_hole_under_live_stream(
        "cross_shard_hole_under_live_stream");
    static const uint64_t all[3] = { 0, 1, 2 };
    static const uint64_t holed[2] = { 0, 2 };
    static const sf_late_expect_t served = {
        .objects = 3, .obj = { { 0, 0 }, { 0, 1 }, { 0, 2 } },
    };
    static const sf_late_expect_t hole = {
        .objects = 2, .obj = { { 0, 0 }, { 0, 2 } },
        .timed_out = true, .to_group = 0, .to_object = 1,
        .at_deadline = true,
    };
    failures += late_range_fill(0, all, 3, &served, "late_range_served");
    failures += late_range_fill(0, holed, 2, &hole,
                                "late_range_hole_timed_out");
    failures += late_range_fill(1, all, 3, &served,
                                "late_range_cross_shard_served");
    failures += late_range_fill(1, holed, 2, &hole,
                                "late_range_cross_shard_hole_timed_out");
    static const uint64_t hole_id[1] = { 1 };
    static const sf_late_expect_t filled = {
        .objects = 3, .obj = { { 0, 0 }, { 0, 1 }, { 0, 2 } },
    };
    failures += late_range_fill_ex(0, holed, 2, hole_id, 1, &filled,
                                   "late_hole_filled");
    failures += late_range_fill_ex(1, holed, 2, hole_id, 1, &filled,
                                   "late_hole_filled_cross_shard");
    printf("%s: %d failure(s)\n", failures == 0 ? "OK" : "FAILED", failures);
    return failures == 0 ? 0 : 1;
}
