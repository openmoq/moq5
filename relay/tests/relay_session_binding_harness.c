/*
 * Definitions behind relay_session_binding_harness.h: the counting
 * allocator, the SimPair rig attached to the production binding, the
 * peer-side event drain, and the fixtures and flows the session-binding
 * suites share.
 */
#include "relay_session_binding_harness.h"

void *relay_bind_test_ca_a(size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* injected OOM at this attempt index */
    }
    if (a->fail_size != 0 && n == a->fail_size) {
        a->fail_size = 0;   /* one-shot: later cleanup allocs succeed */
        return NULL;
    }
    void *p = malloc(n);
    if (p) {
        a->allocs++;
        a->live += (long)n;
    }
    return p;
}
void *relay_bind_test_ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* realloc failure: caller keeps the old buffer p */
    }
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
    }
    return q;
}
void relay_bind_test_ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
void relay_bind_test_ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = relay_bind_test_ca_a;
    a->vt.realloc = relay_bind_test_ca_r;
    a->vt.free = relay_bind_test_ca_f;
}

/* 0 = default; a small value constrains the SimPair sessions' action queues so
 * a burst of publisher-side cancels backs up (cancel_namespace WOULD_BLOCK). */
uint32_t relay_bind_test_max_actions = 0;

/* false = whole-object OBJECT_RECEIVED ingest (default); true creates the
 * relay-side (server) session with streaming_objects so the relay receives
 * OBJECT_CHUNK and ingests into OPEN records. Set before relay_bind_test_rig_connect. */
bool relay_bind_test_server_streaming_objects = false;

/* true creates CLIENT sessions with streaming_objects so a subscriber observes
 * downstream OBJECT_CHUNK events (live-edge chunks, terminal RESET). Set before
 * relay_bind_test_rig_connect; live-edge tests use it to see the wire-visible chunk stream. */
bool relay_bind_test_client_streaming_objects = false;

/* 0 = default; a small value constrains the binding's downstream subgroup slot
 * pool so a burst of distinct (group, subgroup) streams exhausts it. */
uint32_t relay_bind_test_max_open_subgroups = 0;

/* 0 = default (8); a small value makes the log evict old groups quickly so the
 * eviction / subgroup-cleanup paths are exercised. */
uint32_t relay_bind_test_log_max_groups = 0;

/* 0 = 1 MiB; a small value makes the log refuse an object larger than it. */
uint64_t relay_bind_test_log_max_bytes = 0;

/* Track Properties the origin states when it accepts the relay's SUBSCRIBE
 * through relay_bind_test_peer_drain (empty = none). Borrowed. */
moq_bytes_t relay_bind_test_accept_track_properties = { NULL, 0 };

/* 0 = the origin chooses; otherwise the Track Alias the origin gives the
 * relay's SUBSCRIBE through relay_bind_test_peer_drain, so a test can address the relay's
 * upstream session with raw subgroup streams. */
uint64_t relay_bind_test_accept_track_alias = 0;

/* 0 = default (1024); the initial MAX_REQUEST_ID advertised by both sides. A
 * small value exhausts the d16 request-credit window quickly. */
uint32_t relay_bind_test_initial_request_capacity = 0;

/* 0 = binding default; the d16 request-capacity auto-grant window. */
uint32_t relay_bind_test_grant_window = 0;

/* 0 = binding defaults; how long a fill fetch stream waits for upstream before
 * marking a run it does not hold Timed-Out (absent FILL_TIMEOUT), and the cap
 * on a carried FILL_TIMEOUT. Milliseconds. Either nonzero: the rig applies the
 * pair with moqr_bind_set_fill_timeouts right after create. */
uint64_t relay_bind_test_fill_timeout_default_ms = 0;
uint64_t relay_bind_test_fill_timeout_max_ms = 0;

/* 0 = the full current struct; otherwise the struct_size the rig's bind cfg
 * carries, as a caller built against a smaller layout passes it. With
 * relay_bind_test_bind_cfg_exact the cfg is handed over as a heap block of exactly
 * that size, so a read past it is out of bounds under ASan. */
size_t relay_bind_test_bind_cfg_size = 0;
bool   relay_bind_test_bind_cfg_exact = false;

/* 0 = session default; the session event-queue depth. Bulk fixtures that
 * burst hundreds of objects down one stream need the consumer's queue
 * deeper than a delivery pass, or delivery stalls on the consumer's
 * backpressure until its next drain. */
uint32_t relay_bind_test_max_events = 0;

/* 0 = session default; the RELAY-side session's outgoing subgroup pool. A
 * small value makes the SESSION pool (not the bind slot table) the
 * downstream blocker. */
uint32_t relay_bind_test_server_max_open_subgroups = 0;

/* 0 = default (500); the warm-track linger. A large value makes the window
 * between a last-sub retire and the warm transition span several pumps, so a
 * retire that armed the deadline with a stale clock (now_us == 0) warms
 * PREMATURELY while a correctly-clocked one still lingers. */
uint32_t relay_bind_test_linger_us = 0;

/* Optional SimPair trace hook installed on every pair relay_bind_test_rig_connect creates
 * while it is set (diagnostics for the forwarding rows). */
moq_sim_trace_fn relay_bind_test_trace_fn = NULL;
void *relay_bind_test_trace_ctx = NULL;

/* SimPair transport faults configured on every pair relay_bind_test_rig_connect
 * creates while set (0 = none). They stay suppressed until the test calls
 * moq_simpair_enable_faults on that pair, so setup is never affected. */
uint32_t relay_bind_test_fault_flags = 0;
uint32_t relay_bind_test_fault_per_mille = 0;

conn_t *
relay_bind_test_rig_connect(rig_t *r, moq_version_t version)
{
    int slot = -1;
    for (int i = 0; i < MAX_CONNS; i++) {
        if (!r->conns[i].used) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        return NULL;
    }
    conn_t *cn = &r->conns[slot];
    memset(cn, 0, sizeof(*cn));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc->vt;
    cfg.seed = 0x5EED0000u + (uint64_t)slot;
    cfg.version = version;
    cfg.max_actions = relay_bind_test_max_actions; /* 0 = session default */
    cfg.max_events = relay_bind_test_max_events; /* 0 = session default */
    cfg.server_max_open_subgroups = relay_bind_test_server_max_open_subgroups;
    cfg.server_streaming_objects = relay_bind_test_server_streaming_objects;
    cfg.client_streaming_objects = relay_bind_test_client_streaming_objects;
    uint64_t init_cap = relay_bind_test_initial_request_capacity != 0
                            ? relay_bind_test_initial_request_capacity
                            : 1024;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = init_cap;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = init_cap;
    cfg.trace_fn = relay_bind_test_trace_fn;
    cfg.trace_ctx = relay_bind_test_trace_ctx;
    cfg.fault_flags = relay_bind_test_fault_flags;
    cfg.fault_per_mille = relay_bind_test_fault_per_mille;
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return NULL;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(r->bind, cn->rsess, version) != MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        return NULL;
    }
    cn->used = true;
    return cn;
}

/* One deterministic pump cycle: transport steps, then the production
 * binding does everything else (events -> core -> intents -> deliveries). */
void
relay_bind_test_rig_cycle(rig_t *r)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        if (cn->frozen) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

void
relay_bind_test_rig_pump(rig_t *r, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        relay_bind_test_rig_cycle(r);
    }
}

/* One cycle that transports every SimPair EXCEPT one, while still running the
 * real bind pump. Nothing about the session or bind path changes -- only which
 * existing pair is stepped -- so a connection's outbound queue can be held
 * full across pumps and the relay's writes to it genuinely meet WOULD_BLOCK. */
static void
rig_cycle_except(rig_t *r, const conn_t *skip)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used || cn == skip) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

/* Advance ONE pair's transport without running the bind pump, so several
 * publisher-side requests can be delivered to the relay and then drained by a
 * single bind pump. Needed to put two ordered intents in ONE pump batch. */
void
relay_bind_test_rig_step_pair(rig_t *r, conn_t *cn)
{
    r->now += 1000;
    if (cn == NULL || !cn->used) {
        return;
    }
    (void)moq_simpair_advance_to(cn->sp, r->now);
    size_t steps = 0;
    (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
}

/* Exactly one bind pump, no transport stepping. */
void
relay_bind_test_rig_bind_pump_once(rig_t *r)
{
    (void)moqr_bind_pump(r->bind, r->now);
}

void
relay_bind_test_rig_pump_except(rig_t *r, const conn_t *skip, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        rig_cycle_except(r, skip);
    }
}

/* Optional auth hook installed into the rig's core (NULL = allow-all). Tests
 * set it (and, optionally, a context) before relay_bind_test_rig_create and clear it after. */
moqr_authorize_fn relay_bind_test_authorize = NULL;
void *relay_bind_test_authorize_ctx = NULL;
/* 0 = leave the cfg default; a tiny value forces grant_reserve to fail on the
 * byte budget so the reserve-capacity fail-closed path is exercised. */
uint32_t relay_bind_test_grant_bytes = 0;
/* 0 = leave the defaults: the bind's aggregate fetch-forwarding byte pool and
 * the core's concurrent-fetch pool (which sizes the forwarding transactions). */
uint64_t relay_bind_test_fetch_forward_bytes = 0;
uint32_t relay_bind_test_max_fetches = 0;
/* 0 = leave the core's default track-table size. */
uint32_t relay_bind_test_max_tracks = 0;
/* 0 = leave the core's default namespace-trie size. */
uint32_t relay_bind_test_max_ns_nodes = 0;
/* 0 = leave the core's default parked-authorization pool. */
uint32_t relay_bind_test_max_parked = 0;
/* 0 = leave the binding's default announce-table size. */
uint32_t relay_bind_test_max_announces = 0;
/* 0 = leave the core's default namespace-subscription pool. */
uint32_t relay_bind_test_max_ns_subs = 0;

moqr_result_t
relay_bind_test_rig_create_ex(rig_t *r, ca_t *a, uint32_t bind_dsubs, uint32_t bind_usubs,
                              uint32_t bind_pubs, uint32_t core_intents)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    if (moqr_trace_create(&a->vt, 512, &r->trace) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = r->trace;
    cfg.log_budget.max_groups = relay_bind_test_log_max_groups != 0
                                    ? relay_bind_test_log_max_groups
                                    : 8;
    cfg.log_budget.max_bytes = relay_bind_test_log_max_bytes != 0 ? relay_bind_test_log_max_bytes
                                                         : 1 << 20;
    cfg.linger_us = relay_bind_test_linger_us != 0 ? relay_bind_test_linger_us : 500;
    cfg.max_intents = core_intents;
    cfg.authorize = relay_bind_test_authorize; /* NULL = allow-all (default)      */
    cfg.authorize_ctx = relay_bind_test_authorize_ctx;
    if (relay_bind_test_grant_bytes != 0) {
        cfg.grant_bytes = relay_bind_test_grant_bytes;
    }
    if (relay_bind_test_max_fetches != 0) {
        cfg.max_fetches = relay_bind_test_max_fetches;
    }
    if (relay_bind_test_max_tracks != 0) {
        cfg.max_tracks = relay_bind_test_max_tracks;
    }
    if (relay_bind_test_max_ns_nodes != 0) {
        cfg.max_ns_nodes = relay_bind_test_max_ns_nodes;
    }
    if (relay_bind_test_max_parked != 0) {
        cfg.max_parked = relay_bind_test_max_parked;
    }
    if (relay_bind_test_max_ns_subs != 0) {
        cfg.max_ns_subs = relay_bind_test_max_ns_subs;
    }
    if (moqr_core_create(&cfg, &r->core) != MOQR_OK) {
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a->vt);
    bcfg.core = r->core;
    bcfg.max_conns = MAX_CONNS;
    bcfg.max_downstream_subs = bind_dsubs;
    bcfg.max_upstream_subs = bind_usubs;
    bcfg.max_publishes = bind_pubs;
    bcfg.max_announces = relay_bind_test_max_announces;   /* 0 = default */
    bcfg.max_open_subgroups = relay_bind_test_max_open_subgroups; /* 0 = default */
    bcfg.request_grant_window = relay_bind_test_grant_window; /* 0 = default */
    bcfg.fetch_forward_bytes = relay_bind_test_fetch_forward_bytes; /* 0 = default */
    const moqr_bind_cfg_t *bcp = &bcfg;
    unsigned char *bblk = NULL;
    if (relay_bind_test_bind_cfg_size != 0) {
        bcfg.struct_size = (uint32_t)relay_bind_test_bind_cfg_size;
        if (relay_bind_test_bind_cfg_exact) {
            bblk = malloc(relay_bind_test_bind_cfg_size);
            if (bblk == NULL) {
                moqr_core_destroy(r->core);
                moqr_trace_destroy(r->trace);
                return MOQR_ERR_NOMEM;
            }
            memcpy(bblk, &bcfg, relay_bind_test_bind_cfg_size);
            bcp = (const moqr_bind_cfg_t *)(const void *)bblk;
        }
    }
    moqr_result_t brc = moqr_bind_create(bcp, &r->bind);
    free(bblk);
    if (brc == MOQR_OK && (relay_bind_test_fill_timeout_default_ms != 0 ||
                           relay_bind_test_fill_timeout_max_ms != 0)) {
        brc = moqr_bind_set_fill_timeouts(r->bind,
                                          relay_bind_test_fill_timeout_default_ms,
                                          relay_bind_test_fill_timeout_max_ms);
        if (brc != MOQR_OK) {
            moqr_bind_destroy(r->bind);
            r->bind = NULL;
        }
    }
    if (brc != MOQR_OK) {
        moqr_core_destroy(r->core);
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    return MOQR_OK;
}

moqr_result_t
relay_bind_test_rig_create(rig_t *r, ca_t *a)
{
    return relay_bind_test_rig_create_ex(r, a, 0, 0, 0, 0);
}

void
relay_bind_test_rig_destroy(rig_t *r)
{
    moqr_bind_destroy(r->bind);
    moqr_core_destroy(r->core);
    for (int i = 0; i < MAX_CONNS; i++) {
        if (r->conns[i].used) {
            moq_simpair_destroy(r->conns[i].sp);
            r->conns[i].used = false;
        }
    }
    moqr_trace_destroy(r->trace);
}

bool
relay_bind_test_sub_reason_eq(const peer_state_t *ps, const char *s)
{
    size_t len = strlen(s);
    return !ps->sub_error_reason_truncated &&
           ps->sub_error_reason_len == len &&
           (len == 0 || memcmp(ps->sub_error_reason, s, len) == 0);
}

void
relay_bind_test_peer_drain(rig_t *r, conn_t *cn, peer_state_t *ps, bool auto_accept,
                           bool accept_with_largest, uint64_t lg, uint64_t lo)
{
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                ps->up_subs++;
                ps->up_sub = ev->u.subscribe_request.sub; /* even if not accepted */
                if (auto_accept) {
                    moq_accept_subscribe_cfg_t cfg;
                    moq_accept_subscribe_cfg_init(&cfg);
                    cfg.has_largest = accept_with_largest;
                    cfg.largest_group = lg;
                    cfg.largest_object = lo;
                    cfg.track_properties = relay_bind_test_accept_track_properties;
                    if (relay_bind_test_accept_track_alias != 0) {
                        cfg.has_track_alias = true;
                        cfg.track_alias = relay_bind_test_accept_track_alias;
                    }
                    R_CHECK(r, moq_session_accept_subscribe(
                                   cn->peer, ev->u.subscribe_request.sub,
                                   &cfg, r->now) == MOQ_OK);
                    ps->up_seen = true;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                ps->subscribe_errors++;
                ps->sub_error_code = ev->u.subscribe_error.error_code;
                ps->sub_error_can_retry = ev->u.subscribe_error.can_retry;
                ps->sub_error_retry_ms = ev->u.subscribe_error.retry_after_ms;
                ps->sub_error_reason_len = ev->u.subscribe_error.reason.len;
                if (ps->sub_error_reason_len > sizeof(ps->sub_error_reason)) {
                    ps->sub_error_reason_len = sizeof(ps->sub_error_reason);
                    ps->sub_error_reason_truncated = true;
                } else {
                    ps->sub_error_reason_truncated = false;
                }
                if (ps->sub_error_reason_len != 0) {
                    memcpy(ps->sub_error_reason,
                           ev->u.subscribe_error.reason.data,
                           ps->sub_error_reason_len);
                }
                break;
            case MOQ_EVENT_PUBLISH_ERROR:
                ps->publish_error = true;
                ps->publish_errors++;
                ps->publish_error_code = ev->u.publish_error.error_code;
                ps->publish_error_can_retry = ev->u.publish_error.can_retry;
                ps->publish_error_retry_ms =
                    ev->u.publish_error.retry_after_ms;
                break;
            case MOQ_EVENT_FETCH_RESET:
                ps->fetch_resets++;
                ps->fetch_reset_code = ev->u.fetch_reset.error_code;
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                ps->session_closed = true;
                ps->close_code = ev->u.closed.code;
                break;
            case MOQ_EVENT_NAMESPACE_REJECTED:
                ps->ns_rejected++;
                ps->ns_rejected_code = ev->u.namespace_rejected.error_code;
                ps->ns_rejected_can_retry = ev->u.namespace_rejected.can_retry;
                ps->ns_rejected_retry_ms =
                    ev->u.namespace_rejected.retry_after_ms;
                break;
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                ps->namespace_accepted = true;
                ps->namespace_accepts++;
                break;
            case MOQ_EVENT_NS_SUB_ERROR:
                ps->ns_sub_error++;
                ps->ns_sub_error_code = ev->u.ns_sub_error.error_code;
                break;
            case MOQ_EVENT_FETCH_ERROR:
                ps->fetch_error++;
                ps->fetch_error_code = ev->u.fetch_error.error_code;
                ps->fetch_error_can_retry = ev->u.fetch_error.can_retry;
                ps->fetch_error_retry_ms = ev->u.fetch_error.retry_after_ms;
                break;
            case MOQ_EVENT_FETCH_OK:
                ps->fetch_ok = true;
                ps->fok_eot = ev->u.fetch_ok.end_of_track;
                ps->fok_end_group = ev->u.fetch_ok.end_group;
                ps->fok_end_object = ev->u.fetch_ok.end_object;
                break;
            case MOQ_EVENT_FETCH_COMPLETE:
                ps->fetch_complete++;
                break;
            case MOQ_EVENT_FETCH_GAP:
                if (ps->fgap_n <
                    (int)(sizeof(ps->fgap) / sizeof(ps->fgap[0]))) {
                    int i = ps->fgap_n++;
                    ps->fgap[i].kind = ev->u.fetch_gap.range_kind;
                    ps->fgap[i].group = ev->u.fetch_gap.group_id;
                    ps->fgap[i].object = ev->u.fetch_gap.object_id;
                }
                break;
            case MOQ_EVENT_FETCH_OBJECT:
                if (ps->fdlv_n <
                    (int)(sizeof(ps->fdlv) / sizeof(ps->fdlv[0]))) {
                    int i = ps->fdlv_n++;
                    ps->fdlv[i].group = ev->u.fetch_object.group_id;
                    ps->fdlv[i].subgroup = ev->u.fetch_object.subgroup_id;
                    ps->fdlv[i].object = ev->u.fetch_object.object_id;
                    ps->fdlv[i].datagram = ev->u.fetch_object.datagram;
                    const moq_rcbuf_t *fpl = ev->u.fetch_object.payload;
                    uint64_t plen = (uint64_t)moq_rcbuf_len(fpl);
                    uint32_t h = 2166136261u;   /* FNV-1a, order-sensitive */
                    if (fpl != NULL) {
                        const uint8_t *pd = moq_rcbuf_data(fpl);
                        for (uint64_t k = 0; k < plen; k++) {
                            h = (h ^ pd[k]) * 16777619u;
                        }
                    }
                    ps->fdlv[i].fp = fpl != NULL ? moq_rcbuf_data(fpl)[0] : 0u;
                    ps->fdlv[i].len = plen;
                    ps->fdlv[i].fp_all = h;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR:
                ps->tracks_error++;
                ps->tracks_error_code =
                    ev->u.subscribe_tracks_error.error_code;
                break;
            case MOQ_EVENT_NS_SUB_OK:
                ps->ns_sub_ok = true;
                ps->ns_sub_oks++;
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                ps->subscribe_ok = true;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'S';
                }
                ps->has_largest = ev->u.subscribe_ok.has_largest;
                ps->largest_group = ev->u.subscribe_ok.largest_group;
                ps->largest_object = ev->u.subscribe_ok.largest_object;
                break;
            case MOQ_EVENT_SUBGROUP_RESET:
                if (ps->subgroup_resets <
                    (int)(sizeof(ps->reset_gsg) / sizeof(ps->reset_gsg[0]))) {
                    ps->reset_gsg[ps->subgroup_resets][0] =
                        ev->u.subgroup_reset.group_id;
                    ps->reset_gsg[ps->subgroup_resets][1] =
                        ev->u.subgroup_reset.subgroup_id;
                    ps->reset_codes[ps->subgroup_resets] =
                        ev->u.subgroup_reset.error_code;
                }
                if (ps->done_seen) {
                    ps->resets_after_done++;
                }
                ps->subgroup_resets++;
                break;
            case MOQ_EVENT_SUBGROUP_FINISHED:
                ps->subgroup_finished++;
                if (ps->finished_count <
                    (int)(sizeof(ps->finished_gsg) /
                          sizeof(ps->finished_gsg[0]))) {
                    ps->finished_gsg[ps->finished_count][0] =
                        ev->u.subgroup_finished.group_id;
                    ps->finished_gsg[ps->finished_count][1] =
                        ev->u.subgroup_finished.subgroup_id;
                    ps->finished_count++;
                }
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                ps->objects++;
                for (int fi = 0; fi < ps->finished_count; fi++) {
                    if (ps->finished_gsg[fi][0] ==
                            ev->u.object_received.group_id &&
                        ps->finished_gsg[fi][1] ==
                            ev->u.object_received.subgroup_id) {
                        ps->reopen_after_finish = true;
                    }
                }
                ps->last_group = ev->u.object_received.group_id;
                ps->last_object = ev->u.object_received.object_id;
                ps->last_datagram = ev->u.object_received.datagram;
                ps->last_status = ev->u.object_received.status;
                ps->last_payload_null =
                    ev->u.object_received.payload == NULL;
                ps->last_len = 0;
                if (ev->u.object_received.payload != NULL) {
                    ps->last_byte = moq_rcbuf_data(
                        ev->u.object_received.payload)[0];
                    ps->last_len = moq_rcbuf_len(
                        ev->u.object_received.payload);
                }
                ps->last_props_len = 0;
                if (ev->u.object_received.properties != NULL) {
                    ps->last_props_len = moq_rcbuf_len(
                        ev->u.object_received.properties);
                    size_t keep = ps->last_props_len < sizeof(ps->last_props)
                                      ? ps->last_props_len
                                      : sizeof(ps->last_props);
                    memcpy(ps->last_props,
                           moq_rcbuf_data(ev->u.object_received.properties),
                           keep);
                }
                if (ps->dlv_n < DLV_LOG_MAX) {
                    dlv_rec_t *dr = &ps->dlv[ps->dlv_n++];
                    dr->sub = ev->u.object_received.sub._opaque;
                    dr->group = ev->u.object_received.group_id;
                    dr->subgroup = ev->u.object_received.subgroup_id;
                    dr->object_id = ev->u.object_received.object_id;
                    dr->payload_null =
                        ev->u.object_received.payload == NULL;
                    dr->fp = dr->payload_null
                                 ? 0u
                                 : moq_rcbuf_data(
                                       ev->u.object_received.payload)[0];
                    dr->status = (uint8_t)ev->u.object_received.status;
                    dr->eog = ev->u.object_received.end_of_group;
                    dr->first = ev->u.object_received.first_object;
                    dr->datagram = ev->u.object_received.datagram;
                    dr->after_done = ps->done_seen;
                } else {
                    ps->dlv_overflow++;
                }
                break;
            case MOQ_EVENT_OBJECT_CHUNK: {
                /* Streaming-receive subscriber: count data chunks, and
                 * classify the object's terminal — NORMAL completes it, RESET is
                 * the wire-visible abandon the relay propagates downstream. */
                const moq_object_chunk_event_t *oc = &ev->u.object_chunk;
                if (oc->chunk != NULL && moq_rcbuf_len(oc->chunk) > 0) {
                    ps->chunks++;
                    ps->last_chunk_byte = moq_rcbuf_data(oc->chunk)[0];
                }
                if (oc->end) {
                    if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                        ps->obj_complete = true;
                        ps->objects++;
                    } else if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                        ps->saw_reset = true;
                        ps->last_reset_code = oc->error_code;
                    }
                }
                break;
            }
            case MOQ_EVENT_UNSUBSCRIBED:
                ps->unsubscribed++;
                break;
            case MOQ_EVENT_NAMESPACE_GONE:
                ps->ns_gone++;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'G';
                }
                break;
            case MOQ_EVENT_PUBLISH_OK:
                ps->publish_ok = true;
                break;
            case MOQ_EVENT_NAMESPACE_FOUND:
                ps->ns_found++;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'N';
                }
                {
                    const moq_namespace_t *sfx =
                        &ev->u.namespace_found.track_namespace_suffix;
                    if (sfx->count > 0 && sfx->parts[0].len > 0 &&
                        ps->ns_id_len < (int)sizeof(ps->ns_ids) - 1) {
                        ps->ns_ids[ps->ns_id_len++] =
                            (char)sfx->parts[0].data[0];
                    }
                    ps->ns_seen_count = sfx->count;
                    ps->ns_seen_total = 0;
                    for (size_t pi = 0; pi < sfx->count && pi < 8; pi++) {
                        ps->ns_seen_lens[pi] = sfx->parts[pi].len;
                        for (size_t bi = 0; bi < sfx->parts[pi].len &&
                                            ps->ns_seen_total < 64; bi++) {
                            ps->ns_seen_bytes[ps->ns_seen_total++] =
                                sfx->parts[pi].data[bi];
                        }
                    }
                }
                break;
            case MOQ_EVENT_TRACK_STATUS_OK:
                ps->ts_ok = true;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'T';
                }
                ps->ts_largest_group =
                    ev->u.track_status_ok.largest_group;
                break;
            case MOQ_EVENT_TRACK_STATUS_ERROR:
                ps->ts_error++;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'E';
                }
                ps->ts_error_code = ev->u.track_status_error.error_code;
                break;
            case MOQ_EVENT_SUBSCRIPTION_UPDATE_OK:
                ps->update_oks++;
                ps->uok_has_largest = ev->u.subscription_update_ok.has_largest;
                ps->uok_largest_group =
                    ev->u.subscription_update_ok.largest_group;
                ps->uok_largest_object =
                    ev->u.subscription_update_ok.largest_object;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                ps->done_seen = true;
                if (ps->order_len < (int)sizeof(ps->order) - 1) {
                    ps->order[ps->order_len++] = 'D';
                }
                ps->done_count++;
                ps->done_code = ev->u.subscribe_done.status_code;
                break;
            case MOQ_EVENT_FILL_OBJECT: {
                uint64_t g = ev->u.fill_object.group_id;
                if (ps->fill_objects == 0 || g < ps->fill_min_group) {
                    ps->fill_min_group = g;
                }
                if (ps->fill_objects == 0 || g > ps->fill_max_group) {
                    ps->fill_max_group = g;
                }
                ps->fill_objects++;
                break;
            }
            case MOQ_EVENT_FILL_GAP:
                if (ps->fill_gaps < 8) {
                    ps->fill_gap_log[ps->fill_gaps].kind = ev->u.fill_gap.kind;
                    ps->fill_gap_log[ps->fill_gaps].group =
                        ev->u.fill_gap.group_id;
                    ps->fill_gap_log[ps->fill_gaps].object =
                        ev->u.fill_gap.object_id;
                }
                ps->fill_gaps++;
                ps->fill_gap_kind = ev->u.fill_gap.kind;
                ps->fill_gap_group = ev->u.fill_gap.group_id;
                ps->fill_gap_object = ev->u.fill_gap.object_id;
                break;
            case MOQ_EVENT_FILL_TERMINAL:
                ps->fill_terminals++;
                ps->fill_term_reason = ev->u.fill_terminal.reason;
                ps->fill_term_domain = ev->u.fill_terminal.error_domain;
                ps->fill_term_code = ev->u.fill_terminal.error_code;
                break;
            case MOQ_EVENT_NAMESPACE_CANCELLED:
                ps->ns_cancelled++;
                ps->ns_cancelled_code = ev->u.namespace_cancelled.error_code;
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* -- the parity test ------------------------------------------------------------ */

int
relay_bind_test_parity_flow(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: rig create (version %d)\n", (int)version);
        return 1;
    }
    int failures = 0;

    conn_t *pub = relay_bind_test_rig_connect(&rig, version);
    conn_t *sub1 = relay_bind_test_rig_connect(&rig, version);
    conn_t *sub2 = relay_bind_test_rig_connect(&rig, version);
    conn_t *watch = relay_bind_test_rig_connect(&rig, version);
    R_CHECK(&rig, pub && sub1 && sub2 && watch);
    peer_state_t pub_ps, s1_ps, s2_ps, w_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s1_ps, 0, sizeof(s1_ps));
    memset(&s2_ps, 0, sizeof(s2_ps));
    memset(&w_ps, 0, sizeof(w_ps));
    relay_bind_test_rig_pump(&rig, 4); /* handshakes */

    /* Watcher subscribes to the namespace prefix BEFORE the announce. */
    moq_bytes_t pfx[1] = { B("live") };
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = 1 };
    /* Portable across drafts: draft-18's SUBSCRIBE_NAMESPACE carries no
     * interest field and the profile requires NAMESPACE_STATE exactly;
     * draft-16 accepts it too. */
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    R_CHECK(&rig, moq_session_subscribe_namespace(watch->peer, &nscfg,
                                                  rig.now, &nsh) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    /* Publisher announces. */
    moq_bytes_t nsp[2] = { B("live"), B("cam1") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ns_found == 1);   /* discovery through real wire */

    /* Two subscribers subscribe before the publisher answers: coalescing
     * must produce exactly ONE upstream SUBSCRIBE at the publisher. */
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t s1h, s2h;
    R_CHECK(&rig, moq_session_subscribe(sub1->peer, &scfg, rig.now,
                                        &s1h) == MOQ_OK);
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &scfg, rig.now,
                                        &s2h) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, true, 0, 2); /* accept, largest {0,2} */
    R_CHECK(&rig, pub_ps.up_seen);
    R_CHECK(&rig, pub_ps.up_subs == 1);   /* coalesced: EXACTLY one */
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.subscribe_ok && s2_ps.subscribe_ok);
    R_CHECK(&rig, s1_ps.has_largest && s1_ps.largest_group == 0 &&
                      s1_ps.largest_object == 2);

    /* Publisher writes objects on the relay's upstream subscription. */
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 1;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    R_CHECK(&rig, moq_session_open_subgroup(pub->peer, pub_ps.up_sub, &sgc,
                                            rig.now, &sgh) == MOQ_OK);
    for (uint64_t o = 0; o < 3; o++) {
        uint8_t body[64];
        memset(body, (uint8_t)(0xA0 + o), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(pub->peer, sgh, o, pl,
                                               rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(&rig, moq_session_close_subgroup(pub->peer, sgh, rig.now) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    relay_bind_test_peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.objects == 3 && s2_ps.objects == 3);
    R_CHECK(&rig, s1_ps.last_byte == 0xA2);   /* exact bytes end-to-end */

    /* Forward-latency is observed on each successful downstream write: the
     * histogram count tracks deliveries_written exactly (every DELIVERED,
     * none on WOULD_BLOCK/STREAM_ERROR) and is nonzero once objects flow. */
    {
        moqr_bind_stats_t bstats;
        moqr_bind_get_stats(rig.bind, &bstats);
        R_CHECK(&rig, bstats.forward_latency.count > 0);
        R_CHECK(&rig, bstats.forward_latency.count ==
                          bstats.deliveries_written);
    }

    /* Datagram-preference object WITH properties relays intact. */
    {
        uint8_t body[32];
        /* One valid property entry, identical wire form in both drafts:
         * type 2 (even => varint value), value 9. Single-byte varints
         * 0-127 encode literally (vi64.h length table; kvp.h even-type
         * rule), so the block is exactly {0x02, 0x09}. */
        uint8_t props[2] = { 0x02, 0x09 };
        memset(body, 0xD7, sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) ==
                          MOQ_OK);
        R_CHECK(&rig, moq_session_send_object_datagram(
                          pub->peer, pub_ps.up_sub, 1, 10, 90, false, pl,
                          props, sizeof(props), rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    relay_bind_test_rig_pump(&rig, 6);
    memset(&s1_ps, 0, sizeof(s1_ps));
    relay_bind_test_peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.objects == 1);
    R_CHECK(&rig, s1_ps.last_datagram);
    R_CHECK(&rig, s1_ps.last_byte == 0xD7);
    {
        const uint8_t want_props[2] = { 0x02, 0x09 };
        R_CHECK(&rig, s1_ps.last_props_len == sizeof(want_props) &&
                          memcmp(s1_ps.last_props, want_props,
                                 sizeof(want_props)) == 0);
    }

    /* Status-only datagram relays as a status datagram (payload NULL). */
    R_CHECK(&rig, moq_session_send_status_datagram(
                      pub->peer, pub_ps.up_sub, 1, 11, 90,
                      MOQ_OBJECT_END_OF_GROUP, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    memset(&s1_ps, 0, sizeof(s1_ps));
    relay_bind_test_peer_drain(&rig, sub1, &s1_ps, false, false, 0, 0);
    R_CHECK(&rig, s1_ps.objects == 1);
    R_CHECK(&rig, s1_ps.last_datagram);
    R_CHECK(&rig, s1_ps.last_payload_null);
    R_CHECK(&rig, s1_ps.last_status == MOQ_OBJECT_END_OF_GROUP);

    /* Track status answered from live relay state. */
    moq_track_status_cfg_t tsc;
    memset(&tsc, 0, sizeof(tsc));
    moq_track_status_cfg_init(&tsc);
    tsc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    tsc.track_name = B("video");
    moq_track_status_handle_t tsh;
    R_CHECK(&rig, moq_session_track_status(watch->peer, &tsc, rig.now,
                                           &tsh) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ts_ok && w_ps.ts_largest_group == 1);

    /* TRUE warm rejoin: every subscriber leaves, linger expires, the
     * relay unsubscribes upstream (the publisher observes it on the
     * wire), and a rejoining subscriber is served the retained group
     * while a fresh upstream SUBSCRIBE goes out. */
    R_CHECK(&rig, moq_session_unsubscribe(sub2->peer, s2h, rig.now) ==
                      MOQ_OK);
    R_CHECK(&rig, moq_session_unsubscribe(sub1->peer, s1h, rig.now) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6); /* > linger_us (500) at 1000us per cycle */
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.unsubscribed == 1);   /* relay left upstream */
    R_CHECK(&rig, pub_ps.up_subs == 1);   /* and did NOT re-subscribe yet */

    moq_subscription_t s2h2;
    R_CHECK(&rig, moq_session_subscribe(sub2->peer, &scfg, rig.now,
                                        &s2h2) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, true, 1, 12); /* re-subscribe */
    R_CHECK(&rig, pub_ps.up_subs == 2);   /* exactly one, from the rejoin */
    relay_bind_test_rig_pump(&rig, 8);
    memset(&s2_ps, 0, sizeof(s2_ps));
    relay_bind_test_peer_drain(&rig, sub2, &s2_ps, false, false, 0, 0);
    R_CHECK(&rig, s2_ps.subscribe_ok);
    R_CHECK(&rig, s2_ps.objects >= 3);   /* retained group replayed */

    /* Publish push ingest (both drafts carry PUBLISH). */
    moq_publish_cfg_t pubc;
    memset(&pubc, 0, sizeof(pubc));
    moq_publish_cfg_init(&pubc);
    moq_bytes_t nsp2[2] = { B("live"), B("cam2") };
    pubc.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    pubc.track_name = B("push");
    moq_publication_t pubh;
    moq_result_t prc = moq_session_publish(pub->peer, &pubc, rig.now,
                                           &pubh);
    R_CHECK(&rig, prc == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok);   /* accepted on the wire */

    /* Session close retires the publisher: announce withdrawn (watcher
     * sees NAMESPACE_GONE via real wire) and the track goes warm. */
    (void)moq_session_on_transport_close(pub->rsess, 0, rig.now);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, watch, &w_ps, false, false, 0, 0);
    R_CHECK(&rig, w_ps.ns_gone == 1);   /* withdrawal seen on the wire */

    int f = rig.failures + failures;
    relay_bind_test_rig_destroy(&rig);
    if (a.live != 0) {
        printf("FAIL: allocator leak (version %d): %ld bytes\n",
               (int)version, a.live);
        f++;
    }
    if (f == 0) {
        printf("PASS: parity_flow(draft-%d)\n", (int)version);
    }
    return f;
}

/* Establish publisher metadata through the same PUBLISH ingress as production.
 * The auth fixtures defer other actions, so this setup must be accepted before
 * their measured request begins. */
bool
relay_bind_test_rig_publish(rig_t *r, moq_version_t version, moqr_ns_t ns, moq_bytes_t name)
{
    conn_t *pub = relay_bind_test_rig_connect(r, version);
    R_CHECK(r, pub != NULL);
    if (pub == NULL) {
        return false;
    }
    relay_bind_test_rig_pump(r, 4);
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ .parts = ns.parts,
                                            .count = ns.count };
    cfg.track_name = name;
    moq_publication_t ph;
    moq_result_t rc = moq_session_publish(pub->peer, &cfg, r->now, &ph);
    R_CHECK(r, rc == MOQ_OK);
    if (rc != MOQ_OK) {
        return false;
    }
    relay_bind_test_rig_pump(r, 6);
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(r, pub, &ps, false, false, 0, 0);
    bool accepted = ps.publish_ok && !ps.publish_error && !ps.session_closed;
    R_CHECK(r, accepted);
    return accepted;
}

/* -- deferred (DEFER) auth: park + resolve end-to-end --------------------- */

/* DEFER one target action with a fresh recorded ticket; allow everything else
 * (so setup completes). */
moqr_auth_action_t relay_bind_test_defer_action = 0xFFFFFFFFu;
static uint64_t           g_defer_seq = 7000;
uint64_t           relay_bind_test_defer_last = 0;
uint64_t           relay_bind_test_defer_fixed = 0; /* nonzero => reuse this ticket */

void
relay_bind_test_defer_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    (void)ctx;
    if (req->action == relay_bind_test_defer_action) {
        out->decision = MOQR_AUTH_DEFER;
        out->ticket = relay_bind_test_defer_fixed != 0 ? relay_bind_test_defer_fixed : ++g_defer_seq;
        relay_bind_test_defer_last = out->ticket;
    } else {
        out->decision = MOQR_AUTH_ALLOW;
        out->reason = MOQR_AUTH_REASON_OK;
    }
}

/* Park a DEFERred request of `action`, prove it did not reject or mutate core
 * state, then resolve with `decision`: ALLOW resumes the op (core state now
 * exists / peer answered), DENY rejects it carrying a custom wire code. Covers
 * all five request paths through the real binding. */
int
relay_bind_test_run_defer_case(moqr_auth_action_t action, moqr_auth_decision_t decision,
                               const char *name)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_defer_action = action;
    relay_bind_test_defer_fixed = 0;
    relay_bind_test_authorize = relay_bind_test_defer_hook;
    relay_bind_test_authorize_ctx = NULL;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_authorize = NULL;
        relay_bind_test_defer_action = 0xFFFFFFFFu;
        printf("FAIL: %s rig create\n", name);
        return 1;
    }
    conn_t *cn = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_18);
    R_CHECK(&rig, cn != NULL);
    if (cn == NULL) {
        goto cleanup;
    }
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("d"), B("x") };
    moq_namespace_t wns = { .parts = nsp, .count = 2 };
    /* SUBSCRIBE / TRACK_STATUS need a live track for an ALLOW to answer. */
    if (action == MOQR_AUTH_SUBSCRIBE || action == MOQR_AUTH_TRACK_STATUS) {
        if (!relay_bind_test_rig_publish(&rig, MOQ_VERSION_DRAFT_18,
                                         (moqr_ns_t){ nsp, 2 }, B("v"))) {
            goto cleanup;
        }
        relay_bind_test_rig_pump(&rig, 2);
    }
    moqr_core_stats_t st0;
    moqr_core_get_stats(rig.core, &st0);

    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        moq_publish_namespace_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_publish_namespace_cfg_init(&c);
        c.track_namespace = wns;
        moq_announcement_t h;
        R_CHECK(&rig, moq_session_publish_namespace(cn->peer, &c, rig.now,
                                                    &h) == MOQ_OK);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moq_subscribe_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_subscribe_cfg_init(&c);
        c.track_namespace = wns;
        c.track_name = B("v");
        c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        R_CHECK(&rig, moq_session_subscribe(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: {
        moq_bytes_t pfx[1] = { B("d") };
        moq_subscribe_namespace_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_subscribe_namespace_cfg_init(&c);
        c.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
        c.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        R_CHECK(&rig, moq_session_subscribe_namespace(cn->peer, &c, rig.now,
                                                      &h) == MOQ_OK);
        break;
    }
    case MOQR_AUTH_TRACK_STATUS: {
        moq_track_status_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_track_status_cfg_init(&c);
        c.track_namespace = wns;
        c.track_name = B("v");
        moq_track_status_handle_t h;
        R_CHECK(&rig, moq_session_track_status(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    case MOQR_AUTH_PUBLISH: {
        moq_publish_cfg_t c;
        memset(&c, 0, sizeof(c));
        moq_publish_cfg_init(&c);
        c.track_namespace = wns;
        c.track_name = B("v");
        moq_publication_t h;
        R_CHECK(&rig, moq_session_publish(cn->peer, &c, rig.now, &h) ==
                          MOQ_OK);
        break;
    }
    default:
        R_CHECK(&rig, false);
        break;
    }
    relay_bind_test_defer_last = 0;
    relay_bind_test_rig_pump(&rig, 6);
    uint64_t tkt = relay_bind_test_defer_last;
    R_CHECK(&rig, tkt != 0);

    /* Parked: nothing rejected, no persistent core state created. */
    peer_state_t ps;
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, cn, &ps, false, false, 0, 0);
    R_CHECK(&rig, ps.ns_rejected == 0 && ps.subscribe_errors == 0 &&
                      ps.ns_sub_error == 0 && ps.ts_error == 0 &&
                      !ps.publish_error);
    moqr_core_stats_t stp;
    moqr_core_get_stats(rig.core, &stp);
    R_CHECK(&rig, stp.ns_nodes == st0.ns_nodes && stp.subs == st0.subs &&
                      stp.ns_subs == st0.ns_subs && stp.tracks == st0.tracks);

    /* Resolve. */
    uint64_t custom = decision == MOQR_AUTH_DENY ? 0x20u : 0u;
    R_CHECK(&rig, moqr_bind_auth_resolve(
                      rig.bind, tkt,
                      &(moqr_auth_verdict_t){ .decision = decision,
                                              .error_code = custom },
                      rig.now) == MOQR_OK);
    relay_bind_test_rig_pump(&rig, 8);
    memset(&ps, 0, sizeof(ps));
    relay_bind_test_peer_drain(&rig, cn, &ps, false, false, 0, 0);
    moqr_core_stats_t st1;
    moqr_core_get_stats(rig.core, &st1);

    if (decision == MOQR_AUTH_ALLOW) {
        switch (action) {
        case MOQR_AUTH_PUBLISH_NAMESPACE:
            R_CHECK(&rig, st1.ns_nodes > st0.ns_nodes); /* core mutated */
            R_CHECK(&rig, ps.namespace_accepted);       /* AND wire-accepted */
            break;
        case MOQR_AUTH_SUBSCRIBE:
            R_CHECK(&rig, st1.subs > st0.subs);
            break;
        case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
            R_CHECK(&rig, st1.ns_subs > st0.ns_subs);
            R_CHECK(&rig, ps.ns_sub_ok);
            break;
        case MOQR_AUTH_TRACK_STATUS:
            R_CHECK(&rig, ps.ts_ok);
            break;
        case MOQR_AUTH_PUBLISH:
            R_CHECK(&rig, st1.tracks > st0.tracks);
            R_CHECK(&rig, ps.publish_ok);
            break;
        default:
            break;
        }
    } else {
        switch (action) {
        case MOQR_AUTH_PUBLISH_NAMESPACE:
            R_CHECK(&rig, ps.ns_rejected == 1 && ps.ns_rejected_code == 0x20);
            break;
        case MOQR_AUTH_SUBSCRIBE:
            R_CHECK(&rig, ps.subscribe_errors == 1 && ps.sub_error_code == 0x20);
            break;
        case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
            R_CHECK(&rig, ps.ns_sub_error == 1 && ps.ns_sub_error_code == 0x20);
            break;
        case MOQR_AUTH_TRACK_STATUS:
            R_CHECK(&rig, ps.ts_error == 1 && ps.ts_error_code == 0x20);
            break;
        case MOQR_AUTH_PUBLISH:
            R_CHECK(&rig, ps.publish_error && ps.publish_error_code == 0x20);
            break;
        default:
            break;
        }
    }

cleanup:
    relay_bind_test_rig_destroy(&rig);
    relay_bind_test_authorize = NULL;
    relay_bind_test_authorize_ctx = NULL;
    relay_bind_test_defer_action = 0xFFFFFFFFu;
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: auth_defer %s\n", name);
    }
    return f;
}

/* -- CAT moqt-reval lease revalidation (grant lifecycle in the binding) ----- */

/* A controllable revalidation hook. Setup and every non-grantable action always
 * ALLOW with no lease; SUBSCRIBE / PUBLISH_NAMESPACE return the current decision
 * with a per-action lease, so a test creates a grant (ALLOW + lease) and then
 * flips the decision to observe the binding revoking it on the next due tick. */
moqr_auth_decision_t relay_bind_test_rv_decision = MOQR_AUTH_ALLOW;
uint64_t             relay_bind_test_rv_lease_sub = 0;
uint64_t             relay_bind_test_rv_lease_ann = 0;
uint32_t             relay_bind_test_rv_deny_code = 0; /* 0 => no custom DENY code   */

/* See the PUBLISH_NAMESPACE branch below. */
bool relay_bind_test_rv_ann_always_allow = false;
/* NULL = the decision is global (relay_bind_test_rv_decision). When set, ONLY the
 * subscription whose track name matches is revoked, so independent
 * SUBSCRIBE_DONE terminals can be produced one at a time. */
const char *relay_bind_test_rv_deny_track = NULL;
/* true = relay_bind_test_reval_make_subscribe_grant also subscribes a second track, so two
 * independent SUBSCRIBE_DONE terminals are available. */
bool relay_bind_test_rv_second_track = false;

void
relay_bind_test_rv_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    (void)ctx;
    if (req->action == MOQR_AUTH_SUBSCRIBE) {
        if (relay_bind_test_rv_deny_track != NULL) {
            size_t tl = strlen(relay_bind_test_rv_deny_track);
            bool hit = req->name.len == tl && req->name.data != NULL &&
                       memcmp(req->name.data, relay_bind_test_rv_deny_track, tl) == 0;
            out->decision = hit ? MOQR_AUTH_DENY : MOQR_AUTH_ALLOW;
        } else {
            out->decision = relay_bind_test_rv_decision;
        }
        out->revalidate_after_us = relay_bind_test_rv_lease_sub;
    } else if (req->action == MOQR_AUTH_PUBLISH_NAMESPACE) {
        /* Opt-in: keep announces allowed while a subscribe revocation is being
         * driven, so a test can revoke one subscription and still publish a
         * new namespace. Default false leaves every other test unchanged. */
        out->decision = relay_bind_test_rv_ann_always_allow ? MOQR_AUTH_ALLOW : relay_bind_test_rv_decision;
        out->revalidate_after_us = relay_bind_test_rv_lease_ann;
    } else {
        out->decision = MOQR_AUTH_ALLOW; /* setup + non-grantable actions */
        return;
    }
    if (out->decision == MOQR_AUTH_DENY && relay_bind_test_rv_deny_code != 0) {
        out->error_code = relay_bind_test_rv_deny_code;
        /* A revoked ACTIVE subscribe grant ends a subscription: that terminal
         * is PUBLISH_DONE-domain and must be stated as a tagged descriptor.
         * The scalar beside it stays REQUEST_ERROR-domain and is never read as
         * a status. */
        {
            moqr_pd_desc_t t;
            if (moqr_pd_desc_extension(relay_bind_test_rv_deny_code, &t) ==
                MOQR_OK) {
                out->revoke_terminal = t;
            }
        }
    }
}

void
relay_bind_test_rv_reset(void)
{
    relay_bind_test_authorize = NULL;
    relay_bind_test_authorize_ctx = NULL;
    relay_bind_test_grant_bytes = 0;
    relay_bind_test_max_actions = 0;
    relay_bind_test_rv_decision = MOQR_AUTH_ALLOW;
    relay_bind_test_rv_lease_sub = 0;
    relay_bind_test_rv_lease_ann = 0;
    relay_bind_test_rv_deny_code = 0;
    relay_bind_test_rv_ann_always_allow = false;
    relay_bind_test_rv_deny_track = NULL;
    relay_bind_test_rv_second_track = false;
}

/* Announce (no grant when lease_ann==0) + subscribe (ALLOW + lease -> grant)
 * over real sessions; leaves the subscription accepted with a live grant. */
void
relay_bind_test_reval_make_subscribe_grant(rig_t *rig, conn_t *pub, conn_t *sub,
                                           peer_state_t *pub_ps, peer_state_t *sub_ps,
                                           moq_bytes_t *nsp2)
{
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(rig, moq_session_publish_namespace(pub->peer, &pcfg, rig->now,
                                               &ann) == MOQ_OK);
    relay_bind_test_rig_pump(rig, 4);
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp2, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(rig, moq_session_subscribe(sub->peer, &scfg, rig->now, &sh) ==
                     MOQ_OK);
    if (relay_bind_test_rv_second_track) {
        relay_bind_test_rig_pump(rig, 4); /* the first subscribe must clear the queue */
        moq_subscribe_cfg_t scfg2 = scfg;
        scfg2.track_name = B("audio");
        moq_subscription_t sh2;
        R_CHECK(rig, moq_session_subscribe(sub->peer, &scfg2, rig->now,
                                           &sh2) == MOQ_OK);
    }
    relay_bind_test_rig_pump(rig, 4);
    relay_bind_test_peer_drain(rig, pub, pub_ps, true, false, 0, 0); /* accept upstream */
    relay_bind_test_rig_pump(rig, 4);
    relay_bind_test_peer_drain(rig, sub, sub_ps, false, false, 0, 0);
    R_CHECK(rig, sub_ps->subscribe_ok); /* accepted: grant is live */
}

/* -- Downstream subgroup-slot reclamation ----------------------------------- */

/* Publish one object on a FRESH subgroup stream (group, subgroup), then close
 * the stream with a PLAIN FIN — no END_OF_GROUP. This is the legal sequence the
 * relay cannot currently reclaim: the object carries end_of_group=false, so the
 * downstream subgroup slot stays open forever. */
void
relay_bind_test_pub_subgroup_plain_fin(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                           uint64_t group, uint64_t subgroup, uint64_t object_id)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                         &sgh) == MOQ_OK);
    uint8_t body[4] = { (uint8_t)group, (uint8_t)subgroup, 0xC3, 0x3C };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                   MOQ_OK);
    R_CHECK(r, moq_session_write_object(pub->peer, sgh, object_id, pl, r->now) ==
                   MOQ_OK);
    moq_rcbuf_decref(pl);
    /* Plain FIN: closes the stream, but the last object was NOT END_OF_GROUP. */
    R_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
}

/* Publish a subgroup carrying `n` normal objects, optionally a terminal EOG
 * status object, then close (plain FIN). Exercises: subgroup_end must fire only
 * on the LAST record (a multi-object subgroup must deliver every object before
 * the downstream stream closes), and the EOG path still reclaims. */
void
relay_bind_test_pub_subgroup_n(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                   uint64_t group, uint64_t subgroup, uint64_t n, bool eog)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    R_CHECK(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                         &sgh) == MOQ_OK);
    for (uint64_t o = 0; o < n; o++) {
        uint8_t body[4] = { (uint8_t)group, (uint8_t)o, 0xC3, 0x3C };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        R_CHECK(r, moq_session_write_object(pub->peer, sgh, o, pl, r->now) ==
                       MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    if (eog) {
        R_CHECK(r, moq_session_write_status_object(
                       pub->peer, sgh, n, MOQ_OBJECT_END_OF_GROUP, r->now) ==
                       MOQ_OK);
    }
    R_CHECK(r, moq_session_close_subgroup(pub->peer, sgh, r->now) == MOQ_OK);
}

/* Object ids are unique within a GROUP: subgroups partition a group's object-id
 * space, they do NOT restart it, so subgroup sg carries ids
 * [sg*DP_O, sg*DP_O + DP_O). The tag binds payload bytes to (group, id) so a
 * mis-routed payload is caught. */
uint8_t
relay_bind_test_dp_tag(uint64_t g, uint64_t oid)
{
    return (uint8_t)(0x40u + g * DP_OPG + oid);   /* distinct, nonzero       */
}

/* One subgroup: DP_O tagged data objects (payload byte 0 = relay_bind_test_dp_tag binds the
 * bytes to the identity, so a mis-routed payload is caught), then plain FIN. */
void
relay_bind_test_dp_send_subgroup(rig_t *r, conn_t *pub, moq_subscription_t up_sub,
                                 uint64_t group, uint64_t subgroup)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh));
    for (uint64_t o = 0; o < DP_O; o++) {
        uint64_t oid = subgroup * DP_O + o;   /* group-unique object id */
        uint8_t body[4] = { relay_bind_test_dp_tag(group, oid), (uint8_t)group,
                            (uint8_t)subgroup, (uint8_t)oid };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, oid, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

/* ---- Source-end termination oracle -------------------------------------- *
 * When the source ends — a PULL upstream sends SUBSCRIBE_DONE, or a PUSH
 * publisher sends PUBLISH_FINISHED — the relay must terminate open-ended
 * downstream subscribers with a wire SUBSCRIBE_DONE carrying the forwarded
 * status, after delivering every pre-end object exactly once. Both source kinds
 * route through moqr_core_source_finish. */
uint8_t
relay_bind_test_se_tag(uint64_t g, uint64_t o)
{
    return (uint8_t)(0x30u + g * 8u + o);   /* distinct per (group, object) */
}

/* One group's subgroup 0 with `n` tagged objects, plain FIN. Writes on the
 * pull subscription (open_subgroup) or the push publication (open_pub_subgroup). */
void
relay_bind_test_se_send_group(rig_t *r, conn_t *pub, bool push, moq_subscription_t up_sub,
                              moq_publication_t pubh, uint64_t group, uint64_t n)
{
    relay_bind_test_se_send_group_fin(r, pub, push, up_sub, pubh, group, n,
                                      true, NULL);
}

void
relay_bind_test_se_send_group_fin(rig_t *r, conn_t *pub, bool push,
                                  moq_subscription_t up_sub,
                                  moq_publication_t pubh, uint64_t group,
                                  uint64_t n, bool fin,
                                  moq_subgroup_handle_t *open_out)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    if (push) {
        DP_RETRY(r, moq_session_open_pub_subgroup(pub->peer, pubh, &sgc,
                                                  r->now, &sgh));
    } else {
        DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                              &sgh));
    }
    for (uint64_t o = 0; o < n; o++) {
        uint8_t body[4] = { relay_bind_test_se_tag(group, o), (uint8_t)group, 0, (uint8_t)o };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, o, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    if (fin) {
        DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
    } else if (open_out != NULL) {
        *open_out = sgh;
    }
}

/* ---- Mixed d16/d18 object bridging oracle -------------------------------- *
 * Real wire objects from a publisher on one draft must be delivered correctly to
 * a subscriber on the other draft, through the production binding — the relay
 * ingests on the publisher's profile and re-emits on the subscriber's. Runs both
 * directions (d16->d18 and d18->d16). */
static uint8_t
md_tag(uint64_t g, uint64_t o)
{
    return (uint8_t)(0x20u + g * 8u + o);   /* distinct per (group, object) */
}

/* One group's subgroup 0: `n` normal tagged objects (ids 0..n-1), optionally a
 * terminal EOG status object (id n), plain FIN. */
static void
md_send_group(rig_t *r, conn_t *pub, moq_subscription_t up_sub, uint64_t group,
              uint64_t n, bool eog)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    DP_RETRY(r, moq_session_open_subgroup(pub->peer, up_sub, &sgc, r->now,
                                          &sgh));
    for (uint64_t o = 0; o < n; o++) {
        uint8_t body[4] = { md_tag(group, o), (uint8_t)group, 0, (uint8_t)o };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        DP_RETRY(r, moq_session_write_object(pub->peer, sgh, o, pl, r->now));
        moq_rcbuf_decref(pl);
    }
    if (eog) {
        DP_RETRY(r, moq_session_write_status_object(pub->peer, sgh, n,
                                                    MOQ_OBJECT_END_OF_GROUP,
                                                    r->now));
    }
    DP_RETRY(r, moq_session_close_subgroup(pub->peer, sgh, r->now));
}

int
relay_bind_test_dataplane_mixed_draft_oracle(moq_version_t pub_ver, moq_version_t sub_ver,
                                             uint32_t max_actions)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = max_actions;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: dataplane_mixed_draft_oracle rig create\n");
        return 1;
    }
    int pumps = max_actions == 0 ? 40 : 200;
    conn_t *pub = relay_bind_test_rig_connect(&rig, pub_ver);
    conn_t *sub = relay_bind_test_rig_connect(&rig, sub_ver);
    R_CHECK(&rig, pub && sub);
    relay_bind_test_rig_pump(&rig, 4);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 4);

    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) ==
                      MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    moq_subscription_t up_sub = pub_ps.up_sub;
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Matrix: group 0 (2 normal objects, plain FIN), group 1 (2 normal + a
     * terminal EOG status object, plain FIN). */
    md_send_group(&rig, pub, up_sub, 0, 2, false);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    md_send_group(&rig, pub, up_sub, 1, 2, true);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* Source end (forwarded across drafts). */
    moq_done_subscribe_cfg_t dcfg;
    moq_done_subscribe_cfg_init(&dcfg);
    dcfg.status_code = 0x4;
    R_CHECK(&rig, moq_session_done_subscribe(pub->peer, up_sub, &dcfg,
                                             rig.now) == MOQ_OK);
    for (int k = 0; k < pumps; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        if (s_ps.done_seen) {
            break;
        }
    }
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);

    /* ---- oracle (wire-observable only) ---- *
     * The publisher's draft-X wire objects arrive at the draft-Y subscriber
     * unchanged in identity: exactly {g0 o0,o1; g1 o0,o1 normal; g1 o2 EOG
     * status} = 5 records, each once, exact fingerprint, status (NORMAL for the
     * four data objects, END_OF_GROUP for the status object), per-stream order,
     * both plain-FIN subgroups sealed by identity, and the source-end
     * SUBSCRIBE_DONE with its status forwarded — all bridged across drafts. */
    static const struct {
        uint64_t g, o;
        uint8_t  st, fp;
        bool     pn;
    } want[5] = {
        { 0, 0, MOQ_OBJECT_NORMAL,       0x20, false },
        { 0, 1, MOQ_OBJECT_NORMAL,       0x21, false },
        { 1, 0, MOQ_OBJECT_NORMAL,       0x28, false },
        { 1, 1, MOQ_OBJECT_NORMAL,       0x29, false },
        { 1, 2, MOQ_OBJECT_END_OF_GROUP, 0x00, true  },
    };
    R_CHECK(&rig, s_ps.dlv_overflow == 0);
    R_CHECK(&rig, s_ps.dlv_n == 5);
    R_CHECK(&rig, s_ps.done_seen);
    R_CHECK(&rig, s_ps.done_count == 1);    /* exactly one SUBSCRIBE_DONE     */
    R_CHECK(&rig, s_ps.done_code == 0x4);   /* source status forwarded        */
    int seen[5] = { 0, 0, 0, 0, 0 };
    for (int i = 0; i < s_ps.dlv_n; i++) {
        dlv_rec_t *d = &s_ps.dlv[i];
        R_CHECK(&rig, !d->after_done);
        R_CHECK(&rig, d->sub == sh._opaque);
        R_CHECK(&rig, !d->datagram);
        R_CHECK(&rig, d->subgroup == 0);
        int m = -1;
        for (int w = 0; w < 5; w++) {
            if (want[w].g == d->group && want[w].o == d->object_id) {
                m = w;
                break;
            }
        }
        R_CHECK(&rig, m >= 0);
        if (m >= 0) {
            seen[m]++;
            R_CHECK(&rig, d->status == want[m].st);
            R_CHECK(&rig, d->payload_null == want[m].pn);
            R_CHECK(&rig, d->fp == want[m].fp);
        }
    }
    for (int w = 0; w < 5; w++) {
        R_CHECK(&rig, seen[w] == 1);
    }
    for (uint64_t g = 0; g < 2; g++) {
        int64_t last = -1;                   /* per-stream ascending order    */
        for (int i = 0; i < s_ps.dlv_n; i++) {
            dlv_rec_t *d = &s_ps.dlv[i];
            if (d->group == g && d->subgroup == 0) {
                R_CHECK(&rig, (int64_t)d->object_id > last);
                last = (int64_t)d->object_id;
            }
        }
    }
    R_CHECK(&rig, s_ps.subgroup_finished == 2);   /* exact FIN identity */
    for (uint64_t g = 0; g < 2; g++) {
        int fins = 0;
        for (int fi = 0; fi < s_ps.finished_count; fi++) {
            if (s_ps.finished_gsg[fi][0] == g &&
                s_ps.finished_gsg[fi][1] == 0) {
                fins++;
            }
        }
        R_CHECK(&rig, fins == 1);
    }

    relay_bind_test_max_actions = 0;
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: dataplane_mixed_draft_oracle (pub=%d sub=%d aq=%u)\n",
               (int)pub_ver, (int)sub_ver, max_actions);
    }
    return f;
}

/* -- FETCH served over real sessions (SimPair) ---------------------------- *
 * A downstream peer issues moq_session_fetch; the bind serves it from the core
 * retained cursor and the peer observes FETCH_OK / FETCH_OBJECT / FETCH_COMPLETE
 * (or FETCH_ERROR). Publisher content is set up directly on the core (a push
 * publisher needs no session). */

moqr_binding_t
relay_bind_test_fetch_pub_open(rig_t *r)
{
    moqr_binding_t pb;
    moq_bytes_t nsb[1] = { B("demo") };
    R_CHECK(r, moqr_core_binding_open(r->core, 900, &pb) == MOQR_OK);
    R_CHECK(r, moqr_core_announce(r->core, pb, (moqr_ns_t){ nsb, 1 }) ==
                   MOQR_OK);
    return pb;
}

moqr_track_t
relay_bind_test_fetch_pub_track(rig_t *r, moqr_binding_t pb, const char *name)
{
    moq_bytes_t nsb[1] = { B("demo") };
    moqr_track_t tk = MOQR_TRACK_INVALID;
    R_CHECK(r, moqr_core_publish_open(r->core, pb, (moqr_ns_t){ nsb, 1 },
                                      B(name), 901, &tk) == MOQR_OK);
    return tk;
}

void
relay_bind_test_fetch_ing(rig_t *r, moqr_track_t tk, uint64_t g, uint64_t sg, uint64_t o,
                          uint8_t fp)
{
    uint8_t body[8];
    memset(body, fp, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) == 0);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 1;
    d.payload = pl;
    d.now_us = r->now;
    if (moqr_core_ingest(r->core, tk, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        R_CHECK(r, 0);
    }
}

void
relay_bind_test_fetch_issue(rig_t *r, conn_t *cn, const char *name, uint64_t sg, uint64_t so,
                            uint64_t eg, uint64_t eo, moq_fetch_t *out)
{
    moq_bytes_t nsb[1] = { B("demo") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsb, .count = 1 };
    fc.track_name = B(name);
    fc.start_group = sg;
    fc.start_object = so;
    fc.end_group = eg;
    fc.end_object = eo;
    R_CHECK(r, moq_session_fetch(cn->peer, &fc, r->now, out) == MOQ_OK);
}

/* Stream one chunk (len bytes of `byte`) into the active object. */
void
relay_bind_test_parity_write_chunk(rig_t *r, conn_t *pub, moq_subgroup_handle_t sg, uint8_t byte,
                                   size_t len)
{
    uint8_t body[256];
    memset(body, byte, len);
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, len, &pl) == MOQ_OK);
    R_CHECK(r, moq_session_write_object_data(pub->peer, sg, pl, r->now) == MOQ_OK);
    moq_rcbuf_decref(pl);
}

void
relay_bind_test_boot_fill(uint8_t *buf, size_t len, uint8_t seed)
{
    for (size_t i = 0; i < len; i++) {
        buf[i] = (uint8_t)(seed + (uint8_t)i);
    }
}

static bool
boot_payload_is(const moq_rcbuf_t *pl, uint8_t seed, size_t len)
{
    if (pl == NULL || moq_rcbuf_len(pl) != len) {
        return false;
    }
    uint8_t want[64];
    relay_bind_test_boot_fill(want, len, seed);
    return memcmp(moq_rcbuf_data(pl), want, len) == 0;
}

const moq_bytes_t relay_bind_test_JF_NS[2] = { { (const uint8_t *)"boot", 4 },
                                      { (const uint8_t *)"cat", 3 } };

void
relay_bind_test_jf_origin_drain(rig_t *r, jf_origin_t *o)
{
    if (o->gone) {
        return;
    }
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(o->cn->peer, evs, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moq_event_t *ev = &evs[i];
            switch (ev->kind) {
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                o->ns_accepted++;
                break;
            case MOQ_EVENT_SUBSCRIBE_REQUEST: {
                const moq_subscribe_request_event_t *sq = &ev->u.subscribe_request;
                bool is_cat = sq->track_name.len == 7 &&
                              memcmp(sq->track_name.data, "catalog", 7) == 0;
                o->sub_requests++;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                ac.has_largest = true;
                ac.largest_group = is_cat ? o->largest_group : 0;
                ac.largest_object = is_cat ? o->largest_object : 0;
                R_CHECK(r, moq_session_accept_subscribe(o->cn->peer, sq->sub, &ac,
                                                        r->now) == MOQ_OK);
                if (is_cat) {
                    o->cat_sub = sq->sub;
                    o->cat_seen = true;
                } else {
                    o->live_sub = sq->sub;
                    o->live_seen = true;
                }
                break;
            }
            case MOQ_EVENT_FETCH_REQUEST: {
                const moq_fetch_request_event_t *fq = &ev->u.fetch_request;
                if (o->fetch_requests < JF_MAX_REQ) {
                    int k = o->fetch_requests;
                    o->req[k] = fq->fetch;
                    o->req_sg[k] = fq->start_group;
                    o->req_so[k] = fq->start_object;
                    o->req_eg[k] = fq->end_group;
                    o->req_eo[k] = fq->end_object;
                }
                o->fetch_requests++;
                break;
            }
            case MOQ_EVENT_FETCH_CANCELLED:
                o->cancelled++;
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* Well-formed object properties on both drafts: two non-mandatory KVPs with
 * varint values (type 0x10 = object id, type 0x12 = 0x2A). */
static void
jf_props_fill(uint8_t *out, uint64_t oid)
{
    out[0] = 0x10;
    out[1] = (uint8_t)(oid & 0x3F);
    out[2] = 0x12;
    out[3] = 0x2A;
}

static bool
jf_props_is(const moq_rcbuf_t *pr, uint64_t oid)
{
    if (pr == NULL || moq_rcbuf_len(pr) != JF_PROPS_LEN) {
        return false;
    }
    uint8_t want[JF_PROPS_LEN];
    jf_props_fill(want, oid);
    return memcmp(moq_rcbuf_data(pr), want, sizeof(want)) == 0;
}

void
relay_bind_test_jf_down_drain(rig_t *r, jf_down_t *d)
{
    (void)r;
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(d->cn->peer, evs, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moq_event_t *ev = &evs[i];
            bool fetch_item = false;
            uint64_t h = 0;
            switch (ev->kind) {
            case MOQ_EVENT_SUBSCRIBE_OK:
                if (d->fh_set == false && ev->u.subscribe_ok.sub._opaque == d->sh._opaque) {
                    d->sub_ok = true;
                } else if (ev->u.subscribe_ok.sub._opaque == d->live_sh._opaque) {
                    d->live_ok = true;
                } else if (ev->u.subscribe_ok.sub._opaque == d->sh._opaque) {
                    d->sub_ok = true;
                }
                break;
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                d->sub_err++;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                d->done++;
                break;
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                d->ns_accepted++;
                break;
            case MOQ_EVENT_NAMESPACE_REJECTED: {
                d->ns_rejected++;
                d->ns_rejected_code = ev->u.namespace_rejected.error_code;
                size_t n_ = ev->u.namespace_rejected.reason.len < sizeof(d->ns_rejected_reason) - 1
                                ? ev->u.namespace_rejected.reason.len : sizeof(d->ns_rejected_reason) - 1;
                if (n_) memcpy(d->ns_rejected_reason, ev->u.namespace_rejected.reason.data, n_);
                d->ns_rejected_reason[n_] = '\0';
                break;
            }
            case MOQ_EVENT_SESSION_CLOSED: {
                const moq_session_closed_event_t *sc = &ev->u.closed;
                d->closed++;
                d->close_code = sc->code;
                size_t n_ = sc->reason.len < sizeof(d->close_reason) - 1
                                ? sc->reason.len : sizeof(d->close_reason) - 1;
                if (n_ > 0) memcpy(d->close_reason, sc->reason.data, n_);
                d->close_reason[n_] = '\0';
                break;
            }
            case MOQ_EVENT_OBJECT_RECEIVED:
                if (ev->u.object_received.sub._opaque == d->live_sh._opaque) {
                    d->live_objects++;
                    d->live_exact = ev->u.object_received.group_id == 1 &&
                                    ev->u.object_received.object_id == 0 &&
                                    boot_payload_is(ev->u.object_received.payload,
                                                    0xC0, 32);
                }
                break;
            case MOQ_EVENT_FETCH_OK:       fetch_item = true; h = ev->u.fetch_ok.fetch._opaque; break;
            case MOQ_EVENT_FETCH_OBJECT:   fetch_item = true; h = ev->u.fetch_object.fetch._opaque; break;
            case MOQ_EVENT_FETCH_GAP:      fetch_item = true; h = ev->u.fetch_gap.fetch._opaque; break;
            case MOQ_EVENT_FETCH_COMPLETE: fetch_item = true; h = ev->u.fetch_complete.fetch._opaque; break;
            case MOQ_EVENT_FETCH_ERROR:    fetch_item = true; h = ev->u.fetch_error.fetch._opaque; break;
            case MOQ_EVENT_FETCH_RESET:    fetch_item = true; h = ev->u.fetch_reset.fetch._opaque; break;
            default:
                break;
            }
            if (fetch_item) {
                int extra = -1;
                for (int k = 0; k < d->req_n; k++) {
                    if (d->req_fh[k]._opaque == h) extra = k;
                }
                if (extra >= 0) {
                    if (ev->kind == MOQ_EVENT_FETCH_ERROR) d->req_err[extra]++;
                } else if (!d->fh_set || h != d->fh._opaque) {
                    d->foreign++;
                } else {
                    if (d->ledger_n < JF_LEDGER) {
                        d->ledger[d->ledger_n++] = (int)ev->kind;
                    } else {
                        d->overflow = true;
                    }
                    switch (ev->kind) {
                    case MOQ_EVENT_FETCH_OK:
                        d->ok++;
                        d->ok_eot = ev->u.fetch_ok.end_of_track;
                        d->ok_eg = ev->u.fetch_ok.end_group;
                        d->ok_eo = ev->u.fetch_ok.end_object;
                        break;
                    case MOQ_EVENT_FETCH_OBJECT: {
                        const moq_fetch_object_event_t *fo = &ev->u.fetch_object;
                        if (d->objects < 8) {
                            int k = d->objects;
                            d->obj_g[k] = fo->group_id;
                            d->obj_o[k] = fo->object_id;
                            d->obj_len[k] = fo->payload ? moq_rcbuf_len(fo->payload) : 0;
                            d->obj_first[k] = d->obj_len[k] ? moq_rcbuf_data(fo->payload)[0] : 0;
                            d->obj_exact[k] =
                                fo->object_id == 0
                                    ? boot_payload_is(fo->payload, 0xB0, BOOT_BASE_LEN)
                                    : boot_payload_is(fo->payload, 0xD0, BOOT_DELTA_LEN);
                            d->props_len[k] = fo->properties ? moq_rcbuf_len(fo->properties) : 0;
                            d->props_exact[k] = jf_props_is(fo->properties, fo->object_id);
                        }
                        d->objects++;
                        break;
                    }
                    case MOQ_EVENT_FETCH_GAP:      d->gaps++; break;
                    case MOQ_EVENT_FETCH_COMPLETE: d->complete++; break;
                    case MOQ_EVENT_FETCH_ERROR: {
                        const moq_fetch_error_event_t *fe = &ev->u.fetch_error;
                        d->error++;
                        d->error_code = fe->error_code;
                        d->can_retry = fe->can_retry;
                        d->reason_len = fe->reason.len < sizeof(d->reason)
                                            ? fe->reason.len : sizeof(d->reason);
                        if (d->reason_len > 0) {
                            memcpy(d->reason, fe->reason.data, d->reason_len);
                        }
                        break;
                    }
                    case MOQ_EVENT_FETCH_RESET:
                        d->reset++;
                        d->reset_code = ev->u.fetch_reset.error_code;
                        break;
                    default:
                        break;
                    }
                }
            }
            moq_event_cleanup(ev);
        }
    }
}

void
relay_bind_test_jf_cycle(jf_t *j)
{
    relay_bind_test_rig_cycle(&j->rig);
    relay_bind_test_jf_origin_drain(&j->rig, &j->o);
    for (int i = 0; i < j->n_down; i++) {
        relay_bind_test_jf_down_drain(&j->rig, &j->d[i]);
    }
    if (j->has_live) {
        relay_bind_test_jf_down_drain(&j->rig, &j->live);
    }
}

void
relay_bind_test_jf_stats(jf_t *j, moqr_bind_fwd_debug_t *st)
{
    moqr_bind_debug_fwd_stats(j->rig.bind, st);
}

void
relay_bind_test_jf_subscribe_catalog(jf_t *j, jf_down_t *d)
{
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
    sc.track_name = B("catalog");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    R_CHECK(&j->rig, moq_session_subscribe(d->cn->peer, &sc, j->rig.now, &d->sh) == MOQ_OK);
}

void
relay_bind_test_jf_subscribe_live(jf_t *j, jf_down_t *d)
{
    moq_subscribe_cfg_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
    sc.track_name = B("live");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    R_CHECK(&j->rig, moq_session_subscribe(d->cn->peer, &sc, j->rig.now, &d->live_sh) == MOQ_OK);
}

/* Relative Joining FETCH(0) on the catalog subscription (ascending). */
moq_result_t
relay_bind_test_jf_join_ex(jf_t *j, jf_down_t *d, moq_group_order_t order)
{
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.is_joining = true;
    fc.joining_relative = true;
    fc.joining_sub = d->sh;
    fc.joining_start = 0;
    fc.group_order = order;
    memset(&d->fh, 0, sizeof(d->fh));
    moq_result_t rc = moq_session_fetch(d->cn->peer, &fc, j->rig.now, &d->fh);
    d->fh_set = rc == MOQ_OK;
    return rc;
}

void
relay_bind_test_jf_join(jf_t *j, jf_down_t *d)
{
    R_CHECK(&j->rig, relay_bind_test_jf_join_ex(j, d, MOQ_GROUP_ORDER_ASCENDING) == MOQ_OK);
}

/* Rig + origin announce + n_down catalog subscriptions (each SUBSCRIBE_OK'd
 * through the relay) + optional live-control subscriber on its own conn. */
/* When set, relay_bind_test_jf_setup_ex fails the first JF_KEY_LEN-byte allocation while the
 * catalog subscriptions are being established: the relay keeps the upstream
 * subscription but could not retain its track key (no forwarding source). */
bool relay_bind_test_jf_setup_usub_key_oom = false;

bool
relay_bind_test_jf_setup_at(jf_t *j, moq_version_t version, int n_down, bool live_control,
                            uint64_t largest_group, uint64_t largest_object, bool origin_last)
{
    memset(j, 0, sizeof(*j));
    relay_bind_test_ca_init(&j->a);
    j->version = version;
    j->lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    if (relay_bind_test_rig_create(&j->rig, &j->a) != MOQR_OK) {
        printf("FAIL: jf rig create\n");
        return false;
    }
    /* Connection slot order is pump order: origin first by default; with
     * origin_last the downstream peers' events are processed before the
     * origin's in every pump. */
    if (!origin_last) {
        j->o.cn = relay_bind_test_rig_connect(&j->rig, version);
    }
    j->o.largest_group = largest_group;
    j->o.largest_object = largest_object;
    j->n_down = n_down;
    for (int i = 0; i < n_down; i++) {
        j->d[i].cn = relay_bind_test_rig_connect(&j->rig, version);
    }
    if (origin_last) {
        j->o.cn = relay_bind_test_rig_connect(&j->rig, version);
    }
    if (live_control) {
        j->live.cn = relay_bind_test_rig_connect(&j->rig, version);
        j->has_live = true;
    }
    if (j->o.cn == NULL || (live_control && j->live.cn == NULL)) {
        printf("FAIL: jf rig connect\n");
        return false;
    }
    for (int i = 0; i < n_down; i++) {
        if (j->d[i].cn == NULL) {
            printf("FAIL: jf rig connect (down %d)\n", i);
            return false;
        }
    }
    relay_bind_test_rig_pump(&j->rig, 4);
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)relay_bind_test_JF_NS, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(&j->rig, moq_session_publish_namespace(j->o.cn->peer, &pcfg, j->rig.now,
                                                   &ann) == MOQ_OK);
    for (int i = 0; i < 8 && j->o.ns_accepted == 0; i++) relay_bind_test_jf_cycle(j);
    R_CHECK(&j->rig, j->o.ns_accepted == 1);
    if (relay_bind_test_jf_setup_usub_key_oom) {
        j->a.fail_size = JF_KEY_LEN;
    }
    for (int i = 0; i < n_down; i++) {
        relay_bind_test_jf_subscribe_catalog(j, &j->d[i]);
    }
    if (live_control) {
        relay_bind_test_jf_subscribe_live(j, &j->live);
    }
    bool all = false;
    for (int i = 0; i < 16 && !all; i++) {
        relay_bind_test_jf_cycle(j);
        all = !live_control || j->live.live_ok;
        for (int k = 0; k < n_down; k++) {
            all = all && j->d[k].sub_ok;
        }
    }
    j->a.fail_size = 0;
    relay_bind_test_jf_setup_usub_key_oom = false;
    R_CHECK(&j->rig, all);
    R_CHECK(&j->rig, j->o.cat_seen == (n_down > 0));
    R_CHECK(&j->rig, j->o.live_seen == live_control);
    return j->rig.failures == 0;
}

bool
relay_bind_test_jf_setup_ex(jf_t *j, moq_version_t version, int n_down, bool live_control,
                            uint64_t largest_object, bool origin_last)
{
    return relay_bind_test_jf_setup_at(j, version, n_down, live_control, 0,
                                      largest_object, origin_last);
}

bool
relay_bind_test_jf_setup(jf_t *j, moq_version_t version, int n_down, bool live_control,
                         uint64_t largest_object)
{
    return relay_bind_test_jf_setup_ex(j, version, n_down, live_control, largest_object, false);
}

/* One live object origin -> relay -> live-control subscriber, exact bytes:
 * unrelated media progress is preserved whatever the forwarding rows did. */
void
relay_bind_test_jf_live_control(jf_t *j)
{
    if (!j->has_live || j->o.gone) {
        return;
    }
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 1;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 100;
    moq_subgroup_handle_t sgh;
    R_CHECK(&j->rig, moq_session_open_subgroup(j->o.cn->peer, j->o.live_sub, &sgc,
                                               j->rig.now, &sgh) == MOQ_OK);
    uint8_t body[32];
    relay_bind_test_boot_fill(body, sizeof(body), 0xC0);
    moq_rcbuf_t *pl = NULL;
    R_CHECK(&j->rig, moq_rcbuf_create(&j->a.vt, body, sizeof(body), &pl) == MOQ_OK);
    R_CHECK(&j->rig, moq_session_write_object(j->o.cn->peer, sgh, 0, pl, j->rig.now) == MOQ_OK);
    moq_rcbuf_decref(pl);
    R_CHECK(&j->rig, moq_session_close_subgroup(j->o.cn->peer, sgh, j->rig.now) == MOQ_OK);
    for (int i = 0; i < 10 && j->live.live_objects == 0; i++) relay_bind_test_jf_cycle(j);
    R_CHECK(&j->rig, j->live.live_objects == 1 && j->live.live_exact);
}

int
relay_bind_test_jf_teardown(jf_t *j, const char *row)
{
    moqr_bind_fwd_debug_t st;
    relay_bind_test_jf_stats(j, &st);
    R_CHECK(&j->rig, st.live == 0 && st.bytes_used == 0);
    relay_bind_test_rig_destroy(&j->rig);
    if (j->a.live != 0) {
        printf("FAIL: %s %s leak live=%ld\n", row, j->lbl, j->a.live);
        j->rig.failures++;
    }
    relay_bind_test_fetch_forward_bytes = 0;
    relay_bind_test_max_fetches = 0;
    relay_bind_test_max_parked = 0;
    relay_bind_test_max_actions = 0;
    relay_bind_test_max_events = 0;
    relay_bind_test_authorize = NULL;
    relay_bind_test_authorize_ctx = NULL;
    relay_bind_test_jf_setup_usub_key_oom = false;
    if (j->rig.failures == 0) {
        printf("PASS: %s %s\n", row, j->lbl);
    }
    return j->rig.failures;
}

void
relay_bind_test_jf_origin_accept(jf_t *j, int k, uint64_t eg, uint64_t eo, bool eot, bool with_props)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_of_track = eot;
    ac.end_group = eg;
    ac.end_object = eo;
    /* Five varint KVPs. Types are delta-encoded, so these carry 0x10, 0x12,
     * 0x14, 0x16 and 0x18: even (non-mandatory) and unregistered, which keeps
     * them clear of registered properties with validated values. */
    static const uint8_t props[10] = { 0x10, 0x00, 0x02, 0x00, 0x02, 0x00,
                                       0x02, 0x00, 0x02, 0x00 };
    if (with_props) {
        ac.track_properties = (moq_bytes_t){ props, sizeof(props) };
    }
    moq_result_t rc;
    JF_ORIGIN_RETRY(j, moq_session_accept_fetch(j->o.cn->peer, j->o.req[k], &ac, j->rig.now), rc);
    R_CHECK(&j->rig, rc == MOQ_OK);
}

moq_result_t
relay_bind_test_jf_origin_write_raw(jf_t *j, int k, uint64_t g, uint64_t oid, uint8_t seed,
                                    size_t len, bool with_props)
{
    uint8_t body[256];
    if (len > sizeof(body)) len = sizeof(body);
    relay_bind_test_boot_fill(body, len, seed);
    moq_rcbuf_t *pl = NULL, *pr = NULL;
    if (moq_rcbuf_create(&j->a.vt, body, len, &pl) != MOQ_OK) {
        return MOQ_ERR_NOMEM;
    }
    if (with_props) {
        uint8_t props[JF_PROPS_LEN];
        jf_props_fill(props, oid);
        if (moq_rcbuf_create(&j->a.vt, props, sizeof(props), &pr) != MOQ_OK) {
            moq_rcbuf_decref(pl);
            return MOQ_ERR_NOMEM;
        }
    }
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = g;
    oc.subgroup_id = 0;
    oc.object_id = oid;
    oc.publisher_priority = 100;
    oc.payload = pl;
    oc.properties = pr;
    moq_result_t rc;
    JF_ORIGIN_RETRY(j, moq_session_write_fetch_object(j->o.cn->peer, j->o.req[k], &oc, j->rig.now), rc);
    moq_rcbuf_decref(pl);
    if (pr) moq_rcbuf_decref(pr);
    return rc;
}

void
relay_bind_test_jf_origin_end(jf_t *j, int k)
{
    moq_result_t rc;
    JF_ORIGIN_RETRY(j, moq_session_end_fetch(j->o.cn->peer, j->o.req[k], j->rig.now), rc);
    R_CHECK(&j->rig, rc == MOQ_OK);
}

void
relay_bind_test_jf_wait_requests(jf_t *j, int n)
{
    for (int i = 0; i < 12 && j->o.fetch_requests < n; i++) relay_bind_test_jf_cycle(j);
    R_CHECK(&j->rig, j->o.fetch_requests == n);
}

void
relay_bind_test_d22_track_setup(rig_t *rig, d22_track_t *t, moq_version_t version,
                                uint64_t groups)
{
    memset(t, 0, sizeof(*t));
    t->nsp[0] = B("live");
    t->nsp[1] = B("cam");
    t->pub = relay_bind_test_rig_connect(rig, version);
    t->prime = relay_bind_test_rig_connect(rig, version);
    R_CHECK(rig, t->pub != NULL && t->prime != NULL);
    if (t->pub == NULL || t->prime == NULL) {
        return;
    }
    relay_bind_test_rig_pump(rig, 4);
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = t->nsp, .count = 2 };
    moq_announcement_t ann;
    R_CHECK(rig, moq_session_publish_namespace(t->pub->peer, &pcfg, rig->now,
                                               &ann) == MOQ_OK);
    relay_bind_test_rig_pump(rig, 4);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = t->nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t ph;
    R_CHECK(rig, moq_session_subscribe(t->prime->peer, &scfg, rig->now,
                                       &ph) == MOQ_OK);
    relay_bind_test_rig_pump(rig, 6);
    relay_bind_test_peer_drain(rig, t->pub, &t->pub_ps, true, false, 0, 0);
    R_CHECK(rig, t->pub_ps.up_seen);
    relay_bind_test_rig_pump(rig, 6);
    for (uint64_t g = 0; g < groups; g++) {
        relay_bind_test_pub_subgroup_n(rig, t->pub, t->pub_ps.up_sub, g, 0, 3, false);
        relay_bind_test_rig_pump(rig, 4);
    }
    relay_bind_test_rig_pump(rig, 6);
    relay_bind_test_peer_drain(rig, t->prime, &t->prime_ps, false, false, 0, 0);
}

moq_result_t
relay_bind_test_d22_subscribe(rig_t *rig, conn_t *cn, const d22_track_t *t,
                              const moq_location_filter_cfg_t *loc, const moq_fill_cfg_t *fill,
                              moq_subscription_t *out)
{
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = (moq_bytes_t *)t->nsp,
                                              .count = 2 };
    scfg.track_name = B("v");
    scfg.location = loc;
    scfg.fill = fill;
    return moq_session_subscribe(cn->peer, &scfg, rig->now, out);
}

/* The plain-FIN group has an unresolved tail. Reply to the actual FETCH,
 * never turn that tail into END_OF_GROUP or spend an extra pump cycle. */
int
relay_bind_test_d22_fill_origin_tail(rig_t *rig, d22_track_t *t, bool answer)
{
    int requests = 0;
    for (int i = 0; i < 16; i++) {
        moq_event_t ev;
        if (moq_session_poll_events(t->pub->peer, &ev, 1) == 0) {
            break;
        }
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            const moq_fetch_request_event_t *q = &ev.u.fetch_request;
            moq_fetch_t fetch = q->fetch;
            bool tail = q->start_group == 0 && q->start_object == 3 &&
                        q->end_group == 0 && q->end_object == 0 &&
                        q->joining_sub._opaque == 0 && q->joining_pub._opaque == 0;
            requests++;
            R_CHECK(rig, tail);
            if (tail && answer) {
                moq_accept_fetch_cfg_t accept;
                moq_accept_fetch_cfg_init(&accept);
                accept.end_group = 0;
                accept.end_object = 0;
                R_CHECK(rig, moq_session_accept_fetch(t->pub->peer, fetch,
                                                      &accept, rig->now) == MOQ_OK);
                R_CHECK(rig, moq_session_write_fetch_range(
                                 t->pub->peer, fetch, MOQ_FETCH_RANGE_UNKNOWN,
                                 0, UINT64_MAX, rig->now) == MOQ_OK);
                R_CHECK(rig, moq_session_end_fetch(t->pub->peer, fetch,
                                                   rig->now) == MOQ_OK);
            }
        }
        moq_event_cleanup(&ev);
    }
    return requests;
}

/* Publish exactly the given object ids on one subgroup of `group` through the
 * track's origin publisher, so the relay's retained log holds those and no
 * others for that subgroup. */
void
relay_bind_test_d22_pub_ids(rig_t *rig, d22_track_t *t, uint64_t group, uint64_t subgroup,
                            const uint64_t *ids, size_t n)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = subgroup;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t sgh;
    R_CHECK(rig, moq_session_open_subgroup(t->pub->peer, t->pub_ps.up_sub, &sgc,
                                           rig->now, &sgh) == MOQ_OK);
    for (size_t i = 0; i < n; i++) {
        uint8_t body[4] = { (uint8_t)group, (uint8_t)ids[i], 0xC3, 0x3C };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(rig, moq_rcbuf_create(&rig->alloc->vt, body, sizeof(body),
                                      &pl) == MOQ_OK);
        R_CHECK(rig, moq_session_write_object(t->pub->peer, sgh, ids[i], pl,
                                              rig->now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    R_CHECK(rig, moq_session_close_subgroup(t->pub->peer, sgh, rig->now) ==
                     MOQ_OK);
    relay_bind_test_rig_pump(rig, 4);
}

/* A track whose retained log has a hole: group 0 complete (three objects),
 * group 1 holding {0, 2}, so Largest Object is {1,2} and Object {1,1} is not
 * held. */
void
relay_bind_test_d22_hole_setup(rig_t *rig, d22_track_t *t)
{
    relay_bind_test_d22_track_setup(rig, t, MOQ_VERSION_DRAFT_22, 1);
    static const uint64_t ids[2] = { 0, 2 };
    relay_bind_test_d22_pub_ids(rig, t, 1, 0, ids, 2);
    relay_bind_test_rig_pump(rig, 4);
    relay_bind_test_peer_drain(rig, t->prime, &t->prime_ps, false, false, 0, 0);
}

/* Subscribe Next Object with a fill over {0,0}..Largest carrying the given
 * FILL_TIMEOUT (or none). */
moq_result_t
relay_bind_test_d22_subscribe_fill(rig_t *rig, conn_t *cn, const d22_track_t *t,
                                   bool has_timeout, uint64_t timeout_ms,
                                   moq_subscription_t *out)
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
    return relay_bind_test_d22_subscribe(rig, cn, t, &loc, &fill, out);
}
