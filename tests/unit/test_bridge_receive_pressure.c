/* Receive pressure through real bridge ingress and session actions.
 * STOP is a local policy terminal, not a peer RESET or provider retirement.
 * Pools, rings, capacities and handles are never fabricated or modified here.
 * The pair supplies real setup/SUBSCRIBE exchanges; after setup each step()
 * is exactly ONE outer bridge service call. Held callbacks are replayed only
 * explicitly, with their original bytes and FIN. No provider/wire claim.
 * Link like test_bridge_local_retire, against moq-core-test-internals. */
static int failures;
#include "../support/bridge_wire_pair.h"
#include "test_binding_support.h"
#include "../../core/src/bridge/transport_bridge_internal.h"

static const char *row_name;
static unsigned checks, rows;
static unsigned stop_attempts;
static bool check(bool ok, const char *expr, int line)
{
    ++checks;
    if (!ok) {
        ++failures;
        fprintf(stderr, "FAIL[%s] %s:%d: %s\n", row_name, __FILE__, line, expr);
    }
    return ok;
}
#define CHECK(x) check((x), #x, __LINE__)
#define NEED(x) do { if (!(x)) { \
    fprintf(stderr, "SETUP[%s] %s:%d: %s\n", row_name, __FILE__, __LINE__, #x); \
    exit(2); \
} } while (0)

typedef struct fixture {
    rl_pair_t p;
    moq_subscription_t sub;
    uint8_t alias;
} fixture_t;

typedef struct held {
    uint64_t sid;
    uint8_t bytes[8];
    size_t len;
    bool fin;
} held_t;

static moq_transport_result_t counted_stop(void *ctx, uint64_t sid, uint64_t code)
{
    ++stop_attempts;
    return fake_stop(ctx, sid, code);
}

/* Observations only: deliberately independent of the new selector and its
 * outstanding-reservation representation, so this file also links against
 * the core transaction plus the pre-integration bridge. */
static const bridge_stream_entry_t *map(const fixture_t *f, uint64_t sid)
{
    for (uint32_t i = 0; i < f->p.cb->max_streams; ++i)
        if (f->p.cb->streams[i].active && f->p.cb->streams[i].transport_id == sid)
            return &f->p.cb->streams[i];
    return NULL;
}
static const moq_rx_stream_t *rx(const fixture_t *f, uint64_t sid)
{
    moq_stream_ref_t ref = moq_transport_bridge_find_ref(f->p.cb, sid);
    for (size_t i = 0; i < f->p.c->rx_cap; ++i)
        if (f->p.c->rx_streams[i].active &&
            f->p.c->rx_streams[i].stream_ref._v == ref._v)
            return &f->p.c->rx_streams[i];
    return NULL;
}
static const bridge_hold_t *hold(const fixture_t *f, uint64_t sid)
{
    for (uint32_t i = 0; i < f->p.cb->max_holds; ++i)
        if (f->p.cb->holds[i].state != BRIDGE_HOLD_FREE &&
            f->p.cb->holds[i].transport_id == sid) return &f->p.cb->holds[i];
    return NULL;
}
static size_t occupied(const fixture_t *f)
{
    size_t n = 0;
    for (size_t i = 0; i < f->p.c->rx_cap; ++i)
        n += f->p.c->rx_streams[i].active ? 1u : 0u;
    return n;
}
static uint64_t processed(const fixture_t *f)
{
    int slot = test_sub_resolve_handle(f->p.c, f->sub);
    if (!CHECK(slot >= 0)) return UINT64_MAX;
    return f->p.c->subs[slot].processed_stream_count;
}
static void live(const fixture_t *f)
{
    int slot = test_sub_resolve_handle(f->p.c, f->sub);
    CHECK(slot >= 0);
    if (slot >= 0) {
        const moq_sub_entry_t *s = &f->p.c->subs[slot];
        CHECK(s->state == MOQ_SUB_ESTABLISHED && !s->cancel_pending && !s->rs.lifetime.cancelled);
    }
    CHECK(moq_session_state(f->p.c) == MOQ_SESS_ESTABLISHED);
    CHECK(!moq_transport_bridge_is_terminal(f->p.cb));
}
static size_t stops(const fixture_t *f, uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < f->p.cep.count; ++i) {
        const fake_op_t *o = &f->p.cep.ops[i];
        if (o->kind == FAKE_OP_STOP && o->stream_id == sid) {
            CHECK(o->error_code == (f->p.ver == MOQ_VERSION_DRAFT_16 ? 1u : 9u));
            ++n;
        }
    }
    return n;
}
static void no_events(fixture_t *f)
{
    moq_event_t e;
    while (moq_session_poll_events(f->p.c, &e, 1)) {
        CHECK(false);
        fprintf(stderr, "  unexpected event kind=%u\n", (unsigned)e.kind);
        moq_event_cleanup(&e);
    }
}
static bool chunk(fixture_t *f, uint64_t group, bool begin, bool end,
                  moq_object_terminal_t terminal, int byte)
{
    moq_event_t e = {0};
    size_t n = moq_session_poll_events(f->p.c, &e, 1);
    bool ok = CHECK(n == 1 && e.kind == MOQ_EVENT_OBJECT_CHUNK);
    if (n && e.kind == MOQ_EVENT_OBJECT_CHUNK) {
        const moq_object_chunk_event_t *c = &e.u.object_chunk;
        ok &= CHECK(moq_subscription_eq(c->sub, f->sub) && !moq_publication_is_valid(c->pub));
        ok &= CHECK(c->group_id == group && c->subgroup_id == 0 && c->object_id == 0);
        ok &= CHECK(c->begin == begin && c->end == end);
        ok &= CHECK(c->terminal == terminal && c->error_code == 0);
        if (byte < 0) ok &= CHECK(c->chunk == NULL && c->properties == NULL);
        else {
            ok &= CHECK(c->chunk && moq_rcbuf_len(c->chunk) == 1);
            if (c->chunk && moq_rcbuf_len(c->chunk) == 1)
                ok &= CHECK(moq_rcbuf_data(c->chunk)[0] == (uint8_t)byte);
        }
        if (begin) ok &= CHECK(c->payload_length == (end ? 1u : 4u));
    }
    moq_event_cleanup(&e);
    return ok;
}
static bool finished(fixture_t *f, uint64_t group)
{
    moq_event_t e = {0};
    size_t n = moq_session_poll_events(f->p.c, &e, 1);
    bool ok = CHECK(n == 1 && e.kind == MOQ_EVENT_SUBGROUP_FINISHED);
    if (n && e.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
        const moq_subgroup_finished_event_t *g = &e.u.subgroup_finished;
        ok &= CHECK(moq_subscription_eq(g->sub, f->sub) && !moq_publication_is_valid(g->pub));
        ok &= CHECK(g->group_id == group && g->subgroup_id == 0);
        ok &= CHECK(g->has_last_object && g->last_object_id == 0);
    }
    moq_event_cleanup(&e);
    return ok;
}
static bool step(fixture_t *f)
{
    bool ok = CHECK(moq_transport_bridge_service(f->p.cb, ++f->p.now) == MOQ_OK);
    ok &= CHECK(!moq_transport_bridge_is_terminal(f->p.cb));
    ok &= CHECK(!f->p.cep.overflowed && !f->p.cep.truncated);
    return ok;
}

static void create_mode(fixture_t *f, moq_version_t ver, uint32_t events,
                        uint32_t receivers, uint32_t maps, uint32_t pending,
                        bool streaming)
{
    memset(f, 0, sizeof(*f));
    rl_pair_t *p = &f->p;
    p->ver = ver; p->now = 1000;
    p->client_control_sid = p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas); p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = ver;
    cc.streaming_objects = streaming;
    cc.max_events = events; cc.max_actions = 2; sc.max_actions = 16;
    cc.max_data_streams = receivers; cc.max_subscriptions = 8;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    NEED(moq_session_create(&cc, p->now, &p->c) == MOQ_OK);
    NEED(moq_session_create(&sc, p->now, &p->s) == MOQ_OK);
    NEED(test_bind_if_required(p->c) == MOQ_OK && test_bind_if_required(p->s) == MOQ_OK);
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT | MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->cep.vtable.stop_sending = counted_stop;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    bc.max_streams = maps; bc.max_pending = pending;
    NEED(moq_transport_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) == MOQ_OK);
    moq_transport_bridge_cfg_init(&bc, &p->salloc);
    bc.max_streams = 12; bc.max_pending = 4;
    NEED(moq_transport_bridge_create(&bc, p->s, &p->sep.vtable, &p->sep, &p->sb) == MOQ_OK);
    NEED(moq_session_start(p->c, p->now) == MOQ_OK);
    if (moq_session_uses_uni_control(p->s)) NEED(moq_session_start(p->s, p->now) == MOQ_OK);
    shuttle(p);
    NEED(!p->deliver_error && moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
         moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    obs_t setup = {0}; drain(p->c, &setup); drain(p->s, &setup);
    NEED(setup.setup_complete == 2 && setup.closed == 0);

    moq_bytes_t ns = MOQ_BYTES_LITERAL("pressure");
    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){.parts = &ns, .count = 1};
    cfg.track_name = MOQ_BYTES_LITERAL("live");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    NEED(moq_session_subscribe(p->c, &cfg, ++p->now, &f->sub) == MOQ_OK);
    shuttle(p);
    moq_event_t e = {0};
    NEED(moq_session_poll_events(p->s, &e, 1) == 1 && e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t server_sub = e.u.subscribe_request.sub;
    moq_event_cleanup(&e);
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    NEED(moq_session_accept_subscribe(p->s, server_sub, &ac, ++p->now) == MOQ_OK);
    shuttle(p);
    NEED(moq_session_poll_events(p->c, &e, 1) == 1 && e.kind == MOQ_EVENT_SUBSCRIBE_OK);
    NEED(moq_subscription_eq(e.u.subscribe_ok.sub, f->sub) && e.u.subscribe_ok.track_alias < 128);
    f->alias = (uint8_t)e.u.subscribe_ok.track_alias;
    moq_event_cleanup(&e);
    NEED(!p->deliver_error && p->cin.count == 0 && p->sin.count == 0);
    NEED(occupied(f) == 0 && p->c->action_head == p->c->action_tail);
    NEED(moq_session_poll_events(p->s, &e, 1) == 0);
    no_events(f);
    NEED(!p->cep.overflowed && !p->cep.truncated && !p->sep.overflowed && !p->sep.truncated);
    fake_endpoint_clear_ops(&p->cep); fake_endpoint_clear_ops(&p->sep);
    stop_attempts = 0;
}
static void create(fixture_t *f, moq_version_t ver, uint32_t events,
                   uint32_t receivers, uint32_t maps, uint32_t pending)
{
    create_mode(f, ver, events, receivers, maps, pending, true);
}
static void destroy_expecting_close(fixture_t *f, bool closed)
{
    CHECK(!f->p.cep.overflowed && !f->p.cep.truncated);
    CHECK(fake_endpoint_count_kind(&f->p.cep, FAKE_OP_CLOSE) == (closed ? 1u : 0u));
    pair_close(&f->p);
    ++rows;
}
static void destroy(fixture_t *f)
{
    destroy_expecting_close(f, false);
}

static void start(fixture_t *f, uint64_t sid, uint8_t group, uint8_t priority,
                  bool partial)
{
    uint8_t wire[] = {0x10, f->alias, group, priority, 0, 4, 'A'};
    NEED(moq_transport_bridge_on_peer_uni_bytes(f->p.cb, sid, wire,
         partial ? sizeof(wire) : 4, false, ++f->p.now) == MOQ_OK);
    NEED(rx(f, sid) && rx(f, sid)->group_id == group);
    NEED(moq_subscription_eq(rx(f, sid)->sub, f->sub));
    NEED(rx(f, sid)->parse_state == (partial ? MOQ_RX_STREAMING_PAYLOAD : MOQ_RX_AWAITING_OBJECT));
}
static held_t wait_data(fixture_t *f, uint64_t sid, uint8_t group, uint8_t byte)
{
    held_t h = {.sid = sid, .bytes = {0x10, f->alias, group, 10, 0, 1, byte},
                .len = 7, .fin = true};
    NEED(!moq_session_can_admit_data_stream(f->p.c));
    NEED(moq_transport_bridge_on_peer_uni_bytes(f->p.cb, sid, h.bytes, h.len,
         h.fin, ++f->p.now) == MOQ_ERR_INPUT_NOT_CONSUMED);
    NEED(hold(f, sid) && !hold(f, sid)->offered && rx(f, sid) == NULL);
    NEED(moq_transport_bridge_stream_has_pending(f->p.cb, sid));
    return h;
}
static bool replay(fixture_t *f, const held_t *h, uint8_t group, uint8_t byte)
{
    if (!CHECK(hold(f, h->sid) && hold(f, h->sid)->offered)) return false;
    if (!CHECK(!moq_transport_bridge_stream_has_pending(f->p.cb, h->sid))) return false;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(f->p.cb, h->sid,
                         h->bytes, h->len, h->fin, ++f->p.now);
    if (!CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK)) return false;
    if (rc == MOQ_ERR_WOULD_BLOCK &&
        !CHECK(rx(f, h->sid) && map(f, h->sid) && map(f, h->sid)->pending_retry)) return false;
    bool ok = chunk(f, group, true, true, MOQ_OBJECT_TERMINAL_NORMAL, byte);
    /* With one event slot the real FIN can be retained behind the chunk. */
    ok &= step(f);
    ok &= finished(f, group);
    ok &= CHECK(hold(f, h->sid) == NULL && rx(f, h->sid) == NULL);
    return ok;
}
static bool sibling(fixture_t *f, uint64_t sid, uint8_t group)
{
    const uint8_t wire[] = {0, 1, 'S'};
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(f->p.cb, sid, wire,
                         sizeof(wire), true, ++f->p.now);
    if (!CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK)) return false;
    if (rc == MOQ_ERR_WOULD_BLOCK &&
        !CHECK(rx(f, sid) && map(f, sid) && map(f, sid)->pending_retry)) return false;
    bool ok = chunk(f, group, true, true, MOQ_OBJECT_TERMINAL_NORMAL, 'S');
    ok &= step(f); ok &= finished(f, group);
    ok &= CHECK(rx(f, sid) == NULL && stops(f, sid) == 0);
    return ok;
}
static bool stopped(fixture_t *f, uint64_t sid, moq_stream_ref_t ref)
{
    const moq_rx_stream_t *r = rx(f, sid);
    return CHECK(r && r->parse_state == MOQ_RX_STOPPED && r->stream_ref._v == ref._v &&
                 moq_subscription_eq(r->sub, f->sub) && !r->binding_ended);
}
static void provider_final(fixture_t *f, uint64_t sid)
{
    CHECK(moq_transport_bridge_on_peer_stream_terminal(f->p.cb, sid, ++f->p.now) == MOQ_OK);
}

static void lifecycle(moq_version_t ver)
{
    char name[64]; (void)snprintf(name, sizeof(name), "d%u.lifecycle", (unsigned)ver);
    row_name = name;
    fixture_t f; create(&f, ver, 4, 2, 8, 3);
    start(&f, 9000, 10, 200, true); start(&f, 8000, 11, 50, false);
    NEED(chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A'));
    moq_stream_ref_t target = rx(&f, 9000)->stream_ref;
    moq_stream_ref_t sib = rx(&f, 8000)->stream_ref;
    held_t h = wait_data(&f, 9100, 12, 'W');
    f.p.cep.block_stop = true;
    if (!step(&f) || !chunk(&f, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, -1)) goto out;
    CHECK(stop_attempts > 0 && stops(&f, 9000) == 0);
    for (unsigned i = 0; i < 3; ++i) {
        step(&f); no_events(&f); stopped(&f, 9000, target);
        CHECK(occupied(&f) == 2 && processed(&f) == 0);
        CHECK(hold(&f, h.sid) && !hold(&f, h.sid)->offered);
        CHECK(rx(&f, 8000) && rx(&f, 8000)->stream_ref._v == sib._v);
        CHECK(stops(&f, 8000) == 0);
    }
    f.p.cep.block_stop = false;
    for (unsigned i = 0; i < 3; ++i) step(&f);
    CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 0);
    CHECK(map(&f, 9000) && map(&f, 9000)->local_stop_accepted && !map(&f, 9000)->transport_final);
    stopped(&f, 9000, target); CHECK(occupied(&f) == 2 && processed(&f) == 0);
    CHECK(hold(&f, h.sid) && !hold(&f, h.sid)->offered); no_events(&f); live(&f);
    provider_final(&f, 9000);
    CHECK(rx(&f, 9000) != NULL); /* notification only latches; service retires */
    step(&f);
    CHECK(rx(&f, 9000) == NULL && map(&f, 9000) == NULL && processed(&f) == 1);
    no_events(&f);
    if (!replay(&f, &h, 12, 'W')) goto out;
    CHECK(processed(&f) == 2);
    provider_final(&f, 9000); step(&f); CHECK(processed(&f) == 2);
    CHECK(rx(&f, 8000) && rx(&f, 8000)->stream_ref._v == sib._v);
    sibling(&f, 8000, 11); CHECK(processed(&f) == 3); live(&f); no_events(&f);
    CHECK(fake_endpoint_count_kind(&f.p.cep, FAKE_OP_RESET) == 0);
out:
    destroy(&f);
}

static void two_demands(moq_version_t ver)
{
    char name[64]; (void)snprintf(name, sizeof(name), "d%u.two-demands", (unsigned)ver);
    row_name = name;
    fixture_t f; create(&f, ver, 4, 3, 10, 3);
    /* Equal effective priorities: assert the actual full ref tie, not age or
     * transport ID order. Transport IDs intentionally descend. */
    start(&f, 9000, 10, 200, true);
    NEED(chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A'));
    start(&f, 8000, 11, 200, true);
    NEED(chunk(&f, 11, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A'));
    start(&f, 7000, 12, 50, false);
    moq_stream_ref_t a = rx(&f, 9000)->stream_ref, b = rx(&f, 8000)->stream_ref;
    NEED(a._v < b._v);
    held_t first = wait_data(&f, 9900, 20, 'O');
    held_t second = wait_data(&f, 8800, 21, 'N');
    NEED(hold(&f, first.sid)->seq < hold(&f, second.sid)->seq);
    if (!step(&f) || !chunk(&f, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, -1)) goto out;
    CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 0); no_events(&f);
    stopped(&f, 9000, a);
    CHECK(rx(&f, 8000) && rx(&f, 8000)->parse_state == MOQ_RX_STREAMING_PAYLOAD);
    if (!step(&f) || !chunk(&f, 11, false, true, MOQ_OBJECT_TERMINAL_STOP, -1)) goto out;
    stopped(&f, 8000, b);
    for (unsigned i = 0; i < 3; ++i) step(&f);
    CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 1 && stops(&f, 7000) == 0);
    CHECK(occupied(&f) == 3 && processed(&f) == 0); no_events(&f);
    /* A genuine peer RESET may retire the first stopped stream; it must not
     * create a second object terminal or turn its earlier STOP into RESET. */
    CHECK(moq_transport_bridge_on_peer_stream_reset(f.p.cb, 9000, 0, ++f.p.now) == MOQ_OK);
    step(&f); no_events(&f); CHECK(processed(&f) == 1);
    CHECK(hold(&f, first.sid) && hold(&f, first.sid)->offered);
    CHECK(hold(&f, second.sid) && !hold(&f, second.sid)->offered);
    if (!replay(&f, &first, 20, 'O')) goto out;
    /* first's FIN frees its slot, so second can now be offered without a
     * third cancellation, even though the other stopped RX still occupies. */
    step(&f); stopped(&f, 8000, b);
    if (!replay(&f, &second, 21, 'N')) goto out;
    CHECK(stops(&f, 7000) == 0); CHECK(processed(&f) == 3);
    provider_final(&f, 8000); step(&f); CHECK(processed(&f) == 4);
    sibling(&f, 7000, 12); CHECK(processed(&f) == 5); live(&f); no_events(&f);
out:
    destroy(&f);
}

static void demand_changes(bool alternate)
{
    row_name = alternate ? "d22.alternate-slot" : "d22.dropped-demand";
    fixture_t f; create(&f, MOQ_VERSION_DRAFT_22, 4, 2, 8, 3);
    start(&f, 9000, 10, 200, true); start(&f, 8000, 11, 50, false);
    NEED(chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A'));
    moq_stream_ref_t ref = rx(&f, 9000)->stream_ref;
    held_t h = wait_data(&f, 9100, 12, 'W');
    if (!step(&f) || !chunk(&f, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, -1)) goto out;
    if (alternate) {
        if (!sibling(&f, 8000, 11) || !replay(&f, &h, 12, 'W')) goto out;
        CHECK(processed(&f) == 2);
    } else {
        CHECK(moq_transport_bridge_on_peer_stream_reset(f.p.cb, h.sid, 0, ++f.p.now) == MOQ_OK);
        CHECK(hold(&f, h.sid) == NULL && rx(&f, h.sid) == NULL);
        CHECK(processed(&f) == 0);
    }
    for (unsigned i = 0; i < 4; ++i) { step(&f); no_events(&f); }
    stopped(&f, 9000, ref);
    CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 0); live(&f);
    provider_final(&f, 9000); step(&f);
    CHECK(processed(&f) == (alternate ? 3u : 1u)); no_events(&f);
out:
    destroy(&f);
}

static void non_demand(unsigned which)
{
    static const char *names[] = {"d22.request-only", "d22.map-only", "d22.unknown-type"};
    row_name = names[which];
    fixture_t f; create(&f, MOQ_VERSION_DRAFT_22, 4, 2, which == 2 ? 8 : 6, 3);
    start(&f, 9000, 10, 200, true); start(&f, 8000, 11, 50, false);
    NEED(chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A'));
    moq_stream_ref_t ref = rx(&f, 9000)->stream_ref;
    if (which < 2) {
        /* Actual fragmented peer request, not an injected map entry. */
        const uint8_t type = 3;
        NEED(moq_transport_bridge_on_peer_bidi_bytes(f.p.cb, 4001, &type, 1,
                                                     false, ++f.p.now) == MOQ_OK);
        NEED(moq_transport_bridge_stream_count(f.p.cb) == 6);
        if (which == 0) {
            NEED(moq_transport_bridge_on_peer_bidi_bytes(f.p.cb, 4002, &type, 1,
                 false, ++f.p.now) == MOQ_ERR_INPUT_NOT_CONSUMED);
            NEED(hold(&f, 4002) && hold(&f, 4002)->kind == BRIDGE_STREAM_BIDI);
        } else {
            (void)wait_data(&f, 9100, 12, 'W');
            NEED(hold(&f, 9100) && !hold(&f, 9100)->mapped);
        }
    } else {
        /* Incomplete MoQ vi64: no known DATA type and hence no RX demand. */
        const uint8_t prefix = 0x80;
        NEED(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 9100, &prefix, 1,
                                                     false, ++f.p.now) == MOQ_OK);
        NEED(map(&f, 9100) && map(&f, 9100)->uni_disp == BRIDGE_UNI_DISP_PENDING);
        NEED(rx(&f, 9100) == NULL);
    }
    for (unsigned i = 0; i < 4; ++i) { step(&f); no_events(&f); }
    CHECK(stop_attempts == 0 && fake_endpoint_count_kind(&f.p.cep, FAKE_OP_STOP) == 0);
    CHECK(rx(&f, 9000) && rx(&f, 9000)->stream_ref._v == ref._v &&
          rx(&f, 9000)->parse_state == MOQ_RX_STREAMING_PAYLOAD);
    CHECK(rx(&f, 8000) && rx(&f, 8000)->parse_state == MOQ_RX_AWAITING_OBJECT);
    CHECK(occupied(&f) == 2 && processed(&f) == 0); live(&f);
    destroy(&f);
}

static void extra_request(fixture_t *f, const char *name)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("pressure");
    moq_subscribe_cfg_t c; moq_subscribe_cfg_init(&c);
    c.track_namespace = (moq_namespace_t){.parts = &ns, .count = 1};
    c.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub;
    NEED(moq_session_subscribe(f->p.c, &c, ++f->p.now, &sub) == MOQ_OK);
}
static void ring_pressure(bool actions)
{
    row_name = actions ? "d22.action-full" : "d22.event-full";
    fixture_t f; create(&f, MOQ_VERSION_DRAFT_22, actions ? 4 : 1, 2, 10, 1);
    if (actions) {
        f.p.cep.block_open_bidi = true;
        extra_request(&f, "blocked-open"); NEED(step(&f));
        NEED(f.p.cb->pending_count == 1 && f.p.c->action_head == f.p.c->action_tail);
    }
    start(&f, 9000, 10, 200, true); start(&f, 8000, 11, 50, false);
    moq_stream_ref_t ref = rx(&f, 9000)->stream_ref;
    (void)wait_data(&f, 9100, 12, 'W');
    if (actions) { extra_request(&f, "queued-1"); extra_request(&f, "queued-2"); }
    size_t ah = f.p.c->action_head, at = f.p.c->action_tail;
    size_t eh = f.p.c->event_head, et = f.p.c->event_tail;
    NEED(actions ? at - ah == f.p.c->action_cap : et - eh == f.p.c->event_cap);
    uint8_t before[sizeof(moq_rx_stream_t)];
    memcpy(before, rx(&f, 9000), sizeof(before));
    for (unsigned i = 0; i < 3; ++i) {
        step(&f);
        CHECK(stop_attempts == 0);
        CHECK(rx(&f, 9000) && memcmp(before, rx(&f, 9000), sizeof(before)) == 0);
        CHECK(rx(&f, 8000) && rx(&f, 8000)->parse_state == MOQ_RX_AWAITING_OBJECT);
        CHECK(f.p.c->action_head == ah && f.p.c->action_tail == at);
        CHECK(f.p.c->event_head == eh && f.p.c->event_tail == et);
        CHECK(processed(&f) == 0 && hold(&f, 9100) && !hold(&f, 9100)->offered);
    }
    if (actions) {
        /* Queued (unpolled) BEGIN is already visible ownership. Credit comes
         * from real endpoint acceptance, never popping a fabricated action. */
        f.p.cep.block_open_bidi = false;
        step(&f);
        if (!chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A')) goto out;
    } else {
        if (!chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A')) goto out;
        step(&f);
    }
    if (!chunk(&f, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, -1)) goto out;
    stopped(&f, 9000, ref);
    CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 0);
    CHECK(occupied(&f) == 2 && processed(&f) == 0); no_events(&f); live(&f);
out:
    destroy(&f);
}

static void retained_first_cause(void)
{
    row_name = "d22.retained-reset-zero-delivery";
    fixture_t f; create(&f, MOQ_VERSION_DRAFT_22, 1, 2, 8, 3);
    start(&f, 9000, 10, 200, true); start(&f, 8000, 11, 50, false);
    moq_stream_ref_t idle_ref = rx(&f, 8000)->stream_ref;
    held_t h = wait_data(&f, 9100, 12, 'W');
    const uint8_t next = 'B';
    NEED(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 9000, &next, 1,
         false, ++f.p.now) == MOQ_ERR_WOULD_BLOCK);
    NEED(map(&f, 9000) && map(&f, 9000)->pending_retry);
    NEED(moq_transport_bridge_on_peer_stream_reset(f.p.cb, 9000, 0, ++f.p.now) == MOQ_ERR_WOULD_BLOCK);
    NEED(map(&f, 9000)->pending_reset && map(&f, 9000)->pending_reset_code == 0);
    NEED(moq_transport_bridge_on_peer_stream_reset(f.p.cb, 9000, 9, ++f.p.now) == MOQ_ERR_WOULD_BLOCK);
    for (unsigned i = 0; i < 3; ++i) step(&f);
    /* Prior work excludes 9000 before selection; it does not freeze pressure
     * on eligible idle 8000. stops() also checks the accepted operation's
     * D22 EXCESSIVE_LOAD code (9). */
    CHECK(stops(&f, 9000) == 0 && stops(&f, 8000) == 1);
    CHECK(stop_attempts == 1 && fake_endpoint_count_kind(&f.p.cep, FAKE_OP_STOP) == 1);
    stopped(&f, 8000, idle_ref);
    CHECK(map(&f, 8000) && map(&f, 8000)->local_stop_accepted && !map(&f, 8000)->transport_final);
    CHECK(processed(&f) == 0 && occupied(&f) == 2);
    CHECK(map(&f, 9000) && map(&f, 9000)->pending_reset && map(&f, 9000)->pending_reset_code == 0);
    if (!chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A')) goto out;
    step(&f);
    if (!chunk(&f, 10, false, false, MOQ_OBJECT_TERMINAL_NORMAL, 'B')) goto out;
    CHECK(stops(&f, 9000) == 0 && stops(&f, 8000) == 1);
    stopped(&f, 8000, idle_ref); CHECK(processed(&f) == 0);
    step(&f);
    if (!chunk(&f, 10, false, true, MOQ_OBJECT_TERMINAL_RESET, -1)) goto out;
    step(&f);
    CHECK(stops(&f, 9000) == 0 && stops(&f, 8000) == 1);
    CHECK(rx(&f, 9000) == NULL && processed(&f) == 1);
    stopped(&f, 8000, idle_ref); CHECK(occupied(&f) == 1);
    no_events(&f);
    if (!replay(&f, &h, 12, 'W')) goto out;
    CHECK(stops(&f, 9000) == 0 && stops(&f, 8000) == 1);
    CHECK(stop_attempts == 1 && processed(&f) == 2);
    stopped(&f, 8000, idle_ref); CHECK(occupied(&f) == 1);
    live(&f); no_events(&f);
out:
    destroy(&f);
}

static size_t op_code(const fixture_t *f, fake_op_kind_t kind,
                      uint64_t sid, uint64_t code)
{
    size_t n = 0;
    for (size_t i = 0; i < f->p.cep.count; ++i) {
        const fake_op_t *o = &f->p.cep.ops[i];
        if (o->kind == kind && o->stream_id == sid && o->error_code == code) ++n;
    }
    return n;
}

/* Unlike a partial streaming object, this unfinished whole object needs no
 * event slot for pressure STOP. Thus event saturation alone cannot mask a
 * missing guard for the owning request's retained RESET. */
static void retained_request_terminal(void)
{
    row_name = "d22.request-reset-before-whole-object-pressure";
    fixture_t f; create_mode(&f, MOQ_VERSION_DRAFT_22, 1, 1, 8, 3, false);
    int slot = test_sub_resolve_handle(f.p.c, f.sub);
    NEED(slot >= 0);
    moq_stream_ref_t request = f.p.c->subs[slot].rs.ref;
    uint64_t request_id = UINT64_MAX;
    for (uint32_t i = 0; i < f.p.cb->max_streams; ++i) {
        const bridge_stream_entry_t *m = &f.p.cb->streams[i];
        if (m->active && m->ref._v == request._v) request_id = m->transport_id;
    }
    NEED(request._v && request_id != UINT64_MAX && map(&f, request_id));
    NEED(map(&f, request_id)->kind == BRIDGE_STREAM_BIDI);

    const uint8_t partial[] = {0x10, f.alias, 10, 200, 0, 4, 'A'};
    NEED(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 9000, partial,
         sizeof(partial), false, ++f.p.now) == MOQ_OK);
    NEED(rx(&f, 9000) && rx(&f, 9000)->parse_state == MOQ_RX_READING_PAYLOAD);
    NEED(moq_subscription_eq(rx(&f, 9000)->sub, f.sub));
    NEED(f.p.c->event_head == f.p.c->event_tail);
    moq_stream_ref_t data_ref = rx(&f, 9000)->stream_ref;
    /* A real datagram fills the sole event slot. It is unrelated object 30/0,
     * so neither a fabricated event nor the partial object's own terminal. */
    const uint8_t datagram[] = {0, f.alias, 30, 0, 100, 'F'};
    NEED(moq_transport_bridge_on_peer_datagram(f.p.cb, datagram,
         sizeof(datagram), ++f.p.now) == MOQ_OK);
    NEED(f.p.c->event_tail - f.p.c->event_head == f.p.c->event_cap);
    (void)wait_data(&f, 9100, 12, 'W');
    NEED(moq_transport_bridge_on_peer_stream_reset(f.p.cb, request_id, 0,
         ++f.p.now) == MOQ_ERR_WOULD_BLOCK);
    NEED(map(&f, request_id) && map(&f, request_id)->pending_reset &&
         map(&f, request_id)->pending_reset_code == 0);
    NEED(f.p.c->action_head == f.p.c->action_tail && f.p.cb->pending_count == 0);
    NEED(rx(&f, 9000) && rx(&f, 9000)->parse_state == MOQ_RX_READING_PAYLOAD);
    uint8_t before[sizeof(moq_rx_stream_t)];
    memcpy(before, rx(&f, 9000), sizeof(before));
    size_t eh = f.p.c->event_head, et = f.p.c->event_tail;
    for (unsigned i = 0; i < 3; ++i) {
        step(&f);
        CHECK(stop_attempts == 0 && fake_endpoint_count_kind(&f.p.cep, FAKE_OP_STOP) == 0);
        CHECK(rx(&f, 9000) && memcmp(before, rx(&f, 9000), sizeof(before)) == 0);
        const moq_rx_stream_t *r = rx(&f, 9000);
        CHECK(r && r->stream_ref._v == data_ref._v && r->payload_written == 1);
        if (r && r->payload_rcbuf) {
            CHECK(moq_rcbuf_refcount(r->payload_rcbuf) == 1);
            CHECK(moq_rcbuf_data(r->payload_rcbuf)[0] == 'A');
        } else CHECK(false);
        CHECK(map(&f, request_id) && map(&f, request_id)->pending_reset &&
              map(&f, request_id)->pending_reset_code == 0);
        CHECK(f.p.c->event_head == eh && f.p.c->event_tail == et);
        CHECK(f.p.c->action_head == f.p.c->action_tail);
        CHECK(hold(&f, 9100) && !hold(&f, 9100)->offered && occupied(&f) == 1);
        CHECK(processed(&f) == 0); live(&f);
    }

    moq_event_t e = {0};
    size_t n = moq_session_poll_events(f.p.c, &e, 1);
    bool filler = CHECK(n == 1 && e.kind == MOQ_EVENT_OBJECT_RECEIVED);
    if (n && e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
        const moq_object_received_event_t *o = &e.u.object_received;
        filler &= CHECK(moq_subscription_eq(o->sub, f.sub) && o->group_id == 30 && o->object_id == 0);
        filler &= CHECK(o->payload && moq_rcbuf_len(o->payload) == 1);
        if (o->payload && moq_rcbuf_len(o->payload) == 1)
            filler &= CHECK(moq_rcbuf_data(o->payload)[0] == 'F');
    }
    moq_event_cleanup(&e);
    if (!filler) goto out;
    step(&f);
    n = moq_session_poll_events(f.p.c, &e, 1);
    CHECK(n == 1 && e.kind == MOQ_EVENT_UNSUBSCRIBED);
    if (n && e.kind == MOQ_EVENT_UNSUBSCRIBED) {
        CHECK(moq_subscription_eq(e.u.unsubscribed.sub, f.sub));
        CHECK(e.u.unsubscribed.cause == MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM);
        CHECK(e.u.unsubscribed.stream_error_code == 0);
    }
    moq_event_cleanup(&e);
    step(&f);
    CHECK(test_sub_resolve_handle(f.p.c, f.sub) < 0);
    CHECK(rx(&f, 9000) == NULL && occupied(&f) == 0);
    /* Ordinary owner cleanup carries CANCELLED (1), never pressure code 9.
     * Leave the held DATA adapter-owned; do not replay into the ended owner. */
    CHECK(op_code(&f, FAKE_OP_STOP, 9000, 1) == 1);
    CHECK(op_code(&f, FAKE_OP_STOP, 9000, 9) == 0);
    CHECK(fake_endpoint_count_kind(&f.p.cep, FAKE_OP_STOP) == 1);
    CHECK(hold(&f, 9100) && hold(&f, 9100)->offered);
    CHECK(!map(&f, request_id) || !map(&f, request_id)->pending_reset);
    no_events(&f);
out:
    destroy(&f);
}

static moq_result_t type_input(fixture_t *f, const uint8_t *type, moq_rcbuf_t *buffer)
{
    return buffer
        ? moq_transport_bridge_on_peer_uni_rcbuf(f->p.cb, 9100, buffer, false, ++f->p.now)
        : moq_transport_bridge_on_peer_uni_bytes(f->p.cb, 9100, type, 1, false, ++f->p.now);
}

static void type_demand(moq_version_t ver, uint8_t type, bool use_rcbuf, bool unmapped)
{
    char name[72];
    (void)snprintf(name, sizeof(name), "d%u.type-%02x.%s%s", (unsigned)ver,
                   (unsigned)type, use_rcbuf ? "rcbuf" : "bytes",
                   unmapped ? ".unmapped" : "");
    row_name = name;
    bool valid = type == 0x10;
    fixture_t f; create(&f, ver, 4, 2, unmapped ? 6 : 8, 3);
    start(&f, 9000, 10, 200, true); start(&f, 8000, 11, 50, false);
    NEED(chunk(&f, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 'A'));
    NEED(!moq_session_can_admit_data_stream(f.p.c));
    moq_stream_ref_t data_ref = rx(&f, 9000)->stream_ref;
    if (unmapped) {
        /* An incomplete unrelated type consumes the last real map slot but
         * no RX slot. Two unmapped holds then distinguish the oldest offered
         * valid type from the still-unoffered reserved type. With only one
         * hold, offering it on map release would mask a bad demand count. */
        const uint8_t prefix = 0x80, known = 0x10;
        NEED(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 7000, &prefix, 1,
             false, ++f.p.now) == MOQ_OK);
        NEED(map(&f, 7000) && map(&f, 7000)->uni_disp == BRIDGE_UNI_DISP_PENDING);
        NEED(rx(&f, 7000) == NULL && moq_transport_bridge_stream_count(f.p.cb) == 6);
        NEED(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 9200, &known, 1,
             false, ++f.p.now) == MOQ_ERR_INPUT_NOT_CONSUMED);
        NEED(hold(&f, 9200) && !hold(&f, 9200)->mapped && !hold(&f, 9200)->offered);
    }
    moq_rcbuf_t *buffer = NULL;
    if (use_rcbuf) NEED(moq_rcbuf_create(&f.p.calloc, &type, 1, &buffer) == MOQ_OK);
    moq_result_t rc = type_input(&f, &type, buffer);
    if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) {
        /* Immediate parsing of an invalid type is also lawful. The closure
         * oracle below requires the original protocol error, not merely an
         * arbitrary transport error, close, or silent discard. */
        if (valid) { CHECK(false); goto out; }
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_PROTO);
        goto protocol_error;
    }
    NEED(hold(&f, 9100) && hold(&f, 9100)->mapped == !unmapped && !hold(&f, 9100)->offered);
    NEED(rx(&f, 9100) == NULL);
    if (buffer) NEED(moq_rcbuf_refcount(buffer) == 1);
    if (unmapped) {
        NEED(hold(&f, 9200) && hold(&f, 9200)->seq < hold(&f, 9100)->seq);
        rc = moq_transport_bridge_service(f.p.cb, ++f.p.now);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_PROTO);
        CHECK(stop_attempts == 0);
        if (moq_session_state(f.p.c) == MOQ_SESS_CLOSED ||
            moq_transport_bridge_is_terminal(f.p.cb)) goto protocol_error;
        no_events(&f);
        CHECK(hold(&f, 9200) && !hold(&f, 9200)->offered);
        CHECK(hold(&f, 9100) && !hold(&f, 9100)->offered);
        /* This RESET frees only map capacity. There is no request terminal
         * debt and neither of the two admitted RX owners is released. */
        NEED(moq_transport_bridge_on_peer_stream_reset(f.p.cb, 7000, 0, ++f.p.now) == MOQ_OK);
        NEED(map(&f, 7000) == NULL && moq_transport_bridge_stream_count(f.p.cb) == 5);
        NEED(occupied(&f) == 2 && !moq_session_can_admit_data_stream(f.p.c));
    }
    if (valid) {
        /* The complete type alone is enough: no header/alias was admitted. */
        if (!step(&f) || !chunk(&f, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, -1)) goto out;
        CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 0);
        for (unsigned i = 0; i < 3; ++i) { step(&f); no_events(&f); }
        stopped(&f, 9000, data_ref);
        CHECK(occupied(&f) == 2 && processed(&f) == 0);
        CHECK(hold(&f, 9100) && !hold(&f, 9100)->offered);
        CHECK(stops(&f, 9000) == 1 && stops(&f, 8000) == 0);
        if (buffer) CHECK(moq_rcbuf_refcount(buffer) == 1);
        provider_final(&f, 9000); step(&f);
        CHECK(processed(&f) == 1);
        if (!CHECK(hold(&f, 9100) && hold(&f, 9100)->offered)) goto out;
        if (!CHECK(type_input(&f, &type, buffer) == MOQ_OK)) goto out;
        CHECK(rx(&f, 9100) && rx(&f, 9100)->parse_state == MOQ_RX_AWAITING_HEADER);
        const uint8_t tail[] = {f.alias, 12, 10, 0, 1, 'V'};
        CHECK(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 9100, tail,
              sizeof(tail), true, ++f.p.now) == MOQ_OK);
        chunk(&f, 12, true, true, MOQ_OBJECT_TERMINAL_NORMAL, 'V');
        finished(&f, 12); CHECK(processed(&f) == 2);
        sibling(&f, 8000, 11); CHECK(processed(&f) == 3);
        live(&f); no_events(&f);
        goto out;
    }

    for (unsigned i = 0; i < 3; ++i) {
        rc = moq_transport_bridge_service(f.p.cb, ++f.p.now);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_PROTO);
        CHECK(stop_attempts == 0 && fake_endpoint_count_kind(&f.p.cep, FAKE_OP_STOP) == 0);
        if (moq_session_state(f.p.c) == MOQ_SESS_CLOSED ||
            moq_transport_bridge_is_terminal(f.p.cb)) goto protocol_error;
        CHECK(rx(&f, 9000) && rx(&f, 9000)->stream_ref._v == data_ref._v &&
              rx(&f, 9000)->parse_state == MOQ_RX_STREAMING_PAYLOAD);
        CHECK(rx(&f, 8000) && rx(&f, 8000)->parse_state == MOQ_RX_AWAITING_OBJECT);
        CHECK(occupied(&f) == 2 && processed(&f) == 0);
        CHECK(hold(&f, 9100) && !hold(&f, 9100)->offered);
        if (unmapped) {
            CHECK(hold(&f, 9200) && hold(&f, 9200)->offered);
            CHECK(hold(&f, 9100) && !hold(&f, 9100)->mapped);
            CHECK(moq_transport_bridge_stream_count(f.p.cb) == 5);
        }
        if (buffer) CHECK(moq_rcbuf_refcount(buffer) == 1);
        no_events(&f); live(&f);
    }
    if (unmapped) {
        /* The already-offered valid callback was never replayed. Drop that
         * separate demand so the reserved callback can be offered next. */
        CHECK(moq_transport_bridge_on_peer_stream_reset(f.p.cb, 9200, 0, ++f.p.now) == MOQ_OK);
        CHECK(hold(&f, 9200) == NULL);
    }
    /* Independent lawful FIN frees RX. Redeliver the SAME one byte, same
     * rcbuf where used, and same non-FIN flag: the invalid peer input has not
     * been repaired or changed to make the protocol-error oracle pass. */
    if (!CHECK(moq_transport_bridge_on_peer_uni_bytes(f.p.cb, 8000, NULL, 0,
               true, ++f.p.now) == MOQ_OK)) goto out;
    {
        moq_event_t e = {0};
        size_t n = moq_session_poll_events(f.p.c, &e, 1);
        CHECK(n == 1 && e.kind == MOQ_EVENT_SUBGROUP_FINISHED);
        if (n && e.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            CHECK(moq_subscription_eq(e.u.subgroup_finished.sub, f.sub));
            CHECK(e.u.subgroup_finished.group_id == 11 && !e.u.subgroup_finished.has_last_object);
        }
        moq_event_cleanup(&e);
    }
    CHECK(processed(&f) == 1 && stop_attempts == 0);
    rc = moq_transport_bridge_service(f.p.cb, ++f.p.now);
    CHECK(rc == MOQ_OK || rc == MOQ_ERR_PROTO);
    if (moq_session_state(f.p.c) == MOQ_SESS_CLOSED ||
        moq_transport_bridge_is_terminal(f.p.cb)) goto protocol_error;
    if (!CHECK(hold(&f, 9100) && hold(&f, 9100)->offered)) goto out;
    rc = type_input(&f, &type, buffer);
    CHECK(rc == MOQ_OK || rc == MOQ_ERR_PROTO);
protocol_error:
    CHECK(moq_session_state(f.p.c) == MOQ_SESS_CLOSED);
    {
        moq_event_t e = {0};
        size_t n = moq_session_poll_events(f.p.c, &e, 1);
        CHECK(n == 1 && e.kind == MOQ_EVENT_SESSION_CLOSED);
        if (n && e.kind == MOQ_EVENT_SESSION_CLOSED) CHECK(e.u.closed.code == 0x3);
        moq_event_cleanup(&e);
    }
    CHECK(moq_transport_bridge_service(f.p.cb, ++f.p.now) == MOQ_OK);
    CHECK(moq_transport_bridge_is_terminal(f.p.cb));
    CHECK(moq_transport_bridge_close_code(f.p.cb) == 0x3);
    CHECK(stop_attempts == 0 && fake_endpoint_count_kind(&f.p.cep, FAKE_OP_STOP) == 0);
    no_events(&f);
out:
    if (buffer) moq_rcbuf_decref(buffer);
    destroy_expecting_close(&f, !valid);
}

int main(int argc, char **argv)
{
    bool all = argc == 1;
    bool life = all || (argc == 2 && strcmp(argv[1], "--lifecycle") == 0);
    bool two = all || (argc == 2 && strcmp(argv[1], "--two-demands") == 0);
    bool demand = all || (argc == 2 && strcmp(argv[1], "--demand") == 0);
    bool rings = all || (argc == 2 && strcmp(argv[1], "--rings") == 0);
    bool causes = all || (argc == 2 && strcmp(argv[1], "--first-cause") == 0);
    bool request = all || (argc == 2 && strcmp(argv[1], "--request-terminal") == 0);
    bool types = all || (argc == 2 && strcmp(argv[1], "--d16-types") == 0);
    bool modern_types = all || (argc == 2 && strcmp(argv[1], "--modern-types") == 0);
    if (!life && !two && !demand && !rings && !causes && !request && !types && !modern_types) {
        fprintf(stderr, "usage: %s [--lifecycle|--two-demands|--demand|--rings|--first-cause|--request-terminal|--d16-types|--modern-types]\n", argv[0]);
        return 2;
    }
    if (life) { lifecycle(MOQ_VERSION_DRAFT_16); lifecycle(MOQ_VERSION_DRAFT_18); lifecycle(MOQ_VERSION_DRAFT_22); }
    if (two) { two_demands(MOQ_VERSION_DRAFT_18); two_demands(MOQ_VERSION_DRAFT_22); }
    if (demand) { demand_changes(false); demand_changes(true); for (unsigned i = 0; i < 3; ++i) non_demand(i); }
    if (rings) { ring_pressure(true); ring_pressure(false); }
    if (causes) retained_first_cause();
    if (request) retained_request_terminal();
    if (types) {
        const uint8_t type[] = {0x00, 0x16, 0x10};
        for (size_t i = 0; i < sizeof(type); ++i) {
            type_demand(MOQ_VERSION_DRAFT_16, type[i], false, false);
            type_demand(MOQ_VERSION_DRAFT_16, type[i], true, false);
        }
    }
    if (modern_types) {
        type_demand(MOQ_VERSION_DRAFT_18, 0x16, false, false);
        type_demand(MOQ_VERSION_DRAFT_18, 0x10, false, false);
        type_demand(MOQ_VERSION_DRAFT_22, 0x16, false, false);
        type_demand(MOQ_VERSION_DRAFT_22, 0x10, false, false);
        type_demand(MOQ_VERSION_DRAFT_22, 0x16, false, true);
    }
    fprintf(stderr, "bridge_receive_pressure: %u rows, %u checks, %d failures\n", rows, checks, failures);
    return failures ? 1 : 0;
}
