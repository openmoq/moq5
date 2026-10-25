/* Public namespace subscribe -> endpoint refusal -> cancel. A CLOSE_BIDI
 * must survive an unmapped retained open and follow that stream's bytes.
 * The mapped rows distinguish dispatch ordering from pending-retry ordering.
 * FETCH cancellation also owes its whole-bidi abort behind an unmapped open.
 * Draft 18 exercises the shared bridge path; no action/map injection, peer
 * acknowledgement, or transport-capability policy is involved.
 */
static int failures;
#include "../support/bridge_wire_pair.h"
#include "../support/held_bridge_driver.h"
#include "test_binding_support.h"

static const char *row_name;
static unsigned checks;
static uint64_t blocked_body_sid = UINT64_MAX;
static bool native_abort;

#define REQUIRE(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "SETUP %s:%d: %s\n", row_name, __LINE__, #x); \
    return false; } } while (0)
#define EXPECT(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL %s:%d: %s\n", row_name, __LINE__, #x); \
    return false; } } while (0)

typedef struct request {
    moq_ns_sub_handle_t handle;
    moq_stream_ref_t ref;
    uint64_t sid;
    uint8_t bytes[256];
    size_t len;
} request_t;

/* A zero-byte FIN would be accepted while the earlier request bytes are
 * refused. Only production ordering can prevent it from passing those bytes.
 * Nonempty writes remain refused even if an implementation coalesces FIN. */
static moq_transport_result_t write_with_body_gate(void *ctx, uint64_t sid,
    const uint8_t *data, size_t len, bool fin)
{
    fake_endpoint_t *ep = ctx;
    if (sid == blocked_body_sid && len > 0) {
        ep->block_count++;
        return MOQ_TRANSPORT_WOULD_BLOCK;
    }
    return fake_write(ctx, sid, data, len, fin);
}

static bool establish(rl_pair_t *p)
{
    p->ver = MOQ_VERSION_DRAFT_18;
    p->now = 1000;
    p->client_control_sid = p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = p->ver;
    cc.max_actions = sc.max_actions = 16;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    REQUIRE(moq_session_create(&cc, p->now, &p->c) == MOQ_OK);
    REQUIRE(moq_session_create(&sc, p->now, &p->s) == MOQ_OK);
    REQUIRE(test_bind_if_required(p->c) == MOQ_OK);
    REQUIRE(test_bind_if_required(p->s) == MOQ_OK);
    held_endpoint_init(&p->cep, 1000, 2000);
    held_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.write = write_with_body_gate;
    if (native_abort) fake_endpoint_enable_abort(&p->cep);
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    bc.max_streams = 8;
    bc.max_pending = 2;
    REQUIRE(held_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) == MOQ_OK);
    moq_transport_bridge_cfg_init(&bc, &p->salloc);
    bc.max_streams = 8;
    bc.max_pending = 2;
    REQUIRE(held_bridge_create(&bc, p->s, &p->sep.vtable, &p->sep, &p->sb) == MOQ_OK);
    REQUIRE(moq_session_start(p->c, p->now) == MOQ_OK);
    REQUIRE(moq_session_start(p->s, p->now) == MOQ_OK);
    shuttle(p);
    REQUIRE(!p->deliver_error && p->cin.count == 0 && p->sin.count == 0);
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    REQUIRE(!moq_transport_bridge_is_terminal(p->cb));
    REQUIRE(!moq_transport_bridge_is_terminal(p->sb));
    obs_t observed = {0};
    drain(p->c, &observed);
    drain(p->s, &observed);
    REQUIRE(observed.setup_complete == 2 && observed.closed == 0);
    REQUIRE(p->cb->pending_count == 0 && p->sb->pending_count == 0);
    REQUIRE(p->c->action_head == p->c->action_tail);
    REQUIRE(p->c2s.n == 0 && p->s2c.n == 0);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) < p->cb->max_streams);
    fake_endpoint_clear_ops(&p->cep);
    fake_endpoint_clear_ops(&p->sep);
    return true;
}

static bool service(rl_pair_t *p)
{
    EXPECT(moq_transport_bridge_service(p->cb, ++p->now) == MOQ_OK);
    EXPECT(!moq_transport_bridge_is_terminal(p->cb));
    EXPECT(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    EXPECT(p->cb->pending_count <= p->cb->max_pending);
    EXPECT(!p->cep.overflowed && !p->cep.truncated);
    return true;
}

static size_t count_op(const fake_endpoint_t *ep, fake_op_kind_t kind,
                       uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < ep->count; ++i)
        if (ep->ops[i].kind == kind && ep->ops[i].stream_id == sid) ++n;
    return n;
}

static bool begin_request(rl_pair_t *p, request_t *r)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("pending-close");
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ &part, 1 };
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    REQUIRE(moq_session_subscribe_namespace(p->c, &cfg, ++p->now,
                                             &r->handle) == MOQ_OK);
    REQUIRE(moq_ns_sub_handle_is_valid(r->handle));
    REQUIRE(p->c->action_tail == p->c->action_head + 1);
    const moq_action_t *a = &p->c->actions[p->c->action_head % p->c->action_cap];
    REQUIRE(a->kind == MOQ_ACTION_OPEN_BIDI_STREAM);
    REQUIRE(a->u.open_bidi_stream.stream_ref._v != 0);
    REQUIRE(!a->u.open_bidi_stream.fin);
    REQUIRE(a->u.open_bidi_stream.len > 0);
    REQUIRE(a->u.open_bidi_stream.len <= sizeof(r->bytes));
    r->ref = a->u.open_bidi_stream.stream_ref;
    r->sid = p->cep.next_bidi_id;
    r->len = a->u.open_bidi_stream.len;
    memcpy(r->bytes, a->u.open_bidi_stream.data, r->len);
    return true;
}

/* Called before cancel, so these are old-product causal prerequisites, not
 * assertions about a new representation of the owed FIN. */
static bool retained_request(const rl_pair_t *p, const request_t *r, bool mapped)
{
    REQUIRE(p->cb->pending_count == 1);
    const bridge_pending_item_t *q = &p->cb->pending[0];
    REQUIRE(q->kind == (mapped ? PENDING_COPIED_WRITE : PENDING_OPEN_BIDI_DATA));
    REQUIRE(q->stream_ref._v == r->ref._v && !q->fin);
    REQUIRE(q->data_len == r->len && q->data != NULL);
    REQUIRE(memcmp(q->data, r->bytes, r->len) == 0);
    REQUIRE(q->has_stream_id == mapped);
    if (mapped) {
        REQUIRE(q->stream_id == r->sid);
        REQUIRE(moq_transport_bridge_find_ref(p->cb, r->sid)._v == r->ref._v);
    } else {
        REQUIRE(moq_transport_bridge_find_ref(p->cb, r->sid)._v == 0);
    }
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, r->sid) == (mapped ? 1u : 0u));
    REQUIRE(count_op(&p->cep, FAKE_OP_WRITE, r->sid) == 0);
    REQUIRE(p->c->action_head == p->c->action_tail);
    REQUIRE(p->cep.block_count > 0);
    return true;
}

static bool cancel_request(rl_pair_t *p, const request_t *r)
{
    /* This is an advancing public call. The expected request bytes were
     * copied before any bridge poll; no borrowed action is used here. */
    REQUIRE(moq_session_cancel_namespace_sub(p->c, r->handle, ++p->now) == MOQ_OK);
    unsigned closes = 0;
    for (size_t i = p->c->action_head; i < p->c->action_tail; ++i) {
        const moq_action_t *a = &p->c->actions[i % p->c->action_cap];
        if (a->kind == MOQ_ACTION_CLOSE_BIDI_STREAM) {
            REQUIRE(a->u.close_bidi_stream.stream_ref._v == r->ref._v);
            ++closes;
        }
    }
    REQUIRE(closes == 1);
    return true;
}

static bool no_request_output(const rl_pair_t *p, const request_t *r)
{
    EXPECT(count_op(&p->cep, FAKE_OP_WRITE, r->sid) == 0);
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        EXPECT(op->kind == FAKE_OP_OPEN_BIDI && op->stream_id == r->sid);
    }
    return true;
}

static bool complete_once(rl_pair_t *p, const request_t *r)
{
    blocked_body_sid = UINT64_MAX;
    for (unsigned i = 0; i < 3; ++i) EXPECT(service(p));
    EXPECT(p->cb->pending_count == 0);
    EXPECT(p->c->action_head == p->c->action_tail);
    EXPECT(!moq_transport_bridge_has_outbound_pending(p->cb));
    size_t bytes = 0;
    unsigned opens = 0, fins = 0;
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        EXPECT(op->kind == FAKE_OP_OPEN_BIDI || op->kind == FAKE_OP_WRITE);
        EXPECT(op->stream_id == r->sid); /* also rejects an unresolved id-zero FIN */
        if (op->kind == FAKE_OP_OPEN_BIDI) {
            EXPECT(++opens == 1);
        } else {
            EXPECT(opens == 1 && fins == 0);
            EXPECT(bytes <= r->len && op->data_len <= r->len - bytes);
            if (op->data_len)
                EXPECT(memcmp(op->data, r->bytes + bytes, op->data_len) == 0);
            bytes += op->data_len;
            if (op->fin) {
                EXPECT(bytes == r->len);
                ++fins;
            }
        }
    }
    EXPECT(opens == 1 && bytes == r->len);
    EXPECT(fins == 1);
    size_t before = p->cep.count;
    for (unsigned i = 0; i < 3; ++i) EXPECT(service(p));
    EXPECT(p->cep.count == before);
    return true;
}

static bool unmapped_close(rl_pair_t *p)
{
    REQUIRE(establish(p));
    request_t r = {0};
    p->cep.block_open_bidi = true;
    REQUIRE(begin_request(p, &r));
    REQUIRE(service(p));
    REQUIRE(retained_request(p, &r, false));
    REQUIRE(cancel_request(p, &r));
    for (unsigned i = 0; i < 3; ++i) {
        EXPECT(service(p));
        EXPECT(no_request_output(p, &r));
        EXPECT(count_op(&p->cep, FAKE_OP_OPEN_BIDI, r.sid) == 0);
        EXPECT(p->cb->pending_count > 0);
    }
    EXPECT(p->c->action_head == p->c->action_tail);

    /* The open now succeeds, but its bytes still cannot. The originally
     * unmapped close must acquire this exact id and stay behind those bytes. */
    blocked_body_sid = r.sid;
    p->cep.block_open_bidi = false;
    EXPECT(service(p));
    EXPECT(count_op(&p->cep, FAKE_OP_OPEN_BIDI, r.sid) == 1);
    EXPECT(moq_transport_bridge_find_ref(p->cb, r.sid)._v == r.ref._v);
    EXPECT(no_request_output(p, &r));
    EXPECT(complete_once(p, &r));
    return true;
}

static bool mapped_close(rl_pair_t *p, bool retained_fin)
{
    REQUIRE(establish(p));
    request_t r = {0};
    blocked_body_sid = p->cep.next_bidi_id;
    REQUIRE(begin_request(p, &r));
    REQUIRE(service(p));
    REQUIRE(retained_request(p, &r, true));
    REQUIRE(cancel_request(p, &r));
    if (retained_fin) p->cep.block_write = true;
    EXPECT(service(p));
    EXPECT(no_request_output(p, &r));
    EXPECT(p->c->action_head == p->c->action_tail);
    if (retained_fin) {
        /* Both old and corrected sources can retain this mapped FIN. Do not
         * require a separate item if the bridge coalesces it into the write. */
        bool terminal_retained = false;
        for (uint32_t i = 0; i < p->cb->pending_count; ++i) {
            const bridge_pending_item_t *q = &p->cb->pending[i];
            if (q->stream_ref._v == r.ref._v &&
                (q->kind == PENDING_CLOSE_BIDI_FIN || q->fin))
                terminal_retained = true;
        }
        EXPECT(terminal_retained);
        p->cep.block_write = false;
    }
    /* A retry with only the body refused must not release the later FIN. */
    for (unsigned i = 0; i < 2; ++i) {
        EXPECT(service(p));
        EXPECT(no_request_output(p, &r));
        EXPECT(moq_transport_bridge_has_outbound_pending(p->cb));
    }
    EXPECT(complete_once(p, &r));
    return true;
}

static size_t drain_copies(const moq_session_t *s, moq_stream_ref_t ref)
{
    size_t n = 0;
    for (size_t i = 0; i < s->drain_ref_count; ++i)
        if (s->drain_refs[i] == ref._v) ++n;
    return n;
}

static bool begin_fetch(rl_pair_t *p, request_t *r, moq_fetch_t *fetch)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("pending-abort");
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("objects");
    cfg.end_group = 0;
    cfg.end_object = 3;
    REQUIRE(moq_session_fetch(p->c, &cfg, ++p->now, fetch) == MOQ_OK);
    REQUIRE(p->c->action_tail == p->c->action_head + 1);
    const moq_action_t *a = &p->c->actions[p->c->action_head % p->c->action_cap];
    REQUIRE(a->kind == MOQ_ACTION_OPEN_BIDI_STREAM);
    REQUIRE(a->u.open_bidi_stream.stream_ref._v != 0);
    REQUIRE(!a->u.open_bidi_stream.fin);
    REQUIRE(a->u.open_bidi_stream.len > 0);
    REQUIRE(a->u.open_bidi_stream.len <= sizeof(r->bytes));
    r->ref = a->u.open_bidi_stream.stream_ref;
    r->sid = p->cep.next_bidi_id;
    r->len = a->u.open_bidi_stream.len;
    memcpy(r->bytes, a->u.open_bidi_stream.data, r->len);
    return true;
}

/* Check accepted operations, not just counts: exact request bytes precede
 * teardown; sequential RESET precedes STOP; the first CANCELLED code survives
 * a later peer terminal carrying a different code. */
static bool fetch_wire(const rl_pair_t *p, const request_t *r,
                       unsigned resets, unsigned stops, unsigned aborts)
{
    size_t bytes = 0;
    unsigned nopen = 0, nr = 0, ns = 0, na = 0;
    bool ended = false;
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        EXPECT(op->stream_id == r->sid);
        switch (op->kind) {
        case FAKE_OP_OPEN_BIDI:
            EXPECT(++nopen == 1 && !ended && bytes == 0);
            break;
        case FAKE_OP_WRITE:
            EXPECT(nopen == 1 && !ended && !op->fin);
            EXPECT(bytes <= r->len && op->data_len <= r->len - bytes);
            if (op->data_len)
                EXPECT(memcmp(op->data, r->bytes + bytes, op->data_len) == 0);
            bytes += op->data_len;
            break;
        case FAKE_OP_RESET:
            EXPECT(nopen == 1 && bytes == r->len && na == 0 && ns == 0);
            EXPECT(op->error_code == MOQ_STREAM_ERROR_CANCELLED);
            ++nr;
            ended = true;
            break;
        case FAKE_OP_STOP:
            EXPECT(nr == 1 && na == 0);
            EXPECT(op->error_code == MOQ_STREAM_ERROR_CANCELLED);
            ++ns;
            ended = true;
            break;
        case FAKE_OP_ABORT:
            EXPECT(nopen == 1 && bytes == r->len && nr == 0 && ns == 0);
            EXPECT(op->error_code == MOQ_STREAM_ERROR_CANCELLED);
            ++na;
            ended = true;
            break;
        default:
            EXPECT(false);
        }
    }
    EXPECT(nopen == 1 && bytes == r->len);
    EXPECT(nr == resets && ns == stops && na == aborts);
    return true;
}

static bool abort_identity(const rl_pair_t *p, const request_t *r)
{
    bridge_stream_entry_t *e = bridge_find_by_id(p->cb, r->sid);
    EXPECT(e != NULL && e->ref._v == r->ref._v && e->aborting);
    EXPECT(drain_copies(p->c, r->ref) == 1 && p->c->drain_ref_count == 1);
    EXPECT(!moq_session_has_events(p->c));
    return true;
}

static bool unmapped_fetch_cancel(rl_pair_t *p)
{
    REQUIRE(establish(p));
    size_t maps = moq_transport_bridge_stream_count(p->cb);
    REQUIRE(p->c->drain_ref_count == 0);
    request_t r = {0};
    moq_fetch_t fetch;
    p->cep.block_open_bidi = true;
    REQUIRE(begin_fetch(p, &r, &fetch));
    REQUIRE(service(p));
    REQUIRE(retained_request(p, &r, false));
    REQUIRE(moq_session_fetch_cancel(p->c, fetch, ++p->now) == MOQ_OK);
    REQUIRE(drain_copies(p->c, r.ref) == 1);
    REQUIRE(moq_session_fetch_cancel(p->c, fetch, ++p->now) == MOQ_ERR_STALE_HANDLE);
    REQUIRE(p->c->action_tail == p->c->action_head + 1);
    const moq_action_t *a = &p->c->actions[p->c->action_head % p->c->action_cap];
    REQUIRE(a->kind == MOQ_ACTION_ABORT_BIDI_STREAM);
    REQUIRE(a->u.abort_bidi_stream.stream_ref._v == r.ref._v);
    REQUIRE(a->u.abort_bidi_stream.error_code == MOQ_STREAM_ERROR_CANCELLED);
    for (unsigned i = 0; i < 2; ++i) {
        EXPECT(service(p));
        EXPECT(p->cep.count == 0);
        EXPECT(moq_transport_bridge_find_ref(p->cb, r.sid)._v == 0);
    }
    EXPECT(p->c->action_head == p->c->action_tail);

    p->cep.block_abort = native_abort;
    p->cep.block_reset = !native_abort;
    p->cep.block_stop = !native_abort;
    p->cep.block_open_bidi = false;
    int refused = p->cep.block_count;
    EXPECT(service(p));
    EXPECT(count_op(&p->cep, FAKE_OP_OPEN_BIDI, r.sid) == 1);
    EXPECT(fetch_wire(p, &r, 0, 0, 0));
    /* Old source loses ABORT before OPEN resolves, so makes no refused
     * teardown attempt here. This is a behavior oracle, not a setup guard. */
    EXPECT(p->cep.block_count > refused);
    EXPECT(abort_identity(p, &r));
    EXPECT(moq_transport_bridge_has_outbound_pending(p->cb));

    if (native_abort) {
        p->cep.block_abort = false;
        EXPECT(service(p));
        EXPECT(fetch_wire(p, &r, 0, 0, 1));
        EXPECT(abort_identity(p, &r)); /* accepted abort is not peer terminal */
        size_t ops = p->cep.count;
        EXPECT(moq_transport_bridge_on_peer_stop_sending(p->cb, r.sid, 7,
                                                         ++p->now) == MOQ_OK);
        EXPECT(service(p));
        EXPECT(p->cep.count == ops);
        EXPECT(abort_identity(p, &r));
        EXPECT(moq_transport_bridge_on_peer_stream_terminal(p->cb, r.sid,
                                                            ++p->now) == MOQ_OK);
        EXPECT(moq_transport_bridge_on_peer_stream_terminal(p->cb, r.sid,
                                                            ++p->now) == MOQ_OK);
        EXPECT(p->cep.count == ops && drain_copies(p->c, r.ref) == 1);
    } else {
        p->cep.block_reset = false;
        EXPECT(service(p));
        EXPECT(fetch_wire(p, &r, 1, 0, 0));
        EXPECT(abort_identity(p, &r));
        size_t ops = p->cep.count;
        EXPECT(moq_transport_bridge_on_peer_stream_reset(p->cb, r.sid, 7,
                                                         ++p->now) == MOQ_OK);
        EXPECT(moq_transport_bridge_on_peer_stream_reset(p->cb, r.sid, 9,
                                                         ++p->now) == MOQ_OK);
        EXPECT(p->cep.count == ops);
        for (unsigned i = 0; i < 2; ++i) {
            EXPECT(service(p));
            EXPECT(fetch_wire(p, &r, 1, 0, 0)); /* never repeat accepted RESET */
            EXPECT(abort_identity(p, &r)); /* true terminal cannot erase owed STOP */
            EXPECT(moq_transport_bridge_has_outbound_pending(p->cb));
        }
        p->cep.block_stop = false;
    }
    for (unsigned i = 0; i < 2; ++i) EXPECT(service(p));
    EXPECT(fetch_wire(p, &r, native_abort ? 0u : 1u,
                      native_abort ? 0u : 1u, native_abort ? 1u : 0u));
    EXPECT(moq_transport_bridge_find_ref(p->cb, r.sid)._v == 0);
    EXPECT(moq_transport_bridge_stream_count(p->cb) == maps);
    EXPECT(drain_copies(p->c, r.ref) == 0 && p->c->drain_ref_count == 0);
    EXPECT(!moq_session_has_events(p->c));
    EXPECT(!moq_transport_bridge_has_pending(p->cb));
    EXPECT(!moq_transport_bridge_has_outbound_pending(p->cb));
    size_t ops = p->cep.count;
    EXPECT(moq_transport_bridge_on_peer_stream_terminal(p->cb, r.sid, ++p->now) == MOQ_OK);
    EXPECT(moq_transport_bridge_on_peer_stream_reset(p->cb, r.sid, 11, ++p->now) == MOQ_OK);
    EXPECT(moq_transport_bridge_on_peer_stop_sending(p->cb, r.sid, 13, ++p->now) == MOQ_OK);
    for (unsigned i = 0; i < 2; ++i) EXPECT(service(p));
    EXPECT(p->cep.count == ops);
    EXPECT(p->c->action_head == p->c->action_tail);
    EXPECT(p->c->drain_ref_count == 0 && !moq_session_has_events(p->c));
    EXPECT(moq_transport_bridge_find_ref(p->cb, r.sid)._v == 0);
    return true;
}

static const char *const names[] = {
    "unmapped", "mapped-dispatch", "mapped-retry", "fetch-native", "fetch-sequential"
};

int main(int argc, char **argv)
{
    bool selected = false;
    if (argc > 2) { fprintf(stderr, "usage: %s [row]\n", argv[0]); return 2; }
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        if (argc == 2 && strcmp(argv[1], names[i]) != 0) continue;
        selected = true;
        row_name = names[i];
        blocked_body_sid = UINT64_MAX;
        native_abort = i == 3;
        rl_pair_t *p = calloc(1, sizeof(*p));
        if (!p) { fprintf(stderr, "fixture allocation failed\n"); return 2; }
        int before = failures;
        bool ok = i == 0 ? unmapped_close(p) : i < 3 ? mapped_close(p, i == 2)
                                                   : unmapped_fetch_cancel(p);
        if (p->cep.overflowed || p->cep.truncated ||
            p->sep.overflowed || p->sep.truncated) {
            ++failures;
            fprintf(stderr, "FAIL %s: endpoint recorder lost evidence\n", row_name);
        }
        held_bridge_destroy(p->cb);
        held_bridge_destroy(p->sb);
        moq_session_destroy(p->c);
        moq_session_destroy(p->s);
        if (p->cas.balance != 0 || p->sas.balance != 0) {
            ++failures;
            fprintf(stderr, "FAIL %s: allocator balance client=%lld server=%lld\n",
                row_name, (long long)p->cas.balance, (long long)p->sas.balance);
        }
        free(p);
        if (ok && failures == before) printf("PASS %s\n", row_name);
    }
    if (!selected) { fprintf(stderr, "unknown row\n"); return 2; }
    printf("%u checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
