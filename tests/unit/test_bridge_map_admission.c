/* Mixed-map admission, through production session actions and bridge ingress.
 * No injected actions, fabricated map entries, capacity changes after create,
 * or provider-credit claims. The UNI direct row is an existing-picker control;
 * its causal counterpart first reaches a refused open, then loses the slot to
 * peer ingress. Endpoint acceptance, not peer acknowledgement, releases output.
 */
static int failures;
#include "../support/bridge_wire_pair.h"
#include "../support/held_bridge_driver.h"
#include "test_binding_support.h"

static const char *row_name;
static unsigned checks;
static uint64_t blocked_body_sid = UINT64_MAX;
static uint64_t payload_gate_sid = UINT64_MAX;
static const uint8_t *payload_gate_data;
static size_t payload_gate_len;
static unsigned payload_refusals;
#define REQUIRE(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL %s:%d: %s\n", row_name, __LINE__, #x); \
    return false; } } while (0)
#define ADMISSION_SETUP(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "SETUP %s:%d: %s\n", row_name, __LINE__, #x); \
    return false; } } while (0)

typedef struct bytes {
    uint8_t data[512];
    size_t len;
    unsigned fins;
} bytes_t;

typedef struct fixture {
    rl_pair_t p;
    moq_subscription_t source;
    moq_subgroup_handle_t existing;
    moq_stream_ref_t existing_ref;
    uint64_t existing_sid;
} fixture_t;

/* During one retry phase a prior mapped response is refused while its later
 * FIN would fit. The production ordering gate, not the fake's stream-wide
 * block, must keep that FIN behind the refused response. */
static moq_transport_result_t write_with_body_gate(void *ctx, uint64_t sid,
    const uint8_t *data, size_t len, bool fin)
{
    fake_endpoint_t *ep = ctx;
    if (sid == payload_gate_sid && data == payload_gate_data &&
        len == payload_gate_len) {
        ++payload_refusals;
        ep->block_count++;
        return MOQ_TRANSPORT_WOULD_BLOCK;
    }
    if (sid == blocked_body_sid && len > 0 && !fin) {
        ep->block_count++;
        return MOQ_TRANSPORT_WOULD_BLOCK;
    }
    return fake_write(ctx, sid, data, len, fin);
}

static bool append(bytes_t *v, const uint8_t *data, size_t len, bool fin)
{
    REQUIRE(v->fins == 0);
    REQUIRE(len <= sizeof(v->data) - v->len);
    if (len) memcpy(v->data + v->len, data, len);
    v->len += len;
    v->fins += fin ? 1u : 0u;
    return true;
}

static bool recorded(const fake_endpoint_t *ep, uint64_t sid, bytes_t *out)
{
    memset(out, 0, sizeof(*out));
    REQUIRE(!ep->overflowed && !ep->truncated);
    for (size_t i = 0; i < ep->count; ++i) {
        const fake_op_t *op = &ep->ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == sid)
            REQUIRE(append(out, op->data, op->data_len, op->fin));
    }
    return true;
}

static size_t count_op(const fake_endpoint_t *ep, fake_op_kind_t kind,
                       uint64_t sid, uint64_t code)
{
    size_t n = 0;
    for (size_t i = 0; i < ep->count; ++i)
        if (ep->ops[i].kind == kind && ep->ops[i].stream_id == sid &&
            ep->ops[i].error_code == code) ++n;
    return n;
}

/* Read queued production bytes before any advancing call. The test owns this
 * snapshot; the bridge must independently preserve the action's borrow. */
static bool snapshot(moq_session_t *s, moq_stream_ref_t ref, bytes_t *out)
{
    memset(out, 0, sizeof(*out));
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        switch (a->kind) {
        case MOQ_ACTION_OPEN_BIDI_STREAM:
            if (a->u.open_bidi_stream.stream_ref._v == ref._v)
                REQUIRE(append(out, a->u.open_bidi_stream.data,
                    a->u.open_bidi_stream.len, a->u.open_bidi_stream.fin));
            break;
        case MOQ_ACTION_SEND_BIDI_STREAM:
            if (a->u.send_bidi_stream.stream_ref._v == ref._v)
                REQUIRE(append(out, a->u.send_bidi_stream.data,
                    a->u.send_bidi_stream.len, a->u.send_bidi_stream.fin));
            break;
        case MOQ_ACTION_CLOSE_BIDI_STREAM:
            if (a->u.close_bidi_stream.stream_ref._v == ref._v)
                REQUIRE(append(out, NULL, 0, true));
            break;
        case MOQ_ACTION_OPEN_UNI_CONTROL:
            if (a->u.open_uni_control.stream_ref._v == ref._v)
                REQUIRE(append(out, a->u.open_uni_control.data,
                    a->u.open_uni_control.len, false));
            break;
        case MOQ_ACTION_SEND_DATA:
            if (a->u.send_data.stream_ref._v == ref._v) {
                const moq_send_data_action_t *d = &a->u.send_data;
                REQUIRE(append(out, d->header, d->header_len, false));
                REQUIRE(append(out, d->payload ? moq_rcbuf_data(d->payload) : NULL,
                    d->payload ? moq_rcbuf_len(d->payload) : 0, d->fin));
            }
            break;
        default: break;
        }
    }
    REQUIRE(out->len > 0);
    return true;
}

static moq_stream_ref_t queued_open(moq_session_t *s, uint32_t kind)
{
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == kind)
            return kind == MOQ_ACTION_OPEN_BIDI_STREAM
                ? a->u.open_bidi_stream.stream_ref : a->u.open_uni_control.stream_ref;
    }
    return moq_stream_ref_from_u64(0);
}

static bool create_pair(fixture_t *f, moq_version_t version,
                        uint32_t client_maps, uint32_t server_maps,
                        uint32_t pending, bool hold_server)
{
    rl_pair_t *p = &f->p;
    p->ver = version;
    p->now = 1000;
    p->client_control_sid = p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = version;
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
    if (hold_server) p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    bc.max_streams = client_maps;
    bc.max_pending = pending;
    REQUIRE(held_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) == MOQ_OK);
    moq_transport_bridge_cfg_init(&bc, &p->salloc);
    bc.max_streams = server_maps;
    bc.max_pending = pending;
    REQUIRE(held_bridge_create(&bc, p->s, &p->sep.vtable, &p->sep, &p->sb) == MOQ_OK);
    return true;
}

static bool establish(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    REQUIRE(moq_session_start(p->c, p->now) == MOQ_OK);
    if (moq_session_uses_uni_control(p->s))
        REQUIRE(moq_session_start(p->s, p->now) == MOQ_OK);
    shuttle(p);
    REQUIRE(!p->deliver_error);
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    obs_t ignored = {0};
    drain(p->c, &ignored);
    drain(p->s, &ignored);
    REQUIRE(ignored.closed == 0 && ignored.setup_complete == 2);
    fake_endpoint_clear_ops(&p->cep);
    fake_endpoint_clear_ops(&p->sep);
    return true;
}

static bool service(moq_transport_bridge_t *b, uint64_t *now)
{
    REQUIRE(moq_transport_bridge_service(b, ++*now) == MOQ_OK);
    REQUIRE(!moq_transport_bridge_is_terminal(b));
    return true;
}

/* A complete, known stream type with its header still incomplete creates a
 * real receive owner and map entry. RESET releases both, without test writes
 * to either table. Both D16 and D18 accept subgroup header type 0x10. */
static bool occupy_data(moq_transport_bridge_t *b, moq_session_t *s,
                        uint64_t sid, uint64_t now)
{
    uint8_t type = 0x10;
    size_t before = moq_transport_bridge_stream_count(b);
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(b, sid, &type, 1, false, now) == MOQ_OK);
    moq_stream_ref_t ref = moq_transport_bridge_find_ref(b, sid);
    REQUIRE(ref._v != 0 && moq_session_has_transport_stream(s, ref));
    REQUIRE(moq_transport_bridge_stream_count(b) == before + 1);
    return true;
}

static bool namespace_request(rl_pair_t *p, const char *name, bool fin,
                              uint64_t *sid, bytes_t *wire)
{
    moq_bytes_t part = { (const uint8_t *)name, strlen(name) };
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ &part, 1 };
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h;
    REQUIRE(moq_session_subscribe_namespace(p->c, &cfg, p->now, &h) == MOQ_OK);
    if (fin) REQUIRE(moq_session_cancel_namespace_sub(p->c, h, p->now) == MOQ_OK);
    *sid = p->cep.next_bidi_id;
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, *sid, 0) == 1);
    REQUIRE(recorded(&p->cep, *sid, wire));
    REQUIRE(wire->len > 0 && wire->fins == (fin ? 1u : 0u));
    fake_endpoint_clear_ops(&p->cep);
    return true;
}

static bool namespace_event(moq_session_t *s, const char *name)
{
    unsigned count = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) {
        bool match = false;
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
            const moq_namespace_t *ns = &ev.u.ns_sub_request.track_namespace_prefix;
            match = ns->count == 1 && ns->parts[0].len == strlen(name) &&
                memcmp(ns->parts[0].data, name, strlen(name)) == 0;
            ++count;
        }
        moq_event_cleanup(&ev);
        REQUIRE(match);
    }
    REQUIRE(count == 1);
    return true;
}

static uint32_t holds(const moq_transport_bridge_t *b)
{
    uint32_t n = 0;
    for (uint32_t i = 0; b->holds && i < b->max_holds; ++i)
        n += b->holds[i].state != BRIDGE_HOLD_FREE;
    return n;
}

static bool inbound_d16(fixture_t *f, bool enabled)
{
    rl_pair_t *p = &f->p;
    REQUIRE(create_pair(f, MOQ_VERSION_DRAFT_16, 8, 2, 2, enabled));
    REQUIRE(establish(f));
    bytes_t old, first, second, excess;
    uint64_t old_sid, first_sid, second_sid, excess_sid;
    REQUIRE(namespace_request(p, "occupied", false, &old_sid, &old));
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->sb, old_sid,
        old.data, old.len, false, p->now) == MOQ_OK);
    REQUIRE(namespace_event(p->s, "occupied"));
    REQUIRE(occupy_data(p->sb, p->s, 1900, p->now));
    REQUIRE(moq_transport_bridge_stream_count(p->sb) == p->sb->max_streams);
    REQUIRE(bridge_find_by_id(p->sb, old_sid)->kind == BRIDGE_STREAM_BIDI);
    REQUIRE(bridge_find_by_id(p->sb, 1900)->kind == BRIDGE_STREAM_UNI);

    /* Control bypass is real decoded MAX_REQUEST_ID, with or without holds. */
    REQUIRE(moq_session_grant_request_capacity(p->c, 80, p->now) == MOQ_OK);
    REQUIRE(service(p->cb, &p->now));
    bytes_t control;
    REQUIRE(recorded(&p->cep, p->client_control_sid, &control));
    REQUIRE(control.len > 0 && control.fins == 0);
    REQUIRE(moq_transport_bridge_on_peer_control_bytes(p->sb, p->client_control_sid,
        control.data, control.len, false, p->now) == MOQ_OK);
    REQUIRE(moq_session_peer_request_capacity(p->s) == 80);
    REQUIRE(moq_transport_bridge_stream_count(p->sb) == 2 && holds(p->sb) == 0);
    fake_endpoint_clear_ops(&p->cep);

    REQUIRE(namespace_request(p, "first", true, &first_sid, &first));
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(p->sb, first_sid,
        first.data, first.len, true, p->now);
    if (!enabled) {
        REQUIRE(p->sb->holds == NULL);
        REQUIRE(rc == MOQ_ERR_INTERNAL && moq_transport_bridge_is_fatal(p->sb));
        REQUIRE(rc != MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
        return true; /* capability-off legacy request behavior */
    }
    REQUIRE(rc == MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(holds(p->sb) == 1 && moq_transport_bridge_find_ref(p->sb, first_sid)._v == 0);
    REQUIRE(!moq_session_has_events(p->s));
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->sb, first_sid,
        NULL, 0, true, p->now) == MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(namespace_request(p, "second", false, &second_sid, &second));
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->sb, second_sid,
        second.data, second.len, false, p->now) == MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(namespace_request(p, "excess", false, &excess_sid, &excess));
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->sb, excess_sid,
        excess.data, excess.len, false, p->now) == MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    REQUIRE(holds(p->sb) == 2 && p->sb->max_holds == 2);
    REQUIRE(moq_transport_bridge_find_ref(p->sb, excess_sid)._v == 0);
    REQUIRE(!moq_transport_bridge_stream_has_pending(p->sb, excess_sid));
    REQUIRE(p->sep.count == 0 && !moq_session_has_events(p->s));
    /* REFUSE_CANCEL leaves cancellation to the adapter. This fixture asserts
     * zero consumption/no record, not a fake provider cancellation as a core
     * success. The retained-hold RESET below checks actual bridge code 0x1. */

    REQUIRE(moq_transport_bridge_on_peer_stream_reset(p->sb, 1900,
        MOQ_STREAM_ERROR_CANCELLED, ++p->now) == MOQ_OK);
    REQUIRE(moq_transport_bridge_find_ref(p->sb, 1900)._v == 0);
    REQUIRE(service(p->sb, &p->now));
    REQUIRE(!moq_transport_bridge_stream_has_pending(p->sb, first_sid));
    REQUIRE(moq_transport_bridge_stream_has_pending(p->sb, second_sid));
    REQUIRE(!moq_session_has_events(p->s)); /* offer is not consumption */
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->sb, first_sid,
        NULL, 0, true, p->now) == MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->sb, first_sid,
        first.data, first.len, true, p->now) == MOQ_OK);
    REQUIRE(namespace_event(p->s, "first"));
    bridge_stream_entry_t *e = bridge_find_by_id(p->sb, first_sid);
    REQUIRE(e != NULL && e->peer_send_closed);
    REQUIRE(holds(p->sb) == 1);
    REQUIRE(service(p->sb, &p->now));
    REQUIRE(!moq_session_has_events(p->s));

    /* The peer resets a still-unmapped held bidi: only our send half remains.
     * A refused own-half reset is retained, then accepted once with D16 0x1. */
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(p->sb, second_sid, 7,
        ++p->now) == MOQ_OK);
    p->sep.block_reset = true;
    REQUIRE(service(p->sb, &p->now));
    REQUIRE(moq_transport_bridge_stream_has_pending(p->sb, second_sid));
    REQUIRE(count_op(&p->sep, FAKE_OP_RESET, second_sid, 1) == 0);
    p->sep.block_reset = false;
    REQUIRE(service(p->sb, &p->now));
    REQUIRE(count_op(&p->sep, FAKE_OP_RESET, second_sid, 1) == 1);
    REQUIRE(count_op(&p->sep, FAKE_OP_STOP, second_sid, 1) == 0);
    REQUIRE(holds(p->sb) == 0);
    REQUIRE(service(p->sb, &p->now));
    REQUIRE(count_op(&p->sep, FAKE_OP_RESET, second_sid, 1) == 1);
    return true;
}

static bool write_object(rl_pair_t *p, moq_subgroup_handle_t sg, uint64_t object)
{
    const uint8_t data[] = { 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64 };
    moq_rcbuf_t *buf = NULL;
    REQUIRE(moq_rcbuf_create(&p->calloc, data, sizeof(data), &buf) == MOQ_OK);
    moq_object_cfg_t cfg;
    moq_object_cfg_init(&cfg);
    cfg.object_id = object;
    cfg.payload = buf;
    moq_result_t rc = moq_session_write_object_ex(p->c, sg, &cfg, p->now);
    uint32_t refs = moq_rcbuf_refcount(buf);
    moq_rcbuf_decref(buf); /* no test reference props up retained payload */
    REQUIRE(rc == MOQ_OK && refs >= 2);
    return true;
}

static bool open_group(fixture_t *f, uint64_t group, moq_subgroup_handle_t *out,
                       moq_stream_ref_t *ref)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    REQUIRE(moq_session_open_subgroup(f->p.c, f->source, &cfg, f->p.now, out) == MOQ_OK);
    *ref = f->p.c->subgroups[moq_handle_slot(out->_opaque)].stream_ref;
    REQUIRE(ref->_v != 0);
    return true;
}

static bool output_setup(fixture_t *f, uint32_t pending)
{
    rl_pair_t *p = &f->p;
    REQUIRE(create_pair(f, MOQ_VERSION_DRAFT_18, 5, 12, pending, false));
    REQUIRE(establish(f));
    moq_bytes_t part = MOQ_BYTES_LITERAL("map-admission");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("objects");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t receiver;
    REQUIRE(moq_session_subscribe(p->s, &cfg, p->now, &receiver) == MOQ_OK);
    shuttle(p);
    moq_event_t ev;
    unsigned requests = 0;
    while (moq_session_poll_events(p->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            f->source = ev.u.subscribe_request.sub;
            ++requests;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(!p->deliver_error && requests == 1);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    REQUIRE(moq_session_accept_subscribe(p->c, f->source, &ac, p->now) == MOQ_OK);
    shuttle(p);
    obs_t ignored = {0};
    drain(p->c, &ignored);
    drain(p->s, &ignored);
    REQUIRE(open_group(f, 0, &f->existing, &f->existing_ref));
    REQUIRE(write_object(p, f->existing, 0));
    REQUIRE(service(p->cb, &p->now));
    bridge_stream_entry_t *e = bridge_find_by_ref(p->cb, f->existing_ref);
    REQUIRE(e != NULL && e->origin == BRIDGE_ORIGIN_LOCAL);
    f->existing_sid = e->transport_id;
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 4);
    REQUIRE(p->cb->pending_count == 0 && p->cb->dq_items == 0);
    fake_endpoint_clear_ops(&p->cep);
    return true;
}

static bool queue_status(moq_session_t *s, uint64_t now,
                         moq_track_status_handle_t *handle)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("status-map");
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("retained-request");
    REQUIRE(moq_session_track_status(s, &cfg, now, handle) == MOQ_OK);
    return true;
}

static bool bounded_idle(rl_pair_t *p, size_t maps)
{
    REQUIRE(moq_transport_bridge_has_pending(p->cb) ||
        p->c->action_head != p->c->action_tail);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(p->cb));
    size_t ops = p->cep.count;
    int refused = p->cep.block_count;
    for (unsigned i = 0; i < 3; ++i) {
        REQUIRE(service(p->cb, &p->now));
        REQUIRE(moq_transport_bridge_stream_count(p->cb) == maps);
        REQUIRE(p->cb->pending_count <= p->cb->max_pending);
        REQUIRE(p->cep.count == ops && p->cep.block_count == refused);
        REQUIRE(!moq_transport_bridge_has_outbound_pending(p->cb));
    }
    return true;
}

static bool outgoing(fixture_t *f, bool uni, bool retry)
{
    rl_pair_t *p = &f->p;
    REQUIRE(output_setup(f, 2));
    if (!retry) REQUIRE(occupy_data(p->cb, p->c, 3900, p->now));
    moq_stream_ref_t target;
    if (uni) {
        moq_subgroup_handle_t sg;
        REQUIRE(open_group(f, 1, &sg, &target));
        REQUIRE(write_object(p, sg, 0));
        REQUIRE(moq_session_close_subgroup(p->c, sg, p->now) == MOQ_OK);
    } else {
        moq_track_status_handle_t h;
        REQUIRE(queue_status(p->c, p->now, &h));
        target = queued_open(p->c, MOQ_ACTION_OPEN_BIDI_STREAM);
    }
    REQUIRE(target._v != 0);
    bytes_t expected;
    REQUIRE(snapshot(p->c, target, &expected));
    REQUIRE(expected.fins == 1);
    uint64_t sid = uni ? p->cep.next_uni_id : p->cep.next_bidi_id;
    if (retry) {
        p->cep.block_open_uni = uni;
        p->cep.block_open_bidi = !uni;
        REQUIRE(service(p->cb, &p->now));
        REQUIRE(p->cep.block_count > 0 && p->cb->pending_count > 0);
        REQUIRE(p->cb->pending[0].kind == (uni ? PENDING_OPEN_UNI_DATA : PENDING_OPEN_BIDI_DATA));
        REQUIRE(p->cb->pending[0].stream_ref._v == target._v);
        REQUIRE(bridge_find_by_ref(p->cb, target) == NULL);
        REQUIRE(occupy_data(p->cb, p->c, 3900, p->now));
        p->cep.block_open_uni = p->cep.block_open_bidi = false;
    }
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 5);
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(count_op(&p->cep, uni ? FAKE_OP_OPEN_UNI : FAKE_OP_OPEN_BIDI, sid, 0) == 0);
    REQUIRE(bounded_idle(p, 5));

    /* Actual later session work advances the send-buffer borrow epoch. It
     * must not overwrite the retained request/header, and can use mapped IDs. */
    REQUIRE(write_object(p, f->existing, 1));
    /* D18 has no MAX_REQUEST_ID. Its public GOAWAY uses the already-mapped
     * control stream and leaves existing subscriptions/output intact. */
    REQUIRE(moq_session_goaway(p->c, NULL, 0, p->now) == MOQ_OK);
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_DRAINING);
    REQUIRE(p->c->goaway_deadline_us == UINT64_MAX);
    REQUIRE(service(p->cb, &p->now));
    bytes_t live, control;
    REQUIRE(recorded(&p->cep, f->existing_sid, &live));
    REQUIRE(live.len > 0 && live.fins == 0);
    REQUIRE(recorded(&p->cep, p->cb->local_ctrl_uni_stream_id, &control));
    REQUIRE(control.len > 0 && control.fins == 0);
    REQUIRE(count_op(&p->cep, uni ? FAKE_OP_OPEN_UNI : FAKE_OP_OPEN_BIDI, sid, 0) == 0);
    REQUIRE(bounded_idle(p, 5));

    /* FIN acceptance is a genuine map release. No slot is freed by the test. */
    REQUIRE(moq_session_close_subgroup(p->c, f->existing, p->now) == MOQ_OK);
    for (unsigned i = 0; i < 4; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(bridge_find_by_ref(p->cb, f->existing_ref) == NULL);
    REQUIRE(count_op(&p->cep, uni ? FAKE_OP_OPEN_UNI : FAKE_OP_OPEN_BIDI, sid, 0) == 1);
    bytes_t actual;
    REQUIRE(recorded(&p->cep, sid, &actual));
    REQUIRE(actual.len == expected.len && actual.fins == 1);
    REQUIRE(memcmp(actual.data, expected.data, expected.len) == 0);
    size_t fin_at = SIZE_MAX, open_at = SIZE_MAX;
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == f->existing_sid && op->fin) fin_at = i;
        if (op->kind == (uni ? FAKE_OP_OPEN_UNI : FAKE_OP_OPEN_BIDI) && op->stream_id == sid) open_at = i;
    }
    REQUIRE(fin_at != SIZE_MAX && open_at > fin_at);
    REQUIRE(p->cb->pending_count == 0 && p->cb->dq_items == 0);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(p->cb));
    return true;
}

/* A queued local open and an unrelated mapped response followed by its FIN
 * fill the bounded pending array before peer ingress takes the last map slot.
 * Merely replacing full-map fatal with FIFO `goto stop` cannot pass this. */
static bool pending_terminal(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    REQUIRE(create_pair(f, MOQ_VERSION_DRAFT_18, 4, 12, 3, false));
    REQUIRE(establish(f));
    moq_bytes_t part = MOQ_BYTES_LITERAL("mapped-response");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ &part, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("terminal");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t query, answer = MOQ_SUBSCRIPTION_INVALID;
    REQUIRE(moq_session_subscribe(p->s, &sc, p->now, &query) == MOQ_OK);
    shuttle(p);
    unsigned requests = 0;
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            answer = ev.u.subscribe_request.sub;
            ++requests;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(!p->deliver_error && requests == 1);
    bridge_stream_entry_t *mapped = NULL;
    for (uint32_t i = 0; i < p->cb->max_streams; ++i)
        if (p->cb->streams[i].active && p->cb->streams[i].kind == BRIDGE_STREAM_BIDI)
            mapped = &p->cb->streams[i];
    REQUIRE(mapped != NULL && mapped->origin == BRIDGE_ORIGIN_PEER && !mapped->peer_send_closed);
    uint64_t mapped_sid = mapped->transport_id;
    moq_stream_ref_t mapped_ref = mapped->ref;
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 3);

    moq_track_status_handle_t target_handle;
    REQUIRE(queue_status(p->c, p->now, &target_handle));
    moq_stream_ref_t target = queued_open(p->c, MOQ_ACTION_OPEN_BIDI_STREAM);
    REQUIRE(target._v != 0);
    bytes_t expected, response;
    REQUIRE(snapshot(p->c, target, &expected) && expected.fins == 1);
    uint64_t target_sid = p->cep.next_bidi_id;
    p->cep.block_open_bidi = true;
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cb->pending_count == 1);
    REQUIRE(p->cb->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    REQUIRE(p->cb->pending[0].stream_ref._v == target._v);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    REQUIRE(moq_session_accept_subscribe(p->c, answer, &ac, p->now) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(p->c, answer, &dc, p->now) == MOQ_OK);
    REQUIRE(snapshot(p->c, mapped_ref, &response) && response.fins == 1);
    fake_block_stream(&p->cep, mapped_sid);
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cb->pending_count == 3 && p->cb->max_pending == 3);
    REQUIRE(p->cb->pending[1].stream_ref._v == mapped_ref._v && !p->cb->pending[1].fin);
    REQUIRE(p->cb->pending[2].stream_ref._v == mapped_ref._v && p->cb->pending[2].fin);
    /* A real peer FIN after the owner queued DONE: its drain absorbs the
     * terminal; the mapping still belongs to our retained response + FIN. */
    REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->cb, mapped_sid,
        NULL, 0, true, ++p->now) == MOQ_OK);
    REQUIRE(bridge_find_by_ref(p->cb, mapped_ref) != NULL);
    REQUIRE(bridge_find_by_ref(p->cb, mapped_ref)->peer_send_closed);
    REQUIRE(occupy_data(p->cb, p->c, 3900, p->now));
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 4);
    fake_endpoint_clear_ops(&p->cep);
    p->cep.block_open_bidi = false;
    fake_unblock_stream(&p->cep);
    blocked_body_sid = mapped_sid;
    REQUIRE(moq_transport_bridge_has_outbound_pending(p->cb)); /* mapped FIN is runnable */
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cb->pending_count == 3);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 4);
    REQUIRE(p->cep.count == 0); /* neither same-owner FIN nor new open overtakes */
    blocked_body_sid = UINT64_MAX;
    for (unsigned i = 0; i < 4; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(bridge_find_by_ref(p->cb, mapped_ref) == NULL);
    bytes_t actual;
    REQUIRE(recorded(&p->cep, mapped_sid, &actual));
    REQUIRE(actual.len == response.len && actual.fins == 1);
    REQUIRE(memcmp(actual.data, response.data, response.len) == 0);
    REQUIRE(recorded(&p->cep, target_sid, &actual));
    REQUIRE(actual.len == expected.len && actual.fins == 1);
    REQUIRE(memcmp(actual.data, expected.data, expected.len) == 0);
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, target_sid, 0) == 1);
    size_t release_at = SIZE_MAX, open_at = SIZE_MAX;
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == mapped_sid && op->fin) release_at = i;
        if (op->kind == FAKE_OP_OPEN_BIDI && op->stream_id == target_sid) open_at = i;
    }
    REQUIRE(release_at != SIZE_MAX && open_at > release_at);
    REQUIRE(p->cb->pending_count == 0 && !moq_transport_bridge_has_outbound_pending(p->cb));
    size_t before = p->cep.count;
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cep.count == before);
    return true;
}

/* Control opens do not go through the data picker. Two actual split uni
 * types can occupy the legacy, capability-off map before SETUP completes.
 * One is the genuine peer control stream; the other is reset after SETUP.
 * No unknown-stream cancellation policy is imposed by this fixture. */
static bool control_open(fixture_t *f, bool retry)
{
    rl_pair_t *p = &f->p;
    REQUIRE(create_pair(f, MOQ_VERSION_DRAFT_18, 2, 4, 2, false));
    REQUIRE(moq_session_start(p->s, p->now) == MOQ_OK);
    REQUIRE(service(p->sb, &p->now));
    uint64_t peer_sid = p->sb->local_ctrl_uni_stream_id;
    bytes_t setup;
    REQUIRE(recorded(&p->sep, peer_sid, &setup));
    REQUIRE(setup.len > 2 && setup.data[0] == 0xaf && setup.fins == 0);
    REQUIRE(moq_session_start(p->c, p->now) == MOQ_OK);
    moq_stream_ref_t ref = queued_open(p->c, MOQ_ACTION_OPEN_UNI_CONTROL);
    REQUIRE(ref._v != 0);
    bytes_t expected;
    REQUIRE(snapshot(p->c, ref, &expected));
    uint64_t sid = p->cep.next_uni_id;
    if (retry) {
        p->cep.block_open_uni = true;
        REQUIRE(service(p->cb, &p->now));
        REQUIRE(p->cb->pending_count == 1 && p->cb->pending[0].kind == PENDING_OPEN_UNI_CONTROL);
        REQUIRE(p->cep.block_count > 0);
        p->cep.block_open_uni = false;
    }
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(p->cb, peer_sid,
        setup.data, 1, false, p->now) == MOQ_OK);
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(p->cb, 3900,
        setup.data, 1, false, p->now) == MOQ_OK);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(service(p->cb, &p->now));
    REQUIRE(bounded_idle(p, 2));
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(p->cb, peer_sid,
        setup.data + 1, setup.len - 1, false, p->now) == MOQ_OK);
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_transport_bridge_stream_is_control(p->cb, peer_sid));
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(p->cb, 3900,
        MOQ_STREAM_ERROR_CANCELLED, ++p->now) == MOQ_OK);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 1);
    for (unsigned i = 0; i < 3; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_UNI, sid, 0) == 1);
    bytes_t actual;
    REQUIRE(recorded(&p->cep, sid, &actual));
    REQUIRE(actual.len == expected.len && actual.fins == 0);
    REQUIRE(memcmp(actual.data, expected.data, expected.len) == 0);
    REQUIRE(p->cb->local_ctrl_uni_open && p->cb->local_ctrl_uni_stream_id == sid);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(p->cb));
    return true;
}

static size_t core_fin_count(const moq_session_t *s, moq_stream_ref_t ref)
{
    size_t n = 0;
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.fin &&
            a->u.send_data.stream_ref._v == ref._v) ++n;
    }
    return n;
}

static size_t dstream_fin_count(const moq_transport_bridge_t *b,
                               moq_stream_ref_t ref)
{
    size_t n = 0;
    for (uint32_t i = 0; i < b->dstream_cap; ++i) {
        const bridge_dstream_t *d = &b->dstreams[i];
        if (!d->active || d->ref._v != ref._v) continue;
        for (uint32_t j = 0; j < d->q_len; ++j) {
            const moq_action_t *a = &d->q[(d->q_head + j) % d->q_cap].action;
            if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.fin &&
                a->u.send_data.stream_ref._v == ref._v) ++n;
        }
    }
    return n;
}

/* A map-only OPEN occupies the sole generic pending slot. The FIN that can
 * free its map entry is either still core-owned or already scheduler-owned.
 * No mapping is freed by the test; only accepting that FIN can admit B. */
static bool full_pending_fin(fixture_t *f, bool in_dstream)
{
    rl_pair_t *p = &f->p;
    ADMISSION_SETUP(output_setup(f, 1));
    ADMISSION_SETUP(p->cb->max_pending == 1);
    ADMISSION_SETUP(moq_transport_bridge_stream_count(p->cb) == 4);
    fake_block_stream(&p->cep, f->existing_sid);
    if (in_dstream) {
        /* Prime the real scheduler with a refused FIN before filling pending.
         * A corrected pump cannot complete it while the endpoint refuses it. */
        ADMISSION_SETUP(moq_session_close_subgroup(p->c, f->existing,
                                                   ++p->now) == MOQ_OK);
        ADMISSION_SETUP(service(p->cb, &p->now));
        ADMISSION_SETUP(core_fin_count(p->c, f->existing_ref) == 0);
        ADMISSION_SETUP(dstream_fin_count(p->cb, f->existing_ref) == 1);
        ADMISSION_SETUP(p->cb->pending_count == 0 && p->cep.block_count > 0);
    }

    moq_track_status_handle_t request;
    ADMISSION_SETUP(queue_status(p->c, ++p->now, &request));
    moq_stream_ref_t target = queued_open(p->c, MOQ_ACTION_OPEN_BIDI_STREAM);
    ADMISSION_SETUP(target._v != 0);
    bytes_t expected;
    ADMISSION_SETUP(snapshot(p->c, target, &expected));
    ADMISSION_SETUP(expected.fins == 1);
    uint64_t sid = p->cep.next_bidi_id;
    p->cep.block_open_bidi = true;
    int refusals = p->cep.block_count;
    ADMISSION_SETUP(service(p->cb, &p->now));
    ADMISSION_SETUP(p->cep.block_count > refusals);
    ADMISSION_SETUP(p->cb->pending_count == 1);
    ADMISSION_SETUP(p->cb->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    ADMISSION_SETUP(p->cb->pending[0].stream_ref._v == target._v);
    ADMISSION_SETUP(bridge_find_by_ref(p->cb, target) == NULL);
    ADMISSION_SETUP(count_op(&p->cep, FAKE_OP_OPEN_BIDI, sid, 0) == 0);
    ADMISSION_SETUP(count_op(&p->cep, FAKE_OP_WRITE, f->existing_sid, 0) == 0);
    ADMISSION_SETUP(occupy_data(p->cb, p->c, 3900, ++p->now));
    ADMISSION_SETUP(moq_transport_bridge_stream_count(p->cb) == 5);
    if (!in_dstream) {
        ADMISSION_SETUP(moq_session_close_subgroup(p->c, f->existing,
                                                   ++p->now) == MOQ_OK);
        ADMISSION_SETUP(core_fin_count(p->c, f->existing_ref) == 1);
        ADMISSION_SETUP(dstream_fin_count(p->cb, f->existing_ref) == 0);
    } else {
        ADMISSION_SETUP(core_fin_count(p->c, f->existing_ref) == 0);
        ADMISSION_SETUP(dstream_fin_count(p->cb, f->existing_ref) == 1);
    }

    /* From here both endpoint operations can succeed. A true outbound-ready
     * answer must lead to progress, not repeated service with the same FIN. */
    p->cep.block_open_bidi = false;
    fake_unblock_stream(&p->cep);
    if (in_dstream) REQUIRE(moq_transport_bridge_has_outbound_pending(p->cb));
    refusals = p->cep.block_count;
    for (unsigned i = 0; i < 4; ++i) {
        REQUIRE(service(p->cb, &p->now));
        REQUIRE(p->cb->pending_count <= 1);
        REQUIRE(p->cep.block_count == refusals);
    }
    bytes_t closed;
    REQUIRE(recorded(&p->cep, f->existing_sid, &closed));
    REQUIRE(closed.len == 0 && closed.fins == 1);
    REQUIRE(bridge_find_by_ref(p->cb, f->existing_ref) == NULL);
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, sid, 0) == 1);
    bytes_t actual;
    REQUIRE(recorded(&p->cep, sid, &actual));
    REQUIRE(actual.len == expected.len && actual.fins == 1);
    REQUIRE(memcmp(actual.data, expected.data, expected.len) == 0);
    size_t fin_at = SIZE_MAX, open_at = SIZE_MAX;
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == f->existing_sid && op->fin)
            fin_at = i;
        if (op->kind == FAKE_OP_OPEN_BIDI && op->stream_id == sid) open_at = i;
    }
    REQUIRE(fin_at != SIZE_MAX && open_at > fin_at);
    REQUIRE(p->cb->pending_count == 0 && p->cb->dq_items == 0);
    REQUIRE(p->c->action_head == p->c->action_tail);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(p->cb));
    size_t before = p->cep.count;
    for (unsigned i = 0; i < 3; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cep.count == before);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(p->cb));
    return true;
}

static const bridge_dstream_t *data_queue(const moq_transport_bridge_t *b,
                                          moq_stream_ref_t ref)
{
    for (uint32_t i = 0; i < b->dstream_cap; ++i)
        if (b->dstreams[i].active && b->dstreams[i].ref._v == ref._v)
            return &b->dstreams[i];
    return NULL;
}

/* The generic queue is full throughout partial DATA progress. The accepted
 * object header cannot be offered twice, and its refused payload must remain
 * owned at the same scheduler head ahead of the original FIN. */
static bool full_pending_payload(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    ADMISSION_SETUP(output_setup(f, 1));
    fake_block_stream(&p->cep, f->existing_sid);
    ADMISSION_SETUP(write_object(p, f->existing, 1));
    ADMISSION_SETUP(moq_session_close_subgroup(p->c, f->existing,
                                               ++p->now) == MOQ_OK);
    bytes_t expected_data;
    ADMISSION_SETUP(snapshot(p->c, f->existing_ref, &expected_data));
    ADMISSION_SETUP(expected_data.fins == 1);
    ADMISSION_SETUP(service(p->cb, &p->now));
    const bridge_dstream_t *d = data_queue(p->cb, f->existing_ref);
    ADMISSION_SETUP(d != NULL && d->q_len == 2 && d->retained == 0);
    ADMISSION_SETUP(p->cb->pending_count == 0 && p->cb->dq_items == 2);
    ADMISSION_SETUP(p->c->action_head == p->c->action_tail);
    ADMISSION_SETUP(dstream_fin_count(p->cb, f->existing_ref) == 1);
    const bridge_data_item_t *head = &d->q[d->q_head];
    ADMISSION_SETUP(head->action.kind == MOQ_ACTION_SEND_DATA);
    moq_rcbuf_t *payload = head->action.u.send_data.payload;
    size_t header_len = head->action.u.send_data.header_len;
    ADMISSION_SETUP(payload != NULL && header_len > 0);
    size_t payload_len = moq_rcbuf_len(payload);
    ADMISSION_SETUP(payload_len > 0 && moq_rcbuf_refcount(payload) == 1);
    ADMISSION_SETUP(p->cb->dq_bytes == header_len + payload_len);
    ADMISSION_SETUP(expected_data.len == header_len + payload_len);
    uint32_t head_at = d->q_head;
    moq_action_meta_t meta = head->meta;
    const bridge_stream_entry_t *e = bridge_find_by_ref(p->cb, f->existing_ref);
    ADMISSION_SETUP(e != NULL && e->bytes_sent > 0 && e->subgroup_header_len > 0);
    uint64_t sent_before = e->bytes_sent;
    uint8_t reliable_prefix = e->subgroup_header_len;

    moq_track_status_handle_t request;
    ADMISSION_SETUP(queue_status(p->c, ++p->now, &request));
    moq_stream_ref_t target = queued_open(p->c, MOQ_ACTION_OPEN_BIDI_STREAM);
    ADMISSION_SETUP(target._v != 0);
    bytes_t expected_request;
    ADMISSION_SETUP(snapshot(p->c, target, &expected_request));
    uint64_t sid = p->cep.next_bidi_id;
    p->cep.block_open_bidi = true;
    ADMISSION_SETUP(service(p->cb, &p->now));
    ADMISSION_SETUP(p->cb->pending_count == 1);
    ADMISSION_SETUP(p->cb->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    ADMISSION_SETUP(p->cb->pending[0].stream_ref._v == target._v);
    ADMISSION_SETUP(occupy_data(p->cb, p->c, 3900, ++p->now));
    ADMISSION_SETUP(moq_transport_bridge_stream_count(p->cb) == 5);
    ADMISSION_SETUP(p->cep.count == 0);
    p->cep.block_open_bidi = false;
    fake_unblock_stream(&p->cep);
    payload_gate_sid = f->existing_sid;
    payload_gate_data = moq_rcbuf_data(payload);
    payload_gate_len = payload_len;

    for (unsigned i = 0; i < 3; ++i) {
        unsigned refused = payload_refusals;
        REQUIRE(service(p->cb, &p->now));
        REQUIRE(payload_refusals > refused);
        bytes_t actual;
        REQUIRE(recorded(&p->cep, f->existing_sid, &actual));
        REQUIRE(actual.len == header_len && actual.fins == 0);
        REQUIRE(memcmp(actual.data, expected_data.data, header_len) == 0);
        REQUIRE(count_op(&p->cep, FAKE_OP_WRITE, f->existing_sid, 0) == 1);
        REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, sid, 0) == 0);
        REQUIRE(p->cb->pending_count == 1 && p->cb->max_pending == 1);
        REQUIRE(p->cb->pending[0].stream_ref._v == target._v);
        d = data_queue(p->cb, f->existing_ref);
        REQUIRE(d != NULL && d->q_head == head_at && d->q_len == 2);
        REQUIRE(d->retained == 0 && p->cb->dq_items == 2);
        REQUIRE(p->cb->dq_bytes == payload_len);
        head = &d->q[d->q_head];
        REQUIRE(head->action.kind == MOQ_ACTION_SEND_DATA);
        REQUIRE(head->action.u.send_data.stream_ref._v == f->existing_ref._v);
        REQUIRE(head->action.u.send_data.payload == payload);
        REQUIRE(moq_rcbuf_refcount(head->action.u.send_data.payload) == 1);
        REQUIRE(head->meta.owner == meta.owner && head->meta.object_id == meta.object_id);
        REQUIRE(head->meta.deadline_us == meta.deadline_us &&
                head->meta.subgroup_header == meta.subgroup_header);
        REQUIRE(dstream_fin_count(p->cb, f->existing_ref) == 1);
        e = bridge_find_by_ref(p->cb, f->existing_ref);
        REQUIRE(e != NULL && e->bytes_sent == sent_before + header_len);
        REQUIRE(e->subgroup_header_len == reliable_prefix);
        REQUIRE(moq_transport_bridge_has_outbound_pending(p->cb));
    }
    payload_gate_sid = UINT64_MAX;
    payload_gate_data = NULL;
    for (unsigned i = 0; i < 4; ++i) REQUIRE(service(p->cb, &p->now));
    bytes_t actual_data, actual_request;
    REQUIRE(recorded(&p->cep, f->existing_sid, &actual_data));
    REQUIRE(actual_data.len == expected_data.len && actual_data.fins == 1);
    REQUIRE(memcmp(actual_data.data, expected_data.data, expected_data.len) == 0);
    REQUIRE(bridge_find_by_ref(p->cb, f->existing_ref) == NULL);
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, sid, 0) == 1);
    REQUIRE(recorded(&p->cep, sid, &actual_request));
    REQUIRE(actual_request.len == expected_request.len && actual_request.fins == 1);
    REQUIRE(memcmp(actual_request.data, expected_request.data, expected_request.len) == 0);
    size_t fin_at = SIZE_MAX, open_at = SIZE_MAX;
    for (size_t i = 0; i < p->cep.count; ++i) {
        const fake_op_t *op = &p->cep.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == f->existing_sid && op->fin)
            fin_at = i;
        if (op->kind == FAKE_OP_OPEN_BIDI && op->stream_id == sid) open_at = i;
    }
    REQUIRE(fin_at != SIZE_MAX && open_at > fin_at);
    REQUIRE(p->cb->pending_count == 0 && p->cb->dq_items == 0 && p->cb->dq_bytes == 0);
    REQUIRE(p->c->action_head == p->c->action_tail);
    size_t before = p->cep.count;
    for (unsigned i = 0; i < 3; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cep.count == before && !moq_transport_bridge_has_outbound_pending(p->cb));
    return true;
}

/* An unrelated request remains at the core head while generic retention is
 * full. It cannot consume the sole slot or prevent the later mapped FIN from
 * releasing capacity. Its original bytes must still survive that selection. */
static bool full_pending_request_ahead(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    ADMISSION_SETUP(output_setup(f, 1));
    moq_track_status_handle_t first, second;
    ADMISSION_SETUP(queue_status(p->c, ++p->now, &first));
    moq_stream_ref_t first_ref = queued_open(p->c, MOQ_ACTION_OPEN_BIDI_STREAM);
    bytes_t expected_first, expected_second;
    ADMISSION_SETUP(first_ref._v != 0 && snapshot(p->c, first_ref, &expected_first));
    uint64_t first_sid = p->cep.next_bidi_id;
    p->cep.block_open_bidi = true;
    ADMISSION_SETUP(service(p->cb, &p->now));
    ADMISSION_SETUP(p->cb->pending_count == 1);
    ADMISSION_SETUP(p->cb->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    ADMISSION_SETUP(p->cb->pending[0].stream_ref._v == first_ref._v);
    ADMISSION_SETUP(occupy_data(p->cb, p->c, 3900, ++p->now));
    ADMISSION_SETUP(moq_transport_bridge_stream_count(p->cb) == 5);
    ADMISSION_SETUP(queue_status(p->c, ++p->now, &second));
    moq_stream_ref_t second_ref = queued_open(p->c, MOQ_ACTION_OPEN_BIDI_STREAM);
    ADMISSION_SETUP(second_ref._v != 0 && second_ref._v != first_ref._v);
    ADMISSION_SETUP(snapshot(p->c, second_ref, &expected_second));
    ADMISSION_SETUP(moq_session_close_subgroup(p->c, f->existing, ++p->now) == MOQ_OK);
    ADMISSION_SETUP(core_fin_count(p->c, f->existing_ref) == 1);
    ADMISSION_SETUP(dstream_fin_count(p->cb, f->existing_ref) == 0);
    const moq_action_t *front = &p->c->actions[p->c->action_head % p->c->action_cap];
    ADMISSION_SETUP(front->kind == MOQ_ACTION_OPEN_BIDI_STREAM);
    ADMISSION_SETUP(front->u.open_bidi_stream.stream_ref._v == second_ref._v);
    ADMISSION_SETUP(p->cep.count == 0);
    p->cep.block_open_bidi = false;

    for (unsigned i = 0; i < 4; ++i) {
        REQUIRE(service(p->cb, &p->now));
        REQUIRE(p->cb->pending_count <= 1);
    }
    bytes_t actual;
    REQUIRE(recorded(&p->cep, f->existing_sid, &actual));
    REQUIRE(actual.len == 0 && actual.fins == 1);
    REQUIRE(bridge_find_by_ref(p->cb, f->existing_ref) == NULL);
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, first_sid, 0) == 1);
    REQUIRE(recorded(&p->cep, first_sid, &actual));
    REQUIRE(actual.len == expected_first.len && actual.fins == 1);
    REQUIRE(memcmp(actual.data, expected_first.data, actual.len) == 0);
    REQUIRE(bridge_find_by_ref(p->cb, second_ref) == NULL);
    REQUIRE(p->cep.next_bidi_id == first_sid + 1);
    REQUIRE(bounded_idle(p, 5));

    /* The first request's send FIN is already accepted. Its peer RESET is a
     * real completion of the other half and releases the next mapping. */
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(p->cb, first_sid,
        MOQ_STREAM_ERROR_CANCELLED, ++p->now) == MOQ_OK);
    for (unsigned i = 0; i < 4; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(bridge_find_by_ref(p->cb, first_ref) == NULL);
    const bridge_stream_entry_t *e = bridge_find_by_ref(p->cb, second_ref);
    REQUIRE(e != NULL && e->transport_id == first_sid + 1);
    REQUIRE(count_op(&p->cep, FAKE_OP_OPEN_BIDI, first_sid + 1, 0) == 1);
    REQUIRE(recorded(&p->cep, first_sid + 1, &actual));
    REQUIRE(actual.len == expected_second.len && actual.fins == 1);
    REQUIRE(memcmp(actual.data, expected_second.data, actual.len) == 0);
    REQUIRE(p->cb->pending_count == 0 && p->cb->dq_items == 0);
    REQUIRE(p->c->action_head == p->c->action_tail);
    size_t before = p->cep.count;
    for (unsigned i = 0; i < 3; ++i) REQUIRE(service(p->cb, &p->now));
    REQUIRE(p->cep.count == before && !moq_transport_bridge_has_outbound_pending(p->cb));
    return true;
}

static const char *const names[] = {
    "d16-hold", "d16-legacy", "bidi-full", "bidi-retry", "uni-picker",
    "uni-retry", "pending-terminal", "control-full", "control-retry",
    "full-core-fin", "full-dq-fin", "full-payload", "full-request-ahead"
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
        payload_gate_sid = UINT64_MAX;
        payload_gate_data = NULL;
        payload_gate_len = 0;
        payload_refusals = 0;
        fixture_t *f = calloc(1, sizeof(*f));
        if (!f) { fprintf(stderr, "fixture allocation failed\n"); return 2; }
        int before = failures;
        bool ok;
        if (i < 2) ok = inbound_d16(f, i == 0);
        else if (i < 6) ok = outgoing(f, i >= 4, (i & 1u) != 0);
        else if (i == 6) ok = pending_terminal(f);
        else if (i < 9) ok = control_open(f, i == 8);
        else if (i < 11) ok = full_pending_fin(f, i == 10);
        else if (i == 11) ok = full_pending_payload(f);
        else ok = full_pending_request_ahead(f);
        if (f->p.cep.overflowed || f->p.cep.truncated ||
            f->p.sep.overflowed || f->p.sep.truncated) {
            ++failures;
            fprintf(stderr, "FAIL %s: endpoint recorder lost evidence\n", row_name);
        }
        held_bridge_destroy(f->p.cb);
        held_bridge_destroy(f->p.sb);
        moq_session_destroy(f->p.c);
        moq_session_destroy(f->p.s);
        if (f->p.cas.balance != 0 || f->p.sas.balance != 0) {
            ++failures;
            fprintf(stderr, "FAIL %s: allocator balance client=%lld server=%lld\n",
                row_name, (long long)f->p.cas.balance, (long long)f->p.sas.balance);
        }
        free(f);
        if (ok && before == failures) printf("PASS %s\n", row_name);
    }
    if (!selected) { fprintf(stderr, "unknown row\n"); return 2; }
    printf("%u checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
