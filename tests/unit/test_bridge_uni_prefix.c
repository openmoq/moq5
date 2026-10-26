/* Adapter-carried uni prefixes, through real sessions and bridge ingress.
 * Requires the private moq_transport_uni_prefix_t SPI in the matching
 * transport_bridge.h. No local classifier, injected action/event, or writes to
 * bridge/session state. The wire helpers only establish sessions and capture
 * production FETCH output; all prefix calls and refused replays are explicit.
 *
 * This models receive-record storage, not QUIC population/credit or provider
 * release. RESET's memset is the adapter's actual-record-teardown obligation.
 */
static int failures;
#include "../support/bridge_wire_pair.h"
#include "test_binding_support.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include <moq/control_d18.h>
#include <moq/vi64.h>

static const char *row_name;
#define REQUIRE(x) do { if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL %s:%d: %s\n", row_name, __LINE__, #x); \
    return false; } } while (0)

enum { PREFIX_COUNT = 129, MAPS = 4, HOLDS = 2 };
static const uint8_t body[] = {
    0x00, 0xff, 0x91, 0x23, 0x46, 0x57, 0x68, 0x79,
    0x8a, 0x9b, 0xac, 0xbd, 0xce, 0xdf, 0xe0, 0x17
};

typedef struct {
    uint8_t bytes[512];
    size_t len;
    unsigned fins;
} packet_t;

typedef struct {
    rl_pair_t p;
    moq_fetch_t fetch;
    moq_rcbuf_t *input;           /* caller-owned completing callback */
    uint64_t response_sid;
    uint64_t blockers[MAPS];
    size_t nblockers;
    packet_t completion;
    uint8_t first[8];
    unsigned objects, complete, ok, goaway;
    bool separate_fin;             /* a later FIN stays behind refused bytes */
} fixture_t;

static bool fixture_init(fixture_t *f, moq_version_t ver,
                         uint32_t maps, uint32_t events)
{
    rl_pair_t *p = &f->p;
    p->ver = ver;
    p->now = 1000;
    p->client_control_sid = p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = ver;
    cc.max_events = events;
    cc.max_data_streams = sc.max_data_streams = 16;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    cc.idle_timeout_us = sc.idle_timeout_us = 0;
    cc.goaway_timeout_us = sc.goaway_timeout_us = 0;
    REQUIRE(moq_session_create(&cc, p->now, &p->c) == MOQ_OK);
    REQUIRE(moq_session_create(&sc, p->now, &p->s) == MOQ_OK);
    REQUIRE(test_bind_if_required(p->c) == MOQ_OK);
    REQUIRE(test_bind_if_required(p->s) == MOQ_OK);
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT |
                                  MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT |
                                  MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    bc.max_streams = maps;
    bc.max_pending = HOLDS;
    REQUIRE(moq_transport_bridge_create(&bc, p->c, &p->cep.vtable,
                                        &p->cep, &p->cb) == MOQ_OK);
    moq_transport_bridge_cfg_init(&bc, &p->salloc);
    bc.max_streams = 16;
    REQUIRE(moq_transport_bridge_create(&bc, p->s, &p->sep.vtable,
                                        &p->sep, &p->sb) == MOQ_OK);
    return true;
}

static bool healthy(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    REQUIRE(!p->deliver_error);
    REQUIRE(!p->cep.overflowed && !p->cep.truncated);
    REQUIRE(!p->sep.overflowed && !p->sep.truncated);
    REQUIRE(!moq_transport_bridge_is_terminal(p->cb));
    REQUIRE(!moq_transport_bridge_is_terminal(p->sb));
    return true;
}

static bool established(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    REQUIRE(moq_session_start(p->c, p->now) == MOQ_OK);
    REQUIRE(moq_session_start(p->s, p->now) == MOQ_OK);
    shuttle(p);
    REQUIRE(healthy(f));
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    obs_t o = {0};
    drain(p->c, &o);
    drain(p->s, &o);
    REQUIRE(o.setup_complete == 2 && o.closed == 0);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 2);
    return true;
}

static size_t holds(const moq_transport_bridge_t *b)
{
    size_t n = 0;
    for (uint32_t i = 0; i < b->max_holds; ++i)
        if (b->holds[i].state != BRIDGE_HOLD_FREE) ++n;
    return n;
}

static bool empty_prefix(const moq_transport_uni_prefix_t *p)
{
    static const moq_transport_uni_prefix_t zero = {0};
    return memcmp(p, &zero, sizeof(*p)) == 0;
}

/* Construct a legal nonminimal encoding, not a classifier. The values used for
 * SETUP/FETCH below are decoded from production output by the production codec. */
static void vi64_nine(uint8_t out[9], uint64_t value)
{
    out[0] = 0xff;
    for (unsigned i = 0; i < 8; ++i)
        out[8 - i] = (uint8_t)(value >> (8 * i));
}

static moq_result_t fragment(fixture_t *f, uint64_t sid,
    moq_transport_uni_prefix_t *prefix, const uint8_t *data, size_t len,
    bool fin, bool rcbuf)
{
    if (!rcbuf)
        return moq_transport_bridge_on_peer_uni_prefix_bytes(
            f->p.cb, sid, prefix, data, len, fin, f->p.now);
    moq_rcbuf_t *rb = NULL;
    if (len && moq_rcbuf_create(&f->p.calloc, data, len, &rb) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_prefix_rcbuf(
        f->p.cb, sid, prefix, rb, fin, f->p.now);
    if (rb) {
        MOQ_TEST_CHECK(moq_rcbuf_refcount(rb) == 1);
        moq_rcbuf_decref(rb);
    }
    return rc;
}

static bool append(packet_t *p, const uint8_t *data, size_t len, bool fin)
{
    REQUIRE(p->fins == 0 && len <= sizeof(p->bytes) - p->len);
    if (len) memcpy(p->bytes + p->len, data, len);
    p->len += len;
    p->fins += fin ? 1u : 0u;
    return true;
}

/* No unknown stream consumes either reserved mapping or a hold. More records
 * than this fixture's map/H capacity (and the old default 128-map size) retain
 * prefixes simultaneously. This is not a provider population-bound proof. */
static bool population(fixture_t *f, moq_version_t ver, bool rcbuf)
{
    REQUIRE(fixture_init(f, ver, 2, 8));
    rl_pair_t *p = &f->p;
    REQUIRE(moq_session_start(p->s, p->now) == MOQ_OK);
    REQUIRE(moq_transport_bridge_service(p->sb, p->now) == MOQ_OK);
    const fake_op_t *opened = fake_endpoint_find(&p->sep, FAKE_OP_OPEN_UNI);
    REQUIRE(opened != NULL);
    uint64_t sid = opened->stream_id;
    packet_t setup = {0};
    for (size_t i = 0; i < p->sep.count; ++i) {
        const fake_op_t *op = &p->sep.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == sid)
            REQUIRE(append(&setup, op->data, op->data_len, op->fin));
    }
    REQUIRE(healthy(f) && setup.fins == 0);
    uint64_t type = 0;
    size_t nt = moq_vi64_decode(setup.bytes, setup.len, &type);
    REQUIRE(nt > 0 && type == MOQ_D18_STREAM_SETUP);
    uint8_t nine[9];
    vi64_nine(nine, type);
    moq_transport_uni_prefix_t prefixes[PREFIX_COUNT] = {0};
    const uint64_t deadline = moq_session_next_deadline_us(p->c);
    const int64_t balance = p->cas.balance;
    for (unsigned i = 0; i < PREFIX_COUNT; ++i) {
        uint64_t id = i + 1 == PREFIX_COUNT ? sid : 10000 + i;
        REQUIRE(fragment(f, id, &prefixes[i], NULL, 0, false, rcbuf) == MOQ_OK);
        REQUIRE(empty_prefix(&prefixes[i]));
        /* Every split point is exercised on the eventual control stream. */
        for (size_t n = 0; n < 8;) {
            size_t take = i + 1 == PREFIX_COUNT ? 1 : 8;
            REQUIRE(fragment(f, id, &prefixes[i], nine + n, take, false, rcbuf) == MOQ_OK);
            n += take;
            REQUIRE(prefixes[i].len == n && memcmp(prefixes[i].bytes, nine, n) == 0);
        }
        REQUIRE(fragment(f, id, &prefixes[i], NULL, 0, false, rcbuf) == MOQ_OK);
        for (unsigned q = 0; q < 3; ++q)
            REQUIRE(!moq_transport_bridge_stream_has_pending(p->cb, id));
        REQUIRE(moq_transport_bridge_find_ref(p->cb, id)._v == 0);
        REQUIRE(moq_transport_bridge_stream_count(p->cb) == 0 && holds(p->cb) == 0);
        REQUIRE(moq_session_next_deadline_us(p->c) == deadline);
        REQUIRE(p->cas.balance == balance && p->cep.count == 0);
    }
    REQUIRE(!moq_transport_bridge_has_pending(p->cb));
    /* No hold-age cancellation or readiness deadline for parked type bytes. */
    p->now += BRIDGE_HOLD_AGE_US + 1;
    REQUIRE(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
    REQUIRE(healthy(f) && p->cep.count == 0 && holds(p->cb) == 0);
    REQUIRE(moq_session_next_deadline_us(p->c) == deadline);
    for (unsigned i = 0; i < PREFIX_COUNT; ++i)
        REQUIRE(prefixes[i].len == 8 && memcmp(prefixes[i].bytes, nine, 8) == 0);

    REQUIRE(moq_session_start(p->c, p->now) == MOQ_OK); /* queued, not mapped */
    packet_t last = {0};
    REQUIRE(append(&last, nine + 8, 1, false));
    REQUIRE(append(&last, setup.bytes + nt, setup.len - nt, false));
    REQUIRE(fragment(f, sid, &prefixes[PREFIX_COUNT - 1], last.bytes,
                     last.len, false, rcbuf) == MOQ_OK);
    REQUIRE(empty_prefix(&prefixes[PREFIX_COUNT - 1]));
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 1 && holds(p->cb) == 0);
    REQUIRE(p->cb->peer_ctrl_uni_open && !p->cb->local_ctrl_uni_open);
    fake_endpoint_clear_ops(&p->sep); /* this SETUP was just delivered explicitly */
    p->server_control_uni = sid;
    shuttle(p);                      /* local control still has its own slot */
    REQUIRE(healthy(f));
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(p->cb->peer_ctrl_uni_open && p->cb->local_ctrl_uni_open);
    for (unsigned i = 0; i + 1 < PREFIX_COUNT; ++i) {
        REQUIRE(prefixes[i].len == 8 && memcmp(prefixes[i].bytes, nine, 8) == 0);
        REQUIRE(!moq_transport_bridge_stream_has_pending(p->cb, 10000 + i));
    }
    REQUIRE(holds(p->cb) == 0);
    /* Classified padding at the full map uses the existing no-H refusal. */
    moq_transport_uni_prefix_t padding = {0};
    vi64_nine(nine, MOQ_D18_STREAM_PADDING);
    REQUIRE(fragment(f, 15000, &padding, nine, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(fragment(f, 15000, &padding, nine + 8, 1, false, rcbuf) ==
            MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    REQUIRE(holds(p->cb) == 0 && moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(p->cep.vtable.stop_sending(&p->cep, 15000,
              MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == MOQ_TRANSPORT_OK);
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(p->cb, 15000,
              MOQ_STREAM_ERROR_CANCELLED, p->now) == MOQ_OK);
    memset(&padding, 0, sizeof(padding));
    REQUIRE(fake_endpoint_count_kind(&p->cep, FAKE_OP_STOP) == 1);
    REQUIRE(healthy(f));
    return true;
}

/* Produce one real FETCH response with a distinctive object and a trailing FIN;
 * hold only its data wire, so FETCH_OK still reaches the receiver. */
static bool response(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("prefix") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("object");
    fc.end_group = 0;
    fc.end_object = 3;
    REQUIRE(moq_session_fetch(p->c, &fc, p->now, &f->fetch) == MOQ_OK);
    shuttle(p);
    moq_fetch_t source = {0};
    moq_event_t ev;
    unsigned requests = 0, events = 0;
    while (events < 16 && moq_session_poll_events(p->s, &ev, 1)) {
        ++events;
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            source = ev.u.fetch_request.fetch;
            ++requests;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(healthy(f) && requests == 1 && events < 16);
    p->s2c.hold_data = true;
    f->response_sid = p->sep.next_uni_id;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 3;
    REQUIRE(moq_session_accept_fetch(p->s, source, &ac, p->now) == MOQ_OK);
    moq_rcbuf_t *payload = NULL;
    REQUIRE(moq_rcbuf_create(&p->salloc, body, sizeof(body), &payload) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.subgroup_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 100;
    oc.payload = payload;
    moq_result_t rc = moq_session_write_fetch_object(p->s, source, &oc, p->now);
    moq_rcbuf_decref(payload);
    REQUIRE(rc == MOQ_OK);
    REQUIRE(moq_session_end_fetch(p->s, source, p->now) == MOQ_OK);
    shuttle(p);
    REQUIRE(healthy(f));
    packet_t raw = {0};
    for (size_t i = 0; i < p->s2c.n; ++i) {
        const fake_op_t *op = &p->s2c.q[i];
        REQUIRE(op->kind == FAKE_OP_WRITE && op->stream_id == f->response_sid);
        REQUIRE(append(&raw, op->data, op->data_len, op->fin));
    }
    REQUIRE(raw.fins == 1);
    uint64_t type = 0;
    size_t nt = moq_vi64_decode(raw.bytes, raw.len, &type);
    REQUIRE(nt > 0 && type == MOQ_D18_STREAM_FETCH_HEADER);
    uint8_t nine[9];
    vi64_nine(nine, type);
    memcpy(f->first, nine, sizeof(f->first));
    REQUIRE(append(&f->completion, nine + 8, 1, false));
    REQUIRE(append(&f->completion, raw.bytes + nt, raw.len - nt, true));
    p->s2c.n = 0;                  /* all captured bytes stay in completion */
    p->s2c.hold_data = false;
    REQUIRE(moq_rcbuf_create(&p->calloc, f->completion.bytes,
                             f->completion.len, &f->input) == MOQ_OK);
    return true;
}

static bool drain_exact(fixture_t *f)
{
    moq_event_t ev;
    bool exact = true;
    unsigned events = 0;
    while (events < 32 && moq_session_poll_events(f->p.c, &ev, 1)) {
        ++events;
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK: ++f->ok; break;
        case MOQ_EVENT_GOAWAY: ++f->goaway; break;
        case MOQ_EVENT_FETCH_OBJECT: {
            const moq_fetch_object_event_t *o = &ev.u.fetch_object;
            ++f->objects;
            exact &= o->fetch._opaque == f->fetch._opaque && o->group_id == 0 &&
                     o->subgroup_id == 0 && o->object_id == 0;
            exact &= o->payload != NULL && moq_rcbuf_len(o->payload) == sizeof(body);
            if (o->payload && moq_rcbuf_len(o->payload) == sizeof(body))
                exact &= memcmp(moq_rcbuf_data(o->payload), body, sizeof(body)) == 0;
            break;
        }
        case MOQ_EVENT_FETCH_COMPLETE:
            ++f->complete;
            exact &= ev.u.fetch_complete.fetch._opaque == f->fetch._opaque;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
        case MOQ_EVENT_FETCH_RESET:
        case MOQ_EVENT_FETCH_ERROR:
        case MOQ_EVENT_FETCH_CANCELLED:
        case MOQ_EVENT_FETCH_GAP:
            exact = false;
            break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(exact && events < 32);
    return true;
}

static bool fill_maps(fixture_t *f)
{
    while (moq_transport_bridge_stream_count(f->p.cb) < MAPS) {
        REQUIRE(f->nblockers < MAPS);
        uint64_t sid = 20000 + f->nblockers;
        uint8_t type = 0x10;       /* complete subgroup type, incomplete header */
        moq_transport_uni_prefix_t prefix = {0};
        REQUIRE(fragment(f, sid, &prefix, &type, 1, false, false) == MOQ_OK);
        moq_stream_ref_t ref = moq_transport_bridge_find_ref(f->p.cb, sid);
        REQUIRE(ref._v && moq_session_has_transport_stream(f->p.c, ref));
        REQUIRE(empty_prefix(&prefix));
        f->blockers[f->nblockers++] = sid;
    }
    REQUIRE(f->nblockers > 0 && holds(f->p.cb) == 0);
    return true;
}

static moq_result_t complete_input(fixture_t *f,
                                   moq_transport_uni_prefix_t *prefix, bool rcbuf)
{
    return rcbuf
        ? moq_transport_bridge_on_peer_uni_prefix_rcbuf(f->p.cb,
              f->response_sid, prefix, f->input, !f->separate_fin, f->p.now)
        : moq_transport_bridge_on_peer_uni_prefix_bytes(f->p.cb,
              f->response_sid, prefix, f->completion.bytes,
              f->completion.len, !f->separate_fin, f->p.now);
}

static bool refused(fixture_t *f, moq_transport_uni_prefix_t *prefix, bool rcbuf)
{
    REQUIRE(complete_input(f, prefix, rcbuf) == MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(prefix->len == 8 && memcmp(prefix->bytes, f->first, 8) == 0);
    REQUIRE(moq_rcbuf_refcount(f->input) == 1);
    REQUIRE(moq_rcbuf_len(f->input) == f->completion.len);
    REQUIRE(memcmp(moq_rcbuf_data(f->input), f->completion.bytes,
                   f->completion.len) == 0);
    REQUIRE(moq_transport_bridge_find_ref(f->p.cb, f->response_sid)._v == 0);
    REQUIRE(moq_transport_bridge_stream_has_pending(f->p.cb, f->response_sid));
    REQUIRE(holds(f->p.cb) >= 1);
    return true;
}

static bool release_and_replay(fixture_t *f,
                                moq_transport_uni_prefix_t *prefix, bool rcbuf)
{
    rl_pair_t *p = &f->p;
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(p->cb, f->blockers[0],
              MOQ_STREAM_ERROR_CANCELLED, ++p->now) == MOQ_OK);
    REQUIRE(moq_transport_bridge_service(p->cb, ++p->now) == MOQ_OK);
    REQUIRE(!moq_transport_bridge_stream_has_pending(p->cb, f->response_sid));
    REQUIRE(prefix->len == 8);      /* readiness is not acceptance */
    moq_result_t rc = complete_input(f, prefix, rcbuf);
    REQUIRE(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(empty_prefix(prefix));
    REQUIRE(moq_rcbuf_refcount(f->input) == 1);
    if (f->separate_fin) {
        for (unsigned i = 0; i < 4; ++i) {
            REQUIRE(drain_exact(f));
            REQUIRE(moq_transport_bridge_service(p->cb, ++p->now) == MOQ_OK);
        }
        REQUIRE(drain_exact(f) && f->objects == 1 && f->complete == 0);
        REQUIRE(moq_transport_bridge_find_ref(p->cb, f->response_sid)._v != 0);
        /* The same private API must now delegate to the mapped receive stream,
         * not classify this empty FIN as a new/truncated stream type. */
        rc = fragment(f, f->response_sid, prefix, NULL, 0, true, rcbuf);
        REQUIRE(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        REQUIRE(empty_prefix(prefix));
    }
    for (unsigned i = 0; i < 8; ++i) {
        REQUIRE(drain_exact(f));
        REQUIRE(moq_transport_bridge_service(p->cb, ++p->now) == MOQ_OK);
    }
    REQUIRE(drain_exact(f));
    REQUIRE(f->ok == 1 && f->objects == 1 && f->complete == 1);
    REQUIRE(moq_transport_bridge_find_ref(p->cb, f->response_sid)._v == 0);
    REQUIRE(!moq_transport_bridge_stream_has_pending(p->cb, f->response_sid));
    REQUIRE(fake_endpoint_count_kind(&p->cep, FAKE_OP_STOP) == 0);
    REQUIRE(fake_endpoint_count_kind(&p->cep, FAKE_OP_CLOSE) == 0);
    REQUIRE(healthy(f));
    return true;
}

static bool replay(fixture_t *f, moq_version_t ver, bool rcbuf)
{
    REQUIRE(fixture_init(f, ver, MAPS, 8) && established(f) && response(f));
    REQUIRE(drain_exact(f));
    REQUIRE(f->ok == 1 && f->objects == 0 && f->complete == 0);
    REQUIRE(fill_maps(f));
    moq_transport_uni_prefix_t prefix = {0};
    REQUIRE(fragment(f, f->response_sid, &prefix, f->first, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(holds(f->p.cb) == 0);
    REQUIRE(refused(f, &prefix, rcbuf));
    REQUIRE(holds(f->p.cb) == 1);
    const bridge_hold_t *h = NULL;
    for (uint32_t i = 0; i < f->p.cb->max_holds; ++i)
        if (f->p.cb->holds[i].state != BRIDGE_HOLD_FREE &&
            f->p.cb->holds[i].transport_id == f->response_sid)
            h = &f->p.cb->holds[i];
    REQUIRE(h != NULL);
    uint64_t born = h->first_refusal_us, seq = h->seq;
    f->p.now += 7;
    REQUIRE(refused(f, &prefix, rcbuf));
    REQUIRE(holds(f->p.cb) == 1 && h->first_refusal_us == born && h->seq == seq);
    /* A FIN-only call cannot overtake the refused completing bytes. */
    REQUIRE(fragment(f, f->response_sid, &prefix, NULL, 0, true, rcbuf) ==
            MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(prefix.len == 8 && memcmp(prefix.bytes, f->first, 8) == 0);
    REQUIRE(drain_exact(f) && f->objects == 0 && f->complete == 0);
    REQUIRE(release_and_replay(f, &prefix, rcbuf));
    REQUIRE(holds(f->p.cb) == 0);
    return true;
}

static bool replay_later_fin(fixture_t *f, moq_version_t ver, bool rcbuf)
{
    f->separate_fin = true;
    return replay(f, ver, rcbuf);
}

static bool pending_control(fixture_t *f, moq_version_t ver, bool rcbuf)
{
    REQUIRE(fixture_init(f, ver, MAPS, 1) && established(f) && response(f));
    REQUIRE(fill_maps(f));
    REQUIRE(f->p.c->event_tail - f->p.c->event_head == 1); /* real FETCH_OK */
    REQUIRE(moq_session_goaway(f->p.s, NULL, 0, f->p.now) == MOQ_OK);
    shuttle(&f->p);                /* GOAWAY cannot enter the full event queue */
    REQUIRE(healthy(f) && f->p.cb->pending_control);
    moq_transport_uni_prefix_t prefix = {0}, second = {0}, excess = {0};
    REQUIRE(fragment(f, f->response_sid, &prefix, f->first, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(prefix.len == 8 && holds(f->p.cb) == 0);
    REQUIRE(refused(f, &prefix, rcbuf) && holds(f->p.cb) == 1);
    REQUIRE(fragment(f, 30000, &second, f->first, 8, false, rcbuf) == MOQ_OK);
    uint8_t last = (uint8_t)MOQ_D18_STREAM_FETCH_HEADER;
    REQUIRE(fragment(f, 30000, &second, &last, 1, false, rcbuf) == MOQ_ERR_INPUT_NOT_CONSUMED);
    REQUIRE(holds(f->p.cb) == HOLDS);
    REQUIRE(fragment(f, 30001, &excess, f->first, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(fragment(f, 30001, &excess, &last, 1, false, rcbuf) ==
            MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    REQUIRE(holds(f->p.cb) == HOLDS && f->p.cb->pending_control);
    REQUIRE(moq_transport_bridge_stream_count(f->p.cb) == MAPS);
    REQUIRE(moq_transport_bridge_find_ref(f->p.cb, 30001)._v == 0);
    /* Adapter disposition for the now-identified excess DATA, then real RESET. */
    REQUIRE(f->p.cep.vtable.stop_sending(&f->p.cep, 30001,
                                        MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == MOQ_TRANSPORT_OK);
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(f->p.cb, 30001,
              MOQ_STREAM_ERROR_CANCELLED, f->p.now) == MOQ_OK);
    memset(&excess, 0, sizeof(excess));
    REQUIRE(fake_endpoint_count_kind(&f->p.cep, FAKE_OP_STOP) == 1);
    fake_endpoint_clear_ops(&f->p.cep);
    const uint64_t deadline = moq_session_next_deadline_us(f->p.c);
    for (unsigned i = 0; i < 8; ++i) {
        REQUIRE(moq_transport_bridge_stream_has_pending(f->p.cb, f->response_sid));
        REQUIRE(moq_session_next_deadline_us(f->p.c) == deadline);
    }
    REQUIRE(f->p.c->event_tail - f->p.c->event_head == 1);
    REQUIRE(f->p.cep.count == 0);   /* queries neither feed nor drive service */
    REQUIRE(refused(f, &prefix, rcbuf));
    REQUIRE(drain_exact(f) && f->ok == 1 && f->objects == 0 && f->complete == 0);
    REQUIRE(moq_transport_bridge_service(f->p.cb, ++f->p.now) == MOQ_OK);
    REQUIRE(drain_exact(f) && f->goaway == 1 && f->objects == 0 && f->complete == 0);
    REQUIRE(!f->p.cb->pending_control && holds(f->p.cb) == HOLDS);
    REQUIRE(release_and_replay(f, &prefix, rcbuf));
    REQUIRE(holds(f->p.cb) == 1);
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(f->p.cb, 30000,
              MOQ_STREAM_ERROR_CANCELLED, f->p.now) == MOQ_OK);
    memset(&second, 0, sizeof(second));
    REQUIRE(holds(f->p.cb) == 0);
    return true;
}

static bool reset_reuse(fixture_t *f, moq_version_t ver, bool rcbuf)
{
    REQUIRE(fixture_init(f, ver, MAPS, 8) && established(f));
    uint8_t nine[9];
    vi64_nine(nine, MOQ_D18_STREAM_SETUP);
    moq_transport_uni_prefix_t a = {0}, b = {0};
    REQUIRE(fragment(f, 31000, &a, nine, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(fragment(f, 31001, &b, nine, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(f->p.cb, 31000,
              MOQ_STREAM_ERROR_CANCELLED, f->p.now) == MOQ_OK);
    memset(&a, 0, sizeof(a));       /* receive-record teardown, not bridge magic */
    REQUIRE(b.len == 8 && memcmp(b.bytes, nine, 8) == 0);
    REQUIRE(moq_transport_bridge_stream_count(f->p.cb) == 2 && holds(f->p.cb) == 0);
    uint8_t type = 0x10;
    REQUIRE(fragment(f, 31002, &a, &type, 1, false, rcbuf) == MOQ_OK);
    moq_stream_ref_t ref = moq_transport_bridge_find_ref(f->p.cb, 31002);
    REQUIRE(empty_prefix(&a) && ref._v && moq_session_has_transport_stream(f->p.c, ref));
    REQUIRE(fragment(f, 31002, &a, NULL, 0, false, rcbuf) == MOQ_OK); /* mapped path */
    REQUIRE(moq_transport_bridge_find_ref(f->p.cb, 31002)._v == ref._v);
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(f->p.cb, 31002,
              MOQ_STREAM_ERROR_CANCELLED, f->p.now) == MOQ_OK);
    memset(&a, 0, sizeof(a));
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(f->p.cb, 31001,
              MOQ_STREAM_ERROR_CANCELLED, f->p.now) == MOQ_OK);
    memset(&b, 0, sizeof(b));
    REQUIRE(moq_transport_bridge_stream_count(f->p.cb) == 2 && holds(f->p.cb) == 0);
    REQUIRE(fake_endpoint_count_kind(&f->p.cep, FAKE_OP_STOP) == 0);
    REQUIRE(healthy(f));
    return true;
}

/* Classification errors are not resource cancellation. A duplicate control
 * needs no second SETUP body: the completed type itself proves the violation. */
static bool protocol_error(fixture_t *f, moq_version_t ver, bool rcbuf, unsigned which)
{
    REQUIRE(fixture_init(f, ver, MAPS, 1) && established(f) && response(f));
    REQUIRE(moq_session_goaway(f->p.s, NULL, 0, f->p.now) == MOQ_OK);
    shuttle(&f->p);
    REQUIRE(healthy(f) && f->p.cb->pending_control);
    size_t mapped = moq_transport_bridge_stream_count(f->p.cb);
    uint8_t nine[9];
    vi64_nine(nine, which == 1 ? UINT64_MAX : MOQ_D18_STREAM_SETUP);
    moq_transport_uni_prefix_t prefix = {0};
    REQUIRE(fragment(f, 32000, &prefix, nine, 8, false, rcbuf) == MOQ_OK);
    REQUIRE(moq_transport_bridge_stream_count(f->p.cb) == mapped && holds(f->p.cb) == 0);
    moq_result_t rc = which == 0
        ? fragment(f, 32000, &prefix, NULL, 0, true, rcbuf)
        : fragment(f, 32000, &prefix, nine + 8, 1, false, rcbuf);
    REQUIRE(rc == MOQ_ERR_PROTO && moq_transport_bridge_is_fatal(f->p.cb));
    REQUIRE(moq_transport_bridge_fatal_code(f->p.cb) == 0x3);
    REQUIRE(fake_endpoint_count_kind(&f->p.cep, FAKE_OP_STOP) == 0);
    return true;
}

static void fixture_close(fixture_t *f)
{
    if (f->input) moq_rcbuf_decref(f->input);
    moq_transport_bridge_destroy(f->p.cb);
    moq_transport_bridge_destroy(f->p.sb);
    moq_session_destroy(f->p.c);
    moq_session_destroy(f->p.s);
    MOQ_TEST_CHECK(f->p.cas.balance == 0);
    MOQ_TEST_CHECK(f->p.sas.balance == 0);
}

int main(void)
{
    static const moq_version_t versions[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    static const struct {
        const char *name;
        bool (*run)(fixture_t *, moq_version_t, bool);
    } rows[] = {
        { "population/two-controls/zero-call/no-age", population },
        { "DATA-exact-refusal-replay-FIN", replay },
        { "DATA-refusal-later-FIN-mapped-path", replay_later_fin },
        { "pending-control-bounded-placement", pending_control },
        { "RESET-record-drop-reuse", reset_reuse }
    };
    static const char *errors[] = { "truncated-FIN", "invalid-type", "duplicate-control" };
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
        for (unsigned r = 0; r < 2; ++r) {
            for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
                fixture_t f = {0};
                row_name = rows[i].name;
                int before = failures;
                bool ok = rows[i].run(&f, versions[v], r != 0);
                fixture_close(&f); /* also after a failed precondition */
                printf("%s prefix %s draft=%u input=%s\n",
                       ok && failures == before ? "PASS" : "FAIL", row_name,
                       versions[v] == MOQ_VERSION_DRAFT_18 ? 18u : 22u,
                       r ? "rcbuf" : "bytes");
            }
            for (unsigned i = 0; i < 3; ++i) {
                fixture_t f = {0};
                row_name = errors[i];
                int before = failures;
                bool ok = protocol_error(&f, versions[v], r != 0, i);
                fixture_close(&f);
                printf("%s prefix %s draft=%u input=%s\n",
                       ok && failures == before ? "PASS" : "FAIL", row_name,
                       versions[v] == MOQ_VERSION_DRAFT_18 ? 18u : 22u,
                       r ? "rcbuf" : "bytes");
            }
        }
    }
    return failures ? 1 : 0;
}
