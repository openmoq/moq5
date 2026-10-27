/*
 * The bridge's reliable reset (MOQ_TRANSPORT_CAP_RELIABLE_RESET).
 *
 * A data-stream reset the session asks to keep its stream header reaches an
 * endpoint that offers reset_stream_at as RESET_STREAM_AT with exactly that
 * reliable size. Without the capability, or on a connection that did not
 * negotiate the extension (the endpoint answers DROPPED), the bridge sends a
 * plain RESET_STREAM -- the fallback the protocol is designed for. Pinned:
 *
 *   - capability present: one reset_stream_at carrying the header length the
 *     stream's first write had, no plain reset (draft-16, -18, -22);
 *   - capability absent: one plain reset, reset_stream_at never called;
 *   - not negotiated: reset_stream_at tried once, then one plain reset;
 *   - the header still retained under WOULD_BLOCK (a blocked write, or an
 *     open_uni that has not happened yet): the reset waits behind it, so the
 *     header is handed over first and the reset still keeps it -- and a
 *     stream whose open was retained is reset at all once it opens;
 *   - reset_stream_at answering WOULD_BLOCK is retried with the same size;
 *   - the reliable size never exceeds the bytes the stream was handed, and a
 *     RESET_DATA whose detail predates the field is a plain reset;
 *   - end to end over a pair whose transport abandons stream data a plain
 *     reset overtakes (RFC 9000 §3.1): a reset subgroup still counts toward
 *     PUBLISH_DONE's Stream Count and a fill failed on the wire still ends
 *     the subscriber's fill only when the reset is reliable.
 */
#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include "test_binding_support.h"
#include "../support/held_bridge_driver.h"
#include "../../core/src/session/session_internal.h"
#include <stddef.h>
#include <string.h>

static int failures;

/* -- Pair over fake endpoints ------------------------------------------------ */

#define CLIENT_UNI  1000u
#define CLIENT_BIDI 2000u
#define SERVER_UNI  3000u
#define SERVER_BIDI 4000u
#define MAX_TRACKED 64

typedef struct pair {
    moq_version_t           v;
    moq_session_t          *c, *sv;
    fake_endpoint_t         cep, sep;
    moq_transport_bridge_t *cb, *sb;
    uint64_t                now;
    uint64_t                c_ctrl, s_ctrl;
    /* Lossy transport: a plain reset overtakes the stream data written in
     * the same service pass, which is then never delivered; a reliable reset
     * still delivers that data up to its reliable size. */
    bool                    lossy;
    struct { uint64_t sid, delivered; } off[MAX_TRACKED];
    size_t                  noff;
    unsigned                tracks;
} pair_t;

static bool is_bidi(uint64_t sid)
{
    return (sid >= CLIENT_BIDI && sid < SERVER_UNI) || sid >= SERVER_BIDI;
}

static uint64_t *delivered_of(pair_t *p, uint64_t sid)
{
    for (size_t i = 0; i < p->noff; i++)
        if (p->off[i].sid == sid) return &p->off[i].delivered;
    if (p->noff == MAX_TRACKED) return NULL;
    p->off[p->noff].sid = sid;
    p->off[p->noff].delivered = 0;
    return &p->off[p->noff++].delivered;
}

/* The reset that ends `sid` later in this batch: kind, and reliable size. */
static const fake_op_t *later_reset(const fake_endpoint_t *ep, size_t from, uint64_t sid)
{
    for (size_t j = from; j < ep->count; j++)
        if ((ep->ops[j].kind == FAKE_OP_RESET || ep->ops[j].kind == FAKE_OP_RESET_AT) &&
            ep->ops[j].stream_id == sid)
            return &ep->ops[j];
    return NULL;
}

static void deliver(pair_t *p, fake_endpoint_t *from, moq_transport_bridge_t *to, bool from_client)
{
    bool uni_control = moq_transport_bridge_uses_uni_control(to);
    for (size_t i = 0; i < from->count; i++) {
        const fake_op_t *o = &from->ops[i];
        switch (o->kind) {
        case FAKE_OP_WRITE: {
            if (is_bidi(o->stream_id)) {
                if (!uni_control) {
                    uint64_t *ctrl = from_client ? &p->c_ctrl : &p->s_ctrl;
                    if (from_client && *ctrl == UINT64_MAX) *ctrl = o->stream_id;
                    if (o->stream_id == p->c_ctrl) {
                        moq_transport_bridge_on_peer_control_bytes(to, o->stream_id, o->data,
                                                                   o->data_len, o->fin, p->now);
                        break;
                    }
                }
                moq_transport_bridge_on_peer_bidi_bytes(to, o->stream_id, o->data, o->data_len,
                                                        o->fin, p->now);
                break;
            }
            size_t len = o->data_len;
            bool fin = o->fin;
            const fake_op_t *rst = later_reset(from, i + 1, o->stream_id);
            uint64_t *done = delivered_of(p, o->stream_id);
            if (p->lossy && rst && done) {
                uint64_t keep = rst->kind == FAKE_OP_RESET_AT ? rst->reliable_size : 0;
                uint64_t room = keep > *done ? keep - *done : 0;
                if (room < len) { len = (size_t)room; fin = false; }
            }
            if (len == 0 && !fin) break;
            if (done) *done += len;
            held_bridge_uni_bytes(to, o->stream_id, o->data, len, fin, p->now);
            break;
        }
        case FAKE_OP_RESET:
        case FAKE_OP_RESET_AT:
            held_bridge_reset(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_STOP:
            moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_ABORT:
            held_bridge_reset(to, o->stream_id, o->error_code, p->now);
            moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_CLOSE:
            held_bridge_close(to, o->error_code, p->now);
            break;
        default:
            break;
        }
    }
    fake_endpoint_clear_ops(from);
}

static void pump(pair_t *p)
{
    for (int i = 0; i < 16; i++) {
        held_bridge_service(p->cb, p->now);
        deliver(p, &p->cep, p->sb, true);
        held_bridge_service(p->sb, p->now);
        deliver(p, &p->sep, p->cb, false);
    }
}

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == kind) { *out = ev; return true; }
        moq_event_cleanup(&ev);
    }
    return false;
}

/* `reliable` declares reset_stream_at on the server (publisher) endpoint. */
static bool pair_open(pair_t *p, moq_version_t v, bool reliable, uint32_t sv_fills)
{
    memset(p, 0, sizeof(*p));
    p->v = v;
    p->c_ctrl = p->s_ctrl = UINT64_MAX;
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = v;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    cc.max_fills = 4;
    sc.max_fills = sv_fills;
    if (moq_session_create(&cc, 0, &p->c) < 0) return false;
    if (moq_session_create(&sc, 0, &p->sv) < 0) return false;
    if (test_bind_if_required(p->c) < 0 || test_bind_if_required(p->sv) < 0) return false;
    held_endpoint_init(&p->cep, CLIENT_UNI, CLIENT_BIDI);
    held_endpoint_init(&p->sep, SERVER_UNI, SERVER_BIDI);
    if (reliable) fake_endpoint_enable_reset_at(&p->sep);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0) return false;
    if (held_bridge_create(&bcfg, p->sv, &p->sep.vtable, &p->sep, &p->sb) < 0) return false;
    moq_session_start(p->c, 0);
    if (moq_session_uses_uni_control(p->sv)) moq_session_start(p->sv, 0);
    pump(p);
    drain_events(p->c);
    drain_events(p->sv);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->sv) == MOQ_SESS_ESTABLISHED;
}

static void pair_close(pair_t *p)
{
    if (p->cb) held_bridge_destroy(p->cb);
    if (p->sb) held_bridge_destroy(p->sb);
    if (p->c) moq_session_destroy(p->c);
    if (p->sv) moq_session_destroy(p->sv);
    memset(p, 0, sizeof(*p));
}

static moq_bytes_t ns_parts[] = { { (const uint8_t *)"live", 4 } };

static bool subscribe(pair_t *p, const moq_fill_cfg_t *fill, moq_subscription_t *ch,
                      moq_subscription_t *sh)
{
    static const char names[] = "abcdefgh";
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    cfg.track_name = (moq_bytes_t){ (const uint8_t *)&names[p->tracks++ % 8], 1 };
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.fill = fill;
    if (moq_session_subscribe(p->c, &cfg, p->now, ch) != MOQ_OK) return false;
    pump(p);
    moq_event_t ev;
    if (!take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    *sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    if (moq_session_accept_subscribe(p->sv, *sh, &ac, p->now) != MOQ_OK) return false;
    pump(p);
    return true;
}

static moq_subgroup_handle_t open_sg(pair_t *p, moq_subscription_t sh, uint64_t group)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(p->sv, sh, &cfg, p->now, &sg), (int)MOQ_OK);
    return sg;
}

static void write_obj(pair_t *p, moq_subgroup_handle_t sg, uint64_t id)
{
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"payload", 7, &b);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p->sv, sg, id, b, p->now), (int)MOQ_OK);
    moq_rcbuf_decref(b);
}

static size_t count_kind(const fake_endpoint_t *ep, fake_op_kind_t k)
{
    return fake_endpoint_count_kind(ep, k);
}

/* Bytes of the first write on `sid` in the recorded ops: the stream header. */
static size_t first_write_len(const fake_endpoint_t *ep, uint64_t sid)
{
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == FAKE_OP_WRITE && ep->ops[i].stream_id == sid)
            return ep->ops[i].data_len;
    return 0;
}

static size_t total_written(const fake_endpoint_t *ep, uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == FAKE_OP_WRITE && ep->ops[i].stream_id == sid)
            n += ep->ops[i].data_len;
    return n;
}

static int index_of(const fake_endpoint_t *ep, fake_op_kind_t k, uint64_t sid)
{
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == k && ep->ops[i].stream_id == sid) return (int)i;
    return -1;
}

static int last_write_index(const fake_endpoint_t *ep, uint64_t sid)
{
    int last = -1;
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == FAKE_OP_WRITE && ep->ops[i].stream_id == sid) last = (int)i;
    return last;
}

/* Open a subgroup with one object, then reset it, all before the server
 * bridge services: the reset is in the same pass as its header. Leaves the
 * server's recorded ops undelivered for the caller to inspect. */
static uint64_t open_write_reset(pair_t *p, moq_subscription_t sh)
{
    uint64_t sid = p->sep.next_uni_id;
    moq_subgroup_handle_t sg = open_sg(p, sh, 6);
    write_obj(p, sg, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_subgroup(p->sv, sg, 0x9, p->now), (int)MOQ_OK);
    held_bridge_service(p->sb, p->now);
    return sid;
}

/* -- Op-level contract ------------------------------------------------------- */

static void test_capability_present(moq_version_t v)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, v, true, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    uint64_t sid = open_write_reset(&p, sh);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET_AT), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET), 0);
    int r = index_of(&p.sep, FAKE_OP_RESET_AT, sid);
    MOQ_TEST_CHECK(r >= 0);
    if (r >= 0) {
        MOQ_TEST_CHECK(first_write_len(&p.sep, sid) > 0);
        MOQ_TEST_CHECK_EQ_U64(p.sep.ops[r].reliable_size, first_write_len(&p.sep, sid));
        MOQ_TEST_CHECK_EQ_U64(p.sep.ops[r].error_code, 0x9);
        MOQ_TEST_CHECK(r > last_write_index(&p.sep, sid));
    }
    pump(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.sb));
    pair_close(&p);
}

static void test_capability_absent(moq_version_t v)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, v, false, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    uint64_t sid = open_write_reset(&p, sh);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET_AT), 0);
    MOQ_TEST_CHECK_EQ_U64(p.sep.reset_at_calls, 0);
    MOQ_TEST_CHECK(index_of(&p.sep, FAKE_OP_RESET, sid) > last_write_index(&p.sep, sid));
    pair_close(&p);
}

static void test_not_negotiated(moq_version_t v)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, v, true, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    p.sep.reset_at_unavailable = true;
    uint64_t sid = open_write_reset(&p, sh);
    MOQ_TEST_CHECK_EQ_U64(p.sep.reset_at_calls, 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET_AT), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET), 1);
    MOQ_TEST_CHECK(index_of(&p.sep, FAKE_OP_RESET, sid) >= 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.sb));
    pair_close(&p);
}

/* The header write blocks: the reset waits behind it, then keeps it. */
static void test_header_blocked_write(bool reliable)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, MOQ_VERSION_DRAFT_22, reliable, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    p.sep.block_write = true;
    uint64_t sid = open_write_reset(&p, sh);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_OPEN_UNI), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_WRITE), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET) + count_kind(&p.sep, FAKE_OP_RESET_AT), 0);
    MOQ_TEST_CHECK_EQ_U64(p.sep.reset_at_calls, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.sb));

    p.sep.block_write = false;
    held_bridge_service(p.sb, p.now);
    MOQ_TEST_CHECK(first_write_len(&p.sep, sid) > 0);
    int r = index_of(&p.sep, reliable ? FAKE_OP_RESET_AT : FAKE_OP_RESET, sid);
    MOQ_TEST_CHECK(r > last_write_index(&p.sep, sid));
    if (reliable && r >= 0)
        MOQ_TEST_CHECK_EQ_U64(p.sep.ops[r].reliable_size, first_write_len(&p.sep, sid));
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET) + count_kind(&p.sep, FAKE_OP_RESET_AT), 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));
    pair_close(&p);
}

/* The stream is not even open yet: the reset waits for the open and the
 * header, and the stream is reset once it exists. */
static void test_open_blocked(bool reliable)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, MOQ_VERSION_DRAFT_22, reliable, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    p.sep.block_open_uni = true;
    uint64_t sid = open_write_reset(&p, sh);
    MOQ_TEST_CHECK_EQ_SIZE(p.sep.count, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.sb));

    p.sep.block_open_uni = false;
    held_bridge_service(p.sb, p.now);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_OPEN_UNI), 1);
    MOQ_TEST_CHECK(first_write_len(&p.sep, sid) > 0);
    int r = index_of(&p.sep, reliable ? FAKE_OP_RESET_AT : FAKE_OP_RESET, sid);
    MOQ_TEST_CHECK(r >= 0 && r > last_write_index(&p.sep, sid));
    if (reliable && r >= 0)
        MOQ_TEST_CHECK_EQ_U64(p.sep.ops[r].reliable_size, first_write_len(&p.sep, sid));
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET) + count_kind(&p.sep, FAKE_OP_RESET_AT), 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));
    /* The reset retired the stream's mapping. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_find_ref(p.sb, sid)._v, 0);
    pair_close(&p);
}

static void test_reset_at_would_block(void)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, MOQ_VERSION_DRAFT_22, true, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    p.sep.block_reset_at = true;
    uint64_t sid = open_write_reset(&p, sh);
    MOQ_TEST_CHECK_EQ_U64(p.sep.reset_at_calls, 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET_AT) + count_kind(&p.sep, FAKE_OP_RESET), 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.sb));
    size_t hdr = first_write_len(&p.sep, sid);
    p.sep.block_reset_at = false;
    held_bridge_service(p.sb, p.now);
    MOQ_TEST_CHECK_EQ_U64(p.sep.reset_at_calls, 2);
    int r = index_of(&p.sep, FAKE_OP_RESET_AT, sid);
    MOQ_TEST_CHECK(r >= 0);
    if (r >= 0) MOQ_TEST_CHECK_EQ_U64(p.sep.ops[r].reliable_size, hdr);
    MOQ_TEST_CHECK_EQ_SIZE(count_kind(&p.sep, FAKE_OP_RESET), 0);
    pair_close(&p);
}

/* A RESET_DATA asking for more than the stream was handed is bounded by what
 * was handed; one whose detail predates reliable_size is a plain reset. */
static void test_size_bounded_and_detail_gated(void)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, MOQ_VERSION_DRAFT_22, true, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    for (int round = 0; round < 2; round++) {
        uint64_t sid = p.sep.next_uni_id;
        moq_subgroup_handle_t sg = open_sg(&p, sh, 6 + (uint64_t)round);
        write_obj(&p, sg, 0);
        held_bridge_service(p.sb, p.now);
        size_t handed = total_written(&p.sep, sid);
        (void)sg;
        int slot = -1;
        for (size_t k = 0; k < p.sv->sg_cap; k++)
            if (p.sv->subgroups[k].state == MOQ_SG_OPEN &&
                p.sv->subgroups[k].group_id == 6 + (uint64_t)round)
                slot = (int)k;
        MOQ_TEST_CHECK(slot >= 0);
        if (slot < 0) break;
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_RESET_DATA;
        a.detail_size = round == 0
            ? (uint32_t)sizeof(moq_reset_data_action_t)
            : (uint32_t)offsetof(moq_reset_data_action_t, reliable_size);
        a.borrow_epoch = p.sv->borrow_epoch;
        a.u.reset_data.stream_ref = p.sv->subgroups[slot].stream_ref;
        a.u.reset_data.error_code = 0x5;
        a.u.reset_data.reliable_size = handed + 1000;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(p.sv, &a), (int)MOQ_OK);
        p.sv->subgroups[slot].state = MOQ_SG_RESETTING;
        held_bridge_service(p.sb, p.now);
        if (round == 0) {
            int r = index_of(&p.sep, FAKE_OP_RESET_AT, sid);
            MOQ_TEST_CHECK(r >= 0);
            if (r >= 0) MOQ_TEST_CHECK_EQ_U64(p.sep.ops[r].reliable_size, handed);
        } else {
            MOQ_TEST_CHECK(index_of(&p.sep, FAKE_OP_RESET_AT, sid) < 0);
            MOQ_TEST_CHECK(index_of(&p.sep, FAKE_OP_RESET, sid) >= 0);
        }
        fake_endpoint_clear_ops(&p.sep);
    }
    pair_close(&p);
}

/* -- End to end over a lossy transport ----------------------------------- */

/* A subgroup opened and reset in one pass, then PUBLISH_DONE counting it:
 * the subscriber can account for the stream, and so surface the done at
 * once, only when the reset kept its header. */
static void test_done_counts_reset_stream(moq_version_t v, bool reliable)
{
    pair_t p;
    moq_subscription_t ch, sh;
    if (!pair_open(&p, v, reliable, 0) || !subscribe(&p, NULL, &ch, &sh)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    drain_events(p.c);
    p.lossy = true;
    const uint64_t next_uni = p.sep.next_uni_id;
    moq_subgroup_handle_t sg = open_sg(&p, sh, 6);
    write_obj(&p, sg, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_subgroup(p.sv, sg, 0x1, p.now), (int)MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x2;   /* TRACK_ENDED */
    dc.stream_count = 1;
    /* The bridge has not yet opened the queued header's stream. A refused
     * DONE must have no terminal effect; drive real open/header/reset work
     * before retrying, without advancing the receiver's recovery deadline. */
    moq_result_t rc = moq_session_done_subscribe(p.sv, sh, &dc, p.now);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    if (rc != MOQ_ERR_WOULD_BLOCK) { pair_close(&p); return; }
    moq_event_t ev;
    for (unsigned retry = 0; retry < 16; retry++) {
        pump(&p);
        bool premature = take(p.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
        MOQ_TEST_CHECK(!premature);
        if (premature) moq_event_cleanup(&ev);
        rc = moq_session_done_subscribe(p.sv, sh, &dc, p.now);
        if (rc != MOQ_ERR_WOULD_BLOCK) break;
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(p.sep.next_uni_id, next_uni + 1);
    pump(&p);
    bool done = take(p.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    if (done) {
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x2);
        moq_event_cleanup(&ev);
    }
    if (reliable) MOQ_TEST_CHECK(done);
    else MOQ_TEST_CHECK(!done);
    bool duplicate = take(p.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    MOQ_TEST_CHECK(!duplicate);
    if (duplicate) moq_event_cleanup(&ev);
    pair_close(&p);
}

/* A fill the publisher cannot hold fails on the wire with a header and a
 * reset: the subscriber learns which fill ended only from the header. */
static void test_failed_fill_reaches_subscriber(bool reliable)
{
    pair_t p;
    if (!pair_open(&p, MOQ_VERSION_DRAFT_22, reliable, 1)) {
        MOQ_TEST_CHECK(false); pair_close(&p); return;
    }
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 3;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch1, sh1, ch2, sh2;
    MOQ_TEST_CHECK(subscribe(&p, &fill, &ch1, &sh1));   /* holds the publisher's slot */
    drain_events(p.c); drain_events(p.sv);
    p.lossy = true;
    MOQ_TEST_CHECK(subscribe(&p, &fill, &ch2, &sh2));   /* failed on the wire */
    moq_event_t ev;
    bool ended = take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev);
    if (ended) {
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
        moq_event_cleanup(&ev);
    }
    if (reliable) MOQ_TEST_CHECK(ended);
    else MOQ_TEST_CHECK(!ended);
    pair_close(&p);
}

int main(void)
{
    static const moq_version_t all[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
        test_capability_present(all[i]);
        test_capability_absent(all[i]);
        test_not_negotiated(all[i]);
        test_done_counts_reset_stream(all[i], true);
        test_done_counts_reset_stream(all[i], false);
    }
    test_header_blocked_write(true);
    test_header_blocked_write(false);
    test_open_blocked(true);
    test_open_blocked(false);
    test_reset_at_would_block();
    test_size_bounded_and_detail_gated();
    test_failed_fill_reaches_subscriber(true);
    test_failed_fill_reaches_subscriber(false);

    if (failures) {
        fprintf(stderr, "test_bridge_reliable_reset: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_bridge_reliable_reset: all passed\n");
    return 0;
}
