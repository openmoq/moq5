#include "test_cases.h"
#include "../test_binding_support.h"

/* -- New peer uni streams placed by class on a uni-pair profile ----------- *
 *
 * White-box rows on a draft-18 or draft-22 server session whose endpoint
 * advertises MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT. A new peer uni stream is
 * placed by the class of its first chunk before it takes a stream mapping:
 * a complete data type that finds no mapping free beyond the control reserve
 * (one entry for each control stream not yet mapped) is held without one --
 * zero bytes and no FIN consumed -- and a complete padding type is refused
 * for the adapter to cancel. A control, incomplete or unknown type takes the
 * classifying path as without the capability. Every cancellation is
 * stream-local (draft-22 section 15.5, draft-18 section 13.5); the session
 * survives.
 *
 * Stream types are literal wire bytes, derived by hand from the integer
 * encoding table (draft-22 section 8.1, draft-18 section 1.4.1: the number
 * of leading 1 bits of the first byte gives the length) and the stream type
 * registry: SETUP 0x2F00 is "af 00"; PADDING 0x132B3E28 needs five bytes,
 * "f0 13 2b 3e 28"; 0x10 is a subgroup header type. */

static const uint8_t k_fm_setup[2]   = { 0xaf, 0x00 };

static const uint8_t k_fm_padding[5] = { 0xf0, 0x13, 0x2b, 0x3e, 0x28 };

static const uint8_t k_fm_data[4]    = { 0x10, 0x01, 0x00, 0x00 };

/* The padding type in the QUIC variable-length form. In this protocol's
 * encoding "93 2b" is the two-byte type 0x132B, which is no stream type. */
static const uint8_t k_fm_quic_padding[4] = { 0x93, 0x2b, 0x3e, 0x28 };


#define FM_AGE 5000000ULL


/* The integer encoding as the table states it, kept separate from the
 * product decoder so the literal bytes above are checked against the text. */
static size_t fm_spec_vi64(const uint8_t *p, size_t n, uint64_t *v)
{
    if (n == 0) return 0;
    size_t len = 1;
    while (len < 9 && (p[0] & (0x80u >> (len - 1)))) len++;
    if (n < len) return 0;
    uint64_t x = len == 9 ? 0 : (uint64_t)(p[0] & (0xFFu >> len));
    for (size_t i = 1; i < len; i++) x = (x << 8) | p[i];
    *v = x;
    return len;
}


typedef struct {
    bool     hold_bidi;
    uint32_t max_streams;
    uint32_t max_pending;
    uint32_t occupied;       /* peer data streams mapped at open */
    bool     local_mapped;   /* our control stream mapped once */
    bool     peer_mapped;    /* the peer's control stream identified */
} fm_cfg_t;


static int fm_open(hb_rig_t *r, moq_version_t ver, const fm_cfg_t *c)
{
    memset(r, 0, sizeof(*r));
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = ver;
    if (moq_session_create(&cfg, 0, &r->s) < 0) return -1;
    if (test_bind_if_required(r->s) < 0) { moq_session_destroy(r->s); return -1; }
    held_endpoint_init(&r->ep, 100, 200);
    if (c->hold_bidi) r->ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    bcfg.max_streams = c->max_streams;
    bcfg.max_pending = c->max_pending;
    if (held_bridge_create(&bcfg, r->s, &r->ep.vtable, &r->ep, &r->b) < 0) {
        moq_session_destroy(r->s);
        return -1;
    }
    if (c->peer_mapped) {
        r->b->peer_ctrl_uni_open = true;
        r->b->peer_ctrl_uni_stream_id = 3;
    }
    if (c->local_mapped) {
        r->b->local_ctrl_uni_open = true;
        r->b->local_ctrl_uni_stream_id = 11;
    }
    for (uint32_t i = 0; i < c->occupied && i < r->b->max_streams; i++) {
        bridge_stream_entry_t *e = bridge_alloc_stream(r->b);
        if (e != &r->b->streams[i]) goto fail;
        e->transport_id = 900 + i;
        e->kind = BRIDGE_STREAM_UNI;
        e->origin = BRIDGE_ORIGIN_PEER;
        e->uni_disp = BRIDGE_UNI_DISP_DATA;
        e->ref = moq_stream_ref_from_u64(BRIDGE_INBOUND_REF_BASE + 0x900 + i);
    }
    const uint32_t expected = c->occupied < r->b->max_streams ? c->occupied : r->b->max_streams;
    if (r->b->streams_active != expected ||
        moq_transport_bridge_stream_count(r->b) != expected) goto fail;
    return 0;
fail:
    held_bridge_destroy(r->b);
    moq_session_destroy(r->s);
    return -1;
}


/* Two mappings, both taken by peer data streams, both control streams
 * mapped: no mapping is free and none is reserved. */
static const fm_cfg_t k_fm_full = { true, 2, 2, 2, true, true };


static moq_result_t fm_uni(hb_rig_t *r, uint64_t id, const uint8_t *d,
                           size_t n, bool fin, uint64_t now)
{
    return moq_transport_bridge_on_peer_uni_bytes(r->b, id, d, n, fin, now);
}


/* The same chunk through the borrowed-rcbuf entry; *refs is the buffer's
 * reference count after the call, before the caller's own release. */
static moq_result_t fm_uni_rcbuf(hb_rig_t *r, uint64_t id, const uint8_t *d,
                                 size_t n, uint64_t now, uint32_t *refs)
{
    moq_rcbuf_t *rb = NULL;
    *refs = 0;
    if (moq_rcbuf_create(moq_alloc_default(), d, n, &rb) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_rcbuf(r->b, id, rb,
                                                            false, now);
    *refs = moq_rcbuf_refcount(rb);
    moq_rcbuf_decref(rb);
    return rc;
}


static uint32_t fm_holds(const hb_rig_t *r)
{
    uint32_t n = 0;
    for (uint32_t i = 0; r->b->holds && i < r->b->max_holds; i++)
        if (r->b->holds[i].state != BRIDGE_HOLD_FREE) n++;
    return n;
}


static bool fm_occupy(hb_rig_t *r, uint32_t slot, uint64_t id)
{
    if (slot >= r->b->max_streams || r->b->streams[slot].active) return false;
    uint32_t before = r->b->streams_active;
    if (moq_transport_bridge_stream_count(r->b) != before) return false;
    bridge_stream_entry_t *e = bridge_alloc_stream(r->b);
    if (e != &r->b->streams[slot]) {
        if (e) bridge_deactivate_stream(r->b, e);
        return false;
    }
    e->transport_id = id;
    e->kind = BRIDGE_STREAM_UNI;
    e->origin = BRIDGE_ORIGIN_PEER;
    e->uni_disp = BRIDGE_UNI_DISP_DATA;
    e->ref = moq_stream_ref_from_u64(BRIDGE_INBOUND_REF_BASE + id);
    return r->b->streams_active == before + 1 &&
           moq_transport_bridge_stream_count(r->b) == before + 1;
}


/* The literal stream types classify as the stream type registry says, on
 * both profiles, and decode by the encoding table to the registered values.
 * A split SETUP or PADDING type is incomplete, which a one-byte data type
 * never is. */
int test_fm_wire_classes(moq_version_t ver)
{
    int failures = 0;
    const fm_cfg_t c = { false, 2, 2, 0, false, false };
    hb_rig_t r;
    if (fm_open(&r, ver, &c) < 0) { MOQ_TEST_CHECK(false); return failures; }
    uint64_t v = 0;
    MOQ_TEST_CHECK(fm_spec_vi64(k_fm_setup, 2, &v) == 2 && v == 0x2F00u);
    MOQ_TEST_CHECK(fm_spec_vi64(k_fm_padding, 5, &v) == 5 && v == 0x132B3E28u);
    MOQ_TEST_CHECK(fm_spec_vi64(k_fm_data, 1, &v) == 1 && v == 0x10u);
    MOQ_TEST_CHECK(fm_spec_vi64(k_fm_quic_padding, 4, &v) == 2 && v == 0x132Bu);
    MOQ_TEST_CHECK(MOQ_D18_STREAM_SETUP == 0x2F00u);
    MOQ_TEST_CHECK(MOQ_D18_STREAM_PADDING == 0x132B3E28u);

    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_setup, 2) == MOQ_UNI_CLASS_CONTROL);
    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_setup, 1) == MOQ_UNI_CLASS_NEED_MORE);
    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_padding, 5) == MOQ_UNI_CLASS_PADDING);
    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_padding, 4) == MOQ_UNI_CLASS_NEED_MORE);
    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_padding, 1) == MOQ_UNI_CLASS_NEED_MORE);
    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_data, 1) == MOQ_UNI_CLASS_DATA);
    MOQ_TEST_CHECK(moq_session_classify_peer_uni(r.s, k_fm_quic_padding, 4) == MOQ_UNI_CLASS_UNKNOWN);
    hb_close(&r);
    return failures;
}


/* A complete data type at a full map is held without a mapping: zero bytes
 * and no FIN taken (a borrowed rcbuf keeps only its owner's reference),
 * nothing mapped or fed, no endpoint call, and the session is not failed.
 * A FIN-only delivery and an exact redelivery cannot discharge it, and its
 * age runs from the first refusal. With every record in use, the next one
 * is refused for the adapter to cancel and leaves no record. */
int test_fm_data_held(moq_version_t ver, bool use_rcbuf)
{
    int failures = 0;
    hb_rig_t r;
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    uint32_t refs = 0;
    moq_result_t rc = use_rcbuf
        ? fm_uni_rcbuf(&r, 501, k_fm_data, sizeof(k_fm_data), 1000, &refs)
        : fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INPUT_NOT_CONSUMED);
    if (use_rcbuf) MOQ_TEST_CHECK(refs == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(r.ep.count == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(fm_holds(&r) == 1);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + FM_AGE);

    /* The FIN cannot overtake the held bytes; a redelivery while the map is
     * still full is refused again, its age unchanged. */
    MOQ_TEST_CHECK(fm_uni(&r, 501, NULL, 0, true, 1100) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1200) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_holds(&r) == 1);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v == 0);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + FM_AGE);

    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_data, sizeof(k_fm_data), false, 1300) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    rc = use_rcbuf
        ? fm_uni_rcbuf(&r, 509, k_fm_data, sizeof(k_fm_data), 1400, &refs)
        : fm_uni(&r, 509, k_fm_data, sizeof(k_fm_data), false, 1400);
    MOQ_TEST_CHECK(rc == MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    if (use_rcbuf) MOQ_TEST_CHECK(refs == 1);
    MOQ_TEST_CHECK(fm_holds(&r) == 2);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 509));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(r.ep.count == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}


/* Held data streams are offered oldest first when a mapping frees. Their
 * first refusals are a second apart, so the hold age expires exactly the
 * older one: one STOP_SENDING with EXCESSIVE_LOAD and no RESET (a uni stream
 * has no sending half here), after which the next is offered in the same
 * pass. Records that come due together are all cancelled in one pass. */
int test_fm_offer_and_age(moq_version_t ver)
{
    int failures = 0;
    const uint64_t code = MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
    hb_rig_t r;
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_data, sizeof(k_fm_data), false, 1001000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 509, k_fm_data, sizeof(k_fm_data), false, 1002000) ==
                   MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(r.ep.count == 0);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + FM_AGE);

    /* No mapping free: nothing is offered. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1003000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 505));

    /* A mapping frees: only the oldest is offered, and an offer maps
     * nothing until the redelivery. */
    bridge_deactivate_stream(r.b, &r.b->streams[0]);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams - 1);
    MOQ_TEST_CHECK(r.b->streams_active == moq_transport_bridge_stream_count(r.b));
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1004000) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 505));
    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_data, sizeof(k_fm_data), false, 1004000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);

    /* 501 is not redelivered before its age: cancelled, and 505 offered. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000 + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 501, code) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 505, code) == 0);
    MOQ_TEST_CHECK(fake_endpoint_count_kind(&r.ep, FAKE_OP_RESET) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 505));
    MOQ_TEST_CHECK(fm_holds(&r) == 1);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1001000 + FM_AGE);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));

    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1001000 + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 505, code) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 501, code) == 1);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);

    /* Two records due at the same instant: both handled in one pass. */
    if (!fm_occupy(&r, 0, 950)) {
        MOQ_TEST_CHECK(false); hb_close(&r); return failures;
    }
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == r.b->max_streams);
    const uint64_t t = 20000000;
    MOQ_TEST_CHECK(fm_uni(&r, 513, k_fm_data, sizeof(k_fm_data), false, t) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 517, k_fm_data, sizeof(k_fm_data), false, t) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, t + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 513, code) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 517, code) == 1);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 513));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 517));
    MOQ_TEST_CHECK(fake_endpoint_count_kind(&r.ep, FAKE_OP_RESET) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}


/* A cancellation the endpoint cannot take yet stays owed in its record: the
 * stream reports pending and the bridge reports outbound work, and a
 * redelivery is never fed. The peer's RESET then settles the receive half
 * between service calls: no STOP_SENDING is sent afterwards, and the other
 * held stream keeps its record, its age and its place. */
int test_fm_blocked_stop_then_reset(moq_version_t ver)
{
    int failures = 0;
    const uint64_t code = MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
    hb_rig_t r;
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_data, sizeof(k_fm_data), false, 2000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);

    r.ep.block_stop = true;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000 + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 501, code) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(r.b));
    MOQ_TEST_CHECK(fm_holds(&r) == 2);
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1001 + FM_AGE) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(r.b, 501, 0x2,
                                                            1002 + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(r.b));
    MOQ_TEST_CHECK(fm_holds(&r) == 1);

    r.ep.block_stop = false;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1003 + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 501, code) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 505));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 505)._v == 0);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 2000 + FM_AGE);

    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 2000 + FM_AGE) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 505, code) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 501, code) == 0);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}


/* A complete padding type that finds no mapping free beyond the reserve is
 * refused for the adapter to cancel even with hold records free: it is never
 * held, and a borrowed rcbuf keeps only its owner's reference. With a mapping
 * free beyond the reserve it is mapped and discarded as before. A mapping
 * the reserve keeps for the peer's control stream stays free for it. */
int test_fm_padding(moq_version_t ver, bool use_rcbuf)
{
    int failures = 0;
    hb_rig_t r;
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    uint32_t refs = 0;
    moq_result_t rc = use_rcbuf
        ? fm_uni_rcbuf(&r, 501, k_fm_padding, sizeof(k_fm_padding), 1000, &refs)
        : fm_uni(&r, 501, k_fm_padding, sizeof(k_fm_padding), false, 1000);
    MOQ_TEST_CHECK(rc == MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    if (use_rcbuf) MOQ_TEST_CHECK(refs == 1);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(r.ep.count == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));

    bridge_deactivate_stream(r.b, &r.b->streams[0]);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams - 1);
    MOQ_TEST_CHECK(r.b->streams_active == moq_transport_bridge_stream_count(r.b));
    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_padding, sizeof(k_fm_padding), false, 1100) ==
                   MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 505)._v != 0);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    hb_close(&r);

    const fm_cfg_t owed = { true, 2, 2, 1, true, false };
    if (fm_open(&r, ver, &owed) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_padding, sizeof(k_fm_padding), false, 1000) ==
                   MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    rc = fm_uni(&r, 3, k_fm_setup, sizeof(k_fm_setup), false, 1100);
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(r.b->peer_ctrl_uni_open && r.b->peer_ctrl_uni_stream_id == 3);
    hb_close(&r);
    return failures;
}


/* Both control streams are owed on a two-entry map: a complete data type is
 * held and a padding type refused although both mappings are free, and a
 * request stream is held the same way. Each control stream then takes its
 * mapping, in either order, and nothing fails. */
int test_fm_reserve_both_controls(moq_version_t ver, bool local_first)
{
    int failures = 0;
    const fm_cfg_t c = { true, 2, 2, 0, false, false };
    hb_rig_t r;
    if (fm_open(&r, ver, &c) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(moq_session_start(r.s, 0) == MOQ_OK);
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_padding, sizeof(k_fm_padding), false, 1000) ==
                   MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 0);

    moq_result_t rc;
    if (local_first) {
        MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1100) == MOQ_OK);
        MOQ_TEST_CHECK(r.b->local_ctrl_uni_open);
        MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);
        rc = fm_uni(&r, 3, k_fm_setup, sizeof(k_fm_setup), false, 1200);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    } else {
        rc = fm_uni(&r, 3, k_fm_setup, sizeof(k_fm_setup), false, 1100);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);
        MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1200) == MOQ_OK);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(r.b->local_ctrl_uni_open);
    MOQ_TEST_CHECK(r.b->peer_ctrl_uni_open && r.b->peer_ctrl_uni_stream_id == 3);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2001));
    hb_close(&r);
    return failures;
}


/* Our control stream's open is blocked by the endpoint: its mapping is
 * released and stays reserved, so neither a data stream nor a request stream
 * takes the last free entry, and the retried open maps there. */
int test_fm_reserve_local_blocked(moq_version_t ver)
{
    int failures = 0;
    const fm_cfg_t c = { true, 2, 2, 1, false, true };
    hb_rig_t r;
    if (fm_open(&r, ver, &c) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(moq_session_start(r.s, 0) == MOQ_OK);
    r.ep.block_open_uni = true;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(!r.b->local_ctrl_uni_open);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);

    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1001) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1001) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);

    r.ep.block_open_uni = false;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1002) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(r.b->local_ctrl_uni_open);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2001));
    hb_close(&r);
    return failures;
}


/* Our control stream, once mapped, stays out of the reserve after its FIN:
 * the reserve is not raised again while the session drains. */
int test_fm_local_reserve_after_fin(moq_version_t ver)
{
    int failures = 0;
    const fm_cfg_t c = { true, 2, 2, 1, true, true };
    hb_rig_t r;
    if (fm_open(&r, ver, &c) < 0) { MOQ_TEST_CHECK(false); return failures; }
    r.b->local_ctrl_uni_open = false;               /* its FIN was written */
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_padding, sizeof(k_fm_padding), false, 1000) ==
                   MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v != 0);
    hb_close(&r);

    const fm_cfg_t never = { true, 2, 2, 1, false, true };
    if (fm_open(&r, ver, &never) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_padding, sizeof(k_fm_padding), false, 1000) ==
                   MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);
    hb_close(&r);
    return failures;
}


/* Held request and data streams share one queue, oldest first. A new data
 * stream waits behind them even with a mapping free, and a FIN-only delivery
 * cannot overtake a held stream's bytes. When the offered request stream is
 * reset by its peer and cancelled, the older held data stream is offered. */
int test_fm_cross_kind_fifo(moq_version_t ver)
{
    int failures = 0;
    const fm_cfg_t c = { true, 2, 3, 2, true, true };
    hb_rig_t r;
    if (fm_open(&r, ver, &c) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 2000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    bridge_deactivate_stream(r.b, &r.b->streams[0]);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams - 1);
    MOQ_TEST_CHECK(r.b->streams_active == moq_transport_bridge_stream_count(r.b));
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));

    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_data, sizeof(k_fm_data), false, 3000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 505)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 1);
    MOQ_TEST_CHECK(fm_holds(&r) == 3);
    MOQ_TEST_CHECK(fm_uni(&r, 501, NULL, 0, true, 3000) == MOQ_ERR_INPUT_NOT_CONSUMED);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(r.b, 2001, 0x2, 3001) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 3002) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_RESET, 2001, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 505));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}


/* Buffered control input defers delivery but not bounded hold ownership.
 * At a full map, the first refusal records the data stream without consuming
 * bytes. Draining control and redelivering preserve that owner and its age. */
int test_fm_behind_pending_control(moq_version_t ver)
{
    int failures = 0;
    hb_rig_t r;
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    r.b->pending_control = true;
    r.b->pending_control_stream_id = 3;
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_holds(&r) == 1);
    bridge_hold_t *held = NULL;
    for (uint32_t i = 0; r.b->holds && i < r.b->max_holds; i++) {
        if (r.b->holds[i].state != BRIDGE_HOLD_FREE &&
            r.b->holds[i].transport_id == 501) {
            held = &r.b->holds[i];
            break;
        }
    }
    MOQ_TEST_CHECK(held != NULL);
    if (!held) { hb_close(&r); return failures; }
    const uint64_t seq = held->seq;
    MOQ_TEST_CHECK(held->state == BRIDGE_HOLD_RECORDED);
    MOQ_TEST_CHECK(!held->mapped && !held->offered && held->held_payload);
    MOQ_TEST_CHECK(held->first_refusal_us == 1000);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(r.b->streams_active == 2);
    MOQ_TEST_CHECK(r.ep.count == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    r.b->pending_control = false;                      /* the control input drained */
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1001) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(fm_holds(&r) == 1);
    MOQ_TEST_CHECK(held->state == BRIDGE_HOLD_RECORDED);
    MOQ_TEST_CHECK(held->transport_id == 501 && held->seq == seq);
    MOQ_TEST_CHECK(!held->mapped && !held->offered && held->held_payload);
    MOQ_TEST_CHECK(held->first_refusal_us == 1000);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + FM_AGE);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 501)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(r.b->streams_active == 2);
    MOQ_TEST_CHECK(r.ep.count == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 501));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}


/* An unknown type, and a second control stream, keep their existing fatal
 * outcomes: 0x1 at a full map, PROTOCOL_VIOLATION (0x3) with a mapping free.
 * Neither is held or refused for the adapter to cancel. */
int test_fm_unknown_unchanged(moq_version_t ver)
{
    int failures = 0;
    hb_rig_t r;
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_quic_padding, sizeof(k_fm_quic_padding), false,
                          1000) == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x1);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    hb_close(&r);

    const fm_cfg_t one_free = { true, 2, 2, 1, true, true };
    if (fm_open(&r, ver, &one_free) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_quic_padding, sizeof(k_fm_quic_padding), false,
                          1000) == MOQ_ERR_PROTO);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x3);
    hb_close(&r);

    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_setup, sizeof(k_fm_setup), false, 1000) ==
                   MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x1);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    hb_close(&r);
    return failures;
}


/* Temporary known limitation, characterized and not endorsed: a new stream
 * whose first chunk is an incomplete type might be the peer's control
 * stream, so it is neither held nor cancelled, and it has no lossless
 * destination other than a stream mapping yet. It therefore takes any free
 * mapping, an entry the control reserve keeps included, and at a full map it
 * fails the session. These rows keep the outcomes discriminating -- an
 * incomplete type against a complete data type -- until incomplete types
 * get a lossless waiting place. */
int test_fm_incomplete_type_known_limitation(moq_version_t ver)
{
    int failures = 0;
    const fm_cfg_t owed = { true, 2, 2, 1, true, false };
    hb_rig_t r;
    moq_result_t rc;

    /* A split padding type takes the entry reserved for the peer's control
     * stream, which then meets a full map. */
    if (fm_open(&r, ver, &owed) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_padding, 1, false, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(fm_holds(&r) == 0);
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_padding + 1, sizeof(k_fm_padding) - 1, false,
                          1001) == MOQ_OK);
    MOQ_TEST_CHECK(fm_uni(&r, 3, k_fm_setup, sizeof(k_fm_setup), false, 1002) ==
                   MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x1);
    hb_close(&r);

    /* A split control type on that entry is identified and survives. */
    if (fm_open(&r, ver, &owed) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 3, k_fm_setup, 1, false, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(!r.b->peer_ctrl_uni_open);
    rc = fm_uni(&r, 3, k_fm_setup + 1, 1, false, 1001);
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(r.b->peer_ctrl_uni_open && r.b->peer_ctrl_uni_stream_id == 3);
    hb_close(&r);

    /* At a full map a complete data type is held, an incomplete type fails
     * the session. */
    if (fm_open(&r, ver, &k_fm_full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(fm_uni(&r, 505, k_fm_padding, 1, false, 1001) == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x1);
    hb_close(&r);
    return failures;
}


/* Without the capability nothing changes: no hold records exist, a full map
 * is fatal for data and padding alike and never answered with the refusal
 * outcome, and a padding stream may take an entry the reserve would keep. */
int test_fm_capability_off(moq_version_t ver)
{
    int failures = 0;
    const fm_cfg_t full = { false, 2, 2, 2, true, true };
    hb_rig_t r;
    if (fm_open(&r, ver, &full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(r.b->holds == NULL);
    moq_result_t rc = fm_uni(&r, 501, k_fm_data, sizeof(k_fm_data), false, 1000);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(rc != MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x1);
    hb_close(&r);

    if (fm_open(&r, ver, &full) < 0) { MOQ_TEST_CHECK(false); return failures; }
    rc = fm_uni(&r, 501, k_fm_padding, sizeof(k_fm_padding), false, 1000);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(r.b) == 0x1);
    hb_close(&r);

    const fm_cfg_t owed = { false, 2, 2, 1, true, false };
    if (fm_open(&r, ver, &owed) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(fm_uni(&r, 501, k_fm_padding, sizeof(k_fm_padding), false, 1000) ==
                   MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == 2);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}
