#include "test_cases.h"
#include "../test_binding_support.h"

/* Exercise the transport recorder itself under actual admission pressure.
 * The raw one-byte FETCH header occupies the only receive slot. The real
 * publisher's second response must survive copied chunks plus separate FIN. */
int test_held_driver_replay(bool rcbuf)
{
    int failures = 0;
    test_pair_t tp;
    g_client_rx_capacity = 1;
    HELD_REQUIRE(test_pair_init(&tp) == 0);
    g_client_rx_capacity = 0;
    HELD_REQUIRE(setup_handshake(&tp));
    uint8_t partial = 0x05;
    HELD_REQUIRE(held_bridge_uni_bytes(tp.client_bridge, 6000, &partial, 1, false, 0) == MOQ_OK);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("held") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("t");
    fc.end_object = 1;
    moq_fetch_t fh;
    HELD_REQUIRE(moq_session_fetch(tp.client, &fc, 0, &fh) == MOQ_OK);
    pump_until_quiescent(&tp, 8, 0);
    moq_event_t ev;
    moq_fetch_t peer = {0};
    bool found = false;
    while (moq_session_poll_events(tp.server, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { peer = ev.u.fetch_request.fetch; found = true; }
        moq_event_cleanup(&ev);
    }
    HELD_REQUIRE(found);
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_object = 1;
    HELD_REQUIRE(moq_session_accept_fetch(tp.server, peer, &ac, 0) == MOQ_OK);
    const uint8_t payload[] = { 0x12, 0x34, 0x56, 0x78 };
    moq_rcbuf_t *rb = NULL;
    HELD_REQUIRE(moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &rb) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.payload = rb;
    HELD_REQUIRE(moq_session_write_fetch_object(tp.server, peer, &oc, 0) == MOQ_OK);
    moq_rcbuf_decref(rb);
    HELD_REQUIRE(moq_session_end_fetch(tp.server, peer, 0) == MOQ_OK);
    HELD_REQUIRE(held_bridge_service(tp.server_bridge, 0) == MOQ_OK);
    test_held_input_t *h = &held_bridge_driver(tp.client_bridge)->input;
    uint64_t response_sid = 0;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        if (o->stream_id < 3000) {
            HELD_REQUIRE(moq_transport_bridge_on_peer_control_bytes(tp.client_bridge,
                o->stream_id, o->data, o->data_len, o->fin, 0) == MOQ_OK);
            continue;
        }
        response_sid = o->stream_id;
        if (o->data_len) {
            size_t before = h->count;
            if (rcbuf) {
                HELD_REQUIRE(moq_rcbuf_create(moq_alloc_default(), o->data, o->data_len, &rb) == MOQ_OK);
                HELD_REQUIRE(held_bridge_uni_rcbuf(tp.client_bridge, response_sid, rb, false, 0) == MOQ_OK);
                moq_rcbuf_decref(rb);
            } else {
                HELD_REQUIRE(held_bridge_uni_bytes(tp.client_bridge, response_sid,
                    o->data, o->data_len, false, 0) == MOQ_OK);
            }
            MOQ_TEST_CHECK(h->count == before + 1);
            MOQ_TEST_CHECK(h->chunks[before].len == o->data_len && !h->chunks[before].fin);
            MOQ_TEST_CHECK(memcmp(h->chunks[before].bytes, o->data, o->data_len) == 0);
            memset(o->data, 0xee, o->data_len); /* Input storage is no longer borrowed. */
        }
        if (o->fin) HELD_REQUIRE(held_bridge_uni_bytes(tp.client_bridge, response_sid, NULL, 0, true, 0) == MOQ_OK);
    }
    HELD_REQUIRE(h->count >= 2);
    MOQ_TEST_CHECK(h->chunks[h->count - 1].len == 0 && h->chunks[h->count - 1].fin);
    size_t held = h->count;
    HELD_REQUIRE(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(h->count == held);
    HELD_REQUIRE(held_bridge_reset(tp.client_bridge, 6000, 1, 0) == MOQ_OK);
    HELD_REQUIRE(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(h->count == 0);
    int objects = 0, completed = 0;
    while (moq_session_poll_events(tp.client, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
            objects++;
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fetch_object.payload) == sizeof(payload));
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.fetch_object.payload), payload, sizeof(payload)) == 0);
        } else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) completed++;
        else MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FETCH_OK);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(objects == 1 && completed == 1);
    MOQ_TEST_CHECK(moq_session_fetch_cancel(tp.client, fh, 0) == MOQ_ERR_STALE_HANDLE);
    HELD_REQUIRE(held_bridge_uni_bytes(tp.client_bridge, 6001, &partial, 1, false, 0) == MOQ_OK);
    HELD_REQUIRE(held_bridge_uni_bytes(tp.client_bridge, 6002, &partial, 1, false, 0) == MOQ_OK);
    MOQ_TEST_CHECK(h->count == 1);
    HELD_REQUIRE(held_bridge_reset(tp.client_bridge, 6002, 1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(h->count == 0);
    HELD_REQUIRE(held_bridge_uni_bytes(tp.client_bridge, 6003, &partial, 1, false, 0) == MOQ_OK);
    MOQ_TEST_CHECK(h->count == 1);
    if (rcbuf) {
        HELD_REQUIRE(held_bridge_close(tp.client_bridge, 0, 0) == MOQ_OK);
        MOQ_TEST_CHECK(h->count == 0);
    }
    test_pair_destroy(&tp);
    MOQ_TEST_CHECK(h->count == 0);
    return failures;
}


static int hb_open_ver(hb_rig_t *r, moq_version_t ver, bool hold_bidi,
                       bool native_abort)
{
    memset(r, 0, sizeof(*r));
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = ver;
    if (moq_session_create(&cfg, 0, &r->s) < 0) return -1;
    if (test_bind_if_required(r->s) < 0) { moq_session_destroy(r->s); return -1; }
    held_endpoint_init(&r->ep, 100, 200);
    if (hold_bidi) r->ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    if (native_abort) fake_endpoint_enable_abort(&r->ep);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    bcfg.max_streams = 2;
    bcfg.max_pending = 2;
    if (held_bridge_create(&bcfg, r->s, &r->ep.vtable, &r->ep, &r->b) < 0) {
        moq_session_destroy(r->s);
        return -1;
    }
    r->b->peer_ctrl_uni_open = true;
    r->b->peer_ctrl_uni_stream_id = 3;
    r->b->local_ctrl_uni_open = true;
    r->b->local_ctrl_uni_stream_id = 11;
    for (uint32_t i = 0; i < r->b->max_streams; i++) {
        bridge_stream_entry_t *e = bridge_alloc_stream(r->b);
        if (e != &r->b->streams[i]) goto fail;
        e->transport_id = 900 + i;
        e->kind = BRIDGE_STREAM_UNI;
        e->origin = BRIDGE_ORIGIN_PEER;
        e->ref = moq_stream_ref_from_u64(BRIDGE_INBOUND_REF_BASE + 0x900 + i);
    }
    const uint32_t expected = r->b->max_streams;
    if (r->b->streams_active != expected ||
        moq_transport_bridge_stream_count(r->b) != expected) goto fail;
    return 0;
fail:
    held_bridge_destroy(r->b);
    moq_session_destroy(r->s);
    return -1;
}


static int hb_open(hb_rig_t *r, bool hold_bidi, bool native_abort)
{
    return hb_open_ver(r, MOQ_VERSION_DRAFT_22, hold_bidi, native_abort);
}


void hb_close(hb_rig_t *r)
{
    held_bridge_destroy(r->b);     /* the bridge goes before its session */
    moq_session_destroy(r->s);
}


moq_result_t hb_bidi(hb_rig_t *r, uint64_t id, uint64_t now)
{
    static const uint8_t req[3] = { 0x01, 0x02, 0x03 };
    return moq_transport_bridge_on_peer_bidi_bytes(r->b, id, req, sizeof(req),
                                                   false, now);
}


size_t hb_ops(const fake_endpoint_t *ep, fake_op_kind_t kind,
              uint64_t id, uint64_t code)
{
    size_t n = 0;
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == kind && ep->ops[i].stream_id == id &&
            ep->ops[i].error_code == code)
            n++;
    return n;
}


int test_hold_bidi_records_fifo_and_expiry(void)
{
    int failures = 0;
    const uint64_t age = 5000000;
    hb_rig_t r;
    if (hb_open(&r, true, false) < 0) { MOQ_TEST_CHECK(false); return failures; }

    /* Held, not mapped; the hold age is the session's next deadline. */
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 2001)._v == 0);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + age);
    MOQ_TEST_CHECK(hb_bidi(&r, 2002, 2000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    /* Every record in use: refused for the adapter to cancel, nothing
     * recorded and no endpoint call. */
    MOQ_TEST_CHECK(hb_bidi(&r, 2003, 2500) == MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2003));
    MOQ_TEST_CHECK(r.ep.count == 0);

    /* A direct session tick at the hold age leaves it in place. */
    (void)moq_session_tick(r.s, 1000 + age);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + age);

    /* A mapping frees: only the oldest held stream is offered; the next one
     * stays held even when it redelivers. */
    bridge_deactivate_stream(r.b, &r.b->streams[0]);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams - 1);
    MOQ_TEST_CHECK(r.b->streams_active == moq_transport_bridge_stream_count(r.b));
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2002));
    MOQ_TEST_CHECK(hb_bidi(&r, 2002, 3000) == MOQ_ERR_INPUT_NOT_CONSUMED);

    /* 2001 is not redelivered before its hold age: it is cancelled with
     * EXCESSIVE_LOAD on both halves, its record frees, and 2002 is offered. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000 + age) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_RESET, 2001, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 2001, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2002));
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 2000 + age);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));

    /* A peer RESET of a held request stream settles only its receive half:
     * our own half is still reset, and no STOP_SENDING follows. */
    MOQ_TEST_CHECK(hb_bidi(&r, 2004, 6000000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(r.b, 2004, 0x2,
                                                            6000000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2004));
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 6000001) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_RESET, 2004, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 2004, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2004));

    hb_close(&r);
    return failures;
}


/* The native whole-stream abort carries both halves in one operation; a
 * blocked cancellation stays owed (reported as outbound work) and is retried
 * in its own record. */
int test_hold_bidi_abort_and_blocked_cancel(void)
{
    int failures = 0;
    const uint64_t age = 5000000;
    hb_rig_t r;
    if (hb_open(&r, true, true) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    r.ep.block_abort = true;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000 + age) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_ABORT, 2001, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(r.b));
    /* Owed cancellation has no time deadline of its own. */
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) > 1000 + age);
    /* Redelivery while cancelling is never fed. */
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1000 + age) == MOQ_ERR_INPUT_NOT_CONSUMED);
    r.ep.block_abort = false;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1001 + age) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_ABORT, 2001, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_RESET, 2001, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(r.b));
    hb_close(&r);
    return failures;
}


/* An endpoint that fails to take a cancellation is a fatal transport error,
 * after which nothing is reported due. */
int test_hold_bidi_cancel_failure_fatal(void)
{
    int failures = 0;
    const uint64_t age = 5000000;
    hb_rig_t r;
    if (hb_open(&r, true, false) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    r.ep.fail_stop = true;
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000 + age) ==
                   MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(!r.s->has_owner_deadline);
    hb_close(&r);
    return failures;
}


/* Without the capability a full stream map stays fatal for a new request
 * stream, as before: the refusal outcome never reaches such an adapter. */
int test_hold_bidi_requires_capability(void)
{
    int failures = 0;
    hb_rig_t r;
    if (hb_open(&r, false, false) < 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(r.b->holds == NULL);
    moq_result_t rc = hb_bidi(&r, 2001, 1000);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(rc != MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}


/* Draft-16 with the capability: every peer uni stream is data, so a new one
 * that finds no free mapping is held (zero consumption), offered oldest first
 * when a mapping frees, refused for the adapter to cancel when every record
 * is in use, and cancelled at the hold age with STOP_SENDING carrying
 * CANCELLED (0x1) -- draft-16 has no EXCESSIVE_LOAD stream code, and a uni
 * stream has no send half to reset. Request bidis also use bounded holds;
 * the draft-16 control stream uses the separate control-bytes entry. */
int test_hold_uni_d16(void)
{
    int failures = 0;
    const uint64_t age = 5000000;
    static const uint8_t data[3] = { 0x04, 0x01, 0x00 };
    hb_rig_t r;
    if (hb_open_ver(&r, MOQ_VERSION_DRAFT_16, true, false) < 0) {
        MOQ_TEST_CHECK(false);
        return failures;
    }
    MOQ_TEST_CHECK(r.b->holds != NULL);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 101, data,
                       sizeof(data), false, 1000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 102, data,
                       sizeof(data), false, 2000) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 103, data,
                       sizeof(data), false, 2500) == MOQ_BRIDGE_INGRESS_REFUSE_CANCEL);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 101)._v == 0);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 1000 + age);
    MOQ_TEST_CHECK(r.ep.count == 0);

    bridge_deactivate_stream(r.b, &r.b->streams[0]);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams - 1);
    MOQ_TEST_CHECK(r.b->streams_active == moq_transport_bridge_stream_count(r.b));
    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 101));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 102));

    MOQ_TEST_CHECK(moq_transport_bridge_service(r.b, 1000 + age) == MOQ_OK);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 101, MOQ_STREAM_ERROR_CANCELLED) == 1);
    MOQ_TEST_CHECK(hb_ops(&r.ep, FAKE_OP_STOP, 101, MOQ_STREAM_ERROR_EXCESSIVE_LOAD) == 0);
    MOQ_TEST_CHECK(fake_endpoint_count_kind(&r.ep, FAKE_OP_RESET) == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 101));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 102));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));

    /* Refill the map: a request bidi uses the same bounded holds. The
     * control stream enters through its separate control-bytes API. */
    bridge_stream_entry_t *refill = bridge_alloc_stream(r.b);
    MOQ_TEST_CHECK(refill == &r.b->streams[0]);
    if (refill != &r.b->streams[0]) { hb_close(&r); return failures; }
    refill->transport_id = 950;
    refill->kind = BRIDGE_STREAM_UNI;
    refill->origin = BRIDGE_ORIGIN_PEER;
    refill->ref = moq_stream_ref_from_u64(BRIDGE_INBOUND_REF_BASE + 950);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == r.b->max_streams);
    size_t ops_before = r.ep.count;
    MOQ_TEST_CHECK(hb_bidi(&r, 2001, 1001 + age) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 2001));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 2001)._v == 0);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(r.s) == 2000 + age);
    MOQ_TEST_CHECK(r.ep.count == ops_before);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == r.b->max_streams);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    hb_close(&r);
    return failures;
}
