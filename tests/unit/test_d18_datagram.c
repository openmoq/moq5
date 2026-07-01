/*
 * Draft-18 OBJECT_DATAGRAM (§11.3.1) + padding datagram (§11.5.2). The whole
 * datagram framework (public send APIs, MOQ_ACTION_SEND_DATAGRAM,
 * moq_session_on_datagram, the MOQ_EVENT_OBJECT_RECEIVED event with a `datagram`
 * flag, and SimPair routing) already exists and is draft-neutral; this slice adds
 * only the D18 wire codec + the two D18 profile ops. Covers: codec round-trips,
 * the decode reject matrix (invalid types, empty/over-cap properties, bad
 * status/properties combos, unknown status, mandatory track property), padding
 * discard, and SimPair end-to-end delivery (object + status, properties, unknown
 * alias drop, malformed close 0x3).
 */
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

static const moq_bytes_t k_live[1] = { { (const uint8_t *)"live", 4 } };
static const moq_namespace_t k_ns = { (moq_bytes_t *)k_live, 1 };

/* Build a one-entry draft-18 vi64 KVP property block (even type + value). */
static size_t build_props(uint8_t *buf, size_t cap, uint64_t type, uint64_t value)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_buf_write_vi64(&w, type);
    moq_buf_write_vi64(&w, value);
    return moq_buf_writer_offset(&w);
}

/* Encode `dg`, decode it back, and return the decode result; fills `out`. */
static moq_result_t roundtrip(const moq_d18_object_datagram_t *dg,
                              uint8_t *buf, size_t cap,
                              moq_d18_object_datagram_t *out)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_result_t rc = moq_d18_encode_object_datagram(&w, dg);
    if (rc < 0) return rc;
    return moq_d18_decode_object_datagram(buf, moq_buf_writer_offset(&w), out);
}

/* --- SimPair: establish a subscription, return both subscription handles ---- */

static moq_simpair_t *make_pair(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    return sp;
}

static bool setup_sub(moq_simpair_t *sp, moq_subscription_t *out_server)
{
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    if (moq_simpair_start(sp) < 0) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    if (client->state != MOQ_SESS_ESTABLISHED ||
        server->state != MOQ_SESS_ESTABLISHED)
        return false;
    moq_subscribe_cfg_t sub; moq_subscribe_cfg_init(&sub);
    sub.track_namespace = k_ns; sub.track_name = MOQ_BYTES_LITERAL("v");
    moq_subscription_t ch;
    if (moq_session_subscribe(client, &sub, 1, &ch) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_subscription_t sh = MOQ_SUBSCRIPTION_INVALID; bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) { sh = ev.u.subscribe_request.sub; got = true; }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    moq_accept_subscribe_cfg_t acc; moq_accept_subscribe_cfg_init(&acc);
    acc.has_track_alias = true; acc.track_alias = 7;
    if (moq_session_accept_subscribe(server, sh, &acc, moq_simpair_now_us(sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    while (moq_session_poll_events(client, &ev, 1) > 0) moq_event_cleanup(&ev);
    *out_server = sh;
    return true;
}

int main(void)
{
    uint8_t buf[256];

    /* == Codec round-trips ============================================= */
    {   /* payload object with all fields present */
        moq_d18_object_datagram_t dg; memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7; dg.group_id = 100; dg.object_id = 9;
        dg.publisher_priority = 200; dg.end_of_group = true;
        const uint8_t pay[5] = { 1,2,3,4,5 };
        dg.payload = pay; dg.payload_len = 5;
        moq_d18_object_datagram_t out;
        MOQ_TEST_CHECK_EQ_INT((int)roundtrip(&dg, buf, sizeof(buf), &out), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(out.track_alias, 7);
        MOQ_TEST_CHECK_EQ_U64(out.group_id, 100);
        MOQ_TEST_CHECK_EQ_U64(out.object_id, 9);
        MOQ_TEST_CHECK_EQ_INT(out.publisher_priority, 200);
        MOQ_TEST_CHECK(out.end_of_group && !out.is_status && !out.default_priority);
        MOQ_TEST_CHECK_EQ_SIZE(out.payload_len, 5);
        MOQ_TEST_CHECK(memcmp(out.payload, pay, 5) == 0);
    }
    {   /* zero object id + default priority */
        moq_d18_object_datagram_t dg; memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1; dg.group_id = 2; dg.object_id = 0;
        dg.default_priority = true;
        const uint8_t pay[1] = { 0xAB };
        dg.payload = pay; dg.payload_len = 1;
        moq_d18_object_datagram_t out;
        MOQ_TEST_CHECK_EQ_INT((int)roundtrip(&dg, buf, sizeof(buf), &out), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(out.object_id, 0);
        MOQ_TEST_CHECK(out.default_priority);
        MOQ_TEST_CHECK_EQ_INT(out.publisher_priority, 128);
    }
    {   /* payload object with properties */
        uint8_t props[16]; size_t pl = build_props(props, sizeof(props), 2, 42);
        moq_d18_object_datagram_t dg; memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7; dg.group_id = 3; dg.object_id = 1;
        dg.publisher_priority = 10;
        dg.has_properties = true; dg.properties = props; dg.properties_len = pl;
        const uint8_t pay[3] = { 9,9,9 };
        dg.payload = pay; dg.payload_len = 3;
        moq_d18_object_datagram_t out;
        MOQ_TEST_CHECK_EQ_INT((int)roundtrip(&dg, buf, sizeof(buf), &out), (int)MOQ_OK);
        MOQ_TEST_CHECK(out.has_properties);
        MOQ_TEST_CHECK_EQ_SIZE(out.properties_len, pl);
        MOQ_TEST_CHECK(memcmp(out.properties, props, pl) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(out.payload_len, 3);
    }
    {   /* status object (END_OF_GROUP / END_OF_TRACK) */
        moq_d18_object_datagram_t dg; memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7; dg.group_id = 5; dg.object_id = 0;
        dg.publisher_priority = 1; dg.is_status = true;
        dg.object_status = MOQ_OBJECT_STATUS_END_OF_TRACK;
        moq_d18_object_datagram_t out;
        MOQ_TEST_CHECK_EQ_INT((int)roundtrip(&dg, buf, sizeof(buf), &out), (int)MOQ_OK);
        MOQ_TEST_CHECK(out.is_status);
        MOQ_TEST_CHECK_EQ_U64(out.object_status, MOQ_OBJECT_STATUS_END_OF_TRACK);
        MOQ_TEST_CHECK(out.payload == NULL && out.payload_len == 0);
    }

    /* == Decode reject matrix ========================================= */
    {
        moq_d18_object_datagram_t out;
        /* invalid type: STATUS|END_OF_GROUP (0x22) */
        uint8_t b0[] = { 0x22 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b0, sizeof(b0), &out),
                              (int)MOQ_ERR_PROTO);
        /* invalid type: bit 4 set (0x10) — subgroup form, not a datagram */
        uint8_t b1[] = { 0x10 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b1, sizeof(b1), &out),
                              (int)MOQ_ERR_PROTO);
        /* invalid type: out of range (0x30) */
        uint8_t b2[] = { 0x30 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b2, sizeof(b2), &out),
                              (int)MOQ_ERR_PROTO);
        /* PROPERTIES bit set with property length 0 */
        uint8_t b3[] = { 0x01, 0x01, 0x02, 0x03, 0x80, 0x00 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b3, sizeof(b3), &out),
                              (int)MOQ_ERR_PROTO);
        /* STATUS + PROPERTIES with non-Normal status (0x21 ... status=0x03) */
        uint8_t b4[] = { 0x21, 0x01, 0x02, 0x03, 0x80, 0x02, 0x00, 0x00, 0x03 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b4, sizeof(b4), &out),
                              (int)MOQ_ERR_PROTO);
        /* unknown object status (0x20 ... status=0x01) */
        uint8_t b5[] = { 0x20, 0x01, 0x02, 0x03, 0x80, 0x01 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b5, sizeof(b5), &out),
                              (int)MOQ_ERR_PROTO);
        /* mandatory track property (0x4000) carried as an object property */
        uint8_t props[16]; size_t pl = build_props(props, sizeof(props), 0x4000, 0);
        uint8_t b6[64]; moq_buf_writer_t w; moq_buf_writer_init(&w, b6, sizeof(b6));
        moq_buf_write_vi64(&w, 0x01);              /* PROPERTIES */
        moq_buf_write_vi64(&w, 1); moq_buf_write_vi64(&w, 2); moq_buf_write_vi64(&w, 3);
        { uint8_t pr = 0x80; moq_buf_write_raw(&w, &pr, 1); }
        moq_buf_write_vi64(&w, (uint64_t)pl); moq_buf_write_raw(&w, props, pl);
        { uint8_t pp = 0x55; moq_buf_write_raw(&w, &pp, 1); }   /* 1-byte payload */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_object_datagram(b6, moq_buf_writer_offset(&w), &out),
            (int)MOQ_ERR_PROTO);
        /* non-status object with empty payload (mirror D16: malformed) */
        uint8_t b7[] = { 0x00, 0x01, 0x02, 0x03, 0x80 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_object_datagram(b7, sizeof(b7), &out),
                              (int)MOQ_ERR_PROTO);
    }

    /* == Padding datagram (§11.5.2): all-zero ⇒ discard, non-zero ⇒ proto = */
    {
        moq_d18_object_datagram_t out;
        uint8_t pad[16]; moq_buf_writer_t w; moq_buf_writer_init(&w, pad, sizeof(pad));
        moq_buf_write_vi64(&w, MOQ_D18_PADDING_DATAGRAM);
        uint8_t zeros[4] = {0,0,0,0}; moq_buf_write_raw(&w, zeros, 4);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_object_datagram(pad, moq_buf_writer_offset(&w), &out),
            (int)MOQ_DONE);
        /* a non-zero padding byte is malformed (MUST be all zero) */
        moq_buf_writer_t w2; moq_buf_writer_init(&w2, pad, sizeof(pad));
        moq_buf_write_vi64(&w2, MOQ_D18_PADDING_DATAGRAM);
        uint8_t nz[3] = {0,1,0}; moq_buf_write_raw(&w2, nz, 3);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_object_datagram(pad, moq_buf_writer_offset(&w2), &out),
            (int)MOQ_ERR_PROTO);
    }

    /* == Encode is strict: a mandatory track property as an object property is *
     *  rejected before any bytes are written (rollback). */
    {
        uint8_t props[16]; size_t pl = build_props(props, sizeof(props), 0x4000, 0);
        moq_d18_object_datagram_t dg; memset(&dg, 0, sizeof(dg));
        dg.track_alias = 7; dg.group_id = 1; dg.object_id = 1;
        dg.publisher_priority = 1;
        dg.has_properties = true; dg.properties = props; dg.properties_len = pl;
        const uint8_t pay[1] = { 0x5 }; dg.payload = pay; dg.payload_len = 1;
        moq_buf_writer_t w; moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_object_datagram(&w, &dg), (int)MOQ_ERR_PROTO);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);   /* rolled back */
    }

    /* == SimPair end-to-end: object datagram with properties ========== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_sub(sp, &ssub));

        moq_rcbuf_t *pay = NULL;
        const uint8_t data[7] = { 'p','a','y','l','o','a','d' };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(), data, 7, &pay), (int)MOQ_OK);
        uint8_t props[16]; size_t pl = build_props(props, sizeof(props), 2, 9);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_object_datagram(server, ssub, 5, 3, 200, true,
                pay, props, pl, moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(pay);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool got = false; moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                got = o->datagram && o->group_id == 5 && o->object_id == 3 &&
                      o->publisher_priority == 200 && o->end_of_group &&
                      o->payload && moq_rcbuf_len(o->payload) == 7 &&
                      memcmp(moq_rcbuf_data(o->payload), data, 7) == 0 &&
                      o->properties && moq_rcbuf_len(o->properties) == pl;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == SimPair end-to-end: status datagram ========================== */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_sub(sp, &ssub));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_status_datagram(server, ssub, 9, 0, 128,
                MOQ_OBJECT_END_OF_GROUP, moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool got = false; moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                got = o->datagram && o->status == MOQ_OBJECT_END_OF_GROUP &&
                      o->payload == NULL;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Unknown track alias is silently dropped; malformed closes 0x3 = */
    {
        moq_simpair_t *sp = make_pair();
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_sub(sp, &ssub));   /* client alias is 7 */

        /* A valid object datagram for an unknown alias (42) → no event, no close. */
        moq_d18_object_datagram_t dg; memset(&dg, 0, sizeof(dg));
        dg.track_alias = 42; dg.group_id = 1; dg.object_id = 1;
        dg.publisher_priority = 1;
        const uint8_t pay[2] = { 1, 2 }; dg.payload = pay; dg.payload_len = 2;
        uint8_t db[64]; moq_buf_writer_t w; moq_buf_writer_init(&w, db, sizeof(db));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_object_datagram(&w, &dg), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_datagram(client, db, moq_buf_writer_offset(&w),
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_event_t ev; bool any = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);

        /* A malformed datagram (invalid type 0x22) closes the session 0x3. */
        uint8_t bad[] = { 0x22 };
        (void)moq_session_on_datagram(client, bad, sizeof(bad), moq_simpair_now_us(sp));
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_CLOSED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_datagram");
    return failures != 0;
}
