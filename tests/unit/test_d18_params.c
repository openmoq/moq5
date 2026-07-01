/*
 * Draft-18 request Message Parameters (SUBSCRIBER_PRIORITY, FORWARD,
 * GROUP_ORDER, SUBSCRIPTION_FILTER) on SUBSCRIBE and FETCH, both directions.
 * Covers the codec (round-trip, ascending order, allowed-set/unknown rejection),
 * and that an inbound request surfaces the real parameter values to the
 * publisher/relay via the request events.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Codec round-trips --------------------------------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = MOQ_BYTES_LITERAL("video");

    /* SUBSCRIBE carrying all four params incl. an AbsoluteRange filter. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 9;
        p.has_forward = true; p.forward = 0;
        p.has_group_order = true; p.group_order = 2;
        p.has_filter = true; p.filter_type = 4;  /* AbsoluteRange */
        p.filter_start_group = 3; p.filter_start_object = 7;
        p.filter_end_group = 10;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 4, &ns, tn, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_OK);
        MOQ_TEST_CHECK(sub.params.has_subscriber_priority &&
                       sub.params.subscriber_priority == 9);
        MOQ_TEST_CHECK(sub.params.has_forward && sub.params.forward == 0);
        MOQ_TEST_CHECK(sub.params.has_group_order && sub.params.group_order == 2);
        MOQ_TEST_CHECK(sub.params.has_filter && sub.params.filter_type == 4);
        MOQ_TEST_CHECK_EQ_U64(sub.params.filter_start_group, 3);
        MOQ_TEST_CHECK_EQ_U64(sub.params.filter_start_object, 7);
        MOQ_TEST_CHECK_EQ_U64(sub.params.filter_end_group, 10);
    }

    /* Each filter type round-trips (1 next-group, 2 largest, 3 absolute-start). */
    {
        uint32_t types[] = { 1, 2, 3 };
        for (size_t i = 0; i < 3; i++) {
            moq_d18_msg_params_t p = { 0 };
            p.has_filter = true; p.filter_type = types[i];
            p.filter_start_group = 5; p.filter_start_object = 1;
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_subscribe(&w, 0, &ns, tn, &p), (int)MOQ_OK);
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
            moq_control_envelope_t env;
            moq_d18_decode_envelope(&r, &env);
            moq_bytes_t dp[8];
            moq_d18_subscribe_t sub;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp,
                                              8, &sub), (int)MOQ_OK);
            MOQ_TEST_CHECK(sub.params.has_filter &&
                           sub.params.filter_type == types[i]);
            if (types[i] == 3)
                MOQ_TEST_CHECK_EQ_U64(sub.params.filter_start_group, 5);
        }
    }

    /* FETCH carries SUBSCRIBER_PRIORITY / GROUP_ORDER. */
    {
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 2;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = ns;
        f.track_name = tn;
        f.start = (moq_d18_location_t){ 0, 0 };
        f.end = (moq_d18_location_t){ 10, 0 };
        f.params.has_subscriber_priority = true; f.params.subscriber_priority = 3;
        f.params.has_group_order = true; f.params.group_order = 1;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_fetch_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(out.params.has_subscriber_priority &&
                       out.params.subscriber_priority == 3);
        MOQ_TEST_CHECK(out.params.has_group_order && out.params.group_order == 1);
    }

    /* A FETCH carrying a FORWARD parameter is rejected (not in FETCH's set). */
    {
        /* Hand-encode a FETCH with a single FORWARD parameter. */
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_FETCH);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 2);                      /* request id */
        moq_buf_write_vi64(&w, MOQ_D18_FETCH_TYPE_STANDALONE);
        moq_buf_write_vi64(&w, 1);                      /* ns count */
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, 0); moq_buf_write_vi64(&w, 0);  /* start */
        moq_buf_write_vi64(&w, 10); moq_buf_write_vi64(&w, 0); /* end */
        moq_buf_write_vi64(&w, 1);                      /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_FORWARD);
        uint8_t v = 1; moq_buf_write_raw(&w, &v, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_fetch_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_ERR_PROTO);
    }

    /* Encode-side mask: a FETCH cannot carry FORWARD or SUBSCRIPTION_FILTER;
     * the encoder rejects it with the writer left untouched (rollback). */
    {
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = ns; f.track_name = tn;
        f.end = (moq_d18_location_t){ 10, 0 };
        f.params.has_forward = true; f.params.forward = 1;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        f.params.has_forward = false;
        f.params.has_filter = true; f.params.filter_type = 2;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        /* ...nor a delivery-timeout parameter (object or subgroup). */
        f.params.has_filter = false;
        f.params.has_object_delivery_timeout = true;
        f.params.object_delivery_timeout_ms = 1000;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        f.params.has_object_delivery_timeout = false;
        f.params.has_subgroup_delivery_timeout = true;
        f.params.subgroup_delivery_timeout_ms = 1000;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* Encode-side mask: a REQUEST_UPDATE cannot carry GROUP_ORDER or a filter. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_group_order = true; p.group_order = 1;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_update(&w, 2, &p),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        moq_d18_msg_params_t p2 = { 0 };
        p2.has_filter = true; p2.filter_type = 3;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_update(&w, 2, &p2),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* SUBSCRIBE_OK round-trip: LARGEST_OBJECT + EXPIRES params + opaque
     * Track Properties tail (preserved byte-for-byte). */
    {
        /* A valid property tail: SUBGROUP_DELIVERY_TIMEOUT (0x06, even) = 3000. */
        uint8_t props[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x06);
        moq_buf_write_vi64(&pw, 3000);
        moq_bytes_t pb = { props, moq_buf_writer_offset(&pw) };

        moq_d18_msg_params_t p = { 0 };
        p.has_largest = true; p.largest_group = 100; p.largest_object = 5;
        p.has_expires = true; p.expires_ms = 60000;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, 7, &p, pb), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_subscribe_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(ok.track_alias, 7);
        MOQ_TEST_CHECK(ok.params.has_largest &&
                       ok.params.largest_group == 100 &&
                       ok.params.largest_object == 5);
        MOQ_TEST_CHECK(ok.params.has_expires && ok.params.expires_ms == 60000);
        MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, pb.len);
        MOQ_TEST_CHECK(memcmp(ok.track_properties.data, pb.data, pb.len) == 0);
    }

    /* FETCH_OK round-trip with an opaque property tail. */
    {
        uint8_t props[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x04);   /* MAX_CACHE_DURATION (even) */
        moq_buf_write_vi64(&pw, 9000);
        moq_bytes_t pb = { props, moq_buf_writer_offset(&pw) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_fetch_ok(&w, true, (moq_d18_location_t){ 9, 3 },
                                         pb), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_fetch_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.end_of_track && ok.end.group == 9 && ok.end.object == 3);
        MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, pb.len);
        MOQ_TEST_CHECK(memcmp(ok.track_properties.data, pb.data, pb.len) == 0);
    }

    /* A mandatory-but-unknown property (type 0x4000) is rejected on encode, but
     * on decode it is accepted and flagged (track_properties_unsupported) so the
     * session core can respond with a request-level UNSUPPORTED_EXTENSION instead
     * of closing the session (§2.5.1). */
    {
        uint8_t props[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x4000);   /* even, mandatory range */
        moq_buf_write_vi64(&pw, 0);
        moq_bytes_t pb = { props, moq_buf_writer_offset(&pw) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_msg_params_t p = { 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, 7, &p, pb), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);  /* rollback */

        /* Hand-build the wire (track_alias + 0 params + mandatory property). */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_OK);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 7);   /* track alias */
        moq_buf_write_vi64(&w, 0);   /* 0 params */
        moq_buf_write_raw(&w, pb.data, pb.len);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_subscribe_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.track_properties_unsupported);
    }

    /* A malformed property tail (odd type claiming more bytes than present) is
     * rejected. */
    {
        uint8_t props[4];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x0B);   /* odd: length-prefixed */
        moq_buf_write_vi64(&pw, 10);     /* claims 10 bytes, none follow */
        moq_bytes_t pb = { props, moq_buf_writer_offset(&pw) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_msg_params_t p = { 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, 7, &p, pb), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* IMMUTABLE_PROPERTIES (0x0B) wrapping a mandatory-unknown property is
     * rejected (the validator recurses into the nested KVP sequence); a benign
     * inner property is accepted; nested immutable-in-immutable is rejected. */
    {
        /* Inner: a mandatory unknown property [0x4000, 0]. */
        uint8_t inner[8];
        moq_buf_writer_t iw;
        moq_buf_writer_init(&iw, inner, sizeof(inner));
        moq_buf_write_vi64(&iw, 0x4000);
        moq_buf_write_vi64(&iw, 0);
        uint8_t props[16];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x0B);                         /* IMMUTABLE */
        moq_buf_write_vi64(&pw, moq_buf_writer_offset(&iw));   /* length */
        moq_buf_write_raw(&pw, inner, moq_buf_writer_offset(&iw));
        moq_bytes_t pb = { props, moq_buf_writer_offset(&pw) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_msg_params_t pp = { 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, 7, &pp, pb), (int)MOQ_ERR_INVAL);

        /* Benign inner property [0x06, 100] is accepted. */
        moq_buf_writer_init(&iw, inner, sizeof(inner));
        moq_buf_write_vi64(&iw, 0x06);
        moq_buf_write_vi64(&iw, 100);
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x0B);
        moq_buf_write_vi64(&pw, moq_buf_writer_offset(&iw));
        moq_buf_write_raw(&pw, inner, moq_buf_writer_offset(&iw));
        pb.data = props; pb.len = moq_buf_writer_offset(&pw);
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, 7, &pp, pb), (int)MOQ_OK);

        /* Immutable nested inside immutable is malformed. */
        moq_buf_writer_init(&iw, inner, sizeof(inner));
        moq_buf_write_vi64(&iw, 0x0B);
        moq_buf_write_vi64(&iw, 0);
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x0B);
        moq_buf_write_vi64(&pw, moq_buf_writer_offset(&iw));
        moq_buf_write_raw(&pw, inner, moq_buf_writer_offset(&iw));
        pb.data = props; pb.len = moq_buf_writer_offset(&pw);
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_ok(&w, 7, &pp, pb), (int)MOQ_ERR_INVAL);
    }

    /* Delivery-timeout params on SUBSCRIBE: object-only, subgroup-only, both. */
    {
        struct { bool obj, sub; uint64_t ov, sv; } cases[] = {
            { true, false, 1500, 0 },
            { false, true, 0, 2500 },
            { true, true, 3500, 3500 },
        };
        for (size_t i = 0; i < 3; i++) {
            moq_d18_msg_params_t p = { 0 };
            p.has_object_delivery_timeout = cases[i].obj;
            p.object_delivery_timeout_ms = cases[i].ov;
            p.has_subgroup_delivery_timeout = cases[i].sub;
            p.subgroup_delivery_timeout_ms = cases[i].sv;
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_subscribe(&w, 0, &ns, tn, &p), (int)MOQ_OK);
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
            moq_control_envelope_t env;
            moq_d18_decode_envelope(&r, &env);
            moq_bytes_t dp[8];
            moq_d18_subscribe_t sub;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp,
                                              8, &sub), (int)MOQ_OK);
            MOQ_TEST_CHECK(sub.params.has_object_delivery_timeout == cases[i].obj);
            if (cases[i].obj)
                MOQ_TEST_CHECK_EQ_U64(sub.params.object_delivery_timeout_ms,
                                      cases[i].ov);
            MOQ_TEST_CHECK(sub.params.has_subgroup_delivery_timeout == cases[i].sub);
            if (cases[i].sub)
                MOQ_TEST_CHECK_EQ_U64(sub.params.subgroup_delivery_timeout_ms,
                                      cases[i].sv);
        }
    }

    /* A FETCH carrying a delivery-timeout parameter is rejected (not in its
     * permitted set). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_FETCH);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 2);
        moq_buf_write_vi64(&w, MOQ_D18_FETCH_TYPE_STANDALONE);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, 0); moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 10); moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);   /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_OBJECT_DELIVERY_TIMEOUT);
        moq_buf_write_vi64(&w, 1000);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_fetch_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_ERR_PROTO);
    }

    /* Out-of-range GROUP_ORDER (0) and filter type (5) are rejected. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_group_order = true; p.group_order = 0;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_msg_params(&w, &p) != MOQ_OK);
        moq_d18_msg_params_t p2 = { 0 };
        p2.has_filter = true; p2.filter_type = 5;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_msg_params(&w, &p2) != MOQ_OK);
    }

    return failures;
}

/* -- Inbound surfacing --------------------------------------------- */

static moq_session_t *make_server(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

static moq_session_t *make_client(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

int main(void)
{
    int failures = 0;
    failures += test_codec();

    /* == Inbound SUBSCRIBE surfaces the real parameters ============== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);

        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 17;
        p.has_forward = true; p.forward = 0;
        p.has_group_order = true; p.group_order = 2;
        p.has_filter = true; p.filter_type = 4;
        p.filter_start_group = 2; p.filter_start_object = 4; p.filter_end_group = 6;
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("video"), &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
                msg, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);

        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.subscriber_priority, 17);
                MOQ_TEST_CHECK(!ev.u.subscribe_request.forward);
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_request.group_order, 2);
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_request.filter,
                                      (int)MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.start_group, 2);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.start_object, 4);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.end_group, 6);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound FETCH surfaces priority / group order =============== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);

        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 0;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("video");
        f.start = (moq_d18_location_t){ 0, 0 };
        f.end = (moq_d18_location_t){ 10, 0 };
        f.params.has_subscriber_priority = true; f.params.subscriber_priority = 4;
        f.params.has_group_order = true; f.params.group_order = 2;
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_fetch(&w, &f);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
                msg, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);

        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.subscriber_priority, 4);
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.fetch_request.group_order, 2);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound SUBSCRIBE delivery-timeout surfaces / mismatch closes = */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.has_object_delivery_timeout = true; p.object_delivery_timeout_ms = 3000;
        p.has_subgroup_delivery_timeout = true; p.subgroup_delivery_timeout_ms = 3000;
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns2 = { parts, 1 };
        moq_d18_encode_subscribe(&w, 0, &ns2, MOQ_BYTES_LITERAL("video"), &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
                msg, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.subscribe_request.delivery_timeout_us, 3000000);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);

        /* Mismatched object/subgroup timeouts close the session. */
        moq_session_t *s2 = make_server();
        MOQ_TEST_CHECK(s2 != NULL);
        p.object_delivery_timeout_ms = 3000;
        p.subgroup_delivery_timeout_ms = 5000;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_subscribe(&w, 0, &ns2, MOQ_BYTES_LITERAL("video"), &p);
        (void)moq_session_on_bidi_stream_bytes(s2, moq_stream_ref_from_u64(1),
            msg, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s2->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s2);
    }

    /* == Inbound FETCH with no GROUP_ORDER defaults to Ascending ====== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 0;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("video");
        f.end = (moq_d18_location_t){ 10, 0 };
        /* No parameters. */
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_fetch(&w, &f);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
                msg, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.fetch_request.group_order,
                                      (int)MOQ_GROUP_ORDER_ASCENDING);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(s);
    }

    /* == End-to-end: subscriber's non-default params reach publisher == */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        sub.has_subscriber_priority = true; sub.subscriber_priority = 22;
        sub.has_forward = true; sub.forward = false;
        sub.group_order = MOQ_GROUP_ORDER_DESCENDING;
        sub.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        sub.start_group = 8; sub.start_object = 2;
        moq_subscription_t ch;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(client, &sub, 1, &ch),
                              (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.subscriber_priority, 22);
                MOQ_TEST_CHECK(!ev.u.subscribe_request.forward);
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_request.group_order,
                                      (int)MOQ_GROUP_ORDER_DESCENDING);
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_request.filter,
                                      (int)MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.start_group, 8);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.start_object, 2);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Inbound SUBSCRIBE_OK with a mandatory-unknown property: the subscriber
     * surfaces SUBSCRIBE_ERROR(UNSUPPORTED_EXTENSION) and cancels the subscription
     * rather than closing the session (§2.5.1). */
    {
        moq_session_t *s = make_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        moq_subscription_t h;
        moq_session_subscribe(s, &sub, 1, &h);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* Hand-build a SUBSCRIBE_OK whose property tail carries a mandatory
         * unknown property (0x4000). */
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_OK);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 7);   /* track alias */
        moq_buf_write_vi64(&w, 0);   /* 0 params */
        moq_buf_write_vi64(&w, 0x4000); moq_buf_write_vi64(&w, 0);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, buf,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR)
                got = ev.u.subscribe_error.error_code ==
                      MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == End-to-end: accept with largest/expires/properties surfaces === */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        moq_subscription_t ch;
        moq_session_subscribe(client, &sub, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscription_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) sh = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        /* Accept with a Largest Object, Expires, and an opaque property tail. */
        uint8_t props[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x06); moq_buf_write_vi64(&pw, 4000);
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        acfg.has_largest = true; acfg.largest_group = 42; acfg.largest_object = 7;
        acfg.has_expires = true; acfg.expires_ms = 30000;
        acfg.track_properties = (moq_bytes_t){ props, moq_buf_writer_offset(&pw) };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe(server, sh, &acfg,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                ok = true;
                MOQ_TEST_CHECK(ev.u.subscribe_ok.has_largest &&
                               ev.u.subscribe_ok.largest_group == 42 &&
                               ev.u.subscribe_ok.largest_object == 7);
                MOQ_TEST_CHECK(ev.u.subscribe_ok.has_expires &&
                               ev.u.subscribe_ok.expires_ms == 30000);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_ok.track_properties.len,
                                       moq_buf_writer_offset(&pw));
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Inbound FETCH_OK surfaces property bytes to the fetcher ====== *
     *  Poll immediately so the borrowed (scratch) property bytes are still valid
     *  and can be compared exactly. */
    {
        moq_session_t *s = make_client();
        MOQ_TEST_CHECK(s != NULL);
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 10;
        moq_fetch_t fh;
        moq_session_fetch(s, &fc, 1, &fh);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t props[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x04); moq_buf_write_vi64(&pw, 7000);
        uint8_t okbuf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, okbuf, sizeof(okbuf));
        moq_d18_encode_fetch_ok(&w, false, (moq_d18_location_t){ 10, 0 },
            (moq_bytes_t){ props, moq_buf_writer_offset(&pw) });
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, okbuf,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OK) {
                ok = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_ok.track_properties.len,
                                       moq_buf_writer_offset(&pw));
                MOQ_TEST_CHECK(memcmp(ev.u.fetch_ok.track_properties.data,
                                      props, moq_buf_writer_offset(&pw)) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == End-to-end: accept_fetch with properties surfaces to fetcher == */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 10;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(client, &fc, moq_simpair_now_us(sp), &fh),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_t sfh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) sfh = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        uint8_t props[8];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, props, sizeof(props));
        moq_buf_write_vi64(&pw, 0x04); moq_buf_write_vi64(&pw, 7000);
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 10;
        ac.track_properties = (moq_bytes_t){ props, moq_buf_writer_offset(&pw) };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OK) {
                ok = true;
                /* Properties reached the fetcher (the borrowed bytes are only
                 * valid until the next advancing call, so assert the length
                 * here; byte-exactness is checked by the inbound test below). */
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_ok.track_properties.len,
                                       moq_buf_writer_offset(&pw));
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_params");
    return failures != 0;
}
