/*
 * Round-trip tests for the draft-18 SUBSCRIBE-family control codec: SUBSCRIBE,
 * SUBSCRIBE_OK, REQUEST_ERROR. Each is encoded as a full message (vi64 Type +
 * 16-bit Length + payload), framed back through moq_d18_decode_envelope, then
 * field-decoded. Also covers the draft-18 zero-field namespace.
 *
 * Parameter/property bodies are not encoded/decoded yet (the happy path uses
 * zero), so these vectors carry zero parameters.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* == SUBSCRIBE round-trip ========================================= */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("live"), MOQ_BYTES_LITERAL("sports")
        };
        moq_namespace_t ns = { parts, 2 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 42, &ns,
                MOQ_BYTES_LITERAL("video"), &(moq_d18_msg_params_t){0}), (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE);
        /* The envelope consumed exactly the whole message. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

        moq_bytes_t dparts[32];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len,
                dparts, 32, &sub), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(sub.request_id, 42);
        MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.count, 2);
        MOQ_TEST_CHECK(sub.track_namespace.parts[0].len == 4 &&
            memcmp(sub.track_namespace.parts[0].data, "live", 4) == 0);
        MOQ_TEST_CHECK(sub.track_namespace.parts[1].len == 6 &&
            memcmp(sub.track_namespace.parts[1].data, "sports", 6) == 0);
        MOQ_TEST_CHECK(sub.track_name.len == 5 &&
            memcmp(sub.track_name.data, "video", 5) == 0);
        MOQ_TEST_CHECK(!sub.params.has_subscriber_priority &&
                       !sub.params.has_forward &&
                       !sub.params.has_group_order && !sub.params.has_filter);
    }

    /* == SUBSCRIBE_OK round-trip (no Request ID) ====================== */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_subscribe_ok(&w, 7, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0}),
                              (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_OK);

        moq_d18_subscribe_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(ok.track_alias, 7);
        MOQ_TEST_CHECK(!ok.params.has_largest && !ok.params.has_expires);
    }

    /* == REQUEST_ERROR round-trip ===================================== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_error(&w, 0x4, 0,
                MOQ_BYTES_LITERAL("nope")), (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);

        moq_d18_request_error_t e;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_error(env.payload, env.payload_len, &e),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(e.error_code, 0x4);
        MOQ_TEST_CHECK_EQ_U64(e.retry_interval, 0);
        MOQ_TEST_CHECK(e.reason.len == 4 &&
                       memcmp(e.reason.data, "nope", 4) == 0);
    }

    /* == Zero-field namespace (draft-18 allows 0..32) ================= */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_namespace_t ns = { NULL, 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 1, &ns, MOQ_BYTES_LITERAL("t"), &(moq_d18_msg_params_t){0}),
            (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[32];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len,
                dp, 32, &sub), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.count, 0);
        MOQ_TEST_CHECK(sub.track_name.len == 1);
    }

    /* == Negative: SUBSCRIBE with nonzero param_count is rejected ===== */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_encode_subscribe(&w, 1, &ns, MOQ_BYTES_LITERAL("v"), &(moq_d18_msg_params_t){0});
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);

        /* The payload ends with the param-count vi64 (0). Flip it to 1: a
         * nonzero count with no parameter body must be rejected. */
        uint8_t pl[64];
        memcpy(pl, env.payload, env.payload_len);
        pl[env.payload_len - 1] = 0x01;
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe(pl, env.payload_len, dp, 8,
                                                &sub) != MOQ_OK);

        /* Trailing garbage after a valid (zero-param) payload is rejected. */
        memcpy(pl, env.payload, env.payload_len);
        pl[env.payload_len] = 0xAA;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe(pl, env.payload_len + 1, dp, 8,
                                                &sub) != MOQ_OK);
    }

    /* == Negative: REQUEST_ERROR trailing bytes / oversize reason ===== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_encode_request_error(&w, 0x1, 0, MOQ_BYTES_LITERAL("x"));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);

        uint8_t pl[64];
        memcpy(pl, env.payload, env.payload_len);
        pl[env.payload_len] = 0xAA;   /* unexpected Redirect/garbage tail */
        moq_d18_request_error_t e;
        MOQ_TEST_CHECK(moq_d18_decode_request_error(pl, env.payload_len + 1,
                                                    &e) != MOQ_OK);

        /* Reason longer than the cap is rejected on encode. */
        static uint8_t big[MOQ_D18_MAX_REASON + 1];
        memset(big, 'a', sizeof(big));
        moq_bytes_t huge = { big, sizeof(big) };
        uint8_t obuf[2048];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, obuf, sizeof(obuf));
        MOQ_TEST_CHECK(moq_d18_encode_request_error(&ow, 0x1, 0, huge)
                       != MOQ_OK);
    }

    /* == Negative: SUBSCRIBE_OK nonzero params / trailing properties == */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_encode_subscribe_ok(&w, 7, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0});
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);

        /* payload = TrackAlias(vi64) + param_count(vi64=0). Flip count to 1. */
        uint8_t pl[64];
        memcpy(pl, env.payload, env.payload_len);
        pl[env.payload_len - 1] = 0x01;
        moq_d18_subscribe_ok_t ok;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe_ok(pl, env.payload_len, &ok)
                       != MOQ_OK);

        /* Trailing Track Properties bytes are rejected (not decoded yet). */
        memcpy(pl, env.payload, env.payload_len);
        pl[env.payload_len] = 0xAA;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe_ok(pl, env.payload_len + 1, &ok)
                       != MOQ_OK);
    }

    /* == Negative: REQUEST_ERROR decode of an oversize reason ========= */
    {
        /* Hand-build a payload whose reason length claims > MOQ_D18_MAX_REASON
         * but the bytes are present (so only the cap, not truncation, fires). */
        static uint8_t big[16 + MOQ_D18_MAX_REASON + 1];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, big, sizeof(big));
        moq_buf_write_vi64(&w, 0x1);                       /* error code */
        moq_buf_write_vi64(&w, 0);                         /* retry */
        moq_buf_write_vi64(&w, MOQ_D18_MAX_REASON + 1);    /* reason length */
        size_t hdr = moq_buf_writer_offset(&w);
        memset(big + hdr, 'a', MOQ_D18_MAX_REASON + 1);
        moq_d18_request_error_t e;
        MOQ_TEST_CHECK(moq_d18_decode_request_error(big,
            hdr + MOQ_D18_MAX_REASON + 1, &e) != MOQ_OK);
    }

    /* == Negative: zero-length namespace field is rejected ============ */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t empty = { (const uint8_t *)"", 0 };
        moq_namespace_t ns = { &empty, 1 };
        MOQ_TEST_CHECK(moq_d18_encode_subscribe(&w, 1, &ns,
            MOQ_BYTES_LITERAL("v"), &(moq_d18_msg_params_t){0}) != MOQ_OK);
    }

    /* == Negative: Full Track Name over 4096 bytes is rejected ======== */
    {
        static uint8_t field[MOQ_D18_MAX_FULL_TRACK + 1];
        memset(field, 'a', sizeof(field));
        moq_bytes_t parts[] = { { field, sizeof(field) } };
        moq_namespace_t ns = { parts, 1 };
        uint8_t buf[8192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_subscribe(&w, 1, &ns,
            MOQ_BYTES_LITERAL("v"), &(moq_d18_msg_params_t){0}) != MOQ_OK);
    }

    /* == Negative (decode): zero-length namespace field rejected ====== */
    {
        uint8_t pl[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 1);   /* request id */
        moq_buf_write_vi64(&w, 1);   /* namespace: 1 field */
        moq_buf_write_vi64(&w, 0);   /* field length 0 -> violation */
        moq_buf_write_vi64(&w, 1);   /* track name length */
        moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        moq_buf_write_vi64(&w, 0);   /* 0 parameters */
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe(pl, moq_buf_writer_offset(&w),
                                                dp, 8, &sub) != MOQ_OK);
    }

    /* == Negative (decode): Full Track Name over 4096 rejected ======== */
    {
        static uint8_t pl[16 + MOQ_D18_MAX_FULL_TRACK + 8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 1);                          /* request id */
        moq_buf_write_vi64(&w, 1);                          /* 1 field */
        moq_buf_write_vi64(&w, MOQ_D18_MAX_FULL_TRACK + 1); /* field length */
        size_t hdr = moq_buf_writer_offset(&w);
        memset(pl + hdr, 'a', MOQ_D18_MAX_FULL_TRACK + 1);
        size_t after = hdr + MOQ_D18_MAX_FULL_TRACK + 1;
        moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, pl + after, sizeof(pl) - after);
        moq_buf_write_vi64(&w2, 0);   /* track name length 0 */
        moq_buf_write_vi64(&w2, 0);   /* 0 parameters */
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe(pl,
            after + moq_buf_writer_offset(&w2), dp, 8, &sub) != MOQ_OK);
    }

    /* == SUBGROUP_HEADER round-trip (mode present, priority present) === */
    {
        moq_d18_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        hdr.track_alias = 0x4040;   /* multi-byte vi64 */
        hdr.group_id = 9;
        hdr.subgroup_id = 2;
        hdr.publisher_priority = 7;
        hdr.end_of_group = true;
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_subgroup_header(&w, &hdr),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_d18_subgroup_type_valid(buf[0]));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d18_subgroup_header_t got;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_subgroup_header(&r, &got),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(got.track_alias, 0x4040);
        MOQ_TEST_CHECK_EQ_U64(got.group_id, 9);
        MOQ_TEST_CHECK_EQ_U64(got.subgroup_id, 2);
        MOQ_TEST_CHECK_EQ_INT((int)got.publisher_priority, 7);
        MOQ_TEST_CHECK(got.end_of_group);
        MOQ_TEST_CHECK(!got.has_properties);
        MOQ_TEST_CHECK(!got.default_priority);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);
    }

    /* == SUBGROUP type validity (form + reserved mode) ================ */
    {
        MOQ_TEST_CHECK(moq_d18_subgroup_type_valid(0x10));   /* mode zero */
        MOQ_TEST_CHECK(moq_d18_subgroup_type_valid(0x14));   /* mode present */
        MOQ_TEST_CHECK(moq_d18_subgroup_type_valid(0x7D));   /* all bits */
        MOQ_TEST_CHECK(!moq_d18_subgroup_type_valid(0x00));  /* bit 4 clear */
        MOQ_TEST_CHECK(!moq_d18_subgroup_type_valid(0x05));  /* FETCH header */
        MOQ_TEST_CHECK(!moq_d18_subgroup_type_valid(0x80));  /* bit 7 set */
        MOQ_TEST_CHECK(!moq_d18_subgroup_type_valid(0x16));  /* mode 0b11 */
    }

    /* == SUBGROUP decode rejects reserved mode 0b11 =================== */
    {
        uint8_t buf[] = { 0x16, 0x01, 0x02 };   /* type 0x16: mode 0b11 */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, sizeof(buf));
        moq_d18_subgroup_header_t got;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subgroup_header(&r, &got), (int)MOQ_ERR_PROTO);
    }

    /* == SUBGROUP default-priority omits the priority byte ============= */
    {
        moq_d18_subgroup_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO;
        hdr.default_priority = true;
        hdr.track_alias = 1;
        hdr.group_id = 1;
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_subgroup_header(&w, &hdr),
                              (int)MOQ_OK);
        /* type + track_alias + group_id, no subgroup id, no priority byte. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 3);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d18_subgroup_header_t got;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_subgroup_header(&r, &got),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got.default_priority);
    }

    /* == FETCH (standalone) round-trip ================================ */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 4;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("video");
        f.start = (moq_d18_location_t){ 2, 0 };
        f.end = (moq_d18_location_t){ 5, 0 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_FETCH);
        moq_bytes_t dp[8];
        moq_d18_fetch_t got;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(env.payload, env.payload_len, dp, 8, &got),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(got.request_id, 4);
        MOQ_TEST_CHECK_EQ_U64(got.fetch_type, MOQ_D18_FETCH_TYPE_STANDALONE);
        MOQ_TEST_CHECK_EQ_SIZE(got.track_namespace.count, 1);
        MOQ_TEST_CHECK(got.track_name.len == 5 &&
                       memcmp(got.track_name.data, "video", 5) == 0);
        MOQ_TEST_CHECK_EQ_U64(got.start.group, 2);
        MOQ_TEST_CHECK_EQ_U64(got.end.group, 5);
        /* Trailing byte after a well-formed FETCH is rejected. */
        MOQ_TEST_CHECK(moq_d18_decode_fetch(env.payload, env.payload_len + 1,
            dp, 8, &got) != MOQ_OK);
    }

    /* == FETCH bad fetch type is rejected ============================= */
    {
        uint8_t pl[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 1);   /* request id */
        moq_buf_write_vi64(&w, 9);   /* invalid fetch type */
        moq_bytes_t dp[8];
        moq_d18_fetch_t got;
        MOQ_TEST_CHECK(moq_d18_decode_fetch(pl, moq_buf_writer_offset(&w),
            dp, 8, &got) != MOQ_OK);
    }

    /* == FETCH_OK round-trip + trailing/param strictness ============== */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_fetch_ok(&w, true, (moq_d18_location_t){ 9, 3 }, (moq_bytes_t){0}), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_FETCH_OK);
        moq_d18_fetch_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.end_of_track);
        MOQ_TEST_CHECK_EQ_U64(ok.end.group, 9);
        MOQ_TEST_CHECK_EQ_U64(ok.end.object, 3);
        MOQ_TEST_CHECK(moq_d18_decode_fetch_ok(env.payload, env.payload_len + 1,
            &ok) != MOQ_OK);
    }

    /* == FETCH_HEADER round-trip + wrong type ========================= */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch_header(&w, 42),
                              (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        uint64_t rid = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_fetch_header(&r, &rid),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(rid, 42);
        /* A subgroup-typed stream is not a FETCH_HEADER. */
        uint8_t bad[] = { 0x14, 0x00 };
        moq_buf_reader_t r2;
        moq_buf_reader_init(&r2, bad, sizeof(bad));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_fetch_header(&r2, &rid),
                              (int)MOQ_ERR_PROTO);
    }

    /* == FETCH decode rejects NULL parts (standalone deref guard) ===== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 1;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("v");
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        moq_d18_fetch_t got;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(env.payload, env.payload_len, NULL, 8, &got),
            (int)MOQ_ERR_INVAL);
    }

    /* == FETCH encoders roll back the writer on a too-small buffer ===== */
    {
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 1;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("video");
        for (size_t cap = 1; cap < 12; cap++) {
            uint8_t small[16];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, small, cap);
            if (moq_d18_encode_fetch(&w, &f) != MOQ_OK)
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        }
        for (size_t cap = 1; cap < 6; cap++) {
            uint8_t small[16];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, small, cap);
            if (moq_d18_encode_fetch_ok(&w, true, (moq_d18_location_t){ 1, 2 }, (moq_bytes_t){0}) != MOQ_OK)
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        }
        for (size_t cap = 1; cap < 4; cap++) {
            uint8_t small[16];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, small, cap);
            if (moq_d18_encode_fetch_header(&w, 0x4040) != MOQ_OK)
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
        }
    }

    /* == SUBSCRIBE_TRACKS round-trip (FORWARD=0 + auth token) ========= */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = {
            MOQ_BYTES_LITERAL("live"), MOQ_BYTES_LITERAL("sports")
        };
        moq_namespace_t pfx = { parts, 2 };
        moq_d18_msg_params_t p;
        memset(&p, 0, sizeof(p));
        p.has_forward = true;
        p.forward = 0;
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 5;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_tracks(&w, 9, &pfx, &p), (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_TRACKS);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

        moq_bytes_t dp[32];
        moq_d18_subscribe_tracks_t st;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_tracks(env.payload, env.payload_len,
                dp, 32, &st), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.request_id, 9);
        MOQ_TEST_CHECK_EQ_SIZE(st.track_namespace_prefix.count, 2);
        MOQ_TEST_CHECK(st.track_namespace_prefix.parts[1].len == 6 &&
            memcmp(st.track_namespace_prefix.parts[1].data, "sports", 6) == 0);
        MOQ_TEST_CHECK(st.params.has_forward && st.params.forward == 0);
        MOQ_TEST_CHECK_EQ_SIZE(st.params.auth_token_count, 1);
        MOQ_TEST_CHECK(st.params.auth_tokens[0].token_type == 5 &&
            st.params.auth_tokens[0].token_value.len == 3);
    }

    /* == SUBSCRIBE_TRACKS round-trip (zero params, zero-field prefix) == */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_namespace_t pfx = { NULL, 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_tracks(&w, 2, &pfx,
                &(moq_d18_msg_params_t){0}), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[32];
        moq_d18_subscribe_tracks_t st;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_tracks(env.payload, env.payload_len,
                dp, 32, &st), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(st.track_namespace_prefix.count, 0);
        MOQ_TEST_CHECK(!st.params.has_forward);
    }

    /* == SUBSCRIBE_TRACKS rejects a disallowed parameter ============== */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_namespace_t pfx = { NULL, 0 };
        moq_d18_msg_params_t p;
        memset(&p, 0, sizeof(p));
        p.has_subscriber_priority = true;   /* not permitted on SUBSCRIBE_TRACKS */
        p.subscriber_priority = 5;
        MOQ_TEST_CHECK(moq_d18_encode_subscribe_tracks(&w, 1, &pfx, &p)
                       != MOQ_OK);
    }

    /* == SUBSCRIBE_TRACKS decode rejects an over-32 prefix ============ */
    {
        uint8_t pl[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 1);    /* request id */
        moq_buf_write_vi64(&w, 33);   /* 33 prefix fields -> violation */
        moq_bytes_t dp[40];
        moq_d18_subscribe_tracks_t st;
        MOQ_TEST_CHECK(moq_d18_decode_subscribe_tracks(pl,
            moq_buf_writer_offset(&w), dp, 40, &st) != MOQ_OK);
    }

    /* == PUBLISH_BLOCKED round-trip =================================== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("room42") };
        moq_namespace_t sfx = { parts, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_blocked(&w, &sfx,
                MOQ_BYTES_LITERAL("audio")), (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH_BLOCKED);

        moq_bytes_t dp[32];
        moq_d18_publish_blocked_t pb;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_blocked(env.payload, env.payload_len,
                dp, 32, &pb), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(pb.track_namespace_suffix.count, 1);
        MOQ_TEST_CHECK(pb.track_namespace_suffix.parts[0].len == 6 &&
            memcmp(pb.track_namespace_suffix.parts[0].data, "room42", 6) == 0);
        MOQ_TEST_CHECK(pb.track_name.len == 5 &&
            memcmp(pb.track_name.data, "audio", 5) == 0);

        /* Trailing byte after a well-formed PUBLISH_BLOCKED is rejected. */
        uint8_t pl[64];
        memcpy(pl, env.payload, env.payload_len);
        pl[env.payload_len] = 0xAA;
        MOQ_TEST_CHECK(moq_d18_decode_publish_blocked(pl, env.payload_len + 1,
            dp, 32, &pb) != MOQ_OK);
    }

    /* == PUBLISH_BLOCKED zero-field suffix round-trip ================= */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_namespace_t sfx = { NULL, 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_blocked(&w, &sfx,
                MOQ_BYTES_LITERAL("t")), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[32];
        moq_d18_publish_blocked_t pb;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_blocked(env.payload, env.payload_len,
                dp, 32, &pb), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(pb.track_namespace_suffix.count, 0);
        MOQ_TEST_CHECK(pb.track_name.len == 1);
    }

    /* == PUBLISH round-trip (alias + FORWARD + properties) =========== */
    {
        uint8_t buf[256];
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_d18_publish_t p = { 0 };
        p.request_id = 8;
        p.track_namespace = (moq_namespace_t){ parts, 1 };
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 12;
        p.params.has_forward = true; p.params.forward = 0;
        uint8_t props[] = { 0x02, 0x00 };
        p.track_properties = (moq_bytes_t){ props, sizeof(props) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&w, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH);
        moq_bytes_t dp[8];
        moq_d18_publish_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(out.request_id, 8);
        MOQ_TEST_CHECK_EQ_U64(out.track_alias, 12);
        MOQ_TEST_CHECK(out.params.has_forward && out.params.forward == 0);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_properties.len, 2);
    }

    /* == PUBLISH_OK round-trip (delivery params, empty properties) ==== */
    {
        uint8_t buf[256];
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 3;
        p.has_group_order = true; p.group_order = MOQ_GROUP_ORDER_ASCENDING;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish_ok(&w, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
        moq_d18_publish_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.params.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_U64(ok.params.subscriber_priority, 3);
        MOQ_TEST_CHECK(ok.params.has_group_order);
    }

    /* == GOAWAY round-trip (URI + timeout + request id) ============== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_goaway(&w, (const uint8_t *)"https://r", 9,
                                       3000, 4), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_GOAWAY);
        moq_d18_goaway_t ga;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway(env.payload, env.payload_len, &ga),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 9);
        MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, 3000);
        MOQ_TEST_CHECK_EQ_U64(ga.request_id, 4);
    }

    /* == REQUEST_ERROR(REDIRECT) round-trip (Redirect tail) ========== */
    {
        uint8_t buf[128];
        moq_bytes_t parts[1] = { MOQ_BYTES_LITERAL("alt") };
        moq_d18_redirect_t rd;
        memset(&rd, 0, sizeof(rd));
        rd.connect_uri = MOQ_BYTES_LITERAL("https://r2");
        rd.track_namespace = (moq_namespace_t){ parts, 1 };
        rd.track_name = MOQ_BYTES_LITERAL("v2");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_error_redirect(&w,
                MOQ_REQUEST_ERROR_REDIRECT, 0, MOQ_BYTES_LITERAL("go"), &rd),
            (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);
        moq_bytes_t dp[8];
        moq_d18_request_error_t er;
        moq_d18_redirect_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_error_redirect(env.payload,
                env.payload_len, dp, 8, &er, &out), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(er.error_code, MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK_EQ_SIZE(er.reason.len, 2);
        MOQ_TEST_CHECK_EQ_SIZE(out.connect_uri.len, 10);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_name.len, 2);
    }

    /* == Request-stream GOAWAY round-trip (no Request ID) ============ */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_goaway_request(&w, (const uint8_t *)"https://r",
                                               9, 3000), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_GOAWAY);
        moq_d18_goaway_t ga;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway_request(env.payload, env.payload_len, &ga),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 9);
        MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, 3000);
        MOQ_TEST_CHECK_EQ_U64(ga.request_id, 0);
    }

    /* == Track-property UNSUPPORTED_EXTENSION detection (§2.5.1) ====== *
     * The decoder accepts an unknown Mandatory Track Property (0x4000-0x7FFF) and
     * flags it (track_properties_unsupported) rather than failing — the session
     * core turns the flag into a request-level UNSUPPORTED_EXTENSION response.
     * Malformed KVP structure is still a hard PROTOCOL_VIOLATION. */
    {
        uint8_t pl[64];
        moq_buf_writer_t w;
        moq_d18_subscribe_ok_t ok;

        /* Mandatory property present -> OK + flag set. */
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 7);       /* track_alias */
        moq_buf_write_vi64(&w, 0);       /* param count */
        moq_buf_write_vi64(&w, 0x4000);  /* mandatory type (even) */
        moq_buf_write_vi64(&w, 0);       /* value */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(pl, moq_buf_writer_offset(&w), &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.track_properties_unsupported);

        /* Non-mandatory property -> OK + flag clear. */
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 7);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 2);       /* non-mandatory even type */
        moq_buf_write_vi64(&w, 0x41);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(pl, moq_buf_writer_offset(&w), &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(!ok.track_properties_unsupported);

        /* Mandatory hidden inside IMMUTABLE_PROPERTIES (0x0B) -> flag set. */
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 7);
        moq_buf_write_vi64(&w, 0);
        uint8_t nested[8];
        moq_buf_writer_t nw;
        moq_buf_writer_init(&nw, nested, sizeof(nested));
        moq_buf_write_vi64(&nw, 0x4000); /* mandatory inside */
        moq_buf_write_vi64(&nw, 0);
        moq_buf_write_vi64(&w, 0x0B);    /* IMMUTABLE_PROPERTIES (odd) */
        moq_buf_write_vi64(&w, moq_buf_writer_offset(&nw));
        moq_buf_write_raw(&w, nested, moq_buf_writer_offset(&nw));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(pl, moq_buf_writer_offset(&w), &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.track_properties_unsupported);

        /* Malformed KVP (odd type, length past end) -> PROTOCOL_VIOLATION. */
        moq_buf_writer_init(&w, pl, sizeof(pl));
        moq_buf_write_vi64(&w, 7);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 3);       /* odd type */
        moq_buf_write_vi64(&w, 99);      /* declared length > remaining */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(pl, moq_buf_writer_offset(&w), &ok),
            (int)MOQ_ERR_PROTO);
    }

    MOQ_TEST_PASS("control_d18");
    return failures != 0;
}
