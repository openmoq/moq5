#include <moq/codec.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* == Envelope tests ============================================ */

    /* -- Encode/decode roundtrip ----------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t payload[] = { 0xAA, 0xBB, 0xCC };
        MOQ_TEST_CHECK(moq_control_encode_envelope(&w, MOQ_D16_GOAWAY,
                                           payload, 3) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));

        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_GOAWAY);
        MOQ_TEST_CHECK(env.payload_len == 3);
        MOQ_TEST_CHECK(memcmp(env.payload, payload, 3) == 0);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
    }

    /* -- Decode truncation: no type byte --------------------------- */
    {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, NULL, 0);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Decode truncation: type but no length --------------------- */
    {
        uint8_t buf[] = { 0x20 }; /* type only */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 1);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Decode truncation: type + length but payload short -------- */
    {
        uint8_t buf[] = { 0x20, 0x00, 0x05, 0xAA }; /* len=5 but 1 byte payload */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, 4);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_reader_offset(&r) == 0);
    }

    /* -- Unknown message type decodes at envelope level ------------ */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0xFF) == MOQ_OK); /* unknown type */
        MOQ_TEST_CHECK(moq_buf_write_uint16(&w, 2) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, (const uint8_t *)"\xDE\xAD", 2) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == 0xFF);
        MOQ_TEST_CHECK(env.payload_len == 2);
    }

    /* -- write_header + patch -------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        size_t len_off = 0;
        MOQ_TEST_CHECK(moq_control_write_header(&w, MOQ_D16_CLIENT_SETUP, &len_off) == MOQ_OK);
        size_t payload_start = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_OK); /* zero params */
        uint16_t plen = (uint16_t)(moq_buf_writer_offset(&w) - payload_start);
        MOQ_TEST_CHECK(moq_buf_patch_uint16(&w, len_off, plen) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_CLIENT_SETUP);
        MOQ_TEST_CHECK(env.payload_len == plen);
    }

    /* == SETUP tests =============================================== */

    /* -- CLIENT_SETUP roundtrip with params ------------------------ */
    {
        /* Build params: AUTHORITY(0x05, odd, "example.com") */
        moq_kvp_entry_t params[4];
        params[0].type      = 0x05;
        params[0].value     = (const uint8_t *)"example.com";
        params[0].value_len = 11;
        params[0].is_varint = false;
        params[0].raw       = NULL;
        params[0].raw_len   = 0;

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_client_setup(&w, params, 1) == MOQ_OK);

        /* Decode envelope first. */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_CLIENT_SETUP);

        /* Decode setup payload. */
        moq_kvp_entry_t dec_params[8];
        moq_d16_setup_t setup = {
            .params = dec_params,
            .params_count = 0,
            .params_cap = 8
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(env.payload, env.payload_len,
                                           &setup) == MOQ_OK);
        MOQ_TEST_CHECK(setup.params_count == 1);
        MOQ_TEST_CHECK(setup.params[0].type == 0x05);
        MOQ_TEST_CHECK(setup.params[0].is_varint == false);
        MOQ_TEST_CHECK(setup.params[0].value_len == 11);
        MOQ_TEST_CHECK(memcmp(setup.params[0].value, "example.com", 11) == 0);
    }

    /* -- SERVER_SETUP roundtrip ------------------------------------ */
    {
        /* MAX_REQUEST_ID(0x02, even, varint value 100) */
        uint8_t vbuf[8];
        size_t vlen = moq_quic_varint_encode(100, vbuf, sizeof(vbuf));
        MOQ_TEST_CHECK(vlen > 0);
        MOQ_TEST_CHECK(vlen > 0);

        moq_kvp_entry_t params[4];
        params[0].type      = 0x02;
        params[0].value     = vbuf;
        params[0].value_len = vlen;
        params[0].is_varint = true;
        params[0].raw       = NULL;
        params[0].raw_len   = 0;

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_server_setup(&w, params, 1) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_SERVER_SETUP);

        moq_kvp_entry_t dec_params[8];
        moq_d16_setup_t setup = {
            .params = dec_params,
            .params_count = 0,
            .params_cap = 8
        };
        MOQ_TEST_CHECK(moq_d16_decode_server_setup(env.payload, env.payload_len,
                                           &setup) == MOQ_OK);
        MOQ_TEST_CHECK(setup.params_count == 1);
        MOQ_TEST_CHECK(setup.params[0].type == 0x02);
        MOQ_TEST_CHECK(setup.params[0].is_varint == true);

        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_quic_varint_decode(setup.params[0].value,
                                              setup.params[0].value_len,
                                              &v) == setup.params[0].value_len);
        MOQ_TEST_CHECK(v == 100);
    }

    /* -- Zero params ----------------------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_client_setup(&w, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_kvp_entry_t dec_params[4];
        moq_d16_setup_t setup = {
            .params = dec_params,
            .params_count = 0,
            .params_cap = 4
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(env.payload, env.payload_len,
                                           &setup) == MOQ_OK);
        MOQ_TEST_CHECK(setup.params_count == 0);
    }

    /* -- Zero params can decode with no params array ---------------- */
    {
        uint8_t payload[] = { 0x00 }; /* count=0 */
        moq_d16_setup_t setup = {
            .params = NULL,
            .params_count = 99,
            .params_cap = 0
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(payload, sizeof(payload),
                                                   &setup) == MOQ_OK);
        MOQ_TEST_CHECK(setup.params_count == 0);
    }

    /* -- Count/entry mismatch: trailing entries are rejected --------- */
    {
        uint8_t payload[32];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 0) == MOQ_OK); /* claim zero params */

        uint8_t vbuf[1];
        MOQ_TEST_CHECK(moq_quic_varint_encode(7, vbuf, 1) == 1);
        moq_kvp_entry_t e = {
            .type = 0x02, .value = vbuf, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        size_t n = moq_kvp_encode_entry(0, &e, moq_buf_writer_ptr(&pw),
                                        moq_buf_writer_remaining(&pw));
        MOQ_TEST_CHECK(n > 0);
        pw.pos += n;

        moq_d16_setup_t setup = {
            .params = NULL,
            .params_count = 0,
            .params_cap = 0
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(payload,
                                                   moq_buf_writer_offset(&pw),
                                                   &setup) == MOQ_ERR_PROTO);
    }

    /* -- Unknown param preserved ----------------------------------- */
    {
        /* param type 0xFF (odd, unknown) with value "xyz" */
        moq_kvp_entry_t params[2];
        params[0].type      = 0xFF;
        params[0].value     = (const uint8_t *)"xyz";
        params[0].value_len = 3;
        params[0].is_varint = false;
        params[0].raw       = NULL;
        params[0].raw_len   = 0;

        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_client_setup(&w, params, 1) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_kvp_entry_t dec_params[4];
        moq_d16_setup_t setup = {
            .params = dec_params,
            .params_count = 0,
            .params_cap = 4
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(env.payload, env.payload_len,
                                           &setup) == MOQ_OK);
        MOQ_TEST_CHECK(setup.params_count == 1);
        MOQ_TEST_CHECK(setup.params[0].type == 0xFF);
        MOQ_TEST_CHECK(setup.params[0].value_len == 3);
        MOQ_TEST_CHECK(memcmp(setup.params[0].value, "xyz", 3) == 0);
        /* raw span is also set by KVP decoder. */
        MOQ_TEST_CHECK(setup.params[0].raw != NULL);
        MOQ_TEST_CHECK(setup.params[0].raw_len > 0);
    }

    /* -- Truncated setup payload ----------------------------------- */
    {
        uint8_t payload[] = { 0x02 }; /* count=2 but no entries */
        moq_kvp_entry_t dec_params[4];
        moq_d16_setup_t setup = {
            .params = dec_params,
            .params_count = 0,
            .params_cap = 4
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(payload, 1, &setup) == MOQ_ERR_PROTO);
    }

    /* -- Count/entry mismatch: count > actual entries --------------- */
    {
        /* Encode 1 param but claim count=3. */
        uint8_t payload[32];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 3) == MOQ_OK); /* claim 3 params */
        /* But only encode 1 KVP entry. */
        uint8_t vbuf[1];
        MOQ_TEST_CHECK(moq_quic_varint_encode(0, vbuf, 1) == 1);
        moq_kvp_entry_t e = {
            .type = 0x02, .value = vbuf, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        size_t n = moq_kvp_encode_entry(0, &e, moq_buf_writer_ptr(&pw),
                                        moq_buf_writer_remaining(&pw));
        MOQ_TEST_CHECK(n > 0);
        pw.pos += n;

        moq_kvp_entry_t dec_params[8];
        moq_d16_setup_t setup = {
            .params = dec_params,
            .params_count = 0,
            .params_cap = 8
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(payload, moq_buf_writer_offset(&pw),
                                           &setup) == MOQ_ERR_PROTO);
    }

    /* -- Params array too small ------------------------------------ */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t vbuf[1];
        MOQ_TEST_CHECK(moq_quic_varint_encode(1, vbuf, 1) == 1);
        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = 0x02, .value = vbuf, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        params[1] = (moq_kvp_entry_t){
            .type = 0x04, .value = vbuf, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        MOQ_TEST_CHECK(moq_d16_encode_client_setup(&w, params, 2) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_kvp_entry_t tiny[1]; /* cap=1 but 2 params */
        moq_d16_setup_t setup = {
            .params = tiny,
            .params_count = 0,
            .params_cap = 1
        };
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(env.payload, env.payload_len,
                                           &setup) == MOQ_ERR_BUFFER);
    }

    /* -- NULL args ------------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_control_decode_envelope(NULL, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_control_encode_envelope(NULL, 0, NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_control_write_header(NULL, 0, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_decode_client_setup(NULL, 5, NULL) == MOQ_ERR_INVAL);
    }

    /* == Simple varint messages ======================================= */

    /* -- UNSUBSCRIBE roundtrip -------------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, 42) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_UNSUBSCRIBE);

        uint64_t req_id = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len, &req_id) == MOQ_OK);
        MOQ_TEST_CHECK(req_id == 42);
    }

    /* -- MAX_REQUEST_ID roundtrip ----------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w, MOQ_D16_MAX_REQUEST_ID, 1000) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len, &v) == MOQ_OK);
        MOQ_TEST_CHECK(v == 1000);
    }

    /* -- FETCH_CANCEL, REQUESTS_BLOCKED, PUBLISH_NAMESPACE_DONE ----- */
    {
        uint64_t types[] = { MOQ_D16_FETCH_CANCEL, MOQ_D16_REQUESTS_BLOCKED,
                             MOQ_D16_PUBLISH_NAMESPACE_DONE };
        for (int t = 0; t < 3; t++) {
            uint8_t buf[32];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w, types[t], 99) == MOQ_OK);

            moq_buf_reader_t r;
            moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
            moq_control_envelope_t env;
            MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
            MOQ_TEST_CHECK(env.msg_type == types[t]);

            uint64_t v = 0;
            MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len, &v) == MOQ_OK);
            MOQ_TEST_CHECK(v == 99);
        }
    }

    /* -- Varint msg: trailing bytes rejected ------------------------- */
    {
        uint8_t payload[] = { 0x05, 0xFF }; /* varint 5 + trailing byte */
        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(payload, 2, &v) == MOQ_ERR_PROTO);
    }

    /* -- Varint msg: truncated -------------------------------------- */
    {
        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(NULL, 0, &v) == MOQ_ERR_PROTO);
    }

    /* -- Varint msg: boundary value (max varint) -------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w, MOQ_D16_MAX_REQUEST_ID,
                                                  MOQ_QUIC_VARINT_MAX) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len, &v) == MOQ_OK);
        MOQ_TEST_CHECK(v == MOQ_QUIC_VARINT_MAX);
    }

    /* -- Varint msg NULL args --------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(NULL, 0, 0) == MOQ_ERR_INVAL);
    }

    /* -- Varint msg rejects non-scalar message type ------------------ */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w, MOQ_D16_GOAWAY, 1) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* == GOAWAY ===================================================== */

    /* -- GOAWAY roundtrip with URI ---------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_goaway(&w, (const uint8_t *)"wss://new.example.com", 21) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_GOAWAY);

        moq_d16_goaway_t goaway;
        MOQ_TEST_CHECK(moq_d16_decode_goaway(env.payload, env.payload_len, &goaway) == MOQ_OK);
        MOQ_TEST_CHECK(goaway.uri_len == 21);
        MOQ_TEST_CHECK(memcmp(goaway.uri, "wss://new.example.com", 21) == 0);
    }

    /* -- GOAWAY with empty URI -------------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_goaway(&w, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_d16_goaway_t goaway;
        MOQ_TEST_CHECK(moq_d16_decode_goaway(env.payload, env.payload_len, &goaway) == MOQ_OK);
        MOQ_TEST_CHECK(goaway.uri == NULL);
        MOQ_TEST_CHECK(goaway.uri_len == 0);
    }

    /* -- GOAWAY URI > 8192 rejected --------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_goaway(&w, (const uint8_t *)"x", 8193) == MOQ_ERR_INVAL);
    }

    /* -- GOAWAY trailing bytes rejected ----------------------------- */
    {
        uint8_t payload[] = { 0x01, 0x41, 0xFF }; /* URI len=1, URI="A", trailing */
        moq_d16_goaway_t goaway;
        MOQ_TEST_CHECK(moq_d16_decode_goaway(payload, 3, &goaway) == MOQ_ERR_PROTO);
    }

    /* -- GOAWAY NULL args ------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_goaway(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_goaway(NULL, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* == PUBLISH_NAMESPACE_CANCEL ==================================== */

    /* -- Roundtrip -------------------------------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace_cancel(
            &w, 7, 0x20, (const uint8_t *)"not interested", 14) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH_NAMESPACE_CANCEL);

        moq_d16_publish_namespace_cancel_t cancel;
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace_cancel(
            env.payload, env.payload_len, &cancel) == MOQ_OK);
        MOQ_TEST_CHECK(cancel.request_id == 7);
        MOQ_TEST_CHECK(cancel.error_code == 0x20);
        MOQ_TEST_CHECK(cancel.reason_len == 14);
        MOQ_TEST_CHECK(memcmp(cancel.reason, "not interested", 14) == 0);
    }

    /* -- Empty reason ----------------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace_cancel(
            &w, 0, 0, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_d16_publish_namespace_cancel_t cancel;
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace_cancel(
            env.payload, env.payload_len, &cancel) == MOQ_OK);
        MOQ_TEST_CHECK(cancel.reason == NULL);
        MOQ_TEST_CHECK(cancel.reason_len == 0);
    }

    /* -- Reason > 1024 rejected ------------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace_cancel(
            &w, 0, 0, (const uint8_t *)"x", 1025) == MOQ_ERR_INVAL);
    }

    /* -- Truncated payload ------------------------------------------ */
    {
        uint8_t payload[] = { 0x07 }; /* request_id only, missing error_code + reason */
        moq_d16_publish_namespace_cancel_t cancel;
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace_cancel(
            payload, 1, &cancel) == MOQ_ERR_PROTO);
    }

    /* -- Trailing bytes rejected ------------------------------------- */
    {
        uint8_t payload[] = { 0x07, 0x20, 0x00, 0xFF };
        moq_d16_publish_namespace_cancel_t cancel;
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace_cancel(
            payload, sizeof(payload), &cancel) == MOQ_ERR_PROTO);
    }

    /* -- NULL args -------------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace_cancel(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace_cancel(NULL, 0, 0, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* == REQUEST_OK =================================================== */

    /* -- Zero-param roundtrip --------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, 10, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_OK);

        moq_kvp_entry_t dec_p[8];
        moq_d16_request_ok_t ok = { .params = dec_p, .params_cap = 8 };
        MOQ_TEST_CHECK(moq_d16_decode_request_ok(env.payload, env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.request_id == 10);
        MOQ_TEST_CHECK(ok.params_count == 0);
    }

        /* -- Multi-param roundtrip with raw preservation ---------------- */
    {
        uint8_t v1[1];
        MOQ_TEST_CHECK(moq_quic_varint_encode(42, v1, sizeof(v1)) == 1);
        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = 0x08, .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        params[1] = (moq_kvp_entry_t){
            .type = 0x09, .value = (const uint8_t *)"val", .value_len = 3,
            .is_varint = false, .raw = NULL, .raw_len = 0
        };

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, 5, params, 2) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_kvp_entry_t dec_p[8];
        moq_d16_request_ok_t ok = { .params = dec_p, .params_cap = 8 };
        MOQ_TEST_CHECK(moq_d16_decode_request_ok(env.payload, env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.request_id == 5);
        MOQ_TEST_CHECK(ok.params_count == 2);
        MOQ_TEST_CHECK(ok.params[0].type == 0x08);
        MOQ_TEST_CHECK(ok.params[0].raw != NULL);
        MOQ_TEST_CHECK(ok.params[0].raw_len > 0);
        MOQ_TEST_CHECK(ok.params[1].type == 0x09);
        MOQ_TEST_CHECK(ok.params[1].value_len == 3);
        MOQ_TEST_CHECK(memcmp(ok.params[1].value, "val", 3) == 0);
    }

    /* -- Params array too small ------------------------------------- */
    {
        uint8_t v1[1];
        MOQ_TEST_CHECK(moq_quic_varint_encode(1, v1, sizeof(v1)) == 1);
        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = 0x02, .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };
        params[1] = (moq_kvp_entry_t){
            .type = 0x04, .value = v1, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };

        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, 0, params, 2) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_kvp_entry_t tiny[1];
        moq_d16_request_ok_t ok = { .params = tiny, .params_cap = 1 };
        MOQ_TEST_CHECK(moq_d16_decode_request_ok(env.payload, env.payload_len, &ok) == MOQ_ERR_BUFFER);
    }

    /* -- Count mismatch: trailing extra params ---------------------- */
    {
        /* Encode 1 param but manually write count=0 in payload. */
        uint8_t payload[32];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 7) == MOQ_OK);  /* request_id */
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 0) == MOQ_OK);  /* count = 0 */
        /* But add trailing KVP bytes. */
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 0x02) == MOQ_OK); /* delta type */
        MOQ_TEST_CHECK(moq_buf_write_varint(&pw, 99) == MOQ_OK);   /* value */

        moq_kvp_entry_t dec_p[4];
        moq_d16_request_ok_t ok = { .params = dec_p, .params_cap = 4 };
        MOQ_TEST_CHECK(moq_d16_decode_request_ok(payload, moq_buf_writer_offset(&pw), &ok) == MOQ_ERR_PROTO);
    }

    /* -- REQUEST_OK truncated at request_id ------------------------- */
    {
        moq_kvp_entry_t dec_p[4];
        moq_d16_request_ok_t ok = { .params = dec_p, .params_cap = 4 };
        MOQ_TEST_CHECK(moq_d16_decode_request_ok(NULL, 0, &ok) == MOQ_ERR_PROTO);
    }

    /* -- REQUEST_OK NULL args --------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_request_ok(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_request_ok(NULL, 0, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* == REQUEST_ERROR ================================================ */

    /* -- Roundtrip with reason -------------------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_error(
            &w, 3, 0x10, 5000,
            (const uint8_t *)"track not found", 15) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_ERROR);

        moq_d16_request_error_t err;
        MOQ_TEST_CHECK(moq_d16_decode_request_error(env.payload, env.payload_len, &err) == MOQ_OK);
        MOQ_TEST_CHECK(err.request_id == 3);
        MOQ_TEST_CHECK(err.error_code == 0x10);
        MOQ_TEST_CHECK(err.retry_interval == 5000);
        MOQ_TEST_CHECK(err.reason_len == 15);
        MOQ_TEST_CHECK(memcmp(err.reason, "track not found", 15) == 0);
    }

    /* -- Empty reason ----------------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_error(&w, 0, 0x01, 0, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_d16_request_error_t err;
        MOQ_TEST_CHECK(moq_d16_decode_request_error(env.payload, env.payload_len, &err) == MOQ_OK);
        MOQ_TEST_CHECK(err.reason == NULL);
        MOQ_TEST_CHECK(err.reason_len == 0);
        MOQ_TEST_CHECK(err.retry_interval == 0);
    }

    /* -- Reason > 1024 rejected by encoder -------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_error(
            &w, 0, 0, 0, (const uint8_t *)"x", 1025) == MOQ_ERR_INVAL);
    }

    /* -- Truncation at each field ----------------------------------- */
    {
        moq_d16_request_error_t err;
        /* Empty payload — truncated at request_id. */
        MOQ_TEST_CHECK(moq_d16_decode_request_error(NULL, 0, &err) == MOQ_ERR_PROTO);

        /* request_id only — truncated at error_code. */
        uint8_t p1[] = { 0x03 };
        MOQ_TEST_CHECK(moq_d16_decode_request_error(p1, 1, &err) == MOQ_ERR_PROTO);

        /* request_id + error_code — truncated at retry_interval. */
        uint8_t p2[] = { 0x03, 0x10 };
        MOQ_TEST_CHECK(moq_d16_decode_request_error(p2, 2, &err) == MOQ_ERR_PROTO);

        /* request_id + error_code + retry_interval — truncated at reason. */
        uint8_t p3[] = { 0x03, 0x10, 0x00 };
        MOQ_TEST_CHECK(moq_d16_decode_request_error(p3, 3, &err) == MOQ_ERR_PROTO);
    }

    /* -- Trailing bytes rejected ------------------------------------ */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 1) == MOQ_OK);  /* request_id */
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_OK);  /* error_code */
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_OK);  /* retry_interval */
        MOQ_TEST_CHECK(moq_buf_write_varint(&w, 0) == MOQ_OK);  /* reason len */
        MOQ_TEST_CHECK(moq_buf_write_raw(&w, (const uint8_t *)"\xFF", 1) == MOQ_OK); /* trailing */

        moq_d16_request_error_t err;
        MOQ_TEST_CHECK(moq_d16_decode_request_error(
            buf, moq_buf_writer_offset(&w), &err) == MOQ_ERR_PROTO);
    }

    /* -- NULL args -------------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_request_error(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_request_error(NULL, 0, 0, 0, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- Encoder rollback on insufficient buffer -------------------- */
    {
        uint8_t buf[4]; /* too small for any complete message */
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_error(
            &w, 0, 0, 0, NULL, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);

        MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, 0, NULL, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* ============================================================== */
    /* SUBSCRIBE codec                                                */
    /* ============================================================== */

    /* -- SUBSCRIBE roundtrip with namespace + name + params ---------- */
    {
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("example.com"),
        };
        moq_namespace_t ns = { ns_parts, 2 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("video");

        uint8_t fwd_val[1];
        moq_quic_varint_encode(1, fwd_val, 1);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_val, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        moq_result_t rc = moq_d16_encode_subscribe(&w, 42, &ns, name,
                                                     params, 1);
        MOQ_TEST_CHECK(rc == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_SUBSCRIBE);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_params[4];
        moq_d16_subscribe_t sub = {
            .params = dec_params, .params_cap = 4,
        };
        rc = moq_d16_decode_subscribe(env.payload, env.payload_len,
                                       dec_parts, 8, &sub);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(sub.request_id == 42);
        MOQ_TEST_CHECK(sub.track_namespace.count == 2);
        MOQ_TEST_CHECK(sub.track_namespace.parts[0].len == 4);
        MOQ_TEST_CHECK(sub.track_namespace.parts[1].len == 11);
        MOQ_TEST_CHECK(sub.track_name.len == 5);
        MOQ_TEST_CHECK(sub.params_count == 1);
        MOQ_TEST_CHECK(sub.params[0].type == MOQ_MSG_PARAM_FORWARD);
    }

    /* -- SUBSCRIBE with empty track name (valid) --------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t empty_name = { NULL, 0 };

        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, 0, &ns, empty_name,
                                                 NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_bytes_t dec_parts[8];
        moq_d16_subscribe_t sub = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe(env.payload, env.payload_len,
            dec_parts, 8, &sub) == MOQ_OK);
        MOQ_TEST_CHECK(sub.track_name.len == 0);
    }

    /* -- SUBSCRIBE full track name > 4096 rejected at encode --------- */
    {
        uint8_t buf[8192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t big[4090];
        memset(big, 'x', sizeof(big));
        moq_bytes_t ns_parts[] = {{ big, sizeof(big) }};
        moq_namespace_t ns = { ns_parts, 1 };
        uint8_t nm[10];
        memset(nm, 'y', sizeof(nm));
        moq_bytes_t name = { nm, sizeof(nm) };

        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, 0, &ns, name,
            NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- SUBSCRIBE exactly 4096 bytes (valid) ------------------------ */
    {
        uint8_t buf[8192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t big[4000];
        memset(big, 'a', sizeof(big));
        moq_bytes_t ns_parts[] = {{ big, sizeof(big) }};
        moq_namespace_t ns = { ns_parts, 1 };
        uint8_t nm[96];
        memset(nm, 'b', sizeof(nm));
        moq_bytes_t name = { nm, sizeof(nm) };

        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, 0, &ns, name,
            NULL, 0) == MOQ_OK);
    }

    /* -- SUBSCRIBE full track name > 4096 rejected at decode --------- */
    {
        /* Build a valid SUBSCRIBE payload with ns(4090) + name(10) = 4100 */
        uint8_t payload[8192];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));

        moq_buf_write_varint(&pw, 0); /* request_id */
        /* namespace: 1 field of 4090 bytes */
        moq_buf_write_varint(&pw, 1); /* count */
        moq_buf_write_varint(&pw, 4090); /* field length */
        uint8_t big[4090];
        memset(big, 'x', sizeof(big));
        moq_buf_write_raw(&pw, big, sizeof(big));
        /* track name: 10 bytes */
        moq_buf_write_varint(&pw, 10);
        uint8_t nm[10];
        memset(nm, 'y', sizeof(nm));
        moq_buf_write_raw(&pw, nm, sizeof(nm));
        moq_buf_write_varint(&pw, 0); /* 0 params */

        moq_bytes_t dp[8];
        moq_d16_subscribe_t sub = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe(payload,
            moq_buf_writer_offset(&pw), dp, 8, &sub) == MOQ_ERR_PROTO);
    }

    /* -- SUBSCRIBE truncated payload --------------------------------- */
    {
        uint8_t trunc[] = { 0x03, 0x00, 0x02, 0x2A };
        moq_bytes_t dp[8];
        moq_d16_subscribe_t sub = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe(trunc, sizeof(trunc),
            dp, 8, &sub) == MOQ_ERR_PROTO);
    }

    /* ============================================================== */
    /* SUBSCRIBE_OK codec                                              */
    /* ============================================================== */

    /* -- SUBSCRIBE_OK roundtrip with params + track extensions ------- */
    {
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t loc_buf[16];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, loc_buf, sizeof(loc_buf));
        moq_buf_write_varint(&lw, 5);
        moq_buf_write_varint(&lw, 10);

        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc_buf, .value_len = moq_buf_writer_offset(&lw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};

        const uint8_t ext[] = { 0xAA, 0xBB, 0xCC, 0xDD };

        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&w, 42, 99,
            params, 1, ext, sizeof(ext)) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_SUBSCRIBE_OK);

        moq_kvp_entry_t dec_params[4];
        moq_d16_subscribe_ok_t ok = {
            .params = dec_params, .params_cap = 4,
        };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe_ok(env.payload,
            env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.request_id == 42);
        MOQ_TEST_CHECK(ok.track_alias == 99);
        MOQ_TEST_CHECK(ok.params_count == 1);
        MOQ_TEST_CHECK(ok.params[0].type == MOQ_MSG_PARAM_LARGEST_OBJECT);
        MOQ_TEST_CHECK(ok.track_extensions_len == 4);
        MOQ_TEST_CHECK(ok.track_extensions != NULL);
        MOQ_TEST_CHECK(memcmp(ok.track_extensions, ext, 4) == 0);
    }

    /* -- SUBSCRIBE_OK with no params and no extensions --------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&w, 0, 1,
            NULL, 0, NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_d16_subscribe_ok_t ok = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe_ok(env.payload,
            env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.request_id == 0);
        MOQ_TEST_CHECK(ok.track_alias == 1);
        MOQ_TEST_CHECK(ok.params_count == 0);
        MOQ_TEST_CHECK(ok.track_extensions_len == 0);
    }

    /* -- SUBSCRIBE_OK truncated payload ------------------------------ */
    {
        uint8_t trunc[] = { 0x2A };
        moq_d16_subscribe_ok_t ok = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe_ok(trunc,
            sizeof(trunc), &ok) == MOQ_ERR_PROTO);
    }

    /* -- REQUESTS_BLOCKED roundtrip via varint_msg ------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w,
            MOQ_D16_REQUESTS_BLOCKED, 42) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUESTS_BLOCKED);

        uint64_t val = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload,
            env.payload_len, &val) == MOQ_OK);
        MOQ_TEST_CHECK(val == 42);
    }

    /* -- SUBSCRIBE_OK with valid KVP track extension ------------------- */
    {
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        /* Build a syntactically valid KVP entry as track extension bytes.
         * Type 0x101 is odd → is_varint=false (length-prefixed). */
        uint8_t ext_buf[32];
        moq_kvp_entry_t ext_entry = {
            .type = 0x101, .value = (const uint8_t *)"hello", .value_len = 5,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        };
        size_t ext_len = moq_kvp_encode_entry(0, &ext_entry, ext_buf,
                                               sizeof(ext_buf));
        MOQ_TEST_CHECK(ext_len > 0);

        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&w, 7, 3,
            NULL, 0, ext_buf, ext_len) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_d16_subscribe_ok_t ok = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_subscribe_ok(env.payload,
            env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.track_extensions_len == ext_len);
        MOQ_TEST_CHECK(memcmp(ok.track_extensions, ext_buf, ext_len) == 0);
    }

    /* ============================================================== */
    /* Subscription filter parameter helpers                          */
    /* ============================================================== */

    /* -- All four filter types roundtrip ----------------------------- */
    {
        /* Next Group Start (0x1) */
        moq_d16_subscription_filter_t f = { .filter_type = 0x1 };
        uint8_t fb[32]; size_t flen = 0;
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(fb, sizeof(fb),
            &flen, &f) == MOQ_OK);
        moq_d16_subscription_filter_t fd;
        MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(fb, flen,
            &fd) == MOQ_OK);
        MOQ_TEST_CHECK(fd.filter_type == 0x1);

        /* Largest Object (0x2) */
        f.filter_type = 0x2;
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(fb, sizeof(fb),
            &flen, &f) == MOQ_OK);
        MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(fb, flen,
            &fd) == MOQ_OK);
        MOQ_TEST_CHECK(fd.filter_type == 0x2);

        /* AbsoluteStart (0x3) */
        f = (moq_d16_subscription_filter_t){
            .filter_type = 0x3, .start_group = 10, .start_object = 5 };
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(fb, sizeof(fb),
            &flen, &f) == MOQ_OK);
        MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(fb, flen,
            &fd) == MOQ_OK);
        MOQ_TEST_CHECK(fd.filter_type == 0x3);
        MOQ_TEST_CHECK(fd.start_group == 10);
        MOQ_TEST_CHECK(fd.start_object == 5);

        /* AbsoluteRange (0x4) */
        f = (moq_d16_subscription_filter_t){
            .filter_type = 0x4, .start_group = 3, .start_object = 0,
            .end_group = 7 };
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(fb, sizeof(fb),
            &flen, &f) == MOQ_OK);
        MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(fb, flen,
            &fd) == MOQ_OK);
        MOQ_TEST_CHECK(fd.filter_type == 0x4);
        MOQ_TEST_CHECK(fd.start_group == 3);
        MOQ_TEST_CHECK(fd.end_group == 7);
    }

    /* -- Unknown filter type rejected -------------------------------- */
    {
        uint8_t bad[] = { 0x05 };
        moq_d16_subscription_filter_t f;
        MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(bad,
            sizeof(bad), &f) == MOQ_ERR_PROTO);
    }

    /* -- AbsoluteRange end_group < start_group rejected at decode ---- */
    {
        /* Hand-build invalid wire bytes: filter_type=4, start={10,0}, end=5 */
        uint8_t fb[32];
        moq_buf_writer_t fw;
        moq_buf_writer_init(&fw, fb, sizeof(fb));
        moq_buf_write_varint(&fw, 0x4);
        moq_buf_write_varint(&fw, 10);
        moq_buf_write_varint(&fw, 0);
        moq_buf_write_varint(&fw, 5);
        moq_d16_subscription_filter_t fd;
        MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(fb,
            moq_buf_writer_offset(&fw), &fd) == MOQ_ERR_PROTO);
    }

    /* ============================================================== */
    /* Location / Largest Object decode                                */
    /* ============================================================== */
    {
        uint8_t lb[16];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, lb, sizeof(lb));
        moq_buf_write_varint(&lw, 42);
        moq_buf_write_varint(&lw, 99);

        moq_d16_location_t loc;
        MOQ_TEST_CHECK(moq_d16_decode_location(lb,
            moq_buf_writer_offset(&lw), &loc) == MOQ_OK);
        MOQ_TEST_CHECK(loc.group == 42);
        MOQ_TEST_CHECK(loc.object == 99);
    }

    /* ============================================================== */
    /* Simple varint parameter helpers                                */
    /* ============================================================== */

    /* -- FORWARD: 0 and 1 valid, 2 invalid --------------------------- */
    {
        uint8_t v0[] = { 0x00 };
        uint8_t v1[] = { 0x01 };
        uint8_t v2[] = { 0x02 };
        bool fwd;
        MOQ_TEST_CHECK(moq_d16_decode_param_forward(v0, 1, &fwd) == MOQ_OK);
        MOQ_TEST_CHECK(fwd == false);
        MOQ_TEST_CHECK(moq_d16_decode_param_forward(v1, 1, &fwd) == MOQ_OK);
        MOQ_TEST_CHECK(fwd == true);
        MOQ_TEST_CHECK(moq_d16_decode_param_forward(v2, 1, &fwd) == MOQ_ERR_PROTO);
        /* NULL out: reject, do not dereference. */
        MOQ_TEST_CHECK(moq_d16_decode_param_forward(v1, 1, NULL) == MOQ_ERR_INVAL);
    }

    /* -- SUBSCRIBER_PRIORITY: 0 and 255 valid, 256 invalid ----------- */
    {
        uint8_t v0[] = { 0x00 };
        uint8_t v255[] = { 0x40, 0xFF }; /* 2-byte varint for 255 */
        uint8_t v256[] = { 0x41, 0x00 }; /* 2-byte varint for 256 */
        uint8_t pri;
        MOQ_TEST_CHECK(moq_d16_decode_param_subscriber_priority(v0, 1,
            &pri) == MOQ_OK);
        MOQ_TEST_CHECK(pri == 0);
        MOQ_TEST_CHECK(moq_d16_decode_param_subscriber_priority(v255, 2,
            &pri) == MOQ_OK);
        MOQ_TEST_CHECK(pri == 255);
        MOQ_TEST_CHECK(moq_d16_decode_param_subscriber_priority(v256, 2,
            &pri) == MOQ_ERR_PROTO);
        /* NULL out: reject, do not dereference. */
        MOQ_TEST_CHECK(moq_d16_decode_param_subscriber_priority(v0, 1,
            NULL) == MOQ_ERR_INVAL);
    }

    /* -- GROUP_ORDER: 1 and 2 valid, 0 and 3 invalid ----------------- */
    {
        uint8_t v0[] = { 0x00 };
        uint8_t v1[] = { 0x01 };
        uint8_t v2[] = { 0x02 };
        uint8_t v3[] = { 0x03 };
        uint8_t go;
        MOQ_TEST_CHECK(moq_d16_decode_param_group_order(v0, 1,
            &go) == MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(moq_d16_decode_param_group_order(v1, 1,
            &go) == MOQ_OK);
        MOQ_TEST_CHECK(go == 1);
        MOQ_TEST_CHECK(moq_d16_decode_param_group_order(v2, 1,
            &go) == MOQ_OK);
        MOQ_TEST_CHECK(go == 2);
        MOQ_TEST_CHECK(moq_d16_decode_param_group_order(v3, 1,
            &go) == MOQ_ERR_PROTO);
        /* NULL out: reject, do not dereference. */
        MOQ_TEST_CHECK(moq_d16_decode_param_group_order(v1, 1,
            NULL) == MOQ_ERR_INVAL);
    }

    /* -- EXPIRES: any varint is valid -------------------------------- */
    {
        uint8_t v[] = { 0x40, 0x64 }; /* 100 */
        uint64_t exp;
        MOQ_TEST_CHECK(moq_d16_decode_param_expires(v, 2, &exp) == MOQ_OK);
        MOQ_TEST_CHECK(exp == 100);
    }

    /* -- encode SUBSCRIBE with count=33 rejects ---------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t small_parts[2] = {
            MOQ_BYTES_LITERAL("a"), MOQ_BYTES_LITERAL("b") };
        moq_namespace_t ns = { small_parts, 33 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, 0, &ns, name,
            NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- encode AbsoluteRange end_group < start_group rejects -------- */
    {
        uint8_t fb[32]; size_t flen = 0;
        moq_d16_subscription_filter_t f = {
            .filter_type = 0x4, .start_group = 10, .start_object = 0,
            .end_group = 5 };
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(fb, sizeof(fb),
            &flen, &f) == MOQ_ERR_INVAL);
    }

    /* -- Encoder rollback SUBSCRIBE/SUBSCRIBE_OK --------------------- */
    {
        uint8_t buf[4];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("y");

        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, 0, &ns, name,
            NULL, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);

        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&w, 0, 0,
            NULL, 0, NULL, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* ============================================================== */
    /* Subgroup header codec                                           */
    /* ============================================================== */

    /* -- Subgroup header type 0x14 roundtrip with max varints --------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_subgroup_header_t hdr = {
            .type = 0x14,
            .has_extensions = false,
            .subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT,
            .end_of_group = false,
            .default_priority = false,
            .track_alias = MOQ_QUIC_VARINT_MAX,
            .group_id = MOQ_QUIC_VARINT_MAX,
            .subgroup_id = MOQ_QUIC_VARINT_MAX,
            .publisher_priority = 255,
        };

        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &hdr) == MOQ_OK);
        size_t encoded = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(encoded <= 32);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, encoded);
        moq_d16_subgroup_header_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_subgroup_header(&r, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.type == 0x14);
        MOQ_TEST_CHECK(dec.subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT);
        MOQ_TEST_CHECK(dec.track_alias == MOQ_QUIC_VARINT_MAX);
        MOQ_TEST_CHECK(dec.group_id == MOQ_QUIC_VARINT_MAX);
        MOQ_TEST_CHECK(dec.subgroup_id == MOQ_QUIC_VARINT_MAX);
        MOQ_TEST_CHECK(dec.publisher_priority == 255);
        MOQ_TEST_CHECK(!dec.has_extensions);
        MOQ_TEST_CHECK(!dec.end_of_group);
        MOQ_TEST_CHECK(!dec.default_priority);
    }

    /* -- Decode all valid subgroup type variants ----------------------- */
    {
        uint8_t valid_types[] = {
            0x10,0x11,0x12,0x13,0x14,0x15,
            0x18,0x19,0x1A,0x1B,0x1C,0x1D,
            0x30,0x31,0x32,0x33,0x34,0x35,
            0x38,0x39,0x3A,0x3B,0x3C,0x3D,
        };
        for (size_t i = 0; i < sizeof(valid_types); i++) {
            MOQ_TEST_CHECK(moq_d16_subgroup_type_valid(valid_types[i]));
        }
    }

    /* -- Reject invalid subgroup type variants ------------------------ */
    {
        uint8_t invalid_types[] = {
            0x16,0x17,0x1E,0x1F,
            0x36,0x37,0x3E,0x3F,
            0x00,0x01,0x0F,0x40,0x50,0xFF,
        };
        for (size_t i = 0; i < sizeof(invalid_types); i++) {
            MOQ_TEST_CHECK(!moq_d16_subgroup_type_valid(invalid_types[i]));
        }
    }

    /* -- Subgroup header with default priority (type 0x30) ------------ */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_subgroup_header_t hdr = {
            .type = 0x30,
            .subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO,
            .default_priority = true,
            .track_alias = 1,
            .group_id = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &hdr) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_subgroup_header_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_subgroup_header(&r, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.default_priority == true);
        MOQ_TEST_CHECK(dec.subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_ZERO);
    }

    /* -- Subgroup header truncated ------------------------------------ */
    {
        uint8_t trunc[] = { 0x14, 0x01 };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, trunc, sizeof(trunc));
        moq_d16_subgroup_header_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_subgroup_header(&r, &dec) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(r.pos == 0);
    }

    /* -- Subgroup header encoder rollback on small buffer ------------- */
    {
        uint8_t buf[2];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_subgroup_header_t hdr = {
            .type = 0x14, .subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT,
            .track_alias = 1, .group_id = 2, .subgroup_id = 3,
        };
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &hdr) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* ============================================================== */
    /* Object fields codec                                             */
    /* ============================================================== */

    /* -- Object fields roundtrip with payload ------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        const uint8_t payload[] = { 0xDE, 0xAD, 0xBE, 0xEF };
        MOQ_TEST_CHECK(moq_d16_encode_object_fields(&w, 0, sizeof(payload),
            payload) == MOQ_OK);
        size_t encoded = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(encoded <= 32);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, encoded);
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, false, &obj) == MOQ_OK);
        MOQ_TEST_CHECK(obj.object_id_delta == 0);
        MOQ_TEST_CHECK(obj.payload_len == 4);
        MOQ_TEST_CHECK(!obj.has_status);
        MOQ_TEST_CHECK(obj.payload != NULL);
        MOQ_TEST_CHECK(memcmp(obj.payload, payload, 4) == 0);
    }

    /* -- Zero-length Normal object with explicit status 0x0 ----------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_object_fields(&w, 5, 0, NULL) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, false, &obj) == MOQ_OK);
        MOQ_TEST_CHECK(obj.object_id_delta == 5);
        MOQ_TEST_CHECK(obj.payload_len == 0);
        MOQ_TEST_CHECK(obj.has_status);
        MOQ_TEST_CHECK(obj.status == MOQ_OBJECT_STATUS_NORMAL);
        MOQ_TEST_CHECK(obj.payload == NULL);
    }

    /* -- Decode rejects unknown status -------------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 0);   /* delta */
        moq_buf_write_varint(&w, 0);   /* payload_len=0 */
        moq_buf_write_varint(&w, 0x7); /* invalid status */

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, false, &obj) == MOQ_ERR_PROTO);
    }

    /* -- Object fields max-varint IDs fit in 32 bytes ----------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_object_fields(&w, MOQ_QUIC_VARINT_MAX,
            0, NULL) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) <= 32);
    }

    /* -- Object fields truncated -------------------------------------- */
    {
        uint8_t buf[] = { 0x00 };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, sizeof(buf));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, false, &obj) == MOQ_ERR_BUFFER);
    }

    /* -- Object fields encoder rollback on small buffer --------------- */
    {
        uint8_t buf[1];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        const uint8_t payload[] = { 0xAA, 0xBB };
        MOQ_TEST_CHECK(moq_d16_encode_object_fields(&w, 0, sizeof(payload),
            payload) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- Object header-only max delta + max payload_len fits 32 ------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_object_header(&w, MOQ_QUIC_VARINT_MAX,
            MOQ_QUIC_VARINT_MAX) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) <= 32);
    }

    /* -- Object header-only zero-length encodes status 0x0 ------------ */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_object_header(&w, 0, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, false, &obj) == MOQ_OK);
        MOQ_TEST_CHECK(obj.has_status && obj.status == MOQ_OBJECT_STATUS_NORMAL);
    }

    /* -- Object header-only writer rollback on small buffer ----------- */
    {
        uint8_t buf[1];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_object_header(&w, 1000, 2000) ==
                        MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- Subgroup encode: type/field mismatch rejected ---------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;

        /* type 0x10 (mode=ZERO) but subgroup_id_mode=PRESENT → INVAL */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_subgroup_header_t bad1 = {
            .type = 0x10,
            .subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT,
            .track_alias = 1, .group_id = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &bad1) == MOQ_ERR_INVAL);

        /* type 0x30 (default_priority=1) but default_priority=false → INVAL */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_subgroup_header_t bad2 = {
            .type = 0x30,
            .subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO,
            .default_priority = false,
            .track_alias = 1, .group_id = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &bad2) == MOQ_ERR_INVAL);

        /* type 0x11 (extensions=1) but has_extensions=false → INVAL */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_subgroup_header_t bad3 = {
            .type = 0x11,
            .has_extensions = false,
            .subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO,
            .track_alias = 1, .group_id = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &bad3) == MOQ_ERR_INVAL);
    }

    /* -- Extension bytes preserved in object decode ------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        /* Write: delta=0, extension_len=3, ext_bytes, payload_len=2, payload */
        moq_buf_write_varint(&w, 0);           /* delta */
        moq_buf_write_varint(&w, 3);           /* extension headers length */
        uint8_t ext[] = {0xAA, 0xBB, 0xCC};
        moq_buf_write_raw(&w, ext, 3);
        moq_buf_write_varint(&w, 2);           /* payload_len */
        uint8_t pl[] = {0xDE, 0xAD};
        moq_buf_write_raw(&w, pl, 2);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, true, &obj) == MOQ_OK);
        MOQ_TEST_CHECK(obj.extensions_len == 3);
        MOQ_TEST_CHECK(obj.extensions != NULL);
        MOQ_TEST_CHECK(memcmp(obj.extensions, ext, 3) == 0);
        MOQ_TEST_CHECK(obj.payload_len == 2);
        MOQ_TEST_CHECK(memcmp(obj.payload, pl, 2) == 0);
    }

    /* -- Extensions on non-Normal status → PROTO, rollback ------------ */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_buf_write_varint(&w, 0);           /* delta */
        moq_buf_write_varint(&w, 3);           /* extension headers length */
        uint8_t ext[] = {0xAA, 0xBB, 0xCC};
        moq_buf_write_raw(&w, ext, 3);
        moq_buf_write_varint(&w, 0);           /* payload_len = 0 */
        moq_buf_write_varint(&w, MOQ_OBJECT_STATUS_END_OF_GROUP);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, true, &obj) ==
                        MOQ_ERR_PROTO);
        MOQ_TEST_CHECK(r.pos == 0);
    }

    /* -- Zero-length extensions on END_OF_GROUP accepted --------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_buf_write_varint(&w, 0);           /* delta */
        moq_buf_write_varint(&w, 0);           /* extension headers length = 0 */
        moq_buf_write_varint(&w, 0);           /* payload_len = 0 */
        moq_buf_write_varint(&w, MOQ_OBJECT_STATUS_END_OF_GROUP);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_object_fields_t obj;
        MOQ_TEST_CHECK(moq_d16_decode_object_fields(&r, true, &obj) == MOQ_OK);
        MOQ_TEST_CHECK(obj.status == MOQ_OBJECT_STATUS_END_OF_GROUP);
        MOQ_TEST_CHECK(obj.extensions_len == 0);
    }

    /* ============================================================== */
    /* PUBLISH_NAMESPACE codec                                        */
    /* ============================================================== */

    /* -- Roundtrip with zero params ----------------------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("example.com"),
        };
        moq_namespace_t ns = { ns_parts, 2 };

        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 5, &ns,
                                                         NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH_NAMESPACE);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_params[4];
        moq_d16_publish_namespace_t pn = {
            .params = dec_params, .params_cap = 4,
        };
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(env.payload,
            env.payload_len, dec_parts, 8, &pn) == MOQ_OK);
        MOQ_TEST_CHECK(pn.request_id == 5);
        MOQ_TEST_CHECK(pn.track_namespace.count == 2);
        MOQ_TEST_CHECK(pn.track_namespace.parts[0].len == 4);
        MOQ_TEST_CHECK(memcmp(pn.track_namespace.parts[0].data, "live", 4) == 0);
        MOQ_TEST_CHECK(pn.track_namespace.parts[1].len == 11);
        MOQ_TEST_CHECK(pn.params_count == 0);
    }

    /* -- Roundtrip with AUTH_TOKEN param ------------------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = (const uint8_t *)"tok123", .value_len = 6,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};

        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 0, &ns,
                                                         params, 1) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_params[4];
        moq_d16_publish_namespace_t pn = {
            .params = dec_params, .params_cap = 4,
        };
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(env.payload,
            env.payload_len, dec_parts, 8, &pn) == MOQ_OK);
        MOQ_TEST_CHECK(pn.params_count == 1);
        MOQ_TEST_CHECK(pn.params[0].type == MOQ_MSG_PARAM_AUTHORIZATION_TOKEN);
        MOQ_TEST_CHECK(pn.params[0].value_len == 6);
    }

    /* -- Namespace limit: 32 fields ----------------------------------- */
    {
        uint8_t buf[4096];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[32];
        for (int i = 0; i < 32; i++)
            ns_parts[i] = MOQ_BYTES_LITERAL("x");
        moq_namespace_t ns = { ns_parts, 32 };

        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 0, &ns,
                                                         NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_bytes_t dec_parts[32];
        moq_d16_publish_namespace_t pn = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(env.payload,
            env.payload_len, dec_parts, 32, &pn) == MOQ_OK);
        MOQ_TEST_CHECK(pn.track_namespace.count == 32);
    }

    /* -- Truncation returns ERR_BUFFER -------------------------------- */
    {
        uint8_t buf[4];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 0, &ns,
            NULL, 0) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- Params array too small returns ERR_BUFFER --------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_kvp_entry_t params[2];
        params[0] = (moq_kvp_entry_t){
            .type = 0x03, .value = (const uint8_t *)"a", .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0
        };
        params[1] = (moq_kvp_entry_t){
            .type = 0x03, .value = (const uint8_t *)"b", .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0
        };

        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 0, &ns,
            params, 2) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t tiny[1];
        moq_d16_publish_namespace_t pn = {
            .params = tiny, .params_cap = 1,
        };
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(env.payload,
            env.payload_len, dec_parts, 8, &pn) == MOQ_ERR_BUFFER);
    }

    /* -- Empty namespace (0 fields) rejected at encode ----------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_namespace_t ns = { NULL, 0 };
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&w, 0, &ns,
            NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- Empty namespace (0 fields) rejected at decode (ERR_PROTO) ----- */
    {
        /* Hand-build payload: request_id=0, namespace count=0, 0 params */
        uint8_t payload[16];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        moq_buf_write_varint(&pw, 0); /* request_id */
        moq_buf_write_varint(&pw, 0); /* namespace count */
        moq_buf_write_varint(&pw, 0); /* 0 params */

        moq_bytes_t dec_parts[8];
        moq_d16_publish_namespace_t pn = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(payload,
            moq_buf_writer_offset(&pw), dec_parts, 8, &pn) == MOQ_ERR_PROTO);
    }

    /* -- NULL args ----------------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(NULL, 5, NULL, 0, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(NULL, 0, NULL, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* ============================================================== */
    /* Auth token structure codec                                      */
    /* ============================================================== */

    /* -- REGISTER roundtrip ------------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 5,
            .token_type = 1,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        MOQ_TEST_CHECK(moq_d16_auth_token_encode(&w, &tok) == MOQ_OK);

        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(buf, moq_buf_writer_offset(&w), &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.alias_type == MOQ_AUTH_TOKEN_REGISTER);
        MOQ_TEST_CHECK(dec.alias == 5);
        MOQ_TEST_CHECK(dec.token_type == 1);
        MOQ_TEST_CHECK(dec.token_value_len == 6);
        MOQ_TEST_CHECK(memcmp(dec.token_value, "secret", 6) == 0);
    }

    /* -- USE_VALUE roundtrip ------------------------------------------ */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 42,
            .token_value = (const uint8_t *)"jwt",
            .token_value_len = 3,
        };
        MOQ_TEST_CHECK(moq_d16_auth_token_encode(&w, &tok) == MOQ_OK);

        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(buf, moq_buf_writer_offset(&w), &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.alias_type == MOQ_AUTH_TOKEN_USE_VALUE);
        MOQ_TEST_CHECK(dec.token_type == 42);
        MOQ_TEST_CHECK(dec.token_value_len == 3);
    }

    /* -- DELETE roundtrip --------------------------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 7,
        };
        MOQ_TEST_CHECK(moq_d16_auth_token_encode(&w, &tok) == MOQ_OK);

        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(buf, moq_buf_writer_offset(&w), &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.alias_type == MOQ_AUTH_TOKEN_DELETE);
        MOQ_TEST_CHECK(dec.alias == 7);
    }

    /* -- USE_ALIAS roundtrip ------------------------------------------ */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 99,
        };
        MOQ_TEST_CHECK(moq_d16_auth_token_encode(&w, &tok) == MOQ_OK);

        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(buf, moq_buf_writer_offset(&w), &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.alias_type == MOQ_AUTH_TOKEN_USE_ALIAS);
        MOQ_TEST_CHECK(dec.alias == 99);
    }

    /* -- Zero-length token value -------------------------------------- */
    {
        uint8_t buf[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 0, .token_type = 0,
            .token_value = NULL, .token_value_len = 0,
        };
        MOQ_TEST_CHECK(moq_d16_auth_token_encode(&w, &tok) == MOQ_OK);

        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(buf, moq_buf_writer_offset(&w), &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.alias_type == MOQ_AUTH_TOKEN_REGISTER);
        MOQ_TEST_CHECK(dec.token_value_len == 0);
    }

    /* -- Truncation → ERR_PROTO --------------------------------------- */
    {
        uint8_t truncated[] = { 0x01 };  /* REGISTER but no alias */
        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(truncated, 1, &dec) == MOQ_ERR_PROTO);
    }

    /* -- Unknown alias type → ERR_PROTO ------------------------------- */
    {
        uint8_t bad[] = { 0x04 };
        moq_d16_auth_token_t dec;
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(bad, 1, &dec) == MOQ_ERR_PROTO);
    }

    /* -- NULL args ----------------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_auth_token_decode(NULL, 0, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_auth_token_encode(NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* == REQUEST_UPDATE codec ========================================= */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t prio_buf[8];
        size_t prio_len = moq_quic_varint_encode(200, prio_buf, sizeof(prio_buf));
        moq_kvp_entry_t enc_params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
              .value = prio_buf, .value_len = prio_len, .is_varint = true },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(&w, 2, 0,
            enc_params, 1) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_UPDATE);

        moq_kvp_entry_t dec_params[8];
        moq_d16_request_update_t upd = {
            .params = dec_params, .params_cap = 8
        };
        MOQ_TEST_CHECK(moq_d16_decode_request_update(env.payload,
            env.payload_len, &upd) == MOQ_OK);
        MOQ_TEST_CHECK(upd.request_id == 2);
        MOQ_TEST_CHECK(upd.existing_request_id == 0);
        MOQ_TEST_CHECK(upd.params_count == 1);
        MOQ_TEST_CHECK(upd.params[0].type == MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
    }

    /* == REQUEST_UPDATE empty params =================================== */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_update(&w, 4, 0,
            NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_kvp_entry_t dec_params[8];
        moq_d16_request_update_t upd = {
            .params = dec_params, .params_cap = 8
        };
        MOQ_TEST_CHECK(moq_d16_decode_request_update(env.payload,
            env.payload_len, &upd) == MOQ_OK);
        MOQ_TEST_CHECK(upd.request_id == 4);
        MOQ_TEST_CHECK(upd.existing_request_id == 0);
        MOQ_TEST_CHECK(upd.params_count == 0);
    }

    /* == REQUEST_UPDATE oversized payload -> MOQ_ERR_BUFFER ============ */
    /* Params that encode to more than 0xFFFF payload bytes must be
     * rejected, not silently truncated by the 16-bit length patch. A
     * single byte param is capped at 0xFFFF, so two large ones are used
     * to overflow the envelope length. */
    {
        static uint8_t big_buf[200000];
        static uint8_t big_val[60000];
        memset(big_val, 0xAB, sizeof(big_val));
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, big_buf, sizeof(big_buf));

        moq_kvp_entry_t big_params[] = {
            { .type = 33, .value = big_val,
              .value_len = sizeof(big_val), .is_varint = false },
            { .type = 35, .value = big_val,
              .value_len = sizeof(big_val), .is_varint = false },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(&w, 7, 0,
            big_params, 2) == MOQ_ERR_BUFFER);
        /* Writer position rolled back; nothing committed. */
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* == REQUEST_UPDATE params_count > 0 with NULL params -> INVAL ===== */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_update(&w, 9, 0,
            NULL, 1) == MOQ_ERR_INVAL);
    }

    /* ============================================================== */
    /* Byte-boundary truncation tests                                 */
    /*                                                                */
    /* For each message type, encode a valid message, then test decode */
    /* at every byte boundary from 0 to len-1. Each truncated decode  */
    /* must fail (not crash, not succeed). Envelope truncation returns */
    /* MOQ_ERR_BUFFER; payload decoders may return MOQ_ERR_BUFFER or   */
    /* MOQ_ERR_PROTO. The full message decodes successfully.           */
    /* ============================================================== */

    /* -- Build valid messages ----------------------------------------- */

    /* 1. Control envelope */
    uint8_t trunc_env_buf[64];
    size_t  trunc_env_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_env_buf, sizeof(trunc_env_buf));
        uint8_t payload[] = { 0xAA, 0xBB, 0xCC };
        MOQ_TEST_CHECK(moq_control_encode_envelope(&tw, MOQ_D16_GOAWAY,
                                                    payload, 3) == MOQ_OK);
        trunc_env_len = moq_buf_writer_offset(&tw);
    }

    /* 2. CLIENT_SETUP (with AUTHORITY param) */
    uint8_t trunc_cs_buf[128];
    size_t  trunc_cs_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_cs_buf, sizeof(trunc_cs_buf));
        moq_kvp_entry_t cs_params[1];
        cs_params[0].type      = 0x05;
        cs_params[0].value     = (const uint8_t *)"example.com";
        cs_params[0].value_len = 11;
        cs_params[0].is_varint = false;
        cs_params[0].raw       = NULL;
        cs_params[0].raw_len   = 0;
        MOQ_TEST_CHECK(moq_d16_encode_client_setup(&tw, cs_params, 1) == MOQ_OK);
        trunc_cs_len = moq_buf_writer_offset(&tw);
    }

    /* 3. SERVER_SETUP (with MAX_REQUEST_ID param) */
    uint8_t trunc_ss_buf[128];
    size_t  trunc_ss_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_ss_buf, sizeof(trunc_ss_buf));
        uint8_t vbuf[8];
        size_t vlen = moq_quic_varint_encode(100, vbuf, sizeof(vbuf));
        moq_kvp_entry_t ss_params[1];
        ss_params[0].type      = 0x02;
        ss_params[0].value     = vbuf;
        ss_params[0].value_len = vlen;
        ss_params[0].is_varint = true;
        ss_params[0].raw       = NULL;
        ss_params[0].raw_len   = 0;
        MOQ_TEST_CHECK(moq_d16_encode_server_setup(&tw, ss_params, 1) == MOQ_OK);
        trunc_ss_len = moq_buf_writer_offset(&tw);
    }

    /* 4. SUBSCRIBE (with FORWARD param) */
    uint8_t trunc_sub_buf[512];
    size_t  trunc_sub_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_sub_buf, sizeof(trunc_sub_buf));
        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("example.com"),
        };
        moq_namespace_t ns = { ns_parts, 2 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("video");
        uint8_t fwd_val[1];
        moq_quic_varint_encode(1, fwd_val, 1);
        moq_kvp_entry_t sub_params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_val, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};
        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&tw, 42, &ns, name,
                                                 sub_params, 1) == MOQ_OK);
        trunc_sub_len = moq_buf_writer_offset(&tw);
    }

    /* 5. SUBSCRIBE_OK (with track_alias + LARGEST_OBJECT param) */
    uint8_t trunc_sok_buf[512];
    size_t  trunc_sok_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_sok_buf, sizeof(trunc_sok_buf));
        uint8_t loc_buf[16];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, loc_buf, sizeof(loc_buf));
        moq_buf_write_varint(&lw, 5);
        moq_buf_write_varint(&lw, 10);
        moq_kvp_entry_t sok_params[1] = {{
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc_buf, .value_len = moq_buf_writer_offset(&lw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_ok(&tw, 42, 99,
            sok_params, 1, NULL, 0) == MOQ_OK);
        trunc_sok_len = moq_buf_writer_offset(&tw);
    }

    /* 6. REQUEST_ERROR (with reason) */
    uint8_t trunc_re_buf[256];
    size_t  trunc_re_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_re_buf, sizeof(trunc_re_buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_error(
            &tw, 3, 0x10, 5000,
            (const uint8_t *)"track not found", 15) == MOQ_OK);
        trunc_re_len = moq_buf_writer_offset(&tw);
    }

    /* 7. PUBLISH_NAMESPACE (with namespace) */
    uint8_t trunc_pn_buf[256];
    size_t  trunc_pn_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_pn_buf, sizeof(trunc_pn_buf));
        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("example.com"),
        };
        moq_namespace_t ns = { ns_parts, 2 };
        MOQ_TEST_CHECK(moq_d16_encode_publish_namespace(&tw, 5, &ns,
                                                         NULL, 0) == MOQ_OK);
        trunc_pn_len = moq_buf_writer_offset(&tw);
    }

    /* 8. SUBSCRIBE_NAMESPACE (with prefix + subscribe_options) */
    uint8_t trunc_sn_buf[256];
    size_t  trunc_sn_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_sn_buf, sizeof(trunc_sn_buf));
        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
        };
        moq_namespace_t prefix = { ns_parts, 1 };
        MOQ_TEST_CHECK(moq_d16_encode_subscribe_namespace(&tw, 7, &prefix,
            1, NULL, 0) == MOQ_OK);
        trunc_sn_len = moq_buf_writer_offset(&tw);
    }

    /* -- Envelope-level truncation ------------------------------------ */
    /* Test decode_envelope at every byte boundary. */
    {
        for (size_t cut = 0; cut < trunc_env_len; cut++) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, trunc_env_buf, cut);
            moq_control_envelope_t env;
            moq_result_t rc = moq_control_decode_envelope(&r, &env);
            MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_BUFFER);
            MOQ_TEST_CHECK_EQ_SIZE(r.pos, 0);
        }
        /* Full length succeeds. */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, trunc_env_buf, trunc_env_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
    }

    /* -- CLIENT_SETUP payload truncation ------------------------------ */
    /* Payload-level decoders return MOQ_ERR_PROTO on truncation (they
     * operate on raw byte spans, not buf_reader). The important property
     * is: truncation always fails (rc < 0), never succeeds, never crashes. */
    {
        /* Strip envelope to get payload. */
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_cs_buf, trunc_cs_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_kvp_entry_t dec_p[8];
            moq_d16_setup_t setup = {
                .params = dec_p, .params_count = 0, .params_cap = 8
            };
            moq_result_t rc = moq_d16_decode_client_setup(env.payload, cut, &setup);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_kvp_entry_t dec_p[8];
        moq_d16_setup_t setup = {
            .params = dec_p, .params_count = 0, .params_cap = 8
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_client_setup(env.payload,
            env.payload_len, &setup), MOQ_OK);
    }

    /* -- SERVER_SETUP payload truncation ------------------------------ */
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_ss_buf, trunc_ss_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_kvp_entry_t dec_p[8];
            moq_d16_setup_t setup = {
                .params = dec_p, .params_count = 0, .params_cap = 8
            };
            moq_result_t rc = moq_d16_decode_server_setup(env.payload, cut, &setup);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_kvp_entry_t dec_p[8];
        moq_d16_setup_t setup = {
            .params = dec_p, .params_count = 0, .params_cap = 8
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_server_setup(env.payload,
            env.payload_len, &setup), MOQ_OK);
    }

    /* -- SUBSCRIBE payload truncation --------------------------------- */
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_sub_buf, trunc_sub_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_bytes_t dec_parts[8];
            moq_kvp_entry_t dec_p[4];
            moq_d16_subscribe_t sub = {
                .params = dec_p, .params_cap = 4,
            };
            moq_result_t rc = moq_d16_decode_subscribe(env.payload, cut,
                dec_parts, 8, &sub);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_p[4];
        moq_d16_subscribe_t sub = {
            .params = dec_p, .params_cap = 4,
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_subscribe(env.payload,
            env.payload_len, dec_parts, 8, &sub), MOQ_OK);
    }

    /* -- SUBSCRIBE_OK payload truncation ------------------------------ */
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_sok_buf, trunc_sok_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_kvp_entry_t dec_p[4];
            moq_d16_subscribe_ok_t ok = {
                .params = dec_p, .params_cap = 4,
            };
            moq_result_t rc = moq_d16_decode_subscribe_ok(env.payload, cut, &ok);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_kvp_entry_t dec_p[4];
        moq_d16_subscribe_ok_t ok = {
            .params = dec_p, .params_cap = 4,
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_subscribe_ok(env.payload,
            env.payload_len, &ok), MOQ_OK);
    }

    /* -- REQUEST_ERROR payload truncation ------------------------------ */
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_re_buf, trunc_re_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_d16_request_error_t err;
            moq_result_t rc = moq_d16_decode_request_error(env.payload, cut, &err);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_d16_request_error_t err;
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_request_error(env.payload,
            env.payload_len, &err), MOQ_OK);
    }

    /* -- PUBLISH_NAMESPACE payload truncation -------------------------- */
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_pn_buf, trunc_pn_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_bytes_t dec_parts[8];
            moq_kvp_entry_t dec_p[4];
            moq_d16_publish_namespace_t pn = {
                .params = dec_p, .params_cap = 4,
            };
            moq_result_t rc = moq_d16_decode_publish_namespace(env.payload, cut,
                dec_parts, 8, &pn);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_p[4];
        moq_d16_publish_namespace_t pn = {
            .params = dec_p, .params_cap = 4,
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_publish_namespace(env.payload,
            env.payload_len, dec_parts, 8, &pn), MOQ_OK);
    }

    /* -- SUBSCRIBE_NAMESPACE payload truncation ------------------------ */
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_sn_buf, trunc_sn_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_bytes_t dec_parts[8];
            moq_kvp_entry_t dec_p[4];
            moq_d16_subscribe_namespace_t sn = {
                .params = dec_p, .params_cap = 4,
            };
            moq_result_t rc = moq_d16_decode_subscribe_namespace(env.payload, cut,
                dec_parts, 8, &sn);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_p[4];
        moq_d16_subscribe_namespace_t sn = {
            .params = dec_p, .params_cap = 4,
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_subscribe_namespace(env.payload,
            env.payload_len, dec_parts, 8, &sn), MOQ_OK);
    }

    /* ============================================================== */
    /* FETCH codec                                                     */
    /* ============================================================== */

    /* -- 1. Standalone FETCH roundtrip -------------------------------- */
    {
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("stream"),
        };
        moq_namespace_t ns = { ns_parts, 2 };

        /* GROUP_ORDER param = 1 (ascending) */
        uint8_t go_buf[8];
        size_t go_len = moq_quic_varint_encode(1, go_buf, sizeof(go_buf));
        moq_kvp_entry_t enc_params[1] = {{
            .type = MOQ_MSG_PARAM_GROUP_ORDER,
            .value = go_buf, .value_len = go_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        moq_d16_fetch_t fetch = {
            .request_id = 7,
            .fetch_type = MOQ_D16_FETCH_TYPE_STANDALONE,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("video"),
            .start_group = 5, .start_object = 0,
            .end_group = 10, .end_object = 0,
        };

        MOQ_TEST_CHECK(moq_d16_encode_fetch(&w, &fetch, enc_params, 1) == MOQ_OK);
        size_t enc_len = moq_buf_writer_offset(&w);

        /* Decode envelope */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, enc_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_FETCH);

        /* Decode payload */
        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_params[4];
        moq_d16_fetch_t dec_fetch = {
            .params = dec_params, .params_cap = 4,
        };
        MOQ_TEST_CHECK(moq_d16_decode_fetch(env.payload, env.payload_len,
            dec_parts, 8, &dec_fetch) == MOQ_OK);
        MOQ_TEST_CHECK(dec_fetch.request_id == 7);
        MOQ_TEST_CHECK(dec_fetch.fetch_type == MOQ_D16_FETCH_TYPE_STANDALONE);
        MOQ_TEST_CHECK(dec_fetch.track_namespace.count == 2);
        MOQ_TEST_CHECK(dec_fetch.track_namespace.parts[0].len == 4);
        MOQ_TEST_CHECK(memcmp(dec_fetch.track_namespace.parts[0].data, "live", 4) == 0);
        MOQ_TEST_CHECK(dec_fetch.track_namespace.parts[1].len == 6);
        MOQ_TEST_CHECK(memcmp(dec_fetch.track_namespace.parts[1].data, "stream", 6) == 0);
        MOQ_TEST_CHECK(dec_fetch.track_name.len == 5);
        MOQ_TEST_CHECK(memcmp(dec_fetch.track_name.data, "video", 5) == 0);
        MOQ_TEST_CHECK(dec_fetch.start_group == 5);
        MOQ_TEST_CHECK(dec_fetch.start_object == 0);
        MOQ_TEST_CHECK(dec_fetch.end_group == 10);
        MOQ_TEST_CHECK(dec_fetch.end_object == 0);
        MOQ_TEST_CHECK(dec_fetch.params_count == 1);
        MOQ_TEST_CHECK(dec_fetch.params[0].type == MOQ_MSG_PARAM_GROUP_ORDER);

        /* Re-encode and compare bytes */
        uint8_t buf2[512];
        moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, buf2, sizeof(buf2));
        MOQ_TEST_CHECK(moq_d16_encode_fetch(&w2, &dec_fetch,
            dec_fetch.params, dec_fetch.params_count) == MOQ_OK);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w2) == enc_len);
        MOQ_TEST_CHECK(memcmp(buf, buf2, enc_len) == 0);
    }

    /* -- 2. Relative Joining FETCH roundtrip -------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_fetch_t fetch = {
            .request_id = 1,
            .fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN,
            .joining_request_id = 4,
            .joining_start = 3,
        };

        MOQ_TEST_CHECK(moq_d16_encode_fetch(&w, &fetch, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_FETCH);

        moq_d16_fetch_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_fetch(env.payload, env.payload_len,
            NULL, 0, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.fetch_type == MOQ_D16_FETCH_TYPE_RELATIVE_JOIN);
        MOQ_TEST_CHECK(dec.joining_request_id == 4);
        MOQ_TEST_CHECK(dec.joining_start == 3);
        MOQ_TEST_CHECK(dec.params_count == 0);
    }

    /* -- 3. Absolute Joining FETCH roundtrip -------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_fetch_t fetch = {
            .request_id = 2,
            .fetch_type = MOQ_D16_FETCH_TYPE_ABSOLUTE_JOIN,
            .joining_request_id = 4,
            .joining_start = 100,
        };

        MOQ_TEST_CHECK(moq_d16_encode_fetch(&w, &fetch, NULL, 0) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_d16_fetch_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_fetch(env.payload, env.payload_len,
            NULL, 0, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.fetch_type == MOQ_D16_FETCH_TYPE_ABSOLUTE_JOIN);
        MOQ_TEST_CHECK(dec.joining_request_id == 4);
        MOQ_TEST_CHECK(dec.joining_start == 100);
    }

    /* -- 4. Invalid fetch type ---------------------------------------- */
    {
        uint8_t payload[16];
        moq_buf_writer_t pw;
        moq_buf_writer_init(&pw, payload, sizeof(payload));
        moq_buf_write_varint(&pw, 0);  /* request_id */
        moq_buf_write_varint(&pw, 5);  /* invalid fetch_type */

        moq_d16_fetch_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_fetch(payload,
            moq_buf_writer_offset(&pw), NULL, 0, &dec) == MOQ_ERR_PROTO);
    }

    /* -- 5. FETCH_OK roundtrip ---------------------------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        const uint8_t ext[] = "ext";
        moq_d16_fetch_ok_t ok = {
            .request_id = 0,
            .end_of_track = 1,
            .end_group = 20,
            .end_object = 5,
            .params = NULL, .params_count = 0, .params_cap = 0,
            .track_extensions = ext,
            .track_extensions_len = 3,
        };

        MOQ_TEST_CHECK(moq_d16_encode_fetch_ok(&w, &ok) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_FETCH_OK);

        moq_d16_fetch_ok_t dec_ok = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_fetch_ok(env.payload,
            env.payload_len, &dec_ok) == MOQ_OK);
        MOQ_TEST_CHECK(dec_ok.request_id == 0);
        MOQ_TEST_CHECK(dec_ok.end_of_track == 1);
        MOQ_TEST_CHECK(dec_ok.end_group == 20);
        MOQ_TEST_CHECK(dec_ok.end_object == 5);
        MOQ_TEST_CHECK(dec_ok.params_count == 0);
        MOQ_TEST_CHECK(dec_ok.track_extensions_len == 3);
        MOQ_TEST_CHECK(memcmp(dec_ok.track_extensions, "ext", 3) == 0);
    }

    /* -- 6. FETCH_CANCEL via varint_msg ------------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_varint_msg(&w,
            MOQ_D16_FETCH_CANCEL, 99) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_FETCH_CANCEL);

        uint64_t val = 0;
        MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload,
            env.payload_len, &val) == MOQ_OK);
        MOQ_TEST_CHECK(val == 99);
    }

    /* -- 7. FETCH_HEADER roundtrip ------------------------------------ */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_fetch_header(&w, 42) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_fetch_header_t hdr;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_header(&r, &hdr) == MOQ_OK);
        MOQ_TEST_CHECK(hdr.request_id == 42);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);
    }

    /* -- 8. Fetch object — first object explicit ---------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_fetch_object_t obj = {
            .group_id = 1, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .payload = (const uint8_t *)"hello",
            .payload_len = 5,
        };

        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &obj, NULL) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_fetch_object_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, NULL, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(!dec.is_end_of_range);
        MOQ_TEST_CHECK(dec.group_id == 1);
        MOQ_TEST_CHECK(dec.subgroup_id == 0);
        MOQ_TEST_CHECK(dec.object_id == 0);
        MOQ_TEST_CHECK(dec.publisher_priority == 128);
        MOQ_TEST_CHECK(dec.payload_len == 5);
        MOQ_TEST_CHECK(memcmp(dec.payload, "hello", 5) == 0);
    }

    /* -- 9. Fetch object — delta cases -------------------------------- */
    {
        /* First object */
        moq_d16_fetch_object_t first = {
            .group_id = 1, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .payload = (const uint8_t *)"aa",
            .payload_len = 2,
        };

        /* Second object: group=1 (same), subgroup=0 (same), object=1 (prev+1),
         * priority=128 (same). Should produce minimal flags. */
        moq_d16_fetch_object_t second = {
            .group_id = 1, .subgroup_id = 0, .object_id = 1,
            .publisher_priority = 128,
            .payload = (const uint8_t *)"bb",
            .payload_len = 2,
        };

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &second, &first) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));

        /* Read the flags byte to verify minimal encoding */
        uint64_t flags_val = 0;
        moq_buf_reader_t r2;
        moq_buf_reader_init(&r2, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_buf_read_varint(&r2, &flags_val) == MOQ_OK);
        /* group_id same=0, object_id prev+1=0, priority same=0, subgroup zero=0 */
        MOQ_TEST_CHECK(flags_val == 0);

        moq_d16_fetch_object_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, &first, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.group_id == 1);
        MOQ_TEST_CHECK(dec.subgroup_id == 0);
        MOQ_TEST_CHECK(dec.object_id == 1);
        MOQ_TEST_CHECK(dec.publisher_priority == 128);
        MOQ_TEST_CHECK(dec.payload_len == 2);
        MOQ_TEST_CHECK(memcmp(dec.payload, "bb", 2) == 0);
    }

    /* -- 10. Fetch object — group change ------------------------------ */
    {
        moq_d16_fetch_object_t first = {
            .group_id = 1, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .payload = (const uint8_t *)"x",
            .payload_len = 1,
        };

        moq_d16_fetch_object_t changed = {
            .group_id = 2, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .payload = (const uint8_t *)"y",
            .payload_len = 1,
        };

        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &changed, &first) == MOQ_OK);

        /* Verify flags has GROUP_ID and OBJECT_ID set */
        moq_buf_reader_t r2;
        moq_buf_reader_init(&r2, buf, moq_buf_writer_offset(&w));
        uint64_t flags_val = 0;
        MOQ_TEST_CHECK(moq_buf_read_varint(&r2, &flags_val) == MOQ_OK);
        MOQ_TEST_CHECK((flags_val & MOQ_D16_FETCH_FLAG_GROUP_ID) != 0);
        /* object_id=0 which is not prev(0)+1, so OBJECT_ID flag should be set */
        MOQ_TEST_CHECK((flags_val & MOQ_D16_FETCH_FLAG_OBJECT_ID) != 0);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_fetch_object_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, &first, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.group_id == 2);
        MOQ_TEST_CHECK(dec.object_id == 0);
    }

    /* -- 11. End-of-range markers ------------------------------------- */
    {
        /* MOQ_D16_FETCH_END_NON_EXISTENT (0x8C) */
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_fetch_end_of_range(&w,
            MOQ_D16_FETCH_END_NON_EXISTENT, 5, 10) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d16_fetch_object_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, NULL, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.is_end_of_range);
        MOQ_TEST_CHECK(dec.range_kind == MOQ_D16_FETCH_END_NON_EXISTENT);
        MOQ_TEST_CHECK(dec.group_id == 5);
        MOQ_TEST_CHECK(dec.object_id == 10);

        /* MOQ_D16_FETCH_END_UNKNOWN (0x10C) */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_fetch_end_of_range(&w,
            MOQ_D16_FETCH_END_UNKNOWN, 100, 200) == MOQ_OK);

        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, NULL, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.is_end_of_range);
        MOQ_TEST_CHECK(dec.range_kind == MOQ_D16_FETCH_END_UNKNOWN);
        MOQ_TEST_CHECK(dec.group_id == 100);
        MOQ_TEST_CHECK(dec.object_id == 200);
    }

    /* -- 12. Fetch object — NULL pointer with nonzero length ------------ */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;

        /* NULL payload */
        moq_d16_fetch_object_t bad_payload = {
            .group_id = 1, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .payload = NULL,
            .payload_len = 10,
        };
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &bad_payload, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

        /* NULL extensions */
        moq_d16_fetch_object_t bad_ext = {
            .group_id = 1, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .extensions = NULL,
            .extensions_len = 5,
            .payload = (const uint8_t *)"x",
            .payload_len = 1,
        };
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &bad_ext, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* -- 13. End-of-range prior-object semantics ----------------------- */
    {
        /* Object → range marker → object: verify inherited subgroup/priority */
        moq_d16_fetch_object_t first = {
            .group_id = 1, .subgroup_id = 3, .object_id = 0,
            .publisher_priority = 200,
            .payload = (const uint8_t *)"a",
            .payload_len = 1,
        };

        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        /* Encode: first object, end-of-range, third object */
        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &first, NULL) == MOQ_OK);

        MOQ_TEST_CHECK(moq_d16_encode_fetch_end_of_range(&w,
            MOQ_D16_FETCH_END_NON_EXISTENT, 2, 5) == MOQ_OK);

        moq_d16_fetch_object_t third = {
            .group_id = 3, .subgroup_id = 3, .object_id = 0,
            .publisher_priority = 200,
            .payload = (const uint8_t *)"b",
            .payload_len = 1,
        };

        /* Decode sequence: first, range, third */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));

        moq_d16_fetch_object_t dec_first;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, NULL, &dec_first) == MOQ_OK);
        MOQ_TEST_CHECK(!dec_first.is_end_of_range);
        MOQ_TEST_CHECK(dec_first.subgroup_id == 3);
        MOQ_TEST_CHECK(dec_first.publisher_priority == 200);

        moq_d16_fetch_object_t dec_range;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, &dec_first, &dec_range) == MOQ_OK);
        MOQ_TEST_CHECK(dec_range.is_end_of_range);
        MOQ_TEST_CHECK(dec_range.group_id == 2);
        MOQ_TEST_CHECK(dec_range.object_id == 5);
        /* Inherited from first object */
        MOQ_TEST_CHECK(dec_range.subgroup_id == 3);
        MOQ_TEST_CHECK(dec_range.publisher_priority == 200);

        /* Encode the third object using dec_range as prev (naive loop) */
        size_t w_after_range = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&w, &third, &dec_range) == MOQ_OK);

        /* Decode the third object using dec_range as prev */
        moq_buf_reader_init(&r, buf + w_after_range,
                            moq_buf_writer_offset(&w) - w_after_range);
        moq_d16_fetch_object_t dec_third;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, &dec_range, &dec_third) == MOQ_OK);
        MOQ_TEST_CHECK(!dec_third.is_end_of_range);
        MOQ_TEST_CHECK(dec_third.group_id == 3);
        MOQ_TEST_CHECK(dec_third.subgroup_id == 3);
        MOQ_TEST_CHECK(dec_third.object_id == 0);
        MOQ_TEST_CHECK(dec_third.publisher_priority == 200);
        MOQ_TEST_CHECK(dec_third.payload_len == 1);
        MOQ_TEST_CHECK(memcmp(dec_third.payload, "b", 1) == 0);
    }

    /* -- 14. Byte-boundary truncation: FETCH -------------------------- */
    uint8_t trunc_fetch_buf[512];
    size_t  trunc_fetch_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_fetch_buf, sizeof(trunc_fetch_buf));

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("stream"),
        };
        moq_namespace_t ns = { ns_parts, 2 };

        moq_d16_fetch_t fetch = {
            .request_id = 1,
            .fetch_type = MOQ_D16_FETCH_TYPE_STANDALONE,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("audio"),
            .start_group = 0, .start_object = 0,
            .end_group = 5, .end_object = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_fetch(&tw, &fetch, NULL, 0) == MOQ_OK);
        trunc_fetch_len = moq_buf_writer_offset(&tw);
    }
    {
        /* Strip envelope to get payload. */
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_fetch_buf, trunc_fetch_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_bytes_t dec_parts[8];
            moq_d16_fetch_t dec = { .params = NULL, .params_cap = 0 };
            moq_result_t rc = moq_d16_decode_fetch(env.payload, cut,
                dec_parts, 8, &dec);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_bytes_t dec_parts[8];
        moq_d16_fetch_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_fetch(env.payload,
            env.payload_len, dec_parts, 8, &dec), MOQ_OK);
    }

    /* -- Byte-boundary truncation: FETCH_OK --------------------------- */
    uint8_t trunc_fok_buf[256];
    size_t  trunc_fok_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_fok_buf, sizeof(trunc_fok_buf));
        moq_d16_fetch_ok_t ok = {
            .request_id = 3, .end_of_track = 0,
            .end_group = 10, .end_object = 2,
            .params = NULL, .params_count = 0, .params_cap = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_fetch_ok(&tw, &ok) == MOQ_OK);
        trunc_fok_len = moq_buf_writer_offset(&tw);
    }
    {
        moq_buf_reader_t er;
        moq_buf_reader_init(&er, trunc_fok_buf, trunc_fok_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_d16_fetch_ok_t dec_ok = { .params = NULL, .params_cap = 0 };
            moq_result_t rc = moq_d16_decode_fetch_ok(env.payload, cut, &dec_ok);
            MOQ_TEST_CHECK(rc < 0);
        }
        moq_d16_fetch_ok_t dec_ok = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_fetch_ok(env.payload,
            env.payload_len, &dec_ok), MOQ_OK);
    }

    /* -- Byte-boundary truncation: FETCH_HEADER ----------------------- */
    uint8_t trunc_fhdr_buf[32];
    size_t  trunc_fhdr_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_fhdr_buf, sizeof(trunc_fhdr_buf));
        MOQ_TEST_CHECK(moq_d16_encode_fetch_header(&tw, 42) == MOQ_OK);
        trunc_fhdr_len = moq_buf_writer_offset(&tw);
    }
    {
        for (size_t cut = 0; cut < trunc_fhdr_len; cut++) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, trunc_fhdr_buf, cut);
            moq_d16_fetch_header_t hdr;
            moq_result_t rc = moq_d16_decode_fetch_header(&r, &hdr);
            MOQ_TEST_CHECK(rc < 0);
            MOQ_TEST_CHECK(r.pos == 0);
        }
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, trunc_fhdr_buf, trunc_fhdr_len);
        moq_d16_fetch_header_t hdr;
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_fetch_header(&r, &hdr), MOQ_OK);
    }

    /* -- Byte-boundary truncation: Fetch object ----------------------- */
    uint8_t trunc_fobj_buf[64];
    size_t  trunc_fobj_len;
    {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, trunc_fobj_buf, sizeof(trunc_fobj_buf));
        moq_d16_fetch_object_t obj = {
            .group_id = 1, .subgroup_id = 0, .object_id = 0,
            .publisher_priority = 128,
            .payload = (const uint8_t *)"data",
            .payload_len = 4,
        };
        MOQ_TEST_CHECK(moq_d16_encode_fetch_object(&tw, &obj, NULL) == MOQ_OK);
        trunc_fobj_len = moq_buf_writer_offset(&tw);
    }
    {
        for (size_t cut = 0; cut < trunc_fobj_len; cut++) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, trunc_fobj_buf, cut);
            moq_d16_fetch_object_t dec;
            moq_result_t rc = moq_d16_decode_fetch_object(&r, NULL, &dec);
            MOQ_TEST_CHECK(rc < 0);
        }
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, trunc_fobj_buf, trunc_fobj_len);
        moq_d16_fetch_object_t dec;
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_fetch_object(&r, NULL, &dec), MOQ_OK);
    }

    /* ============================================================== */
    /* PUBLISH codec                                                   */
    /* ============================================================== */

    /* -- PUBLISH roundtrip with namespace + name + track_alias + params -- */
    {
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("example.com"),
        };
        moq_namespace_t ns = { ns_parts, 2 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("video");

        uint8_t fwd_val[1];
        moq_quic_varint_encode(1, fwd_val, 1);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = fwd_val, .value_len = 1,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        moq_d16_publish_t pub = {
            .request_id = 7,
            .track_namespace = ns,
            .track_name = name,
            .track_alias = 42,
            .params = params,
            .params_count = 1,
            .params_cap = 1,
            .track_extensions = NULL,
            .track_extensions_len = 0,
        };

        MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_params[4];
        moq_d16_publish_t dec = {
            .params = dec_params, .params_cap = 4,
        };
        MOQ_TEST_CHECK(moq_d16_decode_publish(env.payload, env.payload_len,
                                               dec_parts, 8, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.request_id == 7);
        MOQ_TEST_CHECK(dec.track_namespace.count == 2);
        MOQ_TEST_CHECK(dec.track_namespace.parts[0].len == 4);
        MOQ_TEST_CHECK(memcmp(dec.track_namespace.parts[0].data, "live", 4) == 0);
        MOQ_TEST_CHECK(dec.track_namespace.parts[1].len == 11);
        MOQ_TEST_CHECK(dec.track_name.len == 5);
        MOQ_TEST_CHECK(memcmp(dec.track_name.data, "video", 5) == 0);
        MOQ_TEST_CHECK(dec.track_alias == 42);
        MOQ_TEST_CHECK(dec.params_count == 1);
        MOQ_TEST_CHECK(dec.params[0].type == MOQ_MSG_PARAM_FORWARD);
        MOQ_TEST_CHECK(dec.track_extensions_len == 0);
    }

    /* -- PUBLISH minimal (no params, no extensions) ------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_d16_publish_t pub = {
            .request_id = 0,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("t"),
            .track_alias = 0,
            .params = NULL,
            .params_count = 0,
            .params_cap = 0,
            .track_extensions = NULL,
            .track_extensions_len = 0,
        };

        MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH);

        moq_bytes_t dec_parts[8];
        moq_d16_publish_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_publish(env.payload, env.payload_len,
                                               dec_parts, 8, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.request_id == 0);
        MOQ_TEST_CHECK(dec.track_namespace.count == 1);
        MOQ_TEST_CHECK(dec.track_alias == 0);
        MOQ_TEST_CHECK(dec.params_count == 0);
        MOQ_TEST_CHECK(dec.track_extensions_len == 0);
    }

    /* -- PUBLISH with track extensions -------------------------------- */
    {
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };
        const uint8_t ext[] = { 0xAA, 0xBB, 0xCC, 0xDD };

        moq_d16_publish_t pub = {
            .request_id = 1,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("y"),
            .track_alias = 5,
            .params = NULL,
            .params_count = 0,
            .params_cap = 0,
            .track_extensions = ext,
            .track_extensions_len = sizeof(ext),
        };

        MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_decode_envelope(&r, &env);

        moq_bytes_t dec_parts[8];
        moq_d16_publish_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_publish(env.payload, env.payload_len,
                                               dec_parts, 8, &dec) == MOQ_OK);
        MOQ_TEST_CHECK(dec.track_extensions_len == 4);
        MOQ_TEST_CHECK(dec.track_extensions != NULL);
        MOQ_TEST_CHECK(memcmp(dec.track_extensions, ext, 4) == 0);
    }

    /* -- PUBLISH truncated payload ------------------------------------ */
    {
        uint8_t trunc[] = { 0x01 }; /* request_id only */
        moq_bytes_t dec_parts[8];
        moq_d16_publish_t dec = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_publish(trunc, 1, dec_parts, 8, &dec) == MOQ_ERR_PROTO);
    }

    /* -- PUBLISH encoder rollback on small buffer --------------------- */
    {
        uint8_t buf[4];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("x") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_d16_publish_t pub = {
            .request_id = 0,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("y"),
            .track_alias = 0,
        };
        MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) == 0);
    }

    /* -- PUBLISH NULL args -------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_publish(NULL, 5, NULL, 0, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_publish(NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- PUBLISH_OK roundtrip with params ----------------------------- */
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t pri_val[8];
        size_t pri_len = moq_quic_varint_encode(200, pri_val, sizeof(pri_val));
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = pri_val, .value_len = pri_len,
            .is_varint = true, .raw = NULL, .raw_len = 0,
        }};

        MOQ_TEST_CHECK(moq_d16_encode_publish_ok(&w, 3, params, 1) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH_OK);

        moq_kvp_entry_t dec_params[4];
        moq_d16_publish_ok_t ok = { .params = dec_params, .params_cap = 4 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_ok(env.payload, env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.request_id == 3);
        MOQ_TEST_CHECK(ok.params_count == 1);
        MOQ_TEST_CHECK(ok.params[0].type == MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
        uint8_t pri = 0;
        MOQ_TEST_CHECK(moq_d16_decode_param_subscriber_priority(ok.params[0].value,
            ok.params[0].value_len, &pri) == MOQ_OK);
        MOQ_TEST_CHECK(pri == 200);
    }

    /* -- PUBLISH_OK zero params --------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_publish_ok(&w, 0, NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_d16_publish_ok_t ok = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_ok(env.payload, env.payload_len, &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.request_id == 0);
        MOQ_TEST_CHECK(ok.params_count == 0);
    }

    /* -- PUBLISH_OK NULL args ----------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_publish_ok(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_publish_ok(NULL, 0, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- PUBLISH_DONE roundtrip with reason --------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w, 5, 0x01, 100,
            (const uint8_t *)"done", 4) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_PUBLISH_DONE);

        moq_d16_publish_done_t done;
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(env.payload, env.payload_len, &done) == MOQ_OK);
        MOQ_TEST_CHECK(done.request_id == 5);
        MOQ_TEST_CHECK(done.status_code == 0x01);
        MOQ_TEST_CHECK(done.stream_count == 100);
        MOQ_TEST_CHECK(done.reason_len == 4);
        MOQ_TEST_CHECK(memcmp(done.reason, "done", 4) == 0);
    }

    /* -- PUBLISH_DONE with empty reason ------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w, 0, 0, 0, NULL, 0) == MOQ_OK);

        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);

        moq_d16_publish_done_t done;
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(env.payload, env.payload_len, &done) == MOQ_OK);
        MOQ_TEST_CHECK(done.request_id == 0);
        MOQ_TEST_CHECK(done.status_code == 0);
        MOQ_TEST_CHECK(done.stream_count == 0);
        MOQ_TEST_CHECK(done.reason == NULL);
        MOQ_TEST_CHECK(done.reason_len == 0);
    }

    /* -- PUBLISH_DONE reason > 1024 rejected by encoder --------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w, 0, 0, 0,
            (const uint8_t *)"x", 1025) == MOQ_ERR_INVAL);
    }

    /* -- PUBLISH_DONE truncation at each field ------------------------ */
    {
        moq_d16_publish_done_t done;
        /* Empty payload */
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(NULL, 0, &done) == MOQ_ERR_PROTO);

        /* request_id only */
        uint8_t p1[] = { 0x05 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(p1, 1, &done) == MOQ_ERR_PROTO);

        /* request_id + status_code */
        uint8_t p2[] = { 0x05, 0x01 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(p2, 2, &done) == MOQ_ERR_PROTO);

        /* request_id + status_code + stream_count — truncated at reason */
        uint8_t p3[] = { 0x05, 0x01, 0x64 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(p3, 3, &done) == MOQ_ERR_PROTO);
    }

    /* -- PUBLISH_DONE trailing bytes rejected ------------------------- */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_varint(&w, 1); /* request_id */
        moq_buf_write_varint(&w, 0); /* status_code */
        moq_buf_write_varint(&w, 0); /* stream_count */
        moq_buf_write_varint(&w, 0); /* reason len */
        moq_buf_write_raw(&w, (const uint8_t *)"\xFF", 1); /* trailing */

        moq_d16_publish_done_t done;
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(buf, moq_buf_writer_offset(&w),
            &done) == MOQ_ERR_PROTO);
    }

    /* -- PUBLISH_DONE NULL args --------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_d16_decode_publish_done(NULL, 5, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_d16_encode_publish_done(NULL, 0, 0, 0, NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- PUBLISH encode: oversized namespace rejected, writer unchanged */
    {
        /* Build a namespace with total byte length > MOQ_FULL_TRACK_NAME_MAX. */
        uint8_t big[4100];
        memset(big, 'A', sizeof(big));
        moq_bytes_t big_part = { big, sizeof(big) };
        moq_namespace_t big_ns = { &big_part, 1 };

        moq_d16_publish_t pub;
        memset(&pub, 0, sizeof(pub));
        pub.request_id = 0;
        pub.track_namespace = big_ns;
        pub.track_name = MOQ_BYTES_LITERAL("t");
        pub.track_alias = 1;

        uint8_t buf[8192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* ============================================================== */
    /* TRACK_STATUS codec                                              */
    /* ============================================================== */

    /* -- Minimal encode/decode roundtrip ------------------------------ */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 0, &ns, name,
            NULL, 0) == MOQ_OK);
        size_t encoded_len = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(encoded_len > 0);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, encoded_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_TRACK_STATUS);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_p[4];
        moq_d16_track_status_t ts = {
            .params = dec_p, .params_cap = 4,
        };
        MOQ_TEST_CHECK(moq_d16_decode_track_status(env.payload, env.payload_len,
            dec_parts, 8, &ts) == MOQ_OK);
        MOQ_TEST_CHECK(ts.request_id == 0);
        MOQ_TEST_CHECK(ts.track_namespace.count == 1);
        MOQ_TEST_CHECK(ts.track_namespace.parts[0].len == 2);
        MOQ_TEST_CHECK(memcmp(ts.track_namespace.parts[0].data, "ns", 2) == 0);
        MOQ_TEST_CHECK(ts.track_name.len == 1);
        MOQ_TEST_CHECK(memcmp(ts.track_name.data, "t", 1) == 0);
        MOQ_TEST_CHECK(ts.params_count == 0);
    }

    /* -- Encode with AUTH_TOKEN param --------------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("video");

        uint8_t tok[] = { 0xCA, 0xFE };
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok, .value_len = 2,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 4, &ns, name,
            params, 1) == MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_TRACK_STATUS);

        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_p[4];
        moq_d16_track_status_t ts = { .params = dec_p, .params_cap = 4 };
        MOQ_TEST_CHECK(moq_d16_decode_track_status(env.payload, env.payload_len,
            dec_parts, 8, &ts) == MOQ_OK);
        MOQ_TEST_CHECK(ts.request_id == 4);
        MOQ_TEST_CHECK(ts.params_count == 1);
        MOQ_TEST_CHECK(ts.params[0].type == MOQ_MSG_PARAM_AUTHORIZATION_TOKEN);
        MOQ_TEST_CHECK(ts.params[0].value_len == 2);
    }

    /* -- Null argument tests ----------------------------------------- */
    {
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        MOQ_TEST_CHECK(moq_d16_encode_track_status(NULL, 0, &ns, name,
            NULL, 0) == MOQ_ERR_INVAL);

        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 0, NULL, name,
            NULL, 0) == MOQ_ERR_INVAL);

        MOQ_TEST_CHECK(moq_d16_decode_track_status(NULL, 0,
            NULL, 0, NULL) == MOQ_ERR_INVAL);
    }

    /* -- Encode rollback on buffer overflow --------------------------- */
    {
        uint8_t tiny[3];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, tiny, sizeof(tiny));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        moq_result_t rc = moq_d16_encode_track_status(&w, 0, &ns, name,
            NULL, 0);
        MOQ_TEST_CHECK(rc < 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* -- Full track name > 4096 rejected at encode -------------------- */
    {
        uint8_t buf[8192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t big[4090];
        memset(big, 'x', sizeof(big));
        moq_bytes_t ns_parts[] = {{ big, sizeof(big) }};
        moq_namespace_t ns = { ns_parts, 1 };
        uint8_t nm[10];
        memset(nm, 'y', sizeof(nm));
        moq_bytes_t name = { nm, sizeof(nm) };

        MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 0, &ns, name,
            NULL, 0) == MOQ_ERR_INVAL);
    }

    /* -- Namespace alone > 4096 rejected (underflow guard) ------------ */
    {
        uint8_t buf[8192];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t big[4100];
        memset(big, 'z', sizeof(big));
        moq_bytes_t ns_parts[] = {{ big, sizeof(big) }};
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 0, &ns, name,
            NULL, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    }

    /* -- Byte-boundary truncation ------------------------------------ */
    {
        uint8_t ts_buf[512];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, ts_buf, sizeof(ts_buf));
        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("example.com"),
        };
        moq_namespace_t ns = { ns_parts, 2 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("video");
        uint8_t tok[] = { 0xAB, 0xCD };
        moq_kvp_entry_t ts_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok, .value_len = 2,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        MOQ_TEST_CHECK(moq_d16_encode_track_status(&tw, 10, &ns, name,
            ts_params, 1) == MOQ_OK);
        size_t ts_len = moq_buf_writer_offset(&tw);

        moq_buf_reader_t er;
        moq_buf_reader_init(&er, ts_buf, ts_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&er, &env) == MOQ_OK);

        for (size_t cut = 0; cut < env.payload_len; cut++) {
            moq_bytes_t dec_parts[8];
            moq_kvp_entry_t dec_p[4];
            moq_d16_track_status_t ts = {
                .params = dec_p, .params_cap = 4,
            };
            moq_result_t rc = moq_d16_decode_track_status(env.payload, cut,
                dec_parts, 8, &ts);
            MOQ_TEST_CHECK(rc < 0);
        }
        /* Full length succeeds. */
        moq_bytes_t dec_parts[8];
        moq_kvp_entry_t dec_p[4];
        moq_d16_track_status_t ts = {
            .params = dec_p, .params_cap = 4,
        };
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_track_status(env.payload,
            env.payload_len, dec_parts, 8, &ts), MOQ_OK);
        MOQ_TEST_CHECK(ts.request_id == 10);
        MOQ_TEST_CHECK(ts.track_namespace.count == 2);
        MOQ_TEST_CHECK(ts.params_count == 1);
    }

    /* -- Type code is 0x0D, not 0x03 --------------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        MOQ_TEST_CHECK(moq_d16_encode_track_status(&w, 0, &ns, name,
            NULL, 0) == MOQ_OK);

        /* First byte is the type varint — should be 0x0D not 0x03. */
        MOQ_TEST_CHECK(buf[0] == 0x0D);

        /* Payload should be identical to SUBSCRIBE with same fields. */
        uint8_t sub_buf[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub_buf, sizeof(sub_buf));
        MOQ_TEST_CHECK(moq_d16_encode_subscribe(&sw, 0, &ns, name,
            NULL, 0) == MOQ_OK);

        size_t ts_len = moq_buf_writer_offset(&w);
        size_t sub_len = moq_buf_writer_offset(&sw);
        MOQ_TEST_CHECK_EQ_SIZE(ts_len, sub_len);
        /* Payloads match (skip type byte, compare rest). */
        MOQ_TEST_CHECK(memcmp(buf + 1, sub_buf + 1, ts_len - 1) == 0);
    }

    /* == OBJECT_DATAGRAM tests ======================================= */

    /* -- Normal datagram roundtrip ----------------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 5;
        dg.group_id = 10;
        dg.object_id = 3;
        dg.publisher_priority = 200;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);

        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(buf,
            moq_buf_writer_offset(&w), &out) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(out.track_alias, 5);
        MOQ_TEST_CHECK_EQ_U64(out.group_id, 10);
        MOQ_TEST_CHECK_EQ_U64(out.object_id, 3);
        MOQ_TEST_CHECK(out.publisher_priority == 200);
        MOQ_TEST_CHECK(!out.is_status);
        MOQ_TEST_CHECK(!out.end_of_group);
        MOQ_TEST_CHECK(!out.has_extensions);
        MOQ_TEST_CHECK(!out.default_priority);
        MOQ_TEST_CHECK_EQ_SIZE(out.payload_len, 5);
        MOQ_TEST_CHECK(memcmp(out.payload, "hello", 5) == 0);
    }

    /* -- Datagram with properties + payload --------------------------- */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t props[] = { 0x01, 0x02, 0x03 };
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 2;
        dg.object_id = 3;
        dg.publisher_priority = 100;
        dg.has_extensions = true;
        dg.extensions = props;
        dg.extensions_len = 3;
        dg.payload = (const uint8_t *)"data";
        dg.payload_len = 4;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);

        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(buf,
            moq_buf_writer_offset(&w), &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.has_extensions);
        MOQ_TEST_CHECK_EQ_SIZE(out.extensions_len, 3);
        MOQ_TEST_CHECK(memcmp(out.extensions, props, 3) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(out.payload_len, 4);
        MOQ_TEST_CHECK(memcmp(out.payload, "data", 4) == 0);
    }

    /* -- Status datagram (no payload) --------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 5;
        dg.object_id = 7;
        dg.publisher_priority = 128;
        dg.is_status = true;
        dg.object_status = 0x3;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);

        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(buf,
            moq_buf_writer_offset(&w), &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.is_status);
        MOQ_TEST_CHECK_EQ_U64(out.object_status, 0x3);
        MOQ_TEST_CHECK(out.payload == NULL);
        MOQ_TEST_CHECK_EQ_SIZE(out.payload_len, 0);
    }

    /* -- End-of-group datagram ---------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 2;
        dg.group_id = 3;
        dg.object_id = 4;
        dg.publisher_priority = 0;
        dg.end_of_group = true;
        dg.payload = (const uint8_t *)"x";
        dg.payload_len = 1;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);

        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(buf,
            moq_buf_writer_offset(&w), &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.end_of_group);
    }

    /* -- Default priority datagram ------------------------------------ */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 2;
        dg.default_priority = true;
        dg.payload = (const uint8_t *)"y";
        dg.payload_len = 1;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);

        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(buf,
            moq_buf_writer_offset(&w), &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.default_priority);
        MOQ_TEST_CHECK(out.publisher_priority == 128);
    }

    /* -- Zero object ID datagram -------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 0;
        dg.publisher_priority = 50;
        dg.payload = (const uint8_t *)"x";
        dg.payload_len = 1;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);

        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(buf,
            moq_buf_writer_offset(&w), &out) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(out.object_id, 0);
        /* ZERO_OBJECT_ID flag should be set, omitting object_id from wire */
        size_t no_zero_len;
        {
            uint8_t buf2[64];
            moq_buf_writer_t w2;
            moq_buf_writer_init(&w2, buf2, sizeof(buf2));
            moq_d16_object_datagram_t dg2 = dg;
            dg2.object_id = 2;
            MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w2, &dg2) == MOQ_OK);
            no_zero_len = moq_buf_writer_offset(&w2);
        }
        /* object_id=0 should be shorter (zero_oid flag omits the field) */
        MOQ_TEST_CHECK(moq_buf_writer_offset(&w) < no_zero_len);
    }

    /* -- Byte-boundary truncation ------------------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 2;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hi";
        dg.payload_len = 2;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_OK);
        size_t full_len = moq_buf_writer_offset(&w);

        for (size_t i = 0; i < full_len - 1; i++) {
            moq_d16_object_datagram_t out;
            moq_result_t trc = moq_d16_decode_object_datagram(buf, i, &out);
            MOQ_TEST_CHECK(trc < 0);
        }
    }

    /* -- Encoder rollback on tiny buffer ------------------------------ */
    {
        uint8_t buf[2];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1000;
        dg.group_id = 1000;
        dg.object_id = 2;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hello";
        dg.payload_len = 5;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) != MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(w.pos, 0);
    }

    /* -- Invalid type byte (STATUS + END_OF_GROUP) rejected ----------- */
    {
        uint8_t bad[] = { 0x22, 0x01, 0x01, 0x01, 128 };
        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(bad, sizeof(bad), &out) == MOQ_ERR_PROTO);
    }

    /* -- Invalid type byte (bit 4 set) rejected ----------------------- */
    {
        uint8_t bad[] = { 0x10, 0x01, 0x01, 0x01, 128 };
        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(bad, sizeof(bad), &out) == MOQ_ERR_PROTO);
    }

    /* -- STATUS + END_OF_GROUP encode rejected ------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 2;
        dg.is_status = true;
        dg.end_of_group = true;
        dg.object_status = 0x3;

        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_ERR_INVAL);
    }

    /* -- Status datagram with extensions rejected ---------------------- */
    {
        uint8_t bad[] = { 0x21, 0x01, 0x01, 0x01, 128, 0x02, 0xAA, 0xBB, 0x03 };
        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(bad, sizeof(bad), &out) == MOQ_ERR_PROTO);
    }

    /* -- Unknown status value rejected -------------------------------- */
    {
        uint8_t bad[] = { 0x20, 0x01, 0x01, 0x01, 128, 0x05 };
        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(bad, sizeof(bad), &out) == MOQ_ERR_PROTO);
    }

    /* -- Status datagram with trailing bytes rejected ------------------ */
    {
        uint8_t bad[] = { 0x20, 0x01, 0x01, 0x01, 128, 0x03, 0xFF };
        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(bad, sizeof(bad), &out) == MOQ_ERR_PROTO);
    }

    /* -- Non-status datagram with zero payload rejected ---------------- */
    {
        uint8_t bad[] = { 0x00, 0x01, 0x01, 0x01, 128 };
        moq_d16_object_datagram_t out;
        MOQ_TEST_CHECK(moq_d16_decode_object_datagram(bad, sizeof(bad), &out) == MOQ_ERR_PROTO);
    }

    /* -- Encode: status + extensions rejected ------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 2;
        dg.is_status = true;
        dg.has_extensions = true;
        dg.extensions = (const uint8_t *)"\xAA";
        dg.extensions_len = 1;
        dg.object_status = 0x3;
        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(w.pos, 0);
    }

    /* -- Encode: invalid status value rejected ------------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 2;
        dg.is_status = true;
        dg.object_status = 0x05;
        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(w.pos, 0);
    }

    /* -- Encode: non-status zero payload rejected --------------------- */
    {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 1;
        dg.object_id = 2;
        dg.publisher_priority = 128;
        MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&w, &dg) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(w.pos, 0);
    }

    MOQ_TEST_PASS("test_control_d16");
    return failures;
}
