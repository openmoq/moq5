#include <moq/codec.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, size);

    moq_control_envelope_t env;
    moq_result_t rc = moq_control_decode_envelope(&r, &env);
    if (rc < 0)
        return 0;

    if (env.msg_type == MOQ_D16_CLIENT_SETUP ||
        env.msg_type == MOQ_D16_SERVER_SETUP) {
        moq_kvp_entry_t params[64];
        moq_d16_setup_t setup = {
            .params = params,
            .params_count = 0,
            .params_cap = 64
        };

        if (env.msg_type == MOQ_D16_CLIENT_SETUP)
            moq_d16_decode_client_setup(env.payload, env.payload_len, &setup);
        else
            moq_d16_decode_server_setup(env.payload, env.payload_len, &setup);
    } else if (env.msg_type == MOQ_D16_UNSUBSCRIBE ||
               env.msg_type == MOQ_D16_FETCH_CANCEL ||
               env.msg_type == MOQ_D16_PUBLISH_NAMESPACE_DONE ||
               env.msg_type == MOQ_D16_MAX_REQUEST_ID ||
               env.msg_type == MOQ_D16_REQUESTS_BLOCKED) {
        uint64_t value = 0;
        moq_d16_decode_varint_msg(env.payload, env.payload_len, &value);
    } else if (env.msg_type == MOQ_D16_GOAWAY) {
        moq_d16_goaway_t goaway;
        moq_d16_decode_goaway(env.payload, env.payload_len, &goaway);
    } else if (env.msg_type == MOQ_D16_PUBLISH_NAMESPACE) {
        moq_bytes_t ns_parts[32];
        moq_kvp_entry_t params[64];
        moq_d16_publish_namespace_t pn = {
            .params = params, .params_count = 0, .params_cap = 64,
        };
        moq_d16_decode_publish_namespace(env.payload, env.payload_len,
                                          ns_parts, 32, &pn);
    } else if (env.msg_type == MOQ_D16_PUBLISH_NAMESPACE_CANCEL) {
        moq_d16_publish_namespace_cancel_t cancel;
        moq_d16_decode_publish_namespace_cancel(env.payload, env.payload_len,
                                                &cancel);
    } else if (env.msg_type == MOQ_D16_REQUEST_OK) {
        moq_kvp_entry_t params[64];
        moq_d16_request_ok_t ok = {
            .params = params, .params_count = 0, .params_cap = 64
        };
        moq_d16_decode_request_ok(env.payload, env.payload_len, &ok);
    } else if (env.msg_type == MOQ_D16_REQUEST_ERROR) {
        moq_d16_request_error_t err;
        moq_d16_decode_request_error(env.payload, env.payload_len, &err);
    }

    /* Fuzz auth token structure decode. */
    {
        moq_d16_auth_token_t tok;
        moq_d16_auth_token_decode(data, size, &tok);
    }

    /* Also fuzz subgroup header + object fields as a data stream. */
    {
        moq_buf_reader_t dr;
        moq_buf_reader_init(&dr, data, size);
        moq_d16_subgroup_header_t shdr;
        if (moq_d16_decode_subgroup_header(&dr, &shdr) == MOQ_OK) {
            moq_d16_object_fields_t obj;
            moq_d16_decode_object_fields(&dr, shdr.has_extensions, &obj);
        }
    }

    return 0;
}
