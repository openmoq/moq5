#include "../core/src/wire/messages_d22_internal.h"
#include "moq/vi64.h"
#include <string.h>
#include <stdio.h>
static int failures;
#define CHECK(x)                                                                                   \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "%d: %s\n", __LINE__, #x);                                             \
            ++failures;                                                                            \
        }                                                                                          \
    } while (0)
int main(void)
{
    /* Literal delta type 0x40b57, odd length, path [0,0,7], route delta 1. */
    const uint8_t good[] = {0xc4, 0x0b, 0x57, 3, 0, 0, 7, 1, 0};
    moq_buf_reader_t r = {good, sizeof good, 0};
    moq_d22_params_t ps = {0};
    CHECK(moq_d22_decode_params(&r, 2, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_OK);
    CHECK(r.pos == sizeof good && ps.present == 0);
    r = (moq_buf_reader_t){good, sizeof good, 0};
    CHECK(moq_d22_decode_params(&r, 2, MOQ_D22_SCOPE_SUBSCRIBE, 0, &ps) == MOQ_ERR_PROTO);
    const uint8_t duplicate[] = {0xc4, 0x0b, 0x57, 2, 7, 7};
    r = (moq_buf_reader_t){duplicate, sizeof duplicate, 0};
    CHECK(moq_d22_decode_params(&r, 1, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_ERR_PROTO);
    const uint8_t empty[] = {0xc4, 0x0b, 0x57, 0};
    r = (moq_buf_reader_t){empty, sizeof empty, 0};
    CHECK(moq_d22_decode_params(&r, 1, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_ERR_PROTO);
    const uint8_t truncated[] = {0xc4, 0x0b, 0x57, 1, 0x80};
    r = (moq_buf_reader_t){truncated, sizeof truncated, 0};
    CHECK(moq_d22_decode_params(&r, 1, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_ERR_PROTO);
    const uint8_t full[] = {0xc4, 0x0b, 0x57, 11,   0x80, 0x00, 0xff, 0xff,
                            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    r = (moq_buf_reader_t){full, sizeof full, 0};
    CHECK(moq_d22_decode_params(&r, 1, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_OK);
    CHECK(ps.has_hop_path && !ps.has_route_cost);
    for (unsigned scope = 0; scope <= MOQ_D22_SCOPE_NAMESPACE; scope++) {
        r = (moq_buf_reader_t){good, sizeof good, 0};
        moq_result_t expected =
            (scope == MOQ_D22_SCOPE_PUBLISH_NAMESPACE ||
             scope == MOQ_D22_SCOPE_UPDATE_PUBLISH_NAMESPACE || scope == MOQ_D22_SCOPE_NAMESPACE)
                ? MOQ_OK
                : MOQ_ERR_PROTO;
        CHECK(moq_d22_decode_params(&r, 2, (moq_d22_param_scope_t)scope, 0, &ps) == expected);
    }
    const uint8_t dup_param[] = {0xc4, 0x0b, 0x57, 1, 0, 0, 1, 0};
    r = (moq_buf_reader_t){dup_param, sizeof dup_param, 0};
    CHECK(moq_d22_decode_params(&r, 2, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_ERR_PROTO);
    const uint8_t dup_cost[] = {0xc4, 0x0b, 0x58, 0, 0, 1};
    r = (moq_buf_reader_t){dup_cost, sizeof dup_cost, 0};
    CHECK(moq_d22_decode_params(&r, 2, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) == MOQ_ERR_PROTO);
    /* Incomplete outer parameter conserves all caller output. */
    for (size_t n = 0; n < sizeof good; n++) {
        r = (moq_buf_reader_t){good, n, 0};
        moq_d22_params_t before;
        memset(&ps, 0xa5, sizeof ps);
        before = ps;
        CHECK(moq_d22_decode_params(&r, 2, MOQ_D22_SCOPE_PUBLISH_NAMESPACE, 0, &ps) ==
              MOQ_ERR_BUFFER);
        CHECK(r.pos == 0 && !memcmp(&before, &ps, sizeof ps));
    }
    moq_d22_setup_options_t setup = {0};
    const uint8_t opts[] = {0xc4, 0x0b, 0x54, 0, 2, 0};
    CHECK(moq_d22_decode_setup_options(opts, sizeof opts, &setup) == MOQ_OK);
    CHECK(setup.has_hop_id && setup.hop_id == 0 && setup.has_relay_cost && setup.relay_cost == 0);
    const uint8_t dup_setup[] = {0xc4, 0x0b, 0x54, 0, 0, 1};
    CHECK(moq_d22_decode_setup_options(dup_setup, sizeof dup_setup, &setup) == MOQ_ERR_PROTO);
    const uint8_t ns_wire[] = {8, 0, 6, 1, 4, 'm', 'e', 's', 'h'};
    const uint8_t ns_body[] = {1, 4, 'm', 'e', 's', 'h'};
    moq_d22_message_t message = {.type = MOQ_D22_NAMESPACE,
                                 .ns = {.encoded = {ns_body, sizeof ns_body}}};
    moq_d22_message_context_t ctx = {0};
    uint8_t buf[64];
    moq_buf_writer_t w = {buf, sizeof buf, 0};
    CHECK(moq_d22_encode_message(&w, &message, NULL, 0, &ctx) == MOQ_OK);
    CHECK(w.pos == sizeof ns_wire && !memcmp(buf, ns_wire, sizeof ns_wire));
    uint64_t ids[] = {0, UINT64_MAX};
    moq_d22_param_t params[] = {{.type = MOQ_D22_HOP_PATH, .hop_ids = ids, .hop_count = 2},
                                {.type = MOQ_D22_ROUTE_COST, .value.integer = UINT64_MAX}};
    w.pos = 0;
    ctx.cluster = true;
    CHECK(moq_d22_encode_message(&w, &message, params, 2, &ctx) == MOQ_OK);
    CHECK(buf[sizeof ns_wire] == 2); /* one combined block */
    moq_d22_message_t decoded = {0};
    CHECK(moq_d22_decode_message(MOQ_D22_NAMESPACE, buf + 3, w.pos - 3, &ctx, &decoded) == MOQ_OK);
    CHECK(decoded.params.has_hop_path && decoded.params.has_route_cost);
    ctx.cluster = false;
    CHECK(moq_d22_decode_message(MOQ_D22_NAMESPACE, buf + 3, w.pos - 3, &ctx, &decoded) ==
          MOQ_ERR_PROTO);
    return failures ? 1 : 0;
}
