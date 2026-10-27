#include <moq/moq.h>

/* Freeze the complete input before the independent timeout block. The test
 * can send the new prefix to the old library without using its new fields. */
typedef struct legacy_cfg {
    uint32_t struct_size;
    moq_namespace_t track_namespace;
    moq_bytes_t track_name;
    moq_subscribe_filter_t filter;
    uint64_t start_group, start_object, end_group;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    moq_group_order_t group_order;
    bool has_forward, forward;
    const moq_auth_token_t *auth_tokens;
    size_t auth_token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
    const moq_location_filter_cfg_t *location;
    const moq_fill_cfg_t *fill;
} legacy_cfg_t;

typedef struct timeout_block {
    uint32_t flags, reserved;
    uint64_t object_ms, subgroup_ms;
} timeout_block_t;

_Static_assert(offsetof(moq_subscribe_cfg_t, delivery_timeout_flags) == sizeof(legacy_cfg_t),
               "timeout block follows whole prior cfg");
_Static_assert(offsetof(moq_subscribe_cfg_t, subgroup_delivery_timeout_ms) + sizeof(uint64_t) ==
               sizeof(legacy_cfg_t) + sizeof(timeout_block_t), "whole block extent");
_Static_assert(MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT == 1 &&
               MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT == 2, "selected flags");

#ifndef MOQ_ADMISSION_LAYOUT_ONLY
#include <moq/sim.h>
#include "test_alloc_support.h"
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdlib.h>
#include <string.h>

static void drain(moq_session_t *s)
{
    moq_event_t event;
    while (moq_session_poll_events(s, &event, 1)) moq_event_cleanup(&event);
}

static void integer(uint8_t *p, size_t *n, uint64_t value)
{
    if (value == UINT64_MAX) {
        memset(p + *n, 0xff, 9);
        *n += 9;
    } else p[(*n)++] = (uint8_t)value;
}

static int row(moq_version_t version, timeout_block_t block, size_t tail_size,
               moq_result_t expected_rc, bool explicit_none)
{
    int failures = 0;
    test_alloc_state_t memory = {0};
    moq_alloc_t alloc = test_allocator(&memory);
    moq_simpair_cfg_t pair_cfg = MOQ_SIMPAIR_CFG_INIT;
    pair_cfg.alloc = &alloc;
    pair_cfg.version = version;
    pair_cfg.client_send_request_capacity = true;
    pair_cfg.client_initial_request_capacity = 16;
    pair_cfg.server_send_request_capacity = true;
    pair_cfg.server_initial_request_capacity = 16;
    moq_simpair_t *pair = NULL;
    moq_session_t *client = NULL;
    moq_action_t action;
    bool action_owned = false;
    moq_event_t event;
    bool event_owned = false;
    size_t input_size = sizeof(legacy_cfg_t) + tail_size;
    uint8_t *input = malloc(input_size);
#define NEED(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL: subscribe timeout d%u flags=%u tail=%zu: %s\n", \
            (unsigned)version, block.flags, tail_size, #x); \
    ++failures; goto done; \
} } while (0)
    NEED(input != NULL);
    legacy_cfg_t old = {0};
    old.struct_size = (uint32_t)input_size;
    moq_bytes_t part = MOQ_BYTES_LITERAL("n");
    old.track_namespace = (moq_namespace_t){&part, 1};
    old.track_name = MOQ_BYTES_LITERAL("v");
    old.filter = version == MOQ_VERSION_DRAFT_22 ?
        MOQ_SUBSCRIBE_FILTER_NONE : MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    if (explicit_none) old.location = &location;
    memcpy(input, &old, sizeof(old));
    memset(input + sizeof(old), 0xa5, tail_size);
    size_t copied = tail_size < sizeof(block) ? tail_size : sizeof(block);
    memcpy(input + sizeof(old), &block, copied);
    NEED(moq_simpair_create(&pair_cfg, &pair) == MOQ_OK);
    client = moq_simpair_client(pair);
    NEED(moq_simpair_start(pair) == MOQ_OK);
    NEED(moq_simpair_run_until_quiescent(pair, 16, NULL) == MOQ_OK);
    drain(client);
    drain(moq_simpair_server(pair));
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    uint64_t epoch = client->borrow_epoch, time = client->last_now_us;
    uint64_t stream_ref = client->next_stream_ref;
    int64_t balance = memory.balance;
    moq_result_t rc = moq_session_subscribe(client,
        (const moq_subscribe_cfg_t *)input, moq_simpair_now_us(pair) + 100, &sub);
    if (rc != expected_rc)
        fprintf(stderr, "subscribe timeout result: got %d expected %d\n",
                (int)rc, (int)expected_rc);
    NEED(rc == expected_rc);
    if (expected_rc != MOQ_OK) {
        NEED(client->borrow_epoch == epoch && client->last_now_us == time);
        NEED(client->next_stream_ref == stream_ref && memory.balance == balance);
    }
    action_owned = moq_session_poll_actions(client, &action, 1) == 1;
    if (expected_rc != MOQ_OK) {
        NEED(!action_owned);
    } else {
        NEED(action_owned);
        if (version == MOQ_VERSION_DRAFT_22) {
            uint8_t expected[32] = {3, 0, 0, 0, 1, 1, 'n', 1, 'v', 0};
            size_t n = 10;
            unsigned flags = tail_size >= sizeof(block) ? block.flags : 0;
            expected[9] = (uint8_t)((bool)(flags & 1u) + (bool)(flags & 2u) + explicit_none);
            if (flags & 1u) {
                expected[n++] = 2;
                integer(expected, &n, block.object_ms);
            }
            if (flags & 2u) {
                expected[n++] = (flags & 1u) ? 4 : 6;
                integer(expected, &n, block.subgroup_ms);
            }
            if (explicit_none) {
                expected[n++] = (uint8_t)(0x21 - ((flags & 2u) ? 6 : (flags & 1u) ? 2 : 0));
                expected[n++] = 0;
            }
            expected[2] = (uint8_t)(n - 3);
            NEED(action.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
            NEED(action.u.open_bidi_stream.len == n);
            NEED(memcmp(action.u.open_bidi_stream.data, expected, n) == 0);
            moq_session_t *server = moq_simpair_server(pair);
            NEED(moq_session_on_bidi_stream_bytes(server, moq_stream_ref_from_u64(0x5100),
                action.u.open_bidi_stream.data, n, false, moq_simpair_now_us(pair)) == MOQ_OK);
            event_owned = moq_session_poll_events(server, &event, 1) == 1;
            NEED(event_owned && event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
            NEED(event.detail_size >= offsetof(moq_subscribe_request_event_t, options) +
                                      sizeof(event.u.subscribe_request.options));
            const moq_subscription_admission_options_t *options = event.u.subscribe_request.options;
            NEED(options && options->struct_size >= MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE);
            NEED(options->flags == ((explicit_none ? 1u : 0u) |
                ((flags & 1u) ? 4u : 0u) | ((flags & 2u) ? 8u : 0u)));
            NEED(options->object_delivery_timeout_ms == ((flags & 1u) ? block.object_ms : 0));
            NEED(options->subgroup_delivery_timeout_ms == ((flags & 2u) ? block.subgroup_ms : 0));
            NEED(options->location.kind == MOQ_LOCATION_FILTER_NONE);
        }
    }
    NEED(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
done:
    if (event_owned) moq_event_cleanup(&event);
    if (action_owned) moq_action_cleanup(&action);
    if (pair) moq_simpair_destroy(pair);
    free(input);
    MOQ_TEST_CHECK(memory.balance == 0);
#undef NEED
    return failures;
}

int main(void)
{
    unsigned cases = 0, failed = 0;
    const timeout_block_t valid[] = {
        {0, 0, UINT64_MAX, UINT64_MAX}, {1, 0, 0, UINT64_MAX},
        {1, 0, UINT64_MAX, 0}, {2, 0, UINT64_MAX, 0},
        {2, 0, 0, UINT64_MAX}, {3, 0, 17, 23}, {3, 0, UINT64_MAX, 0}
    };
#define RUN(v, b, n, r) do { ++cases; if (row((v), (b), (n), (r), false)) ++failed; } while (0)
    for (unsigned i = 0; i < sizeof(valid) / sizeof(valid[0]); ++i) {
        RUN(MOQ_VERSION_DRAFT_22, valid[i], sizeof(timeout_block_t), MOQ_OK);
        RUN(MOQ_VERSION_DRAFT_22, valid[i], sizeof(timeout_block_t) + 8, MOQ_OK);
        /* Draft-16 has one DELIVERY_TIMEOUT, a QUIC varint never 0
         * (9.2.2.2): each of these selects 0, a value past the varint or two
         * different values. */
        RUN(MOQ_VERSION_DRAFT_16, valid[i], sizeof(timeout_block_t),
            valid[i].flags ? MOQ_ERR_INVAL : MOQ_OK);
        ++cases;
        if (row(MOQ_VERSION_DRAFT_22, valid[i], sizeof(timeout_block_t), MOQ_OK, true))
            ++failed;
        /* Draft-18 carries both independently (10.2.3, 10.2.4). */
        RUN(MOQ_VERSION_DRAFT_18, valid[i], sizeof(timeout_block_t), MOQ_OK);
    }
    /* What draft-16's single DELIVERY_TIMEOUT can state. */
    const timeout_block_t d16_valid[] = {
        {1, 0, 40, 0}, {2, 0, 0, 40}, {3, 0, 40, 40}
    };
    for (unsigned i = 0; i < sizeof(d16_valid) / sizeof(d16_valid[0]); ++i)
        RUN(MOQ_VERSION_DRAFT_16, d16_valid[i], sizeof(timeout_block_t), MOQ_OK);
    const timeout_block_t invalid_flags = {4, 0, 0, 0};
    const timeout_block_t invalid_reserved = {0, 1, 0, 0};
    for (size_t n = 0; n < sizeof(timeout_block_t); ++n) {
        RUN(MOQ_VERSION_DRAFT_22, invalid_flags, n, MOQ_OK);
        RUN(MOQ_VERSION_DRAFT_18, invalid_flags, n, MOQ_OK);
    }
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                     MOQ_VERSION_DRAFT_22};
    for (unsigned i = 0; i < sizeof(versions) / sizeof(versions[0]); ++i) {
        RUN(versions[i], invalid_flags, sizeof(timeout_block_t), MOQ_ERR_INVAL);
        RUN(versions[i], invalid_reserved, sizeof(timeout_block_t), MOQ_ERR_INVAL);
    }
#undef RUN
    printf("%u/%u subscription timeout cases passed\n", cases - failed, cases);
    return failed ? 1 : 0;
}
#endif
