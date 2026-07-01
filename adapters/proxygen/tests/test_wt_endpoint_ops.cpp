/*
 * Tests for wt_endpoint_ops — the proxygen WebTransport endpoint
 * binding for moq_transport_bridge_t.
 *
 * Uses a FakeWebTransport to verify:
 * - correct stream ID propagation
 * - FCState/ErrorCode → moq_transport_result_t mapping
 * - rcbuf ownership through write_payload
 * - exception containment
 */

#include "fake_webtransport.h"
#include "../src/wt_endpoint_ops.h"

#include <moq/rcbuf.h>
#include <moq/types.h>

#include <cstdio>
#include <cstring>

#define WT_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define WT_CHECK_EQ(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (_a != _b) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s (%d) != %s (%d)\n", \
            __FILE__, __LINE__, #a, (int)_a, #b, (int)_b); \
        failures++; \
    } \
} while (0)

using namespace moq::wt;
using namespace moq::wt::testing;
using WT = proxygen::WebTransport;

struct Fixture {
    FakeWebTransport fake;
    moq_transport_endpoint_ops_t ops;
    wt_endpoint_ctx_t ctx;

    Fixture() {
        wt_endpoint_ops_init(&ops, &ctx, &fake);
    }
};

/* -- open_uni tests ------------------------------------------------- */

static int test_open_uni_returns_stream_id()
{
    int failures = 0;
    Fixture f;
    f.fake.next_stream_id = 42;

    uint64_t id = 0;
    auto rc = f.ops.open_uni(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(id, 42u);
    WT_CHECK_EQ(f.fake.create_uni_count, 1);
    return failures;
}

static int test_open_uni_creation_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_create_uni_error = WT::ErrorCode::STREAM_CREATION_ERROR;

    uint64_t id = 0;
    auto rc = f.ops.open_uni(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    return failures;
}

static int test_open_uni_blocked_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_create_uni_error = WT::ErrorCode::BLOCKED;

    uint64_t id = 0;
    auto rc = f.ops.open_uni(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    return failures;
}

static int test_open_uni_generic_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_create_uni_error = WT::ErrorCode::GENERIC_ERROR;

    uint64_t id = 0;
    auto rc = f.ops.open_uni(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

/* -- open_bidi tests ------------------------------------------------ */

static int test_open_bidi_returns_write_handle_id()
{
    int failures = 0;
    Fixture f;
    f.fake.next_stream_id = 77;

    uint64_t id = 0;
    auto rc = f.ops.open_bidi(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(id, 77u);
    WT_CHECK_EQ(f.fake.create_bidi_count, 1);
    return failures;
}

static int test_open_bidi_creation_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_create_bidi_error = WT::ErrorCode::STREAM_CREATION_ERROR;

    uint64_t id = 0;
    auto rc = f.ops.open_bidi(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    return failures;
}

/* -- write tests ---------------------------------------------------- */

static int test_write_ok_unblocked()
{
    int failures = 0;
    Fixture f;

    const uint8_t data[] = {0x01, 0x02, 0x03};
    auto rc = f.ops.write(&f.ctx, 10, data, sizeof(data), false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.writes.size(), 1u);
    WT_CHECK_EQ(f.fake.writes[0].stream_id, 10u);
    WT_CHECK_EQ(f.fake.writes[0].data_len, 3u);
    WT_CHECK_EQ(f.fake.writes[0].fin, false);
    return failures;
}

static int test_write_ok_with_fin()
{
    int failures = 0;
    Fixture f;

    auto rc = f.ops.write(&f.ctx, 20, nullptr, 0, true);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.writes.size(), 1u);
    WT_CHECK_EQ(f.fake.writes[0].stream_id, 20u);
    WT_CHECK_EQ(f.fake.writes[0].fin, true);
    return failures;
}

static int test_write_fc_blocked_returns_ok()
{
    int failures = 0;
    Fixture f;
    f.fake.next_write_fc_state = WT::FCState::BLOCKED;

    const uint8_t data[] = {0xAA};
    auto rc = f.ops.write(&f.ctx, 10, data, 1, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.writes.size(), 1u);
    return failures;
}

static int test_write_fc_session_closed_returns_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_write_fc_state = WT::FCState::SESSION_CLOSED;

    auto rc = f.ops.write(&f.ctx, 10, nullptr, 0, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_write_error_blocked_returns_would_block()
{
    int failures = 0;
    Fixture f;
    f.fake.next_write_error = WT::ErrorCode::BLOCKED;

    auto rc = f.ops.write(&f.ctx, 10, nullptr, 0, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    WT_CHECK(f.fake.writes.empty());
    return failures;
}

static int test_write_error_send_error_returns_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_write_error = WT::ErrorCode::SEND_ERROR;

    auto rc = f.ops.write(&f.ctx, 10, nullptr, 0, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

/* -- write_payload tests -------------------------------------------- */

static int test_write_payload_ok()
{
    int failures = 0;
    Fixture f;

    const uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
    moq_rcbuf_t *buf = nullptr;
    moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &buf);
    WT_CHECK(buf != nullptr);
    WT_CHECK_EQ(moq_rcbuf_refcount(buf), 1u);

    auto rc = f.ops.write_payload(&f.ctx, 10, buf, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.writes.size(), 1u);
    WT_CHECK_EQ(f.fake.writes[0].stream_id, 10u);
    WT_CHECK_EQ(f.fake.writes[0].data_len, 4u);

    // After successful write, the IOBuf owns one ref.
    // Our ref is still alive too.
    WT_CHECK(moq_rcbuf_refcount(buf) >= 1u);

    moq_rcbuf_decref(buf);
    return failures;
}

static int test_write_payload_error_no_leak()
{
    int failures = 0;
    Fixture f;
    f.fake.next_write_error = WT::ErrorCode::SEND_ERROR;

    const uint8_t payload[] = {0x01, 0x02};
    moq_rcbuf_t *buf = nullptr;
    moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &buf);
    WT_CHECK(buf != nullptr);
    WT_CHECK_EQ(moq_rcbuf_refcount(buf), 1u);

    auto rc = f.ops.write_payload(&f.ctx, 10, buf, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);

    // On error, the IOBuf was never created (writeStreamData failed
    // before consuming it) or the guard cleaned up the extra ref.
    // Either way, our ref should still be exactly 1.
    WT_CHECK_EQ(moq_rcbuf_refcount(buf), 1u);

    moq_rcbuf_decref(buf);
    return failures;
}

static int test_write_payload_fc_blocked_returns_ok()
{
    int failures = 0;
    Fixture f;
    f.fake.next_write_fc_state = WT::FCState::BLOCKED;

    const uint8_t payload[] = {0xBB};
    moq_rcbuf_t *buf = nullptr;
    moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &buf);
    WT_CHECK(buf != nullptr);

    auto rc = f.ops.write_payload(&f.ctx, 10, buf, true);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.writes.size(), 1u);
    WT_CHECK_EQ(f.fake.writes[0].fin, true);

    moq_rcbuf_decref(buf);
    return failures;
}

/* -- reset_stream tests --------------------------------------------- */

static int test_reset_stream_ok()
{
    int failures = 0;
    Fixture f;

    auto rc = f.ops.reset_stream(&f.ctx, 55, 0x100);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.resets.size(), 1u);
    WT_CHECK_EQ(f.fake.resets[0].stream_id, 55u);
    WT_CHECK_EQ(f.fake.resets[0].error, 0x100u);
    return failures;
}

static int test_reset_stream_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_reset_error = WT::ErrorCode::INVALID_STREAM_ID;

    auto rc = f.ops.reset_stream(&f.ctx, 55, 0x100);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_reset_stream_blocked_returns_would_block()
{
    int failures = 0;
    Fixture f;
    f.fake.next_reset_error = WT::ErrorCode::BLOCKED;

    auto rc = f.ops.reset_stream(&f.ctx, 55, 0x100);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    return failures;
}

/* -- stop_sending tests --------------------------------------------- */

static int test_stop_sending_ok()
{
    int failures = 0;
    Fixture f;

    auto rc = f.ops.stop_sending(&f.ctx, 66, 0x200);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.stops.size(), 1u);
    WT_CHECK_EQ(f.fake.stops[0].stream_id, 66u);
    WT_CHECK_EQ(f.fake.stops[0].error, 0x200u);
    return failures;
}

static int test_stop_sending_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_stop_error = WT::ErrorCode::INVALID_STREAM_ID;

    auto rc = f.ops.stop_sending(&f.ctx, 66, 0x200);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_stop_sending_blocked_returns_would_block()
{
    int failures = 0;
    Fixture f;
    f.fake.next_stop_error = WT::ErrorCode::BLOCKED;

    auto rc = f.ops.stop_sending(&f.ctx, 66, 0x200);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    return failures;
}

/* -- send_datagram tests -------------------------------------------- */

static int test_datagram_ok()
{
    int failures = 0;
    Fixture f;

    const uint8_t data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
    auto rc = f.ops.send_datagram(&f.ctx, data, sizeof(data));
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.datagrams.size(), 1u);
    WT_CHECK_EQ(f.fake.datagrams[0].len, 5u);
    return failures;
}

static int test_datagram_blocked_returns_dropped()
{
    int failures = 0;
    Fixture f;
    f.fake.next_datagram_error = WT::ErrorCode::BLOCKED;

    auto rc = f.ops.send_datagram(&f.ctx, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_DROPPED);
    return failures;
}

static int test_datagram_generic_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_datagram_error = WT::ErrorCode::GENERIC_ERROR;

    auto rc = f.ops.send_datagram(&f.ctx, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

/* -- close_transport tests ------------------------------------------ */

static int test_close_with_code()
{
    int failures = 0;
    Fixture f;

    auto rc = f.ops.close_transport(&f.ctx, 0x42, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.closes.size(), 1u);
    WT_CHECK(f.fake.closes[0].error.has_value());
    WT_CHECK_EQ(*f.fake.closes[0].error, 0x42u);
    return failures;
}

static int test_close_without_code()
{
    int failures = 0;
    Fixture f;

    auto rc = f.ops.close_transport(&f.ctx, 0, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_OK);
    WT_CHECK_EQ(f.fake.closes.size(), 1u);
    WT_CHECK(!f.fake.closes[0].error.has_value());
    return failures;
}

static int test_close_error()
{
    int failures = 0;
    Fixture f;
    f.fake.next_close_error = WT::ErrorCode::SESSION_TERMINATED;

    auto rc = f.ops.close_transport(&f.ctx, 0, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_close_blocked_returns_would_block()
{
    int failures = 0;
    Fixture f;
    f.fake.next_close_error = WT::ErrorCode::BLOCKED;

    auto rc = f.ops.close_transport(&f.ctx, 0, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_WOULD_BLOCK);
    return failures;
}

/* -- exception guard tests ------------------------------------------ */

static int test_exception_open_uni()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    uint64_t id = 0;
    auto rc = f.ops.open_uni(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_exception_write()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    auto rc = f.ops.write(&f.ctx, 10, nullptr, 0, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_exception_datagram()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    auto rc = f.ops.send_datagram(&f.ctx, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_exception_open_bidi()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    uint64_t id = 0;
    auto rc = f.ops.open_bidi(&f.ctx, &id);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_exception_write_payload()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    const uint8_t payload[] = {0x01};
    moq_rcbuf_t *buf = nullptr;
    moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &buf);
    WT_CHECK(buf != nullptr);

    auto rc = f.ops.write_payload(&f.ctx, 10, buf, false);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    WT_CHECK_EQ(moq_rcbuf_refcount(buf), 1u);

    moq_rcbuf_decref(buf);
    return failures;
}

static int test_exception_reset()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    auto rc = f.ops.reset_stream(&f.ctx, 10, 0x42);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_exception_stop_sending()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    auto rc = f.ops.stop_sending(&f.ctx, 10, 0x42);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

static int test_exception_close()
{
    int failures = 0;
    Fixture f;
    f.fake.throw_on_next_call = true;

    auto rc = f.ops.close_transport(&f.ctx, 0, nullptr, 0);
    WT_CHECK_EQ(rc, MOQ_TRANSPORT_ERROR);
    return failures;
}

/* -- vtable init test ----------------------------------------------- */

static int test_ops_init()
{
    int failures = 0;
    Fixture f;

    WT_CHECK_EQ(f.ops.struct_size, (uint32_t)sizeof(moq_transport_endpoint_ops_t));
    WT_CHECK(f.ops.capabilities & MOQ_TRANSPORT_CAP_DATAGRAM);
    WT_CHECK(f.ops.capabilities & MOQ_TRANSPORT_CAP_WRITE_PAYLOAD);
    WT_CHECK(f.ops.open_uni != nullptr);
    WT_CHECK(f.ops.open_bidi != nullptr);
    WT_CHECK(f.ops.write != nullptr);
    WT_CHECK(f.ops.write_payload != nullptr);
    WT_CHECK(f.ops.reset_stream != nullptr);
    WT_CHECK(f.ops.stop_sending != nullptr);
    WT_CHECK(f.ops.send_datagram != nullptr);
    WT_CHECK(f.ops.close_transport != nullptr);
    return failures;
}

/* == Main =========================================================== */

int main()
{
    int failures = 0;

    // open_uni
    failures += test_open_uni_returns_stream_id();
    failures += test_open_uni_creation_error();
    failures += test_open_uni_blocked_error();
    failures += test_open_uni_generic_error();

    // open_bidi
    failures += test_open_bidi_returns_write_handle_id();
    failures += test_open_bidi_creation_error();

    // write
    failures += test_write_ok_unblocked();
    failures += test_write_ok_with_fin();
    failures += test_write_fc_blocked_returns_ok();
    failures += test_write_fc_session_closed_returns_error();
    failures += test_write_error_blocked_returns_would_block();
    failures += test_write_error_send_error_returns_error();

    // write_payload
    failures += test_write_payload_ok();
    failures += test_write_payload_error_no_leak();
    failures += test_write_payload_fc_blocked_returns_ok();

    // reset_stream
    failures += test_reset_stream_ok();
    failures += test_reset_stream_error();
    failures += test_reset_stream_blocked_returns_would_block();

    // stop_sending
    failures += test_stop_sending_ok();
    failures += test_stop_sending_error();
    failures += test_stop_sending_blocked_returns_would_block();

    // send_datagram
    failures += test_datagram_ok();
    failures += test_datagram_blocked_returns_dropped();
    failures += test_datagram_generic_error();

    // close_transport
    failures += test_close_with_code();
    failures += test_close_without_code();
    failures += test_close_error();
    failures += test_close_blocked_returns_would_block();

    // exception guard
    failures += test_exception_open_uni();
    failures += test_exception_open_bidi();
    failures += test_exception_write();
    failures += test_exception_write_payload();
    failures += test_exception_reset();
    failures += test_exception_stop_sending();
    failures += test_exception_datagram();
    failures += test_exception_close();

    // vtable init
    failures += test_ops_init();

    if (failures == 0)
        std::printf("test_wt_endpoint_ops: all 38 tests passed\n");
    else
        std::fprintf(stderr, "test_wt_endpoint_ops: %d failure(s)\n", failures);

    return failures;
}
