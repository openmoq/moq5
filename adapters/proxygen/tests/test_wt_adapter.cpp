/*
 * Tests for the proxygen WebTransport adapter.
 *
 * Uses FakeWebTransport + InlineExecutor for deterministic
 * read-loop testing. All futures complete synchronously.
 */

#include "fake_wt_pair.h"
#include "fake_webtransport.h"
#include <moq/proxygen_wt.hpp>
#include <moq/moq.h>

#include <folly/executors/InlineExecutor.h>

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
        std::fprintf(stderr, "FAIL: %s:%d: %s (%lld) != %s (%lld)\n", \
            __FILE__, __LINE__, #a, (long long)_a, #b, (long long)_b); \
        failures++; \
    } \
} while (0)

using namespace moq::wt;
using namespace moq::wt::testing;
using WT = proxygen::WebTransport;

static uint64_t test_time = 1000;
static uint64_t test_now_us() { return test_time; }

struct AdapterFixture {
    FakeWebTransport fake;
    moq_session_t *session = nullptr;
    std::unique_ptr<Adapter> adapter;

    bool init(moq_perspective_t perspective = MOQ_PERSPECTIVE_SERVER)
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), perspective);
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 10;
        if (moq_session_create(&cfg, 0, &session) < 0)
            return false;

        Adapter::Config acfg{};
        acfg.session = session;
        acfg.alloc = moq_alloc_default();
        acfg.executor = &folly::InlineExecutor::instance();
        acfg.now_us = test_now_us;

        adapter = Adapter::create(acfg, &fake);
        return adapter != nullptr;
    }

    ~AdapterFixture()
    {
        adapter.reset();
        if (session) moq_session_destroy(session);
    }
};

/*
 * Pump: take writes from src fake for a given stream, queue as
 * reads on dst fake for another stream.
 */
static size_t pump_writes(FakeWebTransport &src, uint64_t src_id,
                          FakeWebTransport &dst, uint64_t dst_id)
{
    size_t total = 0;
    for (auto &w : src.writes) {
        if (w.stream_id == src_id && w.data) {
            auto *p = w.data->data();
            auto len = w.data->computeChainDataLength();
            dst.queueRead(dst_id, p, len, w.fin);
            total += len;
        }
    }
    src.writes.clear();
    return total;
}

/* ================================================================== */
/* Creation, service, datagram, session end, terminal guard           */
/* ================================================================== */

static int test_create_ok()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    WT_CHECK(f.adapter->session() == f.session);
    WT_CHECK(!f.adapter->is_terminal());
    WT_CHECK(!f.adapter->is_closed());
    WT_CHECK(!f.adapter->is_fatal());
    return failures;
}

static int test_create_null_session()
{
    int failures = 0;
    FakeWebTransport fake;
    Adapter::Config cfg{};
    cfg.alloc = moq_alloc_default();
    cfg.executor = &folly::InlineExecutor::instance();
    WT_CHECK(Adapter::create(cfg, &fake) == nullptr);
    return failures;
}

static int test_create_null_wt()
{
    int failures = 0;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    moq_session_t *s = nullptr;
    moq_session_create(&scfg, 0, &s);
    Adapter::Config cfg{};
    cfg.session = s;
    cfg.alloc = moq_alloc_default();
    cfg.executor = &folly::InlineExecutor::instance();
    WT_CHECK(Adapter::create(cfg, nullptr) == nullptr);
    moq_session_destroy(s);
    return failures;
}

static int test_create_null_alloc()
{
    int failures = 0;
    FakeWebTransport fake;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    moq_session_t *s = nullptr;
    moq_session_create(&scfg, 0, &s);
    Adapter::Config cfg{};
    cfg.session = s;
    cfg.executor = &folly::InlineExecutor::instance();
    WT_CHECK(Adapter::create(cfg, &fake) == nullptr);
    moq_session_destroy(s);
    return failures;
}

static int test_create_null_executor()
{
    int failures = 0;
    FakeWebTransport fake;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    moq_session_t *s = nullptr;
    moq_session_create(&scfg, 0, &s);
    Adapter::Config cfg{};
    cfg.session = s;
    cfg.alloc = moq_alloc_default();
    WT_CHECK(Adapter::create(cfg, &fake) == nullptr);
    moq_session_destroy(s);
    return failures;
}

static int test_service_drives_bridge()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init(MOQ_PERSPECTIVE_CLIENT));
    moq_session_start(f.session, 0);
    WT_CHECK_EQ(f.adapter->service(), MOQ_OK);
    WT_CHECK(f.fake.create_bidi_count > 0);
    return failures;
}

static int test_datagram_forwarded_no_crash()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onDatagram(folly::IOBuf::copyBuffer("dg", 2));
    return failures;
}

static int test_session_end_clean()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionEnd(folly::none);
    WT_CHECK(f.adapter->is_closed());
    WT_CHECK(!f.adapter->is_fatal());
    return failures;
}

static int test_session_end_with_error()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionEnd(folly::Optional<uint32_t>(0x42));
    WT_CHECK(f.adapter->is_closed());
    WT_CHECK_EQ(f.adapter->close_code(), 0x42u);
    return failures;
}

static int test_session_end_error_is_close_not_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionEnd(folly::Optional<uint32_t>(0x99));
    WT_CHECK(f.adapter->is_closed());
    WT_CHECK(!f.adapter->is_fatal());
    return failures;
}

static int test_session_drain_no_op()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionDrain();
    WT_CHECK(!f.adapter->is_terminal());
    return failures;
}

static int test_null_uni_handle_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onNewUniStream(nullptr);
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

static int test_null_bidi_read_handle_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    FakeWriteHandle wh(700);
    WT::BidiStreamHandle bh{nullptr, &wh};
    f.adapter->onNewBidiStream(bh);
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

static int test_handlers_noop_after_close()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionEnd(folly::none);
    FakeReadHandle rh(700);
    f.adapter->onNewUniStream(&rh);
    WT_CHECK(f.adapter->is_terminal());
    return failures;
}

static int test_service_after_close()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionEnd(folly::none);
    WT_CHECK_EQ(f.adapter->service(), MOQ_OK);
    return failures;
}

/* ================================================================== */
/* Read-loop mechanics                                                */
/* ================================================================== */

static int test_uni_read_issues_read()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.fake.queueRead(500, "\x01\x02", 2, false);
    FakeReadHandle rh(500);
    int before = f.fake.read_call_count;
    f.adapter->onNewUniStream(&rh);
    WT_CHECK(f.fake.read_call_count > before);
    return failures;
}

static int test_fin_stops_read_loop()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.fake.queueRead(500, "x", 1, true);
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);
    int after = f.fake.read_call_count;
    f.fake.queueRead(500, "y", 1, false);
    f.adapter->service();
    WT_CHECK_EQ(f.fake.read_call_count, after);
    return failures;
}

static int test_read_error_immediate()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.fake.next_read_error = WT::ErrorCode::INVALID_STREAM_ID;
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

static int test_read_future_exception()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.fake.queueReadException(500, 0x42);
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);
    // Data-stream read exception must be a stream reset, NOT fatal.
    // This catches cross-DSO RTTI failures where get_exception<>()
    // doesn't match and the adapter incorrectly goes fatal.
    WT_CHECK(!f.adapter->is_fatal());
    return failures;
}

static int test_terminal_adapter_no_reads()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.adapter->onSessionEnd(folly::none);
    int before = f.fake.read_call_count;
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);
    WT_CHECK_EQ(f.fake.read_call_count, before);
    return failures;
}

static int test_callback_after_destroy_safe()
{
    int failures = 0;
    {
        AdapterFixture f;
        WT_CHECK(f.init());
        FakeReadHandle rh(500);
        f.adapter->onNewUniStream(&rh);
    }
    return failures;
}

static int test_stream_state_cleaned_on_fin()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.fake.queueRead(500, "x", 1, true);
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);
    int after = f.fake.read_call_count;
    f.adapter->service();
    WT_CHECK_EQ(f.fake.read_call_count, after);
    return failures;
}

static int test_stream_state_cleaned_on_error()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());
    f.fake.queueReadException(500, 0x42);
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);
    int after = f.fake.read_call_count;
    f.adapter->service();
    WT_CHECK_EQ(f.fake.read_call_count, after);
    return failures;
}

/* ================================================================== */
/* Control-stream error → fatal                                       */
/* ================================================================== */

static int test_peer_control_read_error_is_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());

    // First peer bidi = control. Queue an exception on it.
    f.fake.queueReadException(600, 0x10);
    FakeReadHandle rh(600);
    FakeWriteHandle wh(600);
    WT::BidiStreamHandle bh{&rh, &wh};
    f.adapter->onNewBidiStream(bh);

    WT_CHECK(f.adapter->is_terminal());
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

/* ================================================================== */
/* Two-adapter handshake (proves routing, service, local              */
/* bidi reads)                                                        */
/* ================================================================== */

static int test_handshake_through_adapters()
{
    int failures = 0;

    // -- Client --
    FakeWebTransport client_wt;
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    moq_session_t *client_sess = nullptr;
    WT_CHECK(moq_session_create(&ccfg, 0, &client_sess) == MOQ_OK);

    Adapter::Config client_acfg{};
    client_acfg.session = client_sess;
    client_acfg.alloc = moq_alloc_default();
    client_acfg.executor = &folly::InlineExecutor::instance();
    client_acfg.now_us = test_now_us;
    auto client = Adapter::create(client_acfg, &client_wt);
    WT_CHECK(client != nullptr);

    // -- Server --
    FakeWebTransport server_wt;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    moq_session_t *server_sess = nullptr;
    WT_CHECK(moq_session_create(&scfg, 0, &server_sess) == MOQ_OK);

    Adapter::Config server_acfg{};
    server_acfg.session = server_sess;
    server_acfg.alloc = moq_alloc_default();
    server_acfg.executor = &folly::InlineExecutor::instance();
    server_acfg.now_us = test_now_us;
    auto server = Adapter::create(server_acfg, &server_wt);
    WT_CHECK(server != nullptr);

    // 1. Client starts → CLIENT_SETUP written, local bidi opened
    uint64_t client_ctrl = 100; // first fake stream ID
    moq_session_start(client_sess, 0);
    client->service();
    WT_CHECK(client_wt.create_bidi_count > 0);
    WT_CHECK(!client_wt.writes.empty());

    // 2. Pump CLIENT_SETUP → server's first peer bidi (control)
    uint64_t server_ctrl = 200;
    pump_writes(client_wt, client_ctrl, server_wt, server_ctrl);

    FakeReadHandle srv_rh(server_ctrl);
    FakeWriteHandle srv_wh(server_ctrl);
    WT::BidiStreamHandle srv_bh{&srv_rh, &srv_wh};
    server->onNewBidiStream(srv_bh);
    // Read loop + service-after-inbound → SERVER_SETUP written
    WT_CHECK(!server_wt.writes.empty());

    // 3. Pump SERVER_SETUP → client's local control bidi read queue
    pump_writes(server_wt, server_ctrl, client_wt, client_ctrl);

    // 4. Client service → processes local bidi pending → reads
    //    SERVER_SETUP from waiting_reads → handshake complete
    client->service();

    // 5. Verify both sessions completed handshake
    moq_event_t ev;
    bool client_done = false, server_done = false;
    while (moq_session_poll_events(client_sess, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) client_done = true;
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(server_sess, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) server_done = true;
        moq_event_cleanup(&ev);
    }

    WT_CHECK(client_done);
    WT_CHECK(server_done);
    WT_CHECK(!client->is_terminal());
    WT_CHECK(!server->is_terminal());

    client.reset();
    server.reset();
    moq_session_destroy(client_sess);
    moq_session_destroy(server_sess);
    return failures;
}

/* ================================================================== */
/* Exception containment in noexcept handlers                         */
/* ================================================================== */

static int test_readStreamData_throws_marks_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());

    f.fake.throw_on_next_read = true;
    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);

    WT_CHECK(f.adapter->is_terminal());
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

/* ================================================================== */
/* service() returns correct result after read-induced fatal          */
/* ================================================================== */

static int test_service_returns_error_on_read_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init(MOQ_PERSPECTIVE_CLIENT));

    moq_session_start(f.session, 0);
    // Make the first readStreamData (for local control bidi) throw
    f.fake.throw_on_next_read = true;
    auto rc = f.adapter->service();

    WT_CHECK(f.adapter->is_terminal());
    WT_CHECK_EQ(rc, MOQ_ERR_INTERNAL);
    return failures;
}

/* ================================================================== */
/* Empty non-null IOBuf does not spin                                 */
/* ================================================================== */

static int test_empty_nonnull_iobuf_no_spin()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());

    // Queue a non-null but zero-length IOBuf (not FIN)
    f.fake.queueRead(500, "", 0, false);
    f.fake.queueRead(500, "real", 4, true);

    FakeReadHandle rh(500);
    f.adapter->onNewUniStream(&rh);

    // The empty read should stop the loop (waiting_reads_).
    // The real data is only consumed on next service().
    int after_init = f.fake.read_call_count;
    f.adapter->service();
    WT_CHECK(f.fake.read_call_count > after_init);
    return failures;
}

/* ================================================================== */
/* Loopback smoke tests via FakeWtPair                                */
/* ================================================================== */

static int test_loopback_handshake()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    if (!pair.init_ok) return failures;

    WT_CHECK(pair.wait_setup());
    WT_CHECK(!pair.has_fatal());
    return failures;
}

static int test_loopback_subscribe()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    if (!pair.init_ok) return failures;
    WT_CHECK(pair.wait_setup());
    WT_CHECK(!pair.has_fatal());

    // Drain setup events
    moq_event_t ev;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Client subscribes
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"wt", 2},
        {(const uint8_t *)"test", 4}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    moq_result_t rc = moq_session_subscribe(
        pair.client_session, &sc, 0, &client_sub);
    WT_CHECK(rc >= 0);
    if (rc < 0) return failures;

    pair.client->service();
    WT_CHECK(pair.pump_until());

    // Server should see SUBSCRIBE event
    bool sub_received = false;
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sub_received = true;
            server_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;
            moq_accept_subscribe_cfg_init(&acfg);
            moq_session_accept_subscribe(pair.server_session,
                                          server_sub, &acfg, 0);
        }
        moq_event_cleanup(&ev);
    }
    WT_CHECK(sub_received);

    // Pump the accept response
    WT_CHECK(pair.pump_until());

    // Client should see SUBSCRIBE_OK
    bool sub_ok = false;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) sub_ok = true;
        moq_event_cleanup(&ev);
    }
    WT_CHECK(sub_ok);
    WT_CHECK(!pair.has_fatal());
    return failures;
}

static int test_loopback_object_delivery()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    if (!pair.init_ok) return failures;
    WT_CHECK(pair.wait_setup());

    // Drain setup events
    moq_event_t ev;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Subscribe
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"wt", 2},
        {(const uint8_t *)"obj", 3}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)"audio", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    WT_CHECK(moq_session_subscribe(pair.client_session, &sc, 0,
                                    &client_sub) >= 0);
    pair.client->service();
    WT_CHECK(pair.pump_until());

    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            server_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;
            moq_accept_subscribe_cfg_init(&acfg);
            moq_session_accept_subscribe(pair.server_session,
                                          server_sub, &acfg, 0);
        }
        moq_event_cleanup(&ev);
    }
    WT_CHECK(moq_subscription_is_valid(server_sub));
    WT_CHECK(pair.pump_until());

    // Drain client subscribe OK
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Server publishes one object
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    WT_CHECK(moq_session_open_subgroup(pair.server_session, server_sub,
                                        &sgcfg, 0, &sg) >= 0);

    const char *payload = "hello-wt-loopback";
    moq_rcbuf_t *buf = nullptr;
    WT_CHECK(moq_rcbuf_create(moq_alloc_default(),
                               (const uint8_t *)payload,
                               std::strlen(payload), &buf) >= 0);
    WT_CHECK(moq_session_write_object(pair.server_session, sg, 0,
                                       buf, 0) >= 0);
    moq_rcbuf_decref(buf);
    WT_CHECK(moq_session_close_subgroup(pair.server_session, sg,
                                         0) >= 0);

    pair.server->service();
    WT_CHECK(pair.pump_until());

    // Client should receive the object
    bool obj_received = false;
    std::vector<uint8_t> received_payload;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.payload) {
            auto *d = moq_rcbuf_data(ev.u.object_received.payload);
            auto l = moq_rcbuf_len(ev.u.object_received.payload);
            received_payload.assign(d, d + l);
            obj_received = true;
        }
        moq_event_cleanup(&ev);
    }

    WT_CHECK(obj_received);
    if (obj_received) {
        std::vector<uint8_t> expected(payload,
                                       payload + std::strlen(payload));
        WT_CHECK(received_payload == expected);
    }
    WT_CHECK(!pair.has_fatal());
    return failures;
}

/* ================================================================== */
/* Endpoint-specific edge cases                                       */
/* ================================================================== */

static int test_local_control_read_error_is_fatal()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init(MOQ_PERSPECTIVE_CLIENT));

    // Queue an exception for the local control bidi (stream 100).
    // client_wt.next_stream_id starts at 100, so the first bidi is 100.
    f.fake.queueReadException(100, 0x77);

    moq_session_start(f.session, 0);
    f.adapter->service();

    WT_CHECK(f.adapter->is_terminal());
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

static int test_local_bidi_read_starts_exactly_once()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init(MOQ_PERSPECTIVE_CLIENT));

    int before = f.fake.read_call_count;
    moq_session_start(f.session, 0);
    f.adapter->service();

    // service() opens the local bidi, starts a read (1st call),
    // gets empty data → waiting_reads_, then resumes waiting (2nd call).
    int first_service_reads = f.fake.read_call_count - before;
    WT_CHECK(first_service_reads >= 1);

    // Subsequent service should only re-read once from waiting_reads_.
    int after = f.fake.read_call_count;
    f.adapter->service();
    WT_CHECK_EQ(f.fake.read_call_count - after, 1);
    return failures;
}

static int test_loopback_session_close_not_fatal()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    WT_CHECK(pair.wait_setup());

    // Clean close from server side
    pair.server->onSessionEnd(folly::none);
    WT_CHECK(pair.server->is_terminal());
    WT_CHECK(pair.server->is_closed());
    WT_CHECK(!pair.server->is_fatal());

    // Client is still alive (it hasn't received close yet)
    WT_CHECK(!pair.client->is_terminal());
    return failures;
}

static int test_loopback_multi_object_ordering()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    WT_CHECK(pair.wait_setup());

    moq_event_t ev;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Subscribe
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"wt", 2},
        {(const uint8_t *)"multi", 5}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    WT_CHECK(moq_session_subscribe(pair.client_session, &sc, 0,
                                    &client_sub) >= 0);
    pair.client->service();
    WT_CHECK(pair.pump_until());

    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            server_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;
            moq_accept_subscribe_cfg_init(&acfg);
            moq_session_accept_subscribe(pair.server_session,
                                          server_sub, &acfg, 0);
        }
        moq_event_cleanup(&ev);
    }
    WT_CHECK(moq_subscription_is_valid(server_sub));
    WT_CHECK(pair.pump_until());
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Publish 3 objects in one subgroup
    const char *payloads[] = {"alpha", "bravo", "charlie"};
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    WT_CHECK(moq_session_open_subgroup(pair.server_session, server_sub,
                                        &sgcfg, 0, &sg) >= 0);
    for (int i = 0; i < 3; i++) {
        moq_rcbuf_t *buf = nullptr;
        WT_CHECK(moq_rcbuf_create(moq_alloc_default(),
                                   (const uint8_t *)payloads[i],
                                   std::strlen(payloads[i]), &buf) >= 0);
        WT_CHECK(moq_session_write_object(pair.server_session, sg,
                                           (uint64_t)i, buf, 0) >= 0);
        moq_rcbuf_decref(buf);
    }
    WT_CHECK(moq_session_close_subgroup(pair.server_session, sg, 0) >= 0);
    pair.server->service();
    WT_CHECK(pair.pump_until());

    // Client should receive all 3 in order
    struct ObjResult { uint64_t obj_id; std::vector<uint8_t> data; };
    std::vector<ObjResult> results;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.payload) {
            auto *d = moq_rcbuf_data(ev.u.object_received.payload);
            auto l = moq_rcbuf_len(ev.u.object_received.payload);
            results.push_back({ev.u.object_received.object_id,
                               {d, d + l}});
        }
        moq_event_cleanup(&ev);
    }
    WT_CHECK_EQ((int)results.size(), 3);
    for (int i = 0; i < 3 && i < (int)results.size(); i++) {
        WT_CHECK_EQ((int)results[i].obj_id, i);
        std::vector<uint8_t> exp(payloads[i],
                                  payloads[i] + std::strlen(payloads[i]));
        WT_CHECK(results[i].data == exp);
    }
    WT_CHECK(!pair.has_fatal());
    return failures;
}

static int test_loopback_payload_integrity()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    WT_CHECK(pair.wait_setup());

    moq_event_t ev;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Subscribe
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"wt", 2},
        {(const uint8_t *)"chain", 5}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)"c", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    WT_CHECK(moq_session_subscribe(pair.client_session, &sc, 0,
                                    &client_sub) >= 0);
    pair.client->service();
    WT_CHECK(pair.pump_until());

    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(pair.server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            server_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acfg;
            moq_accept_subscribe_cfg_init(&acfg);
            moq_session_accept_subscribe(pair.server_session,
                                          server_sub, &acfg, 0);
        }
        moq_event_cleanup(&ev);
    }
    WT_CHECK(pair.pump_until());
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Publish object with known payload through the adapter path.
    // The chained IOBuf coalescing is tested by the fake pair's
    // cloneCoalesced() in deliver. If coalescing is broken, the
    // payload will be truncated or corrupt.
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    WT_CHECK(moq_session_open_subgroup(pair.server_session, server_sub,
                                        &sgcfg, 0, &sg) >= 0);
    const char *payload = "chained-data-test-payload";
    moq_rcbuf_t *buf = nullptr;
    WT_CHECK(moq_rcbuf_create(moq_alloc_default(),
                               (const uint8_t *)payload,
                               std::strlen(payload), &buf) >= 0);
    WT_CHECK(moq_session_write_object(pair.server_session, sg, 0,
                                       buf, 0) >= 0);
    moq_rcbuf_decref(buf);
    WT_CHECK(moq_session_close_subgroup(pair.server_session, sg, 0) >= 0);
    pair.server->service();
    WT_CHECK(pair.pump_until());

    bool received = false;
    std::vector<uint8_t> got;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.payload) {
            auto *d = moq_rcbuf_data(ev.u.object_received.payload);
            auto l = moq_rcbuf_len(ev.u.object_received.payload);
            got.assign(d, d + l);
            received = true;
        }
        moq_event_cleanup(&ev);
    }
    WT_CHECK(received);
    if (received) {
        std::vector<uint8_t> exp(payload, payload + std::strlen(payload));
        WT_CHECK(got == exp);
    }
    WT_CHECK(!pair.has_fatal());
    return failures;
}

/* ================================================================== */
/* Post-conformance review fixes                                      */
/* ================================================================== */

static int test_client_peer_bidi_not_control()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init(MOQ_PERSPECTIVE_CLIENT));

    // Peer opens a bidi stream. For a client, peer bidi is NOT
    // control (server doesn't open control). Should be BIDI.
    // Queue empty data (no exception) — the read loop stops at
    // empty non-FIN and the stream is classified without error.
    FakeReadHandle rh(300);
    FakeWriteHandle wh(300);
    WT::BidiStreamHandle bh{&rh, &wh};
    f.adapter->onNewBidiStream(bh);

    // Non-control bidi should not be fatal.
    WT_CHECK(!f.adapter->is_fatal());
    return failures;
}

static int test_close_then_error_stays_closed()
{
    int failures = 0;
    AdapterFixture f;
    WT_CHECK(f.init());

    // Clean close first
    f.adapter->onSessionEnd(folly::none);
    WT_CHECK(f.adapter->is_closed());
    WT_CHECK(!f.adapter->is_fatal());

    // A late error-inducing event after close (here: a null uni-stream
    // handle, which would normally mark the adapter fatal) must be
    // ignored once terminal - closed must not flip to fatal.
    f.adapter->onNewUniStream(nullptr);
    WT_CHECK(f.adapter->is_closed());
    WT_CHECK(!f.adapter->is_fatal());

    // service() should return OK, not ERR_INTERNAL
    WT_CHECK_EQ(f.adapter->service(), MOQ_OK);
    return failures;
}

static int test_loopback_close_delivered_to_peer()
{
    int failures = 0;
    FakeWtPair pair;
    WT_CHECK(pair.init_ok);
    WT_CHECK(pair.wait_setup());

    // Manually place a close in server_wt.closes to test the
    // FakeWtPair close delivery path (deliver → onSessionEnd).
    pair.server_wt.closes.push_back({folly::Optional<uint32_t>(0x42)});
    WT_CHECK(pair.pump_until());

    // Client should observe the close via onSessionEnd
    WT_CHECK(pair.client->is_closed());
    WT_CHECK_EQ(pair.client->close_code(),
                0x42u);
    return failures;
}

static int test_dual_virtual_time_pair_fails()
{
    int failures = 0;
    uint64_t t1 = 0, t2 = 0;
    FakeWtPair p1(&t1);
    WT_CHECK(p1.init_ok);

    FakeWtPair p2(&t2);
    // p2 should fail init because p1 holds the virtual-time slot
    WT_CHECK(!p2.init_ok);
    return failures;
}

/* ================================================================== */
/* Throwing now_us: must mark fatal, never std::terminate              */
/* ================================================================== */

static bool g_now_should_throw = false;

static uint64_t throwing_now_us()
{
    if (g_now_should_throw)
        throw std::runtime_error("now_us exploded");
    return test_time;
}

struct ThrowingNowFixture {
    FakeWebTransport fake;
    moq_session_t *session = nullptr;
    std::unique_ptr<Adapter> adapter;

    bool init(moq_perspective_t perspective = MOQ_PERSPECTIVE_SERVER)
    {
        g_now_should_throw = false;
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), perspective);
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 10;
        if (moq_session_create(&cfg, 0, &session) < 0)
            return false;

        Adapter::Config acfg{};
        acfg.session = session;
        acfg.alloc = moq_alloc_default();
        acfg.executor = &folly::InlineExecutor::instance();
        acfg.now_us = throwing_now_us;

        adapter = Adapter::create(acfg, &fake);
        return adapter != nullptr;
    }

    ~ThrowingNowFixture()
    {
        g_now_should_throw = false;
        adapter.reset();
        if (session) moq_session_destroy(session);
    }
};

static int test_throwing_now_during_service()
{
    int failures = 0;
    ThrowingNowFixture f;
    WT_CHECK(f.init(MOQ_PERSPECTIVE_CLIENT));

    moq_session_start(f.session, 0);
    g_now_should_throw = true;
    auto rc = f.adapter->service();

    WT_CHECK(f.adapter->is_terminal());
    WT_CHECK_EQ(rc, MOQ_ERR_INTERNAL);
    return failures;
}

static int test_throwing_now_during_noexcept_callback()
{
    int failures = 0;
    ThrowingNowFixture f;
    WT_CHECK(f.init());

    const uint8_t payload[] = {0x01, 0x02};
    f.fake.queueRead(700, payload, sizeof(payload), false);
    g_now_should_throw = true;
    FakeReadHandle rh(700);
    f.adapter->onNewUniStream(&rh);

    WT_CHECK(f.adapter->is_terminal());
    WT_CHECK(f.adapter->is_fatal());
    return failures;
}

/* ================================================================== */
/* Read-loop: do not pause when service() already cleared pending      */
/* ================================================================== */

/*
 * Regression: onReadData() must decide whether to pause based on the
 * bridge's pending state AFTER its synchronous service(), not on the
 * stale WOULD_BLOCK return from before service().
 *
 * Setup: the client session is given a tiny action queue, which we fill
 * with un-serviced subscribes. We then deliver a uni stream carrying a
 * subgroup header for an unknown track alias: the session wants to STOP
 * the stream, but the full action queue makes that delivery return
 * WOULD_BLOCK with the stream retained, so the bridge marks it pending.
 * The service() inside onReadData drains the queued subscribes and the
 * STOP retry succeeds, clearing the pending state in the SAME callback.
 *
 * Pausing on a stale rc would park the stream in paused_reads_ and leave the
 * queued follow-up read unconsumed until some external service(); instead the
 * follow-up read must be issued immediately.
 */
static int test_read_resumes_when_service_clears_pending()
{
    int failures = 0;
    FakeWtPair pair(nullptr, /*client_max_data_streams=*/0,
                    /*client_max_actions=*/2);
    WT_CHECK(pair.init_ok);
    if (!pair.init_ok) return failures;
    WT_CHECK(pair.wait_setup());
    WT_CHECK(!pair.has_fatal());

    moq_event_t ev;
    while (moq_session_poll_events(pair.client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    // Fill the client's action queue WITHOUT servicing: each subscribe
    // queues one action and the adapter is not pumped, so they sit in
    // the session's action queue (max_actions=2 -> full after two).
    for (int i = 0; i < 2; i++) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[] = {
            {(const uint8_t *)"bp", 2},
            {(const uint8_t *)"t", 1}
        };
        sc.track_namespace.parts = ns;
        sc.track_namespace.count = 2;
        char name[8];
        std::snprintf(name, sizeof(name), "v%d", i);
        sc.track_name = {(const uint8_t *)name, std::strlen(name)};
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_session_subscribe(pair.client_session, &sc, 0, &sub);
        // Intentionally no client->service() here.
    }

    // Deliver a uni stream carrying a complete subgroup header for an
    // UNKNOWN track alias (50, not one of the just-created subscriptions).
    // Type 0x10 = subgroup, id-mode ZERO (no subgroup-id field), explicit
    // priority: [type][track_alias][group_id][publisher_priority].
    // The session takes a data-stream slot, parses the header, finds the
    // alias unknown, and wants to STOP the stream - but the full action
    // queue makes that return WOULD_BLOCK with the stream RETAINED. The
    // service() inside onReadData drains the queued subscribes, the STOP
    // retry succeeds, and the stream's pending state clears in the SAME
    // callback. A follow-up (empty) read is queued: the adapter must
    // re-check pending and consume it immediately rather than park the
    // stream in paused_reads_ and leave the read unconsumed.
    const uint64_t blocked_id = 600;
    const uint8_t hdr[] = { 0x10, 0x32, 0x00, 0x00 };
    pair.client_wt.queueRead(blocked_id, hdr, sizeof(hdr), false);
    pair.client_wt.queueRead(blocked_id, "", 0, false);  // benign follow-up
    FakeReadHandle rh_blocked(blocked_id);
    pair.client->onNewUniStream(&rh_blocked);

    // The follow-up read must have been consumed in the same callback -
    // proving the stream was NOT left paused after service() cleared the
    // pending state. No external adapter.service() was called.
    WT_CHECK(pair.client_wt.read_queues[blocked_id].empty());
    WT_CHECK(!pair.client->is_terminal());
    return failures;
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */

int main()
{
    int failures = 0;

    // Creation / service / session lifecycle
    failures += test_create_ok();
    failures += test_create_null_session();
    failures += test_create_null_wt();
    failures += test_create_null_alloc();
    failures += test_create_null_executor();
    failures += test_service_drives_bridge();
    failures += test_datagram_forwarded_no_crash();
    failures += test_session_end_clean();
    failures += test_session_end_with_error();
    failures += test_session_end_error_is_close_not_fatal();
    failures += test_session_drain_no_op();
    failures += test_null_uni_handle_fatal();
    failures += test_null_bidi_read_handle_fatal();
    failures += test_handlers_noop_after_close();
    failures += test_service_after_close();

    // Read-loop mechanics
    failures += test_uni_read_issues_read();
    failures += test_fin_stops_read_loop();
    failures += test_read_error_immediate();
    failures += test_read_future_exception();
    failures += test_terminal_adapter_no_reads();
    failures += test_callback_after_destroy_safe();
    failures += test_stream_state_cleaned_on_fin();
    failures += test_stream_state_cleaned_on_error();

    // Control-stream error
    failures += test_peer_control_read_error_is_fatal();

    // Two-adapter handshake
    failures += test_handshake_through_adapters();

    // Exception containment
    failures += test_readStreamData_throws_marks_fatal();
    failures += test_service_returns_error_on_read_fatal();

    // Empty IOBuf
    failures += test_empty_nonnull_iobuf_no_spin();

    // Loopback smoke tests
    failures += test_loopback_handshake();
    failures += test_loopback_subscribe();
    failures += test_loopback_object_delivery();

    // Endpoint-specific edge cases
    failures += test_local_control_read_error_is_fatal();
    failures += test_local_bidi_read_starts_exactly_once();
    failures += test_loopback_session_close_not_fatal();
    failures += test_loopback_multi_object_ordering();
    failures += test_loopback_payload_integrity();

    // Post-conformance review fixes
    failures += test_client_peer_bidi_not_control();
    failures += test_close_then_error_stays_closed();
    failures += test_loopback_close_delivered_to_peer();
    failures += test_dual_virtual_time_pair_fails();

    // Throwing now_us safety
    failures += test_throwing_now_during_service();
    failures += test_throwing_now_during_noexcept_callback();

    // Read-loop pause/resume after synchronous service
    failures += test_read_resumes_when_service_clears_pending();

    if (failures == 0)
        std::printf("test_wt_adapter: all 43 tests passed\n");
    else
        std::fprintf(stderr, "test_wt_adapter: %d failure(s)\n", failures);

    return failures;
}
