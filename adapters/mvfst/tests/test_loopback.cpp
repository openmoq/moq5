/*
 * mvfst loopback integration tests using MvfstPair harness.
 *
 * Five scenarios exercising the attach adapter over real QUIC:
 *   1. Setup handshake
 *   2. Subscribe + single object
 *   3. Multi-object subgroup
 *   4. Namespace publish lifecycle
 *   5. Datagram object
 *
 * Timer/deadline coverage: attach-mode timer integration (GOAWAY
 * drain timeout driving session tick via adapter service_all) is
 * tested by test_managed_goaway, which exercises the full path
 * through real mvfst. An attach-only timer test is not included
 * because loopback_pair is limited to one QuicServer per process
 * and the managed test already validates the adapter timer path.
 */

#include "support/mvfst_loopback_pair.hpp"

#include <cstdio>
#include <cstring>
#include <vector>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define MVFST_CHECK_RC(expr) do { \
    moq_result_t _rc = (expr); \
    if (_rc < 0) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s returned %d\n", \
                __FILE__, __LINE__, #expr, _rc); \
        failures++; \
    } \
} while (0)

using namespace moq::mvfst::test;

/* -- Helpers --------------------------------------------------------- */

static void drain_client_events(loopback_pair &lp) {
    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(lp.client_session, ev, 16,
                                sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) moq_event_cleanup(&ev[i]);
}

static moq_subscription_t setup_and_subscribe(
    loopback_pair &lp, const char *ns1, const char *ns2,
    const char *track)
{
    const moq_subscription_t invalid = MOQ_SUBSCRIPTION_INVALID;

    if (!lp.wait_setup()) {
        MVFST_CHECK(false && "wait_setup failed");
        return invalid;
    }
    drain_client_events(lp);

    lp.ss.expect_sub_ns1 = ns1;
    lp.ss.expect_sub_ns2 = ns2;
    lp.ss.expect_sub_track = track;

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)ns1, std::strlen(ns1)},
        {(const uint8_t *)ns2, std::strlen(ns2)}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)track, std::strlen(track)};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    moq_result_t rc = moq_session_subscribe(
        lp.client_session, &sc, 0, &client_sub);
    if (rc < 0) {
        MVFST_CHECK(false && "moq_session_subscribe failed");
        return invalid;
    }
    lp.pump_client();

    std::atomic<bool> sub_ok{false};
    bool wait_ok = lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_OK)
                sub_ok.store(true);
            moq_event_cleanup(&ev[i]);
        }
        return sub_ok.load();
    });
    if (!wait_ok) {
        MVFST_CHECK(false && "wait_for SUBSCRIBE_OK failed");
        return invalid;
    }
    if (!lp.ss.sub_accepted.load()) {
        MVFST_CHECK(false && "server did not accept subscribe");
        return invalid;
    }
    MVFST_CHECK(moq_subscription_is_valid(lp.ss.sub_handle));
    MVFST_CHECK(!lp.has_error());
    return lp.ss.sub_handle;
}

/* ================================================================== */
/* Test: all five scenarios in one env                                 */
/* ================================================================== */

static void test_loopback_scenarios()
{
    loopback_pair lp;
    MVFST_CHECK(lp.init_ok);
    if (!lp.init_ok) return;

    /* ---- Phase 1+2: setup + subscribe + single object ---------------- */

    moq_subscription_t sub = setup_and_subscribe(
        lp, "mvfst", "loopback", "video");
    MVFST_CHECK(moq_subscription_is_valid(sub));
    if (!moq_subscription_is_valid(sub)) return;
    MVFST_CHECK(!lp.has_error());

    const std::string p1 = "hello-mvfst";
    std::atomic<bool> w1_ok{false};
    lp.run_on_server([&]() {
        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0; sgcfg.publisher_priority = 200;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(lp.ss.session, sub,
                                       &sgcfg, 0, &sg) < 0)
            { lp.ss.error.store(true); return; }
        moq_rcbuf_t *buf = nullptr;
        if (moq_rcbuf_create(moq_alloc_default(),
                reinterpret_cast<const uint8_t *>(p1.data()),
                p1.size(), &buf) < 0)
            { lp.ss.error.store(true); return; }
        if (moq_session_write_object(lp.ss.session, sg, 0, buf, 0) < 0)
            { moq_rcbuf_decref(buf); lp.ss.error.store(true); return; }
        moq_rcbuf_decref(buf);
        if (moq_session_close_subgroup(lp.ss.session, sg, 0) < 0)
            { lp.ss.error.store(true); return; }
        lp.service_server();
        w1_ok.store(true);
    });
    MVFST_CHECK(w1_ok.load());

    std::vector<uint8_t> r1;
    MVFST_CHECK(lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev[i].u.object_received.payload) {
                auto *d = moq_rcbuf_data(ev[i].u.object_received.payload);
                auto l = moq_rcbuf_len(ev[i].u.object_received.payload);
                r1.assign(d, d + l);
            }
            moq_event_cleanup(&ev[i]);
        }
        return !r1.empty();
    }));
    MVFST_CHECK(r1 == std::vector<uint8_t>(p1.begin(), p1.end()));

    /* ---- Phase 3: multi-object subgroup ---------------------------- */

    const std::vector<std::string> payloads = {"one", "two", "three"};
    std::atomic<bool> w2_ok{false};
    lp.run_on_server([&]() {
        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 1; sgcfg.publisher_priority = 200;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(lp.ss.session, sub,
                                       &sgcfg, 0, &sg) < 0)
            { lp.ss.error.store(true); return; }
        for (size_t j = 0; j < payloads.size(); j++) {
            moq_rcbuf_t *buf = nullptr;
            if (moq_rcbuf_create(moq_alloc_default(),
                    reinterpret_cast<const uint8_t *>(payloads[j].data()),
                    payloads[j].size(), &buf) < 0)
                { lp.ss.error.store(true); return; }
            if (moq_session_write_object(lp.ss.session, sg,
                    static_cast<uint64_t>(j), buf, 0) < 0)
                { moq_rcbuf_decref(buf); lp.ss.error.store(true); return; }
            moq_rcbuf_decref(buf);
        }
        if (moq_session_close_subgroup(lp.ss.session, sg, 0) < 0)
            { lp.ss.error.store(true); return; }
        lp.service_server();
        w2_ok.store(true);
    });
    MVFST_CHECK(w2_ok.load());

    struct recv_obj { uint64_t g, o; std::vector<uint8_t> pl; };
    std::vector<recv_obj> multi;
    MVFST_CHECK(lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                auto &o = ev[i].u.object_received;
                recv_obj ro;
                ro.g = o.group_id; ro.o = o.object_id;
                if (o.payload) {
                    auto *d = moq_rcbuf_data(o.payload);
                    auto l = moq_rcbuf_len(o.payload);
                    ro.pl.assign(d, d + l);
                }
                multi.push_back(std::move(ro));
            }
            moq_event_cleanup(&ev[i]);
        }
        return multi.size() >= payloads.size();
    }));
    MVFST_CHECK(multi.size() == payloads.size());
    if (multi.size() == payloads.size()) {
        for (size_t j = 0; j < payloads.size(); j++) {
            MVFST_CHECK(multi[j].g == 1);
            MVFST_CHECK(multi[j].o == j);
            std::vector<uint8_t> exp(payloads[j].begin(), payloads[j].end());
            MVFST_CHECK(multi[j].pl == exp);
        }
    }

    /* ---- Phase 4: namespace publish lifecycle ---------------------- */

    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    moq_bytes_t pub_ns[] = {
        {(const uint8_t *)"mvfst", 5},
        {(const uint8_t *)"pub", 3}
    };
    pcfg.track_namespace.parts = pub_ns;
    pcfg.track_namespace.count = 2;
    moq_announcement_t client_ann;
    MVFST_CHECK_RC(moq_session_publish_namespace(
        lp.client_session, &pcfg, 0, &client_ann));
    lp.pump_client();

    MVFST_CHECK(lp.wait_for([&]() { return lp.ss.ns_accepted.load(); }));

    std::atomic<bool> ns_accepted{false};
    MVFST_CHECK(lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_NAMESPACE_ACCEPTED &&
                moq_announcement_eq(ev[i].u.namespace_accepted.ann,
                                     client_ann))
                ns_accepted.store(true);
            moq_event_cleanup(&ev[i]);
        }
        return ns_accepted.load();
    }));

    MVFST_CHECK_RC(moq_session_publish_namespace_done(
        lp.client_session, client_ann, 0));
    lp.pump_client();

    MVFST_CHECK(lp.wait_for([&]() { return lp.ss.ns_done.load(); }));

    /* ---- Phase 5: datagram object ---------------------------------- */

    const std::string dg_payload = "datagram-mvfst";
    std::atomic<bool> dg_ok{false};
    lp.run_on_server([&]() {
        moq_rcbuf_t *buf = nullptr;
        if (moq_rcbuf_create(moq_alloc_default(),
                reinterpret_cast<const uint8_t *>(dg_payload.data()),
                dg_payload.size(), &buf) < 0)
            { lp.ss.error.store(true); return; }
        moq_result_t rc = moq_session_send_object_datagram(
            lp.ss.session, sub, 9, 7, 200, false, buf, nullptr, 0, 0);
        moq_rcbuf_decref(buf);
        if (rc < 0) { lp.ss.error.store(true); return; }
        lp.service_server();
        dg_ok.store(true);
    });
    MVFST_CHECK(dg_ok.load());

    struct dg_result { uint64_t g = 0, o = 0; bool dg = false;
                        std::vector<uint8_t> pl; };
    dg_result dgr;
    std::atomic<bool> dg_recv{false};
    MVFST_CHECK(lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                auto &o = ev[i].u.object_received;
                dgr.g = o.group_id; dgr.o = o.object_id;
                dgr.dg = o.datagram;
                if (o.payload) {
                    auto *d = moq_rcbuf_data(o.payload);
                    auto l = moq_rcbuf_len(o.payload);
                    dgr.pl.assign(d, d + l);
                }
                dg_recv.store(true);
            }
            moq_event_cleanup(&ev[i]);
        }
        return dg_recv.load();
    }));

    MVFST_CHECK(dgr.g == 9);
    MVFST_CHECK(dgr.o == 7);
    MVFST_CHECK(dgr.dg);
    MVFST_CHECK(dgr.pl == std::vector<uint8_t>(dg_payload.begin(), dg_payload.end()));

    /* ---- Final checks ---------------------------------------------- */

    MVFST_CHECK(!lp.has_error());
}

int main()
{
    test_loopback_scenarios();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
