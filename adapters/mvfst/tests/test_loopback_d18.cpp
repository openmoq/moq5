/*
 * Draft-18 MoQ over a REAL mvfst client/server loopback: symmetric
 * uni-control establishment, then subscribe + object delivery.
 *
 * The client's first request bidi is QUIC stream 0 -- the id the draft-16
 * model treats as the MoQ control stream -- so the subscribe round pins the
 * adapter's mode-aware bidi routing on both sides; the object pins uni data
 * classification alongside the uni control pair.
 *
 * Separate binary from test_loopback because loopback_pair is limited to
 * one QuicServer per process.
 */

#include "support/mvfst_loopback_pair.hpp"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace moq::mvfst::test;

static int failures = 0;
#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, \
                     #expr); \
        failures++; \
    } } while (0)

static void drain_client_events(loopback_pair &lp) {
    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(lp.client_session, ev, 16,
                                sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) moq_event_cleanup(&ev[i]);
}

static void test_d18_loopback()
{
    loopback_pair lp(MOQ_VERSION_DRAFT_18);
    MVFST_CHECK(lp.init_ok);
    if (!lp.init_ok) return;

    /* Symmetric establish: both ends reach SETUP_COMPLETE. */
    MVFST_CHECK(lp.wait_setup());
    MVFST_CHECK(!lp.has_error());
    if (lp.has_error() || !lp.ss.setup_complete.load()) return;
    drain_client_events(lp);

    /* Subscribe: the client opens a request bidi (QUIC stream 0). The
     * server's pump auto-accepts when the names match. */
    lp.ss.expect_sub_ns1 = "mvfst";
    lp.ss.expect_sub_ns2 = "d18";
    lp.ss.expect_sub_track = "video";

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {
        {(const uint8_t *)"mvfst", 5},
        {(const uint8_t *)"d18", 3}
    };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub;
    MVFST_CHECK(moq_session_subscribe(lp.client_session, &sc, 0, &sub) >= 0);
    lp.pump_client();

    std::atomic<bool> sub_ok{false};
    MVFST_CHECK(lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_OK)
                sub_ok.store(true);
            moq_event_cleanup(&ev[i]);
        }
        return sub_ok.load();
    }));
    MVFST_CHECK(lp.ss.sub_accepted.load());
    MVFST_CHECK(moq_subscription_is_valid(lp.ss.sub_handle));
    MVFST_CHECK(!lp.has_error());
    if (!lp.ss.sub_accepted.load()) return;

    /* Server publishes one object in a subgroup (uni data stream). */
    const std::string p1 = "hello-d18";
    std::atomic<bool> w1_ok{false};
    moq_subscription_t ssub = lp.ss.sub_handle;
    lp.run_on_server([&]() {
        moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0; sgcfg.publisher_priority = 200;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(lp.ss.session, ssub,
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
    MVFST_CHECK(!lp.has_error());

    /* Regression: extension-bearing objects use the
     * two-action write_object_ex path -- header+properties, then
     * payload-prefix+payload -- whose payload phase goes through the adapter's
     * write_payload (CAP_WRITE_PAYLOAD, zero-copy IOBuf::takeOwnership). Several
     * such objects on one subgroup stream interleave write() (header/prefix
     * copies) and write_payload() calls; a zero-copy framing bug would surface
     * as a phantom/duplicate object or wrong bytes. Assert EXACTLY N objects,
     * each byte-exact in payload + properties, with no late extra object. */
    {
        const int kN = 4;
        /* one even-type Key-Value-Pair (draft-18 §1.4.3): vi64(2), vi64(0x20) */
        const uint8_t props_bytes[] = { 0x02, 0x20 };
        std::vector<std::vector<uint8_t>> sent((size_t)kN);
        for (int k = 0; k < kN; k++) {
            sent[(size_t)k].resize(24 + (size_t)k * 40000);   /* multi-packet */
            for (size_t i = 0; i < sent[(size_t)k].size(); i++)
                sent[(size_t)k][i] = (uint8_t)((k * 31 + (int)i) & 0xFF);
        }
        std::atomic<bool> w2_ok{false};
        lp.run_on_server([&]() {
            moq_subgroup_cfg_t sgcfg; moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 1; sgcfg.publisher_priority = 200;
            sgcfg.object_properties = true;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(lp.ss.session, ssub, &sgcfg, 0, &sg) < 0)
                { lp.ss.error.store(true); return; }
            for (int k = 0; k < kN; k++) {
                moq_rcbuf_t *pay = nullptr, *props = nullptr;
                if (moq_rcbuf_create(moq_alloc_default(), sent[(size_t)k].data(),
                        sent[(size_t)k].size(), &pay) < 0)
                    { lp.ss.error.store(true); return; }
                if (moq_rcbuf_create(moq_alloc_default(), props_bytes,
                        sizeof(props_bytes), &props) < 0)
                    { moq_rcbuf_decref(pay); lp.ss.error.store(true); return; }
                moq_object_cfg_t oc; memset(&oc, 0, sizeof(oc));
                oc.struct_size = sizeof(oc); oc.object_id = (uint64_t)k;
                oc.payload = pay; oc.properties = props;
                int rc = (int)moq_session_write_object_ex(lp.ss.session, sg, &oc, 0);
                moq_rcbuf_decref(pay); moq_rcbuf_decref(props);
                if (rc < 0) { lp.ss.error.store(true); return; }
            }
            if (moq_session_close_subgroup(lp.ss.session, sg, 0) < 0)
                { lp.ss.error.store(true); return; }
            lp.service_server();
            w2_ok.store(true);
        });
        MVFST_CHECK(w2_ok.load());

        std::vector<std::vector<uint8_t>> got((size_t)kN), gp((size_t)kN);
        std::vector<bool> seen((size_t)kN, false);
        int obj_count = 0;        /* total group-1 OBJECT_RECEIVED events */
        bool dup = false;         /* an object id observed more than once */
        bool oob = false;         /* an object id outside 0..kN-1 */

        /* Drain currently-available group-1 object events into the trackers. */
        auto drain_g1 = [&]() {
            moq_event_t ev[16]; size_t ne;
            moq_session_poll_events_ex(lp.client_session, ev, 16,
                                        sizeof(moq_event_t), &ne);
            for (size_t i = 0; i < ne; i++) {
                if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                    ev[i].u.object_received.group_id == 1) {
                    obj_count++;
                    auto &o = ev[i].u.object_received;
                    uint64_t oid = o.object_id;
                    if (oid >= (uint64_t)kN) {
                        oob = true;
                    } else {
                        if (seen[oid]) dup = true;
                        seen[oid] = true;
                        if (o.payload) { auto *d = moq_rcbuf_data(o.payload);
                            got[oid].assign(d, d + moq_rcbuf_len(o.payload)); }
                        if (o.properties) { auto *d = moq_rcbuf_data(o.properties);
                            gp[oid].assign(d, d + moq_rcbuf_len(o.properties)); }
                    }
                }
                moq_event_cleanup(&ev[i]);
            }
        };

        /* Wait until ALL expected ids 0..kN-1 have been delivered (not merely
         * until kN events arrived -- an interleaved phantom could otherwise
         * satisfy a bare count check while a real id is still missing). */
        MVFST_CHECK(lp.wait_for([&]() {
            drain_g1();
            for (int k = 0; k < kN; k++) if (!seen[(size_t)k]) return false;
            return true;
        }));

        /* Then keep pumping a bounded while and assert NOTHING else arrives on
         * group 1 -- no late extra object, no duplicate id, no out-of-range id.
         * (Predicate returns false so wait_for pumps both ends for the full
         * window; the timeout return is expected and ignored.) */
        (void)lp.wait_for([&]() { drain_g1(); return false; }, 300);

        MVFST_CHECK(obj_count == kN);   /* EXACTLY N: no phantom/extra/dup */
        MVFST_CHECK(!dup);
        MVFST_CHECK(!oob);
        for (int k = 0; k < kN; k++) {
            MVFST_CHECK(got[(size_t)k] == sent[(size_t)k]);
            MVFST_CHECK(gp[(size_t)k] == std::vector<uint8_t>(
                props_bytes, props_bytes + sizeof(props_bytes)));
        }
        MVFST_CHECK(!lp.has_error());
    }
}

int main()
{
    test_d18_loopback();
    if (failures == 0)
        std::printf("PASS: mvfst_loopback_d18\n");
    return failures != 0;
}
