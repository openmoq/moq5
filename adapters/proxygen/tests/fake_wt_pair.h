#ifndef MOQ_FAKE_WT_PAIR_H
#define MOQ_FAKE_WT_PAIR_H

/*
 * Paired FakeWebTransport for loopback and conformance testing.
 *
 * Connects two Adapters via fake WebTransport: writes on one side
 * are delivered as reads on the other. Automatically fires
 * onNewBidiStream/onNewUniStream when data arrives on a stream
 * the peer hasn't seen yet.
 *
 * Deterministic — no threads, no real networking.
 * Reusable for conformance via moq_adapter_pair_ops_t.
 */

#include "fake_webtransport.h"
#include <moq/proxygen_wt.hpp>
#include <moq/moq.h>

#include <folly/executors/InlineExecutor.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <unordered_set>

namespace moq::wt::testing {

struct FakeWtPair {
    FakeWebTransport client_wt;
    FakeWebTransport server_wt;
    moq_session_t *client_session = nullptr;
    moq_session_t *server_session = nullptr;
    std::unique_ptr<Adapter> client;
    std::unique_ptr<Adapter> server;

    std::unordered_set<uint64_t> server_known_streams;
    std::unordered_set<uint64_t> client_known_streams;

    std::vector<std::unique_ptr<FakeReadHandle>> peer_read_handles;
    std::vector<std::unique_ptr<FakeWriteHandle>> peer_write_handles;

    uint64_t *time_ptr = nullptr;
    bool init_ok = false;

    struct TimeGuard {
        uint64_t **slot;
        bool armed;
        TimeGuard(uint64_t **s, uint64_t *val)
            : slot(s), armed(false) {
            if (val) { *slot = val; armed = true; }
        }
        ~TimeGuard() { if (armed) { *slot = nullptr; armed = false; } }
        void dismiss() { armed = false; }
    };

    // client_max_data_streams / client_max_actions: 0 = library default.
    // Used by backpressure tests that must force inbound WOULD_BLOCK on
    // the client by exhausting its data-stream pool and action queue.
    explicit FakeWtPair(uint64_t *external_time = nullptr,
                        uint32_t client_max_data_streams = 0,
                        uint32_t client_max_actions = 0)
    {
        time_ptr = external_time;

        if (time_ptr && s_time_ptr) {
            // Only one virtual-time pair at a time. Fail cleanly.
            return;
        }

        // RAII: if construction throws after this, s_time_ptr is cleared.
        TimeGuard tg(&s_time_ptr, time_ptr);

        client_wt.next_stream_id = 100;
        server_wt.next_stream_id = 200;

        moq_session_cfg_t ccfg;
        moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(),
                             MOQ_PERSPECTIVE_CLIENT);
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 64;
        if (client_max_data_streams)
            ccfg.max_data_streams = client_max_data_streams;
        if (client_max_actions)
            ccfg.max_actions = client_max_actions;
        if (moq_session_create(&ccfg, 0, &client_session) < 0)
            return;

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                             MOQ_PERSPECTIVE_SERVER);
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 64;
        scfg.goaway_timeout_us = 1000;
        if (moq_session_create(&scfg, 0, &server_session) < 0) {
            moq_session_destroy(client_session);
            client_session = nullptr;
            return;
        }

        Adapter::Config cacfg{};
        cacfg.session = client_session;
        cacfg.alloc = moq_alloc_default();
        cacfg.executor = &folly::InlineExecutor::instance();
        cacfg.now_us = time_ptr ? trampoline : default_now;
        client = Adapter::create(cacfg, &client_wt);
        if (!client) return;

        Adapter::Config sacfg{};
        sacfg.session = server_session;
        sacfg.alloc = moq_alloc_default();
        sacfg.executor = &folly::InlineExecutor::instance();
        sacfg.now_us = time_ptr ? trampoline : default_now;
        server = Adapter::create(sacfg, &server_wt);
        if (!server) return;

        tg.dismiss();
        init_ok = true;
    }

    ~FakeWtPair()
    {
        if (s_time_ptr == time_ptr) s_time_ptr = nullptr;
        client.reset();
        server.reset();
        if (client_session) moq_session_destroy(client_session);
        if (server_session) moq_session_destroy(server_session);
    }

    FakeWtPair(const FakeWtPair &) = delete;
    FakeWtPair &operator=(const FakeWtPair &) = delete;

    /* -- Time -------------------------------------------------------- */

    // Adapter::Config::now_us is a plain function pointer (no context),
    // so the trampoline must read from a global. Only one FakeWtPair
    // may use virtual time at a time. The constructor guards this.
    static inline uint64_t *s_time_ptr = nullptr;
    static uint64_t trampoline() {
        return s_time_ptr ? *s_time_ptr : 0;
    }
    static uint64_t default_now() { return 0; }

    void set_time(uint64_t t) {
        if (time_ptr) *time_ptr = t;
    }

    uint64_t get_time() const {
        return time_ptr ? *time_ptr : 0;
    }

    /* -- Deliver ------------------------------------------------------ */

    size_t deliver(FakeWebTransport &src,
                   Adapter &dst,
                   std::unordered_set<uint64_t> &dst_known)
    {
        auto &dst_wt = (&dst == &*server) ? server_wt : client_wt;

        struct NewStream { uint64_t id; bool is_bidi; };
        std::vector<NewStream> new_streams;
        size_t count = 0;

        for (auto &w : src.writes) {
            if (!w.data && !w.fin)
                continue;
            uint64_t id = w.stream_id;

            bool dst_local = dst_wt.bidi_stream_ids.count(id) > 0 ||
                             dst_wt.uni_stream_ids.count(id) > 0;
            if (!dst_known.count(id) && !dst_local) {
                dst_known.insert(id);
                bool is_bidi = src.bidi_stream_ids.count(id) > 0;
                new_streams.push_back({id, is_bidi});
            }

            if (w.data) {
                auto coalesced = w.data->cloneCoalesced();
                dst_wt.queueRead(id, coalesced->data(),
                                 coalesced->length(), w.fin);
            } else if (w.fin) {
                dst_wt.queueRead(id, nullptr, 0, true);
            }
            count++;
        }
        src.writes.clear();

        for (auto &ns : new_streams) {
            if (dst.is_terminal()) break;
            auto rh = std::make_unique<FakeReadHandle>(ns.id);
            auto *rh_ptr = rh.get();
            peer_read_handles.push_back(std::move(rh));

            if (ns.is_bidi) {
                auto wh = std::make_unique<FakeWriteHandle>(ns.id);
                auto *wh_ptr = wh.get();
                peer_write_handles.push_back(std::move(wh));
                WT::BidiStreamHandle bh{rh_ptr, wh_ptr};
                dst.onNewBidiStream(bh);
            } else {
                dst.onNewUniStream(rh_ptr);
            }
        }

        // Deliver datagrams → peer's onDatagram
        for (auto &dg : src.datagrams) {
            if (dst.is_terminal()) break;
            if (dg.data) {
                dst.onDatagram(dg.data->cloneCoalesced());
            } else {
                dst.onDatagram(nullptr);
            }
            count++;
        }
        src.datagrams.clear();

        // Deliver resets → peer's read queue as exceptions
        for (auto &r : src.resets) {
            auto &rq = dst_wt.read_queues[r.stream_id];
            rq.clear();
            QueuedRead qr;
            qr.is_exception = true;
            qr.exception_code = r.error;
            rq.push_back(std::move(qr));
            count++;
        }
        src.resets.clear();

        // Deliver stop-sending → recorded but not propagated in v1
        src.stops.clear();

        // Deliver closes → peer's onSessionEnd
        for (auto &cl : src.closes) {
            if (dst.is_terminal()) break;
            dst.onSessionEnd(cl.error);
            count++;
        }
        src.closes.clear();

        return count;
    }

    /* -- Pump --------------------------------------------------------- */

    size_t pump()
    {
        size_t n = 0;
        n += deliver(client_wt, *server, server_known_streams);
        n += deliver(server_wt, *client, client_known_streams);
        if (!client->is_terminal())
            client->service();
        if (!server->is_terminal())
            server->service();
        n += client_wt.writes.size() + client_wt.datagrams.size() +
             client_wt.resets.size() + client_wt.closes.size();
        n += server_wt.writes.size() + server_wt.datagrams.size() +
             server_wt.resets.size() + server_wt.closes.size();
        return n;
    }

    bool pump_until(int max_rounds = 50)
    {
        for (int i = 0; i < max_rounds; i++) {
            if (has_fatal()) return false;
            if (pump() == 0) return true;
        }
        return false;
    }

    bool wait_setup(int max_rounds = 50)
    {
        moq_session_start(client_session, get_time());
        client->service();

        for (int i = 0; i < max_rounds; i++) {
            pump();

            bool c_done = false, s_done = false;
            moq_event_t ev;
            while (moq_session_poll_events(client_session, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) c_done = true;
                moq_event_cleanup(&ev);
            }
            while (moq_session_poll_events(server_session, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) s_done = true;
                moq_event_cleanup(&ev);
            }
            if (c_done && s_done) return true;
        }
        return false;
    }

    bool has_fatal() const
    {
        return client->is_fatal() || server->is_fatal();
    }

    /* -- Conformance helpers ------------------------------------------ */

    void inject_bidi_fin(bool from_client)
    {
        auto &src_wt = from_client ? client_wt : server_wt;
        auto &dst_wt = from_client ? server_wt : client_wt;
        for (auto id : src_wt.bidi_stream_ids) {
            dst_wt.queueRead(id, nullptr, 0, true);
            break;
        }
    }
};

} // namespace moq::wt::testing

#endif // MOQ_FAKE_WT_PAIR_H
