/*
 * Semantic behaviour of the bounded batched poll surface, using SYNTHETIC raw
 * records fed through the existing owning wrappers plus real rcbufs, so
 * ownership is proven by real refcounts and release callbacks rather than by a
 * harness counter.
 *
 * The cardinality proof (one C call, capacity N) lives in
 * test_batched_poll_cardinality.cpp, which macro-renames the poll entry points;
 * that mechanism must stay in its own single TU, so this file exercises the
 * behaviour reachable without faking the C layer.
 */

#include <cstddef>
#include <moq/session.hpp>
#include <moq/rcbuf.h>

#include "test_support.hpp"

#include <cstring>
#include <exception>
#include <optional>
#include <span>
#include <vector>

static int failures = 0;

namespace {

/* Deterministic free observation -- no ASan dependency. */
void freed_cb(void *ctx, const uint8_t *, size_t) { *static_cast<bool *>(ctx) = true; }

moq_rcbuf_t *make_buf(uint8_t *data, size_t len, bool *flag)
{
    moq_rcbuf_t *b = nullptr;
    if (moq_rcbuf_wrap(moq_alloc_default(), data, len, freed_cb, flag, &b) != MOQ_OK)
        return nullptr;
    return b;
}

}  // namespace

int main()
{
    /* =====================================================================
     * CONTROLS (1-4): these construct polled_* DIRECTLY. They are independent
     * controls on the EXISTING wrapper contract and are NOT batched-path
     * coverage; the batched-path ownership proof lives in
     * test_batched_poll_cardinality.cpp, which drives every case through
     * session::poll_actions / poll_events.
     * ===================================================================== */

    /* ---- control 1. owned SEND_DATA payload released exactly once --------- */
    {
        uint8_t data[4] = {0xCA, 0xFE, 0xBA, 0xBE};
        bool freed = false;
        moq_rcbuf_t *buf = make_buf(data, sizeof(data), &freed);
        MOQ_CHECK(buf != nullptr);
        MOQ_CHECK(moq_rcbuf_refcount(buf) == 1);
        {
            moq_action_t a{};
            a.kind = MOQ_ACTION_SEND_DATA;
            a.u.send_data.payload = buf;          /* ref transfers to the owner */
            moq::polled_action owner(a);
            MOQ_CHECK(!freed);
        }
        MOQ_CHECK(freed);                          /* exactly once, on owner death */
    }

    /* ---- control 2. saved owning variant outlives its owner --------------- */
    {
        uint8_t data[4] = {0xDE, 0xAD, 0xBE, 0xEF};
        bool freed = false;
        moq_rcbuf_t *buf = make_buf(data, sizeof(data), &freed);
        MOQ_CHECK(buf != nullptr);
        std::optional<moq::action_variant> saved;
        {
            moq_action_t a{};
            a.kind = MOQ_ACTION_SEND_DATA;
            a.u.send_data.payload = buf;
            moq::polled_action owner(a);
            saved = owner.variant();               /* retains */
        }
        MOQ_CHECK(!freed);                         /* survives owner destruction */
        auto *sd = std::get_if<moq::action::send_data>(&*saved);
        MOQ_CHECK(sd != nullptr);
        if (sd) {
            MOQ_CHECK(sd->payload.size() == 4);
            MOQ_CHECK(sd->payload.data() != nullptr &&
                      sd->payload.data()[0] == 0xDE);
        }
        saved.reset();
        MOQ_CHECK(freed);                          /* exactly once, at the end */
    }

    /* ---- control 3. object_received event payload ownership --------------- */
    {
        uint8_t data[4] = {0x11, 0x22, 0x33, 0x44};
        bool freed = false;
        moq_rcbuf_t *buf = make_buf(data, sizeof(data), &freed);
        MOQ_CHECK(buf != nullptr);
        std::optional<moq::event_variant> saved;
        {
            moq_event_t e{};
            e.kind = MOQ_EVENT_OBJECT_RECEIVED;
            e.u.object_received.payload = buf;
            moq::polled_event owner(e);
            saved = owner.variant();
        }
        MOQ_CHECK(!freed);
        auto *orv = std::get_if<moq::event::object_received>(&*saved);
        MOQ_CHECK(orv != nullptr);
        if (orv) MOQ_CHECK(orv->payload_data.size() == 4);
        saved.reset();
        MOQ_CHECK(freed);
    }

    /* ---- control 4. moving the owner out defers cleanup ------------------- */
    {
        uint8_t data[2] = {0x0A, 0x0B};
        bool freed = false;
        moq_rcbuf_t *buf = make_buf(data, sizeof(data), &freed);
        MOQ_CHECK(buf != nullptr);
        std::optional<moq::polled_action> kept;
        {
            moq_action_t a{};
            a.kind = MOQ_ACTION_SEND_DATA;
            a.u.send_data.payload = buf;
            moq::polled_action owner(a);
            kept.emplace(std::move(owner));        /* moved-from cleans nothing */
        }
        MOQ_CHECK(!freed);                         /* no early cleanup */
        kept.reset();
        MOQ_CHECK(freed);                          /* exactly one final cleanup */
    }

    /* ---- 5. real session: empty poll never invokes the callback ----------- */
    {
        moq::session_config cfg;
        cfg.perspective = moq::perspective::client;
        auto s = moq::session::create(cfg, 0);
        MOQ_CHECK(static_cast<bool>(s));
        if (s) {
            int invoked = 0;
            std::size_t n = s->poll_actions<8>([&](moq::polled_action &) { invoked++; });
            MOQ_CHECK(n == 0);
            MOQ_CHECK(invoked == 0);
            int einvoked = 0;
            std::size_t m = s->poll_events<8>([&](moq::polled_event &) { einvoked++; });
            MOQ_CHECK(m == 0);
            MOQ_CHECK(einvoked == 0);
        }
    }

    /* ---- 6. real session: started client yields its setup action --------- */
    {
        moq::session_config cfg;
        cfg.perspective = moq::perspective::client;
        auto s = moq::session::create(cfg, 0);
        MOQ_CHECK(static_cast<bool>(s));
        if (s) {
            MOQ_CHECK(moq_session_start(s->raw(), 0) == MOQ_OK);
            std::vector<uint32_t> kinds;
            std::size_t n = s->poll_actions<16>([&](moq::polled_action &a) {
                kinds.push_back(a.kind());
            });
            MOQ_CHECK(n >= 1);                     /* setup produces >= 1 action */
            MOQ_CHECK(kinds.size() == n);          /* one callback per record */
            for (uint32_t k : kinds) MOQ_CHECK(k != 0);
            /* a second poll drains the remainder or returns 0; never negative */
            std::size_t again = s->poll_actions<16>([](moq::polled_action &) {});
            MOQ_CHECK(again <= 16);
        }
    }

    /* ---- 7. callback exception propagates; no leak of the suffix ---------- */
    {
        moq::session_config cfg;
        cfg.perspective = moq::perspective::client;
        auto s = moq::session::create(cfg, 0);
        MOQ_CHECK(static_cast<bool>(s));
        if (s) {
            MOQ_CHECK(moq_session_start(s->raw(), 0) == MOQ_OK);
            bool threw = false;
            int seen = 0;
            try {
                s->poll_actions<16>([&](moq::polled_action &) {
                    seen++;
                    throw std::runtime_error("from callback");
                });
            } catch (const std::runtime_error &) {
                threw = true;
            }
            MOQ_CHECK(threw);
            MOQ_CHECK(seen == 1);                  /* stopped at the first item */
        }
    }

    /* ---- 8. producer WOULD_BLOCK -> partial batch -> same-producer retry --
     * Entirely through the public C++ surface. Every public result is checked
     * before dependent state is used; every pumped action and delivered event is
     * classified exactly, so a dropped, duplicated, reordered or silently
     * ignored record fails BY NAME rather than by a late boolean. */
    {
        moq::session_config ccfg{}, scfg{};
        ccfg.perspective              = moq::perspective::client;
        ccfg.send_request_capacity    = true;
        ccfg.initial_request_capacity = 10;
        scfg.perspective              = moq::perspective::server;
        scfg.send_request_capacity    = true;
        scfg.initial_request_capacity = 10;
        scfg.max_actions              = 1;    /* one queued action fills it */
        auto cr = moq::session::create(ccfg, 0);
        auto sr = moq::session::create(scfg, 0);
        MOQ_CHECK(static_cast<bool>(cr));
        MOQ_CHECK(static_cast<bool>(sr));
        if (cr && sr) {
            auto &c = *cr; auto &s = *sr;
            int unexpected_actions = 0, feed_failures = 0;

            /* Batched pump: every action must be one of the declared kinds and
             * every peer feed must succeed. */
            auto pump = [&](moq::session &from, moq::session &to) -> std::size_t {
                std::size_t moved = 0;
                for (;;) {
                    std::size_t n = from.poll_actions<16>([&](moq::polled_action &a) {
                        moved++;
                        a.visit(
                            [&](const moq::action::send_control &sc) {
                                if (!to.on_control_bytes(sc.data, 0).ok()) feed_failures++;
                            },
                            [&](const moq::action::send_data &sd) {
                                std::vector<uint8_t> b(sd.header.begin(), sd.header.end());
                                b.insert(b.end(), sd.payload.begin(), sd.payload.end());
                                if (!to.on_data_bytes(
                                        sd.ref,
                                        std::span<const uint8_t>(b.data(), b.size()),
                                        sd.fin, 0).ok()) feed_failures++;
                            },
                            [&](const auto &) { unexpected_actions++; });
                    });
                    if (n == 0) break;
                }
                return moved;
            };
            /* Classify events exactly instead of discarding them. */
            auto expect_one_event = [&](moq::session &sess, uint32_t want) -> bool {
                int seen = 0, other = 0;
                for (;;) {
                    std::size_t n = sess.poll_events<16>([&](moq::polled_event &e) {
                        if (e.kind() == want) seen++; else other++;
                    });
                    if (n == 0) break;
                }
                MOQ_CHECK(other == 0);
                return seen == 1;
            };

            MOQ_CHECK(c.start(0).ok());
            pump(c, s);
            MOQ_CHECK(expect_one_event(s, MOQ_EVENT_SETUP_COMPLETE));
            pump(s, c);
            MOQ_CHECK(expect_one_event(c, MOQ_EVENT_SETUP_COMPLETE));

            auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
            MOQ_CHECK(sub.ok());
            if (sub.ok()) {
                pump(c, s);
                moq::subscription srv_sub{};
                int reqs = 0, other_ev = 0;
                for (;;) {
                    std::size_t n = s.poll_events<16>([&](moq::polled_event &e) {
                        e.visit([&](const moq::event::subscribe_request &r) {
                                    srv_sub = r.sub; reqs++;
                                },
                                [&](const auto &) { other_ev++; });
                    });
                    if (n == 0) break;
                }
                MOQ_CHECK(reqs == 1);          /* exactly one, not "at least" */
                MOQ_CHECK(other_ev == 0);

                MOQ_CHECK(s.accept_subscribe(srv_sub, {}, 0).ok());
                pump(s, c);
                MOQ_CHECK(expect_one_event(c, MOQ_EVENT_SUBSCRIBE_OK));

                auto sg = s.open_subgroup(srv_sub, {}, 0);
                MOQ_CHECK(sg.ok());
                const uint8_t bytes[3] = {0x51, 0x52, 0x53};
                auto payload = moq::buffer::create(bytes, sizeof(bytes));
                MOQ_CHECK(payload.ok());
                if (sg.ok() && payload.ok()) {
                    auto wr = s.write_object(*sg, 0, *payload, 0);
                    MOQ_CHECK(!wr.ok());
                    MOQ_CHECK(wr.error().code() == moq::errc::would_block);

                    /* PARTIAL batch: exactly one record though N = 16, and it
                     * must be the subgroup-header SEND_DATA. */
                    int hdr = 0, wrong = 0, fed_ok = 0;
                    std::size_t drained = s.poll_actions<16>([&](moq::polled_action &a) {
                        a.visit(
                            [&](const moq::action::send_data &sd) {
                                hdr++;
                                std::vector<uint8_t> b(sd.header.begin(), sd.header.end());
                                b.insert(b.end(), sd.payload.begin(), sd.payload.end());
                                /* advancing the OTHER session here is legal */
                                if (c.on_data_bytes(
                                        sd.ref,
                                        std::span<const uint8_t>(b.data(), b.size()),
                                        sd.fin, 0).ok()) fed_ok++;
                            },
                            [&](const auto &) { wrong++; });
                    });
                    MOQ_CHECK(drained == 1);      /* count < N */
                    MOQ_CHECK(hdr == 1);
                    MOQ_CHECK(wrong == 0);
                    MOQ_CHECK(fed_ok == 1);

                    /* retry the SAME public producer */
                    wr = s.write_object(*sg, 0, *payload, 0);
                    MOQ_CHECK(wr.ok());

                    /* exact post-retry action count, then exact peer delivery */
                    std::size_t after = pump(s, c);
                    MOQ_CHECK(after == 1);        /* exactly the object's action */

                    int objs = 0, others = 0;
                    for (;;) {
                        std::size_t n = c.poll_events<16>([&](moq::polled_event &e) {
                            e.visit(
                                [&](const moq::event::object_received &o) {
                                    objs++;
                                    MOQ_CHECK(o.group_id == 0);
                                    MOQ_CHECK(o.object_id == 0);
                                    MOQ_CHECK(o.payload_data.size() == sizeof(bytes));
                                    if (o.payload_data.size() == sizeof(bytes))
                                        MOQ_CHECK(std::memcmp(
                                            o.payload_data.data(), bytes,
                                            sizeof(bytes)) == 0);
                                },
                                [&](const auto &) { others++; });
                        });
                        if (n == 0) break;
                    }
                    MOQ_CHECK(objs == 1);         /* exactly one, not duplicated */
                    MOQ_CHECK(others == 0);

                    /* both action AND event queues empty afterwards */
                    MOQ_CHECK(s.poll_actions<16>([](moq::polled_action &) {}) == 0);
                    MOQ_CHECK(c.poll_actions<16>([](moq::polled_action &) {}) == 0);
                    MOQ_CHECK(s.poll_events<16>([](moq::polled_event &) {}) == 0);
                    MOQ_CHECK(c.poll_events<16>([](moq::polled_event &) {}) == 0);
                }
            }
            MOQ_CHECK(unexpected_actions == 0);
            MOQ_CHECK(feed_failures == 0);
        }
    }
    /* -- the appended object-chunk reset cause crosses the binding -------- *
     * Through the real polled_event -> variant conversion, not layout
     * assertions: a full-sized RESET terminal carries the exact peer code, a
     * NORMAL terminal reads zero, an older/smaller detail reads zero even with
     * a poisoned physical tail, and copies preserve the code. */
    {
        /* 1. full-sized RESET terminal -> exact nonzero code */
        moq_event_t e{};
        e.kind = MOQ_EVENT_OBJECT_CHUNK;
        e.detail_size = static_cast<uint32_t>(sizeof(moq_object_chunk_event_t));
        e.u.object_chunk.end = true;
        e.u.object_chunk.terminal = MOQ_OBJECT_TERMINAL_RESET;
        e.u.object_chunk.error_code = 0xC0FFEEu;
        moq::polled_event owner(e);
        auto v = owner.variant();
        auto *oc = std::get_if<moq::event::object_chunk>(&v);
        MOQ_CHECK(oc != nullptr);
        if (oc) MOQ_CHECK(oc->error_code == 0xC0FFEEu);

        /* 4. copy preserves the exact code */
        if (oc) {
            moq::event::object_chunk copy = *oc;
            MOQ_CHECK(copy.error_code == 0xC0FFEEu);
            moq::event_variant vcopy = v;
            auto *oc2 = std::get_if<moq::event::object_chunk>(&vcopy);
            MOQ_CHECK(oc2 != nullptr);
            if (oc2) MOQ_CHECK(oc2->error_code == 0xC0FFEEu);
        }
    }
    {
        /* 2. full-sized NORMAL terminal with POISON in the physical tail ->
         *    still zero: a cause is meaningful only under a RESET terminal. */
        moq_event_t e{};
        e.kind = MOQ_EVENT_OBJECT_CHUNK;
        e.detail_size = static_cast<uint32_t>(sizeof(moq_object_chunk_event_t));
        e.u.object_chunk.end = true;
        e.u.object_chunk.terminal = MOQ_OBJECT_TERMINAL_NORMAL;
        e.u.object_chunk.error_code = 0xABCDEF01u;   /* poison */
        moq::polled_event owner(e);
        auto v = owner.variant();
        auto *oc = std::get_if<moq::event::object_chunk>(&v);
        MOQ_CHECK(oc != nullptr);
        if (oc) MOQ_CHECK(oc->error_code == 0u);

        /* Same for the STOP terminal. */
        moq_event_t e2{};
        e2.kind = MOQ_EVENT_OBJECT_CHUNK;
        e2.detail_size = static_cast<uint32_t>(sizeof(moq_object_chunk_event_t));
        e2.u.object_chunk.end = true;
        e2.u.object_chunk.terminal = MOQ_OBJECT_TERMINAL_STOP;
        e2.u.object_chunk.error_code = 0xABCDEF01u;
        moq::polled_event owner2(e2);
        auto v2 = owner2.variant();
        auto *oc2 = std::get_if<moq::event::object_chunk>(&v2);
        MOQ_CHECK(oc2 != nullptr);
        if (oc2) MOQ_CHECK(oc2->error_code == 0u);
    }
    {
        /* 3. old-sized detail ending BEFORE error_code, with deterministic
         *    poison physically present in the tail -> reads zero */
        moq_event_t e{};
        e.kind = MOQ_EVENT_OBJECT_CHUNK;
        e.u.object_chunk.end = true;
        e.u.object_chunk.terminal = MOQ_OBJECT_TERMINAL_RESET;
        e.u.object_chunk.error_code = 0xDEADBEEFu;   /* the poison */
        e.detail_size = static_cast<uint32_t>(
            offsetof(moq_object_chunk_event_t, error_code));
        moq::polled_event owner(e);
        auto v = owner.variant();
        auto *oc = std::get_if<moq::event::object_chunk>(&v);
        MOQ_CHECK(oc != nullptr);
        if (oc) MOQ_CHECK(oc->error_code == 0u);
    }


    MOQ_PASS("test_batched_poll_semantics");
    return failures ? 1 : 0;
}
