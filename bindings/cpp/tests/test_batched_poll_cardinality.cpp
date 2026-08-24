/*
 * Direct proof of batched-poll cardinality, exact ordering, ownership and
 * exception behaviour -- driven THROUGH the new session::poll_actions /
 * poll_events templates, with no product seam and no linker interposition.
 *
 * Mechanism: the two size-aware C poll entry points are macro-renamed to unique
 * test symbols BEFORE any moq include, so the `static inline` convenience
 * wrapper in session.h (which the templates call) is compiled against the fakes
 * in this TU. The names are unique, so the core library's real definitions are
 * untouched and no interposition is involved.
 *
 * This file must remain a SINGLE translation unit: a second, differently
 * macro-expanded moq::session in the same binary would violate the ODR.
 *
 * The fakes model a real QUEUE: each has `remaining` and an absolute cursor, so
 * every call returns min(remaining, cap), advances the cursor and eventually
 * returns 0. A cap-1-loop mutant therefore terminates and fails by name instead
 * of looping forever. Synthetic identity derives from the ABSOLUTE cursor, so
 * aggregate order is checked across calls, not per call.
 */

#define moq_session_poll_actions_ex test_fake_poll_actions_ex
#define moq_session_poll_events_ex  test_fake_poll_events_ex

#include <moq/session.h>
#include <moq/session.hpp>
#include <moq/rcbuf.h>

#undef moq_session_poll_actions_ex
#undef moq_session_poll_events_ex

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace {

constexpr size_t MAXQ = 16;

struct probe {
    int    calls     = 0;
    size_t last_cap  = 0;
    size_t last_elem = 0;
    size_t remaining = 0;   /* records still queued */
    size_t cursor    = 0;   /* absolute index of the next record */
    bool   owned     = false;
    bool   unknown   = false;  /* emit an undefined kind from the fake itself */
};
probe g_act, g_evt;

/* Owned-resource bookkeeping, indexed by ABSOLUTE cursor. Every owning event
 * form holds TWO rcbufs, counted independently so both must clean exactly once. */
constexpr uint32_t KIND_UNKNOWN_ACTION = 55001;
constexpr uint32_t KIND_UNKNOWN_EVENT  = 55002;

/* Borrowed control bytes for the ordinary mixed mode. Static storage, so the
 * span stays valid for the whole batch; the fixture declares the contents
 * independently of anything the product computes. Never retained past the
 * callback by the test. */
constexpr size_t CTRL_LEN = 5;
uint8_t g_ctrl[MAXQ][CTRL_LEN] = {{0}};

void fill_ctrl(size_t abs) {
    for (size_t j = 0; j < CTRL_LEN; j++)
        g_ctrl[abs][j] = (uint8_t)(0x40 + abs * 8 + j);
}
uint8_t expect_ctrl_byte(size_t abs, size_t j) {
    return (uint8_t)(0x40 + abs * 8 + j);
}

int     g_freed[MAXQ]     = {0};   /* primary: payload / chunk */
int     g_freed2[MAXQ]    = {0};   /* secondary: properties */
uint8_t g_bytes[MAXQ][4]  = {{0}};
uint8_t g_bytes2[MAXQ][4] = {{0}};
void owned_freed_cb(void *ctx, const uint8_t *, size_t) { ++*static_cast<int *>(ctx); }

void reset_freed() {
    for (size_t i = 0; i < MAXQ; i++) { g_freed[i] = 0; g_freed2[i] = 0; }
}

/* Independently declared expectations, derived from the absolute index only. */
uint32_t expect_action_kind(size_t i) {
    return (i % 2 == 0) ? MOQ_ACTION_SEND_CONTROL : MOQ_ACTION_RESET_DATA;
}
uint64_t expect_action_code(size_t i) { return 1000 + i; }
uint32_t expect_event_kind(size_t i) {
    return (i % 2 == 0) ? MOQ_EVENT_SUBSCRIBE_OK : MOQ_EVENT_SUBGROUP_FINISHED;
}
uint64_t expect_event_field(size_t i) { return 2000 + i; }

int failures = 0;

}  // namespace

#define CHECK(c) do { if (!(c)) { \
    std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); failures++; } } while (0)

extern "C" {

moq_result_t test_fake_poll_actions_ex(moq_session_t *s, void *out, size_t cap,
                                       size_t element_size, size_t *out_count)
{
    (void)s;                                   /* never dereferenced */
    g_act.calls++;
    g_act.last_cap = cap;
    g_act.last_elem = element_size;
    size_t n = g_act.remaining < cap ? g_act.remaining : cap;
    auto *recs = static_cast<moq_action_t *>(out);
    for (size_t k = 0; k < n; k++) {
        size_t abs = g_act.cursor + k;          /* ABSOLUTE identity */
        std::memset(&recs[k], 0, sizeof(moq_action_t));
        if (g_act.owned && abs < MAXQ) {
            recs[k].kind = MOQ_ACTION_SEND_DATA;
            g_bytes[abs][0] = (uint8_t)(0xA0 + abs);
            moq_rcbuf_t *b = nullptr;
            if (moq_rcbuf_wrap(moq_alloc_default(), g_bytes[abs], 4,
                               owned_freed_cb, &g_freed[abs], &b) != MOQ_OK || !b) {
                *out_count = k;                 /* fail closed: publish nothing bad */
                g_act.cursor += k; g_act.remaining -= k;
                return MOQ_OK;
            }
            recs[k].u.send_data.payload = b;
            recs[k].u.send_data.stream_ref = moq_stream_ref_from_u64(500 + abs);
            continue;
        }
        if (g_act.unknown) { recs[k].kind = KIND_UNKNOWN_ACTION; continue; }
        recs[k].kind = expect_action_kind(abs);
        if (recs[k].kind == MOQ_ACTION_SEND_CONTROL && abs < MAXQ) {
            fill_ctrl(abs);
            recs[k].u.send_control.data = g_ctrl[abs];
            recs[k].u.send_control.len  = CTRL_LEN;
        }
        if (recs[k].kind == MOQ_ACTION_RESET_DATA) {
            recs[k].u.reset_data.error_code = expect_action_code(abs);
            recs[k].u.reset_data.stream_ref = moq_stream_ref_from_u64(700 + abs);
        }
    }
    g_act.cursor += n;
    g_act.remaining -= n;
    *out_count = n;
    return MOQ_OK;
}

moq_result_t test_fake_poll_events_ex(moq_session_t *s, void *out, size_t cap,
                                      size_t element_size, size_t *out_count)
{
    (void)s;
    g_evt.calls++;
    g_evt.last_cap = cap;
    g_evt.last_elem = element_size;
    size_t n = g_evt.remaining < cap ? g_evt.remaining : cap;
    auto *recs = static_cast<moq_event_t *>(out);
    for (size_t k = 0; k < n; k++) {
        size_t abs = g_evt.cursor + k;
        std::memset(&recs[k], 0, sizeof(moq_event_t));
        if (g_evt.owned && abs < MAXQ) {
            /* Rotate the three rcbuf-owning shapes; EACH owns TWO buffers, and
             * moq_event_cleanup must release both. */
            g_bytes[abs][0]  = (uint8_t)(0xE0 + abs);
            g_bytes2[abs][0] = (uint8_t)(0xF0 + abs);
            moq_rcbuf_t *b1 = nullptr, *b2 = nullptr;
            if (moq_rcbuf_wrap(moq_alloc_default(), g_bytes[abs], 4,
                               owned_freed_cb, &g_freed[abs], &b1) != MOQ_OK || !b1) {
                *out_count = k; g_evt.cursor += k; g_evt.remaining -= k;
                return MOQ_OK;
            }
            if (moq_rcbuf_wrap(moq_alloc_default(), g_bytes2[abs], 4,
                               owned_freed_cb, &g_freed2[abs], &b2) != MOQ_OK || !b2) {
                /* never publish a half-initialised owned record */
                moq_rcbuf_decref(b1);
                *out_count = k; g_evt.cursor += k; g_evt.remaining -= k;
                return MOQ_OK;
            }
            switch (abs % 3) {
            case 0:
                recs[k].kind = MOQ_EVENT_OBJECT_RECEIVED;
                recs[k].u.object_received.payload    = b1;
                recs[k].u.object_received.properties = b2;
                break;
            case 1:
                recs[k].kind = MOQ_EVENT_FETCH_OBJECT;
                recs[k].u.fetch_object.payload    = b1;
                recs[k].u.fetch_object.properties = b2;
                break;
            default:
                recs[k].kind = MOQ_EVENT_OBJECT_CHUNK;
                recs[k].u.object_chunk.chunk      = b1;
                recs[k].u.object_chunk.properties = b2;
                break;
            }
            continue;
        }
        if (g_evt.unknown) { recs[k].kind = KIND_UNKNOWN_EVENT; continue; }
        recs[k].kind = expect_event_kind(abs);
        if (recs[k].kind == MOQ_EVENT_SUBSCRIBE_OK)
            recs[k].u.subscribe_ok.track_alias = expect_event_field(abs);
        else
            recs[k].u.subgroup_finished.group_id = expect_event_field(abs);
    }
    g_evt.cursor += n;
    g_evt.remaining -= n;
    *out_count = n;
    return MOQ_OK;
}

}  /* extern "C" */

int main()
{
    moq::session_config cfg;
    cfg.perspective = moq::perspective::client;
    auto made = moq::session::create(cfg, 0);
    if (!made) { std::fputs("session::create failed\n", stderr); return 1; }
    moq::session &s = *made;

    /* ---- 1. actions: ONE call at cap N, exact aggregate order --------- */
    g_act = probe{}; g_act.remaining = 3;
    {
        std::vector<uint32_t> kinds; std::vector<uint64_t> codes; std::vector<uint64_t> refs;
        std::vector<uint64_t> vcodes, vrefs;   /* read THROUGH variant() */
        size_t ctrl_seen = 0;
        std::size_t n = s.poll_actions<4>([&](moq::polled_action &a) {
            kinds.push_back(a.kind());
            if (a.kind() == MOQ_ACTION_RESET_DATA) {
                codes.push_back(a.raw().u.reset_data.error_code);
                refs.push_back(a.raw().u.reset_data.stream_ref._v);
            }
            /* The interpreted value must agree with the raw record: reading only
             * raw() would let a variant()-side field corruption pass. */
            auto v = a.variant();
            if (auto *rd = std::get_if<moq::action::reset_data>(&v)) {
                vcodes.push_back(rd->error_code);
                vrefs.push_back(rd->ref.raw()._v);
            }
            if (auto *sc = std::get_if<moq::action::send_control>(&v)) {
                /* EVERY SEND_CONTROL index, complete span, inside the callback.
                 * Structurally guarded: a wrong pointer or length must fail on a
                 * named assertion WITHOUT being dereferenced, so the negative
                 * evidence never depends on an out-of-bounds read. */
                const size_t abs = ctrl_seen * 2;   /* indices 0 and 2 */
                ctrl_seen++;

                /* (a) RAW span, validated against the declaration on its own
                 *     known-good pointer/length before any indexing. */
                const uint8_t *rawp = a.raw().u.send_control.data;
                const size_t   rawn = a.raw().u.send_control.len;
                CHECK(rawn == CTRL_LEN);
                CHECK(rawp != nullptr);
                if (rawn == CTRL_LEN && rawp != nullptr) {
                    bool raw_ok = true;
                    for (size_t j = 0; j < CTRL_LEN; j++)
                        if (rawp[j] != expect_ctrl_byte(abs, j)) raw_ok = false;
                    CHECK(raw_ok);
                }

                /* (b) INTERPRETED structure: length, non-NULL, exact alias --
                 *     each a separate named assertion. */
                const bool ilen  = (sc->data.size() == CTRL_LEN);
                const bool iptr  = (sc->data.data() != nullptr);
                const bool alias = (sc->data.data() == rawp) &&
                                   (sc->data.size() == rawn);
                CHECK(ilen);
                CHECK(iptr);
                CHECK(alias);

                /* (c) INTERPRETED bytes only when every structural precondition
                 *     holds, including the exact alias. */
                if (ilen && iptr && alias && rawn == CTRL_LEN) {
                    bool ok = true;
                    for (size_t j = 0; j < CTRL_LEN; j++)
                        if (sc->data[j] != expect_ctrl_byte(abs, j)) ok = false;
                    CHECK(ok);
                }
            }
        });
        CHECK(g_act.calls == 1);
        CHECK(g_act.last_cap == 4);
        CHECK(g_act.last_elem == sizeof(moq_action_t));
        CHECK(n == 3);
        CHECK(kinds.size() == 3);
        for (size_t i = 0; i < kinds.size(); i++) CHECK(kinds[i] == expect_action_kind(i));
        CHECK(codes.size() == 1 && codes[0] == expect_action_code(1));
        CHECK(refs.size()  == 1 && refs[0]  == 701);
        CHECK(vcodes.size() == 1 && vcodes[0] == expect_action_code(1));
        CHECK(vrefs.size()  == 1 && vrefs[0]  == 701);
        CHECK(ctrl_seen == 2);   /* indices 0 and 2 are SEND_CONTROL */
        CHECK(g_evt.calls == 0);
    }

    /* ---- 2. events: ONE call, EVERY index checked -------------------- */
    g_act = probe{}; g_evt = probe{}; g_evt.remaining = 5;
    {
        std::vector<uint32_t> kinds; std::vector<uint64_t> fields;
        std::size_t n = s.poll_events<8>([&](moq::polled_event &e) {
            kinds.push_back(e.kind());
            fields.push_back(e.kind() == MOQ_EVENT_SUBSCRIBE_OK
                             ? e.raw().u.subscribe_ok.track_alias
                             : e.raw().u.subgroup_finished.group_id);
        });
        CHECK(g_evt.calls == 1);
        CHECK(g_evt.last_cap == 8);
        CHECK(g_evt.last_elem == sizeof(moq_event_t));
        CHECK(n == 5);
        CHECK(kinds.size() == 5 && fields.size() == 5);
        for (size_t i = 0; i < 5; i++) {          /* every index, not a sample */
            CHECK(kinds[i]  == expect_event_kind(i));
            CHECK(fields[i] == expect_event_field(i));
        }
        CHECK(g_act.calls == 0);
    }

    /* ---- 3. empty and exactly-N, both families ----------------------- */
    g_act = probe{}; g_act.remaining = 0;
    { int inv = 0;
      CHECK(s.poll_actions<4>([&](moq::polled_action &) { inv++; }) == 0);
      CHECK(g_act.calls == 1); CHECK(inv == 0); }
    g_evt = probe{}; g_evt.remaining = 0;
    { int inv = 0;
      CHECK(s.poll_events<4>([&](moq::polled_event &) { inv++; }) == 0);
      CHECK(g_evt.calls == 1); CHECK(inv == 0); }
    g_act = probe{}; g_act.remaining = 9;
    { int inv = 0;
      CHECK(s.poll_actions<4>([&](moq::polled_action &) { inv++; }) == 4);  /* exactly N */
      CHECK(inv == 4); CHECK(g_act.remaining == 5); }
    g_evt = probe{}; g_evt.remaining = 9;
    { int inv = 0;
      CHECK(s.poll_events<4>([&](moq::polled_event &) { inv++; }) == 4);
      CHECK(inv == 4); CHECK(g_evt.remaining == 5); }

    /* ---- 4. queue drains across calls; aggregate order preserved ----- */
    g_act = probe{}; g_act.remaining = 7;
    {
        std::vector<uint32_t> kinds;
        for (;;) {
            std::size_t n = s.poll_actions<4>([&](moq::polled_action &a) {
                kinds.push_back(a.kind());
            });
            if (n == 0) break;
        }
        CHECK(kinds.size() == 7);
        for (size_t i = 0; i < kinds.size(); i++) CHECK(kinds[i] == expect_action_kind(i));
        CHECK(g_act.calls == 3);                  /* 4 + 3 + 0 */
    }

    /* ---- 5. owned ACTION payloads: freed exactly once via the batch --- */
    g_act = probe{}; g_act.remaining = 3; g_act.owned = true;
    reset_freed();
    {
        int visited = 0;
        std::size_t n = s.poll_actions<4>([&](moq::polled_action &a) {
            CHECK(a.kind() == MOQ_ACTION_SEND_DATA);
            CHECK(a.raw().u.send_data.payload != nullptr);
            CHECK(a.raw().u.send_data.stream_ref._v == 500 + (uint64_t)visited);
            visited++;
        });
        CHECK(n == 3); CHECK(visited == 3);
        for (int i = 0; i < 3; i++) CHECK(g_freed[i] == 1);
        for (int i = 3; i < (int)MAXQ; i++) CHECK(g_freed[i] == 0);
    }

    /* ---- 6. owned EVENT payloads through poll_events, all three shapes  */
    g_evt = probe{}; g_evt.remaining = 3; g_evt.owned = true;
    reset_freed();
    {
        std::vector<uint32_t> kinds;
        std::size_t n = s.poll_events<4>([&](moq::polled_event &e) {
            kinds.push_back(e.kind());
        });
        CHECK(n == 3);
        CHECK(kinds.size() == 3);
        CHECK(kinds[0] == MOQ_EVENT_OBJECT_RECEIVED);
        CHECK(kinds[1] == MOQ_EVENT_FETCH_OBJECT);
        CHECK(kinds[2] == MOQ_EVENT_OBJECT_CHUNK);
        for (int i = 0; i < 3; i++) {
            CHECK(g_freed[i]  == 1);   /* payload / chunk exactly once */
            CHECK(g_freed2[i] == 1);   /* properties exactly once */
        }
    }

    /* ---- 7. exception at a MIDDLE item: prefix, current, suffix ------- */
    g_act = probe{}; g_act.remaining = 5; g_act.owned = true;
    reset_freed();
    {
        int seen = 0; bool threw = false;
        try {
            s.poll_actions<8>([&](moq::polled_action &) {
                if (++seen == 3) throw std::runtime_error("middle");
            });
        } catch (const std::runtime_error &) { threw = true; }
        CHECK(threw);
        CHECK(seen == 3);                       /* visited prefix non-empty */
        for (int i = 0; i < 5; i++) CHECK(g_freed[i] == 1);  /* all exactly once */
    }
    g_evt = probe{}; g_evt.remaining = 5; g_evt.owned = true;
    reset_freed();
    {
        int seen = 0; bool threw = false;
        try {
            s.poll_events<8>([&](moq::polled_event &) {
                if (++seen == 3) throw std::runtime_error("middle");
            });
        } catch (const std::runtime_error &) { threw = true; }
        CHECK(threw); CHECK(seen == 3);
        for (int i = 0; i < 5; i++) {
            CHECK(g_freed[i]  == 1);
            CHECK(g_freed2[i] == 1);   /* both refs, incl. unvisited suffix */
        }
    }

    /* ---- 8. move the owner OUT of the batched callback ---------------- */
    g_act = probe{}; g_act.remaining = 2; g_act.owned = true;
    reset_freed();
    {
        std::optional<moq::polled_action> kept;
        std::size_t n = s.poll_actions<4>([&](moq::polled_action &a) {
            if (!kept) kept.emplace(std::move(a));
        });
        CHECK(n == 2);
        CHECK(g_freed[0] == 0);                 /* moved out: not yet freed */
        CHECK(g_freed[1] == 1);                 /* the other one is done */
        kept.reset();
        CHECK(g_freed[0] == 1);                 /* exactly once, later */
    }
    g_evt = probe{}; g_evt.remaining = 2; g_evt.owned = true;
    reset_freed();
    {
        std::optional<moq::polled_event> kept;
        std::size_t n = s.poll_events<4>([&](moq::polled_event &e) {
            if (!kept) kept.emplace(std::move(e));
        });
        CHECK(n == 2);
        CHECK(g_freed[0] == 0);  CHECK(g_freed2[0] == 0);  /* moved out */
        CHECK(g_freed[1] == 1);  CHECK(g_freed2[1] == 1);
        kept.reset();
        CHECK(g_freed[0] == 1);  CHECK(g_freed2[0] == 1);  /* both, exactly once */
    }

    /* ---- 9. save owning variants from the batched callback ------------ */
    g_act = probe{}; g_act.remaining = 1; g_act.owned = true;
    reset_freed();
    {
        std::optional<moq::action_variant> saved;
        s.poll_actions<4>([&](moq::polled_action &a) { saved = a.variant(); });
        CHECK(g_freed[0] == 0);                 /* retained past the owner */
        auto *sd = std::get_if<moq::action::send_data>(&*saved);
        CHECK(sd != nullptr);
        if (sd) { CHECK(sd->payload.size() == 4);
                  CHECK(sd->payload.data() != nullptr && sd->payload.data()[0] == 0xA0); }
        saved.reset();
        CHECK(g_freed[0] == 1);
    }
    g_evt = probe{}; g_evt.remaining = 3; g_evt.owned = true;
    reset_freed();
    {
        std::vector<moq::event_variant> saved;
        std::size_t n = s.poll_events<4>([&](moq::polled_event &e) {
            saved.push_back(e.variant());
        });
        CHECK(n == 3);
        for (int i = 0; i < 3; i++) {          /* retained past their owners */
            CHECK(g_freed[i] == 0); CHECK(g_freed2[i] == 0);
        }
        CHECK(saved.size() == 3);
        if (saved.size() == 3) {
            auto *a0 = std::get_if<moq::event::object_received>(&saved[0]);
            CHECK(a0 != nullptr);
            if (a0) { CHECK(a0->payload_data.size() == 4);
                      CHECK(a0->payload_data.data()[0] == 0xE0);
                      CHECK(a0->properties_data.size() == 4);
                      CHECK(a0->properties_data.data()[0] == 0xF0); }
            auto *a1 = std::get_if<moq::event::fetch_object>(&saved[1]);
            CHECK(a1 != nullptr);
            if (a1) { CHECK(a1->payload_data.size() == 4);
                      CHECK(a1->payload_data.data()[0] == 0xE1);
                      CHECK(a1->properties_data.size() == 4);
                      CHECK(a1->properties_data.data()[0] == 0xF1); }
            auto *a2 = std::get_if<moq::event::object_chunk>(&saved[2]);
            CHECK(a2 != nullptr);
            if (a2) { CHECK(a2->chunk_data.size() == 4);
                      CHECK(a2->chunk_data.data()[0] == 0xE2);
                      CHECK(a2->properties_data.size() == 4);
                      CHECK(a2->properties_data.data()[0] == 0xF2); }
        }
        saved.clear();
        for (int i = 0; i < 3; i++) {          /* both owners, exactly once */
            CHECK(g_freed[i] == 1); CHECK(g_freed2[i] == 1);
        }
    }

    /* ---- 10. unknown kinds emitted BY THE FAKE, seen through the batch --
     * No direct wrapper construction: the owner delivered by the batched call
     * must itself produce the unknown alternative. */
    g_act = probe{}; g_act.remaining = 2; g_act.unknown = true;
    {
        int seen = 0, unknown = 0;
        std::size_t n = s.poll_actions<4>([&](moq::polled_action &a) {
            seen++;
            CHECK(a.kind() == KIND_UNKNOWN_ACTION);
            auto v = a.variant();
            if (std::holds_alternative<moq::action::unknown>(v)) {
                unknown++;
                CHECK(std::get<moq::action::unknown>(v).kind == KIND_UNKNOWN_ACTION);
            }
        });
        CHECK(n == 2); CHECK(seen == 2); CHECK(unknown == 2);
    }
    g_evt = probe{}; g_evt.remaining = 2; g_evt.unknown = true;
    {
        int seen = 0, unknown = 0;
        std::size_t n = s.poll_events<4>([&](moq::polled_event &e) {
            seen++;
            CHECK(e.kind() == KIND_UNKNOWN_EVENT);
            auto v = e.variant();
            if (std::holds_alternative<moq::event::unknown>(v)) {
                unknown++;
                CHECK(std::get<moq::event::unknown>(v).kind == KIND_UNKNOWN_EVENT);
            }
        });
        CHECK(n == 2); CHECK(seen == 2); CHECK(unknown == 2);
    }

    if (failures == 0) std::puts("PASS: test_batched_poll_cardinality");
    return failures ? 1 : 0;
}
