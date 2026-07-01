/*
 * Proxygen WebTransport conformance pair.
 *
 * Wraps FakeWtPair with the moq_adapter_pair_ops_t vtable so the
 * shared conformance scenarios run through real Adapter + endpoint ops.
 */

#include "wt_conformance_pair.h"
#include "fake_wt_pair.h"

#include <cstdio>
#include <cstring>

using namespace moq::wt::testing;

struct wt_conf_ctx {
    FakeWtPair *pair;
    uint64_t now;
    char last_error[256];

    wt_conf_ctx() : pair(nullptr), now(0) {
        last_error[0] = '\0';
    }
};

static wt_conf_ctx *C(void *ctx) {
    return static_cast<wt_conf_ctx *>(ctx);
}

static FakeWtPair *P(void *ctx) { return C(ctx)->pair; }

static FakeWebTransport &side_wt(wt_conf_ctx *c,
                                  moq_adapter_pair_side_t side)
{
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? c->pair->client_wt : c->pair->server_wt;
}

static moq::wt::Adapter *side_adapter(wt_conf_ctx *c,
                                       moq_adapter_pair_side_t side)
{
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? c->pair->client.get() : c->pair->server.get();
}

/* -- Session access ------------------------------------------------- */

static moq_session_t *wc_client_session(void *ctx) {
    return P(ctx)->client_session;
}

static moq_session_t *wc_server_session(void *ctx) {
    return P(ctx)->server_session;
}

/* -- Time ----------------------------------------------------------- */

static uint64_t wc_now_us(void *ctx) {
    return C(ctx)->now;
}

static void wc_advance_to(void *ctx, uint64_t t) {
    C(ctx)->now = t;
    P(ctx)->set_time(t);
}

static uint64_t wc_next_deadline_us(void *ctx) {
    auto *c = C(ctx);
    uint64_t cd = moq_session_next_deadline_us(c->pair->client_session);
    uint64_t sd = moq_session_next_deadline_us(c->pair->server_session);
    uint64_t m = cd < sd ? cd : sd;
    return m;
}

/* -- Pump ----------------------------------------------------------- */

static moq_adapter_pair_pump_result_t wc_pump_once(void *ctx,
                                                    uint64_t now_us)
{
    auto *c = C(ctx);
    c->now = now_us;
    c->pair->set_time(now_us);
    size_t n = c->pair->pump();
    if (c->pair->has_fatal()) {
        std::snprintf(c->last_error, sizeof(c->last_error),
                      "fatal during pump");
        return MOQ_ADAPTER_PAIR_ERROR;
    }
    return n > 0 ? MOQ_ADAPTER_PAIR_PROGRESS
                 : MOQ_ADAPTER_PAIR_QUIESCENT;
}

static moq_adapter_pair_pump_result_t wc_pump_until(void *ctx,
                                                     int max_steps)
{
    auto *c = C(ctx);
    for (int i = 0; i < max_steps; i++) {
        auto r = wc_pump_once(ctx, c->now);
        if (r == MOQ_ADAPTER_PAIR_ERROR)
            return r;
        if (r == MOQ_ADAPTER_PAIR_QUIESCENT)
            return r;
    }
    return MOQ_ADAPTER_PAIR_MAX_STEPS;
}

/* -- Diagnostics ---------------------------------------------------- */

static bool wc_has_error(void *ctx) {
    return P(ctx)->has_fatal();
}

static bool wc_is_closed(void *ctx, moq_adapter_pair_side_t side) {
    return side_adapter(C(ctx), side)->is_closed();
}

static uint64_t wc_close_code(void *ctx, moq_adapter_pair_side_t side) {
    return side_adapter(C(ctx), side)->close_code();
}

static bool wc_has_fatal(void *ctx, moq_adapter_pair_side_t side) {
    return side_adapter(C(ctx), side)->is_fatal();
}

static uint64_t wc_fatal_code(void *ctx, moq_adapter_pair_side_t side) {
    return side_adapter(C(ctx), side)->fatal_code();
}

static const char *wc_last_error(void *ctx) {
    return C(ctx)->last_error;
}

/* -- Fault injection ------------------------------------------------ */

static void wc_block_writes(void *ctx, moq_adapter_pair_side_t side,
                             bool block)
{
    side_wt(C(ctx), side).block_all_writes = block;
}

static void wc_drop_datagrams(void *ctx, moq_adapter_pair_side_t side,
                               bool drop)
{
    side_wt(C(ctx), side).drop_all_datagrams = drop;
}

static size_t wc_stream_count(void *ctx, moq_adapter_pair_side_t side) {
    return side_adapter(C(ctx), side)->stream_count();
}

static size_t wc_tombstone_count(void *ctx,
                                  moq_adapter_pair_side_t side) {
    return side_adapter(C(ctx), side)->tombstone_count();
}

static size_t wc_opened_bidi_count(void *ctx,
                                    moq_adapter_pair_side_t side) {
    auto &wt = side_wt(C(ctx), side);
    return static_cast<size_t>(wt.create_bidi_count);
}

static int wc_inject_bidi_fin(void *ctx,
                               moq_adapter_pair_side_t from_side)
{
    auto *c = C(ctx);
    auto &src_wt = (from_side == MOQ_ADAPTER_PAIR_CLIENT)
        ? c->pair->client_wt : c->pair->server_wt;
    auto &dst_wt = (from_side == MOQ_ADAPTER_PAIR_CLIENT)
        ? c->pair->server_wt : c->pair->client_wt;
    auto &dst = (from_side == MOQ_ADAPTER_PAIR_CLIENT)
        ? *c->pair->server : *c->pair->client;

    // The control bidi is the first one created by this side.
    // Find it by role: lowest ID in bidi_stream_ids.
    uint64_t ctrl_id = UINT64_MAX;
    for (auto id : src_wt.bidi_stream_ids)
        if (id < ctrl_id) ctrl_id = id;

    // Find the last non-control bidi (highest ID that isn't control).
    uint64_t target = UINT64_MAX;
    for (auto id : src_wt.bidi_stream_ids) {
        if (id == ctrl_id) continue;
        if (target == UINT64_MAX || id > target) target = id;
    }
    if (target == UINT64_MAX)
        return -1;

    dst_wt.queueRead(target, nullptr, 0, true);
    if (!dst.is_terminal())
        dst.service();
    return 0;
}

/* -- Destroy -------------------------------------------------------- */

static void wc_destroy(void *ctx) {
    auto *c = C(ctx);
    delete c->pair;
    delete c;
}

/* -- Vtable --------------------------------------------------------- */

static const moq_adapter_pair_ops_t wt_conf_ops = {
    wc_client_session,
    wc_server_session,
    wc_now_us,
    wc_advance_to,
    wc_next_deadline_us,
    wc_pump_once,
    wc_pump_until,
    wc_has_error,
    wc_is_closed,
    wc_close_code,
    wc_has_fatal,
    wc_fatal_code,
    wc_last_error,
    wc_block_writes,
    wc_drop_datagrams,
    wc_stream_count,
    wc_tombstone_count,
    wc_opened_bidi_count,
    wc_inject_bidi_fin,
    wc_destroy,
};

/* -- Factory -------------------------------------------------------- */

moq_adapter_pair_t wt_conformance_create(void)
{
    auto *c = new wt_conf_ctx();
    c->pair = new FakeWtPair(&c->now);

    if (!c->pair->init_ok) {
        delete c->pair;
        delete c;
        return {nullptr, 0, nullptr};
    }

    uint32_t caps =
        MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES |
        MOQ_ADAPTER_PAIR_CAP_DROP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS |
        MOQ_ADAPTER_PAIR_CAP_TOMBSTONES |
        MOQ_ADAPTER_PAIR_CAP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS |
        MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME |
        MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT |
        MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN;

    return {&wt_conf_ops, caps, c};
}
