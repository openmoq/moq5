/*
 * Receive admission (MOQ_TRANSPORT_CAP_HOLD_INPUT) on the mvfst attach
 * adapter over a real loopback (QuicServer + QuicClientTransport).
 *
 * The client session has a receive pool of ONE data stream. While that entry
 * is taken, every further peer uni data stream is refused at its first chunk
 * and the adapter must keep exactly that chunk (bytes and FIN), pause the
 * stream in mvfst, and redeliver the same chunk once a service pass observes
 * the session able to admit again. Transitions are observed through the
 * test-internals seam (held / refused again / accepted / dropped) and the
 * outcome through the session's exact object and terminal inventory.
 *
 * One wire profile per process (argv[1] = 16 or 18): loopback_pair is limited
 * to one QuicServer per process. All phases share one connection:
 *   1. admission, repeated refusal, FIN behind a held chunk, unrelated
 *      progress, capacity return in stream order;
 *   2. peer RESET of a held stream before its redelivery;
 *   3. teardown with a chunk still held.
 */
#include "support/mvfst_loopback_pair.hpp"
#include "../src/mvfst_managed_testing.h"

#include <moq/buf.h>
#include <moq/control.h>
#include <moq/control_d18.h>

#include <folly/io/IOBuf.h>

#include <cstdlib>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <vector>

static int failures = 0;
static uint64_t g_alias = 0;
#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

using namespace moq::mvfst::test;

/* Preserve provider diagnostics in ctest output after glog initialization. */
class warning_sink : public google::LogSink {
public:
    std::atomic<unsigned> count{0};
    void send(google::LogSeverity severity, const char *, const char *file,
              int line, const google::LogMessageTime &, const char *message,
              size_t len) override {
        if (severity < google::GLOG_WARNING) return;
        count.fetch_add(1);
        std::fprintf(stderr, "provider diagnostic %s:%d: %.*s\n",
                     file, line, static_cast<int>(len), message);
    }
};

/* -- observer ------------------------------------------------------------ */

struct hold_rec {
    uint64_t sid; int phase; size_t len; bool fin;
    moq_result_t result = MOQ_OK;
    bool pending = false;
};
static std::mutex g_mu;
static std::vector<hold_rec> g_recs;

static void on_hold(void *, uint64_t sid, int phase, size_t len, bool fin,
                    moq_result_t result, bool pending)
{
    std::lock_guard<std::mutex> lk(g_mu);
    g_recs.push_back({sid, phase, len, fin, result, pending});
}

static std::vector<hold_rec> recs()
{
    std::lock_guard<std::mutex> lk(g_mu);
    return g_recs;
}

static int count_phase(int phase, uint64_t sid = UINT64_MAX)
{
    int n = 0;
    for (auto &r : recs())
        if (r.phase == phase && (sid == UINT64_MAX || r.sid == sid)) n++;
    return n;
}

static std::vector<uint64_t> held_sids()
{
    std::vector<uint64_t> v;
    for (auto &r : recs())
        if (r.phase == MOQ_MVFST_TEST_HOLD_HELD) v.push_back(r.sid);
    std::sort(v.begin(), v.end());
    return v;
}

/* First record of (phase, sid), by value; false leaves `out` zeroed. */
static bool find_rec(int phase, uint64_t sid, hold_rec &out)
{
    out = hold_rec{0, 0, 0, false};
    for (auto &r : recs())
        if (r.phase == phase && r.sid == sid) { out = r; return true; }
    return false;
}

/* -- client-side inventory ----------------------------------------------- */

struct obj { uint64_t group, object; std::string payload; };
struct inv {
    std::vector<obj> objects;
    std::vector<uint64_t> finished;
    std::vector<std::pair<uint64_t, uint64_t>> resets;
    int closed = 0;
};

static void drain(moq_session_t *s, inv &o)
{
    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        auto &e = ev[i];
        if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            std::string p;
            if (e.u.object_received.payload)
                p.assign((const char *)moq_rcbuf_data(e.u.object_received.payload),
                         moq_rcbuf_len(e.u.object_received.payload));
            o.objects.push_back({e.u.object_received.group_id,
                                 e.u.object_received.object_id, p});
        } else if (e.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            o.finished.push_back(e.u.subgroup_finished.group_id);
        } else if (e.kind == MOQ_EVENT_SUBGROUP_RESET) {
            o.resets.push_back({e.u.subgroup_reset.group_id,
                                e.u.subgroup_reset.error_code});
        } else if (e.kind == MOQ_EVENT_SESSION_CLOSED) {
            o.closed++;
        }
        moq_event_cleanup(&e);
    }
}

static int count_obj(const inv &o, uint64_t g, uint64_t id, const char *p)
{
    int n = 0;
    for (auto &x : o.objects)
        if (x.group == g && x.object == id && x.payload == p) n++;
    return n;
}

static int count_fin(const inv &o, uint64_t g)
{
    int n = 0;
    for (auto x : o.finished) if (x == g) n++;
    return n;
}

/* -- server-side publishing (on the server EventBase) -------------------- */

static moq_subgroup_handle_t open_write(loopback_pair &lp, moq_subscription_t sub,
                                        uint64_t group, uint64_t oid,
                                        const char *payload, bool close)
{
    moq_subgroup_handle_t h{};
    lp.run_on_server([&]() {
        moq_subgroup_cfg_t cfg; moq_subgroup_cfg_init(&cfg);
        cfg.group_id = group; cfg.publisher_priority = 200;
        if (moq_session_open_subgroup(lp.ss.session, sub, &cfg, 0, &h) < 0)
            { lp.ss.error.store(true); return; }
        moq_rcbuf_t *b = nullptr;
        if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
                             std::strlen(payload), &b) < 0)
            { lp.ss.error.store(true); return; }
        if (moq_session_write_object(lp.ss.session, h, oid, b, 0) < 0)
            lp.ss.error.store(true);
        moq_rcbuf_decref(b);
        if (close && moq_session_close_subgroup(lp.ss.session, h, 0) < 0)
            lp.ss.error.store(true);
        lp.service_server();
    });
    return h;
}

static void write_more(loopback_pair &lp, moq_subgroup_handle_t h, uint64_t oid,
                       const char *payload)
{
    lp.run_on_server([&]() {
        moq_rcbuf_t *b = nullptr;
        if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
                             std::strlen(payload), &b) < 0)
            { lp.ss.error.store(true); return; }
        if (moq_session_write_object(lp.ss.session, h, oid, b, 0) < 0)
            lp.ss.error.store(true);
        moq_rcbuf_decref(b);
        lp.service_server();
    });
}

static void close_sg(loopback_pair &lp, moq_subgroup_handle_t h)
{
    lp.run_on_server([&]() {
        if (moq_session_close_subgroup(lp.ss.session, h, 0) < 0)
            lp.ss.error.store(true);
        lp.service_server();
    });
}

static void reset_sg(loopback_pair &lp, moq_subgroup_handle_t h, uint64_t code)
{
    lp.run_on_server([&]() {
        if (moq_session_reset_subgroup(lp.ss.session, h, code, 0) < 0)
            lp.ss.error.store(true);
        lp.service_server();
    });
}

/* -- subscribe ----------------------------------------------------------- */

static moq_subscription_t subscribe(loopback_pair &lp)
{
    const moq_subscription_t invalid = MOQ_SUBSCRIPTION_INVALID;
    if (!lp.wait_setup()) { MVFST_CHECK(false && "setup"); return invalid; }
    inv tmp; drain(lp.client_session, tmp);
    lp.ss.expect_sub_ns1 = "hold"; lp.ss.expect_sub_ns2 = "mvfst";
    lp.ss.expect_sub_track = "v";
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = {{(const uint8_t *)"hold", 4}, {(const uint8_t *)"mvfst", 5}};
    sc.track_namespace.parts = ns; sc.track_namespace.count = 2;
    sc.track_name = {(const uint8_t *)"v", 1};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t csub;
    if (moq_session_subscribe(lp.client_session, &sc, 0, &csub) < 0)
        { MVFST_CHECK(false && "subscribe"); return invalid; }
    lp.pump_client();
    std::atomic<bool> ok{false};
    bool w = lp.wait_for([&]() {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(lp.client_session, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_OK) {
                g_alias = ev[i].u.subscribe_ok.track_alias;
                ok.store(true);
            }
            moq_event_cleanup(&ev[i]);
        }
        return ok.load();
    });
    if (!w || !lp.ss.sub_accepted.load()) { MVFST_CHECK(false && "SUBSCRIBE_OK"); return invalid; }
    return lp.ss.sub_handle;
}

/* -- crafted subgroup streams for the injection rows --------------------- */

/* One subgroup stream carrying one object: header (subgroup id zero mode,
 * explicit priority) + object 0 with `payload`, in the given wire profile. */
static std::string craft_stream(moq_version_t v, uint64_t alias, uint64_t group,
                                const char *payload)
{
    uint8_t wire[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    size_t plen = std::strlen(payload);
    if (v == MOQ_VERSION_DRAFT_18) {
        moq_d18_subgroup_header_t h;
        std::memset(&h, 0, sizeof(h));
        h.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO;
        h.track_alias = alias;
        h.group_id = group;
        h.publisher_priority = 128;
        MVFST_CHECK(moq_d18_encode_subgroup_header(&w, &h) == MOQ_OK);
        MVFST_CHECK(moq_buf_write_vi64(&w, 0) == MOQ_OK);          /* object id delta */
        MVFST_CHECK(moq_buf_write_vi64(&w, plen) == MOQ_OK);
        MVFST_CHECK(moq_buf_write_raw(&w, (const uint8_t *)payload, plen) == MOQ_OK);
    } else {
        moq_d16_subgroup_header_t h;
        std::memset(&h, 0, sizeof(h));
        h.type = 0x10;   /* subgroup, id mode zero, explicit priority, no extensions */
        h.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_ZERO;
        h.track_alias = alias;
        h.group_id = group;
        h.publisher_priority = 128;
        MVFST_CHECK(moq_d16_encode_subgroup_header(&w, &h) == MOQ_OK);
        MVFST_CHECK(moq_d16_encode_object_fields(&w, 0, plen, (const uint8_t *)payload) == MOQ_OK);
    }
    return std::string((const char *)wire, moq_buf_writer_offset(&w));
}

/* Backing-store release counter for buffers the test hands to the adapter. */
static int g_freed = 0;
static void count_free(void *buf, void *)
{
    std::free(buf);
    g_freed++;
}

/* A buffer holding `bytes` whose backing allocation is `capacity` bytes
 * (capacity > bytes.size(): a small slice of a larger allocation). */
static std::unique_ptr<folly::IOBuf> owned_buf(const std::string &bytes, size_t capacity)
{
    void *p = std::malloc(capacity);
    std::memcpy(p, bytes.data(), bytes.size());
    return folly::IOBuf::takeOwnership(p, capacity, bytes.size(), count_free, nullptr);
}

/* The bytes split across two chained buffers. */
static std::unique_ptr<folly::IOBuf> chained_buf(const std::string &bytes)
{
    size_t cut = bytes.size() / 2;
    auto a = owned_buf(bytes.substr(0, cut), cut);
    auto b = owned_buf(bytes.substr(cut), bytes.size() - cut);
    a->appendToChain(std::move(b));
    return a;
}

static void inject(loopback_pair &lp, uint64_t sid, std::unique_ptr<folly::IOBuf> buf, bool eof)
{
    moq_mvfst_test_inject_uni_read(lp.client_adapter.get(), sid, std::move(buf), eof);
}

/* Injection rows share this start: a pool-of-one client, one subscription,
 * stream A admitted with a0 delivered (the pool is then full). */
struct injected_rig {
    loopback_pair lp;
    inv c;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    moq_subgroup_handle_t hA{};
    bool ok = false;
    explicit injected_rig(moq_version_t v, uint32_t max_events = 0)
        : lp(v, [max_events](moq_session_cfg_t &cfg) {
              cfg.max_data_streams = 1;
              if (max_events) cfg.max_events = max_events;
          })
    {
        MVFST_CHECK(lp.init_ok);
        if (!lp.init_ok) return;
        sub = subscribe(lp);
        if (!moq_subscription_is_valid(sub)) return;
        drain(lp.client_session, c);
        hA = open_write(lp, sub, 1, 0, "a0", false);
        MVFST_CHECK(lp.wait_for([&]{ drain(lp.client_session, c); return count_obj(c, 1, 0, "a0") == 1; }));
        ok = count_obj(c, 1, 0, "a0") == 1 && !moq_session_can_admit_data_stream(lp.client_session);
    }
    bool wait(std::function<bool()> pred) {
        return lp.wait_for([&]{ drain(lp.client_session, c); return pred(); });
    }
    void settle(int rounds = 20) {
        for (int i = 0; i < rounds; i++) { lp.pump_client(); lp.pump_server(); drain(lp.client_session, c); }
    }
};

static const uint64_t kInjectSid = 4003;   /* server-initiated uni id, never opened by the real server */

/* FIN carried by the refused read: held with its FIN, nothing else delivered
 * on the stream, and after admission the object and exactly one FINISHED
 * arrive with no further read on the stream. */
static void row_fin_in_refused(moq_version_t v)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v16";
    injected_rig r(v);
    if (!r.ok) return;
    std::string bytes = craft_stream(v, g_alias, 9, "x0");
    inject(r.lp, kInjectSid, folly::IOBuf::copyBuffer(bytes), true);
    hold_rec h;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_HELD, kInjectSid, h));
    MVFST_CHECK(h.len == bytes.size() && h.fin);
    r.settle();
    MVFST_CHECK(count_obj(r.c, 9, 0, "x0") == 0 && count_fin(r.c, 9) == 0);
    close_sg(r.lp, r.hA);
    MVFST_CHECK(r.wait([&]{ return count_fin(r.c, 1) == 1 && count_fin(r.c, 9) == 1; }));
    r.settle();
    MVFST_CHECK(count_obj(r.c, 9, 0, "x0") == 1 && count_fin(r.c, 9) == 1);
    hold_rec a;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_ACCEPTED, kInjectSid, a));
    MVFST_CHECK(a.len == bytes.size() && a.fin);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD) == 1 && r.c.resets.empty() && !r.lp.has_error());
    std::printf("%s fin-in-refused: held=%zu fin=%d -> x0 once, FINISHED(9) once\n", lbl, h.len, (int)h.fin);
}

/* Admission into retained WOULD_BLOCK with FIN: the client's event queue is
 * full when the held object+FIN chunk is admitted, the session retains it,
 * and the terminal settles from service() alone -- no further read. */
static void row_admission_would_block_fin(moq_version_t v)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v16";
    injected_rig r(v, /*max_events=*/2);
    if (!r.ok) return;
    std::string bytes = craft_stream(v, g_alias, 9, "w0");
    inject(r.lp, kInjectSid, folly::IOBuf::copyBuffer(bytes), true);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD, kInjectSid) == 1);
    /* fill the event queue: two objects on A, not polled */
    write_more(r.lp, r.hA, 1, "a1");
    write_more(r.lp, r.hA, 2, "a2");
    for (int i = 0; i < 40; i++) { r.lp.pump_client(); r.lp.pump_server(); }
    close_sg(r.lp, r.hA);
    for (int i = 0; i < 40; i++) { r.lp.pump_client(); r.lp.pump_server(); }
    /* poll exactly the two objects: A's FINISHED can now queue, A's entry
     * frees, the held chunk is admitted into a nearly full queue */
    moq_event_t ev; int polled = 0;
    while (polled < 2 && moq_session_poll_events(r.lp.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            auto &e = ev.u.object_received;
            r.c.objects.push_back({e.group_id, e.object_id,
                std::string((const char *)moq_rcbuf_data(e.payload), moq_rcbuf_len(e.payload))});
        }
        moq_event_cleanup(&ev); polled++;
    }
    for (int i = 0; i < 40; i++) { r.lp.pump_client(); r.lp.pump_server(); }
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED, kInjectSid) == 1);
    hold_rec replay;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_REPLAY, kInjectSid, replay));
    MVFST_CHECK(replay.result == MOQ_ERR_WOULD_BLOCK && replay.pending);
    MVFST_CHECK(replay.fin && replay.len == bytes.size());
    hold_rec held;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_HELD, kInjectSid, held));
    MVFST_CHECK(held.fin && held.len == replay.len);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_INPUT, kInjectSid) == 1);
    MVFST_CHECK(count_obj(r.c, 9, 0, "w0") == 0 && count_fin(r.c, 9) == 0);
    const int inputs_before_settle = count_phase(MOQ_MVFST_TEST_HOLD_INPUT);
    /* now drain fully: the retained chunk and its FIN settle from service() */
    for (int i = 0; i < 20; i++) {
        drain(r.lp.client_session, r.c);
        MVFST_CHECK(r.lp.client_adapter->service(0) == MOQ_OK);
    }
    drain(r.lp.client_session, r.c);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_REPLAY, kInjectSid) == 1);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED, kInjectSid) == 1);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_INPUT, kInjectSid) == 1);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_INPUT) == inputs_before_settle);
    MVFST_CHECK(count_obj(r.c, 1, 1, "a1") == 1 && count_obj(r.c, 1, 2, "a2") == 1 && count_fin(r.c, 1) == 1);
    MVFST_CHECK(count_obj(r.c, 9, 0, "w0") == 1 && count_fin(r.c, 9) == 1);
    MVFST_CHECK(r.c.objects.size() == 4 && r.c.finished.size() == 2 && r.c.closed == 0);
    MVFST_CHECK(moq_session_can_admit_data_stream(r.lp.client_session));
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD) == 1 && r.c.resets.empty() && !r.lp.has_error());
    std::printf("%s admission->would-block+fin: replay=%d pending=%d fin=%d; "
                "w0 once, FINISHED(9) once, service only, no further input\n",
                lbl, (int)replay.result, (int)replay.pending, (int)replay.fin);
}

/* Buffer shapes: a slice of a larger backing allocation and a two-buffer
 * chain. The adapter retains exactly the payload: the transport's backing
 * is released at the hold (counted by the free function), the held length
 * is the payload length, and the object arrives intact after admission. */
static void row_backing(moq_version_t v)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v16";
    injected_rig r(v);
    if (!r.ok) return;
    std::string b1 = craft_stream(v, g_alias, 9, "big-backing");
    std::string b2 = craft_stream(v, g_alias, 10, "chained");
    g_freed = 0;
    auto narrowed = owned_buf(b1, 1 << 16);
    narrowed->trimWritableTail(narrowed->tailroom());
    MVFST_CHECK(narrowed->capacity() == narrowed->length());
    inject(r.lp, kInjectSid, std::move(narrowed), true);
    MVFST_CHECK(g_freed == 1);                              /* 64 KiB backing released at the hold */
    hold_rec h1;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_HELD, kInjectSid, h1) && h1.len == b1.size());
    inject(r.lp, kInjectSid + 4, chained_buf(b2), true);
    MVFST_CHECK(g_freed == 3);                              /* both chain buffers released */
    hold_rec h2;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_HELD, kInjectSid + 4, h2) && h2.len == b2.size());
    close_sg(r.lp, r.hA);
    MVFST_CHECK(r.wait([&]{ return count_fin(r.c, 9) == 1 && count_fin(r.c, 10) == 1; }));
    MVFST_CHECK(count_obj(r.c, 9, 0, "big-backing") == 1 && count_obj(r.c, 10, 0, "chained") == 1);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED) == 2 && !r.lp.has_error());
    std::printf("%s backing: freed-at-hold=%d held=%zu/%zu payload bytes\n", lbl, 3, h1.len, h2.len);
}

/* Allocation failure at the named point: no acceptance, the transport's
 * buffer is released, the adapter is fatal, nothing is ever replayed, and
 * teardown with whatever is still held releases it exactly once. */
static void row_fail(moq_version_t v, int which)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v16";
    const char *what = which == MOQ_MVFST_TEST_FAIL_HOLD_RECORD ? "hold-record"
                     : which == MOQ_MVFST_TEST_FAIL_HOLD_COPY ? "hold-copy" : "replay";
    injected_rig r(v);
    if (!r.ok) return;
    std::string bytes = craft_stream(v, g_alias, 9, "f0");
    g_freed = 0;
    if (which == MOQ_MVFST_TEST_FAIL_REPLAY) {
        inject(r.lp, kInjectSid, owned_buf(bytes, bytes.size()), true);
        MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD, kInjectSid) == 1 && g_freed == 1);
        moq_mvfst_test_fail_next(which);
        close_sg(r.lp, r.hA);
        for (int i = 0; i < 40 && !r.lp.client_adapter->is_fatal(); i++) { r.lp.pump_client(); r.lp.pump_server(); }
        MVFST_CHECK(r.lp.client_adapter->is_fatal());
        MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED) == 0);
        MVFST_CHECK(g_freed == 1);                            /* transport backing already released */
    } else {
        moq_mvfst_test_fail_next(which);
        inject(r.lp, kInjectSid, owned_buf(bytes, 1 << 16), true);   /* oversized: the copy path */
        MVFST_CHECK(r.lp.client_adapter->is_fatal());
        MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD) == 0 && count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED) == 0);
        MVFST_CHECK(g_freed == 1);                            /* the transport's buffer released on the failure */
    }
    drain(r.lp.client_session, r.c);
    MVFST_CHECK(count_obj(r.c, 9, 0, "f0") == 0);
    /* nothing replays after the fatal */
    MVFST_CHECK(r.lp.client_adapter->service(0) < 0);
    for (int i = 0; i < 10; i++) { r.lp.pump_client(); drain(r.lp.client_session, r.c); }
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED) == 0 && count_obj(r.c, 9, 0, "f0") == 0);
    int freed_before = g_freed;
    r.lp.client_adapter.reset();
    if (which == MOQ_MVFST_TEST_FAIL_REPLAY) {
        MVFST_CHECK(g_freed == 1);                            /* no second transport release */
        MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_TEARDOWN, kInjectSid) == 1);
    } else {
        MVFST_CHECK(g_freed == freed_before);                 /* nothing left to release */
    }
    std::printf("%s fail-%s: fatal=%d accepted=%d freed=%d\n", lbl, what, 1,
                count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED), g_freed);
}

/* -- the scenario -------------------------------------------------------- */

static void run(moq_version_t v)
{
    const char *lbl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v16";
    inv c;
    auto wait_c = [&](loopback_pair &lp, std::function<bool()> pred) {
        return lp.wait_for([&]() { drain(lp.client_session, c); return pred(); });
    };

    loopback_pair lp(v, [](moq_session_cfg_t &cfg) { cfg.max_data_streams = 1; });
    MVFST_CHECK(lp.init_ok);
    if (!lp.init_ok) return;
    moq_subscription_t sub = subscribe(lp);
    if (!moq_subscription_is_valid(sub)) return;
    drain(lp.client_session, c);

    /* ---- phase 1: admission ----------------------------------------- */
    auto hA = open_write(lp, sub, 1, 0, "a0", false);
    MVFST_CHECK(wait_c(lp, [&]{ return count_obj(c, 1, 0, "a0") == 1; }));
    MVFST_CHECK(!moq_session_can_admit_data_stream(lp.client_session));

    auto hB = open_write(lp, sub, 2, 0, "b0", false);
    auto hC = open_write(lp, sub, 3, 0, "c0", false);
    MVFST_CHECK(wait_c(lp, [&]{ return held_sids().size() == 2; }));
    auto hs = held_sids();
    if (hs.size() != 2) { std::printf("%s: held=%zu -- unwinding\n", lbl, hs.size()); return; }
    uint64_t sB = hs[0], sC = hs[1];
    hold_rec hb, hc;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_HELD, sB, hb));
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_HELD, sC, hc));
    size_t lenB = hb.len, lenC = hc.len;
    MVFST_CHECK(lenB > 0 && lenC > 0);
    MVFST_CHECK(count_obj(c, 2, 0, "b0") == 0 && count_obj(c, 3, 0, "c0") == 0);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED) == 0);

    /* C's FIN arrives while its chunk is held: the paused stream delivers
     * nothing newer (no second hold, no FIN ahead of the bytes, no fatal) */
    close_sg(lp, hC);
    for (int i = 0; i < 40; i++) { lp.pump_client(); lp.pump_server(); drain(lp.client_session, c); }
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD) == 2);
    MVFST_CHECK(count_obj(c, 3, 0, "c0") == 0 && count_fin(c, 3) == 0);
    MVFST_CHECK(!lp.has_error());

    /* unrelated progress on the admitted stream while two chunks are held */
    write_more(lp, hA, 1, "a1");
    MVFST_CHECK(wait_c(lp, [&]{ return count_obj(c, 1, 1, "a1") == 1; }));
    MVFST_CHECK(count_obj(c, 2, 0, "b0") == 0 && count_obj(c, 3, 0, "c0") == 0);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD) == 2);   /* nothing newer read */
    MVFST_CHECK(!lp.has_error());

    /* capacity returns: A ends; the lower stream id (B) is redelivered first
     * and admitted, C is refused again on that pass and stays held */
    close_sg(lp, hA);
    MVFST_CHECK(wait_c(lp, [&]{ return count_obj(c, 2, 0, "b0") == 1 && count_fin(c, 1) == 1; }));
    hold_rec ab;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_ACCEPTED, sB, ab));
    MVFST_CHECK(ab.len == lenB && ab.fin == hb.fin);          /* the same chunk */
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_REFUSED_AGAIN, sC) >= 1);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED, sC) == 0);
    MVFST_CHECK(count_obj(c, 3, 0, "c0") == 0);
    MVFST_CHECK(!moq_session_can_admit_data_stream(lp.client_session));

    close_sg(lp, hB);
    MVFST_CHECK(wait_c(lp, [&]{ return count_obj(c, 3, 0, "c0") == 1 && count_fin(c, 3) == 1 && count_fin(c, 2) == 1; }));
    hold_rec ac;
    MVFST_CHECK(find_rec(MOQ_MVFST_TEST_HOLD_ACCEPTED, sC, ac));
    MVFST_CHECK(ac.len == lenC && ac.fin == hc.fin);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_HELD) == 2);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED) == 2);
    MVFST_CHECK(c.objects.size() == 4 && c.resets.empty() && c.closed == 0);
    MVFST_CHECK(moq_session_can_admit_data_stream(lp.client_session));
    MVFST_CHECK(!lp.has_error());
    std::printf("%s phase1: held B=%zu C=%zu bytes, C fin-with-chunk=%d, refused-again(C)=%d, objects=%zu\n",
                lbl, lenB, lenC, (int)hc.fin,
                count_phase(MOQ_MVFST_TEST_HOLD_REFUSED_AGAIN, sC), c.objects.size());

    /* ---- phase 2: peer RESET of a held stream --------------------------
     * The held stream is paused in mvfst, which does not dispatch readError
     * to a paused read callback; the reset is therefore observed either
     * before the redelivery (chunk dropped, nothing reaches the session) or,
     * once the chunk has been admitted after capacity returns, on the resumed
     * read (exactly one SUBGROUP_RESET after the chunk's objects). Both are
     * lossless and release the stream exactly once; neither may double
     * deliver, leak a held chunk, or go fatal. */
    auto hD = open_write(lp, sub, 4, 0, "d0", false);
    MVFST_CHECK(wait_c(lp, [&]{ return count_obj(c, 4, 0, "d0") == 1; }));
    auto hE = open_write(lp, sub, 5, 0, "e0", false);
    MVFST_CHECK(wait_c(lp, [&]{ return held_sids().size() == 3; }));
    hs = held_sids();
    uint64_t sE = hs.empty() ? 0 : hs.back();
    reset_sg(lp, hE, 7);
    close_sg(lp, hD);
    MVFST_CHECK(wait_c(lp, [&]{
        return count_fin(c, 4) == 1 &&
               (count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_RESET, sE) == 1 ||
                !c.resets.empty());
    }));
    /* settle: a few more pumps must not add anything */
    for (int i = 0; i < 20; i++) { lp.pump_client(); lp.pump_server(); drain(lp.client_session, c); }
    int dropped = count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_RESET, sE);
    int accepted = count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED, sE);
    MVFST_CHECK(dropped + accepted == 1);                 /* released exactly once */
    if (dropped == 1) {
        MVFST_CHECK(count_obj(c, 5, 0, "e0") == 0 && c.resets.empty());
        std::printf("%s phase2: reset observed while held -> chunk dropped, nothing admitted\n", lbl);
    } else {
        MVFST_CHECK(count_obj(c, 5, 0, "e0") == 1);
        MVFST_CHECK(c.resets.size() == 1 && c.resets[0].first == 5 && c.resets[0].second == 7);
        std::printf("%s phase2: reset observed after admission -> e0 once, one SUBGROUP_RESET(7)\n", lbl);
    }
    MVFST_CHECK(moq_session_can_admit_data_stream(lp.client_session));
    MVFST_CHECK(!lp.has_error());

    /* ---- phase 3: teardown with a held chunk ------------------------ */
    open_write(lp, sub, 6, 0, "f0", false);
    MVFST_CHECK(wait_c(lp, [&]{ return count_obj(c, 6, 0, "f0") == 1; }));
    open_write(lp, sub, 7, 0, "g0", false);
    MVFST_CHECK(wait_c(lp, [&]{ return held_sids().size() == 4; }));
    hs = held_sids();
    uint64_t sG = hs.empty() ? 0 : hs.back();
    MVFST_CHECK(count_obj(c, 7, 0, "g0") == 0);
    MVFST_CHECK(!lp.has_error());
    lp.client_adapter.reset();                          /* attach-mode teardown */
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_TEARDOWN, sG) == 1);
    MVFST_CHECK(count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_TEARDOWN) == 1);
    std::printf("%s: HELD=%d ACCEPTED=%d REFUSED_AGAIN=%d DROPPED_RESET=%d DROPPED_TEARDOWN=%d\n", lbl,
                count_phase(MOQ_MVFST_TEST_HOLD_HELD), count_phase(MOQ_MVFST_TEST_HOLD_ACCEPTED),
                count_phase(MOQ_MVFST_TEST_HOLD_REFUSED_AGAIN), count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_RESET),
                count_phase(MOQ_MVFST_TEST_HOLD_DROPPED_TEARDOWN));
}

int main(int argc, char **argv)
{
    google::InitGoogleLogging(argv[0]);
    warning_sink warnings;
    google::AddLogSink(&warnings);
    moq_version_t v = (moq_version_t)0;
    if (argc > 1 && std::strcmp(argv[1], "18") == 0) v = MOQ_VERSION_DRAFT_18;
    else if (argc > 1 && std::strcmp(argv[1], "16") == 0) v = MOQ_VERSION_DRAFT_16;
    const char *mode = argc > 2 ? argv[2] : "loopback";
    /* "force": advertise the capability regardless of the adapter's own
     * setting (the RED control against an adapter that does not hold). */
    if (std::strcmp(mode, "force") == 0) { moq_mvfst_test_set_hold_input(1); mode = "loopback"; }
    moq_mvfst_test_set_hold_observer(on_hold, nullptr);
    if (std::strcmp(mode, "loopback") == 0) run(v);
    else if (std::strcmp(mode, "fin") == 0) row_fin_in_refused(v);
    else if (std::strcmp(mode, "wb") == 0) row_admission_would_block_fin(v);
    else if (std::strcmp(mode, "backing") == 0) row_backing(v);
    else if (std::strcmp(mode, "fail-hold") == 0) row_fail(v, MOQ_MVFST_TEST_FAIL_HOLD_RECORD);
    else if (std::strcmp(mode, "fail-copy") == 0) row_fail(v, MOQ_MVFST_TEST_FAIL_HOLD_COPY);
    else if (std::strcmp(mode, "fail-replay") == 0) row_fail(v, MOQ_MVFST_TEST_FAIL_REPLAY);
    else { std::fprintf(stderr, "unknown mode %s\n", mode); return 2; }
    moq_mvfst_test_set_hold_observer(nullptr, nullptr);
    google::RemoveLogSink(&warnings);
    MVFST_CHECK(warnings.count.load() == 0);
    if (failures == 0) std::printf("PASS: mvfst_hold_input %s %s\n", argc > 1 ? argv[1] : "16", mode);
    return failures ? 1 : 0;
}
