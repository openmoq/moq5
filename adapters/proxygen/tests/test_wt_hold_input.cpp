/*
 * Receive admission (MOQ_TRANSPORT_CAP_HOLD_INPUT) on the proxygen
 * WebTransport adapter, driven through FakeWebTransport with the real bridge
 * and session on both sides.
 *
 * The client session has a receive pool of ONE data stream. While that entry
 * is taken, every further peer uni data stream is refused at its first chunk;
 * the adapter must keep exactly that chunk (and its FIN), issue no further
 * read on the stream, and redeliver the same chunk once a service pass sees
 * the session able to admit again. The oracles are the fake's per-stream read
 * counts and remaining read queues (nothing newer is read while a chunk is
 * held), the session's exact object/terminal inventory (every object once,
 * byte-exact, one terminal per stream), and the adapter's state.
 *
 * Built from the adapter sources with MOQ_PROXYGEN_WT_TESTING so the
 * capability can be forced on (the RED control against an adapter that does
 * not hold) or off (pre-create rejection). No production symbol is
 * added for this.
 */
#include <moq/proxygen_wt.hpp>
#include <moq/session.h>
#include <moq/rcbuf.h>

#include "fake_webtransport.h"
#include "fake_wt_pair.h"
#include "wt_endpoint_ops.h"

#include <folly/executors/InlineExecutor.h>
#include <folly/io/IOBuf.h>

#include <cstdlib>

#include <cstdio>
#include <cstring>
#include <string>
#include <tuple>
#include <vector>

using namespace moq::wt;
using namespace moq::wt::testing;

static int failures = 0;

#define HI_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static const char *vlabel(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_18 ? "v18" : "v16";
}

/* -- observations -------------------------------------------------------- */

struct Obj {
    uint64_t group, object;
    std::string payload;
    bool operator==(const Obj &o) const
    { return group == o.group && object == o.object && payload == o.payload; }
};

struct Obs {
    std::vector<Obj> objects;
    std::vector<uint64_t> finished;   /* group ids */
    std::vector<std::pair<uint64_t, uint64_t>> resets; /* group, code */
    int sub_ok = 0;
    int closed = 0;
};

static void drain(moq_session_t *s, Obs &o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_OBJECT_RECEIVED: {
            auto &e = ev.u.object_received;
            std::string p;
            if (e.payload)
                p.assign((const char *)moq_rcbuf_data(e.payload),
                         moq_rcbuf_len(e.payload));
            o.objects.push_back({e.group_id, e.object_id, p});
            break;
        }
        case MOQ_EVENT_SUBGROUP_FINISHED:
            o.finished.push_back(ev.u.subgroup_finished.group_id);
            break;
        case MOQ_EVENT_SUBGROUP_RESET:
            o.resets.push_back({ev.u.subgroup_reset.group_id,
                                ev.u.subgroup_reset.error_code});
            break;
        case MOQ_EVENT_SUBSCRIBE_OK: o.sub_ok++; break;
        case MOQ_EVENT_SESSION_CLOSED: o.closed++; break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
}

static int count_obj(const Obs &o, uint64_t group, uint64_t object,
                     const char *payload)
{
    int n = 0;
    for (auto &x : o.objects)
        if (x == Obj{group, object, payload}) n++;
    return n;
}

static int count_in(const std::vector<uint64_t> &v, uint64_t g)
{
    int n = 0;
    for (auto x : v) if (x == g) n++;
    return n;
}

/* -- fixture ------------------------------------------------------------- */

struct Rig {
    FakeWtPair pair;
    moq_subscription_t client_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    Obs c;   /* client observations */
    std::vector<std::unique_ptr<FakeReadHandle>> handles;

    Rig(moq_version_t v, uint32_t max_events = 0)
        : pair(nullptr, /*client_max_data_streams=*/1, 0, v, max_events) {}

    bool up()
    {
        if (!pair.init_ok) return false;
        if (!pair.wait_setup()) return false;
        Obs tmp;
        drain(pair.client_session, tmp);
        drain(pair.server_session, tmp);

        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[] = {{(const uint8_t *)"hold", 4},
                            {(const uint8_t *)"wt", 2}};
        sc.track_namespace.parts = ns;
        sc.track_namespace.count = 2;
        sc.track_name = {(const uint8_t *)"v", 1};
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        if (moq_session_subscribe(pair.client_session, &sc, 0,
                                  &client_sub) < 0)
            return false;
        pair.client->service();
        if (!pair.pump_until()) return false;
        moq_event_t ev;
        while (moq_session_poll_events(pair.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                server_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                moq_session_accept_subscribe(pair.server_session, server_sub,
                                             &acfg, 0);
            }
            moq_event_cleanup(&ev);
        }
        if (!moq_subscription_is_valid(server_sub)) return false;
        pair.server->service();
        if (!pair.pump_until()) return false;
        drain(pair.client_session, c);
        return c.sub_ok == 1 && !pair.has_fatal();
    }

    /* The server's newest uni stream id after one more createUniStream. */
    uint64_t newest_uni() const
    {
        uint64_t best = 0;
        for (auto id : pair.server_wt.uni_stream_ids)
            if (id > best) best = id;
        return best;
    }

    /* Open a subgroup on the server and write one object; returns the handle
     * and the fake stream id it was opened on. Nothing is delivered yet. */
    moq_subgroup_handle_t open_write(uint64_t group, uint64_t oid,
                                      const char *payload, uint64_t *sid,
                                      bool close = false)
    {
        moq_subgroup_cfg_t sg;
        moq_subgroup_cfg_init(&sg);
        sg.group_id = group;
        sg.publisher_priority = 200;
        moq_subgroup_handle_t h;
        HI_CHECK(moq_session_open_subgroup(pair.server_session, server_sub, &sg,
                                        0, &h) >= 0);
        write(h, oid, payload);
        if (close) HI_CHECK(moq_session_close_subgroup(pair.server_session, h, 0) >= 0);
        pair.server->service();
        *sid = newest_uni();
        return h;
    }

    void write(moq_subgroup_handle_t h, uint64_t oid, const char *payload)
    {
        moq_rcbuf_t *b = nullptr;
        HI_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
                               std::strlen(payload), &b) >= 0);
        HI_CHECK(moq_session_write_object(pair.server_session, h, oid, b, 0) >= 0);
        moq_rcbuf_decref(b);
    }

    void close(moq_subgroup_handle_t h)
    {
        HI_CHECK(moq_session_close_subgroup(pair.server_session, h, 0) >= 0);
        pair.server->service();
    }

    /* Pull every recorded server write for `sid` out of the fake (so the pair
     * will not deliver it) and return the concatenated bytes + FIN flag. */
    std::string take_writes(uint64_t sid, bool *fin)
    {
        std::string out;
        *fin = false;
        auto &ws = pair.server_wt.writes;
        std::vector<RecordedWrite> keep;
        for (auto &w : ws) {
            if (w.stream_id != sid) { keep.push_back(std::move(w)); continue; }
            if (w.data) {
                auto c = w.data->cloneCoalesced();
                out.append((const char *)c->data(), c->length());
            }
            if (w.fin) *fin = true;
        }
        ws = std::move(keep);
        return out;
    }

    /* Present `sid` to the client adapter as a new peer uni stream whose
     * read queue the caller has already filled. */
    void present(uint64_t sid)
    {
        pair.client_known_streams.insert(sid);
        handles.push_back(std::make_unique<FakeReadHandle>(sid));
        pair.client->onNewUniStream(handles.back().get());
    }

    int reads(uint64_t sid) { return pair.client_wt.read_calls_by_id[sid]; }
    size_t queued(uint64_t sid) { return pair.client_wt.read_queues[sid].size(); }
    bool can_admit() { return moq_session_can_admit_data_stream(pair.client_session); }
    void pump() { HI_CHECK(pair.pump_until()); drain(pair.client_session, c); }
    void service_client() { pair.client->service(); drain(pair.client_session, c); }
};

/* -- rows ---------------------------------------------------------------- */

/* Pool of one. A is admitted; B (open) and C (closed, FIN behind) are refused
 * at their first chunk and held with no further reads. Unrelated traffic on A
 * keeps flowing. When A ends, the lower stream id (B) is redelivered first and
 * admitted; C is refused again on the same pass and stays held, untouched.
 * When B ends, C is admitted and completes. Exact inventory at the end. */
static void row_admission(moq_version_t v)
{
    int before = failures;
    Rig r(v);
    HI_CHECK(r.up());
    if (failures != before) return;

    uint64_t a, b, cc;
    auto hA = r.open_write(1, 0, "a0", &a);
    r.pump();
    HI_CHECK(count_obj(r.c, 1, 0, "a0") == 1);
    HI_CHECK(!r.can_admit());

    auto hB = r.open_write(2, 0, "b0", &b);
    r.open_write(3, 0, "c0", &cc, /*close=*/true);
    r.pump();
    HI_CHECK(b < cc);
    HI_CHECK(r.c.objects.size() == 1);                 /* nothing from B or C */
    HI_CHECK(r.reads(b) == 1 && r.reads(cc) == 1);     /* one read each: held */
    size_t qb = r.queued(b), qc = r.queued(cc);
    HI_CHECK(qb >= 1 && qc >= 1);                      /* the rest stays in the transport */
    HI_CHECK(!r.can_admit());
    HI_CHECK(r.pair.client_wt.stops.empty());          /* nothing STOPped */

    /* unrelated progress on the admitted stream while two chunks are held */
    r.write(hA, 1, "a1");
    r.pair.server->service();
    r.pump();
    HI_CHECK(count_obj(r.c, 1, 1, "a1") == 1);
    HI_CHECK(r.reads(b) == 1 && r.reads(cc) == 1);
    HI_CHECK(r.queued(b) == qb && r.queued(cc) == qc);

    /* capacity returns: A ends; B wins, C refused again on the same pass */
    r.close(hA);
    r.pump();
    HI_CHECK(count_in(r.c.finished, 1) == 1);
    HI_CHECK(count_obj(r.c, 2, 0, "b0") == 1);
    HI_CHECK(count_obj(r.c, 3, 0, "c0") == 0);
    HI_CHECK(r.reads(cc) == 1 && r.queued(cc) == qc);  /* repeat refusal: same chunk, no read */
    HI_CHECK(r.reads(b) >= 2);                         /* B resumed reading after admission */
    HI_CHECK(!r.can_admit());

    r.close(hB);
    r.pump();
    HI_CHECK(count_in(r.c.finished, 2) == 1);
    HI_CHECK(count_obj(r.c, 3, 0, "c0") == 1);
    HI_CHECK(count_in(r.c.finished, 3) == 1);
    HI_CHECK(r.c.objects.size() == 4);
    HI_CHECK(r.c.resets.empty() && r.c.closed == 0);
    HI_CHECK(r.can_admit());
    HI_CHECK(!r.pair.has_fatal());
    HI_CHECK(r.pair.client_wt.stops.empty());
    std::printf("admission %s: objects=%zu finished=%zu reads(b)=%d reads(c)=%d\n",
                vlabel(v), r.c.objects.size(), r.c.finished.size(),
                r.reads(b), r.reads(cc));
    if (failures == before) std::printf("PASS: wt hold_input admission %s\n", vlabel(v));
}

/* The refused chunk carries the FIN (one coalesced delivery). It is held with
 * its FIN, nothing else is read, and after admission the object and exactly
 * one SUBGROUP_FINISHED arrive. */
static void row_fin_with_payload(moq_version_t v)
{
    int before = failures;
    Rig r(v);
    HI_CHECK(r.up());
    if (failures != before) return;
    uint64_t a, x;
    auto hA = r.open_write(1, 0, "a0", &a);
    r.pump();
    r.open_write(5, 0, "x0", &x, /*close=*/true);
    bool fin = false;
    std::string raw = r.take_writes(x, &fin);
    HI_CHECK(fin && raw.size() > 2);
    r.pair.client_wt.queueRead(x, raw.data(), raw.size(), true);
    r.present(x);
    r.service_client();
    HI_CHECK(r.reads(x) == 1 && r.queued(x) == 0);
    HI_CHECK(count_obj(r.c, 5, 0, "x0") == 0 && count_in(r.c.finished, 5) == 0);
    r.pump();
    HI_CHECK(r.reads(x) == 1);                          /* still held, no read */
    r.close(hA);
    r.pump();
    HI_CHECK(count_obj(r.c, 5, 0, "x0") == 1);
    HI_CHECK(count_in(r.c.finished, 5) == 1);
    HI_CHECK(count_in(r.c.finished, 1) == 1);
    HI_CHECK(r.c.resets.empty() && !r.pair.has_fatal());
    if (failures == before) std::printf("PASS: wt hold_input fin-with-payload %s\n", vlabel(v));
}

/* A FIN-only delivery queued behind the held chunk is not read while the
 * chunk is held (FIN cannot overtake the bytes); after admission both are
 * consumed in order and the stream finishes exactly once. */
static void row_fin_only_behind_held(moq_version_t v)
{
    int before = failures;
    Rig r(v);
    HI_CHECK(r.up());
    if (failures != before) return;
    uint64_t a, x;
    auto hA = r.open_write(1, 0, "a0", &a);
    r.pump();
    r.open_write(6, 0, "y0", &x, /*close=*/true);
    bool fin = false;
    std::string raw = r.take_writes(x, &fin);
    HI_CHECK(fin);
    r.pair.client_wt.queueRead(x, raw.data(), raw.size(), false);
    r.pair.client_wt.queueRead(x, nullptr, 0, true);
    r.present(x);
    r.service_client();
    r.pump();
    HI_CHECK(r.reads(x) == 1 && r.queued(x) == 1);      /* the FIN-only read is still queued */
    HI_CHECK(count_in(r.c.finished, 6) == 0);
    r.close(hA);
    r.pump();
    HI_CHECK(count_obj(r.c, 6, 0, "y0") == 1);
    HI_CHECK(count_in(r.c.finished, 6) == 1);
    HI_CHECK(r.queued(x) == 0);
    HI_CHECK(!r.pair.has_fatal());
    if (failures == before) std::printf("PASS: wt hold_input fin-only-behind-held %s\n", vlabel(v));
}

/* Admission into retained WOULD_BLOCK with FIN: the client's event queue is
 * full when the held chunk (object + FIN) is finally admitted, so the session
 * retains it; the FIN is then bridge-owned and settles from service() alone,
 * with no further read on the stream. */
static void row_admission_to_would_block_with_fin(moq_version_t v)
{
    int before = failures;
    Rig r(v, /*max_events=*/2);
    HI_CHECK(r.up());
    if (failures != before) return;
    uint64_t a, x;
    auto hA = r.open_write(1, 0, "a0", &a);
    r.pump();
    HI_CHECK(count_obj(r.c, 1, 0, "a0") == 1);
    r.open_write(8, 0, "w0", &x, /*close=*/true);
    bool fin = false;
    std::string raw = r.take_writes(x, &fin);
    HI_CHECK(fin);
    r.pair.client_wt.queueRead(x, raw.data(), raw.size(), true);
    r.present(x);
    r.pair.client->service();                        /* not draining events from here on */
    HI_CHECK(r.reads(x) == 1);
    /* fill the client's event queue: two objects on A are not polled */
    r.write(hA, 1, "a1");
    r.write(hA, 2, "a2");
    r.pair.server->service();
    HI_CHECK(r.pair.pump_until());
    /* A ends: its FINISHED cannot be queued (events full) -- the entry frees
     * once the app drains. Drain exactly enough to let A finish, then stop. */
    r.close(hA);
    HI_CHECK(r.pair.pump_until());
    Obs tmp;
    moq_event_t ev;
    int polled = 0;
    while (polled < 2 && moq_session_poll_events(r.pair.client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            auto &e = ev.u.object_received;
            std::string p((const char *)moq_rcbuf_data(e.payload), moq_rcbuf_len(e.payload));
            r.c.objects.push_back({e.group_id, e.object_id, p});
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            r.c.finished.push_back(ev.u.subgroup_finished.group_id);
        }
        moq_event_cleanup(&ev);
        polled++;
    }
    HI_CHECK(r.pair.pump_until());                      /* A finishes; X admitted, then blocked on events */
    HI_CHECK(r.reads(x) == 1);                          /* no read was needed, none issued */
    /* Now the app drains everything; the retained chunk and its FIN settle
     * from service() passes alone. */
    for (int i = 0; i < 6; i++) { drain(r.pair.client_session, r.c); HI_CHECK(r.pair.pump_until()); }
    HI_CHECK(count_obj(r.c, 1, 1, "a1") == 1 && count_obj(r.c, 1, 2, "a2") == 1);
    HI_CHECK(count_in(r.c.finished, 1) == 1);
    HI_CHECK(count_obj(r.c, 8, 0, "w0") == 1);
    HI_CHECK(count_in(r.c.finished, 8) == 1);
    HI_CHECK(r.reads(x) == 1);
    HI_CHECK(!r.pair.has_fatal() && r.c.resets.empty());
    if (failures == before) std::printf("PASS: wt hold_input admission-to-would-block-fin %s\n", vlabel(v));
}

/* RESET while held, then capacity: the adapter never reads a held stream, so
 * proxygen surfaces the reset only when the read resumes after admission. The
 * held chunk (a bare subgroup header) is admitted, the resumed read reports
 * the reset, and exactly one SUBGROUP_RESET follows; no stale state, no
 * fatal. Then: session end with a chunk still held releases it (teardown). */
static void row_reset_then_teardown(moq_version_t v)
{
    int before = failures;
    Rig r(v);
    HI_CHECK(r.up());
    if (failures != before) return;
    uint64_t a, b, d;
    auto hA = r.open_write(1, 0, "a0", &a);
    r.pump();
    auto hB = r.open_write(2, 0, "b0", &b);
    r.pump();
    HI_CHECK(r.reads(b) == 1);
    HI_CHECK(moq_session_reset_subgroup(r.pair.server_session, hB, 7, 0) >= 0);
    r.pair.server->service();
    r.pump();
    HI_CHECK(r.reads(b) == 1);                          /* still held: no read while waiting */
    HI_CHECK(r.queued(b) == 1);                         /* the reset sits behind the held chunk */
    r.close(hA);
    r.pump();
    HI_CHECK(count_in(r.c.finished, 1) == 1);
    HI_CHECK(r.c.resets.size() == 1 && r.c.resets[0].first == 2 && r.c.resets[0].second == 7);
    HI_CHECK(count_obj(r.c, 2, 0, "b0") == 0);
    HI_CHECK(r.reads(b) == 2 && r.queued(b) == 0);
    HI_CHECK(!r.pair.has_fatal());
    HI_CHECK(r.can_admit());

    /* teardown with a held chunk */
    auto hC = r.open_write(3, 0, "c0", &d);
    r.pump();
    HI_CHECK(count_obj(r.c, 3, 0, "c0") == 1);          /* admitted: pool has one entry again */
    uint64_t e;
    r.open_write(4, 0, "d0", &e);
    r.pump();
    HI_CHECK(r.reads(e) == 1 && count_obj(r.c, 4, 0, "d0") == 0);
    (void)hC;
    r.pair.client->onSessionEnd(folly::none);
    HI_CHECK(r.pair.client->is_closed() && !r.pair.client->is_fatal());
    HI_CHECK(r.pair.client->service() == MOQ_OK);
    HI_CHECK(r.reads(e) == 1);
    if (failures == before) std::printf("PASS: wt hold_input reset-then-teardown %s\n", vlabel(v));
}

/* -- buffer shapes and allocation failures ------------------------------- */

static int g_freed = 0;
static void count_free(void *buf, void *) { std::free(buf); g_freed++; }

/* `bytes` in a buffer whose backing allocation is `capacity` bytes. */
static std::unique_ptr<folly::IOBuf> owned_buf(const std::string &bytes, size_t capacity)
{
    void *p = std::malloc(capacity);
    std::memcpy(p, bytes.data(), bytes.size());
    return folly::IOBuf::takeOwnership(p, capacity, bytes.size(), count_free, nullptr);
}

static std::unique_ptr<folly::IOBuf> chained_buf(const std::string &bytes)
{
    size_t cut = bytes.size() / 2;
    auto a = owned_buf(bytes.substr(0, cut), cut);
    auto b = owned_buf(bytes.substr(cut), bytes.size() - cut);
    a->appendToChain(std::move(b));
    return a;
}

/* A slice of a 64 KiB backing allocation and a two-buffer chain: the adapter
 * retains exactly the payload -- the transport's backing is released at the
 * hold (free function counted), and the objects arrive intact afterwards. */
static void row_backing(moq_version_t v)
{
    int before = failures;
    Rig r(v);
    HI_CHECK(r.up());
    if (failures != before) return;
    uint64_t a, x, y;
    auto hA = r.open_write(1, 0, "a0", &a);
    r.pump();
    r.open_write(11, 0, "big-backing", &x, true);
    bool fin = false;
    std::string b1 = r.take_writes(x, &fin);
    r.open_write(12, 0, "chained", &y, true);
    std::string b2 = r.take_writes(y, &fin);
    g_freed = 0;
    auto narrowed = owned_buf(b1, 1 << 16);
    narrowed->trimWritableTail(narrowed->tailroom());
    HI_CHECK(narrowed->capacity() == narrowed->length());
    r.pair.client_wt.queueReadBuf(x, std::move(narrowed), true);
    r.present(x);
    r.service_client();
    HI_CHECK(g_freed == 1 && r.reads(x) == 1);              /* backing released at the hold */
    r.pair.client_wt.queueReadBuf(y, chained_buf(b2), true);
    r.present(y);
    r.service_client();
    HI_CHECK(g_freed == 3 && r.reads(y) == 1);              /* both chain buffers released */
    HI_CHECK(count_obj(r.c, 11, 0, "big-backing") == 0 && count_obj(r.c, 12, 0, "chained") == 0);
    r.close(hA);
    r.pump();
    HI_CHECK(count_obj(r.c, 11, 0, "big-backing") == 1 && count_in(r.c.finished, 11) == 1);
    HI_CHECK(count_obj(r.c, 12, 0, "chained") == 1 && count_in(r.c.finished, 12) == 1);
    HI_CHECK(r.reads(x) == 1 && r.reads(y) == 1 && !r.pair.has_fatal());
    if (failures == before) std::printf("PASS: wt hold_input backing %s (freed-at-hold=%d)\n", vlabel(v), g_freed);
}

/* Allocation failure while recording the refused chunk: the adapter goes
 * fatal, the transport's buffer is released, nothing is accepted or replayed
 * later, and destruction releases nothing twice. */
static void row_fail_hold(moq_version_t v)
{
    int before = failures;
    Rig r(v);
    HI_CHECK(r.up());
    if (failures != before) return;
    uint64_t a, x;
    r.open_write(1, 0, "a0", &a);
    r.pump();
    r.open_write(13, 0, "f0", &x, true);
    bool fin = false;
    std::string b = r.take_writes(x, &fin);
    g_freed = 0;
    wt_test_fail_next(1);
    r.pair.client_wt.queueReadBuf(x, owned_buf(b, b.size()), true);
    r.present(x);
    HI_CHECK(r.pair.client->is_fatal());
    HI_CHECK(g_freed == 1);                                 /* released on the failure path */
    HI_CHECK(r.pair.client->service() != MOQ_OK);
    drain(r.pair.client_session, r.c);
    HI_CHECK(count_obj(r.c, 13, 0, "f0") == 0);
    if (failures == before) std::printf("PASS: wt hold_input fail-hold %s\n", vlabel(v));
}

/* Allocation failure in the redelivery snapshot: the held chunk stays owned
 * through the fatal and is released exactly once by destruction. */
static void row_fail_replay(moq_version_t v)
{
    int before = failures;
    g_freed = 0;
    {
        Rig r(v);
        HI_CHECK(r.up());
        if (failures != before) return;
        uint64_t a, x;
        auto hA = r.open_write(1, 0, "a0", &a);
        r.pump();
        r.open_write(14, 0, "r0", &x, true);
        bool fin = false;
        std::string b = r.take_writes(x, &fin);
        r.pair.client_wt.queueReadBuf(x, owned_buf(b, b.size()), true);
        r.present(x);
        r.service_client();
        HI_CHECK(r.reads(x) == 1 && g_freed == 1);
        wt_test_fail_next(2);
        r.close(hA);
        r.pair.pump();
        HI_CHECK(r.pair.client->is_fatal());
        HI_CHECK(g_freed == 1);                             /* no second transport release */
        drain(r.pair.client_session, r.c);
        HI_CHECK(count_obj(r.c, 14, 0, "r0") == 0);
        for (int i = 0; i < 5; i++) { r.pair.client->service(); drain(r.pair.client_session, r.c); }
        HI_CHECK(count_obj(r.c, 14, 0, "r0") == 0 && r.reads(x) == 1);
    }
    HI_CHECK(g_freed == 1);                                 /* destruction released nothing twice */
    if (failures == before) std::printf("PASS: wt hold_input fail-replay %s\n", vlabel(v));
}

/* Destruction with a chunk still held releases it exactly once. */
static void row_teardown_held(moq_version_t v)
{
    int before = failures;
    g_freed = 0;
    {
        Rig r(v);
        HI_CHECK(r.up());
        if (failures != before) return;
        uint64_t a, x;
        r.open_write(1, 0, "a0", &a);
        r.pump();
        r.open_write(15, 0, "t0", &x, false);
        bool fin = false;
        std::string b = r.take_writes(x, &fin);
        r.pair.client_wt.queueReadBuf(x, owned_buf(b, b.size()), false);
        r.present(x);
        r.service_client();
        HI_CHECK(r.reads(x) == 1 && g_freed == 1);
    }
    HI_CHECK(g_freed == 1);
    if (failures == before) std::printf("PASS: wt hold_input teardown-held %s\n", vlabel(v));
}

/* Test-only override applies before adapter/bridge construction. */
static void row_required_hold(moq_version_t v)
{
    int before = failures;
    wt_test_set_hold_input(0);
    {
        Rig r(v);
        HI_CHECK(r.pair.client_session != nullptr && r.pair.server_session != nullptr);
        HI_CHECK(!r.pair.init_ok && !r.pair.client && !r.pair.server);
        HI_CHECK(r.pair.client_wt.stops.empty());
        HI_CHECK(r.pair.client_wt.writes.empty());
        HI_CHECK(r.pair.client_wt.uni_stream_ids.empty());
        HI_CHECK(r.pair.client_wt.bidi_stream_ids.empty());
        HI_CHECK(r.c.objects.empty());
    }
    wt_test_set_hold_input(-1);
    {
        Rig r(v);
        HI_CHECK(r.up());
    }
    if (failures == before)
        std::printf("PASS: wt required HOLD_INPUT %s\n", vlabel(v));
}

/* Draft-18 on this adapter: the attach adapter routes MoQ control on the
 * first bidi stream only (no uni-control-pair routing), so a draft-18 pair
 * does not complete SETUP over the fake transport. This row pins that
 * precondition explicitly; when uni-control routing lands here, it fails and
 * the draft-18 schedules above must be enabled for this adapter. */
static void row_d18_unavailable(void)
{
    int before = failures;
    Rig r(MOQ_VERSION_DRAFT_18);
    bool up = r.up();
    std::printf("draft-18 setup over proxygen attach adapter: %s\n",
                up ? "COMPLETED (enable the draft-18 rows)" : "not reachable (no uni-control routing)");
    HI_CHECK(!up);
    if (failures == before)
        std::printf("PASS: wt hold_input draft-18 precondition pinned (rows not runnable here)\n");
}

int main(int argc, char **argv)
{
    /* Default: the adapter's own capability setting. "force" advertises the
     * capability regardless (the RED control against a non-holding adapter). */
    if (argc > 1 && std::strcmp(argv[1], "force") == 0)
        wt_test_set_hold_input(1);
    const moq_version_t v = MOQ_VERSION_DRAFT_16;
    row_admission(v);
    row_fin_with_payload(v);
    row_fin_only_behind_held(v);
    row_admission_to_would_block_with_fin(v);
    row_reset_then_teardown(v);
    row_backing(v);
    row_fail_hold(v);
    row_fail_replay(v);
    row_teardown_held(v);
    row_required_hold(v);
    row_d18_unavailable();
    if (failures == 0) std::printf("PASS: wt_hold_input\n");
    return failures ? 1 : 0;
}
