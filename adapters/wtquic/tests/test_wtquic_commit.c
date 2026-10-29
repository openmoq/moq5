/* Adapter dispatch only: real session/bridge/adapter, fake WT callbacks.
 * Buffer return is not peer ACK. No provider, packet or wall-clock proof. */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>
#include <moq/wtquic.h>
#include "wtquic_stream_budget_fake.h"
#include "wtquic_attach_bootstrap.h"
#include "../../../tests/unit/test_alloc_support.h"

enum { STREAMS = 40, SENDS = 160, BYTES = 512 };
static unsigned failures, prerequisites, cases, endpoint_depth;
static const char *case_name;
static uint64_t clock_us;
static bool burst_active;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", case_name, __LINE__, #x); ++failures; \
} } while (0)
#define NEED(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE %s:%d: %s\n", case_name, __LINE__, #x); \
    ++prerequisites; return false; \
} } while (0)

struct wtq_stream {
    wtq_session_t *owner;
    wtq_stream_t *peer;
    bool local, bidi, closed, fin, opted, cancelled, committed;
    unsigned refs, writes, enable_calls, resets, aborts;
    unsigned accepted_resets;
    uint32_t reset_code;
};
struct send_record {
    wtq_stream_t *stream;
    uint8_t bytes[BYTES];
    size_t len;
    uint32_t flags;
    void *cookie;
    bool completed;
};
enum synchronous { SYNC_NONE, SYNC_COMMIT_CLOSE, SYNC_STOP_CLOSE };
enum feed_kind { FEED_COMMIT, FEED_STOP, FEED_STREAM_CLOSE, FEED_SESSION_CLOSE };
struct feed_record { enum feed_kind kind; uint64_t key; };
struct wtq_session {
    moq_wtquic_conn_t *conn;
    const wtq_session_events_t *registered_events;
    wtq_send_commit_mode_t mode;
    wtq_result_t enable_rc, reset_rc, open_rc;
    enum synchronous synchronous;
    unsigned refs, stream_count, send_count, relayed, closes, open_calls;
    unsigned commit_feeds, terminal_feeds, in_op_feeds;
    uint64_t last_commit_key;
    uint64_t reset_key;
    moq_transport_result_t reset_result;
    bool terminal, reset_from_hook;
    bool teardown_from_hook;
    unsigned teardown_hook_calls;
    bool burst_from_hook, burst_from_send, burst_stop, burst_duplicates;
    bool burst_close_session;
    unsigned burst_calls, burst_streams, feed_count;
    uint64_t burst_keys[STREAMS];
    struct feed_record feeds[4 * STREAMS];
    struct wtq_stream streams[STREAMS];
    struct send_record sends[SENDS];
};
struct side { wtq_session_t wt; moq_session_t *session; };
struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    struct side sides[2];
    struct side *sender, *receiver;
    moq_subscription_t sub;
    moq_rcbuf_t *payload;
};
static struct fixture *active;
static void close_stream(wtq_stream_t *st);
static void inject_commit(wtq_session_t *s, wtq_stream_t *st);
static void ack_fin(wtq_stream_t *st);
static void close_session_streams(wtq_session_t *s);
static void close_session(wtq_session_t *s);
static void service_hook(moq_wtquic_conn_t *conn, void *ctx);
static void callback_burst(wtq_session_t *s);

static int fake_clock_gettime(clockid_t id, struct timespec *ts)
{
    (void)id;
    ts->tv_sec = (time_t)(clock_us / 1000000u);
    ts->tv_nsec = (long)((clock_us % 1000000u) * 1000u);
    return 0;
}

wtq_result_t wtq_session_receive_contract(const wtq_session_t *s, size_t *n,
                                         wtq_receive_pause_mode_t *mode)
{
    if (!s || !n || !mode) return WTQ_ERR_INVALID_ARG;
    *n = 65535; *mode = WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED;
    return WTQ_OK;
}
wtq_send_commit_mode_t wtq_session_send_commit_mode(const wtq_session_t *s)
{ return s ? s->mode : WTQ_SEND_COMMIT_UNSUPPORTED; }
wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *s)
{ (void)s; return WTQ_LOCAL_FLUSH_UNSUPPORTED; }
wtq_result_t wtq_session_local_flush(const wtq_session_t *s)
{ return s ? WTQ_ERR_UNSUPPORTED : WTQ_ERR_INVALID_ARG; }
wtq_result_t wtq_stream_enable_send_commit(wtq_stream_t *st)
{
    if (!st) return WTQ_ERR_INVALID_ARG;
    ++st->enable_calls;
    if (st->owner->enable_rc != WTQ_OK) return st->owner->enable_rc;
    if (st->closed || !st->local || st->bidi || st->writes) return WTQ_ERR_STATE;
    const wtq_session_events_t *events = st->owner->registered_events
        ? st->owner->registered_events : moq_wtquic_conn_events();
    if (st->owner->mode != WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE ||
        !events->on_stream_send_committed) return WTQ_ERR_UNSUPPORTED;
    st->opted = true;
    return WTQ_OK;
}
void wtq_session_add_ref(wtq_session_t *s) { ++s->refs; }
void wtq_session_release(wtq_session_t *s)
{ CHECK(s->refs > 0); if (s->refs) --s->refs; }
void wtq_stream_add_ref(wtq_stream_t *st)
{ ++st->refs; wtq_session_add_ref(st->owner); }
void wtq_stream_release(wtq_stream_t *st)
{ CHECK(st->refs > 0); if (st->refs) --st->refs; wtq_session_release(st->owner); }
wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s)
{ return s->terminal ? WTQ_ERR_CLOSED : WTQ_OK; }
uint64_t wtq_stream_id(const wtq_stream_t *st)
{ (void)st; return WTQ_STREAM_ID_UNKNOWN; }
bool wtq_stream_is_bidi(const wtq_stream_t *st) { return st && st->bidi; }
bool wtq_stream_is_local(const wtq_stream_t *st) { return st && st->local; }
wtq_session_t *wtq_stream_session(const wtq_stream_t *st) { return st ? st->owner : NULL; }

static wtq_stream_t *new_stream(wtq_session_t *s, bool bidi, bool local)
{
    if (s->stream_count == STREAMS) return NULL;
    wtq_stream_t *st = &s->streams[s->stream_count++];
    st->owner = s; st->bidi = bidi; st->local = local; st->refs = 1;
    return st;
}
static wtq_result_t open_stream(wtq_session_t *s, bool bidi, wtq_stream_t **out)
{
    ++s->open_calls;
    *out = NULL;
    if (s->terminal) return WTQ_ERR_CLOSED;
    if (s->open_rc != WTQ_OK) return s->open_rc;
    *out = new_stream(s, bidi, true);
    return *out ? WTQ_OK : WTQ_ERR_STREAM_LIMIT;
}
wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{ return open_stream(s, false, out); }
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{ return open_stream(s, true, out); }

wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *spans,
                            size_t count, uint32_t flags, void *cookie)
{
    if (!st || st->closed || st->owner->terminal) return WTQ_ERR_CLOSED;
    if (st->fin) return WTQ_ERR_STATE;
    size_t len = 0;
    for (size_t i = 0; i < count; ++i) {
        if (spans[i].len > BYTES - len) return WTQ_ERR_TOO_LARGE;
        len += spans[i].len;
    }
    wtq_session_t *s = st->owner;
    if (s->send_count == SENDS) return WTQ_ERR_WOULD_BLOCK;
    struct send_record *r = &s->sends[s->send_count++];
    r->stream = st; r->len = len; r->flags = flags; r->cookie = cookie;
    size_t at = 0;
    for (size_t i = 0; i < count; ++i) {
        if (spans[i].len) memcpy(r->bytes + at, spans[i].data, spans[i].len);
        at += spans[i].len;
    }
    ++st->writes;
    if (flags & WTQ_SEND_FIN) {
        st->fin = true;
        ++endpoint_depth;
        if (s->burst_from_send) {
            s->burst_from_send = false;
            callback_burst(s);
        } else if (!st->bidi && !st->opted) {
            close_stream(st); /* The provider's unopted local-UNI legacy rule. */
        } else if (st->opted && s->synchronous == SYNC_COMMIT_CLOSE) {
            ack_fin(st);
            close_stream(st);
        } else if (st->opted && s->synchronous == SYNC_STOP_CLOSE) {
            st->cancelled = true;
            moq_wtquic_conn_events()->on_stream_stop(s, st, 0x77, s->conn);
            close_stream(st);
        }
        --endpoint_depth;
    }
    return WTQ_OK;
}
static wtq_result_t reset_stream(wtq_stream_t *st, uint32_t code, bool aborting)
{
    if (!st) return WTQ_ERR_INVALID_ARG;
    if (aborting) ++st->aborts; else ++st->resets;
    if (st->owner->reset_rc != WTQ_OK) return st->owner->reset_rc;
    if (st->closed) return WTQ_ERR_CLOSED;
    /* The provider clears the opted-in send half before notifying STOP. */
    if (st->cancelled || st->committed) return WTQ_ERR_STATE;
    st->reset_code = code; ++st->accepted_resets; st->cancelled = true;
    ++endpoint_depth;
    close_stream(st);
    --endpoint_depth;
    return WTQ_OK;
}
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t code)
{ return reset_stream(st, code, false); }
wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{ return reset_stream(st, code, true); }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t code)
{ (void)code; return st && !st->closed ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st)
{ return st && !st->closed ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st)
{ return st && !st->closed ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t code,
                              const uint8_t *reason, size_t len)
{
    (void)code; (void)reason; (void)len;
    ++s->closes;
    if (s->terminal) return WTQ_ERR_CLOSED;
    ++endpoint_depth;
    close_session(s);
    --endpoint_depth;
    return WTQ_OK;
}

moq_result_t wt_commit_probe(moq_transport_bridge_t *, uint64_t, uint64_t);
static moq_result_t terminal_probe(moq_transport_bridge_t *, uint64_t, uint64_t);
static moq_result_t stop_probe(moq_transport_bridge_t *, uint64_t, uint64_t, uint64_t);
static moq_result_t close_probe(moq_transport_bridge_t *, uint64_t, uint64_t);
static moq_result_t error_probe(moq_transport_bridge_t *, uint64_t, uint64_t);
/* Existing keys/open-close fixture convention: include the real adapter TU.
 * Probes count calls then invoke the unmodified shipping bridge function. */
#define clock_gettime fake_clock_gettime
#define moq_transport_bridge_on_local_stream_committed wt_commit_probe
#define moq_transport_bridge_on_peer_stream_terminal terminal_probe
#define moq_transport_bridge_on_peer_stop_sending stop_probe
#define moq_transport_bridge_on_transport_close close_probe
#define moq_transport_bridge_on_transport_error error_probe
/* Datagrams are not exercised here: reaching the provider's send is a test
 * failure, and the size query reports none available. */
wtq_result_t wtq_session_send_datagram(wtq_session_t *s, const wtq_span_t *spans,
                                       size_t count)
{
    (void)s; (void)spans; (void)count;
    CHECK(false);
    return WTQ_ERR_DGRAM_DISABLED;
}
size_t wtq_session_datagram_max_size(const wtq_session_t *s) { (void)s; return 0; }

#include "../wtquic_adapter.c"
#undef clock_gettime
#undef moq_transport_bridge_on_local_stream_committed
#undef moq_transport_bridge_on_peer_stream_terminal
#undef moq_transport_bridge_on_peer_stop_sending
#undef moq_transport_bridge_on_transport_close
#undef moq_transport_bridge_on_transport_error

static wtq_session_t *bridge_owner(moq_transport_bridge_t *b)
{
    for (unsigned i = 0; active && i < 2; ++i)
        if (active->sides[i].wt.conn && active->sides[i].wt.conn->bridge == b)
            return &active->sides[i].wt;
    return NULL;
}
static void service_hook(moq_wtquic_conn_t *conn, void *ctx)
{
    wtq_session_t *s = ctx;
    if (s->burst_from_hook) {
        s->burst_from_hook = false;
        callback_burst(s);
    }
    if (s->teardown_from_hook) {
        s->teardown_from_hook = false;
        ++s->teardown_hook_calls;
        unsigned feeds = s->feed_count;
        CHECK(conn->in_service);
        burst_active = true;
        close_session_streams(s);
        CHECK(conn->service_pending);
        CHECK(s->feed_count == feeds);
        burst_active = false;
    }
    if (!s->reset_from_hook) return;
    s->reset_from_hook = false;
    s->reset_result = conn->ops.reset_stream(conn, s->reset_key, 0x456);
}
static void record_feed(wtq_session_t *s, enum feed_kind kind, uint64_t key)
{
    CHECK(!burst_active);
    if (!s) return;
    CHECK(s->feed_count < sizeof(s->feeds) / sizeof(s->feeds[0]));
    if (s->feed_count < sizeof(s->feeds) / sizeof(s->feeds[0]))
        s->feeds[s->feed_count++] = (struct feed_record){.kind = kind, .key = key};
}
moq_result_t wt_commit_probe(moq_transport_bridge_t *b, uint64_t key, uint64_t now)
{
    wtq_session_t *s = bridge_owner(b);
    record_feed(s, FEED_COMMIT, key);
    if (s) {
        ++s->commit_feeds; s->last_commit_key = key;
        if (endpoint_depth) ++s->in_op_feeds;
    }
    return moq_transport_bridge_on_local_stream_committed(b, key, now);
}
static moq_result_t terminal_probe(moq_transport_bridge_t *b, uint64_t key, uint64_t now)
{
    wtq_session_t *s = bridge_owner(b);
    record_feed(s, FEED_STREAM_CLOSE, key);
    if (s) { ++s->terminal_feeds; if (endpoint_depth) ++s->in_op_feeds; }
    return moq_transport_bridge_on_peer_stream_terminal(b, key, now);
}
static moq_result_t stop_probe(moq_transport_bridge_t *b, uint64_t key,
                                uint64_t code, uint64_t now)
{
    wtq_session_t *s = bridge_owner(b);
    record_feed(s, FEED_STOP, key);
    if (s && endpoint_depth) ++s->in_op_feeds;
    return moq_transport_bridge_on_peer_stop_sending(b, key, code, now);
}
static moq_result_t close_probe(moq_transport_bridge_t *b, uint64_t code, uint64_t now)
{
    record_feed(bridge_owner(b), FEED_SESSION_CLOSE, code);
    CHECK(endpoint_depth == 0);
    return moq_transport_bridge_on_transport_close(b, code, now);
}
static moq_result_t error_probe(moq_transport_bridge_t *b, uint64_t code, uint64_t now)
{
    record_feed(bridge_owner(b), FEED_SESSION_CLOSE, code);
    CHECK(endpoint_depth == 0);
    return moq_transport_bridge_on_transport_error(b, code, now);
}
static void inject_commit(wtq_session_t *s, wtq_stream_t *st)
{
    const wtq_session_events_t *ev = moq_wtquic_conn_events();
    if (ev->on_stream_send_committed) ev->on_stream_send_committed(s, st, s->conn);
}
static void ack_fin(wtq_stream_t *st)
{
    if (!st->opted || !st->fin || st->closed || st->cancelled || st->committed) return;
    st->committed = true; /* Provider outcome is final before application code. */
    inject_commit(st->owner, st);
}
static void close_stream(wtq_stream_t *st)
{
    if (st->closed) return;
    st->closed = true;
    moq_wtquic_conn_events()->on_stream_closed(st->owner, st, st->owner->conn);
    CHECK(st->refs > 0);
    if (st->refs) --st->refs; /* Provider's own reference. */
}
static void close_session_streams(wtq_session_t *s)
{
    if (s->terminal) return;
    s->terminal = true;
    for (unsigned i = 0; i < s->stream_count; ++i) {
        s->streams[i].cancelled = true;
        close_stream(&s->streams[i]);
    }
}
static void close_session_result(wtq_session_t *s, uint32_t code, bool clean)
{
    if (s->terminal) return;
    close_session_streams(s);
    moq_wtquic_conn_events()->on_closed(s, code, NULL, 0, clean, s->conn);
}
static void close_session(wtq_session_t *s)
{
    close_session_result(s, 0, true);
}
static bool complete_one(wtq_session_t *s, bool cancelled)
{
    for (unsigned i = 0; i < s->send_count; ++i) {
        struct send_record *r = &s->sends[i];
        if (r->completed) continue;
        r->completed = true;
        moq_wtquic_conn_events()->on_send_complete(s, r->cookie, cancelled, s->conn);
        return true;
    }
    return false;
}
static uint64_t stream_key(wtq_stream_t *st)
{
    struct moq_wtq_stream *m = stream_find_by_handle(st->owner->conn, st);
    return m ? m->id : UINT64_MAX;
}
static bool mapped(struct side *s, uint64_t key)
{ return moq_transport_bridge_find_ref(s->wt.conn->bridge, key)._v != 0; }
static unsigned retained_slots(const moq_wtquic_conn_t *conn)
{
    unsigned n = 0;
    for (unsigned i = 0; i < TEST_STREAM_CAPACITY; ++i)
        if (conn->streams[i].in_use) ++n;
    return n;
}
static void callback_burst(wtq_session_t *s)
{
    moq_wtquic_conn_t *conn = s->conn;
    unsigned feeds = s->feed_count;
    CHECK(conn->in_service);
    CHECK(retained_slots(conn) == TEST_STREAM_CAPACITY);
    ++s->burst_calls;
    burst_active = true;
    for (unsigned i = 0; i < s->stream_count; ++i) {
        wtq_stream_t *st = &s->streams[i];
        if (!st->opted || !st->fin || st->closed) continue;
        uint64_t key = stream_key(st);
        CHECK(key != UINT64_MAX);
        unsigned n = s->burst_streams++;
        s->burst_keys[n] = key;
        if (s->burst_stop && n % 2) {
            st->cancelled = true;
            moq_wtquic_conn_events()->on_stream_stop(s, st, 0x77, conn);
            if (s->burst_duplicates)
                moq_wtquic_conn_events()->on_stream_stop(s, st, 0x77, conn);
        } else {
            ack_fin(st);
            if (s->burst_duplicates) inject_commit(s, st);
        }
        close_stream(st);
        /* Duplicate callbacks are defensive injections, not WT guarantees. */
        if (s->burst_duplicates)
            moq_wtquic_conn_events()->on_stream_closed(s, st, conn);
        CHECK(stream_find(conn, key) != NULL);
        CHECK(st->refs == 1); /* adapter lease, after provider close */
    }
    CHECK(s->feed_count == feeds);
    CHECK(retained_slots(conn) == TEST_STREAM_CAPACITY);
    CHECK(conn->term_count == 2 * s->burst_streams);
    CHECK(conn->term_count > TEST_STREAM_CAPACITY);
    CHECK(!conn->term_overflow);
    uint64_t rejected = UINT64_MAX;
    unsigned native_count = s->stream_count;
    CHECK(conn->ops.open_uni(conn, &rejected) == MOQ_TRANSPORT_WOULD_BLOCK);
    CHECK(rejected == UINT64_MAX && s->stream_count == native_count);
    if (s->burst_close_session) {
        close_session(s);
        /* Late success after the session terminal is never admissible. */
        for (unsigned i = 0; i < s->stream_count; ++i)
            inject_commit(s, &s->streams[i]);
        CHECK(s->feed_count == feeds);
    }
    burst_active = false;
}
static bool relay(struct side *from, struct side *to)
{
    wtq_session_t *s = &from->wt;
    while (s->relayed < s->send_count) {
        struct send_record *r = &s->sends[s->relayed++];
        wtq_stream_t *peer = r->stream->peer;
        if (!peer) {
            peer = new_stream(&to->wt, r->stream->bidi, false);
            NEED(peer != NULL);
            peer->peer = r->stream; r->stream->peer = peer;
            moq_wtquic_conn_events()->on_stream_opened(&to->wt, peer, peer->bidi, to->wt.conn);
        }
        moq_wtquic_conn_events()->on_stream_data(&to->wt, peer, r->bytes, r->len,
                                                 (r->flags & WTQ_SEND_FIN) != 0, to->wt.conn);
        if (!peer->bidi && (r->flags & WTQ_SEND_FIN)) close_stream(peer);
    }
    while (complete_one(s, false)) {}
    return true;
}
static bool pump(struct fixture *f)
{
    for (unsigned i = 0; i < 16; ++i) {
        moq_wtquic_conn_service(f->sides[0].wt.conn);
        moq_wtquic_conn_service(f->sides[1].wt.conn);
        if (!relay(&f->sides[0], &f->sides[1]) || !relay(&f->sides[1], &f->sides[0])) return false;
        if (f->sides[0].wt.relayed == f->sides[0].wt.send_count &&
            f->sides[1].wt.relayed == f->sides[1].wt.send_count) return true;
    }
    NEED(false);
}
static bool take(struct side *s, moq_event_kind_t kind, moq_event_t *ev)
{
    NEED(moq_session_poll_events(s->session, ev, 1) == 1);
    if (ev->kind != kind) {
        fprintf(stderr, "event got=%u expected=%u\n", (unsigned)ev->kind, (unsigned)kind);
        moq_event_cleanup(ev);
        NEED(false);
    }
    return true;
}
static bool setup(struct fixture *f, moq_version_t version,
                  wtq_send_commit_mode_t mode, unsigned timeout_ms)
{
    f->alloc = test_allocator(&f->memory);
    f->sender = &f->sides[1]; f->receiver = &f->sides[0];
    clock_us = 1000000;
    for (unsigned i = 0; i < 2; ++i) {
        struct side *s = &f->sides[i];
        s->wt.mode = mode; s->wt.refs = 1;
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), &f->alloc,
            i == 0 ? MOQ_PERSPECTIVE_CLIENT : MOQ_PERSPECTIVE_SERVER);
        cfg.version = version; cfg.send_request_capacity = true; cfg.initial_request_capacity = 16;
        NEED(moq_session_create(&cfg, clock_us, &s->session) == MOQ_OK);
        if (moq_session_binding_requirements(s->session)) {
            moq_transport_binding_t binding;
            moq_transport_binding_init(&binding);
            binding.kind = MOQ_BINDING_WEBTRANSPORT_H3;
            binding.facts_known = binding.facts_supported = MOQ_BINDING_FACTS_ALL;
            binding.target_present = MOQ_BINDING_TARGET_COMPLETE;
            binding.authority = MOQ_BYTES_LITERAL("test.invalid");
            binding.path_query = MOQ_BYTES_LITERAL("/moq");
            NEED(moq_session_bind_transport(s->session, &binding, sizeof(binding)) == MOQ_OK);
        }
        moq_wtquic_conn_cfg_t ac;
        moq_wtquic_conn_cfg_init_sized(&ac, sizeof(ac));
        ac.alloc = &f->alloc; ac.session = s->session; ac.wt_session = &s->wt;
        ac.hook = service_hook; ac.hook_user = &s->wt;
        NEED(moq_wtquic_conn_create(&ac, &s->wt.conn) == MOQ_OK);
    }
    for (unsigned i = 0; i < 2; ++i)
        moq_wtquic_conn_events()->on_established(&f->sides[i].wt,
            (wtq_str_t){.data = NULL, .len = 0}, f->sides[i].wt.conn);
    if (!pump(f)) return false;
    for (unsigned i = 0; i < 2; ++i) {
        moq_event_t ev;
        NEED(moq_session_state(f->sides[i].session) == MOQ_SESS_ESTABLISHED);
        if (!take(&f->sides[i], MOQ_EVENT_SETUP_COMPLETE, &ev)) return false;
        moq_event_cleanup(&ev);
    }
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t part = MOQ_BYTES_LITERAL("wt-commit");
    sc.track_namespace = (moq_namespace_t){&part, 1}; sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t remote;
    NEED(moq_session_subscribe(f->receiver->session, &sc, clock_us, &remote) == MOQ_OK);
    if (!pump(f)) return false;
    moq_event_t ev;
    if (!take(f->sender, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    f->sub = ev.u.subscribe_request.sub; moq_event_cleanup(&ev);
    const uint8_t d16[] = {0x02, 0x40, (uint8_t)timeout_ms};
    const uint8_t later[] = {0x06, (uint8_t)timeout_ms};
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.track_properties = version == MOQ_VERSION_DRAFT_16
        ? (moq_bytes_t){d16, sizeof(d16)} : (moq_bytes_t){later, sizeof(later)};
    NEED(moq_session_accept_subscribe(f->sender->session, f->sub, &accept, clock_us) == MOQ_OK);
    if (!pump(f) || !take(f->receiver, MOQ_EVENT_SUBSCRIBE_OK, &ev)) return false;
    moq_event_cleanup(&ev);
    NEED(moq_rcbuf_create(&f->alloc, (const uint8_t *)"payload", 7, &f->payload) == MOQ_OK);
    NEED(moq_session_next_deadline_us(f->sender->session) == UINT64_MAX);
    return true;
}
static void finish(struct fixture *f, unsigned before, unsigned pre)
{
    for (unsigned i = 0; i < 2; ++i) {
        struct side *s = &f->sides[i];
        if (s->wt.conn) {
            close_session(&s->wt);
            while (complete_one(&s->wt, true)) {}
            moq_wtquic_conn_destroy(s->wt.conn); s->wt.conn = NULL;
            for (unsigned j = 0; j < s->wt.stream_count; ++j) CHECK(s->wt.streams[j].refs == 0);
            CHECK(s->wt.refs == 1);
            CHECK(s->wt.in_op_feeds == 0);
        }
        if (s->session) moq_session_destroy(s->session);
    }
    if (f->payload) { CHECK(moq_rcbuf_refcount(f->payload) == 1); moq_rcbuf_decref(f->payload); }
    CHECK(f->memory.balance == 0);
    printf("%s %s failures=%u prerequisites=%u\n",
        failures == before && prerequisites == pre ? "PASS" : "FAIL", case_name,
        failures - before, prerequisites - pre);
    ++cases; active = NULL; free(f);
}
static bool group(struct fixture *f, uint64_t id, bool with_payload,
                  wtq_stream_t **out, uint64_t *key)
{
    struct side *s = f->sender;
    moq_subgroup_cfg_t cfg; moq_subgroup_cfg_init(&cfg); cfg.group_id = id;
    moq_subgroup_handle_t sg;
    unsigned n = s->wt.stream_count;
    NEED(moq_session_open_subgroup(s->session, f->sub, &cfg, clock_us, &sg) == MOQ_OK);
    moq_wtquic_conn_service(s->wt.conn);
    NEED(s->wt.stream_count == n + 1);
    *out = &s->wt.streams[n]; *key = stream_key(*out);
    NEED(*key != UINT64_MAX && mapped(s, *key));
    if (with_payload)
        NEED(moq_session_write_object(s->session, sg, 0, f->payload, clock_us) == MOQ_OK);
    moq_wtquic_conn_service(s->wt.conn);
    clock_us += 1000;
    NEED(moq_session_close_subgroup(s->session, sg, clock_us) == MOQ_OK);
    moq_wtquic_conn_service(s->wt.conn);
    NEED(s->wt.send_count > 0);
    struct send_record *last = &s->wt.sends[s->wt.send_count - 1];
    NEED(last->stream == *out && (last->flags & WTQ_SEND_FIN));
    NEED(last->len == 0); /* Public subgroup close emits a separate bare FIN. */
    return true;
}
static void pending(struct side *s, uint64_t key, uint64_t deadline)
{
    CHECK(mapped(s, key));
    CHECK(moq_session_next_deadline_us(s->session) == deadline);
}
enum row { ACK, TIMEOUT, ZERO, STOP, RESET, TEARDOWN, GENERIC, SYNC_ACK,
           SYNC_STOP, WRONG, FOREIGN, LEGACY };
static const char *const names[] = {"ack", "timeout", "zero-timeout", "stop",
    "reset", "teardown", "generic-close", "sync-ack-close", "sync-stop-close",
    "wrong-and-stale", "foreign-session", "unqualified-control"};

static void run(moq_version_t version, bool data, enum row row, wtq_send_commit_mode_t mode)
{
    char label[100];
    snprintf(label, sizeof(label), "D%u/%s/%s/mode%u", (unsigned)version,
        data ? "payload-group" : "empty-group", names[row], (unsigned)mode);
    case_name = label;
    unsigned before = failures, pre = prerequisites;
    struct fixture *f = calloc(1, sizeof(*f));
    if (!f) { ++prerequisites; return; }
    active = f;
    unsigned timeout = row == ZERO ? 0 : 100;
    if (!setup(f, version, mode, timeout)) goto done;
    struct side *s = f->sender;
    bool qualified = mode == WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE;
    CHECK(((s->wt.conn->ops.capabilities & MOQ_TRANSPORT_CAP_COMMIT_REPORT) != 0) == qualified);
    CHECK((s->wt.conn->ops.capabilities & MOQ_TRANSPORT_CAP_RELIABLE_RESET) == 0);
    if (qualified) CHECK(moq_wtquic_conn_events()->on_stream_send_committed != NULL);
    if (row == SYNC_ACK) s->wt.synchronous = SYNC_COMMIT_CLOSE;
    if (row == SYNC_STOP) s->wt.synchronous = SYNC_STOP_CLOSE;
    wtq_stream_t *st; uint64_t key;
    if (!group(f, 7, data, &st, &key)) goto done;
    uint64_t deadline = version == MOQ_VERSION_DRAFT_16 || timeout == 0
        ? UINT64_MAX : clock_us + timeout * 1000u;
    CHECK(st->opted == qualified);
    CHECK(qualified ? st->enable_calls == 1 : st->enable_calls == 0);
    if (row == LEGACY) {
        CHECK(!mapped(s, key)); CHECK(st->closed); CHECK(s->wt.commit_feeds == 0);
        goto done;
    }
    if (row == SYNC_ACK || row == SYNC_STOP) {
        while (complete_one(&s->wt, false)) {}
        CHECK(s->wt.in_op_feeds == 0);
        CHECK(!mapped(s, key));
        CHECK(moq_session_next_deadline_us(s->session) == UINT64_MAX);
        CHECK(s->wt.commit_feeds == (row == SYNC_ACK ? 1u : 0u));
        if (row == SYNC_ACK) CHECK(s->wt.last_commit_key == key);
        inject_commit(&s->wt, st);
        CHECK(s->wt.commit_feeds == (row == SYNC_ACK ? 1u : 0u));
        goto done;
    }
    pending(s, key, deadline);
    CHECK(s->wt.commit_feeds == 0); /* Accepted FIN is not peer ACK. */
    CHECK(complete_one(&s->wt, false));
    pending(s, key, deadline); /* One buffer returned. */
    while (complete_one(&s->wt, false)) {}
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(s->wt.commit_feeds == 0);
    pending(s, key, deadline); /* All buffers returned, still no ACK+FIN. */
    if (row == TIMEOUT) {
        clock_us = deadline - 1; moq_wtquic_conn_service(s->wt.conn);
        CHECK(st->accepted_resets == 0);
        clock_us = deadline; moq_wtquic_conn_service(s->wt.conn);
        CHECK(st->accepted_resets == 1);
        CHECK(st->reset_code == MOQ_RESET_DELIVERY_TIMEOUT);
        CHECK(!mapped(s, key));
    } else if (row == STOP) {
        st->cancelled = true;
        moq_wtquic_conn_events()->on_stream_stop(&s->wt, st, 0x77, s->wt.conn);
        CHECK(st->accepted_resets == 0);
        close_stream(st);
        CHECK(!mapped(s, key));
    } else if (row == RESET) {
        s->wt.reset_from_hook = true; s->wt.reset_key = key;
        s->wt.reset_result = MOQ_TRANSPORT_ERROR;
        moq_wtquic_conn_service(s->wt.conn);
        CHECK(!s->wt.reset_from_hook && s->wt.reset_result == MOQ_TRANSPORT_OK);
        CHECK(st->accepted_resets == 1 && st->reset_code == 0x456);
    } else if (row == TEARDOWN) {
        close_session(&s->wt);
        CHECK(moq_transport_bridge_is_terminal(s->wt.conn->bridge));
    } else if (row == GENERIC) {
        close_stream(st);
    } else if (row == FOREIGN) {
        const wtq_session_events_t *ev = moq_wtquic_conn_events();
        if (ev->on_stream_send_committed)
            ev->on_stream_send_committed(&f->receiver->wt, st, s->wt.conn);
        CHECK(s->wt.commit_feeds == 0);
        goto done;
    } else {
        if (row == WRONG) {
            wtq_stream_t unknown = {.owner = &s->wt, .local = true};
            inject_commit(&s->wt, &unknown);
            for (unsigned i = 0; i < s->wt.stream_count; ++i)
                if (&s->wt.streams[i] != st &&
                    (s->wt.streams[i].bidi || !s->wt.streams[i].local))
                    inject_commit(&s->wt, &s->wt.streams[i]);
            pending(s, key, deadline);
            CHECK(s->wt.commit_feeds == 0);
        }
        ack_fin(st);
        CHECK(s->wt.commit_feeds == 1 && s->wt.last_commit_key == key);
        CHECK(!mapped(s, key));
        CHECK(moq_session_next_deadline_us(s->session) == UINT64_MAX);
        close_stream(st); /* Real provider order: commit, then close. */
        wtq_stream_t *other; uint64_t other_key;
        if (!group(f, 8, !data, &other, &other_key)) goto done;
        CHECK(other_key != key);
        uint64_t next = version == MOQ_VERSION_DRAFT_16 || timeout == 0
            ? UINT64_MAX : clock_us + timeout * 1000u;
        unsigned commits = s->wt.commit_feeds;
        inject_commit(&s->wt, st); /* Duplicate retired handle must not hit B. */
        CHECK(s->wt.commit_feeds == commits);
        pending(s, other_key, next);
        while (complete_one(&s->wt, false)) {}
        ack_fin(other); close_stream(other);
        CHECK(s->wt.commit_feeds == 2 && s->wt.last_commit_key == other_key);
        CHECK(!mapped(s, other_key));
        CHECK(moq_session_next_deadline_us(s->session) == UINT64_MAX);
        goto done;
    }
    unsigned commits = s->wt.commit_feeds;
    inject_commit(&s->wt, st); inject_commit(&s->wt, st);
    CHECK(commits == 0 && s->wt.commit_feeds == 0);
done:
    finish(f, before, pre);
}

static void refusal(bool opt_in, wtq_result_t rc, bool aborting)
{
    char label[100];
    snprintf(label, sizeof(label), "D22/%s/%s/rc%d", opt_in ? "opt-in" : "reset",
        aborting ? "abort" : "reset", (int)rc);
    case_name = label;
    unsigned before = failures, pre = prerequisites;
    struct fixture *f = calloc(1, sizeof(*f));
    if (!f) { ++prerequisites; return; }
    active = f;
    if (!setup(f, MOQ_VERSION_DRAFT_22, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE, 100)) goto done;
    struct side *s = f->sender;
    uint64_t key = UINT64_MAX;
    unsigned count = s->wt.stream_count, sends = s->wt.send_count;
    if (opt_in) {
        s->wt.enable_rc = rc;
        CHECK(s->wt.conn->ops.open_uni(s->wt.conn, &key) == MOQ_TRANSPORT_ERROR);
        CHECK(key == UINT64_MAX);
        CHECK(s->wt.stream_count == count + 1);
        if (s->wt.stream_count > count) CHECK(s->wt.streams[count].enable_calls == 1);
        CHECK(s->wt.send_count == sends);
    } else {
        if (s->wt.conn->ops.open_uni(s->wt.conn, &key) != MOQ_TRANSPORT_OK) {
            ++prerequisites; goto done;
        }
        wtq_stream_t *st = &s->wt.streams[count];
        s->wt.reset_rc = rc;
        moq_transport_result_t result = aborting
            ? s->wt.conn->ops.abort_stream(s->wt.conn, key, 0x456)
            : s->wt.conn->ops.reset_stream(s->wt.conn, key, 0x456);
        CHECK(result == MOQ_TRANSPORT_ERROR);
        CHECK(st->accepted_resets == 0 && !st->closed);
        CHECK(stream_key(st) == key && s->wt.commit_feeds == 0);
        s->wt.reset_rc = WTQ_OK;
    }
done:
    finish(f, before, pre);
}
static void controls(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (unsigned i = 0; i < 3; ++i)
        for (unsigned data = 0; data < 2; ++data)
            run(versions[i], data != 0, LEGACY, WTQ_SEND_COMMIT_UNSUPPORTED);
    run(MOQ_VERSION_DRAFT_22, false, LEGACY, (wtq_send_commit_mode_t)99);
    run(MOQ_VERSION_DRAFT_22, true, LEGACY, (wtq_send_commit_mode_t)99);
}

static bool fetch_header_fin(struct fixture *f, bool qualified, bool synchronous)
{
    struct side *s = f->sender;
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init_sized(&cfg, sizeof(cfg));
    moq_bytes_t ns = MOQ_BYTES_LITERAL("wt-commit");
    cfg.track_namespace = (moq_namespace_t){&ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("v"); cfg.end_group = 1; cfg.end_object = 1;
    cfg.group_order = MOQ_GROUP_ORDER_ASCENDING;
    moq_fetch_t fetch;
    NEED(moq_session_fetch(f->receiver->session, &cfg, clock_us, &fetch) == MOQ_OK);
    if (!pump(f)) return false;
    moq_event_t ev;
    if (!take(s, MOQ_EVENT_FETCH_REQUEST, &ev)) return false;
    moq_fetch_t served = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);
    moq_accept_fetch_cfg_t accept;
    moq_accept_fetch_cfg_init(&accept);
    accept.empty = true; accept.end_group = 1; accept.end_object = 1;
    unsigned first = s->wt.send_count;
    uint64_t key = s->wt.conn->next_stream_key;
    s->wt.synchronous = synchronous ? SYNC_COMMIT_CLOSE : SYNC_NONE;
    NEED(moq_session_accept_fetch(s->session, served, &accept, clock_us) == MOQ_OK);
    moq_wtquic_conn_service(s->wt.conn);
    wtq_stream_t *st = NULL;
    for (unsigned i = first; i < s->wt.send_count; ++i) {
        struct send_record *r = &s->wt.sends[i];
        if (r->stream->local && !r->stream->bidi && (r->flags & WTQ_SEND_FIN)) {
            NEED(r->len > 0);
            NEED(st == NULL);
            st = r->stream;
        }
    }
    NEED(st != NULL);
    CHECK(st->opted == qualified);
    CHECK(qualified ? st->enable_calls == 1 : st->enable_calls == 0);
    if (!qualified) {
        CHECK(st->closed && !mapped(s, key) && s->wt.commit_feeds == 0);
        return true;
    }
    if (!synchronous) {
        CHECK(mapped(s, key)); CHECK(s->wt.commit_feeds == 0);
        while (complete_one(&s->wt, false)) {}
        CHECK(mapped(s, key)); CHECK(s->wt.commit_feeds == 0);
        ack_fin(st); close_stream(st);
    }
    CHECK(!mapped(s, key));
    CHECK(s->wt.commit_feeds == 1 && s->wt.last_commit_key == key);
    CHECK(s->wt.in_op_feeds == 0);
    inject_commit(&s->wt, st);
    CHECK(s->wt.commit_feeds == 1);
    return true;
}
static void data_fin(bool qualified, bool synchronous)
{
    char label[96];
    snprintf(label, sizeof(label), "D22/header-data-fin/%s/%s",
        qualified ? "qualified" : "unqualified", synchronous ? "sync-ack-close" : "ack-close");
    case_name = label;
    unsigned before = failures, pre = prerequisites;
    struct fixture *f = calloc(1, sizeof(*f));
    if (!f) { ++prerequisites; return; }
    active = f;
    if (setup(f, MOQ_VERSION_DRAFT_22, qualified
            ? WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE : WTQ_SEND_COMMIT_UNSUPPORTED, 100))
        (void)fetch_header_fin(f, qualified, synchronous);
    finish(f, before, pre);
}
static bool burst_body(struct fixture *f, bool from_send, bool stop,
                        bool duplicates, bool session_close)
{
    struct side *side = f->sender;
    wtq_session_t *s = &side->wt;
    unsigned base = retained_slots(s->conn);
    NEED(base < TEST_STREAM_CAPACITY);
    unsigned n = TEST_STREAM_CAPACITY - base;
    NEED(n > TEST_STREAM_CAPACITY / 2);
    unsigned first_feed = s->feed_count;
    s->burst_stop = stop; s->burst_duplicates = duplicates;
    s->burst_close_session = session_close;
    wtq_stream_t *streams[TEST_STREAM_CAPACITY] = {0};
    uint64_t keys[TEST_STREAM_CAPACITY] = {0};
    for (unsigned i = 0; i < n; ++i) {
        if (from_send && i + 1 == n) s->burst_from_send = true;
        if (!group(f, i + 1, true, &streams[i], &keys[i])) return false;
    }
    if (!from_send) {
        s->burst_from_hook = true;
        moq_wtquic_conn_service(s->conn);
    }
    CHECK(s->burst_calls == 1 && s->burst_streams == n);
    CHECK(s->conn->term_count == 0 && !s->conn->term_overflow);
    for (unsigned i = 0; i < n; ++i) {
        CHECK(s->burst_keys[i] == keys[i]);
        CHECK(!mapped(side, keys[i]));
        CHECK(stream_find(s->conn, keys[i]) == NULL);
        CHECK(streams[i]->refs == 0);
        CHECK(streams[i]->accepted_resets == 0);
        unsigned at = first_feed + 2 * i;
        CHECK(s->feed_count >= at + 2);
        CHECK(s->feeds[at].kind == (stop && i % 2 ? FEED_STOP : FEED_COMMIT));
        CHECK(s->feeds[at].key == keys[i]);
        CHECK(s->feeds[at + 1].kind == FEED_STREAM_CLOSE);
        CHECK(s->feeds[at + 1].key == keys[i]);
    }
    CHECK(s->commit_feeds == (stop ? (n + 1) / 2 : n));
    CHECK(moq_session_next_deadline_us(side->session) == UINT64_MAX);
    unsigned after = s->feed_count;
    if (session_close) {
        CHECK(s->conn->close_fed);
        CHECK(after == first_feed + 2 * n + base + 1);
        CHECK(s->feeds[after - 1].kind == FEED_SESSION_CLOSE);
        CHECK(retained_slots(s->conn) == 0);
        CHECK(moq_transport_bridge_is_terminal(s->conn->bridge));
    } else {
        CHECK(after == first_feed + 2 * n);
        CHECK(retained_slots(s->conn) == base);
        CHECK(!moq_transport_bridge_is_terminal(s->conn->bridge));
        if (moq_transport_bridge_is_terminal(s->conn->bridge)) return true;
        wtq_stream_t *next = NULL; uint64_t key = UINT64_MAX;
        if (!group(f, n + 1, true, &next, &key)) return false;
        CHECK(key > keys[n - 1] && mapped(side, key));
        for (unsigned i = 0; i < n; ++i) inject_commit(s, streams[i]);
        CHECK(s->feed_count == after && mapped(side, key));
        ack_fin(next); close_stream(next);
        CHECK(!mapped(side, key));
    }
    return true;
}
static void bursts(void)
{
    for (unsigned mode = 0; mode < 8; ++mode) {
        bool from_send = (mode & 1) != 0, stop = (mode & 2) != 0;
        bool duplicates = (mode & 4) != 0;
        char label[128];
        snprintf(label, sizeof(label), "D22/full-slots/%s/%s/%s",
            from_send ? "send-op" : "service-hook", stop ? "commit-stop" : "commit",
            duplicates ? "duplicate-injections" : "provider-shaped");
        case_name = label;
        unsigned before = failures, pre = prerequisites;
        struct fixture *f = calloc(1, sizeof(*f));
        if (!f) { ++prerequisites; return; }
        active = f;
        if (setup(f, MOQ_VERSION_DRAFT_22, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE, 0))
            (void)burst_body(f, from_send, stop, duplicates, false);
        finish(f, before, pre);
    }
    for (unsigned from_send = 0; from_send < 2; ++from_send) {
        case_name = from_send ? "D22/commit-before-session-close/send-op"
                              : "D22/commit-before-session-close/service-hook";
        unsigned before = failures, pre = prerequisites;
        struct fixture *f = calloc(1, sizeof(*f));
        if (!f) { ++prerequisites; return; }
        active = f;
        if (setup(f, MOQ_VERSION_DRAFT_22, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE, 0))
            (void)burst_body(f, from_send != 0, false, false, true);
        finish(f, before, pre);
    }
}
static bool bootstrap_body(struct fixture *f)
{
    struct side *side = f->sender;
    wtq_session_t *s = &side->wt;
    s->registered_events = wtq_test_attach_events();
    CHECK(s->registered_events->struct_size == sizeof(wtq_session_events_t));
    CHECK(s->registered_events->on_stream_send_committed != NULL);
    if (!s->registered_events->on_stream_send_committed) return true;
    wtq_stream_t *st = NULL; uint64_t key = UINT64_MAX;
    if (!group(f, 1, true, &st, &key)) return false;
    CHECK(st->opted);
    moq_wtquic_conn_t *absent = NULL;
    wtq_test_attach_t bootstrap = {.out = &absent};
    s->registered_events->on_stream_send_committed(s, st, &bootstrap);
    CHECK(s->commit_feeds == 0 && mapped(side, key));
    bootstrap.out = &s->conn;
    st->committed = true;
    s->registered_events->on_stream_send_committed(s, st, &bootstrap);
    CHECK(s->commit_feeds == 1 && s->last_commit_key == key);
    CHECK(!mapped(side, key));
    close_stream(st);
    s->registered_events->on_stream_send_committed(s, st, &bootstrap);
    CHECK(s->commit_feeds == 1);
    return true;
}
static void bootstrap(void)
{
    case_name = "D22/bootstrap/registered-commit-dispatch";
    unsigned before = failures, pre = prerequisites;
    struct fixture *f = calloc(1, sizeof(*f));
    if (!f) { ++prerequisites; return; }
    active = f;
    if (setup(f, MOQ_VERSION_DRAFT_22, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE, 100))
        (void)bootstrap_body(f);
    finish(f, before, pre);
}
static bool teardown_retry_body(struct fixture *f, bool queue_open,
                                bool clean, bool from_hook)
{
    struct side *s = f->sender;
    unsigned fin_pending = 0;
    for (unsigned i = 0; retained_slots(s->wt.conn) < TEST_STREAM_CAPACITY &&
                         i < TEST_STREAM_CAPACITY; ++i) {
        wtq_stream_t *st = NULL;
        uint64_t key = UINT64_MAX;
        if (!group(f, 10 + i, false, &st, &key)) return false;
        NEED(st->opted && st->fin && !st->closed && !st->committed && !st->cancelled);
        NEED(mapped(s, key));
        ++fin_pending;
    }
    NEED(retained_slots(s->wt.conn) == TEST_STREAM_CAPACITY);
    NEED(fin_pending > 0);
    NEED(!moq_transport_bridge_has_outbound_pending(s->wt.conn->bridge));
    unsigned opened_before = s->wt.stream_count;
    unsigned calls_before = s->wt.open_calls;
    if (queue_open) {
        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = 1000;
        moq_subgroup_handle_t pending_group;
        NEED(moq_session_open_subgroup(s->session, f->sub, &cfg, clock_us,
                                       &pending_group) == MOQ_OK);
        moq_wtquic_conn_service(s->wt.conn);
        NEED(s->wt.stream_count == opened_before);
        NEED(s->wt.open_calls == calls_before);
        NEED(moq_transport_bridge_has_outbound_pending(s->wt.conn->bridge));
    }
    NEED(!moq_transport_bridge_is_terminal(s->wt.conn->bridge));
    NEED(s->wt.commit_feeds == 0);
    unsigned first_feed = s->wt.feed_count;
    uint32_t code = clean ? 0 : 0x2a7;
    /* WT marks terminal, closes streams, then delivers session on_closed.
     * A blocked public open must survive retries during that interval. */
    if (from_hook) {
        s->wt.teardown_from_hook = true;
        moq_wtquic_conn_service(s->wt.conn);
        CHECK(s->wt.teardown_hook_calls == 1);
        CHECK(!s->wt.conn->in_service);
        CHECK(!s->wt.conn->close_pending && !s->wt.conn->close_fed);
        CHECK(!moq_transport_bridge_is_terminal(s->wt.conn->bridge));
        CHECK(s->wt.feed_count == first_feed + TEST_STREAM_CAPACITY);
        /* The provider's session outcome is deferred until its caller unwinds. */
        moq_wtquic_conn_events()->on_closed(&s->wt, code, NULL, 0, clean, s->wt.conn);
    } else {
        close_session_result(&s->wt, code, clean);
    }
    printf("observed %s filled_slots=%u fin_pending=%u provider_streams=%u "
           "fatal=%u closed=%u fatal_code=%" PRIu64 " commits=%u\n",
        case_name, (unsigned)TEST_STREAM_CAPACITY, fin_pending, opened_before,
        moq_transport_bridge_is_fatal(s->wt.conn->bridge) ? 1u : 0u,
        moq_transport_bridge_is_closed(s->wt.conn->bridge) ? 1u : 0u,
        moq_transport_bridge_fatal_code(s->wt.conn->bridge), s->wt.commit_feeds);
    CHECK(moq_transport_bridge_is_closed(s->wt.conn->bridge) == clean);
    CHECK(moq_transport_bridge_is_fatal(s->wt.conn->bridge) == !clean);
    CHECK(moq_transport_bridge_fatal_code(s->wt.conn->bridge) == code);
    CHECK(s->wt.commit_feeds == 0);
    CHECK(s->wt.stream_count == opened_before);
    CHECK(queue_open ? s->wt.open_calls > calls_before : s->wt.open_calls == calls_before);
    CHECK(s->wt.feed_count == first_feed + TEST_STREAM_CAPACITY + 1);
    if (s->wt.feed_count == first_feed + TEST_STREAM_CAPACITY + 1) {
        for (unsigned i = first_feed; i < s->wt.feed_count - 1; ++i)
            CHECK(s->wt.feeds[i].kind == FEED_STREAM_CLOSE);
        CHECK(s->wt.feeds[s->wt.feed_count - 1].kind == FEED_SESSION_CLOSE);
        CHECK(s->wt.feeds[s->wt.feed_count - 1].key == code);
    }
    CHECK(s->wt.conn->close_fed && retained_slots(s->wt.conn) == 0);
    CHECK(s->wt.conn->term_count == 0 && !s->wt.conn->term_overflow);
    return true;
}
static bool bare_open_body(struct fixture *f, wtq_result_t rc)
{
    struct side *s = f->sender;
    unsigned count = s->wt.stream_count, calls = s->wt.open_calls;
    unsigned slots = retained_slots(s->wt.conn);
    uint64_t key = UINT64_MAX;
    s->wt.open_rc = rc;
    CHECK(s->wt.conn->ops.open_uni(s->wt.conn, &key) == MOQ_TRANSPORT_ERROR);
    CHECK(key == UINT64_MAX && s->wt.stream_count == count);
    CHECK(s->wt.open_calls == calls + 1 && retained_slots(s->wt.conn) == slots);
    s->wt.open_rc = WTQ_OK;

    wtq_stream_t *st = NULL;
    if (!group(f, 1, false, &st, &key)) return false;
    close_stream(st);
    /* Generic close retires the adapter lease, not the core's uncommitted FIN. */
    CHECK(mapped(s, key) && !moq_transport_bridge_is_terminal(s->wt.conn->bridge));
    CHECK(stream_find(s->wt.conn, key) == NULL && st->refs == 0);
    CHECK(!s->wt.conn->close_pending && !s->wt.conn->close_fed);
    CHECK(!s->wt.conn->in_service);
    count = s->wt.stream_count; calls = s->wt.open_calls;
    key = UINT64_MAX;
    s->wt.open_rc = rc;
    /* A previous real stream close cannot bless a later unrelated CLOSED. */
    CHECK(s->wt.conn->ops.open_uni(s->wt.conn, &key) == MOQ_TRANSPORT_ERROR);
    CHECK(key == UINT64_MAX && s->wt.stream_count == count);
    CHECK(s->wt.open_calls == calls + 1 && retained_slots(s->wt.conn) == slots);

    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = 2;
    moq_subgroup_handle_t sg;
    NEED(moq_session_open_subgroup(s->session, f->sub, &cfg, clock_us, &sg) == MOQ_OK);
    moq_wtquic_conn_service(s->wt.conn);
    CHECK(moq_transport_bridge_is_fatal(s->wt.conn->bridge));
    CHECK(!moq_transport_bridge_is_closed(s->wt.conn->bridge));
    CHECK(moq_transport_bridge_fatal_code(s->wt.conn->bridge) == 1);
    CHECK(s->wt.stream_count == count && s->wt.open_calls > calls + 1);
    CHECK(s->wt.commit_feeds == 0);
    s->wt.open_rc = WTQ_OK;
    return true;
}
static void teardown_retries(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (unsigned v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
        for (unsigned mode = 0; mode < 8; ++mode) {
            bool pending_open = (mode & 1) != 0;
            bool clean = (mode & 2) == 0, from_hook = (mode & 4) != 0;
            char label[128];
            snprintf(label, sizeof(label), "D%u/%s/%s/%s",
                versions[v] == MOQ_VERSION_DRAFT_18 ? 18u : 22u,
                clean ? "clean-close" : "error-close-0x2a7",
                pending_open ? "blocked-open" : "no-blocked-open",
                from_hook ? "deferred-service-hook" : "direct");
            case_name = label;
            unsigned before = failures, pre = prerequisites;
            struct fixture *f = calloc(1, sizeof(*f));
            if (!f) { ++prerequisites; return; }
            active = f;
            if (setup(f, versions[v], WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE, 100))
                (void)teardown_retry_body(f, pending_open, clean, from_hook);
            finish(f, before, pre);
        }
        const wtq_result_t refusals[] = {WTQ_ERR_CLOSED, WTQ_ERR_STATE};
        for (unsigned r = 0; r < sizeof(refusals) / sizeof(refusals[0]); ++r) {
            char label[96];
            snprintf(label, sizeof(label), "D%u/bare-open/%s/fresh-and-after-stream-close",
                versions[v] == MOQ_VERSION_DRAFT_18 ? 18u : 22u,
                refusals[r] == WTQ_ERR_CLOSED ? "CLOSED" : "STATE");
            case_name = label;
            unsigned before = failures, pre = prerequisites;
            struct fixture *f = calloc(1, sizeof(*f));
            if (!f) { ++prerequisites; return; }
            active = f;
            if (setup(f, versions[v], WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE, 100))
                (void)bare_open_body(f, refusals[r]);
            finish(f, before, pre);
        }
    }
}
int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--teardown-retry") == 0) {
        teardown_retries();
    } else if (argc == 2 && strcmp(argv[1], "--bursts") == 0) {
        bursts();
    } else if (argc == 2 && strcmp(argv[1], "--bootstrap") == 0) {
        bootstrap();
    } else if (argc == 2 && strcmp(argv[1], "--d22-ack-row") == 0) {
        /* The draft-22 ACK row without object data; the default run
         * includes it in the full row sweep. */
        run(MOQ_VERSION_DRAFT_22, false, ACK, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE);
    } else if (argc == 2 && strcmp(argv[1], "--controls") == 0) {
        controls();
        data_fin(false, false);
    } else if (argc == 1) {
        controls();
        for (enum row row = ACK; row <= FOREIGN; ++row)
            for (unsigned data = 0; data < 2; ++data)
                run(MOQ_VERSION_DRAFT_22, data != 0, row, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE);
        for (unsigned data = 0; data < 2; ++data) {
            run(MOQ_VERSION_DRAFT_16, data != 0, ACK, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE);
            run(MOQ_VERSION_DRAFT_18, data != 0, ACK, WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE);
        }
        refusal(true, WTQ_ERR_UNSUPPORTED, false);
        refusal(true, WTQ_ERR_STATE, false);
        refusal(false, WTQ_ERR_STATE, false);
        refusal(false, WTQ_ERR_CLOSED, false);
        refusal(false, WTQ_ERR_STATE, true);
        refusal(false, WTQ_ERR_CLOSED, true);
        data_fin(false, false);
        data_fin(true, false);
        data_fin(true, true);
        bursts();
        bootstrap();
        teardown_retries();
    } else {
        fprintf(stderr, "usage: %s [--d22-ack-row|--controls|--bursts|--bootstrap|--teardown-retry]\n", argv[0]);
        return 2;
    }
    printf("wtquic_commit: %u cases, %u behavioral failures, %u prerequisite failures\n",
        cases, failures, prerequisites);
    return prerequisites ? 2 : failures ? 1 : 0;
}
