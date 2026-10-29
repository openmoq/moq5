/* Public-start close/open ordering, using real adapter/bridge and fake WT. */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <moq/session.h>
#include <moq/transport_bridge.h>
#include <moq/wtquic.h>
#include "wtquic_stream_budget_fake.h"

enum {
    CLEAN_CODE = 0x42,
    UNCLEAN_CODE = 0x53,
    OPEN_BOUND = 64,
    STREAM_BOUND = 8,
    HOOK_BOUND = 64,
    EVENT_BOUND = 8
};
enum test_mode {
    CLEAN_IN_OPEN,
    UNCLEAN_IN_OPEN,
    FOREIGN_IN_OPEN,
    CLOSED_BARE,
    STATE_BARE,
    BACKEND_BARE,
    CREDIT_LIMIT,
    CREDIT_BLOCK,
    SUCCESS,
    CLEAN_BEFORE_SERVICE,
    CLEAN_POST_HOOK,
    UNCLEAN_POST_HOOK,
    MODE_COUNT
};
static const char *const names[MODE_COUNT] = {
    "clean-in-open", "unclean-in-open", "foreign-session-in-open",
    "closed-no-callback", "state-no-callback", "backend-error",
    "stream-credit", "would-block", "success", "clean-before-service",
    "clean-post-hook", "unclean-post-hook"
};
static unsigned failures;
static unsigned row_version;
static const char *row_name;

static bool check(bool condition, const char *expression, int line)
{
    if (!condition) {
        printf("FAIL D%u/%s line %d: %s\n", row_version, row_name, line, expression);
        failures++;
    }
    return condition;
}
#define CHECK(expr) check((expr), #expr, __LINE__)

struct observed { unsigned closes; uint64_t code; };
struct wtq_stream {
    wtq_session_t *session;
    bool bidi;
    bool live;
    unsigned refs;
};
struct wtq_session {
    moq_wtquic_conn_t *conn;
    wtq_session_t *foreign;
    enum test_mode mode;
    wtq_result_t open_rc;
    bool terminal;
    unsigned refs;
    unsigned uni_opens;
    unsigned bidi_opens;
    unsigned successful_opens;
    unsigned sends;
    unsigned callbacks_from_open;
    unsigned callbacks_from_close;
    unsigned foreign_callbacks;
    unsigned close_calls;
    uint32_t last_close_code;
    bool retained_in_open;
    bool foreign_rejected_in_open;
    unsigned hook_calls;
    unsigned first_hook_closes;
    uint64_t first_hook_code;
    unsigned first_hook_close_calls;
    unsigned first_hook_opens;
    bool in_open;
    bool close_feed_seen;
    unsigned close_feeds;
    unsigned initial_credit_refusals;
    unsigned terminal_at_hook;
    unsigned admission_before_feed;
    unsigned resume_before_feed;
    unsigned input_before_feed;
    unsigned teardown_before_feed;
    struct observed observed;
    struct wtq_stream streams[STREAM_BOUND];
    unsigned stream_count;
};

static void emit_terminal(wtq_session_t *s, uint32_t code, bool clean);
static wtq_result_t fake_open(wtq_session_t *s, bool bidi, wtq_stream_t **out);
static wtq_session_t *active_wt;
static moq_result_t observe_close_feed(moq_transport_bridge_t *bridge,
                                       uint64_t code, uint64_t now);
static moq_result_t observe_error_feed(moq_transport_bridge_t *bridge,
                                       uint64_t code, uint64_t now);
static moq_result_t observe_uni_input(moq_transport_bridge_t *bridge, uint64_t id,
                                      const uint8_t *data, size_t len, bool fin,
                                      uint64_t now);

static bool before_close_feed(const wtq_session_t *s)
{
    return s->terminal && !s->close_feed_seen;
}

wtq_result_t wtq_session_receive_contract(const wtq_session_t *s,
                                         size_t *quantum,
                                         wtq_receive_pause_mode_t *mode)
{
    if (!s || !quantum || !mode) return WTQ_ERR_INVALID_ARG;
    *quantum = 65535;
    *mode = WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED;
    return WTQ_OK;
}
void wtq_session_add_ref(wtq_session_t *s) { s->refs++; }
void wtq_session_release(wtq_session_t *s)
{
    if (CHECK(s->refs > 0)) s->refs--;
}
wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s)
{
    if (before_close_feed(s)) s->admission_before_feed++;
    return s->terminal ? WTQ_ERR_CLOSED : WTQ_OK;
}
void wtq_stream_add_ref(wtq_stream_t *st)
{ st->refs++; wtq_session_add_ref(st->session); }
void wtq_stream_release(wtq_stream_t *st)
{
    if (CHECK(st->refs > 0)) {
        st->refs--;
        wtq_session_release(st->session);
    }
}
wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{ return fake_open(s, false, out); }
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{ return fake_open(s, true, out); }
uint64_t wtq_stream_id(const wtq_stream_t *st)
{ (void)st; return WTQ_STREAM_ID_UNKNOWN; }
bool wtq_stream_is_bidi(const wtq_stream_t *st) { return st->bidi; }
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t code)
{ (void)code; return st->live ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{ (void)code; return st->live ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t code)
{ (void)code; return st->live ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st)
{ return st->live ? WTQ_OK : WTQ_ERR_CLOSED; }
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st)
{
    if (before_close_feed(st->session)) st->session->resume_before_feed++;
    return st->live ? WTQ_OK : WTQ_ERR_CLOSED;
}
wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *spans,
                            size_t count, uint32_t flags, void *ctx)
{
    (void)spans; (void)count; (void)flags;
    if (!st->live) return WTQ_ERR_CLOSED;
    st->session->sends++;
    moq_wtquic_conn_events()->on_send_complete(st->session, ctx, false,
                                               st->session->conn);
    return WTQ_OK;
}
void wtq_session_events_init_ex(wtq_session_events_t *events, size_t size)
{
    if (!events || size < sizeof(events->struct_size)) return;
    size_t n = size < sizeof(*events) ? size : sizeof(*events);
    memset(events, 0, n);
    events->struct_size = (uint32_t)n;
}
wtq_send_commit_mode_t wtq_session_send_commit_mode(const wtq_session_t *s)
{ (void)s; return WTQ_SEND_COMMIT_UNSUPPORTED; }
wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *s)
{ (void)s; return WTQ_LOCAL_FLUSH_UNSUPPORTED; }
wtq_result_t wtq_session_local_flush(const wtq_session_t *s)
{ return s ? WTQ_ERR_UNSUPPORTED : WTQ_ERR_INVALID_ARG; }
wtq_result_t wtq_stream_enable_send_commit(wtq_stream_t *st)
{ (void)st; return WTQ_ERR_UNSUPPORTED; }
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t code,
                              const uint8_t *reason, size_t len)
{
    (void)reason; (void)len;
    if (before_close_feed(s)) s->teardown_before_feed++;
    s->close_calls++;
    s->last_close_code = code;
    if (s->terminal) return WTQ_ERR_CLOSED;
    s->callbacks_from_close++;
    emit_terminal(s, code, true);
    return WTQ_OK;
}

/* Same pattern as the keys test; static adapter state proves attribution only. */
#define moq_transport_bridge_on_transport_close observe_close_feed
#define moq_transport_bridge_on_transport_error observe_error_feed
#define moq_transport_bridge_on_peer_uni_bytes observe_uni_input
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
#undef moq_transport_bridge_on_transport_close
#undef moq_transport_bridge_on_transport_error
#undef moq_transport_bridge_on_peer_uni_bytes

/* Observe calls without changing their real shipping-bridge behavior. */
static void observe_terminal_feed(void)
{
    if (active_wt && active_wt->terminal) {
        CHECK(!active_wt->in_open);
        active_wt->close_feed_seen = true;
        active_wt->close_feeds++;
    }
}

static moq_result_t observe_close_feed(moq_transport_bridge_t *bridge,
                                       uint64_t code, uint64_t now)
{
    observe_terminal_feed();
    return moq_transport_bridge_on_transport_close(bridge, code, now);
}

static moq_result_t observe_error_feed(moq_transport_bridge_t *bridge,
                                       uint64_t code, uint64_t now)
{
    observe_terminal_feed();
    return moq_transport_bridge_on_transport_error(bridge, code, now);
}

static moq_result_t observe_uni_input(moq_transport_bridge_t *bridge, uint64_t id,
                                      const uint8_t *data, size_t len, bool fin,
                                      uint64_t now)
{
    if (active_wt && before_close_feed(active_wt)) active_wt->input_before_feed++;
    return moq_transport_bridge_on_peer_uni_bytes(bridge, id, data, len, fin, now);
}

static void emit_terminal(wtq_session_t *s, uint32_t code, bool clean)
{
    if (!CHECK(!s->terminal)) return;
    s->terminal = true;
    for (unsigned i = 0; i < s->stream_count; i++) {
        if (s->streams[i].live) {
            s->streams[i].live = false;
            moq_wtquic_conn_events()->on_stream_closed(s, &s->streams[i], s->conn);
        }
    }
    static const uint8_t reason[] = "provider terminal";
    moq_wtquic_conn_events()->on_closed(s, code, reason, sizeof(reason) - 1,
                                       clean, s->conn);
}

static wtq_result_t fake_open(wtq_session_t *s, bool bidi, wtq_stream_t **out)
{
    *out = NULL;
    if (bidi) s->bidi_opens++; else s->uni_opens++;
    if (!CHECK(s->bidi_opens + s->uni_opens <= OPEN_BOUND)) {
        fflush(stdout);
        abort();
    }
    bool post_hook = s->mode == CLEAN_POST_HOOK || s->mode == UNCLEAN_POST_HOOK;
    if (post_hook && s->hook_calls == 0) {
        CHECK(s->bidi_opens + s->uni_opens == 1);
        CHECK(!s->terminal && s->callbacks_from_open == 0 && s->close_calls == 0);
        s->initial_credit_refusals++;
        return WTQ_ERR_STREAM_LIMIT;
    }
    bool attributed = s->mode == CLEAN_IN_OPEN || s->mode == UNCLEAN_IN_OPEN || post_hook;
    if (attributed && !s->terminal) {
        if (post_hook) {
            CHECK(s->hook_calls == 1 && s->initial_credit_refusals == 1);
            CHECK(s->bidi_opens + s->uni_opens == 2);
        }
        bool clean = s->mode == CLEAN_IN_OPEN || s->mode == CLEAN_POST_HOOK;
        uint32_t code = clean ? CLEAN_CODE : UNCLEAN_CODE;
        s->callbacks_from_open++;
        s->terminal_at_hook = s->hook_calls;
        s->in_open = true;
        emit_terminal(s, code, clean);
        s->retained_in_open = s->conn->in_service &&
            s->conn->close_pending && !s->conn->close_fed &&
            s->conn->close_clean == clean && s->conn->close_code == code;
        s->in_open = false;
    }
    if (s->mode == FOREIGN_IN_OPEN && !s->foreign_callbacks) {
        s->foreign_callbacks++;
        emit_terminal(s->foreign, CLEAN_CODE, true);
        s->foreign_rejected_in_open = s->conn->in_service &&
            s->conn->misrouted && !s->conn->close_pending && !s->conn->close_fed;
    }
    if (s->terminal) return WTQ_ERR_CLOSED;
    if (s->open_rc != WTQ_OK) return s->open_rc;
    if (!CHECK(s->stream_count < STREAM_BOUND)) return WTQ_ERR_STREAM_LIMIT;
    wtq_stream_t *st = &s->streams[s->stream_count++];
    st->session = s;
    st->bidi = bidi;
    st->live = true;
    *out = st;
    s->successful_opens++;
    return WTQ_OK;
}

struct alloc_balance { size_t blocks; size_t bytes; };
static void *count_alloc(size_t size, void *ctx)
{
    struct alloc_balance *balance = ctx;
    void *p = malloc(size);
    if (p) { balance->blocks++; balance->bytes += size; }
    return p;
}
static void *count_realloc(void *p, size_t old_size, size_t new_size, void *ctx)
{
    struct alloc_balance *balance = ctx;
    if (!p) return count_alloc(new_size, ctx);
    if (!CHECK(balance->blocks > 0 && balance->bytes >= old_size)) return NULL;
    if (!new_size) {
        free(p);
        balance->blocks--;
        balance->bytes -= old_size;
        return NULL;
    }
    void *next = realloc(p, new_size);
    if (next) balance->bytes = balance->bytes - old_size + new_size;
    return next;
}
static void count_free(void *p, size_t size, void *ctx)
{
    struct alloc_balance *balance = ctx;
    if (!p) return;
    CHECK(balance->blocks > 0 && balance->bytes >= size);
    if (balance->blocks) balance->blocks--;
    if (balance->bytes >= size) balance->bytes -= size;
    free(p);
}

static void drain_events(moq_session_t *session, struct observed *observed)
{
    moq_event_t events[EVENT_BOUND];
    size_t n = 0;
    if (!CHECK(moq_session_poll_events_ex(session, events, EVENT_BOUND,
                                         sizeof(events[0]), &n) == MOQ_OK)) return;
    if (!CHECK(n <= EVENT_BOUND)) return;
    CHECK(n < EVENT_BOUND);
    for (size_t i = 0; i < n; i++) {
        CHECK(events[i].kind == MOQ_EVENT_SESSION_CLOSED);
        if (events[i].kind == MOQ_EVENT_SESSION_CLOSED) {
            CHECK(events[i].detail_size == sizeof(moq_session_closed_event_t));
            observed->closes++;
            observed->code = events[i].u.closed.code;
        }
        moq_event_cleanup(&events[i]);
    }
}

static void observe_hook(moq_wtquic_conn_t *conn, void *user)
{
    wtq_session_t *s = user;
    s->hook_calls++;
    if (!CHECK(s->hook_calls <= HOOK_BOUND)) {
        fflush(stdout);
        abort();
    }
    drain_events(moq_wtquic_conn_session(conn), &s->observed);
    if (s->hook_calls == 1) {
        s->first_hook_closes = s->observed.closes;
        s->first_hook_code = s->observed.code;
        s->first_hook_close_calls = s->close_calls;
        s->first_hook_opens = s->uni_opens + s->bidi_opens;
    }
}

static void run_row(moq_version_t version, enum test_mode mode)
{
    row_name = names[mode];
    row_version = (unsigned)version;
    unsigned before = failures;
    struct alloc_balance balance = {0};
    moq_alloc_t alloc = {
        .ctx = &balance, .alloc = count_alloc,
        .realloc = count_realloc, .free = count_free
    };
    moq_session_t *session = NULL;
    wtq_session_t foreign = { .refs = 1 };
    wtq_session_t wt = {
        .foreign = &foreign, .mode = mode, .refs = 1, .open_rc = WTQ_OK
    };
    active_wt = &wt;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
    scfg.version = version;
    moq_result_t rc = moq_session_create(&scfg, 0, &session);
    if (!CHECK(rc == MOQ_OK && session != NULL)) goto cleanup;
    switch (mode) {
    case CLEAN_IN_OPEN: case UNCLEAN_IN_OPEN: case FOREIGN_IN_OPEN: case CLOSED_BARE:
        wt.open_rc = WTQ_ERR_CLOSED; break;
    case STATE_BARE: wt.open_rc = WTQ_ERR_STATE; break;
    case BACKEND_BARE: wt.open_rc = WTQ_ERR_BACKEND; break;
    case CREDIT_LIMIT: wt.open_rc = WTQ_ERR_STREAM_LIMIT; break;
    case CREDIT_BLOCK: wt.open_rc = WTQ_ERR_WOULD_BLOCK; break;
    case SUCCESS: case CLEAN_BEFORE_SERVICE: break;
    case CLEAN_POST_HOOK: case UNCLEAN_POST_HOOK:
        wt.open_rc = WTQ_ERR_CLOSED; break;
    case MODE_COUNT: CHECK(false); goto cleanup;
    }
    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &alloc;
    ccfg.session = session;
    ccfg.wt_session = &wt;
    ccfg.hook = observe_hook;
    ccfg.hook_user = &wt;
    rc = moq_wtquic_conn_create(&ccfg, &wt.conn);
    if (!CHECK(rc == MOQ_OK && wt.conn != NULL)) goto cleanup;
    foreign.conn = wt.conn;
    if (!CHECK(moq_session_start(session, 0) == MOQ_OK)) goto cleanup;
    if (mode == CLEAN_BEFORE_SERVICE) emit_terminal(&wt, CLEAN_CODE, true);
    else moq_wtquic_conn_service(wt.conn);
    drain_events(session, &wt.observed);
    bool pressure = mode == CREDIT_LIMIT || mode == CREDIT_BLOCK;
    bool post_hook = mode == CLEAN_POST_HOOK || mode == UNCLEAN_POST_HOOK;
    bool clean = mode == CLEAN_IN_OPEN || mode == CLEAN_BEFORE_SERVICE || mode == CLEAN_POST_HOOK;
    bool unclean = mode == UNCLEAN_IN_OPEN || mode == UNCLEAN_POST_HOOK;
    bool error = mode == CLOSED_BARE || mode == STATE_BARE ||
                 mode == BACKEND_BARE || mode == FOREIGN_IN_OPEN;
    bool terminal = clean || unclean || error;
    unsigned opens = wt.uni_opens + wt.bidi_opens;
    if (mode == CLEAN_BEFORE_SERVICE) CHECK(opens == 0);
    else {
        CHECK(opens > 0 && (pressure || opens == (post_hook ? 2u : 1u)));
        if (version == MOQ_VERSION_DRAFT_16) CHECK(wt.uni_opens == 0);
        else CHECK(wt.bidi_opens == 0);
    }
    if (pressure) {
        CHECK(wt.observed.closes == 0 && wt.close_calls == 0);
        CHECK(wt.callbacks_from_open == 0 && wt.callbacks_from_close == 0);
        CHECK(wt.successful_opens == 0 && wt.sends == 0);
        wt.open_rc = WTQ_OK;
        moq_wtquic_conn_service(wt.conn);
        CHECK(wt.uni_opens + wt.bidi_opens == opens + 1);
        drain_events(session, &wt.observed);
    }
    uint32_t expected_code = clean ? CLEAN_CODE : unclean ? UNCLEAN_CODE : error ? 1u : 0u;
    CHECK(wt.hook_calls > 0);
    if ((clean || unclean) && !post_hook) {
        CHECK(wt.first_hook_closes == 1 && wt.first_hook_code == expected_code);
        CHECK(wt.first_hook_close_calls == 0);
        CHECK(wt.first_hook_opens == (mode == CLEAN_BEFORE_SERVICE ? 0u : 1u));
    } else CHECK(wt.first_hook_closes == 0);
    if (post_hook) {
        CHECK(wt.initial_credit_refusals == 1 && wt.terminal_at_hook == 1);
        CHECK(wt.first_hook_opens == 1 && wt.first_hook_close_calls == 0);
        CHECK(wt.close_feeds == 1 && wt.close_feed_seen);
        CHECK(wt.admission_before_feed == 0);
        CHECK(wt.resume_before_feed == 0 && wt.input_before_feed == 0);
        CHECK(wt.teardown_before_feed == 0);
    }
    if (mode == CLEAN_IN_OPEN || unclean || post_hook) {
        CHECK(wt.callbacks_from_open == 1 && wt.callbacks_from_close == 0);
        CHECK(wt.retained_in_open);
        CHECK(wt.conn->close_fed && wt.conn->close_clean == clean &&
              wt.conn->close_code == expected_code && !wt.conn->misrouted);
    } else CHECK(wt.callbacks_from_open == 0);
    if (mode == FOREIGN_IN_OPEN) {
        CHECK(wt.foreign_callbacks == 1 && wt.foreign_rejected_in_open);
        CHECK(foreign.terminal);
    } else CHECK(wt.foreign_callbacks == 0);
    if (terminal) {
        CHECK(wt.observed.closes == 1 && wt.observed.code == expected_code);
        CHECK(wt.close_calls == (clean ? 0u : 1u));
        if (!clean) CHECK(wt.last_close_code == expected_code);
        if (error) CHECK(wt.callbacks_from_close == 1);
        CHECK(wt.successful_opens == 0 && wt.sends == 0);
    } else {
        CHECK(wt.observed.closes == 0 && wt.close_calls == 0);
        CHECK(wt.successful_opens == 1 && wt.sends > 0);
    }
    opens = wt.uni_opens + wt.bidi_opens;
    unsigned close_calls = wt.close_calls;
    unsigned sends = wt.sends;
    unsigned callbacks = wt.callbacks_from_open + wt.callbacks_from_close + wt.foreign_callbacks;
    unsigned close_feeds = wt.close_feeds;
    unsigned admission_before_feed = wt.admission_before_feed;
    unsigned resume_before_feed = wt.resume_before_feed;
    unsigned input_before_feed = wt.input_before_feed;
    unsigned teardown_before_feed = wt.teardown_before_feed;
    for (unsigned i = 0; i < 3; i++) {
        moq_wtquic_conn_service(wt.conn);
        drain_events(session, &wt.observed);
        CHECK(wt.uni_opens + wt.bidi_opens == opens);
        CHECK(wt.close_calls == close_calls && wt.sends == sends);
        CHECK(wt.callbacks_from_open + wt.callbacks_from_close + wt.foreign_callbacks == callbacks);
        CHECK(wt.close_feeds == close_feeds);
        CHECK(wt.admission_before_feed == admission_before_feed &&
              wt.resume_before_feed == resume_before_feed &&
              wt.input_before_feed == input_before_feed &&
              wt.teardown_before_feed == teardown_before_feed);
        CHECK(wt.observed.closes == (terminal ? 1u : 0u));
        if (terminal) CHECK(wt.observed.code == expected_code);
    }
    printf("D%u/%s opens=%u/%u own-cb=%u foreign-cb=%u "
           "first-hook=%u/0x%" PRIx64 " event=%u/0x%" PRIx64 " teardown=%u/0x%x\n",
           (unsigned)version, row_name, wt.uni_opens, wt.bidi_opens,
           wt.callbacks_from_open, wt.foreign_callbacks,
           wt.first_hook_closes, wt.first_hook_code, wt.observed.closes,
           wt.observed.code, wt.close_calls, wt.last_close_code);
    if (post_hook)
        printf("D%u/%s credit=%u terminal-hook=%u feeds=%u "
               "before-feed admission/resume/input/teardown=%u/%u/%u/%u\n",
               (unsigned)version, row_name, wt.initial_credit_refusals,
               wt.terminal_at_hook, wt.close_feeds, wt.admission_before_feed,
               wt.resume_before_feed, wt.input_before_feed, wt.teardown_before_feed);
cleanup:
    /* No callbacks remain after this synchronous provider quiescence. */
    if (wt.conn) {
        if (!wt.terminal) (void)wtq_session_close(&wt, CLEAN_CODE, NULL, 0);
        drain_events(session, &wt.observed);
        moq_wtquic_conn_destroy(wt.conn);
        wt.conn = NULL;
    }
    for (unsigned i = 0; i < wt.stream_count; i++) CHECK(wt.streams[i].refs == 0);
    CHECK(wt.refs == 1 && foreign.refs == 1);
    wtq_session_release(&wt);
    wtq_session_release(&foreign);
    CHECK(wt.refs == 0 && foreign.refs == 0);
    if (session) moq_session_destroy(session);
    CHECK(balance.blocks == 0 && balance.bytes == 0);
    active_wt = NULL;
    printf("D%u/%s balance=%zu/%zu refs=%u/%u row-failures=%u\n",
           (unsigned)version, row_name, balance.blocks, balance.bytes,
           wt.refs, foreign.refs, failures - before);
}

int main(int argc, char **argv)
{
    bool terminals = argc == 2 && strcmp(argv[1], "--terminal") == 0;
    bool controls = argc == 2 && strcmp(argv[1], "--controls") == 0;
    if (argc != 1 && !terminals && !controls) {
        fprintf(stderr, "usage: %s [--terminal|--controls]\n", argv[0]);
        return 2;
    }
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    unsigned rows = 0;
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        for (enum test_mode mode = CLEAN_IN_OPEN; mode < MODE_COUNT; mode++) {
            bool attributed = mode == CLEAN_IN_OPEN || mode == UNCLEAN_IN_OPEN ||
                              mode == CLEAN_POST_HOOK || mode == UNCLEAN_POST_HOOK;
            if ((terminals && !attributed) || (controls && attributed)) continue;
            run_row(versions[i], mode);
            rows++;
        }
    }
    printf("%s rows=%u failures=%u\n", terminals ? "TERMINAL" : controls ? "CONTROLS" : "ALL",
           rows, failures);
    return failures ? 1 : 0;
}
