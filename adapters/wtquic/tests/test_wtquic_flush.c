/* Composition, not provider qualification. Actual managed + attach TUs,
 * real core/bridge observation, typed WT API fake, no sockets or WT library.
 * Requires section GC, as with the endpoint fixture. */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <moq/wtquic_msquic_managed.h>
#include <wtquic/wtquic_msquic.h>
#include "wtquic_stream_budget_fake.h"
#include "../wtquic_adapter_internal.h"
#include "../../../core/src/session/session_outbound_internal.h"
#include "../../../core/src/bridge/transport_bridge_internal.h"

static _Atomic unsigned failures;
static unsigned cases, alloc_calls, free_calls, clock_calls;
static int allocations;
static void *tracked_facade;
static bool facade_freed;
static const char *case_name = "setup";
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", case_name, __LINE__, #x); \
    ++failures; } } while (0)
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE %s:%d: %s\n", case_name, __LINE__, #x); \
    exit(2); } } while (0)

struct wtq_session {
    wtq_local_flush_mode_t mode;
    wtq_result_t result, send_rc;
    moq_wtquic_conn_t *adapter;
    moq_wtquic_msquic_managed_t *managed;
    void *cookies[8];
    unsigned count, queries, modes, effects, refs;
    bool synchronous, fin_pending, retired;
    void (*during_query)(wtq_session_t *);
    void (*during_send)(wtq_session_t *);
};
struct wtq_stream { wtq_session_t *owner; unsigned refs; bool bidi; };

static void check_query_lane(const wtq_session_t *s);
wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *s)
{
    check_query_lane(s);
    ++((wtq_session_t *)s)->modes;
    return s->mode;
}
wtq_result_t wtq_session_local_flush(const wtq_session_t *s)
{
    check_query_lane(s);
    wtq_session_t *w = (wtq_session_t *)s;
    ++w->queries;
    if (w->during_query) w->during_query(w);
    CHECK(!w->retired);
    return w->result != WTQ_OK ? w->result :
        w->fin_pending ? WTQ_ERR_WOULD_BLOCK : WTQ_OK;
}
wtq_result_t wtq_session_receive_contract(const wtq_session_t *s, size_t *n,
                                         wtq_receive_pause_mode_t *mode)
{
    *n = 65535;
    *mode = WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED;
    return s ? WTQ_OK : WTQ_ERR_INVALID_ARG;
}
wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s)
{ ++s->effects; return WTQ_OK; }
void wtq_session_add_ref(wtq_session_t *s) { ++s->refs; }
void wtq_session_release(wtq_session_t *s)
{ CHECK(s->refs != 0); if (s->refs && --s->refs == 0) s->retired = true; }
void wtq_stream_add_ref(wtq_stream_t *st) { ++st->refs; }
void wtq_stream_release(wtq_stream_t *st)
{ CHECK(st->refs != 0); if (st->refs) --st->refs; }
wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{ ++s->effects; *out = NULL; return WTQ_ERR_WOULD_BLOCK; }
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{ return wtq_session_open_uni(s, out); }
uint64_t wtq_stream_id(const wtq_stream_t *st)
{ (void)st; return WTQ_STREAM_ID_UNKNOWN; }
bool wtq_stream_is_bidi(const wtq_stream_t *st) { return st->bidi; }
wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *sp,
                            size_t n, uint32_t flags, void *cookie)
{
    (void)sp; (void)n;
    wtq_session_t *s = st->owner;
    ++s->effects;
    if (s->during_send) s->during_send(s);
    if (s->send_rc != WTQ_OK) return s->send_rc;
    if (flags & WTQ_SEND_FIN) s->fin_pending = true;
    if (cookie) {
        if (s->synchronous)
            moq_wtquic_conn_events()->on_send_complete(s, cookie, false, s->adapter);
        else {
            REQUIRE(s->count < 8);
            s->cookies[s->count++] = cookie;
        }
    }
    return WTQ_OK;
}
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t code)
{ (void)code; ++st->owner->effects; return WTQ_OK; }
wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{ return wtq_stream_reset(st, code); }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t code)
{ return wtq_stream_reset(st, code); }
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st)
{ ++st->owner->effects; return WTQ_OK; }
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st)
{ ++st->owner->effects; return WTQ_OK; }
wtq_receive_pause_mode_t wtq_stream_receive_pause_mode(const wtq_stream_t *st)
{ (void)st; return WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED; }
wtq_send_commit_mode_t wtq_session_send_commit_mode(const wtq_session_t *s)
{ (void)s; return WTQ_SEND_COMMIT_UNSUPPORTED; }
wtq_result_t wtq_stream_enable_send_commit(wtq_stream_t *st)
{ (void)st; return WTQ_ERR_UNSUPPORTED; }
static void fixture_close_boundary(wtq_session_t *s);
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t code,
                               const uint8_t *reason, size_t len)
{
    (void)code; (void)reason; (void)len;
    fixture_close_boundary(s);
    ++s->effects;
    return WTQ_OK;
}
void wtq_msquic_env_close(wtq_msquic_env_t *env) { (void)env; CHECK(false); }
void wtq_msquic_listener_stop(wtq_msquic_listener_t *l) { (void)l; CHECK(false); }

static int fixture_clock(clockid_t id, struct timespec *ts)
{ (void)id; ++clock_calls; ts->tv_sec = 0; ts->tv_nsec = 10000; return 0; }
static void (*before_trylock)(void);
static int fixture_trylock(pthread_mutex_t *mu)
{
    if (before_trylock) {
        void (*hook)(void) = before_trylock;
        before_trylock = NULL;
        hook();
    }
    return pthread_mutex_trylock(mu);
}

/* These mutation variants affect only this TU, never production sources.
 * Provider-only bypass discriminates upper ownership; all-actions bypass
 * discriminates reliable-only observation from the raw MsQuic adapter's
 * intentionally wider view. */
#ifdef WTQ_FLUSH_TEST_ALL_ACTIONS
#define session_has_reliable_outbound_obligation session_has_outbound_obligation
#elif defined(WTQ_FLUSH_TEST_SUB_ANSWER) || defined(WTQ_FLUSH_TEST_PUB_ANSWER)
static bool fixture_answer_obligation(const moq_session_t *s)
{
#ifdef WTQ_FLUSH_TEST_SUB_ANSWER
    for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next)
        if (s->subs[i].owed_update.owed) return true;
#else
    for (int32_t i = s->pub_occ_head; i >= 0; i = s->publishes[i].occ_next)
        if (s->publishes[i].owed_update.owed) return true;
#endif
    return session_has_reliable_outbound_obligation(s);
}
#define session_has_reliable_outbound_obligation fixture_answer_obligation
#endif
static moq_result_t fixture_real_attach_flush(const moq_wtquic_conn_t *c);
#define moq_wtquic_conn_poll_flush fixture_real_attach_flush
#define clock_gettime fixture_clock
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
#undef moq_wtquic_conn_poll_flush
#undef session_has_reliable_outbound_obligation
moq_result_t moq_wtquic_conn_poll_flush(const moq_wtquic_conn_t *c)
{
#ifdef WTQ_FLUSH_TEST_PROVIDER_ONLY
    if (c && wtq_session_local_flush(c->ws) == WTQ_OK) return MOQ_OK;
#endif
    return fixture_real_attach_flush(c);
}
#define pthread_mutex_trylock fixture_trylock
static int fixture_mutex_lock(pthread_mutex_t *mu);
static int fixture_mutex_unlock(pthread_mutex_t *mu);
static int fixture_detach(pthread_t thread);
#define pthread_mutex_lock fixture_mutex_lock
#define pthread_mutex_unlock fixture_mutex_unlock
#define pthread_detach fixture_detach
#include "../wtquic_msquic_managed.c"
#undef pthread_detach
#undef pthread_mutex_unlock
#undef pthread_mutex_lock
#undef pthread_mutex_trylock
#undef clock_gettime

struct fixture {
    moq_wtquic_msquic_managed_t m;
    moq_wtquic_msquic_managed_lane_t lane;
    moq_wtquic_msquic_managed_conn_t child;
    wtq_session_t wt;
    wtq_stream_t stream;
};
static struct fixture *current;

static void *count_alloc(size_t n, void *ctx)
{ (void)ctx; ++alloc_calls; void *p = malloc(n); if (p) ++allocations; return p; }
static void *count_realloc(void *p, size_t old, size_t n, void *ctx)
{
    (void)old; (void)ctx; ++alloc_calls;
    if (n == 0) { if (p) --allocations; free(p); return NULL; }
    bool fresh = p == NULL;
    void *q = realloc(p, n);
    if (q && fresh) ++allocations;
    return q;
}
static void count_free(void *p, size_t n, void *ctx)
{
    (void)n; (void)ctx; ++free_calls;
    if (p) --allocations;
    if (p && p == tracked_facade) { facade_freed = true; tracked_facade = NULL; }
    free(p);
}

static void setup_parts(moq_wtquic_msquic_managed_t *m,
                        moq_wtquic_msquic_managed_lane_t *lane,
                        moq_wtquic_msquic_managed_conn_t *child,
                        wtq_session_t *wt, wtq_stream_t *stream)
{
    m->alloc = (moq_alloc_t){.alloc = count_alloc, .realloc = count_realloc,
                            .free = count_free};
    m->perspective = MOQ_PERSPECTIVE_CLIENT;
    m->client = child; m->lanes = lane; m->lane_count = 1;
    lane->m = m;
    child->m = m; child->lane = lane; child->ws = wt;
    child->owns_ws_ref = true;
    wt->refs = 1; wt->mode = WTQ_LOCAL_FLUSH_ACKNOWLEDGED; wt->managed = m;
    stream->owner = wt;
    REQUIRE(pthread_mutex_init(&m->mu, NULL) == 0); m->mu_inited = true;
    REQUIRE(pthread_cond_init(&m->cv, NULL) == 0); m->cv_inited = true;
    REQUIRE(pthread_mutex_init(&lane->mu, NULL) == 0); m->lanes_inited = 1;
    REQUIRE(pthread_mutex_init(&lane->bell_mu, NULL) == 0);
    REQUIRE(pthread_cond_init(&lane->bell_cv, NULL) == 0); lane->bell_inited = true;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &m->alloc, MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_16;
    REQUIRE(moq_session_create(&scfg, 0, &child->session) == MOQ_OK);
    moq_wtquic_conn_cfg_t cfg;
    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &m->alloc; cfg.session = child->session; cfg.wt_session = wt;
    REQUIRE(moq_wtquic_conn_create(&cfg, &child->adapter) == MOQ_OK);
    wt->adapter = child->adapter;
    child->session_backed = true; child->adapter->started = true;
    child->session->state = MOQ_SESS_ESTABLISHED;
    child->adapter->streams[0] = (struct moq_wtq_stream){
        .id = 7, .st = stream, .in_use = true,
    };
}

static void setup(struct fixture *f)
{
    memset(f, 0, sizeof(*f));
    setup_parts(&f->m, &f->lane, &f->child, &f->wt, &f->stream);
}

static void finish(struct fixture *f)
{
    if (f->child.adapter) CHECK(f->child.adapter->sends == NULL);
    mm_reap_conn(&f->child);
    REQUIRE(pthread_cond_destroy(&f->lane.bell_cv) == 0);
    REQUIRE(pthread_mutex_destroy(&f->lane.bell_mu) == 0);
    REQUIRE(pthread_mutex_destroy(&f->lane.mu) == 0);
    REQUIRE(pthread_cond_destroy(&f->m.cv) == 0);
    REQUIRE(pthread_mutex_destroy(&f->m.mu) == 0);
}

static void check_query_lane(const wtq_session_t *s)
{
    CHECK(!s->retired && s->refs != 0);
    int rc = pthread_mutex_trylock(&s->managed->lanes[0].mu);
    CHECK(rc == EBUSY);
    if (rc == 0) pthread_mutex_unlock(&s->managed->lanes[0].mu);
}

static void observe(struct fixture *f, const char *label, moq_result_t expected)
{
    case_name = label;
    ++cases;
    moq_wtquic_conn_t *c = f->child.adapter;
    void *before = malloc(sizeof(*c));
    REQUIRE(before != NULL);
    memcpy(before, c, sizeof(*c));
    moq_session_t session_before = *c->session;
    moq_transport_bridge_t bridge_before = *c->bridge;
    unsigned ac = alloc_calls, fc = free_calls, clk = clock_calls;
    unsigned effects = f->wt.effects;
    uint64_t bell = f->lane.pending;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == expected);
    CHECK(alloc_calls == ac && free_calls == fc && clock_calls == clk);
    CHECK(f->wt.effects == effects && f->lane.pending == bell);
    CHECK(memcmp(before, c, sizeof(*c)) == 0);
    CHECK(memcmp(&session_before, c->session, sizeof(session_before)) == 0);
    CHECK(memcmp(&bridge_before, c->bridge, sizeof(bridge_before)) == 0);
    free(before);
}

static void complete(struct fixture *f, unsigned index, bool canceled)
{
    REQUIRE(index < f->wt.count && f->wt.cookies[index] != NULL);
    void *cookie = f->wt.cookies[index];
    f->wt.cookies[index] = NULL;
    /* Keep the completion's unrelated service pass deferred, as it is during
     * a reentrant send. The query must not mistake that latch for output. */
    f->child.adapter->in_service = true;
    mm_guard_enter(&f->lane);
    ev_send_complete(&f->wt, cookie, canceled, f->child.adapter);
    mm_guard_leave(&f->lane);
    f->child.adapter->in_service = false;
}

static void t_results(struct fixture *f)
{
    observe(f, "live-idle", MOQ_OK);
    f->wt.mode = WTQ_LOCAL_FLUSH_TRANSPORT;
    observe(f, "transport-mode", MOQ_OK);
    f->wt.mode = WTQ_LOCAL_FLUSH_UNSUPPORTED;
    observe(f, "unavailable-mode", MOQ_ERR_UNSUPPORTED);
    f->wt.mode = (wtq_local_flush_mode_t)99;
    observe(f, "unknown-mode", MOQ_ERR_UNSUPPORTED);
    f->wt.mode = WTQ_LOCAL_FLUSH_ACKNOWLEDGED;
    const struct { wtq_result_t wt; moq_result_t moq; const char *name; } rows[] = {
        {WTQ_ERR_WOULD_BLOCK, MOQ_DONE, "provider-bytes-pending"},
        {WTQ_ERR_STATE, MOQ_DONE, "provider-unestablished"},
        {WTQ_ERR_UNSUPPORTED, MOQ_ERR_UNSUPPORTED, "provider-unsupported"},
        {WTQ_ERR_BACKEND, MOQ_ERR_INTERNAL, "sticky-unexpected-loss"},
        {WTQ_ERR_CLOSED, MOQ_ERR_CLOSED, "provider-terminal"},
        {WTQ_ERR_PROTO, MOQ_ERR_INTERNAL, "unclassified-error"},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
        f->wt.result = rows[i].wt;
        observe(f, rows[i].name, rows[i].moq);
    }
    f->wt.result = WTQ_OK;
}

static void t_upper(struct fixture *f)
{
    moq_session_t *s = f->child.session;
    moq_transport_bridge_t *b = f->child.adapter->bridge;
    const size_t head = s->action_head, tail = s->action_tail;
    moq_action_t saved = s->actions[0];
    s->action_head = 0; s->action_tail = 1;
    s->actions[0].kind = MOQ_ACTION_SEND_DATAGRAM;
    observe(f, "datagram-only", MOQ_OK);
    s->actions[0].kind = MOQ_ACTION_SEND_DATA;
    observe(f, "queued-stream-data", MOQ_DONE);
    f->wt.result = WTQ_ERR_BACKEND;
    observe(f, "loss-outranks-core-pending", MOQ_ERR_INTERNAL);
    f->wt.result = WTQ_ERR_UNSUPPORTED;
    observe(f, "unsupported-outranks-core-pending", MOQ_ERR_UNSUPPORTED);
    f->wt.result = WTQ_OK;
    s->actions[0] = saved; s->action_head = head; s->action_tail = tail;
    b->pending_count = 1;
    observe(f, "bridge-retained-output", MOQ_DONE);
    b->pending_count = 0;
    b->needs_close = true;
    observe(f, "bridge-close-owed", MOQ_DONE);
    b->needs_close = false;
    b->pending_control = true;
    f->child.adapter->streams[0].held = true;
    f->child.adapter->streams[0].paused = true;
    f->child.adapter->streams[0].borrowed = 1;
    f->child.adapter->service_pending = true;
    observe(f, "receive-only-and-progress-latches", MOQ_OK);
    b->pending_control = false;
    f->child.adapter->streams[0].held = false;
    f->child.adapter->streams[0].paused = false;
    f->child.adapter->streams[0].borrowed = 0;
    f->child.adapter->streams[0].fin_submitting = true;
    observe(f, "in-flight-fin-decision", MOQ_DONE);
    f->child.adapter->streams[0].fin_submitting = false;
    f->child.close_pending = true;
    observe(f, "managed-close-intent", MOQ_DONE);
    f->child.close_pending = false;

    /* Exercise retained owners through the actual SPI. Private state is
     * constructed only for observation, not represented as protocol ingress. */
    moq_session_t owner = {0};
    moq_profile_ops_t profile = {0};
    owner.profile = &profile;
    owner.sg_occ_head = owner.rx_occ_head = owner.sub_occ_head = owner.pub_occ_head = -1;
    f->child.adapter->session = &owner;
#define OWNER_CHECK(label) observe(f, label, MOQ_DONE)
    owner.noslot_carrier_count = 1;
    OWNER_CHECK("no-slot-carrier"); owner.noslot_carrier_count = 0;
    uint8_t reason = MOQ_DRAIN_REJECT_STOP_OWED;
    owner.drain_ref_reasons = &reason; owner.drain_ref_count = 1;
    OWNER_CHECK("rejection-stop"); owner.drain_ref_count = 0;
    moq_sg_entry_t sg = {.occ_next = -1, .state = MOQ_SG_RESET_OWED};
    owner.subgroups = &sg; owner.sg_occ_head = 0;
    OWNER_CHECK("subgroup-reset"); owner.sg_occ_head = -1;
    moq_rx_stream_t rx = {.occ_next = -1, .parse_state = MOQ_RX_NEED_STOP};
    owner.rx_streams = &rx; owner.rx_occ_head = 0;
    OWNER_CHECK("receive-owner-stop"); owner.rx_occ_head = -1;
    moq_sub_entry_t sub = {.occ_next = -1, .state = MOQ_SUB_DONE_HELD};
    owner.subs = &sub; owner.sub_occ_head = 0;
    OWNER_CHECK("subscriber-done-held"); owner.sub_occ_head = -1;
    moq_pub_entry_t pub = {.occ_next = -1, .state = MOQ_PUB_DONE_HELD};
    owner.publishes = &pub; owner.pub_occ_head = 0;
    OWNER_CHECK("publisher-done-held"); owner.pub_occ_head = -1;
    moq_fetch_entry_t fetch = {0};
    fetch.state = MOQ_FETCH_ACCEPTED; fetch.rs.retained_output = MOQ_RETAINED_OUTPUT_FIN;
    owner.fetches = &fetch; owner.fetch_cap = 1;
    OWNER_CHECK("fetch-fin-held"); owner.fetch_cap = 0;
    moq_fill_entry_t fill = {.active = true, .role = MOQ_FILL_PUBLISHER,
                             .stream_bound = true, .finish_requested = true};
    owner.fills.entries = &fill; owner.fills.capacity = 1;
    OWNER_CHECK("fill-fin-held"); owner.fills.capacity = 0;
    moq_ann_entry_t ann = {.state = MOQ_ANN_DRAINING, .finish_fin = true};
    owner.announcements = &ann; owner.ann_cap = 1;
    OWNER_CHECK("namespace-fin-held"); owner.ann_cap = 0;
    moq_ts_entry_t ts = {.state = MOQ_TS_DRAINING_RESPONSE, .goaway_sent = true};
    owner.track_statuses = &ts; owner.ts_cap = 1;
    OWNER_CHECK("track-status-cancel-held"); owner.ts_cap = 0;
    moq_ns_sub_entry_t ns = {.state = MOQ_NS_SUB_ESTABLISHED,
                            .local_subscriber = true, .pending_fin = true};
    owner.ns_subs = &ns; owner.ns_sub_cap = 1;
    OWNER_CHECK("namespace-sub-fin-held"); owner.ns_sub_cap = 0;
    moq_track_sub_entry_t track = {.state = MOQ_TRACK_SUB_ESTABLISHED,
                                  .handoff_fin_pending = true};
    owner.track_subs = &track; owner.track_sub_cap = 1;
    OWNER_CHECK("track-sub-fin-held"); owner.track_sub_cap = 0;
#undef OWNER_CHECK
    sub.state = MOQ_SUB_ESTABLISHED;
    sub.rs.retained_output = MOQ_RETAINED_OUTPUT_NONE;
    sub.owed_update.owed = true; owner.sub_occ_head = 0;
    observe(f, "subscriber-future-answer-only", MOQ_OK);
    owner.sub_occ_head = -1; sub.owed_update.owed = false;
    pub.state = MOQ_PUB_ESTABLISHED;
    pub.rs.retained_output = MOQ_RETAINED_OUTPUT_NONE;
    pub.owed_update.owed = true; owner.pub_occ_head = 0;
    observe(f, "publisher-future-answer-only", MOQ_OK);
    owner.pub_occ_head = -1; pub.owed_update.owed = false;
    owner.event_tail = 1; owner.staged_count = 1;
    observe(f, "receive-events-and-staged-input-only", MOQ_OK);
    owner.event_tail = owner.staged_count = 0;
    owner.send_len = 99;
    observe(f, "borrow-arena-only", MOQ_OK);
    f->child.adapter->session = s;
}

static void during_send(wtq_session_t *s)
{ CHECK(moq_wtquic_conn_poll_flush(s->adapter) == MOQ_DONE); }

static void t_sends(struct fixture *f)
{
    const uint8_t byte = 42;
    case_name = "send-ownership";
    mm_guard_enter(&f->lane);
    f->wt.during_send = during_send;
    REQUIRE(ep_write(f->child.adapter, 7, &byte, 1, false) == MOQ_TRANSPORT_OK);
    f->wt.during_send = NULL;
    mm_guard_leave(&f->lane);
    observe(f, "copied-send-borrow-despite-provider-ok", MOQ_DONE);
    moq_rcbuf_t *buf = NULL;
    REQUIRE(moq_rcbuf_create(&f->m.alloc, &byte, 1, &buf) == MOQ_OK);
    mm_guard_enter(&f->lane);
    REQUIRE(ep_write_payload(f->child.adapter, 7, buf, false) == MOQ_TRANSPORT_OK);
    mm_guard_leave(&f->lane);
    CHECK(moq_rcbuf_refcount(buf) == 2);
    complete(f, 0, false); /* unlink the non-head copied record */
    observe(f, "rcbuf-borrow-after-other-completion", MOQ_DONE);
    f->wt.result = WTQ_ERR_BACKEND;
    observe(f, "loss-outranks-retained-borrow", MOQ_ERR_INTERNAL);
    complete(f, 1, true);
    CHECK(moq_rcbuf_refcount(buf) == 1);
    moq_rcbuf_decref(buf);
    observe(f, "canceled-buffer-return-does-not-clear-loss", MOQ_ERR_INTERNAL);
    f->wt.result = WTQ_OK; /* a distinct scripted healthy intent-settlement case */
    observe(f, "ownership-settled-provider-ok", MOQ_OK);

    f->wt.synchronous = true;
    mm_guard_enter(&f->lane);
    REQUIRE(ep_write(f->child.adapter, 7, &byte, 1, false) == MOQ_TRANSPORT_OK);
    mm_guard_leave(&f->lane);
    observe(f, "reentrant-send-completion", MOQ_OK);
    f->wt.synchronous = false;
    f->wt.send_rc = WTQ_ERR_WOULD_BLOCK;
    mm_guard_enter(&f->lane);
    REQUIRE(ep_write(f->child.adapter, 7, &byte, 1, false) == MOQ_TRANSPORT_WOULD_BLOCK);
    mm_guard_leave(&f->lane);
    f->wt.send_rc = WTQ_OK;
    observe(f, "refused-send-withdraws-record", MOQ_OK);
    mm_guard_enter(&f->lane);
    REQUIRE(ep_write(f->child.adapter, 7, NULL, 0, true) == MOQ_TRANSPORT_OK);
    mm_guard_leave(&f->lane);
    observe(f, "zero-byte-fin-pending", MOQ_DONE);
    f->wt.fin_pending = false;
    observe(f, "fin-provider-complete", MOQ_OK);
}

static struct fixture *other;
static unsigned pump_hooks, deadline_hooks, lane_deadline_hooks, activity_hooks, stopped_hooks;
static unsigned choose_hooks, nested_hooks;
static struct fixture *nested_outer;
static void check_hook(struct fixture *f)
{
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_wtquic_msquic_managed_poll_flush(&other->m) == MOQ_OK);
}
static int pump_hook(moq_wtquic_msquic_managed_t *m,
                     moq_wtquic_msquic_managed_lane_t *lane, uint64_t now, void *ctx)
{ (void)m; (void)lane; (void)now; ++pump_hooks; check_hook(ctx); return 0; }
static uint64_t deadline_hook(void *ctx)
{ ++deadline_hooks; check_hook(ctx); return UINT64_MAX; }
static uint64_t lane_deadline_hook(void *ctx, uint32_t lane)
{ CHECK(lane == 0); ++lane_deadline_hooks; check_hook(ctx); return UINT64_MAX; }
static void activity_hook(moq_wtquic_msquic_managed_t *m, void *ctx)
{
    (void)m; ++activity_hooks; check_hook(ctx);
    if (nested_outer) {
        REQUIRE(mm_pump_lane(&other->m, &other->lane, false) == 0);
        check_hook(ctx);
    }
}
static void stopped_hook(void *ctx) { ++stopped_hooks; check_hook(ctx); }
static void nested_activity(moq_wtquic_msquic_managed_t *m, void *ctx)
{
    (void)ctx; ++nested_hooks;
    CHECK(moq_wtquic_msquic_managed_poll_flush(m) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_wtquic_msquic_managed_poll_flush(&nested_outer->m) == MOQ_ERR_WRONG_STATE);
}
static uint32_t choose_hook(moq_wtquic_msquic_managed_t *m,
                             const moq_wtquic_msquic_accept_info_t *info, void *ctx)
{
    (void)info; (void)ctx; ++choose_hooks;
    CHECK(moq_wtquic_msquic_managed_poll_flush(m) == MOQ_ERR_UNSUPPORTED);
    CHECK(moq_wtquic_msquic_managed_poll_flush(&other->m) == MOQ_OK);
    return 0;
}

static void t_hooks(struct fixture *f, struct fixture *o)
{
    case_name = "actual-managed-hooks"; ++cases; other = o;
    f->m.on_lane_pump = pump_hook; f->m.on_lane_pump_user = f;
    f->m.app_deadline_us = deadline_hook; f->m.app_deadline_ctx = f;
    f->m.app_lane_deadline_us = lane_deadline_hook; f->m.app_lane_deadline_ctx = f;
    f->m.on_activity = activity_hook; f->m.on_activity_ctx = f;
    REQUIRE(mm_pump_lane(&f->m, &f->lane, false) == 0);
    CHECK(pump_hooks == 1 && deadline_hooks == 1 && lane_deadline_hooks == 1 && activity_hooks == 1);
    nested_outer = f; o->m.on_activity = nested_activity;
    REQUIRE(mm_pump_lane(&f->m, &f->lane, false) == 0);
    CHECK(nested_hooks == 1 && mm_flush_hooks == NULL);
    nested_outer = NULL; o->m.on_activity = NULL;
    f->m.on_lane_pump = NULL; f->m.app_deadline_us = NULL;
    f->m.app_lane_deadline_us = NULL; f->m.on_activity = NULL;
    /* Exercise the actual coordinator continuation, without starting workers
     * or retiring this fixture's separate test-owned child. */
    f->m.client = NULL; f->m.lane_count = 0; f->m.stop_requested = true;
    f->m.on_stopped = stopped_hook; f->m.on_stopped_ctx = f;
    CHECK(mm_coord_main(&f->m) == NULL);
    CHECK(stopped_hooks == 1 && mm_flush_hooks == NULL);
    f->m.on_stopped = NULL; f->m.client = &f->child; f->m.lane_count = 1;
    f->m.stop_requested = f->m.stopped = false;
    moq_wtquic_msquic_managed_conn_t child = {0};
    f->m.perspective = MOQ_PERSPECTIVE_SERVER;
    f->m.children = &child; f->m.max_connections = 1;
    f->m.choose_lane = choose_hook;
    wtq_msquic_accept_info_t info;
    wtq_msquic_accept_decision_t decision;
    memset(&info, 0, sizeof(info));
    memset(&decision, 0, sizeof(decision));
    CHECK(mm_accept_prepare(&f->m, &info, &decision) == WTQ_OK);
    CHECK(choose_hooks == 1 && decision.accepted && mm_flush_hooks == NULL);
    mm_accept_abandon(&f->m, &child);
    f->m.perspective = MOQ_PERSPECTIVE_CLIENT;
    f->m.children = NULL; f->m.max_connections = 0; f->m.choose_lane = NULL;
    observe(f, "hook-scope-restored", MOQ_OK);
}

static void stop_before_lock(void)
{ CHECK(moq_wtquic_msquic_managed_stop_begin(&current->m)); }
static void stop_during_query(wtq_session_t *s)
{ CHECK(moq_wtquic_msquic_managed_stop_begin(s->managed)); }

struct race {
    struct fixture *f;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool held, release;
};
static void *hold_lane(void *arg)
{
    struct race *r = arg;
    mm_guard_enter(&r->f->lane);
    pthread_mutex_lock(&r->mu);
    r->held = true; pthread_cond_broadcast(&r->cv);
    while (!r->release) pthread_cond_wait(&r->cv, &r->mu);
    pthread_mutex_unlock(&r->mu);
    mm_guard_leave(&r->f->lane);
    return NULL;
}
static void t_refusal(struct fixture *f)
{
    case_name = "argument-mode-and-unestablished"; ++cases;
    CHECK(moq_wtquic_msquic_managed_poll_flush(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_wtquic_conn_poll_flush(NULL) == MOQ_ERR_INVAL);
    f->m.perspective = MOQ_PERSPECTIVE_SERVER; f->m.stop_requested = true;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_UNSUPPORTED);
    f->m.perspective = MOQ_PERSPECTIVE_CLIENT; f->m.stop_requested = false;
    unsigned queries = f->wt.queries;
    f->m.client = NULL;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_DONE);
    f->m.client = &f->child;
    f->child.ws = NULL;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_DONE);
    f->child.ws = &f->wt;
    moq_session_t *session = f->child.session;
    f->child.session = NULL;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_DONE);
    f->child.session = session;
    moq_wtquic_conn_t *adapter = f->child.adapter;
    f->child.adapter = NULL;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_DONE);
    f->child.adapter = adapter;
    CHECK(f->wt.queries == queries);

    case_name = "real-lane-contention"; ++cases;
    struct race r = {.f = f};
    REQUIRE(pthread_mutex_init(&r.mu, NULL) == 0);
    REQUIRE(pthread_cond_init(&r.cv, NULL) == 0);
    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, hold_lane, &r) == 0);
    pthread_mutex_lock(&r.mu);
    while (!r.held) pthread_cond_wait(&r.cv, &r.mu);
    pthread_mutex_unlock(&r.mu);
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_DONE);
    CHECK(f->wt.queries == queries);
    CHECK(moq_wtquic_msquic_managed_stop_begin(&f->m));
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_CLOSED);
    pthread_mutex_lock(&r.mu); r.release = true; pthread_cond_broadcast(&r.cv);
    pthread_mutex_unlock(&r.mu);
    REQUIRE(pthread_join(thread, NULL) == 0);
    REQUIRE(pthread_cond_destroy(&r.cv) == 0);
    REQUIRE(pthread_mutex_destroy(&r.mu) == 0);
    f->m.stop_requested = false;

    case_name = "stop-before-successful-try-entry"; ++cases; current = f;
    before_trylock = stop_before_lock;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_CLOSED);
    CHECK(f->wt.queries == queries);
    f->m.stop_requested = false;
    f->wt.during_query = stop_during_query;
    case_name = "stop-during-observation"; ++cases;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_CLOSED);
    f->wt.during_query = NULL; f->m.stop_requested = false;
    f->child.logical_terminal = true;
    observe(f, "child-terminal", MOQ_ERR_CLOSED);
    f->child.logical_terminal = false;
    f->child.adapter->close_pending = true;
    f->wt.result = WTQ_ERR_BACKEND;
    observe(f, "actual-terminal-precedes-loss", MOQ_ERR_CLOSED);
    f->child.adapter->close_pending = false; f->wt.result = WTQ_OK;
}

struct teardown_control {
    struct fixture *f;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool release, attempted, blocked, quiesce_reached, query_done;
};
static struct teardown_control teardown_race;
static _Thread_local struct teardown_control *teardown_watch;
static _Thread_local pthread_mutex_t *skipped_guard;

static int fixture_mutex_lock(pthread_mutex_t *mu)
{
    struct teardown_control *r = teardown_watch;
    if (r && mu == &r->f->lane.mu && !r->attempted) {
        REQUIRE(pthread_mutex_lock(&r->mu) == 0);
        r->attempted = true;
        /* This is the actual mm_guard_enter lock site, reached with the
         * observer still inside the WT query. A real try proves contention. */
        REQUIRE(pthread_mutex_trylock(mu) == EBUSY);
#ifdef WTQ_FLUSH_TEST_NO_TEARDOWN_FENCE
        skipped_guard = mu;
        REQUIRE(pthread_mutex_unlock(&r->mu) == 0);
        return 0;
#else
        r->blocked = true;
        REQUIRE(pthread_cond_broadcast(&r->cv) == 0);
        REQUIRE(pthread_mutex_unlock(&r->mu) == 0);
#endif
    }
    return pthread_mutex_lock(mu);
}

static int fixture_mutex_unlock(pthread_mutex_t *mu)
{
    if (mu == skipped_guard) {
        skipped_guard = NULL;
        return 0;
    }
    return pthread_mutex_unlock(mu);
}

static void fixture_close_boundary(wtq_session_t *s)
{
    struct teardown_control *r = teardown_watch;
    if (!r || s != &r->f->wt) return;
    REQUIRE(pthread_mutex_lock(&r->mu) == 0);
    r->quiesce_reached = true;
    REQUIRE(pthread_cond_broadcast(&r->cv) == 0);
    /* With the fence removed, publish the escaped close before allowing the
     * query to finish. The test handshake prevents an unrelated storage race;
     * the real final-pump guard still precedes actual ownership reclamation. */
    while (!r->query_done) REQUIRE(pthread_cond_wait(&r->cv, &r->mu) == 0);
    REQUIRE(pthread_mutex_unlock(&r->mu) == 0);
}

static void *teardown_worker(void *arg)
{
    struct teardown_control *r = arg;
    pthread_mutex_lock(&r->mu);
    while (!r->release) pthread_cond_wait(&r->cv, &r->mu);
    pthread_mutex_unlock(&r->mu);
    CHECK(moq_wtquic_msquic_managed_stop_begin(&r->f->m));
    teardown_watch = r;
    mm_run_teardown(&r->f->m);
    teardown_watch = NULL;
    CHECK(skipped_guard == NULL);
    return NULL;
}
static void await_teardown(wtq_session_t *s)
{
    struct teardown_control *r = &teardown_race;
    pthread_mutex_lock(&r->mu);
    r->release = true; pthread_cond_broadcast(&r->cv);
    while (!r->blocked && !r->quiesce_reached) pthread_cond_wait(&r->cv, &r->mu);
    CHECK(r->attempted);
    CHECK(r->blocked && !r->quiesce_reached);
    CHECK(!s->retired && s->refs == 1);
    CHECK(r->f->child.adapter != NULL && r->f->child.session != NULL);
    r->query_done = true; pthread_cond_broadcast(&r->cv);
    pthread_mutex_unlock(&r->mu);
}
static void t_lifetime(void)
{
    case_name = "stop-and-actual-teardown-during-observation"; ++cases;
    struct fixture *f = calloc(1, sizeof(*f));
    REQUIRE(f != NULL); setup(f);
    struct teardown_control *r = &teardown_race;
    memset(r, 0, sizeof(*r)); r->f = f;
    REQUIRE(pthread_mutex_init(&r->mu, NULL) == 0);
    REQUIRE(pthread_cond_init(&r->cv, NULL) == 0);
    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, teardown_worker, r) == 0);
    f->wt.during_query = await_teardown;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_CLOSED);
    REQUIRE(pthread_join(thread, NULL) == 0);
    CHECK(f->child.adapter == NULL && f->child.session == NULL && f->child.ws == NULL);
    CHECK(f->wt.retired && f->wt.refs == 0);
    unsigned queries = f->wt.queries;
    CHECK(moq_wtquic_msquic_managed_poll_flush(&f->m) == MOQ_ERR_CLOSED);
    CHECK(f->wt.queries == queries);
    REQUIRE(pthread_cond_destroy(&r->cv) == 0);
    REQUIRE(pthread_mutex_destroy(&r->mu) == 0);
    finish(f); free(f);
}

struct destroy_control {
    moq_wtquic_msquic_managed_t *m;
    struct fixture *outer;
    wtq_session_t wt;
    wtq_stream_t stream;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool release;
    unsigned calls, detaches;
};
static _Thread_local struct destroy_control *destroy_watch;

static int fixture_detach(pthread_t thread)
{
    if (destroy_watch) {
        CHECK(pthread_equal(thread, pthread_self()));
        ++destroy_watch->detaches;
        /* Keep only this test's real coordinator joinable so TLS-tail checks
         * and worker completion can be witnessed before stack context dies. */
        return 0;
    }
    return pthread_detach(thread);
}

static void destroy_stopped_hook(void *arg)
{
    struct destroy_control *d = arg;
    ++d->calls;
    CHECK(moq_wtquic_msquic_managed_poll_flush(d->m) == MOQ_ERR_WRONG_STATE);
    CHECK(d->m->stopped && d->m->client->adapter == NULL &&
          d->m->client->session == NULL && d->m->client->ws == NULL);
    CHECK(d->wt.retired && d->wt.refs == 0);
    moq_wtquic_msquic_managed_destroy(d->m);
    d->m = NULL;
    CHECK(facade_freed);
}

static void *destroy_coordinator(void *arg)
{
    struct destroy_control *d = arg;
    REQUIRE(pthread_mutex_lock(&d->mu) == 0);
    while (!d->release) REQUIRE(pthread_cond_wait(&d->cv, &d->mu) == 0);
    REQUIRE(pthread_mutex_unlock(&d->mu) == 0);
    mm_flush_hook_scope_t outer_scope;
    mm_flush_hook_enter(&outer_scope, &d->outer->m);
    destroy_watch = d;
    CHECK(mm_coord_main(d->m) == NULL);
    /* Only the surviving outer scope is inspected after destroy, not m. */
    CHECK(mm_flush_hooks == &outer_scope);
    mm_flush_hook_leave(&outer_scope);
    CHECK(mm_flush_hooks == NULL);
    destroy_watch = NULL;
    return NULL;
}

static void t_destroy_continuation(struct fixture *outer)
{
    case_name = "stopped-continuation-destroys-facade-restores-tls"; ++cases;
    int before = allocations;
    struct destroy_control d = {.outer = outer};
    d.m = count_alloc(sizeof(*d.m), NULL);
    moq_wtquic_msquic_managed_lane_t *lane = count_alloc(sizeof(*lane), NULL);
    moq_wtquic_msquic_managed_conn_t *child = count_alloc(sizeof(*child), NULL);
    REQUIRE(d.m && lane && child);
    memset(d.m, 0, sizeof(*d.m)); memset(lane, 0, sizeof(*lane)); memset(child, 0, sizeof(*child));
    setup_parts(d.m, lane, child, &d.wt, &d.stream);
    d.m->stop_requested = true;
    d.m->on_stopped = destroy_stopped_hook; d.m->on_stopped_ctx = &d;
    d.m->on_stopped_owns = true;
    tracked_facade = d.m; facade_freed = false;
    REQUIRE(pthread_mutex_init(&d.mu, NULL) == 0);
    REQUIRE(pthread_cond_init(&d.cv, NULL) == 0);
    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, destroy_coordinator, &d) == 0);
    REQUIRE(pthread_mutex_lock(&d.mu) == 0);
    d.m->coord = thread; d.m->coord_started = true;
    d.release = true; REQUIRE(pthread_cond_broadcast(&d.cv) == 0);
    REQUIRE(pthread_mutex_unlock(&d.mu) == 0);
    REQUIRE(pthread_join(thread, NULL) == 0);
    CHECK(d.calls == 1 && d.detaches == 1 && d.m == NULL && facade_freed);
    CHECK(allocations == before && mm_flush_hooks == NULL);
    REQUIRE(pthread_cond_destroy(&d.cv) == 0);
    REQUIRE(pthread_mutex_destroy(&d.mu) == 0);
}

int main(void)
{
    struct fixture *f = calloc(1, sizeof(*f));
    struct fixture *o = calloc(1, sizeof(*o));
    REQUIRE(f && o);
    setup(f); setup(o);
    t_results(f); t_upper(f); t_sends(f); t_hooks(f, o); t_refusal(f);
    t_lifetime();
    t_destroy_continuation(o);
    finish(f); finish(o); free(f); free(o);
    CHECK(allocations == 0);
    printf("wtquic composed flush: %u cases, %u failures\n", cases, (unsigned)failures);
    return failures ? 1 : 0;
}
