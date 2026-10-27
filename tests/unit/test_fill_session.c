/*
 * Fill children of a subscription, driven end to end through a simulated
 * pair. A fill is requested on SUBSCRIBE or on a subscription REQUEST_UPDATE
 * (including the update a PUBLISH's subscriber sends), answered by the
 * publisher on a fetch response stream whose header carries the initiating
 * request's id, and ended exactly once with FILL_TERMINAL on both sides.
 *
 * Pinned here: overlapping fills on one subscription; a newer fill leaves an
 * older one running; cancelling the subscription resets every open fill; a
 * fill reset leaves the subscription alive; an independent failure is a
 * header followed by RESET; an empty or paused request opens no stream and
 * ends NO_STREAM on both sides; an update without a fill opens nothing; the
 * subscriber's pool exhaustion is a retryable refusal while the publisher's
 * answers the fill on the wire as a header followed by RESET, with the accept
 * or the acknowledgement; tiny queues refuse without mutating and complete on
 * retry; the terminal is delivered once and the handle is stale afterwards;
 * owned buffers transfer only on a full poll; the Stream Count of
 * PUBLISH_DONE is the application's own count plus the fill streams the
 * session opened, saturating to the profile's unknown sentinel; a stream this
 * side stopped counts toward the gate once the peer's terminal arrives;
 * a profile without fills refuses before anything is sent; a subscription
 * that ends before the publisher opens the fill releases the child's slot;
 * a fill the subscriber resolved to no stream stops a header the peer sends
 * anyway without closing the session; PUBLISH_DONE arriving ahead of the
 * fill data leaves exactly one subscription terminal; the publisher's fill
 * resets are queued ahead of its PUBLISH_DONE; a parent ended by PUBLISH_DONE
 * reports that cause on the child on both sides.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "../../core/src/wire/control_d22_internal.h"
#include "test_support.h"
#include "test_binding_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/fill_owner_internal.h"
#include <string.h>
#include <stdlib.h>

typedef struct count_alloc { int64_t live; } count_alloc_t;
static void *ca_alloc(size_t n, void *ctx)
{
    count_alloc_t *c = ctx;
    void *p = malloc(n);
    if (p) c->live++;
    return p;
}
static void *ca_realloc(void *p, size_t o, size_t n, void *ctx)
{
    count_alloc_t *c = ctx;
    (void)o;
    if (!p) { void *r = malloc(n); if (r) c->live++; return r; }
    return realloc(p, n);
}
static void ca_free(void *p, size_t n, void *ctx)
{
    count_alloc_t *c = ctx;
    (void)n;
    if (p) c->live--;
    free(p);
}

typedef struct pair {
    moq_simpair_t *sp;
    moq_session_t *c;   /* subscriber unless stated otherwise */
    moq_session_t *sv;  /* publisher unless stated otherwise */
} pair_t;

/* Bounded observation of actual outgoing draft-22 DONE bytes. Raw handoff
 * is not OPENED; this records no transport disposition or guessed count. */
static struct {
    size_t n;
    bool decoded, fin;
    uint64_t status, streams;
} done_wire;

static void capture_done(const uint8_t *data, size_t len, bool fin)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    while (r.pos < r.len) {
        moq_control_envelope_t env;
        if (moq_d22_decode_envelope(&r, &env) != MOQ_OK) return;
        if (env.msg_type != 0x0b) continue;
        ++done_wire.n;
        moq_d18_publish_done_t d; /* draft-22 uses the same vi64 body */
        done_wire.decoded = moq_d18_decode_publish_done(env.payload, env.payload_len, &d) == MOQ_OK;
        done_wire.fin = fin && r.pos == r.len;
        if (done_wire.decoded) {
            done_wire.status = d.status_code;
            done_wire.streams = d.stream_count;
        }
    }
}

static void capture_sim_done(void *ctx, const moq_sim_trace_record_t *r)
{
    (void)ctx;
    if (r->kind == MOQ_SIM_TRACE_ACTION && r->from == MOQ_PERSPECTIVE_SERVER &&
        r->action_kind == MOQ_ACTION_SEND_BIDI_STREAM)
        capture_done(r->detail_bytes.data, r->detail_bytes.len, r->fin);
}

static bool mk_pair_cfg_alloc(pair_t *p, moq_version_t version,
                        uint32_t max_actions, uint32_t max_events,
                        const moq_alloc_t *alloc)
{
    memset(&done_wire, 0, sizeof(done_wire));
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.trace_fn = capture_sim_done;
    scfg.alloc = alloc;
    scfg.version = version;
    scfg.max_actions = max_actions;
    scfg.max_events = max_events;
    p->sp = NULL;
    if (moq_simpair_create(&scfg, &p->sp) < 0) return false;
    if (moq_simpair_start(p->sp) < 0) { moq_simpair_destroy(p->sp); return false; }
    moq_simpair_run_until_quiescent(p->sp, 32, NULL);
    p->c = moq_simpair_client(p->sp);
    p->sv = moq_simpair_server(p->sp);
    return true;
}

static bool mk_pair_cfg(pair_t *p, moq_version_t version,
                        uint32_t max_actions, uint32_t max_events)
{
    return mk_pair_cfg_alloc(p, version, max_actions, max_events, moq_alloc_default());
}

static bool mk_pair(pair_t *p)
{
    return mk_pair_cfg(p, MOQ_VERSION_DRAFT_22, 0, 0);
}

static void pump(pair_t *p)
{
    moq_simpair_run_until_quiescent(p->sp, 64, NULL);
}

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

/* Take the first event of `kind`, discarding everything queued before it;
 * later events stay queued. */
static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == kind) { *out = ev; return true; }
        moq_event_cleanup(&ev);
    }
    return false;
}

/* Count queued events of one kind without consuming anything. */
static size_t queued_events(const moq_session_t *s, moq_event_kind_t kind)
{
    size_t n = 0;
    for (size_t i = s->event_head; i < s->event_tail; i++)
        if (s->events[i % s->event_cap].kind == kind) n++;
    return n;
}

/* Count queued actions of one kind without consuming anything. */
static size_t queued_actions(const moq_session_t *s, uint32_t kind)
{
    size_t n = 0;
    for (size_t i = s->action_head; i < s->action_tail; i++)
        if (s->actions[i % s->action_cap].kind == kind) n++;
    return n;
}

static size_t queued_action_total(const moq_session_t *s)
{
    return s->action_tail - s->action_head;
}

/* Snapshot of the fill pool for reserve-before-mutate pins. */
typedef struct pool_snap {
    moq_fill_table_t table;
    moq_fill_entry_t entries[4];
} pool_snap_t;

static void snap_pool(const moq_session_t *s, pool_snap_t *out)
{
    memset(out, 0, sizeof(*out));
    out->table = s->fills;
    size_t n = s->fills.capacity < 4 ? s->fills.capacity : 4;
    if (n) memcpy(out->entries, s->fill_storage, n * sizeof(moq_fill_entry_t));
}

static bool pool_same(const moq_session_t *s, const pool_snap_t *snap)
{
    if (memcmp(&s->fills, &snap->table, sizeof(snap->table)) != 0) return false;
    size_t n = s->fills.capacity < 4 ? s->fills.capacity : 4;
    return n == 0 || memcmp(snap->entries, s->fill_storage, n * sizeof(moq_fill_entry_t)) == 0;
}

static moq_bytes_t ns_parts[] = { { (const uint8_t *)"live", 4 } };

static void subscribe_cfg_with_fill(moq_subscribe_cfg_t *cfg,
                                    const moq_fill_cfg_t *fill)
{
    moq_subscribe_cfg_init(cfg);
    cfg->track_namespace = (moq_namespace_t){ ns_parts, 1 };
    cfg->track_name = MOQ_BYTES_LITERAL("v");
    cfg->filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg->fill = fill;
}

/* SUBSCRIBE (with an optional fill) and accept it with the given Largest.
 * Leaves SUBSCRIBE_OK / FILL_CREATED on the client queue and FILL_REQUEST (if
 * a stream is owed) on the server queue. */
static int establish(pair_t *p, const moq_fill_cfg_t *fill, bool has_largest,
                     uint64_t lg, uint64_t lo, moq_subscription_t *csub,
                     moq_subscription_t *ssub)
{
    int failures = 0;
    moq_subscribe_cfg_t cfg;
    subscribe_cfg_with_fill(&cfg, fill);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p->c, &cfg, 1, csub), (int)MOQ_OK);
    pump(p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    *ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = has_largest;
    ac.largest_group = lg;
    ac.largest_object = lo;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(p->sv, *ssub, &ac, 2), (int)MOQ_OK);
    pump(p);
    return failures;
}

static moq_rcbuf_t *mk_payload(const char *text)
{
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)text, strlen(text), &b);
    return b;
}

static moq_result_t write_fill(moq_session_t *s, moq_fill_t f, uint64_t g, uint64_t o,
                               const char *text, uint64_t now)
{
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = g;
    oc.subgroup_id = 0;
    oc.object_id = o;
    oc.publisher_priority = 5;
    oc.payload = mk_payload(text);
    moq_result_t rc = moq_session_write_fill_object(s, f, &oc, now);
    moq_rcbuf_decref(oc.payload);
    return rc;
}


/* -- Hand-pumped pair: the delivery order between the request bidi and the
 * data streams is chosen by the test, and nothing is delivered implicitly. */

#define RAW_DATA_REF_OFFSET UINT64_C(0x100000)

typedef struct bag { moq_event_t ev[64]; size_t n; } bag_t;

/* Pull every queued event into the bag (overflow is released). */
static void pull_all(moq_session_t *s, bag_t *b)
{
    b->n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (b->n < 64) b->ev[b->n++] = ev; else moq_event_cleanup(&ev);
    }
}

static int bag_count(const bag_t *b, moq_event_kind_t kind, const moq_event_t **first)
{
    int n = 0;
    for (size_t i = 0; i < b->n; i++) {
        if (b->ev[i].kind != kind) continue;
        if (n == 0 && first) *first = &b->ev[i];
        n++;
    }
    return n;
}

static void bag_free(bag_t *b)
{
    for (size_t i = 0; i < b->n; i++) moq_event_cleanup(&b->ev[i]);
    b->n = 0;
}

/* Poll everything, count the events of one kind, keep the first of them in
 * *first (caller releases) and release the rest. */
static int count_kind(moq_session_t *s, moq_event_kind_t kind, moq_event_t *first)
{
    bag_t b;
    pull_all(s, &b);
    const moq_event_t *f = NULL;
    int n = bag_count(&b, kind, &f);
    for (size_t i = 0; i < b.n; i++) {
        if (first && &b.ev[i] == f) *first = b.ev[i];
        else moq_event_cleanup(&b.ev[i]);
    }
    return n;
}

static void deliver(moq_session_t *to, const moq_action_t *a, bool from_server, uint64_t now)
{
    if (from_server && a->kind == MOQ_ACTION_SEND_BIDI_STREAM)
        capture_done(a->u.send_bidi_stream.data, a->u.send_bidi_stream.len,
                     a->u.send_bidi_stream.fin);
    switch (a->kind) {
    case MOQ_ACTION_OPEN_UNI_CONTROL:
        moq_session_on_control_bytes(to, a->u.open_uni_control.data, a->u.open_uni_control.len, now);
        break;
    case MOQ_ACTION_SEND_UNI_CONTROL:
        moq_session_on_control_bytes(to, a->u.send_uni_control.data, a->u.send_uni_control.len, now);
        break;
    case MOQ_ACTION_SEND_CONTROL:
        moq_session_on_control_bytes(to, a->u.send_control.data, a->u.send_control.len, now);
        break;
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        moq_session_on_bidi_stream_bytes(to, a->u.open_bidi_stream.stream_ref,
            a->u.open_bidi_stream.data, a->u.open_bidi_stream.len, a->u.open_bidi_stream.fin, now);
        break;
    case MOQ_ACTION_SEND_BIDI_STREAM:
        moq_session_on_bidi_stream_bytes(to, a->u.send_bidi_stream.stream_ref,
            a->u.send_bidi_stream.data, a->u.send_bidi_stream.len, a->u.send_bidi_stream.fin, now);
        break;
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        moq_session_on_bidi_stream_bytes(to, a->u.close_bidi_stream.stream_ref, NULL, 0, true, now);
        break;
    case MOQ_ACTION_ABORT_BIDI_STREAM:
        moq_session_on_bidi_stream_reset(to, a->u.abort_bidi_stream.stream_ref,
            a->u.abort_bidi_stream.error_code, now);
        moq_session_on_bidi_stream_stop(to, a->u.abort_bidi_stream.stream_ref,
            a->u.abort_bidi_stream.error_code, now);
        break;
    case MOQ_ACTION_SEND_DATA: {
        moq_stream_ref_t r = moq_stream_ref_from_u64(a->u.send_data.stream_ref._v +
            (from_server ? RAW_DATA_REF_OFFSET : 0));
        bool hp = a->u.send_data.payload != NULL;
        if (a->u.send_data.header_len > 0)
            moq_session_on_data_bytes(to, r, a->u.send_data.header, a->u.send_data.header_len,
                                      a->u.send_data.fin && !hp, now);
        if (hp)
            moq_session_on_data_bytes(to, r, moq_rcbuf_data(a->u.send_data.payload),
                                      moq_rcbuf_len(a->u.send_data.payload), a->u.send_data.fin, now);
        if (!hp && a->u.send_data.header_len == 0 && a->u.send_data.fin)
            moq_session_on_data_bytes(to, r, NULL, 0, true, now);
        break;
    }
    case MOQ_ACTION_RESET_DATA:
        moq_session_on_data_reset(to, moq_stream_ref_from_u64(a->u.reset_data.stream_ref._v +
            (from_server ? RAW_DATA_REF_OFFSET : 0)), a->u.reset_data.error_code, now);
        break;
    case MOQ_ACTION_STOP_DATA:
        moq_session_on_data_stop(to, moq_stream_ref_from_u64(a->u.stop_data.stream_ref._v -
            (from_server ? 0 : RAW_DATA_REF_OFFSET)), a->u.stop_data.error_code, now);
        break;
    default: break;
    }
}

/* Deliver one side's actions to the other; returns how many of `kind` went. */
static int raw_pump(moq_session_t *from, moq_session_t *to, bool from_server, uint64_t now,
                    uint32_t kind)
{
    int n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        if (a.kind == kind) n++;
        deliver(to, &a, from_server, now);
        moq_action_cleanup(&a);
    }
    return n;
}

static void raw_run(moq_session_t *c, moq_session_t *sv, uint64_t now)
{
    for (int i = 0; i < 8; i++) {
        raw_pump(c, sv, false, now, 0);
        raw_pump(sv, c, true, now, 0);
    }
}

static moq_session_t *raw_session_alloc(moq_perspective_t persp, moq_version_t version,
                                  uint32_t max_fills, uint32_t max_receive_buffer_bytes,
                                  const moq_alloc_t *alloc)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), alloc, persp);
    cfg.version = version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.max_fills = max_fills;
    cfg.max_receive_buffer_bytes = max_receive_buffer_bytes;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (test_bind_if_required(s) < 0) { moq_session_destroy(s); return NULL; }
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    return s;
}

static bool raw_pair_v_alloc(moq_session_t **c, moq_session_t **sv, moq_version_t version,
                       uint32_t c_fills, uint32_t sv_fills, uint32_t c_recv_budget,
                       const moq_alloc_t *alloc)
{
    memset(&done_wire, 0, sizeof(done_wire));
    *c = raw_session_alloc(MOQ_PERSPECTIVE_CLIENT, version, c_fills, c_recv_budget, alloc);
    *sv = raw_session_alloc(MOQ_PERSPECTIVE_SERVER, version, sv_fills, 0, alloc);
    if (!*c || !*sv) {
        if (*c) moq_session_destroy(*c);
        if (*sv) moq_session_destroy(*sv);
        return false;
    }
    raw_run(*c, *sv, 1);
    drain(*c); drain(*sv);
    return true;
}

static bool raw_pair_v(moq_session_t **c, moq_session_t **sv, moq_version_t version,
                       uint32_t c_fills, uint32_t sv_fills, uint32_t c_recv_budget)
{
    return raw_pair_v_alloc(c, sv, version, c_fills, sv_fills, c_recv_budget, moq_alloc_default());
}

static bool raw_pair(moq_session_t **c, moq_session_t **sv, uint32_t c_fills, uint32_t sv_fills,
                     uint32_t c_recv_budget)
{
    return raw_pair_v(c, sv, MOQ_VERSION_DRAFT_22, c_fills, sv_fills, c_recv_budget);
}

/* SUBSCRIBE with `filter` (and its start group), an optional fill, accepted
 * with Largest (5,2). */
static bool raw_establish(moq_session_t *c, moq_session_t *sv, moq_subscribe_filter_t filter,
                          uint64_t start_group, const moq_fill_cfg_t *fill,
                          moq_subscription_t *ch, moq_subscription_t *sh)
{
    moq_subscribe_cfg_t scfg;
    subscribe_cfg_with_fill(&scfg, fill);
    scfg.filter = filter;
    scfg.start_group = start_group;
    if (moq_session_subscribe(c, &scfg, 2, ch) != MOQ_OK) return false;
    raw_run(c, sv, 3);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    *sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    if (moq_session_accept_subscribe(sv, *sh, &ac, 4) != MOQ_OK) return false;
    raw_run(c, sv, 5);
    return true;
}

static moq_location_filter_cfg_t abs_start(uint64_t g, uint64_t o)
{
    moq_location_filter_cfg_t l;
    moq_location_filter_cfg_init(&l);
    l.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    l.start_group = g; l.start_object = o;
    return l;
}

/* Poll every action in order into `out` (at most `cap`); returns the count. */
static size_t poll_actions_into(moq_session_t *s, moq_action_t *out, size_t cap)
{
    size_t n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (n < cap) out[n++] = a; else moq_action_cleanup(&a);
    }
    return n;
}

static int first_index_of(const moq_action_t *acts, size_t n, uint32_t kind)
{
    for (size_t i = 0; i < n; i++) if (acts[i].kind == kind) return (int)i;
    return -1;
}

/* Apply only to a raw, unobservable fill history after its real header and
 * terminal were attributed. A missing/malformed wire DONE, wrong processed
 * total or finite count FAILS before ticking. This is the receiver's existing
 * bounded recovery, not a replacement for known finite/zero completion. */
static int recover_raw_fill_done(moq_session_t *c, moq_subscription_t sub,
                                 moq_publication_t pub, uint64_t processed,
                                 uint64_t status)
{
    int failures = 0;
    bool publication = moq_publication_is_valid(pub);
    MOQ_TEST_CHECK(publication != moq_subscription_is_valid(sub));
    MOQ_TEST_CHECK_EQ_SIZE(done_wire.n, 1);
    MOQ_TEST_CHECK(done_wire.decoded && done_wire.fin);
    MOQ_TEST_CHECK_EQ_U64(done_wire.streams, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(done_wire.status, status);
    int slot = publication ? pub_resolve_handle(c, pub) : sub_resolve_handle(c, sub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) return failures;
    bool pending = publication ? c->publishes[slot].done_pending : c->subs[slot].done_pending;
    uint64_t count = publication ? c->publishes[slot].done_stream_count : c->subs[slot].done_stream_count;
    uint64_t actual = publication ? c->publishes[slot].processed_stream_count : c->subs[slot].processed_stream_count;
    uint64_t code = publication ? c->publishes[slot].done_status_code : c->subs[slot].done_status_code;
    uint64_t deadline = publication ? c->publishes[slot].done_deadline_us : c->subs[slot].done_deadline_us;
    moq_event_kind_t kind = publication ? MOQ_EVENT_PUBLISH_FINISHED : MOQ_EVENT_SUBSCRIBE_DONE;
    MOQ_TEST_CHECK(pending);
    MOQ_TEST_CHECK_EQ_U64(count, done_wire.streams);
    MOQ_TEST_CHECK_EQ_U64(code, status);
    MOQ_TEST_CHECK_EQ_U64(actual, processed);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, kind), 0);
    MOQ_TEST_CHECK(deadline > c->last_now_us && deadline != UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(c) <= deadline);
    if (done_wire.n != 1 || !done_wire.decoded || !done_wire.fin ||
        done_wire.streams != UINT64_MAX || done_wire.status != status ||
        !pending || count != UINT64_MAX || code != status ||
        actual != processed || deadline <= c->last_now_us || deadline == UINT64_MAX)
        return failures;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(c, deadline - 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, kind), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(c, deadline), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, kind), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(c, deadline), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, kind), 1);
    for (size_t i = c->event_head; i < c->event_tail; ++i) {
        const moq_event_t *event = &c->events[i % c->event_cap];
        if (event->kind != kind) continue;
        if (publication) {
            MOQ_TEST_CHECK(moq_publication_eq(event->u.publish_finished.pub, pub));
            MOQ_TEST_CHECK_EQ_U64(event->u.publish_finished.status_code, status);
            MOQ_TEST_CHECK_EQ_U64(event->u.publish_finished.stream_count, done_wire.streams);
        } else {
            MOQ_TEST_CHECK(moq_subscription_eq(event->u.subscribe_done.sub, sub));
            MOQ_TEST_CHECK_EQ_U64(event->u.subscribe_done.status_code, status);
            MOQ_TEST_CHECK_EQ_U64(event->u.subscribe_done.stream_count, done_wire.streams);
        }
    }
    return failures;
}

/* Lossless SimPair has a real virtual-open observer. Keep these finite and
 * prompt; genuine raw cases above retain their independent sentinel/deadline. */
static int check_sim_fill_done(moq_session_t *c, moq_subscription_t sub,
                               moq_publication_t pub, uint64_t count,
                               uint64_t status)
{
    int failures = 0;
    bool publication = moq_publication_is_valid(pub);
    moq_event_kind_t kind = publication ? MOQ_EVENT_PUBLISH_FINISHED : MOQ_EVENT_SUBSCRIBE_DONE;
    MOQ_TEST_CHECK_EQ_SIZE(done_wire.n, 1);
    MOQ_TEST_CHECK(done_wire.decoded && done_wire.fin);
    MOQ_TEST_CHECK_EQ_U64(done_wire.streams, count);
    MOQ_TEST_CHECK_EQ_U64(done_wire.status, status);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, kind), 1);
    for (size_t i = c->event_head; i < c->event_tail; ++i) {
        const moq_event_t *event = &c->events[i % c->event_cap];
        if (event->kind != kind) continue;
        if (publication) {
            MOQ_TEST_CHECK(moq_publication_eq(event->u.publish_finished.pub, pub));
            MOQ_TEST_CHECK_EQ_U64(event->u.publish_finished.status_code, status);
            MOQ_TEST_CHECK_EQ_U64(event->u.publish_finished.stream_count, count);
        } else {
            MOQ_TEST_CHECK(moq_subscription_eq(event->u.subscribe_done.sub, sub));
            MOQ_TEST_CHECK_EQ_U64(event->u.subscribe_done.status_code, status);
            MOQ_TEST_CHECK_EQ_U64(event->u.subscribe_done.stream_count, count);
        }
    }
    return failures;
}

/* -- A complete fill: request, stream, objects, gap, FIN, terminal once. --- */

static int test_fill_round_trip(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 1; loc.start_object = 0;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = 250;
    fill.has_subscriber_priority = true;
    fill.subscriber_priority = 9;

    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 3, 7, &csub, &ssub);

    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(moq_fill_is_valid(cfill));
    MOQ_TEST_CHECK(moq_subscription_eq(ev.u.fill_created.sub, csub));
    MOQ_TEST_CHECK(!moq_publication_is_valid(ev.u.fill_created.pub));
    uint64_t initiating = ev.u.fill_created.initiating_request_id;
    MOQ_TEST_CHECK_EQ_U64(initiating, p.c->subs[sub_resolve_handle(p.c, csub)].request_id);

    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK(moq_fill_is_valid(sfill));
    MOQ_TEST_CHECK(moq_subscription_eq(ev.u.fill_request.sub, ssub));
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.initiating_request_id, initiating);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_group, 1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_object, 0);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_group, 3);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_object, 7);
    MOQ_TEST_CHECK(ev.u.fill_request.has_fill_timeout);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.fill_timeout_ms, 250);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_request.subscriber_priority, 9);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_request.group_order, (int)MOQ_GROUP_ORDER_ASCENDING);

    /* The publisher cannot write before opening, and the subscriber cannot
     * open or write at all. */
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 1, 0, "x", 3), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.c, cfill, 3), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.c, cfill, 3), (int)MOQ_ERR_WRONG_STATE);

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 3), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 1, 0, "one", 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 1, 1, "two", 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fill_range(p.sv, sfill,
        MOQ_FETCH_RANGE_NON_EXISTENT, 2, 0, 5), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fill_range(p.sv, sfill,
        MOQ_FETCH_RANGE_TIMED_OUT, 3, 7, 5), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 6), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 6), (int)MOQ_ERR_STALE_HANDLE);
    pump(&p);

    /* The publisher's terminal: COMPLETE, exactly once, then the handle is
     * identifier-only. */
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_TERMINAL), 1);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, sfill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_NONE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fill(p.sv, sfill, 1, 7), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);

    /* The subscriber's view, in order: two objects, two gaps, the terminal. */
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 2);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_GAP), 2);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_TERMINAL), 1);
    int seen = 0;
    moq_event_t e2;
    while (moq_session_poll_events(p.c, &e2, 1) > 0) {
        if (e2.kind == MOQ_EVENT_FILL_OBJECT) {
            MOQ_TEST_CHECK(moq_fill_eq(e2.u.fill_object.fill, cfill));
            MOQ_TEST_CHECK(moq_subscription_eq(e2.u.fill_object.sub, csub));
            MOQ_TEST_CHECK_EQ_U64(e2.u.fill_object.initiating_request_id, initiating);
            MOQ_TEST_CHECK_EQ_U64(e2.u.fill_object.group_id, 1);
            MOQ_TEST_CHECK_EQ_U64(e2.u.fill_object.object_id, (uint64_t)seen);
            MOQ_TEST_CHECK(e2.u.fill_object.payload != NULL);
            if (e2.u.fill_object.payload) {
                MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(e2.u.fill_object.payload), 3);
                MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(e2.u.fill_object.payload), 1);
            }
            MOQ_TEST_CHECK_EQ_INT((int)e2.u.fill_object.publisher_priority, 5);
            seen++;
        } else if (e2.kind == MOQ_EVENT_FILL_GAP) {
            MOQ_TEST_CHECK(moq_fill_eq(e2.u.fill_gap.fill, cfill));
            MOQ_TEST_CHECK(seen == 2);
            if (e2.u.fill_gap.group_id == 2) {
                MOQ_TEST_CHECK_EQ_INT((int)e2.u.fill_gap.kind, (int)MOQ_FETCH_RANGE_NON_EXISTENT);
            } else {
                MOQ_TEST_CHECK_EQ_U64(e2.u.fill_gap.group_id, 3);
                MOQ_TEST_CHECK_EQ_U64(e2.u.fill_gap.object_id, 7);
                MOQ_TEST_CHECK_EQ_INT((int)e2.u.fill_gap.kind, (int)MOQ_FETCH_RANGE_TIMED_OUT);
            }
        } else if (e2.kind == MOQ_EVENT_FILL_TERMINAL) {
            MOQ_TEST_CHECK(moq_fill_eq(e2.u.fill_terminal.fill, cfill));
            MOQ_TEST_CHECK_EQ_INT((int)e2.u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
            MOQ_TEST_CHECK_EQ_INT((int)e2.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_NONE);
            MOQ_TEST_CHECK(seen == 2);
        }
        moq_event_cleanup(&e2);
    }
    MOQ_TEST_CHECK_EQ_INT(seen, 2);
    /* Stale after the terminal; the pool slot is reclaimed. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(p.c, cfill, 8), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    /* The fill stream counted toward the subscription's processed streams. */
    MOQ_TEST_CHECK_EQ_U64(p.c->subs[sub_resolve_handle(p.c, csub)].processed_stream_count, 1);
    /* The subscription itself is untouched by the fill's end. */
    MOQ_TEST_CHECK_EQ_INT((int)p.c->subs[sub_resolve_handle(p.c, csub)].state, (int)MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)p.sv->subs[sub_resolve_handle(p.sv, ssub)].state, (int)MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- Overlapping fills: a REQUEST_UPDATE fill does not cancel the first. --- */

/* With max_actions 2 the cancellation needs the parent's action plus two
 * STOPs, more than the queue holds at once: the parent's action is all that
 * is reserved, and the STOPs that do not fit are retained and issued as
 * actions drain, so every outcome below still holds. */
static int test_overlapping_fills_and_parent_cancel(uint32_t max_actions)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair_cfg(&p, MOQ_VERSION_DRAFT_22, max_actions, 0)) {
        MOQ_TEST_CHECK(false);
        return failures;
    }
    drain(p.c); drain(p.sv);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);   /* inherits the subscription's filter */
    moq_subscription_t csub, ssub;
    /* The subscription filter is the next object, so the inherited fill range
     * would be empty; give the fill its own open range. */
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    loc.relative_group_offset = 2;   /* the group before the current one */
    fill.location = &loc;
    failures += establish(&p, &fill, true, 5, 2, &csub, &ssub);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill1 = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill1 = ev.u.fill_request.fill;
    uint64_t id1 = ev.u.fill_request.initiating_request_id;
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_group, 4);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_group, 5);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_object, 2);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill1, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill1, 4, 0, "a", 3), (int)MOQ_OK);
    pump(&p);

    /* A second fill rides a REQUEST_UPDATE while the first is still open. */
    moq_location_filter_cfg_t loc2;
    moq_location_filter_cfg_init(&loc2);
    loc2.kind = MOQ_LOCATION_FILTER_GROUP_END;
    loc2.start_group = 0; loc2.start_object = 0; loc2.end_group = 1;
    moq_fill_cfg_t fill2;
    moq_fill_cfg_init(&fill2);
    fill2.location = &loc2;
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.fill = &fill2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(p.c, csub, &up, 4), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill2 = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(!moq_fill_eq(cfill1, cfill2));
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill2 = ev.u.fill_request.fill;
    MOQ_TEST_CHECK(!moq_fill_eq(sfill1, sfill2));
    MOQ_TEST_CHECK(ev.u.fill_request.initiating_request_id != id1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_group, 0);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_group, 1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_object, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 2);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 2);

    /* The first fill keeps running after the second was requested. */
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill1, 4, 1, "b", 5), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill2, 5), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill2, 0, 0, "c", 5), (int)MOQ_OK);
    pump(&p);
    /* "b" on the first fill and "c" on the second ("a" was consumed with the
     * events before the second FILL_CREATED). */
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 2);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_TERMINAL), 0);
    drain(p.c);

    /* Cancelling the subscription resets every open fill: both streams are
     * reset on the publisher, both children end PARENT_TERMINATED on both
     * sides, and the fetch-response streams stay reset on the wire. */
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.c), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, csub, 6), (int)MOQ_OK);
    if (max_actions == 0)
        MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.c, MOQ_ACTION_STOP_DATA), 2);
    else
        MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.c), max_actions);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_TERMINAL), 2);
    size_t resets = 0;
    moq_event_t t;
    while (moq_session_poll_events(p.sv, &t, 1) > 0) {
        if (t.kind == MOQ_EVENT_FILL_TERMINAL) {
            MOQ_TEST_CHECK_EQ_INT((int)t.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
            resets++;
        }
        moq_event_cleanup(&t);
    }
    MOQ_TEST_CHECK_EQ_SIZE(resets, 2);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_TERMINAL), 2);
    size_t cterms = 0;
    while (moq_session_poll_events(p.c, &t, 1) > 0) {
        if (t.kind == MOQ_EVENT_FILL_TERMINAL) {
            MOQ_TEST_CHECK_EQ_INT((int)t.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
            cterms++;
        }
        moq_event_cleanup(&t);
    }
    MOQ_TEST_CHECK_EQ_SIZE(cterms, 2);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fill_range(p.sv, sfill1,
        MOQ_FETCH_RANGE_UNKNOWN, 4, 2, 7), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- A fill reset leaves the parent alive; a failed fill is header + RESET. */

static int test_fill_failure_leaves_parent_alive(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 2, 0, &csub, &ssub);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;

    /* Abort before opening: the stream is opened with its header and reset
     * right behind it, in that order. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fill(p.sv, sfill, 0x77, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.sv), 2);
    {
        const moq_action_t *a0 = &p.sv->actions[p.sv->action_head % p.sv->action_cap];
        const moq_action_t *a1 = &p.sv->actions[(p.sv->action_head + 1) % p.sv->action_cap];
        MOQ_TEST_CHECK_EQ_INT((int)a0->kind, (int)MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(a0->u.send_data.header_len > 0);
        MOQ_TEST_CHECK(!a0->u.send_data.fin);
        MOQ_TEST_CHECK_EQ_INT((int)a1->kind, (int)MOQ_ACTION_RESET_DATA);
        MOQ_TEST_CHECK_EQ_U64(a1->u.reset_data.error_code, 0x77);
        MOQ_TEST_CHECK(a0->u.send_data.stream_ref._v == a1->u.reset_data.stream_ref._v);
    }
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_LOCAL_ABORTED);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_STREAM);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x77);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, cfill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_STREAM);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x77);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);

    /* The subscription still delivers: a subgroup object arrives as usual. */
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 3; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(p.sv, ssub, &sg, 4, &sgh), (int)MOQ_OK);
    moq_rcbuf_t *pl = mk_payload("live");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, sgh, 0, pl, 4), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.sv, sgh, 4), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_OBJECT_RECEIVED, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)p.c->subs[sub_resolve_handle(p.c, csub)].state, (int)MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- The subscriber cancels one fill with STOP_SENDING only. --------------- */

static int test_subscriber_cancels_one_fill(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 2, 0, &csub, &ssub);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 0, 0, "a", 3), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_OBJECT, &ev));
    moq_event_cleanup(&ev);

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(p.c, cfill, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.c, MOQ_ACTION_STOP_DATA), 1);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.c, MOQ_ACTION_ABORT_BIDI_STREAM), 0);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_LOCAL_CANCELLED);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_TERMINAL), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(p.c, cfill, 4), (int)MOQ_ERR_STALE_HANDLE);
    pump(&p);
    /* The publisher answers the STOP with a RESET and ends the child. */
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, sfill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_STREAM);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 0, 1, "b", 5), (int)MOQ_ERR_STALE_HANDLE);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    /* Neither subscription moved. */
    MOQ_TEST_CHECK_EQ_INT((int)p.c->subs[sub_resolve_handle(p.c, csub)].state, (int)MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)p.sv->subs[sub_resolve_handle(p.sv, ssub)].state, (int)MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- Empty range and paused subscription: no stream, NO_STREAM both sides. - */

static int expect_no_stream(pair_t *p, const moq_fill_cfg_t *fill, bool paused)
{
    int failures = 0;
    moq_subscribe_cfg_t cfg;
    subscribe_cfg_with_fill(&cfg, fill);
    if (paused) { cfg.has_forward = true; cfg.forward = false; }
    moq_subscription_t csub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p->c, &cfg, 1, &csub), (int)MOQ_OK);
    pump(p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 2; ac.largest_object = 0;
    size_t data_before = queued_actions(p->sv, MOQ_ACTION_SEND_DATA);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(p->sv, ssub, &ac, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p->sv, MOQ_EVENT_FILL_REQUEST), 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p->sv, MOQ_ACTION_SEND_DATA), data_before);
    MOQ_TEST_CHECK(take(p->sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_NO_STREAM);
    MOQ_TEST_CHECK(moq_subscription_eq(ev.u.fill_terminal.sub, ssub));
    pump(p);
    MOQ_TEST_CHECK(take(p->c, MOQ_EVENT_FILL_CREATED, &ev));
    MOQ_TEST_CHECK(take(p->c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_NO_STREAM);
    MOQ_TEST_CHECK_EQ_SIZE(p->c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p->sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p->c, csub, 3), (int)MOQ_OK);
    pump(p);
    drain(p->c); drain(p->sv);
    return failures;
}

static int test_no_stream_outcomes(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    /* Starts after Largest: empty. */
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 9;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    failures += expect_no_stream(&p, &fill, false);
    /* A whole-track fill on a paused subscription opens nothing. */
    moq_fill_cfg_t whole;
    moq_fill_cfg_init(&whole);
    moq_location_filter_cfg_t none;
    moq_location_filter_cfg_init(&none);
    whole.location = &none;
    failures += expect_no_stream(&p, &whole, true);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- A REQUEST_UPDATE without a fill opens nothing. ------------------------ */

static int test_update_without_fill_opens_nothing(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_subscription_t csub, ssub;
    failures += establish(&p, NULL, true, 2, 0, &csub, &ssub);
    drain(p.c); drain(p.sv);
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.has_subscriber_priority = true;
    up.subscriber_priority = 3;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(p.c, csub, &up, 3), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_REQUEST), 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_CREATED), 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- The subscriber's pool exhaustion is a retryable refusal. ------------- */

static int test_pool_exhaustion_and_retry(void)
{
    int failures = 0;
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.fill = &fill;
    moq_event_t ev;

    /* The subscriber's pool holds one child: a second fill request is
     * refused as retryable, with no request sent and nothing changed; once
     * the first child ends the retry succeeds. */
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 1, 0, 0)) { MOQ_TEST_CHECK(false); return failures; }
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, &fill, &ch, &sh));
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    pool_snap_t before;
    snap_pool(c, &before);
    size_t acts = queued_action_total(c);
    bool pending = c->subs[sub_resolve_handle(c, ch)].update_pending;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch, &up, 6), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(c, &before));
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(c), acts);
    MOQ_TEST_CHECK(c->subs[sub_resolve_handle(c, ch)].update_pending == pending);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 7), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(sv, sfill, 7), (int)MOQ_OK);
    raw_run(c, sv, 8);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, cfill));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    drain(sv);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch, &up, 9), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_CREATED, NULL), 1);
    raw_run(c, sv, 10);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, NULL), 1);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- The publisher's pool is full: the fill fails on the wire. ------------ */

/* Deliver every action one side queued, checking that the first three are
 * the request response on its bidi, then a fill fetch stream's header, then
 * the RESET of that same stream with code 0. */
static int expect_response_then_failed_fill(moq_session_t *from, moq_session_t *to,
                                            uint8_t subscriber_priority, uint64_t now)
{
    int failures = 0;
    moq_action_t acts[8];
    size_t n = poll_actions_into(from, acts, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, 3);
    if (n == 3) {
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_INT((int)acts[1].kind, (int)MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(acts[1].u.send_data.header_len > 0);
        MOQ_TEST_CHECK(acts[1].u.send_data.payload == NULL);
        MOQ_TEST_CHECK(!acts[1].u.send_data.fin);
        MOQ_TEST_CHECK_EQ_U64((acts[1].u.send_data.scheduling_priority >> 8) & 0xff,
                              subscriber_priority);
        MOQ_TEST_CHECK_EQ_INT((int)acts[2].kind, (int)MOQ_ACTION_RESET_DATA);
        MOQ_TEST_CHECK_EQ_U64(acts[2].u.reset_data.stream_ref._v,
                              acts[1].u.send_data.stream_ref._v);
        MOQ_TEST_CHECK_EQ_U64(acts[2].u.reset_data.error_code, 0);
    }
    for (size_t i = 0; i < n; i++) { deliver(to, &acts[i], true, now); moq_action_cleanup(&acts[i]); }
    return failures;
}

/* The subscriber's view of a fill the publisher failed: exactly one terminal,
 * PEER_CANCELLED_OR_RESET in the stream domain with the stream's code. */
static int expect_failed_terminal(moq_session_t *c, moq_fill_t fill)
{
    int failures = 0;
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, fill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_STREAM);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0);
    moq_event_cleanup(&ev);
    return failures;
}

/* A fill the publisher has no slot for is answered as a fill failure, not a
 * refusal of the request: the SUBSCRIBE or REQUEST_UPDATE is processed
 * normally and a fill fetch stream carrying the initiating request id is
 * opened and reset right after its header -- with the accept for a SUBSCRIBE,
 * with the acknowledgement for a REQUEST_UPDATE on either parent family --
 * at the priority the fill would have had. The subscriber ends that child
 * PEER_CANCELLED_OR_RESET with the stream's code and its subscription stays.
 * A fill whose range would open no stream gets none; a rejected SUBSCRIBE
 * sends nothing for it; the fill the publisher holds is untouched and, once
 * it ends, the next request is admitted. */
static int test_publisher_pool_full(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 4, 1, 0)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_event_t ev;

    /* The one slot is taken by the first subscription's fill, kept unopened. */
    moq_subscription_t ch1, sh1;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, &fill, &ch1, &sh1));
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t held = ev.u.fill_request.fill;
    drain(c); drain(sv);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);

    /* SUBSCRIBE: admitted without a child, the fill remembered as unserved;
     * the accept carries SUBSCRIBE_OK, then the header and RESET. */
    moq_subscribe_cfg_t scfg;
    subscribe_cfg_with_fill(&scfg, &fill);
    scfg.track_name = MOQ_BYTES_LITERAL("second");
    scfg.has_subscriber_priority = true;
    scfg.subscriber_priority = 77;
    moq_subscription_t ch2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 6, &ch2), (int)MOQ_OK);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cf2 = ev.u.fill_created.fill;
    raw_run(c, sv, 7);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t sh2 = ev.u.subscribe_request.sub;
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);
    int s2 = sub_resolve_handle(sv, sh2);
    MOQ_TEST_CHECK(s2 >= 0 && sv->subs[s2].fill_unserved);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(sv), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sh2, &ac, 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(sv, MOQ_EVENT_FILL_REQUEST), 0);
    failures += expect_response_then_failed_fill(sv, c, 77, 9);
    MOQ_TEST_CHECK(s2 >= 0 && !sv->subs[s2].fill_unserved);
    MOQ_TEST_CHECK_EQ_U64(sv->subs[s2].fill_streams_opened, 1);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_SUBSCRIBE_OK, &ev));
    moq_event_cleanup(&ev);
    failures += expect_failed_terminal(c, cf2);
    MOQ_TEST_CHECK(sub_resolve_handle(c, ch2) >= 0);
    MOQ_TEST_CHECK(sub_resolve_handle(sv, sh2) >= 0);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);

    /* REQUEST_UPDATE on the subscription: acknowledged, then header and
     * RESET, at the update's own priority. */
    moq_subscription_update_cfg_t uc;
    moq_subscription_update_cfg_init_sized(&uc, sizeof(uc));
    uc.fill = &fill;
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = 33;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch2, &uc, 10), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_CREATED, &ev), 1);
    moq_fill_t cf3 = ev.u.fill_created.fill;
    moq_event_cleanup(&ev);
    raw_pump(c, sv, false, 11, 0);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(sv, MOQ_EVENT_FILL_REQUEST), 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);
    failures += expect_response_then_failed_fill(sv, c, 33, 12);
    MOQ_TEST_CHECK_EQ_U64(sv->subs[s2].fill_streams_opened, 2);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    failures += expect_failed_terminal(c, cf3);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK(sub_resolve_handle(c, ch2) >= 0);

    /* A fill whose range lies past Largest would open no stream: the update
     * is acknowledged alone and both sides conclude NO_STREAM. */
    moq_location_filter_cfg_t far = abs_start(9, 0);
    moq_fill_cfg_t ffar;
    moq_fill_cfg_init(&ffar);
    ffar.location = &far;
    uc.fill = &ffar;
    uc.has_subscriber_priority = false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch2, &uc, 13), (int)MOQ_OK);
    raw_pump(c, sv, false, 14, 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(sv, MOQ_ACTION_SEND_DATA), 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(sv, MOQ_ACTION_RESET_DATA), 0);
    raw_run(c, sv, 15);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_NO_STREAM);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_U64(sv->subs[s2].fill_streams_opened, 2);
    drain(sv);

    /* A rejected SUBSCRIBE sends nothing for its unserved fill. */
    moq_subscription_t ch4;
    scfg.track_name = MOQ_BYTES_LITERAL("fourth");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 16, &ch4), (int)MOQ_OK);
    raw_run(c, sv, 17);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t sh4 = ev.u.subscribe_request.sub;
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = 0x1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(sv, sh4, &rj, 18), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(sv, MOQ_ACTION_SEND_DATA), 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(sv, MOQ_ACTION_RESET_DATA), 0);
    raw_run(c, sv, 19);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_REJECTED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);

    /* The publication family: the server publishes, the client's update asks
     * for a fill the server has no slot for. */
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("w");
    moq_publication_t spub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(sv, &pc, 20, &spub), (int)MOQ_OK);
    raw_run(c, sv, 21);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t cpub = ev.u.publish_request.pub;
    moq_accept_publish_cfg_t apc;
    moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(c, cpub, &apc, 22), (int)MOQ_OK);
    raw_run(c, sv, 23);
    drain(c); drain(sv);
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 1; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(sv, spub, &sg, 24, &sgh), (int)MOQ_OK);
    moq_rcbuf_t *pl = mk_payload("o");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(sv, sgh, 0, pl, 24), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(sv, sgh, 24), (int)MOQ_OK);
    raw_run(c, sv, 25);
    drain(c); drain(sv);
    moq_location_filter_cfg_t zero = abs_start(0, 0);
    moq_fill_cfg_t fzero;
    moq_fill_cfg_init(&fzero);
    fzero.location = &zero;
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.fill = &fzero;
    up.has_subscriber_priority = true;
    up.subscriber_priority = 55;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cpub, &up, 26), (int)MOQ_OK);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cf5 = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(moq_publication_eq(ev.u.fill_created.pub, cpub));
    raw_pump(c, sv, false, 27, 0);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(sv, MOQ_EVENT_FILL_REQUEST), 0);
    failures += expect_response_then_failed_fill(sv, c, 55, 28);
    int ps = pub_resolve_handle(sv, spub);
    MOQ_TEST_CHECK(ps >= 0);
    if (ps >= 0) MOQ_TEST_CHECK_EQ_U64(sv->publishes[ps].fill_streams_opened, 1);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    failures += expect_failed_terminal(c, cf5);
    MOQ_TEST_CHECK(pub_resolve_handle(c, cpub) >= 0);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);

    /* The fill the publisher holds is untouched: it opens and completes, and
     * with its slot free the next request is admitted. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, held, 29), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(sv, held, 3, 0, "h", 29), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(sv, held, 29), (int)MOQ_OK);
    raw_run(c, sv, 30);
    {
        bag_t b;
        pull_all(c, &b);
        const moq_event_t *ft = NULL;
        MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_OBJECT, NULL), 1);
        MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
        if (ft) MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
        bag_free(&b);
    }
    drain(sv);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    uc.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch2, &uc, 31), (int)MOQ_OK);
    raw_run(c, sv, 32);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, NULL), 1);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- Tiny queues: every WOULD_BLOCK leaves state untouched; retry completes. */

static int test_tiny_queues(void)
{
    int failures = 0;
    pair_t p;
    /* Two action slots: enough for the handshake, too few for a batch. */
    if (!mk_pair_cfg(&p, MOQ_VERSION_DRAFT_22, 2, 0)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 2, 0, &csub, &ssub);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;

    /* Fill the publisher's action queue with a subgroup open + object. */
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 3; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(p.sv, ssub, &sg, 4, &sgh), (int)MOQ_OK);
    moq_rcbuf_t *pl = mk_payload("live");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, sgh, 0, pl, 4), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.sv), 2);

    pool_snap_t before;
    snap_pool(p.sv, &before);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 5), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(p.sv, &before));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fill(p.sv, sfill, 1, 5), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(p.sv, &before));
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.sv), 2);
    pump(&p);
    drain(p.c);
    /* One slot free is still too few for an unopened abort (header + reset). */
    sg.subgroup_id = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(p.sv, ssub, &sg, 6, &sgh), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_abort_fill(p.sv, sfill, 1, 6), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(p.sv, &before));
    pump(&p);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 7), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 0, 0, "a", 7), (int)MOQ_OK);
    snap_pool(p.sv, &before);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 0, 1, "b", 7), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(p.sv, &before));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 7), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(p.sv, &before));
    pump(&p);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 0, 1, "b", 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 8), (int)MOQ_OK);
    pump(&p);

    /* The subscriber side: a cancel that cannot queue its STOP is refused
     * with nothing changed; the completed stream's terminal arrived meanwhile,
     * so the cancel is then stale -- use a fresh fill for the STOP path. */
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 2);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(p.c, cfill, 9), (int)MOQ_ERR_STALE_HANDLE);
    drain(p.sv);

    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(p.c, csub, &up, 10), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill2 = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill2 = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill2, 11), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill2, 0, 0, "a", 11), (int)MOQ_OK);
    pump(&p);
    drain(p.c);
    /* Occupy the subscriber's two action slots with its own updates. */
    moq_subscription_update_cfg_t prio;
    moq_subscription_update_cfg_init_sized(&prio, sizeof(prio));
    prio.has_subscriber_priority = true;
    prio.subscriber_priority = 7;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(p.c, csub, &prio, 12), (int)MOQ_OK);
    /* The second slot: a SUBSCRIBE to another track fills it. */
    moq_subscribe_cfg_t other;
    moq_subscribe_cfg_init(&other);
    other.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    other.track_name = MOQ_BYTES_LITERAL("w");
    moq_subscription_t osub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &other, 12, &osub), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.c), 2);
    snap_pool(p.c, &before);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(p.c, cfill2, 13), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pool_same(p.c, &before));
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_TERMINAL), 0);
    pump(&p);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(p.c, cfill2, 14), (int)MOQ_OK);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_LOCAL_CANCELLED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- Owned buffers transfer on a full poll only. --------------------------- */

static int test_short_poll_keeps_ownership(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 2, 0, &csub, &ssub);
    moq_event_t ev;
    drain(p.c);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 0, 0, "payload", 3), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 1);
    size_t budget_before = p.c->recv_payload_bytes;
    MOQ_TEST_CHECK(budget_before >= 7);

    /* A poll whose element cannot hold the full frozen object event transfers
     * nothing: the event stays queued, the buffer stays owned by the session
     * and the receive budget is still charged. */
    uint8_t small[offsetof(moq_event_t, u) + MOQ_FILL_OBJECT_EVENT_V0_SIZE - 8];
    size_t n = 7;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_events_ex(p.c, small, 1, sizeof(small), &n),
                          (int)MOQ_ERR_ABI_MISMATCH);
    MOQ_TEST_CHECK_EQ_SIZE(n, 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 1);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->recv_payload_bytes, budget_before);

    /* The full poll transfers the single owned reference and releases the
     * budget. */
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_OBJECT, &ev));
    MOQ_TEST_CHECK(ev.u.fill_object.payload != NULL);
    if (ev.u.fill_object.payload) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(ev.u.fill_object.payload), 1);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(ev.u.fill_object.payload), 7);
    }
    MOQ_TEST_CHECK_EQ_SIZE(p.c->recv_payload_bytes, budget_before - 7);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(ev.u.fill_object.payload == NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 4), (int)MOQ_OK);
    pump(&p);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- A PUBLISH's subscriber requests a fill on its REQUEST_UPDATE. --------- */

static int test_publish_initiated_fill(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    pair_t p;
    if (!mk_pair_cfg_alloc(&p, MOQ_VERSION_DRAFT_22, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    /* Here the server publishes and the client is the subscriber. */
    drain(p.c); drain(p.sv);
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("v");
    moq_publication_t spub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(p.sv, &pc, 1, &spub), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t cpub = ev.u.publish_request.pub;
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(p.c, cpub, &ac, 2), (int)MOQ_OK);
    pump(&p);
    drain(p.c); drain(p.sv);
    /* Give the publisher a Largest to resolve against: one subgroup object. */
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 1; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(p.sv, spub, &sg, 3, &sgh), (int)MOQ_OK);
    moq_rcbuf_t *pl = mk_payload("o");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, sgh, 0, pl, 3), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.sv, sgh, 3), (int)MOQ_OK);
    pump(&p);
    drain(p.c); drain(p.sv);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.c, cpub, &up, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(moq_publication_eq(ev.u.fill_created.pub, cpub));
    MOQ_TEST_CHECK(!moq_subscription_is_valid(ev.u.fill_created.sub));
    pump(&p);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK(moq_publication_eq(ev.u.fill_request.pub, spub));
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_group, 0);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_group, 1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_object, 0);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 5), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 1, 0, "o", 5), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 5), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_OBJECT, &ev));
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_object.fill, cfill));
    MOQ_TEST_CHECK(moq_publication_eq(ev.u.fill_object.pub, cpub));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.c, cpub, &up, 4), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK_EQ_U64(p.c->publishes[pub_resolve_handle(p.c, cpub)].processed_stream_count, 2);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    /* Ending the publication resets the still-open second fill. */
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill2 = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill2, 6), (int)MOQ_OK);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    fc.stream_count = 1;   /* the subgroup; the session adds its two fills */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(p.sv, spub, &fc, 7), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv, MOQ_ACTION_RESET_DATA), 0);
    MOQ_TEST_CHECK_EQ_SIZE(done_wire.n, 0);
    pump(&p);             /* actual admission of the second empty fill header */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(p.sv, spub, &fc, 7), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv, MOQ_ACTION_RESET_DATA), 1);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
    pump(&p);
    /* All three real virtual opens retain their history after retirement. */
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_TERMINAL), 1);
    failures += check_sim_fill_done(p.c, MOQ_SUBSCRIPTION_INVALID, cpub, 3, 0);
    bag_t b;
    pull_all(p.c, &b);
    const moq_event_t *pf = NULL, *ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_PUBLISH_FINISHED, &pf), 1);
    if (pf) MOQ_TEST_CHECK_EQ_U64(pf->u.publish_finished.stream_count, 3);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    bag_free(&b);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    moq_simpair_destroy(p.sp);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    return failures;
}

/* -- PUBLISH_DONE's stream count includes fills; UINT64_MAX is unknown. ---- */

static int test_stream_count_includes_fills(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    pair_t p;
    if (!mk_pair_cfg_alloc(&p, MOQ_VERSION_DRAFT_22, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 2, 0, &csub, &ssub);
    moq_event_t ev;
    drain(p.c);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(p.sv, sfill, 3), (int)MOQ_OK);
    pump(&p);
    drain(p.sv);
    /* Application count zero plus one observed session-owned fill is one. */
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x2;
    dc.stream_count = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.sv, ssub, &dc, 4), (int)MOQ_OK);
    pump(&p);
    failures += check_sim_fill_done(p.c, csub, MOQ_PUBLICATION_INVALID, 1, 0x2);
    bool received = take(p.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    MOQ_TEST_CHECK(received);
    if (received) {
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, 1);
        moq_event_cleanup(&ev);
    }
    drain(p.c);

    /* Isolate the explicit application-unknown control in a fresh pair. */
    moq_simpair_destroy(p.sp);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    if (!mk_pair_cfg_alloc(&p, MOQ_VERSION_DRAFT_22, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    /* The explicit unknown sentinel on this profile also defers. */
    failures += establish(&p, NULL, true, 2, 0, &csub, &ssub);
    drain(p.c); drain(p.sv);
    dc.stream_count = UINT64_MAX;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.sv, ssub, &dc, 6), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_SUBSCRIBE_DONE), 0);
    int cslot = sub_resolve_handle(p.c, csub);
    MOQ_TEST_CHECK(cslot >= 0);
    if (cslot >= 0) {
        MOQ_TEST_CHECK(p.c->subs[cslot].done_pending);
        MOQ_TEST_CHECK_EQ_U64(p.c->subs[cslot].done_stream_count, UINT64_MAX);
    }
    moq_simpair_destroy(p.sp);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    return failures;
}

/* -- A profile without fills refuses before anything is sent. ------------- */

static int test_draft18_refuses_fill(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair_cfg(&p, MOQ_VERSION_DRAFT_18, 0, 0)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    moq_subscribe_cfg_t cfg;
    subscribe_cfg_with_fill(&cfg, &fill);
    moq_subscription_t csub;
    size_t acts = queued_action_total(p.c);
    uint64_t next_ref = p.c->next_stream_ref;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &cfg, 1, &csub), (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(!moq_subscription_is_valid(csub));
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.c), acts);
    MOQ_TEST_CHECK_EQ_U64(p.c->next_stream_ref, next_ref);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK(p.c->fill_storage == NULL);   /* nothing allocated for fills */
    /* Without the fill the same request goes out; an update with a fill is
     * refused the same way and leaves the subscription untouched. */
    cfg.fill = NULL;
    moq_subscription_t ssub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &cfg, 1, &csub), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    ssub = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(p.sv, ssub, &ac, 2), (int)MOQ_OK);
    pump(&p);
    drain(p.c); drain(p.sv);
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.fill = &fill;
    acts = queued_action_total(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(p.c, csub, &up, 3), (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.c), acts);
    MOQ_TEST_CHECK(!p.c->subs[sub_resolve_handle(p.c, csub)].update_pending);
    /* The publication update refuses a fill the same way. */
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    moq_bytes_t parts2[] = { MOQ_BYTES_LITERAL("other") };
    pc.track_namespace = (moq_namespace_t){ parts2, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("w");
    moq_publication_t spub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(p.sv, &pc, 4, &spub), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t cpub = ev.u.publish_request.pub;
    moq_accept_publish_cfg_t apc;
    moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(p.c, cpub, &apc, 5), (int)MOQ_OK);
    pump(&p);
    drain(p.c); drain(p.sv);
    moq_publication_update_cfg_t pup;
    moq_publication_update_cfg_init(&pup);
    pup.fill = &fill;
    acts = queued_action_total(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.c, cpub, &pup, 6), (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK_EQ_SIZE(queued_action_total(p.c), acts);
    MOQ_TEST_CHECK(!p.c->publishes[pub_resolve_handle(p.c, cpub)].update_pending);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- The subscription ends before the publisher opens the fill. ----------- */

/* An unbound subscriber child whose parent is gone cannot be counted toward
 * any stream count: it ends PARENT_TERMINATED and its slot is released, so
 * the pool is not consumed by such races. */
static int test_unsubscribe_before_open(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 2, 0, 0)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    moq_event_t ev;
    for (int round = 0; round < 2; round++) {
        MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
        MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, NULL), 1);
        MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_CREATED, NULL), 1);
        MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
        MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 1);
        /* The publisher never opens the fill; the subscriber leaves. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, ch, 6), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_NONE);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
        raw_run(c, sv, 7);
        MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
        drain(c); drain(sv);
    }
    /* With two slots and two such rounds behind it, a third request is
     * still admitted. */
    moq_subscribe_cfg_t scfg;
    subscribe_cfg_with_fill(&scfg, &fill);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 10, &ch), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- A rejected SUBSCRIBE ends the fill with the refusal on both sides. --- */

static int test_parent_rejected(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscribe_cfg_t scfg;
    subscribe_cfg_with_fill(&scfg, &fill);
    moq_subscription_t ch;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &scfg, 1, &ch), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t sh = ev.u.subscribe_request.sub;
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 1);
    moq_reject_subscribe_cfg_t rc;
    moq_reject_subscribe_cfg_init(&rc);
    rc.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(p.sv, sh, &rc, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(count_kind(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_REJECTED);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_PARENT_REQUEST);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, (uint64_t)MOQ_REQUEST_ERROR_UNAUTHORIZED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_SUBSCRIBE_ERROR), 1);
    MOQ_TEST_CHECK_EQ_INT(count_kind(p.c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, cfill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_REJECTED);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_PARENT_REQUEST);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, (uint64_t)MOQ_REQUEST_ERROR_UNAUTHORIZED);
    moq_event_cleanup(&ev);
    /* No stream can follow a refused request: the slot is released. */
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- A cancelled child's late stream is stopped at its header and counted. */

static int test_cancel_before_header(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    moq_session_t *c, *sv;
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 0, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    moq_event_t ev;
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    /* Cancelled before the publisher opened anything: the terminal is
     * immediate; the slot stays for the stream that may still come, so that
     * stream is stopped and counted when it does. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(c, cfill, 10), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_LOCAL_CANCELLED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(c, cfill, 10), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 11), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(sv, sfill, 3, 0, "late", 12), (int)MOQ_OK);
    raw_pump(sv, c, true, 13, 0);
    MOQ_TEST_CHECK_EQ_INT(raw_pump(c, sv, false, 14, MOQ_ACTION_STOP_DATA), 1);
    MOQ_TEST_CHECK_EQ_INT(raw_pump(sv, c, true, 15, MOQ_ACTION_RESET_DATA), 1);
    raw_run(c, sv, 16);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_OBJECT, NULL), 0);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, NULL), 0);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    /* The attributable RESET credits exactly one processed stream. The raw
     * fill handoff nevertheless leaves the sender unable to assert an exact
     * total, so require unknown and the receiver's retained deadline. */
    int cslot = sub_resolve_handle(c, ch);
    MOQ_TEST_CHECK(cslot >= 0);
    if (cslot >= 0) MOQ_TEST_CHECK_EQ_U64(c->subs[cslot].processed_stream_count, 1);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0; dc.stream_count = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 20), (int)MOQ_OK);
    raw_run(c, sv, 21);
    failures += recover_raw_fill_done(c, ch, MOQ_PUBLICATION_INVALID, 1, 0);
    int dones = count_kind(c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    MOQ_TEST_CHECK_EQ_INT(dones, 1);
    if (dones) {
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, UINT64_MAX);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);

    /* Cancelled before the header, and the subscription ends first: no
     * stream can be counted any more, so the slot goes with the parent and
     * no second terminal is published. */
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 0, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_FILL_CREATED, &ev));
    cfill = ev.u.fill_created.fill;
    drain(sv);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_fill(c, cfill, 10), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, NULL), 1);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, ch, 11), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, NULL), 0);
    raw_run(c, sv, 12);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    return failures;
}

/* -- A header for a child the subscriber resolved to no stream. ---------- */

/* The subscriber concluded NO_STREAM and released the child; a peer that
 * opens the stream anyway has it stopped, and the session stays open. */
static int test_header_after_no_stream(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 0, 0, 0)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(9, 0);   /* after Largest (5,2) */
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_NO_STREAM);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, NULL), 0);
    drain(sv);
    /* A FETCH_HEADER naming the SUBSCRIBE arrives on a fresh stream. */
    uint64_t req_id = c->subs[sub_resolve_handle(c, ch)].request_id;
    uint8_t hdr[4];
    size_t hlen = 0;
    hdr[hlen++] = 0x05;
    if (req_id < 64) {
        hdr[hlen++] = (uint8_t)req_id;
    } else {
        hdr[hlen++] = (uint8_t)(0x40 | (req_id >> 8));
        hdr[hlen++] = (uint8_t)(req_id & 0xFF);
    }
    moq_stream_ref_t late = moq_stream_ref_from_u64(RAW_DATA_REF_OFFSET + 77);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, late, hdr, hlen, false, 6), (int)MOQ_OK);
    moq_action_t acts[8];
    size_t n = poll_actions_into(c, acts, 8);
    MOQ_TEST_CHECK_EQ_INT(first_index_of(acts, n, MOQ_ACTION_CLOSE_SESSION), -1);
    int stop = first_index_of(acts, n, MOQ_ACTION_STOP_DATA);
    MOQ_TEST_CHECK(stop >= 0);
    if (stop >= 0) MOQ_TEST_CHECK_EQ_U64(acts[stop].u.stop_data.stream_ref._v, late._v);
    for (size_t i = 0; i < n; i++) moq_action_cleanup(&acts[i]);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_OBJECT, NULL), 0);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, NULL), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    /* The subscription itself is untouched. */
    MOQ_TEST_CHECK_EQ_INT((int)c->subs[sub_resolve_handle(c, ch)].state, (int)MOQ_SUB_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- PUBLISH_DONE reaches the subscriber before the fill data. ------------ */

/* The request bidi is delivered first; the fill's object and FIN follow.
 * The fill ends promptly COMPLETE, independent of the raw unknown-count
 * parent's retained deadline and exactly-once terminal. */
static int test_done_before_fill_data(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    moq_session_t *c, *sv;
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 0, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, &ev), 1);
    moq_fill_t sfill = ev.u.fill_request.fill;
    moq_event_cleanup(&ev);
    drain(c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 6), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(sv, sfill, 3, 0, "x", 6), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(sv, sfill, 6), (int)MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0;
    dc.stream_count = 0;   /* the session adds the fill stream it opened */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 6), (int)MOQ_OK);
    moq_action_t acts[16];
    size_t n = poll_actions_into(sv, acts, 16);
    for (size_t i = 0; i < n; i++)
        if (acts[i].kind != MOQ_ACTION_SEND_DATA && acts[i].kind != MOQ_ACTION_RESET_DATA)
            deliver(c, &acts[i], true, 7);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, MOQ_EVENT_SUBSCRIBE_DONE), 0);
    for (size_t i = 0; i < n; i++)
        if (acts[i].kind == MOQ_ACTION_SEND_DATA || acts[i].kind == MOQ_ACTION_RESET_DATA)
            deliver(c, &acts[i], true, 8);
    for (size_t i = 0; i < n; i++) moq_action_cleanup(&acts[i]);
    raw_run(c, sv, 9);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, MOQ_EVENT_FILL_TERMINAL), 1);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, MOQ_EVENT_FILL_OBJECT), 1);
    failures += recover_raw_fill_done(c, ch, MOQ_PUBLICATION_INVALID, 1, 0);
    bag_t b;
    pull_all(c, &b);
    const moq_event_t *ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_SUBSCRIBE_DONE, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_OBJECT, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) {
        MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
        MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_NONE);
    }
    bag_free(&b);
    MOQ_TEST_CHECK_EQ_INT(raw_pump(c, sv, false, 10, MOQ_ACTION_STOP_DATA), 0);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    return failures;
}

/* -- The receive budget stops a fill stream. ----------------------------- */

/* A fill object larger than the subscriber's retained-byte budget makes the
 * session stop the stream itself: the child ends once as a local cancel,
 * the slot is released, and ending the parent later queues no second STOP. */
static int test_budget_stop_fill_stream(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 0, 0, 4)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, &ev), 1);
    moq_fill_t sfill = ev.u.fill_request.fill;
    moq_event_cleanup(&ev);
    drain(c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 6), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(sv, sfill, 3, 0, "0123456789", 6), (int)MOQ_OK);
    raw_run(c, sv, 7);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_LOCAL_CANCELLED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, ch, 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(raw_pump(c, sv, false, 9, MOQ_ACTION_STOP_DATA), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- An update moves the filter and asks for a fill that inherits it. ----- */

/* SUBSCRIBE starts at a future group; the REQUEST_UPDATE moves the filter
 * back before Largest and requests a fill with no location of its own. Both
 * sides resolve the fill from the update's coordinates: the publisher serves
 * it and the subscriber delivers it with the session still open. */
static int test_update_filter_inheriting_fill(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 0, 0, 0)) { MOQ_TEST_CHECK(false); return failures; }
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 7, NULL, &ch, &sh));
    drain(c); drain(sv);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);   /* inherits the filter */
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.has_filter = true; up.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    up.start_group = 3; up.start_object = 0;
    up.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch, &up, 6), (int)MOQ_OK);
    raw_run(c, sv, 7);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, &ev), 1);
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_group, 3);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.start_object, 0);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_group, 5);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.end_object, 2);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_FILL_TERMINAL, NULL), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(sv, sfill, 3, 0, "y", 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(sv, sfill, 8), (int)MOQ_OK);
    raw_run(c, sv, 9);
    bag_t b;
    pull_all(c, &b);
    const moq_event_t *ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_OBJECT, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_COMPLETE);
    bag_free(&b);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- Ending the parent queues its fill resets ahead of PUBLISH_DONE. ------ */

/* A sender closes every stream it opened before PUBLISH_DONE: with an open
 * fill stream, done_subscribe and finish_publish queue the RESET first and
 * the terminal after it. */
static int test_done_resets_fills_before_terminal(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    moq_session_t *c, *sv;
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 0, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, &ev), 1);
    moq_fill_t sfill = ev.u.fill_request.fill;
    moq_event_cleanup(&ev);
    drain(c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 6), (int)MOQ_OK);
    raw_run(c, sv, 7);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0; dc.stream_count = 0;   /* the session adds its fill */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 8), (int)MOQ_OK);
    moq_action_t acts[8];
    size_t n = poll_actions_into(sv, acts, 8);
    int reset = first_index_of(acts, n, MOQ_ACTION_RESET_DATA);
    int done = first_index_of(acts, n, MOQ_ACTION_SEND_BIDI_STREAM);
    MOQ_TEST_CHECK(reset >= 0);
    MOQ_TEST_CHECK(done >= 0);
    MOQ_TEST_CHECK(reset < done);
    for (size_t i = 0; i < n; i++) { deliver(c, &acts[i], true, 9); moq_action_cleanup(&acts[i]); }
    raw_run(c, sv, 10);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, MOQ_EVENT_FILL_TERMINAL), 1);
    failures += recover_raw_fill_done(c, ch, MOQ_PUBLICATION_INVALID, 1, 0);
    bag_t b;
    pull_all(c, &b);
    const moq_event_t *ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_SUBSCRIBE_DONE, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    bag_free(&b);
    drain(sv);

    /* Independent parent, fresh clocks after the subscription deadline. */
    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 0, 0, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    /* The publication family: the server publishes, the client's update
     * asks for the fill, the server opens it and finishes. */
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("w");
    moq_publication_t spub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(sv, &pc, 11, &spub), (int)MOQ_OK);
    raw_run(c, sv, 12);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t cpub = ev.u.publish_request.pub;
    moq_accept_publish_cfg_t apc;
    moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(c, cpub, &apc, 13), (int)MOQ_OK);
    raw_run(c, sv, 14);
    drain(c); drain(sv);
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 1; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(sv, spub, &sg, 15, &sgh), (int)MOQ_OK);
    moq_rcbuf_t *pl = mk_payload("o");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(sv, sgh, 0, pl, 15), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(sv, sgh, 15), (int)MOQ_OK);
    raw_run(c, sv, 16);
    drain(c); drain(sv);
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.fill = &fill;
    loc = abs_start(0, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cpub, &up, 17), (int)MOQ_OK);
    raw_run(c, sv, 18);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, &ev), 1);
    moq_fill_t pfill = ev.u.fill_request.fill;
    moq_event_cleanup(&ev);
    drain(c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, pfill, 19), (int)MOQ_OK);
    raw_run(c, sv, 20);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    fc.stream_count = 1;   /* the subgroup; the session adds its fill */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(sv, spub, &fc, 21), (int)MOQ_OK);
    n = poll_actions_into(sv, acts, 8);
    reset = first_index_of(acts, n, MOQ_ACTION_RESET_DATA);
    done = first_index_of(acts, n, MOQ_ACTION_SEND_BIDI_STREAM);
    MOQ_TEST_CHECK(reset >= 0);
    MOQ_TEST_CHECK(done >= 0);
    MOQ_TEST_CHECK(reset < done);
    for (size_t i = 0; i < n; i++) { deliver(c, &acts[i], true, 22); moq_action_cleanup(&acts[i]); }
    raw_run(c, sv, 23);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, MOQ_EVENT_FILL_TERMINAL), 1);
    failures += recover_raw_fill_done(c, MOQ_SUBSCRIPTION_INVALID, cpub, 2, 0);
    pull_all(c, &b);
    ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_PUBLISH_FINISHED, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    bag_free(&b);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    return failures;
}

/* -- A parent ended by PUBLISH_DONE names that cause on both sides. ------- */

/* The publisher never opens the fill and ends the subscription: the child
 * ends PARENT_TERMINATED with the done status in the PARENT_DONE domain on
 * the publisher and on the subscriber, for both parent families. */
static int test_parent_done_cause_on_both_sides(void)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair(&c, &sv, 0, 0, 0)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, &fill, &ch, &sh));
    drain(c); drain(sv);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0x3; dc.stream_count = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 6), (int)MOQ_OK);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_PARENT_DONE);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x3);
    moq_event_cleanup(&ev);
    raw_run(c, sv, 7);
    bag_t b;
    pull_all(c, &b);
    const moq_event_t *ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_SUBSCRIBE_DONE, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) {
        MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
        MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_PARENT_DONE);
        MOQ_TEST_CHECK_EQ_U64(ft->u.fill_terminal.error_code, 0x3);
    }
    bag_free(&b);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);

    /* The publication family. */
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("w");
    moq_publication_t spub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(sv, &pc, 11, &spub), (int)MOQ_OK);
    raw_run(c, sv, 12);
    MOQ_TEST_CHECK(take(c, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t cpub = ev.u.publish_request.pub;
    moq_accept_publish_cfg_t apc;
    moq_accept_publish_cfg_init(&apc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(c, cpub, &apc, 13), (int)MOQ_OK);
    raw_run(c, sv, 14);
    drain(c); drain(sv);
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 1; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(sv, spub, &sg, 15, &sgh), (int)MOQ_OK);
    moq_rcbuf_t *pl = mk_payload("o");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(sv, sgh, 0, pl, 15), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(sv, sgh, 15), (int)MOQ_OK);
    raw_run(c, sv, 16);
    drain(c); drain(sv);
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    loc = abs_start(0, 0);
    up.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(c, cpub, &up, 17), (int)MOQ_OK);
    raw_run(c, sv, 18);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_REQUEST, NULL), 1);
    drain(c);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    fc.status_code = 0x4;
    fc.stream_count = 1;   /* the subgroup; the fill was never opened */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(sv, spub, &fc, 19), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(count_kind(sv, MOQ_EVENT_FILL_TERMINAL, &ev), 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_PARENT_DONE);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x4);
    moq_event_cleanup(&ev);
    raw_run(c, sv, 20);
    pull_all(c, &b);
    ft = NULL;
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_PUBLISH_FINISHED, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(bag_count(&b, MOQ_EVENT_FILL_TERMINAL, &ft), 1);
    if (ft) {
        MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.reason, (int)MOQ_FILL_PARENT_TERMINATED);
        MOQ_TEST_CHECK_EQ_INT((int)ft->u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_PARENT_DONE);
        MOQ_TEST_CHECK_EQ_U64(ft->u.fill_terminal.error_code, 0x4);
    }
    bag_free(&b);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

/* -- The unknown stream count defers and surfaces unchanged on expiry. ---- */

static int test_unknown_stream_count(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_subscription_t csub, ssub;
    failures += establish(&p, NULL, true, 5, 2, &csub, &ssub);
    drain(p.c); drain(p.sv);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0;
    dc.stream_count = UINT64_MAX;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.sv, ssub, &dc, 40), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_SUBSCRIBE_DONE), 0);
    int cslot = sub_resolve_handle(p.c, csub);
    MOQ_TEST_CHECK(cslot >= 0);
    if (cslot >= 0) {
        MOQ_TEST_CHECK(p.c->subs[cslot].done_pending);
        MOQ_TEST_CHECK_EQ_U64(p.c->subs[cslot].done_stream_count, UINT64_MAX);
    }
    moq_simpair_advance_to(p.sp, moq_simpair_now_us(p.sp) + 120 * 1000 * 1000);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_SUBSCRIBE_DONE), 1);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, UINT64_MAX);
    moq_event_cleanup(&ev);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* -- The Stream Count sent is the application's plus the session's fills. - */

/* The application reports only the data streams it opened itself; the
 * session adds every fill stream it opened for the subscription, a served
 * fill and one it failed on the wire alike. The subscriber processes both
 * (the failed stream by its RESET) and finalizes on that count. The sum
 * saturates to the profile's unknown sentinel, which defers. */
static int test_stream_count_adds_session_fills(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    moq_session_t *c, *sv;
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 4, 1, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_event_t ev;
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, &fill, &ch, &sh));
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t held = ev.u.fill_request.fill;
    drain(c); drain(sv);
    /* A second fill while the slot is held: failed on the wire (one stream). */
    moq_subscription_update_cfg_t uc;
    moq_subscription_update_cfg_init_sized(&uc, sizeof(uc));
    uc.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, ch, &uc, 6), (int)MOQ_OK);
    raw_run(c, sv, 7);
    int sslot = sub_resolve_handle(sv, sh);
    MOQ_TEST_CHECK(sslot >= 0);
    if (sslot >= 0) MOQ_TEST_CHECK_EQ_U64(sv->subs[sslot].fill_streams_opened, 1);
    /* The held fill is served (a second stream). */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, held, 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(sv, held, 3, 0, "h", 8), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(sv, held, 8), (int)MOQ_OK);
    raw_run(c, sv, 9);
    if (sslot >= 0) MOQ_TEST_CHECK_EQ_U64(sv->subs[sslot].fill_streams_opened, 2);
    drain(c); drain(sv);
    MOQ_TEST_CHECK_EQ_SIZE(c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->fills.live, 0);
    int cslot = sub_resolve_handle(c, ch);
    MOQ_TEST_CHECK(cslot >= 0);
    if (cslot >= 0) MOQ_TEST_CHECK_EQ_U64(c->subs[cslot].processed_stream_count, 2);
    /* Two fills were attributed and completed; raw sender history is still
     * unobservable, so the actual wire must carry unknown rather than 2. */
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0; dc.stream_count = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 10), (int)MOQ_OK);
    raw_run(c, sv, 11);
    failures += recover_raw_fill_done(c, ch, MOQ_PUBLICATION_INVALID, 2, 0);
    int dones = count_kind(c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    MOQ_TEST_CHECK_EQ_INT(dones, 1);
    if (dones) {
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, UINT64_MAX);
        moq_event_cleanup(&ev);
    }
    drain(sv);

    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    if (!raw_pair_v_alloc(&c, &sv, MOQ_VERSION_DRAFT_22, 4, 1, 0, &alloc)) { MOQ_TEST_CHECK(false); return failures; }
    /* Preserve the finite/saturating addition oracle independently of raw
     * transport visibility; no open is inferred from these arithmetic pins. */
    MOQ_TEST_CHECK_EQ_U64(fill_stream_count(c, 0, 2), 2);
    MOQ_TEST_CHECK_EQ_U64(fill_stream_count(c, UINT64_MAX - 2, 1), UINT64_MAX - 1);
    MOQ_TEST_CHECK_EQ_U64(fill_stream_count(c, UINT64_MAX - 1, 1), UINT64_MAX);
    /* A large explicit application count plus unobservable fill history
     * remains unknown; use fresh clocks after the prior deadline. */
    {
        moq_subscribe_cfg_t scfg;
        subscribe_cfg_with_fill(&scfg, &fill);
        scfg.track_name = MOQ_BYTES_LITERAL("second");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &scfg, 12, &ch), (int)MOQ_OK);
        raw_run(c, sv, 12);
        MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        sh = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sh, &ac, 12), (int)MOQ_OK);
        raw_run(c, sv, 12);
    }
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    drain(c); drain(sv);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(sv, sfill, 12), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fill(sv, sfill, 12), (int)MOQ_OK);
    raw_run(c, sv, 13);
    drain(c); drain(sv);
    dc.stream_count = UINT64_MAX - 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 14), (int)MOQ_OK);
    raw_run(c, sv, 15);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(c, MOQ_EVENT_SUBSCRIBE_DONE), 0);
    cslot = sub_resolve_handle(c, ch);
    MOQ_TEST_CHECK(cslot >= 0);
    if (cslot >= 0) {
        MOQ_TEST_CHECK(c->subs[cslot].done_pending);
        MOQ_TEST_CHECK_EQ_U64(c->subs[cslot].done_stream_count, UINT64_MAX);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_U64(ca.live, 0);
    return failures;
}

/* -- A locally stopped subgroup stream counts once its RESET arrives. ----- */

/* The subscriber's receive budget stops a subgroup stream it had bound. The
 * STOP alone counts nothing; the publisher's RESET answering it counts the
 * stream as processed, so a done naming that one stream finalizes at once
 * rather than at the terminal-wait deadline. The same on every profile. */
static int stopped_subgroup_counts(moq_version_t version)
{
    int failures = 0;
    moq_session_t *c, *sv;
    if (!raw_pair_v(&c, &sv, version, 0, 0, 16)) { MOQ_TEST_CHECK(false); return failures; }
    moq_subscription_t ch, sh;
    MOQ_TEST_CHECK(raw_establish(c, sv, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, NULL, &ch, &sh));
    drain(c); drain(sv);
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 0; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t sgh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(sv, sh, &sg, 6, &sgh), (int)MOQ_OK);
    uint8_t big[64];
    memset(big, 'x', sizeof(big));
    moq_rcbuf_t *pl = NULL;
    moq_rcbuf_create(moq_alloc_default(), big, sizeof(big), &pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(sv, sgh, 0, pl, 6), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    raw_pump(sv, c, true, 7, 0);
    MOQ_TEST_CHECK_EQ_INT(raw_pump(c, sv, false, 8, MOQ_ACTION_STOP_DATA), 1);
    int cslot = sub_resolve_handle(c, ch);
    MOQ_TEST_CHECK(cslot >= 0);
    if (cslot >= 0) MOQ_TEST_CHECK_EQ_U64(c->subs[cslot].processed_stream_count, 0);
    MOQ_TEST_CHECK_EQ_INT(raw_pump(sv, c, true, 9, MOQ_ACTION_RESET_DATA), 1);
    if (cslot >= 0) MOQ_TEST_CHECK_EQ_U64(c->subs[cslot].processed_stream_count, 1);
    drain(c); drain(sv);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.status_code = 0; dc.stream_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, sh, &dc, 10), (int)MOQ_OK);
    raw_run(c, sv, 11);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(c, MOQ_EVENT_SUBSCRIBE_DONE, &ev), 1);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, 1);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(c); moq_session_destroy(sv);
    return failures;
}

static int test_stopped_subgroup_counts(void)
{
    int failures = 0;
    failures += stopped_subgroup_counts(MOQ_VERSION_DRAFT_18);
    failures += stopped_subgroup_counts(MOQ_VERSION_DRAFT_22);
    return failures;
}

/* -- Allocations balance with fill objects still queued at destroy. ------- */

static int test_destroy_with_queued_objects(void)
{
    int failures = 0;
    count_alloc_t ca = {0};
    moq_alloc_t alloc = { &ca, ca_alloc, ca_realloc, ca_free };
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.version = MOQ_VERSION_DRAFT_22;
    pair_t p;
    p.sp = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &p.sp), (int)MOQ_OK);
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(p.sp), (int)MOQ_OK);
    pump(&p);
    p.c = moq_simpair_client(p.sp);
    p.sv = moq_simpair_server(p.sp);
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 5, 2, &csub, &ssub);
    drain(p.c);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 10), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 3, 0, "queued", 11), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 3, 1, "queued", 11), (int)MOQ_OK);
    pump(&p);
    /* Two fill objects sit in the subscriber's queue; destroy releases them. */
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 2);
    moq_simpair_destroy(p.sp);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live, 0);
    return failures;
}

/* -- A whole fill through three-slot queues, pumped by hand. -------------- */

/* Feed one SEND_DATA action to the subscriber, resuming a refused delivery
 * with the documented empty re-feed after draining its events. */
static void feed_data_action(moq_session_t *c, const moq_action_t *a, uint64_t now,
                             int *objs, int *terms, int *blocked, int *failures_out)
{
    int failures = 0;
    moq_stream_ref_t r = a->u.send_data.stream_ref;
    bool hp = a->u.send_data.payload != NULL;
    moq_result_t drc = MOQ_OK;
    if (a->u.send_data.header_len > 0)
        drc = moq_session_on_data_bytes(c, r, a->u.send_data.header, a->u.send_data.header_len,
                                        a->u.send_data.fin && !hp, now);
    if (hp && drc >= 0)
        drc = moq_session_on_data_bytes(c, r, moq_rcbuf_data(a->u.send_data.payload),
                                        moq_rcbuf_len(a->u.send_data.payload), a->u.send_data.fin, now);
    if (!hp && a->u.send_data.header_len == 0 && a->u.send_data.fin)
        drc = moq_session_on_data_bytes(c, r, NULL, 0, true, now);
    while (drc == MOQ_ERR_WOULD_BLOCK) {
        (*blocked)++;
        bag_t b;
        pull_all(c, &b);
        *objs += bag_count(&b, MOQ_EVENT_FILL_OBJECT, NULL);
        *terms += bag_count(&b, MOQ_EVENT_FILL_TERMINAL, NULL);
        bag_free(&b);
        drc = moq_session_on_data_bytes(c, r, NULL, 0, false, now);
    }
    MOQ_TEST_CHECK_EQ_INT((int)drc, (int)MOQ_OK);
    *failures_out += failures;
}

static int test_tiny_queue_whole_flow(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair_cfg(&p, MOQ_VERSION_DRAFT_22, 3, 3)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc = abs_start(3, 0);
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 5, 2, &csub, &ssub);
    moq_event_t ev;
    MOQ_TEST_CHECK_EQ_INT(count_kind(p.c, MOQ_EVENT_FILL_CREATED, NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(count_kind(p.sv, MOQ_EVENT_FILL_REQUEST, &ev), 1);
    moq_fill_t sfill = ev.u.fill_request.fill;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 10), (int)MOQ_OK);
    int objs = 0, terms = 0, blocked_writes = 0, blocked_deliveries = 0;
    moq_action_t a;
    /* Every refused write is retried once the publisher's actions are
     * delivered by hand; every refused delivery resumes by re-feed. */
    for (uint64_t o = 0; o <= 4; o++) {
        moq_result_t wr;
        for (;;) {
            wr = o < 4 ? write_fill(p.sv, sfill, 3, o, "p", 11) : moq_session_end_fill(p.sv, sfill, 12);
            if (wr != MOQ_ERR_WOULD_BLOCK) break;
            blocked_writes++;
            while (moq_session_poll_actions(p.sv, &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_SEND_DATA)
                    feed_data_action(p.c, &a, 13, &objs, &terms, &blocked_deliveries, &failures);
                moq_action_cleanup(&a);
            }
            drain(p.sv);
        }
        MOQ_TEST_CHECK_EQ_INT((int)wr, (int)MOQ_OK);
    }
    while (moq_session_poll_actions(p.sv, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA)
            feed_data_action(p.c, &a, 14, &objs, &terms, &blocked_deliveries, &failures);
        moq_action_cleanup(&a);
    }
    bag_t b;
    pull_all(p.c, &b);
    objs += bag_count(&b, MOQ_EVENT_FILL_OBJECT, NULL);
    terms += bag_count(&b, MOQ_EVENT_FILL_TERMINAL, NULL);
    bag_free(&b);
    MOQ_TEST_CHECK(blocked_writes > 0);
    MOQ_TEST_CHECK(blocked_deliveries > 0);
    MOQ_TEST_CHECK_EQ_INT(objs, 4);
    MOQ_TEST_CHECK_EQ_INT(terms, 1);
    MOQ_TEST_CHECK_EQ_INT(count_kind(p.sv, MOQ_EVENT_FILL_TERMINAL, NULL), 1);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

/* A fill object larger than the subscriber's max_object_payload_size is a
 * local resource refusal, not a peer error. §3.4.1: "A subscriber can cancel
 * a fill fetch stream independently using STOP_SENDING ... Resetting or
 * cancelling a fill fetch stream, by either endpoint, does not affect the
 * subscription". The subscriber stops that fill stream with CANCELLED (§12.5),
 * the child ends LOCAL_CANCELLED, nothing is delivered for the object, the
 * publisher's child ends on the STOP's code, and both sessions and the
 * subscription stay up. */
static int test_fill_oversized_object(void)
{
    int failures = 0;
    pair_t p;
    if (!mk_pair(&p)) { MOQ_TEST_CHECK(false); return failures; }
    drain(p.c); drain(p.sv);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 1; loc.start_object = 0;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub, ssub;
    failures += establish(&p, &fill, true, 3, 7, &csub, &ssub);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    moq_fill_t sfill = ev.u.fill_request.fill;
    p.c->max_obj_payload = 4;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_fill(p.sv, sfill, 1, 0, "too long", 4), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.c, MOQ_EVENT_FILL_OBJECT), 0);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, cfill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_LOCAL_CANCELLED);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_TERMINAL, &ev));
    MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, sfill));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.reason, (int)MOQ_FILL_PEER_CANCELLED_OR_RESET);
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.fill_terminal.error_domain, (int)MOQ_FILL_ERROR_STREAM);
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x1);   /* CANCELLED */
    MOQ_TEST_CHECK_EQ_INT((int)p.c->subs[sub_resolve_handle(p.c, csub)].state,
                          (int)MOQ_SUB_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)p.sv->subs[sub_resolve_handle(p.sv, ssub)].state,
                          (int)MOQ_SUB_ESTABLISHED);
    moq_simpair_destroy(p.sp);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_fill_round_trip();
    failures += test_fill_oversized_object();
    failures += test_overlapping_fills_and_parent_cancel(0);
    failures += test_overlapping_fills_and_parent_cancel(2);
    failures += test_fill_failure_leaves_parent_alive();
    failures += test_subscriber_cancels_one_fill();
    failures += test_no_stream_outcomes();
    failures += test_update_without_fill_opens_nothing();
    failures += test_pool_exhaustion_and_retry();
    failures += test_publisher_pool_full();
    failures += test_tiny_queues();
    failures += test_short_poll_keeps_ownership();
    failures += test_publish_initiated_fill();
    failures += test_stream_count_includes_fills();
    failures += test_draft18_refuses_fill();
    failures += test_unsubscribe_before_open();
    failures += test_parent_rejected();
    failures += test_cancel_before_header();
    failures += test_header_after_no_stream();
    failures += test_done_before_fill_data();
    failures += test_budget_stop_fill_stream();
    failures += test_update_filter_inheriting_fill();
    failures += test_done_resets_fills_before_terminal();
    failures += test_parent_done_cause_on_both_sides();
    failures += test_unknown_stream_count();
    failures += test_stream_count_adds_session_fills();
    failures += test_stopped_subgroup_counts();
    failures += test_destroy_with_queued_objects();
    failures += test_tiny_queue_whole_flow();
    if (failures) { fprintf(stderr, "FAIL: test_fill_session (%d)\n", failures); return 1; }
    printf("PASS: test_fill_session\n");
    return 0;
}
