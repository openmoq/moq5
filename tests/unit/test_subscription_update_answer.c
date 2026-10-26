/*
 * Subscription updates answered by the application
 * (moq_session_cfg_t.defer_subscription_updates), on drafts 16, 18 and 22.
 *
 * Pinned on every profile: an update surfaces with answer_owed and nothing
 * is answered or applied until the application decides; accepting applies
 * forward state and priority and sends one REQUEST_OK; rejecting sends
 * REQUEST_ERROR with the application's code and reason followed by
 * PUBLISH_DONE(UPDATE_FAILED), and the subscriber ends the subscription with
 * that status; an answer naming a superseded update is refused without
 * effect; further updates before the answer are coalesced on profiles that
 * advertise no bound (one REQUEST_OK per update on accept; on reject one
 * REQUEST_ERROR for the run on draft-18 and one per update, each by its id,
 * on draft-16) and close the session with TOO_MANY_REQUEST_UPDATES on
 * draft-22, which advertises a bound of one; an update the session refuses
 * itself while an answer is owed fails the whole run the same way; a
 * subscriber cancel or a session close while an answer is owed drops it
 * without any answer and releases its storage; the publisher cannot end the
 * subscription or send a per-request GOAWAY while an answer is owed; a
 * WOULD_BLOCK from either answer leaves the subscription, its data streams
 * and the owed answer exactly as they were; with deferral off an update is
 * answered at once, as it always was. On draft-22 a fill and a replacement
 * filter carried on the update take effect only on accept, and a rejected
 * update's fill ends on the subscriber with the application's code.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/wire/control_d22_internal.h"
#include <stdlib.h>
#include <string.h>

/* -- Byte-balanced allocator: every free names the size it allocated. ---- */

typedef struct { int64_t bytes; int64_t blocks; } bal_t;
typedef struct { size_t size; } bal_hdr_t;
#define BAL_HDR 16u

static void *bal_alloc(size_t size, void *ctx)
{
    bal_t *b = (bal_t *)ctx;
    uint8_t *p = (uint8_t *)malloc(size + BAL_HDR);
    if (!p) return NULL;
    ((bal_hdr_t *)p)->size = size;
    b->bytes += (int64_t)size;
    b->blocks++;
    return p + BAL_HDR;
}

static void *bal_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    bal_t *b = (bal_t *)ctx;
    if (!ptr) return bal_alloc(new_size, ctx);
    uint8_t *base = (uint8_t *)ptr - BAL_HDR;
    if (((bal_hdr_t *)base)->size != old_size) b->bytes += 1u << 30;  /* mismatch */
    uint8_t *n = (uint8_t *)realloc(base, new_size + BAL_HDR);
    if (!n) return NULL;
    ((bal_hdr_t *)n)->size = new_size;
    b->bytes += (int64_t)new_size - (int64_t)old_size;
    return n + BAL_HDR;
}

static void bal_free(void *ptr, size_t size, void *ctx)
{
    bal_t *b = (bal_t *)ctx;
    if (!ptr) return;
    uint8_t *base = (uint8_t *)ptr - BAL_HDR;
    if (((bal_hdr_t *)base)->size != size) b->bytes += 1u << 30;     /* mismatch */
    b->bytes -= (int64_t)size;
    b->blocks--;
    free(base);
}

/* -- Wire log: every message the publisher sent, decoded by profile. ----- */

#define TYPE_REQUEST_OK    0x07u
#define TYPE_REQUEST_ERROR 0x05u
#define TYPE_PUBLISH_DONE  0x0Bu
#define TYPE_CLOSE         0xC105Eu   /* a CLOSE_SESSION action, code in `code` */

typedef struct {
    uint64_t type;
    uint64_t id;        /* REQUEST_OK / REQUEST_ERROR request id (draft-16) */
    uint64_t code;      /* REQUEST_ERROR error code, PUBLISH_DONE status, close code */
    size_t   reason_len;
    bool     fin;
} msg_t;

typedef struct {
    moq_version_t v;
    msg_t m[64];
    size_t n;
} wire_log_t;

static void log_messages(wire_log_t *l, const uint8_t *data, size_t len, bool fin)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    while (r.pos < r.len && l->n < 64) {
        moq_control_envelope_t env;
        moq_result_t rc;
        if (l->v == MOQ_VERSION_DRAFT_16) rc = moq_control_decode_envelope(&r, &env);
        else if (l->v == MOQ_VERSION_DRAFT_18) rc = moq_d18_decode_envelope(&r, &env);
        else rc = moq_d22_decode_envelope(&r, &env);
        if (rc != MOQ_OK) return;
        msg_t *m = &l->m[l->n++];
        memset(m, 0, sizeof(*m));
        m->type = env.msg_type;
        m->fin = fin && r.pos == r.len;
        if (l->v == MOQ_VERSION_DRAFT_16) {
            if (env.msg_type == TYPE_REQUEST_OK) {
                moq_kvp_entry_t params[8];
                moq_d16_request_ok_t ok = { .params = params, .params_cap = 8 };
                if (moq_d16_decode_request_ok(env.payload, env.payload_len, &ok) == MOQ_OK)
                    m->id = ok.request_id;
            } else if (env.msg_type == TYPE_REQUEST_ERROR) {
                moq_d16_request_error_t e;
                if (moq_d16_decode_request_error(env.payload, env.payload_len, &e) == MOQ_OK) {
                    m->id = e.request_id; m->code = e.error_code; m->reason_len = e.reason_len;
                }
            } else if (env.msg_type == TYPE_PUBLISH_DONE) {
                moq_d16_publish_done_t d;
                if (moq_d16_decode_publish_done(env.payload, env.payload_len, &d) == MOQ_OK)
                    m->code = d.status_code;
            }
        } else {
            /* Draft-18 and draft-22 share these two bodies. */
            if (env.msg_type == TYPE_REQUEST_ERROR) {
                moq_d18_request_error_t e;
                if (moq_d18_decode_request_error(env.payload, env.payload_len, &e) == MOQ_OK) {
                    m->code = e.error_code; m->reason_len = e.reason.len;
                }
            } else if (env.msg_type == TYPE_PUBLISH_DONE) {
                moq_d18_publish_done_t d;
                if (moq_d18_decode_publish_done(env.payload, env.payload_len, &d) == MOQ_OK)
                    m->code = d.status_code;
            }
        }
    }
}

static wire_log_t g_log;

static void trace_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    wire_log_t *l = (wire_log_t *)ctx;
    if (r->kind != MOQ_SIM_TRACE_ACTION || r->from != MOQ_PERSPECTIVE_SERVER) return;
    if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
        log_messages(l, r->bytes.data, r->bytes.len, false);
    else if (r->action_kind == MOQ_ACTION_SEND_BIDI_STREAM)
        log_messages(l, r->detail_bytes.data, r->detail_bytes.len, r->fin);
    else if (r->action_kind == MOQ_ACTION_CLOSE_SESSION && l->n < 64) {
        msg_t *m = &l->m[l->n++];
        memset(m, 0, sizeof(*m));
        m->type = TYPE_CLOSE;
        m->code = r->code;
    }
}

static size_t count_type(uint64_t type)
{
    size_t n = 0;
    for (size_t i = 0; i < g_log.n; i++) if (g_log.m[i].type == type) n++;
    return n;
}

static const msg_t *nth_type(uint64_t type, size_t nth)
{
    for (size_t i = 0; i < g_log.n; i++)
        if (g_log.m[i].type == type && nth-- == 0) return &g_log.m[i];
    return NULL;
}

/* -- Pair ---------------------------------------------------------------- */

/* Log what the publisher has queued but not yet delivered. */
static void log_queued(const moq_session_t *s)
{
    for (size_t i = s->action_head; i < s->action_tail; i++) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == MOQ_ACTION_SEND_CONTROL)
            log_messages(&g_log, a->u.send_control.data, a->u.send_control.len, false);
        else if (a->kind == MOQ_ACTION_SEND_BIDI_STREAM)
            log_messages(&g_log, a->u.send_bidi_stream.data, a->u.send_bidi_stream.len,
                         a->u.send_bidi_stream.fin);
    }
}

typedef struct {
    moq_simpair_t *sp;
    moq_session_t *c, *sv;
    moq_subscription_t csub, ssub;
    moq_version_t v;
    bal_t bal;
} pair_t;

static void pump(pair_t *p) { moq_simpair_run_until_quiescent(p->sp, 64, NULL); }

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

static size_t queued_events(const moq_session_t *s, moq_event_kind_t kind)
{
    size_t n = 0;
    for (size_t i = s->event_head; i < s->event_tail; i++)
        if (s->events[i % s->event_cap].kind == kind) n++;
    return n;
}

static size_t queued_actions(const moq_session_t *s)
{
    return s->action_tail - s->action_head;
}

static moq_sub_entry_t *sentry(pair_t *p)
{
    int slot = sub_resolve_handle(p->sv, p->ssub);
    return slot < 0 ? NULL : &p->sv->subs[slot];
}

/* A subscription from the client to the server, accepted, with the server
 * answering updates itself or leaving them to the test. */
static bool mk(pair_t *p, moq_version_t v, uint32_t max_actions, bool defer)
{
    memset(p, 0, sizeof(*p));
    memset(&g_log, 0, sizeof(g_log));
    g_log.v = v;
    p->v = v;
    static moq_alloc_t alloc;
    alloc = (moq_alloc_t){ &p->bal, bal_alloc, bal_realloc, bal_free };
    moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
    scfg.alloc = &alloc;
    scfg.version = v;
    scfg.max_actions = max_actions;
    scfg.client_send_request_capacity = true; scfg.client_initial_request_capacity = 64;
    scfg.server_send_request_capacity = true; scfg.server_initial_request_capacity = 64;
    scfg.trace_fn = trace_fn;
    scfg.trace_ctx = &g_log;
    scfg.auth_token_cache_size = 16;
    scfg.server_defer_subscription_updates = defer;
    if (moq_simpair_create(&scfg, &p->sp) < 0) return false;
    if (moq_simpair_start(p->sp) < 0) return false;
    pump(p);
    p->c = moq_simpair_client(p->sp);
    p->sv = moq_simpair_server(p->sp);
    drain(p->c); drain(p->sv);

    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    if (moq_session_subscribe(p->c, &cfg, 1, &p->csub) != MOQ_OK) return false;
    pump(p);
    moq_event_t ev;
    if (!take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    p->ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
    if (moq_session_accept_subscribe(p->sv, p->ssub, &ac, 2) != MOQ_OK) return false;
    pump(p);
    drain(p->c); drain(p->sv);
    memset(g_log.m, 0, sizeof(g_log.m));
    g_log.n = 0;
    return sentry(p) != NULL;
}

/* Destroy the pair; every byte the sessions allocated was freed with the
 * size it was allocated with. */
static int finish(pair_t *p)
{
    int failures = 0;
    moq_simpair_destroy(p->sp);
    MOQ_TEST_CHECK_EQ_INT((int)p->bal.bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)p->bal.blocks, 0);
    return failures;
}

static moq_result_t send_update(pair_t *p, bool has_forward, bool forward,
                                bool has_priority, uint8_t priority, uint64_t now)
{
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.has_forward = has_forward; up.forward = forward;
    up.has_subscriber_priority = has_priority; up.subscriber_priority = priority;
    return moq_session_update_subscription(p->c, p->csub, &up, now);
}

/* Send a second update while the first is still unacknowledged: the client
 * API holds one at a time, so its pending marker is lifted by hand. The
 * server-side behaviour under test is what the wire then carries. */
static moq_result_t send_pipelined(pair_t *p, bool has_forward, bool forward,
                                   bool has_priority, uint8_t priority, uint64_t now)
{
    int cs = sub_resolve_handle(p->c, p->csub);
    if (cs < 0) return MOQ_ERR_STALE_HANDLE;
    p->c->subs[cs].update_pending = false;
    return send_update(p, has_forward, forward, has_priority, priority, now);
}

static moq_result_t accept_upd(pair_t *p, uint64_t id, uint64_t now)
{
    moq_accept_subscription_update_cfg_t cfg;
    moq_accept_subscription_update_cfg_init(&cfg);
    cfg.update_request_id = id;
    return moq_session_accept_subscription_update(p->sv, p->ssub, &cfg, now);
}

static moq_result_t reject_upd(pair_t *p, uint64_t id, uint64_t code, const char *reason,
                               uint64_t now)
{
    moq_reject_subscription_update_cfg_t cfg;
    moq_reject_subscription_update_cfg_init(&cfg);
    cfg.update_request_id = id;
    cfg.error_code = (moq_request_error_t)code;
    if (reason) cfg.reason = (moq_bytes_t){ (const uint8_t *)reason, strlen(reason) };
    return moq_session_reject_subscription_update(p->sv, p->ssub, &cfg, now);
}

/* -- Accept --------------------------------------------------------------- */

static int test_accept(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    moq_sub_entry_t *e = sentry(&p);
    uint8_t prio0 = e->subscriber_priority;
    MOQ_TEST_CHECK(e->forward);

    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, true, 7, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.detail_size >= sizeof(moq_subscribe_updated_event_t));
    MOQ_TEST_CHECK(ev.u.subscribe_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.update_count, 1);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_forward && !ev.u.subscribe_updated.forward);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.subscriber_priority, 7);
    MOQ_TEST_CHECK(!ev.u.subscribe_updated.fill_requested);
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);

    /* Nothing answered, nothing applied. */
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
    MOQ_TEST_CHECK(!take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
    e = sentry(&p);
    MOQ_TEST_CHECK(e->forward);
    MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, prio0);

    /* An answer naming another update is stale and changes nothing. */
    size_t q0 = queued_actions(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id + 2, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id + 2, 0x5, NULL, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), q0);
    MOQ_TEST_CHECK(sentry(&p) && sentry(&p)->owed_update.owed && sentry(&p)->forward);

    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 5), (int)MOQ_OK);
    e = sentry(&p);
    MOQ_TEST_CHECK(!e->forward);
    MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, 7);
    MOQ_TEST_CHECK(!e->owed_update.owed);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_ERR_WRONG_STATE);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
    if (v == MOQ_VERSION_DRAFT_16 && nth_type(TYPE_REQUEST_OK, 0))
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_OK, 0)->id, id);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    int cs = sub_resolve_handle(p.c, p.csub);
    MOQ_TEST_CHECK(cs >= 0 && !p.c->subs[cs].update_pending && !p.c->subs[cs].forward);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

/* -- Reject --------------------------------------------------------------- */

static int test_reject(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);

    /* Arguments are checked before anything else. */
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, MOQ_REQUEST_ERROR_REDIRECT, NULL, 4),
                          (int)MOQ_ERR_INVAL);
    moq_reject_subscription_update_cfg_t big;
    moq_reject_subscription_update_cfg_init(&big);
    static uint8_t long_reason[1025];
    big.update_request_id = id;
    big.reason = (moq_bytes_t){ long_reason, sizeof(long_reason) };
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscription_update(p.sv, p.ssub, &big, 4),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(sentry(&p) && sentry(&p)->owed_update.owed);

    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, "nope", 5), (int)MOQ_OK);
    MOQ_TEST_CHECK(sentry(&p) == NULL);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 6), (int)MOQ_ERR_STALE_HANDLE);
    pump(&p);

    /* REQUEST_ERROR with the application's code and reason, then the
     * terminal PUBLISH_DONE(UPDATE_FAILED), and no acknowledgement. */
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
    size_t ie = 64, id_ = 64;
    for (size_t i = 0; i < g_log.n; i++) {
        if (g_log.m[i].type == TYPE_REQUEST_ERROR) ie = i;
        if (g_log.m[i].type == TYPE_PUBLISH_DONE) id_ = i;
    }
    MOQ_TEST_CHECK(ie < id_ && id_ < 64);
    if (ie < 64) {
        MOQ_TEST_CHECK_EQ_U64(g_log.m[ie].code, 0x5);
        MOQ_TEST_CHECK_EQ_SIZE(g_log.m[ie].reason_len, 4);
        if (v == MOQ_VERSION_DRAFT_16) MOQ_TEST_CHECK_EQ_U64(g_log.m[ie].id, id);
    }
    if (id_ < 64) {
        MOQ_TEST_CHECK_EQ_U64(g_log.m[id_].code, 0x8);
        if (v != MOQ_VERSION_DRAFT_16) MOQ_TEST_CHECK(g_log.m[id_].fin);
    }

    /* The subscriber took both in order -- the failed update, then the
     * terminal -- and ends the subscription with UPDATE_FAILED without ever
     * seeing the update acknowledged. */
    size_t oks = 0, dones = 0;
    while (moq_session_poll_events(p.c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) oks++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            dones++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x8);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_SIZE(oks, 0);
    MOQ_TEST_CHECK_EQ_SIZE(dones, 1);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, p.csub) < 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

/* -- Deferral off: answered at once, as before ----------------------------- */

static int test_not_deferred(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, false));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    MOQ_TEST_CHECK(!ev.u.subscribe_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.update_count, 1);
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(!sentry(&p)->owed_update.owed);
    MOQ_TEST_CHECK(!sentry(&p)->forward);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 4), (int)MOQ_ERR_WRONG_STATE);
    failures += finish(&p);
    return failures;
}

/* Facade ownership must not change the raw session migration policy. */
static int test_goaway_defer_policy(moq_version_t v, bool defer)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, defer));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK(send_update(&p, true, false, false, 0, 3) == MOQ_OK);
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    moq_result_t rc = moq_session_request_goaway_subscribe(p.sv, p.ssub, &gc, 3);
    MOQ_TEST_CHECK(rc == (v == MOQ_VERSION_DRAFT_16 ? MOQ_ERR_INVAL : MOQ_OK));
    /* The mock peer retains the old request; do not deliver GOAWAY to the
     * stock peer, which would immediately FIN it before we observe the ACK. */
    moq_action_t action;
    unsigned goaways = 0;
    while (moq_session_poll_actions(p.sv, &action, 1)) {
        goaways++;
        MOQ_TEST_CHECK(action.kind == MOQ_ACTION_SEND_BIDI_STREAM);
        if (action.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            MOQ_TEST_CHECK(!action.u.send_bidi_stream.fin);
            MOQ_TEST_CHECK(action.u.send_bidi_stream.len > 0 &&
                           action.u.send_bidi_stream.data[0] == 0x10);
        }
        moq_action_cleanup(&action);
    }
    MOQ_TEST_CHECK_EQ_INT(goaways, v == MOQ_VERSION_DRAFT_16 ? 0 : 1);
    pump(&p);
    moq_event_t ev;
    bool found = take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev);
    MOQ_TEST_CHECK(found);
    if (!found) return failures + finish(&p);
    bool owed = defer && v == MOQ_VERSION_DRAFT_16;
    MOQ_TEST_CHECK(ev.u.subscribe_updated.answer_owed == owed);
    uint64_t id = ev.u.subscribe_updated.update_request_id, queried = 0;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_subscription_owed_update(p.sv, p.ssub, &queried,
        NULL, NULL) == (owed ? MOQ_OK : MOQ_ERR_WRONG_STATE));
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), owed ? 0 : 1);
    if (owed) {
        MOQ_TEST_CHECK_EQ_U64(queried, id);
        MOQ_TEST_CHECK(accept_upd(&p, id, 4) == MOQ_OK);
        pump(&p);
    }
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    MOQ_TEST_PASS("raw_subscription_goaway_defer_policy");
    return failures;
}

/* -- Two updates before the answer ---------------------------------------- */

static int test_coalesced(moq_version_t v, bool accept)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)send_pipelined(&p, false, false, true, 9, 3), (int)MOQ_OK);
    pump(&p);

    moq_event_t ev;
    if (v == MOQ_VERSION_DRAFT_22) {
        /* MAX_REQUEST_UPDATES is 1: the second unanswered update closes the
         * session with TOO_MANY_REQUEST_UPDATES and nothing is answered. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_CLOSE), 1);
        if (nth_type(TYPE_CLOSE, 0)) MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_CLOSE, 0)->code, 0x1b);
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
        failures += finish(&p);
        return failures;
    }

    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.update_count, 1);
    uint64_t id1 = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.u.subscribe_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.update_count, 2);
    /* Cumulative: the first update's forward change and the second's priority. */
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_forward && !ev.u.subscribe_updated.forward);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.subscriber_priority, 9);
    uint64_t id2 = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(id1 != id2);
    MOQ_TEST_CHECK(sentry(&p)->forward);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK) + count_type(TYPE_REQUEST_ERROR), 0);

    /* The superseded update cannot be answered on its own. */
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id1, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id1, 0x5, NULL, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(sentry(&p)->owed_update.owed);

    /* The answer goes to the publisher's wire only; the client's own update
     * bookkeeping was bypassed above, so nothing is delivered back. */
    if (accept) {
        MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id2, 5), (int)MOQ_OK);
        moq_sub_entry_t *e = sentry(&p);
        MOQ_TEST_CHECK(!e->forward);
        MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, 9);
        MOQ_TEST_CHECK(!e->owed_update.owed && e->owed_update.prior_ids == NULL);
    } else {
        MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id2, 0x5, NULL, 5), (int)MOQ_OK);
        MOQ_TEST_CHECK(sentry(&p) == NULL);
    }
    log_queued(p.sv);
    if (accept) {
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 2);
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
        if (v == MOQ_VERSION_DRAFT_16 && count_type(TYPE_REQUEST_OK) == 2) {
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_OK, 0)->id, id1);
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_OK, 1)->id, id2);
        }
    } else {
        /* Draft-18 answers the failed run with one REQUEST_ERROR; draft-16,
         * which has no coalesced error, answers each update by its id. */
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR),
                               v == MOQ_VERSION_DRAFT_16 ? 2 : 1);
        MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
        if (v == MOQ_VERSION_DRAFT_16 && count_type(TYPE_REQUEST_ERROR) == 2) {
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 0)->id, id1);
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 1)->id, id2);
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 0)->code, 0x5);
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 1)->code, 0x5);
        }
        MOQ_TEST_CHECK(g_log.n > 0 && g_log.m[g_log.n - 1].type == TYPE_PUBLISH_DONE);
    }
    failures += finish(&p);
    return failures;
}

/* -- An update the session refuses itself while an answer is owed -------- */

/* A further update carrying an authorization token the session rejects
 * (DELETE of an alias never registered) fails the whole run: the owed
 * update is answered by the failure, never by the application. */
static int test_refused_while_owed(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    uint64_t id1 = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);
    moq_sub_entry_t *e = sentry(&p);
    MOQ_TEST_CHECK(e && e->owed_update.owed);
    if (!e) { failures += finish(&p); return failures; }

    /* The subscriber's next request id. */
    uint64_t id2 = id1 + 2;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc;
    if (v == MOQ_VERSION_DRAFT_16) {
        static const uint8_t del_alias[] = { 0x00, 0x09 };   /* DELETE alias 9 */
        moq_kvp_entry_t tok = { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                                .value = del_alias, .value_len = sizeof(del_alias) };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d16_encode_request_update(&w, id2, e->request_id, &tok, 1),
                              (int)MOQ_OK);
        rc = moq_session_on_control_bytes(p.sv, buf, moq_buf_writer_offset(&w), 4);
    } else {
        moq_d18_msg_params_t prm;
        memset(&prm, 0, sizeof(prm));
        prm.auth_token_count = 1;
        prm.auth_tokens[0].alias_type = 0;   /* DELETE */
        prm.auth_tokens[0].alias = 9;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_update(&w, id2, &prm), (int)MOQ_OK);
        rc = moq_session_on_bidi_stream_bytes(p.sv, e->rs.ref, buf, moq_buf_writer_offset(&w),
                                              false, 4);
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(sentry(&p) == NULL);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id1, 5), (int)MOQ_ERR_STALE_HANDLE);
    log_queued(p.sv);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), v == MOQ_VERSION_DRAFT_16 ? 2 : 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
    for (size_t i = 0; i < count_type(TYPE_REQUEST_ERROR); i++)
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, i)->code, 0x17);
    if (v == MOQ_VERSION_DRAFT_16 && count_type(TYPE_REQUEST_ERROR) == 2) {
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 0)->id, id1);
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 1)->id, id2);
    }
    MOQ_TEST_CHECK(g_log.n > 0 && g_log.m[g_log.n - 1].type == TYPE_PUBLISH_DONE);
    failures += finish(&p);
    return failures;
}

/* -- The subscription ends while an answer is owed ------------------------- */

static int test_cancel_while_owed(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    if (v != MOQ_VERSION_DRAFT_22)   /* draft-22 refuses a second outstanding update */
        MOQ_TEST_CHECK_EQ_INT((int)send_pipelined(&p, false, false, true, 9, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    uint64_t id = 0;
    while (take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev)) {
        id = ev.u.subscribe_updated.update_request_id;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(sentry(&p) && sentry(&p)->owed_update.owed);

    /* The publisher may neither end the subscription nor migrate it first. */
    moq_done_subscribe_cfg_t dc; moq_done_subscribe_cfg_init(&dc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.sv, p.ssub, &dc, 4),
                          (int)MOQ_ERR_WRONG_STATE);
    moq_request_goaway_cfg_t gc; moq_request_goaway_cfg_init(&gc);
    moq_result_t grc = moq_session_request_goaway_subscribe(p.sv, p.ssub, &gc, 4);
    MOQ_TEST_CHECK_EQ_INT((int)grc, v == MOQ_VERSION_DRAFT_16 ? (int)MOQ_ERR_INVAL
                                                             : (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(sentry(&p) && sentry(&p)->owed_update.owed);

    /* The subscriber cancels: the owed answer goes with the subscription. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, p.csub, 5), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_UNSUBSCRIBED, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(sentry(&p) == NULL);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 6), (int)MOQ_ERR_STALE_HANDLE);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);

    /* The next subscription takes the freed slot with nothing owed. */
    int old_slot = -1;
    for (size_t i = 0; i < p.sv->sub_cap; i++)
        if (p.sv->subs[i].owed_update.owed || p.sv->subs[i].owed_update.prior_ids) old_slot = (int)i;
    MOQ_TEST_CHECK_EQ_INT(old_slot, -1);
    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &cfg, 7, &p.csub), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    p.ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(p.sv, p.ssub, &ac, 8), (int)MOQ_OK);
    MOQ_TEST_CHECK(sentry(&p) && !sentry(&p)->owed_update.owed);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 8), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(p.sv, p.ssub, &dc, 8), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

static int test_close_while_owed(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    if (v != MOQ_VERSION_DRAFT_22)
        MOQ_TEST_CHECK_EQ_INT((int)send_pipelined(&p, false, false, true, 9, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    uint64_t id = 0;
    while (take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev)) {
        id = ev.u.subscribe_updated.update_request_id;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(sentry(&p) && sentry(&p)->owed_update.owed);
    if (v != MOQ_VERSION_DRAFT_22)
        MOQ_TEST_CHECK(sentry(&p)->owed_update.prior_ids != NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close(p.sv, 0, "bye", 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 5), (int)MOQ_ERR_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 5), (int)MOQ_ERR_CLOSED);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
    /* finish() proves the owed ids were released with the session. */
    failures += finish(&p);
    return failures;
}

/* -- WOULD_BLOCK leaves everything as it was ------------------------------ */

/* Occupy all but `free_slots` of the publisher's action queue with a data
 * stream on the subscription: a subgroup open plus objects. */
static void occupy_queue(pair_t *p, uint64_t subgroup, size_t free_slots, uint64_t now)
{
    moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
    sg.group_id = 6; sg.subgroup_id = subgroup; sg.publisher_priority = 1;
    moq_subgroup_handle_t h;
    if (moq_session_open_subgroup(p->sv, p->ssub, &sg, now, &h) != MOQ_OK) return;
    uint64_t obj = 0;
    while (action_queue_avail(p->sv) > free_slots && obj < 64) {
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"x", 1, &pl);
        moq_result_t rc = moq_session_write_object(p->sv, h, obj++, pl, now);
        moq_rcbuf_decref(pl);
        if (rc != MOQ_OK) break;
    }
}

static size_t live_subgroups(const moq_session_t *s, moq_subscription_t sub)
{
    size_t n = 0;
    for (size_t i = 0; i < s->sg_cap; i++)
        if ((s->subgroups[i].state == MOQ_SG_OPEN ||
             s->subgroups[i].state == MOQ_SG_STREAMING) &&
            moq_subscription_eq(s->subgroups[i].sub, sub))
            n++;
    return n;
}

static int test_would_block(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 6, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    if (v != MOQ_VERSION_DRAFT_22)
        MOQ_TEST_CHECK_EQ_INT((int)send_pipelined(&p, false, false, true, 9, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    uint64_t id = 0;
    while (take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev)) {
        id = ev.u.subscribe_updated.update_request_id;
        moq_event_cleanup(&ev);
    }
    uint32_t covered = sentry(&p)->owed_update.count;
    MOQ_TEST_CHECK_EQ_U64(covered, v == MOQ_VERSION_DRAFT_22 ? 1 : 2);

    /* A batch larger than the entire send buffer cannot become writable
     * by draining it. Refuse permanently without a partial answer. */
    if (covered > 1) {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_request_update_ok_encode_args_t oka = {
            .request_id = id, .has_largest = true,
            .largest_group = 5, .largest_object = 2,
        };
        MOQ_TEST_CHECK_EQ_INT((int)p.sv->profile->encode_request_update_ok(p.sv, &w, &oka),
                              (int)MOQ_OK);
        size_t one = moq_buf_writer_offset(&w);
        size_t cap0 = p.sv->send_cap;
        MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), 0);
        p.sv->send_cap = p.sv->send_len + covered * one - 1;
        MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_ERR_BUFFER);
        p.sv->send_cap = cap0;
        MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), 0);
        MOQ_TEST_CHECK(sentry(&p)->owed_update.owed && sentry(&p)->forward);
    }

    /* All acknowledgments share one action. A full queue still refuses
     * the whole answer without applying its cumulative update. */
    occupy_queue(&p, 0, 0, 4);
    MOQ_TEST_CHECK_EQ_SIZE(action_queue_avail(p.sv), 0);
    size_t q0 = queued_actions(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), q0);
    moq_sub_entry_t *e = sentry(&p);
    MOQ_TEST_CHECK(e && e->owed_update.owed && e->owed_update.count == covered && e->forward);

    /* Reject: one slot short of its two messages. Nothing may be reset by
     * the refusal. (The open data streams' resets need no room: one that
     * does not fit is owed and the PUBLISH_DONE waits for it.) */
    size_t sgs = live_subgroups(p.sv, p.ssub);
    MOQ_TEST_CHECK_EQ_SIZE(sgs, 1);
    pump(&p);
    drain(p.c);
    occupy_queue(&p, 1, 1, 5);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.ssub), 2);
    q0 = queued_actions(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 5), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), q0);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.ssub), 2);
    e = sentry(&p);
    MOQ_TEST_CHECK(e && e->owed_update.owed && e->owed_update.count == covered);

    /* Once the queue drains, both answers go through; accept is taken. */
    pump(&p);
    drain(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_OK);
    MOQ_TEST_CHECK(!sentry(&p)->forward && !sentry(&p)->owed_update.owed);
    if (v == MOQ_VERSION_DRAFT_22) {
        pump(&p);
        MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
        moq_event_cleanup(&ev);
    }
    failures += finish(&p);
    return failures;
}

/* A reject that cannot take its queue slots is retried to completion. */
static int test_reject_retry(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 6, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);
    occupy_queue(&p, 0, 1, 4);   /* one slot short of the two messages */
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 4), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.ssub), 1);
    pump(&p);
    drain(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 5), (int)MOQ_OK);
    MOQ_TEST_CHECK(sentry(&p) == NULL);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x8);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

/* -- Draft-22: filter and fill take effect only on accept ----------------- */

static moq_result_t send_fill_update(pair_t *p, uint64_t now)
{
    static moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init_sized(&loc, sizeof(loc));
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 1;
    static moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.has_filter = true;
    up.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    up.start_group = 4; up.start_object = 1;
    up.fill = &fill;
    return moq_session_update_subscription(p->c, p->csub, &up, now);
}

static int test_fill_and_filter_d22(bool accept)
{
    int failures = 0;
    pair_t p;
    bool ready = mk(&p, MOQ_VERSION_DRAFT_22, 0, true);
    MOQ_TEST_CHECK(ready);
    if (!ready) goto done;
    uint32_t filter0 = sentry(&p)->filter_type;
    moq_resolved_window_t win0 = sentry(&p)->window;
    moq_result_t rc = send_fill_update(&p, 3);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) goto done;
    moq_event_t ev;
    bool found = take(p.c, MOQ_EVENT_FILL_CREATED, &ev);
    MOQ_TEST_CHECK(found);
    if (!found) goto done;
    moq_fill_t cfill = ev.u.fill_created.fill;
    moq_event_cleanup(&ev);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_REQUEST), 0);
    found = take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev);
    MOQ_TEST_CHECK(found);
    if (!found) goto done;
    MOQ_TEST_CHECK(ev.u.subscribe_updated.answer_owed);
    MOQ_TEST_CHECK(ev.u.subscribe_updated.fill_requested);
    MOQ_TEST_CHECK(!ev.u.subscribe_updated.has_filter);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.start_group, 0);
    MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.start_object, 0);
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    const moq_subscription_update_options_t *options = ev.u.subscribe_updated.options;
    bool complete_options = options != NULL && options->struct_size >= sizeof(*options);
    MOQ_TEST_CHECK(complete_options);
    if (!complete_options) {
        moq_event_cleanup(&ev);
        goto done;
    }
    MOQ_TEST_CHECK_EQ_U64(options->flags, MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION);
    MOQ_TEST_CHECK_EQ_U64(options->initiating_request_id, id);
    MOQ_TEST_CHECK_EQ_U64(options->location.struct_size, MOQ_LOCATION_FILTER_CFG_V0_SIZE);
    MOQ_TEST_CHECK_EQ_U64(options->location.kind, MOQ_LOCATION_FILTER_ABSOLUTE_START);
    MOQ_TEST_CHECK_EQ_U64(options->location.start_group, 4);
    MOQ_TEST_CHECK_EQ_U64(options->location.start_object, 1);
    moq_event_cleanup(&ev);
    /* No fill child, no fill stream, filter and window unchanged. */
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_U64(sentry(&p)->fill_streams_opened, 0);
    MOQ_TEST_CHECK_EQ_U64(sentry(&p)->filter_type, filter0);
    MOQ_TEST_CHECK(memcmp(&sentry(&p)->window, &win0, sizeof(win0)) == 0);
    drain(p.sv);

    if (accept) {
        rc = accept_upd(&p, id, 4);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (rc != MOQ_OK) goto done;
        MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 1);
        MOQ_TEST_CHECK_EQ_U64(sentry(&p)->filter_type, MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START);
        MOQ_TEST_CHECK_EQ_U64(sentry(&p)->req_start_group, 4);
        found = take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev);
        MOQ_TEST_CHECK(found);
        if (!found) goto done;
        moq_event_cleanup(&ev);
        pump(&p);
        found = take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev);
        MOQ_TEST_CHECK(found);
        if (!found) goto done;
        moq_event_cleanup(&ev);
    } else {
        rc = reject_upd(&p, id, 0x7, NULL, 4);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (rc != MOQ_OK) goto done;
        MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
        MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_REQUEST), 0);
        pump(&p);
        /* The subscriber's fill ends with the refusal's code; the
         * subscription ends with UPDATE_FAILED. */
        bool saw_fill = false, saw_done = false;
        while (moq_session_poll_events(p.c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FILL_TERMINAL &&
                moq_fill_eq(ev.u.fill_terminal.fill, cfill)) {
                saw_fill = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_domain,
                                      MOQ_FILL_ERROR_PARENT_REQUEST);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x7);
            }
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                saw_done = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x8);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_fill);
        MOQ_TEST_CHECK(saw_done);
        MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
done:
    failures += finish(&p);
    return failures;
}

/* -- Configuration: the field is read only when struct_size covers it ----- */

static int test_cfg_gate(void)
{
    int failures = 0;
    for (int full = 0; full <= 1; full++) {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        cfg.defer_subscription_updates = true;
        if (!full)
            cfg.struct_size = (uint32_t)offsetof(moq_session_cfg_t, defer_subscription_updates);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        if (!s) continue;
        MOQ_TEST_CHECK(s->defer_subscription_updates == (full != 0));
        moq_session_destroy(s);
    }
    /* The simulator forwards each side's field into its session, and only
     * when struct_size covers that field. */
    static const size_t sizes[] = {
        offsetof(moq_simpair_cfg_t, server_defer_subscription_updates),
        offsetof(moq_simpair_cfg_t, client_defer_subscription_updates),
        sizeof(moq_simpair_cfg_t),
    };
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        for (int side = 0; side <= 1; side++) {
            moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
            scfg.alloc = moq_alloc_default();
            scfg.server_defer_subscription_updates = side == 0;
            scfg.client_defer_subscription_updates = side == 1;
            scfg.struct_size = (uint32_t)sizes[i];
            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
            if (!sp) continue;
            bool sv_on = moq_simpair_server(sp)->defer_subscription_updates;
            bool c_on = moq_simpair_client(sp)->defer_subscription_updates;
            MOQ_TEST_CHECK(sv_on == (side == 0 && i >= 1));
            MOQ_TEST_CHECK(c_on == (side == 1 && i >= 2));
            moq_simpair_destroy(sp);
        }
    }
    return failures;
}

/* The PUBLISH_DONE(UPDATE_FAILED) a rejected update forces carries the
 * number of data streams the publisher opened for the subscription: zero
 * when it opened none, else every subgroup stream it opened, including one
 * already closed. */
static int test_reject_stream_count(moq_version_t v, size_t subgroups)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    for (size_t g = 0; g < subgroups; g++) {
        moq_subgroup_cfg_t sc; moq_subgroup_cfg_init(&sc);
        sc.group_id = 6 + g;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(p.sv, p.ssub, &sc, 3, &sg),
                              (int)MOQ_OK);
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&p.sv->alloc, (const uint8_t *)"x", 1, &pl);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, sg, 0, pl, 3), (int)MOQ_OK);
        moq_rcbuf_decref(pl);
        if (g == 0)
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.sv, sg, 3), (int)MOQ_OK);
    }
    pump(&p);
    drain(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, false, false, true, 9, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 4), (int)MOQ_OK);
    pump(&p);
    size_t dones = 0;
    while (moq_session_poll_events(p.c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            dones++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x8);
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.stream_count, subgroups);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_SIZE(dones, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

int main(void)
{
    int failures = 0;
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    failures += test_cfg_gate();
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        moq_version_t v = versions[i];
        int before = failures;
        failures += test_accept(v);
        failures += test_reject(v);
        failures += test_not_deferred(v);
        failures += test_goaway_defer_policy(v, false);
        failures += test_goaway_defer_policy(v, true);
        failures += test_coalesced(v, true);
        failures += test_coalesced(v, false);
        if (v != MOQ_VERSION_DRAFT_22)   /* draft-22 closes on the second update */
            failures += test_refused_while_owed(v);
        failures += test_cancel_while_owed(v);
        failures += test_close_while_owed(v);
        failures += test_would_block(v);
        failures += test_reject_retry(v);
        failures += test_reject_stream_count(v, 0);
        failures += test_reject_stream_count(v, 2);
        if (failures != before)
            fprintf(stderr, "  version 0x%llx: %d failures\n", (unsigned long long)v,
                    failures - before);
    }
    failures += test_fill_and_filter_d22(true);
    failures += test_fill_and_filter_d22(false);
    if (failures) { fprintf(stderr, "FAIL: test_subscription_update_answer (%d)\n", failures); return 1; }
    printf("PASS: test_subscription_update_answer\n");
    return 0;
}
