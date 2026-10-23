/*
 * Updates of a subscription this side's PUBLISH began, answered by the
 * application (moq_session_cfg_t.defer_subscription_updates), on drafts 16,
 * 18 and 22. A subscriber may update such a subscription exactly as one it
 * began with SUBSCRIBE, and the publisher owes the same single answer, so
 * every behaviour pinned for SUBSCRIBE-initiated subscriptions is pinned
 * here for PUBLISH-initiated ones.
 *
 * Pinned on every profile: the update surfaces as MOQ_EVENT_PUBLISH_UPDATED
 * with answer_owed and nothing is answered or applied until the application
 * decides; accepting applies forward state and priority and sends one
 * REQUEST_OK; rejecting sends REQUEST_ERROR with the application's code and
 * reason followed by PUBLISH_DONE(UPDATE_FAILED) on the PUBLISH request
 * stream, and the subscriber ends the publication with that status; an
 * answer naming a superseded update is refused without effect; further
 * updates before the answer are coalesced where no bound is advertised (one
 * REQUEST_OK per update on accept; on reject one REQUEST_ERROR for the run on
 * draft-18 and one per update, each by its id, on draft-16) and close the
 * session with TOO_MANY_REQUEST_UPDATES on draft-22; an update the session
 * refuses itself ends the subscription with the UPDATE_FAILED terminal,
 * deferred or not, failing any owed run with it, and a refusal the action
 * queue cannot take yet completes once on retry; a subscriber cancel or a session close while an
 * answer is owed drops it without any answer and releases its storage; the
 * publisher cannot finish the publication or send a per-request GOAWAY while
 * an answer is owed; a WOULD_BLOCK from either answer leaves the
 * publication, its data streams and the owed answer exactly as they were;
 * with deferral off an update is answered at once. On draft-22 a fill carried
 * on the update is admitted only on accept, and a rejected update's fill ends
 * on the subscriber with the application's code.
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

typedef struct { int64_t bytes; int64_t blocks; size_t calls; } bal_t;
typedef struct { size_t size; } bal_hdr_t;
#define BAL_HDR 16u

static void *bal_alloc(size_t size, void *ctx)
{
    bal_t *b = (bal_t *)ctx;
    b->calls++;
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
    b->calls++;
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
    b->calls++;
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
    moq_publication_t cpub, spub;
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

/* The publisher's entry for the publication, NULL once it is gone. */
static moq_pub_entry_t *pentry(pair_t *p)
{
    int slot = pub_resolve_handle(p->sv, p->spub);
    return slot < 0 ? NULL : &p->sv->publishes[slot];
}

static moq_pub_entry_t *centry(pair_t *p)
{
    int slot = pub_resolve_handle(p->c, p->cpub);
    return slot < 0 ? NULL : &p->c->publishes[slot];
}

/* The server publishes to the client, which accepts; the server answers the
 * client's updates itself or leaves them to the test. */
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

    moq_publish_cfg_t cfg; moq_publish_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    if (moq_session_publish(p->sv, &cfg, 1, &p->spub) != MOQ_OK) return false;
    pump(p);
    moq_event_t ev;
    if (!take(p->c, MOQ_EVENT_PUBLISH_REQUEST, &ev)) return false;
    p->cpub = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(p->c, p->cpub, &ac, 2) != MOQ_OK) return false;
    pump(p);
    drain(p->c); drain(p->sv);
    memset(g_log.m, 0, sizeof(g_log.m));
    g_log.n = 0;
    return pentry(p) != NULL && pentry(p)->state == MOQ_PUB_ESTABLISHED;
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
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.has_forward = has_forward; up.forward = forward;
    up.has_subscriber_priority = has_priority; up.subscriber_priority = priority;
    return moq_session_update_publication(p->c, p->cpub, &up, now);
}

/* Send a second update while the first is still unacknowledged: the client
 * API holds one at a time, so its pending marker is lifted by hand. The
 * publisher-side behaviour under test is what the wire then carries. */
static moq_result_t send_pipelined(pair_t *p, bool has_forward, bool forward,
                                   bool has_priority, uint8_t priority, uint64_t now)
{
    moq_pub_entry_t *ce = centry(p);
    if (!ce) return MOQ_ERR_STALE_HANDLE;
    ce->update_pending = false;
    return send_update(p, has_forward, forward, has_priority, priority, now);
}

static moq_result_t accept_upd(pair_t *p, uint64_t id, uint64_t now)
{
    moq_accept_subscription_update_cfg_t cfg;
    moq_accept_subscription_update_cfg_init(&cfg);
    cfg.update_request_id = id;
    return moq_session_accept_publication_update(p->sv, p->spub, &cfg, now);
}

static moq_result_t reject_upd(pair_t *p, uint64_t id, uint64_t code, const char *reason,
                               uint64_t now)
{
    moq_reject_subscription_update_cfg_t cfg;
    moq_reject_subscription_update_cfg_init(&cfg);
    cfg.update_request_id = id;
    cfg.error_code = (moq_request_error_t)code;
    if (reason) cfg.reason = (moq_bytes_t){ (const uint8_t *)reason, strlen(reason) };
    return moq_session_reject_publication_update(p->sv, p->spub, &cfg, now);
}

/* Collect every PUBLISH_UPDATED event, returning the newest one's id. */
static uint64_t take_updates(pair_t *p, size_t *count)
{
    moq_event_t ev;
    uint64_t id = 0;
    size_t n = 0;
    while (take(p->sv, MOQ_EVENT_PUBLISH_UPDATED, &ev)) {
        id = ev.u.publish_updated.update_request_id;
        n++;
        moq_event_cleanup(&ev);
    }
    if (count) *count = n;
    return id;
}

/* -- Accept --------------------------------------------------------------- */

static int test_accept(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, true));
    if (!p.sp) return failures;
    moq_pub_entry_t *e = pentry(&p);
    uint8_t prio0 = e->subscriber_priority;
    MOQ_TEST_CHECK(e->send_allowed);

    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, true, 7, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.detail_size >= sizeof(moq_publish_updated_event_t));
    MOQ_TEST_CHECK(moq_publication_eq(ev.u.publish_updated.pub, p.spub));
    MOQ_TEST_CHECK(ev.u.publish_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.update_count, 1);
    MOQ_TEST_CHECK(ev.u.publish_updated.has_forward && !ev.u.publish_updated.forward);
    MOQ_TEST_CHECK(ev.u.publish_updated.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.subscriber_priority, 7);
    MOQ_TEST_CHECK(!ev.u.publish_updated.fill_requested);
    uint64_t id = ev.u.publish_updated.update_request_id;
    moq_event_cleanup(&ev);

    /* Nothing answered, nothing applied. */
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
    MOQ_TEST_CHECK(!take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    e = pentry(&p);
    MOQ_TEST_CHECK(e->send_allowed);
    MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, prio0);

    /* An answer naming another update is stale and changes nothing. */
    size_t q0 = queued_actions(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id + 2, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id + 2, 0x5, NULL, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), q0);
    MOQ_TEST_CHECK(pentry(&p) && pentry(&p)->owed_update.owed && pentry(&p)->send_allowed);
    /* The subscription-handle calls do not answer a publication. */
    moq_accept_subscription_update_cfg_t sac;
    moq_accept_subscription_update_cfg_init(&sac);
    sac.update_request_id = id;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscription_update(
                              p.sv, (moq_subscription_t){ p.spub._opaque }, &sac, 4),
                          (int)MOQ_ERR_STALE_HANDLE);

    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 5), (int)MOQ_OK);
    e = pentry(&p);
    MOQ_TEST_CHECK(!e->send_allowed);
    MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, 7);
    MOQ_TEST_CHECK(!e->owed_update.owed);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_ERR_WRONG_STATE);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
    if (v == MOQ_VERSION_DRAFT_16 && nth_type(TYPE_REQUEST_OK, 0))
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_OK, 0)->id, id);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(centry(&p) && !centry(&p)->update_pending && !centry(&p)->send_allowed);
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
    uint64_t id = take_updates(&p, NULL);

    /* Arguments are checked before anything else. */
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, MOQ_REQUEST_ERROR_REDIRECT, NULL, 4),
                          (int)MOQ_ERR_INVAL);
    moq_reject_subscription_update_cfg_t big;
    moq_reject_subscription_update_cfg_init(&big);
    static uint8_t long_reason[1025];
    big.update_request_id = id;
    big.reason = (moq_bytes_t){ long_reason, sizeof(long_reason) };
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publication_update(p.sv, p.spub, &big, 4),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(pentry(&p) && pentry(&p)->owed_update.owed);

    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, "nope", 5), (int)MOQ_OK);
    MOQ_TEST_CHECK(pentry(&p) == NULL);
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
     * terminal -- and ends the publication with UPDATE_FAILED without ever
     * seeing the update acknowledged. */
    moq_event_t ev;
    size_t oks = 0, dones = 0;
    while (moq_session_poll_events(p.c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) oks++;
        if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            dones++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x8);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_SIZE(oks, 0);
    MOQ_TEST_CHECK_EQ_SIZE(dones, 1);
    MOQ_TEST_CHECK(centry(&p) == NULL);
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
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(!ev.u.publish_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.update_count, 1);
    uint64_t id = ev.u.publish_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(!pentry(&p)->owed_update.owed);
    MOQ_TEST_CHECK(!pentry(&p)->send_allowed);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
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
    moq_result_t rc = moq_session_request_goaway_publish(p.sv, p.spub, &gc, 3);
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
    bool found = take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev);
    MOQ_TEST_CHECK(found);
    if (!found) return failures + finish(&p);
    bool owed = defer && v == MOQ_VERSION_DRAFT_16;
    MOQ_TEST_CHECK(ev.u.publish_updated.answer_owed == owed);
    uint64_t id = ev.u.publish_updated.update_request_id, queried = 0;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_publication_owed_update(p.sv, p.spub, &queried,
        NULL, NULL) == (owed ? MOQ_OK : MOQ_ERR_WRONG_STATE));
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), owed ? 0 : 1);
    if (owed) {
        MOQ_TEST_CHECK_EQ_U64(queried, id);
        MOQ_TEST_CHECK(accept_upd(&p, id, 4) == MOQ_OK);
        pump(&p);
    }
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    MOQ_TEST_PASS("raw_publication_goaway_defer_policy");
    return failures;
}

/* -- Switching deferral on an existing session ---------------------------- */

static int test_setter(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, false));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_set_defer_subscription_updates(NULL, true),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_set_defer_subscription_updates(p.sv, true),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.u.publish_updated.answer_owed);
    uint64_t id = ev.u.publish_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    /* Switching it off leaves the owed answer owed. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_set_defer_subscription_updates(p.sv, false),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(pentry(&p)->owed_update.owed);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 1);
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    moq_event_cleanup(&ev);
    /* And the next update is answered at once again. */
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, true, false, 0, 5), (int)MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(!ev.u.publish_updated.answer_owed);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 2);
    MOQ_TEST_CHECK(pentry(&p)->send_allowed);
    failures += finish(&p);
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

    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.update_count, 1);
    uint64_t id1 = ev.u.publish_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.u.publish_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.update_count, 2);
    /* Cumulative: the first update's forward change and the second's priority. */
    MOQ_TEST_CHECK(ev.u.publish_updated.has_forward && !ev.u.publish_updated.forward);
    MOQ_TEST_CHECK(ev.u.publish_updated.has_subscriber_priority);
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.subscriber_priority, 9);
    uint64_t id2 = ev.u.publish_updated.update_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(id1 != id2);
    MOQ_TEST_CHECK(pentry(&p)->send_allowed);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK) + count_type(TYPE_REQUEST_ERROR), 0);

    /* The superseded update cannot be answered on its own. */
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id1, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id1, 0x5, NULL, 4), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(pentry(&p)->owed_update.owed);

    /* The answer goes to the publisher's wire only; the client's own update
     * bookkeeping was bypassed above, so nothing is delivered back. */
    if (accept) {
        MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id2, 5), (int)MOQ_OK);
        moq_pub_entry_t *e = pentry(&p);
        MOQ_TEST_CHECK(!e->send_allowed);
        MOQ_TEST_CHECK_EQ_U64(e->subscriber_priority, 9);
        MOQ_TEST_CHECK(!e->owed_update.owed && e->owed_update.prior_ids == NULL);
    } else {
        MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id2, 0x5, NULL, 5), (int)MOQ_OK);
        MOQ_TEST_CHECK(pentry(&p) == NULL);
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

/* -- An update the session refuses, on the wire -------------------------- */

/* Feed the publisher an update carrying an authorization token it rejects
 * (DELETE of an alias never registered), as the subscriber's next request. */
static moq_result_t feed_refused_update(pair_t *p, uint64_t id, uint64_t now)
{
    moq_pub_entry_t *e = pentry(p);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    if (p->v == MOQ_VERSION_DRAFT_16) {
        static const uint8_t del_alias[] = { 0x00, 0x09 };   /* DELETE alias 9 */
        moq_kvp_entry_t tok = { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                                .value = del_alias, .value_len = sizeof(del_alias) };
        moq_result_t rc = moq_d16_encode_request_update(&w, id, e->request_id, &tok, 1);
        if (rc < 0) return rc;
        return moq_session_on_control_bytes(p->sv, buf, moq_buf_writer_offset(&w), now);
    }
    moq_d18_msg_params_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.auth_token_count = 1;
    prm.auth_tokens[0].alias_type = 0;   /* DELETE */
    prm.auth_tokens[0].alias = 9;
    moq_result_t rc = moq_d18_encode_request_update(&w, id, &prm);
    if (rc < 0) return rc;
    return moq_session_on_bidi_stream_bytes(p->sv, e->rs.ref, buf, moq_buf_writer_offset(&w),
                                            false, now);
}

/* -- The publication ends while an answer is owed -------------------------- */

/* The subscriber cancels the publication: the PUBLISH request stream is reset
 * (draft-18/22) or UNSUBSCRIBE names it (draft-16). */
static moq_result_t feed_cancel(pair_t *p, uint64_t now)
{
    moq_pub_entry_t *e = pentry(p);
    if (!e) return MOQ_ERR_STALE_HANDLE;
    if (p->v != MOQ_VERSION_DRAFT_16)
        return moq_session_on_bidi_stream_reset(p->sv, e->rs.ref, 0x0, now);
    /* UNSUBSCRIBE (0x0A): 16-bit length, then the Request ID. */
    uint8_t msg[16];
    uint8_t body[8];
    moq_buf_writer_t bw;
    moq_buf_writer_init(&bw, body, sizeof(body));
    if (moq_buf_write_varint(&bw, e->request_id) < 0) return MOQ_ERR_INVAL;
    size_t blen = moq_buf_writer_offset(&bw);
    msg[0] = 0x0A;
    msg[1] = (uint8_t)(blen >> 8);
    msg[2] = (uint8_t)blen;
    memcpy(msg + 3, body, blen);
    return moq_session_on_control_bytes(p->sv, msg, 3 + blen, now);
}

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
    uint64_t id = take_updates(&p, NULL);
    MOQ_TEST_CHECK(pentry(&p) && pentry(&p)->owed_update.owed);

    /* The publisher may neither finish the publication nor migrate it first. */
    moq_finish_publish_cfg_t fc; moq_finish_publish_cfg_init(&fc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(p.sv, p.spub, &fc, 4),
                          (int)MOQ_ERR_WRONG_STATE);
    moq_request_goaway_cfg_t gc; moq_request_goaway_cfg_init(&gc);
    moq_result_t grc = moq_session_request_goaway_publish(p.sv, p.spub, &gc, 4);
    MOQ_TEST_CHECK_EQ_INT((int)grc, v == MOQ_VERSION_DRAFT_16 ? (int)MOQ_ERR_INVAL
                                                             : (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(pentry(&p) && pentry(&p)->owed_update.owed);

    /* The subscriber cancels: the owed answer goes with the publication. */
    MOQ_TEST_CHECK_EQ_INT((int)feed_cancel(&p, 5), (int)MOQ_OK);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UNSUBSCRIBED, &ev));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(pentry(&p) == NULL);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 6), (int)MOQ_ERR_STALE_HANDLE);
    log_queued(p.sv);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);

    /* No freed slot keeps anything owed. */
    for (size_t i = 0; i < p.sv->pub_cap; i++)
        MOQ_TEST_CHECK(!p.sv->publishes[i].owed_update.owed &&
                       p.sv->publishes[i].owed_update.prior_ids == NULL);
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
    uint64_t id = take_updates(&p, NULL);
    MOQ_TEST_CHECK(pentry(&p) && pentry(&p)->owed_update.owed);
    if (v != MOQ_VERSION_DRAFT_22)
        MOQ_TEST_CHECK(pentry(&p)->owed_update.prior_ids != NULL);
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
 * stream on the publication: a subgroup open plus objects. */
static void occupy_queue(pair_t *p, uint64_t subgroup, size_t free_slots, uint64_t now)
{
    moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
    sg.group_id = 6; sg.subgroup_id = subgroup; sg.publisher_priority = 1;
    moq_subgroup_handle_t h;
    if (moq_session_open_pub_subgroup(p->sv, p->spub, &sg, now, &h) != MOQ_OK) return;
    uint64_t obj = 0;
    while (action_queue_avail(p->sv) > free_slots && obj < 64) {
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"x", 1, &pl);
        moq_result_t rc = moq_session_write_object(p->sv, h, obj++, pl, now);
        moq_rcbuf_decref(pl);
        if (rc != MOQ_OK) break;
    }
}

static size_t live_subgroups(const moq_session_t *s, moq_publication_t pub)
{
    size_t n = 0;
    for (size_t i = 0; i < s->sg_cap; i++)
        if ((s->subgroups[i].state == MOQ_SG_OPEN ||
             s->subgroups[i].state == MOQ_SG_STREAMING) &&
            moq_publication_eq(s->subgroups[i].pub, pub))
            n++;
    return n;
}

static int test_would_block(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 6, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, false, false, true, 3, 3), (int)MOQ_OK);
    if (v != MOQ_VERSION_DRAFT_22)
        MOQ_TEST_CHECK_EQ_INT((int)send_pipelined(&p, false, false, true, 9, 3), (int)MOQ_OK);
    pump(&p);
    uint64_t id = take_updates(&p, NULL);
    uint32_t covered = pentry(&p)->owed_update.count;
    MOQ_TEST_CHECK_EQ_U64(covered, v == MOQ_VERSION_DRAFT_22 ? 1 : 2);
    uint8_t prio0 = pentry(&p)->subscriber_priority;

    /* A batch larger than the entire send buffer cannot become writable
     * by draining it. Refuse permanently without a partial answer. */
    if (covered > 1) {
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_request_update_ok_encode_args_t oka = { .request_id = id };
        MOQ_TEST_CHECK_EQ_INT((int)p.sv->profile->encode_request_update_ok(p.sv, &w, &oka),
                              (int)MOQ_OK);
        size_t one = moq_buf_writer_offset(&w);
        size_t cap0 = p.sv->send_cap;
        MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), 0);
        p.sv->send_cap = p.sv->send_len + covered * one - 1;
        MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_ERR_BUFFER);
        p.sv->send_cap = cap0;
        MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), 0);
        MOQ_TEST_CHECK(pentry(&p)->owed_update.owed);
        MOQ_TEST_CHECK_EQ_U64(pentry(&p)->subscriber_priority, prio0);
    }

    /* All acknowledgments share one action. A full queue still refuses
     * the whole answer without applying its cumulative update. */
    occupy_queue(&p, 0, 0, 4);
    MOQ_TEST_CHECK_EQ_SIZE(action_queue_avail(p.sv), 0);
    size_t q0 = queued_actions(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), q0);
    moq_pub_entry_t *e = pentry(&p);
    MOQ_TEST_CHECK(e && e->owed_update.owed && e->owed_update.count == covered);
    MOQ_TEST_CHECK(e && e->subscriber_priority == prio0);

    /* Reject: one slot short of its two messages. Nothing may be reset by
     * the refusal. (The open data streams' resets need no room: one that
     * does not fit is owed and the PUBLISH_DONE waits for it.) */
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.spub), 1);
    pump(&p);
    drain(p.c);
    occupy_queue(&p, 1, 1, 5);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.spub), 2);
    q0 = queued_actions(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 5), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(queued_actions(p.sv), q0);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.spub), 2);
    e = pentry(&p);
    MOQ_TEST_CHECK(e && e->owed_update.owed && e->owed_update.count == covered);

    /* Once the queue drains, the answer goes through; accept is taken. */
    pump(&p);
    drain(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 6), (int)MOQ_OK);
    MOQ_TEST_CHECK(!pentry(&p)->owed_update.owed);
    MOQ_TEST_CHECK_EQ_U64(pentry(&p)->subscriber_priority, v == MOQ_VERSION_DRAFT_22 ? 3 : 9);
    if (v == MOQ_VERSION_DRAFT_22) {
        pump(&p);
        moq_event_t ev;
        MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
        moq_event_cleanup(&ev);
    }
    failures += finish(&p);
    return failures;
}

/* A reject that cannot take its queue slots is retried to completion; it
 * resets the publication's open data stream. */
static int test_reject_retry(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 6, true));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, false, false, true, 3, 3), (int)MOQ_OK);
    pump(&p);
    uint64_t id = take_updates(&p, NULL);
    occupy_queue(&p, 0, 1, 4);   /* one slot short of the two messages */
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 4), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.spub), 1);
    pump(&p);
    drain(p.c);
    moq_publication_t spub = p.spub;
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 5), (int)MOQ_OK);
    MOQ_TEST_CHECK(pentry(&p) == NULL);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, spub), 0);
    size_t resets = 0;
    for (size_t i = p.sv->action_head; i < p.sv->action_tail; i++)
        if (p.sv->actions[i % p.sv->action_cap].kind == MOQ_ACTION_RESET_DATA) resets++;
    MOQ_TEST_CHECK_EQ_SIZE(resets, 1);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLISH_FINISHED, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x8);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

/* -- An update the session refuses itself --------------------------------- */

/* How the publisher stands when the refused update arrives. */
typedef enum {
    REFUSED_NOT_DEFERRED,   /* the session answers updates itself */
    REFUSED_NOTHING_OWED,   /* deferral on, the earlier update answered */
    REFUSED_OWED,           /* deferral on, the earlier update still owed */
} refused_mode_t;

/* Count the RESET_DATA actions the publisher has queued. */
static size_t queued_resets(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = s->action_head; i < s->action_tail; i++)
        if (s->actions[i % s->action_cap].kind == MOQ_ACTION_RESET_DATA) n++;
    return n;
}

/* Open a data stream on the publication and leave it live. */
static void open_live_stream(pair_t *p, uint64_t now)
{
    moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
    sg.group_id = 4; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t h;
    if (moq_session_open_pub_subgroup(p->sv, p->spub, &sg, now, &h) != MOQ_OK) return;
    moq_rcbuf_t *pl = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"x", 1, &pl);
    (void)moq_session_write_object(p->sv, h, 0, pl, now);
    moq_rcbuf_decref(pl);
}

/* The subscriber's side of a refused update it sent: the update is pending
 * under `id`, as moq_session_update_publication() leaves it -- including,
 * on draft-16, the by-id registration its answer is correlated through. */
static void mark_sent(pair_t *p, uint64_t id)
{
    moq_pub_entry_t *ce = centry(p);
    if (!ce) return;
    ce->update_pending = true;
    ce->update_request_id = id;
    if (p->v == MOQ_VERSION_DRAFT_16) {
        moq_request_endpoint_t ep;
        memset(&ep, 0, sizeof(ep));
        ep.kind = MOQ_REQ_PUBLICATION_UPDATE;
        ep.slot = pub_resolve_handle(p->c, p->cpub);
        ep.has_request_id = true;
        ep.request_id = id;
        request_registry_insert_by_id(p->c, id, ep);
    }
}

/* The subscriber ended the publication with UPDATE_FAILED, never seeing the
 * update acknowledged, and both sessions stay up. */
static int check_subscriber_ended(pair_t *p)
{
    int failures = 0;
    moq_event_t ev;
    size_t oks = 0, dones = 0;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) oks++;
        if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            dones++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x8);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_SIZE(oks, 0);
    MOQ_TEST_CHECK_EQ_SIZE(dones, 1);
    MOQ_TEST_CHECK(centry(p) == NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p->c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p->sv), (int)MOQ_SESS_ESTABLISHED);
    return failures;
}

/* An update the session refuses itself (here a rejected authorization token,
 * 0x17) is unsuccessful, so the publisher terminates the subscription:
 * REQUEST_ERROR, then PUBLISH_DONE(UPDATE_FAILED) -- with FIN on the PUBLISH
 * request stream on 18/22 -- after resetting the publication's live data
 * streams, and the publication ends. That holds whether the session answers
 * updates itself or the application does, and whether or not an answer is
 * owed; with one owed, the failure also answers the owed run (one error by
 * id per update on draft-16, one for the run on draft-18), never the
 * application. The refused update itself never surfaces as PUBLISH_UPDATED. */
static int test_refused(moq_version_t v, refused_mode_t mode)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 0, mode != REFUSED_NOT_DEFERRED));
    if (!p.sp) return failures;
    /* A priority change: forwarding stays on, so a data stream can be live. */
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, false, false, true, 3, 3), (int)MOQ_OK);
    pump(&p);
    uint64_t id1 = take_updates(&p, NULL);
    MOQ_TEST_CHECK(pentry(&p) != NULL);
    if (!pentry(&p)) { failures += finish(&p); return failures; }
    MOQ_TEST_CHECK_EQ_INT((int)pentry(&p)->owed_update.owed,
                          (int)(mode != REFUSED_NOT_DEFERRED));
    if (mode == REFUSED_NOTHING_OWED) {
        MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id1, 3), (int)MOQ_OK);
        pump(&p);
    }
    open_live_stream(&p, 3);
    pump(&p);
    drain(p.c);
    drain(p.sv);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, p.spub), 1);
    memset(g_log.m, 0, sizeof(g_log.m));
    g_log.n = 0;

    uint64_t id2 = id1 + 2;   /* the subscriber's next request id */
    if (mode != REFUSED_OWED) mark_sent(&p, id2);
    moq_publication_t spub = p.spub;
    MOQ_TEST_CHECK_EQ_INT((int)feed_refused_update(&p, id2, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(pentry(&p) == NULL);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_PUBLISH_UPDATED), 0);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, spub), 0);
    MOQ_TEST_CHECK_EQ_SIZE(queued_resets(p.sv), 1);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id1, 5), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id2, 0x5, NULL, 5), (int)MOQ_ERR_STALE_HANDLE);

    log_queued(p.sv);
    size_t n_err = (mode == REFUSED_OWED && v == MOQ_VERSION_DRAFT_16) ? 2 : 1;
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), n_err);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
    for (size_t i = 0; i < count_type(TYPE_REQUEST_ERROR); i++)
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, i)->code, 0x17);
    if (v == MOQ_VERSION_DRAFT_16 && count_type(TYPE_REQUEST_ERROR) == n_err) {
        if (n_err == 2) {
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 0)->id, id1);
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 1)->id, id2);
        } else {
            MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_REQUEST_ERROR, 0)->id, id2);
        }
    }
    /* The terminal is the last message, and ends the request stream. */
    MOQ_TEST_CHECK(g_log.n > 0 && g_log.m[g_log.n - 1].type == TYPE_PUBLISH_DONE);
    if (g_log.n > 0) {
        MOQ_TEST_CHECK_EQ_U64(g_log.m[g_log.n - 1].code, 0x8);
        if (v != MOQ_VERSION_DRAFT_16) MOQ_TEST_CHECK(g_log.m[g_log.n - 1].fin);
    }

    if (mode != REFUSED_OWED) {
        pump(&p);
        failures += check_subscriber_ended(&p);
    }
    failures += finish(&p);
    return failures;
}

/* Resume the refused update the publisher retained: an empty input on the
 * stream it arrived on. */
static moq_result_t resume_input(pair_t *p, moq_stream_ref_t ref, uint64_t now)
{
    if (p->v == MOQ_VERSION_DRAFT_16)
        return moq_session_on_control_bytes(p->sv, NULL, 0, now);
    return moq_session_on_bidi_stream_bytes(p->sv, ref, NULL, 0, false, now);
}

/* A refusal the action queue cannot take yet is retryable: the publication
 * stays, nothing is answered, and resuming the retained input completes the
 * same terminal once -- one REQUEST_ERROR, one PUBLISH_DONE(UPDATE_FAILED),
 * and the live data stream reset exactly once across both attempts. */
static int test_refused_would_block(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, v, 6, false));
    if (!p.sp) return failures;
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, false, false, true, 3, 3), (int)MOQ_OK);
    pump(&p);
    uint64_t id1 = take_updates(&p, NULL);
    drain(p.c);
    memset(g_log.m, 0, sizeof(g_log.m));
    g_log.n = 0;
    moq_pub_entry_t *e = pentry(&p);
    MOQ_TEST_CHECK(e != NULL);
    if (!e) { failures += finish(&p); return failures; }
    moq_stream_ref_t ref = e->rs.ref;
    moq_publication_t spub = p.spub;

    /* One free slot: the data stream's reset fits, the messages do not. */
    occupy_queue(&p, 0, 1, 4);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, spub), 1);
    uint64_t id2 = id1 + 2;
    mark_sent(&p, id2);
    size_t resets = queued_resets(p.sv);
    moq_result_t rc = feed_refused_update(&p, id2, 5);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(pentry(&p) != NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    resets = queued_resets(p.sv) - resets;
    log_queued(p.sv);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 0);
    memset(g_log.m, 0, sizeof(g_log.m));
    g_log.n = 0;

    /* Room again: the retained update completes the terminal. */
    moq_action_t a;
    while (moq_session_poll_actions(p.sv, &a, 1) > 0) moq_action_cleanup(&a);
    size_t before = queued_resets(p.sv);
    MOQ_TEST_CHECK_EQ_INT((int)resume_input(&p, ref, 6), (int)MOQ_OK);
    MOQ_TEST_CHECK(pentry(&p) == NULL);
    resets += queued_resets(p.sv) - before;
    MOQ_TEST_CHECK_EQ_SIZE(resets, 1);
    MOQ_TEST_CHECK_EQ_SIZE(live_subgroups(p.sv, spub), 0);
    log_queued(p.sv);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_OK), 0);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_REQUEST_ERROR), 1);
    MOQ_TEST_CHECK_EQ_SIZE(count_type(TYPE_PUBLISH_DONE), 1);
    if (count_type(TYPE_PUBLISH_DONE) == 1)
        MOQ_TEST_CHECK_EQ_U64(nth_type(TYPE_PUBLISH_DONE, 0)->code, 0x8);
    failures += finish(&p);
    return failures;
}

/* -- Draft-22: a fill on the update is admitted only on accept ------------ */

static int test_fill_d22(bool accept)
{
    int failures = 0;
    pair_t p;
    MOQ_TEST_CHECK(mk(&p, MOQ_VERSION_DRAFT_22, 0, true));
    if (!p.sp) return failures;
    /* Objects up to {5,0} exist, so the fill range {1,0}..Largest is not
     * empty. */
    moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
    sg.group_id = 5; sg.subgroup_id = 0; sg.publisher_priority = 1;
    moq_subgroup_handle_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(p.sv, p.spub, &sg, 2, &h),
                          (int)MOQ_OK);
    moq_rcbuf_t *pl = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"x", 1, &pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, h, 0, pl, 2), (int)MOQ_OK);
    moq_rcbuf_decref(pl);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.sv, h, 2), (int)MOQ_OK);
    pump(&p);
    drain(p.c);
    static moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init_sized(&loc, sizeof(loc));
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    loc.start_group = 1;
    static moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.has_subscriber_priority = true;
    up.subscriber_priority = 40;
    up.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.c, p.cpub, &up, 3),
                          (int)MOQ_OK);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t cfill = ev.u.fill_created.fill;
    moq_event_cleanup(&ev);
    pump(&p);
    MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_REQUEST), 0);
    MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.u.publish_updated.answer_owed);
    MOQ_TEST_CHECK(ev.u.publish_updated.fill_requested);
    uint64_t id = ev.u.publish_updated.update_request_id;
    moq_event_cleanup(&ev);
    /* No fill child and no fill stream before the answer. */
    MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
    MOQ_TEST_CHECK_EQ_U64(pentry(&p)->fill_streams_opened, 0);
    drain(p.sv);

    if (accept) {
        MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, id, 4), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 1);
        MOQ_TEST_CHECK_EQ_U64(pentry(&p)->subscriber_priority, 40);
        MOQ_TEST_CHECK(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
        moq_event_cleanup(&ev);
        pump(&p);
        MOQ_TEST_CHECK(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
        moq_event_cleanup(&ev);
    } else {
        MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x7, NULL, 4), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(p.sv->fills.live, 0);
        MOQ_TEST_CHECK_EQ_SIZE(queued_events(p.sv, MOQ_EVENT_FILL_REQUEST), 0);
        pump(&p);
        /* The subscriber's fill ends with the refusal's code; the
         * publication ends with UPDATE_FAILED. */
        bool saw_fill = false, saw_done = false;
        while (moq_session_poll_events(p.c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FILL_TERMINAL &&
                moq_fill_eq(ev.u.fill_terminal.fill, cfill)) {
                saw_fill = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_domain,
                                      MOQ_FILL_ERROR_PARENT_REQUEST);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fill_terminal.error_code, 0x7);
            }
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
                saw_done = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x8);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(saw_fill);
        MOQ_TEST_CHECK(saw_done);
        MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.sv), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

/* The PUBLISH_DONE(UPDATE_FAILED) a rejected update forces carries the
 * number of data streams the publisher opened for the publication: zero
 * when it opened none, else every subgroup stream it opened. */
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
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(p.sv, p.spub, &sc, 3, &sg),
                              (int)MOQ_OK);
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"x", 1, &pl);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, sg, 0, pl, 3), (int)MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    pump(&p);
    drain(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)send_update(&p, true, false, false, 0, 3), (int)MOQ_OK);
    pump(&p);
    uint64_t id = take_updates(&p, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)reject_upd(&p, id, 0x5, NULL, 4), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    size_t dones = 0;
    while (moq_session_poll_events(p.c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            dones++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, 0x8);
            MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.stream_count, subgroups);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_SIZE(dones, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(p.c), (int)MOQ_SESS_ESTABLISHED);
    failures += finish(&p);
    return failures;
}

/* -- Pre-poll owed-update observation ----------------------------------- */

#define QUERY_REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
        goto done; \
    } \
} while (0)

typedef struct {
    uint64_t epoch, now, progress;
    size_t action_head, action_tail, event_head, event_tail;
    size_t send_len, event_scratch_len, output_scratch_len, alloc_calls;
    int64_t bytes, blocks;
} query_state_t;

static query_state_t query_state(const pair_t *p)
{
    const moq_session_t *s = p->sv;
    query_state_t state;
    memset(&state, 0, sizeof(state));
    state.epoch = s->borrow_epoch; state.now = s->last_now_us;
    state.progress = s->event_progress_token;
    state.action_head = s->action_head; state.action_tail = s->action_tail;
    state.event_head = s->event_head; state.event_tail = s->event_tail;
    state.send_len = s->send_len;
    state.event_scratch_len = s->event_scratch_len;
    state.output_scratch_len = s->output_scratch_len;
    state.alloc_calls = p->bal.calls;
    state.bytes = p->bal.bytes; state.blocks = p->bal.blocks;
    return state;
}

static bool all_poison(const void *data, size_t n)
{
    const uint8_t *bytes = data;
    for (size_t i = 0; i < n; i++) if (bytes[i] != 0xa5) return false;
    return true;
}

static int query_refusal(moq_session_t *s, moq_publication_t pub, moq_result_t want)
{
    int failures = 0;
    uint64_t id = UINT64_MAX;
    bool has = true;
    moq_location_filter_cfg_t filter;
    memset(&filter, 0xa5, sizeof(filter));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_owed_update(s, pub,
                              &id, &has, &filter), (int)want);
    MOQ_TEST_CHECK_EQ_U64(id, UINT64_MAX);
    MOQ_TEST_CHECK(has && all_poison(&filter, sizeof(filter)));
    return failures;
}

static int query_outputs(pair_t *p, uint64_t want_id,
                         const moq_location_filter_cfg_t *want_filter)
{
    int failures = 0;
    union {
        moq_location_filter_cfg_t filter;
        uint8_t bytes[sizeof(moq_location_filter_cfg_t) + 16];
    } out;
    query_state_t before = query_state(p);
    for (unsigned mask = 0; mask < 8; mask++) {
        uint64_t id = UINT64_MAX;
        bool has = want_filter == NULL;
        memset(&out, 0xa5, sizeof(out));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_owed_update(p->sv, p->spub,
            (mask & 1) ? &id : NULL, (mask & 2) ? &has : NULL,
            (mask & 4) ? &out.filter : NULL), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(id, (mask & 1) ? want_id : UINT64_MAX);
        MOQ_TEST_CHECK(has == ((mask & 2) ? want_filter != NULL : want_filter == NULL));
        if ((mask & 4) && want_filter) {
            MOQ_TEST_CHECK(memcmp(&out.filter, want_filter, sizeof(*want_filter)) == 0);
            MOQ_TEST_CHECK(all_poison(out.bytes + sizeof(out.filter), 16));
        } else {
            MOQ_TEST_CHECK(all_poison(&out, sizeof(out)));
        }
    }

    /* Refuse every short prefix before writing even the other outputs. */
    for (size_t size = 0; size < 48; size++) {
        uint64_t id = UINT64_MAX;
        bool has = true;
        memset(&out, 0xa5, sizeof(out));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_owed_update_sized(
            p->sv, p->spub, &id, &has, &out.filter, size), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(id == UINT64_MAX && has && all_poison(&out, sizeof(out)));
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_owed_update_sized(
        p->sv, p->spub, NULL, NULL, NULL, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_owed_update_sized(
        p->sv, p->spub, NULL, NULL, NULL, SIZE_MAX), (int)MOQ_OK);

    /* A future-sized caller owns every byte beyond the current output. */
    memset(&out, 0xa5, sizeof(out));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_owed_update_sized(
        p->sv, p->spub, NULL, NULL, &out.filter, sizeof(out)), (int)MOQ_OK);
    if (want_filter) {
        MOQ_TEST_CHECK(memcmp(&out.filter, want_filter, sizeof(*want_filter)) == 0);
        MOQ_TEST_CHECK(all_poison(out.bytes + sizeof(out.filter), 16));
    } else {
        MOQ_TEST_CHECK(all_poison(&out, sizeof(out)));
    }

    /* A function pointer calls the real frozen-v0 symbol, not its macro. */
    moq_result_t (*legacy)(moq_session_t *, moq_publication_t, uint64_t *, bool *,
                           moq_location_filter_cfg_t *) = moq_session_publication_owed_update;
    memset(&out, 0xa5, sizeof(out));
    MOQ_TEST_CHECK_EQ_INT((int)legacy(p->sv, p->spub, NULL, NULL, &out.filter), (int)MOQ_OK);
    if (want_filter) {
        MOQ_TEST_CHECK(memcmp(&out.filter, want_filter, 48) == 0);
        MOQ_TEST_CHECK(all_poison(out.bytes + 48, sizeof(out) - 48));
    } else {
        MOQ_TEST_CHECK(all_poison(&out, sizeof(out)));
    }
    moq_location_filter_cfg_t *exact = malloc(48);
    MOQ_TEST_CHECK(exact != NULL);
    if (exact) {
        memset(exact, 0xa5, 48);
        MOQ_TEST_CHECK_EQ_INT((int)legacy(p->sv, p->spub, NULL, NULL, exact), (int)MOQ_OK);
        if (want_filter) MOQ_TEST_CHECK(memcmp(exact, want_filter, 48) == 0);
        else MOQ_TEST_CHECK(all_poison(exact, 48));
        free(exact);
    }
    query_state_t after = query_state(p);
    MOQ_TEST_CHECK(memcmp(&before, &after, sizeof(before)) == 0);
    return failures;
}

static int test_query_lifecycle(moq_version_t v, bool accept)
{
    int failures = 0;
    pair_t p;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    QUERY_REQUIRE(mk(&p, v, 0, true));
    failures += query_refusal(p.sv, p.spub, MOQ_ERR_WRONG_STATE);
    moq_auth_token_t token = { .token_type = 9, .token_value = MOQ_BYTES_LITERAL("query") };
    moq_publication_update_cfg_t up;
    moq_publication_update_cfg_init(&up);
    up.has_subscriber_priority = true; up.subscriber_priority = 7;
    up.auth_tokens = &token; up.auth_token_count = 1;
    QUERY_REQUIRE(moq_session_update_publication(p.c, p.cpub, &up, 3) == MOQ_OK);
    QUERY_REQUIRE(centry(&p) != NULL);
    uint64_t sent_id = centry(&p)->update_request_id;
    pump(&p);
    QUERY_REQUIRE(queued_events(p.sv, MOQ_EVENT_PUBLISH_UPDATED) == 1);
    moq_finish_publish_cfg_t fin; moq_finish_publish_cfg_init(&fin);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(p.sv, p.spub, &fin, 4),
                          (int)MOQ_ERR_WRONG_STATE);
    failures += query_outputs(&p, sent_id, NULL);
    uint64_t id = UINT64_MAX;
    QUERY_REQUIRE(moq_session_publication_owed_update(p.sv, p.spub, &id, NULL, NULL) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(id, sent_id);
    QUERY_REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.update_request_id, id);
    QUERY_REQUIRE(ev.u.publish_updated.token_count == 1 && ev.u.publish_updated.tokens != NULL);
    const moq_resolved_token_t *borrow = ev.u.publish_updated.tokens;
    QUERY_REQUIRE(borrow->token_value.len == 5 && borrow->token_value.data != NULL);
    failures += query_outputs(&p, sent_id, NULL);
    MOQ_TEST_CHECK(moq_session_borrow_valid(p.sv, ev.borrow_epoch));
    MOQ_TEST_CHECK_EQ_U64(borrow->token_type, 9);
    MOQ_TEST_CHECK(memcmp(borrow->token_value.data, "query", 5) == 0);
    moq_event_cleanup(&ev);
    memset(&ev, 0, sizeof(ev));

    failures += query_refusal(NULL, p.spub, MOQ_ERR_INVAL);
    failures += query_refusal(p.sv, p.cpub, MOQ_ERR_STALE_HANDLE);
    failures += query_refusal(p.c, p.cpub, MOQ_ERR_WRONG_STATE);
    failures += query_refusal(p.sv, MOQ_PUBLICATION_INVALID, MOQ_ERR_STALE_HANDLE);
    moq_subscribe_cfg_t sub_cfg; moq_subscribe_cfg_init(&sub_cfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("other") };
    sub_cfg.track_namespace = (moq_namespace_t){ ns, 1 };
    sub_cfg.track_name = MOQ_BYTES_LITERAL("pool");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub;
    QUERY_REQUIRE(moq_session_subscribe(p.sv, &sub_cfg, 4, &sub) == MOQ_OK);
    moq_publication_t wrong_pool = { ._opaque = sub._opaque };
    failures += query_refusal(p.sv, wrong_pool, MOQ_ERR_STALE_HANDLE);
    if (accept) {
        QUERY_REQUIRE(accept_upd(&p, id, 5) == MOQ_OK);
        failures += query_refusal(p.sv, p.spub, MOQ_ERR_WRONG_STATE);
        QUERY_REQUIRE(moq_session_finish_publish(p.sv, p.spub, &fin, 6) == MOQ_OK);
    } else {
        QUERY_REQUIRE(reject_upd(&p, id, 0x5, "declined", 5) == MOQ_OK);
    }
    failures += query_refusal(p.sv, p.spub, MOQ_ERR_STALE_HANDLE);
    pump(&p);
    QUERY_REQUIRE(take(p.c, MOQ_EVENT_PUBLISH_FINISHED, &ev));
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_finished.status_code, accept ? 0 : 0x8);
done:
    moq_event_cleanup(&ev);
    if (p.sp) failures += finish(&p);
    return failures;
}

static int test_query_coalesced(moq_version_t v)
{
    int failures = 0;
    pair_t p;
    QUERY_REQUIRE(mk(&p, v, 0, true));
    QUERY_REQUIRE(send_update(&p, true, false, false, 0, 3) == MOQ_OK);
    pump(&p);
    uint64_t first = UINT64_MAX, newest = UINT64_MAX;
    QUERY_REQUIRE(moq_session_publication_owed_update(p.sv, p.spub, &first, NULL, NULL) == MOQ_OK);
    QUERY_REQUIRE(send_pipelined(&p, false, false, true, 9, 4) == MOQ_OK);
    pump(&p);
    QUERY_REQUIRE(moq_session_publication_owed_update(p.sv, p.spub, &newest, NULL, NULL) == MOQ_OK);
    MOQ_TEST_CHECK(first != newest);
    failures += query_outputs(&p, newest, NULL);
    size_t count = 0;
    MOQ_TEST_CHECK_EQ_U64(take_updates(&p, &count), newest);
    MOQ_TEST_CHECK_EQ_SIZE(count, 2);
    failures += query_outputs(&p, newest, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)accept_upd(&p, first, 5), (int)MOQ_ERR_STALE_HANDLE);
    QUERY_REQUIRE(accept_upd(&p, newest, 6) == MOQ_OK);
    failures += query_refusal(p.sv, p.spub, MOQ_ERR_WRONG_STATE);
    QUERY_REQUIRE(pentry(&p) != NULL);
    MOQ_TEST_CHECK(!pentry(&p)->send_allowed && pentry(&p)->subscriber_priority == 9);
done:
    if (p.sp) failures += finish(&p);
    return failures;
}

/* Draft22 sections9.5/9.20.9: literal REQUEST_UPDATE on the real PUBLISH
 * stream. No publication-location input API is needed for this query. */
static const uint8_t query_empty[] = { 2, 0, 2, 0, 0 };
static const uint8_t query_none[] = { 2, 0, 4, 0, 1, 33, 0 };
static const uint8_t query_relative[] = { 2, 0, 5, 0, 1, 33, 1, 2 };
static const uint8_t query_relative_max[] = {
    2, 0, 13, 0, 1, 33, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t query_object_end[] = { 2, 0, 8, 0, 1, 33, 4, 1, 2, 2, 3 };
static const uint8_t query_object_max[] = {
    2, 0, 32, 0, 1, 33, 4,
    255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
    255, 255, 255, 255, 255, 255, 255, 255, 255 };

typedef struct {
    const uint8_t *wire;
    size_t len;
    bool has_filter;
    moq_location_filter_cfg_t filter;
} query_filter_case_t;

static const query_filter_case_t query_filters[] = {
    { .wire = query_empty, .len = sizeof(query_empty) },
    { .wire = query_none, .len = sizeof(query_none), .has_filter = true,
      .filter = { .struct_size = 48, .kind = MOQ_LOCATION_FILTER_NONE } },
    { .wire = query_relative, .len = sizeof(query_relative), .has_filter = true,
      .filter = { .struct_size = 48, .kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP,
                  .relative_group_offset = 2 } },
    { .wire = query_relative_max, .len = sizeof(query_relative_max), .has_filter = true,
      .filter = { .struct_size = 48, .kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP,
                  .relative_group_offset = UINT64_MAX } },
    { .wire = query_object_end, .len = sizeof(query_object_end), .has_filter = true,
      .filter = { .struct_size = 48, .kind = MOQ_LOCATION_FILTER_OBJECT_END,
                  .start_group = 1, .start_object = 2, .end_group = 3, .end_object = 3 } },
    { .wire = query_object_max, .len = sizeof(query_object_max), .has_filter = true,
      .filter = { .struct_size = 48, .kind = MOQ_LOCATION_FILTER_OBJECT_END,
                  .start_group = UINT64_MAX, .start_object = UINT64_MAX,
                  .end_group = UINT64_MAX, .end_object = UINT64_MAX } },
};

static int test_query_filter(const query_filter_case_t *row)
{
    int failures = 0;
    pair_t p;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    QUERY_REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, 0, true));
    moq_location_filter_cfg_t current, after;
    QUERY_REQUIRE(moq_session_publication_filter(p.sv, p.spub, &current) == MOQ_OK);
    QUERY_REQUIRE(pentry(&p) != NULL);
    QUERY_REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, pentry(&p)->rs.ref,
        row->wire, row->len, false, 3) == MOQ_OK);
    QUERY_REQUIRE(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    failures += query_outputs(&p, 0, row->has_filter ? &row->filter : NULL);
    QUERY_REQUIRE(moq_session_publication_filter(p.sv, p.spub, &after) == MOQ_OK);
    MOQ_TEST_CHECK(memcmp(&current, &after, sizeof(current)) == 0);
    QUERY_REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    MOQ_TEST_CHECK(ev.u.publish_updated.answer_owed);
    MOQ_TEST_CHECK_EQ_U64(ev.u.publish_updated.update_request_id, 0);
    failures += query_outputs(&p, 0, row->has_filter ? &row->filter : NULL);
    MOQ_TEST_CHECK(moq_session_borrow_valid(p.sv, ev.borrow_epoch));
    QUERY_REQUIRE(reject_upd(&p, 0, 0x5, NULL, 4) == MOQ_OK);
    failures += query_refusal(p.sv, p.spub, MOQ_ERR_STALE_HANDLE);
done:
    moq_event_cleanup(&ev);
    if (p.sp) failures += finish(&p);
    return failures;
}

#undef QUERY_REQUIRE

int main(void)
{
    int failures = 0;
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        moq_version_t v = versions[i];
        int before = failures;
        failures += test_accept(v);
        failures += test_reject(v);
        failures += test_not_deferred(v);
        failures += test_goaway_defer_policy(v, false);
        failures += test_goaway_defer_policy(v, true);
        failures += test_setter(v);
        failures += test_coalesced(v, true);
        failures += test_coalesced(v, false);
        failures += test_cancel_while_owed(v);
        failures += test_close_while_owed(v);
        failures += test_would_block(v);
        failures += test_reject_retry(v);
        failures += test_refused(v, REFUSED_NOT_DEFERRED);
        failures += test_refused(v, REFUSED_NOTHING_OWED);
        if (v != MOQ_VERSION_DRAFT_22)   /* draft-22 closes on the second update */
            failures += test_refused(v, REFUSED_OWED);
        failures += test_refused_would_block(v);
        failures += test_reject_stream_count(v, 0);
        failures += test_reject_stream_count(v, 2);
        failures += test_query_lifecycle(v, true);
        failures += test_query_lifecycle(v, false);
        if (v != MOQ_VERSION_DRAFT_22)
            failures += test_query_coalesced(v);
        if (failures != before)
            fprintf(stderr, "  version 0x%llx: %d failures\n", (unsigned long long)v,
                    failures - before);
    }
    failures += test_fill_d22(true);
    failures += test_fill_d22(false);
    for (size_t i = 0; i < sizeof(query_filters) / sizeof(query_filters[0]); i++) {
        int before = failures;
        failures += test_query_filter(&query_filters[i]);
        if (failures != before)
            fprintf(stderr, "  query filter row %zu (selected=%d, kind=%u): %d failures\n",
                    i, (int)query_filters[i].has_filter,
                    (unsigned)query_filters[i].filter.kind, failures - before);
    }
    if (failures) { fprintf(stderr, "FAIL: test_publication_update_answer (%d)\n", failures); return 1; }
    printf("PASS: test_publication_update_answer\n");
    return 0;
}
