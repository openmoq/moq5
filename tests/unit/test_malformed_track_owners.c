/* Who a Malformed Track fails, and that the failure, once detected, is
 * carried to completion (draft-22 §12.1, draft-18 §2.4.2, draft-16 §2.4.2:
 * a subscriber that detects a Malformed Track "MUST cancel any corresponding
 * subscription or fetches for that Track from that publisher").
 *
 * Pinned:
 * - the failure domain is the Track, not the alias or the request that
 *   carried the violation: every SUBSCRIBE (pending ones included), every
 *   PUBLISH (accepted, or still awaiting this side's answer) and every FETCH
 *   of that Track from the peer ends exactly once with the MALFORMED_TRACK
 *   cause, wherever the violation arrived (a subgroup stream, a FETCH
 *   response, a fill stream); a request for another Track is untouched;
 * - a cancellation that meets a full action queue releases the stream's
 *   input and parses nothing more of it; its STOP is issued once actions
 *   drain (whole and streaming receive, SUBSCRIBE and PUBLISH);
 * - the verdict is kept until every affected request has ended: a request
 *   whose cancellation found the event queue full ends once capacity
 *   returns, without the peer sending anything more, and the violating
 *   object is never delivered to it;
 * - a datagram that makes its Track malformed fails the Track even when the
 *   cancellation could not be queued at once;
 * - the per-alias detection history survives one request on the alias
 *   ending while another for the same Track remains, and is discarded when
 *   the alias is reused for another Track;
 * - a parent's malformed terminal and its fill child's terminal are each
 *   delivered exactly once when only one event slot is free;
 * - in streaming receive, a copy of an object that begins on a second
 *   stream while the object is still being surfaced from the first is
 *   consumed without events, so chunk runs never interleave;
 * - draft-16 reads inside Immutable Extensions (§11.2): an Immutable entry
 *   nested in another, or an inner Key-Value-Pair that cannot be parsed,
 *   makes the Track malformed, as an outer pair that cannot be parsed does,
 *   on a subgroup stream (whole, streaming, and arriving a byte at a time),
 *   a datagram and a FETCH response alike; a
 *   valid Immutable block and an odd-typed value whose bytes merely look
 *   like one are delivered.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/fill.h>
#include <moq/codec.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

/* -- Pair --------------------------------------------------------------------- */

typedef struct pr {
    moq_simpair_t *sp;
    moq_session_t *c;    /* subscriber side */
    moq_session_t *sv;   /* publisher side  */
    moq_version_t  v;
    uint64_t       next_stream;
} pr_t;

static bool pr_open(pr_t *p, moq_version_t v, uint32_t max_actions, uint32_t max_events,
                    bool streaming)
{
    memset(p, 0, sizeof(*p));
    moq_simpair_cfg_t c = MOQ_SIMPAIR_CFG_INIT;
    c.alloc = moq_alloc_default();
    c.version = v;
    c.max_actions = max_actions;
    c.max_events = max_events;
    c.client_streaming_objects = streaming;
    c.server_send_request_capacity = true;
    c.server_initial_request_capacity = 64;
    c.client_send_request_capacity = true;
    c.client_initial_request_capacity = 64;
    if (moq_simpair_create(&c, &p->sp) < 0) return false;
    p->c = moq_simpair_client(p->sp);
    p->sv = moq_simpair_server(p->sp);
    p->v = v;
    if (moq_simpair_start(p->sp) < 0) return false;
    moq_simpair_run_until_quiescent(p->sp, 32, NULL);
    p->next_stream = 0x7000;
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED;
}

static void pr_close(pr_t *p)
{
    if (p->sp) moq_simpair_destroy(p->sp);
    memset(p, 0, sizeof(*p));
}

static uint64_t now(const pr_t *p) { return moq_simpair_now_us(p->sp); }
static void pump(pr_t *p) { moq_simpair_run_until_quiescent(p->sp, 64, NULL); }

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
}

static moq_bytes_t k_parts[] = { { (const uint8_t *)"live", 4 } };

static moq_bytes_t name_of(const char *n)
{
    return (moq_bytes_t){ (const uint8_t *)n, strlen(n) };
}

/* -- Requests ----------------------------------------------------------------- */

/* The client subscribes to `name`; with alias != 0 the server accepts it under
 * that alias, otherwise the request stays pending. */
static bool sub_track(pr_t *p, const char *name, uint64_t alias, moq_subscription_t *csub)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ k_parts, 1 };
    cfg.track_name = name_of(name);
    if (moq_session_subscribe(p->c, &cfg, now(p), csub) < 0) return false;
    pump(p);
    moq_event_t ev;
    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    bool got = false;
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (!got && ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            ssub = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    if (alias != 0) {
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        acc.has_track_alias = true;
        acc.track_alias = alias;
        if (moq_session_accept_subscribe(p->sv, ssub, &acc, now(p)) < 0) return false;
        pump(p);
    }
    drain_events(p->c);
    return sub_resolve_handle(p->c, *csub) >= 0;
}

/* The server publishes `name` under `alias`; the client accepts it when
 * `accept`, otherwise its answer stays owed. */
static bool pub_track(pr_t *p, const char *name, uint64_t alias, bool accept,
                      moq_publication_t *cpub)
{
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ k_parts, 1 };
    pc.track_name = name_of(name);
    pc.has_track_alias = true;
    pc.track_alias = alias;
    moq_publication_t spub;
    if (moq_session_publish(p->sv, &pc, now(p), &spub) < 0) return false;
    pump(p);
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (!got && ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            *cpub = ev.u.publish_request.pub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    if (accept) {
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        if (moq_session_accept_publish(p->c, *cpub, &ac, now(p)) < 0) return false;
        pump(p);
        drain_events(p->c);
    }
    drain_events(p->sv);
    return pub_resolve_handle(p->c, *cpub) >= 0;
}

/* The client fetches groups 0..9 of `name`; the server answers it when
 * `accept`. Returns the server's handle through `sfetch` when wanted. */
static bool fetch_track(pr_t *p, const char *name, bool accept, moq_fetch_t *cfetch,
                        moq_fetch_t *sfetch_out)
{
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.track_namespace = (moq_namespace_t){ k_parts, 1 };
    fc.track_name = name_of(name);
    fc.start_group = 0;
    fc.end_group = 9;
    if (moq_session_fetch(p->c, &fc, now(p), cfetch) < 0) return false;
    pump(p);
    moq_fetch_t sfetch = {0};
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (!got && ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            sfetch = ev.u.fetch_request.fetch;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    if (sfetch_out) *sfetch_out = sfetch;
    if (accept) {
        moq_accept_fetch_cfg_t acc;
        moq_accept_fetch_cfg_init(&acc);
        acc.end_group = 9;
        if (moq_session_accept_fetch(p->sv, sfetch, &acc, now(p)) < 0) return false;
        pump(p);
    }
    drain_events(p->c);
    return fetch_resolve_handle(p->c, *cfetch) >= 0;
}

/* -- Wire ------------------------------------------------------------------------ */

typedef struct wire { uint8_t b[256]; size_t n; } wire_t;

static void put(wire_t *w, uint8_t v) { if (w->n < sizeof(w->b)) w->b[w->n++] = v; }

/* PROPERTIES subgroup header with explicit Subgroup ID and Priority. */
static void sg_header(wire_t *w, uint64_t alias, uint8_t group, uint8_t subgroup,
                      uint8_t prio)
{
    put(w, 0x15);
    put(w, (uint8_t)alias); put(w, group); put(w, subgroup); put(w, prio);
}

static void sg_object(wire_t *w, uint8_t delta, const uint8_t *props, size_t plen)
{
    put(w, delta);
    put(w, (uint8_t)plen);
    for (size_t i = 0; i < plen; i++) put(w, props[i]);
    put(w, 0x01); put(w, 'A');
}

/* A property block no draft can parse: an odd type declaring five bytes with
 * one present (draft-22 §10.7, draft-18 §12.7, draft-16 §11.2: "A
 * Key-Value-Pair cannot be parsed"). */
static const uint8_t k_bad[] = { 0x21, 0x05, 0x00 };

static moq_result_t feed(pr_t *p, uint64_t stream, const wire_t *w, bool fin)
{
    return moq_session_on_data_bytes(p->c, moq_stream_ref_from_u64(stream), w->b, w->n,
                                     fin, now(p));
}

static uint64_t new_stream(pr_t *p) { uint64_t r = p->next_stream; p->next_stream += 4; return r; }

/* -- Observation -------------------------------------------------------------------- */

typedef struct term {
    uint32_t kind;
    uint64_t handle;
    uint32_t cause;
    uint64_t code;
} term_t;

typedef struct rec {
    term_t   t[32];
    size_t   n;
    size_t   objects;           /* OBJECT_RECEIVED + begin chunks */
    size_t   fetch_objects;     /* FETCH_OBJECT */
    uint64_t object_owner[32];  /* the sub or pub handle of each object */
    size_t   fill_terminals;
    size_t   stop_data;
    size_t   abort_malformed;
    uint64_t close_code;
} rec_t;

static void rec_init(rec_t *r)
{
    memset(r, 0, sizeof(*r));
    r->close_code = UINT64_MAX;
}

static void rec_term(rec_t *r, uint32_t kind, uint64_t h, uint32_t cause, uint64_t code)
{
    if (r->n < 32) r->t[r->n++] = (term_t){ kind, h, cause, code };
}

static void rec_object(rec_t *r, moq_subscription_t sub, moq_publication_t pub)
{
    if (r->objects < 32)
        r->object_owner[r->objects] = sub._opaque ? sub._opaque : pub._opaque;
    r->objects++;
}

static void rec_events(moq_session_t *s, rec_t *r)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_OBJECT_RECEIVED:
            rec_object(r, ev.u.object_received.sub, ev.u.object_received.pub);
            break;
        case MOQ_EVENT_OBJECT_CHUNK:
            if (ev.u.object_chunk.begin)
                rec_object(r, ev.u.object_chunk.sub, ev.u.object_chunk.pub);
            break;
        case MOQ_EVENT_FETCH_OBJECT:
            r->fetch_objects++;
            break;
        case MOQ_EVENT_UNSUBSCRIBED:
            rec_term(r, ev.kind, ev.u.unsubscribed.sub._opaque, ev.u.unsubscribed.cause,
                     ev.u.unsubscribed.stream_error_code);
            break;
        case MOQ_EVENT_PUBLISH_FINISHED:
            rec_term(r, ev.kind, ev.u.publish_finished.pub._opaque,
                     ev.u.publish_finished.cause, ev.u.publish_finished.stream_error_code);
            break;
        case MOQ_EVENT_FETCH_RESET:
            rec_term(r, ev.kind, ev.u.fetch_reset.fetch._opaque, ev.u.fetch_reset.cause,
                     ev.u.fetch_reset.error_code);
            break;
        case MOQ_EVENT_FILL_TERMINAL:
            r->fill_terminals++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static void rec_actions(moq_session_t *s, rec_t *r)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_STOP_DATA) r->stop_data++;
        if (a.kind == MOQ_ACTION_ABORT_BIDI_STREAM &&
            a.u.abort_bidi_stream.error_code == MOQ_STREAM_ERROR_MALFORMED_TRACK)
            r->abort_malformed++;
        if (a.kind == MOQ_ACTION_CLOSE_SESSION && r->close_code == UINT64_MAX)
            r->close_code = a.u.close_session.code;
        moq_action_cleanup(&a);
    }
}

/* Terminals for one handle, and whether each carried the malformed cause. */
static int terms_for(const rec_t *r, uint32_t kind, uint64_t h, bool *all_malformed)
{
    int n = 0;
    *all_malformed = true;
    for (size_t i = 0; i < r->n; i++) {
        if (r->t[i].kind != kind || r->t[i].handle != h) continue;
        n++;
        if (r->t[i].cause != MOQ_REQUEST_TERMINAL_MALFORMED_TRACK) *all_malformed = false;
    }
    return n;
}

static void expect_one(const char *what, const rec_t *r, uint32_t kind, uint64_t h)
{
    bool mal = false;
    int n = terms_for(r, kind, h, &mal);
    if (n != 1 || !mal) {
        fprintf(stderr, "FAIL %s: %d terminal(s), malformed cause %s\n", what, n,
                mal ? "yes" : "no");
        failures++;
    }
}

static void expect_none(const char *what, const rec_t *r, uint32_t kind, uint64_t h)
{
    bool mal = false;
    int n = terms_for(r, kind, h, &mal);
    if (n != 0) {
        fprintf(stderr, "FAIL %s: %d unexpected terminal(s)\n", what, n);
        failures++;
    }
}

static size_t objects_for(const rec_t *r, uint64_t h)
{
    size_t n = 0;
    for (size_t i = 0; i < r->objects && i < 32; i++)
        if (r->object_owner[i] == h) n++;
    return n;
}

/* Queue `n` ordinary actions on the client: SUBSCRIBEs for unrelated Tracks,
 * left unanswered and unpolled. */
static bool fill_client_actions(pr_t *p, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        char nm[24];
        snprintf(nm, sizeof(nm), "x%zu", i);
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ k_parts, 1 };
        cfg.track_name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
        moq_subscription_t h;
        if (moq_session_subscribe(p->c, &cfg, now(p), &h) < 0) return false;
    }
    return true;
}

static size_t free_actions(const moq_session_t *s)
{
    return s->action_cap - (s->action_tail - s->action_head);
}

/* -- One free action: the cancelled stream's input is released ----------------------- */

/* A subscription (or PUBLISH) whose cancellation takes the last free action:
 * its stream's STOP stays owed and its input is released. The header and a
 * valid object were consumed from the same feed, so the parse position is
 * past the start of the input when the violating object is found. */
static void test_one_free_action(bool publish, bool streaming)
{
    const char *what = publish ? (streaming ? "PUBLISH streaming" : "PUBLISH whole")
                               : (streaming ? "SUBSCRIBE streaming" : "SUBSCRIBE whole");
    pr_t p;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 6, 0, streaming)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t cpub = MOQ_PUBLICATION_INVALID;
    bool ok = publish ? pub_track(&p, "v", 7, true, &cpub) : sub_track(&p, "v", 7, &csub);
    MOQ_TEST_CHECK(ok);
    drain_actions(p.c);
    MOQ_TEST_CHECK(fill_client_actions(&p, free_actions(p.c) - 1));
    MOQ_TEST_CHECK_EQ_SIZE(free_actions(p.c), 1);

    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 0);
    sg_object(&w, 0, NULL, 0);
    sg_object(&w, 0, k_bad, sizeof(k_bad));
    uint64_t stream = new_stream(&p);
    moq_result_t rc = feed(&p, stream, &w, false);
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    if (publish) {
        expect_one(what, &r, MOQ_EVENT_PUBLISH_FINISHED, cpub._opaque);
        MOQ_TEST_CHECK(pub_resolve_handle(p.c, cpub) < 0);
    } else {
        expect_one(what, &r, MOQ_EVENT_UNSUBSCRIBED, csub._opaque);
        MOQ_TEST_CHECK(sub_resolve_handle(p.c, csub) < 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, 1);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);

    /* Nothing more of the stream is parsed; its STOP follows once actions
     * drain, issued without further input. */
    rec_actions(p.c, &r);
    MOQ_TEST_CHECK_EQ_SIZE(r.abort_malformed, 1);
    rec_actions(p.c, &r);
    if (r.stop_data != 1) {
        fprintf(stderr, "FAIL %s: %zu STOP(s) for the cancelled stream\n", what, r.stop_data);
        failures++;
    }
    MOQ_TEST_CHECK(r.close_code == UINT64_MAX);
    /* Later bytes on the stream are consumed, not delivered. */
    wire_t more = {0};
    sg_object(&more, 0, NULL, 0);
    (void)feed(&p, stream, &more, true);
    rec_events(p.c, &r);
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, 1);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    pr_close(&p);
}

/* -- The Track, not the alias or the request ----------------------------------------- */

typedef struct track_set {
    moq_subscription_t a, b, pending, u;
    moq_publication_t  c, pending_pub;
    moq_fetch_t        f1, f2;
    moq_fetch_t        sf1;
} track_set_t;

/* Draft-22: SUBSCRIBE A and B for one Track under different aliases, a
 * pending SUBSCRIBE for it, an accepted PUBLISH of it and one still awaiting
 * an answer, an answered and an unanswered FETCH of it, and a SUBSCRIBE U for
 * another Track. */
static bool track_set_open(pr_t *p, track_set_t *t)
{
    memset(t, 0, sizeof(*t));
    return sub_track(p, "v", 7, &t->a) && sub_track(p, "v", 8, &t->b) &&
           sub_track(p, "v", 0, &t->pending) && pub_track(p, "v", 9, true, &t->c) &&
           pub_track(p, "v", 10, false, &t->pending_pub) &&
           fetch_track(p, "v", true, &t->f1, &t->sf1) &&
           fetch_track(p, "v", false, &t->f2, NULL) && sub_track(p, "u", 11, &t->u);
}

static void expect_track_failed(const char *what, pr_t *p, const track_set_t *t,
                                rec_t *r)
{
    char buf[128];
    snprintf(buf, sizeof(buf), "%s: SUBSCRIBE A", what);
    expect_one(buf, r, MOQ_EVENT_UNSUBSCRIBED, t->a._opaque);
    snprintf(buf, sizeof(buf), "%s: SUBSCRIBE B (other alias)", what);
    expect_one(buf, r, MOQ_EVENT_UNSUBSCRIBED, t->b._opaque);
    snprintf(buf, sizeof(buf), "%s: pending SUBSCRIBE", what);
    expect_one(buf, r, MOQ_EVENT_UNSUBSCRIBED, t->pending._opaque);
    snprintf(buf, sizeof(buf), "%s: PUBLISH", what);
    expect_one(buf, r, MOQ_EVENT_PUBLISH_FINISHED, t->c._opaque);
    snprintf(buf, sizeof(buf), "%s: unanswered PUBLISH", what);
    expect_one(buf, r, MOQ_EVENT_PUBLISH_FINISHED, t->pending_pub._opaque);
    snprintf(buf, sizeof(buf), "%s: answered FETCH", what);
    expect_one(buf, r, MOQ_EVENT_FETCH_RESET, t->f1._opaque);
    snprintf(buf, sizeof(buf), "%s: unanswered FETCH", what);
    expect_one(buf, r, MOQ_EVENT_FETCH_RESET, t->f2._opaque);
    snprintf(buf, sizeof(buf), "%s: other Track", what);
    expect_none(buf, r, MOQ_EVENT_UNSUBSCRIBED, t->u._opaque);
    MOQ_TEST_CHECK(sub_resolve_handle(p->c, t->a) < 0);
    MOQ_TEST_CHECK(sub_resolve_handle(p->c, t->b) < 0);
    MOQ_TEST_CHECK(sub_resolve_handle(p->c, t->pending) < 0);
    MOQ_TEST_CHECK(pub_resolve_handle(p->c, t->c) < 0);
    MOQ_TEST_CHECK(pub_resolve_handle(p->c, t->pending_pub) < 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p->c, t->f1) < 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p->c, t->f2) < 0);
    MOQ_TEST_CHECK(sub_resolve_handle(p->c, t->u) >= 0);
    MOQ_TEST_CHECK(r->close_code == UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);

    /* The other Track keeps flowing. */
    wire_t w = {0};
    sg_header(&w, 11, 1, 0, 0);
    sg_object(&w, 0, NULL, 0);
    (void)feed(p, new_stream(p), &w, false);
    rec_t u;
    rec_init(&u);
    rec_events(p->c, &u);
    if (objects_for(&u, t->u._opaque) != 1) {
        fprintf(stderr, "FAIL %s: the other Track delivered %zu object(s)\n", what,
                objects_for(&u, t->u._opaque));
        failures++;
    }
}

static void test_track_domain_from_subgroup(void)
{
    pr_t p;
    track_set_t t;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false) || !track_set_open(&p, &t)) {
        fprintf(stderr, "FAIL track set fixture\n"); failures++; pr_close(&p); return;
    }
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 0);
    sg_object(&w, 0, k_bad, sizeof(k_bad));
    (void)feed(&p, new_stream(&p), &w, false);
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    rec_actions(p.c, &r);
    rec_events(p.c, &r);
    expect_track_failed("subgroup violation", &p, &t, &r);
    pr_close(&p);
}

static void test_track_domain_from_fetch(void)
{
    pr_t p;
    track_set_t t;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false) || !track_set_open(&p, &t)) {
        fprintf(stderr, "FAIL track set fixture\n"); failures++; pr_close(&p); return;
    }
    int fs = fetch_resolve_handle(p.c, t.f1);
    MOQ_TEST_CHECK(fs >= 0 && p.c->fetches[fs].data_stream_started);
    if (fs < 0) { pr_close(&p); return; }
    /* A FETCH object {1,4} with an unparseable property block. */
    static const uint8_t bad_obj[] = { 0x3c, 0x01, 0x04, 0x00, 0x04, 0x21, 0x05, 0x00,
                                       0x00, 0x00, 0x01, 'A' };
    (void)moq_session_on_data_bytes(p.c, p.c->fetches[fs].data_stream_ref, bad_obj,
                                    sizeof(bad_obj), false, now(&p));
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    rec_actions(p.c, &r);
    rec_events(p.c, &r);
    expect_track_failed("FETCH response violation", &p, &t, &r);
    pr_close(&p);
}

/* Draft-16 and draft-18 keep one subscription per Track; the SUBSCRIBE and a
 * FETCH of the same Track end together, another Track's does not. */
static void test_track_domain_older(moq_version_t v)
{
    pr_t p;
    moq_subscription_t a, u;
    moq_fetch_t f;
    if (!pr_open(&p, v, 0, 0, false) || !sub_track(&p, "v", 7, &a) ||
        !fetch_track(&p, "v", true, &f, NULL) || !sub_track(&p, "u", 11, &u)) {
        fprintf(stderr, "FAIL d%u fixture\n", (unsigned)v); failures++; pr_close(&p); return;
    }
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 0);
    sg_object(&w, 0, k_bad, sizeof(k_bad));
    (void)feed(&p, new_stream(&p), &w, false);
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    rec_actions(p.c, &r);
    rec_events(p.c, &r);
    char what[64];
    snprintf(what, sizeof(what), "d%u SUBSCRIBE", (unsigned)v);
    expect_one(what, &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque);
    snprintf(what, sizeof(what), "d%u FETCH of the same Track", (unsigned)v);
    expect_one(what, &r, MOQ_EVENT_FETCH_RESET, f._opaque);
    snprintf(what, sizeof(what), "d%u other Track", (unsigned)v);
    expect_none(what, &r, MOQ_EVENT_UNSUBSCRIBED, u._opaque);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, u) >= 0);
    MOQ_TEST_CHECK(r.close_code == UINT64_MAX);
    pr_close(&p);
}

/* -- The verdict outlives partial retirement ----------------------------------------- */

/* A and B share one alias. The first object of Subgroup {1,0} arrives at
 * priority 1; a second stream for that Subgroup at priority 2 is condition 1.
 * With one free event slot the first cancellation fills the queue; the
 * second completes on a later advancing call with no further input, and the
 * violating object is delivered to neither. */
static void test_verdict_survives_partial_retirement(void)
{
    pr_t p;
    moq_subscription_t a, b;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 4, false) || !sub_track(&p, "v", 7, &a) ||
        !sub_track(&p, "v", 7, &b)) {
        fprintf(stderr, "FAIL shared alias fixture\n"); failures++; pr_close(&p); return;
    }
    drain_actions(p.c);
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 1);
    sg_object(&w, 0, NULL, 0);
    uint64_t first = new_stream(&p);
    (void)feed(&p, first, &w, false);
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, 2);   /* one object, both subscriptions */

    /* Two more objects fill the queue; polling one leaves one slot free. */
    wire_t more = {0};
    sg_object(&more, 0, NULL, 0);
    (void)feed(&p, first, &more, false);
    wire_t g2 = {0};
    sg_header(&g2, 7, 2, 0, 1);
    sg_object(&g2, 0, NULL, 0);
    (void)feed(&p, new_stream(&p), &g2, false);
    MOQ_TEST_CHECK(event_queue_full(p.c));
    rec_t r2;
    rec_init(&r2);
    moq_event_t ev;
    if (moq_session_poll_events(p.c, &ev, 1) > 0) {
        rec_object(&r2, ev.u.object_received.sub, ev.u.object_received.pub);
        moq_event_cleanup(&ev);
    }

    wire_t bad = {0};
    sg_header(&bad, 7, 1, 0, 2);
    sg_object(&bad, 2, NULL, 0);
    uint64_t second = new_stream(&p);
    (void)feed(&p, second, &bad, false);
    rec_events(p.c, &r2);
    rec_actions(p.c, &r2);
    /* No further input: advancing calls with nothing to deliver. */
    for (int i = 0; i < 2; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
        rec_events(p.c, &r2);
        rec_actions(p.c, &r2);
    }
    /* A re-drive of the violating stream changes nothing. */
    wire_t none = {0};
    (void)feed(&p, second, &none, false);
    rec_events(p.c, &r2);
    expect_one("first subscription", &r2, MOQ_EVENT_UNSUBSCRIBED, a._opaque);
    expect_one("second subscription", &r2, MOQ_EVENT_UNSUBSCRIBED, b._opaque);
    if (r2.objects != 4) {
        fprintf(stderr, "FAIL shared alias: %zu object event(s), want the 4 queued "
                "before the violation\n", r2.objects);
        failures++;
    }
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) < 0 && sub_resolve_handle(p.c, b) < 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    pr_close(&p);
}

/* One of two subscriptions sharing an alias ends normally: the other keeps
 * the Track's history, so a later priority change in a Subgroup already seen
 * is still condition 1. */
static void test_history_survives_owner_turnover(void)
{
    pr_t p;
    moq_subscription_t a, b;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false) || !sub_track(&p, "v", 7, &a) ||
        !sub_track(&p, "v", 7, &b)) {
        fprintf(stderr, "FAIL turnover fixture\n"); failures++; pr_close(&p); return;
    }
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 1);
    sg_object(&w, 0, NULL, 0);
    (void)feed(&p, new_stream(&p), &w, true);
    drain_events(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, a, now(&p)), (int)MOQ_OK);
    drain_events(p.c);
    drain_actions(p.c);
    wire_t bad = {0};
    sg_header(&bad, 7, 1, 0, 2);
    sg_object(&bad, 1, NULL, 0);
    (void)feed(&p, new_stream(&p), &bad, false);
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    expect_one("remaining subscription keeps the history", &r, MOQ_EVENT_UNSUBSCRIBED,
               b._opaque);
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, 0);
    pr_close(&p);
}

/* The alias is released and reused for another Track: that Track starts with
 * no history, so the same Subgroup at another priority is not malformed. */
static void test_history_discarded_on_alias_reuse(void)
{
    pr_t p;
    moq_subscription_t a, d;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL reuse fixture\n"); failures++; pr_close(&p); return;
    }
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 1);
    sg_object(&w, 0, NULL, 0);
    (void)feed(&p, new_stream(&p), &w, true);
    drain_events(p.c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, a, now(&p)), (int)MOQ_OK);
    pump(&p);
    drain_events(p.c);
    drain_events(p.sv);
    MOQ_TEST_CHECK(sub_track(&p, "w", 7, &d));
    wire_t other = {0};
    sg_header(&other, 7, 1, 0, 2);
    sg_object(&other, 1, NULL, 0);
    (void)feed(&p, new_stream(&p), &other, false);
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    expect_none("another Track on the reused alias", &r, MOQ_EVENT_UNSUBSCRIBED, d._opaque);
    MOQ_TEST_CHECK_EQ_SIZE(objects_for(&r, d._opaque), 1);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, d) >= 0);
    pr_close(&p);
}

/* -- Draft-16 Immutable Extensions ------------------------------------------------------ */

typedef struct d16_block {
    const char   *name;
    uint8_t       b[8];
    size_t        n;
    bool          malformed;
} d16_block_t;

static const d16_block_t k_d16_blocks[] = {
    /* Immutable (0x0B) Length 3 holding Immutable Length 1 00. */
    { "nested immutable", { 0x0B, 0x03, 0x0B, 0x01, 0x00 }, 5, true },
    /* Immutable Length 2 holding an odd type declaring five bytes, none present. */
    { "truncated inner pair", { 0x0B, 0x02, 0x21, 0x05 }, 4, true },
    /* An odd type declaring five bytes with one present, outside Immutable. */
    { "truncated outer pair", { 0x21, 0x05, 0x00 }, 3, true },
    /* Immutable Length 2 holding Type 0x02 = 5. */
    { "valid immutable", { 0x0B, 0x02, 0x02, 0x05 }, 4, false },
    /* Type 0x21 with three opaque bytes spelling an Immutable entry. */
    { "opaque odd value", { 0x21, 0x03, 0x0B, 0x01, 0x00 }, 5, false },
};

static void d16_expect(const char *family, const d16_block_t *k, const rec_t *r,
                       uint32_t kind, uint64_t h, size_t delivered, uint64_t other)
{
    char what[96];
    snprintf(what, sizeof(what), "d16 %s, %s", family, k->name);
    if (k->malformed) {
        expect_one(what, r, kind, h);
    } else {
        expect_none(what, r, kind, h);
    }
    if (delivered != (k->malformed ? 0u : 1u)) {
        fprintf(stderr, "FAIL %s: %zu object(s) delivered\n", what, delivered);
        failures++;
    }
    snprintf(what, sizeof(what), "d16 %s, %s: other Track", family, k->name);
    expect_none(what, r, MOQ_EVENT_UNSUBSCRIBED, other);
    if (r->close_code != UINT64_MAX) {
        fprintf(stderr, "FAIL %s: session closed 0x%llx\n", what,
                (unsigned long long)r->close_code);
        failures++;
    }
}

/* A subgroup stream: `split` feeds it one byte at a time, and no verdict is
 * reached before the property block's last byte has arrived. */
static void test_d16_immutable_subgroup(bool streaming, bool split)
{
    const char *family = split ? (streaming ? "subgroup, streaming, bytewise" : "subgroup, bytewise")
                               : (streaming ? "subgroup, streaming" : "subgroup");
    for (size_t i = 0; i < sizeof(k_d16_blocks) / sizeof(k_d16_blocks[0]); i++) {
        const d16_block_t *k = &k_d16_blocks[i];
        pr_t p;
        moq_subscription_t a, u;
        if (!pr_open(&p, MOQ_VERSION_DRAFT_16, 0, 0, streaming) || !sub_track(&p, "v", 7, &a) ||
            !sub_track(&p, "u", 11, &u)) {
            fprintf(stderr, "FAIL d16 %s fixture\n", family); failures++; pr_close(&p); return;
        }
        wire_t w = {0};
        sg_header(&w, 7, 1, 0, 0);
        size_t block_end = w.n + 2 + k->n;   /* object delta, Length, block */
        sg_object(&w, 0, k->b, k->n);
        uint64_t sid = new_stream(&p);
        rec_t r;
        rec_init(&r);
        if (split) {
            for (size_t b = 0; b < w.n; b++) {
                (void)moq_session_on_data_bytes(p.c, moq_stream_ref_from_u64(sid), &w.b[b], 1,
                                                false, now(&p));
                rec_events(p.c, &r);
                rec_actions(p.c, &r);
                if (b + 1 < block_end && r.n != 0) {
                    fprintf(stderr, "FAIL d16 %s, %s: verdict after %zu of %zu bytes\n", family,
                            k->name, b + 1, block_end);
                    failures++;
                }
            }
        } else {
            (void)feed(&p, sid, &w, false);
        }
        rec_events(p.c, &r);
        rec_actions(p.c, &r);
        rec_events(p.c, &r);
        d16_expect(family, k, &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque, objects_for(&r, a._opaque),
                   u._opaque);
        MOQ_TEST_CHECK((sub_resolve_handle(p.c, a) >= 0) == !k->malformed);
        MOQ_TEST_CHECK(sub_resolve_handle(p.c, u) >= 0);
        pr_close(&p);
    }
}

static void test_d16_immutable_datagram(void)
{
    for (size_t i = 0; i < sizeof(k_d16_blocks) / sizeof(k_d16_blocks[0]); i++) {
        const d16_block_t *k = &k_d16_blocks[i];
        pr_t p;
        moq_subscription_t a, u;
        if (!pr_open(&p, MOQ_VERSION_DRAFT_16, 0, 0, false) || !sub_track(&p, "v", 7, &a) ||
            !sub_track(&p, "u", 11, &u)) {
            fprintf(stderr, "FAIL d16 datagram fixture\n"); failures++; pr_close(&p); return;
        }
        /* Extensions datagram on alias 7, {2,5}, priority 3, payload "hi". */
        wire_t w = {0};
        put(&w, MOQ_D16_DGRAM_FLAG_EXTENSIONS); put(&w, 7); put(&w, 2); put(&w, 5); put(&w, 3);
        put(&w, (uint8_t)k->n);
        for (size_t b = 0; b < k->n; b++) put(&w, k->b[b]);
        put(&w, 'h'); put(&w, 'i');
        (void)moq_session_on_datagram(p.c, w.b, w.n, now(&p));
        rec_t r;
        rec_init(&r);
        rec_events(p.c, &r);
        rec_actions(p.c, &r);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
        rec_events(p.c, &r);
        d16_expect("datagram", k, &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque,
                   objects_for(&r, a._opaque), u._opaque);
        MOQ_TEST_CHECK(sub_resolve_handle(p.c, u) >= 0);
        pr_close(&p);
    }
}

static void test_d16_immutable_fetch(void)
{
    for (size_t i = 0; i < sizeof(k_d16_blocks) / sizeof(k_d16_blocks[0]); i++) {
        const d16_block_t *k = &k_d16_blocks[i];
        pr_t p;
        moq_subscription_t u;
        moq_fetch_t f;
        if (!pr_open(&p, MOQ_VERSION_DRAFT_16, 0, 0, false) ||
            !fetch_track(&p, "v", true, &f, NULL) || !sub_track(&p, "u", 11, &u)) {
            fprintf(stderr, "FAIL d16 FETCH fixture\n"); failures++; pr_close(&p); return;
        }
        int fs = fetch_resolve_handle(p.c, f);
        MOQ_TEST_CHECK(fs >= 0);
        if (fs < 0) { pr_close(&p); return; }
        MOQ_TEST_CHECK(p.c->fetches[fs].data_stream_started);
        /* One object {1, 0, 4} with every field present, on the response's
         * data stream (its FETCH_HEADER already read). */
        wire_t w = {0};
        put(&w, MOQ_D16_FETCH_FLAG_GROUP_ID | MOQ_D16_FETCH_FLAG_OBJECT_ID |
                    MOQ_D16_FETCH_FLAG_PRIORITY | MOQ_D16_FETCH_FLAG_EXTENSIONS |
                    MOQ_D16_FETCH_FLAG_SUBGROUP_MASK);
        put(&w, 1); put(&w, 0); put(&w, 4); put(&w, 0x80);
        put(&w, (uint8_t)k->n);
        for (size_t b = 0; b < k->n; b++) put(&w, k->b[b]);
        put(&w, 0x01); put(&w, 'A');
        (void)moq_session_on_data_bytes(p.c, p.c->fetches[fs].data_stream_ref, w.b, w.n, false,
                                        now(&p));
        rec_t r;
        rec_init(&r);
        rec_events(p.c, &r);
        rec_actions(p.c, &r);
        rec_events(p.c, &r);
        d16_expect("FETCH", k, &r, MOQ_EVENT_FETCH_RESET, f._opaque, r.fetch_objects,
                   u._opaque);
        MOQ_TEST_CHECK((fetch_resolve_handle(p.c, f) >= 0) == !k->malformed);
        pr_close(&p);
    }
}

static void t_d16_sg(void) { test_d16_immutable_subgroup(false, false); }
static void t_d16_sg_split(void) { test_d16_immutable_subgroup(false, true); }
static void t_d16_sg_stream(void) { test_d16_immutable_subgroup(true, false); }
static void t_d16_sg_stream_split(void) { test_d16_immutable_subgroup(true, true); }

/* -- Datagrams under pressure ------------------------------------------------------------ */

/* PROPERTIES datagram on alias 7, {2,5}, with an unparseable block. */
static const uint8_t k_bad_dg[] = { 0x01, 7, 0x02, 0x05, 0x03, 0x03, 0x21, 0x05, 0x00, 'h', 'i' };
static const uint8_t k_good_dg[] = { 0x00, 7, 0x03, 0x00, 0x03, 'h' };

static void test_datagram_under_pressure(bool action_pressure, bool later_data)
{
    const char *what = action_pressure ? "datagram, full action queue"
                                       : "datagram, full event queue";
    pr_t p;
    moq_subscription_t a;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, action_pressure ? 6 : 0,
                 action_pressure ? 0 : 2, false) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    drain_actions(p.c);
    if (action_pressure) {
        MOQ_TEST_CHECK(fill_client_actions(&p, free_actions(p.c)));
    } else {
        /* Two ordinary objects fill the event queue. */
        static const uint8_t g0[] = { 0x00, 7, 0x01, 0x00, 0x03, 'a' };
        static const uint8_t g1[] = { 0x00, 7, 0x01, 0x01, 0x03, 'b' };
        (void)moq_session_on_datagram(p.c, g0, sizeof(g0), now(&p));
        (void)moq_session_on_datagram(p.c, g1, sizeof(g1), now(&p));
        MOQ_TEST_CHECK(event_queue_full(p.c));
    }
    (void)moq_session_on_datagram(p.c, k_bad_dg, sizeof(k_bad_dg), now(&p));
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    rec_actions(p.c, &r);
    if (later_data)
        (void)moq_session_on_datagram(p.c, k_good_dg, sizeof(k_good_dg), now(&p));
    else
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
    rec_events(p.c, &r);
    rec_actions(p.c, &r);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
    rec_events(p.c, &r);
    expect_one(what, &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) < 0);
    /* Only the objects before the violation were delivered. */
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, action_pressure ? 0u : 2u);
    MOQ_TEST_CHECK(r.close_code == UINT64_MAX);
    pr_close(&p);
}

/* -- Parent and fill child under one free event slot ---------------------------------- */

static void test_fill_parent_terminal(bool publish)
{
    const char *what = publish ? "PUBLISH with a fill" : "SUBSCRIBE with a fill";
    pr_t p;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 4, false)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t cpub = MOQ_PUBLICATION_INVALID;
    if (publish) {
        MOQ_TEST_CHECK(pub_track(&p, "v", 7, true, &cpub));
        /* The publisher's Largest, for the fill to resolve against. */
        int sps = -1;
        for (size_t i = 0; i < p.sv->pub_cap; i++)
            if (p.sv->publishes[i].role == MOQ_PUB_ROLE_PUBLISHER &&
                p.sv->publishes[i].state == MOQ_PUB_ESTABLISHED) sps = (int)i;
        MOQ_TEST_CHECK(sps >= 0);
        if (sps >= 0) {
            moq_subgroup_cfg_t sg;
            moq_subgroup_cfg_init(&sg);
            sg.group_id = 3;
            sg.publisher_priority = 1;
            sg.object_properties = true;
            moq_subgroup_handle_t sgh;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(
                                      p.sv, p.sv->publishes[sps].handle, &sg, now(&p), &sgh),
                                  (int)MOQ_OK);
            moq_rcbuf_t *pl = NULL;
            (void)moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"o", 1, &pl);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(p.sv, sgh, 0, pl, now(&p)),
                                  (int)MOQ_OK);
            moq_rcbuf_decref(pl);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(p.sv, sgh, now(&p)),
                                  (int)MOQ_OK);
            pump(&p);
        }
        drain_events(p.c);
        drain_events(p.sv);
        moq_publication_update_cfg_t up;
        moq_publication_update_cfg_init(&up);
        up.fill = &fill;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(p.c, cpub, &up, now(&p)),
                              (int)MOQ_OK);
    } else {
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ k_parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("v");
        cfg.fill = &fill;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &cfg, now(&p), &csub),
                              (int)MOQ_OK);
        pump(&p);
        moq_event_t ev;
        moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ssub = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.has_track_alias = true;
        ac.track_alias = 7;
        ac.has_largest = true;
        ac.largest_group = 3;
        ac.largest_object = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(p.sv, ssub, &ac, now(&p)),
                              (int)MOQ_OK);
    }
    pump(&p);
    drain_events(p.c);
    drain_actions(p.c);
    MOQ_TEST_CHECK(p.c->fills.live == 1);
    /* Three ordinary objects leave one event slot free. */
    wire_t w = {0};
    sg_header(&w, 7, 4, 0, 1);
    for (int i = 0; i < 3; i++) sg_object(&w, 0, NULL, 0);
    uint64_t stream = new_stream(&p);
    (void)feed(&p, stream, &w, false);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->event_tail - p.c->event_head, 3);
    wire_t bad = {0};
    sg_object(&bad, 0, k_bad, sizeof(k_bad));
    (void)feed(&p, stream, &bad, false);
    rec_t r;
    rec_init(&r);
    for (int round = 0; round < 3; round++) {
        rec_events(p.c, &r);
        rec_actions(p.c, &r);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
    }
    rec_events(p.c, &r);
    if (publish) {
        expect_one(what, &r, MOQ_EVENT_PUBLISH_FINISHED, cpub._opaque);
        MOQ_TEST_CHECK(pub_resolve_handle(p.c, cpub) < 0);
    } else {
        expect_one(what, &r, MOQ_EVENT_UNSUBSCRIBED, csub._opaque);
        MOQ_TEST_CHECK(sub_resolve_handle(p.c, csub) < 0);
    }
    if (r.fill_terminals != 1) {
        fprintf(stderr, "FAIL %s: %zu fill terminal(s)\n", what, r.fill_terminals);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, 3);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->fills.live, 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    pr_close(&p);
}


/* A violation inside a fill's response stream: the fill carries its
 * parent's Track, so every request for that Track fails -- the parent and a
 * second subscription sharing the parent's alias -- while another Track's
 * subscription goes on. */
static void test_violation_inside_fill(void)
{
    pr_t p;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false)) {
        fprintf(stderr, "FAIL fill violation fixture\n"); failures++; pr_close(&p); return;
    }
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ k_parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.fill = &fill;
    moq_subscription_t a = MOQ_SUBSCRIPTION_INVALID, b, u;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(p.c, &cfg, now(&p), &a), (int)MOQ_OK);
    pump(&p);
    moq_event_t ev;
    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_track_alias = true;
    ac.track_alias = 7;
    ac.has_largest = true;
    ac.largest_group = 3;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(p.sv, ssub, &ac, now(&p)),
                          (int)MOQ_OK);
    pump(&p);
    moq_fill_t sfill = {0};
    bool got = false;
    while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_REQUEST && !got) {
            sfill = ev.u.fill_request.fill;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(got);
    MOQ_TEST_CHECK(sub_track(&p, "v", 7, &b));       /* shares the alias */
    MOQ_TEST_CHECK(sub_track(&p, "u", 11, &u));      /* another Track    */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(p.sv, sfill, now(&p)), (int)MOQ_OK);
    pump(&p);
    drain_events(p.c);
    drain_actions(p.c);
    int fs = -1;
    for (size_t i = 0; i < p.c->rx_cap; i++)
        if (p.c->rx_streams[i].active && p.c->rx_streams[i].fill_bound) fs = (int)i;
    MOQ_TEST_CHECK(fs >= 0);
    if (fs < 0) { pr_close(&p); return; }
    /* One FETCH-form object {1,4} whose property block cannot be parsed. */
    static const uint8_t bad_obj[] = { 0x3c, 0x01, 0x04, 0x00, 0x04, 0x21, 0x05, 0x00,
                                       0x00, 0x01, 'A' };
    (void)moq_session_on_data_bytes(p.c, p.c->rx_streams[fs].stream_ref, bad_obj,
                                    sizeof(bad_obj), false, now(&p));
    rec_t r;
    rec_init(&r);
    for (int i = 0; i < 3; i++) {
        rec_events(p.c, &r);
        rec_actions(p.c, &r);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
    }
    rec_events(p.c, &r);
    expect_one("fill parent", &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque);
    expect_one("subscription sharing the parent's alias", &r, MOQ_EVENT_UNSUBSCRIBED,
               b._opaque);
    expect_none("other Track", &r, MOQ_EVENT_UNSUBSCRIBED, u._opaque);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, u) >= 0);
    MOQ_TEST_CHECK_EQ_SIZE(r.fill_terminals, 1);
    MOQ_TEST_CHECK(r.close_code == UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    pr_close(&p);
}

/* -- Two streams carrying one object ---------------------------------------------- */

/* A publisher that resets a subgroup stream mid-object and reopens the
 * subgroup on a new stream may resend that object; the receiver can see the
 * new stream's copy before the old stream's remaining bytes or its reset. In
 * streaming receive the chunks of one object identity (group, subgroup,
 * object) must not interleave from two streams: a consumer routes chunks by
 * that identity. The copy that begins while the same object is still in
 * flight on another stream of the Track is consumed without events; the
 * stream's later objects are delivered. */
static void test_copy_in_flight_not_interleaved(void)
{
    pr_t p;
    moq_subscription_t a;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, true) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL in-flight copy fixture\n"); failures++; pr_close(&p); return;
    }
    /* Stream 1: object 1 of Subgroup {1,0}, six bytes, three sent. */
    wire_t w1 = {0};
    sg_header(&w1, 7, 1, 0, 0);
    put(&w1, 1); put(&w1, 0); put(&w1, 6); put(&w1, 'a'); put(&w1, 'b'); put(&w1, 'c');
    uint64_t s1 = new_stream(&p);
    (void)feed(&p, s1, &w1, false);
    /* Stream 2: the same object again, whole, then object 2. */
    wire_t w2 = {0};
    sg_header(&w2, 7, 1, 0, 0);
    put(&w2, 1); put(&w2, 0); put(&w2, 6);
    for (int i = 0; i < 6; i++) put(&w2, 'X');
    put(&w2, 0); put(&w2, 0); put(&w2, 1); put(&w2, 'n');
    uint64_t s2 = new_stream(&p);
    (void)feed(&p, s2, &w2, false);
    /* Stream 1 completes its object. */
    wire_t w3 = {0};
    put(&w3, 'd'); put(&w3, 'e'); put(&w3, 'f');
    (void)feed(&p, s1, &w3, false);

    uint8_t obj1[16];
    size_t obj1_len = 0;
    int begins1 = 0, ends1 = 0, objects2 = 0;
    moq_event_t ev;
    while (moq_session_poll_events(p.c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
            const moq_object_chunk_event_t *x = &ev.u.object_chunk;
            if (x->object_id == 1) {
                if (x->begin) begins1++;
                if (x->end) ends1++;
                size_t n = x->chunk ? moq_rcbuf_len(x->chunk) : 0;
                if (n && obj1_len + n <= sizeof(obj1)) {
                    memcpy(obj1 + obj1_len, moq_rcbuf_data(x->chunk), n);
                    obj1_len += n;
                }
            }
            if (x->object_id == 2 && x->end) objects2++;
        }
        moq_event_cleanup(&ev);
    }
    if (begins1 != 1 || ends1 != 1 || obj1_len != 6 || memcmp(obj1, "abcdef", 6) != 0) {
        fprintf(stderr, "FAIL in-flight copy: object 1 begins=%d ends=%d bytes=%zu '%.*s'\n",
                begins1, ends1, obj1_len, (int)obj1_len, (const char *)obj1);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(objects2, 1);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) >= 0);
    pr_close(&p);
}

/* -- A decoded verdict under receive-byte pressure --------------------------------------- */

/* A datagram whose property block cannot be parsed fails its Track even when
 * the receive-byte budget would drop it: the verdict was decoded before the
 * unreliable admission decision, so later objects are not delivered. Without
 * pressure the same datagram does the same. */
static void test_datagram_byte_pressure(bool pressure)
{
    const char *what = pressure ? "datagram verdict, receive bytes exhausted"
                                : "datagram verdict, receive bytes free";
    pr_t p;
    moq_subscription_t a;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    if (pressure) {
        /* Four retained 4 MiB datagrams fill the default 16 MiB budget. */
        size_t n = 4u * 1024u * 1024u;
        uint8_t *wire = malloc(n + 5);
        MOQ_TEST_CHECK(wire != NULL);
        if (!wire) { pr_close(&p); return; }
        memset(wire, 'a', n + 5);
        wire[0] = 0; wire[1] = 7; wire[2] = 1; wire[4] = 3;
        for (unsigned i = 0; i < 4; i++) {
            wire[3] = (uint8_t)i;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_datagram(p.c, wire, n + 5, now(&p)),
                                  (int)MOQ_OK);
        }
        free(wire);
        MOQ_TEST_CHECK_EQ_SIZE(p.c->recv_payload_bytes, p.c->max_recv_buf);
    }
    (void)moq_session_on_datagram(p.c, k_bad_dg, sizeof(k_bad_dg), now(&p));
    rec_t r;
    rec_init(&r);
    rec_events(p.c, &r);
    rec_actions(p.c, &r);
    for (unsigned i = 0; i < 4; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(p.c, now(&p)), (int)MOQ_OK);
        rec_events(p.c, &r);
        rec_actions(p.c, &r);
    }
    size_t before = r.objects;
    (void)moq_session_on_datagram(p.c, k_good_dg, sizeof(k_good_dg), now(&p));
    rec_events(p.c, &r);
    expect_one(what, &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque);
    MOQ_TEST_CHECK_EQ_SIZE(r.objects, before);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) < 0);
    MOQ_TEST_CHECK(r.close_code == UINT64_MAX);
    pr_close(&p);
}

/* -- A session close inside a request's reserved terminal ---------------------------------- */

/* A PUBLISH still awaiting this side's answer holds a fully framed
 * REQUEST_UPDATE whose body is invalid; refusing the PUBLISH for its
 * malformed Track decodes that body and closes the session. The session's
 * terminal wins over the slot the refusal held for its own event: exactly one
 * SESSION_CLOSED, no PUBLISH_FINISHED after it, and no slot left held, with
 * one free event slot or two. */
static void test_close_inside_reserved_refusal(uint32_t capacity)
{
    char what[64];
    snprintf(what, sizeof(what), "close inside refusal, %u event slot(s)", capacity);
    pr_t p;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, capacity, false)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    drain_events(p.c);
    drain_events(p.sv);
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t pub = MOQ_PUBLICATION_INVALID;
    bool ok = sub_track(&p, "v", 7, &sub) && pub_track(&p, "v", 8, false, &pub);
    MOQ_TEST_CHECK(ok);
    int slot = ok ? pub_resolve_handle(p.c, pub) : -1;
    if (slot < 0) { failures++; pr_close(&p); return; }
    /* REQUEST_UPDATE, length 0: framed, body invalid. */
    static const uint8_t update[] = { 0x02, 0x00, 0x00 };
    moq_result_t rc = moq_session_on_bidi_stream_bytes(p.c,
        p.c->publishes[slot].rs.ref, update, sizeof(update), false, now(&p));
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->publishes[slot].rs.recv_len, sizeof(update));
    drain_actions(p.c);
    wire_t bad = {0};
    sg_header(&bad, 7, 0, 0, 1);
    sg_object(&bad, 0, k_bad, sizeof(k_bad));
    (void)feed(&p, new_stream(&p), &bad, false);
    unsigned closed = 0, finished = 0, after_close = 0;
    for (unsigned round = 0; round < 8; round++) {
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) == 1) {
            if (closed) after_close++;
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) closed++;
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED &&
                moq_publication_eq(ev.u.publish_finished.pub, pub))
                finished++;
            moq_event_cleanup(&ev);
        }
        drain_actions(p.c);
        (void)moq_session_tick(p.c, now(&p));
    }
    if (closed != 1 || finished != 0 || after_close != 0) {
        fprintf(stderr, "FAIL %s: SESSION_CLOSED %u, PUBLISH_FINISHED %u, events after "
                "the close %u\n", what, closed, finished, after_close);
        failures++;
    }
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_SIZE(p.c->event_reserve, 0);
    pr_close(&p);
}

/* -- A PUBLISH refused for its malformed Track --------------------------------------------- */

/* A PUBLISH still awaiting this side's answer is refused with UNINTERESTED
 * (0x20), the subscriber's refusal code; MALFORMED_TRACK (0x12) is the code a
 * relay publisher answers a FETCH with. Locally the request ends with the
 * MALFORMED_TRACK cause and carries the code that was sent. */
static void test_pending_publish_refusal_code(void)
{
    const char *what = "unanswered PUBLISH refused";
    pr_t p;
    moq_subscription_t a;
    moq_publication_t pend = MOQ_PUBLICATION_INVALID;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, false) || !sub_track(&p, "v", 7, &a) ||
        !pub_track(&p, "v", 9, false, &pend)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 0);
    sg_object(&w, 0, k_bad, sizeof(k_bad));
    (void)feed(&p, new_stream(&p), &w, false);
    uint32_t cause = 0;
    uint64_t local_code = UINT64_MAX;
    unsigned finished = 0;
    for (int round = 0; round < 3; round++) {
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED &&
                moq_publication_eq(ev.u.publish_finished.pub, pend)) {
                finished++;
                cause = ev.u.publish_finished.cause;
                local_code = ev.u.publish_finished.stream_error_code;
            }
            moq_event_cleanup(&ev);
        }
        pump(&p);
    }
    unsigned refusals = 0;
    uint64_t wire_code = UINT64_MAX;
    moq_event_t ev;
    while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_PUBLISH_ERROR) {
            refusals++;
            wire_code = ev.u.publish_error.error_code;
        }
        moq_event_cleanup(&ev);
    }
    if (finished != 1 || cause != MOQ_REQUEST_TERMINAL_MALFORMED_TRACK ||
        local_code != MOQ_REQUEST_ERROR_UNINTERESTED || refusals != 1 ||
        wire_code != MOQ_REQUEST_ERROR_UNINTERESTED) {
        fprintf(stderr, "FAIL %s: finished=%u cause=%u local=0x%llx refusals=%u "
                "wire=0x%llx\n", what, finished, (unsigned)cause,
                (unsigned long long)local_code, refusals, (unsigned long long)wire_code);
        failures++;
    }
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pr_close(&p);
}

/* -- Send-arena pressure on a draft-16 cancellation ---------------------------------------- */

/* Draft-16 cancels with UNSUBSCRIBE on the control stream. A send arena that
 * still holds earlier control bytes refuses it for now: the cancellation is
 * kept and sent once actions drain, with exactly one UNSUBSCRIBED. An arena
 * the message can never fit makes the cancellation impossible, and the
 * session closes with INTERNAL_ERROR instead of leaving the Track running. */
static void test_d16_cancel_send_arena(bool permanent)
{
    const char *what = permanent ? "d16 cancel, message never fits"
                                 : "d16 cancel, arena occupied";
    pr_t p;
    moq_subscription_t a;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_16, 0, 0, false) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    drain_actions(p.c);
    size_t saved_cap = p.c->send_cap;
    if (permanent) {
        p.c->send_cap = 2;
    } else {
        /* One unrelated SUBSCRIBE stays queued; two bytes remain. */
        MOQ_TEST_CHECK(fill_client_actions(&p, 1));
        MOQ_TEST_CHECK(p.c->send_len > 0);
        p.c->send_cap = p.c->send_len + 2;
    }
    wire_t w = {0};
    sg_header(&w, 7, 1, 0, 0);
    sg_object(&w, 0, k_bad, sizeof(k_bad));
    (void)feed(&p, new_stream(&p), &w, false);
    rec_t r;
    rec_init(&r);
    size_t unsubscribes = 0;
    for (int round = 0; round < 4; round++) {
        rec_events(p.c, &r);
        moq_action_t act;
        while (moq_session_poll_actions(p.c, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_CONTROL && act.u.send_control.len > 0 &&
                act.u.send_control.data[0] == 0x0a)
                unsubscribes++;
            if (act.kind == MOQ_ACTION_CLOSE_SESSION && r.close_code == UINT64_MAX)
                r.close_code = act.u.close_session.code;
            moq_action_cleanup(&act);
        }
        if (moq_session_state(p.c) == MOQ_SESS_CLOSED) break;
        (void)moq_session_tick(p.c, now(&p));
    }
    rec_events(p.c, &r);
    if (permanent) {
        if (r.close_code != 0x1 || moq_session_state(p.c) != MOQ_SESS_CLOSED) {
            fprintf(stderr, "FAIL %s: close 0x%llx state %d\n", what,
                    (unsigned long long)r.close_code, (int)moq_session_state(p.c));
            failures++;
        }
    } else {
        expect_one(what, &r, MOQ_EVENT_UNSUBSCRIBED, a._opaque);
        MOQ_TEST_CHECK_EQ_SIZE(unsubscribes, 1);
        MOQ_TEST_CHECK(r.close_code == UINT64_MAX);
        MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    }
    p.c->send_cap = saved_cap;
    pr_close(&p);
}

/* A draft-16 cancellation's refusals: WOULD_BLOCK while the send buffer
 * holds earlier output (polling the actions releases it), BUFFER only when the
 * message can never fit, and nothing changed either way. */
static void test_d16_cancel_refusal_classes(void)
{
    const char *what = "d16 cancel refusal classes";
    pr_t p;
    moq_subscription_t a;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_16, 0, 0, false) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    drain_actions(p.c);
    size_t saved_cap = p.c->send_cap;
    MOQ_TEST_CHECK(fill_client_actions(&p, 1));
    p.c->send_cap = p.c->send_len + 2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, a, now(&p)),
                          (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) >= 0);
    drain_actions(p.c);
    p.c->send_cap = 2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, a, now(&p)),
                          (int)MOQ_ERR_BUFFER);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) >= 0);
    p.c->send_cap = saved_cap;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(p.c, a, now(&p)), (int)MOQ_OK);
    MOQ_TEST_CHECK(sub_resolve_handle(p.c, a) < 0);
    pr_close(&p);
}

/* -- Copies of one object and the recipients that receive them ---------------------------- */

static void partial_object(wire_t *w)
{
    sg_header(w, 7, 1, 0, 0);
    put(w, 1); put(w, 0); put(w, 6);
    put(w, 'a'); put(w, 'b'); put(w, 'c');
}

static void count_chunk_bytes(moq_session_t *s, uint64_t ah, uint64_t bh,
                              size_t *a_bytes, size_t *b_bytes)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_CHUNK) {
            const moq_object_chunk_event_t *x = &ev.u.object_chunk;
            size_t n = x->chunk ? moq_rcbuf_len(x->chunk) : 0;
            if (x->sub._opaque == ah) *a_bytes += n;
            if (bh && x->sub._opaque == bh) *b_bytes += n;
        }
        moq_event_cleanup(&ev);
    }
}

/* Streaming receive. A receives the first half of object 1 on a first
 * stream; B then subscribes to the same Track and alias; a complete copy
 * arrives on a second stream while the first is in flight; the first
 * completes. A copy is only redundant for a recipient that has the object in
 * flight: B, which never received it, gets the second stream's copy, A gets
 * the first's, six bytes each. The control has B subscribed before the first
 * stream's header, so both are recipients of it. */
static void test_late_recipient_copy(bool late)
{
    const char *what = late ? "late recipient's first copy" : "both recipients at header";
    pr_t p;
    moq_subscription_t a, b = MOQ_SUBSCRIPTION_INVALID;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 0, 0, true) || !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    if (!late) MOQ_TEST_CHECK(sub_track(&p, "v", 7, &b));
    size_t a_bytes = 0, b_bytes = 0;
    wire_t first = {0};
    partial_object(&first);
    uint64_t s1 = new_stream(&p);
    MOQ_TEST_CHECK(feed(&p, s1, &first, false) == MOQ_OK);
    count_chunk_bytes(p.c, a._opaque, b._opaque, &a_bytes, &b_bytes);
    if (late) MOQ_TEST_CHECK(sub_track(&p, "v", 7, &b));
    wire_t copy = {0};
    partial_object(&copy);
    put(&copy, 'd'); put(&copy, 'e'); put(&copy, 'f');
    MOQ_TEST_CHECK(feed(&p, new_stream(&p), &copy, false) == MOQ_OK);
    count_chunk_bytes(p.c, a._opaque, b._opaque, &a_bytes, &b_bytes);
    wire_t tail = {0};
    put(&tail, 'd'); put(&tail, 'e'); put(&tail, 'f');
    MOQ_TEST_CHECK(feed(&p, s1, &tail, false) == MOQ_OK);
    count_chunk_bytes(p.c, a._opaque, b._opaque, &a_bytes, &b_bytes);
    if (a_bytes != 6 || b_bytes != 6) {
        fprintf(stderr, "FAIL %s: A %zu byte(s), B %zu byte(s)\n", what, a_bytes, b_bytes);
        failures++;
    }
    pr_close(&p);
}

/* -- Admitted receive owners after the verdict --------------------------------------------- */

typedef enum { TAIL_BYTES, TAIL_RCBUF, TAIL_PENDING } tail_mode_t;

/* An object begins on one stream. With the action queue full, another
 * stream makes the Track malformed: the cancellation waits for action
 * capacity, but the verdict is already retained, so the first stream's
 * remaining bytes deliver nothing -- whole or streaming receive, the bytes
 * fed as a copy or as an rcbuf. With TAIL_PENDING the object is already
 * complete on the first stream and waiting for event capacity when the
 * verdict lands; it is not emitted when capacity returns. The controls
 * without a malformed Track deliver the object. */
static void test_after_verdict(bool streaming, bool malformed, tail_mode_t mode)
{
    char what[96];
    snprintf(what, sizeof(what), "after the verdict: %s, %s, %s",
             streaming ? "streaming" : "whole", malformed ? "malformed" : "control",
             mode == TAIL_RCBUF ? "rcbuf" : mode == TAIL_PENDING ? "pending emit" : "bytes");
    pr_t p;
    moq_subscription_t a;
    if (!pr_open(&p, MOQ_VERSION_DRAFT_22, 6, mode == TAIL_PENDING ? 2 : 0, streaming) ||
        !sub_track(&p, "v", 7, &a)) {
        fprintf(stderr, "FAIL %s fixture\n", what); failures++; pr_close(&p); return;
    }
    size_t bytes = 0, begins = 0, ends = 0;
    wire_t first = {0};
    uint64_t s1 = new_stream(&p);
    if (mode == TAIL_PENDING) {
        /* Two objects on another Track's alias fill the event queue; the
         * object on the first stream then completes and waits for an
         * event slot. */
        moq_subscription_t u;
        MOQ_TEST_CHECK(sub_track(&p, "u", 11, &u));
        drain_events(p.c);
        static const uint8_t g0[] = { 0x00, 11, 0x01, 0x00, 0x03, 'a' };
        static const uint8_t g1[] = { 0x00, 11, 0x01, 0x01, 0x03, 'b' };
        (void)moq_session_on_datagram(p.c, g0, sizeof(g0), now(&p));
        (void)moq_session_on_datagram(p.c, g1, sizeof(g1), now(&p));
        MOQ_TEST_CHECK(event_queue_full(p.c));
        partial_object(&first);
        put(&first, 'd'); put(&first, 'e'); put(&first, 'f');
        (void)feed(&p, s1, &first, false);
    } else {
        partial_object(&first);
        MOQ_TEST_CHECK(feed(&p, s1, &first, false) == MOQ_OK);
        /* What the first half already delivered is not counted. */
        drain_events(p.c);
    }
    drain_actions(p.c);
    if (malformed) {
        MOQ_TEST_CHECK(fill_client_actions(&p, free_actions(p.c)));
        wire_t bad = {0};
        sg_header(&bad, 7, 2, 0, 0);
        sg_object(&bad, 0, k_bad, sizeof(k_bad));
        (void)feed(&p, new_stream(&p), &bad, false);
        MOQ_TEST_CHECK(session_alias_failing(p.c, 7));
        MOQ_TEST_CHECK(action_queue_full(p.c));
    }
    moq_event_t ev;
    if (mode == TAIL_PENDING) {
        /* Event capacity returns; the object was complete before. */
        (void)moq_session_on_data_bytes(p.c, moq_stream_ref_from_u64(s1), NULL, 0, false,
                                        now(&p));
    } else {
        if (malformed) drain_events(p.c);
        static const uint8_t tail[] = { 'd', 'e', 'f' };
        if (mode == TAIL_RCBUF) {
            moq_rcbuf_t *rb = NULL;
            MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), tail, sizeof(tail), &rb) ==
                           MOQ_OK);
            if (rb) {
                (void)moq_session_on_data_rcbuf(p.c, moq_stream_ref_from_u64(s1), rb, false,
                                                now(&p));
                moq_rcbuf_decref(rb);
            }
        } else {
            (void)moq_session_on_data_bytes(p.c, moq_stream_ref_from_u64(s1), tail,
                                            sizeof(tail), false, now(&p));
        }
    }
    for (int round = 0; round < 4; round++) {
        while (moq_session_poll_events(p.c, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                moq_subscription_eq(ev.u.object_received.sub, a)) {
                begins++; ends++;
                if (ev.u.object_received.payload)
                    bytes += moq_rcbuf_len(ev.u.object_received.payload);
            }
            if (ev.kind == MOQ_EVENT_OBJECT_CHUNK &&
                moq_subscription_eq(ev.u.object_chunk.sub, a)) {
                /* An end never precedes the begin of its run. */
                if (ev.u.object_chunk.begin) begins++;
                if (ev.u.object_chunk.end) ends++;
                if (ev.u.object_chunk.end && ends > begins && !(streaming && mode != TAIL_PENDING))
                    failures++;
                if (ev.u.object_chunk.chunk)
                    bytes += moq_rcbuf_len(ev.u.object_chunk.chunk);
            }
            moq_event_cleanup(&ev);
        }
        if (mode == TAIL_PENDING)
            (void)moq_session_on_data_bytes(p.c, moq_stream_ref_from_u64(s1), NULL, 0,
                                            false, now(&p));
    }
    /* Whole delivery is one object (one begin and one end); a streaming
     * tail after the begin chunk already polled is its continuation and end
     * only; a pending complete object is surfaced whole. After the verdict
     * nothing at all. */
    size_t want = malformed ? 0 : (mode == TAIL_PENDING || !streaming ? 6 : 3);
    size_t want_begins = malformed ? 0 : (mode == TAIL_PENDING || !streaming ? 1 : 0);
    size_t want_ends = malformed ? 0 : 1;
    if (bytes != want || begins != want_begins || ends != want_ends) {
        fprintf(stderr, "FAIL %s: %zu payload byte(s), %zu begin(s), %zu end(s); want "
                "%zu, %zu, %zu\n", what, bytes, begins, ends, want, want_begins, want_ends);
        failures++;
    }
    pr_close(&p);
}

/* -- Driver ---------------------------------------------------------------------------- */

typedef struct named { const char *name; void (*fn)(void); } named_t;

static void t_one_action_sub(void) { test_one_free_action(false, false); }
static void t_one_action_sub_stream(void) { test_one_free_action(false, true); }
static void t_one_action_pub(void) { test_one_free_action(true, false); }
static void t_one_action_pub_stream(void) { test_one_free_action(true, true); }
static void t_domain_d16(void) { test_track_domain_older(MOQ_VERSION_DRAFT_16); }
static void t_domain_d18(void) { test_track_domain_older(MOQ_VERSION_DRAFT_18); }
static void t_dg_events(void) { test_datagram_under_pressure(false, false); }
static void t_dg_events_more(void) { test_datagram_under_pressure(false, true); }
static void t_dg_actions(void) { test_datagram_under_pressure(true, false); }
static void t_fill_sub(void) { test_fill_parent_terminal(false); }
static void t_fill_pub(void) { test_fill_parent_terminal(true); }
static void t_dg_bytes(void) { test_datagram_byte_pressure(true); }
static void t_dg_bytes_free(void) { test_datagram_byte_pressure(false); }
static void t_close_one(void) { test_close_inside_reserved_refusal(1); }
static void t_close_two(void) { test_close_inside_reserved_refusal(2); }
static void t_arena_occupied(void) { test_d16_cancel_send_arena(false); }
static void t_arena_never(void) { test_d16_cancel_send_arena(true); }
static void t_late_recipient(void) { test_late_recipient_copy(true); }
static void t_both_recipients(void) { test_late_recipient_copy(false); }
static void t_av_whole(void) { test_after_verdict(false, true, TAIL_BYTES); }
static void t_av_stream(void) { test_after_verdict(true, true, TAIL_BYTES); }
static void t_av_stream_rc(void) { test_after_verdict(true, true, TAIL_RCBUF); }
static void t_av_whole_rc(void) { test_after_verdict(false, true, TAIL_RCBUF); }
static void t_av_pending(void) { test_after_verdict(false, true, TAIL_PENDING); }
static void t_av_pending_stream(void) { test_after_verdict(true, true, TAIL_PENDING); }
static void t_av_ctl_whole(void) { test_after_verdict(false, false, TAIL_BYTES); }
static void t_av_ctl_stream(void) { test_after_verdict(true, false, TAIL_BYTES); }
static void t_av_ctl_rc(void) { test_after_verdict(true, false, TAIL_RCBUF); }
static void t_av_ctl_pending(void) { test_after_verdict(false, false, TAIL_PENDING); }

static const named_t k_tests[] = {
    { "one_action_subscribe", t_one_action_sub },
    { "one_action_subscribe_streaming", t_one_action_sub_stream },
    { "one_action_publish", t_one_action_pub },
    { "one_action_publish_streaming", t_one_action_pub_stream },
    { "domain_subgroup", test_track_domain_from_subgroup },
    { "domain_fetch", test_track_domain_from_fetch },
    { "domain_d16", t_domain_d16 },
    { "domain_d18", t_domain_d18 },
    { "verdict_partial_retirement", test_verdict_survives_partial_retirement },
    { "history_owner_turnover", test_history_survives_owner_turnover },
    { "history_alias_reuse", test_history_discarded_on_alias_reuse },
    { "datagram_event_pressure", t_dg_events },
    { "datagram_event_pressure_later_data", t_dg_events_more },
    { "datagram_action_pressure", t_dg_actions },
    { "fill_parent_subscribe", t_fill_sub },
    { "fill_parent_publish", t_fill_pub },
    { "copy_in_flight", test_copy_in_flight_not_interleaved },
    { "violation_inside_fill", test_violation_inside_fill },
    { "datagram_byte_pressure", t_dg_bytes },
    { "datagram_bytes_free", t_dg_bytes_free },
    { "close_inside_refusal_one_slot", t_close_one },
    { "close_inside_refusal_two_slots", t_close_two },
    { "pending_publish_refusal_code", test_pending_publish_refusal_code },
    { "d16_cancel_arena_occupied", t_arena_occupied },
    { "d16_cancel_never_fits", t_arena_never },
    { "d16_cancel_refusal_classes", test_d16_cancel_refusal_classes },
    { "d16_immutable_subgroup", t_d16_sg },
    { "d16_immutable_subgroup_bytewise", t_d16_sg_split },
    { "d16_immutable_subgroup_streaming", t_d16_sg_stream },
    { "d16_immutable_subgroup_streaming_bytewise", t_d16_sg_stream_split },
    { "d16_immutable_datagram", test_d16_immutable_datagram },
    { "d16_immutable_fetch", test_d16_immutable_fetch },
    { "late_recipient_copy", t_late_recipient },
    { "both_recipients_copy", t_both_recipients },
    { "after_verdict_whole", t_av_whole },
    { "after_verdict_streaming", t_av_stream },
    { "after_verdict_streaming_rcbuf", t_av_stream_rc },
    { "after_verdict_whole_rcbuf", t_av_whole_rc },
    { "after_verdict_pending_emit", t_av_pending },
    { "after_verdict_pending_emit_streaming", t_av_pending_stream },
    { "after_verdict_control_whole", t_av_ctl_whole },
    { "after_verdict_control_streaming", t_av_ctl_stream },
    { "after_verdict_control_rcbuf", t_av_ctl_rc },
    { "after_verdict_control_pending_emit", t_av_ctl_pending },
};

int main(int argc, char **argv)
{
    for (size_t i = 0; i < sizeof(k_tests) / sizeof(k_tests[0]); i++) {
        if (argc > 1 && strcmp(argv[1], k_tests[i].name) != 0) continue;
        int before = failures;
        k_tests[i].fn();
        if (failures != before) fprintf(stderr, "  in %s\n", k_tests[i].name);
    }
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: malformed_track_owners\n");
    return 0;
}
