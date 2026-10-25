/* A draft-18 subscriber cancels a subscription on its request stream. After
 * STOP_SENDING the publisher MUST reset every open stream associated with that
 * SUBSCRIBE (draft-18 §5.1.1); the cancellation's resets carry CANCELLED, the
 * code §3.3.3 recommends. The subscriber's RESET_STREAM of the request stream
 * is the other cancellation route of §3.3.2, which this session classifies the
 * same way -- it ends the subscription with UNSUBSCRIBED and releases it -- so
 * the subscription's open data streams are reset on that route as well.
 *
 * The publisher's action and event queues each hold one record. A delivery
 * that finds room for its own record (the UNSUBSCRIBED event, a STOP's drain
 * reference) completes at once: the subscription is released, the resets
 * that fit are queued and the rest are owed by their subgroup entries, which
 * keep their stream and code and are issued as actions are polled, with no
 * further peer input. A delivery without that room is refused before any
 * output and is delivered again, as the transport binding does.
 * Pinned:
 * - the subscription is cancelled from the first delivery, whatever capacity
 *   that delivery finds (event queue full, action queue full, no room for the
 *   STOP's drain reference): while a refused cancellation is pending, every
 *   production call under it -- a new subgroup,
 *   whole-object, status-object, begin, continuation and end writes, a FIN,
 *   object and status datagrams -- and every local terminal (PUBLISH_DONE,
 *   GOAWAY, and for a pending subscription accept, reject and redirect) is
 *   refused with MOQ_ERR_WRONG_STATE, queues nothing and keeps no reference
 *   to a payload, while the same calls succeed on an uncancelled subscription;
 * - every subgroup open or mid-object when the cancellation arrived is reset
 *   exactly once and written to no more; an application reset queued during
 *   the cancellation, or a delivery-timeout reset, stands in for the
 *   cancellation's own reset of that stream, a FIN never does;
 * - no refused delivery has the terminal event queued; when UNSUBSCRIBED is
 *   read -- events polled first -- and when the subscription is released,
 *   every live subgroup's reset is consumed, queued or owed by its entry; it
 *   is surfaced exactly once;
 * - A's slot is reused by a new subscription while A's resets are still
 *   owed: the owed entries keep A's streams and code, a subgroup for the new
 *   subscription waits (retryable) for their pool slots, and polling actions
 *   alone issues every owed reset exactly once and frees them;
 * - a subgroup already FIN-closed and another subscription's open subgroup
 *   are never reset by the cancellation -- the FIN-closed one only by its own
 *   subgroup delivery timer, which draft-18 §8 starts at the FIN -- and the
 *   other subscription keeps writing while the cancellation is pending;
 * - repeated and crossed STOP/RESET deliveries reset nothing twice, and a
 *   RESET ends the peer's direction for good: no later STOP makes the session
 *   keep an identity for it; request bytes and a FIN arriving while the
 *   cancellation is pending are discarded unread, the FIN remembered;
 * - a Joining FETCH naming a cancelled pending subscription is rejected;
 * - a subscription this session sent a request GOAWAY for keeps producing
 *   until the subscriber's STOP/RESET; that STOP/RESET then owes the same
 *   resets under the same rules, and the cancellation completes without an
 *   event. Only that ending is pinned here;
 * - a STOP leaves the subscriber's direction open, so the request stream's
 *   identity is kept as a peer-stopped drain reference, with or without
 *   subgroups and with or without a GOAWAY: a trailing FIN is absorbed by
 *   it, only the subscriber's FIN or RESET retires it -- never another
 *   STOP -- and no later STOP brings it back;
 * - a subscriber-role subscription is never marked cancelled;
 * - once settled, nothing remains under the cancelled subscription, its slot
 *   and its subgroup slots are reused by a new subscription that a late
 *   signal on the old request stream does not reach, and its old handles are
 *   stale;
 * - the fixture stops at any setup step that fails, recording it, so no row
 *   runs on a missing owner, subgroup or stream;
 * - through the real transport bridge, the bridge's own pending STOP/RESET
 *   redelivery completes the cancellation without the application delivering
 *   anything and without a fatal error.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include "../support/held_bridge_driver.h"
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <string.h>

static int failures;

#define T_NOW 10

/* The subgroups the fixture opens, by role. The first SG_A_LIVE belong to the
 * cancelled subscription and are live when the cancellation arrives: two of
 * each live shape, so whichever one is reset first, one of each
 * shape is still left for an operation to try. */
enum {
    SG_A_OPEN,        /* whole object written */
    SG_A_OPEN2,
    SG_A_PARTIAL,     /* mid-object: 4 of 16 payload bytes written */
    SG_A_PARTIAL2,
    SG_A_FULL,        /* mid-object: every payload byte written, not ended */
    SG_A_FULL2,
    SG_A_LIVE,
    SG_A_FIN = SG_A_LIVE, /* FIN-closed before the cancellation */
    SG_B_OPEN,        /* the other subscription's open subgroup */
    SG_COUNT
};

typedef enum { SHAPE_OPEN, SHAPE_PARTIAL, SHAPE_FULL } shape_t;

static shape_t sg_shape(int i)
{
    if (i == SG_A_PARTIAL || i == SG_A_PARTIAL2) return SHAPE_PARTIAL;
    if (i == SG_A_FULL || i == SG_A_FULL2) return SHAPE_FULL;
    return SHAPE_OPEN;
}

/* The fixture's setup steps, so a refusal can name the step it hit. */
typedef enum {
    FX_STEP_NONE,
    FX_STEP_SESSION,
    FX_STEP_SUBSCRIBE_A,
    FX_STEP_SUBSCRIBE_B,
    FX_STEP_A_STATE,
    FX_STEP_A_OPEN,       /* index: which of A's subgroups */
    FX_STEP_A_BEGIN,
    FX_STEP_A_WRITE,
    FX_STEP_A_DRAIN,
    FX_STEP_A_CLOSE,
    FX_STEP_GOAWAY,
    FX_STEP_B_OPEN,
    FX_STEP_B_WRITE,
    FX_STEP_B_DRAIN,
    FX_STEP_ESTABLISHED,
} fx_step_t;

/* One setup step and its result, as expected or as observed. For
 * FX_STEP_SUBSCRIBE_B the result is the REQUEST_ERROR code the session
 * answered with. */
typedef struct fx_refusal {
    fx_step_t step;
    int       index;
    int64_t   result;
} fx_refusal_t;

typedef struct fx_opts {
    bool     pending;       /* A is left pending: no accept, no subgroups */
    bool     no_subgroups;  /* A is accepted but opens no subgroup */
    uint64_t sg_timeout_ms; /* A's subgroup delivery timeout, 0 for none */
    uint64_t obj_timeout_ms; /* A's object delivery timeout, 0 for none */
    bool     goaway;        /* we sent A's request GOAWAY (A established) */
    /* Session pools, 0 for the fixture's own sizes. Only the setup-failure
     * controls shrink them. */
    uint32_t max_subscriptions;
    uint32_t max_open_subgroups;
    /* The one setup refusal a setup-failure control provokes: when exactly
     * this step refuses with exactly this result, the fixture records it in
     * `refused` and stops without counting a failure. Any other miss is an
     * ordinary failure. */
    fx_refusal_t expect;
    /* Record other misses in `unexpected` instead of counting them. Only the
     * control that shows a different miss is not taken for the expected one
     * sets it. */
    bool         capture_unexpected;
} fx_opts_t;

typedef struct fixture {
    moq_session_t        *s;
    fx_opts_t             o;
    uint64_t              now;
    uint64_t              next_rid;
    uint64_t              rid_a;
    moq_stream_ref_t      ref_a, ref_b;
    moq_subscription_t    sub_a, sub_b;
    int                   slot_a;
    moq_subgroup_handle_t sg[SG_COUNT];
    moq_stream_ref_t      sg_ref[SG_COUNT];
    bool                  hold_events; /* redelivery reads no events */
    bool                  peer_fin;    /* the subscriber FIN'd A's stream */
    fx_refusal_t          refused;     /* the expected refusal, once seen */
    fx_refusal_t          unexpected;  /* a captured other miss */
    int                   unexpected_count;
} fixture_t;

typedef struct tally {
    int      resets[SG_COUNT];
    int      outgoing_terminals[SG_COUNT];
    uint64_t reset_code[SG_COUNT];
    int      writes[SG_COUNT];
    int      other_actions;
    int      unsubscribed_a;
    int      committed_at_unsubscribed; /* A's live streams reset by then */
    int      committed_at_release;      /* ... and when A was released */
    int      updated_b;
    int      other_events;
} tally_t;

/* -- Session -------------------------------------------------------- */

static moq_session_t *make_server(uint32_t max_subscriptions,
                                  uint32_t max_open_subgroups)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_actions = 1;
    cfg.max_events = 1;
    cfg.max_subscriptions = max_subscriptions;
    cfg.max_open_subgroups = max_open_subgroups;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0),
        (int)MOQ_OK);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_ESTABLISHED);
    return s;
}

static size_t actions_queued(const moq_session_t *s)
{
    return (size_t)(s->action_tail - s->action_head);
}

static size_t events_queued(const moq_session_t *s)
{
    return (size_t)(s->event_tail - s->event_head);
}

static int drain_count(moq_session_t *s)
{
    int n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        n++;
        moq_action_cleanup(&a);
    }
    return n;
}

/* Poll every queued action, counting them; each must be a write on `ref`. */
static int drain_writes_on(moq_session_t *s, moq_stream_ref_t ref)
{
    int n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        MOQ_TEST_CHECK_EQ_INT((int)a.kind, (int)MOQ_ACTION_SEND_DATA);
        if (a.kind == MOQ_ACTION_SEND_DATA)
            MOQ_TEST_CHECK_EQ_U64(a.u.send_data.stream_ref._v, ref._v);
        n++;
        moq_action_cleanup(&a);
    }
    return n;
}

/* -- Fixture -------------------------------------------------------- */

/* The subscriber's SUBSCRIBE for `name` arrives on `ref` and is surfaced;
 * unless `pending`, it is accepted. Returns the publisher-side handle. */
static moq_subscription_t subscribe(fixture_t *f, moq_stream_ref_t ref,
                                    const char *name, uint64_t sg_timeout_ms,
                                    uint64_t obj_timeout_ms, bool pending)
{
    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    /* LARGEST_OBJECT, so a Joining FETCH may name the subscription. */
    p.has_filter = true;
    p.filter_type = 2;
    if (sg_timeout_ms) {
        p.has_subgroup_delivery_timeout = true;
        p.subgroup_delivery_timeout_ms = sg_timeout_ms;
    }
    if (obj_timeout_ms) {
        p.has_object_delivery_timeout = true;
        p.object_delivery_timeout_ms = obj_timeout_ms;
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, f->next_rid, &ns,
            (moq_bytes_t){ (const uint8_t *)name, strlen(name) }, &p),
        (int)MOQ_OK);
    f->next_rid += 2;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f->s, ref, msg,
            moq_buf_writer_offset(&w), false, f->now), (int)MOQ_OK);
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    int requests = 0;
    moq_event_t e;
    while (moq_session_poll_events(f->s, &e, 1) > 0) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            requests++;
            sub = e.u.subscribe_request.sub;
        }
        moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK_EQ_INT(requests, 1);
    if (!pending) {
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe(f->s, sub, &acc, f->now),
            (int)MOQ_OK);
    }
    moq_action_t a;
    while (moq_session_poll_actions(f->s, &a, 1) > 0) moq_action_cleanup(&a);
    return sub;
}

static moq_result_t open_sg(fixture_t *f, moq_subscription_t sub, uint64_t group,
                            moq_subgroup_handle_t *out, moq_stream_ref_t *out_ref)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = group;
    moq_result_t rc = moq_session_open_subgroup(f->s, sub, &c, f->now, out);
    if (rc != MOQ_OK) return rc;
    moq_action_t a;
    int n = 0;
    while (moq_session_poll_actions(f->s, &a, 1) > 0) {
        MOQ_TEST_CHECK_EQ_INT((int)a.kind, (int)MOQ_ACTION_SEND_DATA);
        if (a.kind == MOQ_ACTION_SEND_DATA) *out_ref = a.u.send_data.stream_ref;
        n++;
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(n, 1);
    return rc;
}

/* A payload the caller owns one reference to. */
static moq_rcbuf_t *payload(size_t n)
{
    static const uint8_t body[16] = { 1, 2, 3, 4, 5, 6, 7, 8,
                                      9, 10, 11, 12, 13, 14, 15, 16 };
    moq_rcbuf_t *buf = NULL;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_rcbuf_create(moq_alloc_default(), body, n, &buf), (int)MOQ_OK);
    return buf;
}

static moq_result_t write_whole(fixture_t *f, moq_subgroup_handle_t sg,
                                uint64_t object_id)
{
    moq_rcbuf_t *buf = payload(4);
    if (!buf) return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_write_object(f->s, sg, object_id, buf, f->now);
    moq_rcbuf_decref(buf);
    return rc;
}

static moq_result_t write_part(fixture_t *f, moq_subgroup_handle_t sg, size_t n)
{
    moq_rcbuf_t *buf = payload(n);
    if (!buf) return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_write_object_data(f->s, sg, buf, f->now);
    moq_rcbuf_decref(buf);
    return rc;
}

/* A fixture prerequisite at setup step `step` (`idx` within it). A miss is
 * recorded like any failed check -- or, for the one control that asks, kept
 * in `unexpected` with its step -- and the fixture stops there and reports
 * failure, so no row ever runs on a missing owner, subgroup or stream. */
#define FX_REQUIRE(step, idx, expr) do { \
    if (!(expr)) { \
        if (f->o.capture_unexpected) { \
            f->unexpected = (fx_refusal_t){ (step), (idx), 0 }; \
            f->unexpected_count++; \
        } else { \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            failures++; \
        } \
        return false; \
    } \
} while (0)

/* Whether `step` (`idx`) refusing with `result` is the refusal the options
 * expect; if so it is recorded as seen. */
static bool fx_expected_refusal(fixture_t *f, fx_step_t step, int idx,
                                int64_t result)
{
    const fx_refusal_t *x = &f->o.expect;
    if (x->step != step || x->index != idx || x->result != result) return false;
    f->refused = *x;
    return true;
}

/* The subscriber's SUBSCRIBE for `name` on `ref`, which the session is
 * expected to turn away: nothing surfaced, and exactly one REQUEST_ERROR,
 * carrying the FIN, on `ref`. Returns that REQUEST_ERROR's code, or -1 when
 * the request was not turned away that way. Nothing is counted here. */
static int64_t subscribe_turned_away(fixture_t *f, moq_stream_ref_t ref,
                                     const char *name)
{
    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (moq_d18_encode_subscribe(&w, f->next_rid, &ns,
            (moq_bytes_t){ (const uint8_t *)name, strlen(name) }, &p) != MOQ_OK)
        return -1;
    f->next_rid += 2;
    if (moq_session_on_bidi_stream_bytes(f->s, ref, msg, moq_buf_writer_offset(&w),
                                         false, f->now) != MOQ_OK)
        return -1;
    int surfaced = 0, errors = 0, other = 0;
    int64_t code = -1;
    moq_event_t e;
    while (moq_session_poll_events(f->s, &e, 1) > 0) {
        surfaced++;
        moq_event_cleanup(&e);
    }
    moq_action_t a;
    while (moq_session_poll_actions(f->s, &a, 1) > 0) {
        moq_control_envelope_t env;
        moq_d18_request_error_t re;
        moq_buf_reader_t rd;
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == ref._v &&
            a.u.send_bidi_stream.fin) {
            moq_buf_reader_init(&rd, a.u.send_bidi_stream.data,
                                a.u.send_bidi_stream.len);
            if (moq_d18_decode_envelope(&rd, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_REQUEST_ERROR &&
                moq_d18_decode_request_error(env.payload, env.payload_len,
                                             &re) == MOQ_OK) {
                errors++;
                code = (int64_t)re.error_code;
            } else {
                other++;
            }
        } else {
            other++;
        }
        moq_action_cleanup(&a);
    }
    return (surfaced == 0 && errors == 1 && other == 0) ? code : -1;
}

/* Two publisher-role subscriptions. Unless the options say otherwise, A is
 * established with two open subgroups, two mid-object with part of the
 * payload written, two mid-object with all of it written, and one already
 * FIN-closed; B has one open subgroup. Every queued record has been polled. */
static bool fixture_open(fixture_t *f, fx_opts_t o)
{
    memset(f, 0, sizeof(*f));
    f->o = o;
    f->now = T_NOW;
    f->slot_a = -1;
    f->s = make_server(o.max_subscriptions ? o.max_subscriptions : 3,
                       o.max_open_subgroups ? o.max_open_subgroups : SG_COUNT);
    FX_REQUIRE(FX_STEP_SESSION, 0, f->s != NULL);
    FX_REQUIRE(FX_STEP_SESSION, 0, moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
    f->ref_a = moq_stream_ref_from_u64(0x90000);
    f->ref_b = moq_stream_ref_from_u64(0x90004);
    f->rid_a = f->next_rid;
    f->sub_a = subscribe(f, f->ref_a, "a", o.sg_timeout_ms,
                         o.obj_timeout_ms, o.pending);
    f->slot_a = sub_resolve_handle(f->s, f->sub_a);
    FX_REQUIRE(FX_STEP_SUBSCRIBE_A, 0, f->slot_a >= 0);
    if (o.expect.step == FX_STEP_SUBSCRIBE_B) {
        int64_t code = subscribe_turned_away(f, f->ref_b, "b");
        if (code >= 0 && fx_expected_refusal(f, FX_STEP_SUBSCRIBE_B, 0, code))
            return false;
        FX_REQUIRE(FX_STEP_SUBSCRIBE_B, 0, false);
    }
    f->sub_b = subscribe(f, f->ref_b, "b", 0, 0, false);
    FX_REQUIRE(FX_STEP_SUBSCRIBE_B, 0, sub_resolve_handle(f->s, f->sub_b) >= 0);
    FX_REQUIRE(FX_STEP_A_STATE, 0, f->s->subs[f->slot_a].state ==
               (o.pending ? MOQ_SUB_PENDING_PUBLISHER : MOQ_SUB_ESTABLISHED));

    if (!o.pending && !o.no_subgroups) {
        for (int i = 0; i <= SG_A_FIN; i++) {
            moq_result_t orc = open_sg(f, f->sub_a, (uint64_t)(10 + i),
                                       &f->sg[i], &f->sg_ref[i]);
            if (orc != MOQ_OK && fx_expected_refusal(f, FX_STEP_A_OPEN, i, orc))
                return false;
            FX_REQUIRE(FX_STEP_A_OPEN, i, orc == MOQ_OK);
            if (i != SG_A_FIN && sg_shape(i) != SHAPE_OPEN) {
                size_t len = sg_shape(i) == SHAPE_PARTIAL ? 16 : 4;
                moq_begin_object_cfg_t bc;
                moq_begin_object_cfg_init(&bc);
                bc.object_id = 0;
                bc.payload_length = len;
                FX_REQUIRE(FX_STEP_A_BEGIN, i,
                           moq_session_begin_object_ex(f->s, f->sg[i], &bc,
                                                       f->now) == MOQ_OK);
                FX_REQUIRE(FX_STEP_A_DRAIN, i,
                           drain_writes_on(f->s, f->sg_ref[i]) == 1);
                FX_REQUIRE(FX_STEP_A_WRITE, i,
                           write_part(f, f->sg[i], 4) == MOQ_OK);
            } else {
                FX_REQUIRE(FX_STEP_A_WRITE, i,
                           write_whole(f, f->sg[i], 0) == MOQ_OK);
            }
            FX_REQUIRE(FX_STEP_A_DRAIN, i,
                       drain_writes_on(f->s, f->sg_ref[i]) == 1);
        }
        FX_REQUIRE(FX_STEP_A_CLOSE, SG_A_FIN,
                   moq_session_close_subgroup(f->s, f->sg[SG_A_FIN], f->now)
                   == MOQ_OK);
        FX_REQUIRE(FX_STEP_A_DRAIN, SG_A_FIN,
                   drain_writes_on(f->s, f->sg_ref[SG_A_FIN]) == 1);
    }

    /* The public request GOAWAY is only for an established subscription. */
    FX_REQUIRE(FX_STEP_GOAWAY, 0, !(o.goaway && o.pending));
    if (o.goaway) {
        /* Our request GOAWAY on A's stream, our send half left open. It does
         * not change the subscription: A keeps producing. */
        moq_request_goaway_cfg_t g;
        moq_request_goaway_cfg_init(&g);
        FX_REQUIRE(FX_STEP_GOAWAY, 0,
                   moq_session_request_goaway_subscribe(f->s, f->sub_a, &g,
                                                        f->now) == MOQ_OK);
        int sent = 0;
        moq_action_t a;
        while (moq_session_poll_actions(f->s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == f->ref_a._v &&
                !a.u.send_bidi_stream.fin)
                sent++;
            moq_action_cleanup(&a);
        }
        FX_REQUIRE(FX_STEP_GOAWAY, 0, sent == 1);
        FX_REQUIRE(FX_STEP_GOAWAY, 0, f->s->subs[f->slot_a].goaway_sent);
        FX_REQUIRE(FX_STEP_GOAWAY, 0, !sub_cancel_pending(f->s, f->sub_a));
        if (!o.no_subgroups) {
            FX_REQUIRE(FX_STEP_GOAWAY, 1,
                       write_whole(f, f->sg[SG_A_OPEN], 1) == MOQ_OK);
            FX_REQUIRE(FX_STEP_GOAWAY, 1,
                       drain_writes_on(f->s, f->sg_ref[SG_A_OPEN]) == 1);
        }
    }

    FX_REQUIRE(FX_STEP_B_OPEN, 0, open_sg(f, f->sub_b, 10, &f->sg[SG_B_OPEN],
                                          &f->sg_ref[SG_B_OPEN]) == MOQ_OK);
    FX_REQUIRE(FX_STEP_B_WRITE, 0, write_whole(f, f->sg[SG_B_OPEN], 0) == MOQ_OK);
    FX_REQUIRE(FX_STEP_B_DRAIN, 0,
               drain_writes_on(f->s, f->sg_ref[SG_B_OPEN]) == 1);
    FX_REQUIRE(FX_STEP_ESTABLISHED, 0,
               moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool open_row(fixture_t *f, const char *what, fx_opts_t o)
{
    if (fixture_open(f, o)) return true;
    failures++;
    if (f->s) moq_session_destroy(f->s);
    fprintf(stderr, "FAIL %s: fixture\n", what);
    return false;
}

static int live_count(const fixture_t *f)
{
    return (f->o.pending || f->o.no_subgroups) ? 0 : SG_A_LIVE;
}

/* -- Observation ---------------------------------------------------- */

static int sg_index(const fixture_t *f, moq_stream_ref_t ref)
{
    if (ref._v == 0) return -1;
    for (int i = 0; i < SG_COUNT; i++)
        if (f->sg_ref[i]._v == ref._v) return i;
    return -1;
}

/* The pool state of the fixture's subgroup `i`. */
static moq_sg_state_t sg_state(const fixture_t *f, int i)
{
    for (size_t k = 0; k < f->s->sg_cap; k++)
        if (f->s->subgroups[k].state != MOQ_SG_FREE &&
            f->s->subgroups[k].stream_ref._v == f->sg_ref[i]._v)
            return f->s->subgroups[k].state;
    return MOQ_SG_FREE;
}

/* Whether a RESET_DATA for the fixture's subgroup `i` sits in the action
 * queue. */
static bool reset_queued_for(const fixture_t *f, int i)
{
    const moq_session_t *s = f->s;
    for (size_t k = s->action_head; k < s->action_tail; k++) {
        const moq_action_t *a = &s->actions[k % s->action_cap];
        if (a->kind == MOQ_ACTION_RESET_DATA &&
            a->u.reset_data.stream_ref._v == f->sg_ref[i]._v)
            return true;
    }
    return false;
}

/* Whether the fixture's subgroup `i` owes its reset (an entry that keeps the
 * stream and code until the action is issued). */
static bool reset_owed_for(const fixture_t *f, int i)
{
    return sg_state(f, i) == MOQ_SG_RESET_OWED;
}

/* A's live subgroups whose reset is consumed, queued or owed. */
static int accounted(const fixture_t *f, const tally_t *t)
{
    int n = 0;
    for (int i = 0; i < live_count(f); i++)
        if (t->resets[i] > 0 || reset_queued_for(f, i) || reset_owed_for(f, i))
            n++;
    return n;
}

/* Whether A's UNSUBSCRIBED sits in the event queue. */
static bool unsubscribed_queued(const fixture_t *f)
{
    const moq_session_t *s = f->s;
    for (size_t k = s->event_head; k < s->event_tail; k++) {
        const moq_event_t *e = &s->events[k % s->event_cap];
        if (e->kind == MOQ_EVENT_UNSUBSCRIBED &&
            moq_subscription_eq(e->u.unsubscribed.sub, f->sub_a))
            return true;
    }
    return false;
}

static void record_action(const fixture_t *f, tally_t *t, const moq_action_t *a)
{
    if (a->kind == MOQ_ACTION_RESET_DATA) {
        int i = sg_index(f, a->u.reset_data.stream_ref);
        if (i >= 0) {
            t->resets[i]++;
            t->reset_code[i] = a->u.reset_data.error_code;
            return;
        }
    }
    if (a->kind == MOQ_ACTION_SEND_DATA) {
        int i = sg_index(f, a->u.send_data.stream_ref);
        if (i >= 0) {
            t->writes[i]++;
            return;
        }
    }
    t->other_actions++;
    fprintf(stderr, "  unexpected action kind %u\n", (unsigned)a->kind);
}

static void record_event(const fixture_t *f, tally_t *t, const moq_event_t *e)
{
    if (e->kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
        e->detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
        const moq_outgoing_subgroup_terminated_event_t *d =
            &e->u.outgoing_subgroup_terminated;
        int i = sg_index(f, d->stream_ref);
        if (i >= 0) {
            ++t->outgoing_terminals[i];
            MOQ_TEST_CHECK_EQ_U64(d->subgroup._opaque, f->sg[i]._opaque);
            MOQ_TEST_CHECK_EQ_U64(d->sub._opaque, f->sub_a._opaque);
            MOQ_TEST_CHECK(!moq_publication_is_valid(d->pub));
            MOQ_TEST_CHECK_EQ_U64(d->group_id, (uint64_t)(10 + i));
            MOQ_TEST_CHECK_EQ_U64(d->subgroup_id, 0);
            MOQ_TEST_CHECK_EQ_U64(d->error_code, 0x2);
            MOQ_TEST_CHECK_EQ_INT((int)d->cause,
                                 (int)MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
            return;
        }
    }
    if (e->kind == MOQ_EVENT_UNSUBSCRIBED &&
        moq_subscription_eq(e->u.unsubscribed.sub, f->sub_a)) {
        t->unsubscribed_a++;
        /* Read before the actions queued with it: each reset must be
         * consumed already, queued or owed. */
        t->committed_at_unsubscribed = accounted(f, t);
        return;
    }
    if (e->kind == MOQ_EVENT_SUBSCRIBE_UPDATED &&
        moq_subscription_eq(e->u.subscribe_updated.sub, f->sub_b)) {
        t->updated_b++;
        return;
    }
    t->other_events++;
    fprintf(stderr, "  unexpected event kind %u\n", (unsigned)e->kind);
}

static void poll_one_action(fixture_t *f, tally_t *t)
{
    moq_action_t a;
    if (moq_session_poll_actions(f->s, &a, 1) > 0) {
        record_action(f, t, &a);
        moq_action_cleanup(&a);
    }
}

/* Events first, then actions. */
static void poll_all(fixture_t *f, tally_t *t)
{
    moq_event_t e;
    while (moq_session_poll_events(f->s, &e, 1) > 0) {
        record_event(f, t, &e);
        moq_event_cleanup(&e);
    }
    moq_action_t a;
    while (moq_session_poll_actions(f->s, &a, 1) > 0) {
        record_action(f, t, &a);
        moq_action_cleanup(&a);
    }
}

/* The subscriber's signal on A's request stream: RESET_STREAM of its own
 * direction, or STOP_SENDING of the publisher's. */
static moq_result_t deliver(fixture_t *f, bool reset)
{
    return reset ? moq_session_on_bidi_stream_reset(f->s, f->ref_a, 0x1, f->now)
                 : moq_session_on_bidi_stream_stop(f->s, f->ref_a, 0x1, f->now);
}

/* A's request stream still resolves to A, in the same slot. */
static bool owner_kept(const fixture_t *f)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(f->s, f->ref_a);
    return sub_resolve_handle(f->s, f->sub_a) == f->slot_a &&
           ep.kind == MOQ_REQ_SUBSCRIPTION && ep.slot == f->slot_a;
}

static int live_under(const moq_session_t *s, moq_subscription_t sub)
{
    int n = 0;
    for (size_t i = 0; i < s->sg_cap; i++)
        if ((s->subgroups[i].state == MOQ_SG_OPEN ||
             s->subgroups[i].state == MOQ_SG_STREAMING) &&
            moq_subscription_eq(s->subgroups[i].sub, sub))
            n++;
    return n;
}

/* A was just released: record how many of its resets were accounted for --
 * consumed already, queued or owed -- at that moment. */
static void note_released(const fixture_t *f, tally_t *t)
{
    MOQ_TEST_CHECK(sub_resolve_handle(f->s, f->sub_a) < 0);
    t->committed_at_release = accounted(f, t);
}

/* Given the result of a delivery already made, deliver `reset`'s signal again
 * until it is taken. At every refusal the owner is kept and no terminal event
 * is queued; then one event and one action are polled. Returns the number of
 * deliveries, counting the one already made. */
static int redeliver_until_taken(fixture_t *f, tally_t *t, bool reset,
                                 moq_result_t rc)
{
    int deliveries = 1;
    while (rc == MOQ_ERR_WOULD_BLOCK && deliveries < 32) {
        MOQ_TEST_CHECK(owner_kept(f));
        MOQ_TEST_CHECK(!unsubscribed_queued(f));
        MOQ_TEST_CHECK_EQ_INT(t->unsubscribed_a, 0);
        MOQ_TEST_CHECK(actions_queued(f->s) <= 1);
        moq_event_t e;
        if (!f->hold_events && moq_session_poll_events(f->s, &e, 1) > 0) {
            record_event(f, t, &e);
            moq_event_cleanup(&e);
        }
        poll_one_action(f, t);
        rc = deliver(f, reset);
        deliveries++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    note_released(f, t);
    poll_all(f, t);
    /* A STOP leaves the subscriber's direction open, so its identity is kept
     * -- a peer-stopped drain reference -- until the subscriber's FIN or
     * RESET. */
    bool keep = !reset && !f->peer_fin;
    MOQ_TEST_CHECK(drain_ref_contains(f->s, f->ref_a) == keep);
    if (keep)
        MOQ_TEST_CHECK_EQ_INT((int)drain_ref_reason(f->s, f->ref_a),
                              (int)MOQ_DRAIN_PEER_STOPPED);
    /* The subscriber's signal on the other direction changes nothing, and
     * then no identity of the request stream remains. */
    MOQ_TEST_CHECK_EQ_INT((int)deliver(f, !reset), (int)MOQ_OK);
    poll_all(f, t);
    MOQ_TEST_CHECK(!drain_ref_contains(f->s, f->ref_a));
    return deliveries;
}

/* The settled outcome every row shares. `code[i]` is the reset code A's live
 * subgroup `i` is owed: CANCELLED unless something else reset it first. The
 * FIN-closed subgroup is never reset by the cancellation; with a subgroup
 * delivery timeout its FIN-started timer resets it once with
 * DELIVERY_TIMEOUT, since this fixture never reports the stream committed. */
static void expect_settled(fixture_t *f, const tally_t *t, int updated_b,
                           const uint64_t *code)
{
    for (int i = 0; i < live_count(f); i++) {
        MOQ_TEST_CHECK_EQ_INT(t->resets[i], 1);
        MOQ_TEST_CHECK_EQ_U64(t->reset_code[i], code ? code[i] : 0x1);
        MOQ_TEST_CHECK_EQ_INT(t->writes[i], 0);
    }
    MOQ_TEST_CHECK_EQ_INT(t->resets[SG_A_FIN], f->o.sg_timeout_ms ? 1 : 0);
    if (f->o.sg_timeout_ms)
        MOQ_TEST_CHECK_EQ_U64(t->reset_code[SG_A_FIN], 0x2);
    MOQ_TEST_CHECK_EQ_INT(t->writes[SG_A_FIN], 0);
    MOQ_TEST_CHECK_EQ_INT(t->resets[SG_B_OPEN], 0);
    MOQ_TEST_CHECK_EQ_INT(t->writes[SG_B_OPEN], 0);
    for (int i = 0; i < SG_COUNT; i++) {
        bool timeout = (i == SG_A_FIN && f->o.sg_timeout_ms != 0) ||
                       (i == SG_A_OPEN && f->o.obj_timeout_ms != 0);
        MOQ_TEST_CHECK_EQ_INT(t->outgoing_terminals[i], timeout ? 1 : 0);
    }
    MOQ_TEST_CHECK_EQ_INT(t->other_actions, 0);
    /* A migrated request completes silently; any other cancellation surfaces
     * exactly one UNSUBSCRIBED. Either way every reset is accounted for when
     * A is released. */
    MOQ_TEST_CHECK_EQ_INT(t->unsubscribed_a, f->o.goaway ? 0 : 1);
    if (!f->o.goaway)
        MOQ_TEST_CHECK_EQ_INT(t->committed_at_unsubscribed, live_count(f));
    MOQ_TEST_CHECK_EQ_INT(t->committed_at_release, live_count(f));
    MOQ_TEST_CHECK_EQ_INT(t->updated_b, updated_b);
    MOQ_TEST_CHECK_EQ_INT(t->other_events, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f->s), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(sub_resolve_handle(f->s, f->sub_a) < 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)request_registry_find_by_streamref(f->s, f->ref_a).kind,
        (int)MOQ_REQ_NONE);
    MOQ_TEST_CHECK_EQ_INT(live_under(f->s, f->sub_a), 0);

    MOQ_TEST_CHECK(sub_resolve_handle(f->s, f->sub_b) >= 0);
    MOQ_TEST_CHECK_EQ_INT((int)write_whole(f, f->sg[SG_B_OPEN], 9), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(drain_writes_on(f->s, f->sg_ref[SG_B_OPEN]), 1);
}

/* An unrelated event left queued: B's REQUEST_UPDATE of its subscriber
 * priority, surfaced as SUBSCRIBE_UPDATED and not yet polled. */
static void fill_event_queue(fixture_t *f)
{
    uint8_t msg[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_subscriber_priority = true;
    p.subscriber_priority = 9;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_update(&w, f->next_rid, &p), (int)MOQ_OK);
    f->next_rid += 2;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f->s, f->ref_b, msg,
            moq_buf_writer_offset(&w), false, f->now), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(f->s, &a, 1) > 0) moq_action_cleanup(&a);
    MOQ_TEST_CHECK_EQ_SIZE(events_queued(f->s), (size_t)1);
}

/* B keeps writing while A's cancellation is pending: exactly one write on
 * B's own stream. */
static void expect_b_writes(fixture_t *f)
{
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f->s), (size_t)0);
    MOQ_TEST_CHECK_EQ_INT((int)write_whole(f, f->sg[SG_B_OPEN], 5), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(drain_writes_on(f->s, f->sg_ref[SG_B_OPEN]), 1);
}

/* -- Operations under the subscription ------------------------------ */

typedef enum {
    OP_OPEN,            /* a new subgroup */
    OP_WRITE,           /* a whole object on an open subgroup */
    OP_STATUS_OBJECT,   /* an END_OF_GROUP status object on an open subgroup */
    OP_BEGIN,           /* begin an object on an open subgroup */
    OP_CONTINUE,        /* more payload of a partly written object */
    OP_END,             /* end a fully written object */
    OP_CLOSE,           /* FIN an open subgroup */
    OP_DATAGRAM,        /* an object datagram */
    OP_STATUS_DATAGRAM, /* a status datagram */
    OP_GOAWAY,          /* a per-request GOAWAY */
    OP_DONE,            /* PUBLISH_DONE */
    OP_COUNT
} op_t;

static const char *const k_op_names[OP_COUNT] = {
    "open subgroup", "whole-object write", "status object", "begin object",
    "continuation write", "end object", "close subgroup", "object datagram",
    "status datagram", "request GOAWAY", "PUBLISH_DONE",
};

/* A live subgroup of A of `shape`, still in that shape's state, that no
 * reset has reached. */
static int untouched(const fixture_t *f, const tally_t *t, shape_t shape)
{
    moq_sg_state_t want = shape == SHAPE_OPEN ? MOQ_SG_OPEN : MOQ_SG_STREAMING;
    for (int i = 0; i < live_count(f); i++)
        if (sg_shape(i) == shape && sg_state(f, i) == want &&
            t->resets[i] == 0 && !reset_queued_for(f, i))
            return i;
    return -1;
}

/* Run `op` under A, with otherwise valid arguments. A payload the call
 * refuses must come back with no reference kept. */
static moq_result_t run_op(fixture_t *f, const tally_t *t, op_t op)
{
    moq_result_t rc = MOQ_ERR_INTERNAL;
    int target = -1;
    moq_rcbuf_t *buf = NULL;
    switch (op) {
    case OP_OPEN: {
        moq_subgroup_cfg_t c;
        moq_subgroup_cfg_init(&c);
        c.group_id = 40;
        moq_subgroup_handle_t h = MOQ_SUBGROUP_INVALID;
        rc = moq_session_open_subgroup(f->s, f->sub_a, &c, f->now, &h);
        break;
    }
    case OP_WRITE:
        target = untouched(f, t, SHAPE_OPEN);
        buf = payload(4);
        if (target >= 0 && buf)
            rc = moq_session_write_object(f->s, f->sg[target], 1, buf, f->now);
        break;
    case OP_STATUS_OBJECT:
        target = untouched(f, t, SHAPE_OPEN);
        if (target >= 0)
            rc = moq_session_write_status_object(f->s, f->sg[target], 2,
                                                 MOQ_OBJECT_END_OF_GROUP, f->now);
        break;
    case OP_BEGIN: {
        target = untouched(f, t, SHAPE_OPEN);
        moq_begin_object_cfg_t bc;
        moq_begin_object_cfg_init(&bc);
        bc.object_id = 3;
        bc.payload_length = 8;
        if (target >= 0)
            rc = moq_session_begin_object_ex(f->s, f->sg[target], &bc, f->now);
        break;
    }
    case OP_CONTINUE:
        target = untouched(f, t, SHAPE_PARTIAL);
        buf = payload(4);
        if (target >= 0 && buf)
            rc = moq_session_write_object_data(f->s, f->sg[target], buf, f->now);
        break;
    case OP_END:
        target = untouched(f, t, SHAPE_FULL);
        if (target >= 0)
            rc = moq_session_end_object(f->s, f->sg[target], f->now);
        break;
    case OP_CLOSE:
        target = untouched(f, t, SHAPE_OPEN);
        if (target >= 0)
            rc = moq_session_close_subgroup(f->s, f->sg[target], f->now);
        break;
    case OP_DATAGRAM:
        buf = payload(4);
        if (buf)
            rc = moq_session_send_object_datagram(f->s, f->sub_a, 30, 0, 0, false,
                                                  buf, NULL, 0, f->now);
        break;
    case OP_STATUS_DATAGRAM:
        rc = moq_session_send_status_datagram(f->s, f->sub_a, 30, 1, 0,
                                              MOQ_OBJECT_END_OF_GROUP, f->now);
        break;
    case OP_GOAWAY: {
        moq_request_goaway_cfg_t g;
        moq_request_goaway_cfg_init(&g);
        rc = moq_session_request_goaway_subscribe(f->s, f->sub_a, &g, f->now);
        break;
    }
    case OP_DONE: {
        moq_done_subscribe_cfg_t d;
        moq_done_subscribe_cfg_init(&d);
        rc = moq_session_done_subscribe(f->s, f->sub_a, &d, f->now);
        break;
    }
    case OP_COUNT:
        break;
    }
    if (op == OP_WRITE || op == OP_STATUS_OBJECT || op == OP_BEGIN ||
        op == OP_CONTINUE || op == OP_END || op == OP_CLOSE)
        MOQ_TEST_CHECK(target >= 0);
    if (buf) {
        if (rc != MOQ_OK) MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(buf), 1);
        moq_rcbuf_decref(buf);
    }
    return rc;
}

/* `op` under the cancelled A: refused as WRONG_STATE with room in the action
 * queue, nothing queued, A's ownership as it was. */
static void expect_refused(fixture_t *f, const tally_t *t, op_t op)
{
    MOQ_TEST_CHECK(actions_queued(f->s) < f->s->action_cap);
    size_t before = actions_queued(f->s);
    int live = live_under(f->s, f->sub_a);
    moq_result_t rc = run_op(f, t, op);
    if (rc != MOQ_ERR_WRONG_STATE)
        fprintf(stderr, "  %s under the cancelled subscription returned %d\n",
                k_op_names[op], (int)rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f->s), before);
    MOQ_TEST_CHECK_EQ_INT(live_under(f->s, f->sub_a), live);
    MOQ_TEST_CHECK(owner_kept(f));
}

static void expect_all_refused(fixture_t *f, const tally_t *t)
{
    for (int op = 0; op < OP_COUNT; op++)
        expect_refused(f, t, (op_t)op);
}

/* -- Rows: the cancellation delivered until taken ------------------- */

typedef enum {
    PRESSURE_NONE,
    PRESSURE_EVENTS,   /* the event queue holds an unrelated event */
    PRESSURE_ACTIONS,  /* the action queue holds an unrelated action */
    PRESSURE_DRAIN,    /* no room for the STOP's drain reference */
} pressure_t;

static const char *const k_pressure_names[] = {
    "", ", event queue full", ", action queue full", ", drain table full",
};

static const char *migrated(bool goaway)
{
    return goaway ? "after GOAWAY, " : "";
}

static void test_cancel(bool reset, pressure_t pressure, bool goaway)
{
    char what[96];
    snprintf(what, sizeof(what), "%s%s%s", migrated(goaway),
             reset ? "RESET" : "STOP", k_pressure_names[pressure]);
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.goaway = goaway;
    if (!open_row(&f, what, o)) return;
    /* A migrated request owes no event, so a full event queue cannot hold
     * its cancellation back: nothing reads events until it is done. */
    bool silent_events = goaway && pressure == PRESSURE_EVENTS;
    f.hold_events = silent_events;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t saved_cap = f.s->drain_ref_cap;
    if (pressure == PRESSURE_EVENTS) fill_event_queue(&f);
    if (pressure == PRESSURE_ACTIONS) {
        MOQ_TEST_CHECK_EQ_INT((int)write_whole(&f, f.sg[SG_B_OPEN], 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
    }
    if (pressure == PRESSURE_DRAIN) f.s->drain_ref_cap = f.s->drain_ref_count;

    /* Only a missing record of the signal's own refuses it: the UNSUBSCRIBED
     * event a migrated request does not owe, or a STOP's drain reference. */
    bool refused = (pressure == PRESSURE_EVENTS && !goaway) ||
                   pressure == PRESSURE_DRAIN;
    moq_result_t rc = deliver(&f, reset);
    if (!refused) {
        /* Taken at once: A is released, the reset that fits is queued and
         * the rest are owed by their entries; B is untouched. */
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK(sub_resolve_handle(f.s, f.sub_a) < 0);
        MOQ_TEST_CHECK(unsubscribed_queued(&f) == !goaway);
        MOQ_TEST_CHECK_EQ_INT(live_under(f.s, f.sub_a), 0);
        MOQ_TEST_CHECK_EQ_INT(accounted(&f, &t), live_count(&f));
        MOQ_TEST_CHECK(sg_state(&f, SG_B_OPEN) == MOQ_SG_OPEN);
        if (pressure == PRESSURE_ACTIONS) {
            /* B's write is ahead of every reset in the queue. */
            poll_one_action(&f, &t);
            MOQ_TEST_CHECK_EQ_INT(t.writes[SG_B_OPEN], 1);
            t.writes[SG_B_OPEN] = 0;
        }
    } else {
        /* Refused before any of the cancellation's output: nothing of it
         * queued, owner and subgroups in place -- but already cancelled. */
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!unsubscribed_queued(&f));
        MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s),
                               (size_t)(pressure == PRESSURE_ACTIONS ? 1 : 0));
        MOQ_TEST_CHECK(owner_kept(&f));
        MOQ_TEST_CHECK_EQ_INT(live_under(f.s, f.sub_a), SG_A_LIVE);
        /* Only the unrelated write is polled, and no delivery is made. */
        if (pressure == PRESSURE_ACTIONS)
            MOQ_TEST_CHECK_EQ_INT(drain_writes_on(f.s, f.sg_ref[SG_B_OPEN]), 1);
        expect_all_refused(&f, &t);
        f.s->drain_ref_cap = saved_cap;
    }
    /* One delivery, plus the refused one when the first found no room. */
    int deliveries = redeliver_until_taken(&f, &t, reset, rc);
    MOQ_TEST_CHECK_EQ_INT(deliveries, refused ? 2 : 1);
    expect_settled(&f, &t, pressure == PRESSURE_EVENTS ? 1 : 0, NULL);

    /* A new subscription takes over A's slot under a new handle and opens as
     * many subgroups as A held: A's subgroup slots were all released. */
    moq_stream_ref_t ref_c = moq_stream_ref_from_u64(0x90008);
    moq_subscription_t sub_c = subscribe(&f, ref_c, "c", 0, 0, false);
    MOQ_TEST_CHECK_EQ_INT(sub_resolve_handle(f.s, sub_c), f.slot_a);
    MOQ_TEST_CHECK(!moq_subscription_eq(sub_c, f.sub_a));
    for (int i = 0; i <= SG_A_FIN; i++) {
        moq_subgroup_handle_t h = MOQ_SUBGROUP_INVALID;
        moq_stream_ref_t r = moq_stream_ref_from_u64(0);
        MOQ_TEST_CHECK_EQ_INT((int)open_sg(&f, sub_c, (uint64_t)(10 + i), &h, &r),
                              (int)MOQ_OK);
        if (moq_subgroup_is_valid(h)) MOQ_TEST_CHECK_EQ_INT(sg_index(&f, r), -1);
    }
    MOQ_TEST_CHECK(sub_resolve_handle(f.s, f.sub_a) < 0);
    MOQ_TEST_CHECK_EQ_INT(drain_count(f.s), 0);

    /* A late signal on A's old request stream reaches nothing: C, which
     * holds A's slot now, is untouched and nothing is queued or surfaced. */
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, false), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, true), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(sub_resolve_handle(f.s, sub_c), f.slot_a);
    MOQ_TEST_CHECK(!sub_cancel_pending(f.s, sub_c));
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(events_queued(f.s), (size_t)0);

    /* A's old subgroup handles are stale. */
    MOQ_TEST_CHECK_EQ_INT((int)write_whole(&f, f.sg[SG_A_OPEN], 1),
                          (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_close_subgroup(f.s, f.sg[SG_A_OPEN2], f.now),
        (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)write_part(&f, f.sg[SG_A_PARTIAL], 4),
                          (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT(drain_count(f.s), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);

    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* -- Rows: one operation while a cancellation is pending ------------ */

/* A cancellation left pending: the first delivery finds no room for its own
 * record -- for a RESET the UNSUBSCRIBED event (an unrelated event fills the
 * queue), for a STOP the drain reference -- and is refused before any output,
 * the owner kept and already cancelled. Returns the drain capacity to restore
 * before the signal is delivered again (SIZE_MAX when none was taken). In
 * this fixture a migrated RESET owes neither record (and no fill or join), so
 * it cannot be left pending here. */
static size_t pending_pass(fixture_t *f, tally_t *t, bool reset)
{
    (void)t;
    size_t saved = SIZE_MAX;
    if (reset) {
        fill_event_queue(f);
    } else {
        saved = f->s->drain_ref_cap;
        f->s->drain_ref_cap = f->s->drain_ref_count;
    }
    moq_result_t rc = deliver(f, reset);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f->s), (size_t)0);
    MOQ_TEST_CHECK(!unsubscribed_queued(f));
    MOQ_TEST_CHECK(owner_kept(f));
    MOQ_TEST_CHECK(sub_cancel_pending(f->s, f->sub_a));
    MOQ_TEST_CHECK_EQ_INT(live_under(f->s, f->sub_a), live_count(f));
    return saved;
}

static void pending_restore(fixture_t *f, size_t saved)
{
    if (saved != SIZE_MAX) f->s->drain_ref_cap = saved;
}

static void test_partial_op(bool reset, op_t op, bool goaway)
{
    char what[96];
    snprintf(what, sizeof(what), "%s%s, pending, %s", migrated(goaway),
             reset ? "RESET" : "STOP", k_op_names[op]);
    /* In this fixture a migrated RESET owes no event, drain reference, fill
     * or join, so nothing can leave it pending; that is a property of the
     * fixture, not a guarantee that a migrated cancellation never blocks
     * (joins: test_join_against_cancelled here; fills:
     * tests/unit/test_teardown_capacity.c). */
    if (reset && goaway) return;
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.goaway = goaway;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t saved = pending_pass(&f, &t, reset);
    expect_refused(&f, &t, op);
    expect_b_writes(&f);
    pending_restore(&f, saved);
    int deliveries = redeliver_until_taken(&f, &t, reset, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(deliveries, 2);
    expect_settled(&f, &t, reset ? 1 : 0, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* The same calls on an uncancelled subscription succeed: the refusals are
 * the cancellation's. PUBLISH_DONE needs no subgroup open, so it is shown
 * against a cancellation by test_terminal_race instead. */
static void test_ops_uncancelled(void)
{
    const char *what = "operations on an uncancelled subscription";
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    static const op_t ops[] = {
        OP_WRITE, OP_STATUS_OBJECT, OP_CLOSE, OP_BEGIN, OP_CONTINUE, OP_END,
        OP_OPEN, OP_DATAGRAM, OP_STATUS_DATAGRAM, OP_GOAWAY,
    };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        moq_result_t rc = run_op(&f, &t, ops[i]);
        if (rc != MOQ_OK)
            fprintf(stderr, "  %s on an uncancelled subscription returned %d\n",
                    k_op_names[ops[i]], (int)rc);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(drain_count(f.s), ops[i] == OP_END ? 0 : 1);
    }
    MOQ_TEST_CHECK(!sub_cancel_pending(f.s, f.sub_a));
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* -- Rows: pressure and signals between passes ---------------------- */

/* A STOP is pending for want of a drain reference, and the event queue then
 * fills: the next delivery is refused with nothing queued, and resumes once
 * the event is read. (A pending RESET already waits on the event queue.) */
static void test_event_pressure_between(bool reset)
{
    char what[64];
    snprintf(what, sizeof(what), "%s, event queue fills between passes",
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t saved = pending_pass(&f, &t, reset);
    fill_event_queue(&f);
    pending_restore(&f, saved);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK(!unsubscribed_queued(&f));
    expect_refused(&f, &t, OP_CLOSE);
    int deliveries = redeliver_until_taken(&f, &t, reset, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(deliveries, 2);
    expect_settled(&f, &t, 1, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* The same signal again before the queued reset is read: the first took the
 * cancellation, the second finds nothing more to do, nothing is reset twice
 * and nothing is surfaced twice. */
static void test_duplicate_without_room(bool reset, bool goaway)
{
    char what[64];
    snprintf(what, sizeof(what), "%s%s twice without room", migrated(goaway),
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.goaway = goaway;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
    MOQ_TEST_CHECK_EQ_SIZE(events_queued(f.s), (size_t)(goaway ? 0 : 1));
    MOQ_TEST_CHECK_EQ_INT(accounted(&f, &t), live_count(&f));
    int deliveries = redeliver_until_taken(&f, &t, reset, MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(deliveries, 1);
    expect_settled(&f, &t, 0, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* The other signal arrives while the first is still pending and finishes the
 * cancellation; the first then arrives again and changes nothing. After a
 * RESET, a STOP finding no room for a drain reference is not refused for it:
 * the RESET ended the peer's direction, and no identity is kept. */
static void test_crossed(bool reset_first, bool goaway)
{
    char what[64];
    snprintf(what, sizeof(what), "%s%s, then %s while pending", migrated(goaway),
             reset_first ? "RESET" : "STOP", reset_first ? "STOP" : "RESET");
    if (reset_first && goaway) return;   /* see test_partial_op */
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.goaway = goaway;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t pending_cap = pending_pass(&f, &t, reset_first);
    size_t saved_cap = pending_cap != SIZE_MAX ? pending_cap : f.s->drain_ref_cap;
    if (reset_first) f.s->drain_ref_cap = f.s->drain_ref_count;
    moq_result_t rc = deliver(&f, !reset_first);
    int deliveries = 1;
    while (rc == MOQ_ERR_WOULD_BLOCK && deliveries < 32) {
        MOQ_TEST_CHECK(owner_kept(&f));
        MOQ_TEST_CHECK(!unsubscribed_queued(&f));
        MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
        moq_event_t e;
        if (moq_session_poll_events(f.s, &e, 1) > 0) {
            record_event(&f, &t, &e);
            moq_event_cleanup(&e);
        }
        rc = deliver(&f, !reset_first);
        deliveries++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    /* A STOP after a pending RESET waits only for the event the RESET was
     * refused for; a RESET after a pending STOP needs no drain reference. */
    MOQ_TEST_CHECK_EQ_INT(deliveries, reset_first ? 2 : 1);
    note_released(&f, &t);
    poll_all(&f, &t);
    f.s->drain_ref_cap = saved_cap;
    MOQ_TEST_CHECK(!drain_ref_contains(f.s, f.ref_a));
    /* The first signal, still pending at the transport, arrives again. */
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset_first), (int)MOQ_OK);
    poll_all(&f, &t);
    MOQ_TEST_CHECK(!drain_ref_contains(f.s, f.ref_a));
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, !reset_first), (int)MOQ_OK);
    poll_all(&f, &t);
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)0);
    expect_settled(&f, &t, reset_first ? 1 : 0, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* While a STOP is pending, the subscriber's REQUEST_UPDATE (Forward 0, a new
 * priority) and then its FIN arrive on the request stream: both are taken
 * without effect -- no event, no response, Forward and priority unchanged,
 * nothing buffered, no close -- and after the FIN no identity is kept for the
 * stream once A is released, even with no drain room. */
static void test_late_request_bytes(void)
{
    const char *what = "STOP, then REQUEST_UPDATE and FIN while pending";
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t pending_cap = pending_pass(&f, &t, false);
    const moq_sub_entry_t *e = &f.s->subs[f.slot_a];
    bool forward = e->forward;
    uint8_t priority = e->subscriber_priority;
    uint8_t msg[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = true;
    p.forward = 0;
    p.has_subscriber_priority = true;
    p.subscriber_priority = (uint8_t)(priority + 1);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_update(&w, f.next_rid, &p), (int)MOQ_OK);
    f.next_rid += 2;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f.s, f.ref_a, msg,
            moq_buf_writer_offset(&w), false, f.now), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(events_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK(e->forward == forward);
    MOQ_TEST_CHECK_EQ_INT((int)e->subscriber_priority, (int)priority);
    MOQ_TEST_CHECK_EQ_SIZE(e->rs.recv_len, (size_t)0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f.s, f.ref_a, NULL, 0, true, f.now),
        (int)MOQ_OK);
    MOQ_TEST_CHECK(e->rs.recv_fin);
    f.peer_fin = true;
    MOQ_TEST_CHECK(owner_kept(&f));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);
    expect_refused(&f, &t, OP_WRITE);
    /* Still no drain room: after the FIN the STOP needs none. */
    int deliveries = redeliver_until_taken(&f, &t, false, MOQ_ERR_WOULD_BLOCK);
    pending_restore(&f, pending_cap);
    MOQ_TEST_CHECK_EQ_INT(deliveries, 2);
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)0);
    expect_settled(&f, &t, 0, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* While a STOP is pending, the subscriber's FIN arrives -- empty, or
 * riding a last message. It records that the subscriber's direction ended
 * and nothing more: the owner and every reset still owed survive it, nothing
 * is read from the bytes, and the redelivered STOP finishes the cancellation
 * keeping no identity for the stream. */
static void test_late_fin(bool goaway, bool with_bytes)
{
    char what[96];
    snprintf(what, sizeof(what), "%sSTOP, then %s while pending",
             migrated(goaway), with_bytes ? "bytes with FIN" : "empty FIN");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.goaway = goaway;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t pending_cap = pending_pass(&f, &t, false);
    int live = live_under(f.s, f.sub_a);
    MOQ_TEST_CHECK_EQ_INT(live, SG_A_LIVE);
    uint8_t msg[64];
    size_t n = 0;
    if (with_bytes) {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_msg_params_t p;
        memset(&p, 0, sizeof(p));
        p.has_subscriber_priority = true;
        p.subscriber_priority = 3;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, f.next_rid, &p), (int)MOQ_OK);
        f.next_rid += 2;
        n = moq_buf_writer_offset(&w);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f.s, f.ref_a, n ? msg : NULL, n,
                                              true, f.now),
        (int)MOQ_OK);
    f.peer_fin = true;
    MOQ_TEST_CHECK(owner_kept(&f));
    MOQ_TEST_CHECK(f.s->subs[f.slot_a].rs.recv_fin);
    MOQ_TEST_CHECK_EQ_INT(live_under(f.s, f.sub_a), live);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(events_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);
    expect_refused(&f, &t, OP_CLOSE);
    /* Still no drain room: after the FIN the STOP needs none. */
    int deliveries = redeliver_until_taken(&f, &t, false, MOQ_ERR_WOULD_BLOCK);
    pending_restore(&f, pending_cap);
    MOQ_TEST_CHECK_EQ_INT(deliveries, 2);
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)0);
    expect_settled(&f, &t, 0, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* With no subgroup open, a STOP ends A at once, leaving the subscriber's
 * direction open: its identity stays as a peer-stopped drain reference until
 * the subscriber's FIN or RESET. A repeated STOP changes nothing. The
 * subscriber's trailing FIN -- or its RESET -- is absorbed by that reference
 * and retires it, never read as a new request and never closing the session;
 * a STOP after that brings nothing back. */
static void test_stop_keeps_identity(bool goaway, bool end_with_reset)
{
    char what[96];
    snprintf(what, sizeof(what), "%sSTOP with no subgroup, then %s",
             migrated(goaway), end_with_reset ? "RESET" : "FIN");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.no_subgroups = true;
    o.goaway = goaway;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, false), (int)MOQ_OK);
    poll_all(&f, &t);
    MOQ_TEST_CHECK_EQ_INT(t.unsubscribed_a, goaway ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(t.other_events, 0);
    MOQ_TEST_CHECK_EQ_INT(t.other_actions, 0);
    MOQ_TEST_CHECK(sub_resolve_handle(f.s, f.sub_a) < 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)request_registry_find_by_streamref(f.s, f.ref_a).kind,
        (int)MOQ_REQ_NONE);
    MOQ_TEST_CHECK(drain_ref_contains(f.s, f.ref_a));
    MOQ_TEST_CHECK_EQ_INT((int)drain_ref_reason(f.s, f.ref_a),
                          (int)MOQ_DRAIN_PEER_STOPPED);
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)1);

    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, false), (int)MOQ_OK);
    MOQ_TEST_CHECK(drain_ref_contains(f.s, f.ref_a));
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)1);

    if (end_with_reset)
        MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, true), (int)MOQ_OK);
    else
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(f.s, f.ref_a, NULL, 0, true,
                                                  f.now), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(!drain_ref_contains(f.s, f.ref_a));
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)0);

    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, false), (int)MOQ_OK);
    MOQ_TEST_CHECK(!drain_ref_contains(f.s, f.ref_a));
    MOQ_TEST_CHECK_EQ_SIZE(f.s->drain_ref_count, (size_t)0);
    poll_all(&f, &t);
    MOQ_TEST_CHECK_EQ_INT(t.unsubscribed_a, goaway ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(t.other_events, 0);
    MOQ_TEST_CHECK_EQ_INT(t.other_actions, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(sub_resolve_handle(f.s, f.sub_b) >= 0);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* -- Rows: other resets standing in for the cancellation's ---------- */

/* An application reset queued during the cancellation discharges that stream:
 * it carries the application's code and is not followed by a second. */
static void test_app_reset(bool reset)
{
    char what[64];
    snprintf(what, sizeof(what), "%s, application reset while pending",
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t pending_cap = pending_pass(&f, &t, reset);
    int target = untouched(&f, &t, SHAPE_PARTIAL);
    MOQ_TEST_CHECK(target >= 0);
    uint64_t code[SG_A_LIVE];
    for (int i = 0; i < SG_A_LIVE; i++) code[i] = 0x1;
    if (target >= 0) {
        code[target] = 0x7;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reset_subgroup(f.s, f.sg[target], 0x7, f.now),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
        poll_one_action(&f, &t);
    }
    pending_restore(&f, pending_cap);
    (void)redeliver_until_taken(&f, &t, reset, MOQ_ERR_WOULD_BLOCK);
    expect_settled(&f, &t, reset ? 1 : 0, code);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* A subgroup delivery timeout expires while the cancellation is pending.
 * Draft-18 §8 starts that timer once every object of the subgroup is
 * published, so only the FIN-closed subgroup has one: it is reset exactly
 * once, with DELIVERY_TIMEOUT, and the cancellation does not reset it again.
 * The live subgroups have no subgroup timer and are each reset exactly once
 * with CANCELLED. */
static void test_timeout_reset(bool reset)
{
    char what[64];
    snprintf(what, sizeof(what), "%s, delivery timeouts while pending",
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.sg_timeout_ms = 5;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t pending_cap = pending_pass(&f, &t, reset);
    f.now = T_NOW + 6000;
    moq_result_t trc = moq_session_tick(f.s, f.now);
    MOQ_TEST_CHECK(trc == MOQ_OK || trc == MOQ_ERR_WOULD_BLOCK);
    poll_all(&f, &t);
    pending_restore(&f, pending_cap);
    (void)redeliver_until_taken(&f, &t, reset, MOQ_ERR_WOULD_BLOCK);
    expect_settled(&f, &t, reset ? 1 : 0, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* An object delivery timeout on a live subgroup races the pending
 * cancellation. Draft-18 §8 checks an object's age before passing it to the
 * transport, so the timer acts on a write still queued: one whole object on
 * A's first open subgroup is queued and left unpolled, the cancellation is
 * refused, and the object expires. The expired write becomes that stream's
 * DELIVERY_TIMEOUT reset, which stands in for the cancellation's own: the
 * stream is reset exactly once and never written, and every other live
 * subgroup is reset exactly once with CANCELLED. */
static void test_object_timeout_race(bool reset)
{
    char what[64];
    snprintf(what, sizeof(what), "%s, object timeout on a queued write",
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.obj_timeout_ms = 5;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    size_t saved = SIZE_MAX;
    if (reset) {
        fill_event_queue(&f);
    } else {
        saved = f.s->drain_ref_cap;
        f.s->drain_ref_cap = f.s->drain_ref_count;
    }
    MOQ_TEST_CHECK_EQ_INT((int)write_whole(&f, f.sg[SG_A_OPEN], 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
    MOQ_TEST_CHECK(!unsubscribed_queued(&f));
    MOQ_TEST_CHECK(owner_kept(&f));
    MOQ_TEST_CHECK(sub_cancel_pending(f.s, f.sub_a));
    MOQ_TEST_CHECK_EQ_INT(live_under(f.s, f.sub_a), live_count(&f));

    f.now = T_NOW + 6000;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(f.s, f.now),
                         (int)(reset ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
    if (reset)
        MOQ_TEST_CHECK(sg_state(&f, SG_A_OPEN) == MOQ_SG_TERMINAL_OWED);
    else
        MOQ_TEST_CHECK(reset_queued_for(&f, SG_A_OPEN));
    MOQ_TEST_CHECK(owner_kept(&f));
    poll_all(&f, &t);
    MOQ_TEST_CHECK_EQ_INT(t.resets[SG_A_OPEN], 1);
    MOQ_TEST_CHECK_EQ_U64(t.reset_code[SG_A_OPEN], 0x2);
    MOQ_TEST_CHECK_EQ_INT(t.writes[SG_A_OPEN], 0);
    for (int i = 0; i < SG_COUNT; i++)
        if (i != SG_A_OPEN) MOQ_TEST_CHECK_EQ_INT(t.resets[i], 0);

    pending_restore(&f, saved);
    (void)redeliver_until_taken(&f, &t, reset, MOQ_ERR_WOULD_BLOCK);
    uint64_t code[SG_A_LIVE];
    for (int i = 0; i < SG_A_LIVE; i++) code[i] = 0x1;
    code[SG_A_OPEN] = 0x2;
    expect_settled(&f, &t, reset ? 1 : 0, code);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* -- Rows: local terminals against a pending cancellation ----------- */

/* The cancellation arrives with the event queue full and is refused before
 * any output. Local terminals -- PUBLISH_DONE and GOAWAY for an established
 * subscription with no subgroup open; accept, reject and redirect for a
 * pending one -- are then refused, and the redelivered signal ends the
 * subscription with exactly one UNSUBSCRIBED. */
static void test_terminal_race(bool reset, bool pending)
{
    char what[96];
    snprintf(what, sizeof(what), "%s, local terminals against a pending %s",
             reset ? "RESET" : "STOP", pending ? "request" : "subscription");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.pending = pending;
    o.no_subgroups = !pending;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    fill_event_queue(&f);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset), (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(owner_kept(&f));
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
    if (pending) {
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe(f.s, f.sub_a, &acc, f.now),
            (int)MOQ_ERR_WRONG_STATE);
        moq_reject_subscribe_cfg_t rej;
        moq_reject_subscribe_cfg_init(&rej);
        rej.error_code = (moq_request_error_t)0x1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(f.s, f.sub_a, &rej, f.now),
            (int)MOQ_ERR_WRONG_STATE);
        moq_reject_subscribe_cfg_t red;
        moq_reject_subscribe_cfg_init(&red);
        red.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        red.redirect.connect_uri = MOQ_BYTES_LITERAL("moqt://elsewhere");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(f.s, f.sub_a, &red, f.now),
            (int)MOQ_ERR_WRONG_STATE);
    } else {
        expect_refused(&f, &t, OP_DONE);
        expect_refused(&f, &t, OP_GOAWAY);
        expect_refused(&f, &t, OP_OPEN);
        expect_refused(&f, &t, OP_DATAGRAM);
    }
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)0);
    MOQ_TEST_CHECK(owner_kept(&f));
    (void)redeliver_until_taken(&f, &t, reset, MOQ_ERR_WOULD_BLOCK);
    expect_settled(&f, &t, 1, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* A Joining FETCH naming a pending subscription whose cancellation is still
 * owed is rejected at once with INVALID_JOINING_REQUEST_ID, never buffered
 * against it. */
static void test_join_against_cancelled(void)
{
    const char *what = "Joining FETCH against a cancelled pending request";
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.pending = true;
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    fill_event_queue(&f);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, false), (int)MOQ_ERR_WOULD_BLOCK);
    moq_d18_fetch_t fe;
    memset(&fe, 0, sizeof(fe));
    fe.request_id = f.next_rid;
    f.next_rid += 2;
    fe.fetch_type = 2;
    fe.joining_request_id = f.rid_a;
    fe.joining_start = 0;
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &fe), (int)MOQ_OK);
    moq_stream_ref_t jref = moq_stream_ref_from_u64(0x9000c);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f.s, jref, buf,
            moq_buf_writer_offset(&w), false, f.now), (int)MOQ_OK);
    int joins = 0;
    for (size_t i = 0; i < f.s->fetch_cap; i++)
        if (f.s->fetches[i].state == MOQ_FETCH_PENDING_JOIN) joins++;
    MOQ_TEST_CHECK_EQ_INT(joins, 0);
    int rejections = 0;
    moq_action_t a;
    while (moq_session_poll_actions(f.s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == jref._v) {
            moq_buf_reader_t rd;
            moq_buf_reader_init(&rd, a.u.send_bidi_stream.data,
                                a.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            moq_d18_request_error_t re;
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rd, &env),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_REQUEST_ERROR);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_request_error(env.payload, env.payload_len, &re),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(re.error_code,
                (uint64_t)MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
            MOQ_TEST_CHECK(a.u.send_bidi_stream.fin);
            rejections++;
        } else {
            t.other_actions++;
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(rejections, 1);
    MOQ_TEST_CHECK(owner_kept(&f));
    (void)redeliver_until_taken(&f, &t, false, MOQ_ERR_WOULD_BLOCK);
    expect_settled(&f, &t, 1, NULL);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* -- Row: a subscriber-role subscription ---------------------------- */

/* The publisher tearing down a subscription this session requested is not a
 * cancellation of anything this session publishes: it is never marked, under
 * pressure or after. */
static void test_subscriber_role(bool reset)
{
    char what[64];
    snprintf(what, sizeof(what), "%s of a subscriber-role request",
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.no_subgroups = true;
    if (!open_row(&f, what, o)) return;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("up") };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("x");
    moq_subscription_t mine = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(f.s, &cfg, f.now, &mine),
                          (int)MOQ_OK);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    moq_action_t a;
    while (moq_session_poll_actions(f.s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            ref = a.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    int slot = sub_resolve_handle(f.s, mine);
    MOQ_TEST_CHECK(slot >= 0);
    fill_event_queue(&f);
    moq_result_t rc = reset
        ? moq_session_on_bidi_stream_reset(f.s, ref, 0x1, f.now)
        : moq_session_on_bidi_stream_stop(f.s, ref, 0x1, f.now);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    if (slot >= 0) {
        MOQ_TEST_CHECK(!f.s->subs[slot].cancel_pending);
        MOQ_TEST_CHECK(!f.s->subs[slot].req_recv_reset);
    }
    MOQ_TEST_CHECK_EQ_INT(drain_count(f.s), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

/* -- Rows: fixture setup failures ----------------------------------- */

/* The fixture never hands a row a missing prerequisite, and a control can
 * provoke exactly one named setup refusal without hiding any other failure.
 * With room for two subgroups, A's third subgroup open is refused with
 * WOULD_BLOCK; with room for one subscription, B's SUBSCRIBE is turned away
 * with REQUEST_ERROR (EXCESSIVE_LOAD, the local-capacity code of draft-18
 * §10.6). The fixture stops at that step and reports failure without counting
 * it, leaving the session for the caller to destroy. Every step before it
 * must have gone cleanly: any failed check on the way is counted as usual. */
static void test_setup_refusal(bool subscriptions)
{
    const char *what = subscriptions ? "setup refusal: no room for B"
                                     : "setup refusal: no room for A's third subgroup";
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    if (subscriptions) {
        o.max_subscriptions = 1;
        o.expect = (fx_refusal_t){ FX_STEP_SUBSCRIBE_B, 0,
                                   MOQ_REQUEST_ERROR_EXCESSIVE_LOAD };
    } else {
        o.max_open_subgroups = 2;
        o.expect = (fx_refusal_t){ FX_STEP_A_OPEN, 2, MOQ_ERR_WOULD_BLOCK };
    }
    bool opened = fixture_open(&f, o);
    MOQ_TEST_CHECK(!opened);
    MOQ_TEST_CHECK_EQ_INT((int)f.refused.step, (int)o.expect.step);
    MOQ_TEST_CHECK_EQ_INT(f.refused.index, o.expect.index);
    MOQ_TEST_CHECK(f.refused.result == o.expect.result);
    MOQ_TEST_CHECK_EQ_INT(f.unexpected_count, 0);
    MOQ_TEST_CHECK(f.s != NULL);
    MOQ_TEST_CHECK(f.slot_a >= 0);
    if (!subscriptions && f.s)
        MOQ_TEST_CHECK_EQ_INT(live_under(f.s, f.sub_a), 2);
    if (f.s) moq_session_destroy(f.s);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
}

/* A different miss is not taken for the expected refusal: expecting A's
 * third subgroup open to be refused, but with room for only one subgroup, it
 * is A's second open that is refused. The fixture records nothing as the
 * expected refusal and reports the second open as an unexpected miss -- here
 * captured with its step instead of counted, which is all this control
 * changes. */
static void test_setup_other_refusal(void)
{
    const char *what = "setup refusal: an earlier miss is not the expected one";
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    o.max_open_subgroups = 1;
    o.expect = (fx_refusal_t){ FX_STEP_A_OPEN, 2, MOQ_ERR_WOULD_BLOCK };
    o.capture_unexpected = true;
    bool opened = fixture_open(&f, o);
    MOQ_TEST_CHECK(!opened);
    MOQ_TEST_CHECK_EQ_INT((int)f.refused.step, (int)FX_STEP_NONE);
    MOQ_TEST_CHECK_EQ_INT(f.unexpected_count, 1);
    MOQ_TEST_CHECK_EQ_INT((int)f.unexpected.step, (int)FX_STEP_A_OPEN);
    MOQ_TEST_CHECK_EQ_INT(f.unexpected.index, 1);
    if (f.s) MOQ_TEST_CHECK_EQ_INT(live_under(f.s, f.sub_a), 1);
    if (f.s) moq_session_destroy(f.s);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
}

/* -- Row: through the transport bridge ------------------------------ */

typedef struct bridge_fx {
    moq_session_t          *c, *sv;
    fake_endpoint_t         cep, sep;
    moq_transport_bridge_t *cb, *sb;
} bridge_fx_t;

/* One endpoint's writes land on the other bridge, by the stream id the
 * writer's endpoint allocated. */
static size_t bridge_feed(moq_transport_bridge_t *to, fake_endpoint_t *from,
                          uint64_t uni_base, uint64_t bidi_base)
{
    size_t n = from->count;
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *op = &from->ops[i];
        if (op->kind != FAKE_OP_WRITE) continue;
        if (op->stream_id >= uni_base && op->stream_id < uni_base + 1000)
            (void)held_bridge_uni_bytes(to, op->stream_id, op->data,
                                        op->data_len, op->fin, T_NOW);
        else if (op->stream_id >= bidi_base && op->stream_id < bidi_base + 1000)
            (void)moq_transport_bridge_on_peer_bidi_bytes(to, op->stream_id,
                op->data, op->data_len, op->fin, T_NOW);
    }
    fake_endpoint_clear_ops(from);
    return n;
}

static void bridge_shuttle(bridge_fx_t *b)
{
    for (int i = 0; i < 40; i++) {
        size_t n = 0;
        (void)held_bridge_service(b->cb, T_NOW);
        n += bridge_feed(b->sb, &b->cep, 1000, 2000);
        (void)held_bridge_service(b->sb, T_NOW);
        n += bridge_feed(b->cb, &b->sep, 3000, 4000);
        if (n == 0) return;
    }
}

static void test_bridge(bool reset, bool goaway)
{
    char what[64];
    snprintf(what, sizeof(what), "%s%s through the transport bridge",
             migrated(goaway), reset ? "RESET" : "STOP");
    int before = failures;
    bridge_fx_t b;
    memset(&b, 0, sizeof(b));
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = MOQ_VERSION_DRAFT_18;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    scfg.max_actions = 1;
    scfg.max_events = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&ccfg, 0, &b.c), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&scfg, 0, &b.sv), (int)MOQ_OK);
    if (!b.c || !b.sv) {
        if (b.c) moq_session_destroy(b.c);
        if (b.sv) moq_session_destroy(b.sv);
        fprintf(stderr, "FAIL %s: sessions\n", what);
        return;
    }
    held_endpoint_init(&b.cep, 1000, 2000);
    held_endpoint_init(&b.sep, 3000, 4000);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    MOQ_TEST_CHECK_EQ_INT((int)held_bridge_create(&bcfg, b.c, &b.cep.vtable,
                                                  &b.cep, &b.cb), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)held_bridge_create(&bcfg, b.sv, &b.sep.vtable,
                                                  &b.sep, &b.sb), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(b.c, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(b.sv, 0), (int)MOQ_OK);
    bridge_shuttle(&b);
    moq_event_t ev;
    while (moq_session_poll_events(b.c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(b.sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(b.sv), (int)MOQ_SESS_ESTABLISHED);

    /* The client's SUBSCRIBE, and the request stream it opened. */
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("a");
    moq_subscription_t csub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(b.c, &cfg, T_NOW, &csub),
                          (int)MOQ_OK);
    (void)held_bridge_service(b.cb, T_NOW);
    uint64_t sid = 0;
    for (size_t i = 0; i < b.cep.count; i++)
        if (b.cep.ops[i].kind == FAKE_OP_OPEN_BIDI) sid = b.cep.ops[i].stream_id;
    MOQ_TEST_CHECK(sid != 0);
    bridge_shuttle(&b);
    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(b.sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(b.sv, ssub, &acc, T_NOW),
                          (int)MOQ_OK);
    bridge_shuttle(&b);
    while (moq_session_poll_events(b.c, &ev, 1) > 0) moq_event_cleanup(&ev);

    /* Three open subgroups, one object each, every action taken by the
     * bridge; their transport stream ids. */
    enum { N = 3 };
    uint64_t data_sid[N] = { 0 };
    moq_subgroup_handle_t first = MOQ_SUBGROUP_INVALID;
    size_t found = 0;
    for (int i = 0; i < N; i++) {
        moq_subgroup_cfg_t c;
        moq_subgroup_cfg_init(&c);
        c.group_id = (uint64_t)(10 + i);
        moq_subgroup_handle_t h = MOQ_SUBGROUP_INVALID;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(b.sv, ssub, &c, T_NOW, &h), (int)MOQ_OK);
        if (i == 0) first = h;
        (void)held_bridge_service(b.sb, T_NOW);
        moq_rcbuf_t *buf = payload(4);
        if (buf) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_write_object(b.sv, h, 0, buf, T_NOW), (int)MOQ_OK);
            moq_rcbuf_decref(buf);
        }
        (void)held_bridge_service(b.sb, T_NOW);
    }
    if (goaway) {
        /* Our GOAWAY on the request stream; production goes on after it. */
        moq_request_goaway_cfg_t g;
        moq_request_goaway_cfg_init(&g);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_request_goaway_subscribe(b.sv, ssub, &g, T_NOW),
            (int)MOQ_OK);
        (void)held_bridge_service(b.sb, T_NOW);
        moq_rcbuf_t *buf = payload(4);
        if (buf) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_write_object(b.sv, first, 1, buf, T_NOW),
                (int)MOQ_OK);
            moq_rcbuf_decref(buf);
        }
        (void)held_bridge_service(b.sb, T_NOW);
    }
    for (size_t i = 0; i < b.sep.count; i++)
        if (b.sep.ops[i].kind == FAKE_OP_OPEN_UNI && found < N)
            data_sid[found++] = b.sep.ops[i].stream_id;
    MOQ_TEST_CHECK_EQ_SIZE(found, (size_t)N);
    fake_endpoint_clear_ops(&b.sep);

    /* The subscriber's signal, delivered once by the transport. The session
     * takes it at once (its event and any drain reference fit): the
     * subscription is released, one reset is queued and the others are owed
     * by their entries. If the bridge is ever refused instead, it keeps the
     * signal pending itself, the subscription already cancelled. */
    moq_result_t rc = reset
        ? held_bridge_reset(b.sb, sid, 0x1, T_NOW)
        : moq_transport_bridge_on_peer_stop_sending(b.sb, sid, 0x1, T_NOW);
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        const bridge_stream_entry_t *e = bridge_find_by_id(b.sb, sid);
        MOQ_TEST_CHECK(e != NULL);
        if (e) MOQ_TEST_CHECK(reset ? e->pending_reset : e->pending_stop);
        MOQ_TEST_CHECK(sub_cancel_pending(b.sv, ssub));
    } else {
        MOQ_TEST_CHECK(sub_resolve_handle(b.sv, ssub) < 0);
    }

    /* Only the bridge's own servicing from here: the application reads its
     * events and delivers nothing; the owed resets go out as the bridge takes
     * actions. */
    int unsubscribed = 0, other_events = 0;
    for (int i = 0; i < 64; i++) {
        (void)held_bridge_service(b.sb, T_NOW);
        while (moq_session_poll_events(b.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED &&
                moq_subscription_eq(ev.u.unsubscribed.sub, ssub))
                unsubscribed++;
            else
                other_events++;
            moq_event_cleanup(&ev);
        }
        if (sub_resolve_handle(b.sv, ssub) < 0 &&
            !moq_transport_bridge_has_pending(b.sb))
            break;
    }
    /* A migrated request completes silently; otherwise one UNSUBSCRIBED. */
    MOQ_TEST_CHECK_EQ_INT(unsubscribed, goaway ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(other_events, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(b.sb));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b.sb));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(b.sv), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(sub_resolve_handle(b.sv, ssub) < 0);

    /* Each data stream reset exactly once with CANCELLED, nothing more
     * written to it. */
    int resets[N] = { 0 }, writes = 0;
    for (size_t i = 0; i < b.sep.count; i++) {
        const fake_op_t *op = &b.sep.ops[i];
        for (int k = 0; k < N; k++) {
            if (op->stream_id != data_sid[k]) continue;
            if (op->kind == FAKE_OP_RESET) {
                resets[k]++;
                MOQ_TEST_CHECK_EQ_U64(op->error_code, 0x1);
            } else if (op->kind == FAKE_OP_WRITE) {
                writes++;
            }
        }
    }
    MOQ_TEST_CHECK(!b.sep.overflowed);
    for (int k = 0; k < N; k++) MOQ_TEST_CHECK_EQ_INT(resets[k], 1);
    MOQ_TEST_CHECK_EQ_INT(writes, 0);

    if (failures != before) fprintf(stderr, "  in %s\n", what);
    held_bridge_destroy(b.cb);
    held_bridge_destroy(b.sb);
    moq_session_destroy(b.c);
    moq_session_destroy(b.sv);
}

/* -- Row: A's slot reused while its resets are owed ------------------ */

/* With B's write filling the action queue, the cancellation is taken at once
 * and every reset of A is owed. A new SUBSCRIBE then takes A's slot under a
 * new handle while those resets are still owed: the owed entries keep A's
 * handle, streams and code, and the subgroup allocator never selects them as
 * free. B's next subgroup is refused retryably meanwhile. Polling actions
 * alone -- no further peer input -- issues every owed reset exactly once, on
 * A's streams; then C is accepted and A's subgroup slots are free again. */
static void test_reuse_while_owed(bool reset)
{
    char what[80];
    snprintf(what, sizeof(what), "%s, A's slot reused while resets are owed",
             reset ? "RESET" : "STOP");
    int before = failures;
    fixture_t f;
    fx_opts_t o = { 0 };
    if (!open_row(&f, what, o)) return;
    tally_t t;
    memset(&t, 0, sizeof(t));
    MOQ_TEST_CHECK_EQ_INT((int)write_whole(&f, f.sg[SG_B_OPEN], 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(actions_queued(f.s), (size_t)1);
    MOQ_TEST_CHECK_EQ_INT((int)deliver(&f, reset), (int)MOQ_OK);
    MOQ_TEST_CHECK(sub_resolve_handle(f.s, f.sub_a) < 0);
    for (int i = 0; i < live_count(&f); i++)
        MOQ_TEST_CHECK(reset_owed_for(&f, i));
    /* Events only: UNSUBSCRIBED is read, no action is polled. */
    moq_event_t e;
    while (moq_session_poll_events(f.s, &e, 1) > 0) {
        record_event(&f, &t, &e);
        moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK_EQ_INT(t.unsubscribed_a, 1);
    MOQ_TEST_CHECK_EQ_INT(t.committed_at_unsubscribed, live_count(&f));

    /* C's SUBSCRIBE takes A's slot, left pending so it needs no action. */
    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_filter = true;
    p.filter_type = 2;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, f.next_rid, &ns,
                                      MOQ_BYTES_LITERAL("c"), &p), (int)MOQ_OK);
    f.next_rid += 2;
    moq_stream_ref_t ref_c = moq_stream_ref_from_u64(0x90008);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(f.s, ref_c, msg,
            moq_buf_writer_offset(&w), false, f.now), (int)MOQ_OK);
    moq_subscription_t sub_c = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(f.s, &e, 1) > 0) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) sub_c = e.u.subscribe_request.sub;
        moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK_EQ_INT(sub_resolve_handle(f.s, sub_c), f.slot_a);
    MOQ_TEST_CHECK(!moq_subscription_eq(sub_c, f.sub_a));
    /* The owed entries still belong to A, not to the slot's new owner. */
    for (size_t k = 0; k < f.s->sg_cap; k++)
        if (f.s->subgroups[k].state == MOQ_SG_RESET_OWED) {
            MOQ_TEST_CHECK(moq_subscription_eq(f.s->subgroups[k].sub, f.sub_a));
            MOQ_TEST_CHECK(sg_index(&f, f.s->subgroups[k].stream_ref) >= 0);
        }
    MOQ_TEST_CHECK(!sub_cancel_pending(f.s, sub_c));

    /* The pool never selects an owed entry as free. With one action slot the
     * queue is refilled from the owed resets at every poll, so an open's
     * refusal cannot isolate the pool; the allocator is asked directly, with
     * every other free slot hidden for the probe. */
    {
        bool owed_slot[64] = { false }, hidden[64] = { false };
        size_t owed_n = 0;
        MOQ_TEST_CHECK(f.s->sg_cap <= 64);
        for (size_t k = 0; k < f.s->sg_cap && k < 64; k++)
            if (f.s->subgroups[k].state == MOQ_SG_RESET_OWED) {
                owed_slot[k] = true;
                owed_n++;
            }
        MOQ_TEST_CHECK_EQ_SIZE(owed_n, (size_t)live_count(&f));
        int fr = sg_find_free(f.s);
        MOQ_TEST_CHECK(fr < 0 || (fr < 64 && !owed_slot[fr]));
        for (size_t k = 0; k < f.s->sg_cap && k < 64; k++)
            if (f.s->subgroups[k].state == MOQ_SG_FREE) {
                hidden[k] = true;
                f.s->subgroups[k].state = MOQ_SG_RESETTING;
            }
        MOQ_TEST_CHECK_EQ_INT(sg_find_free(f.s), -1);
        for (size_t k = 0; k < f.s->sg_cap && k < 64; k++)
            if (hidden[k]) f.s->subgroups[k].state = MOQ_SG_FREE;
    }
    /* B's next subgroup is refused, retryably, while they are owed (the
     * action queue alone would refuse it too). */
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 50;
    moq_subgroup_handle_t hb = MOQ_SUBGROUP_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(f.s, f.sub_b, &sc,
                                                         f.now, &hb),
                          (int)MOQ_ERR_WOULD_BLOCK);

    /* Polling alone issues every owed reset, once each, on A's streams. */
    moq_action_t a;
    while (moq_session_poll_actions(f.s, &a, 1) > 0) {
        record_action(&f, &t, &a);
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(t.writes[SG_B_OPEN], 1);
    t.writes[SG_B_OPEN] = 0;
    for (int i = 0; i < live_count(&f); i++) {
        MOQ_TEST_CHECK_EQ_INT(t.resets[i], 1);
        MOQ_TEST_CHECK_EQ_U64(t.reset_code[i], 0x1);
    }
    MOQ_TEST_CHECK_EQ_INT(t.resets[SG_A_FIN], 0);
    MOQ_TEST_CHECK_EQ_INT(t.resets[SG_B_OPEN], 0);
    MOQ_TEST_CHECK_EQ_INT(t.other_actions, 0);
    for (size_t k = 0; k < f.s->sg_cap; k++)
        MOQ_TEST_CHECK(f.s->subgroups[k].state != MOQ_SG_RESET_OWED);

    /* C is accepted and opens as many subgroups as A held. */
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(f.s, sub_c, &acc, f.now),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(drain_count(f.s), 1);
    for (int i = 0; i <= SG_A_FIN; i++) {
        moq_subgroup_handle_t h = MOQ_SUBGROUP_INVALID;
        moq_stream_ref_t r = moq_stream_ref_from_u64(0);
        MOQ_TEST_CHECK_EQ_INT((int)open_sg(&f, sub_c, (uint64_t)(10 + i), &h, &r),
                              (int)MOQ_OK);
        if (moq_subgroup_is_valid(h)) MOQ_TEST_CHECK_EQ_INT(sg_index(&f, r), -1);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(f.s), (int)MOQ_SESS_ESTABLISHED);
    if (failures != before) fprintf(stderr, "  in %s\n", what);
    moq_session_destroy(f.s);
}

int main(void)
{
    for (int r = 0; r < 2; r++) {
        bool reset = r != 0;
        for (int g = 0; g < 2; g++) {
            bool goaway = g != 0;
            test_cancel(reset, PRESSURE_NONE, goaway);
            test_cancel(reset, PRESSURE_EVENTS, goaway);
            test_cancel(reset, PRESSURE_ACTIONS, goaway);
            if (!reset) test_cancel(reset, PRESSURE_DRAIN, goaway);
            for (int op = 0; op < OP_COUNT; op++)
                test_partial_op(reset, (op_t)op, goaway);
            test_duplicate_without_room(reset, goaway);
            test_crossed(reset, goaway);
            test_bridge(reset, goaway);
            if (!reset) {
                test_late_fin(goaway, false);
                test_late_fin(goaway, true);
                test_stop_keeps_identity(goaway, false);
                test_stop_keeps_identity(goaway, true);
            }
        }
        if (!reset) test_event_pressure_between(reset);
        test_reuse_while_owed(reset);
        test_app_reset(reset);
        test_timeout_reset(reset);
        test_object_timeout_race(reset);
        test_terminal_race(reset, false);
        test_terminal_race(reset, true);
        test_subscriber_role(reset);
    }
    test_setup_refusal(false);
    test_setup_refusal(true);
    test_setup_other_refusal();
    test_ops_uncancelled();
    test_late_request_bytes();
    test_join_against_cancelled();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: request_cancel_subgroups\n");
    return 0;
}
