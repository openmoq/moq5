/*
 * Publisher facade: a subgroup stream lost mid-group, drafts 16, 18 and 22.
 *
 * A subgroup stream goes away either because the subscriber sent
 * STOP_SENDING or because a MOQT 8 delivery timeout fired; the transport
 * reports both through moq_session_on_data_stop. The expectations come from
 * the drafts:
 *
 *   - draft-18 8 / draft-16 9.2.2.2: on a delivery timeout the publisher
 *     resets the stream and SHOULD NOT open a new stream for further Objects
 *     of that Subgroup.
 *   - draft-18 11.4.3 / draft-16 10.4.3: after STOP_SENDING on a Subgroup
 *     stream the publisher SHOULD NOT open a new stream for further Objects
 *     of that Subgroup; resets and STOP_SENDING have no impact on other
 *     Subgroups or on the subscription.
 *
 * The facade puts each group on subgroup 0, so "that Subgroup" is the rest
 * of the group for that destination. Data actions are observed through the
 * SimPair trace; the subscriber's view through the client session's events.
 */
#include <moq/publisher.h>
#include <moq/sim.h>
#include "test_alloc_support.h"
#include "test_support.h"
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

#define STREAM_COUNT_UNKNOWN ((1ull << 62) - 1)

static const moq_version_t k_versions[] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
};

/* -- wire log (server data actions, from the SimPair trace) ----------- */

#define MAX_REFS 128

typedef struct wire_log {
    size_t           n_send;               /* SEND_DATA since the last clear */
    moq_stream_ref_t send[MAX_REFS];
    size_t           n_reset;              /* RESET_DATA since the last clear */
    moq_stream_ref_t reset[MAX_REFS];
    size_t           n_streams;            /* distinct streams, whole test */
    moq_stream_ref_t streams[MAX_REFS];
} wire_log_t;

static bool ref_eq(moq_stream_ref_t a, moq_stream_ref_t b)
{
    return a._v == b._v;
}

static void on_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    wire_log_t *w = (wire_log_t *)ctx;
    if (r->kind != MOQ_SIM_TRACE_ACTION || r->from != MOQ_PERSPECTIVE_SERVER)
        return;
    if (r->struct_size < offsetof(moq_sim_trace_record_t, stream_ref) +
                         sizeof(r->stream_ref))
        return;
    if (r->action_kind == MOQ_ACTION_SEND_DATA) {
        if (w->n_send < MAX_REFS) w->send[w->n_send++] = r->stream_ref;
        bool known = false;
        for (size_t i = 0; i < w->n_streams; i++)
            if (ref_eq(w->streams[i], r->stream_ref)) known = true;
        if (!known && w->n_streams < MAX_REFS)
            w->streams[w->n_streams++] = r->stream_ref;
    } else if (r->action_kind == MOQ_ACTION_RESET_DATA) {
        if (w->n_reset < MAX_REFS) w->reset[w->n_reset++] = r->stream_ref;
    }
}

static void wire_clear(wire_log_t *w)
{
    w->n_send = 0;
    w->n_reset = 0;
}

static bool wire_sent_on(const wire_log_t *w, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < w->n_send; i++)
        if (ref_eq(w->send[i], ref)) return true;
    return false;
}

static bool wire_reset_on(const wire_log_t *w, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < w->n_reset; i++)
        if (ref_eq(w->reset[i], ref)) return true;
    return false;
}

/* -- client log (subscriber-side events) ------------------------------ */

#define MAX_RECS 64

typedef struct obj_rec {
    uint64_t            group;
    uint64_t            object;
    bool                on_pub;   /* publisher-initiated subscription */
    moq_object_status_t status;
    size_t              len;
    uint8_t             first;
} obj_rec_t;

typedef struct client_log {
    size_t    n_obj;
    obj_rec_t obj[MAX_RECS];
    size_t    n_sg_reset;
    uint64_t  sg_reset_group[MAX_RECS];
    bool      sg_reset_on_pub[MAX_RECS];
    size_t    n_done;
    uint64_t  done_status;
    uint64_t  done_count;
    size_t    n_pub_finished;
} client_log_t;

/* -- fixture ---------------------------------------------------------- */

typedef struct fx {
    moq_version_t      ver;
    test_alloc_state_t as;
    moq_alloc_t        alloc;
    moq_simpair_t     *sp;
    moq_publisher_t   *pub;
    moq_pub_track_t   *track;
    moq_subscription_t sub;
    moq_bytes_t        ns_parts[1];
    wire_log_t         wire;
    client_log_t       cl;
} fx_t;

static uint64_t now(fx_t *f) { return moq_simpair_now_us(f->sp); }

static void collect_client(fx_t *f)
{
    moq_session_t *c = moq_simpair_client(f->sp);
    moq_event_t ev;
    while (moq_session_poll_events(c, &ev, 1) == 1) {
        client_log_t *cl = &f->cl;
        switch (ev.kind) {
        case MOQ_EVENT_OBJECT_RECEIVED:
            if (cl->n_obj < MAX_RECS) {
                const moq_object_received_event_t *o = &ev.u.object_received;
                obj_rec_t *r = &cl->obj[cl->n_obj++];
                r->group = o->group_id;
                r->object = o->object_id;
                r->on_pub = moq_publication_is_valid(o->pub);
                r->status = o->status;
                r->len = o->payload ? moq_rcbuf_len(o->payload) : 0;
                r->first = r->len ? moq_rcbuf_data(o->payload)[0] : 0;
            }
            break;
        case MOQ_EVENT_SUBGROUP_RESET:
            if (cl->n_sg_reset < MAX_RECS) {
                cl->sg_reset_group[cl->n_sg_reset] = ev.u.subgroup_reset.group_id;
                cl->sg_reset_on_pub[cl->n_sg_reset] =
                    moq_publication_is_valid(ev.u.subgroup_reset.pub);
                cl->n_sg_reset++;
            }
            break;
        case MOQ_EVENT_SUBSCRIBE_DONE:
            cl->n_done++;
            cl->done_status = ev.u.subscribe_done.status_code;
            cl->done_count = ev.u.subscribe_done.stream_count;
            break;
        case MOQ_EVENT_PUBLISH_FINISHED:
            cl->n_pub_finished++;
            break;
        case MOQ_EVENT_PUBLISH_REQUEST: {
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            moq_session_accept_publish(c, ev.u.publish_request.pub, &acc,
                                       now(f));
            break;
        }
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            moq_accept_namespace_cfg_t nacc;
            moq_accept_namespace_cfg_init(&nacc);
            moq_session_accept_namespace(c, ev.u.namespace_published.ann,
                                         &nacc, now(f));
            break;
        }
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* Route everything both ways, let the facade handle its events, and record
 * what the subscriber saw. */
static void pump(fx_t *f)
{
    for (int i = 0; i < 4; i++) {
        moq_simpair_run_until_quiescent(f->sp, 64, NULL);
        moq_pub_tick(f->pub, now(f));
        moq_simpair_run_until_quiescent(f->sp, 64, NULL);
        collect_client(f);
    }
}

static void clear_logs(fx_t *f)
{
    wire_clear(&f->wire);
    memset(&f->cl, 0, sizeof(f->cl));
}

static bool fx_setup(fx_t *f, moq_version_t ver, bool subscribe, bool publish)
{
    memset(f, 0, sizeof(*f));
    f->ver = ver;
    f->alloc = test_allocator(&f->as);

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    cfg.trace_fn = on_trace;
    cfg.trace_ctx = &f->wire;
    if (moq_simpair_create(&cfg, &f->sp) != MOQ_OK) return false;
    moq_simpair_start(f->sp);
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(moq_simpair_server(f->sp), &f->alloc, &pcfg,
                       &f->pub) != MOQ_OK)
        return false;

    moq_bytes_t *ns_parts = f->ns_parts;
    ns_parts[0] = MOQ_BYTES_LITERAL("live");
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    if (moq_pub_add_track(f->pub, &tcfg, now(f), &f->track) != MOQ_OK)
        return false;

    if (subscribe) {
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("video");
        if (moq_session_subscribe(moq_simpair_client(f->sp), &scfg, now(f),
                                  &f->sub) != MOQ_OK)
            return false;
        pump(f);
        if (moq_pub_active_subscriptions(f->pub, f->track) != 1) return false;
    }
    if (publish) {
        moq_pub_publish_cfg_t pubcfg;
        moq_pub_publish_cfg_init(&pubcfg);
        if (moq_pub_publish_track(f->pub, f->track, &pubcfg, now(f)) != MOQ_OK)
            return false;
        pump(f);
        if (!moq_pub_track_is_published(f->pub, f->track)) return false;
    }
    clear_logs(f);
    return true;
}

static void fx_teardown(fx_t *f)
{
    if (f->pub) {
        moq_pub_remove_track(f->pub, f->track, now(f));
        moq_pub_destroy(f->pub);
    }
    if (f->sp) {
        moq_event_t ev;
        moq_action_t act;
        while (moq_session_poll_events(moq_simpair_client(f->sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_actions(moq_simpair_client(f->sp), &act, 1) > 0)
            moq_action_cleanup(&act);
        while (moq_session_poll_actions(moq_simpair_server(f->sp), &act, 1) > 0)
            moq_action_cleanup(&act);
        moq_simpair_destroy(f->sp);
    }
    MOQ_TEST_CHECK(f->as.balance == 0);
}

/* Payload {tag, group, object}: lets the subscriber side check the bytes. */
static moq_result_t write_obj(fx_t *f, uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[3] = { tag, (uint8_t)g, (uint8_t)o };
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(&f->alloc, d, sizeof(d), &p) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_pub_write_object(f->pub, f->track, g, o, p, now(f));
    moq_rcbuf_decref(p);
    return rc;
}

static moq_result_t write_chunk(fx_t *f, const uint8_t *d, size_t len)
{
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(&f->alloc, d, len, &p) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_pub_write_data(f->pub, f->track, p, now(f));
    moq_rcbuf_decref(p);
    return rc;
}

static moq_result_t begin_obj(fx_t *f, uint64_t g, uint64_t o, uint64_t len)
{
    moq_pub_begin_object_cfg_t bc;
    moq_pub_begin_object_cfg_init_sized(&bc, sizeof(bc));
    bc.group_id = g;
    bc.object_id = o;
    bc.payload_length = len;
    return moq_pub_begin_object(f->pub, f->track, &bc, now(f));
}

/* Normal objects (g, o) the subscriber received, on the publication or on
 * the SUBSCRIBE subscription. */
static size_t objs_at(const fx_t *f, uint64_t g, uint64_t o, bool on_pub)
{
    size_t n = 0;
    for (size_t i = 0; i < f->cl.n_obj; i++) {
        const obj_rec_t *r = &f->cl.obj[i];
        if (r->status == MOQ_OBJECT_NORMAL && r->group == g &&
            r->object == o && r->on_pub == on_pub)
            n++;
    }
    return n;
}

static size_t objs_in_group(const fx_t *f, uint64_t g)
{
    size_t n = 0;
    for (size_t i = 0; i < f->cl.n_obj; i++)
        if (f->cl.obj[i].status == MOQ_OBJECT_NORMAL && f->cl.obj[i].group == g)
            n++;
    return n;
}

/* The single stream the server wrote in the current phase; false if it used
 * none or several. */
static bool only_stream(const wire_log_t *w, moq_stream_ref_t *out)
{
    if (w->n_send == 0) return false;
    for (size_t i = 1; i < w->n_send; i++)
        if (!ref_eq(w->send[i], w->send[0])) return false;
    *out = w->send[0];
    return true;
}

/* Deliver (0,0) to a single destination; returns its stream. */
static moq_stream_ref_t open_group0(fx_t *f, bool on_pub)
{
    moq_stream_ref_t ref = {0};
    MOQ_TEST_CHECK(write_obj(f, 0, 0, 0xA0) == MOQ_OK);
    pump(f);
    MOQ_TEST_CHECK(only_stream(&f->wire, &ref));
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(f, 0, 0, on_pub), 1);
    clear_logs(f);
    return ref;
}

/* The transport reports the stream gone: peer STOP_SENDING (code from the
 * peer) or the bridge's delivery-timeout reset (MOQ_RESET_DELIVERY_TIMEOUT). */
static void lose_stream(fx_t *f, moq_stream_ref_t ref, uint64_t code)
{
    moq_session_on_data_stop(moq_simpair_server(f->sp), ref, code, now(f));
    pump(f);
}

/* After the loss, the next group opens a new stream and reaches the
 * destination (draft-18 11.4.3: no impact on other Subgroups or the
 * subscription). */
static void check_next_group_delivered(fx_t *f, uint64_t g,
                                       moq_stream_ref_t lost, bool on_pub)
{
    clear_logs(f);
    MOQ_TEST_CHECK(write_obj(f, g, 0, 0xB0) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(f, g, 1, 0xB1) == MOQ_OK);
    pump(f);
    moq_stream_ref_t ref = {0};
    MOQ_TEST_CHECK(only_stream(&f->wire, &ref));
    MOQ_TEST_CHECK(!ref_eq(ref, lost));
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(f, g, 0, on_pub), 1);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(f, g, 1, on_pub), 1);
}

/* -- 1, 2: whole-object writes ---------------------------------------- */

/* draft-18 8 / draft-16 9.2.2.2 (code 0x2) and draft-18 11.4.3 /
 * draft-16 10.4.3 (STOP_SENDING): the rest of group 0 goes nowhere, the
 * writes still succeed, the subscription stays, group 1 opens a new stream. */
static void whole_object_case(moq_version_t ver, uint64_t code)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, false));
    moq_stream_ref_t lost = open_group0(&f, false);

    lose_stream(&f, lost, code);
    if (code != MOQ_RESET_DELIVERY_TIMEOUT) {
        /* draft-18 11.4.3: "The publisher SHOULD respond with a reset." */
        MOQ_TEST_CHECK(wire_reset_on(&f.wire, lost));
    }
    clear_logs(&f);

    MOQ_TEST_CHECK(write_obj(&f, 0, 1, 0xA1) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&f, 0, 2, 0xA2) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);
    MOQ_TEST_CHECK_EQ_SIZE(objs_in_group(&f, 0), 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 1);
    MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_done, 0);

    check_next_group_delivered(&f, 1, lost, false);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 1);
    fx_teardown(&f);
}

static void test_whole_object_delivery_timeout(moq_version_t ver)
{
    whole_object_case(ver, MOQ_RESET_DELIVERY_TIMEOUT);
}

static void test_whole_object_stop_sending(moq_version_t ver)
{
    whole_object_case(ver, 0x0);
}

/* -- 3: streaming writes ---------------------------------------------- */

/* draft-18 8 / 11.4.3: the stream dies mid-object; the rest of that object
 * and a later object of group 0 are not sent anywhere, group 1 is. */
static void test_streaming_stop_mid_object(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, false));

    static const uint8_t a[] = { 1, 2, 3 }, b[] = { 4, 5, 6 };
    MOQ_TEST_CHECK(begin_obj(&f, 0, 0, sizeof(a) + sizeof(b)) == MOQ_OK);
    MOQ_TEST_CHECK(write_chunk(&f, a, sizeof(a)) == MOQ_OK);
    pump(&f);
    moq_stream_ref_t lost = {0};
    MOQ_TEST_CHECK(only_stream(&f.wire, &lost));

    lose_stream(&f, lost, MOQ_RESET_DELIVERY_TIMEOUT);
    clear_logs(&f);

    MOQ_TEST_CHECK(write_chunk(&f, b, sizeof(b)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(f.pub, f.track, now(&f)) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);

    static const uint8_t c[] = { 7, 8 };
    MOQ_TEST_CHECK(begin_obj(&f, 0, 1, sizeof(c)) == MOQ_OK);
    MOQ_TEST_CHECK(write_chunk(&f, c, sizeof(c)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(f.pub, f.track, now(&f)) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);
    MOQ_TEST_CHECK_EQ_SIZE(objs_in_group(&f, 0), 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 1);

    clear_logs(&f);
    static const uint8_t d[] = { 0xB0, 1, 0 };
    MOQ_TEST_CHECK(begin_obj(&f, 1, 0, sizeof(d)) == MOQ_OK);
    MOQ_TEST_CHECK(write_chunk(&f, d, sizeof(d)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(f.pub, f.track, now(&f)) == MOQ_OK);
    pump(&f);
    moq_stream_ref_t ref = {0};
    MOQ_TEST_CHECK(only_stream(&f.wire, &ref));
    MOQ_TEST_CHECK(!ref_eq(ref, lost));
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 1, 0, false), 1);
    bool bytes_ok = false;
    for (size_t i = 0; i < f.cl.n_obj; i++)
        if (f.cl.obj[i].group == 1 && f.cl.obj[i].object == 0)
            bytes_ok = f.cl.obj[i].len == sizeof(d) && f.cl.obj[i].first == d[0];
    MOQ_TEST_CHECK(bytes_ok);
    fx_teardown(&f);
}

/* -- 4: end_group ----------------------------------------------------- */

/* draft-18 11.4.3: closing the lost group sends nothing for it (no new
 * stream, no End of Group object) and leaves the subscription alone. */
static void test_end_group_after_stop(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, false));
    moq_stream_ref_t lost = open_group0(&f, false);
    lose_stream(&f, lost, MOQ_RESET_DELIVERY_TIMEOUT);
    clear_logs(&f);

    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.track, now(&f)) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 1);
    MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_done, 0);

    check_next_group_delivered(&f, 1, lost, false);
    fx_teardown(&f);
}

/* -- 5: end_track ----------------------------------------------------- */

/* The lost stream has no impact on the subscription (draft-18 11.4.3), so
 * the track end still reaches the subscriber: an End of Track object, whose
 * Location is past every Object of the track (draft-18 11.2.1.1: no objects
 * at or after it exist). If a done is sent, its Stream Count is exact or
 * 2^62-1 (draft-18 10.11). */
static void test_end_track_after_stop(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, false));
    moq_stream_ref_t lost = open_group0(&f, false);
    lose_stream(&f, lost, MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK(write_obj(&f, 0, 1, 0xA1) == MOQ_OK);
    pump(&f);
    clear_logs(&f);

    MOQ_TEST_CHECK(moq_pub_end_track(f.pub, f.track, now(&f)) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK(!wire_sent_on(&f.wire, lost));

    size_t eot = 0;
    bool past_last = true;
    for (size_t i = 0; i < f.cl.n_obj; i++) {
        const obj_rec_t *r = &f.cl.obj[i];
        if (r->status != MOQ_OBJECT_END_OF_TRACK) continue;
        eot++;
        MOQ_TEST_CHECK(!r->on_pub);
        if (r->group == 0 && r->object <= 1)
            past_last = false;
    }
    MOQ_TEST_CHECK_EQ_SIZE(eot, 1);
    MOQ_TEST_CHECK(past_last);
    if (f.cl.n_done > 0) {
        MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_done, 1);
        MOQ_TEST_CHECK(f.cl.done_count == f.wire.n_streams ||
                       f.cl.done_count == STREAM_COUNT_UNKNOWN);
    }
    fx_teardown(&f);
}

/* -- 6: reset_group --------------------------------------------------- */

/* Abandoning the lost group opens nothing for it and leaves the
 * subscription alone (draft-18 11.4.3). */
static void test_reset_group_after_stop(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, false));
    moq_stream_ref_t lost = open_group0(&f, false);
    lose_stream(&f, lost, MOQ_RESET_DELIVERY_TIMEOUT);
    clear_logs(&f);

    MOQ_TEST_CHECK(moq_pub_reset_group(f.pub, f.track, 0x10, now(&f)) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 1);
    MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_done, 0);

    check_next_group_delivered(&f, 1, lost, false);
    fx_teardown(&f);
}

/* -- 7: two destinations ---------------------------------------------- */

/* draft-18 11.4.3: "Resets and STOP_SENDING on SUBSCRIBE data streams have
 * no impact on other Subgroups in the Group or the subscription." Losing one
 * destination's stream of group 0 leaves the other destination's stream of
 * group 0 running. The two destinations are the SUBSCRIBE subscription and
 * the PUBLISH publication of the same track (one facade serves one
 * session). */
static void test_other_destination_unaffected(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, true));

    MOQ_TEST_CHECK(write_obj(&f, 0, 0, 0xA0) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 0, 0, false), 1);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 0, 0, true), 1);
    MOQ_TEST_CHECK(f.wire.n_send >= 2);
    moq_stream_ref_t lost = f.wire.n_send ? f.wire.send[0] : (moq_stream_ref_t){0};
    moq_stream_ref_t kept = lost;
    for (size_t i = 0; i < f.wire.n_send; i++)
        if (!ref_eq(f.wire.send[i], lost)) kept = f.wire.send[i];
    MOQ_TEST_CHECK(!ref_eq(kept, lost));
    clear_logs(&f);

    moq_session_on_data_stop(moq_simpair_server(f.sp), lost, 0x0, now(&f));
    pump(&f);
    /* The subscriber's SUBGROUP_RESET tells which destination lost it. */
    MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_sg_reset, 1);
    bool lost_on_pub = f.cl.n_sg_reset ? f.cl.sg_reset_on_pub[0] : false;
    MOQ_TEST_CHECK(f.cl.n_sg_reset == 0 || f.cl.sg_reset_group[0] == 0);
    clear_logs(&f);

    MOQ_TEST_CHECK(write_obj(&f, 0, 1, 0xA1) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&f, 0, 2, 0xA2) == MOQ_OK);
    pump(&f);
    moq_stream_ref_t ref = {0};
    MOQ_TEST_CHECK(only_stream(&f.wire, &ref));
    MOQ_TEST_CHECK(ref_eq(ref, kept));
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 0, 1, !lost_on_pub), 1);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 0, 2, !lost_on_pub), 1);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 0, 1, lost_on_pub), 0);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 0, 2, lost_on_pub), 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 1);
    MOQ_TEST_CHECK(moq_pub_track_is_published(f.pub, f.track));

    clear_logs(&f);
    MOQ_TEST_CHECK(write_obj(&f, 1, 0, 0xB0) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 1, 0, false), 1);
    MOQ_TEST_CHECK_EQ_SIZE(objs_at(&f, 1, 0, true), 1);
    MOQ_TEST_CHECK(!wire_sent_on(&f.wire, lost));
    fx_teardown(&f);
}

/* -- 8: publisher-initiated publication ------------------------------- */

/* The same rules hold for a PUBLISH-initiated subscription. */
static void test_publication_delivery_timeout(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, false, true));
    moq_stream_ref_t lost = open_group0(&f, true);

    lose_stream(&f, lost, MOQ_RESET_DELIVERY_TIMEOUT);
    clear_logs(&f);

    MOQ_TEST_CHECK(write_obj(&f, 0, 1, 0xA1) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&f, 0, 2, 0xA2) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);
    MOQ_TEST_CHECK_EQ_SIZE(objs_in_group(&f, 0), 0);
    MOQ_TEST_CHECK(moq_pub_track_is_published(f.pub, f.track));
    MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_pub_finished, 0);

    check_next_group_delivered(&f, 1, lost, true);
    MOQ_TEST_CHECK(moq_pub_track_is_published(f.pub, f.track));
    fx_teardown(&f);
}

/* -- 9: control: the subscription really ends ------------------------- */

/* After a lost stream the subscriber unsubscribes: the subscription is gone
 * and nothing more is sent to it. */
static void test_unsubscribe_after_stop_ends(moq_version_t ver)
{
    fx_t f;
    MOQ_TEST_CHECK(fx_setup(&f, ver, true, false));
    moq_stream_ref_t lost = open_group0(&f, false);
    lose_stream(&f, lost, MOQ_RESET_DELIVERY_TIMEOUT);

    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(f.sp), f.sub,
                                           now(&f)) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 0);
    MOQ_TEST_CHECK(!moq_pub_has_subscriber(f.pub, f.track));
    clear_logs(&f);

    MOQ_TEST_CHECK(write_obj(&f, 0, 1, 0xA1) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&f, 1, 0, 0xB0) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK_EQ_SIZE(f.wire.n_send, 0);
    MOQ_TEST_CHECK_EQ_SIZE(f.cl.n_obj, 0);
    MOQ_TEST_CHECK_EQ_SIZE(moq_pub_active_subscriptions(f.pub, f.track), 0);
    fx_teardown(&f);
}

/* -- driver ----------------------------------------------------------- */

static void run(const char *name, void (*fn)(moq_version_t))
{
    for (size_t v = 0; v < sizeof(k_versions) / sizeof(k_versions[0]); v++) {
        int before = failures;
        fn(k_versions[v]);
        printf("%s: %s [draft-%d]\n", failures == before ? "PASS" : "FAIL",
               name, (int)k_versions[v]);
    }
}

int main(void)
{
    run("whole_object_delivery_timeout", test_whole_object_delivery_timeout);
    run("whole_object_stop_sending", test_whole_object_stop_sending);
    run("streaming_stop_mid_object", test_streaming_stop_mid_object);
    run("end_group_after_stop", test_end_group_after_stop);
    run("end_track_after_stop", test_end_track_after_stop);
    run("reset_group_after_stop", test_reset_group_after_stop);
    run("other_destination_unaffected", test_other_destination_unaffected);
    run("publication_delivery_timeout", test_publication_delivery_timeout);
    run("unsubscribe_after_stop_ends", test_unsubscribe_after_stop_ends);

    if (failures == 0) {
        printf("PASS: all publisher stream loss tests\n");
        return 0;
    }
    printf("FAIL: %d publisher stream loss test failures\n", failures);
    return 1;
}
