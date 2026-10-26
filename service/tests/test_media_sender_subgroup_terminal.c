/* The production sender hook consumes real STOP/expiry through its publisher.
 * Virtual time and a real peer make progress without sockets or sleeps. */
#include <moq/media_sender.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include "../../tests/unit/test_alloc_support.h"
#include <stdio.h>
#include <string.h>

moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session, uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);

static unsigned failures, rows;
static char case_name[96];
#define CHECK(x) do { if (!(x)) { ++failures; \
    fprintf(stderr, "%s/%s:%d: %s\n", case_name, __func__, __LINE__, #x); return false; } } while (0)

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *session, *peer;
    moq_media_sender_t *sender;
    moq_media_track_t *track;
    moq_subscription_t peer_sub;
    moq_publication_t peer_pub;
    uint64_t now, alias, group, subgroup;
    moq_stream_ref_t stopped;
    bool push, watching;
    unsigned first, tail, later, end, finished, left, joined, closed, reopened, obsolete;
    size_t realloc_from_null, realloc_resizes;
} fixture_t;

static void *counted_alloc(size_t size, void *ctx)
{
    fixture_t *f = ctx;
    return test_alloc(size, &f->memory);
}

static void counted_free(void *ptr, size_t size, void *ctx)
{
    fixture_t *f = ctx;
    test_free(ptr, size, &f->memory);
}

static void *counted_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    fixture_t *f = ctx;
    /* The shared counter's realloc ignores NULL -> allocation. Track that
     * ownership transition here, without changing another fixture's helper. */
    if (!ptr) {
        void *p = counted_alloc(new_size, ctx);
        if (p) ++f->realloc_from_null;
        return p;
    }
    if (new_size == 0) {
        counted_free(ptr, old_size, ctx);
        return NULL;
    }
    ++f->realloc_resizes;
    return test_realloc(ptr, old_size, new_size, &f->memory);
}

static bool media_owner(const fixture_t *f, const moq_sg_entry_t *sg)
{
    if (f->push && moq_publication_is_valid(sg->pub)) {
        uint32_t i = moq_handle_slot(sg->pub._opaque);
        return i < f->session->pub_cap && f->session->publishes[i].track_alias == f->alias;
    }
    if (!f->push && moq_subscription_is_valid(sg->sub)) {
        uint32_t i = moq_handle_slot(sg->sub._opaque);
        return i < f->session->sub_cap && f->session->subs[i].track_alias == f->alias;
    }
    return false;
}

static void trace(void *ctx, const moq_sim_trace_record_t *r)
{
    fixture_t *f = ctx;
    if (!f->watching || r->kind != MOQ_SIM_TRACE_ACTION ||
        r->from != MOQ_PERSPECTIVE_CLIENT || r->action_kind != MOQ_ACTION_SEND_DATA)
        return;
    if (r->stream_ref._v == f->stopped._v && r->count) ++f->obsolete;
    for (size_t i = 0; i < f->session->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->session->subgroups[i];
        if (sg->state != MOQ_SG_FREE && sg->stream_ref._v == r->stream_ref._v &&
            sg->group_id == f->group && sg->subgroup_id == f->subgroup &&
            media_owner(f, sg) && sg->stream_ref._v != f->stopped._v)
            ++f->reopened;
    }
}

static void left(void *ctx, moq_media_sender_t *s, moq_media_track_t *t, size_t count)
{
    (void)s; (void)t; (void)count;
    ++((fixture_t *)ctx)->left;
}

static void joined(void *ctx, moq_media_sender_t *s, moq_media_track_t *t, size_t count)
{
    (void)s; (void)t; (void)count;
    ++((fixture_t *)ctx)->joined;
}

static void closed(void *ctx, moq_media_sender_t *s, bool fatal, uint64_t code)
{
    (void)s; (void)fatal; (void)code;
    ++((fixture_t *)ctx)->closed;
}

static bool peer_events(fixture_t *f)
{
    moq_event_t e;
    while (moq_session_poll_events(f->peer, &e, 1)) {
        moq_result_t rc = MOQ_OK;
        if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t c;
            moq_accept_namespace_cfg_init(&c);
            rc = moq_session_accept_namespace(f->peer, e.u.namespace_published.ann, &c, f->now);
        } else if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            if (e.u.publish_request.track_name.len == 1 &&
                e.u.publish_request.track_name.data[0] == 'v') {
                f->peer_pub = e.u.publish_request.pub;
                f->alias = e.u.publish_request.track_alias;
            }
            moq_accept_publish_cfg_t c;
            moq_accept_publish_cfg_init_sized(&c, sizeof(c));
            rc = moq_session_accept_publish(f->peer, e.u.publish_request.pub, &c, f->now);
        } else if (e.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            if (moq_subscription_eq(e.u.subscribe_ok.sub, f->peer_sub))
                f->alias = e.u.subscribe_ok.track_alias;
        } else if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &e.u.object_received;
            bool media = f->push ? moq_publication_eq(o->pub, f->peer_pub)
                                 : moq_subscription_eq(o->sub, f->peer_sub);
            if (media) {
                if (o->status == MOQ_OBJECT_END_OF_TRACK) ++f->end;
                else if (o->status == MOQ_OBJECT_NORMAL) {
                    CHECK(o->payload && moq_rcbuf_len(o->payload) == 6);
                    CHECK(memcmp(moq_rcbuf_data(o->payload), "object", 6) == 0);
                    if (o->group_id == f->group && o->object_id == 0) ++f->first;
                    else if (o->group_id == f->group && o->object_id == 1) ++f->tail;
                    else if (o->group_id == f->group + 1 && o->object_id == 0) ++f->later;
                    else CHECK(false);
                }
            }
        } else if (e.kind == MOQ_EVENT_PUBLISH_FINISHED &&
                   moq_publication_eq(e.u.publish_finished.pub, f->peer_pub)) {
            ++f->finished;
        }
        bool terminal = e.kind == MOQ_EVENT_SESSION_CLOSED;
        moq_event_cleanup(&e);
        CHECK(rc == MOQ_OK && !terminal);
    }
    return true;
}

static bool cycle(fixture_t *f)
{
    for (unsigned i = 0; i < 12; i++) {
        moq_media_sender_test_pump(f->sender, f->session, f->now);
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        CHECK(peer_events(f));
    }
    CHECK(!moq_media_sender_is_fatal(f->sender));
    CHECK(moq_session_state(f->session) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(f->peer) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool setup(fixture_t *f, moq_version_t version, bool push, bool timed)
{
    memset(f, 0, sizeof(*f));
    f->push = push;
    f->alloc = (moq_alloc_t){
        .ctx = f, .alloc = counted_alloc, .realloc = counted_realloc, .free = counted_free
    };
    moq_simpair_cfg_t c = MOQ_SIMPAIR_CFG_INIT;
    c.alloc = &f->alloc;
    c.version = version;
    c.initial_now_us = 1000000;
    c.max_actions = 32;
    c.max_events = 16;
    c.client_send_request_capacity = c.server_send_request_capacity = true;
    c.client_initial_request_capacity = c.server_initial_request_capacity = 64;
    c.trace_fn = trace;
    c.trace_ctx = f;
    CHECK(moq_simpair_create(&c, &f->pair) == MOQ_OK);
    CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    f->session = moq_simpair_client(f->pair);
    f->peer = moq_simpair_server(f->pair);
    f->now = moq_simpair_now_us(f->pair);
    moq_event_t e;
    while (moq_session_poll_events(f->session, &e, 1)) moq_event_cleanup(&e);
    while (moq_session_poll_events(f->peer, &e, 1)) moq_event_cleanup(&e);
    moq_media_sender_cfg_t sc;
    moq_media_sender_cfg_init_live_sized(&sc, sizeof(sc));
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    sc.namespace_ = (moq_namespace_t){&ns, 1};
    sc.publish_tracks = push;
    sc.validate_cmaf = false;
    sc.catalog_refresh_interval_us = UINT64_MAX;
    sc.queue_max_objects = sc.pre_ready_max_objects = 8;
    if (version == MOQ_VERSION_DRAFT_22)
        sc.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    moq_media_sender_callbacks_init_sized(&sc.callbacks, sizeof(sc.callbacks));
    sc.callbacks.ctx = f;
    sc.callbacks.on_subscriber_left = left;
    sc.callbacks.on_subscriber_joined = joined;
    sc.callbacks.on_closed = closed;
    moq_media_sender_test_set_alloc(&f->alloc);
    f->sender = moq_media_sender_test_new_cfg(&sc);
    moq_media_sender_test_set_alloc(NULL);
    CHECK(f->sender != NULL);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1000;
    static const uint8_t timeout[] = {0x02, 0x01};
    if (timed) tc.track_properties = (moq_bytes_t){timeout, sizeof(timeout)};
    CHECK(moq_media_sender_add_track(f->sender, &tc, &f->track) == MOQ_OK);
    CHECK(cycle(f));
    CHECK(moq_media_sender_is_ready(f->sender));
    if (!push) {
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init_sized(&sub, sizeof(sub));
        sub.track_namespace = (moq_namespace_t){&ns, 1};
        sub.track_name = MOQ_BYTES_LITERAL("v");
        sub.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        CHECK(moq_session_subscribe(f->peer, &sub, f->now, &f->peer_sub) == MOQ_OK);
        CHECK(cycle(f));
    }
    CHECK(f->alias != 0);
    CHECK(moq_media_sender_track_subscriptions(f->sender, f->track) == (push ? 0u : 1u));
    return true;
}

static bool write_one(fixture_t *f, bool starts, bool ends)
{
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.starts_group = starts;
    o.ends_group = ends;
    o.is_sync = starts;
    o.presentation_time_us = f->now;
    moq_media_metadata_t m;
    memset(&m, 0, sizeof(m));
    m.struct_size = sizeof(m);
    moq_loc_metadata_init_sized(&m.loc, sizeof(m.loc));
    m.loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    m.loc.timestamp = f->now;
    o.metadata = &m;
    CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &o.payload) == MOQ_OK);
    moq_result_t rc = moq_media_sender_write(f->sender, f->track, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(o.payload);
    CHECK(rc == MOQ_OK);
    return true;
}

static bool watch(fixture_t *f)
{
    for (size_t i = 0; i < f->session->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->session->subgroups[i];
        if (sg->state == MOQ_SG_FREE || !media_owner(f, sg)) continue;
        f->stopped = sg->stream_ref;
        f->group = sg->group_id;
        f->subgroup = sg->subgroup_id;
        return true;
    }
    CHECK(false);
    return false;
}

static bool exercise(fixture_t *f, bool timed, bool pressure)
{
    CHECK(write_one(f, true, false));
    moq_media_sender_test_pump(f->sender, f->session, f->now);
    CHECK(watch(f));
    if (!timed) CHECK(cycle(f));
    f->watching = true;
    if (pressure) {
        while (f->session->event_tail - f->session->event_head < f->session->event_cap) {
            moq_event_t *e = &f->session->events[f->session->event_tail++ % f->session->event_cap];
            memset(e, 0, sizeof(*e));
            e->kind = MOQ_EVENT_SUBGROUP_FINISHED;
        }
    }
    moq_result_t rc;
    if (timed) {
        f->now += 2000;
        rc = moq_simpair_advance_to(f->pair, f->now);
        /* A client-side credit refusal precedes the server's tick; no peer
         * production is needed to release it on the next local pump. */
    } else {
        rc = moq_session_on_data_stop(f->session, f->stopped, 0, f->now);
    }
    CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    CHECK(write_one(f, false, true));
    CHECK(write_one(f, true, true));
    CHECK(cycle(f));
    CHECK(f->first == (timed ? 0u : 1u));
    CHECK(f->tail == 0 && f->later == 1);
    CHECK(f->reopened == 0 && f->obsolete == 0);
    CHECK(f->left == 0 && f->closed == 0 && f->joined == (f->push ? 0u : 1u));
    CHECK(moq_media_sender_is_ready(f->sender));
    CHECK(moq_media_sender_track_subscriptions(f->sender, f->track) == (f->push ? 0u : 1u));
    moq_media_sender_track_stats_t stats;
    CHECK(moq_media_sender_get_track_stats(f->sender, f->track, &stats, sizeof(stats)) == MOQ_OK);
    CHECK(stats.objects_written == 3 && stats.objects_sent == 3);
    CHECK(stats.objects_queued == 0 && stats.bytes_queued == 0 && stats.objects_dropped == 0);
    CHECK(moq_media_sender_end_track(f->sender, f->track) == MOQ_OK);
    CHECK(cycle(f));
    CHECK(f->end == 1);
    CHECK(f->finished == (f->push ? 1u : 0u));
    CHECK(f->closed == 0);
    return true;
}

static void cleanup(fixture_t *f)
{
    int64_t before = f->memory.balance;
    moq_media_sender_test_set_alloc(NULL);
    moq_media_sender_test_free(f->sender);
    int64_t after_sender = f->memory.balance;
    moq_simpair_destroy(f->pair);
    if (f->memory.balance != 0) {
        ++failures;
        fprintf(stderr, "%s/cleanup: allocator balance=%lld, expected 0\n",
            case_name, (long long)f->memory.balance);
    }
    printf("%s/cleanup: allocations before=%lld after_sender=%lld after_pair=%lld realloc_from_null=%zu resizes=%zu\n",
        case_name, (long long)before, (long long)after_sender,
        (long long)f->memory.balance, f->realloc_from_null, f->realloc_resizes);
    ++rows;
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++)
        for (unsigned push = 0; push < 2; push++)
            for (unsigned timed = 0; timed < 2; timed++)
                for (unsigned pressure = 0; pressure < 2; pressure++) {
                    snprintf(case_name, sizeof(case_name), "version%u/push%u/timeout%u/pressure%u",
                        (unsigned)versions[v], push, timed, pressure);
                    fixture_t f;
                    if (setup(&f, versions[v], push != 0, timed != 0))
                        (void)exercise(&f, timed != 0, pressure != 0);
                    cleanup(&f);
                }
    printf("media_sender_subgroup_terminal: %u rows, %u failures\n", rows, failures);
    return failures != 0;
}
