/* Literal D18/D22 FETCH objects: D18 11.4.4.1, D22 11.4.1.1 and 3.2.2.
 * Descending groups subtract delta + 1; objects still increase within a group.
 * Object bytes below never pass through the production object encoder. */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <string.h>

#define VI64_MAX_BYTES 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff

typedef struct fixture {
    moq_simpair_t *pair;
    moq_session_t *client, *server;
    moq_subscription_t client_sub, server_sub;
    moq_fill_t client_fill, server_fill;
    moq_stream_ref_t stream;
    uint64_t initiating_id;
    size_t terminals;
    bool ready;
} fixture_t;

typedef struct expected_event {
    moq_event_kind_t kind;
    uint64_t group, object, subgroup;
    uint32_t range_kind;
    uint8_t priority, payload;
} expected_event_t;

static void drain(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) {
        if (e.kind == kind) { *out = e; return true; }
        moq_event_cleanup(&e);
    }
    return false;
}

static moq_result_t pump(fixture_t *p)
{
    return moq_simpair_run_until_quiescent(p->pair, 32, NULL);
}

static bool make_pair_limits(fixture_t *p, moq_version_t version,
                              uint32_t max_actions, uint32_t max_events)
{
    memset(p, 0, sizeof(*p));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.max_actions = max_actions;
    cfg.max_events = max_events;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    if (moq_simpair_create(&cfg, &p->pair) != MOQ_OK) return false;
    if (moq_simpair_start(p->pair) != MOQ_OK) return false;
    if (pump(p) != MOQ_OK) return false;
    p->client = moq_simpair_client(p->pair);
    p->server = moq_simpair_server(p->pair);
    if (moq_session_version(p->client) != version ||
        moq_session_version(p->server) != version ||
        moq_session_state(p->client) != MOQ_SESS_ESTABLISHED ||
        moq_session_state(p->server) != MOQ_SESS_ESTABLISHED) return false;
    drain(p->client);
    drain(p->server);
    return true;
}

static bool make_pair(fixture_t *p, moq_version_t version, uint32_t max_actions)
{
    return make_pair_limits(p, version, max_actions, 0);
}

static moq_rx_stream_t *find_rx(fixture_t *p)
{
    for (size_t i = 0; i < p->client->rx_cap; ++i)
        if (p->client->rx_streams[i].active &&
            p->client->rx_streams[i].stream_ref._v == p->stream._v)
            return &p->client->rx_streams[i];
    return NULL;
}

/* Copy production control bytes before another advancing call can recycle
 * their arena. The data bytes here are only the real response header. */
static bool capture_response(moq_session_t *s, uint8_t *control, size_t control_cap,
                              size_t *control_len, uint8_t *header, size_t header_cap,
                              size_t *header_len)
{
    bool ok = true;
    *control_len = *header_len = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1)) {
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            size_t n = a.u.send_bidi_stream.len;
            if (n > control_cap - *control_len) ok = false;
            else {
                memcpy(control + *control_len, a.u.send_bidi_stream.data, n);
                *control_len += n;
            }
        } else if (a.kind == MOQ_ACTION_SEND_DATA && !a.u.send_data.payload &&
                   !a.u.send_data.fin) {
            size_t n = a.u.send_data.header_len;
            if (n > header_cap - *header_len) ok = false;
            else {
                memcpy(header + *header_len, a.u.send_data.header, n);
                *header_len += n;
            }
        } else ok = false;
        moq_action_cleanup(&a);
    }
    return ok;
}

static int start_fill(fixture_t *p, uint32_t order, bool inherited,
                      uint32_t max_actions)
{
    int failures = 0;
    bool ok = make_pair(p, MOQ_VERSION_DRAFT_22, max_actions);
    MOQ_TEST_CHECK(ok);
    if (!ok) return failures;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    fill.has_group_order = !inherited;
    fill.group_order = order;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.group_order = inherited ? order : MOQ_GROUP_ORDER_ASCENDING;
    cfg.fill = &fill;
    moq_result_t rc = moq_session_subscribe(p->client, &cfg, 1, &p->client_sub);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    rc = pump(p);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    moq_event_t e;
    ok = take(p->server, MOQ_EVENT_SUBSCRIBE_REQUEST, &e);
    MOQ_TEST_CHECK(ok);
    if (!ok) return failures;
    p->server_sub = e.u.subscribe_request.sub;
    moq_event_cleanup(&e);
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.has_largest = true;
    accept.largest_group = UINT64_MAX;
    accept.largest_object = 10;
    rc = moq_session_accept_subscribe(p->server, p->server_sub, &accept, 2);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    rc = pump(p);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    ok = take(p->client, MOQ_EVENT_FILL_CREATED, &e);
    MOQ_TEST_CHECK(ok);
    if (!ok) return failures;
    p->client_fill = e.u.fill_created.fill;
    p->initiating_id = e.u.fill_created.initiating_request_id;
    MOQ_TEST_CHECK(moq_subscription_eq(e.u.fill_created.sub, p->client_sub));
    MOQ_TEST_CHECK(!moq_publication_is_valid(e.u.fill_created.pub));
    moq_event_cleanup(&e);
    ok = take(p->server, MOQ_EVENT_FILL_REQUEST, &e);
    MOQ_TEST_CHECK(ok);
    if (!ok) return failures;
    p->server_fill = e.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_U64(e.u.fill_request.initiating_request_id, p->initiating_id);
    MOQ_TEST_CHECK_EQ_U64(e.u.fill_request.group_order, order);
    MOQ_TEST_CHECK(moq_subscription_eq(e.u.fill_request.sub, p->server_sub));
    moq_event_cleanup(&e);
    rc = moq_session_open_fill(p->server, p->server_fill, 3);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    rc = pump(p);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    moq_fill_ref_t ref;
    rc = moq_fill_find(&p->client->fills, p->initiating_id, &ref);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return failures;
    const moq_fill_entry_t *entry = &p->client->fill_storage[ref.slot];
    MOQ_TEST_CHECK(entry->delivery_known && entry->stream_bound);
    MOQ_TEST_CHECK_EQ_U64(entry->delivery.group_order, order);
    p->stream = entry->stream;
    drain(p->client);
    drain(p->server);
    p->ready = failures == 0;
    return failures;
}

static int expect_fill_events(fixture_t *p, const expected_event_t *rows,
                              size_t count, bool closed)
{
    int failures = 0;
    size_t seen = 0, closes = 0;
    moq_event_t e;
    while (moq_session_poll_events(p->client, &e, 1)) {
        if (e.kind == MOQ_EVENT_FILL_TERMINAL) {
            ++p->terminals;
            MOQ_TEST_CHECK(moq_fill_eq(e.u.fill_terminal.fill, p->client_fill));
        }
        if (e.kind == MOQ_EVENT_SESSION_CLOSED) {
            ++closes;
            MOQ_TEST_CHECK(closed);
            MOQ_TEST_CHECK_EQ_U64(e.u.closed.code, 3);
            const char reason[] = "malformed fetch object";
            MOQ_TEST_CHECK(e.u.closed.reason.len == sizeof(reason) - 1 &&
                memcmp(e.u.closed.reason.data, reason, sizeof(reason) - 1) == 0);
        }
        if (!closed && (e.kind == MOQ_EVENT_FILL_OBJECT || e.kind == MOQ_EVENT_FILL_GAP)) {
            MOQ_TEST_CHECK(seen < count);
            if (seen < count) {
                const expected_event_t *want = &rows[seen];
                MOQ_TEST_CHECK_EQ_INT(e.kind, want->kind);
                if (e.kind == MOQ_EVENT_FILL_OBJECT) {
                    const moq_fill_object_event_t *o = &e.u.fill_object;
                    MOQ_TEST_CHECK_EQ_U64(o->group_id, want->group);
                    MOQ_TEST_CHECK_EQ_U64(o->object_id, want->object);
                    MOQ_TEST_CHECK_EQ_U64(o->subgroup_id, want->subgroup);
                    MOQ_TEST_CHECK_EQ_U64(o->publisher_priority, want->priority);
                    MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(o->payload), want->payload ? 1 : 0);
                    if (want->payload && moq_rcbuf_len(o->payload) == 1)
                        MOQ_TEST_CHECK(moq_rcbuf_data(o->payload)[0] == want->payload);
                    MOQ_TEST_CHECK(moq_fill_eq(o->fill, p->client_fill));
                    MOQ_TEST_CHECK(moq_subscription_eq(o->sub, p->client_sub));
                    MOQ_TEST_CHECK(!moq_publication_is_valid(o->pub));
                    MOQ_TEST_CHECK_EQ_U64(o->initiating_request_id, p->initiating_id);
                } else {
                    const moq_fill_gap_event_t *g = &e.u.fill_gap;
                    MOQ_TEST_CHECK_EQ_U64(g->group_id, want->group);
                    MOQ_TEST_CHECK_EQ_U64(g->object_id, want->object);
                    MOQ_TEST_CHECK_EQ_U64(g->kind, want->range_kind);
                    MOQ_TEST_CHECK(moq_fill_eq(g->fill, p->client_fill));
                    MOQ_TEST_CHECK(moq_subscription_eq(g->sub, p->client_sub));
                    MOQ_TEST_CHECK_EQ_U64(g->initiating_request_id, p->initiating_id);
                }
            }
            ++seen;
        }
        moq_event_cleanup(&e);
    }
    if (!closed) MOQ_TEST_CHECK_EQ_SIZE(seen, count);
    MOQ_TEST_CHECK_EQ_SIZE(closes, closed ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p->client),
                         closed ? MOQ_SESS_CLOSED : MOQ_SESS_ESTABLISHED);
    return failures;
}

static int fill_bytes(const uint8_t *wire, size_t len,
                       const expected_event_t *rows, size_t count, bool closed,
                       uint32_t order, bool inherited)
{
    fixture_t p;
    int failures = start_fill(&p, order, inherited, 0);
    if (p.ready) {
        /* The production FETCH_HEADER is already bound; only object bytes follow. */
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                               wire, len, false, 4), MOQ_OK);
        failures += expect_fill_events(&p, rows, count, closed);
    }
    moq_simpair_destroy(p.pair);
    return failures;
}

static int fetch_control(moq_version_t version, uint32_t order)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, version, 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init_sized(&cfg, sizeof(cfg));
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.end_group = 20;
    cfg.end_object = 10;
    cfg.group_order = order;
    moq_fetch_t fetch = {0};
    moq_result_t rc = moq_session_fetch(p.client, &cfg, 1, &fetch);
    if (rc != MOQ_OK && order == MOQ_GROUP_ORDER_DESCENDING)
        fprintf(stderr, "UNSUPPORTED: local descending FETCH D%u returned %d\n",
                (unsigned)version, (int)rc);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) goto done;
    rc = pump(&p);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) goto done;
    moq_event_t e;
    ok = take(p.server, MOQ_EVENT_FETCH_REQUEST, &e);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    MOQ_TEST_CHECK_EQ_U64(e.u.fetch_request.group_order, order);
    moq_fetch_t server_fetch = e.u.fetch_request.fetch;
    moq_event_cleanup(&e);
    moq_accept_fetch_cfg_t accept;
    moq_accept_fetch_cfg_init(&accept);
    accept.end_group = 20;
    accept.end_object = 10;
    rc = moq_session_accept_fetch(p.server, server_fetch, &accept, 2);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) goto done;
    rc = pump(&p);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) goto done;
    drain(p.client);
    int slot = fetch_resolve_handle(p.client, fetch);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) goto done;
    MOQ_TEST_CHECK(p.client->fetches[slot].data_stream_started);
    const uint8_t ascending[] = {0x1c, 4, 6, 128, 1, 0x41, 0x08, 2, 1, 0x42};
    const uint8_t descending[] = {0x1c, 10, 6, 128, 1, 0x41, 0x08, 2, 1, 0x42};
    const uint8_t d16[] = {0x1c, 10, 6, 128, 1, 0x41, 0x0c, 7, 7, 1, 0x42};
    const uint8_t *wire = version == MOQ_VERSION_DRAFT_16 ? d16 :
        order == MOQ_GROUP_ORDER_DESCENDING ? descending : ascending;
    size_t len = version == MOQ_VERSION_DRAFT_16 ? sizeof(d16) : sizeof(ascending);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client,
        p.client->fetches[slot].data_stream_ref, wire, len, false, 3), MOQ_OK);
    size_t objects = 0;
    while (moq_session_poll_events(p.client, &e, 1)) {
        MOQ_TEST_CHECK(e.kind != MOQ_EVENT_SESSION_CLOSED);
        if (e.kind == MOQ_EVENT_FETCH_OBJECT) {
            MOQ_TEST_CHECK(objects < 2);
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_object.group_id,
                objects == 0 ? (order == MOQ_GROUP_ORDER_DESCENDING ? 10 : 4) : 7);
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_object.object_id, objects == 0 ? 6 : 7);
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_object.fetch._opaque, fetch._opaque);
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(e.u.fetch_object.payload), 1);
            if (moq_rcbuf_len(e.u.fetch_object.payload) == 1)
                MOQ_TEST_CHECK(moq_rcbuf_data(e.u.fetch_object.payload)[0] ==
                               (objects == 0 ? 0x41 : 0x42));
            ++objects;
        }
        moq_event_cleanup(&e);
    }
    MOQ_TEST_CHECK_EQ_SIZE(objects, 2);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p.client), MOQ_SESS_ESTABLISHED);
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int fill_writer(void)
{
    fixture_t p;
    int failures = start_fill(&p, MOQ_GROUP_ORDER_DESCENDING, false, 0);
    if (p.ready) {
        moq_fetch_object_cfg_t cfg;
        moq_fetch_object_cfg_init(&cfg);
        cfg.group_id = 10;
        cfg.object_id = 6;
        cfg.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server,
                               p.server_fill, &cfg, 4), MOQ_OK);
        cfg.group_id = 7;
        cfg.object_id = 7;
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server,
                               p.server_fill, &cfg, 5), MOQ_OK);
    }
    moq_simpair_destroy(p.pair);
    return failures;
}

static int inbound_fetch_writer(moq_version_t version)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, version, 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    MOQ_TEST_CHECK_EQ_INT(moq_session_set_defer_fetch_ranges(p.server, true), MOQ_OK);
    /* FETCH, 17-byte body, request0, delta/track, GROUP_ORDER=2. This
     * publisher-side test makes no claim that the client owns this request. */
    const uint8_t d22_request[] = {0x16, 0, 17, 0, 1, 5, 'd', 'e', 'l', 't', 'a',
                               5, 't', 'r', 'a', 'c', 'k', 1, 0x22, 2};
    const uint8_t d18_request[] = {0x16, 0, 22, 0, 1, 1, 5, 'd', 'e', 'l', 't', 'a',
        5, 't', 'r', 'a', 'c', 'k', 0, 0, 10, 8, 1, 0x22, 2};
    const uint8_t *request = version == MOQ_VERSION_DRAFT_18 ? d18_request : d22_request;
    size_t request_len = version == MOQ_VERSION_DRAFT_18 ? sizeof(d18_request) : sizeof(d22_request);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.server,
        moq_stream_ref_from_u64(700), request, request_len, false, 1), MOQ_OK);
    moq_event_t e;
    ok = take(p.server, MOQ_EVENT_FETCH_REQUEST, &e);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_fetch_t fetch = e.u.fetch_request.fetch;
    MOQ_TEST_CHECK_EQ_U64(e.u.fetch_request.group_order, MOQ_GROUP_ORDER_DESCENDING);
    moq_event_cleanup(&e);
    int slot = fetch_resolve_handle(p.server, fetch);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) goto done;
    MOQ_TEST_CHECK_EQ_INT(p.server->fetches[slot].role, MOQ_FETCH_ROLE_PUBLISHER);
    moq_accept_fetch_cfg_t accept;
    moq_accept_fetch_cfg_init(&accept);
    accept.end_group = 10;
    accept.end_object = 8;
    MOQ_TEST_CHECK_EQ_INT(moq_session_accept_fetch(p.server, fetch, &accept, 2), MOQ_OK);
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    cfg.group_id = 10;
    cfg.object_id = 6;
    cfg.publisher_priority = 128;
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fetch_object(p.server, fetch, &cfg, 3), MOQ_OK);
    cfg.group_id = 7;
    cfg.object_id = 7;
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fetch_object(p.server, fetch, &cfg, 4), MOQ_OK);
    /* Independently specified wire: first absolute location, then descending
     * delta2 and absolute Object ID7. Do not decode the writer to derive it. */
    const uint8_t want[] = {0x05, 0, 0x1c, 10, 6, 128, 0, 0x1d, 2, 7, 128, 0};
    uint8_t actual[64];
    size_t used = 0;
    moq_action_t a;
    while (moq_session_poll_actions(p.server, &a, 1)) {
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            size_t n = a.u.send_data.header_len;
            MOQ_TEST_CHECK(n <= sizeof(actual) - used);
            if (n <= sizeof(actual) - used) {
                memcpy(actual + used, a.u.send_data.header, n);
                used += n;
            }
            MOQ_TEST_CHECK(!a.u.send_data.payload);
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_SIZE(used, sizeof(want));
    if (used == sizeof(want)) MOQ_TEST_CHECK(memcmp(actual, want, used) == 0);
    if (version == MOQ_VERSION_DRAFT_18) {
        /* This admitted peer order reaches the actual selected D18 codec.
         * Local D18 descending selection remains separately refused. */
        const uint8_t literal[] = {0x1c, 10, 6, 128, 0, 0x08, 2, 0};
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, literal, sizeof(literal));
        moq_fetch_prior_object_t prior = {0};
        moq_decoded_fetch_object_t object;
        MOQ_TEST_CHECK_EQ_INT(p.server->profile->decode_fetch_object(p.server, &r,
            MOQ_GROUP_ORDER_DESCENDING, &prior, &object), MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(object.group_id, 10);
        prior.has_prev = prior.has_actual = true;
        prior.group_id = 10;
        prior.object_id = 6;
        prior.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT(p.server->profile->decode_fetch_object(p.server, &r,
            MOQ_GROUP_ORDER_DESCENDING, &prior, &object), MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(object.group_id, 7);
        MOQ_TEST_CHECK_EQ_U64(object.object_id, 7);
        MOQ_TEST_CHECK_EQ_SIZE(r.pos, sizeof(literal));
    }
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int fill_writer_object_wrap(void)
{
    fixture_t p;
    int failures = start_fill(&p, MOQ_GROUP_ORDER_DESCENDING, false, 0);
    if (p.ready) {
        moq_fetch_object_cfg_t cfg;
        moq_fetch_object_cfg_init(&cfg);
        cfg.group_id = 10;
        cfg.object_id = UINT64_MAX;
        cfg.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server,
                               p.server_fill, &cfg, 4), MOQ_OK);
        moq_fill_ref_t ref;
        MOQ_TEST_CHECK_EQ_INT(moq_fill_find(&p.server->fills, p.initiating_id, &ref), MOQ_OK);
        moq_fetch_prior_object_t before = p.server->fill_side[ref.slot].prior;
        size_t tail = p.server->action_tail;
        const uint8_t payload_byte = 0x52;
        MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), &payload_byte, 1,
                                               &cfg.payload), MOQ_OK);
        cfg.object_id = 0;
        /* Objects ascend within each group even when groups descend. */
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server,
                               p.server_fill, &cfg, 5), MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(p.server->action_tail, tail);
        MOQ_TEST_CHECK(memcmp(&before, &p.server->fill_side[ref.slot].prior,
                             sizeof(before)) == 0);
        MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_refcount(cfg.payload), 1);
        moq_rcbuf_decref(cfg.payload);
    }
    moq_simpair_destroy(p.pair);
    return failures;
}

static int pending_cancel(void)
{
    fixture_t p;
    int failures = start_fill(&p, MOQ_GROUP_ORDER_DESCENDING, false, 2);
    if (!p.ready) goto done;
    const uint8_t first[] = {0x1c, 10, 6, 128, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           first, sizeof(first), false, 4), MOQ_OK);
    drain(p.client);
    moq_rx_stream_t *rx = NULL;
    for (size_t i = 0; i < p.client->rx_cap; ++i)
        if (p.client->rx_streams[i].active &&
            p.client->rx_streams[i].stream_ref._v == p.stream._v)
            rx = &p.client->rx_streams[i];
    MOQ_TEST_CHECK(rx && rx->fill_bound);
    if (!rx) goto done;
    /* White-box owner intent awaiting STOP capacity, not public cancel (which
     * reserves STOP and switches the RX into opaque discard immediately). */
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("pending");
    moq_subscription_t unused;
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(p.client, &cfg, 5, &unused), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(p.client, &cfg, 5, &unused), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(p.client->action_tail - p.client->action_head, 2);
    MOQ_TEST_CHECK_EQ_INT(moq_fill_cancel(&p.client->fills, rx->fill_ref), MOQ_OK);
    const uint8_t next[] = {0x08, 2, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           next, sizeof(next), false, 6), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.group_id, 7);
    MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.object_id, 7);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p.client), MOQ_SESS_ESTABLISHED);
    moq_event_t e;
    while (moq_session_poll_events(p.client, &e, 1)) {
        MOQ_TEST_CHECK(e.kind != MOQ_EVENT_FILL_OBJECT && e.kind != MOQ_EVENT_FILL_GAP);
        moq_event_cleanup(&e);
    }
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int public_cancel_discard(void)
{
    fixture_t p;
    int failures = start_fill(&p, MOQ_GROUP_ORDER_DESCENDING, false, 0);
    if (p.ready) {
        MOQ_TEST_CHECK_EQ_INT(moq_session_cancel_fill(p.client, p.client_fill, 4), MOQ_OK);
        drain(p.client);
        const uint8_t invalid[] = {0x18, 10, 128, 0};
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                               invalid, sizeof(invalid), false, 5), MOQ_OK);
        failures += expect_fill_events(&p, NULL, 0, false);
    }
    moq_simpair_destroy(p.pair);
    return failures;
}

static int pre_ack_fill(bool explicit_order, bool pressure, bool fin)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair_limits(&p, MOQ_VERSION_DRAFT_22, 0, pressure ? 2 : 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    fill.has_group_order = explicit_order;
    fill.group_order = MOQ_GROUP_ORDER_DESCENDING;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(p.client, &cfg, 1, &p.client_sub), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    moq_event_t ev;
    ok = take(p.client, MOQ_EVENT_FILL_CREATED, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.client_fill = ev.u.fill_created.fill;
    p.initiating_id = ev.u.fill_created.initiating_request_id;
    moq_event_cleanup(&ev);
    ok = take(p.server, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.server_sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    int sub_slot = sub_resolve_handle(p.client, p.client_sub);
    MOQ_TEST_CHECK(sub_slot >= 0);
    if (sub_slot < 0) goto done;
    moq_stream_ref_t request_ref = p.client->subs[sub_slot].rs.ref;
    const uint8_t track_order[] = {0x22, 2};
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.has_largest = true;
    accept.largest_group = 10;
    accept.largest_object = 10;
    accept.track_properties = (moq_bytes_t){track_order, sizeof(track_order)};
    MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(p.server, p.server_sub, &accept, 2), MOQ_OK);
    uint8_t ack[128], header[32], ignored[128];
    size_t ack_len, header_len, ignored_len;
    MOQ_TEST_CHECK(capture_response(p.server, ack, sizeof(ack), &ack_len,
                                    header, sizeof(header), &header_len));
    MOQ_TEST_CHECK(ack_len > 0 && header_len == 0);
    ok = take(p.server, MOQ_EVENT_FILL_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.server_fill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.group_order, MOQ_GROUP_ORDER_DESCENDING);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(moq_session_open_fill(p.server, p.server_fill, 3), MOQ_OK);
    MOQ_TEST_CHECK(capture_response(p.server, ignored, sizeof(ignored), &ignored_len,
                                    header, sizeof(header), &header_len));
    MOQ_TEST_CHECK(ignored_len == 0 && header_len > 0);
    /* Deliberately invert the two independent streams: the production response
     * header and literal objects reach the subscriber before its real ACK. */
    p.stream = moq_stream_ref_from_u64(900);
    MOQ_TEST_CHECK(p.stream._v != request_ref._v);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           header, header_len, false, 3), MOQ_OK);
    const uint8_t objects[] = {0x1c, 10, 6, 128, 1, 0x41, 0x08, 2, 1, 0x42};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           objects, sizeof(objects), fin, 4), MOQ_OK);
    moq_rx_stream_t *rx = find_rx(&p);
    MOQ_TEST_CHECK(rx && rx->fill_bound);
    if (!rx) goto done;
    moq_fill_ref_t child = rx->fill_ref;
    MOQ_TEST_CHECK(!p.client->fill_storage[child.slot].delivery_known);
    MOQ_TEST_CHECK_EQ_INT(p.client->fill_storage[child.slot].control, MOQ_FILL_CONTROL_PENDING);
    const expected_event_t want[] = {
        {MOQ_EVENT_FILL_OBJECT, 10, 6, 0, 0, 128, 0x41},
        {MOQ_EVENT_FILL_OBJECT, 7, 7, 0, 0, 128, 0x42},
    };
    if (explicit_order) {
        MOQ_TEST_CHECK(rx->fetch_order_known);
        MOQ_TEST_CHECK_EQ_SIZE(rx->input_len, 0);
        failures += expect_fill_events(&p, want, 2, false);
    } else {
        MOQ_TEST_CHECK(!rx->fetch_order_known && !rx->fetch_prior.has_prev);
        MOQ_TEST_CHECK_EQ_SIZE(rx->input_len, sizeof(objects));
        MOQ_TEST_CHECK_EQ_INT(rx->pending_fin, fin);
        MOQ_TEST_CHECK_EQ_SIZE(p.client->event_tail - p.client->event_head, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.client, request_ref,
                           ack, ack_len, false, 5), MOQ_OK);
    MOQ_TEST_CHECK(rx->fetch_order_known);
    MOQ_TEST_CHECK_EQ_INT(rx->effective_fetch_group_order, MOQ_GROUP_ORDER_DESCENDING);
    MOQ_TEST_CHECK(p.client->fill_storage[child.slot].delivery_known);
    MOQ_TEST_CHECK_EQ_U64(p.client->fill_storage[child.slot].delivery.group_order, 2);
    if (pressure) {
        /* ACK and first object occupy the two slots. The second object is
         * still owned by RX, with prior advanced exactly once, until polling. */
        MOQ_TEST_CHECK(rx->resumed_deferred);
        MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.group_id, 10);
        MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.object_id, 6);
        MOQ_TEST_CHECK_EQ_SIZE(rx->input_len, 4);
    }
    if (!explicit_order) failures += expect_fill_events(&p, want, 2, false);
    else failures += expect_fill_events(&p, NULL, 0, false);
    if (fin) {
        MOQ_TEST_CHECK(!find_rx(&p));
        MOQ_TEST_CHECK_EQ_SIZE(p.terminals, 1);
    } else {
        MOQ_TEST_CHECK_EQ_SIZE(rx->input_len, 0);
        MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.group_id, 7);
        MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.object_id, 7);
    }
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int retired_fetch_rx(bool header_before_retirement)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, MOQ_VERSION_DRAFT_22, 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.group_order = MOQ_GROUP_ORDER_DESCENDING;
    cfg.end_group = 20;
    cfg.end_object = 10;
    moq_fetch_t old_fetch;
    MOQ_TEST_CHECK_EQ_INT(moq_session_fetch(p.client, &cfg, 1, &old_fetch), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    moq_event_t ev;
    ok = take(p.server, MOQ_EVENT_FETCH_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_fetch_t server_fetch = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);
    int old_slot = fetch_resolve_handle(p.client, old_fetch);
    MOQ_TEST_CHECK(old_slot >= 0);
    if (old_slot < 0) goto done;
    moq_stream_ref_t request_ref = p.client->fetches[old_slot].rs.ref;
    moq_accept_fetch_cfg_t accept;
    moq_accept_fetch_cfg_init(&accept);
    accept.end_group = 20;
    accept.end_object = 10;
    MOQ_TEST_CHECK_EQ_INT(moq_session_accept_fetch(p.server, server_fetch, &accept, 2), MOQ_OK);
    uint8_t control[128], header[32];
    size_t control_len, header_len;
    MOQ_TEST_CHECK(capture_response(p.server, control, sizeof(control), &control_len,
                                    header, sizeof(header), &header_len));
    MOQ_TEST_CHECK(control_len > 0 && header_len > 0);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.client, request_ref,
                           control, control_len, false, 2), MOQ_OK);
    p.stream = moq_stream_ref_from_u64(901);
    const uint8_t first[] = {0x1c, 10, 6, 128, 0};
    if (header_before_retirement) {
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                               header, header_len, false, 3), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                               first, sizeof(first), false, 3), MOQ_OK);
    }
    drain(p.client);
    /* White-box production retirement helper, not a public GOAWAY proof:
     * the existing post-FETCH_OK control gate refuses that public route.
     * No synthetic owner/generation edits; the helper retires this real owner. */
    fetch_on_request_goaway_release(p.client, old_slot);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p.client), MOQ_SESS_ESTABLISHED);
    if (!header_before_retirement) {
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                               header, header_len, false, 5), MOQ_OK);
    }
    MOQ_TEST_CHECK(fetch_resolve_handle(p.client, old_fetch) < 0);
    moq_rx_stream_t *rx = find_rx(&p);
    MOQ_TEST_CHECK(rx && rx->fetch_order_known);
    if (!rx) goto done;
    cfg.group_order = MOQ_GROUP_ORDER_ASCENDING;
    moq_fetch_t replacement;
    MOQ_TEST_CHECK_EQ_INT(moq_session_fetch(p.client, &cfg, 6, &replacement), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fetch_resolve_handle(p.client, replacement), old_slot);
    MOQ_TEST_CHECK_EQ_INT(p.client->fetches[old_slot].effective_group_order, MOQ_GROUP_ORDER_ASCENDING);
    drain(p.client);
    if (!header_before_retirement)
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                               first, sizeof(first), false, 7), MOQ_OK);
    const uint8_t rest[] = {0x08, 2, 0, 0x81, 0x0c, 6, 4, 0x08, 0, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           rest, sizeof(rest), false, 8), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(rx->effective_fetch_group_order, MOQ_GROUP_ORDER_DESCENDING);
    MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(rx->fetch_prior.object_id, 5);
    MOQ_TEST_CHECK(rx->fetch_prior.has_actual);
    while (moq_session_poll_events(p.client, &ev, 1)) {
        MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_FETCH_OBJECT && ev.kind != MOQ_EVENT_FETCH_GAP);
        MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_SESSION_CLOSED);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream, NULL, 0, true, 9), MOQ_OK);
    MOQ_TEST_CHECK(!find_rx(&p));
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p.client), MOQ_SESS_ESTABLISHED);
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int fill_range_fin(void)
{
    fixture_t p;
    int failures = start_fill(&p, MOQ_GROUP_ORDER_DESCENDING, false, 0);
    if (!p.ready) goto done;
    const uint8_t wire[] = {0x1f, 10, 3, 6, 128, 0, 0x82, 0x0c, 7, 9};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           wire, sizeof(wire), true, 4), MOQ_OK);
    const moq_event_kind_t kinds[] = {MOQ_EVENT_FILL_OBJECT, MOQ_EVENT_FILL_GAP,
                                     MOQ_EVENT_FILL_TERMINAL};
    size_t count = 0;
    moq_event_t ev;
    while (moq_session_poll_events(p.client, &ev, 1)) {
        MOQ_TEST_CHECK(count < 3);
        if (count < 3) MOQ_TEST_CHECK_EQ_INT(ev.kind, kinds[count]);
        if (ev.kind == MOQ_EVENT_FILL_GAP) {
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_gap.group_id, 7);
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_gap.object_id, 9);
            MOQ_TEST_CHECK_EQ_INT(ev.u.fill_gap.kind, MOQ_FETCH_RANGE_TIMED_OUT);
        }
        if (ev.kind == MOQ_EVENT_FILL_TERMINAL) MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_terminal.fill, p.client_fill));
        moq_event_cleanup(&ev);
        ++count;
    }
    MOQ_TEST_CHECK_EQ_SIZE(count, 3);
    MOQ_TEST_CHECK(!find_rx(&p));
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p.client), MOQ_SESS_ESTABLISHED);
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

/* Draft-18 requests a descending FETCH: one request on the wire, a live
 * handle (its response's group deltas count down, 11.4.4.1). */
static int d18_local_descending_request(void)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, MOQ_VERSION_DRAFT_18, 0);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        moq_fetch_cfg_t cfg;
        moq_fetch_cfg_init(&cfg);
        const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
        cfg.track_namespace = (moq_namespace_t){parts, 1};
        cfg.track_name = MOQ_BYTES_LITERAL("track");
        cfg.group_order = MOQ_GROUP_ORDER_DESCENDING;
        cfg.end_group = 20;
        cfg.end_object = 10;
        size_t tail = p.client->action_tail;
        moq_fetch_t handle;
        MOQ_TEST_CHECK_EQ_INT(moq_session_fetch(p.client, &cfg, 1, &handle), MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(p.client->action_tail, tail + 1);
        MOQ_TEST_CHECK(moq_fetch_is_valid(handle));
    }
    moq_simpair_destroy(p.pair);
    return failures;
}

static int fill_writer_queue_retry(void)
{
    fixture_t p;
    int failures = start_fill(&p, MOQ_GROUP_ORDER_DESCENDING, false, 2);
    if (!p.ready) goto done;
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    cfg.group_id = 10;
    cfg.object_id = 6;
    cfg.publisher_priority = 128;
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server, p.server_fill, &cfg, 4), MOQ_OK);
    moq_fill_ref_t ref;
    moq_result_t rc = moq_fill_find(&p.server->fills, p.initiating_id, &ref);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) goto done;
    moq_fetch_prior_object_t before = p.server->fill_side[ref.slot].prior;
    size_t tail = p.server->action_tail;
    const uint8_t props[] = {2, 0x41}, payload[] = {0x42};
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), props, sizeof(props), &cfg.properties), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &cfg.payload), MOQ_OK);
    cfg.group_id = 7;
    cfg.object_id = 7;
    /* Two actions needed, only one free: refuse without prior/ownership change. */
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server, p.server_fill, &cfg, 5), MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(p.server->action_tail, tail);
    MOQ_TEST_CHECK(memcmp(&before, &p.server->fill_side[ref.slot].prior, sizeof(before)) == 0);
    MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_refcount(cfg.properties), 1);
    MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_refcount(cfg.payload), 1);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    drain(p.client);
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server, p.server_fill, &cfg, 6), MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(p.server->fill_side[ref.slot].prior.group_id, 7);
    MOQ_TEST_CHECK_EQ_U64(p.server->fill_side[ref.slot].prior.object_id, 7);
    MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_refcount(cfg.properties), 2);
    MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_refcount(cfg.payload), 2);
    moq_rcbuf_decref(cfg.properties);
    moq_rcbuf_decref(cfg.payload);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    moq_event_t ev;
    bool ok = take(p.client, MOQ_EVENT_FILL_OBJECT, &ev);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.group_id, 7);
        MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.object_id, 7);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(ev.u.fill_object.payload), 1);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(ev.u.fill_object.properties), 2);
        if (moq_rcbuf_len(ev.u.fill_object.payload) == 1)
            MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.fill_object.payload)[0] == 0x42);
        moq_event_cleanup(&ev);
    }
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int publish_fill_inherited_track_order(void)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, MOQ_VERSION_DRAFT_22, 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    const uint8_t props[] = {0x22, 2};
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){parts, 1};
    pc.track_name = MOQ_BYTES_LITERAL("track");
    pc.track_properties = (moq_bytes_t){props, sizeof(props)};
    moq_publication_t server_pub;
    MOQ_TEST_CHECK_EQ_INT(moq_session_publish(p.server, &pc, 1, &server_pub), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    moq_event_t ev;
    ok = take(p.client, MOQ_EVENT_PUBLISH_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_publication_t client_pub = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t accept;
    moq_accept_publish_cfg_init(&accept);
    MOQ_TEST_CHECK_EQ_INT(moq_session_accept_publish(p.client, client_pub, &accept, 2), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    drain(p.client);
    drain(p.server);
    /* Real published data establishes the Largest used by the update ACK. */
    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 10;
    sg.publisher_priority = 128;
    moq_subgroup_handle_t subgroup;
    MOQ_TEST_CHECK_EQ_INT(moq_session_open_pub_subgroup(p.server, server_pub, &sg, 3, &subgroup), MOQ_OK);
    const uint8_t byte = 0x30;
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), &byte, 1, &payload), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_object(p.server, subgroup, 6, payload, 3), MOQ_OK);
    moq_rcbuf_decref(payload);
    MOQ_TEST_CHECK_EQ_INT(moq_session_close_subgroup(p.server, subgroup, 3), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    drain(p.client);
    drain(p.server);
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    moq_publication_update_cfg_t update;
    moq_publication_update_cfg_init(&update);
    update.fill = &fill;
    MOQ_TEST_CHECK_EQ_INT(moq_session_update_publication(p.client, client_pub, &update, 4), MOQ_OK);
    ok = take(p.client, MOQ_EVENT_FILL_CREATED, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.client_fill = ev.u.fill_created.fill;
    p.initiating_id = ev.u.fill_created.initiating_request_id;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    ok = take(p.server, MOQ_EVENT_FILL_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.server_fill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.group_order, 2);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(moq_session_open_fill(p.server, p.server_fill, 5), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(pump(&p), MOQ_OK);
    moq_fill_ref_t ref;
    moq_result_t rc = moq_fill_find(&p.client->fills, p.initiating_id, &ref);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) goto done;
    MOQ_TEST_CHECK(p.client->fill_storage[ref.slot].delivery_known);
    MOQ_TEST_CHECK_EQ_U64(p.client->fill_storage[ref.slot].delivery.group_order, 2);
    p.stream = p.client->fill_storage[ref.slot].stream;
    drain(p.client);
    const uint8_t objects[] = {0x1c, 10, 6, 128, 0, 0x08, 2, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream,
                           objects, sizeof(objects), false, 6), MOQ_OK);
    size_t count = 0;
    while (moq_session_poll_events(p.client, &ev, 1)) {
        MOQ_TEST_CHECK_EQ_INT(ev.kind, MOQ_EVENT_FILL_OBJECT);
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.group_id, count == 0 ? 10 : 7);
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.object_id, count == 0 ? 6 : 7);
            MOQ_TEST_CHECK(moq_publication_eq(ev.u.fill_object.pub, client_pub));
            MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_object.fill, p.client_fill));
            MOQ_TEST_CHECK(!moq_subscription_is_valid(ev.u.fill_object.sub));
        }
        moq_event_cleanup(&ev);
        ++count;
    }
    MOQ_TEST_CHECK_EQ_SIZE(count, 2);
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

/* INCLUDE_PROPERTIES=0 must preserve the direction for initial AND future
 * inherited fills. Requests, expected ACKs and object oracles are literal;
 * production object output is checked separately, never used as the oracle. */
static int fill_order_property_agreement(moq_bytes_t properties,
    moq_bytes_t expected_properties, uint8_t parent_order, uint8_t fill_order,
    uint8_t effective_order, bool omit, bool later, bool short_output,
    uint8_t expected_priority)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, MOQ_VERSION_DRAFT_22, 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_NONE;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    fill.has_group_order = fill_order != MOQ_GROUP_ORDER_DEFAULT;
    fill.group_order = fill_order;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.group_order = parent_order;
    cfg.fill = later ? NULL : &fill;
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(p.client, &cfg, 1, &p.client_sub), MOQ_OK);
    moq_event_t ev;
    if (!later) {
        ok = take(p.client, MOQ_EVENT_FILL_CREATED, &ev);
        MOQ_TEST_CHECK(ok);
        if (!ok) goto done;
        p.client_fill = ev.u.fill_created.fill;
        p.initiating_id = ev.u.fill_created.initiating_request_id;
        moq_event_cleanup(&ev);
    }
    moq_action_t a;
    ok = moq_session_poll_actions(p.client, &a, 1) == 1;
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    MOQ_TEST_CHECK_EQ_INT(a.kind, MOQ_ACTION_OPEN_BIDI_STREAM);
    moq_stream_ref_t request_ref = a.u.open_bidi_stream.stream_ref;
    moq_action_cleanup(&a);
    /* SUBSCRIBE id0 delta/track. Parameter type deltas, no encoder oracle. */
    uint8_t request[40] = {3, 0, 0, 0, 1, 5, 'd', 'e', 'l', 't', 'a',
        5, 't', 'r', 'a', 'c', 'k', 0};
    size_t n = 18;
    uint8_t previous = 0;
    if (parent_order != MOQ_GROUP_ORDER_DEFAULT) {
        request[17]++;
        request[n++] = 0x22;
        request[n++] = parent_order;
        previous = 0x22;
    }
    if (!later) {
        request[17]++;
        request[n++] = (uint8_t)(0x23 - previous);
        request[n++] = fill_order == MOQ_GROUP_ORDER_DEFAULT ? 3 : 5;
        request[n++] = fill_order == MOQ_GROUP_ORDER_DEFAULT ? 1 : 2;
        request[n++] = 0x21; request[n++] = 0;
        if (fill_order != MOQ_GROUP_ORDER_DEFAULT) {
            request[n++] = 1; request[n++] = fill_order;
        }
        previous = 0x23;
    }
    if (omit) {
        request[17]++;
        request[n++] = (uint8_t)(0x35 - previous); request[n++] = 0;
    }
    request[2] = (uint8_t)(n - 3);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.server, request_ref,
        request, n, false, 2), MOQ_OK);
    ok = take(p.server, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.server_sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.has_largest = true;
    accept.largest_group = accept.largest_object = 10;
    accept.track_properties = properties;
    /* Alias1, one parameter: LARGEST_OBJECT=(10,10); then raw properties. */
    uint8_t expected_ack[128] = {4, 0, 5, 1, 1, 0x09, 10, 10};
    size_t expected_len = 8 + expected_properties.len;
    MOQ_TEST_CHECK(expected_len <= sizeof(expected_ack));
    if (expected_len > sizeof(expected_ack)) goto done;
    if (expected_properties.len)
        memcpy(expected_ack + 8, expected_properties.data, expected_properties.len);
    expected_ack[2] = (uint8_t)(5 + expected_properties.len);
    if (short_output) {
        int slot = sub_resolve_handle(p.server, p.server_sub);
        MOQ_TEST_CHECK(slot >= 0);
        if (slot < 0) goto done;
        moq_sub_entry_t before = p.server->subs[slot];
        uint64_t alias_before = p.server->profile->next_track_alias(p.server);
        size_t head = p.server->action_head, tail = p.server->action_tail;
        size_t send_len = p.server->send_len, send_cap = p.server->send_cap;
        moq_fill_ref_t ref = {0};
        bool has_fill = moq_fill_find(&p.server->fills, 0, &ref) == MOQ_OK;
        moq_fill_entry_t fill_before = {0};
        moq_fill_side_t side_before = {0};
        if (has_fill) {
            fill_before = p.server->fill_storage[ref.slot];
            side_before = p.server->fill_side[ref.slot];
        }
        p.server->send_cap = expected_len - 1;
        MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(p.server, p.server_sub,
            &accept, 3), MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(memcmp(&before, &p.server->subs[slot], sizeof(before)) == 0);
        MOQ_TEST_CHECK_EQ_U64(p.server->profile->next_track_alias(p.server), alias_before);
        MOQ_TEST_CHECK_EQ_SIZE(p.server->action_head, head);
        MOQ_TEST_CHECK_EQ_SIZE(p.server->action_tail, tail);
        MOQ_TEST_CHECK_EQ_SIZE(p.server->send_len, send_len);
        if (has_fill) {
            MOQ_TEST_CHECK(memcmp(&fill_before, &p.server->fill_storage[ref.slot],
                sizeof(fill_before)) == 0);
            MOQ_TEST_CHECK(memcmp(&side_before, &p.server->fill_side[ref.slot],
                sizeof(side_before)) == 0);
        }
        /* Restore the actual allocation's bound, not a larger queue/buffer. */
        p.server->send_cap = send_cap;
    }
    MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(p.server, p.server_sub, &accept, 4), MOQ_OK);
    uint8_t ack[128], header[32];
    size_t ack_len, header_len;
    MOQ_TEST_CHECK(capture_response(p.server, ack, sizeof(ack), &ack_len,
        header, sizeof(header), &header_len));
    MOQ_TEST_CHECK_EQ_SIZE(ack_len, expected_len);
    if (ack_len == expected_len) MOQ_TEST_CHECK(memcmp(ack, expected_ack, expected_len) == 0);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.client, request_ref,
        ack, ack_len, false, 5), MOQ_OK);
    int server_slot = sub_resolve_handle(p.server, p.server_sub);
    int client_slot = sub_resolve_handle(p.client, p.client_sub);
    MOQ_TEST_CHECK(server_slot >= 0 && client_slot >= 0);
    if (server_slot >= 0 && client_slot >= 0) {
        MOQ_TEST_CHECK_EQ_U64(p.server->subs[server_slot].default_publisher_priority,
                              expected_priority);
        MOQ_TEST_CHECK_EQ_U64(p.client->subs[client_slot].default_publisher_priority,
                              expected_priority);
    }
    drain(p.client);
    if (later) {
        MOQ_TEST_CHECK_EQ_SIZE(p.client->fills.live, 0);
        MOQ_TEST_CHECK_EQ_SIZE(p.server->fills.live, 0);
        moq_subscription_update_cfg_t update;
        moq_subscription_update_cfg_init_sized(&update, sizeof(update));
        update.fill = &fill;
        MOQ_TEST_CHECK_EQ_INT(moq_session_update_subscription(p.client, p.client_sub, &update, 6), MOQ_OK);
        ok = take(p.client, MOQ_EVENT_FILL_CREATED, &ev);
        MOQ_TEST_CHECK(ok);
        if (!ok) goto done;
        p.client_fill = ev.u.fill_created.fill;
        p.initiating_id = ev.u.fill_created.initiating_request_id;
        moq_event_cleanup(&ev);
        ok = moq_session_poll_actions(p.client, &a, 1) == 1;
        MOQ_TEST_CHECK(ok);
        if (!ok) goto done;
        MOQ_TEST_CHECK_EQ_INT(a.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK(a.u.send_bidi_stream.stream_ref._v == request_ref._v);
        MOQ_TEST_CHECK(a.u.send_bidi_stream.len <= sizeof(request));
        n = a.u.send_bidi_stream.len;
        if (n > sizeof(request)) { moq_action_cleanup(&a); goto done; }
        memcpy(request, a.u.send_bidi_stream.data, n);
        moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.server, request_ref,
            request, n, false, 7), MOQ_OK);
        MOQ_TEST_CHECK(capture_response(p.server, ack, sizeof(ack), &ack_len,
            header, sizeof(header), &header_len));
        MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.client, request_ref,
            ack, ack_len, false, 8), MOQ_OK);
        drain(p.client);
    }
    ok = take(p.server, MOQ_EVENT_FILL_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.server_fill = ev.u.fill_request.fill;
    MOQ_TEST_CHECK_EQ_U64(ev.u.fill_request.group_order, effective_order);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(moq_session_open_fill(p.server, p.server_fill, 9), MOQ_OK);
    MOQ_TEST_CHECK(capture_response(p.server, ack, sizeof(ack), &ack_len,
        header, sizeof(header), &header_len));
    p.stream = moq_stream_ref_from_u64(911);
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream, header,
        header_len, false, 10), MOQ_OK);
    uint8_t first = effective_order == MOQ_GROUP_ORDER_DESCENDING ? 10 : 4;
    moq_fetch_object_cfg_t object;
    moq_fetch_object_cfg_init(&object);
    object.publisher_priority = 128;
    object.group_id = first; object.object_id = 6;
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server, p.server_fill, &object, 11), MOQ_OK);
    object.group_id = 7; object.object_id = 7;
    MOQ_TEST_CHECK_EQ_INT(moq_session_write_fill_object(p.server, p.server_fill, &object, 12), MOQ_OK);
    MOQ_TEST_CHECK(capture_response(p.server, ack, sizeof(ack), &ack_len,
        header, sizeof(header), &header_len));
    const uint8_t writer_oracle[] = {0x1c, first, 6, 128, 0, 0x1d, 2, 7, 128, 0};
    MOQ_TEST_CHECK_EQ_SIZE(header_len, sizeof(writer_oracle));
    if (header_len == sizeof(writer_oracle))
        MOQ_TEST_CHECK(memcmp(header, writer_oracle, sizeof(writer_oracle)) == 0);
    const uint8_t literal_objects[] = {0x1c, first, 6, 128, 0, 0x08, 2, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_data_bytes(p.client, p.stream, literal_objects,
        sizeof(literal_objects), false, 13), MOQ_OK);
    moq_rx_stream_t *rx = find_rx(&p);
    MOQ_TEST_CHECK(rx && rx->fetch_order_known);
    if (rx) MOQ_TEST_CHECK_EQ_INT(rx->effective_fetch_group_order, effective_order);
    size_t count = 0;
    while (moq_session_poll_events(p.client, &ev, 1)) {
        MOQ_TEST_CHECK_EQ_INT(ev.kind, MOQ_EVENT_FILL_OBJECT);
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.group_id, count == 0 ? first : 7);
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.object_id, count == 0 ? 6 : 7);
            MOQ_TEST_CHECK(moq_fill_eq(ev.u.fill_object.fill, p.client_fill));
            ++count;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_SIZE(count, 2);
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int invalid_accept_default_priority(moq_version_t version, bool omit,
                                           bool immutable)
{
    int failures = 0;
    fixture_t p;
    bool ok = make_pair(&p, version, 0);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    const moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("delta")};
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){parts, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(p.client, &cfg, 1, &p.client_sub), MOQ_OK);
    moq_action_t a;
    ok = moq_session_poll_actions(p.client, &a, 1) == 1;
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    MOQ_TEST_CHECK_EQ_INT(a.kind, MOQ_ACTION_OPEN_BIDI_STREAM);
    const uint8_t request[] = {3, 0, 17, 0, 1, 5, 'd', 'e', 'l', 't', 'a',
        5, 't', 'r', 'a', 'c', 'k', 1, 0x35, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_session_on_bidi_stream_bytes(p.server,
        a.u.open_bidi_stream.stream_ref, omit ? request : a.u.open_bidi_stream.data,
        omit ? sizeof(request) : a.u.open_bidi_stream.len, false, 2), MOQ_OK);
    moq_action_cleanup(&a);
    moq_event_t ev;
    ok = take(p.server, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(ok);
    if (!ok) goto done;
    p.server_sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    int slot = sub_resolve_handle(p.server, p.server_sub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) goto done;
    const uint8_t direct[] = {0x0e, 0x81, 0}; /* vi64 value256 */
    const uint8_t nested[] = {0x0b, 3, 0x0e, 0x81, 0};
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.track_properties = immutable ? (moq_bytes_t){nested, sizeof(nested)}
                                       : (moq_bytes_t){direct, sizeof(direct)};
    moq_sub_entry_t before = p.server->subs[slot];
    uint64_t alias = p.server->profile->next_track_alias(p.server);
    uint64_t epoch = p.server->borrow_epoch, now = p.server->last_now_us;
    size_t head = p.server->action_head, tail = p.server->action_tail;
    size_t send_len = p.server->send_len;
    uint8_t send_prefix[32];
    MOQ_TEST_CHECK(p.server->send_cap >= sizeof(send_prefix));
    if (p.server->send_cap < sizeof(send_prefix)) goto done;
    memcpy(send_prefix, p.server->send_buf, sizeof(send_prefix));
    MOQ_TEST_CHECK_EQ_INT(moq_session_accept_subscribe(p.server, p.server_sub,
                                                     &accept, 100), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&before, &p.server->subs[slot], sizeof(before)) == 0);
    MOQ_TEST_CHECK_EQ_U64(p.server->profile->next_track_alias(p.server), alias);
    MOQ_TEST_CHECK_EQ_U64(p.server->borrow_epoch, epoch);
    MOQ_TEST_CHECK_EQ_U64(p.server->last_now_us, now);
    MOQ_TEST_CHECK_EQ_SIZE(p.server->action_head, head);
    MOQ_TEST_CHECK_EQ_SIZE(p.server->action_tail, tail);
    MOQ_TEST_CHECK_EQ_SIZE(p.server->send_len, send_len);
    MOQ_TEST_CHECK(memcmp(send_prefix, p.server->send_buf, sizeof(send_prefix)) == 0);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(p.server), MOQ_SESS_ESTABLISHED);
done:
    moq_simpair_destroy(p.pair);
    return failures;
}

static int record(const char *name, int failures, int *failed_cases)
{
    fprintf(stderr, "CASE %s: %s (%d failed checks)\n", name,
            failures ? "FAIL" : "PASS", failures);
    if (failures) ++*failed_cases;
    return failures;
}

int main(void)
{
    int failures = 0, failed_cases = 0;
    const expected_event_t two[] = {
        {MOQ_EVENT_FILL_OBJECT, 10, 6, 0, 0, 128, 0x41},
        {MOQ_EVENT_FILL_OBJECT, 7, 7, 0, 0, 128, 0x42},
    };
    const expected_event_t absolute[] = {
        {MOQ_EVENT_FILL_OBJECT, 10, 6, 0, 0, 128, 0x41},
        {MOQ_EVENT_FILL_OBJECT, 7, 1, 0, 0, 128, 0x42},
    };
    const expected_event_t same[] = {
        {MOQ_EVENT_FILL_OBJECT, 10, 6, 0, 0, 128, 0x41},
        {MOQ_EVENT_FILL_OBJECT, 10, 8, 0, 0, 128, 0x42},
    };
    const uint8_t omitted[] = {0x1c, 10, 6, 128, 1, 0x41, 0x08, 2, 1, 0x42};
    const uint8_t explicit[] = {0x1c, 10, 6, 128, 1, 0x41, 0x0c, 2, 1, 1, 0x42};
    const uint8_t same_group[] = {0x1c, 10, 6, 128, 1, 0x41, 0x04, 2, 1, 0x42};
    failures += record("d22_fill_explicit_desc_omitted_object_delta",
        fill_bytes(omitted, sizeof(omitted), two, 2, false, 2, false), &failed_cases);
    failures += record("d22_fill_inherited_desc_ack_snapshot",
        fill_bytes(omitted, sizeof(omitted), two, 2, false, 2, true), &failed_cases);
    failures += record("d22_fill_desc_explicit_object_absolute",
        fill_bytes(explicit, sizeof(explicit), absolute, 2, false, 2, false), &failed_cases);
    failures += record("d22_fill_desc_same_group_object_delta",
        fill_bytes(same_group, sizeof(same_group), same, 2, false, 2, false), &failed_cases);
    const struct {
        const char *name;
        uint8_t marker[2];
        uint32_t kind;
    } markers[] = {
        {"desc_nonexistent_range_prior_actual", {0x80, 0x8c}, MOQ_FETCH_RANGE_NON_EXISTENT},
        {"desc_unknown_range_prior_actual", {0x81, 0x0c}, MOQ_FETCH_RANGE_UNKNOWN},
        {"desc_timedout_range_prior_actual", {0x82, 0x0c}, MOQ_FETCH_RANGE_TIMED_OUT},
    };
    for (size_t i = 0; i < sizeof(markers) / sizeof(markers[0]); ++i) {
        uint8_t wire[] = {0x1f, 10, 3, 6, 128, 1, 0x41,
                          markers[i].marker[0], markers[i].marker[1], 8, 4,
                          0x0a, 0, 1, 0x42};
        const expected_event_t want[] = {
            {MOQ_EVENT_FILL_OBJECT, 10, 6, 3, 0, 128, 0x41},
            {MOQ_EVENT_FILL_GAP, 8, 4, 0, markers[i].kind, 0, 0},
            {MOQ_EVENT_FILL_OBJECT, 7, 5, 4, 0, 128, 0x42},
        };
        failures += record(markers[i].name,
            fill_bytes(wire, sizeof(wire), want, 3, false, 2, false), &failed_cases);
    }
    const uint8_t leading_range[] = {0x81, 0x0c, 8, 4, 0x18, 0, 128, 1, 0x42};
    const expected_event_t leading[] = {
        {MOQ_EVENT_FILL_GAP, 8, 4, 0, MOQ_FETCH_RANGE_UNKNOWN, 0, 0},
        {MOQ_EVENT_FILL_OBJECT, 7, 5, 0, 0, 128, 0x42},
    };
    failures += record("desc_leading_range_location_not_actual",
        fill_bytes(leading_range, sizeof(leading_range), leading, 2, false, 2, false), &failed_cases);
    const uint8_t zero_group[] = {0x1c, 3, 6, 128, 0, 0x0c, 2, 0, 0};
    const expected_event_t zero[] = {
        {MOQ_EVENT_FILL_OBJECT, 3, 6, 0, 0, 128, 0},
        {MOQ_EVENT_FILL_OBJECT, 0, 0, 0, 0, 128, 0},
    };
    failures += record("desc_group_zero_boundary",
        fill_bytes(zero_group, sizeof(zero_group), zero, 2, false, 2, false), &failed_cases);
    const uint8_t max_group[] = {0x1c, VI64_MAX_BYTES, 6, 128, 0, 0x08, 0, 0};
    const expected_event_t max[] = {
        {MOQ_EVENT_FILL_OBJECT, UINT64_MAX, 6, 0, 0, 128, 0},
        {MOQ_EVENT_FILL_OBJECT, UINT64_MAX - 1, 7, 0, 0, 128, 0},
    };
    failures += record("desc_group_max_minus_one_boundary",
        fill_bytes(max_group, sizeof(max_group), max, 2, false, 2, false), &failed_cases);
    const struct {
        const char *name;
        uint8_t wire[32];
        size_t len;
        uint32_t order;
    } rejected[] = {
        {"desc_group_underflow_zero", {0x1c, 0, 6, 128, 0, 0x0c, 0, 1, 0}, 9, 2},
        {"desc_group_underflow_delta_equals_prior", {0x1c, 3, 6, 128, 0, 0x0c, 3, 1, 0}, 9, 2},
        {"desc_group_delta_plus_one_overflow", {0x1c, 10, 6, 128, 0, 0x0c, VI64_MAX_BYTES, 1, 0}, 17, 2},
        {"ascending_group_max_overflow", {0x1c, VI64_MAX_BYTES, 6, 128, 0, 0x0c, 0, 1, 0}, 17, 1},
        {"desc_object_omitted_across_group_overflow", {0x1c, 10, VI64_MAX_BYTES, 128, 0, 0x08, 0, 0}, 16, 2},
        {"desc_same_group_object_add_overflow", {0x1c, 10, VI64_MAX_BYTES, 128, 0, 0x04, 1, 0}, 16, 2},
        {"desc_subgroup_prior_plus_one_overflow", {0x1f, 10, VI64_MAX_BYTES, 6, 128, 0, 0x02, 0}, 16, 2},
        {"first_object_missing_object_delta", {0x18, 10, 128, 0}, 4, 2},
        {"first_object_missing_group_delta", {0x14, 6, 128, 0}, 4, 2},
        {"first_object_missing_priority", {0x0c, 10, 6, 0}, 4, 2},
        {"first_object_prior_subgroup", {0x1d, 10, 6, 128, 0}, 5, 2},
        {"leading_range_missing_actual_priority", {0x81, 0x0c, 8, 4, 0x08, 0, 0}, 7, 2},
        {"leading_range_missing_actual_subgroup", {0x81, 0x0c, 8, 4, 0x19, 0, 128, 0}, 8, 2},
    };
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); ++i)
        failures += record(rejected[i].name,
            fill_bytes(rejected[i].wire, rejected[i].len, NULL, 0, true,
                       rejected[i].order, false), &failed_cases);
    failures += record("ascending_d18_real_fetch_control",
        fetch_control(MOQ_VERSION_DRAFT_18, MOQ_GROUP_ORDER_ASCENDING), &failed_cases);
    failures += record("ascending_d22_real_fetch_control",
        fetch_control(MOQ_VERSION_DRAFT_22, MOQ_GROUP_ORDER_ASCENDING), &failed_cases);
    failures += record("unchanged_d16_desc_absolute_control",
        fetch_control(MOQ_VERSION_DRAFT_16, MOQ_GROUP_ORDER_DESCENDING), &failed_cases);
    failures += record("d22_local_desc_fetch_support_gap",
        fetch_control(MOQ_VERSION_DRAFT_22, MOQ_GROUP_ORDER_DESCENDING), &failed_cases);
    failures += record("d22_inbound_desc_fetch_writer",
        inbound_fetch_writer(MOQ_VERSION_DRAFT_22), &failed_cases);
    failures += record("d18_admitted_peer_desc_fetch_writer_literal_wire",
        inbound_fetch_writer(MOQ_VERSION_DRAFT_18), &failed_cases);
    failures += record("d18_local_descending_request", d18_local_descending_request(),
                       &failed_cases);
    failures += record("d22_desc_fill_writer", fill_writer(), &failed_cases);
    failures += record("desc_fill_writer_object_wrap_refusal",
        fill_writer_object_wrap(), &failed_cases);
    failures += record("desc_pending_cancel_still_decodes", pending_cancel(), &failed_cases);
    failures += record("desc_public_cancel_opaque_discard", public_cancel_discard(), &failed_cases);
    failures += record("pre_ack_unknown_order_retains_then_desc_snapshot",
        pre_ack_fill(false, false, false), &failed_cases);
    failures += record("pre_ack_explicit_order_no_invented_outcome",
        pre_ack_fill(true, false, false), &failed_cases);
    failures += record("pre_ack_unknown_order_queue_pressure_retry",
        pre_ack_fill(false, true, false), &failed_cases);
    failures += record("pre_ack_unknown_order_retained_fin_queue_retry",
        pre_ack_fill(false, true, true), &failed_cases);
    failures += record("desc_rx_whitebox_bound_owner_retirement_reuse",
        retired_fetch_rx(true), &failed_cases);
    failures += record("desc_rx_whitebox_late_retirement_tomb_snapshot_reuse",
        retired_fetch_rx(false), &failed_cases);
    failures += record("desc_range_marker_then_fin", fill_range_fin(), &failed_cases);
    failures += record("desc_writer_queue_refusal_ownership_and_retry",
        fill_writer_queue_retry(), &failed_cases);
    failures += record("publish_update_fill_inherited_track_desc_snapshot",
        publish_fill_inherited_track_order(), &failed_cases);
    const uint8_t mutable_desc[] = {6, 9, 0x1c, 2, 0x56, 1};
    const uint8_t desc[] = {0x22, 2};
    const uint8_t mutable_asc[] = {6, 9, 0x1c, 1, 0x56, 1};
    const uint8_t optional[] = {0x78, 1};
    /* Non-minimal vi64s in the immutable length, type and value stay intact.
     * The outer delta5 is rebased to11 after omitting the preceding type6. */
    const uint8_t immutable_desc[] = {6, 9, 5, 0x80, 6, 0x80, 0x22, 0x80, 2,
        0x56, 1, 0x6d, 3};
    const uint8_t immutable_only[] = {0x0b, 0x80, 6, 0x80, 0x22, 0x80, 2, 0x56, 1};
    const uint8_t immutable_wide[] = {0x80, 0x0b, 0x80, 6, 0x80, 0x22, 0x80, 2,
        0x56, 1, 0x6d, 3};
    const uint8_t immutable_wide_only[] = {0x80, 0x0b, 0x80, 6, 0x80, 0x22, 0x80, 2,
        0x56, 1};
    const moq_bytes_t no_properties = {NULL, 0};
#define AGREEMENT(name, source, expected, parent, child, effective, omit, later, short_out) \
    failures += record(name, fill_order_property_agreement(source, expected, parent, \
        child, effective, omit, later, short_out, 128), &failed_cases)
#define BYTES(a) ((moq_bytes_t){a, sizeof(a)})
    AGREEMENT("omit_inherited_desc_initial", BYTES(mutable_desc), BYTES(desc), 0, 0, 2, true, false, false);
    AGREEMENT("include_inherited_desc_initial", BYTES(mutable_desc), BYTES(mutable_desc), 0, 0, 2, false, false, false);
    AGREEMENT("omit_inherited_desc_later", BYTES(mutable_desc), BYTES(desc), 0, 0, 2, true, true, false);
    AGREEMENT("include_inherited_desc_later", BYTES(mutable_desc), BYTES(mutable_desc), 0, 0, 2, false, true, false);
    AGREEMENT("omit_explicit_parent_asc", BYTES(mutable_desc), no_properties, 1, 0, 1, true, false, false);
    AGREEMENT("omit_explicit_parent_desc", BYTES(mutable_desc), no_properties, 2, 0, 2, true, false, false);
    AGREEMENT("omit_explicit_fill_asc", BYTES(mutable_desc), BYTES(desc), 0, 1, 1, true, false, false);
    AGREEMENT("omit_explicit_fill_desc", BYTES(mutable_desc), BYTES(desc), 0, 2, 2, true, false, false);
    AGREEMENT("omit_absent_order_default_asc", BYTES(optional), no_properties, 0, 0, 1, true, false, false);
    AGREEMENT("omit_explicit_track_asc", BYTES(mutable_asc), no_properties, 0, 0, 1, true, false, false);
    AGREEMENT("omit_empty_track_default_asc", no_properties, no_properties, 0, 0, 1, true, true, false);
    AGREEMENT("omit_desc_output_refusal_retry", BYTES(mutable_desc), BYTES(desc), 0, 0, 2, true, false, true);
    AGREEMENT("omit_immutable_desc_initial", BYTES(immutable_desc), BYTES(immutable_only), 0, 0, 2, true, false, false);
    AGREEMENT("omit_immutable_desc_later", BYTES(immutable_desc), BYTES(immutable_only), 0, 0, 2, true, true, false);
    AGREEMENT("omit_immutable_desc_output_refusal_retry", BYTES(immutable_desc), BYTES(immutable_only), 0, 0, 2, true, false, true);
    AGREEMENT("omit_immutable_wide_container_verbatim", BYTES(immutable_wide), BYTES(immutable_wide_only), 0, 0, 2, true, false, false);
    const uint8_t priority[] = {0x0e, 42};
    const uint8_t priority_optional[] = {0x0e, 42, 0x6a, 1};
    const uint8_t both_optional[] = {6, 9, 8, 42, 0x14, 2, 0x56, 1};
    const uint8_t both[] = {0x0e, 42, 0x14, 2};
    const uint8_t default_priority_desc[] = {0x0e, 0x80, 0x80, 0x14, 2};
    const uint8_t default_priority[] = {0x0e, 0x80, 0x80};
    const uint8_t priority_zero[] = {0x0e, 0};
    const uint8_t priority_max[] = {0x0e, 0x80, 0xff};
    const uint8_t immutable_priority[] = {6, 9, 5, 0x80, 6, 0x80, 0x0e, 0x80, 42,
        0x6a, 1, 0x6d, 3};
    const uint8_t immutable_priority_only[] = {0x0b, 0x80, 6, 0x80, 0x0e, 0x80, 42,
        0x6a, 1};
    const uint8_t immutable_both[] = {6, 9, 5, 0x80, 8, 0x80, 0x0e, 0x80, 42,
        0x14, 2, 0x56, 1, 0x6d, 3};
    const uint8_t immutable_both_only[] = {0x0b, 0x80, 8, 0x80, 0x0e, 0x80, 42,
        0x14, 2, 0x56, 1};
    const uint8_t immutable_default_desc[] = {0x0b, 7, 0x0e, 0x80, 0x80, 0x14, 2, 0x56, 1};
#define PRIORITY_AGREEMENT(name, source, expected, parent, effective, later, short_out, priority_value) \
    failures += record(name, fill_order_property_agreement(source, expected, parent, \
        0, effective, true, later, short_out, priority_value), &failed_cases)
    PRIORITY_AGREEMENT("omit_priority_only", BYTES(priority_optional), BYTES(priority), 0, 1, false, false, 42);
    PRIORITY_AGREEMENT("omit_priority_and_order", BYTES(both_optional), BYTES(both), 0, 2, false, false, 42);
    PRIORITY_AGREEMENT("omit_priority_and_order_later", BYTES(both_optional), BYTES(both), 0, 2, true, false, 42);
    PRIORITY_AGREEMENT("omit_priority_explicit_parent_asc", BYTES(both_optional), BYTES(priority), 1, 1, false, false, 42);
    PRIORITY_AGREEMENT("omit_priority_explicit_parent_desc", BYTES(both_optional), BYTES(priority), 2, 2, false, false, 42);
    PRIORITY_AGREEMENT("omit_priority128_keep_order", BYTES(default_priority_desc), BYTES(desc), 0, 2, false, false, 128);
    PRIORITY_AGREEMENT("omit_priority128_only", BYTES(default_priority), no_properties, 0, 1, false, false, 128);
    PRIORITY_AGREEMENT("omit_priority_zero", BYTES(priority_zero), BYTES(priority_zero), 0, 1, false, false, 0);
    PRIORITY_AGREEMENT("omit_priority255", BYTES(priority_max), BYTES(priority_max), 0, 1, false, false, 255);
    PRIORITY_AGREEMENT("omit_priority_output_refusal_retry", BYTES(priority_optional), BYTES(priority), 0, 1, false, true, 42);
    PRIORITY_AGREEMENT("omit_immutable_priority_only", BYTES(immutable_priority), BYTES(immutable_priority_only), 0, 1, false, false, 42);
    PRIORITY_AGREEMENT("omit_immutable_priority_and_order", BYTES(immutable_both), BYTES(immutable_both_only), 0, 2, false, false, 42);
    PRIORITY_AGREEMENT("omit_immutable_priority_and_order_later", BYTES(immutable_both), BYTES(immutable_both_only), 0, 2, true, false, 42);
    PRIORITY_AGREEMENT("omit_immutable_priority_output_refusal_retry", BYTES(immutable_both), BYTES(immutable_both_only), 0, 2, false, true, 42);
    PRIORITY_AGREEMENT("immutable_priority128_stays_inside_required_container", BYTES(immutable_default_desc), BYTES(immutable_default_desc), 0, 2, false, false, 128);
    failures += record("d22_invalid_priority256_before_advance",
        invalid_accept_default_priority(MOQ_VERSION_DRAFT_22, false, false), &failed_cases);
    failures += record("d22_omission_cannot_hide_priority256",
        invalid_accept_default_priority(MOQ_VERSION_DRAFT_22, true, false), &failed_cases);
    failures += record("d22_omission_cannot_hide_immutable_priority256",
        invalid_accept_default_priority(MOQ_VERSION_DRAFT_22, true, true), &failed_cases);
    failures += record("d18_local_priority256_conformance_refusal",
        invalid_accept_default_priority(MOQ_VERSION_DRAFT_18, false, false), &failed_cases);
#undef PRIORITY_AGREEMENT
#undef BYTES
#undef AGREEMENT
    fprintf(stderr, "descending_fetch_fill: %d failed checks in %d failed cases\n",
            failures, failed_cases);
    return failures ? 1 : 0;
}
