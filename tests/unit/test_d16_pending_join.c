/*
 * Draft-16 section 9.16.2 permits a Joining FETCH against a Pending
 * SUBSCRIBE. Its range uses the single Largest saved at acceptance, not
 * the absence of a snapshot while the application is deciding.
 *
 * Raw, normally encoded peer requests are intentional: the outbound public
 * FETCH API requires an established Largest and cannot originate this order.
 * All mutations use public APIs; private session fields are read-only checks.
 * --controls exercises accepted parents without requiring pending deferral.
 * --coalesced and --split isolate the two pending ingress schedules.
 * --p2 isolates small-buffer rejection and transient send-byte pressure.
 * --admission checks that held joins leave a bare parent-rejection escape.
 */
#include <moq/codec.h>
#include "test_support.h"
#include "../support/failpoint.h"
#include "../../core/src/session/session_internal.h" /* read-only assertions */

static int failures;

/* Prerequisite failures stop the current fixture before using invalid output. */
#define REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        ++failures; return false; \
    } \
} while (0)
#define NEED(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        ++failures; goto done; \
    } \
} while (0)

enum { MAX_OUTPUTS = 32 };
static moq_bytes_t ns_parts[] = { { (const uint8_t *)"live", 4 } };
static const moq_namespace_t track_ns = { ns_parts, 1 };
static const uint8_t catalog[] = "{\"tracks\":[]}";

/* Resolve by reading the handle and slot only; do not call hidden helpers. */
static int sub_slot(const moq_session_t *s, moq_subscription_t h)
{
    uint32_t slot = moq_handle_slot(h._opaque);
    if (moq_handle_pool_tag(h._opaque) != MOQ_HANDLE_POOL_SUBSCRIPTION ||
        moq_handle_session_tag(h._opaque) != s->session_tag || slot >= s->sub_cap ||
        moq_handle_generation(h._opaque) != s->subs[slot].generation ||
        s->subs[slot].state == MOQ_SUB_FREE) return -1;
    return (int)slot;
}

static moq_result_t decode_error_action(const moq_action_t *a,
                                        moq_d16_request_error_t *out)
{
    if (a->kind != MOQ_ACTION_SEND_CONTROL) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    moq_buf_reader_init(&r, a->u.send_control.data, a->u.send_control.len);
    moq_result_t rc = moq_control_decode_envelope(&r, &env);
    if (rc != MOQ_OK) return rc;
    if (env.msg_type != MOQ_D16_REQUEST_ERROR) return MOQ_ERR_INVAL;
    return moq_d16_decode_request_error(env.payload, env.payload_len, out);
}

typedef struct fixture {
    fp_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_session_t *client;
    moq_session_t *server;
    moq_rcbuf_t *retained;
} fixture_t;

static size_t poll_event(moq_session_t *s, moq_event_t *e)
{
    size_t n = 0;
    MOQ_TEST_CHECK_EQ_INT(moq_session_poll_events_ex(s, e, 1, sizeof(*e), &n), MOQ_OK);
    return n;
}

static size_t poll_action(moq_session_t *s, moq_action_t *a)
{
    size_t n = 0;
    MOQ_TEST_CHECK_EQ_INT(moq_session_poll_actions_ex(s, a, 1, sizeof(*a), &n), MOQ_OK);
    return n;
}

static bool no_events(moq_session_t *s)
{
    moq_event_t e;
    size_t n = poll_event(s, &e);
    if (n) moq_event_cleanup(&e);
    REQUIRE(n == 0);
    return true;
}

static bool no_actions(moq_session_t *s)
{
    moq_action_t a;
    size_t n = poll_action(s, &a);
    if (n) {
        moq_d16_request_error_t err;
        if (decode_error_action(&a, &err) == MOQ_OK)
            fprintf(stderr, "unexpected REQUEST_ERROR: request=%llu code=0x%llx\n",
                (unsigned long long)err.request_id, (unsigned long long)err.error_code);
        moq_action_cleanup(&a);
    }
    REQUIRE(n == 0);
    return true;
}

/* Only setup is pumped between real sessions. Subsequent requests belong to
 * the encoded peer, so responses must be decoded here rather than delivered
 * to a client that has never registered those request IDs. */
static bool pump_setup(moq_session_t *from, moq_session_t *to)
{
    for (size_t i = 0; i < MAX_OUTPUTS; ++i) {
        moq_action_t a;
        if (!poll_action(from, &a)) return true;
        bool control = a.kind == MOQ_ACTION_SEND_CONTROL;
        moq_result_t rc = control ? moq_session_on_control_bytes(to,
            a.u.send_control.data, a.u.send_control.len, 0) : MOQ_ERR_INVAL;
        moq_action_cleanup(&a);
        REQUIRE(control && rc == MOQ_OK);
    }
    REQUIRE(false); /* finite output bound */
}

static bool setup_limits(fixture_t *f, uint32_t subs, uint32_t fetches,
                         uint32_t actions, uint32_t events, bool content,
                         uint32_t scratch_bytes, uint32_t send_bytes)
{
    /* Track every pointer and allocation size. These ownership checks do not
     * need an allocation-attempt log or depend on allocation ordinals. */
    f->memory.log_from = UINT64_MAX;
    f->alloc = fp_allocator(&f->memory);
    moq_session_cfg_t c = MOQ_SESSION_CFG_INIT;
    c.alloc = &f->alloc;
    c.version = MOQ_VERSION_DRAFT_16;
    c.perspective = MOQ_PERSPECTIVE_CLIENT;
    c.send_request_capacity = true;
    c.initial_request_capacity = 64;
    c.max_subscriptions = subs;
    c.max_fetches = fetches;
    c.max_actions = actions;
    c.max_events = events;
    c.send_auth_token_cache_size = true;
    c.auth_token_cache_size = 256;
    moq_session_cfg_t s = c;
    s.perspective = MOQ_PERSPECTIVE_SERVER;
    s.output_scratch_size = scratch_bytes;
    s.send_buffer_size = send_bytes;
    REQUIRE(moq_session_create(&c, 0, &f->client) == MOQ_OK);
    REQUIRE(moq_session_create(&s, 0, &f->server) == MOQ_OK);
    REQUIRE(moq_session_start(f->client, 0) == MOQ_OK);
    REQUIRE(pump_setup(f->client, f->server));
    REQUIRE(pump_setup(f->server, f->client));
    REQUIRE(moq_session_state(f->client) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(f->server) == MOQ_SESS_ESTABLISHED);
    moq_event_t e;
    moq_session_t *peers[] = { f->client, f->server };
    for (size_t i = 0; i < 2; ++i) {
        REQUIRE(poll_event(peers[i], &e) == 1);
        bool complete = e.kind == MOQ_EVENT_SETUP_COMPLETE;
        moq_event_cleanup(&e);
        REQUIRE(complete);
        REQUIRE(no_events(peers[i]) && no_actions(peers[i]));
    }
    if (content) {
        REQUIRE(moq_rcbuf_create(&f->alloc, catalog, sizeof(catalog) - 1,
                                 &f->retained) == MOQ_OK);
        REQUIRE(moq_session_note_object_published(f->server, &track_ns,
                    MOQ_BYTES_LITERAL("catalog"), 0, 0) == MOQ_OK);
    }
    return true;
}

static bool setup(fixture_t *f, uint32_t subs, uint32_t fetches,
                  uint32_t actions, uint32_t events, bool content)
{
    return setup_limits(f, subs, fetches, actions, events, content, 0, 0);
}

static void teardown(fixture_t *f)
{
    moq_rcbuf_decref(f->retained);
    moq_session_destroy(f->client);
    moq_session_destroy(f->server);
    failures += fp_sticky_clean(&f->memory, "d16 pending join");
    MOQ_TEST_CHECK_EQ_INT((int)f->memory.balance, 0);
    MOQ_TEST_CHECK_EQ_SIZE(f->memory.table_len, 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)f->memory.live_bytes, 0);
}

static size_t encode_subscribe(uint8_t *wire, size_t cap, uint64_t id,
                               const char *name)
{
    uint8_t filter[16], forward[] = { 1 };
    size_t filter_len = 0;
    moq_d16_subscription_filter_t sf = {
        .filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
    };
    if (moq_d16_encode_subscription_filter(filter, sizeof(filter),
                                           &filter_len, &sf) != MOQ_OK) return 0;
    moq_kvp_entry_t params[] = {
        { .type = MOQ_MSG_PARAM_FORWARD,
          .value = forward, .value_len = sizeof(forward), .is_varint = true },
        { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
          .value = filter, .value_len = filter_len, .is_varint = false },
    };
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, cap);
    if (moq_d16_encode_subscribe(&w, id, &track_ns, moq_bytes_cstr(name),
                                 params, 2) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

static size_t encode_join_tokens(uint8_t *wire, size_t cap, uint64_t id,
    uint64_t parent, uint64_t start, uint32_t type,
    const moq_d16_auth_token_t *tokens, size_t token_count)
{
    if (token_count > 2) return 0;
    moq_d16_fetch_t fetch = {
        .request_id = id,
        .fetch_type = type,
        .joining_request_id = parent,
        .joining_start = start,
    };
    uint8_t token_wire[2][64];
    moq_kvp_entry_t params[4] = {0};
    for (size_t i = 0; i < token_count; ++i) {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, token_wire[i], sizeof(token_wire[i]));
        if (moq_d16_auth_token_encode(&tw, &tokens[i]) != MOQ_OK) return 0;
        params[i] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = token_wire[i], .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false,
        };
    }
    uint8_t priority[] = { 17 }, order[] = { MOQ_GROUP_ORDER_ASCENDING };
    params[token_count] = (moq_kvp_entry_t){
        .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
        .value = priority, .value_len = 1, .is_varint = true,
    };
    params[token_count + 1] = (moq_kvp_entry_t){
        .type = MOQ_MSG_PARAM_GROUP_ORDER,
        .value = order, .value_len = 1, .is_varint = true,
    };
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, cap);
    if (moq_d16_encode_fetch(&w, &fetch, params, token_count + 2) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

static size_t encode_join(uint8_t *wire, size_t cap, uint64_t id,
                          uint64_t parent, uint64_t start, bool token)
{
    moq_d16_auth_token_t auth = {
        .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
        .token_type = 7,
        .token_value = (const uint8_t *)"jointok",
        .token_value_len = 7,
    };
    return encode_join_tokens(wire, cap, id, parent, start,
        MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, token ? &auth : NULL, token ? 1 : 0);
}

static bool feed(moq_session_t *s, const uint8_t *wire, size_t len)
{
    REQUIRE(len > 0);
    REQUIRE(moq_session_on_control_bytes(s, wire, len, 1) == MOQ_OK);
    REQUIRE(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool subscribe(moq_session_t *s, uint64_t id, const char *name)
{
    uint8_t wire[256];
    size_t len = encode_subscribe(wire, sizeof(wire), id, name);
    return feed(s, wire, len);
}

static bool join(moq_session_t *s, uint64_t id, uint64_t parent,
                  uint64_t start, bool token)
{
    uint8_t wire[256];
    size_t len = encode_join(wire, sizeof(wire), id, parent, start, token);
    bool ok = feed(s, wire, len);
    /* The core must not retain the caller's wire/token storage. */
    memset(wire, 0xA5, sizeof(wire));
    return ok;
}

static moq_result_t cancel_message(moq_session_t *s, uint64_t type, uint64_t id)
{
    uint8_t wire[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    moq_result_t rc = moq_d16_encode_varint_msg(&w, type, id);
    if (rc != MOQ_OK) return rc;
    return moq_session_on_control_bytes(s, wire, moq_buf_writer_offset(&w), 2);
}

static bool take_subscribe(moq_session_t *s, moq_subscription_t *h)
{
    moq_event_t e;
    REQUIRE(poll_event(s, &e) == 1);
    bool ok = e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST;
    if (ok) {
        *h = e.u.subscribe_request.sub;
        MOQ_TEST_CHECK(e.u.subscribe_request.filter == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
        MOQ_TEST_CHECK(e.u.subscribe_request.forward);
    }
    moq_event_cleanup(&e);
    REQUIRE(ok && moq_subscription_is_valid(*h));
    return true;
}

static size_t fetch_count(moq_session_t *s, bool pending_only)
{
    size_t n = 0;
    for (size_t i = 0; i < s->fetch_cap; ++i)
        if (pending_only ? s->fetches[i].state == MOQ_FETCH_PENDING_JOIN
                         : s->fetches[i].state != MOQ_FETCH_FREE) ++n;
    return n;
}

static bool pending(moq_session_t *s, moq_subscription_t h, size_t joins)
{
    int slot = sub_slot(s, h);
    REQUIRE(slot >= 0);
    REQUIRE(s->subs[slot].state == MOQ_SUB_PENDING_PUBLISHER);
    REQUIRE(!s->subs[slot].has_largest);
    REQUIRE(no_events(s));
    REQUIRE(no_actions(s)); /* includes no premature INVALID_RANGE */
    REQUIRE(fetch_count(s, true) == joins);
    return true;
}

static moq_result_t accept(moq_session_t *s, moq_subscription_t h,
                            bool largest, uint64_t g, uint64_t o)
{
    moq_accept_subscribe_cfg_t cfg;
    moq_accept_subscribe_cfg_init(&cfg);
    cfg.has_largest = largest;
    cfg.largest_group = g;
    cfg.largest_object = o;
    return moq_session_accept_subscribe(s, h, &cfg, 3);
}

/* Strict control inspection: no unrecognized actions, no extra envelopes,
 * and errors are correlated by their exact request ID, not just their code. */
typedef struct response {
    uint64_t type, id, code, retry;
    bool largest;
    uint64_t group, object;
} response_t;

static bool responses(moq_session_t *s, response_t *out, size_t expected)
{
    for (size_t i = 0; i < expected; ++i) {
        moq_action_t a;
        REQUIRE(poll_action(s, &a) == 1);
        bool ok = a.kind == MOQ_ACTION_SEND_CONTROL;
        moq_buf_reader_t r;
        moq_control_envelope_t env;
        if (ok) {
            moq_buf_reader_init(&r, a.u.send_control.data, a.u.send_control.len);
            ok = moq_control_decode_envelope(&r, &env) == MOQ_OK &&
                 moq_buf_reader_remaining(&r) == 0;
        }
        response_t v = {0};
        if (ok) {
            v.type = env.msg_type;
            if (v.type == MOQ_D16_REQUEST_ERROR) {
                moq_d16_request_error_t e;
                ok = moq_d16_decode_request_error(env.payload, env.payload_len, &e) == MOQ_OK;
                if (ok) {
                    v.id = e.request_id;
                    v.code = e.error_code;
                    v.retry = e.retry_interval;
                }
            } else if (v.type == MOQ_D16_SUBSCRIBE_OK) {
                moq_kvp_entry_t p[16];
                moq_d16_subscribe_ok_t sub = { .params = p, .params_cap = 16 };
                ok = moq_d16_decode_subscribe_ok(env.payload, env.payload_len, &sub) == MOQ_OK;
                if (ok) {
                    v.id = sub.request_id;
                    for (size_t j = 0; j < sub.params_count; ++j) {
                        if (p[j].type != MOQ_MSG_PARAM_LARGEST_OBJECT) continue;
                        MOQ_TEST_CHECK(!v.largest);
                        v.largest = true;
                        moq_buf_reader_t lr;
                        moq_buf_reader_init(&lr, p[j].value, p[j].value_len);
                        ok = moq_buf_read_varint(&lr, &v.group) == MOQ_OK &&
                             moq_buf_read_varint(&lr, &v.object) == MOQ_OK &&
                             moq_buf_reader_remaining(&lr) == 0;
                    }
                }
            } else ok = false;
        }
        moq_action_cleanup(&a);
        REQUIRE(ok);
        out[i] = v;
    }
    REQUIRE(no_actions(s));
    return true;
}

static bool accepted_response(moq_session_t *s, uint64_t id, bool largest,
                               uint64_t g, uint64_t o)
{
    response_t r;
    REQUIRE(responses(s, &r, 1));
    REQUIRE(r.type == MOQ_D16_SUBSCRIBE_OK && r.id == id && r.largest == largest);
    REQUIRE(!largest || (r.group == g && r.object == o));
    return true;
}

static bool error_response(moq_session_t *s, uint64_t id, uint64_t code)
{
    response_t r;
    REQUIRE(responses(s, &r, 1));
    REQUIRE(r.type == MOQ_D16_REQUEST_ERROR && r.id == id && r.code == code);
    return true;
}

/* Occupy action slots using ordinary public operations. Each filler
 * SUBSCRIBE is rejected immediately; its response stays queued. This
 * exercises the same production library in static and shared builds. */
static bool fill_actions(moq_session_t *s, uint64_t first, size_t count)
{
    REQUIRE(count <= MAX_OUTPUTS);
    for (size_t i = 0; i < count; ++i) {
        moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        REQUIRE(subscribe(s, first + 2 * i, "action-occupant"));
        REQUIRE(take_subscribe(s, &h) && no_events(s));
        moq_reject_subscribe_cfg_t cfg;
        moq_reject_subscribe_cfg_init(&cfg);
        cfg.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        REQUIRE(moq_session_reject_subscribe(s, h, &cfg, 2) == MOQ_OK);
    }
    return true;
}

static bool drain_fillers(moq_session_t *s, uint64_t first, size_t count)
{
    response_t r[MAX_OUTPUTS];
    REQUIRE(count <= MAX_OUTPUTS);
    REQUIRE(responses(s, r, count));
    for (size_t i = 0; i < count; ++i) {
        REQUIRE(r[i].type == MOQ_D16_REQUEST_ERROR && r[i].id == first + 2 * i &&
                r[i].code == MOQ_REQUEST_ERROR_NOT_SUPPORTED);
    }
    return true;
}

static bool take_fetch(moq_session_t *s, moq_subscription_t parent,
                        uint64_t start, uint64_t g, uint64_t o, bool token,
                        moq_fetch_t *out)
{
    moq_event_t e;
    REQUIRE(poll_event(s, &e) == 1);
    bool ok = e.kind == MOQ_EVENT_FETCH_REQUEST;
    if (ok) {
        const moq_fetch_request_event_t *r = &e.u.fetch_request;
        *out = r->fetch;
        MOQ_TEST_CHECK(moq_subscription_eq(r->joining_sub, parent));
        MOQ_TEST_CHECK(!moq_publication_is_valid(r->joining_pub));
        MOQ_TEST_CHECK_EQ_U64(r->start_group, start);
        MOQ_TEST_CHECK_EQ_U64(r->start_object, 0);
        MOQ_TEST_CHECK_EQ_U64(r->end_group, g);
        MOQ_TEST_CHECK_EQ_U64(r->end_object, o);
        MOQ_TEST_CHECK_EQ_INT(r->subscriber_priority, 17);
        MOQ_TEST_CHECK_EQ_INT(r->group_order, MOQ_GROUP_ORDER_ASCENDING);
        MOQ_TEST_CHECK_EQ_SIZE(r->track_namespace.count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(r->track_name.len, 0);
        MOQ_TEST_CHECK_EQ_SIZE(r->token_count, token ? 1 : 0);
        if (token && r->token_count == 1 && r->tokens) {
            MOQ_TEST_CHECK_EQ_U64(r->tokens[0].token_type, 7);
            MOQ_TEST_CHECK_EQ_SIZE(r->tokens[0].token_value.len, 7);
            MOQ_TEST_CHECK(r->tokens[0].token_value.len == 7 &&
                r->tokens[0].token_value.data &&
                memcmp(r->tokens[0].token_value.data, "jointok", 7) == 0);
        } else MOQ_TEST_CHECK(!token);
    }
    moq_event_cleanup(&e);
    REQUIRE(ok && moq_fetch_is_valid(*out));
    return true;
}

static bool retry_quiet(moq_session_t *s)
{
    /* Repeated processing must neither release a second event nor resend a
     * terminal response after one successful disposition. */
    for (size_t i = 0; i < 2; ++i) {
        REQUIRE(moq_session_process_pending(s, 4) == MOQ_OK);
        REQUIRE(no_events(s) && no_actions(s));
    }
    REQUIRE(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool deliver_catalog(fixture_t *f, moq_fetch_t h, uint64_t id)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0; ac.end_object = 1;
    REQUIRE(moq_session_accept_fetch(f->server, h, &ac, 4) == MOQ_OK);
    moq_action_t a;
    REQUIRE(poll_action(f->server, &a) == 1);
    bool ok = a.kind == MOQ_ACTION_SEND_CONTROL;
    if (ok) {
        moq_buf_reader_t r;
        moq_control_envelope_t env;
        moq_kvp_entry_t p[16];
        moq_d16_fetch_ok_t fok = { .params = p, .params_cap = 16 };
        moq_buf_reader_init(&r, a.u.send_control.data, a.u.send_control.len);
        ok = moq_control_decode_envelope(&r, &env) == MOQ_OK &&
             env.msg_type == MOQ_D16_FETCH_OK &&
             moq_d16_decode_fetch_ok(env.payload, env.payload_len, &fok) == MOQ_OK &&
             fok.request_id == id && fok.end_group == 0 && fok.end_object == 1 &&
             moq_buf_reader_remaining(&r) == 0;
    }
    moq_action_cleanup(&a);
    REQUIRE(ok);
    REQUIRE(poll_action(f->server, &a) == 1);
    moq_stream_ref_t ref = a.u.send_data.stream_ref;
    ok = a.kind == MOQ_ACTION_SEND_DATA && !a.u.send_data.fin;
    if (ok) {
        moq_buf_reader_t r;
        moq_d16_fetch_header_t hdr;
        moq_buf_reader_init(&r, a.u.send_data.header, a.u.send_data.header_len);
        ok = moq_d16_decode_fetch_header(&r, &hdr) == MOQ_OK &&
             hdr.request_id == id && moq_buf_reader_remaining(&r) == 0;
    }
    moq_action_cleanup(&a);
    REQUIRE(ok && no_actions(f->server));
    moq_fetch_object_cfg_t obj;
    moq_fetch_object_cfg_init(&obj);
    obj.payload = f->retained;
    obj.publisher_priority = 128;
    REQUIRE(moq_session_write_fetch_object(f->server, h, &obj, 5) == MOQ_OK);
    REQUIRE(poll_action(f->server, &a) == 1);
    ok = a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.stream_ref._v == ref._v &&
         !a.u.send_data.fin && a.u.send_data.payload;
    if (ok) {
        uint8_t bytes[256];
        size_t hn = a.u.send_data.header_len;
        size_t pn = moq_rcbuf_len(a.u.send_data.payload);
        ok = hn + pn <= sizeof(bytes);
        if (ok) {
            memcpy(bytes, a.u.send_data.header, hn);
            memcpy(bytes + hn, moq_rcbuf_data(a.u.send_data.payload), pn);
            moq_buf_reader_t r;
            moq_d16_fetch_object_t wire_obj;
            moq_buf_reader_init(&r, bytes, hn + pn);
            ok = moq_d16_decode_fetch_object(&r, NULL, &wire_obj) == MOQ_OK &&
                !wire_obj.is_end_of_range && wire_obj.group_id == 0 &&
                wire_obj.object_id == 0 && wire_obj.payload_len == sizeof(catalog) - 1 &&
                memcmp(wire_obj.payload, catalog, sizeof(catalog) - 1) == 0 &&
                moq_buf_reader_remaining(&r) == 0;
        }
    }
    moq_action_cleanup(&a);
    REQUIRE(ok && no_actions(f->server));
    REQUIRE(moq_session_end_fetch(f->server, h, 6) == MOQ_OK);
    REQUIRE(poll_action(f->server, &a) == 1);
    ok = a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin &&
         a.u.send_data.stream_ref._v == ref._v && a.u.send_data.header_len == 0 &&
         a.u.send_data.payload == NULL;
    moq_action_cleanup(&a);
    REQUIRE(ok && no_actions(f->server));
    REQUIRE(moq_session_end_fetch(f->server, h, 6) == MOQ_ERR_STALE_HANDLE);
    REQUIRE(fetch_count(f->server, false) == 0);
    return retry_quiet(f->server);
}

/* The control accepts before Joining FETCH. Pending variants differ only in
 * delivery order, preserving the same retained object and public accept. */
static void catalog_case(int schedule)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    if (schedule == 1) {
        uint8_t wire[512];
        size_t sn = encode_subscribe(wire, sizeof(wire), 0, "catalog");
        size_t fn = sn ? encode_join(wire + sn, sizeof(wire) - sn, 2, 0, 0, false) : 0;
        NEED(sn && fn);
        NEED(feed(f.server, wire, sn + fn));
        memset(wire, 0xA5, sizeof(wire));
        NEED(take_subscribe(f.server, &h));
    } else {
        NEED(subscribe(f.server, 0, "catalog"));
        NEED(take_subscribe(f.server, &h));
        NEED(no_events(f.server) && no_actions(f.server));
        if (schedule == 2) {
            NEED(join(f.server, 2, 0, 0, false));
        }
    }
    int slot = sub_slot(f.server, h);
    NEED(slot >= 0);
    MOQ_TEST_CHECK(f.server->subs[slot].hist && f.server->subs[slot].hist->has_largest);
    MOQ_TEST_CHECK(f.retained && moq_rcbuf_len(f.retained) == sizeof(catalog) - 1);
    if (schedule) NEED(pending(f.server, h, 1));
    /* No explicit Largest: acceptance must obtain the preexisting {0,0}. */
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 0, 0));
    if (!schedule) NEED(join(f.server, 2, 0, 0, false));
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 0, 0, 1, false, &fh));
    NEED(no_events(f.server));
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, true), 0);
    NEED(deliver_catalog(&f, fh, 2));
done:
    teardown(&f);
}

static void empty_case(bool pending_join)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, 8, false));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "empty") && take_subscribe(f.server, &h));
    if (pending_join) {
        NEED(join(f.server, 2, 0, 0, true));
        NEED(pending(f.server, h, 1));
    }
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_OK);
    if (pending_join) {
        response_t r[2];
        NEED(responses(f.server, r, 2));
        MOQ_TEST_CHECK(r[0].type == MOQ_D16_SUBSCRIBE_OK && r[0].id == 0 && !r[0].largest);
        MOQ_TEST_CHECK(r[1].type == MOQ_D16_REQUEST_ERROR && r[1].id == 2 &&
                       r[1].code == MOQ_REQUEST_ERROR_INVALID_RANGE);
    } else {
        NEED(accepted_response(f.server, 0, false, 0, 0));
        NEED(join(f.server, 2, 0, 0, false));
        NEED(error_response(f.server, 2, MOQ_REQUEST_ERROR_INVALID_RANGE));
    }
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 0);
    NEED(no_events(f.server) && retry_quiet(f.server));
done:
    teardown(&f);
}

static void snapshot_and_tokens(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, 8, true));
    MOQ_TEST_CHECK_EQ_INT(moq_session_note_object_published(f.server, &track_ns,
        MOQ_BYTES_LITERAL("catalog"), 8, 2), MOQ_OK);
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID, churn = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 3, true) && pending(f.server, h, 1));
    /* Reuse the control receive storage before releasing the token. */
    NEED(subscribe(f.server, 4, "overwrite-receive-storage"));
    NEED(take_subscribe(f.server, &churn) && no_events(f.server));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 10, 5), MOQ_OK);
    /* This later history is deliberately greater than the saved Largest.
     * An established subsequent join must use the same acceptance snapshot. */
    MOQ_TEST_CHECK_EQ_INT(moq_session_note_object_published(f.server, &track_ns,
        MOQ_BYTES_LITERAL("catalog"), 99, 9), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 10, 5));
    moq_fetch_t first = MOQ_FETCH_INVALID, second = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 7, 10, 6, true, &first));
    NEED(no_events(f.server) && no_actions(f.server));
    NEED(join(f.server, 6, 0, 3, false));
    NEED(take_fetch(f.server, h, 7, 10, 6, false, &second));
    MOQ_TEST_CHECK(!moq_fetch_eq(first, second));
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static bool two_errors(moq_session_t *s, uint64_t first, uint64_t second)
{
    response_t r[2];
    REQUIRE(responses(s, r, 2));
    REQUIRE(r[0].type == MOQ_D16_REQUEST_ERROR && r[1].type == MOQ_D16_REQUEST_ERROR);
    REQUIRE(r[0].code == MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID &&
            r[1].code == MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
    REQUIRE((r[0].id == first && r[1].id == second) ||
            (r[1].id == first && r[0].id == second));
    return true;
}

static void parent_reject(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, true) && pending(f.server, h, 1));
    moq_reject_subscribe_cfg_t rej;
    moq_reject_subscribe_cfg_init(&rej);
    rej.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    MOQ_TEST_CHECK_EQ_INT(moq_session_reject_subscribe(f.server, h, &rej, 3), MOQ_OK);
    response_t r[2];
    NEED(responses(f.server, r, 2));
    size_t parent = 0, child = 0;
    for (size_t i = 0; i < 2; ++i) {
        MOQ_TEST_CHECK(r[i].type == MOQ_D16_REQUEST_ERROR);
        if (r[i].id == 0 && r[i].code == MOQ_REQUEST_ERROR_NOT_SUPPORTED) ++parent;
        if (r[i].id == 2 && r[i].code == MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID) ++child;
    }
    MOQ_TEST_CHECK_EQ_SIZE(parent, 1);
    MOQ_TEST_CHECK_EQ_SIZE(child, 1);
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 0);
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void fetch_cancel(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 1, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, true) && pending(f.server, h, 1));
    MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_FETCH_CANCEL, 2), MOQ_OK);
    /* No FETCH_REQUEST was delivered, so no FETCH_CANCELLED may be delivered. */
    NEED(pending(f.server, h, 0));
    NEED(retry_quiet(f.server));
    /* A new join must fit the freed single slot and use the same parent. */
    NEED(join(f.server, 4, 0, 0, true) && pending(f.server, h, 1));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 0, 0));
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 0, 0, 1, true, &fh));
    NEED(no_events(f.server) && deliver_catalog(&f, fh, 4));
done:
    teardown(&f);
}

static void reused_parent(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 1, 4, 8, 8, false));
    moq_subscription_t old = MOQ_SUBSCRIPTION_INVALID, h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "old-track") && take_subscribe(f.server, &old));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, old, true, 40, 9), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 40, 9));
    MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_UNSUBSCRIBE, 0), MOQ_OK);
    moq_event_t e;
    NEED(poll_event(f.server, &e) == 1);
    MOQ_TEST_CHECK(e.kind == MOQ_EVENT_UNSUBSCRIBED &&
                   moq_subscription_eq(e.u.unsubscribed.sub, old));
    moq_event_cleanup(&e);
    NEED(no_events(f.server) && no_actions(f.server));
    NEED(subscribe(f.server, 2, "new-track") && take_subscribe(f.server, &h));
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(old._opaque), moq_handle_slot(h._opaque));
    MOQ_TEST_CHECK(!moq_subscription_eq(old, h));
    NEED(join(f.server, 4, 2, 3, true) && pending(f.server, h, 1));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 10, 5), MOQ_OK);
    NEED(accepted_response(f.server, 2, true, 10, 5));
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 7, 10, 6, true, &fh));
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void multiple_and_capacity(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 2, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 3, true));
    NEED(join(f.server, 4, 0, 20, false) && pending(f.server, h, 2));
    NEED(join(f.server, 6, 0, 0, true));
    /* Draft-16 has no EXCESSIVE_LOAD: the existing capacity policy is a
     * non-retryable INTERNAL_ERROR, never INVALID_RANGE or a session close. */
    response_t refusal;
    NEED(responses(f.server, &refusal, 1));
    MOQ_TEST_CHECK(refusal.type == MOQ_D16_REQUEST_ERROR && refusal.id == 6 &&
                   refusal.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR && refusal.retry == 0);
    NEED(pending(f.server, h, 2));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 10, 5), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 10, 5));
    moq_fetch_t handles[2] = { MOQ_FETCH_INVALID, MOQ_FETCH_INVALID };
    size_t seen_token = 0;
    /* The pool walk's ordering is private; correlate handles back to IDs. */
    for (size_t i = 0; i < 2; ++i) {
        moq_event_t e;
        NEED(poll_event(f.server, &e) == 1);
        bool ok = e.kind == MOQ_EVENT_FETCH_REQUEST;
        if (ok) {
            moq_fetch_t fh = e.u.fetch_request.fetch;
            uint32_t slot = moq_handle_slot(fh._opaque);
            ok = slot < f.server->fetch_cap;
            uint64_t id = ok ? f.server->fetches[slot].request_id : UINT64_MAX;
            ok = ok && (id == 2 || id == 4);
            if (ok) {
                size_t index = id == 2 ? 0 : 1;
                MOQ_TEST_CHECK(!moq_fetch_is_valid(handles[index]));
                handles[index] = fh;
            }
            MOQ_TEST_CHECK(moq_subscription_eq(e.u.fetch_request.joining_sub, h));
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_request.start_group, id == 2 ? 7 : 0);
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_request.start_object, 0);
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_request.end_group, 10);
            MOQ_TEST_CHECK_EQ_U64(e.u.fetch_request.end_object, 6);
            seen_token += e.u.fetch_request.token_count;
        }
        moq_event_cleanup(&e);
        NEED(ok);
    }
    MOQ_TEST_CHECK_EQ_SIZE(seen_token, 1);
    MOQ_TEST_CHECK(!moq_fetch_eq(handles[0], handles[1]));
    NEED(no_events(f.server));
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 2);
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, true), 0);
    NEED(retry_quiet(f.server));
    /* Cancelling now is observable, since both handles have been surfaced. */
    MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_FETCH_CANCEL, 2), MOQ_OK);
    moq_event_t e;
    NEED(poll_event(f.server, &e) == 1);
    MOQ_TEST_CHECK(e.kind == MOQ_EVENT_FETCH_CANCELLED);
    MOQ_TEST_CHECK(moq_fetch_eq(e.u.fetch_cancelled.fetch, handles[0]));
    moq_event_cleanup(&e);
    NEED(join(f.server, 8, 0, 1, false));
    moq_fetch_t next = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 9, 10, 6, false, &next));
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void accept_pressure(bool event_pressure)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, event_pressure ? 1 : 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, true) && pending(f.server, h, 1));
    if (event_pressure) {
        NEED(subscribe(f.server, 4, "event-occupant"));
    } else {
        NEED(fill_actions(f.server, 4, f.server->action_cap));
    }
    int slot = sub_slot(f.server, h);
    NEED(slot >= 0);
    int64_t blocks = f.memory.balance;
    size_t send_len = f.server->send_len;
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(f.server->subs[slot].state, MOQ_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK(!f.server->subs[slot].has_largest);
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, true), 1);
    MOQ_TEST_CHECK_EQ_SIZE(f.server->send_len, send_len);
    MOQ_TEST_CHECK(f.memory.balance == blocks);
    if (event_pressure) {
        NEED(no_actions(f.server));
        moq_subscription_t other = MOQ_SUBSCRIPTION_INVALID;
        NEED(take_subscribe(f.server, &other) && no_events(f.server));
    } else {
        NEED(drain_fillers(f.server, 4, f.server->action_cap) && no_events(f.server));
    }
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 0, 0));
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 0, 0, 1, true, &fh));
    NEED(no_events(f.server) && deliver_catalog(&f, fh, 2));
done:
    teardown(&f);
}

/* pressure: 0 = no pressure, 1 = action queue, 2 = event queue. */
static void unsubscribe_case(int pressure)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, pressure == 2 ? 1 : 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, true));
    NEED(join(f.server, 4, 0, 0, false) && pending(f.server, h, 2));
    if (pressure == 1) {
        /* Two terminal errors must be reserved together. Leave just one
         * free action slot to detect partial response/child retirement. */
        NEED(fill_actions(f.server, 6, f.server->action_cap - 1));
    } else if (pressure == 2) {
        NEED(subscribe(f.server, 6, "event-occupant"));
    }
    MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_UNSUBSCRIBE, 0),
                          pressure ? MOQ_ERR_WOULD_BLOCK : MOQ_OK);
    if (pressure) {
        MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, true), 2);
        int slot = sub_slot(f.server, h);
        NEED(slot >= 0);
        MOQ_TEST_CHECK_EQ_INT(f.server->subs[slot].state, MOQ_SUB_PENDING_PUBLISHER);
        if (pressure == 1) {
            NEED(no_events(f.server));
            NEED(drain_fillers(f.server, 6, f.server->action_cap - 1));
        } else {
            moq_subscription_t other = MOQ_SUBSCRIPTION_INVALID;
            NEED(take_subscribe(f.server, &other) && no_events(f.server));
        }
        NEED(no_actions(f.server));
        /* Input is already buffered: retry processing, never re-deliver. */
        MOQ_TEST_CHECK_EQ_INT(moq_session_process_pending(f.server, 3), MOQ_OK);
    }
    NEED(two_errors(f.server, 2, 4));
    moq_event_t e;
    NEED(poll_event(f.server, &e) == 1);
    MOQ_TEST_CHECK(e.kind == MOQ_EVENT_UNSUBSCRIBED &&
                   moq_subscription_eq(e.u.unsubscribed.sub, h));
    moq_event_cleanup(&e);
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 0);
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_ERR_STALE_HANDLE);
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void pending_teardown(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 4, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, true) && pending(f.server, h, 1));
done:
    teardown(&f); /* held token, request registry and retained payload */
}

static moq_d16_auth_token_t register_token(const char *value)
{
    moq_d16_auth_token_t token = {
        .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 5, .token_type = 7,
        .token_value = (const uint8_t *)value, .token_value_len = strlen(value),
    };
    return token;
}

static bool cache_value(const moq_session_t *s, const char *value)
{
    const moq_token_cache_t *cache = &s->peer_token_cache;
    size_t active = 0;
    for (size_t i = 0; i < cache->cap; ++i) {
        const moq_token_cache_entry_t *e = &cache->entries[i];
        if (!e->active) continue;
        ++active;
        REQUIRE(value && e->alias == 5 && e->token_type == 7);
        REQUIRE(e->token_value_len == strlen(value));
        REQUIRE(e->token_value && memcmp(e->token_value, value, strlen(value)) == 0);
    }
    REQUIRE(active == (value ? 1u : 0u));
    REQUIRE(cache->used_bytes == (value ? MOQ_TOKEN_ENTRY_OVERHEAD + strlen(value) : 0));
    return true;
}

static bool registered_id(const moq_session_t *s, uint64_t id)
{
    for (size_t i = 0; i <= s->idx_req_mask; ++i)
        if (s->idx_req_by_rid[i].slot >= 0 && s->idx_req_by_rid[i].key == id)
            return true;
    return false;
}

/* Both operations need only stack-encoded control errors on draft-16. A
 * tokenless short-name request fits a 32-byte output arena. Separately, a
 * 32-byte send buffer fits the exact OK + error even though a bidi rejection
 * bound would not fit. No private capacity field is modified by the test. */
static void small_limits_case(bool cancel, bool small_send)
{
    fixture_t f = {0};
    NEED(setup_limits(&f, 2, 1, 8, 8, false, small_send ? 0 : 32, small_send ? 32 : 0));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "t") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, false) && pending(f.server, h, 1));
    NEED(retry_quiet(f.server));
    fp_map_snap_t map;
    fp_map_capture(&f.memory, &map);
    size_t before_send = f.server->send_len;
    if (!small_send) MOQ_TEST_CHECK_EQ_SIZE(f.server->event_scratch_cap, 32);
    size_t exact_send = 0;
    if (small_send) {
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        NEED(moq_d16_encode_subscribe_ok(&w, 0, 1, NULL, 0, NULL, 0) == MOQ_OK);
        NEED(moq_d16_encode_request_error(&w, 2, MOQ_REQUEST_ERROR_INVALID_RANGE,
                                         0, NULL, 0) == MOQ_OK);
        exact_send = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK_EQ_SIZE(f.server->send_cap, 32);
        NEED(exact_send <= f.server->send_cap);
    }
    moq_result_t rc = cancel ? cancel_message(f.server, MOQ_D16_UNSUBSCRIBE, 0)
                            : accept(f.server, h, false, 0, 0);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) {
        /* A refusal must not partially accept/free the parent, consume a
         * child, emit an error, or retain any staging allocation. */
        NEED(pending(f.server, h, 1));
        MOQ_TEST_CHECK_EQ_SIZE(f.server->send_len, before_send);
        failures += fp_map_equals(&f.memory, &map, "small-buffer refusal");
        goto done;
    }
    if (small_send) MOQ_TEST_CHECK_EQ_SIZE(f.server->send_len, exact_send);
    if (cancel) {
        NEED(error_response(f.server, 2, MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID));
        moq_event_t e;
        NEED(poll_event(f.server, &e) == 1);
        MOQ_TEST_CHECK(e.kind == MOQ_EVENT_UNSUBSCRIBED &&
                       moq_subscription_eq(e.u.unsubscribed.sub, h));
        moq_event_cleanup(&e);
        MOQ_TEST_CHECK_EQ_INT(sub_slot(f.server, h), -1);
    } else {
        response_t r[2];
        NEED(responses(f.server, r, 2));
        MOQ_TEST_CHECK(r[0].type == MOQ_D16_SUBSCRIBE_OK && r[0].id == 0 && !r[0].largest);
        MOQ_TEST_CHECK(r[1].type == MOQ_D16_REQUEST_ERROR && r[1].id == 2 &&
                       r[1].code == MOQ_REQUEST_ERROR_INVALID_RANGE && r[1].retry == 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 0);
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void send_byte_pressure(void)
{
    fixture_t f = {0};
    NEED(setup_limits(&f, 2, 1, 8, 8, false, 0, 128));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID, filler = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "t") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, false) && pending(f.server, h, 1));
    NEED(subscribe(f.server, 4, "f") && take_subscribe(f.server, &filler));
    /* One real response occupies all but one send byte, leaving action room. */
    uint8_t reason[128], wire[256];
    memset(reason, 'x', sizeof(reason));
    size_t reason_len = 0;
    for (size_t n = 0; n < sizeof(reason); ++n) {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        NEED(moq_d16_encode_request_error(&w, 4, MOQ_REQUEST_ERROR_NOT_SUPPORTED,
                                          0, reason, n) == MOQ_OK);
        if (moq_buf_writer_offset(&w) == f.server->send_cap - 1) {
            reason_len = n;
            break;
        }
    }
    NEED(reason_len > 0);
    moq_reject_subscribe_cfg_t reject;
    moq_reject_subscribe_cfg_init(&reject);
    reject.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    reject.reason = (moq_bytes_t){ reason, reason_len };
    NEED(moq_session_reject_subscribe(f.server, filler, &reject, 2) == MOQ_OK);
    NEED(f.server->send_cap - f.server->send_len == 1);
    NEED(f.server->action_tail - f.server->action_head + 1 < f.server->action_cap);
    moq_d16_auth_token_t token = register_token("jointok");
    size_t len = encode_join_tokens(wire, sizeof(wire), 6, 0, 0,
        MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, &token, 1);
    NEED(len > 0);
    fp_map_snap_t map;
    fp_map_capture(&f.memory, &map);
    size_t send_len = f.server->send_len;
    moq_result_t rc = moq_session_on_control_bytes(f.server, wire, len, 3);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_session_state(f.server) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_SIZE(f.server->send_len, send_len);
    MOQ_TEST_CHECK_EQ_SIZE(f.server->recv_len, len);
    MOQ_TEST_CHECK(!registered_id(f.server, 6));
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, true), 1);
    NEED(cache_value(f.server, NULL) && no_events(f.server));
    failures += fp_map_equals(&f.memory, &map, "blocked full-pool refusal");
    NEED(drain_fillers(f.server, 4, 1));
    /* The same request ID and REGISTER must still be uncommitted. Retrying
     * retained input would close on an early ID/cache commit. */
    MOQ_TEST_CHECK_EQ_INT(moq_session_process_pending(f.server, 4), MOQ_OK);
    response_t refusal;
    NEED(responses(f.server, &refusal, 1));
    MOQ_TEST_CHECK(refusal.type == MOQ_D16_REQUEST_ERROR && refusal.id == 6 &&
                   refusal.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR && refusal.retry == 0);
    NEED(cache_value(f.server, "jointok") && pending(f.server, h, 1));
    MOQ_TEST_CHECK(!registered_id(f.server, 6));
    MOQ_TEST_CHECK_EQ_SIZE(f.server->recv_len, 0);
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 0, 0), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 0, 0));
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    NEED(take_fetch(f.server, h, 0, 0, 1, false, &fh));
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void absolute_join_case(bool past_end)
{
    fixture_t f = {0};
    NEED(setup(&f, 2, 1, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    uint8_t wire[128];
    size_t len = encode_join_tokens(wire, sizeof(wire), 2, 0, past_end ? 11 : 7,
                                    MOQ_D16_FETCH_TYPE_ABSOLUTE_JOIN, NULL, 0);
    NEED(feed(f.server, wire, len) && pending(f.server, h, 1));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 10, 5), MOQ_OK);
    if (past_end) {
        response_t r[2];
        NEED(responses(f.server, r, 2));
        MOQ_TEST_CHECK(r[0].type == MOQ_D16_SUBSCRIBE_OK && r[0].id == 0 &&
                       r[0].largest && r[0].group == 10 && r[0].object == 5);
        MOQ_TEST_CHECK(r[1].type == MOQ_D16_REQUEST_ERROR && r[1].id == 2 &&
                       r[1].code == MOQ_REQUEST_ERROR_INVALID_RANGE);
        MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 0);
    } else {
        NEED(accepted_response(f.server, 0, true, 10, 5));
        moq_fetch_t fh = MOQ_FETCH_INVALID;
        NEED(take_fetch(f.server, h, 7, 10, 6, false, &fh));
    }
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

static void alias_ownership(void)
{
    fixture_t f = {0};
    NEED(setup(&f, 2, 3, 8, 8, false));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "t") && take_subscribe(f.server, &h));
    moq_d16_auth_token_t reg = register_token("jointok");
    moq_d16_auth_token_t alias = { .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 5 };
    uint8_t wire[256];
    size_t len = encode_join_tokens(wire, sizeof(wire), 2, 0, 0,
        MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, &reg, 1);
    NEED(feed(f.server, wire, len) && pending(f.server, h, 1));
    NEED(cache_value(f.server, "jointok"));
    len = encode_join_tokens(wire, sizeof(wire), 4, 0, 0,
        MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, &alias, 1);
    NEED(feed(f.server, wire, len) && pending(f.server, h, 2));
    moq_d16_auth_token_t change[] = {
        { .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 5 },
        register_token("CHANGED"),
    };
    len = encode_join_tokens(wire, sizeof(wire), 6, 0, 0,
        MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, change, 2);
    NEED(feed(f.server, wire, len) && pending(f.server, h, 3));
    memset(wire, 0xA5, sizeof(wire));
    NEED(cache_value(f.server, "CHANGED"));
    MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_FETCH_CANCEL, 6), MOQ_OK);
    NEED(pending(f.server, h, 2));
    MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 10, 5), MOQ_OK);
    NEED(accepted_response(f.server, 0, true, 10, 5));
    moq_fetch_t handles[2] = { MOQ_FETCH_INVALID, MOQ_FETCH_INVALID };
    for (size_t i = 0; i < 2; ++i)
        NEED(take_fetch(f.server, h, 10, 10, 6, true, &handles[i]));
    MOQ_TEST_CHECK(!moq_fetch_eq(handles[0], handles[1]));
    NEED(cache_value(f.server, "CHANGED") && retry_quiet(f.server));
done:
    teardown(&f);
}

/* REGISTER allocates a cache copy, then a held-join copy. USE_ALIAS allocates
 * a staging copy taken over by the join. Fail each of those three origins in
 * fresh fixtures, asserting rollback before retrying retained control input. */
static void auth_allocation_failure(bool use_alias, uint64_t ordinal)
{
    fixture_t f = {0};
    NEED(setup(&f, 2, 1, 8, 8, false));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "t") && take_subscribe(f.server, &h));
    moq_d16_auth_token_t token = register_token("jointok");
    uint8_t wire[256];
    uint64_t id = use_alias ? 4 : 2;
    if (use_alias) {
        size_t n = encode_join_tokens(wire, sizeof(wire), 2, 0, 0,
            MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, &token, 1);
        NEED(feed(f.server, wire, n) && pending(f.server, h, 1));
        MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_FETCH_CANCEL, 2), MOQ_OK);
        NEED(pending(f.server, h, 0) && cache_value(f.server, "jointok"));
        token = (moq_d16_auth_token_t){ .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 5 };
    }
    NEED(retry_quiet(f.server));
    fp_map_snap_t map;
    fp_map_capture(&f.memory, &map);
    size_t len = encode_join_tokens(wire, sizeof(wire), id, 0, 0,
        MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, &token, 1);
    NEED(len > 0);
    uint64_t before = f.memory.call_count;
    f.memory.log_from = before;
    f.memory.log_len = 0;
    f.memory.fail_at = before + ordinal;
    moq_result_t rc = moq_session_on_control_bytes(f.server, wire, len, 5);
    f.memory.fail_at = 0;
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_U64(f.memory.call_count, before + ordinal);
    MOQ_TEST_CHECK_EQ_SIZE(f.memory.log_len, (size_t)ordinal);
    for (size_t i = 0; i < f.memory.log_len; ++i) {
        MOQ_TEST_CHECK_EQ_INT(f.memory.log[i].kind, FP_ALLOC);
        MOQ_TEST_CHECK_EQ_SIZE(f.memory.log[i].size, 7);
    }
    f.memory.log_from = UINT64_MAX;
    NEED(pending(f.server, h, 0));
    NEED(cache_value(f.server, use_alias ? "jointok" : NULL));
    MOQ_TEST_CHECK(!registered_id(f.server, id));
    MOQ_TEST_CHECK_EQ_SIZE(f.server->recv_len, len);
    failures += fp_map_equals(&f.memory, &map, "failed pending-join token allocation");
    MOQ_TEST_CHECK_EQ_INT(moq_session_process_pending(f.server, 6), MOQ_OK);
    NEED(pending(f.server, h, 1) && cache_value(f.server, "jointok"));
    MOQ_TEST_CHECK(registered_id(f.server, id));
    MOQ_TEST_CHECK_EQ_SIZE(f.server->recv_len, 0);
    /* Leave the recovered REGISTER join for teardown in the second-copy
     * case; release the others to cover both entry cleanup and event ownership. */
    if (ordinal == 1) {
        MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, true, 10, 5), MOQ_OK);
        NEED(accepted_response(f.server, 0, true, 10, 5));
        moq_fetch_t fh = MOQ_FETCH_INVALID;
        NEED(take_fetch(f.server, h, 10, 10, 6, true, &fh));
        NEED(retry_quiet(f.server));
    }
done:
    teardown(&f);
}

static size_t bare_error_size(uint64_t id, uint64_t code)
{
    uint8_t bytes[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, bytes, sizeof(bytes));
    if (moq_d16_encode_request_error(&w, id, code, 0, NULL, 0) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

/* mode: 0 = two total actions, 1 = 48 total send bytes, 2 = 32 scratch bytes.
 * The first two bounds must refuse excess joins while a bare parent rejection
 * remains possible. The last checks the public bare-reject operation itself. */
static void admission_case(int mode, bool empty_accept)
{
    fixture_t f = {0};
    NEED(setup_limits(&f, 3, 16, mode == 0 ? 2 : 16, 8, false,
                      mode == 2 ? 32 : 0, mode == 1 ? 48 : 0));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "t") && take_subscribe(f.server, &h));
    size_t parent_bytes = bare_error_size(0, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
    NEED(parent_bytes > 0);
    size_t children_bytes = 0, held = 0;
    do {
        uint64_t id = 2 + 2 * held;
        size_t bytes = bare_error_size(id, MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
        NEED(bytes > 0 && held < MAX_OUTPUTS - 1);
        if (mode == 1 && parent_bytes + children_bytes + bytes > f.server->send_cap) break;
        NEED(join(f.server, id, 0, 0, false));
        ++held;
        children_bytes += bytes;
        NEED(pending(f.server, h, held));
    } while (mode == 1);
    NEED(held > 0);
    if (mode == 0) {
        MOQ_TEST_CHECK_EQ_SIZE(f.server->action_cap, 2);
        MOQ_TEST_CHECK_EQ_SIZE(held, 1);
    } else if (mode == 1) {
        MOQ_TEST_CHECK_EQ_SIZE(f.server->send_cap, 48);
        NEED(held + 2 <= f.server->action_cap); /* send, not action, bound */
        NEED(parent_bytes + children_bytes <= f.server->send_cap);
        NEED(parent_bytes + children_bytes + bare_error_size(2 + 2 * held,
            MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID) > f.server->send_cap);
    } else MOQ_TEST_CHECK_EQ_SIZE(f.server->event_scratch_cap, 32);

    if (mode != 2) {
        /* The pool has spare slots; the refusal must be due to the complete
         * future rejection batch, never a full fetch pool. */
        NEED(fetch_count(f.server, false) == held && held + 1 < f.server->fetch_cap);
        uint64_t excess = 2 + 2 * held;
        bool blocked = mode == 0 && !empty_accept;
        if (blocked) {
            NEED(fill_actions(f.server, excess, 2));
            excess += 4;
        }
        moq_d16_auth_token_t token = register_token("jointok");
        uint8_t wire[128];
        size_t len = encode_join_tokens(wire, sizeof(wire), excess, 0, 0,
            MOQ_D16_FETCH_TYPE_RELATIVE_JOIN, &token, 1);
        NEED(len > 0);
        fp_map_snap_t map;
        fp_map_capture(&f.memory, &map);
        size_t before_send = f.server->send_len;
        moq_result_t rc = moq_session_on_control_bytes(f.server, wire, len, 3);
        MOQ_TEST_CHECK_EQ_INT(rc, blocked ? MOQ_ERR_WOULD_BLOCK : MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(f.server) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), held);
        MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, true), held);
        MOQ_TEST_CHECK(!registered_id(f.server, excess));
        NEED(no_events(f.server));
        if (blocked) {
            NEED(cache_value(f.server, NULL));
            MOQ_TEST_CHECK_EQ_SIZE(f.server->recv_len, len);
            MOQ_TEST_CHECK_EQ_SIZE(f.server->send_len, before_send);
            failures += fp_map_equals(&f.memory, &map, "blocked admission refusal");
            NEED(drain_fillers(f.server, excess - 4, 2));
            /* Success on this same buffered ID and REGISTER proves that
             * neither was committed before the refusal could be queued. */
            MOQ_TEST_CHECK_EQ_INT(moq_session_process_pending(f.server, 4), MOQ_OK);
        }
        response_t refusal;
        NEED(responses(f.server, &refusal, 1));
        MOQ_TEST_CHECK(refusal.type == MOQ_D16_REQUEST_ERROR && refusal.id == excess &&
                       refusal.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR && refusal.retry == 0);
        NEED(cache_value(f.server, "jointok") && pending(f.server, h, held));
        MOQ_TEST_CHECK(!registered_id(f.server, excess));
        MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), held);
        MOQ_TEST_CHECK_EQ_SIZE(f.server->recv_len, 0);
    }

    if (empty_accept) {
        MOQ_TEST_CHECK_EQ_INT(accept(f.server, h, false, 0, 0), MOQ_OK);
    } else {
        moq_reject_subscribe_cfg_t reject;
        moq_reject_subscribe_cfg_init(&reject);
        reject.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT(moq_session_reject_subscribe(f.server, h, &reject, 5), MOQ_OK);
        /* Exact control bytes, with no reason or redirect, fit even where a
         * conservative request-bidi bound or scratch probe would not. */
        MOQ_TEST_CHECK_EQ_SIZE(f.server->send_len, parent_bytes + children_bytes);
    }
    response_t out[MAX_OUTPUTS];
    NEED(responses(f.server, out, held + 1));
    size_t parent_seen = 0;
    bool child_seen[MAX_OUTPUTS] = {false};
    for (size_t i = 0; i < held + 1; ++i) {
        if (out[i].id == 0) {
            ++parent_seen;
            MOQ_TEST_CHECK(out[i].type == (empty_accept ? MOQ_D16_SUBSCRIBE_OK : MOQ_D16_REQUEST_ERROR));
            if (empty_accept) MOQ_TEST_CHECK(!out[i].largest);
            else MOQ_TEST_CHECK(out[i].code == MOQ_REQUEST_ERROR_NOT_SUPPORTED && out[i].retry == 0);
        } else {
            uint64_t id = out[i].id;
            bool valid = id >= 2 && id % 2 == 0 && (id - 2) / 2 < held;
            MOQ_TEST_CHECK(valid);
            if (valid) {
                size_t child = (size_t)((id - 2) / 2);
                MOQ_TEST_CHECK(!child_seen[child]);
                child_seen[child] = true;
            }
            MOQ_TEST_CHECK(out[i].type == MOQ_D16_REQUEST_ERROR && out[i].retry == 0 &&
                out[i].code == (empty_accept ? MOQ_REQUEST_ERROR_INVALID_RANGE
                                            : MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID));
        }
    }
    MOQ_TEST_CHECK_EQ_SIZE(parent_seen, 1);
    for (size_t i = 0; i < held; ++i) MOQ_TEST_CHECK(child_seen[i]);
    MOQ_TEST_CHECK_EQ_SIZE(fetch_count(f.server, false), 0);
    if (!empty_accept) MOQ_TEST_CHECK_EQ_INT(sub_slot(f.server, h), -1);
    NEED(retry_quiet(f.server));
done:
    teardown(&f);
}

/* The peer may cancel before it receives our error, even though that error
 * has already retired the local request. Inspecting our output does not
 * deliver it to the encoded peer. */
static void crossed_cancel(bool capacity)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 1, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, false) && pending(f.server, h, 1));
    uint64_t cancelled;
    if (capacity) {
        /* More refusals than the one-entry fetch pool: a finite grace cache
         * cannot establish that the first cancellation is now invalid. */
        for (uint64_t id = 4; id <= 10; id += 2) {
            NEED(join(f.server, id, 0, 0, false));
            NEED(error_response(f.server, id, MOQ_REQUEST_ERROR_INTERNAL_ERROR));
        }
        cancelled = 4;
    } else {
        moq_reject_subscribe_cfg_t reject;
        moq_reject_subscribe_cfg_init(&reject);
        reject.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        NEED(moq_session_reject_subscribe(f.server, h, &reject, 3) == MOQ_OK);
        response_t out[2];
        NEED(responses(f.server, out, 2));
        NEED(out[0].id == 0 && out[1].id == 2);
        cancelled = 2;
    }
    MOQ_TEST_CHECK_EQ_INT(cancel_message(f.server, MOQ_D16_FETCH_CANCEL,
                                        cancelled), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_state(f.server), MOQ_SESS_ESTABLISHED);
    NEED(no_events(f.server) && no_actions(f.server));
    if (capacity) {
        NEED(pending(f.server, h, 1));
        NEED(accept(f.server, h, false, 0, 0) == MOQ_OK);
        NEED(accepted_response(f.server, 0, true, 0, 0));
        moq_fetch_t fetch = MOQ_FETCH_INVALID;
        NEED(take_fetch(f.server, h, 0, 0, 1, false, &fetch));
        NEED(deliver_catalog(&f, fetch, 2));
    }
done:
    teardown(&f);
}

/* Retired-ID absorption must not admit future IDs, another active request
 * family, or IDs with the wrong peer parity. */
static void invalid_cancel(unsigned kind)
{
    fixture_t f = {0};
    NEED(setup(&f, 4, 1, 8, 8, true));
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    NEED(subscribe(f.server, 0, "catalog") && take_subscribe(f.server, &h));
    NEED(join(f.server, 2, 0, 0, false) && pending(f.server, h, 1));
    uint64_t id = kind == 0 ? 4 : kind == 1 ? 0 : 3;
    (void)cancel_message(f.server, MOQ_D16_FETCH_CANCEL, id);
    MOQ_TEST_CHECK(moq_session_state(f.server) != MOQ_SESS_ESTABLISHED);
done:
    teardown(&f);
}

int main(int argc, char **argv)
{
    bool controls = argc == 2 && strcmp(argv[1], "--controls") == 0;
    bool coalesced = argc == 2 && strcmp(argv[1], "--coalesced") == 0;
    bool split = argc == 2 && strcmp(argv[1], "--split") == 0;
    bool p2 = argc == 2 && strcmp(argv[1], "--p2") == 0;
    bool scratch = argc == 2 && strcmp(argv[1], "--small-scratch") == 0;
    bool send_cap = argc == 2 && strcmp(argv[1], "--send-capacity") == 0;
    bool send_pressure = argc == 2 && strcmp(argv[1], "--send-pressure") == 0;
    bool admission = argc == 2 && strcmp(argv[1], "--admission") == 0;
    bool crossing = argc == 2 && strcmp(argv[1], "--cross-cancel") == 0;
    if (argc > 2 || (argc == 2 && !controls && !coalesced && !split && !p2 &&
                     !scratch && !send_cap && !send_pressure && !admission &&
                     !crossing)) {
        fprintf(stderr, "usage: %s [--controls|--coalesced|--split|--p2|"
                        "--small-scratch|--send-capacity|--send-pressure|--admission|"
                        "--cross-cancel]\n", argv[0]);
        return 2;
    }
    if (controls || argc == 1) {
        catalog_case(0);
        empty_case(false);
    }
    if (coalesced || argc == 1) catalog_case(1);
    if (split || argc == 1) catalog_case(2);
    if (p2 || scratch || argc == 1) {
        fprintf(stderr, "CASE small-scratch empty accept\n");
        small_limits_case(false, false);
        fprintf(stderr, "CASE small-scratch parent cancel\n");
        small_limits_case(true, false);
    }
    if (p2 || send_cap || argc == 1) {
        fprintf(stderr, "CASE total send capacity\n");
        small_limits_case(false, true);
    }
    if (p2 || send_pressure || argc == 1) {
        fprintf(stderr, "CASE transient send-byte pressure\n");
        send_byte_pressure();
    }
    if (admission || argc == 1) {
        fprintf(stderr, "CASE admission action bound, blocked refusal, parent reject\n");
        admission_case(0, false);
        fprintf(stderr, "CASE admission action bound, empty accept\n");
        admission_case(0, true);
        fprintf(stderr, "CASE admission total send bound\n");
        admission_case(1, false);
        fprintf(stderr, "CASE 32-byte scratch bare parent reject\n");
        admission_case(2, false);
    }
    if (argc == 1) {
        empty_case(true);
        snapshot_and_tokens();
        parent_reject();
        fetch_cancel();
        reused_parent();
        multiple_and_capacity();
        accept_pressure(false);
        accept_pressure(true);
        unsubscribe_case(0);
        unsubscribe_case(1);
        unsubscribe_case(2);
        pending_teardown();
        absolute_join_case(false);
        absolute_join_case(true);
        alias_ownership();
        auth_allocation_failure(false, 1);
        auth_allocation_failure(false, 2);
        auth_allocation_failure(true, 1);
    }
    if (crossing || argc == 1) {
        crossed_cancel(true);
        crossed_cancel(false);
        invalid_cancel(0);
        invalid_cancel(1);
        invalid_cancel(2);
    }
    MOQ_TEST_PASS("test_d16_pending_join");
    return failures ? 1 : 0;
}
