/*
 * Coalesced deferred draft-18 updates must fit a bounded action queue:
 * every distinct REQUEST_OK must make progress, together with the separate
 * owner-wide priority action when enabled. Draft-18 section 10.9.1 still
 * requires one wire response per update, even when processing is coalesced.
 *
 * Public session APIs and real codec ingress establish both SUBSCRIBE and
 * PUBLISH owners. Each retains an open subgroup whose header is drained
 * before the updates. Eight updates exceed the four-slot action capacity.
 * Real queued objects or a namespace announcement then exercise temporary
 * action/send-byte pressure, atomic refusal and successful drain/retry.
 * Every setup or result failure exits nonzero, including under NDEBUG.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    ACTION_SLOTS = 4,
    SEND_BYTES = 4096,
    INITIAL_PRIORITY = 128,
    PUBLISHER_PRIORITY = 128
};

typedef enum pressure {
    PRESSURE_NONE,
    PRESSURE_ACTIONS,
    PRESSURE_BYTES
} pressure_t;

typedef struct fixture {
    moq_session_t *s;
    bool publication;
    bool priority_actions;
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_subgroup_handle_t subgroup;
    moq_stream_ref_t request_ref;
    moq_stream_ref_t data_ref;
    uint64_t newest_id;
    uint8_t newest_priority;
    uint64_t next_object;
    uint64_t now;
} fixture_t;

static char row[128];
static bool checking_result;
static unsigned rows;

static void fail(const char *expression, int line, bool has_rc, moq_result_t rc)
{
    fprintf(stderr, "%s %s %s:%d: %s", checking_result ? "FAIL" :
            "PREREQUISITE", row, __FILE__, line, expression);
    if (has_rc) fprintf(stderr, " returned %d", (int)rc);
    fputc('\n', stderr);
    exit(checking_result ? 1 : 2);
}

#define REQUIRE(expr) do { \
    if (!(expr)) fail(#expr, __LINE__, false, MOQ_OK); \
} while (0)
#define OK(expr) do { \
    moq_result_t checked_rc = (expr); \
    if (checked_rc != MOQ_OK) fail(#expr, __LINE__, true, checked_rc); \
} while (0)

static bool poll_action(fixture_t *f, moq_action_t *a)
{
    size_t n = 0;
    OK(moq_session_poll_actions_ex(f->s, a, 1, sizeof(*a), &n));
    REQUIRE(n <= 1);
    return n == 1;
}

static bool poll_event(fixture_t *f, moq_event_t *ev)
{
    size_t n = 0;
    OK(moq_session_poll_events_ex(f->s, ev, 1, sizeof(*ev), &n));
    REQUIRE(n <= 1);
    return n == 1;
}

static void no_events(fixture_t *f)
{
    moq_event_t ev;
    REQUIRE(!poll_event(f, &ev));
    REQUIRE(moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
}

static void empty_outputs(fixture_t *f)
{
    moq_action_t a;
    REQUIRE(!poll_action(f, &a));
    no_events(f);
    REQUIRE(moq_session_action_capacity(f->s) == ACTION_SLOTS);
}

static uint64_t owner(const fixture_t *f)
{
    return f->publication ? f->pub._opaque : f->sub._opaque;
}

static moq_result_t owed(fixture_t *f, uint64_t *id, bool *has_filter)
{
    return f->publication
        ? moq_session_publication_owed_update_sized(f->s, f->pub, id,
                                                   has_filter, NULL, 0)
        : moq_session_subscription_owed_update_sized(f->s, f->sub, id,
                                                    has_filter, NULL, 0);
}

static void still_owed(fixture_t *f)
{
    uint64_t id = UINT64_MAX;
    bool has_filter = true;
    OK(owed(f, &id, &has_filter));
    REQUIRE(id == f->newest_id && !has_filter);
}

static void ingress(fixture_t *f, const moq_buf_writer_t *w)
{
    REQUIRE(moq_buf_writer_offset(w) > 0);
    OK(moq_session_on_bidi_stream_bytes(f->s, f->request_ref, w->data,
                                       moq_buf_writer_offset(w), false, ++f->now));
}

static void establish(fixture_t *f)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                              MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_actions = ACTION_SLOTS;
    cfg.max_events = 16;
    cfg.send_buffer_size = SEND_BYTES;
    OK(moq_session_create(&cfg, f->now, &f->s));
    OK(moq_session_set_data_priority_updates(f->s, f->priority_actions));
    OK(moq_session_set_defer_subscription_updates(f->s, true));
    OK(moq_session_start(f->s, f->now));
    moq_action_t a;
    REQUIRE(poll_action(f, &a));
    REQUIRE(a.kind == MOQ_ACTION_OPEN_UNI_CONTROL);
    REQUIRE(a.u.open_uni_control.len > 0);
    moq_action_cleanup(&a);
    REQUIRE(!poll_action(f, &a));

    uint8_t wire[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    OK(moq_d18_encode_setup(&w));
    OK(moq_session_on_control_bytes(f->s, wire, moq_buf_writer_offset(&w),
                                    ++f->now));
    REQUIRE(moq_session_version(f->s) == MOQ_VERSION_DRAFT_18);
    moq_event_t ev;
    REQUIRE(poll_event(f, &ev));
    REQUIRE(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
    moq_event_cleanup(&ev);
    empty_outputs(f);

    moq_bytes_t part = MOQ_BYTES_LITERAL("coalesced");
    moq_namespace_t ns = { .parts = &part, .count = 1 };
    moq_d18_msg_params_t params = {0};
    params.has_subscriber_priority = true;
    params.subscriber_priority = INITIAL_PRIORITY;
    params.has_forward = true;
    params.forward = 1;
    if (f->publication) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = ns;
        pc.track_name = MOQ_BYTES_LITERAL("track");
        OK(moq_session_publish(f->s, &pc, ++f->now, &f->pub));
        REQUIRE(poll_action(f, &a));
        REQUIRE(a.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
        REQUIRE(a.u.open_bidi_stream.len > 0 && !a.u.open_bidi_stream.fin);
        f->request_ref = a.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&a);
        empty_outputs(f);
        moq_buf_writer_init(&w, wire, sizeof(wire));
        OK(moq_d18_encode_publish_ok(&w, &params));
        ingress(f, &w);
        REQUIRE(poll_event(f, &ev));
        REQUIRE(ev.kind == MOQ_EVENT_PUBLISH_OK);
        REQUIRE(ev.u.publish_ok.pub._opaque == f->pub._opaque);
        REQUIRE(ev.u.publish_ok.send_allowed);
        REQUIRE(ev.u.publish_ok.subscriber_priority == INITIAL_PRIORITY);
        moq_event_cleanup(&ev);
    } else {
        f->request_ref = moq_stream_ref_from_u64(0x7000);
        moq_buf_writer_init(&w, wire, sizeof(wire));
        OK(moq_d18_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("track"),
                                    &params));
        ingress(f, &w);
        REQUIRE(poll_event(f, &ev));
        REQUIRE(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        f->sub = ev.u.subscribe_request.sub;
        REQUIRE(ev.u.subscribe_request.subscriber_priority == INITIAL_PRIORITY);
        moq_event_cleanup(&ev);
        empty_outputs(f);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        OK(moq_session_accept_subscribe(f->s, f->sub, &ac, ++f->now));
        REQUIRE(poll_action(f, &a));
        REQUIRE(a.kind == MOQ_ACTION_SEND_BIDI_STREAM);
        REQUIRE(a.u.send_bidi_stream.stream_ref._v == f->request_ref._v);
        REQUIRE(a.u.send_bidi_stream.len > 0 && !a.u.send_bidi_stream.fin);
        moq_action_cleanup(&a);
    }
    REQUIRE(owner(f) != 0 && f->request_ref._v != 0);
    REQUIRE(owed(f, NULL, NULL) == MOQ_ERR_WRONG_STATE);
    empty_outputs(f);
}

static void open_data_owner(fixture_t *f)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 1;
    sc.publisher_priority = PUBLISHER_PRIORITY;
    sc.first_object = true;
    if (f->publication)
        OK(moq_session_open_pub_subgroup(f->s, f->pub, &sc, ++f->now, &f->subgroup));
    else
        OK(moq_session_open_subgroup(f->s, f->sub, &sc, ++f->now, &f->subgroup));
    REQUIRE(moq_subgroup_is_valid(f->subgroup));
    unsigned headers = 0, keys = 0;
    moq_stream_ref_t key_ref = moq_stream_ref_from_u64(0);
    moq_action_t a;
    while (poll_action(f, &a)) {
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            REQUIRE(++headers == 1);
            f->data_ref = a.u.send_data.stream_ref;
            REQUIRE(a.u.send_data.header_len > 0 && !a.u.send_data.fin);
            REQUIRE(a.u.send_data.payload == NULL);
            REQUIRE(a.u.send_data.scheduling_priority == UINT64_C(0x18080));
        } else {
            REQUIRE(a.kind == MOQ_ACTION_SET_DATA_PRIORITY);
            REQUIRE(++keys == 1);
            key_ref = a.u.set_data_priority.stream_ref;
            REQUIRE(a.u.set_data_priority.priority.owner == owner(f));
            REQUIRE(a.u.set_data_priority.priority.subscriber_priority ==
                    INITIAL_PRIORITY);
        }
        moq_action_cleanup(&a);
    }
    REQUIRE(headers == 1 && keys == (f->priority_actions ? 1u : 0u));
    REQUIRE(f->data_ref._v != 0 && f->data_ref._v != f->request_ref._v);
    REQUIRE(!f->priority_actions || key_ref._v == f->data_ref._v);
    empty_outputs(f); /* The subgroup stays open, with no queued header. */
}

static void receive_updates(fixture_t *f, unsigned count)
{
    for (unsigned i = 0; i < count; i++) {
        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        /* Peer requests have client parity. SUBSCRIBE consumed ID 0;
         * the server-initiated PUBLISH consumed only a local odd ID. */
        f->newest_id = 2u * (i + (f->publication ? 0u : 1u));
        f->newest_priority = (uint8_t)(10u + i);
        moq_d18_msg_params_t p = {0};
        p.has_subscriber_priority = true;
        p.subscriber_priority = f->newest_priority;
        OK(moq_d18_encode_request_update(&w, f->newest_id, &p));
        ingress(f, &w);
        moq_event_t ev;
        REQUIRE(poll_event(f, &ev));
        if (f->publication) {
            REQUIRE(ev.kind == MOQ_EVENT_PUBLISH_UPDATED);
            const moq_publish_updated_event_t *u = &ev.u.publish_updated;
            REQUIRE(ev.detail_size >= offsetof(moq_publish_updated_event_t,
                    update_request_id) + sizeof(u->update_request_id));
            REQUIRE(u->pub._opaque == f->pub._opaque && u->answer_owed);
            REQUIRE(u->update_count == i + 1 && u->update_request_id == f->newest_id);
            REQUIRE(u->has_subscriber_priority && u->subscriber_priority ==
                    f->newest_priority && !u->fill_requested);
        } else {
            REQUIRE(ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
            const moq_subscribe_updated_event_t *u = &ev.u.subscribe_updated;
            REQUIRE(ev.detail_size >= offsetof(moq_subscribe_updated_event_t,
                    update_request_id) + sizeof(u->update_request_id));
            REQUIRE(u->sub._opaque == f->sub._opaque && u->answer_owed);
            REQUIRE(u->update_count == i + 1 && u->update_request_id == f->newest_id);
            REQUIRE(u->has_subscriber_priority && u->subscriber_priority ==
                    f->newest_priority && !u->fill_requested);
        }
        moq_event_cleanup(&ev);
        still_owed(f);
        empty_outputs(f);
    }
}

static moq_result_t accept_updates(fixture_t *f)
{
    moq_accept_subscription_update_cfg_t ac;
    moq_accept_subscription_update_cfg_init(&ac);
    ac.update_request_id = f->newest_id;
    return f->publication
        ? moq_session_accept_publication_update(f->s, f->pub, &ac, ++f->now)
        : moq_session_accept_subscription_update(f->s, f->sub, &ac, ++f->now);
}

static void queue_object(fixture_t *f)
{
    moq_rcbuf_t *payload = NULL;
    const uint8_t byte = 0x42;
    OK(moq_rcbuf_create(moq_alloc_default(), &byte, 1, &payload));
    moq_result_t rc = moq_session_write_object(f->s, f->subgroup,
                                              f->next_object, payload, ++f->now);
    moq_rcbuf_decref(payload);
    OK(rc);
    f->next_object++;
}

static void take_object(fixture_t *f, uint8_t expected_priority)
{
    moq_action_t a;
    REQUIRE(poll_action(f, &a));
    REQUIRE(a.kind == MOQ_ACTION_SEND_DATA);
    REQUIRE(a.u.send_data.stream_ref._v == f->data_ref._v);
    REQUIRE(a.u.send_data.header_len > 0 && !a.u.send_data.fin);
    REQUIRE(a.u.send_data.payload != NULL);
    REQUIRE(moq_rcbuf_len(a.u.send_data.payload) == 1);
    REQUIRE(moq_rcbuf_data(a.u.send_data.payload)[0] == 0x42);
    REQUIRE(a.u.send_data.scheduling_priority ==
            (UINT64_C(0x10000) | ((uint64_t)expected_priority << 8) |
             PUBLISHER_PRIORITY));
    moq_action_cleanup(&a);
}

static void check_effective_priority(fixture_t *f, uint8_t expected)
{
    queue_object(f);
    take_object(f, expected);
    empty_outputs(f);
}

static void refused(fixture_t *f, size_t free_actions)
{
    for (unsigned attempt = 0; attempt < 3; attempt++) {
        REQUIRE(moq_session_action_capacity(f->s) == free_actions);
        REQUIRE(accept_updates(f) == MOQ_ERR_WOULD_BLOCK);
        REQUIRE(moq_session_action_capacity(f->s) == free_actions);
        still_owed(f);
        no_events(f);
    }
}

static void action_pressure(fixture_t *f)
{
    /* With priority updates, leave room for the ACK action alone, so an
     * incomplete preflight would leak a partial answer or priority commit.
     * Without them, fill the entire queue with real object output. */
    unsigned queued = ACTION_SLOTS - (f->priority_actions ? 1u : 0u);
    for (unsigned i = 0; i < queued; i++) {
        queue_object(f);
        REQUIRE(moq_session_action_capacity(f->s) == ACTION_SLOTS - i - 1);
    }
    refused(f, ACTION_SLOTS - queued);
    for (unsigned i = 0; i < queued; i++) take_object(f, INITIAL_PRIORITY);
    empty_outputs(f); /* No ACK or priority action escaped the refusal. */
}

static void byte_pressure(fixture_t *f, unsigned count)
{
    /* Queue namespace output in the ordinary 4096-byte arena. Its encoded
     * length leaves less room than the empty-parameter ACK
     * batch, even though three of the four action slots remain available. */
    uint8_t padding[SEND_BYTES - 24];
    uint8_t expected[SEND_BYTES], ack[64];
    memset(padding, 'n', sizeof(padding));
    moq_bytes_t part = { .data = padding, .len = sizeof(padding) };
    moq_namespace_t ns = { .parts = &part, .count = 1 };
    moq_d18_msg_params_t params = {0};
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, expected, sizeof(expected));
    /* PUBLISH used local ID 1; inbound SUBSCRIBE used only peer ID 0. */
    OK(moq_d18_encode_publish_namespace(&w, f->publication ? 3 : 1, &ns,
                                        &params));
    size_t expected_len = moq_buf_writer_offset(&w);
    moq_buf_writer_init(&w, ack, sizeof(ack));
    OK(moq_d18_encode_request_ok(&w));
    size_t batch_len = count * moq_buf_writer_offset(&w);
    REQUIRE(expected_len > 0 && expected_len < SEND_BYTES);
    REQUIRE(batch_len > SEND_BYTES - expected_len && batch_len <= SEND_BYTES);
    REQUIRE(f->next_object == 0); /* These ACKs have no Largest Object yet. */

    moq_publish_namespace_cfg_t nc;
    moq_publish_namespace_cfg_init(&nc);
    nc.track_namespace = ns;
    moq_announcement_t ann;
    OK(moq_session_publish_namespace(f->s, &nc, ++f->now, &ann));
    REQUIRE(moq_announcement_is_valid(ann));
    REQUIRE(moq_session_action_capacity(f->s) == ACTION_SLOTS - 1);
    refused(f, ACTION_SLOTS - 1);

    moq_action_t a;
    REQUIRE(poll_action(f, &a));
    REQUIRE(a.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
    REQUIRE(a.u.open_bidi_stream.stream_ref._v != 0);
    REQUIRE(a.u.open_bidi_stream.stream_ref._v != f->request_ref._v);
    REQUIRE(a.u.open_bidi_stream.stream_ref._v != f->data_ref._v);
    REQUIRE(!a.u.open_bidi_stream.fin && a.u.open_bidi_stream.len == expected_len);
    REQUIRE(memcmp(a.u.open_bidi_stream.data, expected, expected_len) == 0);
    moq_action_cleanup(&a);
    empty_outputs(f); /* Only the intact announcement was queued. */
}

static void check_answers(fixture_t *f, unsigned count)
{
    unsigned acknowledgments = 0, sends = 0, keys = 0;
    moq_action_t a;
    while (poll_action(f, &a)) {
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            sends++;
            REQUIRE(a.u.send_bidi_stream.stream_ref._v == f->request_ref._v);
            REQUIRE(a.u.send_bidi_stream.len > 0 && !a.u.send_bidi_stream.fin);
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, a.u.send_bidi_stream.data,
                                a.u.send_bidi_stream.len);
            while (moq_buf_reader_remaining(&r) > 0) {
                REQUIRE(acknowledgments < count);
                moq_control_envelope_t env;
                OK(moq_d18_decode_envelope(&r, &env));
                REQUIRE(env.msg_type == MOQ_D18_REQUEST_OK);
                /* REQUEST_UPDATE_OK has no ID: its request stream and
                 * response count correlate it. Decode each full parameter
                 * block through the public codec, including Largest Object
                 * after the writes that prove refusal kept the old priority. */
                moq_buf_reader_t body;
                moq_buf_reader_init(&body, env.payload, env.payload_len);
                uint64_t param_count;
                OK(moq_buf_read_vi64(&body, &param_count));
                moq_d18_msg_params_t params;
                OK(moq_d18_decode_msg_params(&body, param_count,
                    MOQ_D18_PARAM_BIT_LARGEST_OBJECT | MOQ_D18_PARAM_BIT_EXPIRES,
                    &params));
                REQUIRE(moq_buf_reader_remaining(&body) == 0); /* empty properties */
                REQUIRE(!params.has_expires);
                REQUIRE(params.has_largest == (f->next_object != 0));
                if (params.has_largest) {
                    REQUIRE(params.largest_group == 1);
                    REQUIRE(params.largest_object == f->next_object - 1);
                }
                acknowledgments++;
            }
        } else {
            REQUIRE(a.kind == MOQ_ACTION_SET_DATA_PRIORITY);
            REQUIRE(++keys == 1);
            REQUIRE(a.u.set_data_priority.stream_ref._v == 0); /* owner-wide */
            REQUIRE(a.u.set_data_priority.priority.owner == owner(f));
            REQUIRE(a.u.set_data_priority.priority.subscriber_priority ==
                    f->newest_priority);
        }
        moq_action_cleanup(&a);
    }
    REQUIRE(sends > 0 && acknowledgments == count);
    REQUIRE(keys == (f->priority_actions ? 1u : 0u));
    REQUIRE(owed(f, NULL, NULL) == MOQ_ERR_WRONG_STATE);
    empty_outputs(f);
}

static void run_row(bool publication, unsigned count, bool priority_actions,
                    pressure_t pressure)
{
    fixture_t f = {0};
    f.publication = publication;
    f.priority_actions = priority_actions;
    snprintf(row, sizeof(row), "%s updates=%u priority=%s pressure=%s",
             publication ? "publication" : "subscription", count,
             priority_actions ? "on" : "off", pressure == PRESSURE_ACTIONS ?
             "actions" : pressure == PRESSURE_BYTES ? "bytes" : "none");
    checking_result = false;
    establish(&f);
    open_data_owner(&f);
    receive_updates(&f, count);

    checking_result = true;
    if (pressure == PRESSURE_ACTIONS) action_pressure(&f);
    if (pressure == PRESSURE_BYTES) byte_pressure(&f, count);
    if (pressure != PRESSURE_NONE) {
        /* A NEW object after refusal observes effective state, not merely
         * the priority saved in output queued before the failed accept. */
        check_effective_priority(&f, INITIAL_PRIORITY);
        still_owed(&f);
    }
    OK(accept_updates(&f));
    check_answers(&f, count);
    REQUIRE(accept_updates(&f) == MOQ_ERR_WRONG_STATE);
    empty_outputs(&f);
    check_effective_priority(&f, f.newest_priority);
    moq_session_destroy(f.s);
    printf("PASS: %s\n", row);
    rows++;
}

int main(void)
{
    for (unsigned publication = 0; publication < 2; publication++) {
        run_row(publication != 0, 3, true, PRESSURE_NONE);
        run_row(publication != 0, 4, false, PRESSURE_NONE);
        run_row(publication != 0, 4, true, PRESSURE_NONE);
        for (unsigned priority = 0; priority < 2; priority++) {
            run_row(publication != 0, 8, priority != 0, PRESSURE_NONE);
            run_row(publication != 0, 8, priority != 0, PRESSURE_ACTIONS);
            run_row(publication != 0, 8, priority != 0, PRESSURE_BYTES);
        }
    }
    printf("coalesced_update_capacity: %u rows passed\n", rows);
    return 0;
}
