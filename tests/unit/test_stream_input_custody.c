/*
 * Stream-input custody: what moq_session_on_data_bytes(),
 * moq_session_on_data_rcbuf() and moq_session_on_bidi_stream_bytes() say
 * about the bytes they were given. MOQ_ERR_BUFFER means none of them were
 * copied. Once the session took them, a refusal for output room which the
 * queued output holds is MOQ_ERR_WOULD_BLOCK with the bytes kept, and one for
 * room that never returns is MOQ_ERR_LIMIT; neither hands the bytes back.
 *
 * Rows:
 *   - draft 16, data: a FETCH object over this side's payload limit is
 *     cancelled, and the cancellation waits for send room earlier output
 *     holds: WOULD_BLOCK with the stream owned, then one cancellation after
 *     a drain and an empty retry; through both data entry points. A send
 *     buffer the cancellation can never fit: LIMIT. A new stream with no
 *     free receive entry: WOULD_BLOCK, stream not owned (nothing kept);
 *   - drafts 18 and 22, bidi: a request whose event waits for the event
 *     arena an earlier event holds: WOULD_BLOCK with the stream owned, then
 *     its event once after a drain and an empty retry. A refusal the send
 *     buffer can never hold: LIMIT;
 *   - draft 16, bidi: a first delivery larger than the request buffer:
 *     BUFFER with nothing copied, and the request alone is then taken.
 */
#include <moq/moq.h>
#include <moq/control.h>
#include "test_binding_support.h"
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

typedef struct {
    moq_version_t     version;
    moq_perspective_t perspective;
    uint32_t          max_object_payload_size;
    uint32_t          max_data_streams;
    uint32_t          recv_buffer_size;
} opts_t;

static moq_session_t *make(const opts_t *o)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), o->perspective);
    cfg.version = o->version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.max_object_payload_size = o->max_object_payload_size;
    cfg.max_data_streams = o->max_data_streams;
    cfg.recv_buffer_size = o->recv_buffer_size;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK) return NULL;
    if (test_bind_if_required(s) != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

/* One captured stream: its ref and the bytes written on it, in order. */
typedef struct {
    moq_stream_ref_t ref;
    uint8_t          bytes[512];
    size_t           len;
} captured_t;

static void capture(captured_t *c, moq_stream_ref_t ref, const uint8_t *d, size_t n)
{
    c->ref = ref;
    if (n <= sizeof(c->bytes) - c->len) {
        memcpy(c->bytes + c->len, d, n);
        c->len += n;
    }
}

/* Deliver `from`'s control bytes to `to`; keep its data-stream bytes in
 * `data` and its first request streams in `bidi[]` (all other actions
 * dropped). Returns the control delivery's result. */
static moq_result_t exchange(moq_session_t *from, moq_session_t *to, uint64_t now,
                             captured_t *data, captured_t *bidi, size_t bidi_cap,
                             size_t *bidi_n)
{
    uint8_t control[4096];
    size_t n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        const uint8_t *d = NULL;
        size_t len = 0;
        switch (a.kind) {
        case MOQ_ACTION_SEND_CONTROL:
            d = a.u.send_control.data; len = a.u.send_control.len; break;
        case MOQ_ACTION_OPEN_UNI_CONTROL:
            d = a.u.open_uni_control.data; len = a.u.open_uni_control.len; break;
        case MOQ_ACTION_SEND_UNI_CONTROL:
            d = a.u.send_uni_control.data; len = a.u.send_uni_control.len; break;
        case MOQ_ACTION_SEND_DATA:
            if (data) {
                capture(data, a.u.send_data.stream_ref, a.u.send_data.header,
                        a.u.send_data.header_len);
                if (a.u.send_data.payload)
                    capture(data, a.u.send_data.stream_ref,
                            moq_rcbuf_data(a.u.send_data.payload),
                            moq_rcbuf_len(a.u.send_data.payload));
            }
            break;
        case MOQ_ACTION_OPEN_BIDI_STREAM:
            if (bidi && bidi_n && *bidi_n < bidi_cap) {
                capture(&bidi[*bidi_n], a.u.open_bidi_stream.stream_ref,
                        a.u.open_bidi_stream.data, a.u.open_bidi_stream.len);
                (*bidi_n)++;
            }
            break;
        default:
            break;
        }
        if (d && len <= sizeof(control) - n) {
            memcpy(control + n, d, len);
            n += len;
        }
        moq_action_cleanup(&a);
    }
    return n ? moq_session_on_control_bytes(to, control, n, now) : MOQ_OK;
}

/* Queued actions (SEND_CONTROL ones counted, the first one's type byte),
 * all dropped. */
static size_t drain_actions(moq_session_t *s, size_t *sends, uint8_t *first_type,
                            size_t *stops)
{
    size_t n = 0;
    if (sends) *sends = 0;
    if (stops) *stops = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        n++;
        if (a.kind == MOQ_ACTION_SEND_CONTROL && sends) {
            if (*sends == 0 && first_type && a.u.send_control.len)
                *first_type = a.u.send_control.data[0];
            (*sends)++;
        }
        if (a.kind == MOQ_ACTION_STOP_DATA && stops) (*stops)++;
        moq_action_cleanup(&a);
    }
    return n;
}

/* Queued events, and how many are of `kind`; all dropped. */
static size_t drain_events(moq_session_t *s, uint32_t kind, size_t *of_kind)
{
    size_t n = 0;
    if (of_kind) *of_kind = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        n++;
        if (of_kind && ev.kind == kind) (*of_kind)++;
        moq_event_cleanup(&ev);
    }
    return n;
}

static moq_bytes_t g_ns_part[] = { { (const uint8_t *)"live", 4 } };

/* A draft-16 pair whose client fetched one object of eight bytes over its
 * four-byte payload limit; the server's data stream is in `data`, not yet
 * delivered. */
static bool fetch_pair(moq_session_t **cp, moq_session_t **svp, captured_t *data)
{
    moq_session_t *c = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                       .perspective = MOQ_PERSPECTIVE_CLIENT,
                                       .max_object_payload_size = 4,
                                       .max_data_streams = 1 });
    moq_session_t *sv = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                        .perspective = MOQ_PERSPECTIVE_SERVER });
    *cp = c;
    *svp = sv;
    if (!c || !sv) return false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)exchange(c, sv, 1, NULL, NULL, 0, NULL), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)exchange(sv, c, 1, NULL, NULL, 0, NULL), MOQ_OK);
    drain_events(c, 0, NULL);
    drain_events(sv, 0, NULL);

    moq_fetch_cfg_t f;
    moq_fetch_cfg_init(&f);
    f.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    f.track_name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    f.start_group = 0;
    f.end_group = 1;
    moq_fetch_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(c, &f, 2, &h), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)exchange(c, sv, 2, NULL, NULL, 0, NULL), MOQ_OK);
    moq_fetch_t sf = { 0 };
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
            sf = ev.u.fetch_request.fetch;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(got);
    if (!got) return false;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(sv, sf, &ac, 3), MOQ_OK);
    static const uint8_t eight[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(moq_alloc_default(), eight, sizeof(eight),
                                                &payload), MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 3;
    oc.payload = payload;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_object(sv, sf, &oc, 3), MOQ_OK);
    moq_rcbuf_decref(payload);
    MOQ_TEST_CHECK_EQ_INT((int)exchange(sv, c, 4, data, NULL, 0, NULL), MOQ_OK);
    drain_events(c, 0, NULL);
    MOQ_TEST_CHECK(data->len > 8);
    return data->len > 8;
}

/* Fill all but `room` bytes of `s`'s send buffer with queued output. */
static void leave_send_room(moq_session_t *s, size_t room)
{
    size_t fill = s->send_cap - s->send_len - room;
    uint8_t *pad = calloc(fill ? fill : 1, 1);
    MOQ_TEST_CHECK(pad != NULL);
    if (!pad) return;
    MOQ_TEST_CHECK_EQ_INT((int)queue_send_control(s, pad, fill), MOQ_OK);
    free(pad);
}

static moq_result_t feed_data(moq_session_t *s, bool rcbuf, moq_stream_ref_t ref,
                              const uint8_t *d, size_t n, uint64_t now)
{
    if (!rcbuf || n == 0) return moq_session_on_data_bytes(s, ref, d, n, false, now);
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), d, n, &b) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_on_data_rcbuf(s, ref, b, false, now);
    moq_rcbuf_decref(b);
    return rc;
}

static int row_data_cancel_waits_for_send_room(void)
{
    failures = 0;
    for (int rcbuf = 0; rcbuf < 2; rcbuf++) {
        const int before = failures;
        moq_session_t *c = NULL, *sv = NULL;
        captured_t data;
        memset(&data, 0, sizeof(data));
        if (!fetch_pair(&c, &sv, &data)) {
            if (c) moq_session_destroy(c);
            if (sv) moq_session_destroy(sv);
            continue;
        }
        /* The cancellation needs more than the three bytes left. */
        leave_send_room(c, 3);
        moq_result_t rc = feed_data(c, rcbuf != 0, data.ref, data.bytes, data.len, 5);
        MOQ_TEST_CHECK(rc != MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_session_has_transport_stream(c, data.ref));
        MOQ_TEST_CHECK_EQ_SIZE(drain_events(c, 0, NULL), 0);

        /* A new stream finds no receive entry: nothing is kept. */
        static const uint8_t other[] = { 0x05, 0x00 };
        const moq_stream_ref_t stranger = moq_stream_ref_from_u64(0x7777);
        MOQ_TEST_CHECK_EQ_INT((int)feed_data(c, rcbuf != 0, stranger, other, sizeof(other), 5),
                              MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(c, stranger));

        /* Draining releases the room; the empty retry sends the
         * cancellation once and stops the stream. */
        size_t sends = 0, stops = 0;
        drain_actions(c, &sends, NULL, &stops);
        MOQ_TEST_CHECK_EQ_SIZE(sends, 1);
        MOQ_TEST_CHECK_EQ_SIZE(stops, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(c, data.ref, NULL, 0, false, 6),
                              MOQ_OK);
        uint8_t type = 0;
        drain_actions(c, &sends, &type, &stops);
        MOQ_TEST_CHECK_EQ_SIZE(sends, 1);
        MOQ_TEST_CHECK_EQ_INT((int)type, (int)MOQ_D16_FETCH_CANCEL);
        MOQ_TEST_CHECK_EQ_SIZE(stops, 1);
        size_t resets = 0;
        drain_events(c, MOQ_EVENT_FETCH_RESET, &resets);
        MOQ_TEST_CHECK_EQ_SIZE(resets, 1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
        if (failures != before)
            fprintf(stderr, "  in row data_cancel_waits_for_send_room (%s)\n",
                    rcbuf ? "rcbuf" : "bytes");
        moq_session_destroy(sv);
        moq_session_destroy(c);
    }
    MOQ_TEST_PASS("a data delivery whose cancellation waits for send room is WOULD_BLOCK, kept");
    return failures;
}

static int row_data_cancel_never_fits(void)
{
    failures = 0;
    for (int rcbuf = 0; rcbuf < 2; rcbuf++) {
        const int before = failures;
        moq_session_t *c = NULL, *sv = NULL;
        captured_t data;
        memset(&data, 0, sizeof(data));
        if (!fetch_pair(&c, &sv, &data)) {
            if (c) moq_session_destroy(c);
            if (sv) moq_session_destroy(sv);
            continue;
        }
        /* White-box: a send buffer smaller than the cancellation. */
        MOQ_TEST_CHECK_EQ_SIZE(c->send_len, 0);
        c->send_cap = 3;
        moq_result_t rc = feed_data(c, rcbuf != 0, data.ref, data.bytes, data.len, 5);
        MOQ_TEST_CHECK(rc != MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_LIMIT);
        MOQ_TEST_CHECK(moq_session_has_transport_stream(c, data.ref));
        MOQ_TEST_CHECK_EQ_SIZE(drain_actions(c, NULL, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_SIZE(drain_events(c, 0, NULL), 0);
        if (failures != before)
            fprintf(stderr, "  in row data_cancel_never_fits (%s)\n", rcbuf ? "rcbuf" : "bytes");
        moq_session_destroy(sv);
        moq_session_destroy(c);
    }
    MOQ_TEST_PASS("a data delivery whose cancellation can never fit is LIMIT");
    return failures;
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "draft-16"
         : v == MOQ_VERSION_DRAFT_18 ? "draft-18" : "draft-22";
}

/* An established pair of `v`; the client's SUBSCRIBE request streams, not
 * yet delivered, in `bidi[0..n)`. */
static size_t subscribe_pair(moq_version_t v, moq_session_t **cp, moq_session_t **svp,
                             captured_t *bidi, size_t n)
{
    moq_session_t *c = make(&(opts_t){ .version = v, .perspective = MOQ_PERSPECTIVE_CLIENT });
    moq_session_t *sv = make(&(opts_t){ .version = v, .perspective = MOQ_PERSPECTIVE_SERVER });
    *cp = c;
    *svp = sv;
    if (!c || !sv) return 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)exchange(c, sv, 1, NULL, NULL, 0, NULL), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)exchange(sv, c, 1, NULL, NULL, 0, NULL), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    drain_events(c, 0, NULL);
    drain_events(sv, 0, NULL);
    static const char names[4][2] = { "a", "b", "c", "d" };
    for (size_t i = 0; i < n && i < 4; i++) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
        sc.track_name = (moq_bytes_t){ (const uint8_t *)names[i], 1 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &sc, 2, &sub), MOQ_OK);
    }
    size_t got = 0;
    memset(bidi, 0, n * sizeof(*bidi));
    MOQ_TEST_CHECK_EQ_INT((int)exchange(c, sv, 2, NULL, bidi, n, &got), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(got, n);
    return got;
}

static int row_bidi_request_waits_for_event_room(void)
{
    failures = 0;
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < 2; i++) {
        const moq_version_t v = versions[i];
        const int before = failures;
        moq_session_t *c = NULL, *sv = NULL;
        captured_t bidi[2];
        if (subscribe_pair(v, &c, &sv, bidi, 2) == 2) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv, bidi[0].ref,
                bidi[0].bytes, bidi[0].len, false, 3), MOQ_OK);
            /* White-box: the event arena keeps one spare byte past the first
             * request's event; the second event waits for it. */
            MOQ_TEST_CHECK(sv->event_scratch_len > 0);
            const size_t cap = sv->event_scratch_cap;
            sv->event_scratch_cap = sv->event_scratch_len + 1;
            moq_result_t rc = moq_session_on_bidi_stream_bytes(sv, bidi[1].ref,
                bidi[1].bytes, bidi[1].len, false, 4);
            MOQ_TEST_CHECK(rc != MOQ_ERR_BUFFER);
            MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK(moq_session_has_transport_stream(sv, bidi[1].ref));
            size_t requests = 0;
            drain_events(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &requests);
            MOQ_TEST_CHECK_EQ_SIZE(requests, 1);
            /* Drained, the empty retry surfaces the second request once. */
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv, bidi[1].ref,
                NULL, 0, false, 5), MOQ_OK);
            drain_events(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &requests);
            MOQ_TEST_CHECK_EQ_SIZE(requests, 1);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv, bidi[1].ref,
                NULL, 0, false, 6), MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(drain_events(sv, 0, NULL), 0);
            sv->event_scratch_cap = cap;
        }
        if (failures != before)
            fprintf(stderr, "  in row bidi_request_waits_for_event_room (%s)\n", vname(v));
        if (sv) moq_session_destroy(sv);
        if (c) moq_session_destroy(c);
    }
    MOQ_TEST_PASS("a request waiting for event room is WOULD_BLOCK, kept, then surfaced once");
    return failures;
}

static int row_bidi_refusal_never_fits(void)
{
    failures = 0;
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < 2; i++) {
        const moq_version_t v = versions[i];
        const int before = failures;
        moq_session_t *c = NULL, *sv = NULL;
        captured_t bidi[1];
        if (subscribe_pair(v, &c, &sv, bidi, 1) == 1) {
            /* The server is going away, so it refuses the new request; its
             * send buffer (white-box) can never hold the refusal. */
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(sv, NULL, 0, 3), MOQ_OK);
            drain_actions(sv, NULL, NULL, NULL);
            moq_action_t a;
            MOQ_TEST_CHECK(moq_session_poll_actions(sv, &a, 1) == 0);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(sv, 4), MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(sv->send_len, 0);
            sv->send_cap = 2;
            moq_result_t rc = moq_session_on_bidi_stream_bytes(sv, bidi[0].ref,
                bidi[0].bytes, bidi[0].len, false, 5);
            MOQ_TEST_CHECK(rc != MOQ_ERR_BUFFER);
            MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_LIMIT);
            MOQ_TEST_CHECK_EQ_SIZE(drain_actions(sv, NULL, NULL, NULL), 0);
            MOQ_TEST_CHECK_EQ_SIZE(drain_events(sv, 0, NULL), 0);
        }
        if (failures != before)
            fprintf(stderr, "  in row bidi_refusal_never_fits (%s)\n", vname(v));
        if (sv) moq_session_destroy(sv);
        if (c) moq_session_destroy(c);
    }
    MOQ_TEST_PASS("a request whose refusal can never fit is LIMIT");
    return failures;
}

static int row_bidi_delivery_larger_than_buffer(void)
{
    failures = 0;
    moq_session_t *c = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                       .perspective = MOQ_PERSPECTIVE_CLIENT });
    moq_session_t *sv = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                        .perspective = MOQ_PERSPECTIVE_SERVER,
                                        .recv_buffer_size = 128 });
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (c && sv) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)exchange(c, sv, 1, NULL, NULL, 0, NULL), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)exchange(sv, c, 1, NULL, NULL, 0, NULL), MOQ_OK);
        drain_events(c, 0, NULL);
        drain_events(sv, 0, NULL);
        moq_subscribe_namespace_cfg_t nc;
        moq_subscribe_namespace_cfg_init(&nc);
        nc.track_namespace_prefix = (moq_namespace_t){ g_ns_part, 1 };
        nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe_namespace(c, &nc, 2, &h), MOQ_OK);
        captured_t bidi[1];
        size_t got = 0;
        memset(bidi, 0, sizeof(bidi));
        MOQ_TEST_CHECK_EQ_INT((int)exchange(c, sv, 2, NULL, bidi, 1, &got), MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(got, 1);
        if (got == 1) {
            uint8_t big[200];
            memset(big, 0, sizeof(big));
            memcpy(big, bidi[0].bytes, bidi[0].len);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv, bidi[0].ref,
                big, sizeof(big), false, 3), MOQ_ERR_BUFFER);
            MOQ_TEST_CHECK(!moq_session_has_transport_stream(sv, bidi[0].ref));
            MOQ_TEST_CHECK_EQ_SIZE(drain_events(sv, 0, NULL), 0);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv, bidi[0].ref,
                bidi[0].bytes, bidi[0].len, false, 4), MOQ_OK);
            size_t requests = 0;
            drain_events(sv, MOQ_EVENT_NS_SUB_REQUEST, &requests);
            MOQ_TEST_CHECK_EQ_SIZE(requests, 1);
        }
    }
    if (sv) moq_session_destroy(sv);
    if (c) moq_session_destroy(c);
    MOQ_TEST_PASS("a first bidi delivery larger than the request buffer is BUFFER, nothing copied");
    return failures;
}

int main(void)
{
    int total = 0;
    total += row_data_cancel_waits_for_send_room();
    total += row_data_cancel_never_fits();
    total += row_bidi_request_waits_for_event_room();
    total += row_bidi_refusal_never_fits();
    total += row_bidi_delivery_larger_than_buffer();
    if (total) fprintf(stderr, "test_stream_input_custody: %d failure(s)\n", total);
    return total ? 1 : 0;
}
