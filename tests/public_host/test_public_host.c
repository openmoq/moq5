/*
 * Full protocol flows driven through a transport host that uses the public
 * headers alone (public_host.h): setup, a subscription with objects, a
 * publication the server opens, a fetch, partial writes and one-byte reads,
 * event pressure, a refused stream offered again, streams the transport
 * retires with no terminal from the peer, a STOP on a request stream this
 * side opened, streams that end before their type is complete, and the end
 * of a control stream. In the tagged host mode: the reset transaction, held
 * output past its tag deadline (subgroup bytes and datagrams), an owner's
 * final message held for its fence, and a tick that cannot act. With the
 * server's output in per-stream order, so other streams may overtake a held
 * record:
 * - a FETCH's request FIN held behind its response stream's FIN or reset
 *   alone, its release, and a cancellation that waits for nothing;
 * - a STOP of a FETCH's response stream (RESET_FINAL_ON_STOP), observed
 *   while that stream is open, ending the paired request with a CANCELLED
 *   reset in place of its final message. Covered: a live FETCH, an ended
 *   one, an empty or unconsumed one, the final not yet polled, a held claim
 *   and a queued reset of the session's own, a repeated STOP, a later owner
 *   of the same slot, and draft 16, which has no such pairing;
 * - the tagged rows once more in that order.
 */
#include "public_host.h"

#include <moq/control_d18.h>
#include <moq/vi64.h>
#include <moq/wire.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned failures, checks;
static char label[96];
/* The second pass of the tagged rows runs them with the server keeping each
 * stream's output order alone (ph_options_t.per_stream_order). */
static bool per_stream_pass;
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
} } while (0)
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "SETUP[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
    exit(2); \
} } while (0)

#define CLIENT 0
#define SERVER 1
#define PASSES 400              /* far more than any flow here needs */
#define PROTOCOL_VIOLATION UINT64_C(0x3)

static void name_case(const char *what, moq_version_t version)
{
    (void)snprintf(label, sizeof(label), "%s%s.d%u", what,
                   per_stream_pass ? ".per_stream" : "", (unsigned)version);
}

/* One polled event, reduced to what the flows assert on. Borrowed bytes
 * are copied; a copy cut short is flagged, never silently shortened. */
#define SEEN_BYTES 32
typedef struct seen {
    moq_event_kind_t kind;
    uint64_t group, object, code, status, count, end_group, end_object;
    uint32_t reason, cause, error_domain;
    char payload[SEEN_BYTES];
    size_t payload_len;
    char text[SEEN_BYTES];           /* a reason phrase or a track name */
    size_t text_len;
    bool cut;                        /* a copy did not fit */
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_fetch_t fetch;
    moq_fill_t fill;
} seen_t;

#define SEEN_MAX 64
typedef struct log {
    seen_t e[SEEN_MAX];
    size_t n;
} log_t;

static void copy_buf(seen_t *x, const moq_rcbuf_t *b)
{
    if (!b) return;
    size_t n = moq_rcbuf_len(b);
    if (n > sizeof(x->payload)) { n = sizeof(x->payload); x->cut = true; }
    if (n) memcpy(x->payload, moq_rcbuf_data(b), n);
    x->payload_len = n;
}

static void copy_text(seen_t *x, moq_bytes_t t)
{
    size_t n = t.len;
    if (n > sizeof(x->text)) { n = sizeof(x->text); x->cut = true; }
    if (n) memcpy(x->text, t.data, n);
    x->text_len = n;
}

/* True when the event's record is stamped through `end`. */
static bool covers(const moq_event_t *ev, size_t end)
{
    return (size_t)ev->detail_size >= end;
}

/* Poll everything a side has queued, releasing each event's resources. An
 * event the log has no room for fails the check: evidence is never
 * dropped. */
static void drain(ph_pair_t *p, int side, log_t *out)
{
    moq_session_t *s = ph_session(p, side);
    moq_event_t ev;
    for (int guard = 0; guard < 256; guard++) {
        if (moq_session_poll_events(s, &ev, 1) == 0) break;
        if (out) CHECK(out->n < SEEN_MAX);
        if (out && out->n < SEEN_MAX) {
            seen_t *x = &out->e[out->n++];
            memset(x, 0, sizeof(*x));
            x->kind = ev.kind;
            switch (ev.kind) {
            case MOQ_EVENT_SESSION_CLOSED:
                x->code = ev.u.closed.code;
                copy_text(x, ev.u.closed.reason);
                break;
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                x->sub = ev.u.subscribe_request.sub;
                copy_text(x, ev.u.subscribe_request.track_name);
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                x->sub = ev.u.subscribe_ok.sub;
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                x->sub = ev.u.object_received.sub;
                x->pub = ev.u.object_received.pub;
                x->group = ev.u.object_received.group_id;
                x->object = ev.u.object_received.object_id;
                copy_buf(x, ev.u.object_received.payload);
                break;
            case MOQ_EVENT_SUBGROUP_FINISHED:
                x->sub = ev.u.subgroup_finished.sub;
                x->pub = ev.u.subgroup_finished.pub;
                x->group = ev.u.subgroup_finished.group_id;
                break;
            case MOQ_EVENT_SUBGROUP_RESET:
                x->sub = ev.u.subgroup_reset.sub;
                x->pub = ev.u.subgroup_reset.pub;
                x->group = ev.u.subgroup_reset.group_id;
                x->code = ev.u.subgroup_reset.error_code;
                break;
            case MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED:
                x->sub = ev.u.outgoing_subgroup_terminated.sub;
                x->group = ev.u.outgoing_subgroup_terminated.group_id;
                x->code = ev.u.outgoing_subgroup_terminated.error_code;
                if (covers(&ev, offsetof(moq_outgoing_subgroup_terminated_event_t, cause) +
                                sizeof(moq_outgoing_subgroup_terminal_cause_t)))
                    x->cause = ev.u.outgoing_subgroup_terminated.cause;
                break;
            case MOQ_EVENT_FETCH_CANCELLED:
                x->fetch = ev.u.fetch_cancelled.fetch;
                if (covers(&ev, offsetof(moq_fetch_cancelled_event_t, stream_error_code) +
                                sizeof(uint64_t))) {
                    x->cause = ev.u.fetch_cancelled.cause;
                    x->code = ev.u.fetch_cancelled.stream_error_code;
                }
                break;
            case MOQ_EVENT_FETCH_RESET:
                x->fetch = ev.u.fetch_reset.fetch;
                x->code = ev.u.fetch_reset.error_code;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                x->sub = ev.u.subscribe_done.sub;
                x->status = ev.u.subscribe_done.status_code;
                x->count = ev.u.subscribe_done.stream_count;
                copy_text(x, ev.u.subscribe_done.reason);
                break;
            case MOQ_EVENT_UNSUBSCRIBED:
                x->sub = ev.u.unsubscribed.sub;
                if (covers(&ev, offsetof(moq_unsubscribed_event_t, stream_error_code) +
                                sizeof(uint64_t))) {
                    x->cause = ev.u.unsubscribed.cause;
                    x->code = ev.u.unsubscribed.stream_error_code;
                }
                break;
            case MOQ_EVENT_PUBLISH_REQUEST:
                x->pub = ev.u.publish_request.pub;
                copy_text(x, ev.u.publish_request.track_name);
                break;
            case MOQ_EVENT_PUBLISH_OK:
                x->pub = ev.u.publish_ok.pub;
                break;
            case MOQ_EVENT_PUBLISH_FINISHED:
                x->pub = ev.u.publish_finished.pub;
                x->status = ev.u.publish_finished.status_code;
                x->count = ev.u.publish_finished.stream_count;
                copy_text(x, ev.u.publish_finished.reason);
                if (covers(&ev, offsetof(moq_publish_finished_event_t, cause) +
                                sizeof(moq_request_terminal_cause_t)))
                    x->cause = ev.u.publish_finished.cause;
                break;
            case MOQ_EVENT_FETCH_REQUEST:
                x->fetch = ev.u.fetch_request.fetch;
                copy_text(x, ev.u.fetch_request.track_name);
                x->group = ev.u.fetch_request.start_group;
                x->object = ev.u.fetch_request.start_object;
                x->end_group = ev.u.fetch_request.end_group;
                x->end_object = ev.u.fetch_request.end_object;
                break;
            case MOQ_EVENT_FETCH_OK:
                x->fetch = ev.u.fetch_ok.fetch;
                x->end_group = ev.u.fetch_ok.end_group;
                x->end_object = ev.u.fetch_ok.end_object;
                break;
            case MOQ_EVENT_FETCH_OBJECT:
                x->fetch = ev.u.fetch_object.fetch;
                x->group = ev.u.fetch_object.group_id;
                x->object = ev.u.fetch_object.object_id;
                copy_buf(x, ev.u.fetch_object.payload);
                break;
            case MOQ_EVENT_FETCH_COMPLETE:
                x->fetch = ev.u.fetch_complete.fetch;
                break;
            case MOQ_EVENT_FETCH_ERROR:
                x->fetch = ev.u.fetch_error.fetch;
                x->code = ev.u.fetch_error.error_code;
                break;
            case MOQ_EVENT_FILL_CREATED:
                x->fill = ev.u.fill_created.fill;
                break;
            case MOQ_EVENT_FILL_REQUEST:
                x->fill = ev.u.fill_request.fill;
                break;
            case MOQ_EVENT_FILL_OBJECT:
                x->fill = ev.u.fill_object.fill;
                x->group = ev.u.fill_object.group_id;
                x->object = ev.u.fill_object.object_id;
                copy_buf(x, ev.u.fill_object.payload);
                break;
            case MOQ_EVENT_FILL_TERMINAL:
                x->fill = ev.u.fill_terminal.fill;
                x->sub = ev.u.fill_terminal.sub;
                x->reason = ev.u.fill_terminal.reason;
                x->error_domain = ev.u.fill_terminal.error_domain;
                x->code = ev.u.fill_terminal.error_code;
                break;
            default:
                break;
            }
            CHECK(!x->cut);
        }
        moq_event_cleanup(&ev);
    }
}

static size_t count_kind(const log_t *l, moq_event_kind_t kind)
{
    size_t n = 0;
    for (size_t i = 0; i < l->n; i++)
        if (l->e[i].kind == kind) n++;
    return n;
}

static int index_of(const log_t *l, moq_event_kind_t kind)
{
    for (size_t i = 0; i < l->n; i++)
        if (l->e[i].kind == kind) return (int)i;
    return -1;
}

/* The n-th event of a kind, or -1. */
static int nth_of(const log_t *l, moq_event_kind_t kind, size_t n)
{
    for (size_t i = 0; i < l->n; i++)
        if (l->e[i].kind == kind && n-- == 0) return (int)i;
    return -1;
}

static bool bytes_are(const char *got, size_t got_len, const char *want)
{
    return got_len == strlen(want) && memcmp(got, want, got_len) == 0;
}

static ph_pair_t *open_pair(moq_version_t version, const ph_options_t *o)
{
    ph_pair_t *p = ph_pair_create(version, o);
    REQUIRE(p != NULL);
    REQUIRE(ph_run(p, PASSES));
    REQUIRE(ph_fault(p, CLIENT) == PH_FAULT_NONE);
    REQUIRE(ph_fault(p, SERVER) == PH_FAULT_NONE);
    REQUIRE(moq_session_state(ph_session(p, CLIENT)) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_ESTABLISHED);
    return p;
}

/* The client ends the session; both sides settle; nothing is left behind,
 * the payloads the test created with the pair's allocator included. */
static void close_pair(ph_pair_t *p)
{
    CHECK(moq_session_close(ph_session(p, CLIENT), 0, NULL, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t server = {0};
    drain(p, CLIENT, NULL);
    drain(p, SERVER, &server);
    uint64_t code = UINT64_MAX;
    CHECK(ph_transport_closed(p, CLIENT, &code) && code == 0);
    CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_CLOSED);
    CHECK(count_kind(&server, MOQ_EVENT_SESSION_CLOSED) == 1);
    CHECK(ph_held_outputs(p, CLIENT) == 0 && ph_held_outputs(p, SERVER) == 0);
    CHECK(ph_pair_destroy(p) == 0);
}

typedef struct track {
    moq_subscription_t csub, ssub;
} track_t;

/* A subscription the server accepts. A nonzero timeout_ms is the object
 * delivery timeout the subscriber asks for; draft-16 has one
 * DELIVERY_TIMEOUT, so there it is sent as both values. */
static void subscribe_timed(ph_pair_t *p, track_t *t, const char *name,
                            uint64_t timeout_ms)
{
    memset(t, 0, sizeof(*t));
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    if (timeout_ms) {
        sc.delivery_timeout_flags = MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
        sc.object_delivery_timeout_ms = timeout_ms;
        if (moq_session_version(ph_session(p, CLIENT)) == MOQ_VERSION_DRAFT_16) {
            sc.delivery_timeout_flags |= MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT;
            sc.subgroup_delivery_timeout_ms = timeout_ms;
        }
    }
    REQUIRE(moq_session_subscribe(ph_session(p, CLIENT), &sc, ph_now(p),
                                  &t->csub) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));

    log_t server = {0};
    drain(p, SERVER, &server);
    int at = index_of(&server, MOQ_EVENT_SUBSCRIBE_REQUEST);
    REQUIRE(at >= 0);
    REQUIRE(bytes_are(server.e[at].text, server.e[at].text_len, name));
    t->ssub = server.e[at].sub;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    REQUIRE(moq_session_accept_subscribe(ph_session(p, SERVER), t->ssub, &ac,
                                         ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    log_t client = {0};
    drain(p, CLIENT, &client);
    at = index_of(&client, MOQ_EVENT_SUBSCRIBE_OK);
    REQUIRE(count_kind(&client, MOQ_EVENT_SUBSCRIBE_OK) == 1);
    REQUIRE(moq_subscription_eq(client.e[at].sub, t->csub));
}

static void subscribe(ph_pair_t *p, track_t *t, const char *name)
{
    subscribe_timed(p, t, name, 0);
}

/* An original publisher's new subgroup, so FIRST_OBJECT is set. */
static void first_object_cfg(moq_subgroup_cfg_t *sc, uint64_t group)
{
    moq_subgroup_cfg_init(sc);
    sc->group_id = group;
    sc->first_object = true;
}

static moq_subgroup_handle_t open_group(ph_pair_t *p, const track_t *t,
                                        uint64_t group)
{
    moq_subgroup_cfg_t sc;
    first_object_cfg(&sc, group);
    moq_subgroup_handle_t sg;
    REQUIRE(moq_session_open_subgroup(ph_session(p, SERVER), t->ssub, &sc,
                                      ph_now(p), &sg) == MOQ_OK);
    return sg;
}

/* A payload from the pair's allocator, so a leaked reference shows in
 * ph_pair_destroy(). The caller drops its reference after handing it on. */
static moq_rcbuf_t *payload(ph_pair_t *p, const char *text)
{
    moq_rcbuf_t *b = NULL;
    REQUIRE(moq_rcbuf_create(ph_allocator(p), (const uint8_t *)text,
                             strlen(text), &b) == MOQ_OK);
    return b;
}

static void write_object(ph_pair_t *p, moq_subgroup_handle_t sg,
                         uint64_t object, const char *text)
{
    moq_rcbuf_t *b = payload(p, text);
    REQUIRE(moq_session_write_object(ph_session(p, SERVER), sg, object, b,
                                     ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(b);
}

/* Check the n-th object of the log: its owner, location and bytes. */
static void check_object(const log_t *l, size_t n, moq_subscription_t sub,
                         moq_publication_t pub, uint64_t group,
                         uint64_t object, const char *text)
{
    int at = nth_of(l, MOQ_EVENT_OBJECT_RECEIVED, n);
    CHECK(at >= 0);
    if (at < 0) return;
    const seen_t *x = &l->e[at];
    CHECK(moq_subscription_eq(x->sub, sub));
    CHECK(moq_publication_eq(x->pub, pub));
    CHECK(x->group == group && x->object == object);
    CHECK(bytes_are(x->payload, x->payload_len, text));
}

/* -- Setup ----------------------------------------------------------- */

static void setup(moq_version_t version, size_t write_limit, size_t read_limit)
{
    name_case(write_limit ? "setup.partial" : "setup", version);
    ph_options_t o = {0};
    o.write_limit = write_limit;
    o.read_limit = read_limit;
    ph_pair_t *p = open_pair(version, &o);
    log_t c = {0}, s = {0};
    drain(p, CLIENT, &c);
    drain(p, SERVER, &s);
    CHECK(count_kind(&c, MOQ_EVENT_SETUP_COMPLETE) == 1);
    CHECK(count_kind(&s, MOQ_EVENT_SETUP_COMPLETE) == 1);

    /* Where each side sends control on its own unidirectional stream, each
     * routed exactly one peer stream there by its leading bytes. */
    unsigned want = ph_bidi_control(p) ? 0u : 1u;
    CHECK(ph_counters(p, CLIENT)->routed_control == want);
    CHECK(ph_counters(p, SERVER)->routed_control == want);
    if (want == 1 && read_limit == 1) {
        /* A two-byte stream type read one byte at a time needs more first. */
        CHECK(ph_counters(p, CLIENT)->need_more >= 1);
        CHECK(ph_counters(p, SERVER)->need_more >= 1);
    }
    if (write_limit) {
        CHECK(ph_counters(p, CLIENT)->partial_writes >= 1);
    }
    CHECK(ph_counters(p, CLIENT)->violations == 0);
    CHECK(ph_counters(p, SERVER)->violations == 0);
    close_pair(p);
}

/* -- A subscription with objects -------------------------------------- */

static void publish_subscribe(moq_version_t version, size_t write_limit,
                              size_t read_limit)
{
    name_case(write_limit ? "publish.partial" : "publish", version);
    ph_options_t o = {0};
    o.write_limit = write_limit;
    o.read_limit = read_limit;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");

    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "public host");
    write_object(p, sg, 1, "second");
    REQUIRE(moq_session_close_subgroup(ph_session(p, SERVER), sg, ph_now(p)) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.stream_count = 1;
    dc.reason = MOQ_BYTES_LITERAL("over");
    REQUIRE(moq_session_done_subscribe(ph_session(p, SERVER), t.ssub, &dc,
                                       ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));

    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 2);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_FINISHED) == 1);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    check_object(&c, 0, t.csub, MOQ_PUBLICATION_INVALID, 0, 0, "public host");
    check_object(&c, 1, t.csub, MOQ_PUBLICATION_INVALID, 0, 1, "second");
    int first = index_of(&c, MOQ_EVENT_OBJECT_RECEIVED);
    int finished = index_of(&c, MOQ_EVENT_SUBGROUP_FINISHED);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    /* The subscriber sees the stream end before the done. Stream Count would
     * hold the done back anyway, so this is not proof of the order the
     * publisher's host wrote them in. */
    CHECK(first >= 0 && first < finished && finished < done);
    if (finished >= 0) {
        CHECK(moq_subscription_eq(c.e[finished].sub, t.csub));
        CHECK(c.e[finished].group == 0);
    }
    if (done >= 0) {
        CHECK(moq_subscription_eq(c.e[done].sub, t.csub));
        CHECK(c.e[done].status == dc.status_code && c.e[done].count == 1);
        CHECK(bytes_are(c.e[done].text, c.e[done].text_len, "over"));
    }
    if (write_limit) CHECK(ph_counters(p, SERVER)->partial_writes >= 1);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    CHECK(ph_counters(p, CLIENT)->violations == 0);
    CHECK(ph_counters(p, CLIENT)->routed_data == 1);
    close_pair(p);
}

/* -- A publication the server opens ------------------------------------ */

/* The server starts the request: a PUBLISH, on a request stream it opens
 * where the profile has them. The client answers on that stream; objects
 * and the publication's end arrive with its identity. */
static void server_publication(moq_version_t version)
{
    name_case("server-publication", version);
    ph_pair_t *p = open_pair(version, NULL);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);

    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    pc.track_name = MOQ_BYTES_LITERAL("pub");
    moq_publication_t spub;
    REQUIRE(moq_session_publish(ph_session(p, SERVER), &pc, ph_now(p), &spub) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t c = {0}, s = {0};
    drain(p, CLIENT, &c);
    int at = index_of(&c, MOQ_EVENT_PUBLISH_REQUEST);
    REQUIRE(at >= 0);
    CHECK(count_kind(&c, MOQ_EVENT_PUBLISH_REQUEST) == 1);
    CHECK(bytes_are(c.e[at].text, c.e[at].text_len, "pub"));
    moq_publication_t cpub = c.e[at].pub;
    REQUIRE(moq_publication_is_valid(cpub));
    if (ph_uses_request_streams(p)) {
        /* The request arrived on a stream the server opened. */
        moq_stream_ref_t none;
        CHECK(!ph_only_open_local_request(p, CLIENT, &none));
        CHECK(ph_only_open_local_request(p, SERVER, &none));
    }

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    REQUIRE(moq_session_accept_publish(ph_session(p, CLIENT), cpub, &ac, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    drain(p, SERVER, &s);
    at = index_of(&s, MOQ_EVENT_PUBLISH_OK);
    CHECK(count_kind(&s, MOQ_EVENT_PUBLISH_OK) == 1);
    if (at >= 0) CHECK(moq_publication_eq(s.e[at].pub, spub));

    moq_subgroup_cfg_t sc;
    first_object_cfg(&sc, 5);
    moq_subgroup_handle_t sg;
    REQUIRE(moq_session_open_pub_subgroup(ph_session(p, SERVER), spub, &sc, ph_now(p),
                                          &sg) == MOQ_OK);
    write_object(p, sg, 0, "p-zero");
    write_object(p, sg, 1, "p-one");
    REQUIRE(moq_session_close_subgroup(ph_session(p, SERVER), sg, ph_now(p)) == MOQ_OK);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    fc.stream_count = 1;
    fc.reason = MOQ_BYTES_LITERAL("end");
    REQUIRE(moq_session_finish_publish(ph_session(p, SERVER), spub, &fc, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));

    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 2);
    check_object(&c, 0, MOQ_SUBSCRIPTION_INVALID, cpub, 5, 0, "p-zero");
    check_object(&c, 1, MOQ_SUBSCRIPTION_INVALID, cpub, 5, 1, "p-one");
    at = index_of(&c, MOQ_EVENT_SUBGROUP_FINISHED);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_FINISHED) == 1);
    if (at >= 0) CHECK(moq_publication_eq(c.e[at].pub, cpub) && c.e[at].group == 5);
    at = index_of(&c, MOQ_EVENT_PUBLISH_FINISHED);
    CHECK(count_kind(&c, MOQ_EVENT_PUBLISH_FINISHED) == 1);
    if (at >= 0) {
        CHECK(moq_publication_eq(c.e[at].pub, cpub));
        CHECK(c.e[at].status == fc.status_code && c.e[at].count == 1);
        CHECK(bytes_are(c.e[at].text, c.e[at].text_len, "end"));
        CHECK(c.e[at].cause == MOQ_REQUEST_TERMINAL_NONE);
    }
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- A fetch ------------------------------------------------------------ */

/* A second request family through the same contracts: the request and its
 * answer on a request stream (or the control stream), the objects on a
 * stream of their own. */
static void fetch_lifecycle(moq_version_t version)
{
    name_case("fetch", version);
    ph_pair_t *p = open_pair(version, NULL);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);

    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    fc.track_name = MOQ_BYTES_LITERAL("past");
    fc.end_group = 0;
    fc.end_object = 3;
    moq_fetch_t cfetch;
    REQUIRE(moq_session_fetch(ph_session(p, CLIENT), &fc, ph_now(p), &cfetch) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t s = {0}, c = {0};
    drain(p, SERVER, &s);
    int at = index_of(&s, MOQ_EVENT_FETCH_REQUEST);
    REQUIRE(at >= 0);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_REQUEST) == 1);
    CHECK(bytes_are(s.e[at].text, s.e[at].text_len, "past"));
    CHECK(s.e[at].group == 0 && s.e[at].object == 0);
    CHECK(s.e[at].end_group == 0 && s.e[at].end_object == 3);
    moq_fetch_t sfetch = s.e[at].fetch;

    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 3;
    REQUIRE(moq_session_accept_fetch(ph_session(p, SERVER), sfetch, &ac, ph_now(p)) == MOQ_OK);
    static const char *const body[] = {"f-zero", "f-one"};
    for (uint64_t i = 0; i < 2; i++) {
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = 0;
        oc.object_id = i;
        oc.publisher_priority = 100;
        oc.payload = payload(p, body[i]);
        REQUIRE(moq_session_write_fetch_object(ph_session(p, SERVER), sfetch, &oc,
                                               ph_now(p)) == MOQ_OK);
        moq_rcbuf_decref(oc.payload);
    }
    REQUIRE(moq_session_end_fetch(ph_session(p, SERVER), sfetch, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));

    drain(p, CLIENT, &c);
    at = index_of(&c, MOQ_EVENT_FETCH_OK);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_OK) == 1);
    if (at >= 0) {
        CHECK(moq_fetch_eq(c.e[at].fetch, cfetch));
        CHECK(c.e[at].end_group == 0 && c.e[at].end_object == 3);
    }
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_OBJECT) == 2);
    for (size_t i = 0; i < 2; i++) {
        int k = nth_of(&c, MOQ_EVENT_FETCH_OBJECT, i);
        CHECK(k >= 0);
        if (k < 0) continue;
        CHECK(moq_fetch_eq(c.e[k].fetch, cfetch));
        CHECK(c.e[k].group == 0 && c.e[k].object == i);
        CHECK(bytes_are(c.e[k].payload, c.e[k].payload_len, body[i]));
    }
    int complete = index_of(&c, MOQ_EVENT_FETCH_COMPLETE);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 1);
    if (complete >= 0) {
        CHECK(moq_fetch_eq(c.e[complete].fetch, cfetch));
        CHECK(complete > nth_of(&c, MOQ_EVENT_FETCH_OBJECT, 1));
    }
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_ERROR) == 0);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_RESET) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- Event pressure --------------------------------------------------- */

static void event_pressure(moq_version_t version)
{
    name_case("event-pressure", version);
    ph_options_t o = {0};
    o.max_events[CLIENT] = 2;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");

    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    static const char *const text[] = {"a", "b", "c", "d", "e", "f"};
    const size_t objects = sizeof(text) / sizeof(text[0]);
    for (uint64_t i = 0; i < objects; i++) write_object(p, sg, i, text[i]);
    REQUIRE(moq_session_close_subgroup(ph_session(p, SERVER), sg, ph_now(p)) == MOQ_OK);

    /* The subscriber has room for two events. Every object still arrives
     * once, in order: a delivery the session kept is resumed, never fed
     * again. */
    log_t c = {0};
    bool finished = false;
    for (int round = 0; round < 64 && !finished; round++) {
        (void)ph_run(p, PASSES);
        drain(p, CLIENT, &c);
        finished = count_kind(&c, MOQ_EVENT_SUBGROUP_FINISHED) == 1;
    }
    CHECK(finished);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == objects);
    uint64_t next = 0;
    for (size_t i = 0; i < c.n; i++) {
        if (c.e[i].kind != MOQ_EVENT_OBJECT_RECEIVED) continue;
        /* An extra object is a failure, never a read past the table. */
        CHECK(next < objects);
        if (next < objects) {
            CHECK(c.e[i].object == next);
            CHECK(moq_subscription_eq(c.e[i].sub, t.csub));
            CHECK(bytes_are(c.e[i].payload, c.e[i].payload_len, text[next]));
        }
        next++;
    }
    CHECK(ph_counters(p, CLIENT)->owned_retries >= 1);
    CHECK(ph_counters(p, CLIENT)->refused_inputs == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- A refused stream is offered again -------------------------------- */

static void refused_then_admitted(moq_version_t version)
{
    name_case("refused-stream", version);
    ph_options_t o = {0};
    o.max_data_streams[CLIENT] = 1;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");

    moq_subgroup_handle_t g0 = open_group(p, &t, 0);
    write_object(p, g0, 0, "zero");
    CHECK(ph_run(p, PASSES));
    moq_subgroup_handle_t g1 = open_group(p, &t, 1);
    write_object(p, g1, 0, "one");
    CHECK(ph_run(p, PASSES));

    /* The subscriber has one receive entry and group 0 holds it: group 1's
     * stream is not admitted, and the host keeps its bytes. */
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);
    CHECK(ph_counters(p, CLIENT)->refused_inputs == 1);
    CHECK(!moq_session_can_admit_data_stream(ph_session(p, CLIENT)));

    /* Group 0 ends; its entry frees; the kept bytes are taken unchanged. */
    REQUIRE(moq_session_close_subgroup(ph_session(p, SERVER), g0, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    drain(p, CLIENT, &c);
    CHECK(ph_counters(p, CLIENT)->redelivered == 1);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 2);
    check_object(&c, 0, t.csub, MOQ_PUBLICATION_INVALID, 0, 0, "zero");
    check_object(&c, 1, t.csub, MOQ_PUBLICATION_INVALID, 1, 0, "one");
    REQUIRE(moq_session_close_subgroup(ph_session(p, SERVER), g1, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_FINISHED) == 2);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- Retirement with no terminal from the peer ------------------------ */

/* The subscriber drops a subscription whose stream is still open. Its
 * transport drops the receive side once the stop is accepted, so neither a
 * FIN nor a RESET ever reaches it: the host reports the retirement, and
 * the session lets go of the stream and, where the request had its own
 * stream, of that one too. */
static void retire_after_stop(moq_version_t version)
{
    name_case("retire", version);
    ph_options_t o = {0};
    o.drop_after_stop = true;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_stream_ref_t request = {0};
    bool has_request = ph_uses_request_streams(p);
    CHECK(ph_only_open_local_request(p, CLIENT, &request) == has_request);

    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "kept open");
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);
    check_object(&c, 0, t.csub, MOQ_PUBLICATION_INVALID, 0, 0, "kept open");
    moq_stream_ref_t data = {0};
    REQUIRE(ph_only_open_peer_data_stream(p, CLIENT, &data));
    CHECK(moq_session_has_transport_stream(ph_session(p, CLIENT), data));

    unsigned resets_before = ph_counters(p, CLIENT)->peer_resets;
    unsigned fins_before = ph_counters(p, CLIENT)->peer_fins;
    REQUIRE(moq_session_unsubscribe(ph_session(p, CLIENT), t.csub, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));

    CHECK(ph_counters(p, CLIENT)->data_retired == 1);
    CHECK(ph_counters(p, CLIENT)->retire_stale == 0);
    CHECK(!moq_session_has_transport_stream(ph_session(p, CLIENT), data));
    CHECK(moq_session_can_admit_data_stream(ph_session(p, CLIENT)));
    /* Nothing of the peer's reached the subscriber after its stop. */
    CHECK(ph_counters(p, CLIENT)->peer_resets == resets_before);
    CHECK(ph_counters(p, CLIENT)->peer_fins == fins_before);
    CHECK(ph_counters(p, SERVER)->peer_stops >= 1);
    if (has_request) {
        CHECK(ph_counters(p, CLIENT)->bidi_retired == 1);
        CHECK(!moq_session_has_transport_stream(ph_session(p, CLIENT), request));
    } else {
        CHECK(ph_counters(p, CLIENT)->bidi_retired == 0);
    }
    /* No peer terminal is fabricated for the stream, and the session lives
     * on. */
    log_t after = {0};
    drain(p, CLIENT, &after);
    CHECK(count_kind(&after, MOQ_EVENT_SUBGROUP_FINISHED) == 0);
    CHECK(count_kind(&after, MOQ_EVENT_SUBGROUP_RESET) == 0);
    CHECK(moq_session_state(ph_session(p, CLIENT)) == MOQ_SESS_ESTABLISHED);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A cancelled fill's stream is retired the same way, and the retirement is
 * what lets the session reclaim the fill. */
static void retire_fill(moq_version_t version)
{
    name_case("retire-fill", version);
    ph_options_t o = {0};
    o.drop_after_stop = true;
    ph_pair_t *p = open_pair(version, &o);
    if (!moq_session_supports_fill(ph_session(p, CLIENT))) {
        close_pair(p);
        return;                  /* this profile has no fills */
    }
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = MOQ_BYTES_LITERAL("f");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.fill = &fill;
    moq_subscription_t csub;
    REQUIRE(moq_session_subscribe(ph_session(p, CLIENT), &sc, ph_now(p), &csub) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    log_t s = {0};
    drain(p, SERVER, &s);
    int at = index_of(&s, MOQ_EVENT_SUBSCRIBE_REQUEST);
    REQUIRE(at >= 0);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 2;
    ac.largest_object = 0;
    REQUIRE(moq_session_accept_subscribe(ph_session(p, SERVER), s.e[at].sub, &ac,
                                         ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    s.n = 0;
    drain(p, SERVER, &s);
    int created = index_of(&c, MOQ_EVENT_FILL_CREATED);
    int requested = index_of(&s, MOQ_EVENT_FILL_REQUEST);
    REQUIRE(created >= 0 && requested >= 0);
    moq_fill_t cfill = c.e[created].fill;
    moq_fill_t sfill = s.e[requested].fill;

    REQUIRE(moq_session_open_fill(ph_session(p, SERVER), sfill, ph_now(p)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 5;
    oc.payload = payload(p, "a");
    REQUIRE(moq_session_write_fill_object(ph_session(p, SERVER), sfill, &oc,
                                          ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    REQUIRE(ph_run(p, PASSES));
    c.n = 0;
    drain(p, CLIENT, &c);
    at = index_of(&c, MOQ_EVENT_FILL_OBJECT);
    CHECK(count_kind(&c, MOQ_EVENT_FILL_OBJECT) == 1);
    if (at >= 0) {
        CHECK(moq_fill_eq(c.e[at].fill, cfill));
        CHECK(c.e[at].group == 0 && c.e[at].object == 0);
        CHECK(bytes_are(c.e[at].payload, c.e[at].payload_len, "a"));
    }
    moq_stream_ref_t data = {0};
    REQUIRE(ph_only_open_peer_data_stream(p, CLIENT, &data));

    REQUIRE(moq_session_cancel_fill(ph_session(p, CLIENT), cfill, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_FILL_TERMINAL) == 1);
    int terminal = index_of(&c, MOQ_EVENT_FILL_TERMINAL);
    if (terminal >= 0) {
        CHECK(moq_fill_eq(c.e[terminal].fill, cfill));
        CHECK(moq_subscription_eq(c.e[terminal].sub, csub));
        CHECK(c.e[terminal].reason == MOQ_FILL_LOCAL_CANCELLED);
        CHECK(c.e[terminal].error_domain == MOQ_FILL_ERROR_NONE);
        CHECK(c.e[terminal].code == 0);
    }
    CHECK(ph_counters(p, CLIENT)->data_retired == 1);
    CHECK(!moq_session_has_transport_stream(ph_session(p, CLIENT), data));
    /* The fill ended once: its handle no longer names anything. */
    CHECK(moq_session_cancel_fill(ph_session(p, CLIENT), cfill, ph_now(p)) ==
          MOQ_ERR_STALE_HANDLE);
    CHECK(ph_run(p, PASSES));
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_FILL_TERMINAL) == 0);
    CHECK(moq_session_state(ph_session(p, CLIENT)) == MOQ_SESS_ESTABLISHED);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- A STOP on a request stream this side opened ----------------------- */

/* The publisher's transport stops the subscriber's request stream. The
 * subscriber's transport answers with a reset of its half, and its session
 * is told too: a reset of the stream alone would leave the subscription
 * waiting for an outcome that never comes. */
static void stop_on_local_request(moq_version_t version)
{
    name_case("stop-local-request", version);
    ph_pair_t *p = open_pair(version, NULL);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_stream_ref_t request = {0};
    REQUIRE(ph_only_open_local_request(p, CLIENT, &request));

    REQUIRE(ph_peer_stops_request(p, CLIENT, request, 0x77));
    CHECK(ph_run(p, PASSES));
    CHECK(ph_counters(p, CLIENT)->request_stops == 1);
    log_t c = {0};
    drain(p, CLIENT, &c);
    int at = index_of(&c, MOQ_EVENT_UNSUBSCRIBED);
    CHECK(count_kind(&c, MOQ_EVENT_UNSUBSCRIBED) == 1);
    if (at >= 0) {
        CHECK(moq_subscription_eq(c.e[at].sub, t.csub));
        CHECK(c.e[at].cause == MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING);
        CHECK(c.e[at].code == 0x77);
    }
    /* The requester's outcome is settled: the handle names nothing. */
    CHECK(moq_session_unsubscribe(ph_session(p, CLIENT), t.csub, ph_now(p)) ==
          MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_state(ph_session(p, CLIENT)) == MOQ_SESS_ESTABLISHED);
    drain(p, SERVER, NULL);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- Streams that end before their type is complete -------------------- */

typedef struct prefix_case {
    const char *name;
    uint8_t     bytes[4];
    size_t      len;
    int         end;            /* PH_END_* */
    bool        closes;         /* the victim closes with 0x3 */
} prefix_case_t;

/* The server's peer transport opens a unidirectional stream with raw bytes.
 * A stream that ends inside or before its type gets no route and never
 * stands for the control channel: a reset or an empty FIN only ends it; a
 * FIN after part of the type truncates it, a protocol violation; a complete
 * unknown type is the separate unknown-type case. On draft-16 every
 * unidirectional stream is data and the session parses its type itself,
 * so the same cases meet the session's own rules there. */
static void truncated_types(moq_version_t version)
{
    uint8_t setup_type[9];
    size_t setup_len = moq_vi64_encode(UINT64_C(0x2F00), setup_type, sizeof(setup_type));
    REQUIRE(setup_len == 2);
    prefix_case_t cases[] = {
        {"empty-reset", {0}, 0, PH_END_RESET, false},
        {"empty-fin", {0}, 0, PH_END_FIN, false},
        {"prefix-reset", {setup_type[0]}, 1, PH_END_RESET, false},
        {"prefix-fin", {setup_type[0]}, 1, PH_END_FIN, true},
        {"unknown-type", {0x06}, 1, PH_END_NONE, true},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const prefix_case_t *k = &cases[i];
        /* A stream type spans more than one byte only where the profile
         * routes streams by vi64 type. */
        bool multibyte = !(version == MOQ_VERSION_DRAFT_16);
        if (!multibyte && k->len == 1 && k->bytes[0] != 0x06) continue;
        char what[48];
        (void)snprintf(what, sizeof(what), "stream-type.%s", k->name);
        name_case(what, version);
        ph_pair_t *p = open_pair(version, NULL);
        drain(p, CLIENT, NULL);
        drain(p, SERVER, NULL);
        ph_counters_t before = *ph_counters(p, SERVER);

        REQUIRE(ph_peer_opens_uni(p, SERVER, k->bytes, k->len, k->end, 0x9));
        CHECK(ph_run(p, PASSES));
        const ph_counters_t *n = ph_counters(p, SERVER);
        /* Never taken for the control channel. */
        CHECK(n->routed_control == before.routed_control);
        CHECK(n->control_ended == before.control_ended);
        CHECK(ph_fault(p, SERVER) == PH_FAULT_NONE);
        if (k->closes) {
            CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_CLOSED);
            uint64_t code = UINT64_MAX;
            CHECK(ph_transport_closed(p, SERVER, &code) && code == PROTOCOL_VIOLATION);
            log_t s = {0};
            drain(p, SERVER, &s);
            int at = index_of(&s, MOQ_EVENT_SESSION_CLOSED);
            CHECK(count_kind(&s, MOQ_EVENT_SESSION_CLOSED) == 1);
            if (at >= 0) CHECK(s.e[at].code == PROTOCOL_VIOLATION);
            /* On draft-16 the session closed itself; elsewhere this host
             * closed it on the route's answer. */
            CHECK(n->violations == before.violations + (multibyte ? 1u : 0u));
            drain(p, CLIENT, NULL);
            CHECK(ph_pair_destroy(p) == 0);
            continue;
        }
        CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_ESTABLISHED);
        CHECK(!ph_transport_closed(p, SERVER, NULL));
        CHECK(n->violations == before.violations);
        if (multibyte || k->len == 0) {
            /* Ended without a route, or (draft-16, empty FIN) fed to the
             * data path, which ends a stream that carried no byte. */
            if (multibyte || k->end == PH_END_RESET)
                CHECK(n->ended_unrouted == before.ended_unrouted + 1);
        }
        if (multibyte && k->len == 1) CHECK(n->need_more > before.need_more);
        log_t s = {0};
        drain(p, SERVER, &s);
        CHECK(count_kind(&s, MOQ_EVENT_SESSION_CLOSED) == 0);
        close_pair(p);
    }
}

/* -- The end of a control stream -------------------------------------- */

/* `victim` sees the control stream its peer sends on end, by FIN or by
 * RESET. It closes as a protocol violation and closes the transport with
 * that code, whatever code the peer's reset carried; the peer learns it
 * from the transport. */
static void control_stream_end(moq_version_t version, int victim, bool by_reset)
{
    char what[48];
    (void)snprintf(what, sizeof(what), "control-end.%s.%s",
                   victim == CLIENT ? "client" : "server",
                   by_reset ? "reset" : "fin");
    name_case(what, version);
    ph_pair_t *p = open_pair(version, NULL);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    int other = 1 - victim;

    if (by_reset) REQUIRE(ph_peer_resets_control(p, victim, 0x55));
    else REQUIRE(ph_peer_finishes_control(p, victim));
    CHECK(ph_run(p, PASSES));

    CHECK(ph_counters(p, victim)->control_ended == 1);
    CHECK(moq_session_state(ph_session(p, victim)) == MOQ_SESS_CLOSED);
    uint64_t code = UINT64_MAX;
    CHECK(ph_transport_closed(p, victim, &code) && code == PROTOCOL_VIOLATION);
    log_t v = {0}, peer = {0};
    drain(p, victim, &v);
    drain(p, other, &peer);
    int at = index_of(&v, MOQ_EVENT_SESSION_CLOSED);
    CHECK(count_kind(&v, MOQ_EVENT_SESSION_CLOSED) == 1);
    if (at >= 0) CHECK(v.e[at].code == PROTOCOL_VIOLATION);
    /* The peer's session closes with what the transport reported. */
    CHECK(moq_session_state(ph_session(p, other)) == MOQ_SESS_CLOSED);
    at = index_of(&peer, MOQ_EVENT_SESSION_CLOSED);
    CHECK(count_kind(&peer, MOQ_EVENT_SESSION_CLOSED) == 1);
    if (at >= 0) CHECK(peer.e[at].code == PROTOCOL_VIOLATION);
    CHECK(!ph_transport_closed(p, other, NULL));
    CHECK(ph_held_outputs(p, CLIENT) == 0 && ph_held_outputs(p, SERVER) == 0);
    CHECK(ph_pair_destroy(p) == 0);
}

/* -- Tagged host mode -------------------------------------------------- */

static ph_options_t tagged_server(void)
{
    ph_options_t o;
    memset(&o, 0, sizeof(o));
    o.tagged[SERVER] = true;
    o.per_stream_order[SERVER] = per_stream_pass;
    return o;
}

/* The mode is enabled on a pristine session (the pair enables it after
 * create and transport binding, before start), only there, for good, and
 * it takes the raw action poll away. */
static void tagged_enable(moq_version_t version)
{
    name_case("tagged.enable", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    moq_session_t *srv = ph_session(p, SERVER), *cli = ph_session(p, CLIENT);
    CHECK(moq_session_tagged_output(srv));
    CHECK(moq_session_enable_tagged_output(srv) == MOQ_OK);   /* repeat */
    /* An advanced session cannot enable it, and nothing changes. */
    CHECK(!moq_session_tagged_output(cli));
    CHECK(moq_session_enable_tagged_output(cli) == MOQ_ERR_WRONG_STATE);
    CHECK(!moq_session_tagged_output(cli));
    moq_action_t a;
    size_t n = 7;
    CHECK(moq_session_poll_actions_ex(cli, &a, 1, sizeof(a), &n) == MOQ_OK);
    CHECK(n == 0);                       /* quiescent: nothing was queued */
    if (n == 1) moq_action_cleanup(&a);
    /* In the mode the raw poll transfers nothing. */
    n = 7;
    CHECK(moq_session_poll_actions_ex(srv, &a, 1, sizeof(a), &n) == MOQ_ERR_WRONG_STATE);
    CHECK(n == 0);
    moq_action_tag_t tag;
    n = 7;
    CHECK(moq_session_poll_actions_tagged(cli, &a, sizeof(a), &tag, sizeof(tag), 1, &n)
          == MOQ_ERR_WRONG_STATE);
    CHECK(n == 0);
    /* Overlapping arrays are refused before anything else. */
    n = 7;
    CHECK(moq_session_poll_actions_tagged(srv, &a, sizeof(a), &a, sizeof(tag), 1, &n)
          == MOQ_ERR_INVAL);
    CHECK(n == 0);
    CHECK(moq_session_poll_actions_tagged(srv, &a, sizeof(a), &tag,
                                          MOQ_ACTION_TAG_V0_SIZE - 1, 1, &n)
          == MOQ_ERR_INVAL);
    CHECK(ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* The session counts the streams it opened: one opened stream is a Stream
 * Count of 1 whatever the application passes; none opened is 0. */
static void tagged_stream_count(moq_version_t version, bool open_one)
{
    name_case(open_one ? "tagged.count.one" : "tagged.count.zero", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    if (open_one) {
        moq_subgroup_handle_t sg = open_group(p, &t, 0);
        write_object(p, sg, 0, "tagged");
        REQUIRE(moq_session_close_subgroup(ph_session(p, SERVER), sg, ph_now(p)) == MOQ_OK);
        /* The owner's final message waits for the open's report. */
        CHECK(ph_run(p, PASSES));
    }
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.stream_count = 99;                /* ignored in the mode */
    REQUIRE(moq_session_done_subscribe(ph_session(p, SERVER), t.ssub, &dc,
                                       ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    if (done >= 0) CHECK(c.e[done].count == (open_one ? 1u : 0u));
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == (open_one ? 1u : 0u));
    CHECK(ph_counters(p, SERVER)->opens_reported == (open_one ? 1u : 0u));
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A subgroup reset before its header ever left the session: the claim
 * settles it as UNOPENED, no stream reaches the transport, and the count
 * stays exact at 0. */
static void tagged_unopened(moq_version_t version)
{
    name_case("tagged.unopened", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "never sent");
    REQUIRE(moq_session_reset_subgroup(srv, sg, 0x7, ph_now(p)) == MOQ_OK);
    moq_data_reset_offer_t off;
    size_t n = 0;
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK);
    CHECK(n == 1);
    if (n == 1) {
        CHECK(off.state == MOQ_DATA_RESET_UNOPENED);
        CHECK(off.error_code == 0x7);
        /* The offer has no effect: offering again gives the same. */
        moq_data_reset_offer_t again;
        CHECK(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK &&
              n == 1 && again.stream_ref._v == off.stream_ref._v &&
              again.error_code == off.error_code && again.state == off.state);
    }
    /* Until the claim settles the open, the owner's final message waits. */
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    CHECK(moq_session_done_subscribe(srv, t.ssub, &dc, ph_now(p)) == MOQ_ERR_WOULD_BLOCK);
    CHECK(ph_run(p, PASSES));
    REQUIRE(moq_session_done_subscribe(srv, t.ssub, &dc, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    const ph_counters_t *sn = ph_counters(p, SERVER);
    CHECK(sn->claims == 1 && sn->claims_settled == 1 && sn->commits == 0);
    /* The settled reset left the session with its stream: never offered
     * or polled again. */
    size_t again_n = 9;
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &again_n) == MOQ_OK &&
          again_n == 0);
    CHECK(sn->opens_reported == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == 0);
    log_t c = {0};
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(done >= 0 && c.e[done].count == 0);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 0);
    CHECK(ph_counters(p, CLIENT)->routed_data == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A STOP the session took is a delivery fact apart from the reset's first
 * cause. An opened stream is reset by the application with 0x9; its reset
 * is offered READY. WAIVED is refused while the session knows of no STOP
 * of the stream: before any, after another stream's, and after a refused
 * report. Once the STOP of the stream itself is taken (RESET_FOLLOWS), the
 * same offer, with the application's code and cause, is claimed WAIVED
 * and committed once. */
static void tagged_waived_after_stop(moq_version_t version)
{
    name_case("tagged.waived_after_stop", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "first");
    CHECK(ph_run(p, PASSES));
    REQUIRE(ph_counters(p, SERVER)->opens_reported == 1);
    REQUIRE(moq_session_reset_subgroup(srv, sg, 0x9, ph_now(p)) == MOQ_OK);

    moq_data_reset_offer_t off;
    size_t n = 0;
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    REQUIRE(off.state == MOQ_DATA_RESET_READY && off.error_code == 0x9);
    moq_data_reset_claim_t cl;
    CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_WAIVED,
                                       &cl, sizeof(cl)) == MOQ_ERR_WRONG_STATE);
    /* Another stream's STOP, and a report the session refuses, record
     * nothing for this one. */
    moq_data_end_result_t end;
    const moq_stream_ref_t other = moq_stream_ref_from_u64(off.stream_ref._v + 1000);
    CHECK(moq_session_on_data_stop_ex(srv, other, 0x2A, ph_now(p), &end,
                                      sizeof(end)) == MOQ_OK);
    CHECK(end.outcome == MOQ_DATA_END_NO_RESET);
    CHECK(moq_session_on_data_stop_ex(srv, off.stream_ref, MOQ_QUIC_VARINT_MAX + 1,
                                      ph_now(p), &end, sizeof(end)) == MOQ_ERR_INVAL);
    CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_WAIVED,
                                       &cl, sizeof(cl)) == MOQ_ERR_WRONG_STATE);

    memset(&end, 0, sizeof(end));
    CHECK(moq_session_on_data_stop_ex(srv, off.stream_ref, 0x2A, ph_now(p), &end,
                                      sizeof(end)) == MOQ_OK);
    CHECK(end.outcome == MOQ_DATA_END_RESET_FOLLOWS && end.flags == 0);
    moq_data_reset_offer_t again;
    REQUIRE(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK && n == 1);
    CHECK(again.state == MOQ_DATA_RESET_READY);
    CHECK(again.stream_ref._v == off.stream_ref._v && again.owner._v == off.owner._v);
    CHECK(again.error_code == 0x9 && again.cause == off.cause);
    CHECK(again.reliable_size == off.reliable_size);
    memset(&cl, 0, sizeof(cl));
    CHECK(moq_session_claim_data_reset(srv, &again, sizeof(again), MOQ_DATA_PREFIX_WAIVED,
                                       &cl, sizeof(cl)) == MOQ_OK);
    CHECK(!(cl.flags & MOQ_DATA_RESET_CLAIM_SETTLED));
    CHECK(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK &&
          n == 1 && again.state == MOQ_DATA_RESET_CLAIMED && again.error_code == 0x9);
    CHECK(moq_session_commit_data_reset(srv, &off, sizeof(off)) == MOQ_OK);
    CHECK(moq_session_commit_data_reset(srv, &off, sizeof(off)) == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK && n == 0);
    CHECK(ph_run(p, PASSES));
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* The same fact for a reset still owed on its subgroup. The host reports
 * the expiry of held bytes of one subgroup while another subgroup's output
 * fills the action queue: the first latches DELIVERY_TIMEOUT, and its reset
 * and terminal event stay owed on the subgroup, offered READY with that
 * code and cause. WAIVED is refused until the peer's STOP of the stream is
 * taken; the reset stays owed through it. Then the same offer is claimed
 * WAIVED, which commits the terminal event once, and the claim commits
 * once. */
static void tagged_waived_after_expiry(moq_version_t version)
{
    name_case("tagged.waived_after_expiry", version);
    ph_options_t o = tagged_server();
    o.max_actions[SERVER] = 8;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t a = open_group(p, &t, 0);
    write_object(p, a, 0, "late");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref;
    REQUIRE(ph_only_open_local_data(p, SERVER, &ref));
    drain(p, SERVER, NULL);
    moq_subgroup_handle_t b = open_group(p, &t, 1);
    bool full = false;
    for (uint64_t obj = 0; obj < 16 && !full; obj++) {
        moq_rcbuf_t *buf = payload(p, "fill");
        moq_result_t rc = moq_session_write_object(srv, b, obj, buf, ph_now(p));
        moq_rcbuf_decref(buf);
        REQUIRE(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        full = rc == MOQ_ERR_WOULD_BLOCK;
    }
    REQUIRE(full);
    moq_data_end_result_t end;
    REQUIRE(moq_session_on_held_data_expired(srv, ref, ph_now(p), &end, sizeof(end)) ==
            MOQ_OK);
    REQUIRE(end.outcome == MOQ_DATA_END_RESET_FOLLOWS);

    moq_data_reset_offer_t off;
    size_t n = 0;
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    REQUIRE(off.state == MOQ_DATA_RESET_READY && off.stream_ref._v == ref._v);
    CHECK(off.error_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(off.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    moq_data_reset_claim_t cl;
    CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_WAIVED,
                                       &cl, sizeof(cl)) == MOQ_ERR_WRONG_STATE);
    memset(&end, 0, sizeof(end));
    CHECK(moq_session_on_data_stop_ex(srv, ref, 0x2A, ph_now(p), &end, sizeof(end)) ==
          MOQ_OK);
    CHECK(end.outcome == MOQ_DATA_END_RESET_FOLLOWS);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) == 0);   /* still owed */
    moq_data_reset_offer_t again;
    REQUIRE(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK && n == 1);
    CHECK(again.state == MOQ_DATA_RESET_READY && again.stream_ref._v == ref._v);
    CHECK(again.error_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(again.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    memset(&cl, 0, sizeof(cl));
    CHECK(moq_session_claim_data_reset(srv, &again, sizeof(again), MOQ_DATA_PREFIX_WAIVED,
                                       &cl, sizeof(cl)) == MOQ_OK);
    CHECK(cl.flags == MOQ_DATA_RESET_CLAIM_EVENT_COMMITTED);
    CHECK(moq_session_commit_data_reset(srv, &again, sizeof(again)) == MOQ_OK);
    CHECK(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK && n == 0);
    s.n = 0;
    drain(p, SERVER, &s);
    int ev = index_of(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    CHECK(count_kind(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) == 1);
    if (ev >= 0) {
        CHECK(s.e[ev].code == MOQ_RESET_DELIVERY_TIMEOUT);
        CHECK(s.e[ev].cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    }
    CHECK(ph_run(p, PASSES));
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* An opened stream reset by the application: claimed HANDED, refused by
 * the transport twice, written on the third try and committed once. The
 * claim holds the reset meanwhile: it is offered again as CLAIMED and no
 * poll hands it out. WAIVED is refused, with no effect, when the peer
 * never stopped the stream. */
static void tagged_claim_roundtrip(moq_version_t version)
{
    name_case("tagged.claim", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "first");
    CHECK(ph_run(p, PASSES));
    CHECK(ph_counters(p, SERVER)->opens_reported == 1);
    REQUIRE(moq_session_reset_subgroup(srv, sg, 0x9, ph_now(p)) == MOQ_OK);

    moq_data_reset_offer_t off;
    size_t n = 0;
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    moq_data_reset_claim_t cl;
    if (n == 1) {
        CHECK(off.state == MOQ_DATA_RESET_READY);
        CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_WAIVED,
                                           &cl, sizeof(cl)) == MOQ_ERR_WRONG_STATE);
        moq_data_reset_offer_t again;
        CHECK(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK &&
              n == 1 && again.state == MOQ_DATA_RESET_READY &&
              again.stream_ref._v == off.stream_ref._v);
        /* A record shorter than its stamp is refused before it is read. */
        moq_data_reset_offer_t cut = off;
        cut.struct_size = (uint32_t)(MOQ_DATA_RESET_OFFER_V0_SIZE - 1);
        CHECK(moq_session_claim_data_reset(srv, &cut, sizeof(cut), MOQ_DATA_PREFIX_HANDED,
                                           &cl, sizeof(cl)) == MOQ_ERR_INVAL);
        /* A record with another identity is stale and consumes nothing. */
        moq_data_reset_offer_t stale = off;
        stale.error_code ^= 1;
        CHECK(moq_session_claim_data_reset(srv, &stale, sizeof(stale), MOQ_DATA_PREFIX_HANDED,
                                           &cl, sizeof(cl)) == MOQ_ERR_STALE_HANDLE);
        /* Claim directly, then show the reset cannot escape the claim. */
        CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_HANDED,
                                           &cl, sizeof(cl)) == MOQ_OK);
        CHECK(!(cl.flags & MOQ_DATA_RESET_CLAIM_SETTLED));
        CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_HANDED,
                                           &cl, sizeof(cl)) == MOQ_ERR_WRONG_STATE);
        moq_action_t a;
        moq_action_tag_t tag;
        size_t got = 0;
        for (int guard = 0; guard < 8; guard++) {
            moq_result_t rc = moq_session_poll_actions_tagged(srv, &a, sizeof(a), &tag,
                                                             sizeof(tag), 1, &got);
            if (rc != MOQ_OK || got == 0) break;
            CHECK(!(a.kind == MOQ_ACTION_RESET_DATA &&
                    a.u.reset_data.stream_ref._v == off.stream_ref._v));
            moq_action_cleanup(&a);
        }
        CHECK(moq_session_offer_data_reset(srv, &again, sizeof(again), &n) == MOQ_OK &&
              n == 1 && again.state == MOQ_DATA_RESET_CLAIMED &&
              again.stream_ref._v == off.stream_ref._v);
        /* Another identity settles nothing; the original record commits
         * once; a duplicate is stale. */
        moq_data_reset_offer_t other = off;
        other.error_code ^= 1;
        CHECK(moq_session_commit_data_reset(srv, &other, sizeof(other)) == MOQ_ERR_STALE_HANDLE);
        CHECK(moq_session_commit_data_reset(srv, &again, sizeof(again)) == MOQ_OK);
        CHECK(moq_session_commit_data_reset(srv, &off, sizeof(off)) == MOQ_ERR_STALE_HANDLE);
    }
    CHECK(ph_run(p, PASSES));
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* The same reset through the host's own transaction, with the transport
 * refusing it twice: one claim, two refusals, one reset, one commit. A
 * refusal is not progress, so the run that meets the second one settles
 * with the claim held (offered as CLAIMED) and nothing committed or reset;
 * the next run writes the reset and commits it. */
static void tagged_refused_reset(moq_version_t version)
{
    name_case("tagged.refused", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "first");
    CHECK(ph_run(p, PASSES));
    unsigned before = ph_transport_resets(p, SERVER, NULL);
    ph_refuse_resets(p, SERVER, 2);
    REQUIRE(moq_session_reset_subgroup(ph_session(p, SERVER), sg, 0x9, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    const ph_counters_t *sn = ph_counters(p, SERVER);
    CHECK(sn->claims == 1 && sn->resets_refused == 2 && sn->commits == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == before);
    moq_data_reset_offer_t off;
    size_t n = 0;
    CHECK(moq_session_offer_data_reset(ph_session(p, SERVER), &off, sizeof(off), &n)
              == MOQ_OK &&
          n == 1 && off.state == MOQ_DATA_RESET_CLAIMED);
    CHECK(ph_run(p, PASSES));
    CHECK(sn->claims == 1 && sn->resets_refused == 2 && sn->commits == 1);
    CHECK(ph_transport_resets(p, SERVER, NULL) == before + 1);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(ph_session(p, SERVER), t.ssub, &dc,
                                       ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(done >= 0 && c.e[done].count == 1);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Close drops the claim: its commit is then MOQ_ERR_CLOSED. */
static void tagged_claim_closed(moq_version_t version)
{
    name_case("tagged.claim.closed", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "first");
    CHECK(ph_run(p, PASSES));
    REQUIRE(moq_session_reset_subgroup(srv, sg, 0x9, ph_now(p)) == MOQ_OK);
    moq_data_reset_offer_t off;
    size_t n = 0;
    moq_data_reset_claim_t cl;
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    REQUIRE(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_HANDED,
                                         &cl, sizeof(cl)) == MOQ_OK);
    REQUIRE(moq_session_close(srv, 0, NULL, ph_now(p)) == MOQ_OK);
    CHECK(moq_session_commit_data_reset(srv, &off, sizeof(off)) == MOQ_ERR_CLOSED);
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_ERR_CLOSED);
    CHECK(ph_run(p, PASSES));
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    CHECK(ph_pair_destroy(p) == 0);
}

/* A peer STOP on the response stream of a live FETCH: one FETCH_CANCELLED
 * with the peer's code, the data stream reset, and on drafts 18 and 22 the
 * request stream reset with CANCELLED; the FETCH is gone for the producer.
 * A later STOP of the request stream resets nothing again and reports
 * nothing more. */
static void fetch_data_stop(moq_version_t version)
{
    name_case("fetch.data_stop", version);
    ph_pair_t *p = open_pair(version, NULL);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    fc.track_name = MOQ_BYTES_LITERAL("past");
    fc.end_group = 0;
    fc.end_object = 9;
    moq_fetch_t cfetch;
    REQUIRE(moq_session_fetch(ph_session(p, CLIENT), &fc, ph_now(p), &cfetch) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t s = {0};
    drain(p, SERVER, &s);
    int at = index_of(&s, MOQ_EVENT_FETCH_REQUEST);
    REQUIRE(at >= 0);
    moq_fetch_t sfetch = s.e[at].fetch;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    REQUIRE(moq_session_accept_fetch(ph_session(p, SERVER), sfetch, &ac, ph_now(p)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 100;
    oc.payload = payload(p, "f-zero");
    REQUIRE(moq_session_write_fetch_object(ph_session(p, SERVER), sfetch, &oc,
                                           ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    CHECK(ph_run(p, PASSES));
    drain(p, CLIENT, NULL);

    moq_stream_ref_t data_ref;
    REQUIRE(ph_only_open_local_data(p, SERVER, &data_ref));
    unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    REQUIRE(ph_peer_stops_data(p, SERVER, data_ref, 0x2A));
    CHECK(ph_run(p, PASSES));
    log_t s2 = {0};
    drain(p, SERVER, &s2);
    int cancelled = index_of(&s2, MOQ_EVENT_FETCH_CANCELLED);
    CHECK(count_kind(&s2, MOQ_EVENT_FETCH_CANCELLED) == 1);
    if (cancelled >= 0) {
        CHECK(s2.e[cancelled].cause == MOQ_REQUEST_TERMINAL_PEER_STOP_DATA);
        CHECK(s2.e[cancelled].code == 0x2A);
    }
    bool streams = version != MOQ_VERSION_DRAFT_16;
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + (streams ? 2u : 1u));
    moq_fetch_object_cfg_init(&oc);
    oc.object_id = 1;
    oc.payload = payload(p, "late");
    CHECK(moq_session_write_fetch_object(ph_session(p, SERVER), sfetch, &oc, ph_now(p))
          == MOQ_ERR_STALE_HANDLE);
    moq_rcbuf_decref(oc.payload);
    CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_ESTABLISHED);
    CHECK(!ph_transport_closed(p, SERVER, NULL));

    if (streams) {
        /* The client stops the request stream later: the reset already
         * answers it, and the session reports nothing more. */
        unsigned repeats0 = 0, repeats1 = 0;
        unsigned resets1 = ph_transport_resets(p, SERVER, &repeats0);
        CHECK(ph_peer_stops_served_request(p, SERVER, 0x5));
        CHECK(ph_run(p, PASSES));
        CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets1);
        CHECK(repeats1 == repeats0);
        log_t s3 = {0};
        drain(p, SERVER, &s3);
        CHECK(count_kind(&s3, MOQ_EVENT_FETCH_CANCELLED) == 0);
    }
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A FETCH of track `name` the client requested and the server holds,
 * not yet answered: the server's handle. */
static moq_fetch_t request_fetch(ph_pair_t *p, const char *name, moq_fetch_t *cfetch)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    fc.track_name = (moq_bytes_t){.data = (const uint8_t *)name, .len = strlen(name)};
    fc.end_group = 0;
    fc.end_object = 9;
    REQUIRE(moq_session_fetch(ph_session(p, CLIENT), &fc, ph_now(p), cfetch) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    log_t s = {0};
    drain(p, SERVER, &s);
    int at = index_of(&s, MOQ_EVENT_FETCH_REQUEST);
    REQUIRE(at >= 0);
    return s.e[at].fetch;
}

/* A FETCH of track `name` the server accepted, one object delivered. */
static moq_fetch_t serve_fetch_named(ph_pair_t *p, const char *name,
                                     moq_fetch_t *cfetch)
{
    moq_fetch_t sfetch = request_fetch(p, name, cfetch);
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    REQUIRE(moq_session_accept_fetch(ph_session(p, SERVER), sfetch, &ac, ph_now(p)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.publisher_priority = 100;
    oc.payload = payload(p, "f-zero");
    REQUIRE(moq_session_write_fetch_object(ph_session(p, SERVER), sfetch, &oc,
                                           ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    REQUIRE(ph_run(p, PASSES));
    drain(p, CLIENT, NULL);
    return sfetch;
}

static moq_fetch_t serve_fetch(ph_pair_t *p, moq_fetch_t *cfetch)
{
    return serve_fetch_named(p, "past", cfetch);
}

/* An earlier native cause wins: the FETCH aborted with code C, then the
 * peer's STOP with P arrives. The data stream is reset once, with C; no
 * FETCH_CANCELLED follows (the abort reports none); the transport took
 * exactly the abort's resets and no repeat. */
static void fetch_first_cause(moq_version_t version)
{
    name_case("fetch.first_cause", version);
    ph_pair_t *p = open_pair(version, NULL);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data_ref;
    REQUIRE(ph_only_open_local_data(p, SERVER, &data_ref));
    unsigned repeats0 = 0, repeats1 = 0;
    unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    REQUIRE(moq_session_abort_fetch(ph_session(p, SERVER), sfetch, 0x11, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_peer_stops_data(p, SERVER, data_ref, 0x2A));
    CHECK(ph_run(p, PASSES));
    bool streams = version != MOQ_VERSION_DRAFT_16;
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + (streams ? 2u : 1u));
    CHECK(repeats1 == repeats0);
    log_t s = {0}, c = {0};
    drain(p, SERVER, &s);
    drain(p, CLIENT, &c);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);
    int reset = index_of(&c, MOQ_EVENT_FETCH_RESET);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_RESET) >= 1);
    if (reset >= 0) CHECK(c.e[reset].code == 0x11);
    CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_ESTABLISHED);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Two STOPs with different codes on an open fill stream: one reset, with
 * the first code. */
static void fill_two_stops(moq_version_t version)
{
    name_case("fill.two_stops", version);
    ph_pair_t *p = open_pair(version, NULL);
    if (!moq_session_supports_fill(ph_session(p, CLIENT))) {
        close_pair(p);
        return;                  /* this profile has no fills */
    }
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = MOQ_BYTES_LITERAL("f");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.fill = &fill;
    moq_subscription_t csub;
    REQUIRE(moq_session_subscribe(ph_session(p, CLIENT), &sc, ph_now(p), &csub) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    log_t s = {0};
    drain(p, SERVER, &s);
    int at = index_of(&s, MOQ_EVENT_SUBSCRIBE_REQUEST);
    REQUIRE(at >= 0);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 2;
    ac.largest_object = 0;
    REQUIRE(moq_session_accept_subscribe(ph_session(p, SERVER), s.e[at].sub, &ac,
                                         ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    drain(p, CLIENT, NULL);
    s.n = 0;
    drain(p, SERVER, &s);
    int requested = index_of(&s, MOQ_EVENT_FILL_REQUEST);
    REQUIRE(requested >= 0);
    REQUIRE(moq_session_open_fill(ph_session(p, SERVER), s.e[requested].fill,
                                  ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    moq_stream_ref_t ref;
    REQUIRE(ph_only_open_local_data(p, SERVER, &ref));
    moq_session_t *srv = ph_session(p, SERVER);
    CHECK(moq_session_on_data_stop(srv, ref, 0x21, ph_now(p)) == MOQ_OK);
    CHECK(moq_session_on_data_stop(srv, ref, 0x22, ph_now(p)) == MOQ_OK);
    unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    CHECK(ph_run(p, PASSES));
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 1);
    log_t c = {0};
    drain(p, CLIENT, &c);
    (void)c;
    CHECK(ph_last_reset_code(p, SERVER) == 0x21);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Held-data expiry reported by the host: a live subgroup latches
 * DELIVERY_TIMEOUT and its reset follows; an application reset already
 * queued keeps its code; a FETCH stream has no deadline. */
static void held_expiry(moq_version_t version)
{
    name_case("tagged.held_expiry", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t a = open_group(p, &t, 0);
    write_object(p, a, 0, "late");
    moq_subgroup_handle_t b = open_group(p, &t, 1);
    write_object(p, b, 0, "reset first");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t refs[2];
    REQUIRE(ph_local_data_refs(p, SERVER, refs, 2) == 2);   /* a, then b */
    REQUIRE(moq_session_reset_subgroup(srv, b, 0x9, ph_now(p)) == MOQ_OK);
    moq_data_end_result_t end;
    CHECK(moq_session_on_held_data_expired(srv, refs[1], ph_now(p), &end, sizeof(end))
          == MOQ_OK);
    CHECK(end.outcome == MOQ_DATA_END_RESET_FOLLOWS);
    moq_data_reset_offer_t off;
    size_t n = 0;
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    if (n == 1) CHECK(off.error_code == 0x9);   /* the first cause stands */
    if (n == 1) CHECK(off.stream_ref._v == refs[1]._v);
    CHECK(moq_session_on_held_data_expired(srv, refs[1], ph_now(p), &end,
                                           MOQ_DATA_END_RESULT_V0_SIZE - 1) == MOQ_ERR_INVAL);
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t live = refs[0];
    CHECK(moq_session_on_held_data_expired(srv, live, ph_now(p), &end, sizeof(end)) == MOQ_OK);
    CHECK(end.outcome == MOQ_DATA_END_RESET_FOLLOWS);
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK);
    if (n == 1) {
        CHECK(off.stream_ref._v == live._v);
        CHECK(off.error_code == MOQ_RESET_DELIVERY_TIMEOUT);
        CHECK(off.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    }
    CHECK(ph_run(p, PASSES));
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* The third barrier case: the header was polled and the host holds it,
 * nothing reached the transport, and the open is unreported. The reset is
 * READY, yet UNOPENED is still allowed: it settles with no stream and the
 * count stays 0. */
static void tagged_handed_unopened(moq_version_t version)
{
    name_case("tagged.handed_unopened", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe(p, &t, "t");
    moq_session_t *srv = ph_session(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    /* Hand the header out of the session without the host's pass. */
    moq_action_t a;
    moq_action_tag_t tag;
    size_t got = 0;
    bool header = false;
    for (int guard = 0; guard < 8 && !header; guard++) {
        if (moq_session_poll_actions_tagged(srv, &a, sizeof(a), &tag, sizeof(tag), 1, &got)
                != MOQ_OK || got == 0) break;
        header = a.kind == MOQ_ACTION_SEND_DATA &&
                 (tag.flags & MOQ_ACTION_TAG_OPEN_COUNTED);
        moq_action_cleanup(&a);
    }
    REQUIRE(header);
    REQUIRE(moq_session_reset_subgroup(srv, sg, 0x7, ph_now(p)) == MOQ_OK);
    moq_data_reset_offer_t off;
    size_t n = 0;
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    moq_data_reset_claim_t cl;
    if (n == 1) {
        CHECK(off.state == MOQ_DATA_RESET_READY);
        CHECK(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_UNOPENED,
                                           &cl, sizeof(cl)) == MOQ_OK);
        CHECK(cl.flags & MOQ_DATA_RESET_CLAIM_SETTLED);
        CHECK(cl.flags & MOQ_DATA_RESET_CLAIM_OPEN_SUPPRESSED);
        CHECK(moq_session_report_data_open(srv, off.stream_ref, off.owner,
                                           MOQ_DATA_OPEN_OPENED) == MOQ_ERR_STALE_HANDLE);
    }
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(srv, t.ssub, &dc, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(done >= 0 && c.e[done].count == 0);
    CHECK(ph_counters(p, CLIENT)->routed_data == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* -- Held output, deadlines and wakes ------------------------------------- */

#define TIMEOUT_MS 50           /* the object delivery timeout of these rows */

/* A subgroup reset that waits behind its stream's still-queued header. Two
 * subgroups each queue a header and one object, and no host pass runs. At
 * the objects' deadline the session latches both itself: each expired
 * object is dropped, its terminal event commits, and its reset waits behind
 * its own header. The first is offered UNOPENED and claimed so: it settles
 * with its header, its open is suppressed, and it is never offered again;
 * the second is offered UNOPENED next, and the host claims that one too.
 * Nothing of either is polled, no stream or reset reaches the transport,
 * and the DONE counts none. */
static void tagged_unopened_owed(moq_version_t version)
{
    name_case("tagged.unopened_owed", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_subgroup_handle_t first = open_group(p, &t, 0);
    write_object(p, first, 0, "a");
    moq_subgroup_handle_t second = open_group(p, &t, 1);
    write_object(p, second, 0, "b");
    /* No host pass runs until both are settled: the headers and objects
     * stay queued. */
    uint64_t due = moq_session_next_deadline_us(srv);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(moq_session_tick(srv, due) == MOQ_OK);
    moq_data_reset_offer_t off;
    size_t n = 0;
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
    REQUIRE(off.state == MOQ_DATA_RESET_UNOPENED);
    CHECK(off.error_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(off.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    moq_data_reset_claim_t cl;
    REQUIRE(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_UNOPENED,
                                         &cl, sizeof(cl)) == MOQ_OK);
    CHECK(cl.flags & MOQ_DATA_RESET_CLAIM_SETTLED);
    CHECK(cl.flags & MOQ_DATA_RESET_CLAIM_OPEN_SUPPRESSED);
    CHECK(!(cl.flags & MOQ_DATA_RESET_CLAIM_EVENT_COMMITTED));   /* at the tick */
    CHECK(cl.released_data_actions == 1);            /* its header */
    /* The settled reset is not offered again; the other one is next. */
    moq_data_reset_offer_t next;
    n = 0;
    CHECK(moq_session_offer_data_reset(srv, &next, sizeof(next), &n) == MOQ_OK && n == 1);
    if (n == 1) {
        CHECK(next.stream_ref._v != off.stream_ref._v);
        CHECK(next.state == MOQ_DATA_RESET_UNOPENED);
    }

    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->claims == 1 && sn->claims_settled == 1 && sn->commits == 0);
    CHECK(sn->resets_unknown == 0);                  /* nothing polled */
    CHECK(sn->opens_reported == 0);
    n = 9;
    CHECK(moq_session_offer_data_reset(srv, &next, sizeof(next), &n) == MOQ_OK && n == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == 0);
    CHECK(ph_counters(p, CLIENT)->routed_data == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) == 2);
    for (size_t i = 0; i < 2; i++) {
        int at = nth_of(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED, i);
        if (at >= 0) CHECK(s.e[at].cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    }
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(srv, t.ssub, &dc, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    CHECK(sn->resets_unknown == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == 0);
    log_t c = {0};
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    if (done >= 0) CHECK(c.e[done].count == 0);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_RESET) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A reset owed on its subgroup, not queued, while the stream's header is
 * still queued, reached through the subscriber's cancellation. Two
 * subgroups' headers fill an action ring of two and no host pass polls
 * them; the publisher then takes the UNSUBSCRIBE (or, on drafts 18 and 22,
 * the end of the request stream), and with no free slot each CANCELLED
 * reset stays owed on its subgroup. Each is offered UNOPENED and claimed
 * so: it settles with its header, no event commits (a cancellation has no
 * terminal) and no counted open is left to suppress (the cancelled owner's
 * count ended with it), and it is never offered again. Nothing of either
 * is polled, and no data stream or data reset reaches the transport (on
 * drafts 18 and 22 the request stream itself still ends with its reset).
 *
 * `direct` (draft-16, whose UNSUBSCRIBE is a control message): the test
 * hands the subscriber's UNSUBSCRIBE to the publisher itself and claims the
 * first reset, so the claim's own result is seen; the host claims the
 * second. Otherwise the publisher's input is held while the subscriber
 * ends the subscription, and released once the headers are queued: the
 * host takes the cancellation and claims both resets in that same pass. */
static void tagged_unopened_owed_cancel(moq_version_t version, bool direct)
{
    name_case(direct ? "tagged.unopened_owed_cancel.direct" : "tagged.unopened_owed_cancel",
              version);
    ph_options_t o = tagged_server();
    o.max_actions[SERVER] = 2;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER), *cli = ph_session(p, CLIENT);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    if (!direct) {
        ph_set_input_hold(p, SERVER, true);
        REQUIRE(moq_session_unsubscribe(cli, t.csub, ph_now(p)) == MOQ_OK);
        CHECK(ph_run(p, PASSES));
    }
    (void)open_group(p, &t, 0);
    (void)open_group(p, &t, 1);
    if (!direct) {
        ph_set_input_hold(p, SERVER, false);
        CHECK(ph_run(p, PASSES));
        CHECK(sn->claims == 2 && sn->claims_settled == 2 && sn->commits == 0);
    } else {
        REQUIRE(moq_session_unsubscribe(cli, t.csub, ph_now(p)) == MOQ_OK);
        moq_action_t a;
        size_t count = 0;
        memset(&a, 0, sizeof(a));
        REQUIRE(moq_session_poll_actions_ex(cli, &a, 1, sizeof(a), &count) == MOQ_OK &&
                count == 1 && a.kind == MOQ_ACTION_SEND_CONTROL);
        REQUIRE(moq_session_on_control_bytes(srv, a.u.send_control.data,
                                             a.u.send_control.len, ph_now(p)) == MOQ_OK);
        moq_action_cleanup(&a);
        moq_data_reset_offer_t off;
        size_t n = 0;
        REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 1);
        REQUIRE(off.state == MOQ_DATA_RESET_UNOPENED);
        CHECK(off.error_code == 0x1);                /* CANCELLED */
        CHECK(off.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE);
        moq_data_reset_claim_t cl;
        REQUIRE(moq_session_claim_data_reset(srv, &off, sizeof(off), MOQ_DATA_PREFIX_UNOPENED,
                                             &cl, sizeof(cl)) == MOQ_OK);
        CHECK(cl.flags == MOQ_DATA_RESET_CLAIM_SETTLED);
        CHECK(cl.released_data_actions == 1);        /* its header */
        /* The settled reset is not offered again; the other one is next. */
        moq_data_reset_offer_t next;
        n = 0;
        CHECK(moq_session_offer_data_reset(srv, &next, sizeof(next), &n) == MOQ_OK && n == 1);
        if (n == 1) {
            CHECK(next.stream_ref._v != off.stream_ref._v);
            CHECK(next.state == MOQ_DATA_RESET_UNOPENED);
            CHECK(next.error_code == 0x1);
        }
        CHECK(ph_run(p, PASSES));
        CHECK(sn->claims == 1 && sn->claims_settled == 1 && sn->commits == 0);
    }
    CHECK(sn->resets_unknown == 0);                  /* nothing polled */
    CHECK(sn->opens_reported == 0);
    moq_data_reset_offer_t none;
    size_t n = 9;
    CHECK(moq_session_offer_data_reset(srv, &none, sizeof(none), &n) == MOQ_OK && n == 0);
    moq_stream_ref_t refs[4];
    CHECK(ph_local_data_refs(p, SERVER, refs, 4) == 0);   /* no data stream exists */
    CHECK(ph_counters(p, CLIENT)->routed_data == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) == 0);
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_RESET) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* An object datagram on the subscription, from the pair's allocator. */
static void send_datagram(ph_pair_t *p, const track_t *t, uint64_t group,
                          const char *text)
{
    moq_rcbuf_t *b = payload(p, text);
    REQUIRE(moq_session_send_object_datagram(ph_session(p, SERVER), t->ssub, group, 0,
                                             128, false, b, NULL, 0, ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(b);
}

/* The client asks for another track; the server's application does not
 * poll the request. */
static void request_unpolled(ph_pair_t *p, const char *name)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub;
    REQUIRE(moq_session_subscribe(ph_session(p, CLIENT), &sc, ph_now(p), &sub) == MOQ_OK);
}

/* Held subgroup bytes pass their tag deadline while the transport refuses
 * stream writes. Until then each run retries the refused write and settles:
 * a refusal is not progress, and the held output and its deadline stay as
 * they were. At the deadline the host reports the expiry before any later
 * write; the session latches DELIVERY_TIMEOUT and its reset follows through
 * the claim; the expired object never reaches the peer, and the terminal
 * event is reported once. With `refuse` the transport refuses that reset
 * twice: the wake settles on the second refusal with the claim held and
 * nothing committed, and the next run writes the reset and commits it once.
 * Every draft: the object's clock starts at its header on drafts 16 and 22
 * and at its first payload byte on draft 18, and no draft gives the open
 * subgroup a timer of its own. */
static void host_held_expiry(moq_version_t version, bool refuse)
{
    name_case(refuse ? "host.expiry.reset_refused" : "host.expiry", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "in time");
    CHECK(ph_run(p, PASSES));
    CHECK(ph_held_outputs(p, SERVER) == 0);
    CHECK(sn->opens_reported == 1);

    ph_set_write_refusal(p, SERVER, PH_REFUSE_STREAM_BYTES);
    write_object(p, sg, 1, "too late");
    CHECK(ph_run(p, PASSES));
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(ph_held_outputs(p, SERVER) == 1);
    for (int round = 0; round < 2; round++) {
        unsigned refused = sn->writes_refused;
        CHECK(ph_run(p, PASSES));
        CHECK(sn->writes_refused > refused);
        CHECK(ph_held_outputs(p, SERVER) == 1);
        CHECK(ph_next_held_deadline(p, SERVER) == due);
    }
    CHECK(sn->held_expired == 0 && sn->claims == 0);
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);
    check_object(&c, 0, t.csub, MOQ_PUBLICATION_INVALID, 0, 0, "in time");

    unsigned repeats0 = 0, repeats1 = 0;
    unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    if (refuse) ph_refuse_resets(p, SERVER, 2);
    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->held_expired == 1);
    CHECK(sn->claims == 1);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    if (refuse) {
        /* Both refusals came in the wake, and the second moved nothing: it
         * settled with the claim held, offered as CLAIMED, not committed. */
        CHECK(sn->resets_refused == 2 && sn->commits == 0);
        CHECK(ph_transport_resets(p, SERVER, NULL) == resets0);
        moq_data_reset_offer_t off;
        size_t n = 0;
        CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK &&
              n == 1 && off.state == MOQ_DATA_RESET_CLAIMED &&
              off.error_code == MOQ_RESET_DELIVERY_TIMEOUT);
        CHECK(ph_run(p, PASSES));
    }
    CHECK(sn->commits == 1);
    CHECK(sn->resets_refused == (refuse ? 2u : 0u));
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 1);
    CHECK(repeats1 == repeats0);
    CHECK(ph_last_reset_code(p, SERVER) == MOQ_RESET_DELIVERY_TIMEOUT);
    log_t s = {0};
    drain(p, SERVER, &s);
    int term = index_of(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    CHECK(count_kind(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) == 1);
    if (term >= 0) {
        CHECK(s.e[term].cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
        CHECK(s.e[term].code == MOQ_RESET_DELIVERY_TIMEOUT);
    }

    /* Writes are taken again: nothing of the expired object follows. */
    ph_set_write_refusal(p, SERVER, 0);
    CHECK(ph_run(p, PASSES));
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 0);
    int reset = index_of(&c, MOQ_EVENT_SUBGROUP_RESET);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_RESET) == 1);
    if (reset >= 0) CHECK(c.e[reset].code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(sn->held_expired == 1);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A datagram held while the transport refuses datagrams is dropped by the
 * host once its tag deadline passes: never sent, never reported to the
 * session. Until then each run retries it, is refused, and settles with it
 * still held and its deadline unchanged; stream output is not held back
 * behind it. A datagram sent once datagrams are taken again is delivered. */
static void host_datagram_expiry(moq_version_t version)
{
    name_case("host.datagram_expiry", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    ph_set_write_refusal(p, SERVER, PH_REFUSE_DATAGRAMS);
    send_datagram(p, &t, 1, "dropped");
    CHECK(ph_run(p, PASSES));
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(ph_held_outputs(p, SERVER) == 1);
    unsigned refused = sn->writes_refused;
    CHECK(ph_run(p, PASSES));
    CHECK(sn->writes_refused > refused);
    CHECK(ph_held_outputs(p, SERVER) == 1);
    CHECK(ph_next_held_deadline(p, SERVER) == due);

    /* A stream object is not held behind the refused datagram. */
    moq_subgroup_handle_t sg = open_group(p, &t, 2);
    write_object(p, sg, 0, "stream");
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);
    check_object(&c, 0, t.csub, MOQ_PUBLICATION_INVALID, 2, 0, "stream");

    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->datagrams_expired == 1 && sn->datagrams_sent == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    ph_set_write_refusal(p, SERVER, 0);
    send_datagram(p, &t, 3, "delivered");
    CHECK(ph_run(p, PASSES));
    CHECK(sn->datagrams_sent == 1 && sn->datagrams_expired == 1);
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);
    check_object(&c, 0, t.csub, MOQ_PUBLICATION_INVALID, 3, 0, "delivered");
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* An owner's final message waits for its fence. The subgroup's last object
 * and its FIN, then the DONE, are held while the transport refuses stream
 * writes, so the session has already handed the DONE out. The object's
 * deadline passes; the transport now takes writes but refuses resets. The
 * DONE still waits while the stream's reset (the session's, or the one this
 * side owes) is not accepted, and goes once it is. The expired object and
 * the FIN never reach the peer. */
static void host_final_fence(moq_version_t version)
{
    name_case("host.final_fence", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "in time");
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    ph_set_write_refusal(p, SERVER, PH_REFUSE_STREAM_BYTES);
    write_object(p, sg, 1, "too late");
    REQUIRE(moq_session_close_subgroup(srv, sg, ph_now(p)) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(srv, t.ssub, &dc, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(ph_held_outputs(p, SERVER) >= 3);       /* the object, the FIN, the DONE */
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 0);

    unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    ph_refuse_resets(p, SERVER, 2);
    ph_set_write_refusal(p, SERVER, 0);
    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->held_expired == 1);
    CHECK(sn->resets_refused == 2);
    CHECK(sn->final_waits >= 1);
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0);
    CHECK(ph_held_outputs(p, SERVER) >= 1);       /* the DONE */
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 0);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 0);

    CHECK(ph_run(p, PASSES));
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 1);
    CHECK(ph_last_reset_code(p, SERVER) == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    c.n = 0;
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    if (done >= 0) CHECK(c.e[done].count == 1);
    int reset = index_of(&c, MOQ_EVENT_SUBGROUP_RESET);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_RESET) == 1);
    if (reset >= 0) CHECK(c.e[reset].code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 0);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_FINISHED) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* The final message also waits for a datagram of its owner the host still
 * holds. Only datagrams are refused: the DONE, which the transport would
 * take, waits (run after run, nothing changing) until the datagram leaves
 * the host, here dropped at its deadline, and then goes. */
static void host_final_fence_datagram(moq_version_t version)
{
    name_case("host.final_fence.datagram", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    ph_set_write_refusal(p, SERVER, PH_REFUSE_DATAGRAMS);
    send_datagram(p, &t, 0, "held");
    CHECK(ph_run(p, PASSES));
    CHECK(ph_held_outputs(p, SERVER) == 1);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(srv, t.ssub, &dc, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(sn->final_waits >= 1);
    CHECK(ph_held_outputs(p, SERVER) >= 2);       /* the datagram and the DONE */
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 0);
    unsigned waits = sn->final_waits;
    CHECK(ph_run(p, PASSES));
    CHECK(sn->final_waits > waits);
    CHECK(ph_next_held_deadline(p, SERVER) == due);
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 0);

    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->datagrams_expired == 1 && sn->datagrams_sent == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    c.n = 0;
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    if (done >= 0) CHECK(c.e[done].count == 0);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 0);
    ph_set_write_refusal(p, SERVER, 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A tick that cannot act is not progress, and a wake ticks at most once.
 * The subgroup's held bytes expire while the server's event queue is full
 * (two requests its application has not polled), so the latched
 * DELIVERY_TIMEOUT cannot commit its event: the reset offer waits, and the
 * session arms a retry deadline. Runs before that deadline settle with it
 * unchanged and nothing claimed. The wake at it ticks once; the tick returns
 * MOQ_ERR_WOULD_BLOCK (the terminal still waits for event capacity) and the
 * wake settles with nothing claimed or reset. The session re-arms its retry
 * 5 ms on: a wake there ticks once more and blocks again, and a second wake
 * at that same time does not tick. Nothing spins: a blocked tick moves
 * nothing, so each run or wake settles. Once the application polls its
 * events, the terminal event surfaces once, behind them, and the reset is
 * claimed, written and committed. Every draft, as for host_held_expiry. */
static void host_blocked_tick(moq_version_t version)
{
    name_case("host.blocked_tick", version);
    ph_options_t o = tagged_server();
    o.max_events[SERVER] = 2;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "in time");
    CHECK(ph_run(p, PASSES));
    request_unpolled(p, "u");
    request_unpolled(p, "v");
    CHECK(ph_run(p, PASSES));

    ph_set_write_refusal(p, SERVER, PH_REFUSE_STREAM_BYTES);
    write_object(p, sg, 1, "too late");
    CHECK(ph_run(p, PASSES));
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->held_expired == 1 && sn->claims == 0);
    moq_data_reset_offer_t off;
    size_t n = 9;
    /* The premise: the reset waits for event capacity alone. */
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_ERR_WOULD_BLOCK);
    uint64_t retry = moq_session_next_deadline_us(srv);
    REQUIRE(retry != UINT64_MAX && retry > ph_now(p));
    unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    unsigned ticks = sn->ticks, blocked = sn->ticks_blocked;
    for (int round = 0; round < 2; round++) {
        unsigned waits = sn->claim_waits;
        CHECK(ph_run(p, PASSES));
        CHECK(sn->claim_waits > waits);            /* offered again, waited again */
        CHECK(sn->ticks == ticks && sn->claims == 0);
        CHECK(moq_session_next_deadline_us(srv) == retry);
    }
    CHECK(ph_wake(p, retry, PASSES));
    CHECK(sn->ticks == ticks + 1);
    CHECK(sn->ticks_blocked == blocked + 1);
    CHECK(sn->claims == 0 && ph_transport_resets(p, SERVER, NULL) == resets0);
    uint64_t rearmed = moq_session_next_deadline_us(srv);
    REQUIRE(rearmed != UINT64_MAX && rearmed > ph_now(p));
    CHECK(ph_wake(p, rearmed, PASSES));
    CHECK(sn->ticks == ticks + 2);
    CHECK(sn->ticks_blocked == blocked + 2);
    CHECK(sn->claims == 0 && ph_transport_resets(p, SERVER, NULL) == resets0);
    uint64_t later = moq_session_next_deadline_us(srv);
    CHECK(later != UINT64_MAX && later > ph_now(p));
    CHECK(ph_wake(p, rearmed, PASSES));            /* the same time again */
    CHECK(sn->ticks == ticks + 2);
    CHECK(sn->claims == 0);
    CHECK(moq_session_next_deadline_us(srv) == later);

    /* The log spans both polls: the capacity the application frees lets the
     * latched terminal event commit, so it may surface in that same poll,
     * behind the two requests that held the queue. */
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_SUBSCRIBE_REQUEST) == 2);
    CHECK(ph_run(p, PASSES));
    CHECK(sn->claims == 1 && sn->commits == 1);
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 1);
    CHECK(ph_last_reset_code(p, SERVER) == MOQ_RESET_DELIVERY_TIMEOUT);
    drain(p, SERVER, &s);
    int term = index_of(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    CHECK(count_kind(&s, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) == 1);
    CHECK(term > nth_of(&s, MOQ_EVENT_SUBSCRIBE_REQUEST, 1));
    if (term >= 0) CHECK(s.e[term].cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
    ph_set_write_refusal(p, SERVER, 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A reset already handed out does not lift the deadline of the bytes held
 * ahead of it. Two subscriptions: A with an opened, reported subgroup, B
 * with no stream. A's next object is held while stream writes are refused.
 * Then B's DONE and A's application reset (0x9) are queued: B's final
 * message is the first action that is not data, so no reset is offered, and
 * the host's next pass polls both, A's reset behind B's DONE, with A's
 * object still held and no claim made. The object's deadline still wakes
 * the host. At it, with writes taken again, the expired object is dropped,
 * never written; A's reset goes once, with 0x9, with no report, claim or
 * second reset; B's DONE goes once the object no longer stands in front of
 * it. Every draft, as for host_held_expiry. */
static void host_reset_before_expiry(moq_version_t version)
{
    name_case("host.reset_before_expiry", version);
    ph_options_t o = tagged_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t a, b;
    subscribe_timed(p, &a, "a", TIMEOUT_MS);
    subscribe(p, &b, "b");
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &a, 0);
    write_object(p, sg, 0, "in time");
    CHECK(ph_run(p, PASSES));
    /* Already open: its header and first object were taken, its open
     * reported. */
    REQUIRE(sn->opens_reported == 1);
    REQUIRE(ph_held_outputs(p, SERVER) == 0);
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    ph_set_write_refusal(p, SERVER, PH_REFUSE_STREAM_BYTES);
    write_object(p, sg, 1, "too late");
    CHECK(ph_run(p, PASSES));
    CHECK(ph_held_outputs(p, SERVER) == 1);
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));

    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    REQUIRE(moq_session_done_subscribe(srv, b.ssub, &dc, ph_now(p)) == MOQ_OK);
    REQUIRE(moq_session_reset_subgroup(srv, sg, 0x9, ph_now(p)) == MOQ_OK);
    moq_data_reset_offer_t off;
    size_t n = 9;
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 0);

    unsigned repeats0 = 0, repeats1 = 0;
    unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    CHECK(ph_run(p, PASSES));
    /* Both were polled: no claim, the reset waits for A's held object, and
     * that object keeps its deadline. */
    REQUIRE(sn->claims == 0);
    CHECK(sn->resets_unknown == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0);
    CHECK(ph_held_outputs(p, SERVER) >= 2);           /* A's object, B's DONE */
    CHECK(ph_next_held_deadline(p, SERVER) == due);
    n = 9;
    CHECK(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_OK && n == 0);
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 0);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_RESET) == 0);

    ph_set_write_refusal(p, SERVER, 0);
    CHECK(ph_wake(p, due, PASSES));
    CHECK(ph_run(p, PASSES));
    CHECK(sn->held_trimmed == 1 && sn->held_expired == 0);
    CHECK(sn->claims == 0 && sn->commits == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 1);
    CHECK(repeats1 == repeats0);
    CHECK(ph_last_reset_code(p, SERVER) == 0x9);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    CHECK(ph_next_held_deadline(p, SERVER) == UINT64_MAX);
    c.n = 0;
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_OBJECT_RECEIVED) == 0);
    int reset = index_of(&c, MOQ_EVENT_SUBGROUP_RESET);
    CHECK(count_kind(&c, MOQ_EVENT_SUBGROUP_RESET) == 1);
    if (reset >= 0) CHECK(c.e[reset].code == 0x9);
    int done = index_of(&c, MOQ_EVENT_SUBSCRIBE_DONE);
    CHECK(count_kind(&c, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    if (done >= 0) {
        CHECK(moq_subscription_eq(c.e[done].sub, b.csub));
        CHECK(c.e[done].count == 0);
    }
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

#define GOAWAY_TIMEOUT_US UINT64_C(200000)  /* past the expiry and its first retry */

/* A hard timeout behind blocked terminal work is still served. The server's
 * GOAWAY timeout is configured (the client's timeouts stay disabled, so no
 * close of the client's can stand in for it). The blocked-tick setup fills
 * the server's event queue with two unpolled requests; then the server
 * sends GOAWAY, which the client takes and polls, before any terminal debt.
 * The held object expires, its terminal cannot commit, and the tick at the
 * retry deadline returns MOQ_ERR_WOULD_BLOCK. With no input, no event
 * polling and no write relief after that, the wake at the GOAWAY deadline
 * ticks again, though the blocked retry is the earliest deadline the
 * session reports, and the server closes with GOAWAY_TIMEOUT. Every draft,
 * as for host_blocked_tick. */
static void host_goaway_behind_blocked_tick(moq_version_t version)
{
    name_case("host.goaway_behind_blocked_tick", version);
    ph_options_t o = tagged_server();
    o.max_events[SERVER] = 2;
    o.goaway_timeout_us[SERVER] = GOAWAY_TIMEOUT_US;
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    track_t t;
    subscribe_timed(p, &t, "t", TIMEOUT_MS);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &t, 0);
    write_object(p, sg, 0, "in time");
    CHECK(ph_run(p, PASSES));
    request_unpolled(p, "u");
    request_unpolled(p, "v");
    CHECK(ph_run(p, PASSES));

    /* GOAWAY: sent, taken and polled by the client before any debt. */
    const uint64_t hard = ph_now(p) + GOAWAY_TIMEOUT_US;
    REQUIRE(moq_session_goaway(srv, NULL, 0, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_GOAWAY) == 1);
    CHECK(moq_session_state(srv) == MOQ_SESS_DRAINING);
    CHECK(moq_session_next_deadline_us(srv) == hard);

    ph_set_write_refusal(p, SERVER, PH_REFUSE_STREAM_BYTES);
    write_object(p, sg, 1, "too late");
    CHECK(ph_run(p, PASSES));
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p) && due < hard);
    CHECK(ph_wake(p, due, PASSES));
    CHECK(sn->held_expired == 1 && sn->claims == 0);
    moq_data_reset_offer_t off;
    size_t n = 9;
    /* The premise: the reset waits for event capacity alone. */
    REQUIRE(moq_session_offer_data_reset(srv, &off, sizeof(off), &n) == MOQ_ERR_WOULD_BLOCK);
    uint64_t retry = moq_session_next_deadline_us(srv);
    REQUIRE(retry != UINT64_MAX && retry > ph_now(p) && retry < hard);
    unsigned ticks = sn->ticks, blocked = sn->ticks_blocked;
    CHECK(ph_wake(p, retry, PASSES));
    CHECK(sn->ticks == ticks + 1 && sn->ticks_blocked == blocked + 1);
    CHECK(moq_session_state(srv) == MOQ_SESS_DRAINING);
    CHECK(!ph_transport_closed(p, SERVER, NULL));
    /* The re-armed retry now stands in front of the GOAWAY deadline. */
    uint64_t rearmed = moq_session_next_deadline_us(srv);
    REQUIRE(rearmed != UINT64_MAX && rearmed > ph_now(p) && rearmed < hard);

    CHECK(ph_wake(p, hard, PASSES));
    uint64_t code = 0;
    CHECK(ph_transport_closed(p, SERVER, &code) && code == MOQ_CLOSE_GOAWAY_TIMEOUT);
    CHECK(moq_session_state(srv) == MOQ_SESS_CLOSED);
    CHECK(sn->ticks == ticks + 2);
    CHECK(sn->claims == 0 && sn->commits == 0);
    CHECK(ph_fault(p, SERVER) == PH_FAULT_NONE);
    CHECK(ph_run(p, PASSES));
    CHECK(moq_session_state(ph_session(p, CLIENT)) == MOQ_SESS_CLOSED);
    CHECK(!ph_transport_closed(p, CLIENT, NULL));
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    CHECK(ph_held_outputs(p, CLIENT) == 0 && ph_held_outputs(p, SERVER) == 0);
    CHECK(ph_pair_destroy(p) == 0);
}

/* -- A FETCH's final message ------------------------------------------- */

/* The server in the tagged mode, keeping each stream's output order alone:
 * output on other streams may overtake a held record, so nothing but the
 * final-message fence keeps a FETCH's request FIN behind its response
 * stream. */
static ph_options_t fence_server(void)
{
    ph_options_t o;
    memset(&o, 0, sizeof(o));
    o.tagged[SERVER] = true;
    o.per_stream_order[SERVER] = true;
    return o;
}

static ph_stream_trace_t data_trace(const ph_pair_t *p, moq_stream_ref_t ref)
{
    ph_stream_trace_t t;
    memset(&t, 0, sizeof(t));
    REQUIRE(ph_data_trace(p, SERVER, ref, &t));
    return t;
}

static ph_stream_trace_t request_trace(const ph_pair_t *p, moq_stream_ref_t ref)
{
    ph_stream_trace_t t;
    memset(&t, 0, sizeof(t));
    REQUIRE(ph_request_trace(p, SERVER, ref, &t));
    return t;
}

static void write_fetch(ph_pair_t *p, moq_fetch_t f, uint64_t object,
                        const char *text)
{
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = object;
    oc.publisher_priority = 100;
    oc.payload = payload(p, text);
    REQUIRE(moq_session_write_fetch_object(ph_session(p, SERVER), f, &oc,
                                           ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
}

/* The served FETCH's response stream and request stream at the server, the
 * only ones it has. */
static void fetch_streams(const ph_pair_t *p, moq_stream_ref_t *data,
                          moq_stream_ref_t *req)
{
    REQUIRE(ph_only_open_local_data(p, SERVER, data));
    REQUIRE(ph_served_request_refs(p, SERVER, req, 1) == 1);
}

/* Drafts 18 and 22. The request FIN, the FETCH's final message, is handed
 * to the transport only after its response stream's FIN is accepted. A
 * FETCH with one object delivered; the transport then refuses its response
 * stream alone. The second object and end_fetch settle with the object and
 * the response stream's FIN held, and the request FIN held by its fence
 * alone: the request stream refuses nothing. A second FETCH, served and
 * ended meanwhile on streams of its own, overtakes them: its FINs go, in
 * order. Once the refusal lifts, the first FETCH's object and FIN go and
 * then its request FIN; no stream is reset and every run settles. */
static void host_fetch_final_fin(moq_version_t version)
{
    name_case("host.fetch_final.fin", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    /* FETCH_OK, the header and the first object went at once. */
    REQUIRE(r.writes >= 1 && r.fin == 0 && d.writes >= 1 && d.fin == 0);
    const unsigned dwrites = d.writes, rwrites = r.writes, waits = sn->final_waits;

    REQUIRE(ph_refuse_data_stream(p, SERVER, data));
    write_fetch(p, sfetch, 1, "f-one");
    REQUIRE(moq_session_end_fetch(srv, sfetch, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.writes == dwrites && d.fin == 0 && d.reset == 0);
    CHECK(r.fin == 0);                 /* no request FIN before the data FIN */
    CHECK(r.writes == rwrites && r.reset == 0);
    CHECK(sn->final_waits > waits);
    CHECK(ph_held_outputs(p, SERVER) == 3);   /* the object, both FINs */
    log_t c = {0};
    drain(p, CLIENT, &c);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_OBJECT) == 0);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 0);

    /* Output polled later, on other streams, overtakes the held records. */
    moq_fetch_t cother;
    moq_fetch_t sother = serve_fetch_named(p, "more", &cother);
    REQUIRE(moq_session_end_fetch(srv, sother, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t datas[2], reqs[2];
    REQUIRE(ph_local_data_refs(p, SERVER, datas, 2) == 2 && datas[0]._v == data._v);
    REQUIRE(ph_served_request_refs(p, SERVER, reqs, 2) == 2 && reqs[0]._v == req._v);
    ph_stream_trace_t od = data_trace(p, datas[1]), orq = request_trace(p, reqs[1]);
    CHECK(od.fin != 0 && orq.fin != 0 && od.fin < orq.fin);
    CHECK(od.reset == 0 && orq.reset == 0);
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.fin == 0 && r.fin == 0);
    c.n = 0;
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_FETCH_COMPLETE);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 1);
    if (done >= 0) CHECK(moq_fetch_eq(c.e[done].fetch, cother));

    /* Relief: the object and the response stream's FIN, then the request
     * FIN. */
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.writes > dwrites && d.fin != 0 && d.last_write < d.fin);
    CHECK(r.fin != 0 && d.fin < r.fin);
    CHECK(d.reset == 0 && r.reset == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    /* Both carried the policy; no STOP came, so neither request changed. */
    CHECK(sn->final_policies == 2 && sn->finals_cancelled == 0);
    c.n = 0;
    drain(p, CLIENT, &c);
    int obj = index_of(&c, MOQ_EVENT_FETCH_OBJECT);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_OBJECT) == 1);
    if (obj >= 0) {
        CHECK(moq_fetch_eq(c.e[obj].fetch, cfetch));
        CHECK(c.e[obj].object == 1);
        CHECK(bytes_are(c.e[obj].payload, c.e[obj].payload_len, "f-one"));
    }
    done = index_of(&c, MOQ_EVENT_FETCH_COMPLETE);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 1);
    if (done >= 0) CHECK(moq_fetch_eq(c.e[done].fetch, cfetch));
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_RESET) == 0);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_ERROR) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. An empty FETCH accepted while the transport refuses
 * the response stream it opens: FETCH_OK goes at once, without a FIN; the
 * header with its FIN is held, and the request FIN, the FETCH's final
 * message, waits behind it though the request stream refuses nothing. Once
 * the refusal lifts the header and its FIN go and the request FIN follows;
 * no stream is reset. */
static void host_fetch_final_empty(moq_version_t version)
{
    name_case("host.fetch_final.empty", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = request_fetch(p, "past", &cfetch);
    const unsigned waits = sn->final_waits;
    REQUIRE(ph_refuse_data_stream(p, SERVER, moq_stream_ref_from_u64(0)));
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.empty = true;
    REQUIRE(moq_session_accept_fetch(ph_session(p, SERVER), sfetch, &ac,
                                     ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(r.writes >= 1 && r.fin == 0 && r.reset == 0);   /* FETCH_OK alone */
    CHECK(d.writes == 0 && d.fin == 0 && d.reset == 0);
    CHECK(sn->final_waits > waits);
    CHECK(ph_held_outputs(p, SERVER) == 2);   /* the header with its FIN, the request FIN */
    log_t c = {0};
    drain(p, CLIENT, &c);
    int ok = index_of(&c, MOQ_EVENT_FETCH_OK);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_OK) == 1);
    if (ok >= 0) CHECK(moq_fetch_eq(c.e[ok].fetch, cfetch));
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 0);

    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.writes == 1 && d.fin != 0 && d.first_write < d.fin);
    CHECK(r.fin != 0 && d.fin < r.fin);
    CHECK(d.reset == 0 && r.reset == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    c.n = 0;
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_FETCH_COMPLETE);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 1);
    if (done >= 0) CHECK(moq_fetch_eq(c.e[done].fetch, cfetch));
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_OBJECT) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. A peer STOP of a live FETCH's response stream while the
 * transport refuses that stream: the teardown is not held behind the
 * refused bytes. The response stream is reset with the STOP's code and the
 * request stream with CANCELLED, neither with a FIN; one FETCH_CANCELLED
 * reports it, and the run settles with nothing held. Lifting the refusal
 * writes nothing more. */
static void host_fetch_cancel_blocked(moq_version_t version)
{
    name_case("host.fetch_final.cancel", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    const unsigned dwrites = data_trace(p, data).writes;
    REQUIRE(ph_refuse_data_stream(p, SERVER, data));
    write_fetch(p, sfetch, 1, "f-one");
    CHECK(ph_run(p, PASSES));
    CHECK(data_trace(p, data).writes == dwrites);
    CHECK(ph_held_outputs(p, SERVER) == 1);       /* the object */

    CHECK(ph_counters(p, SERVER)->final_policies == 1);
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == 0x2A);
    CHECK(d.fin == 0 && d.writes == dwrites);
    CHECK(r.reset != 0 && r.reset_code == MOQ_STREAM_ERROR_CANCELLED);
    CHECK(r.fin == 0);
    /* The session's teardown resets the request itself (REQUEST_ENDED). */
    CHECK(ph_counters(p, SERVER)->finals_cancelled == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    int cancelled = index_of(&s, MOQ_EVENT_FETCH_CANCELLED);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 1);
    if (cancelled >= 0) {
        CHECK(s.e[cancelled].cause == MOQ_REQUEST_TERMINAL_PEER_STOP_DATA);
        CHECK(s.e[cancelled].code == 0x2A);
    }

    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data), r2 = request_trace(p, req);
    CHECK(d2.writes == d.writes && d2.fin == 0 && d2.reset == d.reset);
    CHECK(r2.writes == r.writes && r2.fin == 0 && r2.reset == r.reset);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. A FETCH ended while the transport refuses its response
 * stream, then the peer stops that stream. The publisher resets both
 * streams of a FETCH on that STOP (draft-18 section 5.2, draft-22 section
 * 3.2.4). The response stream was opened tagged RESET_FINAL_ON_STOP, and
 * its FIN is not accepted when the STOP comes. The STOP ends it with a
 * reset; no native reset follows a FETCH that ended, so this side writes
 * it, with the STOP's code. The request's final message, its FIN held by
 * the fence, is never written: the request stream is reset with CANCELLED
 * in its place. Neither stream has a FIN, no FETCH_CANCELLED reports an
 * ended FETCH, nothing is held, and the runs settle. */
static void host_fetch_final_stop(moq_version_t version)
{
    name_case("host.fetch_final.stop", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    CHECK(sn->final_policies == 1);
    REQUIRE(ph_refuse_data_stream(p, SERVER, data));
    REQUIRE(moq_session_end_fetch(ph_session(p, SERVER), sfetch, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(d.fin == 0 && d.reset == 0);
    CHECK(r.fin == 0 && r.reset == 0);            /* the fence holds it */
    CHECK(ph_held_outputs(p, SERVER) == 2);       /* both FINs */
    const unsigned host_resets = sn->host_resets;

    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == 0x2A && d.fin == 0);
    CHECK(r.reset != 0 && r.reset_code == MOQ_STREAM_ERROR_CANCELLED);
    CHECK(r.fin == 0);                            /* never the request FIN */
    CHECK(sn->finals_cancelled == 1);
    CHECK(sn->host_resets == host_resets + 1);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);

    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data), r2 = request_trace(p, req);
    CHECK(d2.fin == 0 && d2.reset == d.reset && r2.fin == 0 && r2.reset == r.reset);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. A FETCH ended before the transport took anything of its
 * response stream, which the transport refuses from its open. The FETCH is
 * empty (its header and FIN one record) or has one object and then
 * end_fetch; FETCH_OK went, the header never did. The peer's STOP of the
 * response stream ends both streams with resets: the response stream with
 * this side's reset, carrying the STOP's code, and the request stream with
 * CANCELLED in place of its held final message. Neither stream has a
 * FIN. */
static void host_fetch_stop_unconsumed(moq_version_t version, bool empty)
{
    name_case(empty ? "host.fetch_stop.empty" : "host.fetch_stop.unconsumed", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = request_fetch(p, "past", &cfetch);
    REQUIRE(ph_refuse_data_stream(p, SERVER, moq_stream_ref_from_u64(0)));
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.empty = empty;
    if (!empty) {
        ac.end_group = 0;
        ac.end_object = 9;
    }
    REQUIRE(moq_session_accept_fetch(srv, sfetch, &ac, ph_now(p)) == MOQ_OK);
    if (!empty) {
        write_fetch(p, sfetch, 0, "f-zero");
        REQUIRE(moq_session_end_fetch(srv, sfetch, ph_now(p)) == MOQ_OK);
    }
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(d.writes == 0 && d.fin == 0 && d.reset == 0);
    CHECK(r.writes >= 1 && r.fin == 0 && r.reset == 0);   /* FETCH_OK alone */
    CHECK(sn->final_policies == 1);

    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == 0x2A && d.writes == 0 && d.fin == 0);
    CHECK(r.reset != 0 && r.reset_code == MOQ_STREAM_ERROR_CANCELLED && r.fin == 0);
    CHECK(sn->finals_cancelled == 1);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);

    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data), r2 = request_trace(p, req);
    CHECK(d2.writes == 0 && d2.fin == 0 && d2.reset == d.reset);
    CHECK(r2.fin == 0 && r2.reset == r.reset);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. The STOP is taken before the request's final message
 * leaves the session. end_fetch queues the response stream's FIN and the
 * request FIN, and the peer's STOP of the response stream is taken before
 * either is polled. The session drops the queued FIN (NO_RESET), and this
 * side resets the response stream with the STOP's code. The request FIN,
 * polled after the STOP, is never written: the request stream is reset
 * with CANCELLED instead, once. */
static void host_fetch_stop_before_final(moq_version_t version)
{
    name_case("host.fetch_stop.before_final", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    const unsigned dwrites = data_trace(p, data).writes, host_resets = sn->host_resets;
    REQUIRE(moq_session_end_fetch(ph_session(p, SERVER), sfetch, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == 0x2A && d.fin == 0 && d.writes == dwrites);
    CHECK(r.reset != 0 && r.reset_code == MOQ_STREAM_ERROR_CANCELLED && r.fin == 0);
    CHECK(sn->finals_cancelled == 1);
    CHECK(sn->host_resets == host_resets + 1);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. The first cause holds, and the obligation belongs to
 * its owner alone.
 * - The FETCH is aborted with 0x11 while the transport refuses the response
 *   stream's reset. This side holds that reset's claim; the session's own
 *   reset of the request stream waits behind it, queued and not polled.
 * - The peer's STOP of the response stream then meets the held claim,
 *   which takes it with no session call. The claim's reset keeps the
 *   abort's code. The request has no final message to replace, and its own
 *   reset stands: written once resets are taken, with 0x11, never a second
 *   one, never CANCELLED, never a FIN.
 * - A later FETCH takes the freed slot under a new generation and completes
 *   ordinarily: its data FIN, then its request FIN, nothing reset.
 * The client reads nothing until the server's resets are out, so no
 * reaction of its own enters the row (host_fetch_stop_request_crossing has
 * a request STOP that crosses the abort). Once it reads, nothing more is
 * reset. */
static void host_fetch_stop_claim(moq_version_t version)
{
    name_case("host.fetch_stop.claim", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    unsigned repeats0 = 0, repeats1 = 0;
    const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    const unsigned claims0 = sn->claims, commits0 = sn->commits;
    ph_set_input_hold(p, CLIENT, true);
    ph_refuse_resets(p, SERVER, 1000);
    REQUIRE(moq_session_abort_fetch(srv, sfetch, 0x11, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    REQUIRE(sn->claims == claims0 + 1);           /* the claim is held */
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(d.reset == 0 && r.reset == 0);

    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.reset == 0 && r.reset == 0 && r.fin == 0);
    CHECK(sn->finals_cancelled == 0);

    ph_refuse_resets(p, SERVER, 0);
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == 0x11 && d.fin == 0);
    CHECK(r.reset != 0 && r.reset_code == 0x11 && r.fin == 0);
    CHECK(sn->commits == commits0 + 1);
    CHECK(sn->finals_cancelled == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 2);
    CHECK(repeats1 == repeats0);
    ph_set_input_hold(p, CLIENT, false);
    CHECK(ph_run(p, PASSES));
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 2);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);

    /* The freed slot, a new generation, an ordinary completion. */
    moq_fetch_t cnext;
    moq_fetch_t snext = serve_fetch_named(p, "more", &cnext);
    REQUIRE(moq_handle_slot(snext._opaque) == moq_handle_slot(sfetch._opaque));
    REQUIRE(moq_handle_generation(snext._opaque) != moq_handle_generation(sfetch._opaque));
    REQUIRE(moq_session_end_fetch(srv, snext, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t datas[2], reqs[2];
    REQUIRE(ph_local_data_refs(p, SERVER, datas, 2) == 2 && datas[0]._v == data._v);
    REQUIRE(ph_served_request_refs(p, SERVER, reqs, 2) == 2 && reqs[0]._v == req._v);
    ph_stream_trace_t nd = data_trace(p, datas[1]), nr = request_trace(p, reqs[1]);
    CHECK(nd.fin != 0 && nr.fin != 0 && nd.fin < nr.fin);
    CHECK(nd.reset == 0 && nr.reset == 0);
    CHECK(sn->final_policies == 2 && sn->finals_cancelled == 0);
    log_t c = {0};
    drain(p, CLIENT, &c);
    int done = index_of(&c, MOQ_EVENT_FETCH_COMPLETE);
    CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 1);
    if (done >= 0) CHECK(moq_fetch_eq(c.e[done].fetch, cnext));
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. The session's reset of the request stream is already
 * queued, and not polled, when the STOP is taken: the FETCH is aborted with
 * 0x11 and the peer stops the response stream before this side polls
 * anything. Through the session's STOP outcome, the abort's data reset is
 * the one that follows (RESET_FOLLOWS). The STOP the session took lets it
 * be claimed WAIVED although its cause is the abort: it goes with 0x11,
 * delivering what the transport took. The request's queued reset goes as
 * it is, with 0x11: no CANCELLED one replaces it and no second one
 * follows. The client reads nothing until both resets are out, as in
 * host_fetch_stop_claim. */
static void host_fetch_stop_native_queued(moq_version_t version)
{
    name_case("host.fetch_stop.native_queued", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    unsigned repeats0 = 0, repeats1 = 0;
    const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    const unsigned claims0 = sn->claims;
    ph_set_input_hold(p, CLIENT, true);
    REQUIRE(moq_session_abort_fetch(ph_session(p, SERVER), sfetch, 0x11, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == 0x11 && d.fin == 0);
    CHECK(d.reset_reliable == d.bytes);
    CHECK(r.reset != 0 && r.reset_code == 0x11 && r.fin == 0);
    CHECK(sn->claims == claims0 + 1);
    CHECK(sn->finals_cancelled == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 2);
    CHECK(repeats1 == repeats0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    ph_set_input_hold(p, CLIENT, false);
    CHECK(ph_run(p, PASSES));
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 2);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

static void refused_fetch_update(ph_pair_t *p, moq_version_t version,
                                 moq_fetch_t cfetch);

/* Fetch terminal events in a log: one FETCH ends once. */
static size_t fetch_terminals(const log_t *l)
{
    return count_kind(l, MOQ_EVENT_FETCH_ERROR) + count_kind(l, MOQ_EVENT_FETCH_CANCELLED) +
           count_kind(l, MOQ_EVENT_FETCH_COMPLETE) + count_kind(l, MOQ_EVENT_FETCH_RESET);
}

/* Drafts 18 and 22. A STOP of the request stream crosses a reset the
 * session already queued for it. The FETCH is aborted with 0x11 and, before
 * this side polls anything, the peer stops the request stream with 0x2B;
 * the client reads as it likes. The request stream is reset once, with the
 * abort's 0x11, never with the STOP's code and never with a FIN; the data
 * stream is reset with 0x11 too. The server reports nothing for its own
 * abort, and the client sees the FETCH end once. */
static void host_fetch_stop_request_crossing(moq_version_t version)
{
    name_case("host.fetch_stop.request_crossing", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    unsigned repeats0 = 0, repeats1 = 0;
    const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    const unsigned answers0 = sn->stop_answers;
    REQUIRE(moq_session_abort_fetch(ph_session(p, SERVER), sfetch, 0x11, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_peer_stops_served_request(p, SERVER, 0x2B));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(r.reset != 0 && r.reset_code == 0x11 && r.fin == 0);
    CHECK(d.reset != 0 && d.reset_code == 0x11 && d.fin == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 2);
    CHECK(repeats1 == repeats0);
    CHECK(sn->stop_answers == answers0);
    CHECK(sn->finals_cancelled == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0}, c = {0};
    drain(p, SERVER, &s);
    drain(p, CLIENT, &c);
    CHECK(fetch_terminals(&s) == 0);
    CHECK(fetch_terminals(&c) == 1);
    CHECK(ph_run(p, PASSES));
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 2);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. An earlier native reset, then the peer's STOP, with the
 * response stream's header unsent (the transport refuses the stream from
 * its open) or partly accepted (it takes one byte of the header). The FETCH
 * is aborted with 0x11 and the response stream stopped with 0x2A before
 * this side polls. The session takes the STOP (RESET_FOLLOWS), so the
 * abort's reset is claimed WAIVED: it keeps the abort's 0x11 and delivers
 * exactly the bytes the transport took, nothing held being written after
 * the STOP, even once the refusal lifts. The request stream's own reset
 * from the abort goes with 0x11: one reset per stream, no FIN, nothing
 * replaced, no event at the server, one end of the FETCH at the client. */
static void host_fetch_stop_waived(moq_version_t version, bool partial)
{
    name_case(partial ? "host.fetch_stop.waived_partial" : "host.fetch_stop.waived_unsent",
              version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_session_t *srv = ph_session(p, SERVER);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = request_fetch(p, "past", &cfetch);
    const uint64_t taken = partial ? 1 : 0;
    REQUIRE(ph_refuse_data_stream(p, SERVER, moq_stream_ref_from_u64(0)));
    ph_set_refusal_allowance(p, SERVER, taken);
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    REQUIRE(moq_session_accept_fetch(srv, sfetch, &ac, ph_now(p)) == MOQ_OK);
    write_fetch(p, sfetch, 0, "f-zero");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    ph_stream_trace_t d = data_trace(p, data);
    REQUIRE(d.bytes == taken && d.reset == 0);
    unsigned repeats0 = 0, repeats1 = 0;
    const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    const unsigned claims0 = sn->claims, commits0 = sn->commits;

    REQUIRE(moq_session_abort_fetch(srv, sfetch, 0x11, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    ph_stream_trace_t r = request_trace(p, req);
    CHECK(sn->claims == claims0 + 1 && sn->commits == commits0 + 1);
    CHECK(d.reset != 0 && d.reset_code == 0x11 && d.fin == 0);
    CHECK(d.reset_reliable == taken && d.bytes == taken);
    CHECK(r.reset != 0 && r.reset_code == 0x11 && r.fin == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 2);
    CHECK(repeats1 == repeats0);
    CHECK(sn->finals_cancelled == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0}, c = {0};
    drain(p, SERVER, &s);
    CHECK(fetch_terminals(&s) == 0);

    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data);
    CHECK(d2.bytes == taken && d2.writes == d.writes && d2.reset == d.reset);
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 2);
    drain(p, CLIENT, &c);
    CHECK(fetch_terminals(&c) == 1);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. The obligation of the first STOP is still pending when
 * the same STOP reaches the host again. The FETCH is ended while the
 * transport refuses its response stream; the peer stops it with 0x2A, and
 * the transport refuses this side's reset of it, so that reset is owed.
 * The STOP is delivered to the host twice more while the reset is owed
 * (with another code each time); the peer then stops the request stream,
 * whose final message the first STOP already replaced, and that STOP too
 * is delivered twice more. Each delivery reaches the host; none adds an
 * obligation. Once resets are taken, each
 * stream is reset once: the response stream with 0x2A, the request stream
 * with CANCELLED; one final message is replaced; nothing has a FIN. */
static void host_fetch_stop_redelivered(moq_version_t version)
{
    name_case("host.fetch_stop.redelivered", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    REQUIRE(ph_refuse_data_stream(p, SERVER, data));
    REQUIRE(moq_session_end_fetch(ph_session(p, SERVER), sfetch, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    unsigned repeats0 = 0, repeats1 = 0;
    const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
    const unsigned entries0 = sn->stop_entries;
    ph_refuse_resets(p, SERVER, 1000);
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    REQUIRE(d.reset == 0);                        /* this side's reset is owed */
    CHECK(r.reset != 0 && r.reset_code == MOQ_STREAM_ERROR_CANCELLED);
    CHECK(sn->stop_entries == entries0 + 1);

    REQUIRE(ph_redeliver_stop(p, SERVER, data, 0x2B));
    CHECK(ph_run(p, PASSES));
    REQUIRE(ph_redeliver_stop(p, SERVER, data, 0x2C));
    REQUIRE(ph_peer_stops_served_request(p, SERVER, 0x6));
    CHECK(ph_run(p, PASSES));
    CHECK(sn->stop_entries == entries0 + 4);
    REQUIRE(ph_redeliver_stop(p, SERVER, req, 0x5));
    CHECK(ph_run(p, PASSES));
    REQUIRE(ph_redeliver_stop(p, SERVER, req, 0x7));
    CHECK(ph_run(p, PASSES));
    CHECK(sn->stop_entries == entries0 + 6);
    d = data_trace(p, data);
    CHECK(d.reset == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 1);

    ph_refuse_resets(p, SERVER, 0);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data), r2 = request_trace(p, req);
    CHECK(d2.reset != 0 && d2.reset_code == 0x2A && d2.fin == 0);
    CHECK(r2.reset == r.reset && r2.reset_code == MOQ_STREAM_ERROR_CANCELLED && r2.fin == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 2);
    CHECK(repeats1 == repeats0);
    CHECK(sn->finals_cancelled == 1 && sn->stop_answers == 0);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. A held claim meets a STOP while the FETCH's final
 * message waits to be replaced. A refused REQUEST_UPDATE ends the FETCH.
 * This side polls one action a pass, so it takes the final message
 * (REQUEST_ERROR with FIN), which its fence holds, before the response
 * stream's reset (CANCELLED) is offered; it claims that reset, and the
 * transport refuses it, so the claim is held and the final message waits
 * behind it. The peer's STOP of the response stream meets the held
 * claim with no session call; the claim keeps its code, and the final
 * message, still held, becomes the request stream's CANCELLED reset: never
 * written, no FIN, once. Once resets are taken the claimed reset goes with
 * CANCELLED, delivering what the transport took, and commits once. */
static void host_fetch_stop_claim_final(moq_version_t version)
{
    name_case("host.fetch_stop.claim_final", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    (void)serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    const unsigned rwrites = request_trace(p, req).writes;
    const unsigned waits = sn->final_waits, claims0 = sn->claims, commits0 = sn->commits;
    ph_refuse_resets(p, SERVER, 1000);
    ph_set_poll_budget(p, SERVER, 1);
    refused_fetch_update(p, version, cfetch);
    CHECK(ph_run(p, PASSES));
    REQUIRE(sn->claims == claims0 + 1);           /* the claim is held */
    REQUIRE(sn->final_waits > waits);             /* ... and the final message */
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    REQUIRE(d.reset == 0 && r.reset == 0 && r.fin == 0 && r.writes == rwrites);
    CHECK(sn->finals_cancelled == 0);

    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.reset == 0);
    CHECK(r.reset != 0 && r.reset_code == MOQ_STREAM_ERROR_CANCELLED);
    CHECK(r.fin == 0 && r.writes == rwrites);
    CHECK(sn->finals_cancelled == 1);

    ph_refuse_resets(p, SERVER, 0);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data), r2 = request_trace(p, req);
    CHECK(d2.reset != 0 && d2.reset_code == MOQ_STREAM_ERROR_CANCELLED && d2.fin == 0);
    CHECK(d2.reset_reliable == d2.bytes);
    CHECK(r2.reset == r.reset && r2.fin == 0 && r2.writes == rwrites);
    CHECK(sn->commits == commits0 + 1 && sn->finals_cancelled == 1);
    CHECK(ph_held_outputs(p, SERVER) == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    CHECK(ph_held_outputs(p, CLIENT) == 0);
    CHECK(ph_pair_destroy(p) == 0);
}

/* Drafts 18 and 22, in the polled order without tags and in each stream's
 * own order with them. The transport refuses the served request stream
 * (FETCH_OK and everything after it on that stream) and is never lifted.
 * With native_abort the FETCH is then aborted with 0x11 while the client
 * reads nothing, so no STOP of its own frees the stream: the request
 * stream's held bytes are obsolete, none is written, and its abort goes at
 * once, reset with 0x11 and the client's direction stopped; the response
 * stream is reset with 0x11. Without it the FETCH ends ordinarily: the
 * request stream's FIN stays behind its held bytes, never written while
 * they are refused, and follows them once the refusal lifts. */
static void host_request_teardown_held(moq_version_t version, bool tagged,
                                       bool native_abort)
{
    name_case(native_abort ? (tagged ? "host.request_teardown.abort_tagged"
                                     : "host.request_teardown.abort")
                           : (tagged ? "host.request_teardown.fin_tagged"
                                     : "host.request_teardown.fin"),
              version);
    ph_options_t o = fence_server();
    if (!tagged) memset(&o, 0, sizeof(o));
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    moq_session_t *srv = ph_session(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = request_fetch(p, "past", &cfetch);
    moq_stream_ref_t req;
    REQUIRE(ph_served_request_refs(p, SERVER, &req, 1) == 1);
    REQUIRE(ph_refuse_request_stream(p, SERVER, req));
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    REQUIRE(moq_session_accept_fetch(srv, sfetch, &ac, ph_now(p)) == MOQ_OK);
    write_fetch(p, sfetch, 0, "f-zero");
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t r = request_trace(p, req);
    REQUIRE(r.writes == 0 && r.fin == 0 && r.reset == 0);

    if (native_abort) {
        unsigned repeats0 = 0, repeats1 = 0;
        const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);
        ph_set_input_hold(p, CLIENT, true);
        REQUIRE(moq_session_abort_fetch(srv, sfetch, 0x11, ph_now(p)) == MOQ_OK);
        CHECK(ph_run(p, PASSES));
        moq_stream_ref_t data;
        REQUIRE(ph_local_data_refs(p, SERVER, &data, 1) == 1);
        ph_stream_trace_t d = data_trace(p, data);
        r = request_trace(p, req);
        CHECK(r.reset != 0 && r.reset_code == 0x11);
        CHECK(r.writes == 0 && r.fin == 0);
        CHECK(d.reset != 0 && d.reset_code == 0x11 && d.fin == 0);
        CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0 + 2);
        CHECK(repeats1 == repeats0);
        CHECK(ph_held_outputs(p, SERVER) == 0);
        log_t s = {0};
        drain(p, SERVER, &s);
        CHECK(fetch_terminals(&s) == 0);
        ph_set_input_hold(p, CLIENT, false);
        CHECK(ph_run(p, PASSES));
        CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 2);
    } else {
        REQUIRE(moq_session_end_fetch(srv, sfetch, ph_now(p)) == MOQ_OK);
        CHECK(ph_run(p, PASSES));
        r = request_trace(p, req);
        CHECK(r.writes == 0 && r.fin == 0 && r.reset == 0);
        ph_lift_data_refusal(p, SERVER);
        CHECK(ph_run(p, PASSES));
        r = request_trace(p, req);
        CHECK(r.writes >= 1 && r.fin != 0 && r.fin > r.last_write && r.reset == 0);
        log_t c = {0};
        drain(p, CLIENT, &c);
        CHECK(count_kind(&c, MOQ_EVENT_FETCH_COMPLETE) == 1);
    }
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Drafts 18 and 22. One STOP's effect, once. After the ended FETCH's
 * streams are both reset (as in host_fetch_final_stop), a second STOP of
 * the response stream and a STOP of the request stream change nothing:
 * there is no second reset of either stream, no repeat, no FIN, and no
 * event. */
static void host_fetch_stop_duplicate(moq_version_t version)
{
    name_case("host.fetch_stop.duplicate", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    REQUIRE(ph_refuse_data_stream(p, SERVER, data));
    REQUIRE(moq_session_end_fetch(ph_session(p, SERVER), sfetch, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    REQUIRE(d.reset != 0 && r.reset != 0);
    unsigned repeats0 = 0, repeats1 = 0;
    const unsigned resets0 = ph_transport_resets(p, SERVER, &repeats0);

    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2B));
    CHECK(ph_peer_stops_served_request(p, SERVER, 0x5));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d2 = data_trace(p, data), r2 = request_trace(p, req);
    CHECK(d2.reset == d.reset && d2.reset_code == 0x2A && d2.fin == 0);
    CHECK(r2.reset == r.reset && r2.reset_code == MOQ_STREAM_ERROR_CANCELLED && r2.fin == 0);
    CHECK(ph_transport_resets(p, SERVER, &repeats1) == resets0);
    CHECK(repeats1 == repeats0);
    CHECK(sn->finals_cancelled == 1);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* Draft 16, the control for RESET_FINAL_ON_STOP. A FETCH there has no
 * request stream to pair with: FETCH_OK and its end travel on the control
 * stream. So its response stream is opened without the policy. The FETCH
 * is ended while the transport refuses that stream, then stopped: only that
 * stream is reset, with the STOP's code, and the control stream carries
 * on. */
static void host_fetch_stop_no_policy(moq_version_t version)
{
    name_case("host.fetch_stop.no_policy", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    moq_fetch_t sfetch = serve_fetch(p, &cfetch);
    moq_stream_ref_t data, none;
    REQUIRE(ph_only_open_local_data(p, SERVER, &data));
    REQUIRE(ph_served_request_refs(p, SERVER, &none, 1) == 0);
    CHECK(sn->final_policies == 0);
    REQUIRE(ph_refuse_data_stream(p, SERVER, data));
    REQUIRE(moq_session_end_fetch(ph_session(p, SERVER), sfetch, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    const unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    REQUIRE(ph_peer_stops_data(p, SERVER, data, 0x2A));
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data);
    CHECK(d.reset != 0 && d.reset_code == 0x2A && d.fin == 0);
    CHECK(ph_transport_resets(p, SERVER, NULL) == resets0 + 1);
    CHECK(sn->final_policies == 0 && sn->finals_cancelled == 0);
    CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_ESTABLISHED);
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    close_pair(p);
}

/* A REQUEST_UPDATE of the FETCH on its request stream that the server
 * refuses. A draft-22 fetcher sends its own, carrying one token twice (a
 * duplicate token is malformed). A draft-18 fetcher sends no update, so its
 * transport writes one, using a token alias never registered. */
static void refused_fetch_update(ph_pair_t *p, moq_version_t version,
                                 moq_fetch_t cfetch)
{
    if (version == MOQ_VERSION_DRAFT_22) {
        static const uint8_t value[] = {0x74};
        const moq_auth_token_t twice[2] = {
            {.token_type = 1, .token_value = {.data = value, .len = sizeof(value)}},
            {.token_type = 1, .token_value = {.data = value, .len = sizeof(value)}},
        };
        moq_fetch_update_cfg_t uc;
        moq_fetch_update_cfg_init(&uc);
        uc.auth_tokens = twice;
        uc.auth_token_count = 2;
        REQUIRE(moq_session_update_fetch(ph_session(p, CLIENT), cfetch, &uc,
                                         ph_now(p)) == MOQ_OK);
        return;
    }
    uint8_t buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_d18_msg_params_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.auth_token_count = 1;
    prm.auth_tokens[0].alias_type = 2;        /* USE_ALIAS */
    prm.auth_tokens[0].alias = 99;
    REQUIRE(moq_d18_encode_request_update(&w, 10, &prm) == MOQ_OK);
    REQUIRE(ph_peer_writes_served_request(p, SERVER, buf, moq_buf_writer_offset(&w)));
}

/* Drafts 18 and 22. A refused REQUEST_UPDATE ends the FETCH with a reset of
 * its response stream (CANCELLED) and REQUEST_ERROR with FIN on its request
 * stream, the FETCH's final message. While the transport refuses resets the
 * final message waits: nothing more is written on the request stream. Once
 * the reset is accepted the REQUEST_ERROR and its FIN follow it, and the
 * request stream is not reset. One FETCH_CANCELLED reports the end at the
 * refusal, while the final message still waits. The draft-22
 * fetcher sees its update refused (FETCH_ERROR). The draft-18 fetcher's
 * session never sent the update, so it takes the answer as a protocol
 * violation and closes the session. */
static void host_fetch_final_refused_update(moq_version_t version)
{
    name_case("host.fetch_final.update", version);
    ph_options_t o = fence_server();
    ph_pair_t *p = open_pair(version, &o);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    moq_fetch_t cfetch;
    (void)serve_fetch(p, &cfetch);
    moq_stream_ref_t data, req;
    fetch_streams(p, &data, &req);
    const unsigned rwrites = request_trace(p, req).writes;
    const unsigned waits = sn->final_waits, refused = sn->resets_refused;
    ph_refuse_resets(p, SERVER, 1000);
    refused_fetch_update(p, version, cfetch);
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t d = data_trace(p, data), r = request_trace(p, req);
    CHECK(sn->resets_refused > refused);
    CHECK(d.reset == 0 && d.fin == 0);
    CHECK(r.writes == rwrites && r.fin == 0);     /* the fence holds it */
    CHECK(r.reset == 0);
    CHECK(sn->final_waits > waits);
    /* The end is reported at the refusal, not behind the fence. */
    log_t s = {0};
    drain(p, SERVER, &s);
    CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 1);

    ph_refuse_resets(p, SERVER, 0);
    CHECK(ph_run(p, PASSES));
    d = data_trace(p, data);
    r = request_trace(p, req);
    CHECK(d.reset != 0 && d.reset_code == MOQ_STREAM_ERROR_CANCELLED);
    CHECK(r.writes == rwrites + 1 && d.reset < r.last_write);
    CHECK(r.fin != 0 && d.reset < r.fin);
    CHECK(r.reset == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    if (version == MOQ_VERSION_DRAFT_22) {
        log_t c = {0};
        s.n = 0;
        drain(p, SERVER, &s);
        drain(p, CLIENT, &c);
        CHECK(count_kind(&s, MOQ_EVENT_FETCH_CANCELLED) == 0);
        int err = index_of(&c, MOQ_EVENT_FETCH_ERROR);
        CHECK(count_kind(&c, MOQ_EVENT_FETCH_ERROR) == 1);
        if (err >= 0) {
            CHECK(moq_fetch_eq(c.e[err].fetch, cfetch));
            CHECK(c.e[err].code == MOQ_REQUEST_ERROR_MALFORMED_AUTH_TOKEN);
        }
        close_pair(p);
        return;
    }
    uint64_t code = 0;
    CHECK(ph_transport_closed(p, CLIENT, &code) && code == PROTOCOL_VIOLATION);
    CHECK(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_CLOSED);
    drain(p, CLIENT, NULL);
    drain(p, SERVER, NULL);
    CHECK(ph_held_outputs(p, CLIENT) == 0 && ph_held_outputs(p, SERVER) == 0);
    CHECK(ph_pair_destroy(p) == 0);
}

/* MOQ_HOST_ROW=<row function>:<draft> runs that one row alone, in that
 * draft (a focused run of one case), and per_stream.<row function>:<draft>
 * the same in the per-stream pass; unset, every row runs. */
static const char *only_row;
static bool row_matched;
static bool row_wanted(const char *fn, unsigned draft)
{
    if (!only_row) return true;
    char want[96];
    (void)snprintf(want, sizeof(want), "%s%s:%u", per_stream_pass ? "per_stream." : "",
                   fn, draft);
    if (strcmp(want, only_row) != 0) return false;
    row_matched = true;
    return true;
}

int main(void)
{
    static const moq_version_t all[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    only_row = getenv("MOQ_HOST_ROW");
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
        if (row_wanted("setup", (unsigned)all[i])) setup(all[i], 0, 0);
        if (row_wanted("setup", (unsigned)all[i])) setup(all[i], 3, 1);
        if (row_wanted("publish_subscribe", (unsigned)all[i])) publish_subscribe(all[i], 0, 0);
        if (row_wanted("publish_subscribe", (unsigned)all[i])) publish_subscribe(all[i], 3, 1);
        if (row_wanted("server_publication", (unsigned)all[i])) server_publication(all[i]);
        if (row_wanted("fetch_lifecycle", (unsigned)all[i])) fetch_lifecycle(all[i]);
        if (row_wanted("event_pressure", (unsigned)all[i])) event_pressure(all[i]);
        if (row_wanted("refused_then_admitted", (unsigned)all[i])) refused_then_admitted(all[i]);
        if (row_wanted("retire_after_stop", (unsigned)all[i])) retire_after_stop(all[i]);
        if (row_wanted("retire_fill", (unsigned)all[i])) retire_fill(all[i]);
        if (all[i] != MOQ_VERSION_DRAFT_16 && row_wanted("stop_on_local_request", (unsigned)all[i]))
            stop_on_local_request(all[i]);
        if (row_wanted("truncated_types", (unsigned)all[i])) truncated_types(all[i]);
        if (row_wanted("control_stream_end", (unsigned)all[i])) control_stream_end(all[i], SERVER, false);
        if (row_wanted("control_stream_end", (unsigned)all[i])) control_stream_end(all[i], CLIENT, false);
        if (row_wanted("control_stream_end", (unsigned)all[i])) control_stream_end(all[i], SERVER, true);
        if (row_wanted("control_stream_end", (unsigned)all[i])) control_stream_end(all[i], CLIENT, true);
        if (row_wanted("tagged_enable", (unsigned)all[i])) tagged_enable(all[i]);
        if (row_wanted("tagged_stream_count", (unsigned)all[i])) tagged_stream_count(all[i], false);
        if (row_wanted("tagged_stream_count", (unsigned)all[i])) tagged_stream_count(all[i], true);
        if (row_wanted("tagged_unopened", (unsigned)all[i])) tagged_unopened(all[i]);
        if (row_wanted("tagged_claim_roundtrip", (unsigned)all[i])) tagged_claim_roundtrip(all[i]);
        if (row_wanted("tagged_waived_after_stop", (unsigned)all[i]))
            tagged_waived_after_stop(all[i]);
        if (row_wanted("tagged_waived_after_expiry", (unsigned)all[i]))
            tagged_waived_after_expiry(all[i]);
        if (row_wanted("tagged_refused_reset", (unsigned)all[i])) tagged_refused_reset(all[i]);
        if (row_wanted("tagged_claim_closed", (unsigned)all[i])) tagged_claim_closed(all[i]);
        if (row_wanted("fetch_data_stop", (unsigned)all[i])) fetch_data_stop(all[i]);
        if (row_wanted("fetch_first_cause", (unsigned)all[i])) fetch_first_cause(all[i]);
        if (row_wanted("fill_two_stops", (unsigned)all[i])) fill_two_stops(all[i]);
        if (row_wanted("held_expiry", (unsigned)all[i])) held_expiry(all[i]);
        if (row_wanted("tagged_handed_unopened", (unsigned)all[i])) tagged_handed_unopened(all[i]);
        if (row_wanted("tagged_unopened_owed", (unsigned)all[i]))
            tagged_unopened_owed(all[i]);
        if (row_wanted("tagged_unopened_owed_cancel", (unsigned)all[i])) {
            tagged_unopened_owed_cancel(all[i], false);
            if (all[i] == MOQ_VERSION_DRAFT_16) tagged_unopened_owed_cancel(all[i], true);
        }
        if (row_wanted("host_held_expiry", (unsigned)all[i])) host_held_expiry(all[i], false);
        if (row_wanted("host_held_expiry", (unsigned)all[i])) host_held_expiry(all[i], true);
        if (row_wanted("host_datagram_expiry", (unsigned)all[i])) host_datagram_expiry(all[i]);
        if (row_wanted("host_final_fence", (unsigned)all[i])) host_final_fence(all[i]);
        if (row_wanted("host_final_fence_datagram", (unsigned)all[i]))
            host_final_fence_datagram(all[i]);
        if (row_wanted("host_blocked_tick", (unsigned)all[i])) host_blocked_tick(all[i]);
        if (row_wanted("host_reset_before_expiry", (unsigned)all[i]))
            host_reset_before_expiry(all[i]);
        if (row_wanted("host_goaway_behind_blocked_tick", (unsigned)all[i]))
            host_goaway_behind_blocked_tick(all[i]);
        /* A FETCH's request stream exists on drafts 18 and 22 alone. */
        if (all[i] != MOQ_VERSION_DRAFT_16) {
            if (row_wanted("host_fetch_final_fin", (unsigned)all[i]))
                host_fetch_final_fin(all[i]);
            if (row_wanted("host_fetch_final_empty", (unsigned)all[i]))
                host_fetch_final_empty(all[i]);
            if (row_wanted("host_fetch_cancel_blocked", (unsigned)all[i]))
                host_fetch_cancel_blocked(all[i]);
            if (row_wanted("host_fetch_final_stop", (unsigned)all[i]))
                host_fetch_final_stop(all[i]);
            if (row_wanted("host_fetch_final_refused_update", (unsigned)all[i]))
                host_fetch_final_refused_update(all[i]);
            /* A STOP of the response stream resets the paired request
             * stream (RESET_FINAL_ON_STOP). */
            if (row_wanted("host_fetch_stop_unconsumed", (unsigned)all[i]))
                host_fetch_stop_unconsumed(all[i], true);
            if (row_wanted("host_fetch_stop_unconsumed", (unsigned)all[i]))
                host_fetch_stop_unconsumed(all[i], false);
            if (row_wanted("host_fetch_stop_before_final", (unsigned)all[i]))
                host_fetch_stop_before_final(all[i]);
            if (row_wanted("host_fetch_stop_claim", (unsigned)all[i]))
                host_fetch_stop_claim(all[i]);
            if (row_wanted("host_fetch_stop_native_queued", (unsigned)all[i]))
                host_fetch_stop_native_queued(all[i]);
            if (row_wanted("host_fetch_stop_duplicate", (unsigned)all[i]))
                host_fetch_stop_duplicate(all[i]);
            if (row_wanted("host_fetch_stop_request_crossing", (unsigned)all[i]))
                host_fetch_stop_request_crossing(all[i]);
            if (row_wanted("host_fetch_stop_waived", (unsigned)all[i]))
                host_fetch_stop_waived(all[i], false);
            if (row_wanted("host_fetch_stop_waived", (unsigned)all[i]))
                host_fetch_stop_waived(all[i], true);
            if (row_wanted("host_fetch_stop_redelivered", (unsigned)all[i]))
                host_fetch_stop_redelivered(all[i]);
            if (row_wanted("host_fetch_stop_claim_final", (unsigned)all[i]))
                host_fetch_stop_claim_final(all[i]);
            /* A teardown discards the request stream's held bytes; a FIN
             * waits behind them. */
            for (int k = 0; k < 4; k++)
                if (row_wanted("host_request_teardown_held", (unsigned)all[i]))
                    host_request_teardown_held(all[i], (k & 1) != 0, (k & 2) != 0);
        } else if (row_wanted("host_fetch_stop_no_policy", (unsigned)all[i])) {
            host_fetch_stop_no_policy(all[i]);
        }
    }
    /* The tagged rows again, with the server keeping each stream's output
     * order alone: every rule they pin holds in that order too. */
    per_stream_pass = true;
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
        const unsigned d = (unsigned)all[i];
        if (row_wanted("tagged_enable", d)) tagged_enable(all[i]);
        if (row_wanted("tagged_stream_count", d)) tagged_stream_count(all[i], false);
        if (row_wanted("tagged_stream_count", d)) tagged_stream_count(all[i], true);
        if (row_wanted("tagged_unopened", d)) tagged_unopened(all[i]);
        if (row_wanted("tagged_claim_roundtrip", d)) tagged_claim_roundtrip(all[i]);
        if (row_wanted("tagged_waived_after_stop", d)) tagged_waived_after_stop(all[i]);
        if (row_wanted("tagged_waived_after_expiry", d)) tagged_waived_after_expiry(all[i]);
        if (row_wanted("tagged_refused_reset", d)) tagged_refused_reset(all[i]);
        if (row_wanted("tagged_claim_closed", d)) tagged_claim_closed(all[i]);
        if (row_wanted("held_expiry", d)) held_expiry(all[i]);
        if (row_wanted("tagged_handed_unopened", d)) tagged_handed_unopened(all[i]);
        if (row_wanted("tagged_unopened_owed", d)) tagged_unopened_owed(all[i]);
        if (row_wanted("tagged_unopened_owed_cancel", d)) {
            tagged_unopened_owed_cancel(all[i], false);
            if (all[i] == MOQ_VERSION_DRAFT_16) tagged_unopened_owed_cancel(all[i], true);
        }
        if (row_wanted("host_held_expiry", d)) host_held_expiry(all[i], false);
        if (row_wanted("host_held_expiry", d)) host_held_expiry(all[i], true);
        if (row_wanted("host_datagram_expiry", d)) host_datagram_expiry(all[i]);
        if (row_wanted("host_final_fence", d)) host_final_fence(all[i]);
        if (row_wanted("host_final_fence_datagram", d)) host_final_fence_datagram(all[i]);
        if (row_wanted("host_blocked_tick", d)) host_blocked_tick(all[i]);
        if (row_wanted("host_reset_before_expiry", d)) host_reset_before_expiry(all[i]);
        if (row_wanted("host_goaway_behind_blocked_tick", d))
            host_goaway_behind_blocked_tick(all[i]);
    }
    per_stream_pass = false;
    if (only_row && !row_matched) {
        fprintf(stderr, "SETUP[selector] MOQ_HOST_ROW=%s names no row\n", only_row);
        return 2;
    }
    if (failures) {
        fprintf(stderr, "%u of %u checks failed\n", failures, checks);
        return 1;
    }
    printf("PASS: public_host (%u checks)\n", checks);
    return 0;
}
