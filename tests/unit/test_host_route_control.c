/*
 * Two operations a transport owner needs from the session and cannot derive
 * from its inputs and polls:
 *
 *   moq_session_route_peer_uni()          where a peer unidirectional stream
 *                                         belongs, from its leading bytes;
 *   moq_session_on_control_stream_ended() a control stream ended while the
 *                                         session was alive; an idle deadline
 *                                         already passed keeps precedence.
 *
 * Public headers only. Sessions are established through real inputs.
 */
#include <moq/moq.h>
#include <moq/vi64.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test_binding_support.h"

static unsigned failures, checks;
static const char *label = "";
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
} } while (0)
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "SETUP[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
    exit(2); \
} } while (0)

/* The setup a server answers with, as the client's control input. */
static const uint8_t SETUP_D16[] = {0x21, 0, 1, 0, 0x15, 0, 1, 32};
static const uint8_t SETUP_MODERN[] = {0xaf, 0, 0, 0};

#define PROTOCOL_VIOLATION UINT64_C(0x3)

typedef struct rig {
    moq_session_t *s;
    moq_version_t version;
    uint64_t now;
} rig_t;

static void rig_create_idle(rig_t *r, moq_version_t version, uint32_t max_events,
                            uint32_t max_actions, uint64_t idle_us)
{
    memset(r, 0, sizeof(*r));
    r->version = version;
    r->now = 1;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.version = version;
    cfg.max_events = max_events;
    cfg.max_actions = max_actions;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 32;
    cfg.idle_timeout_us = idle_us;
    REQUIRE(moq_session_create(&cfg, 0, &r->s) == MOQ_OK);
    REQUIRE(test_bind_if_required(r->s) == MOQ_OK);
}

static void rig_create(rig_t *r, moq_version_t version, uint32_t max_events,
                       uint32_t max_actions)
{
    rig_create_idle(r, version, max_events, max_actions, 0);
}

static size_t drain_actions(rig_t *r);

/* Start the client, hand its setup to the transport, and feed the peer's.
 * SETUP_COMPLETE and whatever the setup queued are left for the caller. */
static void rig_establish(rig_t *r)
{
    REQUIRE(moq_session_start(r->s, r->now) == MOQ_OK);
    REQUIRE(drain_actions(r) > 0);
    const uint8_t *wire = SETUP_MODERN;
    size_t n = sizeof(SETUP_MODERN);
    if (r->version == MOQ_VERSION_DRAFT_16) {
        wire = SETUP_D16;
        n = sizeof(SETUP_D16);
    }
    REQUIRE(moq_session_on_control_bytes(r->s, wire, n, ++r->now) == MOQ_OK);
    REQUIRE(moq_session_state(r->s) == MOQ_SESS_ESTABLISHED);
}

static void rig_destroy(rig_t *r)
{
    moq_session_destroy(r->s);
    r->s = NULL;
}

static size_t drain_actions(rig_t *r)
{
    size_t total = 0;
    moq_action_t a;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_actions(r->s, &a, 1) == 0) break;
        moq_action_cleanup(&a);
        total++;
    }
    return total;
}

static size_t drain_events(rig_t *r)
{
    size_t total = 0;
    moq_event_t e;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_events(r->s, &e, 1) == 0) break;
        moq_event_cleanup(&e);
        total++;
    }
    return total;
}

/* -- Route ----------------------------------------------------------- */

static moq_uni_route_t route_of(const rig_t *r, const uint8_t *data, size_t len)
{
    moq_uni_route_t route = 0xdeadbeefu;
    moq_result_t rc = moq_session_route_peer_uni(r->s, data, len, &route);
    CHECK(rc == MOQ_OK);
    return route;
}

/* One stream type: every proper prefix needs more, the whole type and the
 * type followed by further bytes give its route, and asking twice changes
 * nothing. */
static void route_type(const rig_t *r, uint64_t type, moq_uni_route_t expected)
{
    uint8_t buf[16];
    memset(buf, 0x5a, sizeof(buf));
    size_t n = moq_vi64_encode(type, buf, sizeof(buf));
    REQUIRE(n > 0 && n + 3 <= sizeof(buf));

    for (size_t split = 0; split < n; split++)
        CHECK(route_of(r, buf, split) == MOQ_UNI_ROUTE_NEED_MORE);
    CHECK(route_of(r, buf, n) == expected);
    CHECK(route_of(r, buf, n + 3) == expected);
    CHECK(route_of(r, buf, n) == expected);
}

static void route_modern(moq_version_t version)
{
    char name[40];
    (void)snprintf(name, sizeof(name), "route.d%u", (unsigned)version);
    label = name;
    rig_t r;
    rig_create(&r, version, 16, 16);

    /* The answer is the profile's, so it is the same before setup starts. */
    route_type(&r, UINT64_C(0x2F00), MOQ_UNI_ROUTE_CONTROL);

    rig_establish(&r);
    moq_session_state_t state = moq_session_state(r.s);
    uint64_t deadline = moq_session_next_deadline_us(r.s);
    moq_event_t held;
    REQUIRE(moq_session_poll_events(r.s, &held, 1) == 1);
    REQUIRE(held.kind == MOQ_EVENT_SETUP_COMPLETE);

    route_type(&r, UINT64_C(0x2F00), MOQ_UNI_ROUTE_CONTROL);
    route_type(&r, UINT64_C(0x132B3E28), MOQ_UNI_ROUTE_PADDING);
    route_type(&r, UINT64_C(0x05), MOQ_UNI_ROUTE_DATA);
    /* Subgroup header types are 0b0XX1XXXX. */
    route_type(&r, UINT64_C(0x10), MOQ_UNI_ROUTE_DATA);
    route_type(&r, UINT64_C(0x3D), MOQ_UNI_ROUTE_DATA);
    route_type(&r, UINT64_C(0x7F), MOQ_UNI_ROUTE_DATA);
    /* Complete types the profile does not define. */
    route_type(&r, UINT64_C(0x00), MOQ_UNI_ROUTE_UNKNOWN_TYPE);
    route_type(&r, UINT64_C(0x06), MOQ_UNI_ROUTE_UNKNOWN_TYPE);
    route_type(&r, UINT64_C(0x90), MOQ_UNI_ROUTE_UNKNOWN_TYPE);
    route_type(&r, UINT64_C(0x2F01), MOQ_UNI_ROUTE_UNKNOWN_TYPE);
    route_type(&r, UINT64_C(0x132B3E29), MOQ_UNI_ROUTE_UNKNOWN_TYPE);

    /* Observing: no state moved, no deadline moved, no borrow ended. */
    CHECK(moq_session_state(r.s) == state);
    CHECK(moq_session_next_deadline_us(r.s) == deadline);
    CHECK(moq_session_borrow_valid(r.s, held.borrow_epoch));
    moq_event_cleanup(&held);

    /* Arguments are checked before anything is routed, and a failed call
     * leaves no stale answer behind. */
    static const uint8_t one[] = {0x05};
    moq_uni_route_t route = 0xdeadbeefu;
    CHECK(moq_session_route_peer_uni(NULL, one, 1, &route) == MOQ_ERR_INVAL);
    CHECK(route == MOQ_UNI_ROUTE_NONE);
    CHECK(moq_session_route_peer_uni(r.s, one, 1, NULL) == MOQ_ERR_INVAL);
    route = 0xdeadbeefu;
    CHECK(moq_session_route_peer_uni(r.s, NULL, 1, &route) == MOQ_ERR_INVAL);
    CHECK(route == MOQ_UNI_ROUTE_NONE);
    route = 0xdeadbeefu;
    CHECK(moq_session_route_peer_uni(r.s, NULL, 0, &route) == MOQ_OK);
    CHECK(route == MOQ_UNI_ROUTE_NEED_MORE);

    /* A closed session routes nothing. */
    REQUIRE(moq_session_close(r.s, 0, NULL, ++r.now) == MOQ_OK);
    route = 0xdeadbeefu;
    CHECK(moq_session_route_peer_uni(r.s, one, 1, &route) == MOQ_ERR_CLOSED);
    CHECK(route == MOQ_UNI_ROUTE_NONE);

    rig_destroy(&r);
}

/* One bidirectional control stream: no unidirectional stream carries
 * control, so every input is data, an empty one included. */
static void route_bidi_control(void)
{
    label = "route.d16";
    rig_t r;
    rig_create(&r, MOQ_VERSION_DRAFT_16, 16, 16);
    rig_establish(&r);

    static const uint8_t setup_type[] = {0xaf, 0x00};
    static const uint8_t anything[] = {0x00, 0xff, 0x7f};
    CHECK(route_of(&r, NULL, 0) == MOQ_UNI_ROUTE_DATA);
    CHECK(route_of(&r, setup_type, sizeof(setup_type)) == MOQ_UNI_ROUTE_DATA);
    for (size_t n = 1; n <= sizeof(anything); n++)
        CHECK(route_of(&r, anything, n) == MOQ_UNI_ROUTE_DATA);

    REQUIRE(moq_session_close(r.s, 0, NULL, ++r.now) == MOQ_OK);
    moq_uni_route_t route = 0xdeadbeefu;
    CHECK(moq_session_route_peer_uni(r.s, anything, 1, &route) == MOQ_ERR_CLOSED);
    CHECK(route == MOQ_UNI_ROUTE_NONE);
    rig_destroy(&r);
}

/* -- Control stream ended -------------------------------------------- */

typedef struct close_outputs {
    size_t actions, close_actions, events, close_events;
    uint64_t action_code, event_code;
    char action_reason[64], event_reason[64];
} close_outputs_t;

static void copy_reason(char *out, size_t cap, moq_bytes_t reason)
{
    size_t n = reason.len < cap - 1 ? reason.len : cap - 1;
    if (n) memcpy(out, reason.data, n);
    out[n] = '\0';
}

/* Everything the closed session still has queued. */
static void collect_close(rig_t *r, close_outputs_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_action_t a;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_actions(r->s, &a, 1) == 0) break;
        out->actions++;
        if (a.kind == MOQ_ACTION_CLOSE_SESSION) {
            out->close_actions++;
            out->action_code = a.u.close_session.code;
            copy_reason(out->action_reason, sizeof(out->action_reason),
                        a.u.close_session.reason);
        }
        moq_action_cleanup(&a);
    }
    moq_event_t e;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_events(r->s, &e, 1) == 0) break;
        out->events++;
        if (e.kind == MOQ_EVENT_SESSION_CLOSED) {
            out->close_events++;
            out->event_code = e.u.closed.code;
            copy_reason(out->event_reason, sizeof(out->event_reason),
                        e.u.closed.reason);
        }
        moq_event_cleanup(&e);
    }
}

/* A live session closes as a protocol violation, and both outputs of a
 * local close are queued: the action that has the caller close the
 * transport, and the event. */
static void control_end_live(moq_version_t version)
{
    char name[40];
    (void)snprintf(name, sizeof(name), "control-end.live.d%u", (unsigned)version);
    label = name;
    rig_t r;
    rig_create(&r, version, 16, 16);
    rig_establish(&r);
    (void)drain_actions(&r);
    (void)drain_events(&r);

    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    CHECK(moq_session_state(r.s) == MOQ_SESS_CLOSED);
    close_outputs_t out;
    collect_close(&r, &out);
    CHECK(out.actions == 1 && out.close_actions == 1);
    CHECK(out.action_code == PROTOCOL_VIOLATION);
    CHECK(out.events == 1 && out.close_events == 1);
    CHECK(out.event_code == PROTOCOL_VIOLATION);
    CHECK(strcmp(out.action_reason, out.event_reason) == 0);
    CHECK(out.action_reason[0] != '\0');

    /* A second and a late report change nothing and queue nothing. */
    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    collect_close(&r, &out);
    CHECK(out.actions == 0 && out.events == 0);
    CHECK(moq_session_on_control_stream_ended(NULL, r.now) == MOQ_ERR_INVAL);
    rig_destroy(&r);
}

/* Before setup has completed the stream is as much the session's lifetime
 * as after: the same close. */
static void control_end_before_setup(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "control-end.before-setup.d%u",
                   (unsigned)version);
    label = name;
    rig_t r;
    rig_create(&r, version, 16, 16);
    REQUIRE(moq_session_start(r.s, r.now) == MOQ_OK);
    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    CHECK(moq_session_state(r.s) == MOQ_SESS_CLOSED);
    close_outputs_t out;
    collect_close(&r, &out);
    CHECK(out.close_actions == 1 && out.action_code == PROTOCOL_VIOLATION);
    CHECK(out.close_events == 1 && out.event_code == PROTOCOL_VIOLATION);
    rig_destroy(&r);
}

/* A close this side already chose keeps its code and its reason, on the
 * wire action as well as in the event. */
static void control_end_after_local_close(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "control-end.after-local-close.d%u",
                   (unsigned)version);
    label = name;
    rig_t r;
    rig_create(&r, version, 16, 16);
    rig_establish(&r);
    (void)drain_actions(&r);
    (void)drain_events(&r);

    static const char first[] = "first";
    REQUIRE(moq_session_close(r.s, 0x2, first, ++r.now) == MOQ_OK);
    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    close_outputs_t out;
    collect_close(&r, &out);
    CHECK(out.actions == 1 && out.close_actions == 1);
    CHECK(out.action_code == 0x2);
    CHECK(strcmp(out.action_reason, first) == 0);
    CHECK(out.events == 1 && out.close_events == 1);
    CHECK(out.event_code == 0x2);
    CHECK(strcmp(out.event_reason, first) == 0);
    rig_destroy(&r);
}

/* A session the peer's own protocol error has already closed, with its
 * close outputs still queued. */
static void rig_protocol_error(rig_t *r, moq_version_t version)
{
    rig_create(r, version, 16, 16);
    rig_establish(r);
    (void)drain_actions(r);
    (void)drain_events(r);

    /* A request message where only control messages belong: type 0x03, no
     * body. */
    uint8_t wire[12];
    size_t n = moq_vi64_encode(UINT64_C(0x03), wire, sizeof(wire));
    REQUIRE(n > 0 && n + 2 <= sizeof(wire));
    wire[n++] = 0;
    wire[n++] = 0;
    (void)moq_session_on_control_bytes(r->s, wire, n, ++r->now);
    REQUIRE(moq_session_state(r->s) == MOQ_SESS_CLOSED);
}

/* A close the session chose for an earlier protocol error keeps that
 * error's code and reason: the report does not restate it as its own. The
 * expected outputs are those of a second session closed the same way and
 * never told its control stream ended. */
static void control_end_after_protocol_error(moq_version_t version)
{
    char name[56];
    (void)snprintf(name, sizeof(name), "control-end.after-protocol-error.d%u",
                   (unsigned)version);
    label = name;
    rig_t plain, told;
    close_outputs_t want, out;
    rig_protocol_error(&plain, version);
    collect_close(&plain, &want);
    REQUIRE(want.close_actions == 1 && want.close_events == 1);
    /* The row tells the two closes apart by their reason. */
    REQUIRE(strcmp(want.action_reason, "control stream closed") != 0);

    rig_protocol_error(&told, version);
    CHECK(moq_session_on_control_stream_ended(told.s, ++told.now) == MOQ_OK);
    collect_close(&told, &out);
    CHECK(out.actions == 1 && out.close_actions == 1);
    CHECK(out.events == 1 && out.close_events == 1);
    CHECK(out.action_code == want.action_code);
    CHECK(out.event_code == want.event_code);
    CHECK(strcmp(out.action_reason, want.action_reason) == 0);
    CHECK(strcmp(out.event_reason, want.event_reason) == 0);
    rig_destroy(&plain);
    rig_destroy(&told);
}

/* Full queues do not delay the close or lose either output: what was
 * queued gives way to the close action and the close event. */
static void control_end_full_queues(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "control-end.full-queues.d%u",
                   (unsigned)version);
    label = name;
    rig_t r;
    rig_create(&r, version, 1, 2);
    rig_establish(&r);          /* SETUP_COMPLETE fills the one event slot */

    /* Fill the action queue with real requests. */
    moq_bytes_t part = MOQ_BYTES_LITERAL("full");
    bool blocked = false;
    for (int i = 0; i < 8 && !blocked; i++) {
        char track[8];
        (void)snprintf(track, sizeof(track), "t%d", i);
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
        cfg.track_name = (moq_bytes_t){(const uint8_t *)track, strlen(track)};
        cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(r.s, &cfg, ++r.now, &sub);
        if (rc == MOQ_ERR_WOULD_BLOCK) blocked = true;
        else REQUIRE(rc == MOQ_OK);
    }
    REQUIRE(blocked);

    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    CHECK(moq_session_state(r.s) == MOQ_SESS_CLOSED);
    close_outputs_t out;
    collect_close(&r, &out);
    CHECK(out.actions == 1 && out.close_actions == 1);
    CHECK(out.action_code == PROTOCOL_VIOLATION);
    CHECK(out.events == 1 && out.close_events == 1);
    CHECK(out.event_code == PROTOCOL_VIOLATION);
    rig_destroy(&r);
}

/* Once the transport itself is gone there is nothing to close and nothing
 * to violate: the transport's close stays the session's close, and no
 * close action appears. */
static void control_end_after_transport_close(moq_version_t version)
{
    char name[56];
    (void)snprintf(name, sizeof(name), "control-end.after-transport-close.d%u",
                   (unsigned)version);
    label = name;
    rig_t r;
    rig_create(&r, version, 16, 16);
    rig_establish(&r);
    (void)drain_actions(&r);
    (void)drain_events(&r);

    REQUIRE(moq_session_on_transport_close(r.s, 0x7, ++r.now) == MOQ_OK);
    CHECK(moq_session_on_control_stream_ended(r.s, ++r.now) == MOQ_OK);
    close_outputs_t out;
    collect_close(&r, &out);
    CHECK(out.actions == 0);
    CHECK(out.events == 1 && out.close_events == 1 && out.event_code == 0x7);
    rig_destroy(&r);
}

/* -- The idle timeout keeps its precedence --------------------------- */

#define IDLE_US UINT64_C(1000)
#define IDLE_CLOSE ((uint64_t)MOQ_CLOSE_IDLE_TIMEOUT)

/* An established session with an idle timeout and no other deadline; its
 * outputs drained. Returns the idle deadline D. */
static uint64_t rig_idle(rig_t *r, moq_version_t version)
{
    rig_create_idle(r, version, 16, 16, IDLE_US);
    rig_establish(r);
    (void)drain_actions(r);
    (void)drain_events(r);
    uint64_t deadline = moq_session_next_deadline_us(r->s);
    /* The setup input was the last activity, at r->now. */
    REQUIRE(deadline == r->now + IDLE_US);
    return deadline;
}

static void expect_close(rig_t *r, uint64_t code, const char *reason)
{
    close_outputs_t out;
    collect_close(r, &out);
    CHECK(out.actions == 1 && out.close_actions == 1);
    CHECK(out.action_code == code);
    CHECK(strcmp(out.action_reason, reason) == 0);
    CHECK(out.events == 1 && out.close_events == 1);
    CHECK(out.event_code == code);
    CHECK(strcmp(out.event_reason, reason) == 0);
}

/* The report arrives at D-1, D or D+1 with no tick delivered before it. At
 * the deadline and after it the idle timeout is the cause, as for any peer
 * input; before it the control stream's end is. */
static void control_end_idle(moq_version_t version, int offset)
{
    char name[56];
    (void)snprintf(name, sizeof(name), "control-end.idle%+d.d%u", offset,
                   (unsigned)version);
    label = name;
    rig_t r;
    uint64_t deadline = rig_idle(&r, version);
    uint64_t at = offset < 0 ? deadline - 1 : deadline + (uint64_t)offset;
    CHECK(moq_session_on_control_stream_ended(r.s, at) == MOQ_OK);
    CHECK(moq_session_state(r.s) == MOQ_SESS_CLOSED);
    if (offset < 0) expect_close(&r, PROTOCOL_VIOLATION, "control stream closed");
    else expect_close(&r, IDLE_CLOSE, "idle timeout");
    rig_destroy(&r);
}

/* A close already chosen stays the session's close, whichever came first:
 * the idle timeout delivered by a tick, a local close, or the control
 * stream's end before the deadline followed by a late tick. */
static void control_end_idle_first_cause(moq_version_t version)
{
    char name[56];
    rig_t r;
    uint64_t deadline;

    (void)snprintf(name, sizeof(name), "control-end.idle-tick-first.d%u",
                   (unsigned)version);
    label = name;
    deadline = rig_idle(&r, version);
    (void)moq_session_tick(r.s, deadline);
    REQUIRE(moq_session_state(r.s) == MOQ_SESS_CLOSED);
    CHECK(moq_session_on_control_stream_ended(r.s, deadline + 1) == MOQ_OK);
    expect_close(&r, IDLE_CLOSE, "idle timeout");
    rig_destroy(&r);

    (void)snprintf(name, sizeof(name), "control-end.local-close-first.d%u",
                   (unsigned)version);
    label = name;
    deadline = rig_idle(&r, version);
    REQUIRE(moq_session_close(r.s, 0x2, "first", deadline - 1) == MOQ_OK);
    CHECK(moq_session_on_control_stream_ended(r.s, deadline + 1) == MOQ_OK);
    expect_close(&r, 0x2, "first");
    rig_destroy(&r);

    (void)snprintf(name, sizeof(name), "control-end.before-deadline-first.d%u",
                   (unsigned)version);
    label = name;
    deadline = rig_idle(&r, version);
    CHECK(moq_session_on_control_stream_ended(r.s, deadline - 1) == MOQ_OK);
    (void)moq_session_tick(r.s, deadline + 1);
    expect_close(&r, PROTOCOL_VIOLATION, "control stream closed");
    rig_destroy(&r);
}

int main(void)
{
    static const moq_version_t all[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    route_bidi_control();
    route_modern(MOQ_VERSION_DRAFT_18);
    route_modern(MOQ_VERSION_DRAFT_22);
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
        control_end_live(all[i]);
        control_end_before_setup(all[i]);
        control_end_after_local_close(all[i]);
        control_end_full_queues(all[i]);
        control_end_after_transport_close(all[i]);
        control_end_idle(all[i], -1);
        control_end_idle(all[i], 0);
        control_end_idle(all[i], 1);
        control_end_idle_first_cause(all[i]);
    }
    control_end_after_protocol_error(MOQ_VERSION_DRAFT_18);
    control_end_after_protocol_error(MOQ_VERSION_DRAFT_22);

    if (failures) {
        fprintf(stderr, "%u of %u checks failed\n", failures, checks);
        return 1;
    }
    printf("PASS: host_route_control (%u checks)\n", checks);
    return 0;
}
