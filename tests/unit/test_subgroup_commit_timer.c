/* Native fake-time contract: FIN starts D18/D22 subgroup delivery timing.
 * SimPair establishes the real owners, then only public session inputs and
 * polled action refs are used. Handing FIN to the transport is not a commit. */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_alloc_support.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static unsigned checks, failures, prerequisites, cases, failed_cases;
static const char *case_name;
static const uint64_t MS = 1000;

static bool require(bool ok, const char *expression, int line)
{
    ++checks;
    if (!ok) {
        ++failures;
        ++prerequisites;
        fprintf(stderr, "PREREQUISITE %s:%d: %s\n", case_name, line, expression);
    }
    return ok;
}
#define NEED(x) do { if (!require(!!(x), #x, __LINE__)) goto done; } while (0)
#define READY(x) do { if (!require(!!(x), #x, __LINE__)) return false; } while (0)

static void expect(const char *name, uint64_t actual, uint64_t wanted)
{
    ++checks;
    if (actual != wanted) {
        ++failures;
        fprintf(stderr, "FAIL %s/%s: got=%" PRIu64 " expected=%" PRIu64 "\n",
                case_name, name, actual, wanted);
    }
}

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *receiver;
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_rcbuf_t *payload;
    bool publication;
    uint64_t base;
} fixture_t;

static size_t drain_events(moq_session_t *s)
{
    moq_event_t e;
    size_t count = 0;
    while (moq_session_poll_events(s, &e, 1)) {
        ++count;
        moq_event_cleanup(&e);
    }
    return count;
}

static void expect_terminal(fixture_t *f, moq_subgroup_handle_t subgroup,
                             moq_stream_ref_t ref, uint64_t group, bool expired)
{
    moq_event_t e;
    unsigned count = 0;
    while (moq_session_poll_events(f->sender, &e, 1)) {
        ++count;
        expect("sender-event-is-outgoing-terminal", e.kind,
               MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
        expect("terminal-detail-prefix", e.detail_size,
               MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE);
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
            e.detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            const moq_outgoing_subgroup_terminated_event_t *d =
                &e.u.outgoing_subgroup_terminated;
            expect("terminal-exact-subgroup", d->subgroup._opaque, subgroup._opaque);
            expect("terminal-exact-subscription", d->sub._opaque, f->sub._opaque);
            expect("terminal-exact-publication", d->pub._opaque, f->pub._opaque);
            expect("terminal-exact-stream", d->stream_ref._v, ref._v);
            expect("terminal-exact-group", d->group_id, group);
            expect("terminal-exact-subgroup-id", d->subgroup_id, 0);
            expect("terminal-timeout-code", d->error_code, MOQ_RESET_DELIVERY_TIMEOUT);
            expect("terminal-timeout-cause", d->cause,
                   MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
        }
        moq_event_cleanup(&e);
    }
    expect("exact-terminal-event-count", count, expired ? 1 : 0);
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    READY(moq_session_poll_events(s, out, 1) == 1);
    if (out->kind != kind) {
        moq_event_cleanup(out);
        READY(out->kind == kind);
    }
    return true;
}

static bool pump(fixture_t *f)
{
    READY(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    return true;
}

static bool setup(fixture_t *f, moq_version_t version, bool publication)
{
    memset(f, 0, sizeof(*f));
    f->publication = publication;
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = version;
    cfg.initial_now_us = 1000000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    READY(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    READY(moq_simpair_start(f->pair) == MOQ_OK);
    if (!pump(f)) return false;
    moq_session_t *c = moq_simpair_client(f->pair), *s = moq_simpair_server(f->pair);
    f->sender = publication ? c : s;
    f->receiver = publication ? s : c;
    READY(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    READY(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    READY(drain_events(c) == 1 && drain_events(s) == 1);

    /* D16 section11.1 defines Track Extension2, DELIVERY_TIMEOUT.
     * D18/D22 define Track Property6, SUBGROUP_DELIVERY_TIMEOUT.
     * Each literal encodes its own 100ms value; no D18 encoder is used for D16. */
    static const uint8_t d16_timeout[] = {0x02, 0x40, 0x64};
    static const uint8_t subgroup_timeout[] = {0x06, 0x64};
    moq_bytes_t properties = version == MOQ_VERSION_DRAFT_16
        ? (moq_bytes_t){d16_timeout, sizeof(d16_timeout)}
        : (moq_bytes_t){subgroup_timeout, sizeof(subgroup_timeout)};
    moq_bytes_t part = MOQ_BYTES_LITERAL("timer");
    moq_event_t e;
    uint64_t now = moq_simpair_now_us(f->pair);
    if (publication) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){&part, 1};
        pc.track_name = MOQ_BYTES_LITERAL("v");
        pc.track_properties = properties;
        READY(moq_session_publish(c, &pc, now, &f->pub) == MOQ_OK);
        if (!pump(f) || !take(s, MOQ_EVENT_PUBLISH_REQUEST, &e)) return false;
        moq_publication_t peer = e.u.publish_request.pub;
        bool same = e.u.publish_request.track_properties.len == properties.len &&
            memcmp(e.u.publish_request.track_properties.data, properties.data, properties.len) == 0;
        moq_event_cleanup(&e);
        READY(same);
        moq_accept_publish_cfg_t accept;
        moq_accept_publish_cfg_init_sized(&accept, sizeof(accept));
        READY(moq_session_accept_publish(s, peer, &accept, now) == MOQ_OK);
        if (!pump(f) || !take(c, MOQ_EVENT_PUBLISH_OK, &e)) return false;
        same = e.u.publish_ok.pub._opaque == f->pub._opaque;
        moq_event_cleanup(&e);
        READY(same);
    } else {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&part, 1};
        sc.track_name = MOQ_BYTES_LITERAL("v");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t local;
        READY(moq_session_subscribe(c, &sc, now, &local) == MOQ_OK);
        if (!pump(f) || !take(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &e)) return false;
        f->sub = e.u.subscribe_request.sub;
        moq_event_cleanup(&e);
        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        accept.track_properties = properties;
        READY(moq_session_accept_subscribe(s, f->sub, &accept, now) == MOQ_OK);
        if (!pump(f) || !take(c, MOQ_EVENT_SUBSCRIBE_OK, &e)) return false;
        bool same = e.u.subscribe_ok.sub._opaque == local._opaque &&
            e.u.subscribe_ok.track_properties.len == properties.len &&
            memcmp(e.u.subscribe_ok.track_properties.data, properties.data, properties.len) == 0;
        moq_event_cleanup(&e);
        READY(same);
    }
    READY(drain_events(c) == 0 && drain_events(s) == 0);
    READY(moq_session_next_deadline_us(f->sender) == UINT64_MAX);
    READY(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
    f->base = moq_simpair_now_us(f->pair);
    return true;
}

typedef struct actions {
    unsigned payloads, fins, resets, other;
} actions_t;

static actions_t poll_actions(fixture_t *f, moq_stream_ref_t ref)
{
    actions_t t = {0};
    moq_action_t a;
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.stream_ref._v == ref._v) {
            if (a.u.send_data.payload) {
                ++t.payloads;
                expect("payload-ref", a.u.send_data.payload == f->payload, true);
            }
            if (a.u.send_data.fin) ++t.fins;
            if (!a.u.send_data.payload && !a.u.send_data.fin) ++t.other;
        } else if (a.kind == MOQ_ACTION_RESET_DATA &&
                   a.u.reset_data.stream_ref._v == ref._v &&
                   a.u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT) {
            ++t.resets;
        } else {
            ++t.other;
        }
        moq_action_cleanup(&a);
    }
    expect("caller-buffer-retained", moq_rcbuf_refcount(f->payload), 1);
    expect("no-unrelated-actions", t.other, 0);
    return t;
}

static void timer_actions(fixture_t *f, moq_stream_ref_t ref, const char *name,
                          unsigned resets)
{
    actions_t t = poll_actions(f, ref);
    expect(name, t.resets, resets);
    expect("timer-no-extra-payload", t.payloads, 0);
    expect("timer-no-extra-fin", t.fins, 0);
}

static bool open_group(fixture_t *f, uint64_t group, uint64_t ms,
                       moq_subgroup_handle_t *sg, moq_stream_ref_t *ref)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    moq_result_t rc = f->publication
        ? moq_session_open_pub_subgroup(f->sender, f->pub, &cfg, f->base + ms * MS, sg)
        : moq_session_open_subgroup(f->sender, f->sub, &cfg, f->base + ms * MS, sg);
    READY(rc == MOQ_OK);
    moq_action_t a;
    READY(moq_session_poll_actions(f->sender, &a, 1) == 1);
    bool header = a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.header_len > 0 &&
        a.u.send_data.payload == NULL && !a.u.send_data.fin;
    if (header) *ref = a.u.send_data.stream_ref;
    moq_action_cleanup(&a);
    READY(header && ref->_v != 0);
    size_t extra = moq_session_poll_actions(f->sender, &a, 1);
    if (extra) moq_action_cleanup(&a);
    READY(extra == 0);
    return true;
}

static bool write_object(fixture_t *f, moq_subgroup_handle_t sg, moq_stream_ref_t ref,
                         uint64_t ms)
{
    READY(moq_session_write_object(f->sender, sg, 0, f->payload, f->base + ms * MS) == MOQ_OK);
    expect("queued-payload-ref", moq_rcbuf_refcount(f->payload), 2);
    actions_t t = poll_actions(f, ref);
    expect("object-handed-over", t.payloads, 1);
    expect("object-no-fin", t.fins, 0);
    expect("object-no-reset", t.resets, 0);
    return true;
}

static bool finish_group(fixture_t *f, moq_subgroup_handle_t sg, moq_stream_ref_t ref,
                         uint64_t ms)
{
    READY(moq_session_close_subgroup(f->sender, sg, f->base + ms * MS) == MOQ_OK);
    actions_t t = poll_actions(f, ref);
    expect("exact-fin-handed-over", t.fins, 1);
    expect("fin-no-payload", t.payloads, 0);
    expect("fin-no-reset", t.resets, 0);
    return true;
}

static void cleanup(fixture_t *f, unsigned before)
{
    if (f->sender && f->payload) {
        moq_action_t a;
        while (moq_session_poll_actions(f->sender, &a, 1)) moq_action_cleanup(&a);
        expect("final-buffer-ref", moq_rcbuf_refcount(f->payload), 1);
    }
    if (f->payload) moq_rcbuf_decref(f->payload);
    if (f->pair) moq_simpair_destroy(f->pair);
    expect("allocation-balance", (uint64_t)f->memory.balance, 0);
    ++cases;
    if (failures != before) ++failed_cases;
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", case_name);
}

static void timer_row(moq_version_t version, bool publication, unsigned mode)
{
    char name[80];
    const char *modes[] = {"publishing", "fin-expiry", "commit-correlation"};
    snprintf(name, sizeof(name), "d%u/%s/%s", (unsigned)version,
             publication ? "publish" : "subscribe", modes[mode]);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    if (!setup(&f, version, publication)) goto done;
    moq_subgroup_handle_t a;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0);
    if (!open_group(&f, 7, 10, &a, &ar) || !write_object(&f, a, ar, 20)) goto done;
    moq_subgroup_handle_t expired_subgroup = a;
    moq_stream_ref_t expired_ref = ar;
    uint64_t expired_group = 7;
    if (mode == 0) {
        expect("no-deadline-before-fin", moq_session_next_deadline_us(f.sender), UINT64_MAX);
        NEED(moq_session_on_data_committed(f.sender, ar, f.base + 30 * MS) == MOQ_OK);
        NEED(moq_session_tick(f.sender, f.base + 160 * MS) == MOQ_OK);
        timer_actions(&f, ar, "no-reset-while-publishing", 0);
        expect("still-no-deadline", moq_session_next_deadline_us(f.sender), UINT64_MAX);
        moq_result_t rc = moq_session_write_object(f.sender, a, 1, f.payload, f.base + 161 * MS);
        expect("publishing-can-continue", rc == MOQ_OK, true);
        actions_t t = poll_actions(&f, ar);
        expect("continued-object", t.payloads, 1);
        expect("continued-no-reset", t.resets, 0);
    } else {
        if (!finish_group(&f, a, ar, 50)) goto done;
        expect("fin-starts-100ms", moq_session_next_deadline_us(f.sender), f.base + 150 * MS);
        moq_stream_ref_t pending = ar;
        uint64_t deadline = f.base + 150 * MS;
        if (mode == 2) {
            moq_subgroup_handle_t b;
            moq_stream_ref_t br = moq_stream_ref_from_u64(0);
            if (!open_group(&f, 8, 60, &b, &br) || !write_object(&f, b, br, 60) ||
                !finish_group(&f, b, br, 70)) goto done;
            NEED(br._v != ar._v && ar._v != 0x9999 && br._v != 0x9999);
            NEED(moq_session_on_data_committed(f.sender, moq_stream_ref_from_u64(0x9999),
                                              f.base + 90 * MS) == MOQ_OK);
            expect("unrelated-commit-keeps-timer", moq_session_next_deadline_us(f.sender),
                   f.base + 150 * MS);
            NEED(moq_session_on_data_committed(f.sender, ar, f.base + 100 * MS) == MOQ_OK);
            expect("commit-clears-only-A", moq_session_next_deadline_us(f.sender), f.base + 170 * MS);
            NEED(moq_session_on_data_committed(f.sender, ar, f.base + 105 * MS) == MOQ_OK);
            expect("duplicate-commit-keeps-B", moq_session_next_deadline_us(f.sender), f.base + 170 * MS);
            NEED(moq_session_tick(f.sender, f.base + 150 * MS) == MOQ_OK);
            timer_actions(&f, ar, "committed-A-does-not-reset", 0);
            pending = br;
            expired_subgroup = b;
            expired_ref = br;
            expired_group = 8;
            deadline = f.base + 170 * MS;
        }
        NEED(moq_session_tick(f.sender, deadline - 1) == MOQ_OK);
        timer_actions(&f, pending, "no-early-reset", 0);
        expect("fin-handover-keeps-timer", moq_session_next_deadline_us(f.sender), deadline);
        NEED(moq_session_tick(f.sender, deadline) == MOQ_OK);
        timer_actions(&f, pending, "exact-correlated-timeout-reset", 1);
        expect("expired-timer-cleared", moq_session_next_deadline_us(f.sender), UINT64_MAX);
        NEED(moq_session_tick(f.sender, deadline + MS) == MOQ_OK);
        NEED(moq_session_on_data_committed(f.sender, pending, deadline + 2 * MS) == MOQ_OK);
        timer_actions(&f, pending, "retired-stream-no-duplicate-reset", 0);
    }
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
    expect("receiver-live", moq_session_state(f.receiver), MOQ_SESS_ESTABLISHED);
    expect_terminal(&f, expired_subgroup, expired_ref, expired_group, mode != 0);
    expect("no-receiver-events", drain_events(f.receiver), 0);
    if (mode == 2) {
        NEED(moq_session_close(f.sender, 0, NULL, f.base + 210 * MS) == MOQ_OK);
        expect("closed-session-commit-refused",
               moq_session_on_data_committed(f.sender, ar, f.base + 211 * MS) == MOQ_ERR_CLOSED, true);
    }
done:
    cleanup(&f, before);
}

/* Compatibility control only: D16's declared open-start/close-clear behavior,
 * with its own DELIVERY_TIMEOUT extension, not a D18 conformance assertion. */
static void d16_control(void)
{
    case_name = "d16/subscribe/legacy-control";
    unsigned before = failures;
    fixture_t f;
    if (!setup(&f, MOQ_VERSION_DRAFT_16, false)) goto done;
    moq_subgroup_handle_t sg;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_group(&f, 7, 10, &sg, &ref) || !write_object(&f, sg, ref, 20)) goto done;
    expect("legacy-open-deadline", moq_session_next_deadline_us(f.sender), f.base + 110 * MS);
    NEED(moq_session_on_data_committed(f.sender, ref, f.base + 30 * MS) == MOQ_OK);
    expect("legacy-open-commit-noop", moq_session_next_deadline_us(f.sender), f.base + 110 * MS);
    if (!finish_group(&f, sg, ref, 50)) goto done;
    expect("legacy-close-clears", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    NEED(moq_session_on_data_committed(f.sender, ref, f.base + 60 * MS) == MOQ_OK);
    NEED(moq_session_tick(f.sender, f.base + 200 * MS) == MOQ_OK);
    timer_actions(&f, ref, "legacy-no-post-fin-reset", 0);
    expect("legacy-session-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v)
        for (unsigned origin = 0; origin < 2; ++origin)
            for (unsigned mode = 0; mode < 3; ++mode)
                timer_row(versions[v], origin != 0, mode);
    d16_control();
    printf("%u cases, %u checks, %u failed cases, %u behavioral failures, %u prerequisite failures\n",
           cases, checks, failed_cases, failures - prerequisites, prerequisites);
    return failures ? 1 : 0;
}
