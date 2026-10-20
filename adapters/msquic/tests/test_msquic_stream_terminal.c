/* A request bidi the subscriber cancels is retired by the stream's transport
 * terminal. Real client and server sessions and bridges over the
 * deterministic fake MsQuic table, draft-18 and draft-22, one thread, no
 * socket.
 *
 * Cancelling the subscription aborts the request bidi in both directions (a
 * reset of our send half, STOP_SENDING on our receive half). The bridge keeps
 * the mapping, marked aborting, until the stream's end. Once our receive side
 * is aborted MsQuic reports neither the peer's FIN nor its reset; the only
 * remaining signal is SHUTDOWN_COMPLETE, which the adapter must report as the
 * whole-stream terminal. Pinned:
 *   - SHUTDOWN_COMPLETE outside connection teardown retires the aborting
 *     mapping and the adapter's record;
 *   - a SHUTDOWN_COMPLETE carrying ConnectionShutdown is the connection's
 *     terminal, never an isolated stream completion: the mapping is left to
 *     the connection's own end;
 *   - a repeated or unknown terminal report changes nothing;
 *   - SHUTDOWN_COMPLETE on an active (not aborting) request bidi leaves its
 *     mapping and the subscription as they were;
 *   - a completion delivered while the adapter is servicing (from the
 *     application hook) or inline from inside an endpoint op (the abort's own
 *     StreamShutdown) does not reach the bridge there: the record is kept,
 *     handle gone, and the report follows from the service loop;
 *   - a native refusal of the abort fails the connection, and the stream's
 *     later completion fabricates nothing. */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "msquic_internal.h"
#include "support/fake_msq_table.h"
#include "../../../core/src/bridge/transport_bridge_internal.h"
#include "../../../tests/unit/test_alloc_support.h"

static unsigned failures, prerequisites, cases;
static const char *case_name;
static uint64_t clock_us;
static uint64_t fake_now(void) { return clock_us; }

#define NEED(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE %s:%d: %s\n", case_name, __LINE__, #x); \
    ++prerequisites; return false; } } while (0)
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", case_name, __LINE__, #x); \
    ++failures; } } while (0)

struct side {
    fake_msq_t fake;
    moq_session_t *session;
    moq_msquic_conn_t *conn;
    int relayed;
};

struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    struct side sides[2];          /* [0] client (subscriber), [1] server */
    moq_connection_target_t target;
    moq_subscription_t csub, ssub;
    fake_msq_stream_t *bidi;       /* the client's request bidi */
};

static void conn_event(struct side *s, QUIC_CONNECTION_EVENT *ev)
{
    (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&s->fake), s->conn, ev);
}

static void shutdown_complete(fake_msq_stream_t *st, bool connection)
{
    QUIC_STREAM_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE;
    ev.SHUTDOWN_COMPLETE.ConnectionShutdown = connection ? TRUE : FALSE;
    (void)st->cb((HQUIC)st, st->ctx, &ev);
}

static bridge_stream_entry_t *mapping(struct side *s, uint64_t id)
{
    moq_transport_bridge_t *b = moq_msquic_test_bridge(s->conn);
    for (uint32_t i = 0; i < b->max_streams; ++i)
        if (b->streams[i].active && b->streams[i].transport_id == id)
            return &b->streams[i];
    return NULL;
}

static bool take(struct side *s, moq_event_kind_t kind, moq_event_t *ev)
{
    NEED(moq_session_poll_events(s->session, ev, 1) == 1);
    if (ev->kind != kind) {
        moq_event_cleanup(ev);
        NEED(ev->kind == kind);
    }
    return true;
}

static fake_msq_stream_t *peer_stream(struct side *s, uint64_t id, bool uni)
{
    for (int i = 0; i < s->fake.stream_count; ++i)
        if (s->fake.streams[i].id == id)
            return &s->fake.streams[i];
    fake_msq_stream_t *st = fake_msq_peer_stream(&s->fake, id, uni);
    if (st == NULL) return NULL;
    QUIC_CONNECTION_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED;
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags = uni ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL
                                      : QUIC_STREAM_OPEN_FLAG_NONE;
    conn_event(s, &ev);
    return st;
}

static bool relay(struct side *from, struct side *to)
{
    while (from->relayed < from->fake.send_count) {
        fake_msq_send_t *send = &from->fake.sends[from->relayed++];
        NEED(send->bytes_len == send->total);
        fake_msq_stream_t *st = peer_stream(to, send->stream->id, send->stream->uni);
        NEED(st != NULL);
        fake_msq_deliver_receive(st, send->bytes, send->bytes_len,
                                (send->flags & QUIC_SEND_FLAG_FIN) != 0);
        NEED(fake_msq_deliver_send_complete(&from->fake, false));
    }
    return true;
}

static bool pump(struct fixture *f)
{
    for (unsigned i = 0; i < 16; ++i) {
        moq_msquic_conn_service(f->sides[0].conn);
        moq_msquic_conn_service(f->sides[1].conn);
        if (!relay(&f->sides[0], &f->sides[1]) ||
            !relay(&f->sides[1], &f->sides[0])) return false;
        if (f->sides[0].relayed == f->sides[0].fake.send_count &&
            f->sides[1].relayed == f->sides[1].fake.send_count) return true;
    }
    NEED(false);
}

/* Both sessions established, the client subscribed and accepted; f->bidi is
 * the client's request bidi, mapped and active. */
static bool setup(struct fixture *f, moq_version_t version)
{
    f->alloc = test_allocator(&f->memory);
    f->target.struct_size = sizeof(f->target);
    f->target.target_present = MOQ_BINDING_TARGET_COMPLETE;
    f->target.authority = MOQ_BYTES_LITERAL("test.invalid");
    clock_us = 1000000;
    for (unsigned i = 0; i < 2; ++i) {
        struct side *s = &f->sides[i];
        fake_msq_init(&s->fake, i == 0);
        moq_session_cfg_t sc;
        moq_session_cfg_init_sized(&sc, sizeof(sc), &f->alloc,
            i == 0 ? MOQ_PERSPECTIVE_CLIENT : MOQ_PERSPECTIVE_SERVER);
        sc.version = version;
        sc.send_request_capacity = true;
        sc.initial_request_capacity = 16;
        NEED(moq_session_create(&sc, clock_us, &s->session) == MOQ_OK);
        moq_msquic_conn_cfg_t ac;
        moq_msquic_conn_cfg_init_sized(&ac, sizeof(ac));
        ac.alloc = &f->alloc;
        ac.session = s->session;
        ac.api = fake_msq_table(&s->fake);
        ac.target = &f->target;
        NEED(moq_msquic_conn_create(&ac, &s->conn) == MOQ_OK);
        NEED(moq_msquic_conn_bind(s->conn, fake_msq_conn_handle(&s->fake)) == MOQ_OK);
    }
    for (unsigned i = 0; i < 2; ++i) {
        QUIC_CONNECTION_EVENT ev;
        memset(&ev, 0, sizeof(ev));
        ev.Type = QUIC_CONNECTION_EVENT_CONNECTED;
        conn_event(&f->sides[i], &ev);
    }
    if (!pump(f)) return false;
    for (unsigned i = 0; i < 2; ++i) {
        moq_event_t ev;
        NEED(moq_session_state(f->sides[i].session) == MOQ_SESS_ESTABLISHED);
        if (!take(&f->sides[i], MOQ_EVENT_SETUP_COMPLETE, &ev)) return false;
        moq_event_cleanup(&ev);
    }
    struct side *c = &f->sides[0], *sv = &f->sides[1];
    int before = c->fake.stream_count;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t part = MOQ_BYTES_LITERAL("terminal");
    sc.track_namespace = (moq_namespace_t){&part, 1};
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    NEED(moq_session_subscribe(c->session, &sc, clock_us, &f->csub) == MOQ_OK);
    moq_msquic_conn_service(c->conn);
    NEED(c->fake.stream_count == before + 1);
    f->bidi = &c->fake.streams[before];
    NEED(!f->bidi->uni);
    fake_msq_deliver_start_complete(f->bidi, QUIC_STATUS_SUCCESS);
    if (!pump(f)) return false;
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    f->ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    NEED(moq_session_accept_subscribe(sv->session, f->ssub, &accept, clock_us) == MOQ_OK);
    if (!pump(f) || !take(c, MOQ_EVENT_SUBSCRIBE_OK, &ev)) return false;
    moq_event_cleanup(&ev);
    bridge_stream_entry_t *m = mapping(c, f->bidi->id);
    NEED(m != NULL && m->kind == BRIDGE_STREAM_BIDI && !m->aborting);
    NEED(moq_msquic_test_stream_find(c->conn, f->bidi->id) != NULL);
    return true;
}

/* Cancel the subscription: the request bidi's mapping becomes aborting. */
static bool cancel(struct fixture *f)
{
    struct side *c = &f->sides[0];
    clock_us += 1000;
    NEED(moq_session_unsubscribe(c->session, f->csub, clock_us) == MOQ_OK);
    moq_msquic_conn_service(c->conn);
    return true;
}

static void finish(struct fixture *f, unsigned before, unsigned pre)
{
    for (unsigned i = 0; i < 2; ++i) {
        struct side *s = &f->sides[i];
        if (s->conn) {
            s->conn->hook = NULL;
            s->fake.on_stream_shutdown = NULL;
            QUIC_CONNECTION_EVENT ev;
            memset(&ev, 0, sizeof(ev));
            ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER;
            conn_event(s, &ev);
            while (fake_msq_deliver_send_complete(&s->fake, true)) {}
            for (int j = 0; j < s->fake.stream_count; ++j) {
                fake_msq_stream_t *st = &s->fake.streams[j];
                if (!st->closed)
                    shutdown_complete(st, true);
            }
            ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE;
            conn_event(s, &ev);
            moq_msquic_conn_destroy(s->conn);
        }
        if (s->session) moq_session_destroy(s->session);
    }
    CHECK(f->memory.balance == 0);
    printf("%s %s\n", failures == before && prerequisites == pre ? "PASS" : "FAIL", case_name);
    ++cases;
    free(f);
}

enum mode { RETIRE, CONN_SHUTDOWN, ACTIVE, HOOK, INLINE, REFUSE, MODE_COUNT };
static const char *const names[] = {"retire", "connection-shutdown", "active",
                                    "hook-reentrant", "inline-op", "refuse"};

struct deferred {
    struct fixture *f;
    unsigned calls;
    bool mapped_inside;      /* the aborting mapping survived the delivery */
    bool record_inside;      /* the record survived, its handle gone */
};

static void deliver_and_observe(struct deferred *d)
{
    struct side *c = &d->f->sides[0];
    uint64_t id = d->f->bidi->id;
    shutdown_complete(d->f->bidi, false);
    bridge_stream_entry_t *m = mapping(c, id);
    d->mapped_inside = m != NULL && m->aborting;
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(c->conn, id);
    d->record_inside = ms != NULL && ms->stream == NULL;
}

static void hook_deliver(moq_msquic_conn_t *conn, void *ctx)
{
    (void)conn;
    struct deferred *d = ctx;
    if (d->calls++) return;
    deliver_and_observe(d);
}

static void inline_deliver(fake_msq_stream_t *st, QUIC_STREAM_SHUTDOWN_FLAGS flags,
                           void *ctx)
{
    struct deferred *d = ctx;
    if (st != d->f->bidi || !(flags & QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE) ||
        d->calls++)
        return;
    deliver_and_observe(d);
}

static void run(moq_version_t version, enum mode mode)
{
    char label[96];
    snprintf(label, sizeof(label), "d%u/%s", (unsigned)version, names[mode]);
    case_name = label;
    unsigned before = failures, pre = prerequisites;
    struct fixture *f = calloc(1, sizeof(*f));
    if (!f) { ++prerequisites; return; }
    if (!setup(f, version)) goto done;
    struct side *c = &f->sides[0];
    moq_transport_bridge_t *b = moq_msquic_test_bridge(c->conn);
    uint64_t id = f->bidi->id;
    struct deferred d = {f, 0, false, false};

    if (mode == ACTIVE) {
        /* The transport retires the request bidi while it is still active:
         * the report is a no-op there; the subscription stands. */
        size_t mapped = moq_transport_bridge_stream_count(b);
        shutdown_complete(f->bidi, false);
        CHECK(f->bidi->closed);
        CHECK(moq_msquic_test_stream_find(c->conn, id) == NULL);
        bridge_stream_entry_t *m = mapping(c, id);
        CHECK(m != NULL && !m->aborting);
        CHECK(moq_transport_bridge_stream_count(b) == mapped);
        CHECK(moq_session_state(c->session) == MOQ_SESS_ESTABLISHED);
        CHECK(!moq_transport_bridge_is_fatal(b));
        goto done;
    }
    if (mode == REFUSE) {
        c->fake.stream_shutdown_status = QUIC_STATUS_INVALID_STATE;
        if (!cancel(f)) goto done;
        c->fake.stream_shutdown_status = QUIC_STATUS_SUCCESS;
        CHECK(moq_transport_bridge_is_terminal(b));
        shutdown_complete(f->bidi, false);
        CHECK(f->bidi->closed);
        CHECK(moq_msquic_test_stream_find(c->conn, id) == NULL);
        CHECK(c->conn->terminals_owed == 0);
        goto done;
    }
    if (mode == INLINE) {
        c->fake.on_stream_shutdown = inline_deliver;
        c->fake.on_stream_shutdown_ctx = &d;
    }
    if (!cancel(f)) goto done;
    c->fake.on_stream_shutdown = NULL;
    if (mode == INLINE) {
        CHECK(d.calls == 1);
        CHECK(d.mapped_inside);   /* not reported inside the abort's own op */
        CHECK(d.record_inside);   /* identity kept, handle gone */
        CHECK(mapping(c, id) == NULL);   /* reported by the service loop */
        CHECK(moq_msquic_test_stream_find(c->conn, id) == NULL);
        CHECK(f->bidi->closed);
        goto check_after;
    }
    bridge_stream_entry_t *m = mapping(c, id);
    CHECK(m != NULL && m->aborting);
    CHECK(f->bidi->shutdown_calls >= 1);
    size_t mapped = moq_transport_bridge_stream_count(b);

    if (mode == CONN_SHUTDOWN) {
        shutdown_complete(f->bidi, true);
        CHECK(f->bidi->closed);
        CHECK(moq_msquic_test_stream_find(c->conn, id) == NULL);
        m = mapping(c, id);
        CHECK(m != NULL && m->aborting);   /* no isolated completion */
        CHECK(moq_transport_bridge_stream_count(b) == mapped);
        goto done;
    }
    if (mode == HOOK) {
        c->conn->hook = hook_deliver;
        c->conn->hook_user = &d;
        moq_msquic_conn_service(c->conn);
        c->conn->hook = NULL;
        CHECK(d.calls >= 1);
        CHECK(d.mapped_inside);
        CHECK(d.record_inside);
    } else {
        shutdown_complete(f->bidi, false);
    }
    CHECK(f->bidi->closed);
    CHECK(mapping(c, id) == NULL);
    CHECK(moq_msquic_test_stream_find(c->conn, id) == NULL);
    CHECK(moq_transport_bridge_stream_count(b) == mapped - 1);

check_after:
    CHECK(c->conn->terminals_owed == 0);
    /* A repeated report, and one for a stream never seen, change nothing. */
    {
        size_t now_mapped = moq_transport_bridge_stream_count(b);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(b, id, clock_us) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(b, 0x3fff0, clock_us) == MOQ_OK);
        CHECK(moq_transport_bridge_stream_count(b) == now_mapped);
    }
    CHECK(moq_session_state(c->session) == MOQ_SESS_ESTABLISHED);
    CHECK(!moq_transport_bridge_is_fatal(b));
    /* The session still works: a new subscription opens its own bidi. */
    {
        int n = c->fake.stream_count;
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t part = MOQ_BYTES_LITERAL("terminal");
        sc.track_namespace = (moq_namespace_t){&part, 1};
        sc.track_name = MOQ_BYTES_LITERAL("t2");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t again;
        CHECK(moq_session_subscribe(c->session, &sc, clock_us, &again) == MOQ_OK);
        moq_msquic_conn_service(c->conn);
        CHECK(c->fake.stream_count == n + 1);
        if (c->fake.stream_count == n + 1)
            CHECK(mapping(c, c->fake.streams[n].id) != NULL);
    }
done:
    finish(f, before, pre);
}

int main(void)
{
    moq_msq_test_now_us = fake_now;
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v)
        for (enum mode m = RETIRE; m < MODE_COUNT; ++m)
            run(versions[v], m);
    moq_msq_test_now_us = NULL;
    printf("%u cases, %u behavioral failures, %u prerequisite failures\n",
           cases, failures, prerequisites);
    return failures || prerequisites ? 1 : 0;
}
