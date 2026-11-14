/* Actual managed WT/MsQuic loopback, not an injected transport callback.
 * The lane guard holds FIN submission and the pending witness together.
 * No native ACK can enter that lane until the application pump returns. */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <moq/transport_bridge.h>
#include <moq/vi64.h>
#include <moq/wire.h>
#include <moq/wtquic_msquic_managed.h>
#include "wtq_loopback_port.h"

enum { TIMEOUT_MS = 60000, WAIT_SECS = 15 };
struct run;
struct side {
    struct run *run;
    bool publisher, subscribed, sent;
    unsigned objects, fins;
};
struct run {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_version_t version;
    bool client_publisher, payload_group;
    _Atomic bool stopping, publisher_done, receiver_done;
    _Atomic unsigned failures, commits, bare_fins, data_fins, resets;
    _Atomic size_t allocations;
    moq_alloc_t alloc;
    struct side sides[2];
    /* Values only: borrowed managed handles never outlive a pump window. */
    uint64_t key, deadline;
    moq_stream_ref_t ref;
    bool pending;
    moq_rcbuf_t *payload;
};
static struct run *active;

static void notify(struct run *r)
{
    pthread_mutex_lock(&r->mu);
    pthread_cond_signal(&r->cv);
    pthread_mutex_unlock(&r->mu);
}
static void failure(struct run *r, int line, const char *expr)
{
    fprintf(stderr, "FAIL D%u/%s/%s:%d: %s\n", (unsigned)r->version,
        r->client_publisher ? "client-publisher" : "server-publisher",
        r->payload_group ? "payload-group" : "empty-group", line, expr);
    atomic_fetch_add(&r->failures, 1);
    notify(r);
}
#define CHECK(r, x) do { if (!(x)) failure((r), __LINE__, #x); } while (0)
#define NEED(r, x) do { if (!(x)) { failure((r), __LINE__, #x); return; } } while (0)

static void *tracked_alloc(size_t size, void *ctx)
{
    struct run *r = ctx;
    const moq_alloc_t *a = moq_alloc_default();
    void *p = a->alloc(size, a->ctx);
    if (p) atomic_fetch_add(&r->allocations, 1);
    return p;
}
static void tracked_free(void *p, size_t size, void *ctx)
{
    struct run *r = ctx;
    const moq_alloc_t *a = moq_alloc_default();
    if (p) {
        CHECK(r, atomic_load(&r->allocations) > 0);
        atomic_fetch_sub(&r->allocations, 1);
        a->free(p, size, a->ctx);
    }
}
static void *tracked_realloc(void *p, size_t old_size, size_t new_size, void *ctx)
{
    if (!p) return tracked_alloc(new_size, ctx);
    if (!new_size) { tracked_free(p, old_size, ctx); return NULL; }
    const moq_alloc_t *a = moq_alloc_default();
    return a->realloc(p, old_size, new_size, a->ctx);
}

static moq_result_t observe_commit(moq_transport_bridge_t *, uint64_t, uint64_t);
static wtq_result_t observe_send(wtq_stream_t *, const wtq_span_t *, size_t,
                                 uint32_t, void *);
static wtq_result_t observe_reset(wtq_stream_t *, uint32_t);
static wtq_result_t observe_abort(wtq_stream_t *, uint32_t);
/* As in the private keys/commit fixtures, include the real adapter for mapping
 * observations. These probes call the real provider/bridge unchanged. The
 * managed TU is compiled separately, with its production callback table. */
#define moq_transport_bridge_on_local_stream_committed observe_commit
#define wtq_stream_send observe_send
#define wtq_stream_reset observe_reset
#define wtq_stream_abort observe_abort
#ifndef MOQ_WT_NATIVE_ADAPTER_SOURCE
#define MOQ_WT_NATIVE_ADAPTER_SOURCE "../wtquic_adapter.c"
#endif
#include MOQ_WT_NATIVE_ADAPTER_SOURCE
#undef moq_transport_bridge_on_local_stream_committed
#undef wtq_stream_send
#undef wtq_stream_reset
#undef wtq_stream_abort

static moq_result_t observe_commit(moq_transport_bridge_t *b, uint64_t key, uint64_t now)
{
    struct run *r = active;
    CHECK(r, r->pending);
    CHECK(r, key == r->key);
    CHECK(r, r->ref._v != 0 && moq_transport_bridge_find_ref(b, key)._v == r->ref._v);
    CHECK(r, atomic_load(&r->commits) == 0);
    moq_result_t rc = moq_transport_bridge_on_local_stream_committed(b, key, now);
    CHECK(r, rc == MOQ_OK);
    CHECK(r, moq_transport_bridge_find_ref(b, key)._v == 0);
    atomic_fetch_add(&r->commits, 1);
    return rc;
}
static wtq_result_t observe_send(wtq_stream_t *st, const wtq_span_t *spans,
                                 size_t count, uint32_t flags, void *cookie)
{
    bool group_fin = wtq_stream_is_local(st) && !wtq_stream_is_bidi(st) &&
                     (flags & WTQ_SEND_FIN) != 0;
    size_t bytes = 0;
    for (size_t i = 0; i < count; ++i) bytes += spans[i].len;
    wtq_result_t rc = wtq_stream_send(st, spans, count, flags, cookie);
    if (group_fin && rc == WTQ_OK) {
        if (bytes) atomic_fetch_add(&active->data_fins, 1);
        else atomic_fetch_add(&active->bare_fins, 1);
    }
    return rc;
}
static wtq_result_t observe_reset(wtq_stream_t *st, uint32_t code)
{
    if (!atomic_load(&active->stopping)) atomic_fetch_add(&active->resets, 1);
    return wtq_stream_reset(st, code);
}
static wtq_result_t observe_abort(wtq_stream_t *st, uint32_t code)
{
    if (!atomic_load(&active->stopping)) atomic_fetch_add(&active->resets, 1);
    return wtq_stream_abort(st, code);
}

static void send_group(struct side *side, moq_session_t *s,
                       moq_wtquic_conn_t *c, moq_subscription_t sub, uint64_t now)
{
    struct run *r = side->run;
    NEED(r, !side->sent);
    side->sent = true;
    uint8_t property[10];
    property[0] = r->version == MOQ_VERSION_DRAFT_16 ? 2 : 6;
    size_t n = r->version == MOQ_VERSION_DRAFT_16
        ? moq_quic_varint_encode(TIMEOUT_MS, property + 1, sizeof(property) - 1)
        : moq_vi64_encode(TIMEOUT_MS, property + 1, sizeof(property) - 1);
    NEED(r, n != 0);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){property, n + 1};
    NEED(r, moq_session_accept_subscribe(s, sub, &ac, now) == MOQ_OK);
    NEED(r, moq_session_next_deadline_us(s) == UINT64_MAX);
    NEED(r, (c->ops.capabilities & MOQ_TRANSPORT_CAP_COMMIT_REPORT) != 0);
    NEED(r, wtq_session_send_commit_mode(c->ws) ==
            WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE);

    moq_subgroup_cfg_t gc;
    moq_subgroup_cfg_init(&gc);
    gc.group_id = 7;
    moq_subgroup_handle_t sg;
    r->key = c->next_stream_key;
    NEED(r, moq_session_open_subgroup(s, sub, &gc, now, &sg) == MOQ_OK);
    moq_wtquic_conn_service(c);
    struct moq_wtq_stream *ms = stream_find(c, r->key);
    NEED(r, ms != NULL && !ms->bidi && ms->commit_enabled && !ms->fin_sent);
    r->ref = moq_transport_bridge_find_ref(c->bridge, r->key);
    NEED(r, r->ref._v != 0);
    /* Opening arms no timer on any draft: each object carries its own
     * deadline, and a draft with a subgroup timer starts it at the FIN. */
    CHECK(r, moq_session_next_deadline_us(s) == UINT64_MAX);
    if (r->payload_group) {
        NEED(r, moq_rcbuf_create(&r->alloc, (const uint8_t *)"payload", 7,
                                 &r->payload) == MOQ_OK);
        NEED(r, moq_session_write_object(s, sg, 0, r->payload, now) == MOQ_OK);
        moq_wtquic_conn_service(c);
    }
    NEED(r, moq_session_close_subgroup(s, sg, now) == MOQ_OK);
    moq_wtquic_conn_service(c);
    ms = stream_find(c, r->key);
    NEED(r, ms != NULL && ms->fin_sent && !ms->committed && !ms->send_cancelled);
    NEED(r, moq_transport_bridge_find_ref(c->bridge, r->key)._v == r->ref._v);
    r->deadline = moq_session_next_deadline_us(s);
    CHECK(r, atomic_load(&r->commits) == 0);
    CHECK(r, atomic_load(&r->bare_fins) == 1 && atomic_load(&r->data_fins) == 0);
    if (r->version == MOQ_VERSION_DRAFT_16)
        /* No subgroup timer on draft-16; the object, if any, already left. */
        NEED(r, r->deadline == UINT64_MAX);
    else
        NEED(r, r->deadline != UINT64_MAX && r->deadline > now);
    r->pending = true;
}

static void poll_side(struct side *side, moq_session_t *s,
                      moq_wtquic_conn_t *c, uint64_t now)
{
    struct run *r = side->run;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            CHECK(r, moq_session_version(s) == r->version);
            if (!side->publisher) {
                CHECK(r, !side->subscribed);
                moq_subscribe_cfg_t sc;
                moq_subscribe_cfg_init(&sc);
                moq_bytes_t part = MOQ_BYTES_LITERAL("native-wt-commit");
                sc.track_namespace = (moq_namespace_t){&part, 1};
                sc.track_name = MOQ_BYTES_LITERAL("v");
                sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
                moq_subscription_t sub;
                CHECK(r, moq_session_subscribe(s, &sc, now, &sub) == MOQ_OK);
                side->subscribed = true;
            }
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            CHECK(r, side->publisher);
            if (side->publisher) send_group(side, s, c, ev.u.subscribe_request.sub, now);
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            CHECK(r, !side->publisher && r->payload_group && side->objects == 0);
            CHECK(r, ev.u.object_received.group_id == 7 && ev.u.object_received.object_id == 0);
            moq_rcbuf_t *p = ev.u.object_received.payload;
            CHECK(r, p != NULL && moq_rcbuf_len(p) == 7);
            if (p && moq_rcbuf_len(p) == 7)
                CHECK(r, memcmp(moq_rcbuf_data(p), "payload", 7) == 0);
            ++side->objects;
            break;
        }
        case MOQ_EVENT_SUBGROUP_FINISHED:
            CHECK(r, !side->publisher && side->fins == 0);
            CHECK(r, ev.u.subgroup_finished.group_id == 7 &&
                     ev.u.subgroup_finished.subgroup_id == 0);
            CHECK(r, side->objects == (r->payload_group ? 1u : 0u));
            ++side->fins;
            atomic_store(&r->receiver_done, true);
            notify(r);
            break;
        case MOQ_EVENT_SESSION_CLOSED:
        case MOQ_EVENT_SUBGROUP_RESET:
        case MOQ_EVENT_SUBSCRIBE_ERROR:
        case MOQ_EVENT_SUBSCRIBE_DONE:
            fprintf(stderr, "unexpected event=%u\n", (unsigned)ev.kind);
            CHECK(r, false);
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
        if (atomic_load(&r->failures)) return;
    }
    if (side->publisher && r->pending && !atomic_load(&r->publisher_done)) {
        if (atomic_load(&r->commits)) {
            CHECK(r, atomic_load(&r->commits) == 1);
            CHECK(r, moq_transport_bridge_find_ref(c->bridge, r->key)._v == 0);
            CHECK(r, moq_session_next_deadline_us(s) == UINT64_MAX);
            CHECK(r, moq_session_state(s) == MOQ_SESS_ESTABLISHED);
            CHECK(r, !moq_wtquic_conn_is_closed(c) && !moq_wtquic_conn_is_fatal(c));
            CHECK(r, atomic_load(&r->resets) == 0);
            atomic_store(&r->publisher_done, true);
            notify(r);
        } else if (!stream_find(c, r->key)) {
            /* A real generic stream terminal is not commitment. This also
             * discriminates disabled forwarding without waiting for a timer. */
            CHECK(r, atomic_load(&r->commits) == 1);
            CHECK(r, moq_transport_bridge_find_ref(c->bridge, r->key)._v == 0);
            CHECK(r, moq_session_next_deadline_us(s) == UINT64_MAX);
        }
    }
}

static int pump(moq_wtquic_msquic_managed_t *m,
                 moq_wtquic_msquic_managed_lane_t *lane, uint64_t now, void *ctx)
{
    (void)m;
    struct side *side = ctx;
    if (atomic_load(&side->run->stopping) || atomic_load(&side->run->failures)) return 0;
    for (moq_wtquic_msquic_managed_conn_t *conn = moq_wtquic_msquic_lane_next_conn(lane, NULL);
         conn; conn = moq_wtquic_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *s = moq_wtquic_msquic_managed_conn_session(conn);
        moq_wtquic_conn_t *c = moq_wtquic_msquic_managed_conn_adapter(conn);
        if (s && c) poll_side(side, s, c, now);
    }
    return 0;
}

static int run_case(moq_version_t version, bool client_publisher, bool payload,
                     const char *cert, const char *key)
{
    struct run r = {.mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER,
        .version = version, .client_publisher = client_publisher, .payload_group = payload};
    r.alloc = (moq_alloc_t){.ctx = &r, .alloc = tracked_alloc,
        .realloc = tracked_realloc, .free = tracked_free};
    r.sides[0] = (struct side){.run = &r, .publisher = !client_publisher};
    r.sides[1] = (struct side){.run = &r, .publisher = client_publisher};
    active = &r;
    moq_wtquic_msquic_managed_t *server = NULL, *client = NULL;
    char protocol[16];
    snprintf(protocol, sizeof(protocol), "moqt-%u", (unsigned)version);
    const char *offers[] = {protocol};
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &r.alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.wt_path = "/moq";
    cfg.wt_protocols = offers;
    cfg.wt_protocol_count = 1;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = &r.sides[0];
    int environment = 0;
    moq_result_t rc = wtqt_managed_server_create(&cfg, &server);
    if (rc != MOQ_OK) {
        fprintf(stderr, "ENVIRONMENT listener create=%d\n", (int)rc);
        environment = 2;
        goto done;
    }
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.cert_path = cfg.key_path = NULL;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump_user = &r.sides[1];
    rc = moq_wtquic_msquic_managed_create(&cfg, &client);
    if (rc != MOQ_OK) {
        fprintf(stderr, "ENVIRONMENT client create=%d\n", (int)rc);
        environment = 2;
        goto done;
    }
    struct timespec deadline = {0};
    CHECK(&r, clock_gettime(CLOCK_REALTIME, &deadline) == 0);
    deadline.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&r.mu);
    while (!atomic_load(&r.failures) &&
           !(atomic_load(&r.publisher_done) && atomic_load(&r.receiver_done))) {
        int wait_rc = pthread_cond_timedwait(&r.cv, &r.mu, &deadline);
        if (wait_rc != 0) {
            pthread_mutex_unlock(&r.mu);
            CHECK(&r, wait_rc == 0); /* ETIMEDOUT is a hang guard, never a pass. */
            pthread_mutex_lock(&r.mu);
            break;
        }
    }
    pthread_mutex_unlock(&r.mu);
done:
    atomic_store(&r.stopping, true);
    if (client) {
        CHECK(&r, moq_wtquic_msquic_managed_stop(client) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(client);
    }
    if (server) {
        CHECK(&r, moq_wtquic_msquic_managed_stop(server) == MOQ_OK);
        moq_wtquic_msquic_managed_destroy(server);
    }
    if (r.payload) {
        CHECK(&r, moq_rcbuf_refcount(r.payload) == 1);
        moq_rcbuf_decref(r.payload);
    }
    CHECK(&r, atomic_load(&r.allocations) == 0);
    unsigned failures = atomic_load(&r.failures);
    printf("%s D%u/%s/%s pending=%u deadline=%" PRIu64
           " commits=%u bare-fin=%u data-fin=%u receiver-fin=%u resets=%u allocations=%zu failures=%u\n",
        environment ? "ENVIRONMENT" : failures ? "FAIL" : "PASS", (unsigned)version,
        client_publisher ? "client-publisher" : "server-publisher",
        payload ? "payload-group" : "empty-group", r.pending, r.deadline,
        atomic_load(&r.commits), atomic_load(&r.bare_fins), atomic_load(&r.data_fins),
        r.sides[client_publisher ? 0 : 1].fins, atomic_load(&r.resets),
        atomic_load(&r.allocations), failures);
    active = NULL;
    pthread_cond_destroy(&r.cv);
    pthread_mutex_destroy(&r.mu);
    return environment ? environment : failures ? 1 : 0;
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s cert.pem key.pem\n", argv[0]);
        return 2;
    }
    const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    unsigned failed = 0, cases = 0;
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v)
        for (unsigned client = 0; client < 2; ++client)
            for (unsigned payload = 0; payload < 2; ++payload) {
                int rc = run_case(versions[v], client != 0, payload != 0, argv[1], argv[2]);
                if (rc == 2) return 2;
                if (rc) ++failed;
                ++cases;
            }
    printf("native managed WT commit: %u/%u passed\n", cases - failed, cases);
    return failed ? 1 : 0;
}
