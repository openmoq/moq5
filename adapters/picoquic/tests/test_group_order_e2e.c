#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * Group-order negotiation and complete delivery over real picoquic, for
 * drafts 16, 18 and 22. The subscriber's preference wins, otherwise the
 * track's preference applies, otherwise ascending is selected.
 *
 * Each of four groups carries one distinct, fully checked object. All four
 * subgroup FINs must arrive after their objects. UDP completion order is
 * not a scheduling oracle: loss and flow control can delay one stream while
 * another completes. The deterministic simulator and bridge tests check
 * scheduling precedence separately.
 */
#include <moq/moq.h>
#include <moq/publisher.h>
#include <moq/picoquic_threaded.h>
#include "test_support.h"
#include "test_alloc_support.h"
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define E2E_GROUPS       4
#define E2E_OBJECT_SIZE  (128u * 1024u)
#define E2E_ALL_GROUPS   ((1u << E2E_GROUPS) - 1u)
#define E2E_RECEIVE_US   UINT64_C(10000000)
#define E2E_CLEANUP_US   UINT64_C(1000000)

typedef struct {
    const moq_alloc_t *alloc;
    moq_group_order_t track_order;
    moq_group_order_t sub_order;

    /* Publisher side, network-thread owned until stop joins it. */
    moq_publisher_t  *pub;
    moq_pub_track_t  *track;
    int               next_group;
    moq_rcbuf_t      *payload;       /* same ref across WOULD_BLOCK */
    bool              group_closed;
    atomic_bool       teardown;
    atomic_bool       pub_gone;      /* cleanup complete; never recreate */

    /* Subscriber side, network-thread owned. */
    bool              subscribed;
    moq_subscription_t sub;

    /* Observed by the application thread under mu. */
    pthread_mutex_t   mu;
    unsigned          received;
    unsigned          finished;
    bool              got_subscribe_ok;
    moq_group_order_t effective;
    atomic_bool       failed;
} e2e_t;

static bool e2e_init(e2e_t *e, const moq_alloc_t *alloc)
{
    memset(e, 0, sizeof(*e));
    e->alloc = alloc;
    atomic_init(&e->teardown, false);
    atomic_init(&e->pub_gone, false);
    atomic_init(&e->failed, false);
    return pthread_mutex_init(&e->mu, NULL) == 0;
}

static uint64_t monotonic_us(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        abort();
    }
    return (uint64_t)ts.tv_sec * UINT64_C(1000000) +
           (uint64_t)ts.tv_nsec / UINT64_C(1000);
}

static void sleep_us(uint64_t us)
{
    struct timespec ts = {
        .tv_sec = (time_t)(us / UINT64_C(1000000)),
        .tv_nsec = (long)((us % UINT64_C(1000000)) * UINT64_C(1000)),
    };
    while (nanosleep(&ts, &ts) != 0) {
        if (errno != EINTR) {
            perror("nanosleep");
            abort();
        }
    }
}

static moq_rcbuf_t *make_payload(const moq_alloc_t *alloc, uint8_t fill)
{
    uint8_t *buf = (uint8_t *)malloc(E2E_OBJECT_SIZE);
    if (!buf) return NULL;
    memset(buf, fill, E2E_OBJECT_SIZE);
    moq_rcbuf_t *rb = NULL;
    moq_result_t rc = moq_rcbuf_create(alloc, buf, E2E_OBJECT_SIZE, &rb);
    free(buf);
    return rc == MOQ_OK ? rb : NULL;
}

/* Called only by the publisher pump (or the single-threaded helper controls).
 * The adapter retains terminal sessions until its destruction. Publish the
 * acknowledgement only after releasing the facade and our pending buffer. */
static void publisher_cleanup(e2e_t *e)
{
    if (atomic_load(&e->pub_gone)) return;
    if (e->pub) moq_pub_destroy(e->pub);
    e->pub = NULL;
    e->track = NULL;
    moq_rcbuf_decref(e->payload);
    e->payload = NULL;
    atomic_store(&e->pub_gone, true);
}

static int publisher_fail(e2e_t *e)
{
    atomic_store(&e->failed, true);
    publisher_cleanup(e);
    /* Keep the loop alive; the application stops it after observing cleanup. */
    return 0;
}

static bool publisher_stopping(e2e_t *e, moq_session_t *s, bool terminal)
{
    if (atomic_load(&e->pub_gone)) return true;
    if (atomic_load(&e->teardown) || atomic_load(&e->failed)) {
        publisher_cleanup(e);
        return true;
    }
    if (terminal || (s && (moq_session_state(s) == MOQ_SESS_DRAINING ||
                          moq_session_state(s) == MOQ_SESS_CLOSED))) {
        (void)publisher_fail(e);
        return true;
    }
    return false;
}

static moq_result_t publisher_create(e2e_t *e, moq_session_t *s, uint64_t now_us)
{
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_result_t rc = moq_pub_create(s, e->alloc, &pc, &e->pub);
    if (rc != MOQ_OK) return rc;

    static const uint8_t live[] = { 'l', 'i', 'v', 'e' };
    moq_bytes_t part = { live, sizeof(live) };
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){ &part, 1 };
    tc.track_name = MOQ_BYTES_LITERAL("v");
    tc.group_order = e->track_order;
    rc = moq_pub_add_track(e->pub, &tc, now_us, &e->track);
    if (rc == MOQ_OK) e->next_group = 1;
    return rc;
}

/* Queue the objects, resuming the same operation after backpressure. No
 * assumption is made about how many groups fit in the endpoint queue. */
static int publisher_step(e2e_t *e, moq_session_t *s, uint64_t now_us,
                          bool terminal)
{
    if (publisher_stopping(e, s, terminal)) return 0;
    if (!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED) return 0;

    moq_result_t rc;
    if (!e->pub) {
        rc = publisher_create(e, s, now_us);
        if (rc != MOQ_OK) return publisher_fail(e);
    }
    rc = moq_pub_tick(e->pub, now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) return publisher_fail(e);

    if (!moq_pub_has_subscriber(e->pub, e->track)) return 0;
    while (e->next_group <= E2E_GROUPS) {
        if (!e->payload)
            e->payload = make_payload(e->alloc, (uint8_t)e->next_group);
        if (!e->payload) return publisher_fail(e);
        rc = moq_pub_write_object(e->pub, e->track,
                                 (uint64_t)e->next_group, 0, e->payload, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return 0;
        if (rc != MOQ_OK) return publisher_fail(e);
        moq_rcbuf_decref(e->payload);
        e->payload = NULL;
        e->next_group++;
    }
    if (!e->group_closed) {
        /* Changing groups closed the preceding three subgroups. Close the
         * final one too, so completion is evidenced by four real FINs. */
        rc = moq_pub_end_group(e->pub, e->track, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return 0;
        if (rc != MOQ_OK) return publisher_fail(e);
        e->group_closed = true;
    }
    return 0;
}

static int pub_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                    uint64_t now_us, void *ctx)
{
    (void)lane;
    moq_session_t *s = moq_pq_threaded_session(t);
    moq_pq_conn_t *c = moq_pq_threaded_conn(t);
    bool terminal = c && (moq_pq_conn_is_fatal(c) || moq_pq_conn_is_closed(c));
    return publisher_step((e2e_t *)ctx, s, now_us, terminal);
}

static bool object_valid(const e2e_t *e, const moq_object_received_event_t *o)
{
    if (!e->subscribed || o->sub._opaque != e->sub._opaque || o->pub._opaque ||
        o->group_id < 1 || o->group_id > E2E_GROUPS || o->subgroup_id != 0 ||
        o->object_id != 0 || o->status != MOQ_OBJECT_NORMAL || o->datagram ||
        !o->payload || moq_rcbuf_len(o->payload) != E2E_OBJECT_SIZE)
        return false;
    const uint8_t *data = moq_rcbuf_data(o->payload);
    for (size_t i = 0; i < E2E_OBJECT_SIZE; i++)
        if (data[i] != (uint8_t)o->group_id) return false;
    return true;
}

static bool record_object(e2e_t *e, const moq_object_received_event_t *o)
{
    if (!object_valid(e, o)) return false;
    unsigned bit = 1u << (unsigned)(o->group_id - 1);
    pthread_mutex_lock(&e->mu);
    bool fresh = ((e->received | e->finished) & bit) == 0;
    if (fresh) e->received |= bit;
    pthread_mutex_unlock(&e->mu);
    return fresh;
}

static bool record_finished(e2e_t *e, const moq_subgroup_finished_event_t *f)
{
    if (!e->subscribed || f->sub._opaque != e->sub._opaque || f->pub._opaque ||
        f->group_id < 1 || f->group_id > E2E_GROUPS || f->subgroup_id != 0 ||
        !f->has_last_object || f->last_object_id != 0)
        return false;
    unsigned bit = 1u << (unsigned)(f->group_id - 1);
    pthread_mutex_lock(&e->mu);
    bool fresh = (e->received & bit) != 0 && (e->finished & bit) == 0;
    if (fresh) e->finished |= bit;
    pthread_mutex_unlock(&e->mu);
    return fresh;
}

static bool delivery_complete(e2e_t *e)
{
    pthread_mutex_lock(&e->mu);
    bool complete = e->got_subscribe_ok && e->received == E2E_ALL_GROUPS &&
                    e->finished == E2E_ALL_GROUPS;
    pthread_mutex_unlock(&e->mu);
    return complete;
}

static int sub_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                    uint64_t now_us, void *ctx)
{
    (void)lane;
    e2e_t *e = (e2e_t *)ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s) return 0;

    if (moq_session_state(s) == MOQ_SESS_ESTABLISHED && !e->subscribed &&
        !atomic_load(&e->teardown) && !atomic_load(&e->failed)) {
        static const uint8_t live[] = { 'l', 'i', 'v', 'e' };
        moq_bytes_t part = { live, sizeof(live) };
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ &part, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("v");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        sc.group_order = e->sub_order;
        moq_result_t rc = moq_session_subscribe(s, &sc, now_us, &e->sub);
        if (rc == MOQ_OK) e->subscribed = true;
        else if (rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_REQUEST_BLOCKED)
            atomic_store(&e->failed, true);
    }

    /* Drain final events too: a terminal session must not hide a failure. */
    for (;;) {
        moq_event_t ev;
        size_t n = 0;
        moq_result_t rc = moq_session_poll_events_ex(s, &ev, 1, sizeof(ev), &n);
        if (rc != MOQ_OK) {
            atomic_store(&e->failed, true);
            return 0;
        }
        if (n == 0) break;
        bool valid = true;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            pthread_mutex_lock(&e->mu);
            valid = e->subscribed && !e->got_subscribe_ok &&
                    ev.u.subscribe_ok.sub._opaque == e->sub._opaque;
            if (valid) {
                e->effective = moq_session_subscription_group_order(s, e->sub);
                e->got_subscribe_ok = true;
            }
            pthread_mutex_unlock(&e->mu);
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            valid = record_object(e, &ev.u.object_received);
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            valid = record_finished(e, &ev.u.subgroup_finished);
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            valid = false;
        } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED ||
                   ev.kind == MOQ_EVENT_SUBSCRIBE_DONE ||
                   ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
            valid = atomic_load(&e->teardown);
        }
        if (!valid) atomic_store(&e->failed, true);
        moq_event_cleanup(&ev);
    }
    return 0;
}

static moq_pq_threaded_t *start(bool server, const char *cert,
                                const char *key, int port, int draft,
                                e2e_t *e)
{
    static const char *const alpn16[] = { "moqt-16" };
    static const char *const alpn18[] = { "moqt-18" };
    static const char *const alpn22[] = { "moqt-22" };
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = e->alloc;
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.port = port;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.on_lane_pump = server ? pub_pump : sub_pump;
    cfg.on_lane_pump_ctx = e;
    if (server) {
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.max_connections = 1;
        /* Keep the publisher's session alive through a terminal/fatal pump.
         * Cleanup runs there; stop/destroy later reclaim the retained record. */
        cfg.retain_terminal_until_ack = true;
    } else {
        cfg.host = "127.0.0.1";
        cfg.sni = "test.example.com";
        cfg.insecure_skip_verify = true;
    }
    cfg.alpn_list = draft == 16 ? alpn16 : draft == 18 ? alpn18 : alpn22;
    cfg.alpn_count = 1;
    moq_pq_threaded_t *t = NULL;
    return moq_pq_threaded_create(&cfg, &t) == MOQ_OK ? t : NULL;
}

static void run_case(const char *cert, const char *key, int draft,
                     moq_group_order_t track_order,
                     moq_group_order_t sub_order, moq_group_order_t want)
{
    e2e_t e;
    bool initialized = e2e_init(&e, moq_alloc_default());
    MOQ_TEST_CHECK(initialized);
    if (!initialized) return;
    e.track_order = track_order;
    e.sub_order = sub_order;

    static int calls = 0;
    int base = 21300 + (int)(getpid() % 997) + (calls++ * 17);
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 8 && !srv; attempt++) {
        port = base + attempt * 7;
        srv = start(true, cert, key, port, draft, &e);
    }
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) { pthread_mutex_destroy(&e.mu); return; }
    moq_pq_threaded_t *cli = start(false, cert, key, port, draft, &e);
    MOQ_TEST_CHECK(cli != NULL);
    if (!cli) atomic_store(&e.failed, true);

    uint64_t deadline = monotonic_us() + E2E_RECEIVE_US;
    while (cli && monotonic_us() < deadline) {
        if (delivery_complete(&e) || atomic_load(&e.failed)) break;
        if (moq_pq_threaded_is_fatal(cli) || moq_pq_threaded_is_fatal(srv)) {
            atomic_store(&e.failed, true);
            break;
        }
        sleep_us(25000);
    }
    bool complete = delivery_complete(&e);
    if (!complete) atomic_store(&e.failed, true);

    atomic_store(&e.teardown, true);
    deadline = monotonic_us() + E2E_CLEANUP_US;
    while (!atomic_load(&e.pub_gone) && monotonic_us() < deadline) {
        moq_result_t rc = moq_pq_threaded_wake(srv);
        if (rc != MOQ_OK) break;
        sleep_us(5000);
    }
    if (!atomic_load(&e.pub_gone)) {
        /* Do not destroy its session or return stack state to a live pump.
         * An endpoint-wide failure may make acknowledgement impossible. */
        fprintf(stderr, "FAIL: draft-%d publisher cleanup was not acknowledged\n", draft);
        abort();
    }
    if (cli) {
        MOQ_TEST_CHECK(moq_pq_threaded_stop(cli) == MOQ_OK);
        moq_pq_threaded_destroy(cli);
    }
    MOQ_TEST_CHECK(moq_pq_threaded_stop(srv) == MOQ_OK);
    moq_pq_threaded_destroy(srv);

    MOQ_TEST_CHECK(e.pub == NULL && e.track == NULL && e.payload == NULL);
    MOQ_TEST_CHECK(!atomic_load(&e.failed));
    MOQ_TEST_CHECK(complete);
    MOQ_TEST_CHECK(e.got_subscribe_ok);
    MOQ_TEST_CHECK_EQ_INT((int)e.effective, (int)want);
    MOQ_TEST_CHECK_EQ_INT((int)e.received, (int)E2E_ALL_GROUPS);
    MOQ_TEST_CHECK_EQ_INT((int)e.finished, (int)E2E_ALL_GROUPS);
    printf("  draft-%d track=%u sub=%u -> effective=%u objects=%x FINs=%x\n",
           draft, (unsigned)track_order, (unsigned)sub_order,
           (unsigned)e.effective, e.received, e.finished);
    pthread_mutex_destroy(&e.mu);
}

/* No sockets or threads: exercise the same validators and cleanup helpers
 * with real buffers/publishers, including terminal and partial-create paths. */
static void test_delivery_helpers(void)
{
    e2e_t e;
    bool initialized = e2e_init(&e, moq_alloc_default());
    MOQ_TEST_CHECK(initialized);
    if (!initialized) return;
    e.subscribed = true;
    e.sub._opaque = 17;
    e.got_subscribe_ok = true;
    static const unsigned groups[] = { 3, 1, 4, 2 };
    for (size_t i = 0; i < sizeof(groups) / sizeof(groups[0]); i++) {
        moq_object_received_event_t o = {
            .sub = e.sub, .group_id = groups[i], .status = MOQ_OBJECT_NORMAL,
            .payload = make_payload(e.alloc, (uint8_t)groups[i]),
        };
        MOQ_TEST_CHECK(o.payload != NULL);
        if (!o.payload) break;
        moq_subgroup_finished_event_t f = {
            .sub = e.sub, .group_id = groups[i], .has_last_object = true,
        };
        MOQ_TEST_CHECK(!record_finished(&e, &f)); /* FIN cannot hide a missing object */
        MOQ_TEST_CHECK(record_object(&e, &o));
        MOQ_TEST_CHECK(!record_object(&e, &o));
        MOQ_TEST_CHECK(record_finished(&e, &f));
        MOQ_TEST_CHECK(!record_finished(&e, &f));
        moq_rcbuf_decref(o.payload);
    }
    MOQ_TEST_CHECK(delivery_complete(&e));

    moq_object_received_event_t o = {
        .sub = e.sub, .group_id = 1, .status = MOQ_OBJECT_NORMAL,
        .payload = make_payload(e.alloc, 1),
    };
    MOQ_TEST_CHECK(o.payload != NULL);
    if (o.payload) {
        MOQ_TEST_CHECK(object_valid(&e, &o));
        o.object_id = 1;
        MOQ_TEST_CHECK(!object_valid(&e, &o));
        o.object_id = 0;
        o.sub._opaque++;
        MOQ_TEST_CHECK(!object_valid(&e, &o));
        o.sub = e.sub;
        o.group_id = E2E_GROUPS + 1;
        MOQ_TEST_CHECK(!object_valid(&e, &o));
        o.group_id = 1;
        moq_rcbuf_decref(o.payload);
    }
    uint8_t *bytes = malloc(E2E_OBJECT_SIZE);
    MOQ_TEST_CHECK(bytes != NULL);
    if (bytes) {
        memset(bytes, 1, E2E_OBJECT_SIZE);
        o.payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(e.alloc, bytes, E2E_OBJECT_SIZE - 1,
                                        &o.payload) == MOQ_OK);
        MOQ_TEST_CHECK(!object_valid(&e, &o));
        moq_rcbuf_decref(o.payload);
        bytes[E2E_OBJECT_SIZE - 1] = 2;
        o.payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(e.alloc, bytes, E2E_OBJECT_SIZE,
                                        &o.payload) == MOQ_OK);
        MOQ_TEST_CHECK(!object_valid(&e, &o));
        moq_rcbuf_decref(o.payload);
        free(bytes);
    }
    pthread_mutex_destroy(&e.mu);
}

static void count_payload_release(void *ctx, const uint8_t *data, size_t len)
{
    (void)data;
    (void)len;
    (*(unsigned *)ctx)++;
}

static void test_cleanup_helpers(void)
{
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        for (unsigned mode = 0; mode < 5; mode++) {
            test_alloc_state_t account = {0};
            moq_alloc_t alloc = test_allocator(&account);
            e2e_t e;
            bool initialized = e2e_init(&e, &alloc);
            MOQ_TEST_CHECK(initialized);
            if (!initialized) continue;
            moq_session_cfg_t cfg;
            moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc, MOQ_PERSPECTIVE_SERVER);
            cfg.version = versions[v];
            moq_session_t *s = NULL;
            MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
            if (!s) { pthread_mutex_destroy(&e.mu); continue; }

            /* Invalid preference fails add_track AFTER the publisher exists. */
            if (mode == 1) e.track_order = (moq_group_order_t)99;
            moq_result_t rc = publisher_create(&e, s, 0);
            MOQ_TEST_CHECK(rc == (mode == 1 ? MOQ_ERR_INVAL : MOQ_OK));
            MOQ_TEST_CHECK(e.pub != NULL);
            unsigned released = 0;
            const uint8_t byte = 1;
            MOQ_TEST_CHECK(moq_rcbuf_wrap(&alloc, &byte, 1, count_payload_release,
                                          &released, &e.payload) == MOQ_OK);
            MOQ_TEST_CHECK(e.payload != NULL);

            if (mode == 0)
                MOQ_TEST_CHECK(moq_session_on_transport_close(s, 0, 0) == MOQ_OK);
            else if (mode == 1)
                MOQ_TEST_CHECK(publisher_fail(&e) == 0);
            else if (mode == 2)
                atomic_store(&e.failed, true); /* subscriber failure */
            else if (mode == 3)
                atomic_store(&e.teardown, true);
            /* mode 4: adapter fatal while the session is still nonterminal. */
            MOQ_TEST_CHECK(publisher_step(&e, s, 0, mode == 4) == 0);
            MOQ_TEST_CHECK(atomic_load(&e.pub_gone));
            MOQ_TEST_CHECK(atomic_load(&e.failed) == (mode != 3));
            MOQ_TEST_CHECK(e.pub == NULL && e.track == NULL && e.payload == NULL);
            MOQ_TEST_CHECK_EQ_INT((int)released, 1);
            MOQ_TEST_CHECK(publisher_stopping(&e, s, false));
            MOQ_TEST_CHECK(publisher_step(&e, s, 0, false) == 0);
            MOQ_TEST_CHECK_EQ_INT((int)released, 1);
            moq_session_destroy(s);
            /* The completed latch is safe even after the session is gone. */
            MOQ_TEST_CHECK(publisher_step(&e, NULL, 0, false) == 0);
            MOQ_TEST_CHECK(account.balance == 0);
            pthread_mutex_destroy(&e.mu);
        }
    }
}

int main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--self-test")) {
        test_delivery_helpers();
        test_cleanup_helpers();
        MOQ_TEST_PASS("group order delivery and cleanup helpers");
        return failures ? 1 : 0;
    }
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
        else {
            fprintf(stderr, "usage: --cert <pem> --key <pem>, or --self-test\n");
            return 2;
        }
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>, or --self-test\n");
        return 2;
    }

    static const int drafts[] = { 16, 18, 22 };
    for (size_t d = 0; d < sizeof(drafts) / sizeof(drafts[0]); d++) {
        int draft = drafts[d];
        run_case(cert, key, draft, MOQ_GROUP_ORDER_DEFAULT,
                 MOQ_GROUP_ORDER_DEFAULT, MOQ_GROUP_ORDER_ASCENDING);
        run_case(cert, key, draft, MOQ_GROUP_ORDER_DEFAULT,
                 MOQ_GROUP_ORDER_DESCENDING, MOQ_GROUP_ORDER_DESCENDING);
        run_case(cert, key, draft, MOQ_GROUP_ORDER_DESCENDING,
                 MOQ_GROUP_ORDER_DEFAULT, MOQ_GROUP_ORDER_DESCENDING);
        run_case(cert, key, draft, MOQ_GROUP_ORDER_DESCENDING,
                 MOQ_GROUP_ORDER_ASCENDING, MOQ_GROUP_ORDER_ASCENDING);
    }
    if (failures == 0) printf("test_group_order_e2e: all passed\n");
    return failures ? 1 : 0;
}
