/*
 * End-to-end group order over real picoquic (MOQT 7.1 / 7.2): a publisher
 * (publisher facade on a threaded picoquic server) bursts several large
 * groups at once, so they are all queued at the transport together; a
 * subscriber (raw session on a threaded picoquic client) records the order
 * in which the groups complete.
 *
 *   ascending  -> the groups complete oldest first (1, 2, 3, 4)
 *   descending -> the groups complete newest first (4, 3, 2, 1)
 *
 * The order comes from the subscriber's GROUP_ORDER, else from the track's
 * default_group_order, both drafts. Nothing here is faked: the ordering is
 * picoquic's stream scheduler acting on the priorities the session hands it.
 */
#include <moq/moq.h>
#include <moq/publisher.h>
#include <moq/picoquic_threaded.h>
#include "test_support.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

#define E2E_GROUPS      4
#define E2E_OBJECT_SIZE (128u * 1024u)

typedef struct {
    moq_group_order_t track_order;   /* publisher's default_group_order   */
    moq_group_order_t sub_order;     /* subscriber's GROUP_ORDER          */

    /* publisher (server) side, network-thread owned */
    moq_publisher_t  *pub;
    moq_pub_track_t  *track;
    int               next_group;    /* next group to write (1-based)     */
    moq_rcbuf_t      *payload;       /* retained across a WOULD_BLOCK     */
    atomic_bool       teardown;      /* test asks the pump to drop pub    */
    atomic_bool       pub_gone;      /* ... and the pump has done so      */

    /* subscriber (client) side, network-thread owned */
    bool              subscribed;
    moq_subscription_t sub;

    /* shared */
    pthread_mutex_t   mu;
    uint64_t          order[E2E_GROUPS];
    int               n;
    moq_group_order_t effective;     /* subscriber-side resolved order    */
    atomic_bool       failed;
} e2e_t;

static moq_rcbuf_t *make_payload(uint8_t fill)
{
    uint8_t *buf = (uint8_t *)malloc(E2E_OBJECT_SIZE);
    if (!buf) return NULL;
    memset(buf, fill, E2E_OBJECT_SIZE);
    moq_rcbuf_t *rb = NULL;
    moq_result_t rc = moq_rcbuf_create(moq_alloc_default(), buf,
                                       E2E_OBJECT_SIZE, &rb);
    free(buf);
    return rc == MOQ_OK ? rb : NULL;
}

/* Publisher: accept the subscription, then write every group in one pump
 * pass (retrying a WOULD_BLOCK on later passes), so the transport holds
 * them all at once and its scheduler decides what goes first. */
static int pub_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                    uint64_t now_us, void *ctx)
{
    (void)lane;
    e2e_t *e = (e2e_t *)ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (atomic_load(&e->teardown)) {
        /* The publisher is bound to this thread's session: drop it here. */
        if (e->pub) moq_pub_destroy(e->pub);
        e->pub = NULL;
        atomic_store(&e->pub_gone, true);
        return 0;
    }
    if (!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED) return 0;

    if (!e->pub) {
        moq_pub_cfg_t pc;
        moq_pub_cfg_init_sized(&pc, sizeof(pc));
        pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(s, moq_alloc_default(), &pc, &e->pub) != MOQ_OK) {
            atomic_store(&e->failed, true);
            return 1;
        }
        static const uint8_t live[] = { 'l', 'i', 'v', 'e' };
        moq_bytes_t part = { live, sizeof(live) };
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        tc.track_namespace = (moq_namespace_t){ &part, 1 };
        tc.track_name = MOQ_BYTES_LITERAL("v");
        tc.default_group_order = e->track_order;
        if (moq_pub_add_track(e->pub, &tc, now_us, &e->track) != MOQ_OK) {
            atomic_store(&e->failed, true);
            return 1;
        }
        e->next_group = 1;
    }
    (void)moq_pub_tick(e->pub, now_us);

    if (!moq_pub_has_subscriber(e->pub, e->track)) return 0;
    while (e->next_group <= E2E_GROUPS) {
        if (!e->payload) e->payload = make_payload((uint8_t)e->next_group);
        if (!e->payload) { atomic_store(&e->failed, true); return 1; }
        moq_result_t rc = moq_pub_write_object(e->pub, e->track,
                                               (uint64_t)e->next_group, 0,
                                               e->payload, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) break;   /* same call next pass */
        moq_rcbuf_decref(e->payload);
        e->payload = NULL;
        if (rc != MOQ_OK) { atomic_store(&e->failed, true); return 1; }
        e->next_group++;
    }
    return 0;
}

/* Subscriber: subscribe once, record each group as its object completes. */
static int sub_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                    uint64_t now_us, void *ctx)
{
    (void)lane;
    e2e_t *e = (e2e_t *)ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED) return 0;

    if (!e->subscribed) {
        static const uint8_t live[] = { 'l', 'i', 'v', 'e' };
        moq_bytes_t part = { live, sizeof(live) };
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ &part, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("v");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        sc.group_order = e->sub_order;
        if (moq_session_subscribe(s, &sc, now_us, &e->sub) != MOQ_OK)
            return 0;   /* retry next pass */
        e->subscribed = true;
    }

    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            pthread_mutex_lock(&e->mu);
            e->effective = moq_session_subscription_group_order(s, e->sub);
            pthread_mutex_unlock(&e->mu);
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            pthread_mutex_lock(&e->mu);
            if (e->n < E2E_GROUPS)
                e->order[e->n++] = ev.u.object_received.group_id;
            pthread_mutex_unlock(&e->mu);
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            atomic_store(&e->failed, true);
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

static moq_pq_threaded_t *start(bool server, const char *cert,
                                const char *key, int port, bool d18,
                                e2e_t *e)
{
    static const char *const alpn18[] = { "moqt-18" };
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.port = port;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.on_lane_pump = server ? pub_pump : sub_pump;
    cfg.on_lane_pump_ctx = e;
    if (server) {
        cfg.cert_path = cert;
        cfg.key_path = key;
    } else {
        cfg.host = "127.0.0.1";
        cfg.sni = "test.example.com";
        cfg.insecure_skip_verify = true;
    }
    if (d18) {
        cfg.alpn_list = alpn18;
        cfg.alpn_count = 1;
    }
    moq_pq_threaded_t *t = NULL;
    return moq_pq_threaded_create(&cfg, &t) == MOQ_OK ? t : NULL;
}

static void run_case(const char *cert, const char *key, bool d18,
                     moq_group_order_t track_order,
                     moq_group_order_t sub_order, moq_group_order_t want)
{
    e2e_t e;
    memset(&e, 0, sizeof(e));
    pthread_mutex_init(&e.mu, NULL);
    e.track_order = track_order;
    e.sub_order = sub_order;

    static int calls = 0;
    int base = 21300 + (int)(getpid() % 997) + (calls++ * 17);
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 8 && !srv; attempt++) {
        port = base + attempt * 7;
        srv = start(true, cert, key, port, d18, &e);
    }
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) { pthread_mutex_destroy(&e.mu); return; }
    moq_pq_threaded_t *cli = start(false, cert, key, port, d18, &e);
    MOQ_TEST_CHECK(cli != NULL);

    for (int i = 0; cli && i < 400; i++) {
        pthread_mutex_lock(&e.mu);
        int n = e.n;
        pthread_mutex_unlock(&e.mu);
        if (n >= E2E_GROUPS || atomic_load(&e.failed)) break;
        usleep(25000);
    }

    atomic_store(&e.teardown, true);
    for (int i = 0; i < 200 && !atomic_load(&e.pub_gone); i++) {
        moq_pq_threaded_wake(srv);
        usleep(5000);
    }
    MOQ_TEST_CHECK(atomic_load(&e.pub_gone));
    if (cli) {
        moq_pq_threaded_stop(cli);
        moq_pq_threaded_destroy(cli);
    }
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    /* The network threads are gone: the publisher state is ours now. */
    if (e.payload) moq_rcbuf_decref(e.payload);

    MOQ_TEST_CHECK(!atomic_load(&e.failed));
    MOQ_TEST_CHECK_EQ_INT(e.n, E2E_GROUPS);
    MOQ_TEST_CHECK_EQ_INT((int)e.effective, (int)want);
    printf("  draft-%d track=%u sub=%u -> completion order:",
           d18 ? 18 : 16, (unsigned)track_order, (unsigned)sub_order);
    for (int i = 0; i < e.n; i++) printf(" %llu", (unsigned long long)e.order[i]);
    printf("\n");
    if (e.n == E2E_GROUPS) {
        for (int i = 0; i < E2E_GROUPS; i++)
            MOQ_TEST_CHECK_EQ_U64(e.order[i],
                                  want == MOQ_GROUP_ORDER_ASCENDING
                                      ? (uint64_t)(i + 1)
                                      : (uint64_t)(E2E_GROUPS - i));
    }
    pthread_mutex_destroy(&e.mu);
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 2;
    }

    for (int d = 0; d < 2; d++) {
        bool d18 = d == 1;
        /* No preference anywhere: ascending. */
        run_case(cert, key, d18, MOQ_GROUP_ORDER_DEFAULT,
                 MOQ_GROUP_ORDER_DEFAULT, MOQ_GROUP_ORDER_ASCENDING);
        /* The subscriber asks for descending. */
        run_case(cert, key, d18, MOQ_GROUP_ORDER_DEFAULT,
                 MOQ_GROUP_ORDER_DESCENDING, MOQ_GROUP_ORDER_DESCENDING);
        /* The subscriber asks nothing; the publisher's default applies. */
        run_case(cert, key, d18, MOQ_GROUP_ORDER_DESCENDING,
                 MOQ_GROUP_ORDER_DEFAULT, MOQ_GROUP_ORDER_DESCENDING);
        /* The subscriber's choice wins over the publisher's. */
        run_case(cert, key, d18, MOQ_GROUP_ORDER_DESCENDING,
                 MOQ_GROUP_ORDER_ASCENDING, MOQ_GROUP_ORDER_ASCENDING);
    }

    if (failures == 0)
        printf("test_group_order_e2e: all passed\n");
    return failures ? 1 : 0;
}
