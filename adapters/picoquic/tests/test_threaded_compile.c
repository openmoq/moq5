/*
 * Tests for moq_pq_threaded: config validation, lifecycle, client
 * and server network threads, wake/wait/stop, loopback.
 */

#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>
#include <picoquic.h>
#include <moq/publisher.h>
#include <moq/subscriber.h>
#include <moq/rcbuf.h>
#include <pthread.h>
#include <sched.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                       \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#define PASS(name)                                                      \
    do { if (failures == 0) printf("PASS: %s\n", (name)); } while (0)

/* -- Counting allocator --------------------------------------------- */

typedef struct {
    int64_t balance;
    int     fail_at;
    int     call_count;
} test_alloc_state_t;

static void *test_alloc(size_t size, void *ctx)
{
    test_alloc_state_t *s = (test_alloc_state_t *)ctx;
    s->call_count++;
    if (s->fail_at > 0 && s->call_count >= s->fail_at) return NULL;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}

static void *test_realloc(void *ptr, size_t old_sz, size_t new_sz,
                           void *ctx)
{
    (void)old_sz; (void)ctx;
    return realloc(ptr, new_sz);
}

static void test_free(void *ptr, size_t size, void *ctx)
{
    test_alloc_state_t *s = (test_alloc_state_t *)ctx;
    (void)size;
    if (ptr) s->balance--;
    free(ptr);
}

/* -- Pump helpers --------------------------------------------------- */

static int dummy_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    (void)t; (void)now; (void)ctx;
    return 0;
}

typedef struct {
    volatile int pump_count;
    int          exit_after;
} pump_counter_t;

static int counting_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    (void)t; (void)now;
    pump_counter_t *pc = (pump_counter_t *)ctx;
    int n = __atomic_add_fetch(&pc->pump_count, 1, __ATOMIC_SEQ_CST);
    if (pc->exit_after > 0 && n >= pc->exit_after)
        return 1;
    return 0;
}

/* -- Configure_quic helpers ----------------------------------------- */

static int configure_quic_ok(picoquic_quic_t *quic, void *ctx)
{
    (void)quic;
    int *called = (int *)ctx;
    (*called)++;
    return 0;
}

static int configure_quic_fail(picoquic_quic_t *quic, void *ctx)
{
    (void)quic; (void)ctx;
    return -1;
}

/* Installs the libmoq production verifier (system trust store). */
static int configure_quic_verify(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    return moq_picoquic_set_cert_verifier(quic, NULL);  /* NULL = system */
}

/* Verifier with a nonexistent CA file → helper returns -1 → configure_quic
 * fails → create fails cleanly (exercises the bad-CA-path edge). */
static int configure_quic_bad_ca(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    return moq_picoquic_set_cert_verifier(quic, "/nonexistent/moq-test-ca.pem");
}

/* Short handshake timeout so a connect to a dead port fails fast and
 * bounded, rather than waiting out the default idle timeout. */
static int configure_quic_fast_timeout(picoquic_quic_t *quic, void *ctx)
{
    (void)ctx;
    picoquic_set_default_handshake_timeout(quic, 1000000);  /* 1s */
    return 0;
}

/* -- Stop-from-pump helper ------------------------------------------ */

typedef struct {
    moq_pq_threaded_t  *t;
    volatile int        stop_rc;
    volatile int        called;
} stop_from_pump_ctx_t;

static int stop_from_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    (void)now;
    stop_from_pump_ctx_t *sc = (stop_from_pump_ctx_t *)ctx;
    if (!__atomic_load_n(&sc->called, __ATOMIC_ACQUIRE)) {
        __atomic_store_n(&sc->called, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&sc->stop_rc, (int)moq_pq_threaded_stop(t),
                          __ATOMIC_RELEASE);
    }
    return 1;
}

/* -- Wake/stop stress thread ---------------------------------------- */

static void *wake_stress_thread(void *arg)
{
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)arg;
    for (int i = 0; i < 200; i++)
        moq_pq_threaded_wake(t);
    return NULL;
}

/* -- Facade smoke test state ---------------------------------------- */

#define SMOKE_PAYLOAD "threaded-smoke"
#define SMOKE_PAYLOAD_LEN 14

typedef struct {
    moq_publisher_t  *pub;
    moq_pub_track_t  *track;
    volatile int      obj_written;
    volatile int      do_finish;       /* test triggers finish_subscribers(0x2) */
    volatile int      finished;        /* latched once finish succeeds */
} smoke_srv_t;

static int smoke_srv_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    smoke_srv_t *s = (smoke_srv_t *)ctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess) return 0;

    if (!s->pub) {
        moq_pub_cfg_t pc; moq_pub_cfg_init_sized(&pc, sizeof(pc));
        pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(sess, moq_alloc_default(), &pc, &s->pub) != MOQ_OK)
            return 1;
        moq_pub_track_cfg_t tc; moq_pub_track_cfg_init(&tc);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("smoke") };
        tc.track_namespace.parts = ns; tc.track_namespace.count = 1;
        tc.track_name = MOQ_BYTES_LITERAL("data");
        if (moq_pub_add_track(s->pub, &tc, now, &s->track) != MOQ_OK)
            return 1;
    }

    /* Live-write the smoke object once the subscriber has joined (a plain
     * SUBSCRIBE does not replay retained objects, so deliver it live). */
    if (!__atomic_load_n(&s->obj_written, __ATOMIC_ACQUIRE) &&
        moq_pub_has_subscriber(s->pub, s->track)) {
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(moq_alloc_default(),
                (const uint8_t *)SMOKE_PAYLOAD, SMOKE_PAYLOAD_LEN,
                &payload) != MOQ_OK)
            return 1;
        moq_result_t wrc = moq_pub_write_object(s->pub, s->track, 0, 0,
                                                payload, now);
        moq_rcbuf_decref(payload);
        if (wrc == MOQ_OK)
            __atomic_store_n(&s->obj_written, 1, __ATOMIC_RELEASE);
        else if (wrc != MOQ_ERR_WOULD_BLOCK)
            return 1;
    }

    /* On request: finish the subscriber with Track Ended (0x2) WITHOUT ending
     * the track -- reproduces the live->VOD step-1 path over real QUIC. */
    if (__atomic_load_n(&s->do_finish, __ATOMIC_ACQUIRE) &&
        !__atomic_load_n(&s->finished, __ATOMIC_ACQUIRE) &&
        moq_pub_has_subscriber(s->pub, s->track)) {
        moq_result_t frc = moq_pub_finish_subscribers(
            s->pub, s->track, MOQ_PUB_DONE_TRACK_ENDED, now);
        if (frc == MOQ_OK)
            __atomic_store_n(&s->finished, 1, __ATOMIC_RELEASE);
        else if (frc != MOQ_ERR_WOULD_BLOCK && frc != MOQ_ERR_WRONG_STATE)
            return 1;
    }

    { moq_result_t trc = moq_pub_tick(s->pub, now);
      if (trc < 0 && trc != MOQ_ERR_WOULD_BLOCK) return 1; }
    return 0;
}

typedef struct {
    moq_subscriber_t *sub;
    moq_sub_track_t  *track;
    volatile int      subscribed;
    volatile int      received;
    volatile int      payload_ok;
    volatile int      done;            /* on_subscribe_done count */
    volatile uint64_t done_status;     /* status from on_subscribe_done */
    volatile int      session_closed;  /* on_closed fired (session torn down) */
} smoke_cli_t;

static void smoke_on_subscribed(void *ctx, moq_sub_track_t *track)
{
    (void)track;
    smoke_cli_t *c = (smoke_cli_t *)ctx;
    __atomic_store_n(&c->subscribed, 1, __ATOMIC_RELEASE);
}

static void smoke_on_subscribe_done(void *ctx, moq_sub_track_t *track,
                                    uint64_t status_code)
{
    (void)track;
    smoke_cli_t *c = (smoke_cli_t *)ctx;
    __atomic_store_n(&c->done_status, status_code, __ATOMIC_RELEASE);
    __atomic_store_n(&c->done, 1, __ATOMIC_RELEASE);
}

static void smoke_on_closed(void *ctx, uint64_t error_code)
{
    (void)error_code;
    smoke_cli_t *c = (smoke_cli_t *)ctx;
    __atomic_store_n(&c->session_closed, 1, __ATOMIC_RELEASE);
}

static int smoke_cli_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    smoke_cli_t *c = (smoke_cli_t *)ctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess) return 0;

    if (!c->sub &&
        moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
        /* Sized init: on_subscribe_done is an appended (post-v0) field, so the
         * plain moq_sub_cfg_init() leaves struct_size at the v0 prefix and the
         * callback is silently ignored (SUB_CFG_HAS is false). */
        moq_sub_cfg_t sc; moq_sub_cfg_init_sized(&sc, sizeof(sc));
        sc.callbacks.ctx = c;
        sc.callbacks.on_subscribed = smoke_on_subscribed;
        sc.callbacks.on_closed = smoke_on_closed;
        sc.on_subscribe_done = smoke_on_subscribe_done;
        if (moq_sub_create(sess, moq_alloc_default(), &sc, &c->sub) != MOQ_OK)
            return 1;
        moq_sub_track_cfg_t tc; moq_sub_track_cfg_init(&tc);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("smoke") };
        tc.track_namespace.parts = ns; tc.track_namespace.count = 1;
        tc.track_name = MOQ_BYTES_LITERAL("data");
        if (moq_sub_subscribe(c->sub, &tc, now, &c->track) != MOQ_OK)
            return 1;
    }

    if (!c->sub) return 0;

    { moq_result_t trc = moq_sub_tick(c->sub, now);
      if (trc < 0 && trc != MOQ_ERR_WOULD_BLOCK) return 1; }

    moq_sub_object_t obj;
    while (moq_sub_poll_object(c->sub, &obj) == MOQ_OK) {
        if (obj.payload &&
            moq_rcbuf_len(obj.payload) == SMOKE_PAYLOAD_LEN &&
            memcmp(moq_rcbuf_data(obj.payload),
                   SMOKE_PAYLOAD, SMOKE_PAYLOAD_LEN) == 0) {
            __atomic_store_n(&c->payload_ok, 1, __ATOMIC_RELEASE);
        }
        __atomic_store_n(&c->received, 1, __ATOMIC_RELEASE);
        moq_sub_object_cleanup(&obj);
    }
    return 0;
}

/* -- Multi-connection server test (raw session on both sides) -------- *
 * Uses raw moq_session_* (not the pub/sub facades) so there are no test-owned
 * per-connection objects whose lifetime must be reconciled with the adapter's
 * connection prune. Namespace "mconn" / track "vid"; NEXT_GROUP + group 0 --
 * the proven delivery pattern from test_managed_server_loopback. */

/* Per-connection server state (keyed by the opaque conn handle). sent counts
 * groups written to this conn (0, 1, then 2 for the survivor). */
typedef struct {
    moq_pq_threaded_conn_t *conn;
    moq_subscription_t      sub;
    int                     accepted;
    int                     sent;
} mc_srv_conn_t;

typedef struct {
    mc_srv_conn_t conns[8];
    int           n;
    volatile int  may_close;    /* test arms the drop after both received */
    int           close_done;   /* the 2nd connection has been closed */
} mc_srv_t;

static mc_srv_conn_t *mc_srv_find(mc_srv_t *s, moq_pq_threaded_conn_t *c)
{
    for (int i = 0; i < s->n; i++)
        if (s->conns[i].conn == c) return &s->conns[i];
    if (s->n < 8) {
        memset(&s->conns[s->n], 0, sizeof(mc_srv_conn_t));
        s->conns[s->n].conn = c;
        return &s->conns[s->n++];
    }
    return NULL;
}

/* Write one object into a fresh group `group` on this connection. */
static void mc_srv_write(moq_session_t *sess, moq_subscription_t sub,
                         uint64_t group, uint64_t now)
{
    moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
    sg.group_id = group; sg.publisher_priority = 200;
    moq_subgroup_handle_t h;
    if (moq_session_open_subgroup(sess, sub, &sg, now, &h) < 0) return;
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(),
            (const uint8_t *)SMOKE_PAYLOAD, SMOKE_PAYLOAD_LEN, &b) == MOQ_OK) {
        (void)moq_session_write_object(sess, h, 0, b, now);
        moq_rcbuf_decref(b);
    }
    moq_session_close_subgroup(sess, h, now);
}

static int mc_srv_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    mc_srv_t *s = (mc_srv_t *)ctx;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_next_conn(t, c)) != NULL) {
        moq_session_t *sess = moq_pq_threaded_conn_session(c);
        if (!sess) continue;
        mc_srv_conn_t *e = mc_srv_find(s, c);
        if (!e) continue;

        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(sess, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !e->accepted) {
                e->sub = ev[i].u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                if (moq_session_accept_subscribe(sess, e->sub, &acfg, now) >= 0)
                    e->accepted = 1;
            }
            moq_event_cleanup(&ev[i]);
        }

        /* First object to every accepted connection. */
        if (e->accepted && e->sent == 0) {
            mc_srv_write(sess, e->sub, 0, now);
            e->sent = 1;
        }
        /* After the drop, a SECOND object to the survivor (conns[0]) proves it
         * keeps receiving. conns[0] is never the connection we close. */
        if (s->close_done && e == &s->conns[0] && e->sent == 1) {
            mc_srv_write(sess, e->sub, 1, now);
            e->sent = 2;
        }
    }

    /* Once the test arms the drop (both clients have received), close the
     * SECOND connection (deferred prune) via the public per-connection API. */
    if (!s->close_done && __atomic_load_n(&s->may_close, __ATOMIC_ACQUIRE) &&
        s->n >= 2 && s->conns[0].sent >= 1 && s->conns[1].sent >= 1) {
        moq_pq_threaded_conn_close(s->conns[1].conn, 0);
        s->close_done = 1;
    }
    return 0;
}

/* Raw subscriber client: subscribe to "mconn"/"vid" and count received
 * objects. */
typedef struct {
    int                 subscribed;
    moq_subscription_t  sub;
    volatile int        received;   /* count of objects received */
} mc_cli_t;

static int mc_cli_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    mc_cli_t *c = (mc_cli_t *)ctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess) return 0;

    if (!c->subscribed &&
        moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("mconn") };
        sc.track_namespace.parts = ns; sc.track_namespace.count = 1;
        sc.track_name = MOQ_BYTES_LITERAL("vid");
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        if (moq_session_subscribe(sess, &sc, now, &c->sub) >= 0)
            c->subscribed = 1;
    }

    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(sess, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED)
            __atomic_add_fetch(&c->received, 1, __ATOMIC_SEQ_CST);
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* -- Config builders ------------------------------------------------ */

static void make_client_cfg(moq_pq_threaded_cfg_t *cfg,
                             const moq_alloc_t *alloc)
{
    moq_pq_threaded_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = alloc;
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "localhost";
    cfg->port = 4443;
    cfg->on_pump = dummy_pump;
}

#ifdef MOQ_TEST_CERT_PATH
static void make_server_cfg_real(moq_pq_threaded_cfg_t *cfg,
                                  const moq_alloc_t *alloc)
{
    moq_pq_threaded_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = alloc;
    cfg->perspective = MOQ_PERSPECTIVE_SERVER;
    cfg->cert_path = MOQ_TEST_CERT_PATH;
    cfg->key_path = MOQ_TEST_KEY_PATH;
    cfg->port = 14600 + (rand() % 400);
    cfg->on_pump = dummy_pump;
    cfg->insecure_skip_verify = true;
}
#endif

static void make_server_cfg_stub(moq_pq_threaded_cfg_t *cfg,
                                  const moq_alloc_t *alloc)
{
    moq_pq_threaded_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = alloc;
    cfg->perspective = MOQ_PERSPECTIVE_SERVER;
    cfg->cert_path = "/tmp/nonexistent_cert.pem";
    cfg->key_path = "/tmp/nonexistent_key.pem";
    cfg->port = 4443;
    cfg->on_pump = dummy_pump;
}

/* -- Waiter thread -------------------------------------------------- */

typedef struct {
    moq_pq_threaded_t  *t;
    moq_result_t        rc;
    volatile int        entered;
} waiter_ctx_t;

static void *waiter_thread(void *arg)
{
    waiter_ctx_t *w = (waiter_ctx_t *)arg;
    __atomic_store_n(&w->entered, 1, __ATOMIC_RELEASE);
    /* A blocking wait is woken by ANY loop activity (returns MOQ_OK), not
     * only by terminal stop/fatal. A client connecting (even to a dead
     * port) generates such activity wakes, which can arrive before the
     * main thread's stop(). Drain those and keep waiting so we observe the
     * terminal wake; with UINT64_MAX there is no timeout, so the loop ends
     * only on MOQ_ERR_CLOSED (stop/fatal). */
    moq_result_t rc;
    do {
        rc = moq_pq_threaded_wait(w->t, UINT64_MAX);
    } while (rc == MOQ_OK);
    w->rc = rc;
    return NULL;
}

int main(void)
{
    test_alloc_state_t as = {0, 0, 0};
    moq_alloc_t alloc = { &as, test_alloc, test_realloc, test_free };

    /* ================================================================ */
    /* cfg_init                                                         */
    /* ================================================================ */

    {
        /* Pointer-only init clears and stamps ONLY the frozen prefix (the
         * layout before goaway_timeout_us was appended). The appended field is
         * OUTSIDE that prefix and must be left untouched -- writing the full
         * sizeof would overflow an old caller's smaller struct. */
        moq_pq_threaded_cfg_t cfg;
        memset(&cfg, 0xAB, sizeof(cfg));
        moq_pq_threaded_cfg_init(&cfg);
        CHECK(cfg.struct_size ==
              (uint32_t)offsetof(moq_pq_threaded_cfg_t, goaway_timeout_us));
        CHECK(cfg.alloc == NULL);       /* inside prefix: cleared */
        CHECK(cfg.on_pump == NULL);     /* inside prefix: cleared */
        /* Appended field (immediately after the prefix) NOT written. */
        CHECK(cfg.goaway_timeout_us == 0xABABABABABABABABULL);
        moq_pq_threaded_cfg_init(NULL);
        PASS("cfg_init");
    }

    {
        /* Old-prefix overflow canary with REAL old-sized storage: an old caller
         * allocated only the prefix that existed before goaway_timeout_us, then
         * its own bytes immediately after. The pointer-only init must touch only
         * that prefix -- a memset(sizeof current) would write the appended
         * region and clobber the byte right after the old struct. The
         * union's first member forces alignment for the cfg* cast; the cfg is
         * never accessed through the full struct type (storage is old-sized). */
        union {
            moq_pq_threaded_cfg_t aligner;   /* alignment only */
            struct {
                unsigned char prefix[
                    offsetof(moq_pq_threaded_cfg_t, goaway_timeout_us)];
                uint64_t canary;             /* the old caller's next bytes */
            } box;
        } u;
        memset(&u, 0xAB, sizeof(u));
        moq_pq_threaded_cfg_init((moq_pq_threaded_cfg_t *)&u.box);

        /* Read struct_size out of the prefix without touching the full type. */
        uint32_t ss;
        memcpy(&ss, u.box.prefix +
               offsetof(moq_pq_threaded_cfg_t, struct_size), sizeof(ss));
        CHECK(ss == (uint32_t)offsetof(moq_pq_threaded_cfg_t, goaway_timeout_us));
        CHECK(u.box.canary == 0xABABABABABABABABULL);  /* not overflowed */
        PASS("cfg_init_old_prefix_no_overflow");
    }

    {
        /* Full sized init clears and stamps the whole current struct, with the
         * appended field zero-initialized and enabled. */
        moq_pq_threaded_cfg_t cfg;
        memset(&cfg, 0xFF, sizeof(cfg));
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        CHECK(cfg.struct_size == sizeof(moq_pq_threaded_cfg_t));
        CHECK(cfg.alloc == NULL);
        CHECK(cfg.on_pump == NULL);
        CHECK(cfg.goaway_timeout_us == 0);  /* appended field zero-inits */
        moq_pq_threaded_cfg_init_sized(NULL, sizeof(cfg));
        PASS("cfg_init_sized");
    }

    /* ================================================================ */
    /* ABI: old struct_size predating goaway_timeout_us                 */
    /* ================================================================ */
    /* A caller built before the appended goaway_timeout_us field sets a
     * struct_size that ends before it. Create must remain accepted — the field
     * is read only behind CFG_HAS(cfg, goaway_timeout_us) in the adapter
     * source, so an older/smaller cfg is valid (the appended field defaults to
     * disabled). This asserts old-prefix acceptance; it does not (and cannot
     * here, without a live GOAWAY) observe the runtime timeout value. */
    {
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        cfg.struct_size =
            (uint32_t)offsetof(moq_pq_threaded_cfg_t, goaway_timeout_us);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);   /* old prefix accepted */
        if (rc == MOQ_OK) {
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("cfg_old_prefix_goaway_absent");
    }

    /* ================================================================ */
    /* NULL args                                                        */
    /* ================================================================ */

    {
        moq_pq_threaded_t *t = NULL;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init(&cfg);

        CHECK(moq_pq_threaded_create(NULL, &t) == MOQ_ERR_INVAL);
        CHECK(t == NULL);
        CHECK(moq_pq_threaded_create(&cfg, NULL) == MOQ_ERR_INVAL);
        CHECK(moq_pq_threaded_stop(NULL) == MOQ_ERR_INVAL);
        moq_pq_threaded_destroy(NULL);
        CHECK(moq_pq_threaded_wake(NULL) == MOQ_ERR_INVAL);
        CHECK(moq_pq_threaded_wait(NULL, 0) == MOQ_ERR_INVAL);
        CHECK(moq_pq_threaded_session(NULL) == NULL);
        CHECK(moq_pq_threaded_conn(NULL) == NULL);
        CHECK(moq_pq_threaded_is_fatal(NULL) == false);
        CHECK(moq_pq_threaded_fatal_code(NULL) == 0);
        PASS("null_args");
    }

    /* ================================================================ */
    /* Validation failures                                              */
    /* ================================================================ */

    {
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_t *t = NULL;

        moq_pq_threaded_cfg_init(&cfg);
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "localhost"; cfg.port = 4443;
        cfg.on_pump = dummy_pump;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        moq_alloc_t bad_alloc = { NULL, test_alloc, NULL, NULL };
        cfg.alloc = &bad_alloc;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        moq_pq_threaded_cfg_init(&cfg);
        cfg.alloc = &alloc; cfg.on_pump = dummy_pump;
        cfg.host = "localhost"; cfg.port = 4443;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_client_cfg(&cfg, &alloc);
        cfg.on_pump = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_client_cfg(&cfg, &alloc);
        cfg.host = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_client_cfg(&cfg, &alloc);
        cfg.host = "";
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_client_cfg(&cfg, &alloc);
        cfg.port = 0;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_client_cfg(&cfg, &alloc);
        cfg.port = 70000;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_server_cfg_stub(&cfg, &alloc);
        cfg.cert_path = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_server_cfg_stub(&cfg, &alloc);
        cfg.key_path = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        make_server_cfg_stub(&cfg, &alloc);
        cfg.cert_path = "";
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        cfg.struct_size = 1;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INVAL);

        CHECK(t == NULL);
        PASS("validation_failures");
    }

    /* ================================================================ */
    /* OOM on create                                                     */
    /* ================================================================ */

    {
        as = (test_alloc_state_t){0, 1, 0};
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_NOMEM);
        CHECK(t == NULL);
        CHECK(as.balance == 0);
        PASS("oom_create");
    }

    /* ================================================================ */
    /* Client create/stop/destroy with real network thread               */
    /* ================================================================ */

    {
        as = (test_alloc_state_t){0, 0, 0};
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            CHECK(moq_pq_threaded_session(t) != NULL);
            CHECK(moq_pq_threaded_conn(t) != NULL);
            CHECK(moq_pq_threaded_is_fatal(t) == false);
            CHECK(moq_pq_threaded_stop(t) == MOQ_OK);
            moq_pq_threaded_destroy(t);
        }
        PASS("client_network_thread_lifecycle");
    }

    /* ================================================================ */
    /* Client configure_quic success: called once                        */
    /* ================================================================ */

    {
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        int cq_called = 0;
        cfg.configure_quic = configure_quic_ok;
        cfg.configure_quic_ctx = &cq_called;
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        CHECK(cq_called == 1);
        if (rc == MOQ_OK) {
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("client_configure_quic_success");
    }

    /* ================================================================ */
    /* Client configure_quic failure: rollback                           */
    /* ================================================================ */

    {
        as = (test_alloc_state_t){0, 0, 0};
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.port = 14600 + (rand() % 400);
        cfg.configure_quic = configure_quic_fail;
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INTERNAL);
        CHECK(t == NULL);
        CHECK(as.balance == 0);
        PASS("client_configure_quic_failure_rollback");
    }

    /* ================================================================ */
    /* Cert verifier helper: API edge cases (no network)                */
    /* ================================================================ */

    {
        /* NULL quic → -1 (bad arg), no crash. */
        CHECK(moq_picoquic_set_cert_verifier(NULL, NULL) == -1);

        /* A bad CA path is rejected inside the helper (returns -1),
         * propagated as a configure_quic failure → create fails cleanly,
         * no crash, allocator balanced. */
        as = (test_alloc_state_t){0, 0, 0};
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.port = 14600 + (rand() % 400);
        cfg.configure_quic = configure_quic_bad_ca;
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INTERNAL);
        CHECK(t == NULL);
        CHECK(as.balance == 0);
        PASS("cert_verifier_api_edges");
    }

    /* ================================================================ */
    /* Client wake returns OK and on_pump runs                           */
    /* ================================================================ */

    {
        pump_counter_t pc = { 0, 0 };
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        cfg.on_pump = counting_pump;
        cfg.on_pump_ctx = &pc;
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            CHECK(moq_pq_threaded_wake(t) == MOQ_OK);
            for (int tries = 0; tries < 50 &&
                 __atomic_load_n(&pc.pump_count, __ATOMIC_ACQUIRE) == 0;
                 tries++)
                moq_pq_threaded_wait(t, 10000);
            CHECK(__atomic_load_n(&pc.pump_count, __ATOMIC_ACQUIRE) > 0);
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("wake_triggers_pump");
    }

    /* ================================================================ */
    /* Client wake coalescing                                            */
    /* ================================================================ */

    {
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            for (int i = 0; i < 20; i++)
                CHECK(moq_pq_threaded_wake(t) == MOQ_OK);
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("wake_coalescing");
    }

    /* ================================================================ */
    /* Client pump_exit: wait → CLOSED, is_fatal false                   */
    /* ================================================================ */

    {
        pump_counter_t pc = { 0, 3 };
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        cfg.on_pump = counting_pump;
        cfg.on_pump_ctx = &pc;
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            moq_result_t wr = MOQ_DONE;
            for (int tries = 0; tries < 100; tries++) {
                wr = moq_pq_threaded_wait(t, 50000);
                if (wr == MOQ_ERR_CLOSED) break;
            }
            CHECK(wr == MOQ_ERR_CLOSED);
            CHECK(moq_pq_threaded_is_fatal(t) == false);
            CHECK(moq_pq_threaded_stop(t) == MOQ_OK);
            moq_pq_threaded_destroy(t);
        }
        PASS("pump_exit_not_fatal");
    }

    /* ================================================================ */
    /* Client connect to a dead port → fatal (pre-setup disconnect)      */
    /* ================================================================ */
    /* No server listening: the QUIC handshake never completes and the
     * connection disconnects before the MoQ session reaches ESTABLISHED.
     * The loop's client-side disconnect guard latches is_fatal()==true,
     * fatal_code()==0, and wait() returns MOQ_ERR_CLOSED in bounded time
     * (1s handshake timeout via configure_quic). Mirrors pico WT managed
     * test_connect_failure.
     *
     * Candidate ports (outside the 14600-15000 / 15110-15190 server test
     * ranges) guard against an unlucky local listener: if a candidate
     * somehow reaches ESTABLISHED, skip it and try the next. The fatal
     * assertion is NOT weakened — we just need one dead candidate. */
    {
        const int ports[] = { 15310, 15350, 15390 };
        int proven = 0;
        for (size_t pi = 0;
             pi < sizeof(ports) / sizeof(ports[0]) && !proven; pi++) {
            moq_pq_threaded_cfg_t cfg;
            make_client_cfg(&cfg, &alloc);
            cfg.insecure_skip_verify = true;
            cfg.port = ports[pi];
            cfg.configure_quic = configure_quic_fast_timeout;
            moq_pq_threaded_t *t = NULL;
            if (moq_pq_threaded_create(&cfg, &t) != MOQ_OK || !t) continue;

            int fatal = 0, established = 0;
            for (int tries = 0; tries < 120 && !fatal && !established;
                 tries++) {
                moq_pq_threaded_wait(t, 50000);
                moq_session_t *s = moq_pq_threaded_session(t);
                if (s && moq_session_state(s) == MOQ_SESS_ESTABLISHED)
                    established = 1;            /* unlucky listener: skip */
                fatal = moq_pq_threaded_is_fatal(t);
            }
            if (fatal) {
                CHECK(moq_pq_threaded_fatal_code(t) == 0);
                CHECK(moq_pq_threaded_wait(t, 0) == MOQ_ERR_CLOSED);
                proven = 1;
            }
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        CHECK(proven);  /* a dead port produced the expected pre-setup fatal */
        PASS("connect_dead_port_fatal");
    }

    /* ================================================================ */
    /* Client blocking wait + stop from second thread                    */
    /* ================================================================ */

    {
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            waiter_ctx_t w = { t, MOQ_DONE, 0 };
            pthread_t th;
            pthread_create(&th, NULL, waiter_thread, &w);
            while (!__atomic_load_n(&w.entered, __ATOMIC_ACQUIRE))
                sched_yield();
            moq_pq_threaded_stop(t);
            pthread_join(th, NULL);
            CHECK(w.rc == MOQ_ERR_CLOSED);
            moq_pq_threaded_destroy(t);
        }
        PASS("wait_blocking_stop_wakes");
    }

    /* ================================================================ */
    /* Client stop from on_pump → WRONG_STATE                           */
    /* ================================================================ */

    {
        stop_from_pump_ctx_t sc = { NULL, (int)MOQ_OK, 0 };
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        cfg.on_pump = stop_from_pump;
        cfg.on_pump_ctx = &sc;
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            sc.t = t;
            for (int tries = 0; tries < 100; tries++) {
                moq_result_t wr = moq_pq_threaded_wait(t, 50000);
                if (wr == MOQ_ERR_CLOSED) break;
            }
            CHECK(__atomic_load_n(&sc.called, __ATOMIC_ACQUIRE));
            CHECK(__atomic_load_n(&sc.stop_rc, __ATOMIC_ACQUIRE) ==
                  (int)MOQ_ERR_WRONG_STATE);
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("stop_from_pump_wrong_state");
    }

    /* ================================================================ */
    /* Client concurrent wake/stop stress                                */
    /* ================================================================ */

    {
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            pthread_t waker;
            pthread_create(&waker, NULL, wake_stress_thread, t);
            sched_yield();
            moq_pq_threaded_stop(t);
            pthread_join(waker, NULL);
            moq_pq_threaded_destroy(t);
        }
        PASS("concurrent_wake_stop");
    }

    /* ================================================================ */
    /* Client wake after pump_exit returns CLOSED                        */
    /* ================================================================ */

    {
        pump_counter_t pc = { 0, 2 };
        moq_pq_threaded_cfg_t cfg;
        make_client_cfg(&cfg, &alloc);
        cfg.insecure_skip_verify = true;
        cfg.port = 14600 + (rand() % 400);
        cfg.on_pump = counting_pump;
        cfg.on_pump_ctx = &pc;
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            for (int tries = 0; tries < 100; tries++) {
                moq_result_t wr = moq_pq_threaded_wait(t, 50000);
                if (wr == MOQ_ERR_CLOSED) break;
            }
            CHECK(moq_pq_threaded_wake(t) == MOQ_ERR_CLOSED);
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("wake_after_pump_exit_closed");
    }

    /* ================================================================ */
    /* Server tests (require real cert/key paths)                        */
    /* ================================================================ */

#ifdef MOQ_TEST_CERT_PATH

    /* -- Server create/stop/destroy, no client connects --------------- */
    {
        moq_pq_threaded_cfg_t cfg;
        make_server_cfg_real(&cfg, &alloc);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            CHECK(moq_pq_threaded_session(t) == NULL);
            CHECK(moq_pq_threaded_conn(t) == NULL);
            CHECK(moq_pq_threaded_is_fatal(t) == false);
            CHECK(moq_pq_threaded_stop(t) == MOQ_OK);
            moq_pq_threaded_destroy(t);
        }
        PASS("server_no_client_lifecycle");
    }

    /* -- Server configure_quic success -------------------------------- */
    {
        moq_pq_threaded_cfg_t cfg;
        make_server_cfg_real(&cfg, &alloc);
        int cq_called = 0;
        cfg.configure_quic = configure_quic_ok;
        cfg.configure_quic_ctx = &cq_called;
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        CHECK(cq_called == 1);
        if (rc == MOQ_OK) {
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("server_configure_quic_success");
    }

    /* -- Server configure_quic failure rollback ------------------------ */
    {
        as = (test_alloc_state_t){0, 0, 0};
        moq_pq_threaded_cfg_t cfg;
        make_server_cfg_real(&cfg, &alloc);
        cfg.configure_quic = configure_quic_fail;
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&cfg, &t) == MOQ_ERR_INTERNAL);
        CHECK(t == NULL);
        CHECK(as.balance == 0);
        PASS("server_configure_quic_failure_rollback");
    }

    /* -- Server wake before connection: OK, clears wake_pending ------- */
    {
        moq_pq_threaded_cfg_t cfg;
        make_server_cfg_real(&cfg, &alloc);
        moq_pq_threaded_t *t = NULL;
        moq_result_t rc = moq_pq_threaded_create(&cfg, &t);
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            CHECK(moq_pq_threaded_wake(t) == MOQ_OK);
            CHECK(moq_pq_threaded_session(t) == NULL);
            moq_pq_threaded_stop(t);
            moq_pq_threaded_destroy(t);
        }
        PASS("server_wake_before_connection");
    }

    /* -- Threaded loopback: server + client ----------------------------- */
    {
        int server_port = 14600 + (rand() % 400);

        /* Start server. */
        pump_counter_t srv_pc = { 0, 0 };
        moq_pq_threaded_cfg_t srv_cfg;
        make_server_cfg_real(&srv_cfg, moq_alloc_default());
        srv_cfg.port = server_port;
        srv_cfg.send_request_capacity = true;
        srv_cfg.initial_request_capacity = 64;
        srv_cfg.on_pump = counting_pump;
        srv_cfg.on_pump_ctx = &srv_pc;
        moq_pq_threaded_t *srv = NULL;
        moq_result_t src = moq_pq_threaded_create(&srv_cfg, &srv);
        CHECK(src == MOQ_OK);

        if (src == MOQ_OK) {
            /* Start client connecting to server. */
            pump_counter_t cli_pc = { 0, 0 };
            moq_pq_threaded_cfg_t cli_cfg;
            moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
            cli_cfg.alloc = moq_alloc_default();
            cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
            cli_cfg.host = "localhost";
            cli_cfg.port = server_port;
            cli_cfg.insecure_skip_verify = true;
            cli_cfg.send_request_capacity = true;
            cli_cfg.initial_request_capacity = 64;
            cli_cfg.on_pump = counting_pump;
            cli_cfg.on_pump_ctx = &cli_pc;
            moq_pq_threaded_t *cli = NULL;
            moq_result_t crc = moq_pq_threaded_create(&cli_cfg, &cli);
            CHECK(crc == MOQ_OK);

            if (crc == MOQ_OK) {
                /* Wait until server has accepted a connection and
                 * both pumps have run at least once. */
                for (int tries = 0; tries < 200; tries++) {
                    moq_pq_threaded_wait(srv, 25000);
                    if (moq_pq_threaded_session(srv) != NULL &&
                        __atomic_load_n(&srv_pc.pump_count,
                            __ATOMIC_ACQUIRE) > 0 &&
                        __atomic_load_n(&cli_pc.pump_count,
                            __ATOMIC_ACQUIRE) > 0)
                        break;
                }

                CHECK(moq_pq_threaded_session(srv) != NULL);
                CHECK(moq_pq_threaded_conn(srv) != NULL);
                CHECK(__atomic_load_n(&srv_pc.pump_count,
                    __ATOMIC_ACQUIRE) > 0);
                CHECK(__atomic_load_n(&cli_pc.pump_count,
                    __ATOMIC_ACQUIRE) > 0);

                moq_pq_threaded_stop(cli);
                moq_pq_threaded_destroy(cli);
            }

            moq_pq_threaded_stop(srv);
            moq_pq_threaded_destroy(srv);
        }
        PASS("threaded_loopback_client_server");
    }

    /* -- Multi-connection server: two clients, one server --------------- */
    /* One threaded server accepts TWO client connections (conn_count == 2),
     * delivers an object to each, then one client disconnects and its
     * connection is pruned (conn_count == 1) while the other keeps its
     * delivery and the server stays non-fatal. Progress assertions only
     * (counts + received flags) over bounded waits -- no latency checks. */
    {
        int server_port = 15410 + (rand() % 300);
        mc_srv_t srv_state; memset(&srv_state, 0, sizeof(srv_state));
        moq_pq_threaded_cfg_t srv_cfg;
        make_server_cfg_real(&srv_cfg, moq_alloc_default());
        srv_cfg.port = server_port;
        srv_cfg.send_request_capacity = true;
        srv_cfg.initial_request_capacity = 64;
        srv_cfg.on_pump = mc_srv_pump;
        srv_cfg.on_pump_ctx = &srv_state;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&srv_cfg, &srv) == MOQ_OK && srv) {
            mc_cli_t sa = {0}, sb = {0};
            moq_pq_threaded_cfg_t ca, cb;
            make_client_cfg(&ca, moq_alloc_default());
            ca.port = server_port; ca.insecure_skip_verify = true;
            ca.send_request_capacity = true; ca.initial_request_capacity = 64;
            ca.on_pump = mc_cli_pump; ca.on_pump_ctx = &sa;
            cb = ca; cb.on_pump_ctx = &sb;

            moq_pq_threaded_t *a = NULL, *b = NULL;
            moq_result_t rca = moq_pq_threaded_create(&ca, &a);
            moq_result_t rcb = moq_pq_threaded_create(&cb, &b);
            CHECK(rca == MOQ_OK); CHECK(rcb == MOQ_OK);

            if (rca == MOQ_OK && rcb == MOQ_OK && a && b) {
                /* Both connected + both delivered their first object. Wake the
                 * server each iteration so its pump runs even when the clients
                 * are momentarily idle. */
                int ok = 0;
                for (int i = 0; i < 400 && !ok; i++) {
                    moq_pq_threaded_wake(srv);
                    moq_pq_threaded_wait(srv, 20000);
                    moq_pq_threaded_wait(a, 5000);
                    moq_pq_threaded_wait(b, 5000);
                    ok = moq_pq_threaded_conn_count(srv) == 2 &&
                         __atomic_load_n(&sa.received, __ATOMIC_SEQ_CST) >= 1 &&
                         __atomic_load_n(&sb.received, __ATOMIC_SEQ_CST) >= 1;
                }
                CHECK(moq_pq_threaded_conn_count(srv) == 2);
                CHECK(__atomic_load_n(&sa.received, __ATOMIC_SEQ_CST) >= 1);
                CHECK(__atomic_load_n(&sb.received, __ATOMIC_SEQ_CST) >= 1);
                CHECK(!moq_pq_threaded_is_fatal(srv));

                /* Arm the drop only after both received, so neither is closed
                 * before its first delivery. */
                __atomic_store_n(&srv_state.may_close, 1, __ATOMIC_RELEASE);

                /* The server closes the 2nd connection (moq_pq_threaded_conn_close
                 * inside on_pump, deferred prune) and then delivers a SECOND
                 * object to the survivor. Prove: conn_count drops to 1, the
                 * survivor receives its 2nd object, and the server stays alive. */
                int done = 0;
                for (int i = 0; i < 400 && !done; i++) {
                    moq_pq_threaded_wake(srv);
                    moq_pq_threaded_wait(srv, 20000);
                    moq_pq_threaded_wait(a, 5000);
                    moq_pq_threaded_wait(b, 5000);
                    int surv2 =
                        __atomic_load_n(&sa.received, __ATOMIC_SEQ_CST) >= 2 ||
                        __atomic_load_n(&sb.received, __ATOMIC_SEQ_CST) >= 2;
                    done = moq_pq_threaded_conn_count(srv) == 1 && surv2;
                }
                CHECK(moq_pq_threaded_conn_count(srv) == 1);
                CHECK(__atomic_load_n(&sa.received, __ATOMIC_SEQ_CST) >= 2 ||
                      __atomic_load_n(&sb.received, __ATOMIC_SEQ_CST) >= 2);
                CHECK(!moq_pq_threaded_is_fatal(srv));

                moq_pq_threaded_stop(a);
                moq_pq_threaded_destroy(a);
                moq_pq_threaded_stop(b);
                moq_pq_threaded_destroy(b);
                b = NULL;
            }
            if (b) { moq_pq_threaded_stop(b); moq_pq_threaded_destroy(b); }
            moq_pq_threaded_stop(srv);
            moq_pq_threaded_destroy(srv);
        }
        PASS("server_multi_connection");
    }

    /* -- Server cap above eight is real at the transport ----------------- */
    /* cfg.max_connections must reach picoquic_create(): picoquic fixes
     * max_nb_connections when the context is created, and
     * picoquic_adjust_max_connections can only lower it afterwards. If the
     * server context were created with a smaller hardcoded transport cap
     * (it used to be 8), connections past that cap would be refused by
     * picoquic before server_callback ever saw them, silently contradicting
     * the documented 1024 default. Prove the default cap is real past the
     * old limit: TEN concurrent clients all connect (conn_count == 10) and
     * none goes fatal. */
    {
        enum { MC_MANY = 10 };
        int server_port = 15720 + (rand() % 200);
        pump_counter_t srv_pc = { 0, 0 };
        moq_pq_threaded_cfg_t srv_cfg;
        make_server_cfg_real(&srv_cfg, moq_alloc_default());
        srv_cfg.port = server_port;
        srv_cfg.on_pump = counting_pump;
        srv_cfg.on_pump_ctx = &srv_pc;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&srv_cfg, &srv) == MOQ_OK && srv) {
            moq_pq_threaded_t *cli[MC_MANY] = { 0 };
            pump_counter_t cli_pc[MC_MANY];
            memset(cli_pc, 0, sizeof(cli_pc));
            int created = 1;
            for (int i = 0; i < MC_MANY && created; i++) {
                moq_pq_threaded_cfg_t c;
                make_client_cfg(&c, moq_alloc_default());
                c.port = server_port;
                c.insecure_skip_verify = true;
                c.on_pump = counting_pump;
                c.on_pump_ctx = &cli_pc[i];
                if (moq_pq_threaded_create(&c, &cli[i]) != MOQ_OK)
                    created = 0;
            }
            CHECK(created);

            if (created) {
                int ok = 0;
                for (int i = 0; i < 600 && !ok; i++) {
                    moq_pq_threaded_wake(srv);
                    moq_pq_threaded_wait(srv, 20000);
                    ok = moq_pq_threaded_conn_count(srv) == MC_MANY;
                }
                CHECK(moq_pq_threaded_conn_count(srv) == MC_MANY);
                CHECK(!moq_pq_threaded_is_fatal(srv));
                for (int i = 0; i < MC_MANY; i++)
                    CHECK(!moq_pq_threaded_is_fatal(cli[i]));
            }

            for (int i = 0; i < MC_MANY; i++) {
                if (!cli[i]) continue;
                moq_pq_threaded_stop(cli[i]);
                moq_pq_threaded_destroy(cli[i]);
            }
            moq_pq_threaded_stop(srv);
            moq_pq_threaded_destroy(srv);
        }
        PASS("server_cap_beyond_eight_connections");
    }

    /* -- Server rejects a second connection past max_connections=1 ------ */
    /* With the connection cap set to 1, the server accepts exactly one active
     * connection. Client A connects and is accepted: server session != NULL
     * and both pumps run — the proven-serving milestone.
     * Client B then connects to the SAME server while A is still active.
     * server_callback must REJECT B (return -1 → picoquic closes the cnx)
     * rather than returning success and leaving B open but unmanaged. B
     * observes the close before its MoQ session establishes, so it latches
     * is_fatal()==true and wait() reports MOQ_ERR_CLOSED; A's accepted
     * session is untouched (session(srv) stays non-NULL). Gated on A proving
     * the server served on this port to rule out a dead/busy-port false
     * positive.
     *
     * If the ALPN check accepted unconditionally, the server would accept B's
     * transport but never service it; B would neither establish nor go fatal
     * within the bounded window, so b_fatal would stay false. */
    {
        const int ports[] = { 15210, 15250, 15290 };
        int proven = 0;
        for (size_t pi = 0;
             pi < sizeof(ports) / sizeof(ports[0]) && !proven; pi++) {
            int server_port = ports[pi];
            pump_counter_t srv_pc = { 0, 0 };
            moq_pq_threaded_cfg_t srv_cfg;
            make_server_cfg_real(&srv_cfg, moq_alloc_default());
            srv_cfg.port = server_port;
            srv_cfg.max_connections = 1;   /* cap: only client A fits */
            srv_cfg.send_request_capacity = true;
            srv_cfg.initial_request_capacity = 64;
            srv_cfg.on_pump = counting_pump;
            srv_cfg.on_pump_ctx = &srv_pc;
            moq_pq_threaded_t *srv = NULL;
            if (moq_pq_threaded_create(&srv_cfg, &srv) != MOQ_OK) continue;

            /* Client A: insecure control → accepted; proves server serving. */
            int a_serving = 0;
            pump_counter_t a_pc = { 0, 0 };
            moq_pq_threaded_cfg_t a_cfg;
            make_client_cfg(&a_cfg, moq_alloc_default());
            a_cfg.port = server_port;
            a_cfg.insecure_skip_verify = true;
            a_cfg.send_request_capacity = true;
            a_cfg.initial_request_capacity = 64;
            a_cfg.on_pump = counting_pump;
            a_cfg.on_pump_ctx = &a_pc;
            moq_pq_threaded_t *a = NULL;
            if (moq_pq_threaded_create(&a_cfg, &a) == MOQ_OK && a) {
                for (int tries = 0; tries < 200 && !a_serving; tries++) {
                    moq_pq_threaded_wait(srv, 25000);
                    if (moq_pq_threaded_session(srv) != NULL &&
                        __atomic_load_n(&srv_pc.pump_count,
                            __ATOMIC_ACQUIRE) > 0 &&
                        __atomic_load_n(&a_pc.pump_count,
                            __ATOMIC_ACQUIRE) > 0)
                        a_serving = 1;
                }

                if (a_serving) {
                    /* Client B: a second connection to the SAME server while
                     * A is still up. Must be rejected → fatal. */
                    int b_fatal = 0, b_established = 0;
                    moq_result_t b_wait = MOQ_OK;
                    moq_pq_threaded_cfg_t b_cfg;
                    make_client_cfg(&b_cfg, moq_alloc_default());
                    b_cfg.port = server_port;
                    b_cfg.insecure_skip_verify = true;
                    moq_pq_threaded_t *b = NULL;
                    if (moq_pq_threaded_create(&b_cfg, &b) == MOQ_OK && b) {
                        for (int tries = 0; tries < 120; tries++) {
                            moq_pq_threaded_wait(b, 50000);
                            moq_session_t *s = moq_pq_threaded_session(b);
                            if (s && moq_session_state(s) ==
                                     MOQ_SESS_ESTABLISHED)
                                b_established = 1;
                            if (moq_pq_threaded_is_fatal(b)) {
                                b_fatal = 1;
                                break;
                            }
                            if (b_established) break;
                        }
                        b_wait = moq_pq_threaded_wait(b, 0);
                        moq_pq_threaded_stop(b);
                        moq_pq_threaded_destroy(b);
                    }

                    CHECK(moq_pq_threaded_session(srv) != NULL); /* A intact */
                    CHECK(b_fatal);            /* second connection rejected */
                    CHECK(!b_established);      /* B never established */
                    CHECK(b_wait == MOQ_ERR_CLOSED);
                    proven = 1;
                }

                moq_pq_threaded_stop(a);
                moq_pq_threaded_destroy(a);
            }

            moq_pq_threaded_stop(srv);
            moq_pq_threaded_destroy(srv);
        }
        CHECK(proven);  /* rejected B against a proven-serving peer */
        PASS("server_cap_rejects_second_connection");
    }

    /* -- Cert verifier helper rejects the self-signed server ----------- */
    /* moq_picoquic_set_cert_verifier(quic, NULL) installs the system-trust
     * verifier via configure_quic. Against the self-signed test server the
     * client must FAIL CLOSED as FATAL: the handshake is rejected before the
     * MoQ session reaches ESTABLISHED, so the loop's client-side disconnect
     * guard latches is_fatal()==true with fatal_code()==0 (transport-level)
     * and wait() returns MOQ_ERR_CLOSED — matching pico WT managed.
     *
     * Self-contained, no dead-server false positive: the rejected client
     * fails at TLS before the server reaches almost_ready/ready, so it
     * never claims the one-connection server. An insecure CONTROL client
     * then completes the handshake on the SAME server/port — the server
     * accepts the connection (session != NULL) and both pumps run, the
     * same proven-serving signal the loopback test uses — isolating the
     * verifier as the cause of client A's failure. */
    {
        const int ports[] = { 15110, 15150, 15190 };
        int proven = 0;
        for (size_t pi = 0;
             pi < sizeof(ports) / sizeof(ports[0]) && !proven; pi++) {
            int server_port = ports[pi];
            pump_counter_t srv_pc = { 0, 0 };
            moq_pq_threaded_cfg_t srv_cfg;
            make_server_cfg_real(&srv_cfg, moq_alloc_default());
            srv_cfg.port = server_port;
            srv_cfg.send_request_capacity = true;
            srv_cfg.initial_request_capacity = 64;
            srv_cfg.on_pump = counting_pump;
            srv_cfg.on_pump_ctx = &srv_pc;
            moq_pq_threaded_t *srv = NULL;
            if (moq_pq_threaded_create(&srv_cfg, &srv) != MOQ_OK) continue;

            /* Client A: helper (system trust) → handshake rejected → fatal. */
            int a_established = 0, a_fatal = 0;
            uint64_t a_fatal_code = 0;
            moq_result_t a_wait = MOQ_OK;
            moq_pq_threaded_cfg_t a_cfg;
            make_client_cfg(&a_cfg, moq_alloc_default());
            a_cfg.port = server_port;
            a_cfg.insecure_skip_verify = false;       /* verify the cert */
            a_cfg.configure_quic = configure_quic_verify;
            moq_pq_threaded_t *a = NULL;
            if (moq_pq_threaded_create(&a_cfg, &a) == MOQ_OK && a) {
                for (int tries = 0; tries < 120; tries++) {
                    moq_pq_threaded_wait(a, 50000);
                    moq_session_t *s = moq_pq_threaded_session(a);
                    if (s && moq_session_state(s) == MOQ_SESS_ESTABLISHED)
                        a_established = 1;
                    if (moq_pq_threaded_is_fatal(a)) { a_fatal = 1; break; }
                    if (a_established) break;
                }
                a_fatal_code = moq_pq_threaded_fatal_code(a);
                /* Once fatal latches, wait() reports it terminal. */
                a_wait = moq_pq_threaded_wait(a, 0);
                moq_pq_threaded_stop(a);
                moq_pq_threaded_destroy(a);
            }

            /* Client B: insecure control → the server must accept it on the
             * SAME port (session != NULL, both pumps run), proving it was
             * serving there.  Mirrors threaded_loopback_client_server. */
            int b_reachable = 0;
            pump_counter_t cli_pc = { 0, 0 };
            moq_pq_threaded_cfg_t b_cfg;
            make_client_cfg(&b_cfg, moq_alloc_default());
            b_cfg.port = server_port;
            b_cfg.insecure_skip_verify = true;
            b_cfg.send_request_capacity = true;
            b_cfg.initial_request_capacity = 64;
            b_cfg.on_pump = counting_pump;
            b_cfg.on_pump_ctx = &cli_pc;
            moq_pq_threaded_t *b = NULL;
            if (moq_pq_threaded_create(&b_cfg, &b) == MOQ_OK && b) {
                for (int tries = 0; tries < 200 && !b_reachable; tries++) {
                    moq_pq_threaded_wait(srv, 25000);
                    if (moq_pq_threaded_session(srv) != NULL &&
                        __atomic_load_n(&srv_pc.pump_count,
                            __ATOMIC_ACQUIRE) > 0 &&
                        __atomic_load_n(&cli_pc.pump_count,
                            __ATOMIC_ACQUIRE) > 0)
                        b_reachable = 1;
                }
                moq_pq_threaded_stop(b);
                moq_pq_threaded_destroy(b);
            }

            moq_pq_threaded_stop(srv);
            moq_pq_threaded_destroy(srv);

            /* Only assert once the control proves the server served here
             * (rules out a dead/unbound/busy-port false positive). */
            if (b_reachable) {
                CHECK(!a_established);                 /* never established */
                CHECK(a_fatal);                        /* failed closed: fatal */
                CHECK(a_fatal_code == 0);              /* transport-level */
                CHECK(a_wait == MOQ_ERR_CLOSED);       /* terminal via wait() */
                proven = 1;
            }
        }
        CHECK(proven);  /* verifier rejected against a proven-serving peer */
        PASS("cert_verifier_rejects_self_signed");
    }

    /* -- Facade smoke: publisher live-writes → subscriber receives ------ */
    {
        int server_port = 14600 + (rand() % 400);

        smoke_srv_t srv_state = { NULL, NULL, 0 };
        moq_pq_threaded_cfg_t srv_cfg;
        make_server_cfg_real(&srv_cfg, moq_alloc_default());
        srv_cfg.port = server_port;
        srv_cfg.send_request_capacity = true;
        srv_cfg.initial_request_capacity = 64;
        srv_cfg.on_pump = smoke_srv_pump;
        srv_cfg.on_pump_ctx = &srv_state;
        moq_pq_threaded_t *srv = NULL;
        moq_result_t src = moq_pq_threaded_create(&srv_cfg, &srv);
        CHECK(src == MOQ_OK);

        if (src == MOQ_OK) {
            smoke_cli_t cli_state = {0};
            moq_pq_threaded_cfg_t cli_cfg;
            moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
            cli_cfg.alloc = moq_alloc_default();
            cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
            cli_cfg.host = "localhost";
            cli_cfg.port = server_port;
            cli_cfg.insecure_skip_verify = true;
            cli_cfg.send_request_capacity = true;
            cli_cfg.initial_request_capacity = 64;
            cli_cfg.on_pump = smoke_cli_pump;
            cli_cfg.on_pump_ctx = &cli_state;
            moq_pq_threaded_t *cli = NULL;
            moq_result_t crc = moq_pq_threaded_create(&cli_cfg, &cli);
            CHECK(crc == MOQ_OK);

            if (crc == MOQ_OK) {
                /* Drive both sides until client receives the object. */
                for (int tries = 0; tries < 400; tries++) {
                    moq_pq_threaded_wake(srv);
                    moq_pq_threaded_wake(cli);
                    moq_pq_threaded_wait(cli, 25000);
                    if (__atomic_load_n(&cli_state.received,
                            __ATOMIC_ACQUIRE))
                        break;
                }

                CHECK(__atomic_load_n(&srv_state.obj_written,
                    __ATOMIC_ACQUIRE));
                CHECK(__atomic_load_n(&cli_state.received,
                    __ATOMIC_ACQUIRE));
                CHECK(__atomic_load_n(&cli_state.payload_ok,
                    __ATOMIC_ACQUIRE));

                /* Finish the subscriber (Track Ended, 0x2) over real QUIC and
                 * verify it fires on_subscribe_done with the status WITHOUT
                 * closing the session. */
                __atomic_store_n(&srv_state.do_finish, 1, __ATOMIC_RELEASE);
                for (int tries = 0; tries < 400; tries++) {
                    moq_pq_threaded_wake(srv);
                    moq_pq_threaded_wake(cli);
                    moq_pq_threaded_wait(cli, 25000);
                    if (__atomic_load_n(&cli_state.done, __ATOMIC_ACQUIRE) ||
                        __atomic_load_n(&cli_state.session_closed,
                            __ATOMIC_ACQUIRE))
                        break;
                }
                CHECK(__atomic_load_n(&srv_state.finished, __ATOMIC_ACQUIRE));
                CHECK(__atomic_load_n(&cli_state.done, __ATOMIC_ACQUIRE) == 1);
                CHECK(__atomic_load_n(&cli_state.done_status,
                    __ATOMIC_ACQUIRE) == MOQ_PUB_DONE_TRACK_ENDED);
                CHECK(!__atomic_load_n(&cli_state.session_closed,
                    __ATOMIC_ACQUIRE));

                moq_pq_threaded_stop(cli);
                moq_sub_destroy(cli_state.sub);
                moq_pq_threaded_destroy(cli);
            }

            moq_pq_threaded_stop(srv);
            moq_pub_destroy(srv_state.pub);
            moq_pq_threaded_destroy(srv);
        }
        PASS("facade_smoke_object");
    }

#else
    printf("SKIP: server tests (MOQ_TEST_CERT_PATH not defined)\n");
#endif

    /* ================================================================ */
    /* Summary                                                          */
    /* ================================================================ */

    if (failures == 0) {
        printf("PASS: all threaded tests\n");
        return 0;
    }
    printf("FAIL: %d threaded test failures\n", failures);
    return 1;
}
