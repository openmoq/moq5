/*
 * The endpoint's negotiated version at its FIRST settled ESTABLISHED pump,
 * over a real loopback connection: raw QUIC to a threaded-picoquic server, and
 * WebTransport to a managed pico_wt server.
 *
 * A managed facade starts its network thread inside its create call, so the
 * thread can pump an ESTABLISHED session before the endpoint has
 * published the facade it just created. The test holds the connecting thread
 * at exactly that point -- facade created, not yet published -- until the
 * network thread has reported its first ESTABLISHED cycle, then lets connect()
 * finish. Two kinds of evidence are kept apart:
 *
 *   private: the tuple {state, endpoint version, session version, facade
 *            published} copied under ep->mu at that first cycle, handed to the
 *            fixture through its own mutex; the observer never calls into the
 *            endpoint and the endpoint handle does not exist yet;
 *   public:  after connect() returns, the settled state/version through the
 *            public getters and snapshot, and the version read back after stop.
 *
 * The invariant pinned: ESTABLISHED is never reported with a zero version --
 * the endpoint's version at that first cycle equals the version the session
 * speaks, which is also what the server negotiated (the ALPN for raw QUIC,
 * the WT-Protocol token for WebTransport).
 */
#include <moq/endpoint.h>
#include <moq/picoquic_threaded.h>
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
#include <moq/pico_wt_managed.h>
#endif
#include <moq/session.h>
#include "test_support.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>

static int failures = 0;

/* Test-build seams in endpoint.c (moq-service-endpoint-test-internals). */
moq_result_t moq_endpoint_test_arm_next_connect(
    void (*after_create)(void *),
    void (*first_established)(void *, moq_endpoint_state_t, moq_version_t,
                              moq_version_t, bool),
    void *ctx);
bool moq_endpoint_test_connect_armed(void);
void moq_endpoint_test_disarm_next_connect(void);

/* Bound on how long the connecting thread is held for the first ESTABLISHED
 * cycle. Expiry releases it and is reported as a failed geometry, never as a
 * pass. */
#define BARRIER_BUDGET_S 15

/* -- fixture -------------------------------------------------------------- */

/* The observation itself: plain values, copied out under the fixture lock. */
typedef struct {
    /* written by the network thread (observer) */
    bool                 have;
    unsigned             established_calls;
    moq_endpoint_state_t state;
    moq_version_t        ep_version;
    moq_version_t        session_version;
    bool                 facade_published;
    /* written by the connecting thread (barrier) */
    bool barrier_ran;
    bool barrier_expired;
} observation_t;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    observation_t   obs;
} fixture_t;

static void fixture_init(fixture_t *f)
{
    memset(f, 0, sizeof(*f));
    pthread_mutex_init(&f->mu, NULL);
    pthread_cond_init(&f->cv, NULL);
}

static void fixture_destroy(fixture_t *f)
{
    pthread_cond_destroy(&f->cv);
    pthread_mutex_destroy(&f->mu);
}

/* Network thread, under ep->mu: copy the values, signal, return. */
static void on_first_established(void *ctx, moq_endpoint_state_t st,
                                 moq_version_t ep_version,
                                 moq_version_t session_version,
                                 bool facade_published)
{
    fixture_t *f = (fixture_t *)ctx;
    pthread_mutex_lock(&f->mu);
    f->obs.established_calls++;
    f->obs.state = st;
    f->obs.ep_version = ep_version;
    f->obs.session_version = session_version;
    f->obs.facade_published = facade_published;
    f->obs.have = true;
    pthread_cond_broadcast(&f->cv);
    pthread_mutex_unlock(&f->mu);
}

/* Connecting thread, facade created and unpublished: hold until the first
 * ESTABLISHED cycle has been recorded, or the budget expires. */
static void hold_after_create(void *ctx)
{
    fixture_t *f = (fixture_t *)ctx;
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += BARRIER_BUDGET_S;
    pthread_mutex_lock(&f->mu);
    f->obs.barrier_ran = true;
    while (!f->obs.have) {
        int rc = pthread_cond_timedwait(&f->cv, &f->mu, &deadline);
        if (rc == ETIMEDOUT) { f->obs.barrier_expired = true; break; }
    }
    pthread_mutex_unlock(&f->mu);
}

/* -- server ----------------------------------------------------------------- */

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now_us, void *ctx)
{
    (void)t; (void)lane; (void)now_us; (void)ctx;
    return 0;
}

static moq_pq_threaded_t *start_pq_server(const char *cert, const char *key,
                                       const char *const *alpn_list,
                                       size_t alpn_count, int *out_port)
{
    /* The v0 server accepts one connection, so each case starts its own; the
     * base shifts per call so cases never collide on a port. */
    static int calls = 0;
    int base = 15400 + (int)(getpid() % 997) + (calls++ * 131);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 13;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_lane_pump = server_pump;
        cfg.alpn_list = alpn_list;
        cfg.alpn_count = alpn_count;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return NULL;
}

static bool wait_for_state(moq_endpoint_t *ep, moq_endpoint_state_t want,
                           int budget_ms)
{
    int waited = 0;
    while (waited < budget_ms) {
        if (moq_endpoint_state(ep) == want) return true;
        moq_result_t rc = moq_endpoint_wait(ep, 100000);
        if (rc == MOQ_ERR_CLOSED && want != MOQ_ENDPOINT_CLOSED) return false;
        waited += 100;
    }
    return moq_endpoint_state(ep) == want;
}

/* -- one observed connect ----------------------------------------------------- */

/* The server's own view of the negotiated version, read after connect(). */
typedef moq_version_t (*server_version_fn)(void *srv);

static void observe_connect(const char *name, const char *url,
                            moq_version_t expect,
                            server_version_fn server_version, void *srv)
{
    printf("== %s: %s, expect draft-%d ==\n", name, url, (int)(expect & 0xff));
    fixture_t f;
    fixture_init(&f);

    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);
    c.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    c.insecure_skip_verify = true;      /* self-signed loopback cert; AUTO offer */

    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_arm_next_connect(
                              hold_after_create, on_first_established, &f),
                          (int)MOQ_OK);
    moq_endpoint_t *ep = NULL;
    moq_result_t crc = moq_endpoint_connect(&c, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)crc, (int)MOQ_OK);
    /* The arming is consumed by the allocation; never left for a later
     * connect in this process. A connect() refused before allocation leaves
     * it armed with &f, so it is cleared here, before f can go away. */
    if (moq_endpoint_test_connect_armed()) {
        fprintf(stderr, "FAIL[%s.arming-consumed]: the connect arming was left in place\n", name);
        failures++;
        moq_endpoint_test_disarm_next_connect();
    }

    /* ---- private first-pump evidence ---- */
    pthread_mutex_lock(&f.mu);
    observation_t snap = f.obs;
    pthread_mutex_unlock(&f.mu);
    printf("first-established tuple: state=%d endpoint_version=0x%x "
           "session_version=0x%x facade_published=%d calls=%u "
           "barrier_ran=%d barrier_expired=%d\n",
           (int)snap.state, (unsigned)snap.ep_version,
           (unsigned)snap.session_version, (int)snap.facade_published,
           snap.established_calls, (int)snap.barrier_ran,
           (int)snap.barrier_expired);

    bool geometry = snap.barrier_ran && !snap.barrier_expired && snap.have &&
                    snap.established_calls == 1 && !snap.facade_published &&
                    snap.state == MOQ_ENDPOINT_ESTABLISHED;
    if (!geometry) {
        fprintf(stderr, "FAIL[%s.geometry]: the first ESTABLISHED cycle was not "
                "observed inside the unpublished-facade window (barrier_ran=%d "
                "expired=%d have=%d calls=%u facade_published=%d state=%d)\n",
                name, (int)snap.barrier_ran, (int)snap.barrier_expired,
                (int)snap.have, snap.established_calls,
                (int)snap.facade_published, (int)snap.state);
        failures++;
    } else {
        printf("PASS[%s.geometry]: first ESTABLISHED pumped on the network thread "
               "while the facade was created but unpublished\n", name);
    }
    if (snap.have && snap.session_version != expect) {
        fprintf(stderr, "FAIL[%s.session-version]: the ESTABLISHED session speaks "
                "0x%x, not the negotiated 0x%x\n", name,
                (unsigned)snap.session_version, (unsigned)expect);
        failures++;
    }
    if (geometry) {
        if (snap.ep_version == snap.session_version) {
            printf("PASS[%s.first-established-version]: endpoint version 0x%x "
                   "equals the session version\n", name, (unsigned)snap.ep_version);
        } else {
            fprintf(stderr, "FAIL[%s.first-established-version]: ESTABLISHED with "
                    "endpoint version 0x%x while the session speaks 0x%x\n",
                    name, (unsigned)snap.ep_version,
                    (unsigned)snap.session_version);
            failures++;
        }
    }

    /* ---- public post-connect evidence ---- */
    if (ep) {
        MOQ_TEST_CHECK(wait_for_state(ep, MOQ_ENDPOINT_ESTABLISHED, 15000));
        moq_version_t pv = moq_endpoint_negotiated_version(ep);
        moq_endpoint_snapshot_t sn;
        memset(&sn, 0, sizeof(sn));
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_get_snapshot(ep, &sn, sizeof(sn)),
                              (int)MOQ_OK);
        printf("public: negotiated_version=0x%x snapshot={state=%d version=0x%x} "
               "server_version=0x%x\n", (unsigned)pv, (int)sn.state,
               (unsigned)sn.negotiated_version,
               (unsigned)server_version(srv));
        MOQ_TEST_CHECK_EQ_INT((int)pv, (int)expect);
        MOQ_TEST_CHECK_EQ_INT((int)sn.state, (int)MOQ_ENDPOINT_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sn.negotiated_version, (int)expect);
        MOQ_TEST_CHECK_EQ_INT((int)server_version(srv), (int)expect);

        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
        moq_version_t cv = moq_endpoint_negotiated_version(ep);
        printf("post-close: negotiated_version=0x%x\n", (unsigned)cv);
        MOQ_TEST_CHECK_EQ_INT((int)cv, (int)expect);
        moq_endpoint_destroy(ep);
    }
    /* Nothing can still reach f: the barrier ran synchronously inside
     * connect(); the observer lives only on the endpoint, whose network thread
     * stop()/destroy() have joined; a connect() that failed after allocation
     * created no facade thread; and an unconsumed arming was cleared above. */
    fixture_destroy(&f);
}

/* -- raw QUIC: threaded-picoquic server ----------------------------------------- */

static moq_version_t pq_server_version(void *srv)
{
    return moq_pq_threaded_negotiated_version((moq_pq_threaded_t *)srv);
}

static void run_raw_quic(const char *name, const char *cert, const char *key,
                         const char *const *server_alpn, size_t server_alpn_count,
                         moq_version_t expect)
{
    int port = 0;
    moq_pq_threaded_t *srv = start_pq_server(cert, key, server_alpn,
                                             server_alpn_count, &port);
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) return;
    char url[64];
    snprintf(url, sizeof(url), "moqt://127.0.0.1:%d", port);
    observe_connect(name, url, expect, pq_server_version, srv);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
}

#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
/* -- WebTransport: managed pico_wt server ----------------------------------------- */

static int wt_server_pump(moq_pico_wt_managed_t *m, uint64_t now_us, void *ctx)
{
    (void)m; (void)now_us; (void)ctx;
    return 0;
}

static moq_version_t wt_server_version(void *srv)
{
    return moq_pico_wt_managed_negotiated_version((moq_pico_wt_managed_t *)srv);
}

/* The managed server binds on its own network thread, so a busy port shows up
 * as a fatal server rather than a create failure; such a port is skipped. A
 * wide per-process spread keeps collisions rare. */
static moq_pico_wt_managed_t *start_wt_server(const char *cert, const char *key,
                                              const char *wt_protocols,
                                              int *out_port)
{
    static unsigned calls = 0;
    unsigned base = 20000u +
        (unsigned)(((uintptr_t)getpid() * 2654435761u + calls++ * 7919u) % 30000u);
    for (unsigned attempt = 0; attempt < 4; attempt++) {
        int port = (int)(base + attempt * 311u);
        moq_pico_wt_managed_cfg_t cfg;
        moq_pico_wt_managed_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_pump = wt_server_pump;
        cfg.wt_protocols = wt_protocols;
        moq_pico_wt_managed_t *srv = NULL;
        if (moq_pico_wt_managed_create(&cfg, &srv) != MOQ_OK) continue;
        for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);
        if (!moq_pico_wt_managed_is_fatal(srv)) {
            *out_port = moq_pico_wt_managed_local_port(srv);
            return srv;
        }
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
    }
    return NULL;
}

static void run_webtransport(const char *name, const char *cert, const char *key,
                             const char *wt_protocols, moq_version_t expect)
{
    int port = 0;
    moq_pico_wt_managed_t *srv = start_wt_server(cert, key, wt_protocols, &port);
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) return;
    char url[64];
    snprintf(url, sizeof(url), "https://127.0.0.1:%d/moq", port);
    observe_connect(name, url, expect, wt_server_version, srv);
    moq_pico_wt_managed_stop(srv);
    moq_pico_wt_managed_destroy(srv);
}
#endif

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 1;
    }

    /* A draft-18-capable server: the AUTO offer {18, 16} settles on 18. */
    static const char *const both_alpn[] = { "moqt-18", "moqt-16" };
    run_raw_quic("d18", cert, key, both_alpn, 2, MOQ_VERSION_DRAFT_18);
    /* The server's default offer: the same client settles on 16. */
    run_raw_quic("d16", cert, key, NULL, 0, MOQ_VERSION_DRAFT_16);
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
    /* The same two outcomes selected by the WT-Protocol token instead. */
    run_webtransport("wt-d18", cert, key, "moqt-18, moqt-16", MOQ_VERSION_DRAFT_18);
    run_webtransport("wt-d16", cert, key, "moqt-16", MOQ_VERSION_DRAFT_16);
#else
    printf("SKIP[webtransport]: no managed pico_wt facade in this build\n");
#endif

    if (moq_endpoint_test_connect_armed()) {
        fprintf(stderr, "FAIL[residue]: a connect arming outlived the test\n");
        failures++;
    }
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("endpoint version publication: 0 failures\n");
    return 0;
}
