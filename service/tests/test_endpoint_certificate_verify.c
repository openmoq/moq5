/* Service propagation and raw-QUIC lane retirement of the certificate trust
 * callback. Private test renames record installation and select the existing
 * eager-session facade path. This is lane/forwarder lifetime coverage, not a
 * TLS-wire certificate exchange. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/picoquic_verify.h>

#include <picoquic.h>

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
#include <moq/picoquic_threaded.h>

static bool retirement_mode;

moq_result_t moq_test_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                        moq_pq_threaded_t **out)
{
    moq_pq_threaded_cfg_t copy = *cfg;
    if (retirement_mode) {
        /* The normal ALPN-offer route has no session until TLS completes.
         * Select the existing eager draft-16 session solely so post() runs
         * on the real network lane without requiring a responding peer. */
        copy.alpn_list = NULL;
        copy.alpn_count = 0;
    }
    return moq_pq_threaded_create(&copy, out);
}
#endif

static int failures;

#define DEAD_PEER_URL_QUIC "moqt://127.0.0.1:14433"
#define DEAD_PEER_URL_WT   "https://127.0.0.1:14434/moq"

static unsigned setter_calls;
static moq_result_t setter_result;
static moq_picoquic_cert_verify_fn installed_verify;
static void *installed_ctx;

moq_result_t moq_test_set_cert_verify_callback(
    picoquic_quic_t *quic,
    moq_picoquic_cert_verify_fn verify,
    void *ctx)
{
    setter_calls++;
    MOQ_TEST_CHECK(quic != NULL);
    installed_verify = verify;
    installed_ctx = ctx;
    return setter_result;
}

typedef struct app_probe {
    unsigned calls;
    moq_tls_certificate_verdict_t verdict;
    void *identity;
} app_probe_t;

static moq_tls_certificate_verdict_t app_verify(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    app_probe_t *p = (app_probe_t *)ctx;
    static const uint8_t name[] = "relay.example";
    static const uint8_t leaf[] = {0x30, 0x03, 0x01};
    static const uint8_t issuer[] = {0x30, 0x03, 0x02};
    p->calls++;
    MOQ_TEST_CHECK(p->identity == p);
    MOQ_TEST_CHECK_EQ_U64(server_name.len, sizeof(name) - 1);
    MOQ_TEST_CHECK(server_name.data != NULL &&
                   memcmp(server_name.data, name, sizeof(name) - 1) == 0);
    MOQ_TEST_CHECK_EQ_U64(certificate_count, 2);
    MOQ_TEST_CHECK(certificates != NULL);
    if (certificates != NULL && certificate_count == 2) {
        MOQ_TEST_CHECK_EQ_U64(certificates[0].len, sizeof(leaf));
        MOQ_TEST_CHECK(certificates[0].data != NULL &&
                       memcmp(certificates[0].data, leaf, sizeof(leaf)) == 0);
        MOQ_TEST_CHECK_EQ_U64(certificates[1].len, sizeof(issuer));
        MOQ_TEST_CHECK(certificates[1].data != NULL &&
                       memcmp(certificates[1].data, issuer,
                              sizeof(issuer)) == 0);
    }
    return p->verdict;
}

static moq_bytes_t B(const char *s)
{
    return (moq_bytes_t){(const uint8_t *)s, strlen(s)};
}

static void reset_setter(void)
{
    setter_calls = 0;
    setter_result = MOQ_OK;
    installed_verify = NULL;
    installed_ctx = NULL;
}

static void run_protocol(const char *url, moq_transport_protocol_t protocol)
{
    app_probe_t probe = {
        .calls = 0,
        .verdict = MOQ_TLS_CERTIFICATE_TRUST,
        .identity = NULL
    };
    probe.identity = &probe;
    reset_setter();

    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.url = B(url);
    cfg.protocol = protocol;
    cfg.verify_certificate = app_verify;
    cfg.verify_certificate_ctx = &probe;

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&cfg, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (rc != MOQ_OK || ep == NULL) {
        /* connect() owns and releases the partially constructed endpoint on
         * failure. The setter recorder may therefore hold a stale context;
         * never turn an endpoint-startup failure into a test-side UAF. */
        installed_verify = NULL;
        installed_ctx = NULL;
        return;
    }
    MOQ_TEST_CHECK_EQ_U64(setter_calls, 1);
    MOQ_TEST_CHECK(installed_verify != NULL);
    MOQ_TEST_CHECK(installed_ctx == ep);

    if (installed_verify != NULL) {
        static const uint8_t name[] = "relay.example";
        static const uint8_t leaf[] = {0x30, 0x03, 0x01};
        static const uint8_t issuer[] = {0x30, 0x03, 0x02};
        const moq_bytes_t chain[] = {
            {leaf, sizeof(leaf)},
            {issuer, sizeof(issuer)}
        };
        moq_picoquic_cert_verdict_t verdict = installed_verify(
            installed_ctx, (moq_bytes_t){name, sizeof(name) - 1},
            chain, sizeof(chain) / sizeof(chain[0]));
        MOQ_TEST_CHECK_EQ_INT((int)verdict,
                              (int)MOQ_PICOQUIC_CERT_TRUST);
        MOQ_TEST_CHECK_EQ_U64(probe.calls, 1);

        probe.verdict = MOQ_TLS_CERTIFICATE_REJECT;
        verdict = installed_verify(
            installed_ctx, (moq_bytes_t){name, sizeof(name) - 1},
            chain, sizeof(chain) / sizeof(chain[0]));
        MOQ_TEST_CHECK_EQ_INT((int)verdict,
                              (int)MOQ_PICOQUIC_CERT_REJECT);
        MOQ_TEST_CHECK_EQ_U64(probe.calls, 2);

        probe.verdict = (moq_tls_certificate_verdict_t)99;
        verdict = installed_verify(
            installed_ctx, (moq_bytes_t){name, sizeof(name) - 1},
            chain, sizeof(chain) / sizeof(chain[0]));
        MOQ_TEST_CHECK_EQ_INT((int)verdict,
                              (int)MOQ_PICOQUIC_CERT_REJECT);
        MOQ_TEST_CHECK_EQ_U64(probe.calls, 3);
    }

    if (ep != NULL) {
        moq_result_t stop_rc = moq_endpoint_stop(ep);
        MOQ_TEST_CHECK_EQ_INT((int)stop_rc, (int)MOQ_OK);
        if (stop_rc == MOQ_OK)
            moq_endpoint_destroy(ep);
    }

    /* A provider/setter failure cannot fall back to the default verifier or
     * return an endpoint whose requested policy was not installed. */
    const moq_result_t errors[] = {
        MOQ_ERR_INVAL, MOQ_ERR_UNSUPPORTED, MOQ_ERR_NOMEM,
        MOQ_ERR_WRONG_STATE, MOQ_ERR_INTERNAL
    };
    for (size_t i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
        reset_setter();
        setter_result = errors[i];
        ep = NULL;
        rc = moq_endpoint_connect(&cfg, &ep);
        /* configure_quic is the existing 0/-1 hook; facade creation reports
         * its own failure, not the adapter setter's detailed result. */
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_INTERNAL);
        MOQ_TEST_CHECK(ep == NULL);
        MOQ_TEST_CHECK_EQ_U64(setter_calls, 1);
    }
}

#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
typedef struct retirement_probe {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_endpoint_t *ep;
    moq_picoquic_cert_verify_fn verify;
    void *verify_ctx;
    pthread_t caller_thread;
    pthread_t callback_thread;
    bool entered;
    bool release;
    bool task_done;
    bool callback_returned;
    bool stop_started;
    bool stop_on_other_thread;
    bool stop_done;
    bool stop_saw_return;
    bool timed_out;
    bool alive;
    bool callback_saw_alive;
    bool callback_on_lane;
    bool watchdog_done;
    unsigned calls;
    unsigned order;
    unsigned callback_order;
    unsigned stop_order;
    moq_result_t stop_result;
    moq_picoquic_cert_verdict_t forwarded_verdict;
} retirement_probe_t;

static struct timespec deadline_after(time_t seconds)
{
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        fprintf(stderr, "FAIL: retirement clock_gettime\n");
        _Exit(EXIT_FAILURE);
    }
    deadline.tv_sec += seconds;
    return deadline;
}

/* All waits use conditions for correctness; deadlines only fail a stalled
 * test. A broken stop/join cannot safely be cancelled or followed by freeing
 * live context, so the watchdog bounds that failure by ending the process. */
static void *retirement_watchdog(void *ctx)
{
    retirement_probe_t *p = ctx;
    struct timespec deadline = deadline_after(15);
    pthread_mutex_lock(&p->mu);
    while (!p->watchdog_done) {
        int rc = pthread_cond_timedwait(&p->cv, &p->mu, &deadline);
        if (rc != 0 && !p->watchdog_done) {
            fprintf(stderr, "FAIL: retirement cleanup deadline (%d)\n", rc);
            _Exit(EXIT_FAILURE);
        }
    }
    pthread_mutex_unlock(&p->mu);
    return NULL;
}

static bool retirement_wait(retirement_probe_t *p, const bool *flag)
{
    struct timespec deadline = deadline_after(5);
    while (!*flag) {
        int rc = pthread_cond_timedwait(&p->cv, &p->mu, &deadline);
        if (rc != 0 && !*flag) return false;
    }
    return true;
}

static moq_tls_certificate_verdict_t retirement_verify(
    void *ctx, moq_bytes_t name, const moq_bytes_t *chain, size_t count)
{
    retirement_probe_t *p = ctx;
    (void)name; (void)chain; (void)count;
    pthread_mutex_lock(&p->mu);
    p->calls++;
    p->callback_thread = pthread_self();
    p->callback_on_lane = !pthread_equal(p->callback_thread, p->caller_thread);
    p->callback_saw_alive = p->alive;
    p->entered = true;
    pthread_cond_broadcast(&p->cv);
    if (!retirement_wait(p, &p->release)) p->timed_out = true;
    p->callback_saw_alive = p->callback_saw_alive && p->alive;
    pthread_mutex_unlock(&p->mu);
    return MOQ_TLS_CERTIFICATE_TRUST;
}

static moq_result_t retirement_task(moq_endpoint_t *ep, moq_session_t *session,
                                    uint64_t now_us, void *ctx)
{
    retirement_probe_t *p = ctx;
    (void)ep; (void)now_us;
    moq_picoquic_cert_verdict_t verdict = MOQ_PICOQUIC_CERT_REJECT;
    if (session != NULL) {
        /* Deliberately block this test task to model an in-flight trust
         * callback. Ordinary endpoint_post tasks must never block. */
        verdict = p->verify(p->verify_ctx, B("relay.example"), NULL, 0);
    }
    pthread_mutex_lock(&p->mu);
    p->forwarded_verdict = verdict;
    p->callback_returned = session != NULL;
    p->callback_order = ++p->order;
    p->task_done = true;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    return MOQ_OK;
}

static void *retirement_stop(void *ctx)
{
    retirement_probe_t *p = ctx;
    pthread_mutex_lock(&p->mu);
    p->stop_on_other_thread = !pthread_equal(pthread_self(), p->caller_thread) &&
                              !pthread_equal(pthread_self(), p->callback_thread);
    p->stop_started = true;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    moq_result_t rc = moq_endpoint_stop(p->ep);
    pthread_mutex_lock(&p->mu);
    p->stop_result = rc;
    p->stop_saw_return = p->callback_returned;
    p->stop_order = ++p->order;
    p->stop_done = true;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    return NULL;
}

static void test_retirement(void)
{
    retirement_probe_t *p = calloc(1, sizeof(*p));
    MOQ_TEST_CHECK(p != NULL);
    if (p == NULL) return;
    if (pthread_mutex_init(&p->mu, NULL) != 0) {
        MOQ_TEST_CHECK(false);
        free(p);
        return;
    }
    if (pthread_cond_init(&p->cv, NULL) != 0) {
        MOQ_TEST_CHECK(false);
        pthread_mutex_destroy(&p->mu);
        free(p);
        return;
    }
    p->caller_thread = pthread_self();
    p->alive = true;
    pthread_t watchdog, stopper;
    bool stopper_started = false;
    if (pthread_create(&watchdog, NULL, retirement_watchdog, p) != 0) {
        MOQ_TEST_CHECK(false);
        pthread_cond_destroy(&p->cv);
        pthread_mutex_destroy(&p->mu);
        free(p);
        return;
    }

    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.url = B(DEAD_PEER_URL_QUIC);
    cfg.verify_certificate = retirement_verify;
    cfg.verify_certificate_ctx = p;
    reset_setter();
    retirement_mode = true;
    moq_result_t rc = moq_endpoint_connect(&cfg, &p->ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK || p->ep == NULL) goto cleanup;
    p->verify = installed_verify;
    p->verify_ctx = installed_ctx;
    MOQ_TEST_CHECK(p->verify != NULL && p->verify_ctx == p->ep);
    if (p->verify == NULL || p->verify_ctx != p->ep) goto cleanup;
    rc = moq_endpoint_post(p->ep, retirement_task, p);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;

    pthread_mutex_lock(&p->mu);
    bool entered = retirement_wait(p, &p->entered);
    pthread_mutex_unlock(&p->mu);
    MOQ_TEST_CHECK(entered);
    if (!entered) goto cleanup;
    int thread_rc = pthread_create(&stopper, NULL, retirement_stop, p);
    MOQ_TEST_CHECK_EQ_INT(thread_rc, 0);
    if (thread_rc != 0) goto cleanup;
    stopper_started = true;
    pthread_mutex_lock(&p->mu);
    bool started = retirement_wait(p, &p->stop_started);
    MOQ_TEST_CHECK(started);
    pthread_mutex_unlock(&p->mu);
    /* The facade publishes stopped and signals its waiters BEFORE joining.
     * Observe that receipt while the callback remains at the barrier: merely
     * observing stop_started would allow release before stop was called. */
    do {
        rc = moq_endpoint_wait(p->ep, 5000000);
    } while (rc == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_CLOSED);
    pthread_mutex_lock(&p->mu);
    MOQ_TEST_CHECK(!p->callback_returned && !p->timed_out);
    MOQ_TEST_CHECK(!p->stop_done);
    p->release = true;
    pthread_cond_broadcast(&p->cv);
    bool done = retirement_wait(p, &p->stop_done);
    MOQ_TEST_CHECK(done);
    pthread_mutex_unlock(&p->mu);

cleanup:
    pthread_mutex_lock(&p->mu);
    p->release = true;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    if (stopper_started) {
        MOQ_TEST_CHECK_EQ_INT(pthread_join(stopper, NULL), 0);
        pthread_mutex_lock(&p->mu);
        MOQ_TEST_CHECK_EQ_INT((int)p->stop_result, (int)MOQ_OK);
        MOQ_TEST_CHECK(p->stop_saw_return && p->task_done);
        if (!p->stop_saw_return || !p->task_done) {
            /* A premature stop receipt makes endpoint destruction unsafe.
             * Do not turn that regression into test-owned use-after-free. */
            fprintf(stderr, "FAIL: stop returned before callback retirement\n");
            _Exit(EXIT_FAILURE);
        }
        MOQ_TEST_CHECK(p->callback_order < p->stop_order);
        MOQ_TEST_CHECK(p->callback_on_lane && p->stop_on_other_thread);
        MOQ_TEST_CHECK_EQ_U64(p->calls, 1);
        MOQ_TEST_CHECK(p->callback_saw_alive && !p->timed_out);
        MOQ_TEST_CHECK_EQ_INT((int)p->forwarded_verdict,
                              (int)MOQ_PICOQUIC_CERT_TRUST);
        pthread_mutex_unlock(&p->mu);
    }
    if (p->ep != NULL) {
        /* Also joins on failed setup/post/thread-start paths. The watchdog
         * stays armed through destroy, and ctx is not released before it. */
        rc = moq_endpoint_stop(p->ep);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (rc != MOQ_OK) {
            fprintf(stderr, "FAIL: retirement cannot safely destroy endpoint\n");
            _Exit(EXIT_FAILURE);
        }
        unsigned calls_after_stop = p->calls;
        MOQ_TEST_CHECK(p->alive);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_post(p->ep, retirement_task, p),
                              (int)MOQ_ERR_CLOSED);
        moq_endpoint_destroy(p->ep);
        MOQ_TEST_CHECK(p->alive && p->calls == calls_after_stop);
        p->ep = NULL;
    }
    installed_verify = NULL;
    installed_ctx = NULL;
    retirement_mode = false;
    pthread_mutex_lock(&p->mu);
    p->alive = false;
    p->watchdog_done = true;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    MOQ_TEST_CHECK_EQ_INT(pthread_join(watchdog, NULL), 0);
    pthread_cond_destroy(&p->cv);
    pthread_mutex_destroy(&p->mu);
    free(p);
}
#endif

int main(void)
{
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
    run_protocol(DEAD_PEER_URL_QUIC, MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
    test_retirement();
#endif
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
    run_protocol(DEAD_PEER_URL_WT, MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
#endif
#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
#error "test_endpoint_certificate_verify requires a picoquic service facade"
#endif
    MOQ_TEST_PASS("endpoint_certificate_verify");
    return failures != 0;
}
