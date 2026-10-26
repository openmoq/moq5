/*
 * moq_endpoint_drain succeeds only on a facade observation taken after the
 * hooks' owed output was handed to the session.
 *
 * The facade stand-in keeps a latched observation, as the picoquic backends
 * do: a pass publishes it, drain_invalidate clears it. A pump cycle that newly
 * finds every hook quiet for a drain request invalidates it and only then
 * publishes the request as quiet; the drain reads the quiet request first and
 * probes afterwards. The rows hold one side at the boundary it must respect:
 *   - consumer order: the drain read an old DONE, then the producer publishes
 *     the matching quiet: that DONE never counts;
 *   - producer order: the producer is held inside the invalidation, before it
 *     publishes the quiet, while a drain already past its closed check (so
 *     not blocked on the endpoint lock the producer holds) reads the quiet
 *     request and probes the old DONE: it does not complete;
 * then a later pass's DONE completes the drain. Also: only a newly satisfied
 * request invalidates, an earlier request keeps the progress a later one
 * (whose ticket is observed taken) does not have yet, a hook that stays busy
 * holds the drain to its deadline, and the interrupt latch wins. "Pending"
 * is shown by probes that actually advance, never by a wait that timed out.
 * No sleeps carry an ordering: every wait is a gate or a bounded wait on an
 * observed count.
 */
#include "test_support.h"
#include "endpoint_internal.h"
#include <moq/endpoint.h>
#include <moq/session.h>
#include <moq/types.h>

#include <pthread.h>
#include <stdatomic.h>
#include <string.h>

moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_endpoint_test_pump_cycle(moq_endpoint_t *ep, moq_session_t *session,
                                  uint64_t now_us);
void moq_endpoint_test_set_drain_backend(moq_endpoint_t *ep, int state);
void moq_endpoint_test_drain_backend_publish(int state);
void moq_endpoint_test_drain_backend_gate_invalidate(bool on);
void moq_endpoint_test_drain_backend_gate_checked(bool on);
uint64_t moq_endpoint_test_drain_req(moq_endpoint_t *ep);
void moq_endpoint_test_drain_backend_gate_probe_from(uint64_t n);
bool moq_endpoint_test_drain_backend_wait_parked(int which, uint64_t timeout_us);
void moq_endpoint_test_drain_backend_release(int which);
uint64_t moq_endpoint_test_drain_backend_wait_probes(uint64_t after, uint64_t timeout_us);
uint64_t moq_endpoint_test_drain_backend_invalidations(void);
uint64_t moq_endpoint_test_drain_quiet(moq_endpoint_t *ep);

enum { PENDING = 0, DONE = 1, GATE_PROBE = 0, GATE_INVALIDATE = 1, GATE_CHECKED = 2 };
#define BOUND_US 2000000u

static int failures = 0;

/* A hook whose only content is whether it still owes output. */
static atomic_bool g_owed;
static void quiet_pump(moq_endpoint_t *ep, moq_session_t *s, uint64_t now, void *ctx)
{ (void)ep; (void)s; (void)now; (void)ctx; }
static bool owed(void *ctx) { (void)ctx; return atomic_load(&g_owed); }

typedef struct {
    moq_endpoint_t *ep;
    uint64_t        timeout_us;
    pthread_t       th;
    atomic_bool     done;
    moq_result_t    rc;
} drain_call_t;

static void *drain_main(void *arg)
{
    drain_call_t *d = (drain_call_t *)arg;
    d->rc = moq_endpoint_drain(d->ep, d->timeout_us);
    atomic_store(&d->done, true);
    return NULL;
}
static bool drain_start(drain_call_t *d, moq_endpoint_t *ep, uint64_t timeout_us)
{
    memset(d, 0, sizeof(*d));
    d->ep = ep;
    d->timeout_us = timeout_us;
    atomic_init(&d->done, false);
    bool started = pthread_create(&d->th, NULL, drain_main, d) == 0;
    MOQ_TEST_CHECK(started);
    return started;
}
/* Every gate released first, so a row that failed while a side was held
 * still unwinds: the drains themselves are bounded by their timeouts. */
static void release_all(void)
{
    moq_endpoint_test_drain_backend_release(GATE_PROBE);
    moq_endpoint_test_drain_backend_release(GATE_INVALIDATE);
    moq_endpoint_test_drain_backend_release(GATE_CHECKED);
}
static moq_result_t drain_join(drain_call_t *d)
{
    pthread_join(d->th, NULL);
    return d->rc;
}

static uint64_t probes_now(void)
{
    return moq_endpoint_test_drain_backend_wait_probes(0, 0);
}
/* The probe count advances past `after` within the bound; the new count. */
static uint64_t advances(uint64_t after)
{
    uint64_t n = moq_endpoint_test_drain_backend_wait_probes(after, BOUND_US);
    MOQ_TEST_CHECK(n > after);
    return n;
}

/* The endpoint has issued `n` drain tickets (bounded). */
static bool wait_tickets(moq_endpoint_t *ep, uint64_t n)
{
    for (int i = 0; i < 2000 && moq_endpoint_test_drain_req(ep) < n; i++)
        (void)moq_endpoint_test_drain_backend_wait_probes(probes_now(), 1000);
    bool ok = moq_endpoint_test_drain_req(ep) == n;
    MOQ_TEST_CHECK(ok);
    return ok;
}

/* The drain keeps probing without success: two more probes actually pass
 * and it has not returned. */
static bool keeps_pending(drain_call_t *d)
{
    uint64_t n0 = probes_now();
    uint64_t n1 = advances(n0);
    uint64_t n2 = advances(n1);
    return n2 > n1 && n1 > n0 && !atomic_load(&d->done);
}

typedef struct {
    moq_endpoint_t *ep;
    moq_session_t  *s;
} cycle_t;
static void *cycle_main(void *arg)
{
    cycle_t *c = (cycle_t *)arg;
    moq_endpoint_test_pump_cycle(c->ep, c->s, 1000);
    return NULL;
}

static moq_endpoint_t *make(moq_session_t *s, int state)
{
    (void)s;
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    MOQ_TEST_CHECK(ep != NULL);
    moq_endpoint_test_set_drain_backend(ep, state);
    moq_endpoint_hook_ops_t ops;
    memset(&ops, 0, sizeof(ops));
    ops.pump = quiet_pump;
    ops.has_pending_output = owed;
    MOQ_TEST_CHECK(moq_endpoint_attach_hook(ep, MOQ_ENDPOINT_HOOK_SENDER, &ops, NULL) == MOQ_OK);
    return ep;
}
static void unmake(moq_endpoint_t *ep)
{
    moq_endpoint_detach_hook(ep, MOQ_ENDPOINT_HOOK_SENDER, NULL);
    moq_endpoint_test_free_bare(ep);
}

/* The consumer read an old DONE; the producer then hands the owed output to
 * the session and publishes the matching quiet. That DONE does not count. */
static void consumer_old_done(moq_session_t *s)
{
    atomic_store(&g_owed, true);
    moq_endpoint_t *ep = make(s, DONE);
    moq_endpoint_test_drain_backend_gate_probe_from(2);   /* its first loop probe */
    drain_call_t d;
    if (!drain_start(&d, ep, 10 * BOUND_US)) { unmake(ep); return; }
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_wait_parked(GATE_PROBE, BOUND_US));
    MOQ_TEST_CHECK_EQ_U64(moq_endpoint_test_drain_req(ep), 1);
    atomic_store(&g_owed, false);
    moq_endpoint_test_pump_cycle(ep, s, 1000);       /* invalidate, then quiet */
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_invalidations() == 1);
    MOQ_TEST_CHECK_EQ_U64(moq_endpoint_test_drain_quiet(ep), 1);
    moq_endpoint_test_drain_backend_release(GATE_PROBE);
    MOQ_TEST_CHECK(keeps_pending(&d));
    moq_endpoint_test_drain_backend_publish(DONE);   /* a later pass */
    release_all();
    MOQ_TEST_CHECK_EQ_INT((int)drain_join(&d), (int)MOQ_OK);
    unmake(ep);
}

/* The producer is held inside the invalidation, before publishing the
 * quiet, with the endpoint lock held. A drain already past its closed check
 * (held there, so it is not waiting on that lock) reads the quiet request
 * and probes the old DONE: it does not complete. */
static void producer_boundary(moq_session_t *s)
{
    atomic_store(&g_owed, true);
    moq_endpoint_t *ep = make(s, DONE);
    drain_call_t d;
    if (!drain_start(&d, ep, 10 * BOUND_US)) { unmake(ep); return; }
    if (!wait_tickets(ep, 1)) goto out;
    MOQ_TEST_CHECK(keeps_pending(&d));               /* owed output: no quiet */
    moq_endpoint_test_drain_backend_gate_checked(true);
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_wait_parked(GATE_CHECKED, BOUND_US));
    moq_endpoint_test_drain_backend_gate_invalidate(true);
    atomic_store(&g_owed, false);
    cycle_t c = { ep, s };
    pthread_t net;
    bool net_started = pthread_create(&net, NULL, cycle_main, &c) == 0;
    MOQ_TEST_CHECK(net_started);
    if (!net_started) goto out;
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_wait_parked(GATE_INVALIDATE, BOUND_US));
    MOQ_TEST_CHECK_EQ_U64(moq_endpoint_test_drain_quiet(ep), 0);
    /* The held drain now reads the quiet request and probes the old DONE. */
    uint64_t n = probes_now();
    moq_endpoint_test_drain_backend_release(GATE_CHECKED);
    advances(n);
    MOQ_TEST_CHECK(!atomic_load(&d.done));
    moq_endpoint_test_drain_backend_release(GATE_INVALIDATE);
    pthread_join(net, NULL);
    MOQ_TEST_CHECK_EQ_U64(moq_endpoint_test_drain_quiet(ep), 1);
    MOQ_TEST_CHECK(keeps_pending(&d));               /* invalidated: PENDING */
    moq_endpoint_test_drain_backend_publish(DONE);
out:
    release_all();
    MOQ_TEST_CHECK_EQ_INT((int)drain_join(&d), (int)MOQ_OK);
    unmake(ep);
}

/* Only a newly satisfied request invalidates; a quiet cycle for a request
 * already satisfied leaves the observation alone, so it can complete. */
static void invalidates_once_per_request(moq_session_t *s)
{
    atomic_store(&g_owed, false);
    moq_endpoint_t *ep = make(s, PENDING);
    drain_call_t d;
    if (!drain_start(&d, ep, 10 * BOUND_US)) { unmake(ep); return; }
    wait_tickets(ep, 1);
    for (int i = 0; i < 4; i++) moq_endpoint_test_pump_cycle(ep, s, 1000);
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_invalidations() == 1);
    moq_endpoint_test_drain_backend_publish(DONE);
    MOQ_TEST_CHECK_EQ_INT((int)drain_join(&d), (int)MOQ_OK);
    for (int i = 0; i < 3; i++) moq_endpoint_test_pump_cycle(ep, s, 1000);
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_invalidations() == 1);
    unmake(ep);
}

/* An earlier request satisfied before a later one is made completes on that
 * progress; the later one waits for a cycle of its own. */
static void earlier_request_keeps_progress(moq_session_t *s)
{
    atomic_store(&g_owed, false);
    moq_endpoint_t *ep = make(s, PENDING);
    drain_call_t a, b;
    if (!drain_start(&a, ep, 10 * BOUND_US)) { unmake(ep); return; }
    wait_tickets(ep, 1);
    moq_endpoint_test_pump_cycle(ep, s, 1000);       /* quiet for request 1 */
    MOQ_TEST_CHECK_EQ_U64(moq_endpoint_test_drain_quiet(ep), 1);
    if (!drain_start(&b, ep, 10 * BOUND_US)) {       /* request 2 */
        moq_endpoint_test_drain_backend_publish(DONE);
        (void)drain_join(&a);
        unmake(ep);
        return;
    }
    wait_tickets(ep, 2);                             /* b holds ticket 2 */
    moq_endpoint_test_drain_backend_publish(DONE);
    MOQ_TEST_CHECK_EQ_INT((int)drain_join(&a), (int)MOQ_OK);
    MOQ_TEST_CHECK(keeps_pending(&b));
    moq_endpoint_test_pump_cycle(ep, s, 1000);       /* quiet for request 2 */
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_invalidations() == 2);
    MOQ_TEST_CHECK(keeps_pending(&b));
    moq_endpoint_test_drain_backend_publish(DONE);
    release_all();
    MOQ_TEST_CHECK_EQ_INT((int)drain_join(&b), (int)MOQ_OK);
    unmake(ep);
}

/* A hook that still owes output holds the drain to its deadline, even with
 * the facade reporting DONE; the interrupt latch wins over everything. */
static void owed_output_and_interrupt(moq_session_t *s)
{
    atomic_store(&g_owed, true);
    moq_endpoint_t *ep = make(s, DONE);
    drain_call_t d;
    if (!drain_start(&d, ep, 200000)) { unmake(ep); return; }
    for (int i = 0; i < 3; i++) moq_endpoint_test_pump_cycle(ep, s, 1000);
    MOQ_TEST_CHECK_EQ_INT((int)drain_join(&d), (int)MOQ_DONE);
    MOQ_TEST_CHECK(moq_endpoint_test_drain_backend_invalidations() == 0);
    moq_endpoint_set_interrupted(ep, true);
    atomic_store(&g_owed, false);
    moq_endpoint_test_pump_cycle(ep, s, 1000);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(ep, BOUND_US), (int)MOQ_ERR_INTERRUPTED);
    moq_endpoint_set_interrupted(ep, false);
    unmake(ep);
}

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    atomic_init(&g_owed, false);

    consumer_old_done(s);
    producer_boundary(s);
    invalidates_once_per_request(s);
    earlier_request_keeps_progress(s);
    owed_output_and_interrupt(s);

    moq_session_destroy(s);
    if (failures) {
        fprintf(stderr, "FAIL: %d failures\n", failures);
        return 1;
    }
    printf("PASS: endpoint drain barrier\n");
    return 0;
}
