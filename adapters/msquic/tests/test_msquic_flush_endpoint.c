/* Result translation and both terminal/loss checks in the actual endpoint TU. The
 * managed query is typed and scripted here; its real behavior has a separate
 * executor/provider test. No network backend is created by this test. */
#include "../../../service/src/endpoint.c"
#include <stdio.h>
#include <stdlib.h>

static moq_result_t results[3];
static unsigned probes, wakes, waits, quiet_from, failures;
static const moq_msquic_managed_t *expected_facade;

static _Noreturn void unexpected_managed_call(const char *name)
{
    fprintf(stderr, "unexpected managed call: %s\n", name);
    abort();
}

/* Keep the included endpoint TU link-complete even when instrumentation
 * retains its backend registry. Only poll_flush is part of this script. */
void moq_msquic_managed_cfg_init_sized(moq_msquic_managed_cfg_t *cfg,
                                      size_t size)
{
    (void)cfg; (void)size;
    unexpected_managed_call(__func__);
}

moq_result_t moq_msquic_managed_create(const moq_msquic_managed_cfg_t *cfg,
                                      moq_msquic_managed_t **out)
{
    (void)cfg; (void)out;
    unexpected_managed_call(__func__);
}

moq_result_t moq_msquic_managed_stop(moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

void moq_msquic_managed_destroy(moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

moq_result_t moq_msquic_managed_wait(moq_msquic_managed_t *m,
                                    uint64_t timeout_us)
{
    (void)m; (void)timeout_us;
    unexpected_managed_call(__func__);
}

moq_result_t moq_msquic_managed_wake(moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

moq_version_t moq_msquic_managed_negotiated_version(
    const moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

bool moq_msquic_managed_is_fatal(const moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

uint64_t moq_msquic_managed_fatal_code(const moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

bool moq_msquic_managed_is_closed(const moq_msquic_managed_t *m)
{
    (void)m;
    unexpected_managed_call(__func__);
}

moq_msquic_managed_conn_t *moq_msquic_lane_next_conn(
    moq_msquic_managed_lane_t *lane, moq_msquic_managed_conn_t *prev)
{
    (void)lane; (void)prev;
    unexpected_managed_call(__func__);
}

moq_session_t *moq_msquic_managed_conn_session(moq_msquic_managed_conn_t *conn)
{
    (void)conn;
    unexpected_managed_call(__func__);
}

/* Probe N answers results[N], the last entry repeating. */
moq_result_t moq_msquic_managed_poll_flush(const moq_msquic_managed_t *m)
{
    if (m != expected_facade)
        unexpected_managed_call("poll_flush: wrong facade");
    unsigned slot = probes++;
    return results[slot < 2 ? slot : 2];
}

static bool not_terminal(void *f) { (void)f; return false; }

/* A wake runs one managed pump cycle. From wake `quiet_from` on (0: never)
 * every hook is quiet, and the cycle publishes the request it began under,
 * as ep_pump_cycle does. */
static void fake_wake(void *f)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)f;
    ++wakes;
    if (quiet_from != 0 && wakes >= quiet_from)
        atomic_store(&ep->drain_quiet, atomic_load(&ep->drain_req));
}

static moq_result_t fake_wait(void *f, uint64_t timeout_us)
{
    (void)f;
    (void)timeout_us;
    ++waits;
    return MOQ_OK;
}

static void script(moq_result_t a, moq_result_t b, moq_result_t c, unsigned quiet)
{
    results[0] = a;
    results[1] = b;
    results[2] = c;
    quiet_from = quiet;
    probes = wakes = waits = 0;
}

#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #x); ++failures; } } while (0)

int main(void)
{
    static const ep_backend_vtable_t vt = {
        .wait = fake_wait, .wake = fake_wake,
        .is_fatal = not_terminal, .clean_closed = not_terminal,
        .drain_state = msquic_vt_drain_state,
    };
    moq_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    pthread_mutex_init(&ep.mu, NULL);
    atomic_init(&ep.interrupted, false);
    atomic_init(&ep.drain_req, 0);
    atomic_init(&ep.drain_quiet, 0);
    ep.pump_state = MOQ_ENDPOINT_ESTABLISHED;
    ep.vt = &vt;
    ep.facade = &ep;
    expected_facade = (const moq_msquic_managed_t *)ep.facade;
    const struct { moq_result_t input; int output; } mapping[] = {
        {MOQ_OK, EP_DRAIN_DONE}, {MOQ_DONE, EP_DRAIN_PENDING},
        {MOQ_ERR_WRONG_STATE, EP_DRAIN_WRONG_THREAD},
        {MOQ_ERR_UNSUPPORTED, EP_DRAIN_UNSUPPORTED},
        {MOQ_ERR_INTERNAL, EP_DRAIN_FAILED},
        {MOQ_ERR_CLOSED, EP_DRAIN_CLOSED}, {MOQ_ERR_INVAL, EP_DRAIN_PENDING},
    };
    for (size_t i = 0; i < sizeof(mapping) / sizeof(mapping[0]); ++i) {
        script(mapping[i].input, mapping[i].input, mapping[i].input, 0);
        CHECK(msquic_vt_drain_state(ep.facade) == mapping[i].output);
        CHECK(probes == 1 && wakes == 0 && waits == 0);
    }

    /* Healthy live connection: the drain's own wake runs a quiet cycle, and
     * the flush probe after it succeeds. */
    script(MOQ_OK, MOQ_OK, MOQ_OK, 1);
    CHECK(moq_endpoint_drain(&ep, 0) == MOQ_OK);
    CHECK(probes == 2 && wakes == 1 && waits == 0);
    CHECK(atomic_load(&ep.drain_quiet) == atomic_load(&ep.drain_req));

    /* Output still pending in the transport. */
    script(MOQ_DONE, MOQ_DONE, MOQ_DONE, 1);
    CHECK(moq_endpoint_drain(&ep, 0) == MOQ_DONE);
    CHECK(probes == 2 && wakes == 1 && waits == 0);

    /* A flushed transport is not success while a hook still owes output: no
     * cycle begun after this request was quiet, and the earlier request's
     * quiet does not count for this one. */
    script(MOQ_OK, MOQ_OK, MOQ_OK, 0);
    CHECK(moq_endpoint_drain(&ep, 0) == MOQ_DONE);
    CHECK(probes == 2 && wakes == 1 && waits == 0);
    CHECK(atomic_load(&ep.drain_quiet) < atomic_load(&ep.drain_req));

    /* Hooks go quiet on the drain's re-kick: the drain waits one slice, then
     * a fresh probe after the quiet cycle succeeds. */
    script(MOQ_OK, MOQ_OK, MOQ_OK, 2);
    CHECK(moq_endpoint_drain(&ep, 1000000u) == MOQ_OK);
    CHECK(probes == 3 && wakes == 2 && waits == 1);

    const struct { moq_result_t first, second; unsigned calls; } loss[] = {
        {MOQ_ERR_INTERNAL, MOQ_OK, 1},
        {MOQ_DONE, MOQ_ERR_INTERNAL, 2},
        {MOQ_OK, MOQ_ERR_INTERNAL, 2},
    };
    for (size_t i = 0; i < sizeof(loss) / sizeof(loss[0]); ++i) {
        script(loss[i].first, loss[i].second, loss[i].second, 1);
        CHECK(moq_endpoint_drain(&ep, 0) == MOQ_ERR_INTERNAL);
        CHECK(probes == loss[i].calls);
        CHECK(wakes == (loss[i].calls == 1 ? 0u : 1u));
        CHECK(waits == 0);
        CHECK(moq_endpoint_state(&ep) == MOQ_ENDPOINT_ESTABLISHED);
        CHECK(!ep.stopped && !moq_endpoint_is_closed(&ep));
        CHECK(!moq_endpoint_is_fatal(&ep));
    }
    script(MOQ_ERR_CLOSED, MOQ_OK, MOQ_OK, 1);
    CHECK(moq_endpoint_drain(&ep, 0) == MOQ_ERR_CLOSED);
    CHECK(probes == 1 && wakes == 0 && waits == 0);
    script(MOQ_OK, MOQ_ERR_CLOSED, MOQ_ERR_CLOSED, 1);
    CHECK(moq_endpoint_drain(&ep, 0) == MOQ_ERR_CLOSED);
    CHECK(probes == 2 && wakes == 1 && waits == 0);
    atomic_store(&ep.interrupted, true);
    script(MOQ_OK, MOQ_OK, MOQ_OK, 1);
    CHECK(moq_endpoint_drain(&ep, 0) == MOQ_ERR_INTERRUPTED);
    CHECK(probes == 0 && wakes == 0 && waits == 0);
    pthread_mutex_destroy(&ep.mu);
    printf("msquic endpoint mapping: %u failures\n", failures);
    return failures ? 1 : 0;
}
