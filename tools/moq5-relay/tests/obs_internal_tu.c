/*
 * Internal translation unit for the observability renderer.
 *
 * The checked length accumulation is a static inside metrics.c and cannot be
 * driven to its overflow edge through the public API: reaching it would take
 * SIZE_MAX bytes of output. Compiling the renderer into the test binary lets
 * the accumulator be seeded next to its boundary directly, which is the only
 * way to prove the fail-closed path actually runs.
 *
 * This mirrors the CLI tests, which include cli/main.c the same way. Nothing
 * here is exported from a production library.
 */

#include "../obs/metrics.c"

#include <stdbool.h>

/* Append `n` bytes of content to a writer whose length starts at `start`, and
 * report the resulting length and whether the accumulator refused. */
void
moqr_obs_test_accumulate(size_t start, unsigned repeat, size_t *out_len,
                         bool *out_overflow)
{
    char  sink = 0;
    mw_t  w = { &sink, 1u, start, MOQR_OBS_FMT_PROMETHEUS_004, true, false };
    for (unsigned i = 0; i < repeat; i++) {
        mw_addf(&w, "0123456789");
    }
    *out_len = w.len;
    *out_overflow = w.overflow;
}

/* Drive the SHARED production tail with an already-overflowed accumulator.
 * This is the function both public entry points end in, so neutering its
 * refusal is caught here rather than passing unnoticed. */
moqr_result_t
moqr_obs_test_finish_overflowed(size_t *written)
{
    char sink[64];
    mw_t w = { sink, sizeof(sink), 123u, MOQR_OBS_FMT_PROMETHEUS_004, true,
               false };
    /* Push the accumulator past its limit through the real append path. */
    w.len = SIZE_MAX - 4u;
    mw_addf(&w, "0123456789");
    return mw_finish(&w, sink, sizeof(sink), written, NULL);
}

/* The same tail on a healthy writer still reports the real length. */
moqr_result_t
moqr_obs_test_finish_clean(size_t *written)
{
    char sink[64];
    mw_t w = { sink, sizeof(sink), 0u, MOQR_OBS_FMT_PROMETHEUS_004, true,
               false };
    mw_addf(&w, "0123456789");
    return mw_finish(&w, sink, sizeof(sink), written, NULL);
}

/* Drive the REAL bound finalization. Reaching a poisoned accumulation through
 * the public bound would take SIZE_MAX bytes of output, so the decision
 * function production uses is exercised directly rather than re-implemented. */
moqr_result_t
moqr_obs_test_bound_finalize(int rc, bool poisoned, size_t need,
                             uint64_t *out_bytes)
{
    return bound_finalize((moqr_result_t)rc, poisoned, need, out_bytes);
}

/* mw_finish is the single place w->overflow is read; prove it reports both
 * outcomes through the out-param the bound relies on. */
moqr_result_t
moqr_obs_test_finish_reports_poison(bool seed_overflow, bool *poisoned,
                                    size_t *written)
{
    char sink[64];
    mw_t w = { sink, sizeof(sink), 0u, MOQR_OBS_FMT_PROMETHEUS_004, true,
               false };
    if (seed_overflow) {
        w.len = SIZE_MAX - 4u;
    }
    mw_addf(&w, "0123456789");
    return mw_finish(&w, sink, sizeof(sink), written, poisoned);
}

/* A real render must actually write through the discriminator pointer, or a
 * poisoned document would reach the bound looking healthy. */
moqr_result_t
moqr_obs_test_write_multi_poison_flag(const moqr_snapshot_view_t *vs,
                                      uint32_t n, char *buf, size_t cap,
                                      size_t *written, bool *poisoned)
{
    return write_multi(vs, n, MOQR_OBS_FMT_PROMETHEUS_004, true, buf, cap,
                       written, poisoned);
}
