/*
 * The frozen legacy exposition, and the deliberate split between it and the
 * admin-facing one.
 *
 * The goldens under relay/tests/golden/ were minted by the ACCEPTED BASE renderer,
 * not by this tree. That is the whole point: comparing the current wrapper
 * against the current `_ex` path would only prove the tree agrees with
 * itself. These bytes are external evidence.
 */

#include <moq/relay/moqr_obs.h>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"
#include "obs_golden_fixture.h"

#ifndef MOQR_GOLDEN_DIR
#define MOQR_GOLDEN_DIR "."
#endif

static char *
slurp(const char *name, size_t *out_len)
{
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", MOQR_GOLDEN_DIR, name);
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        fprintf(stderr, "golden missing: %s\n", path);
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long sz = ftell(f);
    rewind(f);
    if (sz < 0) {
        fclose(f);
        return NULL;
    }
    char *buf = malloc((size_t)sz + 1u);
    if (buf == NULL) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[got] = '\0';
    *out_len = got;
    return buf;
}

/* Report the first divergence rather than a bare inequality: a golden that
 * fails is usually one line different, and the offset says which. */
static int
same_bytes(const char *want, size_t wn, const char *got, size_t gn,
           const char *what)
{
    if (wn == gn && memcmp(want, got, wn) == 0) {
        return 0;
    }
    size_t i = 0;
    while (i < wn && i < gn && want[i] == got[i]) {
        i++;
    }
    fprintf(stderr,
            "FAIL: %s diverges from the frozen golden at byte %zu "
            "(golden %zu bytes, rendered %zu bytes)\n",
            what, i, wn, gn);
    fprintf(stderr, "  golden:   %.72s\n", want + (i > 40 ? i - 40 : 0));
    fprintf(stderr, "  rendered: %.72s\n", got + (i > 40 ? i - 40 : 0));
    return 1;
}

static int
test_legacy_single_matches_frozen_golden(void)
{
    int failures = 0;
    size_t wn = 0;
    char *want = slurp("legacy_prometheus_single.txt", &wn);
    MOQ_TEST_CHECK(want != NULL);
    if (want == NULL) {
        return 1;
    }

    moqr_core_stats_t cs;
    moqr_bind_stats_t bs;
    moqr_golden_core(&cs);
    moqr_golden_bind(&bs);
    moqr_obs_labels_t lb = { 0, "msquic", "moqt-18+moqt-16" };

    static char got[1u << 20];
    size_t gn = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_write_prometheus(&cs, &bs, &lb, got, sizeof(got), &gn),
        MOQR_OK);
    failures += same_bytes(want, wn, got, gn, "legacy single");
    free(want);
    return failures;
}

static int
test_legacy_multi_matches_frozen_golden(void)
{
    int failures = 0;
    size_t wn = 0;
    char *want = slurp("legacy_prometheus_multi.txt", &wn);
    MOQ_TEST_CHECK(want != NULL);
    if (want == NULL) {
        return 1;
    }

    moqr_core_stats_t cs;
    moqr_bind_stats_t bs;
    moqr_shards_stats_t sh;
    moqr_golden_core(&cs);
    moqr_golden_bind(&bs);
    moqr_golden_shard(&sh);
    moqr_snapshot_view_t vs[2];
    moqr_golden_views(vs, &cs, &bs, &sh);

    static char got[1u << 20];
    size_t gn = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_write_prometheus_multi(vs, 2, got, sizeof(got), &gn),
        MOQR_OK);
    failures += same_bytes(want, wn, got, gn, "legacy multi");
    free(want);
    return failures;
}

/*
 * The API split, stated as a test so it cannot be quietly undone in either
 * direction: the legacy writer must NOT carry the local wake series, and the
 * `_ex` writer at the very same format MUST.
 */
static int
test_legacy_and_ex_differ_exactly_by_extended_series(void)
{
    int failures = 0;
    moqr_core_stats_t cs;
    moqr_bind_stats_t bs;
    moqr_shards_stats_t sh;
    moqr_golden_core(&cs);
    moqr_golden_bind(&bs);
    moqr_golden_shard(&sh);
    moqr_snapshot_view_t vs[2];
    moqr_golden_views(vs, &cs, &bs, &sh);

    static char legacy[1u << 20];
    static char ext[1u << 20];
    size_t ln = 0, en = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_write_prometheus_multi(vs, 2, legacy, sizeof(legacy),
                                            &ln),
        MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_write_multi_ex(vs, 2, MOQR_OBS_FMT_PROMETHEUS_004, ext,
                                    sizeof(ext), &en),
        MOQR_OK);

    /* Same format, different documents — deliberately. */
    MOQ_TEST_CHECK(ln != en);

    MOQ_TEST_CHECK(strstr(legacy, "cause=\"local\"") == NULL);
    MOQ_TEST_CHECK(strstr(ext, "cause=\"local\"") != NULL);

    /* Both per-shard series and the process aggregate are load-bearing. */
    MOQ_TEST_CHECK(strstr(ext,
                          "moqrelay_wake_requests_total{shard=\"0\","
                          "transport=\"msquic\","
                          "version=\"moqt-18+moqt-16\",cause=\"local\"} 900")
                   != NULL);
    MOQ_TEST_CHECK(
        strstr(ext, "moqrelay_process_wake_requests_total{cause=\"local\"} "
                    "1800") != NULL);

    /* The shared causes are unchanged across the split. */
    MOQ_TEST_CHECK(strstr(legacy, "cause=\"push\"") != NULL);
    MOQ_TEST_CHECK(strstr(ext, "cause=\"push\"") != NULL);

    /* F5: the legacy help text keeps its accepted wording; the extended one
     * must not still claim every cause crosses a shard. */
    MOQ_TEST_CHECK(strstr(legacy, "Cross-shard wake requests") != NULL);
    MOQ_TEST_CHECK(strstr(ext, "crosses no shard") != NULL);
    MOQ_TEST_CHECK(strstr(ext, "Cross-shard wake requests") == NULL);
    return failures;
}

/* -- F2: the bound must be safe under genuine concurrency ----------------- */

#define CONC_THREADS 8
#define CONC_ROUNDS  400

struct conc_arg {
    uint32_t          lanes;
    moqr_obs_format_t fmt;
    uint64_t          expect;
    int               mismatches;
};

static void *
conc_worker(void *p)
{
    struct conc_arg *a = (struct conc_arg *)p;
    for (int i = 0; i < CONC_ROUNDS; i++) {
        uint64_t b = 0;
        if (moqr_metrics_bound(a->lanes, a->fmt, true, true, &b) != MOQR_OK ||
            b != a->expect) {
            a->mismatches++;
        }
    }
    return NULL;
}

/*
 * Concurrent callers must each get the same answer. Under the previous
 * implementation this shared one mutable static view array, so this races and
 * can also observe another thread's stack pointers. Sequential repetition
 * would not have found it.
 */
static int
test_bound_is_concurrency_safe(void)
{
    int failures = 0;
    const uint32_t lanes = 16;
    uint64_t expect = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_bound(lanes, MOQR_OBS_FMT_OPENMETRICS_100, true, true,
                           &expect),
        MOQR_OK);
    MOQ_TEST_CHECK(expect > 0);

    pthread_t       th[CONC_THREADS];
    struct conc_arg args[CONC_THREADS];
    for (int i = 0; i < CONC_THREADS; i++) {
        args[i].lanes = lanes;
        args[i].fmt = MOQR_OBS_FMT_OPENMETRICS_100;
        args[i].expect = expect;
        args[i].mismatches = 0;
        MOQ_TEST_CHECK_EQ_INT(
            pthread_create(&th[i], NULL, conc_worker, &args[i]), 0);
    }
    for (int i = 0; i < CONC_THREADS; i++) {
        pthread_join(th[i], NULL);
        MOQ_TEST_CHECK_EQ_INT(args[i].mismatches, 0);
    }
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_legacy_single_matches_frozen_golden();
    failures += test_legacy_multi_matches_frozen_golden();
    failures += test_legacy_and_ex_differ_exactly_by_extended_series();
    failures += test_bound_is_concurrency_safe();
    if (failures != 0) {
        fprintf(stderr, "test_relay_obs_golden: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_relay_obs_golden: OK\n");
    return 0;
}
