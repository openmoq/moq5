/* Exposition formats: Prometheus 0.0.4 compatibility, OpenMetrics 1.0, and
 * the worst-case size bound that lets a caller preallocate. */

#include <moqr_obs.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

#define HAS(hay, needle) (strstr((hay), (needle)) != NULL)

/* Every counter value at its maximum width, so a bound that assumed short
 * decimals fails here rather than in production. */
static moqr_core_stats_t
core_max(void)
{
    moqr_core_stats_t cs;
    /* Every field at its own type's maximum. The internal-entity exclusion
     * invariant holds by equality against an equally-saturated shard
     * snapshot, so the render is not suppressed. */
    memset(&cs, 0xff, sizeof(cs));
    return cs;
}

static moqr_bind_stats_t
bind_max(void)
{
    moqr_bind_stats_t bs;
    memset(&bs, 0xff, sizeof(bs));
    return bs;
}

static moqr_shards_stats_t
shard_max(void)
{
    moqr_shards_stats_t sh;
    memset(&sh, 0xff, sizeof(sh));
    return sh;
}

/* A label value that is all escape-expanding characters: every byte becomes
 * two, so the bound must budget for expansion, not raw length. */
static const char *
label_worst(void)
{
    static char s[64];
    memset(s, '"', sizeof(s) - 1);
    s[sizeof(s) - 1] = '\0';
    return s;
}

/* -- the legacy / admin-facing split -------------------------------------
 *
 * Byte identity with the ACCEPTED BASE is proven in test_relay_obs_golden.c
 * against frozen goldens minted by the base renderer. Comparing this tree's
 * wrapper with this tree's `_ex` path proves only that the tree agrees with
 * itself, so what is pinned here is the DIFFERENCE the two APIs are supposed
 * to have.
 */

/* The single-snapshot document carries no shard plane, so it has no wake
 * family and no extended series: legacy and `_ex` coincide here. */
static int
test_p004_single_has_no_extended_series(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_bind_stats_t bs = bind_max();
    moqr_obs_labels_t lb = { 3, "msquic", "moqt-18+moqt-16" };

    static char legacy[262144];
    static char via_ex[262144];
    size_t n_legacy = 0, n_ex = 0;

    moqr_result_t r1 = moqr_metrics_write_prometheus(&cs, &bs, &lb, legacy,
                                                     sizeof(legacy),
                                                     &n_legacy);
    moqr_result_t r2 = moqr_metrics_write_ex(&cs, &bs, &lb,
                                             MOQR_OBS_FMT_PROMETHEUS_004,
                                             via_ex, sizeof(via_ex), &n_ex);
    MOQ_TEST_CHECK_EQ_INT(r1, MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(r2, MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n_legacy, (uint64_t)n_ex);
    MOQ_TEST_CHECK(memcmp(legacy, via_ex, n_legacy) == 0);

    /* 0.0.4 has no OpenMetrics furniture. */
    MOQ_TEST_CHECK(!HAS(legacy, "# EOF"));
    MOQ_TEST_CHECK(!HAS(legacy, "# UNIT"));
    /* 0.0.4 keeps the _total suffix on the family name itself. */
    MOQ_TEST_CHECK(HAS(legacy, "# TYPE moqrelay_objects_ingested_total counter"));
    return failures;
}

/* The multi-shard document DOES carry the extended series, so the legacy
 * writer and `_ex` at the same format must differ — and differ only there. */
static int
test_p004_multi_legacy_omits_extended_series(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_bind_stats_t bs = bind_max();
    moqr_shards_stats_t sh = shard_max();
    moqr_snapshot_view_t vs[2];
    memset(vs, 0, sizeof(vs));
    vs[0].core = &cs; vs[0].bind = &bs; vs[0].shard = &sh;
    vs[0].labels = (moqr_obs_labels_t){ 0, "msquic", "moqt-18" };
    vs[1].core = &cs; vs[1].bind = &bs; vs[1].shard = &sh;
    vs[1].labels = (moqr_obs_labels_t){ 1, "wtquic-msquic", "moqt-18" };

    static char legacy[262144];
    static char via_ex[262144];
    size_t n_legacy = 0, n_ex = 0;

    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_prometheus_multi(
                              vs, 2, legacy, sizeof(legacy), &n_legacy),
                          MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_multi_ex(
                              vs, 2, MOQR_OBS_FMT_PROMETHEUS_004, via_ex,
                              sizeof(via_ex), &n_ex),
                          MOQR_OK);

    MOQ_TEST_CHECK(!HAS(legacy, "cause=\"local\""));
    MOQ_TEST_CHECK(HAS(via_ex, "cause=\"local\""));
    MOQ_TEST_CHECK(n_ex > n_legacy);
    MOQ_TEST_CHECK(!HAS(legacy, "# EOF"));
    MOQ_TEST_CHECK(!HAS(via_ex, "# EOF"));
    return failures;
}

/* -- OpenMetrics 1.0 ------------------------------------------------------ */

static int
test_openmetrics_shape(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_bind_stats_t bs = bind_max();
    moqr_obs_labels_t lb = { 0, "msquic", "moqt-18" };

    static char om[262144];
    size_t n = 0;
    moqr_result_t rc = moqr_metrics_write_ex(&cs, &bs, &lb,
                                             MOQR_OBS_FMT_OPENMETRICS_100, om,
                                             sizeof(om), &n);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQR_OK);

    /* Terminal # EOF, and it really is terminal. */
    MOQ_TEST_CHECK(n >= 6);
    MOQ_TEST_CHECK(memcmp(om + n - 6, "# EOF\n", 6) == 0);

    /* MetricFamily name drops _total; the sample keeps it. */
    MOQ_TEST_CHECK(HAS(om, "# TYPE moqrelay_objects_ingested counter\n"));
    MOQ_TEST_CHECK(!HAS(om, "# TYPE moqrelay_objects_ingested_total counter"));
    MOQ_TEST_CHECK(HAS(om, "moqrelay_objects_ingested_total{"));

    /* Gauges are unsuffixed and unchanged. */
    MOQ_TEST_CHECK(HAS(om, "# TYPE moqrelay_bindings gauge\n"));
    return failures;
}

static int
test_openmetrics_unit_metadata(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_bind_stats_t bs = bind_max();
    moqr_shards_stats_t sh = shard_max();
    moqr_snapshot_view_t v;
    memset(&v, 0, sizeof(v));
    v.core = &cs; v.bind = &bs; v.shard = &sh;
    v.labels = (moqr_obs_labels_t){ 0, "msquic", "moqt-18" };

    static char om[262144];
    size_t n = 0;
    moqr_result_t rc = moqr_metrics_write_multi_ex(
        &v, 1, MOQR_OBS_FMT_OPENMETRICS_100, om, sizeof(om), &n);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQR_OK);

    /* A family whose name ends in a base unit carries # UNIT. */
    MOQ_TEST_CHECK(HAS(om, "# UNIT moqrelay_forward_latency_seconds seconds\n"));
    /* A family with no unit suffix must not invent one. */
    MOQ_TEST_CHECK(!HAS(om, "# UNIT moqrelay_bindings"));
    MOQ_TEST_CHECK(memcmp(om + n - 6, "# EOF\n", 6) == 0);
    return failures;
}

static int
test_unknown_format_refused(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_obs_labels_t lb = { 0, "msquic", "moqt-18" };
    char buf[4096];
    size_t n = 12345;

    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_ex(&cs, NULL, &lb,
                                                MOQR_OBS_FMT__COUNT, buf,
                                                sizeof(buf), &n),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);

    moqr_snapshot_view_t v;
    memset(&v, 0, sizeof(v));
    v.core = &cs;
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_multi_ex(&v, 1, 99u, buf,
                                                      sizeof(buf), &n),
                          MOQR_ERR_INVAL);
    return failures;
}

/* -- the size bound ------------------------------------------------------- */

/* Render at `lanes` with everything maximal and prove the advertised bound
 * holds. This is the acceptance criterion for preallocation: if it can be
 * exceeded, a fixed buffer truncates in production. */
static int
bound_holds_at(uint32_t lanes, moqr_obs_format_t fmt, bool with_bind,
               bool with_shard, size_t *out_rendered, uint64_t *out_bound)
{
    int failures = 0;
    static moqr_core_stats_t cs;
    static moqr_bind_stats_t bs;
    static moqr_shards_stats_t sh;
    static moqr_snapshot_view_t vs[MOQR_SHARDS_MAX];
    cs = core_max();
    bs = bind_max();
    sh = shard_max();

    memset(vs, 0, sizeof(vs));
    for (uint32_t i = 0; i < lanes; i++) {
        vs[i].core = &cs;
        vs[i].bind = with_bind ? &bs : NULL;
        vs[i].shard = with_shard ? &sh : NULL;
        vs[i].lane_wakes = UINT64_MAX;
        /* Worst-case labels: maximum shard number and escape-expanding
         * transport/version tokens. */
        vs[i].labels.shard = UINT16_MAX;
        vs[i].labels.transport = label_worst();
        vs[i].labels.version = label_worst();
    }

    uint64_t bound = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_bound(lanes, fmt, with_bind, with_shard, &bound),
        MOQR_OK);

    /* Render into a buffer sized exactly by the bound: OK (not CAPACITY)
     * proves the content fit with room for the NUL. */
    static char big[8u * 1024u * 1024u];
    MOQ_TEST_CHECK(bound + 1u <= sizeof(big));
    size_t n = 0;
    moqr_result_t rc =
        moqr_metrics_write_multi_ex(vs, lanes, fmt, big, (size_t)bound + 1u,
                                    &n);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQR_OK);
    MOQ_TEST_CHECK((uint64_t)n <= bound);

    if (out_rendered != NULL) {
        *out_rendered = n;
    }
    if (out_bound != NULL) {
        *out_bound = bound;
    }
    return failures;
}

static int
test_bound_holds_every_capability(void)
{
    int failures = 0;
    const uint32_t lane_counts[] = { 1u, 2u, MOQR_SHARDS_MAX };
    const moqr_obs_format_t fmts[] = { MOQR_OBS_FMT_PROMETHEUS_004,
                                       MOQR_OBS_FMT_OPENMETRICS_100 };

    for (size_t li = 0; li < sizeof(lane_counts) / sizeof(lane_counts[0]);
         li++) {
        for (size_t fi = 0; fi < sizeof(fmts) / sizeof(fmts[0]); fi++) {
            for (int wb = 0; wb < 2; wb++) {
                for (int ws = 0; ws < 2; ws++) {
                    failures += bound_holds_at(lane_counts[li], fmts[fi],
                                               wb != 0, ws != 0, NULL, NULL);
                }
            }
        }
    }
    return failures;
}

/*
 * The bound must be ACHIEVABLE, not merely safe: a bound with slack in it
 * cannot be shrunk by a test, so nothing would catch a bound that quietly
 * drifts below the document it is supposed to cover.
 *
 * The widest render is not the all-saturated snapshot. Several gauges render
 * `core minus internal` alongside the internal itself, so saturating both
 * makes the difference collapse to a single `0` digit. Half-saturating each
 * internal makes both the difference and the internal render at full width,
 * which is the true maximum — and is what the bound must equal.
 */
static void
build_worst_views(moqr_snapshot_view_t *vs, uint32_t lanes,
                  moqr_core_stats_t *cs, moqr_bind_stats_t *bs,
                  moqr_shards_stats_t *sh, bool with_bind, bool with_shard)
{
    *cs = core_max();
    *bs = bind_max();
    *sh = shard_max();
    sh->pump_subs_parked = cs->subs_parked / 2u;
    sh->pump_subs_active = cs->subs_active / 2u;
    sh->internal_bindings = cs->bindings / 2u;
    sh->internal_ns_subs = cs->ns_subs / 2u;

    memset(vs, 0, sizeof(*vs) * lanes);
    for (uint32_t i = 0; i < lanes; i++) {
        vs[i].core = cs;
        vs[i].bind = with_bind ? bs : NULL;
        vs[i].shard = with_shard ? sh : NULL;
        vs[i].lane_wakes = UINT64_MAX;
        vs[i].labels.shard = UINT16_MAX;
        vs[i].labels.transport = label_worst();
        vs[i].labels.version = label_worst();
    }
}

static int
test_bound_is_exactly_achieved(void)
{
    int failures = 0;
    static char big[8u * 1024u * 1024u];
    static moqr_core_stats_t cs;
    static moqr_bind_stats_t bs;
    static moqr_shards_stats_t sh;
    static moqr_snapshot_view_t vs[MOQR_SHARDS_MAX];
    const moqr_obs_format_t fmts[] = { MOQR_OBS_FMT_PROMETHEUS_004,
                                       MOQR_OBS_FMT_OPENMETRICS_100 };
    const uint32_t lane_counts[] = { 1u, MOQR_SHARDS_MAX };

    for (size_t fi = 0; fi < sizeof(fmts) / sizeof(fmts[0]); fi++) {
        for (size_t li = 0; li < sizeof(lane_counts) / sizeof(lane_counts[0]);
             li++) {
            uint32_t lanes = lane_counts[li];
            build_worst_views(vs, lanes, &cs, &bs, &sh, true, true);

            uint64_t bound = 0;
            MOQ_TEST_CHECK_EQ_INT(
                moqr_metrics_bound(lanes, fmts[fi], true, true, &bound),
                MOQR_OK);

            size_t n = 0;
            MOQ_TEST_CHECK_EQ_INT(
                moqr_metrics_write_multi_ex(vs, lanes, fmts[fi], big,
                                            sizeof(big), &n),
                MOQR_OK);

            /* Tightness. A bound larger than this is slack no test can
             * shrink; a bound smaller truncates in production. */
            MOQ_TEST_CHECK_EQ_U64(bound, (uint64_t)n);

            /* The boundary itself: bound bytes of capacity leaves no room
             * for the NUL, so the advertised size must be bound + 1. This is
             * what a bound reduced by one fails. */
            size_t n2 = 0;
            MOQ_TEST_CHECK_EQ_INT(
                moqr_metrics_write_multi_ex(vs, lanes, fmts[fi], big,
                                            (size_t)bound, &n2),
                MOQR_ERR_CAPACITY);
            MOQ_TEST_CHECK_EQ_U64((uint64_t)n2, bound);
        }
    }
    return failures;
}

static int
test_bound_rejects_bad_args(void)
{
    int failures = 0;
    uint64_t b = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_bound(0, MOQR_OBS_FMT_PROMETHEUS_004, true, true, &b),
        MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_bound(MOQR_SHARDS_MAX + 1u,
                                             MOQR_OBS_FMT_PROMETHEUS_004, true,
                                             true, &b),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_bound(1, MOQR_OBS_FMT__COUNT, true, true, &b),
        MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_metrics_bound(1, MOQR_OBS_FMT_PROMETHEUS_004, true, true, NULL),
        MOQR_ERR_INVAL);
    return failures;
}

/* A render must never reset a high-water mark: two consecutive renders of
 * the same snapshot are byte-identical, and the source stats are untouched. */
static int
test_render_does_not_reset_highwater(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_shards_stats_t sh = shard_max();
    sh.channel_entries_hwm = 4242;
    sh.channel_bytes_hwm = 999999;
    cs.intent_highwater = 777;

    moqr_shards_stats_t sh_before = sh;
    moqr_core_stats_t cs_before = cs;

    moqr_snapshot_view_t v;
    memset(&v, 0, sizeof(v));
    v.core = &cs; v.shard = &sh;
    v.labels = (moqr_obs_labels_t){ 0, "msquic", "moqt-18" };

    static char a[262144];
    static char b[262144];
    size_t na = 0, nb = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_multi_ex(
                              &v, 1, MOQR_OBS_FMT_OPENMETRICS_100, a,
                              sizeof(a), &na),
                          MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_multi_ex(
                              &v, 1, MOQR_OBS_FMT_OPENMETRICS_100, b,
                              sizeof(b), &nb),
                          MOQR_OK);

    MOQ_TEST_CHECK_EQ_U64((uint64_t)na, (uint64_t)nb);
    MOQ_TEST_CHECK(memcmp(a, b, na) == 0);
    MOQ_TEST_CHECK(memcmp(&sh, &sh_before, sizeof(sh)) == 0);
    MOQ_TEST_CHECK(memcmp(&cs, &cs_before, sizeof(cs)) == 0);
    MOQ_TEST_CHECK(HAS(a, "4242"));
    return failures;
}

/* The third wake cause is carried in the snapshot but had no series. */
static int
test_wake_requests_local_series(void)
{
    int failures = 0;
    moqr_core_stats_t cs = core_max();
    moqr_shards_stats_t sh = shard_max();
    sh.wake_requests_push = 11;
    sh.wake_requests_credit = 22;
    sh.wake_requests_local = 33;

    moqr_snapshot_view_t v;
    memset(&v, 0, sizeof(v));
    v.core = &cs; v.shard = &sh;
    v.labels = (moqr_obs_labels_t){ 0, "msquic", "moqt-18" };

    static char om[262144];
    size_t n = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_multi_ex(
                              &v, 1, MOQR_OBS_FMT_PROMETHEUS_004, om,
                              sizeof(om), &n),
                          MOQR_OK);
    /* The per-shard series must be named AND labelled: a bare
     * `cause="local"} 33` would also match the process aggregate below, so
     * dropping the per-shard line would go unnoticed. */
#define WAKE_SERIES(cause_)                                                   \
    "moqrelay_wake_requests_total{shard=\"0\",transport=\"msquic\","          \
    "version=\"moqt-18\",cause=\"" cause_ "\"} "
    MOQ_TEST_CHECK(HAS(om, WAKE_SERIES("push") "11"));
    MOQ_TEST_CHECK(HAS(om, WAKE_SERIES("credit") "22"));
    MOQ_TEST_CHECK(HAS(om, WAKE_SERIES("local") "33"));
#undef WAKE_SERIES
    MOQ_TEST_CHECK(
        HAS(om, "moqrelay_process_wake_requests_total{cause=\"local\"} 33"));
    return failures;
}


/* -- F3: the checked accumulator ----------------------------------------- */

void moqr_obs_test_accumulate(size_t start, unsigned repeat, size_t *out_len,
                              bool *out_overflow);
moqr_result_t moqr_obs_test_finish_overflowed(size_t *written);
moqr_result_t moqr_obs_test_bound_finalize(int rc, bool poisoned, size_t need,
                                           uint64_t *out_bytes);
moqr_result_t moqr_obs_test_finish_reports_poison(bool seed_overflow,
                                                  bool *poisoned,
                                                  size_t *written);
moqr_result_t moqr_obs_test_write_multi_poison_flag(
    const moqr_snapshot_view_t *vs, uint32_t n, char *buf, size_t cap,
    size_t *written, bool *poisoned);
moqr_result_t moqr_obs_test_finish_clean(size_t *written);

static int
test_length_accumulation_is_checked(void)
{
    int failures = 0;
    size_t len = 0;
    bool ovf = true;

    /* Ordinary accumulation is exact and does not trip the guard. */
    moqr_obs_test_accumulate(0u, 3u, &len, &ovf);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)len, 30u);
    MOQ_TEST_CHECK(!ovf);

    /* One append that still fits leaves the guard clear. */
    moqr_obs_test_accumulate(SIZE_MAX - 10u, 1u, &len, &ovf);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)len, (uint64_t)SIZE_MAX);
    MOQ_TEST_CHECK(!ovf);

    /* One byte further would wrap: the guard fires and the length saturates
     * instead of becoming a small, plausible, wrong requirement. */
    moqr_obs_test_accumulate(SIZE_MAX - 9u, 1u, &len, &ovf);
    MOQ_TEST_CHECK(ovf);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)len, (uint64_t)SIZE_MAX);

    /* Sticky: a later append cannot clear it. */
    moqr_obs_test_accumulate(SIZE_MAX - 5u, 3u, &len, &ovf);
    MOQ_TEST_CHECK(ovf);

    /* And the shared tail both entry points return through refuses, carrying
     * no size a caller could allocate from. */
    size_t written = 12345;
    MOQ_TEST_CHECK_EQ_INT(moqr_obs_test_finish_overflowed(&written),
                          MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)written, 0u);

    /* The same tail on a healthy writer still reports the real length, so the
     * refusal above is the overflow guard and not a blanket failure. */
    written = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_obs_test_finish_clean(&written), MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)written, 10u);
    return failures;
}

/* -- F4: deterministic seeded snapshots ---------------------------------- */

/* A tiny reproducible PRNG so a failure names one seed rather than "sometimes". */
static uint64_t
seed_next(uint64_t *st)
{
    uint64_t x = *st;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *st = x;
    return x;
}

/* Label values that sit on the escape and truncation boundaries: just under,
 * exactly at, and past what esc_label can hold once every byte doubles. */
static const char *
seeded_label(uint64_t r, char *out, size_t cap)
{
    static const char *const fixed[] = {
        "", "msquic", "wtquic-msquic", "moqt-18+moqt-16",
        "a\"b", "back\\slash", "nl\nin",
    };
    unsigned pick = (unsigned)(r % 10u);
    if (pick < sizeof(fixed) / sizeof(fixed[0])) {
        return fixed[pick];
    }
    /* 30, 31, 32 quote characters: esc_label doubles each, so these straddle
     * its 64-byte output buffer exactly. */
    size_t n = 30u + (size_t)(pick - 7u);
    if (n >= cap) {
        n = cap - 1u;
    }
    memset(out, '"', n);
    out[n] = '\0';
    return out;
}

/*
 * Valid-invariant snapshots: internals stay at or below their core
 * counterparts so nothing is suppressed, values span the width range rather
 * than sitting at synthetic maxima, and optional capabilities vary PER VIEW
 * so a mixed document is covered too.
 */
static void
seeded_views(uint64_t seed, uint32_t lanes, moqr_snapshot_view_t *vs,
             moqr_core_stats_t *cs, moqr_bind_stats_t *bs,
             moqr_shards_stats_t *sh, char (*labbuf)[80], bool with_bind,
             bool with_shard)
{
    uint64_t st = seed | 1u;
    memset(vs, 0, sizeof(*vs) * lanes);
    for (uint32_t i = 0; i < lanes; i++) {
        memset(&cs[i], 0, sizeof(cs[i]));
        memset(&bs[i], 0, sizeof(bs[i]));
        memset(&sh[i], 0, sizeof(sh[i]));

        cs[i].ingested_total = seed_next(&st);
        cs[i].delivered_total = seed_next(&st);
        cs[i].evicted_total = seed_next(&st) >> 32;
        cs[i].retained_bytes = seed_next(&st);
        cs[i].route_epoch = seed_next(&st);
        cs[i].bindings = (uint32_t)(seed_next(&st) & 0xffffu);
        cs[i].tracks = (uint32_t)(seed_next(&st) & 0xffffu);
        cs[i].subs_parked = (uint32_t)(seed_next(&st) & 0xffffu);
        cs[i].subs_active = (uint32_t)(seed_next(&st) & 0xffffu);
        cs[i].subs = cs[i].subs_parked + cs[i].subs_active;
        cs[i].ns_nodes = (uint32_t)(seed_next(&st) & 0xffffu);
        cs[i].ns_subs = (uint32_t)(seed_next(&st) & 0xffffu);
        cs[i].intent_highwater = (uint32_t)(seed_next(&st) & 0xffffu);
        for (uint32_t k = 0; k < MOQR_REFUSE__COUNT; k++) {
            cs[i].refusals[k] = seed_next(&st) >> 16;
        }
        for (uint32_t a = 0; a < MOQR_AUTH_ACTION__COUNT; a++) {
            for (uint32_t d = 0; d < 3u; d++) {
                cs[i].auth_decisions[a][d] = seed_next(&st) >> 20;
            }
        }
        for (uint32_t r = 0; r < MOQR_AUTH_REASON__COUNT; r++) {
            cs[i].auth_denials[r] = seed_next(&st) >> 20;
        }

        bs[i].conns = seed_next(&st) >> 40;
        bs[i].events_translated = seed_next(&st);
        bs[i].deliveries_written = seed_next(&st);
        bs[i].ingest_refusals = seed_next(&st) >> 32;
        bs[i].session_errors = seed_next(&st) >> 32;

        sh[i].wake_requests_push = seed_next(&st);
        sh[i].wake_requests_credit = seed_next(&st);
        sh[i].wake_requests_local = seed_next(&st);
        sh[i].pump_turns = seed_next(&st);
        sh[i].journal_epoch = seed_next(&st);
        sh[i].channel_entries_hwm = (uint32_t)(seed_next(&st) & 0xffffu);
        sh[i].channel_bytes_hwm = seed_next(&st);
        /* Invariant: an internal never exceeds its core counterpart. */
        sh[i].pump_subs_parked = cs[i].subs_parked;
        sh[i].pump_subs_active = cs[i].subs_active;
        sh[i].internal_bindings = cs[i].bindings;
        sh[i].internal_ns_subs = cs[i].ns_subs;
        if ((seed_next(&st) & 1u) != 0u) {
            sh[i].pump_subs_parked /= 2u;
            sh[i].internal_bindings /= 3u;
        }

        vs[i].core = &cs[i];
        /* Per-view capability mixing: within a run that allows an optional
         * plane, some views still omit it. */
        vs[i].bind = (with_bind && (seed_next(&st) & 3u) != 0u) ? &bs[i] : NULL;
        vs[i].shard =
            (with_shard && (seed_next(&st) & 3u) != 0u) ? &sh[i] : NULL;
        vs[i].lane_wakes = seed_next(&st);
        vs[i].labels.shard = (uint16_t)(seed_next(&st) & 0xffffu);
        vs[i].labels.transport =
            seeded_label(seed_next(&st), labbuf[2u * i], 80u);
        vs[i].labels.version =
            seeded_label(seed_next(&st), labbuf[2u * i + 1u], 80u);
    }
}

static int
test_seeded_snapshots_never_exceed_bound(void)
{
    int failures = 0;
    static moqr_core_stats_t cs[MOQR_SHARDS_MAX];
    static moqr_bind_stats_t bs[MOQR_SHARDS_MAX];
    static moqr_shards_stats_t sh[MOQR_SHARDS_MAX];
    static moqr_snapshot_view_t vs[MOQR_SHARDS_MAX];
    static char labbuf[2u * MOQR_SHARDS_MAX][80];
    static char out[8u * 1024u * 1024u];

    const uint32_t lane_counts[] = { 1u, MOQR_SHARDS_MAX };
    const moqr_obs_format_t fmts[] = { MOQR_OBS_FMT_PROMETHEUS_004,
                                       MOQR_OBS_FMT_OPENMETRICS_100 };

    for (size_t li = 0; li < sizeof(lane_counts) / sizeof(lane_counts[0]);
         li++) {
        for (size_t fi = 0; fi < sizeof(fmts) / sizeof(fmts[0]); fi++) {
            for (int wb = 0; wb < 2; wb++) {
                for (int ws = 0; ws < 2; ws++) {
                    uint64_t bound = 0;
                    MOQ_TEST_CHECK_EQ_INT(
                        moqr_metrics_bound(lane_counts[li], fmts[fi], wb != 0,
                                           ws != 0, &bound),
                        MOQR_OK);
                    for (uint64_t seed = 1; seed <= 64; seed++) {
                        seeded_views(seed, lane_counts[li], vs, cs, bs, sh,
                                     labbuf, wb != 0, ws != 0);
                        size_t n = 0;
                        moqr_result_t rc = moqr_metrics_write_multi_ex(
                            vs, lane_counts[li], fmts[fi], out, sizeof(out),
                            &n);
                        if (rc != MOQR_OK || (uint64_t)n > bound) {
                            fprintf(stderr,
                                    "FAIL: seed %llu lanes %u fmt %u "
                                    "bind %d shard %d: rc=%d n=%zu bound=%llu\n",
                                    (unsigned long long)seed, lane_counts[li],
                                    (unsigned)fmts[fi], wb, ws, (int)rc, n,
                                    (unsigned long long)bound);
                            failures++;
                        }
                    }
                }
            }
        }
    }
    return failures;
}

/* -- OpenMetrics structural validation ------------------------------------
 *
 * A bounded family registry, not line counting. Counting HELP and TYPE lines
 * and comparing totals passes a document with a duplicate HELP for one family
 * and none for another, so the registry resolves every metadata line and
 * every sample to a named family and judges each family on its own.
 */

/* Count non-overlapping occurrences of `needle`. */
static size_t
count_of(const char *hay, const char *needle)
{
    size_t n = 0;
    size_t nl = strlen(needle);
    for (const char *p = strstr(hay, needle); p != NULL;
         p = strstr(p + nl, needle)) {
        n++;
    }
    return n;
}

typedef enum {
    OMV_OK = 0,
    OMV_DUP_HELP,
    OMV_DUP_TYPE,
    OMV_DUP_UNIT,
    OMV_HELP_WITHOUT_TYPE,
    OMV_UNIT_WITHOUT_TYPE,
    OMV_UNIT_SUFFIX_MISMATCH,
    OMV_TYPE_WITHOUT_HELP,
    OMV_COUNTER_FAMILY_HAS_TOTAL,
    OMV_SAMPLE_ORPHAN,
    OMV_UNKNOWN_TYPE,
    OMV_LINE_TOO_LONG,
    OMV_EOF_MISSING,
    OMV_EOF_DUPLICATE,
    OMV_EOF_NOT_TERMINAL,
    OMV_TOO_MANY_FAMILIES,
} omv_t;

static const char *
omv_name(omv_t v)
{
    switch (v) {
    case OMV_OK: return "OK";
    case OMV_DUP_HELP: return "DUP_HELP";
    case OMV_DUP_TYPE: return "DUP_TYPE";
    case OMV_DUP_UNIT: return "DUP_UNIT";
    case OMV_HELP_WITHOUT_TYPE: return "HELP_WITHOUT_TYPE";
    case OMV_UNIT_WITHOUT_TYPE: return "UNIT_WITHOUT_TYPE";
    case OMV_UNIT_SUFFIX_MISMATCH: return "UNIT_SUFFIX_MISMATCH";
    case OMV_TYPE_WITHOUT_HELP: return "TYPE_WITHOUT_HELP";
    case OMV_COUNTER_FAMILY_HAS_TOTAL: return "COUNTER_FAMILY_HAS_TOTAL";
    case OMV_SAMPLE_ORPHAN: return "SAMPLE_ORPHAN";
    case OMV_UNKNOWN_TYPE: return "UNKNOWN_TYPE";
    case OMV_LINE_TOO_LONG: return "LINE_TOO_LONG";
    case OMV_EOF_MISSING: return "EOF_MISSING";
    case OMV_EOF_DUPLICATE: return "EOF_DUPLICATE";
    case OMV_EOF_NOT_TERMINAL: return "EOF_NOT_TERMINAL";
    case OMV_TOO_MANY_FAMILIES: return "TOO_MANY_FAMILIES";
    default: return "?";
    }
}

#define OMV_MAX_FAMILIES 256u
#define OMV_NAME_MAX     192u

typedef struct {
    char     name[OMV_NAME_MAX];
    char     type[32];
    unsigned helps;
    unsigned types;
    unsigned units;
} omv_family_t;

typedef struct {
    omv_family_t fam[OMV_MAX_FAMILIES];
    unsigned     count;
} omv_reg_t;

static omv_family_t *
omv_find(omv_reg_t *r, const char *name)
{
    for (unsigned i = 0; i < r->count; i++) {
        if (strcmp(r->fam[i].name, name) == 0) {
            return &r->fam[i];
        }
    }
    return NULL;
}

static omv_family_t *
omv_intern(omv_reg_t *r, const char *name)
{
    omv_family_t *f = omv_find(r, name);
    if (f != NULL) {
        return f;
    }
    if (r->count >= OMV_MAX_FAMILIES || strlen(name) >= OMV_NAME_MAX) {
        return NULL;
    }
    f = &r->fam[r->count++];
    memset(f, 0, sizeof(*f));
    snprintf(f->name, sizeof(f->name), "%s", name);
    return f;
}

/*
 * The metric types this renderer emits, and for each the sample shape that is
 * legal for it. Resolution is TYPE-DIRECTED: a direct family-name sample is
 * legal only for a type whose text representation actually uses one. Treating
 * "not a counter" as "direct sample allowed" would accept a bare
 * `moqrelay_forward_latency_seconds` line, which no histogram may emit.
 */
typedef struct {
    const char *type;
    bool        direct;  /* sample may be the family name itself */
    const char *sfx[4];  /* NULL-terminated legal suffixes       */
} omv_type_rule_t;

static const omv_type_rule_t k_omv_types[] = {
    { "gauge", true, { NULL, NULL, NULL, NULL } },
    { "counter", false, { "_total", NULL, NULL, NULL } },
    { "histogram", false, { "_bucket", "_count", "_sum", NULL } },
};

static const omv_type_rule_t *
omv_rule(const char *type)
{
    for (size_t i = 0; i < sizeof(k_omv_types) / sizeof(k_omv_types[0]); i++) {
        if (strcmp(k_omv_types[i].type, type) == 0) {
            return &k_omv_types[i];
        }
    }
    return NULL;
}

static bool
omv_rule_allows_suffix(const omv_type_rule_t *r, const char *sfx)
{
    for (size_t i = 0; i < 4u && r->sfx[i] != NULL; i++) {
        if (strcmp(r->sfx[i], sfx) == 0) {
            return true;
        }
    }
    return false;
}

/* Resolve a sample name to its declared family under that family's own type
 * rule. Every family must already be declared (metadata pass ran first). */
static bool
omv_sample_resolves(omv_reg_t *r, const char *sample)
{
    omv_family_t *f = omv_find(r, sample);
    if (f != NULL && f->types > 0) {
        const omv_type_rule_t *rule = omv_rule(f->type);
        if (rule != NULL && rule->direct) {
            return true;
        }
        /* Declared, but this type does not use a direct sample. Fall through:
         * the name could still be a suffixed sample of another family. */
    }
    static const char *const sfx[] = { "_total", "_bucket", "_count", "_sum" };
    size_t slen = strlen(sample);
    for (size_t k = 0; k < sizeof(sfx) / sizeof(sfx[0]); k++) {
        size_t xl = strlen(sfx[k]);
        if (slen <= xl || strcmp(sample + slen - xl, sfx[k]) != 0) {
            continue;
        }
        char base[OMV_NAME_MAX];
        if (slen - xl >= sizeof(base)) {
            continue;
        }
        memcpy(base, sample, slen - xl);
        base[slen - xl] = '\0';
        omv_family_t *bf = omv_find(r, base);
        if (bf == NULL || bf->types == 0) {
            continue;
        }
        const omv_type_rule_t *rule = omv_rule(bf->type);
        if (rule != NULL && omv_rule_allows_suffix(rule, sfx[k])) {
            return true;
        }
    }
    return false;
}


static const char *
unit_of(const char *family)
{
    static const char *const units[] = { "seconds", "bytes", "ratio" };
    size_t fl = strlen(family);
    for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); i++) {
        size_t ul = strlen(units[i]);
        if (fl > ul + 1 && family[fl - ul - 1] == '_' &&
            strcmp(family + fl - ul, units[i]) == 0) {
            return units[i];
        }
    }
    return NULL;
}

/*
 * Validate an OpenMetrics document. Two passes: metadata first so every
 * family is declared before samples are resolved against it, then samples.
 * `detail` names the offending family so a failure is actionable.
 */
static omv_t
omv_validate(const char *doc, size_t n, char *detail, size_t dcap)
{
    static omv_reg_t reg;
    memset(&reg, 0, sizeof(reg));
    if (detail != NULL && dcap > 0) {
        detail[0] = '\0';
    }

    /* EOF: present exactly once, and terminal. */
    size_t eofs = count_of(doc, "# EOF\n");
    if (eofs == 0) {
        return OMV_EOF_MISSING;
    }
    if (eofs > 1) {
        return OMV_EOF_DUPLICATE;
    }
    if (n < 6 || memcmp(doc + n - 6, "# EOF\n", 6) != 0) {
        return OMV_EOF_NOT_TERMINAL;
    }

    for (int pass = 0; pass < 2; pass++) {
        for (const char *line = doc; *line != '\0';) {
            const char *eol = strchr(line, '\n');
            if (eol == NULL) {
                break;
            }
            size_t llen = (size_t)(eol - line);
            char buf[1024];
            if (llen >= sizeof(buf)) {
                /* A bounded validator must refuse over-limit input, not skip
                 * it: an orphan or malformed sample could otherwise evade
                 * every check simply by being long. */
                return OMV_LINE_TOO_LONG;
            }
            memcpy(buf, line, llen);
            buf[llen] = '\0';
            line = eol + 1;

            bool meta = (strncmp(buf, "# ", 2) == 0);
            if ((pass == 0) != meta) {
                continue;
            }

            if (pass == 0) {
                char kind[16], fam[OMV_NAME_MAX];
                if (sscanf(buf, "# %15s %191s", kind, fam) != 2) {
                    continue;
                }
                if (strcmp(kind, "EOF") == 0) {
                    continue;
                }
                omv_family_t *f = omv_intern(&reg, fam);
                if (f == NULL) {
                    return OMV_TOO_MANY_FAMILIES;
                }
                if (detail != NULL) {
                    snprintf(detail, dcap, "%s", fam);
                }
                if (strcmp(kind, "HELP") == 0) {
                    if (++f->helps > 1) {
                        return OMV_DUP_HELP;
                    }
                } else if (strcmp(kind, "TYPE") == 0) {
                    if (++f->types > 1) {
                        return OMV_DUP_TYPE;
                    }
                    char t[32];
                    if (sscanf(buf, "# TYPE %*s %31s", t) != 1 ||
                        omv_rule(t) == NULL) {
                        return OMV_UNKNOWN_TYPE;
                    }
                    snprintf(f->type, sizeof(f->type), "%s", t);
                    size_t fl = strlen(fam);
                    if (strcmp(f->type, "counter") == 0 && fl > 6 &&
                        strcmp(fam + fl - 6, "_total") == 0) {
                        return OMV_COUNTER_FAMILY_HAS_TOTAL;
                    }
                } else if (strcmp(kind, "UNIT") == 0) {
                    if (++f->units > 1) {
                        return OMV_DUP_UNIT;
                    }
                    char u[64];
                    const char *want = unit_of(fam);
                    if (sscanf(buf, "# UNIT %*s %63s", u) != 1 ||
                        want == NULL || strcmp(u, want) != 0) {
                        return OMV_UNIT_SUFFIX_MISMATCH;
                    }
                }
            } else {
                char sample[OMV_NAME_MAX];
                size_t k = 0;
                while (buf[k] != '\0' && buf[k] != '{' && buf[k] != ' ' &&
                       k + 1 < sizeof(sample)) {
                    sample[k] = buf[k];
                    k++;
                }
                sample[k] = '\0';
                if (k == 0) {
                    continue;
                }
                if (!omv_sample_resolves(&reg, sample)) {
                    if (detail != NULL) {
                        snprintf(detail, dcap, "%s", sample);
                    }
                    return OMV_SAMPLE_ORPHAN;
                }
            }
        }
    }

    /* Every declared family must carry both metadata lines. */
    for (unsigned i = 0; i < reg.count; i++) {
        if (detail != NULL) {
            snprintf(detail, dcap, "%s", reg.fam[i].name);
        }
        if (reg.fam[i].types == 0) {
            return OMV_HELP_WITHOUT_TYPE;
        }
        if (reg.fam[i].helps == 0) {
            return OMV_TYPE_WITHOUT_HELP;
        }
        if (reg.fam[i].units > 0 && reg.fam[i].types == 0) {
            return OMV_UNIT_WITHOUT_TYPE;
        }
    }
    if (detail != NULL && dcap > 0) {
        detail[0] = '\0';
    }
    return OMV_OK;
}

static size_t
render_om_document(char *out, size_t cap)
{
    enum { LANES = 4u };
    static moqr_core_stats_t cs[LANES];
    static moqr_bind_stats_t bs[LANES];
    static moqr_shards_stats_t sh[LANES];
    static moqr_snapshot_view_t vs[LANES];
    static char labbuf[2u * LANES][80];

    seeded_views(20260902u, LANES, vs, cs, bs, sh, labbuf, true, true);
    for (uint32_t i = 0; i < LANES; i++) {
        vs[i].bind = &bs[i];
        vs[i].shard = &sh[i];
        vs[i].labels.shard = (uint16_t)i;
        vs[i].labels.transport = (i % 2u) ? "wtquic-msquic" : "msquic";
        vs[i].labels.version = "moqt-18+moqt-16";
    }
    size_t n = 0;
    if (moqr_metrics_write_multi_ex(vs, LANES, MOQR_OBS_FMT_OPENMETRICS_100,
                                    out, cap, &n) != MOQR_OK) {
        return 0;
    }
    return n;
}

static int
test_openmetrics_document_is_structurally_valid(void)
{
    int failures = 0;
    static char om[4u << 20];
    size_t n = render_om_document(om, sizeof(om));
    MOQ_TEST_CHECK(n > 0);

    char detail[256];
    omv_t v = omv_validate(om, n, detail, sizeof(detail));
    if (v != OMV_OK) {
        fprintf(stderr, "FAIL: OpenMetrics document invalid: %s (%s)\n",
                omv_name(v), detail);
        failures++;
    }
    return failures;
}

/*
 * The oracle itself must be load-bearing. Each mutation is applied to the
 * DOCUMENT and must be rejected for its own specific reason -- an oracle that
 * rejected everything through one catch-all check would pass a weaker test
 * but catch nothing precisely.
 */
static int
expect_reject(char *doc, size_t n, omv_t want, const char *what)
{
    char detail[256];
    /* The mutation buffer is reused, so terminate at the intended length:
     * bytes left over from a longer previous case would otherwise be read as
     * part of this document. */
    doc[n] = '\0';
    omv_t got = omv_validate(doc, n, detail, sizeof(detail));
    if (got == want) {
        return 0;
    }
    fprintf(stderr, "FAIL: %s -> %s, expected %s (%s)\n", what, omv_name(got),
            omv_name(want), detail);
    return 1;
}

static int
test_openmetrics_oracle_rejects_document_mutants(void)
{
    int failures = 0;
    static char base[4u << 20];
    static char mut[4u << 20];
    size_t bn = render_om_document(base, sizeof(base));
    MOQ_TEST_CHECK(bn > 0);

    /* A clean document is accepted, so the rejections below mean something. */
    failures += expect_reject(base, bn, OMV_OK, "unmutated document");

    /* Duplicate HELP for one family. */
    {
        const char *ins = "# HELP moqrelay_bindings Live session bindings.\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_DUP_HELP, "duplicate HELP");
    }

    /* HELP naming a family that is never declared. */
    {
        const char *ins = "# HELP moqrelay_not_a_family Orphaned.\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_HELP_WITHOUT_TYPE,
                                  "orphan HELP");
    }

    /* Duplicate UNIT for one family. */
    {
        const char *ins =
            "# UNIT moqrelay_forward_latency_seconds seconds\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_DUP_UNIT, "duplicate UNIT");
    }

    /* A UNIT whose value is not the family's own suffix. */
    {
        const char *ins = "# UNIT moqrelay_forward_latency_seconds bytes\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_UNIT_SUFFIX_MISMATCH,
                                  "UNIT suffix mismatch");
    }

    /* A sample belonging to no declared family. */
    {
        const char *ins = "moqrelay_ghost_total{shard=\"0\"} 1\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_SAMPLE_ORPHAN,
                                  "orphan sample");
    }

    /* A counter family that wrongly keeps the sample suffix. */
    {
        const char *ins =
            "# HELP moqrelay_ghost_total G.\n# TYPE moqrelay_ghost_total counter\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_COUNTER_FAMILY_HAS_TOTAL,
                                  "counter family keeps _total");
    }

    /* A histogram sample using the bare family name. Histograms have no
     * direct sample, so this must be rejected as an orphan -- the case that
     * previously passed. */
    {
        const char *ins =
            "moqrelay_forward_latency_seconds{shard=\"0\"} 1\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_SAMPLE_ORPHAN,
                                  "bare histogram sample");
    }

    /* A gauge sample by its family name stays legal, so the rule above is
     * type-directed rather than a blanket ban on direct samples. */
    {
        const char *ins = "moqrelay_bindings{shard=\"0\"} 1\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_OK, "direct gauge sample");
    }

    /* An unsupported TYPE token. */
    {
        const char *ins =
            "# HELP moqrelay_ghost G.\n# TYPE moqrelay_ghost summary\n";
        size_t il = strlen(ins);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, ins, il);
        failures += expect_reject(mut, bn + il, OMV_UNKNOWN_TYPE,
                                  "unsupported TYPE token");
    }

    /* An over-limit line must be refused, not skipped. */
    {
        static char longline[2048];
        memset(longline, 'x', sizeof(longline) - 2u);
        longline[0] = 'm';
        longline[sizeof(longline) - 2u] = '\n';
        longline[sizeof(longline) - 1u] = '\0';
        size_t il = strlen(longline);
        memcpy(mut, base, bn);
        memmove(mut + il, mut, bn);
        memcpy(mut, longline, il);
        failures += expect_reject(mut, bn + il, OMV_LINE_TOO_LONG,
                                  "over-limit line");
    }

    /* EOF removed, duplicated, and made non-terminal. */
    {
        memcpy(mut, base, bn);
        failures += expect_reject(mut, bn - 6u, OMV_EOF_MISSING, "EOF removed");

        memcpy(mut, base, bn);
        memcpy(mut + bn, "# EOF\n", 6);
        failures += expect_reject(mut, bn + 6u, OMV_EOF_DUPLICATE,
                                  "EOF duplicated");

        memcpy(mut, base, bn);
        memcpy(mut + bn, "moqrelay_bindings{shard=\"0\"} 1\n", 31);
        failures += expect_reject(mut, bn + 31u, OMV_EOF_NOT_TERMINAL,
                                  "EOF not terminal");
    }
    return failures;
}


/*
 * The bound must not confuse its two MOQR_ERR_CAPACITY sources. Truncation
 * against the one-byte sink is the ordinary path and carries the exact
 * length; a poisoned accumulation returns the same code but no usable length,
 * and collapsing them would hand a caller a zero-byte allocation.
 */
static int
test_bound_refuses_poison(void)
{
    int failures = 0;
    uint64_t out = 12345;

    /* Ordinary sink truncation: exact bound, success. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_bound_finalize(MOQR_ERR_CAPACITY, false, 4096u, &out),
        MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(out, 4096u);

    /* A render that fit outright is equally fine. */
    out = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_bound_finalize(MOQR_OK, false, 512u, &out), MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(out, 512u);

    /* Poison saturates and refuses, per the public contract. */
    out = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_bound_finalize(MOQR_ERR_CAPACITY, true, 0u, &out),
        MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_U64(out, UINT64_MAX);

    /* Poison wins even when a length looks plausible. */
    out = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_bound_finalize(MOQR_ERR_CAPACITY, true, 4096u, &out),
        MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_U64(out, UINT64_MAX);

    /* A genuine error is still propagated unchanged. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_bound_finalize(MOQR_ERR_INVAL, false, 0u, &out),
        MOQR_ERR_INVAL);
    return failures;
}

/*
 * The discriminator has to survive the whole way from the writer to the bound
 * decision, so both ends are pinned: mw_finish must report each outcome, and
 * a real render must actually write through the pointer rather than leaving
 * it untouched -- either failure would let a poisoned document reach the
 * bound looking healthy.
 */
static int
test_poison_discriminator_is_plumbed(void)
{
    int failures = 0;
    bool poisoned = false;
    size_t written = 0;

    /* mw_finish, healthy: reports false and the real length. */
    poisoned = true;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_finish_reports_poison(false, &poisoned, &written),
        MOQR_OK);
    MOQ_TEST_CHECK(!poisoned);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)written, 10u);

    /* mw_finish, poisoned: reports true and refuses. */
    poisoned = false;
    written = 999;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_obs_test_finish_reports_poison(true, &poisoned, &written),
        MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(poisoned);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)written, 0u);

    /* A real render writes through the pointer: pre-set true must be cleared,
     * so a writer that drops the pointer is caught. */
    moqr_core_stats_t cs = core_max();
    moqr_shards_stats_t sh = shard_max();
    moqr_snapshot_view_t v;
    memset(&v, 0, sizeof(v));
    v.core = &cs;
    v.shard = &sh;
    v.labels = (moqr_obs_labels_t){ 0, "msquic", "moqt-18" };

    static char buf[262144];
    poisoned = true;
    written = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_obs_test_write_multi_poison_flag(
                              &v, 1, buf, sizeof(buf), &written, &poisoned),
                          MOQR_OK);
    MOQ_TEST_CHECK(!poisoned);
    MOQ_TEST_CHECK(written > 0);
    return failures;
}

/*
 * "Differ exactly by the extended series" as a proof rather than an
 * assertion: strip the local wake samples from the extended document and
 * restore the frozen help wording, then require the remainder to be
 * byte-identical to the legacy document.
 */
/* Local substring search over a counted buffer. memmem() is a GNU/BSD
 * extension that needs _GNU_SOURCE on glibc, which this strict-C11 lane does
 * not define. */
static const char *
find_bytes(const char *hay, size_t hn, const char *needle, size_t nn)
{
    if (nn == 0 || hn < nn) {
        return NULL;
    }
    for (size_t i = 0; i + nn <= hn; i++) {
        if (memcmp(hay + i, needle, nn) == 0) {
            return hay + i;
        }
    }
    return NULL;
}

/* What the normalization removed, so the test can require the exact delta
 * rather than "whatever happened to carry a local label". */
typedef struct {
    unsigned dropped_per_view;
    unsigned dropped_process;
    unsigned dropped_other;
    unsigned help_rewritten;
} norm_stats_t;

static const char kWakeFamily[] = "moqrelay_wake_requests_total";
static const char kWakeProcFamily[] = "moqrelay_process_wake_requests_total";

/* Sample or metadata name at the head of a line, up to '{' or a space. */
static size_t
line_name(const char *line, size_t llen, char *out, size_t cap)
{
    size_t k = 0;
    while (k < llen && line[k] != '{' && line[k] != ' ' && line[k] != '\n' &&
           k + 1 < cap) {
        out[k] = line[k];
        k++;
    }
    out[k] = '\0';
    return k;
}

/*
 * Project the extended document back onto the frozen one: remove the local
 * wake samples of the two known wake families, and restore the frozen help
 * wording on those two families' HELP records. Everything is matched by exact
 * family name -- a future unrelated family carrying a `cause="local"` label
 * must NOT be normalized away, or this proof would quietly stop proving
 * anything.
 */
static size_t
normalize_extended(const char *src, size_t n, char *dst, size_t cap,
                   norm_stats_t *st)
{
    static const char kExtHelp[] =
        "Wake requests, by cause (mask-level: one per destination per step; "
        "the local cause is a self-wake and crosses no shard).";
    static const char kLegacyHelp[] =
        "Cross-shard wake requests, by cause (mask-level: one per destination "
        "per step).";
    memset(st, 0, sizeof(*st));

    size_t o = 0;
    size_t i = 0;
    while (i < n) {
        const char *eol = memchr(src + i, '\n', n - i);
        size_t llen = (eol != NULL) ? (size_t)(eol - (src + i)) + 1u : n - i;
        const char *line = src + i;

        char name[OMV_NAME_MAX];
        line_name(line, llen, name, sizeof(name));

        if (find_bytes(line, llen, "cause=\"local\"", 13) != NULL) {
            if (strcmp(name, kWakeFamily) == 0) {
                st->dropped_per_view++;
                i += llen;
                continue;
            }
            if (strcmp(name, kWakeProcFamily) == 0) {
                st->dropped_process++;
                i += llen;
                continue;
            }
            /* Some other family carries a local label: keep it, and let the
             * byte comparison fail loudly. */
            st->dropped_other++;
        }

        /* Match the WHOLE family token. A length-limited compare would also
         * claim any family whose name merely begins with an accepted one
         * (`..._total_extra`), laundering it. line_name() stops at the
         * delimiter, so the comparison below is exact by construction. */
        bool is_wake_help = false;
        if (llen > 7u && strncmp(line, "# HELP ", 7) == 0) {
            char fam[OMV_NAME_MAX];
            line_name(line + 7, llen - 7u, fam, sizeof(fam));
            is_wake_help = (strcmp(fam, kWakeFamily) == 0 ||
                            strcmp(fam, kWakeProcFamily) == 0);
        }
        const char *hit =
            is_wake_help
                ? find_bytes(line, llen, kExtHelp, sizeof(kExtHelp) - 1u)
                : NULL;
        if (hit != NULL) {
            size_t pre = (size_t)(hit - line);
            size_t post = llen - pre - (sizeof(kExtHelp) - 1u);
            if (o + pre + sizeof(kLegacyHelp) - 1u + post > cap) {
                return 0;
            }
            memcpy(dst + o, line, pre);
            o += pre;
            memcpy(dst + o, kLegacyHelp, sizeof(kLegacyHelp) - 1u);
            o += sizeof(kLegacyHelp) - 1u;
            memcpy(dst + o, hit + sizeof(kExtHelp) - 1u, post);
            o += post;
            st->help_rewritten++;
            i += llen;
            continue;
        }
        if (o + llen > cap) {
            return 0;
        }
        memcpy(dst + o, line, llen);
        o += llen;
        i += llen;
    }
    return o;
}

/*
 * The normalization must key on the two known wake families, not on the mere
 * presence of a local label. Nothing in today's document exercises that, so
 * feed it a synthetic one: an unrelated family carrying cause="local" has to
 * survive normalization, or a future family could be laundered away while
 * this proof still reported "only the known delta".
 */
static int
test_normalization_keys_on_family_not_label(void)
{
    int failures = 0;
    static const char doc[] =
        "# HELP moqrelay_wake_requests_total Wake requests, by cause "
        "(mask-level: one per destination per step; the local cause is a "
        "self-wake and crosses no shard).\n"
        "# TYPE moqrelay_wake_requests_total counter\n"
        "moqrelay_wake_requests_total{shard=\"0\",cause=\"push\"} 1\n"
        "moqrelay_wake_requests_total{shard=\"0\",cause=\"local\"} 2\n"
        "# HELP moqrelay_other_total Unrelated.\n"
        "# TYPE moqrelay_other_total counter\n"
        "moqrelay_other_total{shard=\"0\",cause=\"local\"} 7\n"
        /* A different family that happens to quote the extended wake help
         * text. Only the two wake families may have their HELP rewritten, so
         * this one must be left exactly as it is. */
        "# HELP moqrelay_mimic_total Wake requests, by cause (mask-level: "
        "one per destination per step; the local cause is a self-wake and "
        "crosses no shard).\n"
        "# TYPE moqrelay_mimic_total counter\n"
        /* Prefix collisions: each of these BEGINS with an accepted wake
         * family name and quotes the extended sentence, so a comparison that
         * stops before the delimiter would rewrite them. The family token
         * must match whole, delimiter included. */
        "# HELP moqrelay_wake_requests_total_extra Wake requests, by cause "
        "(mask-level: one per destination per step; the local cause is a "
        "self-wake and crosses no shard).\n"
        "# TYPE moqrelay_wake_requests_total_extra counter\n"
        "# HELP moqrelay_process_wake_requests_total_extra Wake requests, by "
        "cause (mask-level: one per destination per step; the local cause is "
        "a self-wake and crosses no shard).\n"
        "# TYPE moqrelay_process_wake_requests_total_extra counter\n"
        /* The same collision on the SAMPLE path: a local-labelled sample of
         * a prefix-colliding family must be preserved, not dropped. */
        "moqrelay_wake_requests_total_extra{shard=\"0\",cause=\"local\"} 3\n";

    static char out[4096];
    norm_stats_t st;
    size_t n = normalize_extended(doc, sizeof(doc) - 1u, out, sizeof(out),
                                  &st);
    MOQ_TEST_CHECK(n > 0);
    out[n] = '\0';

    /* The wake family's local sample is removed... */
    MOQ_TEST_CHECK_EQ_U64(st.dropped_per_view, 1u);
    MOQ_TEST_CHECK(strstr(out, "moqrelay_wake_requests_total{shard=\"0\","
                               "cause=\"local\"}") == NULL);
    /* ...and its HELP is restored to the frozen wording. */
    MOQ_TEST_CHECK_EQ_U64(st.help_rewritten, 1u);
    MOQ_TEST_CHECK(strstr(out, "Cross-shard wake requests") != NULL);

    /* Unrelated local-labelled samples are counted and PRESERVED: the plain
     * one, and the prefix-colliding one. */
    MOQ_TEST_CHECK_EQ_U64(st.dropped_other, 2u);
    MOQ_TEST_CHECK(strstr(out, "moqrelay_wake_requests_total_extra{shard="
                               "\"0\",cause=\"local\"} 3") != NULL);
    MOQ_TEST_CHECK(strstr(out, "moqrelay_other_total{shard=\"0\","
                               "cause=\"local\"} 7") != NULL);
    /* Its HELP is left alone. */
    MOQ_TEST_CHECK(strstr(out, "# HELP moqrelay_other_total Unrelated.")
                   != NULL);

    /* And a family merely quoting the extended wake text keeps it: the
     * rewrite is keyed on the family name, not on the sentence. */
    MOQ_TEST_CHECK(strstr(out, "# HELP moqrelay_mimic_total Wake requests")
                   != NULL);

    /* Neither prefix-collision family may be rewritten: matching an accepted
     * name only up to its length would claim both. Exactly one HELP record
     * was rewritten overall -- the real wake family's. */
    MOQ_TEST_CHECK(strstr(out,
                          "# HELP moqrelay_wake_requests_total_extra Wake "
                          "requests") != NULL);
    MOQ_TEST_CHECK(strstr(out,
                          "# HELP moqrelay_process_wake_requests_total_extra "
                          "Wake requests") != NULL);
    MOQ_TEST_CHECK(strstr(out, "# HELP moqrelay_wake_requests_total_extra "
                               "Cross-shard") == NULL);
    MOQ_TEST_CHECK(strstr(out,
                          "# HELP moqrelay_process_wake_requests_total_extra "
                          "Cross-shard") == NULL);
    return failures;
}

static int
test_extended_differs_only_by_known_delta(void)
{
    int failures = 0;
    static moqr_core_stats_t cs[4];
    static moqr_bind_stats_t bs[4];
    static moqr_shards_stats_t sh[4];
    static moqr_snapshot_view_t vs[4];
    static char labbuf[8][80];
    static char legacy[1u << 20];
    static char ext[1u << 20];
    static char norm[1u << 20];

    seeded_views(7u, 4u, vs, cs, bs, sh, labbuf, true, true);
    for (uint32_t i = 0; i < 4u; i++) {
        vs[i].bind = &bs[i];
        vs[i].shard = &sh[i];
    }

    size_t ln = 0, en = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_prometheus_multi(
                              vs, 4u, legacy, sizeof(legacy), &ln),
                          MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_metrics_write_multi_ex(
                              vs, 4u, MOQR_OBS_FMT_PROMETHEUS_004, ext,
                              sizeof(ext), &en),
                          MOQR_OK);
    MOQ_TEST_CHECK(en > ln);

    norm_stats_t st;
    size_t nn = normalize_extended(ext, en, norm, sizeof(norm), &st);
    MOQ_TEST_CHECK(nn > 0);

    /* The delta is exactly what it is supposed to be: one local sample per
     * view carrying shard stats, one process aggregate, two rewritten HELP
     * records, and nothing else touched. */
    unsigned with_shard = 0;
    for (uint32_t i = 0; i < 4u; i++) {
        if (vs[i].shard != NULL) {
            with_shard++;
        }
    }
    MOQ_TEST_CHECK_EQ_U64(st.dropped_per_view, with_shard);
    MOQ_TEST_CHECK_EQ_U64(st.dropped_process, 1u);
    MOQ_TEST_CHECK_EQ_U64(st.dropped_other, 0u);
    MOQ_TEST_CHECK_EQ_U64(st.help_rewritten, 2u);

    MOQ_TEST_CHECK_EQ_U64((uint64_t)nn, (uint64_t)ln);
    MOQ_TEST_CHECK(nn == ln && memcmp(norm, legacy, ln) == 0);
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_p004_single_has_no_extended_series();
    failures += test_p004_multi_legacy_omits_extended_series();
    failures += test_openmetrics_shape();
    failures += test_openmetrics_unit_metadata();
    failures += test_unknown_format_refused();
    failures += test_bound_holds_every_capability();
    failures += test_bound_is_exactly_achieved();
    failures += test_bound_rejects_bad_args();
    failures += test_render_does_not_reset_highwater();
    failures += test_wake_requests_local_series();
    failures += test_length_accumulation_is_checked();
    failures += test_bound_refuses_poison();
    failures += test_poison_discriminator_is_plumbed();
    failures += test_normalization_keys_on_family_not_label();
    failures += test_extended_differs_only_by_known_delta();
    failures += test_seeded_snapshots_never_exceed_bound();
    failures += test_openmetrics_document_is_structurally_valid();
    failures += test_openmetrics_oracle_rejects_document_mutants();
    if (failures != 0) {
        fprintf(stderr, "test_relay_obs_format: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_relay_obs_format: OK\n");
    return 0;
}
