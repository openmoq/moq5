/* Observability serializers: Prometheus text + trace JSONL (pure renders). */

#include <moq/relay/moqr_obs.h>
#include <moq/relay/trace.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

#define HAS(hay, needle) (strstr((hay), (needle)) != NULL)

/* A representative, fully-populated core snapshot. */
static moqr_core_stats_t
sample_core(void)
{
    moqr_core_stats_t cs;
    memset(&cs, 0, sizeof(cs));
    cs.ingested_total = 100;
    cs.delivered_total = 250;
    cs.evicted_total = 7;
    cs.bindings = 3;
    cs.tracks = 5;
    cs.subs = 8;
    cs.subs_parked = 2;
    cs.subs_active = 6;
    cs.ns_nodes = 4;
    cs.ns_subs = 2;
    cs.retained_bytes = 4096;
    cs.intent_highwater = 9;
    cs.route_epoch = 42;
    cs.refusals[MOQR_REFUSE_SUBS] = 3;
    cs.refusals[MOQR_REFUSE_NAME_BYTES] = 1;
    cs.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_ALLOW] = 12;
    cs.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY] = 3;
    cs.auth_decisions[MOQR_AUTH_PUBLISH][MOQR_AUTH_DEFER] = 1;
    cs.auth_denials[MOQR_AUTH_REASON_UNSCOPED] = 3;
    return cs;
}

static int
test_prometheus_format(void)
{
    int failures = 0;
    moqr_core_stats_t cs = sample_core();
    moqr_bind_stats_t bs;
    memset(&bs, 0, sizeof(bs));
    bs.conns = 3;
    bs.events_translated = 500;
    bs.deliveries_written = 250;
    bs.ingest_refusals = 4;
    bs.session_errors = 1;
    moqr_obs_labels_t lb = { 0, "picoquic", "draft-18" };

    char buf[16384];
    size_t written = 0;
    moqr_result_t rc =
        moqr_metrics_write_prometheus(&cs, &bs, &lb, buf, sizeof(buf),
                                      &written);
    MOQ_TEST_CHECK(rc == MOQR_OK);
    MOQ_TEST_CHECK(written > 0 && written < sizeof(buf));
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), written);

    /* HELP/TYPE metadata present and typed correctly. */
    MOQ_TEST_CHECK(HAS(buf,
        "# TYPE moqrelay_objects_ingested_total counter"));
    MOQ_TEST_CHECK(HAS(buf, "# TYPE moqrelay_subscriptions gauge"));
    MOQ_TEST_CHECK(HAS(buf, "# TYPE moqrelay_route_epoch gauge"));
    MOQ_TEST_CHECK(HAS(buf, "# HELP moqrelay_refusals_total"));

    /* Base labels + values on representative series. */
    const char *L = "shard=\"0\",transport=\"picoquic\",version=\"draft-18\"";
    char line[256];
    snprintf(line, sizeof(line),
             "moqrelay_objects_ingested_total{%s} 100", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_objects_delivered_total{%s} 250", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_objects_evicted_total{%s} 7", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line), "moqrelay_retained_bytes{%s} 4096", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_intent_ring_highwater{%s} 9", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line), "moqrelay_route_epoch{%s} 42", L);
    MOQ_TEST_CHECK(HAS(buf, line));

    /* Refusals fan out by reason, including the zero series (so a graph
     * shows the whole vocabulary, not just non-zero reasons). */
    snprintf(line, sizeof(line),
             "moqrelay_refusals_total{%s,reason=\"subs\"} 3", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_refusals_total{%s,reason=\"name_bytes\"} 1", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_refusals_total{%s,reason=\"tracks\"} 0", L);
    MOQ_TEST_CHECK(HAS(buf, line));

    /* Auth decisions fan out by action × verdict, including zero series so a
     * dashboard shows the whole matrix, not just what has fired. */
    MOQ_TEST_CHECK(HAS(buf, "# TYPE moqrelay_auth_decisions_total counter"));
    snprintf(line, sizeof(line),
             "moqrelay_auth_decisions_total{%s,action=\"subscribe\","
             "verdict=\"allow\"} 12", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_auth_decisions_total{%s,action=\"subscribe\","
             "verdict=\"deny\"} 3", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_auth_decisions_total{%s,action=\"publish\","
             "verdict=\"defer\"} 1", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_auth_decisions_total{%s,action=\"fetch\","
             "verdict=\"allow\"} 0", L);
    MOQ_TEST_CHECK(HAS(buf, line));   /* zero series present */
    /* Denials broken out by reason. */
    MOQ_TEST_CHECK(HAS(buf, "# TYPE moqrelay_auth_denials_total counter"));
    snprintf(line, sizeof(line),
             "moqrelay_auth_denials_total{%s,reason=\"unscoped\"} 3", L);
    MOQ_TEST_CHECK(HAS(buf, line));

    /* Subscriptions split by state. */
    snprintf(line, sizeof(line),
             "moqrelay_subscriptions{%s,state=\"parked\"} 2", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_subscriptions{%s,state=\"active\"} 6", L);
    MOQ_TEST_CHECK(HAS(buf, line));

    /* Binding-tier series present. */
    snprintf(line, sizeof(line), "moqrelay_connections{%s} 3", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_session_errors_total{%s} 1", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_ingest_refusals_total{%s} 4", L);
    MOQ_TEST_CHECK(HAS(buf, line));

    MOQ_TEST_PASS("prometheus_format");
    return failures;
}

static int
test_prometheus_core_only(void)
{
    int failures = 0;
    moqr_core_stats_t cs = sample_core();
    moqr_obs_labels_t lb = { 1, "sim", "draft-16" };
    char buf[16384];
    size_t written = 0;
    moqr_result_t rc =
        moqr_metrics_write_prometheus(&cs, NULL, &lb, buf, sizeof(buf),
                                      &written);
    MOQ_TEST_CHECK(rc == MOQR_OK);
    /* Core series present with the shard/transport labels for this call. */
    MOQ_TEST_CHECK(HAS(buf,
        "moqrelay_objects_ingested_total{shard=\"1\",transport=\"sim\","
        "version=\"draft-16\"} 100"));
    /* No binding-tier series when bind is NULL. */
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_connections"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_session_errors_total"));
    MOQ_TEST_PASS("prometheus_core_only");
    return failures;
}

static int
test_prometheus_truncation(void)
{
    int failures = 0;
    moqr_core_stats_t cs = sample_core();
    moqr_obs_labels_t lb = { 0, "picoquic", "draft-18" };

    /* First get the full size. */
    char big[16384];
    size_t need = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus(&cs, NULL, &lb, big,
                                                 sizeof(big), &need) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(need > 0);

    /* A buffer exactly one short of (content + NUL) must report CAPACITY and
     * still tell the caller the full size needed. */
    char small[64];
    size_t written = 0;
    moqr_result_t rc =
        moqr_metrics_write_prometheus(&cs, NULL, &lb, small, sizeof(small),
                                      &written);
    MOQ_TEST_CHECK(rc == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_SIZE(written, need);      /* required size reported */
    MOQ_TEST_CHECK(small[sizeof(small) - 1] == '\0');   /* NUL-terminated */

    /* Growing to need + 1 succeeds. */
    char exact[16384];
    MOQ_TEST_CHECK(need + 1 <= sizeof(exact));
    written = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus(&cs, NULL, &lb, exact,
                                                 need + 1, &written) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(written, need);
    MOQ_TEST_PASS("prometheus_truncation");
    return failures;
}

static int
test_prometheus_label_escaping(void)
{
    int failures = 0;
    moqr_core_stats_t cs = sample_core();
    /* A transport value carrying a quote and backslash must be escaped so
     * the exposition stays parseable. (Not a real transport — a guard.) */
    moqr_obs_labels_t lb = { 0, "pi\"co\\q", "draft-18" };
    char buf[16384];
    size_t written = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus(&cs, NULL, &lb, buf,
                                                 sizeof(buf), &written) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "transport=\"pi\\\"co\\\\q\""));
    MOQ_TEST_PASS("prometheus_label_escaping");
    return failures;
}

static int
test_trace_jsonl(void)
{
    int failures = 0;
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_trace_t *t = NULL;
    MOQ_TEST_CHECK(moqr_trace_create(alloc, 16, &t) == MOQR_OK);

    moqr_epochs_t ep = { 0, 0, 5 };
    moqr_trace_set_epochs(t, ep);
    moqr_trace_rec_t r0;
    memset(&r0, 0, sizeof(r0));
    r0.kind = MOQR_TRACE_INGEST;
    r0.detail = 0;
    r0.e0 = 11;
    r0.e1 = 22;
    r0.e2 = 33;
    r0.e3 = 44;
    moqr_trace_emit(t, r0);

    moqr_trace_rec_t r1;
    memset(&r1, 0, sizeof(r1));
    r1.kind = MOQR_TRACE_CURSOR_RETIRE;
    r1.detail = 7;
    r1.e0 = 99;
    moqr_trace_emit(t, r1);

    char buf[2048];
    size_t written = 0;
    MOQ_TEST_CHECK(moqr_trace_write_jsonl(t, buf, sizeof(buf), &written) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(written > 0);
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), written);

    /* Oldest-first, one object per line, kind names resolved, scalars and
     * the epoch triple present. */
    MOQ_TEST_CHECK(HAS(buf,
        "{\"seq\":0,\"kind\":\"ingest\",\"detail\":0,"
        "\"node_epoch\":0,\"shard_epoch\":0,\"route_epoch\":5,"
        "\"e0\":11,\"e1\":22,\"e2\":33,\"e3\":44}"));
    MOQ_TEST_CHECK(HAS(buf,
        "{\"seq\":1,\"kind\":\"cursor_retire\",\"detail\":7,"
        "\"node_epoch\":0,\"shard_epoch\":0,\"route_epoch\":5,"
        "\"e0\":99,\"e1\":0,\"e2\":0,\"e3\":0}"));
    /* Exactly two lines. */
    int newlines = 0;
    for (size_t i = 0; i < written; i++) {
        if (buf[i] == '\n') {
            newlines++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(newlines, 2);

    /* Truncation contract mirrors the metrics writer. */
    char tiny[8];
    size_t need = 0;
    moqr_result_t rc = moqr_trace_write_jsonl(t, tiny, sizeof(tiny), &need);
    MOQ_TEST_CHECK(rc == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(need == written);
    MOQ_TEST_CHECK(tiny[sizeof(tiny) - 1] == '\0');

    moqr_trace_destroy(t);
    MOQ_TEST_PASS("trace_jsonl");
    return failures;
}

/* The pure histogram folder: each latency lands in the first bucket whose
 * upper bound it does not exceed; > the last bound is the +Inf overflow. */
static int
test_latency_observe(void)
{
    int failures = 0;
    moqr_latency_hist_t h;
    memset(&h, 0, sizeof(h));
    /* bounds (us): 100,250,500,1000,2500,5000,10000,25000,50000,100000,
     * 250000,1000000. */
    moqr_latency_observe(&h, 50);        /* <=100    -> bucket 0 */
    moqr_latency_observe(&h, 100);       /* ==100    -> bucket 0 */
    moqr_latency_observe(&h, 101);       /* (100,250]-> bucket 1 */
    moqr_latency_observe(&h, 300);       /* (250,500]-> bucket 2 */
    moqr_latency_observe(&h, 2000000);   /* >1000000 -> +Inf     */
    MOQ_TEST_CHECK_EQ_U64(h.bucket[0], 2);
    MOQ_TEST_CHECK_EQ_U64(h.bucket[1], 1);
    MOQ_TEST_CHECK_EQ_U64(h.bucket[2], 1);
    MOQ_TEST_CHECK_EQ_U64(h.bucket[MOQR_LATENCY_BUCKETS], 1);   /* +Inf */
    MOQ_TEST_CHECK_EQ_U64(h.count, 5);
    MOQ_TEST_CHECK_EQ_U64(h.sum_us, (uint64_t)(50 + 100 + 101 + 300 + 2000000));
    MOQ_TEST_CHECK_EQ_U64(moqr_latency_bound_us(0), 100);
    MOQ_TEST_CHECK_EQ_U64(moqr_latency_bound_us(MOQR_LATENCY_BUCKETS - 1),
                          1000000);
    MOQ_TEST_CHECK_EQ_U64(moqr_latency_bound_us(MOQR_LATENCY_BUCKETS), 0);
    MOQ_TEST_PASS("latency_observe");
    return failures;
}

/* The exporter renders a Prometheus histogram: cumulative _bucket{le} in
 * seconds, a _sum, and a _count. */
static int
test_prometheus_histogram(void)
{
    int failures = 0;
    moqr_core_stats_t cs = sample_core();
    moqr_bind_stats_t bs;
    memset(&bs, 0, sizeof(bs));
    moqr_latency_observe(&bs.forward_latency, 50);        /* bucket 0    */
    moqr_latency_observe(&bs.forward_latency, 300);       /* bucket 2    */
    moqr_latency_observe(&bs.forward_latency, 2000000);   /* +Inf        */
    moqr_obs_labels_t lb = { 0, "picoquic", "draft-18" };

    char buf[16384];
    size_t written = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus(&cs, &bs, &lb, buf,
                                                 sizeof(buf), &written) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf,
        "# TYPE moqrelay_forward_latency_seconds histogram"));

    const char *L = "shard=\"0\",transport=\"picoquic\",version=\"draft-18\"";
    char line[256];
    /* Cumulative: 50us in le=0.000100; 300us adds at le=0.000500; the +Inf
     * observation only lands in le="+Inf". */
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_bucket{%s,le=\"0.000100\"} 1",
             L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_bucket{%s,le=\"0.000250\"} 1",
             L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_bucket{%s,le=\"0.000500\"} 2",
             L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_bucket{%s,le=\"1.000000\"} 2",
             L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_bucket{%s,le=\"+Inf\"} 3", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    /* sum = 50 + 300 + 2000000 = 2000350 us = 2.000350 s; count = 3. */
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_sum{%s} 2.000350", L);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_count{%s} 3", L);
    MOQ_TEST_CHECK(HAS(buf, line));

    /* Truncation contract holds with the histogram present. */
    char small[64];
    size_t need = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus(&cs, &bs, &lb, small,
                                                 sizeof(small), &need) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(need == written);

    MOQ_TEST_PASS("prometheus_histogram");
    return failures;
}

/* Occurrences of `needle` in `hay`. */
static int
count_of(const char *hay, const char *needle)
{
    int n = 0;
    for (const char *p = strstr(hay, needle); p != NULL;
         p = strstr(p + 1, needle)) {
        n++;
    }
    return n;
}

/* Two fully-populated shard views for the multi-snapshot exposition. */
static void
sample_views(moqr_core_stats_t cs[2], moqr_bind_stats_t bs[2],
             moqr_shards_stats_t ss[2], moqr_snapshot_view_t v[2])
{
    memset(&cs[0], 0, sizeof(cs[0]));
    cs[0].ingested_total = 100;
    cs[0].subs = 9;
    cs[0].subs_parked = 3;
    cs[0].subs_active = 6;
    cs[0].bindings = 5;
    cs[0].ns_subs = 3;
    cs[0].intent_highwater = 9;
    cs[0].route_epoch = 42;
    memset(&cs[1], 0, sizeof(cs[1]));
    cs[1].ingested_total = 11;
    cs[1].subs = 4;
    cs[1].subs_parked = 2;
    cs[1].subs_active = 2;
    cs[1].bindings = 4;
    cs[1].ns_subs = 1;
    cs[1].intent_highwater = 4;
    cs[1].route_epoch = 17;

    memset(&bs[0], 0, sizeof(bs[0]));
    bs[0].conns = 3;
    moqr_latency_observe(&bs[0].forward_latency, 50);
    moqr_latency_observe(&bs[0].forward_latency, 300);
    memset(&bs[1], 0, sizeof(bs[1]));
    bs[1].conns = 2;
    moqr_latency_observe(&bs[1].forward_latency, 100);

    memset(&ss[0], 0, sizeof(ss[0]));
    ss[0].pump_subs_parked = 1;
    ss[0].pump_subs_active = 2;
    ss[0].internal_bindings = 2;
    ss[0].internal_ns_subs = 1;
    ss[0].enqueued[MOQR_SHARDS_MSG_OBJ] = 7;
    ss[0].enqueued[MOQR_SHARDS_MSG_SG_SEAL] = 1;
    ss[0].wake_requests_push = 5;
    ss[0].wake_requests_credit = 6;
    ss[0].channel_entries_hwm = 3;
    ss[0].channel_bytes_hwm = 96;
    ss[0].journal_epoch = 4;
    ss[0].pending_demands = 1;
    memset(&ss[1], 0, sizeof(ss[1]));
    ss[1].pump_subs_active = 1;
    ss[1].internal_bindings = 2;
    ss[1].internal_ns_subs = 1;
    ss[1].enqueued[MOQR_SHARDS_MSG_OBJ] = 3;
    ss[1].wake_requests_push = 2;
    ss[1].wake_requests_credit = 1;
    ss[1].channel_entries_hwm = 5;
    ss[1].channel_bytes_hwm = 50;
    ss[1].journal_epoch = 9;

    v[0] = (moqr_snapshot_view_t){
        .core = &cs[0], .bind = &bs[0], .shard = &ss[0],
        .labels = { 0, "msquic", "draft-18" }, .lane_wakes = 11
    };
    v[1] = (moqr_snapshot_view_t){
        .core = &cs[1], .bind = &bs[1], .shard = &ss[1],
        .labels = { 1, "msquic", "draft-18" }, .lane_wakes = 4
    };
}

/* One document over N shards: HELP/TYPE exactly once per metric, per-shard
 * series under the existing names/labels, process aggregates under
 * moqrelay_process_* (counters/gauges sum, HWMs max, histogram sums),
 * internal-entity exclusion subtracting like from like, and NO aggregated
 * route_epoch or journal_epoch. */
static int
test_prometheus_multi(void)
{
    int failures = 0;
    moqr_core_stats_t cs[2];
    moqr_bind_stats_t bs[2];
    moqr_shards_stats_t ss[2];
    moqr_snapshot_view_t v[2];
    sample_views(cs, bs, ss, v);

    static char buf[128 * 1024];
    size_t written = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, buf,
                                                       sizeof(buf),
                                                       &written) == MOQR_OK);
    MOQ_TEST_CHECK(written > 0 && written < sizeof(buf));
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), written);

    /* HELP/TYPE exactly once per metric — the per-shard series share one
     * header, and the process series get their own single header. */
    MOQ_TEST_CHECK_EQ_INT(
        count_of(buf, "# TYPE moqrelay_objects_ingested_total counter"), 1);
    MOQ_TEST_CHECK_EQ_INT(
        count_of(buf, "# TYPE moqrelay_subscriptions gauge"), 1);
    MOQ_TEST_CHECK_EQ_INT(
        count_of(buf, "# TYPE moqrelay_refusals_total counter"), 1);
    MOQ_TEST_CHECK_EQ_INT(
        count_of(buf,
                 "# TYPE moqrelay_process_objects_ingested_total counter"),
        1);
    MOQ_TEST_CHECK_EQ_INT(
        count_of(buf, "# TYPE moqrelay_forward_latency_seconds histogram"),
        1);

    const char *L0 = "shard=\"0\",transport=\"msquic\",version=\"draft-18\"";
    const char *L1 = "shard=\"1\",transport=\"msquic\",version=\"draft-18\"";
    char line[256];

    /* Per-shard series retain names + labels; the process sum is its own
     * separate series. */
    snprintf(line, sizeof(line), "moqrelay_objects_ingested_total{%s} 100",
             L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line), "moqrelay_objects_ingested_total{%s} 11",
             L1);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_objects_ingested_total 111"));

    /* Internal-entity exclusion, like from like: subscriptions minus pump
     * subs of the SAME state; bindings minus the manager slots; namespace
     * subscriptions minus the watcher. Internals surface separately. */
    snprintf(line, sizeof(line),
             "moqrelay_subscriptions{%s,state=\"parked\"} 2", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_subscriptions{%s,state=\"active\"} 4", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_subscriptions{%s,state=\"parked\"} 2", L1);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(
        HAS(buf, "moqrelay_process_subscriptions{state=\"parked\"} 4"));
    MOQ_TEST_CHECK(
        HAS(buf, "moqrelay_process_subscriptions{state=\"active\"} 5"));
    snprintf(line, sizeof(line), "moqrelay_bindings{%s} 3", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_bindings 5"));
    snprintf(line, sizeof(line), "moqrelay_namespace_subscriptions{%s} 2",
             L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line), "moqrelay_internal_bindings{%s} 2", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_pump_subscriptions{%s,state=\"parked\"} 1", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_pump_subscriptions{%s,state=\"active\"} 2", L0);
    MOQ_TEST_CHECK(HAS(buf, line));

    /* Counters sum; HWMs take the MAX; per-kind and per-cause labels. */
    snprintf(line, sizeof(line),
             "moqrelay_channel_enqueues_total{%s,kind=\"obj\"} 7", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line),
             "moqrelay_channel_enqueues_total{%s,kind=\"sg_seal\"} 1", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(HAS(
        buf, "moqrelay_process_channel_enqueues_total{kind=\"obj\"} 10"));
    snprintf(line, sizeof(line),
             "moqrelay_wake_requests_total{%s,cause=\"push\"} 5", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(
        HAS(buf, "moqrelay_process_wake_requests_total{cause=\"push\"} 7"));
    MOQ_TEST_CHECK(HAS(
        buf, "moqrelay_process_wake_requests_total{cause=\"credit\"} 7"));
    snprintf(line, sizeof(line), "moqrelay_lane_wakes_total{%s} 11", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_lane_wakes_total 15"));
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_intent_ring_highwater 9"));
    MOQ_TEST_CHECK(HAS(
        buf, "moqrelay_process_demand_channel_entries_highwater 5"));
    MOQ_TEST_CHECK(
        HAS(buf, "moqrelay_process_demand_channel_bytes_highwater 96"));

    /* Histogram: per-shard series plus a SUMMED process histogram. */
    snprintf(line, sizeof(line),
             "moqrelay_forward_latency_seconds_bucket{%s,le=\"0.000100\"} 1",
             L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_forward_latency_seconds_bucket"
                            "{le=\"0.000100\"} 2"));
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_forward_latency_seconds_bucket"
                            "{le=\"+Inf\"} 3"));
    MOQ_TEST_CHECK(
        HAS(buf, "moqrelay_process_forward_latency_seconds_count 3"));

    /* Per-shard identities are NEVER aggregated. */
    snprintf(line, sizeof(line), "moqrelay_route_epoch{%s} 42", L0);
    MOQ_TEST_CHECK(HAS(buf, line));
    snprintf(line, sizeof(line), "moqrelay_journal_epoch{%s} 9", L1);
    MOQ_TEST_CHECK(HAS(buf, line));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_process_route_epoch"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_process_journal_epoch"));

    /* Truncation contract mirrors the single-snapshot writer. */
    char small[64];
    size_t need = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, small,
                                                       sizeof(small),
                                                       &need) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_SIZE(need, written);
    MOQ_TEST_CHECK(small[sizeof(small) - 1] == '\0');

    MOQ_TEST_PASS("prometheus_multi");
    return failures;
}

/* A negative internal subtraction is a broken invariant: the whole
 * exposition is suppressed (INVAL, nothing written) — never floored,
 * wrapped, or invented. */
static int
test_prometheus_multi_negative(void)
{
    int failures = 0;
    moqr_core_stats_t cs[2];
    moqr_bind_stats_t bs[2];
    moqr_shards_stats_t ss[2];
    moqr_snapshot_view_t v[2];
    sample_views(cs, bs, ss, v);
    ss[1].pump_subs_parked = cs[1].subs_parked + 1;   /* impossible */

    char buf[4096];
    memset(buf, 'x', sizeof(buf));
    size_t written = 123;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, buf,
                                                       sizeof(buf),
                                                       &written) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(written, (size_t)0);
    MOQ_TEST_CHECK(buf[0] == '\0');   /* suppressed, not partial */

    /* Each of the four invariants suppresses independently. */
    sample_views(cs, bs, ss, v);
    ss[0].pump_subs_active = cs[0].subs_active + 1;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, buf,
                                                       sizeof(buf),
                                                       &written) ==
                   MOQR_ERR_INVAL);
    sample_views(cs, bs, ss, v);
    ss[0].internal_bindings = cs[0].bindings + 1;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, buf,
                                                       sizeof(buf),
                                                       &written) ==
                   MOQR_ERR_INVAL);
    sample_views(cs, bs, ss, v);
    ss[0].internal_ns_subs = cs[0].ns_subs + 1;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, buf,
                                                       sizeof(buf),
                                                       &written) ==
                   MOQR_ERR_INVAL);

    MOQ_TEST_PASS("prometheus_multi_negative");
    return failures;
}

/* Views without bind/shard snapshots render only the core families — no
 * empty shard-plane headers, no subtraction, still one process aggregate. */
static int
test_prometheus_multi_core_only(void)
{
    int failures = 0;
    moqr_core_stats_t cs[2];
    memset(&cs[0], 0, sizeof(cs[0]));
    cs[0].ingested_total = 5;
    cs[0].subs_parked = 1;
    cs[0].subs_active = 1;
    cs[0].subs = 2;
    memset(&cs[1], 0, sizeof(cs[1]));
    cs[1].ingested_total = 6;
    moqr_snapshot_view_t v[2] = {
        { .core = &cs[0], .bind = NULL, .shard = NULL,
          .labels = { 0, "sim", "draft-16" }, .lane_wakes = 0 },
        { .core = &cs[1], .bind = NULL, .shard = NULL,
          .labels = { 1, "sim", "draft-16" }, .lane_wakes = 0 },
    };
    static char buf[64 * 1024];
    size_t written = 0;
    MOQ_TEST_CHECK(moqr_metrics_write_prometheus_multi(v, 2, buf,
                                                       sizeof(buf),
                                                       &written) == MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_process_objects_ingested_total 11"));
    /* Subscriptions render RAW when no shard plane exists (nothing to
     * exclude). */
    MOQ_TEST_CHECK(HAS(buf, "moqrelay_subscriptions{shard=\"0\","
                            "transport=\"sim\",version=\"draft-16\","
                            "state=\"parked\"} 1"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_connections"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_pump_subscriptions"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_channel_enqueues_total"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_lane_wakes_total"));
    MOQ_TEST_CHECK(!HAS(buf, "moqrelay_journal_epoch"));
    MOQ_TEST_PASS("prometheus_multi_core_only");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_prometheus_format();
    failures += test_latency_observe();
    failures += test_prometheus_histogram();
    failures += test_prometheus_core_only();
    failures += test_prometheus_truncation();
    failures += test_prometheus_label_escaping();
    failures += test_trace_jsonl();
    failures += test_prometheus_multi();
    failures += test_prometheus_multi_negative();
    failures += test_prometheus_multi_core_only();
    return failures != 0;
}
