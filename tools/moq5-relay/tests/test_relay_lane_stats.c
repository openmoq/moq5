/*
 * The per-lane attribution row grammar (cli/lanestats.{c,h}): every required
 * field exactly once in a stable order, deterministic lane ordering, refusal
 * records that can never read as valid zeros, the total/samples average with
 * zero samples UNAVAILABLE, record-level validation (missing/duplicate/
 * malformed lanes invalidate), and the structurally-valid lanes=1 shape.
 * Pure string-level tests — no transport, no threads.
 */

#include "../cli/lanestats.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static moqr_cli_lane_stats_row_t sample_row(uint32_t lane)
{
    moqr_cli_lane_stats_row_t r;
    memset(&r, 0, sizeof(r));
    r.lane = lane;
    r.wakes_same_lane = 10 + lane;
    r.wakes_cross_lane = 20 + lane;
    r.wakes_external = 1;
    r.wakes_coalesced = 3;
    r.pump_sweeps = 40 + lane;
    r.deadline_sweeps = 2;
    r.idle_cap_wakes = 5;
    r.wake_to_pump_max_us = 1500;
    r.wake_to_pump_total_us = 6000;
    r.wake_to_pump_samples = 4;
    r.service_passes = 80;
    r.flush_sends = 12;
    r.flush_bytes = 34567;
    r.pump_turns = 100 + lane;
    r.pump_messages = 200 + lane;
    r.pump_bytes = 51200;
    r.wake_requests_push = 7;
    r.wake_requests_credit = 6;
    r.wake_requests_local = 5;
    r.enq_obj = 128;
    r.enq_demand = 1;
    r.enq_ack = 1;
    r.enq_undemand = 1;
    r.channel_entries_hwm = 32;
    r.channel_bytes_hwm = 16384;
    return r;
}

/* (1) every required field appears exactly once, and the round trip is
 * exact (parse(format(row)) == row). */
static void t_fields_once_roundtrip(void)
{
    int before = failures;
    moqr_cli_lane_stats_row_t in = sample_row(3), out;
    char line[1024];
    int len = moqr_cli_lane_stats_format(line, sizeof(line), &in);
    CHECK(len > 0 && (size_t)len < sizeof(line));

    static const char *const required[] = {
        "lane=", "wakes_same_lane=", "wakes_cross_lane=", "wakes_external=",
        "wakes_coalesced=", "pump_sweeps=", "deadline_sweeps=",
        "idle_cap_wakes=", "wake_to_pump_max_us=", "wake_to_pump_total_us=",
        "wake_to_pump_samples=", "service_passes=", "flush_sends=",
        "flush_bytes=", "pump_turns=", "pump_messages=", "pump_bytes=",
        "wake_requests_push=", "wake_requests_credit=",
        "wake_requests_local=", "enq_demand=",
        "enq_undemand=", "enq_done=", "enq_ack=", "enq_obj=",
        "enq_obj_open=", "enq_obj_chunk=", "enq_obj_end=", "enq_obj_reset=",
        "enq_grp_reset=", "enq_grp_evict=", "enq_sg_seal=",
        "channel_entries_hwm=", "channel_bytes_hwm=",
    };
    for (size_t i = 0; i < sizeof(required) / sizeof(required[0]); i++) {
        int count = 0;
        for (const char *p = line; (p = strstr(p, required[i])) != NULL; p++)
            count++;
        /* "pump_sweeps=" would also match inside nothing else; keys with a
         * shared prefix (pump_*, wake_*) are distinct full tokens because
         * every occurrence is preceded by ',' — count exact-once by
         * requiring the ",key=" form (the first field after the prefix is
         * lane=, checked via the version prefix below). */
        int strict = 0;
        for (const char *p = line; (p = strstr(p, required[i])) != NULL; p++)
            if (p > line && p[-1] == ',')
                strict++;
        (void)count;
        if (strict != 1) {
            fprintf(stderr, "FAIL: field '%s' appears %d times\n",
                    required[i], strict);
            failures++;
        }
    }
    CHECK(strncmp(line, MOQR_LANE_STATS_PREFIX ",lane=3,", 25) == 0);

    uint32_t rl = 0;
    CHECK(moqr_cli_lane_stats_parse(line, &out, &rl) == MOQR_LANE_STATS_ROW);
    CHECK(memcmp(&in, &out, sizeof(in)) == 0);   /* exact round trip */
    if (failures == before)
        printf("PASS: fields exactly once + exact round trip\n");
}

/* (2) lane rows are distinct and deterministically ordered: the record
 * validator accepts ascending 0..N-1 exactly, and the same counters always
 * format to the same bytes. */
static void t_record_order(void)
{
    int before = failures;
    char l0[1024], l1[1024], l0b[1024];
    moqr_cli_lane_stats_row_t r0 = sample_row(0), r1 = sample_row(1);
    moqr_cli_lane_stats_row_t rows[2];
    CHECK(moqr_cli_lane_stats_format(l0, sizeof(l0), &r0) > 0);
    CHECK(moqr_cli_lane_stats_format(l1, sizeof(l1), &r1) > 0);
    CHECK(moqr_cli_lane_stats_format(l0b, sizeof(l0b), &r0) > 0);
    CHECK(strcmp(l0, l0b) == 0);   /* byte-deterministic */
    CHECK(strcmp(l0, l1) != 0);    /* distinct lanes differ */

    const char *ok[2] = { l0, l1 };
    CHECK(moqr_cli_lane_stats_record(ok, 2, 2, rows));
    CHECK(rows[0].lane == 0 && rows[1].lane == 1);

    const char *swapped[2] = { l1, l0 };   /* out of order -> invalid */
    CHECK(!moqr_cli_lane_stats_record(swapped, 2, 2, rows));
    if (failures == before)
        printf("PASS: deterministic distinct lane rows; order enforced\n");
}

/* (3) a refusal can never look like valid zeros: the refusal record is a
 * DISTINCT kind, and a record containing one is invalid. */
static void t_refusal_not_zeros(void)
{
    int before = failures;
    char refusal[256], zeros[1024];
    moqr_cli_lane_stats_row_t z, out, rows[2];
    memset(&z, 0, sizeof(z));   /* an all-zero but VALID row */
    CHECK(moqr_cli_lane_stats_format_refused(refusal, sizeof(refusal), 0,
                                             "adapter") > 0);
    CHECK(moqr_cli_lane_stats_format(zeros, sizeof(zeros), &z) > 0);
    CHECK(strcmp(refusal, zeros) != 0);

    uint32_t rl = 99;
    CHECK(moqr_cli_lane_stats_parse(refusal, &out, &rl) ==
          MOQR_LANE_STATS_REFUSED);
    CHECK(rl == 0);
    /* a refusal with an unknown source is malformed, not silently valid */
    CHECK(moqr_cli_lane_stats_parse(
              MOQR_LANE_STATS_PREFIX ",lane=0,refused=gremlin", &out, &rl) ==
          MOQR_LANE_STATS_MALFORMED);

    char l1[1024];
    moqr_cli_lane_stats_row_t r1 = sample_row(1);
    CHECK(moqr_cli_lane_stats_format(l1, sizeof(l1), &r1) > 0);
    const char *withref[2] = { refusal, l1 };
    CHECK(!moqr_cli_lane_stats_record(withref, 2, 2, rows));
    if (failures == before)
        printf("PASS: refusal is a distinct kind and poisons the record\n");
}

/* (4) the wake average is total/samples; zero samples is UNAVAILABLE. */
static void t_average_rule(void)
{
    int before = failures;
    moqr_cli_lane_stats_row_t r = sample_row(0);
    double avg = -1.0;
    CHECK(moqr_cli_lane_stats_avg_us(&r, &avg));
    CHECK(avg == 1500.0);   /* 6000 / 4 */
    r.wake_to_pump_samples = 0;
    r.wake_to_pump_total_us = 0;
    CHECK(!moqr_cli_lane_stats_avg_us(&r, &avg));   /* unavailable */
    if (failures == before)
        printf("PASS: average = total/samples; zero samples unavailable\n");
}

/* (5) malformed, duplicate, or missing lane rows invalidate the record. */
static void t_record_invalidation(void)
{
    int before = failures;
    char l0[1024], l1[1024];
    moqr_cli_lane_stats_row_t r0 = sample_row(0), r1 = sample_row(1);
    moqr_cli_lane_stats_row_t rows[3];
    CHECK(moqr_cli_lane_stats_format(l0, sizeof(l0), &r0) > 0);
    CHECK(moqr_cli_lane_stats_format(l1, sizeof(l1), &r1) > 0);

    const char *missing[1] = { l0 };
    CHECK(!moqr_cli_lane_stats_record(missing, 1, 2, rows));   /* short */
    const char *dup[2] = { l0, l0 };
    CHECK(!moqr_cli_lane_stats_record(dup, 2, 2, rows));       /* duplicate */
    const char *extra[3] = { l0, l1, l1 };
    CHECK(!moqr_cli_lane_stats_record(extra, 3, 2, rows));     /* extra row */

    /* malformed variants: truncated tail, junk tail, renamed field */
    char bad[1024 + 16];   /* room for a full row plus an appended tail */
    moqr_cli_lane_stats_row_t out;
    uint32_t rl;
    snprintf(bad, sizeof(bad), "%.*s", (int)(strlen(l0) - 3), l0);
    CHECK(moqr_cli_lane_stats_parse(bad, &out, &rl) !=
          MOQR_LANE_STATS_ROW);
    snprintf(bad, sizeof(bad), "%s,junk=1", l0);
    CHECK(moqr_cli_lane_stats_parse(bad, &out, &rl) ==
          MOQR_LANE_STATS_MALFORMED);
    snprintf(bad, sizeof(bad), "%s", l0);
    char *hit = strstr(bad, "flush_sends");
    CHECK(hit != NULL);
    if (hit != NULL)
        hit[0] = 'F';
    CHECK(moqr_cli_lane_stats_parse(bad, &out, &rl) ==
          MOQR_LANE_STATS_MALFORMED);
    const char *withbad[2] = { bad, l1 };
    CHECK(!moqr_cli_lane_stats_record(withbad, 2, 2, rows));
    if (failures == before)
        printf("PASS: malformed/duplicate/missing rows invalidate\n");
}

/* (6) lanes=1 is structurally valid with zero cross-lane wakes. */
static void t_single_lane(void)
{
    int before = failures;
    moqr_cli_lane_stats_row_t r = sample_row(0), rows[1];
    r.wakes_cross_lane = 0;   /* no other lane exists to cross-wake */
    char l0[1024];
    CHECK(moqr_cli_lane_stats_format(l0, sizeof(l0), &r) > 0);
    const char *one[1] = { l0 };
    CHECK(moqr_cli_lane_stats_record(one, 1, 1, rows));
    CHECK(rows[0].wakes_cross_lane == 0);
    CHECK(rows[0].lane == 0);
    if (failures == before)
        printf("PASS: lanes=1 record valid with zero cross-lane wakes\n");
}

/* Rebuild a formatted row with one field's value swapped for a literal. */
static void splice_value(char *dst, size_t n, const char *src,
                         const char *key, const char *value)
{
    char needle[64];
    snprintf(needle, sizeof(needle), ",%s=", key);
    const char *hit = strstr(src, needle);
    CHECK(hit != NULL);
    if (hit == NULL) {
        snprintf(dst, n, "%s", src);
        return;
    }
    const char *vstart = hit + strlen(needle);
    const char *vend = vstart;
    while (*vend >= '0' && *vend <= '9')
        vend++;
    snprintf(dst, n, "%.*s%s%s", (int)(vstart - src), src, value, vend);
}

/* (7) numeric bounds: exact UINT64_MAX round-trips; UINT64_MAX+1 is
 * malformed (never silently saturated); lane is bounded at UINT32_MAX. */
static void t_numeric_bounds(void)
{
    int before = failures;
    moqr_cli_lane_stats_row_t r = sample_row(0), out;
    uint32_t rl;
    r.wakes_same_lane = UINT64_MAX;
    char line[1024], bad[1024 + 32];
    CHECK(moqr_cli_lane_stats_format(line, sizeof(line), &r) > 0);
    CHECK(moqr_cli_lane_stats_parse(line, &out, &rl) ==
          MOQR_LANE_STATS_ROW);
    CHECK(out.wakes_same_lane == UINT64_MAX);

    splice_value(bad, sizeof(bad), line,
                 "wakes_same_lane", "18446744073709551616");
    CHECK(moqr_cli_lane_stats_parse(bad, &out, &rl) ==
          MOQR_LANE_STATS_MALFORMED);

    moqr_cli_lane_stats_row_t rmax = sample_row(0);
    rmax.lane = UINT32_MAX;
    CHECK(moqr_cli_lane_stats_format(line, sizeof(line), &rmax) > 0);
    CHECK(moqr_cli_lane_stats_parse(line, &out, &rl) ==
          MOQR_LANE_STATS_ROW);
    CHECK(out.lane == UINT32_MAX);
    char lane_over[1024 + 32];
    static const char pfx[] = MOQR_LANE_STATS_PREFIX ",lane=4294967295";
    CHECK(strncmp(line, pfx, sizeof(pfx) - 1) == 0);
    snprintf(lane_over, sizeof(lane_over),
             MOQR_LANE_STATS_PREFIX ",lane=4294967296%s",
             line + sizeof(pfx) - 1);
    CHECK(moqr_cli_lane_stats_parse(lane_over, &out, &rl) ==
          MOQR_LANE_STATS_MALFORMED);
    if (failures == before)
        printf("PASS: u64/u32 bounds enforced (max ok, max+1 malformed)\n");
}

/* (8) current-version lane rows carry the turn-outcome fields and the
 * local-continuation wake cause; a row missing the appended field is
 * malformed; pair rows round-trip byte-exactly with the same truncation
 * discipline. */
static void t_v2_and_pair_roundtrip(void)
{
    int before = failures;
    moqr_cli_lane_stats_row_t r = sample_row(0), out;
    uint32_t rl;
    r.turns_msg_budget = 7;
    r.turns_byte_budget = 3;
    r.turns_blocked = 2;
    r.turns_drained = 9;
    r.turns_with_messages = 11;
    r.arb_class_refusals = 1;
    r.wake_requests_local = 4;
    char line[1024];
    CHECK(moqr_cli_lane_stats_format(line, sizeof(line), &r) > 0);
    CHECK(strncmp(line, "RELAY_LANE_STATS_V3,", 20) == 0);
    CHECK(strstr(line, ",turns_msg_budget=7,") != NULL);
    CHECK(strstr(line, ",wake_requests_local=4,") != NULL);
    CHECK(moqr_cli_lane_stats_parse(line, &out, &rl) == MOQR_LANE_STATS_ROW);
    CHECK(memcmp(&r, &out, sizeof(r)) == 0);
    /* a current-prefix row WITHOUT the appended field is malformed — the
     * grammar is exact per version, never optional-tailed */
    char *tail = strstr(line, ",wake_requests_local=4");
    CHECK(tail != NULL);
    memmove(tail, tail + strlen(",wake_requests_local=4"),
            strlen(tail + strlen(",wake_requests_local=4")) + 1);
    CHECK(moqr_cli_lane_stats_parse(line, &out, &rl) ==
          MOQR_LANE_STATS_MALFORMED);

    moqr_cli_pair_stats_row_t pin, pout;
    memset(&pin, 0, sizeof(pin));
    pin.src = 0;
    pin.dst = 2;
    pin.data_messages = 1234;
    pin.data_bytes = 56789;
    pin.control_messages = 4;
    pin.refused_entries = 2;
    pin.refused_bytes = 1;
    char pl[512], bad[512 + 16];
    int n = moqr_cli_pair_stats_format(pl, sizeof(pl), &pin);
    CHECK(n > 0);
    CHECK(moqr_cli_pair_stats_parse(pl, &pout) == MOQR_LANE_STATS_ROW);
    CHECK(memcmp(&pin, &pout, sizeof(pin)) == 0);
    /* truncation (mid-number, eor gone) and junk tail are malformed */
    snprintf(bad, sizeof(bad), "%.*s", n - 3, pl);
    CHECK(moqr_cli_pair_stats_parse(bad, &pout) == MOQR_LANE_STATS_MALFORMED);
    snprintf(bad, sizeof(bad), "%s,junk=1", pl);
    CHECK(moqr_cli_pair_stats_parse(bad, &pout) == MOQR_LANE_STATS_MALFORMED);
    if (failures == before)
        printf("PASS: current lane row + pair row round-trip; truncation "
               "malformed\n");
}

/* (9) pair records fail closed on every incompleteness mode: exactly
 * K*(K-1) non-self ordered pairs in the deterministic emission order. */
static void t_pair_record_failure_modes(void)
{
    int before = failures;
    enum { K = 3, N = K * (K - 1) };
    char bufs[N][256];
    const char *lines[N + 1];
    moqr_cli_pair_stats_row_t rows[N + 1];
    size_t i = 0;
    for (uint32_t src = 0; src < K; src++) {
        for (uint32_t dst = 0; dst < K; dst++) {
            if (dst == src)
                continue;
            moqr_cli_pair_stats_row_t r;
            memset(&r, 0, sizeof(r));
            r.src = src;
            r.dst = dst;
            r.data_messages = 100 * src + dst;
            CHECK(moqr_cli_pair_stats_format(bufs[i], sizeof(bufs[i]),
                                             &r) > 0);
            lines[i] = bufs[i];
            i++;
        }
    }
    CHECK(moqr_cli_pair_stats_record(lines, N, K, rows));
    CHECK(rows[1].src == 0 && rows[1].dst == 2);

    /* missing row */
    CHECK(!moqr_cli_pair_stats_record(lines, N - 1, K, rows));
    /* duplicate row (last replaced by a copy of the first) */
    const char *dup[N];
    memcpy(dup, lines, sizeof(dup));
    dup[N - 1] = lines[0];
    CHECK(!moqr_cli_pair_stats_record(dup, N, K, rows));
    /* self pair */
    char selfbuf[256];
    moqr_cli_pair_stats_row_t sp;
    memset(&sp, 0, sizeof(sp));
    sp.src = 1;
    sp.dst = 1;
    CHECK(moqr_cli_pair_stats_format(selfbuf, sizeof(selfbuf), &sp) > 0);
    const char *wself[N];
    memcpy(wself, lines, sizeof(wself));
    wself[0] = selfbuf;
    CHECK(!moqr_cli_pair_stats_record(wself, N, K, rows));
    /* out-of-range index */
    char oob[256];
    memset(&sp, 0, sizeof(sp));
    sp.src = 0;
    sp.dst = K;   /* dst == K is out of range */
    CHECK(moqr_cli_pair_stats_format(oob, sizeof(oob), &sp) > 0);
    const char *woob[N];
    memcpy(woob, lines, sizeof(woob));
    woob[0] = oob;
    CHECK(!moqr_cli_pair_stats_record(woob, N, K, rows));
    /* malformed row */
    const char *wmal[N];
    memcpy(wmal, lines, sizeof(wmal));
    wmal[2] = "RELAY_PAIR_STATS_V1,src=1,dst=0,data_messages=";
    CHECK(!moqr_cli_pair_stats_record(wmal, N, K, rows));
    /* K=0 is never a valid pair record (K=1's EMPTY record is — see the
     * dedicated test below) */
    CHECK(!moqr_cli_pair_stats_record(lines, 0, 0, rows));
    if (failures == before)
        printf("PASS: pair record fails closed (missing/dup/self/range/"
               "malformed)\n");
}

/* (10) the run-config record round-trips and is strict. */
static void t_run_config_record(void)
{
    int before = failures;
    moqr_cli_run_config_row_t in, out;
    memset(&in, 0, sizeof(in));
    in.pump_turn_messages = 64;
    in.pump_turn_bytes = 262144;
    in.demand_channel_entries = 64;
    in.demand_channel_bytes = 1048576;
    char line[512], bad[512 + 16];
    int n = moqr_cli_run_config_format(line, sizeof(line), &in);
    CHECK(n > 0);
    CHECK(strncmp(line, "RELAY_RUN_CONFIG_V1,", 20) == 0);
    CHECK(moqr_cli_run_config_parse(line, &out) == MOQR_LANE_STATS_ROW);
    CHECK(memcmp(&in, &out, sizeof(in)) == 0);
    snprintf(bad, sizeof(bad), "%.*s", n - 2, line);
    CHECK(moqr_cli_run_config_parse(bad, &out) ==
          MOQR_LANE_STATS_MALFORMED);
    snprintf(bad, sizeof(bad), "%s,junk=1", line);
    CHECK(moqr_cli_run_config_parse(bad, &out) ==
          MOQR_LANE_STATS_MALFORMED);
    /* resolved-value bounds: zero anywhere is impossible (the resolver
     * guarantees nonzero); the u32-backed fields cap at UINT32_MAX */
    CHECK(snprintf(bad, sizeof(bad),
                   "RELAY_RUN_CONFIG_V1,pump_turn_messages=0,"
                   "pump_turn_bytes=262144,demand_channel_entries=64,"
                   "demand_channel_bytes=1048576,eor=1") > 0);
    CHECK(moqr_cli_run_config_parse(bad, &out) ==
          MOQR_LANE_STATS_MALFORMED);
    CHECK(snprintf(bad, sizeof(bad),
                   "RELAY_RUN_CONFIG_V1,pump_turn_messages=64,"
                   "pump_turn_bytes=262144,demand_channel_entries=64,"
                   "demand_channel_bytes=0,eor=1") > 0);
    CHECK(moqr_cli_run_config_parse(bad, &out) ==
          MOQR_LANE_STATS_MALFORMED);
    CHECK(snprintf(bad, sizeof(bad),
                   "RELAY_RUN_CONFIG_V1,pump_turn_messages=4294967295,"
                   "pump_turn_bytes=18446744073709551615,"
                   "demand_channel_entries=4294967295,"
                   "demand_channel_bytes=18446744073709551615,eor=1") > 0);
    CHECK(moqr_cli_run_config_parse(bad, &out) == MOQR_LANE_STATS_ROW);
    CHECK(out.pump_turn_messages == 0xFFFFFFFFull);
    CHECK(snprintf(bad, sizeof(bad),
                   "RELAY_RUN_CONFIG_V1,pump_turn_messages=4294967296,"
                   "pump_turn_bytes=262144,demand_channel_entries=64,"
                   "demand_channel_bytes=1048576,eor=1") > 0);
    CHECK(moqr_cli_run_config_parse(bad, &out) ==
          MOQR_LANE_STATS_MALFORMED);
    CHECK(snprintf(bad, sizeof(bad),
                   "RELAY_RUN_CONFIG_V1,pump_turn_messages=64,"
                   "pump_turn_bytes=262144,demand_channel_entries="
                   "4294967296,demand_channel_bytes=1048576,eor=1") > 0);
    CHECK(moqr_cli_run_config_parse(bad, &out) ==
          MOQR_LANE_STATS_MALFORMED);
    if (failures == before)
        printf("PASS: run-config record round-trips; truncation "
               "malformed; resolved-value bounds enforced\n");
}

/* (11) K=1's complete pair record is EXACTLY zero rows — the lanes=1
 * control must validate, and K=0 stays invalid. An all-max lane row must
 * fit the production 2048-byte buffer (and provably NOT fit 1024). */
static void t_k1_record_and_max_row(void)
{
    int before = failures;
    moqr_cli_pair_stats_row_t rows[1];
    const char *none[1] = { NULL };
    CHECK(moqr_cli_pair_stats_record(none, 0, 1, rows));    /* K=1: empty */
    CHECK(!moqr_cli_pair_stats_record(none, 1, 1, rows));   /* extra row  */
    CHECK(!moqr_cli_pair_stats_record(none, 0, 0, rows));   /* K=0 invalid */

    moqr_cli_lane_stats_row_t r, out;
    uint32_t rl;
    memset(&r, 0xFF, sizeof(r));   /* every field at UINT64_MAX / UINT32_MAX */
    char big[2048];
    int n = moqr_cli_lane_stats_format(big, sizeof(big), &r);
    CHECK(n > 0);
    CHECK((size_t)n < sizeof(big));         /* fits the production buffer */
    CHECK((size_t)n >= 1024);               /* ...and would NOT fit 1024  */
    CHECK(moqr_cli_lane_stats_parse(big, &out, &rl) == MOQR_LANE_STATS_ROW);
    /* field equality via re-format (memcmp would compare struct padding,
     * which 0xFF poisons but the parser's memset zeroes) */
    char big2[2048];
    CHECK(moqr_cli_lane_stats_format(big2, sizeof(big2), &out) == n);
    CHECK(strcmp(big, big2) == 0);
    /* the would-be-length contract on a too-small buffer */
    char small[1024];
    int n2 = moqr_cli_lane_stats_format(small, sizeof(small), &r);
    CHECK(n2 == n);   /* reports the full required length */
    if (failures == before)
        printf("PASS: K=1 empty pair record valid; all-max row sizing\n");
}


/* The JSON records: the same key tables as the text rows in the same order,
 * the `schema`/`elapsed_us` envelope with a nullable elapsed, the closed
 * refusal vocabulary of each family, and buffer-capacity refusal that leaves
 * nothing behind. */
static void t_json_records(void)
{
    char b[4096];
    size_t n = 0;
    moqr_cli_lane_stats_row_t r = sample_row(3);

    CHECK(moqr_cli_lane_stats_field_count() == 39u);
    CHECK(moqr_cli_pair_stats_field_count() == 5u);
    CHECK(moqr_cli_run_config_field_count() == 4u);

    CHECK(moqr_cli_lane_stats_json(b, sizeof(b), true, 42, &r, &n) == MOQR_OK);
    CHECK(n == strlen(b));
    {
        static const char head[] = "{\"schema\":\"RELAY_LANE_STATS_V3\",\"elapsed_us\":42,"
                                   "\"lane\":3,\"wakes_same_lane\":13,";
        CHECK(strncmp(b, head, sizeof(head) - 1u) == 0);
    }
    CHECK(strstr(b, "\"wake_requests_local\":5}") != NULL);
    CHECK(strstr(b, "eor") == NULL);
    /* key order: every text key appears in the JSON record, in the same order */
    {
        char t[2048];
        int tl = moqr_cli_lane_stats_format(t, sizeof(t), &r);
        const char *tp;
        const char *jp = b;
        CHECK(tl > 0);
        tp = strstr(t, ",lane=");
        while (tp != NULL) {
            const char *eq;
            char key[80];
            size_t kl;
            tp++;
            eq = strchr(tp, '=');
            kl = (size_t)(eq - tp);
            if (kl == 3 && memcmp(tp, "eor", 3) == 0) {
                break;
            }
            (void)snprintf(key, sizeof(key), "\"%.*s\":", (int)kl, tp);
            jp = strstr(jp, key);
            CHECK(jp != NULL);
            if (jp == NULL) {
                break;
            }
            tp = strchr(tp, ',');
        }
    }
    /* the nullable elapsed */
    CHECK(moqr_cli_lane_stats_json(b, sizeof(b), false, 42, &r, &n) == MOQR_OK);
    {
        static const char head[] = "{\"schema\":\"RELAY_LANE_STATS_V3\",\"elapsed_us\":null,";
        CHECK(strncmp(b, head, sizeof(head) - 1u) == 0);
    }
    /* widest counters */
    memset(&r, 0xff, sizeof(r));
    CHECK(moqr_cli_lane_stats_json(b, sizeof(b), true, UINT64_MAX, &r, &n) == MOQR_OK);
    CHECK(strstr(b, "18446744073709551615") != NULL);
    /* refusals: the closed vocabulary */
    CHECK(moqr_cli_lane_stats_refused_json(b, sizeof(b), true, 1, 7, "adapter", &n) == MOQR_OK);
    CHECK(strcmp(b, "{\"schema\":\"RELAY_LANE_STATS_V3\",\"elapsed_us\":1,\"lane\":7,\"refused\":\"adapter\"}") == 0);
    CHECK(moqr_cli_lane_stats_refused_json(b, sizeof(b), true, 1, 7, "shard", &n) == MOQR_OK);
    CHECK(moqr_cli_lane_stats_refused_json(b, sizeof(b), true, 1, 7, "resolver", &n) == MOQR_ERR_INVAL);
    CHECK(moqr_cli_lane_stats_refused_json(b, sizeof(b), true, 1, 7, "", &n) == MOQR_ERR_INVAL);
    CHECK(moqr_cli_lane_stats_refused_json(b, sizeof(b), true, 1, 7, NULL, &n) == MOQR_ERR_INVAL);
    CHECK(moqr_cli_pair_stats_refused_json(b, sizeof(b), false, 0, 1, 2, "shard", &n) == MOQR_OK);
    CHECK(strcmp(b, "{\"schema\":\"RELAY_PAIR_STATS_V1\",\"elapsed_us\":null,\"src\":1,\"dst\":2,\"refused\":\"shard\"}") == 0);
    CHECK(moqr_cli_pair_stats_refused_json(b, sizeof(b), false, 0, 1, 2, "adapter", &n) == MOQR_ERR_INVAL);
    CHECK(moqr_cli_run_config_refused_json(b, sizeof(b), true, 9, "resolver", &n) == MOQR_OK);
    CHECK(strcmp(b, "{\"schema\":\"RELAY_RUN_CONFIG_V1\",\"elapsed_us\":9,\"refused\":\"resolver\"}") == 0);
    CHECK(moqr_cli_run_config_refused_json(b, sizeof(b), true, 9, "format", &n) == MOQR_OK);
    CHECK(moqr_cli_run_config_refused_json(b, sizeof(b), true, 9, "shard", &n) == MOQR_ERR_INVAL);
    /* pair and run-config records */
    {
        moqr_cli_pair_stats_row_t pr;
        moqr_cli_run_config_row_t rc;
        memset(&pr, 0, sizeof(pr));
        pr.src = 0; pr.dst = 1; pr.data_messages = 5; pr.data_bytes = 6;
        pr.control_messages = 7; pr.refused_entries = 8; pr.refused_bytes = 9;
        CHECK(moqr_cli_pair_stats_json(b, sizeof(b), true, 3, &pr, &n) == MOQR_OK);
        CHECK(strcmp(b, "{\"schema\":\"RELAY_PAIR_STATS_V1\",\"elapsed_us\":3,\"src\":0,\"dst\":1,"
                        "\"data_messages\":5,\"data_bytes\":6,\"control_messages\":7,"
                        "\"refused_entries\":8,\"refused_bytes\":9}") == 0);
        memset(&rc, 0, sizeof(rc));
        rc.pump_turn_messages = 64; rc.pump_turn_bytes = 1048576;
        rc.demand_channel_entries = 256; rc.demand_channel_bytes = 4194304;
        CHECK(moqr_cli_run_config_json(b, sizeof(b), true, 4, &rc, &n) == MOQR_OK);
        CHECK(strcmp(b, "{\"schema\":\"RELAY_RUN_CONFIG_V1\",\"elapsed_us\":4,"
                        "\"pump_turn_messages\":64,\"pump_turn_bytes\":1048576,"
                        "\"demand_channel_entries\":256,\"demand_channel_bytes\":4194304}") == 0);
    }
    /* capacity: exact fit accepted, one short refused and cleared */
    {
        moqr_cli_pair_stats_row_t pr;
        size_t need;
        memset(&pr, 0, sizeof(pr));
        CHECK(moqr_cli_pair_stats_json(b, sizeof(b), true, 0, &pr, &need) == MOQR_OK);
        memset(b, 'x', sizeof(b));
        CHECK(moqr_cli_pair_stats_json(b, need + 1u, true, 0, &pr, &n) == MOQR_OK && n == need);
        memset(b, 'x', sizeof(b));
        CHECK(moqr_cli_pair_stats_json(b, need, true, 0, &pr, &n) == MOQR_ERR_CAPACITY);
        CHECK(n == 0 && b[0] == '\0');
        CHECK(moqr_cli_pair_stats_json(NULL, 8, true, 0, &pr, &n) == MOQR_ERR_INVAL);
        CHECK(moqr_cli_pair_stats_json(b, 0, true, 0, &pr, &n) == MOQR_ERR_INVAL);
        CHECK(moqr_cli_pair_stats_json(b, sizeof(b), true, 0, NULL, &n) == MOQR_ERR_INVAL);
    }
}

int main(void)
{
    t_fields_once_roundtrip();
    t_record_order();
    t_refusal_not_zeros();
    t_average_rule();
    t_record_invalidation();
    t_single_lane();
    t_numeric_bounds();
    t_v2_and_pair_roundtrip();
    t_pair_record_failure_modes();
    t_run_config_record();
    t_k1_record_and_max_row();
    t_json_records();
    if (failures == 0)
        printf("# ALL PASS\n");
    return failures;
}
