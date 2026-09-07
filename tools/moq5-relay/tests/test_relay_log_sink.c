/*
 * The serve's stdout sink and its structured events (cli/logevent.[ch] and
 * the JSON row formatters in cli/lanestats.c).
 *
 * TEXT is pinned against a FROZEN baseline: the literal bytes the pre-refactor
 * emission paths produced for fixed sentinel fixtures, generated from the old
 * format strings and formatters before either was touched. JSON is checked
 * through an independent parser (the vendored json.h), value by value.
 * Everything else -- the latching stream, the nullable clock, the once-only
 * diagnostics, the terminal sink states -- runs on scripted clocks and
 * scripted I/O. Nothing here sleeps, waits or opens a socket.
 */
#include "../cli/logevent.h"
#include "../cli/lanestats.h"
#include "../cli/admin_targets.h"
#include "../admin/json_out.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "json.h"
#include "../../../tests/unit/test_support.h"

/* -- the frozen text baseline (see the generator in the report) ------------ */
static const char *const k_text_baseline[] = {
    "MOQ5 Relay: listening on 10.0.0.7:4433 (moqt-16+moqt-18)\n",
    "MOQ5 Relay: admin endpoint on 127.0.0.1:9109 (GET /metrics)\n",
    "RELAY_RUN_CONFIG_V1,pump_turn_messages=64,pump_turn_bytes=1048576,demand_channel_entries=256,demand_channel_bytes=4194304,eor=1\n",
    "RELAY_RUN_CONFIG_V1,refused=resolver\n",
    "signals: SIGUSR1 -> metrics + route dump, SIGUSR2 -> trace JSONL (both to stderr)\n",
    "MOQ5 Relay: listening on 10.0.0.7:4433 (moqt-16+moqt-18), 2 lanes\n",
    "MOQ5 Relay: admin endpoint on 127.0.0.1:9109 (GET /metrics)\n",
    "MOQ5 Relay: WebTransport on 10.0.0.8:4434/moq (moqt-16+moqt-18, profile current), 1 lanes\n",
    "signals: SIGUSR1 -> metrics + route dump, SIGUSR2 -> trace JSONL (per shard, to stderr)\n",
    "RELAY_LANE_STATS_V3,lane=1,wakes_same_lane=101,wakes_cross_lane=102,wakes_external=103,wakes_coalesced=104,pump_sweeps=105,deadline_sweeps=106,idle_cap_wakes=107,wake_to_pump_max_us=108,wake_to_pump_total_us=109,wake_to_pump_samples=110,service_passes=111,flush_sends=112,flush_bytes=113,pump_turns=72,pump_messages=115,pump_bytes=116,wake_requests_push=117,wake_requests_credit=118,enq_demand=119,enq_undemand=120,enq_done=121,enq_ack=122,enq_obj=123,enq_obj_open=124,enq_obj_chunk=125,enq_obj_end=126,enq_obj_reset=127,enq_grp_reset=128,enq_grp_evict=129,enq_sg_seal=130,channel_entries_hwm=131,channel_bytes_hwm=132,turns_msg_budget=133,turns_byte_budget=134,turns_blocked=135,turns_drained=136,turns_with_messages=137,arb_class_refusals=138,wake_requests_local=139,eor=1\n",
    "RELAY_LANE_STATS_V3,lane=2,refused=adapter\n",
    "RELAY_LANE_STATS_V3,lane=3,refused=shard\n",
    "RELAY_LANE_STATS_V3,lane=4,refused=adapter\n",
    "RELAY_PAIR_STATS_V1,src=0,dst=1,data_messages=201,data_bytes=202,control_messages=203,refused_entries=204,refused_bytes=205,eor=1\n",
    "RELAY_PAIR_STATS_V1,src=1,dst=0,refused=shard\n",
    "MOQ5 Relay: stopping \xe2\x80\x94 conns 1, tracks 3, ingested 301, delivered 302, session errors 303\n",
    "MOQ5 Relay: shard 2 \xe2\x80\x94 conns 1, tracks 3, ingested 301, delivered 302, session errors 303, pump turns 71, wakes 73 (requests: push 74, credit 75, local 76)\n",
    "MOQ5 Relay: shard 2 \xe2\x80\x94 conns 1, tracks 3, ingested 301, delivered 302, session errors 303, pump turns 71, wakes 73 (shard stats refused: poisoned snapshot)\n",
    "MOQ5 Relay: stopping \xe2\x80\x94 total conns 4, tracks 9, ingested 901, delivered 902, session errors 903\n",
};
#define K_TEXT_BASELINE (sizeof(k_text_baseline) / sizeof(k_text_baseline[0]))

/* -- fixtures ----------------------------------------------------------------- */

static moqr_cli_lane_stats_row_t
lane_row(void)
{
    moqr_cli_lane_stats_row_t r;
    memset(&r, 0, sizeof(r));
    r.lane = 1; r.wakes_same_lane = 101; r.wakes_cross_lane = 102;
    r.wakes_external = 103; r.wakes_coalesced = 104; r.pump_sweeps = 105;
    r.deadline_sweeps = 106; r.idle_cap_wakes = 107; r.wake_to_pump_max_us = 108;
    r.wake_to_pump_total_us = 109; r.wake_to_pump_samples = 110;
    r.service_passes = 111; r.flush_sends = 112; r.flush_bytes = 113;
    r.pump_turns = 72; r.pump_messages = 115; r.pump_bytes = 116;
    r.wake_requests_push = 117; r.wake_requests_credit = 118;
    r.enq_demand = 119; r.enq_undemand = 120; r.enq_done = 121; r.enq_ack = 122;
    r.enq_obj = 123; r.enq_obj_open = 124; r.enq_obj_chunk = 125;
    r.enq_obj_end = 126; r.enq_obj_reset = 127; r.enq_grp_reset = 128;
    r.enq_grp_evict = 129; r.enq_sg_seal = 130; r.channel_entries_hwm = 131;
    r.channel_bytes_hwm = 132; r.turns_msg_budget = 133; r.turns_byte_budget = 134;
    r.turns_blocked = 135; r.turns_drained = 136; r.turns_with_messages = 137;
    r.arb_class_refusals = 138; r.wake_requests_local = 139;
    return r;
}

static moqr_cli_pair_stats_row_t
pair_row(void)
{
    moqr_cli_pair_stats_row_t p;
    memset(&p, 0, sizeof(p));
    p.src = 0; p.dst = 1; p.data_messages = 201; p.data_bytes = 202;
    p.control_messages = 203; p.refused_entries = 204; p.refused_bytes = 205;
    return p;
}

static moqr_cli_run_config_row_t
run_config_row(void)
{
    moqr_cli_run_config_row_t rc;
    memset(&rc, 0, sizeof(rc));
    rc.pump_turn_messages = 64; rc.pump_turn_bytes = 1048576;
    rc.demand_channel_entries = 256; rc.demand_channel_bytes = 4194304;
    return rc;
}

#define SV(lit) (lit), (sizeof(lit) - 1u)

static moqr_cli_log_ready_t
ready_raw(uint32_t comp)
{
    moqr_cli_log_ready_t v;
    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_RAW; v.composition = comp;
    v.host = "10.0.0.7"; v.host_n = 8; v.port = 4433;
    v.alpn_set = "moqt-16+moqt-18"; v.alpn_n = 15; v.lanes = 2;
    return v;
}
static moqr_cli_log_ready_t
ready_admin(uint32_t comp)
{
    moqr_cli_log_ready_t v;
    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_ADMIN; v.composition = comp;
    v.host = "127.0.0.1"; v.host_n = 9; v.port = 9109;
    return v;
}
static moqr_cli_log_ready_t
ready_wt(void)
{
    moqr_cli_log_ready_t v;
    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_WEBTRANSPORT; v.composition = MOQR_CLI_LOG_COMP_LANES;
    v.host = "10.0.0.8"; v.host_n = 8; v.port = 4434; v.path = "/moq"; v.path_n = 4;
    v.alpn_set = "moqt-16+moqt-18"; v.alpn_n = 15; v.profile = "current";
    v.profile_n = 7; v.lanes = 1;
    return v;
}
static moqr_cli_log_ready_t
ready_signals(uint32_t comp)
{
    moqr_cli_log_ready_t v;
    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_SIGNALS; v.composition = comp;
    return v;
}
static moqr_cli_log_stop_t
stop_k1(void)
{
    moqr_cli_log_stop_t s;
    memset(&s, 0, sizeof(s));
    s.conns = 1; s.tracks = 3; s.ingested = 301; s.delivered = 302;
    s.session_errors = 303;
    return s;
}
static moqr_cli_log_stop_t
stop_shard(bool poisoned)
{
    moqr_cli_log_stop_t s = stop_k1();
    s.per_shard = true; s.shard = 2; s.poisoned = poisoned;
    s.have_wake_requests = !poisoned;
    s.pump_turns = 71; s.wakes = 73;      /* serve-context sentinels */
    s.wr_push = 74; s.wr_credit = 75; s.wr_local = 76;
    return s;
}
static moqr_cli_log_stop_t
stop_total(void)
{
    moqr_cli_log_stop_t s;
    memset(&s, 0, sizeof(s));
    s.total = true; s.shards = 3; s.conns = 4; s.tracks = 9; s.ingested = 901;
    s.delivered = 902; s.session_errors = 903;
    return s;
}

/* The baseline sequence, through the sink. */
static void
emit_sequence(moqr_cli_log_t *log)
{
    moqr_cli_log_ready_t v;
    moqr_cli_lane_stats_row_t r = lane_row();
    moqr_cli_pair_stats_row_t p = pair_row();
    moqr_cli_run_config_row_t rc = run_config_row();
    moqr_cli_log_stop_t s;

    v = ready_raw(MOQR_CLI_LOG_COMP_K1); moqr_cli_log_ready(log, &v);
    v = ready_admin(MOQR_CLI_LOG_COMP_K1); moqr_cli_log_ready(log, &v);
    moqr_cli_log_run_config(log, &rc);
    moqr_cli_log_run_config_refused(log, "resolver");
    v = ready_signals(MOQR_CLI_LOG_COMP_K1); moqr_cli_log_ready(log, &v);
    v = ready_raw(MOQR_CLI_LOG_COMP_LANES); moqr_cli_log_ready(log, &v);
    v = ready_admin(MOQR_CLI_LOG_COMP_LANES); moqr_cli_log_ready(log, &v);
    v = ready_wt(); moqr_cli_log_ready(log, &v);
    v = ready_signals(MOQR_CLI_LOG_COMP_LANES); moqr_cli_log_ready(log, &v);
    moqr_cli_log_lane(log, &r);
    moqr_cli_log_lane_refused(log, 2, "adapter");
    moqr_cli_log_lane_refused(log, 3, "shard");
    moqr_cli_log_lane_refused(log, 4, "adapter");
    moqr_cli_log_pair(log, &p);
    moqr_cli_log_pair_refused(log, 1, 0, "shard");
    s = stop_k1(); moqr_cli_log_stop(log, &s);
    s = stop_shard(false); moqr_cli_log_stop(log, &s);
    s = stop_shard(true); moqr_cli_log_stop(log, &s);
    s = stop_total(); moqr_cli_log_stop(log, &s);
}

/* -- scripted I/O ------------------------------------------------------------- */

typedef struct script {
    /* clock: a fixed list of samples; a negative entry is a failed read */
    long long clock[32];
    unsigned  nclock;
    unsigned  clock_i;
    /* write: bytes to accept on the Nth call (-1 = error); after the list,
     * everything is accepted */
    long      write_take[32];
    unsigned  nwrite;
    unsigned  write_i;
    /* flush: which flush numbers (1-based) fail */
    unsigned  flush_fail_at[4];
    unsigned  nflush_fail;
    /* observed */
    unsigned  write_calls;
    unsigned  flush_calls;
    unsigned  clock_calls;
    char      out[65536];
    size_t    out_n;
} script_t;

static bool
sc_clock(void *ctx, uint64_t *out)
{
    script_t *s = (script_t *)ctx;
    s->clock_calls++;
    if (s->clock_i >= s->nclock) {
        return false;
    }
    if (s->clock[s->clock_i] < 0) {
        s->clock_i++;
        return false;
    }
    *out = (uint64_t)s->clock[s->clock_i++];
    return true;
}

static size_t
sc_write(void *ctx, FILE *f, const char *p, size_t n)
{
    script_t *s = (script_t *)ctx;
    size_t take = n;
    (void)f;
    s->write_calls++;
    if (s->write_i < s->nwrite) {
        long t = s->write_take[s->write_i++];
        if (t < 0) {
            return 0;
        }
        take = (size_t)t < n ? (size_t)t : n;
    }
    if (take > sizeof(s->out) - s->out_n - 1u) {
        take = sizeof(s->out) - s->out_n - 1u;
    }
    memcpy(s->out + s->out_n, p, take);
    s->out_n += take;
    s->out[s->out_n] = '\0';
    return take;
}

static int
sc_flush(void *ctx, FILE *f)
{
    script_t *s = (script_t *)ctx;
    (void)f;
    s->flush_calls++;
    for (unsigned i = 0; i < s->nflush_fail; i++) {
        if (s->flush_fail_at[i] == s->flush_calls) {
            return EOF;
        }
    }
    return 0;
}

static void
sc_init(script_t *s)
{
    memset(s, 0, sizeof(*s));
    s->clock[0] = 1000; s->nclock = 1;   /* the baseline only */
}

static void
sc_clocks(script_t *s, const long long *v, unsigned n)
{
    memcpy(s->clock, v, n * sizeof(v[0]));
    s->nclock = n;
    s->clock_i = 0;
}

static moqr_cli_log_io_t
sc_io(script_t *s)
{
    moqr_cli_log_io_t io = { sc_clock, sc_write, sc_flush, s };
    return io;
}

/* A test-owned prose stream. */
static FILE *
prose_open(void)
{
    return tmpfile();
}

static size_t
prose_read(FILE *f, char *out, size_t cap)
{
    size_t n;
    fflush(f);
    rewind(f);
    n = fread(out, 1, cap - 1u, f);
    out[n] = '\0';
    return n;
}

static unsigned
count_lines(const char *s)
{
    unsigned n = 0;
    for (; *s; s++) {
        if (*s == '\n') {
            n++;
        }
    }
    return n;
}

/* -- text ---------------------------------------------------------------------- */

/* The frozen baseline, byte for byte, in default text mode; nothing on a
 * separate prose stream; the readiness flush is exactly one flush and the
 * emits are none. */
static int
test_text_baseline(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose = prose_open();
    char pbuf[256];
    size_t expect_n = 0;

    sc_init(&sc);
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_TEXT, stdout,
                                            prose, &io), MOQR_OK);
    if (moqr_cli_log_prose_stream(&log) != stdout) {
        printf("  text: prose does not go to stdout\n");
        failures++;
    }
    emit_sequence(&log);
    for (size_t i = 0; i < K_TEXT_BASELINE; i++) {
        size_t l = strlen(k_text_baseline[i]);
        if (sc.out_n < expect_n + l ||
            memcmp(sc.out + expect_n, k_text_baseline[i], l) != 0) {
            printf("  text: baseline line %zu differs:\n    got  [%.*s]\n    want [%s]",
                   i, (int)(sc.out_n > expect_n ? (sc.out_n - expect_n < 200 ? sc.out_n - expect_n : 200) : 0),
                   sc.out + expect_n, k_text_baseline[i]);
            failures++;
            break;
        }
        expect_n += l;
    }
    if (sc.out_n != expect_n) {
        printf("  text: %zu bytes emitted, baseline is %zu\n", sc.out_n, expect_n);
        failures++;
    }
    if (sc.clock_calls != 0) {
        printf("  text: the clock was sampled %u times\n", sc.clock_calls);
        failures++;
    }
    if (prose_read(prose, pbuf, sizeof(pbuf)) != 0) {
        printf("  text: something reached the prose stream: [%s]\n", pbuf);
        failures++;
    }
    /* Flush points: emits flush nothing; the readiness flush is one flush. */
    if (sc.flush_calls != 0) {
        printf("  text: emits performed %u flushes\n", sc.flush_calls);
        failures++;
    }
    moqr_cli_log_readiness_flush(&log);
    if (sc.flush_calls != 1) {
        printf("  text: the readiness flush performed %u flushes\n", sc.flush_calls);
        failures++;
    }
    /* Ordinary-return finalization adds no bytes and no flush in text mode. */
    moqr_cli_log_finish(&log);
    if (sc.flush_calls != 1 || sc.out_n != expect_n) {
        printf("  text: finish changed the stream (flushes %u, bytes %zu)\n",
               sc.flush_calls, sc.out_n);
        failures++;
    }
    fclose(prose);
    return failures;
}

/* -- JSON ---------------------------------------------------------------------- */

typedef struct jline {
    struct json_value_s  *root;
    struct json_object_s *obj;
} jline_t;

static bool
jparse(const char *line, size_t n, jline_t *out)
{
    out->obj = NULL;
    out->root = json_parse(line, n);
    if (out->root == NULL) {
        return false;
    }
    out->obj = json_value_as_object(out->root);
    return out->obj != NULL;
}

static struct json_object_element_s *
jfield(const jline_t *j, size_t index)
{
    struct json_object_element_s *e = j->obj != NULL ? j->obj->start : NULL;
    for (size_t i = 0; i < index && e != NULL; i++) {
        e = e->next;
    }
    return e;
}

static struct json_value_s *
jget(const jline_t *j, const char *key)
{
    if (j->obj == NULL) {
        return NULL;
    }
    for (struct json_object_element_s *e = j->obj->start; e != NULL; e = e->next) {
        if (strcmp(e->name->string, key) == 0) {
            return e->value;
        }
    }
    return NULL;
}

static bool
jnum_is(const jline_t *j, const char *key, const char *decimal)
{
    struct json_value_s *v = jget(j, key);
    struct json_number_s *n = v != NULL ? json_value_as_number(v) : NULL;
    return n != NULL && strcmp(n->number, decimal) == 0;
}

static bool
jstr_is(const jline_t *j, const char *key, const char *want)
{
    struct json_value_s *v = jget(j, key);
    struct json_string_s *s = v != NULL ? json_value_as_string(v) : NULL;
    return s != NULL && strcmp(s->string, want) == 0;
}

static bool
jnull_is(const jline_t *j, const char *key)
{
    struct json_value_s *v = jget(j, key);
    return v != NULL && json_value_is_null(v);
}

/* Split captured output into lines (in place). */
static unsigned
split_lines(char *buf, char *lines[], unsigned cap)
{
    unsigned n = 0;
    char *p = buf;
    while (*p && n < cap) {
        char *nl = strchr(p, '\n');
        lines[n++] = p;
        if (nl == NULL) {
            break;
        }
        *nl = '\0';
        p = nl + 1;
    }
    return n;
}

static int
test_json_events(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose = prose_open();
    char *lines[32];
    unsigned n;
    static const long long clocks[] = { 1000, 1000, 1012, 1015, 1015, 1020,
                                        2000, 2001, 2002, 2003, 3000, 3001,
                                        3002, 3003, 3004, 3005, 4000, 4001,
                                        4002, 4003 };
    char pbuf[512];

    sc_init(&sc);
    sc_clocks(&sc, clocks, sizeof(clocks) / sizeof(clocks[0]));
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout,
                                            prose, &io), MOQR_OK);
    if (moqr_cli_log_prose_stream(&log) != prose) {
        printf("  json: prose does not go to the prose stream\n");
        failures++;
    }
    emit_sequence(&log);
    moqr_cli_log_finish(&log);
    if (prose_read(prose, pbuf, sizeof(pbuf)) != 0) {
        printf("  json: a diagnostic was printed for a clean run: [%s]\n", pbuf);
        failures++;
    }
    n = split_lines(sc.out, lines, 32);
    if (n != K_TEXT_BASELINE) {
        printf("  json: %u events for %zu baseline lines\n", n, K_TEXT_BASELINE);
        failures++;
    }
    /* every line is one JSON object whose first two keys are the envelope */
    for (unsigned i = 0; i < n; i++) {
        jline_t j;
        struct json_object_element_s *e0, *e1;
        if (!jparse(lines[i], strlen(lines[i]), &j)) {
            printf("  json: line %u does not parse: [%.80s]\n", i, lines[i]);
            failures++;
            continue;
        }
        e0 = jfield(&j, 0); e1 = jfield(&j, 1);
        if (e0 == NULL || e1 == NULL || strcmp(e0->name->string, "schema") != 0 ||
            strcmp(e1->name->string, "elapsed_us") != 0) {
            printf("  json: line %u envelope is wrong\n", i);
            failures++;
        }
        for (struct json_object_element_s *e = j.obj->start; e != NULL; e = e->next) {
            static const char *const banned[] = { "trace_id", "span_id",
                                                  "resource", "timestamp",
                                                  "time", "ts" };
            for (size_t b = 0; b < sizeof(banned) / sizeof(banned[0]); b++) {
                if (strcmp(e->name->string, banned[b]) == 0) {
                    printf("  json: forbidden key %s on line %u\n", banned[b], i);
                    failures++;
                }
            }
        }
        free(j.root);
    }
    if (n < 19) {
        fclose(prose);
        return failures + 1;
    }
    /* exact values, line by line (order = emit_sequence) */
    {
        jline_t j;
        /* 0: READY raw K=1 -- elapsed 1000-1000 = 0 */
        MOQ_TEST_CHECK(jparse(lines[0], strlen(lines[0]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "schema", "RELAY_READY_V1"));
        MOQ_TEST_CHECK(jnum_is(&j, "elapsed_us", "0"));
        MOQ_TEST_CHECK(jstr_is(&j, "listener", "raw"));
        MOQ_TEST_CHECK(jstr_is(&j, "host", "10.0.0.7"));
        MOQ_TEST_CHECK(jnum_is(&j, "port", "4433"));
        MOQ_TEST_CHECK(jstr_is(&j, "alpn_set", "moqt-16+moqt-18"));
        MOQ_TEST_CHECK(jnum_is(&j, "lanes", "1"));   /* K=1: one lane */
        free(j.root);
        /* 1: READY admin: the full closed target list */
        MOQ_TEST_CHECK(jparse(lines[1], strlen(lines[1]), &j));
        MOQ_TEST_CHECK(jnum_is(&j, "elapsed_us", "12"));
        MOQ_TEST_CHECK(jstr_is(&j, "listener", "admin"));
        MOQ_TEST_CHECK(jnum_is(&j, "port", "9109"));
        {
            struct json_value_s *tv = jget(&j, "targets");
            struct json_array_s *ta = tv != NULL ? json_value_as_array(tv) : NULL;
            size_t k = 0;
            if (ta == NULL || ta->length != MOQR_CLI_ADMIN_TARGET_COUNT) {
                printf("  json: admin targets array is wrong\n");
                failures++;
            } else {
                for (struct json_array_element_s *ae = ta->start; ae != NULL; ae = ae->next, k++) {
                    struct json_string_s *ts = json_value_as_string(ae->value);
                    if (ts == NULL || strcmp(ts->string, moqr_cli_admin_target(k)) != 0) {
                        printf("  json: target %zu is wrong\n", k);
                        failures++;
                    }
                }
            }
        }
        free(j.root);
        /* 2: RUN_CONFIG */
        MOQ_TEST_CHECK(jparse(lines[2], strlen(lines[2]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "schema", "RELAY_RUN_CONFIG_V1"));
        MOQ_TEST_CHECK(jnum_is(&j, "pump_turn_messages", "64"));
        MOQ_TEST_CHECK(jnum_is(&j, "pump_turn_bytes", "1048576"));
        MOQ_TEST_CHECK(jnum_is(&j, "demand_channel_entries", "256"));
        MOQ_TEST_CHECK(jnum_is(&j, "demand_channel_bytes", "4194304"));
        MOQ_TEST_CHECK(jget(&j, "eor") == NULL);
        free(j.root);
        /* 3: RUN_CONFIG refused resolver */
        MOQ_TEST_CHECK(jparse(lines[3], strlen(lines[3]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "refused", "resolver"));
        MOQ_TEST_CHECK(jget(&j, "pump_turn_messages") == NULL);
        free(j.root);
        /* 4: READY signals */
        MOQ_TEST_CHECK(jparse(lines[4], strlen(lines[4]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "listener", "signals"));
        MOQ_TEST_CHECK(jstr_is(&j, "sigusr1", "metrics+routes"));
        MOQ_TEST_CHECK(jstr_is(&j, "sigusr2", "trace"));
        MOQ_TEST_CHECK(jstr_is(&j, "sink", "stderr"));
        free(j.root);
        /* 7: READY webtransport */
        MOQ_TEST_CHECK(jparse(lines[7], strlen(lines[7]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "listener", "webtransport"));
        MOQ_TEST_CHECK(jstr_is(&j, "path", "/moq"));
        MOQ_TEST_CHECK(jstr_is(&j, "profile", "current"));
        MOQ_TEST_CHECK(jnum_is(&j, "port", "4434"));
        MOQ_TEST_CHECK(jnum_is(&j, "lanes", "1"));
        free(j.root);
        /* 9: LANE row: 39 numeric fields after schema, elapsed_us, lane */
        MOQ_TEST_CHECK(jparse(lines[9], strlen(lines[9]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "schema", "RELAY_LANE_STATS_V3"));
        MOQ_TEST_CHECK(jnum_is(&j, "lane", "1"));
        MOQ_TEST_CHECK(jnum_is(&j, "wakes_same_lane", "101"));
        MOQ_TEST_CHECK(jnum_is(&j, "pump_turns", "72"));
        MOQ_TEST_CHECK(jnum_is(&j, "wake_requests_local", "139"));
        {
            size_t count = 0;
            for (struct json_object_element_s *e = j.obj->start; e != NULL; e = e->next) count++;
            if (count != 3u + 39u) {
                printf("  json: lane row has %zu members, want 42\n", count);
                failures++;
            }
        }
        free(j.root);
        /* 10-12: refused lane rows */
        MOQ_TEST_CHECK(jparse(lines[10], strlen(lines[10]), &j));
        MOQ_TEST_CHECK(jnum_is(&j, "lane", "2") && jstr_is(&j, "refused", "adapter"));
        free(j.root);
        MOQ_TEST_CHECK(jparse(lines[11], strlen(lines[11]), &j));
        MOQ_TEST_CHECK(jnum_is(&j, "lane", "3") && jstr_is(&j, "refused", "shard"));
        free(j.root);
        /* 13: PAIR */
        MOQ_TEST_CHECK(jparse(lines[13], strlen(lines[13]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "schema", "RELAY_PAIR_STATS_V1"));
        MOQ_TEST_CHECK(jnum_is(&j, "src", "0") && jnum_is(&j, "dst", "1"));
        MOQ_TEST_CHECK(jnum_is(&j, "refused_bytes", "205"));
        free(j.root);
        MOQ_TEST_CHECK(jparse(lines[14], strlen(lines[14]), &j));
        MOQ_TEST_CHECK(jnum_is(&j, "src", "1") && jnum_is(&j, "dst", "0") && jstr_is(&j, "refused", "shard"));
        free(j.root);
        /* 15: STOP K=1: no per-shard extras */
        MOQ_TEST_CHECK(jparse(lines[15], strlen(lines[15]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "schema", "RELAY_STOP_V1"));
        MOQ_TEST_CHECK(jnum_is(&j, "shard", "0") && jnum_is(&j, "conns", "1") &&
                       jnum_is(&j, "session_errors", "303"));
        MOQ_TEST_CHECK(jget(&j, "pump_turns") == NULL && jget(&j, "wakes") == NULL &&
                       jget(&j, "wake_requests") == NULL && jget(&j, "total") == NULL);
        free(j.root);
        /* 16: STOP shard healthy: context counters + wake_requests */
        MOQ_TEST_CHECK(jparse(lines[16], strlen(lines[16]), &j));
        MOQ_TEST_CHECK(jnum_is(&j, "shard", "2") && jnum_is(&j, "pump_turns", "71") &&
                       jnum_is(&j, "wakes", "73"));
        {
            struct json_value_s *wv = jget(&j, "wake_requests");
            struct json_object_s *wo = wv != NULL ? json_value_as_object(wv) : NULL;
            jline_t wj = { NULL, wo };
            if (wo == NULL || !jnum_is(&wj, "push", "74") || !jnum_is(&wj, "credit", "75") ||
                !jnum_is(&wj, "local", "76")) {
                printf("  json: healthy shard wake_requests is wrong\n");
                failures++;
            }
        }
        MOQ_TEST_CHECK(jget(&j, "refused") == NULL);
        free(j.root);
        /* 17: STOP shard poisoned: context counters, no wake_requests */
        MOQ_TEST_CHECK(jparse(lines[17], strlen(lines[17]), &j));
        MOQ_TEST_CHECK(jstr_is(&j, "refused", "poisoned") && jnum_is(&j, "pump_turns", "71") &&
                       jnum_is(&j, "wakes", "73") && jget(&j, "wake_requests") == NULL);
        free(j.root);
        /* 18: STOP total */
        MOQ_TEST_CHECK(jparse(lines[18], strlen(lines[18]), &j));
        MOQ_TEST_CHECK(jget(&j, "total") != NULL && json_value_is_true(jget(&j, "total")));
        MOQ_TEST_CHECK(jnum_is(&j, "shards", "3") && jnum_is(&j, "ingested", "901"));
        MOQ_TEST_CHECK(jget(&j, "shard") == NULL && jget(&j, "pump_turns") == NULL);
        free(j.root);
    }
    /* one write and one flush per event, none after finish beyond the final flush */
    if (sc.write_calls != n || sc.flush_calls != n + 1u) {
        printf("  json: %u writes / %u flushes for %u events (want %u / %u)\n",
               sc.write_calls, sc.flush_calls, n, n, n + 1u);
        failures++;
    }
    fclose(prose);
    return failures;
}

/* The JSON numeric records carry exactly the text records' fields. */
static int
test_key_parity(void)
{
    int failures = 0;
    char text[2048], jsonb[4096];
    size_t jl = 0;
    moqr_cli_lane_stats_row_t r = lane_row();
    moqr_cli_pair_stats_row_t p = pair_row();
    moqr_cli_run_config_row_t rc = run_config_row();
    struct { int tl; moqr_result_t jr; const char *coord[2]; } cases[3];

    cases[0].tl = moqr_cli_lane_stats_format(text, sizeof(text), &r);
    cases[0].jr = moqr_cli_lane_stats_json(jsonb, sizeof(jsonb), true, 5, &r, &jl);
    cases[0].coord[0] = "lane"; cases[0].coord[1] = NULL;
    for (int c = 0; c < 3; c++) {
        jline_t j;
        char *save;
        char *tok;
        if (c == 1) {
            cases[1].tl = moqr_cli_pair_stats_format(text, sizeof(text), &p);
            cases[1].jr = moqr_cli_pair_stats_json(jsonb, sizeof(jsonb), true, 5, &p, &jl);
            cases[1].coord[0] = "src"; cases[1].coord[1] = "dst";
        } else if (c == 2) {
            cases[2].tl = moqr_cli_run_config_format(text, sizeof(text), &rc);
            cases[2].jr = moqr_cli_run_config_json(jsonb, sizeof(jsonb), true, 5, &rc, &jl);
            cases[2].coord[0] = NULL; cases[2].coord[1] = NULL;
        }
        if (cases[c].tl <= 0 || cases[c].jr != MOQR_OK || !jparse(jsonb, jl, &j)) {
            printf("  parity[%d]: render failed (%d / %d)\n", c, cases[c].tl, (int)cases[c].jr);
            failures++;
            continue;
        }
        /* every text k=v (excluding prefix and eor) is a JSON member with the
         * same decimal value, and JSON has no members beyond envelope + coordinates + these */
        {
            size_t text_fields = 0, json_members = 0;
            for (struct json_object_element_s *e = j.obj->start; e != NULL; e = e->next) json_members++;
            tok = strtok_r(text, ",", &save);   /* the prefix */
            while ((tok = strtok_r(NULL, ",", &save)) != NULL) {
                char *eq = strchr(tok, '=');
                if (eq == NULL) { failures++; break; }
                *eq = '\0';
                if (strcmp(tok, "eor") == 0) continue;
                text_fields++;
                if (!jnum_is(&j, tok, eq + 1)) {
                    printf("  parity[%d]: %s=%s not carried identically\n", c, tok, eq + 1);
                    failures++;
                }
            }
            if (json_members != 2u + text_fields) {   /* schema + elapsed_us + (coords + values) */
                printf("  parity[%d]: %zu JSON members for %zu text fields\n", c, json_members, text_fields);
                failures++;
            }
        }
        free(j.root);
    }
    if (moqr_cli_lane_stats_field_count() != 39u || moqr_cli_pair_stats_field_count() != 5u ||
        moqr_cli_run_config_field_count() != 4u) {
        printf("  parity: field counts are %zu/%zu/%zu\n", moqr_cli_lane_stats_field_count(),
               moqr_cli_pair_stats_field_count(), moqr_cli_run_config_field_count());
        failures++;
    }
    return failures;
}

/* UINT64_MAX renders as its exact decimal; the refusal vocabulary is closed. */
static int
test_json_extremes(void)
{
    int failures = 0;
    char b[4096];
    size_t n = 0;
    moqr_cli_lane_stats_row_t r;
    jline_t j;

    memset(&r, 0xff, sizeof(r));
    r.lane = UINT32_MAX;
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_lane_stats_json(b, sizeof(b), true, UINT64_MAX, &r, &n), MOQR_OK);
    MOQ_TEST_CHECK(jparse(b, n, &j));
    MOQ_TEST_CHECK(jnum_is(&j, "elapsed_us", "18446744073709551615"));
    MOQ_TEST_CHECK(jnum_is(&j, "lane", "4294967295"));
    MOQ_TEST_CHECK(jnum_is(&j, "flush_bytes", "18446744073709551615"));
    free(j.root);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_lane_stats_json(b, sizeof(b), false, 77, &r, &n), MOQR_OK);
    MOQ_TEST_CHECK(jparse(b, n, &j));
    MOQ_TEST_CHECK(jnull_is(&j, "elapsed_us"));
    free(j.root);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_lane_stats_refused_json(b, sizeof(b), true, 1, 4, "bogus", &n), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_pair_stats_refused_json(b, sizeof(b), true, 1, 0, 1, "adapter", &n), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_run_config_refused_json(b, sizeof(b), true, 1, "shard", &n), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_run_config_refused_json(b, sizeof(b), false, 1, "format", &n), MOQR_OK);
    MOQ_TEST_CHECK(jparse(b, n, &j));
    MOQ_TEST_CHECK(jnull_is(&j, "elapsed_us") && jstr_is(&j, "refused", "format"));
    free(j.root);
    /* the fixed fallback honours caller bounds: a too-small buffer is CAPACITY */
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_run_config_refused_json(b, 8, true, 1, "format", &n), MOQR_ERR_CAPACITY);
    return failures;
}

/* -- bounds -------------------------------------------------------------------- */

/* An independently counted escaped-string fixture (WT readiness with 255
 * control bytes of host and path), and a max-width numeric fixture whose
 * length is computed from the text row's own keys. */
static int
test_bounds(void)
{
    int failures = 0;
    static char host[256], path[256], want[8192], got[8192], text[2048];
    size_t n = 0, want_n = 0;
    moqr_cli_log_ready_t v;

    memset(host, 0x1f, 255); host[255] = '\0';
    path[0] = '/'; memset(path + 1, 0x1f, 254); path[255] = '\0';
    memset(&v, 0, sizeof(v));
    v.kind = MOQR_CLI_LOG_READY_WEBTRANSPORT; v.composition = MOQR_CLI_LOG_COMP_LANES;
    v.host = host; v.host_n = 255; v.port = 65535; v.path = path; v.path_n = 255;
    v.alpn_set = "moqt-16+moqt-18+moqt-16+moqt-18"; v.alpn_n = 31;
    v.profile = "d02_rfc9297_compat"; v.profile_n = 18; v.lanes = 64;
    /* the expected bytes, written here by hand */
    {
        size_t k = 0;
        k += (size_t)snprintf(want + k, sizeof(want) - k,
            "{\"schema\":\"RELAY_READY_V1\",\"elapsed_us\":18446744073709551615,"
            "\"listener\":\"webtransport\",\"host\":\"");
        for (int i = 0; i < 255; i++) { memcpy(want + k, "\\u001f", 6); k += 6; }
        k += (size_t)snprintf(want + k, sizeof(want) - k, "\",\"port\":65535,\"path\":\"/");
        for (int i = 0; i < 254; i++) { memcpy(want + k, "\\u001f", 6); k += 6; }
        k += (size_t)snprintf(want + k, sizeof(want) - k,
            "\",\"alpn_set\":\"moqt-16+moqt-18+moqt-16+moqt-18\","
            "\"profile\":\"d02_rfc9297_compat\",\"lanes\":64}");
        want[k] = '\0';
        want_n = k;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_ready_json(got, sizeof(got), true, UINT64_MAX, &v, &n), MOQR_OK);
    if (n != want_n || strcmp(got, want) != 0) {
        printf("  bounds: escaped READY differs (%zu vs %zu)\n", n, want_n);
        failures++;
    }
    /* exact fit and one short, against the independently known length */
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_ready_json(got, want_n + 1u, true, UINT64_MAX, &v, &n), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_ready_json(got, want_n, true, UINT64_MAX, &v, &n), MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(got[0] == '\0');
    if ((uint64_t)want_n > moqr_cli_log_line_bound() ||
        moqr_cli_log_line_bound() + 2u > MOQR_CLI_LOG_LINE_CAP) {
        printf("  bounds: the measured bound %llu does not cover %zu, or exceeds the cap\n",
               (unsigned long long)moqr_cli_log_line_bound(), want_n);
        failures++;
    }
    /*
     * The widest record the bound is actually claiming.
     *
     * The hand-written line above is one real record; this is the widest one,
     * built from the same maxima the bound is computed over. Rendering it must
     * take EXACTLY the advertised bound -- a bound one byte larger would be
     * slack nothing detects, and one byte smaller would truncate the longest
     * line the relay can emit. The fixed sink buffer must hold it and its
     * newline.
     */
    {
        static char wide_alpn[68];
        moqr_cli_log_ready_t w;
        size_t wn = 0;
        uint64_t bound = moqr_cli_log_line_bound();

        memset(wide_alpn, 0x1f, 67); wide_alpn[67] = '\0';
        memset(&w, 0, sizeof(w));
        w.kind = MOQR_CLI_LOG_READY_WEBTRANSPORT;
        w.composition = MOQR_CLI_LOG_COMP_LANES;
        w.host = host; w.host_n = 255;
        w.port = INT32_MIN;
        w.alpn_set = wide_alpn; w.alpn_n = 67;
        w.lanes = UINT32_MAX;
        w.path = host; w.path_n = 255;
        w.profile = "d02_rfc9297_compat"; w.profile_n = 18;

        MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_ready_json(got, sizeof(got), true,
                                                      UINT64_MAX, &w, &wn),
                              MOQR_OK);
        if ((uint64_t)wn != bound) {
            printf("  bounds: the widest READY is %zu bytes, the bound says "
                   "%llu\n", wn, (unsigned long long)bound);
            failures++;
        }
        /* exactly enough, and one byte short */
        MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_ready_json(got, (size_t)bound + 1u,
                                                      true, UINT64_MAX, &w, &wn),
                              MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_ready_json(got, (size_t)bound, true,
                                                      UINT64_MAX, &w, &wn),
                              MOQR_ERR_CAPACITY);
        MOQ_TEST_CHECK(got[0] == '\0');
        /* and the fixed sink buffer holds that line plus its newline */
        MOQ_TEST_CHECK(bound + 2u <= MOQR_CLI_LOG_LINE_CAP);
    }
    /* max-width LANE: expected length from the text row's keys */
    {
        moqr_cli_lane_stats_row_t r;
        char *save, *tok;
        size_t expect = 0;
        int tl;
        memset(&r, 0xff, sizeof(r));
        r.lane = UINT32_MAX;
        tl = moqr_cli_lane_stats_format(text, sizeof(text), &r);
        MOQ_TEST_CHECK(tl > 0 && (size_t)tl < sizeof(text));
        expect = 1 + strlen("\"schema\":\"RELAY_LANE_STATS_V3\",") +
                 strlen("\"elapsed_us\":18446744073709551615,") +
                 strlen("\"lane\":4294967295");
        tok = strtok_r(text, ",", &save);
        while ((tok = strtok_r(NULL, ",", &save)) != NULL) {
            char *eq = strchr(tok, '=');
            if (eq == NULL || strncmp(tok, "lane=", 5) == 0 || strncmp(tok, "eor=", 4) == 0) continue;
            expect += 1u + 1u + (size_t)(eq - tok) + 2u + 20u;   /* ,"key":<20 digits> */
        }
        expect += 1;   /* closing brace */
        MOQ_TEST_CHECK_EQ_INT(moqr_cli_lane_stats_json(got, sizeof(got), true, UINT64_MAX, &r, &n), MOQR_OK);
        if (n != expect) {
            printf("  bounds: max-width LANE is %zu bytes, computed %zu\n", n, expect);
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_cli_lane_stats_json(got, expect + 1u, true, UINT64_MAX, &r, &n), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_cli_lane_stats_json(got, expect, true, UINT64_MAX, &r, &n), MOQR_ERR_CAPACITY);
    }
    return failures;
}

/* -- the stream: latch on the first failure ---------------------------------- */

static int
test_sink_latches(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose = prose_open();
    char pbuf[1024];
    moqr_cli_log_ready_t v = ready_raw(MOQR_CLI_LOG_COMP_K1);
    static const long long clocks[] = { 1000, 1001, 1002, 1003, 1004, 1005 };

    /* 1. a 5-byte short write */
    sc_init(&sc);
    sc_clocks(&sc, clocks, 6);
    sc.write_take[0] = 5; sc.nwrite = 1;
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_ready(&log, &v);
    if (log.sink_state != MOQR_CLI_LOG_SINK_FAILED || sc.out_n != 5u ||
        memcmp(sc.out, "{\"sch", 5) != 0) {
        printf("  latch: a short write did not latch (state %u, %zu bytes)\n",
               (unsigned)log.sink_state, sc.out_n);
        failures++;
    }
    {
        unsigned w = sc.write_calls, f = sc.flush_calls, c = sc.clock_calls;
        moqr_cli_log_ready(&log, &v);
        moqr_cli_log_ready(&log, &v);
        moqr_cli_log_lane_refused(&log, 1, "adapter");
        moqr_cli_log_finish(&log);
        if (sc.write_calls != w || sc.flush_calls != f || sc.clock_calls != c) {
            printf("  latch: a failed sink still wrote/flushed/sampled (%u/%u/%u after %u/%u/%u)\n",
                   sc.write_calls, sc.flush_calls, sc.clock_calls, w, f, c);
            failures++;
        }
        if (log.dropped != 3u) {
            printf("  latch: %u events counted dropped, want 3\n", log.dropped);
            failures++;
        }
        if (sc.out_n != 5u) {
            printf("  latch: bytes were appended after the failure\n");
            failures++;
        }
    }
    prose_read(prose, pbuf, sizeof(pbuf));
    if (count_lines(pbuf) != 1u || strstr(pbuf, "stdout write failed") == NULL) {
        printf("  latch: diagnostics: [%s]\n", pbuf);
        failures++;
    }
    fclose(prose);

    /* 2. a write error (zero) on the second event */
    prose = prose_open();
    sc_init(&sc);
    sc_clocks(&sc, clocks, 6);
    sc.write_take[0] = 100000; sc.write_take[1] = -1; sc.nwrite = 2;
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_ready(&log, &v);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_OPEN);
    moqr_cli_log_ready(&log, &v);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_FAILED);
    MOQ_TEST_CHECK(count_lines(sc.out) == 1u);
    moqr_cli_log_finish(&log);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_FAILED);   /* terminal */
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u);
    fclose(prose);

    /* 3. an emit-time flush failure */
    prose = prose_open();
    sc_init(&sc);
    sc_clocks(&sc, clocks, 6);
    sc.flush_fail_at[0] = 2; sc.nflush_fail = 1;
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_ready(&log, &v);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_OPEN);
    moqr_cli_log_ready(&log, &v);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_FAILED);
    {
        unsigned w = sc.write_calls;
        moqr_cli_log_ready(&log, &v);
        MOQ_TEST_CHECK(sc.write_calls == w);
    }
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u);
    fclose(prose);

    /* 4. a FINAL flush failure: every emit flushed, only finish's fails */
    prose = prose_open();
    sc_init(&sc);
    sc_clocks(&sc, clocks, 6);
    sc.flush_fail_at[0] = 3; sc.nflush_fail = 1;   /* two emits, then finish */
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_ready(&log, &v);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_OPEN);
    moqr_cli_log_finish(&log);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_FAILED);
    MOQ_TEST_CHECK(sc.flush_calls == 3u);
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u);
    /* repeated finish: nothing more */
    moqr_cli_log_finish(&log);
    MOQ_TEST_CHECK(sc.flush_calls == 3u && sc.write_calls == 2u);
    fclose(prose);
    return failures;
}

/* The escaped prefix on a REAL FILE: the sink's own fwrite path through a
 * scripted stream (funopen), the probe's model. */
#if defined(__APPLE__) || defined(__FreeBSD__)
typedef struct fo {
    char bytes[512];
    size_t len;
    unsigned calls;
} fo_t;
static int
fo_write(void *cookie, const char *buf, int len)
{
    fo_t *o = (fo_t *)cookie;
    o->calls++;
    if (o->calls == 1u) {
        int take = len > 5 ? 5 : len;
        memcpy(o->bytes + o->len, buf, (size_t)take);
        o->len += (size_t)take;
        return take;
    }
    errno = EIO;
    return -1;
}
static int
test_real_stream_prefix(void)
{
    int failures = 0;
    fo_t o;
    FILE *f;
    FILE *prose = prose_open();
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    moqr_cli_log_ready_t v = ready_raw(MOQR_CLI_LOG_COMP_K1);
    char pbuf[512];
    static const long long clocks[] = { 1000, 1001, 1002 };

    memset(&o, 0, sizeof(o));
    f = funopen(&o, NULL, fo_write, NULL, NULL);
    MOQ_TEST_CHECK(f != NULL && setvbuf(f, NULL, _IONBF, 0) == 0);
    /* real fwrite/fflush, scripted clock only */
    sc_init(&sc);
    sc_clocks(&sc, clocks, 3);
    io = sc_io(&sc);
    io.write = NULL;
    io.flush = NULL;
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, f, prose, &io), MOQR_OK);
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_finish(&log);
    if (o.len != 5u || memcmp(o.bytes, "{\"sch", 5) != 0 || log.sink_state != MOQR_CLI_LOG_SINK_FAILED) {
        printf("  prefix: %zu bytes reached the stream [%.*s], state %u\n", o.len, (int)o.len, o.bytes, (unsigned)log.sink_state);
        failures++;
    }
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u);
    fclose(f);
    fclose(prose);
    return failures;
}
#else
static int test_real_stream_prefix(void) { printf("  prefix: no funopen on this platform; the scripted-io latch test stands\n"); return 0; }
#endif

/* -- the clock ----------------------------------------------------------------- */

static int
test_clock(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose;
    char pbuf[1024];
    char *lines[16];
    unsigned n;
    moqr_cli_log_ready_t v = ready_signals(MOQR_CLI_LOG_COMP_K1);
    jline_t j;

    /* a. baseline unavailable: every event null, one diagnostic, still emitted */
    prose = prose_open();
    sc_init(&sc);
    { static const long long c[] = { -1, 5, 6 }; sc_clocks(&sc, c, 3); }
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    MOQ_TEST_CHECK(log.clock_state == MOQR_CLI_LOG_CLOCK_UNAVAILABLE);
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_finish(&log);
    n = split_lines(sc.out, lines, 16);
    MOQ_TEST_CHECK(n == 2u);
    for (unsigned i = 0; i < n; i++) {
        MOQ_TEST_CHECK(jparse(lines[i], strlen(lines[i]), &j) && jnull_is(&j, "elapsed_us"));
        free(j.root);
    }
    /* no further clock reads once unavailable */
    MOQ_TEST_CHECK(sc.clock_calls == 1u);
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u && strstr(pbuf, "monotonic clock") != NULL);
    fclose(prose);

    /* b. the sequence: baseline 100; 100 -> 0, 300 -> 200, 250 -> null,
     *    260 -> null, 310 -> 210; a failed read -> null; recovery afterwards */
    prose = prose_open();
    sc_init(&sc);
    { static const long long c[] = { 100, 100, 300, 250, 260, 310, -1, 400 }; sc_clocks(&sc, c, 8); }
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    for (int i = 0; i < 7; i++) {
        moqr_cli_log_ready(&log, &v);
    }
    moqr_cli_log_finish(&log);
    n = split_lines(sc.out, lines, 16);
    MOQ_TEST_CHECK(n == 7u);
    if (n == 7u) {
        static const char *const want[7] = { "0", "200", NULL, NULL, "210", NULL, "300" };
        for (unsigned i = 0; i < 7; i++) {
            MOQ_TEST_CHECK(jparse(lines[i], strlen(lines[i]), &j));
            if (want[i] == NULL) {
                if (!jnull_is(&j, "elapsed_us")) { printf("  clock: event %u not null\n", i); failures++; }
            } else if (!jnum_is(&j, "elapsed_us", want[i])) {
                printf("  clock: event %u elapsed is not %s: [%.80s]\n", i, want[i], lines[i]);
                failures++;
            }
            free(j.root);
        }
    }
    MOQ_TEST_CHECK(log.base_us == 100u && log.last_us == 400u);
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u);
    fclose(prose);
    return failures;
}

/* -- the sink state machine ----------------------------------------------------- */

/* Every entry point checks the terminal state BEFORE it formats: after finish
 * (text and JSON), on a zeroed sink and on an invalid format, no emit touches
 * the line buffer, the rows stream, the flush count or the clock. */
static int
terminal_entries(moqr_cli_log_t *log, script_t *sc, const char *what)
{
    int failures = 0;
    char before[MOQR_CLI_LOG_LINE_CAP];
    unsigned writes = sc->write_calls, flushes = sc->flush_calls,
             clocks = sc->clock_calls;
    size_t n = sc->out_n;
    moqr_cli_lane_stats_row_t lr = lane_row();
    moqr_cli_pair_stats_row_t pr = pair_row();
    moqr_cli_run_config_row_t rc = run_config_row();
    moqr_cli_log_ready_t rd = ready_raw(MOQR_CLI_LOG_COMP_K1);
    moqr_cli_log_stop_t st = stop_k1();

    memset(log->line, 'Q', sizeof(log->line));
    memcpy(before, log->line, sizeof(before));
    moqr_cli_log_ready(log, &rd);
    moqr_cli_log_run_config(log, &rc);
    moqr_cli_log_run_config_refused(log, "resolver");
    moqr_cli_log_lane(log, &lr);
    moqr_cli_log_lane_refused(log, 2, "adapter");
    moqr_cli_log_pair(log, &pr);
    moqr_cli_log_pair_refused(log, 1, 0, "shard");
    moqr_cli_log_stop(log, &st);
    moqr_cli_log_readiness_flush(log);
    if (memcmp(before, log->line, sizeof(before)) != 0) {
        printf("  terminal/%s: an emit formatted into the line buffer\n", what);
        failures++;
    }
    if (sc->write_calls != writes || sc->flush_calls != flushes ||
        sc->clock_calls != clocks || sc->out_n != n) {
        printf("  terminal/%s: an emit reached the stream (writes %u->%u, "
               "flushes %u->%u, clocks %u->%u)\n", what, writes, sc->write_calls,
               flushes, sc->flush_calls, clocks, sc->clock_calls);
        failures++;
    }
    return failures;
}

static int
test_terminal_entry_points(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose = prose_open();

    sc_init(&sc);
    io = sc_io(&sc);
    /* text, after finish (repeatedly) */
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_TEXT, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_finish(&log);
    moqr_cli_log_finish(&log);
    failures += terminal_entries(&log, &sc, "text-closed");
    /* JSON, after finish */
    sc_init(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_finish(&log);
    moqr_cli_log_finish(&log);
    failures += terminal_entries(&log, &sc, "json-closed");
    /* a zeroed sink */
    memset(&log, 0, sizeof(log));
    log.io = &io;
    log.rows = stdout;
    log.prose = prose;
    failures += terminal_entries(&log, &sc, "zeroed");
    /* an invalid format with an OPEN state */
    memset(&log, 0, sizeof(log));
    log.io = &io;
    log.rows = stdout;
    log.prose = prose;
    log.format = (moqr_cli_log_format_t)5;
    log.sink_state = MOQR_CLI_LOG_SINK_OPEN;
    failures += terminal_entries(&log, &sc, "invalid-format");
    /* a valid format with an invalid state value */
    memset(&log, 0, sizeof(log));
    log.io = &io;
    log.rows = stdout;
    log.prose = prose;
    log.format = MOQR_CLI_LOG_JSON;
    log.sink_state = 9;
    failures += terminal_entries(&log, &sc, "invalid-state");
    /* the JSON FAILED state still counts what it drops, and nothing else */
    sc_init(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    log.sink_state = MOQR_CLI_LOG_SINK_FAILED;
    failures += terminal_entries(&log, &sc, "json-failed");
    if (log.dropped != 8u) {
        printf("  terminal/json-failed: dropped %u, expected 8\n", (unsigned)log.dropped);
        failures++;
    }
    if (prose_read(prose, (char[256]){0}, 256) != 0) {
        printf("  terminal: a diagnostic was printed\n");
        failures++;
    }
    fclose(prose);
    return failures;
}

static int
test_sink_states(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose = prose_open();
    moqr_cli_log_ready_t v = ready_signals(MOQR_CLI_LOG_COMP_K1);
    static const long long clocks[] = { 1, 2, 3, 4, 5 };

    /* invalid discriminants never fall through to a valid mode */
    sc_init(&sc);
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, 7u, stdout, prose, &io), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, NULL, prose, &io), MOQR_ERR_INVAL);
    /* OPEN -> CLOSED by finish; emit after finish does nothing at all */
    sc_init(&sc);
    sc_clocks(&sc, clocks, 5);
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_OPEN);
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_finish(&log);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_CLOSED);
    {
        unsigned w = sc.write_calls, f = sc.flush_calls, c = sc.clock_calls;
        moqr_cli_log_ready(&log, &v);
        moqr_cli_log_finish(&log);
        MOQ_TEST_CHECK(sc.write_calls == w && sc.flush_calls == f && sc.clock_calls == c);
        MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_CLOSED);
    }
    /* a sink that was never initialised (zeroed) accepts nothing */
    memset(&log, 0, sizeof(log));
    moqr_cli_log_ready(&log, &v);
    moqr_cli_log_finish(&log);
    fclose(prose);
    return failures;
}

/* -- render refusals ------------------------------------------------------------- */

static int
test_render_refusal(void)
{
    int failures = 0;
    moqr_cli_log_t log;
    script_t sc;
    moqr_cli_log_io_t io;
    FILE *prose = prose_open();
    char pbuf[1024];
    moqr_cli_log_ready_t bad = ready_raw(MOQR_CLI_LOG_COMP_K1);
    moqr_cli_log_ready_t good = ready_signals(MOQR_CLI_LOG_COMP_K1);
    static const long long clocks[] = { 1, 2, 3, 4, 5, 6 };

    bad.host = "h\xff"; bad.host_n = 2;   /* not UTF-8 */
    sc_init(&sc);
    sc_clocks(&sc, clocks, 6);
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_ready(&log, &bad);
    moqr_cli_log_ready(&log, &bad);
    moqr_cli_log_ready(&log, &good);
    moqr_cli_log_finish(&log);
    /* the refused events were dropped, the sink stayed OPEN, the good one landed */
    MOQ_TEST_CHECK(log.dropped == 2u && count_lines(sc.out) == 1u && strstr(sc.out, "signals") != NULL);
    MOQ_TEST_CHECK(log.sink_state == MOQR_CLI_LOG_SINK_CLOSED);
    prose_read(prose, pbuf, sizeof(pbuf));
    if (count_lines(pbuf) != 1u || strstr(pbuf, "RELAY_READY_V1") == NULL) {
        printf("  refusal: diagnostics: [%s]\n", pbuf);
        failures++;
    }
    /* an invalid refusal vocabulary word is a render refusal too, and it is
     * NEVER relabelled as an acquisition refusal */
    fclose(prose);
    prose = prose_open();
    sc_init(&sc);
    sc_clocks(&sc, clocks, 6);
    io = sc_io(&sc);
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_log_init(&log, MOQR_CLI_LOG_JSON, stdout, prose, &io), MOQR_OK);
    moqr_cli_log_lane_refused(&log, 1, "bogus");
    moqr_cli_log_finish(&log);
    MOQ_TEST_CHECK(log.dropped == 1u && sc.out_n == 0u);
    prose_read(prose, pbuf, sizeof(pbuf));
    MOQ_TEST_CHECK(count_lines(pbuf) == 1u && strstr(pbuf, "RELAY_LANE_STATS_V3") != NULL);
    fclose(prose);
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_text_baseline();
    failures += test_json_events();
    failures += test_key_parity();
    failures += test_json_extremes();
    failures += test_bounds();
    failures += test_sink_latches();
    failures += test_real_stream_prefix();
    failures += test_clock();
    failures += test_sink_states();
    failures += test_terminal_entry_points();
    failures += test_render_refusal();
    if (failures != 0) {
        printf("FAIL: %d log sink violation(s)\n", failures);
        return 1;
    }
    printf("PASS: relay_log_sink\n");
    return 0;
}
