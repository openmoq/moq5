/*
 * Contract test for the BLOCK_TIMEOUT wait's deadline arithmetic and error
 * handling, driven through the REAL public write/backpressure path.
 *
 * The queue is filled through moq_media_sender_write, so the second write
 * enters the production BLOCK_TIMEOUT branch. Inside that branch the two
 * clock reads and the timed wait are routed through the test rail, so this
 * oracle DECLARES every clock value and wait result up front and OBSERVES the
 * exact absolute time the product passes to each wait. Expected values are
 * computed in the vector tables, independently of the implementation.
 *
 * SCOPE AND LIMITS.
 *  - A scripted wait is a MECHANISM test. It does not prove a kernel wait or
 *    any target runtime occurred; the real wait/terminal coverage stays in
 *    test_media_sender_terminal_boundary.
 *  - The host time_t width is pinned below. Vectors that depend on it are
 *    compiled only for that width; nothing here is an execution claim for a
 *    narrower type, and size_t width is an independent fact.
 *  - A signed tv_sec overflow is undefined behaviour and is NEVER executed:
 *    the arithmetic MODEL covers it, and a model result is not a runtime
 *    observation.
 *  - Each vector declares the EXACT number of clock and wait calls a correct
 *    implementation may make. Script entries beyond that prefix are provisioned
 *    only so an incorrect implementation cannot run off the end; consuming them
 *    is a named failure, not a pass.
 *  - An independent hard call guard ends the process nonzero with a named
 *    diagnostic if the product keeps waiting legitimately, without relying on
 *    the outer CTest timeout.
 *  - The CMAF arm measures OWNERSHIP of non-NULL opaque property bytes under
 *    the documented explicit-passthrough option. It does not validate that
 *    those bytes are a conforming property encoding, and claims nothing about
 *    property parsing.
 *
 * White-box: links moq-service-sender-test-internals (MOQ_MEDIA_SENDER_TESTING).
 */
#define _POSIX_C_SOURCE 200809L
#include <moq/media_sender.h>
#include "test_support.h"
#include <errno.h>
#include <inttypes.h>
#include <spawn.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

_Static_assert(sizeof(time_t) == 8, "these host vectors are written for a 64-bit time_t");

static int failures = 0;

/* Captured failure identities, for the perturbation arms only. */
static bool g_capture;
/* Fixture-local mutant switch: when set, cleanup follows the DECLARED
 * expectation instead of the actual result -- the behaviour 1904 removed.
 * Used only by an isolated sanitizer child control. */
static bool g_expectation_driven_cleanup;
static char g_captured[16][128];
static int  g_captured_n;

moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_clock_hook(int (*fn)(struct timespec *, void *), void *ctx);
void moq_media_sender_test_set_cond_wait_hook(int (*fn)(const struct timespec *, void *), void *ctx);
void moq_media_sender_test_release_one_locked(moq_media_sender_t *s);

static void row(const char *what, const char *name, bool ok, int line)
{
    if (ok) return;
    if (g_capture && g_captured_n < 16)
        snprintf(g_captured[g_captured_n++], sizeof(g_captured[0]), "%s.%s", what, name);
    fprintf(stderr, "FAIL[%s.%s]: %s:%d\n", what, name, __FILE__, line);
    failures++;
}
#define ROW(what, name, cond) row((what), (name), (cond), __LINE__)

/* -- scripted clock and wait ---------------------------------------------- */
#define MAX_STEPS 16
#define MAX_OBS   64
/* Independent of script consumption: the product cannot exceed this many hook
 * calls, however it treats the results. */
#define HARD_CALL_LIMIT 48

typedef struct { int rc; long long sec; long nsec; } clock_step_t;
typedef struct { int rc; bool free_one; } wait_step_t;

typedef struct {
    clock_step_t clock[MAX_STEPS];
    int clock_n, clock_used, clock_calls;   /* consumed entries vs ACTUAL calls */
    wait_step_t wait[MAX_STEPS];
    int wait_n, wait_used, wait_calls;
    struct timespec observed[MAX_OBS];
    int observed_n;
    bool observation_overflow;              /* sticky */
    moq_media_sender_t *sender;
    long long fallback_sec;                 /* past every declared deadline */
    bool no_fallback;                       /* guard control: never let the product exit */
} script_t;

static script_t g_script;

static void hard_guard(script_t *sc, const char *where)
{
    if (sc->clock_calls + sc->wait_calls <= HARD_CALL_LIMIT) return;
    fprintf(stderr, "FAIL[guard.hard_call_limit]: %s exceeded %d hook calls"
            " (clock %d, wait %d); ending the process\n",
            where, HARD_CALL_LIMIT, sc->clock_calls, sc->wait_calls);
    fflush(stderr);
    _Exit(EXIT_FAILURE);
}

static int scripted_clock(struct timespec *ts, void *ctx)
{
    script_t *sc = (script_t *)ctx;
    sc->clock_calls++;
    hard_guard(sc, "clock");
    if (sc->clock_used >= sc->clock_n) {
        /* Past every declared deadline, so an implementation that ignores its
         * results still terminates -- except in the guard control. */
        ts->tv_sec = (time_t)(sc->no_fallback
                              ? (sc->clock_n ? sc->clock[sc->clock_n - 1].sec : 0)
                              : sc->fallback_sec);
        ts->tv_nsec = 0;
        return 0;
    }
    const clock_step_t *st = &sc->clock[sc->clock_used++];
    ts->tv_sec = (time_t)st->sec;
    ts->tv_nsec = st->nsec;
    return st->rc;
}

static int scripted_wait(const struct timespec *abs, void *ctx)
{
    script_t *sc = (script_t *)ctx;
    sc->wait_calls++;
    hard_guard(sc, "wait");
    if (sc->observed_n < MAX_OBS) sc->observed[sc->observed_n++] = *abs;
    else sc->observation_overflow = true;
    if (sc->wait_used >= sc->wait_n)
        return sc->no_fallback ? 0 : ETIMEDOUT;   /* guard: valid spurious wake */
    const wait_step_t *st = &sc->wait[sc->wait_used++];
    if (st->free_one) moq_media_sender_test_release_one_locked(sc->sender);
    return st->rc;
}

static void script_install(moq_media_sender_t *s) { g_script.sender = s;
    moq_media_sender_test_set_clock_hook(scripted_clock, &g_script);
    moq_media_sender_test_set_cond_wait_hook(scripted_wait, &g_script); }
static void script_remove(void) {
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    moq_media_sender_test_set_cond_wait_hook(NULL, NULL); }

/* -- fixture -------------------------------------------------------------- */
static const uint8_t NS0[] = "live";
static const uint8_t NAME[] = "video";
static const uint8_t CODEC[] = "avc1.64001f";

static moq_media_sender_t *lossless_sender(uint64_t block_timeout_us, bool cmaf,
                                           moq_media_track_t **t)
{
    static const moq_bytes_t parts[1] = { { NS0, 4 } };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_lossless_sized(&cfg, sizeof(cfg));
    cfg.namespace_.parts = parts; cfg.namespace_.count = 1;
    cfg.block_timeout_us = block_timeout_us;
    cfg.queue_max_objects = 1;
    cfg.pre_ready_max_objects = 1;
    /* CMAF arm: explicit passthrough, so an opaque object carries its own
     * property block without a CMSF parse (the documented cfg option, not a
     * violation of the RAW property rule). */
    if (cmaf) cfg.validate_cmaf = false;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    if (!s) return NULL;
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ NAME, 5 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = cmaf ? MOQ_MEDIA_PACKAGING_CMAF : MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ CODEC, 11 };
    tc.bitrate = 1000000;
    if (moq_media_sender_add_track(s, &tc, t) != MOQ_OK) { moq_media_sender_test_free(s); return NULL; }
    return s;
}

static moq_rcbuf_t *payload(void)
{
    static const uint8_t bytes[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &b) != MOQ_OK) return NULL;
    return b;
}
static void send_obj(moq_media_send_object_t *o, moq_rcbuf_t *p, moq_rcbuf_t *props,
                     bool sync, bool starts)
{
    memset(o, 0, sizeof(*o));
    o->struct_size = sizeof(*o);
    o->payload = p;
    o->properties = props;
    o->is_sync = sync;
    o->starts_group = starts;
    o->presentation_time_us = 1000;
}

/* -- one vector ----------------------------------------------------------- */
typedef struct {
    const char *what;
    uint64_t    timeout_us;
    bool        cmaf;            /* CMAF track + a real property block */
    clock_step_t clock[MAX_STEPS]; int clock_n;
    wait_step_t  wait[MAX_STEPS];  int wait_n;
    long long   fallback_sec;
    /* declared expectations -- what a CORRECT implementation must do */
    moq_result_t expect_rc;
    bool        expect_transfer;
    int         expect_clock_calls;   /* exact; anything beyond is a failure */
    int         expect_wait_calls;
    struct { long long sec; long nsec; } expect_abs[MAX_OBS];
    int         expect_abs_n;         /* must equal expect_wait_calls */
    /* Test-only: simulate a consume-on-refusal defect by dropping the caller's
     * reference after a refusal, so the ownership checker can be exercised.
     * Used by the perturbation arms only; never set by a product vector. */
    bool        perturb_consume_on_refusal;
} vector_t;

static void run_vector(const vector_t *v)
{
    const char *what = v->what;
    /* Dimensions before any copy or index. */
    bool dims = v->clock_n >= 0 && v->clock_n <= MAX_STEPS &&
                v->wait_n  >= 0 && v->wait_n  <= MAX_STEPS &&
                v->expect_abs_n >= 0 && v->expect_abs_n <= MAX_OBS &&
                v->expect_abs_n == v->expect_wait_calls &&
                v->expect_clock_calls >= 0 && v->expect_wait_calls >= 0;
    ROW(what, "fixture.vector_dimensions", dims);
    if (!dims) return;

    moq_media_track_t *t = NULL;
    moq_media_sender_t *s = lossless_sender(v->timeout_us, v->cmaf, &t);
    ROW(what, "fixture.sender", s != NULL);
    if (!s) return;
    moq_rcbuf_t *first = payload(), *second = payload();
    moq_rcbuf_t *props = v->cmaf ? payload() : NULL;
    bool have = first && second && (!v->cmaf || props);
    ROW(what, "fixture.payloads", have);
    if (!have) {
        if (first) moq_rcbuf_decref(first);
        if (second) moq_rcbuf_decref(second);
        if (props) moq_rcbuf_decref(props);
        moq_media_sender_test_free(s);
        return;
    }
    moq_media_send_object_t o;
    send_obj(&o, first, v->cmaf ? payload() : NULL, true, true);
    if (v->cmaf && !o.properties) {           /* allocation failure on the filler */
        ROW(what, "fixture.filler_properties", false);
        moq_rcbuf_decref(first); moq_rcbuf_decref(second); moq_rcbuf_decref(props);
        moq_media_sender_test_free(s);
        return;
    }
    moq_rcbuf_t *filler_props = o.properties;
    moq_result_t wr1 = moq_media_sender_write(s, t, &o);   /* fills the queue */
    ROW(what, "fixture.first_write_ok", wr1 == MOQ_OK);
    if (wr1 != MOQ_OK) {
        moq_rcbuf_decref(first);
        if (filler_props) moq_rcbuf_decref(filler_props);
        moq_rcbuf_decref(second);
        if (props) moq_rcbuf_decref(props);
        moq_media_sender_test_free(s);
        return;
    }
    moq_media_sender_stats_t before; memset(&before, 0, sizeof(before));
    ROW(what, "stats_before", moq_media_sender_get_stats(s, &before, sizeof(before)) == MOQ_OK);

    memset(&g_script, 0, sizeof(g_script));
    memcpy(g_script.clock, v->clock, sizeof(g_script.clock));
    g_script.clock_n = v->clock_n;
    memcpy(g_script.wait, v->wait, sizeof(g_script.wait));
    g_script.wait_n = v->wait_n;
    g_script.fallback_sec = v->fallback_sec;
    script_install(s);

    /* An OBSERVER reference, so ownership is read without depending on the
     * caller's own reference surviving. */
    moq_rcbuf_t *obs_payload = moq_rcbuf_incref(second);
    moq_rcbuf_t *obs_props = props ? moq_rcbuf_incref(props) : NULL;
    uint32_t pay_before = moq_rcbuf_refcount(second);          /* caller + observer */
    uint32_t props_before = props ? moq_rcbuf_refcount(props) : 0;

    send_obj(&o, second, props, false, false);                 /* delta: blocks */
    moq_result_t wr = moq_media_sender_write(s, t, &o);
    script_remove();

    /* Controlled defect injection (perturbation arms only): the caller's own
     * reference disappears exactly as a consume-on-refusal bug would lose it.
     * The observer reference keeps the buffer alive, so nothing is freed early
     * and the checker below reads a live object. */
    if (v->perturb_consume_on_refusal && wr != MOQ_OK) {
        moq_rcbuf_decref(second);
        if (props) moq_rcbuf_decref(props);
    }
    uint32_t pay_after = moq_rcbuf_refcount(second);
    uint32_t props_after = props ? moq_rcbuf_refcount(props) : 0;

    if (wr != v->expect_rc)
        fprintf(stderr, "  %s: measured result %d, declared %d (clock calls %d, wait calls %d)\n",
                what, (int)wr, (int)v->expect_rc, g_script.clock_calls, g_script.wait_calls);
    ROW(what, "result_matches_declared", wr == v->expect_rc);
    if (g_script.clock_calls != v->expect_clock_calls || g_script.wait_calls != v->expect_wait_calls)
        fprintf(stderr, "  %s: clock calls %d (declared %d), wait calls %d (declared %d)\n",
                what, g_script.clock_calls, v->expect_clock_calls,
                g_script.wait_calls, v->expect_wait_calls);
    ROW(what, "clock_calls_match_declared", g_script.clock_calls == v->expect_clock_calls);
    ROW(what, "wait_calls_match_declared", g_script.wait_calls == v->expect_wait_calls);
    ROW(what, "no_observation_overflow", !g_script.observation_overflow);
    /* Exact absolute-time inventory: every declared wait must exist. */
    for (int i = 0; i < v->expect_abs_n; i++) {
        char nm[64]; snprintf(nm, sizeof(nm), "absolute_wait_%d_matches_declared", i);
        bool ok = i < g_script.observed_n &&
                  (long long)g_script.observed[i].tv_sec == v->expect_abs[i].sec &&
                  g_script.observed[i].tv_nsec == v->expect_abs[i].nsec;
        if (!ok && i < g_script.observed_n)
            fprintf(stderr, "  %s: wait %d observed (%lld, %ld), declared (%lld, %ld)\n",
                    what, i, (long long)g_script.observed[i].tv_sec,
                    g_script.observed[i].tv_nsec, v->expect_abs[i].sec, v->expect_abs[i].nsec);
        ROW(what, nm, ok);
    }

    /* Ownership: exactly one owner besides the observer must remain, whatever
     * the result was. A consume-on-error defect shows up here as a LOST ref. */
    bool consistent = (pay_after == pay_before) &&
                      (!props || props_after == props_before);
    if (!consistent)
        fprintf(stderr, "  %s: payload refs %u -> %u, properties refs %u -> %u\n",
                what, pay_before, pay_after, props_before, props_after);
    ROW(what, "ownership.exactly_one_owner_after_write", consistent);

    moq_media_sender_stats_t after; memset(&after, 0, sizeof(after));
    ROW(what, "stats_after", moq_media_sender_get_stats(s, &after, sizeof(after)) == MOQ_OK);
    if (v->expect_transfer) {
        ROW(what, "transfer.written_advanced", after.objects_written == before.objects_written + 1);
    } else {
        ROW(what, "refusal.written_unchanged", after.objects_written == before.objects_written);
        ROW(what, "refusal.queued_unchanged", after.objects_queued == before.objects_queued);
        ROW(what, "refusal.bytes_queued_unchanged", after.bytes_queued == before.bytes_queued);
        ROW(what, "refusal.dropped_unchanged", after.objects_dropped == before.objects_dropped);
        ROW(what, "refusal.last_error_is_result", after.last_error == wr);
    }
    ROW(what, "one_stall_counted", after.backpressure_stalls == before.backpressure_stalls + 1);

    /* Release by ACTUAL ownership, never by the declared expectation. On an
     * inconsistent observation the caller's reference is NOT released again
     * (the count already says it is gone); the sender is still torn down and
     * the observer references are still released, which is exactly what the
     * observed counts show to be safe. */
    bool release_caller_ref = g_expectation_driven_cleanup
        ? !v->expect_transfer          /* mutant: the removed, unsafe rule */
        : (wr != MOQ_OK);              /* correct: follow the actual result */
    if (consistent && release_caller_ref) {
        moq_rcbuf_decref(second);
        if (props) moq_rcbuf_decref(props);
    }
    moq_media_sender_test_free(s);           /* releases anything the queue took */
    if (consistent) {
        ROW(what, "ownership.observer_sole_owner_after_teardown",
            moq_rcbuf_refcount(obs_payload) == 1 &&
            (!obs_props || moq_rcbuf_refcount(obs_props) == 1));
    }
    moq_rcbuf_decref(obs_payload);
    if (obs_props) moq_rcbuf_decref(obs_props);
}

/* -- vectors -------------------------------------------------------------- */
#define T0 1000LL
#define NS(ms) ((long)((ms) * 1000000L))

static void test_c1_successful_wait(void)
{
    vector_t v = {
        .what = "c1.wait_succeeds_when_space_frees",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {0, T0, 0} }, .clock_n = 2,
        .wait  = { {0, true} },              .wait_n = 1,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_OK, .expect_transfer = true,
        .expect_clock_calls = 2, .expect_wait_calls = 1,
        .expect_abs = { { T0, NS(50) } }, .expect_abs_n = 1,
    };
    run_vector(&v);
}

static void test_c2_normal_timeout(void)
{
    vector_t v = {
        .what = "c2.normal_timeout",
        .timeout_us = 1000000,
        /* expiry is observed by a READ, not by an extra expired wait */
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0, NS(960)}, {0, T0 + 1, 0} }, .clock_n = 4,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false} },   .wait_n = 2,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0, NS(50) }, { T0 + 1, 0 } }, .expect_abs_n = 2,
    };
    run_vector(&v);
}

static void test_c3_nanosecond_carry(void)
{
    vector_t v = {
        .what = "c3.nanosecond_carry_and_sub_ms_remainder",
        .timeout_us = 1500500,
        .clock = { {0, T0, NS(600)}, {0, T0, NS(600)}, {0, T0, NS(980)}, {0, T0 + 2, NS(200)} },
        .clock_n = 4,
        .wait = { {ETIMEDOUT, false}, {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 3,
        .fallback_sec = T0 + 100,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0, NS(650) }, { T0 + 1, NS(30) } },
        .expect_abs_n = 2,
    };
    run_vector(&v);
}

static void test_c4_full_width_duration(void)
{
    /* UINT64_MAX us = 18,446,744,073,709 s + 551,615 us exactly. On a 64-bit
     * time_t this neither truncates nor overflows, so the host cannot exercise
     * the narrowing; the model covers a narrower width. */
    vector_t v = {
        .what = "c4.full_width_duration_no_truncation_on_64bit_time_t",
        .timeout_us = UINT64_MAX,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0, 0},
                   {0, 18446744073709LL + T0 + 1, 0} }, .clock_n = 4,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 3,
        .fallback_sec = 18446744073709LL + T0 + 11,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0, NS(50) }, { T0, NS(50) } }, .expect_abs_n = 2,
    };
    run_vector(&v);
}

static void test_c5_backward_clock_extends_the_budget(void)
{
    vector_t v = {
        .what = "c5.backward_clock_extends_wall_budget",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0 - 100, 0}, {0, T0 + 1, 0} }, .clock_n = 4,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 3,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0, NS(50) }, { T0 - 100, NS(50) } }, .expect_abs_n = 2,
    };
    run_vector(&v);
}

static void test_c6_forward_clock_shortens_the_budget(void)
{
    vector_t v = {
        .what = "c6.forward_clock_shortens_wall_budget",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0 + 4000, 0} }, .clock_n = 3,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 2,
        .fallback_sec = T0 + 10000,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 3, .expect_wait_calls = 1,
        .expect_abs = { { T0, NS(50) } }, .expect_abs_n = 1,
    };
    run_vector(&v);
}

static void test_c7_representable_maximum_second(void)
{
    /* The largest representable second is a LEGAL absolute wait: (MAX, 10 ms)
     * and (MAX, 500 ms) are both representable, and from (MAX, 600 ms) a
     * further 50 ms is still representable. This is a positive boundary
     * control, not a defect: nothing here may be rejected for reaching the
     * maximum second. */
    const long long MAXS = (long long)INT64_MAX;
    vector_t v = {
        .what = "c7.representable_maximum_second_is_legal",
        .timeout_us = 1100000,
        .clock = { {0, MAXS - 1, NS(400)}, {0, MAXS - 1, NS(960)}, {0, MAXS, NS(600)} },
        .clock_n = 3,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 2,
        .fallback_sec = MAXS,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 3, .expect_wait_calls = 1,
        .expect_abs = { { MAXS, NS(10) } }, .expect_abs_n = 1,
    };
    run_vector(&v);
}

static void test_c8_sub_microsecond_remainder(void)
{
    /* A ONE MICROSECOND budget with 100 ns / 200 ns forward observations and
     * spurious wakes. The remaining budget is 1000 ns, then 900 ns, then
     * 800 ns, so every wait must land on the SAME absolute instant
     * (T0, 1000). Rounding the REMAINING budget down to whole microseconds
     * turns 900 ns and 800 ns into zero, so those waits would land on
     * (T0, 100) and (T0, 200) instead -- which is exactly what mutant m18
     * produces. */
    vector_t v = {
        .what = "c8.sub_microsecond_remaining_budget",
        .timeout_us = 1,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0, 100}, {0, T0, 200}, {0, T0, 1000} },
        .clock_n = 5,
        .wait  = { {0, false}, {0, false}, {0, false} }, .wait_n = 3,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 5, .expect_wait_calls = 3,
        .expect_abs = { { T0, 1000 }, { T0, 1000 }, { T0, 1000 } }, .expect_abs_n = 3,
    };
    run_vector(&v);
}

static void test_c10_backward_jump_then_recovery(void)
{
    /* A ONE MILLISECOND budget. After a 100 s backward jump the remaining
     * budget is 100.001 s -- at least a slice -- so the wait must be a full
     * 50 ms slice from the jumped-back clock. Treating a backward clock as
     * zero elapsed would instead wait only the original 1 ms. The clock then
     * recovers and the budget expires exactly. The PRE-JUMP leg uses a
     * spurious wake so the backward observation is reached on any
     * implementation, rather than ending the run at the original deadline for
     * an unrelated reason. */
    vector_t v = {
        .what = "c10.backward_jump_then_partial_recovery",
        .timeout_us = 1000,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0 - 100, 0}, {0, T0, 0}, {0, T0, NS(1)} },
        .clock_n = 5,
        .wait  = { {0, false}, {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 3,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 5, .expect_wait_calls = 3,
        .expect_abs = { { T0, NS(1) }, { T0 - 100, NS(50) }, { T0, NS(1) } },
        .expect_abs_n = 3,
    };
    run_vector(&v);
}

static void test_c9_cmaf_properties_ownership(void)
{
    /* A CMAF track with explicit passthrough carries an OPAQUE, non-NULL
     * property block, so refusal ownership is MEASURED for properties as well
     * as payload. Nothing here claims those bytes are a conforming property
     * encoding. */
    vector_t v = {
        .what = "c9.cmaf_properties_refusal_ownership",
        .timeout_us = 1000000, .cmaf = true,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0, NS(960)}, {0, T0 + 1, 0} }, .clock_n = 4,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 2,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0, NS(50) }, { T0 + 1, 0 } }, .expect_abs_n = 2,
    };
    run_vector(&v);
}

/* -- RED vectors ----------------------------------------------------------- */
static void test_d1_initial_clock_read_failure(void)
{
    /* The FIRST clock read fails. A correct implementation stops there: one
     * clock call, no wait. The extra entries exist only so today's product,
     * which ignores the failure, still terminates. */
    vector_t v = {
        .what = "d1.initial_clock_read_failure",
        .timeout_us = 1000000,
        .clock = { {-1, 0, 0}, {0, T0, 0} }, .clock_n = 2,
        .wait  = { {ETIMEDOUT, false} },     .wait_n = 1,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_INTERNAL, .expect_transfer = false,
        .expect_clock_calls = 1, .expect_wait_calls = 0, .expect_abs_n = 0,
    };
    run_vector(&v);
}

static void test_d2_slice_clock_read_failure(void)
{
    /* A LATER clock read fails: two clock calls, no wait. */
    vector_t v = {
        .what = "d2.slice_clock_read_failure",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {-1, 0, 0}, {0, T0 + 1, 0} }, .clock_n = 3,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 2,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_INTERNAL, .expect_transfer = false,
        .expect_clock_calls = 2, .expect_wait_calls = 0, .expect_abs_n = 0,
    };
    run_vector(&v);
}

static void test_d3_unexpected_wait_error(void)
{
    /* The wait returns EINVAL: a correct implementation returns INTERNAL after
     * exactly one wait. Today it ignores the code and iterates again, which the
     * exact call counts report by name. */
    vector_t v = {
        .what = "d3.unexpected_wait_error",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {0, T0, 0} }, .clock_n = 2,
        .wait  = { {EINVAL, false} }, .wait_n = 1,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_INTERNAL, .expect_transfer = false,
        .expect_clock_calls = 2, .expect_wait_calls = 1,
        .expect_abs = { { T0, NS(50) } }, .expect_abs_n = 1,
    };
    run_vector(&v);
}

static void test_c11_full_width_with_backward_movement(void)
{
    /* The complete uint64 duration AND a backward jump in the same run: the
     * backward leg must still be classified without forming an overflowing
     * sum, and the budget only ends when the clock passes the origin by more
     * than the whole duration. */
    vector_t v = {
        .what = "c11.full_width_duration_with_backward_jump",
        .timeout_us = UINT64_MAX,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0 - 100, 0}, {0, T0, 0},
                   {0, 18446744073709LL + T0 + 1, 0} }, .clock_n = 5,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false}, {ETIMEDOUT, false} }, .wait_n = 3,
        .fallback_sec = 18446744073709LL + T0 + 11,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 5, .expect_wait_calls = 3,
        .expect_abs = { { T0, NS(50) }, { T0 - 100, NS(50) }, { T0, NS(50) } },
        .expect_abs_n = 3,
    };
    run_vector(&v);
}

static void test_c12_sub_second_backward_distance(void)
{
    /* A 30 ms budget from an origin at 990 ms, so the first wait BORROWS
     * across a second: (T0, 990ms) + 30ms = (T0+1, 20ms). The clock then moves
     * back by 10 ms -- less than a second, so the backward branch must add
     * that fractional distance: 30ms + 10ms from (T0, 980ms) is the SAME
     * instant. Dropping the fractional contribution would wait at
     * (T0+1, 10ms). */
    vector_t v = {
        .what = "c12.sub_second_backward_distance",
        .timeout_us = 30000,
        .clock = { {0, T0, NS(990)}, {0, T0, NS(990)}, {0, T0, NS(980)},
                   {0, T0 + 1, NS(20)} }, .clock_n = 4,
        .wait  = { {0, false}, {0, false} }, .wait_n = 2,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0 + 1, NS(20) }, { T0 + 1, NS(20) } }, .expect_abs_n = 2,
    };
    run_vector(&v);
}

static void test_c13_seconds_sign_crossing(void)
{
    /* The ordered seconds difference is taken in unsigned arithmetic. These
     * two vectors cross zero and then span the entire representable range;
     * the fixture performs no arithmetic of its own, only declared literals. */
    vector_t a = {
        .what = "c13a.origin_negative_now_positive",
        .timeout_us = 20000000,                      /* 20 s */
        .clock = { {0, -5, 0}, {0, 5, 0}, {0, 25, 0} }, .clock_n = 3,
        .wait  = { {ETIMEDOUT, false} }, .wait_n = 1,
        .fallback_sec = 100,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 3, .expect_wait_calls = 1,
        .expect_abs = { { 5, NS(50) } }, .expect_abs_n = 1,
    };
    run_vector(&a);
    vector_t b = {
        .what = "c13b.extreme_ordered_pair",
        .timeout_us = 1000000,
        .clock = { {0, (long long)INT64_MIN, 0}, {0, (long long)INT64_MAX, 0} }, .clock_n = 2,
        .wait  = { {ETIMEDOUT, false} }, .wait_n = 1,
        .fallback_sec = (long long)INT64_MAX,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 2, .expect_wait_calls = 0, .expect_abs_n = 0,
    };
    run_vector(&b);
}

/* An absolute wait the implementation MUST refuse: from a clock already at the
 * maximum representable second, the required 40 ms remainder carries past it.
 * Run only under --ceiling: on an implementation without the check this input
 * is a signed overflow (undefined behaviour), so it is never part of the
 * ordinary row. */
static void test_e1_unrepresentable_future_wait(void)
{
    const long long MAXS = (long long)INT64_MAX;
    vector_t v = {
        .what = "e1.unrepresentable_future_wait",
        .timeout_us = 1000000,
        .clock = { {0, MAXS, 0}, {0, MAXS, NS(960)} }, .clock_n = 2,
        .wait  = { {ETIMEDOUT, false} }, .wait_n = 1,
        .fallback_sec = MAXS,
        .expect_rc = MOQ_ERR_INTERNAL, .expect_transfer = false,
        .expect_clock_calls = 2, .expect_wait_calls = 0, .expect_abs_n = 0,
    };
    run_vector(&v);
}

/* -- pure arithmetic MODEL (not a runtime claim) --------------------------- */
typedef struct { long long sec; long nsec; bool truncated, overflowed; } model_t;

static model_t model_deadline(long long now_sec, long now_nsec, uint64_t add_us,
                              long long time_t_max)
{
    model_t m = { now_sec, now_nsec, false, false };
    uint64_t whole = add_us / 1000000ull;
    long long narrowed = (time_t_max == (long long)INT32_MAX)
        ? (long long)(int32_t)(uint32_t)whole
        : (long long)(int64_t)whole;
    m.truncated = (narrowed < 0) || ((uint64_t)narrowed != whole);
    if (!m.truncated && narrowed > time_t_max - m.sec) m.overflowed = true;
    else if (!m.truncated) m.sec += narrowed;
    m.nsec += (long)((add_us % 1000000ull) * 1000ull);
    if (m.nsec >= 1000000000L) {
        if (m.sec >= time_t_max) m.overflowed = true; else m.sec++;
        m.nsec -= 1000000000L;
    }
    return m;
}

static void test_m1_arithmetic_model(void)
{
    const char *what = "m1.model";
    const long long I32 = (long long)INT32_MAX, I64 = (long long)INT64_MAX;
    /* 64-bit: the full domain neither truncates nor overflows. */
    model_t a = model_deadline(1000, 0, UINT64_MAX, I64);
    ROW(what, "int64.full_width_no_truncation", !a.truncated);
    ROW(what, "int64.full_width_no_overflow", !a.overflowed);
    ROW(what, "int64.full_width_seconds", a.sec == 1000LL + 18446744073709LL);
    /* 32-bit: the same duration TRUNCATES. */
    model_t b = model_deadline(1000, 0, UINT64_MAX, I32);
    ROW(what, "int32.full_width_truncates", b.truncated);
    /* 32-bit: 4,294,967,296 s is 2^32 -- it truncates (to zero), it does not
     * overflow an addition. Declared explicitly, since both words apply to
     * different inputs. */
    model_t c = model_deadline(1000, 0, 4294967296000000ull, I32);
    ROW(what, "int32.4294967296s_truncates_not_overflows", c.truncated && !c.overflowed);
    /* 32-bit: a seconds value that CONVERTS exactly and still overflows the
     * addition -- INT32_MAX seconds added to a nonzero origin. */
    model_t d = model_deadline(1000, 0, (uint64_t)INT32_MAX * 1000000ull, I32);
    ROW(what, "int32.fitting_conversion_overflows_addition", !d.truncated && d.overflowed);
    /* Both widths at their own ceiling: a 50 ms carry does not fit. */
    model_t e = model_deadline(I32, 960000000L, 50000ull, I32);
    ROW(what, "int32.ceiling_carry_does_not_fit", e.overflowed);
    model_t f = model_deadline(I64, 960000000L, 50000ull, I64);
    ROW(what, "int64.ceiling_carry_does_not_fit", f.overflowed);
    /* And the representable maximum second itself is NOT an overflow. */
    model_t g = model_deadline(I64 - 1, 0, 1000000ull, I64);
    ROW(what, "int64.maximum_second_is_representable", !g.overflowed && g.sec == I64);
}

/* -- child spawn setup ------------------------------------------------------
 * The file-actions object is destroyed ONLY after a successful init; an init
 * failure leaves nothing to destroy. The operations are indirected so a
 * test-local control can drive both failure paths and count what happened. */
typedef struct {
    int (*fa_init)(posix_spawn_file_actions_t *);
    int (*fa_adddup2)(posix_spawn_file_actions_t *, int, int);
    int (*fa_addclose)(posix_spawn_file_actions_t *, int);
    int (*fa_destroy)(posix_spawn_file_actions_t *);
    int (*spawn)(pid_t *, const char *, const posix_spawn_file_actions_t *,
                 const posix_spawnattr_t *, char *const[], char *const[]);
    /* Perturbation only: skip the destroy the action-failure path owes. */
    bool omit_destroy_on_action_failure;
} spawn_ops_t;

static const spawn_ops_t REAL_SPAWN_OPS = {
    posix_spawn_file_actions_init, posix_spawn_file_actions_adddup2,
    posix_spawn_file_actions_addclose, posix_spawn_file_actions_destroy,
    posix_spawn, false
};

/* The file-actions object is owned by the CALLER, so a perturbation that
 * skips the destroy can still be cleaned up. `*initialized` says whether the
 * object is live on return. Nothing here counts anything: the controls count
 * inside their own stubs, so deleting a call cannot be masked by a counter
 * that sits beside it. */
static int spawn_capturing_child(const spawn_ops_t *ops,
                                 posix_spawn_file_actions_t *fa,
                                 bool *initialized, const char *self,
                                 const int fds[2], pid_t *out_pid)
{
    *initialized = false;
    int se = ops->fa_init(fa);
    if (se != 0) return se;                  /* nothing was initialized */
    *initialized = true;
    se = ops->fa_adddup2(fa, fds[1], 2);
    if (!se) se = ops->fa_addclose(fa, fds[0]);
    if (!se) se = ops->fa_addclose(fa, fds[1]);
    if (se != 0) {
        if (!ops->omit_destroy_on_action_failure) {
            ops->fa_destroy(fa); *initialized = false;
        }
        return se;
    }
    char *argv[] = { (char *)self, (char *)"--control", (char *)"guard", NULL };
    int rc = ops->spawn(out_pid, self, fa, NULL, argv, environ);
    ops->fa_destroy(fa); *initialized = false;
    return rc;
}

/* -- controls: the hard guard, proven in a child process ------------------- */
static int run_guard_child(void)
{
    /* A script a CORRECT product may legitimately keep working on: the clock
     * never advances and every wait is a valid spurious wake (return 0, no
     * capacity freed). Nothing here is an error the product should report, so
     * only the independent hard call guard can end it. */
    moq_media_track_t *t = NULL;
    moq_media_sender_t *s = lossless_sender(1000000, false, &t);
    if (!s) { fprintf(stderr, "guard child: fixture failed\n"); return 3; }
    moq_rcbuf_t *first = payload(), *second = payload();
    if (!first || !second) { fprintf(stderr, "guard child: payload failed\n"); return 3; }
    moq_media_send_object_t o; send_obj(&o, first, NULL, true, true);
    if (moq_media_sender_write(s, t, &o) != MOQ_OK) { fprintf(stderr, "guard child: fill failed\n"); return 3; }
    memset(&g_script, 0, sizeof(g_script));
    g_script.clock[0] = (clock_step_t){ 0, T0, 0 };
    g_script.clock_n = 1;
    g_script.no_fallback = true;
    script_install(s);
    send_obj(&o, second, NULL, false, false);
    (void)moq_media_sender_write(s, t, &o);      /* must not return */
    script_remove();
    fprintf(stderr, "guard child: the write RETURNED; the guard never fired\n");
    return 4;
}

static int run_controls(const char *self)
{
    const char *what = "control.hard_guard";
    int fds[2];
    if (pipe(fds) != 0) { fprintf(stderr, "FAIL[%s.pipe]\n", what); return 1; }
    pid_t pid = 0;
    posix_spawn_file_actions_t fa;
    bool fa_live = false;
    int rc = spawn_capturing_child(&REAL_SPAWN_OPS, &fa, &fa_live, self, fds, &pid);
    if (fa_live) posix_spawn_file_actions_destroy(&fa);   /* never on this path */
    close(fds[1]);
    if (rc != 0) {
        fprintf(stderr, "FAIL[%s.spawn]: %d\n", what, rc);
        close(fds[0]);
        return 1;
    }
    char buf[4096]; size_t n = 0;
    int read_errno = 0;
    bool read_error = false, capture_overflow = false;
    for (;;) {
        ssize_t got = read(fds[0], buf + n, sizeof(buf) - 1 - n);
        if (got < 0) {
            if (errno == EINTR) continue;
            read_errno = errno;             /* saved before close()/waitpid() */
            read_error = true; break;
        }
        if (got == 0) break;
        n += (size_t)got;
        if (n >= sizeof(buf) - 1) { capture_overflow = true; break; }
    }
    close(fds[0]);
    buf[n] = '\0';
    int status = 0;
    for (;;) {
        pid_t w = waitpid(pid, &status, 0);
        if (w == pid) break;
        if (w < 0 && errno == EINTR) continue;
        fprintf(stderr, "FAIL[%s.waitpid]: errno %d\n", what, errno);
        return 1;
    }
    if (read_error) { fprintf(stderr, "FAIL[%s.pipe_read]: errno %d\n", what, read_errno); return 1; }
    if (capture_overflow) { fprintf(stderr, "FAIL[%s.capture_overflow]\n", what); return 1; }
    int f = 0;
#define CROW(name, cond) do { if (!(cond)) { fprintf(stderr, "FAIL[%s.%s]\n", what, name); f++; } } while (0)
    CROW("exited_not_signalled", WIFEXITED(status));
    CROW("exit_status_is_failure", WIFEXITED(status) && WEXITSTATUS(status) == EXIT_FAILURE);
    CROW("named_guard_diagnostic", strstr(buf, "FAIL[guard.hard_call_limit]") != NULL);
    CROW("write_did_not_return", strstr(buf, "the write RETURNED") == NULL);
#undef CROW
    if (f) { fprintf(stderr, "child stderr:\n%s", buf); return 1; }
    fprintf(stderr, "control.hard_guard: child exit %d, guard fired by name, write never returned\n",
            WEXITSTATUS(status));
    return 0;
}

/* Stubs for the spawn-setup control. Every observation counter lives HERE, in
 * the callback that is or is not invoked, so removing a call in the helper
 * cannot be hidden by a counter next to it. */
static int g_destroys, g_spawns, g_forbidden_destroys;
static int stub_init_fails(posix_spawn_file_actions_t *fa) { (void)fa; return ENOMEM; }
static int stub_init_ok(posix_spawn_file_actions_t *fa) { return posix_spawn_file_actions_init(fa); }
static int stub_adddup2_fails(posix_spawn_file_actions_t *fa, int a, int b)
{ (void)fa; (void)a; (void)b; return EBADF; }
static int stub_destroy_counting(posix_spawn_file_actions_t *fa)
{ g_destroys++; return posix_spawn_file_actions_destroy(fa); }
/* Records a destroy that must never happen WITHOUT touching the object, so an
 * erroneous call is reported instead of running on uninitialized storage. */
static int stub_destroy_forbidden(posix_spawn_file_actions_t *fa)
{ (void)fa; g_forbidden_destroys++; return 0; }
static int stub_spawn_counting(pid_t *p, const char *f, const posix_spawn_file_actions_t *fa,
                               const posix_spawnattr_t *at, char *const av[], char *const ev[])
{ (void)p; (void)f; (void)fa; (void)at; (void)av; (void)ev; g_spawns++; return ENOSYS; }

static int spawn_setup_controls(void)
{
    const char *what = "control.spawn_setup";
    int fds[2] = { -1, -1 };
    if (pipe(fds) != 0) { fprintf(stderr, "FAIL[%s.pipe]\n", what); return 1; }
    int bad = 0;
    pid_t pid = 0;
    posix_spawn_file_actions_t fa;
    bool live = false;
#define SROW(name, cond) do { if (!(cond)) { fprintf(stderr, "FAIL[%s.%s]\n", what, name); bad++; } } while (0)
    /* init fails: nothing was initialized, so destroy must never be called. */
    spawn_ops_t a = REAL_SPAWN_OPS;
    a.fa_init = stub_init_fails; a.fa_destroy = stub_destroy_forbidden;
    a.spawn = stub_spawn_counting;
    g_destroys = g_spawns = g_forbidden_destroys = 0;
    int rc = spawn_capturing_child(&a, &fa, &live, "unused", fds, &pid);
    SROW("init_failure_reported", rc == ENOMEM);
    SROW("init_failure_not_initialized", !live);
    SROW("init_failure_no_destroy_call", g_forbidden_destroys == 0);
    SROW("init_failure_no_spawn_call", g_spawns == 0);
    if (live) { posix_spawn_file_actions_destroy(&fa); live = false; }

    /* a later action fails on an INITIALIZED object: exactly one destroy call */
    spawn_ops_t b = REAL_SPAWN_OPS;
    b.fa_init = stub_init_ok; b.fa_adddup2 = stub_adddup2_fails;
    b.fa_destroy = stub_destroy_counting; b.spawn = stub_spawn_counting;
    g_destroys = g_spawns = g_forbidden_destroys = 0;
    rc = spawn_capturing_child(&b, &fa, &live, "unused", fds, &pid);
    SROW("action_failure_reported", rc == EBADF);
    SROW("action_failure_one_destroy_call", g_destroys == 1);
    SROW("action_failure_no_spawn_call", g_spawns == 0);
    SROW("action_failure_object_released", !live);
    if (live) { posix_spawn_file_actions_destroy(&fa); live = false; }

    /* Perturbation: omitting that destroy must FAIL the initialized case by
     * name. The caller still releases the object, so nothing leaks. */
    spawn_ops_t c = b;
    c.omit_destroy_on_action_failure = true;
    g_destroys = g_spawns = g_forbidden_destroys = 0;
    rc = spawn_capturing_child(&c, &fa, &live, "unused", fds, &pid);
    bool perturbation_detected = (rc == EBADF) && (g_destroys == 0) && live;
    SROW("omitted_destroy_is_detected", perturbation_detected);
    if (live) { posix_spawn_file_actions_destroy(&fa); live = false; }
#undef SROW
    close(fds[0]); close(fds[1]);
    if (bad) return 1;
    fprintf(stderr, "control.spawn_setup: init failure -> 0 destroy calls, 0 spawns;"
            " action failure -> 1 destroy call, 0 spawns; omitted destroy detected\n");
    return 0;
}

/* -- perturbations: the oracle must detect a wrong declaration -------------- */
static vector_t c2_vector(void)
{
    vector_t v = {
        .what = "p.base",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {0, T0, 0}, {0, T0, NS(960)}, {0, T0 + 1, 0} }, .clock_n = 4,
        .wait  = { {ETIMEDOUT, false}, {ETIMEDOUT, false} },   .wait_n = 2,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
        .expect_clock_calls = 4, .expect_wait_calls = 2,
        .expect_abs = { { T0, NS(50) }, { T0 + 1, 0 } }, .expect_abs_n = 2,
    };
    return v;
}

static int perturbations(void)
{
    const char *what = "p.oracle_perturbations";
    int verdict = 0;
    struct { const char *name; const char *expect_rows[6]; int expect_n; vector_t v; } arms[6];
    memset(arms, 0, sizeof(arms));
    arms[0].name = "detects_wrong_absolute_time";
    arms[0].expect_rows[0] = "p1.wrong_absolute.absolute_wait_1_matches_declared"; arms[0].expect_n = 1;
    arms[0].v = c2_vector(); arms[0].v.what = "p1.wrong_absolute";
    arms[0].v.expect_abs[1].nsec += 1;
    arms[1].name = "detects_extra_wait_call";
    arms[1].expect_rows[0] = "p2.wait_count.wait_calls_match_declared"; arms[1].expect_n = 1;
    arms[1].v = c2_vector(); arms[1].v.what = "p2.wait_count";
    arms[1].v.expect_wait_calls = 1; arms[1].v.expect_abs_n = 1;
    arms[2].name = "detects_wrong_result_classification";
    arms[2].expect_rows[0] = "p3.wrong_result.result_matches_declared"; arms[2].expect_n = 1;
    arms[2].v = c2_vector(); arms[2].v.what = "p3.wrong_result";
    arms[2].v.expect_rc = MOQ_OK;
    arms[3].name = "detects_wrong_ownership_expectation";
    arms[3].expect_rows[0] = "p4.wrong_transfer.transfer.written_advanced"; arms[3].expect_n = 1;
    arms[3].v = c2_vector(); arms[3].v.what = "p4.wrong_transfer";
    arms[3].v.expect_transfer = true;
    /* Cleanup must follow the ACTUAL result in the success direction too: the
     * declared result is wrong, the write really succeeds, and the test must
     * not release a buffer the queue now owns. */
    arms[4].name = "detects_unexpected_success_without_double_release";
    /* Declares a REFUSAL in every respect while the wait really frees capacity
     * and the write succeeds, so the refusal inventory fires too. The old
     * expectation-driven cleanup would release a buffer the queue now owns. */
    /* The wait frees one entry and the write enqueues one object of the same
     * size, so the queue depth and byte rows correctly do NOT fire; only the
     * result and the written counter contradict the declaration. */
    arms[4].expect_rows[0] = "p5.unexpected_success.result_matches_declared";
    arms[4].expect_rows[1] = "p5.unexpected_success.refusal.written_unchanged";
    arms[4].expect_n = 2;
    arms[4].v = (vector_t){
        .what = "p5.unexpected_success",
        .timeout_us = 1000000,
        .clock = { {0, T0, 0}, {0, T0, 0} }, .clock_n = 2,
        .wait  = { {0, true} },              .wait_n = 1,
        .fallback_sec = T0 + 10,
        .expect_rc = MOQ_ERR_WOULD_BLOCK,    /* deliberately wrong: it succeeds */
        .expect_transfer = false,            /* and contradicts the transfer too */
        .expect_clock_calls = 2, .expect_wait_calls = 1,
        .expect_abs = { { T0, NS(50) } }, .expect_abs_n = 1,
    };
    /* A controlled consume-on-refusal: the caller's reference disappears, which
     * only the ownership checker can see. The observer reference keeps the
     * buffer alive, so the arm is bounded and non-crashing. */
    arms[5].name = "detects_consume_on_refusal";
    arms[5].expect_rows[0] = "p6.consume_on_refusal.ownership.exactly_one_owner_after_write"; arms[5].expect_n = 1;
    arms[5].v = c2_vector(); arms[5].v.what = "p6.consume_on_refusal";
    arms[5].v.perturb_consume_on_refusal = true;
    for (int i = 0; i < 6; i++) {
        int before = failures;
        g_capture = true; g_captured_n = 0;
        run_vector(&arms[i].v);
        g_capture = false;
        int induced = failures - before;
        failures = before;                       /* induced, not a product failure */
        bool exact = (induced == arms[i].expect_n) && (g_captured_n == arms[i].expect_n);
        for (int j = 0; exact && j < arms[i].expect_n; j++) {
            bool found = false;
            for (int k = 0; k < g_captured_n; k++)
                if (strcmp(g_captured[k], arms[i].expect_rows[j]) == 0) { found = true; break; }
            exact = found;
        }
        if (!exact) {
            fprintf(stderr, "FAIL[%s.%s]: induced %d row(s)", what, arms[i].name, induced);
            for (int j = 0; j < g_captured_n; j++) fprintf(stderr, " [%s]", g_captured[j]);
            fprintf(stderr, ", declared exactly %d:", arms[i].expect_n);
            for (int j = 0; j < arms[i].expect_n; j++) fprintf(stderr, " [%s]", arms[i].expect_rows[j]);
            fprintf(stderr, "\n");
            verdict = 1;
        } else {
            fprintf(stderr, "  %s: %s -> exactly %d declared row(s)\n",
                    what, arms[i].name, arms[i].expect_n);
        }
    }
    return verdict;
}

int main(int argc, char **argv)
{
    if (argc == 3 && strcmp(argv[1], "--control") == 0 && strcmp(argv[2], "guard") == 0)
        return run_guard_child();
    if (argc == 3 && strcmp(argv[1], "--control") == 0 &&
        strcmp(argv[2], "old-cleanup") == 0) {
        /* Restores the cleanup rule 1904 removed (release by the DECLARED
         * expectation) and runs the p5 vector, whose declaration says refusal
         * while the write really succeeds. The caller's reference is then
         * released although the queue owns it, so the sender's teardown and
         * the observer release operate on a freed buffer. Under ASan this
         * child is expected to abort with a use-after-free; it is run only
         * from the sanitizer lane and is classified as sanitizer evidence,
         * not as a test assertion. */
        g_expectation_driven_cleanup = true;
        vector_t v = {
            .what = "old_cleanup.p5",
            .timeout_us = 1000000,
            .clock = { {0, T0, 0}, {0, T0, 0} }, .clock_n = 2,
            .wait  = { {0, true} },              .wait_n = 1,
            .fallback_sec = T0 + 10,
            .expect_rc = MOQ_ERR_WOULD_BLOCK, .expect_transfer = false,
            .expect_clock_calls = 2, .expect_wait_calls = 1,
            .expect_abs = { { T0, NS(50) } }, .expect_abs_n = 1,
        };
        run_vector(&v);
        fprintf(stderr, "old_cleanup child: completed without a sanitizer abort\n");
        return 0;
    }
    if (argc == 2 && strcmp(argv[1], "--ceiling") == 0) {
        test_e1_unrepresentable_future_wait();
        if (failures) {
            fprintf(stderr, "media_sender_deadline_ceiling: %d failure(s)\n", failures);
            return 1;
        }
        MOQ_TEST_PASS("media_sender_deadline_ceiling");
        return 0;
    }
    if (argc == 2 && strcmp(argv[1], "--controls") == 0) {
        int bad = spawn_setup_controls() | run_controls(argv[0]) | perturbations();
        if (bad) { fprintf(stderr, "media_sender_deadline_controls: failed\n"); return 1; }
        MOQ_TEST_PASS("media_sender_deadline_controls");
        return 0;
    }
    test_c1_successful_wait();
    test_c2_normal_timeout();
    test_c3_nanosecond_carry();
    test_c4_full_width_duration();
    test_c5_backward_clock_extends_the_budget();
    test_c6_forward_clock_shortens_the_budget();
    test_c7_representable_maximum_second();
    test_c8_sub_microsecond_remainder();
    test_c10_backward_jump_then_recovery();
    test_c11_full_width_with_backward_movement();
    test_c12_sub_second_backward_distance();
    test_c13_seconds_sign_crossing();
    test_c9_cmaf_properties_ownership();
    test_d1_initial_clock_read_failure();
    test_d2_slice_clock_read_failure();
    test_d3_unexpected_wait_error();
    test_m1_arithmetic_model();
    fprintf(stderr, "deadline: host sizeof(time_t)=%zu (pinned); scripted waits are a mechanism"
            " test, not a kernel-wait or target-runtime claim\n", sizeof(time_t));
    if (failures) {
        fprintf(stderr, "media_sender_deadline: %d failure(s)\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_sender_deadline");
    return 0;
}
