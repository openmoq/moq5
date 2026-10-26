/* The permanent contract for moq_endpoint_get_snapshot() -- ONE coherent
 * observation of (state, negotiated version, terminal reason + detail).
 *
 * It exists because the three legacy accessors are read under DIFFERENT locks:
 * moq_endpoint_state() releases ep->mu before consulting the facade,
 * moq_endpoint_get_terminal() takes no ep->mu at all, and
 * moq_endpoint_negotiated_version() takes its own. A caller composing them
 * samples several instants; the composite accessor exists so no caller has to.
 *
 * Each test drives a REAL transition, interposed at an exact point inside the
 * call, and requires the record to describe one instant:
 *
 *   - a first establishment interposed mid-call, in both accessor orders;
 *   - a terminal that lands while a classification is in flight;
 *   - a change that CROSSES the terminal sample -- a pump state change or a
 *     stop -- which must force the local tuple to be read again;
 *   - a terminal latched BETWEEN the composed reads of one classification
 *     sample, which the repeated sampling must settle;
 *   - the documented generic classification for a NULL terminal slot;
 *   - the sized-output ABI, and CLOSED + NONE;
 *   - every partial-setup cleanup arm and the allocator balance.
 *
 * Nothing here forges product state. The negotiated version comes from the
 * established SimPair session the pump is handed, exactly as a live backend's
 * session supplies it; the scripted facade supplies only the fatal/clean
 * inputs a real backend reports. The endpoint's own commit path and classifier
 * decide what is latched and reported; the interposition seams choose WHEN the
 * world moves, never WHAT the getter answers. There is no socket, sleep or
 * scheduler race.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "moq/endpoint.h"
#include "moq/session.h"
#include "moq/sim.h"
#include "test_support.h"

/* The service testing seam (declared here, as the sibling service tests do;
 * it is compiled only under the service testing gate and is never part of a
 * shipped library). */
moq_endpoint_t *moq_endpoint_test_make_bare_alloc(const moq_alloc_t *alloc);
void         moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void         moq_endpoint_test_pump_cycle(moq_endpoint_t *ep,
                                          moq_session_t *session,
                                          uint64_t now_us);
size_t       moq_endpoint_test_probe_size(void);
moq_result_t moq_endpoint_test_install_probe(moq_endpoint_t *ep, void *storage,
                                             size_t size, int report);
void         moq_endpoint_test_remove_probe(moq_endpoint_t *ep);
void         moq_endpoint_test_probe_set_terminal(void *probe, bool fatal,
                                                  uint64_t fatal_code,
                                                  bool clean);
moq_result_t moq_endpoint_test_install_probe_null_terminal(moq_endpoint_t *ep,
                                                           void *storage,
                                                           size_t size);
void         moq_endpoint_test_probe_arm_hook(void *probe, int at,
                                              void (*fn)(void *), void *ctx);
void         moq_endpoint_test_probe_get_terminal(const void *probe, bool *fatal,
                                                  uint64_t *fatal_code,
                                                  bool *clean);
unsigned     moq_endpoint_test_probe_hook_fires(const void *probe);
unsigned     moq_endpoint_test_interpose_fires(const moq_endpoint_t *ep);
unsigned     moq_endpoint_test_probe_terminal_samples(const void *probe);
void         moq_endpoint_test_arm_interpose(moq_endpoint_t *ep, int at,
                                             void (*fn)(void *), void *ctx);

/* The version the established SimPair session speaks, which is the endpoint's
 * version authority. fix_init_mode() checks the session against it before any
 * case runs, so every expectation below is anchored to the session. */
#define SCRIPTED_VERSION MOQ_VERSION_DRAFT_16
#define SCRIPTED_FATAL_CODE 0x5ull

/* Where a probe one-shot fires inside one terminal classification sample. */
#define HOOK_BEFORE 0
#define HOOK_MID    1
#define HOOK_AFTER  2

typedef union { unsigned long long a; void *p; unsigned char b[256]; } probe_mem_t;

/* Counting allocator: every allocation this fixture makes is visible in the
 * balance, so a partial-setup mutant that leaks cannot pass silently. */
static long g_balance;
static void *cnt_alloc(size_t n, void *ctx)
{
    (void)ctx; void *p = malloc(n); if (p) g_balance++; return p;
}
static void *cnt_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)ctx; (void)o;
    void *q = realloc(p, n);
    if (!p && q) g_balance++;
    return q;
}
static void cnt_free(void *p, size_t n, void *ctx)
{
    (void)ctx; (void)n; if (p) { g_balance--; free(p); }
}
static moq_alloc_t counting_alloc(void)
{
    moq_alloc_t a; memset(&a, 0, sizeof(a));
    a.alloc = cnt_alloc; a.realloc = cnt_realloc; a.free = cnt_free; a.ctx = NULL;
    return a;
}

/* Test-local injection so each partial-setup failure path is EXERCISED, not
 * assumed. It touches no product code. */
typedef enum {
    SETUP_OK = 0,
    SETUP_FAIL_NOT_ESTABLISHED,
    SETUP_FAIL_SESSION_VERSION,
    SETUP_FAIL_ENDPOINT_ALLOC,
    SETUP_FAIL_PROBE_STORAGE,
    SETUP_FAIL_PROBE_INSTALL,
} setup_fault_t;
static setup_fault_t g_setup_fault = SETUP_OK;

typedef struct {
    moq_simpair_t *sp;
    moq_endpoint_t *ep;
    probe_mem_t probe_mem;
    void *probe;
} fix_t;

static const char *state_name(moq_endpoint_state_t s)
{
    switch (s) {
    case MOQ_ENDPOINT_CONNECTING:   return "CONNECTING";
    case MOQ_ENDPOINT_ESTABLISHED:  return "ESTABLISHED";
    case MOQ_ENDPOINT_RECONNECTING: return "RECONNECTING";
    case MOQ_ENDPOINT_DRAINING:     return "DRAINING";
    case MOQ_ENDPOINT_CLOSED:       return "CLOSED";
    }
    return "?";
}

static const char *reason_name(moq_endpoint_terminal_reason_t r)
{
    switch (r) {
    case MOQ_ENDPOINT_TERMINAL_NONE:            return "NONE";
    case MOQ_ENDPOINT_TERMINAL_CLEAN:           return "CLEAN";
    case MOQ_ENDPOINT_TERMINAL_PROTOCOL:        return "PROTOCOL";
    case MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE: return "TLS_CERTIFICATE";
    case MOQ_ENDPOINT_TERMINAL_TLS:             return "TLS";
    case MOQ_ENDPOINT_TERMINAL_TRANSPORT:       return "TRANSPORT";
    }
    return "?";
}

/* The accessor's own result is part of every terminal oracle: a failed
 * getter must never be reported as a valid NONE observation, or a row whose
 * expectation IS NONE would pass on a broken accessor. */
static moq_result_t read_terminal(const moq_endpoint_t *ep,
                                  moq_endpoint_terminal_reason_t *reason,
                                  uint64_t *detail)
{
    moq_endpoint_terminal_t t;
    memset(&t, 0, sizeof(t));
    moq_result_t rc = moq_endpoint_get_terminal(ep, &t, sizeof(t));
    *reason = (rc == MOQ_OK) ? t.reason : MOQ_ENDPOINT_TERMINAL_NONE;
    *detail = (rc == MOQ_OK) ? t.detail_code : 0;
    return rc;
}

static moq_simpair_t *make_established_pair(void)
{
    static moq_alloc_t alloc;
    alloc = counting_alloc();
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = 7;
    cfg.initial_now_us = 1000;
    cfg.version = MOQ_VERSION_DRAFT_16;
    moq_simpair_t *sp = NULL;
    moq_result_t rc = moq_simpair_create(&cfg, &sp);
    if (rc != MOQ_OK) { printf("  simpair_create -> %d\n", (int)rc); return NULL; }
    moq_result_t src = moq_simpair_start(sp);
    if (src != MOQ_OK) {
        printf("  simpair_start -> %d\n", (int)src);
        moq_simpair_destroy(sp);
        return NULL;
    }
    moq_result_t qrc = moq_simpair_run_until_quiescent(sp, 64, NULL);
    if (qrc != MOQ_OK) {
        printf("  simpair_run_until_quiescent -> %d\n", (int)qrc);
        moq_simpair_destroy(sp);
        return NULL;
    }
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

/* A fresh endpoint that has NEVER established: committed pair CONNECTING/0. */
static void fix_free(fix_t *fix);

static bool fix_init_mode(fix_t *fix, bool null_terminal)
{
    memset(fix, 0, sizeof(*fix));
    fix->sp = make_established_pair();
    if (!fix->sp) { printf("  setup: simpair create/establish failed\n"); return false; }
    if (g_setup_fault == SETUP_FAIL_NOT_ESTABLISHED) {
        printf("  setup: INJECTED not-established fault (the session is in fact "
               "state %d)\n", (int)moq_session_state(moq_simpair_client(fix->sp)));
        fix_free(fix);
        return false;
    }
    if (moq_session_state(moq_simpair_client(fix->sp)) != MOQ_SESS_ESTABLISHED) {
        printf("  setup: client session is not ESTABLISHED (state %d)\n",
               (int)moq_session_state(moq_simpair_client(fix->sp)));
        fix_free(fix);
        return false;
    }
    if (g_setup_fault == SETUP_FAIL_SESSION_VERSION ||
        moq_session_version(moq_simpair_client(fix->sp)) != SCRIPTED_VERSION) {
        printf("  setup: %sthe established session speaks 0x%x, not the expected "
               "0x%x\n", g_setup_fault == SETUP_FAIL_SESSION_VERSION ? "INJECTED " : "",
               (unsigned)moq_session_version(moq_simpair_client(fix->sp)),
               (unsigned)SCRIPTED_VERSION);
        fix_free(fix);
        return false;
    }
    static moq_alloc_t ep_alloc;
    ep_alloc = counting_alloc();
    fix->ep = (g_setup_fault == SETUP_FAIL_ENDPOINT_ALLOC)
        ? NULL
        : moq_endpoint_test_make_bare_alloc(&ep_alloc);
    if (!fix->ep) {
        printf("  setup: %sbare endpoint allocation failed\n",
               g_setup_fault == SETUP_FAIL_ENDPOINT_ALLOC ? "INJECTED " : "");
        fix_free(fix);
        return false;
    }
    if (g_setup_fault == SETUP_FAIL_PROBE_STORAGE ||
        moq_endpoint_test_probe_size() > sizeof(fix->probe_mem)) {
        printf("  setup: %sprobe storage rejected (%zu vs %zu)\n",
               g_setup_fault == SETUP_FAIL_PROBE_STORAGE ? "INJECTED " : "",
               moq_endpoint_test_probe_size(), sizeof(fix->probe_mem));
        fix_free(fix);
        return false;
    }
    moq_result_t irc = (g_setup_fault == SETUP_FAIL_PROBE_INSTALL)
        ? MOQ_ERR_INVAL
        : (null_terminal
           ? moq_endpoint_test_install_probe_null_terminal(fix->ep, &fix->probe_mem,
                                                           sizeof(fix->probe_mem))
           : moq_endpoint_test_install_probe(fix->ep, &fix->probe_mem,
                                             sizeof(fix->probe_mem), 0));
    if (irc != MOQ_OK) {
        printf("  setup: %sinstall_probe -> %d\n",
               g_setup_fault == SETUP_FAIL_PROBE_INSTALL ? "INJECTED " : "", (int)irc);
        fix_free(fix);
        return false;
    }
    fix->probe = &fix->probe_mem;
    return true;
}

static bool fix_init(fix_t *fix) { return fix_init_mode(fix, false); }

static void fix_free(fix_t *fix)
{
    if (fix->ep) {
        moq_endpoint_test_remove_probe(fix->ep);
        moq_endpoint_test_free_bare(fix->ep);
    }
    if (fix->sp) moq_simpair_destroy(fix->sp);
    memset(fix, 0, sizeof(*fix));
}

/* Drive the endpoint's own first-establishment commit. */
static void pump_established(fix_t *fix)
{
    moq_endpoint_test_pump_cycle(fix->ep, moq_simpair_client(fix->sp), 2000);
}

/* The composed pair must be one the endpoint actually committed. */
static int check_pair_committed(const char *label,
                                moq_endpoint_state_t s, moq_version_t v,
                                moq_endpoint_state_t before_s,
                                moq_version_t before_v,
                                moq_endpoint_state_t after_s,
                                moq_version_t after_v)
{
    bool ok = (s == before_s && v == before_v) ||
              (s == after_s && v == after_v);
    if (!ok) {
        printf("FAIL[%s]: composed (%s, version %u) is a TORN pair: the "
               "endpoint committed (%s, %u) before the transition and "
               "(%s, %u) after it, and never held the composition\n",
               label, state_name(s), (unsigned)v,
               state_name(before_s), (unsigned)before_v,
               state_name(after_s), (unsigned)after_v);
        return 1;
    }
    return 0;
}

static moq_result_t snap(const moq_endpoint_t *ep, moq_endpoint_snapshot_t *out)
{
    memset(out, 0, sizeof(*out));
    return moq_endpoint_get_snapshot(ep, out, sizeof(*out));
}

static void pump_established_cb(void *ctx) { pump_established((fix_t *)ctx); }

/* -- 1. one snapshot never straddles first establishment ------------------ *
 * A REAL first-establishment transition is interposed INSIDE the call, fired
 * from the facade's own terminal classification. Whatever the getter does
 * before or after that point, the pair it reports must be one the endpoint
 * actually committed: CONNECTING/0 or ESTABLISHED/16, never a mixture. */
static int test_snapshot_never_straddles_establishment(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    int failures = 0;

    moq_endpoint_snapshot_t before;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &before), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)before.state, (int)MOQ_ENDPOINT_CONNECTING);
    MOQ_TEST_CHECK_EQ_INT((int)before.negotiated_version, 0);

    moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_BEFORE, pump_established_cb,
                                     &fix);
    moq_endpoint_snapshot_t s;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);

    /* The pair must be committed, never composed. */
    bool committed =
        (s.state == MOQ_ENDPOINT_CONNECTING && s.negotiated_version == 0) ||
        (s.state == MOQ_ENDPOINT_ESTABLISHED &&
         s.negotiated_version == SCRIPTED_VERSION);
    if (!committed) {
        printf("FAIL[snapshot.straddles-establishment]: (%s, version %u) is a "
               "TORN pair: the endpoint committed (CONNECTING, 0) before the "
               "interposed transition and (ESTABLISHED, %u) after it, and never "
               "held the composition\n",
               state_name(s.state), (unsigned)s.negotiated_version,
               (unsigned)SCRIPTED_VERSION);
        failures++;
    }

    fix_free(&fix);
    return failures;
}

/* -- 1b. neither legacy composition survives ------------------------------ *
 * Each order gets TWO fixtures. The first is a NEGATIVE CONTROL that composes
 * the two real legacy accessors with the seam armed: it must fire exactly once
 * and must observe the exact torn pair, so an inert or mis-positioned seam
 * fails here before it can make the oracle below look meaningful. The second
 * runs the composite getter, which must report a committed pair. */
static int test_snapshot_kills_both_legacy_compositions(void)
{
    struct {
        const char *label;
        int at;
        moq_endpoint_state_t torn_state;
        moq_version_t        torn_version;
    } cases[] = {
        /* state(); version()  -- the transition lands before version reads,
         * so the composition sees CONNECTING with the new version. */
        { "state-then-version", 1, MOQ_ENDPOINT_CONNECTING, SCRIPTED_VERSION },
        /* version(); state()  -- the transition lands after version reads,
         * so the composition sees ESTABLISHED with no version. */
        { "version-then-state", 2, MOQ_ENDPOINT_ESTABLISHED, 0 },
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        /* -- negative control: the seam is live and tears as designed ------ */
        {
            fix_t ctl;
            if (!fix_init(&ctl)) { printf("FAIL[setup]: fixture\n"); return failures + 1; }
            moq_endpoint_test_arm_interpose(ctl.ep, cases[i].at,
                                            pump_established_cb, &ctl);
            moq_endpoint_state_t cs;
            moq_version_t cv;
            if (cases[i].at == 1) {
                cs = moq_endpoint_state(ctl.ep);
                cv = moq_endpoint_negotiated_version(ctl.ep);
            } else {
                cv = moq_endpoint_negotiated_version(ctl.ep);
                cs = moq_endpoint_state(ctl.ep);
            }
            unsigned fires = moq_endpoint_test_interpose_fires(ctl.ep);
            if (fires != 1) {
                printf("FAIL[snapshot.composition.%s.liveness]: the "
                       "interposition fired %u times, expected exactly 1; an "
                       "inert seam proves nothing about the oracle below\n",
                       cases[i].label, fires);
                failures++;
            }
            if (cs != cases[i].torn_state || cv != cases[i].torn_version) {
                printf("FAIL[snapshot.composition.%s.control]: composing the "
                       "legacy accessors gave (%s, version %u); the seam is "
                       "positioned to produce (%s, version %u)\n",
                       cases[i].label, state_name(cs), (unsigned)cv,
                       state_name(cases[i].torn_state),
                       (unsigned)cases[i].torn_version);
                failures++;
            }
            /* The transition really committed. */
            moq_endpoint_snapshot_t after;
            MOQ_TEST_CHECK_EQ_INT((int)snap(ctl.ep, &after), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)after.state, (int)MOQ_ENDPOINT_ESTABLISHED);
            MOQ_TEST_CHECK_EQ_INT((int)after.negotiated_version,
                                  (int)SCRIPTED_VERSION);
            fix_free(&ctl);
        }

        /* -- the oracle: a fresh fixture, the composite getter ------------- */
        {
            fix_t fix;
            if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return failures + 1; }
            moq_endpoint_test_arm_interpose(fix.ep, cases[i].at,
                                            pump_established_cb, &fix);
            moq_endpoint_snapshot_t s;
            MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);
            bool committed =
                (s.state == MOQ_ENDPOINT_CONNECTING && s.negotiated_version == 0) ||
                (s.state == MOQ_ENDPOINT_ESTABLISHED &&
                 s.negotiated_version == SCRIPTED_VERSION);
            if (!committed) {
                printf("FAIL[snapshot.composition.%s]: (%s, version %u) is a "
                       "TORN pair: the endpoint committed (CONNECTING, 0) "
                       "before the interposed transition and (ESTABLISHED, %u) "
                       "after it\n",
                       cases[i].label, state_name(s.state),
                       (unsigned)s.negotiated_version, (unsigned)SCRIPTED_VERSION);
                failures++;
            }
            fix_free(&fix);
        }
    }
    return failures;
}

/* -- 2. a classified terminal implies CLOSED in the SAME snapshot --------- *
 * The transition interposed inside the call is a real terminal, produced by
 * the product's own classifier from the facade's fatal inputs. */
static void go_terminal_cb(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    moq_endpoint_test_probe_set_terminal(fix->probe, true, SCRIPTED_FATAL_CODE,
                                         false);
}

static int test_snapshot_classified_terminal_implies_closed(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    pump_established(&fix);
    int failures = 0;

    moq_endpoint_snapshot_t before;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &before), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)before.state, (int)MOQ_ENDPOINT_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)before.terminal_reason,
                          (int)MOQ_ENDPOINT_TERMINAL_NONE);

    moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_BEFORE, go_terminal_cb, &fix);
    moq_endpoint_snapshot_t s;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);

    if (s.terminal_reason != MOQ_ENDPOINT_TERMINAL_NONE &&
        s.state != MOQ_ENDPOINT_CLOSED) {
        printf("FAIL[snapshot.terminal-implies-closed]: (%s, %s) -- a non-NONE "
               "terminal reason must imply CLOSED in the same snapshot\n",
               state_name(s.state), reason_name(s.terminal_reason));
        failures++;
    }

    /* And the version survives the terminal. */
    moq_endpoint_snapshot_t after;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &after), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)after.state, (int)MOQ_ENDPOINT_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)after.terminal_reason,
                          (int)MOQ_ENDPOINT_TERMINAL_PROTOCOL);
    MOQ_TEST_CHECK(after.terminal_detail_code == SCRIPTED_FATAL_CODE);
    MOQ_TEST_CHECK_EQ_INT((int)after.negotiated_version, (int)SCRIPTED_VERSION);

    fix_free(&fix);
    return failures;
}

/* -- 3. sized-output ABI, and CLOSED+NONE -------------------------------- */
static int test_snapshot_sized_output_and_unclassified(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    int failures = 0;
    moq_endpoint_snapshot_t s;

    /* v0 floor is the V0 SIZE, not sizeof(current). */
    MOQ_TEST_CHECK(MOQ_ENDPOINT_SNAPSHOT_V0_SIZE <= sizeof(s));

    /* Rejections write NO caller byte. */
    unsigned char guard[sizeof(moq_endpoint_snapshot_t) + 16];
    memset(guard, 0xA5, sizeof(guard));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_get_snapshot(fix.ep,
                                       (moq_endpoint_snapshot_t *)guard,
                                       MOQ_ENDPOINT_SNAPSHOT_V0_SIZE - 1),
        (int)MOQ_ERR_INVAL);
    for (size_t i = 0; i < sizeof(guard); i++) {
        if (guard[i] != 0xA5) {
            printf("FAIL[snapshot.undersized-writes]: an undersized call wrote "
                   "caller byte %zu\n", i);
            failures++;
            break;
        }
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_get_snapshot(fix.ep, NULL, sizeof(s)),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_get_snapshot(NULL, &s, sizeof(s)),
                          (int)MOQ_ERR_INVAL);

    /* Exactly-v0 call: struct_size stamps the bytes written. */
    memset(guard, 0xA5, sizeof(guard));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_get_snapshot(fix.ep,
                                       (moq_endpoint_snapshot_t *)guard,
                                       MOQ_ENDPOINT_SNAPSHOT_V0_SIZE),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)((moq_endpoint_snapshot_t *)guard)->struct_size,
        (int)MOQ_ENDPOINT_SNAPSHOT_V0_SIZE);
    /* An oversized caller tail is untouched. */
    for (size_t i = MOQ_ENDPOINT_SNAPSHOT_V0_SIZE; i < sizeof(guard); i++) {
        if (guard[i] != 0xA5) {
            printf("FAIL[snapshot.oversized-tail]: byte %zu past the requested "
                   "size was overwritten\n", i);
            failures++;
            break;
        }
    }

    /* An OVERSIZED request writes at most sizeof(*out) and never the caller's
     * tail beyond it. At the v0 layout MOQ_ENDPOINT_SNAPSHOT_V0_SIZE equals
     * sizeof(moq_endpoint_snapshot_t), so this -- not an undersized request --
     * is the only case that can distinguish a copy bounded by the STRUCT from
     * one bounded by the caller's REQUEST. */
    memset(guard, 0xA5, sizeof(guard));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_get_snapshot(fix.ep,
                                       (moq_endpoint_snapshot_t *)guard,
                                       sizeof(guard)),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)((moq_endpoint_snapshot_t *)guard)->struct_size,
                          (int)sizeof(moq_endpoint_snapshot_t));
    for (size_t i = sizeof(moq_endpoint_snapshot_t); i < sizeof(guard); i++) {
        if (guard[i] != 0xA5) {
            printf("FAIL[snapshot.oversized-request]: byte %zu past the struct "
                   "was written; a sized copy is bounded by the STRUCT, not by "
                   "the caller's requested size\n", i);
            failures++;
            break;
        }
    }

    /* Repeated calls are stable and every written byte is deterministic. */
    moq_endpoint_snapshot_t a, b;
    memset(&a, 0x11, sizeof(a));
    memset(&b, 0x22, sizeof(b));
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_get_snapshot(fix.ep, &a, sizeof(a)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_get_snapshot(fix.ep, &b, sizeof(b)),
                          (int)MOQ_OK);
    if (memcmp(&a, &b, sizeof(a)) != 0) {
        printf("FAIL[snapshot.deterministic]: two snapshots of an unchanged "
               "endpoint differ byte for byte\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)a.struct_size, (int)sizeof(a));

    /* CLOSED + NONE: a local stop before any terminal condition. The endpoint
     * is never pumped, so no session ever reaches it and nothing is
     * negotiated. */
    fix_t nf;
    if (!fix_init(&nf)) { printf("FAIL[setup]: fixture\n"); fix_free(&fix); return failures + 1; }
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(nf.ep), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)snap(nf.ep, &s), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s.state, (int)MOQ_ENDPOINT_CLOSED);
    MOQ_TEST_CHECK_EQ_INT((int)s.terminal_reason,
                          (int)MOQ_ENDPOINT_TERMINAL_NONE);
    MOQ_TEST_CHECK_EQ_INT((int)s.negotiated_version, 0);
    fix_free(&nf);

    fix_free(&fix);
    return failures;
}

/* -- 3b. a terminal that lands WHILE a classification is in flight -------- *
 * The hard ordering, and the one a terminal-first getter does not survive:
 *
 *   1. the terminal is classified as NONE, with the endpoint CONNECTING/0;
 *   2. the facade goes terminal;
 *   3. first establishment commits ESTABLISHED/version;
 *   4. the getter reads that new pair and pairs it with the stale NONE.
 *
 * The endpoint was already terminal when it established, so no observer could
 * ever have seen ESTABLISHED/version/NONE. Monotonicity does not rescue this:
 * a NONE sampled early hides a terminal that happened afterwards. */
static void terminal_then_establish_cb(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    /* The facade goes terminal FIRST ... */
    moq_endpoint_test_probe_set_terminal(fix->probe, true, SCRIPTED_FATAL_CODE,
                                         false);
    /* ... and only then does first establishment commit. */
    pump_established(fix);
}

static int test_snapshot_survives_terminal_during_classification(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    int failures = 0;

    moq_endpoint_snapshot_t before;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &before), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)before.state, (int)MOQ_ENDPOINT_CONNECTING);
    MOQ_TEST_CHECK_EQ_INT((int)before.negotiated_version, 0);
    MOQ_TEST_CHECK_EQ_INT((int)before.terminal_reason,
                          (int)MOQ_ENDPOINT_TERMINAL_NONE);

    moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_AFTER,
                                     terminal_then_establish_cb, &fix);
    moq_endpoint_snapshot_t s;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);

    /* Liveness: an unfired one-shot would leave the endpoint untouched and let
     * a CONNECTING/0/NONE answer pass for the wrong reason. */
    unsigned fires = moq_endpoint_test_probe_hook_fires(fix.probe);
    if (fires != 1) {
        printf("FAIL[snapshot.terminal-during-classification.liveness]: the "
               "after-classification one-shot fired %u times, expected exactly "
               "1\n", fires);
        failures++;
    }
    /* The EXACT settled record: the retry path must have re-read the local
     * tuple and re-classified the now-terminal facade. */
    if (s.state != MOQ_ENDPOINT_CLOSED ||
        s.negotiated_version != SCRIPTED_VERSION ||
        s.terminal_reason != MOQ_ENDPOINT_TERMINAL_PROTOCOL ||
        s.terminal_detail_code != SCRIPTED_FATAL_CODE) {
        printf("FAIL[snapshot.terminal-during-classification]: reported "
               "(%s, version %u, %s, detail %llu); the facade went terminal "
               "and the endpoint then established, so the only coherent "
               "record is (CLOSED, %u, PROTOCOL, %llu)\n",
               state_name(s.state), (unsigned)s.negotiated_version,
               reason_name(s.terminal_reason),
               (unsigned long long)s.terminal_detail_code,
               (unsigned)SCRIPTED_VERSION,
               (unsigned long long)SCRIPTED_FATAL_CODE);
        failures++;
    }

    fix_free(&fix);
    return failures;
}

/* -- 3c. a mutation that CROSSES the sample forces a re-read -------------- *
 * The generation guarantee is not merely "coherent": a change that crosses the
 * terminal sample forces the local tuple to be read again, so the returned
 * record is never older than a mutation the call itself observed.
 *
 * These two cases are what make each generation advance load-bearing.
 *
 * (a) establishment + terminal cross the sample. Without the advance at first
 *     establishment the reader keeps its pre-establishment pair and reports a
 *     terminal with NO negotiated version -- which asserts an endpoint that
 *     closed without ever negotiating, and this one did. */
static int test_snapshot_rereads_across_establishment(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    int failures = 0;

    /* Fires BEFORE the classification is computed, so the reason returned is
     * the post-transition one while the pair was captured before it. */
    moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_BEFORE,
                                     terminal_then_establish_cb, &fix);
    moq_endpoint_snapshot_t s;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);

    if (s.terminal_reason != MOQ_ENDPOINT_TERMINAL_NONE &&
        s.negotiated_version == 0) {
        printf("FAIL[snapshot.reread-establishment]: reported (%s, version 0, "
               "%s) -- the endpoint DID negotiate version %u before this call "
               "classified its terminal, so a version of 0 asserts a "
               "never-established endpoint that this one is not\n",
               state_name(s.state), reason_name(s.terminal_reason),
               (unsigned)SCRIPTED_VERSION);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)s.state, (int)MOQ_ENDPOINT_CLOSED);
    fix_free(&fix);
    return failures;
}

/* (a2) the pump STATE changes alone, with the version already latched.
 *
 * A real ESTABLISHED -> CLOSED session transition, driven through the same
 * ep_pump_cycle the product uses. The version is already latched, so the
 * version's own advance cannot stand in for the state's here. */
static moq_result_t g_close_rc;

static void close_session_and_pump_cb(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    g_close_rc = moq_session_close(moq_simpair_client(fix->sp), 0, NULL, 0);
    pump_established(fix);
}

static int test_snapshot_rereads_across_state_only_change(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    pump_established(&fix);
    int failures = 0;

    moq_endpoint_snapshot_t before;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &before), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)before.state, (int)MOQ_ENDPOINT_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)before.negotiated_version, (int)SCRIPTED_VERSION);

    g_close_rc = MOQ_ERR_INVAL;
    moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_BEFORE,
                                     close_session_and_pump_cb, &fix);
    moq_endpoint_snapshot_t s;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);
    /* The precondition is named in its own right: a failed close must not
     * surface only as a downstream state mismatch. */
    if (g_close_rc != MOQ_OK) {
        printf("FAIL[snapshot.reread-state-only.prereq]: moq_session_close "
               "returned %d, so no real ESTABLISHED -> CLOSED transition was "
               "driven\n", (int)g_close_rc);
        failures++;
    }

    if (s.state != MOQ_ENDPOINT_CLOSED) {
        printf("FAIL[snapshot.reread-state-only]: reported %s from a call "
               "during which the session closed and the endpoint committed "
               "CLOSED; the pump state's own generation advance is what forces "
               "the re-read here, the version being already latched\n",
               state_name(s.state));
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)s.negotiated_version, (int)SCRIPTED_VERSION);
    fix_free(&fix);
    return failures;
}

/* (b) a stop crosses the sample. Without the advance at stop the reader keeps
 *     its pre-stop state and reports a live endpoint from a call during which
 *     it observed the stop. */
static moq_result_t g_stop_rc;

static void stop_cb(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    g_stop_rc = moq_endpoint_stop(fix->ep);
}

static int test_snapshot_rereads_across_stop(void)
{
    fix_t fix;
    if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return 1; }
    pump_established(&fix);
    int failures = 0;

    moq_endpoint_snapshot_t before;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &before), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)before.state, (int)MOQ_ENDPOINT_ESTABLISHED);

    g_stop_rc = MOQ_ERR_INVAL;
    moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_BEFORE, stop_cb, &fix);
    moq_endpoint_snapshot_t s;
    MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &s), (int)MOQ_OK);
    if (g_stop_rc != MOQ_OK) {
        printf("FAIL[snapshot.reread-stop.prereq]: moq_endpoint_stop returned "
               "%d, so no real stop crossed the sample\n", (int)g_stop_rc);
        failures++;
    }

    if (s.state != MOQ_ENDPOINT_CLOSED) {
        printf("FAIL[snapshot.reread-stop]: reported %s from a call during "
               "which the endpoint was stopped; a mutation that crosses the "
               "terminal sample must force the local tuple to be read again\n",
               state_name(s.state));
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)s.negotiated_version, (int)SCRIPTED_VERSION);
    fix_free(&fix);
    return failures;
}

/* -- 3f. a terminal latched INSIDE one classification sample -------------- *
 * Every shipped vt->terminal composes separately-locked facade accessors --
 * msquic_managed.c latches term_fatal, term_closed and both codes together
 * under m->mu (:438-444) while each public accessor takes that mutex on its
 * own (:2874+), and pico_wt is the same shape. So this ordering is legal:
 *
 *   1. the shim reads fatal == false;
 *   2. the backend atomically latches fatal, its code, and closed;
 *   3. the shim reads closed == true and classifies CLEAN.
 *
 * There was no instant at which that facade was cleanly closed and not fatal.
 * The endpoint's own generation cannot see it: nothing endpoint-local moved. */
static void latch_fatal_and_clean_cb(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    /* One atomic latch, exactly as the backend does it. */
    moq_endpoint_test_probe_set_terminal(fix->probe, true, SCRIPTED_FATAL_CODE,
                                         true);
}

static int test_terminal_latched_inside_one_sample(void)
{
    int failures = 0;
    /* Both public accessors must survive it; each gets its own fixture so the
     * one-shot is armed for exactly one call. */
    for (int use_snapshot = 0; use_snapshot <= 1; use_snapshot++) {
        fix_t fix;
        if (!fix_init(&fix)) { printf("FAIL[setup]: fixture\n"); return failures + 1; }
        pump_established(&fix);
        moq_endpoint_test_probe_arm_hook(fix.probe, HOOK_MID,
                                     latch_fatal_and_clean_cb, &fix);
        moq_endpoint_terminal_reason_t r = MOQ_ENDPOINT_TERMINAL_NONE;
        uint64_t detail = 0;
        const char *which;
        if (use_snapshot) {
            which = "snapshot";
            moq_endpoint_snapshot_t sn;
            MOQ_TEST_CHECK_EQ_INT((int)snap(fix.ep, &sn), (int)MOQ_OK);
            r = sn.terminal_reason;
            detail = sn.terminal_detail_code;
        } else {
            which = "get_terminal";
            MOQ_TEST_CHECK_EQ_INT((int)read_terminal(fix.ep, &r, &detail),
                                  (int)MOQ_OK);
        }

        /* Liveness: an inert seam must not let this pass. */
        unsigned fires = moq_endpoint_test_probe_hook_fires(fix.probe);
        if (fires != 1) {
            printf("FAIL[terminal.mid-sample.%s.liveness]: the mid-sample "
                   "one-shot fired %u times, expected exactly 1\n",
                   which, fires);
            failures++;
        }
        /* And the transition really happened in the facade -- OBSERVED, not
         * asserted by writing the expected state back. */
        bool pf = false, pc = false;
        uint64_t pcode = 0;
        moq_endpoint_test_probe_get_terminal(fix.probe, &pf, &pcode, &pc);
        if (!pf || pcode != SCRIPTED_FATAL_CODE || !pc) {
            printf("FAIL[terminal.mid-sample.%s.inputs]: after the callback the "
                   "facade holds (fatal %d, code %llu, clean %d); the "
                   "interposed latch sets (1, %llu, 1)\n",
                   which, (int)pf, (unsigned long long)pcode, (int)pc,
                   (unsigned long long)SCRIPTED_FATAL_CODE);
            failures++;
        }

        if (r == MOQ_ENDPOINT_TERMINAL_CLEAN) {
            printf("FAIL[terminal.mid-sample.%s]: classified CLEAN from a "
                   "sample that read fatal BEFORE the backend latched it and "
                   "clean AFTER; the facade was never cleanly closed without "
                   "being fatal\n", which);
            failures++;
        }
        if (r != MOQ_ENDPOINT_TERMINAL_PROTOCOL || detail != SCRIPTED_FATAL_CODE) {
            printf("FAIL[terminal.mid-sample.%s.settled]: reported (%s, detail "
                   "%llu); the settled facade is fatal with code %llu, so the "
                   "only coherent answer is PROTOCOL with that code\n",
                   which, reason_name(r), (unsigned long long)detail,
                   (unsigned long long)SCRIPTED_FATAL_CODE);
            failures++;
        }
        fix_free(&fix);
    }
    return failures;
}

/* -- 4. the documented NULL-terminal generic fallback -------------------- *
 * The backend vtable's `terminal` field documents NULL as "treated as generic
 * (NONE/PROTOCOL/TRANSPORT/CLEAN only)", and the classifier honours that: a
 * NULL slot is classified from the facade's own fatal/code/clean inputs. Every
 * shipped backend sets the slot, so these rows pin the documented fallback for
 * the next backend that does not -- without it such a backend would report NONE
 * for every terminal, which is exactly the unclassifiable case the Kotlin
 * mapping must not confuse with a clean close.
 *
 * The rows are the generic classifier's own outcomes for the facade inputs. */
static int test_null_terminal_slot_classifies_generically(void)
{
    struct {
        const char *label;
        bool     fatal;
        uint64_t code;
        bool     clean;
        moq_endpoint_terminal_reason_t want;
        uint64_t want_detail;
    } rows[] = {
        { "protocol",  true,  SCRIPTED_FATAL_CODE, false,
          MOQ_ENDPOINT_TERMINAL_PROTOCOL,  SCRIPTED_FATAL_CODE },
        { "transport", true,  0,                   false,
          MOQ_ENDPOINT_TERMINAL_TRANSPORT, 0 },
        { "clean",     false, 0,                   true,
          MOQ_ENDPOINT_TERMINAL_CLEAN,     0 },
        { "none",      false, 0,                   false,
          MOQ_ENDPOINT_TERMINAL_NONE,      0 },
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        fix_t fix;
        if (!fix_init_mode(&fix, /*null_terminal=*/true)) {
            printf("FAIL[setup]: fixture\n");
            return 1;
        }
        moq_endpoint_test_probe_set_terminal(fix.probe, rows[i].fatal,
                                             rows[i].code, rows[i].clean);
        uint64_t detail = 0;
        moq_endpoint_terminal_reason_t r = MOQ_ENDPOINT_TERMINAL_NONE;
        moq_result_t trc = read_terminal(fix.ep, &r, &detail);
        if (trc != MOQ_OK) {
            printf("FAIL[terminal.null-slot.%s.accessor]: "
                   "moq_endpoint_get_terminal returned %d; a failed getter is "
                   "not a NONE observation\n", rows[i].label, (int)trc);
            failures++;
        } else if (r != rows[i].want || detail != rows[i].want_detail) {
            printf("FAIL[terminal.null-slot.%s]: a NULL terminal slot reported "
                   "(%s, detail %llu); the vtable documents NULL as generic "
                   "classification, which for these facade inputs is "
                   "(%s, detail %llu)\n",
                   rows[i].label, reason_name(r), (unsigned long long)detail,
                   reason_name(rows[i].want),
                   (unsigned long long)rows[i].want_detail);
            failures++;
        }
        fix_free(&fix);
    }
    return failures;
}

/* -- 5. every post-pair partial setup path releases what it acquired ------ *
 * Each of the five branches after the SimPair exists is REACHED by injection
 * rather than argued from the successful path, and the allocator balance must
 * return to its entry value. */
static int test_partial_setup_releases_everything(void)
{
    /* Every post-pair branch of fix_init_mode(), each REACHED by injection.
     * The SimPair create/start/quiescence branches are not injected here; they
     * destroy the pair explicitly in source before returning NULL, and this
     * test does not claim otherwise. */
    struct { const char *label; setup_fault_t fault; } cases[] = {
        { "not-established", SETUP_FAIL_NOT_ESTABLISHED },
        { "session-version", SETUP_FAIL_SESSION_VERSION },
        { "endpoint-alloc",  SETUP_FAIL_ENDPOINT_ALLOC },
        { "probe-storage",   SETUP_FAIL_PROBE_STORAGE },
        { "probe-install",   SETUP_FAIL_PROBE_INSTALL },
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        long before = g_balance;
        fix_t fix;
        g_setup_fault = cases[i].fault;
        bool ok = fix_init_mode(&fix, /*null_terminal=*/false);
        g_setup_fault = SETUP_OK;
        if (ok) {
            printf("FAIL[setup.partial.%s]: the injected setup failure did not "
                   "reach its failure path\n", cases[i].label);
            fix_free(&fix);
            failures++;
            continue;
        }
        if (g_balance != before) {
            printf("FAIL[setup.partial.%s]: the failed setup leaked; allocator "
                   "balance %ld, expected %ld\n",
                   cases[i].label, g_balance, before);
            failures++;
        }
    }
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_snapshot_never_straddles_establishment();
    failures += test_snapshot_kills_both_legacy_compositions();
    failures += test_snapshot_classified_terminal_implies_closed();
    failures += test_snapshot_sized_output_and_unclassified();
    failures += test_snapshot_survives_terminal_during_classification();
    failures += test_snapshot_rereads_across_establishment();
    failures += test_snapshot_rereads_across_state_only_change();
    failures += test_snapshot_rereads_across_stop();
    failures += test_terminal_latched_inside_one_sample();
    failures += test_null_terminal_slot_classifies_generically();
    failures += test_partial_setup_releases_everything();

    /* Every allocation this fixture made is released, including on the
     * partial-setup paths. */
    if (g_balance != 0) {
        printf("FAIL[fixture.balance]: allocator balance %ld, expected 0\n",
               g_balance);
        failures++;
    }
    if (failures == 0) {
        printf("endpoint snapshot coherence: 0 failure(s)\n");
        return 0;
    }
    printf("\nendpoint snapshot coherence: %d failure(s)\n", failures);
    return 1;
}
