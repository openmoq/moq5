/*
 * White-box discriminators for the moq_wtquic_network_managed deadline state
 * machine (recompiled with MOQ_WTQ_MM_TESTING). The transport + adapter
 * primitives are stubbed inside the facade, so nwm_doorbell / nwm_rearm_deadline
 * run against a synthetic connection with NO real timer, NO real transport, and
 * NO session-deadline fixture. Each property is asserted STRUCTURALLY from an
 * ordered op recorder (service / pump / ring / cancel), and each is pinned
 * independently of the others:
 *
 *   due ordering            a due deadline services before it pumps
 *   ordinary-wake preserve  a not-due wake leaves the existing arm in place
 *   rearm replace (both)    a recompute REPLACES the arm (one doorbell slot)
 *   cancellation            UINT64_MAX cancels rather than arming
 *   no self-spin            an arm never schedules a zero/past-due delay
 *
 * The real-transport idle periodic-wake test (test_wtquic_network_managed.c,
 * t_idle_deadline) remains the native-doorbell linchpin; these are its
 * deterministic complements, not a substitute.
 */
#include "wtquic_network_managed_test_internal.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* --- ordered op recorder ----------------------------------------------------- */

#define MAXOPS 64
static struct { moq_wtquic_nwm_op_t op; uint64_t arg; } g_ops[MAXOPS];
static int g_nops;
static void rec(void *ctx, moq_wtquic_nwm_op_t op, uint64_t arg)
{
    (void)ctx;
    if (g_nops < MAXOPS) { g_ops[g_nops].op = op; g_ops[g_nops].arg = arg; g_nops++; }
}
static void rec_reset(void) { g_nops = 0; }
static int op_count(moq_wtquic_nwm_op_t op)
{
    int n = 0;
    for (int i = 0; i < g_nops; i++) if (g_ops[i].op == op) n++;
    return n;
}
static int first_index(moq_wtquic_nwm_op_t op)
{
    for (int i = 0; i < g_nops; i++) if (g_ops[i].op == op) return i;
    return -1;
}
/* last RING's delay argument, or UINT64_MAX if none */
static uint64_t last_ring_delay(void)
{
    uint64_t d = UINT64_MAX;
    for (int i = 0; i < g_nops; i++) if (g_ops[i].op == MOQ_WTQ_NWM_OP_RING) d = g_ops[i].arg;
    return d;
}

/* --- controllable app service-deadline --------------------------------------- */

static _Atomic uint64_t g_app_deadline = UINT64_MAX;
static uint64_t app_cb(void *ctx) { (void)ctx; return atomic_load(&g_app_deadline); }

/* --- cases ------------------------------------------------------------------- */

/* Due doorbell services the session BEFORE the app pump: the first op is
 * SERVICE (conn_service pass 1), the app pump runs only inside its between-pass
 * hook -- an ordinary (not-due) wake would run the PUMP op first. */
static void t_due_orders_service_before_pump(void)
{
    int before = failures;
    atomic_store(&g_app_deadline, UINT64_MAX);          /* no re-arm afterwards */
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    /* cached deadline already in the past -> due */
    moq_wtquic_network_managed_test_set_wake_deadline(
        m, moq_wtquic_network_managed_test_now_us() - 1000);
    rec_reset();
    moq_wtquic_network_managed_test_doorbell(m);

    CHECK(g_nops >= 2);
    CHECK(first_index(MOQ_WTQ_NWM_OP_SERVICE) == 0);    /* service is first */
    CHECK(first_index(MOQ_WTQ_NWM_OP_SERVICE) <
          first_index(MOQ_WTQ_NWM_OP_PUMP));            /* before the app pump */

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: due_orders_service_before_pump\n");
}

/* Not-due (ordinary) doorbell pumps first, then services, and MUST NOT drop the
 * pending arm: it re-arms with the still-future delay (RING, not CANCEL). */
static void t_ordinary_wake_preserves_arm(void)
{
    int before = failures;
    uint64_t now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 10 * 1000000ull);   /* 10 s out */
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    moq_wtquic_network_managed_test_set_wake_deadline(m, now + 10 * 1000000ull);
    rec_reset();
    moq_wtquic_network_managed_test_doorbell(m);

    CHECK(first_index(MOQ_WTQ_NWM_OP_PUMP) == 0);        /* pump first (not-due) */
    CHECK(first_index(MOQ_WTQ_NWM_OP_PUMP) <
          first_index(MOQ_WTQ_NWM_OP_SERVICE));
    CHECK(op_count(MOQ_WTQ_NWM_OP_RING) >= 1);           /* arm preserved */
    CHECK(op_count(MOQ_WTQ_NWM_OP_CANCEL) == 0);         /* never cancelled */
    /* the re-armed delay is still ~10 s, not consumed */
    CHECK(last_ring_delay() >= 5 * 1000000ull);

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: ordinary_wake_preserves_arm\n");
}

/* A recompute replaces the previous arm with an EARLIER deadline. */
static void t_rearm_replace_earlier(void)
{
    int before = failures;
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    uint64_t now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 5 * 1000000ull);    /* 5 s */
    rec_reset();
    moq_wtquic_network_managed_test_rearm(m);
    uint64_t d1 = last_ring_delay();
    CHECK(d1 >= 4 * 1000000ull && d1 <= 6 * 1000000ull);

    now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 100 * 1000ull);     /* 100 ms (earlier) */
    rec_reset();
    moq_wtquic_network_managed_test_rearm(m);
    uint64_t d2 = last_ring_delay();
    CHECK(d2 <= 300 * 1000ull);                              /* replaced */
    CHECK(d2 < d1);

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: rearm_replace_earlier\n");
}

/* A recompute replaces the previous arm with a LATER deadline; the old (earlier)
 * arm must not survive to fire early. */
static void t_rearm_replace_later(void)
{
    int before = failures;
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    uint64_t now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 100 * 1000ull);     /* 100 ms */
    rec_reset();
    moq_wtquic_network_managed_test_rearm(m);
    uint64_t d1 = last_ring_delay();
    CHECK(d1 <= 300 * 1000ull);

    now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 5 * 1000000ull);    /* 5 s (later) */
    rec_reset();
    moq_wtquic_network_managed_test_rearm(m);
    uint64_t d2 = last_ring_delay();
    CHECK(d2 >= 4 * 1000000ull);                            /* replaced */
    CHECK(d2 > d1);
    /* the cached deadline reflects the new, later time */
    CHECK(moq_wtquic_network_managed_test_get_wake_deadline(m) >=
          now + 4 * 1000000ull);

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: rearm_replace_later\n");
}

/* A callback result of UINT64_MAX cancels the arm (cancel_after, no ring), and
 * the cached deadline clears. */
static void t_cancel_on_no_deadline(void)
{
    int before = failures;
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    uint64_t now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 200 * 1000ull);
    rec_reset();
    moq_wtquic_network_managed_test_rearm(m);
    CHECK(op_count(MOQ_WTQ_NWM_OP_RING) == 1);              /* armed */

    atomic_store(&g_app_deadline, UINT64_MAX);              /* app cleared it */
    rec_reset();
    moq_wtquic_network_managed_test_rearm(m);
    CHECK(op_count(MOQ_WTQ_NWM_OP_CANCEL) == 1);            /* cancelled */
    CHECK(op_count(MOQ_WTQ_NWM_OP_RING) == 0);
    CHECK(moq_wtquic_network_managed_test_get_wake_deadline(m) == UINT64_MAX);

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: cancel_on_no_deadline\n");
}

/* After a due wake whose app deadline has PROGRESSED to the future, the rearm
 * arms a strictly-future delay (>0): the satisfied deadline cannot immediately
 * self-refire. */
static void t_no_self_spin_after_progress(void)
{
    int before = failures;
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    /* the deadline fired (cached in the past) but the app has moved on */
    uint64_t now = moq_wtquic_network_managed_test_now_us();
    atomic_store(&g_app_deadline, now + 100 * 1000ull);     /* progressed */
    moq_wtquic_network_managed_test_set_wake_deadline(m, now - 1000); /* due */
    rec_reset();
    moq_wtquic_network_managed_test_doorbell(m);

    CHECK(first_index(MOQ_WTQ_NWM_OP_SERVICE) == 0);        /* took the due path */
    /* the re-arm is in the future -> no delay-0 immediate re-fire */
    CHECK(last_ring_delay() >= 10 * 1000ull);
    CHECK(last_ring_delay() != UINT64_MAX);

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: no_self_spin_after_progress\n");
}

/* An always-due finite deadline that COUNTS its invocations, so a terminal
 * rearm that (wrongly) consulted it is directly observable. */
static _Atomic int g_app_calls;
static uint64_t app_cb_due_counting(void *ctx)
{ (void)ctx; atomic_fetch_add(&g_app_calls, 1); return 1; /* µs since boot: always due */ }

/* Terminal/teardown state must stop the scheduler consulting the app deadline
 * and drop any pending arm: a generic callback that keeps returning an
 * already-due deadline would otherwise rearm the doorbell forever. */
static void terminal_no_rearm(const char *name,
    void (*latch)(moq_wtquic_network_managed_t *, uint64_t))
{
    int before = failures;
    moq_wtquic_network_managed_t *m =
        moq_wtquic_network_managed_test_new(app_cb_due_counting, NULL);
    CHECK(m != NULL);
    if (m == NULL) return;

    latch(m, 0);                    /* mark terminal (clean close / fatal) */
    /* pre-seed a live cached arm to prove the terminal path clears it */
    moq_wtquic_network_managed_test_set_wake_deadline(m, 1);
    rec_reset();
    atomic_store(&g_app_calls, 0);
    moq_wtquic_network_managed_test_rearm(m);

    CHECK(atomic_load(&g_app_calls) == 0);           /* callback never consulted */
    CHECK(op_count(MOQ_WTQ_NWM_OP_CANCEL) == 1);     /* arm dropped */
    CHECK(op_count(MOQ_WTQ_NWM_OP_RING) == 0);       /* nothing (re)armed */
    CHECK(moq_wtquic_network_managed_test_get_wake_deadline(m) == UINT64_MAX);

    moq_wtquic_network_managed_test_free(m);
    if (failures == before) printf("PASS: %s\n", name);
}
static void t_terminal_closed_no_rearm(void)
{ terminal_no_rearm("terminal_closed_no_rearm",
                    moq_wtquic_network_managed_test_latch_closed); }
static void t_terminal_fatal_no_rearm(void)
{ terminal_no_rearm("terminal_fatal_no_rearm",
                    moq_wtquic_network_managed_test_latch_fatal); }

int main(void)
{
    moq_wtquic_network_managed_test_set_op_recorder(rec, NULL);

    t_due_orders_service_before_pump();
    t_ordinary_wake_preserves_arm();
    t_rearm_replace_earlier();
    t_rearm_replace_later();
    t_cancel_on_no_deadline();
    t_no_self_spin_after_progress();
    t_terminal_closed_no_rearm();
    t_terminal_fatal_no_rearm();

    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("all wtquic network managed deadline discriminators passed\n");
    return 0;
}
