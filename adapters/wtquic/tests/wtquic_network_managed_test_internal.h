/*
 * Private test seam for white-box coverage of the moq_wtquic_network_managed
 * deadline state machine. These symbols exist ONLY in a build with
 * MOQ_WTQ_MM_TESTING defined (the white-box test recompiles the facade source
 * with it); they are NEVER compiled into the production/installed library and
 * never exported.
 *
 * The seam drives nwm_doorbell / nwm_rearm_deadline directly against a
 * synthetic facade whose transport + adapter primitives are stubbed, so the
 * scheduler's ordering, arming, replacement, and cancellation are asserted
 * structurally with no real connection, no wall-clock timer, and no session-
 * deadline fixture. An ordered op recorder captures the exact sequence of
 * service / pump / ring / cancel operations each call produces.
 */
#ifndef WTQUIC_NETWORK_MANAGED_TEST_INTERNAL_H
#define WTQUIC_NETWORK_MANAGED_TEST_INTERNAL_H

#include <moq/wtquic_network_managed.h>

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MOQ_WTQ_NWM_OP_SERVICE = 1,  /* conn_service (its between-pass hook pumps) */
    MOQ_WTQ_NWM_OP_PUMP,         /* on_lane_pump ran */
    MOQ_WTQ_NWM_OP_RING,         /* doorbell_ring_after(delay); arg = delay_us */
    MOQ_WTQ_NWM_OP_CANCEL        /* doorbell_cancel_after */
} moq_wtquic_nwm_op_t;

/* Ordered recorder: invoked for every scheduler primitive, in call order. */
typedef void (*moq_wtquic_nwm_op_recorder_fn)(void *ctx,
                                              moq_wtquic_nwm_op_t op,
                                              uint64_t arg);
void moq_wtquic_network_managed_test_set_op_recorder(
    moq_wtquic_nwm_op_recorder_fn fn, void *ctx);

/* Build / free a synthetic facade with the transport + adapter primitives
 * stubbed (non-NULL conn + adapter sentinels, no session, on_lane_pump
 * recorded). app_deadline_us/ctx wire the app service-deadline query. */
moq_wtquic_network_managed_t *moq_wtquic_network_managed_test_new(
    uint64_t (*app_deadline_us)(void *), void *app_deadline_ctx);
void moq_wtquic_network_managed_test_free(moq_wtquic_network_managed_t *m);

/* Directly drive the two on-domain entry points. */
void moq_wtquic_network_managed_test_doorbell(moq_wtquic_network_managed_t *m);
void moq_wtquic_network_managed_test_rearm(moq_wtquic_network_managed_t *m);

/* Drive the terminal latches (clean close / fatal), so the terminal rearm
 * guard can be exercised white-box. */
void moq_wtquic_network_managed_test_latch_closed(
    moq_wtquic_network_managed_t *m, uint64_t code);
void moq_wtquic_network_managed_test_latch_fatal(
    moq_wtquic_network_managed_t *m, uint64_t code);

/* Read/write the cached absolute combined deadline (drives due detection). */
void moq_wtquic_network_managed_test_set_wake_deadline(
    moq_wtquic_network_managed_t *m, uint64_t v);
uint64_t moq_wtquic_network_managed_test_get_wake_deadline(
    const moq_wtquic_network_managed_t *m);

/* The facade's monotonic clock, so a test can express deadlines relative to
 * the same base the scheduler folds against. */
uint64_t moq_wtquic_network_managed_test_now_us(void);

#ifdef __cplusplus
}
#endif

#endif
