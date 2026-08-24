/* TEST-ONLY seam shared by pico_wt_managed.c (compiled with
 * MOQ_PICO_WT_TESTING) and the ownership oracle. Never installed, never
 * exported, and not referenced by any shipped translation unit.
 *
 * The kinds are compared INSIDE the implementation, so no test converts a
 * function pointer through an object pointer type.
 */
#ifndef MOQ_PICO_WT_TEST_SEAM_H
#define MOQ_PICO_WT_TEST_SEAM_H

#include <stddef.h>
#include <stdint.h>

typedef struct moq_pico_wt_managed moq_pico_wt_managed_t;

/* which function is bound to the WT control stream */
typedef enum {
    MOQ_PWT_CB_NONE = 0,     /* path_callback == NULL */
    MOQ_PWT_CB_ADAPTER,      /* moq_pico_wt_callback */
    MOQ_PWT_CB_FACADE,       /* managed_server_wt_cb */
    MOQ_PWT_CB_OTHER
} moq_pwt_cb_kind_t;

/* which object the bound context points at */
typedef enum {
    MOQ_PWT_CTX_NONE = 0,    /* path_callback_ctx == NULL */
    MOQ_PWT_CTX_ADAPTER,     /* the moq_pico_wt_conn_t the facade owns */
    MOQ_PWT_CTX_FACADE,      /* the moq_pico_wt_managed_t itself */
    MOQ_PWT_CTX_OTHER
} moq_pwt_ctx_kind_t;

#define MOQ_PWT_EV_MAX 128

/* Read the server's WT control-stream binding. Returns 1 if the control
 * stream is still present in h3zero's tree, 0 if it is gone, <0 on misuse.
 * MUST be called only after the owning network thread has been joined. */
int moq_pico_wt_managed_test_ctrl_binding(moq_pico_wt_managed_t *m,
                                          moq_pwt_cb_kind_t  *out_cb,
                                          moq_pwt_ctx_kind_t *out_ctx);

/* Per-facade callback event inventory, recorded on the network thread and
 * readable only after that thread is joined. */
size_t moq_pico_wt_managed_test_event_count(moq_pico_wt_managed_t *m);
size_t moq_pico_wt_managed_test_event_overflow(moq_pico_wt_managed_t *m);
int    moq_pico_wt_managed_test_event_at(moq_pico_wt_managed_t *m, size_t i,
                                         int *out_event,
                                         moq_pwt_ctx_kind_t *out_ctx);

/* Extract the facade's owned adapter pointer and invoke the real adapter
 * destructor. Not the facade teardown order: it detaches the owned pointer
 * after quiescence, then calls moq_pico_wt_conn_destroy() directly, so the
 * facade can still be destroyed afterwards without a double free. */
void moq_pico_wt_managed_test_detach_conn(moq_pico_wt_managed_t *m);

#endif
