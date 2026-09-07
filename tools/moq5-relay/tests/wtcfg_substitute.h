/*
 * The two transport callees, replaced at compile time.
 *
 * The production step is compiled unchanged against these declarations, so the
 * substitutes stand exactly where the facade's own functions stand: same
 * signatures, same call sites, same result path. A wrapper that merely returned
 * a struct would prove nothing about what create is handed -- the capture here
 * happens AT the create invocation, from the pointer the transport would have
 * received.
 */
#ifndef MOQR_TEST_WTCFG_SUBSTITUTE_H
#define MOQR_TEST_WTCFG_SUBSTITUTE_H

#include <stdbool.h>
#include <stddef.h>

#include "moq/wtquic_msquic_managed.h"

/* What the substitutes observed on the last call.
 *
 * The configuration the step builds is an automatic object inside it, so its
 * address is only meaningful while the step is running. Anything that depends
 * on that address is therefore decided INSIDE the create substitute and kept
 * as a result, not as a pointer to compare once the object is gone. */
typedef struct relay_wt_capture {
    bool     init_called;
    size_t   init_size;
    /* live only between the two substitute calls; cleared by create */
    const moq_wtquic_msquic_managed_cfg_t *init_pending;
    /* decided while that object is alive: create was handed the same object
     * the initializer prepared, not a second one */
    bool     init_same_object;

    bool     create_called;
    unsigned create_calls;
    /* the complete configuration, copied at the create invocation */
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_t **out;

    /* what create will return */
    moq_result_t result;
} relay_wt_capture_t;

extern relay_wt_capture_t g_relay_wt_capture;

void relay_test_wt_cfg_init_sized(moq_wtquic_msquic_managed_cfg_t *cfg,
                                  size_t size);
moq_result_t relay_test_wt_create(const moq_wtquic_msquic_managed_cfg_t *cfg,
                                  moq_wtquic_msquic_managed_t **out);

#define MOQR_WT_CFG_INIT_SIZED relay_test_wt_cfg_init_sized
#define MOQR_WT_CREATE         relay_test_wt_create

#endif /* MOQR_TEST_WTCFG_SUBSTITUTE_H */
