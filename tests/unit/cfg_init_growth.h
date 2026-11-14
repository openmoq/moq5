#ifndef TEST_CFG_INIT_GROWTH_H
#define TEST_CFG_INIT_GROWTH_H

#include <stddef.h>

/* Defined by the translation unit built against the grown header. */

/* sizeof each config as the grown library declares it. */
size_t cfg_init_growth_done_subscribe_size(void);
size_t cfg_init_growth_reject_subscribe_size(void);

/* The grown-header caller's checks. Returns the number that failed. */
int cfg_init_growth_grown_caller(void);

#endif
