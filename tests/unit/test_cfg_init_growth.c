/*
 * The exported config initializers stay frozen when a config grows.
 *
 * This program links a private build of the core in which
 * moq_done_subscribe_cfg_t and moq_reject_subscribe_cfg_t each have one more
 * field after their last. This translation unit is the caller that existed
 * before the growth: it is built against the header as it is, allocates the
 * struct that header declares, and calls the exported symbol. The symbol must
 * clear and stamp exactly the frozen layout and leave the bytes after the
 * caller's struct alone. cfg_init_growth_new.c is the caller built against
 * the grown header, where the same-name macro and the sized function reach
 * the appended field.
 */
#include <moq/session.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "cfg_init_growth.h"
#include "test_support.h"

static int failures = 0;

#define GUARD  32u
#define CANARY 0xA5u

static bool guard_intact(const uint8_t *guard)
{
    for (size_t i = 0; i < GUARD; i++)
        if (guard[i] != CANARY) return false;
    return true;
}

/* A config whose one size macro is both the symbol's stamp and the floor. */
static void v0_caller_done_subscribe(void)
{
    struct {
        moq_done_subscribe_cfg_t cfg;
        uint8_t guard[GUARD];
    } box;

    /* The library's struct really is larger than this caller's, and the
     * guard starts where this caller's struct ends. */
    MOQ_TEST_CHECK(cfg_init_growth_done_subscribe_size() > sizeof(box.cfg));
    MOQ_TEST_CHECK((const uint8_t *)box.guard ==
                   (const uint8_t *)&box.cfg + sizeof(box.cfg));

    /* The exported symbol: the frozen layout, and nothing after the struct. */
    memset(&box, CANARY, sizeof(box));
    (moq_done_subscribe_cfg_init)(&box.cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)box.cfg.struct_size,
                           (size_t)MOQ_DONE_SUBSCRIBE_CFG_V0_SIZE);
    MOQ_TEST_CHECK(guard_intact(box.guard));

    /* This caller's own macro passes its smaller sizeof, which the library
     * honours: its struct, and nothing after it. */
    memset(&box, CANARY, sizeof(box));
    moq_done_subscribe_cfg_init(&box.cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)box.cfg.struct_size, sizeof(box.cfg));
    MOQ_TEST_CHECK(guard_intact(box.guard));
}

/* A config whose symbol stamps the whole published layout while the
 * consuming call accepts a shorter one. */
static void v0_caller_reject_subscribe(void)
{
    struct {
        moq_reject_subscribe_cfg_t cfg;
        uint8_t guard[GUARD];
    } box;

    MOQ_TEST_CHECK(cfg_init_growth_reject_subscribe_size() > sizeof(box.cfg));
    MOQ_TEST_CHECK((const uint8_t *)box.guard ==
                   (const uint8_t *)&box.cfg + sizeof(box.cfg));

    memset(&box, CANARY, sizeof(box));
    (moq_reject_subscribe_cfg_init)(&box.cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)box.cfg.struct_size,
                           (size_t)MOQ_REJECT_SUBSCRIBE_CFG_SIZE_THROUGH_REDIRECT);
    MOQ_TEST_CHECK(guard_intact(box.guard));

    memset(&box, CANARY, sizeof(box));
    moq_reject_subscribe_cfg_init(&box.cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)box.cfg.struct_size, sizeof(box.cfg));
    MOQ_TEST_CHECK(guard_intact(box.guard));
}

int main(void)
{
    v0_caller_done_subscribe();
    v0_caller_reject_subscribe();
    failures += cfg_init_growth_grown_caller();
    MOQ_TEST_PASS("cfg_init_growth");
    return failures ? 1 : 0;
}
