/*
 * The caller built against the grown header; see test_cfg_init_growth.c.
 *
 * This unit is compiled with the generated copy of session.h read ahead of
 * the real one, so both configs have their appended field here, as they do
 * in the library this program links.
 */
#include <moq/session.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "cfg_init_growth.h"
#include "test_support.h"

static int failures = 0;

#define CANARY 0xA5u

size_t cfg_init_growth_done_subscribe_size(void)
{
    return sizeof(moq_done_subscribe_cfg_t);
}

size_t cfg_init_growth_reject_subscribe_size(void)
{
    return sizeof(moq_reject_subscribe_cfg_t);
}

/* What an untouched uint64_t holds after the canary fill. */
static uint64_t canary_word(void)
{
    uint64_t word;
    memset(&word, CANARY, sizeof(word));
    return word;
}

static void grown_caller_done_subscribe(void)
{
    moq_done_subscribe_cfg_t cfg;
    const size_t frozen = MOQ_DONE_SUBSCRIBE_CFG_V0_SIZE;

    /* The appended field lies wholly past the frozen layout. */
    MOQ_TEST_CHECK(offsetof(moq_done_subscribe_cfg_t, growth_probe_tail) >= frozen);
    MOQ_TEST_CHECK(sizeof(cfg) > frozen);

    /* The same-name macro reaches the whole grown struct. */
    memset(&cfg, CANARY, sizeof(cfg));
    moq_done_subscribe_cfg_init(&cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, sizeof(cfg));
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, 0);

    /* The sized function given the frozen size stops there. */
    memset(&cfg, CANARY, sizeof(cfg));
    moq_done_subscribe_cfg_init_sized(&cfg, frozen);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, frozen);
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, canary_word());

    /* The exported symbol stamps the frozen layout for this caller too. */
    memset(&cfg, CANARY, sizeof(cfg));
    (moq_done_subscribe_cfg_init)(&cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, frozen);
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, canary_word());
}

static void grown_caller_reject_subscribe(void)
{
    moq_reject_subscribe_cfg_t cfg;
    const size_t frozen = MOQ_REJECT_SUBSCRIBE_CFG_SIZE_THROUGH_REDIRECT;
    const size_t floor = MOQ_REJECT_SUBSCRIBE_CFG_V0_SIZE;

    MOQ_TEST_CHECK(offsetof(moq_reject_subscribe_cfg_t, growth_probe_tail) >= frozen);
    MOQ_TEST_CHECK(sizeof(cfg) > frozen);
    MOQ_TEST_CHECK(floor < frozen);

    memset(&cfg, CANARY, sizeof(cfg));
    moq_reject_subscribe_cfg_init(&cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, sizeof(cfg));
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, 0);

    memset(&cfg, CANARY, sizeof(cfg));
    moq_reject_subscribe_cfg_init_sized(&cfg, frozen);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, frozen);
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, canary_word());

    /* A caller that holds only the shorter accepted layout is stamped at
     * its own size. */
    memset(&cfg, CANARY, sizeof(cfg));
    moq_reject_subscribe_cfg_init_sized(&cfg, floor);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, floor);
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, canary_word());

    memset(&cfg, CANARY, sizeof(cfg));
    (moq_reject_subscribe_cfg_init)(&cfg);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, frozen);
    MOQ_TEST_CHECK_EQ_U64(cfg.growth_probe_tail, canary_word());
}

int cfg_init_growth_grown_caller(void)
{
    grown_caller_done_subscribe();
    grown_caller_reject_subscribe();
    return failures;
}
