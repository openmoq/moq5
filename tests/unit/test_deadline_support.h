#ifndef TEST_DEADLINE_SUPPORT_H
#define TEST_DEADLINE_SUPPORT_H

/*
 * White-box test helpers for internal deadline/timer fields.
 * Not part of the public API. Only linked into unit tests.
 */

#include "../../core/src/session/session_internal.h"

#ifdef __GNUC__
#define TDS_UNUSED __attribute__((unused))
#else
#define TDS_UNUSED
#endif

TDS_UNUSED static uint64_t
test_get_sub_delivery_timeout(moq_session_t *s, moq_subscription_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBSCRIPTION) return 0;
    if (tag != s->session_tag) return 0;
    if (slot >= s->sub_cap) return 0;
    if (s->subs[slot].generation != gen) return 0;
    if (s->subs[slot].state == MOQ_SUB_FREE) return 0;
    return s->subs[(size_t)slot].delivery_timeout_us;
}

TDS_UNUSED static uint64_t
test_get_sg_deadline(moq_session_t *s, moq_subgroup_handle_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBGROUP) return UINT64_MAX;
    if (tag != s->session_tag) return UINT64_MAX;
    if (slot >= s->sg_cap) return UINT64_MAX;
    if (s->subgroups[slot].generation != gen) return UINT64_MAX;
    return s->subgroups[slot].delivery_deadline_us;
}

TDS_UNUSED static void
test_set_sg_deadline(moq_session_t *s, moq_subgroup_handle_t h,
                      uint64_t deadline)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBGROUP) return;
    if (tag != s->session_tag) return;
    if (slot >= s->sg_cap) return;
    if (s->subgroups[slot].generation != gen) return;
    s->subgroups[slot].delivery_deadline_us = deadline;
    /* Recompute session min cache. */
    uint64_t d = UINT64_MAX;
    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state != MOQ_SG_FREE &&
            s->subgroups[i].delivery_deadline_us < d)
            d = s->subgroups[i].delivery_deadline_us;
    s->subgroup_deadline_us = d;
}

TDS_UNUSED static uint64_t
test_get_session_sg_deadline(moq_session_t *s)
{
    return s->subgroup_deadline_us;
}

#endif /* TEST_DEADLINE_SUPPORT_H */
