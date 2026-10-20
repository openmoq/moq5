/* Model an old application at the C ABI, separately compiled from the current
 * receiver implementation. The allocation remains exactly the old sizeof. */
#include "receiver_legacy_ts_caller.h"
#include "test_support.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

int moq_test_receiver_legacy_ts_caller(moq_namespace_t namespace_,
                                       void (*probe)(const void *))
{
    int failures = 0;
    MOQ_TEST_CHECK(probe != NULL);
    if (!probe) return failures;
    const size_t end = offsetof(ts_receiver_cfg_t, mpeg2ts_join_psi) + sizeof(bool);
    for (unsigned round = 0; round < 2; round++) {
        ts_receiver_cfg_t *old = malloc(sizeof(*old));
        MOQ_TEST_CHECK(old != NULL);
        if (!old) return failures;
        memset(old, 0xa5, sizeof(*old));
        moq_media_receiver_cfg_init_live_sized((moq_media_receiver_cfg_t *)old,
                                               sizeof(*old));
        MOQ_TEST_CHECK(old->struct_size == sizeof(*old));
        old->namespace_ = namespace_;
        old->auto_subscribe = true;
        /* Padding must be ignored whether it resembles a profile or garbage. */
        memset((unsigned char *)old + end, round ? 0x01 : 0xa5, sizeof(*old) - end);
        probe(old);
        free(old);
    }
    return failures;
}
