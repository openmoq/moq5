#ifndef TEST_PUBLISHER_TRACK_LAYOUT_ASSERTS_H
#define TEST_PUBLISHER_TRACK_LAYOUT_ASSERTS_H

#include <moq/publisher.h>
#include "publisher_track_old_layout.h"

_Static_assert(offsetof(moq_pub_track_cfg_t, monotonic_groups) == PUB_TRACK_DIM(64, 40, 40),
               "frozen current monotonic offset");
_Static_assert(offsetof(moq_pub_track_cfg_t, monotonic_groups) >=
               sizeof(test_pub_track_pre_monotonic_t), "no historical allocation reuse");
_Static_assert(sizeof(((moq_pub_track_cfg_t *)0)->monotonic_groups) == 1,
               "frozen monotonic width");
_Static_assert(sizeof(((moq_pub_track_cfg_t *)0)->_reserved_track_tail) == 7,
               "frozen explicit reservation");
_Static_assert(offsetof(moq_pub_track_cfg_t, _reserved_track_tail) == PUB_TRACK_DIM(57, 33, 33),
               "frozen reserved offset");
_Static_assert(offsetof(moq_pub_track_cfg_t, monotonic_groups) ==
               offsetof(moq_pub_track_cfg_t, _reserved_track_tail) + 7,
               "no drift after explicit reservation");
/* The layout through monotonic_groups had sizeof 72 / 44 / 48; the Track
 * Properties fields start at or past it, behind a never-read reservation. */
_Static_assert(offsetof(moq_pub_track_cfg_t, _reserved_track_tail2) ==
               offsetof(moq_pub_track_cfg_t, monotonic_groups) + 1,
               "second reservation follows monotonic_groups");
_Static_assert(offsetof(moq_pub_track_cfg_t, track_properties) == PUB_TRACK_DIM(72, 48, 48),
               "track_properties offset");
_Static_assert(offsetof(moq_pub_track_cfg_t, track_properties) >= PUB_TRACK_DIM(72, 44, 48),
               "track_properties starts past the monotonic-era sizeof");
_Static_assert(offsetof(moq_pub_track_cfg_t, dynamic_groups) == PUB_TRACK_DIM(88, 56, 56),
               "dynamic_groups offset");
_Static_assert(offsetof(moq_pub_track_cfg_t, namespace_auth_tokens) ==
               PUB_TRACK_DIM(96, 60, 64), "complete pre-auth allocation");
_Static_assert(offsetof(moq_pub_track_cfg_t, namespace_auth_token_count) ==
               PUB_TRACK_DIM(104, 64, 68), "namespace auth count offset");
_Static_assert(sizeof(moq_pub_track_cfg_t) == PUB_TRACK_DIM(112, 68, 72), "current sizeof");
_Static_assert(_Alignof(moq_pub_track_cfg_t) == PUB_TRACK_DIM(8, 4, 8), "current alignment");
_Static_assert(offsetof(moq_pub_track_cfg_t, max_retained_bytes) ==
               sizeof(test_pub_track_original_t), "original prefix includes old tail padding");

#define PUB_TRACK_SAME(f) \
    _Static_assert(offsetof(moq_pub_track_cfg_t, f) == \
                   offsetof(test_pub_track_pre_monotonic_t, f), "old offset: " #f); \
    _Static_assert(sizeof(((moq_pub_track_cfg_t *)0)->f) == \
                   sizeof(((test_pub_track_pre_monotonic_t *)0)->f), "old width: " #f)
PUB_TRACK_SAME(struct_size);
PUB_TRACK_SAME(track_namespace);
PUB_TRACK_SAME(track_name);
PUB_TRACK_SAME(advertise_namespace);
PUB_TRACK_SAME(publisher_priority);
PUB_TRACK_SAME(max_retained_bytes);
PUB_TRACK_SAME(has_publisher_priority);
#undef PUB_TRACK_SAME

#endif
