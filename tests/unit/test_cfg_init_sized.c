/*
 * Config initializers that have a sized form beside the exported symbol.
 *
 * The exported symbol clears and stamps one frozen layout and writes nothing
 * past it, so a caller that holds only that layout is never overwritten. The
 * same-name macro and the sized function reach exactly what the caller
 * supplied: never more than this library's struct, and nothing at all when
 * the caller's size cannot hold struct_size.
 */
#include <moq/session.h>
#include <moq/publisher.h>
#include <moq/subscriber.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "test_support.h"

static int failures = 0;

#define MAX_CFG 512u
#define GUARD   16u
#define CANARY  0xA5u

/* X(type, initializer, smallest accepted stamp, symbol's stamp, nonzero defaults) */
#define INITIALIZERS(X)                                                        \
    X(moq_accept_subscribe_cfg_t, moq_accept_subscribe_cfg_init,               \
      MOQ_ACCEPT_SUBSCRIBE_CFG_V0_SIZE, MOQ_ACCEPT_SUBSCRIBE_CFG_V0_SIZE, false) \
    X(moq_reject_subscribe_cfg_t, moq_reject_subscribe_cfg_init,               \
      MOQ_REJECT_SUBSCRIBE_CFG_V0_SIZE,                                        \
      MOQ_REJECT_SUBSCRIBE_CFG_SIZE_THROUGH_REDIRECT, false)                   \
    X(moq_done_subscribe_cfg_t, moq_done_subscribe_cfg_init,                   \
      MOQ_DONE_SUBSCRIBE_CFG_V0_SIZE, MOQ_DONE_SUBSCRIBE_CFG_V0_SIZE, false)   \
    X(moq_accept_fetch_cfg_t, moq_accept_fetch_cfg_init,                       \
      MOQ_ACCEPT_FETCH_CFG_V0_SIZE,                                            \
      MOQ_ACCEPT_FETCH_CFG_SIZE_THROUGH_TRACK_PROPERTIES, false)               \
    X(moq_reject_fetch_cfg_t, moq_reject_fetch_cfg_init,                       \
      MOQ_REJECT_FETCH_CFG_V0_SIZE,                                            \
      MOQ_REJECT_FETCH_CFG_SIZE_THROUGH_REDIRECT, false)                       \
    X(moq_fetch_object_cfg_t, moq_fetch_object_cfg_init,                       \
      MOQ_FETCH_OBJECT_CFG_V0_SIZE, MOQ_FETCH_OBJECT_CFG_V0_SIZE, false)       \
    X(moq_fetch_update_cfg_t, moq_fetch_update_cfg_init,                       \
      MOQ_FETCH_UPDATE_CFG_V0_SIZE, MOQ_FETCH_UPDATE_CFG_V0_SIZE, false)       \
    X(moq_request_goaway_cfg_t, moq_request_goaway_cfg_init,                   \
      MOQ_REQUEST_GOAWAY_CFG_V0_SIZE, MOQ_REQUEST_GOAWAY_CFG_V0_SIZE, false)   \
    X(moq_reject_publish_cfg_t, moq_reject_publish_cfg_init,                   \
      MOQ_REJECT_PUBLISH_CFG_V0_SIZE, MOQ_REJECT_PUBLISH_CFG_V0_SIZE, false)   \
    X(moq_accept_namespace_cfg_t, moq_accept_namespace_cfg_init,               \
      MOQ_ACCEPT_NAMESPACE_CFG_V0_SIZE, MOQ_ACCEPT_NAMESPACE_CFG_V0_SIZE, false) \
    X(moq_reject_namespace_cfg_t, moq_reject_namespace_cfg_init,               \
      MOQ_REJECT_NAMESPACE_CFG_V0_SIZE,                                        \
      MOQ_REJECT_NAMESPACE_CFG_SIZE_THROUGH_REDIRECT, false)                   \
    X(moq_cancel_namespace_cfg_t, moq_cancel_namespace_cfg_init,               \
      MOQ_CANCEL_NAMESPACE_CFG_V0_SIZE, MOQ_CANCEL_NAMESPACE_CFG_V0_SIZE, false) \
    X(moq_reject_track_status_cfg_t, moq_reject_track_status_cfg_init,         \
      MOQ_REJECT_TRACK_STATUS_CFG_V0_SIZE,                                     \
      MOQ_REJECT_TRACK_STATUS_CFG_SIZE_THROUGH_REDIRECT, false)                \
    X(moq_subscribe_namespace_cfg_t, moq_subscribe_namespace_cfg_init,         \
      MOQ_SUBSCRIBE_NAMESPACE_CFG_V0_SIZE,                                     \
      MOQ_SUBSCRIBE_NAMESPACE_CFG_SIZE_THROUGH_AUTH_TOKEN_COUNT, false)        \
    X(moq_accept_ns_sub_cfg_t, moq_accept_ns_sub_cfg_init,                     \
      MOQ_ACCEPT_NS_SUB_CFG_V0_SIZE, MOQ_ACCEPT_NS_SUB_CFG_V0_SIZE, false)     \
    X(moq_reject_ns_sub_cfg_t, moq_reject_ns_sub_cfg_init,                     \
      MOQ_REJECT_NS_SUB_CFG_V0_SIZE,                                           \
      MOQ_REJECT_NS_SUB_CFG_SIZE_THROUGH_REDIRECT, false)                      \
    X(moq_accept_subscribe_tracks_cfg_t, moq_accept_subscribe_tracks_cfg_init, \
      MOQ_ACCEPT_SUBSCRIBE_TRACKS_CFG_V0_SIZE,                                 \
      MOQ_ACCEPT_SUBSCRIBE_TRACKS_CFG_V0_SIZE, false)                          \
    X(moq_reject_subscribe_tracks_cfg_t, moq_reject_subscribe_tracks_cfg_init, \
      MOQ_REJECT_SUBSCRIBE_TRACKS_CFG_V0_SIZE,                                 \
      MOQ_REJECT_SUBSCRIBE_TRACKS_CFG_V0_SIZE, false)                          \
    X(moq_finish_publish_cfg_t, moq_finish_publish_cfg_init,                   \
      MOQ_FINISH_PUBLISH_CFG_V0_SIZE, MOQ_FINISH_PUBLISH_CFG_V0_SIZE, false)   \
    X(moq_begin_object_cfg_t, moq_begin_object_cfg_init,                       \
      MOQ_BEGIN_OBJECT_CFG_V0_SIZE, MOQ_BEGIN_OBJECT_CFG_V0_SIZE, false)       \
    X(moq_pub_publish_cfg_t, moq_pub_publish_cfg_init,                         \
      MOQ_PUB_PUBLISH_CFG_V0_SIZE, MOQ_PUB_PUBLISH_CFG_V0_SIZE, true)          \
    X(moq_pub_retained_group_cfg_t, moq_pub_retained_group_cfg_init,           \
      MOQ_PUB_RETAINED_GROUP_CFG_V0_SIZE, MOQ_PUB_RETAINED_GROUP_CFG_V0_SIZE,  \
      false)                                                                   \
    X(moq_sub_callbacks_t, moq_sub_callbacks_init,                             \
      MOQ_SUB_CALLBACKS_V0_SIZE, MOQ_SUB_CALLBACKS_V0_SIZE, false)             \
    X(moq_sub_status_cfg_t, moq_sub_status_cfg_init,                           \
      MOQ_SUB_STATUS_CFG_V0_SIZE,                                              \
      MOQ_SUB_STATUS_CFG_SIZE_THROUGH_AUTH_TOKEN_COUNT, false)                 \
    X(moq_sub_fetch_cfg_t, moq_sub_fetch_cfg_init,                             \
      MOQ_SUB_FETCH_CFG_V0_SIZE,                                               \
      MOQ_SUB_FETCH_CFG_SIZE_THROUGH_AUTH_TOKEN_COUNT, false)

/* The parenthesized name is the exported symbol; the bare call is the macro. */
#define THUNKS(type, fn, floor, extent, defaults)                              \
    _Static_assert(sizeof(type) <= MAX_CFG, #type " outgrew the test storage"); \
    static void symbol_##fn(void *p) { (fn)((type *)p); }                      \
    static void macro_##fn(void *p) { fn((type *)p); }                         \
    static void sized_##fn(void *p, size_t n) { fn##_sized((type *)p, n); }
INITIALIZERS(THUNKS)

typedef struct {
    const char *name;
    size_t size;      /* sizeof the struct in this build */
    size_t floor;     /* the smallest stamp the consuming call accepts */
    size_t extent;    /* what the exported symbol stamps */
    bool defaults;    /* the initializer sets a nonzero default */
    void (*symbol)(void *cfg);
    void (*macro)(void *cfg);
    void (*sized)(void *cfg, size_t cfg_size);
} row_t;

#define ROW(type, fn, floor, extent, defaults)                                 \
    { #fn, sizeof(type), (floor), (extent), (defaults),                        \
      symbol_##fn, macro_##fn, sized_##fn },
static const row_t rows[] = { INITIALIZERS(ROW) };

static union {
    uint8_t bytes[MAX_CFG + GUARD];
    uint64_t align_u64;
    void *align_ptr;
    void (*align_fn)(void);
} box;

static void poison(void)
{
    memset(box.bytes, CANARY, sizeof(box.bytes));
}

static size_t stamp(void)
{
    uint32_t v;
    memcpy(&v, box.bytes, sizeof(v));
    return (size_t)v;
}

/* Every byte from `from` to the end of the storage still holds the canary. */
static bool untouched_from(size_t from)
{
    for (size_t i = from; i < sizeof(box.bytes); i++)
        if (box.bytes[i] != CANARY) return false;
    return true;
}

/* Every byte after struct_size, up to `to`, was cleared. */
static bool cleared_to(size_t to)
{
    for (size_t i = sizeof(uint32_t); i < to; i++)
        if (box.bytes[i] != 0) return false;
    return true;
}

static void check_row(const row_t *r)
{
    MOQ_TEST_CHECK(r->floor >= sizeof(uint32_t));
    MOQ_TEST_CHECK(r->floor <= r->extent);
    MOQ_TEST_CHECK(r->extent <= r->size);

    /* The exported symbol: its frozen layout, and nothing past it. */
    poison();
    r->symbol(box.bytes);
    MOQ_TEST_CHECK_EQ_SIZE(stamp(), r->extent);
    MOQ_TEST_CHECK(untouched_from(r->extent));
    if (!r->defaults) MOQ_TEST_CHECK(cleared_to(r->extent));

    /* The same-name macro: the whole struct, and nothing past it. */
    poison();
    r->macro(box.bytes);
    MOQ_TEST_CHECK_EQ_SIZE(stamp(), r->size);
    MOQ_TEST_CHECK(untouched_from(r->size));
    if (!r->defaults) MOQ_TEST_CHECK(cleared_to(r->size));

    /* A caller's struct larger than this library's is clamped to ours. */
    poison();
    r->sized(box.bytes, r->size + GUARD);
    MOQ_TEST_CHECK_EQ_SIZE(stamp(), r->size);
    MOQ_TEST_CHECK(untouched_from(r->size));

    /* A caller's smaller struct is cleared and stamped at its own size. */
    poison();
    r->sized(box.bytes, r->floor);
    MOQ_TEST_CHECK_EQ_SIZE(stamp(), r->floor);
    MOQ_TEST_CHECK(untouched_from(r->floor));

    poison();
    r->sized(box.bytes, sizeof(uint32_t));
    MOQ_TEST_CHECK_EQ_SIZE(stamp(), sizeof(uint32_t));
    MOQ_TEST_CHECK(untouched_from(sizeof(uint32_t)));

    /* Too small to hold struct_size: nothing is written. */
    poison();
    r->sized(box.bytes, sizeof(uint32_t) - 1);
    MOQ_TEST_CHECK(untouched_from(0));
    poison();
    r->sized(box.bytes, 0);
    MOQ_TEST_CHECK(untouched_from(0));

    /* A null pointer is ignored by every form. */
    r->symbol(NULL);
    r->macro(NULL);
    r->sized(NULL, r->size);
}

/* PUBLISH forwarding defaults to on, but only in a struct that holds it. */
static void check_publish_forward_default(void)
{
    moq_pub_publish_cfg_t cfg;
    uint8_t byte;
    const size_t at = offsetof(moq_pub_publish_cfg_t, forward);

    moq_pub_publish_cfg_init(&cfg);
    MOQ_TEST_CHECK(cfg.has_forward && cfg.forward);

    (moq_pub_publish_cfg_init)(&cfg);
    MOQ_TEST_CHECK(cfg.has_forward && cfg.forward);

    memset(&cfg, CANARY, sizeof(cfg));
    moq_pub_publish_cfg_init_sized(&cfg, at);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)cfg.struct_size, at);
    memcpy(&byte, (const uint8_t *)&cfg + at, sizeof(byte));
    MOQ_TEST_CHECK(byte == CANARY);
}

int main(void)
{
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        int before = failures;
        check_row(&rows[i]);
        if (failures != before)
            fprintf(stderr, "FAIL: initializer %s\n", rows[i].name);
    }
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(rows) / sizeof(rows[0]), (size_t)25);
    check_publish_forward_default();
    MOQ_TEST_PASS("cfg_init_sized");
    return failures ? 1 : 0;
}
