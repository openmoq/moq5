/*
 * Scaffold smoke: the relay-core types compile, link against moq::core,
 * and honor the handle/config contracts. No data structures exist yet —
 * this proves the skeleton, not behavior.
 */

#include <moq/relay/placement.h>
#include <moq/relay/types.h>

#include <stddef.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* Trace records must stay fixed-size and scalar-only (borrow discipline); freeze the
 * current size so growth is a conscious ABI decision, not drift. */
_Static_assert(sizeof(moqr_trace_rec_t) ==
                   sizeof(uint32_t) * 2 + sizeof(uint64_t) +
                       sizeof(moqr_epochs_t) + sizeof(uint64_t) * 4,
               "moqr_trace_rec_t must be packed scalars, no padding surprises");

static moqr_owner_t
place_everything_on_shard_zero(const moqr_place_key_t *key,
                               const moqr_placement_state_t *state)
{
    (void)key;
    (void)state;
    return (moqr_owner_t){ .node = 0, .shard = 0 };
}

int
main(void)
{
    int failures = 0;

    /* Result strings exist for every code. */
    MOQ_TEST_CHECK(strcmp(moqr_strerror(MOQR_OK), "ok") == 0);
    MOQ_TEST_CHECK(strcmp(moqr_strerror(MOQR_ERR_CAPACITY),
                          "capacity refused") == 0);
    MOQ_TEST_CHECK(strcmp(moqr_strerror(-1234), "unknown error") == 0);

    /* Every defined result code has a real string. */
    const moqr_result_t known[] = {
        MOQR_OK, MOQR_DONE, MOQR_ERR_NOMEM, MOQR_ERR_INVAL,
        MOQR_ERR_WOULD_BLOCK, MOQR_ERR_STALE_HANDLE, MOQR_ERR_WRONG_STATE,
        MOQR_ERR_CAPACITY, MOQR_ERR_UNSUPPORTED, MOQR_ERR_TOO_OLD,
        MOQR_ERR_INTERNAL,
    };
    for (size_t i = 0; i < sizeof(known) / sizeof(known[0]); i++) {
        MOQ_TEST_CHECK(strcmp(moqr_strerror(known[i]), "unknown error") != 0);
    }

    /* Zero-init sentinels are invalid; a packed live handle is valid and
     * pool-tag checked. */
    moqr_track_t no_track = MOQR_TRACK_INVALID;
    MOQ_TEST_CHECK(!moqr_track_is_valid(no_track));

    uint64_t raw = moq_handle_pack(MOQR_HANDLE_POOL_TRACK,
                                   /*shard_tag=*/1, /*generation=*/3,
                                   /*slot=*/7);
    MOQ_TEST_CHECK(raw != 0);
    moqr_track_t track = { raw };
    MOQ_TEST_CHECK(moqr_track_is_valid(track));
    MOQ_TEST_CHECK(moqr_track_eq(track, track));
    MOQ_TEST_CHECK(!moqr_track_eq(track, no_track));

    /* Wrong pool tag must not validate as a track. */
    moqr_cursor_t cursor = { moq_handle_pack(MOQR_HANDLE_POOL_CURSOR, 1, 3, 7) };
    MOQ_TEST_CHECK(moqr_cursor_is_valid(cursor));
    moqr_track_t cross = { cursor._opaque };
    MOQ_TEST_CHECK(!moqr_track_is_valid(cross));

    /* Even (freed) generation must not validate. moq_handle_pack refuses to
     * mint even generations (returns 0), so the stale value is constructed
     * by hand — flip the generation's low bit on a live handle — and the
     * layout assumption is verified through the public accessor before the
     * validity assertion. */
    moqr_binding_t live = { moq_handle_pack(MOQR_HANDLE_POOL_BINDING, 1, 3, 9) };
    MOQ_TEST_CHECK(moqr_binding_is_valid(live));
    MOQ_TEST_CHECK(moq_handle_pack(MOQR_HANDLE_POOL_BINDING, 1, 2, 9) == 0);
    moqr_binding_t stale = { live._opaque ^ (UINT64_C(1) << 16) };
    MOQ_TEST_CHECK_EQ_U64(moq_handle_generation(stale._opaque), 2);
    MOQ_TEST_CHECK(!moqr_binding_is_valid(stale));

    /* Zero shard tag must not validate (tag field, bits 44..59). */
    moqr_binding_t untagged = { live._opaque & ~(UINT64_C(0xFFFF) << 44) };
    MOQ_TEST_CHECK_EQ_U64(moq_handle_session_tag(untagged._opaque), 0);
    MOQ_TEST_CHECK(!moqr_binding_is_valid(untagged));

    /* Config init stamps min(size, sizeof) and defaults shard_count=1. */
    moqr_core_cfg_t cfg;
    memset(&cfg, 0xAB, sizeof(cfg));
    moqr_core_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default());
    MOQ_TEST_CHECK_EQ_U64(cfg.struct_size, sizeof(moqr_core_cfg_t));
    MOQ_TEST_CHECK(cfg.alloc == moq_alloc_default());
    MOQ_TEST_CHECK_EQ_U64(cfg.shard_count, 1);
    MOQ_TEST_CHECK_EQ_U64(cfg.max_tracks, 0);
    MOQ_TEST_CHECK_EQ_U64(cfg.log_default.max_bytes, 0);

    /* Prefix-size ABI safety: initializing through a smaller declared size
     * must stamp struct_size to that size, write a defaulted field only when
     * the field fits entirely, and never touch a byte past cfg_size
     * (canary-checked). */
    union {
        moqr_core_cfg_t cfg;
        unsigned char   raw[sizeof(moqr_core_cfg_t)];
    } u;

    size_t no_alloc = offsetof(moqr_core_cfg_t, alloc);
    memset(u.raw, 0x5A, sizeof(u.raw));
    moqr_core_cfg_init_sized(&u.cfg, no_alloc, moq_alloc_default());
    MOQ_TEST_CHECK_EQ_U64(u.cfg.struct_size, no_alloc);
    int canary_ok = 1;
    for (size_t i = no_alloc; i < sizeof(u.raw); i++) {
        if (u.raw[i] != 0x5A) {
            canary_ok = 0;
            break;
        }
    }
    MOQ_TEST_CHECK(canary_ok); /* alloc (and everything after) untouched */

    size_t no_shard_count = offsetof(moqr_core_cfg_t, shard_count);
    memset(u.raw, 0x5A, sizeof(u.raw));
    moqr_core_cfg_init_sized(&u.cfg, no_shard_count, moq_alloc_default());
    MOQ_TEST_CHECK_EQ_U64(u.cfg.struct_size, no_shard_count);
    MOQ_TEST_CHECK(u.cfg.alloc == moq_alloc_default()); /* alloc fits */
    canary_ok = 1;
    for (size_t i = no_shard_count; i < sizeof(u.raw); i++) {
        if (u.raw[i] != 0x5A) {
            canary_ok = 0;
            break;
        }
    }
    MOQ_TEST_CHECK(canary_ok); /* shard_count default not written past size */

    /* Epoch triple initializes to generation zero. */
    moqr_epochs_t epochs = MOQR_EPOCHS_INIT;
    MOQ_TEST_CHECK_EQ_U64(epochs.node_epoch, 0);
    MOQ_TEST_CHECK_EQ_U64(epochs.shard_epoch, 0);
    MOQ_TEST_CHECK_EQ_U64(epochs.route_epoch, 0);

    /* The placement seam is usable as declared: a pure function over
     * (key, state) with no context pointer to smuggle live state through. */
    moqr_place_fn place = place_everything_on_shard_zero;
    moqr_placement_state_t state = {
        .struct_size = sizeof(state),
        .epochs = MOQR_EPOCHS_INIT,
        .shard_count = 1,
        .node_count = 1,
        .params = NULL,
        .params_size = 0,
    };
    moqr_place_key_t key = {
        .full_track_name = MOQ_BYTES_LITERAL("demo/track"),
        .hash = 0x1234u,
    };
    moqr_owner_t owner = place(&key, &state);
    MOQ_TEST_CHECK_EQ_U64(owner.node, 0);
    MOQ_TEST_CHECK_EQ_U64(owner.shard, 0);

    MOQ_TEST_PASS("relay_scaffold");
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
