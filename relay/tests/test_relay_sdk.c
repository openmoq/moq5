#include <moq/relay/relay.h>
#ifdef TEST_RUNTIME
#include <moq/relay/moqr_shards.h>
#endif
#include <stdio.h>
#include <string.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "sdk:%d: %s\n", __LINE__, #expr); return 1; \
} } while (0)

/* Check every rejected prefix and the complete output plus caller-owned tail. */
#define CHECK_STATS(type, getter, object) do { \
    struct { type value; unsigned char tail[23]; } out; \
    unsigned char before[sizeof(out)]; \
    memset(&out, 0xa5, sizeof(out)); \
    memcpy(before, &out, sizeof(out)); \
    for (size_t n = 0; n < sizeof(type); ++n) { \
        CHECK(getter(object, &out.value, n) == MOQR_ERR_INVAL); \
        CHECK(memcmp(before, &out, sizeof(out)) == 0); \
    } \
    CHECK(getter(NULL, &out.value, sizeof(out)) == MOQR_ERR_INVAL); \
    CHECK(memcmp(before, &out, sizeof(out)) == 0); \
    CHECK(getter(object, NULL, sizeof(out)) == MOQR_ERR_INVAL); \
    CHECK(getter(object, &out.value, sizeof(out)) == MOQR_OK); \
    CHECK(memcmp(before + sizeof(type), (unsigned char *)&out + sizeof(type), \
                 sizeof(out) - sizeof(type)) == 0); \
    type exact; \
    memset(&exact, 0x5a, sizeof(exact)); \
    CHECK(getter(object, &exact, sizeof(exact)) == MOQR_OK); \
    CHECK(memcmp(&exact, &out.value, sizeof(exact)) == 0); \
} while (0)

#ifdef TEST_RUNTIME
static moqr_result_t shard_stats(moqr_shards_t *s, moqr_shards_stats_t *out,
                                 size_t size)
{
    return moqr_shards_get_stats_sized(s, 0, out, size);
}
#endif

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_core_relay_cfg_t cfg;
    moqr_core_t *core = NULL;
    moqr_log_cfg_t lcfg;
    moqr_log_t *log = NULL;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), alloc);
    CHECK(moqr_core_create(&cfg, &core) == MOQR_OK);
    CHECK_STATS(moqr_core_stats_t, moqr_core_get_stats_sized, core);
    moqr_log_cfg_init_sized(&lcfg, sizeof(lcfg), alloc);
    CHECK(moqr_log_create(&lcfg, &log) == MOQR_OK);
    CHECK_STATS(moqr_log_stats_t, moqr_log_get_stats_sized, log);
    moqr_log_destroy(log);
    moqr_core_destroy(core);
#ifdef TEST_RUNTIME
    moqr_shards_cfg_t scfg;
    moqr_shards_t *a = NULL, *b = NULL;
    moqr_binding_t binding;
    moqr_core_stats_t ac, bc;
    moqr_shards_cfg_init_sized(&scfg, sizeof(scfg), alloc);
    scfg.shards = 2;
    CHECK(moqr_shards_create(&scfg, &a) == MOQR_OK);
    CHECK(moqr_shards_create(&scfg, &b) == MOQR_OK);
    CHECK_STATS(moqr_shards_stats_t, shard_stats, a);
    CHECK_STATS(moqr_bind_stats_t, moqr_bind_get_stats_sized,
                moqr_shards_bind(a, 0));
    moqr_shards_stats_t rejected, before;
    memset(&rejected, 0xa5, sizeof(rejected));
    memcpy(&before, &rejected, sizeof(before));
    CHECK(moqr_shards_get_stats_sized(a, 2, &rejected, sizeof(rejected)) == MOQR_ERR_INVAL);
    CHECK(memcmp(&before, &rejected, sizeof(before)) == 0);
    CHECK(moqr_shards_core(a, 2) == NULL);
    CHECK(moqr_shards_bind(a, 2) == NULL);
    CHECK(moqr_shards_trace(a, 2) == NULL);
    CHECK(moqr_core_get_stats_sized(moqr_shards_core(a, 0), &ac, sizeof(ac)) == MOQR_OK);
    CHECK(moqr_core_get_stats_sized(moqr_shards_core(b, 0), &bc, sizeof(bc)) == MOQR_OK);
    CHECK(ac.bindings == bc.bindings);
    uint64_t base_bindings = bc.bindings;
    CHECK(moqr_core_binding_open(moqr_shards_core(a, 0), 91, &binding) == MOQR_OK);
    CHECK(moqr_core_get_stats_sized(moqr_shards_core(a, 0), &ac, sizeof(ac)) == MOQR_OK);
    CHECK(moqr_core_get_stats_sized(moqr_shards_core(b, 0), &bc, sizeof(bc)) == MOQR_OK);
    CHECK(ac.bindings == base_bindings + 1 && bc.bindings == base_bindings);
    CHECK(moqr_core_binding_close(moqr_shards_core(a, 0), binding, 0) == MOQR_OK);
    moqr_shards_destroy(a);
    CHECK(moqr_shards_step(b, 1) == MOQR_OK);
    CHECK(moqr_core_get_stats_sized(moqr_shards_core(b, 0), &bc, sizeof(bc)) == MOQR_OK);
    CHECK(bc.bindings == base_bindings);
    moqr_shards_destroy(b);
#endif
    return 0;
}
