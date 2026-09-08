#include <moq/relay/relay.h>
#include <moq/relay/capacity.h>
#ifdef TEST_RUNTIME
#include <moq/relay/moqr_obs.h>
#endif
#include <stdio.h>
#include <string.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "consumer:%d: %s\n", __LINE__, #expr); return 1; \
} } while (0)

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_core_relay_cfg_t cfg;
    moqr_core_t *a = NULL, *b = NULL;
    moqr_binding_t binding;
    moqr_core_stats_t stats;
    uint64_t wire = UINT64_MAX;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), alloc);
    cfg.max_bindings = 4;
    CHECK(moqr_core_create(&cfg, &a) == MOQR_OK);
    CHECK(moqr_core_create(&cfg, &b) == MOQR_OK);
    CHECK(moqr_core_binding_open(a, 41, &binding) == MOQR_OK);
    CHECK(moqr_core_get_stats_sized(a, &stats, sizeof(stats)) == MOQR_OK);
    CHECK(stats.bindings == 1);
    CHECK(moqr_core_get_stats_sized(b, &stats, sizeof(stats)) == MOQR_OK);
    CHECK(stats.bindings == 0);
    CHECK(moqr_pd_encode(MOQ_VERSION_DRAFT_18, MOQR_PD_TRACK_ENDED, &wire) == MOQR_OK);
    CHECK(moqr_pd_decode(MOQ_VERSION_DRAFT_18, wire) == MOQR_PD_TRACK_ENDED);
    CHECK(moqr_core_binding_close(a, binding, 0) == MOQR_OK);
    moqr_core_destroy(a);
    CHECK(moqr_core_tick(b, 1) == MOQR_OK);
    moqr_core_destroy(b);
#ifdef TEST_RUNTIME
    {
        moqr_shards_cfg_t scfg;
        moqr_shards_t *runtime = NULL;
        moqr_shards_stats_t shard;
        moqr_bind_stats_t bind;
        moqr_obs_labels_t labels = {0, "", ""};
        size_t written = 0;
        char text[32768];
        moqr_shards_cfg_init_sized(&scfg, sizeof(scfg), alloc);
        scfg.shards = 1;
        CHECK(moqr_shards_create(&scfg, &runtime) == MOQR_OK);
        CHECK(moqr_shards_step(runtime, 0) == MOQR_OK);
        CHECK(moqr_shards_get_stats_sized(runtime, 0, &shard, sizeof(shard)) == MOQR_OK);
        CHECK(moqr_core_get_stats_sized(moqr_shards_core(runtime, 0), &stats, sizeof(stats)) == MOQR_OK);
        CHECK(moqr_bind_get_stats_sized(moqr_shards_bind(runtime, 0), &bind, sizeof(bind)) == MOQR_OK);
        CHECK(moqr_metrics_write_ex(&stats, &bind, &labels,
              MOQR_OBS_FMT_OPENMETRICS_100, text, sizeof(text), &written) == MOQR_OK);
        CHECK(written >= 6 && strcmp(text + written - 6, "# EOF\n") == 0);
        moqr_shards_destroy(runtime);
    }
#endif
    return 0;
}
