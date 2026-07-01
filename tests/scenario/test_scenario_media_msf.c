/*
 * Seeded MSF/CMSF catalog conformance runner (scenario style).
 *
 * Generates valid MSF/CMSF catalog models -- initDataList/initRef, eventtimeline
 * depends/mimeType/eventType, contentProtections + contentProtectionRefIDs, max
 * SAP fields -- and checks the producer/consumer round trip via canonical
 * byte-equality: encode -> parse -> encode yields identical JSON. Uses the
 * MOQ_SCENARIO_* env knobs and the canonical "Replay:" output. Slice 2 of the
 * seeded media-conformance extension: generate-only (no byte mutation yet).
 *
 * Light ctest default (200 seeds x 16 cases). Heavier local runs:
 *   MOQ_SCENARIO_SEEDS=100000 ./build/tests/test_scenario_media_msf
 */

#include "../support/media_fuzz.h"

#include <stdint.h>
#include <stdio.h>

int main(int argc, char **argv)
{
    const char *argv0 = (argc > 0 && argv[0]) ? argv[0]
        : "./build/tests/test_scenario_media_msf";

    moq_fuzz_cfg_t cfg;
    moq_fuzz_cfg_from_env(&cfg, /*default_seeds=*/200, /*default_steps=*/16);

    int failures = 0;
    for (uint64_t i = 0; i < cfg.seeds; i++)
        failures += moq_fuzz_msf_run_seed(cfg.seed_start + i, cfg.steps,
                                          cfg.mutate_permille, argv0);

    if (failures == 0)
        fprintf(stderr, "PASS: test_scenario_media_msf (%llu seeds)\n",
                (unsigned long long)cfg.seeds);
    else
        fprintf(stderr, "FAIL: test_scenario_media_msf (%d failures in %llu seeds)\n",
                failures, (unsigned long long)cfg.seeds);

    return failures != 0;
}
