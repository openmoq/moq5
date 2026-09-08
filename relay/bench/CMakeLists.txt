# Relay-owned deterministic and owner-boundary benchmarks.

function(moqr_bench_warnings target)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(${target} PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
endfunction()

add_executable(bench_relay_core bench_relay_core.c)
target_link_libraries(bench_relay_core PRIVATE moq::relay-core)
moqr_bench_warnings(bench_relay_core)

add_executable(bench_relay_shards bench_relay_shards.c)
target_link_libraries(bench_relay_shards PRIVATE moq-relay-inspect)
moqr_bench_warnings(bench_relay_shards)

add_executable(bench_relay_sched bench_relay_sched.c)
target_link_libraries(bench_relay_sched PRIVATE moq::relay-core)
moqr_bench_warnings(bench_relay_sched)

add_executable(bench_relay_saturation bench_relay_saturation.c)
target_link_libraries(bench_relay_saturation PRIVATE
    moq-relay-inspect Threads::Threads)
moqr_bench_warnings(bench_relay_saturation)

if(MOQ_BUILD_TESTS)
    add_test(NAME bench_relay_core_smoke
        COMMAND bench_relay_core --objects 3000 --warmup 500
            --cursors 4 --object-size 256 --json)
    set_tests_properties(bench_relay_core_smoke PROPERTIES TIMEOUT 60)
    add_test(NAME bench_relay_core_rejects_unknown_flag
        COMMAND bench_relay_core --no-such-flag)
    set_tests_properties(bench_relay_core_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE)

    add_test(NAME bench_relay_shards_smoke COMMAND bench_relay_shards)
    set_tests_properties(bench_relay_shards_smoke PROPERTIES TIMEOUT 120)
    add_test(NAME bench_relay_shards_rejects_unknown_flag
        COMMAND bench_relay_shards --no-such-flag)
    set_tests_properties(bench_relay_shards_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE)

    add_test(NAME bench_relay_sched_smoke COMMAND bench_relay_sched --quick)
    set_tests_properties(bench_relay_sched_smoke PROPERTIES TIMEOUT 60)
    add_test(NAME bench_relay_sched_rejects_unknown_flag
        COMMAND bench_relay_sched --no-such-flag)
    set_tests_properties(bench_relay_sched_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE)

    add_test(NAME bench_relay_saturation_smoke
        COMMAND bench_relay_saturation --verify)
    set_tests_properties(bench_relay_saturation_smoke PROPERTIES TIMEOUT 120)
    add_test(NAME bench_relay_saturation_rejects_unknown_flag
        COMMAND bench_relay_saturation --no-such-flag)
    set_tests_properties(bench_relay_saturation_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE)
    add_test(NAME bench_relay_saturation_rejects_oversized_cap
        COMMAND bench_relay_saturation --cap 4294967295 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_oversized_cap PROPERTIES
        WILL_FAIL TRUE TIMEOUT 5)
    add_test(NAME bench_relay_saturation_rejects_aliased_property
        COMMAND bench_relay_saturation --payload-bytes 256
            --property-bytes 256 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_aliased_property
        PROPERTIES WILL_FAIL TRUE TIMEOUT 5)
    add_test(NAME bench_relay_saturation_rejects_huge_lambda
        COMMAND bench_relay_saturation --campaign knee --lambda 1e300
            --duration-ms 1 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_huge_lambda PROPERTIES
        WILL_FAIL TRUE TIMEOUT 5)
    add_test(NAME bench_relay_saturation_rejects_tiny_lambda
        COMMAND bench_relay_saturation --campaign knee --lambda 1e-300
            --duration-ms 1 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_tiny_lambda PROPERTIES
        WILL_FAIL TRUE TIMEOUT 5)

    set(MOQR_RELAY_BENCH_TESTS
        bench_relay_core_smoke bench_relay_core_rejects_unknown_flag
        bench_relay_shards_smoke bench_relay_shards_rejects_unknown_flag
        bench_relay_sched_smoke bench_relay_sched_rejects_unknown_flag
        bench_relay_saturation_smoke
        bench_relay_saturation_rejects_unknown_flag
        bench_relay_saturation_rejects_oversized_cap
        bench_relay_saturation_rejects_aliased_property
        bench_relay_saturation_rejects_huge_lambda
        bench_relay_saturation_rejects_tiny_lambda)
    foreach(t IN LISTS MOQR_RELAY_BENCH_TESTS)
        set_tests_properties(${t} PROPERTIES LABELS "relay;tooling")
    endforeach()
endif()
