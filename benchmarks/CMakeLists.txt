# Benchmarks: performance code, intentionally NOT covered by
# MOQ_WARNINGS_AS_ERRORS (not a correctness gate). See the option in the
# top-level CMakeLists for the warnings-as-errors scope/policy.
add_executable(moq_bench_core bench_core.c)
target_link_libraries(moq_bench_core PRIVATE moq-sim)

add_executable(moq_bench_session_scale bench_session_scale.c)
target_link_libraries(moq_bench_session_scale PRIVATE moq-sim)

add_executable(moq_bench_process_fanout bench_process_fanout.c)
target_link_libraries(moq_bench_process_fanout PRIVATE moq-sim)

add_executable(moq_bench_session_lookup bench_session_lookup.c)
target_link_libraries(moq_bench_session_lookup PRIVATE moq-sim)

add_executable(moq_bench_session_subgroup bench_session_subgroup.c)
target_link_libraries(moq_bench_session_subgroup PRIVATE moq-sim)

add_executable(moq_bench_relay_fanout bench_relay_fanout.c)
target_link_libraries(moq_bench_relay_fanout PRIVATE moq-sim)

if(MOQ_BUILD_ADAPTER_PICOQUIC)
    add_executable(moq_bench_picoquic_loopback bench_picoquic_loopback.c)
    target_link_libraries(moq_bench_picoquic_loopback PRIVATE moq::adapter-picoquic)
endif()

if(MOQ_BUILD_TESTS)
    add_test(NAME bench_core_smoke
             COMMAND moq_bench_core --objects 8 --object-size 64)
    set_tests_properties(bench_core_smoke PROPERTIES
        TIMEOUT 10
        LABELS "bench;smoke")

    add_test(NAME bench_core_rejects_zero_size
             COMMAND moq_bench_core --objects 1 --object-size 0)
    set_tests_properties(bench_core_rejects_zero_size PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "bench;smoke")

    add_test(NAME bench_session_scale_smoke
             COMMAND moq_bench_session_scale --subscribers 3 --objects 8
                     --object-size 64 --warmup 0)
    set_tests_properties(bench_session_scale_smoke PROPERTIES
        TIMEOUT 10
        LABELS "bench;smoke")

    add_test(NAME bench_session_scale_rejects_zero_subs
             COMMAND moq_bench_session_scale --subscribers 0)
    set_tests_properties(bench_session_scale_rejects_zero_subs PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "bench;smoke")

    add_test(NAME bench_process_fanout_smoke
             COMMAND moq_bench_process_fanout --subscribers 3 --objects 8
                     --object-size 64 --warmup 0)
    set_tests_properties(bench_process_fanout_smoke PROPERTIES
        TIMEOUT 10
        LABELS "bench;smoke")

    add_test(NAME bench_process_fanout_rejects_zero_subs
             COMMAND moq_bench_process_fanout --subscribers 0)
    set_tests_properties(bench_process_fanout_rejects_zero_subs PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "bench;smoke")

    # >64 subscriptions to prove the SimPair client subscription-pool cap is
    # lifted (client_max_subscriptions), so it cannot silently regress.
    add_test(NAME bench_session_lookup_smoke
             COMMAND moq_bench_session_lookup --mode alias --subscriptions 128
                     --objects 16 --object-size 64 --warmup 0)
    set_tests_properties(bench_session_lookup_smoke PROPERTIES
        TIMEOUT 15
        LABELS "bench;smoke")

    add_test(NAME bench_session_lookup_rejects_zero_subs
             COMMAND moq_bench_session_lookup --subscriptions 0)
    set_tests_properties(bench_session_lookup_rejects_zero_subs PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "bench;smoke")

    add_test(NAME bench_session_subgroup_smoke
             COMMAND moq_bench_session_subgroup --objects 16 --object-size 64
                     --warmup 0)
    set_tests_properties(bench_session_subgroup_smoke PROPERTIES
        TIMEOUT 10
        LABELS "bench;smoke")

    add_test(NAME bench_session_subgroup_rejects_zero_objects
             COMMAND moq_bench_session_subgroup --objects 0)
    set_tests_properties(bench_session_subgroup_rejects_zero_objects PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "bench;smoke")

    add_test(NAME bench_relay_fanout_smoke
             COMMAND moq_bench_relay_fanout --fanout 4 --objects 16
                     --object-size 64 --warmup 4)
    set_tests_properties(bench_relay_fanout_smoke PROPERTIES
        TIMEOUT 15
        LABELS "bench;smoke")

    add_test(NAME bench_relay_fanout_rejects_zero_fanout
             COMMAND moq_bench_relay_fanout --fanout 0)
    set_tests_properties(bench_relay_fanout_rejects_zero_fanout PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "bench;smoke")

    if(MOQ_BUILD_ADAPTER_PICOQUIC)
        add_test(NAME bench_picoquic_loopback_smoke
                 COMMAND ${CMAKE_COMMAND} -E env bash
                         ${CMAKE_SOURCE_DIR}/scripts/smoke_bench_picoquic.sh
                         ${CMAKE_BINARY_DIR})
        set_tests_properties(bench_picoquic_loopback_smoke PROPERTIES
            TIMEOUT 30
            LABELS "bench;smoke;transport")
    endif()
endif()
