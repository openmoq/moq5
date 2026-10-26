# Tests owned by the transport-independent relay libraries.
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../private")

set(MOQR_TEST_CORE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/core")
set(MOQR_TEST_BIND_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/bind")
set(MOQR_TEST_SHARD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/shard")
set(MOQR_TEST_OBS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src/obs")

function(moqr_test_warnings target)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(${target} PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
endfunction()

# The same core compiled with the private begun-ledger test seam.
add_library(moq-relay-core-test-internals STATIC
    ${MOQR_TEST_CORE_DIR}/wire_codes.c
    ${MOQR_TEST_CORE_DIR}/capacity.c
    ${MOQR_TEST_CORE_DIR}/log.c
    ${MOQR_TEST_CORE_DIR}/relay.c
    ${MOQR_TEST_CORE_DIR}/trace.c
    ${MOQR_TEST_CORE_DIR}/types.c
    ${MOQR_TEST_CORE_DIR}/stats.c
    ${MOQR_TEST_CORE_DIR}/props_codec.c)
target_compile_definitions(moq-relay-core-test-internals PRIVATE
    MOQR_CORE_TESTING MOQ_RELAY_CORE_STATIC)
target_include_directories(moq-relay-core-test-internals PUBLIC
    ${MOQ_RELAY_INCLUDE_DIR})
target_link_libraries(moq-relay-core-test-internals PUBLIC moq::core)
moqr_test_warnings(moq-relay-core-test-internals)

foreach(t IN ITEMS test_relay_scaffold test_relay_log
        test_relay_scenario test_relay_fetch test_relay_forcewithdraw
        test_relay_upstreamcancel test_relay_capacity test_relay_track_reclaim)
    add_executable(${t} ${t}.c)
    target_link_libraries(${t} PRIVATE moq::relay-core)
    moqr_test_warnings(${t})
endforeach()

add_executable(test_relay_control test_relay_control.c)
target_link_libraries(test_relay_control PRIVATE
    moq-relay-core-test-internals)
target_compile_definitions(test_relay_control PRIVATE MOQR_CORE_TESTING)
moqr_test_warnings(test_relay_control)

add_executable(test_relay_props_codec test_relay_props_codec.c)
target_link_libraries(test_relay_props_codec PRIVATE moq::relay-core)
target_include_directories(test_relay_props_codec PRIVATE ${MOQR_TEST_CORE_DIR})
moqr_test_warnings(test_relay_props_codec)
add_test(NAME relay_props_codec COMMAND test_relay_props_codec)

# White-box core and binding tests compile the production source into their
# own translation units; no production target exposes these internals.
target_include_directories(test_relay_log PRIVATE ${MOQR_TEST_CORE_DIR})

add_executable(test_relay_track_index test_relay_track_index.c
    ${MOQR_TEST_CORE_DIR}/wire_codes.c
    ${MOQR_TEST_CORE_DIR}/capacity.c
    ${MOQR_TEST_CORE_DIR}/log.c
    ${MOQR_TEST_CORE_DIR}/trace.c
    ${MOQR_TEST_CORE_DIR}/types.c)
target_include_directories(test_relay_track_index PRIVATE
    ${MOQR_TEST_CORE_DIR} ${MOQ_RELAY_INCLUDE_DIR})
target_link_libraries(test_relay_track_index PRIVATE moq::core)
moqr_test_warnings(test_relay_track_index)

add_executable(test_relay_wire_codes test_relay_wire_codes.c
    ${MOQR_TEST_CORE_DIR}/wire_codes.c
    ${MOQR_TEST_CORE_DIR}/capacity.c
    ${MOQR_TEST_CORE_DIR}/log.c
    ${MOQR_TEST_CORE_DIR}/trace.c
    ${MOQR_TEST_CORE_DIR}/types.c)
target_include_directories(test_relay_wire_codes PRIVATE
    ${MOQR_TEST_CORE_DIR} ${MOQ_RELAY_INCLUDE_DIR}
    ${PROJECT_SOURCE_DIR}/tests/unit)
target_link_libraries(test_relay_wire_codes PRIVATE moq::core)
moqr_test_warnings(test_relay_wire_codes)

add_executable(test_relay_ready_set test_relay_ready_set.c
    ${MOQR_TEST_CORE_DIR}/wire_codes.c
    ${MOQR_TEST_CORE_DIR}/capacity.c
    ${MOQR_TEST_CORE_DIR}/log.c
    ${MOQR_TEST_CORE_DIR}/trace.c
    ${MOQR_TEST_CORE_DIR}/types.c)
target_include_directories(test_relay_ready_set PRIVATE
    ${MOQR_TEST_CORE_DIR} ${MOQ_RELAY_INCLUDE_DIR})
target_link_libraries(test_relay_ready_set PRIVATE moq::core)
moqr_test_warnings(test_relay_ready_set)

if(MOQ_BUILD_RELAY_RUNTIME)
add_executable(test_relay_bind_layout test_relay_bind_layout.c)
target_include_directories(test_relay_bind_layout PRIVATE
    ${MOQR_TEST_BIND_DIR} ${MOQ_RELAY_INCLUDE_DIR})
target_link_libraries(test_relay_bind_layout PRIVATE moq::relay-core)
moqr_test_warnings(test_relay_bind_layout)
add_test(NAME relay_bind_layout COMMAND test_relay_bind_layout)
endif()

add_test(NAME relay_track_index COMMAND test_relay_track_index)
add_test(NAME relay_wire_codes COMMAND test_relay_wire_codes)
add_test(NAME relay_ready_set COMMAND test_relay_ready_set)
add_test(NAME relay_scaffold COMMAND test_relay_scaffold)
add_test(NAME relay_log COMMAND test_relay_log)
add_test(NAME relay_control COMMAND test_relay_control)
add_test(NAME relay_scenario COMMAND test_relay_scenario)
add_test(NAME relay_fetch COMMAND test_relay_fetch)
add_test(NAME relay_forcewithdraw COMMAND test_relay_forcewithdraw)
add_test(NAME relay_upstreamcancel COMMAND test_relay_upstreamcancel)
add_test(NAME relay_capacity COMMAND test_relay_capacity)
add_test(NAME relay_track_reclaim COMMAND test_relay_track_reclaim)
set_tests_properties(relay_scaffold relay_log relay_control relay_scenario
    relay_fetch relay_forcewithdraw relay_upstreamcancel relay_capacity
    relay_track_reclaim relay_track_index relay_wire_codes relay_ready_set
    PROPERTIES LABELS "relay")
set_tests_properties(relay_scenario PROPERTIES TIMEOUT 120
    LABELS "relay;scenario")

add_executable(test_relay_route_dump test_relay_route_dump.c)
target_link_libraries(test_relay_route_dump PRIVATE moq::relay-core)
moqr_test_warnings(test_relay_route_dump)
add_test(NAME relay_route_dump COMMAND test_relay_route_dump)

if(MOQ_BUILD_RELAY_RUNTIME)
add_executable(test_relay_obs test_relay_obs.c)
target_link_libraries(test_relay_obs PRIVATE moq::relay)
moqr_test_warnings(test_relay_obs)
add_test(NAME relay_obs COMMAND test_relay_obs)

add_executable(test_relay_obs_format
    test_relay_obs_format.c obs_internal_tu.c)
target_link_libraries(test_relay_obs_format PRIVATE moq::relay)
target_include_directories(test_relay_obs_format PRIVATE
    ${MOQR_TEST_OBS_DIR} ${MOQ_RELAY_INCLUDE_DIR})
moqr_test_warnings(test_relay_obs_format)
add_test(NAME relay_obs_format COMMAND test_relay_obs_format)

add_executable(test_relay_obs_golden test_relay_obs_golden.c)
target_link_libraries(test_relay_obs_golden PRIVATE
    moq::relay Threads::Threads)
target_compile_definitions(test_relay_obs_golden PRIVATE
    MOQR_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/golden")
moqr_test_warnings(test_relay_obs_golden)
add_test(NAME relay_obs_golden COMMAND test_relay_obs_golden)

add_executable(test_relay_shards test_relay_shards.c)
target_link_libraries(test_relay_shards PRIVATE moq-relay-inspect)
moqr_test_warnings(test_relay_shards)
add_test(NAME relay_shards COMMAND test_relay_shards)

add_executable(test_relay_shards_mt test_relay_shards_mt.c)
target_link_libraries(test_relay_shards_mt PRIVATE moq-relay-inspect)
moqr_test_warnings(test_relay_shards_mt)
add_test(NAME relay_shards_mt COMMAND test_relay_shards_mt)
set_tests_properties(relay_shards_mt PROPERTIES TIMEOUT 60)

if(TARGET moq::sim)
    # These private object targets are also consumed by CLI composition tests.
    add_library(moq-relay-shard-test-internals OBJECT
        ${MOQR_TEST_SHARD_DIR}/moqr_shards.c)
    target_compile_definitions(moq-relay-shard-test-internals PRIVATE
        MOQR_BIND_TESTING MOQ_RELAY_BUILDING)
    target_include_directories(moq-relay-shard-test-internals PRIVATE
        ${MOQ_RELAY_INCLUDE_DIR})
    target_link_libraries(moq-relay-shard-test-internals PRIVATE
        moq::relay Threads::Threads)
    moqr_test_warnings(moq-relay-shard-test-internals)

    add_library(moq-relay-bind-test-internals OBJECT
        ${MOQR_TEST_BIND_DIR}/moqr_bind.c)
    target_compile_definitions(moq-relay-bind-test-internals PRIVATE
        MOQR_BIND_TESTING MOQ_RELAY_BUILDING)
    target_link_libraries(moq-relay-bind-test-internals PRIVATE
        moq::relay-core)
    moqr_test_warnings(moq-relay-bind-test-internals)

    # The session-binding suites share one test-only rig (allocator, SimPair
    # rig, peer drain, shared fixtures), compiled once and linked into each
    # suite executable; it is never part of a library.
    add_library(moq-relay-session-binding-harness OBJECT
        relay_session_binding_harness.c)
    target_compile_definitions(moq-relay-session-binding-harness PRIVATE
        MOQR_BIND_TESTING)
    target_link_libraries(moq-relay-session-binding-harness PRIVATE
        moq::sim moq-relay-policy)
    moqr_test_warnings(moq-relay-session-binding-harness)

    # One executable per suite, so one failing suite never hides the others.
    # The core suite keeps the relay_session_binding test name; the others
    # register as relay_session_binding_<suite>.
    set(MOQR_SESSION_BINDING_SUITES
        core auth delivery ingress fetch namespace d22 metadata reset)
    foreach(suite IN LISTS MOQR_SESSION_BINDING_SUITES)
        set(_exe test_relay_session_binding_${suite})
        add_executable(${_exe} ${_exe}.c
            $<TARGET_OBJECTS:moq-relay-session-binding-harness>
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
        target_compile_definitions(${_exe} PRIVATE MOQR_BIND_TESTING)
        target_link_libraries(${_exe} PRIVATE moq::sim moq-relay-policy)
        moqr_test_warnings(${_exe})
        if(suite STREQUAL "core")
            set(_test relay_session_binding)
        else()
            set(_test relay_session_binding_${suite})
        endif()
        add_test(NAME ${_test} COMMAND ${_exe})
        set_tests_properties(${_test} PROPERTIES TIMEOUT 120)
    endforeach()
    # Object Properties across drafts, by delivery family (same rig).
    add_executable(test_relay_props_delivery test_relay_props_delivery.c
        $<TARGET_OBJECTS:moq-relay-session-binding-harness>
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
    target_compile_definitions(test_relay_props_delivery PRIVATE MOQR_BIND_TESTING)
    target_link_libraries(test_relay_props_delivery PRIVATE moq::sim moq-relay-policy)
    moqr_test_warnings(test_relay_props_delivery)
    add_test(NAME relay_props_delivery COMMAND test_relay_props_delivery)
    set_tests_properties(relay_props_delivery PROPERTIES TIMEOUT 120 LABELS "relay;sansio")
    # Wire decoding belongs to a scenario outside the relay source boundary.
    add_executable(test_relay_session_binding_stop
        ${PROJECT_SOURCE_DIR}/tests/scenario/test_relay_session_binding_stop.c
        $<TARGET_OBJECTS:moq-relay-session-binding-harness>
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
    target_compile_definitions(test_relay_session_binding_stop PRIVATE MOQR_BIND_TESTING)
    target_link_libraries(test_relay_session_binding_stop PRIVATE moq::sim moq-relay-policy)
    moqr_test_warnings(test_relay_session_binding_stop)
    foreach(draft IN ITEMS 16 18 22)
        foreach(row IN ITEMS seal continuation siblings no-reopen capacity)
            string(REPLACE "-" "_" _row "${row}")
            foreach(disposition IN ITEMS stop control)
                foreach(load IN ITEMS normal pressure)
                    set(_test relay_subgroup_stop_d${draft}_${_row}_${disposition}_${load})
                    add_test(NAME ${_test} COMMAND test_relay_session_binding_stop
                        ${draft} ${row} ${disposition} ${load})
                    set_tests_properties(${_test} PROPERTIES TIMEOUT 120
                        LABELS "relay;sansio;subgroup-stop;d${draft}")
                endforeach()
            endforeach()
        endforeach()
    endforeach()
    add_test(NAME relay_metadata_lifetime
        COMMAND test_relay_session_binding_metadata --metadata-lifetime)
    add_test(NAME relay_direct_core_publisher
        COMMAND test_relay_session_binding_metadata --direct-core-publisher)
    set_tests_properties(relay_metadata_lifetime relay_direct_core_publisher
        PROPERTIES TIMEOUT 120 LABELS "relay")
    # Core contract (fetcher-side late response stream after cancel): its
    # own entry so its result is reported apart from the ordinary suite.
    add_test(NAME relay_core_cancel_after_stop
        COMMAND test_relay_session_binding_fetch --core-cancel-after-stop)

    add_executable(test_relay_standalone_fetch test_relay_standalone_fetch.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
    target_compile_definitions(test_relay_standalone_fetch PRIVATE
        MOQR_BIND_TESTING)
    target_link_libraries(test_relay_standalone_fetch PRIVATE
        moq::sim moq-relay-policy)
    moqr_test_warnings(test_relay_standalone_fetch)
    foreach(row IN ITEMS publish-gap pull-gap cached-only timeout-zero
            absent-source source-error source-reset total-deadline
            cancel-reuse capacity-recovery descending cancel-cached
            coalesce-oom coalesce-control legacy descending-groups
            evicted-range abort-blocked d18-origin d16-origin d18-descending-wire
            d18-descending-wire-d18-origin source-lost expired-zero
            expired-budget)
        string(REPLACE "-" "_" _row "${row}")
        add_test(NAME relay_standalone_fetch_${_row}
            COMMAND test_relay_standalone_fetch ${row})
        set_tests_properties(relay_standalone_fetch_${_row} PROPERTIES
            TIMEOUT 60 LABELS "relay;sansio;d22")
    endforeach()
    # A draft-18 origin and subscriber: the relay's own upstream FETCH is a
    # draft-18 one, descending included.
    foreach(row IN ITEMS publish-gap descending total-deadline source-error)
        string(REPLACE "-" "_" _row "${row}")
        add_test(NAME relay_standalone_fetch_d18x_${_row}
            COMMAND test_relay_standalone_fetch d18x:${row})
        set_tests_properties(relay_standalone_fetch_d18x_${_row} PROPERTIES
            TIMEOUT 60 LABELS "relay;sansio")
    endforeach()
    # The same retrieval rows for draft-16 and draft-18 FETCH subscribers.
    # A draft-18 session does not originate a descending FETCH (its local
    # policy), so that row runs for draft-16 only.
    foreach(draft IN ITEMS d16 d18)
        set(_rows publish-gap pull-gap cached-only absent-source source-error
            source-reset total-deadline cancel-reuse capacity-recovery
            cancel-cached abort-blocked)
        if(draft STREQUAL "d16")
            list(APPEND _rows descending)
        endif()
        foreach(row IN LISTS _rows)
            string(REPLACE "-" "_" _row "${row}")
            add_test(NAME relay_standalone_fetch_${draft}_${_row}
                COMMAND test_relay_standalone_fetch ${draft}:${row})
            set_tests_properties(relay_standalone_fetch_${draft}_${_row} PROPERTIES
                TIMEOUT 60 LABELS "relay;sansio")
        endforeach()
    endforeach()

    add_executable(test_relay_unfiltered test_relay_unfiltered.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
    target_compile_definitions(test_relay_unfiltered PRIVATE MOQR_BIND_TESTING)
    target_link_libraries(test_relay_unfiltered PRIVATE
        moq::sim moq-relay-policy)
    moqr_test_warnings(test_relay_unfiltered)
    add_test(NAME relay_unfiltered COMMAND test_relay_unfiltered)
    set_tests_properties(relay_unfiltered PROPERTIES TIMEOUT 120)

    add_executable(test_relay_lifecycle test_relay_lifecycle.c)
    target_link_libraries(test_relay_lifecycle PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_lifecycle)
    add_test(NAME relay_lifecycle COMMAND test_relay_lifecycle)
    set_tests_properties(relay_lifecycle PROPERTIES TIMEOUT 120)

    add_executable(test_relay_ns_propagation test_relay_ns_propagation.c)
    target_link_libraries(test_relay_ns_propagation PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_ns_propagation)
    add_test(NAME relay_ns_propagation COMMAND test_relay_ns_propagation)
    set_tests_properties(relay_ns_propagation PROPERTIES TIMEOUT 30)

    add_executable(test_relay_namespace_baseline
        test_relay_namespace_baseline.c)
    target_link_libraries(test_relay_namespace_baseline PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_namespace_baseline)
    add_test(NAME relay_namespace_controls
        COMMAND test_relay_namespace_baseline --controls)
    add_test(NAME relay_namespace_features
        COMMAND test_relay_namespace_baseline --features)
    set_tests_properties(relay_namespace_controls relay_namespace_features
        PROPERTIES TIMEOUT 30)

    add_executable(test_relay_namespace_d22 test_relay_namespace_d22.c)
    target_link_libraries(test_relay_namespace_d22 PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_namespace_d22)
    add_test(NAME relay_namespace_d22 COMMAND test_relay_namespace_d22)
    set_tests_properties(relay_namespace_d22 PROPERTIES TIMEOUT 60)

    add_executable(test_relay_standalone_fetch_shard
        test_relay_standalone_fetch_shard.c)
    target_link_libraries(test_relay_standalone_fetch_shard PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_standalone_fetch_shard)
    foreach(row IN ITEMS cross-gap cross-cache cross-cancel cross-absent
            cross-descending cross-capacity cross-capacity-requester
            cross-capacity-owner-rows)
        string(REPLACE "-" "_" _row "${row}")
        add_test(NAME relay_standalone_fetch_shard_${_row}
            COMMAND test_relay_standalone_fetch_shard ${row})
        set_tests_properties(relay_standalone_fetch_shard_${_row} PROPERTIES
            TIMEOUT 60 LABELS "relay;sansio;d22")
    endforeach()

    # Cross-shard standalone FETCH Track Properties, which reach the
    # binding's private metadata calls: built from the bind and shard test
    # objects, as the CLI's cross-shard composition tests are.
    if(TARGET moq-relay-bind-test-internals AND TARGET moq-relay-shard-test-internals)
        add_executable(test_relay_props_shard test_relay_props_shard.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>)
        target_compile_definitions(test_relay_props_shard PRIVATE MOQR_BIND_TESTING)
        target_link_libraries(test_relay_props_shard PRIVATE moq::relay moq::sim)
        moqr_test_warnings(test_relay_props_shard)
        # plainNN-*: property-less controls, a draft-NN fetcher on the owner
        # shard (local) or the other one (cross).
        foreach(row IN ITEMS same convert to16 to18 immutable oom parked cancel ceiling
                plain16-local plain16-cross plain18-local plain18-cross plain22-cross)
            string(REPLACE "-" "_" _row "${row}")
            add_test(NAME relay_props_shard_${_row}
                COMMAND test_relay_props_shard ${row})
            set_tests_properties(relay_props_shard_${_row} PROPERTIES
                TIMEOUT 60 LABELS "relay;sansio")
        endforeach()
    endif()

    add_executable(test_relay_join_shard test_relay_join_shard.c)
    target_link_libraries(test_relay_join_shard PRIVATE moq-relay-inspect moq::sim)
    moqr_test_warnings(test_relay_join_shard)
    foreach(row IN ITEMS cached hole saved-largest cancel publish-queued
            replaced-while-queued)
        string(REPLACE "-" "_" _row "${row}")
        add_test(NAME relay_join_shard_${_row}
            COMMAND test_relay_join_shard ${row})
        set_tests_properties(relay_join_shard_${_row} PROPERTIES
            TIMEOUT 60 LABELS "relay;sansio")
    endforeach()

    # Eviction at a remote owner over real sessions, every draft (the same
    # rig as test_relay_shard_fill, which the source includes).
    add_executable(test_relay_shard_evict_terminal
        test_relay_shard_evict_terminal.c)
    target_link_libraries(test_relay_shard_evict_terminal PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_shard_evict_terminal)
    foreach(row IN ITEMS whole chunks watermark-only nodes refused)
        string(REPLACE "-" "_" _row "${row}")
        add_test(NAME relay_shard_evict_terminal_${_row}
            COMMAND test_relay_shard_evict_terminal ${row})
        set_tests_properties(relay_shard_evict_terminal_${_row} PROPERTIES
            TIMEOUT 60 LABELS "relay;sansio")
    endforeach()

    add_executable(test_relay_shard_fill test_relay_shard_fill.c)
    target_link_libraries(test_relay_shard_fill PRIVATE
        moq::relay moq::sim)
    moqr_test_warnings(test_relay_shard_fill)
    add_test(NAME relay_shard_fill COMMAND test_relay_shard_fill)
    set_tests_properties(relay_shard_fill PROPERTIES TIMEOUT 60)
    add_test(NAME relay_shard_fill_mixed_draft
        COMMAND test_relay_shard_fill --mixed-draft)
    set_tests_properties(relay_shard_fill_mixed_draft PROPERTIES TIMEOUT 60)

endif()

endif() # runtime tests

# Deterministic scheduler codec/model foundations shared with tool-level
# managed-adapter exploration tests.
add_executable(test_relay_sched_vectors
    test_relay_sched_vectors.c seedx/seedx_trace.c)
target_include_directories(test_relay_sched_vectors PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})
moqr_test_warnings(test_relay_sched_vectors)
add_test(NAME relay_sched_vectors COMMAND test_relay_sched_vectors)
set_tests_properties(relay_sched_vectors PROPERTIES TIMEOUT 30
    ENVIRONMENT "SCHED_MANIFEST=${CMAKE_CURRENT_SOURCE_DIR}/vectors/sched_seeds_v1.txt")

add_executable(test_relay_seedx_units
    test_relay_seedx_units.c seedx/seedx_trace.c)
target_include_directories(test_relay_seedx_units PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})
moqr_test_warnings(test_relay_seedx_units)
add_test(NAME relay_seedx_units COMMAND test_relay_seedx_units
    ${CMAKE_CURRENT_SOURCE_DIR}/vectors/prng_splitmix64_v1.txt)
set_tests_properties(relay_seedx_units PROPERTIES TIMEOUT 30)

find_program(MOQR_RELAY_TEST_BASH bash)
if(MOQR_RELAY_TEST_BASH)
    add_test(NAME relay_sub_done_pd COMMAND ${MOQR_RELAY_TEST_BASH}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_sub_done_pd.sh
        ${MOQR_TEST_CORE_DIR}/relay.c)
    if(MOQ_BUILD_RELAY_RUNTIME)
    add_test(NAME relay_bind_pump_stack COMMAND ${MOQR_RELAY_TEST_BASH}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_bind_pump_stack.sh
        ${MOQR_TEST_BIND_DIR}/moqr_bind.c)
    add_test(NAME relay_metrics_const COMMAND ${MOQR_RELAY_TEST_BASH}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_metrics_const.sh
        ${MOQR_TEST_OBS_DIR}/metrics.c)
    add_test(NAME relay_wake_self_bit COMMAND ${MOQR_RELAY_TEST_BASH}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_wake_self_bit.sh
        ${CMAKE_CURRENT_SOURCE_DIR}/../bench/bench_relay_saturation.c)
    add_test(NAME relay_migration_codes COMMAND ${MOQR_RELAY_TEST_BASH}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_migration_codes.sh
        ${MOQR_TEST_BIND_DIR}/moqr_bind.c
        ${MOQR_TEST_SHARD_DIR}/moqr_shards.c)
    set_tests_properties(relay_sub_done_pd relay_bind_pump_stack
        relay_metrics_const relay_wake_self_bit relay_migration_codes
        PROPERTIES TIMEOUT 30 LABELS "relay")
    endif()
endif()

set(MOQR_LIBRARY_TESTS_SANSIO
    relay_track_index relay_ready_set relay_bind_layout relay_scaffold
    relay_log relay_control relay_scenario relay_fetch relay_forcewithdraw
    relay_upstreamcancel relay_capacity relay_track_reclaim relay_obs
    relay_route_dump
    relay_shards relay_session_binding relay_session_binding_auth
    relay_session_binding_delivery relay_session_binding_ingress
    relay_session_binding_fetch relay_session_binding_namespace
    relay_session_binding_d22 relay_session_binding_metadata
    relay_session_binding_reset
    relay_lifecycle relay_ns_propagation
    relay_shard_fill relay_namespace_controls relay_namespace_features
    relay_namespace_d22
    relay_sched_vectors relay_seedx_units relay_migration_codes
    relay_metrics_const relay_wake_self_bit)
foreach(t IN LISTS MOQR_LIBRARY_TESTS_SANSIO)
    if(TEST ${t})
        set_tests_properties(${t} PROPERTIES LABELS "relay;sansio")
    endif()
endforeach()

if(TEST relay_shards_mt)
    set_tests_properties(relay_shards_mt PROPERTIES LABELS "relay;boundary")
endif()

add_executable(test_relay_sdk test_relay_sdk.c)
if(MOQ_BUILD_RELAY_RUNTIME)
    target_link_libraries(test_relay_sdk PRIVATE moq::relay)
    target_compile_definitions(test_relay_sdk PRIVATE TEST_RUNTIME)
else()
    target_link_libraries(test_relay_sdk PRIVATE moq::relay-core)
endif()
moqr_test_warnings(test_relay_sdk)
add_test(NAME relay_sdk COMMAND test_relay_sdk)
set_tests_properties(relay_sdk PROPERTIES TIMEOUT 30 LABELS "relay;sansio")

# This boundary test installs only into an owned temporary prefix. It builds
# external consumers, not another copy of the library or any network fixture.
find_package(Python3 QUIET COMPONENTS Interpreter)
if(CMAKE_CXX_COMPILER)
    set(MOQR_SDK_CXX "${CMAKE_CXX_COMPILER}")
else()
    find_program(MOQR_SDK_CXX NAMES c++ clang++ g++)
endif()
if(Python3_Interpreter_FOUND AND UNIX AND MOQR_SDK_CXX)
    add_test(NAME relay_sdk_options COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_sdk_options.py
        --source ${PROJECT_SOURCE_DIR} --cmake ${CMAKE_COMMAND} --cc ${CMAKE_C_COMPILER})
    set_tests_properties(relay_sdk_options PROPERTIES TIMEOUT 90 LABELS "relay;boundary")
    include("${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerBuildContext.cmake")
    set(_sdk_consumer_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-context.cmake")
    moq_write_consumer_context("${_sdk_consumer_context}")
    add_test(NAME relay_sdk_package COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_sdk_package.py
        --build ${PROJECT_BINARY_DIR} --source ${CMAKE_CURRENT_SOURCE_DIR}/..
        --context "${_sdk_consumer_context}"
        --libdir ${CMAKE_INSTALL_LIBDIR} --includedir ${CMAKE_INSTALL_INCLUDEDIR}
        --cmake ${CMAKE_COMMAND} --cc ${CMAKE_C_COMPILER} --cxx ${MOQR_SDK_CXX}
        --runtime $<BOOL:${MOQ_BUILD_RELAY_RUNTIME}>
        --shared $<BOOL:${BUILD_SHARED_LIBS}>
        --managed $<TARGET_EXISTS:moq::adapter-msquic-managed>
        --sim $<TARGET_EXISTS:moq-sim>
        --msquic-dir "${msquic_DIR}")
    set_tests_properties(relay_sdk_package PROPERTIES TIMEOUT 180 LABELS "relay;boundary")
endif()

add_executable(test_relay_cluster test_relay_cluster.c)
target_link_libraries(test_relay_cluster PRIVATE moq::relay-core)
moqr_test_warnings(test_relay_cluster)
add_test(NAME relay_cluster COMMAND test_relay_cluster)

if(MOQ_BUILD_RELAY_RUNTIME)
add_executable(test_relay_cluster_shards test_relay_cluster_shards.c)
target_link_libraries(test_relay_cluster_shards PRIVATE moq-relay-inspect)
moqr_test_warnings(test_relay_cluster_shards)
add_test(NAME relay_cluster_shards COMMAND test_relay_cluster_shards)
if(TARGET moq::sim)
add_executable(test_relay_cluster_mesh test_relay_cluster_mesh.c)
target_link_libraries(test_relay_cluster_mesh PRIVATE moq::relay moq::sim)
moqr_test_warnings(test_relay_cluster_mesh)
add_test(NAME relay_cluster_mesh COMMAND test_relay_cluster_mesh)
endif()
endif()
