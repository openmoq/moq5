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
    ${MOQR_TEST_CORE_DIR}/stats.c)
target_compile_definitions(moq-relay-core-test-internals PRIVATE
    MOQR_CORE_TESTING MOQ_RELAY_CORE_STATIC)
target_include_directories(moq-relay-core-test-internals PUBLIC
    ${MOQ_RELAY_INCLUDE_DIR})
target_link_libraries(moq-relay-core-test-internals PUBLIC moq::core)
moqr_test_warnings(moq-relay-core-test-internals)

foreach(t IN ITEMS test_relay_scaffold test_relay_log
        test_relay_scenario test_relay_fetch test_relay_forcewithdraw
        test_relay_upstreamcancel test_relay_capacity)
    add_executable(${t} ${t}.c)
    target_link_libraries(${t} PRIVATE moq::relay-core)
    moqr_test_warnings(${t})
endforeach()

add_executable(test_relay_control test_relay_control.c)
target_link_libraries(test_relay_control PRIVATE
    moq-relay-core-test-internals)
target_compile_definitions(test_relay_control PRIVATE MOQR_CORE_TESTING)
moqr_test_warnings(test_relay_control)

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
set_tests_properties(relay_scaffold relay_log relay_control relay_scenario
    relay_fetch relay_forcewithdraw relay_upstreamcancel relay_capacity
    relay_track_index relay_wire_codes relay_ready_set
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

    add_executable(test_relay_session_binding test_relay_session_binding.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
    target_compile_definitions(test_relay_session_binding PRIVATE
        MOQR_BIND_TESTING)
    target_link_libraries(test_relay_session_binding PRIVATE
        moq::sim moq-relay-policy)
    moqr_test_warnings(test_relay_session_binding)
    add_test(NAME relay_session_binding COMMAND test_relay_session_binding)
    set_tests_properties(relay_session_binding PROPERTIES TIMEOUT 120)

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
    relay_upstreamcancel relay_capacity relay_obs relay_route_dump
    relay_shards relay_session_binding relay_lifecycle relay_ns_propagation
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
    add_test(NAME relay_sdk_package COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/check_sdk_package.py
        --build ${CMAKE_BINARY_DIR} --source ${CMAKE_CURRENT_SOURCE_DIR}/..
        --libdir ${CMAKE_INSTALL_LIBDIR} --includedir ${CMAKE_INSTALL_INCLUDEDIR}
        --cmake ${CMAKE_COMMAND} --cc ${CMAKE_C_COMPILER} --cxx ${MOQR_SDK_CXX}
        --runtime $<BOOL:${MOQ_BUILD_RELAY_RUNTIME}>
        --shared $<BOOL:${BUILD_SHARED_LIBS}>
        --managed $<TARGET_EXISTS:moq::adapter-msquic-managed>
        --msquic-dir "${msquic_DIR}")
    set_tests_properties(relay_sdk_package PROPERTIES TIMEOUT 180 LABELS "relay;boundary")
endif()
