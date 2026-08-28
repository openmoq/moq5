# moq5-relay: deterministic MoQ relay (tool + internal relay-core library).
#
# moq-relay-core is a sans-I/O static library over the PUBLIC libmoq session
# API only — no transport, no threads, no private headers (enforced by
# scripts/check_relay_boundary.sh, registered as the relay_boundary test).
# The CLI and transport bindings arrive with the transport binding and gate on adapter
# options; the core library and its tests require nothing beyond moq::core.

add_library(moq-relay-core STATIC
    core/src/auth_toy.c
    core/src/wire_codes.c
    core/src/capacity.c
    core/src/log.c
    core/src/relay.c
    core/src/trace.c
    core/src/types.c
)
add_library(moq::relay-core ALIAS moq-relay-core)

target_include_directories(moq-relay-core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/core/include)
target_link_libraries(moq-relay-core PUBLIC moq::core)
target_compile_features(moq-relay-core PUBLIC c_std_11)
set_target_properties(moq-relay-core PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON)

# Owned code: full warning baseline (PRIVATE; -Werror gated by the option).
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-relay-core PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    # Test-internals core (bind/shard precedent): the SAME core sources
    # compiled with MOQR_CORE_TESTING for the begun-ledger gauge. The invariant
    # it reads has no public witness, and no production target links this.
    add_library(moq-relay-core-test-internals STATIC
        core/src/wire_codes.c core/src/auth_toy.c core/src/capacity.c
        core/src/log.c core/src/relay.c core/src/trace.c core/src/types.c)
    target_compile_definitions(moq-relay-core-test-internals PRIVATE
        MOQR_CORE_TESTING)
    target_include_directories(moq-relay-core-test-internals PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/core/include)
    target_link_libraries(moq-relay-core-test-internals PUBLIC moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-relay-core-test-internals PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    foreach(t IN ITEMS test_relay_scaffold test_relay_log
            test_relay_scenario test_relay_fetch test_relay_forcewithdraw
            test_relay_upstreamcancel test_relay_capacity)
        # (session-binding parity test added below; needs moq::sim)
        add_executable(${t} tests/${t}.c)
        target_link_libraries(${t} PRIVATE moq::relay-core)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(${t} PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
    endforeach()
    # The control test drives the begun-ledger gauge, so it links the gated
    # core rather than the shipped one.
    add_executable(test_relay_control tests/test_relay_control.c)
    target_link_libraries(test_relay_control PRIVATE
        moq-relay-core-test-internals)
    target_compile_definitions(test_relay_control PRIVATE MOQR_CORE_TESTING)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_control PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    # White-box: the log test's capacity checker walks internal layout.
    target_include_directories(test_relay_log PRIVATE core/src)
    # White-box: the track-index oracle compiles relay.c into its own TU
    # (the intrusive index and the exhaustive-scan predicate it mirrors are
    # both internal), so it builds the OTHER core sources directly instead
    # of linking moq::relay-core.
    add_executable(test_relay_track_index tests/test_relay_track_index.c
        core/src/wire_codes.c core/src/auth_toy.c core/src/capacity.c core/src/log.c
        core/src/trace.c core/src/types.c)
    target_include_directories(test_relay_track_index PRIVATE core/src
        ${CMAKE_CURRENT_SOURCE_DIR}/core/include)
    target_link_libraries(test_relay_track_index PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_track_index PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_track_index COMMAND test_relay_track_index)
    add_executable(test_relay_wire_codes tests/test_relay_wire_codes.c
        core/src/wire_codes.c core/src/auth_toy.c core/src/capacity.c
        core/src/log.c core/src/trace.c core/src/types.c)
    target_include_directories(test_relay_wire_codes PRIVATE core/src
        ${CMAKE_CURRENT_SOURCE_DIR}/core/include
        ${CMAKE_SOURCE_DIR}/tests/unit)
    target_link_libraries(test_relay_wire_codes PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_wire_codes PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_wire_codes COMMAND test_relay_wire_codes)
    add_executable(test_relay_ready_set tests/test_relay_ready_set.c
        core/src/wire_codes.c core/src/auth_toy.c core/src/capacity.c core/src/log.c
        core/src/trace.c core/src/types.c)
    target_include_directories(test_relay_ready_set PRIVATE core/src
        ${CMAKE_CURRENT_SOURCE_DIR}/core/include)
    target_link_libraries(test_relay_ready_set PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_ready_set PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_ready_set COMMAND test_relay_ready_set)
    # White-box: the bind scheduler layout/capacity oracle compiles
    # moqr_bind.c into its own TU (b_conn_t and the bitset helpers are
    # internal), linking relay-core for the core it drives.
    add_executable(test_relay_bind_layout tests/test_relay_bind_layout.c)
    target_include_directories(test_relay_bind_layout PRIVATE bind)
    target_link_libraries(test_relay_bind_layout PRIVATE moq::relay-core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_bind_layout PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_bind_layout COMMAND test_relay_bind_layout)
    add_test(NAME relay_scaffold COMMAND test_relay_scaffold)
    add_test(NAME relay_log COMMAND test_relay_log)
    add_test(NAME relay_control COMMAND test_relay_control)
    add_test(NAME relay_scenario COMMAND test_relay_scenario)
    add_test(NAME relay_fetch COMMAND test_relay_fetch)
    add_test(NAME relay_forcewithdraw COMMAND test_relay_forcewithdraw)
    add_test(NAME relay_upstreamcancel COMMAND test_relay_upstreamcancel)
    add_test(NAME relay_capacity COMMAND test_relay_capacity)
    set_tests_properties(relay_scaffold relay_log relay_control
        relay_scenario relay_fetch relay_forcewithdraw relay_upstreamcancel
        relay_capacity relay_track_index relay_ready_set relay_bind_layout
        PROPERTIES LABELS "relay")
    set_tests_properties(relay_scenario PROPERTIES TIMEOUT 120
        LABELS "relay;scenario")
    add_executable(test_relay_cli_config
        tests/test_relay_cli_config.c cli/config.c cli/snapshot.c)
    target_include_directories(test_relay_cli_config PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(test_relay_cli_config PRIVATE moq::relay-obs)
    # The per-lane attribution row grammar (pure string tests; no transport).
    add_executable(test_relay_lane_stats
        tests/test_relay_lane_stats.c cli/lanestats.c)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_lane_stats PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_lane_stats COMMAND test_relay_lane_stats)
    set_tests_properties(relay_lane_stats PROPERTIES LABELS "relay")
    # The blocked-heavy reason record grammar (pure string tests; no transport).
    add_executable(test_relay_blocked_stats
        tests/test_relay_blocked_stats.c cli/blockedstats.c)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_blocked_stats PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_blocked_stats COMMAND test_relay_blocked_stats)
    set_tests_properties(relay_blocked_stats PROPERTIES LABELS "relay")
    # The blocked-scenario constraint seam: cli/config.c compiled WITH
    # MOQR_VERIFY_SEAM (the moq-relay-measure composition; moq-relay-verify
    # adds MOQR_BIND_TESTING on top) — strict env parsing + the bind-pool
    # override reaching the one shard-config builder.
    add_executable(test_relay_verify_seam
        tests/test_relay_verify_seam.c cli/config.c cli/snapshot.c)
    target_compile_definitions(test_relay_verify_seam PRIVATE
        MOQR_VERIFY_SEAM)
    target_include_directories(test_relay_verify_seam PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(test_relay_verify_seam PRIVATE moq::relay-obs)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_verify_seam PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_verify_seam COMMAND test_relay_verify_seam)
    set_tests_properties(relay_verify_seam PROPERTIES LABELS "relay")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_cli_config PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_cli_config COMMAND test_relay_cli_config)

    # The command line as a pure function of argv: no file, no environment.
    add_executable(test_relay_cli_args tests/test_relay_cli_args.c cli/cliargs.c)
    target_include_directories(test_relay_cli_args PRIVATE cli)
    target_link_libraries(test_relay_cli_args PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_cli_args PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_cli_args COMMAND test_relay_cli_args)
    set_tests_properties(relay_cli_args PROPERTIES TIMEOUT 30)

    # Source-level wiring gate: an endpoint runtime cannot distinguish the two
    # main.c serve compositions, so this pins them directly.
    find_program(MOQR_WIRE_BASH bash)
    if(MOQR_WIRE_BASH)
        add_test(NAME relay_serve_version_wiring COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_serve_version_wiring.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c
            ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)
        set_tests_properties(relay_serve_version_wiring
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_sub_done_pd COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_sub_done_pd.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/core/src/relay.c)
        set_tests_properties(relay_sub_done_pd
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_bind_pump_stack COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_bind_pump_stack.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/bind/moqr_bind.c)
        set_tests_properties(relay_bind_pump_stack
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_reap_wiring COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_reap_wiring.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c
            ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)
        set_tests_properties(relay_reap_wiring
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_signal_order COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_signal_order.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_signal_order
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_metrics_const COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_metrics_const.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/obs/metrics.c)
        set_tests_properties(relay_metrics_const
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_wake_self_bit COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_wake_self_bit.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/bench/bench_relay_saturation.c)
        set_tests_properties(relay_wake_self_bit
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_migration_codes COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_migration_codes.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/bind/moqr_bind.c
            ${CMAKE_CURRENT_SOURCE_DIR}/shard/moqr_shards.c)
        set_tests_properties(relay_migration_codes
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_test_inventory COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_test_inventory.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/scenario_owners.md)
        set_tests_properties(relay_test_inventory
            PROPERTIES LABELS "relay" TIMEOUT 30)
    endif()
    set_tests_properties(relay_cli_config PROPERTIES LABELS "relay")

    add_executable(test_relay_obs tests/test_relay_obs.c)
    target_link_libraries(test_relay_obs PRIVATE moq::relay-obs)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_obs PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_obs COMMAND test_relay_obs)
    set_tests_properties(relay_obs PROPERTIES LABELS "relay")

    add_executable(test_relay_route_dump tests/test_relay_route_dump.c)
    target_link_libraries(test_relay_route_dump PRIVATE moq::relay-core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_route_dump PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_route_dump COMMAND test_relay_route_dump)
    set_tests_properties(relay_route_dump PROPERTIES LABELS "relay")

    # Multi-shard runtime skeleton (pure bind+core, no SimPair).
    add_executable(test_relay_shards tests/test_relay_shards.c)
    target_link_libraries(test_relay_shards PRIVATE moq::relay-shard)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_shards PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    add_executable(test_relay_shards_mt tests/test_relay_shards_mt.c)
    target_link_libraries(test_relay_shards_mt PRIVATE moq::relay-shard)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_shards_mt PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_shards COMMAND test_relay_shards)
    set_tests_properties(relay_shards PROPERTIES LABELS "relay")

    add_test(NAME relay_shards_mt COMMAND test_relay_shards_mt)
    set_tests_properties(relay_shards_mt PROPERTIES LABELS "relay" TIMEOUT 60)

    if(TARGET moq::sim)
        # Test-internals bind build (mvfst precedent): same source compiled
        # with MOQR_BIND_TESTING for the delivery-probe gauge. Never linked
        # by production targets.
        # Shard runtime with the same testing seam (verify-only SG_SEAL
        # ingest evidence ring). Never linked into production binaries.
        add_library(moq-relay-shard-test-internals OBJECT shard/moqr_shards.c)
        target_compile_definitions(moq-relay-shard-test-internals PRIVATE
            MOQR_BIND_TESTING)
        target_include_directories(moq-relay-shard-test-internals PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/shard)
        target_link_libraries(moq-relay-shard-test-internals PRIVATE
            moq::relay-bind Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(moq-relay-shard-test-internals PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_library(moq-relay-bind-test-internals OBJECT bind/moqr_bind.c)
        target_compile_definitions(moq-relay-bind-test-internals PRIVATE
            MOQR_BIND_TESTING)

        target_link_libraries(moq-relay-bind-test-internals PRIVATE
            moq::relay-core)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(moq-relay-bind-test-internals PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_executable(test_relay_session_binding
            tests/test_relay_session_binding.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>)
        target_compile_definitions(test_relay_session_binding PRIVATE
            MOQR_BIND_TESTING)
        target_link_libraries(test_relay_session_binding PRIVATE
            moq::relay-core moq::sim)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_session_binding PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_session_binding
                 COMMAND test_relay_session_binding)
        set_tests_properties(relay_session_binding PROPERTIES TIMEOUT 120
            LABELS "relay")

        # Seeded production-binding lifecycle scenario: event-surface
        # accounting + run-twice determinism over SimPair churn.
        add_executable(test_relay_lifecycle tests/test_relay_lifecycle.c)
        target_link_libraries(test_relay_lifecycle PRIVATE
            moq::relay-bind moq::sim)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_lifecycle PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_lifecycle COMMAND test_relay_lifecycle)
        set_tests_properties(relay_lifecycle PROPERTIES TIMEOUT 120
            LABELS "relay;scenario")

        # Cross-shard authorization: K=2 multi-bind SimPair rig proving auth
        # stays requester-local and lane-safe across the demand channel.
        # Carries the bind test-internals seam: the cross-shard BIND_SG
        # recovery scenario asserts the per-conn refusal counters and the
        # ready/parked scheduling state directly.
        add_executable(test_relay_auth_shards tests/test_relay_auth_shards.c
            cli/blockedstats.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>)
        target_compile_definitions(test_relay_auth_shards PRIVATE
            MOQR_BIND_TESTING)
        target_link_libraries(test_relay_auth_shards PRIVATE
            moq::relay-shard moq::sim)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_auth_shards PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_auth_shards COMMAND test_relay_auth_shards)
        set_tests_properties(relay_auth_shards PROPERTIES TIMEOUT 120
            LABELS "relay")

        # Namespace advertisement propagation: the draft-18 Section 6.2 MUST
        # across both arrival orders and both shard topologies.
        add_executable(test_relay_ns_propagation
            tests/test_relay_ns_propagation.c)
        target_link_libraries(test_relay_ns_propagation PRIVATE
            moq::relay-shard moq::sim)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_ns_propagation PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_ns_propagation COMMAND test_relay_ns_propagation)
        # Sans-I/O: measured 0.03 s (Debug) to 0.58 s (TSan). The bound is a
        # deadlock guard with ~50x headroom, never part of correctness.
        set_tests_properties(relay_ns_propagation PROPERTIES TIMEOUT 30
            LABELS "relay")
    endif()
endif()

# Production session binding: transport-agnostic (sees only moq_session_t);
# links relay-core + moq::core, NO transport. The parity test drives it
# over SimPair; the relay executable drives it over managed adapters.
add_library(moq-relay-bind STATIC bind/moqr_bind.c)
add_library(moq::relay-bind ALIAS moq-relay-bind)
target_include_directories(moq-relay-bind PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/bind)
target_link_libraries(moq-relay-bind PUBLIC moq::relay-core)
target_compile_features(moq-relay-bind PUBLIC c_std_11)
set_target_properties(moq-relay-bind PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-relay-bind PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Observability serializers: pure renderers over stat snapshots. Links the
# shard runtime (hence the binding and the core) so it sees every stat
# struct, including the cross-shard plane; no transport, no I/O.
add_library(moq-relay-obs STATIC obs/metrics.c)
add_library(moq::relay-obs ALIAS moq-relay-obs)
target_include_directories(moq-relay-obs PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/obs)
target_link_libraries(moq-relay-obs PUBLIC moq::relay-shard)
target_compile_features(moq-relay-obs PUBLIC c_std_11)
set_target_properties(moq-relay-obs PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-relay-obs PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Multi-shard runtime: owns K {core, bind, trace} shards and steps them
# deterministically. Links the binding (hence the core); no transport, no I/O.
find_package(Threads REQUIRED)   # the shard runtime's mailbox leaf mutexes
add_library(moq-relay-shard STATIC shard/moqr_shards.c)
add_library(moq::relay-shard ALIAS moq-relay-shard)
target_include_directories(moq-relay-shard PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/shard)
target_link_libraries(moq-relay-shard PUBLIC moq::relay-bind Threads::Threads)
target_compile_features(moq-relay-shard PUBLIC c_std_11)
set_target_properties(moq-relay-shard PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-relay-shard PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Pure-primitive benchmark: links the relay core only (no sim, no transport).
add_executable(bench_relay_core bench/bench_relay_core.c)
target_link_libraries(bench_relay_core PRIVATE moq::relay-core)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(bench_relay_core PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Cross-shard cadence benchmark: exact message counts and derived pump-turn
# bounds under the deterministic runner. Links the shard runtime; no sim, no
# transport, no wall clock.
add_executable(bench_relay_shards bench/bench_relay_shards.c)
target_link_libraries(bench_relay_shards PRIVATE moq::relay-shard)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(bench_relay_shards PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Scheduler benchmark: drives the production delivery path (moqr_core_next_delivery
# / moqr_core_delivery_done), not the log cursor. Relay core only, no transport.
add_executable(bench_relay_sched bench/bench_relay_sched.c)
target_link_libraries(bench_relay_sched PRIVATE moq::relay-core)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(bench_relay_sched PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Owner-boundary saturation harness: threaded per-shard steppers over the
# cross-shard plane, machine-readable schema, closed-loop ceiling/verification.
# Measures and diagnoses; optimizes nothing. Threaded, so it links pthreads.
find_package(Threads REQUIRED)
add_executable(bench_relay_saturation bench/bench_relay_saturation.c)
target_link_libraries(bench_relay_saturation
    PRIVATE moq::relay-shard Threads::Threads)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(bench_relay_saturation PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# The relay executable: needs the MsQuic managed server (the production
# transport). The core, binding, and every test above build with zero
# transport deps. Without the msquic-managed target the executable is simply
# not built — there is no picoquic fallback.
if(TARGET moq::adapter-msquic-managed)
    add_executable(moq5-relay cli/main.c cli/cliargs.c cli/config.c cli/versions.c
        cli/lanestats.c cli/snapshot.c cli/conn_reap.c)
    target_include_directories(moq5-relay PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(moq5-relay PRIVATE
        moq::relay-bind moq::relay-obs moq::relay-shard
        moq::adapter-msquic-managed)

    # What ships is the command and the material needed to operate it.
    # Relay archives and the moqrelay/ headers are build-tree interfaces, not a
    # supported SDK, so they are deliberately not installed and no relay CMake
    # export or pkg-config surface is added.
    include(GNUInstallDirs)
    # The installed command resolves its runtime dependencies relative to
    # itself, under the prefix it was installed into. No build-tree path
    # survives the install, and no absolute host path is baked in.
    if(APPLE)
        set_target_properties(moq5-relay PROPERTIES
            INSTALL_RPATH "@loader_path/../${CMAKE_INSTALL_LIBDIR}")
    else()
        set_target_properties(moq5-relay PROPERTIES
            INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
    endif()
    set(CMAKE_INSTALL_DOCDIR_MOQ5_RELAY
        ${CMAKE_INSTALL_DATAROOTDIR}/doc/moq5-relay)
    install(TARGETS moq5-relay
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
                    COMPONENT moq5-relay-runtime)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/docs/moq5-relay.1
            DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
            COMPONENT moq5-relay-runtime)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/README.md
            DESTINATION ${CMAKE_INSTALL_DOCDIR_MOQ5_RELAY}
            COMPONENT moq5-relay-runtime)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/examples/relay.json
            DESTINATION ${CMAKE_INSTALL_DOCDIR_MOQ5_RELAY}/examples
            COMPONENT moq5-relay-runtime)
    if(MOQ_BUILD_TESTS AND MOQR_WIRE_BASH)
        # Informational and malformed invocations only: no transport, no
        # configuration, so this stays in the fast lane.
        add_test(NAME relay_cli_surface COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_cli_surface.sh
            $<TARGET_FILE:moq5-relay>
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_cli_surface PROPERTIES TIMEOUT 60)

        # A staged install, checked as an exact set: the command and its
        # documentation, and nothing private.
        add_test(NAME relay_install_surface COMMAND ${CMAKE_COMMAND}
            -DCMD=$<TARGET_FILE:moq5-relay>
            -DSRCROOT=${CMAKE_SOURCE_DIR}
            -DBUILDDIR=${CMAKE_BINARY_DIR}
            -DGUARD=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_install_surface.sh
            -DBASH=${MOQR_WIRE_BASH}
            -DDEPLIB=$<TARGET_FILE_DIR:msquic::msquic>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_install_surface.cmake)
        set_tests_properties(relay_install_surface PROPERTIES TIMEOUT 300)
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq5-relay PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    # moq-relay-verify: the SAME CLI over the bind test-internals seam
    # (MOQR_BIND_TESTING). SIGUSR1 renders a RELAY_BLOCKED_V0 document (one
    # line per lane, coordinator-emitted for a coherent same-epoch set)
    # instead of the metrics exposition, so the gauntlet can prove the exact
    # blocked reason live. The plain moq5-relay above never sees the seam or
    # the blockedstats sources, so it carries no blocked-record string and no
    # test-internal symbol (pinned by relay_verify_hygiene below).
    if(TARGET moq-relay-bind-test-internals)
        add_executable(moq-relay-verify cli/main.c cli/cliargs.c cli/config.c
            cli/versions.c cli/lanestats.c cli/snapshot.c cli/conn_reap.c
            cli/blockedstats.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>)
        target_compile_definitions(moq-relay-verify PRIVATE
            MOQR_BIND_TESTING MOQR_VERIFY_SEAM)
        target_include_directories(moq-relay-verify PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(moq-relay-verify PRIVATE
            moq::relay-core moq::relay-obs moq::relay-shard
            moq::adapter-msquic-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(moq-relay-verify PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        # moq-relay-measure: the SAME constrained pools over the PRODUCTION
        # bind library — the baseline/candidate timing binary for the
        # blocked arm. No debug counters, no blocked records (hygiene-pinned
        # below): the verify build proves the reason, this build times the
        # exact traffic shape symmetrically.
        add_executable(moq-relay-measure cli/main.c cli/cliargs.c cli/config.c
            cli/versions.c cli/lanestats.c cli/snapshot.c cli/conn_reap.c)
        target_compile_definitions(moq-relay-measure PRIVATE MOQR_VERIFY_SEAM)
        target_include_directories(moq-relay-measure PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(moq-relay-measure PRIVATE
            moq::relay-bind moq::relay-obs moq::relay-shard
            moq::adapter-msquic-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(moq-relay-measure PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        # Symbol/string hygiene: the production binary carries no blocked
        # record and no test seam; the verify binary carries both. Archive
        # rule (checked at the archives because final-binary dead stripping can
        # hide a compiled-in symbol): the core and bind archives export no
        # debug seam of any tier, while the shard archive forbids only
        # cross-tier debug symbols and the TESTING-gated verify seams (the
        # seal-evidence ring and demand-join resolvers) — its remaining
        # moqr_shards_debug_* accessors are DELIBERATELY production. All six
        # arguments are mandatory.
        if(MOQ_BUILD_TESTS)
            find_program(MOQ_BASH bash)
            if(MOQ_BASH)
                add_test(NAME relay_verify_hygiene COMMAND ${MOQ_BASH}
                    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/relay_verify_hygiene.sh
                    $<TARGET_FILE:moq5-relay> $<TARGET_FILE:moq-relay-verify>
                    $<TARGET_FILE:moq-relay-measure>
                    $<TARGET_FILE:moq-relay-core>
                    $<TARGET_FILE:moq-relay-bind>
                    $<TARGET_FILE:moq-relay-shard>)
                set_tests_properties(relay_verify_hygiene
                    PROPERTIES LABELS "relay" TIMEOUT 30)
                # Fail-closed pins: a missing archive argument is a hard
                # usage failure, and a leaked debug symbol in ANY production
                # archive fails (proven against a synthetic bad archive
                # built by the test itself).
                add_test(NAME relay_verify_hygiene_rejects_missing_archive
                    COMMAND ${MOQ_BASH}
                    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/relay_verify_hygiene.sh
                    $<TARGET_FILE:moq5-relay> $<TARGET_FILE:moq-relay-verify>
                    $<TARGET_FILE:moq-relay-measure>)
                set_tests_properties(relay_verify_hygiene_rejects_missing_archive
                    PROPERTIES LABELS "relay" TIMEOUT 30 WILL_FAIL TRUE)
                add_test(NAME relay_verify_hygiene_rejects_leaked_symbol
                    COMMAND ${MOQ_BASH} -c
                    "set -e; d=$(mktemp -d); trap 'rm -rf \"$d\"' EXIT; \
                     printf 'int moqr_core_debug_leak(void){return 1;}\\n' > \"$d/leak.c\"; \
                     '${CMAKE_C_COMPILER}' -c \"$d/leak.c\" -o \"$d/leak.o\"; \
                     '${CMAKE_AR}' rcs \"$d/bad.a\" \"$d/leak.o\"; \
                     '${MOQ_BASH}' '${CMAKE_CURRENT_SOURCE_DIR}/scripts/relay_verify_hygiene.sh' \
                       '$<TARGET_FILE:moq5-relay>' '$<TARGET_FILE:moq-relay-verify>' \
                       '$<TARGET_FILE:moq-relay-measure>' \"$d/bad.a\" \
                       '$<TARGET_FILE:moq-relay-bind>' '$<TARGET_FILE:moq-relay-shard>'")
                set_tests_properties(relay_verify_hygiene_rejects_leaked_symbol
                    PROPERTIES LABELS "relay" TIMEOUT 30 WILL_FAIL TRUE)
            endif()
        endif()
    endif()

    if(MOQ_BUILD_TESTS)
        # The thin real-transport cells use LibMoQ's committed test-only
        # certificate and key. Nothing is generated at run time: a fixture that
        # shells out to a certificate tool makes the boundary depend on which
        # tool the host happens to have, and on the clock, for a credential the
        # test never inspects.
        set(MOQR_TEST_CERT
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/test_only_loopback_cert.pem)
        set(MOQR_TEST_KEY
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/test_only_loopback_key.pem)
        if(NOT EXISTS ${MOQR_TEST_CERT} OR NOT EXISTS ${MOQR_TEST_KEY})
            message(FATAL_ERROR
                "the committed test-only certificate/key are missing: "
                "${MOQR_TEST_CERT} ${MOQR_TEST_KEY}")
        endif()
        # The serve-shaped admission regression consumes the CLI serve
        # composition, so the loopback test compiles cli/config + snapshot.
        add_executable(test_relay_loopback tests/test_relay_loopback.c
            cli/config.c cli/snapshot.c cli/conn_reap.c)
        target_include_directories(test_relay_loopback PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(test_relay_loopback PRIVATE
            moq::relay-bind moq::relay-obs moq::relay-shard
            moq::adapter-msquic-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_loopback PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        # Minimum real-QUIC boundary smokes: the SAME scenario functions
        # via the fail-closed selector, one run each. Protocol/scheduler
        # behavior is proven sans-I/O; these prove the adapter wiring.
        add_test(NAME relay_loopback_same_d18
            COMMAND test_relay_loopback
                ${MOQR_TEST_CERT}
                ${MOQR_TEST_KEY}
                lanes_same_d18)
        set_tests_properties(relay_loopback_same_d18 PROPERTIES
            TIMEOUT 60)
        add_test(NAME relay_loopback_cross_d18
            COMMAND test_relay_loopback
                ${MOQR_TEST_CERT}
                ${MOQR_TEST_KEY}
                lanes_cross_d18)
        set_tests_properties(relay_loopback_cross_d18 PROPERTIES
            TIMEOUT 60)

        # Multi-version listener at the RELAY boundary: the test links the
        # production cli/versions.c mapping, so it exercises the same
        # function both serve paths call rather than a copy of it.
        add_executable(test_relay_multiversion
            tests/test_relay_multiversion.c cli/config.c cli/versions.c
            cli/snapshot.c cli/conn_reap.c)
        target_include_directories(test_relay_multiversion PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(test_relay_multiversion PRIVATE
            moq::adapter-msquic-managed moq::relay-obs moq::relay-bind)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_multiversion PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()

        # Every production nonzero-return branch of the lane pumps,
        # activated one at a time through the REAL cli/main.c callbacks
        # (main renamed away) over the sans-I/O facade.
        add_executable(test_relay_pump_branches
            tests/test_relay_pump_branches.c
            tests/pump_branches_cli_tu.c
            cli/cliargs.c cli/config.c cli/versions.c cli/lanestats.c
            cli/snapshot.c cli/conn_reap.c cli/blockedstats.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
        target_include_directories(test_relay_pump_branches PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor
            ${CMAKE_SOURCE_DIR}/adapters/msquic
            ${CMAKE_SOURCE_DIR}/adapters/msquic/include
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests)
        target_compile_definitions(test_relay_pump_branches PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING
            MOQR_PUMP_TESTING MOQR_BIND_TESTING)
        target_link_libraries(test_relay_pump_branches PRIVATE
            moq::relay-obs moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_pump_branches PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_pump_branches
            COMMAND test_relay_pump_branches)
        set_tests_properties(relay_pump_branches PROPERTIES
            LABELS "relay;sansio"
            TIMEOUT 30)

        # The thread/bell boundary, once: real doorbells over the fake
        # table, barrier-ordered, condvar-completed. The only timed wait
        # is a fail-closed hang guard.
        add_executable(test_relay_lane_boundary
            tests/test_relay_lane_boundary.c
            cli/conn_reap.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
        target_include_directories(test_relay_lane_boundary PRIVATE
            cli
            ${CMAKE_SOURCE_DIR}/adapters/msquic
            ${CMAKE_SOURCE_DIR}/adapters/msquic/include
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests)
        target_compile_definitions(test_relay_lane_boundary PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_relay_lane_boundary PRIVATE
            moq::relay-bind moq::relay-obs moq::relay-shard
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_lane_boundary PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_lane_boundary
            COMMAND test_relay_lane_boundary)
        set_tests_properties(relay_lane_boundary PROPERTIES
            LABELS "relay;boundary"
            TIMEOUT 30)

        # The terminal-to-reclaim lifecycle, sans-I/O: the managed facade
        # over a fake API table, no doorbell threads, every production
        # doorbell iteration driven explicitly on the test thread.
        add_executable(test_relay_lane_lifecycle
            tests/test_relay_lane_lifecycle.c
            cli/conn_reap.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
        target_include_directories(test_relay_lane_lifecycle PRIVATE
            cli
            ${CMAKE_SOURCE_DIR}/adapters/msquic
            ${CMAKE_SOURCE_DIR}/adapters/msquic/include
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests)
        target_compile_definitions(test_relay_lane_lifecycle PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING)
        target_link_libraries(test_relay_lane_lifecycle PRIVATE
            moq::relay-bind moq::relay-obs moq::relay-shard
            moq::core msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_lane_lifecycle PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_lane_lifecycle
            COMMAND test_relay_lane_lifecycle)
        set_tests_properties(relay_lane_lifecycle PROPERTIES
            LABELS "relay;sansio"
            TIMEOUT 30)

        add_test(NAME relay_multiversion
            COMMAND test_relay_multiversion
                ${MOQR_TEST_CERT}
                ${MOQR_TEST_KEY})
        set_tests_properties(relay_multiversion PROPERTIES TIMEOUT 180
            LABELS "relay;loopback")
    endif()
endif()

if(MOQ_BUILD_TESTS)
    add_test(NAME bench_relay_core_smoke
             COMMAND bench_relay_core --objects 3000 --warmup 500
                     --cursors 4 --object-size 256 --json)
    set_tests_properties(bench_relay_core_smoke PROPERTIES
        TIMEOUT 60
        LABELS "relay;bench")
    add_test(NAME bench_relay_core_rejects_unknown_flag
             COMMAND bench_relay_core --no-such-flag)
    set_tests_properties(bench_relay_core_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE
        LABELS "relay;bench")

    add_test(NAME bench_relay_shards_smoke COMMAND bench_relay_shards)
    set_tests_properties(bench_relay_shards_smoke PROPERTIES
        TIMEOUT 120
        LABELS "relay;bench")
    add_test(NAME bench_relay_shards_rejects_unknown_flag
             COMMAND bench_relay_shards --no-such-flag)
    set_tests_properties(bench_relay_shards_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE
        LABELS "relay;bench")

    add_test(NAME bench_relay_sched_smoke
             COMMAND bench_relay_sched --quick)
    set_tests_properties(bench_relay_sched_smoke PROPERTIES
        TIMEOUT 60
        LABELS "relay;bench")
    add_test(NAME bench_relay_sched_rejects_unknown_flag
             COMMAND bench_relay_sched --no-such-flag)
    set_tests_properties(bench_relay_sched_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE
        LABELS "relay;bench")

    # Saturation harness: --verify is the CI structural smoke (exact
    # lifetime gates, K=1 inertness, F-vs-2F clone invariance, the ceiling/
    # starvation distinction, and the retention-preflight refusal arms; no
    # wall-clock threshold).
    add_test(NAME bench_relay_saturation_smoke
             COMMAND bench_relay_saturation --verify)
    set_tests_properties(bench_relay_saturation_smoke PROPERTIES
        TIMEOUT 120
        LABELS "relay;bench")
    add_test(NAME bench_relay_saturation_rejects_unknown_flag
             COMMAND bench_relay_saturation --no-such-flag)
    set_tests_properties(bench_relay_saturation_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE
        LABELS "relay;bench")
    # The CLI retention preflight refuses a config that overflows the fixed
    # source-log model (here a huge --cap) rather than entering the worker loop.
    add_test(NAME bench_relay_saturation_rejects_oversized_cap
             COMMAND bench_relay_saturation --cap 4294967295 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_oversized_cap PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "relay;bench")
    # A property size equal to the payload size collapses the two clone oracles
    # into one allocator bucket, so the property-copy oracle is unmeasurable; the
    # preflight must refuse it before launching workers.
    add_test(NAME bench_relay_saturation_rejects_aliased_property
             COMMAND bench_relay_saturation --payload-bytes 256 --property-bytes 256
                     --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_aliased_property PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "relay;bench")
    # A finite but astronomically large lambda must be refused by the retention
    # preflight (bounded in double before any integer conversion), never enter
    # the workers and fill the log.
    add_test(NAME bench_relay_saturation_rejects_huge_lambda
             COMMAND bench_relay_saturation --campaign knee --lambda 1e300
                     --duration-ms 1 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_huge_lambda PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "relay;bench")
    # A finite but tiny lambda has a period past the whole window (1e9/lambda
    # can overflow to inf): the preflight must refuse it, never enter a
    # long-running injection state.
    add_test(NAME bench_relay_saturation_rejects_tiny_lambda
             COMMAND bench_relay_saturation --campaign knee --lambda 1e-300
                     --duration-ms 1 --objects 32)
    set_tests_properties(bench_relay_saturation_rejects_tiny_lambda PROPERTIES
        WILL_FAIL TRUE
        TIMEOUT 5
        LABELS "relay;bench")
    # The saturation aggregator's synthetic self-test: knee detection on a
    # tracks-then-plateaus curve, invalid/starved row exclusion, and a
    # deterministic bootstrap CI (no wall-clock, no bench binary needed).
    find_program(MOQ_PYTHON3 python3)
    if(MOQ_PYTHON3)
        add_test(NAME shard_saturation_aggregator_selftest
                 COMMAND ${MOQ_PYTHON3}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/shard_saturation_agg.py
                         --selftest)
        set_tests_properties(shard_saturation_aggregator_selftest PROPERTIES
            TIMEOUT 30
            LABELS "relay;bench")
        add_test(NAME shard_saturation_aggregator_rejects_unknown_flag
                 COMMAND ${MOQ_PYTHON3}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/shard_saturation_agg.py
                         --no-such-flag)
        set_tests_properties(shard_saturation_aggregator_rejects_unknown_flag
            PROPERTIES WILL_FAIL TRUE TIMEOUT 10 LABELS "relay;bench")
        # The relay-blast report summarizer: synthetic med/min/max, malformed
        # and missing-field rejection, and the measurement-basis separation
        # gate (a non-relay_e2e_wall report is a hard error, never averaged).
        add_test(NAME relay_blast_summarizer_selftest
                 COMMAND ${MOQ_PYTHON3}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/relay_blast_summarize.py
                         --selftest)
        set_tests_properties(relay_blast_summarizer_selftest PROPERTIES
            TIMEOUT 30
            LABELS "relay;bench")
        add_test(NAME relay_blast_summarizer_rejects_unknown_flag
                 COMMAND ${MOQ_PYTHON3}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/relay_blast_summarize.py
                         --no-such-flag)
        set_tests_properties(relay_blast_summarizer_rejects_unknown_flag
            PROPERTIES WILL_FAIL TRUE TIMEOUT 10 LABELS "relay;bench")
        # The two-binary perturbation comparator: identical-pass, rate/p99
        # regression rejection, fail-closed incomplete blocks, metric and
        # fingerprint validation, and argument bounds.
        add_test(NAME relay_perturbation_compare_selftest
                 COMMAND ${MOQ_PYTHON3}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/perturbation_compare.py
                         --selftest)
        set_tests_properties(relay_perturbation_compare_selftest PROPERTIES
            TIMEOUT 30
            LABELS "relay;bench")
        add_test(NAME relay_perturbation_compare_rejects_unknown_flag
                 COMMAND ${MOQ_PYTHON3}
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/perturbation_compare.py
                         --no-such-flag)
        set_tests_properties(relay_perturbation_compare_rejects_unknown_flag
            PROPERTIES WILL_FAIL TRUE TIMEOUT 10 LABELS "relay;bench")
    endif()
    # The perturbation orchestrator's structural gates: provenance
    # prerequisites, manifest content, and schedule determinism (dry-run
    # only — no relay/blast processes).
    add_test(NAME relay_perturbation_check_selftest
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_perturbation_check.sh
                     --selftest)
    set_tests_properties(relay_perturbation_check_selftest PROPERTIES
        TIMEOUT 30 LABELS "relay;bench")
    add_test(NAME relay_perturbation_check_rejects_unknown_flag
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_perturbation_check.sh
                     --no-such-flag)
    set_tests_properties(relay_perturbation_check_rejects_unknown_flag
        PROPERTIES WILL_FAIL TRUE TIMEOUT 10 LABELS "relay;bench")
    # The manual repro orchestrator's structural gates (no gauntlet or relay
    # process involved): an unknown flag and a missing external binary both
    # refuse before anything starts.
    add_test(NAME relay_blast_repro_rejects_unknown_flag
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_relay_blast_repro.sh
                     --no-such-flag)
    set_tests_properties(relay_blast_repro_rejects_unknown_flag PROPERTIES
        WILL_FAIL TRUE TIMEOUT 10 LABELS "relay;bench")
    add_test(NAME relay_blast_repro_rejects_missing_binary
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_relay_blast_repro.sh
                     --lanes 1)
    set_tests_properties(relay_blast_repro_rejects_missing_binary PROPERTIES
        ENVIRONMENT "RELAY_BIN=/nonexistent-relay;BLAST_BIN=/nonexistent-blast"
        WILL_FAIL TRUE TIMEOUT 10 LABELS "relay;bench")
    # Manifest discipline: the summary consumes exactly this invocation's
    # reports (stub binaries; a stale report seeded in the output directory
    # must not leak into the summary).
    add_test(NAME relay_blast_repro_selftest
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_relay_blast_repro.sh
                     --selftest)
    set_tests_properties(relay_blast_repro_selftest PROPERTIES
        TIMEOUT 60 LABELS "relay;bench")
endif()

# Phase-A exact scripted arms: production pump + binding over the sans-I/O
# facade; peers are bare sessions behind the real bridge over the fake
# endpoint, connected by the byte shuttle.
if(TARGET moq::adapter-msquic-managed AND TARGET moq-relay-shard-test-internals)
    add_executable(test_relay_seedx_scripts
        tests/test_relay_seedx_scripts.c
        tests/pump_branches_cli_tu.c
        cli/cliargs.c cli/config.c cli/versions.c cli/lanestats.c
        cli/snapshot.c cli/conn_reap.c cli/blockedstats.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>
        $<TARGET_OBJECTS:moq-relay-shard-test-internals>
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
    target_include_directories(test_relay_seedx_scripts PRIVATE
        cli tests ${CMAKE_SOURCE_DIR}/media/msf/vendor
        ${CMAKE_SOURCE_DIR}/adapters/msquic
        ${CMAKE_SOURCE_DIR}/adapters/msquic/include
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests
        ${CMAKE_SOURCE_DIR}/tests/support)
    target_compile_definitions(test_relay_seedx_scripts PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING
        MOQR_PUMP_TESTING MOQR_BIND_TESTING)
    target_link_libraries(test_relay_seedx_scripts PRIVATE
        moq::relay-obs moq::core msquic::msquic Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_seedx_scripts PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    # Mixed-draft forwarding: one listener offering BOTH drafts, a publisher
    # on one and a subscriber on the other, with every expectation decoded off
    # the downstream peer's own wire.
    add_executable(test_relay_mixed_draft
        tests/test_relay_mixed_draft.c
        tests/pump_branches_cli_tu.c
        cli/cliargs.c cli/config.c cli/versions.c cli/lanestats.c
        cli/snapshot.c cli/conn_reap.c cli/blockedstats.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>
        $<TARGET_OBJECTS:moq-relay-shard-test-internals>
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
    target_include_directories(test_relay_mixed_draft PRIVATE
        cli tests ${CMAKE_SOURCE_DIR}/media/msf/vendor
        ${CMAKE_SOURCE_DIR}/adapters/msquic
        ${CMAKE_SOURCE_DIR}/adapters/msquic/include
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests
        ${CMAKE_SOURCE_DIR}/tests/support)
    target_compile_definitions(test_relay_mixed_draft PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING
        MOQR_PUMP_TESTING MOQR_BIND_TESTING)
    target_link_libraries(test_relay_mixed_draft PRIVATE
        moq::relay-obs moq::core msquic::msquic Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_mixed_draft PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_mixed_draft COMMAND test_relay_mixed_draft)
    set_tests_properties(relay_mixed_draft PROPERTIES LABELS "relay"
                         TIMEOUT 60)

    add_test(NAME relay_seedx_scripts COMMAND test_relay_seedx_scripts)
    set_tests_properties(relay_seedx_scripts PROPERTIES TIMEOUT 30)
    # the multipublisher source-lifecycle suite (draft-16 SS8.3 / draft-18
    # SS9.3): retarget-to-standby and no-alternate explicit-terminal arms
    add_test(NAME relay_multipub
             COMMAND test_relay_seedx_scripts --multipub)
    set_tests_properties(relay_multipub PROPERTIES TIMEOUT 30)
    # post-revocation re-admission: same/replacement publisher, single- and
    # cross-shard, through a third generation
    add_test(NAME relay_readmission
             COMMAND test_relay_seedx_scripts --readmission)
    set_tests_properties(relay_readmission PROPERTIES TIMEOUT 30)
endif()

# The seeded scheduler explorer: same rig as the exact scripted arms, plus
# the pure model/generator shared with the trace-vector unit.
if(TARGET moq::adapter-msquic-managed AND TARGET moq-relay-shard-test-internals)
    add_executable(test_relay_sched_explore
        tests/test_relay_sched_explore.c
        tests/seedx/seedx_trace.c
        tests/pump_branches_cli_tu.c
        cli/cliargs.c cli/config.c cli/versions.c cli/lanestats.c
        cli/snapshot.c cli/conn_reap.c cli/blockedstats.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>
        $<TARGET_OBJECTS:moq-relay-shard-test-internals>
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
    target_include_directories(test_relay_sched_explore PRIVATE
        cli tests ${CMAKE_SOURCE_DIR}/media/msf/vendor
        ${CMAKE_SOURCE_DIR}/adapters/msquic
        ${CMAKE_SOURCE_DIR}/adapters/msquic/include
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests
        ${CMAKE_SOURCE_DIR}/tests/support)
    target_compile_definitions(test_relay_sched_explore PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING
        MOQR_PUMP_TESTING MOQR_BIND_TESTING)
    target_link_libraries(test_relay_sched_explore PRIVATE
        moq::relay-obs moq::core msquic::msquic Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_sched_explore PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_sched_seeded COMMAND test_relay_sched_explore --seeds)
    set_tests_properties(relay_sched_seeded PROPERTIES TIMEOUT 30
        ENVIRONMENT "SCHED_MANIFEST=${CMAKE_CURRENT_SOURCE_DIR}/tests/vectors/sched_seeds_v1.txt")
    add_test(NAME relay_sched_cli_units
             COMMAND test_relay_sched_explore --cli-units)
    set_tests_properties(relay_sched_cli_units PROPERTIES TIMEOUT 30)
    add_test(NAME relay_sched_close_feed
             COMMAND test_relay_sched_explore --close-feed)
    set_tests_properties(relay_sched_close_feed PROPERTIES TIMEOUT 30)
endif()

# The pure trace-vector unit: model + generator + codec only, no adapter —
# the compiler-independence pin for the committed trace bytes.
add_executable(test_relay_sched_vectors
    tests/test_relay_sched_vectors.c tests/seedx/seedx_trace.c)
target_include_directories(test_relay_sched_vectors PRIVATE tests)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(test_relay_sched_vectors PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
add_test(NAME relay_sched_vectors COMMAND test_relay_sched_vectors)
set_tests_properties(relay_sched_vectors PROPERTIES TIMEOUT 30
    ENVIRONMENT "SCHED_MANIFEST=${CMAKE_CURRENT_SOURCE_DIR}/tests/vectors/sched_seeds_v1.txt")

# Phase-A seedx foundations: PRNG golden vectors, canonical trace codec,
# reference-ledger identities. Pure units, no facade, no transport.
add_executable(test_relay_seedx_units
    tests/test_relay_seedx_units.c tests/seedx/seedx_trace.c)
target_include_directories(test_relay_seedx_units PRIVATE tests)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(test_relay_seedx_units PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
add_test(NAME relay_seedx_units COMMAND test_relay_seedx_units
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/vectors/prng_splitmix64_v1.txt)
set_tests_properties(relay_seedx_units PROPERTIES TIMEOUT 30)

# -- merge-lane labels: one authoritative pass -------------------------------
# The last property setting wins at generate time, so every relay test's lane
# membership is decided HERE, in one reviewable place. Lanes: sansio (exact
# deterministic), boundary (irreducible physical surface, run once each) and
# tooling (deterministic tool selftests). Every registered relay test belongs
# to one of the three: a scenario that needs repetition or wall-time to say
# anything has no owner and is not registered.
set(MOQR_TESTS_SANSIO
    relay_boundary relay_track_index relay_ready_set relay_bind_layout
    relay_scaffold relay_log relay_control relay_scenario relay_fetch
    relay_forcewithdraw relay_upstreamcancel relay_capacity relay_lane_stats
    relay_blocked_stats relay_verify_seam relay_cli_config
    relay_serve_version_wiring relay_reap_wiring relay_obs relay_route_dump
    relay_test_inventory relay_migration_codes relay_metrics_const
    relay_wake_self_bit
    relay_signal_order
    relay_cli_args
    relay_cli_surface relay_install_surface
    relay_shards relay_session_binding relay_lifecycle relay_auth_shards
    relay_ns_propagation
    relay_verify_hygiene relay_verify_hygiene_rejects_missing_archive
    relay_verify_hygiene_rejects_leaked_symbol relay_pump_branches
    relay_lane_lifecycle relay_seedx_units relay_seedx_scripts
    relay_sched_vectors relay_sched_cli_units relay_sched_close_feed
    relay_multipub relay_readmission)
set(MOQR_TESTS_BOUNDARY
    relay_shards_mt relay_lane_boundary relay_multiversion
    relay_loopback_same_d18 relay_loopback_cross_d18)
set(MOQR_TESTS_TOOLING
    bench_relay_core_smoke bench_relay_core_rejects_unknown_flag
    bench_relay_shards_smoke bench_relay_shards_rejects_unknown_flag
    bench_relay_sched_smoke bench_relay_sched_rejects_unknown_flag
    bench_relay_saturation_smoke bench_relay_saturation_rejects_unknown_flag
    bench_relay_saturation_rejects_oversized_cap
    bench_relay_saturation_rejects_aliased_property
    bench_relay_saturation_rejects_huge_lambda
    bench_relay_saturation_rejects_tiny_lambda
    shard_saturation_aggregator_selftest
    shard_saturation_aggregator_rejects_unknown_flag
    relay_blast_summarizer_selftest relay_blast_summarizer_rejects_unknown_flag
    relay_perturbation_compare_selftest
    relay_perturbation_compare_rejects_unknown_flag
    relay_perturbation_check_selftest
    relay_perturbation_check_rejects_unknown_flag
    relay_blast_repro_rejects_unknown_flag
    relay_blast_repro_rejects_missing_binary relay_blast_repro_selftest)
foreach(t IN LISTS MOQR_TESTS_SANSIO)
    if(TEST ${t})
        set_tests_properties(${t} PROPERTIES LABELS "relay;sansio")
    endif()
endforeach()
foreach(t IN LISTS MOQR_TESTS_BOUNDARY)
    if(TEST ${t})
        set_tests_properties(${t} PROPERTIES LABELS "relay;boundary")
    endif()
endforeach()
foreach(t IN LISTS MOQR_TESTS_TOOLING)
    if(TEST ${t})
        set_tests_properties(${t} PROPERTIES LABELS "relay;tooling")
    endif()
endforeach()
# the committed 16-seed merge entry: its own lane label
if(TEST relay_sched_seeded)
    set_tests_properties(relay_sched_seeded PROPERTIES
        LABELS "relay;seeded"
        ENVIRONMENT "SCHED_MANIFEST=${CMAKE_CURRENT_SOURCE_DIR}/tests/vectors/sched_seeds_v1.txt")
endif()
