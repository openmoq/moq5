# moq5-relay: command-line composition for the reusable relay libraries.
#
# Forwarding behavior lives under relay/.  This directory owns only the CLI,
# administration transport, process lifecycle, integration tests, and tools.
set(MOQR_LIBRARY_DIR "${PROJECT_SOURCE_DIR}/relay")
set(MOQR_LIBRARY_INCLUDE_DIR "${MOQR_LIBRARY_DIR}/include")
set(MOQR_LIBRARY_CORE_DIR "${MOQR_LIBRARY_DIR}/src/core")
set(MOQR_LIBRARY_BIND_DIR "${MOQR_LIBRARY_DIR}/src/bind")
set(MOQR_LIBRARY_SHARD_DIR "${MOQR_LIBRARY_DIR}/src/shard")
set(MOQR_LIBRARY_OBS_DIR "${MOQR_LIBRARY_DIR}/src/obs")
set(MOQR_LIBRARY_TEST_DIR "${MOQR_LIBRARY_DIR}/tests")

# CLI administration and coordinator tests own their thread dependency.  Do
# not rely on the directory-scoped imported target created by relay/.
find_package(Threads REQUIRED)
include_directories("${MOQR_LIBRARY_DIR}/private")

# The sans-I/O admin tier: bounded parser, content negotiation, and the
# deterministic response state machine. No sockets and no clock of its own.
# Production code links this -- the admin endpoint is a shipped surface -- so it
# is defined outside the test block.
add_library(moqr-admin STATIC
    admin/http_parse.c admin/admin_sm.c admin/json_out.c)
target_include_directories(moqr-admin PUBLIC admin ${MOQR_LIBRARY_INCLUDE_DIR})
target_link_libraries(moqr-admin PUBLIC moq::relay moq-relay-policy)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moqr-admin PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    # Relay-owned tests are registered by relay/tests.
    add_executable(test_relay_cli_config
        tests/test_relay_cli_config.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/snapshot.c)
    target_include_directories(test_relay_cli_config PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(test_relay_cli_config PRIVATE moqr-admin)
    # The per-lane attribution row grammar (pure string tests; no transport).
    add_executable(test_relay_lane_stats
        tests/test_relay_lane_stats.c cli/lanestats.c cli/logevent.c)
    target_include_directories(test_relay_lane_stats PRIVATE
        cli ${CMAKE_SOURCE_DIR}/core/include)
    target_link_libraries(test_relay_lane_stats PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_lane_stats PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_lane_stats COMMAND test_relay_lane_stats)
    set_tests_properties(relay_lane_stats PROPERTIES LABELS "relay")

    # The serve's stdout sink and structured events: frozen text baseline,
    # JSON events through an independent parser, the latching stream, the
    # nullable clock, and the once-only diagnostics -- all with scripted I/O.
    add_executable(test_relay_log_sink
        tests/test_relay_log_sink.c cli/lanestats.c cli/logevent.c cli/config.c
        cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/snapshot.c)
    target_include_directories(test_relay_log_sink PRIVATE
        cli ${CMAKE_SOURCE_DIR}/core/include
        ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(test_relay_log_sink PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_log_sink PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_log_sink COMMAND test_relay_log_sink)
    set_tests_properties(relay_log_sink PROPERTIES LABELS "relay;sansio" TIMEOUT 60)

    # Dual-listener config + shard plan (pure; no transport linkage).
    add_executable(test_relay_dual_listener
        tests/test_relay_dual_listener.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/snapshot.c)
    target_include_directories(test_relay_dual_listener PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(test_relay_dual_listener PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_dual_listener PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_dual_listener COMMAND test_relay_dual_listener)
    set_tests_properties(relay_dual_listener PROPERTIES LABELS "relay")
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
        tests/test_relay_verify_seam.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/snapshot.c)
    target_compile_definitions(test_relay_verify_seam PRIVATE
        MOQR_VERIFY_SEAM)
    target_include_directories(test_relay_verify_seam PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(test_relay_verify_seam PRIVATE moqr-admin)
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
        add_test(NAME relay_reap_wiring COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_reap_wiring.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c
            ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)
        set_tests_properties(relay_reap_wiring
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_dual_listener_wiring COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_dual_listener_wiring.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_dual_listener_wiring
            PROPERTIES LABELS "relay" TIMEOUT 30)
        # The production command and verify seam require the managed MsQuic
        # facade.  A relay-library-only build intentionally has neither
        # target, so do not register a test that demands them there.
        if(MOQ_BUILD_MSQUIC_MANAGED)
            add_test(NAME relay_production_targets_build COMMAND ${MOQR_WIRE_BASH}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_production_targets_build.sh
                ${CMAKE_BINARY_DIR})
            set_tests_properties(relay_production_targets_build
                PROPERTIES LABELS "relay" TIMEOUT 600)
        endif()
        add_test(NAME relay_snapshot_cap_oracle COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_snapshot_cap_oracle.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_snapshot_cap.cmake
            ${CMAKE_COMMAND})
        set_tests_properties(relay_snapshot_cap_oracle
            PROPERTIES LABELS "relay" TIMEOUT 60)
        add_test(NAME relay_admin_contracts COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_admin_contracts.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/admin/moqr_admin.h
            ${CMAKE_CURRENT_SOURCE_DIR}/admin/admin_sm.c
            ${CMAKE_CURRENT_SOURCE_DIR}/admin/http_parse.c)
        set_tests_properties(relay_admin_contracts
            PROPERTIES LABELS "relay;sansio" TIMEOUT 60)
        add_test(NAME relay_request_lifetime COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_request_lifetime.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_admin_listen.c)
        set_tests_properties(relay_request_lifetime
            PROPERTIES LABELS "relay" TIMEOUT 60)
        add_test(NAME relay_request_lifetime_selftest COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_request_lifetime_selftest.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_request_lifetime.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_admin_listen.c)
        set_tests_properties(relay_request_lifetime_selftest
            PROPERTIES LABELS "relay" TIMEOUT 60)
        add_test(NAME relay_clock_call_sites COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_clock_call_sites.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/admin_listen.h
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_admin_listen.c
            ${CMAKE_C_COMPILER}
            -I${CMAKE_CURRENT_SOURCE_DIR}/tests
            -I${CMAKE_CURRENT_SOURCE_DIR}/cli
            -I${CMAKE_CURRENT_SOURCE_DIR}/admin
            -I${MOQR_LIBRARY_INCLUDE_DIR}
            -I${PROJECT_SOURCE_DIR}/relay/private
            -I${CMAKE_SOURCE_DIR}/core/include)
        set_tests_properties(relay_clock_call_sites
            PROPERTIES LABELS "relay" TIMEOUT 60)
        add_test(NAME relay_listener_contracts COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_listener_contracts.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/admin_listen.c
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/admin_listen_layout.h
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c
            ${CMAKE_C_COMPILER}
            -I${CMAKE_CURRENT_SOURCE_DIR}/cli
            -I${CMAKE_CURRENT_SOURCE_DIR}/admin
            -I${MOQR_LIBRARY_INCLUDE_DIR}
            -I${CMAKE_SOURCE_DIR}/core/include)
        set_tests_properties(relay_listener_contracts
            PROPERTIES LABELS "relay" TIMEOUT 60)

        add_test(NAME relay_pump_overlap COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_pump_overlap.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_snapshot_cap.c
            ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)
        set_tests_properties(relay_pump_overlap
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_release_wake_consumed COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_release_wake_consumed.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_release_wake_consumed
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_broker_unwind COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_broker_unwind.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_broker_unwind
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_broker_init_order COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_broker_init_order.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_broker_init_order
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_signal_order COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_signal_order.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c)
        set_tests_properties(relay_signal_order
            PROPERTIES LABELS "relay" TIMEOUT 30)
        add_test(NAME relay_log_sink_wiring COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_log_sink_wiring.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cli/main.c
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_coord_seam.c)
        set_tests_properties(relay_log_sink_wiring
            PROPERTIES LABELS "relay" TIMEOUT 30)
        # Link hygiene: the relay libraries are one PUBLIC chain, so a target
        # names the highest member it needs; naming a lower one as well makes
        # the generator emit that archive twice and the linker warn.
        add_test(NAME relay_link_hygiene COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_link_hygiene.sh
            ${CMAKE_BINARY_DIR})
        set_tests_properties(relay_link_hygiene
            PROPERTIES LABELS "relay" TIMEOUT 60)
        # The gate reads generated commands, so a shape it cannot interpret
        # would report nothing at all: its verdicts are pinned against link
        # commands whose answer is known, including ones CMake itself
        # generates with a quoted archive path.
        # ninja is looked up rather than assumed from CMAKE_MAKE_PROGRAM,
        # which is the generator of THIS tree; without it the selftest says so
        # and runs the Makefile cases only.
        find_program(MOQR_SELFTEST_NINJA ninja)
        add_test(NAME relay_link_hygiene_selftest COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_link_hygiene_selftest.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_link_hygiene.sh
            ${CMAKE_COMMAND} ${MOQR_SELFTEST_NINJA})
        set_tests_properties(relay_link_hygiene_selftest
            PROPERTIES LABELS "relay" TIMEOUT 120)
        # The same cases under the SYSTEM shell. These scripts live outside
        # the directory scripts/selftest_all.sh sweeps, so nothing else runs
        # them on the interpreter floor -- and macOS still ships Bash 3.2 as
        # /bin/bash, which lacks constructs a newer Bash accepts silently.
        if(EXISTS /bin/bash)
            add_test(NAME relay_link_hygiene_selftest_system_shell
                COMMAND /bin/bash
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_link_hygiene_selftest.sh
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_link_hygiene.sh
                ${CMAKE_COMMAND} ${MOQR_SELFTEST_NINJA})
            set_tests_properties(relay_link_hygiene_selftest_system_shell
                PROPERTIES LABELS "relay" TIMEOUT 120)
        endif()
        add_test(NAME relay_test_inventory COMMAND ${MOQR_WIRE_BASH}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_test_inventory.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt
            ${MOQR_LIBRARY_TEST_DIR}/CMakeLists.txt
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/scenario_owners.md
            ${PROJECT_SOURCE_DIR})
        set_tests_properties(relay_test_inventory
            PROPERTIES LABELS "relay" TIMEOUT 30)
    endif()
    set_tests_properties(relay_cli_config PROPERTIES LABELS "relay")

    # Relay-owned renderer tests are registered by relay/tests.
    # The snapshot broker and the capability tri-state: pure state-machine
    # and render-composition behaviour, no transport and no clock.
    add_executable(test_relay_snapshot_cap
        tests/test_relay_snapshot_cap.c cli/snapshot.c cli/broker.c)
    target_link_libraries(test_relay_snapshot_cap PRIVATE moq::relay
        Threads::Threads)
    target_include_directories(test_relay_snapshot_cap PRIVATE
        cli ${MOQR_LIBRARY_INCLUDE_DIR})
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_snapshot_cap PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    # The ONLY registration: a CMake-script oracle requiring exit status zero,
    # exactly one completion receipt and exactly one success record. There is
    # deliberately no shell-availability branch -- a fallback that registered
    # the bare executable would restore precisely the false green the receipt
    # exists to prevent, and PASS_REGULAR_EXPRESSION would ignore the exit
    # status.
    add_test(NAME relay_snapshot_cap COMMAND ${CMAKE_COMMAND}
        -DMOQR_BIN=$<TARGET_FILE:test_relay_snapshot_cap>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_snapshot_cap.cmake)
    set_tests_properties(relay_snapshot_cap PROPERTIES LABELS "relay"
        TIMEOUT 60)

    # Authority over the GENERATED test graph. The source pins prove the
    # add_test text exists; this proves CMake evaluated it, that no required
    # test is DISABLED, and that the commands CTest will actually run are
    # exactly the intended ones.
    #
    # Registered unconditionally: the verifier is a `cmake -P` script and needs
    # no shell, so gating it on Bash would make the production check optional on
    # exactly the platforms that can least afford to lose it. MOQR_EXPECT_SELFTEST
    # carries the shell boundary instead -- the shell self-test is expected only
    # where a shell was found, while the production check is required either way.
    #
    # Every expected command element is passed explicitly. The verifier must not
    # infer identity from a basename pattern: a lookalike executable would
    # satisfy a containment rule while running something else entirely.
    if(MOQR_WIRE_BASH)
        set(MOQR_GRAPH_EXPECT_SELFTEST ON)
    else()
        set(MOQR_GRAPH_EXPECT_SELFTEST OFF)
    endif()
    add_test(NAME relay_ctest_graph COMMAND ${CMAKE_COMMAND}
        -DMOQR_BUILD_DIR=${CMAKE_BINARY_DIR}
        -DMOQR_CTEST=${CMAKE_CTEST_COMMAND}
        -DMOQR_CMAKE=${CMAKE_COMMAND}
        -DMOQR_PROD_BIN=$<TARGET_FILE:test_relay_snapshot_cap>
        -DMOQR_ORACLE=${CMAKE_CURRENT_SOURCE_DIR}/tests/run_snapshot_cap.cmake
        -DMOQR_SELFTEST_DRIVER=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_snapshot_cap_oracle.sh
        -DMOQR_GRAPH_SCRIPT=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_ctest_graph.cmake
        -DMOQR_PUMP_GUARD=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_pump_overlap.sh
        -DMOQR_PUMP_SUBJECT=${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_snapshot_cap.c
        -DMOQR_CMAKELISTS=${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt
        -DMOQR_BASH=${MOQR_WIRE_BASH}
        -DMOQR_EXPECT_SELFTEST=${MOQR_GRAPH_EXPECT_SELFTEST}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_ctest_graph.cmake)
    set_tests_properties(relay_ctest_graph
        PROPERTIES LABELS "relay" TIMEOUT 120)

    add_executable(test_relay_http_parse tests/test_relay_http_parse.c)
    target_link_libraries(test_relay_http_parse PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_http_parse PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_http_parse COMMAND test_relay_http_parse)
    set_tests_properties(relay_http_parse PROPERTIES LABELS "relay;sansio")

    add_executable(test_relay_admin_json tests/test_relay_admin_json.c)
    target_link_libraries(test_relay_admin_json PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_admin_json PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_admin_json COMMAND test_relay_admin_json)
    set_tests_properties(relay_admin_json PROPERTIES LABELS "relay;sansio")

    add_executable(test_relay_admin_sm tests/test_relay_admin_sm.c)
    target_link_libraries(test_relay_admin_sm PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_admin_sm PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_admin_sm COMMAND test_relay_admin_sm)
    set_tests_properties(relay_admin_sm PROPERTIES LABELS "relay;sansio")

    add_executable(test_relay_admin_seeded tests/test_relay_admin_seeded.c)
    target_link_libraries(test_relay_admin_seeded PRIVATE moqr-admin)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_admin_seeded PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_admin_seeded COMMAND test_relay_admin_seeded)
    set_tests_properties(relay_admin_seeded PROPERTIES
        LABELS "relay;sansio;seeded" TIMEOUT 120)

    # The only test in this tier that opens a socket. It exercises the real
    # listener, a real owner thread and the real frozen state machine over
    # loopback. Keep that wall-clock boundary serial; everything else about
    # the admin tier is proven sans-I/O and remains parallel-safe.
    add_executable(test_relay_admin_listen
        tests/test_relay_admin_listen.c cli/admin_listen.c
        cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/broker.c)
    # The listener's deterministic failure seams (clock, poll, join). Compiled
    # only into this target's own copy; the shipped object has no such symbols.
    target_compile_definitions(test_relay_admin_listen PRIVATE
        MOQR_ADMIN_LISTEN_TESTING)
    target_link_libraries(test_relay_admin_listen PRIVATE moqr-admin
        Threads::Threads)
    target_include_directories(test_relay_admin_listen PRIVATE cli)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_admin_listen PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_admin_listen COMMAND test_relay_admin_listen)
    set_tests_properties(relay_admin_listen PROPERTIES
        LABELS "relay;boundary" TIMEOUT 180 RUN_SERIAL TRUE)

    add_executable(test_relay_admin_broker
        tests/test_relay_admin_broker.c cli/broker.c)
    target_link_libraries(test_relay_admin_broker PRIVATE moqr-admin
        Threads::Threads)
    target_include_directories(test_relay_admin_broker PRIVATE cli)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_admin_broker PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_admin_broker COMMAND test_relay_admin_broker)
    set_tests_properties(relay_admin_broker PROPERTIES LABELS "relay;sansio")

    if(TARGET moq-relay-bind-test-internals AND
       TARGET moq-relay-shard-test-internals)
        # Cross-shard authorization is a CLI composition test because it also
        # consumes the blocked-state renderer. The bind and shard test objects
        # themselves are owned and built by relay/tests.
        add_executable(test_relay_auth_shards tests/test_relay_auth_shards.c
            cli/blockedstats.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>)
        target_compile_definitions(test_relay_auth_shards PRIVATE
            MOQR_BIND_TESTING)
        target_link_libraries(test_relay_auth_shards PRIVATE
            moq::relay moq::sim)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_auth_shards PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_auth_shards COMMAND test_relay_auth_shards)
        set_tests_properties(relay_auth_shards PROPERTIES TIMEOUT 120
            LABELS "relay")
    endif()

endif()

# Relay-owned benchmarks are defined by relay/bench.

# The relay executable: needs the MsQuic managed server (the production
# transport). The core, binding, and every test above build with zero
# transport deps. Without the msquic-managed target the executable is simply
# not built — there is no picoquic fallback.
if(NOT TARGET moq::adapter-msquic-managed)
    message(STATUS
        "moq5-relay executable disabled: enable MOQ_BUILD_ADAPTER_MSQUIC=ON "
        "and MOQ_BUILD_MSQUIC_MANAGED=ON with a discoverable MsQuic package "
        "(msquic_DIR). Relay libraries and provider-free tests remain available.")
endif()
if(TARGET moq::adapter-msquic-managed)
    add_executable(moq5-relay cli/main.c cli/admin_listen.c cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c
        cli/lanestats.c cli/logevent.c cli/servelog.c cli/snapshot.c cli/broker.c cli/conn_reap.c)
    target_include_directories(moq5-relay PRIVATE
        cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
    target_link_libraries(moq5-relay PRIVATE moqr-admin
        moq::adapter-msquic-managed)
    # The browser-facing listener is additive: the command keeps building and
    # behaving exactly as before for downstreams that have no wtquic, and gains
    # the WebTransport listener only where BOTH managed facades exist.
    if(TARGET moq::adapter-wtquic-msquic-managed)
        target_link_libraries(moq5-relay PRIVATE
            moq::adapter-wtquic-msquic-managed)
        target_compile_definitions(moq5-relay PRIVATE MOQR_DUAL_LISTENER)
        # The construction-and-create step depends on the WebTransport header,
        # so it joins the build only where that facade does.
        target_sources(moq5-relay PRIVATE cli/wtcfg.c)
    endif()

    # What the WebTransport transport is handed, captured at the create call.
    #
    # The production step is compiled unchanged; only its two transport callees
    # are replaced, so no transport, socket or thread is involved. It takes the
    # facade's header USAGE REQUIREMENTS without linking the facade, so the
    # substitutions are the only definitions of those two symbols and no
    # transport library reaches the executable.
    if(MOQ_BUILD_TESTS AND TARGET moq::adapter-wtquic-msquic-managed)
        add_executable(test_relay_wt_cfg
            tests/test_relay_wt_cfg.c cli/wtcfg.c cli/config.c
            cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c
            cli/snapshot.c)
        target_include_directories(test_relay_wt_cfg PRIVATE
            cli tests ${CMAKE_SOURCE_DIR}/media/msf/vendor
            $<TARGET_PROPERTY:moq::adapter-wtquic-msquic-managed,INTERFACE_INCLUDE_DIRECTORIES>)
        target_compile_options(test_relay_wt_cfg PRIVATE
            -include ${CMAKE_CURRENT_SOURCE_DIR}/tests/wtcfg_substitute.h)
        target_link_libraries(test_relay_wt_cfg PRIVATE moqr-admin)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_wt_cfg PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_wt_cfg COMMAND test_relay_wt_cfg)
        set_tests_properties(relay_wt_cfg PROPERTIES LABELS "relay;sansio")
        # Whether that target exists at all is a build-option decision, and
        # only the generated build system can settle it. Configure-only.
        if(MOQR_WIRE_BASH)
            # The child configures are handed the package directories THIS
            # build already resolved. A caller may have found its dependencies
            # through a search list, through explicit package directories with
            # no search list at all, or through several prefixes; the resolved
            # directories are the one input that means the same thing in every
            # case. The search list follows as supplemental input, quoted so
            # its ";" survives as CMake list separators.
            add_test(NAME relay_wt_cfg_target_gating COMMAND ${MOQR_WIRE_BASH}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_wt_cfg_target_gating.sh
                ${CMAKE_SOURCE_DIR}
                ${CMAKE_COMMAND}
                "${wtquic_DIR}"
                "${msquic_DIR}"
                "${CMAKE_C_COMPILER}"
                "${CMAKE_PREFIX_PATH}"
                "${OPENSSL_ROOT_DIR}")
            set_tests_properties(relay_wt_cfg_target_gating PROPERTIES
                LABELS "relay;tooling" TIMEOUT 300)
        endif()
    endif()

    # What ships is the command and the material needed to operate it.
    # Relay archives and the moq/relay/ headers are build-tree interfaces, not a
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
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/docs/moq5-relay.json.5
            DESTINATION ${CMAKE_INSTALL_MANDIR}/man5
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
        # The external prebuilt libraries the command loads but does not
        # install. MsQuic is always one; the WebTransport backend is another
        # wherever that facade is linked.
        set(MOQR_INSTALL_SURFACE_DEPLIBS "$<TARGET_FILE_DIR:msquic::msquic>")
        if(TARGET wtq::msquic)
            get_target_property(_wtq_type wtq::msquic TYPE)
            if(NOT _wtq_type STREQUAL "INTERFACE_LIBRARY")
                string(APPEND MOQR_INSTALL_SURFACE_DEPLIBS
                       ":$<TARGET_FILE_DIR:wtq::msquic>")
            endif()
        endif()
        add_test(NAME relay_install_surface COMMAND ${CMAKE_COMMAND}
            -DCMD=$<TARGET_FILE:moq5-relay>
            -DSRCROOT=${CMAKE_SOURCE_DIR}
            -DBUILDDIR=${CMAKE_BINARY_DIR}
            -DGUARD=${CMAKE_CURRENT_SOURCE_DIR}/tests/check_install_surface.sh
            -DBASH=${MOQR_WIRE_BASH}
            -DDEPLIB=${MOQR_INSTALL_SURFACE_DEPLIBS}
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
        add_executable(moq-relay-verify cli/main.c cli/admin_listen.c cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c
            cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c cli/snapshot.c cli/broker.c cli/conn_reap.c
            cli/blockedstats.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>)
        target_compile_definitions(moq-relay-verify PRIVATE
            MOQR_BIND_TESTING MOQR_VERIFY_SEAM)
        target_include_directories(moq-relay-verify PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(moq-relay-verify PRIVATE moqr-admin
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
        add_executable(moq-relay-measure cli/main.c cli/admin_listen.c cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c
            cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c cli/snapshot.c cli/broker.c cli/conn_reap.c)
        target_compile_definitions(moq-relay-measure PRIVATE MOQR_VERIFY_SEAM)
        target_include_directories(moq-relay-measure PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(moq-relay-measure PRIVATE moqr-admin
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
        # debug seam of any tier. The two production libraries are checked
        # before dead stripping can hide a leak. All five arguments are mandatory.
        if(MOQ_BUILD_TESTS)
            find_program(MOQ_BASH bash)
            if(MOQ_BASH)
                add_test(NAME relay_verify_hygiene COMMAND ${MOQ_BASH}
                    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/relay_verify_hygiene.sh
                    $<TARGET_FILE:moq5-relay> $<TARGET_FILE:moq-relay-verify>
                    $<TARGET_FILE:moq-relay-measure>
                    $<TARGET_FILE:moq-relay-core>
                    $<TARGET_FILE:moq-relay>)
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
                       '$<TARGET_FILE:moq-relay>'")
                set_tests_properties(relay_verify_hygiene_rejects_leaked_symbol
                    PROPERTIES LABELS "relay" TIMEOUT 30 WILL_FAIL TRUE)
            endif()
        endif()
        if(MOQR_WIRE_BASH)
            add_test(NAME relay_serve_log_refusal COMMAND ${MOQR_WIRE_BASH}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_serve_log_refusal.sh
                $<TARGET_FILE:moq-relay-measure> $<TARGET_FILE:moq-relay-verify>)
            set_tests_properties(relay_serve_log_refusal
                PROPERTIES LABELS "relay" TIMEOUT 60)
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
            cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/snapshot.c cli/conn_reap.c)
        target_include_directories(test_relay_loopback PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(test_relay_loopback PRIVATE moqr-admin
            moq::adapter-msquic-managed moq-relay-inspect)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_loopback PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        # Minimum real-QUIC boundary smokes: the SAME scenario functions
        # via the fail-closed selector, one run each. Protocol/scheduler
        # behavior is proven sans-I/O; these prove the adapter wiring.
        # The one real-transport boundary for the dual listener: an object
        # published over one transport reaching a subscriber on the other,
        # through the shipping pumps and ONE shard runtime. Built only where
        # both managed facades exist.
        if(TARGET moq::adapter-wtquic-msquic-managed)
            add_executable(test_relay_dual_loopback
                tests/test_relay_dual_loopback.c
                tests/dual_loopback_cli_tu.c cli/admin_listen.c
                cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c
                cli/lanestats.c cli/logevent.c cli/servelog.c cli/snapshot.c cli/broker.c cli/conn_reap.c
                cli/wtcfg.c)
            target_include_directories(test_relay_dual_loopback PRIVATE
                cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
            target_compile_definitions(test_relay_dual_loopback PRIVATE
                MOQR_PUMP_TESTING MOQR_DUAL_LISTENER)
            target_link_libraries(test_relay_dual_loopback PRIVATE moqr-admin
                moq::adapter-msquic-managed moq::adapter-wtquic-msquic-managed)
            if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
                target_compile_options(test_relay_dual_loopback PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            # Capacity must describe the process serve will run: a dual
            # 1+1 config is the multi-shard composition, not the direct model.
            if(MOQR_WIRE_BASH)
                add_test(NAME relay_dual_capacity COMMAND ${MOQR_WIRE_BASH}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_dual_capacity.sh
                    $<TARGET_FILE:moq5-relay>
                    $<TARGET_FILE:moq-relay-verify>)
                set_tests_properties(relay_dual_capacity
                    PROPERTIES LABELS "relay" TIMEOUT 60)
                # What the gate above accepts as the seam's single-lane
                # refusal, driven by captured statuses and messages plus
                # substitute tools. The classifier decides what a real run
                # proved, so a broad one would read an unrelated failure as
                # evidence the guard still holds.
                add_test(NAME relay_verify_refusal_selftest
                    COMMAND ${MOQR_WIRE_BASH}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_verify_refusal_selftest.sh
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_dual_capacity.sh
                    $<TARGET_FILE:moq5-relay>
                    $<TARGET_FILE:moq-relay-verify>)
                set_tests_properties(relay_verify_refusal_selftest PROPERTIES
                    LABELS "relay;sansio" TIMEOUT 120
                    ENVIRONMENT "BASH=${MOQR_WIRE_BASH}")
            endif()
            # Live SIGTERM against a relay running both listeners: the
            # source guard pins the order, this proves the process honours it.
            if(MOQR_WIRE_BASH)
                add_test(NAME relay_dual_sigterm COMMAND ${MOQR_WIRE_BASH}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_dual_sigterm.sh
                    $<TARGET_FILE:moq5-relay>
                    ${MOQR_TEST_CERT} ${MOQR_TEST_KEY})
                set_tests_properties(relay_dual_sigterm
                    PROPERTIES LABELS "relay" TIMEOUT 120)
            endif()
            add_test(NAME relay_dual_loopback_raw_to_wt
                COMMAND test_relay_dual_loopback
                    ${MOQR_TEST_CERT} ${MOQR_TEST_KEY} raw_to_wt)
            set_tests_properties(relay_dual_loopback_raw_to_wt
                PROPERTIES LABELS "relay" TIMEOUT 120)
            add_test(NAME relay_dual_loopback_wt_to_raw
                COMMAND test_relay_dual_loopback
                    ${MOQR_TEST_CERT} ${MOQR_TEST_KEY} wt_to_raw)
            set_tests_properties(relay_dual_loopback_wt_to_raw
                PROPERTIES LABELS "relay" TIMEOUT 120)
        endif()
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
            tests/test_relay_multiversion.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c
            cli/snapshot.c cli/conn_reap.c)
        target_include_directories(test_relay_multiversion PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor)
        target_link_libraries(test_relay_multiversion PRIVATE moqr-admin
            moq::adapter-msquic-managed moq-relay-inspect)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_multiversion PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()

        # Every production nonzero-return branch of the lane pumps,
        # activated one at a time through the REAL cli/main.c callbacks
        # (main renamed away) over the sans-I/O facade.
        # The production coordinator's generation lifecycle, driven directly
        # through a seam over the real main.c objects: produce, freeze, take
        # the token, dispatch by frozen demand, release. Built WITHOUT
        # MOQR_BIND_TESTING so it exercises the normal coordinator.
        add_executable(test_relay_coord_seam
            tests/test_relay_coord_seam.c
            tests/coord_seam_cli_tu.c cli/admin_listen.c
            cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
            cli/snapshot.c cli/broker.c cli/conn_reap.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
        target_include_directories(test_relay_coord_seam PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor
            ${CMAKE_SOURCE_DIR}/adapters/msquic
            ${CMAKE_SOURCE_DIR}/adapters/msquic/include
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests)
        target_compile_definitions(test_relay_coord_seam PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING MOQR_PUMP_TESTING)
        target_link_libraries(test_relay_coord_seam PRIVATE moqr-admin
            msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_coord_seam PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_coord_seam COMMAND test_relay_coord_seam)
        set_tests_properties(relay_coord_seam PROPERTIES
            LABELS "relay;sansio" TIMEOUT 60)

        add_executable(test_relay_admin_halt
            tests/test_relay_admin_halt.c cli/admin_listen.c
            cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
            cli/snapshot.c cli/broker.c cli/conn_reap.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
        target_include_directories(test_relay_admin_halt PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor
            ${CMAKE_SOURCE_DIR}/adapters/msquic
            ${CMAKE_SOURCE_DIR}/adapters/msquic/include
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests)
        target_compile_definitions(test_relay_admin_halt PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING MOQR_PUMP_TESTING)
        target_link_libraries(test_relay_admin_halt PRIVATE moqr-admin
            msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_admin_halt PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_admin_halt COMMAND test_relay_admin_halt)
        set_tests_properties(relay_admin_halt PROPERTIES
            LABELS "relay" TIMEOUT 120)

        # The same seam against the VERIFY coordinator: it must ride the
        # identical generation lifecycle, not render and walk away.
        add_executable(test_relay_coord_seam_verify
            tests/test_relay_coord_seam.c
            tests/coord_seam_cli_tu.c cli/admin_listen.c
            cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
            cli/snapshot.c cli/broker.c cli/conn_reap.c cli/blockedstats.c
            $<TARGET_OBJECTS:moq-relay-bind-test-internals>
            $<TARGET_OBJECTS:moq-relay-shard-test-internals>
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
        target_include_directories(test_relay_coord_seam_verify PRIVATE
            cli ${CMAKE_SOURCE_DIR}/media/msf/vendor
            ${CMAKE_SOURCE_DIR}/adapters/msquic
            ${CMAKE_SOURCE_DIR}/adapters/msquic/include
            ${CMAKE_SOURCE_DIR}/adapters/msquic/tests)
        target_compile_definitions(test_relay_coord_seam_verify PRIVATE
            MOQ_MSQUIC_TESTING MOQ_BUILDING
            MOQR_PUMP_TESTING MOQR_BIND_TESTING)
        target_link_libraries(test_relay_coord_seam_verify PRIVATE moqr-admin
            msquic::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_relay_coord_seam_verify PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME relay_coord_seam_verify
            COMMAND test_relay_coord_seam_verify)
        set_tests_properties(relay_coord_seam_verify PROPERTIES
            LABELS "relay;sansio" TIMEOUT 60)

        add_executable(test_relay_pump_branches
            tests/test_relay_pump_branches.c
            tests/pump_branches_cli_tu.c cli/admin_listen.c
            cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
            cli/snapshot.c cli/broker.c cli/conn_reap.c cli/blockedstats.c
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
        target_link_libraries(test_relay_pump_branches PRIVATE moqr-admin
            msquic::msquic Threads::Threads)
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
        target_link_libraries(test_relay_lane_boundary PRIVATE moq::relay
            msquic::msquic Threads::Threads)
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
        target_link_libraries(test_relay_lane_lifecycle PRIVATE moq::relay
            msquic::msquic Threads::Threads)
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
    # Relay-owned benchmark tests are registered by relay/bench.
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
        tests/pump_branches_cli_tu.c cli/admin_listen.c
        cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
        cli/snapshot.c cli/broker.c cli/conn_reap.c cli/blockedstats.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>
        $<TARGET_OBJECTS:moq-relay-shard-test-internals>
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
    target_include_directories(test_relay_seedx_scripts PRIVATE
        cli tests ${MOQR_LIBRARY_TEST_DIR}
        ${CMAKE_SOURCE_DIR}/media/msf/vendor
        ${CMAKE_SOURCE_DIR}/adapters/msquic
        ${CMAKE_SOURCE_DIR}/adapters/msquic/include
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests
        ${CMAKE_SOURCE_DIR}/tests/support)
    target_compile_definitions(test_relay_seedx_scripts PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING
        MOQR_PUMP_TESTING MOQR_BIND_TESTING)
    target_link_libraries(test_relay_seedx_scripts PRIVATE moqr-admin
        msquic::msquic Threads::Threads)
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
        tests/pump_branches_cli_tu.c cli/admin_listen.c
        cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
        cli/snapshot.c cli/broker.c cli/conn_reap.c cli/blockedstats.c
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
    target_link_libraries(test_relay_mixed_draft PRIVATE moqr-admin
        msquic::msquic Threads::Threads)
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
        ${MOQR_LIBRARY_TEST_DIR}/seedx/seedx_trace.c
        tests/pump_branches_cli_tu.c cli/admin_listen.c
        cli/cliargs.c cli/config.c cli/admin_listen_size.c cli/info_doc.c cli/shards_doc.c cli/versions.c cli/lanestats.c cli/logevent.c cli/servelog.c
        cli/snapshot.c cli/broker.c cli/conn_reap.c cli/blockedstats.c
        $<TARGET_OBJECTS:moq-relay-bind-test-internals>
        $<TARGET_OBJECTS:moq-relay-shard-test-internals>
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_adapter.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/msquic_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_managed.c
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests/support/fake_msq_table.c)
    target_include_directories(test_relay_sched_explore PRIVATE
        cli tests ${MOQR_LIBRARY_TEST_DIR}
        ${CMAKE_SOURCE_DIR}/media/msf/vendor
        ${CMAKE_SOURCE_DIR}/adapters/msquic
        ${CMAKE_SOURCE_DIR}/adapters/msquic/include
        ${CMAKE_SOURCE_DIR}/adapters/msquic/tests
        ${CMAKE_SOURCE_DIR}/tests/support)
    target_compile_definitions(test_relay_sched_explore PRIVATE
        MOQ_MSQUIC_TESTING MOQ_BUILDING
        MOQR_PUMP_TESTING MOQR_BIND_TESTING)
    target_link_libraries(test_relay_sched_explore PRIVATE moqr-admin
        msquic::msquic Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_relay_sched_explore PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME relay_sched_seeded COMMAND test_relay_sched_explore --seeds)
    set_tests_properties(relay_sched_seeded PROPERTIES TIMEOUT 30
        ENVIRONMENT "SCHED_MANIFEST=${MOQR_LIBRARY_TEST_DIR}/vectors/sched_seeds_v1.txt")
    add_test(NAME relay_sched_cli_units
             COMMAND test_relay_sched_explore --cli-units)
    set_tests_properties(relay_sched_cli_units PROPERTIES TIMEOUT 30)
    add_test(NAME relay_sched_close_feed
             COMMAND test_relay_sched_explore --close-feed)
    set_tests_properties(relay_sched_close_feed PROPERTIES TIMEOUT 30)
endif()

# Relay-owned deterministic seed/model units are registered by relay/tests.

# -- tool-owned merge-lane labels: one authoritative pass --------------------
# The last property setting wins at generate time, so every relay test's lane
# membership is decided in its owning directory. This list owns tool tests;
# relay/tests and relay/bench own the library and benchmark lists. Lanes are
# sansio (exact deterministic), boundary (irreducible physical surface, run
# once each), and tooling (deterministic tool selftests).
set(MOQR_TESTS_SANSIO
    relay_boundary relay_lane_stats
    relay_blocked_stats relay_verify_seam relay_cli_config
    relay_serve_version_wiring relay_reap_wiring relay_test_inventory
    relay_signal_order
    relay_cli_args
    relay_cli_surface relay_install_surface
    relay_auth_shards
    relay_verify_hygiene relay_verify_hygiene_rejects_missing_archive
    relay_verify_hygiene_rejects_leaked_symbol relay_pump_branches
    relay_lane_lifecycle relay_seedx_scripts
    relay_sched_cli_units relay_sched_close_feed
    relay_multipub relay_readmission)
set(MOQR_TESTS_BOUNDARY
    relay_lane_boundary relay_multiversion relay_loopback_same_d18
    relay_loopback_cross_d18)
set(MOQR_TESTS_TOOLING
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
        ENVIRONMENT "SCHED_MANIFEST=${MOQR_LIBRARY_TEST_DIR}/vectors/sched_seeds_v1.txt")
endif()

# Trust root for the configured-test-graph authority.
#
# relay_ctest_graph audits the generated CTest graph, but a test cannot be the
# sole proof that it was not removed from CTest. Conditioning it away or marking
# it DISABLED is invisible to every check that only runs as part of that same
# graph: `ctest` then reports a fully green run with no authority present.
#
# This assertion runs at configure time, before any test graph is executed, and
# fails the build outright. It reads the directory's actual test set and each
# test's final DISABLED property, so a later property override is caught as well
# as a removed registration.
#
# It is invoked through cmake_language(DEFER) so it runs at the very end of this
# directory's processing rather than at this line. Asserting at the call site
# would only cover declarations written above it, leaving a property override
# appended afterwards free to disable a required test unseen.
#
# Its own presence is pinned by tests/check_pump_overlap.sh.
function(moqr_assert_authority_chain)
    get_property(configured DIRECTORY PROPERTY TESTS)
    set(required relay_ctest_graph relay_snapshot_cap)
    if(MOQR_WIRE_BASH)
        list(APPEND required relay_snapshot_cap_oracle relay_pump_overlap)
    endif()
    foreach(t IN LISTS required)
        if(NOT t IN_LIST configured)
            message(FATAL_ERROR
                "required test ${t} is not registered in the configured test "
                "graph -- the snapshot-cap authority chain is incomplete")
        endif()
        get_test_property(${t} DISABLED disabled)
        if(disabled)
            message(FATAL_ERROR
                "required test ${t} is registered but DISABLED -- the "
                "snapshot-cap authority chain is incomplete")
        endif()
    endforeach()

    # Execute the source pin here, not merely require that a CTest entry for it
    # exists. Requiring the entry authenticates its name; it does not
    # authenticate its command, so a registration rewritten to a no-op would
    # leave the pin nominally present and entirely inert.
    #
    # This breaks the trust cycle. "The graph authority checks the pin, and the
    # pin checks the graph authority" is circular while both are ordinary CTest
    # entries: rewriting either command satisfies the other. Running the pin
    # from configure gives it an authority that does not pass through the test
    # graph at all, so its verdict survives any rewrite of how CTest invokes it.
    if(MOQR_WIRE_BASH)
        execute_process(
            COMMAND "${MOQR_WIRE_BASH}"
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_pump_overlap.sh"
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_relay_snapshot_cap.c"
                    "${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt"
            RESULT_VARIABLE pin_status
            OUTPUT_VARIABLE pin_out
            ERROR_VARIABLE pin_err)
        if(NOT pin_status EQUAL 0)
            message(FATAL_ERROR
                "the snapshot-cap source pin refused this tree at configure "
                "time:\n${pin_out}${pin_err}")
        endif()
    endif()
endfunction()

if(MOQ_BUILD_TESTS)
    cmake_language(DEFER CALL moqr_assert_authority_chain)
endif()
