if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    add_compile_options(-Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

add_test(NAME cmake_apple_abi_rules
    COMMAND ${CMAKE_COMMAND}
        "-DTEST_OUTPUT=${CMAKE_CURRENT_BINARY_DIR}/apple-abi-rules"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/AppleAbiProbeRulesTest.cmake")
add_test(NAME cmake_picoquic_pkgconfig_provenance
    COMMAND ${CMAKE_COMMAND} "-DSRC=${PROJECT_SOURCE_DIR}"
        "-DWORK=${CMAKE_CURRENT_BINARY_DIR}/picoquic-pkgconfig-provenance"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PicoquicPkgConfigProvenanceTest.cmake")
add_test(NAME cmake_consumer_preload_argv
    COMMAND ${CMAKE_COMMAND} "-DCTEST=${CMAKE_CTEST_COMMAND}"
        "-DTEST_BUILD=${PROJECT_BINARY_DIR}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ConsumerPreloadArgvTest.cmake")
add_test(NAME cmake_consumer_abi_bundle
    COMMAND ${CMAKE_COMMAND}
        "-DTEST_OUTPUT=${CMAKE_CURRENT_BINARY_DIR}/consumer-abi-bundle"
        "-DTEST_GENERATOR=${CMAKE_GENERATOR}"
        "-DTEST_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ConsumerAbiBundleTest.cmake")
set_tests_properties(cmake_consumer_abi_bundle PROPERTIES TIMEOUT 60)

# Installed package modules reach siblings through ${CMAKE_CURRENT_LIST_DIR}
# only at a downstream configure, so check the installed set itself.
include(GNUInstallDirs)
add_test(NAME cmake_install_module_siblings
    COMMAND ${CMAKE_COMMAND}
        "-DMOQ_BUILD=${PROJECT_BINARY_DIR}"
        "-DMOQ_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/install-module-siblings"
        "-DMOQ_LIBDIR=${CMAKE_INSTALL_LIBDIR}"
        "-DMOQ_CONFIG=$<CONFIG>"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckInstalledModuleSiblings.cmake")
set_tests_properties(cmake_install_module_siblings PROPERTIES TIMEOUT 300)

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/ConsumerBuildContext.cmake")

# The OpenSSL selection test-owned children receive. Placeholder library files
# are named for static archives on Unix-like hosts.
if(NOT WIN32)
    set(_provider_context_toolchain
        "${CMAKE_CURRENT_BINARY_DIR}/consumer-provider-context-toolchain.cmake")
    moq_write_consumer_context("${_provider_context_toolchain}")
    add_test(NAME cmake_consumer_provider_context
        COMMAND ${CMAKE_COMMAND}
            "-DMOQ_SOURCE=${PROJECT_SOURCE_DIR}"
            "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/consumer-provider-context"
            "-DMOQ_CONTEXT=${_provider_context_toolchain}"
            "-DMOQ_GENERATOR=${CMAKE_GENERATOR}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ConsumerProviderContextTest.cmake")
    set_tests_properties(cmake_consumer_provider_context PROPERTIES TIMEOUT 120)
endif()

# FindPicoquic's picotls selection from MOQ_PICOTLS_PREFIX, including
# reconfigures and explicit overrides. Placeholder library files are named
# for static archives on Unix-like hosts.
if(NOT WIN32)
    set(_picotls_selection_context
        "${CMAKE_CURRENT_BINARY_DIR}/picotls-prefix-selection-context.cmake")
    moq_write_consumer_context("${_picotls_selection_context}")
    # The adapter cases need OpenSSL for the verifier; use the one this
    # build found, if any.
    set(_picotls_selection_openssl "")
    if(OPENSSL_ROOT_DIR)
        set(_picotls_selection_openssl "${OPENSSL_ROOT_DIR}")
    elseif(OPENSSL_INCLUDE_DIR)
        get_filename_component(_picotls_selection_openssl
            "${OPENSSL_INCLUDE_DIR}/.." ABSOLUTE)
    endif()
    add_test(NAME cmake_picotls_prefix_selection
        COMMAND ${CMAKE_COMMAND}
            "-DMOQ_SOURCE=${PROJECT_SOURCE_DIR}"
            "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/picotls-prefix-selection"
            "-DMOQ_GENERATOR=${CMAKE_GENERATOR}"
            "-DMOQ_CONTEXT=${_picotls_selection_context}"
            "-DMOQ_OPENSSL_ROOT=${_picotls_selection_openssl}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PicotlsPrefixSelectionTest.cmake")
    set_tests_properties(cmake_picotls_prefix_selection PROPERTIES TIMEOUT 300)
endif()

# libmoq as a subproject: a superproject add_subdirectory()s this checkout
# with the same options and provider hints, and no libmoq path may resolve
# against the superproject's roots.
set(_superproject_context "${CMAKE_CURRENT_BINARY_DIR}/superproject-context.cmake")
moq_write_consumer_context("${_superproject_context}")
get_cmake_property(_superproject_cache CACHE_VARIABLES)
foreach(_var IN LISTS _superproject_cache)
    get_property(_type CACHE "${_var}" PROPERTY TYPE)
    if(_type STREQUAL "INTERNAL" OR _type STREQUAL "STATIC")
        continue()
    endif()
    if(_var MATCHES "^(MOQ_|PTLS_|OPENSSL_)" OR _var MATCHES "^[A-Za-z0-9]+_DIR$")
        file(APPEND "${_superproject_context}"
            "set(${_var} [==[${${_var}}]==] CACHE STRING \"Parent build option\")\n")
    endif()
endforeach()
add_test(NAME cmake_superproject_subdirectory
    COMMAND ${CMAKE_COMMAND}
        "-DMOQ_SOURCE=${PROJECT_SOURCE_DIR}"
        "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/superproject-subdirectory"
        "-DMOQ_CONTEXT=${_superproject_context}"
        "-DMOQ_GENERATOR=${CMAKE_GENERATOR}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/SuperprojectSubdirectory.cmake")
set_tests_properties(cmake_superproject_subdirectory PROPERTIES TIMEOUT 600)

# Tests on, moq-sim off: SimPair-driven tests stay behind if(TARGET moq::sim).
set(_sim_off_context "${CMAKE_CURRENT_BINARY_DIR}/sim-off-context.cmake")
moq_write_consumer_context("${_sim_off_context}")
add_test(NAME cmake_sim_off_configure
    COMMAND ${CMAKE_COMMAND}
        "-DMOQ_SOURCE=${PROJECT_SOURCE_DIR}"
        "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/sim-off-configure"
        "-DMOQ_CONTEXT=${_sim_off_context}"
        "-DMOQ_GENERATOR=${CMAKE_GENERATOR}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/SimOffConfigure.cmake")
set_tests_properties(cmake_sim_off_configure PROPERTIES TIMEOUT 300)

# Source-mode downstream of an installed prefix: the installed FindPicoquic
# adds picoquic from MOQ_PICOQUIC_SOURCE_DIR and hooks its project().
if(MOQ_PICOQUIC_SOURCE_DIR AND (MOQ_BUILD_ADAPTER_PICOQUIC OR MOQ_BUILD_ADAPTER_PICO_WT))
    if(MOQ_BUILD_ADAPTER_PICOQUIC)
        set(_pq_source_component adapter-picoquic)
    else()
        set(_pq_source_component adapter-pico-wt)
    endif()
    set(_pq_source_context
        "${CMAKE_CURRENT_BINARY_DIR}/picoquic-source-install-context.cmake")
    moq_write_consumer_context("${_pq_source_context}")
    # picotls is named by MOQ_PICOTLS_PREFIX alone; the consumer checks that
    # FindPicoquic resolves its headers and libraries from that prefix.
    foreach(_dep IN ITEMS OPENSSL_ROOT_DIR OPENSSL_INCLUDE_DIR
                          OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
        if(${_dep})
            file(APPEND "${_pq_source_context}"
                "set(${_dep} [==[${${_dep}}]==] CACHE STRING \"Provider hint\")\n")
        endif()
    endforeach()
    add_test(NAME picoquic_source_mode_install_consumer
        COMMAND ${CMAKE_COMMAND}
            "-DMOQ_BUILD=${PROJECT_BINARY_DIR}"
            "-DMOQ_LIBDIR=${CMAKE_INSTALL_LIBDIR}"
            "-DMOQ_CONFIG=$<CONFIG>"
            "-DMOQ_GENERATOR=${CMAKE_GENERATOR}"
            "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/picoquic-source-install-consumer"
            "-DMOQ_CONSUMER_SRC=${CMAKE_CURRENT_SOURCE_DIR}/cmake/picoquic_source_install_consumer"
            "-DMOQ_CONSUMER_COMPONENT=${_pq_source_component}"
            "-DMOQ_CONSUMER_CONTEXT=${_pq_source_context}"
            "-DMOQ_PICOQUIC_SOURCE_DIR=${MOQ_PICOQUIC_SOURCE_DIR}"
            "-DMOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PicoquicSourceInstallConsumer.cmake")
    set_tests_properties(picoquic_source_mode_install_consumer PROPERTIES TIMEOUT 600)
endif()

# Loopback-test fixture: holds IPv4-only UDP sockets on much of the
# ephemeral range while a test runs (see support/port_shadow.c). Adapter,
# service and relay trees wrap their loopback tests in it.
if(NOT WIN32)
    add_executable(moq_port_shadow support/port_shadow.c)
endif()

# FindPicoquic's one OpenSSL link identity, against a stand-in provider.
add_test(NAME picoquic_openssl_link_identity
    COMMAND ${CMAKE_COMMAND}
        "-DMOQ_SOURCE=${PROJECT_SOURCE_DIR}"
        "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/picoquic-openssl-identity"
        "-DMOQ_GENERATOR=${CMAKE_GENERATOR}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PicoquicOpenSSLIdentityTest.cmake")
set_tests_properties(picoquic_openssl_link_identity PROPERTIES TIMEOUT 120)

# -- C tests --------------------------------------------------------
add_test(NAME picoquic_version_metadata
    COMMAND ${CMAKE_COMMAND}
        "-DMOQ_SOURCE=${PROJECT_SOURCE_DIR}"
        "-DMOQ_WORK=${CMAKE_CURRENT_BINARY_DIR}/picoquic-version-metadata"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PicoquicVersionMetadata.cmake")
set_tests_properties(picoquic_version_metadata PROPERTIES TIMEOUT 30)

add_executable(test_version unit/test_version.c)
target_link_libraries(test_version PRIVATE moq::core)
add_test(NAME version COMMAND test_version)

add_executable(test_fill_identity_allocator unit/test_fill_identity_allocator.c)
target_link_libraries(test_fill_identity_allocator PRIVATE moq-core-test-internals)
target_compile_definitions(test_fill_identity_allocator PRIVATE MOQ_STATIC)
add_test(NAME fill_identity_allocator COMMAND test_fill_identity_allocator)
set_tests_properties(fill_identity_allocator PROPERTIES TIMEOUT 30)

add_executable(test_outbound_observation unit/test_outbound_observation.c)
target_link_libraries(test_outbound_observation PRIVATE moq-core-test-internals)
add_test(NAME outbound_observation COMMAND test_outbound_observation)
set_tests_properties(outbound_observation PROPERTIES TIMEOUT 30 LABELS "core;unit")

# Internal adapter ALPN<->version mapping helper (header-only).
add_executable(test_alpn unit/test_alpn.c)
target_link_libraries(test_alpn PRIVATE moq::core)
target_include_directories(test_alpn PRIVATE
    ${PROJECT_SOURCE_DIR}/adapters/common)
add_test(NAME alpn COMMAND test_alpn)

add_executable(test_adapter_target_copy unit/test_adapter_target_copy.c)
target_link_libraries(test_adapter_target_copy PRIVATE moq::core)
add_test(NAME adapter_target_copy COMMAND test_adapter_target_copy)

add_executable(test_bytes unit/test_bytes.c)
target_link_libraries(test_bytes PRIVATE moq::core)
add_test(NAME bytes COMMAND test_bytes)

add_executable(test_quic_varint unit/test_quic_varint.c)
target_link_libraries(test_quic_varint PRIVATE moq::core)
add_test(NAME quic_varint COMMAND test_quic_varint)

add_executable(test_handles unit/test_handles.c)
target_link_libraries(test_handles PRIVATE moq::core)
add_test(NAME handles COMMAND test_handles)

add_executable(test_rcbuf unit/test_rcbuf.c)
target_link_libraries(test_rcbuf PRIVATE moq::core)
add_test(NAME rcbuf COMMAND test_rcbuf)

# White-box: reaches the core-internal rcbuf constructors, so it links the
# default-visibility test-internals archive rather than moq::core.
add_executable(test_rcbuf_internal unit/test_rcbuf_internal.c)
target_link_libraries(test_rcbuf_internal PRIVATE moq-core-test-internals)
add_test(NAME rcbuf_internal COMMAND test_rcbuf_internal)

add_executable(test_kvp unit/test_kvp.c)
target_link_libraries(test_kvp PRIVATE moq::core)
add_test(NAME kvp COMMAND test_kvp)

add_executable(test_buf unit/test_buf.c)
target_link_libraries(test_buf PRIVATE moq::core)
add_test(NAME buf COMMAND test_buf)

add_executable(test_url unit/test_url.c)
target_link_libraries(test_url PRIVATE moq::core)
add_test(NAME url COMMAND test_url)

add_executable(test_control_d16 unit/test_control_d16.c)
target_link_libraries(test_control_d16 PRIVATE moq::core)
add_test(NAME control_d16 COMMAND test_control_d16)

add_executable(test_datagram_receive_policy unit/test_datagram_receive_policy.c)
target_link_libraries(test_datagram_receive_policy PRIVATE moq::core)
add_test(NAME datagram_receive_policy COMMAND test_datagram_receive_policy)

add_executable(test_session_sweep unit/test_session_sweep.c)
target_link_libraries(test_session_sweep PRIVATE moq-core-test-internals)
target_compile_definitions(test_session_sweep PRIVATE MOQ_SESSION_SWEEP_TESTING)
add_test(NAME session_sweep COMMAND test_session_sweep)

add_executable(test_session_goaway_storage unit/test_session_goaway_storage.c)
target_link_libraries(test_session_goaway_storage PRIVATE moq-core-test-internals)
add_test(NAME session_goaway_storage COMMAND test_session_goaway_storage)

add_executable(test_session_occupancy unit/test_session_occupancy.c)
target_link_libraries(test_session_occupancy PRIVATE moq-core-test-internals)
add_test(NAME session_occupancy COMMAND test_session_occupancy)

# Exact-work oracle: advancing-preamble work must scale with the active/ready
# owner set, not with configured pool capacity.
add_executable(test_session_exact_work unit/test_session_exact_work.c)
target_link_libraries(test_session_exact_work PRIVATE moq-core-test-internals)
target_compile_definitions(test_session_exact_work PRIVATE MOQ_SESSION_SWEEP_TESTING)
add_test(NAME session_exact_work COMMAND test_session_exact_work)

# Same fixture with ONLY the complexity family compiled out. It proves the
# probe counters are observational: every lifecycle/output/model control still
# passes while the capacity defect is simply no longer reported.
add_executable(test_session_exact_work_controls unit/test_session_exact_work.c)
target_link_libraries(test_session_exact_work_controls PRIVATE moq-core-test-internals)
target_compile_definitions(test_session_exact_work_controls PRIVATE
    MOQ_SESSION_SWEEP_TESTING EXACT_WORK_NO_COMPLEXITY)
add_test(NAME session_exact_work_controls COMMAND test_session_exact_work_controls)

add_executable(test_transport_bridge unit/test_transport_bridge.c)
target_link_libraries(test_transport_bridge PRIVATE moq-core-test-internals)
target_compile_definitions(test_transport_bridge PRIVATE MOQ_SESSION_SWEEP_TESTING)
target_include_directories(test_transport_bridge PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME transport_bridge COMMAND test_transport_bridge)

add_executable(test_pq_priority ../adapters/common/test_pq_priority.c
    ../adapters/common/moq_pq_send_queue.c)
target_link_libraries(test_pq_priority PRIVATE moq::core)
add_test(NAME pq_priority COMMAND test_pq_priority)

# Fake-endpoint fidelity: every recorded-operation capacity loss and every
# byte-bearing truncation is observable and sticky, so a fixture cannot
# assert an exact operation set or exact bytes on evidence that was dropped.
add_executable(test_fake_endpoint_fidelity unit/test_fake_endpoint_fidelity.c)
target_link_libraries(test_fake_endpoint_fidelity PRIVATE moq-core-test-internals)
target_include_directories(test_fake_endpoint_fidelity PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME fake_endpoint_fidelity COMMAND test_fake_endpoint_fidelity)

add_executable(test_bridge_conformance
    unit/test_bridge_conformance.c
    support/bridge_conformance_pair.c
    conformance/conformance_scenarios.c)
target_link_libraries(test_bridge_conformance PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_conformance PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_conformance COMMAND test_bridge_conformance)

# White-box session tests include session_internal.h and call
# internal functions. Link against moq-core-test-internals (static,
# default visibility) instead of moq::core (may be shared/hidden).
add_executable(test_session_foundation unit/test_session_foundation.c)
target_link_libraries(test_session_foundation PRIVATE moq-core-test-internals)
add_test(NAME session_foundation COMMAND test_session_foundation)

# Public-header consumer proof for the hard session close: includes only
# <moq/session.h> and links moq::core (NOT the test-internals archive),
# exactly as an external app would.
add_executable(test_session_public_close unit/test_session_public_close.c)
target_link_libraries(test_session_public_close PRIVATE moq::core)
add_test(NAME session_public_close COMMAND test_session_public_close)

add_executable(test_session_subscribe unit/test_session_subscribe.c)
target_link_libraries(test_session_subscribe PRIVATE moq-core-test-internals)
# The draft-18 early-object regression establishes its pair through SimPair
# (D18 uses unidirectional control streams the bidi-only manual pump can't
# carry); link it and flag the case when the sim harness is built.
if(MOQ_BUILD_SIM)
    target_link_libraries(test_session_subscribe PRIVATE moq::sim)
    target_compile_definitions(test_session_subscribe PRIVATE MOQ_TEST_SIM)
endif()
add_test(NAME session_subscribe COMMAND test_session_subscribe)

add_executable(test_session_fetch unit/test_session_fetch.c)
target_link_libraries(test_session_fetch PRIVATE moq-core-test-internals)
add_test(NAME session_fetch COMMAND test_session_fetch)

# These rows establish their peers through SimPair and exist only when the
# sim harness is built.
if(TARGET moq::sim)
    add_executable(test_session_rx_admission unit/test_session_rx_admission.c)
    target_link_libraries(test_session_rx_admission PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_rx_admission COMMAND test_session_rx_admission)
    # Core contract: a cancelling fetcher must absorb the late response
    # stream (STOP at the header, discard the bytes already behind it).
    add_executable(test_session_fetch_cancel_late_bytes unit/test_session_fetch_cancel_late_bytes.c)
    target_link_libraries(test_session_fetch_cancel_late_bytes PRIVATE moq::core moq::sim)
    add_test(NAME session_fetch_cancel_late_bytes COMMAND test_session_fetch_cancel_late_bytes)
    # Receive-stream lifetime after a local STOP: stopped/discard state until the
    # peer's FIN/RESET, exactly-once STOP, capacity and slot reuse.
    add_executable(test_session_rx_stopped_stream unit/test_session_rx_stopped_stream.c)
    target_link_libraries(test_session_rx_stopped_stream PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_rx_stopped_stream COMMAND test_session_rx_stopped_stream)
    # No-entry input is retained by the caller,
    # then replayed before the late FETCH_OK with exactly one completion.
    add_test(NAME session_rx_stopped_stream_replay COMMAND test_session_rx_stopped_stream --replay)
endif()
# Receive admission backpressure through the real bridge with an endpoint
# that retains refused input, including the lifetime-capability control.
add_executable(test_bridge_hold_input unit/test_bridge_hold_input.c)
add_executable(test_bridge_required_hold unit/test_bridge_required_hold.c)
target_link_libraries(test_bridge_required_hold PRIVATE moq::core)
add_test(NAME bridge_required_hold COMMAND test_bridge_required_hold)
target_link_libraries(test_bridge_hold_input PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_hold_input PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_hold_input COMMAND test_bridge_hold_input)
# FETCH and TRACK_STATUS request-stream lifecycle through two real bridges,
# draft-16 controls and draft-18 rows: completion closes both halves, late
# peer terminals and reordering are ordinary input, pressure is retried.
add_executable(test_bridge_budget_pending_output unit/test_bridge_budget_pending_output.c)
target_link_libraries(test_bridge_budget_pending_output PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_budget_pending_output PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_budget_pending_output COMMAND test_bridge_budget_pending_output)
add_executable(test_bridge_request_lifecycle unit/test_bridge_request_lifecycle.c)
target_link_libraries(test_bridge_request_lifecycle PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_request_lifecycle PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_request_lifecycle COMMAND test_bridge_request_lifecycle)
# SETUP authorization tokens (moq_session_cfg_t.setup_auth_tokens) on both
# profiles through two real bridges: delivery by value, owned copy, refusals,
# older cfg layouts, credential wiping and the draft-18 option codec.
add_executable(test_session_setup_tokens unit/test_session_setup_tokens.c)
target_link_libraries(test_session_setup_tokens PRIVATE moq-core-test-internals)
target_include_directories(test_session_setup_tokens PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME session_setup_tokens COMMAND test_session_setup_tokens)
# Receive-capacity saturation through the real bridge over fake endpoints:
# fill receive entries, retain later deliveries, and replay after capacity
# returns. No request loses its response or receives a fabricated terminal.
# Local receive retirement: a stopped peer data stream whose FIN or RESET the
# transport never reports is released by the transport's terminal report, after
# the STOP was accepted, with no FIN, reset or delivery claimed.
add_executable(test_bridge_local_retire unit/test_bridge_local_retire.c)
target_link_libraries(test_bridge_local_retire PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_local_retire PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_local_retire COMMAND test_bridge_local_retire)

# A peer RESET of a request bidi ends only the peer's half: our send half keeps
# its mapping until it is closed, or reset once in answer to a later STOP.
add_executable(test_bridge_bidi_reset_half unit/test_bridge_bidi_reset_half.c)
target_link_libraries(test_bridge_bidi_reset_half PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_bidi_reset_half PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_bidi_reset_half COMMAND test_bridge_bidi_reset_half)

add_executable(test_bridge_rx_saturation unit/test_bridge_rx_saturation.c)
target_link_libraries(test_bridge_rx_saturation PRIVATE moq-core-test-internals)
target_include_directories(test_bridge_rx_saturation PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME bridge_rx_saturation COMMAND test_bridge_rx_saturation)
# Historical failure schedules migrated to mandatory lossless admission.
add_test(NAME bridge_rx_saturation_live_replay COMMAND test_bridge_rx_saturation --live-request)
add_test(NAME bridge_rx_saturation_live_record_replay COMMAND test_bridge_rx_saturation --live-record)
add_test(NAME bridge_rx_saturation_live_fragmented_replay COMMAND test_bridge_rx_saturation --live-fragmented)
add_test(NAME bridge_rx_saturation_admission_replay COMMAND test_bridge_rx_saturation --admission)
# Preserve the former explicit --hold invocation as a compatibility alias.
add_test(NAME bridge_rx_saturation_hold_cancelled COMMAND test_bridge_rx_saturation --hold)
add_test(NAME bridge_rx_saturation_hold_live COMMAND test_bridge_rx_saturation --hold --live-request)
add_test(NAME bridge_rx_saturation_hold_live_record COMMAND test_bridge_rx_saturation --hold --live-record)
add_test(NAME bridge_rx_saturation_hold_live_fragmented COMMAND test_bridge_rx_saturation --hold --live-fragmented)
add_test(NAME bridge_rx_saturation_hold_admission COMMAND test_bridge_rx_saturation --hold --admission)
# Publisher-side FETCH abort/reset terminals over the shared SimPair rig.
if(TARGET moq::sim)
    add_executable(test_session_fetch_abort unit/test_session_fetch_abort.c)
    target_link_libraries(test_session_fetch_abort PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_fetch_abort COMMAND test_session_fetch_abort)
    add_executable(test_session_fetch_reset_terminal unit/test_session_fetch_reset_terminal.c)
    target_link_libraries(test_session_fetch_reset_terminal PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_fetch_reset_terminal COMMAND test_session_fetch_reset_terminal)
    add_executable(test_subscriber_fetch_reset unit/test_subscriber_fetch_reset.c)
    target_link_libraries(test_subscriber_fetch_reset PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber_fetch_reset COMMAND test_subscriber_fetch_reset)
    add_executable(test_subscriber_history_ownership unit/test_subscriber_history_ownership.c)
    target_link_libraries(test_subscriber_history_ownership PRIVATE moq::core moq::sim)
    add_test(NAME subscriber_history_ownership COMMAND test_subscriber_history_ownership)
    set_tests_properties(subscriber_history_ownership PROPERTIES TIMEOUT 30)
    add_executable(test_subscriber_event_admission unit/test_subscriber_history_ownership.c)
    target_link_libraries(test_subscriber_event_admission PRIVATE moq::core moq::sim)
    target_compile_definitions(test_subscriber_event_admission PRIVATE MOQ_TEST_ADMISSION_INTERNALS)
    add_test(NAME subscriber_event_admission COMMAND test_subscriber_event_admission
        capacity_leave_preserves_core_ownership_credit_progress)
    set_tests_properties(subscriber_event_admission PROPERTIES TIMEOUT 30)
    add_executable(test_request_staging_ownership unit/test_request_staging_ownership.c)
    target_link_libraries(test_request_staging_ownership PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_staging_ownership COMMAND test_request_staging_ownership)
    set_tests_properties(request_staging_ownership PROPERTIES TIMEOUT 30)
    foreach(_status_test subscriber_status_legacy subscriber_status_outcome)
        add_executable(test_${_status_test} unit/test_${_status_test}.c)
        target_link_libraries(test_${_status_test} PRIVATE moq::core moq::sim)
        add_test(NAME ${_status_test} COMMAND test_${_status_test})
        set_tests_properties(${_status_test} PROPERTIES TIMEOUT 30)
    endforeach()
endif()

add_executable(test_subscriber_status_outcome_layout unit/test_subscriber_status_outcome_layout.c)
target_link_libraries(test_subscriber_status_outcome_layout PRIVATE moq::core)
add_test(NAME subscriber_status_outcome_layout COMMAND test_subscriber_status_outcome_layout)
set_tests_properties(subscriber_status_outcome_layout PROPERTIES TIMEOUT 30)

add_executable(test_publisher_namespace_terminal_layout unit/test_publisher_namespace_terminal_layout.c)
target_link_libraries(test_publisher_namespace_terminal_layout PRIVATE moq::core)
add_test(NAME publisher_namespace_terminal_layout COMMAND test_publisher_namespace_terminal_layout)
set_tests_properties(publisher_namespace_terminal_layout PROPERTIES TIMEOUT 30)

add_executable(test_session_publish unit/test_session_publish.c)
target_link_libraries(test_session_publish PRIVATE moq-core-test-internals)
add_test(NAME session_publish COMMAND test_session_publish)

add_executable(test_issue44_rejected_update unit/test_issue44_rejected_update.c)
target_link_libraries(test_issue44_rejected_update PRIVATE moq-core-test-internals)
foreach(case publish-reject publish-ok subscribe-reject)
    add_test(NAME issue44_rejected_update_${case}
        COMMAND test_issue44_rejected_update ${case})
endforeach()
add_executable(test_issue44_rejected_update_pressure unit/test_issue44_rejected_update_pressure.c)
target_link_libraries(test_issue44_rejected_update_pressure PRIVATE moq-core-test-internals)
add_test(NAME issue44_rejected_update_pressure COMMAND test_issue44_rejected_update_pressure)

add_executable(test_session_track_hist unit/test_session_track_hist.c)
target_link_libraries(test_session_track_hist PRIVATE moq-core-test-internals)
add_test(NAME session_track_hist COMMAND test_session_track_hist)

# test_session_track_hist_lc / _publish_params / _stream_count are SimPair-based
# (moq::sim) — they live in the sim-gated block below, alongside the other
# SimPair tests, so a sim-disabled build (e.g. the shared-link lane) configures.

add_executable(test_session_track_status unit/test_session_track_status.c)
target_link_libraries(test_session_track_status PRIVATE moq-core-test-internals)
add_test(NAME session_track_status COMMAND test_session_track_status)

add_executable(test_session_datagram unit/test_session_datagram.c)
target_link_libraries(test_session_datagram PRIVATE moq-core-test-internals)
add_test(NAME session_datagram COMMAND test_session_datagram)

add_executable(test_session_subgroup unit/test_session_subgroup.c)
target_link_libraries(test_session_subgroup PRIVATE moq-core-test-internals)
add_test(NAME session_subgroup COMMAND test_session_subgroup)

add_executable(test_session_receive unit/test_session_receive.c)
target_link_libraries(test_session_receive PRIVATE moq-core-test-internals)
add_test(NAME session_receive COMMAND test_session_receive)
add_executable(test_subgroup_terminal_last_object unit/test_subgroup_terminal_last_object.c)
target_link_libraries(test_subgroup_terminal_last_object PRIVATE moq-core-test-internals)
add_test(NAME subgroup_terminal_last_object COMMAND test_subgroup_terminal_last_object)

add_executable(test_session_namespace unit/test_session_namespace.c)
target_link_libraries(test_session_namespace PRIVATE moq-core-test-internals)
add_test(NAME session_namespace COMMAND test_session_namespace)

add_executable(test_auth_cache unit/test_auth_cache.c)
target_link_libraries(test_auth_cache PRIVATE moq-core-test-internals)
add_test(NAME auth_cache COMMAND test_auth_cache)

add_executable(test_session_auth unit/test_session_auth.c)
target_link_libraries(test_session_auth PRIVATE moq-core-test-internals)
add_test(NAME session_auth COMMAND test_session_auth)

add_executable(test_request_registry_streamref unit/test_request_registry_streamref.c)
target_link_libraries(test_request_registry_streamref PRIVATE moq-core-test-internals)
add_test(NAME request_registry_streamref COMMAND test_request_registry_streamref)

add_executable(test_request_error_width unit/test_request_error_width.c)
target_link_libraries(test_request_error_width PRIVATE moq-core-test-internals)
add_test(NAME request_error_width COMMAND test_request_error_width)

add_executable(test_action_uni_control unit/test_action_uni_control.c)
target_link_libraries(test_action_uni_control PRIVATE moq-core-test-internals)
add_test(NAME action_uni_control COMMAND test_action_uni_control)

add_executable(test_profile_stream_caps unit/test_profile_stream_caps.c)
target_link_libraries(test_profile_stream_caps PRIVATE moq-core-test-internals)
add_test(NAME profile_stream_caps COMMAND test_profile_stream_caps)

add_executable(test_bridge_uni_control unit/test_bridge_uni_control.c)
target_link_libraries(test_bridge_uni_control PRIVATE moq-core-test-internals)
add_test(NAME bridge_uni_control COMMAND test_bridge_uni_control)

add_executable(test_vi64 unit/test_vi64.c)
target_link_libraries(test_vi64 PRIVATE moq-core-test-internals)
add_test(NAME vi64 COMMAND test_vi64)

add_executable(test_version_registry unit/test_version_registry.c)
target_link_libraries(test_version_registry PRIVATE moq-core-test-internals)
add_test(NAME version_registry COMMAND test_version_registry)

add_executable(test_transport_binding unit/test_transport_binding.c)
target_link_libraries(test_transport_binding PRIVATE moq-core-test-internals)
add_test(NAME transport_binding COMMAND test_transport_binding)

add_executable(test_d18_setup unit/test_d18_setup.c)
target_link_libraries(test_d18_setup PRIVATE moq-core-test-internals)
add_test(NAME d18_setup COMMAND test_d18_setup)

add_executable(test_d18_unsupported unit/test_d18_unsupported.c)
target_link_libraries(test_d18_unsupported PRIVATE moq-core-test-internals)
add_test(NAME d18_unsupported COMMAND test_d18_unsupported)

add_executable(test_control_d18 unit/test_control_d18.c)
target_link_libraries(test_control_d18 PRIVATE moq-core-test-internals)
add_test(NAME control_d18 COMMAND test_control_d18)

add_executable(test_d18_subscribe unit/test_d18_subscribe.c)
target_link_libraries(test_d18_subscribe PRIVATE moq-core-test-internals)
add_test(NAME d18_subscribe COMMAND test_d18_subscribe)

add_executable(test_d18_inbound_subscribe unit/test_d18_inbound_subscribe.c)
target_link_libraries(test_d18_inbound_subscribe PRIVATE moq-core-test-internals)
add_test(NAME d18_inbound_subscribe COMMAND test_d18_inbound_subscribe)

add_executable(test_d18_subscribe_response unit/test_d18_subscribe_response.c)
target_link_libraries(test_d18_subscribe_response PRIVATE moq-core-test-internals)
add_test(NAME d18_subscribe_response COMMAND test_d18_subscribe_response)

add_executable(test_namespace_cardinality unit/test_namespace_cardinality.c)
target_link_libraries(test_namespace_cardinality PRIVATE moq-core-test-internals)
add_test(NAME namespace_cardinality COMMAND test_namespace_cardinality)

add_executable(test_d18_fetch unit/test_d18_fetch.c)
target_link_libraries(test_d18_fetch PRIVATE moq-core-test-internals)
add_test(NAME d18_fetch COMMAND test_d18_fetch)

if(TARGET moq::sim)
    add_executable(test_request_stop_lifetime unit/test_request_stop_lifetime.c)
    target_link_libraries(test_request_stop_lifetime PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_stop_lifetime COMMAND test_request_stop_lifetime)
endif()

add_executable(test_request_cancel_subgroups unit/test_request_cancel_subgroups.c)
target_link_libraries(test_request_cancel_subgroups PRIVATE moq-core-test-internals)
add_test(NAME request_cancel_subgroups COMMAND test_request_cancel_subgroups)

add_executable(test_priority_layout unit/test_priority_layout.c)
target_link_libraries(test_priority_layout PRIVATE moq::core)
add_test(NAME priority_layout COMMAND test_priority_layout)

# Static layout pins only; the include directories of the other tiers are
# added when present so their extended structs are pinned too.
add_executable(test_abi_layout_d22 unit/test_abi_layout_d22.c)
target_link_libraries(test_abi_layout_d22 PRIVATE moq::core)
foreach(_abi_dir sim/include media/loc/include media/object/include
                 media/msf/include media/cmaf/include service/include relay/include
                 adapters/mvfst/include adapters/proxygen/include)
    if(EXISTS ${PROJECT_SOURCE_DIR}/${_abi_dir})
        target_include_directories(test_abi_layout_d22 PRIVATE
            ${PROJECT_SOURCE_DIR}/${_abi_dir})
    endif()
endforeach()
add_test(NAME abi_layout_d22 COMMAND test_abi_layout_d22)

add_executable(test_publisher_fill_layout unit/test_publisher_fill_layout.c)
target_link_libraries(test_publisher_fill_layout PRIVATE moq::core)
add_test(NAME publisher_fill_layout COMMAND test_publisher_fill_layout)

if(TARGET moq::sim)
    add_executable(test_priority_metadata unit/test_priority_metadata.c)
    target_link_libraries(test_priority_metadata PRIVATE moq::sim)
    add_test(NAME priority_metadata COMMAND test_priority_metadata)

    add_executable(test_simpair unit/test_simpair.c)
    target_link_libraries(test_simpair PRIVATE moq::sim)
    add_test(NAME simpair COMMAND test_simpair)

    # SimPair trace record contract: the frozen v0 prefix and the appended
    # detail fields. Uses sim internals (simpair_internal.h) directly; no
    # exported test seam. Those internals are hidden in the shared build, so
    # this target RECOMPILES the sim sources rather than linking the library.
    add_executable(test_sim_trace_contract
        unit/test_sim_trace_contract.c
        ${PROJECT_SOURCE_DIR}/sim/src/simpair.c
        ${PROJECT_SOURCE_DIR}/sim/src/simpair_fault.c
        ${PROJECT_SOURCE_DIR}/sim/src/simpair_delay.c
        ${PROJECT_SOURCE_DIR}/sim/src/simpair_hold.c
        ${PROJECT_SOURCE_DIR}/sim/src/simpair_pump.c)
    target_link_libraries(test_sim_trace_contract PRIVATE moq::core)
    # These translation units DEFINE the MOQ_SIM_API functions, so they must be
    # compiled as the library is: without MOQ_SIM_BUILDING, export.h declares
    # them __declspec(dllimport) on a shared Windows build and MSVC rejects a
    # definition of an imported function.
    target_compile_definitions(test_sim_trace_contract PRIVATE MOQ_SIM_BUILDING)
    target_include_directories(test_sim_trace_contract PRIVATE
        ${PROJECT_SOURCE_DIR}/sim/src
        ${PROJECT_SOURCE_DIR}/sim/include)
    add_test(NAME sim_trace_contract COMMAND test_sim_trace_contract)

    # moq_session_version() over a REAL handshake: the value is the configured
    # profile and is fixed across every phase, on both ends.
    add_executable(test_session_version unit/test_session_version.c)
    target_link_libraries(test_session_version PRIVATE moq::sim)
    add_test(NAME session_version COMMAND test_session_version)
    foreach(_sim_env_version 22 99)
        add_test(NAME sim_default_ignores_env_${_sim_env_version}
            COMMAND ${CMAKE_COMMAND} -E env MOQ_SIM_VERSION=${_sim_env_version}
                $<TARGET_FILE:test_session_version>)
    endforeach()

    # End-to-end allocation-failure lane over the public SimPair surface.
    add_executable(test_simpair_failpoints unit/test_simpair_failpoints.c)
    target_link_libraries(test_simpair_failpoints PRIVATE moq::sim)
    add_test(NAME simpair_failpoints COMMAND test_simpair_failpoints)

    # SimPair-based session tests (moq_simpair_* fixtures over paired sessions).
    add_executable(test_session_track_hist_lc unit/test_session_track_hist_lc.c)
    target_link_libraries(test_session_track_hist_lc PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_track_hist_lc COMMAND test_session_track_hist_lc)

    add_executable(test_session_publish_params unit/test_session_publish_params.c)
    target_link_libraries(test_session_publish_params PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_publish_params COMMAND test_session_publish_params)

    add_executable(test_publisher_group_order_wire unit/test_publisher_group_order_wire.c)
    target_link_libraries(test_publisher_group_order_wire PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME publisher_group_order_wire_controls COMMAND test_publisher_group_order_wire --controls)
    add_test(NAME publisher_group_order_wire_invalid COMMAND test_publisher_group_order_wire --invalid)

    add_executable(test_d22_profile unit/test_d22_profile.c)
    target_link_libraries(test_d22_profile PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_profile COMMAND test_d22_profile)

    add_executable(test_control_d22 unit/test_control_d22.c)
    target_link_libraries(test_control_d22 PRIVATE moq-core-test-internals)
    add_test(NAME control_d22 COMMAND test_control_d22)

    add_executable(test_d22_params unit/test_d22_params.c)
    target_link_libraries(test_d22_params PRIVATE moq-core-test-internals)
    add_test(NAME d22_params COMMAND test_d22_params)

    add_executable(test_d22_messages unit/test_d22_messages.c)
    target_link_libraries(test_d22_messages PRIVATE moq-core-test-internals)
    add_test(NAME d22_messages COMMAND test_d22_messages)

    add_executable(test_location_cfg unit/test_location_cfg.c)
    target_link_libraries(test_location_cfg PRIVATE moq-core-test-internals)
    add_test(NAME location_cfg COMMAND test_location_cfg)

    add_executable(test_request_terminal unit/test_request_terminal.c)
    target_link_libraries(test_request_terminal PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_terminal COMMAND test_request_terminal)
    add_executable(test_d22_track_status_terminal unit/test_d22_track_status_terminal.c)
    target_link_libraries(test_d22_track_status_terminal PRIVATE moq::core moq::sim)
    add_test(NAME d22_track_status_terminal COMMAND test_d22_track_status_terminal)
    set_tests_properties(d22_track_status_terminal PROPERTIES TIMEOUT 30)
    add_executable(test_d22_namespace_terminal unit/test_d22_namespace_terminal.c)
    target_link_libraries(test_d22_namespace_terminal PRIVATE moq::core moq::sim)
    add_test(NAME d22_namespace_terminal COMMAND test_d22_namespace_terminal)
    set_tests_properties(d22_namespace_terminal PROPERTIES TIMEOUT 30)
    add_executable(test_d22_ns_subscription_terminal unit/test_d22_ns_subscription_terminal.c)
    target_link_libraries(test_d22_ns_subscription_terminal PRIVATE moq::core moq::sim)
    add_test(NAME d22_ns_subscription_terminal COMMAND test_d22_ns_subscription_terminal)
    set_tests_properties(d22_ns_subscription_terminal PROPERTIES TIMEOUT 30)
    add_executable(test_subscriber_cancel_streams unit/test_subscriber_cancel_streams.c)
    target_link_libraries(test_subscriber_cancel_streams PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber_cancel_streams COMMAND test_subscriber_cancel_streams)
    add_executable(test_fetch_receive_retry unit/test_fetch_receive_retry.c)
    target_link_libraries(test_fetch_receive_retry PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME fetch_receive_retry COMMAND test_fetch_receive_retry)
    add_executable(test_track_alias_direction unit/test_track_alias_direction.c)
    target_link_libraries(test_track_alias_direction PRIVATE moq::core moq::sim)
    add_test(NAME track_alias_direction COMMAND test_track_alias_direction)
    add_executable(test_fill_inheritance unit/test_fill_inheritance.c)
    target_link_libraries(test_fill_inheritance PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME fill_inheritance COMMAND test_fill_inheritance)
    add_executable(test_request_goaway_late unit/test_request_goaway_late.c)
    target_link_libraries(test_request_goaway_late PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_goaway_late COMMAND test_request_goaway_late)
    add_executable(test_request_refusal unit/test_request_refusal.c)
    target_link_libraries(test_request_refusal PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_refusal COMMAND test_request_refusal)

    add_executable(test_request_refusal_progress unit/test_request_refusal_progress.c)
    target_link_libraries(test_request_refusal_progress PRIVATE moq::sim)
    add_test(NAME request_refusal_progress COMMAND test_request_refusal_progress)

    add_executable(test_d22_session_namespace_wire unit/test_d22_session_namespace_wire.c)
    target_link_libraries(test_d22_session_namespace_wire PRIVATE moq::core moq::sim)
    add_test(NAME d22_session_namespace_wire COMMAND test_d22_session_namespace_wire)
    add_executable(test_teardown_capacity unit/test_teardown_capacity.c)
    target_link_libraries(test_teardown_capacity PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME teardown_capacity COMMAND test_teardown_capacity)
    add_executable(test_subscriber_stop_codes unit/test_subscriber_stop_codes.c)
    target_link_libraries(test_subscriber_stop_codes PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber_stop_codes COMMAND test_subscriber_stop_codes)
    add_executable(test_group_order_default unit/test_group_order_default.c)
    target_link_libraries(test_group_order_default PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME group_order_default COMMAND test_group_order_default)
    add_executable(test_default_publisher_priority unit/test_default_publisher_priority.c)
    target_link_libraries(test_default_publisher_priority PRIVATE moq::core)
    add_test(NAME default_publisher_priority COMMAND test_default_publisher_priority)
    add_executable(test_request_fin unit/test_request_fin.c)
    target_link_libraries(test_request_fin PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_fin COMMAND test_request_fin)
    add_executable(test_d22_publish_response unit/test_d22_publish_response.c)
    target_link_libraries(test_d22_publish_response PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_publish_response COMMAND test_d22_publish_response)
    add_executable(test_d22_publish_parameters unit/test_d22_publish_parameters.c)
    target_link_libraries(test_d22_publish_parameters PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_publish_parameters COMMAND test_d22_publish_parameters)
    add_executable(test_d22_early_update unit/test_d22_early_update.c)
    target_link_libraries(test_d22_early_update PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_early_update COMMAND test_d22_early_update)
    add_executable(test_subscription_update_options unit/test_subscription_update_options.c)
    target_link_libraries(test_subscription_update_options PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscription_update_options COMMAND test_subscription_update_options)
    add_executable(test_d22_track_property_ranges unit/test_d22_track_property_ranges.c)
    target_link_libraries(test_d22_track_property_ranges PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_track_property_ranges COMMAND test_d22_track_property_ranges)
    add_executable(test_d22_redirect unit/test_d22_redirect.c)
    target_link_libraries(test_d22_redirect PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_redirect COMMAND test_d22_redirect)

    add_executable(test_d22_namespace_update unit/test_d22_namespace_update.c)
    target_link_libraries(test_d22_namespace_update PRIVATE moq::core moq::sim)
    add_test(NAME d22_namespace_update COMMAND test_d22_namespace_update)
    add_executable(test_d22_namespace_updates unit/test_d22_namespace_updates.c)
    target_link_libraries(test_d22_namespace_updates PRIVATE moq::core moq::sim)
    add_test(NAME d22_namespace_updates COMMAND test_d22_namespace_updates)
    set_tests_properties(d22_namespace_updates PROPERTIES TIMEOUT 30)
    add_executable(test_d22_namespace_contract unit/test_d22_namespace_contract.c)
    target_link_libraries(test_d22_namespace_contract PRIVATE moq::core moq::sim)
    add_test(NAME d22_namespace_contract COMMAND test_d22_namespace_contract)
    add_executable(test_subscription_rendezvous unit/test_subscription_rendezvous.c)
    target_link_libraries(test_subscription_rendezvous PRIVATE moq::core moq::sim)
    add_test(NAME subscription_rendezvous COMMAND test_subscription_rendezvous)
    add_executable(test_d22_fetch_filter unit/test_d22_fetch_filter.c)
    target_link_libraries(test_d22_fetch_filter PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_fetch_filter COMMAND test_d22_fetch_filter)
    add_executable(test_fetch_request_options unit/test_fetch_request_options.c)
    target_link_libraries(test_fetch_request_options PRIVATE moq::core moq::sim)
    add_test(NAME fetch_request_options COMMAND test_fetch_request_options)
    set_tests_properties(fetch_request_options PROPERTIES TIMEOUT 30)
    add_executable(test_subscription_admission_options unit/test_subscription_admission_options.c)
    target_link_libraries(test_subscription_admission_options PRIVATE moq::core moq::sim)
    add_test(NAME subscription_admission_options COMMAND test_subscription_admission_options)
    add_executable(test_subscription_timeout_input unit/test_subscription_timeout_input.c)
    target_link_libraries(test_subscription_timeout_input PRIVATE moq::core moq::sim)
    add_test(NAME subscription_timeout_input COMMAND test_subscription_timeout_input)
    set_tests_properties(subscription_admission_options subscription_timeout_input PROPERTIES TIMEOUT 30)
    add_executable(test_subgroup_location_width unit/test_subgroup_location_width.c)
    target_link_libraries(test_subgroup_location_width PRIVATE moq::core moq::sim)
    add_test(NAME subgroup_location_width COMMAND test_subgroup_location_width)
    set_tests_properties(subgroup_location_width PROPERTIES TIMEOUT 30)
    add_executable(test_subgroup_commit_timer unit/test_subgroup_commit_timer.c)
    target_link_libraries(test_subgroup_commit_timer PRIVATE moq::core moq::sim)
    add_test(NAME subgroup_commit_timer COMMAND test_subgroup_commit_timer)
    set_tests_properties(subgroup_commit_timer PROPERTIES TIMEOUT 30)
    add_executable(test_subgroup_stop_after_fin unit/test_subgroup_stop_after_fin.c)
    target_link_libraries(test_subgroup_stop_after_fin PRIVATE moq-core-test-internals moq::sim)
    target_compile_definitions(test_subgroup_stop_after_fin PRIVATE MOQ_SESSION_SWEEP_TESTING)
    add_test(NAME subgroup_stop_after_fin COMMAND test_subgroup_stop_after_fin)
    set_tests_properties(subgroup_stop_after_fin PROPERTIES TIMEOUT 30)
    add_executable(test_d22_fetch_ok unit/test_d22_fetch_ok.c)
    target_link_libraries(test_d22_fetch_ok PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_fetch_ok COMMAND test_d22_fetch_ok)
    add_executable(test_fetch_request_update unit/test_fetch_request_update.c)
    target_link_libraries(test_fetch_request_update PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME fetch_request_update COMMAND test_fetch_request_update)
    add_executable(test_d22_tracks_ok_properties unit/test_d22_tracks_ok_properties.c)
    target_link_libraries(test_d22_tracks_ok_properties PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_tracks_ok_properties COMMAND test_d22_tracks_ok_properties)
    add_executable(test_d22_include_properties unit/test_d22_include_properties.c)
    target_link_libraries(test_d22_include_properties PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_include_properties COMMAND test_d22_include_properties)
    add_executable(test_d22_track_status_properties unit/test_d22_track_status_properties.c)
    target_link_libraries(test_d22_track_status_properties PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_track_status_properties COMMAND test_d22_track_status_properties)
    add_executable(test_publisher_track_status unit/test_publisher_track_status.c)
    target_link_libraries(test_publisher_track_status PRIVATE moq::core moq::sim)
    add_test(NAME publisher_track_status COMMAND test_publisher_track_status)
    add_executable(test_d22_concurrent_subs unit/test_d22_concurrent_subs.c)
    target_link_libraries(test_d22_concurrent_subs PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_concurrent_subs COMMAND test_d22_concurrent_subs)
    add_executable(test_retry_interval unit/test_retry_interval.c)
    target_link_libraries(test_retry_interval PRIVATE moq::core moq::sim)
    add_test(NAME retry_interval COMMAND test_retry_interval)
    add_executable(test_request_drain_stop unit/test_request_drain_stop.c)
    target_link_libraries(test_request_drain_stop PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME request_drain_stop COMMAND test_request_drain_stop)
    add_executable(test_d22_request_ids unit/test_d22_request_ids.c)
    target_link_libraries(test_d22_request_ids PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_request_ids COMMAND test_d22_request_ids)
    add_executable(test_d22_update_peers unit/test_d22_update_peers.c)
    target_link_libraries(test_d22_update_peers PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_update_peers COMMAND test_d22_update_peers)
    add_executable(test_d22_session_surfaces unit/test_d22_session_surfaces.c)
    target_link_libraries(test_d22_session_surfaces PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_session_surfaces COMMAND test_d22_session_surfaces)
    add_executable(test_d22_facade_surfaces unit/test_d22_facade_surfaces.c)
    target_link_libraries(test_d22_facade_surfaces PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_facade_surfaces COMMAND test_d22_facade_surfaces)
    add_executable(test_d16_cancel_crossing_done unit/test_d16_cancel_crossing_done.c)
    target_link_libraries(test_d16_cancel_crossing_done PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d16_cancel_crossing_done COMMAND test_d16_cancel_crossing_done)
    add_executable(test_d16_obsolete_done unit/test_d16_obsolete_done.c)
    target_link_libraries(test_d16_obsolete_done PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d16_obsolete_done COMMAND test_d16_obsolete_done)

    add_executable(test_subscription_update_answer unit/test_subscription_update_answer.c)
    target_link_libraries(test_subscription_update_answer PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscription_update_answer COMMAND test_subscription_update_answer)
    add_executable(test_publication_update_answer unit/test_publication_update_answer.c)
    target_link_libraries(test_publication_update_answer PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME publication_update_answer COMMAND test_publication_update_answer)
    add_executable(test_publication_typed_update unit/test_publication_typed_update.c)
    target_link_libraries(test_publication_typed_update PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME publication_typed_update COMMAND test_publication_typed_update)
    set_tests_properties(publication_typed_update PROPERTIES TIMEOUT 30)

    add_executable(test_update_resolution unit/test_update_resolution.c)
    target_link_libraries(test_update_resolution PRIVATE moq::core moq::sim)
    add_test(NAME update_resolution COMMAND test_update_resolution)
    set_tests_properties(update_resolution PROPERTIES TIMEOUT 30)

    add_executable(test_update_resolution_contract unit/test_update_resolution_contract.c)
    target_link_libraries(test_update_resolution_contract PRIVATE moq::core moq::sim)
    add_test(NAME update_resolution_contract COMMAND test_update_resolution_contract)
    set_tests_properties(update_resolution_contract PROPERTIES TIMEOUT 30)

    add_executable(test_local_relative_filter unit/test_local_relative_filter.c)
    target_link_libraries(test_local_relative_filter PRIVATE moq::core moq::sim)
    add_test(NAME local_relative_filter COMMAND test_local_relative_filter)
    set_tests_properties(local_relative_filter PROPERTIES TIMEOUT 30)

    add_executable(test_d22_state_notify unit/test_d22_state_notify.c)
    target_link_libraries(test_d22_state_notify PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_state_notify COMMAND test_d22_state_notify)
    add_executable(test_publish_state_snapshot unit/test_publish_state_snapshot.c)
    target_link_libraries(test_publish_state_snapshot PRIVATE moq::core moq::sim)
    add_test(NAME publish_state_snapshot COMMAND test_publish_state_snapshot)
    set_tests_properties(publish_state_snapshot PROPERTIES TIMEOUT 30)
    add_executable(test_publish_window_snapshot unit/test_publish_window_snapshot.c)
    target_link_libraries(test_publish_window_snapshot PRIVATE moq::core moq::sim)
    add_test(NAME publish_window_snapshot COMMAND test_publish_window_snapshot)
    set_tests_properties(publish_window_snapshot PROPERTIES TIMEOUT 30)
    add_executable(test_publish_state_storage unit/test_publish_state_storage.c)
    target_link_libraries(test_publish_state_storage PRIVATE moq::core)
    add_test(NAME publish_state_storage COMMAND test_publish_state_storage)
    set_tests_properties(publish_state_storage PROPERTIES TIMEOUT 30)
    add_executable(test_publish_state_permission unit/test_publish_state_permission.c)
    target_link_libraries(test_publish_state_permission PRIVATE moq::core moq::sim)
    add_test(NAME publish_state_permission COMMAND test_publish_state_permission)
    set_tests_properties(publish_state_permission PROPERTIES TIMEOUT 30)
    add_executable(test_d22_malformed_track unit/test_d22_malformed_track.c)
    target_link_libraries(test_d22_malformed_track PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_malformed_track COMMAND test_d22_malformed_track)
    add_executable(test_malformed_track_owners unit/test_malformed_track_owners.c)
    target_link_libraries(test_malformed_track_owners PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME malformed_track_owners COMMAND test_malformed_track_owners)
    add_executable(test_vectors_d22_data unit/test_vectors_d22_data.c)
    target_link_libraries(test_vectors_d22_data PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME vectors_d22_data COMMAND test_vectors_d22_data
        ${CMAKE_CURRENT_SOURCE_DIR}/vectors/d22)
    add_executable(test_delivery_timers unit/test_delivery_timers.c)
    target_link_libraries(test_delivery_timers PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME delivery_timers COMMAND test_delivery_timers)
    add_executable(test_scheduling_order unit/test_scheduling_order.c)
    target_link_libraries(test_scheduling_order PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME scheduling_order COMMAND test_scheduling_order)
    add_executable(test_d22_filters unit/test_d22_filters.c)
    target_link_libraries(test_d22_filters PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d22_filters COMMAND test_d22_filters)

    add_executable(test_request_lifetime unit/test_request_lifetime.c)
    target_link_libraries(test_request_lifetime PRIVATE moq-core-test-internals)
    add_test(NAME request_lifetime COMMAND test_request_lifetime)
    add_executable(test_fill_owner unit/test_fill_owner.c)
    target_link_libraries(test_fill_owner PRIVATE moq-core-test-internals)
    add_test(NAME fill_owner COMMAND test_fill_owner)
    add_executable(test_fill_range unit/test_fill_range.c)
    target_link_libraries(test_fill_range PRIVATE moq-core-test-internals)
    add_test(NAME fill_range COMMAND test_fill_range)
    add_executable(test_fill_input unit/test_fill_input.c)
    target_link_libraries(test_fill_input PRIVATE moq-core-test-internals)
    add_test(NAME fill_input COMMAND test_fill_input)
    add_executable(test_fill_session unit/test_fill_session.c)
    target_link_libraries(test_fill_session PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME fill_session COMMAND test_fill_session)
    add_executable(test_reset_reliable_size unit/test_reset_reliable_size.c)
    target_link_libraries(test_reset_reliable_size PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME reset_reliable_size COMMAND test_reset_reliable_size)
    add_executable(test_bridge_reliable_reset unit/test_bridge_reliable_reset.c)
    target_link_libraries(test_bridge_reliable_reset PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME bridge_reliable_reset COMMAND test_bridge_reliable_reset)

    add_executable(test_abi_old_callers_d22 unit/test_abi_old_callers_d22.c)
    target_link_libraries(test_abi_old_callers_d22 PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME abi_old_callers_d22 COMMAND test_abi_old_callers_d22)
    add_executable(test_abi_event_poll_d22 unit/test_abi_event_poll_d22.c)
    target_link_libraries(test_abi_event_poll_d22 PRIVATE moq-core-test-internals)
    add_test(NAME abi_event_poll_d22 COMMAND test_abi_event_poll_d22)

    add_executable(test_session_stream_count unit/test_session_stream_count.c)
    target_link_libraries(test_session_stream_count PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_stream_count COMMAND test_session_stream_count)

    add_executable(test_publish_done_history unit/test_publish_done_history.c)
    target_link_libraries(test_publish_done_history PRIVATE moq::core moq::sim)
    add_test(NAME publish_done_history COMMAND test_publish_done_history)
    set_tests_properties(publish_done_history PROPERTIES TIMEOUT 15)

    add_executable(test_scenario_lifecycle scenario/test_scenario_lifecycle.c)
    target_link_libraries(test_scenario_lifecycle PRIVATE moq::sim)
    add_test(NAME scenario_lifecycle COMMAND test_scenario_lifecycle)

    add_executable(test_scenario_object scenario/test_scenario_object.c)
    target_link_libraries(test_scenario_object PRIVATE moq::sim)
    add_test(NAME scenario_object COMMAND test_scenario_object)

    add_executable(test_publisher unit/test_publisher.c)
    target_link_libraries(test_publisher PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME publisher COMMAND test_publisher)

    add_executable(test_publisher_admission_v1 unit/test_publisher_admission_v1.c)
    target_link_libraries(test_publisher_admission_v1 PRIVATE moq::core moq::sim)
    add_test(NAME publisher_admission_v1 COMMAND test_publisher_admission_v1)
    set_tests_properties(publisher_admission_v1 PROPERTIES TIMEOUT 30)
    add_executable(test_publisher_fill_ownership unit/test_publisher_fill_ownership.c)
    target_link_libraries(test_publisher_fill_ownership PRIVATE moq::core moq::sim)
    add_test(NAME publisher_fill_ownership COMMAND test_publisher_fill_ownership)
    set_tests_properties(publisher_fill_ownership PROPERTIES TIMEOUT 60)
    add_executable(test_publisher_retained_fetch_progress unit/test_publisher_retained_fetch_progress.c)
    target_link_libraries(test_publisher_retained_fetch_progress PRIVATE moq::core moq::sim)
    add_test(NAME publisher_retained_fetch_progress COMMAND test_publisher_retained_fetch_progress)
    set_tests_properties(publisher_retained_fetch_progress PROPERTIES TIMEOUT 30)
    add_executable(test_publisher_retained_fetch_event_credit unit/test_publisher_retained_fetch_event_credit.c)
    target_link_libraries(test_publisher_retained_fetch_event_credit PRIVATE moq::core moq::sim)
    add_test(NAME publisher_retained_fetch_event_credit COMMAND test_publisher_retained_fetch_event_credit)
    set_tests_properties(publisher_retained_fetch_event_credit PROPERTIES TIMEOUT 30)
    add_executable(test_publisher_retained_fetch_owners unit/test_publisher_retained_fetch_owners.c)
    target_link_libraries(test_publisher_retained_fetch_owners PRIVATE moq::core moq::sim)
    add_test(NAME publisher_retained_fetch_owners COMMAND test_publisher_retained_fetch_owners)
    set_tests_properties(publisher_retained_fetch_owners PROPERTIES TIMEOUT 30)

    add_executable(test_publisher_namespace_terminated unit/test_publisher_namespace_terminated.c)
    target_link_libraries(test_publisher_namespace_terminated PRIVATE moq::core moq::sim)
    add_test(NAME publisher_namespace_terminated COMMAND test_publisher_namespace_terminated)
    set_tests_properties(publisher_namespace_terminated PROPERTIES TIMEOUT 30)

    add_executable(test_publisher_old_alignment
        unit/test_publisher_old_alignment.c
        unit/publisher_old_v0.c
        unit/publisher_old_callbacks.c
        unit/publisher_old_pre_v1.c
        unit/publisher_old_v1.c)
    target_link_libraries(test_publisher_old_alignment PRIVATE moq::core moq::sim)
    add_test(NAME publisher_old_alignment COMMAND test_publisher_old_alignment)
    set_tests_properties(publisher_old_alignment PROPERTIES TIMEOUT 30)
    add_executable(test_publisher_track_old_compatibility
        unit/test_publisher_track_old_compatibility.c
        unit/publisher_track_old_original.c
        unit/publisher_track_old_pre_monotonic.c)
    target_link_libraries(test_publisher_track_old_compatibility PRIVATE moq::core moq::sim)
    add_test(NAME publisher_track_old_compatibility COMMAND test_publisher_track_old_compatibility)
    set_tests_properties(publisher_track_old_compatibility PROPERTIES TIMEOUT 30)

    add_executable(test_shard_fanout_cookbook unit/test_shard_fanout_cookbook.c)
    target_link_libraries(test_shard_fanout_cookbook PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME shard_fanout_cookbook COMMAND test_shard_fanout_cookbook)

    add_executable(test_subscriber unit/test_subscriber.c)
    target_link_libraries(test_subscriber PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber COMMAND test_subscriber)

    add_executable(test_subscriber_group_end unit/test_subscriber_group_end.c)
    target_link_libraries(test_subscriber_group_end PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber_group_end COMMAND test_subscriber_group_end)
    add_executable(test_subscriber_stream_terminals unit/test_subscriber_stream_terminals.c)
    target_link_libraries(test_subscriber_stream_terminals PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber_stream_terminals COMMAND test_subscriber_stream_terminals)

    add_executable(test_subscriber_track_result unit/test_subscriber_track_result.c)
    target_link_libraries(test_subscriber_track_result PRIVATE moq::core moq::sim)
    add_test(NAME subscriber_track_result COMMAND test_subscriber_track_result)

    add_executable(test_facade_fill unit/test_facade_fill.c)
    target_link_libraries(test_facade_fill PRIVATE moq::core moq::sim)
    add_test(NAME facade_fill COMMAND test_facade_fill)

    add_executable(test_subscriber_fill_identity unit/test_subscriber_fill_identity.c)
    target_link_libraries(test_subscriber_fill_identity PRIVATE moq::core moq::sim)
    if(UNIX)
        set(THREADS_PREFER_PTHREAD_FLAG TRUE)
        find_package(Threads REQUIRED)
        target_link_libraries(test_subscriber_fill_identity PRIVATE Threads::Threads)
        target_compile_definitions(test_subscriber_fill_identity PRIVATE MOQ_TEST_PTHREADS=1)
    endif()
    add_test(NAME subscriber_fill_identity COMMAND test_subscriber_fill_identity)
    set_tests_properties(subscriber_fill_identity PROPERTIES TIMEOUT 30)

    add_executable(test_subscriber_fill_item unit/test_subscriber_fill_item.c)
    target_link_libraries(test_subscriber_fill_item PRIVATE moq::core moq::sim)
    add_test(NAME subscriber_fill_item COMMAND test_subscriber_fill_item)
    set_tests_properties(subscriber_fill_item PROPERTIES TIMEOUT 30)

    add_executable(test_subscriber_fill_item_layout unit/test_subscriber_fill_item_layout.c)
    target_link_libraries(test_subscriber_fill_item_layout PRIVATE moq::core)
    add_test(NAME subscriber_fill_item_layout COMMAND test_subscriber_fill_item_layout)
    set_tests_properties(subscriber_fill_item_layout PROPERTIES TIMEOUT 30)

    add_executable(test_d18_object unit/test_d18_object.c)
    target_link_libraries(test_d18_object PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_object COMMAND test_d18_object)

    add_executable(test_d18_teardown unit/test_d18_teardown.c)
    target_link_libraries(test_d18_teardown PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_teardown COMMAND test_d18_teardown)

    add_executable(test_d18_update unit/test_d18_update.c)
    target_link_libraries(test_d18_update PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_update COMMAND test_d18_update)

    add_executable(test_update_capacity unit/test_update_capacity.c)
    target_link_libraries(test_update_capacity PRIVATE moq-core-test-internals moq::sim)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_update_capacity PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME update_capacity COMMAND test_update_capacity)
    set_tests_properties(update_capacity PROPERTIES TIMEOUT 30)

    add_executable(test_d18_publish_done unit/test_d18_publish_done.c)
    target_link_libraries(test_d18_publish_done PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_publish_done COMMAND test_d18_publish_done)

    add_executable(test_terminal_send_arena unit/test_terminal_send_arena.c)
    target_link_libraries(test_terminal_send_arena PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME terminal_send_arena COMMAND test_terminal_send_arena)

    add_executable(test_owed_update_observer unit/test_owed_update_observer.c)
    target_link_libraries(test_owed_update_observer PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME owed_update_observer COMMAND test_owed_update_observer)

    add_executable(test_d18_params unit/test_d18_params.c)
    target_link_libraries(test_d18_params PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_params COMMAND test_d18_params)

    add_executable(test_d18_auth unit/test_d18_auth.c)
    target_link_libraries(test_d18_auth PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_auth COMMAND test_d18_auth)

    add_executable(test_d18_publish_namespace unit/test_d18_publish_namespace.c)
    target_link_libraries(test_d18_publish_namespace PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_publish_namespace COMMAND test_d18_publish_namespace)

    add_executable(test_d18_subscribe_namespace unit/test_d18_subscribe_namespace.c)
    target_link_libraries(test_d18_subscribe_namespace PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_subscribe_namespace COMMAND test_d18_subscribe_namespace)

    add_executable(test_d18_track_status unit/test_d18_track_status.c)
    target_link_libraries(test_d18_track_status PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_track_status COMMAND test_d18_track_status)

    add_executable(test_d18_subscribe_tracks unit/test_d18_subscribe_tracks.c)
    target_link_libraries(test_d18_subscribe_tracks PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_subscribe_tracks COMMAND test_d18_subscribe_tracks)

    add_executable(test_ownership_graph unit/test_ownership_graph.c)
    target_link_libraries(test_ownership_graph PRIVATE moq-core-test-internals)
    add_test(NAME ownership_graph COMMAND test_ownership_graph)

    add_executable(test_session_pending_output unit/test_session_pending_output.c)
    target_link_libraries(test_session_pending_output PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_pending_output COMMAND test_session_pending_output)

    add_executable(test_d18_fin_handoff unit/test_d18_fin_handoff.c)
    target_link_libraries(test_d18_fin_handoff PRIVATE moq-core-test-internals)
    add_test(NAME d18_fin_handoff COMMAND test_d18_fin_handoff)

    add_executable(test_d18_ns_response_fin unit/test_d18_ns_response_fin.c)
    target_link_libraries(test_d18_ns_response_fin PRIVATE moq-core-test-internals)
    add_test(NAME d18_ns_response_fin COMMAND test_d18_ns_response_fin)

    # The interop client's URL policy is header-only and private to the tool,
    # so the test reaches it by include path and links only moq::core. The
    # client target itself stays gated by MOQ_BUILD_INTEROP_CLIENT.
    add_executable(test_interop_url_policy
                   ${PROJECT_SOURCE_DIR}/tools/moq-interop-client/test_url_policy.c)
    target_include_directories(test_interop_url_policy PRIVATE
        ${PROJECT_SOURCE_DIR}/tools/moq-interop-client
        ${CMAKE_CURRENT_SOURCE_DIR}/unit)
    target_link_libraries(test_interop_url_policy PRIVATE moq::core)
    add_test(NAME interop_url_policy COMMAND test_interop_url_policy)
    set_tests_properties(interop_url_policy PROPERTIES LABELS "interop;unit")

    add_executable(test_d18_publish unit/test_d18_publish.c)
    target_link_libraries(test_d18_publish PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_publish COMMAND test_d18_publish)

    add_executable(test_d18_goaway unit/test_d18_goaway.c)
    target_link_libraries(test_d18_goaway PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_goaway COMMAND test_d18_goaway)

    add_executable(test_d18_redirect unit/test_d18_redirect.c)
    target_link_libraries(test_d18_redirect PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_redirect COMMAND test_d18_redirect)

    add_executable(test_d18_request_goaway unit/test_d18_request_goaway.c)
    target_link_libraries(test_d18_request_goaway PRIVATE moq-core-test-internals)
    add_test(NAME d18_request_goaway COMMAND test_d18_request_goaway)

    add_executable(test_d18_request_goaway_send unit/test_d18_request_goaway_send.c)
    target_link_libraries(test_d18_request_goaway_send PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_request_goaway_send COMMAND test_d18_request_goaway_send)

    add_executable(test_d18_datagram unit/test_d18_datagram.c)
    target_link_libraries(test_d18_datagram PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_datagram COMMAND test_d18_datagram)

    add_executable(test_fetch_object_delta unit/test_fetch_object_delta.c)
    target_link_libraries(test_fetch_object_delta PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME fetch_object_delta COMMAND test_fetch_object_delta)

    add_executable(test_descending_fetch_fill unit/test_descending_fetch_fill.c)
    target_link_libraries(test_descending_fetch_fill PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME descending_fetch_fill COMMAND test_descending_fetch_fill)

    add_executable(test_fetch_goaway_after_response unit/test_fetch_goaway_after_response.c)
    target_link_libraries(test_fetch_goaway_after_response PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME fetch_goaway_after_response COMMAND test_fetch_goaway_after_response)

    add_executable(test_publish_joining_fetch unit/test_publish_joining_fetch.c)
    target_link_libraries(test_publish_joining_fetch PRIVATE
        moq-core-test-internals moq::sim)
    add_test(NAME publish_joining_fetch COMMAND test_publish_joining_fetch)

    add_executable(test_d18_joining unit/test_d18_joining.c)
    target_link_libraries(test_d18_joining PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_joining COMMAND test_d18_joining)

    add_executable(test_d18_setup_options unit/test_d18_setup_options.c)
    target_link_libraries(test_d18_setup_options PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_setup_options COMMAND test_d18_setup_options)

    add_executable(test_auth_token_width unit/test_auth_token_width.c)
    target_link_libraries(test_auth_token_width PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME auth_token_width COMMAND test_auth_token_width)

    add_executable(test_d18_unsupported_ext unit/test_d18_unsupported_ext.c)
    target_link_libraries(test_d18_unsupported_ext PRIVATE moq-core-test-internals)
    add_test(NAME d18_unsupported_ext COMMAND test_d18_unsupported_ext)

    add_executable(test_sim_bidi_lifecycle unit/test_sim_bidi_lifecycle.c)
    target_link_libraries(test_sim_bidi_lifecycle PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME sim_bidi_lifecycle COMMAND test_sim_bidi_lifecycle)

    add_executable(test_scenario_publisher scenario/test_scenario_publisher.c)
    target_link_libraries(test_scenario_publisher PRIVATE moq::sim)
    add_test(NAME scenario_publisher COMMAND test_scenario_publisher)

    add_executable(test_scenario_faults scenario/test_scenario_faults.c)
    target_link_libraries(test_scenario_faults PRIVATE moq::sim)
    add_test(NAME scenario_faults COMMAND test_scenario_faults)

    add_executable(test_scenario_transport_faults scenario/test_scenario_transport_faults.c)
    target_link_libraries(test_scenario_transport_faults PRIVATE moq::sim)
    add_test(NAME scenario_transport_faults COMMAND test_scenario_transport_faults)

    add_executable(test_scenario_combined_faults scenario/test_scenario_combined_faults.c)
    target_link_libraries(test_scenario_combined_faults PRIVATE moq::sim)
    add_test(NAME scenario_combined_faults COMMAND test_scenario_combined_faults)

    add_executable(test_scenario_goaway scenario/test_scenario_goaway.c)
    target_link_libraries(test_scenario_goaway PRIVATE moq::sim)
    add_test(NAME scenario_goaway COMMAND test_scenario_goaway)

    add_executable(test_scenario_streaming_object scenario/test_scenario_streaming_object.c)
    target_link_libraries(test_scenario_streaming_object PRIVATE moq::sim)
    add_test(NAME scenario_streaming_object COMMAND test_scenario_streaming_object)

    add_executable(test_scenario_namespace_sub scenario/test_scenario_namespace_sub.c)
    target_link_libraries(test_scenario_namespace_sub PRIVATE moq::sim)
    add_test(NAME scenario_namespace_sub COMMAND test_scenario_namespace_sub)

    add_executable(test_scenario_request_bidi_identity scenario/test_scenario_request_bidi_identity.c)
    target_link_libraries(test_scenario_request_bidi_identity PRIVATE moq::sim)
    add_test(NAME scenario_request_bidi_identity COMMAND test_scenario_request_bidi_identity)

    add_executable(test_scenario_request_bidi_auth scenario/test_scenario_request_bidi_auth.c)
    target_link_libraries(test_scenario_request_bidi_auth PRIVATE moq::sim)
    add_test(NAME scenario_request_bidi_auth COMMAND test_scenario_request_bidi_auth)

    add_executable(test_scenario_streaming_faults scenario/test_scenario_streaming_faults.c)
    target_link_libraries(test_scenario_streaming_faults PRIVATE moq::sim)
    add_test(NAME scenario_streaming_faults COMMAND test_scenario_streaming_faults)

    add_executable(test_scenario_fetch scenario/test_scenario_fetch.c)
    target_link_libraries(test_scenario_fetch PRIVATE moq::sim)
    add_test(NAME scenario_fetch COMMAND test_scenario_fetch)
    add_executable(test_scenario_fill scenario/test_scenario_fill.c)
    target_link_libraries(test_scenario_fill PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME scenario_fill COMMAND test_scenario_fill)
    add_executable(test_scenario_overlap_delivery scenario/test_scenario_overlap_delivery.c)
    target_link_libraries(test_scenario_overlap_delivery PRIVATE moq::sim)
    add_test(NAME scenario_overlap_delivery COMMAND test_scenario_overlap_delivery)
    add_executable(test_scenario_input_backpressure scenario/test_scenario_input_backpressure.c)
    target_link_libraries(test_scenario_input_backpressure PRIVATE moq::sim)
    add_test(NAME scenario_input_backpressure COMMAND test_scenario_input_backpressure)
    add_executable(test_scenario_concurrent_subs scenario/test_scenario_concurrent_subs.c)
    target_link_libraries(test_scenario_concurrent_subs PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME scenario_concurrent_subs COMMAND test_scenario_concurrent_subs)

    add_executable(test_scenario_update_answer scenario/test_scenario_update_answer.c)
    target_link_libraries(test_scenario_update_answer PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME scenario_update_answer COMMAND test_scenario_update_answer)
    foreach(update_answer_version IN ITEMS 16 18)
        add_test(NAME scenario_update_answer_d${update_answer_version}
                 COMMAND test_scenario_update_answer)
        set_tests_properties(scenario_update_answer_d${update_answer_version} PROPERTIES
            ENVIRONMENT "MOQ_SIM_VERSION=${update_answer_version}"
            LABELS "version-lane;d${update_answer_version}")
    endforeach()

    add_executable(test_scenario_publish scenario/test_scenario_publish.c)
    target_link_libraries(test_scenario_publish PRIVATE moq::sim)
    add_test(NAME scenario_publish_initiated COMMAND test_scenario_publish)

    add_executable(test_scenario_track_status scenario/test_scenario_track_status.c)
    target_link_libraries(test_scenario_track_status PRIVATE moq::sim)
    add_test(NAME scenario_track_status COMMAND test_scenario_track_status)

    add_executable(test_scenario_object_datagram scenario/test_scenario_object_datagram.c)
    target_link_libraries(test_scenario_object_datagram PRIVATE moq::sim)
    add_test(NAME scenario_object_datagram COMMAND test_scenario_object_datagram)

    add_executable(test_scenario_delay scenario/test_scenario_delay.c)
    target_link_libraries(test_scenario_delay PRIVATE moq::sim)
    add_test(NAME scenario_delay COMMAND test_scenario_delay)

    add_executable(test_scenario_crossed scenario/test_scenario_crossed.c)
    target_link_libraries(test_scenario_crossed PRIVATE moq::sim)
    add_test(NAME scenario_crossed COMMAND test_scenario_crossed)

    add_executable(test_scenario_delay_backpressure scenario/test_scenario_delay_backpressure.c)
    target_link_libraries(test_scenario_delay_backpressure PRIVATE moq::sim)
    add_test(NAME scenario_delay_backpressure COMMAND test_scenario_delay_backpressure)

endif()

add_executable(test_scenario_auth scenario/test_scenario_auth.c)
target_link_libraries(test_scenario_auth PRIVATE moq::core)
add_test(NAME scenario_auth COMMAND test_scenario_auth)

add_executable(test_session_namespace_sub unit/test_session_namespace_sub.c)
target_link_libraries(test_session_namespace_sub PRIVATE moq-core-test-internals)
add_test(NAME session_namespace_sub COMMAND test_session_namespace_sub)

add_executable(test_scenario_backpressure scenario/test_scenario_backpressure.c)
target_link_libraries(test_scenario_backpressure PRIVATE moq-core-test-internals)
add_test(NAME scenario_backpressure COMMAND test_scenario_backpressure)

add_executable(test_scenario_request_credit scenario/test_scenario_request_credit.c)
target_link_libraries(test_scenario_request_credit PRIVATE moq-core-test-internals)
add_test(NAME scenario_request_credit COMMAND test_scenario_request_credit)

# -- D16 wire test vectors ------------------------------------------
add_executable(test_vectors_d22 unit/test_vectors_d22.c)
target_link_libraries(test_vectors_d22 PRIVATE moq-core-test-internals)
add_test(NAME vectors_d22 COMMAND test_vectors_d22
    ${CMAKE_CURRENT_SOURCE_DIR}/vectors/d22)

add_executable(test_vectors_d16 unit/test_vectors_d16.c)
target_link_libraries(test_vectors_d16 PRIVATE moq::core)
add_test(NAME vectors_d16 COMMAND test_vectors_d16
    ${CMAKE_CURRENT_SOURCE_DIR}/vectors/d16)

# -- LOC helpers test -----------------------------------------------
if(TARGET moq::loc)
    add_executable(test_loc unit/test_loc.c)
    target_link_libraries(test_loc PRIVATE moq::loc)
    add_test(NAME loc COMMAND test_loc)
    add_executable(test_loc_metadata unit/test_loc_metadata.c)
    target_link_libraries(test_loc_metadata PRIVATE moq::loc)
    add_test(NAME loc_metadata COMMAND test_loc_metadata)
    add_executable(test_loc_layout unit/test_loc_layout.c)
    target_compile_definitions(test_loc_layout PRIVATE MOQ_LOC_LAYOUT_EXECUTE=1)
    target_link_libraries(test_loc_layout PRIVATE moq::loc)
    add_test(NAME loc_layout COMMAND test_loc_layout)
endif()

# -- Cross-format media conformance harness -------------------------
# Shared "pit both sides against each other" vectors: catalog round-trip
# (MSF/CMSF), typed round-trip (LOC), object validation (CMAF). Severable like
# the per-format media tests; TS plugs in here when it lands.
if(TARGET moq::msf AND TARGET moq::cmaf AND TARGET moq::loc)
    add_executable(test_media_conformance
        unit/test_media_conformance.c
        support/media_conformance.c
        support/media_builders.c)
    target_link_libraries(test_media_conformance PRIVATE
        moq::msf)
    if(NOT TARGET moq::media-object)
        # Without the normalizer, MSF does not propagate CMAF and LOC.
        target_link_libraries(test_media_conformance PRIVATE moq::cmaf moq::loc)
    endif()
    add_test(NAME media_conformance COMMAND test_media_conformance)
endif()

# -- Seeded media-conformance runners (scenario style) --------------
# Seeded generation of valid models + round-trip oracles, per format for replay
# isolation (MOQ_SCENARIO_* env + canonical "Replay:" output). Distinct from the
# fixed media_conformance vectors and from fuzz/ (libFuzzer). support/media_fuzz.c
# is the shared core (PRNG, env, replay, counting allocator); each format's
# generator+oracle is its own TU so a runner links only the libs it exercises.
# Binary name is test_${runner} so future run_seed_sweeps.sh integration is
# drop-in. Generate-only: the runners do not mutate bytes.
if(TARGET moq::loc)
    add_executable(test_scenario_media_loc
        scenario/test_scenario_media_loc.c
        support/media_fuzz.c support/media_fuzz_loc.c)
    target_link_libraries(test_scenario_media_loc PRIVATE moq::loc)
    add_test(NAME scenario_media_loc COMMAND test_scenario_media_loc)
endif()
if(TARGET moq::msf)
    add_executable(test_scenario_media_msf
        scenario/test_scenario_media_msf.c
        support/media_fuzz.c support/media_fuzz_msf.c)
    target_link_libraries(test_scenario_media_msf PRIVATE moq::msf)
    add_test(NAME scenario_media_msf COMMAND test_scenario_media_msf)
endif()
if(TARGET moq::cmaf)
    add_executable(test_scenario_media_cmaf
        scenario/test_scenario_media_cmaf.c
        support/media_fuzz.c support/media_fuzz_cmaf.c support/media_builders.c)
    target_link_libraries(test_scenario_media_cmaf PRIVATE moq::cmaf)
    add_test(NAME scenario_media_cmaf COMMAND test_scenario_media_cmaf)
endif()

# -- OOM sweep test -------------------------------------------------
add_executable(test_oom unit/test_oom.c)
target_link_libraries(test_oom PRIVATE moq::core)
add_test(NAME oom COMMAND test_oom)

# -- C++ compatibility test -----------------------------------------
enable_language(CXX)
add_executable(test_cxx_compat unit/test_cxx_compat.cpp)
target_link_libraries(test_cxx_compat PRIVATE moq::core)
target_compile_features(test_cxx_compat PRIVATE cxx_std_17)
add_test(NAME cxx_compat COMMAND test_cxx_compat)

if(TARGET moq::sim)
    add_executable(test_sim_cxx_compat unit/test_sim_cxx_compat.cpp)
    target_link_libraries(test_sim_cxx_compat PRIVATE moq::sim)
    target_compile_features(test_sim_cxx_compat PRIVATE cxx_std_17)
    add_test(NAME sim_cxx_compat COMMAND test_sim_cxx_compat)
endif()

# -- Symbol-policy: transport_bridge is private exported adapter ABI ---
# These symbols are intentionally exported from shared libmoq-core for
# separately-built adapter DSOs. The header is NOT installed. This test
# catches accidental visibility breakage.
if(BUILD_SHARED_LIBS)
    add_test(NAME bridge_symbol_policy
        COMMAND ${CMAKE_COMMAND}
            -DDYLIB=$<TARGET_FILE:moq-core>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_bridge_symbols.cmake)
    # The core's private same-build SPI for the relay (core/src/spi/core_spi.h,
    # not installed): the exact moq_core_internal_* export set, scanned with the
    # configured nm in the target's native format. Other formats are reported
    # as skipped (UNSUPPORTED), never as a pass.
    if(CMAKE_EXECUTABLE_FORMAT STREQUAL "MACHO")
        set(_moq_spi_format macho)
    elseif(CMAKE_EXECUTABLE_FORMAT STREQUAL "ELF")
        set(_moq_spi_format elf)
    else()
        set(_moq_spi_format unsupported)
    endif()
    add_test(NAME core_spi_symbol_policy
        COMMAND ${CMAKE_COMMAND}
            -DDYLIB=$<TARGET_FILE:moq-core>
            -DNM=${CMAKE_NM}
            -DFORMAT=${_moq_spi_format}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_core_spi_symbols.cmake)
    set_tests_properties(core_spi_symbol_policy PROPERTIES
        SKIP_REGULAR_EXPRESSION "UNSUPPORTED object format")
endif()

# The core SPI checker's own controls: scripted nm stand-ins for the exact
# native inventories and for every refusal (missing, extra, embedded-name near
# misses, diagnostics with exit 0, empty, nonzero exit, malformed, universal,
# unsupported format, launch failure).
if(UNIX)
    add_test(NAME core_spi_symbol_policy_controls
        COMMAND ${CMAKE_COMMAND}
            -DCHECKER=${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_core_spi_symbols.cmake
            -DWORK=${CMAKE_CURRENT_BINARY_DIR}/core_spi_symbol_controls
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_core_spi_symbols_controls.cmake)
endif()

# -- Negotiated-profile substrate ------------------------------------
#
# Four roles, kept apart on purpose: the PURE ORACLE re-derives the
# draft encodings with libc only, the tables/corpus declare the topology, and
# the scripted builder / product adapter / closure runner roles are declared
# in cmake/check_np_roles.cmake.
# The oracle+tables+corpus objects are built ONCE here and reused by the C++
# reader test, so the C parser is the single parser.
# Role 1 gets its OWN object library: the policy check scans exactly this
# object for moq_ symbols, so it must not be pooled with the table/corpus
# translation units.
add_library(np_oracle_obj OBJECT support/np/np_oracle.c)
target_include_directories(np_oracle_obj PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np)
target_compile_features(np_oracle_obj PUBLIC c_std_11)

add_library(np_substrate OBJECT
    support/np/np_tables.c
    support/np/np_corpus.c)
target_include_directories(np_substrate PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np)
target_compile_features(np_substrate PUBLIC c_std_11)

# Roles 2 and 4 now exist. Each gets its OWN object library for the same reason
# role 1 does: the policy scans exactly one object per role, and pooling them
# would make a forbidden symbol unattributable. Include paths are spelled as
# source paths rather than taken from library targets, so these objects -- and
# therefore the policy's object audit -- exist in every configuration, not only
# the ones that build the sim and service libraries.
add_library(np_builder_obj OBJECT support/np/np_wire_builder.c)
target_include_directories(np_builder_obj PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np)
target_compile_features(np_builder_obj PUBLIC c_std_11)

# Role 3, the product adapter: the ONLY substrate translation unit permitted
# to call product LOC APIs. It gets its own object library for the same
# per-role attribution reason as the others.
add_library(np_adapter_obj OBJECT support/np/np_product_adapter.c)
target_include_directories(np_adapter_obj PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np
    ${PROJECT_SOURCE_DIR}/core/include
    ${PROJECT_SOURCE_DIR}/media/loc/include)
target_compile_features(np_adapter_obj PUBLIC c_std_11)

add_library(np_runner_obj OBJECT support/np/np_closure_runner.c)
target_include_directories(np_runner_obj PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np
    ${PROJECT_SOURCE_DIR}/core/include
    ${PROJECT_SOURCE_DIR}/sim/include
    ${PROJECT_SOURCE_DIR}/service/include
    ${PROJECT_SOURCE_DIR}/media/object/include
    ${PROJECT_SOURCE_DIR}/media/loc/include
    ${PROJECT_SOURCE_DIR}/media/msf/include
    ${PROJECT_SOURCE_DIR}/media/cmaf/include)
target_compile_features(np_runner_obj PUBLIC c_std_11)

add_executable(test_negotiated_profile unit/test_negotiated_profile.c
    $<TARGET_OBJECTS:np_oracle_obj> $<TARGET_OBJECTS:np_substrate>)
target_link_libraries(test_negotiated_profile PRIVATE moq-core-test-internals)
target_include_directories(test_negotiated_profile PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np
    ${PROJECT_SOURCE_DIR}/core/src/session
    ${PROJECT_SOURCE_DIR}/adapters/common)
# The corpus is read from an ABSOLUTE SOURCE path fixed at configure time: no
# working-directory dependency, and no checked-in copy anywhere.
target_compile_definitions(test_negotiated_profile PRIVATE
    NP_CORPUS_PATH="${CMAKE_CURRENT_SOURCE_DIR}/vectors/negotiated_profile.vectors")
add_test(NAME negotiated_profile COMMAND test_negotiated_profile)

# The C++17 reader REUSES the C parser object; it does not reimplement the
# grammar. The count and digest are pinned at configure time so all three
# readers are compared against the same declared numbers.
set(NP_CORPUS_FILE ${CMAKE_CURRENT_SOURCE_DIR}/vectors/negotiated_profile.vectors)
set(NP_CORPUS_COUNT 79)
set(NP_CORPUS_DIGEST 0xaf21b87308068ae6)
# C++ DISCOVERY, before the conditional that needs it.
#
# The earlier spelling tested a nonexistent plural option and relied on
# CMAKE_CXX_COMPILER already being set -- which it is in a cache that once had
# CXX enabled, and is NOT in a fresh C-only configure, so the C++ reader
# silently vanished from a clean tree. check_language() is CMake's own optional
# language probe: it answers before enable_language() is called, so this block
# cannot fail the configure by asking.
#
# HONEST SCOPE: this makes the READER's registration correct in a fresh
# configure. It does NOT make the test suite C++-optional -- a tests-enabled
# LibMoQ configure ALREADY requires a C++ compiler, because the existing C++
# compatibility test below calls enable_language(CXX) unconditionally. The
# not-found branch here is therefore a clean local fallback, not a claim that
# the whole configuration proceeds with only C and Swift.
include(CheckLanguage)
set(_np_have_cxx OFF)
if(CMAKE_CXX_COMPILER_LOADED)
    # CXX is already an enabled language in this project (e.g. an adapter or
    # the C++ binding turned it on ahead of us).
    set(_np_have_cxx ON)
else()
    # Not enabled yet: ASK, then enable. check_language() is the only step that
    # can answer without failing the configure when no C++ compiler exists.
    check_language(CXX)
    if(CMAKE_CXX_COMPILER)
        set(_np_have_cxx ON)
    endif()
endif()
if(_np_have_cxx)
    enable_language(CXX)
    add_executable(test_negotiated_profile_cxx unit/test_negotiated_profile_cxx.cpp
        $<TARGET_OBJECTS:np_substrate>)
    target_include_directories(test_negotiated_profile_cxx PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/support/np)
    target_compile_features(test_negotiated_profile_cxx PRIVATE cxx_std_17)
    target_compile_definitions(test_negotiated_profile_cxx PRIVATE
        NP_CORPUS_PATH="${NP_CORPUS_FILE}"
        NP_CORPUS_COUNT=${NP_CORPUS_COUNT}
        NP_CORPUS_DIGEST=${NP_CORPUS_DIGEST})
    add_test(NAME negotiated_profile_cxx COMMAND test_negotiated_profile_cxx)
else()
    # Reached only if a C++ compiler is genuinely absent. The suite's own
    # mandatory enable_language(CXX) further down would then fail the
    # configure anyway; this branch keeps THIS block from being the thing that
    # fails, and says so rather than implying a C-only configuration works.
    message(STATUS "np: no C++ compiler found; the C++ corpus reader is not "
                   "registered (note: the wider test suite requires C++)")
endif()

# The four-role dependency policy, run as a test so it gates every lane. It
# reads the compiled oracle object, so it depends on np_substrate being built.
# Closure direction C: the codec alone, builder against product adapter, over
# every record the corpus declares. Registered only when the LOC library is
# built, since role 3 links against it.
if(TARGET moq-loc)
    add_executable(test_np_codec_closure unit/test_np_codec_closure.c
        $<TARGET_OBJECTS:np_oracle_obj> $<TARGET_OBJECTS:np_substrate>
        $<TARGET_OBJECTS:np_builder_obj> $<TARGET_OBJECTS:np_adapter_obj>)
    target_include_directories(test_np_codec_closure PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/support/np)
    target_link_libraries(test_np_codec_closure PRIVATE moq::loc moq::core)
    target_compile_definitions(test_np_codec_closure PRIVATE
        NP_CORPUS_PATH="${NP_CORPUS_FILE}")
    add_test(NAME np_codec_closure COMMAND test_np_codec_closure)
endif()

add_test(NAME negotiated_profile_roles
    COMMAND ${CMAKE_COMMAND}
        -DNP_SRC_DIR=${CMAKE_CURRENT_SOURCE_DIR}/support/np
        -DNP_ORACLE_OBJ=$<TARGET_OBJECTS:np_oracle_obj>
        -DNP_BUILDER_OBJ=$<TARGET_OBJECTS:np_builder_obj>
        -DNP_RUNNER_OBJ=$<TARGET_OBJECTS:np_runner_obj>
        -DNP_ADAPTER_OBJ=$<TARGET_OBJECTS:np_adapter_obj>
        -DNP_NM=${CMAKE_NM}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_np_roles.cmake)

# Relay rows that drive the publisher/subscriber facades over the production
# relay binding live here, outside relay/ (the relay boundary forbids the
# facades in relay sources, tests included).
if(TARGET moq::relay AND TARGET moq::sim)
    add_executable(test_scenario_relay_bootstrap
        scenario/test_scenario_relay_bootstrap.c)
    target_link_libraries(test_scenario_relay_bootstrap PRIVATE moq::relay moq::sim)
    add_test(NAME scenario_relay_bootstrap COMMAND test_scenario_relay_bootstrap)

    # The wire oracle belongs outside relay/, whose sources exclude codecs.
    add_executable(test_relay_completion_stream_count
        scenario/test_relay_completion_stream_count.c)
    target_compile_definitions(test_relay_completion_stream_count PRIVATE
        MOQR_BIND_TESTING MOQ_RELAY_BUILDING)
    target_include_directories(test_relay_completion_stream_count PRIVATE
        ${PROJECT_SOURCE_DIR}/relay/private)
    target_link_libraries(test_relay_completion_stream_count PRIVATE
        moq::sim moq-relay-policy)
    add_test(NAME relay_completion_stream_count
        COMMAND test_relay_completion_stream_count)
    set_tests_properties(relay_completion_stream_count PROPERTIES TIMEOUT 120)
    add_executable(test_relay_auto_update_terminal
        scenario/test_relay_auto_update_terminal.c)
    target_compile_definitions(test_relay_auto_update_terminal PRIVATE
        MOQR_BIND_TESTING MOQ_RELAY_BUILDING)
    target_include_directories(test_relay_auto_update_terminal PRIVATE
        ${PROJECT_SOURCE_DIR}/relay/private)
    target_link_libraries(test_relay_auto_update_terminal PRIVATE
        moq-core-test-internals moq::sim moq-relay-policy)
    add_test(NAME relay_auto_update_terminal
        COMMAND test_relay_auto_update_terminal)
    set_tests_properties(relay_auto_update_terminal PROPERTIES TIMEOUT 120)
endif()

# The real media_receiver behind the production relay binding with a real
# origin facade: catalog bootstrap through retained upstream retrieval.
if(TARGET moq::relay AND TARGET moq::sim AND TARGET moq-service-receiver-test-internals)
    add_executable(test_media_receiver_relay_bootstrap
        ${PROJECT_SOURCE_DIR}/service/tests/test_media_receiver_relay_bootstrap.c)
    target_link_libraries(test_media_receiver_relay_bootstrap PRIVATE
        moq-service-receiver-test-internals moq::sim moq::relay)
    target_include_directories(test_media_receiver_relay_bootstrap PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/unit)
    add_test(NAME media_receiver_relay_bootstrap
        COMMAND test_media_receiver_relay_bootstrap)
    set_tests_properties(media_receiver_relay_bootstrap
        PROPERTIES TIMEOUT 90 LABELS "service;relay;sansio")
    foreach(draft IN ITEMS 16 18 22)
        add_test(NAME media_receiver_relay_bootstrap_draft_${draft}
            COMMAND test_media_receiver_relay_bootstrap d${draft})
        set_tests_properties(media_receiver_relay_bootstrap_draft_${draft}
            PROPERTIES TIMEOUT 30 LABELS "service;relay;sansio")
    endforeach()

    add_executable(test_media_receiver_finite_tail_ordering
        ${PROJECT_SOURCE_DIR}/service/tests/test_media_receiver_finite_tail_ordering.c)
    target_link_libraries(test_media_receiver_finite_tail_ordering PRIVATE
        moq-service-receiver-test-internals moq::sim moq::relay)
    target_include_directories(test_media_receiver_finite_tail_ordering PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/unit)
    add_test(NAME media_receiver_finite_tail_ordering
        COMMAND test_media_receiver_finite_tail_ordering)
    set_tests_properties(media_receiver_finite_tail_ordering PROPERTIES TIMEOUT 120)
endif()

# The receiver's stream-terminal lane over a real publisher and SimPair.
if(TARGET moq::sim AND TARGET moq-service-receiver-test-internals)
    add_executable(test_media_receiver_stream_terminals
        ${PROJECT_SOURCE_DIR}/service/tests/test_media_receiver_stream_terminals.c)
    target_link_libraries(test_media_receiver_stream_terminals PRIVATE
        moq-service-receiver-test-internals moq::sim)
    if(TARGET moq::mpeg2ts)
        target_include_directories(test_media_receiver_stream_terminals PRIVATE
            ${PROJECT_SOURCE_DIR}/service/tests
            ${PROJECT_SOURCE_DIR}/media/mpeg2ts/tests)
        target_compile_definitions(test_media_receiver_stream_terminals PRIVATE
            MOQ_TEST_HAVE_TS=1)
    endif()
    add_test(NAME media_receiver_stream_terminals
        COMMAND test_media_receiver_stream_terminals)
    set_tests_properties(media_receiver_stream_terminals PROPERTIES TIMEOUT 120)
endif()

# A completed broadcast on the receiver over a real publisher and SimPair.
if(TARGET moq::sim AND TARGET moq-service-receiver-test-internals)
    add_executable(test_media_receiver_completion
        ${PROJECT_SOURCE_DIR}/service/tests/test_media_receiver_completion.c)
    target_link_libraries(test_media_receiver_completion PRIVATE
        moq-service-receiver-test-internals moq::sim)
    target_include_directories(test_media_receiver_completion PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/unit)
    add_test(NAME media_receiver_completion
        COMMAND test_media_receiver_completion)
    set_tests_properties(media_receiver_completion PROPERTIES TIMEOUT 120)
endif()

# The receiver surfaces the media player is built on.
if(TARGET moq::sim AND TARGET moq-service-receiver-test-internals)
    add_executable(test_media_receiver_player_support
        ${PROJECT_SOURCE_DIR}/service/tests/test_media_receiver_player_support.c)
    target_link_libraries(test_media_receiver_player_support PRIVATE
        moq-service-receiver-test-internals moq::sim)
    add_test(NAME media_receiver_player_support
        COMMAND test_media_receiver_player_support)
    set_tests_properties(media_receiver_player_support PROPERTIES TIMEOUT 120)
endif()

# -- Version lanes --------------------------------------------------
# These protocol runners read MOQ_SIM_VERSION and explicitly configure their
# session profile. Auth, request-credit and backpressure retain D16 fixtures;
# the three media runners are codec-only and relay-bootstrap always runs its
# fixed D16/D18 cases. None earns a duplicate version lane.
# Each entry pairs the registered test name with its executable target;
# targets behind a build option are skipped when absent.
set(MOQ_VERSION_LANE_TESTS
    bridge_conformance:test_bridge_conformance
    scenario_lifecycle:test_scenario_lifecycle
    scenario_object:test_scenario_object
    scenario_publisher:test_scenario_publisher
    scenario_faults:test_scenario_faults
    scenario_transport_faults:test_scenario_transport_faults
    scenario_combined_faults:test_scenario_combined_faults
    scenario_goaway:test_scenario_goaway
    scenario_streaming_object:test_scenario_streaming_object
    scenario_namespace_sub:test_scenario_namespace_sub
    scenario_streaming_faults:test_scenario_streaming_faults
    scenario_fetch:test_scenario_fetch
    scenario_fill:test_scenario_fill
    scenario_overlap_delivery:test_scenario_overlap_delivery
    scenario_input_backpressure:test_scenario_input_backpressure
    scenario_concurrent_subs:test_scenario_concurrent_subs
    scenario_publish_initiated:test_scenario_publish
    scenario_track_status:test_scenario_track_status
    scenario_object_datagram:test_scenario_object_datagram
    scenario_delay:test_scenario_delay
    scenario_crossed:test_scenario_crossed
    scenario_delay_backpressure:test_scenario_delay_backpressure)
foreach(lane_version IN ITEMS 18 22)
    foreach(lane_entry IN LISTS MOQ_VERSION_LANE_TESTS)
        string(REPLACE ":" ";" lane_pair "${lane_entry}")
        list(GET lane_pair 0 lane_test)
        list(GET lane_pair 1 lane_target)
        if(NOT TARGET ${lane_target})
            continue()
        endif()
        add_test(NAME ${lane_test}_d${lane_version} COMMAND ${lane_target})
        set_tests_properties(${lane_test}_d${lane_version} PROPERTIES
            ENVIRONMENT "MOQ_SIM_VERSION=${lane_version}"
            LABELS "version-lane;d${lane_version}")
    endforeach()
endforeach()

add_executable(test_cluster_wire test_cluster_wire.c)
target_link_libraries(test_cluster_wire PRIVATE moq-core-test-internals)
add_test(NAME cluster_wire COMMAND test_cluster_wire)
add_executable(test_cluster_session test_cluster_session.c)
target_link_libraries(test_cluster_session PRIVATE moq-core-test-internals)
add_test(NAME cluster_session COMMAND test_cluster_session)
add_executable(test_cluster_work test_cluster_work.c)
target_link_libraries(test_cluster_work PRIVATE moq-core-test-internals)
add_test(NAME cluster_work COMMAND test_cluster_work)
