if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    add_compile_options(-Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# -- C tests --------------------------------------------------------
add_executable(test_version unit/test_version.c)
target_link_libraries(test_version PRIVATE moq::core)
add_test(NAME version COMMAND test_version)

# Internal adapter ALPN<->version mapping helper (header-only).
add_executable(test_alpn unit/test_alpn.c)
target_link_libraries(test_alpn PRIVATE moq::core)
target_include_directories(test_alpn PRIVATE
    ${PROJECT_SOURCE_DIR}/adapters/common)
add_test(NAME alpn COMMAND test_alpn)

add_executable(test_bytes unit/test_bytes.c)
target_link_libraries(test_bytes PRIVATE moq::core)
add_test(NAME bytes COMMAND test_bytes)

add_executable(test_request_id_window unit/test_request_id_window.c)
target_link_libraries(test_request_id_window PRIVATE moq::core)
add_test(NAME request_id_window COMMAND test_request_id_window)

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

add_executable(test_session_sweep unit/test_session_sweep.c)
target_link_libraries(test_session_sweep PRIVATE moq-core-test-internals)
target_compile_definitions(test_session_sweep PRIVATE MOQ_SESSION_SWEEP_TESTING)
add_test(NAME session_sweep COMMAND test_session_sweep)

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

add_executable(test_session_publish unit/test_session_publish.c)
target_link_libraries(test_session_publish PRIVATE moq-core-test-internals)
add_test(NAME session_publish COMMAND test_session_publish)

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
    add_executable(test_simpair unit/test_simpair.c)
    target_link_libraries(test_simpair PRIVATE moq::sim)
    add_test(NAME simpair COMMAND test_simpair)

    # SimPair trace record contract: the frozen v0 prefix and the appended
    # detail fields. Uses sim internals (simpair_internal.h) directly; no
    # exported test seam. Those internals are hidden in the shared build, so
    # this target RECOMPILES the sim sources rather than linking the library.
    add_executable(test_sim_trace_contract
        unit/test_sim_trace_contract.c
        ${CMAKE_SOURCE_DIR}/sim/src/simpair.c
        ${CMAKE_SOURCE_DIR}/sim/src/simpair_fault.c
        ${CMAKE_SOURCE_DIR}/sim/src/simpair_delay.c
        ${CMAKE_SOURCE_DIR}/sim/src/simpair_pump.c)
    target_link_libraries(test_sim_trace_contract PRIVATE moq::core)
    # These translation units DEFINE the MOQ_SIM_API functions, so they must be
    # compiled as the library is: without MOQ_SIM_BUILDING, export.h declares
    # them __declspec(dllimport) on a shared Windows build and MSVC rejects a
    # definition of an imported function.
    target_compile_definitions(test_sim_trace_contract PRIVATE MOQ_SIM_BUILDING)
    target_include_directories(test_sim_trace_contract PRIVATE
        ${CMAKE_SOURCE_DIR}/sim/src
        ${CMAKE_SOURCE_DIR}/sim/include)
    add_test(NAME sim_trace_contract COMMAND test_sim_trace_contract)

    # moq_session_version() over a REAL handshake: the value is the configured
    # profile and is fixed across every phase, on both ends.
    add_executable(test_session_version unit/test_session_version.c)
    target_link_libraries(test_session_version PRIVATE moq::sim)
    add_test(NAME session_version COMMAND test_session_version)

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

    add_executable(test_session_stream_count unit/test_session_stream_count.c)
    target_link_libraries(test_session_stream_count PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME session_stream_count COMMAND test_session_stream_count)

    add_executable(test_scenario_lifecycle scenario/test_scenario_lifecycle.c)
    target_link_libraries(test_scenario_lifecycle PRIVATE moq::sim)
    add_test(NAME scenario_lifecycle COMMAND test_scenario_lifecycle)

    add_executable(test_scenario_object scenario/test_scenario_object.c)
    target_link_libraries(test_scenario_object PRIVATE moq::sim)
    add_test(NAME scenario_object COMMAND test_scenario_object)

    add_executable(test_publisher unit/test_publisher.c)
    target_link_libraries(test_publisher PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME publisher COMMAND test_publisher)

    add_executable(test_shard_fanout_cookbook unit/test_shard_fanout_cookbook.c)
    target_link_libraries(test_shard_fanout_cookbook PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME shard_fanout_cookbook COMMAND test_shard_fanout_cookbook)

    add_executable(test_subscriber unit/test_subscriber.c)
    target_link_libraries(test_subscriber PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME subscriber COMMAND test_subscriber)

    add_executable(test_d18_object unit/test_d18_object.c)
    target_link_libraries(test_d18_object PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_object COMMAND test_d18_object)

    add_executable(test_d18_teardown unit/test_d18_teardown.c)
    target_link_libraries(test_d18_teardown PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_teardown COMMAND test_d18_teardown)

    add_executable(test_d18_update unit/test_d18_update.c)
    target_link_libraries(test_d18_update PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_update COMMAND test_d18_update)

    add_executable(test_d18_publish_done unit/test_d18_publish_done.c)
    target_link_libraries(test_d18_publish_done PRIVATE moq-core-test-internals moq::sim)
    add_test(NAME d18_publish_done COMMAND test_d18_publish_done)

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
                   ${CMAKE_SOURCE_DIR}/tools/moq-interop-client/test_url_policy.c)
    target_include_directories(test_interop_url_policy PRIVATE
        ${CMAKE_SOURCE_DIR}/tools/moq-interop-client
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

    add_executable(test_scenario_streaming_faults scenario/test_scenario_streaming_faults.c)
    target_link_libraries(test_scenario_streaming_faults PRIVATE moq::sim)
    add_test(NAME scenario_streaming_faults COMMAND test_scenario_streaming_faults)

    add_executable(test_scenario_fetch scenario/test_scenario_fetch.c)
    target_link_libraries(test_scenario_fetch PRIVATE moq::sim)
    add_test(NAME scenario_fetch COMMAND test_scenario_fetch)

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
add_executable(test_vectors_d16 unit/test_vectors_d16.c)
target_link_libraries(test_vectors_d16 PRIVATE moq::core)
add_test(NAME vectors_d16 COMMAND test_vectors_d16
    ${CMAKE_CURRENT_SOURCE_DIR}/vectors/d16)

# -- LOC helpers test -----------------------------------------------
if(TARGET moq::loc)
    add_executable(test_loc unit/test_loc.c)
    target_link_libraries(test_loc PRIVATE moq::loc)
    add_test(NAME loc COMMAND test_loc)
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
        moq::msf moq::cmaf moq::loc)
    add_test(NAME media_conformance COMMAND test_media_conformance)
endif()

# -- Seeded media-conformance runners (scenario style) --------------
# Seeded generation of valid models + round-trip oracles, per format for replay
# isolation (MOQ_SCENARIO_* env + canonical "Replay:" output). Distinct from the
# fixed media_conformance vectors and from fuzz/ (libFuzzer). support/media_fuzz.c
# is the shared core (PRNG, env, replay, counting allocator); each format's
# generator+oracle is its own TU so a runner links only the libs it exercises.
# Binary name is test_${runner} so future run_seed_sweeps.sh integration is
# drop-in. Generate-only (byte mutation is a later slice).
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
endif()

# -- Negotiated-profile substrate (step 5) ---------------------------
#
# Four roles, kept apart on purpose (0659/0661): the PURE ORACLE re-derives the
# draft encodings with libc only, the tables/corpus declare the topology, and
# the future scripted builder / product adapter / closure runner are staged.
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
    ${CMAKE_SOURCE_DIR}/core/include
    ${CMAKE_SOURCE_DIR}/media/loc/include)
target_compile_features(np_adapter_obj PUBLIC c_std_11)

add_library(np_runner_obj OBJECT support/np/np_closure_runner.c)
target_include_directories(np_runner_obj PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np
    ${CMAKE_SOURCE_DIR}/core/include
    ${CMAKE_SOURCE_DIR}/sim/include
    ${CMAKE_SOURCE_DIR}/service/include
    ${CMAKE_SOURCE_DIR}/media/object/include
    ${CMAKE_SOURCE_DIR}/media/msf/include
    ${CMAKE_SOURCE_DIR}/media/cmaf/include)
target_compile_features(np_runner_obj PUBLIC c_std_11)

add_executable(test_negotiated_profile unit/test_negotiated_profile.c
    $<TARGET_OBJECTS:np_oracle_obj> $<TARGET_OBJECTS:np_substrate>)
target_link_libraries(test_negotiated_profile PRIVATE moq-core-test-internals)
target_include_directories(test_negotiated_profile PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/support/np
    ${CMAKE_SOURCE_DIR}/core/src/session
    ${CMAKE_SOURCE_DIR}/adapters/common)
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
