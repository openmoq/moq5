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

add_executable(test_transport_bridge unit/test_transport_bridge.c)
target_link_libraries(test_transport_bridge PRIVATE moq-core-test-internals)
target_include_directories(test_transport_bridge PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/unit)
add_test(NAME transport_bridge COMMAND test_transport_bridge)

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

add_executable(test_d18_fetch unit/test_d18_fetch.c)
target_link_libraries(test_d18_fetch PRIVATE moq-core-test-internals)
add_test(NAME d18_fetch COMMAND test_d18_fetch)

if(TARGET moq::sim)
    add_executable(test_simpair unit/test_simpair.c)
    target_link_libraries(test_simpair PRIVATE moq::sim)
    add_test(NAME simpair COMMAND test_simpair)

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
