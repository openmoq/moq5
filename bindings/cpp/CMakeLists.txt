enable_language(CXX)

# Header-only C++ binding target
add_library(moq_cpp INTERFACE)
target_include_directories(moq_cpp INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(moq_cpp INTERFACE moq::core)
target_compile_features(moq_cpp INTERFACE cxx_std_20)
add_library(moq::cpp ALIAS moq_cpp)

if(TARGET moq::loc)
    target_link_libraries(moq_cpp INTERFACE moq::loc)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_LOC)
endif()

if(TARGET moq::msf)
    target_link_libraries(moq_cpp INTERFACE moq::msf)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_MSF)
endif()

if(TARGET moq::cmaf)
    target_link_libraries(moq_cpp INTERFACE moq::cmaf)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_CMAF)
endif()

if(TARGET moq::mpeg2ts)
    target_link_libraries(moq_cpp INTERFACE moq::mpeg2ts)
    target_compile_definitions(moq_cpp INTERFACE MOQ_HAS_MPEG2TS)
endif()

set_target_properties(moq_cpp PROPERTIES EXPORT_NAME cpp)

install(TARGETS moq_cpp
    EXPORT libmoqTargets)

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.hpp"
    PATTERN "loc.hpp" EXCLUDE
    PATTERN "msf.hpp" EXCLUDE
    PATTERN "cmaf.hpp" EXCLUDE
    PATTERN "mpeg2ts.hpp" EXCLUDE
    PATTERN "media_player.hpp" EXCLUDE)

if(TARGET moq::loc)
    install(FILES include/moq/loc.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

if(TARGET moq::msf)
    install(FILES include/moq/msf.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

if(TARGET moq::cmaf)
    install(FILES include/moq/cmaf.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

if(TARGET moq::mpeg2ts)
    install(FILES include/moq/mpeg2ts.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

# <moq/media_player.hpp> wraps the service tier's player. It is opt-in, not
# part of moq::cpp's link interface: a consumer links moq::service next to
# moq::cpp (find_package(libmoq COMPONENTS service)), so core-only moq::cpp
# users never take on the service tier.
if(TARGET moq::service AND TARGET moq::playback)
    install(FILES include/moq/media_player.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)
endif()

# Tests (only when tests are enabled)
if(MOQ_BUILD_TESTS)
    # Owned warning baseline for the in-tree C++ binding test executables
    # below (directory scope -> applies to targets defined after this in
    # this dir). It does NOT touch the header-only `moq_cpp` INTERFACE
    # target (defined above, compiles nothing) nor the out-of-tree
    # `cpp_consumer` cold-consumer build (a separate cmake invocation that
    # must stay free of -Werror). -Werror gated by MOQ_WARNINGS_AS_ERRORS.
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        add_compile_options(-Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    add_executable(test_cpp_result tests/test_result.cpp)
    target_link_libraries(test_cpp_result PRIVATE moq::cpp)
    add_test(NAME cpp_result COMMAND test_cpp_result)

    add_executable(test_cpp_buffer tests/test_buffer.cpp)
    target_link_libraries(test_cpp_buffer PRIVATE moq::cpp)
    add_test(NAME cpp_buffer COMMAND test_cpp_buffer)

    add_executable(test_cpp_types tests/test_types.cpp)
    target_link_libraries(test_cpp_types PRIVATE moq::cpp)
    add_test(NAME cpp_types COMMAND test_cpp_types)

    add_executable(test_cpp_session tests/test_session.cpp)
    target_link_libraries(test_cpp_session PRIVATE moq::cpp)
    add_test(NAME cpp_session COMMAND test_cpp_session)

    # Single-TU cardinality proof: macro-renames only the two size-aware poll
    # entry points so the fakes in that TU are reached without interposition.
    # Must stay one TU -- a second, differently macro-expanded moq::session in
    # the same binary would violate the ODR.
    add_executable(test_cpp_batched_poll_cardinality
                   tests/test_batched_poll_cardinality.cpp)
    target_link_libraries(test_cpp_batched_poll_cardinality PRIVATE moq::cpp)
    add_test(NAME cpp_batched_poll_cardinality
             COMMAND test_cpp_batched_poll_cardinality)

    add_executable(test_cpp_batched_poll_semantics
                   tests/test_batched_poll_semantics.cpp)
    target_link_libraries(test_cpp_batched_poll_semantics PRIVATE moq::cpp)
    add_test(NAME cpp_batched_poll_semantics
             COMMAND test_cpp_batched_poll_semantics)

    add_executable(test_cpp_operations tests/test_operations.cpp)
    target_link_libraries(test_cpp_operations PRIVATE moq::cpp)
    add_test(NAME cpp_operations COMMAND test_cpp_operations)

    add_executable(test_cpp_advanced_operations tests/test_advanced_operations.cpp)
    target_link_libraries(test_cpp_advanced_operations PRIVATE moq::cpp)
    add_test(NAME cpp_advanced_operations COMMAND test_cpp_advanced_operations)

    add_executable(test_cpp_publisher tests/test_publisher.cpp)
    target_link_libraries(test_cpp_publisher PRIVATE moq::cpp)
    add_test(NAME cpp_publisher COMMAND test_cpp_publisher)

    add_executable(test_cpp_subscriber tests/test_subscriber.cpp)
    target_link_libraries(test_cpp_subscriber PRIVATE moq::cpp)
    add_test(NAME cpp_subscriber COMMAND test_cpp_subscriber)

    add_executable(test_cpp_subscriber_status_legacy
        ../../tests/unit/test_subscriber_status_legacy_cpp.cpp)
    target_link_libraries(test_cpp_subscriber_status_legacy PRIVATE moq::cpp)
    add_test(NAME cpp_subscriber_status_legacy COMMAND test_cpp_subscriber_status_legacy)
    set_tests_properties(cpp_subscriber_status_legacy PROPERTIES TIMEOUT 30)

    add_executable(test_cpp_facade_terminal_observations
        tests/test_facade_terminal_observations.cpp)
    target_link_libraries(test_cpp_facade_terminal_observations PRIVATE moq::cpp)
    add_test(NAME cpp_facade_terminal_observations COMMAND test_cpp_facade_terminal_observations)
    set_tests_properties(cpp_facade_terminal_observations PROPERTIES TIMEOUT 30)

    add_executable(test_cpp_subscriber_track_result tests/test_subscriber_track_result.cpp)
    target_link_libraries(test_cpp_subscriber_track_result PRIVATE moq::cpp)
    add_test(NAME cpp_subscriber_track_result COMMAND test_cpp_subscriber_track_result)

    add_executable(test_cpp_subscriber_local_failure tests/test_subscriber_local_failure.cpp)
    target_link_libraries(test_cpp_subscriber_local_failure PRIVATE moq::cpp)
    add_test(NAME cpp_subscriber_local_failure COMMAND test_cpp_subscriber_local_failure)
    set_tests_properties(cpp_subscriber_local_failure PROPERTIES TIMEOUT 30)

    add_executable(test_cpp_fill tests/test_fill.cpp)
    target_link_libraries(test_cpp_fill PRIVATE moq::cpp)
    add_test(NAME cpp_fill COMMAND test_cpp_fill)

    add_executable(test_cpp_d22_surface tests/test_d22_surface.cpp)
    target_link_libraries(test_cpp_d22_surface PRIVATE moq::cpp)
    add_test(NAME cpp_d22_surface COMMAND test_cpp_d22_surface)

    add_executable(test_cpp_session_control tests/test_session_control.cpp)
    target_link_libraries(test_cpp_session_control PRIVATE moq::cpp)
    add_test(NAME cpp_session_control COMMAND test_cpp_session_control)

    add_executable(test_cpp_namespace_updates tests/test_namespace_updates.cpp)
    target_link_libraries(test_cpp_namespace_updates PRIVATE moq::cpp)
    add_test(NAME cpp_namespace_updates COMMAND test_cpp_namespace_updates)

    add_executable(test_cpp_legacy_surface tests/test_legacy_surface.cpp
        ../compatibility/reset_decomposition.cpp
        ../compatibility/action_visitor.cpp)
    target_link_libraries(test_cpp_legacy_surface PRIVATE moq::cpp)
    add_test(NAME cpp_legacy_surface COMMAND test_cpp_legacy_surface)

    add_executable(test_cpp_extended_views tests/test_extended_views.cpp)
    target_link_libraries(test_cpp_extended_views PRIVATE moq::cpp)
    add_test(NAME cpp_extended_views COMMAND test_cpp_extended_views)
    add_executable(test_cpp_outgoing_subgroup_terminal tests/test_outgoing_subgroup_terminal.cpp)
    target_link_libraries(test_cpp_outgoing_subgroup_terminal PRIVATE moq::cpp)
    add_test(NAME cpp_outgoing_subgroup_terminal COMMAND test_cpp_outgoing_subgroup_terminal)
    set_tests_properties(cpp_outgoing_subgroup_terminal PROPERTIES TIMEOUT 30)

    if(TARGET moq::loc)
        add_executable(test_cpp_loc tests/test_loc.cpp)
        target_link_libraries(test_cpp_loc PRIVATE moq::cpp)
        add_test(NAME cpp_loc COMMAND test_cpp_loc)
    endif()

    if(TARGET moq::msf)
        add_executable(test_cpp_msf tests/test_msf.cpp)
        target_link_libraries(test_cpp_msf PRIVATE moq::cpp)
        add_test(NAME cpp_msf COMMAND test_cpp_msf)
    endif()

    if(TARGET moq::cmaf)
        add_executable(test_cpp_cmaf tests/test_cmaf.cpp)
        target_link_libraries(test_cpp_cmaf PRIVATE moq::cpp)
        add_test(NAME cpp_cmaf COMMAND test_cpp_cmaf)
    endif()

    if(TARGET moq::mpeg2ts)
        add_executable(test_cpp_mpeg2ts tests/test_mpeg2ts.cpp)
        target_link_libraries(test_cpp_mpeg2ts PRIVATE moq::cpp)
        add_test(NAME cpp_mpeg2ts COMMAND test_cpp_mpeg2ts)
    endif()

    if(TARGET moq::service AND TARGET moq::playback)
        # The C++ player over a scripted C player, in one TU: it defines the
        # moq_media_player_* functions itself, so it takes the C header's
        # include paths without linking the service library.
        add_executable(test_cpp_media_player_fake tests/test_media_player_fake.cpp)
        target_link_libraries(test_cpp_media_player_fake PRIVATE moq::cpp moq::playback)
        target_include_directories(test_cpp_media_player_fake PRIVATE
            $<TARGET_PROPERTY:moq-service,INTERFACE_INCLUDE_DIRECTORIES>)
        add_test(NAME cpp_media_player_fake COMMAND test_cpp_media_player_fake)

        if(TARGET moq::adapter-picoquic-threaded AND MOQ_PICOQUIC_SOURCE_DIR)
            add_executable(test_cpp_media_player_service tests/test_media_player_service.cpp)
            target_link_libraries(test_cpp_media_player_service PRIVATE
                moq::cpp moq::service moq::adapter-picoquic-threaded moq::loc moq::msf)
            add_test(NAME cpp_media_player_service
                COMMAND test_cpp_media_player_service
                    --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                    --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
            set_tests_properties(cpp_media_player_service PROPERTIES
                LABELS "network" TIMEOUT 60 RESOURCE_LOCK service_network)
        endif()
    endif()

    # Out-of-tree consumer smoke test
    set(_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer")
    set(_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-test-build")
    include(${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerBuildContext.cmake)
    include(${PROJECT_SOURCE_DIR}/tests/cmake/RejectConsumerWarnings.cmake)
    set(_consumer_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-context.cmake")
    moq_write_consumer_context("${_consumer_context}")
    if(MOQ_BUILD_PACKAGE_EXPORTS)
    add_test(NAME cpp_consumer
        COMMAND ${CMAKE_COMMAND}
            "-DSRC=${_consumer_src}" "-DWORK=${_consumer_bld}"
            "-DBUILD=${PROJECT_BINARY_DIR}"
            "-DCONSUMER_CONTEXT=${_consumer_context}"
            "-DPICOQUIC_SOURCE_DIR=${MOQ_PICOQUIC_SOURCE_DIR}"
            "-DPICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer_test.cmake")
    moq_test_reject_consumer_warnings(cpp_consumer)
    endif()

    # The C++ player against an INSTALLED SDK (this tree's library type and
    # the CTest configuration): picoquic and libmoq are installed to a
    # test-owned prefix and the consumer links moq::cpp and moq::service from
    # there only. Both children take this tree's full build context (the
    # same preload cpp_consumer uses) and its OpenSSL selection.
    if(TARGET moq::service AND TARGET moq::playback AND MOQ_PICOQUIC_SOURCE_DIR)
        if(MOQ_BUILD_PACKAGE_EXPORTS)
        include(${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerProviderContext.cmake)
        set(_cpp_player_openssl_context
            "${CMAKE_CURRENT_BINARY_DIR}/player-consumer-openssl-context.cmake")
        moq_write_consumer_openssl_context("${_cpp_player_openssl_context}")
        # The parent's resolved picotls selection, preloaded into the child
        # picoquic build so its own discovery never falls back to another
        # picotls (an entry the parent did not resolve is left out; the
        # runner then requires the child not to have found one either).
        set(_cpp_player_picotls_context
            "${CMAKE_CURRENT_BINARY_DIR}/player-consumer-picotls-context.cmake")
        set(_cpp_player_picotls "# picotls selection of the parent build.\n")
        foreach(_spec "PTLS_INCLUDE_DIR|PATH" "PTLS_CORE_LIBRARY|FILEPATH"
                "PTLS_MINICRYPTO_LIBRARY|FILEPATH" "PTLS_OPENSSL_LIBRARY|FILEPATH"
                "PTLS_FUSION_LIBRARY|FILEPATH")
            string(REPLACE "|" ";" _spec "${_spec}")
            list(GET _spec 0 _var)
            list(GET _spec 1 _type)
            if(DEFINED ${_var} AND NOT "${${_var}}" STREQUAL "" AND
               NOT "${${_var}}" MATCHES "-NOTFOUND$")
                string(APPEND _cpp_player_picotls
                    "set(${_var} [==[${${_var}}]==] CACHE ${_type} \"Parent picotls selection\")\n")
            endif()
        endforeach()
        file(WRITE "${_cpp_player_picotls_context}" "${_cpp_player_picotls}")
        include(${PROJECT_SOURCE_DIR}/tests/cmake/PicoquicInstallStaging.cmake)
        moq_picoquic_install_staging(_cpp_player_install_staging)
        # Service imports the managed WT components when they were installed.
        # Pin their package just as the runner pins its installed picoquic;
        # an ambient WT SDK must not satisfy this producer's consumer test.
        set(_cpp_player_wtquic_dir "")
        if(TARGET moq::adapter-wtquic-msquic-managed OR
           TARGET moq::adapter-wtquic-network-managed)
            if(NOT wtquic_DIR OR wtquic_DIR MATCHES "-NOTFOUND$")
                message(FATAL_ERROR
                    "The installed player consumer requires the selected wtquic_DIR")
            endif()
            set(_cpp_player_wtquic_dir "${wtquic_DIR}")
        endif()
        add_test(NAME cpp_player_install_consumer
            COMMAND bash
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_player_install_consumer.sh"
                "${CMAKE_COMMAND}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/player_consumer"
                "${PROJECT_BINARY_DIR}"
                "${MOQ_PICOQUIC_SOURCE_DIR}"
                "$<CONFIG>"
                "${CMAKE_GENERATOR}"
                "${_consumer_context}"
                "${_cpp_player_openssl_context}"
                "${_cpp_player_install_staging}"
                "${CMAKE_INSTALL_INCLUDEDIR}"
                "${CMAKE_INSTALL_LIBDIR}"
                "${PTLS_INCLUDE_DIR}"
                "${MOQ_PTLS_LIB_DIR}"
                "${MOQ_PICOTLS_PREFIX}"
                "${CMAKE_CURRENT_BINARY_DIR}/player-install-consumer-work-$<CONFIG>"
                "${_cpp_player_picotls_context}"
                "${_cpp_player_wtquic_dir}")
        set_tests_properties(cpp_player_install_consumer
            PROPERTIES TIMEOUT 900 RUN_SERIAL TRUE)
        moq_test_reject_consumer_warnings(cpp_player_install_consumer)
        endif()

        # The runner's path and configuration rules, without a build.
        add_test(NAME cpp_player_install_consumer_rules
            COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_player_consumer_lib.sh")
        set_tests_properties(cpp_player_install_consumer_rules
            PROPERTIES TIMEOUT 30 LABELS "tooling")
    endif()
endif()
