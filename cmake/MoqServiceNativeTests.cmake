# Requires the real relay and both managed providers, after their targets exist.
if(NOT UNIX OR NOT MOQ_BUILD_TESTS OR NOT TARGET moq::service
   OR NOT TARGET moq::cmaf OR NOT TARGET moqr-admin
   OR NOT TARGET moq::adapter-msquic-managed
   OR NOT TARGET moq::adapter-wtquic-msquic-managed)
    return()
endif()

find_package(Threads REQUIRED)
set(_moq_native_fixture "${PROJECT_SOURCE_DIR}/service/tests")
set(_moq_native_cli "${PROJECT_SOURCE_DIR}/tools/moq5-relay/cli")
add_library(media_relay_native_host STATIC
    "${_moq_native_fixture}/media_relay_native_host.c"
    "${PROJECT_SOURCE_DIR}/tools/moq5-relay/tests/fill_deadline_cli_tu.c"
    "${_moq_native_cli}/admin_listen.c"
    "${_moq_native_cli}/cliargs.c"
    "${_moq_native_cli}/config.c"
    "${_moq_native_cli}/admin_listen_size.c"
    "${_moq_native_cli}/info_doc.c"
    "${_moq_native_cli}/shards_doc.c"
    "${_moq_native_cli}/versions.c"
    "${_moq_native_cli}/lanestats.c"
    "${_moq_native_cli}/logevent.c"
    "${_moq_native_cli}/servelog.c"
    "${_moq_native_cli}/snapshot.c"
    "${_moq_native_cli}/broker.c"
    "${_moq_native_cli}/conn_reap.c"
    "${_moq_native_cli}/wtcfg.c")
target_include_directories(media_relay_native_host PRIVATE
    "${_moq_native_cli}" "${PROJECT_SOURCE_DIR}/media/msf/vendor")
target_compile_definitions(media_relay_native_host PRIVATE
    MOQR_PUMP_TESTING MOQR_DUAL_LISTENER)
target_link_libraries(media_relay_native_host PRIVATE moqr-admin
    moq::adapter-msquic-managed moq::adapter-wtquic-msquic-managed
    Threads::Threads)

add_executable(test_media_relay_native
    "${_moq_native_fixture}/test_media_relay_native.c")
target_link_libraries(test_media_relay_native PRIVATE
    moq::service media_relay_native_host)
foreach(_moq_native_target media_relay_native_host test_media_relay_native)
    set_target_properties(${_moq_native_target} PROPERTIES
        C_STANDARD 11 C_STANDARD_REQUIRED YES C_EXTENSIONS OFF)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(${_moq_native_target} PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
endforeach()

set(_moq_native_cert "${PROJECT_SOURCE_DIR}/adapters/msquic/tests/test_only_loopback_cert.pem")
set(_moq_native_key "${PROJECT_SOURCE_DIR}/adapters/msquic/tests/test_only_loopback_key.pem")
if(NOT EXISTS "${_moq_native_cert}" OR NOT EXISTS "${_moq_native_key}")
    message(FATAL_ERROR "Native media relay test credentials are missing")
endif()
foreach(_moq_native_row d18_raw d22_raw d22_wt d22_split d22_cmaf d18_push
        d22_push d18_complete d22_complete d18_complete_now d22_complete_now
        d18_complete_slow d22_complete_slow)
    add_test(NAME media_relay_native_${_moq_native_row}
        COMMAND test_media_relay_native
            "${_moq_native_cert}" "${_moq_native_key}" ${_moq_native_row})
    set_tests_properties(media_relay_native_${_moq_native_row} PROPERTIES
        TIMEOUT 100 RUN_SERIAL TRUE LABELS "service;relay;native")
endforeach()
unset(_moq_native_fixture)
unset(_moq_native_cli)
unset(_moq_native_target)
unset(_moq_native_cert)
unset(_moq_native_key)
unset(_moq_native_row)
