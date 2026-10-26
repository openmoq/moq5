# Fuzz targets for moq-core.
# Note: intentionally NOT covered by MOQ_WARNINGS_AS_ERRORS - fuzz targets
# build with a dedicated -fsanitize=fuzzer toolchain where -Werror is
# orthogonal to the goal (finding crashes). See the option in the
# top-level CMakeLists for the full warnings-as-errors scope/policy.
# Built only when MOQ_BUILD_FUZZ=ON and a supported fuzzer is available.
# Set MOQ_REQUIRE_FUZZ=ON to make missing Clang/libFuzzer a fatal error.

if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang")
    if(MOQ_REQUIRE_FUZZ)
        message(FATAL_ERROR
            "MOQ_REQUIRE_FUZZ=ON but compiler is not Clang. "
            "Set CC to a Clang compiler.")
    endif()
    message(STATUS "Fuzz targets require Clang; skipping.")
    return()
endif()

set(FUZZ_FLAGS -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer)
set(FUZZ_LIB_FLAGS -fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer)
set(FUZZ_REQUIRED_FLAGS "-fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer")

include(CheckCSourceCompiles)
set(CMAKE_REQUIRED_FLAGS "${FUZZ_REQUIRED_FLAGS}")
check_c_source_compiles("
#include <stddef.h>
#include <stdint.h>
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    (void)data;
    (void)size;
    return 0;
}
" MOQ_HAS_LIBFUZZER_RUNTIME)
unset(CMAKE_REQUIRED_FLAGS)

if(NOT MOQ_HAS_LIBFUZZER_RUNTIME)
    if(MOQ_REQUIRE_FUZZ)
        message(FATAL_ERROR
            "MOQ_REQUIRE_FUZZ=ON but libFuzzer runtime is not linkable. "
            "Install upstream LLVM Clang (brew install llvm) and set "
            "CC=/opt/homebrew/opt/llvm/bin/clang.")
    endif()
    message(STATUS
        "libFuzzer runtime is not linkable with this compiler; "
        "skipping fuzz targets. Set MOQ_REQUIRE_FUZZ=ON to make this fatal.")
    return()
endif()

# Instrument the in-tree libraries linked by the fuzz harnesses so coverage-
# guided mutation and ASan/UBSan checks reach into the core, media, and relay
# parsers rather than stopping at the harness translation units. A harness
# that needs internal symbols links moq-core-test-internals, which compiles
# the core sources again, so that copy is instrumented the same way.
foreach(_fuzz_lib
        moq-core
        moq-core-test-internals
        moq-loc
        moq-cmaf
        moq-mpeg2ts
        moq-cmaf-packager
        moq-codec-signaling
        moq-media-object
        moq-msf
        moq-relay-core
        moq-relay
        moq-relay-policy
        moqr-admin)
    if(TARGET ${_fuzz_lib})
        target_compile_options(${_fuzz_lib} PRIVATE ${FUZZ_LIB_FLAGS})
        # Installed archives retain instrumentation too. Their consumers need
        # its runtime, but must never inherit libFuzzer's main function.
        target_link_options(${_fuzz_lib} PUBLIC ${FUZZ_LIB_FLAGS})
    endif()
endforeach()

add_executable(fuzz_quic_varint fuzz_quic_varint.c)
target_link_libraries(fuzz_quic_varint PRIVATE moq::core)
target_compile_options(fuzz_quic_varint PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_quic_varint PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_kvp_decode fuzz_kvp_decode.c)
target_link_libraries(fuzz_kvp_decode PRIVATE moq::core)
target_compile_options(fuzz_kvp_decode PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_kvp_decode PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_control_d16 fuzz_control_d16.c)
target_link_libraries(fuzz_control_d16 PRIVATE moq::core)
target_compile_options(fuzz_control_d16 PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_control_d16 PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_control_d18 fuzz_control_d18.c)
target_link_libraries(fuzz_control_d18 PRIVATE moq::core)
target_compile_options(fuzz_control_d18 PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_control_d18 PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_control_d22 fuzz_control_d22.c)
target_link_libraries(fuzz_control_d22 PRIVATE moq-core-test-internals)
target_compile_options(fuzz_control_d22 PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_control_d22 PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_session_control fuzz_session_control.c)
target_link_libraries(fuzz_session_control PRIVATE moq::core)
target_compile_options(fuzz_session_control PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_session_control PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_session_data fuzz_session_data.c)
target_link_libraries(fuzz_session_data PRIVATE moq::core)
target_compile_options(fuzz_session_data PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_session_data PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_session_d18 fuzz_session_d18.c)
target_link_libraries(fuzz_session_d18 PRIVATE moq::core)
target_compile_options(fuzz_session_d18 PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_session_d18 PRIVATE ${FUZZ_FLAGS})

# The PicoQUIC lane group's datagram router: a pure function of the bytes
# with no transport dependency, compiled here from its source so the fuzz
# tree needs no adapter.
add_executable(fuzz_pq_cid_route fuzz_pq_cid_route.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../adapters/picoquic/pq_cid_route.c)
target_include_directories(fuzz_pq_cid_route PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../adapters/picoquic)
target_compile_options(fuzz_pq_cid_route PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_pq_cid_route PRIVATE ${FUZZ_FLAGS})
set(_fuzz_target_count 9)
set(_fuzz_test_count 15)

# The relay admin parser, built only when the relay tier is in this tree.
if(TARGET moqr-admin)
    add_executable(fuzz_relay_admin_http fuzz_relay_admin_http.c)
    target_link_libraries(fuzz_relay_admin_http PRIVATE moqr-admin)
    target_compile_options(fuzz_relay_admin_http PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_relay_admin_http PRIVATE ${FUZZ_FLAGS})
    add_test(NAME fuzz_relay_admin_http_smoke
             COMMAND fuzz_relay_admin_http -runs=1 -max_len=0)
    add_test(NAME fuzz_relay_admin_http_corpus
             COMMAND fuzz_relay_admin_http
             ${CMAKE_CURRENT_SOURCE_DIR}/corpus/relay_admin_http -runs=0)
    math(EXPR _fuzz_target_count "${_fuzz_target_count} + 1")
    math(EXPR _fuzz_test_count "${_fuzz_test_count} + 2")
endif()

if(TARGET moq-loc)
    add_executable(fuzz_loc fuzz_loc.c)
    target_link_libraries(fuzz_loc PRIVATE moq::loc)
    target_compile_options(fuzz_loc PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_loc PRIVATE ${FUZZ_FLAGS})
    add_test(NAME fuzz_loc_smoke
             COMMAND fuzz_loc -runs=1 -max_len=0)
    add_test(NAME fuzz_loc_corpus
             COMMAND fuzz_loc
             ${CMAKE_CURRENT_SOURCE_DIR}/corpus/loc -runs=0)
    math(EXPR _fuzz_target_count "${_fuzz_target_count} + 1")
    math(EXPR _fuzz_test_count "${_fuzz_test_count} + 2")
endif()

if(TARGET moq-cmaf)
    add_executable(fuzz_cmaf fuzz_cmaf.c)
    target_link_libraries(fuzz_cmaf PRIVATE moq::cmaf)
    target_compile_options(fuzz_cmaf PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_cmaf PRIVATE ${FUZZ_FLAGS})
    add_test(NAME fuzz_cmaf_smoke
             COMMAND fuzz_cmaf -runs=1 -max_len=0)
    add_test(NAME fuzz_cmaf_corpus
             COMMAND fuzz_cmaf
             ${CMAKE_CURRENT_SOURCE_DIR}/corpus/cmaf -runs=0)
    math(EXPR _fuzz_target_count "${_fuzz_target_count} + 1")
    math(EXPR _fuzz_test_count "${_fuzz_test_count} + 2")
endif()

if(TARGET moq-msf)
    add_executable(fuzz_msf fuzz_msf.c)
    target_link_libraries(fuzz_msf PRIVATE moq::msf)
    if(TARGET moq-media-object AND TARGET moq-cmaf)
        target_compile_definitions(fuzz_msf PRIVATE MOQ_FUZZ_MSF_HAS_MEDIA=1)
    endif()
    target_compile_options(fuzz_msf PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_msf PRIVATE ${FUZZ_FLAGS})
    add_test(NAME fuzz_msf_smoke
             COMMAND fuzz_msf -runs=1 -max_len=0)
    add_test(NAME fuzz_msf_corpus
             COMMAND fuzz_msf
             ${CMAKE_CURRENT_SOURCE_DIR}/corpus/msf -runs=0)
    math(EXPR _fuzz_target_count "${_fuzz_target_count} + 1")
    math(EXPR _fuzz_test_count "${_fuzz_test_count} + 2")
endif()

# MPEG-2 TS helpers and catalog extensions have independent feature gates.
if(TARGET moq::mpeg2ts)
    add_executable(fuzz_mpeg2ts fuzz_mpeg2ts.c)
    target_link_libraries(fuzz_mpeg2ts PRIVATE moq::mpeg2ts)
    target_compile_options(fuzz_mpeg2ts PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_mpeg2ts PRIVATE ${FUZZ_FLAGS})
    add_test(NAME fuzz_mpeg2ts_smoke
             COMMAND fuzz_mpeg2ts -runs=1 -max_len=0)
    add_test(NAME fuzz_mpeg2ts_corpus
             COMMAND fuzz_mpeg2ts ${CMAKE_CURRENT_SOURCE_DIR}/corpus/mpeg2ts -runs=0)
    math(EXPR _fuzz_target_count "${_fuzz_target_count} + 1")
    math(EXPR _fuzz_test_count "${_fuzz_test_count} + 2")
endif()
if(TARGET moq::msf)
    add_executable(fuzz_msf_mpeg2ts fuzz_msf_mpeg2ts.c)
    target_link_libraries(fuzz_msf_mpeg2ts PRIVATE moq::msf)
    target_compile_options(fuzz_msf_mpeg2ts PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_msf_mpeg2ts PRIVATE ${FUZZ_FLAGS})
    add_test(NAME fuzz_msf_mpeg2ts_smoke
             COMMAND fuzz_msf_mpeg2ts -runs=1 -max_len=0)
    add_test(NAME fuzz_msf_mpeg2ts_corpus
             COMMAND fuzz_msf_mpeg2ts ${CMAKE_CURRENT_SOURCE_DIR}/corpus/msf_mpeg2ts -runs=0)
    math(EXPR _fuzz_target_count "${_fuzz_target_count} + 1")
    math(EXPR _fuzz_test_count "${_fuzz_test_count} + 2")
endif()

# Smoke tests: single-iteration startup validation.
add_test(NAME fuzz_quic_varint_smoke
         COMMAND fuzz_quic_varint -runs=1 -max_len=0)
add_test(NAME fuzz_kvp_decode_smoke
         COMMAND fuzz_kvp_decode -runs=1 -max_len=0)
add_test(NAME fuzz_control_d16_smoke
         COMMAND fuzz_control_d16 -runs=1 -max_len=0)
add_test(NAME fuzz_control_d18_smoke
         COMMAND fuzz_control_d18 -runs=1 -max_len=0)
add_test(NAME fuzz_control_d22_smoke
         COMMAND fuzz_control_d22 -runs=1 -max_len=0)
add_test(NAME fuzz_session_control_smoke
         COMMAND fuzz_session_control -runs=1 -max_len=0)
add_test(NAME fuzz_session_data_smoke
         COMMAND fuzz_session_data -runs=1 -max_len=0)
add_test(NAME fuzz_session_d18_smoke
         COMMAND fuzz_session_d18 -runs=1 -max_len=0)
add_test(NAME fuzz_pq_cid_route_smoke
         COMMAND fuzz_pq_cid_route -runs=1 -max_len=0)

# Non-empty corpus smoke: prove the targets reach the parser.
add_test(NAME fuzz_control_d18_corpus
         COMMAND fuzz_control_d18
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/control_d18 -runs=0)
add_test(NAME fuzz_control_d22_corpus
         COMMAND fuzz_control_d22
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/control_d22 -runs=0)
add_test(NAME fuzz_session_control_corpus
         COMMAND fuzz_session_control
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/session_control -runs=0)
add_test(NAME fuzz_session_data_corpus
         COMMAND fuzz_session_data
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/session_data -runs=0)
add_test(NAME fuzz_session_d18_corpus
         COMMAND fuzz_session_d18
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/session_d18 -runs=0)
add_test(NAME fuzz_pq_cid_route_corpus
         COMMAND fuzz_pq_cid_route
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/pq_cid_route -runs=0)

message(STATUS
    "Fuzz targets: ${_fuzz_target_count} targets, "
    "${_fuzz_test_count} smoke/corpus tests")
