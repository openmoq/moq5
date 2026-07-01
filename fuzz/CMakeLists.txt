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

add_executable(fuzz_session_control fuzz_session_control.c)
target_link_libraries(fuzz_session_control PRIVATE moq::core)
target_compile_options(fuzz_session_control PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_session_control PRIVATE ${FUZZ_FLAGS})

add_executable(fuzz_session_data fuzz_session_data.c)
target_link_libraries(fuzz_session_data PRIVATE moq::core)
target_compile_options(fuzz_session_data PRIVATE ${FUZZ_FLAGS})
target_link_options(fuzz_session_data PRIVATE ${FUZZ_FLAGS})

# Smoke tests: single-iteration startup validation.
add_test(NAME fuzz_quic_varint_smoke
         COMMAND fuzz_quic_varint -runs=1 -max_len=0)
add_test(NAME fuzz_kvp_decode_smoke
         COMMAND fuzz_kvp_decode -runs=1 -max_len=0)
add_test(NAME fuzz_control_d16_smoke
         COMMAND fuzz_control_d16 -runs=1 -max_len=0)
add_test(NAME fuzz_session_control_smoke
         COMMAND fuzz_session_control -runs=1 -max_len=0)
add_test(NAME fuzz_session_data_smoke
         COMMAND fuzz_session_data -runs=1 -max_len=0)

# Non-empty corpus smoke: prove the session targets reach the parser.
add_test(NAME fuzz_session_control_corpus
         COMMAND fuzz_session_control
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/session_control -runs=0)
add_test(NAME fuzz_session_data_corpus
         COMMAND fuzz_session_data
         ${CMAKE_CURRENT_SOURCE_DIR}/corpus/session_data -runs=0)

message(STATUS "Fuzz targets: 5 targets, 7 smoke tests")
