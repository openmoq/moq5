cmake_minimum_required(VERSION 3.20)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(moq_simple_relay C)
    find_package(libmoq CONFIG REQUIRED COMPONENTS relay adapter-msquic-managed)
endif()
if(NOT UNIX)
    message(FATAL_ERROR "The simple relay example requires POSIX signal handling")
endif()

add_executable(moq_simple_relay main.c)
target_compile_features(moq_simple_relay PRIVATE c_std_11)
target_link_libraries(moq_simple_relay PRIVATE moq::relay moq::adapter-msquic-managed)
set_target_properties(moq_simple_relay PROPERTIES C_EXTENSIONS OFF) # api-boundary-exempt: C dialect, not protocol fields
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(moq_simple_relay PRIVATE -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR AND MOQ_BUILD_TESTS)
    add_subdirectory(tests)
endif()
