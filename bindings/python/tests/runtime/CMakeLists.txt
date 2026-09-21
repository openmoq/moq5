# Consumer build of the finite receiver runtime fixture's reference publisher.
# Built against an INSTALLED libmoq service SDK; every dependency input is
# passed explicitly on the configure line (see README.md). Not part of the
# default deterministic test suite.
cmake_minimum_required(VERSION 3.26)
project(moq5_runtime_fixture VERSION 0.1.0 LANGUAGES C)

find_package(libmoq 0.1 CONFIG REQUIRED COMPONENTS service)

option(MOQ5_RUNTIME_WARNINGS_AS_ERRORS "Treat fixture warnings as errors" ON)

add_executable(moq5_runtime_publisher publisher.c)
target_compile_features(moq5_runtime_publisher PRIVATE c_std_11)
target_link_libraries(moq5_runtime_publisher PRIVATE moq::service)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq5_runtime_publisher PRIVATE -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ5_RUNTIME_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
