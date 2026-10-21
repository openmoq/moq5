cmake_minimum_required(VERSION 3.16)
project(moq_msquic_consumer LANGUAGES C)

# Request both components, but link managed only: its interface owns attach.
# main.c resolves symbols from both surfaces without duplicating the archive.
find_package(libmoq REQUIRED COMPONENTS adapter-msquic adapter-msquic-managed)

add_executable(moq_msquic_consumer_test main.c)
target_link_libraries(moq_msquic_consumer_test PRIVATE
    moq::adapter-msquic-managed)
if(CMAKE_C_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$" AND NOT MSVC
   AND NOT CMAKE_C_COMPILER_FRONTEND_VARIANT STREQUAL "MSVC")
    target_compile_options(moq_msquic_consumer_test PRIVATE
        -Wall -Wextra -Wpedantic -Werror)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_compile_definitions(moq_msquic_consumer_test PRIVATE _GNU_SOURCE)
    target_link_libraries(moq_msquic_consumer_test PRIVATE ${CMAKE_DL_LIBS})
endif()

# Record resolved target artifacts, not inferred package directory layouts.
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/consumer_targets.cmake" CONTENT
"set(CONSUMER_PROVIDER_FILE [==[$<TARGET_FILE:msquic::msquic>]==])
set(CONSUMER_ADAPTER_FILE [==[$<TARGET_FILE:moq::adapter-msquic>]==])
set(CONSUMER_ADAPTER_TYPE [==[$<TARGET_PROPERTY:moq::adapter-msquic,TYPE>]==])
set(CONSUMER_CORE_FILE [==[$<TARGET_FILE:moq::core>]==])
set(CONSUMER_CORE_TYPE [==[$<TARGET_PROPERTY:moq::core,TYPE>]==])
")
