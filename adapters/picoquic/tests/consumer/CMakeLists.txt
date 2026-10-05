cmake_minimum_required(VERSION 3.20)
project(moq_adapter_consumer_test LANGUAGES C)

# Let capable linkers deduplicate archives without removing dependency edges.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()
if(POLICY CMP0179)
    cmake_policy(SET CMP0179 NEW)
endif()

find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)

add_executable(moq_adapter_consumer_test main.c)
target_link_libraries(moq_adapter_consumer_test PRIVATE moq::adapter-picoquic)

option(MOQ_CONSUMER_WARNINGS_AS_ERRORS
    "Treat warnings in the standalone consumer as errors" OFF)
if(MOQ_CONSUMER_WARNINGS_AS_ERRORS)
    if(MSVC)
        target_compile_options(moq_adapter_consumer_test PRIVATE /W4 /WX)
    else()
        target_compile_options(moq_adapter_consumer_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif()
endif()
