cmake_minimum_required(VERSION 3.20)

# A source-tree picoquic makes picoquic-core and picoquic-log mutually
# dependent static archives in this project. Let capable linkers de-duplicate
# CMake's cycle expansion, as the libmoq build itself does.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()

project(moq_pico_wt_adapter_consumer_test LANGUAGES C)

find_package(libmoq REQUIRED COMPONENTS adapter-pico-wt)

add_executable(moq_pico_wt_adapter_consumer_test main.c)
target_link_libraries(moq_pico_wt_adapter_consumer_test
    PRIVATE moq::adapter-pico-wt)

option(MOQ_CONSUMER_WARNINGS_AS_ERRORS
    "Treat warnings in the standalone consumer as errors" OFF)
if(MOQ_CONSUMER_WARNINGS_AS_ERRORS)
    if(MSVC)
        target_compile_options(moq_pico_wt_adapter_consumer_test PRIVATE /W4 /WX)
    else()
        target_compile_options(moq_pico_wt_adapter_consumer_test
            PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif()
endif()
