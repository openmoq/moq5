cmake_minimum_required(VERSION 3.20)

# A source-tree picoquic makes picoquic-core and picoquic-log mutually
# dependent static archives in this project. Let capable linkers de-duplicate
# CMake's cycle expansion, as the libmoq build itself does.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()

project(moq_pico_wt_managed_consumer_test LANGUAGES C)

find_package(libmoq REQUIRED COMPONENTS adapter-pico-wt-managed)

add_executable(moq_pico_wt_managed_consumer_test main.c)
target_link_libraries(moq_pico_wt_managed_consumer_test
    PRIVATE moq::adapter-pico-wt-managed)
