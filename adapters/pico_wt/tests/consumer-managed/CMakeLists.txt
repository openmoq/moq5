cmake_minimum_required(VERSION 3.20)
project(moq_pico_wt_managed_consumer_test LANGUAGES C)

# Let capable linkers deduplicate archives without removing dependency edges.
if(POLICY CMP0156)
    cmake_policy(SET CMP0156 NEW)
endif()
if(POLICY CMP0179)
    cmake_policy(SET CMP0179 NEW)
endif()

find_package(libmoq REQUIRED COMPONENTS adapter-pico-wt-managed)

add_executable(moq_pico_wt_managed_consumer_test main.c)
target_link_libraries(moq_pico_wt_managed_consumer_test
    PRIVATE moq::adapter-pico-wt-managed)
