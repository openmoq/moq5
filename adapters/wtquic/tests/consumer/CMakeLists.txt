cmake_minimum_required(VERSION 3.16)
project(moq_wtquic_consumer LANGUAGES C)

option(WITH_NETWORK_MANAGED
    "Also consume the adapter-wtquic-network-managed component" OFF)
option(WITH_MSQUIC_MANAGED
    "Also consume the adapter-wtquic-msquic-managed component" OFF)

# Request ONLY the managed component(s) when asked: the base moq::adapter-wtquic
# target must then be pulled in TRANSITIVELY by libmoqConfig's managed-component
# block (its `if(NOT TARGET moq::adapter-wtquic)` path). The base consumer below
# still links moq::adapter-wtquic, proving that transitive load happened.
set(_components "")
if(WITH_MSQUIC_MANAGED)
    list(APPEND _components adapter-wtquic-msquic-managed)
endif()
if(WITH_NETWORK_MANAGED)
    list(APPEND _components adapter-wtquic-network-managed)
endif()
if(NOT _components)
    set(_components adapter-wtquic)
endif()
find_package(libmoq REQUIRED COMPONENTS ${_components})

add_executable(moq_wtquic_consumer_test main.c)
target_link_libraries(moq_wtquic_consumer_test PRIVATE moq::adapter-wtquic)

if(WITH_NETWORK_MANAGED)
    add_executable(moq_wtquic_network_consumer_test main_network.c)
    target_link_libraries(moq_wtquic_network_consumer_test PRIVATE
        moq::adapter-wtquic-network-managed)
    enable_language(CXX)
    add_executable(moq_wtquic_network_consumer_test_cxx main_network.cpp)
    target_link_libraries(moq_wtquic_network_consumer_test_cxx PRIVATE
        moq::adapter-wtquic-network-managed)
    set_target_properties(moq_wtquic_network_consumer_test_cxx PROPERTIES
        CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
endif()

if(WITH_MSQUIC_MANAGED)
    add_executable(moq_wtquic_msquic_consumer_test main_msquic.c)
    target_link_libraries(moq_wtquic_msquic_consumer_test PRIVATE
        moq::adapter-wtquic-msquic-managed)
    enable_language(CXX)
    add_executable(moq_wtquic_msquic_consumer_test_cxx main_msquic.cpp)
    target_link_libraries(moq_wtquic_msquic_consumer_test_cxx PRIVATE
        moq::adapter-wtquic-msquic-managed)
    set_target_properties(moq_wtquic_msquic_consumer_test_cxx PROPERTIES
        CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
endif()
