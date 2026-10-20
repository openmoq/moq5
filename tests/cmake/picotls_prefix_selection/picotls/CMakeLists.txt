# Stand-in for the picotls that picoquic builds with PICOQUIC_FETCH_PTLS.
# MOQ_TEST_PTLS_LAYOUT selects where its archives are written:
#   default | custom (ARCHIVE_OUTPUT_DIRECTORY) | config
#   (ARCHIVE_OUTPUT_DIRECTORY_RELEASE beside a generic one) | relative
#   (a relative ARCHIVE_OUTPUT_DIRECTORY) | shared (shared libraries,
#   LIBRARY_OUTPUT_DIRECTORY) | split (one archive elsewhere).
# Probe targets, built on request only, show the compile flags this directory
# gets; the caller-hook scope variable and the options visible at the end of
# project() are recorded as global properties.
cmake_minimum_required(VERSION 3.20)
project(picotls C)
get_property(_moq_options DIRECTORY PROPERTY COMPILE_OPTIONS)
set_property(GLOBAL PROPERTY MOQ_TEST_PTLS_OPTIONS "${_moq_options}")
set_property(GLOBAL PROPERTY MOQ_TEST_PTLS_SCOPE "${MOQ_TEST_CALLER_SCOPE}")

set(_moq_layout "${MOQ_TEST_PTLS_LAYOUT}")
set(_moq_type STATIC)
if(_moq_layout STREQUAL "shared")
    set(_moq_type SHARED)
endif()
set(_moq_targets picotls-core picotls-openssl picotls-minicrypto)
foreach(_moq_t IN LISTS _moq_targets)
    add_library(${_moq_t} ${_moq_type} lib.c)
endforeach()
add_library(picotls::core ALIAS picotls-core)
if(_moq_layout STREQUAL "custom")
    set_target_properties(${_moq_targets} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/custom")
elseif(_moq_layout STREQUAL "config")
    set_target_properties(${_moq_targets} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/generic"
        ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${CMAKE_CURRENT_BINARY_DIR}/cfg")
elseif(_moq_layout STREQUAL "relative")
    set_target_properties(${_moq_targets} PROPERTIES ARCHIVE_OUTPUT_DIRECTORY rel)
elseif(_moq_layout STREQUAL "shared")
    set_target_properties(${_moq_targets} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/shlib"
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/arch")
elseif(_moq_layout STREQUAL "split")
    set_target_properties(picotls-minicrypto PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/other")
endif()

add_library(picotls-ndebug-probe STATIC EXCLUDE_FROM_ALL ndebug_probe.c)
add_library(picotls-werror-probe STATIC EXCLUDE_FROM_ALL werror_probe.c)
