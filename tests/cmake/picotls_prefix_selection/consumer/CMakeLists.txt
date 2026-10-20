# Resolves picoquic through libmoq's FindPicoquic, either in source-tree mode
# against the stand-in provider or through a stand-in installed CONFIG
# package, then records the header and library selections FindPicoquic
# publishes for libmoq's own use (cache entries and the pkg-config link
# closure) and the project-hook state around it. Options shape the
# surroundings:
#   MOQ_TEST_UNRELATED_FETCH  populate a picotls through FetchContent first,
#                             unrelated to the picoquic that is selected;
#   MOQ_TEST_PRE_ADD          add the stand-in picoquic before
#                             find_package, so it is a pre-existing target;
#   MOQ_TEST_FIND_TWICE       call find_package(Picoquic) a second time;
#   MOQ_TEST_NESTED           find_package from a subdirectory, below the
#                             PTLS_*_LIBRARY values the fetched FindPTLS sets
#                             (MOQ_TEST_NESTED_CORE replaces the core one);
#   MOQ_TEST_HOOK_DIR         added to CMAKE_MODULE_PATH for caller hooks;
#   MOQ_TEST_BINDING          unset | empty | set | cache | over: how the
#                             caller binds the four hook variables (cache and
#                             over also need the driver's cache preload); the
#                             bindings are checked after find_package.
cmake_minimum_required(VERSION 3.20)
project(moq_picotls_prefix_selection LANGUAGES C)

list(APPEND CMAKE_MODULE_PATH "${MOQ_TEST_MODULE_DIR}")
if(MOQ_TEST_HOOK_DIR)
    list(APPEND CMAKE_MODULE_PATH "${MOQ_TEST_HOOK_DIR}")
endif()
if(MOQ_TEST_UNRELATED_FETCH)
    include(FetchContent)
    FetchContent_Declare(picotls
        GIT_REPOSITORY "file:///moq-no-network-fetch.invalid" GIT_TAG none)
    FetchContent_MakeAvailable(picotls)
endif()
if(MOQ_TEST_PRE_ADD)
    add_subdirectory("${MOQ_PICOQUIC_SOURCE_DIR}" "${CMAKE_BINARY_DIR}/pre-picoquic")
endif()
add_library(consumer-isolation-probe STATIC EXCLUDE_FROM_ALL
    "${MOQ_TEST_FIXTURE}/isolation_probe.c")
add_library(consumer-isolation-extra-probe STATIC EXCLUDE_FROM_ALL
    "${MOQ_TEST_FIXTURE}/isolation_probe_extra.c")
set(_moq_binding_vars CMAKE_PROJECT_picoquic_INCLUDE MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE
    CMAKE_PROJECT_picotls_INCLUDE MOQ_PICOTLS_CALLER_PROJECT_INCLUDE)
if(MOQ_TEST_BINDING MATCHES "^(set|over)$")
    foreach(_moq_v IN LISTS _moq_binding_vars)
        set(${_moq_v} "${MOQ_TEST_HOOK_DIR}/noop_normal.cmake")
    endforeach()
elseif(MOQ_TEST_BINDING STREQUAL "empty")
    foreach(_moq_v IN LISTS _moq_binding_vars)
        set(${_moq_v} "")
    endforeach()
endif()
if(MOQ_TEST_NESTED)
    set(PTLS_CORE_LIBRARY picotls-core)
    if(MOQ_TEST_NESTED_CORE)
        set(PTLS_CORE_LIBRARY "${MOQ_TEST_NESTED_CORE}")
    endif()
    set(PTLS_MINICRYPTO_LIBRARY picotls-minicrypto)
    set(PTLS_OPENSSL_LIBRARY picotls-openssl)
    add_subdirectory(nested)
else()
    include("${CMAKE_CURRENT_LIST_DIR}/find_and_record.cmake")
endif()
