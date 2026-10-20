# Stand-in for a picoquic source tree. Its picotls lookups have the same
# shape as picoquic's cmake/FindPTLS.cmake: prebuilt picotls through a header
# hint one level too deep, fallbacks beside the top-level source and build
# trees and cached results kept; with PICOQUIC_FETCH_PTLS, picotls from
# FetchContent, library names that are its targets, and PTLS_INCLUDE_DIR
# required (but not set) in the mbedTLS configuration. It records what the
# provider ends up with.
cmake_minimum_required(VERSION 3.20)
project(picoquic C)

if(PICOQUIC_FETCH_PTLS)
    include(FetchContent)
    # Without FETCHCONTENT_SOURCE_DIR_PICOTLS the source is cloned from
    # MOQ_TEST_PICOTLS_REPO, a local repository.
    if(NOT MOQ_TEST_PICOTLS_REPO)
        set(MOQ_TEST_PICOTLS_REPO "file:///moq-no-network-fetch.invalid")
        set(MOQ_TEST_PICOTLS_TAG none)
    endif()
    FetchContent_Declare(picotls
        GIT_REPOSITORY "${MOQ_TEST_PICOTLS_REPO}" GIT_TAG "${MOQ_TEST_PICOTLS_TAG}")
    FetchContent_MakeAvailable(picotls)
    set(PTLS_CORE_LIBRARY picotls-core)
    set(PTLS_MINICRYPTO_LIBRARY picotls-minicrypto)
    set(PTLS_OPENSSL_LIBRARY picotls-openssl)
    set(PTLS_FUSION_LIBRARY "")
    if(WITH_MBEDTLS AND NOT PTLS_INCLUDE_DIR)
        message(FATAL_ERROR "Could NOT find PTLS (missing: PTLS_INCLUDE_DIR)")
    endif()
    set(PTLS_LIBRARIES picotls-core picotls-openssl picotls-minicrypto)
    if(MOQ_TEST_PQ_LINK_ALIAS)
        set(PTLS_LIBRARIES "$<LINK_ONLY:picotls::core>" picotls-openssl picotls-minicrypto)
    endif()
else()
    find_path(PTLS_INCLUDE_DIR
        NAMES picotls/minicrypto.h
        HINTS ${PTLS_PREFIX}/include/picotls
            ${CMAKE_SOURCE_DIR}/../picotls/include
            ${CMAKE_BINARY_DIR}/../picotls/include
            ../picotls/include/)
    set(PTLS_HINTS ${PTLS_PREFIX}/lib ${CMAKE_BINARY_DIR}/../picotls ../picotls)
    find_library(PTLS_CORE_LIBRARY picotls-core HINTS ${PTLS_HINTS})
    find_library(PTLS_MINICRYPTO_LIBRARY picotls-minicrypto HINTS ${PTLS_HINTS})
    find_library(PTLS_OPENSSL_LIBRARY picotls-openssl HINTS ${PTLS_HINTS})
    find_library(PTLS_FUSION_LIBRARY picotls-fusion HINTS ${PTLS_HINTS})
    set(PTLS_LIBRARIES "")
    foreach(_lib PTLS_CORE_LIBRARY PTLS_OPENSSL_LIBRARY PTLS_MINICRYPTO_LIBRARY)
        if(${_lib})
            list(APPEND PTLS_LIBRARIES "${${_lib}}")
        endif()
    endforeach()
endif()

file(WRITE "${MOQ_TEST_RESULT}"
    "INC=${PTLS_INCLUDE_DIR}\n"
    "CORE=${PTLS_CORE_LIBRARY}\n"
    "MINICRYPTO=${PTLS_MINICRYPTO_LIBRARY}\n"
    "OPENSSL=${PTLS_OPENSSL_LIBRARY}\n"
    "FUSION=${PTLS_FUSION_LIBRARY}\n")

add_library(picoquic-core INTERFACE)
target_link_libraries(picoquic-core INTERFACE ${PTLS_LIBRARIES})

# Compile flags outside picotls stay the caller's: NDEBUG as the configuration
# defines it, warnings not errors.
add_library(picoquic-isolation-probe STATIC EXCLUDE_FROM_ALL
    "${MOQ_TEST_FIXTURE}/isolation_probe.c")
