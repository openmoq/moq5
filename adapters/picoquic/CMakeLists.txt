list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
find_package(Picoquic REQUIRED)

# Resolve the picoquic target name.
if(TARGET picoquic::picoquic-core)
    set(_PQ picoquic::picoquic-core)
elseif(TARGET picoquic-core)
    set(_PQ picoquic-core)
elseif(TARGET Picoquic::picoquic-core)
    set(_PQ Picoquic::picoquic-core)
else()
    message(FATAL_ERROR "No picoquic-core target available")
endif()

add_library(moq-adapter-picoquic
    moq_picoquic.c picoquic_endpoint.c moq_picoquic_verify.c
    ../common/moq_pq_send_queue.c)
add_library(moq::adapter-picoquic ALIAS moq-adapter-picoquic)

# Only the MOQ_PQ_API declarations -- <moq/picoquic.h>, <moq/picoquic_verify.h>
# and the private SPI in picoquic_spi.h -- are exported; every other symbol is
# internal. MOQ_PQ_BUILDING (not the core's MOQ_BUILDING) marks this library
# as their producer, so the core's MOQ_API functions it calls stay imports.
set_target_properties(moq-adapter-picoquic PROPERTIES C_VISIBILITY_PRESET hidden)
target_compile_definitions(moq-adapter-picoquic PRIVATE MOQ_PQ_BUILDING)

target_include_directories(moq-adapter-picoquic
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# moq_picoquic_verify.c wraps the TLS verifier supplied by the configured
# picoquic crypto backend. OpenSSL remains the default for source compatibility;
# mbedTLS is selected explicitly, or automatically when picoquic is configured
# with WITH_OPENSSL=OFF and WITH_MBEDTLS=ON.
# ONE normalized provider record, published as cache-internal so every consumer
# directory sees the same values regardless of processing order. The selected
# verifier and the two upstream capability facts are distinct: picoquic can be
# built with both providers enabled while LibMoQ selects one verifier.
#
# ABSENCE OF EVIDENCE IS NOT `OFF`. A source-tree picoquic defines WITH_OPENSSL
# and WITH_MBEDTLS in this scope; an INSTALLED or already-defined picoquic
# target does not, and mapping that silence to OFF would publish a provider
# matrix that is demonstrably false -- an installed package whose config runs
# find_dependency(OpenSSL) would be recorded as OpenSSL-incapable. So an
# unavailable fact fails closed and the caller supplies it explicitly.
#
# What this record IS: the upstream capability facts as stated by the build
# that produced the picoquic in use. What it is NOT: a proof that the picoquic
# artifact actually linked those providers. Verifying the artifact would mean
# inspecting the imported target's dependency closure, which is
# package-compatibility work and is deliberately out of scope here; the record
# therefore carries its own provenance rather than implying more than it knows.
set(_moq_provider_facts_origin "")
set(_moq_provider_missing "")
if(NOT DEFINED WITH_OPENSSL)
    list(APPEND _moq_provider_missing WITH_OPENSSL)
endif()
if(NOT DEFINED WITH_MBEDTLS)
    list(APPEND _moq_provider_missing WITH_MBEDTLS)
endif()

if(_moq_provider_missing)
    string(REPLACE ";" " and " _moq_missing_text "${_moq_provider_missing}")
    message(FATAL_ERROR
        "LIBMOQ_PICOQUIC_PROVIDER_FACTS_UNKNOWN: ${_moq_missing_text} "
        "not available in this configuration, so the upstream provider "
        "capability is UNKNOWN -- not OFF. This happens with an installed or "
        "already-defined picoquic target, whose build settings are not in "
        "caller scope. LibMoQ records the provider its artifact was built "
        "against, so it cannot invent one. Re-run with both facts stated "
        "explicitly, matching the picoquic actually in use, e.g. "
        "-DWITH_OPENSSL=ON -DWITH_MBEDTLS=OFF. "
        "missing=${_moq_missing_text}")
endif()

# Normalize only RECOGNIZED boolean spellings. An unknown spelling is rejected
# rather than falling through to a default, so a typo cannot become truthy.
foreach(_moq_pair "WITH_OPENSSL;_MOQ_PARENT_WITH_OPENSSL"
                  "WITH_MBEDTLS;_MOQ_PARENT_WITH_MBEDTLS")
    list(GET _moq_pair 0 _moq_in)
    list(GET _moq_pair 1 _moq_out)
    set(_moq_raw "${${_moq_in}}")
    string(TOUPPER "${_moq_raw}" _moq_up)
    if(_moq_up STREQUAL "ON" OR _moq_up STREQUAL "TRUE" OR
       _moq_up STREQUAL "YES" OR _moq_up STREQUAL "Y" OR _moq_up STREQUAL "1")
        set(${_moq_out} ON)
    elseif(_moq_up STREQUAL "OFF" OR _moq_up STREQUAL "FALSE" OR
           _moq_up STREQUAL "NO" OR _moq_up STREQUAL "N" OR
           _moq_up STREQUAL "0" OR _moq_up STREQUAL "")
        set(${_moq_out} OFF)
    else()
        message(FATAL_ERROR
            "LIBMOQ_PICOQUIC_PROVIDER_FACTS_UNKNOWN: ${_moq_in} is "
            "'${_moq_raw}', which is not a recognized CMake boolean. It must "
            "not be guessed in either direction. field=${_moq_in}")
    endif()
endforeach()
set(_moq_provider_facts_origin "explicit")

# The ONE publication step: the private locals are transient inputs here and
# are read nowhere else. Every consumer reads the published record below.
set(MOQ_PARENT_WITH_OPENSSL "${_MOQ_PARENT_WITH_OPENSSL}" CACHE INTERNAL
    "normalized parent picoquic WITH_OPENSSL")
set(MOQ_PARENT_WITH_MBEDTLS "${_MOQ_PARENT_WITH_MBEDTLS}" CACHE INTERNAL
    "normalized parent picoquic WITH_MBEDTLS")
# The record's own provenance, so an explicitly stated fact stays
# distinguishable from anything a future revision might infer.
set(MOQ_PARENT_PROVIDER_FACTS_ORIGIN "${_moq_provider_facts_origin}"
    CACHE INTERNAL "how the provider capability facts were established")


# The resolved picotls artifacts, canonicalized to absolute existing files.
# A target name or a nonexistent path is NOT an artifact identity: it would
# compare equal to itself while proving nothing.
set(MOQ_PARENT_PTLS_CORE "" CACHE INTERNAL "canonical picotls core artifact")
set(MOQ_PARENT_PTLS_MINICRYPTO "" CACHE INTERNAL
    "canonical picotls minicrypto artifact")
foreach(_moq_ptls_pair "PTLS_CORE_LIBRARY;MOQ_PARENT_PTLS_CORE"
                       "PTLS_MINICRYPTO_LIBRARY;MOQ_PARENT_PTLS_MINICRYPTO")
    list(GET _moq_ptls_pair 0 _moq_src)
    list(GET _moq_ptls_pair 1 _moq_dst)
    set(_moq_val "${${_moq_src}}")
    if(_moq_val AND IS_ABSOLUTE "${_moq_val}" AND EXISTS "${_moq_val}")
        get_filename_component(_moq_real "${_moq_val}" REALPATH)
        set(${_moq_dst} "${_moq_real}" CACHE INTERNAL
            "canonical picotls artifact")
    elseif(_moq_val AND TARGET "${_moq_val}")
        # Resolve the target's actual artifact rather than serializing its name.
        set(${_moq_dst} "$<TARGET_FILE:${_moq_val}>" CACHE INTERNAL
            "canonical picotls artifact")
    endif()
endforeach()

# Only NOW is the backend selected. AUTO reads the NORMALIZED record, never the
# raw variables: reading them directly converts an UNAVAILABLE fact to false
# before the record can reject it, so a single missing fact would surface as a
# capability failure instead of an unknown-fact one.
set(_moq_pq_verify_backend "${MOQ_PICOQUIC_VERIFY_BACKEND}")
string(TOLOWER "${_moq_pq_verify_backend}" _moq_pq_verify_backend)
if(_moq_pq_verify_backend STREQUAL "auto")
    if(NOT MOQ_PARENT_WITH_OPENSSL)
        if(MOQ_PARENT_WITH_MBEDTLS)
            set(_moq_pq_verify_backend "mbedtls")
        else()
            message(FATAL_ERROR
                "LIBMOQ_PICOQUIC_PROVIDER_CAPABILITY_MISMATCH: "
                "moq-adapter-picoquic cannot install a production TLS verifier "
                "when picoquic is built with WITH_OPENSSL=OFF and "
                "WITH_MBEDTLS=OFF. Enable mbedTLS or set "
                "MOQ_PICOQUIC_VERIFY_BACKEND to a supported backend. "
                "selected=auto WITH_OPENSSL=${MOQ_PARENT_WITH_OPENSSL} "
                "WITH_MBEDTLS=${MOQ_PARENT_WITH_MBEDTLS}")
        endif()
    else()
        set(_moq_pq_verify_backend "openssl")
    endif()
endif()

# The selected verifier must be one the recorded capability actually offers.
# The facts are KNOWN here -- absence was rejected during ingestion -- so
# this is a capability mismatch and carries its own token.
if(_moq_pq_verify_backend STREQUAL "openssl" AND
   NOT MOQ_PARENT_WITH_OPENSSL)
    message(FATAL_ERROR
        "LIBMOQ_PICOQUIC_PROVIDER_CAPABILITY_MISMATCH: the OpenSSL verifier is "
        "selected while the recorded upstream capability says OpenSSL is not "
        "built. selected=openssl WITH_OPENSSL=${MOQ_PARENT_WITH_OPENSSL}")
endif()
if(_moq_pq_verify_backend STREQUAL "mbedtls" AND
   NOT MOQ_PARENT_WITH_MBEDTLS)
    message(FATAL_ERROR
        "LIBMOQ_PICOQUIC_PROVIDER_CAPABILITY_MISMATCH: the mbedTLS verifier is "
        "selected while the recorded upstream capability says mbedTLS is not "
        "built. selected=mbedtls WITH_MBEDTLS=${MOQ_PARENT_WITH_MBEDTLS}")
endif()

set(_adapter_tls_link "")
set(_adapter_tls_install_link "")
set(MOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL OFF CACHE INTERNAL
    "adapter-picoquic install-tree dependency on OpenSSL")
set(MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND "${_moq_pq_verify_backend}" CACHE INTERNAL
    "adapter-picoquic configured verifier backend")



if(_moq_pq_verify_backend STREQUAL "openssl")
    # The OpenSSL verifier branch needs picotls' OpenSSL header
    # (<picotls/openssl.h>). picotls is not installed by picoquic, so locate
    # its source include dir from the hints FindPicoquic resolves. PRIVATE:
    # not part of the public interface (the public header forward-declares
    # picoquic_quic_t only). A deliberate, actionable error if absent -- never
    # a cryptic missing-header failure mid-build.
    find_path(MOQ_PTLS_OPENSSL_INCLUDE_DIR
        NAMES picotls/openssl.h
        HINTS
            ${PTLS_INCLUDE_DIR}
            ${PTLS_PREFIX}/include
            ${MOQ_PICOTLS_PREFIX}/include
            ${MOQ_PICOTLS_PREFIX}/../include
        DOC "Directory containing <picotls/openssl.h> for the cert verifier")
    if(NOT MOQ_PTLS_OPENSSL_INCLUDE_DIR)
        message(FATAL_ERROR
            "moq-adapter-picoquic selected the OpenSSL certificate verifier, "
            "but picotls headers (<picotls/openssl.h>) were not found. "
            "Pass -DMOQ_PICOTLS_PREFIX=<picotls-build-or-prefix>, build "
            "against source picoquic so FindPicoquic sets PTLS_INCLUDE_DIR, "
            "or use -DMOQ_PICOQUIC_VERIFY_BACKEND=mbedtls with an mbedTLS "
            "picoquic build.")
    endif()
    find_package(OpenSSL REQUIRED)
    target_include_directories(moq-adapter-picoquic
        PRIVATE ${MOQ_PTLS_OPENSSL_INCLUDE_DIR})
    target_compile_definitions(moq-adapter-picoquic
        PRIVATE MOQ_PICOQUIC_VERIFY_OPENSSL)
    set(_adapter_tls_link OpenSSL::SSL OpenSSL::Crypto)
    set(_adapter_tls_install_link ";OpenSSL::SSL;OpenSSL::Crypto")
    set(MOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL ON CACHE INTERNAL
        "adapter-picoquic install-tree dependency on OpenSSL")
elseif(_moq_pq_verify_backend STREQUAL "mbedtls")
    find_path(MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR
        NAMES ptls_mbedtls.h
        HINTS
            ${MOQ_PICOQUIC_SOURCE_DIR}/picoquic_mbedtls
            ${PTLS_INCLUDE_DIR}
            ${PTLS_PREFIX}/include
            ${MOQ_PICOTLS_PREFIX}/include
            ${MOQ_PICOTLS_PREFIX}/../include
        DOC "Directory containing picoquic's ptls_mbedtls.h")
    if(NOT MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR)
        message(FATAL_ERROR
            "moq-adapter-picoquic selected the mbedTLS certificate verifier, "
            "but picoquic's ptls_mbedtls.h was not found. Build against a "
            "picoquic source tree configured with -DWITH_OPENSSL=OFF "
            "-DWITH_MBEDTLS=ON, or set MOQ_PICOQUIC_VERIFY_BACKEND=openssl.")
    endif()
    if(NOT MOQ_PARENT_WITH_MBEDTLS)
        message(FATAL_ERROR
            "MOQ_PICOQUIC_VERIFY_BACKEND=mbedtls requires picoquic built with "
            "-DWITH_MBEDTLS=ON.")
    endif()
    # picoquic's ptls_mbedtls.h includes <picotls.h>, so the verifier source
    # needs picotls' own headers as well as picoquic_mbedtls'. The OpenSSL arm
    # resolves its equivalent explicitly (MOQ_PTLS_OPENSSL_INCLUDE_DIR); this
    # arm previously relied on picotls arriving transitively, which does not
    # happen in a prebuilt-picotls configuration -- FindPicoquic normalizes
    # PTLS_INCLUDE_DIR precisely because picoquic does not reliably export or
    # install picotls headers. PRIVATE, so nothing about picotls enters this
    # adapter's public interface, install target or pkg-config metadata.
    if(NOT PTLS_INCLUDE_DIR OR NOT EXISTS "${PTLS_INCLUDE_DIR}/picotls.h")
        message(FATAL_ERROR
            "moq-adapter-picoquic selected the mbedTLS certificate verifier, "
            "but picotls' own headers (<picotls.h>) were not located. "
            "picoquic's ptls_mbedtls.h includes them. Pass "
            "-DMOQ_PICOTLS_PREFIX=<picotls-build-or-prefix> (the tree "
            "containing include/picotls.h), or build against source picoquic "
            "so FindPicoquic sets PTLS_INCLUDE_DIR.")
    endif()
    target_include_directories(moq-adapter-picoquic
        SYSTEM PRIVATE
            ${MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR}
            ${PTLS_INCLUDE_DIR})
    target_compile_definitions(moq-adapter-picoquic
        PRIVATE MOQ_PICOQUIC_VERIFY_MBEDTLS)
else()
    message(FATAL_ERROR
        "Unsupported MOQ_PICOQUIC_VERIFY_BACKEND='${MOQ_PICOQUIC_VERIFY_BACKEND}'. "
        "Expected auto, openssl, or mbedtls.")
endif()

# Resolve picoquic-log target name before linking.
set(_PQ_LOG "")
if(TARGET picoquic::picoquic-log)
    set(_PQ_LOG "picoquic::picoquic-log")
elseif(TARGET picoquic-log)
    set(_PQ_LOG "picoquic-log")
endif()

# picoquic-core and picoquic-log are mutually dependent archives; the adapter
# carries their whole closure (cmake/MoqPicoquicLink.cmake).
moq_picoquic_build_link_closure(_PQ_CLOSURE_BUILD ${_PQ} ${_PQ_LOG})
target_link_libraries(moq-adapter-picoquic
    PUBLIC moq::core ${_adapter_tls_link}
    PUBLIC ${_PQ_CLOSURE_BUILD})

set_property(TARGET moq-adapter-picoquic APPEND PROPERTY
    INTERFACE_LINK_LIBRARIES "$<INSTALL_INTERFACE:${_PQ}>")

set_target_properties(moq-adapter-picoquic PROPERTIES
    C_STANDARD 11
    C_STANDARD_REQUIRED ON
)

# Owned-code warning baseline (PRIVATE; never reaches picoquic/picotls
# deps or consumers). -Werror gated by MOQ_WARNINGS_AS_ERRORS.
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-adapter-picoquic PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# -- Install ------------------------------------------------------------

install(TARGETS moq-adapter-picoquic
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
    PATTERN "picoquic_threaded.h" EXCLUDE)

install(FILES moq_picoquic.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# -- Adapter targets files -----------------------------------------------
# Source-tree picoquic targets can't pass CMake's export-set validation,
# so we generate the adapter import module manually: one for build-tree
# consumption (absolute paths) and one for install-tree (relocatable).

# Build-tree targets (absolute paths — valid on this machine)
moq_picoquic_link_closure(_PQ_CLOSURE ${_PQ} ${_PQ_LOG})
set(_adapter_pq_link "moq::core;${_adapter_tls_link};${_PQ_CLOSURE}")
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqAdapterTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-picoquic)\n\
  add_library(moq::adapter-picoquic $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-picoquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-picoquic PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-picoquic>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include;${CMAKE_CURRENT_SOURCE_DIR}\"\n\
    INTERFACE_LINK_LIBRARIES \"${_adapter_pq_link}\")\n\
endif()\n")

# Install-tree targets (relocatable via prefix detection).
# Always use namespaced names since install-tree consumers will have
# installed picoquic with picoquic::picoquic-core / picoquic::picoquic-log.
set(_adapter_lib_name "$<TARGET_FILE_NAME:moq-adapter-picoquic>")
set(_adapter_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-picoquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
set(_adapter_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_adapter_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
set(_adapter_pq_target "picoquic::picoquic-core")
set(_adapter_pq_log "")
set(_adapter_pq_grouped FALSE)
if(_PQ_LOG)
    set(_adapter_pq_log ";picoquic::picoquic-log")
    set(_adapter_pq_grouped TRUE)   # core/log form a static-archive cycle
endif()
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqAdapterTargets.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterTargets-install.cmake.in
    @ONLY)
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterTargets-install.cmake
    INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterTargets-install.cmake.in)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqAdapterTargets.cmake)

# -- Threaded helper (optional) ------------------------------------------

if(MOQ_BUILD_PQ_THREADED)
    add_library(moq-adapter-picoquic-threaded moq_picoquic_threaded.c)
    add_library(moq::adapter-picoquic-threaded ALIAS moq-adapter-picoquic-threaded)

    target_include_directories(moq-adapter-picoquic-threaded
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    # The threaded drain (moq_endpoint_drain) computes its "stream backlog
    # empty" predicate locally in moq_pq_stream_backlog.h, which intentionally
    # reads picoquic's private stream state from picoquic_internal.h. picoquic
    # does not install that header, so this adapter requires source-tree
    # picoquic (-DMOQ_PICOQUIC_SOURCE_DIR=...); the installed CONFIG-package
    # mode cannot satisfy it.
    if(NOT MOQ_PICOQUIC_SOURCE_DIR)
        message(FATAL_ERROR
            "MOQ_BUILD_PQ_THREADED requires source-tree picoquic: the threaded "
            "drain predicate reads picoquic private stream state "
            "(picoquic_internal.h), which the installed picoquic package does "
            "not provide. Reconfigure with -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic.")
    endif()
    target_include_directories(moq-adapter-picoquic-threaded PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")

    find_package(Threads REQUIRED)
    target_link_libraries(moq-adapter-picoquic-threaded
        PUBLIC moq::adapter-picoquic Threads::Threads)

    set_target_properties(moq-adapter-picoquic-threaded PROPERTIES
        C_STANDARD 11
        C_STANDARD_REQUIRED ON)

    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-adapter-picoquic-threaded PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    install(TARGETS moq-adapter-picoquic-threaded
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

    install(FILES include/moq/picoquic_threaded.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)

    # Build-tree imported target
    file(GENERATE
        OUTPUT "${PROJECT_BINARY_DIR}/libmoqAdapterThreadedTargets.cmake"
        CONTENT "\
if(NOT TARGET moq::adapter-picoquic-threaded)\n\
  add_library(moq::adapter-picoquic-threaded $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-picoquic-threaded,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-picoquic-threaded PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-picoquic-threaded>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-picoquic$<SEMICOLON>Threads::Threads\")\n\
endif()\n")

    # Install-tree imported target
    set(_threaded_lib_name "$<TARGET_FILE_NAME:moq-adapter-picoquic-threaded>")
    file(GENERATE
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterThreadedTargets-install.cmake
        CONTENT "\
get_filename_component(_THREADED_PREFIX \"\${CMAKE_CURRENT_LIST_FILE}\" PATH)\n\
get_filename_component(_THREADED_PREFIX \"\${_THREADED_PREFIX}\" PATH)\n\
get_filename_component(_THREADED_PREFIX \"\${_THREADED_PREFIX}\" PATH)\n\
get_filename_component(_THREADED_PREFIX \"\${_THREADED_PREFIX}\" PATH)\n\
\n\
if(NOT TARGET moq::adapter-picoquic-threaded)\n\
  add_library(moq::adapter-picoquic-threaded $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-picoquic-threaded,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-picoquic-threaded PROPERTIES\n\
    IMPORTED_LOCATION \"\${_THREADED_PREFIX}/${_adapter_libdir}/${_threaded_lib_name}\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"\${_THREADED_PREFIX}/${_adapter_incdir}\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-picoquic$<SEMICOLON>Threads::Threads\")\n\
endif()\n\
\n\
unset(_THREADED_PREFIX)\n")

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterThreadedTargets-install.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
        RENAME libmoqAdapterThreadedTargets.cmake)
endif()

# -- Tests ---------------------------------------------------------------

add_library(test_adapter_compile OBJECT test_compile.c)
target_link_libraries(test_adapter_compile PRIVATE moq::adapter-picoquic)

if(MOQ_BUILD_TESTS)
    # Test-only parent provenance manifest. CMakeCache.txt is NOT a total
    # authority -- PTLS_* may be ordinary CMake-scope values that never reach
    # the cache -- so the parent writes what it actually selected and resolved,
    # and the drivers compare against that.
    #
    # It belongs to the ADAPTER's test scope, not to the raw threaded facade:
    # every consumer driver that checks the provider contract needs it,
    # including the pico-WT consumers in a build with the threaded facade OFF.
    set(_MOQ_PROVENANCE_MANIFEST "${PROJECT_BINARY_DIR}/moq-parent-provenance.txt")
    # Generated exactly once, here, and published for the other consumer drivers:
    # two file(GENERATE) commands writing one path is a configure error.
    set(MOQ_PARENT_PROVENANCE_MANIFEST "${_MOQ_PROVENANCE_MANIFEST}" CACHE INTERNAL
        "test-only parent provenance manifest")
    # The CONTENT is a multi-line string literal: its lines are manifest
    # records, not CMake statements, so they stay at column zero. Indenting
    # them would prefix every key with whitespace and break the drivers' parse.
    file(GENERATE OUTPUT "${_MOQ_PROVENANCE_MANIFEST}" CONTENT
"VERIFIER=${MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND}
WITH_OPENSSL=${MOQ_PARENT_WITH_OPENSSL}
WITH_MBEDTLS=${MOQ_PARENT_WITH_MBEDTLS}
PTLS_CORE_LIBRARY=${MOQ_PARENT_PTLS_CORE}
PTLS_MINICRYPTO_LIBRARY=${MOQ_PARENT_PTLS_MINICRYPTO}
")

    # Permanent gate for the consumer drivers' provenance contract, run under
    # BOTH /bin/bash (3.2 on macOS, where an empty array under `set -u` is an
    # unbound variable) and the PATH bash. It sits with the manifest it
    # validates and in the same scope: every consumer driver that checks the
    # provider contract needs the gate, including the pico-WT consumers in a
    # build with the threaded facade OFF.
    add_test(NAME consumer_driver_selftest
        COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/tests/selftest_consumer_drivers.sh")

    # The shared adapter's dynamic exports are exactly its installed public API
    # plus the private SPI the threaded adapter links (picoquic_spi.h).
    if(BUILD_SHARED_LIBS)
        add_test(NAME adapter_picoquic_symbol_policy
            COMMAND ${CMAKE_COMMAND}
                -DDYLIB=$<TARGET_FILE:moq-adapter-picoquic>
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_adapter_symbols.cmake)
    endif()

    # One expected-provider macro for every adapter test that has a
    # provider-dependent expectation, taken from the CONFIGURED backend. A
    # runtime capability answer must never select which assertions run: those
    # answers are themselves under test.
    if(MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND STREQUAL "openssl")
        set(_MOQ_PQ_TEST_PROVIDER_DEF MOQ_TEST_VERIFY_BACKEND_OPENSSL=1)
    elseif(MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND STREQUAL "mbedtls")
        set(_MOQ_PQ_TEST_PROVIDER_DEF MOQ_TEST_VERIFY_BACKEND_MBEDTLS=1)
    else()
        message(FATAL_ERROR
            "moq-adapter-picoquic tests: the configured certificate-verifier "
            "backend is '${MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND}', which is "
            "neither 'openssl' nor 'mbedtls'. Provider-dependent rows have no "
            "defined expectation for that value.")
    endif()

    # Compile and run the real verifier source against both public picoquic API
    # generations. The legacy row intentionally has no _ex declaration; the
    # modern row makes setter refusal and ownership observable.
    foreach(_verify_api IN ITEMS legacy modern)
        add_executable(test_picoquic_verify_api_${_verify_api}
            tests/test_verify_api_compat.c
            moq_picoquic_verify.c)
        target_include_directories(test_picoquic_verify_api_${_verify_api}
            BEFORE PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/support/verify_api
                ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${PROJECT_SOURCE_DIR}/core/include)
        target_compile_definitions(test_picoquic_verify_api_${_verify_api}
            PRIVATE MOQ_PICOQUIC_VERIFY_MBEDTLS)
        if(_verify_api STREQUAL "modern")
            target_compile_definitions(test_picoquic_verify_api_${_verify_api}
                PRIVATE MOQ_TEST_PICOQUIC_MODERN_API)
        endif()
        set_target_properties(test_picoquic_verify_api_${_verify_api}
            PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_picoquic_verify_api_${_verify_api}
                PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME picoquic_verify_api_${_verify_api}
                 COMMAND test_picoquic_verify_api_${_verify_api})
    endforeach()

    # The delegated certificate-trust wrapper's own contract. The real verifier
    # source is compiled against the picoquic setter/context recorder in
    # tests/support/verify_callback while picotls and the selected crypto
    # provider stay REAL, so the installed verifier and disposer are directly
    # observable and preservation is proven by identity. The provider is the
    # CONFIGURED one; a runtime capability answer never selects a target.
    # The wrapper test inspects OpenSSL key lifetime, so it must compile
    # against the headers matching the library it links. FindOpenSSL can
    # resolve OPENSSL_INCLUDE_DIR to an unrelated framework's older headers
    # while OPENSSL_SSL_LIBRARY points at a different installation; derive the
    # include directory from the library actually linked.
    set(_MOQ_VCB_OPENSSL_INCLUDE "")
    if(OPENSSL_SSL_LIBRARY)
        get_filename_component(_vcb_ossl_libdir "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
        get_filename_component(_vcb_ossl_root "${_vcb_ossl_libdir}" DIRECTORY)
        if(EXISTS "${_vcb_ossl_root}/include/openssl/crypto.h")
            set(_MOQ_VCB_OPENSSL_INCLUDE "${_vcb_ossl_root}/include")
        endif()
    endif()

    if(MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND STREQUAL "openssl")
        foreach(_verify_cb_api IN ITEMS legacy modern)
            add_executable(test_picoquic_verify_callback_${_verify_cb_api}
                tests/test_verify_callback.c
                moq_picoquic_verify.c)
            target_include_directories(
                test_picoquic_verify_callback_${_verify_cb_api}
                BEFORE PRIVATE
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/support/verify_callback
                    ${CMAKE_CURRENT_SOURCE_DIR}/include
                    ${PROJECT_SOURCE_DIR}/core/include
                    ${_MOQ_VCB_OPENSSL_INCLUDE}
                    ${MOQ_PTLS_OPENSSL_INCLUDE_DIR})
            # The candidate verifier's own allocation and disposal are
            # counted directly by renaming the two allocator calls the verifier
            # source uses. Test targets only; no shipped object sees these.
            target_compile_definitions(
                test_picoquic_verify_callback_${_verify_cb_api} PRIVATE
                    MOQ_PICOQUIC_VERIFY_OPENSSL
                    MOQ_PICOQUIC_VERIFY_TESTING=1
                    calloc=moq_test_verify_calloc
                    free=moq_test_verify_free
                    MOQ_TEST_CA_PEM="${PROJECT_SOURCE_DIR}/adapters/msquic/tests/test_only_loopback_cert.pem")
            if(_verify_cb_api STREQUAL "modern")
                target_compile_definitions(
                    test_picoquic_verify_callback_${_verify_cb_api} PRIVATE
                        MOQ_TEST_PICOQUIC_MODERN_API
                        picoquic_set_verify_certificate_callback_ex=moq_test_set_verify_certificate_callback_ex)
            else()
                target_compile_definitions(
                    test_picoquic_verify_callback_${_verify_cb_api} PRIVATE
                        picoquic_set_verify_certificate_callback=moq_test_set_verify_certificate_callback)
            endif()
            target_link_libraries(test_picoquic_verify_callback_${_verify_cb_api}
                PRIVATE ${_adapter_tls_link} ${_PQ})
            set_target_properties(test_picoquic_verify_callback_${_verify_cb_api}
                PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
            if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
                target_compile_options(
                    test_picoquic_verify_callback_${_verify_cb_api}
                    PRIVATE -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME picoquic_verify_callback_${_verify_cb_api}
                     COMMAND test_picoquic_verify_callback_${_verify_cb_api})
        endforeach()
    elseif(MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND STREQUAL "mbedtls")
        add_executable(test_picoquic_verify_callback_mbedtls
            tests/test_verify_callback.c
            moq_picoquic_verify.c)
        target_include_directories(test_picoquic_verify_callback_mbedtls
            BEFORE PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/support/verify_callback
                ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${PROJECT_SOURCE_DIR}/core/include)
        target_include_directories(test_picoquic_verify_callback_mbedtls
            SYSTEM PRIVATE
                ${MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR} ${PTLS_INCLUDE_DIR})
        # The same allocation recorder as the OpenSSL targets, so the
        # unsupported row can OBSERVE that no helper-owned allocation happened
        # rather than infer it from the setter count.
        target_compile_definitions(test_picoquic_verify_callback_mbedtls PRIVATE
            MOQ_PICOQUIC_VERIFY_MBEDTLS
            MOQ_PICOQUIC_VERIFY_TESTING=1
            calloc=moq_test_verify_calloc
            free=moq_test_verify_free
            MOQ_TEST_CA_PEM="${PROJECT_SOURCE_DIR}/adapters/msquic/tests/test_only_loopback_cert.pem"
            picoquic_set_verify_certificate_callback=moq_test_set_verify_certificate_callback)
        target_link_libraries(test_picoquic_verify_callback_mbedtls
            PRIVATE ${_adapter_tls_link} ${_PQ})
        set_target_properties(test_picoquic_verify_callback_mbedtls
            PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_picoquic_verify_callback_mbedtls
                PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME picoquic_verify_callback_mbedtls
                 COMMAND test_picoquic_verify_callback_mbedtls)
    endif()

    # Backpressure test provides its own picoquic function stubs so it
    # can exercise moq_pq_callback/moq_pq_service without a real QUIC
    # stack. Links the adapter source directly with picoquic headers
    # for type definitions but does not link the picoquic library.
    add_executable(test_adapter_backpressure
        test_backpressure.c moq_picoquic.c picoquic_endpoint.c
        ../common/moq_pq_send_queue.c)
    target_include_directories(test_adapter_backpressure PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
        $<TARGET_PROPERTY:${_PQ},INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_adapter_backpressure
        PRIVATE moq::core ${_adapter_tls_link})
    add_test(NAME adapter_backpressure COMMAND test_adapter_backpressure)

    add_executable(test_pq_conformance
        tests/test_pq_conformance.c
        tests/support/fake_pq_pair.c
        moq_picoquic.c
        picoquic_endpoint.c
        ../common/moq_pq_send_queue.c
        ${PROJECT_SOURCE_DIR}/tests/conformance/conformance_scenarios.c)
    target_include_directories(test_pq_conformance PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/tests/conformance
        $<TARGET_PROPERTY:${_PQ},INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_pq_conformance
        PRIVATE moq::core ${_adapter_tls_link})
    add_test(NAME pq_conformance COMMAND test_pq_conformance)

    # Real-transport establishment in the deterministic simulator: genuine
    # QUIC packets and stream ids through the actual adapter + picoquic
    # stack (the seam the fake-pair suite cannot cover). Requires the
    # picoquic-test simulation library (source-tree only).
    if(TARGET picoquic-test AND MOQ_PICOQUIC_SOURCE_DIR)
        add_executable(test_pq_sim_establish tests/test_pq_sim_establish.c)
        target_include_directories(test_pq_sim_establish PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(test_pq_sim_establish PRIVATE
            moq::adapter-picoquic
            picoquic-test
            ${_adapter_tls_link})
        target_compile_definitions(test_pq_sim_establish PRIVATE
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_sim_establish PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>
                -Wno-unused-parameter)
        endif()
        add_test(NAME pq_sim_establish COMMAND test_pq_sim_establish)
        set_tests_properties(pq_sim_establish PROPERTIES TIMEOUT 30)

        # Per-stream receive backpressure over real picoquic: proves the
        # adapter's application-controlled flow control ARRESTS the peer's
        # sender at the bounded window when the app stalls (white-box on the
        # server's picoquic stream state), then resumes on drain. Needs the
        # picoquic private header for stream introspection, exactly as the
        # upstream flow_control_test.c does.
        add_executable(test_pq_sim_flow_control tests/test_pq_sim_flow_control.c)
        target_include_directories(test_pq_sim_flow_control PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_sim_flow_control PRIVATE
            moq::adapter-picoquic
            picoquic-test
            ${_adapter_tls_link})
        target_compile_definitions(test_pq_sim_flow_control PRIVATE
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_sim_flow_control PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>
                -Wno-unused-parameter)
        endif()
        add_test(NAME pq_sim_flow_control COMMAND test_pq_sim_flow_control)
        set_tests_properties(pq_sim_flow_control PROPERTIES TIMEOUT 60)
    endif()
endif()

if(MOQ_BUILD_TESTS AND MOQ_BUILD_PQ_THREADED)
    add_executable(test_threaded_compile tests/test_threaded_compile.c)
    target_link_libraries(test_threaded_compile PRIVATE moq::adapter-picoquic-threaded)
    target_compile_definitions(test_threaded_compile PRIVATE
        ${_MOQ_PQ_TEST_PROVIDER_DEF})
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_compile PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    add_test(NAME threaded_compile COMMAND test_threaded_compile)

    # Focused RED row for the raw threaded SERVER terminal ownership defect:
    # the same scenario as threaded_compile's
    # server_terminal_conn_observed_before_prune, on its own, so the ASan
    # abort is attributable without running the scenarios that precede it.
    add_executable(test_threaded_server_terminal_uaf
        tests/test_threaded_server_terminal_uaf.c)
    target_link_libraries(test_threaded_server_terminal_uaf
        PRIVATE moq::adapter-picoquic-threaded)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_server_terminal_uaf PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_server_terminal_uaf PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    # Two rows, registered separately so each terminal shape is attributable:
    #   peer_terminal -- RED: picoquic_callback_close at
    #                    picoquic_state_disconnected, cnx already deleted
    #   app_close     -- CONTROL (passes today): APPLICATION_CLOSE at
    #                    picoquic_state_closing_received, cnx still live
    add_test(NAME threaded_server_terminal_uaf
             COMMAND test_threaded_server_terminal_uaf peer_terminal)
    set_tests_properties(threaded_server_terminal_uaf
        PROPERTIES TIMEOUT 120)
    add_test(NAME threaded_terminal_control_app_close
             COMMAND test_threaded_server_terminal_uaf app_close)
    set_tests_properties(threaded_terminal_control_app_close
        PROPERTIES TIMEOUT 120)

    # Same binary, same row, characterization telemetry ON. The stats path has
    # its own picoquic_cnx_t consumers -- pq_sum_pkts_sent on every after_send
    # turn, and the stop-time aggregation -- which the destructor fix alone
    # does not cover. This row passes only if the THREADED duplicate cnx
    # pointer is synchronized to NULL when the raw adapter releases it.
    add_test(NAME threaded_server_terminal_stats
             COMMAND test_threaded_server_terminal_uaf peer_terminal)
    set_tests_properties(threaded_server_terminal_stats PROPERTIES
        TIMEOUT 120
        ENVIRONMENT "MOQ_PQ_THREADED_STATS=1")

    # Terminal-ownership controls: these pass today and must keep passing, so
    # the eventual fix cannot be obtained by unbinding a live caller-owned cnx
    # too late, or by never unbinding at all.
    add_executable(test_threaded_terminal_controls
        tests/test_threaded_terminal_controls.c)
    target_link_libraries(test_threaded_terminal_controls
        PRIVATE moq::adapter-picoquic-threaded moq::adapter-picoquic)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_terminal_controls PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_terminal_controls PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_terminal_control_client_unbind
             COMMAND test_threaded_terminal_controls client_unbind)
    set_tests_properties(threaded_terminal_control_client_unbind
        PROPERTIES TIMEOUT 120)
    add_test(NAME threaded_terminal_control_local_close
             COMMAND test_threaded_terminal_controls local_close)
    set_tests_properties(threaded_terminal_control_local_close
        PROPERTIES TIMEOUT 120)

    # White-box internals variant (test-only, never installed): the threaded
    # adapter compiled with MOQ_PQ_THREADED_TESTING so the lifecycle test can
    # inject a post-on_lane_pump service-pass failure -- the terminal-conn
    # observation window a black-box peer close cannot reach (peer closes
    # land at packet ingestion, before the ordinary pump). Mirrors the mvfst
    # test-internals precedent.
    add_library(moq-adapter-picoquic-threaded-test-internals STATIC
        moq_picoquic_threaded.c)
    target_include_directories(moq-adapter-picoquic-threaded-test-internals
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
    target_include_directories(moq-adapter-picoquic-threaded-test-internals
        PRIVATE "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    target_link_libraries(moq-adapter-picoquic-threaded-test-internals
        PUBLIC moq::adapter-picoquic Threads::Threads)
    target_compile_definitions(moq-adapter-picoquic-threaded-test-internals
        PRIVATE MOQ_PQ_THREADED_TESTING)
    set_target_properties(moq-adapter-picoquic-threaded-test-internals
        PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-adapter-picoquic-threaded-test-internals
            PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    add_executable(test_threaded_lifecycle tests/test_threaded_lifecycle.c)
    target_link_libraries(test_threaded_lifecycle PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_lifecycle PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    add_test(NAME threaded_lifecycle COMMAND test_threaded_lifecycle)

    # TLS test fixtures are REQUIRED for the threaded TLS gates, not optional.
    # The threaded adapter already requires source-tree picoquic, whose certs/
    # directory ships these files, so a missing fixture is a broken checkout and
    # must fail configuration -- never silently turn a security gate into a
    # passing runtime SKIP.
    set(_pq_tls_cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
    set(_pq_tls_key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    set(_pq_tls_ca   "${MOQ_PICOQUIC_SOURCE_DIR}/certs/test-ca.crt")
    foreach(_pq_tls_fixture "${_pq_tls_cert}" "${_pq_tls_key}" "${_pq_tls_ca}")
        if(NOT EXISTS "${_pq_tls_fixture}")
            message(FATAL_ERROR
                "picoquic threaded TLS tests require the fixture:\n"
                "    ${_pq_tls_fixture}\n"
                "It ships in the picoquic source tree's certs/ directory. Point "
                "-DMOQ_PICOQUIC_SOURCE_DIR at a picoquic checkout that includes "
                "certs/cert.pem, certs/key.pem and certs/test-ca.crt.")
        endif()
    endforeach()

    # Deterministic TLS certificate-policy oracle for the raw threaded client.
    # White-box: it reads the picotls master context off the QUIC context
    # (picoquic_internal.h + picotls.h) and drives the MOQ_PQ_THREADED_TESTING
    # installer seam, so it needs the same PRIVATE picoquic source include the
    # test-internals adapter uses, plus the picotls headers.
    add_executable(test_threaded_tls_policy tests/test_threaded_tls_policy.c)
    target_link_libraries(test_threaded_tls_policy PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    target_include_directories(test_threaded_tls_policy PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic"
        ${PTLS_INCLUDE_DIR})
    target_compile_definitions(test_threaded_tls_policy PRIVATE
        ${_MOQ_PQ_TEST_PROVIDER_DEF}
        MOQ_TEST_CERT_PATH="${_pq_tls_cert}"
        MOQ_TEST_KEY_PATH="${_pq_tls_key}")
    add_test(NAME threaded_tls_policy COMMAND test_threaded_tls_policy)

    # Real-handshake controls (loopback) for the client TLS policy: default
    # rejects the untrusted test server cert (terminal), insecure accepts, custom
    # CA with a matching SNI accepts, and custom CA with a WRONG SNI is rejected
    # on the hostname mismatch (terminal). Needs the test CA that signed the
    # server cert.
    add_executable(test_threaded_tls_handshake tests/test_threaded_tls_handshake.c)
    target_link_libraries(test_threaded_tls_handshake PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    target_compile_definitions(test_threaded_tls_handshake PRIVATE
        ${_MOQ_PQ_TEST_PROVIDER_DEF}
        MOQ_TEST_CERT_PATH="${_pq_tls_cert}"
        MOQ_TEST_KEY_PATH="${_pq_tls_key}"
        MOQ_TEST_CA_PATH="${_pq_tls_ca}")
    add_test(NAME threaded_tls_handshake COMMAND test_threaded_tls_handshake)

    # Keepalive arming oracle (Vojtech idle-publisher report). White-box: drives
    # the MOQ_PQ_THREADED_TESTING keepalive seam to prove keep_alive_interval_ms
    # is applied to the client connection with the correct microsecond interval,
    # and that the appended field is ABI-safe (a short struct_size never reads
    # it). No live handshake, no relay, no wall clock.
    add_executable(test_threaded_keepalive tests/test_threaded_keepalive.c)
    target_link_libraries(test_threaded_keepalive PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    target_include_directories(test_threaded_keepalive PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    add_test(NAME threaded_keepalive COMMAND test_threaded_keepalive)

    set(_threaded_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-threaded")
    set(_threaded_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-threaded-test-build")
    add_test(NAME adapter_threaded_consumer
        COMMAND ${CMAKE_COMMAND} -E env bash -c
            "set -e; \
             '${CMAKE_COMMAND}' -E rm -rf '${_threaded_consumer_bld}'; \
             '${CMAKE_COMMAND}' -S '${_threaded_consumer_src}' \
                 -B '${_threaded_consumer_bld}' \
                 -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                 -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                 -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}'${MOQ_NESTED_SANITIZER_SHELL_ARGS} && \
             '${CMAKE_COMMAND}' --build '${_threaded_consumer_bld}' && \
             '${_threaded_consumer_bld}/moq_adapter_threaded_consumer_test'")
    set_tests_properties(adapter_threaded_consumer PROPERTIES
        FAIL_REGULAR_EXPRESSION "${MOQ_NESTED_BUILD_WARNING_REGEX}")

    # Install-tree consumer: installs picoquic + libmoq to a temp prefix
    # and consumes raw managed + the verifier helper against THAT prefix
    # both via find_package(adapter-picoquic-threaded) and via
    # `pkg-config libmoq`. Source includes only <moq/picoquic_threaded.h>
    # + <moq/picoquic_verify.h>; no source-tree include paths.

    if(MOQ_PICOQUIC_SOURCE_DIR)
        set(_threaded_openssl_root "")
        if(OPENSSL_SSL_LIBRARY)
            get_filename_component(_tom "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
            get_filename_component(_threaded_openssl_root "${_tom}" DIRECTORY)
        endif()
        add_test(NAME adapter_threaded_install_consumer
            COMMAND bash
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_install_consumer_threaded.sh"
                "${CMAKE_COMMAND}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-threaded-install"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-threaded-install/main.c"
                "${PROJECT_BINARY_DIR}"
                "${MOQ_PICOQUIC_SOURCE_DIR}"
                "${MOQ_PICOTLS_PREFIX}"
                "${CMAKE_C_COMPILER}"
                "${CMAKE_C_FLAGS}"
                "${CMAKE_EXE_LINKER_FLAGS}"
                "${CMAKE_BUILD_TYPE}"
                "${_threaded_openssl_root}"
                "${CMAKE_CURRENT_BINARY_DIR}/install-consumer-threaded-work"
                "${CMAKE_INSTALL_LIBDIR}"
                "${MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND}"
                "${MOQ_PARENT_WITH_OPENSSL}"
                "${MOQ_PARENT_WITH_MBEDTLS}"
                "${CMAKE_PREFIX_PATH}"
                "${MOQ_PARENT_PTLS_CORE}"
                "${MOQ_PARENT_PTLS_MINICRYPTO}"
                "${MOQ_PARENT_PROVENANCE_MANIFEST}")
        set_tests_properties(adapter_threaded_install_consumer
            PROPERTIES TIMEOUT 600
            FAIL_REGULAR_EXPRESSION "${MOQ_NESTED_BUILD_WARNING_REGEX}")
        endif()
endif()

if(MOQ_BUILD_TESTS)
    set(_adapter_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer")
    set(_adapter_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-test-build")
    add_test(NAME adapter_consumer
        COMMAND ${CMAKE_COMMAND} -E env bash -c
            "set -e; \
             '${CMAKE_COMMAND}' -E rm -rf '${_adapter_consumer_bld}'; \
             '${CMAKE_COMMAND}' -S '${_adapter_consumer_src}' \
                 -B '${_adapter_consumer_bld}' \
                 -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                 -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                 -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}'${MOQ_NESTED_SANITIZER_SHELL_ARGS} && \
             '${CMAKE_COMMAND}' --build '${_adapter_consumer_bld}' && \
             '${_adapter_consumer_bld}/moq_adapter_consumer_test'")
    set_tests_properties(adapter_consumer PROPERTIES
        FAIL_REGULAR_EXPRESSION "${MOQ_NESTED_BUILD_WARNING_REGEX}")

    # The adapter surfaces with CMP0156 left unset, as any consumer below
    # CMake 3.29 has it, from the generated build tree and from an installed
    # prefix: every surface must link and run with no warning from any step.
    set(_oldpolicy_threaded OFF)
    if(TARGET moq-adapter-picoquic-threaded)
        set(_oldpolicy_threaded ON)
    endif()
    add_test(NAME adapter_oldpolicy_consumer
        COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_oldpolicy_consumer.sh"
            "${CMAKE_COMMAND}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-oldpolicy"
            "${CMAKE_CURRENT_BINARY_DIR}/consumer-oldpolicy-work"
            "${PROJECT_BINARY_DIR}"
            "${MOQ_PICOQUIC_SOURCE_DIR}"
            "${MOQ_PICOTLS_PREFIX}"
            "${CMAKE_C_COMPILER}"
            "${_oldpolicy_threaded}"
            "$<BOOL:$<TARGET_EXISTS:moq-adapter-pico-wt-managed>>"
            "$<BOOL:$<TARGET_EXISTS:moq-adapter-pico-wt>>"
            "${CMAKE_PREFIX_PATH}"
            "${picoquic_DIR}"
            "${CMAKE_INSTALL_LIBDIR}"
            ${MOQ_NESTED_SANITIZER_DRIVER_ARGS})
    set_tests_properties(adapter_oldpolicy_consumer PROPERTIES
        TIMEOUT 300
        FAIL_REGULAR_EXPRESSION "${MOQ_NESTED_BUILD_WARNING_REGEX}")

    # Source-tree picoquic with a PREBUILT picotls compiles and links against
    # MOQ_PICOTLS_PREFIX's headers and archives, and nothing a sibling search
    # could find beside the project or the build tree. PICOQUIC_FETCH_PTLS=ON
    # builds picotls from a source checkout instead, a different authority this
    # prebuilt-prefix gate does not apply to.
    if(MOQ_PICOQUIC_SOURCE_DIR AND MOQ_PICOTLS_PREFIX AND NOT PICOQUIC_FETCH_PTLS)
        add_test(NAME ptls_prefix_authority
            COMMAND ${CMAKE_COMMAND}
                -DCMAKE_EXE=${CMAKE_COMMAND}
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/ptls-prefix-authority
                -DMODULES=${PROJECT_SOURCE_DIR}/cmake
                -DPICOQUIC_SRC=${MOQ_PICOQUIC_SOURCE_DIR}
                -DPICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}
                -P ${PROJECT_SOURCE_DIR}/tests/test_ptls_prefix_authority.cmake)
        set_tests_properties(ptls_prefix_authority PROPERTIES
            TIMEOUT 600
            FAIL_REGULAR_EXPRESSION "${MOQ_NESTED_BUILD_WARNING_REGEX}")
    endif()

    # PICOQUIC_FETCH_PTLS=ON with a local picotls checkout (the iOS xcframework
    # shape): the checkout is the authority. Registered wherever a checkout is
    # known -- the fetch mode's own override, or the source tree beside a
    # prebuilt prefix -- and run with downloads disabled.
    set(_ptls_checkout "")
    if(PICOQUIC_FETCH_PTLS AND FETCHCONTENT_SOURCE_DIR_PICOTLS)
        set(_ptls_checkout "${FETCHCONTENT_SOURCE_DIR_PICOTLS}")
    elseif(MOQ_PICOTLS_PREFIX AND EXISTS "${MOQ_PICOTLS_PREFIX}/../CMakeLists.txt"
           AND EXISTS "${MOQ_PICOTLS_PREFIX}/../include/picotls.h")
        get_filename_component(_ptls_checkout "${MOQ_PICOTLS_PREFIX}/.." ABSOLUTE)
    endif()
    if(MOQ_PICOQUIC_SOURCE_DIR AND _ptls_checkout)
        add_test(NAME ptls_fetch_local_source
            COMMAND ${CMAKE_COMMAND}
                -DCMAKE_EXE=${CMAKE_COMMAND}
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/ptls-fetch-local-source
                -DMODULES=${PROJECT_SOURCE_DIR}/cmake
                -DPICOQUIC_SRC=${MOQ_PICOQUIC_SOURCE_DIR}
                -DPICOTLS_SOURCE=${_ptls_checkout}
                -P ${PROJECT_SOURCE_DIR}/tests/test_ptls_fetch_local_source.cmake)
        set_tests_properties(ptls_fetch_local_source PROPERTIES
            TIMEOUT 600
            FAIL_REGULAR_EXPRESSION "${MOQ_NESTED_BUILD_WARNING_REGEX}")
    endif()
endif()
