# moq-adapter-pico-wt: picoquic WebTransport adapter for libmoq.
#
# Optional. Requires MOQ_BUILD_ADAPTER_PICO_WT=ON and
# MOQ_BUILD_ADAPTER_PICOQUIC=ON (for FindPicoquic + picohttp-core).
#
# Experimental. Consumable via
# find_package(libmoq COMPONENTS adapter-pico-wt) from both the build
# tree (CMAKE_PREFIX_PATH=<build-dir>) and an install prefix.

# picohttp-core (picoquic HTTP/3) is the WebTransport dependency. Its presence
# is the contract of FindPicoquic.cmake, which normalizes it to
# picoquic::picohttp-core or fails early with an actionable message when the
# installed picoquic lacks HTTP support -- so by here it is guaranteed. The
# guard below is a belt-and-suspenders assertion, not the primary diagnostic.
if(NOT TARGET picoquic::picohttp-core)
    message(FATAL_ERROR
        "picoquic::picohttp-core not available. find_package(Picoquic) must run "
        "with MOQ_BUILD_ADAPTER_PICO_WT=ON (it guarantees this target or fails "
        "with a precise message).")
endif()
set(_PICOHTTP picoquic::picohttp-core)

add_library(moq-adapter-pico-wt
    pico_wt_endpoint.c
    pico_wt_adapter.c
)

add_library(moq::adapter-pico-wt ALIAS moq-adapter-pico-wt)

target_include_directories(moq-adapter-pico-wt
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

find_package(OpenSSL REQUIRED)

# picohttp-core is PUBLIC: <moq/pico_wt.h> publicly includes
# pico_webtransport.h / h3zero_common.h, so consumers of the adapter
# target must inherit picohttp-core's include dirs. Wrap in
# BUILD_INTERFACE: the install-tree link interface is hand-written in
# the generated targets file (referencing picoquic::picohttp-core), so
# the build-tree target name must never leak into an export.
target_link_libraries(moq-adapter-pico-wt
    PUBLIC  moq::core $<BUILD_INTERFACE:${_PICOHTTP}>
    PRIVATE OpenSSL::SSL OpenSSL::Crypto
)

# Export public symbols past C_VISIBILITY_PRESET hidden (MOQ_API).
target_compile_definitions(moq-adapter-pico-wt PRIVATE MOQ_BUILDING)

set_target_properties(moq-adapter-pico-wt PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-adapter-pico-wt
)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-adapter-pico-wt PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
endif()

# -- Build-tree package target ------------------------------------------
# Build-tree imported target so `find_package(libmoq COMPONENTS
# adapter-pico-wt)` works against ${PROJECT_BINARY_DIR}, mirroring the
# picoquic adapter. The install-tree counterpart is generated in the
# install section below.
set(_picowt_link "moq::core;OpenSSL::SSL;OpenSSL::Crypto;${_PICOHTTP}")
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqPicoWtAdapterTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-pico-wt)\n\
  add_library(moq::adapter-pico-wt $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-pico-wt,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-pico-wt PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-pico-wt>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"${_picowt_link}\")\n\
endif()\n")

# -- Install ------------------------------------------------------------
# True install-tree consumption. The pico WT adapter is NOT part of the
# libmoqTargets export set (it links source-tree picoquic, which can't
# pass CMake export-set validation), so we install the target plainly
# and hand-generate a relocatable imported-target file, mirroring the
# picoquic adapter.
install(TARGETS moq-adapter-pico-wt
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Public attach-mode header only. Internal headers (pico_wt_adapter.h,
# pico_wt_endpoint.h) live outside include/. pico_wt_managed.h is
# excluded here and installed explicitly in the managed block below, so
# it ships only when MOQ_BUILD_PICO_WT_MANAGED=ON.
install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
    PATTERN "pico_wt_managed.h" EXCLUDE)

# Relocatable install-tree imported target file.
set(_picowt_lib_name "$<TARGET_FILE_NAME:moq-adapter-pico-wt>")
set(_picowt_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-pico-wt,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
set(_picowt_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_picowt_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqPicoWtAdapterTargets.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtAdapterTargets-install.cmake.in
    @ONLY)
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtAdapterTargets-install.cmake
    INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtAdapterTargets-install.cmake.in)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtAdapterTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqPicoWtAdapterTargets.cmake)

# -- pkg-config (for Meson/autotools consumers) -----------------------
# Optional component .pc files (the core libmoq.pc is generated in the
# root CMakeLists with the same relocatable-prefix + baked picotls/OpenSSL
# -L approach). picoquic ships no .pc, so picotls + OpenSSL lib dirs are
# baked here as absolute -L flags (same relocatability caveat as the
# picoquic CMake package). Reused by libmoq-pico-wt-managed.pc below.
set(LIBMOQ_PICOWT_PTLS_LDFLAG "")
# MOQ_PTLS_LIB_DIR is normalized once by FindPicoquic and is reliable in every
# mode (unlike PTLS_OPENSSL_LIBRARY, which picoquic's FindPTLS may set to a bare
# target name that is invisible at this scope).
if(MOQ_PTLS_LIB_DIR)
    set(LIBMOQ_PICOWT_PTLS_LDFLAG "-L${MOQ_PTLS_LIB_DIR}")
endif()
set(LIBMOQ_PICOWT_SSL_LDFLAG "")
if(OPENSSL_CRYPTO_LIBRARY)
    get_filename_component(_picowt_ssldir "${OPENSSL_CRYPTO_LIBRARY}" DIRECTORY)
    set(LIBMOQ_PICOWT_SSL_LDFLAG "-L${_picowt_ssldir}")
endif()
# Relocatable prefix: the .pc lands at <prefix>/<libdir>/pkgconfig, so the
# up-path from ${pcfiledir} back to <prefix> is (libdir depth + 1) levels.
# Computing it from CMAKE_INSTALL_LIBDIR handles lib, lib64, and multiarch
# dirs like lib/x86_64-linux-gnu. Shared by both pico WT .pc files.
string(REPLACE "/" ";" _picowt_libdir_parts "${CMAKE_INSTALL_LIBDIR}")
list(LENGTH _picowt_libdir_parts _picowt_libdir_depth)
math(EXPR _picowt_pc_ascents "${_picowt_libdir_depth} + 1")
set(LIBMOQ_PICOWT_PC_UP "..")
foreach(_picowt_i RANGE 2 ${_picowt_pc_ascents})
    string(APPEND LIBMOQ_PICOWT_PC_UP "/..")
endforeach()
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoq-pico-wt.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoq-pico-wt.pc
    @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoq-pico-wt.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

# -- Tests -----------------------------------------------------------
# Public-header compile/link smoke. Proves <moq/pico_wt.h> stands
# alone (no internal header needed). Builds whenever tests are on —
# does not require the deterministic-sim infrastructure.
if(MOQ_BUILD_TESTS)
    add_executable(test_pico_wt_public_compile
        tests/test_pico_wt_public_compile.c
    )
    # Link ONLY the adapter — proves moq::adapter-pico-wt propagates
    # the picohttp/h3zero include dirs its public header requires.
    target_link_libraries(test_pico_wt_public_compile PRIVATE
        moq::adapter-pico-wt
    )
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pico_wt_public_compile PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME pico_wt_public_compile
             COMMAND test_pico_wt_public_compile)
    set_tests_properties(pico_wt_public_compile PROPERTIES TIMEOUT 30)
endif()

# FindPicoquic WebTransport dependency contract: a picoquic core target without
# picohttp must fail in FindPicoquic with a precise message (not as a generic
# "target not found" later in this file), and a non-namespaced picohttp-core
# must normalize to picoquic::picohttp-core. Hermetic (no picoquic install).
if(MOQ_BUILD_TESTS)
    add_test(NAME pico_wt_findpicoquic_http_contract
        COMMAND bash
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_findpicoquic_http_contract.sh"
            "${CMAKE_SOURCE_DIR}/cmake/FindPicoquic.cmake"
            "${CMAKE_COMMAND}")
    set_tests_properties(pico_wt_findpicoquic_http_contract
        PROPERTIES TIMEOUT 120)
endif()

# Build-tree consumer test: a throwaway downstream project that
# resolves the adapter via find_package(libmoq COMPONENTS
# adapter-pico-wt) against the build tree, includes ONLY
# <moq/pico_wt.h>, and links ONLY moq::adapter-pico-wt. Proves the
# build-tree package is consumable end to end. Needs picoquic source
# (the consumer's FindPicoquic re-resolves it) and
# MOQ_BUILD_ADAPTER_PICO_WT=ON so BUILD_HTTP defines picohttp-core.
if(MOQ_BUILD_TESTS AND MOQ_PICOQUIC_SOURCE_DIR)
    set(_picowt_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer")
    set(_picowt_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-test-build")
    # Forward the parent's compile/link flags and build type so an
    # ASAN parent build produces an ASAN-consistent consumer (it links
    # the parent's instrumented .a files; mismatched instrumentation
    # would fail to link).
    add_test(NAME pico_wt_adapter_consumer
        COMMAND ${CMAKE_COMMAND} -E env bash -c
            "set -e; \
             '${CMAKE_COMMAND}' -E rm -rf '${_picowt_consumer_bld}'; \
             '${CMAKE_COMMAND}' -S '${_picowt_consumer_src}' \
                 -B '${_picowt_consumer_bld}' \
                 -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                 -DMOQ_BUILD_ADAPTER_PICO_WT=ON \
                 -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                 -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}' \
                 -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}' \
                 -DCMAKE_EXE_LINKER_FLAGS='${CMAKE_EXE_LINKER_FLAGS}' \
                 -DCMAKE_BUILD_TYPE='${CMAKE_BUILD_TYPE}' && \
             '${CMAKE_COMMAND}' --build '${_picowt_consumer_bld}' && \
             '${_picowt_consumer_bld}/moq_pico_wt_adapter_consumer_test'")
    set_tests_properties(pico_wt_adapter_consumer PROPERTIES TIMEOUT 180)
endif()

# Install-tree consumer test: installs a BUILD_HTTP picoquic and libmoq
# to a temp prefix, then verifies install-tree consumption by building
# the consumer against THAT prefix using picoquic's installed HTTP
# headers. Guards the install promotion end to end.
if(MOQ_BUILD_TESTS AND MOQ_PICOQUIC_SOURCE_DIR)
    # Derive an OpenSSL root hint for the standalone picoquic install.
    set(_picowt_openssl_root "")
    if(OPENSSL_SSL_LIBRARY)
        get_filename_component(_o "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
        get_filename_component(_picowt_openssl_root "${_o}" DIRECTORY)
    endif()
    add_test(NAME pico_wt_install_consumer
        COMMAND bash
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_install_consumer.sh"
            "${CMAKE_COMMAND}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer"
            "${PROJECT_BINARY_DIR}"
            "${MOQ_PICOQUIC_SOURCE_DIR}"
            "${MOQ_PICOTLS_PREFIX}"
            "${CMAKE_C_COMPILER}"
            "${CMAKE_C_FLAGS}"
            "${CMAKE_EXE_LINKER_FLAGS}"
            "${CMAKE_BUILD_TYPE}"
            "${_picowt_openssl_root}"
            "${CMAKE_CURRENT_BINARY_DIR}/install-consumer-work")
    set_tests_properties(pico_wt_install_consumer PROPERTIES TIMEOUT 600)

    # pkg-config consumer (attach): a plain C program built with only
    # `pkg-config --cflags --libs libmoq-pico-wt` from the install prefix.
    find_program(PKG_CONFIG_EXECUTABLE pkg-config)
    if(PKG_CONFIG_EXECUTABLE)
        add_test(NAME pico_wt_pkgconfig_consumer
            COMMAND bash
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_pkgconfig_consumer.sh"
                "${CMAKE_COMMAND}"
                "libmoq-pico-wt"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer/main.c"
                "${PROJECT_BINARY_DIR}"
                "${MOQ_PICOQUIC_SOURCE_DIR}"
                "${MOQ_PICOTLS_PREFIX}"
                "${CMAKE_C_COMPILER}"
                "${CMAKE_C_FLAGS}"
                "${CMAKE_EXE_LINKER_FLAGS}"
                "${CMAKE_BUILD_TYPE}"
                "${_picowt_openssl_root}"
                "${CMAKE_CURRENT_BINARY_DIR}/pkgconfig-consumer-work"
                "${CMAKE_INSTALL_LIBDIR}")
        set_tests_properties(pico_wt_pkgconfig_consumer PROPERTIES TIMEOUT 600)
    endif()
endif()

# Loopback test requires picoquic-test for deterministic simulation.
# Enabled via MOQ_PICO_WT_BUILD_LOOPBACK=ON (see root CMakeLists).
if(MOQ_BUILD_TESTS AND TARGET picoquic-test AND MOQ_PICOQUIC_SOURCE_DIR)
    add_executable(test_pico_wt_loopback
        tests/test_pico_wt_loopback.c
        tests/pico_wt_harness.c
    )
    target_include_directories(test_pico_wt_loopback PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_link_libraries(test_pico_wt_loopback PRIVATE
        moq::adapter-pico-wt
        picoquic-test
        OpenSSL::SSL OpenSSL::Crypto
    )
    target_compile_definitions(test_pico_wt_loopback PRIVATE
        PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pico_wt_loopback PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME pico_wt_loopback COMMAND test_pico_wt_loopback)
    set_tests_properties(pico_wt_loopback PROPERTIES TIMEOUT 30)

    # Draft-18 over the real WT transport in the deterministic sim:
    # symmetric uni-control establish, request-bidi routing, subgroup
    # data delivery (draft-16 run as the harness control).
    add_executable(test_pico_wt_d18
        tests/test_pico_wt_d18.c
        tests/pico_wt_harness.c
    )
    target_include_directories(test_pico_wt_d18 PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_link_libraries(test_pico_wt_d18 PRIVATE
        moq::adapter-pico-wt
        picoquic-test
        OpenSSL::SSL OpenSSL::Crypto
    )
    target_compile_definitions(test_pico_wt_d18 PRIVATE
        PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pico_wt_d18 PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME pico_wt_d18 COMMAND test_pico_wt_d18)
    set_tests_properties(pico_wt_d18 PROPERTIES TIMEOUT 30)

    # WT protocol-token negotiation (WT-Available-Protocols
    # emit + selected WT-Protocol readback) against a strict in-sim server.
    add_executable(test_pico_wt_protocol_nego
        tests/test_pico_wt_protocol_nego.c
    )
    target_include_directories(test_pico_wt_protocol_nego PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_link_libraries(test_pico_wt_protocol_nego PRIVATE
        moq::adapter-pico-wt
        picoquic-test
        OpenSSL::SSL OpenSSL::Crypto
    )
    target_compile_definitions(test_pico_wt_protocol_nego PRIVATE
        PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pico_wt_protocol_nego PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME pico_wt_protocol_nego COMMAND test_pico_wt_protocol_nego)
    set_tests_properties(pico_wt_protocol_nego PROPERTIES TIMEOUT 30)

    add_executable(test_pico_wt_conformance
        tests/test_pico_wt_conformance.c
        tests/pico_wt_conformance_pair.c
        tests/pico_wt_harness.c
        ${PROJECT_SOURCE_DIR}/tests/conformance/conformance_scenarios.c
    )
    target_include_directories(test_pico_wt_conformance PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PROJECT_SOURCE_DIR}/tests/conformance
    )
    target_link_libraries(test_pico_wt_conformance PRIVATE
        moq::adapter-pico-wt
        picoquic-test
        OpenSSL::SSL OpenSSL::Crypto
    )
    target_compile_definitions(test_pico_wt_conformance PRIVATE
        PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pico_wt_conformance PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME pico_wt_conformance COMMAND test_pico_wt_conformance)
    set_tests_properties(pico_wt_conformance PROPERTIES TIMEOUT 60)
endif()

# -- Managed facade -------------------------------------------------
# Optional helper that owns the QUIC context, network thread, WT
# CONNECT, and MoQ session/adapter behind a small C facade. Installed
# (CMake-only, experimental) when MOQ_BUILD_PICO_WT_MANAGED=ON.
if(MOQ_BUILD_PICO_WT_MANAGED)
    find_package(Threads REQUIRED)
    if(TARGET picoquic::picoquic-core)
        set(_PICO_CORE picoquic::picoquic-core)
    elseif(TARGET picoquic-core)
        set(_PICO_CORE picoquic-core)
    else()
        message(FATAL_ERROR
            "picoquic-core target not found for pico_wt managed")
    endif()

    add_library(moq-adapter-pico-wt-managed pico_wt_managed.c)
    add_library(moq::adapter-pico-wt-managed ALIAS
                moq-adapter-pico-wt-managed)
    target_include_directories(moq-adapter-pico-wt-managed
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    # The managed drain (moq_endpoint_drain) computes its "stream backlog
    # empty" predicate locally in moq_pq_stream_backlog.h, which intentionally
    # reads picoquic's private stream state from picoquic_internal.h. picoquic
    # does not install that header, so this adapter requires source-tree
    # picoquic (-DMOQ_PICOQUIC_SOURCE_DIR=...); the installed CONFIG-package
    # mode cannot satisfy it.
    if(NOT MOQ_PICOQUIC_SOURCE_DIR)
        message(FATAL_ERROR
            "MOQ_BUILD_PICO_WT_MANAGED requires source-tree picoquic: the "
            "managed drain predicate reads picoquic private stream state "
            "(picoquic_internal.h), which the installed picoquic package does "
            "not provide. Reconfigure with -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic.")
    endif()
    target_include_directories(moq-adapter-pico-wt-managed PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    # adapter-picoquic is PUBLIC: the managed client now installs the default
    # certificate verifier (moq_picoquic_set_cert_verifier from
    # <moq/picoquic_verify.h>) itself, so the symbol + its public header must
    # reach standalone managed consumers (the lib is static here, so the
    # dependency cannot be hidden behind the .so boundary).
    target_link_libraries(moq-adapter-pico-wt-managed
        PUBLIC  moq::adapter-pico-wt moq::adapter-picoquic
        PRIVATE $<BUILD_INTERFACE:${_PICOHTTP}> ${_PICO_CORE}
                Threads::Threads OpenSSL::SSL OpenSSL::Crypto)
    # Export public symbols past C_VISIBILITY_PRESET hidden (MOQ_API).
    target_compile_definitions(moq-adapter-pico-wt-managed PRIVATE
        MOQ_BUILDING)
    set_target_properties(moq-adapter-pico-wt-managed PROPERTIES
        C_VISIBILITY_PRESET hidden)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-adapter-pico-wt-managed PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()

    # -- Build-tree package target --------------------------------------
    # Build-tree imported target so `find_package(libmoq COMPONENTS
    # adapter-pico-wt-managed)` works against ${PROJECT_BINARY_DIR},
    # mirroring the pico WT attach + picoquic-threaded patterns. The link
    # interface is moq::adapter-pico-wt + moq::adapter-picoquic (the managed
    # client installs the default cert verifier from the base picoquic adapter)
    # + Threads; picoquic-core / picohttp-core / OpenSSL / moq::core arrive
    # transitively through those. libmoqConfig loads moq::adapter-picoquic
    # alongside the attach adapter before this file so both references resolve.
    file(GENERATE
        OUTPUT "${PROJECT_BINARY_DIR}/libmoqPicoWtManagedTargets.cmake"
        CONTENT "\
if(NOT TARGET moq::adapter-pico-wt-managed)\n\
  add_library(moq::adapter-pico-wt-managed $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-pico-wt-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-pico-wt-managed PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-pico-wt-managed>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-pico-wt$<SEMICOLON>moq::adapter-picoquic$<SEMICOLON>Threads::Threads\")\n\
endif()\n")

    # -- Install ----------------------------------------------------------
    # True install-tree consumption (CMake-only, like the attach adapter
    # and mvfst). Not part of the libmoqTargets export set (it links
    # source-tree picoquic), so install plainly + hand-generate a
    # relocatable imported-target file.
    install(TARGETS moq-adapter-pico-wt-managed
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

    # The managed header is excluded from the unconditional adapter
    # directory install (above) and installed explicitly here, so it ships
    # only when the managed lib is built — mirroring picoquic_threaded.h.
    install(FILES include/moq/pico_wt_managed.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)

    # Relocatable install-tree imported target file.
    set(_picowtm_lib_name "$<TARGET_FILE_NAME:moq-adapter-pico-wt-managed>")
    set(_picowtm_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-pico-wt-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
    set(_picowtm_libdir "${CMAKE_INSTALL_LIBDIR}")
    set(_picowtm_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqPicoWtManagedTargets.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtManagedTargets-install.cmake.in
        @ONLY)
    file(GENERATE
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtManagedTargets-install.cmake
        INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtManagedTargets-install.cmake.in)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqPicoWtManagedTargets-install.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
        RENAME libmoqPicoWtManagedTargets.cmake)

    # pkg-config: Requires libmoq-pico-wt (installed above), which pulls
    # in the full picoquic/picotls/OpenSSL chain, so a consumer needs only
    # `pkg-config --cflags --libs libmoq-pico-wt-managed`.
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoq-pico-wt-managed.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoq-pico-wt-managed.pc
        @ONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoq-pico-wt-managed.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

    # Symbol-policy: covered by the same compile-flag mechanism as the
    # already-installed attach adapter (C_VISIBILITY_PRESET hidden +
    # MOQ_API + MOQ_BUILDING). A dynamic nm export check needs a shared
    # object; a global BUILD_SHARED_LIBS build is blocked by the picoquic
    # core<->log static cycle, and a static-lib nm cannot validate
    # visibility (hidden symbols stay global in the static symbol table).
    # The attach adapter ships installed with the identical flag posture
    # and no nm test, so the managed lib is consistent.

    # Build-tree consumer test: a throwaway downstream project that
    # resolves the managed facade via find_package(libmoq COMPONENTS
    # adapter-pico-wt-managed) against the build tree, includes ONLY
    # <moq/pico_wt_managed.h>, and links ONLY
    # moq::adapter-pico-wt-managed. Compile/link/run smoke — no network,
    # no certs. Mirrors pico_wt_adapter_consumer.
    if(MOQ_BUILD_TESTS AND MOQ_PICOQUIC_SOURCE_DIR)
        set(_picowtm_consumer_src
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-managed")
        set(_picowtm_consumer_bld
            "${CMAKE_CURRENT_BINARY_DIR}/consumer-managed-test-build")
        add_test(NAME pico_wt_managed_consumer
            COMMAND ${CMAKE_COMMAND} -E env bash -c
                "set -e; \
                 '${CMAKE_COMMAND}' -E rm -rf '${_picowtm_consumer_bld}'; \
                 '${CMAKE_COMMAND}' -S '${_picowtm_consumer_src}' \
                     -B '${_picowtm_consumer_bld}' \
                     -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                     -DMOQ_BUILD_ADAPTER_PICO_WT=ON \
                     -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                     -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                     -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}' \
                     -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}' \
                     -DCMAKE_EXE_LINKER_FLAGS='${CMAKE_EXE_LINKER_FLAGS}' \
                     -DCMAKE_BUILD_TYPE='${CMAKE_BUILD_TYPE}' && \
                 '${CMAKE_COMMAND}' --build '${_picowtm_consumer_bld}' && \
                 '${_picowtm_consumer_bld}/moq_pico_wt_managed_consumer_test'")
        set_tests_properties(pico_wt_managed_consumer PROPERTIES TIMEOUT 180)

        # Install-tree managed consumer: installs a BUILD_HTTP picoquic +
        # libmoq to a temp prefix and builds the managed consumer against
        # THAT prefix (installed picoquic, no source dir). Asserts the
        # installed manifest (pico_wt.h + pico_wt_managed.h present;
        # internal headers absent). Guards the managed install promotion.
        set(_picowtm_openssl_root "")
        if(OPENSSL_SSL_LIBRARY)
            get_filename_component(_om "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
            get_filename_component(_picowtm_openssl_root "${_om}" DIRECTORY)
        endif()
        add_test(NAME pico_wt_managed_install_consumer
            COMMAND bash
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_install_consumer_managed.sh"
                "${CMAKE_COMMAND}"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-managed"
                "${PROJECT_BINARY_DIR}"
                "${MOQ_PICOQUIC_SOURCE_DIR}"
                "${MOQ_PICOTLS_PREFIX}"
                "${CMAKE_C_COMPILER}"
                "${CMAKE_C_FLAGS}"
                "${CMAKE_EXE_LINKER_FLAGS}"
                "${CMAKE_BUILD_TYPE}"
                "${_picowtm_openssl_root}"
                "${CMAKE_CURRENT_BINARY_DIR}/install-consumer-managed-work")
        set_tests_properties(pico_wt_managed_install_consumer
            PROPERTIES TIMEOUT 600)

        # pkg-config consumer (managed): a plain C program built with only
        # `pkg-config --cflags --libs libmoq-pico-wt-managed` — exercises
        # the Requires: libmoq-pico-wt chain end to end.
        find_program(PKG_CONFIG_EXECUTABLE pkg-config)
        if(PKG_CONFIG_EXECUTABLE)
            add_test(NAME pico_wt_managed_pkgconfig_consumer
                COMMAND bash
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_pkgconfig_consumer.sh"
                    "${CMAKE_COMMAND}"
                    "libmoq-pico-wt-managed"
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-managed/main.c"
                    "${PROJECT_BINARY_DIR}"
                    "${MOQ_PICOQUIC_SOURCE_DIR}"
                    "${MOQ_PICOTLS_PREFIX}"
                    "${CMAKE_C_COMPILER}"
                    "${CMAKE_C_FLAGS}"
                    "${CMAKE_EXE_LINKER_FLAGS}"
                    "${CMAKE_BUILD_TYPE}"
                    "${_picowtm_openssl_root}"
                    "${CMAKE_CURRENT_BINARY_DIR}/pkgconfig-managed-consumer-work"
                    "${CMAKE_INSTALL_LIBDIR}")
            set_tests_properties(pico_wt_managed_pkgconfig_consumer
                PROPERTIES TIMEOUT 600)
        endif()

        # Verifier cross-link consumer (CMake): a WT managed consumer that
        # ALSO calls moq_picoquic_set_cert_verifier (<moq/picoquic_verify.h>)
        # directly. The managed component now carries adapter-picoquic
        # transitively, so requesting BOTH is redundant — this proves that the
        # explicit cross-link still resolves + links from the build tree.
        # Compile/link/run smoke — no network, no certs.
        set(_picowtmv_consumer_src
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-managed-verify")
        set(_picowtmv_consumer_bld
            "${CMAKE_CURRENT_BINARY_DIR}/consumer-managed-verify-test-build")
        add_test(NAME pico_wt_managed_verify_consumer
            COMMAND ${CMAKE_COMMAND} -E env bash -c
                "set -e; \
                 '${CMAKE_COMMAND}' -E rm -rf '${_picowtmv_consumer_bld}'; \
                 '${CMAKE_COMMAND}' -S '${_picowtmv_consumer_src}' \
                     -B '${_picowtmv_consumer_bld}' \
                     -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                     -DMOQ_BUILD_ADAPTER_PICO_WT=ON \
                     -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                     -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                     -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}' \
                     -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}' \
                     -DCMAKE_EXE_LINKER_FLAGS='${CMAKE_EXE_LINKER_FLAGS}' \
                     -DCMAKE_BUILD_TYPE='${CMAKE_BUILD_TYPE}' && \
                 '${CMAKE_COMMAND}' --build '${_picowtmv_consumer_bld}' && \
                 '${_picowtmv_consumer_bld}/moq_pico_wt_managed_verify_consumer_test'")
        set_tests_properties(pico_wt_managed_verify_consumer
            PROPERTIES TIMEOUT 180)

        # Verifier cross-link consumer (pkg-config): the Meson/autotools
        # path — `pkg-config --cflags --libs libmoq-pico-wt-managed libmoq`
        # (libmoq carries the verifier helper; managed first so it precedes
        # its dependency in the static link line). Temp-prefix install.
        if(PKG_CONFIG_EXECUTABLE)
            add_test(NAME pico_wt_managed_verify_pkgconfig_consumer
                COMMAND bash
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_pkgconfig_consumer.sh"
                    "${CMAKE_COMMAND}"
                    "libmoq-pico-wt-managed libmoq"
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-managed-verify/main.c"
                    "${PROJECT_BINARY_DIR}"
                    "${MOQ_PICOQUIC_SOURCE_DIR}"
                    "${MOQ_PICOTLS_PREFIX}"
                    "${CMAKE_C_COMPILER}"
                    "${CMAKE_C_FLAGS}"
                    "${CMAKE_EXE_LINKER_FLAGS}"
                    "${CMAKE_BUILD_TYPE}"
                    "${_picowtm_openssl_root}"
                    "${CMAKE_CURRENT_BINARY_DIR}/pkgconfig-managed-verify-consumer-work"
                    "${CMAKE_INSTALL_LIBDIR}")
            set_tests_properties(pico_wt_managed_verify_pkgconfig_consumer
                PROPERTIES TIMEOUT 600)
        endif()
    endif()

    # Managed client test: drives the facade against the example
    # server (built under MOQ_BUILD_ADAPTER_PICO_WT). Reuses the
    # example smoke driver. Labeled "network" so UDP-restricted
    # environments can exclude it.
    if(MOQ_BUILD_TESTS AND MOQ_PICOQUIC_SOURCE_DIR AND
       EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem" AND
       EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        add_executable(test_pico_wt_managed_client
            tests/test_pico_wt_managed_client.c)
        target_link_libraries(test_pico_wt_managed_client
            PRIVATE moq::adapter-pico-wt-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pico_wt_managed_client PRIVATE
                -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
        endif()
        add_test(NAME pico_wt_managed_client
            COMMAND bash
                "${PROJECT_SOURCE_DIR}/examples/pico_wt/run_smoke.sh"
                "$<TARGET_FILE:moq_pico_wt_server>"
                "$<TARGET_FILE:test_pico_wt_managed_client>"
                "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem"
                18450)
        set_tests_properties(pico_wt_managed_client PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Managed server <-> managed client, both in-process, each on
        # its own network thread, over real loopback UDP.
        add_executable(test_pico_wt_managed_loopback
            tests/test_pico_wt_managed_loopback.c)
        target_link_libraries(test_pico_wt_managed_loopback
            PRIVATE moq::adapter-pico-wt-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pico_wt_managed_loopback PRIVATE
                -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
        endif()
        add_test(NAME pico_wt_managed_loopback
            COMMAND test_pico_wt_managed_loopback
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_loopback PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Large-object delivery: a 256 KiB object over a WT subgroup stream
        # must arrive in full (exact length + byte pattern). Guards both the
        # large-stream send/receive path and the harness's real-elapsed-time
        # wait accounting for multi-RTT transfers.
        add_test(NAME pico_wt_managed_bigobj
            COMMAND test_pico_wt_managed_loopback --mode bigobj
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_bigobj PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Same large transfer, but with the receiver's advertised inbound
        # per-uni-stream window forced back to ~64 KiB: the 256 KiB object spans
        # many flow-control windows, so it only completes if the publisher
        # resumes sending after each peer MAX_STREAM_DATA. Proves send resumption
        # is independent of teardown timing.
        add_test(NAME pico_wt_managed_bigobj_smallwin
            COMMAND test_pico_wt_managed_loopback --mode bigobj_smallwin
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_bigobj_smallwin PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Graceful drain: publish 256 KiB under a forced ~64 KiB window, poll the
        # drain predicate until the publisher's transport reports flushed, and
        # prove full delivery at that point (backs moq_endpoint_drain).
        add_test(NAME pico_wt_managed_bigobj_drain
            COMMAND test_pico_wt_managed_loopback --mode bigobj_drain
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_bigobj_drain PROPERTIES
            TIMEOUT 90
            LABELS "network;pico_wt;managed")

        # WT protocol-token negotiation through the managed pair: the
        # d16-only server selects from the client's {18, 16} offer, and a
        # no-overlap offer is a deterministic refusal.
        add_test(NAME pico_wt_managed_nego
            COMMAND test_pico_wt_managed_loopback --mode nego
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_nego PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")
        add_test(NAME pico_wt_managed_nego_refuse
            COMMAND test_pico_wt_managed_loopback --mode nego_refuse
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_nego_refuse PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Legacy no-subprotocol fallback: a server that echoes no WT-Protocol
        # (moxygen-style) lets a client offering {18, 16} fall back to
        # draft-16; an EXACT draft-18 offer with no draft-16 stays fatal
        # (never a silent 18 -> 16 downgrade).
        add_test(NAME pico_wt_managed_nego_legacy
            COMMAND test_pico_wt_managed_loopback --mode nego_legacy
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_nego_legacy PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")
        add_test(NAME pico_wt_managed_nego_legacy_18
            COMMAND test_pico_wt_managed_loopback --mode nego_legacy_18
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_nego_legacy_18 PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # A peer that omits the reset_stream_at transport parameter
        # (e.g. moqx) must still establish the WT session with the DEFAULT client
        # (no allow-missing flag) and deliver media.
        add_test(NAME pico_wt_managed_no_reset_stream_at
            COMMAND test_pico_wt_managed_loopback --mode no_reset_stream_at
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_no_reset_stream_at PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # One-connection semantics: a second WT CONNECT is refused.
        add_test(NAME pico_wt_managed_refuse
            COMMAND test_pico_wt_managed_loopback --mode refuse
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_refuse PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Clean-close terminal semantics: client GOAWAY → drain →
        # CLOSE_SESSION makes wait()/wake() terminal (is_closed, not fatal).
        add_test(NAME pico_wt_managed_close
            COMMAND test_pico_wt_managed_loopback --mode close
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_close PROPERTIES
            TIMEOUT 30
            LABELS "network;pico_wt;managed")

        # Edge cases: config validation, NULL safety, connect failure,
        # cert-verification failure, stop/wake/wait lifecycle, and
        # stop-from-on_pump rejection.
        add_executable(test_pico_wt_managed_edges
            tests/test_pico_wt_managed_edges.c)
        # Also links adapter-picoquic for moq_picoquic_set_cert_verifier
        # (<moq/picoquic_verify.h>) — exercises the documented cross-link
        # that pico WT managed consumers use for the verifier helper.
        target_link_libraries(test_pico_wt_managed_edges
            PRIVATE moq::adapter-pico-wt-managed moq::adapter-picoquic)
        # The cert-failure test installs a picotls verify callback.
        if(PTLS_INCLUDE_DIR)
            target_include_directories(test_pico_wt_managed_edges
                PRIVATE ${PTLS_INCLUDE_DIR})
        endif()
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pico_wt_managed_edges PRIVATE
                -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
        endif()
        add_test(NAME pico_wt_managed_edges
            COMMAND test_pico_wt_managed_edges
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key  "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(pico_wt_managed_edges PROPERTIES
            TIMEOUT 60
            LABELS "network;pico_wt;managed")
    endif()
endif()
