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
    moq_picoquic.c picoquic_endpoint.c moq_picoquic_verify.c)
add_library(moq::adapter-picoquic ALIAS moq-adapter-picoquic)

target_include_directories(moq-adapter-picoquic
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# moq_picoquic_verify.c needs picotls' OpenSSL verifier header
# (<picotls/openssl.h>). picotls is not installed by picoquic, so locate
# its source include dir from the hints FindPicoquic resolves (it always
# sets PTLS_PREFIX / MOQ_PICOTLS_PREFIX, in both source- and installed-
# picoquic modes). PRIVATE: not part of the public interface (the public
# header forward-declares picoquic_quic_t only). A deliberate, actionable
# error if absent — never a cryptic missing-header failure mid-build.
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
        "moq-adapter-picoquic requires picotls headers (<picotls/openssl.h>) "
        "to build the certificate-verifier helper (moq_picoquic_verify.c), "
        "but they were not found. picotls is not installed by picoquic; "
        "pass -DMOQ_PICOTLS_PREFIX=<picotls-build-or-prefix> (the tree "
        "containing include/picotls/openssl.h), or build against source "
        "picoquic so FindPicoquic sets PTLS_INCLUDE_DIR.")
endif()
target_include_directories(moq-adapter-picoquic
    PRIVATE ${MOQ_PTLS_OPENSSL_INCLUDE_DIR})

find_package(OpenSSL REQUIRED)

# Resolve picoquic-log target name before linking.
set(_PQ_LOG "")
if(TARGET picoquic::picoquic-log)
    set(_PQ_LOG "picoquic::picoquic-log")
elseif(TARGET picoquic-log)
    set(_PQ_LOG "picoquic-log")
endif()

target_link_libraries(moq-adapter-picoquic
    PUBLIC moq::core OpenSSL::SSL OpenSSL::Crypto
    PUBLIC $<BUILD_INTERFACE:${_PQ}>)

if(_PQ_LOG)
    target_link_libraries(moq-adapter-picoquic
        PUBLIC $<BUILD_INTERFACE:${_PQ_LOG}>)
endif()

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
set(_adapter_pq_link "moq::core;OpenSSL::SSL;OpenSSL::Crypto;${_PQ}")
if(_PQ_LOG)
    string(APPEND _adapter_pq_link ";${_PQ_LOG}")
endif()
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
    # Backpressure test provides its own picoquic function stubs so it
    # can exercise moq_pq_callback/moq_pq_service without a real QUIC
    # stack. Links the adapter source directly with picoquic headers
    # for type definitions but does not link the picoquic library.
    add_executable(test_adapter_backpressure
        test_backpressure.c moq_picoquic.c picoquic_endpoint.c)
    target_include_directories(test_adapter_backpressure PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
        $<TARGET_PROPERTY:${_PQ},INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_adapter_backpressure
        PRIVATE moq::core OpenSSL::SSL OpenSSL::Crypto)
    add_test(NAME adapter_backpressure COMMAND test_adapter_backpressure)

    add_executable(test_pq_conformance
        tests/test_pq_conformance.c
        tests/support/fake_pq_pair.c
        moq_picoquic.c
        picoquic_endpoint.c
        ${PROJECT_SOURCE_DIR}/tests/conformance/conformance_scenarios.c)
    target_include_directories(test_pq_conformance PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/tests/conformance
        $<TARGET_PROPERTY:${_PQ},INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_pq_conformance
        PRIVATE moq::core OpenSSL::SSL OpenSSL::Crypto)
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
            OpenSSL::SSL OpenSSL::Crypto)
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
    endif()
endif()


if(MOQ_BUILD_TESTS AND MOQ_BUILD_PQ_THREADED)
    add_executable(test_threaded_compile tests/test_threaded_compile.c)
    target_link_libraries(test_threaded_compile PRIVATE moq::adapter-picoquic-threaded)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_compile PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    add_test(NAME threaded_compile COMMAND test_threaded_compile)

    # White-box internals variant (test-only, never installed): the threaded
    # adapter compiled with MOQ_PQ_THREADED_TESTING so the lifecycle test can
    # inject a post-on_pump service-pass failure -- the terminal-conn
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
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}' && \
             '${CMAKE_COMMAND}' --build '${_threaded_consumer_bld}' && \
             '${_threaded_consumer_bld}/moq_adapter_threaded_consumer_test'")

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
                "${CMAKE_INSTALL_LIBDIR}")
        set_tests_properties(adapter_threaded_install_consumer
            PROPERTIES TIMEOUT 600)
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
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}' && \
             '${CMAKE_COMMAND}' --build '${_adapter_consumer_bld}' && \
             '${_adapter_consumer_bld}/moq_adapter_consumer_test'")
endif()
