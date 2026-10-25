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
set(_moq_pq_verify_backend "${MOQ_PICOQUIC_VERIFY_BACKEND}")
string(TOLOWER "${_moq_pq_verify_backend}" _moq_pq_verify_backend)
if(_moq_pq_verify_backend STREQUAL "auto")
    if(DEFINED WITH_OPENSSL AND NOT WITH_OPENSSL)
        if(WITH_MBEDTLS)
            set(_moq_pq_verify_backend "mbedtls")
        else()
            message(FATAL_ERROR
                "moq-adapter-picoquic cannot install a production TLS verifier "
                "when picoquic is built with WITH_OPENSSL=OFF and "
                "WITH_MBEDTLS=OFF. Enable mbedTLS or set "
                "MOQ_PICOQUIC_VERIFY_BACKEND to a supported backend.")
        endif()
    else()
        set(_moq_pq_verify_backend "openssl")
    endif()
endif()

set(_adapter_tls_link "")
set(_adapter_tls_install_link "")
set(MOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL OFF CACHE INTERNAL
    "adapter-picoquic install-tree dependency on OpenSSL")
set(MOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND "${_moq_pq_verify_backend}" CACHE INTERNAL
    "adapter-picoquic configured verifier backend")

if(_moq_pq_verify_backend STREQUAL "openssl")
    # The OpenSSL verifier branch needs picotls' OpenSSL header
    # (<picotls/openssl.h>). picotls is not installed by picoquic. When
    # FindPicoquic selected picotls headers (PTLS_INCLUDE_DIR, which follows
    # MOQ_PICOTLS_PREFIX across reconfigures), that directory is the only
    # source: a missing header there is refused rather than taken from a
    # previously cached or unrelated picotls, which would compile the
    # verifier against one picotls and link another. Only without a selection
    # is the header searched for. PRIVATE: not part of the public interface
    # (the public header forward-declares picoquic_quic_t only).
    if(PTLS_INCLUDE_DIR)
        if(NOT EXISTS "${PTLS_INCLUDE_DIR}/picotls/openssl.h")
            message(FATAL_ERROR
                "moq-adapter-picoquic selected the OpenSSL certificate "
                "verifier, but the selected picotls headers "
                "(PTLS_INCLUDE_DIR=${PTLS_INCLUDE_DIR}, "
                "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}) have no "
                "<picotls/openssl.h>. Point MOQ_PICOTLS_PREFIX at a picotls "
                "built with OpenSSL support, or use "
                "-DMOQ_PICOQUIC_VERIFY_BACKEND=mbedtls with an mbedTLS "
                "picoquic build.")
        endif()
        # The cache records the directory the verifier compiles against.
        set(MOQ_PTLS_OPENSSL_INCLUDE_DIR "${PTLS_INCLUDE_DIR}" CACHE PATH
            "Directory containing <picotls/openssl.h> for the cert verifier" FORCE)
    else()
        find_path(MOQ_PTLS_OPENSSL_INCLUDE_DIR
            NAMES picotls/openssl.h
            HINTS
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
    if(DEFINED WITH_MBEDTLS AND NOT WITH_MBEDTLS)
        message(FATAL_ERROR
            "MOQ_PICOQUIC_VERIFY_BACKEND=mbedtls requires picoquic built with "
            "-DWITH_MBEDTLS=ON.")
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

target_link_libraries(moq-adapter-picoquic
    PUBLIC moq::core ${_adapter_tls_link}
    PUBLIC $<BUILD_INTERFACE:${_PQ}>)

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
set(_adapter_pq_link "moq::core;${_adapter_tls_link};${_PQ}")
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
# installed picoquic with picoquic::picoquic-core. The adapter needs only
# picoquic-core: it uses nothing from picoquic-log, and picoquic-core does
# not depend on it.
set(_adapter_lib_name "$<TARGET_FILE_NAME:moq-adapter-picoquic>")
set(_adapter_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-picoquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
set(_adapter_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_adapter_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
set(_adapter_pq_target "picoquic::picoquic-core")
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
    # The threaded facade and its lane group (pq_lane_group.c): one source
    # list, compiled into the library and again into its test-internals.
    set(_pq_threaded_sources
        moq_picoquic_threaded.c
        pq_lane_group.c
        pq_cid_route.c
        pq_inbox.c
        pq_group_crypto.c)
    # The lane group's keyed primitives (pq_group_crypto.c) come from the
    # adapter's TLS backend through picotls's public API.
    function(moq_pq_threaded_backend target)
        if(_moq_pq_verify_backend STREQUAL "openssl")
            target_include_directories(${target}
                PRIVATE ${MOQ_PTLS_OPENSSL_INCLUDE_DIR})
            target_compile_definitions(${target}
                PRIVATE MOQ_PICOQUIC_VERIFY_OPENSSL)
        elseif(_moq_pq_verify_backend STREQUAL "mbedtls")
            target_include_directories(${target}
                SYSTEM PRIVATE
                    ${MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR}
                    ${PTLS_INCLUDE_DIR})
            target_compile_definitions(${target}
                PRIVATE MOQ_PICOQUIC_VERIFY_MBEDTLS)
        endif()
    endfunction()
    add_library(moq-adapter-picoquic-threaded ${_pq_threaded_sources})
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

    moq_pq_threaded_backend(moq-adapter-picoquic-threaded)
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
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(test_adapter_compile PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    include("${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerBuildContext.cmake")
    set(_picoquic_abi_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-abi-context.cmake")
    moq_write_consumer_abi_context("${_picoquic_abi_context}")
    # Children that link this build's archives also take its OpenSSL selection.
    include("${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerProviderContext.cmake")
    set(_picoquic_openssl_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-openssl-context.cmake")
    moq_write_consumer_openssl_context("${_picoquic_openssl_context}")

    add_executable(test_pq_core_closure tests/test_pq_core_closure.c)
    target_link_libraries(test_pq_core_closure PRIVATE ${_PQ})
    set_target_properties(test_pq_core_closure PROPERTIES
        C_STANDARD 11 C_STANDARD_REQUIRED ON)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_core_closure PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_core_closure COMMAND test_pq_core_closure)
    set_tests_properties(pq_core_closure PROPERTIES TIMEOUT 20)

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

    if(_moq_pq_verify_backend STREQUAL "openssl")
        # The delegated-trust contract on the OpenSSL provider: the real
        # verifier source against a tiny picoquic setter recorder, picotls and
        # OpenSSL real, under both public setter generations.
        foreach(_verify_callback_api IN ITEMS legacy modern)
            add_executable(test_picoquic_verify_callback_${_verify_callback_api}
                tests/test_verify_callback.c
                moq_picoquic_verify.c)
            target_include_directories(
                test_picoquic_verify_callback_${_verify_callback_api}
                BEFORE PRIVATE
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/support/verify_callback
                    ${CMAKE_CURRENT_SOURCE_DIR}/include
                    ${PROJECT_SOURCE_DIR}/core/include
                    ${PTLS_INCLUDE_DIR})
            target_compile_definitions(
                test_picoquic_verify_callback_${_verify_callback_api} PRIVATE
                    MOQ_PICOQUIC_VERIFY_OPENSSL
                    MOQ_PICOQUIC_VERIFY_TESTING=1
                    calloc=moq_test_verify_calloc
                    free=moq_test_verify_free
                    X509_STORE_new=moq_test_verify_store_new
                    X509_STORE_free=moq_test_verify_store_free
                    ptls_openssl_init_verify_certificate=moq_test_openssl_init_verify_certificate
                    ptls_openssl_dispose_verify_certificate=moq_test_openssl_dispose_verify_certificate)
            if(_verify_callback_api STREQUAL "modern")
                target_compile_definitions(
                    test_picoquic_verify_callback_${_verify_callback_api} PRIVATE
                        MOQ_TEST_PICOQUIC_MODERN_API
                        picoquic_set_verify_certificate_callback_ex=moq_test_set_verify_certificate_callback_ex)
            else()
                target_compile_definitions(
                    test_picoquic_verify_callback_${_verify_callback_api} PRIVATE
                        picoquic_set_verify_certificate_callback=moq_test_set_verify_certificate_callback)
            endif()
            target_link_libraries(
                test_picoquic_verify_callback_${_verify_callback_api} PRIVATE
                    moq::core ${_PQ} ${_adapter_tls_link})
            set_target_properties(
                test_picoquic_verify_callback_${_verify_callback_api}
                PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
            if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
                target_compile_options(
                    test_picoquic_verify_callback_${_verify_callback_api}
                    PRIVATE -Wall -Wextra -Wpedantic
                        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME picoquic_verify_callback_${_verify_callback_api}
                     COMMAND test_picoquic_verify_callback_${_verify_callback_api})
        endforeach()
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
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_adapter_backpressure PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME adapter_backpressure COMMAND test_adapter_backpressure)

    # The shared send queue's reliable-prefix accounting across the stream's
    # FIN (a reset requested after the FIN was pushed), and the bound on its
    # per-stream records. Queue and core only.
    add_executable(test_pq_send_queue_offsets
        ../common/test_pq_send_queue_offsets.c
        ../common/moq_pq_send_queue.c)
    target_compile_definitions(test_pq_send_queue_offsets PRIVATE MOQ_STATIC)
    target_include_directories(test_pq_send_queue_offsets PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../common)
    target_link_libraries(test_pq_send_queue_offsets PRIVATE moq::core)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_send_queue_offsets PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_send_queue_offsets COMMAND test_pq_send_queue_offsets)

    # Receive admission (MOQ_TRANSPORT_CAP_HOLD_INPUT): the fetcher attached
    # to the adapter through the real bridge, picoquic stubbed, a raw
    # publisher over the fake transport endpoint relayed in. Recompiles the
    # adapter source with MOQ_PICOQUIC_TESTING for the capability and
    # receive-state seams; no seam symbol exists in the shipped library.
    add_executable(test_pq_hold_input
        tests/test_pq_hold_input.c moq_picoquic.c picoquic_endpoint.c
        ../common/moq_pq_send_queue.c)
    target_compile_definitions(test_pq_hold_input PRIVATE
        MOQ_BUILDING MOQ_PICOQUIC_TESTING)
    target_include_directories(test_pq_hold_input PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/support
        ${PROJECT_SOURCE_DIR}/tests/support
        $<TARGET_PROPERTY:${_PQ},INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(test_pq_hold_input
        PRIVATE moq::core ${_adapter_tls_link})
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_hold_input PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_hold_input COMMAND test_pq_hold_input)

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
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_conformance PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_conformance COMMAND test_pq_conformance)

    # The send-half acknowledgement a graceful drain waits for, over
    # picoquic's own SACK ranges (its private stream state: source tree only).
    if(MOQ_PICOQUIC_SOURCE_DIR)
        add_executable(test_pq_stream_acked tests/test_pq_stream_acked.c)
        target_include_directories(test_pq_stream_acked PRIVATE
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_stream_acked PRIVATE $<BUILD_INTERFACE:${_PQ}>)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_stream_acked PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_stream_acked COMMAND test_pq_stream_acked)
    endif()

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
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
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
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_sim_flow_control COMMAND test_pq_sim_flow_control)
        set_tests_properties(pq_sim_flow_control PROPERTIES TIMEOUT 60)

        # MOQT 7.2 scheduling over real picoquic: group order and priorities
        # decide what the subscriber receives first on a congested link.
        add_executable(test_pq_sim_scheduling tests/test_pq_sim_scheduling.c)
        target_include_directories(test_pq_sim_scheduling PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_sim_scheduling PRIVATE
            moq::adapter-picoquic
            picoquic-test
            ${_adapter_tls_link})
        target_compile_definitions(test_pq_sim_scheduling PRIVATE
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_sim_scheduling PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_sim_scheduling COMMAND test_pq_sim_scheduling)
        set_tests_properties(pq_sim_scheduling PROPERTIES TIMEOUT 120)

        # Local-stream reset after FIN against the pinned, patched picoquic
        # (patches/picoquic/0003): constructed stream/SACK rows plus packet
        # rows over the simulated links. Provider-only; no adapter code.
        add_executable(test_pq_reset_after_fin tests/test_pq_reset_after_fin.c)
        target_include_directories(test_pq_reset_after_fin PRIVATE
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_reset_after_fin PRIVATE picoquic-test)
        target_compile_definitions(test_pq_reset_after_fin PRIVATE
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_reset_after_fin PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_reset_after_fin COMMAND test_pq_reset_after_fin)
        set_tests_properties(pq_reset_after_fin PROPERTIES TIMEOUT 120)

        # Provider-capacity policy regression: picoquic's peer uni stream
        # credit around a gapped FIN and around RESET_STREAM /
        # RESET_STREAM_AT, by decoder and producer testing after a simulated
        # handshake, plus one real-packet row for the grant reaching the peer.
        # Provider-only; no adapter code. A provider without picoquic patch
        # 0003 fails its policy rows.
        add_executable(test_pq_provider_stream_credit
            tests/test_pq_provider_stream_credit.c)
        target_include_directories(test_pq_provider_stream_credit PRIVATE
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_provider_stream_credit PRIVATE picoquic-test)
        target_compile_definitions(test_pq_provider_stream_credit PRIVATE
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_provider_stream_credit PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_provider_stream_credit COMMAND test_pq_provider_stream_credit)
        set_tests_properties(pq_provider_stream_credit PROPERTIES TIMEOUT 120)

        # Reliable resets over real picoquic: a stream reset in the pass that
        # opened it still delivers its header to the receiving application
        # when both ends advertise reset_stream_at.
        add_executable(test_pq_sim_reliable_reset tests/test_pq_sim_reliable_reset.c)
        target_include_directories(test_pq_sim_reliable_reset PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(test_pq_sim_reliable_reset PRIVATE
            moq::adapter-picoquic
            picoquic-test
            ${_adapter_tls_link})
        target_compile_definitions(test_pq_sim_reliable_reset PRIVATE
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_sim_reliable_reset PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_sim_reliable_reset COMMAND test_pq_sim_reliable_reset)
        set_tests_properties(pq_sim_reliable_reset PROPERTIES TIMEOUT 120)

        # The end of a locally stopped receive half over real picoquic: a FIN
        # picoquic withholds after STOP_SENDING still retires the stream.
        add_executable(test_pq_stop_release
            tests/test_pq_stop_release.c moq_picoquic.c picoquic_endpoint.c
            ../common/moq_pq_send_queue.c)
        target_compile_definitions(test_pq_stop_release PRIVATE
            MOQ_BUILDING MOQ_PICOQUIC_TESTING
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        target_include_directories(test_pq_stop_release PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/support
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_stop_release PRIVATE
            moq::core
            picoquic-test
            ${_adapter_tls_link})
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_stop_release PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_stop_release COMMAND test_pq_stop_release)
        set_tests_properties(pq_stop_release PROPERTIES TIMEOUT 120)

        # Shared send admission over real picoquic: the adapter's queue with
        # a small budget, the capability advertised and withdrawn, what the
        # subscriber receives compared, coalesced and standalone FINs while
        # the budget is spent.
        add_executable(test_pq_sim_shared_admission
            tests/test_pq_sim_shared_admission.c moq_picoquic.c
            picoquic_endpoint.c ../common/moq_pq_send_queue.c)
        target_compile_definitions(test_pq_sim_shared_admission PRIVATE
            MOQ_BUILDING MOQ_PICOQUIC_TESTING
            PICOQUIC_SOURCE_DIR="${MOQ_PICOQUIC_SOURCE_DIR}")
        target_include_directories(test_pq_sim_shared_admission PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../common
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/support
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_sim_shared_admission PRIVATE
            moq::core
            picoquic-test
            ${_adapter_tls_link})
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_sim_shared_admission PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_sim_shared_admission COMMAND test_pq_sim_shared_admission)
        set_tests_properties(pq_sim_shared_admission PROPERTIES TIMEOUT 300)

        include(${PROJECT_SOURCE_DIR}/cmake/MoqPicoquicTerminalTests.cmake)
        moq_add_picoquic_terminal_tests(pq moq::adapter-picoquic ${_adapter_tls_link})
    endif()

    # Source-mode builds carry patches/picoquic (see patches/picoquic/series);
    # the contract below holds only with 0004 and 0009 applied.
    if(MOQ_BUILD_TESTS AND MOQ_PICOQUIC_SOURCE_DIR AND _PQ)
        # The stateless-reset contract patches/picoquic/0009 carries: detection
        # after a NAT rebinding and behind another connection's ID, and a
        # context that never sends a reset. Public picoquic API only, so a
        # picoquic without the patch fails here, not in a relay.
        add_executable(test_pq_stateless_reset_contract
            tests/test_pq_stateless_reset_contract.c)
        target_compile_definitions(test_pq_stateless_reset_contract PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        target_include_directories(test_pq_stateless_reset_contract PRIVATE
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_stateless_reset_contract PRIVATE
            ${_PQ} ${_adapter_tls_link})
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_stateless_reset_contract PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_stateless_reset_contract
            COMMAND test_pq_stateless_reset_contract)
        set_tests_properties(pq_stateless_reset_contract PROPERTIES TIMEOUT 120)
    endif()

    # Source-mode link-policy contract (see tests/source_mode_link_policy.cmake):
    # configures scratch trees and reads their generated link lines, so it is
    # independent of the host linker's warnings.
    if(MOQ_PICOQUIC_SOURCE_DIR)
        add_executable(test_pq_close_reason tests/test_pq_close_reason.c)
        target_include_directories(test_pq_close_reason PRIVATE
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_close_reason PRIVATE moq::adapter-picoquic)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_close_reason PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_close_reason COMMAND test_pq_close_reason)
        set_tests_properties(pq_close_reason PROPERTIES TIMEOUT 30)
        get_target_property(_pq_close_provider_type ${_PQ} TYPE)
        if(_pq_close_provider_type STREQUAL "STATIC_LIBRARY")
        add_library(pq_close_reason_sender OBJECT
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic/sender.c")
        target_link_libraries(pq_close_reason_sender PRIVATE ${_PQ})
        target_include_directories(pq_close_reason_sender PRIVATE
            $<TARGET_PROPERTY:${_PQ},INCLUDE_DIRECTORIES>)
        target_compile_definitions(pq_close_reason_sender PRIVATE
            $<TARGET_PROPERTY:${_PQ},COMPILE_DEFINITIONS>
            picoquic_string_duplicate=moq_test_reason_duplicate)
        target_compile_options(pq_close_reason_sender PRIVATE
            $<TARGET_PROPERTY:${_PQ},COMPILE_OPTIONS>)
        # Upstream picoquic's own source, rebuilt only to rename its string
        # duplicator: it is compiled as the provider compiles it (options,
        # definitions, include paths, language standard), not under
        # LibMoQ's warning policy, which governs LibMoQ's own sources.
        foreach(_pq_lang_prop C_STANDARD C_STANDARD_REQUIRED C_EXTENSIONS)
            get_target_property(_pq_lang_value ${_PQ} ${_pq_lang_prop})
            if(NOT _pq_lang_value STREQUAL "_pq_lang_value-NOTFOUND")
                set_target_properties(pq_close_reason_sender PROPERTIES
                    ${_pq_lang_prop} "${_pq_lang_value}")
            endif()
        endforeach()
        add_executable(test_pq_close_reason_oom
            tests/test_pq_close_reason_oom.c
            $<TARGET_OBJECTS:pq_close_reason_sender>)
        target_include_directories(test_pq_close_reason_oom PRIVATE
            "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
        target_link_libraries(test_pq_close_reason_oom PRIVATE ${_PQ})
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_pq_close_reason_oom PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME pq_close_reason_oom COMMAND test_pq_close_reason_oom)
        set_tests_properties(pq_close_reason_oom PROPERTIES TIMEOUT 30)
        endif()
        if(UNIX)
            add_test(NAME picoquic_setup_recipe
                COMMAND bash ${PROJECT_SOURCE_DIR}/scripts/setup_picoquic_deps.sh --self-test)
            set_tests_properties(picoquic_setup_recipe PROPERTIES TIMEOUT 120)
        endif()
        add_test(NAME picoquic_source_mode_link_policy
            COMMAND ${CMAKE_COMMAND}
                -DSRC=${PROJECT_SOURCE_DIR}
                "-DWORK=${CMAKE_CURRENT_BINARY_DIR}/source-mode link-policy work"
                "-DPQ_SRC=${MOQ_PICOQUIC_SOURCE_DIR}"
                "-DPTLS_PREFIX=${MOQ_PICOTLS_PREFIX}"
                "-DC_COMPILER=${CMAKE_C_COMPILER}"
                "-DCXX_COMPILER=${CMAKE_CXX_COMPILER}"
                "-DGENERATOR=${CMAKE_GENERATOR}"
                "-DOPENSSL_CONTEXT=${_picoquic_openssl_context}"
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/source_mode_link_policy.cmake)
        set_tests_properties(picoquic_source_mode_link_policy PROPERTIES TIMEOUT 900)
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
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_compile PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_compile COMMAND test_threaded_compile)

    # Focused row for the raw threaded SERVER terminal ownership hazard:
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
    #   peer_terminal -- the hazard: picoquic_callback_close at
    #                    picoquic_state_disconnected, cnx already deleted
    #   app_close     -- CONTROL: APPLICATION_CLOSE at
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

    # Terminal-ownership controls: these must always pass, so terminal
    # ownership cannot be satisfied by unbinding a live caller-owned cnx too
    # late, or by never unbinding at all.
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
    add_test(NAME threaded_terminal_control_stop_close
             COMMAND test_threaded_terminal_controls stop_close)
    set_tests_properties(threaded_terminal_control_stop_close
        PROPERTIES TIMEOUT 120)
    add_test(NAME threaded_terminal_control_pump_exit_close
             COMMAND test_threaded_terminal_controls pump_exit_close)
    set_tests_properties(threaded_terminal_control_pump_exit_close
        PROPERTIES TIMEOUT 120)
    add_test(NAME threaded_terminal_control_client_fatal_close
             COMMAND test_threaded_terminal_controls client_fatal_close)
    set_tests_properties(threaded_terminal_control_client_fatal_close
        PROPERTIES TIMEOUT 120)
    add_test(NAME threaded_terminal_control_client_fatal_close_code
             COMMAND test_threaded_terminal_controls client_fatal_close_code)
    set_tests_properties(threaded_terminal_control_client_fatal_close_code
        PROPERTIES TIMEOUT 120)
    add_test(NAME threaded_terminal_control_server_pump_exit_close
             COMMAND test_threaded_terminal_controls server_pump_exit_close)
    set_tests_properties(threaded_terminal_control_server_pump_exit_close
        PROPERTIES TIMEOUT 120)

    # White-box internals variant (test-only, never installed): the threaded
    # adapter compiled with MOQ_PQ_THREADED_TESTING so the lifecycle test can
    # inject a post-on_lane_pump service-pass failure -- the terminal-conn
    # observation window a black-box peer close cannot reach (peer closes
    # land at packet ingestion, before the ordinary pump). Mirrors the mvfst
    # test-internals precedent.
    add_library(moq-adapter-picoquic-threaded-test-internals STATIC
        ${_pq_threaded_sources})
    moq_pq_threaded_backend(moq-adapter-picoquic-threaded-test-internals)
    target_include_directories(moq-adapter-picoquic-threaded-test-internals
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
    target_include_directories(moq-adapter-picoquic-threaded-test-internals
        PRIVATE "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    # The group crypto in this archive (pq_group_crypto.c) calls the picotls
    # OpenSSL backend directly. A picotls built outside this tree reaches
    # picoquic-core as plain archive paths, which carry no edge to the
    # backend's crypto library, and picoquic-core lists its OpenSSL ahead of
    # them. A linker that keeps a shared library only when something before it
    # needs one (GNU ld's --as-needed) then leaves the backend archive's
    # OpenSSL references unresolved in a test whose only OpenSSL user is that
    # archive. So this archive also depends on the backend archive as an
    # imported target that carries the edge, which places OpenSSL after it.
    # The archive is then named twice, harmlessly for such a linker; Apple's
    # linker needs no order and reports a library named twice, so there the
    # archive is left to picoquic-core alone.
    set(_pq_ptls_backend "")
    if(_moq_pq_verify_backend STREQUAL "openssl" AND NOT APPLE AND
       TARGET picoquic-core AND TARGET OpenSSL::Crypto)
        get_target_property(_pq_core_links picoquic-core INTERFACE_LINK_LIBRARIES)
        foreach(_item IN LISTS _pq_core_links)
            if(_item MATCHES "(^|/)libpicotls-openssl\\.a$")
                add_library(moq_pq_test_ptls_openssl STATIC IMPORTED)
                set_target_properties(moq_pq_test_ptls_openssl PROPERTIES
                    IMPORTED_LOCATION "${_item}"
                    INTERFACE_LINK_LIBRARIES OpenSSL::Crypto)
                set(_pq_ptls_backend moq_pq_test_ptls_openssl)
                break()
            endif()
        endforeach()
    endif()
    target_link_libraries(moq-adapter-picoquic-threaded-test-internals
        PUBLIC ${_pq_ptls_backend} moq::adapter-picoquic Threads::Threads)
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
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_lifecycle PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_lifecycle COMMAND test_threaded_lifecycle)

    add_executable(test_threaded_terminal_error
        tests/test_threaded_terminal_error.c)
    target_link_libraries(test_threaded_terminal_error PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    target_include_directories(test_threaded_terminal_error PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    target_compile_definitions(test_threaded_terminal_error PRIVATE
        MOQ_PQ_THREADED_TESTING)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_terminal_error PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_terminal_error COMMAND test_threaded_terminal_error)
    set_tests_properties(threaded_terminal_error PROPERTIES TIMEOUT 30)

    # A server receive with no connection input still pumps the application;
    # only its pre-pump service is skipped.
    add_executable(test_threaded_quiet_receive
        tests/test_threaded_quiet_receive.c)
    target_link_libraries(test_threaded_quiet_receive PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    target_include_directories(test_threaded_quiet_receive PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    target_compile_definitions(test_threaded_quiet_receive PRIVATE
        MOQ_PQ_THREADED_TESTING)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_quiet_receive PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_quiet_receive COMMAND test_threaded_quiet_receive)
    set_tests_properties(threaded_quiet_receive PROPERTIES TIMEOUT 30)

    # Terminal retention until the application acknowledges, for every
    # terminal cause, plus the per-connection user slot (test-internals for
    # the service-failure seam).
    add_executable(test_threaded_retain_terminal
        tests/test_threaded_retain_terminal.c)
    target_link_libraries(test_threaded_retain_terminal PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_retain_terminal PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_retain_terminal PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    foreach(_row peer_close app_close service_fatal cap cap_legacy legacy prefix
                 client stop)
        add_test(NAME threaded_retain_terminal_${_row}
                 COMMAND test_threaded_retain_terminal ${_row})
        set_tests_properties(threaded_retain_terminal_${_row}
            PROPERTIES TIMEOUT 120)
    endforeach()

    # The facade's wake scheduling against a model of picoquic's select.
    add_executable(test_pq_wake_sched tests/test_pq_wake_sched.c)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_wake_sched PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_wake_sched COMMAND test_pq_wake_sched)

    # The lane group's pure parts: the router over fake owner functions, the
    # inbox under producers, and the connection IDs over the TLS backend.
    add_executable(test_pq_cid_route tests/test_pq_cid_route.c pq_cid_route.c)
    add_executable(test_pq_inbox tests/test_pq_inbox.c pq_inbox.c)
    target_link_libraries(test_pq_inbox PRIVATE moq::core Threads::Threads)
    add_executable(test_pq_recv_budget tests/test_pq_recv_budget.c)
    # The group crypto is internal (hidden in the shared adapter), so the test
    # takes it from the static test-internals archive, not the installed
    # library's exported surface. The backend include lets its header's
    # <picotls.h> compile.
    add_executable(test_pq_group_crypto tests/test_pq_group_crypto.c)
    target_include_directories(test_pq_group_crypto PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    target_link_libraries(test_pq_group_crypto PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    moq_pq_threaded_backend(test_pq_group_crypto)
    foreach(_t test_pq_cid_route test_pq_inbox test_pq_group_crypto test_pq_recv_budget)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(${_t} PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
    endforeach()
    add_test(NAME pq_cid_route COMMAND test_pq_cid_route)
    add_test(NAME pq_inbox COMMAND test_pq_inbox)
    add_test(NAME pq_group_crypto COMMAND test_pq_group_crypto)
    add_test(NAME pq_recv_budget COMMAND test_pq_recv_budget)
    set_tests_properties(pq_inbox PROPERTIES TIMEOUT 120)

    # The streaming_objects, socket_buffer_bytes and bind_address options, and
    # the server's pump and wake behaviour.
    add_executable(test_threaded_server_options
        tests/test_threaded_server_options.c)
    target_link_libraries(test_threaded_server_options PRIVATE
        moq::adapter-picoquic-threaded Threads::Threads)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_server_options PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_server_options PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    foreach(_row streaming_on streaming_off socket_buffer wake_pressure bind_v4
                 bind_v6 socket_buffer_limit idle_wake idle_deadline)
        add_test(NAME threaded_server_options_${_row}
                 COMMAND test_threaded_server_options ${_row})
        set_tests_properties(threaded_server_options_${_row}
            PROPERTIES TIMEOUT 120)
    endforeach()

    # The lane group over loopback: real clients against a 4-lane server, a
    # NAT rebinding through a proxy, a stop under load, the per-lane
    # deadline, the group's admission total, and the bounds and accessors.
    add_executable(test_threaded_lane_group tests/test_threaded_lane_group.c)
    # Links the test-internals archive: stop_load places clients on lanes
    # through its routing control. The white-box facade header is read, so the
    # test sees the same testing layout as the archive.
    target_compile_definitions(test_threaded_lane_group PRIVATE
        MOQ_PQ_THREADED_TESTING
        MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
        MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    target_link_libraries(test_threaded_lane_group PRIVATE
        moq-adapter-picoquic-threaded-test-internals Threads::Threads)
    # The invariants row reads context settings the group re-asserts; the
    # backend include provides <picotls.h> for that white-box check.
    moq_pq_threaded_backend(test_threaded_lane_group)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_lane_group PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    foreach(_row spread rebind reset reset_crosslane reset_local reset_forwarded reset_controls stop_load deadline deadline_one admission limits invariants oom bind_specific)
        add_test(NAME threaded_lane_group_${_row}
            COMMAND test_threaded_lane_group ${_row})
        set_tests_properties(threaded_lane_group_${_row} PROPERTIES TIMEOUT 120)
    endforeach()

    # The drain gate on session pending output at a parked pass boundary,
    # for the client single-connection and server aggregate paths (stage
    # hooks of the test-internals build).
    add_executable(test_threaded_drain_gates tests/test_threaded_drain_gates.c)
    target_link_libraries(test_threaded_drain_gates PRIVATE
        moq-adapter-picoquic-threaded-test-internals Threads::Threads)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_drain_gates PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_drain_gates PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_drain_gates COMMAND test_threaded_drain_gates)
    set_tests_properties(threaded_drain_gates PROPERTIES TIMEOUT 120)

    # What does and does not hold the drain: receive-only backlog, an open
    # idle draft-18 request stream and a future timer do not; an unACKed FIN
    # does.
    add_executable(test_threaded_drain_controls tests/test_threaded_drain_controls.c)
    target_link_libraries(test_threaded_drain_controls PRIVATE
        moq-adapter-picoquic-threaded-test-internals Threads::Threads)
    target_include_directories(test_threaded_drain_controls PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_drain_controls PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_drain_controls PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_drain_controls COMMAND test_threaded_drain_controls)
    set_tests_properties(threaded_drain_controls PROPERTIES TIMEOUT 180)

    # The drain holds while the session retains reliable lifecycle output: a
    # draft-18 rejection that reaches a client whose event queue is full.
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        add_executable(test_threaded_drain_retained tests/test_threaded_drain_retained.c)
        target_link_libraries(test_threaded_drain_retained PRIVATE
            moq-adapter-picoquic-threaded-test-internals Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_threaded_drain_retained PRIVATE
                -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME threaded_drain_retained
            COMMAND test_threaded_drain_retained
                "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(threaded_drain_retained PROPERTIES TIMEOUT 120)
    endif()

    # SETUP authorization tokens through the facade over real loopback: the
    # server's SETUP_COMPLETE carries the configured tokens by value; a set
    # whose SETUP cannot fit the session send arena is refused at create.
    add_executable(test_threaded_setup_tokens tests/test_threaded_setup_tokens.c)
    target_link_libraries(test_threaded_setup_tokens PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        target_compile_definitions(test_threaded_setup_tokens PRIVATE
            MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
            MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_setup_tokens PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_setup_tokens COMMAND test_threaded_setup_tokens)
    set_tests_properties(threaded_setup_tokens PROPERTIES
        TIMEOUT 120 LABELS "network")

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
        MOQ_TEST_CERT_PATH="${_pq_tls_cert}"
        MOQ_TEST_KEY_PATH="${_pq_tls_key}")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_tls_policy PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
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
        MOQ_TEST_CERT_PATH="${_pq_tls_cert}"
        MOQ_TEST_KEY_PATH="${_pq_tls_key}"
        MOQ_TEST_CA_PATH="${_pq_tls_ca}")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_tls_handshake PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
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
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_threaded_keepalive PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME threaded_keepalive COMMAND test_threaded_keepalive)

    add_executable(test_pq_deadline tests/test_pq_deadline.c)
    target_link_libraries(test_pq_deadline PRIVATE
        moq-adapter-picoquic-threaded-test-internals)
    target_include_directories(test_pq_deadline PRIVATE
        "${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
    # Builds a facade struct directly, so it must see the same layout as the
    # test-internals archive (the testing-only fields change field offsets).
    target_compile_definitions(test_pq_deadline PRIVATE MOQ_PQ_THREADED_TESTING)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_deadline PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_deadline COMMAND test_pq_deadline)
    set_tests_properties(pq_deadline PROPERTIES TIMEOUT 30)

    # Drives construction to a lane-spawn failure after an earlier lane's
    # worker is running, through the test-internals spawn seam, and confirms the
    # unwind leaks no thread, descriptor or allocation. Public API only, so no
    # facade-layout dependency: the seam symbols come from the archive.
    add_executable(test_pq_lane_group_unwind tests/test_pq_lane_group_unwind.c)
    target_link_libraries(test_pq_lane_group_unwind PRIVATE
        moq-adapter-picoquic-threaded-test-internals Threads::Threads)
    target_compile_definitions(test_pq_lane_group_unwind PRIVATE
        MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
        MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_lane_group_unwind PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_lane_group_unwind COMMAND test_pq_lane_group_unwind)
    set_tests_properties(pq_lane_group_unwind PROPERTIES TIMEOUT 30)

    # Drives one real receive turn through the test-internals seams: over two
    # scripted sockets the per-turn budget is shared exactly fairly
    # (pq_recv_turn), and over two real loopback sockets the turn reads within
    # each socket's share (pq_recv_turn_native). The use of pq_recv_share, not
    # its arithmetic; no connection context.
    add_executable(test_pq_recv_turn tests/test_pq_recv_turn.c)
    target_link_libraries(test_pq_recv_turn PRIVATE
        moq-adapter-picoquic-threaded-test-internals Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_recv_turn PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_recv_turn COMMAND test_pq_recv_turn fair)
    set_tests_properties(pq_recv_turn PROPERTIES TIMEOUT 30)
    add_test(NAME pq_recv_turn_native COMMAND test_pq_recv_turn native)
    set_tests_properties(pq_recv_turn_native PROPERTIES TIMEOUT 30)

    # Reads a lane's detect-drop fold through the public lane-stats getter after
    # presetting the counters through the test-internals seam, so the public
    # projection's sum and saturation are covered, not only the helper.
    add_executable(test_pq_lane_stats tests/test_pq_lane_stats.c)
    target_link_libraries(test_pq_lane_stats PRIVATE
        moq-adapter-picoquic-threaded-test-internals Threads::Threads)
    target_compile_definitions(test_pq_lane_stats PRIVATE
        MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
        MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_lane_stats PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_lane_stats COMMAND test_pq_lane_stats)
    set_tests_properties(pq_lane_stats PROPERTIES TIMEOUT 30)

    # A raw picoquic client resumes against a real two-lane group server and
    # confirms, with traffic, that the group refuses 0-RTT (its ticket forbids
    # early data) while ordinary 1-RTT resumption works. Reads the client's
    # resumption fields, so it needs the backend picoquic headers.
    add_executable(test_pq_zero_rtt_refused tests/test_pq_zero_rtt_refused.c)
    target_compile_definitions(test_pq_zero_rtt_refused PRIVATE
        MOQ_TEST_CERT_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
        MOQ_TEST_KEY_PATH="${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
    target_link_libraries(test_pq_zero_rtt_refused PRIVATE
        moq::adapter-picoquic-threaded Threads::Threads)
    moq_pq_threaded_backend(test_pq_zero_rtt_refused)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_pq_zero_rtt_refused PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME pq_zero_rtt_refused COMMAND test_pq_zero_rtt_refused)
    set_tests_properties(pq_zero_rtt_refused PROPERTIES TIMEOUT 60)

    set(_threaded_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer-threaded")
    set(_threaded_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-threaded-test-build")
    include("${PROJECT_SOURCE_DIR}/tests/cmake/NestedToolchain.cmake")
    moq_nested_cxx_args(_threaded_consumer_cxx)
    # Forward the parent's compile/link flags and build type: the consumer
    # links the parent's archives, so a sanitizer parent build needs a
    # consumer instrumented the same way.
    add_test(NAME adapter_threaded_consumer
        COMMAND ${CMAKE_COMMAND} -E env bash -c
            "set -e; \
             '${CMAKE_COMMAND}' -E rm -rf '${_threaded_consumer_bld}'; \
             '${CMAKE_COMMAND}' -C '${_picoquic_abi_context}' \
                 -C '${_picoquic_openssl_context}' -S '${_threaded_consumer_src}' \
                 -B '${_threaded_consumer_bld}' \
                 -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                 -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                 -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}'${_threaded_consumer_cxx} \
                 -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}' \
                 -DCMAKE_EXE_LINKER_FLAGS='${CMAKE_EXE_LINKER_FLAGS}' \
                 -DCMAKE_BUILD_TYPE='${CMAKE_BUILD_TYPE}' && \
             '${CMAKE_COMMAND}' --build '${_threaded_consumer_bld}' && \
             '${_threaded_consumer_bld}/moq_adapter_threaded_consumer_test'")
    include("${PROJECT_SOURCE_DIR}/tests/cmake/RejectConsumerWarnings.cmake")
    moq_test_reject_consumer_warnings(adapter_threaded_consumer)

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
        include("${PROJECT_SOURCE_DIR}/tests/cmake/NestedToolchain.cmake")
        moq_nested_cxx_env(_threaded_install_cxx_env)
        include("${PROJECT_SOURCE_DIR}/tests/cmake/PicoquicInstallStaging.cmake")
        moq_picoquic_install_staging(_threaded_install_staging)
        add_test(NAME adapter_threaded_install_consumer
            COMMAND ${CMAKE_COMMAND} -E env ${_threaded_install_cxx_env} bash
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
                "${CMAKE_CXX_FLAGS}"
                "${_threaded_install_staging}"
                "${CMAKE_INSTALL_INCLUDEDIR}"
                "${_picoquic_abi_context}")
        set_tests_properties(adapter_threaded_install_consumer
            PROPERTIES TIMEOUT 600 RUN_SERIAL TRUE)
    endif()

    # Group-order negotiation and complete delivery over real picoquic in
    # drafts 16, 18 and 22. Deterministic tests own the scheduling oracle.
    if(MOQ_PICOQUIC_SOURCE_DIR AND EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem")
        add_executable(test_group_order_e2e tests/test_group_order_e2e.c)
        target_link_libraries(test_group_order_e2e PRIVATE
            moq::adapter-picoquic-threaded)
        target_include_directories(test_group_order_e2e PRIVATE
            ${PROJECT_SOURCE_DIR}/tests/unit)
        target_compile_options(test_group_order_e2e PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        add_test(NAME group_order_e2e_helpers
            COMMAND test_group_order_e2e --self-test)
        set_tests_properties(group_order_e2e_helpers PROPERTIES TIMEOUT 30)
        add_test(NAME group_order_e2e
            COMMAND test_group_order_e2e
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        # Twelve cases: 12 * (10 s receive + 1 s cleanup + 2 * 3 s stop)
        # = 204 s, plus headroom for listener startup and scheduling.
        set_tests_properties(group_order_e2e PROPERTIES
            LABELS "network" TIMEOUT 240 RESOURCE_LOCK service_network)
    endif()
endif()

if(MOQ_BUILD_TESTS)
    set(_adapter_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer")
    set(_adapter_consumer_bld "${CMAKE_CURRENT_BINARY_DIR}/consumer-test-build")
    set(_adapter_consumer_deps "")
    include("${PROJECT_SOURCE_DIR}/tests/cmake/NestedToolchain.cmake")
    moq_nested_cxx_args(_adapter_consumer_cxx FLAGS)
    foreach(_dep IN ITEMS PTLS_INCLUDE_DIR MOQ_PTLS_LIB_DIR OPENSSL_INCLUDE_DIR
                          OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
        if(${_dep})
            string(APPEND _adapter_consumer_deps " -D${_dep}='${${_dep}}'")
        endif()
    endforeach()
    add_test(NAME adapter_consumer
        COMMAND ${CMAKE_COMMAND} -E env bash -c
            "set -e; \
             '${CMAKE_COMMAND}' -E rm -rf '${_adapter_consumer_bld}'; \
             '${CMAKE_COMMAND}' -C '${_picoquic_abi_context}' -S '${_adapter_consumer_src}' \
                 -B '${_adapter_consumer_bld}' \
                 -DCMAKE_PREFIX_PATH='${PROJECT_BINARY_DIR}' \
                 -DMOQ_PICOQUIC_SOURCE_DIR='${MOQ_PICOQUIC_SOURCE_DIR}' \
                 -DMOQ_PICOTLS_PREFIX='${MOQ_PICOTLS_PREFIX}' \
                 -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}' \
                 -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}'${_adapter_consumer_cxx} \
                 -DCMAKE_EXE_LINKER_FLAGS='${CMAKE_EXE_LINKER_FLAGS}' \
                 -DCMAKE_BUILD_TYPE='${CMAKE_BUILD_TYPE}' \
                 -DCMAKE_OSX_SYSROOT='${CMAKE_OSX_SYSROOT}' \
                 -DMOQ_CONSUMER_WARNINGS_AS_ERRORS='${MOQ_WARNINGS_AS_ERRORS}' \
                 ${_adapter_consumer_deps} && \
             '${CMAKE_COMMAND}' --build '${_adapter_consumer_bld}' && \
             '${_adapter_consumer_bld}/moq_adapter_consumer_test'")
    include("${PROJECT_SOURCE_DIR}/tests/cmake/RejectConsumerWarnings.cmake")
    moq_test_reject_consumer_warnings(adapter_consumer)
endif()
