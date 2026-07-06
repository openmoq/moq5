# moq-service — the service tier (moq_endpoint_t + media receiver/sender).
# Severable: lower tiers build and work without it; the core stays sans-I/O
# and all threading lives here.

add_library(moq-service
    src/endpoint.c
    src/media_receiver.c
    src/media_sender.c
)

add_library(moq::service ALIAS moq-service)

target_include_directories(moq-service
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-service PUBLIC moq::core moq::msf)
target_compile_features(moq-service PUBLIC c_std_11)

# Managed transport facades: linked when built, otherwise the connect path
# returns MOQ_ERR_UNSUPPORTED (severability -- never an accidental link
# failure). The endpoint adds the service-tier contracts on top of them.
find_package(Threads REQUIRED)
target_link_libraries(moq-service PRIVATE Threads::Threads)
set(_MOQ_SERVICE_FACADE_DEFS "")
set(_MOQ_SERVICE_FACADE_LIBS "")
if(MOQ_BUILD_PQ_THREADED)
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_PQ_THREADED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-picoquic-threaded
                                         moq::adapter-picoquic)
endif()
if(MOQ_BUILD_PICO_WT_MANAGED)
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_PICO_WT_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-pico-wt-managed)
    if(NOT MOQ_BUILD_PQ_THREADED)
        # The cert-verifier helper lives in the base picoquic adapter.
        list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-picoquic)
    endif()
endif()
if(MOQ_BUILD_ADAPTER_MVFST)
    # RAW_QUIC over Meta's mvfst (exact-version only; see src/endpoint.c). The
    # managed C facade lives in moq::adapter-mvfst -- there is no separate
    # -managed target. CMake-only: mvfst/folly/fizz ship no pkg-config, so this
    # backend is NOT advertised in libmoq-service.pc (see below).
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_MVFST_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-mvfst)
endif()
if(MOQ_BUILD_ADAPTER_PROXYGEN)
    # WebTransport over proxygen (multi-version negotiation; see src/endpoint.c).
    # The managed C facade is moq::adapter-proxygen-wt-managed. CMake-only:
    # proxygen/folly/fizz ship no pkg-config, so this backend is NOT advertised
    # in libmoq-service.pc (see below).
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-proxygen-wt-managed)
endif()
target_compile_definitions(moq-service PRIVATE ${_MOQ_SERVICE_FACADE_DEFS})
target_link_libraries(moq-service PRIVATE ${_MOQ_SERVICE_FACADE_LIBS})

set_target_properties(moq-service PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-service
    EXPORT_NAME service
)

install(TARGETS moq-service
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

# pkg-config: a standalone libmoq-service.pc so Meson/autotools/VLC-style
# consumers can `pkg-config --cflags --libs libmoq-service` (or Meson
# `dependency('libmoq-service')`) without CMake COMPONENTS. NOT folded into
# libmoq.pc -- the service tier is an optional higher-level component, like the
# WT adapter packages. The CMake find_package(libmoq COMPONENTS service) path
# above is unaffected.
#
# Requires: libmoq (always; pulls core/codec + the picoquic raw-QUIC facade libs
# when built) plus libmoq-pico-wt-managed when the managed WebTransport facade
# was compiled in, so its static deps come through.
#
# When the service is built WITH the mvfst OR proxygen backend, the .pc is NOT
# generated or installed at all: a static moq-service then references
# moq_mvfst_managed_* / moq_proxygen_wt_managed_* from endpoint.c.o, whose link
# chain is mvfst/proxygen/folly/fizz -- a C++ CMake-config stack with no
# pkg-config representation. A `pkg-config --libs libmoq-service` build would
# fail to link. Rather than advertise an unlinkable library, an mvfst/proxygen-
# enabled service is CMake-only (find_package(libmoq COMPONENTS service) links
# the managed adapter). pkg-config consumers must use a service build without
# those C++ backends. (A future split could keep a pkg-config-linkable
# picoquic-only service.)
if(NOT MOQ_BUILD_ADAPTER_MVFST AND NOT MOQ_BUILD_ADAPTER_PROXYGEN)
    set(LIBMOQ_SERVICE_PC_REQUIRES "libmoq")
    if(MOQ_BUILD_PICO_WT_MANAGED)
        string(APPEND LIBMOQ_SERVICE_PC_REQUIRES " libmoq-pico-wt-managed")
    endif()

    # Relocatable prefix: ascend (libdir depth + 1) levels from
    # <prefix>/<libdir>/pkgconfig back to <prefix> (handles lib, lib64, multiarch).
    string(REPLACE "/" ";" _svc_pc_libdir_parts "${CMAKE_INSTALL_LIBDIR}")
    list(LENGTH _svc_pc_libdir_parts _svc_pc_libdir_depth)
    math(EXPR _svc_pc_ascents "${_svc_pc_libdir_depth} + 1")
    set(LIBMOQ_SERVICE_PC_UP "..")
    foreach(_svc_pc_i RANGE 2 ${_svc_pc_ascents})
        string(APPEND LIBMOQ_SERVICE_PC_UP "/..")
    endforeach()

    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoq-service.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoq-service.pc
        @ONLY)

    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoq-service.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endif()

# find_package export (hand-written imported target, like the adapters --
# the service links the managed facades, whose imported targets are not in
# the install(EXPORT) set, so install(EXPORT) cannot record them). A static
# moq-service carries its PRIVATE facade deps as link-only interface deps,
# so the consumer must link the facades too: reflect the linked facade set
# (plus the PUBLIC core/msf deps) into INTERFACE_LINK_LIBRARIES. The
# `service` component branch in libmoqConfig loads those targets first.
set(_service_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_service_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
set(_service_iface "moq::core;moq::msf;Threads::Threads;${_MOQ_SERVICE_FACADE_LIBS}")
string(REPLACE ";" "$<SEMICOLON>" _service_iface_gen "${_service_iface}")

file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqServiceTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::service)\n\
  add_library(moq::service $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-service,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::service PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-service>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"${_service_iface_gen}\")\n\
endif()\n")

set(_service_lib_name "$<TARGET_FILE_NAME:moq-service>")
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqServiceTargets-install.cmake
    CONTENT "\
get_filename_component(_SERVICE_PREFIX \"\${CMAKE_CURRENT_LIST_FILE}\" PATH)\n\
get_filename_component(_SERVICE_PREFIX \"\${_SERVICE_PREFIX}\" PATH)\n\
get_filename_component(_SERVICE_PREFIX \"\${_SERVICE_PREFIX}\" PATH)\n\
get_filename_component(_SERVICE_PREFIX \"\${_SERVICE_PREFIX}\" PATH)\n\
\n\
if(NOT TARGET moq::service)\n\
  add_library(moq::service $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-service,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::service PROPERTIES\n\
    IMPORTED_LOCATION \"\${_SERVICE_PREFIX}/${_service_libdir}/${_service_lib_name}\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"\${_SERVICE_PREFIX}/${_service_incdir}\"\n\
    INTERFACE_LINK_LIBRARIES \"${_service_iface_gen}\")\n\
endif()\n\
\n\
unset(_SERVICE_PREFIX)\n")

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqServiceTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqServiceTargets.cmake)

target_compile_definitions(moq-service PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-service PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    # White-box target: the resolution layer is internal (endpoint_internal.h,
    # not installed); tests compile the sources directly so the internal
    # symbols stay un-exported in the shared library (same pattern as
    # moq-core-test-internals).
    add_library(moq-service-test-internals OBJECT src/endpoint.c)
    target_include_directories(moq-service-test-internals
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(moq-service-test-internals PUBLIC moq::core
        Threads::Threads)
    target_compile_definitions(moq-service-test-internals
        PRIVATE ${_MOQ_SERVICE_FACADE_DEFS})
    target_link_libraries(moq-service-test-internals
        PUBLIC ${_MOQ_SERVICE_FACADE_LIBS})
    target_compile_features(moq-service-test-internals PUBLIC c_std_11)

    add_executable(test_endpoint_resolve tests/test_endpoint_resolve.c)
    target_link_libraries(test_endpoint_resolve PRIVATE
        moq-service-test-internals moq::core)
    # The test's compile-guarded cases (#ifdef MOQ_SERVICE_HAVE_*_MANAGED) must
    # see the same facade set the linked resolver was built with, or a built
    # backend takes the "not built" branch and mismatches.
    target_compile_definitions(test_endpoint_resolve PRIVATE
        ${_MOQ_SERVICE_FACADE_DEFS})
    target_include_directories(test_endpoint_resolve PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/tests/unit)
    add_test(NAME endpoint_resolve COMMAND test_endpoint_resolve)

    # White-box catalog-update test: compile the receiver (+ endpoint) with the
    # test seam so the catalog ingest/reconcile path can be driven with crafted
    # objects, no network. Built as its own object set (the testing define would
    # otherwise leak into the shipping receiver).
    if(TARGET moq::msf)
        add_library(moq-service-receiver-test-internals OBJECT
            src/media_receiver.c src/endpoint.c)
        target_include_directories(moq-service-receiver-test-internals
            PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(moq-service-receiver-test-internals
            PUBLIC moq::core moq::msf Threads::Threads)
        target_compile_definitions(moq-service-receiver-test-internals
            PRIVATE MOQ_MEDIA_RECEIVER_TESTING=1 ${_MOQ_SERVICE_FACADE_DEFS})
        target_link_libraries(moq-service-receiver-test-internals
            PUBLIC ${_MOQ_SERVICE_FACADE_LIBS})
        target_compile_features(moq-service-receiver-test-internals
            PUBLIC c_std_11)

        add_executable(test_media_catalog_update
            tests/test_media_catalog_update.c)
        target_link_libraries(test_media_catalog_update PRIVATE
            moq-service-receiver-test-internals moq::core moq::msf
            Threads::Threads)
        target_include_directories(test_media_catalog_update PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_catalog_update COMMAND test_media_catalog_update)

        # Scripted-peer facade test (no network): drive the REAL
        # receiver_hook with a simpair client session and inject crafted moqx
        # control + subgroup data bytes. Requires moq-sim.
        if(TARGET moq::sim)
            add_executable(test_media_receiver_scripted
                tests/test_media_receiver_scripted.c)
            target_link_libraries(test_media_receiver_scripted PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_scripted PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_scripted
                COMMAND test_media_receiver_scripted)

            # Large-object intake guard: a single large LOC video object
            # (64/128/256 KiB, plus a type-0x31 extension-enabled variant) fed
            # in wire chunks must surface via poll_object byte-exact through the
            # real media_receiver, covering the session -> subscriber ->
            # media_receiver intake path for large payloads.
            add_executable(test_media_receiver_largeobj
                tests/test_media_receiver_largeobj.c)
            target_link_libraries(test_media_receiver_largeobj PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_largeobj PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_largeobj
                COMMAND test_media_receiver_largeobj)

            # Regression: catalog delivered via a FETCH object
            # stream (object arrives before FETCH_OK).
            add_executable(test_media_receiver_fetch_catalog
                tests/test_media_receiver_fetch_catalog.c)
            target_link_libraries(test_media_receiver_fetch_catalog PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_fetch_catalog PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_fetch_catalog
                COMMAND test_media_receiver_fetch_catalog)

            # Roleless MSF catalog classified by codec.
            add_executable(test_media_receiver_roleless
                tests/test_media_receiver_roleless.c)
            target_link_libraries(test_media_receiver_roleless PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_roleless PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_roleless
                COMMAND test_media_receiver_roleless)

            # Receiver destroy cancels live subscriptions: drive the real hook to
            # ACTIVE catalog + media subscriptions, run the production teardown
            # path, and assert UNSUBSCRIBE is emitted for each (not just a local
            # facade destroy that leaves session subscriptions active).
            add_executable(test_media_receiver_teardown
                tests/test_media_receiver_teardown.c)
            target_link_libraries(test_media_receiver_teardown PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_teardown PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_teardown
                COMMAND test_media_receiver_teardown)

            # Teardown RESUMES under transient session-queue backpressure: the
            # cancel task returns WOULD_BLOCK and the pump requeues the same node.
            add_executable(test_media_receiver_teardown_retry
                tests/test_media_receiver_teardown_retry.c)
            target_link_libraries(test_media_receiver_teardown_retry PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_teardown_retry PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_teardown_retry
                COMMAND test_media_receiver_teardown_retry)
        endif()

        # White-box sender catalog-content test (no network): build the catalog
        # the sender would publish after add/remove, to pin coalescing and the
        # no-op dedup deterministically.
        add_library(moq-service-sender-test-internals OBJECT
            src/media_sender.c src/endpoint.c)
        target_include_directories(moq-service-sender-test-internals
            PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(moq-service-sender-test-internals
            PUBLIC moq::core moq::msf Threads::Threads)
        target_compile_definitions(moq-service-sender-test-internals
            PRIVATE MOQ_MEDIA_SENDER_TESTING=1 ${_MOQ_SERVICE_FACADE_DEFS})
        target_link_libraries(moq-service-sender-test-internals
            PUBLIC ${_MOQ_SERVICE_FACADE_LIBS})
        target_compile_features(moq-service-sender-test-internals
            PUBLIC c_std_11)

        add_executable(test_media_sender_catalog
            tests/test_media_sender_catalog.c)
        target_link_libraries(test_media_sender_catalog PRIVATE
            moq-service-sender-test-internals moq::core moq::msf
            Threads::Threads)
        target_include_directories(test_media_sender_catalog PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_sender_catalog COMMAND test_media_sender_catalog)

        # Lifecycle callbacks (on_ready / on_closed): deterministic, no network.
        # Drives the production fire helpers via the same test seam.
        add_executable(test_media_sender_lifecycle
            tests/test_media_sender_lifecycle.c)
        target_link_libraries(test_media_sender_lifecycle PRIVATE
            moq-service-sender-test-internals moq::core moq::msf
            Threads::Threads)
        target_include_directories(test_media_sender_lifecycle PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_sender_lifecycle
            COMMAND test_media_sender_lifecycle)
    endif()

    # Lifecycle tests need a real network loopback: a threaded-picoquic
    # server on one side, the endpoint client on the other.
    if(MOQ_BUILD_PQ_THREADED)
        add_executable(test_endpoint_lifecycle tests/test_endpoint_lifecycle.c)
        target_link_libraries(test_endpoint_lifecycle PRIVATE
            moq::service moq::adapter-picoquic-threaded Threads::Threads)
        target_include_directories(test_endpoint_lifecycle PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME endpoint_lifecycle
            COMMAND test_endpoint_lifecycle
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        # Real-UDP-socket tests with PID-derived port ranges are not safe to
        # run concurrently with each other; the shared RESOURCE_LOCK serializes
        # them under ctest -jN (LABELS alone do not).
        set_tests_properties(endpoint_lifecycle PROPERTIES
            LABELS "network" TIMEOUT 60 RESOURCE_LOCK service_network)

        add_executable(test_media_receiver
            tests/test_media_receiver.c
            ${CMAKE_SOURCE_DIR}/tests/support/media_builders.c)
        target_link_libraries(test_media_receiver PRIVATE
            moq::service moq::adapter-picoquic-threaded moq::msf
            Threads::Threads)
        target_include_directories(test_media_receiver PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit
            ${CMAKE_SOURCE_DIR}/tests/support)
        add_test(NAME media_receiver
            COMMAND test_media_receiver
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(media_receiver PROPERTIES
            LABELS "network" TIMEOUT 60 RESOURCE_LOCK service_network)

        add_executable(test_media_receiver_subscribe
            tests/test_media_receiver_subscribe.c)
        target_link_libraries(test_media_receiver_subscribe PRIVATE
            moq::service moq::adapter-picoquic-threaded moq::msf
            Threads::Threads)
        target_include_directories(test_media_receiver_subscribe PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_receiver_subscribe
            COMMAND test_media_receiver_subscribe
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(media_receiver_subscribe PROPERTIES
            LABELS "network" TIMEOUT 60 RESOURCE_LOCK service_network)

        add_executable(test_media_sender
            tests/test_media_sender.c
            ${CMAKE_SOURCE_DIR}/tests/support/media_builders.c)
        target_link_libraries(test_media_sender PRIVATE
            moq::service moq::adapter-picoquic-threaded moq::msf
            Threads::Threads)
        target_include_directories(test_media_sender PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit
            ${CMAKE_SOURCE_DIR}/tests/support)
        add_test(NAME media_sender
            COMMAND test_media_sender
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(media_sender PROPERTIES
            LABELS "network" TIMEOUT 60 RESOURCE_LOCK service_network)

        # pkg-config consumer: a plain C program built with ONLY
        # `pkg-config --cflags --libs libmoq-service` from an install prefix --
        # the Meson/VLC consumption path. Reuses the WT adapter's installer
        # script (installs picoquic + libmoq to a temp prefix, then compiles the
        # consumer against the installed headers/libs; compile + link + run, no
        # network). Gated on a picoquic source tree (the script installs it) and
        # pkg-config being present. Skipped in mvfst/proxygen builds: no
        # libmoq-service.pc is installed there (the static link chain pulls the
        # folly/fizz stack, which pkg-config cannot express -- see the .pc block).
        find_program(PKG_CONFIG_EXECUTABLE pkg-config)
        if(MOQ_PICOQUIC_SOURCE_DIR AND PKG_CONFIG_EXECUTABLE
           AND NOT MOQ_BUILD_ADAPTER_MVFST AND NOT MOQ_BUILD_ADAPTER_PROXYGEN)
            set(_svc_openssl_root "")
            if(OPENSSL_SSL_LIBRARY)
                get_filename_component(_svc_o "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
                get_filename_component(_svc_openssl_root "${_svc_o}" DIRECTORY)
            endif()
            add_test(NAME service_pkgconfig_consumer
                COMMAND bash
                    "${CMAKE_SOURCE_DIR}/adapters/pico_wt/tests/run_pkgconfig_consumer.sh"
                    "${CMAKE_COMMAND}"
                    "libmoq-service"
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer/main.c"
                    "${PROJECT_BINARY_DIR}"
                    "${MOQ_PICOQUIC_SOURCE_DIR}"
                    "${MOQ_PICOTLS_PREFIX}"
                    "${CMAKE_C_COMPILER}"
                    "${CMAKE_C_FLAGS}"
                    "${CMAKE_EXE_LINKER_FLAGS}"
                    "${CMAKE_BUILD_TYPE}"
                    "${_svc_openssl_root}"
                    "${CMAKE_CURRENT_BINARY_DIR}/pkgconfig-consumer-work"
                    "${CMAKE_INSTALL_LIBDIR}")
            set_tests_properties(service_pkgconfig_consumer
                PROPERTIES TIMEOUT 600)
        endif()
    endif()
endif()
