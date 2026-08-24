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
if(MOQ_BUILD_MSQUIC_MANAGED)
    # RAW_QUIC over MsQuic (exact-version, lane pump API; see src/endpoint.c).
    # Opt-in only: reached by an explicit backend = MSQUIC, never by AUTO --
    # picoquic stays the default/preferred RAW_QUIC facade. CMake-only: MsQuic
    # ships its own CMake package (msquic::msquic) but no libmoq pkg-config, so
    # this backend is NOT advertised in libmoq-service.pc (see below).
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_MSQUIC_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-msquic-managed)
endif()
if(MOQ_BUILD_ADAPTER_PROXYGEN)
    # WebTransport over proxygen (multi-version negotiation; see src/endpoint.c).
    # The managed C facade is moq::adapter-proxygen-wt-managed. CMake-only:
    # proxygen/folly/fizz ship no pkg-config, so this backend is NOT advertised
    # in libmoq-service.pc (see below).
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-proxygen-wt-managed)
endif()
if(MOQ_BUILD_WTQUIC_NETWORK_MANAGED)
    # WebTransport over wtquic's Network.framework backend (real multi-version
    # negotiation, lane pump API; Apple/client-only -- the adapter option is a
    # FATAL error off Apple, so this gate never fires elsewhere). Opt-in only:
    # reached by an explicit backend = WTQUIC_NETWORK, never by AUTO --
    # pico_wt stays the default/preferred WEBTRANSPORT facade. Advertised in
    # libmoq-service.pc via libmoq-wtquic-network-managed (wtquic ships
    # pkg-config, unlike the C++ stacks).
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-wtquic-network-managed)
endif()
if(MOQ_BUILD_WTQUIC_MSQUIC_MANAGED)
    # WebTransport over wtquic's MsQuic backend (real multi-version negotiation,
    # lane pump API; cross-platform, client-only). Opt-in only: reached by an
    # explicit backend = WTQUIC_MSQUIC, never by AUTO -- pico_wt stays the
    # default/preferred WEBTRANSPORT facade. Advertised in libmoq-service.pc via
    # libmoq-wtquic-msquic-managed (wtquic ships pkg-config, unlike the C++
    # stacks; its wtquic-msquic dependency carries the MsQuic search path).
    list(APPEND _MOQ_SERVICE_FACADE_DEFS MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED=1)
    list(APPEND _MOQ_SERVICE_FACADE_LIBS moq::adapter-wtquic-msquic-managed)
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
# picoquic-only service.) MsQuic is the same story: msquic::msquic is a CMake
# package with no pkg-config, so an MsQuic-enabled service is CMake-only too.
if(NOT MOQ_BUILD_ADAPTER_MVFST AND NOT MOQ_BUILD_ADAPTER_PROXYGEN
   AND NOT MOQ_BUILD_MSQUIC_MANAGED)
    set(LIBMOQ_SERVICE_PC_REQUIRES "libmoq")
    if(MOQ_BUILD_PICO_WT_MANAGED)
        string(APPEND LIBMOQ_SERVICE_PC_REQUIRES " libmoq-pico-wt-managed")
    endif()
    if(MOQ_BUILD_WTQUIC_NETWORK_MANAGED)
        # wtquic ships pkg-config (wtquic-network), so an NF-enabled service
        # stays pkg-config-linkable -- unlike the CMake-only C++ backends.
        string(APPEND LIBMOQ_SERVICE_PC_REQUIRES " libmoq-wtquic-network-managed")
    endif()
    if(MOQ_BUILD_WTQUIC_MSQUIC_MANAGED)
        # wtquic ships pkg-config (wtquic-msquic), so an MsQuic-WebTransport
        # service stays pkg-config-linkable -- the wtquic-msquic dependency
        # carries the MsQuic search path, unlike the CMake-only msquic::msquic
        # package the DIRECT MsQuic backend pulls in.
        string(APPEND LIBMOQ_SERVICE_PC_REQUIRES " libmoq-wtquic-msquic-managed")
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

    # Handshake-bound propagation (picoquic only): prove the resolved value
    # reaches the QUIC context through the real connect path. endpoint.c is
    # recompiled with the picoquic setter renamed to a recorder the test
    # defines, so the seam exists only in this binary -- the shipping library
    # never sees it. Own object set for exactly that reason.
    if(MOQ_BUILD_PQ_THREADED OR MOQ_BUILD_PICO_WT_MANAGED)
        add_library(moq-service-hs-test-internals OBJECT src/endpoint.c)
        target_include_directories(moq-service-hs-test-internals
            PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(moq-service-hs-test-internals
            PUBLIC moq::core Threads::Threads ${_MOQ_SERVICE_FACADE_LIBS})
        target_compile_definitions(moq-service-hs-test-internals PRIVATE
            ${_MOQ_SERVICE_FACADE_DEFS}
            picoquic_set_default_handshake_timeout=moq_test_set_default_handshake_timeout)
        target_compile_features(moq-service-hs-test-internals PUBLIC c_std_11)

        add_executable(test_endpoint_handshake_timeout
            tests/test_endpoint_handshake_timeout.c)
        target_link_libraries(test_endpoint_handshake_timeout PRIVATE
            moq-service-hs-test-internals moq::core Threads::Threads)
        target_compile_definitions(test_endpoint_handshake_timeout PRIVATE
            ${_MOQ_SERVICE_FACADE_DEFS})
        target_include_directories(test_endpoint_handshake_timeout PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME endpoint_handshake_timeout
                 COMMAND test_endpoint_handshake_timeout)
    endif()

    # Service picoquic keepalive default (Vojtech idle-publisher report): prove
    # the endpoint's raw-threaded client carries keep_alive_interval_ms = 15000
    # into moq_pq_threaded_create(). endpoint.c is recompiled with the adapter
    # create renamed to a recorder the test defines, so the seam exists only in
    # this binary -- the shipping library never sees it. Own object set for
    # exactly that reason.
    if(MOQ_BUILD_PQ_THREADED)
        add_library(moq-service-ka-test-internals OBJECT src/endpoint.c)
        target_include_directories(moq-service-ka-test-internals
            PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(moq-service-ka-test-internals
            PUBLIC moq::core Threads::Threads ${_MOQ_SERVICE_FACADE_LIBS})
        target_compile_definitions(moq-service-ka-test-internals PRIVATE
            ${_MOQ_SERVICE_FACADE_DEFS}
            moq_pq_threaded_create=moq_test_pq_threaded_create)
        target_compile_features(moq-service-ka-test-internals PUBLIC c_std_11)

        add_executable(test_endpoint_keepalive_default
            tests/test_endpoint_keepalive_default.c)
        target_link_libraries(test_endpoint_keepalive_default PRIVATE
            moq-service-ka-test-internals moq::core Threads::Threads
            ${_MOQ_SERVICE_FACADE_LIBS})
        target_compile_definitions(test_endpoint_keepalive_default PRIVATE
            ${_MOQ_SERVICE_FACADE_DEFS})
        target_include_directories(test_endpoint_keepalive_default PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME endpoint_keepalive_default
                 COMMAND test_endpoint_keepalive_default)
    endif()

    # White-box post() NULL-session contract test (no network, no msf): endpoint.c
    # with the bare-endpoint + pump seam so ep_pump_cycle can be driven with a
    # controlled session (NULL pre-establishment, then a live IDLE session). Own
    # object set so the testing define never leaks into the shipping library.
    add_library(moq-service-endpoint-test-internals OBJECT src/endpoint.c)
    target_include_directories(moq-service-endpoint-test-internals
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(moq-service-endpoint-test-internals
        PUBLIC moq::core Threads::Threads ${_MOQ_SERVICE_FACADE_LIBS})
    target_compile_definitions(moq-service-endpoint-test-internals
        PRIVATE MOQ_MEDIA_SENDER_TESTING=1 ${_MOQ_SERVICE_FACADE_DEFS})
    target_compile_features(moq-service-endpoint-test-internals PUBLIC c_std_11)

    add_executable(test_endpoint_post_contract tests/test_endpoint_post_contract.c)
    target_link_libraries(test_endpoint_post_contract PRIVATE
        moq-service-endpoint-test-internals moq::core Threads::Threads)
    target_include_directories(test_endpoint_post_contract PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/unit)
    add_test(NAME endpoint_post_contract COMMAND test_endpoint_post_contract)

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

        # Publication-race RED (MOQ-MEDIA-CP-LOOKUP-RACE): deterministic
        # try-lock phase oracle for the effective-catalog publication vs the
        # public content-protection lookup, plus a TSan stress companion.
        add_executable(test_media_catalog_publication_race
            tests/test_media_catalog_publication_race.c)
        target_link_libraries(test_media_catalog_publication_race PRIVATE
            moq-service-receiver-test-internals moq::core moq::msf
            Threads::Threads)
        target_include_directories(test_media_catalog_publication_race PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_catalog_publication_race
            COMMAND test_media_catalog_publication_race)

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
            add_executable(test_media_receiver_update_ack
                tests/test_media_receiver_update_ack.c)
            target_link_libraries(test_media_receiver_update_ack PRIVATE
                moq-service-receiver-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_receiver_update_ack PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_receiver_update_ack
                COMMAND test_media_receiver_update_ack)

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
        # Pacing attribution: one production sender_hook == one drain pass,
        # driven against a real SimPair subscriber. Deterministic pump units.
        add_executable(test_media_sender_pacing
            tests/test_media_sender_pacing.c)
        target_link_libraries(test_media_sender_pacing PRIVATE
            moq-service-sender-test-internals moq::core moq::msf
            moq::sim Threads::Threads)
        target_include_directories(test_media_sender_pacing PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_sender_pacing
            COMMAND test_media_sender_pacing)

        add_executable(test_media_sender_reject
            tests/test_media_sender_reject.c)
        target_link_libraries(test_media_sender_reject PRIVATE
            moq-service-sender-test-internals moq::core moq::msf
            moq::sim Threads::Threads)
        target_include_directories(test_media_sender_reject PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_sender_reject
            COMMAND test_media_sender_reject)

        # Automatic catalog-refresh test. Needs BOTH the sender and
        # receiver seams (a real sender publishes periodic catalogs; a real
        # receiver dedups them), so it links a combined internals object set
        # compiling media_sender.c + media_receiver.c + endpoint.c ONCE with
        # both testing defines -- linking the two separate internals libs would
        # duplicate endpoint.c. Requires moq::msf and moq::sim.
        if(TARGET moq::sim)
            add_library(moq-service-refresh-test-internals OBJECT
                src/media_sender.c src/media_receiver.c src/endpoint.c)
            target_include_directories(moq-service-refresh-test-internals
                PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
                PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
            target_link_libraries(moq-service-refresh-test-internals
                PUBLIC moq::core moq::msf Threads::Threads)
            target_compile_definitions(moq-service-refresh-test-internals
                PRIVATE MOQ_MEDIA_SENDER_TESTING=1 MOQ_MEDIA_RECEIVER_TESTING=1
                ${_MOQ_SERVICE_FACADE_DEFS})
            target_link_libraries(moq-service-refresh-test-internals
                PUBLIC ${_MOQ_SERVICE_FACADE_LIBS})
            target_compile_features(moq-service-refresh-test-internals
                PUBLIC c_std_11)

            add_executable(test_media_sender_refresh
                tests/test_media_sender_refresh.c)
            target_link_libraries(test_media_sender_refresh PRIVATE
                moq-service-refresh-test-internals moq::core moq::msf
                moq::sim Threads::Threads)
            target_include_directories(test_media_sender_refresh PRIVATE
                ${CMAKE_SOURCE_DIR}/tests/unit)
            add_test(NAME media_sender_refresh
                COMMAND test_media_sender_refresh)
        endif()

        # C++17 public-header consumer: the service headers must compile,
        # link, and run from plain C++ (extern "C" surface only). Init-only,
        # no network -- the C++ mirror of tests/consumer/main.c. Skipped when
        # no C++ compiler is available.
        include(CheckLanguage)
        check_language(CXX)
        if(CMAKE_CXX_COMPILER)
            enable_language(CXX)
            add_executable(test_service_consumer_cxx
                tests/consumer/main_cxx.cpp)
            target_link_libraries(test_service_consumer_cxx PRIVATE
                moq::service)
            set_target_properties(test_service_consumer_cxx PROPERTIES
                CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
            if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_service_consumer_cxx PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME service_consumer_cxx
                COMMAND test_service_consumer_cxx)
        endif()
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

        # Idle managed-loopback discriminator for automatic catalog refresh:
        # a media_sender over a REAL pq_threaded endpoint whose catalog group
        # advances while idle only if the adapter wakes itself at the refresh
        # deadline. Public API only (shipping moq::service). Network.
        add_executable(test_media_sender_idle_refresh
            tests/test_media_sender_idle_refresh.c)
        target_link_libraries(test_media_sender_idle_refresh PRIVATE
            moq::service moq::adapter-picoquic-threaded moq::msf
            Threads::Threads)
        target_include_directories(test_media_sender_idle_refresh PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_sender_idle_refresh
            COMMAND test_media_sender_idle_refresh
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(media_sender_idle_refresh PROPERTIES
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
        # Dedicated internals-linked cursor test: the ONLY network binary on
        # the MOQ_MEDIA_SENDER_TESTING rail (the production suite above links
        # the shipping moq::service). Narrowly scoped: one push-mode consumer,
        # one forced mid-generation WOULD_BLOCK.
        add_executable(test_media_sender_push_cursor
            tests/test_media_sender_push_cursor.c)
        target_link_libraries(test_media_sender_push_cursor PRIVATE
            moq-service-sender-test-internals
            moq::adapter-picoquic-threaded moq::msf
            Threads::Threads)
        target_include_directories(test_media_sender_push_cursor PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        add_test(NAME media_sender_push_cursor
            COMMAND test_media_sender_push_cursor
                --cert "${MOQ_PICOQUIC_SOURCE_DIR}/certs/cert.pem"
                --key "${MOQ_PICOQUIC_SOURCE_DIR}/certs/key.pem")
        set_tests_properties(media_sender_push_cursor PROPERTIES
            LABELS "network" TIMEOUT 60 RESOURCE_LOCK service_network)

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
           AND NOT MOQ_BUILD_ADAPTER_MVFST AND NOT MOQ_BUILD_ADAPTER_PROXYGEN
           AND NOT MOQ_BUILD_MSQUIC_MANAGED)
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

    # Explicit MsQuic backend selection over a real MsQuic loopback: the
    # endpoint client (backend = MSQUIC) against a managed MsQuic server peer,
    # one SUBSCRIBE + object + clean close. Self-signed certs into the build
    # dir (its own fixture -- the adapter's msquic_gen_certs lives in a
    # different tree). Port 0 (ephemeral) on both sides, so no port lock.
    if(MOQ_BUILD_MSQUIC_MANAGED)
        add_executable(test_endpoint_msquic_smoke
            tests/test_endpoint_msquic_smoke.c)
        target_link_libraries(test_endpoint_msquic_smoke PRIVATE
            moq::service moq::adapter-msquic-managed Threads::Threads)
        target_include_directories(test_endpoint_msquic_smoke PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        find_program(MOQ_OPENSSL openssl)
        if(MOQ_OPENSSL)
            add_test(NAME service_msquic_gen_certs
                COMMAND ${MOQ_OPENSSL} req -x509 -newkey rsa:2048
                    -sha256 -days 30 -nodes
                    -keyout ${CMAKE_CURRENT_BINARY_DIR}/msquic_key.pem
                    -out ${CMAKE_CURRENT_BINARY_DIR}/msquic_cert.pem
                    -subj /CN=localhost)
            set_tests_properties(service_msquic_gen_certs PROPERTIES
                FIXTURES_SETUP service_msquic_certs
                LABELS "network;msquic")
            add_test(NAME endpoint_msquic_smoke
                COMMAND test_endpoint_msquic_smoke
                    --cert ${CMAKE_CURRENT_BINARY_DIR}/msquic_cert.pem
                    --key ${CMAKE_CURRENT_BINARY_DIR}/msquic_key.pem)
            set_tests_properties(endpoint_msquic_smoke PROPERTIES
                FIXTURES_REQUIRED service_msquic_certs
                LABELS "network;msquic" TIMEOUT 120)
        endif()
    endif()

    # Explicit wtquic-over-MsQuic backend selection over a real WT loopback: the
    # endpoint client (backend = WTQUIC_MSQUIC) against the same facade in server
    # mode, one SUBSCRIBE + object + clean close, run once per draft with real
    # WT-Protocol negotiation. Cross-platform (wherever wtquic's MsQuic component
    # builds). Self-signed certs into the build dir; port 0 (ephemeral).
    if(MOQ_BUILD_WTQUIC_MSQUIC_MANAGED)
        add_executable(test_endpoint_wtquic_msquic_smoke
            tests/test_endpoint_wtquic_msquic_smoke.c)
        target_link_libraries(test_endpoint_wtquic_msquic_smoke PRIVATE
            moq::service moq::adapter-wtquic-msquic-managed
            moq::adapter-wtquic wtq::msquic Threads::Threads)
        target_include_directories(test_endpoint_wtquic_msquic_smoke PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        find_program(MOQ_OPENSSL openssl)
        if(MOQ_OPENSSL)
            add_test(NAME service_wtquic_msquic_gen_certs
                COMMAND ${MOQ_OPENSSL} req -x509 -newkey rsa:2048
                    -sha256 -days 30 -nodes
                    -keyout ${CMAKE_CURRENT_BINARY_DIR}/wtquic_msquic_key.pem
                    -out ${CMAKE_CURRENT_BINARY_DIR}/wtquic_msquic_cert.pem
                    -subj /CN=localhost)
            set_tests_properties(service_wtquic_msquic_gen_certs PROPERTIES
                FIXTURES_SETUP service_wtquic_msquic_certs
                LABELS "network")
            add_test(NAME endpoint_wtquic_msquic_smoke
                COMMAND test_endpoint_wtquic_msquic_smoke
                    --cert ${CMAKE_CURRENT_BINARY_DIR}/wtquic_msquic_cert.pem
                    --key ${CMAKE_CURRENT_BINARY_DIR}/wtquic_msquic_key.pem)
            set_tests_properties(endpoint_wtquic_msquic_smoke PROPERTIES
                FIXTURES_REQUIRED service_wtquic_msquic_certs
                LABELS "network" TIMEOUT 120)
        endif()

        # Installed packaging: a relocated-prefix consumer that requests ONLY
        # COMPONENTS service (and, when the .pc is generated, the pkg-config
        # chain libmoq-service -> libmoq-wtquic-msquic-managed -> wtquic-msquic).
        # No injected -L: the wtquic-msquic dependency carries the MsQuic search
        # path. The .pc exists unless a CMake-only backend (mvfst/proxygen/direct
        # MsQuic) was also built.
        # This consumer pins that an installed picoquic is discovered FROM THE
        # PREFIX, and its negative control requires that dropping picoquic makes
        # the consumer fail to configure. Both only hold when the service build
        # actually pulls a picoquic-based facade (pico_wt or picoquic-threaded);
        # a wtquic-only service legitimately needs no picoquic, and the control
        # would then fire on a correct build. It also needs the caller-supplied
        # hints that materialize the installed picoquic. Register only when all
        # of that is true — an abort is not a result. A tree wanting this
        # coverage builds pico_wt/pq-threaded and passes
        # -DMOQ_PICOQUIC_SOURCE_DIR=... -DMOQ_PICOTLS_PREFIX=...
        if(wtquic_DIR AND MOQ_PICOQUIC_SOURCE_DIR AND MOQ_PICOTLS_PREFIX
           AND (MOQ_BUILD_PICO_WT_MANAGED OR MOQ_BUILD_PQ_THREADED))
            get_filename_component(_svc_wtqm_prefix "${wtquic_DIR}" DIRECTORY)
            get_filename_component(_svc_wtqm_prefix "${_svc_wtqm_prefix}" DIRECTORY)
            get_filename_component(_svc_wtqm_prefix "${_svc_wtqm_prefix}" DIRECTORY)
            set(_svc_wtqm_do_pc OFF)
            if(NOT MOQ_BUILD_ADAPTER_MVFST AND NOT MOQ_BUILD_ADAPTER_PROXYGEN
               AND NOT MOQ_BUILD_MSQUIC_MANAGED)
                set(_svc_wtqm_do_pc ON)
            endif()
            # openssl root so the materialized picoquic (and the relocated
            # consumer link) find the same OpenSSL the build used.
            set(_svc_wtqm_ossl "")
            if(OPENSSL_SSL_LIBRARY)
                get_filename_component(_svc_wtqm_o "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
                get_filename_component(_svc_wtqm_ossl "${_svc_wtqm_o}" DIRECTORY)
            endif()
            add_test(NAME service_wtquic_msquic_install_consumer
                COMMAND ${CMAKE_COMMAND}
                    -DBUILD=${PROJECT_BINARY_DIR}
                    -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
                    -DWORK=${CMAKE_CURRENT_BINARY_DIR}/wtquic-msquic-consumer-work
                    "-DWTQUIC_PREFIX=${_svc_wtqm_prefix}"
                    "-DMSQUIC_DIR_HINT=${msquic_DIR}"
                    "-DPICOQUIC_SOURCE_DIR=${MOQ_PICOQUIC_SOURCE_DIR}"
                    "-DPICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}"
                    "-DOPENSSL_ROOT=${_svc_wtqm_ossl}"
                    "-DBUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    -DC_COMPILER=${CMAKE_C_COMPILER}
                    -DDO_PKGCONFIG=${_svc_wtqm_do_pc}
                    -DLIBDIR=${CMAKE_INSTALL_LIBDIR}
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/wtquic_msquic_install_consumer.cmake)
            set_tests_properties(service_wtquic_msquic_install_consumer PROPERTIES
                TIMEOUT 900)
        elseif(wtquic_DIR)
            message(STATUS
                "service: skipping service_wtquic_msquic_install_consumer "
                "(needs a picoquic-based facade — pico_wt or picoquic-threaded — "
                "plus -DMOQ_PICOQUIC_SOURCE_DIR and -DMOQ_PICOTLS_PREFIX, so the "
                "installed picoquic it pins is genuinely a dependency)")
        endif()
    endif()

    # Explicit wtquic Network.framework backend selection over a real WT
    # loopback: the endpoint client (backend = WTQUIC_NETWORK) against a
    # wtquic MsQuic attach server peer, one SUBSCRIBE + object + clean close
    # with real multi-version negotiation. Apple-only (the facade). Certs are
    # this test's own fixture; port 0 (ephemeral), no port lock.
    if(MOQ_BUILD_WTQUIC_NETWORK_MANAGED)
        add_executable(test_endpoint_wtquic_network_smoke
            tests/test_endpoint_wtquic_network_smoke.c)
        target_link_libraries(test_endpoint_wtquic_network_smoke PRIVATE
            moq::service moq::adapter-wtquic-network-managed
            moq::adapter-wtquic wtq::msquic Threads::Threads)
        target_include_directories(test_endpoint_wtquic_network_smoke PRIVATE
            ${CMAKE_SOURCE_DIR}/tests/unit)
        find_program(MOQ_OPENSSL openssl)
        if(MOQ_OPENSSL)
            add_test(NAME service_wtquic_network_gen_certs
                COMMAND ${MOQ_OPENSSL} req -x509 -newkey rsa:2048
                    -sha256 -days 30 -nodes
                    -keyout ${CMAKE_CURRENT_BINARY_DIR}/wtquic_network_key.pem
                    -out ${CMAKE_CURRENT_BINARY_DIR}/wtquic_network_cert.pem
                    -subj /CN=localhost)
            set_tests_properties(service_wtquic_network_gen_certs PROPERTIES
                FIXTURES_SETUP service_wtquic_network_certs
                LABELS "network")
            add_test(NAME endpoint_wtquic_network_smoke
                COMMAND test_endpoint_wtquic_network_smoke
                    --cert ${CMAKE_CURRENT_BINARY_DIR}/wtquic_network_cert.pem
                    --key ${CMAKE_CURRENT_BINARY_DIR}/wtquic_network_key.pem)
            set_tests_properties(endpoint_wtquic_network_smoke PROPERTIES
                FIXTURES_REQUIRED service_wtquic_network_certs
                LABELS "network" TIMEOUT 120)
        endif()

        # One-shot WT smoke server for the .wtquicNetwork runtime proof
        # (test support only, never installed). It is a wtquic MsQuic
        # ATTACH peer, so it is gated on the msquic component being present
        # in the wtquic package (a network-only wtquic -- e.g. the iOS
        # build -- has no wtq::msquic and simply skips this peer). The
        # runtime proof itself is a STANDALONE Swift executable
        # (bindings/swift/Examples/WtquicNetworkRuntimeProof), driven by
        # scripts/check_wtquic_network_runtime.sh: Network.framework does not
        # establish connections inside an xctest host, so the proof must
        # run as a normal process, not a unit test.
        if(TARGET wtq::msquic)
            add_executable(wtquic_network_smoke_server tests/wtquic_network_smoke_server.c)
            # the publisher facade lives in moq::core; moq::loc encodes
            # the object properties; moq::msf only lends its header (the
            # catalog track-name constant)
            target_link_libraries(wtquic_network_smoke_server PRIVATE
                moq::adapter-wtquic moq::loc wtq::msquic Threads::Threads)
            target_include_directories(wtquic_network_smoke_server PRIVATE
                ${PROJECT_SOURCE_DIR}/media/msf/include)
        endif()

        # NF-only installed packaging: CMake COMPONENTS service consumer
        # against a relocated prefix, plus (when the .pc is generated)
        # the pkg-config chain libmoq-service ->
        # libmoq-wtquic-network-managed -> wtquic-network.
        if(wtquic_DIR)
            get_filename_component(_svc_wtq_prefix "${wtquic_DIR}" DIRECTORY)
            get_filename_component(_svc_wtq_prefix "${_svc_wtq_prefix}" DIRECTORY)
            get_filename_component(_svc_wtq_prefix "${_svc_wtq_prefix}" DIRECTORY)
            set(_svc_wtq_do_pc OFF)
            if(NOT MOQ_BUILD_ADAPTER_MVFST AND NOT MOQ_BUILD_ADAPTER_PROXYGEN
               AND NOT MOQ_BUILD_MSQUIC_MANAGED)
                set(_svc_wtq_do_pc ON)
            endif()
            set(_svc_wtq_ossl "")
            if(OPENSSL_SSL_LIBRARY)
                get_filename_component(_svc_wtq_o "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
                get_filename_component(_svc_wtq_ossl "${_svc_wtq_o}" DIRECTORY)
            endif()
            add_test(NAME service_wtquic_network_install_consumer
                COMMAND ${CMAKE_COMMAND}
                    -DBUILD=${PROJECT_BINARY_DIR}
                    -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
                    -DWORK=${CMAKE_CURRENT_BINARY_DIR}/wtquic-network-consumer-work
                    "-DWTQUIC_PREFIX=${_svc_wtq_prefix}"
                    "-DMSQUIC_DIR_HINT=${msquic_DIR}"
                    "-DPICOQUIC_SOURCE_DIR=${MOQ_PICOQUIC_SOURCE_DIR}"
                    "-DPICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}"
                    "-DOPENSSL_ROOT=${_svc_wtq_ossl}"
                    "-DBUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    -DC_COMPILER=${CMAKE_C_COMPILER}
                    -DDO_PKGCONFIG=${_svc_wtq_do_pc}
                    -DLIBDIR=${CMAKE_INSTALL_LIBDIR}
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/wtquic_network_install_consumer.cmake)
            set_tests_properties(service_wtquic_network_install_consumer PROPERTIES
                TIMEOUT 900)
        endif()
    endif()
endif()
