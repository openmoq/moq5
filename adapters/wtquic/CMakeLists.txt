include("${PROJECT_SOURCE_DIR}/cmake/MoqComponentExports.cmake")

# MoQ over wtquic (WebTransport over HTTP/3). The attach adapter is
# BACKEND-NEUTRAL: its production dependency is the wtquic core
# (wtq::wtquic), not any one transport backend. MsQuic is just one wtquic
# backend and is required only by particular real-transport tests below.

add_library(moq-adapter-wtquic
    wtquic_adapter.c
)
add_library(moq::adapter-wtquic ALIAS moq-adapter-wtquic)

target_include_directories(moq-adapter-wtquic PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
# BACKEND-NEUTRAL: the adapter header names only core wtquic types
# (wtq_session_t, wtq_session_events_t), never a transport backend —
# wtq::wtquic is the public dependency. Tests that drive a REAL
# transport link that backend (wtq::msquic) explicitly themselves.
target_link_libraries(moq-adapter-wtquic
    PUBLIC moq::core wtq::wtquic
)
target_compile_definitions(moq-adapter-wtquic PRIVATE MOQ_BUILDING)
set_target_properties(moq-adapter-wtquic PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON
)
if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(moq-adapter-wtquic PRIVATE
        -Wall -Wextra -Wpedantic
        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Native build/install exports retain the optional component boundary.
moq_export_component(moq-adapter-wtquic adapter-wtquic libmoqWtquicAdapterTargets)

# -- Install / export --------------------------------------------------
include(GNUInstallDirs)


install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

# -- pkg-config ---------------------------------------------------------
# Unlike the picoquic-family adapters, the transport dependency is
# expressed via Requires: wtquic — the BACKEND-NEUTRAL core (wtquic
# installs real .pc files); a consumer additionally links whichever
# backend .pc it uses. CMake remains the fully supported consumer path.
string(REPLACE "/" ";" _wtquic_libdir_parts "${CMAKE_INSTALL_LIBDIR}")
list(LENGTH _wtquic_libdir_parts _wtquic_libdir_depth)
math(EXPR _wtquic_pc_ascents "${_wtquic_libdir_depth} + 1")
set(LIBMOQ_WTQUIC_PC_UP "..")
foreach(_wtquic_i RANGE 2 ${_wtquic_pc_ascents})
    string(APPEND LIBMOQ_WTQUIC_PC_UP "/..")
endforeach()
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoq-wtquic.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoq-wtquic.pc
    @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoq-wtquic.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

# -- loopback smoke (real MsQuic on localhost, self-signed certs) -----

# -- Managed Network.framework client (Apple-only, optional) -----------
# moq::adapter-wtquic-network-managed: the facade owning MoQ session +
# attach adapter + wtq_nw_conn, with REAL WT-Protocol negotiation.
if(MOQ_BUILD_WTQUIC_NETWORK_MANAGED)
    if(NOT APPLE)
        message(FATAL_ERROR
            "MOQ_BUILD_WTQUIC_NETWORK_MANAGED requires an Apple platform")
    endif()
    if(NOT TARGET wtq::network)
        message(FATAL_ERROR
            "MOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON but wtquic's `network` "
            "component was not found (build/install wtquic with "
            "-DWTQ_BUILD_NETWORK=ON).")
    endif()
    find_package(Threads REQUIRED) # the facade itself uses pthreads;
                                   # tests-off builds must find it too
    add_library(moq-adapter-wtquic-network-managed
        wtquic_network_managed.c
    )
    add_library(moq::adapter-wtquic-network-managed ALIAS
        moq-adapter-wtquic-network-managed)
    target_include_directories(moq-adapter-wtquic-network-managed PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_include_directories(moq-adapter-wtquic-network-managed PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
    )
    target_link_libraries(moq-adapter-wtquic-network-managed
        PUBLIC moq::adapter-wtquic wtq::network
        PRIVATE Threads::Threads
    )
    target_compile_definitions(moq-adapter-wtquic-network-managed
        PRIVATE MOQ_BUILDING)
    set_target_properties(moq-adapter-wtquic-network-managed PROPERTIES
        C_VISIBILITY_PRESET hidden
        POSITION_INDEPENDENT_CODE ON
    )
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(moq-adapter-wtquic-network-managed PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    moq_export_component(moq-adapter-wtquic-network-managed
        adapter-wtquic-network-managed libmoqWtquicNetworkManagedTargets)

    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoq-wtquic-network-managed.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoq-wtquic-network-managed.pc @ONLY)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/libmoq-wtquic-network-managed.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endif()

# moq::adapter-wtquic-msquic-managed: the managed facade owning the MoQ
# session + attach adapter over wtquic's MsQuic backend, with its own teardown
# coordinator and lane model. Needs wtquic's `msquic` component.
if(MOQ_BUILD_WTQUIC_MSQUIC_MANAGED)
    if(NOT TARGET wtq::msquic)
        message(FATAL_ERROR
            "MOQ_BUILD_WTQUIC_MSQUIC_MANAGED=ON but wtquic's `msquic` "
            "component was not found (build/install wtquic with its msquic "
            "backend).")
    endif()
    # The facade forwards max_local_streams to the provider's client/listener
    # configs: refuse an older MsQuic header, or a backend/core missing a
    # probed entry point, here, with the same contract message installed
    # consumers get from the package preflight.
    moq_check_wtquic_msquic_api(_moq_wt_msquic_api_ok _moq_wt_msquic_api_reason)
    if(NOT _moq_wt_msquic_api_ok)
        message(FATAL_ERROR "${_moq_wt_msquic_api_reason}")
    endif()
    find_package(Threads REQUIRED) # the facade owns a coordinator thread
    add_library(moq-adapter-wtquic-msquic-managed
        wtquic_msquic_managed.c
    )
    add_library(moq::adapter-wtquic-msquic-managed ALIAS
        moq-adapter-wtquic-msquic-managed)
    target_include_directories(moq-adapter-wtquic-msquic-managed PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_include_directories(moq-adapter-wtquic-msquic-managed PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
    )
    target_link_libraries(moq-adapter-wtquic-msquic-managed
        PUBLIC moq::adapter-wtquic wtq::msquic
        PRIVATE Threads::Threads
    )
    target_compile_definitions(moq-adapter-wtquic-msquic-managed
        PRIVATE MOQ_BUILDING)
    set_target_properties(moq-adapter-wtquic-msquic-managed PROPERTIES
        C_VISIBILITY_PRESET hidden
        POSITION_INDEPENDENT_CODE ON
    )
    # A shared facade exports only its own moq_wtquic_msquic_* API. When
    # wtquic's MsQuic backend is a static archive, its WTQ_API functions are
    # linked into this image and would otherwise be re-exported from here;
    # callers that use wtquic directly link wtq::msquic themselves (it is a
    # PUBLIC dependency). Pinned by wtquic_msquic_managed_symbol_policy.
    get_target_property(_wtq_mm_type moq-adapter-wtquic-msquic-managed TYPE)
    if(_wtq_mm_type STREQUAL "SHARED_LIBRARY")
        if(APPLE)
            target_link_options(moq-adapter-wtquic-msquic-managed PRIVATE
                "LINKER:-exported_symbol,_moq_wtquic_msquic_*")
        elseif(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_link_options(moq-adapter-wtquic-msquic-managed PRIVATE
                "LINKER:--exclude-libs,ALL")
        endif()
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(moq-adapter-wtquic-msquic-managed PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    moq_export_component(moq-adapter-wtquic-msquic-managed
        adapter-wtquic-msquic-managed libmoqWtquicMsquicManagedTargets)

    # MsQuic's library dir is carried by wtquic-msquic.pc (Requires:), so this
    # package's .pc needs no MsQuic path of its own.
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoq-wtquic-msquic-managed.pc.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoq-wtquic-msquic-managed.pc @ONLY)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/libmoq-wtquic-msquic-managed.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endif()

if(MOQ_BUILD_TESTS)
    find_package(Threads REQUIRED)

    # Public-header compile/link smokes: <moq/wtquic.h> stands alone,
    # in C and C++, against the build-tree target.
    add_executable(test_wtquic_public_compile
        tests/test_wtquic_public_compile.c)
    target_link_libraries(test_wtquic_public_compile PRIVATE
        moq::adapter-wtquic)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_public_compile PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_public_compile COMMAND test_wtquic_public_compile)

    include(CheckLanguage)
    check_language(CXX)
    if(CMAKE_CXX_COMPILER)
        enable_language(CXX)
        add_executable(test_wtquic_public_compile_cxx
            tests/test_wtquic_public_compile.cpp)
        target_link_libraries(test_wtquic_public_compile_cxx PRIVATE
            moq::adapter-wtquic)
        set_target_properties(test_wtquic_public_compile_cxx PROPERTIES
            CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
        add_test(NAME wtquic_public_compile_cxx
            COMMAND test_wtquic_public_compile_cxx)
    endif()

    # <moq/wtquic_msquic_managed.h> public-declaration compile coverage: OBJECT
    # compiles that signature-check every declaration under pedantic C11/C++17
    # (not linked or run). These build even when the managed facade is OFF, so
    # the header stays self-contained regardless; they build against
    # moq::adapter-wtquic purely for the header search paths (moq core + wtquic
    # + this adapter's include/). Runtime coverage is the linked tests below.
    add_library(test_wtquic_msquic_managed_public_compile OBJECT
        tests/test_wtquic_msquic_managed_public_compile.c)
    target_link_libraries(test_wtquic_msquic_managed_public_compile PRIVATE
        moq::adapter-wtquic)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_msquic_managed_public_compile PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    if(CMAKE_CXX_COMPILER)
        add_library(test_wtquic_msquic_managed_public_compile_cxx OBJECT
            tests/test_wtquic_msquic_managed_public_compile.cpp)
        target_link_libraries(test_wtquic_msquic_managed_public_compile_cxx
            PRIVATE moq::adapter-wtquic)
        set_target_properties(test_wtquic_msquic_managed_public_compile_cxx
            PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(
                test_wtquic_msquic_managed_public_compile_cxx PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
    endif()

    # -- Origin authorization gate ---------------------------------------
    #
    # The orchestration -- the frozen row table, the observation recorders
    # and the verdict -- is transport-free by construction, so it builds
    # and runs here with no provider, no socket and no certificate. This is
    # the test that proves the RULES; the native fixture below only wires a
    # transport to them.
    add_executable(test_wtquic_origin_rows
        tests/test_wtquic_origin_rows.c tests/origin_rows.c)
    target_include_directories(test_wtquic_origin_rows PRIVATE tests)
    # origin_rows.c uses pthreads; Darwin's libSystem hides the omission,
    # other link environments do not.
    target_link_libraries(test_wtquic_origin_rows PRIVATE Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_origin_rows PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_origin_rows COMMAND test_wtquic_origin_rows)

    # The DRIVER -- build, connect, wait, tear down, decide -- exercised
    # through a fake transport that can produce a callback before connect
    # returns, a reentrant callback, a late contradiction, a partial
    # construction and a call that blocks past the budget. Time is
    # injected, so nothing sleeps and nothing opens a socket.
    add_executable(test_wtquic_origin_driver
        tests/test_wtquic_origin_driver.c tests/origin_driver.c
        tests/origin_rows.c)
    target_include_directories(test_wtquic_origin_driver PRIVATE tests)
    # origin_rows.c uses pthreads; Darwin's libSystem hides the omission,
    # other link environments do not.
    target_link_libraries(test_wtquic_origin_driver PRIVATE Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_origin_driver PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_origin_driver COMMAND test_wtquic_origin_driver)

    # The parent/child report path with REAL processes and a real pipe:
    # a codec round trip proves none of what this checks.
    add_executable(test_wtquic_origin_super
        tests/test_wtquic_origin_super.c tests/origin_super.c
        tests/origin_rows.c)
    target_include_directories(test_wtquic_origin_super PRIVATE tests)
    # origin_rows.c uses pthreads; Darwin's libSystem hides the omission,
    # other link environments do not.
    target_link_libraries(test_wtquic_origin_super PRIVATE Threads::Threads)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_origin_super PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_origin_super COMMAND test_wtquic_origin_super)
    set_tests_properties(wtquic_origin_super PROPERTIES TIMEOUT 120)

    # The native gate lives inside the facade guard below, so an explicit
    # request with no facade target would otherwise generate nothing at all.
    if(MOQ_WTQUIC_ORIGIN_GATE AND NOT TARGET moq::adapter-wtquic-msquic-managed)
        message(FATAL_ERROR
            "MOQ_WTQUIC_ORIGIN_GATE=ON but the managed wtquic-MsQuic facade "
            "target was not built. It needs a wtquic prefix providing the "
            "msquic component; configure with -Dwtquic_DIR=<prefix>/lib/cmake/"
            "wtquic and -Dmsquic_DIR=<prefix>/share/msquic.")
    endif()

    # Managed wtquic-MsQuic facade: behavioral + linked C/C++ consumers. These
    # link the real library, so they build only when the facade is enabled.
    if(TARGET moq::adapter-wtquic-msquic-managed)
        add_executable(test_wtquic_msquic_managed
            tests/test_wtquic_msquic_managed.c)
        target_link_libraries(test_wtquic_msquic_managed PRIVATE
            moq::adapter-wtquic-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_msquic_managed PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_msquic_managed COMMAND test_wtquic_msquic_managed)

        # The native fixture is EXPLICITLY REQUESTED and never built by an
        # ordinary configure. It needs a wtquic providing the trusted client
        # entry: against a prefix without it the request FAILS here, loudly,
        # rather than quietly dropping a target and letting a smaller suite be
        # reported as a complete run. Ordinary old-prefix consumers are
        # unaffected while the fixture is not requested.
        if(MOQ_WTQUIC_ORIGIN_GATE)
            # Use the imported target's FULL compile and link requirements.
            # Copying only its direct include directories loses the transitive
            # ones -- msquic.h among them -- so the check fails against a
            # perfectly good prefix and proves nothing about the symbol.
            include(CheckCSourceCompiles)
            include(CMakePushCheckState)
            cmake_push_check_state(RESET)
            set(CMAKE_REQUIRED_LIBRARIES wtq::msquic)
            # A typed pointer the compiler cannot fold away: the check must
            # LINK the symbol, not merely see its declaration.
            check_c_source_compiles("
#include <wtquic/wtquic_msquic.h>
static wtq_result_t (*volatile p)(wtq_msquic_env_t *,
                                  const wtq_msquic_client_cfg_t *,
                                  const wtq_msquic_client_trust_cfg_t *,
                                  wtq_session_t **) =
    wtq_msquic_client_connect_with_trust;
int main(void) { return p == 0; }
" MOQ_WTQUIC_HAVE_TRUSTED_CLIENT)
            cmake_pop_check_state()
            if(NOT MOQ_WTQUIC_HAVE_TRUSTED_CLIENT)
                message(FATAL_ERROR
                    "MOQ_WTQUIC_ORIGIN_GATE was requested, but this wtquic "
                    "does not provide wtq_msquic_client_connect_with_trust. "
                    "The Origin fixture cannot verify a server certificate "
                    "without it; select a wtquic that has it rather than "
                    "building a fixture that cannot prove anything.")
            endif()
            add_executable(test_wtquic_origin_gate
                tests/test_wtquic_origin_gate.c tests/origin_driver.c
                tests/origin_super.c tests/origin_rows.c)
            target_include_directories(test_wtquic_origin_gate PRIVATE tests)
            target_link_libraries(test_wtquic_origin_gate PRIVATE
                moq::adapter-wtquic-msquic-managed wtq::msquic Threads::Threads)
            target_include_directories(test_wtquic_origin_gate PRIVATE
                ${PROJECT_SOURCE_DIR}/tests/support)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_origin_gate PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            # Identity only. The row execution is held: no registered test
            # starts an environment, a listener or a client.
            add_test(NAME wtquic_origin_gate_identity
                COMMAND test_wtquic_origin_gate --identity)
            set_tests_properties(wtquic_origin_gate_identity PROPERTIES
                TIMEOUT 60)

            # The ACTUAL native callbacks and owning pump, with only the
            # transport operations substituted: no provider, socket,
            # certificate or network. It needs the WT headers, which is why it
            # lives behind the same request as the fixture.
            add_executable(test_wtquic_origin_native
                tests/test_wtquic_origin_native.c tests/origin_driver.c
                tests/origin_rows.c tests/origin_super.c)
            target_include_directories(test_wtquic_origin_native PRIVATE tests)
            target_link_libraries(test_wtquic_origin_native PRIVATE
                Threads::Threads)
            # Only the HEADERS of the facade and the client are needed. They
            # come in as SYSTEM includes, exactly as the imported targets would
            # supply them, so the provider's own pedantic diagnostics are not
            # attributed to this fixture -- and no warning flag is relaxed.
            target_include_directories(test_wtquic_origin_native SYSTEM PRIVATE
                $<TARGET_PROPERTY:moq::adapter-wtquic-msquic-managed,INTERFACE_INCLUDE_DIRECTORIES>
                $<TARGET_PROPERTY:wtq::msquic,INTERFACE_INCLUDE_DIRECTORIES>)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_origin_native PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME wtquic_origin_native
                COMMAND test_wtquic_origin_native)
            set_tests_properties(wtquic_origin_native PROPERTIES TIMEOUT 120)
        endif()

        # Causal whole-block cfg-ABI gate (no network): an over-read of
        # app_deadline_ctx is a heap-buffer-overflow under the canonical ASan
        # build. Aborts on an oversized offer before dialing.
        add_executable(test_wtquic_msquic_managed_cfg_abi
            tests/test_wtquic_msquic_managed_cfg_abi.c)
        target_link_libraries(test_wtquic_msquic_managed_cfg_abi PRIVATE
            moq::adapter-wtquic-msquic-managed Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_msquic_managed_cfg_abi PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_msquic_managed_cfg_abi
            COMMAND test_wtquic_msquic_managed_cfg_abi)

        add_executable(test_wtquic_msquic_managed_link
            tests/test_wtquic_msquic_managed_link.c)
        target_link_libraries(test_wtquic_msquic_managed_link PRIVATE
            moq::adapter-wtquic-msquic-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_msquic_managed_link PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_msquic_managed_link
            COMMAND test_wtquic_msquic_managed_link)

        if(CMAKE_CXX_COMPILER)
            add_executable(test_wtquic_msquic_managed_link_cxx
                tests/test_wtquic_msquic_managed_link.cpp)
            target_link_libraries(test_wtquic_msquic_managed_link_cxx PRIVATE
                moq::adapter-wtquic-msquic-managed)
            set_target_properties(test_wtquic_msquic_managed_link_cxx PROPERTIES
                CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
            add_test(NAME wtquic_msquic_managed_link_cxx
                COMMAND test_wtquic_msquic_managed_link_cxx)
        endif()

        # White-box: recompile the facade source with MOQ_WTQ_MM_TESTING so the
        # test can drive the private pthread-init failure seam and read back the
        # owned copies. Links the DEPS (not the managed library) to avoid
        # duplicate symbols; the testing seam never enters the shipped library.
        add_executable(test_wtquic_msquic_managed_internal
            tests/test_wtquic_msquic_managed_internal.c
            tests/wtquic_msquic_managed_internal_tu.c)
        target_compile_definitions(test_wtquic_msquic_managed_internal PRIVATE
            MOQ_WTQ_MM_TESTING MOQ_BUILDING)
        target_include_directories(test_wtquic_msquic_managed_internal PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/..)
        target_link_libraries(test_wtquic_msquic_managed_internal PRIVATE
            moq::adapter-wtquic wtq::msquic Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_msquic_managed_internal PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_msquic_managed_internal
            COMMAND test_wtquic_msquic_managed_internal)
    endif()

    # Export policy: the adapter's moq_wtquic_* surface is the installed public
    # API plus the explicitly listed private lockstep SPI; nothing else (in particular no
    # white-box test seam) may be exported.
    add_test(NAME wtquic_adapter_symbol_policy
        COMMAND ${CMAKE_COMMAND}
            -DLIB=$<TARGET_FILE:moq-adapter-wtquic>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_wtquic_adapter_symbols.cmake)
    set_tests_properties(wtquic_adapter_symbol_policy PROPERTIES
        LABELS "wtquic")

    # Export policy for the MsQuic managed facade: exactly the API declared in
    # moq/wtquic_msquic_managed.h and, for a shared library, nothing outside
    # the moq_ namespace (no re-exported wtquic implementation).
    if(TARGET moq-adapter-wtquic-msquic-managed)
        add_test(NAME wtquic_msquic_managed_symbol_policy
            COMMAND ${CMAKE_COMMAND}
                -DLIB=$<TARGET_FILE:moq-adapter-wtquic-msquic-managed>
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_wtquic_msquic_managed_symbols.cmake)
        set_tests_properties(wtquic_msquic_managed_symbol_policy PROPERTIES
            LABELS "wtquic")
    endif()

    # Installed consumer: installs this build to a scratch prefix and
    # builds find_package(libmoq COMPONENTS adapter-wtquic) against it.
    get_filename_component(_wtquic_dep_prefix "${wtquic_DIR}/../../.."
        ABSOLUTE)
    if(TARGET moq-adapter-wtquic-network-managed)
        set(_wtquic_consumer_network_managed 1)
    else()
        set(_wtquic_consumer_network_managed 0)
    endif()
    if(TARGET moq-adapter-wtquic-msquic-managed)
        set(_wtquic_consumer_msquic_managed 1)
    else()
        set(_wtquic_consumer_msquic_managed 0)
    endif()
    # The MsQuic identity is only meaningful when that optional backend
    # exists. Evaluating $<TARGET_FILE:msquic::msquic> unconditionally makes
    # generation fail for a core-only WTQuic install, which the backend-neutral
    # attach adapter must still support.
    set(_wtquic_consumer_msquic_lib "")
    if(TARGET msquic::msquic)
        set(_wtquic_consumer_msquic_lib
            "-DMSQUIC_LIB=$<TARGET_FILE:msquic::msquic>")
    endif()
    include("${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerBuildContext.cmake")
    set(_moq_consumer_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-context.cmake")
    moq_write_consumer_context("${_moq_consumer_context}")
    if(MOQ_BUILD_PACKAGE_EXPORTS)
    add_test(NAME wtquic_install_consumer
        COMMAND ${CMAKE_COMMAND}
            -DBUILD=${PROJECT_BINARY_DIR}
            -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
            -DWORK=${CMAKE_CURRENT_BINARY_DIR}/install-consumer-work
            "-DCONSUMER_CONTEXT=${_moq_consumer_context}"
            -DWTQUIC_PREFIX=${_wtquic_dep_prefix}
            -DNETWORK_MANAGED=${_wtquic_consumer_network_managed}
            -DMSQUIC_MANAGED=${_wtquic_consumer_msquic_managed}
            -DMSQUIC_DIR=${msquic_DIR}
            -DC_COMPILER=${CMAKE_C_COMPILER}
            -DSHARED=${BUILD_SHARED_LIBS}
            ${_wtquic_consumer_msquic_lib}
            "-DC_FLAGS=${CMAKE_C_FLAGS}"
            "-DLINK_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer_install.cmake)
    set_tests_properties(wtquic_install_consumer PROPERTIES TIMEOUT 300)
    endif()
    # The loader-image helper decides what the consumer lane proved, so its
    # own decisions are pinned against captured fixture text for both
    # supported platforms. No process is started.
    add_test(NAME wtquic_loader_images_selftest
        COMMAND ${CMAKE_COMMAND}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/loader_images_selftest.cmake)
    set_tests_properties(wtquic_loader_images_selftest PROPERTIES TIMEOUT 60)

    # Build-tree consumer: the same consumer project resolved against
    # ${PROJECT_BINARY_DIR} (no install step).
    if(MOQ_BUILD_PACKAGE_EXPORTS)
    add_test(NAME wtquic_adapter_consumer
        COMMAND ${CMAKE_COMMAND}
            -DBUILD=${PROJECT_BINARY_DIR}
            -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
            -DWORK=${CMAKE_CURRENT_BINARY_DIR}/buildtree-consumer-work
            -DC_COMPILER=${CMAKE_C_COMPILER}
            "-DC_FLAGS=${CMAKE_C_FLAGS}"
            "-DLINK_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer_buildtree.cmake)
    set_tests_properties(wtquic_adapter_consumer PROPERTIES TIMEOUT 180)
    endif()

    # Opaque-key allocator unit test: includes the adapter TU directly to
    # reach the static allocator. No network/certs — pure logic.
    # MsQuic-free: the wtq_* transport calls are stubbed in the test, so it
    # links NEITHER libwtquic NOR libmsquic — only the white-box core seam
    # (the test calls internal push_action, which a shared moq::core hides)
    # plus the wtquic + adapter HEADERS (declarations only).
    add_executable(test_wtquic_keys tests/test_wtquic_keys.c)
    target_link_libraries(test_wtquic_keys PRIVATE moq-core-test-internals)
    # core/include must come FIRST: the wtq::wtquic interface dir is the
    # install prefix, which may hold a stale installed libmoq whose
    # headers would otherwise shadow this build tree's.
    target_include_directories(test_wtquic_keys PRIVATE
        ${PROJECT_SOURCE_DIR}/core/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
    target_compile_definitions(test_wtquic_keys PRIVATE MOQ_BUILDING)
    add_test(NAME wtquic_keys COMMAND test_wtquic_keys)
    set_tests_properties(wtquic_keys PROPERTIES LABELS "wtquic")

    # Stream table sized from the provider's queried api_capacity, and the
    # checked layout arithmetic; real adapter/bridge over a fake WT provider.
    add_executable(test_wtquic_capacity tests/test_wtquic_capacity.c)
    target_link_libraries(test_wtquic_capacity PRIVATE moq::core)
    target_include_directories(test_wtquic_capacity PRIVATE
        ${PROJECT_SOURCE_DIR}/core/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
    target_compile_definitions(test_wtquic_capacity PRIVATE MOQ_BUILDING WTQ_STATIC)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_capacity PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_capacity COMMAND test_wtquic_capacity)
    set_tests_properties(wtquic_capacity PROPERTIES LABELS "wtquic" TIMEOUT 30)

    # Public startup through the real adapter/bridge, with a fake WT provider.
    add_executable(test_wtquic_open_close tests/test_wtquic_open_close.c)
    target_link_libraries(test_wtquic_open_close PRIVATE moq::core)
    target_include_directories(test_wtquic_open_close PRIVATE
        ${PROJECT_SOURCE_DIR}/core/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
    target_compile_definitions(test_wtquic_open_close PRIVATE MOQ_BUILDING WTQ_STATIC)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_open_close PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_open_close COMMAND test_wtquic_open_close)
    set_tests_properties(wtquic_open_close PROPERTIES
        LABELS "wtquic;lifecycle" TIMEOUT 15)

    # Real adapter/core dispatch with a bounded fake WT provider. Native peer
    # ACK semantics are qualified separately against the selected WT backend.
    add_executable(test_wtquic_commit tests/test_wtquic_commit.c)
    target_link_libraries(test_wtquic_commit PRIVATE moq::core)
    target_include_directories(test_wtquic_commit PRIVATE
        ${PROJECT_SOURCE_DIR}/core/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
    target_compile_definitions(test_wtquic_commit PRIVATE MOQ_BUILDING WTQ_STATIC)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_commit PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_commit COMMAND test_wtquic_commit)
    set_tests_properties(wtquic_commit PROPERTIES
        LABELS "wtquic;lifecycle" TIMEOUT 30)

    # Includes the actual managed and attach TUs with typed WT fakes. Use only
    # provider headers; backend completion is covered by a separate native test.
    # The managed TU is the MsQuic backend's, so this needs that backend's
    # headers and is absent from a Network-only build.
    if(TARGET wtq::msquic AND TARGET msquic::msquic)
        add_executable(test_wtquic_flush tests/test_wtquic_flush.c)
        target_link_libraries(test_wtquic_flush PRIVATE moq::core Threads::Threads)
        target_include_directories(test_wtquic_flush PRIVATE
            ${PROJECT_SOURCE_DIR}/core/include ${PROJECT_SOURCE_DIR}/core/src
            ${PROJECT_SOURCE_DIR}/core/src/session
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
        # The managed TU includes the MsQuic facade, whose header includes
        # <msquic.h>: MsQuic's headers, without linking it (the WT calls are
        # faked). SYSTEM, as the imported target would supply them.
        target_include_directories(test_wtquic_flush SYSTEM PRIVATE
            $<TARGET_PROPERTY:msquic::msquic,INTERFACE_INCLUDE_DIRECTORIES>)
        target_compile_definitions(test_wtquic_flush PRIVATE MOQ_BUILDING WTQ_STATIC)
        set_target_properties(test_wtquic_flush PROPERTIES
            C_STANDARD 11 C_STANDARD_REQUIRED YES C_EXTENSIONS NO)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_flush PRIVATE
                -Wall -Wextra -Wpedantic -ffunction-sections -fdata-sections
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        if(APPLE)
            target_link_options(test_wtquic_flush PRIVATE -Wl,-dead_strip)
        elseif(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_link_options(test_wtquic_flush PRIVATE -Wl,--gc-sections)
        endif()
        add_test(NAME wtquic_flush COMMAND test_wtquic_flush)
        set_tests_properties(wtquic_flush PROPERTIES
            LABELS "wtquic;lifecycle" TIMEOUT 30)
    endif()

    # Prospective receive-admission ownership, with a fake provider only.
    add_executable(test_wtquic_hold_input tests/test_wtquic_hold_input.c)
    add_executable(test_wtquic_qualification tests/test_wtquic_qualification.c)
    target_link_libraries(test_wtquic_qualification PRIVATE moq-core-test-internals)
    target_include_directories(test_wtquic_qualification PRIVATE
        ${PROJECT_SOURCE_DIR}/core/include ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
    target_compile_definitions(test_wtquic_qualification PRIVATE MOQ_BUILDING)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_qualification PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_qualification COMMAND test_wtquic_qualification)
    add_executable(test_wtquic_attach_reporting tests/test_wtquic_attach_reporting.c)
    target_link_libraries(test_wtquic_attach_reporting PRIVATE moq::adapter-wtquic)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_attach_reporting PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME wtquic_attach_reporting COMMAND test_wtquic_attach_reporting)
    target_link_libraries(test_wtquic_hold_input PRIVATE moq-core-test-internals)
    target_include_directories(test_wtquic_hold_input PRIVATE
        ${PROJECT_SOURCE_DIR}/core/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<TARGET_PROPERTY:wtq::wtquic,INTERFACE_INCLUDE_DIRECTORIES>)
    target_compile_definitions(test_wtquic_hold_input PRIVATE MOQ_BUILDING)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_hold_input PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    foreach(_case direct replay fin_only paused terminal reuse resume_close resume_reuse pause_close pause_reuse)
        add_test(NAME wtquic_hold_input_${_case}
            COMMAND test_wtquic_hold_input ${_case})
        set_tests_properties(wtquic_hold_input_${_case} PROPERTIES
            LABELS "wtquic;admission" TIMEOUT 10)
    endforeach()
    foreach(_case resume_close resume_reuse pause_close pause_reuse)
        foreach(_result state closed error)
            add_test(NAME wtquic_hold_input_${_case}_${_result}
                COMMAND test_wtquic_hold_input ${_case} ${_result})
            set_tests_properties(wtquic_hold_input_${_case}_${_result} PROPERTIES
                LABELS "wtquic;callback-generation" TIMEOUT 10)
        endforeach()
    endforeach()

    # Network-managed cfg initializer ABI + short-config rejection test.
    # No network: create() rejects the bogus offer list (MOQ_ERR_INVAL)
    # during validation, before allocation. The optional-tail import,
    # including the app_deadline whole-block gate, lies below an
    # unconditional capability rejection and is unreachable here.
    # Links only the managed lib (transitively wtq::network); no MsQuic
    # server, no certs. Gated on the optional managed target so a
    # tests-on build without it still configures.
    if(TARGET moq-adapter-wtquic-network-managed)
        add_executable(test_wtquic_network_managed_cfg_abi
            tests/test_wtquic_network_managed_cfg_abi.c)
        target_link_libraries(test_wtquic_network_managed_cfg_abi PRIVATE
            moq::adapter-wtquic-network-managed)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_network_managed_cfg_abi PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_network_managed_cfg_abi
            COMMAND test_wtquic_network_managed_cfg_abi)
        set_tests_properties(wtquic_network_managed_cfg_abi PROPERTIES
            LABELS "wtquic")

        # White-box deadline-scheduler discriminators. Recompiles the facade
        # with MOQ_WTQ_MM_TESTING so the transport/adapter primitives are
        # stubbed and nwm_doorbell/nwm_rearm_deadline run against a synthetic
        # connection — deterministic, no real timer/transport, no network.
        # Links the DEPS (not the managed library) to avoid duplicate symbols;
        # the testing seam never enters the shipped library.
        add_executable(test_wtquic_network_managed_internal
            tests/test_wtquic_network_managed_internal.c
            tests/wtquic_network_managed_internal_tu.c)
        target_compile_definitions(test_wtquic_network_managed_internal PRIVATE
            MOQ_WTQ_MM_TESTING MOQ_BUILDING)
        target_include_directories(test_wtquic_network_managed_internal PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/..)
        target_link_libraries(test_wtquic_network_managed_internal PRIVATE
            moq::adapter-wtquic wtq::network Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_network_managed_internal PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_network_managed_internal
            COMMAND test_wtquic_network_managed_internal)
        add_executable(test_wtquic_managed_reporting tests/test_wtquic_managed_reporting.c)
        target_link_libraries(test_wtquic_managed_reporting PRIVATE
            moq::adapter-wtquic wtq::network Threads::Threads)
        target_compile_definitions(test_wtquic_managed_reporting PRIVATE MOQ_BUILDING)
        if(MOQ_BUILD_SERVICE)
            target_link_libraries(test_wtquic_managed_reporting PRIVATE moq::service)
            target_compile_definitions(test_wtquic_managed_reporting PRIVATE MOQ_TEST_SERVICE)
        endif()
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_managed_reporting PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_managed_reporting COMMAND test_wtquic_managed_reporting)
        set_tests_properties(wtquic_network_managed_internal PROPERTIES
            LABELS "wtquic")
    endif()

    # Real-transport tests drive an MsQuic peer; a tests-on build with
    # only the Network backend installed must still configure, so
    # everything that links wtq::msquic gates on the target. The pure
    # adapter/key/consumer tests above stay unconditional.
    if(TARGET wtq::msquic)
    add_executable(test_wtquic_loopback tests/test_wtquic_loopback.c)
    target_link_libraries(test_wtquic_loopback PRIVATE
        moq::adapter-wtquic wtq::msquic Threads::Threads)
    target_include_directories(test_wtquic_loopback PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/support)

    # Production-contract conformance over real MsQuic loopback: every
    # MoQ session call runs inside the adapter hook on the transport
    # worker, driven by per-side event-triggered scripts (see
    # tests/wtquic_contract_pair.h — deliberately not the shared
    # sim-shaped conformance rail).
    add_executable(test_wtquic_conformance
        tests/test_wtquic_conformance.c
        tests/wtquic_contract_pair.c)
    target_link_libraries(test_wtquic_conformance PRIVATE
        moq::adapter-wtquic wtq::msquic Threads::Threads)
    target_include_directories(test_wtquic_conformance PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/support)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_conformance PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_loopback PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    # Stress/soak: repeated full-lifecycle churn over the same script
    # harness (no thresholds; MOQ_WTQUIC_STRESS_LOOPS extends the run
    # for manual soaks).
    add_executable(test_wtquic_stress
        tests/test_wtquic_stress.c
        tests/wtquic_contract_pair.c)
    target_link_libraries(test_wtquic_stress PRIVATE
        moq::adapter-wtquic wtq::msquic Threads::Threads)
    target_include_directories(test_wtquic_stress PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/support)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_stress PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    # A pair's listener stays reachable over IPv4 loopback while IPv4-only
    # sockets hold much of the ephemeral port range.
    add_executable(test_wtquic_port_shadow
        tests/test_wtquic_port_shadow.c
        tests/wtquic_contract_pair.c)
    target_link_libraries(test_wtquic_port_shadow PRIVATE
        moq::adapter-wtquic wtq::msquic Threads::Threads)
    target_include_directories(test_wtquic_port_shadow PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/support)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_port_shadow PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    find_program(MOQ_OPENSSL openssl)
    if(MOQ_OPENSSL)
        add_test(NAME wtquic_gen_certs
            COMMAND ${MOQ_OPENSSL} req -x509 -newkey rsa:2048 -sha256
                -days 30 -nodes
                -keyout ${CMAKE_CURRENT_BINARY_DIR}/key.pem
                -out ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                -subj /CN=localhost)
        set_tests_properties(wtquic_gen_certs PROPERTIES
            FIXTURES_SETUP wtquic_certs
            LABELS "network;wtquic")
        add_test(NAME wtquic_loopback
            COMMAND test_wtquic_loopback
                ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
        set_tests_properties(wtquic_loopback PROPERTIES
            FIXTURES_REQUIRED wtquic_certs
            LABELS "network;wtquic"
            TIMEOUT 120)

        # Managed facade CLIENT over the public listener: real draft-16/18
        # negotiation + establishment. Registered here so it shares the cert
        # fixture; guarded on the facade target.
        if(TARGET moq::adapter-wtquic-msquic-managed)
            # The fixture includes the real adapter TU for private mapping
            # observations; the managed TU and WT provider are unchanged.
            add_executable(test_wtquic_commit_loopback
                tests/test_wtquic_commit_loopback.c
                wtquic_msquic_managed.c)
            target_compile_features(test_wtquic_commit_loopback PRIVATE c_std_11)
            target_compile_definitions(test_wtquic_commit_loopback PRIVATE MOQ_BUILDING)
            target_include_directories(test_wtquic_commit_loopback PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${CMAKE_CURRENT_SOURCE_DIR}
                ${CMAKE_CURRENT_SOURCE_DIR}/..
                ${PROJECT_SOURCE_DIR}/tests/support)
            target_link_libraries(test_wtquic_commit_loopback PRIVATE
                moq::core wtq::wtquic wtq::msquic Threads::Threads)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_commit_loopback PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME wtquic_commit_loopback
                COMMAND test_wtquic_commit_loopback
                    ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                    ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
            set_tests_properties(wtquic_commit_loopback PROPERTIES
                FIXTURES_REQUIRED wtquic_certs
                LABELS "network;wtquic"
                TIMEOUT 240)

            # White-box loopback: recompile the facade with MOQ_WTQ_MM_TESTING so
            # the SERVER case can observe an established child under its lane
            # guard (proving establishment, not just admission). Links the DEPS
            # (not the managed library) to avoid duplicate symbols; the testing
            # seam never enters the shipped library.
            add_executable(test_wtquic_msquic_managed_loopback
                tests/test_wtquic_msquic_managed_loopback.c
                ${CMAKE_CURRENT_SOURCE_DIR}/wtquic_msquic_managed.c)
            target_compile_definitions(test_wtquic_msquic_managed_loopback PRIVATE
                MOQ_WTQ_MM_TESTING MOQ_BUILDING)
            target_include_directories(test_wtquic_msquic_managed_loopback PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/..)
            target_link_libraries(test_wtquic_msquic_managed_loopback PRIVATE
                moq::adapter-wtquic wtq::msquic Threads::Threads)
            target_include_directories(test_wtquic_msquic_managed_loopback PRIVATE
                ${PROJECT_SOURCE_DIR}/tests/support)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_msquic_managed_loopback PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME wtquic_msquic_managed_loopback
                COMMAND test_wtquic_msquic_managed_loopback
                    ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                    ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
            set_tests_properties(wtquic_msquic_managed_loopback PROPERTIES
                FIXTURES_REQUIRED wtquic_certs
                LABELS "network;wtquic"
                TIMEOUT 120)
            # The same run while IPv4-only sockets hold much of the ephemeral
            # range (tests/support/port_shadow.c): the raw wtquic listeners take
            # their ports from the IPv4 allocator (tests/wtq_loopback_port.h);
            # the managed server picks its port 0 that way itself.
            if(NOT WIN32)
                add_test(NAME wtquic_msquic_managed_loopback_port_shadow
                    COMMAND $<TARGET_FILE:moq_port_shadow> --
                        $<TARGET_FILE:test_wtquic_msquic_managed_loopback>
                        ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                        ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
                # Every port-shadow test holds thousands of IPv4 ports for its run; two at
                # once can exhaust the ephemeral range, so they share one resource lock.
                set_tests_properties(wtquic_msquic_managed_loopback_port_shadow
                    PROPERTIES
                    FIXTURES_REQUIRED wtquic_certs
                    LABELS "network;wtquic"
                    TIMEOUT 180
                    RESOURCE_LOCK ipv4_port_shadow)

                # A server created with port 0 is reachable over IPv4
                # loopback under the same shadowing: the facade, not the
                # test, chooses the port. Public managed API only.
                add_executable(test_wtquic_msquic_managed_ephemeral_port
                    tests/test_wtquic_msquic_managed_ephemeral_port.c)
                target_link_libraries(test_wtquic_msquic_managed_ephemeral_port
                    PRIVATE moq::adapter-wtquic-msquic-managed wtq::msquic
                    Threads::Threads)
                if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                    target_compile_options(
                        test_wtquic_msquic_managed_ephemeral_port PRIVATE
                        -Wall -Wextra -Wpedantic
                        $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
                endif()
                add_test(NAME wtquic_msquic_managed_ephemeral_port_shadow
                    COMMAND $<TARGET_FILE:moq_port_shadow> --
                        $<TARGET_FILE:test_wtquic_msquic_managed_ephemeral_port>
                        ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                        ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
                # Every port-shadow test holds thousands of IPv4 ports for its run; two at
                # once can exhaust the ephemeral range, so they share one resource lock.
                set_tests_properties(wtquic_msquic_managed_ephemeral_port_shadow
                    PROPERTIES
                    FIXTURES_REQUIRED wtquic_certs
                    LABELS "network;wtquic"
                    TIMEOUT 180
                    RESOURCE_LOCK ipv4_port_shadow)
            endif()

            # Stream-resource block over real MsQuic: managed server and client
            # hold a mixed persistent load and still deliver media before EOF;
            # forwarded local reservations bound local opens. Public API only.
            add_executable(test_wtquic_msquic_managed_capacity
                tests/test_wtquic_msquic_managed_capacity.c)
            target_link_libraries(test_wtquic_msquic_managed_capacity PRIVATE
                moq::adapter-wtquic-msquic-managed Threads::Threads)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_msquic_managed_capacity PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME wtquic_msquic_managed_capacity
                COMMAND test_wtquic_msquic_managed_capacity
                    ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                    ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
            set_tests_properties(wtquic_msquic_managed_capacity PROPERTIES
                FIXTURES_REQUIRED wtquic_certs
                LABELS "network;wtquic"
                TIMEOUT 180)

            # Autonomous bounded-poll delivery: a MANAGED subscriber must deliver
            # a whole track to a bounded-poll app with no explicit receiver wake
            # after setup, over a RAW wtquic publisher. Uses the PUBLIC managed
            # API (no white-box seam), so it links the shipped managed library.
            add_executable(test_wtquic_msquic_managed_autodrain
                tests/test_wtquic_msquic_managed_autodrain.c
                tests/wtquic_contract_pair.c)
            target_include_directories(test_wtquic_msquic_managed_autodrain PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/..)
            target_link_libraries(test_wtquic_msquic_managed_autodrain PRIVATE
                moq::adapter-wtquic-msquic-managed moq::adapter-wtquic
                wtq::msquic Threads::Threads)
            target_include_directories(test_wtquic_msquic_managed_autodrain PRIVATE
                ${PROJECT_SOURCE_DIR}/tests/support)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_msquic_managed_autodrain PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME wtquic_msquic_managed_autodrain
                COMMAND test_wtquic_msquic_managed_autodrain
                    ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                    ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
            set_tests_properties(wtquic_msquic_managed_autodrain PROPERTIES
                FIXTURES_REQUIRED wtquic_certs
                LABELS "network;wtquic"
                TIMEOUT 300)
        endif()
        add_test(NAME wtquic_conformance
            COMMAND test_wtquic_conformance
                ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
        set_tests_properties(wtquic_conformance PROPERTIES
            FIXTURES_REQUIRED wtquic_certs
            LABELS "network;wtquic"
            TIMEOUT 180)
        add_test(NAME wtquic_stress
            COMMAND test_wtquic_stress
                ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
        set_tests_properties(wtquic_stress PROPERTIES
            FIXTURES_REQUIRED wtquic_certs
            LABELS "network;wtquic"
            TIMEOUT 600)
        add_test(NAME wtquic_port_shadow
            COMMAND test_wtquic_port_shadow
                ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
        # Every port-shadow test holds thousands of IPv4 ports for its run; two at
        # once can exhaust the ephemeral range, so they share one resource lock.
        set_tests_properties(wtquic_port_shadow PROPERTIES
            FIXTURES_REQUIRED wtquic_certs
            LABELS "network;wtquic"
            TIMEOUT 120
            RESOURCE_LOCK ipv4_port_shadow)

    endif()
    endif() # TARGET wtq::msquic

    # Public Network rejection policy: no peer, certificates or MsQuic required.
    if(TARGET moq-adapter-wtquic-network-managed)
        add_executable(test_wtquic_network_managed
            tests/test_wtquic_network_managed.c)
        target_link_libraries(test_wtquic_network_managed PRIVATE
            moq::adapter-wtquic-network-managed
            Threads::Threads)
        if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(test_wtquic_network_managed PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME wtquic_network_managed
            COMMAND test_wtquic_network_managed)
        set_tests_properties(wtquic_network_managed PROPERTIES
            LABELS "wtquic"
            TIMEOUT 300)
        add_test(NAME wtquic_network_managed_tests_off
            COMMAND ${CMAKE_COMMAND}
                -DSRC=${PROJECT_SOURCE_DIR}
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/network-tests-off-work
                "-DCONSUMER_CONTEXT=${_moq_consumer_context}"
                -DWTQUIC_PREFIX=${_wtquic_dep_prefix}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/network_managed_tests_off.cmake)
        set_tests_properties(wtquic_network_managed_tests_off PROPERTIES
            LABELS "wtquic"
            TIMEOUT 600)
    endif()

    # Network-only tests-on lane: a fresh tree configured against a
    # prefix WITHOUT the msquic component must still generate and
    # build (core + Network vertical only; the MsQuic server tests
    # gate themselves away).
    if(TARGET moq-adapter-wtquic-network-managed)
        add_test(NAME wtquic_network_only_tests_on
            COMMAND ${CMAKE_COMMAND}
                -DSRC=${PROJECT_SOURCE_DIR}
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/network-only-work
                "-DCONSUMER_CONTEXT=${_moq_consumer_context}"
                -DWTQUIC_PREFIX=${_wtquic_dep_prefix}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/network_only_tests_on.cmake)
        set_tests_properties(wtquic_network_only_tests_on PROPERTIES
            LABELS "wtquic"
            TIMEOUT 600)
    endif()
endif()
