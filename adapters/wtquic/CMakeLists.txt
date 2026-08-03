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

# Build-tree imported target so `find_package(libmoq COMPONENTS
# adapter-wtquic)` works against ${PROJECT_BINARY_DIR}, mirroring the
# pico WT adapter. The install-tree counterpart is generated below.
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqWtquicAdapterTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-wtquic)\n\
  add_library(moq::adapter-wtquic $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-wtquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-wtquic PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-wtquic>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::core;wtq::wtquic\")\n\
endif()\n")

# -- Install / export --------------------------------------------------
include(GNUInstallDirs)

install(TARGETS moq-adapter-wtquic
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

# Relocatable install-tree imported target file (the wtquic dependency
# is re-found by the adapter-wtquic component block in libmoqConfig).
set(_wtquic_lib_name "$<TARGET_FILE_NAME:moq-adapter-wtquic>")
set(_wtquic_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-wtquic,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
set(_wtquic_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_wtquic_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqWtquicAdapterTargets.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicAdapterTargets-install.cmake.in
    @ONLY)
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicAdapterTargets-install.cmake
    INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicAdapterTargets-install.cmake.in)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicAdapterTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqWtquicAdapterTargets.cmake)

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

    file(GENERATE
        OUTPUT "${PROJECT_BINARY_DIR}/libmoqWtquicNetworkManagedTargets.cmake"
        CONTENT "\
if(NOT TARGET moq::adapter-wtquic-network-managed)\n\
  add_library(moq::adapter-wtquic-network-managed $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-wtquic-network-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-wtquic-network-managed PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-wtquic-network-managed>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-wtquic;wtq::network\")\n\
endif()\n")

    install(TARGETS moq-adapter-wtquic-network-managed
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    set(_network_managed_lib_name
        "$<TARGET_FILE_NAME:moq-adapter-wtquic-network-managed>")
    set(_network_managed_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-wtquic-network-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
    set(_network_managed_libdir "${CMAKE_INSTALL_LIBDIR}")
    set(_network_managed_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqWtquicNetworkManagedTargets.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicNetworkManagedTargets-install.cmake.in
        @ONLY)
    file(GENERATE
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicNetworkManagedTargets-install.cmake
        INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicNetworkManagedTargets-install.cmake.in)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicNetworkManagedTargets-install.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
        RENAME libmoqWtquicNetworkManagedTargets.cmake)
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
    find_package(Threads REQUIRED) # the facade owns a coordinator thread
    add_library(moq-adapter-wtquic-msquic-managed
        wtquic_msquic_managed.c
    )
    add_library(moq::adapter-wtquic-msquic-managed ALIAS
        moq-adapter-wtquic-msquic-managed)
    target_include_directories(moq-adapter-wtquic-msquic-managed PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
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
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(moq-adapter-wtquic-msquic-managed PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    file(GENERATE
        OUTPUT "${PROJECT_BINARY_DIR}/libmoqWtquicMsquicManagedTargets.cmake"
        CONTENT "\
if(NOT TARGET moq::adapter-wtquic-msquic-managed)\n\
  add_library(moq::adapter-wtquic-msquic-managed $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-wtquic-msquic-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-wtquic-msquic-managed PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-wtquic-msquic-managed>\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-wtquic;wtq::msquic\")\n\
endif()\n")

    install(TARGETS moq-adapter-wtquic-msquic-managed
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    set(_msquic_managed_lib_name
        "$<TARGET_FILE_NAME:moq-adapter-wtquic-msquic-managed>")
    set(_msquic_managed_lib_type "$<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-wtquic-msquic-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED>")
    set(_msquic_managed_libdir "${CMAKE_INSTALL_LIBDIR}")
    set(_msquic_managed_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/libmoqWtquicMsquicManagedTargets.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicMsquicManagedTargets-install.cmake.in
        @ONLY)
    file(GENERATE
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicMsquicManagedTargets-install.cmake
        INPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicMsquicManagedTargets-install.cmake.in)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/libmoqWtquicMsquicManagedTargets-install.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
        RENAME libmoqWtquicMsquicManagedTargets.cmake)
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
            ${CMAKE_CURRENT_SOURCE_DIR}/wtquic_msquic_managed.c)
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
    # API plus exactly ONE private lockstep SPI; nothing else (in particular no
    # white-box test seam) may be exported.
    add_test(NAME wtquic_adapter_symbol_policy
        COMMAND ${CMAKE_COMMAND}
            -DLIB=$<TARGET_FILE:moq-adapter-wtquic>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_wtquic_adapter_symbols.cmake)
    set_tests_properties(wtquic_adapter_symbol_policy PROPERTIES
        LABELS "wtquic")

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
    add_test(NAME wtquic_install_consumer
        COMMAND ${CMAKE_COMMAND}
            -DBUILD=${PROJECT_BINARY_DIR}
            -DSRC=${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer
            -DWORK=${CMAKE_CURRENT_BINARY_DIR}/install-consumer-work
            -DWTQUIC_PREFIX=${_wtquic_dep_prefix}
            -DNETWORK_MANAGED=${_wtquic_consumer_network_managed}
            -DMSQUIC_MANAGED=${_wtquic_consumer_msquic_managed}
            -DC_COMPILER=${CMAKE_C_COMPILER}
            "-DC_FLAGS=${CMAKE_C_FLAGS}"
            "-DLINK_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer_install.cmake)
    set_tests_properties(wtquic_install_consumer PROPERTIES TIMEOUT 300)

    # Build-tree consumer: the same consumer project resolved against
    # ${PROJECT_BINARY_DIR} (no install step).
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

    # Network-managed cfg initializer + whole-block read-gate ABI test.
    # No network: it drives create()'s config-import and aborts on a bogus
    # offer list before dialing. The whole-block gate is proven causally
    # (an over-read of app_deadline_ctx is a heap-buffer-overflow under the
    # canonical ASan build). Links only the managed lib (transitively
    # wtq::network); no MsQuic server, no certs. Gated on the optional
    # managed target so a tests-on build without it still configures.
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
            ${CMAKE_CURRENT_SOURCE_DIR}/wtquic_network_managed.c)
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
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_wtquic_stress PRIVATE
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

        # Managed Network.framework client against a real MsQuic server:
        # the full vertical (session -> bridge -> adapter -> wtq_nw_conn
        # -> Network.framework) through the public facade only.
        if(TARGET moq-adapter-wtquic-network-managed)
            add_executable(test_wtquic_network_managed
                tests/test_wtquic_network_managed.c)
            target_link_libraries(test_wtquic_network_managed PRIVATE
                moq::adapter-wtquic-network-managed wtq::msquic
                Threads::Threads)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(test_wtquic_network_managed PRIVATE
                    -Wall -Wextra -Wpedantic
                    $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
            endif()
            add_test(NAME wtquic_network_managed
                COMMAND test_wtquic_network_managed
                    ${CMAKE_CURRENT_BINARY_DIR}/cert.pem
                    ${CMAKE_CURRENT_BINARY_DIR}/key.pem)
            set_tests_properties(wtquic_network_managed PROPERTIES
                FIXTURES_REQUIRED wtquic_certs
                LABELS "network;wtquic"
                TIMEOUT 300)
            add_test(NAME wtquic_network_managed_tests_off
                COMMAND ${CMAKE_COMMAND}
                    -DSRC=${PROJECT_SOURCE_DIR}
                    -DWORK=${CMAKE_CURRENT_BINARY_DIR}/network-tests-off-work
                    -DWTQUIC_PREFIX=${_wtquic_dep_prefix}
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/network_managed_tests_off.cmake)
            set_tests_properties(wtquic_network_managed_tests_off PROPERTIES
                LABELS "wtquic"
                TIMEOUT 600)
        endif()
    endif()
    endif() # TARGET wtq::msquic

    # Network-only tests-on lane: a fresh tree configured against a
    # prefix WITHOUT the msquic component must still generate and
    # build (core + Network vertical only; the MsQuic server tests
    # gate themselves away).
    if(TARGET moq-adapter-wtquic-network-managed)
        add_test(NAME wtquic_network_only_tests_on
            COMMAND ${CMAKE_COMMAND}
                -DSRC=${PROJECT_SOURCE_DIR}
                -DWORK=${CMAKE_CURRENT_BINARY_DIR}/network-only-work
                -DWTQUIC_PREFIX=${_wtquic_dep_prefix}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/network_only_tests_on.cmake)
        set_tests_properties(wtquic_network_only_tests_on PROPERTIES
            LABELS "wtquic"
            TIMEOUT 600)
    endif()
endif()
