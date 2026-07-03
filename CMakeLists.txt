cmake_minimum_required(VERSION 3.20)

project(libmoq
    VERSION 0.1.0
    LANGUAGES C
    DESCRIPTION "Sans-I/O Media over QUIC Transport library"
)

# -- Build options --------------------------------------------------
option(MOQ_BUILD_TESTS             "Build test suite"               ON)
option(MOQ_BUILD_SIM               "Build moq-sim"                  ${MOQ_BUILD_TESTS})
option(MOQ_BUILD_ADAPTER_PICOQUIC  "Build picoquic adapter"         OFF)
option(MOQ_BUILD_CPP_BINDING       "Build C++ binding"              OFF)
option(MOQ_BUILD_LOC               "Build moq-loc LOC helpers"      ON)
option(MOQ_BUILD_MSF               "Build moq-msf catalog parser"   OFF)
option(MOQ_BUILD_CMAF              "Build moq-cmaf segment parser"  ON)
if(MOQ_BUILD_LOC AND MOQ_BUILD_CMAF)
    set(_MOQ_MEDIA_OBJECT_DEFAULT ON)
else()
    set(_MOQ_MEDIA_OBJECT_DEFAULT OFF)
endif()
option(MOQ_BUILD_MEDIA_OBJECT      "Build moq-media-object normalizer" ${_MOQ_MEDIA_OBJECT_DEFAULT})
option(MOQ_BUILD_PLAYBACK          "Build moq-playback pipeline"    OFF)
option(MOQ_BUILD_SERVICE           "Build moq-service (endpoint + media service tier)" OFF)
option(MOQ_BUILD_BENCHMARKS        "Build benchmarks"               OFF)
option(MOQ_BUILD_EXAMPLES          "Build LibMoQ example programs"  ON)
option(MOQ_BUILD_PQ_THREADED       "Build threaded picoquic helper" OFF)
option(MOQ_BUILD_INTEROP_CLIENT    "Build interop test client"      OFF)
option(MOQ_BUILD_ADAPTER_MVFST     "Build mvfst QUIC adapter"       OFF)
option(MOQ_BUILD_ADAPTER_PROXYGEN  "Build proxygen WebTransport adapter" OFF)
option(MOQ_BUILD_ADAPTER_PICO_WT   "Build picoquic WebTransport adapter" OFF)
option(MOQ_PICO_WT_BUILD_LOOPBACK "Build pico_wt loopback test (needs picoquic-test)" OFF)
option(MOQ_BUILD_PICO_WT_MANAGED  "Build managed picoquic WebTransport helper" OFF)
option(MOQ_BUILD_FUZZ              "Build fuzz targets"             OFF)
option(MOQ_REQUIRE_FUZZ            "Fail if fuzz targets cannot be built" OFF)

# Treat warnings as errors for libmoq-OWNED targets only. Applied PRIVATE
# (or directory-scope in dirs that define only owned targets) so it never
# reaches dependency targets, generated/imported targets, installed/
# exported interfaces, or cold-consumer builds. Default OFF; CI/dev
# presets turn it ON. Consumed via the genexpr
# $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> alongside -Wall -Wextra
# -Wpedantic on each owned target.
#
# Policy / scope (audited):
#   COVERED  - core, media, sim, the shared bridge, all adapters
#              (picoquic raw + threaded, mvfst, pico_wt, proxygen) and
#              their tests, the conformance tests, the C/C++ unit+scenario
#              tests, the C++ binding's in-tree tests, and tools
#              (moq-interop-client).
#   EXCLUDED - the `moq_cpp` / adapter INTERFACE (consumer-facing) targets
#              and cold-consumer test builds (must not force -Werror on
#              downstreams); examples (demonstration code, several gated
#              behind adapter deps); benchmarks (performance code, not a
#              correctness gate); fuzz targets (built with a dedicated
#              -fsanitize=fuzzer toolchain - -Werror is orthogonal there).
option(MOQ_WARNINGS_AS_ERRORS      "Treat libmoq-owned warnings as errors" OFF)

include(GNUInstallDirs)

if(MOQ_BUILD_TESTS)
    enable_testing()
endif()

# The project is C by default. The mvfst and proxygen adapters are C++ (folly/
# fizz), and moq-service now links the mvfst C++ target directly -- so CXX must
# be enabled in this top-level scope, before any subdirectory that defines or
# links a C++ target (service/ is a sibling of adapters/mvfst, so enabling CXX
# only inside adapters/mvfst/ does not reach it). Harmless when neither adapter
# is built.
if(MOQ_BUILD_ADAPTER_MVFST OR MOQ_BUILD_ADAPTER_PROXYGEN)
    enable_language(CXX)
endif()

# -- Core (always built) -------------------------------------------
add_subdirectory(core)

# -- Simulation -----------------------------------------------------
if(MOQ_BUILD_SIM)
    add_subdirectory(sim)
endif()

# -- LOC helpers ----------------------------------------------------
if(MOQ_BUILD_LOC)
    add_subdirectory(media/loc)
endif()

# -- CMAF segment parser --------------------------------------------
if(MOQ_BUILD_CMAF)
    add_subdirectory(media/cmaf)
endif()

# -- Media object normalizer ----------------------------------------
if(MOQ_BUILD_MEDIA_OBJECT)
    if(NOT MOQ_BUILD_LOC)
        message(FATAL_ERROR "MOQ_BUILD_MEDIA_OBJECT=ON requires MOQ_BUILD_LOC=ON")
    endif()
    if(NOT MOQ_BUILD_CMAF)
        message(FATAL_ERROR "MOQ_BUILD_MEDIA_OBJECT=ON requires MOQ_BUILD_CMAF=ON")
    endif()
    add_subdirectory(media/object)
endif()

# -- Playback pipeline ----------------------------------------------
if(MOQ_BUILD_PLAYBACK)
    if(NOT MOQ_BUILD_MEDIA_OBJECT)
        message(FATAL_ERROR "MOQ_BUILD_PLAYBACK=ON requires MOQ_BUILD_MEDIA_OBJECT=ON")
    endif()
    add_subdirectory(media/playback)
endif()

# -- MSF catalog parser ---------------------------------------------
if(MOQ_BUILD_MSF)
    add_subdirectory(media/msf)
endif()

# -- Adapters -------------------------------------------------------
# Resolve picoquic once here, at the top level, so its targets are visible to
# every adapter subdirectory. find_package(picoquic CONFIG) creates *imported*
# targets (picoquic::picoquic-core / picoquic::picohttp-core), and an imported
# target is only visible in the directory that creates it and below. Running the
# find here (root scope) makes those targets visible in all subdirectories -- in
# particular adapters/pico_wt, a sibling of adapters/picoquic that consumes
# picoquic::picohttp-core without calling find_package itself. (Source-tree mode
# add_subdirectory's picoquic here for the same reason.) FindPicoquic enforces
# the picohttp-core / WebTransport contract based on MOQ_BUILD_ADAPTER_PICO_WT.
if(MOQ_BUILD_ADAPTER_PICOQUIC OR MOQ_BUILD_ADAPTER_PICO_WT)
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
    find_package(Picoquic REQUIRED)
endif()

if(MOQ_BUILD_ADAPTER_PICOQUIC)
    add_subdirectory(adapters/picoquic)
    if(MOQ_BUILD_EXAMPLES)
        add_subdirectory(examples/picoquic)
        if(MOQ_BUILD_LOC AND MOQ_BUILD_MSF AND MOQ_BUILD_CMAF AND MOQ_BUILD_PLAYBACK)
            add_subdirectory(examples/local-media)
        endif()
    endif()
endif()

if(MOQ_BUILD_PQ_THREADED AND NOT MOQ_BUILD_ADAPTER_PICOQUIC)
    message(FATAL_ERROR "MOQ_BUILD_PQ_THREADED=ON requires MOQ_BUILD_ADAPTER_PICOQUIC=ON")
endif()

# -- mvfst adapter ---------------------------------------------------
if(MOQ_BUILD_ADAPTER_MVFST)
    add_subdirectory(adapters/mvfst)
    if(MOQ_BUILD_EXAMPLES)
        add_subdirectory(examples/mvfst)
    endif()
endif()

if(MOQ_BUILD_ADAPTER_PROXYGEN)
    add_subdirectory(adapters/proxygen)
endif()

if(MOQ_BUILD_PICO_WT_MANAGED AND NOT MOQ_BUILD_ADAPTER_PICO_WT)
    message(FATAL_ERROR
        "MOQ_BUILD_PICO_WT_MANAGED=ON requires MOQ_BUILD_ADAPTER_PICO_WT=ON")
endif()

if(MOQ_BUILD_ADAPTER_PICO_WT)
    if(NOT MOQ_BUILD_ADAPTER_PICOQUIC)
        message(FATAL_ERROR
            "MOQ_BUILD_ADAPTER_PICO_WT=ON requires MOQ_BUILD_ADAPTER_PICOQUIC=ON")
    endif()
    add_subdirectory(adapters/pico_wt)
    if(MOQ_BUILD_EXAMPLES)
        add_subdirectory(examples/pico_wt)
    endif()
endif()

# -- Service tier (endpoint + media receiver/sender) ----------------
# After the adapters: the endpoint links the managed facades when built.
# The media receiver/sender surface depends on the MSF catalog parser and
# the media-object helpers; the service tier is one library by design, so
# the prerequisites are explicit (no silent option mutation).
if(MOQ_BUILD_SERVICE)
    if(NOT MOQ_BUILD_MSF)
        message(FATAL_ERROR "MOQ_BUILD_SERVICE=ON requires MOQ_BUILD_MSF=ON")
    endif()
    if(NOT MOQ_BUILD_MEDIA_OBJECT)
        message(FATAL_ERROR
            "MOQ_BUILD_SERVICE=ON requires MOQ_BUILD_MEDIA_OBJECT=ON")
    endif()
    add_subdirectory(service)
endif()

if(MOQ_BUILD_INTEROP_CLIENT)
    if(NOT MOQ_BUILD_ADAPTER_PICOQUIC OR NOT MOQ_BUILD_PQ_THREADED)
        message(FATAL_ERROR
            "MOQ_BUILD_INTEROP_CLIENT=ON requires MOQ_BUILD_ADAPTER_PICOQUIC=ON and MOQ_BUILD_PQ_THREADED=ON")
    endif()
    add_subdirectory(tools/moq-interop-client)
    if(MOQ_BUILD_TESTS)
        add_test(NAME interop_client_smoke
                 COMMAND ${CMAKE_COMMAND} -E env sh
                         ${CMAKE_CURRENT_SOURCE_DIR}/scripts/smoke_interop_client.sh
                         ${CMAKE_BINARY_DIR})
        set_tests_properties(interop_client_smoke PROPERTIES
            TIMEOUT 30
            LABELS "interop;smoke")
    endif()
endif()

# -- Install/export --------------------------------------------------
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

install(EXPORT libmoqTargets
    NAMESPACE moq::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)

configure_package_config_file(
    cmake/libmoqConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/cmake-install/libmoqConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/libmoqConfigVersion.cmake
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/cmake-install/libmoqConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/libmoqConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)

if(MOQ_BUILD_ADAPTER_PICOQUIC OR MOQ_BUILD_ADAPTER_PICO_WT)
    install(FILES
        cmake/FindPicoquic.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)
endif()

install(FILES LICENSE.txt
    DESTINATION ${CMAKE_INSTALL_DOCDIR})

# -- pkg-config for SwiftPM systemLibrary --------------------------
# Static linking requires dependents before dependencies.
set(LIBMOQ_PC_LIBS "")

if(MOQ_BUILD_ADAPTER_PICOQUIC)
    if(MOQ_BUILD_PQ_THREADED)
        string(APPEND LIBMOQ_PC_LIBS " -lmoq-adapter-picoquic-threaded")
    endif()
    string(APPEND LIBMOQ_PC_LIBS " -lmoq-adapter-picoquic")
endif()
if(MOQ_BUILD_PLAYBACK)
    string(APPEND LIBMOQ_PC_LIBS " -lmoq-playback")
endif()
if(MOQ_BUILD_MSF)
    string(APPEND LIBMOQ_PC_LIBS " -lmoq-msf")
endif()
if(MOQ_BUILD_MEDIA_OBJECT)
    string(APPEND LIBMOQ_PC_LIBS " -lmoq-media-object")
endif()
if(MOQ_BUILD_CMAF)
    string(APPEND LIBMOQ_PC_LIBS " -lmoq-cmaf")
endif()
if(MOQ_BUILD_LOC)
    string(APPEND LIBMOQ_PC_LIBS " -lmoq-loc")
endif()
string(APPEND LIBMOQ_PC_LIBS " -lmoq-core")
if(MOQ_BUILD_ADAPTER_PICOQUIC)
    # picoquic-core and picoquic-log are mutually dependent static archives
    # (core -> picoquic_set_qlog in log; log -> picoquic_frame_name etc. in
    # core). A single-pass linker (GNU ld) must scan them as a group for both
    # directions to resolve; macOS ld64 and other multi-pass linkers take the
    # plain list. The .pc is consumed on the platform it was installed on, so
    # gate on the build system here.
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        string(APPEND LIBMOQ_PC_LIBS
            " -Wl,--start-group -lpicoquic-core -lpicoquic-log -Wl,--end-group")
    else()
        string(APPEND LIBMOQ_PC_LIBS " -lpicoquic-core -lpicoquic-log")
    endif()
    # picotls is not installed into the prefix, so bake its lib dir as an
    # absolute -L (external dependency; same non-relocatable caveat as the
    # OpenSSL -L below and the pico WT .pc files). The -L precedes its
    # -lpicotls-* libs (matching the pico WT .pc pattern); without it a
    # `pkg-config libmoq` link cannot find -lpicotls-*. MOQ_PTLS_LIB_DIR is
    # normalized once by FindPicoquic and is reliable in every mode (unlike
    # PTLS_OPENSSL_LIBRARY, which picoquic's FindPTLS may set to a bare target
    # name that is invisible at this scope).
    if(MOQ_PTLS_LIB_DIR)
        string(APPEND LIBMOQ_PC_LIBS " -L${MOQ_PTLS_LIB_DIR}")
    endif()
    string(APPEND LIBMOQ_PC_LIBS " -lpicotls-openssl -lpicotls-core -lpicotls-minicrypto")
    find_package(OpenSSL QUIET)
    if(OPENSSL_FOUND AND OPENSSL_CRYPTO_LIBRARY)
        get_filename_component(_ssl_dir "${OPENSSL_CRYPTO_LIBRARY}" DIRECTORY)
        string(APPEND LIBMOQ_PC_LIBS " -L${_ssl_dir}")
    endif()
    string(APPEND LIBMOQ_PC_LIBS " -lssl -lcrypto -lpthread")
endif()
string(STRIP "${LIBMOQ_PC_LIBS}" LIBMOQ_PC_LIBS)

# Relocatable prefix: the .pc lands at <prefix>/<libdir>/pkgconfig, so the
# up-path from ${pcfiledir} back to <prefix> is (libdir depth + 1) levels.
# Computing it from CMAKE_INSTALL_LIBDIR handles lib, lib64, and multiarch
# dirs (lib/x86_64-linux-gnu). Lets `pkg-config libmoq` resolve correctly
# from any install prefix, not only the configure-time CMAKE_INSTALL_PREFIX.
# (Absolute -L flags for system OpenSSL remain non-relocatable — a system
# dependency, same caveat as the picoquic CMake package.)
string(REPLACE "/" ";" _libmoq_pc_libdir_parts "${CMAKE_INSTALL_LIBDIR}")
list(LENGTH _libmoq_pc_libdir_parts _libmoq_pc_libdir_depth)
math(EXPR _libmoq_pc_ascents "${_libmoq_pc_libdir_depth} + 1")
set(LIBMOQ_PC_UP "..")
foreach(_libmoq_pc_i RANGE 2 ${_libmoq_pc_ascents})
    string(APPEND LIBMOQ_PC_UP "/..")
endforeach()

configure_file(
    cmake/libmoq.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/libmoq.pc
    @ONLY)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoq.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

# -- Tests ----------------------------------------------------------
if(MOQ_BUILD_TESTS)
    add_subdirectory(tests)
endif()

# -- Examples -------------------------------------------------------
# Examples are demonstration code and are intentionally NOT covered by
# MOQ_WARNINGS_AS_ERRORS (see the option's policy note above): they
# prioritize readability and several are gated behind adapter deps.
# MOQ_BUILD_EXAMPLES=OFF gives a library-only build (packaging /
# cross-compiles); it also removes the tests that run example binaries.
if(MOQ_BUILD_TESTS AND MOQ_BUILD_EXAMPLES)
    add_executable(publisher_example examples/publisher/main.c)
    target_link_libraries(publisher_example PRIVATE moq::core)
    add_test(NAME publisher_example COMMAND publisher_example)

    add_executable(setup_handshake examples/session/setup_handshake/main.c)
    target_link_libraries(setup_handshake PRIVATE moq::core)
    add_test(NAME setup_handshake COMMAND setup_handshake)

    add_executable(sans_io_demo examples/session/sans_io_demo/main.c)
    if(TARGET moq::sim)
        target_link_libraries(sans_io_demo PRIVATE moq::sim)
        target_compile_definitions(sans_io_demo PRIVATE MOQ_DEMO_HAS_SIM)
    else()
        target_link_libraries(sans_io_demo PRIVATE moq::core)
    endif()
    add_test(NAME sans_io_demo COMMAND sans_io_demo)
endif()

# -- Consumer-facing service-tier examples --------------------------
# Built whenever the service tier is enabled (independent of MOQ_BUILD_TESTS):
# these are the high-level "start here" examples for media integrators. They
# compile against public headers + moq::service only. No add_test -- running
# them needs a relay -- but they are always compiled for coverage.
if(MOQ_BUILD_SERVICE AND MOQ_BUILD_EXAMPLES)
    add_subdirectory(examples/service)
endif()

# -- C++ examples ---------------------------------------------------
if(MOQ_BUILD_TESTS AND MOQ_BUILD_CPP_BINDING AND MOQ_BUILD_EXAMPLES)
    enable_language(CXX)
    add_executable(local_publish_subscribe
        examples/cpp/local-publish-subscribe/main.cpp)
    target_link_libraries(local_publish_subscribe PRIVATE moq::cpp)
    add_test(NAME local_publish_subscribe COMMAND local_publish_subscribe)

    add_executable(local_relay
        examples/cpp/local-relay/main.cpp)
    target_link_libraries(local_relay PRIVATE moq::cpp)
    add_test(NAME local_relay COMMAND local_relay)
endif()

# -- Local media demo smoke test ------------------------------------
# Mirrors the examples/local-media build condition exactly (the demo
# binaries also need the playback pipeline).
if(MOQ_BUILD_TESTS AND MOQ_BUILD_EXAMPLES AND MOQ_BUILD_ADAPTER_PICOQUIC AND MOQ_BUILD_LOC AND MOQ_BUILD_MSF AND MOQ_BUILD_CMAF AND MOQ_BUILD_PLAYBACK)
    add_test(NAME local_media_demo
             COMMAND ${CMAKE_COMMAND} -E env bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_local_media_demo.sh
                     ${CMAKE_CURRENT_BINARY_DIR} 1)
    set_tests_properties(local_media_demo PROPERTIES TIMEOUT 45)
endif()

# -- Boundary guard -------------------------------------------------
if(MOQ_BUILD_TESTS)
    add_test(NAME api_boundary
             COMMAND ${CMAKE_COMMAND} -E env bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_api_boundary.sh)
    add_test(NAME profile_boundary
             COMMAND ${CMAKE_COMMAND} -E env bash
                     ${CMAKE_CURRENT_SOURCE_DIR}/scripts/check_profile_boundary.sh)
endif()

# -- C++ Binding ----------------------------------------------------
if(MOQ_BUILD_CPP_BINDING)
    add_subdirectory(bindings/cpp)
endif()

# -- Benchmarks -----------------------------------------------------
if(MOQ_BUILD_BENCHMARKS)
    if(NOT MOQ_BUILD_SIM)
        message(FATAL_ERROR "MOQ_BUILD_BENCHMARKS=ON requires MOQ_BUILD_SIM=ON")
    endif()
    add_subdirectory(benchmarks)
endif()

# -- Fuzz -----------------------------------------------------------
if(MOQ_BUILD_FUZZ)
    add_subdirectory(fuzz)
endif()

# -- Build-tree export (for downstream consumption without install) -----
export(EXPORT libmoqTargets
    NAMESPACE moq::
    FILE ${CMAKE_CURRENT_BINARY_DIR}/libmoqTargets.cmake)

set(_build_tree_config "include(CMakeFindDependencyMacro)\n")
string(APPEND _build_tree_config
    "include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqTargets.cmake\")\n")
if(MOQ_BUILD_ADAPTER_PICOQUIC)
    string(APPEND _build_tree_config
        "if(\"adapter-picoquic\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/cmake\")\n"
        "  find_dependency(OpenSSL)\n"
        "  find_dependency(Picoquic)\n"
        "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "  set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-picoquic_FOUND TRUE)\n"
        "endif()\n")
    if(MOQ_BUILD_PQ_THREADED)
        string(APPEND _build_tree_config
            "if(\"adapter-picoquic-threaded\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
            "  if(NOT \${CMAKE_FIND_PACKAGE_NAME}_adapter-picoquic_FOUND)\n"
            "    list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/cmake\")\n"
            "    find_dependency(OpenSSL)\n"
            "    find_dependency(Picoquic)\n"
            "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
            "    set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-picoquic_FOUND TRUE)\n"
            "  endif()\n"
            "  find_dependency(Threads)\n"
            "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterThreadedTargets.cmake\")\n"
            "  set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-picoquic-threaded_FOUND TRUE)\n"
            "endif()\n")
    endif()
endif()
if(MOQ_BUILD_ADAPTER_MVFST)
    string(APPEND _build_tree_config
        "if(\"adapter-mvfst\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  include(CheckLanguage)\n"
        "  check_language(CXX)\n"
        "  if(NOT CMAKE_CXX_COMPILER)\n"
        "    message(FATAL_ERROR \"adapter-mvfst requires a C++ compiler\")\n"
        "  endif()\n"
        "  enable_language(CXX)\n"
        "  list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/adapters/mvfst/cmake\")\n"
        "  find_dependency(mvfst CONFIG)\n"
        "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterMvfstTargets.cmake\")\n"
        "  set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-mvfst_FOUND TRUE)\n"
        "endif()\n")
endif()
if(MOQ_BUILD_ADAPTER_PROXYGEN)
    string(APPEND _build_tree_config
        "if(\"adapter-proxygen-wt\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  include(CheckLanguage)\n"
        "  check_language(CXX)\n"
        "  if(NOT CMAKE_CXX_COMPILER)\n"
        "    message(FATAL_ERROR \"adapter-proxygen-wt requires a C++ compiler\")\n"
        "  endif()\n"
        "  enable_language(CXX)\n"
        "  list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/adapters/mvfst/cmake\")\n"
        "  find_dependency(proxygen CONFIG)\n"
        "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterProxygenWtTargets.cmake\")\n"
        "  set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-proxygen-wt_FOUND TRUE)\n"
        "endif()\n")
    # Managed proxygen WT facade: loads the attach target first (it links it).
    string(APPEND _build_tree_config
        "if(\"adapter-proxygen-wt-managed\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  include(CheckLanguage)\n"
        "  check_language(CXX)\n"
        "  if(NOT CMAKE_CXX_COMPILER)\n"
        "    message(FATAL_ERROR \"adapter-proxygen-wt-managed requires a C++ compiler\")\n"
        "  endif()\n"
        "  enable_language(CXX)\n"
        "  list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/adapters/mvfst/cmake\")\n"
        "  find_dependency(proxygen CONFIG)\n"
        "  if(NOT TARGET moq::adapter-proxygen-wt)\n"
        "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterProxygenWtTargets.cmake\")\n"
        "  endif()\n"
        "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterProxygenWtManagedTargets.cmake\")\n"
        "  set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-proxygen-wt-managed_FOUND TRUE)\n"
        "endif()\n")
endif()
# Build-tree resolution for the pico WT adapter component. The
# install-tree equivalent lives in cmake/libmoqConfig.cmake.in.
if(MOQ_BUILD_ADAPTER_PICO_WT)
    string(APPEND _build_tree_config
        "if(\"adapter-pico-wt\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/cmake\")\n"
        "  find_dependency(OpenSSL)\n"
        "  find_dependency(Picoquic)\n"
        "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtAdapterTargets.cmake\")\n"
        "  set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-pico-wt_FOUND TRUE)\n"
        "endif()\n")
endif()
# Build-tree resolution for the pico WT managed component (build-tree
# only in 15b-1; install-tree wiring is 15b-2). Loads the attach adapter
# first if the consumer did not request it explicitly, mirroring the
# picoquic-threaded branch.
if(MOQ_BUILD_PICO_WT_MANAGED)
    string(APPEND _build_tree_config
        "if(\"adapter-pico-wt-managed\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  if(NOT \${CMAKE_FIND_PACKAGE_NAME}_adapter-pico-wt_FOUND\n"
        "     AND EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtAdapterTargets.cmake\")\n"
        "    list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/cmake\")\n"
        "    find_dependency(OpenSSL)\n"
        "    find_dependency(Picoquic)\n"
        "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtAdapterTargets.cmake\")\n"
        "    set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-pico-wt_FOUND TRUE)\n"
        "  endif()\n"
        "  if(NOT TARGET moq::adapter-picoquic\n"
        "     AND EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "    list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/cmake\")\n"
        "    find_dependency(OpenSSL)\n"
        "    find_dependency(Picoquic)\n"
        "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "    set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-picoquic_FOUND TRUE)\n"
        "  endif()\n"
        "  if(TARGET moq::adapter-pico-wt AND TARGET moq::adapter-picoquic)\n"
        "    find_dependency(Threads)\n"
        "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtManagedTargets.cmake\")\n"
        "    set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-pico-wt-managed_FOUND TRUE)\n"
        "  else()\n"
        "    set(\${CMAKE_FIND_PACKAGE_NAME}_adapter-pico-wt-managed_FOUND FALSE)\n"
        "  endif()\n"
        "endif()\n")
endif()
# Build-tree resolution for the service component (endpoint + media tier).
# Mirrors cmake/libmoqConfig.cmake.in: load whichever managed facades were
# built before the service target (its static interface carries them).
if(MOQ_BUILD_SERVICE)
    string(APPEND _build_tree_config
        "if(\"service\" IN_LIST \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
        "  find_dependency(Threads)\n"
        "  list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/cmake\")\n"
        "  if(NOT TARGET moq::adapter-picoquic-threaded AND\n"
        "     EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterThreadedTargets.cmake\")\n"
        "    if(NOT TARGET moq::adapter-picoquic AND\n"
        "       EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "      find_dependency(OpenSSL)\n"
        "      find_dependency(Picoquic)\n"
        "      include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "    endif()\n"
        "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterThreadedTargets.cmake\")\n"
        "  endif()\n"
        "  if(NOT TARGET moq::adapter-pico-wt-managed AND\n"
        "     EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtManagedTargets.cmake\")\n"
        "    if(NOT TARGET moq::adapter-pico-wt AND\n"
        "       EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtAdapterTargets.cmake\")\n"
        "      find_dependency(OpenSSL)\n"
        "      find_dependency(Picoquic)\n"
        "      include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtAdapterTargets.cmake\")\n"
        "    endif()\n"
        "    if(NOT TARGET moq::adapter-picoquic AND\n"
        "       EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "      find_dependency(OpenSSL)\n"
        "      find_dependency(Picoquic)\n"
        "      include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterTargets.cmake\")\n"
        "    endif()\n"
        "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqPicoWtManagedTargets.cmake\")\n"
        "  endif()\n")
    if(MOQ_BUILD_ADAPTER_MVFST)
        # RAW_QUIC-over-mvfst facade (C++/folly/fizz); CMake-only (no .pc).
        string(APPEND _build_tree_config
            "  if(NOT TARGET moq::adapter-mvfst AND\n"
            "     EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterMvfstTargets.cmake\")\n"
            "    include(CheckLanguage)\n"
            "    check_language(CXX)\n"
            "    if(NOT CMAKE_CXX_COMPILER)\n"
            "      message(FATAL_ERROR\n"
            "        \"libmoq service was built with the mvfst backend; \"\n"
            "        \"linking it requires a C++ compiler\")\n"
            "    endif()\n"
            "    enable_language(CXX)\n"
            "    list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/adapters/mvfst/cmake\")\n"
            "    find_dependency(mvfst CONFIG)\n"
            "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterMvfstTargets.cmake\")\n"
            "  endif()\n")
    endif()
    if(MOQ_BUILD_ADAPTER_PROXYGEN)
        # WEBTRANSPORT-over-proxygen facade (C++/folly/fizz); CMake-only (no .pc).
        # The managed target links the attach target, so load that first.
        string(APPEND _build_tree_config
            "  if(NOT TARGET moq::adapter-proxygen-wt-managed AND\n"
            "     EXISTS \"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterProxygenWtManagedTargets.cmake\")\n"
            "    include(CheckLanguage)\n"
            "    check_language(CXX)\n"
            "    if(NOT CMAKE_CXX_COMPILER)\n"
            "      message(FATAL_ERROR\n"
            "        \"libmoq service was built with the proxygen backend; \"\n"
            "        \"linking it requires a C++ compiler\")\n"
            "    endif()\n"
            "    enable_language(CXX)\n"
            "    list(APPEND CMAKE_MODULE_PATH \"${CMAKE_CURRENT_SOURCE_DIR}/adapters/mvfst/cmake\")\n"
            "    find_dependency(proxygen CONFIG)\n"
            "    if(NOT TARGET moq::adapter-proxygen-wt)\n"
            "      include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterProxygenWtTargets.cmake\")\n"
            "    endif()\n"
            "    include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqAdapterProxygenWtManagedTargets.cmake\")\n"
            "  endif()\n")
    endif()
    string(APPEND _build_tree_config
        "  include(\"\${CMAKE_CURRENT_LIST_DIR}/libmoqServiceTargets.cmake\")\n"
        "  set(\${CMAKE_FIND_PACKAGE_NAME}_service_FOUND TRUE)\n"
        "endif()\n")
endif()
string(APPEND _build_tree_config
    "foreach(_libmoq_comp IN LISTS \${CMAKE_FIND_PACKAGE_NAME}_FIND_COMPONENTS)\n"
    "  if(NOT \${CMAKE_FIND_PACKAGE_NAME}_\${_libmoq_comp}_FOUND)\n"
    "    if(\${CMAKE_FIND_PACKAGE_NAME}_FIND_REQUIRED_\${_libmoq_comp})\n"
    "      set(\${CMAKE_FIND_PACKAGE_NAME}_FOUND FALSE)\n"
    "      set(\${CMAKE_FIND_PACKAGE_NAME}_NOT_FOUND_MESSAGE \"Required component '\${_libmoq_comp}' not found\")\n"
    "    endif()\n"
    "  endif()\n"
    "endforeach()\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/libmoqConfig.cmake
    "${_build_tree_config}")
