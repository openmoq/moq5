# moq-adapter-proxygen-wt: proxygen WebTransport adapter for libmoq.
#
# Optional. Requires MOQ_BUILD_ADAPTER_PROXYGEN=ON and installed proxygen.

enable_language(CXX)

# Sodium is needed by fizz (via mvfst via proxygen) but has no CMake config.
# Use the vendored FindSodium.cmake from the mvfst adapter if available.
if(MOQ_MVFST_SOURCE_DIR)
    list(APPEND CMAKE_MODULE_PATH "${MOQ_MVFST_SOURCE_DIR}/cmake")
endif()
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../mvfst/cmake")

find_package(proxygen CONFIG REQUIRED)
find_package(c-ares CONFIG QUIET)

add_library(moq-adapter-proxygen-wt
    src/wt_endpoint_ops.cpp
    src/wt_adapter.cpp
)

add_library(moq::adapter-proxygen-wt ALIAS moq-adapter-proxygen-wt)

target_include_directories(moq-adapter-proxygen-wt
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# proxygen is PUBLIC: <moq/proxygen_wt.hpp> publicly includes proxygen +
# folly headers, so consumers of this component must inherit proxygen's
# include dirs. This dependency is scoped to the proxygen adapter target
# only — it never enters libmoq core or any non-proxygen installed header.
target_link_libraries(moq-adapter-proxygen-wt
    PUBLIC  moq::core proxygen::proxygen
)

target_compile_features(moq-adapter-proxygen-wt PUBLIC cxx_std_17)

set_target_properties(moq-adapter-proxygen-wt PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-adapter-proxygen-wt
    EXPORT_NAME adapter-proxygen-wt
)

target_compile_definitions(moq-adapter-proxygen-wt PRIVATE MOQ_BUILDING)

# wangle/proxygen exports a bare -lcares without a library path.
# Resolve once for the adapter library and all test executables.
if(c-ares_FOUND)
    get_target_property(_cares_loc c-ares::cares LOCATION)
    if(_cares_loc)
        get_filename_component(_cares_dir "${_cares_loc}" DIRECTORY)
        target_link_directories(moq-adapter-proxygen-wt PRIVATE "${_cares_dir}")
    endif()
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-adapter-proxygen-wt PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
endif()

# -- Install (separate from core libmoqTargets) ---------------------
# Mirrors the mvfst adapter: plain install(TARGETS) + a hand-generated
# imported-target file (build-tree + relocatable install-tree), because
# the proxygen link interface can't pass core export-set validation.

install(TARGETS moq-adapter-proxygen-wt
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Install ONLY the single public header explicitly — preserves the
# "only this surface is public" rule. Internal headers (e.g.
# wt_endpoint_ops.h) live in src/ and are never installed.
install(FILES include/moq/proxygen_wt.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)

# proxygen's dependency chain (fizz) needs libsodium, which ships no CMake
# config — install the vendored FindSodium.cmake beside the package config
# so install-tree find_dependency(proxygen CONFIG) can resolve it. Shared
# dest with the mvfst adapter; identical file, so the install is idempotent
# when both adapters are enabled.
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../mvfst/cmake/FindSodium.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)

# proxygen/wangle export a bare "-lcares" with no library path. The
# adapter library works around it with target_link_directories; consumers
# of the imported target need the same path, so propagate it via
# INTERFACE_LINK_DIRECTORIES (absolute — a system dependency, same
# non-relocatable caveat as picotls/OpenSSL elsewhere). Empty if c-ares
# was found as a proper imported target with its own location.
set(_pxg_linkdir_line "")
if(_cares_dir)
    set(_pxg_linkdir_line "    INTERFACE_LINK_DIRECTORIES \"${_cares_dir}\"\n")
endif()

# Build-tree imported target (absolute paths — valid on this machine).
set(_pxg_adapter_link "moq::core;proxygen::proxygen")
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqAdapterProxygenWtTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-proxygen-wt)\n\
  add_library(moq::adapter-proxygen-wt $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-proxygen-wt,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-proxygen-wt PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-proxygen-wt>\"\n\
    IMPORTED_LINK_INTERFACE_LANGUAGES \"CXX\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
${_pxg_linkdir_line}\
    INTERFACE_LINK_LIBRARIES \"${_pxg_adapter_link}\")\n\
endif()\n")

# Install-tree imported target (relocatable via prefix detection).
set(_pxg_adapter_lib_name "$<TARGET_FILE_NAME:moq-adapter-proxygen-wt>")
set(_pxg_adapter_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_pxg_adapter_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterProxygenWtTargets-install.cmake
    CONTENT "\
get_filename_component(_PXG_PREFIX \"\${CMAKE_CURRENT_LIST_FILE}\" PATH)\n\
get_filename_component(_PXG_PREFIX \"\${_PXG_PREFIX}\" PATH)\n\
get_filename_component(_PXG_PREFIX \"\${_PXG_PREFIX}\" PATH)\n\
get_filename_component(_PXG_PREFIX \"\${_PXG_PREFIX}\" PATH)\n\
\n\
if(NOT TARGET moq::adapter-proxygen-wt)\n\
  add_library(moq::adapter-proxygen-wt $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-proxygen-wt,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-proxygen-wt PROPERTIES\n\
    IMPORTED_LOCATION \"\${_PXG_PREFIX}/${_pxg_adapter_libdir}/${_pxg_adapter_lib_name}\"\n\
    IMPORTED_LINK_INTERFACE_LANGUAGES \"CXX\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"\${_PXG_PREFIX}/${_pxg_adapter_incdir}\"\n\
${_pxg_linkdir_line}\
    INTERFACE_LINK_LIBRARIES \"moq::core$<SEMICOLON>proxygen::proxygen\")\n\
endif()\n\
\n\
unset(_PXG_PREFIX)\n")

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterProxygenWtTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqAdapterProxygenWtTargets.cmake)

# -- Managed client facade (owns connect + thread + session) ---------
# Separate target: the managed facade uses C++20 coroutines (proxygen
# HTTPCoroConnector/sendWtReq), whereas the attach adapter is C++17.
add_library(moq-adapter-proxygen-wt-managed
    src/wt_managed.cpp
)
add_library(moq::adapter-proxygen-wt-managed ALIAS moq-adapter-proxygen-wt-managed)

target_include_directories(moq-adapter-proxygen-wt-managed
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_link_libraries(moq-adapter-proxygen-wt-managed
    PUBLIC  moq::adapter-proxygen-wt proxygen::proxygen)

target_compile_features(moq-adapter-proxygen-wt-managed PUBLIC cxx_std_20)

set_target_properties(moq-adapter-proxygen-wt-managed PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-adapter-proxygen-wt-managed
    EXPORT_NAME adapter-proxygen-wt-managed)

target_compile_definitions(moq-adapter-proxygen-wt-managed PRIVATE MOQ_BUILDING)

if(_cares_dir)
    # PUBLIC so in-tree consumers (the service tier and its test executables)
    # inherit the c-ares search path: proxygen/wangle emit a bare -lcares with
    # no -L. External consumers get it via INTERFACE_LINK_DIRECTORIES in the
    # generated imported-target file (_pxg_linkdir_line).
    target_link_directories(moq-adapter-proxygen-wt-managed PUBLIC "${_cares_dir}")
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-adapter-proxygen-wt-managed PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
endif()

install(TARGETS moq-adapter-proxygen-wt-managed
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(FILES include/moq/proxygen_wt_managed.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/moq)

# Build-tree imported target (absolute paths).
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqAdapterProxygenWtManagedTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-proxygen-wt-managed)\n\
  add_library(moq::adapter-proxygen-wt-managed $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-proxygen-wt-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-proxygen-wt-managed PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-proxygen-wt-managed>\"\n\
    IMPORTED_LINK_INTERFACE_LANGUAGES \"CXX\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
${_pxg_linkdir_line}\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-proxygen-wt$<SEMICOLON>proxygen::proxygen\")\n\
endif()\n")

# Install-tree imported target (relocatable).
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterProxygenWtManagedTargets-install.cmake
    CONTENT "\
get_filename_component(_PXGM_PREFIX \"\${CMAKE_CURRENT_LIST_FILE}\" PATH)\n\
get_filename_component(_PXGM_PREFIX \"\${_PXGM_PREFIX}\" PATH)\n\
get_filename_component(_PXGM_PREFIX \"\${_PXGM_PREFIX}\" PATH)\n\
get_filename_component(_PXGM_PREFIX \"\${_PXGM_PREFIX}\" PATH)\n\
\n\
if(NOT TARGET moq::adapter-proxygen-wt-managed)\n\
  add_library(moq::adapter-proxygen-wt-managed $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-proxygen-wt-managed,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-proxygen-wt-managed PROPERTIES\n\
    IMPORTED_LOCATION \"\${_PXGM_PREFIX}/${_pxg_adapter_libdir}/$<TARGET_FILE_NAME:moq-adapter-proxygen-wt-managed>\"\n\
    IMPORTED_LINK_INTERFACE_LANGUAGES \"CXX\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"\${_PXGM_PREFIX}/${_pxg_adapter_incdir}\"\n\
${_pxg_linkdir_line}\
    INTERFACE_LINK_LIBRARIES \"moq::adapter-proxygen-wt$<SEMICOLON>proxygen::proxygen\")\n\
endif()\n\
\n\
unset(_PXGM_PREFIX)\n")

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterProxygenWtManagedTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqAdapterProxygenWtManagedTargets.cmake)

# -- Tests -----------------------------------------------------------
if(MOQ_BUILD_TESTS)
    add_executable(test_wt_endpoint_ops
        tests/test_wt_endpoint_ops.cpp
        src/wt_endpoint_ops.cpp
    )
    target_include_directories(test_wt_endpoint_ops PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    target_link_libraries(test_wt_endpoint_ops PRIVATE
        moq::core
        proxygen::proxygen
    )
    if(_cares_dir)
        target_link_directories(test_wt_endpoint_ops PRIVATE "${_cares_dir}")
    endif()
    target_compile_features(test_wt_endpoint_ops PRIVATE cxx_std_17)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_wt_endpoint_ops PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME proxygen_wt_endpoint_ops COMMAND test_wt_endpoint_ops)

    add_executable(test_wt_adapter
        tests/test_wt_adapter.cpp
    )
    target_include_directories(test_wt_adapter PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    target_link_libraries(test_wt_adapter PRIVATE
        moq::adapter-proxygen-wt
        proxygen::proxygen
    )
    if(_cares_dir)
        target_link_directories(test_wt_adapter PRIVATE "${_cares_dir}")
    endif()
    target_compile_features(test_wt_adapter PRIVATE cxx_std_17)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_wt_adapter PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME proxygen_wt_adapter COMMAND test_wt_adapter)

    add_executable(test_wt_conformance
        tests/test_wt_conformance.cpp
        tests/wt_conformance_pair.cpp
        ${PROJECT_SOURCE_DIR}/tests/conformance/conformance_scenarios.c
    )
    target_include_directories(test_wt_conformance PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/tests/conformance
    )
    target_link_libraries(test_wt_conformance PRIVATE
        moq::adapter-proxygen-wt
        proxygen::proxygen
    )
    if(c-ares_FOUND AND _cares_dir)
        target_link_directories(test_wt_conformance PRIVATE "${_cares_dir}")
    endif()
    target_compile_features(test_wt_conformance PRIVATE cxx_std_17)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_wt_conformance PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME proxygen_wt_conformance COMMAND test_wt_conformance)

    # Managed client facade: config/validation + ABI (no network).
    add_executable(test_wt_managed tests/test_wt_managed.cpp)
    target_link_libraries(test_wt_managed PRIVATE
        moq::adapter-proxygen-wt-managed proxygen::proxygen)
    if(_cares_dir)
        target_link_directories(test_wt_managed PRIVATE "${_cares_dir}")
    endif()
    target_compile_features(test_wt_managed PRIVATE cxx_std_20)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_wt_managed PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()
    add_test(NAME proxygen_wt_managed COMMAND test_wt_managed)
    set_tests_properties(proxygen_wt_managed PROPERTIES TIMEOUT 30)

    # -- Cold-consumer tests ------------------------------------------
    # Build-tree consumer: find_package(libmoq COMPONENTS
    # adapter-proxygen-wt) against the build tree, include ONLY
    # <moq/proxygen_wt.hpp>, link ONLY moq::adapter-proxygen-wt, run a
    # no-network smoke. (proxygen resolves from the default prefix, same
    # as the parent build.)
    set(_pxg_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/consumer")
    set(_pxg_consumer_bin "${CMAKE_CURRENT_BINARY_DIR}/consumer_build")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/run_consumer_test.cmake"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    -S \"${_pxg_consumer_src}\"\n"
        "    -B \"${_pxg_consumer_bin}\"\n"
        "    -Dlibmoq_DIR=${PROJECT_BINARY_DIR}\n"
        "    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"configure failed\")\nendif()\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${_pxg_consumer_bin}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"build failed\")\nendif()\n"
        "execute_process(COMMAND \"${_pxg_consumer_bin}/consumer\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"consumer run failed\")\nendif()\n")
    add_test(NAME proxygen_wt_consumer
        COMMAND ${CMAKE_COMMAND} -P
            "${CMAKE_CURRENT_BINARY_DIR}/run_consumer_test.cmake")
    set_tests_properties(proxygen_wt_consumer PROPERTIES TIMEOUT 120)

    # Install-tree consumer + negative installed-package audit. Install
    # libmoq to a temp prefix, consume via find_package from that prefix,
    # and assert the installed include tree exposes only the public header.
    set(_pxg_install_prefix "${CMAKE_CURRENT_BINARY_DIR}/install_test_prefix")
    set(_pxg_install_bin "${CMAKE_CURRENT_BINARY_DIR}/install_consumer_build")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/run_install_consumer_test.cmake"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${PROJECT_BINARY_DIR}\"\n"
        "    --target moq-core moq-adapter-proxygen-wt\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"build failed\")\nendif()\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --install \"${PROJECT_BINARY_DIR}\"\n"
        "    --prefix \"${_pxg_install_prefix}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"install failed\")\nendif()\n"
        "\n"
        "# Negative installed-package audit.\n"
        "set(_inc \"${_pxg_install_prefix}/${CMAKE_INSTALL_INCLUDEDIR}\")\n"
        "if(NOT EXISTS \"\${_inc}/moq/proxygen_wt.hpp\")\n"
        "  message(FATAL_ERROR \"public header not installed\")\nendif()\n"
        "foreach(_priv transport_bridge.h wt_endpoint_ops.h\n"
        "              fake_webtransport.h fake_wt_pair.h)\n"
        "  file(GLOB_RECURSE _hit \"\${_inc}/\${_priv}\" \"\${_inc}/**/\${_priv}\")\n"
        "  if(_hit)\n    message(FATAL_ERROR \"private header leaked: \${_priv} -> \${_hit}\")\nendif()\n"
        "endforeach()\n"
        "file(READ \"\${_inc}/moq/proxygen_wt.hpp\" _pub)\n"
        "if(_pub MATCHES \"transport_bridge.h\")\n"
        "  message(FATAL_ERROR \"public header still requires transport_bridge.h\")\nendif()\n"
        "if(_pub MATCHES \"AdapterTestAccess\")\n"
        "  message(FATAL_ERROR \"public header exposes test seam AdapterTestAccess\")\nendif()\n"
        "if(_pub MATCHES \"MOQ_BUILDING\")\n"
        "  message(FATAL_ERROR \"public header references build-only macro MOQ_BUILDING\")\nendif()\n"
        "\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    -S \"${_pxg_consumer_src}\"\n"
        "    -B \"${_pxg_install_bin}\"\n"
        "    -DCMAKE_PREFIX_PATH=${_pxg_install_prefix}\n"
        "    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"install consumer configure failed\")\nendif()\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${_pxg_install_bin}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"install consumer build failed\")\nendif()\n"
        "execute_process(COMMAND \"${_pxg_install_bin}/consumer\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"install consumer run failed\")\nendif()\n")
    add_test(NAME proxygen_wt_install_consumer
        COMMAND ${CMAKE_COMMAND} -P
            "${CMAKE_CURRENT_BINARY_DIR}/run_install_consumer_test.cmake")
    set_tests_properties(proxygen_wt_install_consumer PROPERTIES TIMEOUT 180)
endif()
