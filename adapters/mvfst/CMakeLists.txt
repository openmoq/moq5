# moq-adapter-mvfst: mvfst QUIC transport adapter for libmoq.
#
# Optional. Requires MOQ_BUILD_ADAPTER_MVFST=ON and an installed
# mvfst (with fizz, folly). C++ is enabled only for this path;
# the rest of libmoq stays C-only.

enable_language(CXX)

# -- Dependency prefix resolution -----------------------------------
# These cache vars live here, not in the root CMakeLists.txt, because
# they are only relevant when building the mvfst adapter.

set(MOQ_MVFST_PREFIX "" CACHE PATH "Install prefix for mvfst")
set(MOQ_FIZZ_PREFIX  "" CACHE PATH "Install prefix for fizz")
set(MOQ_FOLLY_PREFIX "" CACHE PATH "Install prefix for folly")
set(MOQ_MVFST_SOURCE_DIR "" CACHE PATH
    "mvfst source tree (provides cmake/FindSodium.cmake)")

foreach(_pfx MOQ_MVFST_PREFIX MOQ_FIZZ_PREFIX MOQ_FOLLY_PREFIX)
    if(NOT "${${_pfx}}" STREQUAL "")
        list(APPEND CMAKE_PREFIX_PATH "${${_pfx}}")
    endif()
endforeach()

# -- FindSodium.cmake -----------------------------------------------
# fizz calls find_dependency(Sodium), but libsodium has no CMake
# config files — it needs a FindSodium.cmake module. If the caller
# provides MOQ_MVFST_SOURCE_DIR, prefer its copy; otherwise fall
# back to the CC0-licensed vendored copy in our cmake/ directory.
if(MOQ_MVFST_SOURCE_DIR)
    list(APPEND CMAKE_MODULE_PATH "${MOQ_MVFST_SOURCE_DIR}/cmake")
endif()
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# -- Resolve mvfst --------------------------------------------------

find_package(mvfst CONFIG REQUIRED)

# -- Adapter library ------------------------------------------------

add_library(moq-adapter-mvfst
    src/mvfst_adapter.cpp
    src/mvfst_endpoint_ops.cpp
)

add_library(moq::adapter-mvfst ALIAS moq-adapter-mvfst)

target_include_directories(moq-adapter-mvfst
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-adapter-mvfst
    PUBLIC  moq::core
    PRIVATE mvfst::mvfst
)

target_compile_features(moq-adapter-mvfst PUBLIC cxx_std_17)

set_target_properties(moq-adapter-mvfst PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-adapter-mvfst
    EXPORT_NAME adapter-mvfst
)

target_compile_definitions(moq-adapter-mvfst PRIVATE MOQ_BUILDING)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-adapter-mvfst PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
endif()

# -- Install (separate from core libmoqTargets) ---------------------

install(TARGETS moq-adapter-mvfst
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp")

# Install vendored FindSodium.cmake beside the package config files
# so install-tree consumers can find it.
install(FILES cmake/FindSodium.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq)

# -- Adapter target files -------------------------------------------
# Separate from core libmoqTargets to keep the optional boundary.
# Build-tree targets (absolute paths — valid on this machine).
set(_mvfst_adapter_link "moq::core;mvfst::mvfst")
file(GENERATE
    OUTPUT "${PROJECT_BINARY_DIR}/libmoqAdapterMvfstTargets.cmake"
    CONTENT "\
if(NOT TARGET moq::adapter-mvfst)\n\
  add_library(moq::adapter-mvfst $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-mvfst,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-mvfst PROPERTIES\n\
    IMPORTED_LOCATION \"$<TARGET_FILE:moq-adapter-mvfst>\"\n\
    IMPORTED_LINK_INTERFACE_LANGUAGES \"CXX\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"${CMAKE_CURRENT_SOURCE_DIR}/include\"\n\
    INTERFACE_LINK_LIBRARIES \"${_mvfst_adapter_link}\")\n\
endif()\n")

# Install-tree targets (relocatable via prefix detection).
set(_mvfst_adapter_lib_name "$<TARGET_FILE_NAME:moq-adapter-mvfst>")
set(_mvfst_adapter_libdir "${CMAKE_INSTALL_LIBDIR}")
set(_mvfst_adapter_incdir "${CMAKE_INSTALL_INCLUDEDIR}")
file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterMvfstTargets-install.cmake
    CONTENT "\
get_filename_component(_MVFST_PREFIX \"\${CMAKE_CURRENT_LIST_FILE}\" PATH)\n\
get_filename_component(_MVFST_PREFIX \"\${_MVFST_PREFIX}\" PATH)\n\
get_filename_component(_MVFST_PREFIX \"\${_MVFST_PREFIX}\" PATH)\n\
get_filename_component(_MVFST_PREFIX \"\${_MVFST_PREFIX}\" PATH)\n\
\n\
if(NOT TARGET moq::adapter-mvfst)\n\
  add_library(moq::adapter-mvfst $<IF:$<STREQUAL:$<TARGET_PROPERTY:moq-adapter-mvfst,TYPE>,STATIC_LIBRARY>,STATIC,SHARED> IMPORTED)\n\
  set_target_properties(moq::adapter-mvfst PROPERTIES\n\
    IMPORTED_LOCATION \"\${_MVFST_PREFIX}/${_mvfst_adapter_libdir}/${_mvfst_adapter_lib_name}\"\n\
    IMPORTED_LINK_INTERFACE_LANGUAGES \"CXX\"\n\
    INTERFACE_INCLUDE_DIRECTORIES \"\${_MVFST_PREFIX}/${_mvfst_adapter_incdir}\"\n\
    INTERFACE_LINK_LIBRARIES \"moq::core$<SEMICOLON>mvfst::mvfst\")\n\
endif()\n\
\n\
unset(_MVFST_PREFIX)\n")

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmoqAdapterMvfstTargets-install.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/libmoq
    RENAME libmoqAdapterMvfstTargets.cmake)

# -- Smoke tests ----------------------------------------------------

if(MOQ_BUILD_TESTS)
    add_executable(test_mvfst_c_smoke tests/test_c_smoke.c)
    target_link_libraries(test_mvfst_c_smoke PRIVATE moq::adapter-mvfst)
    add_test(NAME mvfst_c_smoke COMMAND test_mvfst_c_smoke)

    add_executable(test_mvfst_cxx_smoke tests/test_cxx_smoke.cpp)
    target_link_libraries(test_mvfst_cxx_smoke PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_cxx_smoke COMMAND test_mvfst_cxx_smoke)

    # Old mvfst_bridge_conformance removed — replaced by shared
    # bridge_conformance_pair in core tests (13/13 scenarios).

    add_executable(test_mvfst_managed tests/test_managed.c)
    find_package(Threads REQUIRED)
    target_link_libraries(test_mvfst_managed PRIVATE moq::adapter-mvfst Threads::Threads)
    add_test(NAME mvfst_managed COMMAND test_mvfst_managed)

    add_executable(test_mvfst_loopback tests/test_loopback.cpp)
    target_link_libraries(test_mvfst_loopback PRIVATE
        moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_loopback COMMAND test_mvfst_loopback)
    set_tests_properties(mvfst_loopback PROPERTIES TIMEOUT 10)

    # Draft-18 loopback: symmetric uni-control establish + request-bidi
    # routing + subgroup data. Own binary (one QuicServer per process).
    add_executable(test_mvfst_loopback_d18 tests/test_loopback_d18.cpp)
    target_link_libraries(test_mvfst_loopback_d18 PRIVATE
        moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_loopback_d18 COMMAND test_mvfst_loopback_d18)
    set_tests_properties(mvfst_loopback_d18 PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_loopback tests/test_managed_loopback.cpp)
    target_link_libraries(test_mvfst_managed_loopback PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_loopback COMMAND test_mvfst_managed_loopback)
    set_tests_properties(mvfst_managed_loopback PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_cert tests/test_managed_cert.cpp)
    target_link_libraries(test_mvfst_managed_cert PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_cert COMMAND test_mvfst_managed_cert)
    set_tests_properties(mvfst_managed_cert PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_cert_mismatch tests/test_managed_cert_mismatch.cpp)
    target_link_libraries(test_mvfst_managed_cert_mismatch PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_cert_mismatch COMMAND test_mvfst_managed_cert_mismatch)
    set_tests_properties(mvfst_managed_cert_mismatch PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_cert_dns_as_ip tests/test_managed_cert_dns_as_ip.cpp)
    target_link_libraries(test_mvfst_managed_cert_dns_as_ip PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_cert_dns_as_ip COMMAND test_mvfst_managed_cert_dns_as_ip)
    set_tests_properties(mvfst_managed_cert_dns_as_ip PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_dns tests/test_managed_dns.cpp)
    target_link_libraries(test_mvfst_managed_dns PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_dns COMMAND test_mvfst_managed_dns)
    set_tests_properties(mvfst_managed_dns PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_dns_mismatch tests/test_managed_dns_mismatch.cpp)
    target_link_libraries(test_mvfst_managed_dns_mismatch PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_dns_mismatch COMMAND test_mvfst_managed_dns_mismatch)
    set_tests_properties(mvfst_managed_dns_mismatch PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_server_loopback tests/test_managed_server_loopback.cpp)
    target_link_libraries(test_mvfst_managed_server_loopback PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_server_loopback COMMAND test_mvfst_managed_server_loopback)
    set_tests_properties(mvfst_managed_server_loopback PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_server_multi tests/test_managed_server_multi.cpp)
    target_link_libraries(test_mvfst_managed_server_multi PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_server_multi COMMAND test_mvfst_managed_server_multi)
    set_tests_properties(mvfst_managed_server_multi PROPERTIES TIMEOUT 15)

    add_executable(test_mvfst_managed_server tests/test_managed_server.cpp)
    target_link_libraries(test_mvfst_managed_server PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_server COMMAND test_mvfst_managed_server)
    set_tests_properties(mvfst_managed_server PROPERTIES TIMEOUT 10)

    add_executable(test_mvfst_managed_negative tests/test_managed_negative.cpp)
    target_link_libraries(test_mvfst_managed_negative PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_negative COMMAND test_mvfst_managed_negative)
    set_tests_properties(mvfst_managed_negative PROPERTIES TIMEOUT 30)

    add_executable(test_mvfst_managed_server_negative tests/test_managed_server_negative.cpp)
    target_link_libraries(test_mvfst_managed_server_negative PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_server_negative COMMAND test_mvfst_managed_server_negative)
    set_tests_properties(mvfst_managed_server_negative PROPERTIES TIMEOUT 15)

    add_executable(test_mvfst_managed_stress tests/test_managed_stress.cpp)
    target_link_libraries(test_mvfst_managed_stress PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_stress COMMAND test_mvfst_managed_stress)
    set_tests_properties(mvfst_managed_stress PROPERTIES TIMEOUT 60)

    add_executable(test_mvfst_managed_goaway tests/test_managed_goaway.cpp)
    target_link_libraries(test_mvfst_managed_goaway PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_goaway COMMAND test_mvfst_managed_goaway)
    set_tests_properties(mvfst_managed_goaway PROPERTIES TIMEOUT 15)

    add_executable(test_mvfst_managed_backpressure tests/test_managed_backpressure.cpp)
    target_link_libraries(test_mvfst_managed_backpressure PRIVATE moq::adapter-mvfst mvfst::mvfst)
    add_test(NAME mvfst_managed_backpressure COMMAND test_mvfst_managed_backpressure)
    set_tests_properties(mvfst_managed_backpressure PROPERTIES TIMEOUT 30)

    # Managed-mode outbound stream-credit gating. The test
    # observes credit block/grant counts via adapter-local accessors that are
    # NOT public API. To reach them without exporting hidden adapter symbols
    # (which a shared moq-adapter-mvfst would not expose), compile the adapter
    # sources into a default-visibility test-internals object library that
    # defines MOQ_MVFST_TESTING. The shipped adapter is unaffected: it neither
    # defines MOQ_MVFST_TESTING nor exports these symbols.
    add_library(moq-adapter-mvfst-test-internals OBJECT
        src/mvfst_adapter.cpp
        src/mvfst_endpoint_ops.cpp)
    target_include_directories(moq-adapter-mvfst-test-internals
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(moq-adapter-mvfst-test-internals
        PUBLIC moq::core mvfst::mvfst)
    target_compile_features(moq-adapter-mvfst-test-internals PUBLIC cxx_std_17)
    target_compile_definitions(moq-adapter-mvfst-test-internals
        PRIVATE MOQ_BUILDING MOQ_MVFST_TESTING)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-adapter-mvfst-test-internals PRIVATE
            -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror> -Wno-unused-parameter)
    endif()

    add_executable(test_mvfst_managed_stream_credit tests/test_managed_stream_credit.cpp)
    target_link_libraries(test_mvfst_managed_stream_credit PRIVATE
        moq-adapter-mvfst-test-internals mvfst::mvfst)
    add_test(NAME mvfst_managed_stream_credit COMMAND test_mvfst_managed_stream_credit)
    set_tests_properties(mvfst_managed_stream_credit PROPERTIES TIMEOUT 30)

    # -- Build-tree consumer tests ------------------------------------
    # C-only consumer: configure + build against build-tree config.
    set(_c_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/c_consumer_test")
    set(_c_consumer_bin "${CMAKE_CURRENT_BINARY_DIR}/c_consumer_build")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/run_c_consumer_test.cmake"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    -S \"${_c_consumer_src}\"\n"
        "    -B \"${_c_consumer_bin}\"\n"
        "    -Dlibmoq_DIR=${PROJECT_BINARY_DIR}\n"
        "    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"configure failed\")\nendif()\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${_c_consumer_bin}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"build failed\")\nendif()\n")
    add_test(NAME mvfst_c_consumer_build
        COMMAND ${CMAKE_COMMAND} -P
            "${CMAKE_CURRENT_BINARY_DIR}/run_c_consumer_test.cmake")
    set_tests_properties(mvfst_c_consumer_build PROPERTIES TIMEOUT 60)

    # -- Install-tree consumer tests ----------------------------------
    # Install to a temp prefix, then build C and C++ consumers against it.
    set(_cxx_consumer_src "${CMAKE_CURRENT_SOURCE_DIR}/tests/cxx_consumer_test")
    set(_install_prefix "${CMAKE_CURRENT_BINARY_DIR}/install_test_prefix")
    set(_install_c_bin "${CMAKE_CURRENT_BINARY_DIR}/install_c_consumer_build")
    set(_install_cxx_bin "${CMAKE_CURRENT_BINARY_DIR}/install_cxx_consumer_build")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/run_install_consumer_test.cmake"
        "# Build before install to ensure targets are up to date\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${PROJECT_BINARY_DIR}\"\n"
        "    --target moq-core moq-adapter-mvfst\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"build failed\")\nendif()\n"
        "\n"
        "# Install libmoq to temp prefix\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --install \"${PROJECT_BINARY_DIR}\"\n"
        "    --prefix \"${_install_prefix}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"install failed\")\nendif()\n"
        "\n"
        "# C consumer against install-tree\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    -S \"${_c_consumer_src}\"\n"
        "    -B \"${_install_c_bin}\"\n"
        "    -DCMAKE_PREFIX_PATH=${_install_prefix}\n"
        "    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"C consumer configure failed\")\nendif()\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${_install_c_bin}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"C consumer build failed\")\nendif()\n"
        "\n"
        "# C++ consumer against install-tree\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    -S \"${_cxx_consumer_src}\"\n"
        "    -B \"${_install_cxx_bin}\"\n"
        "    -DCMAKE_PREFIX_PATH=${_install_prefix}\n"
        "    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"C++ consumer configure failed\")\nendif()\n"
        "execute_process(COMMAND \${CMAKE_COMMAND}\n"
        "    --build \"${_install_cxx_bin}\"\n"
        "    RESULT_VARIABLE rc)\n"
        "if(rc)\n  message(FATAL_ERROR \"C++ consumer build failed\")\nendif()\n")
    add_test(NAME mvfst_install_consumer_build
        COMMAND ${CMAKE_COMMAND} -P
            "${CMAKE_CURRENT_BINARY_DIR}/run_install_consumer_test.cmake")
    set_tests_properties(mvfst_install_consumer_build PROPERTIES TIMEOUT 120)
endif()
